#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE 1
#endif
#include "lc_collection_unit.h"
#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_pouch_record.h"
#include <errno.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
#include <zlib.h>

#ifdef LOCKDC_TEST_BUILD
ssize_t (*lc_collection_unit_test_preadv)(int, const struct iovec *, int,
                                          off_t);
#endif

static void lc_cu_put(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static lc_u64 lc_cu_get(const unsigned char *p) {
  lc_u64 n = 0;
  size_t i;
  for (i = 0; i < 8U; ++i)
    n |= (lc_u64)p[i] << (8U * i);
  return n;
}
static void lc_cu_short(unsigned char *p, size_t n) {
  p[0] = (unsigned char)n;
  p[1] = (unsigned char)(n >> 8U);
}
static size_t lc_cu_length(const unsigned char *p) {
  return p[0] | ((size_t)p[1] << 8U);
}
static int lc_cu_corrupt(lc_error *e) {
  return lc_collection_failure(
      e, LC_ERR_PROTOCOL, "collection_corrupt",
      "collection codec unit metadata is inconsistent");
}
/* The native collection owner uses positional private writes: another upload
 * may append concurrently on the same inode. Keep native fault injection at
 * this capability boundary, outside the shared unit codec. */
static int lc_cu_file_read(void *context, uint64_t offset,
                           const struct iovec *parts, int count,
                           lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  struct iovec pending[2];
  int first = 0;
  size_t used;
  ssize_t n;
  if (count == 2 && lc_collection_unit_test_preadv) {
    memcpy(pending, parts, sizeof(pending));
    while (first < count) {
      if (!pending[first].iov_len) {
        ++first;
        continue;
      }
      n = lc_collection_unit_test_preadv((int)(intptr_t)context,
                                         pending + first, count - first,
                                         (off_t)offset);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        return lc_error_set(error, n == 0 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
                            0L, "failed to read collection unit prefix",
                            n < 0 ? strerror(errno) : NULL, NULL, "pouch");
      offset += (size_t)n;
      used = (size_t)n;
      while (first < count && used >= pending[first].iov_len) {
        used -= pending[first].iov_len;
        ++first;
      }
      if (first < count) {
        pending[first].iov_base =
            (unsigned char *)pending[first].iov_base + used;
        pending[first].iov_len -= used;
      }
    }
    return LC_OK;
  }
#endif
  return lc_pouch_record_io_read(
      lc_pouch_record_file_io((int)(intptr_t)context), offset, parts, count,
      error);
}

static int lc_cu_file_patch(void *context, uint64_t offset,
                            const struct iovec *parts, int count,
                            lc_error *error) {
  int i, fd = (int)(intptr_t)context;
  for (i = 0; i < count; ++i) {
    const unsigned char *p = parts[i].iov_base;
    size_t remaining = parts[i].iov_len;
    while (remaining) {
      ssize_t n;
#ifdef LOCKDC_TEST_BUILD
      if (count == 1 && parts[0].iov_len == 32U &&
          lc_collection_chunks_test_pwrite)
        n = lc_collection_chunks_test_pwrite(fd, p, remaining, (off_t)offset);
      else
#endif
        n = pwrite(fd, p, remaining, (off_t)offset);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to write collection codec unit",
                            n < 0 ? strerror(errno) : NULL, NULL, "pouch");
      p += (size_t)n;
      offset += (size_t)n;
      remaining -= (size_t)n;
    }
  }
  return LC_OK;
}

static int lc_cu_file_append(void *context, const struct iovec *parts,
                             int count, lc_error *e) {
  return lc_pouch_record_io_append(
      lc_pouch_record_file_io((int)(intptr_t)context), parts, count, e);
}
static int lc_cu_file_tail(void *context, uint64_t *tail, lc_error *e) {
  return lc_pouch_record_io_tail(
      lc_pouch_record_file_io((int)(intptr_t)context), tail, e);
}
static void lc_cu_file_discard(void *context, uint64_t at) {
  lc_pouch_record_io file = lc_pouch_record_file_io((int)(intptr_t)context);
  file.ops->discard(file.context, at);
}
lc_pouch_record_io lc_collection_unit_file_io(int fd) {
  static const lc_pouch_record_io_ops ops = {lc_cu_file_read, lc_cu_file_append,
                                             lc_cu_file_patch, lc_cu_file_tail,
                                             lc_cu_file_discard};
  lc_pouch_record_io io;
  io.ops = fd < 0 ? NULL : &ops;
  io.context = (void *)(intptr_t)fd;
  return io;
}

int lc_collection_unit_write_meta(lc_pouch_record_io output, lc_u64 offset,
                                  const lc_collection_unit *unit,
                                  lc_error *error) {
  unsigned char meta[LC_COLLECTION_UNIT_META];
  unsigned char prefix[32];
  lc_pouch_record_header header;
  struct iovec part;
  size_t desc = strlen(unit->descriptor), context = strlen(unit->context),
         etag = strlen(unit->etag);
  unsigned long crc;
  int rc;
  if (!unit->plain || unit->plain > LC_COLLECTION_UNIT_MAX || !unit->stored ||
      desc >= sizeof(unit->descriptor) || context >= sizeof(unit->context) ||
      (etag && etag != 64U) ||
      24U + desc + context + etag > LC_COLLECTION_UNIT_META - 8U)
    return lc_cu_corrupt(error);
  memset(meta, 0, sizeof(meta));
  lc_cu_put(meta, unit->plain);
  lc_cu_put(meta + 8U, unit->ordinal);
  lc_cu_short(meta + 16U, desc);
  lc_cu_short(meta + 18U, context);
  meta[22] = (unsigned char)etag;
  memcpy(meta + 24U, unit->descriptor, desc);
  memcpy(meta + 24U + desc, unit->context, context);
  memcpy(meta + 24U + desc + context, unit->etag, etag);
  crc = (unsigned long)crc32(0L, meta, sizeof(meta) - 8U);
  lc_cu_put(meta + sizeof(meta) - 8U, crc);
  memset(&header, 0, sizeof(header));
  header.type = LC_POUCH_COLLECTION_CHUNK;
  header.meta_len = sizeof(meta);
  header.payload_len = unit->stored;
  header.payload_crc = unit->crc;
  lc_pouch_record_header_encode(&header, prefix);
  /* The private metadata is complete before its final header is visible. A
   * failure never truncates another writer's shared canonical inode. */
  part.iov_base = meta;
  part.iov_len = sizeof(meta);
  if (offset > (lc_u64)LC_I64_MAX - sizeof(prefix))
    return lc_cu_corrupt(error);
  rc = lc_pouch_record_io_patch(output, offset + sizeof(prefix), &part, 1,
                                error);
  if (rc == LC_OK) {
    part.iov_base = prefix;
    part.iov_len = sizeof(prefix);
    rc = lc_pouch_record_io_patch(output, offset, &part, 1, error);
  }
  return rc;
}

static int lc_cu_decode(unsigned char *encoded, lc_u64 stored,
                        lc_collection_unit *out, lc_error *error) {
  static const unsigned char empty[LC_COLLECTION_UNIT_META] = {0};
  unsigned char *meta = encoded + 32U;
  lc_pouch_record_header header;
  size_t desc, context, etag, used;
  int rc = lc_pouch_record_header_decode(encoded, &header, error);
  if (rc != LC_OK)
    return rc;
  if (header.type != LC_POUCH_COLLECTION_CHUNK || header.flags ||
      header.key_len || header.meta_len != LC_COLLECTION_UNIT_META ||
      header.payload_len != stored)
    return lc_cu_corrupt(error);
  desc = lc_cu_length(meta + 16U);
  context = lc_cu_length(meta + 18U);
  etag = meta[22];
  used = 24U + desc + context + etag;
  if (desc >= sizeof(out->descriptor) || context >= sizeof(out->context) ||
      used > LC_COLLECTION_UNIT_META - 8U || meta[20] || meta[21] ||
      (etag && etag != 64U) || meta[23] ||
      lc_cu_get(meta + LC_COLLECTION_UNIT_META - 8U) !=
          (lc_u64)crc32(0L, meta, LC_COLLECTION_UNIT_META - 8U) ||
      memchr(meta + 24U, 0, desc + context + etag))
    return lc_cu_corrupt(error);
  if (memcmp(meta + used, empty, LC_COLLECTION_UNIT_META - 8U - used))
    return lc_cu_corrupt(error);
  memset(out, 0, sizeof(*out));
  out->plain = lc_cu_get(meta);
  out->ordinal = lc_cu_get(meta + 8U);
  out->stored = stored;
  out->crc = header.payload_crc;
  if (!out->plain || out->plain > LC_COLLECTION_UNIT_MAX || !stored)
    return lc_cu_corrupt(error);
  memcpy(out->descriptor, meta + 24U, desc);
  memcpy(out->context, meta + 24U + desc, context);
  memcpy(out->etag, meta + 24U + desc + context, etag);
  return LC_OK;
}

int lc_collection_unit_read(lc_pouch_record_io input, lc_u64 offset,
                            lc_u64 stored, lc_collection_unit *out,
                            lc_error *error) {
  unsigned char encoded[LC_COLLECTION_UNIT_HEADER];
  struct iovec part;
  int rc;
  part.iov_base = encoded;
  part.iov_len = sizeof(encoded);
  rc = lc_pouch_record_io_read(input, offset, &part, 1, error);
  return rc == LC_OK ? lc_cu_decode(encoded, stored, out, error) : rc;
}

/* Scatter the metadata and the consumer's first bounded input chunk in one
 * canonical read. The caller owns the prefix; no payload storage is retained.
 */
int lc_collection_unit_read_prefix(lc_pouch_record_io input, lc_u64 offset,
                                   lc_u64 stored, lc_collection_unit *out,
                                   void *prefix, size_t length,
                                   lc_error *error) {
  unsigned char encoded[LC_COLLECTION_UNIT_HEADER];
  struct iovec parts[2];
  int rc;
  if (length > stored || offset > (lc_u64)LC_I64_MAX ||
      stored > (lc_u64)LC_I64_MAX - offset ||
      LC_COLLECTION_UNIT_HEADER > (lc_u64)LC_I64_MAX - offset - stored)
    return lc_cu_corrupt(error);
  parts[0].iov_base = encoded;
  parts[0].iov_len = sizeof(encoded);
  parts[1].iov_base = prefix;
  parts[1].iov_len = length;
  rc = lc_pouch_record_io_read(input, offset, parts, 2, error);
  return rc == LC_OK ? lc_cu_decode(encoded, stored, out, error) : rc;
}

int lc_collection_unit_context(const char *base, lc_u64 ordinal, char out[256],
                               lc_error *error) {
  char digits[32];
  int n;
  if (!ordinal) {
    if (strlen(base) >= 256U)
      return lc_cu_corrupt(error);
    strcpy(out, base);
    return LC_OK;
  }
  if (lc_u64_format_base10(ordinal, digits, sizeof(digits)) < 0)
    return lc_cu_corrupt(error);
  n = snprintf(out, 256, "%s:%s", base, digits);
  return n >= 0 && n < 256 ? LC_OK : lc_cu_corrupt(error);
}

int lc_collection_unit_manifest(const lc_pouch_collection_record *record,
                                const char *collection, EVP_MD_CTX **hash,
                                char *out, unsigned char *proof,
                                lc_error *error) {
  static const unsigned char domain[] = "lc-collection-unit-manifest";
  static const char hex[] = "0123456789abcdef";
  const char *strings[4];
  unsigned char metadata[640], digest[32];
  unsigned int count = 0;
  size_t at = sizeof(domain) - 1U, length, i, j;
  EVP_MD_CTX *ctx = hash ? *hash : NULL;
  const EVP_MD *algorithm = NULL;
  int valid;
  if (!record->position || !record->published_at || !record->stream_extents)
    return lc_cu_corrupt(error);
  memcpy(metadata, domain, at);
  lc_cu_put(metadata + at, record->position);
  lc_cu_put(metadata + at + 8U, record->published_at);
  lc_cu_put(metadata + at + 16U, record->bytes);
  lc_cu_put(metadata + at + 24U, record->stream_extents);
  at += 32U;
  strings[0] = collection;
  strings[1] = record->append_id;
  strings[2] = record->context;
  strings[3] = record->etag;
  for (i = 0; i < 4U; ++i) {
    length = strlen(strings[i]);
    if (at > sizeof(metadata) - 8U || length > sizeof(metadata) - at - 8U)
      return lc_cu_corrupt(error);
    lc_cu_put(metadata + at, length);
    at += 8U;
    memcpy(metadata + at, strings[i], length);
    at += length;
  }
  if (!ctx) {
    ctx = EVP_MD_CTX_new();
    if (!ctx)
      return lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection manifest hash");
    algorithm = EVP_sha256();
    if (hash)
      *hash = ctx;
  }
  valid = EVP_DigestInit_ex(ctx, algorithm, NULL) == 1 &&
          EVP_DigestUpdate(ctx, metadata, at) == 1 &&
          EVP_DigestFinal_ex(ctx, digest, &count) == 1 &&
          count == sizeof(digest);
  if (!hash)
    EVP_MD_CTX_free(ctx);
  if (!valid)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to hash collection manifest");
  if (proof)
    memcpy(proof, digest, sizeof(digest));
  if (!out)
    return LC_OK;
  strcpy(out, "collection-manifest:");
  j = strlen(out);
  for (i = 0; i < sizeof(digest); ++i) {
    out[j++] = hex[digest[i] >> 4U];
    out[j++] = hex[digest[i] & 15U];
  }
  out[j] = 0;
  return LC_OK;
}

int lc_collection_unit_binding(const lc_pouch_collection_record *record,
                               const char *collection, EVP_MD_CTX **hash,
                               char out[256], lc_error *error) {
  return lc_collection_unit_manifest(record, collection, hash, out, NULL,
                                     error);
}

int lc_collection_unit_check_manifest(const lc_pouch_collection_record *record,
                                      lc_error *error) {
  char collection[LC_XID_STRING_SIZE];
  unsigned char proof[32];
  int rc;
  if (!record->stream_extents)
    return LC_OK;
  if (strlen(record->context) < 11U + LC_XID_STRING_SIZE ||
      strncmp(record->context, "collection:", 11U) ||
      record->context[11U + LC_XID_STRING_SIZE - 1U] != ':')
    return lc_cu_corrupt(error);
  memcpy(collection, record->context + 11U, LC_XID_STRING_SIZE - 1U);
  collection[LC_XID_STRING_SIZE - 1U] = 0;
  if (!lc_xid_is_valid(collection))
    return lc_cu_corrupt(error);
  rc =
      lc_collection_unit_manifest(record, collection, NULL, NULL, proof, error);
  if (rc != LC_OK)
    return rc;
  return memcmp(record->manifest, proof, sizeof(proof)) ? lc_cu_corrupt(error)
                                                        : LC_OK;
}
