#include "lc_collection_payload.h"
#include "lc_pouch_crypto.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static size_t lc_cp_source_read(lc_source *source, void *bytes, size_t capacity,
                                lc_error *error) {
  size_t got;
  return lc_collection_payload_read((lc_collection_payload *)source, bytes,
                                    capacity, &got, error) == LC_OK
             ? got
             : 0U;
}

static size_t lc_cp_encoded_read(lc_source *source, void *bytes,
                                 size_t capacity, lc_error *error) {
  lc_collection_payload *payload = source->impl;
  size_t take = payload->prefix_size - payload->prefix_used;
  lc_u64 remaining;
  ssize_t n;
  if (take > capacity)
    take = capacity;
  if (take) {
    memmove(bytes, payload->prefix + payload->prefix_used, take);
    payload->prefix_used += take;
    payload->offset += take;
    return take;
  }
  remaining =
      payload->record->stored_bytes - (payload->offset - payload->start);
  if ((lc_u64)capacity > remaining)
    capacity = (size_t)remaining;
  if (!capacity)
    return 0U;
  do {
    n = pread(payload->fd, bytes, capacity, (off_t)payload->offset);
  } while (n < 0 && errno == EINTR);
  if (n <= 0) {
    lc_collection_failure(error, n == 0 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
                          "collection_corrupt",
                          "collection body is truncated or unreadable");
    return 0U;
  }
  payload->offset += (size_t)n;
  return (size_t)n;
}

static int lc_cp_encoded_reset(lc_source *source, lc_error *error) {
  lc_collection_payload *payload = source->impl;
  (void)error;
  /* Initial codec binding can reset without consuming input. After output
   * has reused the borrowed chunk, replay must read the canonical fd. */
  if (payload->prefix_used) {
    payload->prefix_size = payload->prefix_used = 0U;
    payload->prefix = NULL;
  }
  payload->offset = payload->start;
  return LC_OK;
}

void lc_collection_payload_reset(lc_collection_payload *payload) {
  lc_pouch_crypto_reader_clear(payload->decoder);
  payload->decoded = NULL;
  payload->record = NULL;
  payload->fd = -1;
  payload->prefix = NULL;
  payload->prefix_size = payload->prefix_used = 0U;
}

void lc_collection_payload_cleanup(lc_collection_payload *payload) {
  if (payload == NULL)
    return;
  lc_collection_payload_reset(payload);
  lc_pouch_crypto_reader_close(payload->decoder);
  if (payload->body_fd_initialized && payload->body_fd >= 0)
    (void)close(payload->body_fd);
  lc_collection_chunks_reader_close(&payload->chunks);
  EVP_MD_CTX_free(payload->hash);
  memset(payload, 0, sizeof(*payload));
}

static int lc_cp_open(lc_collection_payload *payload,
                      lc_collection_handle *collection, int fd,
                      const lc_pouch_collection_record *record,
                      const lc_collection_unit *unit, const void *prefix,
                      size_t prefix_size, lc_error *error) {
  char position[32];
  char context[256];
  char upload_context[256];
  unsigned char proof[32];
  const char *descriptor = record->descriptor;
  int n;
  int rc, indexed = 0;
  const EVP_MD *digest;
  lc_collection_payload_reset(payload);
  if (record->metadata_only) {
    if (!payload->body_fd_initialized) {
      payload->body_fd = -1;
      payload->body_fd_initialized = 1;
    }
    rc = lc_collection_record_body(collection, record, &payload->body_fd,
                                   &payload->body_segment, &payload->body_end,
                                   error);
    if (rc != LC_OK)
      return rc;
    fd = payload->body_fd;
  }
  payload->source.read = lc_cp_source_read;
  payload->start = payload->offset = record->payload_offset;
  if (unit)
    payload->start = payload->offset =
        record->stream_offset + LC_COLLECTION_UNIT_HEADER;
  payload->bytes = 0U;
  payload->record = record;
  payload->fd = fd;
  if (lc_u64_format_base10(record->position, position, sizeof(position)) < 0)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection position cannot be formatted");
  n = snprintf(context, sizeof(context), "collection:%s:%s:%s", collection->id,
               record->append_id, position);
  if (n < 0 || (size_t)n >= sizeof(context))
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection identity context exceeds its bound");
  if (strcmp(record->context, context) != 0) {
    n = snprintf(upload_context, sizeof(upload_context),
                 "collection:%s:%s:", collection->id, record->append_id);
    if (n < 0 || (size_t)n >= sizeof(upload_context) ||
        strncmp(record->context, upload_context, (size_t)n) ||
        (strcmp(record->context + n, "upload") &&
         !lc_xid_is_valid(record->context + n)))
      return lc_collection_failure(
          error, LC_ERR_PROTOCOL, "collection_corrupt",
          "collection body has a different identity binding");
  }
  digest = NULL;
  if (payload->hash == NULL) {
    payload->hash = EVP_MD_CTX_new();
    digest = EVP_sha256();
  }
  if (payload->hash == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection body hash context");
  /* Reinitializing this read's context with NULL retains its selected SHA-256
   * implementation; no provider lookup is needed for every next entry. */
  if (EVP_DigestInit_ex(payload->hash, digest, NULL) != 1)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to initialize collection body hash");
  if (unit) {
    rc = lc_collection_unit_manifest(record, collection->id,
                                     &payload->chunks.manifest, context, proof,
                                     error);
    if (rc != LC_OK)
      return rc;
    if (memcmp(proof, record->manifest, sizeof(proof)))
      return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                   "collection unit has a different identity");
    rc = lc_pouch_crypto_descriptor_same_payload(record->descriptor,
                                                 unit->descriptor, error);
    if (rc != LC_OK)
      return rc;
    if (descriptor[0] || lc_pouch_crypto_enabled(collection->pouch->crypto)) {
      if (!payload->decoder)
        rc = lc_pouch_crypto_reader_open(collection->pouch->crypto,
                                         &payload->decoder, error);
      if (rc == LC_OK)
        rc = lc_pouch_crypto_reader_verify(payload->decoder, descriptor,
                                           context, error);
      if (rc != LC_OK)
        return rc;
    }
    descriptor = unit->descriptor;
    strcpy(context, unit->context);
  } else if (record->stream_extents) {
    rc = lc_collection_chunks_open(&payload->chunks, &collection->allocator,
                                   collection->pouch->crypto, collection->id,
                                   collection->directory, fd, record, error);
    if (rc != LC_OK)
      return rc;
    strcpy(context, payload->chunks.binding);
    if (record->stream_extents == 1) {
      rc = lc_collection_chunks_single(&payload->chunks, &fd, &payload->start,
                                       error);
      if (rc != LC_OK)
        return rc;
      payload->fd = fd;
      payload->offset = payload->start;
      descriptor = payload->chunks.unit.descriptor;
      strcpy(context, payload->chunks.unit.context);
    } else {
      indexed = 1;
      payload->decoded = &payload->chunks.source;
    }
  }
  if (indexed)
    return LC_OK;
  if (prefix_size) {
    payload->prefix = prefix;
    payload->prefix_size = prefix_size;
    payload->encoded.impl = payload;
    payload->encoded.read = lc_cp_encoded_read;
    payload->encoded.reset = lc_cp_encoded_reset;
    payload->decoded = &payload->encoded;
  }
  if (descriptor[0] != '\0' ||
      lc_pouch_crypto_enabled(collection->pouch->crypto)) {
    if (payload->decoder == NULL) {
      rc = lc_pouch_crypto_reader_open(collection->pouch->crypto,
                                       &payload->decoder, error);
      if (rc != LC_OK)
        return rc;
    }
    if (prefix_size)
      return lc_pouch_crypto_reader_bind_source(
          payload->decoder, record->context, context, &payload->encoded,
          record->stored_bytes, descriptor, &payload->decoded, error);
    return lc_pouch_crypto_reader_bind(
        payload->decoder, record->context, context, fd, payload->start,
        record->stored_bytes, descriptor, &payload->decoded, error);
  }
  return LC_OK;
}

int lc_collection_payload_open(lc_collection_payload *payload,
                               lc_collection_handle *collection, int fd,
                               const lc_pouch_collection_record *record,
                               lc_error *error) {
  return lc_cp_open(payload, collection, fd, record, NULL, NULL, 0U, error);
}

int lc_collection_payload_open_prefix(lc_collection_payload *payload,
                                      lc_collection_handle *collection, int fd,
                                      const lc_pouch_collection_record *record,
                                      const lc_collection_unit *unit,
                                      const void *prefix, size_t prefix_size,
                                      lc_error *error) {
  return lc_cp_open(payload, collection, fd, record, unit, prefix, prefix_size,
                    error);
}

int lc_collection_payload_read(lc_collection_payload *payload, void *bytes,
                               size_t capacity, size_t *got, lc_error *error) {
  const lc_pouch_collection_record *record;
  lc_u64 remaining;
  ssize_t n;
  *got = 0U;
  record = payload->record;
  if (record == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection body source is not open");
  if (capacity == 0U)
    return LC_OK;
  if (payload->decoded != NULL) {
    *got = payload->decoded->read(payload->decoded, bytes, capacity, error);
    if (error != NULL && error->code != LC_OK)
      return error->code;
  } else {
    remaining = record->stored_bytes - (payload->offset - payload->start);
    if ((lc_u64)capacity > remaining)
      capacity = (size_t)remaining;
    if (capacity == 0U)
      return LC_OK;
    do {
      n = pread(payload->fd, bytes, capacity, (off_t)payload->offset);
    } while (n < 0 && errno == EINTR);
    if (n <= 0)
      return lc_collection_failure(
          error, n == 0 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
          "collection_corrupt", "collection body is truncated or unreadable");
    *got = (size_t)n;
    payload->offset += (lc_u64)*got;
  }
  if (*got > capacity || (lc_u64)*got > record->bytes - payload->bytes)
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection body exceeds its committed length");
  payload->bytes += (lc_u64)*got;
  if (*got != 0U && EVP_DigestUpdate(payload->hash, bytes, *got) != 1)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to hash collection body");
  return LC_OK;
}

int lc_collection_payload_finish(lc_collection_payload *payload,
                                 lc_error *error) {
  unsigned char digest[32];
  char etag[65];
  static const char hex[] = "0123456789abcdef";
  unsigned int length;
  size_t i;
  length = 0U;
  if (payload->record == NULL || payload->hash == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection body source is not open");
  if (EVP_DigestFinal_ex(payload->hash, digest, &length) != 1 ||
      length != sizeof(digest))
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to finalize collection body hash");
  for (i = 0U; i < sizeof(digest); ++i) {
    etag[i * 2U] = hex[digest[i] >> 4U];
    etag[i * 2U + 1U] = hex[digest[i] & 15U];
  }
  etag[64] = '\0';
  if (payload->bytes != payload->record->bytes ||
      strcmp(etag, payload->record->etag) != 0)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection body length or fingerprint "
                                 "differs from the committed entry");
  return LC_OK;
}
