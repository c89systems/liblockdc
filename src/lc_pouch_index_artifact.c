#define _POSIX_C_SOURCE 200809L
#include "lc_pouch_index_artifact.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch_index_store.h"
#include "lc_pouch_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define INDEX_FOOTER_BYTES 16U
static const char *const leaves[] = {
    "query.manifest",     "query.index",        "query.index.lcpseg",
    "query.index.lcpdtg", "query.index.lcpttg", "query.index.lcppg",
    "query.index.lcprg",  "query.index.lcptxg", "query.index.lcpt3g",
    "query.index.lcptdg", "query.index.lcpdel"};
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL, NULL, "pouch");
}
static const lc_pouch_index_store_ops *store(lc_pouch *pouch, void **context) {
  *context = pouch->storage.ops ? pouch->storage.context : (void *)pouch;
  if (!pouch->storage.ops)
    return &lc_pouch_index_file_ops;
  return pouch->storage.ops->query_indexes
             ? pouch->storage.ops->query_indexes(pouch->storage.context,
                                                 context)
             : NULL;
}
int lc_pouch_index_publication_acquire(lc_pouch *pouch, const char *ns,
                                       void **guard, lc_error *e) {
  const lc_pouch_index_store_ops *ops;
  void *context;
  if (guard)
    *guard = NULL;
  if (!pouch || !ns || !guard)
    return failure(e, LC_ERR_INVALID, "query publication requires namespace");
  ops = store(pouch, &context);
  if (!ops || !ops->acquire || !ops->release)
    return failure(e, LC_ERR_UNSUPPORTED,
                   "query publication owner unavailable");
  return ops->acquire(context, ns, guard, e);
}
void lc_pouch_index_publication_release(lc_pouch *pouch, void *guard) {
  const lc_pouch_index_store_ops *ops;
  void *context;
  if (!pouch || !guard)
    return;
  ops = store(pouch, &context);
  ops->release(context, guard);
}
int lc_pouch_index_artifact_kind(const char *leaf, unsigned int *kind) {
  unsigned int i;
  if (!leaf || !kind)
    return 0;
  for (i = 0U; i < LC_POUCH_INDEX_ARTIFACT_KINDS; ++i)
    if (!strcmp(leaf, leaves[i])) {
      *kind = i;
      return 1;
    }
  return 0;
}
int lc_pouch_index_artifact_leaf(const lc_pouch_index_artifact_id *id,
                                 char *out, size_t capacity, lc_error *e) {
  char number[32];
  int length, written;
  if (!id || !out || id->kind >= LC_POUCH_INDEX_ARTIFACT_KINDS ||
      (id->segmented && id->kind == LC_POUCH_INDEX_MANIFEST) ||
      (!id->segmented && id->generation))
    return failure(e, LC_ERR_INVALID, "invalid query-index artifact identity");
  if (!id->segmented) {
    if (strlen(leaves[id->kind]) >= capacity)
      return failure(e, LC_ERR_INVALID, "query-index identity output is short");
    memcpy(out, leaves[id->kind], strlen(leaves[id->kind]) + 1U);
    return LC_OK;
  }
  length = lc_u64_format_base10(id->generation, number, sizeof(number));
  if (length < 1 || length > 20 || capacity < 27U)
    return failure(e, LC_ERR_INVALID, "query-index generation exceeds bounds");
  memcpy(out, "query.", 6U);
  memset(out + 6U, '0', 20U - (size_t)length);
  memcpy(out + 26U - (size_t)length, number, (size_t)length);
  written = snprintf(out + 26U, capacity - 26U, ".%s", leaves[id->kind]);
  if (written < 0 || (size_t)written >= capacity - 26U)
    return failure(e, LC_ERR_INVALID, "query-index identity output is short");
  return LC_OK;
}
lc_pouch_index_artifact *
lc_pouch_index_artifact_new(lc_pouch *pouch, const char *ns,
                            const lc_pouch_index_artifact_id *id, lc_error *e) {
  lc_pouch_index_artifact *artifact;
  char leaf[96], *cursor;
  size_t ns_length, leaf_length, bytes;
  if (!pouch || !ns || !ns[0]) {
    failure(e, LC_ERR_INVALID, "query-index identity requires a namespace");
    return NULL;
  }
  if (lc_pouch_index_artifact_leaf(id, leaf, sizeof(leaf), e) != LC_OK)
    return NULL;
  ns_length = strlen(ns);
  leaf_length = strlen(leaf);
  if (ns_length > ((size_t)-1 - sizeof(*artifact) - 128U) / 2U ||
      leaf_length >
          ((size_t)-1 - sizeof(*artifact) - 128U - 2U * ns_length) / 2U) {
    failure(e, LC_ERR_NOMEM, "query-index identity exceeds local bounds");
    return NULL;
  }
  bytes = sizeof(*artifact) + 2U * ns_length + 2U * leaf_length + 128U;
  artifact = lc_alloc_with_allocator(&pouch->allocator, bytes);
  if (!artifact) {
    failure(e, LC_ERR_NOMEM, "cannot allocate query-index identity");
    return NULL;
  }
  artifact->pouch = pouch;
  artifact->id = *id;
  cursor = (char *)(artifact + 1);
  artifact->ns = cursor;
  memcpy(cursor, ns, ns_length + 1U);
  cursor += ns_length + 1U;
  artifact->leaf = cursor;
  memcpy(cursor, leaf, leaf_length + 1U);
  cursor += leaf_length + 1U;
  artifact->key = cursor;
  (void)snprintf(cursor, bytes - (size_t)(cursor - (char *)artifact),
                 "%lu:%s/%s", (unsigned long)ns_length, ns, leaf);
  return artifact;
}
int lc_pouch_index_artifact_signature(const lc_pouch_index_artifact *artifact,
                                      lc_pouch_index_signature *out,
                                      lc_error *e) {
  void *context;
  const lc_pouch_index_store_ops *ops;
  if (!artifact || !out)
    return failure(e, LC_ERR_INVALID, "query-index signature requires output");
  memset(out, 0, sizeof(*out));
  ops = store(artifact->pouch, &context);
  if (!ops)
    return failure(e, LC_ERR_UNSUPPORTED, "query-index owner is unavailable");
  return ops->signature(context, artifact->ns, &artifact->id, out, e);
}
static int read_at(lc_pouch_record_io io, uint64_t offset, void *bytes,
                   size_t length, lc_error *e) {
  struct iovec part;
  part.iov_base = bytes;
  part.iov_len = length;
  return lc_pouch_record_io_read(io, offset, &part, 1, e);
}
static uint64_t get64(const unsigned char *p) {
  uint64_t value = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    value |= (uint64_t)p[i] << (8U * i);
  return value;
}
static void put64(unsigned char *p, uint64_t value) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    p[i] = (unsigned char)(value >> (8U * i));
}
static int footer_read(const lc_allocator *a, lc_pouch_record_io io,
                       uint64_t bytes, size_t maximum, char **descriptor,
                       uint64_t *cipher_bytes, lc_error *e) {
  unsigned char footer[INDEX_FOOTER_BYTES];
  uint64_t length;
  char *copy;
  int rc;
  *descriptor = NULL;
  *cipher_bytes = 0U;
  if (bytes < sizeof(footer))
    return failure(e, LC_ERR_INVALID, "query-index crypto footer is missing");
  rc = read_at(io, bytes - sizeof(footer), footer, sizeof(footer), e);
  if (rc != LC_OK)
    return rc;
  if (memcmp(footer, "LPQICRF1", 8U))
    return failure(e, LC_ERR_INVALID, "query-index crypto footer is invalid");
  length = get64(footer + 8U);
  if (!length || length > bytes - sizeof(footer) ||
      length > (uint64_t)((size_t)-1) - 1U || (maximum && length > maximum))
    return failure(e, LC_ERR_INVALID,
                   "query-index descriptor length is invalid");
  copy = lc_alloc_with_allocator(a, (size_t)length + 1U);
  if (!copy)
    return failure(e, LC_ERR_NOMEM, "cannot allocate query-index descriptor");
  *cipher_bytes = bytes - sizeof(footer) - length;
  rc = read_at(io, *cipher_bytes, copy, (size_t)length, e);
  if (rc == LC_OK) {
    copy[length] = '\0';
    *descriptor = copy;
  } else
    lc_free_with_allocator(a, copy);
  return rc;
}
static int write_bytes(lc_sink *sink, const void *bytes, size_t length,
                       lc_error *e) {
  if (!length || sink->write(sink, bytes, length, e))
    return LC_OK;
  return e && e->code != LC_OK ? e->code
                               : failure(e, LC_ERR_TRANSPORT,
                                         "query-index output made no progress");
}
static int footer_write(lc_sink *sink, const char *descriptor, lc_error *e) {
  unsigned char footer[INDEX_FOOTER_BYTES];
  size_t length;
  int rc;
  if (!descriptor || !descriptor[0])
    return failure(e, LC_ERR_INVALID, "query-index descriptor is missing");
  length = strlen(descriptor);
  memcpy(footer, "LPQICRF1", 8U);
  put64(footer + 8U, (uint64_t)length);
  rc = write_bytes(sink, descriptor, length, e);
  return rc == LC_OK ? write_bytes(sink, footer, sizeof(footer), e) : rc;
}
static char *binding_context(const lc_pouch_index_artifact *artifact,
                             lc_error *e) {
  size_t bytes =
      strlen(artifact->ns) + strlen(artifact->leaf) + sizeof("query-index//");
  char *context = lc_alloc_with_allocator(&artifact->pouch->allocator, bytes);
  if (context)
    (void)snprintf(context, bytes, "query-index/%s/%s", artifact->ns,
                   artifact->leaf);
  else
    failure(e, LC_ERR_NOMEM, "cannot allocate query-index codec identity");
  return context;
}
static int source_bytes(const lc_allocator *a, lc_source *source,
                        size_t maximum, char **out, size_t *length,
                        lc_error *e) {
  size_t size = 0U, capacity = 0U, got;
  size_t ceiling = maximum ? maximum : (size_t)-1 - 2U;
  char *bytes = NULL, *grown;
  int rc = LC_OK;
  for (;;) {
    if (capacity - size <= 1U) {
      size_t next = capacity ? capacity * 2U : 4096U;
      if (next < capacity || next > ceiling + 2U)
        next = ceiling + 2U;
      if (next <= capacity) {
        rc = failure(e, LC_ERR_PROTOCOL,
                     "query-index metadata exceeds its decoded bound");
        break;
      }
      grown = lc_realloc_with_allocator(a, bytes, next);
      if (!grown) {
        rc = failure(e, LC_ERR_NOMEM, "cannot read query-index metadata");
        break;
      }
      bytes = grown;
      capacity = next;
    }
    got = source->read(source, bytes + size, capacity - size - 1U, e);
    if ((e && e->code != LC_OK) || got > capacity - size - 1U ||
        got > ceiling - size) {
      rc = e && e->code != LC_OK
               ? e->code
               : failure(e, LC_ERR_PROTOCOL,
                         "query-index decoder exceeded output");
      break;
    }
    size += got;
    if (!got)
      break;
  }
  if (rc == LC_OK) {
    bytes[size] = '\0';
    *out = bytes;
    *length = size;
  } else
    lc_free_with_allocator(a, bytes);
  return rc;
}
int lc_pouch_index_metadata_decode(
    lc_pouch *pouch, const lc_pouch_index_input *input, const char *binding,
    int encrypted, uint64_t stored_maximum, size_t decoded_maximum,
    size_t descriptor_maximum, char **out, size_t *length, lc_error *e) {
  lc_pouch_record_span span;
  lc_source *source = NULL;
  lc_pouch_crypto_reader *decoder = NULL;
  char *descriptor = NULL, *bytes = NULL;
  uint64_t cipher_bytes = 0U;
  int rc = LC_OK;
  *out = NULL;
  *length = 0U;
  if (stored_maximum && input->bytes > stored_maximum)
    return failure(e, LC_ERR_PROTOCOL, "query-index stored span exceeds bound");
  if (encrypted) {
    rc = footer_read(&pouch->allocator, input->io, input->bytes,
                     descriptor_maximum, &descriptor, &cipher_bytes, e);
    if (rc == LC_OK)
      rc = lc_pouch_record_span_bind(&span, input->io, 0U, cipher_bytes, NULL,
                                     0U, e);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_reader_open(pouch->crypto, &decoder, e);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_reader_bind_source(decoder, binding, binding,
                                              &span.source, cipher_bytes,
                                              descriptor, &source, e);
    if (rc == LC_OK)
      rc = source_bytes(&pouch->allocator, source, decoded_maximum, out, length,
                        e);
  } else {
    if (decoded_maximum && input->bytes > decoded_maximum)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "query-index metadata exceeds its decoded bound");
    if (rc == LC_OK && input->bytes > (uint64_t)((size_t)-1) - 1U)
      rc =
          failure(e, LC_ERR_NOMEM, "query-index metadata exceeds local bounds");
    if (rc == LC_OK) {
      bytes =
          lc_alloc_with_allocator(&pouch->allocator, (size_t)input->bytes + 1U);
      if (!bytes)
        rc = failure(e, LC_ERR_NOMEM, "cannot allocate query-index metadata");
    }
    if (rc == LC_OK && input->bytes)
      rc = read_at(input->io, 0U, bytes, (size_t)input->bytes, e);
    if (rc == LC_OK) {
      bytes[input->bytes] = '\0';
      *out = bytes;
      *length = (size_t)input->bytes;
      bytes = NULL;
    }
  }
  lc_pouch_crypto_reader_close(decoder);
  lc_free_with_allocator(&pouch->allocator, bytes);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  return rc;
}
int lc_pouch_index_artifact_load(const lc_pouch_index_artifact *artifact,
                                 int strict, char **out, size_t *length,
                                 int *present, int *valid, lc_error *e) {
  lc_pouch_index_input input;
  const lc_pouch_index_store_ops *ops;
  void *context;
  char *binding = NULL;
  int rc, encrypted;
  if (!artifact || !out || !length || !present || !valid)
    return failure(e, LC_ERR_INVALID, "query-index load requires outputs");
  *out = NULL;
  *length = 0U;
  *present = *valid = 0;
  memset(&input, 0, sizeof(input));
  ops = store(artifact->pouch, &context);
  if (!ops)
    return failure(e, LC_ERR_UNSUPPORTED, "query-index owner is unavailable");
  rc = ops->open(context, artifact->ns, &artifact->id, &input, e);
  if (rc != LC_OK || !input.signature.present)
    return rc;
  *present = 1;
  encrypted = artifact->id.kind != LC_POUCH_INDEX_MANIFEST &&
              lc_pouch_crypto_enabled(artifact->pouch->crypto);
  if (encrypted) {
    binding = binding_context(artifact, e);
    if (!binding)
      rc = LC_ERR_NOMEM;
  }
  if (rc == LC_OK)
    rc = lc_pouch_index_metadata_decode(artifact->pouch, &input, binding,
                                        encrypted, 0U, 0U, 0U, out, length, e);
  ops->close(context, &input);
  lc_free_with_allocator(&artifact->pouch->allocator, binding);
  if (rc == LC_OK)
    *valid = 1;
  else if (!strict && (rc == LC_ERR_INVALID || rc == LC_ERR_PROTOCOL)) {
    lc_error_cleanup(e);
    rc = LC_OK;
  }
  return rc;
}
typedef struct artifact_producer {
  const lc_pouch_index_artifact *artifact;
  const char *bytes, *binding;
  size_t length;
  int encrypted;
} artifact_producer;
int lc_pouch_index_metadata_bound(lc_pouch *pouch, size_t length, int encrypted,
                                  uint64_t *out, lc_error *e) {
  size_t descriptor_bytes = 0U;
  uint64_t bound = (uint64_t)length;
  if (encrypted) {
    bound = lc_pouch_crypto_stream_bound(pouch->crypto, length, 0,
                                         &descriptor_bytes);
    if (bound > LC_U64_MAX - descriptor_bytes - INDEX_FOOTER_BYTES)
      return failure(e, LC_ERR_NOMEM, "query-index encoded bound is too large");
    bound += descriptor_bytes + INDEX_FOOTER_BYTES;
  }
  *out = bound;
  return LC_OK;
}
int lc_pouch_index_metadata_encode(lc_pouch *pouch, const char *binding,
                                   const char *bytes, size_t length,
                                   int encrypted, lc_sink *sink, lc_error *e) {
  lc_source *source = NULL;
  char *descriptor = NULL;
  uint64_t plain, stored;
  unsigned long crc;
  int rc;
  if (!encrypted)
    return write_bytes(sink, bytes, length, e);
  rc = lc_source_from_memory(bytes, length, &source, e);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_stream_to_sink(pouch->crypto, binding, sink, source,
                                        &plain, &stored, &crc, &descriptor, 0,
                                        e);
  lc_source_close(source);
  if (rc == LC_OK && plain != length)
    rc =
        failure(e, LC_ERR_PROTOCOL, "query-index encoder consumed short input");
  if (rc == LC_OK)
    rc = footer_write(sink, descriptor, e);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  return rc;
}
static int produce(void *context, lc_sink *sink, lc_error *e) {
  artifact_producer *p = context;
  return lc_pouch_index_metadata_encode(p->artifact->pouch, p->binding,
                                        p->bytes, p->length, p->encrypted, sink,
                                        e);
}
static int producer_init(const lc_pouch_index_artifact *artifact,
                         const char *bytes, size_t length, artifact_producer *p,
                         char **binding, uint64_t *bound, lc_error *e) {
  int rc;
  *binding = NULL;
  if (!artifact || (!bytes && length))
    return failure(e, LC_ERR_INVALID, "query-index replacement requires bytes");
  memset(p, 0, sizeof(*p));
  p->artifact = artifact;
  p->bytes = bytes ? bytes : "";
  p->length = length;
  p->encrypted = artifact->id.kind != LC_POUCH_INDEX_MANIFEST &&
                 lc_pouch_crypto_enabled(artifact->pouch->crypto);
  rc = lc_pouch_index_metadata_bound(artifact->pouch, length, p->encrypted,
                                     bound, e);
  if (rc == LC_OK && p->encrypted) {
    *binding = binding_context(artifact, e);
    if (!*binding)
      return LC_ERR_NOMEM;
    p->binding = *binding;
  }
  return rc;
}
int lc_pouch_index_artifact_write(const lc_pouch_index_artifact *artifact,
                                  const char *bytes, size_t length,
                                  int unpublished, int durable, lc_error *e) {
  artifact_producer p;
  const lc_pouch_index_store_ops *ops;
  void *context;
  char *binding = NULL;
  uint64_t bound = 0U;
  int rc;
  if (!artifact || (!bytes && length))
    return failure(e, LC_ERR_INVALID, "query-index replacement requires bytes");
  ops = store(artifact->pouch, &context);
  if (!ops)
    return failure(e, LC_ERR_UNSUPPORTED, "query-index owner is unavailable");
  rc = producer_init(artifact, bytes, length, &p, &binding, &bound, e);
  if (rc == LC_OK)
    rc = ops->replace(context, artifact->ns, &artifact->id, bound, produce, &p,
                      unpublished, durable, e);
  lc_free_with_allocator(&artifact->pouch->allocator, binding);
  return rc;
}
struct lc_pouch_index_revision {
  const lc_pouch_index_artifact *generation;
  const lc_pouch_index_store_ops *ops;
  void *context, *build;
  int finished;
};
void lc_pouch_index_revision_dispose(lc_pouch_index_revision *revision) {
  if (!revision)
    return;
  revision->ops->dispose(revision->context, revision->build);
  lc_free_with_allocator(&revision->generation->pouch->allocator, revision);
}
int lc_pouch_index_revision_prepare(const lc_pouch_index_artifact *generation,
                                    const char *bytes, size_t length,
                                    int unpublished, uint64_t manifest_bound,
                                    lc_pouch_index_revision **out,
                                    lc_pouch_index_signature *signature,
                                    lc_error *e) {
  lc_pouch_index_revision *revision;
  const lc_pouch_index_store_ops *ops;
  artifact_producer p;
  void *context;
  char *binding = NULL;
  uint64_t bound = 0;
  int rc;
  if (out)
    *out = NULL;
  if (!out || !signature || !generation ||
      generation->id.kind != LC_POUCH_INDEX_PACKED)
    return failure(e, LC_ERR_INVALID, "revision requires a packed generation");
  memset(signature, 0, sizeof(*signature));
  ops = store(generation->pouch, &context);
  if (!ops || !ops->prepare || !ops->publish || !ops->dispose)
    return failure(e, LC_ERR_UNSUPPORTED, "query revision owner unavailable");
  revision = lc_calloc_with_allocator(&generation->pouch->allocator, 1,
                                      sizeof(*revision));
  if (!revision)
    return failure(e, LC_ERR_NOMEM, "cannot allocate query-index revision");
  revision->generation = generation;
  revision->ops = ops;
  revision->context = context;
  rc = producer_init(generation, bytes, length, &p, &binding, &bound, e);
  if (rc == LC_OK)
    rc = ops->prepare(context, generation->ns, &generation->id, bound, produce,
                      &p, unpublished, manifest_bound, &revision->build,
                      signature, e);
  lc_free_with_allocator(&generation->pouch->allocator, binding);
  if (rc == LC_OK)
    *out = revision;
  else
    lc_pouch_index_revision_dispose(revision);
  return rc;
}
int lc_pouch_index_revision_publish(lc_pouch_index_revision *revision,
                                    const lc_pouch_index_artifact *manifest,
                                    const char *bytes, size_t length,
                                    lc_error *e) {
  artifact_producer p;
  char *binding = NULL;
  uint64_t bound = 0;
  int rc;
  if (!revision || revision->finished || !manifest ||
      manifest->pouch != revision->generation->pouch ||
      strcmp(manifest->ns, revision->generation->ns) ||
      manifest->id.kind != LC_POUCH_INDEX_MANIFEST)
    return failure(e, LC_ERR_INVALID,
                   "revision requires its namespace manifest");
  revision->finished = 1;
  rc = producer_init(manifest, bytes, length, &p, &binding, &bound, e);
  if (rc == LC_OK)
    rc =
        revision->ops->publish(revision->context, revision->build, manifest->ns,
                               &manifest->id, bound, produce, &p, e);
  lc_free_with_allocator(&manifest->pouch->allocator, binding);
  return rc;
}
int lc_pouch_index_artifact_remove(const lc_pouch_index_artifact *artifact,
                                   lc_error *e) {
  const lc_pouch_index_store_ops *ops;
  void *context;
  if (!artifact)
    return failure(e, LC_ERR_INVALID, "query-index removal requires identity");
  ops = store(artifact->pouch, &context);
  return ops ? ops->remove(context, artifact->ns, &artifact->id, e)
             : failure(e, LC_ERR_UNSUPPORTED,
                       "query-index owner is unavailable");
}
int lc_pouch_index_artifacts_prune(lc_pouch *pouch, const char *ns,
                                   const uint64_t *keep, size_t count,
                                   lc_error *e) {
  const lc_pouch_index_store_ops *ops;
  void *context;
  if (!pouch || !ns || (!keep && count))
    return failure(e, LC_ERR_INVALID,
                   "query-index pruning requires identities");
  ops = store(pouch, &context);
  return ops ? ops->prune(context, ns, keep, count, e)
             : failure(e, LC_ERR_UNSUPPORTED,
                       "query-index owner is unavailable");
}
static int file_sink_write(lc_sink *sink, const void *bytes, size_t length,
                           lc_error *e) {
  struct iovec part;
  part.iov_base = (void *)bytes;
  part.iov_len = length;
  return lc_pouch_record_io_append(*(lc_pouch_record_io *)sink->impl, &part, 1,
                                   e) == LC_OK;
}
int lc_pouch_query_index_append_crypto_footer_fd(int fd, const char *descriptor,
                                                 lc_error *e) {
  lc_pouch_record_io io = lc_pouch_record_file_io(fd);
  lc_sink sink;
  memset(&sink, 0, sizeof(sink));
  sink.write = file_sink_write;
  sink.impl = &io;
  return fd < 0 ? failure(e, LC_ERR_INVALID, "query-index footer requires fd")
                : footer_write(&sink, descriptor, e);
}
int lc_pouch_query_index_read_crypto_footer(const lc_allocator *a,
                                            const char *path, size_t maximum,
                                            char **descriptor,
                                            uint64_t *cipher_bytes,
                                            lc_error *e) {
  struct stat st;
  int fd, rc;
  if (!path || !descriptor || !cipher_bytes)
    return failure(e, LC_ERR_INVALID, "query-index footer requires outputs");
  fd = open(path, O_RDONLY);
  if (fd < 0)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "cannot open query-index crypto footer",
                        strerror(errno), NULL, "pouch");
  rc = fstat(fd, &st) == 0 && st.st_size >= 0
           ? footer_read(a, lc_pouch_record_file_io(fd), (uint64_t)st.st_size,
                         maximum, descriptor, cipher_bytes, e)
           : failure(e, LC_ERR_TRANSPORT, "cannot inspect query-index footer");
  (void)close(fd);
  return rc;
}
