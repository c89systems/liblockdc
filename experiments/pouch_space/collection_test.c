#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "collection_segment.h"
#include "lc_api_internal.h"
#include "lc_collection_catalog_record.h"
#include "lc_collection_chunks.h"
#include "lc_collection_ids.h"
#include "lc_collection_order.h"
#include "lc_collection_unit.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_storage.h"
#include "root.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1U, __ATOMIC_RELAXED);                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static const char leaf[] = "seg-00000000000000000001.log";
static const char copy_leaf[] = "seg-00000000000000000002.log";
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "collection %d: %s\n", rc, e->message ? e->message : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static unsigned char content_at(uint64_t at, uint64_t size) {
  static const char begin[] = "{\"blob\":\"";
  static const char alphabet[] =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
  uint64_t n;
  if (at < sizeof(begin) - 1U)
    return (unsigned char)begin[at];
  if (at >= size - 2U)
    return at == size - 2U ? '"' : '}';
  n = at + UINT64_C(0x9e3779b97f4a7c15);
  n = (n ^ (n >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
  n = (n ^ (n >> 27U)) * UINT64_C(0x94d049bb133111eb);
  return (unsigned char)alphabet[(n ^ (n >> 31U)) & 63U];
}
typedef struct generated {
  lc_source source;
  lc_pouch_record_io output;
  uint64_t size, at, start;
  int streamed;
} generated;
static size_t generate(lc_source *source, void *bytes, size_t capacity,
                       lc_error *e) {
  generated *g = source->impl;
  unsigned char *p = bytes;
  uint64_t tail;
  if (capacity > 1024U)
    capacity = 1024U;
  if ((uint64_t)capacity > g->size - g->at)
    capacity = (size_t)(g->size - g->at);
  if (g->at >= g->size / 2U && g->at < g->size) {
    ok(lc_pouch_record_io_tail(g->output, &tail, e), e);
    if (tail > g->start + 4096U)
      g->streamed = 1;
  }
  for (size_t i = 0; i < capacity; ++i)
    p[i] = content_at(g->at + i, g->size);
  g->at += capacity;
  return capacity;
}
static void body(lc_pouch *pouch, lc_pouch_record_io input,
                 const lc_pouch_collection_record *record, lc_error *e) {
  lc_pouch_record_span span;
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *decoded;
  unsigned char bytes[2048];
  unsigned char prefix[37];
  struct iovec part = {prefix, sizeof(prefix)};
  uint64_t offset = 0U;
  ok(lc_pouch_record_io_read(input, record->payload_offset, &part, 1, e), e);
  ok(lc_pouch_record_span_bind(&span, input, record->payload_offset,
                               record->stored_bytes, prefix, sizeof(prefix), e),
     e);
  decoded = &span.source;
  if (record->descriptor[0]) {
    ok(lc_pouch_crypto_reader_open(pouch->crypto, &reader, e), e);
    ok(lc_pouch_crypto_reader_bind_source(
           reader, record->context, record->context, &span.source,
           record->stored_bytes, record->descriptor, &decoded, e),
       e);
  }
  for (int pass = 0; pass < 2; ++pass) {
    if (pass) {
      /* The borrowed first chunk can be reused after consumption. A reset
       * must read canonical bytes rather than this overwritten scratch. */
      memset(prefix, 0xa5, sizeof(prefix));
      ok(decoded->reset(decoded, e), e);
      offset = 0U;
    }
    size_t n;
    while ((n = decoded->read(decoded, bytes, sizeof(bytes), e)) != 0U) {
      REQUIRE(offset + n <= record->bytes);
      for (size_t i = 0; i < n; ++i)
        REQUIRE(bytes[i] == content_at(offset + i, record->bytes));
      offset += n;
    }
    ok(e->code, e);
    REQUIRE(offset == record->bytes);
  }
  lc_pouch_crypto_reader_close(reader);
}
static int recover(lc_pouch_record_io input, uint64_t checkpoint,
                   uint64_t capacity, uint64_t *accepted, void *context,
                   lc_error *e) {
  lc_pouch_collection_record record;
  unsigned char prefix[32], empty[32] = {0};
  struct iovec part = {prefix, sizeof(prefix)};
  uint64_t at = checkpoint;
  size_t *calls = context;
  ++*calls;
  while (capacity - at >= sizeof(prefix)) {
    int rc = lc_pouch_record_io_read(input, at, &part, 1, e);
    if (rc != LC_OK)
      return rc;
    if (!memcmp(prefix, empty, sizeof(prefix)))
      break;
    rc = lc_pouch_collection_record_read(input, at, capacity, leaf, &record, e);
    if (rc == LC_ERR_PENDING) {
      clear(e);
      break;
    }
    if (rc != LC_OK)
      return rc;
    REQUIRE(record.next_offset > at);
    at = record.next_offset;
  }
  *accepted = at;
  return LC_OK;
}
static void canonical(const char *directory, const char *key, int codec,
                      int durable) {
  char path[1024], id[LC_XID_STRING_SIZE];
  ps_root_options options = {0};
  ps_root *root = NULL;
  lc_pouch_open_options logical = {0};
  lc_pouch *pouch = NULL;
  ps_collection_segment *segment = NULL, *destination = NULL;
  ps_key segment_key = {71, 1, PS_STREAM}, copy_key = {71, 2, PS_STREAM};
  lc_pouch_collection_record written, metadata, copied, deleted, loaded;
  lc_pouch_record_io input, output;
  lc_collection_segment commit;
  ps_owned_status status;
  generated producer = {0};
  unsigned char *scratch = malloc(PS_CHUNK);
  uint64_t end, next;
  size_t recovery_calls = 0U;
  lc_error e;
  REQUIRE(scratch);
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/canonical-%d-%d", directory, codec,
                   durable) > 0);
  options.storage.durable_sync = durable;
  options.storage.initial_capacity = 10U * PS_MIB;
  options.crypto.key_string = codec & 1 ? key : NULL;
  options.crypto.compression_enabled = (codec & 2) != 0;
  options.compression_explicit = 1;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  logical.background_compaction_enabled_set = 1;
  logical.query_indexing_enabled_set = 1;
  logical.durable_sync = durable;
  logical.compression = codec & 2 ? "zlib" : "none";
  ok(lc_pouch_runtime_open(path, NULL, &logical, &pouch, &e), &e);
  pouch->crypto =
      ps_root_crypto(root); /* Fixture borrows the real root codec. */
  ok(lc_xid_new(id, &e), &e);
  ok(ps_collection_segment_open(ps_root_space(root), segment_key, 1, NULL,
                                recover, &recovery_calls, &segment, &e),
     &e);
  input = ps_collection_segment_io(segment);
  producer.source.read = generate;
  producer.source.impl = &producer;
  producer.output = input;
  producer.size = 768U * 1024U;
  ok(lc_pouch_collection_record_append(pouch, input, leaf, id, "upload", 7, 1,
                                       0, 0, 0, &producer.source, &written, &e),
     &e);
  REQUIRE(producer.at == producer.size && producer.streamed);
  ps_collection_segment_bind(segment, &commit);
  ok(lc_collection_segment_ready(&commit, &e), &e);
  /* A later producer can enter before this caller's durability wait. Its
   * pending bytes must not advance the completed outcome's captured tail. */
  ok(lc_pouch_record_write_prefix(input, LC_POUCH_COLLECTION_EDIT_BODY, "later",
                                  5, NULL, 0, 0, 0,
                                  LC_POUCH_RECORD_FLAG_PENDING, &e),
     &e);
  ok(lc_collection_segment_publish(&commit, pouch, &e), &e);
  ok(ps_collection_segment_status(segment, &status, &end, &e), &e);
  REQUIRE(status.tail == written.next_offset && end > status.tail);
  ok(lc_collection_segment_truncate(&commit, written.next_offset, &e), &e);
  lc_collection_segment_close(&commit);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  ok(lc_pouch_record_io_tail(input, &end, &e), &e);
  ok(lc_pouch_collection_record_read(input, 0, end, leaf, &loaded, &e), &e);
  REQUIRE(loaded.position == 7 && loaded.generation == 1 &&
          !strcmp(loaded.etag, written.etag));
  body(pouch, input, &loaded, &e);
  /* A complete canonical update after a checkpoint can survive before its
   * physical tail hint. The following private PENDING append cannot. */
  ok(lc_pouch_collection_record_metadata(pouch, input, leaf, &written, 1, 2, 0,
                                         &metadata, &e),
     &e);
  ok(lc_pouch_record_io_tail(input, &next, &e), &e);
  ok(lc_pouch_record_write_prefix(input, LC_POUCH_COLLECTION_EDIT_BODY, "next",
                                  4, NULL, 0, 0, 0,
                                  LC_POUCH_RECORD_FLAG_PENDING, &e),
     &e);
  ok(ps_collection_segment_sync(segment, &e), &e);
  ps_collection_segment_close(segment);
  segment = NULL;
  ok(ps_root_close(root, &e), &e);
  root = NULL;
  pouch->crypto = NULL;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  pouch->crypto = ps_root_crypto(root);
  ok(ps_collection_segment_open(ps_root_space(root), segment_key, 0, NULL,
                                recover, &recovery_calls, &segment, &e),
     &e);
  REQUIRE(recovery_calls == 1U);
  input = ps_collection_segment_io(segment);
  ok(lc_pouch_record_io_tail(input, &end, &e), &e);
  REQUIRE(end == next && metadata.record_offset == written.next_offset);
  ok(lc_pouch_collection_record_read(input, metadata.record_offset, end, leaf,
                                     &loaded, &e),
     &e);
  REQUIRE(loaded.metadata_only && loaded.position == 7 &&
          loaded.generation == 2 && !strcmp(loaded.etag, written.etag));
  ok(ps_collection_segment_open(ps_root_space(root), copy_key, 1, NULL, NULL,
                                NULL, &destination, &e),
     &e);
  output = ps_collection_segment_io(destination);
  ok(lc_pouch_collection_record_copy(pouch, input, input, leaf, &metadata,
                                     output, copy_leaf, scratch, PS_CHUNK,
                                     &copied, &e),
     &e);
  REQUIRE(copied.position == 7 && copied.generation == 2 &&
          !copied.metadata_only && !strcmp(copied.etag, written.etag));
  body(pouch, output, &copied, &e);
  ok(lc_pouch_collection_record_delete(pouch, output, &copied, 3, &deleted, &e),
     &e);
  ok(ps_collection_segment_publish(destination, PS_COMMIT_DEFAULT, &e), &e);
  ok(lc_pouch_record_io_tail(output, &end, &e), &e);
  ok(lc_pouch_collection_record_read(output, deleted.record_offset, end,
                                     copy_leaf, &loaded, &e),
     &e);
  REQUIRE(loaded.deleted && loaded.position == 7 && loaded.generation == 3 &&
          !strcmp(loaded.etag, written.etag));
  REQUIRE(ps_collection_segment_truncate(destination, 0U, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  ps_collection_segment_close(destination);
  ps_collection_segment_close(segment);
  pouch->crypto = NULL;
  lc_pouch_close(pouch);
  ok(ps_root_close(root, &e), &e);
  lc_error_cleanup(&e);
  free(scratch);
}
static int scratch_create(void *context, lc_error *e) {
  size_t *calls = context;
  int fd;
  ++*calls;
  fd = memfd_create("collection-derived", MFD_CLOEXEC);
  if (fd < 0)
    lc_error_set(e, LC_ERR_TRANSPORT, 0, "cannot create projection scratch",
                 NULL, NULL, "test");
  return fd;
}
typedef struct private_upload {
  lc_collection_segment storage;
  lc_collection_unit unit;
  pthread_barrier_t *paused, *resume;
  uint64_t offset, plain_offset, total;
} private_upload;
static void rendezvous(pthread_barrier_t *barrier) {
  int rc = pthread_barrier_wait(barrier);
  REQUIRE(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD);
}
static void *upload_private(void *context) {
  private_upload *upload = context;
  unsigned char bytes[2048];
  lc_error e;
  lc_error_init(&e);
  upload->unit.crc = (unsigned long)crc32(0L, Z_NULL, 0);
  for (uint64_t at = 0U; at < upload->unit.stored; at += sizeof(bytes)) {
    if (at == upload->unit.stored / 2U) {
      rendezvous(upload->paused);
      rendezvous(upload->resume);
    }
    for (size_t i = 0; i < sizeof(bytes); ++i)
      bytes[i] = content_at(upload->plain_offset + at + i, upload->total);
    upload->unit.crc =
        (unsigned long)crc32(upload->unit.crc, bytes, sizeof(bytes));
    ok(lc_collection_segment_patch(
           &upload->storage, upload->offset + LC_COLLECTION_UNIT_HEADER + at,
           bytes, sizeof(bytes), &e),
       &e);
  }
  ok(lc_collection_unit_write_meta(upload->storage.unit_io, upload->offset,
                                   &upload->unit, &e),
     &e);
  lc_collection_segment_close(&upload->storage);
  lc_error_cleanup(&e);
  return NULL;
}
typedef struct segment_inputs {
  ps_collection_segment *data, *publication;
  size_t opened, closed;
} segment_inputs;
static int input_open(void *context, lc_u64 id, lc_pouch_record_io *input,
                      void **pin, lc_error *e) {
  segment_inputs *inputs = context;
  ps_collection_segment *segment = id == 1 ? inputs->data : inputs->publication;
  (void)e;
  REQUIRE(id == 1 || id == 2);
  ps_collection_segment_retain(segment);
  *input = ps_collection_segment_io(segment);
  *pin = segment;
  ++inputs->opened;
  return LC_OK;
}
static void input_close(void *context, void *pin) {
  segment_inputs *inputs = context;
  ++inputs->closed;
  ps_collection_segment_close(pin);
}
static void tuple_put(unsigned char *p, uint64_t n) {
  for (size_t i = 0; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (i * 8U));
}
static void indexed(const char *directory, int durable) {
  char path[1024], id[LC_XID_STRING_SIZE], binding[256];
  ps_root_options options = {0};
  ps_root *root = NULL;
  lc_pouch_open_options logical = {0};
  lc_pouch *pouch = NULL;
  ps_collection_segment *data = NULL, *publication = NULL;
  ps_key data_key = {91, 1, PS_STREAM}, publication_key = {91, 2, PS_STREAM};
  lc_pouch_record_io data_io, publication_io, unit_input;
  lc_collection_segment data_storage, publication_storage;
  lc_collection_input captured = {0};
  lc_pouch_collection_record record, other;
  lc_pouch_collection_writer *writer = NULL;
  lc_collection_chunk_reader *reader = calloc(1, sizeof(*reader));
  lc_collection_chunk_ref index, ref;
  lc_collection_unit unit;
  segment_inputs inputs = {0};
  lc_collection_read_storage access = {input_open, input_close, &inputs, NULL};
  generated producer = {0};
  private_upload uploads[2] = {0};
  pthread_t threads[2];
  pthread_barrier_t paused, resume;
  lc_pouch_record_header header = {0};
  unsigned char prefix[32], tuples[2U * LC_COLLECTION_CHUNK_TUPLE];
  unsigned char bytes[2048];
  struct iovec part = {prefix, sizeof(prefix)};
  uint64_t at, total = 128U * 1024U,
               stride = 65536U + LC_COLLECTION_UNIT_HEADER;
  int found;
  lc_error e;
  REQUIRE(reader);
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/indexed-%d", directory, durable) >
          0);
  options.storage.initial_capacity = 10U * PS_MIB;
  options.storage.durable_sync = durable;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  logical.background_compaction_enabled_set = 1;
  logical.query_indexing_enabled_set = 1;
  logical.compression = "none";
  ok(lc_pouch_runtime_open(path, NULL, &logical, &pouch, &e), &e);
  pouch->crypto = ps_root_crypto(root);
  ok(lc_xid_new(id, &e), &e);
  ok(ps_collection_segment_open(ps_root_space(root), data_key, 1, NULL, NULL,
                                NULL, &data, &e),
     &e);
  ok(ps_collection_segment_open(ps_root_space(root), publication_key, 1, NULL,
                                NULL, NULL, &publication, &e),
     &e);
  data_io = ps_collection_segment_io(data);
  publication_io = ps_collection_segment_io(publication);
  ps_collection_segment_bind(data, &data_storage);
  ps_collection_segment_bind(publication, &publication_storage);
  ok(lc_collection_segment_reserve(&data_storage, 32U + 2U * stride, &e), &e);
  header.type = LC_POUCH_COLLECTION_REGION;
  header.payload_len = 2U * stride;
  lc_pouch_record_header_encode(&header, prefix);
  ok(lc_pouch_record_io_patch(data_io, 0U, &part, 1, &e), &e);
  ok(ps_collection_segment_publish(data, PS_COMMIT_DEFAULT, &e), &e);
  /* Hash and validate the generated document with the production JSON writer.
   * Its actual private unit bytes are produced by independent workers below. */
  REQUIRE(snprintf(binding, sizeof(binding), "collection:%s:upload:upload",
                   id) > 0);
  ok(lc_pouch_collection_writer_open_sink(pouch, NULL, id, "upload", binding, 0,
                                          0, 0, &writer, &e),
     &e);
  producer.source.read = generate;
  producer.source.impl = &producer;
  producer.size = total;
  producer.output = data_io;
  ok(lc_pouch_collection_writer_consume(writer, pouch, &producer.source,
                                        &record, &e),
     &e);
  REQUIRE(!pthread_barrier_init(&paused, NULL, 3U));
  REQUIRE(!pthread_barrier_init(&resume, NULL, 3U));
  for (size_t i = 0; i < 2U; ++i) {
    ok(lc_collection_segment_clone(&data_storage, &uploads[i].storage, &e), &e);
    uploads[i].offset = 32U + i * stride;
    uploads[i].plain_offset = i * 65536U;
    uploads[i].total = total;
    uploads[i].paused = &paused;
    uploads[i].resume = &resume;
    uploads[i].unit.plain = uploads[i].unit.stored = 65536U;
    uploads[i].unit.ordinal = i;
    ok(lc_collection_unit_context(binding, i, uploads[i].unit.context, &e), &e);
    if (!i)
      strcpy(uploads[i].unit.etag, record.etag);
    REQUIRE(!pthread_create(&threads[i], NULL, upload_private, &uploads[i]));
  }
  rendezvous(&paused);
  /* Sealing allocation metadata cannot wait for these private payloads.
   * A peer's completed canonical record remains independently publishable. */
  ok(ps_collection_segment_seal(data, &e), &e);
  producer.at = 0U;
  producer.output = publication_io;
  ok(lc_pouch_collection_record_append(pouch, publication_io, copy_leaf, id,
                                       "peer", 1, 1, 0, 0, 0, &producer.source,
                                       &other, &e),
     &e);
  ok(ps_collection_segment_publish(publication, PS_COMMIT_DEFAULT, &e), &e);
  rendezvous(&resume);
  for (size_t i = 0; i < 2U; ++i)
    REQUIRE(!pthread_join(threads[i], NULL));
  REQUIRE(!pthread_barrier_destroy(&paused));
  REQUIRE(!pthread_barrier_destroy(&resume));
  ok(lc_collection_segment_sync_private(&data_storage, &e), &e);
  for (size_t i = 0; i < 2U; ++i) {
    tuple_put(tuples + i * LC_COLLECTION_CHUNK_TUPLE, 1U);
    tuple_put(tuples + i * LC_COLLECTION_CHUNK_TUPLE + 8U, uploads[i].offset);
    tuple_put(tuples + i * LC_COLLECTION_CHUNK_TUPLE + 16U, 65536U);
  }
  ok(lc_collection_chunks_write_index(publication_io, 2, 0, tuples,
                                      sizeof(tuples), &index, &e),
     &e);
  record.position = 7;
  record.published_at = record.generation = 2;
  record.stream_extents = 2;
  record.stored_bytes = total;
  ok(lc_collection_unit_manifest(&record, id, NULL, binding, record.manifest,
                                 &e),
     &e);
  ok(lc_pouch_collection_writer_storage(writer, &record, &e), &e);
  ok(lc_pouch_collection_writer_publish_stream(
         writer, publication_io, copy_leaf, 2, index.offset, 2, index.bytes, 1,
         7, 2, &record, &e),
     &e);
  lc_pouch_collection_writer_close(writer);
  ok(ps_collection_segment_publish(publication, PS_COMMIT_DEFAULT, &e), &e);
  inputs.data = data;
  inputs.publication = publication;
  ok(lc_collection_input_open(access, 1, &captured, &e), &e);
  REQUIRE(captured.segment == 1 && captured.end == 32U + 2U * stride);
  lc_collection_input_advise(&captured, 0);
  lc_collection_input_close(&captured);
  ok(lc_collection_chunks_open(reader, &pouch->allocator, pouch->crypto, id,
                               access, publication_io, &record, &e),
     &e);
  for (int pass = 0; pass < 2; ++pass) {
    at = 0U;
    size_t n;
    while (
        (n = reader->source.read(&reader->source, bytes, sizeof(bytes), &e))) {
      REQUIRE(at + n <= total);
      for (size_t i = 0; i < n; ++i)
        REQUIRE(bytes[i] == content_at(at + i, total));
      at += n;
    }
    ok(e.code, &e);
    REQUIRE(at == total);
    ok(reader->source.reset(&reader->source, &e), &e);
  }
  for (size_t i = 0; i < 2U; ++i) {
    ok(lc_collection_chunks_next_unit(reader, &found, &unit_input, &ref, &unit,
                                      &e),
       &e);
    REQUIRE(found && unit.ordinal == i && ref.segment == 1 &&
            ref.offset == uploads[i].offset && unit.stored == 65536U);
  }
  ok(lc_collection_chunks_next_unit(reader, &found, &unit_input, &ref, &unit,
                                    &e),
     &e);
  REQUIRE(!found);
  lc_collection_chunks_reader_close(reader);
  REQUIRE(inputs.opened > 0U && inputs.opened == inputs.closed);
  free(reader);
  lc_collection_segment_close(&data_storage);
  lc_collection_segment_close(&publication_storage);
  ps_collection_segment_close(publication);
  ps_collection_segment_close(data);
  pouch->crypto = NULL;
  lc_pouch_close(pouch);
  ok(ps_root_close(root, &e), &e);
  lc_error_cleanup(&e);
}
static int id_match(void *context, const lc_collection_id_ref *ref, int *match,
                    lc_error *e) {
  (void)e;
  *match = ref->position == *(lc_u64 *)context;
  return LC_OK;
}
static void projections(void) {
  lc_collection_order order;
  lc_collection_ids ids;
  lc_collection_id_ref ref = {0}, found;
  lc_collection_order_row row = {0};
  size_t calls = 0;
  int matched;
  lc_error e;
  lc_error_init(&e);
  ok(lc_collection_order_adopt(scratch_create(&calls, &e), 1, &order, &e), &e);
  ok(lc_collection_ids_scratch_open(scratch_create, &calls, NULL, &ids, &e),
     &e);
  for (lc_u64 n = 1U; n <= 257U; ++n) {
    row.position = row.published_at = n;
    row.segment = 1;
    row.record_offset = n * 64U;
    row.bytes = 9;
    ok(lc_collection_order_append(&order, &row, 1, n * 64U + 64U, &e), &e);
    memset(ref.digest, 0, sizeof(ref.digest));
    memcpy(ref.digest, &n, sizeof(n));
    ref.position = ref.generation = n;
    ref.segment = 1;
    ref.offset = row.record_offset;
    ok(lc_collection_ids_put(&ids, &ref, &e), &e);
    ok(lc_collection_ids_checkpoint(&ids, n, n, 1, n * 64U + 64U, &e), &e);
  }
  REQUIRE(calls >= 6U && ids.capacity >= 1024U && !ids.path);
  ok(lc_collection_ids_refresh(&ids, &e), &e);
  ok(lc_collection_order_refresh(&order, &e), &e);
  REQUIRE(order.count == 257U && order.upper_position == 257U);
  for (lc_u64 n = 1U; n <= 257U; ++n) {
    memset(ref.digest, 0, sizeof(ref.digest));
    memcpy(ref.digest, &n, sizeof(n));
    ok(lc_collection_ids_find(&ids, ref.digest, id_match, &n, &found, &matched,
                              &e),
       &e);
    REQUIRE(matched && found.position == n && found.generation == n);
  }
  lc_collection_ids_close(&ids);
  lc_collection_order_close(&order);
  lc_error_cleanup(&e);
}
#include "collection_codec_cases.h"
typedef struct unit_run {
  const char *directory, *key;
  int faults;
} unit_run;
static void *unit_worker(void *context) {
  unit_run *run = context;
  if (run->faults == 2)
    unit_concurrency(run->directory, run->key);
  else
    unit_cases(run->directory, run->key, run->faults);
  return NULL;
}
int main(int argc, char **argv) {
  char path[1024], directory[1024];
  char *key = NULL;
  lc_error e;
  REQUIRE(argc == 2 || argc == 3);
  REQUIRE(snprintf(path, sizeof(path), "%s/pouch-collections-XXXXXX", argv[1]) >
          0);
  REQUIRE(lc_test_tmp_mkdtemp(path, directory, sizeof(directory), argv[1]));
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  if (argc == 3) {
    REQUIRE(!strcmp(argv[2], "units") || !strcmp(argv[2], "unit-faults") ||
            !strcmp(argv[2], "unit-concurrency"));
    unit_run run = {directory, key,
                    !strcmp(argv[2], "unit-concurrency")
                        ? 2
                        : !strcmp(argv[2], "unit-faults")};
    pthread_t worker;
    REQUIRE(!pthread_create(&worker, NULL, unit_worker, &run));
    REQUIRE(!pthread_join(worker, NULL));
  } else {
    projections();
    for (int durable = 0; durable < 2; ++durable)
      indexed(directory, durable);
    for (int codec = 0; codec < 4; ++codec)
      for (int durable = 0; durable < 2; ++durable)
        canonical(directory, key, codec, durable);
  }
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, argv[1]);
  printf("collection storage checks=%zu\n", checks);
  return 0;
}
