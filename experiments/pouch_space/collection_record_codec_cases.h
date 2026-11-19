#ifndef POUCH_SPACE_COLLECTION_RECORD_CODEC_CASES_H
#define POUCH_SPACE_COLLECTION_RECORD_CODEC_CASES_H
#include "control.h"
#include "lc_collection_record_recode.h"

#define RECORD_PARTS 128U
typedef struct record_part {
  ps_collection_segment *owner;
  unit_access access;
  lc_u64 id;
  char leaf[64];
} record_part;
typedef struct record_fixture {
  ps_root *root;
  lc_pouch *pouch;
  lc_pouch_crypto *target;
  ps_root_options options;
  ps_collection_segment *source;
  record_part parts[RECORD_PARTS];
  size_t count;
  lc_u64 next_id;
  lc_collection_unit first;
  lc_collection_chunk_reader reader;
  lc_collection_chunks *seed;
  unit_watch watch;
  unit_access input;
  char path[1024], id[LC_XID_STRING_SIZE];
  lc_pouch_collection_record stream, meta[2], small, small_meta, edit_body,
      receipt, deleted;
} record_fixture;
static int record_source_read(void *context, uint64_t at,
                              const struct iovec *parts, int count,
                              lc_error *e) {
  unit_access *access = context;
  unit_watch *watch = access->watch;
  pthread_mutex_lock(&watch->mutex);
  if (watch->armed) {
    watch->armed = 0;
    watch->paused = 1;
    pthread_cond_broadcast(&watch->changed);
    while (!watch->released)
      pthread_cond_wait(&watch->changed, &watch->mutex);
  }
  pthread_mutex_unlock(&watch->mutex);
  return unit_read(context, at, parts, count, e);
}
static lc_pouch_record_io record_source_io(record_fixture *f) {
  static const lc_pouch_record_io_ops ops = {record_source_read, NULL, NULL,
                                             unit_tail, NULL};
  lc_pouch_record_io io = {&ops, &f->input};
  return io;
}
static int record_index(void *context, unsigned int level, const void *tuples,
                        size_t bytes, lc_collection_chunk_ref *ref,
                        lc_error *e) {
  record_fixture *f = context;
  return lc_collection_chunks_write_index(ps_collection_segment_io(f->source),
                                          1, level, tuples, bytes, ref, e);
}
static int record_output(void *context, lc_u64 bound, int index,
                         lc_pouch_record_io *out, lc_u64 *segment,
                         lc_error *e) {
  record_fixture *f = context;
  uint64_t tail = 0;
  REQUIRE(bound && bound < 10 * PS_MIB && (index == 0 || index == 1));
  if (f->count)
    ok(lc_pouch_record_io_tail(f->parts[f->count - 1].access.io, &tail, e), e);
  if (!f->count || tail >= 65536) {
    if (f->count) {
      int rc = ps_collection_segment_seal(f->parts[f->count - 1].owner, e);
      if (rc != LC_OK)
        return rc;
    }
    REQUIRE(f->count < RECORD_PARTS);
    record_part *part = &f->parts[f->count];
    part->id = f->next_id++;
    ps_key key = {73, part->id, PS_STREAM};
    int rc = ps_collection_segment_open(ps_root_space(f->root), key, 1, NULL,
                                        NULL, NULL, &part->owner, e);
    if (rc != LC_OK)
      return rc;
    ++f->count;
    part->access =
        (unit_access){ps_collection_segment_io(part->owner), &f->watch, 0};
    ok(lc_collection_record_leaf(part->id, part->leaf, e), e);
  }
  *segment = f->parts[f->count - 1].id;
  *out = unit_io(&f->parts[f->count - 1].access);
  return LC_OK;
}
static int record_open(void *context, lc_u64 id, lc_pouch_record_io *io,
                       void **pin, lc_error *e) {
  record_fixture *f = context;
  ps_collection_segment *owner = id == 1 ? f->source : NULL;
  for (size_t i = 0; !owner && i < f->count; ++i)
    if (f->parts[i].id == id)
      owner = f->parts[i].owner;
  if (!owner)
    return lc_error_set(e, LC_ERR_PROTOCOL, 0,
                        "fixture has no referenced segment", NULL, NULL,
                        "test");
  ps_collection_segment_retain(owner);
  *io = id == 1 ? record_source_io(f) : ps_collection_segment_io(owner);
  *pin = owner;
  return LC_OK;
}
static void record_close(void *context, void *pin) {
  (void)context;
  ps_collection_segment_close(pin);
}
static lc_collection_read_storage record_reads(record_fixture *f) {
  lc_collection_read_storage reads = {record_open, record_close, f, NULL};
  return reads;
}
static void record_seed_unit(record_fixture *f, lc_u64 ordinal, lc_u64 at,
                             lc_u64 bytes, lc_collection_chunk_ref *ref,
                             lc_error *e) {
  lc_pouch_record_io io = ps_collection_segment_io(f->source);
  lc_collection_unit unit = {0};
  lc_pouch_record_header header = {0};
  unsigned char prefix[LC_COLLECTION_UNIT_HEADER] = {0}, scratch[2048];
  lc_sink destination = {.write = unit_seed_write, .impl = &io};
  lc_pouch_crypto_writer *writer = NULL;
  lc_u64 start;
  ok(lc_pouch_record_io_tail(io, &start, e), e);
  ok(lc_collection_unit_context(f->stream.context, ordinal, unit.context, e),
     e);
  unit.ordinal = ordinal;
  if (!ordinal && !f->options.crypto.compression_enabled)
    strcpy(unit.etag, f->stream.etag);
  header.type = LC_POUCH_COLLECTION_CHUNK;
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  header.meta_len = LC_COLLECTION_UNIT_META;
  lc_pouch_record_header_encode(&header, prefix);
  struct iovec part = {prefix, sizeof(prefix)};
  ok(lc_pouch_record_io_append(io, &part, 1, e), e);
  ok(lc_pouch_crypto_writer_open_sink(f->pouch->crypto, unit.context,
                                      &destination, &writer, e),
     e);
  lc_sink *sink = lc_pouch_crypto_writer_sink(writer);
  for (lc_u64 copied = 0; copied < bytes;) {
    size_t n = bytes - copied < sizeof(scratch) ? (size_t)(bytes - copied)
                                                : sizeof(scratch);
    for (size_t i = 0; i < n; ++i)
      scratch[i] = content_at(at + copied + i, f->stream.bytes);
    REQUIRE(sink->write(sink, scratch, n, e));
    copied += n;
  }
  const char *descriptor = NULL;
  ok(lc_pouch_crypto_writer_finish_borrowed(writer, &unit.plain, &unit.stored,
                                            &unit.crc, &descriptor, e),
     e);
  if (descriptor) {
    REQUIRE(strlen(descriptor) < sizeof(unit.descriptor));
    strcpy(unit.descriptor, descriptor);
  }
  ok(lc_collection_unit_write_meta(io, start, &unit, e), e);
  if (!ordinal)
    f->first = unit;
  f->stream.stored_bytes += unit.stored;
  *ref = (lc_collection_chunk_ref){1, start, unit.stored};
  lc_pouch_crypto_writer_close(writer);
}
static void record_inline(record_fixture *f, const char *key, lc_u64 position,
                          lc_u64 generation, unsigned char kind,
                          lc_pouch_collection_record *out, lc_error *e) {
  lc_pouch_collection_writer *writer = NULL;
  lc_pouch_record_io io = ps_collection_segment_io(f->source);
  generated producer = {0};
  producer.source = (lc_source){0};
  producer.source.impl = &producer;
  producer.source.read = generate;
  producer.size = 4097;
  producer.output = io;
  ok(lc_pouch_collection_writer_open(f->pouch, io, leaf, f->id, key, position,
                                     generation, 12345, 1, 23456, &writer, e),
     e);
  lc_pouch_collection_writer_kind(writer, kind);
  ok(lc_pouch_collection_writer_consume(writer, f->pouch, &producer.source, out,
                                        e),
     e);
  ok(lc_pouch_collection_writer_publish(writer, leaf, position, generation, out,
                                        e),
     e);
  lc_pouch_collection_writer_close(writer);
}
static record_fixture *record_fixture_open_io(const char *directory,
                                              const char *key, int codec,
                                              int sync, size_t units,
                                              lc_u64 bytes, const ps_io *io,
                                              int sealed, lc_error *e) {
  record_fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  f->next_id = 1000;
  REQUIRE(!pthread_mutex_init(&f->watch.mutex, NULL));
  REQUIRE(!pthread_cond_init(&f->watch.changed, NULL));
  REQUIRE(snprintf(f->path, sizeof(f->path), "%s/record-codec", directory) > 0);
  ps_root_options options = {0};
  options.crypto.key_string = codec & 1 ? key : NULL;
  options.crypto.compression_enabled = !!(codec & 2);
  options.storage.durable_sync = sync;
  options.storage.io = io;
  options.storage.initial_capacity = options.storage.maximum_capacity =
      10 * PS_MIB;
  f->options = options;
  ok(ps_root_open(f->path, 1, &options, &f->root, e), e);
  lc_pouch_open_options logical = {0};
  logical.background_compaction_enabled_set =
      logical.query_indexing_enabled_set = 1;
  logical.compression = "none";
  ok(lc_pouch_runtime_open(f->path, NULL, &logical, &f->pouch, e), e);
  f->pouch->crypto = ps_root_crypto(f->root);
  ok(lc_xid_new(f->id, e), e);
  ps_key source = {73, 1, PS_STREAM};
  ok(ps_collection_segment_open(ps_root_space(f->root), source, 1, NULL, NULL,
                                NULL, &f->source, e),
     e);
  f->input = (unit_access){ps_collection_segment_io(f->source), &f->watch, 0};
  f->seed = calloc(1, sizeof(*f->seed));
  REQUIRE(f->seed);
  f->seed->append_index = record_index;
  f->seed->index_context = f;
  generated producer = {0};
  producer.source.impl = &producer;
  producer.source.read = generate;
  producer.size = bytes;
  producer.output = f->input.io;
  char context[256];
  REQUIRE(snprintf(context, sizeof(context), "collection:%s:stream:upload",
                   f->id) > 0);
  lc_pouch_collection_writer *writer = NULL;
  ok(lc_pouch_collection_writer_open_sink(f->pouch, NULL, f->id, "stream",
                                          context, 12345, 1, 23456, &writer, e),
     e);
  ok(lc_pouch_collection_writer_consume(writer, f->pouch, &producer.source,
                                        &f->stream, e),
     e);
  f->stream.stored_bytes = 0;
  lc_u64 offset = 0;
  for (size_t i = 0; i < units; ++i) {
    lc_u64 n = (bytes - offset) / (units - i);
    lc_collection_chunk_ref ref;
    record_seed_unit(f, i, offset, n, &ref, e);
    ok(lc_collection_chunks_link(NULL, f->seed, &ref, e), e);
    offset += n;
  }
  ok(lc_collection_chunks_finish(NULL, f->seed, e), e);
  lc_collection_chunk_ref root = f->seed->root;
  unsigned int height = f->seed->height;
  if (units > 1) {
    unsigned char tuple[LC_COLLECTION_CHUNK_TUPLE];
    tuple_put(tuple, root.segment);
    tuple_put(tuple + 8, root.offset);
    tuple_put(tuple + 16, root.bytes);
    ok(lc_collection_chunks_write_index(f->input.io, 1, height, tuple,
                                        sizeof(tuple), &root, e),
       e);
    ++height;
  }
  f->stream.position = 7;
  f->stream.published_at = f->stream.generation = 2;
  f->stream.stream_extents = units;
  ok(lc_collection_unit_manifest(&f->stream, f->id, NULL, context,
                                 f->stream.manifest, e),
     e);
  if (f->first.descriptor[0]) {
    char *descriptor = NULL;
    ok(lc_pouch_crypto_rebind_descriptor(f->pouch->crypto, f->first.descriptor,
                                         f->first.context, context, &descriptor,
                                         e),
       e);
    REQUIRE(strlen(descriptor) < sizeof(f->stream.descriptor));
    strcpy(f->stream.descriptor, descriptor);
    lc_free_with_allocator(&f->pouch->allocator, descriptor);
  }
  ok(lc_pouch_collection_writer_storage(writer, &f->stream, e), e);
  ok(lc_pouch_collection_writer_publish_stream(
         writer, f->input.io, leaf, root.segment, root.offset, units,
         root.bytes, height, 7, 2, &f->stream, e),
     e);
  lc_pouch_collection_writer_close(writer);
  ok(lc_pouch_collection_record_metadata(
         f->pouch, f->input.io, leaf, &f->stream, 1, 3, 34567, &f->meta[0], e),
     e);
  ok(lc_pouch_collection_record_metadata(f->pouch, f->input.io, leaf,
                                         &f->meta[0], 1, 4, 0, &f->meta[1], e),
     e);
  record_inline(f, "small", 8, 5, LC_POUCH_STATE_RECORD_STATE_PUT, &f->small,
                e);
  record_inline(f, "replacement", 9, 6, LC_POUCH_COLLECTION_EDIT_BODY,
                &f->edit_body, e);
  record_inline(f, "receipt", 9, 7, LC_POUCH_COLLECTION_EDIT, &f->receipt, e);
  ok(lc_pouch_collection_record_delete(f->pouch, f->input.io, &f->meta[1], 8,
                                       &f->deleted, e),
     e);
  ok(lc_pouch_collection_record_metadata(f->pouch, f->input.io, leaf, &f->small,
                                         1, 9, 45678, &f->small_meta, e),
     e);
  ok(sealed ? ps_collection_segment_seal(f->source, e)
            : ps_collection_segment_publish(f->source, PS_COMMIT_DEFAULT, e),
     e);
  lc_pouch_collection_record *records[] = {
      &f->stream,     &f->meta[0],   &f->meta[1], &f->small,
      &f->small_meta, &f->edit_body, &f->receipt, &f->deleted};
  for (size_t i = 0; i < sizeof(records) / sizeof(*records); ++i) {
    lc_u64 at = records[i]->record_offset, end = records[i]->next_offset;
    ok(lc_pouch_collection_record_read(f->input.io, at, end, leaf, records[i],
                                       e),
       e);
  }
  lc_pouch_crypto_open_options target = options.crypto;
  target.compression_enabled = !target.compression_enabled;
  ok(lc_pouch_crypto_open(NULL, &target, &f->target, NULL, e), e);
  ok(lc_collection_chunks_open(&f->reader, &f->pouch->allocator,
                               f->pouch->crypto, f->id, record_reads(f),
                               record_source_io(f), &f->stream, e),
     e);
  return f;
}
static record_fixture *record_fixture_open(const char *directory,
                                           const char *key, int codec, int sync,
                                           size_t units, lc_u64 bytes,
                                           lc_error *e) {
  return record_fixture_open_io(directory, key, codec, sync, units, bytes, NULL,
                                1, e);
}
static void record_fixture_close(record_fixture *f, lc_error *e) {
  lc_collection_chunks_reader_close(&f->reader);
  lc_collection_chunks_close(f->seed);
  free(f->seed);
  for (size_t i = 0; i < f->count; ++i)
    ps_collection_segment_close(f->parts[i].owner);
  ps_collection_segment_close(f->source);
  f->pouch->crypto = NULL;
  lc_pouch_close(f->pouch);
  lc_pouch_crypto_close(f->target);
  ok(ps_root_close(f->root, e), e);
  REQUIRE(!unlink(f->path));
  REQUIRE(!pthread_mutex_destroy(&f->watch.mutex));
  REQUIRE(!pthread_cond_destroy(&f->watch.changed));
  free(f);
}
static void record_reopen(record_fixture *f, lc_error *e) {
  lc_collection_chunks_reader_close(&f->reader);
  for (size_t i = 0; i < f->count; ++i)
    ps_collection_segment_close(f->parts[i].owner);
  ps_collection_segment_close(f->source);
  f->pouch->crypto = NULL;
  lc_pouch_close(f->pouch);
  ok(ps_root_close(f->root, e), e);
  ok(ps_root_open(f->path, 0, &f->options, &f->root, e), e);
  lc_pouch_open_options logical = {0};
  logical.background_compaction_enabled_set =
      logical.query_indexing_enabled_set = 1;
  logical.compression = "none";
  ok(lc_pouch_runtime_open(f->path, NULL, &logical, &f->pouch, e), e);
  f->pouch->crypto = ps_root_crypto(f->root);
  ps_key source = {73, 1, PS_STREAM};
  ok(ps_collection_segment_open(ps_root_space(f->root), source, 0, NULL, NULL,
                                NULL, &f->source, e),
     e);
  f->input.io = ps_collection_segment_io(f->source);
  for (size_t i = 0; i < f->count; ++i) {
    ps_key key = {73, f->parts[i].id, PS_STREAM};
    ok(ps_collection_segment_open(ps_root_space(f->root), key, 0, NULL, NULL,
                                  NULL, &f->parts[i].owner, e),
       e);
    f->parts[i].access.io = ps_collection_segment_io(f->parts[i].owner);
  }
}
static int record_encode(record_fixture *f,
                         const lc_pouch_collection_record *source,
                         const lc_collection_recode_links *links,
                         lc_pouch_collection_record *out, lc_u64 *segment,
                         lc_error *e) {
  lc_pouch_record_io io;
  int rc = record_output(f, source->bytes + 2048, 0, &io, segment, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_recode(
        f->pouch, f->target, f->id, record_source_io(f), leaf,
        source->record_offset, source->next_offset, links, io,
        f->parts[f->count - 1].leaf, out, e);
  return rc;
}
static void record_identity(const lc_pouch_collection_record *a,
                            const lc_pouch_collection_record *b) {
  REQUIRE(a->position == b->position && a->published_at == b->published_at &&
          a->generation == b->generation && a->bytes == b->bytes &&
          a->updated_at_unix == b->updated_at_unix &&
          a->retention == b->retention &&
          a->has_collection_retention == b->has_collection_retention &&
          a->collection_retention == b->collection_retention &&
          a->deleted == b->deleted && a->metadata_only == b->metadata_only &&
          a->kind == b->kind && a->stream_extents == b->stream_extents &&
          !memcmp(a->manifest, b->manifest, sizeof(a->manifest)) &&
          !strcmp(a->append_id, b->append_id) && !strcmp(a->etag, b->etag) &&
          !strcmp(a->context, b->context));
}
static void record_stream_check(record_fixture *f,
                                const lc_pouch_collection_record *record,
                                lc_u64 segment, lc_error *e) {
  lc_collection_chunk_reader *reader = calloc(1, sizeof(*reader));
  REQUIRE(reader);
  ps_collection_segment *owner = segment == 1 ? f->source : NULL;
  for (size_t i = 0; i < f->count; ++i)
    if (f->parts[i].id == segment)
      owner = f->parts[i].owner;
  REQUIRE(owner);
  ok(lc_collection_chunks_open(reader, &f->pouch->allocator,
                               segment == 1 ? f->pouch->crypto : f->target,
                               f->id, record_reads(f),
                               ps_collection_segment_io(owner), record, e),
     e);
  unsigned char bytes[2048];
  for (int pass = 0; pass < 2; ++pass) {
    lc_u64 at = 0;
    if (pass)
      ok(reader->source.reset(&reader->source, e), e);
    size_t got;
    while (
        (got = reader->source.read(&reader->source, bytes, sizeof(bytes), e))) {
      REQUIRE(got <= record->bytes - at);
      int same = 1;
      for (size_t i = 0; i < got; ++i)
        if (bytes[i] != content_at(at + i, record->bytes))
          same = 0;
      REQUIRE(same);
      at += got;
    }
    ok(e->code, e);
    REQUIRE(at == record->bytes);
  }
  lc_collection_chunks_reader_close(reader);
  free(reader);
}
typedef struct record_job {
  lc_collection_stream_recode *work;
  lc_collection_recode_stream stream;
  size_t visited;
  int finished, rc;
} record_job;
static void *record_step_thread(void *context) {
  record_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc = lc_collection_stream_recode_step(job->work, 1, &job->visited,
                                             &job->finished, &job->stream, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void record_paused_step(record_fixture *f,
                               lc_collection_stream_recode *work, lc_error *e) {
  record_job job = {.work = work, .stream = {.units = 999}};
  f->watch.armed = 1;
  pthread_t worker;
  REQUIRE(!pthread_create(&worker, NULL, record_step_thread, &job));
  pthread_mutex_lock(&f->watch.mutex);
  while (!f->watch.paused)
    pthread_cond_wait(&f->watch.changed, &f->watch.mutex);
  pthread_mutex_unlock(&f->watch.mutex);
  ps_collection_segment *peer = NULL;
  ps_key key = {74, 1, PS_STREAM};
  ok(ps_collection_segment_open(ps_root_space(f->root), key, 1, NULL, NULL,
                                NULL, &peer, e),
     e);
  char bytes[] = "independent writer";
  struct iovec part = {bytes, sizeof(bytes)};
  ok(lc_pouch_record_io_append(ps_collection_segment_io(peer), &part, 1, e), e);
  ok(ps_collection_segment_seal(peer, e), e);
  ps_collection_segment_close(peer);
  pthread_mutex_lock(&f->watch.mutex);
  f->watch.released = 1;
  pthread_cond_broadcast(&f->watch.changed);
  pthread_mutex_unlock(&f->watch.mutex);
  REQUIRE(!pthread_join(worker, NULL));
  ok(job.rc, e);
  REQUIRE(job.visited == 1 && !job.finished && job.stream.units == 999);
}
static void record_abort(record_fixture *, lc_error *);
static void record_reject(record_fixture *f,
                          const lc_pouch_collection_record *source,
                          const lc_collection_recode_links *links, int expected,
                          lc_error *e) {
  lc_pouch_record_io output;
  lc_u64 segment;
  uint64_t before, after;
  ok(record_output(f, 2048, 0, &output, &segment, e), e);
  ok(lc_pouch_record_io_tail(output, &before, e), e);
  lc_pouch_collection_record untouched = {.generation = 999};
  REQUIRE(lc_pouch_collection_record_recode(
              f->pouch, f->target, f->id, record_source_io(f), leaf,
              source->record_offset, source->next_offset, links, output,
              f->parts[f->count - 1].leaf, &untouched, e) == expected);
  REQUIRE(untouched.generation == 999);
  clear(e);
  ok(lc_pouch_record_io_tail(output, &after, e), e);
  REQUIRE(after == before);
}
static void record_stream_guards(record_fixture *f,
                                 const lc_collection_recode_stream *stream,
                                 lc_error *e) {
  for (int mutation = 0; mutation < 5; ++mutation) {
    lc_collection_recode_stream bad = *stream;
    if (mutation == 0)
      ++bad.units;
    else if (mutation == 1)
      bad.first.etag[0] = bad.first.etag[0] == 'a' ? 'b' : 'a';
    else if (mutation == 2)
      bad.first.context[0] ^= 1;
    else if (mutation == 3)
      bad.height = 0;
    else
      strcpy(bad.first.descriptor, f->first.descriptor);
    lc_collection_recode_links links = {.stream = &bad};
    record_reject(f, &f->stream, &links,
                  mutation == 4 ? LC_ERR_INVALID : LC_ERR_PROTOCOL, e);
  }
}
static void record_alias_guards(record_fixture *f,
                                const lc_pouch_collection_record *body,
                                lc_u64 segment, lc_error *e) {
  for (int mutation = 0; mutation < 4; ++mutation) {
    lc_pouch_collection_record bad = *body;
    lc_collection_recode_links links = {.body = &bad,
                                        .previous_segment = segment,
                                        .previous_offset = body->record_offset};
    if (mutation == 0)
      ++bad.position;
    else if (mutation == 1)
      bad.generation = f->meta[0].generation + 1;
    else if (mutation == 2)
      links.previous_segment = 0;
    else
      strcpy(bad.descriptor, f->stream.descriptor);
    record_reject(f, &f->meta[0], &links,
                  mutation == 3 ? LC_ERR_INVALID : LC_ERR_PROTOCOL, e);
  }
}
static void record_lifecycle(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync)
      for (size_t count = 1; count <= 66; count = count == 1   ? 3
                                                  : count == 3 ? 66
                                                               : 67) {
        lc_error e;
        lc_error_init(&e);
        record_fixture *f = record_fixture_open(directory, key, codec, sync,
                                                count, PS_MIB + 17, &e);
        lc_collection_stream_recode *work = NULL;
        lc_collection_recode_stream stream = {.units = 999};
        ok(lc_collection_stream_recode_open(&f->pouch->allocator, f->target,
                                            &f->reader, record_output, f, &work,
                                            &e),
           &e);
        int finished = 0;
        size_t visited = 999, calls = 0;
        REQUIRE(lc_collection_stream_recode_step(work, 0, &visited, &finished,
                                                 &stream,
                                                 NULL) == LC_ERR_INVALID &&
                visited == 999 && !finished && stream.units == 999);
        if (count == 3) {
          record_paused_step(f, work, &e);
          lc_collection_stream_recode_close(work);
          record_abort(f, &e);
          ok(lc_collection_stream_recode_open(&f->pouch->allocator, f->target,
                                              &f->reader, record_output, f,
                                              &work, &e),
             &e);
        }
        while (!finished) {
          ok(lc_collection_stream_recode_step(work, 2, &visited, &finished,
                                              &stream, &e),
             &e);
          REQUIRE(visited <= 2 && ++calls <= count);
          REQUIRE(finished || stream.units == 999);
        }
        REQUIRE(stream.units == count &&
                (count == 1 || stream.height + 1 == f->stream.stream_height));
        ps_stats before, after;
        ok(ps_status(ps_root_space(f->root), &before, &e), &e);
        ok(lc_collection_stream_recode_step(work, 1, &visited, &finished,
                                            &stream, &e),
           &e);
        ok(ps_status(ps_root_space(f->root), &after, &e), &e);
        REQUIRE(!visited && finished && before.syncs == after.syncs &&
                before.allocated_bytes == after.allocated_bytes);
        lc_collection_stream_recode_close(work);
        lc_collection_recode_links links = {.stream = &stream};
        if (count == 3)
          record_stream_guards(f, &stream, &e);
        lc_pouch_collection_record converted, meta[2], small, other, deleted;
        lc_u64 segment, body_segment, previous_segment;
        ok(record_encode(f, &f->stream, &links, &converted, &body_segment, &e),
           &e);
        record_identity(&f->stream, &converted);
        REQUIRE(converted.stream_height == stream.height);
        if (count == 3)
          record_alias_guards(f, &converted, body_segment, &e);
        record_stream_check(f, &converted, body_segment, &e);
        links = (lc_collection_recode_links){.body = &converted,
                                             .previous_segment = body_segment,
                                             .previous_offset =
                                                 converted.record_offset};
        for (size_t i = 0; i < 2; ++i) {
          previous_segment = links.previous_segment;
          ok(record_encode(f, &f->meta[i], &links, &meta[i], &segment, &e), &e);
          record_identity(&f->meta[i], &meta[i]);
          REQUIRE(meta[i].previous_segment == previous_segment &&
                  meta[i].previous_offset == links.previous_offset &&
                  meta[i].stream_height == stream.height);
          ok(lc_pouch_collection_record_check_body(&meta[i], &converted, &e),
             &e);
          record_stream_check(f, &meta[i], body_segment, &e);
          links.previous_segment = segment;
          links.previous_offset = meta[i].record_offset;
        }
        ok(record_encode(f, &f->small, NULL, &small, &segment, &e), &e);
        record_identity(&f->small, &small);
        body(f->pouch, f->parts[f->count - 1].access.io, &small, &e);
        links = (lc_collection_recode_links){.body = &small,
                                             .previous_segment = segment,
                                             .previous_offset =
                                                 small.record_offset};
        ok(record_encode(f, &f->edit_body, NULL, &other, &segment, &e), &e);
        record_identity(&f->edit_body, &other);
        ok(record_encode(f, &f->receipt, NULL, &other, &segment, &e), &e);
        record_identity(&f->receipt, &other);
        ok(record_encode(f, &f->deleted, NULL, &deleted, &segment, &e), &e);
        record_identity(&f->deleted, &deleted);
        ok(record_encode(f, &f->small_meta, &links, &other, &segment, &e), &e);
        record_identity(&f->small_meta, &other);
        ok(lc_pouch_collection_record_check_body(&other, &small, &e), &e);
        ok(ps_collection_segment_seal(f->parts[f->count - 1].owner, &e), &e);
        lc_collection_disk_head head = {0}, loaded;
        head.active = f->parts[f->count - 1].id;
        head.tail_segment = f->parts[0].id;
        ok(ps_collection_head_store(ps_root_space(f->root), 73, &head, NULL,
                                    &e),
           &e);
        ok(ps_collection_head_load(ps_root_space(f->root), 73, NULL, &loaded,
                                   &e),
           &e);
        REQUIRE(loaded.active == head.active &&
                loaded.tail_segment == head.tail_segment);
        record_reopen(f, &e);
        ok(ps_collection_head_load(ps_root_space(f->root), 73, NULL, &loaded,
                                   &e),
           &e);
        REQUIRE(loaded.active == head.active &&
                loaded.tail_segment == head.tail_segment &&
                lc_pouch_crypto_compression_enabled(f->pouch->crypto) ==
                    !!(codec & 2));
        record_stream_check(f, &converted, body_segment, &e);
        record_stream_check(f, &meta[1], body_segment, &e);
        printf("native record conversion codec=%d sync=%d units=%zu "
               "height=%u->%u parts=%zu\n",
               codec, sync, count, f->stream.stream_height, stream.height,
               f->count);
        record_fixture_close(f, &e);
        lc_error_cleanup(&e);
      }
}
static int record_build(record_fixture *f, lc_collection_recode_stream *out,
                        lc_error *e) {
  lc_collection_stream_recode *work = NULL;
  int rc = lc_collection_stream_recode_open(
      &f->pouch->allocator, f->target, &f->reader, record_output, f, &work, e);
  int finished = 0;
  while (rc == LC_OK && !finished) {
    size_t visited = 999;
    finished = 777;
    rc = lc_collection_stream_recode_step(work, 1, &visited, &finished, out, e);
    if (rc != LC_OK) {
      REQUIRE(visited == 999 && finished == 777);
      REQUIRE(lc_collection_stream_recode_step(work, 1, &visited, &finished,
                                               out, NULL) == LC_ERR_INVALID);
    } else
      REQUIRE(visited <= 1);
  }
  lc_collection_stream_recode_close(work);
  return rc;
}
static void record_abort(record_fixture *f, lc_error *e) {
  lc_collection_chunks_reader_close(&f->reader);
  size_t parts = f->count;
  for (size_t i = 0; i < parts; ++i) {
    ps_collection_segment_close(f->parts[i].owner);
    memset(&f->parts[i], 0, sizeof(f->parts[i]));
  }
  f->count = 0;
  uint64_t reclaimed, skipped;
  ok(ps_root_reclaim_orphans(f->root, &reclaimed, &skipped, e), e);
  REQUIRE(reclaimed >= parts);
  ok(ps_checkpoint(ps_root_space(f->root), e), e);
  ok(lc_collection_chunks_open(&f->reader, &f->pouch->allocator,
                               f->pouch->crypto, f->id, record_reads(f),
                               record_source_io(f), &f->stream, e),
     e);
}
static size_t record_fault_trial(const char *directory, const char *key,
                                 int codec, int sync, int stage, int kind,
                                 size_t cut) {
  lc_error e;
  lc_error_init(&e);
  record_fixture *f =
      record_fixture_open(directory, key, codec, sync, 3, 65537, &e);
  lc_collection_recode_stream stream = {.units = 999};
  lc_collection_recode_links links = {0};
  lc_pouch_collection_record body_record, converted = {.generation = 999};
  const lc_pouch_collection_record *source = NULL;
  lc_u64 segment;
  if (stage == 1 || stage == 2) {
    ok(record_build(f, &stream, &e), &e);
    links.stream = &stream;
    if (stage == 2) {
      ok(record_encode(f, &f->stream, &links, &body_record, &segment, &e), &e);
      links = (lc_collection_recode_links){.body = &body_record,
                                           .previous_segment = segment,
                                           .previous_offset =
                                               body_record.record_offset};
    }
    source = stage == 1 ? &f->stream : &f->meta[0];
  } else if (stage >= 3) {
    source = stage == 3   ? &f->small
             : stage == 4 ? &f->edit_body
             : stage == 5 ? &f->receipt
                          : &f->deleted;
  }
  lc_pouch_record_io output = {0};
  uint64_t before = 0, after = 0;
  if (stage) {
    ok(record_output(f, source->bytes + 2048, 0, &output, &segment, &e), &e);
    ok(lc_pouch_record_io_tail(f->parts[f->count - 1].access.io, &before, &e),
       &e);
  }
  unit_allocations = unit_largest = 0;
  unit_fail_allocation = kind == 1 ? (cut ? cut : SIZE_MAX) : 0;
  f->watch.calls = 0;
  f->watch.fail = kind >= 2 ? (cut ? cut : SIZE_MAX) : 0;
  f->watch.torn = kind == 3;
  int rc;
  if (stage) {
    rc = lc_pouch_collection_record_recode(
        f->pouch, f->target, f->id, record_source_io(f), leaf,
        source->record_offset, source->next_offset, &links, output,
        f->parts[f->count - 1].leaf, &converted,
        kind == 1 && cut % 2 ? NULL : &e);
  } else
    rc = record_build(f, &stream, kind == 1 && cut % 2 ? NULL : &e);
  size_t calls = kind == 1 ? unit_allocations : f->watch.calls;
  unit_fail_allocation = f->watch.fail = 0;
  if (cut) {
    if (rc != (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT))
      fprintf(stderr,
              "record fault codec=%d sync=%d stage=%d kind=%d cut=%zu "
              "calls=%zu rc=%d %s\n",
              codec, sync, stage, kind, cut, calls, rc,
              e.message ? e.message : "?");
    REQUIRE(calls >= cut &&
            rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
    REQUIRE(stage ? converted.generation == 999 : stream.units == 999);
    clear(&e);
    if (stage) {
      ok(lc_pouch_record_io_tail(f->parts[f->count - 1].access.io, &after, &e),
         &e);
      REQUIRE(after == before);
    }
  } else
    ok(rc, &e);
  record_abort(f, &e);
  record_stream_check(f, &f->stream, 1, &e);
  ok(record_build(f, &stream, &e), &e);
  links = (lc_collection_recode_links){.stream = &stream};
  ok(record_encode(f, &f->stream, &links, &converted, &segment, &e), &e);
  record_identity(&f->stream, &converted);
  record_stream_check(f, &converted, segment, &e);
  record_fixture_close(f, &e);
  lc_error_cleanup(&e);
  return calls;
}
static void record_faults(const char *directory, const char *key) {
  size_t sites = 0;
  for (int codec = 0; codec <= 3; codec += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int stage = 0; stage < 7; ++stage)
        for (int kind = 1; kind <= 3; ++kind) {
          size_t count =
              record_fault_trial(directory, key, codec, sync, stage, kind, 0);
          REQUIRE(count);
          for (size_t cut = 1; cut <= count; ++cut) {
            (void)record_fault_trial(directory, key, codec, sync, stage, kind,
                                     cut);
            ++sites;
          }
        }
  printf("native collection record/index allocation/I/O/partial sites=%zu\n",
         sites);
}
#endif
