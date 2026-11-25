#ifndef POUCH_SPACE_COLLECTION_CODEC_CASES_H
#define POUCH_SPACE_COLLECTION_CODEC_CASES_H
#include "fixture_directory_sync.h"
#include "lc_collection_unit_recode.h"

static size_t unit_allocations, unit_fail_allocation, unit_largest;
static int unit_measuring;
static int unit_allocate(size_t bytes) {
  if (unit_measuring && bytes > unit_largest)
    unit_largest = bytes;
  return unit_fail_allocation && ++unit_allocations == unit_fail_allocation;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return unit_allocate(n) ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t b) {
  return unit_allocate(n * b) ? NULL : __real_lc_calloc_with_allocator(a, n, b);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return unit_allocate(n) ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return unit_allocate(p ? strlen(p) + 1 : 0)
             ? NULL
             : __real_lc_strdup_with_allocator(a, p);
}
typedef struct unit_watch unit_watch;
typedef struct unit_access {
  lc_pouch_record_io io;
  unit_watch *watch;
  int source;
} unit_access;
struct unit_watch {
  unit_access input, output;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  size_t calls, fail;
  lc_u64 stored;
  int torn, armed, paused, released, streamed, corruption;
};
static int unit_io_error(lc_error *e) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected unit I/O failure", NULL,
                      NULL, "test");
}
static int unit_cut(unit_watch *w) { return w->fail && ++w->calls == w->fail; }
static int unit_read(void *context, uint64_t at, const struct iovec *parts,
                     int count, lc_error *e) {
  unit_access *a = context;
  unit_watch *w = a->watch;
  if (unit_cut(w))
    return unit_io_error(e);
  if (a->source && at >= LC_COLLECTION_UNIT_HEADER) {
    pthread_mutex_lock(&w->mutex);
    if (w->armed) {
      w->armed = 0;
      w->paused = 1;
      pthread_cond_broadcast(&w->changed);
      while (!w->released)
        pthread_cond_wait(&w->changed, &w->mutex);
    }
    pthread_mutex_unlock(&w->mutex);
    if (at - LC_COLLECTION_UNIT_HEADER >= w->stored / 2 && !w->streamed) {
      uint64_t tail;
      ok(lc_pouch_record_io_tail(w->output.io, &tail, e), e);
      w->streamed = tail > 32 + LC_COLLECTION_UNIT_HEADER;
    }
  }
  int rc = lc_pouch_record_io_read(a->io, at, parts, count, e);
  if (rc == LC_OK && a->source && w->corruption) {
    if (!at && parts[0].iov_len == LC_COLLECTION_UNIT_HEADER) {
      unsigned char *bytes = parts[0].iov_base;
      if (w->corruption == 1)
        bytes[0] ^= 0x80;
      else if (w->corruption == 3) {
        /* Valid metadata CRC around a false plaintext length. */
        bytes[32] ^= 1;
        uint64_t crc = crc32(0L, bytes + 32, LC_COLLECTION_UNIT_META - 8);
        for (size_t i = 0; i < 8; ++i)
          bytes[LC_COLLECTION_UNIT_HEADER - 8 + i] =
              (unsigned char)(crc >> (8 * i));
      }
    } else if (w->corruption == 2 && parts[0].iov_len)
      ((unsigned char *)parts[0].iov_base)[0] ^= 1;
  }
  return rc;
}
static int unit_append(void *context, const struct iovec *parts, int count,
                       lc_error *e) {
  unit_access *a = context;
  if (unit_cut(a->watch)) {
    if (a->watch->torn && count && parts[0].iov_len) {
      struct iovec half = {parts[0].iov_base, (parts[0].iov_len + 1) / 2};
      ok(lc_pouch_record_io_append(a->io, &half, 1, e), e);
    }
    return unit_io_error(e);
  }
  return lc_pouch_record_io_append(a->io, parts, count, e);
}
static int unit_patch(void *context, uint64_t at, const struct iovec *parts,
                      int count, lc_error *e) {
  unit_access *a = context;
  if (unit_cut(a->watch)) {
    if (a->watch->torn && count && parts[0].iov_len) {
      struct iovec half = {parts[0].iov_base, (parts[0].iov_len + 1) / 2};
      ok(lc_pouch_record_io_patch(a->io, at, &half, 1, e), e);
    }
    return unit_io_error(e);
  }
  return lc_pouch_record_io_patch(a->io, at, parts, count, e);
}
static int unit_tail(void *context, uint64_t *tail, lc_error *e) {
  unit_access *a = context;
  return unit_cut(a->watch) ? unit_io_error(e)
                            : lc_pouch_record_io_tail(a->io, tail, e);
}
static void unit_discard(void *context, uint64_t at) {
  unit_access *a = context;
  a->io.ops->discard(a->io.context, at);
}
static lc_pouch_record_io unit_io(unit_access *a) {
  static const lc_pouch_record_io_ops ops = {unit_read, unit_append, unit_patch,
                                             unit_tail, unit_discard};
  lc_pouch_record_io io = {&ops, a};
  return io;
}
static unsigned char unit_byte(lc_u64 at, lc_u64 size, int compressible) {
  return compressible ? (unsigned char)('a' + (at % 31 == 0))
                      : content_at(at, size);
}
static int unit_seed_write(lc_sink *sink, const void *bytes, size_t count,
                           lc_error *e) {
  lc_pouch_record_io *io = sink->impl;
  struct iovec part = {(void *)bytes, count};
  return lc_pouch_record_io_append(*io, &part, 1, e) == LC_OK;
}
static void unit_seed(lc_pouch_crypto *crypto, lc_pouch_record_io output,
                      lc_u64 bytes, int compressible, lc_collection_unit *out,
                      lc_error *e) {
  lc_collection_unit unit = {0};
  lc_pouch_record_header header = {0};
  unsigned char prefix[LC_COLLECTION_UNIT_HEADER] = {0}, scratch[2048];
  lc_sink destination = {0};
  destination.impl = &output;
  destination.write = unit_seed_write;
  lc_pouch_crypto_writer *writer = NULL;
  strcpy(unit.context, "collection-codec-unit:17");
  unit.ordinal = 17;
  memset(unit.etag, 'a', 64);
  header.type = LC_POUCH_COLLECTION_CHUNK;
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  header.meta_len = LC_COLLECTION_UNIT_META;
  lc_pouch_record_header_encode(&header, prefix);
  struct iovec part = {prefix, sizeof(prefix)};
  ok(lc_pouch_record_io_append(output, &part, 1, e), e);
  ok(lc_pouch_crypto_writer_open_sink(crypto, unit.context, &destination,
                                      &writer, e),
     e);
  lc_sink *sink = lc_pouch_crypto_writer_sink(writer);
  for (lc_u64 at = 0; at < bytes;) {
    size_t n =
        bytes - at < sizeof(scratch) ? (size_t)(bytes - at) : sizeof(scratch);
    for (size_t i = 0; i < n; ++i)
      scratch[i] = unit_byte(at + i, bytes, compressible);
    REQUIRE(sink->write(sink, scratch, n, e));
    at += n;
  }
  const char *descriptor = NULL;
  ok(lc_pouch_crypto_writer_finish_borrowed(writer, &unit.plain, &unit.stored,
                                            &unit.crc, &descriptor, e),
     e);
  if (descriptor) {
    REQUIRE(strlen(descriptor) < sizeof(unit.descriptor));
    strcpy(unit.descriptor, descriptor);
  }
  ok(lc_collection_unit_write_meta(output, 0, &unit, e), e);
  *out = unit;
  lc_pouch_crypto_writer_close(writer);
}
static void unit_check(lc_pouch_crypto *crypto, lc_pouch_record_io input,
                       lc_u64 at, const lc_collection_unit *expected,
                       int compressible, lc_error *e) {
  lc_collection_unit loaded;
  ok(lc_collection_unit_read(input, at, expected->stored, &loaded, e), e);
  REQUIRE(loaded.plain == expected->plain &&
          loaded.stored == expected->stored &&
          loaded.ordinal == expected->ordinal && loaded.crc == expected->crc &&
          !strcmp(loaded.context, expected->context) &&
          !strcmp(loaded.etag, expected->etag) &&
          !strcmp(loaded.descriptor, expected->descriptor));
  lc_pouch_record_span span;
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *source = NULL;
  ok(lc_pouch_record_span_bind(&span, input, at + LC_COLLECTION_UNIT_HEADER,
                               loaded.stored, NULL, 0, e),
     e);
  ok(lc_pouch_crypto_reader_open(crypto, &reader, e), e);
  ok(lc_pouch_crypto_reader_bind_source(reader, loaded.context, loaded.context,
                                        &span.source, loaded.stored,
                                        loaded.descriptor, &source, e),
     e);
  unsigned char scratch[2048];
  for (int pass = 0; pass < 2; ++pass) {
    lc_u64 offset = 0;
    size_t got;
    if (pass)
      ok(source->reset(source, e), e);
    while ((got = source->read(source, scratch, sizeof(scratch), e))) {
      REQUIRE(got <= loaded.plain - offset);
      int same = 1;
      for (size_t i = 0; i < got; ++i)
        if (scratch[i] != unit_byte(offset + i, loaded.plain, compressible))
          same = 0;
      REQUIRE(same);
      offset += got;
    }
    ok(e->code, e);
    REQUIRE(offset == loaded.plain);
  }
  lc_pouch_crypto_reader_close(reader);
}
typedef struct unit_fixture {
  ps_root *root;
  ps_collection_segment *source, *output;
  lc_pouch_crypto *target;
  lc_collection_unit original;
  ps_root_options options;
  unit_watch watch;
  char path[1024];
} unit_fixture;
static void unit_fixture_open(unit_fixture *f, const char *directory,
                              const char *key, int codec, int durable,
                              lc_u64 bytes, int compressible, lc_error *e) {
  REQUIRE(snprintf(f->path, sizeof(f->path), "%s/codec-unit", directory) > 0);
  f->options.storage.initial_capacity = f->options.storage.maximum_capacity =
      10 * PS_MIB;
  f->options.storage.durable_sync = durable;
  f->options.crypto.key_string = codec & 1 ? key : NULL;
  f->options.crypto.compression_enabled = !!(codec & 2);
  ok(ps_root_open(f->path, 1, &f->options, &f->root, e), e);
  ps_key source = {73, 1, PS_STREAM}, output = {73, 2, PS_STREAM};
  ok(ps_collection_segment_open(ps_root_space(f->root), source, 1, NULL, NULL,
                                NULL, &f->source, e),
     e);
  ok(ps_collection_segment_open(ps_root_space(f->root), output, 1, NULL, NULL,
                                NULL, &f->output, e),
     e);
  f->watch.input =
      (unit_access){ps_collection_segment_io(f->source), &f->watch, 1};
  f->watch.output =
      (unit_access){ps_collection_segment_io(f->output), &f->watch, 0};
  REQUIRE(!pthread_mutex_init(&f->watch.mutex, NULL));
  REQUIRE(!pthread_cond_init(&f->watch.changed, NULL));
  unit_seed(ps_root_crypto(f->root), f->watch.input.io, bytes, compressible,
            &f->original, e);
  ok(ps_collection_segment_seal(f->source, e), e);
  f->watch.stored = f->original.stored;
  unsigned char guard[32];
  memset(guard, 0x5a, sizeof(guard));
  struct iovec part = {guard, sizeof(guard)};
  ok(lc_pouch_record_io_append(f->watch.output.io, &part, 1, e), e);
  ok(ps_collection_segment_publish(f->output, PS_COMMIT_DEFAULT, e), e);
  lc_pouch_crypto_open_options target = f->options.crypto;
  target.compression_enabled = !target.compression_enabled;
  ok(lc_pouch_crypto_open(NULL, &target, &f->target, NULL, e), e);
}
static void unit_fixture_close(unit_fixture *f, lc_error *e) {
  ps_collection_segment_close(f->source);
  ps_collection_segment_close(f->output);
  ok(ps_root_close(f->root, e), e);
  lc_pouch_crypto_close(f->target);
  REQUIRE(!pthread_mutex_destroy(&f->watch.mutex));
  REQUIRE(!pthread_cond_destroy(&f->watch.changed));
  REQUIRE(!unlink(f->path));
}
static size_t unit_trial(const char *directory, const char *key, int codec,
                         int durable, lc_u64 bytes, int compressible, int kind,
                         size_t cut, int corruption) {
  unit_fixture f = {0};
  ps_io io = ps_native_io;
  io.sync = fixture_complete_sync;
  if (kind)
    f.options.storage.io = &io;
  lc_error e;
  lc_error_init(&e);
  unit_fixture_open(&f, directory, key, codec, durable, bytes, compressible,
                    &e);
  lc_collection_unit_recode *work = NULL;
  lc_collection_unit converted = {.plain = 999};
  lc_u64 offset = 999, bound = 0;
  ok(lc_collection_unit_recode_bound(f.target, bytes, &bound, &e), &e);
  REQUIRE(bound >= LC_COLLECTION_UNIT_HEADER);
  unit_allocations = unit_largest = 0;
  unit_measuring = 1;
  unit_fail_allocation = kind == 1 ? (cut ? cut : SIZE_MAX) : 0;
  f.watch.fail = kind >= 2 ? (cut ? cut : SIZE_MAX) : 0;
  f.watch.torn = kind == 3;
  f.watch.corruption = corruption;
  int rc = lc_collection_unit_recode_open(NULL, f.target, &work, &e);
  if (rc == LC_OK)
    rc = lc_collection_unit_recode_run(
        work, unit_io(&f.watch.input), 0, f.original.stored,
        unit_io(&f.watch.output), &offset, &converted,
        kind == 1 && cut % 2 ? NULL : &e);
  size_t calls = kind == 1 ? unit_allocations : f.watch.calls;
  unit_fail_allocation = unit_measuring = 0;
  f.watch.fail = f.watch.corruption = 0;
  if (cut || corruption) {
    REQUIRE(rc == (corruption  ? LC_ERR_PROTOCOL
                   : kind == 1 ? LC_ERR_NOMEM
                               : LC_ERR_TRANSPORT));
    REQUIRE(offset == 999 && converted.plain == 999);
    uint64_t tail;
    clear(&e);
    ok(lc_pouch_record_io_tail(f.watch.output.io, &tail, &e), &e);
    REQUIRE(tail == 32);
  } else {
    ok(rc, &e);
    REQUIRE(offset == 32 &&
            converted.stored + LC_COLLECTION_UNIT_HEADER <= bound &&
            unit_largest <= 256 * 1024);
    if (!compressible && bytes >= 2 * PS_MIB)
      REQUIRE(f.watch.streamed);
    if (bytes >= 2 * PS_MIB)
      printf("native unit codec=%d sync=%d plain=%llu stored=%llu largest=%zu "
             "streamed=%d\n",
             codec, durable, (unsigned long long)bytes,
             (unsigned long long)converted.stored, unit_largest,
             f.watch.streamed);
    unit_check(f.target, f.watch.output.io, offset, &converted, compressible,
               &e);
  }
  unsigned char guard[32];
  struct iovec part = {guard, sizeof(guard)};
  ok(lc_pouch_record_io_read(f.watch.output.io, 0, &part, 1, &e), &e);
  for (size_t i = 0; i < sizeof(guard); ++i)
    REQUIRE(guard[i] == 0x5a);
  unit_check(ps_root_crypto(f.root), f.watch.input.io, 0, &f.original,
             compressible, &e);
  if (!work)
    ok(lc_collection_unit_recode_open(NULL, f.target, &work, &e), &e);
  lc_u64 invalid_bound = 999;
  REQUIRE(lc_collection_unit_recode_bound(f.target, 0, &invalid_bound, NULL) ==
              LC_ERR_INVALID &&
          invalid_bound == 999);
  REQUIRE(lc_collection_unit_recode_bound(f.target, LC_COLLECTION_UNIT_MAX + 1,
                                          &invalid_bound,
                                          NULL) == LC_ERR_INVALID &&
          invalid_bound == 999);
  REQUIRE(lc_collection_unit_recode_run(
              work, f.watch.input.io, 0, f.original.stored, f.watch.input.io,
              &invalid_bound, &converted, NULL) == LC_ERR_INVALID &&
          invalid_bound == 999);
  ok(ps_collection_segment_truncate(f.output, 32, &e), &e);
  /* Reuse scratch after success, input corruption, allocation failure and
   * partial append/fixup failure. Each output remains independently decodable.
   */
  ok(lc_collection_unit_recode_run(work, unit_io(&f.watch.input), 0,
                                   f.original.stored, unit_io(&f.watch.output),
                                   &offset, &converted, &e),
     &e);
  unit_check(f.target, f.watch.output.io, offset, &converted, compressible, &e);
  REQUIRE(converted.plain == f.original.plain &&
          converted.ordinal == f.original.ordinal &&
          !strcmp(converted.context, f.original.context) &&
          !strcmp(converted.etag, f.original.etag));
  int matches = 0;
  ok(lc_pouch_crypto_descriptor_matches(f.target, converted.descriptor,
                                        &matches, &e),
     &e);
  REQUIRE(matches);
  lc_collection_unit_recode_close(work);
  ok(ps_collection_segment_seal(f.output, &e), &e);
  ps_collection_segment_close(f.source);
  ps_collection_segment_close(f.output);
  ok(ps_root_close(f.root, &e), &e);
  ok(ps_root_open(f.path, 0, &f.options, &f.root, &e), &e);
  ps_key source = {73, 1, PS_STREAM}, output = {73, 2, PS_STREAM};
  ok(ps_collection_segment_open(ps_root_space(f.root), source, 0, NULL, NULL,
                                NULL, &f.source, &e),
     &e);
  ok(ps_collection_segment_open(ps_root_space(f.root), output, 0, NULL, NULL,
                                NULL, &f.output, &e),
     &e);
  unit_check(f.target, ps_collection_segment_io(f.output), offset, &converted,
             compressible, &e);
  unit_fixture_close(&f, &e);
  lc_error_cleanup(&e);
  return calls;
}
static void unit_cases(const char *directory, const char *key, int faults) {
  size_t sites = 0;
  fixture_directory_sync_model(faults);
  for (int codec = 0; codec < 4; ++codec)
    for (int durable = 0; durable < 2; ++durable) {
      if (faults) {
        for (int kind = 1; kind <= 3; ++kind) {
          size_t count =
              unit_trial(directory, key, codec, durable, 4097, 0, kind, 0, 0);
          REQUIRE(count);
          for (size_t cut = 1; cut <= count; ++cut) {
            (void)unit_trial(directory, key, codec, durable, 4097, 0, kind, cut,
                             0);
            ++sites;
          }
        }
        /* Fail after a substantial encoded suffix exists, as well as in its
         * final fixups. The small exhaustive matrix covers every call site. */
        for (int kind = 2; kind <= 3; ++kind) {
          size_t count = unit_trial(directory, key, codec, durable, 2 * PS_MIB,
                                    0, kind, 0, 0);
          REQUIRE(count > 4);
          const size_t cuts[] = {count / 2, count - 1, count};
          for (size_t i = 0; i < sizeof(cuts) / sizeof(*cuts); ++i) {
            (void)unit_trial(directory, key, codec, durable, 2 * PS_MIB, 0,
                             kind, cuts[i], 0);
            ++sites;
          }
        }
      } else {
        const lc_u64 sizes[] = {1, 65537, 2 * PS_MIB};
        for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); ++i)
          (void)unit_trial(directory, key, codec, durable, sizes[i], 0, 0, 0,
                           0);
        /* The maximum unit is compressible so old+new fit the fixed 10 MiB
         * capacity in either direction. No whole-unit payload buffer exists. */
        (void)unit_trial(directory, key, codec, durable, LC_COLLECTION_UNIT_MAX,
                         1, 0, 0, 0);
        for (int corruption = 1; corruption <= 3; ++corruption)
          (void)unit_trial(directory, key, codec, durable, 65537, 0, 0, 0,
                           corruption);
      }
    }
  fixture_directory_sync_model(0);
  if (faults)
    printf(
        "native collection unit conversion allocation/I/O/partial sites=%zu\n",
        sites);
}
typedef struct unit_job {
  unit_fixture *fixture;
  lc_collection_unit_recode *work;
  lc_collection_unit converted;
  lc_u64 offset;
  int rc;
} unit_job;
static void *unit_convert_thread(void *context) {
  unit_job *job = context;
  unit_fixture *f = job->fixture;
  lc_error e;
  lc_error_init(&e);
  job->rc = lc_collection_unit_recode_run(
      job->work, unit_io(&f->watch.input), 0, f->original.stored,
      unit_io(&f->watch.output), &job->offset, &job->converted, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void unit_concurrency(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int durable = 0; durable < 2; ++durable) {
      unit_fixture f = {0};
      lc_error e;
      lc_error_init(&e);
      unit_fixture_open(&f, directory, key, codec, durable, 2 * PS_MIB, 0, &e);
      unit_job job = {.fixture = &f, .rc = LC_ERR_INVALID};
      ok(lc_collection_unit_recode_open(NULL, f.target, &job.work, &e), &e);
      f.watch.armed = 1;
      pthread_t worker;
      REQUIRE(!pthread_create(&worker, NULL, unit_convert_thread, &job));
      pthread_mutex_lock(&f.watch.mutex);
      while (!f.watch.paused)
        pthread_cond_wait(&f.watch.changed, &f.watch.mutex);
      pthread_mutex_unlock(&f.watch.mutex);
      unsigned char prefix[32];
      struct iovec part = {prefix, sizeof(prefix)};
      ok(lc_pouch_record_io_read(f.watch.output.io, 32, &part, 1, &e), &e);
      lc_pouch_record_header header;
      ok(lc_pouch_record_header_decode(prefix, &header, &e), &e);
      REQUIRE(header.type == LC_POUCH_COLLECTION_CHUNK &&
              header.flags == LC_POUCH_RECORD_FLAG_PENDING);
      ps_owned_status status;
      ok(ps_collection_segment_status(f.output, &status, NULL, &e), &e);
      REQUIRE(status.tail == 32);
      ps_collection_segment *peer = NULL;
      ps_key peer_key = {73, 3, PS_STREAM};
      ok(ps_collection_segment_open(ps_root_space(f.root), peer_key, 1, NULL,
                                    NULL, NULL, &peer, &e),
         &e);
      memset(prefix, 0x73, sizeof(prefix));
      ok(lc_pouch_record_io_append(ps_collection_segment_io(peer), &part, 1,
                                   &e),
         &e);
      ok(ps_collection_segment_seal(peer, &e), &e);
      ps_collection_segment_close(peer);
      unit_check(ps_root_crypto(f.root), f.watch.input.io, 0, &f.original, 0,
                 &e);
      pthread_mutex_lock(&f.watch.mutex);
      f.watch.released = 1;
      pthread_cond_broadcast(&f.watch.changed);
      pthread_mutex_unlock(&f.watch.mutex);
      REQUIRE(!pthread_join(worker, NULL));
      ok(job.rc, &e);
      REQUIRE(job.offset == 32);
      unit_check(f.target, f.watch.output.io, job.offset, &job.converted, 0,
                 &e);
      ok(ps_collection_segment_status(f.output, &status, NULL, &e), &e);
      REQUIRE(status.tail == 32);
      lc_collection_unit_recode_close(job.work);
      unit_fixture_close(&f, &e);
      lc_error_cleanup(&e);
    }
}
#endif
