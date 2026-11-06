#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_collection_chunks.h"
#include "lc_collection_unit.h"
#include "lc_pouch_collection_record.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_record.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-record-"
static const char leaf[] = "seg-00000000000000000001.log";

typedef struct fixture {
  char root[256];
  char path[512];
  char id[LC_XID_STRING_SIZE];
  char *key;
  lc_pouch *pouch;
  lc_source *source;
  lc_source *decoded;
  lc_sink *sink;
  lc_error error;
  int fd;
  int copy_fd;
  int relocated_fd;
  lc_pouch_unix_seconds now;
  int clock_failed;
  unsigned int next_case;
  lc_allocator allocator;
  size_t live, rejected_size, rejected;
  int custom_allocator, reject_metadata;
} fixture;

static void *record_alloc(void *context, size_t size) {
  fixture *f = context;
  void *p;
  if (f->reject_metadata && size == f->rejected_size) {
    ++f->rejected;
    return NULL;
  }
  p = malloc(size);
  if (p)
    ++f->live;
  return p;
}
static void record_free(void *context, void *p) {
  fixture *f = context;
  if (p) {
    assert_true(f->live > 0);
    --f->live;
    free(p);
  }
}
static void *record_resize(void *context, void *p, size_t size) {
  fixture *f = context;
  if (!p)
    return record_alloc(context, size);
  if (!size) {
    record_free(context, p);
    return NULL;
  }
  if (f->reject_metadata && size == f->rejected_size) {
    ++f->rejected;
    return NULL;
  }
  return realloc(p, size);
}

static void clear_error(fixture *f) {
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char template_path[] = PREFIX "XXXXXX";
  assert_non_null(f);
  f->fd = -1;
  f->copy_fd = -1;
  f->relocated_fd = -1;
  f->now = 1000;
  lc_error_init(&f->error);
  *state = f;
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  assert_int_equal(lc_xid_new(f->id, &f->error), LC_OK);
  assert_int_equal(lc_pouch_crypto_generate_key_string(&f->key, &f->error),
                   LC_OK);
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_unit_test_preadv = NULL;
  lc_source_close(f->source);
  lc_source_close(f->decoded);
  lc_sink_close(f->sink);
  if (f->fd >= 0)
    close(f->fd);
  if (f->copy_fd >= 0)
    close(f->copy_fd);
  if (f->relocated_fd >= 0)
    close(f->relocated_fd);
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_pouch_close(f->pouch);
  assert_int_equal(f->live, 0);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static void open_case(fixture *f, int mode, int durable) {
  lc_pouch_open_options opts;
  char root[384];
  if (f->fd >= 0)
    assert_int_equal(close(f->fd), 0);
  f->fd = -1;
  lc_pouch_close(f->pouch);
  f->pouch = NULL;
  memset(&opts, 0, sizeof(opts));
  opts.background_compaction_enabled_set = 1;
  opts.background_compaction_enabled = 0;
  opts.query_indexing_enabled_set = 1;
  opts.query_indexing_enabled = 0;
  opts.crypto_key = mode & 1 ? f->key : NULL;
  opts.compression = mode & 2 ? "zlib" : "none";
  opts.durable_sync = durable;
  assert_true(
      snprintf(root, sizeof(root), "%s/case-%u", f->root, f->next_case++) > 0);
  assert_int_equal(lc_pouch_open(root,
                                 f->custom_allocator ? &f->allocator : NULL,
                                 &opts, &f->pouch, &f->error),
                   LC_OK);
  assert_true(snprintf(f->path, sizeof(f->path), "%s/%s", root, leaf) > 0);
  f->fd = open(f->path, O_CREAT | O_RDWR | O_TRUNC, 0600);
  assert_true(f->fd >= 0);
}

static uint64_t file_end(fixture *f) {
  struct stat st;
  assert_int_equal(fstat(f->fd, &st), 0);
  return (uint64_t)st.st_size;
}

static int append_json(fixture *f, const char *id, const char *json,
                       uint64_t position, uint64_t generation,
                       lc_pouch_collection_record *out) {
  int rc;
  assert_int_equal(
      lc_source_from_memory(json, strlen(json), &f->source, &f->error), LC_OK);
  rc = lc_pouch_collection_record_append(f->pouch, f->fd, leaf, f->id, id,
                                         position, generation, 0, 0, 0,
                                         f->source, out, &f->error);
  lc_source_close(f->source);
  f->source = NULL;
  return rc;
}

static void assert_body_context(fixture *f,
                                const lc_pouch_collection_record *record,
                                const char *binding, const char *expected) {
  const void *bytes;
  size_t length;
  size_t written;
  assert_int_equal(lc_pouch_crypto_source_from_file_span(
                       f->pouch->crypto, record->context, binding, f->path,
                       record->payload_offset, record->stored_bytes,
                       record->descriptor[0] ? record->descriptor : NULL,
                       &f->decoded, &f->error),
                   LC_OK);
  assert_int_equal(lc_sink_to_memory(&f->sink, &f->error), LC_OK);
  assert_int_equal(lc_copy(f->decoded, f->sink, &written, &f->error), LC_OK);
  assert_int_equal(lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error),
                   LC_OK);
  assert_int_equal(length, strlen(expected));
  assert_memory_equal(bytes, expected, length);
  lc_source_close(f->decoded);
  lc_sink_close(f->sink);
  f->decoded = NULL;
  f->sink = NULL;
}

static void assert_body(fixture *f, const lc_pouch_collection_record *record,
                        const char *expected) {
  assert_body_context(f, record, record->context, expected);
}

static int record_clock(clockid_t clock_id, struct timespec *out,
                        void *context) {
  fixture *f = context;
  (void)clock_id;
  if (f->clock_failed) {
    errno = EIO;
    return -1;
  }
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}

static void test_retention_metadata_references_body_and_compacts(void **state) {
  fixture *f = *state;
  const char *meta_leaf = "seg-00000000000000000002.log";
  const char *snapshot = "snapshot-00000000000000000001.log";
  const char *json = "{\"event\":\"keep these exact bytes\"}";
  lc_pouch_collection_record original, renewed, cleared, read, copied, deleted;
  lc_pouch_record_header header;
  char metadata_path[512], snapshot_path[512];
  unsigned char prefix[LC_POUCH_RECORD_HEADER_BYTES], before[128], after[128];
  unsigned char scratch[7];
  struct stat st;
  uint64_t saved_end;
  lc_pouch_test_queue_clock_gettime = record_clock;
  lc_pouch_test_queue_time_context = f;
  for (int mode = 0; mode < 4; ++mode) {
    for (int durable = 0; durable <= 1; ++durable) {
      open_case(f, mode, durable);
      assert_int_equal(
          lc_source_from_memory(json, strlen(json), &f->source, &f->error),
          LC_OK);
      assert_int_equal(lc_pouch_collection_record_append(
                           f->pouch, f->fd, leaf, f->id, "retained", 5, 7, -60,
                           0, 0, f->source, &original, &f->error),
                       LC_OK);
      lc_source_close(f->source);
      f->source = NULL;
      assert_int_equal(original.retention, 1060);
      assert_int_equal(original.updated_at_unix, 1000);
      saved_end = file_end(f);
      assert_true(original.stored_bytes <= sizeof(before));
      assert_int_equal(pread(f->fd, before, original.stored_bytes,
                             (off_t)original.payload_offset),
                       original.stored_bytes);
      assert_int_equal(
          lc_pouch_collection_record_read(f->fd, original.record_offset,
                                          saved_end, leaf, &read, &f->error),
          LC_OK);
      assert_int_equal(read.retention, 1060);
      assert_false(read.metadata_only);
      assert_string_equal(read.payload_container, leaf);
      assert_true(snprintf(metadata_path, sizeof(metadata_path), "%s/meta-%u",
                           f->root, f->next_case) > 0);
      if (f->copy_fd >= 0)
        close(f->copy_fd);
      f->copy_fd = open(metadata_path, O_RDWR | O_CREAT | O_EXCL, 0600);
      assert_true(f->copy_fd >= 0);
      /* The update has no open body descriptor and writes only STATE_META. */
      assert_int_equal(close(f->fd), 0);
      f->fd = -1;
      assert_int_equal(lc_pouch_collection_record_metadata(
                           f->pouch, f->copy_fd, meta_leaf, &original, 1, 8,
                           1200, &renewed, &f->error),
                       LC_OK);
      assert_int_equal(pread(f->copy_fd, prefix, sizeof(prefix), 0),
                       sizeof(prefix));
      assert_int_equal(
          lc_pouch_record_header_decode(prefix, &header, &f->error), LC_OK);
      assert_int_equal(header.type, 4); /* ordinary canonical STATE_META */
      assert_int_equal(header.payload_len, 0);
      assert_int_equal(header.payload_crc, 0);
      assert_int_equal(renewed.next_offset, sizeof(prefix) +
                                                strlen(renewed.append_id) +
                                                header.meta_len);
      assert_int_equal(
          lc_pouch_collection_record_read(f->copy_fd, 0, renewed.next_offset,
                                          meta_leaf, &read, &f->error),
          LC_OK);
      assert_true(read.metadata_only);
      assert_int_equal(read.retention, 1200);
      assert_int_equal(read.published_at, 7);
      assert_int_equal(read.generation, 8);
      assert_int_equal(read.previous_segment, 1);
      assert_int_equal(read.previous_offset, original.record_offset);
      assert_int_equal(read.payload_offset, original.payload_offset);
      assert_int_equal(read.payload_record_offset, original.record_offset);
      assert_string_equal(read.etag, original.etag);
      assert_int_equal(lc_pouch_collection_record_metadata(
                           f->pouch, f->copy_fd, meta_leaf, &read, 2, 9, 0,
                           &cleared, &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_read(
                           f->copy_fd, cleared.record_offset,
                           cleared.next_offset, meta_leaf, &read, &f->error),
                       LC_OK);
      assert_int_equal(read.retention, 0);
      assert_int_equal(read.previous_segment, 2);
      assert_int_equal(read.previous_offset, renewed.record_offset);
      assert_int_equal(read.published_at, 7);
      f->fd = open(f->path, O_RDWR | O_CLOEXEC);
      assert_true(f->fd >= 0);
      assert_int_equal(file_end(f), saved_end);
      assert_body(f, &original, json);
      assert_true(snprintf(snapshot_path, sizeof(snapshot_path), "%s/copy-%u",
                           f->root, f->next_case) > 0);
      if (f->relocated_fd >= 0)
        close(f->relocated_fd);
      f->relocated_fd = open(snapshot_path, O_RDWR | O_CREAT | O_EXCL, 0600);
      assert_true(f->relocated_fd >= 0);
      for (int step = 0; step < 2; ++step) {
        const lc_pouch_collection_record *current = step ? &cleared : &renewed;
        assert_int_equal(lc_pouch_collection_record_copy(
                             f->pouch, f->copy_fd, f->fd, meta_leaf, current,
                             f->relocated_fd, snapshot, scratch,
                             sizeof(scratch), &copied, &f->error),
                         LC_OK);
        assert_int_equal(lc_pouch_collection_record_read(
                             f->relocated_fd, copied.record_offset,
                             copied.next_offset, snapshot, &read, &f->error),
                         LC_OK);
        assert_false(read.metadata_only);
        assert_int_equal(read.retention, current->retention);
        assert_int_equal(read.published_at, 7);
        assert_int_equal(read.generation, current->generation);
        assert_int_equal(read.previous_segment, 0);
        assert_string_equal(read.payload_container, snapshot);
        assert_int_equal(pread(f->relocated_fd, after, read.stored_bytes,
                               (off_t)read.payload_offset),
                         read.stored_bytes);
        assert_memory_equal(before, after, read.stored_bytes);
      }
      assert_int_equal(lc_pouch_collection_record_delete(f->pouch, f->copy_fd,
                                                         &cleared, 10, &deleted,
                                                         &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_read(
                           f->copy_fd, deleted.record_offset,
                           deleted.next_offset, meta_leaf, &read, &f->error),
                       LC_OK);
      assert_true(read.deleted);
      assert_int_equal(read.published_at, 7);
      assert_int_equal(read.retention, 0);
      assert_int_equal(fstat(f->copy_fd, &st), 0);
      assert_int_equal(lc_pouch_collection_record_metadata(
                           f->pouch, f->copy_fd, meta_leaf, &read, 2, 11, 1500,
                           &renewed, &f->error),
                       LC_ERR_INVALID);
      clear_error(f);
      struct stat after_stat;
      assert_int_equal(fstat(f->copy_fd, &after_stat), 0);
      assert_int_equal(after_stat.st_size, st.st_size);
    }
  }
}

typedef struct delayed_record_source {
  lc_source source;
  fixture *f;
  size_t offset;
  int fail_clock;
} delayed_record_source;

static size_t delayed_record_read(lc_source *self, void *bytes, size_t capacity,
                                  lc_error *error) {
  delayed_record_source *source = (delayed_record_source *)self;
  size_t count = 2 - source->offset;
  (void)error;
  if (count > capacity)
    count = capacity;
  memcpy(bytes, "{}" + source->offset, count);
  source->offset += count;
  if (source->offset == 2) {
    source->f->now = 2000;
    if (count == 0 && source->fail_clock)
      source->f->clock_failed = 1;
  }
  return count;
}

static void test_retention_uses_publication_clock_and_rolls_back(void **state) {
  fixture *f = *state;
  delayed_record_source source = {0};
  lc_pouch_collection_record record, failed, read;
  uint64_t saved_end;
  open_case(f, 0, 0);
  lc_pouch_test_queue_clock_gettime = record_clock;
  lc_pouch_test_queue_time_context = f;
  source.source.read = delayed_record_read;
  source.f = f;
  assert_int_equal(lc_pouch_collection_record_append(
                       f->pouch, f->fd, leaf, f->id, "slow", 1, 1, -60, 1,
                       -7776000, &source.source, &record, &f->error),
                   LC_OK);
  assert_int_equal(record.retention, 2060);
  assert_int_equal(record.updated_at_unix, 2000);
  assert_true(record.has_collection_retention);
  assert_int_equal(record.collection_retention, 7778000);
  saved_end = file_end(f);
  assert_int_equal(lc_pouch_collection_record_append(
                       f->pouch, f->fd, leaf, f->id, "overflow", 2, 2,
                       LC_I64_MIN, 0, 0, &source.source, &failed, &f->error),
                   LC_ERR_INVALID);
  assert_false(failed.published);
  assert_int_equal(file_end(f), saved_end);
  clear_error(f);
  source.offset = 0;
  assert_int_equal(lc_pouch_collection_record_append(
                       f->pouch, f->fd, leaf, f->id, "overflow", 2, 2,
                       -LC_I64_MAX, 0, 0, &source.source, &failed, &f->error),
                   LC_ERR_INVALID);
  assert_false(failed.published);
  assert_int_equal(file_end(f), saved_end);
  clear_error(f);
  source.offset = 0;
  source.fail_clock = 1;
  f->now = 1000;
  assert_int_equal(lc_pouch_collection_record_append(
                       f->pouch, f->fd, leaf, f->id, "clock", 2, 2, -60, 0, 0,
                       &source.source, &failed, &f->error),
                   LC_ERR_TRANSPORT);
  assert_false(failed.published);
  assert_int_equal(file_end(f), saved_end);
  f->clock_failed = 0;
  clear_error(f);
  assert_int_equal(lc_pouch_collection_record_read(f->fd, record.record_offset,
                                                   saved_end, leaf, &read,
                                                   &f->error),
                   LC_OK);
  assert_int_equal(read.retention, 2060);
  assert_body(f, &read, "{}");
}

static void
test_invalid_retention_metadata_and_payload_references_fail(void **state) {
  fixture *f = *state;
  const char *snapshot = "snapshot-00000000000000000001.log";
  lc_pouch_collection_record original, metadata, other, read, failed;
  unsigned char saved[8], invalid[8], scratch[11];
  uint64_t end, deadline_offset;
  char output[512];
  struct stat st;
  open_case(f, 0, 0);
  assert_int_equal(append_json(f, "original", "{\"x\":1}", 1, 1, &original),
                   LC_OK);
  assert_int_equal(lc_pouch_collection_record_metadata(f->pouch, f->fd, leaf,
                                                       &original, 1, 2, 1200,
                                                       &metadata, &f->error),
                   LC_OK);
  end = file_end(f);
  for (uint64_t limit = metadata.record_offset; limit < end; limit += 7) {
    assert_int_equal(
        lc_pouch_collection_record_read(f->fd, metadata.record_offset, limit,
                                        leaf, &read, &f->error),
        LC_ERR_PROTOCOL);
    assert_false(read.published);
    clear_error(f);
  }
  /* Retention precedes the common twelve-byte index trailer; STATE_META has
   * no descriptor padding. Negative committed retention is invalid. */
  deadline_offset = metadata.next_offset - 12 - sizeof(saved);
  assert_int_equal(pread(f->fd, saved, sizeof(saved), (off_t)deadline_offset),
                   sizeof(saved));
  memset(invalid, 0xff, sizeof(invalid));
  assert_int_equal(
      pwrite(f->fd, invalid, sizeof(invalid), (off_t)deadline_offset),
      sizeof(invalid));
  assert_int_equal(lc_pouch_collection_record_read(f->fd,
                                                   metadata.record_offset, end,
                                                   leaf, &read, &f->error),
                   LC_ERR_PROTOCOL);
  assert_string_equal(f->error.server_code, "collection_corrupt");
  clear_error(f);
  assert_int_equal(pwrite(f->fd, saved, sizeof(saved), (off_t)deadline_offset),
                   sizeof(saved));
  assert_int_equal(lc_pouch_collection_record_read(f->fd,
                                                   metadata.record_offset, end,
                                                   leaf, &read, &f->error),
                   LC_OK);
  assert_int_equal(read.retention, 1200);
  assert_true(snprintf(output, sizeof(output), "%s/output", f->root) > 0);
  f->copy_fd = open(output, O_CREAT | O_EXCL | O_RDWR, 0600);
  assert_true(f->copy_fd >= 0);
  assert_int_equal(append_json(f, "other", "{\"x\":1}", 2, 3, &other), LC_OK);
  /* Equal body bytes cannot validate a reference to another identity. */
  unsigned char offset_bytes[8];
  for (unsigned int i = 0; i < sizeof(offset_bytes); ++i)
    offset_bytes[i] = (unsigned char)(other.record_offset >> (i * 8));
  uint64_t span_offset = metadata.record_offset + LC_POUCH_RECORD_HEADER_BYTES +
                         strlen(metadata.append_id) + 32;
  assert_int_equal(
      pwrite(f->fd, offset_bytes, sizeof(offset_bytes), (off_t)span_offset),
      sizeof(offset_bytes));
  for (unsigned int i = 0; i < sizeof(offset_bytes); ++i)
    offset_bytes[i] = (unsigned char)(other.payload_offset >> (i * 8));
  assert_int_equal(pwrite(f->fd, offset_bytes, sizeof(offset_bytes),
                          (off_t)(span_offset + 8)),
                   sizeof(offset_bytes));
  assert_int_equal(
      lc_pouch_collection_record_read(f->fd, metadata.record_offset,
                                      file_end(f), leaf, &read, &f->error),
      LC_OK);
  assert_int_equal(lc_pouch_collection_record_copy(
                       f->pouch, f->fd, f->fd, leaf, &read, f->copy_fd,
                       snapshot, scratch, sizeof(scratch), &failed, &f->error),
                   LC_ERR_PROTOCOL);
  assert_false(failed.published);
  assert_string_equal(f->error.server_code, "collection_corrupt");
  assert_int_equal(fstat(f->copy_fd, &st), 0);
  assert_int_equal(st.st_size, 0);
}

static void test_canonical_roundtrip_delete_and_durability(void **state) {
  fixture *f = *state;
  for (int mode = 0; mode < 4; ++mode) {
    for (int durable = 0; durable <= 1; ++durable) {
      lc_pouch_collection_record original, read, deleted, retry;
      uint64_t committed_end;
      open_case(f, mode, durable);
      assert_int_equal(append_json(f, "event_1",
                                   " { \"z\": [1, 2], \"s\": \"hello\" } ",
                                   (uint64_t)LC_I64_MAX + 3, 7, &original),
                       LC_OK);
      assert_true(original.published && !original.deleted);
      assert_true(original.bytes == strlen("{\"z\":[1,2],\"s\":\"hello\"}"));
      assert_int_equal(lc_pouch_fsync_commit(f->pouch, f->fd, &f->error),
                       LC_OK);
      committed_end = file_end(f);
      assert_true(committed_end == original.next_offset);
      assert_int_equal(lc_pouch_collection_record_read(f->fd, 0, committed_end,
                                                       leaf, &read, &f->error),
                       LC_OK);
      assert_true(read.position == original.position && read.published_at == 7);
      assert_true(read.generation == 7 && read.bytes == original.bytes);
      assert_string_equal(read.append_id, "event_1");
      assert_string_equal(read.etag, original.etag);
      assert_string_equal(read.descriptor, original.descriptor);
      assert_string_equal(read.context, original.context);
      assert_body(f, &read, "{\"z\":[1,2],\"s\":\"hello\"}");
      assert_int_equal(lc_pouch_collection_record_delete(
                           f->pouch, f->fd, &read, 12, &deleted, &f->error),
                       LC_OK);
      assert_true(deleted.published && deleted.deleted);
      assert_int_equal(lc_pouch_fsync_commit(f->pouch, f->fd, &f->error),
                       LC_OK);
      assert_true(deleted.record_offset == committed_end);
      assert_int_equal(
          lc_pouch_collection_record_read(f->fd, deleted.record_offset,
                                          file_end(f), leaf, &retry, &f->error),
          LC_OK);
      assert_true(retry.deleted && retry.published_at == 7 &&
                  retry.generation == 12);
      assert_true(retry.bytes == original.bytes &&
                  retry.position == original.position);
      assert_string_equal(retry.append_id, original.append_id);
      assert_string_equal(retry.etag, original.etag);
      assert_true(retry.stored_bytes == 0 && retry.descriptor[0] == 0);
      committed_end = file_end(f);
      assert_int_equal(lc_pouch_collection_record_delete(
                           f->pouch, f->fd, &retry, 13, &deleted, &f->error),
                       LC_OK);
      assert_true(file_end(f) == committed_end && deleted.generation == 12);
      /* A pinned reader still references the original immutable body. */
      assert_body(f, &original, "{\"z\":[1,2],\"s\":\"hello\"}");
    }
  }
}

static void test_failed_validation_rolls_back_only_new_record(void **state) {
  fixture *f = *state;
  for (int mode = 0; mode < 4; ++mode) {
    lc_pouch_collection_record original, failed, after;
    uint64_t committed_end;
    open_case(f, mode, 0);
    assert_int_equal(append_json(f, "first", "{\"n\":1}", 1, 1, &original),
                     LC_OK);
    committed_end = file_end(f);
    assert_int_equal(
        append_json(f, "second", "{\"n\":2} trailing", 2, 2, &failed),
        LC_ERR_INVALID);
    assert_false(failed.published);
    assert_true(file_end(f) == committed_end);
    clear_error(f);
    assert_int_equal(lc_pouch_collection_record_read(f->fd, 0, file_end(f),
                                                     leaf, &after, &f->error),
                     LC_OK);
    assert_string_equal(after.etag, original.etag);
    assert_int_equal(append_json(f, "second", "{\"n\":2}", 2, 2, &after),
                     LC_OK);
    assert_true(after.published && after.record_offset == committed_end);
    assert_body(f, &original, "{\"n\":1}");
    assert_body(f, &after, "{\"n\":2}");
  }
}

static void
test_truncated_corrupt_and_pending_records_are_explicit(void **state) {
  fixture *f = *state;
  lc_pouch_collection_record original, read;
  lc_pouch_record_header header;
  unsigned char prefix[LC_POUCH_RECORD_HEADER_BYTES];
  unsigned char changed;
  uint64_t end;
  open_case(f, 0, 0);
  assert_int_equal(append_json(f, "event", "{}", 1, 1, &original), LC_OK);
  end = file_end(f);
  for (uint64_t limit = 0; limit < end; limit += 7) {
    assert_int_equal(lc_pouch_collection_record_read(f->fd, 0, limit, leaf,
                                                     &read, &f->error),
                     LC_ERR_PROTOCOL);
    assert_false(read.published);
    clear_error(f);
  }
  assert_int_equal(pread(f->fd, prefix, sizeof(prefix), 0), sizeof(prefix));
  assert_int_equal(lc_pouch_record_header_decode(prefix, &header, &f->error),
                   LC_OK);
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  lc_pouch_record_header_encode(&header, prefix);
  assert_int_equal(pwrite(f->fd, prefix, sizeof(prefix), 0), sizeof(prefix));
  assert_int_equal(
      lc_pouch_collection_record_read(f->fd, 0, end, leaf, &read, &f->error),
      LC_ERR_PENDING);
  assert_false(read.published);
  clear_error(f);
  header.flags = 0;
  lc_pouch_record_header_encode(&header, prefix);
  assert_int_equal(pwrite(f->fd, prefix, sizeof(prefix), 0), sizeof(prefix));
  /* Corrupt an authoritative payload reference, not only the outer checksum. */
  assert_int_equal(pread(f->fd, &changed, 1, 32 + header.key_len + 40), 1);
  changed ^= 1;
  assert_int_equal(pwrite(f->fd, &changed, 1, 32 + header.key_len + 40), 1);
  assert_int_equal(
      lc_pouch_collection_record_read(f->fd, 0, end, leaf, &read, &f->error),
      LC_ERR_PROTOCOL);
  assert_string_equal(f->error.server_code, "collection_corrupt");
  assert_false(read.published);
}

static void test_relocation_keeps_raw_payload_and_all_bindings(void **state) {
  fixture *f = *state;
  const char *snapshot = "snapshot-00000000000000000001.log";
  const char *json = "{\"escaped\":\"x\\ny\\u0020\",\"n\":1e+02}";
  /* Initial JSON validation normalizes escapes; relocation must preserve
   * those already stored bytes, not recreate the original upload syntax. */
  const char *stored = "{\"escaped\":\"x\\ny \",\"n\":1e+02}";
  char destination[512];
  unsigned char buffer[17], before[17], after[17];
  unsigned char timestamp[16], copied_timestamp[16];
  lc_pouch_collection_record original, copied, read, deleted, copied_delete,
      roundtrip;
  assert_true(snprintf(destination, sizeof(destination), "%s/%s", f->root,
                       snapshot) > 0);
  for (int mode = 0; mode < 4; ++mode) {
    for (int durable = 0; durable < 2; ++durable) {
      open_case(f, mode, durable);
      if (f->copy_fd >= 0)
        close(f->copy_fd);
      f->copy_fd = open(destination, O_RDWR | O_CREAT | O_TRUNC, 0600);
      assert_true(f->copy_fd >= 0);
      assert_int_equal(append_json(f, "relocate_1", json,
                                   (uint64_t)INT64_MAX + 40U, 7U, &original),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_copy(
                           f->pouch, f->fd, f->fd, leaf, &original, f->copy_fd,
                           snapshot, buffer, sizeof(buffer), &copied,
                           &f->error),
                       LC_OK);
      assert_true(copied.published && !copied.deleted &&
                  copied.position == original.position &&
                  copied.published_at == original.published_at &&
                  copied.generation == original.generation &&
                  copied.bytes == original.bytes);
      assert_string_equal(copied.append_id, original.append_id);
      assert_string_equal(copied.etag, original.etag);
      assert_string_equal(copied.context, original.context);
      assert_string_equal(copied.descriptor, original.descriptor);
      assert_int_equal(pread(f->fd, timestamp, sizeof(timestamp),
                             (off_t)(original.record_offset + 32U +
                                     strlen(original.append_id))),
                       sizeof(timestamp));
      assert_int_equal(
          pread(f->copy_fd, copied_timestamp, sizeof(copied_timestamp),
                (off_t)(copied.record_offset + 32U + strlen(copied.append_id))),
          sizeof(copied_timestamp));
      assert_memory_equal(timestamp, copied_timestamp, sizeof(timestamp));
      for (uint64_t offset = 0U; offset < original.stored_bytes;) {
        size_t size = original.stored_bytes - offset < sizeof(before)
                          ? (size_t)(original.stored_bytes - offset)
                          : sizeof(before);
        assert_int_equal(pread(f->fd, before, size,
                               (off_t)(original.payload_offset + offset)),
                         size);
        assert_int_equal(pread(f->copy_fd, after, size,
                               (off_t)(copied.payload_offset + offset)),
                         size);
        assert_memory_equal(before, after, size);
        offset += size;
      }
      assert_int_equal(
          lc_pouch_collection_record_read(f->copy_fd, 0U, copied.next_offset,
                                          snapshot, &read, &f->error),
          LC_OK);
      assert_true(read.payload_offset == copied.payload_offset &&
                  read.bytes == original.bytes);
      /* Also shrink a snapshot container back to a segment, at a nonzero
       * output offset. Neither direction changes the logical body context. */
      assert_int_equal(lc_pouch_collection_record_copy(
                           f->pouch, f->copy_fd, f->copy_fd, snapshot, &read,
                           f->fd, leaf, buffer, sizeof(buffer), &roundtrip,
                           &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_read(
                           f->fd, roundtrip.record_offset,
                           roundtrip.next_offset, leaf, &read, &f->error),
                       LC_OK);
      assert_true(read.record_offset == original.next_offset &&
                  read.position == original.position);
      assert_string_equal(read.context, original.context);
      assert_int_equal(lc_pouch_crypto_source_from_fd_span(
                           f->pouch->crypto, copied.context, copied.context,
                           dup(f->copy_fd), copied.payload_offset,
                           copied.stored_bytes, copied.descriptor, &f->decoded,
                           &f->error),
                       LC_OK);
      assert_int_equal(lc_sink_to_memory(&f->sink, &f->error), LC_OK);
      size_t written, length;
      const void *bytes;
      assert_int_equal(lc_copy(f->decoded, f->sink, &written, &f->error),
                       LC_OK);
      assert_int_equal(
          lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error), LC_OK);
      assert_int_equal(length, strlen(stored));
      assert_memory_equal(bytes, stored, length);
      lc_source_close(f->decoded);
      f->decoded = NULL;
      lc_sink_close(f->sink);
      f->sink = NULL;
      assert_int_equal(lc_pouch_collection_record_delete(f->pouch, f->fd,
                                                         &original, 12U,
                                                         &deleted, &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_copy(
                           f->pouch, f->fd, f->fd, leaf, &deleted, f->copy_fd,
                           snapshot, buffer, sizeof(buffer), &copied_delete,
                           &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_record_read(
                           f->copy_fd, copied_delete.record_offset,
                           copied_delete.next_offset, snapshot, &read,
                           &f->error),
                       LC_OK);
      assert_true(read.deleted && read.position == original.position &&
                  read.published_at == 7U && read.generation == 12U &&
                  read.bytes == original.bytes && read.stored_bytes == 0U);
      assert_string_equal(read.append_id, original.append_id);
      assert_string_equal(read.etag, original.etag);
    }
  }
}

static void test_failed_relocation_preserves_prior_output(void **state) {
  fixture *f = *state;
  const char *snapshot = "snapshot-00000000000000000001.log";
  char destination[512];
  unsigned char buffer[7], changed, original_byte;
  lc_pouch_collection_record original, copied, failed;
  struct stat st;
  open_case(f, 0, 0);
  assert_true(snprintf(destination, sizeof(destination), "%s/%s", f->root,
                       snapshot) > 0);
  f->copy_fd = open(destination, O_RDWR | O_CREAT | O_EXCL, 0600);
  assert_true(f->copy_fd >= 0);
  assert_int_equal(
      append_json(f, "relocate_1", "{\"v\":123456789}", 1U, 1U, &original),
      LC_OK);
  assert_int_equal(lc_pouch_collection_record_copy(
                       f->pouch, f->fd, f->fd, leaf, &original, f->copy_fd,
                       snapshot, buffer, sizeof(buffer), &copied, &f->error),
                   LC_OK);
  assert_int_equal(
      pread(f->fd, &original_byte, 1U, (off_t)original.payload_offset), 1);
  changed = original_byte ^ 1U;
  assert_int_equal(pwrite(f->fd, &changed, 1U, (off_t)original.payload_offset),
                   1);
  assert_int_equal(lc_pouch_collection_record_copy(
                       f->pouch, f->fd, f->fd, leaf, &original, f->copy_fd,
                       snapshot, buffer, sizeof(buffer), &failed, &f->error),
                   LC_ERR_PROTOCOL);
  assert_false(failed.published);
  assert_string_equal(f->error.server_code, "collection_corrupt");
  assert_int_equal(fstat(f->copy_fd, &st), 0);
  assert_true((uint64_t)st.st_size == copied.next_offset);
  clear_error(f);
  assert_int_equal(
      pwrite(f->fd, &original_byte, 1U, (off_t)original.payload_offset), 1);
  assert_int_equal(ftruncate(f->fd, (off_t)(original.next_offset - 1U)), 0);
  assert_int_equal(lc_pouch_collection_record_copy(
                       f->pouch, f->fd, f->fd, leaf, &original, f->copy_fd,
                       snapshot, buffer, sizeof(buffer), &failed, &f->error),
                   LC_ERR_PROTOCOL);
  assert_false(failed.published);
  assert_int_equal(fstat(f->copy_fd, &st), 0);
  assert_true((uint64_t)st.st_size == copied.next_offset);
  assert_true(fcntl(f->fd, F_GETFD) >= 0 && fcntl(f->copy_fd, F_GETFD) >= 0);
}

typedef struct generated_json {
  size_t offset;
  size_t string_bytes;
} generated_json;

static size_t generated_read(void *context, void *buffer, size_t capacity,
                             lc_error *error) {
  generated_json *source = context;
  unsigned char *bytes = buffer;
  const char prefix[] = "{\"s\":\"";
  const char suffix[] = "\"}";
  size_t total =
      sizeof(prefix) - 1U + source->string_bytes + sizeof(suffix) - 1U;
  size_t count =
      total - source->offset < capacity ? total - source->offset : capacity;
  (void)error;
  for (size_t i = 0U; i < count; ++i) {
    size_t offset = source->offset + i;
    bytes[i] =
        offset < sizeof(prefix) - 1U ? (unsigned char)prefix[offset]
        : offset < sizeof(prefix) - 1U + source->string_bytes
            ? 'x'
            : (unsigned char)
                  suffix[offset - (sizeof(prefix) - 1U + source->string_bytes)];
  }
  source->offset += count;
  return count;
}

static void test_relocation_crosses_large_buffer_boundaries(void **state) {
  fixture *f = *state;
  const char *snapshot = "snapshot-00000000000000000001.log";
  char path[512];
  generated_json source = {0U, 3U * 128U * 1024U + 137U};
  lc_pouch_collection_record original, copied;
  const size_t capacity = 128U * 1024U;
  unsigned char *buffer = malloc(capacity + 64U);
  unsigned char *compare = malloc(capacity);
  assert_non_null(buffer);
  assert_non_null(compare);
  memset(buffer + capacity, 0xa5, 64U);
  open_case(f, 0, 0);
  assert_int_equal(lc_source_from_callbacks(generated_read, NULL, NULL, &source,
                                            &f->source, &f->error),
                   LC_OK);
  assert_int_equal(lc_pouch_collection_record_append(
                       f->pouch, f->fd, leaf, f->id, "large", 1U, 1U, 0, 0, 0,
                       f->source, &original, &f->error),
                   LC_OK);
  lc_source_close(f->source);
  f->source = NULL;
  assert_true(original.bytes == source.string_bytes + 8U);
  assert_true(snprintf(path, sizeof(path), "%s/%s", f->root, snapshot) > 0);
  f->copy_fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
  assert_true(f->copy_fd >= 0);
  assert_int_equal(lc_pouch_collection_record_copy(
                       f->pouch, f->fd, f->fd, leaf, &original, f->copy_fd,
                       snapshot, buffer, capacity, &copied, &f->error),
                   LC_OK);
  for (size_t i = 0U; i < 64U; ++i)
    assert_int_equal(buffer[capacity + i], 0xa5);
  for (uint64_t offset = 0U; offset < original.stored_bytes;) {
    size_t size = original.stored_bytes - offset < capacity
                      ? (size_t)(original.stored_bytes - offset)
                      : capacity;
    assert_int_equal(
        pread(f->fd, buffer, size, (off_t)(original.payload_offset + offset)),
        size);
    assert_int_equal(pread(f->copy_fd, compare, size,
                           (off_t)(copied.payload_offset + offset)),
                     size);
    assert_memory_equal(buffer, compare, size);
    offset += size;
  }
  free(buffer);
  free(compare);
}

static void
test_stream_publication_is_metadata_only_and_rolls_back(void **state) {
  fixture *f = *state;
  lc_pouch_collection_writer *writer = NULL;
  lc_pouch_collection_record body, published, prior, read;
  const void *encoded;
  size_t length;
  uint64_t end, extent_offset;
  for (int mode = 0; mode < 4; ++mode) {
    for (int failure = 0; failure < 2; ++failure) {
      open_case(f, mode, 0);
      assert_int_equal(append_json(f, "prior", "{}", 1, 1, &prior), LC_OK);
      end = file_end(f);
      assert_int_equal(lc_sink_to_memory(&f->sink, &f->error), LC_OK);
      char context[256], binding[256];
      lc_pouch_crypto_writer *codec = NULL;
      lc_collection_unit unit = {0};
      char *descriptor = NULL, *rebound = NULL;
      uint64_t plain_bytes, stored_bytes;
      unsigned long stored_crc;
      snprintf(context, sizeof(context), "collection:%s:streamed:upload",
               f->id);
      assert_int_equal(lc_pouch_crypto_writer_open_sink(f->pouch->crypto,
                                                        context, f->sink,
                                                        &codec, &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_writer_open_sink(
                           f->pouch, lc_pouch_crypto_writer_sink(codec), f->id,
                           "streamed", context, 0, 0, 0, &writer, &f->error),
                       LC_OK);
      assert_int_equal(lc_pouch_collection_writer_write(writer, "{ \"n\" : 2 }",
                                                        11, &f->error),
                       LC_OK);
      assert_int_equal(
          lc_pouch_collection_writer_finish(writer, &body, &f->error), LC_OK);
      assert_int_equal(lc_pouch_crypto_writer_finish(codec, &plain_bytes,
                                                     &stored_bytes, &stored_crc,
                                                     &descriptor, &f->error),
                       LC_OK);
      lc_pouch_crypto_writer_close(codec);
      body.position = body.published_at = 2;
      body.stream_extents = 1;
      body.stored_bytes = stored_bytes;
      body.stored_crc = 0;
      assert_int_equal(lc_collection_unit_manifest(&body, f->id, NULL, binding,
                                                   body.manifest, &f->error),
                       LC_OK);
      if (descriptor && descriptor[0])
        assert_int_equal(lc_pouch_crypto_rebind_descriptor(
                             f->pouch->crypto, descriptor, context, binding,
                             &rebound, &f->error),
                         LC_OK);
      if (rebound)
        strcpy(body.descriptor, rebound);
      unit.plain = plain_bytes;
      unit.stored = stored_bytes;
      unit.crc = stored_crc;
      strcpy(unit.context, context);
      if (descriptor)
        strcpy(unit.descriptor, descriptor);
      lc_free_with_allocator(&f->pouch->allocator, descriptor);
      lc_free_with_allocator(&f->pouch->allocator, rebound);
      assert_int_equal(
          lc_pouch_collection_writer_storage(writer, &body, &f->error), LC_OK);
      assert_int_equal(
          lc_sink_memory_bytes(f->sink, &encoded, &length, &f->error), LC_OK);
      assert_int_equal(length, body.stored_bytes);
      unsigned char prefix[LC_COLLECTION_UNIT_HEADER] = {0};
      extent_offset = end;
      assert_int_equal(lseek(f->fd, 0, SEEK_END), end);
      assert_int_equal(write(f->fd, prefix, sizeof(prefix)), sizeof(prefix));
      assert_int_equal(write(f->fd, encoded, length), length);
      assert_int_equal(
          lc_collection_unit_write_meta(f->fd, extent_offset, &unit, &f->error),
          LC_OK);
      end = file_end(f);
      if (failure) {
        f->clock_failed = 1;
        lc_pouch_test_queue_clock_gettime = record_clock;
        lc_pouch_test_queue_time_context = f;
      }
      int rc = lc_pouch_collection_writer_publish_stream(
          writer, f->fd, leaf, 1, extent_offset, 1, length, 0, 2, 2, &published,
          &f->error);
      f->clock_failed = 0;
      lc_pouch_test_queue_clock_gettime = NULL;
      lc_pouch_test_queue_time_context = NULL;
      assert_int_equal(rc, failure ? LC_ERR_TRANSPORT : LC_OK);
      if (!failure) {
        assert_int_equal(lc_pouch_collection_record_read(
                             f->fd, end, file_end(f), leaf, &read, &f->error),
                         LC_OK);
        assert_int_equal(read.stream_extents, 1);
        assert_int_equal(read.stream_offset, extent_offset);
        assert_int_equal(read.stored_bytes, length);
        assert_int_equal(read.next_offset, read.payload_offset);
        assert_string_equal(read.etag, body.etag);
        /* Metadata corruption must fail even while the outer header CRC and
         * the referenced payload remain valid. */
        unsigned char metadata[1024];
        size_t at = 0;
        off_t meta_offset = (off_t)(end + 32U + strlen(read.append_id));
        assert_true(read.metadata_length <= sizeof(metadata));
        assert_int_equal(
            pread(f->fd, metadata, read.metadata_length, meta_offset),
            read.metadata_length);
        while (at + sizeof(read.manifest) <= read.metadata_length &&
               memcmp(metadata + at, read.manifest, sizeof(read.manifest)))
          ++at;
        assert_true(at + sizeof(read.manifest) <= read.metadata_length);
        unsigned char original = metadata[at];
        metadata[at] ^= 1U;
        assert_int_equal(pwrite(f->fd, metadata + at, 1, meta_offset + at), 1);
        assert_int_equal(lc_pouch_collection_record_read(
                             f->fd, end, file_end(f), leaf, &read, &f->error),
                         LC_ERR_PROTOCOL);
        clear_error(f);
        assert_int_equal(pwrite(f->fd, &original, 1, meta_offset + at), 1);
        assert_int_equal(lc_pouch_collection_record_read(
                             f->fd, end, file_end(f), leaf, &read, &f->error),
                         LC_OK);
      }
      lc_pouch_collection_writer_close(writer);
      writer = NULL;
      if (failure) {
        assert_int_equal(file_end(f), end);
        clear_error(f);
      }
      lc_sink_close(f->sink);
      f->sink = NULL;
      assert_body(f, &prior, "{}");
    }
  }
}

static void test_publication_reuses_reserved_metadata(void **state) {
  fixture *f = *state;
  lc_pouch_collection_writer *writer = NULL;
  lc_pouch_collection_record record, replayed;
  char binding[256];
  int rc;
  snprintf(binding, sizeof(binding), "collection:%s:reserved:1", f->id);
  f->custom_allocator = 1;
  f->allocator.malloc_fn = record_alloc;
  f->allocator.realloc_fn = record_resize;
  f->allocator.free_fn = record_free;
  f->allocator.context = f;
  for (int mode = 0; mode < 4; ++mode) {
    for (int deferred = 0; deferred < 2; ++deferred) {
      open_case(f, mode, 0);
      assert_int_equal(lc_pouch_collection_writer_open(
                           f->pouch, f->fd, leaf, f->id, "reserved",
                           deferred ? 0 : 1, deferred ? 0 : 1, -60, 1, -120,
                           &writer, &f->error),
                       LC_OK);
      assert_int_equal(
          lc_source_from_memory("{\"small\":1}", 11, &f->source, &f->error),
          LC_OK);
      assert_int_equal(lc_pouch_collection_writer_consume(
                           writer, f->pouch, f->source, &record, &f->error),
                       LC_OK);
      lc_source_close(f->source);
      f->source = NULL;
      f->rejected_size = record.metadata_length;
      f->reject_metadata = 1;
      rc = lc_pouch_collection_writer_publish(writer, leaf, 1, 1, &record,
                                              &f->error);
      f->reject_metadata = 0;
      lc_pouch_collection_writer_close(writer);
      writer = NULL;
      assert_int_equal(rc, LC_OK);
      assert_int_equal(f->rejected, 0);
      assert_int_equal(lc_pouch_collection_record_read(
                           f->fd, 0, file_end(f), leaf, &replayed, &f->error),
                       LC_OK);
      assert_body_context(f, &replayed, binding, "{\"small\":1}");
      assert_true(replayed.retention > 0 &&
                  replayed.collection_retention > replayed.retention);
      assert_string_equal(replayed.etag, record.etag);
    }
  }
}

static void test_only_pending_extent_tails_are_discardable(void **state) {
  fixture *f = *state;
  lc_pouch_record_header header = {0};
  lc_pouch_collection_record record;
  unsigned char bytes[64] = {0};
  open_case(f, 0, 0);
  for (int node = 0; node < 2; ++node) {
    assert_int_equal(ftruncate(f->fd, 0), 0);
    assert_int_equal(lseek(f->fd, 0, SEEK_SET), 0);
    header.type = node ? LC_POUCH_COLLECTION_NODE : LC_POUCH_COLLECTION_CHUNK;
    header.flags = 0;
    header.meta_len = node ? 8 : 0;
    header.payload_len = node ? 24 : 16;
    lc_pouch_record_header_encode(&header, bytes);
    size_t n = 32 + header.meta_len + header.payload_len - 1;
    assert_int_equal(write(f->fd, bytes, n), n);
    assert_int_equal(
        lc_pouch_collection_record_read(f->fd, 0, n, leaf, &record, &f->error),
        LC_ERR_PROTOCOL);
    clear_error(f);
    header.flags = LC_POUCH_RECORD_FLAG_PENDING;
    lc_pouch_record_header_encode(&header, bytes);
    assert_int_equal(pwrite(f->fd, bytes, 32, 0), 32);
    assert_int_equal(
        lc_pouch_collection_record_read(f->fd, 0, n, leaf, &record, &f->error),
        LC_ERR_PENDING);
    clear_error(f);
  }
}

static void test_incomplete_allocation_is_not_a_published_entry(void **state) {
  fixture *f = *state;
  lc_pouch_record_header header = {0};
  lc_pouch_collection_record record;
  unsigned char prefix[32];
  open_case(f, 0, 0);
  header.type = LC_POUCH_COLLECTION_REGION;
  header.payload_len = LC_COLLECTION_REGION_MAX;
  lc_pouch_record_header_encode(&header, prefix);
  assert_int_equal(write(f->fd, prefix, sizeof(prefix)), sizeof(prefix));
  /* A short allocation may be retired by active-tail recovery. Its payload
   * prefix is never a published document; finalized document/chunk truncation
   * remains an error in test_only_pending_extent_tails_are_discardable. */
  assert_int_equal(lc_pouch_collection_record_read(f->fd, 0, sizeof(prefix),
                                                   leaf, &record, &f->error),
                   LC_ERR_PENDING);
  clear_error(f);
  assert_int_equal(ftruncate(f->fd, 32U + header.payload_len), 0);
  assert_int_equal(lc_pouch_collection_record_read(f->fd, 0,
                                                   32U + header.payload_len,
                                                   leaf, &record, &f->error),
                   LC_OK);
  assert_int_equal(record.kind, LC_POUCH_COLLECTION_REGION);
  assert_int_equal(record.next_offset, 32U + header.payload_len);
  /* The address reservation has bounded framing headroom, separate from the
   * encoded-unit limit. Reject even one byte beyond that reservation bound. */
  ++header.payload_len;
  lc_pouch_record_header_encode(&header, prefix);
  assert_int_equal(pwrite(f->fd, prefix, sizeof(prefix), 0), sizeof(prefix));
  assert_int_equal(ftruncate(f->fd, 32U + header.payload_len), 0);
  assert_int_equal(lc_pouch_collection_record_read(f->fd, 0,
                                                   32U + header.payload_len,
                                                   leaf, &record, &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
}

static void
test_manifest_reuse_preserves_identity_and_relocation(void **state) {
  fixture *f = *state;
  const char *collection = "00000000000000000001";
  const char *golden =
      "collection-manifest:"
      "af809ec51560989e40213ea5fcc5483b81bada9098d0ae491738267347a874d2";
  lc_pouch_collection_record *records =
      lc_calloc_with_allocator(NULL, 2, sizeof(*records));
  EVP_MD_CTX *hash = NULL;
  char once[256], reused[256];
  assert_non_null(records);
  records[0].position = 7;
  records[0].published_at = 9;
  records[0].bytes = 2;
  records[0].stream_extents = 1;
  strcpy(records[0].append_id, "event-1");
  strcpy(records[0].context, "collection:00000000000000000001:event-1:upload");
  strcpy(records[0].etag,
         "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a");
  assert_int_equal(lc_collection_unit_binding(&records[0], collection, &hash,
                                              reused, &f->error),
                   LC_OK);
  assert_string_equal(reused, golden);
  records[0].generation = 99;
  records[0].retention = 100;
  records[0].collection_retention = 200;
  records[0].stored_bytes = 300;
  records[0].stream_segment = 4;
  records[0].stream_offset = 500;
  assert_int_equal(lc_collection_unit_binding(&records[0], collection, &hash,
                                              reused, &f->error),
                   LC_OK);
  assert_string_equal(reused, golden);
  for (int part = 0; part < 8; ++part) {
    const char *id = collection;
    records[1] = records[0];
    switch (part) {
    case 0:
      ++records[1].position;
      break;
    case 1:
      ++records[1].published_at;
      break;
    case 2:
      ++records[1].bytes;
      break;
    case 3:
      ++records[1].stream_extents;
      break;
    case 4:
      strcpy(records[1].append_id, "event-2");
      break;
    case 5:
      strcat(records[1].context, ":1");
      break;
    case 6:
      records[1].etag[0] = '5';
      break;
    case 7:
      id = "00000000000000000002";
      break;
    }
    assert_int_equal(
        lc_collection_unit_binding(&records[1], id, NULL, once, &f->error),
        LC_OK);
    assert_int_equal(
        lc_collection_unit_binding(&records[1], id, &hash, reused, &f->error),
        LC_OK);
    assert_string_equal(reused, once);
    assert_true(strcmp(reused, golden) != 0);
  }
  records[1].position = 0;
  assert_int_equal(lc_collection_unit_binding(&records[1], collection, &hash,
                                              reused, &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
  assert_int_equal(lc_collection_unit_binding(&records[0], collection, &hash,
                                              reused, &f->error),
                   LC_OK);
  assert_string_equal(reused, golden);
  EVP_MD_CTX_free(hash);
  lc_free_with_allocator(NULL, records);
}

static unsigned int prefix_calls, prefix_fail_at;
static ssize_t prefix_short_read(int fd, const struct iovec *parts, int count,
                                 off_t offset) {
  size_t length;
  assert_true(count > 0 && count <= 2);
  ++prefix_calls;
  if (prefix_calls == 1) {
    errno = EINTR;
    return -1;
  }
  if (prefix_calls == prefix_fail_at) {
    errno = EIO;
    return -1;
  }
  length = parts[0].iov_len;
  if (length > 17)
    length = 17;
  return pread(fd, parts[0].iov_base, length, offset);
}

static void test_unit_prefix_partial_reads_and_failures(void **state) {
  fixture *f = *state;
  lc_collection_unit unit = {0}, decoded;
  unsigned char body[512], prefix[128];
  const off_t start = 37;
  size_t i;
  open_case(f, 0, 0);
  for (i = 0; i < sizeof(body); ++i)
    body[i] = (unsigned char)(i * 17U);
  unit.plain = unit.stored = sizeof(body);
  strcpy(unit.context, "collection:test:event:upload");
  memset(unit.etag, 'a', 64);
  assert_int_equal(
      pwrite(f->fd, body, sizeof(body), start + LC_COLLECTION_UNIT_HEADER),
      sizeof(body));
  assert_int_equal(
      lc_collection_unit_write_meta(f->fd, start, &unit, &f->error), LC_OK);
  assert_int_equal(lseek(f->fd, 11, SEEK_SET), 11);
  prefix_calls = prefix_fail_at = 0;
  lc_collection_unit_test_preadv = prefix_short_read;
  assert_int_equal(lc_collection_unit_read_prefix(f->fd, start, sizeof(body),
                                                  &decoded, prefix,
                                                  sizeof(prefix), &f->error),
                   LC_OK);
  assert_true(prefix_calls > 2);
  assert_memory_equal(prefix, body, sizeof(prefix));
  assert_string_equal(decoded.etag, unit.etag);
  assert_string_equal(decoded.context, unit.context);
  assert_int_equal(lseek(f->fd, 0, SEEK_CUR), 11);
  prefix_calls = 0;
  prefix_fail_at = 7;
  assert_int_equal(lc_collection_unit_read_prefix(f->fd, start, sizeof(body),
                                                  &decoded, prefix,
                                                  sizeof(prefix), &f->error),
                   LC_ERR_TRANSPORT);
  clear_error(f);
  lc_collection_unit_test_preadv = NULL;
  /* A readable metadata header cannot hide a truncated first input chunk. */
  assert_int_equal(
      ftruncate(f->fd, start + LC_COLLECTION_UNIT_HEADER + sizeof(prefix) - 1),
      0);
  assert_int_equal(lc_collection_unit_read_prefix(f->fd, start, sizeof(body),
                                                  &decoded, prefix,
                                                  sizeof(prefix), &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
  assert_int_equal(lc_collection_unit_read_prefix(f->fd, start, sizeof(body),
                                                  &decoded, prefix,
                                                  sizeof(body) + 1U, &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
  assert_int_equal(lc_collection_unit_read_prefix(
                       f->fd, (lc_u64)LC_I64_MAX, sizeof(body), &decoded,
                       prefix, sizeof(prefix), &f->error),
                   LC_ERR_PROTOCOL);
}

static void test_unit_metadata_lengths_and_maximum_text(void **state) {
  fixture *f = *state;
  lc_collection_unit unit = {0};
  open_case(f, 0, 0);
  unit.plain = unit.stored = 1;
  memset(unit.descriptor, 'd', sizeof(unit.descriptor) - 1);
  memset(unit.context, 'c', sizeof(unit.context) - 1);
  memset(unit.etag, 'a', sizeof(unit.etag) - 2);
  assert_int_equal(lc_collection_unit_write_meta(f->fd, 0, &unit, &f->error),
                   LC_ERR_PROTOCOL);
  assert_int_equal(file_end(f), 0);
  clear_error(f);
  unit.etag[63] = 'a';
  assert_int_equal(pwrite(f->fd, "x", 1, LC_COLLECTION_UNIT_HEADER), 1);
  assert_int_equal(lc_collection_unit_write_meta(f->fd, 0, &unit, &f->error),
                   LC_OK);
  lc_collection_unit decoded;
  assert_int_equal(lc_collection_unit_read(f->fd, 0, 1, &decoded, &f->error),
                   LC_OK);
  assert_string_equal(decoded.etag, unit.etag);
  assert_string_equal(decoded.context, unit.context);
  assert_string_equal(decoded.descriptor, unit.descriptor);
}

static void
test_contiguous_prefix_validates_metadata_and_truncation(void **state) {
  fixture *f = *state;
  const char *json =
      "{\"event\":\"bounded prefix with no retained body buffer\"}";
  lc_pouch_collection_record written, ordinary, prefixed, alias;
  unsigned char prefix[256], expected[256], zero = 0;
  size_t size;
  for (int mode = 0; mode < 4; ++mode) {
    open_case(f, mode, 0);
    assert_int_equal(append_json(f, "prefix", json, 1, 1, &written), LC_OK);
    uint64_t end = file_end(f);
    assert_int_equal(lc_pouch_collection_record_read(f->fd, 0, end, leaf,
                                                     &ordinary, &f->error),
                     LC_OK);
    assert_int_equal(lseek(f->fd, 7, SEEK_SET), 7);
    for (size_t capacity = 0; capacity <= sizeof(prefix); capacity += 7) {
      size_t wanted = written.stored_bytes < capacity
                          ? (size_t)written.stored_bytes
                          : capacity;
      assert_int_equal(lc_pouch_collection_record_read_prefix(
                           f->fd, 0, end, leaf, &prefixed, prefix, capacity,
                           &size, &f->error),
                       LC_OK);
      assert_int_equal(size, wanted);
      assert_memory_equal(&prefixed, &ordinary, sizeof(ordinary));
      assert_int_equal(pread(f->fd, expected, wanted, written.payload_offset),
                       wanted);
      assert_memory_equal(prefix, expected, wanted);
      assert_int_equal(lseek(f->fd, 0, SEEK_CUR), 7);
    }
    assert_int_equal(pwrite(f->fd, &zero, 1, 32), 1);
    size = 99;
    assert_int_equal(lc_pouch_collection_record_read_prefix(
                         f->fd, 0, end, leaf, &prefixed, prefix, sizeof(prefix),
                         &size, &f->error),
                     LC_ERR_PROTOCOL);
    assert_int_equal(size, 0);
    clear_error(f);
    assert_int_equal(pwrite(f->fd, "p", 1, 32), 1);
    assert_int_equal(lc_pouch_collection_record_metadata(f->pouch, f->fd, leaf,
                                                         &written, 1, 2, 0,
                                                         &alias, &f->error),
                     LC_OK);
    size = 99;
    assert_int_equal(lc_pouch_collection_record_read_prefix(
                         f->fd, alias.record_offset, file_end(f), leaf,
                         &prefixed, prefix, sizeof(prefix), &size, &f->error),
                     LC_OK);
    assert_true(prefixed.metadata_only);
    assert_int_equal(size, 0);
    /* A stale captured end must not let a partial vector read expose a
     * prefix from a truncated published body as a successful record. */
    assert_int_equal(ftruncate(f->fd, (off_t)end - 1), 0);
    size = 99;
    assert_int_equal(lc_pouch_collection_record_read_prefix(
                         f->fd, 0, end, leaf, &prefixed, prefix, sizeof(prefix),
                         &size, &f->error),
                     LC_ERR_PROTOCOL);
    assert_int_equal(size, 0);
    clear_error(f);
  }
}

static void test_chunk_cleanup_releases_owned_descriptors_once(void **state) {
  fixture *f = *state;
  lc_collection_chunks *chunks = calloc(1, sizeof(*chunks));
  int first, pending;
  assert_non_null(chunks);
  open_case(f, 0, 0);
  /* Zero initialization must not acquire ownership of descriptor zero. */
  lc_collection_chunks_close(chunks);
  first = dup(f->fd);
  pending = dup(f->fd);
  assert_true(first >= 0 && pending >= 0);
  chunks->first_fd = first;
  chunks->first_fd_owned = 1;
  chunks->pending.segment = 2;
  chunks->region_fd = pending;
  lc_collection_chunks_close(chunks);
  assert_int_equal(fcntl(first, F_GETFD), -1);
  assert_int_equal(errno, EBADF);
  assert_int_equal(fcntl(pending, F_GETFD), -1);
  assert_int_equal(errno, EBADF);
  /* A repeated close must not close a descriptor recycled by the caller. */
  f->copy_fd = dup(f->fd);
  assert_true(f->copy_fd >= 0);
  lc_collection_chunks_close(chunks);
  assert_true(fcntl(f->copy_fd, F_GETFD) >= 0);
  free(chunks);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_contiguous_prefix_validates_metadata_and_truncation, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_chunk_cleanup_releases_owned_descriptors_once, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_unit_prefix_partial_reads_and_failures, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_unit_metadata_lengths_and_maximum_text, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_manifest_reuse_preserves_identity_and_relocation, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_incomplete_allocation_is_not_a_published_entry, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_only_pending_extent_tails_are_discardable, setup, teardown),
      cmocka_unit_test_setup_teardown(test_publication_reuses_reserved_metadata,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_stream_publication_is_metadata_only_and_rolls_back, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_invalid_retention_metadata_and_payload_references_fail, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_metadata_references_body_and_compacts, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_uses_publication_clock_and_rolls_back, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_canonical_roundtrip_delete_and_durability, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_validation_rolls_back_only_new_record, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_truncated_corrupt_and_pending_records_are_explicit, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_relocation_keeps_raw_payload_and_all_bindings, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_relocation_preserves_prior_output, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_relocation_crosses_large_buffer_boundaries, setup, teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
