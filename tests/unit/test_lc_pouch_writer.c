#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_crypto.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/pouch-writer-"
#define FRAME (64U * 1024U)
#define BODY (4096U + 2U * FRAME + 19U)
#define CONTEXT "writer:independent-frames"

typedef struct allocation {
  void *ptr;
  size_t size;
} allocation;

typedef struct unit_record {
  uint64_t offset, bytes, stored;
  unsigned long crc;
  char descriptor[256], context[64];
} unit_record;

typedef struct fixture {
  allocation slots[32];
  size_t calls, fail_at, live, peak, large, peak_large, wiped;
  int check_wipe, dirty_free, invalid_free;
  lc_allocator allocator;
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_writer *writer;
  unit_record *units;
  lc_error error;
  lc_sink sink;
  char root[256], *key, *descriptor, *reference_descriptor;
  unsigned char *body, *reference, chunk[4093];
  uint64_t plain, stored;
  unsigned long crc;
  size_t sink_calls, reject_call, partial;
  size_t source_calls;
  int sink_error, fd, rc;
} fixture;

static size_t allocation_slot(fixture *f, void *ptr) {
  size_t i;
  for (i = 0; i < 32; ++i)
    if (f->slots[i].ptr == ptr)
      return i;
  return 32;
}

static void *resize(void *context, void *ptr, size_t size) {
  fixture *f = context;
  size_t slot = allocation_slot(f, ptr), old;
  void *out;
  int fresh = ptr == NULL;
  if (++f->calls == f->fail_at || slot == 32)
    return NULL;
  old = ptr != NULL ? f->slots[slot].size : 0;
  out = realloc(ptr, size);
  if (out == NULL)
    return NULL;
  if (fresh)
    memset(out, 0xa5, size);
  f->live = f->live - old + size;
  if (old >= FRAME)
    --f->large;
  if (size >= FRAME)
    ++f->large;
  if (f->live > f->peak)
    f->peak = f->live;
  if (f->large > f->peak_large)
    f->peak_large = f->large;
  f->slots[slot].ptr = out;
  f->slots[slot].size = size;
  return out;
}

static void *allocate(void *context, size_t size) {
  return resize(context, NULL, size);
}

static void release(void *context, void *ptr) {
  fixture *f = context;
  size_t i, slot;
  if (ptr == NULL)
    return;
  slot = allocation_slot(f, ptr);
  if (slot == 32) {
    f->invalid_free = 1;
    return;
  }
  if (f->slots[slot].size >= FRAME) {
    --f->large;
    if (f->check_wipe) {
      ++f->wiped;
      for (i = 0; i < f->slots[slot].size; ++i)
        if (((unsigned char *)ptr)[i] != 0)
          f->dirty_free = 1;
    }
  }
  f->live -= f->slots[slot].size;
  memset(&f->slots[slot], 0, sizeof(f->slots[slot]));
  free(ptr);
}

static int destination(lc_sink *sink, const void *bytes, size_t size,
                       lc_error *error) {
  fixture *f = sink->impl;
  const unsigned char *cursor = bytes;
  size_t remaining = size;
  ssize_t n;
  int reject = ++f->sink_calls == f->reject_call;
  if (reject && remaining > f->partial)
    remaining = f->partial;
  while (remaining != 0) {
    n = write(f->fd, cursor, remaining);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return 0;
    cursor += n;
    remaining -= (size_t)n;
  }
  if (reject && f->sink_error)
    lc_error_set(error, LC_ERR_TRANSPORT, 0, "fixture rejected frame", NULL,
                 NULL, NULL);
  return !reject;
}

static void reset_output(fixture *f) {
  lc_pouch_crypto_writer_close(f->writer);
  f->writer = NULL;
  lc_free_with_allocator(&f->allocator, f->descriptor);
  f->descriptor = NULL;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(ftruncate(f->fd, 0), 0);
  assert_int_equal(lseek(f->fd, 0, SEEK_SET), 0);
  f->sink_calls = f->reject_call = f->partial = 0;
  f->sink_error = 0;
  f->peak = f->live;
  f->peak_large = f->large;
  f->wiped = 0;
}

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char template_path[] = PREFIX "XXXXXX", path[320];
  lc_pouch_crypto_open_options opts;
  unsigned long value = 1;
  size_t i;
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = f;
  f->sink.write = destination;
  f->sink.impl = f;
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  snprintf(path, sizeof(path), "%s/body", f->root);
  f->fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
  assert_true(f->fd >= 0);
  f->body = malloc(BODY);
  assert_non_null(f->body);
  for (i = 0; i < BODY; ++i) {
    value = (value * 1664525UL + 1013904223UL) & 0xffffffffUL;
    f->body[i] = (unsigned char)(value >> 24);
  }
  assert_int_equal(lc_pouch_crypto_generate_key_string(&f->key, &f->error),
                   LC_OK);
  memset(&opts, 0, sizeof(opts));
  opts.key_string = f->key;
  assert_int_equal(
      lc_pouch_crypto_open(&f->allocator, &opts, &f->crypto, NULL, &f->error),
      LC_OK);
  f->check_wipe = 1;
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_pouch_crypto_test_set_random_material(NULL, 0);
  lc_pouch_crypto_writer_close(f->writer);
  lc_free_with_allocator(&f->allocator, f->descriptor);
  lc_free_with_allocator(NULL, f->reference);
  lc_free_with_allocator(NULL, f->reference_descriptor);
  lc_pouch_crypto_close(f->crypto);
  assert_int_equal(f->live, 0);
  assert_false(f->dirty_free);
  assert_false(f->invalid_free);
  close(f->fd);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  free(f->body);
  free(f->units);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static int write_body(fixture *f, size_t length, size_t chunk, int sink_mode) {
  lc_sink *sink;
  size_t offset = 0, take;
  int rc = sink_mode ? lc_pouch_crypto_writer_open_sink(
                           f->crypto, CONTEXT, &f->sink, &f->writer, &f->error)
                     : lc_pouch_crypto_writer_open(f->crypto, CONTEXT, f->fd, 1,
                                                   &f->writer, &f->error);
  if (rc != LC_OK)
    return rc;
  sink = lc_pouch_crypto_writer_sink(f->writer);
  while (offset < length) {
    take = length - offset;
    if (take > chunk)
      take = chunk;
    if (!sink->write(sink, f->body + offset, take, &f->error))
      return f->error.code != LC_OK ? f->error.code : LC_ERR_TRANSPORT;
    offset += take;
  }
  return lc_pouch_crypto_writer_finish(f->writer, &f->plain, &f->stored,
                                       &f->crc, &f->descriptor, &f->error);
}

static int verify_body(fixture *f, size_t length) {
  lc_source *source = NULL;
  size_t got, offset = 0;
  int fd = dup(f->fd);
  int rc;
  if (fd < 0)
    return LC_ERR_TRANSPORT;
  rc = lc_pouch_crypto_source_from_fd_span(f->crypto, CONTEXT, CONTEXT, fd, 0,
                                           f->stored, f->descriptor, &source,
                                           &f->error);
  if (rc != LC_OK)
    return rc;
  do {
    got = source->read(source, f->chunk, sizeof(f->chunk), &f->error);
    rc = f->error.code;
    if (rc == LC_OK &&
        (got > length - offset || memcmp(f->chunk, f->body + offset, got) != 0))
      rc = LC_ERR_PROTOCOL;
    offset += got;
  } while (rc == LC_OK && got != 0);
  lc_source_close(source);
  return rc != LC_OK ? rc : offset == length ? LC_OK : LC_ERR_PROTOCOL;
}

static void test_roundtrip_boundaries_and_owned_heap_budget(void **state) {
  fixture *f = *state;
  lc_pouch_crypto_open_options opts;
  const size_t lengths[] = {
      0,     1,         4095,         4096,         4097, FRAME - 1,
      FRAME, FRAME + 1, FRAME + 4096, FRAME + 4097, BODY};
  size_t i, base, offset, got;
  unsigned long crc;
  int mode, sink_mode;
  for (mode = 0; mode < 4; ++mode) {
    reset_output(f);
    lc_pouch_crypto_close(f->crypto);
    f->crypto = NULL;
    memset(&opts, 0, sizeof(opts));
    opts.key_string = mode < 2 ? f->key : NULL;
    opts.compression_enabled = mode == 1 || mode == 2;
    assert_int_equal(
        lc_pouch_crypto_open(&f->allocator, &opts, &f->crypto, NULL, &f->error),
        LC_OK);
    for (sink_mode = 0; sink_mode < 2; ++sink_mode) {
      for (i = 0; i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
        reset_output(f);
        base = f->live;
        f->check_wipe = 1;
        assert_int_equal(
            write_body(f, lengths[i], i % 2 ? 4093 : 17, sink_mode), LC_OK);
        assert_int_equal(f->plain, lengths[i]);
        if (mode != 3)
          assert_int_equal(f->peak_large, 1);
        /* A producer's project-owned scratch stays below 72 KiB; OpenSSL
         * and zlib own their internal contexts independently. */
        assert_true(f->peak - base < 72U * 1024U);
        lc_pouch_crypto_writer_close(f->writer);
        f->writer = NULL;
        if (mode != 3)
          assert_int_equal(f->wiped, 1);
        assert_false(f->dirty_free);
        assert_true(fcntl(f->fd, F_GETFD) >= 0);
        assert_int_equal(lseek(f->fd, 0, SEEK_END), f->stored);
        crc = (unsigned long)crc32(0L, Z_NULL, 0);
        for (offset = 0; offset < f->stored; offset += got) {
          got = (size_t)pread(f->fd, f->chunk, sizeof(f->chunk), (off_t)offset);
          assert_true(got > 0 && got <= sizeof(f->chunk));
          crc = (unsigned long)crc32((uLong)crc, f->chunk, (uInt)got);
        }
        assert_int_equal(crc, f->crc);
        f->check_wipe = 0;
        assert_int_equal(verify_body(f, lengths[i]), LC_OK);
      }
    }
  }
}

static void test_final_frames_coalesce_with_one_frame_bound(void **state) {
  fixture *f = *state;
  const size_t lengths[] = {0,
                            1,
                            256,
                            4095,
                            4096,
                            4097,
                            4096 + FRAME - 1,
                            4096 + FRAME,
                            4096 + FRAME + 1};
  const size_t calls[] = {1, 1, 1, 1, 1, 2, 2, 2, 3};
  lc_sink *sink;
  for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
    reset_output(f);
    f->check_wipe = 1;
    assert_int_equal(write_body(f, lengths[i], 17, 1), LC_OK);
    assert_int_equal(f->sink_calls, calls[i]);
    f->check_wipe = 0;
    assert_int_equal(verify_body(f, lengths[i]), LC_OK);
  }
  reset_output(f);
  f->check_wipe = 1;
  assert_int_equal(lc_pouch_crypto_writer_open_sink(
                       f->crypto, CONTEXT, &f->sink, &f->writer, &f->error),
                   LC_OK);
  sink = lc_pouch_crypto_writer_sink(f->writer);
  assert_true(sink->write(sink, f->body, 4096, &f->error));
  assert_int_equal(f->sink_calls, 0);
  /* One additional byte must deliver the full frame before EOF. */
  assert_true(sink->write(sink, f->body + 4096, 1, &f->error));
  assert_int_equal(f->sink_calls, 1);
  assert_int_equal(lc_pouch_crypto_writer_finish(f->writer, &f->plain,
                                                 &f->stored, &f->crc,
                                                 &f->descriptor, &f->error),
                   LC_OK);
  for (int explicit_error = 0; explicit_error < 2; ++explicit_error) {
    for (int compressed = 0; compressed < 2; ++compressed) {
      reset_output(f);
      lc_pouch_crypto_set_compression(f->crypto, compressed);
      f->reject_call = 1;
      f->partial = 7;
      f->sink_error = explicit_error;
      assert_int_equal(write_body(f, 256, 17, 1), LC_ERR_TRANSPORT);
      assert_null(f->descriptor);
    }
    reset_output(f);
    lc_pouch_crypto_set_compression(f->crypto, 0);
    f->reject_call = 2; /* Final delivery after a full first frame. */
    f->partial = 7;
    f->sink_error = explicit_error;
    assert_int_equal(write_body(f, 4097, 17, 1), LC_ERR_TRANSPORT);
    assert_null(f->descriptor);
  }
}

static void test_ciphertext_matches_out_of_place_encoder(void **state) {
  fixture *f = *state;
  unsigned char material[52];
  const size_t lengths[] = {0, 1, 4095, 4096, 4097, FRAME};
  size_t i, j, stored, got, offset;
  unsigned long crc;
  for (j = 0; j < sizeof(material); ++j)
    material[j] = (unsigned char)(j + 1);
  for (i = 0; i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
    reset_output(f);
    lc_pouch_crypto_test_set_random_material(material, sizeof(material));
    assert_int_equal(lc_pouch_crypto_transform_memory(
                         NULL, f->crypto, CONTEXT, f->body, lengths[i], 0,
                         &f->reference, &stored, &crc, &f->reference_descriptor,
                         &f->error),
                     LC_OK);
    lc_pouch_crypto_test_set_random_material(material, sizeof(material));
    assert_int_equal(write_body(f, lengths[i], 97, 1), LC_OK);
    lc_pouch_crypto_test_set_random_material(NULL, 0);
    assert_string_equal(f->descriptor, f->reference_descriptor);
    assert_int_equal(f->stored, stored);
    assert_int_equal(f->crc, crc);
    for (offset = 0; offset < stored; offset += got) {
      got = (size_t)pread(f->fd, f->chunk, sizeof(f->chunk), (off_t)offset);
      assert_true(got > 0 && got <= sizeof(f->chunk));
      assert_memory_equal(f->chunk, f->reference + offset, got);
    }
    lc_free_with_allocator(NULL, f->reference);
    lc_free_with_allocator(NULL, f->reference_descriptor);
    f->reference = NULL;
    f->reference_descriptor = NULL;
  }
}

static void test_rejected_frames_and_abort_wipe_scratch(void **state) {
  fixture *f = *state;
  size_t base, calls, reject;
  lc_sink *sink;
  int compressed, explicit_error;
  for (compressed = 0; compressed < 2; ++compressed) {
    lc_pouch_crypto_set_compression(f->crypto, compressed);
    for (explicit_error = 0; explicit_error < 2; ++explicit_error) {
      for (reject = 1; reject <= 4; ++reject) {
        reset_output(f);
        base = f->live;
        f->reject_call = reject;
        f->partial = reject % 2 ? 0 : 7;
        f->sink_error = explicit_error;
        assert_int_equal(write_body(f, BODY, 4093, 1), LC_ERR_TRANSPORT);
        assert_null(f->descriptor);
        calls = f->sink_calls;
        sink = lc_pouch_crypto_writer_sink(f->writer);
        assert_false(sink->write(sink, f->body, 1, &f->error));
        assert_int_equal(f->sink_calls, calls);
        lc_pouch_crypto_writer_close(f->writer);
        f->writer = NULL;
        assert_int_equal(f->live, base);
        assert_true(f->wiped > 0);
        assert_false(f->dirty_free);
      }
    }
    reset_output(f);
    base = f->live;
    assert_int_equal(lc_pouch_crypto_writer_open_sink(
                         f->crypto, CONTEXT, &f->sink, &f->writer, &f->error),
                     LC_OK);
    sink = lc_pouch_crypto_writer_sink(f->writer);
    assert_true(sink->write(sink, f->body, 4097, &f->error));
    lc_pouch_crypto_writer_close(f->writer);
    f->writer = NULL;
    assert_int_equal(f->live, base);
    assert_int_equal(f->wiped, 1);
  }
}

static void test_open_failures_wipe_and_release_scratch(void **state) {
  fixture *f = *state;
  size_t offset, base;
  unsigned char material = 0;
  int rc, compressed;
  for (compressed = 0; compressed < 2; ++compressed) {
    lc_pouch_crypto_set_compression(f->crypto, compressed);
    for (offset = 1; offset < 12; ++offset) {
      reset_output(f);
      base = f->live;
      f->fail_at = f->calls + offset;
      rc = lc_pouch_crypto_writer_open(f->crypto, CONTEXT, f->fd, 1, &f->writer,
                                       &f->error);
      f->fail_at = 0;
      if (rc == LC_OK) {
        lc_pouch_crypto_writer_close(f->writer);
        f->writer = NULL;
        assert_int_equal(f->live, base);
        break;
      }
      assert_int_equal(rc, LC_ERR_NOMEM);
      assert_null(f->writer);
      assert_int_equal(f->live, base);
      assert_false(f->dirty_free);
    }
    assert_true(offset < 12);
    reset_output(f);
    base = f->live;
    lc_pouch_crypto_test_set_random_material(&material, 1);
    rc = lc_pouch_crypto_writer_open(f->crypto, CONTEXT, f->fd, 1, &f->writer,
                                     &f->error);
    lc_pouch_crypto_test_set_random_material(NULL, 0);
    assert_int_equal(rc, LC_ERR_TRANSPORT);
    assert_null(f->writer);
    assert_int_equal(f->live, base);
    assert_int_equal(f->wiped, 1);
  }
}

static size_t failing_source(lc_source *source, void *bytes, size_t capacity,
                             lc_error *error) {
  fixture *f = source->impl;
  size_t size = capacity < 3073 ? capacity : 3073;
  memcpy(bytes, f->body, size);
  if (++f->source_calls == 3)
    lc_error_set(error, LC_ERR_TRANSPORT, 0, "fixture source failed", NULL,
                 NULL, NULL);
  return size;
}

static void test_source_and_fd_failures_release_scratch(void **state) {
  fixture *f = *state;
  lc_source source;
  lc_sink *sink;
  size_t base;
  char path[320];
  int compressed, fd;
  memset(&source, 0, sizeof(source));
  source.read = failing_source;
  source.impl = f;
  snprintf(path, sizeof(path), "%s/body", f->root);
  for (compressed = 0; compressed < 2; ++compressed) {
    lc_pouch_crypto_set_compression(f->crypto, compressed);
    reset_output(f);
    f->source_calls = 0;
    base = f->live;
    assert_int_equal(lc_pouch_crypto_stream_to_fd_crc(
                         f->crypto, CONTEXT, f->fd, &source, &f->plain,
                         &f->stored, &f->crc, &f->descriptor, &f->error),
                     LC_ERR_TRANSPORT);
    assert_null(f->descriptor);
    assert_int_equal(f->live, base);
    assert_true(f->wiped > 0);
    assert_false(f->dirty_free);
    reset_output(f);
    fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(lc_pouch_crypto_writer_open(f->crypto, CONTEXT, fd, 1,
                                                 &f->writer, &f->error),
                     LC_OK);
    sink = lc_pouch_crypto_writer_sink(f->writer);
    assert_false(sink->write(sink, f->body, BODY, &f->error));
    assert_int_equal(f->error.code, LC_ERR_TRANSPORT);
    assert_int_equal(lc_pouch_crypto_writer_finish(f->writer, &f->plain,
                                                   &f->stored, &f->crc,
                                                   &f->descriptor, &f->error),
                     LC_ERR_TRANSPORT);
    assert_null(f->descriptor);
    lc_pouch_crypto_writer_close(f->writer);
    f->writer = NULL;
    assert_int_equal(f->live, base);
    assert_int_equal(f->wiped, 1);
    assert_true(fcntl(fd, F_GETFD) >= 0);
    close(fd);
  }
}

static int verify_unit(fixture *f, const unit_record *unit,
                       const char *binding) {
  lc_source *source = NULL;
  size_t got = 0, offset = 0;
  int fd = dup(f->fd), rc;
  if (fd < 0)
    return LC_ERR_TRANSPORT;
  rc = lc_pouch_crypto_source_from_fd_span(
      f->crypto, unit->context, binding, fd, unit->offset, unit->stored,
      unit->descriptor, &source, &f->error);
  if (rc != LC_OK)
    return rc;
  do {
    got = source->read(source, f->chunk, sizeof(f->chunk), &f->error);
    rc = f->error.code;
    if (rc == LC_OK && (got > unit->bytes - offset ||
                        memcmp(f->chunk, f->body + offset, got) != 0))
      rc = LC_ERR_PROTOCOL;
    offset += got;
  } while (rc == LC_OK && got);
  lc_source_close(source);
  return rc != LC_OK ? rc : offset == unit->bytes ? LC_OK : LC_ERR_PROTOCOL;
}

static void clear_error(fixture *f) {
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}

static void
test_restartable_units_are_independent_without_allocations(void **state) {
  fixture *f = *state;
  lc_pouch_crypto_open_options opts;
  lc_sink *sink;
  const char *borrowed = NULL;
  size_t calls, unit, at, take, base;
  unsigned char raw[4093];
  char saved_descriptor[256];
  unsigned long crc;
  ssize_t got;
  int mode;
  f->units = calloc(12U, sizeof(*f->units));
  assert_non_null(f->units);
  for (mode = 0; mode < 4; ++mode) {
    reset_output(f);
    lc_pouch_crypto_close(f->crypto);
    f->crypto = NULL;
    memset(&opts, 0, sizeof(opts));
    opts.key_string = mode < 2 ? f->key : NULL;
    opts.compression_enabled = mode == 1 || mode == 2;
    assert_int_equal(
        lc_pouch_crypto_open(&f->allocator, &opts, &f->crypto, NULL, &f->error),
        LC_OK);
    snprintf(f->units[0].context, sizeof(f->units[0].context), "unit:0");
    base = f->live;
    assert_int_equal(
        lc_pouch_crypto_writer_open_sink(f->crypto, f->units[0].context,
                                         &f->sink, &f->writer, &f->error),
        LC_OK);
    calls = f->calls;
    f->check_wipe = 1;
    f->fail_at = calls + 1U;
    sink = lc_pouch_crypto_writer_sink(f->writer);
    assert_int_equal(
        lc_pouch_crypto_writer_restart(f->writer, "premature", &f->error),
        LC_ERR_INVALID);
    clear_error(f);
    for (unit = 0; unit < 12U; ++unit) {
      unit_record *record = &f->units[unit];
      /* Include empty/short/frame boundaries, and enough repeated units to
       * stream more than 1 MiB through one admitted 64 KiB scratch frame. */
      record->bytes = unit == 0 ? 0 : unit == 1 ? 1 : unit == 2 ? FRAME : BODY;
      snprintf(record->context, sizeof(record->context), "unit:%lu",
               (unsigned long)(unit / 2U));
      if (unit) {
        /* Invalid restart does not consume the previous completed unit. */
        if (mode < 2) {
          assert_int_equal(
              lc_pouch_crypto_writer_restart(f->writer, "", &f->error),
              LC_ERR_INVALID);
          clear_error(f);
        }
        assert_int_equal(lc_pouch_crypto_writer_restart(
                             f->writer, record->context, &f->error),
                         LC_OK);
      }
      record->offset = (uint64_t)lseek(f->fd, 0, SEEK_CUR);
      for (at = 0; at < record->bytes; at += take) {
        take = (size_t)(record->bytes - at);
        if (take > 4093U)
          take = 4093U;
        assert_true(sink->write(sink, f->body + at, take, &f->error));
      }
      assert_int_equal(lc_pouch_crypto_writer_finish_borrowed(
                           f->writer, &f->plain, &record->stored, &record->crc,
                           &borrowed, &f->error),
                       LC_OK);
      assert_int_equal(f->plain, record->bytes);
      snprintf(record->descriptor, sizeof(record->descriptor), "%s",
               borrowed ? borrowed : "");
      if (mode < 2 && unit)
        assert_string_not_equal(record->descriptor,
                                f->units[unit - 1U].descriptor);
      assert_int_equal(f->calls, calls);
      assert_true(f->live - base < 72U * 1024U);
      assert_false(sink->write(sink, f->body, 1, &f->error));
      clear_error(f);
    }
    f->fail_at = 0;
    lc_pouch_crypto_writer_close(f->writer);
    f->writer = NULL;
    assert_int_equal(f->live, base);
    assert_false(f->dirty_free);
    f->check_wipe = 0;
    for (unit = 0; unit < 12U; ++unit) {
      unit_record *record = &f->units[unit];
      assert_int_equal(verify_unit(f, record, record->context), LC_OK);
      crc = (unsigned long)crc32(0L, Z_NULL, 0);
      for (at = 0; at < record->stored; at += (size_t)got) {
        take = (size_t)(record->stored - at);
        if (take > sizeof(raw))
          take = sizeof(raw);
        got = pread(f->fd, raw, take, (off_t)(record->offset + at));
        assert_true(got > 0);
        crc = (unsigned long)crc32((uLong)crc, raw, (uInt)got);
      }
      assert_int_equal(crc, record->crc);
    }
    if (mode < 2) {
      /* Units 4/5 have identical plaintext and identity context. A descriptor
       * from the earlier unit must not authenticate the next unit's bytes. */
      strcpy(saved_descriptor, f->units[5].descriptor);
      strcpy(f->units[5].descriptor, f->units[4].descriptor);
      assert_int_equal(verify_unit(f, &f->units[5], f->units[5].context),
                       LC_ERR_PROTOCOL);
      clear_error(f);
      strcpy(f->units[5].descriptor, saved_descriptor);
      assert_int_equal(verify_unit(f, &f->units[3], "another-unit"),
                       LC_ERR_PROTOCOL);
      clear_error(f);
      assert_int_equal(ftruncate(f->fd, (off_t)(f->units[11].offset +
                                                f->units[11].stored - 1U)),
                       0);
      assert_int_equal(verify_unit(f, &f->units[11], f->units[11].context),
                       LC_ERR_PROTOCOL);
      clear_error(f);
    }
  }
}

static void test_failed_or_transferred_units_cannot_restart(void **state) {
  fixture *f = *state;
  const char *borrowed = NULL;
  lc_sink *sink;
  unsigned char material = 1;
  size_t calls, emitted;
  assert_int_equal(write_body(f, 7, 7, 1), LC_OK);
  assert_int_equal(lc_pouch_crypto_writer_restart(f->writer, "next", &f->error),
                   LC_ERR_INVALID);
  reset_output(f);
  assert_int_equal(lc_pouch_crypto_writer_open_sink(
                       f->crypto, CONTEXT, &f->sink, &f->writer, &f->error),
                   LC_OK);
  sink = lc_pouch_crypto_writer_sink(f->writer);
  assert_true(sink->write(sink, f->body, 7, &f->error));
  assert_int_equal(lc_pouch_crypto_writer_finish_borrowed(f->writer, &f->plain,
                                                          &f->stored, &f->crc,
                                                          &borrowed, &f->error),
                   LC_OK);
  calls = f->calls;
  emitted = f->sink_calls;
  lc_pouch_crypto_test_set_random_material(&material, 1);
  assert_int_equal(lc_pouch_crypto_writer_restart(f->writer, "next", &f->error),
                   LC_ERR_TRANSPORT);
  lc_pouch_crypto_test_set_random_material(NULL, 0);
  assert_int_equal(f->calls, calls);
  clear_error(f);
  assert_false(sink->write(sink, f->body, 7, &f->error));
  clear_error(f);
  assert_int_equal(lc_pouch_crypto_writer_restart(f->writer, "next", &f->error),
                   LC_ERR_INVALID);
  clear_error(f);
  assert_int_equal(lc_pouch_crypto_writer_finish_borrowed(f->writer, &f->plain,
                                                          &f->stored, &f->crc,
                                                          &borrowed, &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(f->sink_calls, emitted);
  reset_output(f);
  assert_int_equal(lc_pouch_crypto_writer_open_sink(
                       f->crypto, CONTEXT, &f->sink, &f->writer, &f->error),
                   LC_OK);
  f->reject_call = 1;
  f->sink_error = 1;
  f->partial = 7;
  sink = lc_pouch_crypto_writer_sink(f->writer);
  assert_false(sink->write(sink, f->body, 4097, &f->error));
  clear_error(f);
  assert_int_equal(lc_pouch_crypto_writer_finish_borrowed(f->writer, &f->plain,
                                                          &f->stored, &f->crc,
                                                          &borrowed, &f->error),
                   LC_ERR_TRANSPORT);
  assert_null(borrowed);
  clear_error(f);
  assert_int_equal(lc_pouch_crypto_writer_restart(f->writer, "next", &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(f->sink_calls, 1);
}

static void *worker(void *context) {
  fixture *f = context;
  f->rc = write_body(f, BODY, 4093, 1);
  lc_pouch_crypto_writer_close(f->writer);
  f->writer = NULL;
  if (f->rc == LC_OK && f->peak_large != 1)
    f->rc = LC_ERR_PROTOCOL;
  f->check_wipe = 0;
  if (f->rc == LC_OK)
    f->rc = verify_body(f, BODY);
  return NULL;
}

static void test_writer_uses_default_worker_stack(void **state) {
  fixture *f = *state;
  pthread_t thread;
  lc_pouch_crypto_set_compression(f->crypto, 1);
  assert_int_equal(pthread_create(&thread, NULL, worker, f), 0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(f->rc, LC_OK);
  assert_int_equal(f->wiped, 1);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_final_frames_coalesce_with_one_frame_bound, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_roundtrip_boundaries_and_owned_heap_budget, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_ciphertext_matches_out_of_place_encoder, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_rejected_frames_and_abort_wipe_scratch, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_open_failures_wipe_and_release_scratch, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_source_and_fd_failures_release_scratch, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_restartable_units_are_independent_without_allocations, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_or_transferred_units_cannot_restart, setup, teardown),
      cmocka_unit_test_setup_teardown(test_writer_uses_default_worker_stack,
                                      setup, teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
