#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <lonejson.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_crypto.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/pouch-reader-"
#define BODY_BYTES (132U * 1024U + 19U)

typedef struct allocation_state {
  size_t calls, fail_at, live;
  pthread_mutex_t mutex;
} allocation_state;

typedef struct sample {
  char context[320];
  char *descriptor;
  uint64_t offset, stored;
  size_t length;
} sample;

typedef struct fixture {
  char root[256];
  char *key;
  unsigned char *body, *chunk;
  lc_allocator allocator;
  allocation_state allocations;
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_reader *reader;
  lc_error error;
  sample samples[3];
  int fd;
} fixture;

static void *tracked_malloc(void *context, size_t size) {
  allocation_state *s = context;
  void *p;
  pthread_mutex_lock(&s->mutex);
  if (++s->calls == s->fail_at) {
    pthread_mutex_unlock(&s->mutex);
    return NULL;
  }
  p = malloc(size);
  if (p != NULL)
    ++s->live;
  pthread_mutex_unlock(&s->mutex);
  return p;
}

static void *tracked_realloc(void *context, void *ptr, size_t size) {
  allocation_state *s = context;
  void *p;
  int fresh = ptr == NULL;
  pthread_mutex_lock(&s->mutex);
  if (++s->calls == s->fail_at) {
    pthread_mutex_unlock(&s->mutex);
    return NULL;
  }
  p = realloc(ptr, size);
  if (p != NULL && fresh)
    ++s->live;
  pthread_mutex_unlock(&s->mutex);
  return p;
}

static void tracked_free(void *context, void *ptr) {
  allocation_state *s = context;
  if (ptr != NULL) {
    pthread_mutex_lock(&s->mutex);
    assert_true(s->live > 0);
    --s->live;
    free(ptr);
    pthread_mutex_unlock(&s->mutex);
  }
}

static void clear_error(fixture *f) {
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}

static void close_case(fixture *f) {
  size_t i;
  lc_pouch_crypto_reader_close(f->reader);
  f->reader = NULL;
  for (i = 0; i < 3; ++i) {
    lc_free_with_allocator(f->crypto != NULL ? &f->allocator : NULL,
                           f->samples[i].descriptor);
    f->samples[i].descriptor = NULL;
  }
  lc_pouch_crypto_close(f->crypto);
  f->crypto = NULL;
  assert_int_equal(f->allocations.live, 0);
}

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char template_path[] = PREFIX "XXXXXX";
  char path[320];
  size_t i;
  unsigned long value = 1;
  assert_non_null(f);
  *state = f;
  f->fd = -1;
  assert_int_equal(pthread_mutex_init(&f->allocations.mutex, NULL), 0);
  lc_error_init(&f->error);
  f->allocator.malloc_fn = tracked_malloc;
  f->allocator.realloc_fn = tracked_realloc;
  f->allocator.free_fn = tracked_free;
  f->allocator.context = &f->allocations;
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  assert_true(snprintf(path, sizeof(path), "%s/body", f->root) > 0);
  f->fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0600);
  assert_true(f->fd >= 0);
  f->body = malloc(BODY_BYTES);
  f->chunk = malloc(16385);
  assert_non_null(f->body);
  assert_non_null(f->chunk);
  /* Incompressible enough to exercise multiple encrypted 64 KiB frames. */
  for (i = 0; i < BODY_BYTES; ++i) {
    value = (value * 1664525UL + 1013904223UL) & 0xffffffffUL;
    f->body[i] = (unsigned char)(value >> 24);
  }
  assert_int_equal(lc_pouch_crypto_generate_key_string(&f->key, &f->error),
                   LC_OK);
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  close_case(f);
  if (f->fd >= 0)
    close(f->fd);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  assert_int_equal(pthread_mutex_destroy(&f->allocations.mutex), 0);
  free(f->body);
  free(f->chunk);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static void open_case(fixture *f, int mode) {
  lc_pouch_crypto_open_options options;
  lc_source *input;
  uint64_t plain;
  size_t i;
  close_case(f);
  memset(&options, 0, sizeof(options));
  options.key_string = mode & 1 ? f->key : NULL;
  options.compression_enabled = (mode & 2) != 0;
  assert_int_equal(lc_pouch_crypto_open(&f->allocator, &options, &f->crypto,
                                        NULL, &f->error),
                   LC_OK);
  assert_int_equal(ftruncate(f->fd, 0), 0);
  assert_int_equal(lseek(f->fd, 0, SEEK_SET), 0);
  for (i = 0; i < 3; ++i) {
    sample *s = &f->samples[i];
    memset(s->context, 'a' + (int)i, i == 0 ? 300 : 31 + i);
    s->context[i == 0 ? 300 : 31 + i] = '\0';
    s->length = i == 0 ? BODY_BYTES : i == 1 ? 4097 : 0;
    s->offset = (uint64_t)lseek(f->fd, 0, SEEK_CUR);
    assert_int_equal(
        lc_source_from_memory(f->body, s->length, &input, &f->error), LC_OK);
    assert_int_equal(lc_pouch_crypto_stream_to_fd_crc_with_compression(
                         f->crypto, s->context, f->fd, input, &plain,
                         &s->stored, NULL, &s->descriptor, i != 1, &f->error),
                     LC_OK);
    lc_source_close(input);
    assert_int_equal(plain, s->length);
  }
  /* No source may consume another span, even when its contents look valid. */
  assert_int_equal(write(f->fd, "sentinel", 8), 8);
  assert_int_equal(
      lc_pouch_crypto_reader_open(f->crypto, &f->reader, &f->error), LC_OK);
}

static lc_source *bind_sample(fixture *f, size_t i) {
  lc_source *source = NULL;
  sample *s = &f->samples[i];
  assert_int_equal(lc_pouch_crypto_reader_bind(
                       f->reader, s->context, s->context, f->fd, s->offset,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_OK);
  assert_non_null(source);
  return source;
}

static void assert_body(fixture *f, lc_source *source, size_t i,
                        size_t capacity) {
  size_t offset = 0, got;
  do {
    got = source->read(source, f->chunk, capacity, &f->error);
    assert_int_equal(f->error.code, LC_OK);
    assert_true(got <= capacity);
    assert_true(got <= f->samples[i].length - offset);
    assert_memory_equal(f->chunk, f->body + offset, got);
    offset += got;
  } while (got != 0);
  assert_int_equal(offset, f->samples[i].length);
}

static void test_rebind_replay_partial_and_codec_changes(void **state) {
  fixture *f = *state;
  lc_source *source;
  size_t i, calls;
  int mode;
  for (mode = 0; mode < 4; ++mode) {
    open_case(f, mode);
    for (i = 0; i < 3; ++i)
      assert_body(f, bind_sample(f, i), i, 16385);
    calls = f->allocations.calls;
    for (i = 0; i < 12; ++i) {
      source = bind_sample(f, i % 3);
      assert_int_equal(source->read(source, f->chunk, 0, &f->error), 0);
      if (i % 3 != 2)
        assert_int_equal(source->read(source, f->chunk, 17, &f->error), 17);
      assert_int_equal(source->reset(source, &f->error), LC_OK);
      assert_body(f, source, i % 3, 4093);
      /* Deliberately abandon a partially consumed large body. */
      source = bind_sample(f, 0);
      assert_int_equal(source->read(source, f->chunk, 17, &f->error), 17);
      lc_pouch_crypto_reader_clear(f->reader);
      assert_true(fcntl(f->fd, F_GETFD) >= 0);
    }
    /* The library allocator owns constant scratch, independent of entry count.
     */
    assert_int_equal(f->allocations.calls, calls);
  }
}

static void test_corrupt_spans_do_not_poison_next_body(void **state) {
  fixture *f = *state;
  lc_source *source;
  sample *s;
  unsigned char old, bad;
  uint64_t at;
  size_t got;
  int failure, mode;
  for (mode = 2; mode < 4; ++mode) {
    open_case(f, mode);
    s = &f->samples[0];
    for (failure = 0; failure < (mode == 3 ? 6 : 5); ++failure) {
      /* Frame header, ciphertext, final tag, truncation, trailing bytes, AAD.
       */
      at = s->offset + (failure == 0 ? 0 : failure == 1 ? 12 : s->stored - 1);
      assert_int_equal(pread(f->fd, &old, 1, (off_t)at), 1);
      if (failure < 3) {
        bad = old ^ 1;
        assert_int_equal(pwrite(f->fd, &bad, 1, (off_t)at), 1);
      }
      assert_int_equal(lc_pouch_crypto_reader_bind(
                           f->reader, failure == 5 ? "wrong" : s->context,
                           s->context, f->fd, s->offset,
                           s->stored - (failure == 3) + (failure == 4),
                           s->descriptor, &source, &f->error),
                       LC_OK);
      do {
        got = source->read(source, f->chunk, 16385, &f->error);
      } while (got != 0 && f->error.code == LC_OK);
      assert_int_equal(f->error.code, LC_ERR_PROTOCOL);
      clear_error(f);
      if (failure < 3)
        assert_int_equal(pwrite(f->fd, &old, 1, (off_t)at), 1);
      assert_body(f, bind_sample(f, 1), 1, 17);
      assert_body(f, bind_sample(f, 0), 0, 16385);
    }
  }
  assert_int_equal(lc_pouch_crypto_reader_bind(
                       f->reader, s->context, "wrong binding", f->fd, s->offset,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_ERR_PROTOCOL);
  assert_null(source);
  clear_error(f);
  assert_body(f, bind_sample(f, 2), 2, 17);
}

static void test_allocation_failures_and_fd_ownership(void **state) {
  fixture *f = *state;
  lc_source *source;
  sample *s;
  size_t baseline, fail;
  int rc, owned;
  open_case(f, 3);
  s = &f->samples[0];
  lc_pouch_crypto_reader_close(f->reader);
  f->reader = NULL;
  baseline = f->allocations.live;
  for (fail = 1; fail < 16; ++fail) {
    f->allocations.fail_at = f->allocations.calls + fail;
    rc = lc_pouch_crypto_reader_open(f->crypto, &f->reader, &f->error);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_reader_bind(f->reader, s->context, s->context, f->fd,
                                       s->offset, s->stored, s->descriptor,
                                       &source, &f->error);
    f->allocations.fail_at = 0;
    if (rc != LC_OK) {
      assert_int_equal(rc, LC_ERR_NOMEM);
      clear_error(f);
      /* A failed bind is reusable without rebuilding its operation owner. */
      if (f->reader != NULL)
        assert_body(f, bind_sample(f, 0), 0, 16385);
    }
    lc_pouch_crypto_reader_close(f->reader);
    f->reader = NULL;
    assert_int_equal(f->allocations.live, baseline);
    assert_true(fcntl(f->fd, F_GETFD) >= 0);
    if (rc == LC_OK)
      break;
  }
  assert_true(fail < 16);
  assert_true(fail >= 4);
  assert_int_equal(
      lc_pouch_crypto_reader_open(f->crypto, &f->reader, &f->error), LC_OK);
  assert_body(f, bind_sample(f, 1), 1, 17);
  f->allocations.fail_at = f->allocations.calls + 1;
  assert_int_equal(lc_pouch_crypto_reader_bind(
                       f->reader, s->context, s->context, f->fd, s->offset,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_ERR_NOMEM);
  assert_null(source);
  f->allocations.fail_at = 0;
  clear_error(f);
  assert_body(f, bind_sample(f, 1), 1, 17);
  assert_body(f, bind_sample(f, 0), 0, 16385);
  lc_pouch_crypto_reader_close(f->reader);
  f->reader = NULL;
  owned = dup(f->fd);
  assert_true(owned >= 0);
  assert_int_equal(lc_pouch_crypto_source_from_fd_span(
                       f->crypto, s->context, s->context, owned, s->offset,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_OK);
  assert_body(f, source, 0, 16385);
  assert_int_equal(source->reset(source, &f->error), LC_OK);
  assert_body(f, source, 0, 4093);
  lc_source_close(source);
  assert_int_equal(fcntl(owned, F_GETFD), -1);
  assert_int_equal(errno, EBADF);
  owned = dup(f->fd);
  assert_int_equal(lc_pouch_crypto_source_from_fd_span(
                       f->crypto, s->context, "wrong", owned, s->offset,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_ERR_PROTOCOL);
  assert_null(source);
  assert_int_equal(fcntl(owned, F_GETFD), -1);
  assert_int_equal(errno, EBADF);
  assert_int_equal(f->allocations.live, baseline);
}

typedef struct reader_work {
  fixture *f;
  int rc;
} reader_work;

static void test_file_frames_replay_and_reject_buffered_tails(void **state) {
  fixture *f = *state;
  lc_source *source;
  sample *s;
  char path[320];
  size_t got;
  int mode, small;
  for (mode = 1; mode < 4; mode += 2) {
    for (small = 0; small < 2; ++small) {
      open_case(f, mode);
      s = &f->samples[small];
      if (small) {
        assert_true(s->stored <= 16385);
        assert_int_equal(
            pread(f->fd, f->chunk, (size_t)s->stored, (off_t)s->offset),
            s->stored);
        assert_int_equal(pwrite(f->fd, f->chunk, (size_t)s->stored, 0),
                         s->stored);
      }
      assert_int_equal(ftruncate(f->fd, (off_t)s->stored), 0);
      assert_true(snprintf(path, sizeof(path), "%s/body", f->root) > 0);
      assert_int_equal(lc_pouch_crypto_source_from_file(
                           f->crypto, s->context, s->context, path,
                           s->descriptor, &source, &f->error),
                       LC_OK);
      assert_body(f, source, (size_t)small, 16385);
      assert_int_equal(source->reset(source, &f->error), LC_OK);
      assert_int_equal(source->read(source, f->chunk, 17, &f->error), 17);
      assert_int_equal(source->reset(source, &f->error), LC_OK);
      assert_body(f, source, (size_t)small, 4093);
      assert_int_equal(pwrite(f->fd, "!", 1, (off_t)s->stored), 1);
      assert_int_equal(source->reset(source, &f->error), LC_OK);
      do {
        got = source->read(source, f->chunk, 16385, &f->error);
      } while (got && f->error.code == LC_OK);
      assert_int_equal(f->error.code, LC_ERR_PROTOCOL);
      clear_error(f);
      assert_int_equal(ftruncate(f->fd, (off_t)s->stored - 1), 0);
      assert_int_equal(source->reset(source, &f->error), LC_OK);
      do {
        got = source->read(source, f->chunk, 16385, &f->error);
      } while (got && f->error.code == LC_OK);
      assert_int_equal(f->error.code, LC_ERR_PROTOCOL);
      clear_error(f);
      lc_source_close(source);
      assert_true(fcntl(f->fd, F_GETFD) >= 0);
    }
  }
}

static void
test_repeated_binding_failures_preserve_independent_keys(void **state) {
  fixture *f = *state;
  lc_source *source;
  sample *s;
  char descriptor[192];
  size_t i;
  open_case(f, 3);
  for (i = 0; i < 12; ++i) {
    s = &f->samples[i % 3];
    assert_true(strlen(s->descriptor) < sizeof(descriptor));
    strcpy(descriptor, s->descriptor);
    /* A different salt must not reuse a previous payload or binding key. */
    descriptor[50] = descriptor[50] == 'A' ? 'B' : 'A';
    assert_int_equal(lc_pouch_crypto_reader_bind(
                         f->reader, s->context, s->context, f->fd, s->offset,
                         s->stored, descriptor, &source, &f->error),
                     LC_ERR_PROTOCOL);
    assert_null(source);
    clear_error(f);
    assert_body(f, bind_sample(f, i % 3), i % 3, 4093);
    assert_int_equal(
        lc_pouch_crypto_reader_bind(f->reader, s->context, "different identity",
                                    f->fd, s->offset, s->stored, s->descriptor,
                                    &source, &f->error),
        LC_ERR_PROTOCOL);
    assert_null(source);
    clear_error(f);
    assert_body(f, bind_sample(f, (i + 1) % 3), (i + 1) % 3, 17);
  }
}

static void
test_unrepresentable_span_offset_fails_without_wrapping(void **state) {
  fixture *f = *state;
  lc_source *source;
  sample *s;
  open_case(f, 3);
  s = &f->samples[0];
  assert_int_equal(lc_pouch_crypto_reader_bind(
                       f->reader, s->context, s->context, f->fd, (uint64_t)-2,
                       s->stored, s->descriptor, &source, &f->error),
                   LC_OK);
  assert_int_equal(source->read(source, f->chunk, 17, &f->error), 0);
  assert_int_equal(f->error.code, LC_ERR_INVALID);
  clear_error(f);
  assert_body(f, bind_sample(f, 0), 0, 4093);
}

static void *read_worker(void *context) {
  reader_work *work = context;
  fixture *f = work->f;
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *source;
  lc_error error;
  unsigned char chunk[1024];
  size_t i, offset, got;
  int rc;
  lc_error_init(&error);
  rc = lc_pouch_crypto_reader_open(f->crypto, &reader, &error);
  for (i = 0; rc == LC_OK && i < 12; ++i) {
    sample *s = &f->samples[i % 3];
    rc = lc_pouch_crypto_reader_bind(reader, s->context, s->context, f->fd,
                                     s->offset, s->stored, s->descriptor,
                                     &source, &error);
    offset = 0;
    got = 1;
    while (rc == LC_OK && got != 0) {
      got = source->read(source, chunk, sizeof(chunk), &error);
      rc = error.code;
      if (rc == LC_OK && (got > s->length - offset ||
                          memcmp(chunk, f->body + offset, got) != 0))
        rc = LC_ERR_PROTOCOL;
      if (rc == LC_OK)
        offset += got;
    }
    if (rc == LC_OK && offset != s->length)
      rc = LC_ERR_PROTOCOL;
  }
  lc_pouch_crypto_reader_close(reader);
  lc_error_cleanup(&error);
  work->rc = rc;
  return NULL;
}

static void test_parallel_readers_use_default_worker_stacks(void **state) {
  fixture *f = *state;
  pthread_t threads[2];
  reader_work work[2];
  size_t i;
  open_case(f, 3);
  for (i = 0; i < 2; ++i) {
    work[i].f = f;
    work[i].rc = LC_ERR_INVALID;
    assert_int_equal(pthread_create(&threads[i], NULL, read_worker, &work[i]),
                     0);
  }
  for (i = 0; i < 2; ++i) {
    assert_int_equal(pthread_join(threads[i], NULL), 0);
    assert_int_equal(work[i].rc, LC_OK);
  }
  assert_true(fcntl(f->fd, F_GETFD) >= 0);
}

static void test_descriptor_alphabet_bounds_and_recovery(void **state) {
  fixture *f = *state;
  const char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const char prefix[] = "lc-pouch-desc-v1:";
  char descriptor[1024];
  sample *s;
  lc_source *source;
  unsigned int ch;
  size_t i;
  int rc;
  open_case(f, 3);
  s = &f->samples[0];
  for (ch = 0; ch < 256; ++ch) {
    strcpy(descriptor, s->descriptor);
    descriptor[50] =
        (char)ch; /* A salt byte, leaving the layout/flags valid. */
    rc = lc_pouch_crypto_reader_bind(f->reader, s->context, s->context, f->fd,
                                     s->offset, s->stored, descriptor, &source,
                                     &f->error);
    if (ch == (unsigned char)s->descriptor[50]) {
      assert_int_equal(rc, LC_OK);
    } else {
      assert_int_equal(rc, ch != 0 && strchr(alphabet, (int)ch) != NULL
                               ? LC_ERR_PROTOCOL
                               : LC_ERR_INVALID);
      assert_null(source);
    }
    clear_error(f);
  }
  /* Exercise invalid remainders, short versions and oversized decoded data. */
  for (i = 0; i < sizeof(descriptor) - sizeof(prefix); ++i) {
    memcpy(descriptor, prefix, sizeof(prefix) - 1);
    memset(descriptor + sizeof(prefix) - 1, 'A', i);
    descriptor[sizeof(prefix) - 1 + i] = '\0';
    assert_int_equal(lc_pouch_crypto_reader_bind(
                         f->reader, s->context, s->context, f->fd, s->offset,
                         s->stored, descriptor, &source, &f->error),
                     LC_ERR_INVALID);
    assert_null(source);
    clear_error(f);
  }
  assert_body(f, bind_sample(f, 0), 0, 4093);
}

static void test_root_key_decoding_matches_independent_vectors(void **state) {
  fixture *f = *state;
  const char prefix[] = "lc-pouch-key-v1:";
  const char label[] = "liblockdc-pouch-root-key-id-v1";
  const char id_prefix[] = "hmac-sha256:";
  unsigned char raw[33], digest[32];
  char key[128], expected[128];
  char *id;
  lc_pouch_crypto_open_options opts;
  lc_pouch_crypto *crypto;
  lonejson_error encoding_error;
  size_t i, batch, encoded, digest_len;
  memset(&opts, 0, sizeof(opts));
  lonejson_error_init(&encoding_error);
  for (batch = 0; batch < 8; ++batch) {
    for (i = 0; i < 32; ++i)
      raw[i] = (unsigned char)(batch * 32 + i);
    memcpy(key, prefix, sizeof(prefix) - 1);
    assert_int_equal(lonejson_base64_encode(raw, 32, LONEJSON_BASE64_URL_RAW,
                                            key + sizeof(prefix) - 1,
                                            sizeof(key) - sizeof(prefix),
                                            &encoded, &encoding_error),
                     LONEJSON_STATUS_OK);
    key[sizeof(prefix) - 1 + encoded] = '\0';
    opts.key_string = key;
    crypto = NULL;
    assert_int_equal(
        lc_pouch_crypto_open(NULL, &opts, &crypto, NULL, &f->error), LC_OK);
    id = NULL;
    assert_int_equal(lc_pouch_crypto_key_id(crypto, &id, &f->error), LC_OK);
    digest_len = 0;
    assert_non_null(EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, raw, 32,
                              (const unsigned char *)label, sizeof(label) - 1,
                              digest, sizeof(digest), &digest_len));
    assert_int_equal(digest_len, 32);
    memcpy(expected, id_prefix, sizeof(id_prefix) - 1);
    assert_int_equal(
        lonejson_base64_encode(digest, sizeof(digest), LONEJSON_BASE64_URL_RAW,
                               expected + sizeof(id_prefix) - 1,
                               sizeof(expected) - sizeof(id_prefix), &encoded,
                               &encoding_error),
        LONEJSON_STATUS_OK);
    expected[sizeof(id_prefix) - 1 + encoded] = '\0';
    assert_string_equal(id, expected);
    lc_free_with_allocator(NULL, id);
    lc_pouch_crypto_close(crypto);
  }
  for (i = 31; i <= 33; i += 2) {
    memset(raw, 0xFF, sizeof(raw));
    memcpy(key, prefix, sizeof(prefix) - 1);
    assert_int_equal(lonejson_base64_encode(raw, i, LONEJSON_BASE64_URL_RAW,
                                            key + sizeof(prefix) - 1,
                                            sizeof(key) - sizeof(prefix),
                                            &encoded, &encoding_error),
                     LONEJSON_STATUS_OK);
    key[sizeof(prefix) - 1 + encoded] = '\0';
    opts.key_string = key;
    crypto = NULL;
    assert_int_equal(
        lc_pouch_crypto_open(NULL, &opts, &crypto, NULL, &f->error),
        LC_ERR_INVALID);
    assert_null(crypto);
    clear_error(f);
  }
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_rebind_replay_partial_and_codec_changes, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_corrupt_spans_do_not_poison_next_body, setup, teardown),
      cmocka_unit_test_setup_teardown(test_allocation_failures_and_fd_ownership,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_parallel_readers_use_default_worker_stacks, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_file_frames_replay_and_reject_buffered_tails, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_repeated_binding_failures_preserve_independent_keys, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_unrepresentable_span_offset_fails_without_wrapping, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_descriptor_alphabet_bounds_and_recovery, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_root_key_decoding_matches_independent_vectors, setup, teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
