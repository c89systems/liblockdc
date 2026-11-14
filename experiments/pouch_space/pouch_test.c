#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "namespace.h"
#include "pouch.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocation, fail_at;
static int armed;
static char *crypto_key;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static int fails(void) {
  return __atomic_load_n(&armed, __ATOMIC_ACQUIRE) &&
         __atomic_add_fetch(&allocation, 1, __ATOMIC_RELAXED) == fail_at;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t s) {
  return fails() ? NULL : __real_lc_calloc_with_allocator(a, n, s);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
typedef union header {
  long double alignment;
  struct {
    uintptr_t cookie;
    size_t size;
  } value;
} header;
static size_t live, measured_max;
static int measure;
static void *allocate(void *context, size_t size) {
  header *h = malloc(sizeof(*h) + size);
  (void)context;
  if (__atomic_load_n(&measure, __ATOMIC_RELAXED)) {
    size_t old = __atomic_load_n(&measured_max, __ATOMIC_RELAXED);
    while (size > old &&
           !__atomic_compare_exchange_n(&measured_max, &old, size, 0,
                                        __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
    }
  }
  if (!h)
    return NULL;
  h->value.cookie = (uintptr_t)h ^ UINT64_C(0x721d9629abcd1234);
  h->value.size = size;
  __atomic_add_fetch(&live, 1, __ATOMIC_RELAXED);
  return h + 1;
}
static void deallocate(void *context, void *p) {
  header *h;
  (void)context;
  if (!p)
    return;
  h = (header *)p - 1;
  REQUIRE(h->value.cookie == ((uintptr_t)h ^ UINT64_C(0x721d9629abcd1234)));
  h->value.cookie = 0;
  __atomic_sub_fetch(&live, 1, __ATOMIC_RELAXED);
  free(h);
}
static void *resize(void *context, void *p, size_t size) {
  void *next = allocate(context, size);
  if (next && p) {
    header *h = (header *)p - 1;
    memcpy(next, p, h->value.size < size ? h->value.size : size);
    deallocate(context, p);
  }
  return next;
}
static lc_allocator allocator = {allocate, resize, deallocate, NULL};
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "pouch %d: %s\n", rc, e->message ? e->message : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static ps_pouch_state_options options(int crypto, int sync) {
  ps_pouch_state_options o = {0};
  o.logical.background_compaction_enabled_set = 1;
  o.logical.query_indexing_enabled_set = 1;
  o.logical.segment_target_bytes = 32768;
  o.logical.durable_sync = sync;
  o.logical.crypto_key = crypto & 1 ? crypto_key : NULL;
  o.logical.compression = crypto & 2 ? "zlib" : "none";
  return o;
}
typedef struct input {
  lc_source pub;
  size_t length, offset, calls, fail_after;
  unsigned char seed;
} input;
static unsigned char byte_at(size_t at, unsigned char seed) {
  return (unsigned char)((at * 97 + at / 251 + seed) & 255);
}
static size_t input_read(lc_source *pub, void *bytes, size_t size,
                         lc_error *e) {
  input *in = pub->impl;
  unsigned char *p = bytes;
  ++in->calls;
  if (in->fail_after && in->offset >= in->fail_after) {
    (void)lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected producer failure",
                       NULL, NULL, "test");
    return 0;
  }
  if (size > 4096)
    size = 4096;
  if (size > in->length - in->offset)
    size = in->length - in->offset;
  for (size_t i = 0; i < size; ++i)
    p[i] = byte_at(in->offset + i, in->seed);
  in->offset += size;
  return size;
}
static input source(size_t bytes, unsigned char seed) {
  input in = {0};
  in.pub.read = input_read;
  in.length = bytes;
  in.seed = seed;
  return in;
}
static int put(lc_pouch *p, const char *ns, const char *key, input *in,
               const lc_pouch_state_write_options *o,
               lc_pouch_state_write_result *out, lc_error *e) {
  in->pub.impl = in;
  return lc_pouch_state_write(p, ns, key, &in->pub, o, out, e);
}
static void drain(lc_source *src, size_t length, unsigned char seed,
                  lc_error *e) {
  unsigned char bytes[997];
  size_t at = 0, got;
  while ((got = src->read(src, bytes, sizeof(bytes), e)) != 0) {
    REQUIRE(at + got <= length);
    for (size_t i = 0; i < got; ++i)
      REQUIRE(bytes[i] == byte_at(at + i, seed));
    at += got;
  }
  ok(e->code, e);
  REQUIRE(at == length);
}
static void read_value(lc_pouch *p, const char *ns, const char *key,
                       size_t length, unsigned char seed, uint64_t version,
                       lc_error *e) {
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read(p, ns, key, &r, e), e);
  REQUIRE(r.found && r.has_body && r.version == version && r.bytes == length);
  drain(r.body, length, seed, e);
  ok(r.body->reset(r.body, e), e);
  drain(r.body, length, seed, e);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
}
static void one_file(const char *directory) {
  DIR *dir = opendir(directory);
  struct dirent *ent;
  size_t count = 0;
  REQUIRE(dir != NULL);
  while ((ent = readdir(dir)) != NULL) {
    if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
      continue;
    if (!strcmp(ent->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    REQUIRE(!strcmp(ent->d_name, "store"));
    ++count;
  }
  REQUIRE(count == 1 && closedir(dir) == 0);
}
typedef struct output {
  size_t offset, length;
  unsigned char seed;
} output;
static int output_write(lc_sink *sink, const void *bytes, size_t length,
                        lc_error *e) {
  output *out = sink->impl;
  const unsigned char *p = bytes;
  (void)e;
  REQUIRE(length <= out->length - out->offset);
  for (size_t i = 0; i < length; ++i)
    REQUIRE(p[i] == byte_at(out->offset + i, out->seed));
  out->offset += length;
  return 1;
}
static void lifecycle(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync) {
      ps_pouch_state_options o = options(crypto, sync);
      lc_pouch *p = NULL;
      lc_pouch_state_write_result w = {0};
      lc_pouch_state_read_result r = {0};
      lc_pouch_state_write_options write = {0};
      input in = source(1048601, 19);
      struct stat st;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      REQUIRE(stat(path, &st) == 0 && st.st_size == PS_DEFAULT_CAPACITY);
      ok(lc_pouch_state_read(p, "a/b", "missing", &r, &e), &e);
      REQUIRE(!r.found);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      write.has_expected_version = 1;
      write.has_metadata = 1;
      write.metadata = (const unsigned char *)"metadata";
      write.metadata_length = 8;
      write.has_query_hidden = 1;
      write.query_hidden = 1;
      write.content_type = "application/test";
      ok(put(p, "a/b", "key", &in, &write, &w, &e), &e);
      REQUIRE(in.calls > 1 && w.version == 1 && w.index_seq == 1 &&
              w.bytes == in.length);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      read_value(p, "a/b", "key", in.length, 19, 1, &e);
      /* This version's read pin and crypto binding survive replacement and
       * rotation while the same key acquires a new body and version. */
      ok(lc_pouch_state_read(p, "a/b", "key", &r, &e), &e);
      in = source(70, 2);
      REQUIRE(put(p, "a/b", "key", &in, &write, &w, &e) == LC_ERR_INVALID);
      REQUIRE(in.calls == 0);
      clear(&e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      write.expected_version = 1;
      write.has_metadata = 0;
      write.has_query_hidden = 0;
      in = source(70001, 3);
      in.fail_after = 17000;
      REQUIRE(put(p, "a/b", "key", &in, &write, &w, &e) == LC_ERR_TRANSPORT);
      clear(&e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      read_value(p, "a/b", "key", 1048601, 19, 1, &e);
      in = source(70001, 7);
      ok(put(p, "a/b", "key", &in, &write, &w, &e), &e);
      REQUIRE(w.version == 2 && w.has_query_hidden && w.query_hidden &&
              w.metadata_length == 8 && !memcmp(w.metadata, "metadata", 8));
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      REQUIRE(r.version == 1);
      drain(r.body, 1048601, 19, &e);
      ok(r.body->reset(r.body, &e), &e);
      drain(r.body, 1048601, 19, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      ok(lc_pouch_state_read_metadata(p, "a/b", "key", &r, &e), &e);
      REQUIRE(r.found && !r.body && r.version == 2 && r.metadata_length == 8);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      /* Caller-owned small memory bodies retain the production optimized path.
       */
      {
        unsigned char bytes[31];
        lc_source *memory = NULL;
        for (size_t i = 0; i < sizeof(bytes); ++i)
          bytes[i] = byte_at(i, 11);
        ok(lc_source_from_memory(bytes, sizeof(bytes), &memory, &e), &e);
        ok(lc_pouch_state_write(p, "a/b", "small", memory, NULL, &w, &e), &e);
        lc_source_close(memory);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
      }
      one_file(directory);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      /* Reopen a populated root, inheriting compression policy. */
      o.logical.compression = NULL;
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      read_value(p, "a/b", "key", 70001, 7, 2, &e);
      {
        output out = {0, 70001, 7};
        lc_sink sink = {0};
        sink.write = output_write;
        sink.impl = &out;
        ok(lc_pouch_state_copy(p, "a/b", "key", &sink, &r, &e), &e);
        REQUIRE(out.offset == out.length && r.found && !r.body &&
                r.version == 2);
        lc_pouch_state_read_result_cleanup(&allocator, &r);
      }
      read_value(p, "a/b", "small", 31, 11, 1, &e);
      in = source(0, 0);
      ok(put(p, "a/b", "empty", &in, NULL, &w, &e), &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      read_value(p, "a/b", "empty", 0, 0, 1, &e);
      ok(lc_pouch_state_read(p, "a/b", "key", &r, &e), &e);
      lc_pouch_close(p);
      REQUIRE(live > 0);
      {
        lc_pouch *other = NULL;
        REQUIRE(ps_pouch_state_open(path, 0, &allocator, &o, &other, &e) ==
                LC_ERR_INVALID);
        REQUIRE(other == NULL);
        clear(&e);
      }
      drain(r.body, 70001, 7, &e);
      ok(r.body->reset(r.body, &e), &e);
      drain(r.body, 70001, 7, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      one_file(directory);
      REQUIRE(unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}

static size_t fault_case(const char *path, size_t nth, int phase, int transform,
                         int sync) {
  ps_pouch_state_options o = options(transform, sync);
  lc_pouch *p = NULL;
  lc_error e;
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result r = {0};
  lc_pouch_state_write_options write = {0};
  input in = source(1009, 31);
  size_t observed;
  int rc;
  lc_error_init(&e);
  if (phase != 0) {
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    ok(put(p, "test", "anchor", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    in = source(42, 10);
    ok(put(p, "test", "key", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    if (phase == 3) {
      lc_pouch_close(p);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    }
  }
  allocation = 0;
  write.has_metadata = 1;
  write.metadata = (const unsigned char *)"fault metadata";
  write.metadata_length = 14;
  fail_at = nth;
  __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
  if (phase == 0) {
    rc = ps_pouch_state_open(path, 1, &allocator, &o, &p, &e);
  } else if (phase <= 2) {
    if (phase == 1) {
      in = source(65569, 12);
      rc = put(p, "test", "key", &in, &write, &w, &e);
    } else {
      unsigned char bytes[31];
      lc_source *memory = NULL;
      for (size_t i = 0; i < sizeof(bytes); ++i)
        bytes[i] = byte_at(i, 12);
      rc = lc_source_from_memory(bytes, sizeof(bytes), &memory, &e);
      if (rc == LC_OK)
        rc = lc_pouch_state_write(p, "test", "key", memory, &write, &w, &e);
      lc_source_close(memory);
    }
  } else {
    rc = lc_pouch_state_read(p, "test", "anchor", &r, &e);
  }
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  observed = allocation;
  if (nth == SIZE_MAX)
    ok(rc, &e);
  else
    REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  clear(&e);
  if (phase >= 3)
    read_value(p, "test", "anchor", 1009, 31, 1, &e);
  lc_pouch_close(p);
  if (live)
    fprintf(stderr,
            "leak at allocation %lu phase %d transform %d sync %d: %lu live\n",
            (unsigned long)nth, phase, transform, sync, (unsigned long)live);
  REQUIRE(live == 0);
  if (phase != 0) {
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    read_value(p, "test", "anchor", 1009, 31, 1, &e);
    ok(lc_pouch_state_read(p, "test", "key", &r, &e), &e);
    REQUIRE(r.found && (r.version == 1 || r.version == 2));
    if (r.version == 1)
      drain(r.body, 42, 10, &e);
    else {
      REQUIRE(r.metadata_length == 14 &&
              !memcmp(r.metadata, "fault metadata", 14));
      drain(r.body, phase == 2 ? 31 : 65569, 12, &e);
    }
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    lc_pouch_close(p);
    REQUIRE(live == 0);
  }
  if (access(path, F_OK) == 0)
    REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
  return observed;
}
static void faults(const char *path) {
  size_t count = 0;
  for (int transform = 0; transform <= 3; transform += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int phase = 0; phase < 5; ++phase) {
        size_t total = fault_case(path, SIZE_MAX, phase, transform, sync);
        for (size_t n = 1; n <= total; ++n)
          (void)fault_case(path, n, phase, transform, sync);
        count += total;
      }
  printf("production state allocation failure sites: %lu\n",
         (unsigned long)count);
}

typedef struct io_fault {
  size_t calls, fail;
  size_t read_bytes;
  int armed, partial;
} io_fault;
static int io_fails(io_fault *f) {
  if (f->armed && ++f->calls == f->fail) {
    errno = EIO;
    return 1;
  }
  return 0;
}
static ssize_t io_read(void *context, int fd, void *p, size_t n, off_t at) {
  io_fault *f = context;
  f->read_bytes += n;
  return pread(fd, p, n, at);
}
static ssize_t io_write(void *context, int fd, const void *p, size_t n,
                        off_t at) {
  io_fault *f = context;
  if (io_fails(f)) {
    /* Some failed operations have already persisted a prefix. */
    if (f->partial && n > 1)
      (void)pwrite(fd, p, n / 2, at);
    errno = EIO;
    return -1;
  }
  return pwrite(fd, p, n, at);
}
static int io_sync(void *context, int fd) {
  return io_fails(context) ? -1 : fdatasync(fd);
}
static int io_reserve(void *context, int fd, off_t at, off_t n) {
  return io_fails(context) ? -1 : fallocate(fd, 0, at, n);
}
static size_t io_case(const char *path, size_t nth, int sync, int partial) {
  io_fault f = {0};
  ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(1, sync);
  lc_pouch *p = NULL;
  lc_error e;
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result r = {0};
  input in = source(1009, 31);
  size_t count;
  int rc;
  o.io = &io;
  lc_error_init(&e);
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  ok(put(p, "test", "anchor", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  f.armed = 1;
  f.fail = nth;
  f.partial = partial;
  in = source(65569, 12);
  rc = put(p, "test", "key", &in, NULL, &w, &e);
  f.armed = 0;
  count = f.calls;
  if (nth == SIZE_MAX)
    ok(rc, &e);
  else
    REQUIRE(rc == LC_OK || rc == LC_ERR_TRANSPORT);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  clear(&e);
  {
    /* The fault seam can poison physical ownership. Even a cached metadata
     * lookup must then fail admission instead of returning its old projection.
     * A healthy rollback continues to expose the unchanged anchor. */
    int health = p->storage.ops->check(p->storage.context, &e);
    clear(&e);
    rc = lc_pouch_state_read_metadata(p, "test", "anchor", &r, &e);
    if (health != LC_OK)
      REQUIRE(rc == health);
    else {
      ok(rc, &e);
      REQUIRE(r.found && r.version == 1 && r.bytes == 1009 && !r.body);
    }
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    clear(&e);
  }
  lc_pouch_close(p);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  read_value(p, "test", "anchor", 1009, 31, 1, &e);
  ok(lc_pouch_state_read(p, "test", "key", &r, &e), &e);
  if (r.found) {
    REQUIRE(r.version == 1);
    drain(r.body, 65569, 12, &e);
  }
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  in = source(31, 7);
  ok(put(p, "test", "retry", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  read_value(p, "test", "retry", 31, 7, 1, &e);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void io_faults(const char *path) {
  size_t count = 0;
  for (int sync = 0; sync < 2; ++sync)
    for (int partial = 0; partial < 2; ++partial) {
      size_t total = io_case(path, SIZE_MAX, sync, partial);
      for (size_t n = 1; n <= total; ++n)
        (void)io_case(path, n, sync, partial);
      count += total;
    }
  printf("production state I/O failure sites: %lu\n", (unsigned long)count);
}

typedef struct gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int entered, released;
} gate;
typedef struct job {
  lc_pouch *pouch;
  gate *held;
  int rc, started;
  size_t calls;
} job;
static size_t held_read(lc_source *pub, void *bytes, size_t length,
                        lc_error *e) {
  job *j = pub->impl;
  gate *g = j->held;
  (void)bytes;
  (void)length;
  (void)e;
  pthread_mutex_lock(&g->mutex);
  g->entered = 1;
  pthread_cond_broadcast(&g->changed);
  while (!g->released)
    pthread_cond_wait(&g->changed, &g->mutex);
  pthread_mutex_unlock(&g->mutex);
  return 0;
}
static void *held_writer(void *context) {
  job *j = context;
  lc_error e;
  lc_source body = {0};
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_write_options options = {0};
  input in = source(31, 9);
  options.has_expected_version = 1;
  options.expected_version = 1;
  body.read = held_read;
  body.impl = j;
  lc_error_init(&e);
  __atomic_store_n(&j->started, 1, __ATOMIC_RELEASE);
  if (j->held)
    j->rc =
        lc_pouch_state_write(j->pouch, "held", "key", &body, &options, &w, &e);
  else {
    j->rc = put(j->pouch, "held", "key", &in, &options, &w, &e);
    j->calls = in.calls;
  }
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(const char *path, const char *directory) {
  for (int sync = 0; sync < 2; ++sync) {
    ps_pouch_state_options o = options(3, sync);
    lc_pouch *p = NULL;
    lc_error e;
    lc_pouch_state_write_result w = {0};
    gate g;
    job j = {0}, contender = {0};
    pthread_t producer, competing;
    input in = source(1009, 31);
    memset(&g, 0, sizeof(g));
    REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&g.changed, NULL) == 0);
    lc_error_init(&e);
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    ok(put(p, "held", "anchor", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    in = source(42, 10);
    ok(put(p, "held", "key", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    j.pouch = p;
    j.held = &g;
    j.rc = -1;
    REQUIRE(pthread_create(&producer, NULL, held_writer, &j) == 0);
    pthread_mutex_lock(&g.mutex);
    while (!g.entered)
      pthread_cond_wait(&g.changed, &g.mutex);
    pthread_mutex_unlock(&g.mutex);
    contender.pouch = p;
    REQUIRE(pthread_create(&competing, NULL, held_writer, &contender) == 0);
    while (!__atomic_load_n(&contender.started, __ATOMIC_ACQUIRE))
      sched_yield();
    /* Same-namespace reads and another namespace's write finish before the
     * held producer can return. No timeout or scheduling sleep proves this. */
    read_value(p, "held", "anchor", 1009, 31, 1, &e);
    in = source(65569, 12);
    ok(put(p, "independent", "key", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    read_value(p, "independent", "key", 65569, 12, 1, &e);
    one_file(directory);
    pthread_mutex_lock(&g.mutex);
    REQUIRE(!g.released);
    g.released = 1;
    pthread_cond_broadcast(&g.changed);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(pthread_join(producer, NULL) == 0 && j.rc == LC_OK);
    REQUIRE(pthread_join(competing, NULL) == 0 &&
            contender.rc == LC_ERR_INVALID && contender.calls == 0);
    read_value(p, "held", "key", 0, 0, 2, &e);
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    read_value(p, "held", "anchor", 1009, 31, 1, &e);
    read_value(p, "held", "key", 0, 0, 2, &e);
    read_value(p, "independent", "key", 65569, 12, 1, &e);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(path) == 0);
    pthread_cond_destroy(&g.changed);
    pthread_mutex_destroy(&g.mutex);
    lc_error_cleanup(&e);
  }
}
static int reject_view(const lc_pouch_state_precondition_view *view,
                       void *context, lc_error *e) {
  size_t *calls = context;
  ++*calls;
  REQUIRE(view->found && view->has_body && view->version == 1);
  return lc_error_set(e, LC_ERR_INVALID, 0, "rejected by test view", NULL, NULL,
                      "test");
}
static void metadata_read(lc_pouch *p, const char *key, const char *value,
                          uint64_t version, int has_body, lc_error *e) {
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "mutation/ns", key, &r, e), e);
  REQUIRE(r.found && r.version == version && r.has_body == has_body && !r.body);
  REQUIRE(
      r.metadata_length == strlen(value) &&
      (!r.metadata_length || !memcmp(r.metadata, value, r.metadata_length)));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
}
static int hint(lc_pouch *p, const char *name, lc_error *e) {
  void *ns = NULL;
  int present = -1;
  ok(p->storage.ops->namespace_open(p->storage.context, name, 0, &ns, e), e);
  ok(ps_namespace_retention_hint_get(ns, &present, e), e);
  p->storage.ops->namespace_close(p->storage.context, ns);
  return present;
}
static void mutations(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int transform = 0; transform < 4; ++transform)
    for (int sync = 0; sync < 2; ++sync) {
      ps_pouch_state_options o = options(transform, sync);
      lc_pouch_state_write_options wopts = {0}, meta = {0};
      lc_pouch_state_write_result w = {0};
      lc_pouch_state_read_result r = {0}, pinned = {0};
      lc_pouch *p = NULL;
      input in = source(65569, 31);
      size_t rejected = 0;
      uint64_t seq;
      char *etag;
      o.logical.segment_target_bytes = 1;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      wopts.has_metadata = 1;
      wopts.metadata = (const unsigned char *)"before";
      wopts.metadata_length = 6;
      ok(put(p, "mutation/ns", "key", &in, &wopts, &w, &e), &e);
      REQUIRE(w.index_seq == 1 && w.version == 1);
      etag = lc_strdup_with_allocator(&allocator, w.etag);
      REQUIRE(etag != NULL);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      ok(lc_pouch_state_read(p, "mutation/ns", "key", &pinned, &e), &e);
      meta.has_metadata = 1;
      meta.metadata = (const unsigned char *)"after";
      meta.metadata_length = 5;
      meta.has_expected_version = 1;
      meta.expected_version = 99;
      REQUIRE(lc_pouch_state_update_metadata(p, "mutation/ns", "key", &meta, &w,
                                             &e) == LC_ERR_INVALID);
      clear(&e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      meta.expected_version = 1;
      meta.view_precondition = reject_view;
      meta.view_precondition_context = &rejected;
      REQUIRE(lc_pouch_state_update_metadata(p, "mutation/ns", "key", &meta, &w,
                                             &e) == LC_ERR_INVALID);
      REQUIRE(rejected == 1);
      clear(&e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      meta.view_precondition = NULL;
      meta.has_query_hidden = 1;
      meta.query_hidden = 1;
      meta.has_retention = 1;
      meta.retention = 4102444800LL;
      ok(lc_pouch_state_update_metadata(p, "mutation/ns", "key", &meta, &w, &e),
         &e);
      REQUIRE(w.version == 1 && w.index_seq == 2 && !strcmp(w.etag, etag) &&
              w.bytes == 65569 && w.retention == meta.retention &&
              w.has_query_hidden && w.query_hidden);
      seq = w.index_seq;
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      metadata_read(p, "key", "after", 1, 1, &e);
      REQUIRE(hint(p, "mutation/ns", &e) == 1);
      read_value(p, "mutation/ns", "key", 65569, 31, 1, &e);
      lc_pouch_close(p);
      drain(pinned.body, 65569, 31, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &pinned);
      REQUIRE(live == 1); /* The caller still owns its etag copy. */
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      metadata_read(p, "key", "after", 1, 1, &e);
      REQUIRE(hint(p, "mutation/ns", &e) == 1);
      ok(lc_pouch_state_read(p, "mutation/ns", "key", &pinned, &e), &e);
      REQUIRE(pinned.index_seq == seq && pinned.retention == meta.retention &&
              pinned.has_query_hidden && pinned.query_hidden &&
              !strcmp(pinned.etag, etag));
      memset(&meta, 0, sizeof(meta));
      meta.has_expected_version = 1;
      meta.expected_version = 99;
      REQUIRE(lc_pouch_state_delete(p, "mutation/ns", "key", &meta, &w, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      meta.expected_version = 1;
      meta.expected_etag = "wrong";
      REQUIRE(lc_pouch_state_delete(p, "mutation/ns", "key", &meta, &w, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      meta.expected_etag = etag;
      ok(lc_pouch_state_delete(p, "mutation/ns", "key", &meta, &w, &e), &e);
      REQUIRE(w.version == 2 && w.index_seq == seq + 1);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      ok(lc_pouch_state_read(p, "mutation/ns", "key", &r, &e), &e);
      REQUIRE(!r.found && !r.body);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      drain(pinned.body, 65569, 31, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &pinned);
      ok(lc_pouch_state_delete(p, "mutation/ns", "key", NULL, &w, &e), &e);
      REQUIRE(w.version == 0);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      lc_pouch_close(p);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      ok(lc_pouch_state_read_metadata(p, "mutation/ns", "key", &r, &e), &e);
      REQUIRE(!r.found);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      in = source(31, 9);
      ok(put(p, "mutation/ns", "key", &in, NULL, &w, &e), &e);
      REQUIRE(w.version == 1 && w.index_seq == seq + 2);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      memset(&meta, 0, sizeof(meta));
      meta.has_metadata = 1;
      meta.metadata = (const unsigned char *)"placeholder";
      meta.metadata_length = 11;
      meta.has_expected_version = 1;
      meta.has_query_hidden = meta.query_hidden = 1;
      ok(lc_pouch_state_update_metadata(p, "mutation/ns", "empty", &meta, &w,
                                        &e),
         &e);
      REQUIRE(w.version == 0 && !w.has_body);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      metadata_read(p, "empty", "placeholder", 0, 0, &e);
      ok(lc_pouch_state_delete(p, "mutation/ns", "empty", NULL, &w, &e), &e);
      REQUIRE(w.version == 0);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      metadata_read(p, "empty", "placeholder", 0, 0, &e);
      in = source(31, 7);
      ok(put(p, "mutation/ns", "empty", &in, NULL, &w, &e), &e);
      REQUIRE(w.version == 1 && !w.query_hidden);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      metadata_read(p, "empty", "placeholder", 1, 1, &e);
      /* Unsupported lease/staging options cannot publish partial state. */
      meta.query_index_operation_active = 1;
      REQUIRE(lc_pouch_state_update_metadata(p, "mutation/ns", "empty", &meta,
                                             &w, &e) == LC_ERR_UNSUPPORTED);
      clear(&e);
      REQUIRE(lc_pouch_state_delete(p, "mutation/ns", "empty", &meta, &w, &e) ==
              LC_ERR_UNSUPPORTED);
      clear(&e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      one_file(directory);
      lc_pouch_close(p);
      lc_free_with_allocator(&allocator, etag);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}

static unsigned char mutation_metadata[20001];
static size_t mutation_fault_case(const char *path, size_t nth, int operation,
                                  int transform, int sync, int use_io) {
  io_fault f = {0};
  ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(transform, sync);
  lc_pouch *p = NULL;
  lc_error e;
  lc_pouch_state_write_options write = {0}, change = {0};
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result r = {0};
  input in = source(1009, 31);
  size_t count;
  int rc, applied = 0, health;
  const char *key = operation == 2 ? "empty" : "key";
  lc_error_init(&e);
  o.logical.segment_target_bytes = 4096;
  if (use_io)
    o.io = &io;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  ok(put(p, "mutation/ns", "anchor", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  in = source(65569, 12);
  write.has_metadata = 1;
  write.metadata = (const unsigned char *)"before";
  write.metadata_length = 6;
  ok(put(p, "mutation/ns", "key", &in, &write, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  change.has_metadata = 1;
  change.metadata = mutation_metadata;
  change.metadata_length = sizeof(mutation_metadata);
  change.has_expected_version = 1;
  change.expected_version = operation == 2 ? 0 : 1;
  change.has_retention = operation == 0;
  change.retention = 4102444800LL;
  change.has_query_hidden = change.query_hidden = 1;
  allocation = 0;
  fail_at = nth;
  if (use_io) {
    f.armed = 1;
    f.fail = nth;
    f.partial = 1;
  } else
    __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
  if (operation == 1)
    rc = lc_pouch_state_delete(p, "mutation/ns", key, &change, &w, &e);
  else
    rc = lc_pouch_state_update_metadata(p, "mutation/ns", key, &change, &w, &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  count = use_io ? f.calls : allocation;
  if (rc != LC_OK && rc != LC_ERR_NOMEM && !(use_io && rc == LC_ERR_TRANSPORT))
    fprintf(stderr,
            "mutation failure %lu op %d transform %d sync %d IO %d rc %d: %s\n",
            (unsigned long)nth, operation, transform, sync, use_io, rc,
            e.message ? e.message : "?");
  if (nth == SIZE_MAX)
    ok(rc, &e);
  else
    REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM ||
            (use_io && rc == LC_ERR_TRANSPORT));
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  clear(&e);
  health = p->storage.ops->check(p->storage.context, &e);
  clear(&e);
  rc = lc_pouch_state_read_metadata(p, "mutation/ns", "anchor", &r, &e);
  if (health != LC_OK)
    REQUIRE(rc == health);
  else {
    ok(rc, &e);
    REQUIRE(r.found && r.version == 1 && r.bytes == 1009);
  }
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  clear(&e);
  lc_pouch_close(p);
  if (live)
    fprintf(stderr, "mutation leak %lu op %d transform %d sync %d IO %d: %lu\n",
            (unsigned long)nth, operation, transform, sync, use_io,
            (unsigned long)live);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  read_value(p, "mutation/ns", "anchor", 1009, 31, 1, &e);
  ok(lc_pouch_state_read_metadata(p, "mutation/ns", key, &r, &e), &e);
  if (operation == 1) {
    if (r.found) {
      REQUIRE(r.version == 1 && r.metadata_length == 6 &&
              !memcmp(r.metadata, "before", 6));
      read_value(p, "mutation/ns", key, 65569, 12, 1, &e);
    } else
      applied = 1;
  } else if (r.found) {
    REQUIRE(r.version == (operation == 2 ? 0 : 1) &&
            r.has_body == (operation == 0));
    if (r.metadata_length == sizeof(mutation_metadata)) {
      REQUIRE(
          !memcmp(r.metadata, mutation_metadata, sizeof(mutation_metadata)) &&
          r.query_hidden);
      applied = 1;
      if (operation == 0) {
        REQUIRE(r.retention == change.retention &&
                hint(p, "mutation/ns", &e) == 1);
      }
    } else
      REQUIRE(operation == 0 && r.metadata_length == 6 &&
              !memcmp(r.metadata, "before", 6));
    if (operation == 0)
      read_value(p, "mutation/ns", key, 65569, 12, 1, &e);
  } else
    REQUIRE(operation == 2);
  if (nth == SIZE_MAX)
    REQUIRE(applied);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  change.has_retention = 0;
  change.expected_version = 0;
  ok(lc_pouch_state_update_metadata(p, "mutation/ns", "retry", &change, &w, &e),
     &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void mutation_faults(const char *path, int use_io) {
  size_t count = 0;
  for (int transform = 0; transform <= 3; transform += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int operation = 0; operation < 3; ++operation) {
        size_t total = mutation_fault_case(path, SIZE_MAX, operation, transform,
                                           sync, use_io);
        for (size_t n = 1; n <= total; ++n)
          (void)mutation_fault_case(path, n, operation, transform, sync,
                                    use_io);
        count += total;
      }
  printf("metadata/delete %s failure sites: %lu\n",
         use_io ? "I/O" : "allocation", (unsigned long)count);
}

static void hint_checksum(unsigned char bytes[32]) {
  uint64_t sum = crc32(0L, bytes, 24);
  for (size_t i = 0; i < 8; ++i)
    bytes[24 + i] = (unsigned char)(sum >> (i * 8));
}
static void hint_store(const char *path, ps_key key, const void *bytes,
                       size_t length, lc_error *e) {
  ps_space *space = NULL;
  ps_writer *writer = NULL;
  ok(ps_open(path, 0, NULL, &space, e), e);
  ok(ps_begin(space, key, &writer, e), e);
  ok(ps_write(writer, bytes, length, e), e);
  ok(ps_publish(writer, NULL, e), e);
  ps_writer_close(writer);
  ok(ps_close(space, e), e);
}
static void retention_hint(const char *path, const char *directory) {
  io_fault f = {0};
  ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(0, 0);
  ps_root_options raw_options = {0};
  lc_pouch *p = NULL;
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_info identity;
  ps_key key = {0, 2, PS_CONTROL};
  lc_pouch_state_write_options write = {0};
  lc_pouch_state_write_result w = {0};
  lc_error e;
  input in = source(1048601, 9);
  unsigned char valid[32] = {0}, bytes[33];
  int present;
  lc_error_init(&e);
  o.io = &io;
  write.has_retention = 1;
  write.retention = 4102444800LL;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  ok(put(p, "hint", "key", &in, &write, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  REQUIRE(hint(p, "hint", &e) == 1);
  lc_pouch_close(p);
  REQUIRE(live == 0);
  raw_options.storage.io = &io;
  ok(ps_root_open(path, 0, &raw_options, &root, &e), &e);
  ok(ps_root_namespace_acquire(root, "hint", 0, &ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  f.read_bytes = 0;
  ok(ps_namespace_retention_hint_get(ns, &present, &e), &e);
  REQUIRE(present == 1 && f.read_bytes <= PS_CHUNK);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  memcpy(valid, "LCPSRETN", 8);
  key.owner = identity.owner;
  for (size_t i = 0; i < 8; ++i)
    valid[8 + i] = (unsigned char)(key.owner >> (i * 8));
  valid[16] = 1;
  hint_checksum(valid);
  /* Valid outer storage checksums do not excuse a corrupt typed marker. */
  for (size_t bad = 0; bad < 37; ++bad) {
    size_t length = sizeof(valid);
    memset(bytes, 0, sizeof(bytes));
    memcpy(bytes, valid, sizeof(valid));
    if (bad < 32)
      bytes[bad] ^= 1;
    else if (bad == 32)
      length = 31;
    else if (bad == 33)
      length = 33;
    else {
      bytes[bad == 34 ? 8 : bad == 35 ? 16 : 17] ^= 1;
      hint_checksum(bytes);
    }
    hint_store(path, key, bytes, length, &e);
    ok(ps_root_open(path, 0, &raw_options, &root, &e), &e);
    ok(ps_root_namespace_acquire(root, "hint", 0, &ns, &e), &e);
    REQUIRE(ps_namespace_retention_hint_get(ns, &present, &e) ==
            LC_ERR_PROTOCOL);
    clear(&e);
    REQUIRE(ps_namespace_retention_hint_ensure(ns, &e) == LC_ERR_PROTOCOL);
    clear(&e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
  }
  hint_store(path, key, valid, sizeof(valid), &e);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  REQUIRE(hint(p, "hint", &e) == 1);
  read_value(p, "hint", "key", 1048601, 9, 1, &e);
  one_file(directory);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
}

typedef struct metadata_job {
  lc_pouch *p;
  gate *start;
  size_t id;
  uint64_t sequence[8];
} metadata_job;
static void *metadata_writer(void *context) {
  metadata_job *j = context;
  lc_pouch_state_write_options change = {0};
  lc_pouch_state_write_result w = {0};
  lc_error e;
  char key[32], value[32];
  lc_error_init(&e);
  snprintf(key, sizeof(key), "key-%lu", (unsigned long)j->id);
  change.has_expected_version = 1;
  change.expected_version = 1;
  change.has_metadata = 1;
  pthread_mutex_lock(&j->start->mutex);
  while (!j->start->released)
    pthread_cond_wait(&j->start->changed, &j->start->mutex);
  pthread_mutex_unlock(&j->start->mutex);
  for (size_t n = 0; n < 8; ++n) {
    snprintf(value, sizeof(value), "%lu/%lu", (unsigned long)j->id,
             (unsigned long)n);
    change.metadata = (const unsigned char *)value;
    change.metadata_length = strlen(value);
    ok(lc_pouch_state_update_metadata(j->p, "batch", key, &change, &w, &e), &e);
    REQUIRE(w.version == 1 && w.bytes == 1009);
    j->sequence[n] = w.index_seq;
    lc_pouch_state_write_result_cleanup(&allocator, &w);
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void mutation_concurrency(const char *path, const char *directory) {
  for (int sync = 0; sync < 2; ++sync) {
    ps_pouch_state_options o = options(3, sync);
    lc_pouch *p = NULL;
    lc_pouch_state_write_result w = {0};
    lc_pouch_state_read_result r = {0};
    lc_error e;
    gate g = {0};
    metadata_job jobs[8] = {0};
    pthread_t threads[8];
    int seen[73] = {0};
    char key[32], value[32];
    o.logical.segment_target_bytes = 4096;
    lc_error_init(&e);
    REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&g.changed, NULL) == 0);
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    for (size_t i = 0; i < 8; ++i) {
      input in = source(1009, (unsigned char)i);
      snprintf(key, sizeof(key), "key-%lu", (unsigned long)i);
      ok(put(p, "batch", key, &in, NULL, &w, &e), &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      jobs[i].p = p;
      jobs[i].start = &g;
      jobs[i].id = i;
      REQUIRE(pthread_create(&threads[i], NULL, metadata_writer, &jobs[i]) ==
              0);
    }
    pthread_mutex_lock(&g.mutex);
    g.released = 1;
    pthread_cond_broadcast(&g.changed);
    pthread_mutex_unlock(&g.mutex);
    for (size_t i = 0; i < 8; ++i) {
      REQUIRE(pthread_join(threads[i], NULL) == 0);
      for (size_t n = 0; n < 8; ++n) {
        uint64_t seq = jobs[i].sequence[n];
        REQUIRE(seq > 8 && seq <= 72 && !seen[seq]);
        seen[seq] = 1;
      }
    }
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    for (size_t i = 0; i < 8; ++i) {
      snprintf(key, sizeof(key), "key-%lu", (unsigned long)i);
      snprintf(value, sizeof(value), "%lu/7", (unsigned long)i);
      ok(lc_pouch_state_read_metadata(p, "batch", key, &r, &e), &e);
      REQUIRE(r.found && r.version == 1 && r.index_seq == jobs[i].sequence[7] &&
              r.metadata_length == strlen(value) &&
              !memcmp(r.metadata, value, r.metadata_length));
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      read_value(p, "batch", key, 1009, (unsigned char)i, 1, &e);
    }
    one_file(directory);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(path) == 0);
    pthread_cond_destroy(&g.changed);
    pthread_mutex_destroy(&g.mutex);
    lc_error_cleanup(&e);
  }
}

static lc_pouch_storage_namespace_info
storage_info(lc_pouch *p, const char *name, lc_error *e) {
  lc_pouch_storage_namespace_info info = {0};
  void *ns = NULL;
  ok(p->storage.ops->namespace_open(p->storage.context, name, 0, &ns, e), e);
  ok(p->storage.ops->namespace_info(p->storage.context, ns, &info, e), e);
  p->storage.ops->namespace_close(p->storage.context, ns);
  return info;
}
static void mutation_capacity(const char *path, const char *directory) {
  ps_pouch_state_options o = options(0, 0);
  lc_pouch *p = NULL;
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result r = {0};
  lc_error e;
  size_t count;
  char key[32];
  int rc, removed, peer_removed;
  lc_pouch_storage_namespace_info full_before, full_after, peer_before,
      peer_after, reopened;
  o.logical.segment_target_bytes = 1;
  lc_error_init(&e);
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  {
    input victim = source(65569, 99);
    ok(put(p, "victim", "key", &victim, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
  }
  for (count = 0; count < 256; ++count) {
    input in = source(65569, (unsigned char)count);
    snprintf(key, sizeof(key), "key-%lu", (unsigned long)count);
    rc = put(p, "full", key, &in, NULL, &w, &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    if (rc != LC_OK) {
      REQUIRE(rc == LC_ERR_INVALID && e.message &&
              strstr(e.message, "capacity"));
      clear(&e);
      break;
    }
  }
  REQUIRE(count > 0 && count < 256);
  full_before = storage_info(p, "full", &e);
  peer_before = storage_info(p, "victim", &e);
  read_value(p, "full", "key-0", 65569, 0, 1, &e);
  rc = lc_pouch_state_delete(p, "full", "key-0", NULL, &w, &e);
  REQUIRE(rc == LC_OK || rc == LC_ERR_INVALID);
  if (rc != LC_OK)
    REQUIRE(e.message && strstr(e.message, "capacity"));
  removed = rc == LC_OK;
  clear(&e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  full_after = storage_info(p, "full", &e);
  if (!removed)
    REQUIRE(full_after.active_segment == full_before.active_segment &&
            full_after.tail == full_before.tail);
  ok(lc_pouch_state_read_metadata(p, "full", "key-0", &r, &e), &e);
  REQUIRE(r.found == !removed);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  rc = lc_pouch_state_delete(p, "victim", "key", NULL, &w, &e);
  REQUIRE(rc == LC_OK || rc == LC_ERR_INVALID);
  if (rc != LC_OK)
    REQUIRE(e.message && strstr(e.message, "capacity"));
  peer_removed = rc == LC_OK;
  clear(&e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  peer_after = storage_info(p, "victim", &e);
  if (!peer_removed)
    REQUIRE(peer_after.active_segment == peer_before.active_segment &&
            peer_after.tail == peer_before.tail);
  lc_pouch_close(p);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  reopened = storage_info(p, "full", &e);
  REQUIRE(reopened.active_segment == full_after.active_segment &&
          reopened.tail == full_after.tail);
  reopened = storage_info(p, "victim", &e);
  REQUIRE(reopened.active_segment == peer_after.active_segment &&
          reopened.tail == peer_after.tail);
  ok(lc_pouch_state_read_metadata(p, "full", "key-0", &r, &e), &e);
  REQUIRE(r.found == !removed);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  if (!removed)
    read_value(p, "full", "key-0", 65569, 0, 1, &e);
  ok(lc_pouch_state_read_metadata(p, "victim", "key", &r, &e), &e);
  REQUIRE(r.found == !peer_removed);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  if (!peer_removed)
    read_value(p, "victim", "key", 65569, 99, 1, &e);
  one_file(directory);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  printf("capacity probe: %lu live keys; deletion %s; peer deletion %s; "
         "protected maintenance "
         "admission remains an acceptance requirement\n",
         (unsigned long)count,
         removed ? "committed" : "needs maintenance admission",
         peer_removed ? "committed" : "needs maintenance admission");
  lc_error_cleanup(&e);
}

static int bounds_failed;
static void key_bounds(const char *directory) {
  char path[1400];
  const size_t body_key_bytes = 60U * 1024U;
  char *key = lc_alloc_with_allocator(&allocator, PS_MIB + 2);
  lc_error e;
  REQUIRE(key != NULL);
  memset(key, 'k', PS_MIB + 1);
  key[PS_MIB + 1] = 0;
  lc_error_init(&e);
  for (int container = 1; container >= 0; --container)
    for (int transform = 0; transform <= 3; transform += 3)
      for (int sync = 1; sync >= 0; --sync) {
        ps_pouch_state_options o = options(transform, sync);
        lc_pouch *p = NULL;
        lc_pouch_state_write_result w = {0};
        lc_pouch_state_write_options change = {0};
        lc_pouch_state_read_result r = {0};
        input in = source(17, 12);
        int rc;
        REQUIRE(snprintf(path, sizeof(path), "%s/bounds-%d-%d-%d", directory,
                         container, transform, sync) < (int)sizeof(path));
        if (container)
          ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
        else
          ok(lc_pouch_open(path, &allocator, &o.logical, &p, &e), &e);
        key[PS_MIB] = 'k';
        rc = put(p, "bounds", key, &in, NULL, &w, &e);
        if (rc != LC_ERR_INVALID || in.calls) {
          fprintf(stderr,
                  "oversized key admitted/consumed source: container=%d "
                  "transform=%d sync=%d rc=%d calls=%lu\n",
                  container, transform, sync, rc, (unsigned long)in.calls);
          bounds_failed = 1;
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          lc_pouch_close(p);
          lc_free_with_allocator(&allocator, key);
          lc_error_cleanup(&e);
          return; /* Let main remove the owned fixture before failing. */
        }
        clear(&e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        change.has_metadata = 1;
        change.metadata = (const unsigned char *)"bound";
        change.metadata_length = 5;
        rc = lc_pouch_state_update_metadata(p, "bounds", key, &change, &w, &e);
        if (rc != LC_ERR_INVALID) {
          fprintf(stderr,
                  "oversized metadata key: container=%d sync=%d rc=%d\n",
                  container, sync, rc);
          bounds_failed = 1;
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          lc_pouch_close(p);
          if (container && rc == LC_OK) {
            ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
            rc = lc_pouch_state_read_metadata(p, "bounds", key, &r, &e);
            fprintf(stderr, "oversized key after reopen: rc=%d found=%d\n", rc,
                    r.found);
            lc_pouch_state_read_result_cleanup(&allocator, &r);
            lc_pouch_close(p);
          }
          lc_free_with_allocator(&allocator, key);
          lc_error_cleanup(&e);
          return;
        }
        clear(&e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        REQUIRE(lc_pouch_state_delete(p, "bounds", key, NULL, &w, &e) ==
                LC_ERR_INVALID);
        clear(&e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        key[PS_MIB] = 0;
        /* Metadata-only keys reach the decoder limit. Body keys also occupy
         * the existing 16-bit payload-context field, so exercise 60 KiB. */
        ok(lc_pouch_state_update_metadata(p, "bounds", key, &change, &w, &e),
           &e);
        REQUIRE(w.version == 0 && w.index_seq == 1);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        lc_pouch_close(p);
        if (container)
          ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        else
          ok(lc_pouch_open(path, &allocator, &o.logical, &p, &e), &e);
        ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
        REQUIRE(r.found && !r.has_body && r.version == 0 && r.index_seq == 1 &&
                r.metadata_length == 5 && !memcmp(r.metadata, "bound", 5));
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        key[body_key_bytes] = 0;
        in = source(17, 12);
        ok(put(p, "bounds", key, &in, NULL, &w, &e), &e);
        REQUIRE(w.version == 1 && w.index_seq == 2);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        ok(lc_pouch_state_update_metadata(p, "bounds", key, &change, &w, &e),
           &e);
        REQUIRE(w.version == 1 && w.index_seq == 3);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        lc_pouch_close(p);
        REQUIRE(live == 1); /* Caller-owned key. */
        if (container)
          ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        else
          ok(lc_pouch_open(path, &allocator, &o.logical, &p, &e), &e);
        read_value(p, "bounds", key, 17, 12, 1, &e);
        ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
        REQUIRE(r.index_seq == 3 && r.metadata_length == 5 &&
                !memcmp(r.metadata, "bound", 5));
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        ok(lc_pouch_state_delete(p, "bounds", key, NULL, &w, &e), &e);
        REQUIRE(w.version == 2 && w.index_seq == 4);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        lc_pouch_close(p);
        if (container)
          ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        else
          ok(lc_pouch_open(path, &allocator, &o.logical, &p, &e), &e);
        ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
        REQUIRE(!r.found);
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        key[body_key_bytes] = 'k';
        ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
        REQUIRE(r.found && r.version == 0 && r.index_seq == 1 &&
                r.metadata_length == 5 && !memcmp(r.metadata, "bound", 5));
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        lc_pouch_close(p);
        REQUIRE(live == 1);
      }
  lc_free_with_allocator(&allocator, key);
  REQUIRE(live == 0);
  lc_error_cleanup(&e);
}
static size_t read_length(unsigned index) {
  return index == 0 ? 0 : index == 1 ? 65569 : 17 + index;
}
static void seed_reads(lc_pouch *p, size_t count, lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result w = {0};
  char key[16];
  o.has_metadata = 1;
  o.metadata = (const unsigned char *)"before";
  o.metadata_length = 6;
  o.content_type = "application/test";
  for (size_t i = 0; i < count; ++i) {
    input in = source(read_length((unsigned)i), (unsigned char)(10 + i));
    snprintf(key, sizeof(key), "k%02u", (unsigned)i);
    ok(put(p, "reads", key, &in, &o, &w, e), e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
  }
}
typedef struct read_view {
  lc_pouch *pouch;
  const char *const *keys;
  size_t count, calls;
  unsigned seen;
  int bodies, mutate, repeated, stop;
  lc_source *saved;
} read_view;
static unsigned read_index(const char *key) {
  char *end;
  unsigned long index;
  REQUIRE(key[0] == 'k');
  index = strtoul(key + 1, &end, 10);
  REQUIRE(!*end && index < 12);
  return (unsigned)index;
}
static void mutate_reads(read_view *view, lc_error *e) {
  lc_pouch_state_write_result w = {0};
  input in = source(97, 201);
  ok(put(view->pouch, "reads", "k05", &in, NULL, &w, e), e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  ok(lc_pouch_state_delete(view->pouch, "reads", "k11", NULL, &w, e), e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  in = source(31, 99);
  ok(put(view->pouch, "reads", "k12", &in, NULL, &w, e), e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
}
static int read_many_visit(const char *key, const lc_pouch_state_read_result *r,
                           void *context, lc_error *e) {
  read_view *view = context;
  REQUIRE(view->calls < view->count && !strcmp(key, view->keys[view->calls]));
  if (!view->calls && view->mutate)
    mutate_reads(view, e);
  ++view->calls;
  if (strcmp(key, "missing") && strcmp(key, "meta") && strcmp(key, "k03")) {
    unsigned index = read_index(key);
    REQUIRE(r->found && r->has_body && r->body && r->version == 1 &&
            r->bytes == read_length(index) && r->metadata_length == 6 &&
            !memcmp(r->metadata, "before", 6) && r->content_type &&
            !strcmp(r->content_type, "application/test"));
    drain(r->body, read_length(index), (unsigned char)(10 + index), e);
    ok(r->body->reset(r->body, e), e);
    drain(r->body, read_length(index), (unsigned char)(10 + index), e);
  } else
    REQUIRE(!r->found && !r->body && !r->has_body);
  return view->stop ? LC_POUCH_STATE_READ_MANY_STOP : LC_OK;
}
static int read_summary(const lc_pouch_state_scan_summary_entry *entry,
                        void *context, lc_error *e) {
  read_view *view = context;
  unsigned index = read_index(entry->key);
  REQUIRE(!(view->seen & (1U << index)) && index != 3 && entry->version == 1 &&
          entry->bytes == read_length(index) && entry->content_type &&
          !strcmp(entry->content_type, "application/test") && entry->etag);
  if (view->seen)
    REQUIRE(view->seen < (1U << index));
  view->seen |= 1U << index;
  if (!view->calls && view->mutate)
    mutate_reads(view, e);
  ++view->calls;
  if (view->bodies)
    for (int repeat = 0; repeat <= view->repeated; ++repeat) {
      lc_pouch_state_read_result r = {0};
      ok(lc_pouch_state_scan_summary_read_body(view->pouch, "reads", entry,
                                               repeat, &r, e),
         e);
      REQUIRE(r.found && r.has_body && r.version == 1 &&
              r.bytes == read_length(index) && r.metadata_length == 6 &&
              !memcmp(r.metadata, "before", 6));
      if (index == 11 && !view->saved && view->mutate) {
        view->saved = r.body;
        r.body = NULL;
      } else {
        drain(r.body, read_length(index), (unsigned char)(10 + index), e);
        ok(r.body->reset(r.body, e), e);
        drain(r.body, read_length(index), (unsigned char)(10 + index), e);
      }
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      /* The row remains borrowed for the complete visitor, including after
       * body-result cleanup and a second independent body open. */
      REQUIRE(!strcmp(entry->content_type, "application/test") &&
              strlen(entry->etag) == 64 &&
              (entry->descriptor == NULL || entry->descriptor[0]));
    }
  return view->stop;
}
static int read_metadata_visit(const lc_pouch_state_visit_entry *entry,
                               void *context, lc_error *e) {
  read_view *view = context;
  (void)e;
  REQUIRE(entry->metadata_length == 6 && !memcmp(entry->metadata, "before", 6));
  if (!strcmp(entry->key, "meta"))
    REQUIRE(!entry->has_payload && entry->version == 0);
  else {
    unsigned index = read_index(entry->key);
    REQUIRE(index != 3 && entry->has_payload && entry->version == 1);
  }
  ++view->calls;
  return LC_OK;
}
static void reads(const char *path, const char *directory) {
  static const char *const keys[] = {"k01",  "missing", "k00", "k05", "k03",
                                     "meta", "k01",     "k11", "k10", "k09",
                                     "k08",  "k07",     "k06", "k04"};
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int transform = 0; transform < 4; ++transform)
      for (int sync = 0; sync < 2; ++sync)
        for (int phase = 0; phase < 3; ++phase) {
          ps_pouch_state_options o = options(transform, sync);
          io_fault f = {0};
          ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
          lc_pouch *p = NULL;
          lc_pouch_state_write_options metadata = {0};
          lc_pouch_state_write_result w = {0};
          lc_pouch_state_scan_summaries_result page = {0};
          lc_pouch_state_scan_summaries_result next_page = {0};
          read_view view = {0};
          char native[1400];
          const char *store = path;
          o.logical.segment_target_bytes = 1;
          o.io = &io;
          if (multifile) {
            snprintf(native, sizeof(native), "%s/native-%d-%d-%d", directory,
                     transform, sync, phase);
            store = native;
            ok(lc_pouch_open(store, &allocator, &o.logical, &p, &e), &e);
          } else
            ok(ps_pouch_state_open(store, 1, &allocator, &o, &p, &e), &e);
          seed_reads(p, 12, &e);
          ok(lc_pouch_state_delete(p, "reads", "k03", NULL, &w, &e), &e);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          metadata.has_metadata = 1;
          metadata.metadata = (const unsigned char *)"before";
          metadata.metadata_length = 6;
          ok(lc_pouch_state_update_metadata(p, "reads", "meta", &metadata, &w,
                                            &e),
             &e);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          view.pouch = p;
          view.keys = keys;
          view.count = sizeof(keys) / sizeof(keys[0]);
          if (phase == 0) {
            ok(lc_pouch_state_read_many(p, "reads", keys, view.count,
                                        read_many_visit, &view, &e),
               &e);
            REQUIRE(view.calls == view.count);
            view.calls = 0;
            view.stop = 1;
            ok(lc_pouch_state_read_many_cached(p, "reads", keys, view.count,
                                               read_many_visit, &view, &e),
               &e);
            REQUIRE(view.calls == 1);
            view.calls = 0;
            view.stop = 0;
            view.mutate = 1;
            ok(lc_pouch_state_read_many_cached(p, "reads", keys, view.count,
                                               read_many_visit, &view, &e),
               &e);
            REQUIRE(view.calls == view.count);
          } else if (phase == 1) {
            view.mutate = view.bodies = view.repeated = 1;
            ok(lc_pouch_state_scan_summaries(p, "reads", NULL, 0, read_summary,
                                             &view, &page, &e),
               &e);
            REQUIRE(view.calls == 11 && view.seen == (4095U ^ 8U) &&
                    !page.truncated && !page.next_start_after && view.saved);
          } else {
            size_t read_bytes = f.read_bytes;
            __atomic_store_n(&measured_max, 0, __ATOMIC_RELAXED);
            __atomic_store_n(&measure, 1, __ATOMIC_RELAXED);
            int rc = lc_pouch_state_scan_summaries(
                p, "reads", NULL, 4, read_summary, &view, &page, &e);
            __atomic_store_n(&measure, 0, __ATOMIC_RELAXED);
            ok(rc, &e);
            REQUIRE(view.calls == 4 && page.truncated &&
                    !strcmp(page.next_start_after, "k04"));
            if (!multifile)
              REQUIRE(f.read_bytes == read_bytes &&
                      __atomic_load_n(&measured_max, __ATOMIC_RELAXED) <
                          PS_CHUNK);
            view.calls = 0;
            view.seen = 0;
            ok(lc_pouch_state_scan_summaries(p, "reads", page.next_start_after,
                                             4, read_summary, &view, &next_page,
                                             &e),
               &e);
            REQUIRE(view.calls == 4 && view.seen == (32U | 64U | 128U | 256U));
            REQUIRE(next_page.truncated &&
                    !strcmp(next_page.next_start_after, "k08"));
            lc_pouch_state_scan_summaries_result_cleanup(&allocator,
                                                         &next_page);
            lc_pouch_state_scan_summaries_result_cleanup(&allocator, &page);
            view.calls = 0;
            ok(lc_pouch_state_visit(p, "reads", read_metadata_visit, &view, &e),
               &e);
            REQUIRE(view.calls == 12);
            view.calls = 0;
            ok(lc_pouch_state_visit_prefix(p, "reads", "k0",
                                           read_metadata_visit, &view, &e),
               &e);
            REQUIRE(view.calls == 9);
            view.calls = 0;
            view.seen = 0;
            view.stop = LC_POUCH_STATE_READ_MANY_STOP;
            REQUIRE(lc_pouch_state_scan_summaries(
                        p, "reads", NULL, 4, read_summary, &view, &page, &e) ==
                    LC_POUCH_STATE_READ_MANY_STOP);
            REQUIRE(view.calls == 1 && !page.truncated &&
                    !page.next_start_after);
            view.calls = 0;
            view.seen = 0;
            view.stop = 0;
            ok(lc_pouch_state_scan_summaries(p, "reads", "k99", 4, read_summary,
                                             &view, &page, &e),
               &e);
            REQUIRE(view.calls == 0 && !page.truncated &&
                    !page.next_start_after);
            {
              const char *bad[] = {"k01", NULL};
              REQUIRE(lc_pouch_state_read_many(p, "reads", bad, 2,
                                               read_many_visit, &view,
                                               &e) == LC_ERR_INVALID);
              REQUIRE(view.calls == 0);
              clear(&e);
            }
            ok(lc_pouch_state_read_many(p, "reads", NULL, 0, read_many_visit,
                                        &view, &e),
               &e);
            REQUIRE(view.calls == 0);
          }
          lc_pouch_state_scan_summaries_result_cleanup(&allocator, &page);
          if (!multifile)
            one_file(directory);
          lc_pouch_close(p);
          if (view.saved) {
            drain(view.saved, read_length(11), 21, &e);
            ok(view.saved->reset(view.saved, &e), &e);
            drain(view.saved, read_length(11), 21, &e);
            view.saved->close(view.saved);
          }
          REQUIRE(live == 0);
          if (!multifile)
            REQUIRE(unlink(store) == 0);
        }
  lc_error_cleanup(&e);
}
static int consume_read(lc_source *source, unsigned index, lc_error *e) {
  unsigned char bytes[997];
  size_t offset = 0, got;
  while ((got = source->read(source, bytes, sizeof(bytes), e)) != 0) {
    REQUIRE(got <= read_length(index) - offset);
    for (size_t i = 0; i < got; ++i)
      REQUIRE(bytes[i] == byte_at(offset + i, (unsigned char)(10 + index)));
    offset += got;
  }
  if (e->code != LC_OK)
    return e->code;
  REQUIRE(offset == read_length(index));
  return LC_OK;
}
static int fault_read_many(const char *key, const lc_pouch_state_read_result *r,
                           void *context, lc_error *e) {
  read_view *view = context;
  unsigned index = read_index(key);
  ++view->calls;
  REQUIRE(index < 4 && r->found && r->has_body && r->body && r->version == 1 &&
          r->metadata_length == 6 && !memcmp(r->metadata, "before", 6));
  return consume_read(r->body, index, e);
}
static int fault_read_summary(const lc_pouch_state_scan_summary_entry *entry,
                              void *context, lc_error *e) {
  read_view *view = context;
  unsigned index = read_index(entry->key);
  ++view->calls;
  REQUIRE(index < 4 && entry->version == 1);
  if (view->bodies)
    for (int repeat = 0; repeat < 2; ++repeat) {
      lc_pouch_state_read_result r = {0};
      int rc = lc_pouch_state_scan_summary_read_body(view->pouch, "reads",
                                                     entry, repeat, &r, e);
      if (rc == LC_OK) {
        REQUIRE(r.found && r.has_body && r.version == 1 &&
                r.metadata_length == 6 && !memcmp(r.metadata, "before", 6));
        rc = consume_read(r.body, index, e);
      }
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      if (rc != LC_OK)
        return rc;
      REQUIRE(strlen(entry->etag) == 64 &&
              !strcmp(entry->content_type, "application/test"));
    }
  return LC_OK;
}
static int fault_read_metadata(const lc_pouch_state_visit_entry *entry,
                               void *context, lc_error *e) {
  read_view *view = context;
  (void)e;
  REQUIRE(read_index(entry->key) < 4 && entry->has_payload &&
          entry->version == 1 && entry->metadata_length == 6 &&
          !memcmp(entry->metadata, "before", 6));
  ++view->calls;
  return LC_OK;
}
static int run_read(lc_pouch *p, int phase, size_t *calls, lc_error *e) {
  static const char *const keys[] = {"k01", "k00", "k02", "k03", "k01",
                                     "k02", "k00", "k01", "k03", "k02"};
  read_view view = {0};
  lc_pouch_state_scan_summaries_result page = {0};
  int rc;
  view.pouch = p;
  if (phase == 0)
    rc = lc_pouch_state_read_many(p, "reads", keys, 10, fault_read_many, &view,
                                  e);
  else if (phase == 1)
    rc = lc_pouch_state_read_many_cached(p, "reads", keys, 10, fault_read_many,
                                         &view, e);
  else if (phase == 4)
    rc = lc_pouch_state_visit_prefix(p, "reads", "k", fault_read_metadata,
                                     &view, e);
  else {
    view.bodies = phase == 3;
    rc = lc_pouch_state_scan_summaries(p, "reads", NULL, 0, fault_read_summary,
                                       &view, &page, e);
    lc_pouch_state_scan_summaries_result_cleanup(&allocator, &page);
  }
  *calls = view.calls;
  if (rc == LC_OK)
    REQUIRE(view.calls == (phase < 2 ? 10U : 4U));
  return rc;
}
static ssize_t read_fail(void *context, int fd, void *bytes, size_t n,
                         off_t at) {
  io_fault *f = context;
  if (io_fails(f))
    return -1;
  return pread(fd, bytes, n, at);
}
static size_t read_failure_case(const char *path, size_t nth, int phase,
                                int cold, int use_io, int transform) {
  ps_pouch_state_options o = options(transform, 0);
  io_fault f = {0};
  ps_io io = {&f, read_fail, io_write, io_sync, io_reserve, NULL};
  lc_pouch *p = NULL;
  lc_error e;
  size_t count, calls;
  int rc;
  lc_error_init(&e);
  o.io = &io;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  seed_reads(p, 4, &e);
  if (cold) {
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  }
  if (use_io) {
    f.calls = 0;
    f.fail = nth;
    f.armed = 1;
  } else {
    allocation = 0;
    fail_at = nth;
    __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
  }
  rc = run_read(p, phase, &calls, &e);
  count = use_io ? f.calls : allocation;
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  REQUIRE(rc == LC_OK || rc == (use_io ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
  if (nth == SIZE_MAX)
    ok(rc, &e);
  clear(&e);
  if (use_io && rc != LC_OK) {
    REQUIRE(p->storage.ops->check(p->storage.context, &e) == LC_ERR_TRANSPORT);
    clear(&e);
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  }
  ok(run_read(p, phase, &calls, &e), &e);
  for (unsigned i = 0; i < 4; ++i) {
    char key[16];
    snprintf(key, sizeof(key), "k%02u", i);
    read_value(p, "reads", key, read_length(i), (unsigned char)(10 + i), 1, &e);
  }
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void read_faults(const char *path, int use_io) {
  size_t total = 0;
  for (int transform = 0; transform <= 3; transform += 3)
    for (int cold = 0; cold < 2; ++cold)
      for (int phase = 0; phase < 5; ++phase) {
        size_t count =
            read_failure_case(path, SIZE_MAX, phase, cold, use_io, transform);
        for (size_t n = 1; n <= count; ++n)
          (void)read_failure_case(path, n, phase, cold, use_io, transform);
        total += count;
      }
  printf("container read %s failure sites: %lu\n",
         use_io ? "I/O" : "allocation", (unsigned long)total);
}
typedef struct read_job {
  gate held;
  read_view view;
  int phase, rc;
} read_job;
static void hold_read(read_job *job) {
  if (job->view.calls)
    return;
  pthread_mutex_lock(&job->held.mutex);
  job->held.entered = 1;
  pthread_cond_broadcast(&job->held.changed);
  while (!job->held.released)
    pthread_cond_wait(&job->held.changed, &job->held.mutex);
  pthread_mutex_unlock(&job->held.mutex);
}
static int held_read_many(const char *key, const lc_pouch_state_read_result *r,
                          void *context, lc_error *e) {
  read_job *job = context;
  hold_read(job);
  return fault_read_many(key, r, &job->view, e);
}
static int held_read_summary(const lc_pouch_state_scan_summary_entry *entry,
                             void *context, lc_error *e) {
  read_job *job = context;
  hold_read(job);
  return fault_read_summary(entry, &job->view, e);
}
static void *run_read_job(void *context) {
  read_job *job = context;
  static const char *const keys[] = {"k01", "k02", "k03", "k00"};
  lc_pouch_state_scan_summaries_result page = {0};
  lc_error e;
  lc_error_init(&e);
  if (job->phase == 0)
    job->rc = lc_pouch_state_read_many(job->view.pouch, "reads", keys, 4,
                                       held_read_many, job, &e);
  else if (job->phase == 1)
    job->rc = lc_pouch_state_read_many_cached(job->view.pouch, "reads", keys, 4,
                                              held_read_many, job, &e);
  else
    job->rc = lc_pouch_state_scan_summaries(job->view.pouch, "reads", NULL, 0,
                                            held_read_summary, job, &page, &e);
  lc_pouch_state_scan_summaries_result_cleanup(&allocator, &page);
  lc_error_cleanup(&e);
  return NULL;
}
static void read_concurrency(const char *path, const char *directory) {
  for (int transform = 0; transform <= 3; transform += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int phase = 0; phase < 4; ++phase) {
        ps_pouch_state_options o = options(transform, sync);
        lc_pouch *p = NULL;
        lc_pouch_state_write_result w = {0};
        lc_pouch_state_read_result r = {0};
        read_job job = {0};
        pthread_t thread;
        input in = source(97, 201);
        lc_error e;
        lc_error_init(&e);
        o.logical.segment_target_bytes = 1;
        ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
        seed_reads(p, 4, &e);
        REQUIRE(pthread_mutex_init(&job.held.mutex, NULL) == 0);
        REQUIRE(pthread_cond_init(&job.held.changed, NULL) == 0);
        job.phase = phase;
        job.view.pouch = p;
        job.view.bodies = phase == 2;
        REQUIRE(pthread_create(&thread, NULL, run_read_job, &job) == 0);
        pthread_mutex_lock(&job.held.mutex);
        while (!job.held.entered)
          pthread_cond_wait(&job.held.changed, &job.held.mutex);
        pthread_mutex_unlock(&job.held.mutex);
        ok(put(p, "reads", "k01", &in, NULL, &w, &e), &e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        ok(lc_pouch_state_delete(p, "reads", "k03", NULL, &w, &e), &e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        in = source(31, 99);
        ok(put(p, "peer", "key", &in, NULL, &w, &e), &e);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        pthread_mutex_lock(&job.held.mutex);
        job.held.released = 1;
        pthread_cond_broadcast(&job.held.changed);
        pthread_mutex_unlock(&job.held.mutex);
        REQUIRE(pthread_join(thread, NULL) == 0);
        ok(job.rc, &e);
        REQUIRE(job.view.calls == 4);
        read_value(p, "reads", "k01", 97, 201, 2, &e);
        read_value(p, "peer", "key", 31, 99, 1, &e);
        ok(lc_pouch_state_read_metadata(p, "reads", "k03", &r, &e), &e);
        REQUIRE(!r.found);
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        one_file(directory);
        lc_pouch_close(p);
        REQUIRE(live == 0 && unlink(path) == 0);
        REQUIRE(pthread_cond_destroy(&job.held.changed) == 0);
        REQUIRE(pthread_mutex_destroy(&job.held.mutex) == 0);
        lc_error_cleanup(&e);
      }
}
static void validation(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int bad = 0; bad < 7; ++bad) {
    ps_pouch_state_options o = options(0, 0);
    lc_pouch *p = NULL;
    int expected = LC_ERR_UNSUPPORTED;
    if (bad == 1)
      o.logical.background_compaction_enabled = 1;
    if (bad == 2)
      o.logical.query_indexing_enabled = 1;
    if (bad == 3) {
      o.logical.single_writer_set = 1;
      o.logical.single_writer = 0;
    }
    if (bad == 4)
      o.logical.queue_watch = 1;
    if (bad == 5)
      o.logical.crypto_generate_key_file = 1;
    if (bad == 6) {
      o.logical.compression = "unknown";
      expected = LC_ERR_INVALID;
    }
    REQUIRE(ps_pouch_state_open(path, 1, &allocator, bad ? &o : NULL, &p, &e) ==
            expected);
    REQUIRE(!p && live == 0 && access(path, F_OK) != 0);
    clear(&e);
  }
  lc_error_cleanup(&e);
}
static void snapshots(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync) {
      ps_pouch_state_options o = options(crypto, sync);
      lc_pouch *p = NULL;
      lc_pouch_state_write_result w = {0};
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      input in = source(65569, 37);
      ok(put(p, "snapshot", "key", &in, NULL, &w, &e), &e);
      REQUIRE(w.version == 1 && w.index_seq == 1);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      void *handle = NULL;
      ok(p->storage.ops->namespace_open(p->storage.context, "snapshot", 0,
                                        &handle, &e),
         &e);
      ps_namespace *ns = handle;
      ps_namespace_snapshot *b = NULL;
      ok(ps_namespace_snapshot_begin(ns, &b, &e), &e);
      lc_pouch_state_entry request = {0}, result = {0};
      request.key = "key";
      request.version = request.index_seq = 1;
      request.record_type = 1;
      request.payload_context =
          lc_pouch_state_crypto_context(&allocator, "snapshot", "key", 1, 1);
      REQUIRE(request.payload_context != NULL);
      request.content_type = "application/octet-stream";
      in = source(65569, 37);
      in.pub.impl = &in;
      ok(lc_pouch_state_record_append(
             &allocator, p->crypto, ps_namespace_snapshot_output(b),
             ps_namespace_snapshot_leaf(b), &request, &in.pub, 0, &result, &e),
         &e);
      lc_pouch_state_entry_cleanup(&allocator, &result);
      lc_free_with_allocator(&allocator, request.payload_context);
      ok(ps_namespace_snapshot_commit(b, &e), &e);
      ps_namespace_snapshot_close(b);
      p->storage.ops->namespace_close(p->storage.context, ns);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      read_value(p, "snapshot", "key", 65569, 37, 1, &e);
      ok(p->storage.ops->namespace_open(p->storage.context, "snapshot", 0,
                                        &handle, &e),
         &e);
      ns = handle;
      ok(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &e), &e);
      lc_pouch_state_write_options meta = {0};
      meta.has_metadata = 1;
      meta.metadata = (const unsigned char *)"snapshot metadata";
      meta.metadata_length = 17;
      ok(lc_pouch_state_update_metadata(p, "snapshot", "key", &meta, &w, &e),
         &e);
      REQUIRE(w.version == 1 && w.index_seq == 2);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      p->storage.ops->namespace_close(p->storage.context, ns);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      read_value(p, "snapshot", "key", 65569, 37, 1, &e);
      lc_pouch_state_read_result r = {0};
      ok(lc_pouch_state_read_metadata(p, "snapshot", "key", &r, &e), &e);
      REQUIRE(r.metadata_length == 17 &&
              !memcmp(r.metadata, "snapshot metadata", 17));
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      in = source(31, 71);
      ok(put(p, "snapshot", "key", &in, NULL, &w, &e), &e);
      REQUIRE(w.version == 2 && w.index_seq == 3);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      read_value(p, "snapshot", "key", 31, 71, 2, &e);
      ok(p->storage.ops->namespace_open(p->storage.context, "snapshot", 0,
                                        &handle, &e),
         &e);
      ps_namespace_status status;
      ok(ps_namespace_status_get(handle, &status, &e), &e);
      REQUIRE(status.snapshot && status.snapshot_boundary == 1 &&
              status.segments >= 1);
      p->storage.ops->namespace_close(p->storage.context, handle);
      one_file(directory);
      lc_pouch_close(p);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
typedef struct run {
  const char *path, *directory, *mode;
} run;
static void *worker(void *context) {
  run *r = context;
  if (!strcmp(r->mode, "lifecycle"))
    lifecycle(r->path, r->directory);
  else if (!strcmp(r->mode, "faults"))
    faults(r->path);
  else if (!strcmp(r->mode, "concurrency"))
    concurrency(r->path, r->directory);
  else if (!strcmp(r->mode, "io-faults"))
    io_faults(r->path);
  else if (!strcmp(r->mode, "validation"))
    validation(r->path);
  else if (!strcmp(r->mode, "mutations"))
    mutations(r->path, r->directory);
  else if (!strcmp(r->mode, "mutation-faults"))
    mutation_faults(r->path, 0);
  else if (!strcmp(r->mode, "mutation-io-faults"))
    mutation_faults(r->path, 1);
  else if (!strcmp(r->mode, "retention-hint"))
    retention_hint(r->path, r->directory);
  else if (!strcmp(r->mode, "mutation-concurrency"))
    mutation_concurrency(r->path, r->directory);
  else if (!strcmp(r->mode, "mutation-capacity"))
    mutation_capacity(r->path, r->directory);
  else if (!strcmp(r->mode, "key-bounds"))
    key_bounds(r->directory);
  else if (!strcmp(r->mode, "reads"))
    reads(r->path, r->directory);
  else if (!strcmp(r->mode, "read-faults"))
    read_faults(r->path, 0);
  else if (!strcmp(r->mode, "read-io-faults"))
    read_faults(r->path, 1);
  else if (!strcmp(r->mode, "read-concurrency"))
    read_concurrency(r->path, r->directory);
  else if (!strcmp(r->mode, "snapshots"))
    snapshots(r->path, r->directory);
  else
    REQUIRE(0);
  return NULL;
}
int main(int argc, char **argv) {
  char directory[1200], template_path[1200], path[1300], prefix[1100];
  pthread_t thread;
  lc_error e;
  run r;
  REQUIRE(argc == 3);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-runtime-", argv[1]) <
          (int)sizeof(prefix));
  REQUIRE(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) <
          (int)sizeof(template_path));
  REQUIRE(
      lc_test_tmp_mkdtemp(template_path, directory, sizeof(directory), prefix));
  REQUIRE(!strncmp(directory, prefix, strlen(prefix)));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  r.path = path;
  r.directory = directory;
  r.mode = argv[2];
  for (size_t i = 0; i < sizeof(mutation_metadata); ++i)
    mutation_metadata[i] = byte_at(i, 37);
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&crypto_key, &e), &e);
  REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, prefix);
  REQUIRE(access(directory, F_OK) != 0);
  REQUIRE(!bounds_failed);
  printf("container production state: %lu checks\n", (unsigned long)checks);
  return 0;
}
