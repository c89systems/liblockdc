#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
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
static int fails(void) { return armed && ++allocation == fail_at; }
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
static size_t live;
static void *allocate(void *context, size_t size) {
  header *h = malloc(sizeof(*h) + size);
  (void)context;
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
  armed = 1;
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
  armed = 0;
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
  (void)context;
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
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&crypto_key, &e), &e);
  REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, prefix);
  REQUIRE(access(directory, F_OK) != 0);
  printf("container production state: %lu checks\n", (unsigned long)checks);
  return 0;
}
