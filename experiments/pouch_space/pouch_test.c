#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "namespace.h"
#include "pouch.h"
#include "pouch_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/crypto.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocation, fail_at;
static int armed;
static char *crypto_key;
static int fail_crypto_init;
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
int __real_OPENSSL_init_crypto(uint64_t, const OPENSSL_INIT_SETTINGS *);
int __wrap_OPENSSL_init_crypto(uint64_t options,
                               const OPENSSL_INIT_SETTINGS *settings) {
  if (__atomic_load_n(&fail_crypto_init, __ATOMIC_ACQUIRE))
    return 0;
  return __real_OPENSSL_init_crypto(options, settings);
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
  if (!r.found || !r.has_body || r.version != version || r.bytes != length)
    fprintf(stderr,
            "read %s/%s: found=%d body=%d version=%lu bytes=%lu; "
            "expected version=%lu bytes=%lu\n",
            ns, key, r.found, r.has_body, (unsigned long)r.version,
            (unsigned long)r.bytes, (unsigned long)version,
            (unsigned long)length);
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

static uint64_t scope_syncs(lc_pouch *p, lc_error *e) {
  ps_stats stats;
  ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &stats, e), e);
  return stats.syncs;
}
static void scope_put(lc_pouch *p, const char *ns, unsigned char seed,
                      lc_error *e) {
  input in = source(17, seed);
  lc_pouch_state_write_result w = {0};
  ok(put(p, ns, "key", &in, NULL, &w, e), e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
}
static void crypto_initialization(const char *path) {
  ps_pouch_state_options o = options(0, 0);
  lc_pouch *p = NULL;
  lc_error e;
  lc_error_init(&e);
  for (int container = 0; container < 2; ++container) {
    int rc;
    __atomic_store_n(&fail_crypto_init, 1, __ATOMIC_RELEASE);
    if (container)
      rc = ps_pouch_state_open(path, 1, &allocator, &o, &p, &e);
    else
      rc = lc_pouch_open(path, &allocator, &o.logical, &p, &e);
    __atomic_store_n(&fail_crypto_init, 0, __ATOMIC_RELEASE);
    REQUIRE(rc == LC_ERR_TRANSPORT && p == NULL && live == 0);
    REQUIRE(access(path, F_OK) != 0 && errno == ENOENT);
    clear(&e);
  }
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  scope_put(p, "initialized", 1, &e);
  lc_pouch_close(p);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  read_value(p, "initialized", "key", 17, 1, 1, &e);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct scope_include {
  lc_pouch *pouch;
  const char *name;
} scope_include;
static int scope_include_locked(void *context, lc_error *e) {
  scope_include *include = context;
  return lc_pouch_state_durable_scope_include_namespace_locked(
      include->pouch, include->name, e);
}
typedef struct scope_peer {
  lc_pouch *pouch;
  int rc;
} scope_peer;
static void *scope_peer_write(void *context) {
  scope_peer *peer = context;
  input in = source(17, 9);
  lc_pouch_state_write_result w = {0};
  lc_error e;
  lc_error_init(&e);
  peer->rc = put(peer->pouch, "peer", "key", &in, NULL, &w, &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  lc_error_cleanup(&e);
  return NULL;
}
static void durable_scope(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int transform = 0; transform < 4; ++transform)
    for (int durable = 0; durable < 2; ++durable) {
      ps_pouch_state_options o = options(transform, durable);
      lc_pouch *p = NULL;
      lc_pouch_state_commit_group *scope = NULL, *nested = NULL;
      ps_namespace *held = NULL;
      ps_namespace_append *private = NULL;
      ps_namespace_status before, after;
      lc_pouch_state_write_options metadata = {0};
      lc_pouch_state_write_result w = {0};
      lc_pouch_state_read_result r = {0};
      lc_pouch_state_read_result pinned = {0};
      scope_peer peer;
      pthread_t thread;
      scope_include include;
      uint64_t syncs;
      struct iovec part = {(void *)"unfinished", 10};
      o.logical.segment_target_bytes = 8 * PS_MIB;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      scope_put(p, "one", 1, &e);
      scope_put(p, "two", 1, &e);
      scope_put(p, "three", 1, &e);
      scope_put(p, "peer", 1, &e);
      scope_put(p, "held", 1, &e);
      ok(ps_root_namespace_acquire(ps_pouch_state_root(p), "held", 0, &held,
                                   &e),
         &e);
      ok(ps_namespace_status_get(held, &before, &e), &e);
      ok(ps_namespace_append_begin(held, 0, 10, PS_APPEND_PREFIX, &private, &e),
         &e);
      REQUIRE(ps_namespace_append_commit(private, (enum ps_commit_mode)2, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      ok(lc_pouch_record_io_append(ps_namespace_append_output(private), &part,
                                   1, &e),
         &e);
      ok(ps_flush(ps_root_space(ps_pouch_state_root(p)), &e), &e);
      syncs = scope_syncs(p, &e);
      scope_put(p, "one", 2, &e);
      REQUIRE(scope_syncs(p, &e) == syncs + durable);
      /* Include a completed mutation from before this scope. The caller's
       * namespace authority is mandatory, but no pathname is opened. */
      REQUIRE(lc_pouch_state_durable_scope_include_namespace_locked(
                  p, "one", &e) == LC_ERR_INVALID);
      clear(&e);
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      REQUIRE(lc_pouch_state_durable_scope_begin(p, &nested, &e) ==
                  LC_ERR_INVALID &&
              nested == NULL);
      clear(&e);
      include = (scope_include){p, "one"};
      ok(lc_pouch_state_with_namespace_lock(p, "one", scope_include_locked,
                                            &include, &e),
         &e);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_barrier(scope, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs + !durable);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_barrier(scope, &e), &e);
      ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs);
      /* One forced barrier covers complete PUT, META and DELETE records in
       * different namespaces, while another producer remains unfinished. */
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      scope_put(p, "one", 3, &e);
      scope_put(p, "two", 4, &e);
      read_value(p, "one", "key", 17, 3, 3, &e);
      read_value(p, "two", "key", 17, 4, 2, &e);
      metadata.has_metadata = 1;
      metadata.metadata = (const unsigned char *)"scope";
      metadata.metadata_length = 5;
      ok(lc_pouch_state_update_metadata(p, "one", "key", &metadata, &w, &e),
         &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      ok(lc_pouch_state_delete(p, "two", "key", NULL, &w, &e), &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      REQUIRE(scope_syncs(p, &e) == syncs);
      ok(lc_pouch_state_read_metadata(p, "two", "key", &r, &e), &e);
      REQUIRE(!r.found);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      syncs = scope_syncs(p, &e);
      allocation = 0;
      fail_at = 1;
      __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
      int rc = lc_pouch_state_durable_scope_barrier(scope, &e);
      __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
      ok(rc, &e);
      REQUIRE(allocation == 0 && scope_syncs(p, &e) == syncs + 1);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs);
      /* End supplies the barrier even when the caller already has an error.
       * That error takes precedence over a later cleanup failure. */
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      scope_put(p, "three", 5, &e);
      syncs = scope_syncs(p, &e);
      lc_error_set(&e, LC_ERR_INVALID, 0, "original operation error", NULL,
                   NULL, "test");
      REQUIRE(lc_pouch_state_durable_scope_end(scope, LC_ERR_INVALID, &e) ==
                  LC_ERR_INVALID &&
              e.code == LC_ERR_INVALID &&
              !strcmp(e.message, "original operation error"));
      REQUIRE(scope_syncs(p, NULL) == syncs + 1);
      clear(&e);
      allocation = 0;
      fail_at = 1;
      __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
      rc = lc_pouch_state_durable_scope_begin(p, &scope, &e);
      __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
      REQUIRE(rc == LC_ERR_NOMEM && scope == NULL);
      clear(&e);
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      scope_put(p, "one", 6, &e);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs + 1);
      /* The active scope belongs to this thread. A normal peer must finish
       * its durable write while this scope stays open. Its file barrier may
       * cover our completed prefix too; the captured old body stays pinned. */
      ok(lc_pouch_state_read(p, "one", "key", &pinned, &e), &e);
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      syncs = scope_syncs(p, &e);
      scope_put(p, "one", 7, &e);
      REQUIRE(scope_syncs(p, &e) == syncs);
      peer = (scope_peer){p, 0};
      REQUIRE(pthread_create(&thread, NULL, scope_peer_write, &peer) == 0);
      REQUIRE(pthread_join(thread, NULL) == 0 && peer.rc == LC_OK);
      REQUIRE(scope_syncs(p, &e) == syncs + durable);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_barrier(scope, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs + !durable);
      syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
      REQUIRE(scope_syncs(p, &e) == syncs);
      drain(pinned.body, 17, 6, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &pinned);
      /* A later producer failure does not roll back prior complete appends.
       * Scope end must stabilize those appends and retain the source error. */
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      scope_put(p, "one", 8, &e);
      input broken = source(65569, 10);
      broken.fail_after = 4096;
      REQUIRE(put(p, "three", "key", &broken, NULL, &w, &e) ==
              LC_ERR_TRANSPORT);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      REQUIRE(p->storage.ops->check(p->storage.context, NULL) == LC_OK);
      syncs = scope_syncs(p, NULL);
      REQUIRE(lc_pouch_state_durable_scope_end(scope, LC_ERR_TRANSPORT, &e) ==
                  LC_ERR_TRANSPORT &&
              e.code == LC_ERR_TRANSPORT &&
              !strcmp(e.message, "injected producer failure"));
      REQUIRE(scope_syncs(p, NULL) == syncs + 1);
      clear(&e);
      read_value(p, "three", "key", 17, 5, 2, &e);
      ok(ps_namespace_status_get(held, &after, &e), &e);
      REQUIRE(after.tail == before.tail &&
              after.active_segment == before.active_segment);
      read_value(p, "held", "key", 17, 1, 1, &e);
      ps_namespace_append_close(private);
      ps_namespace_release(held);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      read_value(p, "one", "key", 17, 8, 6, &e);
      read_value(p, "peer", "key", 17, 9, 2, &e);
      read_value(p, "three", "key", 17, 5, 2, &e);
      ok(lc_pouch_state_read_metadata(p, "two", "key", &r, &e), &e);
      REQUIRE(!r.found);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      read_value(p, "held", "key", 17, 1, 1, &e);
      ok(lc_pouch_state_read_metadata(p, "one", "key", &r, &e), &e);
      REQUIRE(r.metadata_length == 5 && !memcmp(r.metadata, "scope", 5));
      lc_pouch_state_read_result_cleanup(&allocator, &r);
      one_file(directory);
      lc_pouch_close(p);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
static void durable_scope_faults(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int variant = 0; variant < 24; ++variant) {
    int phase = variant % 3;
    io_fault f = {0};
    ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
    ps_pouch_state_options o = options(variant / 6, (variant / 3) % 2);
    lc_pouch *p = NULL;
    lc_pouch_state_commit_group *scope = NULL;
    lc_pouch_state_read_result r = {0};
    o.logical.segment_target_bytes = 8 * PS_MIB;
    o.io = &io;
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    scope_put(p, "one", 1, &e);
    ok(ps_flush(ps_root_space(ps_pouch_state_root(p)), &e), &e);
    ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
    scope_put(p, "one", 2, &e);
    f.armed = 1;
    f.fail = 1;
    if (phase == 0) {
      REQUIRE(lc_pouch_state_durable_scope_barrier(scope, &e) ==
              LC_ERR_TRANSPORT);
      REQUIRE(lc_pouch_state_durable_scope_end(scope, LC_ERR_TRANSPORT, &e) ==
              LC_ERR_TRANSPORT);
    } else if (phase == 1)
      REQUIRE(lc_pouch_state_durable_scope_end(scope, LC_OK, &e) ==
              LC_ERR_TRANSPORT);
    else {
      lc_error_set(&e, LC_ERR_PENDING, 0, "original pending operation", NULL,
                   NULL, "test");
      REQUIRE(lc_pouch_state_durable_scope_end(scope, LC_ERR_PENDING, &e) ==
                  LC_ERR_PENDING &&
              e.code == LC_ERR_PENDING &&
              !strcmp(e.message, "original pending operation"));
    }
    REQUIRE(f.calls == 1);
    f.armed = 0;
    clear(&e);
    REQUIRE(p->storage.ops->check(p->storage.context, &e) != LC_OK);
    clear(&e);
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    ok(lc_pouch_state_read(p, "one", "key", &r, &e), &e);
    REQUIRE(r.found && (r.version == 1 || r.version == 2));
    drain(r.body, 17, r.version == 1 ? 1 : 2, &e);
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    /* Both an error and an uncertain flush must have released the TLS scope. */
    ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
    ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
typedef struct scope_image {
  io_fault fault;
  int fd;
  unsigned char *scratch;
} scope_image;
/* The test image changes only at successful root sync calls. Reopening it
 * models loss of unsynced writes instead of relying on surviving page cache. */
static int scope_image_sync(void *context, int fd) {
  scope_image *image = context;
  struct stat st;
  off_t at = 0;
  if (fdatasync(fd) != 0 || fstat(fd, &st) != 0 ||
      ftruncate(image->fd, st.st_size) != 0)
    return -1;
  while (at < st.st_size) {
    size_t want = (uint64_t)(st.st_size - at) < PS_PAGE
                      ? (size_t)(st.st_size - at)
                      : PS_PAGE;
    ssize_t got, written;
    size_t done = 0;
    do {
      got = pread(fd, image->scratch, want, at);
    } while (got < 0 && errno == EINTR);
    if (got <= 0) {
      if (!got)
        errno = EIO;
      return -1;
    }
    while (done < (size_t)got) {
      do {
        written = pwrite(image->fd, image->scratch + done, (size_t)got - done,
                         at + (off_t)done);
      } while (written < 0 && errno == EINTR);
      if (written <= 0) {
        if (!written)
          errno = EIO;
        return -1;
      }
      done += (size_t)written;
    }
    at += got;
  }
  return fdatasync(image->fd);
}
static void durable_scope_crash(const char *path) {
  char image_path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(image_path, sizeof(image_path), "%s-image", path) <
          (int)sizeof(image_path));
  for (int variant = 0; variant < 24; ++variant) {
    int phase = variant % 3, status;
    scope_image image = {0};
    ps_io io = {&image, io_read, io_write, scope_image_sync, io_reserve, NULL};
    ps_pouch_state_options o = options(variant / 6, (variant / 3) % 2);
    lc_pouch *p = NULL;
    lc_pouch_state_read_result r = {0};
    pid_t child;
    image.scratch = lc_alloc_with_allocator(&allocator, PS_PAGE);
    REQUIRE(image.scratch != NULL);
    image.fd = open(image_path, O_RDWR | O_CREAT | O_EXCL, 0600);
    REQUIRE(image.fd >= 0);
    o.initial_capacity = o.maximum_capacity = PS_MIB;
    o.logical.segment_target_bytes = 8 * PS_MIB;
    o.io = &io;
    /* This fixture forks on the main thread before starting any test worker.
     * The child opens its own storage handles and dies without closing them. */
    child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      lc_pouch_state_commit_group *scope = NULL;
      lc_pouch_state_write_options metadata = {0};
      lc_pouch_state_write_result w = {0};
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      scope_put(p, "one", 1, &e);
      scope_put(p, "two", 1, &e);
      ok(ps_flush(ps_root_space(ps_pouch_state_root(p)), &e), &e);
      uint64_t syncs = scope_syncs(p, &e);
      ok(lc_pouch_state_durable_scope_begin(p, &scope, &e), &e);
      scope_put(p, "one", 2, &e);
      metadata.has_metadata = 1;
      metadata.metadata = (const unsigned char *)"after";
      metadata.metadata_length = 5;
      ok(lc_pouch_state_update_metadata(p, "one", "key", &metadata, &w, &e),
         &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      ok(lc_pouch_state_delete(p, "two", "key", NULL, &w, &e), &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      REQUIRE(scope_syncs(p, &e) == syncs);
      if (phase == 1)
        ok(lc_pouch_state_durable_scope_barrier(scope, &e), &e);
      else if (phase == 2)
        ok(lc_pouch_state_durable_scope_end(scope, LC_OK, &e), &e);
      _exit(0);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0);
    REQUIRE(close(image.fd) == 0);
    lc_free_with_allocator(&allocator, image.scratch);
    REQUIRE(live == 0);
    o.io = NULL;
    ok(ps_pouch_state_open(image_path, 0, &allocator, &o, &p, &e), &e);
    read_value(p, "one", "key", 17, phase ? 2 : 1, phase ? 2 : 1, &e);
    ok(lc_pouch_state_read_metadata(p, "one", "key", &r, &e), &e);
    REQUIRE(r.metadata_length == (phase ? 5 : 0));
    if (phase)
      REQUIRE(!memcmp(r.metadata, "after", 5) && r.index_seq == 3);
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    if (!phase)
      read_value(p, "two", "key", 17, 1, 1, &e);
    else {
      ok(lc_pouch_state_read_metadata(p, "two", "key", &r, &e), &e);
      REQUIRE(!r.found);
      lc_pouch_state_read_result_cleanup(&allocator, &r);
    }
    scope_put(p, "continued", 3, &e);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(image_path) == 0 && unlink(path) == 0);
  }
  lc_error_cleanup(&e);
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
  int rc, started, staged;
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
  if (j->staged) {
    in.pub.impl = &in;
    j->rc =
        lc_pouch_state_stage_write(j->pouch, "held", "key", "attempt",
                                   j->held ? &body : &in.pub, &options, &w, &e);
    j->calls = in.calls;
  } else if (j->held)
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
static void concurrency(const char *path, const char *directory, int staged) {
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
    if (staged) {
      in = source(42, 10);
      in.pub.impl = &in;
      ok(lc_pouch_state_stage_write(p, "held", "key", "attempt", &in.pub, NULL,
                                    &w, &e),
         &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
    }
    j.pouch = p;
    j.staged = staged;
    j.held = &g;
    j.rc = -1;
    REQUIRE(pthread_create(&producer, NULL, held_writer, &j) == 0);
    pthread_mutex_lock(&g.mutex);
    while (!g.entered)
      pthread_cond_wait(&g.changed, &g.mutex);
    pthread_mutex_unlock(&g.mutex);
    contender.pouch = p;
    contender.staged = staged;
    REQUIRE(pthread_create(&competing, NULL, held_writer, &contender) == 0);
    while (!__atomic_load_n(&contender.started, __ATOMIC_ACQUIRE))
      sched_yield();
    /* Same-namespace reads and another namespace's write finish before the
     * held producer can return. No timeout or scheduling sleep proves this. */
    read_value(p, "held", "anchor", 1009, 31, 1, &e);
    in = source(65569, 12);
    if (staged) {
      in.pub.impl = &in;
      ok(lc_pouch_state_stage_write(p, "independent", "key", "attempt", &in.pub,
                                    NULL, &w, &e),
         &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
      ok(lc_pouch_state_commit_staged(p, "independent", "key", "attempt", &w,
                                      &e),
         &e);
    } else
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
    if (staged) {
      ok(lc_pouch_state_commit_staged(p, "held", "key", "attempt", &w, &e), &e);
      lc_pouch_state_write_result_cleanup(&allocator, &w);
    }
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
  ok(ps_begin(space, NULL, key, &writer, e), e);
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
  int rc;
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
  /* These small-key tombstones fit the standing bounded-work reserve even
   * when rotation is required. This does not cover the maximum key size. */
  ok(lc_pouch_state_delete(p, "full", "key-0", NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  full_after = storage_info(p, "full", &e);
  REQUIRE(full_after.active_segment == full_before.active_segment + 1 &&
          full_after.tail);
  ok(lc_pouch_state_read_metadata(p, "full", "key-0", &r, &e), &e);
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_delete(p, "victim", "key", NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  peer_after = storage_info(p, "victim", &e);
  REQUIRE(peer_after.active_segment == peer_before.active_segment + 1 &&
          peer_after.tail);
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
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_read_metadata(p, "victim", "key", &r, &e), &e);
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  one_file(directory);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  printf("capacity probe: %lu live keys; bounded-key deletion and peer "
         "deletion committed and recovered\n",
         (unsigned long)count);
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
        /* Deleting a metadata-only placeholder is a no-op even at the key
         * limit. It must survive maintenance, while the shorter body's
         * tombstone must continue to hide that distinct key. */
        ok(lc_pouch_state_delete(p, "bounds", key, NULL, &w, &e), &e);
        REQUIRE(w.version == 0 && w.index_seq == 4);
        lc_pouch_state_write_result_cleanup(&allocator, &w);
        for (int pass = 0; pass < 2; ++pass) {
          if (container) {
            ps_pouch_compaction_result result;
            ok(ps_pouch_state_compact(p, "bounds", &result, &e), &e);
            REQUIRE(!result.skipped && result.snapshot &&
                    result.metadata_bytes > PS_MIB && !result.body_bytes &&
                    !result.copied_bytes && !result.pending_sources);
          }
          lc_pouch_close(p);
          REQUIRE(live == 1);
          if (container)
            ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
          else
            ok(lc_pouch_open(path, &allocator, &o.logical, &p, &e), &e);
          ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
          REQUIRE(r.found && !r.has_body && r.version == 0 &&
                  r.index_seq == 1 && r.metadata_length == 5 &&
                  !memcmp(r.metadata, "bound", 5));
          lc_pouch_state_read_result_cleanup(&allocator, &r);
          key[body_key_bytes] = 0;
          ok(lc_pouch_state_read_metadata(p, "bounds", key, &r, &e), &e);
          REQUIRE(!r.found);
          lc_pouch_state_read_result_cleanup(&allocator, &r);
          key[body_key_bytes] = 'k';
        }
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
  if (phase == 5 || phase == 6) {
    lc_pouch_generation sequence = 99;
    *calls = 0;
    rc = phase == 5 ? lc_pouch_state_index_seq(p, "reads", &sequence, e)
                    : lc_pouch_state_query_index_seq(p, "reads", &sequence, e);
    REQUIRE(sequence == (rc == LC_OK ? 4 : 0));
    return rc;
  }
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
      for (int phase = 0; phase < 7; ++phase) {
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
typedef struct snapshot_copy {
  lc_pouch *pouch;
  ps_namespace *ns;
  ps_namespace_snapshot *builder;
  unsigned char *buffer;
  size_t count;
} snapshot_copy;
static int replay_snapshot(void *context, lc_pouch_storage_visit_fn visit,
                           void *visitor, lc_error *e) {
  return ps_namespace_view_visit(context, 0, visit, visitor, e);
}
static int measure_copy_record(void *context,
                               const lc_pouch_state_entry *record,
                               lc_error *e) {
  uint64_t bytes = 0, *total = context;
  int rc = lc_pouch_state_record_measure_snapshot(
      &allocator, "snapshot-00000000000000000001.log", record,
      !record->control && record->found && record->payload_span.present, &bytes,
      e);
  if (rc == LC_OK) {
    REQUIRE(bytes <= (uint64_t)INT64_MAX - *total);
    *total += bytes;
  }
  return rc;
}
static int plan_copy_snapshot(void *context, ps_namespace_view *view,
                              uint64_t boundary,
                              ps_namespace_snapshot_plan *plan, lc_error *e) {
  lc_pouch_state_snapshot *capture = NULL;
  uint64_t bytes = 0;
  (void)boundary;
  int rc = lc_pouch_state_snapshot_capture(context, replay_snapshot, view,
                                           &capture, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_snapshot_visit(capture, measure_copy_record, &bytes, e);
  lc_pouch_state_snapshot_close(capture);
  if (rc == LC_OK)
    plan->metadata_bytes = bytes;
  return rc;
}
static int copy_snapshot_record(void *context,
                                const lc_pouch_state_entry *record,
                                lc_error *e) {
  snapshot_copy *copy = context;
  lc_source *encoded = NULL;
  if (record->control)
    return lc_pouch_state_record_write_high_water(
        &allocator, ps_namespace_snapshot_output(copy->builder),
        record->index_seq, record->query_index_seq, e);
  int rc = LC_OK;
  if (record->found && record->payload_span.present)
    rc = copy->pouch->storage.ops->source_open(
        copy->pouch->storage.context, copy->ns,
        record->payload_span.container_leaf,
        record->payload_span.payload_offset,
        record->payload_span.payload_length, record->payload_span.payload_crc,
        &encoded, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_write_snapshot(
        &allocator, ps_namespace_snapshot_output(copy->builder),
        ps_namespace_snapshot_leaf(copy->builder), record, encoded,
        copy->buffer, 16384, e);
  if (encoded)
    encoded->close(encoded);
  ++copy->count;
  return rc;
}
static int projection_object(const lc_pouch_state_visit_entry *entry,
                             void *context, lc_error *e) {
  unsigned int *seen = context;
  (void)e;
  if (!strcmp(entry->key, "object")) {
    REQUIRE(entry->object_record && entry->has_payload);
    *seen |= 1U;
  } else {
    REQUIRE(!strcmp(entry->key, "state") && !entry->object_record);
    *seen |= 2U;
  }
  return LC_OK;
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
      ok(ps_namespace_snapshot_begin(ns, plan_copy_snapshot, p, &b, &e), &e);
      snapshot_copy copy = {p, ns, b,
                            lc_alloc_with_allocator(&allocator, 16384), 0};
      REQUIRE(copy.buffer != NULL);
      lc_pouch_state_snapshot *projection = NULL;
      ok(lc_pouch_state_snapshot_capture(p, replay_snapshot,
                                         ps_namespace_snapshot_view(b),
                                         &projection, &e),
         &e);
      ok(lc_pouch_state_snapshot_visit(projection, copy_snapshot_record, &copy,
                                       &e),
         &e);
      REQUIRE(copy.count == 1);
      lc_pouch_state_snapshot_close(projection);
      lc_free_with_allocator(&allocator, copy.buffer);
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
  /* The same reference writer serves native clean-close projections. Internal
   * objects must retain their classification after that projection is loaded.
   */
  for (int sync = 0; sync < 2; ++sync) {
    char native[1400];
    REQUIRE(snprintf(native, sizeof(native), "%s/native-projection-%d",
                     directory, sync) > 0);
    ps_pouch_state_options o = options(0, sync);
    lc_pouch *p = NULL;
    lc_pouch_state_write_options object = {0};
    object.object_record = 1;
    lc_pouch_state_write_result w = {0};
    ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
    input in = source(65569, 55);
    ok(put(p, "projection", "object", &in, &object, &w, &e), &e);
    REQUIRE(w.index_seq == 1);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    in = source(31, 56);
    ok(put(p, "projection", "state", &in, NULL, &w, &e), &e);
    REQUIRE(w.index_seq == 2);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    unsigned int seen = 0;
    ok(lc_pouch_state_visit(p, "projection", projection_object, &seen, &e), &e);
    REQUIRE(seen == 3);
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
    seen = 0;
    ok(lc_pouch_state_visit(p, "projection", projection_object, &seen, &e), &e);
    REQUIRE(seen == 3);
    read_value(p, "projection", "object", 65569, 55, 1, &e);
    in = source(31, 57);
    ok(put(p, "projection", "state", &in, NULL, &w, &e), &e);
    REQUIRE(w.index_seq == 3 && w.version == 2);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    lc_pouch_close(p);
    REQUIRE(live == 0);
  }
  lc_error_cleanup(&e);
}
typedef struct projected_check {
  size_t count;
  int newer;
  char protected_leaf[64];
} projected_check;
static int check_projected(void *context, const lc_pouch_state_entry *r,
                           lc_error *e) {
  projected_check *c = context;
  (void)e;
  size_t ordinal = c->count++;
  REQUIRE(r->seen && r->has_query_index_seq);
  if (!ordinal) {
    REQUIRE(r->control && r->record_type == 6);
    REQUIRE(r->version == (c->newer ? 14 : 11) && r->index_seq == r->version &&
            r->query_index_seq == (c->newer ? 13 : 10));
    return LC_OK;
  }
  REQUIRE(!r->control);
  if (ordinal == 1) {
    REQUIRE(!strcmp(r->key, "a-state") && r->found && r->payload_span.present);
    if (r->version != (c->newer ? 3 : 2) ||
        r->index_seq != (c->newer ? 12 : 6) ||
        r->query_index_seq != (c->newer ? 12 : 5))
      fprintf(
          stderr,
          "projection a-state: version=%llu index=%llu query=%llu type=%u\n",
          (unsigned long long)r->version, (unsigned long long)r->index_seq,
          (unsigned long long)r->query_index_seq, r->record_type);
    REQUIRE(r->version == (c->newer ? 3 : 2) &&
            r->index_seq == (c->newer ? 12 : 6) &&
            r->query_index_seq == (c->newer ? 12 : 5));
  } else if (ordinal == 2) {
    REQUIRE(!strcmp(r->key, "l-placeholder") && r->found &&
            !r->payload_span.present && !r->version && r->index_seq == 11 &&
            !r->query_index_seq);
    REQUIRE(r->has_query_hidden && r->query_hidden && r->metadata_length == 5 &&
            !memcmp(r->metadata, "lease", 5));
  } else if (ordinal == 3) {
    REQUIRE(!strcmp(r->key, "m-delete") && !r->found &&
            !r->payload_span.present && r->version == 2 && r->index_seq == 8 &&
            r->query_index_seq == 8);
  } else if (ordinal == 4) {
    REQUIRE(!strcmp(r->key, "n-state") && r->found && r->payload_span.present &&
            !r->bytes && r->version == 1 && r->index_seq == 10 &&
            r->query_index_seq == 10);
  } else if (c->newer && ordinal == 5) {
    REQUIRE(!strcmp(r->key, "tail") && r->found && r->version == 1 &&
            r->index_seq == 13 && r->query_index_seq == 13);
  } else {
    REQUIRE(ordinal == (c->newer ? 6 : 5));
    REQUIRE(!strcmp(r->key, "z-object") && r->found && r->record_type == 7 &&
            r->version == 2 && r->index_seq == (c->newer ? 14 : 9) &&
            !r->query_index_seq && r->bytes == 1048601);
    REQUIRE(strlen(r->payload_span.container_leaf) < sizeof(c->protected_leaf));
    strcpy(c->protected_leaf, r->payload_span.container_leaf);
    if (c->newer)
      REQUIRE(r->metadata_length == 11 &&
              !memcmp(r->metadata, "tail-object", 11));
  }
  return LC_OK;
}
static void projection_put(lc_pouch *p, const char *key, size_t bytes,
                           unsigned char seed, int object,
                           lc_pouch_generation sequence, lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result w = {0};
  input in = source(bytes, seed);
  o.object_record = object;
  ok(put(p, "projection", key, &in, &o, &w, e), e);
  REQUIRE(w.index_seq == sequence);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
}
static void projection_meta(lc_pouch *p, const char *key, const char *value,
                            int hidden, lc_pouch_generation sequence,
                            lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result w = {0};
  o.has_metadata = 1;
  o.metadata = (const unsigned char *)value;
  o.metadata_length = strlen(value);
  o.has_query_hidden = hidden;
  o.query_hidden = hidden;
  ok(lc_pouch_state_update_metadata(p, "projection", key, &o, &w, e), e);
  REQUIRE(w.index_seq == sequence);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
}
static void projection_fixture(lc_pouch *p, lc_error *e) {
  projection_put(p, "a-state", 1048601, 30, 0, 1, e);
  projection_put(p, "z-object", 65569, 32, 1, 2, e);
  projection_put(p, "m-delete", 31, 33, 0, 3, e);
  projection_meta(p, "l-placeholder", "lease", 1, 4, e);
  projection_put(p, "a-state", 65569, 34, 0, 5, e);
  projection_meta(p, "a-state", "state-meta", 0, 6, e);
  projection_meta(p, "z-object", "object-meta", 0, 7, e);
  lc_pouch_state_write_result w = {0};
  ok(lc_pouch_state_delete(p, "projection", "m-delete", NULL, &w, e), e);
  REQUIRE(w.index_seq == 8 && w.version == 2);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  projection_put(p, "z-object", 1048601, 35, 1, 9, e);
  projection_put(p, "n-state", 0, 36, 0, 10, e);
  projection_meta(p, "l-placeholder", "lease", 1, 11, e);
}
static void projection_readback(lc_pouch *p, lc_error *e) {
  read_value(p, "projection", "a-state", 31, 99, 3, e);
  read_value(p, "projection", "z-object", 1048601, 35, 2, e);
  read_value(p, "projection", "n-state", 0, 36, 1, e);
  read_value(p, "projection", "tail", 31, 77, 1, e);
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "projection", "m-delete", &r, e), e);
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_read_metadata(p, "projection", "l-placeholder", &r, e), e);
  REQUIRE(r.found && !r.has_body && !r.version && r.query_hidden &&
          r.metadata_length == 5 && !memcmp(r.metadata, "lease", 5));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_read_metadata(p, "projection", "z-object", &r, e), e);
  REQUIRE(r.metadata_length == 11 && !memcmp(r.metadata, "tail-object", 11));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  lc_pouch_generation seq;
  ok(lc_pouch_state_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 14);
  ok(lc_pouch_state_query_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 13);
}
static void projection(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync) {
      io_fault f = {0};
      ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
      ps_pouch_state_options o = options(crypto, sync);
      o.io = &io;
      lc_pouch *p = NULL;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      projection_fixture(p, &e);
      lc_pouch_state_read_result pinned = {0};
      ok(lc_pouch_state_read(p, "projection", "a-state", &pinned, &e), &e);
      REQUIRE(pinned.version == 2);
      void *handle = NULL;
      ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0,
                                        &handle, &e),
         &e);
      ps_namespace *ns = handle;
      ps_namespace_snapshot *b = NULL;
      ok(ps_namespace_snapshot_begin(ns, plan_copy_snapshot, p, &b, &e), &e);
      uint64_t boundary = ps_namespace_snapshot_boundary(b);
      lc_pouch_state_snapshot *captured = NULL;
      size_t before = f.read_bytes;
      measured_max = 0;
      __atomic_store_n(&measure, 1, __ATOMIC_RELEASE);
      int rc = lc_pouch_state_snapshot_capture(
          p, replay_snapshot, ps_namespace_snapshot_view(b), &captured, &e);
      __atomic_store_n(&measure, 0, __ATOMIC_RELEASE);
      ok(rc, &e);
      REQUIRE(measured_max < 65536);
      if (!crypto)
        REQUIRE(f.read_bytes - before < PS_MIB / 2);
      projected_check c = {0};
      ok(lc_pouch_state_snapshot_visit(captured, check_projected, &c, &e), &e);
      REQUIRE(c.count == 6);
      /* These later records are outside the capture. The newer object META
       * still refers to an original covered body, which must stay bound. */
      projection_put(p, "a-state", 31, 99, 0, 12, &e);
      projection_put(p, "tail", 31, 77, 0, 13, &e);
      projection_meta(p, "z-object", "tail-object", 0, 14, &e);
      projected_check unchanged = {0};
      ok(lc_pouch_state_snapshot_visit(captured, check_projected, &unchanged,
                                       &e),
         &e);
      REQUIRE(unchanged.count == 6);
      snapshot_copy copy = {p, ns, b,
                            lc_alloc_with_allocator(&allocator, 16384), 0};
      REQUIRE(copy.buffer != NULL);
      ok(lc_pouch_state_snapshot_visit(captured, copy_snapshot_record, &copy,
                                       &e),
         &e);
      REQUIRE(copy.count == 5);
      lc_free_with_allocator(&allocator, copy.buffer);
      ok(ps_namespace_snapshot_commit(b, &e), &e);
      ps_namespace_status first;
      ok(ps_namespace_status_get(ns, &first, &e), &e);
      ps_namespace_snapshot_close(b);
      unsigned int keep_kind;
      uint64_t keep_id;
      REQUIRE(ps_namespace_parse_source_leaf(c.protected_leaf, &keep_kind,
                                             &keep_id));
      for (uint64_t id = 1; id <= boundary; ++id)
        if (keep_kind != PS_CATALOG_SEGMENT || id != keep_id)
          ok(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, id, &e), &e);
      drain(pinned.body, 65569, 34, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &pinned);
      uint64_t closed, pending;
      ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
      REQUIRE(!pending);
      p->storage.ops->namespace_close(p->storage.context, ns);
      lc_pouch_close(p);
      /* The immutable metadata capture owns its fields independently. */
      unchanged.count = 0;
      ok(lc_pouch_state_snapshot_visit(captured, check_projected, &unchanged,
                                       &e),
         &e);
      REQUIRE(unchanged.count == 6);
      lc_pouch_state_snapshot_close(captured);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      projection_readback(p, &e);
      ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0,
                                        &handle, &e),
         &e);
      ns = handle;
      ok(ps_namespace_snapshot_begin(ns, plan_copy_snapshot, p, &b, &e), &e);
      boundary = ps_namespace_snapshot_boundary(b);
      ok(lc_pouch_state_snapshot_capture(
             p, replay_snapshot, ps_namespace_snapshot_view(b), &captured, &e),
         &e);
      c.count = 0;
      c.newer = 1;
      ok(lc_pouch_state_snapshot_visit(captured, check_projected, &c, &e), &e);
      REQUIRE(c.count == 7);
      copy = (snapshot_copy){p, ns, b,
                             lc_alloc_with_allocator(&allocator, 16384), 0};
      REQUIRE(copy.buffer != NULL);
      ok(lc_pouch_state_snapshot_visit(captured, copy_snapshot_record, &copy,
                                       &e),
         &e);
      REQUIRE(copy.count == 6);
      lc_free_with_allocator(&allocator, copy.buffer);
      lc_pouch_state_snapshot_close(captured);
      ok(ps_namespace_snapshot_commit(b, &e), &e);
      ps_namespace_snapshot_close(b);
      for (uint64_t id = 1; id <= boundary; ++id)
        ok(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, id, &e), &e);
      ok(ps_namespace_reclaim(ns, PS_CATALOG_SNAPSHOT, first.snapshot, &e), &e);
      p->storage.ops->namespace_close(p->storage.context, ns);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      projection_readback(p, &e);
      one_file(directory);
      lc_pouch_close(p);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
typedef struct capture_fixture {
  lc_pouch_state_entry *entries;
  char (*keys)[16];
  unsigned char (*metadata)[3];
  size_t count, fail_before;
} capture_fixture;
static void capture_fixture_open(capture_fixture *f) {
  memset(f, 0, sizeof(*f));
  f->entries = lc_calloc_with_allocator(&allocator, 85, sizeof(*f->entries));
  f->keys = lc_calloc_with_allocator(&allocator, 80, sizeof(*f->keys));
  f->metadata = lc_calloc_with_allocator(&allocator, 80, sizeof(*f->metadata));
  REQUIRE(f->entries && f->keys && f->metadata);
  f->count = 85;
  f->fail_before = SIZE_MAX;
  for (size_t i = 0; i < 80; ++i) {
    size_t key = 79 - i;
    REQUIRE(snprintf(f->keys[i], sizeof(f->keys[i]), "k%03u",
                     (unsigned int)key) > 0);
    f->metadata[i][0] = (unsigned char)key;
    f->metadata[i][2] = (unsigned char)(255 - key);
    lc_pouch_state_entry *r = &f->entries[i];
    r->key = f->keys[i];
    r->seen = r->found = 1;
    r->record_type = 1;
    r->version = 1;
    r->index_seq = key + 1;
    r->lifetime = key + 1;
    r->has_query_index_seq = !(key % 2);
    r->query_index_seq = key / 2;
    r->content_type = "application/x";
    r->etag = "etag";
    r->payload_context = "immutable-context";
    r->descriptor = "borrowed-descriptor";
    r->metadata = f->metadata[i];
    r->metadata_length = 3;
    r->bytes = r->cipher_bytes = 7;
    r->updated_at_unix = 1234;
    r->retention = 4321;
    r->has_query_hidden = 1;
    r->payload_span.present = r->has_record_ref = 1;
    r->payload_span.container_leaf = r->record_container_leaf =
        "seg-00000000000000000001.log";
    r->record_offset = r->payload_span.record_offset = key * 100;
    r->payload_span.payload_offset = key * 100 + 70;
    r->payload_span.payload_length = 7;
    r->payload_span.payload_crc = 12345;
  }
  f->entries[80] = f->entries[79];
  f->entries[80].version = 3;
  f->entries[80].index_seq = 100;
  f->entries[80].query_index_seq = 97;
  f->entries[81] = f->entries[80];
  f->entries[81].version = 2;
  f->entries[81].index_seq = 200;
  f->entries[81].query_index_seq = 199;
  f->entries[82] = f->entries[80];
  f->entries[82].index_seq = 99;
  f->entries[82].query_index_seq = 98;
  f->entries[83] = f->entries[80];
  f->entries[83].record_type = 4;
  f->entries[83].index_seq = 101;
  f->entries[83].has_query_index_seq = 0;
  f->entries[83].query_index_seq = 0;
  f->entries[84].seen = f->entries[84].control = 1;
  f->entries[84].key = "water";
  f->entries[84].record_type = 6;
  f->entries[84].version = f->entries[84].index_seq = 1000;
  f->entries[84].query_index_seq = 900;
}
static void capture_fixture_close(capture_fixture *f) {
  lc_free_with_allocator(&allocator, f->entries);
  lc_free_with_allocator(&allocator, f->keys);
  lc_free_with_allocator(&allocator, f->metadata);
}
static int replay_fixture(void *context, lc_pouch_storage_visit_fn visit,
                          void *visitor, lc_error *e) {
  capture_fixture *f = context;
  for (size_t i = 0; i <= f->count; ++i) {
    if (i == f->fail_before)
      return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected captured replay",
                          NULL, NULL, "test");
    if (i < f->count) {
      int rc = visit(visitor, i + 1, &f->entries[i], e);
      if (rc != LC_OK)
        return rc;
    }
  }
  return LC_OK;
}
typedef struct capture_observer {
  size_t count, fail;
} capture_observer;
static int inspect_capture(void *context, const lc_pouch_state_entry *r,
                           lc_error *e) {
  capture_observer *c = context;
  size_t ordinal = c->count++;
  REQUIRE(r->has_query_index_seq && r->seen);
  if (!ordinal)
    REQUIRE(r->control && r->version == 1000 && r->index_seq == 1000 &&
            r->query_index_seq == 900);
  else {
    size_t key = ordinal - 1;
    char expected[16];
    REQUIRE(snprintf(expected, sizeof(expected), "k%03u", (unsigned int)key) >
            0);
    REQUIRE(key < 80 && !strcmp(expected, r->key) && r->found && !r->control);
    REQUIRE(r->version == (key ? 1 : 3) &&
            r->index_seq == (key ? key + 1 : 101) &&
            r->query_index_seq == (key ? key % 2 ? key + 1 : key / 2 : 97));
    REQUIRE(!strcmp(r->content_type, "application/x") &&
            !strcmp(r->etag, "etag") &&
            !strcmp(r->descriptor, "borrowed-descriptor") &&
            !strcmp(r->payload_context, "immutable-context"));
    REQUIRE(r->metadata_length == 3 && r->metadata[0] == key &&
            !r->metadata[1] && r->metadata[2] == 255 - key);
    REQUIRE(r->bytes == 7 && r->cipher_bytes == 7 &&
            r->updated_at_unix == 1234 && r->retention == 4321 &&
            r->has_query_hidden && !r->query_hidden &&
            r->payload_span.present && r->has_record_ref);
    REQUIRE(!strcmp(r->payload_span.container_leaf,
                    "seg-00000000000000000001.log") &&
            !strcmp(r->record_container_leaf, r->payload_span.container_leaf) &&
            r->record_offset == key * 100 &&
            r->payload_span.record_offset == key * 100 &&
            r->payload_span.payload_offset == key * 100 + 70 &&
            r->payload_span.payload_length == 7 &&
            r->payload_span.payload_crc == 12345);
  }
  if (c->fail && c->count == c->fail)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected snapshot visitor",
                        NULL, NULL, "test");
  return LC_OK;
}
static int inspect_empty_capture(void *context, const lc_pouch_state_entry *r,
                                 lc_error *e) {
  size_t *count = context;
  (void)e;
  REQUIRE(!(*count)++ && r->control && !r->version && !r->index_seq &&
          !r->query_index_seq);
  return LC_OK;
}
static int inspect_uncheckpointed_capture(void *context,
                                          const lc_pouch_state_entry *r,
                                          lc_error *e) {
  capture_observer *c = context;
  if (c->count)
    return inspect_capture(context, r, e);
  REQUIRE(r->control && r->version == 200 && r->index_seq == 200 &&
          r->query_index_seq == 97);
  ++c->count;
  return LC_OK;
}
static void projection_faults(const char *path) {
  lc_error e;
  lc_error_init(&e);
  ps_pouch_state_options o = options(0, 0);
  lc_pouch *p = NULL;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  projection_put(p, "outside", 31, 50, 0, 1, &e);
  capture_fixture f;
  capture_fixture_open(&f);
  size_t baseline = live, operations = 0;
  lc_pouch_state_snapshot *captured = NULL;
  /* Rejected payload generations still consume their namespace index. A
   * later HIGH_WATER must not mask loss of that allocation during capture. */
  for (int low_water = 0; low_water < 2; ++low_water) {
    lc_pouch_state_entry water = f.entries[84];
    f.count = low_water ? 85 : 84;
    f.entries[84].version = f.entries[84].index_seq = 50;
    f.entries[84].query_index_seq = 40;
    ok(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured, &e),
       &e);
    capture_observer c = {0};
    ok(lc_pouch_state_snapshot_visit(captured, inspect_uncheckpointed_capture,
                                     &c, &e),
       &e);
    REQUIRE(c.count == 81);
    lc_pouch_state_snapshot_close(captured);
    captured = NULL;
    f.entries[84] = water;
    REQUIRE(live == baseline);
  }
  f.count = 85;
  for (int null_error = 0; null_error < 2; ++null_error)
    for (size_t nth = 0; nth <= operations; ++nth) {
      fail_at = nth ? nth : SIZE_MAX;
      allocation = 0;
      __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
      int rc = lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured,
                                               null_error ? NULL : &e);
      __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
      if (!nth)
        operations = allocation;
      if (nth)
        REQUIRE(rc == LC_ERR_NOMEM && !captured);
      else {
        ok(rc, &e);
        capture_observer c = {0};
        ok(lc_pouch_state_snapshot_visit(captured, inspect_capture, &c, &e),
           &e);
        REQUIRE(c.count == 81);
        lc_pouch_state_snapshot_close(captured);
      }
      captured = NULL;
      clear(&e);
      REQUIRE(live == baseline);
    }
  for (int null_error = 0; null_error < 2; ++null_error)
    for (size_t i = 0; i < 3; ++i) {
      f.fail_before = i == 0 ? 0 : i == 1 ? 42 : f.count;
      REQUIRE(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured,
                                              null_error ? NULL : &e) ==
              LC_ERR_TRANSPORT);
      REQUIRE(!captured);
      clear(&e);
      REQUIRE(live == baseline);
    }
  f.fail_before = SIZE_MAX;
  lc_pouch_state_entry saved = f.entries[40];
  for (int bad = 1; bad < 3; ++bad) {
    if (bad == 1)
      f.entries[40].key = ".staging/transaction/key";
    else
      f.entries[40].metadata = NULL;
    REQUIRE(
        lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured, &e) ==
        (bad == 2 ? LC_ERR_INVALID : LC_ERR_UNSUPPORTED));
    REQUIRE(!captured);
    clear(&e);
    REQUIRE(live == baseline);
    f.entries[40] = saved;
  }
  ok(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured, &e), &e);
  /* Every borrowed source field is copied. Altering the source cannot alter
   * visits, including a source record which was superseded during replay. */
  for (size_t i = 0; i < 80; ++i) {
    f.keys[i][0] = 'X';
    f.metadata[i][0] = 255;
  }
  for (size_t ordinal = 1; ordinal <= 81; ++ordinal) {
    capture_observer c = {0, ordinal};
    REQUIRE(lc_pouch_state_snapshot_visit(captured, inspect_capture, &c, &e) ==
            LC_ERR_TRANSPORT);
    REQUIRE(c.count == ordinal);
    clear(&e);
  }
  capture_observer c = {0};
  fail_at = 1;
  allocation = 0;
  __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
  int rc = lc_pouch_state_snapshot_visit(captured, inspect_capture, &c, &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  ok(rc, &e);
  REQUIRE(!allocation && c.count == 81);
  REQUIRE(lc_pouch_state_snapshot_visit(captured, NULL, NULL, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  REQUIRE(lc_pouch_state_snapshot_visit(NULL, inspect_capture, &c, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  lc_pouch_state_snapshot_close(captured);
  captured = (lc_pouch_state_snapshot *)(uintptr_t)1;
  REQUIRE(lc_pouch_state_snapshot_capture(NULL, replay_fixture, &f, &captured,
                                          &e) == LC_ERR_INVALID);
  REQUIRE(!captured);
  clear(&e);
  captured = (lc_pouch_state_snapshot *)(uintptr_t)1;
  REQUIRE(lc_pouch_state_snapshot_capture(p, NULL, &f, &captured, &e) ==
          LC_ERR_INVALID);
  REQUIRE(!captured);
  clear(&e);
  REQUIRE(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, NULL, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  f.count = 0;
  ok(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &captured, &e), &e);
  size_t empty = 0;
  ok(lc_pouch_state_snapshot_visit(captured, inspect_empty_capture, &empty, &e),
     &e);
  REQUIRE(empty == 1);
  lc_pouch_state_snapshot_close(captured);
  lc_pouch_state_snapshot_close(NULL);
  REQUIRE(live == baseline);
  capture_fixture_close(&f);
  /* The temporary projection never advances or replaces the live namespace. */
  read_value(p, "projection", "outside", 31, 50, 1, &e);
  lc_pouch_generation seq;
  ok(lc_pouch_state_index_seq(p, "projection", &seq, &e), &e);
  REQUIRE(seq == 1);
  projection_put(p, "outside", 31, 51, 0, 2, &e);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  printf("logical capture allocation failure sites: %lu (with/without error)\n",
         (unsigned long)operations);
  lc_error_cleanup(&e);
}
static void sequence_recovery_check(lc_pouch *p, int object, lc_error *e) {
  lc_pouch_generation seq;
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_index_seq(p, "sequence", &seq, e), e);
  REQUIRE(seq == 2);
  ok(lc_pouch_state_query_index_seq(p, "sequence", &seq, e), e);
  REQUIRE(seq == (object ? 0 : 1));
  ok(lc_pouch_state_read_metadata(p, "sequence", "key", &r, e), e);
  REQUIRE(r.found && r.has_body && r.version == 3 && r.index_seq == 1);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  read_value(p, "sequence", "key", 31, 61, 3, e);
}
static void sequence_recovery_compact(lc_pouch *p, const char *ns,
                                      int multifile, lc_error *e) {
  if (multifile) {
    lc_pouch_maintenance_options o = {0};
    lc_pouch_maintenance_result r = {0};
    o.ns = ns;
    o.force = 1;
    ok(lc_pouch_maintenance_run(p, &o, &r, e), e);
    REQUIRE(r.compacted);
    lc_pouch_maintenance_result_cleanup(&allocator, &r);
  } else {
    ps_pouch_compaction_result r = {0};
    ok(ps_pouch_state_compact(p, ns, &r, e), e);
    REQUIRE(r.snapshot && !r.skipped);
  }
}
static void sequence_recovery(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int sync = 0; sync < 2; ++sync)
        for (int object = 0; object < 2; ++object)
          for (int compact = 0; compact < 3; ++compact) {
            ps_pouch_state_options o = options(crypto, sync);
            lc_pouch *p = NULL;
            char native[1400];
            o.logical.background_compaction_enabled_set = 1;
            o.logical.background_compaction_enabled = 0;
            o.logical.segment_target_bytes = 1;
            REQUIRE(snprintf(native, sizeof(native), "%s/native-%d-%d-%d-%d",
                             directory, crypto, sync, object,
                             compact) < (int)sizeof(native));
            if (multifile)
              ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
            else
              ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
            lc_pouch_generation seq = 99;
            ok(lc_pouch_state_query_index_seq(p, "empty", &seq, &e), &e);
            REQUIRE(!seq);
            seq = 99;
            ok(lc_pouch_state_index_seq(p, "empty", &seq, &e), &e);
            REQUIRE(!seq);
            for (int stale = 0; stale < 2; ++stale) {
              lc_pouch_state_write_options w = {0};
              lc_pouch_state_write_result r = {0};
              input in = source(31, stale ? 62 : 61);
              w.object_record = object;
              w.has_logical_version = 1;
              w.logical_version = stale ? 2 : 3;
              ok(put(p, "sequence", "key", &in, &w, &r, &e), &e);
              REQUIRE(r.version == w.logical_version &&
                      r.index_seq == (lc_pouch_generation)(stale + 1));
              lc_pouch_state_write_result_cleanup(&allocator, &r);
            }
            sequence_recovery_check(p, object, &e);
            if (compact == 1) {
              sequence_recovery_compact(p, "sequence", multifile, &e);
              sequence_recovery_check(p, object, &e);
            }
            lc_pouch_close(p);
            REQUIRE(live == 0);
            for (int reopened = 0; reopened < 2; ++reopened) {
              if (multifile)
                ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
              else
                ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
              if (reopened) {
                /* Independently exercise query as the first cold operation;
                 * the first reopening above starts with index instead. */
                seq = 99;
                ok(lc_pouch_state_query_index_seq(p, "sequence", &seq, &e), &e);
                REQUIRE(seq == (object ? 0 : 1));
              }
              sequence_recovery_check(p, object, &e);
              if (compact == 2 && !reopened) {
                sequence_recovery_compact(p, "sequence", multifile, &e);
                sequence_recovery_check(p, object, &e);
              }
              if (!reopened) {
                lc_pouch_close(p);
                REQUIRE(live == 0);
              }
            }
            lc_pouch_state_write_result r = {0};
            input in = source(17, 63);
            ok(put(p, "sequence", "next", &in, NULL, &r, &e), &e);
            REQUIRE(r.version == 1 && r.index_seq == 3);
            lc_pouch_state_write_result_cleanup(&allocator, &r);
            read_value(p, "sequence", "key", 31, 61, 3, &e);
            read_value(p, "sequence", "next", 17, 63, 1, &e);
            lc_pouch_close(p);
            REQUIRE(live == 0);
            if (!multifile)
              REQUIRE(unlink(path) == 0);
          }
  lc_error_cleanup(&e);
}
static int inspect_lifetime_capture(void *context,
                                    const lc_pouch_state_entry *r,
                                    lc_error *e) {
  size_t *count = context;
  (void)e;
  if (!(*count)++)
    REQUIRE(r->control && r->index_seq == 203 && r->query_index_seq == 105);
  else
    REQUIRE(!r->control && r->found && r->payload_span.present &&
            r->version == 1 && r->index_seq == 105 && r->lifetime == 103 &&
            r->query_index_seq == 105);
  return LC_OK;
}
static void lifetime_ordering(lc_pouch *p, lc_error *e) {
  capture_fixture f;
  capture_fixture_open(&f);
  lc_pouch_state_entry body = f.entries[0];
  static const unsigned long versions[] = {19, 20, 0, 1, 999, 1000,
                                           0,  1,  0, 1, 1};
  static const unsigned long indices[] = {100, 101, 102, 103, 200, 201,
                                          202, 104, 203, 105, 104};
  for (size_t i = 0; i < 11; ++i) {
    f.entries[i] = body;
    f.entries[i].version = versions[i];
    f.entries[i].index_seq = indices[i];
    f.entries[i].has_query_index_seq = 1;
    f.entries[i].query_index_seq = i == 2 ? 101 : i == 7 ? 103 : indices[i];
    f.entries[i].lifetime = i < 3 || (i >= 4 && i <= 6) ? 1 : 103;
    if (i == 1 || i == 5) {
      f.entries[i].record_type = 2;
      f.entries[i].found = f.entries[i].payload_span.present = 0;
      f.entries[i].content_type = NULL;
      f.entries[i].metadata = NULL;
      f.entries[i].metadata_length = 0;
    }
    if (i == 2 || i == 6 || i == 7) {
      f.entries[i].record_type = 4;
      if (i != 7)
        f.entries[i].payload_span.present = 0;
    }
    if (!f.entries[i].payload_span.present) {
      f.entries[i].bytes = f.entries[i].cipher_bytes = 0;
      f.entries[i].descriptor = f.entries[i].payload_context = NULL;
      f.entries[i].retention = 0;
    }
  }
  /* Replay both complete history and a selected snapshot containing only
   * the recreated lifetime, followed by stale records from the former one. */
  for (int compacted = 0; compacted < 2; ++compacted) {
    lc_pouch_state_snapshot *capture = NULL;
    size_t count = 0;
    f.count = compacted ? 8 : 11;
    if (compacted)
      f.entries += 3;
    ok(lc_pouch_state_snapshot_capture(p, replay_fixture, &f, &capture, e), e);
    ok(lc_pouch_state_snapshot_visit(capture, inspect_lifetime_capture, &count,
                                     e),
       e);
    REQUIRE(count == 2);
    lc_pouch_state_snapshot_close(capture);
    if (compacted)
      f.entries -= 3;
  }
  capture_fixture_close(&f);
}
static void lifetime_put(lc_pouch *p, int object, int memory, size_t length,
                         unsigned char seed, lc_pouch_generation version,
                         lc_pouch_generation index, lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result r = {0};
  o.object_record = object;
  if (memory) {
    unsigned char bytes[31];
    lc_source *body = NULL;
    REQUIRE(length <= sizeof(bytes));
    for (size_t i = 0; i < length; ++i)
      bytes[i] = byte_at(i, seed);
    ok(lc_source_from_memory(bytes, length, &body, e), e);
    ok(lc_pouch_state_write(p, "lifetimes", "key", body, &o, &r, e), e);
    lc_source_close(body);
  } else {
    input in = source(length, seed);
    ok(put(p, "lifetimes", "key", &in, &o, &r, e), e);
  }
  REQUIRE(r.version == version && r.index_seq == index);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  read_value(p, "lifetimes", "key", length, seed, version, e);
}
static uint64_t native_lifetime(lc_pouch *p, lc_error *e) {
  lc_pouch_namespace_manifest manifest = {0};
  char path[1800];
  struct stat st;
  uint64_t at = 0, lifetime = 0;
  int found, truncated;
  ok(lc_pouch_namespace_manifest_open(&allocator, p->root_path, "lifetimes",
                                      &manifest, NULL, NULL, e),
     e);
  REQUIRE(snprintf(path, sizeof(path), "%s/segments/%s",
                   manifest.namespace_path,
                   manifest.active_segment) < (int)sizeof(path));
  int fd = open(path, O_RDONLY);
  REQUIRE(fd >= 0 && fstat(fd, &st) == 0 && st.st_size > 0);
  while (at < (uint64_t)st.st_size) {
    lc_pouch_state_entry r = {0};
    uint64_t next;
    ok(lc_pouch_state_record_read(
           lc_pouch_record_file_io(fd), &allocator, manifest.active_segment, at,
           (uint64_t)st.st_size, 0, 0, NULL, &r, &next, &found, &truncated, e),
       e);
    REQUIRE(found && !truncated && next > at);
    if (!r.control && !strcmp(r.key, "key"))
      lifetime = r.lifetime;
    lc_pouch_state_entry_cleanup(&allocator, &r);
    at = next;
  }
  REQUIRE(close(fd) == 0 && lifetime);
  lc_pouch_namespace_manifest_cleanup(&allocator, &manifest);
  return lifetime;
}
static void lifetime_staging(lc_pouch *p, int object, lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result r = {0};
  o.object_record = object;
  o.has_logical_version = 1;
  o.logical_version = 2;
  input in = source(31, 91);
  in.pub.impl = &in;
  ok(lc_pouch_state_stage_write(p, "lifetimes", "key", "lifetime-live", &in.pub,
                                &o, &r, e),
     e);
  REQUIRE(r.version == 2);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_commit_staged(p, "lifetimes", "key", "lifetime-live", &r,
                                  e),
     e);
  REQUIRE(r.version == 2);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  REQUIRE(native_lifetime(p, e) == 18);
  read_value(p, "lifetimes", "key", 31, 91, 2, e);
  ok(lc_pouch_state_delete(p, "lifetimes", "key", &o, &r, e), e);
  REQUIRE(r.version == 3);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  o.logical_version = 1;
  in = source(31, 92);
  in.pub.impl = &in;
  ok(lc_pouch_state_stage_write(p, "lifetimes", "key", "lifetime-recreate",
                                &in.pub, &o, &r, e),
     e);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_promote_staged(p, "lifetimes", "key", "lifetime-recreate",
                                   NULL, &r, e),
     e);
  REQUIRE(r.version == 1 && native_lifetime(p, e) == r.index_seq);
  lc_pouch_state_write_result_cleanup(&allocator, &r);
  read_value(p, "lifetimes", "key", 31, 92, 1, e);
  sequence_recovery_compact(p, "lifetimes", 1, e);
  read_value(p, "lifetimes", "key", 31, 92, 1, e);
}
static void lifetime_exhaustion(const char *path, int multifile,
                                const ps_pouch_state_options *o,
                                lc_pouch_generation query, lc_error *e) {
  lc_pouch *p = NULL;
  if (multifile) {
    lc_pouch_namespace_manifest manifest = {0};
    char file[1800];
    ok(lc_pouch_namespace_manifest_open(&allocator, path, "lifetimes",
                                        &manifest, NULL, NULL, e),
       e);
    REQUIRE(snprintf(file, sizeof(file), "%s/segments/%s",
                     manifest.namespace_path,
                     manifest.active_segment) < (int)sizeof(file));
    int fd = open(file, O_RDWR);
    REQUIRE(fd >= 0 && lseek(fd, 0, SEEK_END) >= 0);
    ok(lc_pouch_state_record_write_high_water(
           &allocator, lc_pouch_record_file_io(fd), UINT64_MAX, 0, e),
       e);
    REQUIRE(fdatasync(fd) == 0 && close(fd) == 0);
    REQUIRE(snprintf(file, sizeof(file), "%s/sequence.clean",
                     manifest.namespace_path) < (int)sizeof(file));
    REQUIRE(unlink(file) == 0 || errno == ENOENT);
    lc_pouch_namespace_manifest_cleanup(&allocator, &manifest);
  } else {
    void *ns = NULL;
    lc_pouch_storage_append append = {0};
    ok(ps_pouch_state_open(path, 0, &allocator, o, &p, e), e);
    ok(p->storage.ops->namespace_open(p->storage.context, "lifetimes", 0, &ns,
                                      e),
       e);
    ok(p->storage.ops->append_begin(p->storage.context, ns, 0, 48,
                                    LC_POUCH_STORAGE_APPEND_COMPLETE, &append,
                                    e),
       e);
    ok(lc_pouch_state_record_write_high_water(&allocator, append.io, UINT64_MAX,
                                              0, e),
       e);
    ok(p->storage.ops->append_commit(p->storage.context, &append,
                                     LC_POUCH_STORAGE_COMMIT_DEFAULT, e),
       e);
    p->storage.ops->append_close(p->storage.context, &append);
    p->storage.ops->namespace_close(p->storage.context, ns);
    lc_pouch_close(p);
    REQUIRE(!live);
  }
  for (int reopened = 0; reopened < 2; ++reopened) {
    if (multifile)
      ok(lc_pouch_open(path, &allocator, &o->logical, &p, e), e);
    else
      ok(ps_pouch_state_open(path, 0, &allocator, o, &p, e), e);
    lc_pouch_generation seq;
    ok(lc_pouch_state_index_seq(p, "lifetimes", &seq, e), e);
    REQUIRE(seq == UINT64_MAX);
    ok(lc_pouch_state_query_index_seq(p, "lifetimes", &seq, e), e);
    REQUIRE(seq == query);
    lc_pouch_state_write_result w = {0};
    lc_pouch_state_read_result r = {0};
    input in = source(17, 99);
    REQUIRE(put(p, "lifetimes", "exhausted", &in, NULL, &w, e) ==
            LC_ERR_INVALID);
    REQUIRE(!w.version && !w.etag);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    clear(e);
    ok(lc_pouch_state_read_metadata(p, "lifetimes", "exhausted", &r, e), e);
    REQUIRE(!r.found);
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    ok(lc_pouch_state_index_seq(p, "lifetimes", &seq, e), e);
    REQUIRE(seq == UINT64_MAX);
    lc_pouch_close(p);
    REQUIRE(!live);
  }
}
static void lifetimes(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int sync = 0; sync < 2; ++sync)
        for (int object = 0; object < 2; ++object) {
          ps_pouch_state_options o = options(crypto, sync);
          lc_pouch *p = NULL;
          char native[1400];
          o.logical.segment_target_bytes = 1;
          REQUIRE(snprintf(native, sizeof(native), "%s/native-%d-%d-%d",
                           directory, crypto, sync,
                           object) < (int)sizeof(native));
          if (multifile)
            ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
          else
            ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
          lifetime_ordering(p, &e);
          lc_pouch_state_write_options meta = {0}, del = {0};
          lc_pouch_state_write_result w = {0};
          meta.object_record = del.object_record = object;
          meta.has_metadata = 1;
          meta.metadata = (const unsigned char *)"lease";
          meta.metadata_length = 5;
          ok(lc_pouch_state_update_metadata(p, "lifetimes", "key", &meta, &w,
                                            &e),
             &e);
          REQUIRE(w.version == 0 && w.index_seq == 1);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          lifetime_put(p, object, 0, 65569, 61, 1, 2, &e);
          size_t old_length = 65569;
          unsigned char old_seed = 61;
          for (size_t cycle = 0; cycle < 4; ++cycle) {
            lc_pouch_state_read_result held = {0}, placeholder = {0};
            ok(lc_pouch_state_read(p, "lifetimes", "key", &held, &e), &e);
            lifetime_put(p, object, !(cycle % 2), 17, 70, 2, 3 + 4 * cycle, &e);
            ok(lc_pouch_state_delete(p, "lifetimes", "key", &del, &w, &e), &e);
            REQUIRE(w.version == 3 && w.index_seq == 4 + 4 * cycle);
            lc_pouch_state_write_result_cleanup(&allocator, &w);
            ok(lc_pouch_state_read_metadata(p, "lifetimes", "key", &placeholder,
                                            &e),
               &e);
            REQUIRE(!placeholder.found);
            lc_pouch_state_read_result_cleanup(&allocator, &placeholder);
            ok(lc_pouch_state_update_metadata(p, "lifetimes", "key", &meta, &w,
                                              &e),
               &e);
            REQUIRE(w.version == 0 && w.index_seq == 5 + 4 * cycle);
            lc_pouch_state_write_result_cleanup(&allocator, &w);
            sequence_recovery_compact(p, "lifetimes", multifile, &e);
            ok(lc_pouch_state_read_metadata(p, "lifetimes", "key", &placeholder,
                                            &e),
               &e);
            REQUIRE(placeholder.found && !placeholder.has_body &&
                    !placeholder.version && placeholder.metadata_length == 5);
            lc_pouch_state_read_result_cleanup(&allocator, &placeholder);
            drain(held.body, old_length, old_seed, &e);
            ok(held.body->reset(held.body, &e), &e);
            if (!(cycle % 2)) {
              lc_pouch_state_read_result_cleanup(&allocator, &held);
              lc_pouch_close(p);
              REQUIRE(!live);
              if (multifile)
                ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
              else
                ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
            }
            old_length = 31;
            old_seed = (unsigned char)(80 + cycle);
            lifetime_put(p, object, cycle % 2, old_length, old_seed, 1,
                         6 + 4 * cycle, &e);
            sequence_recovery_compact(p, "lifetimes", multifile, &e);
            if (held.body) {
              drain(held.body, 31, (unsigned char)(79 + cycle), &e);
              lc_pouch_state_read_result_cleanup(&allocator, &held);
            }
            lc_pouch_close(p);
            REQUIRE(!live);
            if (multifile && cycle % 2)
              o.logical.compression =
                  !strcmp(o.logical.compression, "zlib") ? "none" : "zlib";
            if (multifile)
              ok(lc_pouch_open(native, &allocator, &o.logical, &p, &e), &e);
            else
              ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
            read_value(p, "lifetimes", "key", old_length, old_seed, 1, &e);
            lc_pouch_generation seq;
            ok(lc_pouch_state_index_seq(p, "lifetimes", &seq, &e), &e);
            REQUIRE(seq == 6 + 4 * cycle);
            ok(lc_pouch_state_query_index_seq(p, "lifetimes", &seq, &e), &e);
            REQUIRE(seq == (object ? 0 : 6 + 4 * cycle));
          }
          if (multifile)
            lifetime_staging(p, object, &e);
          lc_pouch_generation query;
          ok(lc_pouch_state_query_index_seq(p, "lifetimes", &query, &e), &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          lifetime_exhaustion(multifile ? native : path, multifile, &o, query,
                              &e);
          if (!multifile)
            REQUIRE(unlink(path) == 0);
        }
  lc_error_cleanup(&e);
}
static size_t lifetime_failure(const char *path, size_t nth, int crypto,
                               int sync, int object, int memory, int kind) {
  io_fault f = {0};
  ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(crypto, sync);
  lc_pouch *p = NULL;
  lc_error e;
  lc_error_init(&e);
  o.io = &io;
  o.logical.segment_target_bytes = 1;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  lifetime_put(p, object, 1, 17, 10, 1, 1, &e);
  lifetime_put(p, object, 1, 17, 11, 2, 2, &e);
  lc_pouch_state_write_options wopts = {0};
  lc_pouch_state_write_result w = {0};
  wopts.object_record = object;
  ok(lc_pouch_state_delete(p, "lifetimes", "key", &wopts, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  wopts.has_metadata = 1;
  wopts.metadata = (const unsigned char *)"lease";
  wopts.metadata_length = 5;
  ok(lc_pouch_state_update_metadata(p, "lifetimes", "key", &wopts, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  sequence_recovery_compact(p, "lifetimes", 0, &e);
  input in = source(memory ? 31 : 65569, 12);
  lc_source *body = NULL;
  unsigned char bytes[31];
  if (memory) {
    for (size_t i = 0; i < sizeof(bytes); ++i)
      bytes[i] = byte_at(i, 12);
    ok(lc_source_from_memory(bytes, sizeof(bytes), &body, &e), &e);
  }
  allocation = f.calls = 0;
  fail_at = f.fail = nth;
  f.partial = kind == 2;
  f.armed = kind != 0;
  __atomic_store_n(&armed, !kind, __ATOMIC_RELEASE);
  int rc =
      memory ? lc_pouch_state_write(p, "lifetimes", "key", body, &wopts, &w, &e)
             : put(p, "lifetimes", "key", &in, &wopts, &w, &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  size_t count = kind ? f.calls : allocation;
  lc_source_close(body);
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM ||
          (kind && rc == LC_ERR_TRANSPORT));
  if (nth == SIZE_MAX)
    ok(rc, &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  clear(&e);
  lc_pouch_close(p);
  REQUIRE(!live);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "lifetimes", "key", &r, &e), &e);
  if (!r.found || r.version != (r.has_body ? 1U : 0U))
    fprintf(stderr,
            "lifetime failure nth=%lu crypto=%d sync=%d object=%d memory=%d "
            "kind=%d rc=%d found=%d body=%d version=%llu index=%llu\n",
            (unsigned long)nth, crypto, sync, object, memory, kind, rc, r.found,
            r.has_body, (unsigned long long)r.version,
            (unsigned long long)r.index_seq);
  REQUIRE(r.found && r.version == (r.has_body ? 1 : 0));
  REQUIRE(r.metadata_length == 5 && !memcmp(r.metadata, "lease", 5));
  if (r.has_body)
    read_value(p, "lifetimes", "key", memory ? 31 : 65569, 12, 1, &e);
  if (rc == LC_OK)
    REQUIRE(r.has_body);
  lc_pouch_generation version = r.has_body ? 2 : 1;
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  in = source(31, 13);
  ok(put(p, "lifetimes", "key", &in, &wopts, &w, &e), &e);
  REQUIRE(w.version == version);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  sequence_recovery_compact(p, "lifetimes", 0, &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  read_value(p, "lifetimes", "key", 31, 13, version, &e);
  lc_pouch_close(p);
  REQUIRE(!live && unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void lifetime_faults(const char *path) {
  size_t total = 0;
  for (int crypto = 0; crypto <= 3; crypto += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int object = 0; object < 2; ++object)
        for (int memory = 0; memory < 2; ++memory)
          for (int kind = 0; kind < 3; ++kind) {
            size_t count = lifetime_failure(path, SIZE_MAX, crypto, sync,
                                            object, memory, kind);
            for (size_t nth = 1; nth <= count; ++nth)
              (void)lifetime_failure(path, nth, crypto, sync, object, memory,
                                     kind);
            total += count;
          }
  printf("recreated lifetime allocation/I/O/torn failure sites: %lu\n",
         (unsigned long)total);
}
typedef struct capture_visit_job {
  const lc_pouch_state_snapshot *snapshot;
  int hold, ready, release;
  size_t passes;
} capture_visit_job;
static int held_capture_visit(void *context, const lc_pouch_state_entry *r,
                              lc_error *e) {
  capture_visit_job *job = context;
  (void)r;
  (void)e;
  if (job->hold) {
    __atomic_store_n(&job->ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&job->release, __ATOMIC_ACQUIRE))
      sched_yield();
    job->hold = 0;
  }
  return LC_OK;
}
static void *capture_visit_worker(void *context) {
  capture_visit_job *job = context;
  lc_error e;
  lc_error_init(&e);
  if (job->hold) {
    ok(lc_pouch_state_snapshot_visit(job->snapshot, held_capture_visit, job,
                                     &e),
       &e);
    ++job->passes;
  } else
    for (size_t n = 0; n < 16; ++n) {
      projected_check c = {0};
      ok(lc_pouch_state_snapshot_visit(job->snapshot, check_projected, &c, &e),
         &e);
      REQUIRE(c.count == 6);
      ++job->passes;
    }
  lc_error_cleanup(&e);
  return NULL;
}
typedef struct capture_build_job {
  lc_pouch *pouch;
  ps_namespace_view *view;
  lc_pouch_state_snapshot *snapshot;
  lc_pouch_storage_visit_fn forward;
  void *visitor;
  int ready, release, done, rc;
} capture_build_job;
static int held_replay_record(void *context, uint64_t segment,
                              const lc_pouch_state_entry *r, lc_error *e) {
  capture_build_job *job = context;
  int rc = job->forward(job->visitor, segment, r, e);
  if (rc == LC_OK && !__atomic_load_n(&job->ready, __ATOMIC_ACQUIRE)) {
    __atomic_store_n(&job->ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&job->release, __ATOMIC_ACQUIRE))
      sched_yield();
  }
  return rc;
}
static int replay_held(void *context, lc_pouch_storage_visit_fn visit,
                       void *visitor, lc_error *e) {
  capture_build_job *job = context;
  job->forward = visit;
  job->visitor = visitor;
  return ps_namespace_view_visit(job->view, 0, held_replay_record, job, e);
}
static void *capture_build_worker(void *context) {
  capture_build_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc = lc_pouch_state_snapshot_capture(job->pouch, replay_held, job,
                                            &job->snapshot, &e);
  if (job->rc != LC_OK)
    fprintf(stderr, "held capture: %d: %s\n", job->rc,
            e.message ? e.message : "?");
  lc_error_cleanup(&e);
  __atomic_store_n(&job->done, 1, __ATOMIC_RELEASE);
  return NULL;
}
static void projection_concurrency(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int mode = 0; mode < 2; ++mode) {
    ps_pouch_state_options o = options(mode ? 3 : 0, mode);
    lc_pouch *p = NULL;
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    projection_fixture(p, &e);
    void *ns = NULL;
    ps_namespace_view *view = NULL;
    ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0, &ns,
                                      &e),
       &e);
    ok(ps_namespace_view_open(ns, &view, &e), &e);
    capture_build_job build = {0};
    build.pouch = p;
    build.view = view;
    pthread_t building;
    REQUIRE(pthread_create(&building, NULL, capture_build_worker, &build) == 0);
    while (!__atomic_load_n(&build.ready, __ATOMIC_ACQUIRE) &&
           !__atomic_load_n(&build.done, __ATOMIC_ACQUIRE))
      sched_yield();
    REQUIRE(__atomic_load_n(&build.ready, __ATOMIC_ACQUIRE));
    projection_put(p, "a-state", 31, 99, 0, 12, &e);
    __atomic_store_n(&build.release, 1, __ATOMIC_RELEASE);
    REQUIRE(pthread_join(building, NULL) == 0 && build.rc == LC_OK);
    lc_pouch_state_snapshot *captured = build.snapshot;
    read_value(p, "projection", "a-state", 31, 99, 3, &e);
    ps_namespace_view_close(view);
    p->storage.ops->namespace_close(p->storage.context, ns);
    capture_visit_job held = {captured, 1, 0, 0, 0};
    capture_visit_job independent = {captured, 0, 0, 0, 0};
    pthread_t first, second;
    REQUIRE(pthread_create(&first, NULL, capture_visit_worker, &held) == 0);
    while (!__atomic_load_n(&held.ready, __ATOMIC_ACQUIRE))
      sched_yield();
    REQUIRE(pthread_create(&second, NULL, capture_visit_worker, &independent) ==
            0);
    REQUIRE(pthread_join(second, NULL) == 0 && independent.passes == 16);
    /* The first visitor remains blocked while this same-namespace mutation
     * commits. Neither immutable visits nor capture hold mutation authority. */
    projection_put(p, "a-state", 31, 100, 0, 13, &e);
    read_value(p, "projection", "a-state", 31, 100, 4, &e);
    __atomic_store_n(&held.release, 1, __ATOMIC_RELEASE);
    REQUIRE(pthread_join(first, NULL) == 0 && held.passes == 1);
    projected_check c = {0};
    ok(lc_pouch_state_snapshot_visit(captured, check_projected, &c, &e), &e);
    REQUIRE(c.count == 6);
    lc_pouch_state_snapshot_close(captured);
    one_file(directory);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
static int maintenance_reserved(void *context, const ps_namespace_source *s,
                                lc_error *e) {
  (void)context;
  (void)e;
  if (s->kind == PS_CATALOG_SNAPSHOT || s->kind == PS_CATALOG_BODY)
    REQUIRE(s->sealed && s->tail && s->capacity >= s->tail &&
            s->capacity - s->tail < PS_PAGE);
  return LC_OK;
}
static void maintenance_empty(const char *path) {
  lc_error e;
  lc_error_init(&e);
  ps_pouch_compaction_result result;
  REQUIRE(ps_pouch_state_compact(NULL, "projection", &result, &e) ==
          LC_ERR_INVALID);
  REQUIRE(!result.snapshot && !result.records);
  clear(&e);
  ps_pouch_state_options o = options(0, 0);
  lc_pouch *p = NULL;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  REQUIRE(ps_pouch_state_compact(p, "", &result, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(ps_pouch_state_compact(p, NULL, &result, &e) == LC_ERR_INVALID);
  clear(&e);
  void *ns = NULL;
  ok(p->storage.ops->namespace_open(p->storage.context, "projection", 1, &ns,
                                    &e),
     &e);
  ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
  REQUIRE(result.skipped && !result.snapshot && !result.records);
  projection_meta(p, "placeholder", "only", 1, 1, &e);
  ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
  REQUIRE(!result.skipped && result.snapshot && result.records == 1 &&
          !result.body_bytes && !result.copied_bytes);
  ok(ps_namespace_sources_visit(ns, maintenance_reserved, NULL, &e), &e);
  p->storage.ops->namespace_close(p->storage.context, ns);
  lc_pouch_close(p);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "projection", "placeholder", &r, &e), &e);
  REQUIRE(r.found && !r.has_body && !r.version && r.query_hidden &&
          r.metadata_length == 4 && !memcmp(r.metadata, "only", 4));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
  REQUIRE(result.records == 1 && !result.body_bytes);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void maintenance(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  maintenance_empty(path);
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync) {
      ps_pouch_state_options o = options(crypto, sync);
      o.logical.segment_target_bytes = 8 * PS_MIB;
      lc_pouch *p = NULL;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      projection_fixture(p, &e);
      lc_pouch_state_read_result pinned = {0};
      ok(lc_pouch_state_read(p, "projection", "a-state", &pinned, &e), &e);
      ps_pouch_compaction_result result;
      ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
      REQUIRE(!result.skipped && result.records == 5 && result.snapshot &&
              result.boundary == 1 && result.body_bytes &&
              result.metadata_bytes < 65536 && result.copied_bytes);
      ok(ps_metadata_repack(ps_root_space(ps_pouch_state_root(p)), &e), &e);
      /* The resident cache must be invalidated before covered sources can be
       * retired. These reads cannot fall back to their prior segment labels. */
      read_value(p, "projection", "a-state", 65569, 34, 2, &e);
      read_value(p, "projection", "z-object", 1048601, 35, 2, &e);
      drain(pinned.body, 65569, 34, &e);
      lc_pouch_state_read_result_cleanup(&allocator, &pinned);
      uint64_t first = result.snapshot;
      ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
      REQUIRE(result.snapshot > first && result.boundary == 1 &&
              result.records == 5 && !result.pending_sources);
      projection_put(p, "a-state", 31, 99, 0, 12, &e);
      projection_put(p, "tail", 31, 77, 0, 13, &e);
      projection_meta(p, "z-object", "tail-object", 0, 14, &e);
      ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
      REQUIRE(result.records == 6 && result.boundary == 2);
      projection_readback(p, &e);
      ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
      REQUIRE(result.records == 6 && result.boundary == 2);
      projection_readback(p, &e);
      void *ns = NULL;
      ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0,
                                        &ns, &e),
         &e);
      ok(ps_namespace_sources_visit(ns, maintenance_reserved, NULL, &e), &e);
      p->storage.ops->namespace_close(p->storage.context, ns);
      one_file(directory);
      lc_pouch_close(p);
      REQUIRE(live == 0);
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      projection_readback(p, &e);
      lc_pouch_close(p);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
static void maintenance_fixture(lc_pouch *p, lc_error *e) {
  projection_put(p, "a-state", 65569, 1, 0, 1, e);
  projection_put(p, "z-object", 65569, 2, 1, 2, e);
  projection_put(p, "a-state", 65569, 3, 0, 3, e);
  projection_meta(p, "a-state", "keep", 0, 4, e);
  projection_meta(p, "l-placeholder", "lease", 1, 5, e);
  projection_put(p, "m-delete", 31, 4, 0, 6, e);
  lc_pouch_state_write_result w = {0};
  ok(lc_pouch_state_delete(p, "projection", "m-delete", NULL, &w, e), e);
  REQUIRE(w.index_seq == 7);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  projection_put(p, "n-state", 0, 5, 0, 8, e);
}
static void maintenance_readback(lc_pouch *p, lc_error *e) {
  read_value(p, "projection", "a-state", 65569, 3, 2, e);
  read_value(p, "projection", "z-object", 65569, 2, 1, e);
  read_value(p, "projection", "n-state", 0, 5, 1, e);
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "projection", "m-delete", &r, e), e);
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_read_metadata(p, "projection", "a-state", &r, e), e);
  REQUIRE(r.index_seq == 4 && r.metadata_length == 4 &&
          !memcmp(r.metadata, "keep", 4));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_read_metadata(p, "projection", "l-placeholder", &r, e), e);
  REQUIRE(r.found && !r.has_body && !r.version && r.metadata_length == 5);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  lc_pouch_generation seq;
  ok(lc_pouch_state_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 8);
  ok(lc_pouch_state_query_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 8);
}
static ssize_t maintenance_io_read(void *context, int fd, void *p, size_t n,
                                   off_t at) {
  io_fault *f = context;
  if (io_fails(f))
    return -1;
  return io_read(context, fd, p, n, at);
}
static size_t maintenance_failure(const char *path, size_t nth, int kind,
                                  int durable, int partial) {
  lc_error e;
  lc_error_init(&e);
  io_fault f = {0};
  ps_io io = {&f,         kind == 2 ? maintenance_io_read : io_read,
              io_write,   io_sync,
              io_reserve, NULL};
  ps_pouch_state_options o = options(0, durable);
  o.logical.segment_target_bytes = 8 * PS_MIB;
  o.io = &io;
  lc_pouch *p = NULL;
  ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
  maintenance_fixture(p, &e);
  allocation = 0;
  fail_at = nth;
  f.fail = nth;
  f.partial = partial;
  if (!kind)
    __atomic_store_n(&armed, 1, __ATOMIC_RELEASE);
  else
    f.armed = 1;
  ps_pouch_compaction_result result;
  int rc = ps_pouch_state_compact(p, "projection", &result, &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  size_t total = kind ? f.calls : allocation;
  if (nth == SIZE_MAX)
    ok(rc, &e);
  else {
    REQUIRE(total >= nth);
    REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM || rc == LC_ERR_TRANSPORT);
  }
  if (rc == LC_OK)
    REQUIRE(result.records == 5 && result.snapshot && !result.skipped);
  clear(&e);
  lc_pouch_close(p);
  REQUIRE(live == 0);
  ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
  maintenance_readback(p, &e);
  /* Retry reconciles either the previous or a newly committed topology. */
  ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
  maintenance_readback(p, &e);
  lc_pouch_close(p);
  REQUIRE(live == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
  return total;
}
static void maintenance_faults(const char *path, int io) {
  size_t cases = 0;
  for (int durable = 0; durable < 2; ++durable)
    for (int kind = io ? 1 : 0; kind <= (io ? 2 : 0); ++kind)
      for (int partial = 0; partial <= (io ? 1 : 0); ++partial) {
        size_t count =
            maintenance_failure(path, SIZE_MAX, kind, durable, partial);
        for (size_t nth = 1; nth <= count; ++nth)
          (void)maintenance_failure(path, nth, kind, durable, partial);
        cases += count;
      }
  printf("maintenance %s failure sites: %lu\n", io ? "I/O" : "allocation",
         (unsigned long)cases);
}
typedef struct maintenance_hold {
  io_fault io;
  int armed, ready, release, done;
  lc_pouch *pouch;
  ps_pouch_compaction_result result;
} maintenance_hold;
static ssize_t maintenance_held_read(void *context, int fd, void *p, size_t n,
                                     off_t at) {
  maintenance_hold *h = context;
  ssize_t got = pread(fd, p, n, at);
  if (got >= 256 && __atomic_load_n(&h->armed, __ATOMIC_ACQUIRE)) {
    size_t i;
    for (i = 0; i < 256; ++i)
      if (((unsigned char *)p)[i] != byte_at(i, 3))
        break;
    if (i == 256 && __atomic_exchange_n(&h->armed, 0, __ATOMIC_ACQ_REL)) {
      __atomic_store_n(&h->ready, 1, __ATOMIC_RELEASE);
      while (!__atomic_load_n(&h->release, __ATOMIC_ACQUIRE))
        sched_yield();
    }
  }
  return got;
}
static void *maintenance_worker(void *context) {
  maintenance_hold *h = context;
  lc_error e;
  lc_error_init(&e);
  ok(ps_pouch_state_compact(h->pouch, "projection", &h->result, &e), &e);
  lc_error_cleanup(&e);
  __atomic_store_n(&h->done, 1, __ATOMIC_RELEASE);
  return NULL;
}
static void maintenance_tail_readback(lc_pouch *p, lc_error *e) {
  lc_pouch_state_read_result r = {0};
  lc_pouch_generation seq;
  read_value(p, "projection", "a-state", 65569, 3, 2, e);
  read_value(p, "projection", "z-object", 65569, 2, 1, e);
  read_value(p, "peer", "live", 31, 9, 1, e);
  ok(lc_pouch_state_read_metadata(p, "projection", "a-state", &r, e), e);
  REQUIRE(r.index_seq == 9 && r.metadata_length == 4 &&
          !memcmp(r.metadata, "tail", 4));
  lc_pouch_state_read_result_cleanup(&allocator, &r);
  ok(lc_pouch_state_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 9);
  ok(lc_pouch_state_query_index_seq(p, "projection", &seq, e), e);
  REQUIRE(seq == 8);
}
static void maintenance_concurrency(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    maintenance_hold h = {0};
    ps_io io = {&h, maintenance_held_read, io_write, io_sync, io_reserve, NULL};
    ps_pouch_state_options o = options(0, sync);
    o.logical.segment_target_bytes = 8 * PS_MIB;
    o.io = &io;
    lc_pouch *p = NULL;
    ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
    maintenance_fixture(p, &e);
    h.pouch = p;
    __atomic_store_n(&h.armed, 1, __ATOMIC_RELEASE);
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, maintenance_worker, &h) == 0);
    while (!__atomic_load_n(&h.ready, __ATOMIC_ACQUIRE) &&
           !__atomic_load_n(&h.done, __ATOMIC_ACQUIRE))
      sched_yield();
    REQUIRE(__atomic_load_n(&h.ready, __ATOMIC_ACQUIRE));
    /* A tail metadata update still references the captured original segment.
     * Publication must recheck that tail before retiring the copied source. */
    projection_meta(p, "a-state", "tail", 0, 9, &e);
    input in = source(31, 9);
    lc_pouch_state_write_result w = {0};
    ok(put(p, "peer", "live", &in, NULL, &w, &e), &e);
    lc_pouch_state_write_result_cleanup(&allocator, &w);
    maintenance_tail_readback(p, &e);
    __atomic_store_n(&h.release, 1, __ATOMIC_RELEASE);
    REQUIRE(pthread_join(thread, NULL) == 0);
    maintenance_tail_readback(p, &e);
    void *ns = NULL;
    ps_namespace_status status;
    ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0, &ns,
                                      &e),
       &e);
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.bound_segments == 2 && status.bound_bodies == 1 &&
            status.bound_snapshots == 1);
    p->storage.ops->namespace_close(p->storage.context, ns);
    ps_pouch_compaction_result result;
    ok(ps_pouch_state_compact(p, "projection", &result, &e), &e);
    REQUIRE(result.copied_bytes == 65569 && result.boundary == 2);
    maintenance_tail_readback(p, &e);
    one_file(directory);
    lc_pouch_close(p);
    REQUIRE(live == 0);
    ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
    maintenance_tail_readback(p, &e);
    lc_pouch_close(p);
    REQUIRE(live == 0 && unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
typedef struct checksum_span {
  lc_pouch_state_payload_span span;
  char leaf[40];
} checksum_span;
static int checksum_capture(void *context, const lc_pouch_state_entry *r,
                            lc_error *e) {
  checksum_span *s = context;
  (void)e;
  if (r->key && !strcmp(r->key, "a-state")) {
    REQUIRE(r->found && r->payload_span.present &&
            strlen(r->payload_span.container_leaf) < sizeof(s->leaf));
    s->span = r->payload_span;
    strcpy(s->leaf, r->payload_span.container_leaf);
    s->span.container_leaf = s->leaf;
  }
  return LC_OK;
}
typedef struct checksum_io {
  io_fault io;
  int corrupt;
} checksum_io;
static ssize_t checksum_read(void *context, int fd, void *p, size_t n,
                             off_t at) {
  checksum_io *f = context;
  ssize_t got = io_read(&f->io, fd, p, n, at);
  if (got > 0 && f->corrupt) {
    ((unsigned char *)p)[0] ^= 1;
    f->corrupt = 0;
  }
  return got;
}
static void checksum_drain(lc_source *s, uint64_t length, int mismatch,
                           lc_error *e) {
  unsigned char bytes[997];
  uint64_t count = 0;
  size_t n;
  while ((n = s->read(s, bytes, sizeof(bytes), e)) != 0)
    count += n;
  if (mismatch) {
    REQUIRE(e->code == LC_ERR_PROTOCOL && count < length);
    clear(e);
    REQUIRE(!s->read(s, bytes, sizeof(bytes), e) && e->code == LC_ERR_PROTOCOL);
    clear(e);
  } else {
    ok(e->code, e);
    REQUIRE(count == length);
  }
}
static void maintenance_checksums(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int relocated = 0; relocated < 2; ++relocated) {
      checksum_io f = {0};
      ps_io io = {&f, checksum_read, io_write, io_sync, io_reserve, NULL};
      ps_pouch_state_options o = options(crypto, 0);
      o.logical.segment_target_bytes = 8 * PS_MIB;
      o.io = &io;
      lc_pouch *p = NULL;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      maintenance_fixture(p, &e);
      if (relocated)
        ok(ps_pouch_state_compact(p, "projection", NULL, &e), &e);
      void *ns = NULL;
      ps_namespace_view *view = NULL;
      lc_pouch_state_snapshot *capture = NULL;
      checksum_span span = {0};
      ok(p->storage.ops->namespace_open(p->storage.context, "projection", 0,
                                        &ns, &e),
         &e);
      ok(ps_namespace_view_open(ns, &view, &e), &e);
      ok(lc_pouch_state_snapshot_capture(p, replay_snapshot, view, &capture,
                                         &e),
         &e);
      ok(lc_pouch_state_snapshot_visit(capture, checksum_capture, &span, &e),
         &e);
      lc_pouch_state_snapshot_close(capture);
      ps_namespace_view_close(view);
      REQUIRE(
          span.span.present && span.span.payload_length &&
          !strncmp(span.leaf, relocated ? "body-" : "seg-", relocated ? 5 : 4));
      lc_source *encoded = NULL, *clone = NULL, *wrong = NULL;
      size_t before = f.io.read_bytes;
      ok(p->storage.ops->source_open(
             p->storage.context, ns, span.leaf, span.span.payload_offset,
             span.span.payload_length, span.span.payload_crc, &encoded, &e),
         &e);
      REQUIRE(f.io.read_bytes == before);
      f.corrupt = 1;
      checksum_drain(encoded, span.span.payload_length, 1, &e);
      before = f.io.read_bytes;
      ok(p->storage.ops->source_clone(p->storage.context, encoded, &clone, &e),
         &e);
      REQUIRE(f.io.read_bytes == before);
      checksum_drain(clone, span.span.payload_length, 0, &e);
      ok(encoded->reset(encoded, &e), &e);
      checksum_drain(encoded, span.span.payload_length, 0, &e);
      ok(p->storage.ops->source_open(
             p->storage.context, ns, span.leaf, span.span.payload_offset,
             span.span.payload_length, span.span.payload_crc ^ 1, &wrong, &e),
         &e);
      checksum_drain(wrong, span.span.payload_length, 1, &e);
      wrong->close(wrong);
      wrong = NULL;
      REQUIRE(p->storage.ops->source_open(p->storage.context, ns, span.leaf,
                                          span.span.payload_offset, 0, 1,
                                          &wrong, &e) == LC_ERR_PROTOCOL &&
              !wrong);
      clear(&e);
      ok(encoded->reset(encoded, &e), &e);
      ok(clone->reset(clone, &e), &e);
      p->storage.ops->namespace_close(p->storage.context, ns);
      lc_pouch_close(p);
      checksum_drain(encoded, span.span.payload_length, 0, &e);
      checksum_drain(clone, span.span.payload_length, 0, &e);
      encoded->close(encoded);
      clone->close(clone);
      REQUIRE(live == 0 && unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
static void staged_open(const char *path, int multifile, int create,
                        ps_pouch_state_options *o, lc_pouch **p, lc_error *e) {
  if (multifile)
    ok(lc_pouch_open(path, &allocator, &o->logical, p, e), e);
  else
    ok(ps_pouch_state_open(path, create, &allocator, o, p, e), e);
}
static int staged_prepare(const lc_pouch_state_metadata_view *committed,
                          const lc_pouch_state_metadata_view *staged,
                          const lc_pouch_state_metadata_view *lease,
                          void *context, lc_pouch_state_write_options *o,
                          int *apply, lc_error *e) {
  (void)committed;
  (void)staged;
  (void)lease;
  (void)o;
  (void)e;
  *apply = context == NULL;
  return LC_OK;
}
/* Deliberately publish only DECISION, as in an interrupted native batch. The
 * fixture is closed immediately so no resident projection can mask it. */
static lc_pouch_generation staged_decision(lc_pouch *p, unsigned char kind,
                                           lc_error *e) {
  const char *key = "key/.staging/attempt";
  unsigned char meta[39] = {0};
  lc_pouch_record_io io;
  lc_pouch_storage_append append = {0};
  lc_pouch_namespace_manifest manifest = {0};
  void *ns = NULL;
  lc_pouch_generation index;
  int fd = -1;
  ok(lc_pouch_state_index_seq(p, "staged", &index, e), e);
  ++index;
  meta[0] = 2;
  meta[18] = kind;
  memcpy(meta + 27, "ISCL", 4); /* Little-endian LCSI. */
  for (size_t i = 0; i < 8; ++i)
    meta[31 + i] = (unsigned char)(index >> (8 * i));
  if (p->storage.ops) {
    ok(p->storage.ops->namespace_open(p->storage.context, "staged", 0, &ns, e),
       e);
    ok(p->storage.ops->append_begin(
           p->storage.context, ns, 1, 32 + strlen(key) + sizeof(meta),
           LC_POUCH_STORAGE_APPEND_COMPLETE, &append, e),
       e);
    io = append.io;
  } else {
    char path[1800];
    ok(lc_pouch_namespace_manifest_open(&allocator, p->root_path, "staged",
                                        &manifest, NULL, NULL, e),
       e);
    REQUIRE(snprintf(path, sizeof(path), "%s/segments/%s",
                     manifest.namespace_path,
                     manifest.active_segment) < (int)sizeof(path));
    fd = open(path, O_WRONLY | O_APPEND);
    REQUIRE(fd >= 0);
    io = lc_pouch_record_file_io(fd);
  }
  lc_pouch_record_header h = {0};
  unsigned char header_bytes[32];
  struct iovec parts[3];
  h.type = 5;
  h.key_len = strlen(key);
  h.meta_len = sizeof(meta);
  lc_pouch_record_header_encode(&h, header_bytes);
  parts[0].iov_base = header_bytes;
  parts[0].iov_len = sizeof(header_bytes);
  parts[1].iov_base = (void *)key;
  parts[1].iov_len = strlen(key);
  parts[2].iov_base = meta;
  parts[2].iov_len = sizeof(meta);
  ok(lc_pouch_record_io_append(io, parts, 3, e), e);
  if (p->storage.ops) {
    ok(p->storage.ops->append_commit(p->storage.context, &append,
                                     LC_POUCH_STORAGE_COMMIT_DEFAULT, e),
       e);
    p->storage.ops->append_close(p->storage.context, &append);
    p->storage.ops->namespace_close(p->storage.context, ns);
  } else {
    REQUIRE(fdatasync(fd) == 0 && close(fd) == 0);
    lc_pouch_namespace_manifest_cleanup(&allocator, &manifest);
  }
  return index;
}
static void staged_put(lc_pouch *p, const char *txn, size_t length,
                       unsigned char seed, lc_pouch_generation version,
                       int object, int prepared, int marker, lc_error *e) {
  input in = source(length, seed);
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result w = {0};
  o.has_logical_version = 1;
  o.logical_version = version;
  o.object_record = object;
  o.staged_delete_marker = marker;
  o.has_retention = !marker;
  o.retention = marker ? 0 : -3600;
  in.pub.impl = &in;
  if (prepared)
    ok(lc_pouch_state_stage_write_prepared(p, "staged", "key", txn, "key",
                                           &in.pub, &o, staged_prepare, NULL,
                                           NULL, &w, e),
       e);
  else
    ok(lc_pouch_state_stage_write(p, "staged", "key", txn, &in.pub, &o, &w, e),
       e);
  REQUIRE(w.version == version && in.offset == length);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
}
static void staged_absent(lc_pouch *p, const char *key, lc_error *e) {
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(p, "staged", key, &r, e), e);
  REQUIRE(!r.found);
  lc_pouch_state_read_result_cleanup(&allocator, &r);
}
static void staging(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int sync = 0; sync < 2; ++sync)
        for (int object = 0; object < 2; ++object) {
          char native[1400];
          ps_pouch_state_options o = options(crypto, sync);
          lc_pouch *p = NULL;
          lc_pouch_state_write_options write = {0};
          lc_pouch_state_write_result w = {0};
          lc_pouch_state_read_result r = {0}, held = {0}, stage_held = {0};
          lc_pouch_generation query, index, next;
          int discarded;
          o.logical.segment_target_bytes = 1;
          REQUIRE(snprintf(native, sizeof(native), "%s/staged-%d-%d-%d",
                           directory, crypto, sync,
                           object) < (int)sizeof(native));
          const char *root = multifile ? native : path;
          staged_open(root, multifile, 1, &o, &p, &e);
          write.object_record = object;
          input in = source(31, 11);
          ok(put(p, "staged", "key", &in, &write, &w, &e), &e);
          char *etag = lc_strdup_with_allocator(&allocator, w.etag);
          REQUIRE(etag);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          ok(lc_pouch_state_query_index_seq(p, "staged", &query, &e), &e);
          staged_put(p, "attempt", 65569, 12, 2, object, 1, 0, &e);
          read_value(p, "staged", "key", 31, 11, 1, &e);
          read_value(p, "staged", "key/.staging/attempt", 65569, 12, 2, &e);
          ok(lc_pouch_state_query_index_seq(p, "staged", &next, &e), &e);
          REQUIRE(next == query);
          if (!multifile) {
            REQUIRE(ps_pouch_state_compact(p, "staged", NULL, &e) ==
                    LC_ERR_UNSUPPORTED);
            clear(&e);
          }
          REQUIRE(lc_pouch_state_promote_staged(p, "staged", "key", "attempt",
                                                "wrong", &w,
                                                &e) == LC_ERR_INVALID);
          clear(&e);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          /* A stage with no decision survives repeated cold recovery. */
          for (int cold = 0; cold < 2; ++cold) {
            ok(lc_pouch_state_index_seq(p, "staged", &index, &e), &e);
            lc_pouch_close(p);
            REQUIRE(live == 1); /* The copied precondition is caller-owned. */
            staged_open(root, multifile, 0, &o, &p, &e);
            ok(lc_pouch_state_recover_staged_decisions(p, "staged", &e), &e);
            ok(lc_pouch_state_index_seq(p, "staged", &next, &e), &e);
            REQUIRE(next == index);
            read_value(p, "staged", "key/.staging/attempt", 65569, 12, 2, &e);
          }
          ok(lc_pouch_state_read(p, "staged", "key", &held, &e), &e);
          ok(lc_pouch_state_read(p, "staged", "key/.staging/attempt",
                                 &stage_held, &e),
             &e);
          ok(lc_pouch_state_promote_staged(p, "staged", "key", "attempt", etag,
                                           &w, &e),
             &e);
          REQUIRE(w.version == 2 && w.retention > w.updated_at_unix &&
                  w.retention <= w.updated_at_unix + 3600);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          lc_free_with_allocator(&allocator, etag);
          read_value(p, "staged", "key", 65569, 12, 2, &e);
          staged_absent(p, "key/.staging/attempt", &e);
          lc_pouch_close(p);
          drain(held.body, 31, 11, &e);
          drain(stage_held.body, 65569, 12, &e);
          lc_pouch_state_read_result_cleanup(&allocator, &held);
          lc_pouch_state_read_result_cleanup(&allocator, &stage_held);
          REQUIRE(!live);
          staged_open(root, multifile, 0, &o, &p, &e);
          sequence_recovery_compact(p, "staged", multifile, &e);
          read_value(p, "staged", "key", 65569, 12, 2, &e);
          /* Decisions from a previous key lifetime cannot retire a new one.
           * Use another completed attempt without compacting its decision. */
          staged_put(p, "reuse", 31, 13, 3, object, 0, 0, &e);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "reuse", &w, &e),
             &e);
          REQUIRE(w.version == 3);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          staged_put(p, "reuse", 31, 14, 4, object, 0, 0, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          staged_open(root, multifile, 0, &o, &p, &e);
          ok(lc_pouch_state_recover_staged_decisions(p, "staged", &e), &e);
          read_value(p, "staged", "key/.staging/reuse", 31, 14, 4, &e);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "reuse", &w, &e),
             &e);
          REQUIRE(w.version == 4);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "reuse", &w, &e),
             &e);
          REQUIRE(!w.version && !w.etag);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          staged_put(p, "discard", 31, 15, 5, object, 0, 0, &e);
          for (int repeat = 0; repeat < 2; ++repeat) {
            ok(lc_pouch_state_discard_staged(p, "staged", "key", "discard",
                                             &discarded, &e),
               &e);
            REQUIRE(discarded == !repeat);
          }
          read_value(p, "staged", "key", 31, 14, 4, &e);
          staged_put(p, "delete", 0, 0, 5, object, 0, 1, &e);
          ok(lc_pouch_state_read_metadata(p, "staged", "key/.staging/delete",
                                          &r, &e),
             &e);
          REQUIRE(r.found && r.staged_delete_marker);
          lc_pouch_state_read_result_cleanup(&allocator, &r);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "delete", &w, &e),
             &e);
          REQUIRE(w.version == 5);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          staged_absent(p, "key", &e);
          ok(lc_pouch_state_query_index_seq(p, "staged", &query, &e), &e);
          staged_put(p, "delete-absent", 0, 0, 1, object, 0, 1, &e);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "delete-absent",
                                          &w, &e),
             &e);
          REQUIRE(!w.version);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          ok(lc_pouch_state_query_index_seq(p, "staged", &next, &e), &e);
          REQUIRE(next == query);
          /* Prepared META must preserve absence instead of inventing a body. */
          if (!object) {
            lc_pouch_state_write_options meta = {0};
            meta.has_metadata = 1;
            meta.metadata = (const unsigned char *)"lease";
            meta.metadata_length = 5;
            ok(lc_pouch_state_stage_metadata_prepared(
                   p, "staged", "key", "meta", "key", &meta, staged_prepare,
                   NULL, &w, &e),
               &e);
            REQUIRE(!w.version && !w.has_body);
            lc_pouch_state_write_result_cleanup(&allocator, &w);
            ok(lc_pouch_state_commit_staged(p, "staged", "key", "meta", &w, &e),
               &e);
            REQUIRE(!w.version && !w.has_body);
            lc_pouch_state_write_result_cleanup(&allocator, &w);
            ok(lc_pouch_state_read_metadata(p, "staged", "key", &r, &e), &e);
            REQUIRE(r.found && !r.has_body && r.metadata_length == 5);
            lc_pouch_state_read_result_cleanup(&allocator, &r);
          }
          staged_put(p, "recreate", 31, 16, 1, object, 1, 0, &e);
          ok(lc_pouch_state_promote_staged(p, "staged", "key", "recreate", NULL,
                                           &w, &e),
             &e);
          REQUIRE(w.version == 1);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          sequence_recovery_compact(p, "staged", multifile, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          staged_open(root, multifile, 0, &o, &p, &e);
          read_value(p, "staged", "key", 31, 16, 1, &e);
          if (object) {
            ok(lc_pouch_state_query_index_seq(p, "staged", &next, &e), &e);
            REQUIRE(!next);
          }
          lc_pouch_close(p);
          REQUIRE(!live);
          if (!multifile)
            REQUIRE(unlink(path) == 0);
        }
  lc_error_cleanup(&e);
}
static int
staged_recovery_prepare(const lc_pouch_state_metadata_view *committed,
                        const lc_pouch_state_metadata_view *staged,
                        const lc_pouch_state_metadata_view *lease,
                        void *context, lc_pouch_state_write_options *o,
                        int *apply, lc_error *e) {
  (void)lease;
  (void)context;
  (void)o;
  (void)e;
  REQUIRE(committed->found && !staged->found);
  *apply = 0;
  return LC_OK;
}
static void staging_recovery(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int crypto = 0; crypto <= 3; crypto += 3)
      for (int sync = 0; sync < 2; ++sync)
        for (unsigned char decision = 1; decision <= 2; ++decision) {
          char native[1400];
          ps_pouch_state_options o = options(crypto, sync);
          o.logical.segment_target_bytes = 1;
          lc_pouch *p = NULL;
          lc_pouch_state_write_result w = {0};
          lc_pouch_generation query, next;
          int discarded;
          REQUIRE(snprintf(native, sizeof(native), "%s/recover-%d-%d-%d",
                           directory, crypto, sync,
                           decision) < (int)sizeof(native));
          const char *root = multifile ? native : path;
          staged_open(root, multifile, 1, &o, &p, &e);
          staged_put(p, "attempt", 31, 11, 99, 0, 0, 0, &e);
          ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w,
                                          &e),
             &e);
          lc_pouch_state_write_result_cleanup(&allocator, &w);
          /* This later lifetime has a lower public generation. Its newer
           * decision must supersede the old generation-100 decision. */
          staged_put(p, "attempt", 31, 12, 1, 0, 0, 0, &e);
          staged_put(p, "pending", 31, 13, 1, 0, 0, 0, &e);
          ok(lc_pouch_state_query_index_seq(p, "staged", &query, &e), &e);
          lc_pouch_generation index = staged_decision(p, decision, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          staged_open(root, multifile, 0, &o, &p, &e);
          /* Both namespace callbacks and normal reads must enter only after
           * recovery. Alternate the first cold operation across fixtures. */
          if (decision == 2) {
            lc_pouch_state_write_options meta = {0};
            ok(lc_pouch_state_stage_metadata_prepared(
                   p, "staged", "key", "attempt", "key", &meta,
                   staged_recovery_prepare, NULL, &w, &e),
               &e);
            REQUIRE(!w.version && !w.etag);
            lc_pouch_state_write_result_cleanup(&allocator, &w);
          }
          staged_absent(p, "key/.staging/attempt", &e);
          read_value(p, "staged", "key/.staging/pending", 31, 13, 1, &e);
          read_value(p, "staged", "key", 31, 11, 99, &e);
          for (int repeat = 0; repeat < 2; ++repeat) {
            ok(lc_pouch_state_recover_staged_decisions(p, "staged", &e), &e);
            ok(lc_pouch_state_index_seq(p, "staged", &next, &e), &e);
            REQUIRE(next == index + 1);
            ok(lc_pouch_state_query_index_seq(p, "staged", &next, &e), &e);
            REQUIRE(next == query);
          }
          ok(lc_pouch_state_discard_staged(p, "staged", "key", "pending",
                                           &discarded, &e),
             &e);
          REQUIRE(discarded);
          sequence_recovery_compact(p, "staged", multifile, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          staged_open(root, multifile, 0, &o, &p, &e);
          staged_absent(p, "key/.staging/attempt", &e);
          staged_absent(p, "key/.staging/pending", &e);
          read_value(p, "staged", "key", 31, 11, 99, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          if (!multifile)
            REQUIRE(unlink(path) == 0);
        }
  lc_error_cleanup(&e);
}
static size_t staging_failure(const char *path, size_t nth, int crypto,
                              int sync, int operation, int kind, int rotating) {
  io_fault f = {0};
  ps_io io = {&f, io_read, io_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(crypto, sync);
  lc_pouch *p = NULL;
  lc_error e;
  lc_error_init(&e);
  o.io = &io;
  o.logical.segment_target_bytes = rotating ? 1 : 8 * PS_MIB;
  staged_open(path, 0, 1, &o, &p, &e);
  lc_pouch_state_write_result w = {0};
  input in = source(31, 11);
  ok(put(p, "staged", "key", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  staged_put(p, "attempt", operation == 3 ? 0 : 65569, 12, 2, 0, 0,
             operation == 3, &e);
  if (operation == 4) {
    (void)staged_decision(p, 2, &e);
    lc_pouch_close(p);
    REQUIRE(!live);
    staged_open(path, 0, 0, &o, &p, &e);
  }
  lc_pouch_state_write_options write = {0};
  write.has_logical_version = 1;
  write.logical_version = 3;
  in = source(65569, 13);
  in.pub.impl = &in;
  allocation = f.calls = 0;
  fail_at = f.fail = nth;
  f.partial = kind == 2;
  f.armed = kind != 0;
  __atomic_store_n(&armed, !kind, __ATOMIC_RELEASE);
  int discarded = 0, rc;
  if (!operation)
    rc = lc_pouch_state_stage_write(p, "staged", "key", "attempt", &in.pub,
                                    &write, &w, &e);
  else if (operation == 2)
    rc = lc_pouch_state_discard_staged(p, "staged", "key", "attempt",
                                       &discarded, &e);
  else if (operation == 4)
    rc = lc_pouch_state_recover_staged_decisions(p, "staged", &e);
  else
    rc = lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  size_t count = kind ? f.calls : allocation;
  if (rc != LC_OK && rc != LC_ERR_NOMEM && !(kind && rc == LC_ERR_TRANSPORT))
    fprintf(stderr,
            "staging fault nth=%lu crypto=%d sync=%d op=%d kind=%d "
            "rc=%d message=%s\n",
            (unsigned long)nth, crypto, sync, operation, kind, rc,
            e.message ? e.message : "?");
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM ||
          (kind && rc == LC_ERR_TRANSPORT));
  if (nth == SIZE_MAX)
    ok(rc, &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  clear(&e);
  lc_pouch_close(p);
  REQUIRE(!live);
  staged_open(path, 0, 0, &o, &p, &e);
  lc_pouch_state_read_result committed = {0}, staged = {0};
  ok(lc_pouch_state_read_metadata(p, "staged", "key", &committed, &e), &e);
  ok(lc_pouch_state_read_metadata(p, "staged", "key/.staging/attempt", &staged,
                                  &e),
     &e);
  /* Every selected prefix has a coherent participant/decision pair. A failed
   * acknowledgement may select either complete state, never a half commit. */
  if (!operation) {
    REQUIRE(committed.found && committed.version == 1 && staged.found &&
            (staged.version == 2 || staged.version == 3));
    if (rc == LC_OK)
      REQUIRE(staged.version == 3);
    read_value(p, "staged", "key/.staging/attempt", 65569,
               staged.version == 3 ? 13 : 12, staged.version, &e);
  } else if (operation == 1) {
    REQUIRE(committed.found && ((committed.version == 1 && staged.found) ||
                                (committed.version == 2 && !staged.found)));
    if (rc == LC_OK)
      REQUIRE(committed.version == 2);
    read_value(p, "staged", "key", committed.version == 1 ? 31 : 65569,
               committed.version == 1 ? 11 : 12, committed.version, &e);
  } else if (operation == 2 || operation == 4) {
    REQUIRE(committed.found && committed.version == 1);
    REQUIRE(!staged.found || operation == 2);
    if (rc == LC_OK) {
      REQUIRE(!staged.found);
      if (operation == 2)
        REQUIRE(discarded);
    }
  } else {
    REQUIRE((committed.found && committed.version == 1 && staged.found &&
             staged.staged_delete_marker) ||
            (!committed.found && !staged.found));
    if (rc == LC_OK)
      REQUIRE(!committed.found);
  }
  int pending = staged.found;
  lc_pouch_generation final_version = !operation ? staged.version
                                      : operation == 2 || operation == 4 ? 1
                                                                         : 2;
  unsigned char final_seed = operation == 2 || operation == 4    ? 11
                             : !operation && staged.version == 3 ? 13
                                                                 : 12;
  lc_pouch_state_read_result_cleanup(&allocator, &committed);
  lc_pouch_state_read_result_cleanup(&allocator, &staged);
  if (pending) {
    if (operation == 2)
      ok(lc_pouch_state_discard_staged(p, "staged", "key", "attempt",
                                       &discarded, &e),
         &e);
    else
      ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, &e),
         &e);
  }
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  sequence_recovery_compact(p, "staged", 0, &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  staged_open(path, 0, 0, &o, &p, &e);
  staged_absent(p, "key/.staging/attempt", &e);
  if (operation == 3)
    staged_absent(p, "key", &e);
  else
    read_value(p, "staged", "key",
               operation == 2 || operation == 4 ? 31 : 65569, final_seed,
               final_version, &e);
  lc_pouch_close(p);
  REQUIRE(!live && unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void staging_faults(const char *path, int recovery, int rotating) {
  size_t total = 0;
  for (int crypto = 0; crypto <= 3; crypto += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int operation = recovery ? 4 : 0; operation < (recovery ? 5 : 4);
           ++operation)
        for (int kind = 0; kind < 3; ++kind) {
          size_t count = staging_failure(path, SIZE_MAX, crypto, sync,
                                         operation, kind, rotating);
          for (size_t nth = 1; nth <= count; ++nth)
            (void)staging_failure(path, nth, crypto, sync, operation, kind,
                                  rotating);
          total += count;
        }
  printf("staged publication failure sites: %lu\n", (unsigned long)total);
}
static int staged_crash_armed;
static ssize_t staged_crash_write(void *context, int fd, const void *bytes,
                                  size_t length, off_t at) {
  (void)context;
  if (staged_crash_armed && length >= 2 * LC_POUCH_RECORD_HEADER_BYTES &&
      !memcmp(bytes, "LHCP", 4)) {
    lc_pouch_record_header header;
    lc_error e;
    lc_error_init(&e);
    ok(lc_pouch_record_header_decode(bytes, &header, &e), &e);
    if (header.type == PS_FRAME_TYPE) {
      ok(lc_pouch_record_header_decode(
             (const unsigned char *)bytes + PS_FRAME_BYTES, &header, &e),
         &e);
      REQUIRE(header.type == 3);
      size_t first = PS_FRAME_BYTES + LC_POUCH_RECORD_HEADER_BYTES +
                     header.key_len + header.meta_len +
                     (size_t)header.payload_len;
      REQUIRE(first < length);
      REQUIRE(pwrite(fd, bytes, first, at) == (ssize_t)first);
      _exit(77);
    }
    lc_error_cleanup(&e);
  }
  return pwrite(fd, bytes, length, at);
}
/* The frame header and LINK are complete, but DECISION and the staged
 * tombstone have not been written. No append cleanup gets to run. */
static void staging_crash(const char *path) {
  io_fault fault = {0};
  ps_io io = {&fault, io_read, staged_crash_write, io_sync, io_reserve, NULL};
  ps_pouch_state_options o = options(0, 0);
  lc_pouch *p = NULL;
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result current = {0}, staged = {0};
  lc_error e;
  lc_error_init(&e);
  o.io = &io;
  o.logical.segment_target_bytes = 8 * PS_MIB;
  staged_open(path, 0, 1, &o, &p, &e);
  input in = source(31, 11);
  ok(put(p, "staged", "key", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  staged_put(p, "attempt", 31, 12, 2, 0, 0, 0, &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    staged_open(path, 0, 0, &o, &p, &e);
    read_value(p, "staged", "key/.staging/attempt", 31, 12, 2, &e);
    staged_crash_armed = 1;
    ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, &e), &e);
    _exit(78);
  }
  int status;
  REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
          WEXITSTATUS(status) == 77);
  staged_open(path, 0, 0, &o, &p, &e);
  ok(lc_pouch_state_read_metadata(p, "staged", "key", &current, &e), &e);
  ok(lc_pouch_state_read_metadata(p, "staged", "key/.staging/attempt", &staged,
                                  &e),
     &e);
  fprintf(stderr, "interrupted active batch: public=%llu staged=%d\n",
          (unsigned long long)current.version, staged.found);
  REQUIRE((current.version == 1 && staged.found) ||
          (current.version == 2 && !staged.found));
  REQUIRE(current.version == 1 && staged.found);
  lc_pouch_state_read_result_cleanup(&allocator, &current);
  lc_pouch_state_read_result_cleanup(&allocator, &staged);
  ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, &e), &e);
  REQUIRE(w.version == 2);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  staged_absent(p, "key/.staging/attempt", &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  staged_open(path, 0, 0, &o, &p, &e);
  read_value(p, "staged", "key", 31, 12, 2, &e);
  staged_absent(p, "key/.staging/attempt", &e);
  lc_pouch_close(p);
  REQUIRE(!live && unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct batch_crash_model {
  unsigned char *stable, *dirty, *scratch;
  size_t calls, stop;
  int armed, cut;
} batch_crash_model;
static int batch_crash_here(batch_crash_model *m) {
  return m->armed && ++m->calls == m->stop;
}
static ssize_t batch_model_read(void *context, int fd, void *p, size_t n,
                                off_t at) {
  (void)context;
  return pread(fd, p, n, at);
}
static ssize_t batch_model_write(void *context, int fd, const void *p, size_t n,
                                 off_t at) {
  batch_crash_model *m = context;
  int cut = batch_crash_here(m);
  size_t boundary = 0;
  if (m->armed && m->cut >= 3 && n > PS_FRAME_BYTES && !memcmp(p, "LHCP", 4)) {
    lc_pouch_record_header h;
    REQUIRE(lc_pouch_record_header_decode(p, &h, NULL) == LC_OK);
    if (h.type == PS_FRAME_TYPE) {
      REQUIRE(h.payload_len == n - PS_FRAME_BYTES);
      boundary = m->cut == 3 ? PS_FRAME_BYTES / 2 : PS_FRAME_BYTES;
      for (int record = 0; record < m->cut - 4; ++record) {
        REQUIRE(boundary + LC_POUCH_RECORD_HEADER_BYTES < n);
        REQUIRE(lc_pouch_record_header_decode(
                    (const unsigned char *)p + boundary, &h, NULL) == LC_OK);
        boundary += LC_POUCH_RECORD_HEADER_BYTES + h.key_len + h.meta_len +
                    (size_t)h.payload_len;
      }
      REQUIRE(boundary < n);
      cut = 1;
    }
  }
  if (cut && !m->cut)
    _exit(77);
  size_t want = boundary ? boundary : cut && m->cut == 2 && n > 1 ? n / 2 : n;
  ssize_t written = pwrite(fd, p, want, at);
  REQUIRE(written == (ssize_t)want && at >= 0 &&
          (uint64_t)at + want <= PS_DEFAULT_CAPACITY);
  if (written)
    for (size_t page = (size_t)at / PS_PAGE;
         page <= ((size_t)at + (size_t)written - 1) / PS_PAGE; ++page)
      m->dirty[page] = 1;
  if (cut)
    _exit(77);
  return written;
}
static int batch_model_sync(void *context, int fd) {
  batch_crash_model *m = context;
  int cut = batch_crash_here(m);
  if (cut && m->cut != 1)
    _exit(77);
  if (fdatasync(fd) != 0)
    return -1;
  /* Only pages written since the last successful barrier enter the model's
   * durable image. The test never copies the whole container per barrier. */
  for (size_t page = 0; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
    if (m->dirty[page]) {
      REQUIRE(pread(fd, m->scratch, PS_PAGE, (off_t)(page * PS_PAGE)) ==
              PS_PAGE);
      memcpy(m->stable + page * PS_PAGE, m->scratch, PS_PAGE);
      m->dirty[page] = 0;
    }
  if (cut)
    _exit(77);
  return 0;
}
static int batch_model_reserve(void *context, int fd, off_t at, off_t n) {
  batch_crash_model *m = context;
  int cut = batch_crash_here(m);
  if (cut && m->cut != 1)
    _exit(77);
  int rc = fallocate(fd, 0, at, n);
  if (cut) {
    REQUIRE(!rc);
    _exit(77);
  }
  return rc;
}
static void batch_crash_verify(const char *path, ps_pouch_state_options *o,
                               int operation, int acknowledged, lc_error *e) {
  lc_pouch *p = NULL;
  lc_pouch_state_write_result w = {0};
  lc_pouch_state_read_result value = {0}, staged = {0};
  o->io = NULL;
  staged_open(path, 0, 0, o, &p, e);
  ok(lc_pouch_state_read_metadata(p, "staged", "key", &value, e), e);
  ok(lc_pouch_state_read_metadata(p, "staged", "key/.staging/attempt", &staged,
                                  e),
     e);
  int complete = !staged.found;
  int valid = !operation ? value.found && value.version == (complete ? 2U : 1U)
              : operation == 1 ? value.found && value.version == 1
              : complete       ? !value.found
                               : value.found && value.version == 1;
  if (!valid || (acknowledged && o->logical.durable_sync && !complete))
    fprintf(stderr,
            "batch recovery path=%s op=%d ack=%d sync=%d found=%d version=%llu "
            "stage=%d\n",
            path, operation, acknowledged, o->logical.durable_sync, value.found,
            (unsigned long long)value.version, staged.found);
  REQUIRE(valid);
  if (!operation) {
    REQUIRE(value.found && value.version == (complete ? 2 : 1));
    read_value(p, "staged", "key", 31, complete ? 12 : 11, complete ? 2 : 1, e);
  } else if (operation == 1) {
    REQUIRE(value.found && value.version == 1);
    read_value(p, "staged", "key", 31, 11, 1, e);
  } else {
    REQUIRE(complete ? !value.found : value.found && value.version == 1);
    if (!complete)
      read_value(p, "staged", "key", 31, 11, 1, e);
  }
  if (!complete) {
    REQUIRE(staged.version == 2 &&
            staged.staged_delete_marker == (operation == 2));
    if (operation != 2)
      read_value(p, "staged", "key/.staging/attempt", 31, 12, 2, e);
  }
  if (acknowledged && o->logical.durable_sync)
    REQUIRE(complete);
  lc_pouch_state_read_result_cleanup(&allocator, &value);
  lc_pouch_state_read_result_cleanup(&allocator, &staged);
  int discarded;
  if (!complete) {
    if (operation == 1)
      ok(lc_pouch_state_discard_staged(p, "staged", "key", "attempt",
                                       &discarded, e),
         e);
    else
      ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, e), e);
  }
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  staged_absent(p, "key/.staging/attempt", e);
  sequence_recovery_compact(p, "staged", 0, e);
  lc_pouch_close(p);
  REQUIRE(!live);
  staged_open(path, 0, 0, o, &p, e);
  staged_absent(p, "key/.staging/attempt", e);
  if (operation == 2)
    staged_absent(p, "key", e);
  else
    read_value(p, "staged", "key", 31, operation ? 11 : 12, operation ? 1 : 2,
               e);
  lc_pouch_close(p);
  REQUIRE(!live);
}
static size_t batch_crash_case(const char *path, size_t stop, int cut,
                               int crypto, int sync, int rotating,
                               int operation) {
  if (stop == SIZE_MAX)
    fprintf(stderr, "batch crash baseline crypto=%d sync=%d rotate=%d op=%d\n",
            crypto, sync, rotating, operation);
  char image_path[1500];
  REQUIRE(snprintf(image_path, sizeof(image_path), "%s-stable", path) <
          (int)sizeof(image_path));
  int image = open(image_path, O_RDWR | O_CREAT | O_EXCL, 0600);
  REQUIRE(image >= 0 && ftruncate(image, PS_DEFAULT_CAPACITY) == 0);
  batch_crash_model m = {0};
  m.stable = mmap(NULL, PS_DEFAULT_CAPACITY, PROT_READ | PROT_WRITE, MAP_SHARED,
                  image, 0);
  m.dirty = calloc(PS_DEFAULT_CAPACITY / PS_PAGE, 1);
  m.scratch = malloc(PS_PAGE);
  REQUIRE(m.stable != MAP_FAILED && m.dirty && m.scratch);
  ps_io io = {&m,
              batch_model_read,
              batch_model_write,
              batch_model_sync,
              batch_model_reserve,
              NULL};
  ps_pouch_state_options o = options(crypto, sync);
  o.io = &io;
  o.initial_capacity = o.maximum_capacity = PS_DEFAULT_CAPACITY;
  o.logical.segment_target_bytes = rotating ? 1 : 8 * PS_MIB;
  lc_error e;
  lc_error_init(&e);
  lc_pouch *p = NULL;
  staged_open(path, 0, 1, &o, &p, &e);
  lc_pouch_state_write_result w = {0};
  input in = source(31, 11);
  ok(put(p, "staged", "key", &in, NULL, &w, &e), &e);
  lc_pouch_state_write_result_cleanup(&allocator, &w);
  staged_put(p, "attempt", operation == 2 ? 0 : 31, 12, 2, 0, 0, operation == 2,
             &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  int report[2];
  REQUIRE(pipe(report) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    REQUIRE(close(report[0]) == 0);
    staged_open(path, 0, 0, &o, &p, &e);
    lc_pouch_state_read_result r = {0};
    ok(lc_pouch_state_read_metadata(p, "staged", "key/.staging/attempt", &r,
                                    &e),
       &e);
    REQUIRE(r.found);
    lc_pouch_state_read_result_cleanup(&allocator, &r);
    m.calls = 0;
    m.stop = stop;
    m.cut = cut;
    m.armed = 1;
    int discarded;
    if (operation == 1) {
      ok(lc_pouch_state_discard_staged(p, "staged", "key", "attempt",
                                       &discarded, &e),
         &e);
      REQUIRE(discarded);
    } else {
      ok(lc_pouch_state_commit_staged(p, "staged", "key", "attempt", &w, &e),
         &e);
      REQUIRE(w.version == 2);
    }
    REQUIRE(write(report[1], &m.calls, sizeof(m.calls)) == sizeof(m.calls));
    _exit(0); /* Deliberately skip Pouch close and any cleanup durability. */
  }
  REQUIRE(close(report[1]) == 0);
  int status;
  REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status));
  int acknowledged = WEXITSTATUS(status) == 0;
  size_t calls = 0;
  ssize_t got = read(report[0], &calls, sizeof(calls));
  REQUIRE(close(report[0]) == 0);
  if (stop != SIZE_MAX && (acknowledged || WEXITSTATUS(status) != 77 || got))
    fprintf(stderr,
            "batch cut not reached stop=%lu cut=%d crypto=%d sync=%d rotate=%d "
            "op=%d status=%d calls=%lu\n",
            (unsigned long)stop, cut, crypto, sync, rotating, operation, status,
            (unsigned long)calls);
  REQUIRE(stop == SIZE_MAX
              ? acknowledged && got == sizeof(calls)
              : !acknowledged && WEXITSTATUS(status) == 77 && !got);
  REQUIRE(munmap(m.stable, PS_DEFAULT_CAPACITY) == 0 && close(image) == 0);
  free(m.dirty);
  free(m.scratch);
  /* The real file models process death with surviving page cache. The image
   * independently models loss of every write not covered by a successful sync.
   */
  batch_crash_verify(path, &o, operation, acknowledged, &e);
  batch_crash_verify(image_path, &o, operation, acknowledged, &e);
  REQUIRE(unlink(path) == 0 && unlink(image_path) == 0);
  lc_error_cleanup(&e);
  return calls;
}
static void staging_crash_matrix(const char *path, int crypto, int sync,
                                 int rotating) {
  size_t cases = 0, sites = 0;
  for (int operation = 0; operation < 3; ++operation) {
    size_t calls =
        batch_crash_case(path, SIZE_MAX, 0, crypto, sync, rotating, operation);
    REQUIRE(calls > 0);
    ++cases;
    sites += calls;
    for (size_t stop = 1; stop <= calls; ++stop)
      for (int cut = 0; cut < 3; ++cut) {
        (void)batch_crash_case(path, stop, cut, crypto, sync, rotating,
                               operation);
        ++cases;
      }
    /* Also cut the actual frame at its header and each inner boundary.
     * Discard has two inner records; commit/delete have three. */
    for (int cut = 3; cut <= (operation == 1 ? 5 : 6); ++cut) {
      (void)batch_crash_case(path, 0, cut, crypto, sync, rotating, operation);
      ++cases;
    }
  }
  printf("batch process-death/lost-write cases: %lu; operation sites: %lu\n",
         (unsigned long)cases, (unsigned long)sites);
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
    concurrency(r->path, r->directory, 0);
  else if (!strcmp(r->mode, "staging-concurrency"))
    concurrency(r->path, r->directory, 1);
  else if (!strcmp(r->mode, "io-faults"))
    io_faults(r->path);
  else if (!strcmp(r->mode, "crypto-init"))
    crypto_initialization(r->path);
  else if (!strcmp(r->mode, "durable-scope"))
    durable_scope(r->path, r->directory);
  else if (!strcmp(r->mode, "durable-scope-faults"))
    durable_scope_faults(r->path);
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
  else if (!strcmp(r->mode, "projection"))
    projection(r->path, r->directory);
  else if (!strcmp(r->mode, "projection-faults"))
    projection_faults(r->path);
  else if (!strcmp(r->mode, "projection-concurrency"))
    projection_concurrency(r->path, r->directory);
  else if (!strcmp(r->mode, "sequence-recovery"))
    sequence_recovery(r->path, r->directory);
  else if (!strcmp(r->mode, "lifetimes"))
    lifetimes(r->path, r->directory);
  else if (!strcmp(r->mode, "lifetime-faults"))
    lifetime_faults(r->path);
  else if (!strcmp(r->mode, "staging"))
    staging(r->path, r->directory);
  else if (!strcmp(r->mode, "staging-faults"))
    staging_faults(r->path, 0, 1);
  else if (!strcmp(r->mode, "staging-active-faults"))
    staging_faults(r->path, 0, 0);
  else if (!strcmp(r->mode, "staging-recovery-faults"))
    staging_faults(r->path, 1, 1);
  else if (!strcmp(r->mode, "staging-active-recovery-faults"))
    staging_faults(r->path, 1, 0);
  else if (!strcmp(r->mode, "staging-recovery"))
    staging_recovery(r->path, r->directory);
  else if (!strcmp(r->mode, "maintenance"))
    maintenance(r->path, r->directory);
  else if (!strcmp(r->mode, "maintenance-faults"))
    maintenance_faults(r->path, 0);
  else if (!strcmp(r->mode, "maintenance-io-faults"))
    maintenance_faults(r->path, 1);
  else if (!strcmp(r->mode, "maintenance-concurrency"))
    maintenance_concurrency(r->path, r->directory);
  else if (!strcmp(r->mode, "maintenance-checksums"))
    maintenance_checksums(r->path);
  else
    REQUIRE(0);
  return NULL;
}
int main(int argc, char **argv) {
  char directory[1200], template_path[1200], path[1300], prefix[1100];
  pthread_t thread;
  lc_error e;
  run r;
  REQUIRE(argc == 3 || argc == 6);
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
  if (!strcmp(r.mode, "durable-scope-crash"))
    durable_scope_crash(r.path);
  else if (!strcmp(r.mode, "staging-crash"))
    staging_crash(r.path);
  else if (!strcmp(r.mode, "staging-crash-matrix")) {
    REQUIRE(argc == 6 && strlen(argv[3]) == 1 && argv[3][0] >= '0' &&
            argv[3][0] <= '3' && strlen(argv[4]) == 1 && argv[4][0] >= '0' &&
            argv[4][0] <= '1' && strlen(argv[5]) == 1 && argv[5][0] >= '0' &&
            argv[5][0] <= '1');
    staging_crash_matrix(r.path, argv[3][0] - '0', argv[4][0] - '0',
                         argv[5][0] - '0');
  } else {
    REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
    REQUIRE(pthread_join(thread, NULL) == 0);
  }
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, prefix);
  REQUIRE(access(directory, F_OK) != 0);
  REQUIRE(!bounds_failed);
  printf("container production state: %lu checks\n", (unsigned long)checks);
  return 0;
}
