#define _GNU_SOURCE
#include "budget.h"
#include "catalog.h"
#include "directory.h"
#include "lc_api_internal.h"
#include "lc_pouch_record.h"
#include "owned.h"
#include "space.h"
#include "stream.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

static lc_error error;
static char path[1024], other[1024];
static size_t checks;
static size_t allocation_call, allocation_fail;
static size_t allocation_max;
static int measure_allocations;
static void fixture_put(unsigned char *, uint64_t, size_t);
typedef struct allocation_pause {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int entered, released;
} allocation_pause;
static allocation_pause *paused_allocation;

/* Linker fault seam: exercise real allocation failures without another mode in
 * the storage interface. Disabled during concurrency and error cleanup. */
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  if (measure_allocations && n > allocation_max)
    allocation_max = n;
  return allocation_fail && ++allocation_call == allocation_fail
             ? NULL
             : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t bytes) {
  if (paused_allocation && n == 1 && bytes == sizeof(void *)) {
    pthread_mutex_lock(&paused_allocation->mutex);
    paused_allocation->entered = 1;
    pthread_cond_broadcast(&paused_allocation->cond);
    while (!paused_allocation->released)
      pthread_cond_wait(&paused_allocation->cond, &paused_allocation->mutex);
    pthread_mutex_unlock(&paused_allocation->mutex);
  }
  if (measure_allocations && n * bytes > allocation_max)
    allocation_max = n * bytes;
  return allocation_fail && ++allocation_call == allocation_fail
             ? NULL
             : __real_lc_calloc_with_allocator(a, n, bytes);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  if (measure_allocations && n > allocation_max)
    allocation_max = n;
  return allocation_fail && ++allocation_call == allocation_fail
             ? NULL
             : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *s) {
  return allocation_fail && ++allocation_call == allocation_fail
             ? NULL
             : __real_lc_strdup_with_allocator(a, s);
}
#define CHECK(x)                                                               \
  do {                                                                         \
    ++checks;                                                                  \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s (%s)\n", __FILE__, __LINE__, #x,              \
              error.message ? error.message : "no error");                     \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
#define OK(x) CHECK((x) == LC_OK)
#define BAD(x)                                                                 \
  do {                                                                         \
    CHECK((x) != LC_OK);                                                       \
    lc_error_cleanup(&error);                                                  \
  } while (0)

static ps_key key(unsigned int kind, uint64_t owner, uint64_t number) {
  ps_key k = {owner, number, kind};
  return k;
}
static ps_key state(uint64_t n) { return key(PS_STATE, 1, n); }
static void clean(void) {
  CHECK(unlink(path) == 0 || errno == ENOENT);
  CHECK(unlink(other) == 0 || errno == ENOENT);
}
static ps_space *create(uint64_t capacity, int durable, const ps_io *io) {
  ps_options options = {0};
  ps_space *s = NULL;
  options.initial_capacity = capacity;
  options.durable_sync = durable;
  options.io = io;
  OK(ps_open(path, 1, &options, &s, &error));
  return s;
}
static ps_space *reopen(const ps_io *io) {
  ps_options options = {0};
  ps_space *s = NULL;
  options.io = io;
  OK(ps_open(path, 0, &options, &s, &error));
  return s;
}
static ps_ref put(ps_space *s, ps_key k, const void *bytes, size_t n,
                  size_t chunk) {
  ps_writer *w = NULL;
  ps_ref ref;
  size_t offset = 0, step;
  OK(ps_begin(s, NULL, k, &w, &error));
  while (offset < n) {
    step = n - offset;
    if (step > chunk)
      step = chunk;
    OK(ps_write(w, (const unsigned char *)bytes + offset, step, &error));
    offset += step;
  }
  OK(ps_publish(w, &ref, &error));
  BAD(ps_write(w, "x", 1, &error));
  BAD(ps_publish(w, NULL, &error));
  ps_writer_close(w);
  return ref;
}
static void read_value(ps_reader *r, const void *bytes, size_t n) {
  unsigned char buffer[317];
  size_t offset = 0, got;
  CHECK(ps_read_size(r) == n);
  do {
    OK(ps_read(r, buffer, sizeof(buffer), &got, &error));
    CHECK(got <= n - offset);
    CHECK(!got || !memcmp(buffer, (const unsigned char *)bytes + offset, got));
    offset += got;
  } while (got);
  CHECK(offset == n);
}
static void expect(ps_space *s, ps_key k, const ps_ref *ref, const void *bytes,
                   size_t n) {
  ps_reader *r = NULL;
  OK(ps_read_open(s, k, ref, &r, &error));
  read_value(r, bytes, n);
  ps_reader_close(r);
}
static void absent(ps_space *s, ps_key k) {
  ps_reader *r = NULL;
  BAD(ps_read_open(s, k, NULL, &r, &error));
  CHECK(r == NULL);
}

static void basic(void) {
  ps_space *s, *s2 = NULL;
  ps_options options = {0};
  ps_stats stats;
  struct stat st;
  ps_ref ref, changed;
  ps_reader *held = NULL;
  ps_reader *rejected = NULL;
  ps_writer *w = NULL;
  clean();
  s = create(0, 0, NULL);
  CHECK(stat(path, &st) == 0);
  CHECK(st.st_size == PS_DEFAULT_CAPACITY);
  CHECK((uint64_t)st.st_blocks * 512 >= PS_DEFAULT_CAPACITY);
  CHECK((st.st_mode & 0777) == 0600);
  BAD(ps_open(path, 1, NULL, &s2, &error));
  BAD(ps_open(path, 0, NULL, &s2, &error));
  ref = put(s, state(1), "first", 5, 1);
  OK(ps_read_open(s, state(1), &ref, &held, &error));
  changed = put(s, state(1), "second", 6, 2);
  BAD(ps_read_open(s, state(1), &ref, &rejected, &error));
  (void)changed;
  read_value(held, "first", 5);
  expect(s, state(1), NULL, "second", 6);
  expect(s, state(1), &changed, "second", 6);
  put(s, key(PS_COLLECTION, 1, 1), "collection", 10, 4);
  put(s, key(PS_STATE, 2, 1), "owner-two", 9, 4);
  expect(s, key(PS_COLLECTION, 1, 1), NULL, "collection", 10);
  OK(ps_begin(s, NULL, key(PS_COLLECTION, 1, 1), &w, &error));
  OK(ps_write(w, "overwrite", 9, &error));
  BAD(ps_publish(w, NULL, &error));
  ps_writer_close(w);
  put(s, state(2), NULL, 0, 1);
  expect(s, state(2), NULL, NULL, 0);
  BAD(ps_close(s, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.retired_bytes == PS_CELL); /* held reader still pins first */
  ps_reader_close(held);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.retired_bytes == 0);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, state(1), &changed, "second", 6);
  expect(s, state(2), NULL, NULL, 0);
  expect(s, key(PS_STATE, 2, 1), NULL, "owner-two", 9);
  OK(ps_close(s, &error));
  options.initial_capacity = PS_MIB + 1;
  BAD(ps_open(other, 1, &options, &s2, &error));
  options.initial_capacity = PS_MIB;
  options.maximum_capacity = PS_MIB;
  OK(ps_open(other, 1, &options, &s2, &error));
  BAD(ps_grow(s2, PS_MIB, &error));
  OK(ps_close(s2, &error));
  clean();
  puts("basic: preallocation, typed ownership, immutable pins and reopen");
}

static void streaming_capacity(void) {
  ps_space *s;
  ps_writer *w;
  ps_reader *held;
  ps_stats before, after;
  unsigned char *payload = malloc((size_t)PS_MIB + 123);
  size_t i;
  CHECK(payload != NULL);
  for (i = 0; i < (size_t)PS_MIB + 123; ++i)
    payload[i] = (unsigned char)(i * 37 + i / 19);
  clean();
  s = create(PS_MIB, 0, NULL);
  OK(ps_begin(s, NULL, state(99), &w, &error));
  BAD(ps_write(w, payload, (size_t)PS_MIB, &error));
  BAD(ps_publish(w, NULL, &error));
  ps_writer_close(w);
  absent(s, state(99));
  OK(ps_status(s, &before, &error));
  CHECK(before.private_bytes == 0 && before.allocated_bytes == 0);
  OK(ps_grow(s, PS_MIB, &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.capacity == 2 * PS_MIB);
  allocation_max = 0;
  measure_allocations = 1;
  put(s, state(1), payload, (size_t)PS_MIB + 123, 7);
  measure_allocations = 0;
  CHECK(allocation_max <= PS_CHUNK + 256);
  expect(s, state(1), NULL, payload, (size_t)PS_MIB + 123);
  OK(ps_read_open(s, state(1), NULL, &held, &error));
  OK(ps_delete(s, state(1), &error));
  OK(ps_status(s, &before, &error));
  CHECK(before.retired_bytes > PS_MIB);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.retired_bytes == before.retired_bytes);
  OK(ps_grow(s, PS_MIB, &error));
  put(s, state(2), payload, (size_t)PS_MIB, 8191);
  read_value(held, payload, (size_t)PS_MIB + 123);
  ps_reader_close(held);
  OK(ps_status(s, &after, &error));
  CHECK(after.retired_bytes == 0 && after.capacity == 3 * PS_MIB);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  absent(s, state(1));
  expect(s, state(2), NULL, payload, (size_t)PS_MIB);
  OK(ps_close(s, &error));
  free(payload);
  clean();
  puts("streaming: tiny chunks, capacity exhaustion, 1/2/3 MiB growth and "
       "pinned reuse");
}

/* A deterministic volatile/durable file model. Successful sync snapshots bytes;
 * crash discards every later write. Selective persistence is tested separately.
 * This does not pretend that killing a process simulates loss of page cache. */
typedef struct model {
  unsigned char *stable;
  size_t stable_capacity;
  size_t stable_bytes, operations, fail_at, short_limit, reads;
  size_t writes, syncs, grants, fail_sync_at;
  uint64_t write_bytes;
  size_t fail_read;
  size_t zero_calls;
  int unsupported_zero, interrupt_zero;
  uint64_t read_bytes;
  int tear, persist_torn, interrupt_read, interrupt_write;
  int persist_grants, fail_grant_sync;
} model;
static int fail(model *m) { return ++m->operations == m->fail_at; }
static ssize_t model_read(void *v, int fd, void *p, size_t n, off_t offset) {
  model *m = v;
  ++m->reads;
  if (m->fail_read && m->reads == m->fail_read) {
    errno = EIO;
    return -1;
  }
  if (m->interrupt_read) {
    m->interrupt_read = 0;
    errno = EINTR;
    return -1;
  }
  if (m->short_limit && n > m->short_limit)
    n = m->short_limit;
  ssize_t result = pread(fd, p, n, offset);
  if (result > 0)
    m->read_bytes += (uint64_t)result;
  return result;
}
static ssize_t model_write(void *v, int fd, const void *p, size_t n,
                           off_t offset) {
  model *m = v;
  ++m->writes;
  m->write_bytes += n;
  if (m->interrupt_write) {
    m->interrupt_write = 0;
    errno = EINTR;
    return -1;
  }
  if (fail(m)) {
    if (m->tear && n > 1) {
      CHECK(pwrite(fd, p, n / 2, offset) == (ssize_t)(n / 2));
      if (m->persist_torn && (uint64_t)offset + n / 2 <= m->stable_bytes)
        memcpy(m->stable + offset, p, n / 2);
    }
    errno = EIO;
    return -1;
  }
  if (m->short_limit && n > m->short_limit)
    n = m->short_limit;
  ssize_t result = pwrite(fd, p, n, offset);
  /* Selective persistence: an opening grant may reach stable storage even
   * when earlier cleanup writes and the following sync are lost. */
  if (result == (ssize_t)n && m->persist_grants && n == PS_TOKEN_RECORD_BYTES &&
      (offset == PS_TOKEN_RECORD_OFFSET ||
       offset == PS_PAGE + PS_TOKEN_RECORD_OFFSET)) {
    memcpy(m->stable + offset, p, n);
    ++m->grants;
  }
  return result;
}
static int model_sync(void *v, int fd) {
  model *m = v;
  struct stat st;
  ++m->syncs;
  if (fail(m) || (m->fail_sync_at && m->syncs == m->fail_sync_at) ||
      (m->fail_grant_sync && m->grants)) {
    errno = EIO;
    return -1;
  }
  CHECK(fdatasync(fd) == 0);
  CHECK(fstat(fd, &st) == 0 && st.st_size <= (off_t)m->stable_capacity);
  m->stable_bytes = (size_t)st.st_size;
  CHECK(pread(fd, m->stable, m->stable_bytes, 0) == (ssize_t)m->stable_bytes);
  return 0;
}
static int model_reserve(void *v, int fd, off_t offset, off_t n) {
  model *m = v;
  if (fail(m)) {
    errno = ENOSPC;
    return -1;
  }
  return fallocate(fd, 0, offset, n);
}
static int model_zero(void *v, int fd, off_t offset, off_t n) {
  model *m = v;
  struct stat st;
  int rc;
  ++m->zero_calls;
  if (m->interrupt_zero) {
    m->interrupt_zero = 0;
    errno = EINTR;
    return -1;
  }
  if (m->unsupported_zero) {
    errno = EOPNOTSUPP;
    return -1;
  }
  if (fail(m)) {
    if (m->tear && n > 1) {
      CHECK(fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, offset,
                      n / 2) == 0);
      if (m->persist_torn &&
          (uint64_t)offset + (uint64_t)n / 2 <= m->stable_bytes)
        memset(m->stable + offset, 0, (size_t)n / 2);
    }
    errno = EIO;
    return -1;
  }
  rc = fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, offset, n);
  if (rc == 0) {
    CHECK(fstat(fd, &st) == 0 && (uint64_t)st.st_size == m->stable_bytes);
    CHECK((uint64_t)st.st_blocks * 512 >= (uint64_t)st.st_size);
  }
  return rc;
}
static ps_io model_init_capacity(model *m, size_t capacity) {
  ps_io io = {m, model_read, model_write, model_sync, model_reserve, NULL};
  memset(m, 0, sizeof(*m));
  m->stable_capacity = capacity;
  m->stable = malloc(capacity);
  CHECK(m->stable != NULL);
  return io;
}
static ps_io model_init(model *m) {
  return model_init_capacity(m, (size_t)(4 * PS_MIB));
}
static void crash(model *m) {
  int fd = open(path, O_RDWR | O_TRUNC);
  CHECK(fd >= 0);
  CHECK(pwrite(fd, m->stable, m->stable_bytes, 0) == (ssize_t)m->stable_bytes);
  CHECK(fallocate(fd, 0, 0, (off_t)m->stable_bytes) == 0);
  CHECK(fdatasync(fd) == 0);
  CHECK(close(fd) == 0);
  m->fail_at = m->operations = 0;
}
static void fault_boundaries(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_writer *w;
  ps_reader *r;
  ps_stats stats;
  size_t phase, operation, count;
  int rc;
  unsigned char *payload = malloc(PS_CHUNK + 111);
  CHECK(payload != NULL);
  memset(payload, 'B', PS_CHUNK + 111);
  for (operation = 0; operation < 3; ++operation)
    for (phase = 1; phase <= 10; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      s = create(PS_MIB, 0, &io);
      put(s, state(1), "durable-A", 9, 9);
      OK(ps_checkpoint(s, &error));
      if (operation != 0)
        put(s, state(1), payload, PS_CHUNK + 111, 47);
      m.operations = 0;
      m.fail_at = phase;
      m.tear = 1;
      m.persist_torn = 1;
      if (operation == 0) {
        OK(ps_begin(s, NULL, state(1), &w, &error));
        rc = ps_write(w, payload, PS_CHUNK + 111, &error);
        if (rc == LC_OK)
          rc = ps_publish(w, NULL, &error);
        ps_writer_close(w);
      } else if (operation == 1)
        rc = ps_checkpoint(s, &error);
      else
        rc = ps_grow(s, PS_MIB, &error);
      if (rc != LC_OK) {
        lc_error_cleanup(&error);
        BAD(ps_delete(s, state(1), &error));
      }
      m.fail_at = 0;
      OK(ps_close(s, &error));
      crash(&m);
      s = reopen(&io);
      OK(ps_read_open(s, state(1), NULL, &r, &error));
      count = (size_t)ps_read_size(r);
      if (count == 9)
        read_value(r, "durable-A", 9);
      else {
        CHECK(count == PS_CHUNK + 111);
        read_value(r, payload, PS_CHUNK + 111);
      }
      ps_reader_close(r);
      OK(ps_status(s, &stats, &error));
      CHECK(stats.capacity == PS_MIB ||
            (operation == 2 && stats.capacity == 2 * PS_MIB));
      /* Recovery must leave the allocator usable and preserve prior data. */
      put(s, state(2), "after-crash", 11, 2);
      expect(s, state(2), NULL, "after-crash", 11);
      OK(ps_checkpoint(s, &error));
      OK(ps_close(s, &error));
    }
  free(payload);
  free(m.stable);
  clean();
  puts("faults: torn writes/sync/preallocation at publication, checkpoint and "
       "growth boundaries");
}

static void reordered_nosync(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_stats stats;
  ps_reader *r;
  unsigned char *tail = malloc(PS_JOURNAL);
  int fd;
  CHECK(tail != NULL);
  clean();
  s = create(PS_MIB, 0, &io);
  put(s, state(1), "old-durable", 11, 4);
  OK(ps_checkpoint(s, &error));
  put(s, state(1), "new-unsynced", 12, 4);
  put(s, state(2), "later-tail", 10, 2);
  fd = open(path, O_RDONLY);
  CHECK(fd >= 0);
  CHECK(pread(fd, tail, PS_JOURNAL, PS_JOURNAL_OFFSET) == PS_JOURNAL);
  CHECK(close(fd) == 0);
  OK(ps_close(s, &error));
  crash(&m);
  /* Persist metadata ahead of payload: valid checksums alone cannot publish. */
  fd = open(path, O_RDWR);
  CHECK(fd >= 0);
  CHECK(pwrite(fd, tail, PS_JOURNAL, PS_JOURNAL_OFFSET) == PS_JOURNAL);
  CHECK(close(fd) == 0);
  s = reopen(&io);
  expect(s, state(1), NULL, "old-durable", 11);
  absent(s, state(2));
  put(s, state(3), "new-journal", 11, 2);
  OK(ps_close(s, &error));
  s = reopen(&io);
  absent(s, state(2)); /* a reused sequence must not resurrect the suffix */
  expect(s, state(3), NULL, "new-journal", 11);
  OK(ps_read_open(s, state(1), NULL, &r, &error));
  OK(ps_delete(s, state(1), &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.retired_bytes == PS_CELL);
  OK(ps_checkpoint(s, &error)); /* NoSync retirement must become durable */
  ps_reader_close(r);
  put(s, state(4), "overwrites-free-space", 21, 1);
  OK(ps_close(s, &error));
  crash(&m); /* drop the new value, retain the durable retirement */
  s = reopen(&io);
  absent(s, state(1));
  expect(s, state(3), NULL, "new-journal", 11);
  OK(ps_close(s, &error));
  free(tail);
  free(m.stable);
  clean();
  puts("NoSync: metadata-before-body, discarded suffix and durable retirement "
       "before reuse");
}

static void short_io_and_corruption(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s, *foreign;
  ps_options options = {0};
  ps_ref ref;
  ps_reader *r;
  size_t n;
  unsigned char byte, buffer[16];
  int fd;
  clean();
  m.short_limit = 37;
  m.interrupt_write = 1;
  s = create(PS_MIB, 1, &io);
  ref = put(s, state(1), "checksummed", 11, 3);
  m.interrupt_read = 1;
  expect(s, state(1), &ref, "checksummed", 11);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  options.initial_capacity = PS_MIB;
  OK(ps_open(other, 1, &options, &foreign, &error));
  put(foreign, state(1), "other", 5, 1);
  r = NULL;
  BAD(ps_read_open(foreign, state(1), &ref, &r, &error));
  OK(ps_close(foreign, &error));
  m.read_bytes = 0;
  s = reopen(&io);
  CHECK(m.read_bytes < PS_DATA); /* checkpoint open does not read bodies */
  expect(s, state(1), &ref, "checksummed", 11);
  fd = open(path, O_RDWR);
  CHECK(fd >= 0);
  CHECK(pread(fd, &byte, 1, PS_DATA) == 1);
  byte ^= 1;
  CHECK(pwrite(fd, &byte, 1, PS_DATA) == 1);
  CHECK(close(fd) == 0);
  OK(ps_read_open(s, state(1), &ref, &r, &error));
  BAD(ps_read(r, buffer, sizeof(buffer), &n, &error));
  CHECK(n == 0);
  ps_reader_close(r);
  OK(ps_close(s, &error));
  /* Corrupt the selected checkpoint while leaving its root checksum intact.
   * Recovery must fail, never resurrect an obsolete root. */
  fd = open(path, O_RDWR);
  CHECK(fd >= 0);
  byte = 0xff;
  CHECK(pwrite(fd, &byte, 1, PS_METADATA_START) ==
        1); /* root 0 after explicit checkpoint */
  CHECK(close(fd) == 0);
  BAD(ps_open(path, 0, NULL, &s, &error));
  CHECK(s == NULL);
  free(m.stable);
  clean();
  puts("I/O: EINTR/short transfers, foreign refs, bounded reopen and "
       "corruption detection");
}

typedef struct worker {
  ps_space *s;
  unsigned int ordinal;
  int rc;
} worker;
static void *concurrent_writer(void *context) {
  worker *v = context;
  lc_error e;
  ps_writer *w = NULL;
  unsigned char bytes[193];
  size_t i;
  lc_error_init(&e);
  memset(bytes, (int)v->ordinal, sizeof(bytes));
  v->rc = LC_OK;
  for (i = 0; i < 40 && v->rc == LC_OK; ++i) {
    v->rc = ps_begin(v->s, NULL, key(PS_COLLECTION, v->ordinal, i), &w, &e);
    if (v->rc == LC_OK)
      v->rc = ps_write(w, bytes, sizeof(bytes), &e);
    if (v->rc == LC_OK)
      v->rc = ps_publish(w, NULL, &e);
    ps_writer_close(w);
    w = NULL;
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(void) {
  ps_space *s;
  ps_writer *slow;
  pthread_t threads[4];
  worker workers[4];
  unsigned char bytes[193];
  unsigned char *slow_bytes = malloc(PS_CHUNK + 4);
  ps_stats stats;
  size_t i, j;
  CHECK(slow_bytes != NULL);
  memset(slow_bytes, 0x53, PS_CHUNK + 4);
  clean();
  s = create(2 * PS_MIB, 0, NULL);
  /* An unfinished producer owns a private page but no admission/publication
   * lock. Independent writers, growth and checkpoint must all progress. */
  OK(ps_begin(s, NULL, state(7), &slow, &error));
  OK(ps_write(slow, slow_bytes, PS_CHUNK + 4, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.private_bytes == PS_CHUNK);
  for (i = 0; i < 4; ++i) {
    workers[i].s = s;
    workers[i].ordinal = (unsigned int)i + 1;
    CHECK(pthread_create(&threads[i], NULL, concurrent_writer, &workers[i]) ==
          0);
  }
  OK(ps_grow(s, PS_MIB, &error));
  for (i = 0; i < 4; ++i) {
    CHECK(pthread_join(threads[i], NULL) == 0);
    CHECK(workers[i].rc == LC_OK);
  }
  OK(ps_publish(slow, NULL, &error));
  ps_writer_close(slow);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, state(7), NULL, slow_bytes, PS_CHUNK + 4);
  for (i = 0; i < 4; ++i) {
    memset(bytes, (int)i + 1, sizeof(bytes));
    for (j = 0; j < 40; ++j)
      expect(s, key(PS_COLLECTION, i + 1, j), NULL, bytes, sizeof(bytes));
  }
  OK(ps_close(s, &error));
  free(slow_bytes);
  clean();
  puts("concurrency: four writers, unfinished producer, simultaneous growth "
       "and checkpoint");
}

static void churn_and_metadata_limit(void) {
  ps_space *s;
  ps_stats stats, before;
  ps_ref replacement;
  size_t i;
  clean();
  s = create(PS_MIB, 0, NULL);
  for (i = 0; i < 1800; ++i) {
    if (i % 40 == 0)
      OK(ps_checkpoint(s, &error));
    put(s, state(i % 20), &i, sizeof(i), 3);
  }
  OK(ps_status(s, &stats, &error));
  CHECK(stats.entries == 20 && stats.capacity == PS_MIB);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  for (i = 1780; i < 1800; ++i)
    expect(s, state(i % 20), NULL, &i, sizeof(i));
  OK(ps_close(s, &error));
  clean();
  s = create(PS_MIB, 0, NULL);
  for (i = 0; i < 4200; ++i)
    put(s, state(i % 7), NULL, 0, 1);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.checkpoints >= 3 && stats.entries == 7);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  for (i = 0; i < 7; ++i)
    expect(s, state(i), NULL, NULL, 0);
  OK(ps_close(s, &error));
  clean();
  s = create(10 * PS_MIB, 0, NULL);
  for (i = 0; i < 12000; ++i)
    put(s, state(i), NULL, 0, 1); /* isolate growable directory capacity */
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &before, &error));
  CHECK(before.entries == 12000 && before.metadata_bytes > 65536);
  CHECK(before.metadata_pages > PS_METADATA_RESERVE / PS_PAGE);
  replacement = put(s, state(1), NULL, 0, 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.directory_page_writes - before.directory_page_writes <= 4);
  CHECK(stats.descriptor_page_writes == before.descriptor_page_writes + 1);
  OK(ps_delete(s, state(0), &error));
  put(s, state(20000), NULL, 0, 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  absent(s, state(0));
  expect(s, state(1), &replacement, NULL, 0);
  for (i = 2; i < 12000; ++i)
    expect(s, state(i), NULL, NULL, 0);
  expect(s, state(20000), NULL, NULL, 0);
  OK(ps_close(s, &error));
  clean();
  puts("churn: repeated reuse, growable typed directory and incremental "
       "checkpoint I/O");
}

static void fragmented_allocation(void) {
  ps_space *s;
  unsigned char *page = malloc(PS_PAGE), *large = malloc(2 * PS_CHUNK);
  size_t i, pages = (PS_MIB - PS_DATA - PS_MAINTENANCE_RESERVE) / PS_PAGE;
  CHECK(page && large);
  memset(page, 0xa5, PS_PAGE);
  memset(large, 0x37, 2 * PS_CHUNK);
  clean();
  s = create(PS_MIB, 0, NULL);
  for (i = 0; i < pages; ++i)
    put(s, state(i), page, PS_PAGE, PS_PAGE);
  for (i = 0; i < pages; i += 2)
    OK(ps_delete(s, state(i), &error));
  OK(ps_checkpoint(s, &error));
  put(s, state(1000), large, 2 * PS_CHUNK, 7);
  expect(s, state(1000), NULL, large, 2 * PS_CHUNK);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  for (i = 1; i < pages; i += 2)
    expect(s, state(i), NULL, page, PS_PAGE);
  expect(s, state(1000), NULL, large, 2 * PS_CHUNK);
  OK(ps_close(s, &error));
  free(page);
  free(large);
  clean();
  puts("fragmentation: a 32 KiB input reuses isolated 4 KiB holes without "
       "false exhaustion");
}

static void process_death(void) {
  ps_space *s;
  ps_writer *w;
  pid_t child;
  int status;
  int gate[2];
  char ready;
  unsigned char *bytes = malloc(PS_CHUNK);
  CHECK(bytes != NULL);
  memset(bytes, 42, PS_CHUNK);
  clean();
  CHECK(pipe(gate) == 0);
  child = fork(); /* fork before either process opens the container */
  CHECK(child >= 0);
  if (child == 0) {
    ps_space *peer = NULL;
    CHECK(close(gate[1]) == 0);
    CHECK(read(gate[0], &ready, 1) == 1);
    _exit(ps_open(path, 0, NULL, &peer, &error) == LC_OK ? 1 : 0);
  }
  CHECK(close(gate[0]) == 0);
  s = create(PS_MIB, 1, NULL);
  put(s, state(1), "survivor", 8, 2);
  CHECK(write(gate[1], "x", 1) == 1);
  CHECK(close(gate[1]) == 0);
  CHECK(waitpid(child, &status, 0) == child);
  CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  OK(ps_close(s, &error));
  child = fork();
  CHECK(child >= 0);
  if (child == 0) {
    s = reopen(NULL);
    OK(ps_begin(s, NULL, state(2), &w, &error));
    OK(ps_write(w, bytes, PS_CHUNK, &error));
    _exit(0); /* no close: ownership must be recoverable without sidecars */
  }
  CHECK(waitpid(child, &status, 0) == child);
  CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  s = reopen(NULL);
  expect(s, state(1), NULL, "survivor", 8);
  absent(s, state(2));
  put(s, state(2), bytes, PS_CHUNK, 31);
  expect(s, state(2), NULL, bytes, PS_CHUNK);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
  puts("process death: abandoned private reservations and exclusive lock "
       "takeover");
}

static void allocation_failures(void) {
  ps_space *s;
  ps_writer *w;
  ps_reader *r;
  unsigned char *bytes = malloc(PS_CHUNK);
  size_t operation, nth;
  int rc;
  CHECK(bytes != NULL);
  memset(bytes, 17, PS_CHUNK);
  for (operation = 0; operation < 5; ++operation)
    for (nth = 1; nth <= 12; ++nth) {
      clean();
      s = create(PS_MIB, 0, NULL);
      put(s, state(1), "preserved", 9, 9);
      OK(ps_checkpoint(s, &error));
      if (operation == 1)
        put(s, state(1), "replacement", 11, 3);
      if (operation == 4)
        OK(ps_close(s, &error));
      allocation_call = 0;
      allocation_fail = nth;
      if (operation == 0) {
        w = NULL;
        rc = ps_begin(s, NULL, state(2), &w, &error);
        if (rc == LC_OK)
          rc = ps_write(w, bytes, PS_CHUNK, &error);
        if (rc == LC_OK)
          rc = ps_publish(w, NULL, &error);
        ps_writer_close(w);
      } else if (operation == 1)
        rc = ps_checkpoint(s, &error);
      else if (operation == 2) {
        r = NULL;
        rc = ps_read_open(s, state(1), NULL, &r, &error);
        ps_reader_close(r);
      } else if (operation == 3)
        rc = ps_grow(s, PS_MIB, &error);
      else
        rc = ps_open(path, 0, NULL, &s, &error);
      allocation_fail = 0;
      if (rc != LC_OK)
        lc_error_cleanup(&error);
      if (operation != 4 || rc == LC_OK)
        OK(ps_close(s, &error));
      s = reopen(NULL);
      if (operation == 1)
        expect(s, state(1), NULL, "replacement", 11);
      else
        expect(s, state(1), NULL, "preserved", 9);
      OK(ps_close(s, &error));
    }
  free(bytes);
  clean();
  puts("allocation: fail each allocation during write, reclaim, read, growth "
       "and recovery");
}

typedef struct expected_value {
  int present;
  size_t bytes;
  unsigned char value[256];
} expected_value;

static void model_churn(void) {
  expected_value *values = calloc(64, sizeof(*values));
  ps_space *s;
  ps_writer *w;
  ps_reader *held = NULL;
  unsigned char *held_value = malloc(256);
  size_t held_size = 0, step, slot, n, i;
  uint32_t random = 0x1937a52bU;
  ps_key k;
  int abort_write, rc;
  CHECK(values && held_value);
  clean();
  s = create(PS_MIB, 0, NULL);
  for (step = 0; step < 3000; ++step) {
    random = random * 1664525U + 1013904223U;
    slot = (random >> 16) % 64;
    k = key(slot % 2 ? PS_COLLECTION : PS_STATE, slot / 16, slot % 16);
    if (step % 43 == 0 || step % 17 == 0) {
      if (step % 43 == 0)
        OK(ps_checkpoint(s, &error));
      if (held) {
        read_value(held, held_value, held_size);
        ps_reader_close(held);
        held = NULL;
      }
      OK(ps_close(s, &error));
      s = reopen(NULL);
    }
    if ((random & 7U) < 4) {
      unsigned char body[256];
      n = (random >> 8) % sizeof(body);
      for (i = 0; i < n; ++i)
        body[i] = (unsigned char)(step + i * 17);
      abort_write = step % 11 == 0;
      OK(ps_begin(s, NULL, k, &w, &error));
      OK(ps_write(w, body, n, &error));
      if (!abort_write) {
        rc = ps_publish(w, NULL, &error);
        if (k.kind == PS_COLLECTION && values[slot].present) {
          CHECK(rc != LC_OK);
          lc_error_cleanup(&error);
        } else {
          CHECK(rc == LC_OK);
          values[slot].present = 1;
          values[slot].bytes = n;
          memcpy(values[slot].value, body, n);
        }
      }
      ps_writer_close(w);
    } else if ((random & 7U) < 6) {
      if (values[slot].present) {
        OK(ps_delete(s, k, &error));
        values[slot].present = 0;
      } else
        BAD(ps_delete(s, k, &error));
    } else if (values[slot].present) {
      expect(s, k, NULL, values[slot].value, values[slot].bytes);
      if (!held) {
        OK(ps_read_open(s, k, NULL, &held, &error));
        held_size = values[slot].bytes;
        memcpy(held_value, values[slot].value, held_size);
      }
    } else
      absent(s, k);
  }
  if (held) {
    read_value(held, held_value, held_size);
    ps_reader_close(held);
  }
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  for (slot = 0; slot < 64; ++slot) {
    k = key(slot % 2 ? PS_COLLECTION : PS_STATE, slot / 16, slot % 16);
    if (values[slot].present)
      expect(s, k, NULL, values[slot].value, values[slot].bytes);
    else
      absent(s, k);
  }
  OK(ps_close(s, &error));
  free(values);
  free(held_value);
  clean();
  puts("reference model: 3000 mixed typed writes, aborts, deletes, pins and "
       "checkpointed/uncheckpointed reopen cycles");
}

static void incarnation_grants(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_reader *r = NULL;
  ps_ref durable, lost, next;
  ps_options options = {0};
  ps_stats stats;
  size_t phase, i;
  int rc;
  clean();
  s = create(PS_MIB, 0, &io);
  durable = put(s, state(1), "durable", 7, 2);
  OK(ps_checkpoint(s, &error));
  lost = put(s, state(1), "lost", 4, 1);
  CHECK(lost.incarnation > durable.incarnation);
  OK(ps_close(s, &error));
  crash(&m);
  s = reopen(&io);
  expect(s, state(1), &durable, "durable", 7);
  next = put(s, state(1), "next", 4, 1);
  CHECK(next.incarnation > lost.incarnation);
  BAD(ps_read_open(s, state(1), &lost, &r, &error));
  OK(ps_close(s, &error));
  crash(&m); /* lose next as well, but the opening grant must survive */
  s = reopen(&io);
  durable = put(s, state(1), "again", 5, 1);
  CHECK(durable.incarnation > next.incarnation);
  BAD(ps_read_open(s, state(1), &next, &r, &error));
  OK(ps_close(s, &error));
  clean();

  s = create(PS_MIB, 0, &io);
  for (i = 0; i < PS_JOURNAL / 64; ++i)
    lost = put(s, state(1), NULL, 0, 1);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.journal_bytes == PS_JOURNAL);
  OK(ps_close(s, &error));
  s = reopen(&io); /* grant publication can leave a full journal intact */
  expect(s, state(1), &lost, NULL, 0);
  OK(ps_status(s, &stats, &error));
  CHECK(!stats.checkpoints && stats.journal_bytes == PS_JOURNAL);
  next = put(s, state(1), NULL, 0, 1);
  CHECK(next.incarnation > lost.incarnation);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.checkpoints == 1 && stats.journal_bytes == 64);
  OK(ps_close(s, &error));
  s = reopen(&io);
  expect(s, state(1), &next, NULL, 0);
  OK(ps_close(s, &error));
  clean();

  options.io = &io;
  for (phase = 1; phase <= 5; ++phase) {
    m.fail_at = m.operations = 0;
    s = create(PS_MIB, 0, &io);
    durable = put(s, state(1), "keep", 4, 1);
    OK(ps_checkpoint(s, &error));
    OK(ps_close(s, &error));
    m.operations = 0;
    m.fail_at = phase;
    m.tear = m.persist_torn = 1;
    rc = ps_open(path, 0, &options, &s, &error);
    m.fail_at = 0;
    if (rc == LC_OK) {
      lost = put(s, state(1), "lost", 4, 1);
      OK(ps_close(s, &error));
    } else {
      CHECK(s == NULL);
      lc_error_cleanup(&error);
      lost = durable;
    }
    crash(&m);
    s = reopen(&io);
    expect(s, state(1), &durable, "keep", 4);
    next = put(s, state(1), "new", 3, 1);
    CHECK(next.incarnation > lost.incarnation);
    OK(ps_close(s, &error));
    clean();
  }
  free(m.stable);
  puts("incarnations: durable ranges fence lost NoSync references and "
       "interrupted opens");
}

static void shared_pool_power_loss(void) {
  model m;
  ps_io io = model_init(&m);
  ps_options options = {0};
  ps_space *first = NULL, *second = NULL, *third = NULL;
  ps_reader *reader = NULL;
  ps_ref keep, lost[2], next;
  size_t syncs, writes;
  options.io = &io;
  options.shared = 1;
  options.initial_capacity = PS_MIB;
  clean();
  OK(ps_open(path, 1, &options, &first, &error));
  keep = put(first, state(1), "keep", 4, 1);
  OK(ps_flush(first, &error));
  syncs = m.syncs;
  writes = m.writes;
  OK(ps_open(path, 0, &options, &second, &error));
  OK(ps_open(path, 0, &options, &third, &error));
  CHECK(m.syncs == syncs && m.writes == writes);
  lost[0] = put(second, state(1), "lost", 4, 1);
  lost[1] = put(third, state(2), "lost", 4, 1);
  CHECK(lost[1].incarnation > lost[0].incarnation &&
        lost[0].incarnation > keep.incarnation && m.syncs == syncs);
  OK(ps_close(first, &error));
  OK(ps_close(second, &error));
  OK(ps_close(third, &error));
  crash(&m);

  OK(ps_open(path, 0, &options, &first, &error));
  expect(first, state(1), &keep, "keep", 4);
  absent(first, state(2));
  next = put(first, state(2), "next", 4, 1);
  CHECK(next.incarnation > lost[1].incarnation);
  BAD(ps_read_open(first, state(2), &lost[1], &reader, &error));
  CHECK(!reader);
  OK(ps_close(first, &error));
  free(m.stable);
  clean();
  puts("shared incarnations: pooled joins survive loss of every NoSync write");
}

/* Recovery must invalidate every rejected byte before reusing journal
 * sequences, while a clean open need only persist its incarnation grant.
 * Fixtures retain valid payloads for a stale event hidden behind a zero gap:
 * clearing just that gap would resurrect the event on the next reopen. */
static ps_ref reopen_sanitize_fixture(model *m, const ps_io *io, int durable,
                                      int layout) {
  ps_space *s;
  ps_stats first, second;
  ps_ref keep;
  unsigned char old_root[PS_ROOT_BYTES];
  unsigned char *journal = calloc(1, PS_JOURNAL);
  size_t sibling;
  int fd;
  CHECK(journal != NULL);
  clean();
  s = create(PS_MIB, durable, io);
  memcpy(old_root, m->stable + (!memcmp(m->stable, "PSROOT1", 7) ? 0 : PS_PAGE),
         sizeof(old_root));
  keep = put(s, state(1), "keep", 4, 1);
  OK(ps_checkpoint(s, &error));
  sibling = !memcmp(m->stable, "PSROOT1", 7) ? PS_PAGE : 0;
  if (layout == 2 || layout == 3 || layout == 6) {
    put(s, state(2), NULL, 0, 1);
    OK(ps_status(s, &first, &error));
    if (layout == 6)
      OK(ps_delete(s, state(1), &error));
    else
      put(s, state(3), "stale-second", 12, 1);
    OK(ps_status(s, &second, &error));
    CHECK(first.journal_bytes == 64 &&
          second.journal_bytes > first.journal_bytes &&
          second.journal_bytes < PS_JOURNAL);
    fd = open(path, O_RDONLY);
    CHECK(fd >= 0);
    CHECK(pread(fd, journal + first.journal_bytes,
                (size_t)(second.journal_bytes - first.journal_bytes),
                PS_JOURNAL_OFFSET + (off_t)first.journal_bytes) ==
          (ssize_t)(second.journal_bytes - first.journal_bytes));
    CHECK(close(fd) == 0);
    /* The stale suffix still has a valid payload and the sequence that
     * follows a new 64-byte publication at the beginning of the journal. */
    if (layout == 3)
      journal[PS_JOURNAL - 1] = 0xa5;
  } else if (layout == 1)
    journal[PS_JOURNAL - 1] = 0xa5;
  OK(ps_close(s, &error));
  fd = open(path, O_RDWR);
  CHECK(fd >= 0);
  CHECK(pwrite(fd, journal, PS_JOURNAL, PS_JOURNAL_OFFSET) == PS_JOURNAL);
  if (layout == 4)
    CHECK(pwrite(fd, old_root, sizeof(old_root), (off_t)sibling) ==
          (ssize_t)sizeof(old_root));
  else if (layout == 5) {
    memset(old_root, 0xa5, sizeof(old_root));
    CHECK(pwrite(fd, old_root, sizeof(old_root), (off_t)sibling) ==
          (ssize_t)sizeof(old_root));
  }
  CHECK(fdatasync(fd) == 0);
  CHECK(pread(fd, m->stable, m->stable_bytes, 0) == (ssize_t)m->stable_bytes);
  CHECK(close(fd) == 0);
  free(journal);
  return keep;
}
static size_t reopen_sanitize_case(int durable, int layout, size_t fail_at,
                                   int persist_torn) {
  model m;
  ps_io io = model_init(&m);
  ps_options options = {0};
  ps_space *s = NULL;
  ps_stats status;
  ps_ref keep = reopen_sanitize_fixture(&m, &io, durable, layout);
  ps_ref issued, next;
  unsigned char *journal = malloc(PS_JOURNAL);
  unsigned char sibling[PS_ROOT_BYTES];
  size_t operations;
  int fd, rc;
  CHECK(journal != NULL);
  options.io = &io;
  options.durable_sync = durable;
  m.operations = m.writes = m.syncs = 0;
  m.write_bytes = 0;
  m.fail_at = fail_at;
  m.tear = persist_torn;
  m.persist_torn = persist_torn;
  rc = ps_open(path, 0, &options, &s, &error);
  operations = m.operations;
  m.fail_at = 0;
  if (fail_at) {
    CHECK(rc == LC_ERR_TRANSPORT && !s);
    lc_error_cleanup(&error);
    crash(&m);
    OK(ps_open(path, 0, &options, &s, &error));
  } else {
    OK(rc);
    /* This is an I/O bound, not an on-disk encoding assertion. Cleanup must
     * not dirty a clean 128 KiB journal or an already retired root. */
    CHECK(m.syncs == 1);
    if (layout == 0)
      CHECK(m.writes == 1 && m.write_bytes <= PS_PAGE);
    else if (layout == 1 || layout == 4 || layout == 5)
      CHECK(m.writes <= 2 && m.write_bytes <= PS_PAGE);
    printf("reopen cleanup: durable=%d layout=%d writes=%zu bytes=%llu "
           "syncs=%zu\n",
           durable, layout, m.writes, (unsigned long long)m.write_bytes,
           m.syncs);
  }
  expect(s, state(1), &keep, "keep", 4);
  absent(s, state(2));
  absent(s, state(3));
  OK(ps_status(s, &status, &error));
  CHECK(status.capacity == PS_MIB && !status.journal_bytes);
  fd = open(path, O_RDONLY);
  CHECK(fd >= 0);
  CHECK(pread(fd, journal, PS_JOURNAL, PS_JOURNAL_OFFSET) == PS_JOURNAL);
  for (size_t i = (size_t)status.journal_bytes; i < PS_JOURNAL; ++i)
    CHECK(journal[i] == 0);
  CHECK(pread(fd, sibling, sizeof(sibling),
              !memcmp(m.stable, "PSROOT1", 7) ? PS_PAGE : 0) ==
        (ssize_t)sizeof(sibling));
  for (size_t i = 0; i < sizeof(sibling); ++i)
    CHECK(sibling[i] == 0);
  CHECK(close(fd) == 0);
  OK(ps_close(s, &error));
  crash(&m);
  OK(ps_open(path, 0, &options, &s, &error));
  expect(s, state(1), &keep, "keep", 4);
  absent(s, state(2));
  absent(s, state(3));
  issued = put(s, state(4), "issued", 6, 1);
  OK(ps_close(s, &error));
  crash(&m);
  OK(ps_open(path, 0, &options, &s, &error));
  next = put(s, state(5), "next", 4, 1);
  CHECK(next.incarnation > issued.incarnation);
  expect(s, state(1), &keep, "keep", 4);
  absent(s, state(2));
  absent(s, state(3));
  OK(ps_close(s, &error));
  free(journal);
  free(m.stable);
  clean();
  return operations;
}
static void reopen_sanitization(void) {
  size_t sites = 0;
  for (int durable = 0; durable < 2; ++durable)
    for (int layout = 0; layout < 6; ++layout) {
      size_t n = reopen_sanitize_case(durable, layout, 0, 0);
      CHECK(n >= 2);
      sites += n;
      for (size_t nth = 1; nth <= n; ++nth)
        for (int torn = 0; torn < 2; ++torn)
          reopen_sanitize_case(durable, layout, nth, torn);
    }
  /* An otherwise checked root with the former layout must fail before an
   * opener can clear its journal. The former reader rejects the new bit. */
  model m;
  ps_io io = model_init(&m);
  ps_options options = {0};
  ps_space *space = create(PS_MIB, 0, &io);
  unsigned char root[PS_ROOT_BYTES];
  int fd;
  OK(ps_close(space, &error));
  size_t offset = !memcmp(m.stable, "PSROOT1", 7) ? 0 : PS_PAGE;
  memcpy(root, m.stable + offset, sizeof(root));
  fixture_put(root + 72, PS_ROOT_LAYOUT & ~PS_ROOT_LAYOUT_FIXED_GRANTS, 4);
  fixture_put(root + 76, crc32(0, root, 76), 4);
  fd = open(path, O_RDWR);
  CHECK(fd >= 0 &&
        pwrite(fd, root, sizeof(root), (off_t)offset) == (ssize_t)sizeof(root));
  CHECK(close(fd) == 0);
  m.writes = m.syncs = 0;
  options.io = &io;
  CHECK(ps_open(path, 0, &options, &space, &error) == LC_ERR_PROTOCOL &&
        !space);
  CHECK(!m.writes && !m.syncs);
  lc_error_cleanup(&error);
  free(m.stable);
  clean();
  printf("reopen: bounded clean writes, sparse/stale tails, both sibling-root "
         "states and %zu I/O failure sites with lost/torn persistence\n",
         sites);
}

static void reopen_cleanup_ordering(void) {
  const int layouts[] = {6, 2};
  for (int durable = 0; durable < 2; ++durable)
    for (size_t layout = 0; layout < sizeof(layouts) / sizeof(*layouts);
         ++layout)
      for (int hot_retry = 0; hot_retry < 2; ++hot_retry) {
        model m;
        ps_io io = model_init(&m);
        ps_options options = {0};
        ps_space *s = NULL;
        ps_ref keep =
            reopen_sanitize_fixture(&m, &io, durable, layouts[layout]);
        options.io = &io;
        options.durable_sync = durable;
        if (hot_retry) {
          unsigned char prefix[64];
          int fd;
          /* Leave zeroed cleanup in the live file cache after a failed
           * barrier. A retry must stabilize it even if reads see zeros. */
          m.syncs = 0;
          m.fail_sync_at = 1;
          CHECK(ps_open(path, 0, &options, &s, &error) == LC_ERR_TRANSPORT &&
                !s);
          lc_error_cleanup(&error);
          m.fail_sync_at = 0;
          fd = open(path, O_RDONLY);
          CHECK(fd >= 0);
          CHECK(pread(fd, prefix, sizeof(prefix), PS_JOURNAL_OFFSET + 64) ==
                (ssize_t)sizeof(prefix));
          CHECK(close(fd) == 0);
          for (size_t i = 0; i < sizeof(prefix); ++i)
            CHECK(prefix[i] == 0);
        }
        m.grants = 0;
        m.persist_grants = m.fail_grant_sync = 1;
        CHECK(ps_open(path, 0, &options, &s, &error) == LC_ERR_TRANSPORT && !s);
        CHECK(m.grants == 1);
        lc_error_cleanup(&error);
        m.persist_grants = m.fail_grant_sync = 0;
        crash(&m);
        OK(ps_open(path, 0, &options, &s, &error));
        put(s, state(4), NULL, 0, 1);
        OK(ps_flush(s, &error));
        OK(ps_close(s, &error));
        crash(&m);
        OK(ps_open(path, 0, &options, &s, &error));
        expect(s, state(1), &keep, "keep", 4);
        absent(s, state(2));
        absent(s, state(3));
        expect(s, state(4), NULL, NULL, 0);
        OK(ps_close(s, &error));
        free(m.stable);
        clean();
      }
  puts("reopen ordering: 8 independently persisted grants preserve the "
       "journal sequence across stale PUT/DELETE and cached-zero retries");
}

typedef struct commit_gate {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int enabled, entered, released, fail_sync, publications, finished;
  int maintenance, fd, hold_payload;
  int hold_second, second_entered, second_released;
  size_t syncs;
} commit_gate;
typedef struct commit_actor {
  ps_space *space;
  commit_gate *gate;
  int ordinal, rc, maintenance, remove, same_key, repack;
  ps_ref ref;
} commit_actor;
static ssize_t gate_read(void *v, int fd, void *p, size_t n, off_t offset) {
  (void)v;
  return pread(fd, p, n, offset);
}
static ssize_t gate_write(void *v, int fd, const void *p, size_t n,
                          off_t offset) {
  commit_gate *g = v;
  ssize_t written;
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && g->hold_payload && !g->entered && offset >= PS_DATA) {
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  written = pwrite(fd, p, n, offset);
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && written == (ssize_t)n) {
    if (offset >= PS_JOURNAL_OFFSET && offset < PS_DATA && n >= 64)
      ++g->publications;
    if (offset >= 2 * PS_PAGE && offset < PS_JOURNAL_OFFSET)
      g->maintenance = 1;
    pthread_cond_broadcast(&g->cond);
  }
  pthread_mutex_unlock(&g->mutex);
  return written;
}
static int gate_sync(void *v, int fd) {
  commit_gate *g = v;
  int failed = 0;
  pthread_mutex_lock(&g->mutex);
  g->fd = fd;
  ++g->syncs;
  if (g->enabled && !g->entered) {
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
    failed = g->fail_sync;
  } else if (g->enabled && g->hold_second && !g->second_entered) {
    g->second_entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->second_released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  if (failed) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
static int gate_reserve(void *v, int fd, off_t offset, off_t bytes) {
  (void)v;
  return fallocate(fd, 0, offset, bytes);
}
static int gate_zero(void *v, int fd, off_t offset, off_t bytes) {
  commit_gate *g = v;
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && g->hold_payload && !g->entered) {
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  return fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, offset,
                   bytes);
}
static void await_gate(commit_gate *g, int phase) {
  struct timespec deadline;
  int rc = 0, ready;
  CHECK(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&g->mutex);
  for (;;) {
    ready = phase == 0   ? g->entered
            : phase == 1 ? g->publications == 4
            : phase == 2 ? g->maintenance
                         : g->second_entered;
    if (ready || rc != 0)
      break;
    rc = pthread_cond_timedwait(&g->cond, &g->mutex, &deadline);
  }
  pthread_mutex_unlock(&g->mutex);
  CHECK(ready && rc == 0);
}
static void *commit_worker(void *context) {
  commit_actor *actor = context;
  lc_error e;
  ps_writer *w = NULL;
  unsigned char body[1024];
  lc_error_init(&e);
  memset(body, 'A' + actor->ordinal, sizeof(body));
  if (actor->maintenance)
    actor->rc = actor->repack ? ps_metadata_repack(actor->space, &e)
                              : ps_checkpoint(actor->space, &e);
  else if (actor->remove)
    actor->rc = ps_delete(actor->space, state(actor->ordinal + 1), &e);
  else {
    actor->rc =
        ps_begin(actor->space, NULL,
                 state(actor->same_key ? 1 : actor->ordinal + 1), &w, &e);
    if (actor->rc == LC_OK)
      actor->rc = ps_write(w, body, sizeof(body), &e);
    if (actor->rc == LC_OK)
      actor->rc = ps_publish(w, &actor->ref, &e);
    ps_writer_close(w);
  }
  lc_error_cleanup(&e);
  pthread_mutex_lock(&actor->gate->mutex);
  ++actor->gate->finished;
  pthread_cond_broadcast(&actor->gate->cond);
  pthread_mutex_unlock(&actor->gate->mutex);
  return NULL;
}
static void commit_coordination(void) {
  commit_gate gate;
  commit_actor actors[5];
  pthread_t threads[5];
  ps_io io;
  ps_space *s;
  ps_writer *private_writer;
  ps_reader *old_reader;
  ps_reader *current_reader;
  ps_stats before, after;
  model saved;
  unsigned char *body = malloc(PS_CHUNK);
  size_t mode, i, matches;
  CHECK(body != NULL);
  memset(&saved, 0, sizeof(saved));
  saved.stable_bytes = (size_t)PS_MIB;
  saved.stable = malloc(saved.stable_bytes);
  CHECK(saved.stable != NULL);
  for (mode = 0; mode < 6; ++mode) {
    clean();
    memset(&gate, 0, sizeof(gate));
    memset(actors, 0, sizeof(actors));
    CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
    CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
    io.context = &gate;
    io.read = gate_read;
    io.write = gate_write;
    io.sync = gate_sync;
    io.reserve = gate_reserve;
    io.zero = NULL;
    s = create(PS_MIB, 1, &io);
    OK(ps_status(s, &before, &error));
    put(s, state(1), "old", 3, 1);
    OK(ps_status(s, &after, &error));
    CHECK(after.syncs ==
          before.syncs + 1); /* one writer still syncs each commit */
    if (mode == 3)
      for (i = 2; i <= 4; ++i)
        put(s, state(i), "old", 3, 1);
    OK(ps_checkpoint(s, &error));
    CHECK(pread(gate.fd, saved.stable, saved.stable_bytes, 0) ==
          (ssize_t)saved.stable_bytes);
    OK(ps_read_open(s, state(1), NULL, &old_reader, &error));
    OK(ps_status(s, &before, &error));
    gate.enabled = 1;
    gate.fail_sync = mode == 1;
    gate.hold_second = mode == 4;
    for (i = 0; i < 5; ++i) {
      actors[i].space = s;
      actors[i].gate = &gate;
      actors[i].ordinal = (int)i;
      actors[i].maintenance = i == 4;
      actors[i].repack = mode == 5;
      actors[i].remove = mode == 3;
      actors[i].same_key = mode == 4;
    }
    CHECK(pthread_create(&threads[0], NULL, commit_worker, &actors[0]) == 0);
    await_gate(&gate, 0);
    for (i = 1; i < 4; ++i)
      CHECK(pthread_create(&threads[i], NULL, commit_worker, &actors[i]) == 0);
    await_gate(&gate, 1);
    /* Reads must expose the committed prefix during a held sync, including
     * replacements, deletions, and multiple pending versions of one key. */
    memset(body, 'A', PS_CHUNK);
    expect(s, state(1), NULL, "old", 3);
    for (i = 2; i <= 4; ++i) {
      if (mode == 3)
        expect(s, state(i), NULL, "old", 3);
      else
        absent(s, state(i));
    }
    OK(ps_begin(s, NULL, state(99), &private_writer, &error));
    OK(ps_write(private_writer, body, PS_CHUNK, &error));
    ps_writer_close(private_writer);
    absent(s, state(99));
    pthread_mutex_lock(&gate.mutex);
    CHECK(gate.finished == 0); /* no durable success before the barrier */
    pthread_mutex_unlock(&gate.mutex);
    if (mode == 2 || mode == 5) {
      CHECK(pthread_create(&threads[4], NULL, commit_worker, &actors[4]) == 0);
      await_gate(&gate, 2);
    }
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.cond);
    pthread_mutex_unlock(&gate.mutex);
    if (mode == 4) {
      await_gate(&gate, 3);
      expect(s, state(1), NULL, body, 1024);
      pthread_mutex_lock(&gate.mutex);
      CHECK(gate.finished <= 1);
      gate.second_released = 1;
      pthread_cond_broadcast(&gate.cond);
      pthread_mutex_unlock(&gate.mutex);
    }
    for (i = 0; i < (mode == 2 || mode == 5 ? 5U : 4U); ++i) {
      CHECK(pthread_join(threads[i], NULL) == 0);
      CHECK(mode == 1 ? actors[i].rc != LC_OK : actors[i].rc == LC_OK);
    }
    read_value(old_reader, "old", 3);
    ps_reader_close(old_reader);
    OK(ps_status(s, &after, &error));
    if (mode == 0 || mode == 3 || mode == 4) {
      CHECK(after.sync_requests == before.sync_requests + 4);
      CHECK(after.syncs == before.syncs + 2);
      CHECK(after.max_sync_batch >= 3);
    }
    if (mode == 3) {
      for (i = 1; i <= 4; ++i)
        absent(s, state(i));
    } else if (mode == 4) {
      matches = 0;
      for (i = 0; i < 4; ++i) {
        if (ps_read_open(s, state(1), &actors[i].ref, &current_reader,
                         &error) == LC_OK) {
          ++matches;
          memset(body, 'A' + (int)i, 1024);
          read_value(current_reader, body, 1024);
          ps_reader_close(current_reader);
        } else
          lc_error_cleanup(&error);
      }
      CHECK(matches == 1);
    } else if (mode != 1) {
      for (i = 0; i < 4; ++i) {
        memset(body, 'A' + (int)i, 1024);
        expect(s, state(i + 1), NULL, body, 1024);
      }
    }
    OK(ps_close(s, &error));
    if (mode == 1)
      crash(&saved); /* failed barrier must not erase prior durable state */
    s = reopen(NULL);
    if (mode == 1) {
      expect(s, state(1), NULL, "old", 3);
      for (i = 2; i <= 4; ++i)
        absent(s, state(i));
    } else if (mode == 3) {
      for (i = 1; i <= 4; ++i)
        absent(s, state(i));
    } else if (mode == 4) {
      expect(s, state(1), NULL, body, 1024);
    } else {
      for (i = 0; i < 4; ++i) {
        memset(body, 'A' + (int)i, 1024);
        expect(s, state(i + 1), NULL, body, 1024);
      }
    }
    OK(ps_close(s, &error));
    CHECK(pthread_cond_destroy(&gate.cond) == 0);
    CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  }
  free(saved.stable);
  free(body);
  clean();
  puts("commit coordination: reader/writer progress, grouped durability, "
       "failure and checkpoint ordering");
}

static void reopen_retirement(void) {
  ps_space *s;
  ps_key k;
  ps_ref committed;
  ps_stats stats;
  unsigned char body[193], later[193];
  size_t kind, revision;
  memset(later, 'D', sizeof(later));
  for (kind = PS_STATE; kind <= PS_COLLECTION; ++kind) {
    clean();
    k = key((unsigned int)kind, 7, 1);
    s = create(PS_MIB, 1, NULL);
    memset(body, 'A', sizeof(body));
    put(s, k, body, sizeof(body), 17);
    OK(ps_checkpoint(s, &error));
    for (revision = 1; revision <= 2; ++revision) {
      if (kind == PS_COLLECTION)
        OK(ps_delete(s, k, &error));
      memset(body, 'A' + (int)revision, sizeof(body));
      committed = put(s, k, body, sizeof(body), 17);
    }
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, k, &committed, body, sizeof(body));
    /* These allocations must not overwrite checkpoint/journal bodies needed
     * by the next replay, even though only the newest value is logically live.
     */
    put(s, state(101), later, sizeof(later), 19);
    put(s, state(102), later, sizeof(later), 19);
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, k, &committed, body, sizeof(body));
    expect(s, state(101), NULL, later, sizeof(later));
    expect(s, state(102), NULL, later, sizeof(later));
    OK(ps_status(s, &stats, &error));
    CHECK(stats.retired_bytes >= 2 * 4 * PS_CELL);
    OK(ps_checkpoint(s, &error));
    OK(ps_status(s, &stats, &error));
    CHECK(stats.retired_bytes == 0);
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, k, &committed, body, sizeof(body));
    expect(s, state(101), NULL, later, sizeof(later));
    expect(s, state(102), NULL, later, sizeof(later));
    OK(ps_close(s, &error));
  }
  clean();
  puts("reopen: retain replay-dependent retired bodies until durable "
       "checkpoint");
}

static void metadata_lifecycle(void) {
  ps_space *s;
  ps_reader *held;
  ps_key collection = key(PS_COLLECTION, 1, 1);
  ps_stats stats;
  unsigned char *large = malloc(3 * (size_t)PS_MIB + 7);
  size_t i;
  CHECK(large != NULL);
  memset(large, 0x8b, 3 * (size_t)PS_MIB + 7);
  clean();
  s = create(10 * PS_MIB, 0, NULL);
  put(s, state(1), large, 3 * (size_t)PS_MIB + 7, 37);
  for (i = 2; i < 18; ++i)
    put(s, state(i), "small", 5, 3);
  OK(ps_checkpoint(s, &error));
  OK(ps_read_open(s, state(1), NULL, &held, &error));
  OK(ps_delete(s, state(1), &error));
  OK(ps_checkpoint(s, &error));
  put(s, state(99), large, 3 * (size_t)PS_MIB + 7, 4093);
  OK(ps_checkpoint(s, &error));
  read_value(held, large, 3 * (size_t)PS_MIB + 7);
  ps_reader_close(held);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  absent(s, state(1));
  expect(s, state(99), NULL, large, 3 * (size_t)PS_MIB + 7);
  for (i = 2; i < 18; ++i) {
    expect(s, state(i), NULL, "small", 5);
    OK(ps_delete(s, state(i), &error));
  }
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, state(99), NULL, large, 3 * (size_t)PS_MIB + 7);
  OK(ps_close(s, &error));
  clean();
  s = create(PS_MIB, 0, NULL);
  put(s, collection, NULL, 0, 1);
  OK(ps_delete(s, collection, &error));
  for (i = 2; i < PS_JOURNAL / 64; ++i)
    put(s, state(1), NULL, 0, 1);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.journal_bytes == PS_JOURNAL);
  put(s, collection, "rebound", 7, 3);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, collection, NULL, "rebound", 7);
  OK(ps_close(s, &error));
  free(large);
  clean();
  puts("metadata: spanning packed records, shared-page retirement and rollover "
       "rebinding");
}
static void metadata_faults(int repack) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_reader *r;
  unsigned char actual[16];
  size_t kind, phase, count, i, n;
  int rc, resumed;
  for (kind = 0; kind < 3U; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.operations = m.fail_at = m.fail_read = 0;
      resumed = 0;
      s = create(3 * PS_MIB, 0, &io);
      for (i = 0; i < 512; ++i)
        put(s, state(i), "A", 1, 1);
      OK(ps_checkpoint(s, &error));
      for (i = 0; i < 16; ++i)
        put(s, state(i * 31), "B", 1, 1);
      m.operations = 0;
      m.reads = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.fail_read = kind == 2 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = repack ? ps_metadata_repack(s, &error) : ps_checkpoint(s, &error);
      allocation_fail = 0;
      m.fail_at = m.fail_read = 0;
      if (!phase)
        count = kind == 0   ? m.operations
                : kind == 1 ? allocation_call
                            : m.reads;
      if (rc != LC_OK) {
        CHECK(kind != 1 || rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
        if (kind == 1) {
          if (repack) {
            put(s, state(511), "C", 1, 1);
            OK(ps_delete(s, state(510), &error));
            resumed = 1;
          }
          OK(ps_checkpoint(s,
                           &error)); /* failed private builds are retryable */
        }
      }
      OK(ps_close(s, &error));
      crash(&m);
      s = reopen(&io);
      for (i = 0; i < 512; ++i) {
        if (resumed && i == 510) {
          absent(s, state(i));
          continue;
        }
        OK(ps_read_open(s, state(i), NULL, &r, &error));
        OK(ps_read(r, actual, sizeof(actual), &n, &error));
        if (resumed && i == 511)
          CHECK(n == 1 && actual[0] == 'C');
        else
          CHECK(n == 1 && (actual[0] == 'A' ||
                           (i % 31 == 0 && i / 31 < 16 && actual[0] == 'B')));
        ps_reader_close(r);
      }
      put(s, state(9999), "after", 5, 1);
      OK(ps_checkpoint(s, &error));
      OK(ps_close(s, &error));
    }
    CHECK(kind == 2 ? (repack ? !count : count > 0) : count > 15);
    if (repack)
      printf("metadata repack failure sites (%s): %lu\n",
             kind == 0   ? "write/sync"
             : kind == 1 ? "allocation"
                         : "read",
             (unsigned long)count);
  }
  free(m.stable);
  clean();
  printf("metadata %s: every multi-page I/O and heap failure, "
         "abort/retry and reuse\n",
         repack ? "repack" : "checkpoint");
}
static void metadata_recovery_allocations(void) {
  model m;
  ps_io io = model_init(&m);
  ps_options options = {0};
  ps_space *s;
  unsigned char *large = malloc(3 * (size_t)PS_MIB + 7);
  unsigned char *baseline = malloc(4 * (size_t)PS_MIB);
  size_t phase, count = 0;
  int rc;
  CHECK(large && baseline);
  memset(large, 0x49, 3 * (size_t)PS_MIB + 7);
  options.io = &io;
  clean();
  s = create(4 * PS_MIB, 0, &io);
  put(s, state(1), large, 3 * (size_t)PS_MIB + 7, 8191);
  put(s, state(2), "small", 5, 3);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  memcpy(baseline, m.stable, 4 * (size_t)PS_MIB);
  for (phase = 0; phase <= count; ++phase) {
    memcpy(m.stable, baseline, 4 * (size_t)PS_MIB);
    crash(&m);
    allocation_call = 0;
    allocation_fail = phase ? phase : (size_t)-1;
    rc = ps_open(path, 0, &options, &s, &error);
    allocation_fail = 0;
    if (!phase)
      count = allocation_call;
    if (rc == LC_OK)
      OK(ps_close(s, &error));
    else {
      CHECK(rc == LC_ERR_NOMEM && s == NULL);
      lc_error_cleanup(&error);
    }
    m.read_bytes = 0;
    s = reopen(&io);
    CHECK(m.read_bytes <= PS_JOURNAL + 2 * PS_ROOT_BYTES + 6 * PS_PAGE);
    expect(s, state(1), NULL, large, 3 * (size_t)PS_MIB + 7);
    expect(s, state(2), NULL, "small", 5);
    OK(ps_close(s, &error));
  }
  CHECK(count > 10);
  free(baseline);
  free(large);
  free(m.stable);
  clean();
  puts("metadata: every allocation during multi-page descriptor recovery");
}

static void metadata_repack(void) {
  const size_t entries = 180;
  ps_space *s;
  ps_stats before, after, unchanged;
  ps_reader *held = NULL;
  ps_writer *private_writer = NULL;
  unsigned char *private_bytes = malloc(PS_CHUNK);
  size_t i;
  CHECK(private_bytes != NULL);
  memset(private_bytes, 0x37, PS_CHUNK);
  BAD(ps_metadata_repack(NULL, &error));
  for (int sync = 0; sync < 2; ++sync) {
    clean();
    s = create(PS_DEFAULT_CAPACITY, sync, NULL);
    OK(ps_metadata_repack(s, &error));
    for (i = 0; i < entries; ++i)
      put(s, state(i), "A", 1, 1);
    OK(ps_checkpoint(s, &error));
    OK(ps_read_open(s, state(0), NULL, &held, &error));
    /* Keep one cold reference per initial descriptor page, then separate hot
     * descriptors into one-page checkpoints. Logical metadata stays small. */
    for (i = 0; i < entries; ++i) {
      if (i % 45 == 44)
        continue;
      put(s, state(i), "B", 1, 1);
      OK(ps_checkpoint(s, &error));
    }
    OK(ps_status(s, &before, &error));
    size_t fragmented = before.metadata_pages;
    CHECK(fragmented > entries / 2);
    /* Repack also includes dirty replacement/tombstone ownership and excludes
     * an unpublished writer, even though its payload pages are already owned.
     */
    put(s, state(0), "C", 1, 1);
    OK(ps_delete(s, state(1), &error));
    OK(ps_begin(s, NULL, state(999), &private_writer, &error));
    OK(ps_write(private_writer, private_bytes, PS_CHUNK, &error));
    OK(ps_status(s, &before, &error));
    OK(ps_metadata_repack(s, &error));
    OK(ps_status(s, &after, &error));
    CHECK(after.capacity == before.capacity && after.entries == entries - 1 &&
          !after.journal_bytes && after.live_bytes == before.live_bytes &&
          after.private_bytes == before.private_bytes &&
          after.checkpoints == before.checkpoints + 1 &&
          after.metadata_pages == (after.metadata_bytes + 4031) / 4032 +
                                      ps_directory_packed_pages(after.entries));
    CHECK(after.metadata_pages < before.metadata_pages / 4);
    read_value(held, "A", 1);
    ps_reader_close(held);
    for (i = 0; i < entries; ++i) {
      if (i == 1)
        absent(s, state(i));
      else
        expect(s, state(i), NULL, i == 0 ? "C" : i % 45 == 44 ? "A" : "B", 1);
    }
    OK(ps_publish(private_writer, NULL, &error));
    ps_writer_close(private_writer);
    expect(s, state(999), NULL, private_bytes, PS_CHUNK);
    OK(ps_checkpoint(s, &error));
    OK(ps_status(s, &unchanged, &error));
    /* An ordinary clean checkpoint still writes no directory/descriptor page.
     */
    OK(ps_checkpoint(s, &error));
    OK(ps_status(s, &before, &error));
    CHECK(before.directory_page_writes == unchanged.directory_page_writes &&
          before.descriptor_page_writes == unchanged.descriptor_page_writes);
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, state(0), NULL, "C", 1);
    absent(s, state(1));
    expect(s, state(44), NULL, "A", 1);
    expect(s, state(2), NULL, "B", 1);
    expect(s, state(999), NULL, private_bytes, PS_CHUNK);
    OK(ps_close(s, &error));
    printf("metadata repack: %lu fragmented pages to %lu dense pages\n",
           (unsigned long)fragmented, (unsigned long)after.metadata_pages);
  }
  free(private_bytes);
  clean();
}
typedef struct repack_audit {
  model model;
  uint64_t written;
} repack_audit;
static ssize_t repack_write(void *context, int fd, const void *p, size_t bytes,
                            off_t offset) {
  repack_audit *audit = context;
  ssize_t n = model_write(&audit->model, fd, p, bytes, offset);
  if (n > 0)
    audit->written += (uint64_t)n;
  return n;
}
static void metadata_repack_surfaces(void) {
  repack_audit audit;
  ps_io io = model_init(&audit.model);
  ps_space *s;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_reader *stream_pin = NULL;
  ps_reader *body_pin = NULL;
  ps_stream_info info;
  ps_catalog_view *view = NULL;
  ps_catalog_info binding, observed;
  ps_stats before, after;
  ps_key stream_key = {77, 1, PS_STREAM};
  size_t length = 3 * (size_t)PS_MIB + 7, name_bytes = PS_CHUNK + 17, got;
  unsigned char *body = malloc(length), *name = malloc(name_bytes);
  char readback[3];
  CHECK(body && name);
  memset(body, 0x49, length);
  memset(name, 0x53, name_bytes);
  io.context = &audit;
  io.write = repack_write;
  audit.written = 0;
  clean();
  s = create(4 * PS_MIB, 0, &io);
  put(s, state(1), body, length, 8191);
  OK(ps_stream_open(s, NULL, stream_key, 1, NULL, NULL, NULL, &stream, &error));
  OK(ps_stream_begin(stream, &writer, &error));
  OK(ps_stream_write(writer, "abc", 3, &error));
  OK(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  ps_stream_writer_close(writer);
  OK(ps_stream_seal(stream, &error));
  OK(ps_stream_status(stream, &info, &error));
  OK(ps_catalog_put(s, NULL, 99, PS_CATALOG_PROJECTION, name, name_bytes, NULL,
                    stream_key, &info.ref, &binding, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_read_open(s, state(1), NULL, &body_pin, &error));
  OK(ps_stream_read_open(stream, 0, 3, &stream_pin, &error));
  OK(ps_catalog_get(s, 99, PS_CATALOG_PROJECTION, name, name_bytes, &view,
                    &error));
  CHECK(view != NULL);
  OK(ps_status(s, &before, &error));
  audit.written = 0;
  audit.model.read_bytes = 0;
  allocation_max = 0;
  measure_allocations = 1;
  OK(ps_metadata_repack(s, &error));
  measure_allocations = 0;
  OK(ps_status(s, &after, &error));
  CHECK(after.live_bytes == before.live_bytes &&
        after.allocated_bytes == before.allocated_bytes &&
        after.entries == before.entries && after.capacity == before.capacity);
  CHECK(audit.model.read_bytes <=
        ps_directory_page_bound(before.entries) * PS_PAGE);
  CHECK(audit.written ==
        (after.directory_page_writes - before.directory_page_writes +
         after.descriptor_page_writes - before.descriptor_page_writes) *
                PS_PAGE +
            2 * PS_ROOT_BYTES);
  CHECK(allocation_max <= PS_CHUNK + 256);
  OK(ps_catalog_view_info(view, &observed, &error));
  CHECK(observed.id == binding.id &&
        observed.revision.incarnation == binding.revision.incarnation &&
        observed.target.incarnation == info.ref.incarnation);
  ps_catalog_view_close(view);
  read_value(body_pin, body, length);
  ps_reader_close(body_pin);
  OK(ps_stream_read(stream_pin, readback, sizeof(readback), &got, &error));
  CHECK(got == 3 && !memcmp(readback, "abc", 3));
  ps_stream_reader_close(stream_pin);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  crash(&audit.model);
  s = reopen(&io);
  expect(s, state(1), NULL, body, length);
  OK(ps_catalog_get(s, 99, PS_CATALOG_PROJECTION, name, name_bytes, &view,
                    &error));
  CHECK(view != NULL);
  OK(ps_catalog_view_info(view, &observed, &error));
  CHECK(observed.id == binding.id &&
        observed.revision.incarnation == binding.revision.incarnation);
  ps_catalog_view_close(view);
  OK(ps_stream_open(s, NULL, stream_key, 0, &info.ref, NULL, NULL, &stream,
                    &error));
  OK(ps_stream_read_open(stream, 0, 3, &stream_pin, &error));
  OK(ps_stream_read(stream_pin, readback, sizeof(readback), &got, &error));
  CHECK(got == 3 && !memcmp(readback, "abc", 3));
  ps_stream_reader_close(stream_pin);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  free(body);
  free(name);
  free(audit.model.stable);
  clean();
}
static size_t metadata_growth_fixture(ps_space *s, const unsigned char *body,
                                      int full) {
  size_t i, count;
  int rc = LC_OK;
  for (i = 0; i < 180; ++i)
    put(s, state(i), "A", 1, 1);
  OK(ps_checkpoint(s, &error));
  for (i = 0; i < 180; ++i) {
    if (i % 45 == 44)
      continue;
    put(s, state(i), "B", 1, 1);
    OK(ps_checkpoint(s, &error));
  }
  for (count = 0; count < 1024; ++count) {
    ps_writer *w = NULL;
    OK(ps_begin(s, NULL, state(1000 + count), &w, &error));
    rc = ps_write(w, body, PS_PAGE, &error);
    if (rc == LC_OK)
      rc = ps_publish(w, NULL, &error);
    ps_writer_close(w);
    if (rc != LC_OK)
      break;
  }
  CHECK(count > 100 && count < 1024 && rc == LC_ERR_INVALID && error.message &&
        strstr(error.message, "capacity"));
  lc_error_cleanup(&error);
  OK(ps_checkpoint(s, &error));
  /* A clean checkpoint needs no new metadata pages at this same capacity. */
  OK(ps_checkpoint(s, &error));
  OK(ps_metadata_repack(s, &error));
  if (!full)
    return count;
  /* Fill the space recovered from fragmented metadata too. The final dense
   * checkpoint then leaves only the normal foreground headroom, so growth
   * rollback must preserve that headroom as well as the visible capacity. */
  for (; count < 1024; ++count) {
    ps_writer *w = NULL;
    OK(ps_begin(s, NULL, state(1000 + count), &w, &error));
    rc = ps_write(w, body, PS_PAGE, &error);
    if (rc == LC_OK)
      rc = ps_publish(w, NULL, &error);
    ps_writer_close(w);
    if (rc != LC_OK)
      break;
  }
  CHECK(count < 1024 && rc == LC_ERR_INVALID && error.message &&
        strstr(error.message, "capacity"));
  lc_error_cleanup(&error);
  OK(ps_checkpoint(s, &error));
  OK(ps_metadata_repack(s, &error));
  return count;
}
static void metadata_workspace_abort(void) {
  ps_space *s;
  ps_writer *w = NULL;
  ps_stats before, enlarged, after;
  unsigned char *body = malloc(PS_CHUNK);
  size_t i;
  CHECK(body != NULL);
  memset(body, 0x51, PS_CHUNK);
  clean();
  s = create(3 * PS_MIB, 0, NULL);
  for (i = 0; i < 2000; ++i)
    put(s, state(i), "A", 1, 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &before, &error));
  OK(ps_begin(s, NULL, state(9999), &w, &error));
  for (i = 0; i < 64; ++i)
    OK(ps_write(w, body, PS_CHUNK, &error));
  OK(ps_status(s, &enlarged, &error));
  CHECK(enlarged.metadata_workspace_bytes > before.metadata_workspace_bytes &&
        enlarged.metadata_available_bytes > before.metadata_available_bytes);
  ps_writer_close(w);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.metadata_workspace_bytes == before.metadata_workspace_bytes &&
        after.metadata_available_bytes == before.metadata_available_bytes &&
        after.metadata_pages == before.metadata_pages && !after.private_bytes &&
        after.allocated_bytes == before.allocated_bytes &&
        after.directory_page_writes == before.directory_page_writes &&
        after.descriptor_page_writes == before.descriptor_page_writes);
  expect(s, state(0), NULL, "A", 1);
  expect(s, state(1999), NULL, "A", 1);
  absent(s, state(9999));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  OK(ps_status(s, &after, &error));
  CHECK(after.metadata_available_bytes >= after.metadata_workspace_bytes);
  expect(s, state(0), NULL, "A", 1);
  expect(s, state(1999), NULL, "A", 1);
  OK(ps_close(s, &error));
  free(body);
  clean();
}
static void metadata_capacity(void) {
  ps_space *s;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_reader *reader = NULL;
  ps_reader *held = NULL;
  ps_key key = {77, 1, PS_STREAM};
  ps_stats stats;
  ps_stream_info info;
  ps_catalog_info binding, observed;
  ps_catalog_view *catalog_pin = NULL;
  ps_ref original, expected;
  uint64_t fallbacks;
  unsigned char *body = malloc(PS_PAGE);
  unsigned char *name = malloc(PS_CHUNK + 17);
  unsigned char tail[33];
  size_t i, count, got, pass, filled = 0;
  int rc;
  CHECK(body && name);
  memset(body, 0x51, PS_PAGE);
  memset(name, 0x53, PS_CHUNK + 17);
  for (int sync = 0; sync < 2; ++sync) {
    clean();
    s = create(3 * PS_MIB, sync, NULL);
    OK(ps_stream_open(s, NULL, key, 1, NULL, NULL, NULL, &stream, &error));
    OK(ps_stream_begin(stream, &writer, &error));
    OK(ps_stream_reserve_exact(writer, NULL, sizeof(tail), &error));
    OK(ps_stream_write(writer, "A", 1, &error));
    OK(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &error));
    ps_stream_writer_close(writer);
    OK(ps_stream_status(stream, &info, &error));
    OK(ps_catalog_put(s, NULL, 77, PS_CATALOG_PROJECTION, name, PS_CHUNK + 17,
                      NULL, key, &info.ref, &binding, &error));
    original = binding.revision;
    OK(ps_catalog_get(s, 77, PS_CATALOG_PROJECTION, name, PS_CHUNK + 17,
                      &catalog_pin, &error));
    for (i = 0; i < 180; ++i)
      put(s, state(i), "A", 1, 1);
    OK(ps_checkpoint(s, &error));
    for (i = 0; i < 180; ++i) {
      if (i % 45 == 44)
        continue;
      put(s, state(i), "B", 1, 1);
      OK(ps_checkpoint(s, &error));
    }
    OK(ps_read_open(s, state(44), NULL, &held, &error));
    count = 0;
    for (pass = 0; pass < 2; ++pass) {
      for (; count < 1024; ++count) {
        ps_writer *value = NULL;
        OK(ps_begin(s, NULL, state(1000 + count), &value, &error));
        rc = ps_write(value, body, PS_PAGE, &error);
        if (rc == LC_OK)
          rc = ps_publish(value, NULL, &error);
        ps_writer_close(value);
        if (rc != LC_OK)
          break;
      }
      CHECK(count > 100 && count < 1024 && rc == LC_ERR_INVALID &&
            error.message && strstr(error.message, "capacity"));
      lc_error_cleanup(&error);
      if (!pass) {
        filled = count;
        OK(ps_checkpoint(s, &error));
        OK(ps_metadata_repack(s, &error));
      }
    }
    CHECK(count > filled);
    expected = binding.revision;
    OK(ps_catalog_put(s, NULL, 77, PS_CATALOG_PROJECTION, name, PS_CHUNK + 17,
                      &expected, key, &info.ref, &binding, &error));
    OK(ps_status(s, &stats, &error));
    fallbacks = stats.metadata_fallbacks;
    /* Rewriting metadata and publishing within an already owned grant must
     * remain possible after unrelated payloads have exhausted admission. */
    for (i = 0; i < sizeof(tail) - 1; ++i) {
      OK(ps_stream_begin(stream, &writer, &error));
      OK(ps_stream_write(writer, "B", 1, &error));
      OK(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &error));
      ps_stream_writer_close(writer);
      if (i % 7 == 6)
        OK(ps_metadata_repack(s, &error));
      else
        OK(ps_checkpoint(s, &error));
      OK(ps_status(s, &stats, &error));
      CHECK(stats.metadata_available_bytes >= stats.metadata_workspace_bytes);
      if (!i)
        CHECK(stats.metadata_fallbacks > fallbacks);
    }
    OK(ps_status(s, &stats, &error));
    CHECK(stats.capacity == 3 * PS_MIB && stats.metadata_fallbacks);
    read_value(held, "A", 1);
    ps_reader_close(held);
    OK(ps_catalog_view_info(catalog_pin, &observed, &error));
    CHECK(observed.id == binding.id &&
          observed.revision.incarnation == original.incarnation);
    ps_catalog_view_close(catalog_pin);
    for (i = 0; i < count; ++i)
      expect(s, state(1000 + i), NULL, body, PS_PAGE);
    OK(ps_stream_status(stream, &info, &error));
    CHECK(info.tail == sizeof(tail));
    OK(ps_stream_seal(stream, &error));
    OK(ps_checkpoint(s, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(s, &error));
    s = reopen(NULL);
    OK(ps_status(s, &stats, &error));
    CHECK(stats.metadata_available_bytes >= stats.metadata_workspace_bytes);
    OK(ps_stream_open(s, NULL, key, 0, &info.ref, NULL, NULL, &stream, &error));
    OK(ps_stream_read_open(stream, 0, sizeof(tail), &reader, &error));
    OK(ps_stream_read(reader, tail, sizeof(tail), &got, &error));
    CHECK(got == sizeof(tail) && tail[0] == 'A');
    for (i = 1; i < sizeof(tail); ++i)
      CHECK(tail[i] == 'B');
    ps_stream_reader_close(reader);
    OK(ps_stream_close(stream, &error));
    OK(ps_close(s, &error));
  }
  free(body);
  free(name);
  clean();
}
static void metadata_capacity_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_key control = {77, 1, PS_CONTROL};
  ps_stats before, after;
  unsigned char *body = malloc(PS_PAGE);
  unsigned char *baseline = malloc(3 * (size_t)PS_MIB);
  size_t count, kind, phase, sites = 0, i;
  int rc, resumed;
  CHECK(body && baseline);
  memset(body, 0x51, PS_PAGE);
  clean();
  s = create(3 * PS_MIB, 0, &io);
  put(s, control, "A", 1, 1);
  for (count = 0; count < 1024; ++count) {
    ps_writer *w = NULL;
    OK(ps_begin(s, NULL, state(1000 + count), &w, &error));
    rc = ps_write(w, body, PS_PAGE, &error);
    if (rc == LC_OK)
      rc = ps_publish(w, NULL, &error);
    ps_writer_close(w);
    if (rc != LC_OK)
      break;
  }
  CHECK(count > 100 && count < 1024 && rc == LC_ERR_INVALID && error.message &&
        strstr(error.message, "capacity"));
  lc_error_cleanup(&error);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  CHECK(m.stable_bytes == 3 * PS_MIB);
  memcpy(baseline, m.stable, 3 * (size_t)PS_MIB);
  for (kind = 0; kind < 3; ++kind) {
    sites = 0;
    for (phase = 0; phase <= sites; ++phase) {
      memcpy(m.stable, baseline, 3 * (size_t)PS_MIB);
      m.stable_bytes = 3 * PS_MIB;
      crash(&m);
      s = reopen(&io);
      put(s, control, "B", 1, 1);
      OK(ps_status(s, &before, &error));
      m.operations = m.reads = 0;
      m.tear = m.persist_torn = 1;
      m.fail_at = kind == 0 ? phase : 0;
      m.fail_read = kind == 2 ? phase : 0;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_checkpoint(s, &error);
      allocation_fail = 0;
      m.fail_at = m.fail_read = 0;
      resumed = 0;
      if (!phase) {
        OK(rc);
        sites = kind == 0   ? m.operations
                : kind == 1 ? allocation_call
                            : m.reads;
        OK(ps_status(s, &after, &error));
        CHECK(after.metadata_fallbacks == before.metadata_fallbacks + 1 &&
              after.metadata_available_bytes >=
                  after.metadata_workspace_bytes &&
              after.capacity == before.capacity);
      } else {
        CHECK(rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        lc_error_cleanup(&error);
        if (kind == 1) {
          put(s, control, "C", 1, 1);
          OK(ps_checkpoint(s, &error));
          resumed = 1;
        }
      }
      OK(ps_close(s, &error));
      crash(&m);
      s = reopen(&io);
      expect(s, control, NULL, resumed ? "C" : "B", 1);
      for (i = 0; i < count; ++i)
        expect(s, state(1000 + i), NULL, body, PS_PAGE);
      OK(ps_status(s, &after, &error));
      CHECK(after.capacity == 3 * PS_MIB &&
            after.metadata_available_bytes >= after.metadata_workspace_bytes);
      OK(ps_metadata_repack(s, &error));
      OK(ps_close(s, &error));
    }
    CHECK(kind == 2 ? !sites : sites > 0);
    printf("packed checkpoint capacity failure sites (%s): %lu\n",
           kind == 0   ? "write/sync"
           : kind == 1 ? "allocation"
                       : "read",
           (unsigned long)sites);
  }
  free(body);
  free(baseline);
  free(m.stable);
  clean();
}
static void metadata_growth(void) {
  unsigned char *body = malloc(PS_PAGE);
  ps_space *s;
  ps_stats stats;
  size_t count, step;
  CHECK(body != NULL);
  memset(body, 0x51, PS_PAGE);
  clean();
  s = create(3 * PS_MIB, 0, NULL);
  count = metadata_growth_fixture(s, body, 1);
  for (step = 1; step <= 3; ++step) {
    OK(ps_grow(s, PS_MIB, &error));
    OK(ps_status(s, &stats, &error));
    CHECK(stats.capacity == (3 + step) * PS_MIB);
    expect(s, state(0), NULL, "B", 1);
    expect(s, state(44), NULL, "A", 1);
    expect(s, state(1000), NULL, body, PS_PAGE);
    expect(s, state(999 + count), NULL, body, PS_PAGE);
    OK(ps_close(s, &error));
    s = reopen(NULL);
    OK(ps_status(s, &stats, &error));
    CHECK(stats.capacity == (3 + step) * PS_MIB);
  }
  OK(ps_close(s, &error));
  free(body);
  clean();
}
static void metadata_growth_faults(void) {
  repack_audit audit;
  ps_io io = model_init(&audit.model);
  unsigned char *body = malloc(PS_PAGE);
  unsigned char *baseline = malloc(3 * (size_t)PS_MIB);
  ps_space *s;
  ps_reader *r = NULL;
  ps_stats before, after;
  size_t count, kind, phase, sites = 0, i, got, profile;
  unsigned char current;
  int rc;
  CHECK(body && baseline);
  memset(body, 0x51, PS_PAGE);
  io.context = &audit;
  io.write = repack_write;
  audit.written = 0;
  for (profile = 0; profile < 2; ++profile) {
    clean();
    s = create(3 * PS_MIB, 0, &io);
    count = metadata_growth_fixture(s, body, (int)profile);
    OK(ps_close(s, &error));
    CHECK(audit.model.stable_bytes == 3 * PS_MIB);
    memcpy(baseline, audit.model.stable, 3 * (size_t)PS_MIB);
    for (kind = 0; kind < 3; ++kind) {
      sites = 0;
      for (phase = 0; phase <= sites; ++phase) {
        memcpy(audit.model.stable, baseline, 3 * (size_t)PS_MIB);
        audit.model.stable_bytes = 3 * PS_MIB;
        crash(&audit.model);
        s = reopen(&io);
        OK(ps_metadata_repack(s, &error));
        put(s, state(0), "C", 1, 1);
        OK(ps_status(s, &before, &error));
        audit.model.operations = audit.model.reads = 0;
        audit.model.tear = audit.model.persist_torn = 1;
        audit.model.fail_at = kind == 0 ? phase : 0;
        audit.model.fail_read = kind == 2 ? phase : 0;
        allocation_call = 0;
        allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
        rc = ps_grow(s, PS_MIB, &error);
        allocation_fail = 0;
        audit.model.fail_at = audit.model.fail_read = 0;
        if (!phase) {
          OK(rc);
          OK(ps_status(s, &after, &error));
          CHECK(after.descriptor_page_writes > before.descriptor_page_writes &&
                after.metadata_available_bytes >=
                    after.metadata_workspace_bytes);
          sites = kind == 0   ? audit.model.operations
                  : kind == 1 ? allocation_call
                              : audit.model.reads;
        } else {
          CHECK(rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
          lc_error_cleanup(&error);
          if (kind == 1) {
            OK(ps_status(s, &after, &error));
            CHECK(after.capacity == before.capacity &&
                  after.metadata_pages == before.metadata_pages &&
                  after.entries == before.entries &&
                  after.journal_bytes == before.journal_bytes &&
                  audit.model.stable_bytes == 3 * PS_MIB);
            put(s, state(0), "C", 1, 1);
            CHECK(after.metadata_available_bytes <=
                      before.metadata_available_bytes &&
                  after.metadata_available_bytes >=
                      after.metadata_workspace_bytes);
            /* Retry on this same live owner proves that private growth pages
             * returned by checkpoint abort did not leak or remain admitted. */
            OK(ps_grow(s, PS_MIB, &error));
          }
        }
        OK(ps_close(s, &error));
        crash(&audit.model);
        s = reopen(&io);
        OK(ps_status(s, &after, &error));
        CHECK(after.capacity == 3 * PS_MIB || after.capacity == 4 * PS_MIB);
        OK(ps_read_open(s, state(0), NULL, &r, &error));
        OK(ps_read(r, &current, 1, &got, &error));
        CHECK(got == 1 && (current == 'B' || current == 'C'));
        ps_reader_close(r);
        expect(s, state(44), NULL, "A", 1);
        for (i = 0; i < count; ++i)
          expect(s, state(1000 + i), NULL, body, PS_PAGE);
        if (after.capacity == 3 * PS_MIB)
          OK(ps_grow(s, PS_MIB, &error));
        OK(ps_metadata_repack(s, &error));
        put(s, state(9999), "after", 5, 1);
        OK(ps_checkpoint(s, &error));
        OK(ps_close(s, &error));
      }
      CHECK(sites > 0);
      printf("private growth failure sites (%s, %s): %lu\n",
             profile ? "full" : "repacked",
             kind == 0   ? "write/sync/reserve"
             : kind == 1 ? "allocation"
                         : "read",
             (unsigned long)sites);
    }
  }
  free(body);
  free(baseline);
  free(audit.model.stable);
  clean();
}
static void packing_lifecycle(void) {
  ps_space *s;
  ps_reader *held;
  ps_stats stats;
  unsigned char *body = malloc(PS_CHUNK + 1);
  size_t i, n, k = 0;
  CHECK(body != NULL);
  for (i = 0; i <= PS_CHUNK; ++i)
    body[i] = (unsigned char)(i * 19 + i / 31);
  clean();
  s = create(2 * PS_MIB, 0, NULL);
  for (i = 0; i < 64; ++i)
    put(s, state(i), body, 1, 1);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.allocated_bytes == PS_PAGE && stats.packed_pages == 1 &&
        stats.packed_bytes == PS_PAGE && stats.live_bytes == 64);
  OK(ps_read_open(s, state(0), NULL, &held, &error));
  for (i = 0; i < 64; i += 2)
    OK(ps_delete(s, state(i), &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.packed_bytes == 33 * PS_CELL && stats.retired_bytes == PS_CELL);
  /* Only isolated one-cell holes are free; fill them without new pages. */
  for (i = 0; i < 31; ++i)
    put(s, state(100 + i), body, PS_CELL, 7);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.allocated_bytes == PS_PAGE && stats.packed_bytes == PS_PAGE);
  put(s, state(1000), body, PS_CELL + 1, 7);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.packed_pages == 2 && stats.packed_bytes == PS_PAGE + 2 * PS_CELL);
  read_value(held, body, 1);
  ps_reader_close(held);
  for (i = 1; i < 64; i += 2)
    OK(ps_delete(s, state(i), &error));
  for (i = 0; i < 31; ++i)
    OK(ps_delete(s, state(100 + i), &error));
  OK(ps_delete(s, state(1000), &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(!stats.allocated_bytes && !stats.packed_bytes && !stats.packed_pages);
  /* Both sides of every cell boundary, including 63->64 cells, and whole-page
   * plus packed-tail splits. Producer chunking does not change ownership. */
  for (n = 1; n <= PS_PAGE; ++n)
    if (n % PS_CELL <= 1 || n % PS_CELL == PS_CELL - 1)
      put(s, state(k++), body, n, 37);
  for (n = PS_PAGE + 1; n <= PS_CHUNK + 1; n += PS_PAGE)
    put(s, state(k++), body, n, 317);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  k = 0;
  for (n = 1; n <= PS_PAGE; ++n)
    if (n % PS_CELL <= 1 || n % PS_CELL == PS_CELL - 1)
      expect(s, state(k++), NULL, body, n);
  for (n = PS_PAGE + 1; n <= PS_CHUNK + 1; n += PS_PAGE)
    expect(s, state(k++), NULL, body, n);
  OK(ps_close(s, &error));
  free(body);
  clean();
  puts("packing: cell boundaries, shared-page pins, fragmented spans and full "
       "page return");
}
static void packing_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_writer *w;
  ps_reader *r;
  ps_stats stats;
  unsigned char *body = malloc(PS_CHUNK + PS_PAGE);
  size_t kind, fresh, phase, count, old_size, wanted;
  int rc;
  CHECK(body != NULL);
  memset(body, 0xb3, PS_CHUNK + PS_PAGE);
  for (fresh = 0; fresh < 3; ++fresh)
    for (kind = 0; kind < 2; ++kind) {
      count = 0;
      old_size = fresh ? PS_PAGE - 1 : 1024;
      wanted = fresh == 2 ? PS_CHUNK + 65 : 1024;
      for (phase = 0; phase <= count; ++phase) {
        clean();
        m.operations = m.fail_at = 0;
        s = create(PS_MIB, 1, &io);
        put(s, state(1), body, old_size, 317);
        OK(ps_checkpoint(s, &error));
        m.operations = 0;
        m.fail_at = kind == 0 ? phase : 0;
        m.tear = m.persist_torn = 1;
        allocation_call = 0;
        allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
        w = NULL;
        rc = ps_begin(s, NULL, state(2), &w, &error);
        if (rc == LC_OK)
          rc = ps_write(w, body, wanted, &error);
        if (rc == LC_OK)
          rc = ps_publish(w, NULL, &error);
        allocation_fail = 0;
        m.fail_at = 0;
        if (!phase)
          count = kind == 0 ? m.operations : allocation_call;
        if (rc != LC_OK) {
          CHECK(kind == 0 || rc == LC_ERR_NOMEM);
          lc_error_cleanup(&error);
        }
        ps_writer_close(w);
        if (kind == 1) {
          expect(s, state(1), NULL, body, old_size);
          OK(ps_status(s, &stats, &error));
          CHECK(!stats.private_bytes);
          if (rc != LC_OK)
            CHECK(stats.allocated_bytes == PS_PAGE && stats.packed_pages == 1 &&
                  stats.packed_bytes ==
                      (old_size + PS_CELL - 1) / PS_CELL * PS_CELL);
        }
        OK(ps_close(s, &error));
        crash(&m);
        s = reopen(&io);
        expect(s, state(1), NULL, body, old_size);
        r = NULL;
        if (rc == LC_OK) {
          expect(s, state(2), NULL, body, wanted);
        } else if (ps_read_open(s, state(2), NULL, &r, &error) == LC_OK) {
          read_value(r, body, wanted);
          ps_reader_close(r);
        } else
          lc_error_cleanup(&error);
        put(s, state(3), body, 65, 1);
        expect(s, state(1), NULL, body, old_size);
        OK(ps_checkpoint(s, &error));
        OK(ps_status(s, &stats, &error));
        CHECK(stats.packed_pages == (fresh ? 2 : 1));
        OK(ps_close(s, &error));
      }
      CHECK(count >= 3);
    }
  /* A mixed whole-run/packed-tail abort cannot partly return ownership when
   * free-interval allocation fails. Poison prevents reuse until recovery. */
  clean();
  s = create(PS_MIB, 1, NULL);
  ps_key collection = key(PS_COLLECTION, 9, 1);
  put(s, collection, body, PS_CELL, 1);
  OK(ps_begin(s, NULL, collection, &w, &error));
  OK(ps_write(w, body, PS_CHUNK + 65, &error));
  BAD(ps_publish(w, NULL, &error)); /* existing live collection binding */
  allocation_call = 0;
  allocation_fail = 1;
  ps_writer_close(w);
  allocation_fail = 0;
  CHECK(allocation_call >= 1);
  BAD(ps_begin(s, NULL, state(99), &w, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, collection, NULL, body, PS_CELL);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.allocated_bytes == PS_PAGE && stats.packed_bytes == PS_CELL);
  put(s, state(99), body, PS_CELL + 1, 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  free(body);
  free(m.stable);
  clean();
  puts("packing: each publication I/O/heap failure in shared and fresh pages, "
       "persistent torn prefixes and neighbour survival");
}
static void packing_concurrency(void) {
  commit_gate gate;
  commit_actor actor;
  ps_io io;
  ps_space *s;
  ps_stats stats;
  pthread_t thread;
  unsigned char body[1024];
  size_t durable;
  for (durable = 0; durable < 2; ++durable) {
    clean();
    memset(&gate, 0, sizeof(gate));
    memset(&actor, 0, sizeof(actor));
    CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
    CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
    io.context = &gate;
    io.read = gate_read;
    io.write = gate_write;
    io.sync = gate_sync;
    io.reserve = gate_reserve;
    io.zero = NULL;
    s = create(PS_MIB, (int)durable, &io);
    gate.enabled = gate.hold_payload = 1;
    actor.space = s;
    actor.gate = &gate;
    CHECK(pthread_create(&thread, NULL, commit_worker, &actor) == 0);
    await_gate(&gate, 0); /* first writer has reserved cells, no payload I/O */
    memset(body, 'B', sizeof(body));
    put(s, state(2), body, sizeof(body), 31);
    OK(ps_checkpoint(s, &error));
    expect(s, state(2), NULL, body, sizeof(body));
    OK(ps_status(s, &stats, &error));
    CHECK(stats.packed_pages == 1 && stats.private_bytes == sizeof(body));
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.cond);
    pthread_mutex_unlock(&gate.mutex);
    CHECK(pthread_join(thread, NULL) == 0 && actor.rc == LC_OK);
    expect(s, state(2), NULL, body, sizeof(body));
    memset(body, 'A', sizeof(body));
    expect(s, state(1), &actor.ref, body, sizeof(body));
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, state(1), &actor.ref, body, sizeof(body));
    memset(body, 'B', sizeof(body));
    expect(s, state(2), NULL, body, sizeof(body));
    OK(ps_close(s, &error));
    CHECK(pthread_cond_destroy(&gate.cond) == 0);
    CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  }
  clean();
  puts("packing: independent publication and checkpoint on a shared page "
       "during held payload I/O, NoSync and durable");
}
static uint64_t fixture_get64(const unsigned char *p) {
  uint64_t value = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (i * 8);
  return value;
}
static void fixture_put(unsigned char *p, uint64_t value, size_t bytes) {
  size_t i;
  for (i = 0; i < bytes; ++i)
    p[i] = (unsigned char)(value >> (i * 8));
}
static void fixture_entry_crc(unsigned char *record) {
  size_t bytes = (size_t)(uint32_t)fixture_get64(record + 4);
  CHECK(bytes >= 64 && bytes <= PS_JOURNAL);
  uint32_t crc = (uint32_t)crc32(0, record, 60);
  fixture_put(record + 60, crc32(crc, record + 64, (uInt)(bytes - 64)), 4);
}
static void packing_invalid_ownership(void) {
  ps_space *s = NULL;
  unsigned char *buffer = malloc(PS_PAGE), *body = malloc(PS_PAGE);
  unsigned char roots[PS_ROOT_BYTES], records[176];
  unsigned char *record;
  uint64_t directory, reference, offset;
  size_t phase;
  int fd;
  CHECK(buffer && body);
  memset(body, 'A', PS_PAGE);
  /* Keep all outer CRCs valid so these cases exercise ownership validation,
   * rather than being rejected merely as damaged encoded bytes. */
  for (phase = 0; phase < 8; ++phase) {
    clean();
    s = create(PS_MIB, 0, NULL);
    put(s, state(1), body,
        phase == 1   ? PS_PAGE
        : phase == 7 ? PS_CELL + 1
                     : PS_CELL,
        31);
    put(s, state(2), body, phase == 2 ? PS_PAGE : PS_CELL, 31);
    if (phase >= 3)
      OK(ps_checkpoint(s, &error));
    OK(ps_close(s, &error));
    fd = open(path, O_RDWR);
    CHECK(fd >= 0);
    if (phase < 3) {
      CHECK(pread(fd, records, sizeof(records), PS_JOURNAL_OFFSET) ==
            (ssize_t)sizeof(records));
      /* Alias two packed cells, a whole page with packed cells, and the
       * reverse recovery order. Bodies match, so both payload CRCs pass. */
      record = records + (phase == 2 ? 0 : 88);
      offset = fixture_get64(records + (phase == 2 ? 88 : 0) + 64);
      fixture_put(record + 64, offset, 8);
      fixture_entry_crc(record);
      CHECK(pwrite(fd, records, sizeof(records), PS_JOURNAL_OFFSET) ==
            (ssize_t)sizeof(records));
    } else {
      CHECK(pread(fd, roots, sizeof(roots), 0) == (ssize_t)sizeof(roots));
      directory = fixture_get64(roots + 32);
      CHECK(pread(fd, buffer, PS_PAGE, (off_t)directory) == PS_PAGE);
      reference = fixture_get64(buffer + 64 + 24); /* first sorted leaf key */
      offset = reference / PS_PAGE * PS_PAGE;
      CHECK(pread(fd, buffer, PS_PAGE, (off_t)offset) == PS_PAGE);
      record = buffer + reference % PS_PAGE;
      if (phase == 3)
        fixture_put(record + 84, 2, 4); /* unknown extent flags */
      else if (phase == 4)
        fixture_put(record + 64, PS_DATA + 1, 8); /* cell misalignment */
      else if (phase == 5)
        fixture_put(record + 84, 0, 4); /* packed address as whole page */
      else if (phase == 6)
        fixture_put(record + 64, PS_DATA + PS_CELL, 8); /* duplicate cells */
      else
        fixture_put(record + 64, PS_DATA + PS_PAGE - PS_CELL,
                    8); /* span crosses physical page */
      fixture_entry_crc(record);
      uint32_t crc = (uint32_t)crc32(0, buffer, 48);
      fixture_put(buffer + 48, crc32(crc, buffer + 52, PS_PAGE - 52), 4);
      CHECK(pwrite(fd, buffer, PS_PAGE, (off_t)offset) == PS_PAGE);
    }
    CHECK(fdatasync(fd) == 0 && close(fd) == 0);
    CHECK(ps_open(path, 0, NULL, &s, &error) == LC_ERR_PROTOCOL && s == NULL);
    lc_error_cleanup(&error);
  }
  free(buffer);
  free(body);
  clean();
  puts("packing: checksummed duplicate/cross-kind ownership, invalid flags "
       "and alignment rejected on recovery");
}

typedef struct recovery_trace {
  uint64_t start, bytes, records;
} recovery_trace;
/* Actual Pouch header/CRC and PENDING-finalization rules. This canonical
 * fixture owns key/meta validation; physical stream recovery has no parser. */
static int canonical_recover(ps_stream *stream, uint64_t start,
                             uint64_t capacity, uint64_t *accepted,
                             void *context, lc_error *e) {
  recovery_trace *trace = context;
  lc_pouch_record_header record;
  unsigned char header[32], prefix[11], zero[32] = {0};
  unsigned char *buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
  uint64_t cursor = start, offset, remaining;
  unsigned long crc;
  size_t n;
  int rc = LC_OK;
  if (!buffer)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "canonical recovery buffer", NULL,
                        NULL, "test");
  if (trace)
    trace->start = start;
  while (capacity - cursor >= sizeof(header)) {
    rc = ps_stream_recovery_read(stream, cursor, header, sizeof(header), e);
    if (rc != LC_OK)
      break;
    if (trace)
      trace->bytes += sizeof(header);
    if (!memcmp(header, zero, sizeof(header)))
      break;
    rc = lc_pouch_record_header_decode(header, &record, e);
    if (rc == LC_ERR_PROTOCOL) {
      lc_error_cleanup(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK)
      break;
    if (record.flags || record.type != 1 || record.key_len != 3 ||
        record.meta_len != 8 || capacity - cursor < 43 ||
        record.payload_len > capacity - cursor - 43)
      break;
    rc =
        ps_stream_recovery_read(stream, cursor + 32, prefix, sizeof(prefix), e);
    if (rc != LC_OK || memcmp(prefix, "key", 3))
      break;
    if (trace)
      trace->bytes += sizeof(prefix);
    remaining = record.payload_len;
    offset = cursor + 43;
    crc = 0;
    while (remaining && rc == LC_OK) {
      n = remaining > PS_CHUNK ? PS_CHUNK : (size_t)remaining;
      rc = ps_stream_recovery_read(stream, offset, buffer, n, e);
      if (rc == LC_OK)
        crc = crc32(crc, buffer, (uInt)n);
      if (trace)
        trace->bytes += n;
      offset += n;
      remaining -= n;
    }
    if (rc != LC_OK || crc != record.payload_crc)
      break;
    cursor = offset;
    if (trace)
      ++trace->records;
  }
  *accepted = cursor;
  lc_free_with_allocator(NULL, buffer);
  return rc;
}
static ps_key stream_key(uint64_t id) { return key(PS_STREAM, 10, id); }
static ps_stream *stream_open(ps_space *space, uint64_t id, int create,
                              const ps_ref *ref, recovery_trace *trace) {
  ps_stream *stream = NULL;
  OK(ps_stream_open(space, NULL, stream_key(id), create, ref, canonical_recover,
                    trace, &stream, &error));
  return stream;
}
static int canonical_append(ps_stream *s, const void *bytes, size_t n,
                            size_t chunk, uint64_t tag, uint64_t *start,
                            lc_error *e) {
  ps_stream_writer *w = NULL;
  lc_pouch_record_header record;
  unsigned char header[32], prefix[11];
  size_t offset = 0, take;
  int rc;
  memset(&record, 0, sizeof(record));
  record.type = 1;
  record.flags = LC_POUCH_RECORD_FLAG_PENDING;
  record.key_len = 3;
  record.meta_len = 8;
  lc_pouch_record_header_encode(&record, header);
  memcpy(prefix, "key", 3);
  memset(prefix + 3, 0xff, 8);
  rc = ps_stream_begin(s, &w, e);
  if (rc == LC_OK) {
    *start = ps_stream_writer_start(w);
    rc = ps_stream_write(w, header, sizeof(header), e);
  }
  if (rc == LC_OK)
    rc = ps_stream_write(w, prefix, sizeof(prefix), e);
  while (rc == LC_OK && offset < n) {
    take = n - offset;
    if (take > chunk)
      take = chunk;
    rc = ps_stream_write(w, (const unsigned char *)bytes + offset, take, e);
    offset += take;
  }
  fixture_put(prefix + 3, tag, 8);
  if (rc == LC_OK)
    rc = ps_stream_write_at(w, 32, prefix, sizeof(prefix), e);
  record.flags = 0;
  record.payload_len = n;
  record.payload_crc = crc32(0, bytes, (uInt)n);
  lc_pouch_record_header_encode(&record, header);
  if (rc == LC_OK)
    rc = ps_stream_write_at(w, 0, header, sizeof(header), e);
  if (rc == LC_OK)
    rc = ps_stream_commit(w, PS_COMMIT_DEFAULT, NULL, NULL, e);
  ps_stream_writer_close(w);
  return rc;
}
static void stream_expect(ps_stream *s, uint64_t start, const void *bytes,
                          size_t n, uint64_t tag) {
  ps_stream_reader *r = NULL;
  lc_pouch_record_header header;
  unsigned char prefix[43], buffer[317];
  size_t offset = 0, got;
  OK(ps_stream_read_open(s, start, 43 + n, &r, &error));
  BAD(ps_stream_close(s, &error));
  OK(ps_stream_read(r, prefix, sizeof(prefix), &got, &error));
  CHECK(got == sizeof(prefix));
  OK(lc_pouch_record_header_decode(prefix, &header, &error));
  CHECK(!header.flags && header.type == 1 && header.key_len == 3 &&
        header.meta_len == 8 && header.payload_len == n &&
        header.payload_crc == crc32(0, bytes, (uInt)n) &&
        !memcmp(prefix + 32, "key", 3) && fixture_get64(prefix + 35) == tag);
  do {
    OK(ps_stream_read(r, buffer, sizeof(buffer), &got, &error));
    CHECK(
        got <= n - offset &&
        (!got || !memcmp(buffer, (const unsigned char *)bytes + offset, got)));
    offset += got;
  } while (got);
  CHECK(offset == n);
  ps_stream_reader_close(r);
}

static void canonical_stream_exact_reservations(void) {
  const size_t spans[] = {4031,  4032,  4033,  20351, 20352,
                          20353, 85823, 85824, 85825};
  unsigned char *body = malloc(spans[8]);
  size_t i, j;
  CHECK(body != NULL);
  for (j = 0; j < spans[8]; ++j)
    body[j] = (unsigned char)(j * 17 + j / 29);
  for (i = 0; i < sizeof(spans) / sizeof(*spans); ++i) {
    ps_space *space;
    ps_stream *stream;
    ps_stream_writer *writer = NULL;
    ps_stream_info before, after;
    uint64_t offset;
    clean();
    space = create(PS_DEFAULT_CAPACITY, 0, NULL);
    stream = stream_open(space, 1, 1, NULL, NULL);
    OK(ps_stream_begin(stream, &writer, &error));
    OK(ps_stream_reserve_exact(writer, NULL, spans[i], &error));
    OK(ps_stream_status(stream, &before, &error));
    CHECK(!before.tail && before.capacity >= spans[i] &&
          before.capacity - spans[i] < PS_PAGE);
    ps_stream_writer_close(writer);
    OK(canonical_append(stream, body, spans[i] - 43, 7, i, &offset, &error));
    OK(ps_stream_status(stream, &after, &error));
    CHECK(offset == 0 && after.tail == spans[i] &&
          after.capacity == before.capacity && after.runs == before.runs);
    stream_expect(stream, 0, body, spans[i] - 43, i);
    OK(ps_checkpoint(space, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    space = reopen(NULL);
    stream = stream_open(space, 1, 0, &after.ref, NULL);
    stream_expect(stream, 0, body, spans[i] - 43, i);
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    clean();
  }
  free(body);
}
static void canonical_stream_lifecycle(void) {
  ps_space *space;
  ps_stream *stream, *other_stream = NULL;
  ps_stream_writer *w = NULL, *busy = NULL;
  ps_writer *wrong_kind = NULL;
  ps_stream_reader *held = NULL;
  ps_stream_info info;
  ps_stats before, after;
  ps_ref ref, invalid_ref;
  recovery_trace trace = {0};
  unsigned char *body = malloc((size_t)PS_MIB + 17);
  unsigned char frame[107], readback[107];
  lc_pouch_record_header header;
  uint64_t offsets[100], large_offset, last;
  size_t i, got;
  CHECK(body != NULL);
  for (i = 0; i < (size_t)PS_MIB + 17; ++i)
    body[i] = (unsigned char)(i * 17 + i / 29);
  clean();
  space = create(2 * PS_MIB, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_status(stream, &info, &error));
  ref = info.ref;
  CHECK(!info.tail && !info.runs && !info.capacity);
  BAD(ps_close(space, &error));
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, NULL, canonical_recover,
                     NULL, &other_stream, &error));
  BAD(ps_begin(space, NULL, stream_key(1), &wrong_kind, &error));
  BAD(ps_delete(space, stream_key(1), &error));
  BAD(ps_stream_recovery_read(stream, 0, frame, 1, &error));
  for (i = 0; i < 100; ++i) {
    if (i == 1)
      OK(ps_status(space, &before, &error));
    OK(canonical_append(stream, body, 1024, 7, i, &offsets[i], &error));
    if (i == 1) {
      OK(ps_status(space, &after, &error));
      CHECK(before.journal_bytes == after.journal_bytes &&
            before.syncs == after.syncs);
    }
    if (i == 20)
      OK(ps_checkpoint(space, &error));
  }
  OK(ps_stream_read_open(stream, offsets[0], sizeof(frame), &held, &error));
  OK(ps_stream_read(held, frame, sizeof(frame), &got, &error));
  CHECK(got == sizeof(frame));
  ps_stream_reader_close(held);
  OK(ps_stream_read_open(stream, offsets[0], sizeof(frame), &held, &error));
  allocation_max = 0;
  measure_allocations = 1;
  OK(canonical_append(stream, body, (size_t)PS_MIB + 17, 31, 100, &large_offset,
                      &error));
  measure_allocations = 0;
  CHECK(allocation_max <= PS_CHUNK + 256);
  OK(ps_grow(space, PS_MIB, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_read(held, readback, sizeof(readback), &got, &error));
  CHECK(got == sizeof(readback) && !memcmp(frame, readback, got));
  ps_stream_reader_close(held);
  stream_expect(stream, large_offset, body, (size_t)PS_MIB + 17, 100);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.runs <= 2 + info.tail / (PS_STREAM_PIECE_BYTES - PS_RUN_HEADER) &&
        info.capacity - info.tail < PS_STREAM_PIECE_BYTES);
  BAD(ps_stream_read_open(stream, UINT64_MAX, 0, &held, &error));
  CHECK(held == NULL);
  BAD(ps_stream_read_open(stream, 0, UINT64_MAX, &held, &error));
  CHECK(held == NULL);
  OK(ps_stream_read_open(stream, info.tail, 0, &held, &error));
  OK(ps_stream_read(held, NULL, 0, &got, &error));
  CHECK(got == 0);
  OK(ps_stream_read(held, readback, sizeof(readback), &got, &error));
  CHECK(got == 0);
  ps_stream_reader_close(held);
  memset(&header, 0, sizeof(header));
  header.type = 1;
  header.key_len = 3;
  header.meta_len = 8;
  header.payload_len = 64;
  header.payload_crc = crc32(0, body, 64);
  lc_pouch_record_header_encode(&header, frame);
  memcpy(frame + 32, "key", 3);
  fixture_put(frame + 35, 999, 8);
  memcpy(frame + 43, body, 64);
  /* Flush complete-looking records, abandon them, then publish a shorter
   * record. Valid stale suffix records must never resurrect after reopen. */
  OK(ps_stream_begin(stream, &w, &error));
  BAD(ps_stream_begin(stream, &busy, &error));
  for (i = 0; i < 200; ++i)
    OK(ps_stream_write(w, frame, sizeof(frame), &error));
  OK(ps_stream_write_at(w, 200 * sizeof(frame), NULL, 0, &error));
  OK(ps_checkpoint(space, &error));
  ps_stream_info pending_info;
  OK(ps_stream_status(stream, &pending_info, &error));
  CHECK(pending_info.tail == info.tail);
  BAD(ps_stream_write_at(w, UINT64_MAX, frame, 1, &error));
  ps_stream_writer_close(w);
  OK(canonical_append(stream, body, 64, 3, 101, &last, &error));
  CHECK(last == info.tail);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  invalid_ref = ref;
  ++invalid_ref.incarnation;
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, &invalid_ref,
                     canonical_recover, NULL, &other_stream, &error));
  CHECK(other_stream == NULL);
  invalid_ref = ref;
  invalid_ref.container[0] ^= 1;
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, &invalid_ref,
                     canonical_recover, NULL, &other_stream, &error));
  CHECK(other_stream == NULL);
  BAD(ps_stream_open(space, NULL, stream_key(1), 1, &ref, canonical_recover,
                     NULL, &other_stream, &error));
  BAD(ps_stream_open(space, NULL, stream_key(999), 0, NULL, canonical_recover,
                     NULL, &other_stream, &error));
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, &ref, NULL, NULL,
                     &other_stream, &error));
  CHECK(other_stream == NULL);
  stream = stream_open(space, 1, 0, &ref, &trace);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == last + 107 && trace.start == info.tail &&
        trace.bytes <= 32 && !trace.records);
  for (i = 0; i < 100; ++i)
    stream_expect(stream, offsets[i], body, 1024, i);
  stream_expect(stream, large_offset, body, (size_t)PS_MIB + 17, 100);
  stream_expect(stream, last, body, 64, 101);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  free(body);
  clean();
  puts("streams: stable refs, bounded append/patch, run growth, pinned reads, "
       "checkpoint tails and durable abandoned-suffix clearing");
}
static void canonical_stream_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  unsigned char *body = malloc(PS_CHUNK + 333);
  uint64_t first, next;
  size_t kind, phase, count;
  int rc;
  CHECK(body != NULL);
  memset(body, 0xd3, PS_CHUNK + 333);
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      space = create(PS_MIB, 1, &io);
      stream = stream_open(space, 1, 1, NULL, NULL);
      OK(canonical_append(stream, body, 64, 11, 1, &first, &error));
      OK(ps_checkpoint(space, &error));
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = canonical_append(stream, body, PS_CHUNK + 333, 37, 2, &next, &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc != LC_OK)
        lc_error_cleanup(&error);
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      crash(&m);
      space = reopen(&io);
      stream = stream_open(space, 1, 0, NULL, NULL);
      stream_expect(stream, first, body, 64, 1);
      OK(ps_stream_status(stream, &info, &error));
      CHECK(info.tail == 107 || info.tail == 107 + 43 + PS_CHUNK + 333);
      if (rc == LC_OK)
        CHECK(info.tail != 107);
      if (info.tail != 107)
        stream_expect(stream, next, body, PS_CHUNK + 333, 2);
      OK(canonical_append(stream, body, 65, 7, 3, &next, &error));
      OK(ps_checkpoint(space, &error));
      stream_expect(stream, next, body, 65, 3);
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
    }
    CHECK(count > 5);
  }
  free(body);
  free(m.stable);
  clean();
  puts("streams: every append I/O/heap fault across grants, private patches "
       "and commit, persisted torn prefixes and canonical recovery");
}
static void canonical_stream_reused_grants(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  lc_pouch_record_header header;
  unsigned char *old = calloc(1, PS_PAGE), grant[PS_OWNED_DESCRIPTOR_BYTES];
  uint64_t start;
  size_t phase, count = 0, i;
  int rc, fd;
  CHECK(old != NULL);
  memset(&header, 0, sizeof(header));
  header.type = 1;
  header.key_len = 3;
  header.meta_len = 8;
  header.payload_len = 64;
  header.payload_crc = crc32(0, old, 64);
  lc_pouch_record_header_encode(&header, old + PS_RUN_HEADER);
  memcpy(old + PS_RUN_HEADER + 32, "key", 3);
  for (phase = 0; phase <= count; ++phase) {
    clean();
    m.operations = m.fail_at = 0;
    space = create(PS_MIB, 0, &io);
    put(space, state(1), old, PS_PAGE, 317);
    OK(ps_checkpoint(space, &error));
    OK(ps_delete(space, state(1), &error));
    OK(ps_checkpoint(space, &error));
    stream = stream_open(space, 1, 1, NULL, NULL);
    OK(ps_checkpoint(space, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    space = reopen(&io); /* invalidate all transient zero certificates */
    stream = stream_open(space, 1, 0, NULL, NULL);
    OK(ps_checkpoint(space, &error)); /* keep grant-at-journal-start fixture */
    m.operations = 0;
    m.fail_at = phase;
    m.tear = m.persist_torn = 1;
    rc = canonical_append(stream, old, 64, 7, 2, &start, &error);
    m.fail_at = 0;
    if (!phase)
      count = m.operations;
    if (rc != LC_OK)
      lc_error_cleanup(&error);
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    if (rc == LC_OK) {
      /* Persist the physical grant ahead of its canonical body. Its required
       * preparation barrier must already have durably removed stale records. */
      for (i = PS_RUN_HEADER; i < PS_PAGE; ++i)
        CHECK(m.stable[PS_DATA + i] == 0);
      fd = open(path, O_RDONLY);
      CHECK(fd >= 0);
      CHECK(pread(fd, grant, sizeof(grant), PS_JOURNAL_OFFSET) ==
            (ssize_t)sizeof(grant));
      CHECK(close(fd) == 0);
      memcpy(m.stable + PS_JOURNAL_OFFSET, grant, sizeof(grant));
    }
    crash(&m);
    space = reopen(&io);
    stream = stream_open(space, 1, 0, NULL, NULL);
    OK(ps_stream_status(stream, &info, &error));
    CHECK(info.tail == 0); /* a run grant never publishes its stale payload */
    OK(canonical_append(stream, old, 65, 1, 3, &start, &error));
    stream_expect(stream, start, old, 65, 3);
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
  }
  CHECK(count >= 5);
  free(old);
  free(m.stable);
  clean();
  puts("streams: reused-run initialization faults, durable zero provenance "
       "and grant-before-body persistence without stale record resurrection");
}
typedef struct stream_actor {
  ps_stream *stream;
  commit_gate *gate;
  uint64_t start;
  int ordinal, rc;
} stream_actor;
static void *stream_commit_worker(void *context) {
  stream_actor *actor = context;
  unsigned char body[1024];
  lc_error e;
  lc_error_init(&e);
  memset(body, 'A' + actor->ordinal, sizeof(body));
  actor->rc = canonical_append(actor->stream, body, sizeof(body), 31, 2,
                               &actor->start, &e);
  lc_error_cleanup(&e);
  pthread_mutex_lock(&actor->gate->mutex);
  ++actor->gate->finished;
  pthread_cond_broadcast(&actor->gate->cond);
  pthread_mutex_unlock(&actor->gate->mutex);
  return NULL;
}
static void await_stream_requests(ps_space *space, uint64_t requests) {
  ps_stats stats;
  struct timespec now, deadline, pause = {0, 1000000};
  OK(ps_status(space, &stats, &error));
  CHECK(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
  deadline.tv_sec += 5;
  while (stats.sync_requests < requests) {
    CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    CHECK(now.tv_sec < deadline.tv_sec ||
          (now.tv_sec == deadline.tv_sec && now.tv_nsec < deadline.tv_nsec));
    CHECK(nanosleep(&pause, NULL) == 0);
    OK(ps_status(space, &stats, &error));
  }
}
static void await_stream_finished(commit_gate *g, int count) {
  struct timespec deadline;
  int rc = 0;
  CHECK(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&g->mutex);
  while (g->finished < count && !rc)
    rc = pthread_cond_timedwait(&g->cond, &g->mutex, &deadline);
  int done = g->finished >= count;
  pthread_mutex_unlock(&g->mutex);
  CHECK(done && !rc);
}
static void canonical_stream_concurrency(void) {
  commit_gate gate;
  stream_actor actors[4];
  commit_actor state_actor;
  ps_io io;
  ps_space *space;
  ps_stream *streams[4];
  ps_stream_info info;
  ps_stats before, after;
  pthread_t threads[5];
  unsigned char body[1024];
  uint64_t start;
  size_t i;
  clean();
  memset(&gate, 0, sizeof(gate));
  memset(actors, 0, sizeof(actors));
  memset(&state_actor, 0, sizeof(state_actor));
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  io.context = &gate;
  io.read = gate_read;
  io.write = gate_write;
  io.sync = gate_sync;
  io.reserve = gate_reserve;
  io.zero = NULL;
  space = create(PS_MIB, 1, &io);
  memset(body, 'O', sizeof(body));
  for (i = 0; i < 4; ++i) {
    streams[i] = stream_open(space, i, 1, NULL, NULL);
    OK(canonical_append(streams[i], body, 64, 7, 1, &start, &error));
    actors[i].stream = streams[i];
    actors[i].gate = &gate;
    actors[i].ordinal = (int)i;
  }
  put(space, state(1), "old", 3, 1);
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &before, &error));
  size_t syncs = gate.syncs;
  gate.enabled = gate.hold_second = 1;
  CHECK(pthread_create(&threads[0], NULL, stream_commit_worker, &actors[0]) ==
        0);
  await_gate(&gate, 0);
  for (i = 1; i < 4; ++i)
    CHECK(pthread_create(&threads[i], NULL, stream_commit_worker, &actors[i]) ==
          0);
  state_actor.space = space;
  state_actor.gate = &gate;
  CHECK(pthread_create(&threads[4], NULL, commit_worker, &state_actor) == 0);
  await_stream_requests(space, before.sync_requests + 5);
  for (i = 0; i < 4; ++i) {
    OK(ps_stream_status(streams[i], &info, &error));
    CHECK(info.tail == 107);
    stream_expect(streams[i], 0, body, 64, 1);
  }
  expect(space, state(1), NULL, "old", 3);
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  await_gate(&gate, 3);
  await_stream_finished(&gate, 1);
  OK(ps_stream_status(streams[0], &info, &error));
  CHECK(info.tail == 1174);
  for (i = 1; i < 4; ++i) {
    OK(ps_stream_status(streams[i], &info, &error));
    CHECK(info.tail == 107);
  }
  expect(space, state(1), NULL, "old", 3);
  pthread_mutex_lock(&gate.mutex);
  gate.second_released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  for (i = 0; i < 5; ++i)
    CHECK(pthread_join(threads[i], NULL) == 0);
  for (i = 0; i < 4; ++i) {
    CHECK(actors[i].rc == LC_OK && actors[i].start == 107);
    memset(body, 'A' + (int)i, sizeof(body));
    stream_expect(streams[i], 107, body, sizeof(body), 2);
    OK(ps_stream_close(streams[i], &error));
  }
  CHECK(state_actor.rc == LC_OK);
  OK(ps_status(space, &after, &error));
  CHECK(gate.syncs == syncs + 2 &&
        after.sync_batches == before.sync_batches + 2 &&
        after.max_sync_batch >= 4);
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0);
  CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
  puts("streams: held root barriers, independent ready producers, exact prefix "
       "visibility and mixed immutable/stream commit groups");
}
static void canonical_stream_read_recovery(int native_zero, int durable,
                                           int action) {
  model m;
  ps_io io = model_init(&m);
  ps_options o = {0};
  ps_space *space;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_reader *held = NULL, *rejected = NULL;
  ps_stream_info info;
  ps_stats before, after;
  recovery_trace trace = {0};
  lc_pouch_record_header header = {0};
  unsigned char body[64], captured[107];
  unsigned char *private = calloc(1, 2 * PS_CHUNK);
  unsigned char *image = malloc(PS_MIB);
  uint64_t first, second, next = 0;
  size_t operations, zeros, got;
  CHECK(private && image);
  if (native_zero)
    io.zero = model_zero;
  o.io = &io;
  o.durable_sync = durable;
  memset(body, 0x64, sizeof(body));
  clean();
  space = create(PS_MIB, durable, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(canonical_append(stream, body, 64, 7, 1, &first, &error));
  OK(ps_checkpoint(space, &error));
  OK(canonical_append(stream, body, 31, 7, 2, &second, &error));
  /* A persisted PENDING gap hides another valid record exactly where a later
   * shorter append will end. Missing deferred repair would resurrect it. */
  header.type = 1;
  header.key_len = 3;
  header.meta_len = 8;
  header.payload_len = 64;
  memset(private + 60 + 43, 0xee, 64);
  header.payload_crc = crc32(0, private + 60 + 43, 64);
  lc_pouch_record_header_encode(&header, private + 60);
  memcpy(private + 60 + 32, "key", 3);
  fixture_put(private + 60 + 35, 999, 8);
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  lc_pouch_record_header_encode(&header, private);
  OK(ps_stream_begin(stream, &writer, &error));
  OK(ps_stream_write(writer, private, 2 * PS_CHUNK, &error));
  put(space, key(PS_CONTROL, 20, 1), "peer", 4, 4);
  memcpy(image, m.stable, PS_MIB);
  ps_stream_writer_close(writer);
  writer = NULL;
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  memcpy(m.stable, image, PS_MIB);
  crash(&m);
  OK(ps_open(path, 0, &o, &space, &error));
  operations = m.operations;
  zeros = m.zero_calls;
  OK(ps_status(space, &before, &error));
  stream = stream_open(space, 1, 0, NULL, &trace);
  CHECK(trace.start == 107 && trace.records == 1);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == 181);
  stream_expect(stream, first, body, 64, 1);
  stream_expect(stream, second, body, 31, 2);
  BAD(ps_stream_read_open(stream, info.tail, 1, &rejected, &error));
  CHECK(!rejected);
  OK(ps_status(space, &after, &error));
  CHECK(m.operations == operations && m.zero_calls == zeros &&
        after.syncs == before.syncs &&
        after.journal_bytes == before.journal_bytes);
  /* Newly accepted bytes join explicit, peer and checkpoint durability,
   * even before suffix repair. Covered barriers must not sync again. */
  if (!action)
    OK(ps_flush(space, &error));
  else if (action == 1)
    put(space, key(PS_CONTROL, 20, 1), "covered", 7, 7);
  else
    OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK((action == 2 ? after.syncs > before.syncs
                     : after.syncs == before.syncs + 1) &&
        m.zero_calls == zeros);
  before = after;
  OK(ps_flush(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.syncs == before.syncs);
  if (!action)
    OK(ps_checkpoint(space, &error));
  OK(ps_stream_read_open(stream, first, 107, &held, &error));
  if (!action) {
    operations = m.operations;
    allocation_call = 0;
    allocation_fail = 1;
    CHECK(ps_stream_begin(stream, &writer, &error) == LC_ERR_NOMEM && !writer);
    allocation_fail = 0;
    lc_error_cleanup(&error);
    CHECK(m.operations == operations && m.zero_calls == zeros);
    OK(ps_status(space, &before, &error));
    OK(ps_stream_begin(stream, &writer, &error));
    OK(ps_status(space, &after, &error));
    CHECK(after.syncs == before.syncs + 1);
    ps_stream_writer_close(writer);
    writer = NULL;
    before = after;
    OK(canonical_append(stream, body, 17, 7, 3, &next, &error));
    CHECK(next == 181);
    OK(ps_status(space, &after, &error));
    CHECK(after.syncs == before.syncs + durable);
    OK(ps_flush(space, &error));
  } else if (action == 1) {
    OK(ps_stream_seal(stream, &error));
    CHECK(m.zero_calls == zeros);
    BAD(ps_stream_begin(stream, &writer, &error));
    CHECK(!writer);
  } else {
    OK(ps_stream_retire(stream, &error));
    CHECK(m.zero_calls == zeros);
    BAD(ps_stream_read_open(stream, 0, 107, &rejected, &error));
    CHECK(!rejected);
  }
  OK(ps_stream_read(held, captured, sizeof(captured), &got, &error));
  CHECK(got == sizeof(captured) && fixture_get64(captured + 35) == 1 &&
        !memcmp(captured + 43, body, 64));
  ps_stream_reader_close(held);
  if (action == 2)
    OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  crash(&m);
  OK(ps_open(path, 0, &o, &space, &error));
  if (action == 2) {
    BAD(ps_stream_open(space, NULL, stream_key(1), 0, NULL, canonical_recover,
                       NULL, &stream, &error));
    CHECK(!stream);
  } else {
    trace = (recovery_trace){0};
    stream = stream_open(space, 1, 0, NULL, &trace);
    OK(ps_stream_status(stream, &info, &error));
    CHECK(info.tail == (action ? 181 : 241));
    CHECK(!action || (!trace.records && info.sealed));
    stream_expect(stream, first, body, 64, 1);
    stream_expect(stream, second, body, 31, 2);
    if (!action)
      stream_expect(stream, next, body, 17, 3);
    OK(ps_stream_close(stream, &error));
  }
  OK(ps_close(space, &error));
  free(private);
  free(image);
  free(m.stable);
  clean();
}
static void canonical_stream_repair_concurrency(int native_zero, int durable) {
  commit_gate gate = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_options o = {0};
  ps_space *space;
  ps_stream *a, *b;
  ps_stream_writer *blocked = NULL;
  ps_stream_reader *held = NULL;
  ps_stream_info info;
  stream_actor actor = {0};
  pthread_t thread;
  unsigned char old[64], peer[31], body[1024], captured[107];
  uint64_t start;
  size_t got;
  clean();
  if (native_zero)
    io.zero = gate_zero;
  o.io = &io;
  o.durable_sync = durable;
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  memset(old, 0x73, sizeof(old));
  memset(peer, 0x81, sizeof(peer));
  space = create(PS_MIB, durable, &io);
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  OK(canonical_append(a, old, sizeof(old), 7, 1, &start, &error));
  OK(canonical_append(b, old, sizeof(old), 7, 1, &start, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  OK(ps_open(path, 0, &o, &space, &error));
  a = stream_open(space, 1, 0, NULL, NULL);
  b = stream_open(space, 2, 0, NULL, NULL);
  OK(ps_stream_read_open(a, 0, 107, &held, &error));
  actor.stream = a;
  actor.gate = &gate;
  gate.enabled = gate.hold_payload = 1;
  CHECK(pthread_create(&thread, NULL, stream_commit_worker, &actor) == 0);
  await_gate(&gate, 0);
  OK(ps_stream_status(a, &info, &error));
  CHECK(info.tail == 107);
  BAD(ps_stream_begin(a, &blocked, &error));
  CHECK(!blocked);
  stream_expect(a, 0, old, sizeof(old), 1);
  OK(canonical_append(b, peer, sizeof(peer), 7, 2, &start, &error));
  CHECK(start == 107);
  stream_expect(b, start, peer, sizeof(peer), 2);
  OK(ps_checkpoint(space, &error));
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(thread, NULL) == 0 && actor.rc == LC_OK &&
        actor.start == 107);
  memset(body, 'A', sizeof(body));
  stream_expect(a, actor.start, body, sizeof(body), 2);
  OK(ps_stream_read(held, captured, sizeof(captured), &got, &error));
  CHECK(got == sizeof(captured) && !memcmp(captured + 43, old, sizeof(old)));
  ps_stream_reader_close(held);
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0);
  CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
}
typedef struct repair_actor {
  ps_stream *stream;
  commit_gate *gate;
  int rc;
} repair_actor;
static void *stream_repair_worker(void *context) {
  repair_actor *actor = context;
  ps_stream_writer *writer = NULL;
  lc_error e;
  lc_error_init(&e);
  actor->rc = ps_stream_begin(actor->stream, &writer, &e);
  ps_stream_writer_close(writer);
  lc_error_cleanup(&e);
  pthread_mutex_lock(&actor->gate->mutex);
  ++actor->gate->finished;
  pthread_cond_broadcast(&actor->gate->cond);
  pthread_mutex_unlock(&actor->gate->mutex);
  return NULL;
}
static void canonical_stream_repair_groups(int native_zero, int durable) {
  commit_gate gate = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_options o = {0};
  ps_space *space;
  ps_stream *streams[4];
  repair_actor actors[4] = {{0}};
  pthread_t threads[4];
  ps_stats before, after;
  unsigned char body[64];
  uint64_t start;
  size_t syncs;
  clean();
  if (native_zero)
    io.zero = gate_zero;
  o.io = &io;
  o.durable_sync = durable;
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  memset(body, 0x75, sizeof(body));
  space = create(PS_MIB, durable, &io);
  for (size_t i = 0; i < 4; ++i) {
    streams[i] = stream_open(space, i + 1, 1, NULL, NULL);
    OK(canonical_append(streams[i], body, sizeof(body), 7, 1, &start, &error));
  }
  OK(ps_checkpoint(space, &error));
  for (size_t i = 0; i < 4; ++i)
    OK(ps_stream_close(streams[i], &error));
  OK(ps_close(space, &error));
  OK(ps_open(path, 0, &o, &space, &error));
  for (size_t i = 0; i < 4; ++i) {
    streams[i] = stream_open(space, i + 1, 0, NULL, NULL);
    actors[i] = (repair_actor){streams[i], &gate, 0};
  }
  OK(ps_status(space, &before, &error));
  syncs = gate.syncs;
  gate.enabled = gate.hold_second = 1;
  CHECK(pthread_create(&threads[0], NULL, stream_repair_worker, &actors[0]) ==
        0);
  await_gate(&gate, 0);
  for (size_t i = 1; i < 4; ++i)
    CHECK(pthread_create(&threads[i], NULL, stream_repair_worker, &actors[i]) ==
          0);
  await_stream_requests(space, before.sync_requests + 4);
  for (size_t i = 0; i < 4; ++i)
    stream_expect(streams[i], 0, body, sizeof(body), 1);
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  await_gate(&gate, 3);
  await_stream_finished(&gate, 1);
  for (size_t i = 0; i < 4; ++i)
    stream_expect(streams[i], 0, body, sizeof(body), 1);
  pthread_mutex_lock(&gate.mutex);
  gate.second_released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  for (size_t i = 0; i < 4; ++i) {
    ps_stream_writer *writer = NULL;
    CHECK(pthread_join(threads[i], NULL) == 0 && actors[i].rc == LC_OK);
    OK(ps_stream_begin(streams[i], &writer, &error));
    ps_stream_writer_close(writer);
    OK(ps_stream_close(streams[i], &error));
  }
  OK(ps_status(space, &after, &error));
  CHECK(gate.syncs == syncs + 2 &&
        after.sync_batches == before.sync_batches + 2);
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0);
  CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
}
static void canonical_stream_open_faults(int native_zero) {
  model m;
  ps_io io = model_init(&m);
  if (native_zero)
    io.zero = model_zero;
  ps_options options = {0};
  ps_space *space = NULL;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  unsigned char *body = malloc(65537), *baseline = malloc(3 * (size_t)PS_MIB);
  uint64_t first, large, last, next;
  size_t kind, phase, count;
  int rc;
  CHECK(body && baseline);
  memset(body, 0x75, 65537);
  options.io = &io;
  options.durable_sync = 1;
  clean();
  space = create(3 * PS_MIB, 1, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(canonical_append(stream, body, 64, 1, 1, &first, &error));
  OK(ps_checkpoint(space, &error));
  OK(canonical_append(stream, body, 65537, 31, 2, &large, &error));
  OK(canonical_append(stream, body, 1024, 7, 3, &last, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  memcpy(baseline, m.stable, 3 * (size_t)PS_MIB);
  for (kind = 0; kind < 3; ++kind) {
    size_t opening_failures = 0, repair_failures = 0;
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      int attempted = 0;
      memcpy(m.stable, baseline, 3 * (size_t)PS_MIB);
      crash(&m);
      m.reads = m.operations = 0;
      m.fail_read = kind == 1 ? phase : 0;
      m.fail_at = kind == 2 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 0 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_open(path, 0, &options, &space, &error);
      stream = NULL;
      if (rc == LC_OK)
        rc = ps_stream_open(space, NULL, stream_key(1), 0, NULL,
                            canonical_recover, NULL, &stream, &error);
      if (rc == LC_OK) {
        attempted = 1;
        rc = ps_stream_begin(stream, &writer, &error);
      }
      ps_stream_writer_close(writer);
      writer = NULL;
      allocation_fail = 0;
      m.fail_at = m.fail_read = 0;
      if (!phase) {
        CHECK(rc == LC_OK);
        count = kind == 0   ? allocation_call
                : kind == 1 ? m.reads
                            : m.operations;
      }
      if (rc != LC_OK) {
        CHECK(rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        if (attempted)
          ++repair_failures;
        else
          ++opening_failures;
        if (kind == 2 && attempted) {
          CHECK(!writer && ps_check(space, NULL) == LC_ERR_TRANSPORT);
          CHECK(ps_stream_begin(stream, &writer, NULL) == LC_ERR_TRANSPORT &&
                !writer);
        }
        lc_error_cleanup(&error);
      }
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      memcpy(m.stable, baseline, 3 * (size_t)PS_MIB);
      crash(&m);
      space = reopen(&io);
      stream = stream_open(space, 1, 0, NULL, NULL);
      stream_expect(stream, first, body, 64, 1);
      stream_expect(stream, large, body, 65537, 2);
      stream_expect(stream, last, body, 1024, 3);
      OK(canonical_append(stream, body, 65, 11, 4, &next, &error));
      stream_expect(stream, next, body, 65, 4);
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
    }
    if (kind == 2)
      CHECK(count > 0 && opening_failures && repair_failures);
    else
      CHECK(count > 5);
    printf("stream open faults: native_zero=%d kind=%zu sites=%zu "
           "opening_failures=%zu repair_failures=%zu\n",
           native_zero, kind, count, opening_failures, repair_failures);
  }
  free(body);
  free(baseline);
  free(m.stable);
  clean();
  puts("streams: every heap/read/repair-I/O failure during container and "
       "canonical-tail recovery, bounded read-ahead and retry");
}
static void canonical_stream_abort_faults(int native_zero) {
  model m;
  ps_io io = model_init(&m);
  if (native_zero)
    io.zero = model_zero;
  ps_space *space;
  ps_stream *stream;
  ps_stream_writer *w;
  ps_stream_info info;
  unsigned char *body = calloc(1, 2 * PS_CHUNK), prefix[43];
  lc_pouch_record_header header;
  uint64_t first, next;
  size_t phase, count = 0;
  CHECK(body != NULL);
  memset(&header, 0, sizeof(header));
  header.type = 1;
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  header.key_len = 3;
  header.meta_len = 8;
  lc_pouch_record_header_encode(&header, prefix);
  memcpy(prefix + 32, "key", 3);
  memset(prefix + 35, 0, 8);
  for (phase = 0; phase <= count; ++phase) {
    clean();
    m.fail_at = m.operations = 0;
    space = create(PS_MIB, 0, &io);
    stream = stream_open(space, 1, 1, NULL, NULL);
    OK(canonical_append(stream, body, 64, 7, 1, &first, &error));
    OK(ps_stream_begin(stream, &w, &error));
    OK(ps_stream_write(w, prefix, sizeof(prefix), &error));
    OK(ps_stream_write(w, body, 2 * PS_CHUNK, &error));
    OK(ps_checkpoint(space, &error));
    m.operations = 0;
    m.fail_at = phase;
    m.tear = m.persist_torn = 1;
    ps_stream_writer_close(w);
    m.fail_at = 0;
    if (!phase)
      count = m.operations;
    if (phase)
      BAD(ps_stream_begin(stream, &w, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    crash(&m);
    space = reopen(&io);
    stream = stream_open(space, 1, 0, NULL, NULL);
    OK(ps_stream_status(stream, &info, &error));
    CHECK(info.tail == 107);
    stream_expect(stream, first, body, 64, 1);
    OK(canonical_append(stream, body, 65, 1, 2, &next, &error));
    OK(ps_checkpoint(space, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
  }
  CHECK(count > 3);
  free(body);
  free(m.stable);
  clean();
  puts("streams: failed abandoned-tail clearing/sync poisons admission, "
       "checkpointed private PENDING data never publishes after crash");
}
static void canonical_stream_private_grant(int reopened) {
  commit_gate gate;
  stream_actor actor;
  model saved;
  ps_io io;
  ps_space *space;
  ps_stream *a, *b;
  ps_stream_info info;
  ps_stats stats;
  pthread_t thread;
  uint64_t start;
  unsigned char body[1024];
  clean();
  memset(&gate, 0, sizeof(gate));
  memset(&actor, 0, sizeof(actor));
  memset(&saved, 0, sizeof(saved));
  saved.stable_bytes = (size_t)(reopened ? 3 * PS_MIB : PS_MIB);
  saved.stable = malloc(saved.stable_bytes);
  CHECK(saved.stable != NULL);
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  io.context = &gate;
  io.read = gate_read;
  io.write = gate_write;
  io.sync = gate_sync;
  io.reserve = gate_reserve;
  io.zero = reopened ? gate_zero : NULL;
  space = create(saved.stable_bytes, 0, &io);
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  if (reopened) {
    OK(ps_stream_close(a, &error));
    OK(ps_stream_close(b, &error));
    OK(ps_close(space, &error));
    space = reopen(&io);
    a = stream_open(space, 1, 0, NULL, NULL);
    b = stream_open(space, 2, 0, NULL, NULL);
  }
  gate.enabled = gate.hold_payload = 1;
  actor.stream = a;
  actor.gate = &gate;
  CHECK(pthread_create(&thread, NULL, stream_commit_worker, &actor) == 0);
  await_gate(&gate, 0); /* A has reserved a private run, header I/O held */
  memset(body, 'B', sizeof(body));
  OK(canonical_append(b, body, sizeof(body), 31, 2, &start, &error));
  stream_expect(b, start, body, sizeof(body), 2);
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &stats, &error));
  CHECK(stats.private_bytes == (reopened ? PS_MIB : PS_PAGE));
  OK(ps_stream_status(a, &info, &error));
  CHECK(!info.tail && !info.capacity);
  CHECK(pread(gate.fd, saved.stable, saved.stable_bytes, 0) ==
        (ssize_t)saved.stable_bytes);
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(thread, NULL) == 0 && actor.rc == LC_OK);
  memset(body, 'A', sizeof(body));
  stream_expect(a, 0, body, sizeof(body), 2);
  memset(body, 'B', sizeof(body));
  stream_expect(b, 0, body, sizeof(body), 2);
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  crash(&saved);
  space = reopen(NULL);
  a = stream_open(space, 1, 0, NULL, NULL);
  b = stream_open(space, 2, 0, NULL, NULL);
  OK(ps_stream_status(a, &info, &error));
  CHECK(!info.tail && !info.capacity);
  memset(body, 'B', sizeof(body));
  stream_expect(b, 0, body, sizeof(body), 2);
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0);
  CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  free(saved.stable);
  clean();
  puts("streams: independent allocation/publication/checkpoint during held "
       "private grant I/O, crash preserves only the published ownership");
}
static void canonical_stream_private_capacity(void) {
  commit_gate gate = {0};
  stream_actor actor = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_catalog_info owner;
  ps_stats stats;
  ps_reader *held[1024] = {0};
  ps_ref latest, next;
  pthread_t thread;
  unsigned char *body = malloc(PS_PAGE), *name;
  unsigned char canonical[1024], expected = 0;
  size_t length, count;
  uint64_t start;
  int rc;
  CHECK(body != NULL);
  memset(body, 0x42, PS_PAGE);
  memset(canonical, 'A', sizeof(canonical));
  clean();
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0 &&
        pthread_cond_init(&gate.cond, NULL) == 0);
  space = create(3 * PS_MIB, 0, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  latest = put(space, state(1), body, PS_PAGE, PS_PAGE);
  gate.enabled = gate.hold_payload = 1;
  actor.stream = stream;
  actor.gate = &gate;
  CHECK(pthread_create(&thread, NULL, stream_commit_worker, &actor) == 0);
  await_gate(&gate, 0);

  /* The pending grant has no descriptor yet. A peer grows the metadata to
   * an exact packed-page boundary and checkpoints while it remains private. */
  OK(ps_status(space, &stats, &error));
  CHECK(stats.metadata_bytes < 32 * 4032 - 104 &&
        stats.private_bytes == PS_PAGE);
  length = 32 * 4032 - (size_t)stats.metadata_bytes - 104;
  CHECK(length <= PS_CATALOG_NAME_MAX);
  name = malloc(length);
  CHECK(name != NULL);
  memset(name, 0x73, length);
  OK(ps_catalog_create_namespace(space, name, length, &owner, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &stats, &error));
  CHECK(stats.metadata_bytes == 32 * 4032 &&
        stats.metadata_available_bytes == stats.metadata_workspace_bytes);
  /* Replacing one fixed-size descriptor consumes payload capacity without
   * changing that boundary. Pin each replaced value so the publication
   * recheck cannot recover headroom by checkpointing it. */
  for (count = 0; count < sizeof(held) / sizeof(*held); ++count) {
    ps_writer *w = NULL;
    OK(ps_read_open(space, state(1), &latest, &held[count], &error));
    body[0] = (unsigned char)count;
    OK(ps_begin(space, NULL, state(1), &w, &error));
    rc = ps_write(w, body, PS_PAGE, &error);
    if (rc == LC_OK)
      rc = ps_publish(w, &next, &error);
    ps_writer_close(w);
    if (rc != LC_OK)
      break;
    latest = next;
    expected = body[0];
  }
  CHECK(count > 100 && count < sizeof(held) / sizeof(*held) &&
        rc == LC_ERR_INVALID && error.message &&
        strstr(error.message, "capacity"));
  lc_error_cleanup(&error);
  body[0] = expected;
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(actor.rc == LC_ERR_INVALID);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(!info.tail && !info.capacity);
  OK(ps_status(space, &stats, &error));
  CHECK(!stats.private_bytes &&
        stats.metadata_available_bytes >= stats.metadata_workspace_bytes);
  expect(space, state(1), &latest, body, PS_PAGE);

  /* Checkpoint retires the peer's old payloads. A fresh attempt can now
   * acquire its descriptor workspace and publish without growth. */
  for (size_t i = 0; i <= count; ++i)
    ps_reader_close(held[i]);
  OK(ps_checkpoint(space, &error));
  OK(canonical_append(stream, canonical, sizeof(canonical), 31, 2, &start,
                      &error));
  stream_expect(stream, start, canonical, sizeof(canonical), 2);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  expect(space, state(1), &latest, body, PS_PAGE);
  stream = stream_open(space, 1, 0, NULL, NULL);
  stream_expect(stream, start, canonical, sizeof(canonical), 2);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0 &&
        pthread_mutex_destroy(&gate.mutex) == 0);
  free(name);
  free(body);
  clean();
  puts("streams: peer growth and capacity exhaustion during private grant I/O "
       "rechecks metadata before publication, reclaims and retries");
}
static void canonical_stream_invalid_metadata(void) {
  ps_space *space;
  ps_stream *stream = NULL;
  unsigned char roots[PS_ROOT_BYTES], *page = malloc(PS_PAGE), body[64] = {0};
  unsigned char *record;
  uint64_t start, ref, offset;
  uint32_t crc;
  size_t phase;
  int fd;
  CHECK(page != NULL);
  for (phase = 0; phase < 4; ++phase) {
    clean();
    space = create(PS_MIB, 1, NULL);
    stream = stream_open(space, 1, 1, NULL, NULL);
    OK(canonical_append(stream, body, sizeof(body), 1, 1, &start, &error));
    OK(ps_checkpoint(space, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    fd = open(path, O_RDWR);
    CHECK(fd >= 0);
    if (phase < 3) {
      CHECK(pread(fd, roots, sizeof(roots), 0) == (ssize_t)sizeof(roots));
      offset = fixture_get64(roots + 32);
      CHECK(pread(fd, page, PS_PAGE, (off_t)offset) == PS_PAGE);
      ref = fixture_get64(page + 64 + 24); /* typed stream head, sorted first */
      offset = ref / PS_PAGE * PS_PAGE;
      CHECK(pread(fd, page, PS_PAGE, (off_t)offset) == PS_PAGE);
      record = page + ref % PS_PAGE;
      CHECK(record[16] == PS_STREAM);
      if (phase == 0)
        fixture_put(record + 48, PS_MIB, 8); /* tail beyond owned capacity */
      else if (phase == 1) {
        fixture_put(record + 64, 2 * (PS_PAGE - PS_RUN_HEADER), 8);
        fixture_put(record + 72, 2,
                    8); /* valid scalars, nonexistent second grant */
      } else
        fixture_put(record + 64, PS_PAGE - PS_RUN_HEADER - 1,
                    8); /* a present run extends beyond the logical span */
      crc = (uint32_t)crc32(0, record, 60);
      fixture_put(record + 60, crc32(crc, record + 64, 16), 4);
      crc = (uint32_t)crc32(0, page, 48);
      fixture_put(page + 48, crc32(crc, page + 52, PS_PAGE - 52), 4);
      CHECK(pwrite(fd, page, PS_PAGE, (off_t)offset) == PS_PAGE);
    } else {
      CHECK(pread(fd, page, 1, PS_DATA) == 1);
      page[0] ^= 1; /* checkpointed run identity, not canonical tail damage */
      CHECK(pwrite(fd, page, 1, PS_DATA) == 1);
    }
    CHECK(fdatasync(fd) == 0 && close(fd) == 0);
    if (phase < 3) {
      CHECK(ps_open(path, 0, NULL, &space, &error) == LC_ERR_PROTOCOL);
      CHECK(space == NULL);
    } else {
      space = reopen(NULL);
      CHECK(ps_stream_open(space, NULL, stream_key(1), 0, NULL,
                           canonical_recover, NULL, &stream,
                           &error) == LC_ERR_PROTOCOL);
      CHECK(stream == NULL);
      OK(ps_close(space, &error));
    }
    lc_error_cleanup(&error);
  }
  free(page);
  clean();
  puts("streams: checked head bounds/run count, complete ownership chain and "
       "checkpointed run identity corruption rejection");
}
static void canonical_stream_checkpoint_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  unsigned char *body = malloc(65553);
  uint64_t first, next;
  size_t kind, phase, count;
  int rc;
  CHECK(body != NULL);
  memset(body, 0x79, 65553);
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      space = create(3 * PS_MIB, 0, &io);
      stream = stream_open(space, 1, 1, NULL, NULL);
      OK(canonical_append(stream, body, 64, 1, 1, &first, &error));
      OK(ps_checkpoint(space, &error));
      OK(canonical_append(stream, body, 65553, 31, 2, &next, &error));
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_checkpoint(space, &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc != LC_OK) {
        CHECK(kind == 0 || rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
        if (kind == 1)
          OK(ps_checkpoint(space, &error));
      }
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      crash(&m);
      space = reopen(&io);
      stream = stream_open(space, 1, 0, NULL, NULL);
      stream_expect(stream, first, body, 64, 1);
      OK(ps_stream_status(stream, &info, &error));
      CHECK(info.tail == 107 || info.tail == 107 + 43 + 65553);
      if (rc == LC_OK || kind == 1)
        CHECK(info.tail != 107);
      if (info.tail != 107)
        stream_expect(stream, next, body, 65553, 2);
      OK(canonical_append(stream, body, 65, 1, 3, &next, &error));
      OK(ps_checkpoint(space, &error));
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
    }
    CHECK(count > 5);
  }
  free(body);
  free(m.stable);
  clean();
  puts("streams: every checkpoint I/O/heap failure with grown grant directory "
       "and advanced canonical tail, abort/retry and crash recovery");
}
static void canonical_stream_capacity(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_stats stats;
  unsigned char body[1024];
  uint64_t offset;
  size_t count;
  int rc = LC_OK;
  memset(body, 0x46, sizeof(body));
  clean();
  space = create(PS_MIB, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  for (count = 0; count < 2000; ++count) {
    rc = canonical_append(stream, body, sizeof(body), 17, count, &offset,
                          &error);
    if (rc != LC_OK)
      break;
  }
  CHECK(rc == LC_ERR_INVALID && count > 100 && count < 2000);
  lc_error_cleanup(&error);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == count * (sizeof(body) + 43));
  OK(ps_status(space, &stats, &error));
  CHECK(!stats.private_bytes && stats.capacity == PS_MIB &&
        stats.allocated_bytes <= PS_MIB - PS_DATA - PS_MAINTENANCE_RESERVE);
  stream_expect(stream, 0, body, sizeof(body), 0);
  stream_expect(stream, (count - 1) * (sizeof(body) + 43), body, sizeof(body),
                count - 1);
  OK(ps_checkpoint(space, &error)); /* exhausted foreground leaves progress */
  OK(ps_grow(space, PS_MIB, &error));
  OK(canonical_append(stream, body, sizeof(body), 7, count, &offset, &error));
  CHECK(offset == info.tail);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, offset, body, sizeof(body), count);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  clean();
  puts("streams: shrinking grant requests, fixed-capacity exhaustion without "
       "poisoning, reserved checkpoint progress and 1 MiB growth");
}
static void canonical_stream_fragmentation(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_reader *held;
  unsigned char *page = malloc(PS_PAGE), *body = malloc(2 * PS_CHUNK);
  size_t i, pages = (PS_MIB - PS_DATA - PS_MAINTENANCE_RESERVE) / PS_PAGE;
  uint64_t offset;
  CHECK(page && body);
  memset(page, 0xa5, PS_PAGE);
  memset(body, 0x69, 2 * PS_CHUNK);
  clean();
  space = create(PS_MIB, 0, NULL);
  for (i = 0; i < pages; ++i)
    put(space, state(i), page, PS_PAGE, 317);
  OK(ps_checkpoint(space, &error));
  OK(ps_read_open(space, state(0), NULL, &held, &error));
  for (i = 0; i < pages; i += 2)
    OK(ps_delete(space, state(i), &error));
  OK(ps_checkpoint(space, &error));
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(canonical_append(stream, body, 2 * PS_CHUNK, 31, 1, &offset, &error));
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.runs >=
        5); /* consume the 16 KiB tail, then isolated 4 KiB holes */
  for (i = 1; i < pages; i += 2)
    expect(space, state(i), NULL, page, PS_PAGE);
  read_value(held, page, PS_PAGE);
  ps_reader_close(held);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, offset, body, 2 * PS_CHUNK, 1);
  for (i = 1; i < pages; i += 2)
    expect(space, state(i), NULL, page, PS_PAGE);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  free(page);
  free(body);
  clean();
  puts("streams: fragmented physical grants, bounded cross-run patch/read, "
       "and independently pinned immutable neighbours across reuse");
}
static void canonical_stream_grown_tail(int native_zero) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_owned_head *head;
  ps_owned_run *run;
  ps_owned_status owned;
  ps_stream *stream;
  ps_stream_info info;
  ps_stats before, after;
  lc_pouch_record_header record = {0};
  unsigned char frame[107], readback[107], body[64];
  uint64_t offset;
  size_t i;
  int fd, found;
  if (native_zero)
    io.zero = model_zero;
  memset(body, 0x63, sizeof(body));
  record.type = 1;
  record.key_len = 3;
  record.meta_len = 8;
  record.payload_len = sizeof(body);
  record.payload_crc = crc32(0, body, sizeof(body));
  lc_pouch_record_header_encode(&record, frame);
  memcpy(frame + 32, "key", 3);
  fixture_put(frame + 35, 999, 8);
  memcpy(frame + 43, body, sizeof(body));
  clean();
  space = create(PS_MIB, 0, &io);
  OK(ps_owned_open(space, NULL, stream_key(1), 1, NULL, &head, &owned, &error));
  OK(ps_checkpoint(space, &error));
  ps_owned_close(head);
  OK(ps_close(space, &error));
  /* Extra file bytes are outside the committed capacity. Preallocation must
   * not turn their existing contents into a durable zero certificate. */
  fd = open(path, O_RDWR);
  CHECK(fd >= 0 && fallocate(fd, 0, PS_MIB, PS_MIB) == 0);
  CHECK(pwrite(fd, frame, sizeof(frame), PS_MIB + PS_RUN_HEADER) ==
        (ssize_t)sizeof(frame));
  CHECK(fdatasync(fd) == 0 && close(fd) == 0);
  space = reopen(&io);
  OK(ps_status(space, &before, &error));
  CHECK(before.capacity == PS_MIB);
  OK(ps_owned_open(space, NULL, stream_key(1), 0, NULL, &head, &owned, &error));
  OK(ps_grow(space, PS_MIB, &error));
  OK(ps_status(space, &before, &error));
  OK(ps_owned_grant(head, NULL, 0, PS_CHUNK, &run, &found, &error));
  CHECK(found && ps_owned_run_capacity(run) == PS_CHUNK - PS_RUN_HEADER);
  OK(ps_owned_read(run, 0, readback, sizeof(readback), &error));
  for (i = 0; i < sizeof(readback); ++i)
    CHECK(readback[i] == 0 && m.stable[PS_MIB + PS_RUN_HEADER + i] == 0);
  OK(ps_status(space, &after, &error));
  CHECK(after.syncs == before.syncs + 1);
  OK(ps_owned_barrier(head, &error));
  ps_owned_run_close(run);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  space = reopen(&io);
  stream = stream_open(space, 1, 0, NULL, NULL);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == 0);
  OK(canonical_append(stream, body, sizeof(body), 7, 1, &offset, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  stream = stream_open(space, 1, 0, NULL, NULL);
  stream_expect(stream, offset, body, sizeof(body), 1);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(offset == 0 && info.tail == sizeof(frame));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  free(m.stable);
  clean();
  puts("streams: growth never certifies pre-existing trailing bytes as zero; "
       "valid stale records cannot recover into a new grant");
}
static void canonical_stream_prepared_pool(int zero_mode) {
  model m;
  ps_io io = model_init(&m);
  if (zero_mode)
    io.zero = model_zero;
  m.unsupported_zero = zero_mode == 2;
  m.interrupt_zero = zero_mode == 1;
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_stats before, after;
  unsigned char *old = malloc((size_t)PS_MIB);
  uint64_t offset;
  size_t i;
  CHECK(old != NULL);
  memset(old, 0x69, (size_t)PS_MIB);
  clean();
  space = create(3 * PS_MIB, 0, &io);
  put(space, state(1), old, (size_t)PS_MIB, PS_CHUNK);
  OK(ps_checkpoint(space, &error));
  OK(ps_delete(space, state(1), &error));
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  stream = stream_open(space, 1, 0, NULL, NULL);
  OK(ps_status(space, &before, &error));
  allocation_max = 0;
  measure_allocations = 1;
  for (i = 0; i < 100; ++i)
    OK(canonical_append(stream, old, 1024, 31, i, &offset, &error));
  measure_allocations = 0;
  CHECK(allocation_max <= PS_CHUNK + 256);
  OK(ps_status(space, &after, &error));
  OK(ps_stream_status(stream, &info, &error));
  CHECK(after.syncs == before.syncs + 1 &&
        info.runs <= 2 + info.tail / (PS_STREAM_PIECE_BYTES - PS_RUN_HEADER) &&
        info.capacity - info.tail < PS_STREAM_PIECE_BYTES);
  CHECK(zero_mode ? m.zero_calls > 0 : m.zero_calls == 0);
  CHECK(info.tail == 100 * (1024 + 43) && !after.private_bytes);
  /* All spare pages were certified only after the first preparation sync.
   * Subsequent NoSync records do not change that durable zero snapshot. */
  for (i = PS_RUN_HEADER; i < (size_t)PS_MIB; ++i)
    CHECK(m.stable[PS_DATA + i] == 0);
  stream_expect(stream, 0, old, 1024, 0);
  stream_expect(stream, offset, old, 1024, 99);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  stream = stream_open(space, 1, 0, NULL, NULL);
  stream_expect(stream, offset, old, 1024, 99);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  free(old);
  free(m.stable);
  clean();
  puts("streams: one bounded preparation certifies shared spare pages for "
       "multiple grants, zero provenance and constant producer buffers");
}
static void canonical_stream_preparation_faults(int native_zero, int funded,
                                                int durable) {
  model m;
  ps_io io = model_init(&m);
  if (native_zero)
    io.zero = model_zero;
  ps_options options = {0};
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  unsigned char *body = malloc(PS_CHUNK + 333);
  uint64_t first, next;
  size_t kind, phase, count;
  int rc;
  CHECK(body != NULL);
  memset(body, 0x79, PS_CHUNK + 333);
  options.io = &io;
  options.durable_sync = durable;
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      space = create(3 * PS_MIB, durable, &io);
      stream = stream_open(space, 1, 1, NULL, NULL);
      OK(canonical_append(stream, body, 64, 1, 1, &first, &error));
      OK(ps_checkpoint(space, &error));
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      OK(ps_open(path, 0, &options, &space, &error));
      stream = stream_open(space, 1, 0, NULL, NULL);
      OK(ps_checkpoint(space, &error));
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      ps_budget *budget = NULL;
      ps_stream_writer *reservation = NULL;
      ps_budget_plan plan = {0};
      rc = LC_OK;
      if (funded) {
        rc = ps_budget_plan_span(&plan, PS_CHUNK + 333 + 43, &error);
        if (rc == LC_OK)
          rc = ps_budget_open(space, &plan, &budget, &error);
        if (rc == LC_OK)
          rc = ps_stream_begin(stream, &reservation, &error);
        if (rc == LC_OK)
          rc = ps_stream_reserve_exact(reservation, budget, PS_CHUNK + 333 + 43,
                                       &error);
        ps_stream_writer_close(reservation);
      }
      if (rc == LC_OK)
        rc = canonical_append(stream, body, PS_CHUNK + 333, 31, 2, &next,
                              &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc != LC_OK) {
        CHECK(kind == 0 || rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
      }
      OK(ps_budget_close(budget, &error));
      if (funded && kind && rc != LC_OK) {
        /* Healthy allocation failure must not require reopening the owner to
         * admit another complete reservation. No new canonical bytes here. */
        OK(ps_check(space, &error));
        budget = NULL;
        reservation = NULL;
        OK(ps_budget_open(space, &plan, &budget, &error));
        OK(ps_stream_begin(stream, &reservation, &error));
        OK(ps_stream_reserve_exact(reservation, budget, PS_CHUNK + 333 + 43,
                                   &error));
        ps_stream_writer_close(reservation);
        OK(ps_budget_close(budget, &error));
      }
      if (rc == LC_OK)
        stream_expect(stream, next, body, PS_CHUNK + 333, 2);
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      crash(&m);
      space = reopen(&io);
      stream = stream_open(space, 1, 0, NULL, NULL);
      stream_expect(stream, first, body, 64, 1);
      OK(ps_stream_status(stream, &info, &error));
      CHECK(info.tail == 107 || info.tail == 107 + PS_CHUNK + 333 + 43);
      if (rc == LC_OK && durable)
        CHECK(info.tail != 107);
      if (info.tail != 107)
        stream_expect(stream, next, body, PS_CHUNK + 333, 2);
      OK(canonical_append(stream, body, 65, 1, 3, &next, &error));
      OK(ps_checkpoint(space, &error));
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
    }
    CHECK(count > 5);
    if (funded)
      printf("funded grant failure cuts (zero=%d durable=%d %s): %lu\n",
             native_zero, durable, kind ? "allocation" : "write/sync",
             (unsigned long)count);
  }
  free(body);
  free(m.stable);
  clean();
  puts("streams: every preparation-pool zero/header/sync/publication I/O and "
       "heap failure, no early spare reuse and usable post-crash ownership");
}

static void stream_reader_expect(ps_stream_reader *r, const void *bytes,
                                 size_t n) {
  unsigned char buffer[317];
  size_t at = 0, got;
  do {
    OK(ps_stream_read(r, buffer, sizeof(buffer), &got, &error));
    CHECK(got <= n - at &&
          (!got || !memcmp(buffer, (const unsigned char *)bytes + at, got)));
    at += got;
  } while (got);
  CHECK(at == n);
}
static void stream_lifecycle(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *old, *fresh, *missing = NULL;
  ps_stream_writer *w = NULL;
  ps_stream_reader *held = NULL;
  ps_stream_info info;
  ps_stats before, after;
  ps_ref prior, current;
  recovery_trace trace = {0};
  unsigned char *body = malloc(PS_CHUNK * 4 + 17);
  uint64_t offset;
  CHECK(body != NULL);
  memset(body, 0x37, PS_CHUNK * 4 + 17);
  clean();
  space = create(3 * PS_MIB, durable, &io);
  old = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_begin(old, &w, &error));
  BAD(ps_stream_seal(old, &error));
  BAD(ps_stream_retire(old, &error));
  ps_stream_writer_close(w);
  OK(canonical_append(old, body, PS_CHUNK * 4 + 17, 31, 1, &offset, &error));
  OK(ps_stream_status(old, &info, &error));
  prior = info.ref;
  OK(ps_status(space, &before, &error));
  OK(ps_stream_seal(old, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.journal_bytes == before.journal_bytes + 80 &&
        after.syncs == before.syncs + (durable ? 1 : 2));
  OK(ps_stream_seal(old, &error));
  OK(ps_status(space, &before, &error));
  CHECK(before.journal_bytes == after.journal_bytes &&
        before.syncs == after.syncs);
  BAD(ps_stream_begin(old, &w, &error));
  stream_expect(old, offset, body, PS_CHUNK * 4 + 17, 1);
  OK(ps_stream_close(old, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  m.read_bytes = 0;
  old = stream_open(space, 1, 0, &prior, &trace);
  OK(ps_stream_status(old, &info, &error));
  CHECK(info.sealed && !info.retired && !trace.bytes &&
        m.read_bytes == info.runs * PS_RUN_HEADER);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(old, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  OK(ps_stream_open(space, NULL, stream_key(1), 0, &prior, NULL, NULL, &old,
                    &error));
  stream_expect(old, offset, body, PS_CHUNK * 4 + 17, 1);
  OK(ps_stream_read_open(old, 43, PS_CHUNK * 4 + 17, &held, &error));
  OK(ps_status(space, &before, &error));
  OK(ps_stream_retire(old, &error));
  OK(ps_status(space, &after, &error));
  CHECK(!after.entries && !after.live_bytes &&
        after.retired_bytes == before.allocated_bytes &&
        after.journal_bytes == before.journal_bytes + 64 &&
        after.syncs == before.syncs + 1);
  OK(ps_stream_retire(old, &error));
  BAD(ps_stream_seal(old, &error));
  BAD(ps_stream_begin(old, &w, &error));
  ps_stream_reader *rejected = NULL;
  BAD(ps_stream_read_open(old, 0, 1, &rejected, &error));
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, &prior, canonical_recover,
                     NULL, &missing, &error));
  fresh = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_status(fresh, &info, &error));
  current = info.ref;
  CHECK(current.incarnation != prior.incarnation && !info.sealed &&
        !info.retired);
  OK(canonical_append(fresh, body, 64, 7, 2, &offset, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.entries == 2 && after.retired_bytes > 0);
  stream_reader_expect(held, body, PS_CHUNK * 4 + 17);
  ps_stream_reader_close(held);
  OK(ps_stream_close(old, &error));
  OK(ps_status(space, &after, &error));
  CHECK(!after.retired_bytes);
  OK(ps_stream_close(fresh, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  BAD(ps_stream_open(space, NULL, stream_key(1), 0, &prior, canonical_recover,
                     NULL, &missing, &error));
  fresh = stream_open(space, 1, 0, &current, NULL);
  stream_expect(fresh, offset, body, 64, 2);
  OK(ps_stream_retire(fresh, &error));
  OK(ps_stream_close(fresh, &error));
  OK(ps_checkpoint(space, &error));
  fresh = stream_open(space, 2, 1, NULL, NULL);
  OK(ps_stream_seal(fresh, &error));
  OK(ps_stream_close(fresh, &error));
  OK(ps_close(space, &error));
  space = reopen(&io);
  OK(ps_stream_open(space, NULL, stream_key(2), 0, NULL, NULL, NULL, &fresh,
                    &error));
  OK(ps_stream_status(fresh, &info, &error));
  CHECK(info.sealed && !info.tail && !info.capacity);
  OK(ps_stream_retire(fresh, &error));
  OK(ps_stream_close(fresh, &error));
  OK(ps_close(space, &error));
  free(body);
  free(m.stable);
  clean();
  puts("lifecycle: durable seal/retirement, fixed sealed tail, zero body scan, "
       "reader pins, rebinding and empty streams");
}
static void stream_lifecycle_faults(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream, *found = NULL;
  ps_stream_info info;
  ps_stats stats;
  unsigned char *body = malloc(PS_CHUNK * 2 + 17);
  unsigned char event[80];
  uint64_t first, second;
  size_t operation, kind, phase, count, event_bytes;
  int rc, opened;
  CHECK(body != NULL);
  memset(body, 0x39, PS_CHUNK * 2 + 17);
  for (operation = 0; operation < 2; ++operation)
    for (kind = 0; kind < 2; ++kind) {
      count = 0;
      for (phase = 0; phase <= count; ++phase) {
        clean();
        m.fail_at = m.operations = 0;
        space = create(3 * PS_MIB, durable, &io);
        stream = stream_open(space, 1, 1, NULL, NULL);
        OK(canonical_append(stream, body, PS_CHUNK * 2 + 17, 31, 1, &first,
                            &error));
        OK(ps_checkpoint(space, &error));
        OK(canonical_append(stream, body, 1027, 7, 2, &second, &error));
        OK(ps_status(space, &stats, &error));
        m.operations = 0;
        m.fail_at = kind == 0 ? phase : 0;
        m.tear = m.persist_torn = 1;
        allocation_call = 0;
        allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
        rc = operation ? ps_stream_retire(stream, &error)
                       : ps_stream_seal(stream, &error);
        allocation_fail = 0;
        m.fail_at = 0;
        if (!phase)
          count = kind == 0 ? m.operations : allocation_call;
        if (rc != LC_OK) {
          CHECK(kind == 0 || rc == LC_ERR_NOMEM);
          lc_error_cleanup(&error);
        }
        /* Persist a complete lifecycle event ahead of its failed final sync.
         * Seal's first barrier must already cover the new canonical body. */
        if (kind == 0 && phase == count) {
          int fd = open(path, O_RDONLY);
          event_bytes = operation ? 64 : 80;
          CHECK(fd >= 0 && pread(fd, event, event_bytes,
                                 PS_JOURNAL_OFFSET + stats.journal_bytes) ==
                               (ssize_t)event_bytes);
          CHECK(close(fd) == 0);
          memcpy(m.stable + PS_JOURNAL_OFFSET + stats.journal_bytes, event,
                 event_bytes);
        }
        OK(ps_stream_close(stream, &error));
        OK(ps_close(space, &error));
        crash(&m);
        space = reopen(&io);
        opened = ps_stream_open(space, NULL, stream_key(1), 0, NULL,
                                canonical_recover, NULL, &found, &error);
        if (opened == LC_OK) {
          CHECK(!operation || rc != LC_OK);
          stream_expect(found, first, body, PS_CHUNK * 2 + 17, 1);
          OK(ps_stream_status(found, &info, &error));
          CHECK(info.tail == second || info.tail == second + 1070);
          if (durable)
            CHECK(info.tail == second + 1070);
          if (info.sealed || rc == LC_OK) {
            CHECK(!operation && info.sealed && info.tail == second + 1070);
            stream_expect(found, second, body, 1027, 2);
          }
          if (kind == 0 && phase == count && !operation)
            CHECK(info.sealed);
          OK(ps_stream_retire(found, &error));
          OK(ps_stream_close(found, &error));
        } else {
          CHECK(operation && opened == LC_ERR_INVALID && found == NULL);
          lc_error_cleanup(&error);
        }
        found = stream_open(space, 1, 1, NULL, NULL);
        OK(canonical_append(found, body, 65, 7, 3, &first, &error));
        OK(ps_stream_close(found, &error));
        OK(ps_close(space, &error));
        /* A second replay must retain the old journal's verification bodies
         * even after deletion/rebinding, until checkpoint retirement. */
        space = reopen(&io);
        found = stream_open(space, 1, 0, NULL, NULL);
        stream_expect(found, first, body, 65, 3);
        OK(ps_checkpoint(space, &error));
        OK(ps_stream_close(found, &error));
        OK(ps_close(space, &error));
      }
      CHECK(count >= 1);
    }
  free(body);
  free(m.stable);
  clean();
  puts("lifecycle: each seal/retirement I/O and heap fault, torn events, "
       "metadata-before-body persistence, replay after rebinding");
}
static void stream_lifecycle_checkpoint_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *old, *fresh;
  ps_stream_reader *held;
  ps_stream_info info;
  ps_ref ref;
  unsigned char *body = malloc(PS_CHUNK * 2 + 17);
  uint64_t offset;
  size_t kind, phase, count;
  int rc;
  CHECK(body != NULL);
  memset(body, 0x43, PS_CHUNK * 2 + 17);
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      space = create(3 * PS_MIB, 1, &io);
      old = stream_open(space, 1, 1, NULL, NULL);
      OK(canonical_append(old, body, PS_CHUNK * 2 + 17, 31, 1, &offset,
                          &error));
      OK(ps_checkpoint(space, &error));
      OK(ps_stream_read_open(old, 43, PS_CHUNK * 2 + 17, &held, &error));
      OK(ps_stream_seal(old, &error));
      OK(ps_stream_retire(old, &error));
      fresh = stream_open(space, 1, 1, NULL, NULL);
      OK(canonical_append(fresh, body, 64, 7, 2, &offset, &error));
      OK(ps_stream_status(fresh, &info, &error));
      ref = info.ref;
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_checkpoint(space, &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc == LC_ERR_NOMEM) {
        lc_error_cleanup(&error);
        stream_reader_expect(held, body, PS_CHUNK * 2 + 17);
        OK(ps_checkpoint(space, &error));
      } else if (rc != LC_OK)
        lc_error_cleanup(&error);
      else
        stream_reader_expect(held, body, PS_CHUNK * 2 + 17);
      ps_stream_reader_close(held);
      OK(ps_stream_close(old, &error));
      OK(ps_stream_close(fresh, &error));
      OK(ps_close(space, &error));
      crash(&m);
      space = reopen(&io);
      fresh = stream_open(space, 1, 0, &ref, NULL);
      stream_expect(fresh, offset, body, 64, 2);
      OK(ps_stream_retire(fresh, &error));
      OK(ps_stream_close(fresh, &error));
      OK(ps_checkpoint(space, &error));
      OK(ps_close(space, &error));
    }
    CHECK(count > 5);
  }
  free(body);
  free(m.stable);
  clean();
  puts("lifecycle: each checkpoint I/O/heap fault with retired pinned runs and "
       "rebound head; acknowledged new incarnation survives");
}
static void stream_lifecycle_capacity(void) {
  ps_space *space;
  ps_owned_head *head;
  ps_owned_run *run = NULL;
  ps_owned_status status;
  ps_stats before, after;
  size_t count = 0;
  uint64_t base = 0;
  int found, rc;
  clean();
  space = create(PS_MIB, 0, NULL);
  OK(ps_owned_open(space, NULL, stream_key(1), 1, NULL, &head, &status,
                   &error));
  BAD(ps_owned_commit(head, 0, (enum ps_commit_mode)2, NULL, NULL, &error));
  for (;;) {
    rc = ps_owned_grant(head, NULL, base, PS_PAGE, &run, &found, &error);
    if (rc != LC_OK)
      break;
    CHECK(found);
    base += ps_owned_run_capacity(run);
    ps_owned_run_close(run);
    ++count;
  }
  CHECK(rc == LC_ERR_INVALID && count > 100);
  lc_error_cleanup(&error);
  OK(ps_owned_seal(head, &error));
  BAD(ps_owned_recovered(head, 0, &error));
  BAD(ps_owned_commit(head, 0, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  OK(ps_status(space, &before, &error));
  allocation_max = allocation_call = 0;
  measure_allocations = 1;
  allocation_fail = (size_t)-1;
  OK(ps_owned_retire(head, &error));
  allocation_fail = 0;
  measure_allocations = 0;
  CHECK(allocation_call == 1 && allocation_max == 64);
  BAD(ps_owned_recovered(head, 0, &error));
  BAD(ps_owned_commit(head, 0, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.journal_bytes == before.journal_bytes + 64 && !after.entries &&
        after.retired_bytes == before.allocated_bytes);
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(!after.allocated_bytes && !after.retired_bytes);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  space = reopen(NULL);
  OK(ps_status(space, &after, &error));
  CHECK(!after.entries && !after.allocated_bytes);
  OK(ps_owned_open(space, NULL, stream_key(1), 1, NULL, &head, &status,
                   &error));
  OK(ps_owned_grant(head, NULL, 0, PS_PAGE, &run, &found, &error));
  CHECK(found);
  ps_owned_run_close(run);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  clean();
  puts("lifecycle: full capacity, hundreds of grants retired with one 64-byte "
       "event, bounded allocation and complete reuse");
}
typedef struct lifecycle_actor {
  ps_stream *stream;
  commit_gate *gate;
  int retirement, rc;
} lifecycle_actor;
static void *lifecycle_worker(void *context) {
  lifecycle_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  actor->rc = actor->retirement ? ps_stream_retire(actor->stream, &e)
                                : ps_stream_seal(actor->stream, &e);
  lc_error_cleanup(&e);
  pthread_mutex_lock(&actor->gate->mutex);
  ++actor->gate->finished;
  pthread_cond_broadcast(&actor->gate->cond);
  pthread_mutex_unlock(&actor->gate->mutex);
  return NULL;
}
static void stream_lifecycle_concurrency(int retirement) {
  commit_gate gate = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  lifecycle_actor actor = {0};
  stream_actor other_actor = {0};
  ps_space *space;
  ps_stream *stream, *other, *rejected = NULL;
  ps_stream_writer *busy = NULL;
  ps_stream_reader *held, *reader = NULL;
  ps_stream_info info;
  ps_stats before, after;
  pthread_t threads[2];
  unsigned char body[1024];
  uint64_t offset;
  size_t syncs;
  clean();
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  space = create(PS_MIB, 1, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  other = stream_open(space, 2, 1, NULL, NULL);
  memset(body, 'O', sizeof(body));
  OK(canonical_append(stream, body, 64, 7, 1, &offset, &error));
  OK(canonical_append(other, body, 64, 7, 1, &offset, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_read_open(stream, 43, 64, &held, &error));
  OK(ps_status(space, &before, &error));
  actor.stream = stream;
  actor.gate = &gate;
  actor.retirement = retirement;
  other_actor.stream = other;
  other_actor.gate = &gate;
  other_actor.ordinal = 1;
  syncs = gate.syncs;
  gate.enabled = gate.hold_second = 1;
  CHECK(pthread_create(&threads[0], NULL, lifecycle_worker, &actor) == 0);
  await_gate(&gate, 0);
  CHECK(pthread_create(&threads[1], NULL, stream_commit_worker, &other_actor) ==
        0);
  await_stream_requests(space, before.sync_requests + 2);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(!info.sealed && !info.retired);
  stream_expect(stream, 0, body, 64, 1);
  BAD(ps_stream_begin(stream, &busy, &error));
  BAD(ps_stream_open(space, NULL, stream_key(1), 1, NULL, canonical_recover,
                     NULL, &rejected, &error));
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  await_gate(&gate, 3);
  await_stream_finished(&gate, 1);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(retirement ? info.retired : info.sealed);
  OK(ps_stream_status(other, &info, &error));
  CHECK(info.tail == 107);
  if (retirement)
    BAD(ps_stream_read_open(stream, 43, 64, &reader, &error));
  else {
    OK(ps_stream_read_open(stream, 43, 64, &reader, &error));
    ps_stream_reader_close(reader);
  }
  pthread_mutex_lock(&gate.mutex);
  gate.second_released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(threads[0], NULL) == 0);
  CHECK(pthread_join(threads[1], NULL) == 0);
  CHECK(actor.rc == LC_OK && other_actor.rc == LC_OK &&
        gate.syncs == syncs + 2);
  OK(ps_status(space, &after, &error));
  CHECK(after.sync_batches == before.sync_batches + 2);
  if (!retirement)
    OK(ps_stream_retire(stream, &error));
  OK(ps_checkpoint(space, &error));
  stream_reader_expect(held, body, 64);
  ps_stream_reader_close(held);
  OK(ps_stream_close(stream, &error));
  memset(body, 'B', sizeof(body));
  stream_expect(other, 107, body, sizeof(body), 2);
  OK(ps_stream_close(other, &error));
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0);
  CHECK(pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
  puts("lifecycle: held transition barriers, independent ready producer, exact "
       "prefix visibility, pinned reader and no early rebinding");
}
static void stream_lifecycle_invalid_events(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_stats stats;
  unsigned char body[64], wire[80];
  uint64_t offset, creation_position;
  size_t phase;
  int fd;
  memset(body, 0x47, sizeof(body));
  for (phase = 0; phase < 8; ++phase) {
    clean();
    space = create(PS_MIB, 0, NULL);
    OK(ps_status(space, &stats, &error));
    creation_position = stats.journal_bytes;
    stream = stream_open(space, 1, 1, NULL, NULL);
    if (phase != 7)
      OK(ps_checkpoint(space, &error));
    OK(canonical_append(stream, body, sizeof(body), 7, 1, &offset, &error));
    OK(ps_status(space, &stats, &error));
    OK(ps_stream_seal(stream, &error));
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    off_t at = PS_JOURNAL_OFFSET +
               (phase == 7 ? creation_position : stats.journal_bytes);
    fd = open(path, O_RDWR);
    CHECK(fd >= 0 &&
          pread(fd, wire, sizeof(wire), at) == (ssize_t)sizeof(wire));
    if (phase == 0)
      fixture_put(wire + 56, 2, 4);
    else if (phase == 1)
      fixture_put(wire + 48, fixture_get64(wire + 64) + 1, 8);
    else if (phase == 2)
      fixture_put(wire + 56, 0, 4);
    else if (phase == 3)
      fixture_put(wire + 16, PS_STATE, 4);
    else if (phase == 4)
      fixture_put(wire + 40, fixture_get64(wire + 40) + 1, 8);
    else if (phase == 5)
      fixture_put(wire + 64, fixture_get64(wire + 64) + PS_PAGE, 8);
    else if (phase == 6)
      fixture_put(wire + 72, 2, 8);
    else
      fixture_put(wire + 56, 1,
                  4); /* sealed creation cannot bypass dependency ordering */
    fixture_entry_crc(wire);
    CHECK(pwrite(fd, wire, sizeof(wire), at) == (ssize_t)sizeof(wire));
    CHECK(fdatasync(fd) == 0 && close(fd) == 0);
    if (phase == 4 || phase == 5 || phase == 7) {
      CHECK(ps_open(path, 0, NULL, &space, &error) == LC_ERR_PROTOCOL &&
            space == NULL);
      lc_error_cleanup(&error);
    } else {
      space = reopen(NULL);
      stream = stream_open(space, 1, 0, NULL, NULL);
      OK(ps_stream_status(stream, &info, &error));
      CHECK(!info.sealed && info.tail == 107);
      stream_expect(stream, 0, body, sizeof(body), 1);
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
    }
  }
  clean();
  puts("lifecycle: malformed checksummed seal flags/bounds/type/count, "
       "identity mismatch and sealed-creation bypass rejected");
}
static void stream_lifecycle_rebind_rollover(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stats before, after;
  ps_stream_info info;
  ps_ref ref;
  uint64_t offset, checkpoints;
  clean();
  space = create(3 * PS_MIB, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_retire(stream, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_status(space, &before, &error));
  checkpoints = before.checkpoints;
  while (PS_JOURNAL - before.journal_bytes >= 88) {
    put(space, state(1), "x", 1, 1);
    OK(ps_status(space, &before, &error));
  }
  CHECK(PS_JOURNAL - before.journal_bytes < 80 &&
        before.checkpoints == checkpoints);
  /* Creating the new head checkpoints and collects the unpinned old
   * tombstone before installing the replacement. */
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_status(space, &after, &error));
  CHECK(after.checkpoints == checkpoints + 1);
  OK(canonical_append(stream, "new", 3, 1, 1, &offset, &error));
  OK(ps_stream_status(stream, &info, &error));
  ref = info.ref;
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  stream = stream_open(space, 1, 0, &ref, NULL);
  stream_expect(stream, offset, "new", 3, 1);
  expect(space, state(1), NULL, "x", 1);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  clean();
  puts("lifecycle: rebinding checkpoints a full journal and collects the "
       "previous unpinned tombstone safely");
}

static ps_catalog_info catalog_expect(ps_space *space, uint64_t owner,
                                      unsigned int kind, const void *name,
                                      size_t length, uint64_t target) {
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  OK(ps_catalog_get(space, owner, kind, name, length, &view, &error));
  CHECK(view != NULL);
  OK(ps_catalog_view_info(view, &info, &error));
  CHECK(info.stream.kind == PS_STREAM && info.stream.owner == 10 &&
        info.stream.key == target);
  ps_catalog_view_close(view);
  return info;
}
static void catalog_basic(int durable) {
  ps_space *space;
  ps_stream *a, *b;
  ps_stream_info ai, bi;
  ps_catalog_info first, second, out;
  ps_catalog_view *held = NULL, *missing = NULL;
  ps_stats before, after;
  uint64_t owner, next, offset;
  ps_ref wrong;
  unsigned char binary[] = {'a', 0, '/', 0xff};
  clean();
  space = create(3 * PS_MIB, durable, NULL);
  OK(ps_catalog_allocate_id(space, &owner, &error));
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  OK(canonical_append(a, "old", 3, 1, 1, &offset, &error));
  OK(canonical_append(b, "new", 3, 1, 2, &offset, &error));
  OK(ps_stream_status(a, &ai, &error));
  OK(ps_stream_status(b, &bi, &error));
  OK(ps_catalog_get(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                    &missing, &error));
  CHECK(!missing);
  OK(ps_status(space, &before, &error));
  OK(ps_catalog_put(space, NULL, owner, PS_CATALOG_COLLECTION, binary,
                    sizeof(binary), NULL, stream_key(1), &ai.ref, &first,
                    &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.journal_bytes == before.journal_bytes + 104 + sizeof(binary) &&
        after.syncs == before.syncs + 1);
  catalog_expect(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                 1);
  BAD(ps_stream_retire(a, &error));
  BAD(ps_catalog_put(space, NULL, owner, PS_CATALOG_COLLECTION, binary,
                     sizeof(binary), NULL, stream_key(2), &bi.ref, &out,
                     &error));
  wrong = first.revision;
  ++wrong.incarnation;
  BAD(ps_catalog_remove(space, owner, PS_CATALOG_COLLECTION, binary,
                        sizeof(binary), &wrong, &error));
  wrong = ai.ref;
  wrong.container[0] ^= 1;
  BAD(ps_catalog_put(space, NULL, owner, PS_CATALOG_SEGMENT, binary,
                     sizeof(binary), NULL, stream_key(1), &wrong, &out,
                     &error));
  OK(ps_catalog_get(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                    &held, &error));
  BAD(ps_close(space, &error));
  OK(ps_catalog_put(space, NULL, owner, PS_CATALOG_COLLECTION, binary,
                    sizeof(binary), &first.revision, stream_key(2), &bi.ref,
                    &second, &error));
  CHECK(second.id == first.id &&
        second.revision.incarnation != first.revision.incarnation);
  BAD(ps_stream_retire(a, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_catalog_view_info(held, &out, &error));
  CHECK(out.target.incarnation == ai.ref.incarnation &&
        out.revision.incarnation == first.revision.incarnation);
  ps_catalog_view_close(held);
  OK(ps_stream_retire(a, &error));
  OK(ps_stream_close(a, &error));
  catalog_expect(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                 2);
  OK(ps_catalog_put(space, NULL, owner, PS_CATALOG_SEGMENT, NULL, 0, NULL,
                    stream_key(2), &bi.ref, &out, &error));
  OK(ps_catalog_remove(space, owner, PS_CATALOG_SEGMENT, NULL, 0, &out.revision,
                       &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  b = stream_open(space, 2, 0, &bi.ref, NULL);
  catalog_expect(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                 2);
  BAD(ps_stream_retire(b, &error));
  OK(ps_catalog_remove(space, owner, PS_CATALOG_COLLECTION, binary,
                       sizeof(binary), &second.revision, &error));
  OK(ps_catalog_get(space, owner, PS_CATALOG_COLLECTION, binary, sizeof(binary),
                    &missing, &error));
  CHECK(!missing);
  OK(ps_catalog_put(space, NULL, owner, PS_CATALOG_COLLECTION, binary,
                    sizeof(binary), NULL, stream_key(2), &bi.ref, &out,
                    &error));
  CHECK(out.id != second.id &&
        out.revision.incarnation != second.revision.incarnation);
  OK(ps_catalog_remove(space, owner, PS_CATALOG_COLLECTION, binary,
                       sizeof(binary), &out.revision, &error));
  OK(ps_stream_retire(b, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_catalog_allocate_id(space, &next, &error));
  CHECK(next > owner);
  OK(ps_status(space, &after, &error));
  CHECK(!after.entries && !after.live_bytes && !after.allocated_bytes);
  OK(ps_close(space, &error));
  space = reopen(NULL);
  OK(ps_catalog_allocate_id(space, &owner, &error));
  CHECK(owner > next);
  OK(ps_close(space, &error));
  clean();
  puts("catalog: typed byte names, exact revisions, durable publication, "
       "pinned targets, rebinding, recovery and owner IDs");
}
static void catalog_scale(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info target;
  ps_catalog_info info;
  char name[80];
  unsigned char *large = malloc(PS_CHUNK + 17);
  size_t i;
  CHECK(large != NULL);
  memset(large, 0x6a, PS_CHUNK + 17);
  clean();
  space = create(PS_DEFAULT_CAPACITY, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_status(stream, &target, &error));
  for (i = 0; i < 2000; ++i) {
    snprintf(name, sizeof(name), "name/%lu", (unsigned long)i);
    OK(ps_catalog_put(space, NULL, i % 7, PS_CATALOG_COLLECTION, name,
                      strlen(name), NULL, stream_key(1), &target.ref, &info,
                      &error));
  }
  OK(ps_catalog_put(space, NULL, 99, PS_CATALOG_CONTROL, large, PS_CHUNK + 17,
                    NULL, stream_key(1), &target.ref, &info, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  stream = stream_open(space, 1, 0, &target.ref, NULL);
  for (i = 0; i < 2000; ++i) {
    snprintf(name, sizeof(name), "name/%lu", (unsigned long)i);
    info = catalog_expect(space, i % 7, PS_CATALOG_COLLECTION, name,
                          strlen(name), 1);
    OK(ps_catalog_remove(space, i % 7, PS_CATALOG_COLLECTION, name,
                         strlen(name), &info.revision, &error));
  }
  info = catalog_expect(space, 99, PS_CATALOG_CONTROL, large, PS_CHUNK + 17, 1);
  OK(ps_catalog_remove(space, 99, PS_CATALOG_CONTROL, large, PS_CHUNK + 17,
                       &info.revision, &error));
  OK(ps_stream_retire(stream, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_close(space, &error));
  free(large);
  clean();
  puts("catalog: growing name index, two thousand typed names, spanning "
       "metadata, journal rollover and complete reclamation");
}

static void catalog_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *a, *b;
  ps_stream_info ai, bi;
  ps_catalog_info original, changed, seen;
  ps_catalog_view *view = NULL;
  ps_stats stats;
  unsigned char event[108];
  char pad[32];
  uint64_t offset;
  size_t operation, kind, phase, count, i, bytes;
  int rc;
  for (operation = 0; operation < 4; ++operation)
    for (kind = 0; kind < 2; ++kind) {
      count = 0;
      for (phase = 0; phase <= count; ++phase) {
        clean();
        m.operations = m.fail_at = 0;
        space = create(3 * PS_MIB, 1, &io);
        a = stream_open(space, 1, 1, NULL, NULL);
        b = stream_open(space, 2, 1, NULL, NULL);
        OK(canonical_append(a, "old", 3, 1, 1, &offset, &error));
        OK(canonical_append(b, "new", 3, 1, 2, &offset, &error));
        OK(ps_stream_status(a, &ai, &error));
        OK(ps_stream_status(b, &bi, &error));
        OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4,
                          NULL, stream_key(1), &ai.ref, &original, &error));
        for (i = 0; i < 11; ++i) {
          snprintf(pad, sizeof(pad), "pad-%lu", (unsigned long)i);
          OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, pad,
                            strlen(pad), NULL, stream_key(1), &ai.ref, &seen,
                            &error));
        }
        OK(ps_checkpoint(space, &error));
        if (operation == 3)
          OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4,
                            &original.revision, stream_key(2), &bi.ref,
                            &changed, &error));
        OK(ps_status(space, &stats, &error));
        m.operations = 0;
        m.fail_at = kind == 0 ? phase : 0;
        m.tear = m.persist_torn = 1;
        allocation_call = 0;
        allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
        if (operation == 0)
          rc = ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "next", 4,
                              NULL, stream_key(2), &bi.ref, &changed, &error);
        else if (operation == 1)
          rc = ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4,
                              &original.revision, stream_key(2), &bi.ref,
                              &changed, &error);
        else if (operation == 2)
          rc = ps_catalog_remove(space, 1, PS_CATALOG_COLLECTION, "slot", 4,
                                 &original.revision, &error);
        else
          rc = ps_checkpoint(space, &error);
        allocation_fail = 0;
        m.fail_at = 0;
        if (!phase)
          count = kind == 0 ? m.operations : allocation_call;
        if (rc != LC_OK) {
          CHECK(kind == 0 || rc == LC_ERR_NOMEM);
          lc_error_cleanup(&error);
        }
        if (kind == 0 && phase == count && operation != 3) {
          int fd = open(path, O_RDONLY);
          bytes = operation == 2 ? 64 : sizeof(event);
          CHECK(fd >= 0 && pread(fd, event, bytes,
                                 PS_JOURNAL_OFFSET + stats.journal_bytes) ==
                               (ssize_t)bytes);
          CHECK(close(fd) == 0);
          memcpy(m.stable + PS_JOURNAL_OFFSET + stats.journal_bytes, event,
                 bytes);
        }
        OK(ps_stream_close(a, &error));
        OK(ps_stream_close(b, &error));
        OK(ps_close(space, &error));
        crash(&m);
        space = reopen(&io);
        for (i = 0; i < 11; ++i) {
          snprintf(pad, sizeof(pad), "pad-%lu", (unsigned long)i);
          catalog_expect(space, 1, PS_CATALOG_COLLECTION, pad, strlen(pad), 1);
        }
        OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION,
                          operation == 0 ? "next" : "slot", 4, &view, &error));
        if (view) {
          OK(ps_catalog_view_info(view, &seen, &error));
          CHECK(seen.stream.key == 1 || seen.stream.key == 2);
          if (operation == 0 || operation == 3 ||
              (operation == 1 && rc == LC_OK))
            CHECK(seen.stream.key == 2);
          if (operation == 2)
            CHECK(rc != LC_OK);
          ps_catalog_view_close(view);
          view = NULL;
        } else
          CHECK(operation == 2 || (operation == 0 && rc != LC_OK));
        if (operation == 0)
          catalog_expect(space, 1, PS_CATALOG_COLLECTION, "slot", 4, 1);
        a = stream_open(space, 1, 0, &ai.ref, NULL);
        b = stream_open(space, 2, 0, &bi.ref, NULL);
        stream_expect(a, 0, "old", 3, 1);
        stream_expect(b, 0, "new", 3, 2);
        BAD(ps_stream_retire(a, &error));
        OK(ps_checkpoint(space, &error));
        OK(ps_stream_close(a, &error));
        OK(ps_stream_close(b, &error));
        OK(ps_close(space, &error));
        space = reopen(&io);
        catalog_expect(space, 1, PS_CATALOG_COLLECTION, "pad-0", 5, 1);
        OK(ps_close(space, &error));
      }
      CHECK(count >= 1);
    }
  free(m.stable);
  clean();
  puts("catalog: every create/replace/delete/checkpoint I/O and heap failure, "
       "growing index, torn records and durable target references");
}
typedef struct catalog_actor {
  ps_space *space;
  commit_gate *gate;
  ps_ref expected, target;
  ps_catalog_info result;
  const char *name;
  uint64_t stream;
  int create, remove, rc;
} catalog_actor;
static void *catalog_worker(void *context) {
  catalog_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  a->rc =
      a->remove
          ? ps_catalog_remove(a->space, 1, PS_CATALOG_COLLECTION, a->name,
                              strlen(a->name), &a->expected, &e)
          : ps_catalog_put(a->space, NULL, 1, PS_CATALOG_COLLECTION, a->name,
                           strlen(a->name), a->create ? NULL : &a->expected,
                           stream_key(a->stream), &a->target, &a->result, &e);
  lc_error_cleanup(&e);
  pthread_mutex_lock(&a->gate->mutex);
  ++a->gate->finished;
  pthread_cond_broadcast(&a->gate->cond);
  pthread_mutex_unlock(&a->gate->mutex);
  return NULL;
}
static void catalog_concurrency(int operation) {
  commit_gate gate = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_space *space;
  ps_stream *streams[3];
  ps_stream_info targets[3];
  ps_catalog_info initial, result;
  ps_catalog_view *held = NULL, *view = NULL;
  ps_stats before;
  catalog_actor actors[2] = {{0}};
  pthread_t threads[2];
  size_t i, syncs;
  clean();
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
  space = create(PS_MIB, 0, &io);
  for (i = 0; i < 3; ++i) {
    streams[i] = stream_open(space, i + 1, 1, NULL, NULL);
    OK(ps_stream_status(streams[i], &targets[i], &error));
  }
  if (operation != 0) {
    OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4, NULL,
                      stream_key(1), &targets[0].ref, &initial, &error));
    OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "slot", 4, &held,
                      &error));
  }
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &before, &error));
  actors[0].space = actors[1].space = space;
  actors[0].gate = actors[1].gate = &gate;
  actors[0].name = "slot";
  actors[0].stream = 2;
  actors[0].target = targets[1].ref;
  actors[0].create = operation == 0;
  actors[0].remove = operation == 2;
  if (operation)
    actors[0].expected = initial.revision;
  actors[1].name = "other";
  actors[1].stream = 3;
  actors[1].target = targets[2].ref;
  actors[1].create = 1;
  syncs = gate.syncs;
  gate.enabled = gate.hold_second = 1;
  CHECK(pthread_create(&threads[0], NULL, catalog_worker, &actors[0]) == 0);
  await_gate(&gate, 0);
  CHECK(pthread_create(&threads[1], NULL, catalog_worker, &actors[1]) == 0);
  await_stream_requests(space, before.sync_requests + 2);
  OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "slot", 4, &view, &error));
  if (operation) {
    CHECK(view != NULL);
    OK(ps_catalog_view_info(view, &result, &error));
    CHECK(result.stream.key == 1);
    ps_catalog_view_close(view);
    view = NULL;
    BAD(ps_stream_retire(streams[0], &error));
  } else
    CHECK(view == NULL);
  BAD(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4,
                     operation ? &initial.revision : NULL, stream_key(3),
                     &targets[2].ref, &result, &error));
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  await_gate(&gate, 3);
  await_stream_finished(&gate, 1);
  OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "slot", 4, &view, &error));
  if (operation == 2)
    CHECK(!view);
  else {
    CHECK(view != NULL);
    OK(ps_catalog_view_info(view, &result, &error));
    CHECK(result.stream.key == 2);
    ps_catalog_view_close(view);
    view = NULL;
  }
  OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "other", 5, &view,
                    &error));
  CHECK(!view);
  if (held) {
    OK(ps_catalog_view_info(held, &result, &error));
    CHECK(result.stream.key == 1);
    BAD(ps_stream_retire(streams[0], &error));
    ps_catalog_view_close(held);
  }
  pthread_mutex_lock(&gate.mutex);
  gate.second_released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(threads[0], NULL) == 0 &&
        pthread_join(threads[1], NULL) == 0);
  CHECK(actors[0].rc == LC_OK && actors[1].rc == LC_OK &&
        gate.syncs == syncs + 2);
  catalog_expect(space, 1, PS_CATALOG_COLLECTION, "other", 5, 3);
  if (operation)
    OK(ps_stream_retire(streams[0], &error));
  for (i = 0; i < 3; ++i)
    OK(ps_stream_close(streams[i], &error));
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&gate.cond) == 0 &&
        pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
  puts("catalog: NoSync structural barriers, exact durable-prefix "
       "create/replace/delete, independent names, one CAS winner and pinned "
       "targets");
}

typedef struct catalog_visit_context {
  ps_space *space;
  ps_stream *stream;
  ps_catalog_info rescued;
  size_t seen;
  uint64_t candidate;
  int mode;
} catalog_visit_context;
static int catalog_visit_test(void *context, const void *name, size_t length,
                              const ps_catalog_info *info, lc_error *e) {
  catalog_visit_context *v = context;
  ++v->seen;
  if (v->mode == 1)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "injected catalog visitor failure", NULL, NULL, "test");
  CHECK(length == 1 && info->stream.key == 1);
  OK(ps_catalog_remove(v->space, 1, PS_CATALOG_COLLECTION, name, length,
                       &info->revision, &error));
  OK(ps_checkpoint(v->space, &error));
  BAD(ps_stream_retire(v->stream, &error));
  return LC_OK;
}
static int catalog_orphan_test(void *context, ps_key key, const ps_ref *ref,
                               lc_error *e) {
  catalog_visit_context *v = context;
  ps_owned_head *head;
  ps_owned_status status;
  ++v->seen;
  CHECK(key.key == (v->candidate ? v->candidate : 2));
  if (v->mode == 3)
    return LC_OK;
  if (v->mode == 1)
    return lc_error_set(e, LC_ERR_INVALID, 0, "injected orphan visitor failure",
                        NULL, NULL, "test");
  if (v->mode == 2)
    OK(ps_catalog_put(v->space, NULL, 1, PS_CATALOG_NAMESPACE, "rescued", 7,
                      NULL, key, ref, &v->rescued, &error));
  OK(ps_owned_open(v->space, NULL, key, 0, ref, &head, &status, &error));
  if (v->mode == 2)
    BAD(ps_owned_retire(head, &error));
  else {
    OK(ps_owned_retire(head, &error));
    OK(ps_checkpoint(v->space, &error));
  }
  ps_owned_close(head);
  return LC_OK;
}
static void catalog_visitors(void) {
  ps_space *space;
  ps_stream *a, *b, *c;
  ps_stream_info ai;
  ps_catalog_info info;
  ps_catalog_view *view = NULL;
  ps_stats stats;
  catalog_visit_context context = {0};
  size_t i;
  uint64_t offset;
  unsigned char name;
  clean();
  space = create(3 * PS_MIB, 0, NULL);
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  c = stream_open(space, 3, 1, NULL, NULL);
  OK(canonical_append(b, "orphan", 6, 1, 1, &offset, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_stream_status(a, &ai, &error));
  for (i = 0; i < 3; ++i) {
    name = (unsigned char)i;
    OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, &name, 1, NULL,
                      stream_key(1), &ai.ref, &info, &error));
  }
  allocation_call = 0;
  allocation_fail = 1;
  CHECK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, &name, 1, &view,
                       &error) == LC_ERR_NOMEM &&
        !view);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  context.space = space;
  context.stream = a;
  allocation_call = 0;
  allocation_fail = 1;
  CHECK(ps_catalog_visit(space, 1, PS_CATALOG_COLLECTION, catalog_visit_test,
                         &context, &error) == LC_ERR_NOMEM);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  context.mode = 1;
  BAD(ps_catalog_visit(space, 1, PS_CATALOG_COLLECTION, catalog_visit_test,
                       &context, &error));
  context.mode = 0;
  context.seen = 0;
  OK(ps_catalog_visit(space, 1, PS_CATALOG_COLLECTION, catalog_visit_test,
                      &context, &error));
  CHECK(context.seen == 3);
  OK(ps_stream_retire(a, &error));
  OK(ps_stream_close(a, &error));
  context.seen = 0;
  context.mode = 1;
  allocation_call = 0;
  allocation_fail = 1;
  CHECK(ps_catalog_visit_orphans(space, catalog_orphan_test, &context,
                                 &error) == LC_ERR_NOMEM);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  BAD(ps_catalog_visit_orphans(space, catalog_orphan_test, &context, &error));
  context.seen = 0;
  context.mode = 2;
  OK(ps_catalog_visit_orphans(space, catalog_orphan_test, &context, &error));
  CHECK(context.seen == 1);
  catalog_expect(space, 1, PS_CATALOG_NAMESPACE, "rescued", 7, 2);
  OK(ps_catalog_remove(space, 1, PS_CATALOG_NAMESPACE, "rescued", 7,
                       &context.rescued.revision, &error));
  context.seen = 0;
  context.mode = 0;
  OK(ps_catalog_visit_orphans(space, catalog_orphan_test, &context, &error));
  CHECK(context.seen == 1);
  OK(ps_status(space, &stats, &error));
  CHECK(!stats.allocated_bytes);
  OK(ps_stream_close(c, &error));
  OK(ps_close(space, &error));
  clean();
  puts("catalog: snapshot callbacks outside locks, pinned mutations, "
       "callback/allocation failure cleanup, orphan candidates and publication "
       "races");
}
static void catalog_reopen_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_options options = {0};
  ps_space *space;
  ps_stream *a, *b, *c;
  ps_stream_info ai, bi;
  ps_catalog_info info, expected;
  catalog_visit_context context = {0};
  unsigned char *saved = malloc((size_t)(3 * PS_MIB));
  unsigned char *large = malloc(PS_CHUNK + 17);
  char name[32];
  size_t kind, phase, count, i;
  uint64_t offset, reads;
  int rc;
  CHECK(saved && large);
  memset(large, 0x6b, PS_CHUNK + 17);
  clean();
  space = create(3 * PS_MIB, 1, &io);
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  c = stream_open(space, 3, 1, NULL, NULL);
  OK(canonical_append(c, "staged", 6, 1, 3, &offset, &error));
  OK(ps_stream_close(c, &error));
  OK(ps_stream_status(a, &ai, &error));
  OK(ps_stream_status(b, &bi, &error));
  for (i = 0; i < 24; ++i) {
    snprintf(name, sizeof(name), "name-%lu", (unsigned long)i);
    OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, name, strlen(name),
                      NULL, stream_key(1), &ai.ref, &info, &error));
  }
  OK(ps_catalog_put(space, NULL, 9, PS_CATALOG_CONTROL, large, PS_CHUNK + 17,
                    NULL, stream_key(2), &bi.ref, &info, &error));
  OK(ps_checkpoint(space, &error));
  info = catalog_expect(space, 1, PS_CATALOG_COLLECTION, "name-1", 6, 1);
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "name-1", 6,
                    &info.revision, stream_key(2), &bi.ref, &expected, &error));
  info = catalog_expect(space, 1, PS_CATALOG_COLLECTION, "name-2", 6, 1);
  OK(ps_catalog_remove(space, 1, PS_CATALOG_COLLECTION, "name-2", 6,
                       &info.revision, &error));
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "name-2", 6, NULL,
                    stream_key(2), &bi.ref, &expected, &error));
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  OK(ps_close(space, &error));
  memcpy(saved, m.stable, (size_t)(3 * PS_MIB));
  options.io = &io;
  options.durable_sync = 1;
  for (kind = 0; kind < 3; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      memcpy(m.stable, saved, (size_t)(3 * PS_MIB));
      crash(&m);
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.reads = 0;
      m.fail_read = kind == 2 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_open(path, 0, &options, &space, &error);
      allocation_fail = 0;
      m.fail_at = m.fail_read = 0;
      if (!phase)
        count = kind == 0   ? m.operations
                : kind == 1 ? allocation_call
                            : m.reads;
      if (rc == LC_OK)
        OK(ps_close(space, &error));
      else {
        CHECK(space == NULL && (kind != 1 || rc == LC_ERR_NOMEM));
        lc_error_cleanup(&error);
      }
      crash(&m);
      space = reopen(&io);
      context.space = space;
      context.mode = 3;
      context.candidate = 3;
      context.seen = 0;
      reads = m.read_bytes;
      OK(ps_catalog_visit_orphans(space, catalog_orphan_test, &context,
                                  &error));
      CHECK(context.seen == 1 && m.read_bytes == reads);
      for (i = 0; i < 24; ++i) {
        snprintf(name, sizeof(name), "name-%lu", (unsigned long)i);
        info = catalog_expect(space, 1, PS_CATALOG_COLLECTION, name,
                              strlen(name), i == 1 || i == 2 ? 2 : 1);
        if (i == 2)
          CHECK(info.id == expected.id &&
                info.revision.incarnation == expected.revision.incarnation);
      }
      catalog_expect(space, 9, PS_CATALOG_CONTROL, large, PS_CHUNK + 17, 2);
      OK(ps_close(space, &error));
    }
    CHECK(count > (kind == 1 ? 10 : 1));
    printf("catalog recovery fault sites: kind=%lu count=%lu\n",
           (unsigned long)kind, (unsigned long)count);
  }
  free(saved);
  free(large);
  free(m.stable);
  clean();
  puts("catalog: every recovery read/write/sync and heap failure, name-index "
       "rebuild, spanning descriptors and acknowledged revisions survive");
}

static void catalog_removal_prefix(void) {
  static const char *names[] = {"first", "second", "third"};
  model m;
  ps_io io = model_init(&m);
  for (size_t fault = 0; fault <= 3; ++fault) {
    ps_catalog_info saved[3];
    ps_catalog_removal items[3];
    ps_catalog_view *view = NULL;
    ps_ref stale;
    ps_space *s;
    size_t completed, syncs;
    int rc;
    if (fault == 1)
      continue;
    clean();
    s = create(PS_MIB, 0, &io);
    for (size_t i = 0; i < 3; ++i) {
      OK(ps_catalog_put_marker(s, NULL, 1, names[i], strlen(names[i]), NULL,
                               "m", 1, &saved[i], &error));
      items[i].name = names[i];
      items[i].length = strlen(names[i]);
      items[i].expected = &saved[i].revision;
      items[i].kind = PS_CATALOG_MARKER;
    }
    stale = saved[1].revision;
    ++stale.incarnation;
    if (!fault)
      items[1].expected = &stale;
    m.operations = 0;
    m.fail_at = fault;
    m.tear = 1;
    syncs = m.syncs;
    rc = ps_catalog_remove_many(s, 1, items, 2, &completed, &error);
    CHECK(rc == (fault ? LC_ERR_TRANSPORT : LC_ERR_INVALID));
    CHECK(completed == (fault ? 0U : 1U));
    lc_error_cleanup(&error);
    if (!fault) {
      CHECK(m.syncs == syncs + 1);
      OK(ps_catalog_get(s, 1, PS_CATALOG_MARKER, names[0], items[0].length,
                        &view, &error));
      CHECK(!view);
      items[1].expected = &saved[1].revision;
      syncs = m.syncs;
      OK(ps_catalog_remove_many(s, 1, items + 1, 2, &completed, &error));
      CHECK(completed == 2 && m.syncs == syncs + 1);
    }
    OK(ps_close(s, &error));
    crash(&m);
    s = reopen(&io);
    for (size_t i = 0; i < 3; ++i) {
      OK(ps_catalog_get(s, 1, PS_CATALOG_MARKER, names[i], items[i].length,
                        &view, &error));
      CHECK(!!view == !!fault);
      ps_catalog_view_close(view);
    }
    OK(ps_close(s, &error));
  }
  free(m.stable);
  clean();
}
static void catalog_bounds_rollover(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info target;
  ps_catalog_info first, next;
  ps_catalog_view *view = NULL;
  ps_stats stats;
  unsigned char *name = malloc(PS_CATALOG_NAME_MAX + 1);
  uint64_t checkpoints;
  CHECK(name != NULL);
  memset(name, 0x6c, PS_CATALOG_NAME_MAX + 1);
  clean();
  space = create(3 * PS_MIB, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_status(stream, &target, &error));
  BAD(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, name,
                     PS_CATALOG_NAME_MAX + 1, NULL, stream_key(1), &target.ref,
                     &first, &error));
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, name,
                    PS_CATALOG_NAME_MAX, NULL, stream_key(1), &target.ref,
                    &first, &error));
  OK(ps_status(space, &stats, &error));
  CHECK(stats.journal_bytes == PS_JOURNAL);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL); /* Grant publication leaves the full journal intact. */
  stream = stream_open(space, 1, 0, &target.ref, NULL);
  next = catalog_expect(space, 1, PS_CATALOG_COLLECTION, name,
                        PS_CATALOG_NAME_MAX, 1);
  CHECK(next.id == first.id &&
        next.revision.incarnation == first.revision.incarnation);
  OK(ps_catalog_remove(space, 1, PS_CATALOG_COLLECTION, name,
                       PS_CATALOG_NAME_MAX, &next.revision, &error));
  OK(ps_stream_retire(stream, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_checkpoint(space, &error));
  OK(ps_close(space, &error));
  clean();
  space = create(3 * PS_MIB, 0, NULL);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_stream_status(stream, &target, &error));
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4, NULL,
                    stream_key(1), &target.ref, &first, &error));
  OK(ps_catalog_remove(space, 1, PS_CATALOG_COLLECTION, "slot", 4,
                       &first.revision, &error));
  OK(ps_status(space, &stats, &error));
  checkpoints = stats.checkpoints;
  while (PS_JOURNAL - stats.journal_bytes >= 88) {
    put(space, state(1), "x", 1, 1);
    OK(ps_status(space, &stats, &error));
  }
  CHECK(PS_JOURNAL - stats.journal_bytes < 108 &&
        stats.checkpoints == checkpoints);
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4, NULL,
                    stream_key(1), &target.ref, &next, &error));
  CHECK(next.id != first.id);
  OK(ps_status(space, &stats, &error));
  CHECK(stats.checkpoints == checkpoints + 1);
  OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "slot", 4, &view, &error));
  CHECK(view != NULL);
  ps_catalog_view_close(view);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = reopen(NULL);
  catalog_expect(space, 1, PS_CATALOG_COLLECTION, "slot", 4, 1);
  expect(space, state(1), NULL, "x", 1);
  OK(ps_close(space, &error));
  free(name);
  clean();
  puts("catalog: maximum inline name, full-journal recovery, byte bounds and "
       "rebinding after tombstone collection");
}
static void catalog_invalid_records(void) {
  ps_space *space;
  ps_stream *stream;
  ps_stream_info target;
  ps_catalog_info first, second;
  ps_catalog_view *view = NULL;
  ps_stats stats;
  unsigned char wire[108], zero[8] = {0};
  size_t phase;
  off_t offset;
  int fd;
  for (phase = 0; phase < 8; ++phase) {
    clean();
    space = create(PS_MIB, 0, NULL);
    stream = stream_open(space, 1, 1, NULL, NULL);
    OK(ps_stream_status(stream, &target, &error));
    OK(ps_checkpoint(space, &error));
    OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "slot", 4, NULL,
                      stream_key(1), &target.ref, &first, &error));
    offset = PS_JOURNAL_OFFSET;
    if (phase == 6) {
      OK(ps_status(space, &stats, &error));
      offset += (off_t)stats.journal_bytes;
      OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION, "next", 4, NULL,
                        stream_key(1), &target.ref, &second, &error));
    } else if (phase == 7) {
      OK(ps_checkpoint(space, &error));
      OK(ps_catalog_remove(space, 1, PS_CATALOG_COLLECTION, "slot", 4,
                           &first.revision, &error));
      OK(ps_stream_retire(stream, &error));
    }
    OK(ps_stream_close(stream, &error));
    OK(ps_close(space, &error));
    fd = open(path, O_RDWR);
    size_t bytes = phase == 7 ? 64 : sizeof(wire);
    CHECK(fd >= 0 && pread(fd, wire, bytes, offset) == (ssize_t)bytes);
    if (phase == 0)
      fixture_put(wire + 64, 99, 4);
    else if (phase == 1)
      fixture_put(wire + 68, PS_CATALOG_BODY + 1, 4);
    else if (phase == 2)
      fixture_put(wire + 72, PS_STATE, 4);
    else if (phase == 3)
      fixture_put(wire + 96, target.ref.incarnation + 1, 8);
    else if (phase == 4)
      fixture_put(wire + 76, 1, 4);
    else if (phase == 5)
      fixture_put(wire + 32, 0, 8);
    else if (phase == 6)
      fixture_put(wire + 32, first.id, 8);
    else {
      fixture_put(wire + 16, PS_STREAM, 4);
      fixture_put(wire + 24, 10, 8);
      fixture_put(wire + 32, 1, 8);
      fixture_put(wire + 40, target.ref.incarnation, 8);
      CHECK(pwrite(fd, zero, sizeof(zero), offset + 64) ==
            (ssize_t)sizeof(zero));
    }
    fixture_entry_crc(wire);
    CHECK(pwrite(fd, wire, bytes, offset) == (ssize_t)bytes);
    CHECK(fdatasync(fd) == 0 && close(fd) == 0);
    if (phase == 3 || phase >= 5) {
      CHECK(ps_open(path, 0, NULL, &space, &error) == LC_ERR_PROTOCOL &&
            space == NULL);
      lc_error_cleanup(&error);
    } else {
      space = reopen(NULL);
      OK(ps_catalog_get(space, 1, PS_CATALOG_COLLECTION, "slot", 4, &view,
                        &error));
      CHECK(view == NULL);
      OK(ps_close(space, &error));
    }
  }
  clean();
  puts("catalog: malformed checksummed type/length/reserved fields, stale "
       "targets, name IDs and referenced-head retirement rejected");
}

typedef struct catalog_snapshot_actor {
  ps_space *space;
  size_t seen;
  int orphans, rc;
} catalog_snapshot_actor;
static int snapshot_name_counter(void *context, const void *name, size_t length,
                                 const ps_catalog_info *info, lc_error *e) {
  catalog_snapshot_actor *a = context;
  (void)e;
  if (length != 4 || memcmp(name, "base", 4) || info->stream.key != 1)
    return LC_ERR_PROTOCOL;
  ++a->seen;
  return LC_OK;
}
static int snapshot_orphan_counter(void *context, ps_key key, const ps_ref *ref,
                                   lc_error *e) {
  catalog_snapshot_actor *a = context;
  (void)ref;
  (void)e;
  if (key.key != 1)
    return LC_ERR_PROTOCOL;
  ++a->seen;
  return LC_OK;
}
static void *catalog_snapshot_worker(void *context) {
  catalog_snapshot_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  a->rc = a->orphans ? ps_catalog_visit_orphans(a->space,
                                                snapshot_orphan_counter, a, &e)
                     : ps_catalog_visit(a->space, 1, PS_CATALOG_COLLECTION,
                                        snapshot_name_counter, a, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void catalog_snapshot_prefix(int orphans) {
  commit_gate gate = {0};
  allocation_pause pause = {0};
  ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_space *space;
  ps_stream *a, *b;
  ps_stream_info ai, bi;
  ps_catalog_info binding;
  catalog_actor publication = {0};
  catalog_snapshot_actor snapshot = {0};
  pthread_t threads[2];
  struct timespec deadline;
  int rc = 0, entered;
  clean();
  CHECK(pthread_mutex_init(&gate.mutex, NULL) == 0 &&
        pthread_cond_init(&gate.cond, NULL) == 0);
  CHECK(pthread_mutex_init(&pause.mutex, NULL) == 0 &&
        pthread_cond_init(&pause.cond, NULL) == 0);
  space = create(PS_MIB, 0, &io);
  a = stream_open(space, 1, 1, NULL, NULL);
  b = stream_open(space, 2, 1, NULL, NULL);
  OK(ps_stream_status(a, &ai, &error));
  OK(ps_stream_status(b, &bi, &error));
  OK(ps_catalog_put(space, NULL, 1, PS_CATALOG_COLLECTION,
                    orphans ? "slot" : "base", 4, NULL,
                    stream_key(orphans ? 2 : 1), orphans ? &bi.ref : &ai.ref,
                    &binding, &error));
  OK(ps_stream_close(a, &error));
  OK(ps_stream_close(b, &error));
  publication.space = space;
  publication.gate = &gate;
  publication.name = "slot";
  publication.create = !orphans;
  publication.remove = orphans;
  publication.expected = binding.revision;
  publication.target = bi.ref;
  publication.stream = 2;
  gate.enabled = 1;
  CHECK(pthread_create(&threads[0], NULL, catalog_worker, &publication) == 0);
  await_gate(&gate, 0);
  snapshot.space = space;
  snapshot.orphans = orphans;
  paused_allocation = &pause;
  CHECK(pthread_create(&threads[1], NULL, catalog_snapshot_worker, &snapshot) ==
        0);
  CHECK(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&pause.mutex);
  while (!pause.entered && !rc)
    rc = pthread_cond_timedwait(&pause.cond, &pause.mutex, &deadline);
  entered = pause.entered;
  pthread_mutex_unlock(&pause.mutex);
  CHECK(entered && !rc);
  /* The snapshot owns the allocator mutex here. Commit completion must still
   * advance its watermark while vector allocation is held. */
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  await_stream_finished(&gate, 1);
  CHECK(pthread_join(threads[0], NULL) == 0 && publication.rc == LC_OK);
  pthread_mutex_lock(&pause.mutex);
  pause.released = 1;
  pthread_cond_broadcast(&pause.cond);
  pthread_mutex_unlock(&pause.mutex);
  CHECK(pthread_join(threads[1], NULL) == 0);
  paused_allocation = NULL;
  CHECK(snapshot.rc == LC_OK && snapshot.seen == 1);
  OK(ps_close(space, &error));
  CHECK(pthread_cond_destroy(&pause.cond) == 0 &&
        pthread_mutex_destroy(&pause.mutex) == 0);
  CHECK(pthread_cond_destroy(&gate.cond) == 0 &&
        pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
  puts("catalog: fixed snapshot watermark across commit completion during "
       "vector allocation, no count/capture race");
}

static void budget_lifecycle(int durable) {
  ps_space *s, *foreign = NULL;
  ps_budget *b = NULL, *rejected = NULL;
  ps_budget_plan plan = {PS_PAGE, 88, 1, 0}, remaining;
  ps_writer *w = NULL;
  ps_stats before, after;
  unsigned char *bytes = malloc(2 * PS_PAGE);
  CHECK(bytes);
  memset(bytes, 0x63, 2 * PS_PAGE);
  clean();
  s = create(PS_MIB, durable, NULL);
  OK(ps_status(s, &before, &error));
  plan.payload_bytes = 1;
  BAD(ps_budget_open(s, &plan, &b, &error));
  CHECK(!b);
  plan.payload_bytes = PS_PAGE;
  plan.metadata_bytes = UINT64_MAX;
  BAD(ps_budget_open(s, &plan, &b, &error));
  CHECK(!b);
  plan.metadata_bytes = 88;
  plan.entries = UINT64_MAX;
  BAD(ps_budget_open(s, &plan, &b, &error));
  CHECK(!b);
  plan.entries = 1;
  plan.payload_bytes = PS_MIB;
  BAD(ps_budget_open(s, &plan, &b, &error));
  CHECK(!b);
  OK(ps_status(s, &after, &error));
  CHECK(after.entries == before.entries && after.budgets == 0 &&
        after.journal_bytes == before.journal_bytes &&
        after.reserved_payload_bytes == 0 &&
        after.reserved_metadata_bytes == 0 && after.reserved_entries == 0);
  plan.payload_bytes = PS_PAGE;
  OK(ps_budget_open(s, &plan, &b, &error));
  OK(ps_open(other, 1, NULL, &foreign, &error));
  BAD(ps_begin(foreign, b, state(1), &w, &error));
  CHECK(!w);
  OK(ps_close(foreign, &error));
  BAD(ps_close(s, &error));
  OK(ps_begin(s, b, state(1), &w, &error));
  BAD(ps_budget_close(b, &error));
  OK(ps_write(w, bytes, PS_PAGE, &error));
  OK(ps_publish(w, NULL, &error));
  OK(ps_budget_status(b, &remaining, &error));
  CHECK(!remaining.payload_bytes && !remaining.metadata_bytes &&
        !remaining.entries);
  ps_writer_close(w);
  OK(ps_budget_close(b, &error));
  b = NULL;
  /* Replacing a same-shape value needs no descriptor or entry growth. */
  plan.metadata_bytes = plan.entries = 0;
  OK(ps_budget_open(s, &plan, &b, &error));
  OK(ps_begin(s, b, state(1), &w, &error));
  OK(ps_write(w, bytes + PS_PAGE, PS_PAGE, &error));
  OK(ps_publish(w, NULL, &error));
  ps_writer_close(w);
  OK(ps_budget_close(b, &error));
  /* A caller's under-sized plan cannot silently borrow ordinary capacity. */
  plan.metadata_bytes = 88;
  plan.entries = 1;
  for (int kind = 0; kind < 3; ++kind) {
    plan.payload_bytes = kind == 0 ? 0 : PS_PAGE;
    plan.metadata_bytes = kind == 1 ? 87 : 88;
    plan.entries = kind == 2 ? 0 : 1;
    OK(ps_budget_open(s, &plan, &b, &error));
    OK(ps_begin(s, b, state(2), &w, &error));
    OK(ps_write(w, bytes, PS_PAGE, &error));
    BAD(ps_publish(w, NULL, &error));
    ps_writer_close(w);
    absent(s, state(2));
    OK(ps_budget_close(b, &error));
  }
  plan.payload_bytes = PS_PAGE;
  plan.metadata_bytes = 88;
  plan.entries = 1;
  OK(ps_budget_open(s, &plan, &b, &error));
  OK(ps_status(s, &before, &error));
  for (int kind = 0; kind < 3; ++kind) {
    plan.payload_bytes = kind == 0 ? UINT64_MAX / PS_PAGE * PS_PAGE : PS_PAGE;
    plan.metadata_bytes = kind == 1 ? UINT64_MAX : 88;
    plan.entries = kind == 2 ? UINT64_MAX : 1;
    BAD(ps_budget_open(s, &plan, &rejected, &error));
    CHECK(!rejected);
    OK(ps_status(s, &after, &error));
    CHECK(after.reserved_payload_bytes == before.reserved_payload_bytes &&
          after.reserved_metadata_bytes == before.reserved_metadata_bytes &&
          after.reserved_entries == before.reserved_entries &&
          after.budgets == before.budgets);
  }
  OK(ps_budget_close(b, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &after, &error));
  CHECK(!after.budgets && !after.reserved_payload_bytes &&
        !after.reserved_metadata_bytes && !after.reserved_entries);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, state(1), NULL, bytes, PS_PAGE);
  absent(s, state(2));
  OK(ps_status(s, &after, &error));
  CHECK(!after.budgets && !after.reserved_entries);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void budget_fill(ps_space *s, uint64_t *next, const void *bytes) {
  ps_writer *w = NULL;
  int rc;
  for (;;) {
    OK(ps_begin(s, NULL, state((*next)++), &w, &error));
    OK(ps_write(w, bytes, PS_PAGE, &error));
    rc = ps_publish(w, NULL, &error);
    ps_writer_close(w);
    if (rc != LC_OK) {
      CHECK(rc == LC_ERR_INVALID);
      lc_error_cleanup(&error);
      break;
    }
    CHECK(*next < 4096);
  }
}
static void budget_capacity(int durable) {
  ps_space *s;
  ps_budget_plan plan = {8 * PS_PAGE, 256 + 80 + 108 + 512 * 64, 515, 0};
  ps_budget_plan maintenance = {PS_MAINTENANCE_RESERVE, 0, 0, 1};
  ps_budget *b = NULL, *peer = NULL;
  ps_owned_head *head = NULL;
  ps_owned_status head_info;
  ps_catalog_info binding;
  ps_stats stats;
  ps_writer *w = NULL;
  ps_ref ref;
  unsigned char *bytes = malloc(8 * PS_PAGE);
  uint64_t next = 1;
  CHECK(bytes);
  memset(bytes, 0x79, 8 * PS_PAGE);
  clean();
  s = create(3 * PS_MIB, durable, NULL);
  OK(ps_budget_open(s, &plan, &b, &error));
  budget_fill(s, &next, bytes);
  OK(ps_checkpoint(s, &error));
  budget_fill(s, &next, bytes);
  OK(ps_metadata_repack(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.reserved_payload_bytes == plan.payload_bytes &&
        stats.reserved_metadata_bytes == plan.metadata_bytes &&
        stats.reserved_entries == plan.entries && stats.budgets == 1 &&
        stats.metadata_available_bytes >= stats.metadata_workspace_bytes);
  /* Once admitted, the foreground writer owns its claim even when a peer
   * maintenance operation takes the remaining maintenance floor. */
  OK(ps_budget_open(s, &maintenance, &peer, &error));
  OK(ps_owned_open(s, b, key(PS_STREAM, 2, 1), 1, NULL, &head, &head_info,
                   &error));
  OK(ps_catalog_put(s, b, 2, PS_CATALOG_SEGMENT, "slot", 4, NULL,
                    key(PS_STREAM, 2, 1), &head_info.ref, &binding, &error));
  OK(ps_begin(s, b, key(PS_STATE, 2, 1), &w, &error));
  OK(ps_write(w, bytes, PS_CHUNK, &error));
  /* Private payload allocation must not consume future descriptor credits;
   * a checkpoint must retain them until this writer actually publishes. */
  OK(ps_checkpoint(s, &error));
  OK(ps_write(w, bytes + PS_CHUNK, PS_CHUNK, &error));
  OK(ps_publish(w, &ref, &error));
  ps_writer_close(w);
  for (uint64_t i = 0; i < 512; ++i) {
    OK(ps_begin(s, b, key(PS_STATE, 3, i), &w, &error));
    OK(ps_publish(w, NULL, &error));
    ps_writer_close(w);
    if (i % 128 == 127)
      OK(ps_checkpoint(s, &error));
  }
  OK(ps_budget_close(peer, &error));
  OK(ps_budget_close(b, &error));
  ps_owned_close(head);
  OK(ps_status(s, &stats, &error));
  CHECK(!stats.budgets && !stats.reserved_payload_bytes &&
        !stats.reserved_metadata_bytes && !stats.reserved_entries);
  OK(ps_checkpoint(s, &error));
  expect(s, key(PS_STATE, 2, 1), &ref, bytes, 8 * PS_PAGE);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, key(PS_STATE, 2, 1), &ref, bytes, 8 * PS_PAGE);
  for (uint64_t i = 0; i < 512; ++i)
    expect(s, key(PS_STATE, 3, i), NULL, NULL, 0);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
typedef struct budget_actor {
  ps_space *space;
  ps_budget *budget;
  int rc;
} budget_actor;
static void *budget_produce(void *context) {
  budget_actor *a = context;
  ps_writer *w = NULL;
  lc_error e;
  unsigned char *bytes = malloc(PS_CHUNK);
  if (!bytes)
    abort();
  memset(bytes, 0x73, PS_CHUNK);
  lc_error_init(&e);
  a->rc = ps_begin(a->space, a->budget, key(PS_STATE, 99, 1), &w, &e);
  if (a->rc == LC_OK)
    a->rc = ps_write(w, bytes, PS_CHUNK, &e);
  if (a->rc == LC_OK)
    a->rc = ps_publish(w, NULL, &e);
  ps_writer_close(w);
  lc_error_cleanup(&e);
  free(bytes);
  return NULL;
}
static void budget_concurrency(void) {
  commit_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&g, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_space *s;
  ps_budget *b = NULL, *peer = NULL;
  ps_budget_plan plan = {PS_CHUNK, 160, 1, 0}, pending;
  ps_stats stats;
  budget_actor actor;
  pthread_t thread;
  unsigned char *bytes = malloc(PS_CHUNK);
  CHECK(bytes);
  memset(bytes, 0x73, PS_CHUNK);
  clean();
  s = create(PS_MIB, 0, &io);
  OK(ps_budget_open(s, &plan, &b, &error));
  actor.space = s;
  actor.budget = b;
  actor.rc = -1;
  g.enabled = g.hold_payload = 1;
  CHECK(pthread_create(&thread, NULL, budget_produce, &actor) == 0);
  await_gate(&g, 0);
  BAD(ps_budget_close(b, &error));
  for (uint64_t i = 0; i < 512; ++i)
    put(s, state(i), NULL, 0, 1);
  OK(ps_budget_open(s, &plan, &peer, &error));
  OK(ps_metadata_repack(s, &error));
  OK(ps_budget_close(peer, &error));
  OK(ps_grow(s, PS_MIB, &error));
  OK(ps_budget_status(b, &pending, &error));
  CHECK(!pending.payload_bytes && pending.metadata_bytes == 160 &&
        pending.entries == 1);
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(actor.rc == LC_OK);
  OK(ps_budget_close(b, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(!stats.budgets && !stats.reserved_entries);
  expect(s, key(PS_STATE, 99, 1), NULL, bytes, PS_CHUNK);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, key(PS_STATE, 99, 1), NULL, bytes, PS_CHUNK);
  OK(ps_close(s, &error));
  CHECK(pthread_cond_destroy(&g.cond) == 0);
  CHECK(pthread_mutex_destroy(&g.mutex) == 0);
  free(bytes);
  clean();
}
static void budget_allocation_faults(void) {
  ps_budget_plan plan = {PS_PAGE, 4000 * 64, 4000, 0};
  unsigned char *bytes = malloc(PS_CHUNK);
  size_t count = 0;
  CHECK(bytes);
  memset(bytes, 0x29, PS_CHUNK);
  for (size_t cut = 0; cut <= count; ++cut) {
    ps_space *s;
    ps_budget *b = NULL;
    ps_stats before, after;
    int rc;
    clean();
    s = create(3 * PS_MIB, 0, NULL);
    for (uint64_t i = 0; i < 128; ++i)
      put(s, state(i + 10), bytes, PS_CHUNK, PS_CHUNK);
    for (uint64_t i = 0; i < 128; i += 2)
      OK(ps_delete(s, state(i + 10), &error));
    OK(ps_checkpoint(s, &error));
    OK(ps_status(s, &before, &error));
    allocation_call = 0;
    allocation_fail = cut ? cut : (size_t)-1;
    rc = ps_budget_open(s, &plan, &b, &error);
    allocation_fail = 0;
    if (!cut) {
      count = allocation_call;
      CHECK(count >= 16);
      OK(rc);
      OK(ps_budget_close(b, &error));
    } else {
      CHECK(rc == LC_ERR_NOMEM && !b);
      lc_error_cleanup(&error);
    }
    OK(ps_status(s, &after, &error));
    CHECK(!after.budgets && !after.reserved_payload_bytes &&
          !after.reserved_metadata_bytes && !after.reserved_entries &&
          after.entries == before.entries &&
          after.metadata_available_bytes == before.metadata_available_bytes &&
          after.metadata_workspace_bytes == before.metadata_workspace_bytes);
    OK(ps_budget_open(s, &plan, &b, &error));
    OK(ps_budget_close(b, &error));
    put(s, state(1), "retry", 5, 1);
    OK(ps_checkpoint(s, &error));
    OK(ps_close(s, &error));
  }
  printf("capacity budget admission: %lu allocation failure cuts\n",
         (unsigned long)count);
  free(bytes);
  clean();
}
static int budget_value_write(ps_space *s, ps_key k, const void *bytes,
                              size_t n, size_t chunk, ps_ref *ref,
                              lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *w = NULL;
  size_t at = 0, step;
  int rc = ps_budget_plan_value(&plan, n, e);
  if (rc == LC_OK)
    rc = ps_budget_open(s, &plan, &budget, e);
  if (rc == LC_OK)
    rc = ps_begin(s, budget, k, &w, e);
  while (rc == LC_OK && at < n) {
    step = n - at;
    if (step > chunk)
      step = chunk;
    rc = ps_write(w, (const unsigned char *)bytes + at, step, e);
    at += step;
  }
  if (rc == LC_OK)
    rc = ps_publish(w, ref, e);
  ps_writer_close(w);
  if (ps_budget_close(budget, NULL) != LC_OK)
    abort();
  return rc;
}
static void value_budget_planning(void) {
  uint64_t bound = (PS_JOURNAL - 64) / 24 * (uint64_t)PS_PAGE;
  uint64_t sizes[] = {
      0, 1, PS_PAGE, PS_PAGE + 1, PS_CHUNK + 1, 3 * PS_MIB + 128, bound};
  ps_budget_plan p, before;
  for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); ++i) {
    p = (ps_budget_plan){PS_PAGE, 88, 1, 1};
    uint64_t pages = sizes[i] / PS_PAGE + (sizes[i] % PS_PAGE != 0);
    uint64_t extents = pages + (sizes[i] % PS_PAGE != 0);
    OK(ps_budget_plan_value(&p, sizes[i], &error));
    CHECK(p.payload_bytes == PS_PAGE + pages * PS_PAGE &&
          p.metadata_bytes == 88 + 64 + extents * 24 && p.entries == 2 &&
          p.maintenance == 1);
  }
  for (int kind = 0; kind < 6; ++kind) {
    p = (ps_budget_plan){PS_PAGE, 88, 1, 0};
    if (kind == 0)
      p.payload_bytes = UINT64_MAX;
    if (kind == 1)
      p.metadata_bytes = UINT64_MAX;
    if (kind == 2)
      p.entries = UINT64_MAX;
    before = p;
    CHECK(ps_budget_plan_value(&p,
                               kind == 3   ? bound + 1
                               : kind == 4 ? (uint64_t)INT64_MAX
                               : kind == 5 ? UINT64_MAX
                                           : 1,
                               &error) == LC_ERR_INVALID);
    lc_error_cleanup(&error);
    CHECK(p.payload_bytes == before.payload_bytes &&
          p.metadata_bytes == before.metadata_bytes &&
          p.entries == before.entries && p.maintenance == before.maintenance);
  }
  BAD(ps_budget_plan_value(NULL, 0, &error));
  BAD(ps_budget_plan_value(NULL, 1, NULL));
}
static void value_budget_lifecycle(int durable) {
  size_t sizes[] = {0,
                    1,
                    PS_PAGE,
                    PS_PAGE + 1,
                    PS_CHUNK - 1,
                    PS_CHUNK,
                    PS_CHUNK + 1,
                    3 * (size_t)PS_MIB + 128};
  size_t max = sizes[sizeof(sizes) / sizeof(*sizes) - 1];
  unsigned char *a = malloc(max), *b = malloc(max);
  ps_key k = key(PS_CONTROL, 101, 1);
  ps_space *s;
  ps_reader *held = NULL, *rejected = NULL;
  ps_ref old, next;
  ps_stats stats;
  CHECK(a && b);
  for (size_t i = 0; i < max; ++i) {
    a[i] = (unsigned char)(i * 17);
    b[i] = (unsigned char)~a[i];
  }
  clean();
  s = create(0, durable, NULL);
  for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); ++i) {
    size_t n = sizes[i];
    allocation_max = 0;
    measure_allocations = n == max;
    OK(budget_value_write(s, k, a, n, 317, &old, &error));
    measure_allocations = 0;
    if (n == max)
      CHECK(allocation_max <= 2 * PS_CHUNK);
    OK(ps_read_open(s, k, &old, &held, &error));
    OK(budget_value_write(s, k, b, n, PS_CHUNK + 1, &next, &error));
    CHECK(old.incarnation != next.incarnation);
    BAD(ps_read_open(s, k, &old, &rejected, &error));
    CHECK(!rejected);
    expect(s, k, &next, b, n);
    OK(ps_checkpoint(s, &error));
    read_value(held, a, n);
    ps_reader_close(held);
    OK(ps_status(s, &stats, &error));
    CHECK(stats.capacity == PS_DEFAULT_CAPACITY && !stats.budgets &&
          !stats.private_bytes && !stats.retired_bytes &&
          !stats.reserved_payload_bytes && !stats.reserved_metadata_bytes &&
          !stats.reserved_entries);
    OK(ps_close(s, &error));
    s = reopen(NULL);
    expect(s, k, &next, b, n);
    OK(ps_delete(s, k, &error));
    OK(ps_close(s, &error));
    s = reopen(NULL);
    absent(s, k);
  }
  OK(ps_close(s, &error));
  free(a);
  free(b);
  clean();
}
static void value_budget_process(void) {
  size_t size = PS_CHUNK + 17;
  unsigned char *bytes = malloc(size);
  ps_key k = key(PS_CONTROL, 101, 1);
  ps_options options = {0};
  ps_space *s;
  pid_t child;
  int status;
  CHECK(bytes);
  memset(bytes, 0x72, size);
  for (int durable = 0; durable < 2; ++durable) {
    clean();
    s = create(0, durable, NULL);
    OK(ps_close(s, &error));
    options.durable_sync = durable;
    for (int remove = 0; remove < 2; ++remove) {
      child = fork();
      CHECK(child >= 0);
      if (!child) {
        int rc = ps_open(path, 0, &options, &s, &error);
        if (rc == LC_OK)
          rc = remove
                   ? ps_delete(s, k, &error)
                   : budget_value_write(s, k, bytes, size, 317, NULL, &error);
        /* Terminate without root close, checkpoint or exit handlers. */
        _exit(rc == LC_OK ? 0 : 1);
      }
      CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0);
      s = reopen(NULL);
      if (remove)
        absent(s, k);
      else
        expect(s, k, NULL, bytes, size);
      OK(ps_close(s, &error));
    }
  }
  free(bytes);
  clean();
}
static void value_budget_rejections(void) {
  ps_space *s;
  ps_writer *w = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan;
  ps_stats stats;
  unsigned char *bytes = malloc(PS_CHUNK + 17);
  CHECK(bytes);
  memset(bytes, 0x37, PS_CHUNK + 17);
  clean();
  s = create(0, 0, NULL);
  for (int kind = PS_CONTROL; kind <= PS_ROUTING; ++kind) {
    ps_key k = key((unsigned int)kind, 101, 1);
    for (int funded = 0; funded < 2; ++funded) {
      if (kind == PS_CONTROL && funded)
        continue;
      plan = (ps_budget_plan){0};
      OK(ps_budget_plan_value(&plan, PS_PAGE + 1, &error));
      if (funded)
        OK(ps_budget_open(s, &plan, &budget, &error));
      OK(ps_begin(s, budget, k, &w, &error));
      BAD(ps_write(w, bytes, PS_PAGE + 1, &error));
      OK(ps_write(w, bytes, PS_PAGE, &error));
      BAD(ps_write(w, bytes, 1, &error));
      OK(ps_write(w, NULL, 0, &error));
      OK(ps_publish(w, NULL, &error));
      ps_writer_close(w);
      OK(ps_budget_close(budget, &error));
      budget = NULL;
      expect(s, k, NULL, bytes, PS_PAGE);
      OK(ps_delete(s, k, &error));
    }
  }
  ps_key k = key(PS_CONTROL, 101, 1);
  put(s, k, bytes, PS_PAGE, PS_PAGE);
  for (int kind = 0; kind < 3; ++kind) {
    plan = (ps_budget_plan){0};
    OK(ps_budget_plan_value(&plan, PS_CHUNK + 17, &error));
    if (kind == 0)
      plan.payload_bytes = PS_CHUNK;
    if (kind == 1)
      plan.metadata_bytes = 0;
    if (kind == 2)
      plan.entries = 0;
    /* Test new binding entry credits, or replacement growth/payload credits. */
    ps_key target = kind == 2 ? key(PS_CONTROL, 101, 2) : k;
    OK(ps_budget_open(s, &plan, &budget, &error));
    OK(ps_begin(s, budget, target, &w, &error));
    int rc = ps_write(w, bytes, PS_CHUNK + 17, &error);
    if (rc == LC_OK)
      rc = ps_publish(w, NULL, &error);
    CHECK(rc == LC_ERR_INVALID);
    lc_error_cleanup(&error);
    ps_writer_close(w);
    OK(ps_budget_close(budget, &error));
    expect(s, k, NULL, bytes, PS_PAGE);
    absent(s, key(PS_CONTROL, 101, 2));
    OK(ps_checkpoint(s, &error));
    OK(ps_status(s, &stats, &error));
    CHECK(!stats.private_bytes && !stats.budgets &&
          !stats.reserved_payload_bytes && !stats.reserved_metadata_bytes &&
          !stats.reserved_entries);
  }
  OK(budget_value_write(s, k, bytes, PS_CHUNK + 17, 1, NULL, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, k, NULL, bytes, PS_CHUNK + 17);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void value_budget_capacity(int durable, int fragmented) {
  ps_space *s;
  ps_key k = key(PS_CONTROL, 101, 1);
  ps_budget *budget = NULL, *peer = NULL;
  ps_budget_plan plan = {0}, floor = {PS_MAINTENANCE_RESERVE, 0, 0, 1};
  ps_writer *w = NULL;
  ps_stats before, after;
  size_t size = 6 * PS_CHUNK + 17;
  unsigned char *bytes = malloc(size);
  uint64_t next = 1, end;
  CHECK(bytes);
  memset(bytes, 0x7d, size);
  clean();
  s = create(0, durable, NULL);
  if (fragmented) {
    budget_fill(s, &next, bytes);
    end = next - 1;
    for (uint64_t i = 1; i < end; i += 2)
      OK(ps_delete(s, state(i), &error));
    OK(ps_checkpoint(s, &error));
  }
  OK(ps_budget_plan_value(&plan, size, &error));
  OK(ps_budget_open(s, &plan, &budget, &error));
  budget_fill(s, &next, bytes);
  OK(ps_checkpoint(s, &error));
  OK(ps_budget_open(s, &floor, &peer, &error));
  OK(ps_status(s, &before, &error));
  CHECK(before.reserved_payload_bytes ==
        plan.payload_bytes + floor.payload_bytes);
  OK(ps_begin(s, budget, k, &w, &error));
  OK(ps_write(w, bytes, 3 * PS_CHUNK, &error));
  OK(ps_metadata_repack(s, &error));
  OK(ps_write(w, bytes + 3 * PS_CHUNK, size - 3 * PS_CHUNK, &error));
  OK(ps_publish(w, NULL, &error));
  ps_writer_close(w);
  OK(ps_budget_close(peer, &error));
  OK(ps_budget_close(budget, &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.entries == before.entries + 1 &&
        after.metadata_bytes - before.metadata_bytes <= plan.metadata_bytes &&
        after.capacity == PS_DEFAULT_CAPACITY && !after.budgets &&
        !after.reserved_payload_bytes && !after.reserved_metadata_bytes &&
        !after.reserved_entries);
  expect(s, k, NULL, bytes, size);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, k, NULL, bytes, size);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void value_budget_faults(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_key k = key(PS_CONTROL, 101, 1);
  ps_reader *r = NULL;
  ps_stats stats;
  size_t size = PS_CHUNK + PS_PAGE + 37, sites;
  unsigned char *bytes = malloc(size), *baseline = malloc((size_t)PS_MIB);
  int rc;
  CHECK(bytes && baseline);
  memset(bytes, 0x7d, size);
  for (int action = 0; action < 3; ++action) {
    clean();
    s = create(PS_MIB, durable, &io);
    if (!action)
      put(s, k, "prior", 5, 5);
    else
      OK(budget_value_write(s, k, bytes, size, 317, NULL, &error));
    OK(ps_checkpoint(s, &error));
    /* Dirty a large descriptor again so checkpoint exercises its publication.
     */
    if (action == 1)
      OK(budget_value_write(s, k, bytes, size, 317, NULL, &error));
    OK(ps_close(s, &error));
    memcpy(baseline, m.stable, (size_t)PS_MIB);
    for (int kind = 0; kind < 2; ++kind) {
      sites = 0;
      for (size_t cut = 0; cut <= sites; ++cut) {
        memcpy(m.stable, baseline, (size_t)PS_MIB);
        m.stable_bytes = PS_MIB;
        crash(&m);
        s = reopen(&io);
        /* Give checkpoint a fresh replacement after every reopen. */
        if (action == 1)
          OK(budget_value_write(s, k, bytes, size, 317, NULL, &error));
        m.operations = 0;
        m.fail_at = kind == 0 ? cut : 0;
        m.tear = m.persist_torn = 1;
        allocation_call = 0;
        allocation_fail = kind == 1 ? (cut ? cut : (size_t)-1) : 0;
        rc = action == 0
                 ? budget_value_write(s, k, bytes, size, 317, NULL, &error)
             : action == 1 ? ps_checkpoint(s, &error)
                           : ps_delete(s, k, &error);
        allocation_fail = m.fail_at = 0;
        int completed = rc == LC_OK;
        if (!cut) {
          OK(rc);
          sites = kind == 0 ? m.operations : allocation_call;
          CHECK(sites);
        } else {
          CHECK(kind == 0 ? rc == LC_ERR_TRANSPORT
                          : (rc == LC_ERR_NOMEM || rc == LC_OK));
          if (kind)
            CHECK(allocation_call >= cut);
          lc_error_cleanup(&error);
        }
        OK(ps_close(s, &error));
        crash(&m);
        s = reopen(&io);
        rc = ps_read_open(s, k, NULL, &r, &error);
        if (rc == LC_OK) {
          size_t n = (size_t)ps_read_size(r);
          CHECK(n == size || (!action && cut && n == 5));
          if (!action && (completed || kind == 1))
            CHECK(n == (completed ? size : 5));
          if (action == 2)
            CHECK(!completed);
          read_value(r, n == 5 ? (const void *)"prior" : bytes, n);
          ps_reader_close(r);
        } else {
          CHECK(action == 2 && (completed || kind == 0));
          CHECK(rc == LC_ERR_INVALID && !r);
          lc_error_cleanup(&error);
        }
        OK(ps_status(s, &stats, &error));
        CHECK(!stats.budgets && !stats.reserved_payload_bytes &&
              !stats.reserved_metadata_bytes && !stats.reserved_entries);
        OK(budget_value_write(s, k, bytes, size, 317, NULL, &error));
        expect(s, k, NULL, bytes, size);
        OK(ps_close(s, &error));
      }
      printf("value budget action%d sync%d %s: %lu failure sites\n", action,
             durable, kind ? "allocation" : "write/sync", (unsigned long)sites);
    }
  }
  free(bytes);
  free(baseline);
  free(m.stable);
  clean();
}
typedef struct value_budget_actor {
  ps_space *space;
  const unsigned char *bytes;
  size_t size;
  int remove, rc;
} value_budget_actor;
static void *value_budget_worker(void *context) {
  value_budget_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  ps_key k = key(PS_CONTROL, 101, 1);
  a->rc = a->remove ? ps_delete(a->space, k, &e)
                    : budget_value_write(a->space, k, a->bytes, a->size, 317,
                                         NULL, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void value_budget_concurrency(void) {
  size_t size = PS_CHUNK + 17;
  unsigned char *bytes = malloc(size);
  ps_key k = key(PS_CONTROL, 101, 1);
  CHECK(bytes);
  memset(bytes, 0x59, size);
  for (int durable = 0; durable < 2; ++durable) {
    for (int phase = 0; phase < 3; ++phase) {
      commit_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
      ps_io io = {&g, gate_read, gate_write, gate_sync, gate_reserve, NULL};
      value_budget_actor actor = {NULL, bytes, size, phase == 2, -1};
      pthread_t thread;
      ps_reader *held = NULL;
      clean();
      ps_space *s = create(0, durable, &io);
      if (phase == 2)
        OK(budget_value_write(s, k, bytes, size, 317, NULL, &error));
      else
        put(s, k, "prior", 5, 5);
      OK(ps_read_open(s, k, NULL, &held, &error));
      actor.space = s;
      g.enabled = 1;
      g.hold_payload = phase == 0;
      CHECK(pthread_create(&thread, NULL, value_budget_worker, &actor) == 0);
      await_gate(&g, 0);
      expect(s, k, NULL, phase == 2 ? (const void *)bytes : "prior",
             phase == 2 ? size : 5);
      if (!phase) {
        /* This peer must complete while the producer's payload I/O is held. */
        put(s, key(PS_CONTROL, 102, 1), "peer", 4, 1);
        OK(ps_metadata_repack(s, &error));
        expect(s, key(PS_CONTROL, 102, 1), NULL, "peer", 4);
      }
      pthread_mutex_lock(&g.mutex);
      g.released = 1;
      pthread_cond_broadcast(&g.cond);
      pthread_mutex_unlock(&g.mutex);
      CHECK(pthread_join(thread, NULL) == 0 && actor.rc == LC_OK);
      read_value(held, phase == 2 ? (const void *)bytes : "prior",
                 phase == 2 ? size : 5);
      ps_reader_close(held);
      if (phase == 2)
        absent(s, k);
      else
        expect(s, k, NULL, bytes, size);
      OK(ps_close(s, &error));
      s = reopen(NULL);
      if (phase == 2)
        absent(s, k);
      else
        expect(s, k, NULL, bytes, size);
      OK(ps_close(s, &error));
      CHECK(pthread_cond_destroy(&g.cond) == 0 &&
            pthread_mutex_destroy(&g.mutex) == 0);
    }
  }
  free(bytes);
  clean();
}
static void grant_budget_plans(void) {
  ps_budget_plan p = {0}, before;
  OK(ps_budget_plan_stream(&p, &error));
  OK(ps_budget_plan_catalog(&p, 7, &error));
  OK(ps_budget_plan_routing(&p, 1, &error));
  OK(ps_budget_plan_routing(&p, 0, &error));
  OK(ps_budget_plan_span(&p, PS_PAGE - PS_RUN_HEADER + 1, &error));
  CHECK(p.payload_bytes == 4 * PS_PAGE &&
        p.metadata_bytes ==
            80 + 111 + 112 + 24 + 2 * PS_OWNED_DESCRIPTOR_BYTES &&
        p.entries == 5 && !p.maintenance);
  before = p;
  BAD(ps_budget_plan_span(&p, UINT64_MAX, &error));
  BAD(ps_budget_plan_catalog(&p, PS_CATALOG_NAME_MAX + 1, &error));
  BAD(ps_budget_plan_routing(&p, 2, &error));
  CHECK(p.payload_bytes == before.payload_bytes &&
        p.metadata_bytes == before.metadata_bytes &&
        p.entries == before.entries);
  BAD(ps_budget_plan_span(NULL, 0, &error));
  BAD(ps_budget_plan_stream(NULL, &error));
  for (int kind = 0; kind < 3; ++kind) {
    memset(&p, 0, sizeof(p));
    if (kind == 0)
      p.payload_bytes = UINT64_MAX;
    if (kind == 1)
      p.metadata_bytes = UINT64_MAX;
    if (kind == 2)
      p.entries = UINT64_MAX;
    before = p;
    BAD(ps_budget_plan_span(&p, 1, &error));
    CHECK(p.payload_bytes == before.payload_bytes &&
          p.metadata_bytes == before.metadata_bytes &&
          p.entries == before.entries);
  }
  memset(&p, 0, sizeof(p));
  OK(ps_budget_plan_span(&p, 0, &error));
  CHECK(!p.payload_bytes && !p.metadata_bytes && !p.entries);
}
static void routing_budget_packing(void) {
  unsigned char bytes[PS_PAGE];
  memset(bytes, 0x6b, sizeof(bytes));
  for (int durable = 0; durable < 2; ++durable)
    for (int replace = 0; replace < 2; ++replace) {
      clean();
      ps_space *s = create(PS_DEFAULT_CAPACITY, durable, NULL);
      ps_key route = key(PS_ROUTING, 10, 2);
      if (replace)
        (void)put(s, route, bytes, 128, 128);
      /* Leave exactly one cell; the new 128-byte head must split over two
       * extents, including when replacing a one-extent head. */
      size_t fill = PS_PAGE - (replace ? 3 : 1) * PS_CELL;
      (void)put(s, state(1), bytes, fill, fill);
      ps_budget_plan plan = {0}, remaining;
      ps_budget *budget = NULL;
      ps_writer *writer = NULL;
      OK(ps_budget_plan_routing(&plan, !replace, &error));
      OK(ps_budget_open(s, &plan, &budget, &error));
      OK(ps_begin(s, budget, route, &writer, &error));
      OK(ps_write(writer, bytes, 128, &error));
      OK(ps_publish(writer, NULL, &error));
      ps_writer_close(writer);
      OK(ps_budget_status(budget, &remaining, &error));
      CHECK(!remaining.metadata_bytes);
      OK(ps_budget_close(budget, &error));
      expect(s, route, NULL, bytes, 128);
      OK(ps_flush(s, &error));
      OK(ps_close(s, &error));
      s = reopen(NULL);
      expect(s, route, NULL, bytes, 128);
      OK(ps_close(s, &error));
    }
  clean();
}
static void grant_budget_capacity(int durable, int fragmented, int preferred) {
  ps_space *s;
  ps_budget *budget = NULL, *peer = NULL;
  ps_budget_plan plan = {0}, floor = {PS_MAINTENANCE_RESERVE, 0, 0, 1};
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_reader *reader = NULL;
  ps_stream_info info;
  ps_catalog_info binding;
  ps_stats stats;
  unsigned char *bytes = malloc(6 * PS_CHUNK + 17);
  uint64_t next = 1, end;
  CHECK(bytes);
  memset(bytes, 0x7b, 6 * PS_CHUNK + 17);
  clean();
  s = create(fragmented && !preferred ? PS_MIB : 3 * PS_MIB, durable, NULL);
  if (fragmented) {
    budget_fill(s, &next, bytes);
    end = next - 1;
    for (uint64_t i = 1; i < end; i += 2)
      OK(ps_delete(s, state(i), &error));
    OK(ps_checkpoint(s, &error));
  }
  OK(ps_budget_plan_stream(&plan, &error));
  OK(ps_budget_plan_catalog(&plan, 4, &error));
  if (preferred) {
    OK(ps_budget_plan_preferred_span(&plan, 6 * PS_CHUNK + 17, PS_PAGE,
                                     &error));
    OK(ps_budget_plan_routing(&plan, 1, &error));
  } else
    OK(ps_budget_plan_span(&plan, 6 * PS_CHUNK + 17, &error));
  OK(ps_budget_open(s, &plan, &budget, &error));
  OK(ps_stream_open(s, budget, stream_key(1), 1, NULL, NULL, NULL, &stream,
                    &error));
  budget_fill(s, &next, bytes);
  OK(ps_checkpoint(s, &error));
  OK(ps_budget_open(s, &floor, &peer, &error));
  OK(ps_stream_begin(stream, &writer, &error));
  if (preferred) {
    ps_budget_plan remaining;
    OK(ps_stream_reserve(writer, budget, 6 * PS_CHUNK + 17, &error));
    OK(ps_budget_status(budget, &remaining, &error));
    CHECK(remaining.payload_bytes >= PS_PAGE &&
          remaining.metadata_bytes >= 88 + 108);
  } else
    OK(ps_stream_reserve_exact(writer, budget, 6 * PS_CHUNK + 17, &error));
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.capacity >= 6 * PS_CHUNK + 17 && !info.tail &&
        (!fragmented || info.runs > 10));
  for (size_t at = 0; at < 6 * PS_CHUNK + 17;) {
    size_t n = 6 * PS_CHUNK + 17 - at;
    if (n > 317)
      n = 317;
    OK(ps_stream_write(writer, bytes + at, n, &error));
    at += n;
  }
  OK(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  ps_stream_writer_close(writer);
  OK(ps_stream_seal(stream, &error));
  if (preferred) {
    ps_writer *route = NULL;
    OK(ps_begin(s, budget, key(PS_ROUTING, 10, 2), &route, &error));
    OK(ps_write(route, "route", 5, &error));
    OK(ps_publish(route, NULL, &error));
    ps_writer_close(route);
  }
  OK(ps_catalog_put(s, budget, 10, PS_CATALOG_BODY, "pack", 4, NULL,
                    stream_key(1), &info.ref, &binding, &error));
  OK(ps_budget_close(peer, &error));
  OK(ps_budget_close(budget, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(!stats.budgets && !stats.reserved_payload_bytes &&
        !stats.reserved_metadata_bytes && !stats.reserved_entries &&
        stats.metadata_available_bytes >= stats.metadata_workspace_bytes);
  OK(ps_stream_read_open(stream, 0, 6 * PS_CHUNK + 17, &reader, &error));
  stream_reader_expect(reader, bytes, 6 * PS_CHUNK + 17);
  ps_stream_reader_close(reader);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  if (preferred)
    expect(s, key(PS_ROUTING, 10, 2), NULL, "route", 5);
  OK(ps_stream_open(s, NULL, stream_key(1), 0, &info.ref, NULL, NULL, &stream,
                    &error));
  OK(ps_stream_read_open(stream, 0, 6 * PS_CHUNK + 17, &reader, &error));
  stream_reader_expect(reader, bytes, 6 * PS_CHUNK + 17);
  ps_stream_reader_close(reader);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void grant_budget_rejections(void) {
  ps_space *s, *foreign = NULL;
  ps_owned_head *head = NULL, *other_head = NULL;
  ps_owned_status status;
  ps_owned_run *run = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {PS_PAGE, PS_OWNED_DESCRIPTOR_BYTES, 1, 0}, remaining;
  ps_stats before, after;
  int found;
  clean();
  s = create(PS_MIB, 0, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &status, &error));
  for (int kind = 0; kind < 3; ++kind) {
    plan.payload_bytes = kind == 0 ? 0 : PS_PAGE;
    plan.metadata_bytes = PS_OWNED_DESCRIPTOR_BYTES - (kind == 1);
    plan.entries = kind == 2 ? 0 : 1;
    OK(ps_budget_open(s, &plan, &budget, &error));
    OK(ps_status(s, &before, &error));
    BAD(ps_owned_grant(head, budget, 0, PS_PAGE, &run, &found, &error));
    CHECK(!run && !found);
    OK(ps_status(s, &after, &error));
    CHECK(after.entries == before.entries &&
          after.allocated_bytes == before.allocated_bytes &&
          after.journal_bytes == before.journal_bytes);
    OK(ps_budget_status(budget, &remaining, &error));
    CHECK(remaining.payload_bytes == plan.payload_bytes &&
          remaining.metadata_bytes == plan.metadata_bytes &&
          remaining.entries == plan.entries);
    OK(ps_budget_close(budget, &error));
  }
  plan.payload_bytes = PS_PAGE;
  plan.metadata_bytes = PS_OWNED_DESCRIPTOR_BYTES;
  plan.entries = 1;
  OK(ps_budget_open(s, &plan, &budget, &error));
  BAD(ps_owned_grant(head, budget, 1, PS_PAGE, &run, &found, &error));
  OK(ps_open(other, 1, NULL, &foreign, &error));
  OK(ps_owned_open(foreign, NULL, stream_key(1), 1, NULL, &other_head, &status,
                   &error));
  BAD(ps_owned_grant(other_head, budget, 0, PS_PAGE, &run, &found, &error));
  CHECK(!run && !found);
  ps_owned_close(other_head);
  OK(ps_close(foreign, &error));
  OK(ps_budget_close(budget, &error));
  ps_owned_close(head);
  OK(ps_close(s, &error));
  clean();
}
typedef struct funded_grant_actor {
  ps_owned_head *head;
  ps_budget *budget;
  ps_owned_run *run;
  int rc, found;
} funded_grant_actor;
static void *funded_grant_produce(void *context) {
  funded_grant_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  a->rc =
      ps_owned_grant(a->head, a->budget, 0, PS_PAGE, &a->run, &a->found, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void grant_budget_concurrency(void) {
  commit_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&g, gate_read, gate_write, gate_sync, gate_reserve, NULL};
  ps_space *s;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {PS_PAGE, PS_OWNED_DESCRIPTOR_BYTES, 1, 0}, remaining;
  funded_grant_actor actor = {0};
  pthread_t thread;
  unsigned char *bytes = malloc(PS_PAGE);
  uint64_t next = 600;
  CHECK(bytes);
  memset(bytes, 0x5b, PS_PAGE);
  clean();
  s = create(2 * PS_MIB, 0, &io);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &status, &error));
  OK(ps_budget_open(s, &plan, &budget, &error));
  actor.head = head;
  actor.budget = budget;
  g.enabled = g.hold_payload = 1;
  CHECK(pthread_create(&thread, NULL, funded_grant_produce, &actor) == 0);
  await_gate(&g, 0);
  BAD(ps_budget_close(budget, &error));
  OK(ps_budget_status(budget, &remaining, &error));
  CHECK(!remaining.payload_bytes &&
        remaining.metadata_bytes == PS_OWNED_DESCRIPTOR_BYTES &&
        remaining.entries == 1);
  for (uint64_t i = 0; i < 512; ++i)
    put(s, state(i), NULL, 0, 1);
  OK(ps_metadata_repack(s, &error));
  budget_fill(s, &next, bytes);
  OK(ps_checkpoint(s, &error));
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(actor.rc == LC_OK && actor.found && actor.run);
  OK(ps_budget_status(budget, &remaining, &error));
  CHECK(!remaining.payload_bytes && !remaining.metadata_bytes &&
        !remaining.entries);
  OK(ps_owned_read(actor.run, 0, bytes, PS_PAGE - PS_RUN_HEADER, &error));
  for (size_t i = 0; i < PS_PAGE - PS_RUN_HEADER; ++i)
    CHECK(!bytes[i]);
  ps_owned_run_close(actor.run);
  OK(ps_budget_close(budget, &error));
  ps_owned_close(head);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 0, &status.ref, &head, &status,
                   &error));
  CHECK(status.runs == 1 && status.capacity == PS_PAGE - PS_RUN_HEADER);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  CHECK(pthread_cond_destroy(&g.cond) == 0);
  CHECK(pthread_mutex_destroy(&g.mutex) == 0);
  free(bytes);
  clean();
}
static void append_budget_planning(void) {
  const uint64_t spans[] = {1, 4032, 4033, 30000, PS_MIB};
  const uint64_t grants[] = {PS_PAGE, 16 * PS_PAGE, PS_MIB};
  for (size_t i = 0; i < sizeof(spans) / sizeof(*spans); ++i)
    for (size_t j = 0; j < sizeof(grants) / sizeof(*grants); ++j) {
      ps_budget_plan exact = {0}, preferred = {0};
      OK(ps_budget_plan_span(&exact, spans[i], &error));
      OK(ps_budget_plan_preferred_span(&preferred, spans[i], grants[j],
                                       &error));
      CHECK(preferred.payload_bytes >= exact.payload_bytes &&
            preferred.payload_bytes - exact.payload_bytes < PS_MIB &&
            preferred.metadata_bytes == exact.metadata_bytes &&
            preferred.entries == exact.entries);
    }
  ps_budget_plan p = {0}, before;
  OK(ps_budget_plan_preferred_span(&p, 0, PS_PAGE, &error));
  CHECK(!p.payload_bytes && !p.entries);
  BAD(ps_budget_plan_preferred_span(&p, 1, 1, &error));
  p.payload_bytes = UINT64_MAX - PS_PAGE;
  before = p;
  BAD(ps_budget_plan_preferred_span(&p, 1, PS_MIB, &error));
  CHECK(p.payload_bytes == before.payload_bytes && !p.metadata_bytes &&
        !p.entries);
  BAD(ps_budget_plan_preferred_span(&p, UINT64_MAX, PS_PAGE, &error));
  CHECK(p.payload_bytes == before.payload_bytes && !p.metadata_bytes &&
        !p.entries);
}
static void append_growth_high_water(void) {
  unsigned char *bytes = malloc((size_t)PS_MIB);
  ps_stream_info before, clipped, after;
  ps_stream_writer *w = NULL;
  uint64_t at, fill;
  CHECK(bytes);
  memset(bytes, 0x73, (size_t)PS_MIB);
  clean();
  ps_space *s = create(PS_DEFAULT_CAPACITY, 0, NULL);
  ps_stream *stream = stream_open(s, 1, 1, NULL, NULL);
  OK(canonical_append(stream, bytes, 400000, 317, 1, &at, &error));
  OK(ps_stream_status(stream, &before, &error));
  CHECK(before.preferred_grant_bytes == PS_STREAM_PIECE_BYTES);
  OK(ps_stream_begin(stream, &w, &error));
  OK(ps_stream_reserve_exact(w, NULL, before.capacity - before.tail + 1,
                             &error));
  ps_stream_writer_close(w);
  OK(ps_stream_status(stream, &clipped, &error));
  CHECK(clipped.capacity - before.capacity == PS_PAGE - PS_RUN_HEADER &&
        clipped.preferred_grant_bytes == PS_STREAM_PIECE_BYTES);
  fill = clipped.capacity - clipped.tail - 43;
  CHECK(fill <= PS_MIB);
  OK(canonical_append(stream, bytes, (size_t)fill, 317, 2, &at, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  stream = stream_open(s, 1, 0, &clipped.ref, NULL);
  OK(ps_stream_status(stream, &before, &error));
  CHECK(before.tail == before.capacity &&
        before.preferred_grant_bytes == PS_STREAM_PIECE_BYTES);
  OK(canonical_append(stream, bytes, 31, 7, 3, &at, &error));
  OK(ps_stream_status(stream, &after, &error));
  CHECK(after.capacity - before.capacity ==
        PS_STREAM_PIECE_BYTES - PS_RUN_HEADER);
  stream_expect(stream, at, bytes, 31, 3);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static uint64_t piece_physical(uint64_t owner) {
  unsigned char header[PS_RUN_IDENTITY_BYTES];
  uint64_t at;
  int fd = open(path, O_RDONLY);
  CHECK(fd >= 0);
  for (at = PS_DATA; at < 3 * PS_MIB; at += PS_PAGE) {
    CHECK(pread(fd, header, sizeof(header), (off_t)at) == sizeof(header));
    if (fixture_get64(header) == UINT64_C(0x314e55524f5350) &&
        fixture_get64(header + 16) == owner && !fixture_get64(header + 24) &&
        fixture_get64(header + 32) == at)
      break;
  }
  CHECK(close(fd) == 0 && at < 3 * PS_MIB);
  return at;
}
static ps_space *piece_reopen(int durable, const ps_io *io) {
  ps_options options = {0};
  ps_space *space = NULL;
  options.durable_sync = durable;
  options.io = io;
  OK(ps_open(path, 0, &options, &space, &error));
  return space;
}
static void piece_incremental(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream;
  ps_stream_info info;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  ps_owned_run *run = NULL;
  unsigned char body[64], scratch[PS_CHUNK], original, wrong,
      root[PS_ROOT_BYTES];
  uint64_t first, second, physical;
  int fd, found;
  memset(body, 0x65, sizeof(body));
  clean();
  space = create(3 * PS_MIB, durable, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(canonical_append(stream, body, sizeof(body), 7, 1, &first, &error));
  OK(ps_stream_status(stream, &info, &error));
  physical = piece_physical(info.ref.incarnation) + PS_RUN_HEADER + 43;
  fd = open(path, O_RDWR);
  CHECK(fd >= 0 && pread(fd, &original, 1, (off_t)physical) == 1);
  wrong = original ^ 1;
  CHECK(pwrite(fd, &wrong, 1, (off_t)physical) == 1);
  m.read_bytes = 0;
  OK(canonical_append(stream, body, 31, 7, 2, &second, &error));
  CHECK(m.read_bytes == 31 + 43); /* unchanged prefix is never read again */
  OK(ps_stream_status(stream, &info, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_owned_open(space, NULL, stream_key(1), 0, &info.ref, &head, &status,
                   &error));
  OK(ps_owned_grant(head, NULL, 0, 0, &run, &found, &error));
  CHECK(found && ps_owned_piece_verify(run, info.tail, scratch, &error) ==
                     LC_ERR_PROTOCOL); /* extension cannot bless old damage */
  lc_error_cleanup(&error);
  CHECK(pwrite(fd, &original, 1, (off_t)physical) == 1);
  OK(ps_owned_piece_verify(run, info.tail, scratch, &error));
  ps_owned_run_close(run);
  ps_owned_close(head);
  CHECK(close(fd) == 0);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, first, body, sizeof(body), 1);
  stream_expect(stream, second, body, 31, 2);
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  /* Bind the physical prologue width directly, without a format-version bump
   * or a legacy reader that might misinterpret old payload offsets. */
  fd = open(path, O_RDWR);
  CHECK(fd >= 0);
  for (int slot = 0; slot < 2; ++slot) {
    CHECK(pread(fd, root, sizeof(root), (off_t)(slot * PS_PAGE)) ==
          sizeof(root));
    if (fixture_get64(root)) {
      CHECK((uint32_t)fixture_get64(root + 72) == PS_ROOT_LAYOUT);
      fixture_put(root + 72, PS_RUN_IDENTITY_BYTES, 4);
      fixture_put(root + 76, crc32(0, root, 76), 4);
      CHECK(pwrite(fd, root, sizeof(root), (off_t)(slot * PS_PAGE)) ==
            sizeof(root));
    }
  }
  CHECK(close(fd) == 0);
  space = NULL;
  CHECK(ps_open(path, 0, NULL, &space, &error) == LC_ERR_PROTOCOL && !space);
  lc_error_cleanup(&error);
  free(m.stable);
  clean();
}
static void piece_recovery_partial(int durable, int fault) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream = NULL;
  ps_stream_info info;
  ps_stats before, after;
  ps_owned_head *head = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status owned;
  ps_piece_checksum checksum = {0};
  unsigned char body[64], filler[PS_PAGE], scratch[PS_CHUNK];
  uint64_t first, physical, next;
  size_t fill;
  int found;
  memset(body, 0x75, sizeof(body));
  clean();
  space = create(3 * PS_MIB, durable, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(canonical_append(stream, body, sizeof(body), 7, 1, &first, &error));
  OK(ps_stream_status(stream, &info, &error));
  physical = piece_physical(info.ref.incarnation) + PS_RUN_IDENTITY_BYTES;
  OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  memset(m.stable + physical, 0, PS_RUN_HEADER - PS_RUN_IDENTITY_BYTES);
  crash(&m);
  space = piece_reopen(durable, &io);
  stream = NULL;
  if (fault) {
    m.operations = 0;
    m.fail_at = 1;
    m.tear = m.persist_torn = 1;
    CHECK(ps_stream_open(space, NULL, stream_key(1), 0, &info.ref,
                         canonical_recover, NULL, &stream,
                         &error) == LC_ERR_TRANSPORT &&
          !stream);
    lc_error_cleanup(&error);
    m.fail_at = 0;
    OK(ps_close(space, &error));
    crash(&m);
    space = piece_reopen(durable, &io);
  }
  OK(ps_status(space, &before, &error));
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == sizeof(body) + 43);
  OK(ps_status(space, &after, &error));
  CHECK(after.syncs == before.syncs &&
        after.journal_bytes == before.journal_bytes);
  /* The accepted canonical tail did not advance, but the repaired footer is
   * fresh readiness. A peer flush must cover it exactly once. */
  OK(ps_flush(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.syncs == before.syncs + 1);
  OK(ps_flush(space, &error));
  OK(ps_status(space, &before, &error));
  CHECK(before.syncs == after.syncs);
  stream_expect(stream, first, body, sizeof(body), 1);
  /* Filling this piece must carry the reconstructed-prefix obligation forward.
   * Its CRC cannot be used as independent proof for relocation until the
   * canonical body owner validates the inherited bytes. */
  fill = (size_t)(info.capacity - info.tail - 43);
  CHECK(fill <= sizeof(filler));
  memset(filler, 0x76, sizeof(filler));
  OK(canonical_append(stream, filler, fill, 31, 2, &next, &error));
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.tail == info.capacity);
  OK(ps_stream_close(stream, &error));
  OK(ps_owned_open(space, NULL, stream_key(1), 0, &info.ref, &head, &owned,
                   &error));
  OK(ps_owned_grant(head, NULL, 0, 0, &run, &found, &error));
  CHECK(found);
  OK(ps_owned_piece_inspect(run, info.tail, &checksum, &error));
  CHECK(checksum.reconstructed);
  CHECK(ps_owned_piece_verify(run, info.tail, scratch, &error) ==
        LC_ERR_INVALID);
  CHECK(error.server_code &&
        !strcmp(error.server_code, "pouch_piece_needs_validation"));
  lc_error_cleanup(&error);
  ps_owned_run_close(run);
  ps_owned_close(head);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, first, body, sizeof(body), 1);
  stream_expect(stream, next, filler, fill, 2);
  OK(ps_stream_seal(stream, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = piece_reopen(durable, &io);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, first, body, sizeof(body), 1);
  stream_expect(stream, next, filler, fill, 2);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  free(m.stable);
  clean();
}
static void bounded_pieces(int durable, int checkpointed) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_stream *stream, *rejected = NULL;
  ps_stream_info info;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  ps_owned_run *run = NULL;
  ps_piece_checksum checksum = {0};
  recovery_trace trace = {0};
  const size_t length = 3 * PS_STREAM_PIECE_BYTES + 17;
  unsigned char *body = malloc(length);
  unsigned char *scratch = malloc(PS_CHUNK);
  unsigned char footer[PS_RUN_HEADER - PS_RUN_IDENTITY_BYTES], damaged[32];
  unsigned char byte, wrong;
  uint64_t base, bytes, first, offset, physical;
  int found, fd, rc, checked_late_window = 0;
  CHECK(body && scratch);
  for (size_t i = 0; i < length; ++i)
    body[i] = (unsigned char)(i * 17U + 3U);
  clean();
  space = create(3 * PS_MIB, durable, &io);
  stream = stream_open(space, 1, 1, NULL, NULL);
  OK(ps_checkpoint(space,
                   &error)); /* zero-tail anchor for candidate recovery */
  OK(canonical_append(stream, body, length, 37, 1, &offset, &error));
  stream_expect(stream, offset, body, length, 1);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.runs >= 4 && info.preferred_grant_bytes == PS_STREAM_PIECE_BYTES);
  if (checkpointed)
    OK(ps_checkpoint(space, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_owned_open(space, NULL, stream_key(1), 0, &info.ref, &head, &status,
                   &error));
  base = first = 0;
  for (uint64_t i = 0; i < status.runs; ++i) {
    OK(ps_owned_grant(head, NULL, base, 0, &run, &found, &error));
    CHECK(found);
    bytes = ps_owned_run_capacity(run);
    CHECK(bytes && bytes + PS_RUN_HEADER <= PS_STREAM_PIECE_BYTES);
    if (base + bytes <= info.tail) {
      m.read_bytes = 0;
      OK(ps_owned_piece_verify(run, bytes, scratch, &error));
      CHECK(m.read_bytes == bytes + sizeof(footer));
      if (!checked_late_window && bytes > PS_CHUNK + 37) {
        uint64_t late = PS_CHUNK + 37;
        OK(ps_owned_read(run, late, &byte, 1, &error));
        wrong = byte ^ 1;
        OK(ps_owned_write(run, late, &wrong, 1, &error));
        CHECK(ps_owned_piece_verify(run, bytes, scratch, &error) ==
              LC_ERR_PROTOCOL);
        lc_error_cleanup(&error);
        OK(ps_owned_write(run, late, &byte, 1, &error));
        OK(ps_owned_piece_verify(run, bytes, scratch, &error));
        checked_late_window = 1;
      }
    }
    if (!base) {
      first = bytes;
      OK(ps_owned_read(run, 43, &byte, 1, &error));
      wrong = byte ^ 1;
      OK(ps_owned_write(run, 43, &wrong, 1, &error));
      CHECK(ps_owned_piece_verify(run, bytes, scratch, &error) ==
            LC_ERR_PROTOCOL);
      lc_error_cleanup(&error);
      OK(ps_owned_write(run, 43, &byte, 1, &error));
      OK(ps_owned_piece_verify(run, bytes, scratch, &error));
    }
    base += bytes;
    ps_owned_run_close(run);
    run = NULL;
  }
  CHECK(base == info.capacity && checked_late_window);
  OK(ps_owned_grant(head, NULL, 0, 0, &run, &found, &error));
  physical = piece_physical(status.ref.incarnation) + PS_RUN_IDENTITY_BYTES;
  fd = open(path, O_RDWR);
  CHECK(fd >= 0 &&
        pread(fd, footer, sizeof(footer), (off_t)physical) == sizeof(footer));
  for (int fault = 0; fault < 3; ++fault) {
    memcpy(damaged, footer, sizeof(footer));
    if (!fault)
      damaged[16] ^= 1; /* torn footer CRC */
    else {
      if (fault == 1)
        fixture_put(damaged + 8, first - 1, 8); /* wrong checked prefix */
      else
        damaged[20] ^= 1; /* another incarnation, valid footer checksum */
      fixture_put(damaged + 28, crc32(0, damaged, 28), 4);
    }
    CHECK(pwrite(fd, damaged, sizeof(damaged), (off_t)physical) ==
          sizeof(damaged));
    CHECK(ps_owned_piece_inspect(run, first, NULL, &error) == LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
  }
  CHECK(pwrite(fd, footer, sizeof(footer), (off_t)physical) == sizeof(footer));
  CHECK(close(fd) == 0);
  OK(ps_owned_piece_verify(run, first, scratch, &error));
  ps_owned_run_close(run);
  OK(ps_owned_barrier(head, &error));
  ps_owned_close(head);
  OK(ps_close(space, &error));
  /* Preserve the complete canonical candidate and grants, but lose its final
   * completion footer. A trusted prefix must reject this damage; an uncovered
   * NoSync candidate can rebuild only after canonical CRC/frame validation. */
  memset(m.stable + physical, 0, sizeof(footer));
  crash(&m);
  space = piece_reopen(durable, &io);
  rc = ps_stream_open(space, NULL, stream_key(1), 0, &info.ref,
                      canonical_recover, &trace, &rejected, &error);
  if (checkpointed) {
    CHECK(rc == LC_ERR_PROTOCOL && !rejected);
    lc_error_cleanup(&error);
    fd = open(path, O_RDWR);
    CHECK(fd >= 0 && pwrite(fd, footer, sizeof(footer), (off_t)physical) ==
                         sizeof(footer));
    CHECK(close(fd) == 0);
    stream = stream_open(space, 1, 0, &info.ref, NULL);
  } else {
    CHECK(!durable && rc == LC_OK && rejected && trace.bytes);
    stream = rejected;
  }
  stream_expect(stream, offset, body, length, 1);
  OK(ps_stream_seal(stream, &error));
  OK(ps_stream_close(stream, &error));
  OK(ps_close(space, &error));
  space = piece_reopen(durable, &io);
  stream = stream_open(space, 1, 0, &info.ref, NULL);
  stream_expect(stream, offset, body, length, 1);
  OK(ps_stream_close(stream, &error));
  OK(ps_owned_open(space, NULL, stream_key(1), 0, &info.ref, &head, &status,
                   &error));
  OK(ps_owned_grant(head, NULL, 0, 0, &run, &found, &error));
  CHECK(found);
  OK(ps_owned_piece_inspect(run, first, &checksum, &error));
  CHECK(!checksum.reconstructed);
  OK(ps_owned_piece_verify(run, first, scratch, &error));
  ps_owned_run_close(run);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  free(body);
  free(scratch);
  free(m.stable);
  clean();
}
#include "abandoned_cases.h"
#include "capacity_cases.h"
#include "checkpoint_cases.h"
#include "native_metadata_cases.h"
#include "reclaim_cases.h"
#include "revision_cases.h"
#include "tail_cases.h"
int main(int argc, char **argv) {
  if (argc != 2 && argc != 3)
    return 2;
  lc_error_init(&error);
  CHECK(snprintf(path, sizeof(path), "%s/space-test-%ld", argv[1],
                 (long)getpid()) > 0);
  CHECK(snprintf(other, sizeof(other), "%s/space-other-%ld", argv[1],
                 (long)getpid()) > 0);
  if (argc == 3) {
    if (strcmp(argv[2], "native-metadata") == 0) {
      for (int durable = 0; durable < 2; ++durable) {
        native_metadata_lifecycle(durable);
        native_metadata_retirement(durable);
      }
    } else if (strcmp(argv[2], "native-metadata-shared-reserve") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        native_metadata_shared_reserve(durable);
    } else if (strcmp(argv[2], "native-metadata-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int kind = 0; kind < 3; ++kind)
          native_metadata_faults(durable, kind);
    } else if (strcmp(argv[2], "native-metadata-concurrency") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        native_metadata_concurrency(durable);
    } else if (strcmp(argv[2], "namespace-abandoned-compare") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int large = 0; large < 2; ++large)
          namespace_abandoned_case(durable, 0, 1, 1, 0, large, 1);
    } else if (strcmp(argv[2], "namespace-abandoned") == 0) {
      namespace_abandoned();
      for (int durable = 0; durable < 2; ++durable)
        abandoned_owned(durable);
    } else if (strcmp(argv[2], "namespace-abandoned-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int unselected = 0; unselected < 2; ++unselected)
          namespace_abandoned_faults(durable, unselected);
    } else if (strcmp(argv[2], "namespace-abandoned-empty") == 0) {
      namespace_abandoned_empty();
    } else if (strcmp(argv[2], "namespace-abandoned-concurrency") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_abandoned_concurrency(durable);
    } else if (strcmp(argv[2], "namespace-tail") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_tail(durable);
      for (int durable = 0; durable < 2; ++durable)
        namespace_tail_dense(durable);
    } else if (strcmp(argv[2], "namespace-tail-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_tail_faults(durable);
    } else if (strcmp(argv[2], "namespace-tail-concurrency") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_tail_concurrency(durable);
    } else if (strcmp(argv[2], "namespace-active-retirement") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int shared = 0; shared < 2; ++shared)
          namespace_active_retirement(durable, shared);
    } else if (strcmp(argv[2], "namespace-active-retirement-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int shared = 0; shared < 2; ++shared)
          namespace_active_retirement_faults(durable, shared);
    } else if (strcmp(argv[2], "standing-prefix") == 0) {
      standing_bound();
      for (int durable = 0; durable < 2; ++durable)
        standing_prefix(durable);
    } else if (strcmp(argv[2], "standing-owners") == 0) {
      standing_value_bound();
      standing_owners();
    } else if (strcmp(argv[2], "standing-capacity") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        standing_capacity(durable);
    } else if (strcmp(argv[2], "standing-concurrency") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        standing_concurrency(durable);
    } else if (strcmp(argv[2], "standing-deletion") == 0) {
      for (int durable = 0; durable < 2; ++durable) {
        /* The first case aligns the admitted checkpoint byte bound to a page.
         */
        standing_deletion(durable, 3491);
        standing_deletion(durable, 60 * 1024);
        standing_deletion(durable, (size_t)PS_MIB);
      }
    } else if (strcmp(argv[2], "root-covered-admission") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        covered_root_admission(durable);
    } else if (strcmp(argv[2], "root-obligations") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int partial = 0; partial < 2; ++partial)
          for (int full = 0; full < 2; ++full)
            root_obligations(durable, partial, full);
    } else if (strcmp(argv[2], "root-obligation-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        root_obligation_faults(durable);
    } else if (strcmp(argv[2], "checkpoint-idempotence") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_idempotence(durable);
    } else if (strcmp(argv[2], "checkpoint-records") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_records(durable);
    } else if (strcmp(argv[2], "checkpoint-owner") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_owner(durable);
    } else if (strcmp(argv[2], "checkpoint-ranges") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_ranges(durable);
    } else if (strcmp(argv[2], "namespace-repack") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_repack(durable);
    } else if (strcmp(argv[2], "namespace-repack-concurrency") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_repack_concurrency(durable, 0);
    } else if (strcmp(argv[2], "namespace-repack-shared-refresh") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_repack_concurrency(durable, 1);
    } else if (strcmp(argv[2], "namespace-repack-churn") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        namespace_repack_churn(durable);
    } else if (strcmp(argv[2], "owned-repack-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int bitmap = 0; bitmap < 2; ++bitmap)
          owned_repack_faults(durable, bitmap);
    } else if (strcmp(argv[2], "checkpoint-namespace") == 0) {
      for (int durable = 0; durable < 2; ++durable) {
        checkpoint_namespace(durable);
        checkpoint_retirement(durable);
      }
    } else if (strcmp(argv[2], "checkpoint-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int heap = 0; heap < 2; ++heap)
          checkpoint_faults(durable, heap);
    } else if (strcmp(argv[2], "checkpoint-large-group") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_large_group(durable);
    } else if (strcmp(argv[2], "checkpoint-history") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        checkpoint_history(durable);
    } else if (strcmp(argv[2], "revision") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int control = 0; control < 2; ++control)
          revision_lifecycle(durable, control);
      revision_admission();
      revision_candidate_images();
      revision_rollover();
    } else if (strcmp(argv[2], "revision-concurrency") == 0) {
      revision_concurrency();
    } else if (strcmp(argv[2], "revision-values") == 0) {
      revision_values();
      revision_derived_recovery();
    } else if (strcmp(argv[2], "revision-process") == 0) {
      revision_processes(argv[1]);
    } else if (strcmp(argv[2], "revision-crash-child") == 0) {
      revision_death_child();
    } else if (strcmp(argv[2], "revision-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int control = 0; control < 2; ++control)
          revision_faults(durable, control);
      revision_recovery_failures();
    } else if (strcmp(argv[2], "value-budget") == 0) {
      value_budget_planning();
      value_budget_rejections();
      for (int durable = 0; durable < 2; ++durable)
        value_budget_lifecycle(durable);
    } else if (strcmp(argv[2], "value-budget-process") == 0) {
      value_budget_process();
    } else if (strcmp(argv[2], "value-budget-capacity") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int fragmented = 0; fragmented < 2; ++fragmented)
          value_budget_capacity(durable, fragmented);
    } else if (strcmp(argv[2], "value-budget-faults") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        value_budget_faults(durable);
    } else if (strcmp(argv[2], "value-budget-concurrency") == 0) {
      value_budget_concurrency();
    } else if (strcmp(argv[2], "append-budget") == 0) {
      append_budget_planning();
      append_growth_high_water();
      for (int durable = 0; durable < 2; ++durable)
        for (int fragmented = 0; fragmented < 2; ++fragmented)
          grant_budget_capacity(durable, fragmented, 1);
    } else if (strcmp(argv[2], "grant-budget") == 0) {
      grant_budget_plans();
      routing_budget_packing();
      grant_budget_rejections();
      for (int durable = 0; durable < 2; ++durable)
        for (int fragmented = 0; fragmented < 2; ++fragmented)
          grant_budget_capacity(durable, fragmented, 0);
      grant_budget_concurrency();
    } else if (strcmp(argv[2], "grant-budget-faults") == 0) {
      for (int zero = 0; zero < 2; ++zero)
        for (int durable = 0; durable < 2; ++durable)
          canonical_stream_preparation_faults(zero, 1, durable);
    } else if (strcmp(argv[2], "budget") == 0) {
      budget_lifecycle(0);
      budget_lifecycle(1);
      budget_concurrency();
      budget_allocation_faults();
    } else if (strcmp(argv[2], "budget-capacity") == 0) {
      budget_capacity(0);
      budget_capacity(1);
    } else if (strcmp(argv[2], "commit") == 0)
      commit_coordination();
    else if (strcmp(argv[2], "reopen-ordering") == 0) {
      reopen_cleanup_ordering();
      shared_pool_power_loss();
    } else if (strcmp(argv[2], "reopen-sanitize") == 0)
      reopen_sanitization();
    else if (strcmp(argv[2], "reopen") == 0)
      reopen_retirement();
    else if (strcmp(argv[2], "metadata") == 0) {
      metadata_lifecycle();
      metadata_faults(0);
      metadata_recovery_allocations();
    } else if (strcmp(argv[2], "metadata-capacity") == 0) {
      metadata_capacity();
      metadata_workspace_abort();
    } else if (strcmp(argv[2], "metadata-capacity-faults") == 0) {
      metadata_capacity_faults();
    } else if (strcmp(argv[2], "metadata-repack") == 0) {
      metadata_repack();
      metadata_repack_surfaces();
    } else if (strcmp(argv[2], "metadata-repack-faults") == 0) {
      metadata_faults(1);
    } else if (strcmp(argv[2], "metadata-growth") == 0) {
      metadata_growth();
    } else if (strcmp(argv[2], "metadata-growth-faults") == 0) {
      metadata_growth_faults();
    } else if (strcmp(argv[2], "packing") == 0) {
      packing_lifecycle();
      packing_faults();
      packing_concurrency();
      packing_invalid_ownership();
    } else if (strcmp(argv[2], "catalog") == 0) {
      catalog_basic(0);
      catalog_basic(1);
      catalog_faults();
      catalog_visitors();
      catalog_reopen_faults();
      catalog_removal_prefix();
      catalog_bounds_rollover();
      catalog_invalid_records();
      catalog_concurrency(0);
      catalog_concurrency(1);
      catalog_concurrency(2);
      catalog_snapshot_prefix(0);
      catalog_snapshot_prefix(1);
      catalog_scale();
    } else if (strcmp(argv[2], "lifecycle") == 0) {
      stream_lifecycle(0);
      stream_lifecycle(1);
      stream_lifecycle_faults(0);
      stream_lifecycle_faults(1);
      stream_lifecycle_checkpoint_faults();
      stream_lifecycle_capacity();
      stream_lifecycle_concurrency(0);
      stream_lifecycle_concurrency(1);
      stream_lifecycle_invalid_events();
      stream_lifecycle_rebind_rollover();
    } else if (strcmp(argv[2], "stream-recovery") == 0) {
      for (int zero = 0; zero < 2; ++zero)
        for (int durable = 0; durable < 2; ++durable) {
          for (int action = 0; action < 3; ++action)
            canonical_stream_read_recovery(zero, durable, action);
          canonical_stream_repair_concurrency(zero, durable);
          canonical_stream_repair_groups(zero, durable);
        }
    } else if (strcmp(argv[2], "owned-repack") == 0) {
      owned_repack(0);
      owned_repack(1);
    } else if (strcmp(argv[2], "stream-repack") == 0) {
      stream_repack(0);
      stream_repack(1);
    } else if (strcmp(argv[2], "owned-bitmap") == 0) {
      owned_bitmap(0);
      owned_bitmap(1);
    } else if (strcmp(argv[2], "owned-page-packing") == 0) {
      owned_page_packing(0);
      owned_page_packing(1);
    } else if (strcmp(argv[2], "stream-piece-repair") == 0) {
      for (int durable = 0; durable < 2; ++durable)
        for (int fault = 0; fault < 2; ++fault)
          piece_recovery_partial(durable, fault);
    } else if (strcmp(argv[2], "stream-pieces") == 0) {
      bounded_pieces(0, 0);
      bounded_pieces(0, 1);
      bounded_pieces(1, 1);
      piece_incremental(0);
      piece_incremental(1);
      append_growth_high_water();
    } else if (strcmp(argv[2], "streams") == 0) {
      canonical_stream_exact_reservations();
      canonical_stream_lifecycle();
      canonical_stream_faults();
      canonical_stream_reused_grants();
      canonical_stream_concurrency();
      canonical_stream_open_faults(0);
      canonical_stream_open_faults(1);
      canonical_stream_abort_faults(0);
      canonical_stream_abort_faults(1);
      canonical_stream_private_grant(0);
      canonical_stream_private_grant(1);
      canonical_stream_private_capacity();
      canonical_stream_invalid_metadata();
      canonical_stream_checkpoint_faults();
      canonical_stream_capacity();
      canonical_stream_fragmentation();
      canonical_stream_grown_tail(0);
      canonical_stream_grown_tail(1);
      canonical_stream_prepared_pool(0);
      canonical_stream_prepared_pool(1);
      canonical_stream_prepared_pool(2);
      canonical_stream_preparation_faults(0, 0, 1);
      canonical_stream_preparation_faults(1, 0, 1);
    } else
      return 2;
    lc_error_cleanup(&error);
    printf("pouch-space %s: %zu observable checks passed\n", argv[2], checks);
    return 0;
  }
  basic();
  streaming_capacity();
  fault_boundaries();
  reordered_nosync();
  short_io_and_corruption();
  concurrency();
  churn_and_metadata_limit();
  fragmented_allocation();
  process_death();
  allocation_failures();
  model_churn();
  incarnation_grants();
  lc_error_cleanup(&error);
  printf("pouch-space: %zu observable checks passed\n", checks);
  return 0;
}
