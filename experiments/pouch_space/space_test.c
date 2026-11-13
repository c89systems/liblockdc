#define _GNU_SOURCE
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
  OK(ps_begin(s, k, &w, &error));
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
  OK(ps_begin(s, key(PS_COLLECTION, 1, 1), &w, &error));
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
  OK(ps_begin(s, state(99), &w, &error));
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
  size_t stable_bytes, operations, fail_at, short_limit, reads;
  size_t fail_read;
  size_t zero_calls;
  int unsupported_zero, interrupt_zero;
  uint64_t read_bytes;
  int tear, persist_torn, interrupt_read, interrupt_write;
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
  return pwrite(fd, p, n, offset);
}
static int model_sync(void *v, int fd) {
  model *m = v;
  struct stat st;
  if (fail(m)) {
    errno = EIO;
    return -1;
  }
  CHECK(fdatasync(fd) == 0);
  CHECK(fstat(fd, &st) == 0 && st.st_size <= (off_t)(4 * PS_MIB));
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
static ps_io model_init(model *m) {
  ps_io io = {m, model_read, model_write, model_sync, model_reserve, NULL};
  memset(m, 0, sizeof(*m));
  m->stable = malloc((size_t)(4 * PS_MIB));
  CHECK(m->stable != NULL);
  return io;
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
        OK(ps_begin(s, state(1), &w, &error));
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
  CHECK(pwrite(fd, &byte, 1, 2 * PS_PAGE) ==
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
    v->rc = ps_begin(v->s, key(PS_COLLECTION, v->ordinal, i), &w, &e);
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
  OK(ps_begin(s, state(7), &slow, &error));
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
    OK(ps_begin(s, state(2), &w, &error));
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
        rc = ps_begin(s, state(2), &w, &error);
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
      OK(ps_begin(s, k, &w, &error));
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
  s = reopen(&io); /* range grant must checkpoint a completely full journal */
  expect(s, state(1), &lost, NULL, 0);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.checkpoints == 1 && stats.journal_bytes == 64);
  next = put(s, state(1), NULL, 0, 1);
  CHECK(next.incarnation > lost.incarnation);
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
  int ordinal, rc, maintenance, remove, same_key;
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
    actor->rc = ps_checkpoint(actor->space, &e);
  else if (actor->remove)
    actor->rc = ps_delete(actor->space, state(actor->ordinal + 1), &e);
  else {
    actor->rc = ps_begin(
        actor->space, state(actor->same_key ? 1 : actor->ordinal + 1), &w, &e);
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
  for (mode = 0; mode < 5; ++mode) {
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
    OK(ps_begin(s, state(99), &private_writer, &error));
    OK(ps_write(private_writer, body, PS_CHUNK, &error));
    ps_writer_close(private_writer);
    absent(s, state(99));
    pthread_mutex_lock(&gate.mutex);
    CHECK(gate.finished == 0); /* no durable success before the barrier */
    pthread_mutex_unlock(&gate.mutex);
    if (mode == 2) {
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
    for (i = 0; i < (mode == 2 ? 5U : 4U); ++i) {
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
static void metadata_faults(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_reader *r;
  unsigned char actual[16];
  size_t kind, phase, count, i, n;
  int rc;
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.operations = m.fail_at = 0;
      s = create(3 * PS_MIB, 0, &io);
      for (i = 0; i < 512; ++i)
        put(s, state(i), "A", 1, 1);
      OK(ps_checkpoint(s, &error));
      for (i = 0; i < 16; ++i)
        put(s, state(i * 31), "B", 1, 1);
      m.operations = 0;
      m.fail_at = kind == 0 ? phase : 0;
      m.tear = m.persist_torn = 1;
      allocation_call = 0;
      allocation_fail = kind == 1 ? (phase ? phase : (size_t)-1) : 0;
      rc = ps_checkpoint(s, &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc != LC_OK) {
        CHECK(kind == 0 || rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
        if (kind == 1)
          OK(ps_checkpoint(s,
                           &error)); /* failed private builds are retryable */
      }
      OK(ps_close(s, &error));
      crash(&m);
      s = reopen(&io);
      for (i = 0; i < 512; ++i) {
        OK(ps_read_open(s, state(i), NULL, &r, &error));
        OK(ps_read(r, actual, sizeof(actual), &n, &error));
        CHECK(n == 1 && (actual[0] == 'A' ||
                         (i % 31 == 0 && i / 31 < 16 && actual[0] == 'B')));
        ps_reader_close(r);
      }
      put(s, state(9999), "after", 5, 1);
      OK(ps_checkpoint(s, &error));
      OK(ps_close(s, &error));
    }
    CHECK(count > 15);
  }
  free(m.stable);
  clean();
  puts("metadata: every multi-page checkpoint I/O and heap failure, "
       "abort/retry and reuse");
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
        rc = ps_begin(s, state(2), &w, &error);
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
  OK(ps_begin(s, collection, &w, &error));
  OK(ps_write(w, body, PS_CHUNK + 65, &error));
  BAD(ps_publish(w, NULL, &error)); /* existing live collection binding */
  allocation_call = 0;
  allocation_fail = 1;
  ps_writer_close(w);
  allocation_fail = 0;
  CHECK(allocation_call >= 1);
  BAD(ps_begin(s, state(99), &w, &error));
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
  OK(ps_stream_open(space, stream_key(id), create, ref, canonical_recover,
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
    rc = ps_stream_commit(w, e);
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
  BAD(ps_stream_open(space, stream_key(1), 0, NULL, canonical_recover, NULL,
                     &other_stream, &error));
  BAD(ps_begin(space, stream_key(1), &wrong_kind, &error));
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
  CHECK(info.runs <= 6 && info.capacity < 2 * PS_MIB);
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
  BAD(ps_stream_open(space, stream_key(1), 0, &invalid_ref, canonical_recover,
                     NULL, &other_stream, &error));
  CHECK(other_stream == NULL);
  invalid_ref = ref;
  invalid_ref.container[0] ^= 1;
  BAD(ps_stream_open(space, stream_key(1), 0, &invalid_ref, canonical_recover,
                     NULL, &other_stream, &error));
  CHECK(other_stream == NULL);
  BAD(ps_stream_open(space, stream_key(1), 1, &ref, canonical_recover, NULL,
                     &other_stream, &error));
  BAD(ps_stream_open(space, stream_key(999), 0, NULL, canonical_recover, NULL,
                     &other_stream, &error));
  BAD(ps_stream_open(space, stream_key(1), 0, &ref, NULL, NULL, &other_stream,
                     &error));
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
  unsigned char *old = calloc(1, PS_PAGE), grant[88];
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
static void canonical_stream_open_faults(int native_zero) {
  model m;
  ps_io io = model_init(&m);
  if (native_zero)
    io.zero = model_zero;
  ps_options options = {0};
  ps_space *space = NULL;
  ps_stream *stream = NULL;
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
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
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
        rc = ps_stream_open(space, stream_key(1), 0, NULL, canonical_recover,
                            NULL, &stream, &error);
      allocation_fail = 0;
      m.fail_at = m.fail_read = 0;
      if (!phase)
        count = kind == 0   ? allocation_call
                : kind == 1 ? m.reads
                            : m.operations;
      if (rc != LC_OK) {
        CHECK(rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
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
    CHECK(count > 5);
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
        fixture_put(record + 64, PS_PAGE,
                    8); /* sum of run capacities differs */
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
      CHECK(ps_stream_open(space, stream_key(1), 0, NULL, canonical_recover,
                           NULL, &stream, &error) == LC_ERR_PROTOCOL);
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
  OK(ps_owned_open(space, stream_key(1), 1, NULL, &head, &owned, &error));
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
  OK(ps_owned_open(space, stream_key(1), 0, NULL, &head, &owned, &error));
  OK(ps_grow(space, PS_MIB, &error));
  OK(ps_status(space, &before, &error));
  OK(ps_owned_grant(head, 0, PS_MIB, &run, &found, &error));
  CHECK(found && ps_owned_run_capacity(run) == PS_MIB - PS_RUN_HEADER);
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
  CHECK(after.syncs == before.syncs + 1 && info.runs == 4);
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
static void canonical_stream_preparation_faults(int native_zero) {
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
  options.durable_sync = 1;
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      clean();
      m.fail_at = m.operations = 0;
      space = create(3 * PS_MIB, 1, &io);
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
      rc = canonical_append(stream, body, PS_CHUNK + 333, 31, 2, &next, &error);
      allocation_fail = 0;
      m.fail_at = 0;
      if (!phase)
        count = kind == 0 ? m.operations : allocation_call;
      if (rc != LC_OK) {
        CHECK(kind == 0 || rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
      }
      OK(ps_stream_close(stream, &error));
      OK(ps_close(space, &error));
      crash(&m);
      space = reopen(&io);
      stream = stream_open(space, 1, 0, NULL, NULL);
      stream_expect(stream, first, body, 64, 1);
      OK(ps_stream_status(stream, &info, &error));
      CHECK(info.tail == 107 || info.tail == 107 + PS_CHUNK + 333 + 43);
      if (rc == LC_OK)
        CHECK(info.tail != 107);
      if (info.tail != 107)
        stream_expect(stream, next, body, PS_CHUNK + 333, 2);
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
  OK(ps_stream_open(space, stream_key(1), 0, &prior, NULL, NULL, &old, &error));
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
  BAD(ps_stream_open(space, stream_key(1), 0, &prior, canonical_recover, NULL,
                     &missing, &error));
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
  BAD(ps_stream_open(space, stream_key(1), 0, &prior, canonical_recover, NULL,
                     &missing, &error));
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
  OK(ps_stream_open(space, stream_key(2), 0, NULL, NULL, NULL, &fresh, &error));
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
        opened = ps_stream_open(space, stream_key(1), 0, NULL,
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
  OK(ps_owned_open(space, stream_key(1), 1, NULL, &head, &status, &error));
  for (;;) {
    rc = ps_owned_grant(head, base, PS_PAGE, &run, &found, &error);
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
  BAD(ps_owned_commit(head, 0, &error));
  OK(ps_status(space, &before, &error));
  allocation_max = allocation_call = 0;
  measure_allocations = 1;
  allocation_fail = (size_t)-1;
  OK(ps_owned_retire(head, &error));
  allocation_fail = 0;
  measure_allocations = 0;
  CHECK(allocation_call == 1 && allocation_max == 64);
  BAD(ps_owned_recovered(head, 0, &error));
  BAD(ps_owned_commit(head, 0, &error));
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
  OK(ps_owned_open(space, stream_key(1), 1, NULL, &head, &status, &error));
  OK(ps_owned_grant(head, 0, PS_PAGE, &run, &found, &error));
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
  BAD(ps_stream_open(space, stream_key(1), 1, NULL, canonical_recover, NULL,
                     &rejected, &error));
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

int main(int argc, char **argv) {
  if (argc != 2 && argc != 3)
    return 2;
  lc_error_init(&error);
  CHECK(snprintf(path, sizeof(path), "%s/space-test-%ld", argv[1],
                 (long)getpid()) > 0);
  CHECK(snprintf(other, sizeof(other), "%s/space-other-%ld", argv[1],
                 (long)getpid()) > 0);
  if (argc == 3) {
    if (strcmp(argv[2], "commit") == 0)
      commit_coordination();
    else if (strcmp(argv[2], "reopen") == 0)
      reopen_retirement();
    else if (strcmp(argv[2], "metadata") == 0) {
      metadata_lifecycle();
      metadata_faults();
      metadata_recovery_allocations();
    } else if (strcmp(argv[2], "packing") == 0) {
      packing_lifecycle();
      packing_faults();
      packing_concurrency();
      packing_invalid_ownership();
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
    } else if (strcmp(argv[2], "streams") == 0) {
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
      canonical_stream_invalid_metadata();
      canonical_stream_checkpoint_faults();
      canonical_stream_capacity();
      canonical_stream_fragmentation();
      canonical_stream_grown_tail(0);
      canonical_stream_grown_tail(1);
      canonical_stream_prepared_pool(0);
      canonical_stream_prepared_pool(1);
      canonical_stream_prepared_pool(2);
      canonical_stream_preparation_faults(0);
      canonical_stream_preparation_faults(1);
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
