#define _GNU_SOURCE
#include "space.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

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
  CHECK(stats.retired_bytes == PS_PAGE); /* held reader still pins first */
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
  uint64_t read_bytes;
  int tear, persist_torn, interrupt_read, interrupt_write;
} model;
static int fail(model *m) { return ++m->operations == m->fail_at; }
static ssize_t model_read(void *v, int fd, void *p, size_t n, off_t offset) {
  model *m = v;
  ++m->reads;
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
static ps_io model_init(model *m) {
  ps_io io = {m, model_read, model_write, model_sync, model_reserve};
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
  CHECK(stats.retired_bytes == PS_PAGE);
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
  int maintenance, fd;
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
  ssize_t written = pwrite(fd, p, n, offset);
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
    CHECK(stats.retired_bytes >= 2 * PS_PAGE);
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
