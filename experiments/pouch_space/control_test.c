#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "control.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "control error %d: %s\n", rc, e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static ps_key key(uint64_t owner) {
  ps_key k = {owner, 1, PS_CONTROL};
  return k;
}
static void put64(unsigned char *p, uint64_t v) {
  for (unsigned i = 0; i < 8; ++i) {
    p[i] = (unsigned char)v;
    v >>= 8;
  }
}
static void seal(unsigned char *p) {
  uint32_t c = (uint32_t)crc32(0L, p, LC_COLLECTION_HEAD_BYTES - 4);
  for (unsigned i = 0; i < 4; ++i) {
    p[LC_COLLECTION_HEAD_BYTES - 4 + i] = (unsigned char)c;
    c >>= 8;
  }
}
static lc_collection_disk_head sample(void) {
  lc_collection_disk_head h;
  memset(&h, 0, sizeof(h));
  h.active = 17;
  h.snapshot = 7;
  h.snapshot_generation = 123;
  h.snapshot_extra_parts = 2;
  h.tail_segment = 15;
  h.tail_offset = 987654321;
  h.next_snapshot = 9;
  h.published_at = 4321;
  h.retention = 7654;
  h.retention_generation = 8901;
  h.max_entries = 4000;
  h.lease.fencing_token = 0xfedcba9876543210ULL;
  h.lease.expires_at_unix = 99887766;
  h.lease.private_read = 1;
  memcpy(h.lease.lease_id, "0123456789abcdefghig", LC_XID_STRING_SIZE);
  memset(h.lease.owner, 'x', sizeof(h.lease.owner) - 1);
  return h;
}
static void equal_head(const lc_collection_disk_head *a,
                       const lc_collection_disk_head *b, lc_error *e) {
  unsigned char x[LC_COLLECTION_HEAD_BYTES], y[LC_COLLECTION_HEAD_BYTES];
  ok(lc_collection_head_encode(a, x, e), e);
  ok(lc_collection_head_encode(b, y, e), e);
  REQUIRE(memcmp(x, y, sizeof(x)) == 0);
}
static void codec(void) {
  lc_collection_disk_head h = sample(), got, prior;
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES],
      golden[LC_COLLECTION_HEAD_BYTES], bad[LC_COLLECTION_HEAD_BYTES];
  lc_error e;
  lc_error_init(&e);
  memset(golden, 0, sizeof(golden));
  put64(golden, 0x4c434348);
  put64(golden + 8, 17);
  put64(golden + 16, 7);
  put64(golden + 24, 123);
  put64(golden + 32, 15);
  put64(golden + 40, 987654321);
  put64(golden + 48, 9);
  put64(golden + 64, 4321);
  put64(golden + 72, 7654);
  put64(golden + 80, 4000);
  put64(golden + 88, 8901);
  put64(golden + 96, 0xfedcba9876543210ULL);
  put64(golden + 104, 99887766);
  put64(golden + 112, 1);
  memcpy(golden + 120, "0123456789abcdefghig", LC_XID_STRING_SIZE);
  memset(golden + 144, 'x', 128);
  put64(golden + 280, 2);
  seal(golden);
  ok(lc_collection_head_encode(&h, bytes, &e), &e);
  REQUIRE(memcmp(bytes, golden, sizeof(bytes)) == 0);
  ok(lc_collection_head_decode(golden, &got, &e), &e);
  equal_head(&h, &got, &e);
  memset(&prior, 0x5a, sizeof(prior));
  for (size_t i = 0; i < sizeof(bytes); ++i)
    for (unsigned bit = 0; bit < 8; ++bit) {
      memcpy(bad, bytes, sizeof(bad));
      bad[i] ^= (unsigned char)(1U << bit);
      got = prior;
      REQUIRE(lc_collection_head_decode(bad, &got, &e) == LC_ERR_PROTOCOL);
      REQUIRE(memcmp(&got, &prior, sizeof(got)) == 0);
      clear(&e);
    }
  /* Correct CRCs must not disguise invalid semantic fields. */
  const struct {
    unsigned offset;
    uint64_t value;
  } invalid[] = {{0, 0},
                 {8, 0},
                 {8, LC_COLLECTION_SNAPSHOT_BIT},
                 {16, LC_COLLECTION_SNAPSHOT_BIT},
                 {32, 0},
                 {32, 18},
                 {48, 6},
                 {48, LC_COLLECTION_SNAPSHOT_BIT},
                 {72, LC_COLLECTION_SNAPSHOT_BIT},
                 {104, LC_COLLECTION_SNAPSHOT_BIT},
                 {112, 2},
                 {280, 3},
                 {16, 0},
                 {104, 0},
                 {96, 0}};
  for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
    memcpy(bad, bytes, sizeof(bad));
    put64(bad + invalid[i].offset, invalid[i].value);
    seal(bad);
    got = prior;
    REQUIRE(lc_collection_head_decode(bad, &got, &e) == LC_ERR_PROTOCOL);
    REQUIRE(memcmp(&got, &prior, sizeof(got)) == 0);
    clear(&e);
  }
  for (size_t i = 273; i < sizeof(bytes) - 4; ++i) {
    if (i >= 280 && i < 288)
      continue;
    memcpy(bad, bytes, sizeof(bad));
    bad[i] = 1;
    seal(bad);
    REQUIRE(lc_collection_head_decode(bad, &got, &e) == LC_ERR_PROTOCOL);
    clear(&e);
  }
  for (unsigned field = 0; field < 3; ++field) {
    memcpy(bad, bytes, sizeof(bad));
    if (field == 0)
      memset(bad + 120, 'a', LC_XID_STRING_SIZE);
    if (field == 1)
      memset(bad + 144, 'a', 129);
    if (field == 2)
      bad[120] = 'z';
    seal(bad);
    REQUIRE(lc_collection_head_decode(bad, &got, &e) == LC_ERR_PROTOCOL);
    clear(&e);
  }
  REQUIRE(lc_collection_head_decode(NULL, &got, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(lc_collection_head_decode(bytes, NULL, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(lc_collection_head_encode(NULL, bytes, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(lc_collection_head_encode(&h, NULL, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(lc_collection_head_prefix_valid(bytes, 1, &e) == LC_ERR_PROTOCOL);
  clear(&e);
  h.retention = -1;
  REQUIRE(lc_collection_head_encode(&h, bytes, &e) == LC_ERR_PROTOCOL);
  clear(&e);
  h = sample();
  h.snapshot = 0;
  h.snapshot_extra_parts = 0;
  h.next_snapshot = 0;
  memset(&h.lease, 0, sizeof(h.lease));
  h.retention = 0;
  h.dropped = 1;
  ok(lc_collection_head_encode(&h, bytes, &e), &e);
  ok(lc_collection_head_decode(bytes, &got, &e), &e);
  equal_head(&h, &got, &e);
  lc_error_cleanup(&e);
}
static void raw_store(ps_space *s, ps_key k, const void *p, size_t n,
                      lc_error *e) {
  ps_writer *w = NULL;
  ok(ps_begin(s, NULL, k, &w, e), e);
  ok(ps_write(w, p, n, e), e);
  ok(ps_publish(w, NULL, e), e);
  ps_writer_close(w);
}
static void lifecycle(const char *path, int durable) {
  ps_options options = {0};
  ps_space *s = NULL;
  ps_reader *pin = NULL;
  ps_ref first, second, foreign;
  ps_stats stats;
  lc_collection_disk_head h = sample(), got, before;
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  size_t n;
  lc_error e;
  lc_error_init(&e);
  options.durable_sync = durable;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(ps_collection_head_store(s, 11, &h, &first, &e), &e);
  ok(ps_collection_head_load(s, 11, &first, &got, &e), &e);
  equal_head(&h, &got, &e);
  ok(ps_read_open(s, key(11), &first, &pin, &e), &e);
  /* Control identity must not alias another typed record. */
  ps_key state = {11, 1, PS_STATE};
  raw_store(s, state, "state", 5, &e);
  before = h;
  h.lease.fencing_token++;
  h.retention_generation++;
  h.tail_offset++;
  ok(ps_collection_head_store(s, 11, &h, &second, &e), &e);
  REQUIRE(second.incarnation != first.incarnation);
  REQUIRE(ps_collection_head_load(s, 11, &first, &got, &e) == LC_ERR_INVALID);
  clear(&e);
  foreign = second;
  foreign.container[0] ^= 1;
  REQUIRE(ps_collection_head_load(s, 11, &foreign, &got, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_read(pin, bytes, sizeof(bytes), &n, &e), &e);
  REQUIRE(n == sizeof(bytes));
  ok(lc_collection_head_decode(bytes, &got, &e), &e);
  equal_head(&before, &got, &e);
  ps_reader_close(pin);
  /* Replay replacement after checkpoint without another checkpoint. */
  h.dropped = 1;
  ok(ps_collection_head_store(s, 11, &h, &second, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ok(ps_collection_head_load(s, 11, &second, &got, &e), &e);
  equal_head(&h, &got, &e);
  h.dropped = 0;
  for (uint64_t owner = 20; owner < 520; ++owner) {
    h.tail_offset = owner;
    ok(ps_collection_head_store(s, owner, &h, NULL, &e), &e);
  }
  ok(ps_status(s, &stats, &e), &e);
  REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  for (uint64_t owner = 20; owner < 520; ++owner) {
    ok(ps_collection_head_load(s, owner, NULL, &got, &e), &e);
    REQUIRE(got.tail_offset == owner);
  }
  before = got;
  raw_store(s, key(12), "short", 5, &e);
  REQUIRE(ps_collection_head_load(s, 12, NULL, &got, &e) == LC_ERR_PROTOCOL);
  REQUIRE(memcmp(&got, &before, sizeof(got)) == 0);
  clear(&e);
  ok(lc_collection_head_encode(&h, bytes, &e), &e);
  bytes[15] ^= 1;
  raw_store(s, key(13), bytes, sizeof(bytes), &e);
  REQUIRE(ps_collection_head_load(s, 13, NULL, &got, &e) == LC_ERR_PROTOCOL);
  clear(&e);
  REQUIRE(ps_collection_head_store(s, 0, &h, NULL, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(ps_collection_head_load(s, 0, NULL, &got, &e) == LC_ERR_INVALID);
  clear(&e);
  h.active = 0;
  REQUIRE(ps_collection_head_store(s, 11, &h, NULL, &e) == LC_ERR_PROTOCOL);
  clear(&e);
  ok(ps_delete(s, key(11), &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  REQUIRE(ps_collection_head_load(s, 11, NULL, &got, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}

/* Volatile file writes and durable image are deliberately distinct. Crash
 * restoration loses every write since the last successful barrier. */
typedef struct model {
  unsigned char *stable;
  size_t calls, fail_at;
  size_t reads, fail_read;
  int tear, persist_torn;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int hold, entered, released, publications;
} model;
static int fail(model *m) {
  return __atomic_add_fetch(&m->calls, 1, __ATOMIC_RELAXED) == m->fail_at;
}
static ssize_t rd(void *v, int fd, void *p, size_t n, off_t at) {
  model *m = v;
  if (__atomic_add_fetch(&m->reads, 1, __ATOMIC_RELAXED) == m->fail_read) {
    errno = EIO;
    return -1;
  }
  return pread(fd, p, n, at);
}
static ssize_t wr(void *v, int fd, const void *p, size_t n, off_t at) {
  model *m = v;
  if (fail(m)) {
    if (m->tear && n > 1) {
      if (pwrite(fd, p, n / 2, at) != (ssize_t)(n / 2))
        abort();
      if (m->persist_torn)
        memcpy(m->stable + at, p, n / 2);
    }
    errno = EIO;
    return -1;
  }
  ssize_t written = pwrite(fd, p, n, at);
  if (written == (ssize_t)n && at >= PS_JOURNAL_OFFSET && at < PS_DATA) {
    pthread_mutex_lock(&m->mutex);
    ++m->publications;
    pthread_cond_broadcast(&m->cond);
    pthread_mutex_unlock(&m->mutex);
  }
  return written;
}
static int sync_file(void *v, int fd) {
  model *m = v;
  int failed = fail(m);
  pthread_mutex_lock(&m->mutex);
  if (m->hold) {
    m->entered = 1;
    pthread_cond_broadcast(&m->cond);
    while (!m->released)
      pthread_cond_wait(&m->cond, &m->mutex);
  }
  pthread_mutex_unlock(&m->mutex);
  if (failed) {
    errno = EIO;
    return -1;
  }
  if (fdatasync(fd) != 0)
    return -1;
  if (m->stable && pread(fd, m->stable, PS_MIB, 0) != (ssize_t)PS_MIB)
    abort();
  return 0;
}
static int reserve(void *v, int fd, off_t at, off_t n) {
  (void)v;
  return fallocate(fd, 0, at, n);
}
static void crash(model *m, const char *path) {
  int fd = open(path, O_RDWR);
  REQUIRE(fd >= 0);
  REQUIRE(pwrite(fd, m->stable, PS_MIB, 0) == (ssize_t)PS_MIB);
  REQUIRE(fdatasync(fd) == 0);
  REQUIRE(close(fd) == 0);
  m->calls = m->fail_at = 0;
}
static int mutate(ps_space *s, int phase, lc_collection_disk_head *h,
                  lc_error *e) {
  if (phase == 1)
    return ps_delete(s, key(11), e);
  int rc = ps_collection_head_store(s, 11, h, NULL, e);
  if (rc == LC_OK && phase == 2)
    rc = ps_checkpoint(s, e);
  return rc;
}
static void faults(const char *path) {
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_options options = {PS_MIB, PS_MIB, 0, &io};
  lc_collection_disk_head h = sample(), got, old;
  lc_error e;
  ps_space *s;
  size_t count;
  m.stable = malloc(PS_MIB);
  REQUIRE(m.stable != NULL);
  REQUIRE(pthread_mutex_init(&m.mutex, NULL) == 0);
  REQUIRE(pthread_cond_init(&m.cond, NULL) == 0);
  lc_error_init(&e);
  for (int durable = 0; durable < 2; ++durable)
    for (int phase = 0; phase < 4; ++phase) {
      options.durable_sync = durable;
      count = 0;
      for (unsigned mode = 0; mode < 3; ++mode)
        for (size_t at = 0; at <= count; ++at) {
          if (mode && !at)
            continue;
          m.calls = m.fail_at = 0;
          m.tear = 0;
          m.persist_torn = 0;
          ok(ps_open(path, 1, &options, &s, &e), &e);
          old = sample();
          if (phase != 3)
            ok(ps_collection_head_store(s, 11, &old, NULL, &e), &e);
          ok(ps_checkpoint(s, &e), &e);
          h = old;
          h.lease.fencing_token++;
          h.tail_offset++;
          m.calls = 0;
          m.fail_at = at;
          m.tear = mode >= 1;
          m.persist_torn = mode == 2;
          int rc, stored = 0;
          if (phase == 2) {
            rc = ps_collection_head_store(s, 11, &h, NULL, &e);
            stored = rc == LC_OK;
            if (stored)
              rc = ps_checkpoint(s, &e);
          } else
            rc = mutate(s, phase, &h, &e);
          if (!at) {
            ok(rc, &e);
            count = m.calls;
            REQUIRE(count > 0);
          } else
            REQUIRE(rc != LC_OK);
          m.fail_at = 0;
          clear(&e);
          ok(ps_close(s, &e), &e);
          crash(&m, path);
          ok(ps_open(path, 0, &options, &s, &e), &e);
          rc = ps_collection_head_load(s, 11, NULL, &got, &e);
          if ((!at && phase == 1) || (at && phase == 3)) {
            REQUIRE(rc == LC_ERR_INVALID);
            clear(&e);
          } else if (!at || stored) {
            ok(rc, &e);
            equal_head(&h, &got, &e);
          } else {
            ok(rc, &e);
            equal_head(&old, &got, &e);
          }
          ok(ps_close(s, &e), &e);
          REQUIRE(unlink(path) == 0);
        }
    }
  lc_error_cleanup(&e);
  free(m.stable);
  REQUIRE(pthread_cond_destroy(&m.cond) == 0);
  REQUIRE(pthread_mutex_destroy(&m.mutex) == 0);
}
typedef struct actor {
  ps_space *space;
  uint64_t owner;
  int remove, rc;
  lc_collection_disk_head head;
} actor;
static void *worker(void *v) {
  actor *a = v;
  lc_error e;
  lc_error_init(&e);
  a->rc = a->remove ? ps_delete(a->space, key(a->owner), &e)
                    : ps_collection_head_store(a->space, a->owner, &a->head,
                                               NULL, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void held_barrier(const char *path, int remove, int durable) {
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_options options = {PS_MIB, PS_MIB, 0, &io};
  actor a = {0};
  pthread_t thread;
  ps_space *s;
  lc_collection_disk_head old = sample(), got;
  lc_error e;
  struct timespec deadline;
  lc_error_init(&e);
  options.durable_sync = durable;
  REQUIRE(pthread_mutex_init(&m.mutex, NULL) == 0);
  REQUIRE(pthread_cond_init(&m.cond, NULL) == 0);
  ok(ps_open(path, 1, &options, &s, &e), &e);
  if (remove != 2)
    ok(ps_collection_head_store(s, 11, &old, NULL, &e), &e);
  a.space = s;
  a.owner = 11;
  a.remove = remove == 1;
  a.head = old;
  a.head.tail_offset++;
  m.hold = 1;
  REQUIRE(pthread_create(&thread, NULL, worker, &a) == 0);
  REQUIRE(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 10;
  pthread_mutex_lock(&m.mutex);
  while (!m.entered)
    REQUIRE(pthread_cond_timedwait(&m.cond, &m.mutex, &deadline) == 0);
  pthread_mutex_unlock(&m.mutex);
  int observed = ps_collection_head_load(s, 11, NULL, &got, &e);
  if (remove == 2) {
    REQUIRE(observed == LC_ERR_INVALID);
    clear(&e);
  } else {
    ok(observed, &e);
    equal_head(&old, &got, &e);
  }
  /* A different producer can reserve and fill private bytes while control
   * authority waits on disk. No root mutex is held across the barrier. */
  ps_writer *private = NULL;
  ps_key other = {99, 1, PS_STATE};
  ok(ps_begin(s, NULL, other, &private, &e), &e);
  ok(ps_write(private, "private", 7, &e), &e);
  if (!durable)
    ok(ps_publish(private, NULL, &e), &e);
  ps_writer_close(private);
  observed = ps_collection_head_load(s, 11, NULL, &got, &e);
  if (remove == 2) {
    REQUIRE(observed == LC_ERR_INVALID);
    clear(&e);
  } else {
    ok(observed, &e);
    equal_head(&old, &got, &e);
  }
  pthread_mutex_lock(&m.mutex);
  m.released = 1;
  pthread_cond_broadcast(&m.cond);
  pthread_mutex_unlock(&m.mutex);
  REQUIRE(pthread_join(thread, NULL) == 0);
  ok(a.rc, &e);
  int rc = ps_collection_head_load(s, 11, NULL, &got, &e);
  if (remove == 1) {
    REQUIRE(rc == LC_ERR_INVALID);
    clear(&e);
  } else {
    ok(rc, &e);
    equal_head(&a.head, &got, &e);
  }
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
  REQUIRE(pthread_cond_destroy(&m.cond) == 0);
  REQUIRE(pthread_mutex_destroy(&m.mutex) == 0);
}
static void groups(const char *path, int durable) {
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_options options = {PS_MIB, PS_MIB, 0, &io};
  actor actors[4] = {0};
  pthread_t threads[4];
  ps_space *s;
  ps_stats before, after;
  lc_collection_disk_head old = sample(), got;
  lc_error e;
  struct timespec deadline;
  lc_error_init(&e);
  options.durable_sync = durable;
  REQUIRE(pthread_mutex_init(&m.mutex, NULL) == 0);
  REQUIRE(pthread_cond_init(&m.cond, NULL) == 0);
  ok(ps_open(path, 1, &options, &s, &e), &e);
  for (unsigned i = 0; i < 4; ++i) {
    actors[i].space = s;
    actors[i].owner = 11 + i;
    actors[i].head = old;
    actors[i].head.lease.fencing_token++;
    ok(ps_collection_head_store(s, actors[i].owner, &old, NULL, &e), &e);
  }
  ok(ps_status(s, &before, &e), &e);
  m.hold = 1;
  m.publications = 0;
  REQUIRE(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 10;
  REQUIRE(pthread_create(&threads[0], NULL, worker, &actors[0]) == 0);
  pthread_mutex_lock(&m.mutex);
  while (!m.entered)
    REQUIRE(pthread_cond_timedwait(&m.cond, &m.mutex, &deadline) == 0);
  pthread_mutex_unlock(&m.mutex);
  for (unsigned i = 1; i < 4; ++i)
    REQUIRE(pthread_create(&threads[i], NULL, worker, &actors[i]) == 0);
  pthread_mutex_lock(&m.mutex);
  while (m.publications != 4)
    REQUIRE(pthread_cond_timedwait(&m.cond, &m.mutex, &deadline) == 0);
  pthread_mutex_unlock(&m.mutex);
  for (unsigned i = 0; i < 4; ++i) {
    ok(ps_collection_head_load(s, actors[i].owner, NULL, &got, &e), &e);
    equal_head(&old, &got, &e);
  }
  pthread_mutex_lock(&m.mutex);
  m.released = 1;
  pthread_cond_broadcast(&m.cond);
  pthread_mutex_unlock(&m.mutex);
  for (unsigned i = 0; i < 4; ++i) {
    REQUIRE(pthread_join(threads[i], NULL) == 0);
    ok(actors[i].rc, &e);
    ok(ps_collection_head_load(s, actors[i].owner, NULL, &got, &e), &e);
    equal_head(&actors[i].head, &got, &e);
  }
  ok(ps_status(s, &after, &e), &e);
  REQUIRE(after.syncs == before.syncs + 2);
  REQUIRE(after.sync_requests == before.sync_requests + 4);
  REQUIRE(after.max_sync_batch >= 3);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
  REQUIRE(pthread_cond_destroy(&m.cond) == 0);
  REQUIRE(pthread_mutex_destroy(&m.mutex) == 0);
}
static int bounded_store(ps_space *s, ps_key k,
                         const lc_collection_disk_head *h, lc_error *e) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  ps_writer *w = NULL;
  int rc = lc_collection_head_encode(h, bytes, e);
  if (rc == LC_OK)
    rc = ps_begin(s, NULL, k, &w, e);
  if (rc == LC_OK)
    rc = ps_write(w, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(w, NULL, e);
  ps_writer_close(w);
  return rc;
}
static void bounded_load(ps_space *s, ps_key k, lc_collection_disk_head *h,
                         lc_error *e) {
  ps_reader *r = NULL;
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  size_t got;
  ok(ps_read_open(s, k, NULL, &r, e), e);
  REQUIRE(ps_read_size(r) == sizeof(bytes));
  ok(ps_read(r, bytes, sizeof(bytes), &got, e), e);
  REQUIRE(got == sizeof(bytes));
  ps_reader_close(r);
  ok(lc_collection_head_decode(bytes, h, e), e);
}
static void capacity(const char *path, unsigned kind) {
  ps_options options = {PS_MIB, PS_MIB, 0, NULL};
  ps_space *s;
  ps_writer *w = NULL;
  lc_error e;
  lc_collection_disk_head h = sample(), got;
  unsigned char page[PS_PAGE] = {0};
  ps_key existing = {11, 1, kind}, fresh = {12, 1, kind};
  lc_error_init(&e);
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(bounded_store(s, existing, &h, &e), &e);
  /* Fill the head's packed page, forcing its replacement to need a new page. */
  ps_key filler = {99, 1, PS_STATE};
  raw_store(s, filler, page, PS_PAGE - LC_COLLECTION_HEAD_BYTES, &e);
  uint64_t count;
  for (count = 2; count < 300; ++count) {
    filler.key = count;
    ok(ps_begin(s, NULL, filler, &w, &e), &e);
    ok(ps_write(w, page, sizeof(page), &e), &e);
    int rc = ps_publish(w, NULL, &e);
    ps_writer_close(w);
    if (rc != LC_OK) {
      clear(&e);
      break;
    }
  }
  REQUIRE(count > 100 && count < 300);
  /* Foreground exhaustion cannot consume the bounded replacement allowance. */
  h.lease.fencing_token++;
  ok(bounded_store(s, existing, &h, &e), &e);
  REQUIRE(bounded_store(s, fresh, &h, &e) != LC_OK);
  clear(&e);
  for (unsigned i = 0; i < 24; ++i) {
    ok(ps_checkpoint(s, &e), &e);
    h.lease.fencing_token++;
    ok(bounded_store(s, existing, &h, &e), &e);
  }
  bounded_load(s, existing, &got, &e);
  equal_head(&h, &got, &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  bounded_load(s, existing, &got, &e);
  equal_head(&h, &got, &e);
  ok(ps_delete(s, existing, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  filler.key = count - 1;
  ok(ps_delete(s, filler, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  /* A maximum control is one page, including incremental writes. A rejected
   * oversized write contributes no bytes to the eventual publication. */
  ok(ps_begin(s, NULL, fresh, &w, &e), &e);
  ok(ps_write(w, page, sizeof(page) - 1, &e), &e);
  REQUIRE(ps_write(w, page, 2, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_write(w, page, 1, &e), &e);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  ps_reader *reader = NULL;
  ok(ps_read_open(s, fresh, NULL, &reader, &e), &e);
  REQUIRE(ps_read_size(reader) == sizeof(page));
  ps_reader_close(reader);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void read_fault(const char *path) {
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_options options = {PS_MIB, PS_MIB, 0, &io};
  ps_space *s;
  lc_collection_disk_head h = sample(), got = h;
  lc_error e;
  lc_error_init(&e);
  REQUIRE(pthread_mutex_init(&m.mutex, NULL) == 0);
  REQUIRE(pthread_cond_init(&m.cond, NULL) == 0);
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(ps_collection_head_store(s, 11, &h, NULL, &e), &e);
  m.fail_read = m.reads + 1;
  REQUIRE(ps_collection_head_load(s, 11, NULL, &got, &e) == LC_ERR_TRANSPORT);
  REQUIRE(memcmp(&got, &h, sizeof(got)) == 0);
  clear(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
  REQUIRE(pthread_cond_destroy(&m.cond) == 0);
  REQUIRE(pthread_mutex_destroy(&m.mutex) == 0);
}
static void *run(void *v) {
  const char *path = v;
  codec();
  lifecycle(path, 0);
  lifecycle(path, 1);
  faults(path);
  capacity(path, PS_CONTROL);
  capacity(path, PS_ROUTING);
  read_fault(path);
  groups(path, 0);
  groups(path, 1);
  for (int durable = 0; durable < 2; ++durable)
    for (int remove = 0; remove < 3; ++remove)
      held_barrier(path, remove, durable);
  return NULL;
}
int main(int argc, char **argv) {
  char root[1024], prefix[1024], template_path[1100], path[1100];
  pthread_t thread;
  REQUIRE(argc == 2);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-control-", argv[1]) > 0);
  REQUIRE(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) >
          0);
  REQUIRE(lc_test_tmp_mkdtemp(template_path, root, sizeof(root), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/container", root) > 0);
  REQUIRE(pthread_create(&thread, NULL, run, path) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  DIR *directory = opendir(root);
  struct dirent *entry;
  REQUIRE(directory != NULL);
  while ((entry = readdir(directory)) != NULL)
    REQUIRE(strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0 ||
            strcmp(entry->d_name, ".liblockdc-test-tmp-owner") == 0);
  REQUIRE(closedir(directory) == 0);
  lc_test_tmp_cleanup_path(root, prefix);
  printf("pouch-space canonical controls: %lu observable checks passed\n",
         (unsigned long)checks);
  return 0;
}
