#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "lc_api_internal.h"
#include "revision.h"
#include "space_internal.h"
#include "stream.h"
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocations, allocation_fail;
static int measuring;
static int cooperative_wait;
static int mapping_fail;
void *__real_mmap(void *, size_t, int, int, int, off_t);
static int fail_mapping(int flags, off_t offset) {
  if (mapping_fail && (flags & MAP_SHARED) && offset == PS_READINESS_OFFSET) {
    errno = ENOMEM;
    return 1;
  }
  return 0;
}
void *__wrap_mmap(void *p, size_t n, int prot, int flags, int fd,
                  off_t offset) {
  return fail_mapping(flags, offset)
             ? MAP_FAILED
             : __real_mmap(p, n, prot, flags, fd, offset);
}
void *__wrap_mmap64(void *p, size_t n, int prot, int flags, int fd,
                    off_t offset) {
  /* Both entry points already take a 64-bit offset on x86_64. Musl
   * exports mmap without the separate glibc mmap64 symbol. */
  return __wrap_mmap(p, n, prot, flags, fd, offset);
}
typedef struct test_adapter {
  ps_coordination_io original, selected;
  struct test_adapter *next;
} test_adapter;
static test_adapter *adapters;
static char path[1500], directory[1400], executable[1600];
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
int __real_ps_coordination_open(int, const ps_coordination_options *,
                                ps_coordination **, lc_error *);
static int native_test_lock(void *context, int fd, int command,
                            struct flock *lock) {
  (void)context;
  return fcntl(fd, command, lock);
}
static int cooperative_lock(void *context, int fd, int command,
                            struct flock *lock) {
  test_adapter *a = context;
  if (command != F_OFD_SETLKW)
    return a->original.lock(a->original.context, fd, command, lock);
  /* The memory-checker's raw blocking OFD syscall can stop guest scheduling.
   * Keep actual kernel authority; yield between nonblocking acquisition tries.
   * Product, native qualification and TSan always use the blocking syscall. */
  for (;;) {
    int rc = a->original.lock(a->original.context, fd, F_OFD_SETLK, lock);
    if (rc == 0 || (errno != EAGAIN && errno != EACCES))
      return rc;
    struct timespec pause = {0, 1000000};
    (void)nanosleep(&pause, NULL);
  }
}
int __wrap_ps_coordination_open(int fd, const ps_coordination_options *o,
                                ps_coordination **out, lc_error *e) {
  ps_coordination_options selected = {0};
  if (o)
    selected = *o;
  selected.shared = 1;
  if (!cooperative_wait)
    return __real_ps_coordination_open(fd, &selected, out, e);
  test_adapter *a = calloc(1, sizeof(*a));
  REQUIRE(a);
  if (selected.io)
    a->original = *selected.io;
  else
    a->original.lock = native_test_lock;
  a->selected.context = a;
  a->selected.lock = cooperative_lock;
  selected.io = &a->selected;
  int rc = __real_ps_coordination_open(fd, &selected, out, e);
  if (rc == LC_OK) {
    a->next = adapters;
    adapters = a;
  } else
    free(a);
  return rc;
}
static int allocation_fails(void) {
  return measuring && ++allocations == allocation_fail;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return allocation_fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t b) {
  return allocation_fails() ? NULL : __real_lc_calloc_with_allocator(a, n, b);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return allocation_fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return allocation_fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "live allocator %d: %s\n", rc,
            e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static ps_key key(uint64_t id) {
  ps_key k = {88, id, PS_STATE};
  return k;
}
static ps_stats status(ps_space *s) {
  lc_error e = {0};
  ps_stats st;
  ok(ps_status(s, &st, &e), &e);
  return st;
}
static void barrier(pthread_barrier_t *b) {
  int rc = pthread_barrier_wait(b);
  REQUIRE(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD);
}
typedef struct span {
  uint64_t first, last;
} span;
typedef struct saved {
  span range;
  unsigned char *bytes;
  struct saved *next;
} saved;
static saved *images;
static uint64_t coverage(ps_space *ignore, int capture) {
  span *pending = calloc(4096, sizeof(*pending));
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(pending && fd >= 0);
  struct stat st;
  REQUIRE(fstat(fd, &st) == 0 && st.st_size >= 0);
  size_t count = 2;
  pending[0].first = 2 * PS_PAGE;
  pending[0].last = PS_JOURNAL_OFFSET;
  pending[1].first = PS_DATA;
  pending[1].last = (uint64_t)st.st_size > PS_DEFAULT_CAPACITY
                        ? (uint64_t)st.st_size
                        : PS_DEFAULT_CAPACITY;
  uint64_t total = 0;
  while (count) {
    span current = pending[--count];
    struct flock lock = {0};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = (off_t)current.first;
    lock.l_len = (off_t)(current.last - current.first);
    REQUIRE(fcntl(ignore ? ignore->fd : fd, F_OFD_GETLK, &lock) == 0);
    if (lock.l_type == F_UNLCK)
      continue;
    REQUIRE(lock.l_type == F_RDLCK && lock.l_len > 0 && lock.l_start >= 0);
    uint64_t first = (uint64_t)lock.l_start,
             last = first + (uint64_t)lock.l_len;
    if (first < current.first)
      first = current.first;
    if (last > current.last)
      last = current.last;
    REQUIRE(first < last && count + 2 <= 4096);
    total += last - first;
    if (capture) {
      saved *p = calloc(1, sizeof(*p));
      REQUIRE(p);
      p->range.first = first;
      p->range.last = last;
      p->bytes = malloc((size_t)(last - first));
      REQUIRE(p->bytes);
      REQUIRE(pread(fd, p->bytes, (size_t)(last - first), (off_t)first) ==
              (ssize_t)(last - first));
      p->next = images;
      images = p;
    }
    if (first > current.first) {
      pending[count].first = current.first;
      pending[count++].last = first;
    }
    if (last < current.last) {
      pending[count].first = last;
      pending[count++].last = current.last;
    }
  }
  REQUIRE(close(fd) == 0);
  free(pending);
  return total;
}
static void unchanged(void) {
  int fd = open(path, O_RDONLY | O_CLOEXEC);
  REQUIRE(fd >= 0);
  while (images) {
    saved *p = images;
    images = p->next;
    size_t n = (size_t)(p->range.last - p->range.first);
    unsigned char *bytes = malloc(n);
    REQUIRE(bytes);
    REQUIRE(pread(fd, bytes, n, (off_t)p->range.first) == (ssize_t)n);
    REQUIRE(!memcmp(bytes, p->bytes, n));
    free(bytes);
    free(p->bytes);
    free(p);
  }
  REQUIRE(close(fd) == 0);
}
static ps_writer *private_value(ps_space *s, ps_budget *b, uint64_t id) {
  lc_error e = {0};
  ps_writer *w = NULL;
  unsigned char *bytes = malloc(PS_CHUNK);
  REQUIRE(bytes);
  memset(bytes, (int)(id % 251), PS_CHUNK);
  ok(ps_begin(s, b, key(id), &w, &e), &e);
  ok(ps_write(w, bytes, PS_CHUNK, &e), &e);
  free(bytes);
  return w;
}
typedef struct worker {
  ps_space *s;
  ps_budget *budget;
  pthread_barrier_t *gate;
  int rc, pressure;
  size_t rounds;
  uint64_t id;
} worker;
static void *admissions(void *context) {
  worker *w = context;
  for (size_t round = 0; round < w->rounds; ++round) {
    lc_error e = {0};
    ps_budget_plan plan = {
        w->pressure ? 6 * PS_MIB : (16 + round % 7) * PS_PAGE, 8192, 16, 0};
    barrier(w->gate);
    w->rc = ps_budget_open(w->s, &plan, &w->budget, &e);
    REQUIRE(w->rc == LC_OK || (w->pressure && w->rc == LC_ERR_INVALID));
    lc_error_cleanup(&e);
    ps_writer *value =
        w->rc == LC_OK ? private_value(w->s, w->budget, w->id + round) : NULL;
    barrier(w->gate);
    barrier(w->gate);
    ps_writer_close(value);
    ok(ps_budget_close(w->budget, &e), &e);
    w->budget = NULL;
    barrier(w->gate);
  }
  return NULL;
}
static void threads(void) {
  for (int pressure = 0; pressure < 2; ++pressure) {
    lc_error e = {0};
    ps_space *a = NULL, *b = NULL;
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    pthread_barrier_t gate;
    REQUIRE(pthread_barrier_init(&gate, NULL, 3) == 0);
    worker w[2] = {{a, NULL, &gate, 0, pressure, pressure ? 8 : 64, 2000},
                   {b, NULL, &gate, 0, pressure, pressure ? 8 : 64, 3000}};
    pthread_t jobs[2];
    REQUIRE(pthread_create(&jobs[0], NULL, admissions, &w[0]) == 0);
    REQUIRE(pthread_create(&jobs[1], NULL, admissions, &w[1]) == 0);
    for (size_t round = 0; round < w[0].rounds; ++round) {
      barrier(&gate);
      barrier(&gate);
      REQUIRE((w[0].rc == LC_OK) + (w[1].rc == LC_OK) == (pressure ? 1 : 2));
      ps_stats x = status(a), y = status(b);
      REQUIRE(coverage(NULL, 0) ==
              x.claimed_metadata_bytes + y.claimed_metadata_bytes +
                  x.claimed_payload_bytes + y.claimed_payload_bytes);
      REQUIRE(x.private_bytes == (w[0].rc == LC_OK ? PS_CHUNK : 0));
      REQUIRE(y.private_bytes == (w[1].rc == LC_OK ? PS_CHUNK : 0));
      barrier(&gate);
      barrier(&gate);
      x = status(a);
      y = status(b);
      REQUIRE(!x.budgets && !y.budgets && !x.claimed_payload_bytes &&
              !y.claimed_payload_bytes);
      REQUIRE(coverage(NULL, 0) ==
              x.claimed_metadata_bytes + y.claimed_metadata_bytes);
    }
    REQUIRE(pthread_join(jobs[0], NULL) == 0 &&
            pthread_join(jobs[1], NULL) == 0);
    REQUIRE(pthread_barrier_destroy(&gate) == 0);
    ok(ps_close(a, &e), &e);
    ok(ps_close(b, &e), &e);
    REQUIRE(coverage(NULL, 0) == 0);
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    ps_reader *r = NULL;
    REQUIRE(ps_read_open(a, key(2000), NULL, &r, &e) == LC_ERR_INVALID && !r);
    lc_error_cleanup(&e);
    ok(ps_close(a, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
}
typedef struct fault {
  int kind, reason, armed;
  size_t calls;
} fault;
static int lock_io(void *context, int fd, int command, struct flock *lock) {
  fault *f = context;
  int match = f->kind == 1   ? command == F_OFD_SETLKW && lock->l_start == 4
              : f->kind == 2 ? command == F_OFD_SETLK && lock->l_start == 4 &&
                                   lock->l_type == F_UNLCK
                             : f->kind == 3 && command == F_OFD_GETLK;
  if (f->armed && match && ++f->calls == 1) {
    errno = f->reason;
    return -1;
  }
  return fcntl(fd, command, lock);
}
static void faults(void) {
  for (int kind = 1; kind <= 3; ++kind)
    for (int reason = 0; reason < 3; ++reason) {
      fault f = {kind, reason == 0 ? EINTR : reason == 1 ? EAGAIN : EIO, 1, 0};
      ps_coordination_io io = {&f, lock_io};
      ps_options o = {0};
      o.coordination_io = &io;
      lc_error e = {0};
      ps_space *s = NULL;
      int rc = ps_open(path, 1, &o, &s, &e);
      f.armed = 0;
      if (!reason) {
        ok(rc, &e);
        ok(ps_close(s, &e), &e);
      } else {
        REQUIRE(rc == (kind == 1 && reason == 1 ? LC_ERR_PENDING
                                                : LC_ERR_TRANSPORT) &&
                !s);
        lc_error_cleanup(&e);
        if (kind == 2) {
          /* Root creation committed before uncertain guard release. Its
           * descriptor closes, and a normal reopen can recover that root. */
          ok(ps_open(path, 0, &o, &s, &e), &e);
          ok(ps_close(s, &e), &e);
        }
      }
      REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
    }
  for (int kind = 1; kind <= 3; ++kind)
    for (int reason = 0; reason < 3; ++reason) {
      fault f = {kind, reason == 0 ? EINTR : reason == 1 ? EAGAIN : EIO, 0, 0};
      ps_coordination_io io = {&f, lock_io};
      ps_options o = {0};
      o.coordination_io = &io;
      lc_error e = {0};
      ps_space *s = NULL;
      ps_budget *b = NULL;
      ok(ps_open(path, 1, &o, &s, &e), &e);
      uint64_t initial = coverage(NULL, 0);
      ps_budget_plan plan = {PS_MIB, 8192, 16, 0};
      f.armed = 1;
      int rc = ps_budget_open(s, &plan, &b, &e);
      f.armed = 0;
      if (!reason) {
        ok(rc, &e);
        ok(ps_budget_close(b, &e), &e);
      } else {
        REQUIRE(rc == (kind == 1 && reason == 1 ? LC_ERR_PENDING
                                                : LC_ERR_TRANSPORT) &&
                !b);
        lc_error_cleanup(&e);
        REQUIRE(ps_check(s, &e) == (kind == 1 ? LC_OK : LC_ERR_TRANSPORT));
        lc_error_cleanup(&e);
      }
      if (!reason || kind == 1)
        REQUIRE(coverage(NULL, 0) == initial);
      ok(ps_close(s, &e), &e);
      REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
    }
  size_t sites = 0;
  for (size_t attempt = 0; attempt <= sites; ++attempt) {
    lc_error e = {0};
    ps_space *a = NULL, *b = NULL;
    ps_budget *foreign = NULL, *local = NULL;
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    ps_budget_plan plan = {PS_MIB, 4096, 4, 0};
    ok(ps_budget_open(b, &plan, &foreign, &e), &e);
    uint64_t initial = coverage(NULL, 0);
    allocations = 0;
    allocation_fail = attempt;
    measuring = 1;
    int rc = ps_budget_open(a, &plan, &local, &e);
    measuring = 0;
    if (!attempt) {
      sites = allocations;
      REQUIRE(sites >= 4);
      ok(rc, &e);
      ok(ps_budget_close(local, &e), &e);
    } else {
      REQUIRE(rc == LC_ERR_NOMEM && !local);
      lc_error_cleanup(&e);
    }
    ok(ps_check(a, &e), &e);
    REQUIRE(coverage(NULL, 0) == initial);
    ok(ps_budget_close(foreign, &e), &e);
    ok(ps_close(a, &e), &e);
    ok(ps_close(b, &e), &e);
    REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
  }
}
static void fence(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {PS_PAGE, 4096, 4, 0};
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ps_writer *seed = private_value(a, NULL, 7);
  ok(ps_publish(seed, NULL, &e), &e);
  ps_writer_close(seed);
  ok(ps_checkpoint(a, &e), &e);
  uint64_t endpoint = a->token_end;
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  REQUIRE(a->token_limit > endpoint && a->token_end == endpoint);
  /* Boundary fixture: simulate consuming the participant's original grant.
   * Observing the peer's larger frontier must not admit one extra writer. */
  uint64_t prior = a->token_next;
  a->token_next = endpoint + 1;
  ps_writer *w = NULL;
  REQUIRE(ps_begin(a, NULL, key(1), &w, &e) == LC_ERR_INVALID && !w);
  lc_error_cleanup(&e);
  a->token_next = prior;
  ok(ps_budget_close(budget, &e), &e);
  w = private_value(a, NULL, 1);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  uint64_t before = coverage(NULL, 0);
  ok(ps_budget_open(b, &plan, &budget, &e), &e);
  w = private_value(a, NULL, 2);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  plan.payload_bytes += PS_PAGE;
  ok(ps_budget_resize(budget, &plan, &e), &e);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_check(b, &e), &e);
  REQUIRE(coverage(NULL, 0) == before);
  ok(ps_checkpoint(a, &e), &e);
  REQUIRE(ps_checkpoint(b, &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  ok(ps_check(b, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ps_reader *r = NULL;
  ok(ps_read_open(a, key(1), NULL, &r, &e), &e);
  REQUIRE(ps_read_size(r) == PS_CHUNK);
  ps_reader_close(r);
  ok(ps_close(a, &e), &e);
  REQUIRE(unlink(path) == 0);
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  unsigned char rejected[64];
  memset(rejected, 0xa5, sizeof(rejected));
  /* Simulate an interrupted foreign control write. An unknown changed prefix
   * cannot become a new allocation's baseline merely because it is invalid. */
  ps_coordination_guard *guard = NULL;
  ok(ps_coordination_lock(a->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                          &guard, &e),
     &e);
  ps_readiness_begin(a, PS_READINESS_JOURNAL);
  REQUIRE(pwrite(a->fd, rejected, sizeof(rejected), PS_JOURNAL_OFFSET) ==
          (ssize_t)sizeof(rejected));
  ps_coordination_unlock(guard);
  REQUIRE(ps_budget_open(a, &plan, &budget, &e) == LC_ERR_PENDING && !budget);
  lc_error_cleanup(&e);
  ok(ps_check(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  /* Recovery retires the rejected tail before its reopen grant. Observing
   * that valid identity-only boundary permits this unchanged view to resume. */
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
}
static void child(int ready, int control) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *b = NULL;
  ok(ps_open(path, 0, NULL, &s, &e), &e);
  ps_budget_plan plan = {PS_MIB, 4096, 4, 0};
  ok(ps_budget_open(s, &plan, &b, &e), &e);
  ps_writer *w = private_value(s, b, 9000);
  ps_stats st = status(s);
  REQUIRE(write(ready, &st, sizeof(st)) == (ssize_t)sizeof(st));
  char command;
  REQUIRE(read(control, &command, 1) == 1 && command == 'B');
  ps_writer_close(w);
  ok(ps_budget_close(b, &e), &e);
  REQUIRE(write(ready, "B", 1) == 1);
  REQUIRE(read(control, &command, 1) == 1 && command == 'C');
  ok(ps_close(s, &e), &e);
  REQUIRE(write(ready, "C", 1) == 1);
  REQUIRE(close(ready) == 0 && close(control) == 0);
}
typedef struct trace {
  uint64_t header;
} trace;
static ssize_t read_io(void *c, int fd, void *p, size_t n, off_t at) {
  (void)c;
  return pread(fd, p, n, at);
}
static ssize_t write_io(void *c, int fd, const void *p, size_t n, off_t at) {
  if (n == PS_RUN_HEADER && at >= PS_DATA)
    ((trace *)c)->header = (uint64_t)at;
  return pwrite(fd, p, n, at);
}
static int sync_io(void *c, int fd) {
  (void)c;
  return fdatasync(fd);
}
static int reserve_io(void *c, int fd, off_t at, off_t n) {
  (void)c;
  return fallocate(fd, 0, at, n);
}
static ssize_t trace_private_write(void *context, int fd, const void *p,
                                   size_t n, off_t at) {
  if (n == PS_CHUNK && at >= PS_DATA)
    *(uint64_t *)context = (uint64_t)at;
  return pwrite(fd, p, n, at);
}
static void prepared_grant_child(int ready, int control) {
  lc_error e = {0};
  ps_space *space = NULL;
  uint64_t written = 0;
  ps_io io = {&written, read_io,    trace_private_write,
              sync_io,  reserve_io, NULL};
  ps_options options = {0};
  char command;
  options.io = &io;
  ok(ps_open(path, 0, &options, &space, &e), &e);
  REQUIRE(write(ready, "O", 1) == 1);
  REQUIRE(read(control, &command, 1) == 1 && command == 'W');
  REQUIRE(private_value(space, NULL, 9000));
  REQUIRE(write(ready, &written, sizeof(written)) == sizeof(written));
  for (;;)
    pause();
}
static uint64_t expect_zero_grant(ps_space *space, ps_owned_head *head,
                                  const ps_owned_status *info, uint64_t base,
                                  uint64_t physical, uint64_t syncs) {
  lc_error e = {0};
  ps_owned_run *run = NULL;
  ps_key key = {info->ref.incarnation, base, PS_OWNED_RUN};
  unsigned char bytes[PS_PAGE - PS_RUN_HEADER];
  ps_stats before = status(space);
  int found = 0;
  ok(ps_owned_grant(head, NULL, base, PS_PAGE, &run, &found, &e), &e);
  REQUIRE(found && ps_owned_run_capacity(run) == sizeof(bytes));
  REQUIRE(status(space).syncs == before.syncs + syncs);
  ps_entry *p = ps_internal_lookup(space, key);
  REQUIRE(p && p->count == 1 &&
          (!physical || p->extents[0].offset == physical));
  uint64_t selected = p->extents[0].offset;
  ok(ps_owned_read(run, 0, bytes, sizeof(bytes), &e), &e);
  for (size_t i = 0; i < sizeof(bytes); ++i)
    REQUIRE(!bytes[i]);
  ps_owned_run_close(run);
  return selected;
}
static uint64_t prepared_prefix(ps_space *space, uint64_t offset,
                                uint64_t bytes) {
  ps_coordination_guard *guard = NULL;
  lc_error e = {0};
  REQUIRE(!pthread_mutex_lock(&space->mutex));
  ok(ps_internal_capture_begin(space, &guard, &e), &e);
  uint64_t prepared = ps_readiness_zero_prefix(space, offset, bytes);
  ok(ps_internal_capture_end(space, guard, LC_OK, &e), &e);
  REQUIRE(!pthread_mutex_unlock(&space->mutex));
  return prepared;
}
static void prepared_grants(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *x = NULL, *y = NULL;
  ps_owned_status ix, iy;
  ps_key kx = {89, 1, PS_STREAM}, ky = {89, 2, PS_STREAM};
  const uint64_t payload = PS_PAGE - PS_RUN_HEADER;
  int ready[2], control[2], exited;
  unsigned char observed;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  REQUIRE(pipe(ready) == 0 && pipe(control) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char output[16], input[16];
    REQUIRE(close(ready[0]) == 0 && close(control[1]) == 0);
    snprintf(output, sizeof(output), "%d", ready[1]);
    snprintf(input, sizeof(input), "%d", control[0]);
    execl(executable, executable, "--prepared-grant-child", path, output, input,
          (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(ready[1]) == 0 && close(control[0]) == 0);
  REQUIRE(read(ready[0], &observed, 1) == 1 && observed == 'O');
  ok(ps_owned_open(a, NULL, kx, 1, NULL, &x, &ix, &e), &e);
  ok(ps_owned_open(b, NULL, ky, 1, NULL, &y, &iy, &e), &e);
  uint64_t first = expect_zero_grant(a, x, &ix, 0, 0, 1);
  expect_zero_grant(b, y, &iy, 0, first + PS_PAGE, 0);
  uint64_t start = first + 2 * PS_PAGE;
  uint64_t end = start + prepared_prefix(a, start, PS_MIB);

  /* A private writer makes no payload publication. Let it disappear before
   * the peer can observe the claim, then require preparation on reuse. */
  REQUIRE(write(control[1], "W", 1) == 1 && close(control[1]) == 0);
  uint64_t dirtied;
  REQUIRE(read(ready[0], &dirtied, sizeof(dirtied)) == sizeof(dirtied));
  REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(close(ready[0]) == 0);
  REQUIRE(dirtied >= start && dirtied + PS_CHUNK <= end);
  REQUIRE(pread(a->fd, &observed, 1, (off_t)(dirtied + PS_RUN_HEADER)) == 1 &&
          observed == 9000 % 251);
  for (uint64_t at = dirtied; at < dirtied + PS_CHUNK; at += PS_PAGE)
    REQUIRE(!prepared_prefix(a, at, PS_PAGE));
  REQUIRE(prepared_prefix(a, dirtied + PS_CHUNK, PS_PAGE) == PS_PAGE);
  uint64_t replacement = expect_zero_grant(a, x, &ix, payload, 0, 1);
  ok(ps_checkpoint(a, &e), &e);
  expect_zero_grant(b, y, &iy, payload, replacement + PS_PAGE, 0);
  REQUIRE(prepared_prefix(b, dirtied + PS_CHUNK, PS_PAGE) == PS_PAGE);
  ps_owned_close(x);
  ps_owned_close(y);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);

  /* The next process cohort has no live zero proof, even when pages remain
   * physically zero and the bitmap bytes survived the previous close. */
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_owned_open(a, NULL, kx, 0, &ix.ref, &x, &ix, &e), &e);
  REQUIRE(ix.capacity == 2 * payload);
  REQUIRE(!prepared_prefix(a, dirtied + PS_CHUNK, PS_PAGE));
  expect_zero_grant(a, x, &ix, ix.capacity, 0, 1);
  ps_owned_close(x);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct prepare_fault {
  trace trace;
  int armed, calls;
} prepare_fault;
static int fail_prepare_sync(void *context, int fd) {
  prepare_fault *fault = context;
  if (fault->armed) {
    ++fault->calls;
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
static void prepared_grant_fault(void) {
  lc_error e = {0};
  prepare_fault fault = {0};
  ps_io io = {&fault, read_io, write_io, fail_prepare_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *head = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status info;
  const ps_key key = {89, 1, PS_STREAM};
  int found = 0;
  options.io = &io;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_owned_open(a, NULL, key, 1, NULL, &head, &info, &e), &e);
  fault.armed = 1;
  REQUIRE(ps_owned_grant(head, NULL, 0, PS_PAGE, &run, &found, &e) ==
              LC_ERR_TRANSPORT &&
          !run && !found && fault.calls == 1 && fault.trace.header >= PS_DATA);
  lc_error_cleanup(&e);
  ps_owned_close(head);
  ok(ps_close(a, &e), &e);
  ok(ps_owned_open(b, NULL, key, 0, NULL, &head, &info, &e), &e);
  REQUIRE(!info.capacity);
  expect_zero_grant(b, head, &info, 0, fault.trace.header, 1);
  ps_owned_close(head);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void processes(void) {
  for (int killed = 0; killed < 2; ++killed) {
    lc_error e = {0};
    ps_space *s = NULL;
    trace record = {0};
    ps_io io = {&record, read_io, write_io, sync_io, reserve_io, NULL};
    ps_options options = {0};
    options.io = &io;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    int ready[2], control[2];
    REQUIRE(pipe(ready) == 0 && pipe(control) == 0);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (!pid) {
      char a[32], b[32];
      close(ready[0]);
      close(control[1]);
      snprintf(a, sizeof(a), "%d", ready[1]);
      snprintf(b, sizeof(b), "%d", control[0]);
      execl(executable, executable, "--child", path, a, b, (char *)NULL);
      _exit(127);
    }
    REQUIRE(close(ready[1]) == 0 && close(control[0]) == 0);
    ps_stats foreign;
    REQUIRE(read(ready[0], &foreign, sizeof(foreign)) ==
            (ssize_t)sizeof(foreign));
    REQUIRE(coverage(s, 1) ==
            foreign.claimed_metadata_bytes + foreign.claimed_payload_bytes);
    ps_budget *budget = NULL, *padding = NULL;
    ps_budget_plan plan = {PS_CHUNK, 1024, 4, 0};
    ok(ps_budget_open(s, &plan, &budget, &e), &e);
    ps_stats st = status(s);
    REQUIRE(st.retired_bytes >=
            foreign.claimed_metadata_bytes + foreign.claimed_payload_bytes);
    REQUIRE(coverage(NULL, 0) ==
            st.claimed_metadata_bytes + st.claimed_payload_bytes +
                foreign.claimed_metadata_bytes + foreign.claimed_payload_bytes);
    ps_writer *w = private_value(s, budget, 1);
    ok(ps_publish(w, NULL, &e), &e);
    ps_writer_close(w);
    ok(ps_budget_close(budget, &e), &e);
    ps_owned_head *head = NULL;
    ps_owned_status info;
    ps_key stream = {89, 1, PS_STREAM};
    ok(ps_owned_open(s, NULL, stream, 1, NULL, &head, &info, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    unchanged();
    st = status(s);
    REQUIRE(st.claimed_metadata_bytes <= PS_METADATA_RESERVE);
    ps_budget_plan fill = {st.capacity - PS_DATA - st.allocated_bytes, 0, 0, 1};
    ok(ps_budget_open(s, &fill, &padding, &e), &e);
    int state;
    char reply;
    if (killed) {
      REQUIRE(kill(pid, SIGKILL) == 0);
      REQUIRE(waitpid(pid, &state, 0) == pid && WIFSIGNALED(state) &&
              WTERMSIG(state) == SIGKILL);
    } else {
      REQUIRE(write(control[1], "B", 1) == 1 &&
              read(ready[0], &reply, 1) == 1 && reply == 'B');
      REQUIRE(write(control[1], "C", 1) == 1 &&
              read(ready[0], &reply, 1) == 1 && reply == 'C');
      REQUIRE(waitpid(pid, &state, 0) == pid && WIFEXITED(state) &&
              WEXITSTATUS(state) == 0);
    }
    REQUIRE(close(ready[0]) == 0 && close(control[1]) == 0);
    ps_owned_run *run = NULL;
    int found = 0;
    ok(ps_owned_grant(head, NULL, 0, PS_MIB, &run, &found, &e), &e);
    REQUIRE(found && record.header == PS_DATA);
    /* Only the released peer's dirty range is unclaimed. A formerly clean
     * local hint must not allow its private bytes into a selected zero grant.
     */
    unsigned char bytes[257];
    REQUIRE(ps_owned_run_capacity(run) >= 3 * PS_CHUNK);
    for (uint64_t at = 0; at < 3 * PS_CHUNK; at += sizeof(bytes)) {
      size_t n = sizeof(bytes);
      if (n > 3 * PS_CHUNK - at)
        n = (size_t)(3 * PS_CHUNK - at);
      ok(ps_owned_read(run, at, bytes, n, &e), &e);
      for (size_t i = 0; i < n; ++i)
        REQUIRE(!bytes[i]);
    }
    ps_owned_run_close(run);
    ps_owned_close(head);
    ok(ps_budget_close(padding, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_close(s, &e), &e);
    REQUIRE(coverage(NULL, 0) == 0);
    ok(ps_open(path, 0, NULL, &s, &e), &e);
    ps_reader *r = NULL;
    ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
    REQUIRE(ps_read_size(r) == PS_CHUNK);
    ps_reader_close(r);
    REQUIRE(ps_read_open(s, key(9000), NULL, &r, &e) == LC_ERR_INVALID && !r);
    lc_error_cleanup(&e);
    ok(ps_close(s, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
}
typedef struct query_gate {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int armed, entered, released, attempted, finished;
} query_gate;
static int pause_query(void *context, int fd, int command, struct flock *lock) {
  query_gate *g = context;
  int rc = fcntl(fd, command, lock);
  if (rc == 0 && command == F_OFD_GETLK && lock->l_type == F_UNLCK &&
      lock->l_start >= PS_DATA) {
    pthread_mutex_lock(&g->mutex);
    if (g->armed) {
      g->armed = 0;
      g->entered = 1;
      pthread_cond_broadcast(&g->cond);
      while (!g->released)
        pthread_cond_wait(&g->cond, &g->mutex);
    }
    pthread_mutex_unlock(&g->mutex);
  }
  return rc;
}
static int trace_attempt(void *context, int fd, int command,
                         struct flock *lock) {
  query_gate *g = context;
  if ((command == F_OFD_SETLKW || (cooperative_wait && command == F_OFD_SETLK &&
                                   lock->l_type == F_WRLCK)) &&
      lock->l_start == 4) {
    pthread_mutex_lock(&g->mutex);
    g->attempted = 1;
    pthread_cond_broadcast(&g->cond);
    pthread_mutex_unlock(&g->mutex);
  }
  return fcntl(fd, command, lock);
}
typedef struct admission {
  ps_space *s;
  ps_budget *budget;
  query_gate *gate;
  int rc, second;
} admission;
static void *one_admission(void *context) {
  admission *a = context;
  lc_error e = {0};
  ps_budget_plan plan = {PS_MIB, 4096, 4, 0};
  a->rc = ps_budget_open(a->s, &plan, &a->budget, &e);
  ok(a->rc, &e);
  if (a->second) {
    pthread_mutex_lock(&a->gate->mutex);
    a->gate->finished = 1;
    pthread_cond_broadcast(&a->gate->cond);
    pthread_mutex_unlock(&a->gate->mutex);
  }
  return NULL;
}
static void serialized(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  query_gate g = {0};
  REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
          pthread_cond_init(&g.cond, NULL) == 0);
  ps_coordination_io ai = {&g, pause_query}, bi = {&g, trace_attempt};
  ps_options ao = {0}, bo = {0};
  ao.coordination_io = &ai;
  bo.coordination_io = &bi;
  ok(ps_open(path, 1, &ao, &a, &e), &e);
  ok(ps_open(path, 0, &bo, &b, &e), &e);
  ps_budget_plan baseline = {0, 1, 1, 0};
  ps_budget *held = NULL;
  ok(ps_budget_open(b, &baseline, &held, &e), &e);
  admission first = {a, NULL, &g, 0, 0}, second = {b, NULL, &g, 0, 1};
  pthread_t x, y;
  g.attempted = 0;
  g.armed = 1;
  REQUIRE(pthread_create(&x, NULL, one_admission, &first) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.entered)
    pthread_cond_wait(&g.cond, &g.mutex);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_create(&y, NULL, one_admission, &second) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.attempted)
    pthread_cond_wait(&g.cond, &g.mutex);
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(fd >= 0);
  struct flock lock = {0};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = 4;
  lock.l_len = 1;
  REQUIRE(fcntl(fd, F_OFD_GETLK, &lock) == 0 && lock.l_type == F_WRLCK);
  REQUIRE(!g.finished && close(fd) == 0);
  ps_coordination_guard *independent = NULL;
  ok(ps_coordination_lock(a->coordination, PS_COORDINATION_PUBLICATION, 0, 0,
                          &independent, &e),
     &e);
  ps_coordination_unlock(independent);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_join(x, NULL) == 0 && pthread_join(y, NULL) == 0);
  ps_stats u = status(a), v = status(b);
  REQUIRE(coverage(NULL, 0) ==
          u.claimed_payload_bytes + v.claimed_payload_bytes +
              u.claimed_metadata_bytes + v.claimed_metadata_bytes);
  ok(ps_budget_close(first.budget, &e), &e);
  ok(ps_budget_close(second.budget, &e), &e);
  ok(ps_budget_close(held, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_close(b, &e), &e);
  REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
          pthread_mutex_destroy(&g.mutex) == 0);
  REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
}
typedef struct body_gate {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  uint64_t offset;
  size_t bytes;
  int paused, entered, released, rc;
  ps_space *s;
} body_gate;
static ssize_t body_write(void *context, int fd, const void *p, size_t n,
                          off_t at) {
  body_gate *g = context;
  if (at >= PS_DATA && n == g->bytes) {
    pthread_mutex_lock(&g->mutex);
    g->offset = (uint64_t)at;
    if (g->paused) {
      g->entered = 1;
      pthread_cond_broadcast(&g->cond);
      while (!g->released)
        pthread_cond_wait(&g->cond, &g->mutex);
    }
    pthread_mutex_unlock(&g->mutex);
  }
  return pwrite(fd, p, n, at);
}
static int small_publish(ps_space *s, ps_budget *b, uint64_t id, size_t n,
                         lc_error *e) {
  unsigned char bytes[65];
  REQUIRE(n <= sizeof(bytes));
  memset(bytes, (int)(id % 251), n);
  ps_writer *w = NULL;
  int rc = ps_begin(s, b, key(id), &w, e);
  if (rc == LC_OK)
    rc = ps_write(w, bytes, n, e);
  if (rc == LC_OK)
    rc = ps_publish(w, NULL, e);
  ps_writer_close(w);
  return rc;
}
static void *packed_producer(void *context) {
  body_gate *g = context;
  lc_error e = {0};
  g->rc = small_publish(g->s, NULL, 64, 1, &e);
  ok(g->rc, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void packed(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  body_gate x = {0}, y = {0};
  x.bytes = 65;
  y.bytes = 1;
  REQUIRE(pthread_mutex_init(&x.mutex, NULL) == 0 &&
          pthread_cond_init(&x.cond, NULL) == 0);
  REQUIRE(pthread_mutex_init(&y.mutex, NULL) == 0 &&
          pthread_cond_init(&y.cond, NULL) == 0);
  ps_io xi = {&x, read_io, body_write, sync_io, reserve_io, NULL};
  ps_io yi = {&y, read_io, body_write, sync_io, reserve_io, NULL};
  ps_options xo = {0}, yo = {0};
  xo.io = &xi;
  yo.io = &yi;
  ok(ps_open(path, 1, &xo, &a, &e), &e);
  /* Exactly one unoccupied cell remains in this packed page. The peer
   * privately occupies it after both canonical views already exist. */
  for (uint64_t id = 1; id <= 63; ++id)
    ok(small_publish(a, NULL, id, 1, &e), &e);
  ok(ps_open(path, 0, &yo, &b, &e), &e);
  y.s = b;
  y.paused = 1;
  pthread_t job;
  REQUIRE(pthread_create(&job, NULL, packed_producer, &y) == 0);
  pthread_mutex_lock(&y.mutex);
  while (!y.entered)
    pthread_cond_wait(&y.cond, &y.mutex);
  pthread_mutex_unlock(&y.mutex);
  ps_budget_plan plan = {PS_PAGE, 88, 1, 0};
  ps_budget *budget = NULL;
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  ok(small_publish(a, budget, 1000, 65, &e), &e);
  REQUIRE(x.offset && (x.offset + 65 <= y.offset || y.offset + 1 <= x.offset));
  pthread_mutex_lock(&y.mutex);
  y.released = 1;
  pthread_cond_broadcast(&y.cond);
  pthread_mutex_unlock(&y.mutex);
  REQUIRE(pthread_join(job, NULL) == 0);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_refresh(a, &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  for (uint64_t id = 1; id <= 1000; ++id) {
    if (id > 64 && id < 1000)
      continue;
    ps_reader *r = NULL;
    int rc = ps_read_open(a, key(id), NULL, &r, &e);
    ok(rc, &e);
    unsigned char bytes[65];
    size_t got = 0;
    ok(ps_read(r, bytes, sizeof(bytes), &got, &e), &e);
    REQUIRE(got == (id == 1000 ? 65 : 1));
    for (size_t i = 0; i < got; ++i)
      REQUIRE(bytes[i] == id % 251);
    ps_reader_close(r);
  }
  ok(ps_close(a, &e), &e);
  REQUIRE(pthread_cond_destroy(&x.cond) == 0 &&
          pthread_mutex_destroy(&x.mutex) == 0);
  REQUIRE(pthread_cond_destroy(&y.cond) == 0 &&
          pthread_mutex_destroy(&y.mutex) == 0);
  REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
}
static void history(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  trace record = {0};
  ps_io io = {&record, read_io, write_io, sync_io, reserve_io, NULL};
  ps_options options = {0};
  options.io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_key stream = {89, 1, PS_STREAM};
  ok(ps_owned_open(s, NULL, stream, 1, NULL, &head, &info, &e), &e);
  uint64_t headroom = PS_MIB + 16 * PS_PAGE + PS_MAINTENANCE_RESERVE;
  ps_budget_plan fill = {PS_DEFAULT_CAPACITY - PS_DATA - headroom, 0, 0, 1};
  ps_budget *padding = NULL;
  ok(ps_budget_open(s, &fill, &padding, &e), &e);
  int ready[2], control[2];
  REQUIRE(pipe(ready) == 0 && pipe(control) == 0);
  pid_t pid = fork();
  REQUIRE(pid >= 0);
  if (!pid) {
    char x[32], y[32];
    close(ready[0]);
    close(control[1]);
    snprintf(x, sizeof(x), "%d", ready[1]);
    snprintf(y, sizeof(y), "%d", control[0]);
    execl(executable, executable, "--child", path, x, y, (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(ready[1]) == 0 && close(control[0]) == 0);
  ps_stats foreign;
  REQUIRE(read(ready[0], &foreign, sizeof(foreign)) ==
          (ssize_t)sizeof(foreign));
  /* The allocator does not observe the foreign extent while it is owned.
   * Current absence of a witness cannot certify that the old bytes are zero. */
  char reply;
  int state;
  REQUIRE(write(control[1], "B", 1) == 1 && read(ready[0], &reply, 1) == 1 &&
          reply == 'B');
  REQUIRE(write(control[1], "C", 1) == 1 && read(ready[0], &reply, 1) == 1 &&
          reply == 'C');
  REQUIRE(waitpid(pid, &state, 0) == pid && WIFEXITED(state) &&
          WEXITSTATUS(state) == 0);
  REQUIRE(close(ready[0]) == 0 && close(control[1]) == 0);
  ps_owned_run *run = NULL;
  int found = 0;
  ok(ps_owned_grant(head, NULL, 0, PS_MIB, &run, &found, &e), &e);
  REQUIRE(found && record.header == PS_DEFAULT_CAPACITY - headroom);
  unsigned char bytes[257];
  for (uint64_t at = 0; at < 3 * PS_CHUNK; at += sizeof(bytes)) {
    size_t n = sizeof(bytes);
    if (n > 3 * PS_CHUNK - at)
      n = (size_t)(3 * PS_CHUNK - at);
    ok(ps_owned_read(run, at, bytes, n, &e), &e);
    for (size_t i = 0; i < n; ++i)
      REQUIRE(!bytes[i]);
  }
  ps_owned_run_close(run);
  ps_owned_close(head);
  ok(ps_budget_close(padding, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(coverage(NULL, 0) == 0 && unlink(path) == 0);
}
static void captured_expect(ps_reader *r, unsigned char value, size_t bytes) {
  lc_error e = {0};
  unsigned char buffer[997];
  size_t done = 0, n;
  while (done < bytes) {
    ok(ps_read(r, buffer, sizeof(buffer), &n, &e), &e);
    REQUIRE(n && n <= bytes - done);
    for (size_t i = 0; i < n; ++i)
      REQUIRE(buffer[i] == value);
    done += n;
  }
  ok(ps_read(r, buffer, sizeof(buffer), &n, &e), &e);
  REQUIRE(!n);
}
static void capture_put_key(ps_space *s, ps_key k, size_t bytes) {
  lc_error e = {0};
  unsigned char buffer[997];
  ps_writer *w = NULL;
  memset(buffer, (int)(k.key % 251), sizeof(buffer));
  ok(ps_begin(s, NULL, k, &w, &e), &e);
  while (bytes) {
    size_t n = bytes < sizeof(buffer) ? bytes : sizeof(buffer);
    ok(ps_write(w, buffer, n, &e), &e);
    bytes -= n;
  }
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
}
static void capture_put(ps_space *s, uint64_t id, size_t bytes) {
  capture_put_key(s, key(id), bytes);
}
static void capture_fragmented(ps_space *s, uint64_t id) {
  lc_error e = {0};
  unsigned char *buffer = malloc(PS_CHUNK);
  REQUIRE(buffer);
  memset(buffer, (int)(id % 251), PS_CHUNK);
  ps_writer *w = NULL;
  ok(ps_begin(s, NULL, key(id), &w, &e), &e);
  for (size_t part = 0; part < 3; ++part) {
    ok(ps_write(w, buffer, PS_CHUNK, &e), &e);
    ps_key separator = {89, id * 4 + part, PS_STATE};
    capture_put_key(s, separator, PS_CHUNK);
  }
  ok(ps_write(w, buffer, 65, &e), &e);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  free(buffer);
}
static void captures(void) {
  const size_t sizes[] = {1, 65, 3 * PS_CHUNK + 65};
  for (size_t test = 0; test < sizeof(sizes) / sizeof(*sizes); ++test) {
    lc_error e = {0};
    ps_space *a = NULL, *b = NULL;
    ps_reader *first = NULL, *second = NULL, *snapshot = NULL, *fresh = NULL;
    ps_values *view = NULL;
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    capture_put(a, 1, sizes[test]);
    ok(ps_checkpoint(a, &e), &e);
    uint64_t baseline = coverage(NULL, 0);
    ok(ps_read_open(a, key(1), NULL, &first, &e), &e);
    uint64_t held = status(a).retained_payload_bytes;
    REQUIRE(held == (sizes[test] + PS_CELL - 1) / PS_CELL * PS_CELL);
    REQUIRE(coverage(NULL, 0) == baseline + held);
    ok(ps_read_open(a, key(1), NULL, &second, &e), &e);
    ok(ps_values_capture(a, 88, PS_STATE, PS_STATE, &view, &e), &e);
    REQUIRE(ps_values_count(view) == 1 &&
            status(a).retained_payload_bytes == held &&
            coverage(NULL, 0) == baseline + held);
    ps_reader_close(first);
    REQUIRE(coverage(NULL, 0) == baseline + held);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    ok(ps_delete(b, key(1), &e), &e);
    ok(ps_checkpoint(b, &e), &e);
    REQUIRE(status(b).retired_bytes >= held);
    for (uint64_t id = 2; id < 34; ++id)
      capture_put(b, id, PS_CHUNK + 1);
    ok(ps_checkpoint(b, &e), &e);
    /* Existing captures survive automatic adoption of the peer checkpoint.
     * A new selection observes the removed key without pinning old bytes. */
    REQUIRE(ps_read_open(a, key(1), NULL, &fresh, &e) == LC_ERR_INVALID &&
            !fresh);
    lc_error_cleanup(&e);
    ok(ps_check(a, &e), &e);
    ok(ps_values_read_open(view, 0, &snapshot, &e), &e);
    ps_values_close(view);
    captured_expect(second, 1, sizes[test]);
    captured_expect(snapshot, 1, sizes[test]);
    ps_reader_close(second);
    REQUIRE(status(a).retained_payload_bytes == held);
    /* Cold recovery must reconstruct the witnesses even after the peer
     * discards its retired metadata. No retained application body is copied. */
    ok(ps_close(b, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    REQUIRE(status(b).retired_bytes >= held);
    unsigned char byte;
    ok(ps_read_at(snapshot, 0, &byte, 1, &e), &e);
    REQUIRE(byte == 1);
    ps_reader_close(snapshot);
    REQUIRE(!status(a).retained_payload_bytes);
    ok(ps_close(a, &e), &e);
    ok(ps_checkpoint(b, &e), &e);
    REQUIRE(!status(b).retired_bytes);
    ok(ps_close(b, &e), &e);
    REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  }
}
typedef struct capture_fault {
  int kind, reason, armed, rollback;
  size_t nth, calls;
} capture_fault;
static int capture_lock(void *context, int fd, int command,
                        struct flock *lock) {
  capture_fault *f = context;
  int payload = command == F_OFD_SETLK && lock->l_start >= PS_DATA;
  int match = f->kind == 1   ? payload && lock->l_type == F_RDLCK
              : f->kind == 2 ? payload && lock->l_type == F_UNLCK
              : f->kind == 3 ? command == F_OFD_SETLKW && lock->l_start == 4
              : f->kind == 4 ? command == F_OFD_SETLK && lock->l_start == 4 &&
                                   lock->l_type == F_UNLCK
              : f->kind == 5
                  ? command == F_OFD_GETLK
                  : command == F_OFD_SETLK && lock->l_start >= 2 * PS_PAGE &&
                        lock->l_type == F_RDLCK;
  if (f->armed && ((match && ++f->calls == f->nth) ||
                   (f->rollback && command == F_OFD_SETLK &&
                    lock->l_start >= 2 * PS_PAGE && lock->l_type == F_UNLCK))) {
    errno = f->reason;
    return -1;
  }
  return fcntl(fd, command, lock);
}
static void capture_faults(void) {
  /* Each extent acquisition, guard admission and guard release, both ordinary
   * readers and vector captures. Clean acquisition failures are retryable;
   * uncertain releases fail stop and retain residual authority until close. */
  for (int vector = 0; vector < 2; ++vector)
    for (int kind = 1; kind <= 4; ++kind)
      for (int reason = 0; reason < 3; ++reason)
        for (size_t nth = 1; nth <= (kind <= 2 ? (vector ? 8U : 4U) : 1U);
             ++nth) {
          lc_error e = {0};
          capture_fault f = {
              kind, reason == 0 ? EINTR : reason == 1 ? EAGAIN : EIO, 0, 0, nth,
              0};
          ps_coordination_io io = {&f, capture_lock};
          ps_options options = {0};
          options.coordination_io = &io;
          ps_space *s = NULL;
          ps_reader *r = NULL;
          ps_values *view = NULL;
          ok(ps_open(path, 1, &options, &s, &e), &e);
          capture_fragmented(s, 1);
          if (vector)
            capture_fragmented(s, 2);
          ok(ps_checkpoint(s, &e), &e);
          uint64_t before = coverage(NULL, 0);
          f.armed = kind != 2;
          int rc = vector
                       ? ps_values_capture(s, 88, PS_STATE, PS_STATE, &view, &e)
                       : ps_read_open(s, key(1), NULL, &r, &e);
          if (kind == 2 || !reason) {
            ok(rc, &e);
            REQUIRE(status(s).retained_payload_bytes ==
                    (vector ? 2U : 1U) * (3 * PS_CHUNK + 128));
            if (kind == 2)
              f.armed = 1;
            ps_values_close(view);
            ps_reader_close(r);
          } else {
            REQUIRE(rc == ((kind == 1 || kind == 3) && reason == 1
                               ? LC_ERR_PENDING
                               : LC_ERR_TRANSPORT));
            REQUIRE(!r && !view && !s->readers);
            lc_error_cleanup(&e);
          }
          REQUIRE(f.calls >= nth);
          f.armed = 0;
          if ((kind == 2 || kind == 4) && reason) {
            REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
            lc_error_cleanup(&e);
            REQUIRE(status(s).retained_payload_bytes > 0);
          } else {
            ok(ps_check(s, &e), &e);
            REQUIRE(!status(s).retained_payload_bytes &&
                    coverage(NULL, 0) == before);
            ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
            captured_expect(r, 1, 3 * PS_CHUNK + 65);
            ps_reader_close(r);
          }
          ok(ps_close(s, &e), &e);
          REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
        }
  /* A failed acquisition followed by an uncertain rollback must not escape
   * as a clean Pending result or expose a partially captured vector. */
  for (int vector = 0; vector < 2; ++vector) {
    lc_error e = {0};
    capture_fault f = {1, EAGAIN, 0, 1, 3, 0};
    ps_coordination_io io = {&f, capture_lock};
    ps_options options = {0};
    options.coordination_io = &io;
    ps_space *s = NULL;
    ps_reader *r = NULL;
    ps_values *view = NULL;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    capture_fragmented(s, 1);
    ok(ps_checkpoint(s, &e), &e);
    f.armed = 1;
    int rc = vector ? ps_values_capture(s, 88, PS_STATE, PS_STATE, &view, &e)
                    : ps_read_open(s, key(1), NULL, &r, &e);
    REQUIRE(rc == LC_ERR_TRANSPORT && !r && !view && !s->readers);
    lc_error_cleanup(&e);
    REQUIRE(status(s).retained_payload_bytes == 2 * PS_CHUNK);
    f.armed = 0;
    ok(ps_close(s, &e), &e);
    REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  }
  for (size_t nth = 0; nth <= 2; ++nth) {
    lc_error e = {0};
    ps_space *s = NULL;
    ps_values *view = NULL;
    capture_fault f = {1, EIO, 0, 0, (size_t)-1, 0};
    ps_coordination_io io = {&f, capture_lock};
    ps_options options = {0};
    options.coordination_io = &io;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    capture_put(s, 1, 3 * PS_CHUNK + 65);
    ok(ps_checkpoint(s, &e), &e);
    uint64_t before = coverage(NULL, 0);
    allocations = 0;
    allocation_fail = nth;
    measuring = 1;
    f.armed = 1;
    int rc = ps_values_capture(s, 88, PS_STATE, PS_STATE, &view, &e);
    measuring = 0;
    REQUIRE(allocations == (nth ? nth : 2));
    /* Contiguous multi-chunk values need one physical witness operation;
     * body size does not multiply the capture's range syscalls. */
    REQUIRE(f.calls == (nth ? 0U : 1U));
    if (nth) {
      REQUIRE(rc == LC_ERR_NOMEM && !view && !s->readers);
      lc_error_cleanup(&e);
    } else {
      ok(rc, &e);
      ps_values_close(view);
    }
    REQUIRE(coverage(NULL, 0) == before);
    ok(ps_close(s, &e), &e);
    REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  }
}
static void capture_owned(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *head = NULL;
  ps_owned_run *original = NULL, *mapped = NULL, *again = NULL, **list = NULL;
  ps_key stream = {88, 1, PS_STREAM};
  ps_owned_status info;
  capture_fault f = {1, EAGAIN, 0, 0, 1, 0};
  ps_coordination_io io = {&f, capture_lock};
  ps_options options = {0};
  options.coordination_io = &io;
  unsigned char *scratch = malloc(PS_CHUNK);
  REQUIRE(scratch);
  memset(scratch, 0x51, PS_CHUNK);
  int found;
  size_t count;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_owned_open(a, NULL, stream, 1, NULL, &head, &info, &e), &e);
  ok(ps_owned_grant(head, NULL, 0, PS_PAGE, &original, &found, &e), &e);
  REQUIRE(found && status(a).retained_payload_bytes == PS_PAGE &&
          !status(a).claimed_payload_bytes);
  ok(ps_owned_write(original, 0, scratch, 1000, &e), &e);
  ps_piece_checksum crc;
  ok(ps_owned_piece_extend(original, 0, NULL, 1000, scratch, &crc, &e), &e);
  ok(ps_owned_commit(head, 1000, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ok(ps_owned_seal(head, &e), &e);
  ps_owned_run_close(original);
  original = NULL;
  REQUIRE(!status(a).retained_payload_bytes);
  f.armed = 1;
  REQUIRE(ps_owned_grant(head, NULL, 0, 0, &original, &found, &e) ==
              LC_ERR_PENDING &&
          !original && !found);
  lc_error_cleanup(&e);
  f.calls = 0;
  REQUIRE(ps_owned_runs_capture(head, &list, &count, &e) == LC_ERR_PENDING &&
          !list && !count && !status(a).retained_payload_bytes);
  lc_error_cleanup(&e);
  f.armed = 0;
  ok(ps_owned_grant(head, NULL, 0, 0, &original, &found, &e), &e);
  REQUIRE(found && status(a).retained_payload_bytes == PS_PAGE);
  ps_owned_range range = {100, 100};
  ok(ps_owned_run_repack(head, original, NULL, &range, 1, NULL, NULL, &mapped,
                         &e),
     &e);
  uint64_t held = PS_PAGE + ps_owned_run_storage(mapped);
  REQUIRE(status(a).retained_payload_bytes == held &&
          !status(a).claimed_payload_bytes);
  /* The packed result was pinned while private. Publication must transfer
   * its exact witness, rather than release the producer's overlapping lock. */
  REQUIRE(coverage(NULL, 0) == status(a).claimed_metadata_bytes + held);
  ok(ps_owned_grant(head, NULL, 0, 0, &again, &found, &e), &e);
  REQUIRE(found);
  ok(ps_owned_runs_capture(head, &list, &count, &e), &e);
  REQUIRE(count == 1 && status(a).retained_payload_bytes == held);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ps_owned_head *peer = NULL;
  ok(ps_owned_open(b, NULL, stream, 0, &info.ref, &peer, &info, &e), &e);
  ok(ps_owned_retire(peer, &e), &e);
  ps_owned_close(peer);
  ok(ps_checkpoint(b, &e), &e);
  for (uint64_t id = 2; id < 34; ++id)
    capture_put(b, id, PS_CHUNK + 65);
  ok(ps_owned_read(original, 100, scratch, 100, &e), &e);
  for (size_t i = 0; i < 100; ++i)
    REQUIRE(scratch[i] == 0x51);
  ok(ps_owned_read(mapped, 100, scratch, 100, &e), &e);
  for (size_t i = 0; i < 100; ++i)
    REQUIRE(scratch[i] == 0x51);
  ps_owned_run_close(mapped);
  ps_owned_run_close(again);
  REQUIRE(status(a).retained_payload_bytes == held);
  ps_owned_run_close(list[0]);
  lc_free_with_allocator(NULL, list);
  REQUIRE(status(a).retained_payload_bytes == PS_PAGE);
  ps_owned_run_close(original);
  ps_owned_close(head);
  REQUIRE(!status(a).retained_payload_bytes);
  ok(ps_close(a, &e), &e);
  ok(ps_checkpoint(b, &e), &e);
  REQUIRE(!status(b).retired_bytes);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  free(scratch);
}
static int pause_capture(void *context, int fd, int command,
                         struct flock *lock) {
  query_gate *g = context;
  if (command == F_OFD_SETLK && lock->l_start >= PS_DATA &&
      lock->l_type == F_RDLCK) {
    pthread_mutex_lock(&g->mutex);
    if (g->armed) {
      g->armed = 0;
      g->entered = 1;
      pthread_cond_broadcast(&g->cond);
      while (!g->released)
        pthread_cond_wait(&g->cond, &g->mutex);
    }
    pthread_mutex_unlock(&g->mutex);
  }
  return fcntl(fd, command, lock);
}
typedef struct capture_worker {
  ps_space *space;
  ps_reader *reader;
  query_gate *gate;
  int retire;
} capture_worker;
static void *one_capture(void *context) {
  capture_worker *w = context;
  lc_error e = {0};
  if (w->retire) {
    ok(ps_delete(w->space, key(1), &e), &e);
    ok(ps_checkpoint(w->space, &e), &e);
    pthread_mutex_lock(&w->gate->mutex);
    w->gate->finished = 1;
    pthread_cond_broadcast(&w->gate->cond);
    pthread_mutex_unlock(&w->gate->mutex);
  } else
    ok(ps_read_open(w->space, key(1), NULL, &w->reader, &e), &e);
  return NULL;
}
static void capture_race(void) {
  for (size_t round = 0; round < 8; ++round) {
    lc_error e = {0};
    ps_space *a = NULL, *b = NULL;
    query_gate g = {0};
    REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
            pthread_cond_init(&g.cond, NULL) == 0);
    ps_coordination_io ai = {&g, pause_capture}, bi = {&g, trace_attempt};
    ps_options ao = {0}, bo = {0};
    ao.coordination_io = &ai;
    bo.coordination_io = &bi;
    ok(ps_open(path, 1, &ao, &a, &e), &e);
    capture_put(a, 1, PS_CHUNK + 65);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_open(path, 0, &bo, &b, &e), &e);
    capture_worker first = {a, NULL, &g, 0}, second = {b, NULL, &g, 1};
    pthread_t x, y;
    g.attempted = 0;
    g.armed = 1;
    REQUIRE(pthread_create(&x, NULL, one_capture, &first) == 0);
    pthread_mutex_lock(&g.mutex);
    while (!g.entered)
      pthread_cond_wait(&g.cond, &g.mutex);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(pthread_create(&y, NULL, one_capture, &second) == 0);
    pthread_mutex_lock(&g.mutex);
    while (!g.attempted)
      pthread_cond_wait(&g.cond, &g.mutex);
    int fd = open(path, O_RDWR | O_CLOEXEC);
    REQUIRE(fd >= 0);
    struct flock lock = {0};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 4;
    lock.l_len = 1;
    REQUIRE(fcntl(fd, F_OFD_GETLK, &lock) == 0 && lock.l_type == F_WRLCK);
    /* Query through the retiring OFD too: it ignores its own locks. This
     * proves the paused capture, rather than the peer, owns the authority. */
    lock.l_type = F_WRLCK;
    lock.l_pid = 0;
    REQUIRE(fcntl(b->fd, F_OFD_GETLK, &lock) == 0 && lock.l_type == F_WRLCK);
    REQUIRE(!g.finished && close(fd) == 0);
    g.released = 1;
    pthread_cond_broadcast(&g.cond);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(pthread_join(x, NULL) == 0 && pthread_join(y, NULL) == 0);
    captured_expect(first.reader, 1, PS_CHUNK + 65);
    REQUIRE(status(a).retained_payload_bytes == PS_CHUNK + 128);
    REQUIRE(status(b).retired_bytes >= PS_CHUNK + 128);
    ps_reader_close(first.reader);
    ok(ps_close(a, &e), &e);
    ok(ps_checkpoint(b, &e), &e);
    REQUIRE(!status(b).retired_bytes);
    ok(ps_close(b, &e), &e);
    REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
    REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
            pthread_mutex_destroy(&g.mutex) == 0);
  }
}
typedef struct projection_io {
  size_t operations, fail, syncs, payload_reads;
  size_t root_reads, journal_reads;
  int armed, phase, ready, pause_journal;
  span forbidden[16];
  size_t ranges;
} projection_io;
static ssize_t projection_read(void *context, int fd, void *bytes, size_t n,
                               off_t offset) {
  projection_io *m = context;
  if (m->armed) {
    if (offset < 2 * PS_PAGE)
      ++m->root_reads;
    if (offset >= PS_JOURNAL_OFFSET && offset < PS_DATA)
      ++m->journal_reads;
    for (size_t i = 0; i < m->ranges; ++i)
      if ((uint64_t)offset < m->forbidden[i].last &&
          (uint64_t)offset + n > m->forbidden[i].first)
        ++m->payload_reads;
    if (++m->operations == m->fail) {
      errno = EIO;
      return -1;
    }
  }
  return pread(fd, bytes, n, offset);
}
static int projection_sync(void *context, int fd) {
  projection_io *m = context;
  if (m->armed) {
    ++m->syncs;
    if (m->phase && m->syncs == (size_t)m->phase) {
      REQUIRE(write(m->ready, "R", 1) == 1);
      for (;;)
        pause();
    }
    if (++m->operations == m->fail) {
      errno = EIO;
      return -1;
    }
  }
  return fdatasync(fd);
}
static ssize_t projection_write(void *context, int fd, const void *bytes,
                                size_t n, off_t offset) {
  projection_io *m = context;
  if (m->pause_journal && offset >= PS_JOURNAL_OFFSET && offset < PS_DATA) {
    REQUIRE(n >= 64 && pwrite(fd, bytes, 32, offset) == 32);
    REQUIRE(write(m->ready, "J", 1) == 1);
    for (;;)
      pause();
  }
  if (m->armed && ++m->operations == m->fail) {
    errno = EIO;
    return -1;
  }
  return pwrite(fd, bytes, n, offset);
}
static ps_stats projection_coverage(ps_space *a, ps_space *b) {
  ps_stats x = status(a), y = status(b);
  REQUIRE(coverage(NULL, 0) ==
          x.claimed_payload_bytes + x.claimed_metadata_bytes +
              x.retained_payload_bytes + y.claimed_payload_bytes +
              y.claimed_metadata_bytes + y.retained_payload_bytes);
  return x;
}
static double projection_milliseconds(void) {
  struct timespec now;
  REQUIRE(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
  return now.tv_sec * 1000.0 + now.tv_nsec / 1000000.0;
}
static void projection_lifecycle(void) {
  lc_error e = {0};
  projection_io m = {0};
  ps_io io = {0};
  ps_options options = {0};
  io.context = &m;
  io.read = projection_read;
  io.write = projection_write;
  io.reserve = reserve_io;
  io.sync = projection_sync;
  options.io = &io;
  ps_space *a = NULL, *b = NULL;
  ps_reader *old = NULL, *replaced = NULL, *current = NULL;
  ps_values *values = NULL;
  ps_catalog_view *binding = NULL;
  ps_catalog_info catalog, observed;
  ps_owned_head *head = NULL, *peer = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status owned;
  ps_key stream = {88, 900, PS_STREAM}, snapshot = {777, 1, PS_STATE};
  int found;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  for (uint64_t id = 1; id <= 350; ++id)
    capture_put(a, id, id == 3 ? 3 * PS_CHUNK : 65);
  capture_put_key(a, snapshot, 65);
  ok(ps_owned_open(a, NULL, stream, 1, NULL, &head, &owned, &e), &e);
  ok(ps_owned_grant(head, NULL, 0, PS_PAGE, &run, &found, &e), &e);
  REQUIRE(found);
  unsigned char body[100];
  memset(body, 0x33, sizeof(body));
  ok(ps_owned_write(run, 0, body, sizeof(body), &e), &e);
  ok(ps_catalog_put(a, NULL, 88, PS_CATALOG_BODY, "retained", 8, NULL, stream,
                    &owned.ref, &catalog, &e),
     &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_read_open(a, key(1), NULL, &old, &e), &e);
  ok(ps_read_open(a, key(2), NULL, &replaced, &e), &e);
  ok(ps_values_capture(a, 777, PS_STATE, PS_STATE, &values, &e), &e);
  ok(ps_catalog_get(a, 88, PS_CATALOG_BODY, "retained", 8, &binding, &e), &e);
  REQUIRE(binding && ps_values_count(values) == 1);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ps_budget_plan plan = {PS_CHUNK + 2 * PS_PAGE, 4096, 8, 0}, before, after;
  ps_budget *budget = NULL;
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  ps_writer *producer = private_value(a, budget, 9000);
  ok(ps_budget_status(budget, &before, &e), &e);
  uint64_t endpoint = a->token_end, next_token = a->token_next;
  ok(ps_owned_commit(head, sizeof(body), PS_COMMIT_DEFAULT, NULL, NULL, &e),
     &e);
  /* Its structural tail hint is deliberately absent from the peer's view. */
  ok(ps_delete(b, key(1), &e), &e);
  ok(ps_delete(b, snapshot, &e), &e);
  capture_put(b, 2, 1000);
  for (size_t round = 0; round < 3; ++round) {
    for (uint64_t id = 10; id <= 350; id += 3)
      capture_put(b, id, 65 + round);
    uint64_t protected_bytes = coverage(b, 1);
    REQUIRE(protected_bytes > PS_CHUNK);
    ok(ps_checkpoint(b, &e), &e);
    unchanged();
    ps_entry *large = ps_internal_lookup(b, key(3));
    REQUIRE(large && large->count <= 16);
    m.ranges = large->count;
    for (size_t i = 0; i < m.ranges; ++i) {
      m.forbidden[i].first = large->extents[i].offset;
      m.forbidden[i].last = large->extents[i].offset + large->extents[i].bytes;
    }
    m.syncs = m.operations = m.payload_reads = 0;
    m.armed = 1;
    uint64_t generation = a->generation;
    double started = projection_milliseconds();
    ok(ps_refresh(a, &e), &e);
    double elapsed = projection_milliseconds() - started;
    fprintf(stderr,
            "projection_measure entries=%llu refresh_ms=%.3f operations=%zu "
            "syncs=%zu payload_reads=%zu\n",
            (unsigned long long)a->live_entries, elapsed, m.operations, m.syncs,
            m.payload_reads);
    REQUIRE(a->generation > generation && a->generation == b->generation &&
            m.syncs == 0 && !m.payload_reads && m.operations < 100);
    ok(ps_refresh(a, &e), &e);
    REQUIRE(m.syncs == 0 && !m.payload_reads);
    m.armed = 0;
    REQUIRE(a->token_next == next_token + round && a->token_end == endpoint);
    ok(ps_budget_status(budget, &after, &e), &e);
    REQUIRE(after.payload_bytes == before.payload_bytes &&
            after.metadata_bytes == before.metadata_bytes &&
            after.entries == before.entries);
    REQUIRE(projection_coverage(a, b).private_bytes == PS_CHUNK);
    ps_writer *scratch = private_value(a, NULL, 9500 + round);
    ps_writer_close(scratch);
    for (uint64_t id = 1; id <= 350; ++id) {
      ps_ref ref;
      uint64_t bytes;
      int present;
      ok(ps_value_info(a, key(id), &ref, &bytes, &present, &e), &e);
      REQUIRE(present == (id != 1));
      if (present)
        REQUIRE(bytes == (id == 2                          ? 1000U
                          : id == 3                        ? 3U * PS_CHUNK
                          : id >= 10 && (id - 10) % 3 == 0 ? 65U + round
                                                           : 65U));
    }
    ok(ps_owned_status_get(head, &owned, &e), &e);
    REQUIRE(owned.tail == sizeof(body));
    ok(ps_read_open(a, key(350), NULL, &current, &e), &e);
    captured_expect(current, 350 % 251, 65);
    ps_reader_close(current);
  }
  ok(ps_grow(b, PS_MIB, &e), &e);
  ok(ps_refresh(a, &e), &e);
  REQUIRE(status(a).capacity == PS_DEFAULT_CAPACITY + PS_MIB);
  (void)projection_coverage(a, b);
  /* Canonical buckets and backlinks must survive reuse, including collisions.
   * The unjournaled local tail remains dirty and reaches the next checkpoint.
   */
  captured_expect(old, 1, 65);
  captured_expect(replaced, 2, 65);
  ok(ps_values_read_open(values, 0, &current, &e), &e);
  captured_expect(current, 1, 65);
  ps_reader_close(current);
  ok(ps_publish(producer, NULL, &e), &e);
  ps_writer_close(producer);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_refresh(b, &e), &e);
  ok(ps_read_open(b, key(9000), NULL, &current, &e), &e);
  captured_expect(current, 9000 % 251, PS_CHUNK);
  ps_reader_close(current);
  ok(ps_owned_open(b, NULL, stream, 0, &owned.ref, &peer, &owned, &e), &e);
  REQUIRE(owned.tail == sizeof(body));
  ok(ps_catalog_remove(b, 88, PS_CATALOG_BODY, "retained", 8, &catalog.revision,
                       &e),
     &e);
  ok(ps_owned_retire(peer, &e), &e);
  ps_owned_close(peer);
  ok(ps_checkpoint(b, &e), &e);
  ok(ps_refresh(a, &e), &e);
  ok(ps_catalog_view_info(binding, &observed, &e), &e);
  REQUIRE(observed.revision.incarnation == catalog.revision.incarnation);
  ok(ps_owned_status_get(head, &owned, &e), &e);
  REQUIRE(owned.retired);
  ok(ps_owned_read(run, 0, body, sizeof(body), &e), &e);
  for (size_t i = 0; i < sizeof(body); ++i)
    REQUIRE(body[i] == 0x33);
  ps_reader_close(old);
  ps_reader_close(replaced);
  ps_values_close(values);
  ps_catalog_view_close(binding);
  ps_owned_run_close(run);
  ps_owned_close(head);
  ok(ps_close(a, &e), &e);
  ok(ps_checkpoint(b, &e), &e);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0));
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_read_open(a, key(9000), NULL, &current, &e), &e);
  captured_expect(current, 9000 % 251, PS_CHUNK);
  ps_reader_close(current);
  ok(ps_close(a, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void projection_faults(void) {
  size_t sites = 0, operations = 0;
  for (int kind = 0; kind < 2; ++kind)
    for (size_t phase = 0; phase <= (kind ? operations : sites); ++phase) {
      lc_error e = {0};
      projection_io m = {0};
      ps_io io = {0};
      ps_options options = {0};
      io.context = &m;
      io.read = projection_read;
      io.write = projection_write;
      io.reserve = reserve_io;
      io.sync = projection_sync;
      options.io = &io;
      ps_space *a = NULL, *b = NULL;
      ps_reader *held = NULL;
      ps_writer *producer = NULL;
      ok(ps_open(path, 1, &options, &a, &e), &e);
      for (uint64_t id = 1; id <= 6; ++id)
        capture_put(a, id, 65);
      ok(ps_checkpoint(a, &e), &e);
      ok(ps_read_open(a, key(1), NULL, &held, &e), &e);
      ok(ps_open(path, 0, NULL, &b, &e), &e);
      producer = private_value(a, NULL, 9000);
      capture_put(b, 7, 129);
      ok(ps_checkpoint(b, &e), &e);
      capture_put(b, 1, 129);
      capture_put(b, 8, PS_CHUNK);
      uint64_t generation = a->generation, physical = coverage(NULL, 0);
      allocations = 0;
      allocation_fail = kind ? 0 : phase;
      measuring = !kind;
      m.fail = kind ? phase : 0;
      m.armed = 1;
      int rc = ps_refresh(a, &e);
      m.armed = measuring = 0;
      if (!phase) {
        ok(rc, &e);
        if (kind)
          operations = m.operations;
        else
          sites = allocations;
      } else {
        REQUIRE(rc == (kind ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
        lc_error_cleanup(&e);
        REQUIRE(a->generation == generation);
        if (!kind) {
          ok(ps_check(a, &e), &e);
          REQUIRE(coverage(NULL, 0) == physical);
          ok(ps_refresh(a, &e), &e);
        } else {
          REQUIRE(ps_check(a, &e) == LC_ERR_TRANSPORT);
          lc_error_cleanup(&e);
        }
      }
      captured_expect(held, 1, 65);
      ps_reader_close(held);
      ps_writer_close(producer);
      ok(ps_close(a, &e), &e);
      ok(ps_close(b, &e), &e);
      REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
    }
  REQUIRE(sites > 10 && operations > 4);
  fprintf(stderr, "projection faults: %zu allocation sites, %zu I/O sites\n",
          sites, operations);
}
static void projection_child(int ready, int phase) {
  lc_error e = {0};
  projection_io m = {0};
  ps_io io = {0};
  ps_options options = {0};
  io.context = &m;
  io.read = projection_read;
  io.write = projection_write;
  io.reserve = reserve_io;
  io.sync = projection_sync;
  options.io = &io;
  ps_space *s = NULL;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  capture_put(s, 1, 1000);
  m.ready = ready;
  m.phase = phase;
  m.armed = 1;
  ok(ps_checkpoint(s, &e), &e);
  REQUIRE(0);
}
static void projection_recovery(void) {
  for (int phase = 2; phase <= 3; ++phase) {
    lc_error e = {0};
    ps_space *s = NULL;
    ps_reader *old = NULL, *current = NULL;
    ok(ps_open(path, 1, NULL, &s, &e), &e);
    capture_put(s, 1, 65);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_read_open(s, key(1), NULL, &old, &e), &e);
    int ready[2], child_status;
    REQUIRE(pipe(ready) == 0);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (!pid) {
      char a[24], b[24];
      close(ready[0]);
      snprintf(a, sizeof(a), "%d", ready[1]);
      snprintf(b, sizeof(b), "%d", phase);
      execl(executable, executable, "--projection-child", path, a, b,
            (char *)NULL);
      _exit(127);
    }
    REQUIRE(close(ready[1]) == 0);
    char signal;
    REQUIRE(read(ready[0], &signal, 1) == 1 && signal == 'R');
    REQUIRE(kill(pid, SIGKILL) == 0 && waitpid(pid, &child_status, 0) == pid &&
            WIFSIGNALED(child_status) && WTERMSIG(child_status) == SIGKILL);
    REQUIRE(close(ready[0]) == 0);
    ps_stats before = status(s);
    ok(ps_read_open(s, key(1), NULL, &current, &e), &e);
    REQUIRE(status(s).syncs == before.syncs + 1);
    captured_expect(old, 1, 65);
    captured_expect(current, 1, 1000);
    ps_reader_close(old);
    ps_reader_close(current);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_close(s, &e), &e);
    ok(ps_open(path, 0, NULL, &s, &e), &e);
    ok(ps_read_open(s, key(1), NULL, &current, &e), &e);
    captured_expect(current, 1, 1000);
    ps_reader_close(current);
    ok(ps_close(s, &e), &e);
    REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  }
}
typedef struct projection_reader_work {
  ps_reader *reader;
  int stop;
  size_t reads;
} projection_reader_work;
static void *projection_reader_worker(void *context) {
  projection_reader_work *w = context;
  lc_error e = {0};
  unsigned char bytes[65];
  while (!__atomic_load_n(&w->stop, __ATOMIC_ACQUIRE)) {
    ok(ps_read_at(w->reader, 0, bytes, sizeof(bytes), &e), &e);
    REQUIRE(ps_read_size(w->reader) == sizeof(bytes));
    for (size_t i = 0; i < sizeof(bytes); ++i)
      REQUIRE(bytes[i] == 1);
    __atomic_add_fetch(&w->reads, 1, __ATOMIC_RELEASE);
    struct timespec yield = {0, 100000};
    (void)nanosleep(&yield, NULL);
  }
  return NULL;
}
typedef struct projection_grant_work {
  ps_owned_head *head;
  ps_owned_run *run;
  int rc, found;
} projection_grant_work;
static void *projection_grant_worker(void *context) {
  projection_grant_work *w = context;
  lc_error e = {0};
  w->rc = ps_owned_grant(w->head, NULL, 0, PS_PAGE, &w->run, &w->found, &e);
  REQUIRE(w->rc == LC_OK || w->rc == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  return NULL;
}
static void projection_threads(void) {
  for (int retired = 0; retired < 2; ++retired) {
    lc_error e = {0};
    body_gate g = {0};
    REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
            pthread_cond_init(&g.cond, NULL) == 0);
    g.bytes = PS_RUN_HEADER;
    ps_io io = {&g, read_io, body_write, sync_io, reserve_io, NULL};
    ps_options options = {0};
    options.io = &io;
    ps_space *a = NULL, *b = NULL;
    ps_owned_head *head = NULL, *peer = NULL;
    ps_owned_status owned;
    ps_key stream = {88, 1, PS_STREAM};
    ok(ps_open(path, 1, &options, &a, &e), &e);
    capture_put(a, 1, 65);
    ok(ps_owned_open(a, NULL, stream, 1, NULL, &head, &owned, &e), &e);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    projection_reader_work reader = {0};
    ok(ps_read_open(a, key(1), NULL, &reader.reader, &e), &e);
    pthread_t reading, producing;
    REQUIRE(pthread_create(&reading, NULL, projection_reader_worker, &reader) ==
            0);
    g.paused = 1;
    projection_grant_work producer = {head, NULL, 0, 0};
    REQUIRE(pthread_create(&producing, NULL, projection_grant_worker,
                           &producer) == 0);
    pthread_mutex_lock(&g.mutex);
    while (!g.entered)
      pthread_cond_wait(&g.cond, &g.mutex);
    pthread_mutex_unlock(&g.mutex);
    for (size_t round = 0; round < 16; ++round) {
      capture_put(b, 2, 65 + round);
      if (retired && round == 15) {
        ok(ps_owned_open(b, NULL, stream, 0, &owned.ref, &peer, &owned, &e),
           &e);
        ok(ps_owned_retire(peer, &e), &e);
        ps_owned_close(peer);
      }
      ok(ps_checkpoint(b, &e), &e);
      ok(ps_refresh(a, &e), &e);
      REQUIRE(status(a).private_bytes >= PS_PAGE);
    }
    pthread_mutex_lock(&g.mutex);
    g.released = 1;
    pthread_cond_broadcast(&g.cond);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(pthread_join(producing, NULL) == 0);
    REQUIRE(producer.rc == (retired ? LC_ERR_INVALID : LC_OK) &&
            producer.found == !retired);
    __atomic_store_n(&reader.stop, 1, __ATOMIC_RELEASE);
    REQUIRE(pthread_join(reading, NULL) == 0 && reader.reads);
    ps_reader_close(reader.reader);
    ps_owned_run_close(producer.run);
    ps_owned_close(head);
    ok(ps_check(a, &e), &e);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_close(a, &e), &e);
    ok(ps_close(b, &e), &e);
    REQUIRE(!coverage(NULL, 0));
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    int rc = ps_owned_open(a, NULL, stream, 0, &owned.ref, &head, &owned, &e);
    REQUIRE(rc == (retired ? LC_ERR_INVALID : LC_OK));
    if (retired)
      lc_error_cleanup(&e);
    else {
      REQUIRE(owned.runs == 1);
      ps_owned_close(head);
    }
    ok(ps_close(a, &e), &e);
    REQUIRE(unlink(path) == 0);
    REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
            pthread_mutex_destroy(&g.mutex) == 0);
  }
}
static void projection_boundaries(void) {
  /* Admission and physical staging are one revision scope. Exercise partial
   * workspace acquisition/rollback as well as uncertain final guard release. */
  for (int kind = 3; kind <= 6; ++kind)
    for (int reason = 0; reason < 4; ++reason) {
      if (reason == 3 && kind != 6)
        continue;
      lc_error e = {0};
      capture_fault f = {kind,
                         reason == 0                  ? EINTR
                         : reason == 1 || reason == 3 ? EAGAIN
                                                      : EIO,
                         0,
                         reason == 3,
                         kind == 6 ? 2U : 1U,
                         0};
      ps_coordination_io io = {&f, capture_lock};
      ps_options options = {0};
      options.coordination_io = &io;
      ps_space *a = NULL, *b = NULL;
      ps_reader *reader = NULL;
      ps_budget *budget = NULL;
      ps_budget_plan plan = {2 * PS_PAGE, 512 * 1024, 4096, 0};
      ok(ps_open(path, 1, &options, &a, &e), &e);
      capture_put(a, 1, 65);
      ok(ps_checkpoint(a, &e), &e);
      ok(ps_read_open(a, key(1), NULL, &reader, &e), &e);
      ok(ps_budget_open(a, &plan, &budget, &e), &e);
      ok(ps_open(path, 0, NULL, &b, &e), &e);
      for (uint64_t id = 2; id <= 258; ++id)
        capture_put(b, id, 65);
      ok(ps_checkpoint(b, &e), &e);
      ok(ps_close(b, &e), &e);
      uint64_t generation = a->generation, bytes = coverage(NULL, 0);
      f.armed = 1;
      int rc = ps_refresh(a, &e);
      f.armed = 0;
      REQUIRE(f.calls >= f.nth);
      int poisoned = ((kind == 4 || kind == 5) && reason) || reason == 3;
      if (!reason) {
        ok(rc, &e);
        REQUIRE(a->generation > generation);
      } else {
        REQUIRE(rc == ((kind == 3 || kind == 6) && reason == 1
                           ? LC_ERR_PENDING
                           : LC_ERR_TRANSPORT));
        lc_error_cleanup(&e);
        REQUIRE((a->generation > generation) == (kind == 4));
        if (!poisoned) {
          REQUIRE(coverage(NULL, 0) == bytes);
          ok(ps_refresh(a, &e), &e);
        }
      }
      REQUIRE(ps_check(a, &e) == (poisoned ? LC_ERR_TRANSPORT : LC_OK));
      lc_error_cleanup(&e);
      ps_stats st = status(a);
      REQUIRE(coverage(NULL, 0) == st.claimed_payload_bytes +
                                       st.claimed_metadata_bytes +
                                       st.retained_payload_bytes);
      captured_expect(reader, 1, 65);
      ps_reader_close(reader);
      ok(ps_budget_close(budget, &e), &e);
      ok(ps_close(a, &e), &e);
      REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
    }
  /* A missing certified tail fences adoption until the actual bytes return. */
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  capture_put(b, 2, 65);
  ok(ps_checkpoint(b, &e), &e);
  capture_put(b, 3, 65);
  uint64_t generation = a->generation, claims = coverage(NULL, 0);
  unsigned char pending[64], erased[64] = {0};
  REQUIRE(pread(b->fd, pending, sizeof(pending), PS_JOURNAL_OFFSET) ==
          sizeof(pending));
  REQUIRE(pwrite(b->fd, erased, sizeof(erased), PS_JOURNAL_OFFSET) ==
          sizeof(erased));
  REQUIRE(ps_refresh(a, &e) == LC_ERR_PENDING && a->generation == generation);
  lc_error_cleanup(&e);
  REQUIRE(coverage(NULL, 0) == claims);
  REQUIRE(pwrite(b->fd, pending, sizeof(pending), PS_JOURNAL_OFFSET) ==
          sizeof(pending));
  ok(ps_refresh(a, &e), &e);
  REQUIRE(a->generation == b->generation && a->sequence == b->sequence);
  unsigned char prefix[64], damaged[64];
  REQUIRE(pread(b->fd, prefix, sizeof(prefix), PS_JOURNAL_OFFSET) ==
          sizeof(prefix));
  memset(damaged, 0xa5, sizeof(damaged));
  capture_put(b, 4, 65);
  ok(ps_checkpoint(b, &e), &e);
  REQUIRE(pwrite(b->fd, damaged, sizeof(damaged), PS_JOURNAL_OFFSET) ==
          sizeof(damaged));
  generation = a->generation;
  REQUIRE(ps_refresh(a, &e) == LC_ERR_PENDING && a->generation == generation);
  lc_error_cleanup(&e);
  /* Reopening retires the unknown tail and issues a checked grant. */
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_refresh(a, &e), &e);
  REQUIRE(a->token_end < a->token_limit);
  ok(ps_close(a, &e), &e);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
}
static void projection_pressure(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_budget *funded = NULL, *padding = NULL;
  ps_budget_plan plan = {PS_CHUNK, 4096, 8, 0}, occupied = {0};
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_budget_open(a, &plan, &funded, &e), &e);
  ps_writer *private = private_value(a, funded, 9000);
  ok(ps_budget_open(b, &occupied, &padding, &e), &e);
  for (uint64_t step = 4 * PS_MIB; step >= PS_PAGE;) {
    ps_budget_plan attempt = occupied;
    attempt.payload_bytes += step;
    int rc = ps_budget_resize(padding, &attempt, &e);
    if (rc == LC_OK)
      occupied = attempt;
    else {
      REQUIRE(rc == LC_ERR_INVALID);
      lc_error_cleanup(&e);
      step /= 2;
    }
  }
  REQUIRE(occupied.payload_bytes > 8 * PS_MIB);
  ok(ps_checkpoint(b, &e), &e);
  ok(ps_refresh(a, &e), &e);
  ps_writer *probe = NULL;
  unsigned char *bytes = malloc(PS_CHUNK);
  REQUIRE(bytes);
  memset(bytes, 0x7b, PS_CHUNK);
  ok(ps_begin(a, NULL, key(9500), &probe, &e), &e);
  /* Only maintenance capacity is unclaimed. The unfinished funded producer
   * must not appear as an extra ordinary allocation after view replacement. */
  REQUIRE(ps_write(probe, bytes, PS_CHUNK, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  ps_writer_close(probe);
  (void)projection_coverage(a, b);
  ok(ps_publish(private, NULL, &e), &e);
  ps_writer_close(private);
  ok(ps_budget_close(funded, &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_budget_close(padding, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ps_reader *reader = NULL;
  ok(ps_read_open(a, key(9000), NULL, &reader, &e), &e);
  captured_expect(reader, 9000 % 251, PS_CHUNK);
  ps_reader_close(reader);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  free(bytes);
}
static ps_readiness_certificate readiness(ps_space *s) {
  lc_error e = {0};
  ps_coordination_guard *guard = NULL;
  ps_readiness_certificate certificate;
  pthread_mutex_lock(&s->mutex);
  ok(ps_coordination_lock(s->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                          &guard, &e),
     &e);
  ok(ps_readiness_read(s, &certificate, &e), &e);
  ps_coordination_unlock(guard);
  pthread_mutex_unlock(&s->mutex);
  return certificate;
}
typedef struct durable_gate {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int paused, entered, released, attempted, finished;
  size_t calls;
} durable_gate;
static int durable_sync(void *context, int fd) {
  durable_gate *g = context;
  pthread_mutex_lock(&g->mutex);
  ++g->calls;
  if (g->paused) {
    g->paused = 0;
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  return fdatasync(fd);
}
static ssize_t durable_write(void *context, int fd, const void *p, size_t n,
                             off_t offset) {
  (void)context;
  return pwrite(fd, p, n, offset);
}
static int durable_attempt(void *context, int fd, int command,
                           struct flock *lock) {
  durable_gate *g = context;
  if (lock->l_start == 5 && lock->l_type == F_WRLCK &&
      (command == F_OFD_SETLKW ||
       (cooperative_wait && command == F_OFD_SETLK))) {
    pthread_mutex_lock(&g->mutex);
    g->attempted = 1;
    pthread_cond_broadcast(&g->cond);
    pthread_mutex_unlock(&g->mutex);
  }
  return fcntl(fd, command, lock);
}
typedef struct durable_work {
  ps_space *space;
  durable_gate *gate;
} durable_work;
static void *durable_flush(void *context) {
  durable_work *work = context;
  lc_error e = {0};
  ok(ps_flush(work->space, &e), &e);
  pthread_mutex_lock(&work->gate->mutex);
  work->gate->finished = 1;
  pthread_cond_broadcast(&work->gate->cond);
  pthread_mutex_unlock(&work->gate->mutex);
  return NULL;
}
static void shared_durability(void) {
  for (int late = 0; late < 2; ++late) {
    lc_error e = {0};
    durable_gate x = {0}, y = {0};
    REQUIRE(!pthread_mutex_init(&x.mutex, NULL) &&
            !pthread_cond_init(&x.cond, NULL));
    REQUIRE(!pthread_mutex_init(&y.mutex, NULL) &&
            !pthread_cond_init(&y.cond, NULL));
    ps_io ai = {&x, read_io, durable_write, durable_sync, reserve_io, NULL};
    ps_io bi = {&y, read_io, durable_write, durable_sync, reserve_io, NULL};
    ps_coordination_io coordinator = {&y, durable_attempt};
    ps_options ao = {0}, bo = {0};
    ao.io = &ai;
    ao.coordination_io = &coordinator;
    bo.io = &bi;
    ps_space *a = NULL, *b = NULL;
    ps_owned_head *head = NULL;
    ps_owned_run *run = NULL;
    ps_owned_status info;
    ps_key stream = {88, 1, PS_STREAM};
    int found;
    ok(ps_open(path, 1, &bo, &b, &e), &e);
    ok(ps_owned_open(b, NULL, stream, 1, NULL, &head, &info, &e), &e);
    ok(ps_owned_grant(head, NULL, 0, PS_PAGE, &run, &found, &e), &e);
    REQUIRE(found);
    ok(ps_checkpoint(b, &e), &e);
    ok(ps_open(path, 0, &ao, &a, &e), &e);
    unsigned char byte = 0x41;
    ok(ps_owned_write(run, 0, &byte, 1, &e), &e);
    ok(ps_owned_commit(head, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
    capture_put(a, 1, 65);
    uint64_t wire = readiness(a).ready;
    ps_stats before_a = status(a), before_b = status(b);
    x.calls = y.calls = 0;
    x.paused = late;
    y.paused = 1;
    y.attempted = 0;
    pthread_t first, second;
    durable_work first_work = {b, &y}, second_work = {a, &x};
    REQUIRE(!pthread_create(&first, NULL, durable_flush, &first_work));
    pthread_mutex_lock(&y.mutex);
    while (!y.entered && !y.finished)
      pthread_cond_wait(&y.cond, &y.mutex);
    REQUIRE(y.entered);
    pthread_mutex_unlock(&y.mutex);
    REQUIRE(!pthread_create(&second, NULL, durable_flush, &second_work));
    pthread_mutex_lock(&y.mutex);
    while (!y.attempted)
      pthread_cond_wait(&y.cond, &y.mutex);
    pthread_mutex_unlock(&y.mutex);
    ps_coordination_guard *independent = NULL;
    ok(ps_coordination_lock(a->coordination, PS_COORDINATION_ALLOCATION, 0, 0,
                            &independent, &e),
       &e);
    ps_coordination_unlock(independent);
    /* Sync leadership waits outside local and global allocation authority. */
    if (late)
      capture_put(a, 2, 65);
    ps_readiness_certificate during = readiness(a);
    REQUIRE(during.durable < wire && during.ready == wire + (uint64_t)late);
    pthread_mutex_lock(&y.mutex);
    y.released = 1;
    pthread_cond_broadcast(&y.cond);
    pthread_mutex_unlock(&y.mutex);
    REQUIRE(!pthread_join(first, NULL));
    if (late) {
      pthread_mutex_lock(&x.mutex);
      while (!x.entered && !x.finished)
        pthread_cond_wait(&x.cond, &x.mutex);
      REQUIRE(x.entered);
      pthread_mutex_unlock(&x.mutex);
      during = readiness(a);
      REQUIRE(during.durable == wire && during.ready == wire + 1);
      uint64_t selected, durable;
      ok(ps_publication_prefix(a, &selected, &durable, &e), &e);
      REQUIRE(durable < selected);
      pthread_mutex_lock(&x.mutex);
      x.released = 1;
      pthread_cond_broadcast(&x.cond);
      pthread_mutex_unlock(&x.mutex);
    }
    REQUIRE(!pthread_join(second, NULL));
    REQUIRE(x.calls == (size_t)late && y.calls == 1);
    REQUIRE(status(a).syncs == before_a.syncs + (uint64_t)late &&
            status(b).syncs == before_b.syncs + 1);
    REQUIRE(readiness(a).durable == readiness(a).ready);
    ps_owned_run_close(run);
    ps_owned_close(head);
    ok(ps_close(a, &e), &e);
    ok(ps_close(b, &e), &e);
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    ps_reader *reader = NULL;
    ok(ps_read_open(a, key(1), NULL, &reader, &e), &e);
    captured_expect(reader, 1, 65);
    ps_reader_close(reader);
    ok(ps_close(a, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
    REQUIRE(!pthread_cond_destroy(&x.cond) && !pthread_mutex_destroy(&x.mutex));
    REQUIRE(!pthread_cond_destroy(&y.cond) && !pthread_mutex_destroy(&y.mutex));
  }
}
static void readiness_cohort(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL, *c = NULL;
  mapping_fail = 1;
  REQUIRE(ps_open(path, 1, NULL, &a, &e) == LC_ERR_TRANSPORT && !a);
  mapping_fail = 0;
  lc_error_cleanup(&e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, 65);
  ps_readiness_certificate certificate = readiness(a);
  REQUIRE(certificate.ready == a->sequence &&
          certificate.durable < certificate.ready);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  uint64_t syncs = status(a).syncs;
  ok(ps_flush(a, &e), &e);
  REQUIRE(status(a).syncs == syncs);
  certificate = readiness(a);
  REQUIRE(certificate.ready == certificate.durable);
  ps_reader *source = NULL, *imported = NULL;
  ok(ps_read_open(a, key(1), NULL, &source, &e), &e);
  size_t ticket_bytes = ps_reader_ticket_bytes(source);
  void *ticket = malloc(ticket_bytes);
  REQUIRE(ticket);
  ok(ps_reader_ticket(source, ticket, ticket_bytes, &e), &e);
  ok(ps_reader_import(path, ticket, ticket_bytes, &imported, &e), &e);
  ps_reader_close(source);
  free(ticket);
  uint64_t records[2 * PS_READINESS_WORDS], zero = 0;
  REQUIRE(pread(a->fd, records, sizeof(records), PS_READINESS_OFFSET) ==
          sizeof(records));
  size_t selected =
      records[0] > records[PS_READINESS_WORDS] ? 0 : PS_READINESS_WORDS;
  size_t older = PS_READINESS_WORDS - selected;
  /* Interrupted overwrite of the older slot leaves the selected record. */
  REQUIRE(pwrite(a->fd, &zero, sizeof(zero),
                 PS_READINESS_OFFSET + older * sizeof(zero)) == sizeof(zero));
  REQUIRE(readiness(a).ready == certificate.ready);
  /* A committed slot with a bad checksum cannot fall back to an older slot. */
  REQUIRE(pwrite(a->fd, records + older, PS_READINESS_WORDS * sizeof(zero),
                 PS_READINESS_OFFSET + older * sizeof(zero)) ==
          PS_READINESS_WORDS * sizeof(zero));
  size_t crc = selected + PS_READINESS_WORDS - 1;
  uint64_t damaged = records[crc] ^ 1;
  REQUIRE(pwrite(a->fd, &damaged, sizeof(damaged),
                 PS_READINESS_OFFSET + crc * sizeof(damaged)) ==
          sizeof(damaged));
  REQUIRE(ps_refresh(a, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  REQUIRE(ps_open(path, 0, NULL, &c, &e) == LC_ERR_TRANSPORT && !c);
  lc_error_cleanup(&e);
  /* A checksum-valid record from the previous live protocol also fences
   * admission. The disk format can still reopen after the cohort drains. */
  records[selected + 1] = UINT64_C(0x3159444145525350);
  records[crc] = crc32(0, (const unsigned char *)(records + selected),
                       (uInt)((PS_READINESS_WORDS - 1) * sizeof(zero)));
  REQUIRE(pwrite(b->fd, records + selected, PS_READINESS_WORDS * sizeof(zero),
                 PS_READINESS_OFFSET + selected * sizeof(zero)) ==
          PS_READINESS_WORDS * sizeof(zero));
  ok(ps_close(a, &e), &e);
  REQUIRE(ps_open(path, 0, NULL, &c, &e) == LC_ERR_TRANSPORT && !c);
  lc_error_cleanup(&e);
  ok(ps_close(b, &e), &e);
  /* Imported readers hold physical presence, not canonical membership. */
  ok(ps_open(path, 0, NULL, &c, &e), &e);
  certificate = readiness(c);
  REQUIRE(certificate.ready == certificate.durable);
  ps_reader *restored = NULL;
  ok(ps_read_open(c, key(1), NULL, &restored, &e), &e);
  captured_expect(restored, 1, 65);
  ps_reader_close(restored);
  captured_expect(imported, 1, 65);
  ps_reader_close(imported);
  ok(ps_close(c, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct readiness_fault {
  int kind, reason, armed;
  size_t calls, nth;
} readiness_fault;
static int readiness_lock(void *context, int fd, int command,
                          struct flock *lock) {
  readiness_fault *f = context;
  int match = f->kind == 1   ? command == F_OFD_SETLKW && lock->l_start == 5
              : f->kind == 2 ? command == F_OFD_SETLK && lock->l_start == 5 &&
                                   lock->l_type == F_UNLCK
              : f->kind == 3 ? command == F_OFD_SETLKW && lock->l_start == 4
              : f->kind == 4 ? command == F_OFD_SETLK && lock->l_start == 4 &&
                                   lock->l_type == F_UNLCK
                             : command == F_OFD_SETLK && lock->l_start == 3 &&
                                   lock->l_type == F_RDLCK;
  if (f->armed && match && ++f->calls == f->nth) {
    errno = f->reason;
    return -1;
  }
  return fcntl(fd, command, lock);
}
static int readiness_sync_fault(void *context, int fd) {
  readiness_fault *f = context;
  if (f->kind == 6 && f->armed && ++f->calls == f->nth) {
    errno = f->reason;
    return -1;
  }
  return fdatasync(fd);
}
static void readiness_faults(void) {
  for (int kind = 1; kind <= 6; ++kind)
    for (int reason = 0; reason < 3; ++reason)
      for (size_t nth = 1; nth <= (kind == 3 || kind == 4 ? 2U : 1U); ++nth) {
        lc_error e = {0};
        readiness_fault f = {kind,
                             reason == 0   ? EINTR
                             : reason == 1 ? EAGAIN
                                           : EIO,
                             kind == 5, 0, nth};
        ps_coordination_io io = {&f, readiness_lock};
        ps_io disk = {&f,         read_io, durable_write, readiness_sync_fault,
                      reserve_io, NULL};
        ps_options options = {0};
        options.coordination_io = &io;
        options.io = &disk;
        ps_space *s = NULL;
        int rc = ps_open(path, 1, &options, &s, &e);
        if (kind == 5 && reason) {
          REQUIRE(rc == (reason == 1 ? LC_ERR_PENDING : LC_ERR_TRANSPORT) &&
                  !s);
          lc_error_cleanup(&e);
          REQUIRE(!coverage(NULL, 0) && !unlink(path));
          continue;
        }
        ok(rc, &e);
        f.armed = 0;
        ps_space *observer = NULL;
        if (kind == 6)
          ok(ps_open(path, 0, NULL, &observer, &e), &e);
        capture_put(s, 1, 65);
        uint64_t stable = kind == 6 ? readiness(observer).durable : 0;
        uint64_t selected, durable;
        ok(ps_publication_prefix(s, &selected, &durable, &e), &e);
        REQUIRE(durable < selected);
        f.calls = 0;
        f.armed = kind != 5;
        rc = ps_flush(s, &e);
        f.armed = 0;
        if (!reason || kind == 5)
          ok(rc, &e);
        else {
          REQUIRE(rc == ((kind == 1 || kind == 3) && reason == 1
                             ? LC_ERR_PENDING
                             : LC_ERR_TRANSPORT));
          lc_error_cleanup(&e);
          uint64_t after, acknowledged;
          ok(ps_status(s, &(ps_stats){0}, &e), &e);
          if (kind == 2 || kind == 4 || kind == 6) {
            REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
            lc_error_cleanup(&e);
            if (kind == 6)
              REQUIRE(readiness(observer).durable == stable);
          } else {
            ok(ps_publication_prefix(s, &after, &acknowledged, &e), &e);
            REQUIRE(after == selected && acknowledged == durable);
            ok(ps_flush(s, &e), &e);
          }
        }
        ok(ps_close(s, &e), &e);
        ok(ps_close(observer, &e), &e);
        REQUIRE(!coverage(NULL, 0) && !unlink(path));
      }
}
static ps_writer *replay_value(ps_space *s, ps_key name, int byte, size_t n,
                               lc_error *e) {
  ps_writer *w = NULL;
  unsigned char body[PS_CHUNK];
  REQUIRE(n <= sizeof(body));
  memset(body, byte, n);
  ok(ps_begin(s, NULL, name, &w, e), e);
  ok(ps_write(w, body, n, e), e);
  return w;
}
static void expect_value(ps_space *s, ps_key name, size_t bytes, int byte) {
  lc_error e = {0};
  ps_reader *r = NULL;
  int rc = ps_read_open(s, name, NULL, &r, &e);
  if (!bytes) {
    REQUIRE(rc == LC_ERR_INVALID && !r);
    lc_error_cleanup(&e);
    return;
  }
  ok(rc, &e);
  captured_expect(r, (unsigned char)byte, bytes);
  ps_reader_close(r);
}
static void certified_prefix_reads(void) {
  lc_error e = {0};
  projection_io disk = {0};
  ps_io io = {&disk,           projection_read, projection_write,
              projection_sync, reserve_io,      NULL};
  ps_options observer = {0}, publisher = {0};
  ps_space *a = NULL, *b = NULL;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  uint64_t prefix;
  observer.io = &io;
  publisher.durable_sync = 1;
  ok(ps_open(path, 1, &observer, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_open(path, 0, &publisher, &b, &e), &e);
  ok(ps_refresh(a, &e), &e);
  REQUIRE(!readiness(a).pending);

  disk.armed = 1;
  expect_value(a, key(1), 65, 1);
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(!disk.root_reads && !disk.journal_reads);

  ps_writer *w = replay_value(b, key(1), 42, 65, &e);
  ok(ps_revision_publish(&w, 1, 0, PS_COMMIT_DEFER_SYNC, NULL, &prefix, &e),
     &e);
  ps_writer_close(w);
  expect_value(a, key(1), 65, 1);
  REQUIRE(!disk.root_reads && disk.journal_reads == 2);
  ok(ps_flush(b, &e), &e);
  expect_value(a, key(1), 65, 42);
  REQUIRE(!disk.root_reads && disk.journal_reads == 2);

  ok(ps_checkpoint(b, &e), &e);
  expect_value(a, key(1), 65, 42);
  REQUIRE(disk.root_reads == 2 && disk.journal_reads == 3);
  expect_value(a, key(1), 65, 42);
  REQUIRE(disk.root_reads == 2 && disk.journal_reads == 3);
  ok(ps_refresh(a, &e), &e);
  REQUIRE(disk.root_reads == 4 && disk.journal_reads == 4);
  /* A writer can exit just before issuing its first write. Confirm the old
   * end marker once, then return to reads using the live certificate. */
  ps_coordination_guard *guard = NULL;
  ok(ps_coordination_lock(b->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                          &guard, &e),
     &e);
  ps_readiness_begin(b, PS_READINESS_JOURNAL);
  ps_coordination_unlock(guard);
  expect_value(a, key(1), 65, 42);
  REQUIRE(!readiness(a).pending && disk.root_reads == 4 &&
          disk.journal_reads == 5);
  expect_value(a, key(1), 65, 42);
  REQUIRE(disk.root_reads == 4 && disk.journal_reads == 5);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void certified_prefix_child(int notify, int proceed) {
  lc_error e = {0};
  projection_io disk = {0};
  ps_io io = {&disk,           projection_read, projection_write,
              projection_sync, reserve_io,      NULL};
  ps_options options = {0};
  ps_space *s = NULL;
  char message;
  options.io = &io;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  REQUIRE(write(notify, "O", 1) == 1);
  REQUIRE(read(proceed, &message, 1) == 1 && message == 'W');
  disk.ready = notify;
  disk.pause_journal = 1;
  capture_put(s, 2, 65);
  REQUIRE(0);
}
static void certified_prefix_interruption(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_reader *held = NULL, *reader = NULL;
  int notify[2], proceed[2], exited;
  char message;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_read_open(a, key(1), NULL, &held, &e), &e);
  REQUIRE(pipe(notify) == 0 && pipe(proceed) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char output[16], input[16];
    REQUIRE(!close(notify[0]) && !close(proceed[1]));
    snprintf(output, sizeof(output), "%d", notify[1]);
    snprintf(input, sizeof(input), "%d", proceed[0]);
    execl(executable, executable, "--prefix-child", path, output, input,
          (char *)NULL);
    _exit(127);
  }
  REQUIRE(!close(notify[1]) && !close(proceed[0]));
  REQUIRE(read(notify[0], &message, 1) == 1 && message == 'O');
  expect_value(a, key(1), 65, 1);
  ps_readiness_certificate before = readiness(a);
  REQUIRE(!before.pending && before.ready == a->sequence);
  REQUIRE(write(proceed[1], "W", 1) == 1);
  REQUIRE(read(notify[0], &message, 1) == 1 && message == 'J');
  REQUIRE(!kill(child, SIGKILL) && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(!close(notify[0]) && !close(proceed[1]));
  ps_readiness_certificate after = readiness(a);
  REQUIRE(after.ready == before.ready && after.root == before.root &&
          after.pending == PS_READINESS_JOURNAL);
  REQUIRE(ps_read_open(a, key(1), NULL, &reader, &e) == LC_ERR_PENDING &&
          !reader);
  lc_error_cleanup(&e);
  captured_expect(held, 1, 65);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  REQUIRE(!readiness(a).pending);
  expect_value(a, key(1), 65, 1);
  expect_value(a, key(2), 0, 0);
  capture_put(a, 2, 65);
  expect_value(b, key(2), 65, 2);
  ps_reader_close(held);
  ok(ps_close(a, &e), &e);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
enum { QUARANTINE_WITNESSES = 24 };
typedef struct quarantine_queries {
  uint64_t visited;
  size_t count;
} quarantine_queries;
static uint64_t quarantine_offset(size_t index) {
  return PS_DATA + (2 * index + 1) * PS_PAGE;
}
static int quarantine_query(void *context, int fd, int command,
                            struct flock *lock) {
  quarantine_queries *q = context;
  uint64_t offset = (uint64_t)lock->l_start;
  if (command == F_OFD_GETLK && lock->l_len == PS_PAGE &&
      offset >= quarantine_offset(0) &&
      offset <= quarantine_offset(QUARANTINE_WITNESSES)) {
    uint64_t relative = offset - quarantine_offset(0);
    if (!(relative % (2 * PS_PAGE))) {
      q->visited |= UINT64_C(1) << (relative / (2 * PS_PAGE));
      ++q->count;
    }
  }
  return fcntl(fd, command, lock);
}
static int quarantine_witness(size_t first, size_t last) {
  int fd = open(path, O_RDWR | O_CLOEXEC);
  struct flock lock = {0};
  REQUIRE(fd >= 0);
  lock.l_type = F_RDLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = 2; /* Physical ownership presence byte. */
  lock.l_len = 1;
  REQUIRE(fcntl(fd, F_OFD_SETLK, &lock) == 0);
  lock.l_len = PS_PAGE;
  for (size_t index = first; index < last; ++index) {
    lock.l_start = (off_t)quarantine_offset(index);
    REQUIRE(fcntl(fd, F_OFD_SETLK, &lock) == 0);
  }
  return fd;
}
static void quarantine_capacity(void) {
  lc_error e = {0};
  quarantine_queries q = {0};
  ps_coordination_io io = {&q, quarantine_query};
  ps_options options = {0};
  ps_space *s = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {0};
  int ready[2], lifetime[2], exited;
  char byte;
  options.coordination_io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  REQUIRE(!pipe(ready) && !pipe(lifetime));
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    REQUIRE(!close(ready[0]) && !close(lifetime[1]) && !close(s->fd));
    (void)quarantine_witness(1, QUARANTINE_WITNESSES + 1);
    REQUIRE(write(ready[1], "Q", 1) == 1);
    REQUIRE(read(lifetime[0], &byte, 1) == 0);
    _exit(0);
  }
  REQUIRE(!close(ready[1]) && !close(lifetime[0]));
  int survivor = quarantine_witness(0, 1);
  REQUIRE(read(ready[0], &byte, 1) == 1 && byte == 'Q' && !close(ready[0]));
  ok(ps_budget_foreground_check(s, &e), &e);
  uint64_t held = (QUARANTINE_WITNESSES + 1) * PS_PAGE;
  REQUIRE(status(s).retired_bytes == held);
  q.visited = 0;
  for (int sample = 0; sample <= QUARANTINE_WITNESSES / 2; ++sample) {
    q.count = 0;
    ok(ps_budget_foreground_check(s, &e), &e);
    REQUIRE(q.count && q.count <= 2);
  }
  REQUIRE(q.visited == (UINT64_C(1) << (QUARANTINE_WITNESSES + 1)) - 1);
  REQUIRE(status(s).retired_bytes == held);
  plan.payload_bytes = s->free_bytes + QUARANTINE_WITNESSES * PS_PAGE;
  plan.maintenance = 1;
  REQUIRE(ps_budget_open(s, &plan, &budget, &e) == LC_ERR_INVALID && !budget);
  lc_error_cleanup(&e);
  REQUIRE(!kill(child, SIGKILL) && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(!close(lifetime[1]));
  ok(ps_budget_foreground_check(s, &e), &e);
  REQUIRE(status(s).retired_bytes >= held - 2 * PS_PAGE);
  ok(ps_budget_open(s, &plan, &budget, &e), &e);
  REQUIRE(status(s).claimed_payload_bytes == plan.payload_bytes);
  REQUIRE(status(s).retired_bytes == PS_PAGE);
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(!close(survivor));
  ok(ps_checkpoint(s, &e), &e);
  REQUIRE(!status(s).retired_bytes);
  ok(ps_close(s, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void quarantine_packed(void) {
  enum { PAGES = 12, BODY = PS_PAGE / 2 + PS_CELL };
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL, *holder = NULL;
  ps_reader *held[PAGES];
  ps_budget *budget = NULL;
  ps_budget_plan plan = {0, 256, 1, 0};
  ps_writer *writer = NULL;
  unsigned char bytes[65] = {7};
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ok(ps_open(path, 0, NULL, &holder, &e), &e);
  for (size_t i = 0; i < PAGES; ++i) {
    capture_put(a, i + 1, BODY);
    ok(ps_read_open(holder, key(i + 1), NULL, &held[i], &e), &e);
  }
  REQUIRE(status(a).packed_pages == PAGES);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_budget_open(b, &plan, &budget, &e), &e);
  ps_payload_page *last = b->pinned_pages;
  REQUIRE(last && b->pinned_page_probe && *b->pinned_page_probe);
  while (last->pinned_next)
    last = last->pinned_next;
  uint64_t offset = last->run.offset;
  size_t release = 0;
  while (release < PAGES) {
    ps_entry *entry = ps_internal_lookup(b, key(release + 1));
    REQUIRE(entry && entry->count == 1);
    if (entry->extents[0].offset / PS_PAGE * PS_PAGE == offset)
      break;
    ++release;
  }
  REQUIRE(release < PAGES);
  ps_reader_close(held[release]);
  ok(ps_begin(b, budget, key(PAGES + 1), &writer, &e), &e);
  ok(ps_write(writer, bytes, sizeof(bytes), &e), &e);
  ok(ps_publish(writer, NULL, &e), &e);
  ps_writer_close(writer);
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(status(b).packed_pages == PAGES);
  ps_entry *published = ps_internal_lookup(b, key(PAGES + 1));
  REQUIRE(published && published->count == 1 &&
          published->extents[0].offset / PS_PAGE * PS_PAGE == offset);
  ps_reader *reader = NULL;
  unsigned char observed[sizeof(bytes)];
  ok(ps_read_open(b, key(PAGES + 1), NULL, &reader, &e), &e);
  ok(ps_read_at(reader, 0, observed, sizeof(observed), &e), &e);
  REQUIRE(!memcmp(bytes, observed, sizeof(bytes)));
  ps_reader_close(reader);
  for (size_t i = 0; i < PAGES; ++i)
    if (i != release) {
      captured_expect(held[i], (unsigned char)(i + 1), BODY);
      ps_reader_close(held[i]);
    }
  ok(ps_close(b, &e), &e);
  ok(ps_close(holder, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void journal_replay(void) {
  lc_error e = {0};
  projection_io m = {0};
  ps_io io = {0};
  ps_options observer = {0}, publisher = {0};
  ps_space *a = NULL, *b = NULL;
  ps_reader *held = NULL;
  ps_writer *writers[3];
  ps_ref reference[3];
  uint64_t prefix;
  io.context = &m;
  io.read = projection_read;
  io.write = projection_write;
  io.reserve = reserve_io;
  io.sync = projection_sync;
  observer.io = &io;
  publisher.durable_sync = 1;
  ok(ps_open(path, 1, &observer, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_read_open(a, key(1), NULL, &held, &e), &e);
  ok(ps_open(path, 0, &publisher, &b, &e), &e);
  writers[0] = replay_value(b, key(1), 23, PS_CHUNK, &e);
  writers[1] = replay_value(b, key(2), 24, 63, &e);
  writers[2] = replay_value(b, key(3), 25, 64, &e);
  /* Quarantine the foreign writer's full run before the group owns it. */
  ps_budget *watch = NULL;
  ps_budget_plan plan = {PS_PAGE, 0, 0, 0};
  ok(ps_budget_open(a, &plan, &watch, &e), &e);
  REQUIRE(a->pinned_free_bytes >= PS_CHUNK);
  ok(ps_revision_publish(writers, 3, 0, PS_COMMIT_DEFER_SYNC, reference,
                         &prefix, &e),
     &e);
  for (size_t i = 0; i < 3; ++i)
    ps_writer_close(writers[i]);
  ps_entry *published = ps_internal_lookup(b, key(1));
  REQUIRE(published && published->count == 1);
  m.ranges = 1;
  m.forbidden[0].first = published->extents[0].offset;
  m.forbidden[0].last = m.forbidden[0].first + PS_CHUNK;
  m.armed = 1;
  uint64_t generation = a->generation;
  ok(ps_refresh(a, &e), &e);
  REQUIRE(a->generation == generation && a->sequence == b->sequence);
  REQUIRE(!m.syncs && !m.payload_reads);
  m.armed = 0;
  expect_value(a, key(1), 65, 1);
  expect_value(a, key(2), 0, 0);
  expect_value(a, key(3), 0, 0);
  ok(ps_flush(b, &e), &e);
  expect_value(a, key(1), PS_CHUNK, 23);
  expect_value(a, key(2), 63, 24);
  expect_value(a, key(3), 64, 25);
  captured_expect(held, 1, 65);
  ok(ps_value_remove(a, key(2), &reference[1], &e), &e);
  expect_value(a, key(2), 0, 0);
  expect_value(b, key(2), 63, 24);
  ok(ps_flush(a, &e), &e);
  expect_value(b, key(2), 0, 0);
  capture_put(b, 4, 65);
  expect_value(a, key(4), 65, 4);
  writers[0] = replay_value(b, key(5), 26, 65, &e);
  ok(ps_revision_publish(writers, 1, 0, PS_COMMIT_DEFER_SYNC, NULL, &prefix,
                         &e),
     &e);
  ps_writer_close(writers[0]);
  expect_value(a, key(5), 0, 0);
  ok(ps_budget_close(watch, &e), &e);
  ps_reader_close(held);
  ok(ps_checkpoint(a, &e), &e);
  int present;
  uint64_t bytes;
  ok(ps_value_info(a, key(5), &reference[0], &bytes, &present, &e), &e);
  REQUIRE(present && bytes == 65);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  expect_value(a, key(1), PS_CHUNK, 23);
  expect_value(a, key(2), 0, 0);
  expect_value(a, key(4), 65, 4);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void projection_tail(void) {
  lc_error e = {0};
  projection_io m = {0};
  ps_io io = {0};
  ps_options observer = {0}, publisher = {0};
  ps_space *a = NULL, *b = NULL;
  ps_reader *old = NULL, *prior = NULL;
  ps_writer *writers[3], *producer = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {PS_CHUNK * 2, 4096, 32, 0};
  unsigned char residue[64], zero[64] = {0};
  uint64_t prefix;
  io.context = &m;
  io.read = projection_read;
  io.write = projection_write;
  io.reserve = reserve_io;
  io.sync = projection_sync;
  observer.io = &io;
  publisher.durable_sync = 1;
  ok(ps_open(path, 1, &observer, &a, &e), &e);
  capture_put(a, 1, 65);
  capture_put(a, 2, 65);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_read_open(a, key(1), NULL, &old, &e), &e);
  ps_entry *original = ps_internal_lookup(a, key(1));
  ok(ps_open(path, 0, &publisher, &b, &e), &e);
  REQUIRE(b->journal == sizeof(residue));
  REQUIRE(pread(b->fd, residue, sizeof(residue), PS_JOURNAL_OFFSET) ==
          sizeof(residue));
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  producer = private_value(a, budget, 9000);
  capture_put(b, 6, 65);
  ok(ps_checkpoint(b, &e), &e);
  ok(ps_delete(b, key(2), &e), &e);
  writers[0] = replay_value(b, key(1), 31, PS_CHUNK, &e);
  writers[1] = replay_value(b, key(3), 32, 65, &e);
  writers[2] = replay_value(b, key(4), 33, 64, &e);
  ok(ps_revision_publish(writers, 3, 0, PS_COMMIT_DEFER_SYNC, NULL, &prefix,
                         &e),
     &e);
  for (size_t i = 0; i < 3; ++i)
    ps_writer_close(writers[i]);
  ps_entry *published = ps_internal_lookup(b, key(1));
  REQUIRE(published && published->count == 1);
  m.ranges = 1;
  m.forbidden[0].first = published->extents[0].offset;
  m.forbidden[0].last = m.forbidden[0].first + PS_CHUNK;
  /* The first tail operation is valid. Damage the following atomic group
   * and verify even the valid deletion stays outside our live projection. */
  unsigned char last, damaged;
  off_t end = (off_t)(PS_JOURNAL_OFFSET + b->journal - 1);
  REQUIRE(pread(b->fd, &last, 1, end) == 1);
  damaged = last ^ 1;
  REQUIRE(pwrite(b->fd, &damaged, 1, end) == 1);
  uint64_t generation = a->generation, sequence = a->sequence;
  uint64_t physical = coverage(NULL, 0);
  size_t journal = a->journal;
  REQUIRE(ps_refresh(a, &e) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&e);
  REQUIRE(a->generation == generation && a->sequence == sequence &&
          a->journal == journal && coverage(NULL, 0) == physical);
  REQUIRE(ps_internal_lookup(a, key(1)) == original &&
          !ps_internal_lookup(a, key(2))->deleted);
  REQUIRE(pwrite(b->fd, &last, 1, end) == 1);
  /* A valid previous-generation grant can only appear at the journal start.
   * It must never be accepted as the end of an adopted tail. */
  REQUIRE(pwrite(b->fd, residue, sizeof(residue), end + 1) == sizeof(residue));
  REQUIRE(ps_refresh(a, &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  REQUIRE(a->generation == generation && a->sequence == sequence &&
          a->journal == journal && coverage(NULL, 0) == physical);
  REQUIRE(pwrite(b->fd, zero, sizeof(zero), end + 1) == sizeof(zero));
  m.armed = 1;
  ok(ps_refresh(a, &e), &e);
  m.armed = 0;
  REQUIRE(a->generation > generation && a->generation == b->generation &&
          a->sequence == b->sequence && a->journal == b->journal);
  REQUIRE(!m.syncs && !m.payload_reads);
  REQUIRE(ps_internal_lookup(a, key(1))->previous == original);
  ok(ps_read_open(a, key(1), NULL, &prior, &e), &e);
  expect_value(a, key(2), 0, 0);
  expect_value(a, key(3), 0, 0);
  expect_value(a, key(4), 0, 0);
  expect_value(a, key(6), 65, 6);
  ok(ps_flush(b, &e), &e);
  expect_value(a, key(1), PS_CHUNK, 31);
  expect_value(a, key(3), 65, 32);
  expect_value(a, key(4), 64, 33);
  captured_expect(old, 1, 65);
  ok(ps_publish(producer, NULL, &e), &e);
  ps_writer_close(producer);
  ok(ps_flush(a, &e), &e);
  expect_value(b, key(9000), PS_CHUNK, 9000 % 251);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  captured_expect(prior, 1, 65);
  ps_reader_close(prior);
  ps_reader_close(old);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  expect_value(a, key(1), PS_CHUNK, 31);
  expect_value(a, key(2), 0, 0);
  expect_value(a, key(4), 64, 33);
  expect_value(a, key(9000), PS_CHUNK, 9000 % 251);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void journal_replay_faults(void) {
  size_t sites = 0;
  for (size_t failure = 0; failure <= sites; ++failure) {
    lc_error e = {0};
    ps_space *a = NULL, *b = NULL;
    ps_writer *writers[4];
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    ok(ps_refresh(a, &e), &e);
    capture_put(b, 21, 65);
    ok(ps_refresh(a, &e), &e);
    writers[0] = replay_value(b, key(1), 23, 65, &e);
    writers[1] = replay_value(b, key(2), 24, PS_CHUNK, &e);
    writers[2] = replay_value(b, key(3), 25, 66, &e);
    writers[3] = replay_value(b, key(4), 26, PS_CHUNK, &e);
    ok(ps_revision_publish(writers, 4, 0, PS_COMMIT_DEFAULT, NULL, NULL, &e),
       &e);
    for (size_t i = 0; i < 4; ++i)
      ps_writer_close(writers[i]);
    uint64_t latest = a->sequence, free_before = a->free_bytes;
    uint64_t packed = a->packed_bytes, pages = a->packed_pages;
    size_t cursor = a->journal;
    if (!failure) {
      unsigned char original, damaged;
      off_t last = (off_t)(PS_JOURNAL_OFFSET + b->journal - 1);
      REQUIRE(pread(b->fd, &original, 1, last) == 1);
      damaged = original ^ 1;
      REQUIRE(pwrite(b->fd, &damaged, 1, last) == 1);
      REQUIRE(ps_refresh(a, &e) == LC_ERR_PROTOCOL);
      lc_error_cleanup(&e);
      REQUIRE(a->sequence == latest && a->journal == cursor &&
              a->free_bytes == free_before && a->packed_bytes == packed);
      for (size_t i = 1; i <= 4; ++i)
        REQUIRE(!ps_internal_lookup(a, key(i)));
      REQUIRE(pwrite(b->fd, &original, 1, last) == 1);
    }
    allocations = 0;
    allocation_fail = failure;
    measuring = 1;
    int rc = ps_refresh(a, &e);
    measuring = 0;
    if (!failure) {
      ok(rc, &e);
      sites = allocations;
      REQUIRE(sites > 8);
    } else {
      REQUIRE(rc == LC_ERR_NOMEM && a->sequence == latest &&
              a->journal == cursor && a->free_bytes == free_before &&
              a->packed_bytes == packed && a->packed_pages == pages);
      lc_error_cleanup(&e);
      for (size_t i = 1; i <= 4; ++i)
        REQUIRE(!ps_internal_lookup(a, key(i)));
      ok(ps_refresh(a, &e), &e);
    }
    expect_value(a, key(1), 65, 23);
    expect_value(a, key(2), PS_CHUNK, 24);
    expect_value(a, key(3), 66, 25);
    expect_value(a, key(4), PS_CHUNK, 26);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_close(a, &e), &e);
    ok(ps_close(b, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
  fprintf(stderr, "journal replay allocated at %zu fault sites\n", sites);
}
static ps_key tail_stream(void) {
  ps_key result = {88, 110, PS_STREAM};
  return result;
}
static void expect_tail(ps_owned_head *head, uint64_t bytes, int sealed) {
  lc_error e = {0};
  ps_owned_status info;
  ok(ps_owned_status_get(head, &info, &e), &e);
  REQUIRE(info.tail == bytes && info.sealed == sealed && !info.retired);
}
static void journal_replay_child(int notify, int proceed) {
  lc_error e = {0};
  ps_space *b = NULL;
  ps_writer *writers[2];
  ps_owned_head *head = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status info;
  char message;
  int found;
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_owned_open(b, NULL, tail_stream(), 0, NULL, &head, &info, &e), &e);
  ok(ps_owned_grant(head, NULL, 0, 0, &run, &found, &e), &e);
  REQUIRE(found);
  ok(ps_owned_write(run, 0, "x", 1, &e), &e);
  REQUIRE(write(notify, "W", 1) == 1 && read(proceed, &message, 1) == 1);
  REQUIRE(message == 'C');
  ok(ps_owned_commit(head, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_owned_run_close(run);
  ps_owned_close(head);
  writers[0] = replay_value(b, key(1), 31, PS_CHUNK, &e);
  writers[1] = replay_value(b, key(2), 32, 65, &e);
  ok(ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_writer_close(writers[0]);
  ps_writer_close(writers[1]);
  REQUIRE(write(notify, "R", 1) == 1 && read(proceed, &message, 1) == 1);
  REQUIRE(message == 'P');
  expect_value(b, key(3), 65, 3);
  ok(ps_close(b, &e), &e);
  REQUIRE(close(notify) == 0 && close(proceed) == 0);
}
static void journal_replay_process(void) {
  lc_error e = {0};
  ps_space *a = NULL;
  for (int killed = 0; killed < 2; ++killed) {
    int notify[2], proceed[2], exited;
    ps_owned_head *observer = NULL;
    ps_owned_run *run = NULL;
    ps_owned_status info;
    int found;
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    ok(ps_owned_open(a, NULL, tail_stream(), 1, NULL, &observer, &info, &e),
       &e);
    ok(ps_owned_grant(observer, NULL, 0, PS_PAGE, &run, &found, &e), &e);
    REQUIRE(found);
    ok(ps_checkpoint(a, &e), &e);
    REQUIRE(pipe(notify) == 0 && pipe(proceed) == 0);
    pid_t child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      char output[16], input[16];
      REQUIRE(!close(notify[0]) && !close(proceed[1]));
      snprintf(output, sizeof(output), "%d", notify[1]);
      snprintf(input, sizeof(input), "%d", proceed[0]);
      execl(executable, executable, "--journal-child", path, output, input,
            (char *)NULL);
      _exit(127);
    }
    REQUIRE(!close(notify[1]) && !close(proceed[0]));
    char message;
    REQUIRE(read(notify[0], &message, 1) == 1 && message == 'W');
    ok(ps_refresh(a, &e), &e);
    expect_tail(observer, 0, 0);
    REQUIRE(write(proceed[1], "C", 1) == 1);
    REQUIRE(read(notify[0], &message, 1) == 1 && message == 'R');
    if (killed)
      REQUIRE(kill(child, SIGKILL) == 0);
    uint64_t generation = a->generation;
    expect_value(a, key(1), PS_CHUNK, 31);
    expect_value(a, key(2), 65, 32);
    expect_tail(observer, 1, 0);
    ok(ps_owned_read(run, 0, &message, 1, &e), &e);
    REQUIRE(message == 'x');
    capture_put(a, 3, 65);
    REQUIRE(a->generation == generation);
    if (!killed)
      REQUIRE(write(proceed[1], "P", 1) == 1);
    REQUIRE(waitpid(child, &exited, 0) == child);
    REQUIRE(killed ? WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL
                   : WIFEXITED(exited) && WEXITSTATUS(exited) == 0);
    REQUIRE(!close(notify[0]) && !close(proceed[1]));
    ps_owned_run_close(run);
    ps_owned_close(observer);
    ok(ps_flush(a, &e), &e);
    ok(ps_close(a, &e), &e);
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    expect_value(a, key(1), PS_CHUNK, 31);
    expect_value(a, key(2), 65, 32);
    expect_value(a, key(3), 65, 3);
    ok(ps_close(a, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
}
static void replay_structural(int rollover) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_key stream = {88, 101, PS_STREAM};
  ps_owned_head *head = NULL, *observed = NULL;
  ps_owned_status info, peer;
  ps_owned_run *original = NULL, *retained = NULL, *mapped = NULL,
               *current = NULL;
  ps_catalog_info binding, captured;
  ps_catalog_view *view = NULL, *missing = NULL;
  unsigned char *bytes = malloc(PS_CHUNK);
  int found;
  REQUIRE(bytes);
  memset(bytes, 0x51, PS_CHUNK);
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_owned_open(b, NULL, stream, 1, NULL, &head, &info, &e), &e);
  ok(ps_refresh(a, &e), &e);
  ok(ps_owned_inspect(a, stream, &info.ref, &peer, &e), &e);
  REQUIRE(!peer.sealed && !peer.tail && !peer.runs);
  ok(ps_owned_grant(head, NULL, 0, PS_PAGE, &original, &found, &e), &e);
  REQUIRE(found);
  ok(ps_owned_write(original, 0, bytes, 1000, &e), &e);
  ps_piece_checksum crc;
  ok(ps_owned_piece_extend(original, 0, NULL, 1000, bytes, &crc, &e), &e);
  ok(ps_owned_commit(head, 1000, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ok(ps_owned_seal(head, &e), &e);
  ok(ps_catalog_put(b, NULL, 88, PS_CATALOG_BODY, "stream", 6, NULL, stream,
                    &info.ref, &binding, &e),
     &e);
  ok(ps_catalog_get(a, 88, PS_CATALOG_BODY, "stream", 6, &view, &e), &e);
  REQUIRE(view);
  ok(ps_owned_open(a, NULL, stream, 0, &info.ref, &observed, &peer, &e), &e);
  REQUIRE(peer.sealed && peer.tail == 1000 && peer.runs == 1);
  ok(ps_owned_grant(observed, NULL, 0, 0, &retained, &found, &e), &e);
  REQUIRE(found && !ps_owned_run_mapped(retained));
  ps_owned_range range = {100, 100};
  if (rollover)
    ok(ps_checkpoint(b, &e), &e);
  ok(ps_owned_run_repack(head, original, NULL, &range, 1, NULL, NULL, &mapped,
                         &e),
     &e);
  ok(ps_owned_grant(observed, NULL, 0, 0, &current, &found, &e), &e);
  REQUIRE(found && ps_owned_run_mapped(current));
  if (rollover)
    ok(ps_checkpoint(b, &e), &e);
  ok(ps_catalog_remove(b, 88, PS_CATALOG_BODY, "stream", 6, &binding.revision,
                       &e),
     &e);
  ok(ps_owned_retire(head, &e), &e);
  ok(ps_catalog_get(a, 88, PS_CATALOG_BODY, "stream", 6, &missing, &e), &e);
  REQUIRE(!missing);
  ok(ps_catalog_view_info(view, &captured, &e), &e);
  REQUIRE(captured.target.incarnation == info.ref.incarnation);
  ok(ps_owned_status_get(observed, &peer, &e), &e);
  REQUIRE(peer.retired);
  ok(ps_owned_read(retained, 0, bytes, 1000, &e), &e);
  for (size_t i = 0; i < 1000; ++i)
    REQUIRE(bytes[i] == 0x51);
  ok(ps_owned_read(current, 100, bytes, 100, &e), &e);
  for (size_t i = 0; i < 100; ++i)
    REQUIRE(bytes[i] == 0x51);
  ps_catalog_view_close(view);
  ps_owned_run_close(current);
  ps_owned_run_close(retained);
  ps_owned_run_close(mapped);
  ps_owned_run_close(original);
  ps_owned_close(observed);
  ps_owned_close(head);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_catalog_get(a, 88, PS_CATALOG_BODY, "stream", 6, &missing, &e), &e);
  REQUIRE(!missing);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  free(bytes);
}
static void *publish_catalog_namespace(void *context) {
  ps_catalog_info info;
  lc_error e = {0};
  ok(ps_catalog_create_namespace(context, "new", 3, &info, &e), &e);
  return NULL;
}
static int count_catalog_namespace(void *context, const void *name,
                                   size_t bytes, const ps_catalog_info *info,
                                   lc_error *e) {
  size_t *count = context;
  (void)e;
  REQUIRE(bytes == 3 && !memcmp(name, "new", 3) && info->id);
  ++*count;
  return LC_OK;
}
typedef struct remove_catalog {
  ps_space *space;
  ps_ref revision;
} remove_catalog;
static void *remove_catalog_namespace(void *context) {
  remove_catalog *work = context;
  lc_error e = {0};
  ok(ps_catalog_remove(work->space, 0, PS_CATALOG_NAMESPACE, "new", 3,
                       &work->revision, &e),
     &e);
  return NULL;
}
static void wait_catalog_sync(durable_gate *gate) {
  pthread_mutex_lock(&gate->mutex);
  while (!gate->entered)
    pthread_cond_wait(&gate->cond, &gate->mutex);
  pthread_mutex_unlock(&gate->mutex);
}
static void finish_catalog_sync(durable_gate *gate, pthread_t thread) {
  pthread_mutex_lock(&gate->mutex);
  gate->released = 1;
  pthread_cond_broadcast(&gate->cond);
  pthread_mutex_unlock(&gate->mutex);
  REQUIRE(!pthread_join(thread, NULL));
}
static void additive_catalog(void) {
  lc_error e = {0};
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_catalog_view *view = NULL;
  ps_budget_plan plan = {PS_PAGE, 0, 0, 0};
  ps_budget *budget = NULL;
  ps_reserve *reserve = NULL;
  ps_catalog_info info;
  pthread_t thread;
  size_t seen = 0;
  options.io = &io;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_open(path, 0, &options, &b, &e), &e);
  gate.paused = 1;
  REQUIRE(!pthread_create(&thread, NULL, publish_catalog_namespace, a));
  wait_catalog_sync(&gate);
  ok(ps_reserve_open(b, &plan, 0, &reserve, &e), &e);
  ok(ps_budget_open(b, &plan, &budget, &e), &e);
  ok(ps_catalog_get(b, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(!view);
  ok(ps_catalog_visit(b, 0, PS_CATALOG_NAMESPACE, count_catalog_namespace,
                      &seen, &e),
     &e);
  REQUIRE(!seen);
  capture_put(b, 2, 65);
  ok(ps_budget_close(budget, &e), &e);
  ps_reserve_close(reserve);
  finish_catalog_sync(&gate, thread);
  ok(ps_catalog_get(b, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, &info, &e), &e);
  ps_catalog_view_close(view);
  ok(ps_catalog_visit(b, 0, PS_CATALOG_NAMESPACE, count_catalog_namespace,
                      &seen, &e),
     &e);
  REQUIRE(seen == 1);
  /* The same descriptor now carries a local removal. Its live addition's
   * earlier wire prefix cannot certify that new removal. */
  remove_catalog work = {b, info.revision};
  gate.entered = gate.released = 0;
  gate.paused = 1;
  REQUIRE(!pthread_create(&thread, NULL, remove_catalog_namespace, &work));
  wait_catalog_sync(&gate);
  ok(ps_catalog_get(b, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(view);
  ps_catalog_view_close(view);
  REQUIRE(ps_catalog_get(a, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e) ==
              LC_ERR_PENDING &&
          !view);
  lc_error_cleanup(&e);
  finish_catalog_sync(&gate, thread);
  ok(ps_catalog_get(b, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(!view);
  ok(ps_catalog_get(a, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(!view);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
  REQUIRE(!unlink(path));
}
static void journal_replay_structural(void) {
  replay_structural(0);
  replay_structural(1);
  additive_catalog();
}
typedef struct tail_callback {
  ps_space *owner, *peer;
  ps_owned_head *observer;
  size_t calls;
  int reject;
} tail_callback;
static int tail_publish(void *context, uint64_t tail, lc_error *e) {
  tail_callback *state = context;
  ++state->calls;
  REQUIRE(tail == 3);
  ok(ps_refresh(state->peer, e), e);
  expect_tail(state->observer, 2, 0);
  if (state->reject)
    return LC_ERR_PENDING;
  capture_put(state->peer, 11, 65);
  return ps_flush(state->owner, e);
}
static void owned_stream_readiness(void) {
  for (int durable = 0; durable <= 1; ++durable) {
    lc_error e = {0};
    ps_options options = {0};
    ps_space *a = NULL, *b = NULL;
    ps_owned_head *owner = NULL, *observer = NULL;
    ps_owned_run *run = NULL, *captured = NULL;
    ps_owned_status info;
    unsigned char observed[4] = {0};
    int found;
    options.durable_sync = durable;
    ok(ps_open(path, 1, &options, &a, &e), &e);
    ok(ps_owned_open(a, NULL, tail_stream(), 1, NULL, &owner, &info, &e), &e);
    ok(ps_owned_grant(owner, NULL, 0, PS_PAGE, &run, &found, &e), &e);
    REQUIRE(found);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_owned_write(run, 0, "ab", 2, &e), &e);
    ok(ps_owned_commit(owner, 2, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    ok(ps_owned_open(b, NULL, tail_stream(), 0, &info.ref, &observer, &info,
                     &e),
       &e);
    expect_tail(observer, 2, 0);
    ok(ps_owned_grant(observer, NULL, 0, 0, &captured, &found, &e), &e);
    REQUIRE(found);
    uint64_t selected, stable;
    ok(ps_publication_prefix(b, &selected, &stable, &e), &e);
    uint64_t wire = readiness(a).ready;
    ok(ps_owned_write(run, 2, "c", 1, &e), &e);
    tail_callback callback = {a, b, observer, 0, 1};
    REQUIRE(ps_owned_commit(owner, 3, PS_COMMIT_DEFAULT, tail_publish,
                            &callback, &e) == LC_ERR_PENDING);
    REQUIRE(callback.calls == 1 && readiness(a).ready == wire);
    expect_tail(owner, 2, 0);
    callback.reject = 0;
    ok(ps_owned_commit(owner, 3, PS_COMMIT_DEFAULT, tail_publish, &callback,
                       &e),
       &e);
    REQUIRE(callback.calls == 2);
    allocations = 0;
    measuring = 1;
    int rc = ps_refresh(b, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations);
    expect_tail(observer, 3, 0);
    uint64_t peer_selected, peer_stable;
    ok(ps_publication_prefix(b, &peer_selected, &peer_stable, &e), &e);
    REQUIRE(peer_selected == selected + 1 && peer_stable >= stable);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_owned_write(run, 3, "d", 1, &e), &e);
    ok(ps_owned_commit(owner, 4, PS_COMMIT_DEFER_SYNC, NULL, NULL, &e), &e);
    ok(ps_refresh(b, &e), &e);
    expect_tail(observer, 4, 0);
    ok(ps_owned_read(captured, 0, observed, sizeof(observed), &e), &e);
    REQUIRE(!memcmp(observed, "abcd", sizeof(observed)));
    REQUIRE(readiness(b).durable < readiness(b).ready);
    uint64_t syncs = status(b).syncs;
    ok(ps_owned_seal(observer, &e), &e);
    REQUIRE(status(b).syncs == syncs + 2);
    ok(ps_refresh(a, &e), &e);
    expect_tail(owner, 4, 1);
    ps_owned_run_close(captured);
    ps_owned_run_close(run);
    ps_owned_close(observer);
    ps_owned_close(owner);
    ok(ps_close(b, &e), &e);
    ok(ps_close(a, &e), &e);
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    ok(ps_owned_open(a, NULL, tail_stream(), 0, NULL, &owner, &info, &e), &e);
    expect_tail(owner, 4, 1);
    expect_value(a, key(11), 65, 11);
    ps_owned_close(owner);
    ok(ps_close(a, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
}
static void owned_stream_cold(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *owner = NULL, *observer = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status info;
  int found;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ok(ps_owned_open(a, NULL, tail_stream(), 1, NULL, &owner, &info, &e), &e);
  ok(ps_owned_grant(owner, NULL, 0, PS_PAGE, &run, &found, &e), &e);
  REQUIRE(found);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_owned_write(run, 0, "abc", 3, &e), &e);
  ok(ps_owned_commit(owner, 3, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  capture_put(a, 11, 65);
  ok(ps_flush(a, &e), &e);
  ps_owned_run_close(run);
  ps_owned_close(owner);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_owned_open(a, NULL, tail_stream(), 0, NULL, &owner, &info, &e), &e);
  expect_tail(owner, 0, 0);
  expect_value(a, key(11), 65, 11);
  ok(ps_owned_grant(owner, NULL, 0, 0, &run, &found, &e), &e);
  REQUIRE(found);
  /* A complete old hint survived. Canonical recovery accepts a shorter
   * prefix and publishes its result to the new cohort. */
  ok(ps_owned_recovered(owner, 1, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_owned_open(b, NULL, tail_stream(), 0, NULL, &observer, &info, &e), &e);
  expect_tail(observer, 1, 0);
  expect_value(b, key(11), 65, 11);
  ps_owned_run_close(run);
  ps_owned_close(owner);
  ps_owned_close(observer);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct tail_write_fault {
  size_t persist, fired;
} tail_write_fault;
static ssize_t tail_partial_write(void *context, int fd, const void *p,
                                  size_t n, off_t offset) {
  tail_write_fault *fault = context;
  if (fault->persist && n == 128 && offset >= PS_JOURNAL_OFFSET &&
      offset < PS_JOURNAL_OFFSET + PS_JOURNAL) {
    REQUIRE(pwrite(fd, p, fault->persist, offset) == (ssize_t)fault->persist);
    ++fault->fired;
    errno = EIO;
    return -1;
  }
  return pwrite(fd, p, n, offset);
}
typedef struct tail_selection {
  ps_space *space;
  tail_write_fault *fault;
} tail_selection;
static int tail_select(void *context, uint64_t tail, lc_error *e) {
  tail_selection *selection = context;
  REQUIRE(tail == 3);
  capture_put(selection->space, 11, 65);
  ok(ps_flush(selection->space, e), e);
  selection->fault->persist = 128;
  return LC_OK;
}
static void owned_stream_selected_fault(void) {
  lc_error e = {0};
  tail_write_fault failure = {0};
  ps_io io = {&failure, read_io, tail_partial_write, sync_io, reserve_io, NULL};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_owned_head *observer = NULL;
  ps_owned_run *run = NULL;
  ps_owned_status info;
  unsigned char body[3];
  int found;
  options.io = &io;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 1, NULL, NULL, NULL, &stream, &e),
     &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_stream_begin(stream, &writer, &e), &e);
  ok(ps_stream_write(writer, "abc", 3, &e), &e);
  tail_selection selection = {a, &failure};
  REQUIRE(ps_stream_commit(writer, PS_COMMIT_DEFAULT, tail_select, &selection,
                           &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  REQUIRE(failure.fired == 1);
  ps_stream_writer_close(writer);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  expect_value(b, key(11), 65, 11);
  ok(ps_owned_open(b, NULL, tail_stream(), 0, NULL, &observer, &info, &e), &e);
  expect_tail(observer, 0, 0);
  ok(ps_owned_grant(observer, NULL, 0, 0, &run, &found, &e), &e);
  REQUIRE(found);
  ok(ps_owned_read(run, 0, body, sizeof(body), &e), &e);
  REQUIRE(!memcmp(body, "abc", sizeof(body)));
  ps_owned_run_close(run);
  ps_owned_close(observer);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void owned_stream_faults(void) {
  for (size_t bytes = 32; bytes <= 128; bytes *= 2) {
    lc_error e = {0};
    tail_write_fault failure = {0};
    ps_io io = {&failure, read_io,    tail_partial_write,
                sync_io,  reserve_io, NULL};
    ps_options options = {0};
    ps_space *a = NULL, *b = NULL;
    ps_owned_head *owner = NULL, *observer = NULL;
    ps_owned_run *run = NULL;
    ps_owned_status info;
    int found;
    options.io = &io;
    ok(ps_open(path, 1, &options, &a, &e), &e);
    ok(ps_owned_open(a, NULL, tail_stream(), 1, NULL, &owner, &info, &e), &e);
    ok(ps_owned_grant(owner, NULL, 0, PS_PAGE, &run, &found, &e), &e);
    REQUIRE(found);
    ok(ps_checkpoint(a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    ok(ps_owned_open(b, NULL, tail_stream(), 0, NULL, &observer, &info, &e),
       &e);
    ok(ps_owned_write(run, 0, "x", 1, &e), &e);
    failure.persist = bytes;
    REQUIRE(ps_owned_commit(owner, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e) ==
            LC_ERR_TRANSPORT);
    lc_error_cleanup(&e);
    REQUIRE(failure.fired == 1);
    REQUIRE(ps_refresh(b, &e) == LC_ERR_PENDING);
    lc_error_cleanup(&e);
    expect_tail(observer, 0, 0);
    ps_owned_run_close(run);
    ps_owned_close(owner);
    ok(ps_close(a, &e), &e);
    ok(ps_open(path, 0, NULL, &a, &e), &e);
    ok(ps_refresh(b, &e), &e);
    expect_tail(observer, 0, 0);
    ok(ps_owned_open(a, NULL, tail_stream(), 0, NULL, &owner, &info, &e), &e);
    expect_tail(owner, 0, 0);
    ps_owned_close(owner);
    ps_owned_close(observer);
    ok(ps_close(b, &e), &e);
    ok(ps_close(a, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
  owned_stream_selected_fault();
}
typedef struct stream_recovery {
  size_t calls;
  int reject;
} stream_recovery;
static int recover_stream(ps_stream *stream, uint64_t checkpoint,
                          uint64_t capacity, uint64_t *accepted, void *context,
                          lc_error *e) {
  stream_recovery *state = context;
  unsigned char marker;
  int rc = LC_OK;
  ++state->calls;
  if (state->reject)
    return LC_ERR_NOMEM;
  *accepted = checkpoint;
  /* These bytes stand for complete test records. The killed appender follows
   * its candidate record with bytes the reader must reject. */
  while (*accepted < capacity) {
    rc = ps_stream_recovery_read(stream, *accepted, &marker, 1, e);
    if (rc != LC_OK || (marker != 'a' && marker != 'b' && marker != 'R'))
      break;
    ++*accepted;
  }
  return rc;
}
static void append_byte(ps_stream *s, uint64_t start, const char *body,
                        lc_error *e) {
  ps_stream_writer *writer = NULL;
  ok(ps_stream_begin(s, &writer, e), e);
  REQUIRE(ps_stream_writer_start(writer) == start);
  ok(ps_stream_write(writer, body, 1, e), e);
  ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, e), e);
  ps_stream_writer_close(writer);
}
static void *finish_stream_append(void *context) {
  lc_error e = {0};
  ps_stream_writer *w = context;
  ok(ps_stream_commit(w, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(w);
  return NULL;
}
static void stream_handoff(int durable) {
  lc_error e = {0};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_stream *x = NULL, *y = NULL, *other = NULL;
  ps_stream_writer *writer = NULL, *blocked = NULL;
  ps_stream_window *old = NULL, *view = NULL;
  stream_recovery rx = {0}, ry = {0};
  ps_stream_info info;
  ps_stats before, after;
  const ps_key unrelated = {88, 111, PS_STREAM};
  pthread_t producer;
  char byte;
  options.durable_sync = durable;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 1, NULL, recover_stream, &rx, &x,
                    &e),
     &e);
  append_byte(x, 0, "a", &e);
  ok(ps_open(path, 0, &options, &b, &e), &e);
  ok(ps_stream_open(b, NULL, tail_stream(), 0, NULL, recover_stream, &ry, &y,
                    &e),
     &e);
  ok(ps_stream_open(b, NULL, unrelated, 1, NULL, NULL, NULL, &other, &e), &e);
  ok(ps_stream_window_open(y, 0, 1, &old, &e), &e);
  ok(ps_stream_begin(x, &writer, &e), &e);
  REQUIRE(ps_stream_writer_start(writer) == 1 && !rx.calls && !ry.calls);
  ok(ps_stream_status(x, &info, &e), &e);
  ps_key grant = {info.ref.incarnation, info.capacity, PS_OWNED_RUN};
  ok(ps_stream_reserve(writer, NULL, PS_CHUNK, &e), &e);
  ps_entry *prepared = ps_internal_lookup(a, grant);
  REQUIRE(prepared && prepared->count == 1);
  off_t offset = (off_t)prepared->extents[0].offset;
  unsigned char first, damaged;
  REQUIRE(pread(a->fd, &first, 1, offset) == 1);
  damaged = first ^ 1;
  REQUIRE(pwrite(a->fd, &damaged, 1, offset) == 1);
  uint64_t free_bytes = b->free_bytes;
  REQUIRE(ps_refresh(b, &e) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&e);
  REQUIRE(!ps_internal_lookup(b, grant) && b->free_bytes == free_bytes);
  REQUIRE(pwrite(a->fd, &first, 1, offset) == 1);
  REQUIRE(ps_stream_begin(y, &blocked, &e) == LC_ERR_PENDING && !blocked);
  lc_error_cleanup(&e);
  REQUIRE(ps_stream_seal(y, &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  REQUIRE(ps_stream_retire(y, &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  ps_owned_range retained = {0, 5};
  REQUIRE(ps_stream_piece_repack(y, 0, NULL, &retained, 1, NULL, NULL, &e) ==
          LC_ERR_PENDING);
  lc_error_cleanup(&e);
  ok(ps_stream_window_read(old, 0, &byte, 1, &e), &e);
  REQUIRE(byte == 'a');
  ok(ps_stream_window_open(y, 0, 1, &view, &e), &e);
  ps_stream_window_close(view);
  append_byte(other, 0, "x", &e);
  ok(ps_stream_write(writer, "b", 1, &e), &e);
  REQUIRE(pthread_create(&producer, NULL, finish_stream_append, writer) == 0);
  REQUIRE(pthread_join(producer, NULL) == 0);
  ry.reject = 1;
  ok(ps_status(b, &before, &e), &e);
  ok(ps_stream_begin(y, &writer, &e), &e);
  REQUIRE(ps_stream_writer_start(writer) == 2);
  ok(ps_status(b, &after, &e), &e);
  REQUIRE(after.syncs == before.syncs && !ry.calls);
  ok(ps_stream_write(writer, "c", 1, &e), &e);
  ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(writer);
  ry.reject = 0;
  ok(ps_stream_status(y, &info, &e), &e);
  REQUIRE(!ry.calls && info.tail == 3 && info.runs > 1);
  append_byte(y, 3, "d", &e);
  REQUIRE(!ry.calls);
  ok(ps_checkpoint(b, &e), &e);
  rx.reject = 1;
  REQUIRE(ps_stream_begin(x, &blocked, &e) == LC_ERR_NOMEM && !blocked);
  ok(ps_stream_status(x, &info, &e), &e);
  REQUIRE(info.tail == 2);
  rx.reject = 0;
  append_byte(x, 4, "e", &e);
  REQUIRE(rx.calls == 2);
  ok(ps_stream_seal(x, &e), &e);
  ok(ps_stream_window_read(old, 0, &byte, 1, &e), &e);
  REQUIRE(byte == 'a');
  REQUIRE(ps_stream_begin(y, &blocked, &e) == LC_ERR_INVALID && !blocked);
  lc_error_cleanup(&e);
  ok(ps_stream_status(y, &info, &e), &e);
  REQUIRE(info.sealed && info.tail == 5);
  ok(ps_stream_piece_repack(y, 0, NULL, &retained, 1, NULL, NULL, &e), &e);
  ok(ps_stream_window_read(old, 0, &byte, 1, &e), &e);
  REQUIRE(byte == 'a');
  ps_stream_window_close(old);
  ok(ps_stream_close(other, &e), &e);
  ok(ps_stream_close(y, &e), &e);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 0, NULL, NULL, NULL, &x, &e), &e);
  ok(ps_stream_window_open(x, 0, 5, &old, &e), &e);
  char body[5];
  ok(ps_stream_window_read(old, 0, body, sizeof(body), &e), &e);
  REQUIRE(!memcmp(body, "abcde", sizeof(body)));
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_stream_open(b, NULL, tail_stream(), 0, NULL, NULL, NULL, &y, &e), &e);
  ok(ps_stream_retire(y, &e), &e);
  REQUIRE(ps_stream_window_open(x, 0, 1, &view, &e) == LC_ERR_INVALID && !view);
  lc_error_cleanup(&e);
  ok(ps_stream_window_read(old, 0, body, sizeof(body), &e), &e);
  REQUIRE(!memcmp(body, "abcde", sizeof(body)));
  ok(ps_stream_close(y, &e), &e);
  ok(ps_close(b, &e), &e);
  ps_stream_window_close(old);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static int reject_stream_publish(void *context, uint64_t tail, lc_error *e) {
  ps_stream *peer = context;
  ps_stream_writer *blocked = NULL;
  REQUIRE(tail == 2);
  REQUIRE(ps_stream_begin(peer, &blocked, e) == LC_ERR_PENDING && !blocked);
  return LC_ERR_PENDING;
}
static void stream_abort_and_cold_open(void) {
  lc_error e = {0};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_stream *x = NULL, *y = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_window *view = NULL;
  stream_recovery rx = {0}, ry = {0};
  ps_stream_info info;
  char body[3];
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 1, NULL, recover_stream, &rx, &x,
                    &e),
     &e);
  append_byte(x, 0, "a", &e);
  ok(ps_open(path, 0, &options, &b, &e), &e);
  ok(ps_stream_open(b, NULL, tail_stream(), 0, NULL, recover_stream, &ry, &y,
                    &e),
     &e);
  REQUIRE(!ry.calls);
  ok(ps_stream_begin(x, &writer, &e), &e);
  ok(ps_stream_write(writer, "R", 1, &e), &e);
  REQUIRE(ps_stream_commit(writer, PS_COMMIT_DEFAULT, reject_stream_publish, y,
                           &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  ps_stream_writer_close(writer);
  append_byte(y, 1, "b", &e);
  REQUIRE(ry.calls == 1);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_stream_status(y, &info, &e), &e);
  const ps_key first_key = {info.ref.incarnation, 0, PS_OWNED_RUN};
  ps_entry *first_run = ps_internal_lookup(b, first_key);
  REQUIRE(first_run && first_run->count == 1);
  unsigned char invalid_footer = 0;
  off_t footer = (off_t)first_run->extents[0].offset + PS_RUN_IDENTITY_BYTES;
  REQUIRE(pwrite(b->fd, &invalid_footer, 1, footer) == 1);
  ok(ps_stream_open(a, NULL, tail_stream(), 0, NULL, recover_stream, &rx, &x,
                    &e),
     &e);
  ok(ps_stream_status(x, &info, &e), &e);
  REQUIRE(rx.calls == 1 && info.tail == 2);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_stream_close(y, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 0, NULL, recover_stream, &rx, &x,
                    &e),
     &e);
  REQUIRE(rx.calls == 2);
  append_byte(x, 2, "c", &e);
  ok(ps_stream_window_open(x, 0, sizeof(body), &view, &e), &e);
  ok(ps_stream_window_read(view, 0, body, sizeof(body), &e), &e);
  REQUIRE(!memcmp(body, "abc", sizeof(body)));
  ps_stream_window_close(view);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void stream_handoff_child(int ready, int scenario) {
  lc_error e = {0};
  ps_options options = {0};
  ps_space *s = NULL;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  stream_recovery recovery = {0};
  unsigned char *body = malloc(PS_CHUNK);
  REQUIRE(body);
  options.durable_sync = 1;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ok(ps_stream_open(s, NULL, tail_stream(), 0, NULL, recover_stream, &recovery,
                    &stream, &e),
     &e);
  ok(ps_stream_begin(stream, &writer, &e), &e);
  REQUIRE(ps_stream_writer_start(writer) == 1);
  memset(body, '!', PS_CHUNK);
  if (scenario != 0)
    body[0] = 'R';
  ok(ps_stream_write(writer, body, PS_CHUNK, &e), &e);
  if (scenario == 2)
    ok(ps_checkpoint(s, &e), &e);
  else
    ok(ps_flush(s, &e), &e);
  REQUIRE(write(ready, "R", 1) == 1);
  for (;;)
    pause();
}
static void stream_takeover(void) {
  for (int scenario = 0; scenario < 3; ++scenario) {
    int complete = scenario != 0;
    lc_error e = {0};
    ps_options options = {0};
    ps_space *s = NULL;
    ps_stream *stream = NULL;
    ps_stream_writer *blocked = NULL;
    ps_stream_window *window = NULL;
    stream_recovery recovery = {0};
    int ready[2], exited;
    char body[3] = {0};
    options.durable_sync = 1;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    ok(ps_stream_open(s, NULL, tail_stream(), 1, NULL, recover_stream,
                      &recovery, &stream, &e),
       &e);
    append_byte(stream, 0, "a", &e);
    ok(ps_stream_window_open(stream, 0, 1, &window, &e), &e);
    REQUIRE(pipe(ready) == 0);
    pid_t child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      char fd[16], valid[16];
      REQUIRE(close(ready[0]) == 0);
      snprintf(fd, sizeof(fd), "%d", ready[1]);
      snprintf(valid, sizeof(valid), "%d", scenario);
      execl(executable, executable, "--stream-child", path, fd, valid,
            (char *)NULL);
      _exit(127);
    }
    REQUIRE(close(ready[1]) == 0);
    REQUIRE(read(ready[0], body, 1) == 1 && body[0] == 'R');
    REQUIRE(ps_stream_begin(stream, &blocked, &e) == LC_ERR_PENDING &&
            !blocked);
    lc_error_cleanup(&e);
    REQUIRE(!recovery.calls);
    ok(ps_stream_window_read(window, 0, body, 1, &e), &e);
    REQUIRE(body[0] == 'a');
    REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &exited, 0) == child &&
            WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
    REQUIRE(close(ready[0]) == 0);
    append_byte(stream, 1 + (uint64_t)complete, "y", &e);
    REQUIRE(recovery.calls == 1);
    ok(ps_stream_seal(stream, &e), &e);
    ok(ps_stream_window_read(window, 0, body, 1, &e), &e);
    REQUIRE(body[0] == 'a');
    ps_stream_window_close(window);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(s, &e), &e);
    ok(ps_open(path, 0, NULL, &s, &e), &e);
    ok(ps_stream_open(s, NULL, tail_stream(), 0, NULL, NULL, NULL, &stream, &e),
       &e);
    ok(ps_stream_window_open(stream, 0, 2 + (uint64_t)complete, &window, &e),
       &e);
    ok(ps_stream_window_read(window, 0, body, 2 + (size_t)complete, &e), &e);
    REQUIRE(!memcmp(body, complete ? "aRy" : "ay", 2 + (size_t)complete));
    ps_stream_window_close(window);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(s, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
}
static int stream_unlock_fault(void *context, int fd, int command,
                               struct flock *lock) {
  int *fail = context;
  if (*fail == 1 && command == F_OFD_SETLK && lock->l_type == F_UNLCK &&
      lock->l_start >= 8 && lock->l_start < 256) {
    ++*fail;
    errno = EIO;
    return -1;
  }
  return fcntl(fd, command, lock);
}
static void stream_handoff_faults(void) {
  lc_error e = {0};
  int fail = 0;
  ps_coordination_io coordination = {&fail, stream_unlock_fault};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_stream *x = NULL, *y = NULL;
  ps_stream_writer *writer = NULL, *blocked = NULL;
  ps_stream_window *window = NULL;
  stream_recovery rx = {0}, ry = {0};
  char body[3];
  options.coordination_io = &coordination;
  options.durable_sync = 1;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 1, NULL, recover_stream, &rx, &x,
                    &e),
     &e);
  append_byte(x, 0, "a", &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_stream_open(b, NULL, tail_stream(), 0, NULL, recover_stream, &ry, &y,
                    &e),
     &e);
  ok(ps_stream_begin(x, &writer, &e), &e);
  ok(ps_stream_write(writer, "b", 1, &e), &e);
  fail = 1;
  REQUIRE(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e) ==
          LC_ERR_TRANSPORT);
  REQUIRE(fail == 2);
  lc_error_cleanup(&e);
  ps_stream_writer_close(writer);
  REQUIRE(ps_stream_begin(y, &blocked, &e) == LC_ERR_PENDING && !blocked);
  lc_error_cleanup(&e);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(a, &e), &e);
  append_byte(y, 2, "c", &e);
  ok(ps_stream_window_open(y, 0, 3, &window, &e), &e);
  ok(ps_stream_window_read(window, 0, body, sizeof(body), &e), &e);
  REQUIRE(!memcmp(body, "abc", sizeof(body)));
  ps_stream_window_close(window);
  ok(ps_stream_close(y, &e), &e);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
int main(int argc, char **argv) {
  if (argc == 5 && !strcmp(argv[1], "--prepared-grant-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    prepared_grant_child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--stream-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    stream_handoff_child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--journal-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    journal_replay_child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--prefix-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    certified_prefix_child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--projection-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    projection_child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  REQUIRE(argc == 3 || (argc == 4 && !strcmp(argv[3], "--cooperative-wait")));
  cooperative_wait = argc == 4;
  if (cooperative_wait) {
    REQUIRE(!strcmp(argv[2], "threads") || !strcmp(argv[2], "serialized") ||
            !strcmp(argv[2], "packed") || !strcmp(argv[2], "capture-race") ||
            !strcmp(argv[2], "shared-durable"));
    fprintf(stderr,
            "test-only cooperative OFD waits; real kernel locks retained\n");
  }
  ssize_t n = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
  REQUIRE(n > 0 && (size_t)n < sizeof(executable) - 1);
  executable[n] = 0;
  REQUIRE(snprintf(directory, sizeof(directory), "%s/live-allocator-XXXXXX",
                   argv[1]) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  if (!strcmp(argv[2], "threads"))
    threads();
  else if (!strcmp(argv[2], "faults"))
    faults();
  else if (!strcmp(argv[2], "fence"))
    fence();
  else if (!strcmp(argv[2], "serialized"))
    serialized();
  else if (!strcmp(argv[2], "packed"))
    packed();
  else if (!strcmp(argv[2], "history"))
    history();
  else if (!strcmp(argv[2], "captures"))
    captures();
  else if (!strcmp(argv[2], "capture-faults"))
    capture_faults();
  else if (!strcmp(argv[2], "capture-owned"))
    capture_owned();
  else if (!strcmp(argv[2], "capture-race"))
    capture_race();
  else if (!strcmp(argv[2], "projection"))
    projection_lifecycle();
  else if (!strcmp(argv[2], "projection-faults"))
    projection_faults();
  else if (!strcmp(argv[2], "projection-recovery"))
    projection_recovery();
  else if (!strcmp(argv[2], "projection-threads"))
    projection_threads();
  else if (!strcmp(argv[2], "projection-boundaries"))
    projection_boundaries();
  else if (!strcmp(argv[2], "projection-pressure"))
    projection_pressure();
  else if (!strcmp(argv[2], "projection-tail"))
    projection_tail();
  else if (!strcmp(argv[2], "shared-durable"))
    shared_durability();
  else if (!strcmp(argv[2], "readiness-cohort"))
    readiness_cohort();
  else if (!strcmp(argv[2], "readiness-faults"))
    readiness_faults();
  else if (!strcmp(argv[2], "certified-prefix")) {
    certified_prefix_reads();
    certified_prefix_interruption();
  } else if (!strcmp(argv[2], "quarantine")) {
    quarantine_capacity();
    quarantine_packed();
  } else if (!strcmp(argv[2], "journal-replay"))
    journal_replay();
  else if (!strcmp(argv[2], "journal-replay-faults"))
    journal_replay_faults();
  else if (!strcmp(argv[2], "journal-replay-structural"))
    journal_replay_structural();
  else if (!strcmp(argv[2], "journal-replay-process"))
    journal_replay_process();
  else if (!strcmp(argv[2], "owned-stream-readiness"))
    owned_stream_readiness();
  else if (!strcmp(argv[2], "owned-stream-cold"))
    owned_stream_cold();
  else if (!strcmp(argv[2], "owned-stream-faults"))
    owned_stream_faults();
  else if (!strcmp(argv[2], "prepared-grants")) {
    prepared_grants();
    prepared_grant_fault();
  } else if (!strcmp(argv[2], "stream-handoff")) {
    stream_handoff(0);
    stream_handoff(1);
    stream_abort_and_cold_open();
  } else if (!strcmp(argv[2], "stream-handoff-faults"))
    stream_handoff_faults();
  else if (!strcmp(argv[2], "stream-takeover"))
    stream_takeover();
  else {
    REQUIRE(!strcmp(argv[2], "processes"));
    processes();
  }
  REQUIRE(rmdir(directory) == 0);
  while (adapters) {
    test_adapter *a = adapters;
    adapters = a->next;
    free(a);
  }
  fprintf(stderr, "live allocator %s: %zu checks\n", argv[2], checks);
  return 0;
}
