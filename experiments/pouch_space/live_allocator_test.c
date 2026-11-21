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
  /* Count allocator range witnesses. The readiness page carries logical
   * lifetime locks that own no workspace or payload bytes. */
  pending[0].first = PS_METADATA_START;
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
  ps_budget_plan plan = {PS_CHUNK, 4096, 1, 0};
  ps_budget *budget = NULL;
  ps_io io = {&written, read_io,    trace_private_write,
              sync_io,  reserve_io, NULL};
  ps_options options = {0};
  char command;
  options.io = &io;
  ok(ps_open(path, 0, &options, &space, &e), &e);
  REQUIRE(write(ready, "O", 1) == 1);
  REQUIRE(read(control, &command, 1) == 1 && command == 'W');
  ok(ps_budget_open(space, &plan, &budget, &e), &e);
  REQUIRE(private_value(space, budget, 9000));
  REQUIRE(write(ready, &written, sizeof(written)) == sizeof(written));
  for (;;)
    pause();
}
static uint64_t expect_zero_grant(ps_space *space, ps_budget *budget,
                                  ps_owned_head *head,
                                  const ps_owned_status *info, uint64_t base,
                                  uint64_t physical, uint64_t syncs) {
  lc_error e = {0};
  ps_owned_run *run = NULL;
  ps_key key = {info->ref.incarnation, base, PS_OWNED_RUN};
  unsigned char bytes[PS_PAGE - PS_RUN_HEADER];
  ps_stats before = status(space);
  int found = 0;
  ok(ps_owned_grant(head, budget, base, PS_PAGE, &run, &found, &e), &e);
  REQUIRE(found && ps_owned_run_capacity(run) == sizeof(bytes));
  REQUIRE(status(space).syncs == before.syncs + syncs);
  ps_entry *p = ps_internal_lookup(space, key);
  REQUIRE(p && p->count == 1);
  uint64_t selected = p->extents[0].offset;
  if (physical && selected != physical)
    fprintf(stderr, "grant at %llu selected %llu, expected %llu\n",
            (unsigned long long)base, (unsigned long long)selected,
            (unsigned long long)physical);
  REQUIRE(!physical || selected == physical);
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
static void retain_preparation_after(ps_space *space, uint64_t first) {
  ps_coordination_guard *guard = NULL;
  lc_error e = {0};
  REQUIRE(!pthread_mutex_lock(&space->mutex));
  ok(ps_internal_capture_begin(space, &guard, &e), &e);
  ps_readiness_zero_pages(space, PS_DATA, first - PS_DATA, 0);
  ok(ps_internal_capture_end(space, guard, LC_OK, &e), &e);
  REQUIRE(!pthread_mutex_unlock(&space->mutex));
  lc_error_cleanup(&e);
}
static void prepared_grants(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *x = NULL, *y = NULL;
  ps_owned_status ix, iy;
  ps_budget_plan plan = {4 * PS_PAGE, PS_OWNED_DESCRIPTOR_BYTES, 1, 0};
  ps_budget *budget = NULL;
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
  uint64_t first = expect_zero_grant(a, NULL, x, &ix, 0, 0, 1);
  uint64_t funded = first + PS_PAGE;
  ok(ps_budget_open(b, &plan, &budget, &e), &e);
  REQUIRE(prepared_prefix(a, funded, plan.payload_bytes) == plan.payload_bytes);
  expect_zero_grant(b, budget, y, &iy, 0, funded, 0);
  REQUIRE(!prepared_prefix(a, funded, PS_PAGE));
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(prepared_prefix(a, funded + PS_PAGE, plan.payload_bytes - PS_PAGE) ==
          plan.payload_bytes - PS_PAGE);
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
  /* Isolate the prepared suffix of the writer's free interval from any
   * earlier prepared pages returned by the metadata workspace. */
  retain_preparation_after(a, dirtied + PS_CHUNK);
  uint64_t replacement =
      expect_zero_grant(a, NULL, x, &ix, payload, dirtied + PS_CHUNK, 0);
  ok(ps_checkpoint(a, &e), &e);
  expect_zero_grant(b, NULL, y, &iy, payload, replacement + PS_PAGE, 0);
  REQUIRE(!prepared_prefix(b, dirtied, PS_CHUNK));
  REQUIRE(prepared_prefix(b, replacement + 2 * PS_PAGE, PS_PAGE) == PS_PAGE);
  ps_owned_close(x);
  ps_owned_close(y);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);

  /* The next process cohort has no live zero proof, even when pages remain
   * physically zero and the bitmap bytes survived the previous close. */
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ok(ps_owned_open(a, NULL, kx, 0, &ix.ref, &x, &ix, &e), &e);
  REQUIRE(ix.capacity == 2 * payload);
  REQUIRE(!prepared_prefix(a, replacement + 2 * PS_PAGE, PS_PAGE));
  expect_zero_grant(a, NULL, x, &ix, ix.capacity, 0, 1);
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
  expect_zero_grant(b, NULL, head, &info, 0, fault.trace.header, 1);
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
typedef struct packed_replay_probe {
  uint64_t page;
  size_t reads;
} packed_replay_probe;
static ssize_t packed_replay_read(void *context, int fd, void *bytes, size_t n,
                                  off_t offset) {
  packed_replay_probe *probe = context;
  if (probe->page && (uint64_t)offset >= probe->page &&
      (uint64_t)offset - probe->page < PS_PAGE)
    ++probe->reads;
  return pread(fd, bytes, n, offset);
}
static ssize_t packed_replay_write(void *context, int fd, const void *bytes,
                                   size_t n, off_t offset) {
  (void)context;
  return pwrite(fd, bytes, n, offset);
}
static int packed_replay_visit(void *context, ps_key item, ps_reader *reader,
                               lc_error *e) {
  char byte;
  size_t *seen = context;
  REQUIRE(item.key >= 800 && item.key < 816);
  REQUIRE(!(*seen & ((size_t)1 << (item.key - 800))));
  *seen |= (size_t)1 << (item.key - 800);
  ok(ps_read_at(reader, 64, &byte, 1, e), e);
  REQUIRE((unsigned char)byte == item.key % 251);
  captured_expect(reader, (unsigned char)(item.key % 251), 65);
  return LC_OK;
}
static void packed_replay_locality(void) {
  packed_replay_probe probe = {0};
  ps_io io = {&probe,  packed_replay_read, packed_replay_write,
              sync_io, reserve_io,         NULL};
  ps_options options = {0};
  ps_space *s = NULL;
  ps_writer *group[8] = {0};
  ps_values *view = NULL;
  ps_reader *reader = NULL;
  ps_entry *slot;
  off_t corrupt;
  size_t seen = 0;
  char bytes[65];
  lc_error e = {0};
  options.io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  for (uint64_t i = 800; i < 808; ++i)
    capture_put(s, i, sizeof(bytes));
  for (size_t i = 0; i < 8; ++i) {
    memset(bytes, (int)((i + 808) % 251), sizeof(bytes));
    ok(ps_begin(s, NULL, key(i + 808), &group[i], &e), &e);
    ok(ps_write(group[i], bytes, sizeof(bytes), &e), &e);
  }
  ok(ps_revision_publish(group, 8, 0, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  for (size_t i = 0; i < 8; ++i)
    ps_writer_close(group[i]);
  ok(ps_flush(s, &e), &e);
  slot = ps_internal_lookup(s, key(800));
  REQUIRE(slot && slot->count == 1);
  probe.page = slot->extents[0].offset / PS_PAGE * PS_PAGE;
  for (uint64_t i = 800; i < 816; ++i) {
    slot = ps_internal_lookup(s, key(i));
    REQUIRE(slot && slot->count == 1 &&
            slot->extents[0].flags == EXTENT_PACKED &&
            slot->extents[0].offset / PS_PAGE * PS_PAGE == probe.page);
  }
  corrupt = (off_t)slot->extents[0].offset;
  ok(ps_close(s, &e), &e);
  probe.reads = 0;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  REQUIRE(probe.reads == 1);
  ps_key sample = key(800);
  ok(ps_values_capture(s, sample.owner, PS_STATE, PS_STATE, &view, &e), &e);
  REQUIRE(ps_values_count(view) == 16);
  memset(bytes, 'z', sizeof(bytes));
  ps_writer *later = NULL;
  ok(ps_begin(s, NULL, key(807), &later, &e), &e);
  ok(ps_write(later, bytes, sizeof(bytes), &e), &e);
  ok(ps_publish(later, NULL, &e), &e);
  ps_writer_close(later);
  ok(ps_flush(s, &e), &e);
  probe.reads = 0;
  ok(ps_values_read_walk(view, packed_replay_visit, &seen, &e), &e);
  REQUIRE(seen == 65535 && probe.reads == 1);
  ps_values_close(view);
  ok(ps_read_open(s, key(807), NULL, &reader, &e), &e);
  captured_expect(reader, 'z', sizeof(bytes));
  ps_reader_close(reader);
  /* Damage a later cell after the first seven members of its revision. One
   * page fetch still requires independent integrity checks for every slot. */
  REQUIRE(pwrite(s->fd, "!", 1, corrupt) == 1 && !fdatasync(s->fd));
  ok(ps_close(s, &e), &e);
  probe.reads = 0;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  REQUIRE(probe.reads == 1);
  ok(ps_read_open(s, key(807), NULL, &reader, &e), &e);
  captured_expect(reader, 807 % 251, sizeof(bytes));
  ps_reader_close(reader);
  for (uint64_t i = 808; i < 816; ++i) {
    REQUIRE(ps_read_open(s, key(i), NULL, &reader, &e) == LC_ERR_INVALID &&
            !reader);
    lc_error_cleanup(&e);
  }
  ok(ps_close(s, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
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
static int retain_first_value(void *context, ps_key item,
                              uint64_t incarnation) {
  (void)context;
  (void)incarnation;
  return item.key == 1;
}
static void capture_identities(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_reader *reader = NULL;
  ps_values *view = NULL;
  ps_key names[2];
  ps_ref original[2];
  uint64_t bytes;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, PS_PAGE);
  capture_put(a, 2, PS_PAGE);
  ok(ps_checkpoint(a, &e), &e);
  uint64_t baseline = coverage(NULL, 0);
  ok(ps_internal_values_capture(a, key(1).owner, PS_STATE, PS_STATE,
                                retain_first_value, NULL, &view, &e),
     &e);
  REQUIRE(ps_values_count(view) == 2 &&
          status(a).retained_payload_bytes == PS_PAGE &&
          coverage(NULL, 0) == baseline + PS_PAGE);
  for (size_t i = 0; i < 2; ++i) {
    ok(ps_values_info(view, i, &names[i], &original[i], &bytes, &e), &e);
    REQUIRE(bytes == PS_PAGE && original[i].incarnation);
  }
  REQUIRE(names[0].key + names[1].key == 3);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_delete(b, key(1), &e), &e);
  ok(ps_delete(b, key(2), &e), &e);
  ok(ps_checkpoint(b, &e), &e);
  REQUIRE(status(b).retired_bytes >= PS_PAGE);
  REQUIRE(ps_read_open(a, key(2), NULL, &reader, &e) == LC_ERR_INVALID &&
          !reader);
  lc_error_cleanup(&e);
  ok(ps_check(a, &e), &e);
  for (size_t i = 0; i < 2; ++i) {
    ps_key item;
    ps_ref current;
    ok(ps_values_info(view, i, &item, &current, &bytes, &e), &e);
    REQUIRE(item.owner == names[i].owner && item.kind == names[i].kind &&
            item.key == names[i].key && bytes == PS_PAGE &&
            current.incarnation == original[i].incarnation &&
            !memcmp(current.container, original[i].container,
                    sizeof(current.container)));
    if (item.key == 2) {
      REQUIRE(ps_values_read_open(view, i, &reader, &e) == LC_ERR_INVALID &&
              !reader);
      lc_error_cleanup(&e);
    } else {
      ok(ps_values_read_open(view, i, &reader, &e), &e);
      captured_expect(reader, 1, PS_PAGE);
      ps_reader_close(reader);
      reader = NULL;
    }
  }
  ps_values_close(view);
  REQUIRE(!status(a).retained_payload_bytes);
  ok(ps_close(a, &e), &e);
  ok(ps_checkpoint(b, &e), &e);
  REQUIRE(!status(b).retired_bytes);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
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
  if (m->armed && m->phase >= 4 && offset == PS_JOURNAL_OFFSET) {
    /* The selected root is durable. Stop after erasing only part of its
     * predecessor's journal so no valid header can describe the old tail. */
    REQUIRE(m->syncs == 2 && n >= 64 && !((const unsigned char *)bytes)[0]);
    REQUIRE(pwrite(fd, bytes, 32, offset) == 32);
    REQUIRE(write(m->ready, "R", 1) == 1);
    for (;;)
      pause();
  }
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
typedef struct descriptor_probe {
  uint64_t pages[144];
  size_t reads[144], count;
  size_t calls, superblocks;
  uint64_t following;
} descriptor_probe;
static ssize_t descriptor_read(void *context, int fd, void *bytes, size_t n,
                               off_t offset) {
  descriptor_probe *probe = context;
  size_t covered = 0;
  if (!offset && n == PS_PAGE + PS_TOKEN_RECORD_OFFSET + PS_TOKEN_RECORD_BYTES)
    ++probe->superblocks;
  for (size_t i = 0; i < probe->count; ++i)
    if (n >= PS_PAGE && (uint64_t)offset <= probe->pages[i] &&
        probe->pages[i] - (uint64_t)offset <= n - PS_PAGE) {
      ++probe->reads[i];
      ++covered;
    }
  if (covered)
    ++probe->calls;
  if (covered > 1 && !probe->following)
    probe->following = (uint64_t)offset + PS_PAGE;
  return pread(fd, bytes, n, offset);
}
enum directory_layout {
  DIRECTORY_SMALL,
  DIRECTORY_INTERLEAVED,
  DIRECTORY_ORDERED
};
enum directory_admission { DIRECTORY_ADOPT, DIRECTORY_JOIN, DIRECTORY_COLD };
static void directory_reopen_locality(enum directory_layout layout) {
  lc_error e = {0};
  descriptor_probe probe = {0};
  ps_io io = {&probe, descriptor_read, write_io, sync_io, reserve_io, NULL};
  ps_options options = {.io = &io};
  ps_space *writer = NULL, *observer = NULL;
  uint64_t count = layout == DIRECTORY_ORDERED       ? 4200
                   : layout == DIRECTORY_INTERLEAVED ? 3150
                                                     : 96;
  ok(ps_open(path, 1, NULL, &writer, &e), &e);
  /* Interleave disjoint key ranges on the same descriptor pages. The
   * sorted directory visits those pages from both key ranges. */
  for (uint64_t id = 1; id <= count; ++id) {
    capture_put(writer, id, 0);
    capture_put(writer, 8192 + id, 0);
  }
  if (layout == DIRECTORY_INTERLEAVED)
    ok(ps_open(path, 0, &options, &observer, &e), &e);
  /* The wide interleaving revisits pages across batches. The ordered
   * rewrite spans more than one cacheful of checked descriptor pages. */
  ok(layout == DIRECTORY_ORDERED ? ps_metadata_repack(writer, &e)
                                 : ps_checkpoint(writer, &e),
     &e);
  for (ps_meta_page *page = writer->pages; page; page = page->next)
    if (!page->index) {
      REQUIRE(probe.count < sizeof(probe.pages) / sizeof(probe.pages[0]));
      probe.pages[probe.count++] = page->run.offset;
    }
  if (layout == DIRECTORY_ORDERED)
    REQUIRE(probe.count > 128U);
  else if (layout == DIRECTORY_INTERLEAVED)
    REQUIRE(probe.count > 64U && probe.count <= 128U);
  else
    REQUIRE(probe.count >= 3U);
  for (int admission = observer ? DIRECTORY_ADOPT : DIRECTORY_JOIN;
       admission <= DIRECTORY_COLD; ++admission) {
    if (admission == DIRECTORY_COLD)
      ok(ps_close(writer, &e), &e);
    memset(probe.reads, 0, sizeof(probe.reads));
    probe.calls = 0;
    probe.superblocks = 0;
    probe.following = 0;
    if (admission == DIRECTORY_ADOPT)
      ok(ps_refresh(observer, &e), &e);
    else
      ok(ps_open(path, 0, &options, &observer, &e), &e);
    for (size_t i = 0; i < probe.count; ++i)
      REQUIRE(probe.reads[i] == 1);
    REQUIRE(probe.calls < probe.count && probe.following);
    if (admission != DIRECTORY_ADOPT)
      REQUIRE(probe.superblocks == 1);
    printf("directory_admission layout=%d pass=%d pages=%zu reads=%zu\n",
           layout, admission, probe.count, probe.calls);
    for (uint64_t id = 1; id <= count; ++id) {
      ps_ref ref;
      uint64_t bytes;
      int present;
      ok(ps_value_info(observer, key(id), &ref, &bytes, &present, &e), &e);
      REQUIRE(present && !bytes && ref.incarnation);
      ok(ps_value_info(observer, key(8192 + id), &ref, &bytes, &present, &e),
         &e);
      REQUIRE(present && !bytes && ref.incarnation);
    }
    ok(ps_close(observer, &e), &e);
    observer = NULL;
  }
  if (layout == DIRECTORY_SMALL) {
    /* The second page in a shared transfer must pass its own checks. */
    unsigned char original, changed;
    off_t offset = (off_t)probe.following;
    int fd = open(path, O_RDWR | O_CLOEXEC);
    REQUIRE(fd >= 0 && pread(fd, &original, 1, offset) == 1);
    changed = original ^ 1U;
    REQUIRE(pwrite(fd, &changed, 1, offset) == 1 && fdatasync(fd) == 0);
    REQUIRE(ps_open(path, 0, &options, &observer, &e) == LC_ERR_PROTOCOL &&
            !observer);
    lc_error_cleanup(&e);
    REQUIRE(pwrite(fd, &original, 1, offset) == 1 && fdatasync(fd) == 0 &&
            close(fd) == 0);
    ok(ps_open(path, 0, &options, &observer, &e), &e);
    ok(ps_close(observer, &e), &e);
  }
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void projection_quarantine(int durable, int explicit_sweep) {
  lc_error e = {0};
  ps_options options = {.durable_sync = durable};
  ps_space *writer = NULL, *peer = NULL;
  ps_values *view = NULL;
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  capture_put(writer, 1, 65);
  ok(ps_metadata_repack(writer, &e), &e);
  capture_put(writer, 2, 66);
  ok(ps_metadata_repack(writer, &e), &e);
  ok(ps_open(path, 0, &options, &peer, &e), &e);
  if (!explicit_sweep) {
    ok(ps_values_capture(peer, key(1).owner, PS_STATE, PS_STATE, &view, &e),
       &e);
    REQUIRE(ps_values_count(view) == 2);
  }
  /* A third repack selects the first image's two pages. The peer still
   * sees those pages as a quarantined interval in its resident projection. */
  capture_put(writer, 3, 67);
  ok(ps_metadata_repack(writer, &e), &e);
  if (explicit_sweep)
    ok(ps_checkpoint(peer, &e), &e);
  else
    ps_values_close(view);
  ok(ps_refresh(peer, &e), &e);
  capture_put(peer, 4, 68);
  ok(ps_checkpoint(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_close(peer, &e), &e);
  ok(ps_open(path, 0, &options, &writer, &e), &e);
  for (uint64_t id = 1; id <= 4; ++id) {
    ps_reader *reader = NULL;
    ok(ps_read_open(writer, key(id), NULL, &reader, &e), &e);
    captured_expect(reader, (unsigned char)id, 64 + id);
    ps_reader_close(reader);
  }
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
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
  for (int phase = 2; phase <= 5; ++phase) {
    lc_error e = {0};
    ps_space *s = NULL, *join = NULL;
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
    if (phase == 5) {
      ok(ps_open(path, 0, NULL, &join, &e), &e);
      REQUIRE(status(join).syncs == 1);
    }
    ok(ps_read_open(s, key(1), NULL, &current, &e), &e);
    REQUIRE(status(s).syncs == before.syncs + (phase == 5 ? 0 : 1));
    captured_expect(old, 1, 65);
    captured_expect(current, 1, 1000);
    ps_reader_close(old);
    ps_reader_close(current);
    ok(ps_close(join, &e), &e);
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
  ps_coordination_guard *guard = NULL;
  uint64_t headroom;
  int pressure;
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  capture_put(a, 1, 65);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_maintenance_pressure(a, 0, &pressure, &headroom, &e), &e);
  REQUIRE(!pressure && !headroom);
  ok(ps_maintenance_pressure(a, 1, &pressure, &headroom, &e), &e);
  REQUIRE(!pressure && headroom > 7 * PS_MIB);
  REQUIRE(!ps_readiness_activity(a) && !ps_readiness_activity(b));
  ok(ps_budget_open(a, &plan, &funded, &e), &e);
  ps_writer *private = private_value(a, funded, 9000);
  ok(ps_budget_open(b, &occupied, &padding, &e), &e);
  occupied.payload_bytes = 4 * PS_MIB;
  ok(ps_budget_resize(padding, &occupied, &e), &e);
  ok(ps_maintenance_pressure(a, 0, &pressure, &headroom, &e), &e);
  REQUIRE(pressure && headroom > 5 * PS_MIB && headroom < 6 * PS_MIB);
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
  ok(ps_coordination_lock(ps_space_coordination(b), PS_COORDINATION_ALLOCATION,
                          0, 1, &guard, &e),
     &e);
  REQUIRE(ps_maintenance_pressure(a, 0, &pressure, &headroom, &e) ==
              LC_ERR_PENDING &&
          pressure && !headroom);
  ps_readiness_note_mutation(a);
  ps_readiness_note_mutation(b);
  REQUIRE(ps_readiness_activity(a) == 2 && ps_readiness_activity(b) == 2);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  ok(ps_maintenance_pressure(a, 0, &pressure, &headroom, &e), &e);
  REQUIRE(pressure && headroom < 2 * PS_MIB);
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
  ok(ps_maintenance_pressure(b, 0, &pressure, &headroom, &e), &e);
  REQUIRE(!pressure && headroom > 7 * PS_MIB);
  ok(ps_maintenance_pressure(b, 0, &pressure, &headroom, &e), &e);
  REQUIRE(!pressure && !headroom);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  REQUIRE(!ps_readiness_activity(a));
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
  int hold_leadership, fail_sync;
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
  if (g->fail_sync) {
    errno = EIO;
    return -1;
  }
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
      (command == F_OFD_SETLKW || command == F_OFD_SETLK)) {
    pthread_mutex_lock(&g->mutex);
    g->attempted = 1;
    pthread_cond_broadcast(&g->cond);
    while (g->hold_leadership && !g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
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
static void join_durability_waiter(pthread_t thread) {
  struct timespec deadline;
  REQUIRE(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 3;
  REQUIRE(!pthread_timedjoin_np(thread, NULL, &deadline));
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
  records[selected + 1] = UINT64_C(0x3359444145525350);
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
typedef struct resident_names {
  ps_catalog_info owner;
  size_t count;
} resident_names;
static int count_namespace(void *context, const void *name, size_t length,
                           const ps_catalog_info *info, lc_error *e) {
  resident_names *names = context;
  (void)e;
  REQUIRE(length == 8 && !memcmp(name, "resident", length) &&
          info->id == names->owner.id &&
          info->revision.incarnation == names->owner.revision.incarnation);
  ++names->count;
  return LC_OK;
}
static void certified_resident_observation(int durable) {
  lc_error e = {0};
  readiness_fault fault = {3, EAGAIN, 0, 0, 1};
  ps_coordination_io io = {&fault, readiness_lock};
  ps_options options = {0}, peer_options = {0};
  ps_coordination_guard *namespace = NULL, *allocator = NULL;
  ps_space *a = NULL, *b = NULL;
  resident_names names = {0};
  uint64_t owner;
  ps_key artifact = {88, 1, PS_QUERY_FIRST};
  ps_ref original, replacement;
  uint64_t bytes;
  int continued, present, valid;
  options.durable_sync = durable;
  options.coordination_io = &io;
  peer_options.durable_sync = 1;
  ok(ps_open(path, 1, &options, &b, &e), &e);
  ok(ps_catalog_create_namespace(b, "resident", 8, &names.owner, &e), &e);
  owner = artifact.owner = names.owner.id;
  capture_put_key(b, artifact, PS_PAGE);
  ok(ps_flush(b, &e), &e);
  ok(ps_open(path, 0, &peer_options, &a, &e), &e);
  ok(ps_coordination_lock(b->coordination, PS_COORDINATION_NAMESPACE, owner, 0,
                          &namespace, &e),
     &e);
  ok(ps_internal_namespace_stamp(b, owner, &continued, &e), &e);
  REQUIRE(!continued);
  ok(ps_value_info(b, artifact, &original, &bytes, &present, &e), &e);
  REQUIRE(present && bytes == PS_PAGE);
  ok(ps_coordination_lock(a->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                          &allocator, &e),
     &e);
  /* Fail an accidental wait so that a regression cannot hang this test. */
  fault.armed = 1;
  ok(ps_internal_namespace_stamp(b, owner, &continued, &e), &e);
  REQUIRE(continued);
  ok(ps_internal_namespace_checkpoint_stabilize(b, owner, &e), &e);
  ok(ps_catalog_visit(b, 0, PS_CATALOG_NAMESPACE, count_namespace, &names, &e),
     &e);
  REQUIRE(names.count == 1);
  ok(ps_value_validate(b, artifact, &original, &valid, &e), &e);
  REQUIRE(valid && !fault.calls);
  /* A pending transition denies the lock-free path even if the record still
   * names the resident prefix. */
  ps_readiness_begin(a, PS_READINESS_RECOVERY);
  REQUIRE(ps_value_validate(b, artifact, &original, &valid, &e) ==
              LC_ERR_PENDING &&
          !valid && fault.calls == 1);
  lc_error_cleanup(&e);
  ps_readiness_finish(a, PS_READINESS_RECOVERY);
  ps_coordination_unlock(allocator);
  ps_coordination_unlock(namespace);

  /* The old resident cache cannot validate a displaced incarnation. The
   * first check of the new payload has to acquire a physical witness. */
  fault.nth = SIZE_MAX;
  fault.calls = 0;
  ps_writer *w = replay_value(a, artifact, 42, 2 * PS_PAGE, &e);
  ok(ps_revision_publish(&w, 1, 0, PS_COMMIT_DEFER_SYNC, &replacement, NULL,
                         &e),
     &e);
  ps_writer_close(w);
  ok(ps_value_validate(b, artifact, &original, &valid, &e), &e);
  REQUIRE(valid && fault.calls == 1);
  ok(ps_value_validate(b, artifact, &original, &valid, &e), &e);
  REQUIRE(valid && fault.calls == 1);
  ok(ps_flush(a, &e), &e);
  ok(ps_value_validate(b, artifact, &original, &valid, &e), &e);
  REQUIRE(!valid && fault.calls == 2);
  ok(ps_value_validate(b, artifact, &replacement, &valid, &e), &e);
  REQUIRE(valid && fault.calls == 3);
  ok(ps_value_validate(b, artifact, &replacement, &valid, &e), &e);
  REQUIRE(valid && fault.calls == 3);
  fault.armed = 0;
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void fragmented_preparation(void) {
  lc_error e = {0};
  ps_space *a = NULL, *b = NULL;
  ps_owned_head *x = NULL, *y = NULL;
  ps_owned_status ix, iy;
  const ps_key hole = key(51), separator = key(52);
  const ps_key kx = {89, 1, PS_STREAM}, ky = {89, 2, PS_STREAM};
  ok(ps_open(path, 1, NULL, &a, &e), &e);
  ps_writer *w = replay_value(a, hole, 0x51, PS_PAGE, &e);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  w = replay_value(a, separator, 0x52, PS_PAGE, &e);
  ok(ps_publish(w, NULL, &e), &e);
  ps_writer_close(w);
  uint64_t low = ps_internal_lookup(a, hole)->extents[0].offset;
  REQUIRE(ps_internal_lookup(a, separator)->extents[0].offset == low + PS_PAGE);
  ok(ps_delete(a, hole, &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  ok(ps_owned_open(a, NULL, kx, 1, NULL, &x, &ix, &e), &e);
  ok(ps_owned_open(b, NULL, ky, 1, NULL, &y, &iy, &e), &e);
  /* The low dirty hole fits this grant exactly. A wider dirty run can
   * prepare the same grant and a usable peer grant in one barrier. */
  uint64_t first = expect_zero_grant(a, NULL, x, &ix, 0, 0, 1);
  expect_zero_grant(b, NULL, y, &iy, 0, first + PS_PAGE, 0);
  REQUIRE(first > low + PS_PAGE);
  REQUIRE(!prepared_prefix(b, low, PS_PAGE));
  expect_value(b, separator, PS_PAGE, 0x52);
  ps_owned_close(x);
  ps_owned_close(y);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct checkpoint_preparation_io {
  uint64_t offsets[16], lengths[16], bytes;
  size_t ranges, syncs;
  int fail_zero, fail_barrier;
} checkpoint_preparation_io;
static int checkpoint_zero(void *context, int fd, off_t offset, off_t bytes) {
  static const unsigned char zero[PS_PAGE];
  checkpoint_preparation_io *io = context;
  REQUIRE(offset >= PS_DATA && !(offset % PS_PAGE) && bytes > 0 &&
          !(bytes % PS_PAGE) && io->ranges < 16);
  io->offsets[io->ranges] = (uint64_t)offset;
  io->lengths[io->ranges++] = (uint64_t)bytes;
  io->bytes += (uint64_t)bytes;
  for (off_t at = 0; at < bytes; at += PS_PAGE) {
    REQUIRE(pwrite(fd, zero, sizeof(zero), offset + at) == sizeof(zero));
    if (io->fail_zero) {
      errno = EIO;
      return -1;
    }
  }
  return 0;
}
static int checkpoint_preparation_sync(void *context, int fd) {
  checkpoint_preparation_io *io = context;
  ++io->syncs;
  if (io->fail_barrier && io->ranges) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
enum checkpoint_preparation_case {
  CHECKPOINT_PREPARE,
  CHECKPOINT_ZERO_FAILURE,
  CHECKPOINT_BARRIER_FAILURE
};
static void checkpoint_preparation(enum checkpoint_preparation_case outcome) {
  const ps_key stream = {89, 1, PS_STREAM};
  checkpoint_preparation_io probe = {0};
  ps_io io = {&probe,     read_io,        write_io, checkpoint_preparation_sync,
              reserve_io, checkpoint_zero};
  ps_options options = {.io = &io};
  ps_space *writer = NULL, *peer = NULL;
  ps_reader *held = NULL;
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_coordination_guard *guard = NULL;
  lc_error e = {0};
  uint64_t base, syncs;

  ok(ps_open(path, 1, &options, &writer, &e), &e);
  for (uint64_t id = 1; id <= 24; ++id)
    capture_put(writer, id, PS_STREAM_PIECE_BYTES);
  ok(ps_checkpoint(writer, &e), &e);
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  ok(ps_read_open(peer, key(2), NULL, &held, &e), &e);
  for (uint64_t id = 2; id <= 24; id += 2)
    ok(ps_delete(writer, key(id), &e), &e);
  ok(ps_checkpoint(writer, &e), &e);
  REQUIRE(!probe.ranges);
  capture_put(writer, 42, 65);
  REQUIRE(!pthread_mutex_lock(&writer->mutex));
  ok(ps_internal_capture_begin(writer, &guard, &e), &e);
  ps_readiness_set_pressure(writer, 1);
  ok(ps_internal_capture_end(writer, guard, LC_OK, &e), &e);
  REQUIRE(!pthread_mutex_unlock(&writer->mutex));

  probe.fail_zero = outcome == CHECKPOINT_ZERO_FAILURE;
  probe.fail_barrier = outcome == CHECKPOINT_BARRIER_FAILURE;
  syncs = probe.syncs;
  int rc = ps_checkpoint(writer, &e);
  if (probe.fail_barrier) {
    REQUIRE(rc == LC_ERR_TRANSPORT && probe.syncs == syncs + 1);
    lc_error_cleanup(&e);
  } else {
    ok(rc, &e);
    REQUIRE(probe.syncs == syncs + 3);
  }
  REQUIRE(probe.ranges && probe.bytes <= PS_MIB);
  REQUIRE(probe.fail_zero ? probe.ranges == 1 : probe.ranges > 1);
  for (size_t i = 0; i < probe.ranges; ++i)
    REQUIRE(prepared_prefix(peer, probe.offsets[i], probe.lengths[i]) ==
            (outcome == CHECKPOINT_PREPARE ? probe.lengths[i] : 0));
  captured_expect(held, 2, PS_STREAM_PIECE_BYTES);
  ps_reader_close(held);
  ok(ps_close(writer, &e), &e);
  ok(ps_owned_open(peer, NULL, stream, 1, NULL, &head, &info, &e), &e);
  if (outcome == CHECKPOINT_PREPARE) {
    base = expect_zero_grant(peer, NULL, head, &info, 0, probe.offsets[0], 0);
    REQUIRE(!prepared_prefix(peer, base, PS_PAGE));
  } else {
    expect_zero_grant(peer, NULL, head, &info, 0, 0, 1);
  }
  ps_owned_close(head);
  ok(ps_checkpoint(peer, &e), &e);
  ok(ps_close(peer, &e), &e);
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  REQUIRE(!prepared_prefix(peer, probe.offsets[0], probe.lengths[0]));
  expect_value(peer, key(1), PS_STREAM_PIECE_BYTES, 1);
  ok(ps_close(peer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct reopen_disk {
  size_t syncs, fail_sync;
} reopen_disk;
static int reopen_sync(void *context, int fd) {
  reopen_disk *disk = context;
  if (++disk->syncs == disk->fail_sync) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
static void reopen_after_checkpoint(void) {
  for (int durable = 0; durable <= 1; ++durable) {
    reopen_disk disk = {0};
    ps_io io = {&disk, read_io, durable_write, reopen_sync, reserve_io, NULL};
    ps_options options = {.durable_sync = durable, .io = &io};
    ps_space *writer = NULL, *peer = NULL, *join = NULL;
    lc_error e = {0};
    ok(ps_open(path, 1, &options, &writer, &e), &e);
    capture_put(writer, 1, 65);
    ok(ps_flush(writer, &e), &e);
    ok(ps_open(path, 0, &options, &peer, &e), &e);
    disk.syncs = 0;
    ok(ps_checkpoint(writer, &e), &e);
    REQUIRE(disk.syncs == 3);

    disk.syncs = 0;
    ok(ps_open(path, 0, &options, &join, &e), &e);
    REQUIRE(!disk.syncs);
    expect_value(join, key(1), 65, 1);
    ok(ps_refresh(peer, &e), &e);
    REQUIRE(!disk.syncs);
    expect_value(peer, key(1), 65, 1);
    capture_put(join, 2, 65);
    ok(ps_flush(join, &e), &e);
    expect_value(writer, key(2), 65, 2);
    ok(ps_close(peer, &e), &e);
    ok(ps_close(join, &e), &e);
    ok(ps_close(writer, &e), &e);

    ok(ps_open(path, 0, &options, &join, &e), &e);
    expect_value(join, key(1), 65, 1);
    expect_value(join, key(2), 65, 2);
    ok(ps_close(join, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
  }
}
static void reopen_cohort(void) {
  lc_error e = {0};
  reopen_disk disk = {0};
  ps_io io = {&disk, read_io, durable_write, reopen_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *first = NULL, *join = NULL;
  ps_ref before, after;
  ps_coordination_guard *guard = NULL;
  ps_budget_plan plan = {PS_PAGE, 0, 0, 0};
  ps_budget *budget = NULL;
  unsigned char byte = 0xa5;
  reopen_after_checkpoint();
  options.io = &io;
  ok(ps_open(path, 1, NULL, &first, &e), &e);
  capture_put(first, 1, 65);
  ok(ps_flush(first, &e), &e);

  ps_readiness_certificate cert = readiness(first);
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(!disk.syncs && readiness(first).ready == cert.ready &&
          readiness(first).durable == cert.durable);
  ps_writer *writer = replay_value(join, key(2), 2, 65, &e);
  ok(ps_publish(writer, &before, &e), &e);
  ps_writer_close(writer);
  ok(ps_close(join, &e), &e);
  join = NULL;

  /* A failed attempt to stabilize published data cannot assign a range or
   * certify that data. Successful recovery needs only that one barrier. */
  cert = readiness(first);
  REQUIRE(cert.ready > cert.durable);
  disk = (reopen_disk){0, 1};
  REQUIRE(ps_open(path, 0, &options, &join, &e) == LC_ERR_TRANSPORT && !join);
  lc_error_cleanup(&e);
  REQUIRE(disk.syncs == 1 && readiness(first).durable == cert.durable &&
          readiness(first).ready == cert.ready &&
          readiness(first).grant_used == cert.grant_used);
  disk = (reopen_disk){0};
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(disk.syncs == 1 && readiness(first).durable == cert.ready);
  expect_value(join, key(2), 65, 2);
  ok(ps_close(join, &e), &e);
  join = NULL;

  /* Each reopen burns a private range even if the owner publishes nothing.
   * Refill needs a durable grant only when the live pool runs out. */
  cert = readiness(first);
  for (uint64_t used = cert.grant_used; used < cert.grant_limit;
       used += TOKEN_RANGE) {
    disk = (reopen_disk){0};
    ok(ps_open(path, 0, &options, &join, &e), &e);
    REQUIRE(!disk.syncs && join->token_next == used + 1 &&
            join->token_end == used + TOKEN_RANGE);
    ok(ps_close(join, &e), &e);
    join = NULL;
  }
  cert = readiness(first);
  disk = (reopen_disk){0, 1};
  REQUIRE(ps_open(path, 0, &options, &join, &e) == LC_ERR_TRANSPORT && !join);
  lc_error_cleanup(&e);
  REQUIRE(disk.syncs == 1 && readiness(first).durable == cert.durable &&
          readiness(first).ready == cert.ready &&
          readiness(first).grant_limit == cert.grant_limit);
  disk = (reopen_disk){0};
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(disk.syncs == 1 && join->token_next > cert.grant_limit &&
          readiness(first).ready == cert.ready);
  /* An existing allocator can adopt the grant without changing its own
   * range or replaying a synthetic journal event. */
  uint64_t prior_end = first->token_end;
  ok(ps_budget_open(first, &plan, &budget, &e), &e);
  REQUIRE(first->token_limit == join->token_limit &&
          first->token_end == prior_end);
  ok(ps_budget_close(budget, &e), &e);
  budget = NULL;
  ok(ps_close(join, &e), &e);
  join = NULL;

  /* Leave a rejected suffix on a different page from the next grant.
   * Interrupt recovery after the clear and before its first barrier. */
  ok(ps_refresh(first, &e), &e);
  ok(ps_coordination_lock(first->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                          &guard, &e),
     &e);
  ps_readiness_begin(first, PS_READINESS_JOURNAL);
  off_t suffix = PS_JOURNAL_OFFSET + PS_JOURNAL - 1;
  REQUIRE(pwrite(first->fd, &byte, 1, suffix) == 1);
  REQUIRE(!fdatasync(first->fd));
  ps_coordination_unlock(guard);
  int ready[2], child_status;
  REQUIRE(!pipe(ready));
  pid_t pid = fork();
  REQUIRE(pid >= 0);
  if (!pid) {
    close(ready[0]);
    projection_io stopped = {.armed = 1, .phase = 1, .ready = ready[1]};
    ps_io child_io = {&stopped,        projection_read, projection_write,
                      projection_sync, reserve_io,      NULL};
    options.io = &child_io;
    (void)ps_open(path, 0, &options, &join, &e);
    _exit(1);
  }
  REQUIRE(!close(ready[1]));
  char signal;
  REQUIRE(read(ready[0], &signal, 1) == 1 && signal == 'R');
  REQUIRE(!kill(pid, SIGKILL) && waitpid(pid, &child_status, 0) == pid &&
          WIFSIGNALED(child_status) && WTERMSIG(child_status) == SIGKILL);
  REQUIRE(!close(ready[0]));
  REQUIRE(pread(first->fd, &byte, 1, suffix) == 1 && byte == 0);
  REQUIRE(ps_checkpoint(first, &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
  REQUIRE(ps_budget_open(first, &plan, &budget, &e) == LC_ERR_PENDING &&
          !budget);
  lc_error_cleanup(&e);
  disk = (reopen_disk){0};
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(disk.syncs == 1 && !readiness(first).pending);
  ok(ps_budget_open(first, &plan, &budget, &e), &e);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_close(first, &e), &e);
  ok(ps_close(join, &e), &e);
  join = NULL;

  disk = (reopen_disk){0};
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(disk.syncs == 1);
  expect_value(join, key(1), 65, 1);
  expect_value(join, key(2), 65, 2);
  writer = replay_value(join, key(3), 3, 65, &e);
  ok(ps_publish(writer, &after, &e), &e);
  REQUIRE(after.incarnation > before.incarnation);
  ps_writer_close(writer);

  /* Appended data needs one barrier even when the journal has stayed in the
   * same scrubbed generation since the cold open. */
  cert = readiness(join);
  REQUIRE(cert.ready > cert.durable);
  disk.syncs = 0;
  ok(ps_open(path, 0, &options, &first, &e), &e);
  REQUIRE(disk.syncs == 1);
  expect_value(first, key(3), 65, 3);
  ok(ps_close(join, &e), &e);
  join = NULL;
  ok(ps_checkpoint(first, &e), &e);
  capture_put(first, 4, 65);
  disk.syncs = 0;
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(disk.syncs == 1);
  expect_value(join, key(4), 65, 4);
  ps_key stream = {88, 115, PS_STREAM};
  ps_owned_head *head = NULL;
  ps_owned_status status = {0};
  int continued;
  ok(ps_owned_open(first, NULL, stream, 1, NULL, &head, &status, &e), &e);
  ps_owned_close(head);
  ok(ps_checkpoint(first, &e), &e);
  ok(ps_close(first, &e), &e);
  ok(ps_close(join, &e), &e);
  ok(ps_open(path, 0, NULL, &first, &e), &e);
  ok(ps_owned_open(first, NULL, stream, 0, NULL, &head, &status, &e), &e);
  ok(ps_owned_edit_begin(head, 0, &guard, &status, &continued, &e), &e);
  REQUIRE(!continued);
  ok(ps_owned_edit_end(head, guard, LC_OK, &e), &e);
  /* An ownership-only hint can remain volatile through the join. An explicit
   * flush on that joiner must still synchronize the observed prefix. */
  cert = readiness(first);
  REQUIRE(cert.ready > cert.durable);
  disk = (reopen_disk){0};
  ok(ps_open(path, 0, &options, &join, &e), &e);
  REQUIRE(!disk.syncs && readiness(join).durable == cert.durable);
  ok(ps_flush(join, &e), &e);
  REQUIRE(disk.syncs == 1 && readiness(first).durable == cert.ready);
  ps_owned_close(head);
  ok(ps_close(first, &e), &e);
  ok(ps_close(join, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
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
typedef struct witness_queries {
  size_t payload;
  size_t large;
} witness_queries;
static int count_payload_queries(void *context, int fd, int command,
                                 struct flock *lock) {
  witness_queries *queries = context;
  if (command == F_OFD_GETLK && lock->l_start >= PS_DATA) {
    ++queries->payload;
    if (lock->l_len >= (off_t)PS_MIB)
      ++queries->large;
  }
  return fcntl(fd, command, lock);
}
static void witness_filter(void) {
  witness_queries queries = {0};
  size_t ticket_bytes;
  uint64_t retired;
  ps_coordination_io io = {&queries, count_payload_queries};
  ps_options options = {.coordination_io = &io};
  ps_budget_plan plan = {2 * PS_PAGE};
  ps_budget *budget = NULL;
  ps_space *a = NULL, *b = NULL;
  ps_reader *reader = NULL, *imported = NULL;
  unsigned char *ticket;
  lc_error e = {0};

  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  capture_put(a, 42, PS_PAGE);
  /* Keep a member witness on allocated data while scanning common pages. */
  ok(ps_read_open(b, key(42), NULL, &reader, &e), &e);
  ok(ps_budget_foreground_check(b, &e), &e);
  ok(ps_budget_foreground_check(a, &e), &e);
  retired = status(a).retired_bytes;
  queries = (witness_queries){0};
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(!queries.large);

  /* A new private reservation can exclude pages a peer just skipped. */
  ok(ps_budget_open(b, &plan, &budget, &e), &e);
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(queries.payload &&
          status(a).retired_bytes == retired + plan.payload_bytes);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(status(a).retired_bytes == retired);
  queries = (witness_queries){0};
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(!queries.large);

  ticket_bytes = ps_reader_ticket_bytes(reader);
  ticket = malloc(ticket_bytes);
  REQUIRE(ticket);
  ok(ps_reader_ticket(reader, ticket, ticket_bytes, &e), &e);
  queries = (witness_queries){0};
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(!queries.large);
  ok(ps_reader_import(path, ticket, ticket_bytes, &imported, &e), &e);
  free(ticket);
  ps_reader_close(reader);
  queries = (witness_queries){0};
  ok(ps_budget_foreground_check(a, &e), &e);
  REQUIRE(queries.large); /* The read-only importer requires a full scan. */
  ok(ps_delete(a, key(42), &e), &e);
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);

  /* The imported OFD outlives every member and the volatile page map. */
  ok(ps_open(path, 0, &options, &a, &e), &e);
  REQUIRE(status(a).retired_bytes >= PS_PAGE);
  captured_expect(imported, 42, PS_PAGE);
  ps_reader_close(imported);
  ok(ps_checkpoint(a, &e), &e);
  REQUIRE(!status(a).retired_bytes);
  ok(ps_close(a, &e), &e);
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
static void journal_replay_retired(void) {
  for (int shape = 0; shape < 3; ++shape) {
    size_t old_bytes = shape == 0 ? PS_CHUNK : 65;
    size_t new_bytes = shape == 2 ? PS_PAGE : old_bytes;
    ps_space *a = NULL, *b = NULL;
    ps_reader *held = NULL;
    lc_error e = {0};
    ok(ps_open(path, 1, NULL, &a, &e), &e);
    ok(ps_open(path, 0, NULL, &b, &e), &e);
    capture_put(a, 1, old_bytes);
    ok(ps_checkpoint(a, &e), &e);
    ps_entry *original = ps_internal_lookup(a, key(1));
    REQUIRE(original && original->count == 1);
    uint64_t offset = original->extents[0].offset;
    ok(ps_read_open(b, key(1), NULL, &held, &e), &e);
    ok(ps_delete(a, key(1), &e), &e);
    ok(ps_checkpoint(a, &e), &e);
    uint64_t generation = a->generation;
    REQUIRE(status(a).retired_bytes >= old_bytes);
    ok(ps_refresh(b, &e), &e);
    ps_budget_plan plan = {0};
    ps_budget *budget = NULL;
    ps_writer *writer = NULL;
    unsigned char chunk[1024];
    ok(ps_budget_plan_value(&plan, new_bytes, &e), &e);
    ok(ps_budget_open(b, &plan, &budget, &e), &e);
    ok(ps_begin(b, budget, key(2), &writer, &e), &e);
    captured_expect(held, 1, old_bytes);
    ps_reader_close(held);

    /* B reuses the exact physical range within the same checkpoint generation.
     * A's earlier quarantine must admit this certified journal frame. */
    memset(chunk, 2, sizeof(chunk));
    for (size_t left = new_bytes; left;) {
      size_t n = left < sizeof(chunk) ? left : sizeof(chunk);
      ok(ps_write(writer, chunk, n, &e), &e);
      left -= n;
    }
    ok(ps_publish(writer, NULL, &e), &e);
    ps_writer_close(writer);
    ok(ps_budget_close(budget, &e), &e);
    ok(ps_flush(b, &e), &e);
    ps_entry *published = ps_internal_lookup(b, key(2));
    if (published && published->extents[0].offset != offset)
      fprintf(stderr, "shape=%d old=%lu new=%lu\n", shape,
              (unsigned long)offset,
              (unsigned long)published->extents[0].offset);
    REQUIRE(published && published->count == 1 &&
            published->extents[0].offset == offset);
    REQUIRE(b->generation == generation);
    expect_value(a, key(2), new_bytes, 2);
    REQUIRE(a->generation == generation);
    expect_value(a, key(1), 0, 0);
    ok(ps_close(b, &e), &e);
    ok(ps_close(a, &e), &e);
    REQUIRE(!coverage(NULL, 0) && !unlink(path));
    lc_error_cleanup(&e);
  }
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
  ps_writer *residue_writer = replay_value(b, key(7), 0, 0, &e);
  ok(ps_publish(residue_writer, NULL, &e), &e);
  ps_writer_close(residue_writer);
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
  /* A valid previous-generation frame can only appear at the journal start.
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
typedef struct grant_work {
  ps_owned_head *head;
  ps_owned_run *run;
} grant_work;
static void *prepare_grant(void *context) {
  grant_work *work = context;
  lc_error e = {0};
  int found;
  ok(ps_owned_grant(work->head, NULL, 0, PS_PAGE, &work->run, &found, &e), &e);
  REQUIRE(found);
  return NULL;
}
enum preparation_barrier {
  READY_BEFORE_SYNC,
  READY_DURING_SYNC,
  FAILED_SYNC,
  READY_BEFORE_CHECKPOINT
};
static void shared_preparation(enum preparation_barrier when) {
  durable_gate leader = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  durable_gate writer = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io ai = {&leader, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_io bi = {&writer, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_coordination_io coordinator = {&writer, durable_attempt};
  ps_options ao = {.io = &ai},
             bo = {.io = &bi, .coordination_io = &coordinator};
  const ps_key stream = {88, 1, PS_STREAM};
  ps_space *a = NULL, *b = NULL;
  ps_owned_status info, observed;
  grant_work grant = {0};
  pthread_t sync_thread, grant_thread;
  lc_error e = {0};
  ok(ps_open(path, 1, &bo, &b, &e), &e);
  ok(ps_owned_open(b, NULL, stream, 1, NULL, &grant.head, &info, &e), &e);
  ok(ps_flush(b, &e), &e);
  ok(ps_open(path, 0, &ao, &a, &e), &e);
  capture_put(a, 1, 65);
  uint64_t a_syncs = status(a).syncs, b_syncs = status(b).syncs;
  durable_work flush = {a, &leader};
  if (when == READY_DURING_SYNC) {
    leader.paused = 1;
    REQUIRE(!pthread_create(&sync_thread, NULL, durable_flush, &flush));
    wait_catalog_sync(&leader);
  }
  writer.attempted = 0;
  writer.hold_leadership = 1;
  REQUIRE(!pthread_create(&grant_thread, NULL, prepare_grant, &grant));
  pthread_mutex_lock(&writer.mutex);
  while (!writer.attempted)
    pthread_cond_wait(&writer.cond, &writer.mutex);
  pthread_mutex_unlock(&writer.mutex);
  ok(ps_owned_inspect(a, stream, &info.ref, &observed, &e), &e);
  REQUIRE(!observed.runs && status(b).syncs == b_syncs);

  if (when == READY_DURING_SYNC)
    finish_catalog_sync(&leader, sync_thread);
  else if (when == FAILED_SYNC) {
    leader.fail_sync = 1;
    REQUIRE(ps_flush(a, &e) == LC_ERR_TRANSPORT);
    lc_error_cleanup(&e);
  } else if (when == READY_BEFORE_CHECKPOINT)
    ok(ps_checkpoint(a, &e), &e);
  else
    ok(ps_flush(a, &e), &e);
  REQUIRE(status(a).syncs ==
          a_syncs + (when == READY_BEFORE_CHECKPOINT ? 3 : 1));
  uint64_t later = 0;
  if (when == READY_BEFORE_SYNC) {
    /* A later local frame can arrive while the first preparation is waiting
     * to consume the certificate. It still needs its own durability proof. */
    capture_put(b, 2, 67);
    later = __atomic_load_n(&b->pending_sequence, __ATOMIC_ACQUIRE);
  }
  finish_catalog_sync(&writer, grant_thread);
  REQUIRE(status(b).syncs ==
          b_syncs + (when == READY_DURING_SYNC || when == FAILED_SYNC));
  REQUIRE(!later ||
          __atomic_load_n(&b->durable_sequence, __ATOMIC_ACQUIRE) < later);

  unsigned char marker = 0x6a, byte = 0;
  ok(ps_owned_write(grant.run, 0, &marker, 1, &e), &e);
  ok(ps_owned_commit(grant.head, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_owned_run_close(grant.run);
  ps_owned_close(grant.head);
  ok(ps_close(a, &e), &e);
  ok(ps_checkpoint(b, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &b, &e), &e);
  REQUIRE(!ps_readiness_issued_tickets(b) && !readiness(b).prepared_durable);
  ok(ps_owned_open(b, NULL, stream, 0, &info.ref, &grant.head, &observed, &e),
     &e);
  int found;
  ok(ps_owned_grant(grant.head, NULL, 0, 0, &grant.run, &found, &e), &e);
  REQUIRE(found && observed.runs == 1 && observed.tail == 1);
  ok(ps_owned_read(grant.run, 0, &byte, 1, &e), &e);
  REQUIRE(byte == marker);
  ps_owned_run_close(grant.run);
  ps_owned_close(grant.head);
  ok(ps_close(b, &e), &e);
  REQUIRE(!coverage(NULL, 0) && unlink(path) == 0);
  REQUIRE(!pthread_cond_destroy(&leader.cond) &&
          !pthread_mutex_destroy(&leader.mutex));
  REQUIRE(!pthread_cond_destroy(&writer.cond) &&
          !pthread_mutex_destroy(&writer.mutex));
}
static int observe_sync_conflict(void *context, int fd, int command,
                                 struct flock *lock) {
  durable_gate *gate = context;
  int rc, reason;
  if (command == F_OFD_SETLKW)
    return durable_attempt(context, fd, command, lock);
  rc = fcntl(fd, command, lock);
  reason = errno;
  if (rc < 0 && (reason == EAGAIN || reason == EACCES) &&
      command == F_OFD_SETLK && lock->l_start == 5 && lock->l_type == F_WRLCK) {
    pthread_mutex_lock(&gate->mutex);
    gate->attempted = 1;
    pthread_cond_broadcast(&gate->cond);
    pthread_mutex_unlock(&gate->mutex);
  }
  errno = reason;
  return rc;
}
static void shared_preparation_wait(int owner_dies) {
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_coordination_io coordinator = {&gate, observe_sync_conflict};
  ps_options options = {.coordination_io = &coordinator};
  const ps_key stream = {88, 1, PS_STREAM};
  ps_space *writer = NULL, *peer = NULL;
  ps_owned_status info;
  grant_work grant = {0};
  pthread_t thread;
  lc_error e = {0};
  int ready[2], lifetime[2], status_code, found;
  unsigned char marker = 0x6a, byte = 0;

  ok(ps_open(path, 1, &options, &writer, &e), &e);
  ok(ps_owned_open(writer, NULL, stream, 1, NULL, &grant.head, &info, &e), &e);
  ok(ps_flush(writer, &e), &e);
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  capture_put(peer, 1, 65);
  REQUIRE(!pipe(ready) && !pipe(lifetime));
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    struct flock lock = {
        .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 5, .l_len = 1};
    REQUIRE(!close(ready[0]) && !close(lifetime[1]) && !close(writer->fd) &&
            !close(peer->fd));
    int fd = open(path, O_RDWR | O_CLOEXEC);
    REQUIRE(fd >= 0 && fcntl(fd, F_OFD_SETLK, &lock) == 0 &&
            write(ready[1], "L", 1) == 1);
    REQUIRE(read(lifetime[0], &byte, 1) == 0);
    _exit(0);
  }
  REQUIRE(!close(ready[1]) && !close(lifetime[0]) &&
          read(ready[0], &byte, 1) == 1 && byte == 'L' && !close(ready[0]));
  uint64_t syncs = status(writer).syncs;
  gate.attempted = 0;
  REQUIRE(!pthread_create(&thread, NULL, prepare_grant, &grant));
  pthread_mutex_lock(&gate.mutex);
  while (!gate.attempted)
    pthread_cond_wait(&gate.cond, &gate.mutex);
  pthread_mutex_unlock(&gate.mutex);
  uint64_t ticket = ps_readiness_issued_tickets(writer);
  REQUIRE(ticket > readiness(peer).prepared_durable);
  if (owner_dies)
    REQUIRE(!kill(child, SIGKILL));
  else
    ok(ps_checkpoint(peer, &e), &e);
  /* Structural completion can satisfy the waiter while the OFD leader is
   * still alive. A killed leader releases the OFD without sending a wake. */
  join_durability_waiter(thread);
  if (!owner_dies)
    REQUIRE(!kill(child, SIGKILL));
  REQUIRE(waitpid(child, &status_code, 0) == child &&
          WIFSIGNALED(status_code) && WTERMSIG(status_code) == SIGKILL &&
          !close(lifetime[1]));
  REQUIRE(status(writer).syncs == syncs + (uint64_t)owner_dies &&
          readiness(peer).prepared_durable >= ticket);

  ok(ps_owned_write(grant.run, 0, &marker, 1, &e), &e);
  ok(ps_owned_commit(grant.head, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_owned_run_close(grant.run);
  ps_owned_close(grant.head);
  ok(ps_close(peer, &e), &e);
  ok(ps_checkpoint(writer, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_open(path, 0, NULL, &writer, &e), &e);
  ok(ps_owned_open(writer, NULL, stream, 0, &info.ref, &grant.head, &info, &e),
     &e);
  ok(ps_owned_grant(grant.head, NULL, 0, 0, &grant.run, &found, &e), &e);
  REQUIRE(found && info.tail == 1);
  ok(ps_owned_read(grant.run, 0, &byte, 1, &e), &e);
  REQUIRE(byte == marker);
  ps_owned_run_close(grant.run);
  ps_owned_close(grant.head);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
}
static void preparation_leaves_peer_headroom(void) {
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_coordination_io coordinator = {&gate, durable_attempt};
  ps_options options = {.io = &io, .coordination_io = &coordinator};
  ps_space *writer = NULL, *peer = NULL;
  const ps_key stream = {88, 1, PS_STREAM};
  grant_work grant = {0};
  ps_owned_status status;
  ps_budget *pressure = NULL, *continuation = NULL;
  ps_budget_plan plan = {0};
  pthread_t thread;
  lc_error e = {0};
  uint64_t available;
  int needed;

  ok(ps_open(path, 1, &options, &writer, &e), &e);
  ok(ps_owned_open(writer, NULL, stream, 1, NULL, &grant.head, &status, &e),
     &e);
  ok(ps_flush(writer, &e), &e);
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  ok(ps_maintenance_pressure(peer, 1, &needed, &available, &e), &e);
  REQUIRE(!needed && available > 2 * PS_MIB);
  plan.payload_bytes = available - 2 * PS_MIB;
  ok(ps_budget_open(peer, &plan, &pressure, &e), &e);
  REQUIRE(ps_maintenance_pressure_requested(writer));

  gate.attempted = 0;
  gate.hold_leadership = 1;
  REQUIRE(!pthread_create(&thread, NULL, prepare_grant, &grant));
  pthread_mutex_lock(&gate.mutex);
  while (!gate.attempted)
    pthread_cond_wait(&gate.cond, &gate.mutex);
  pthread_mutex_unlock(&gate.mutex);
  /* The grant's private zero writes cannot be reused while its barrier is
   * held. Another writer still has ordinary headroom for this claim. */
  plan.payload_bytes = 3 * PS_MIB / 2;
  ok(ps_budget_open(peer, &plan, &continuation, &e), &e);
  finish_catalog_sync(&gate, thread);
  ps_owned_run_close(grant.run);
  ps_owned_close(grant.head);
  ok(ps_budget_close(continuation, &e), &e);
  ok(ps_budget_close(pressure, &e), &e);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
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
  uint64_t generation = a->generation;
  ok(ps_catalog_get(a, 0, PS_CATALOG_NAMESPACE, "new", 3, &view, &e), &e);
  REQUIRE(view);
  ps_catalog_view_close(view);
  int continued;
  REQUIRE(ps_internal_namespace_stamp(a, info.id, &continued, &e) ==
          LC_ERR_PENDING);
  lc_error_cleanup(&e);
  ok(ps_budget_open(a, &plan, &budget, &e), &e);
  capture_put(a, 3, 65);
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(a->generation == generation);
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
enum structural_action {
  STRUCTURAL_SEAL,
  STRUCTURAL_REPACK,
  STRUCTURAL_DROP_PIECE,
  STRUCTURAL_EDIT_CATALOG,
  STRUCTURAL_RETIRE
};
typedef struct structural_edit {
  ps_owned_head *head;
  ps_owned_run *old, *replacement;
  ps_space *space;
  ps_ref binding, target;
  ps_key target_key;
  enum structural_action action;
} structural_edit;
static void *publish_structural_edit(void *context) {
  structural_edit *edit = context;
  lc_error e = {0};
  ps_owned_range range = {100, 100};
  if (edit->action == STRUCTURAL_SEAL)
    ok(ps_owned_seal(edit->head, &e), &e);
  else if (edit->action == STRUCTURAL_EDIT_CATALOG) {
    if (edit->target.incarnation) {
      ps_catalog_info result;
      ok(ps_catalog_put(edit->space, NULL, 88, PS_CATALOG_BODY, "stream", 6,
                        &edit->binding, edit->target_key, &edit->target,
                        &result, &e),
         &e);
    } else
      ok(ps_catalog_remove(edit->space, 88, PS_CATALOG_BODY, "stream", 6,
                           &edit->binding, &e),
         &e);
  } else if (edit->action == STRUCTURAL_RETIRE)
    ok(ps_owned_retire(edit->head, &e), &e);
  else
    ok(ps_owned_run_repack(edit->head, edit->old, NULL, &range,
                           edit->action == STRUCTURAL_REPACK, NULL, NULL,
                           &edit->replacement, &e),
       &e);
  lc_error_cleanup(&e);
  return NULL;
}
typedef struct orphan_probe {
  uint64_t incarnation;
  int found;
} orphan_probe;
static int find_orphan(void *context, ps_key key, const ps_ref *ref,
                       lc_error *error) {
  orphan_probe *probe = context;
  (void)key;
  (void)error;
  if (ref->incarnation == probe->incarnation)
    probe->found = 1;
  return LC_OK;
}
static void ready_structural_stream(int rollover) {
  lc_error e = {0};
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *owner = NULL, *peer = NULL;
  ps_owned_head *observer = NULL;
  ps_owned_run *captured = NULL, *current = NULL;
  ps_owned_status info;
  ps_catalog_info binding;
  ps_catalog_view *view = NULL;
  structural_edit edit = {0};
  unsigned char bytes[PS_CHUNK];
  pthread_t thread;
  int found;
  options.io = &io;
  ok(ps_open(path, 1, &options, &owner, &e), &e);
  ok(ps_owned_open(owner, NULL, tail_stream(), 1, NULL, &edit.head, &info, &e),
     &e);
  ps_ref ref = info.ref;
  ok(ps_owned_grant(edit.head, NULL, 0, PS_PAGE, &edit.old, &found, &e), &e);
  REQUIRE(found);
  memset(bytes, 's', sizeof(bytes));
  ok(ps_owned_write(edit.old, 0, bytes, 1000, &e), &e);
  ps_piece_checksum crc;
  ok(ps_owned_piece_extend(edit.old, 0, NULL, 1000, bytes, &crc, &e), &e);
  ok(ps_owned_commit(edit.head, 1000, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ok(ps_flush(owner, &e), &e);
  ok(ps_catalog_put(owner, NULL, 88, PS_CATALOG_BODY, "stream", 6, NULL,
                    tail_stream(), &ref, &binding, &e),
     &e);
  edit.space = owner;
  edit.binding = binding.revision;
  if (rollover) {
    ps_owned_head *next = NULL;
    edit.target_key = tail_stream();
    ++edit.target_key.key;
    ok(ps_owned_open(owner, NULL, edit.target_key, 1, NULL, &next, &info, &e),
       &e);
    edit.target = info.ref;
    ps_owned_close(next);
  }
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  ok(ps_owned_open(peer, NULL, tail_stream(), 0, &ref, &observer, &info, &e),
     &e);
  ok(ps_owned_grant(observer, NULL, 0, 0, &captured, &found, &e), &e);
  REQUIRE(found);
  orphan_probe orphan = {ref.incarnation, 0};
  for (edit.action = STRUCTURAL_SEAL; edit.action <= STRUCTURAL_RETIRE;
       ++edit.action) {
    if (rollover) {
      ok(ps_refresh(owner, &e), &e);
      ok(ps_checkpoint(owner, &e), &e);
    }
    gate.entered = gate.released = 0;
    gate.paused = 1;
    REQUIRE(!pthread_create(&thread, NULL, publish_structural_edit, &edit));
    wait_catalog_sync(&gate);
    uint64_t generation = owner->generation;
    ps_readiness_certificate before = readiness(peer);
    REQUIRE(before.ready > before.durable);
    ok(ps_refresh(peer, &e), &e);
    if (edit.action == STRUCTURAL_EDIT_CATALOG) {
      ok(ps_catalog_get(peer, 88, PS_CATALOG_BODY, "stream", 6, &view, &e), &e);
      REQUIRE(view);
      ok(ps_catalog_view_info(view, &binding, &e), &e);
      REQUIRE(binding.target.incarnation == ref.incarnation);
      ps_catalog_view_close(view);
      REQUIRE(ps_owned_claim_orphan(observer, &e) == LC_ERR_INVALID);
      lc_error_cleanup(&e);
      REQUIRE(ps_owned_retire(observer, &e) == LC_ERR_INVALID);
      lc_error_cleanup(&e);
      ps_owned_close(observer);
      observer = NULL;
      ok(ps_catalog_visit_orphans(peer, find_orphan, &orphan, &e), &e);
      REQUIRE(!orphan.found);
    } else if (edit.action == STRUCTURAL_REPACK ||
               edit.action == STRUCTURAL_DROP_PIECE) {
      int kept = edit.action == STRUCTURAL_REPACK;
      ok(ps_owned_status_capture(observer, &info, &e), &e);
      REQUIRE(info.runs == (uint64_t)kept && info.sealed && !info.retired);
      ok(ps_owned_grant(observer, NULL, 0, 0, &current, &found, &e), &e);
      REQUIRE(found == kept);
      if (kept) {
        REQUIRE(ps_owned_run_mapped(current));
        ok(ps_owned_read(current, 100, bytes, 100, &e), &e);
        REQUIRE(bytes[0] == 's' && bytes[99] == 's');
        ps_owned_run_close(current);
        current = NULL;
      }
    } else {
      REQUIRE(ps_owned_status_capture(observer, &info, &e) == LC_ERR_PENDING);
      lc_error_cleanup(&e);
      REQUIRE(ps_owned_grant(observer, NULL, 0, 0, &current, &found, &e) ==
                  LC_ERR_PENDING &&
              !current && !found);
      lc_error_cleanup(&e);
    }
    ok(ps_owned_read(captured, 600, bytes, 1, &e), &e);
    REQUIRE(bytes[0] == 's');
    capture_put(peer, 10U + (unsigned int)edit.action, 65);
    REQUIRE(peer->generation == generation &&
            readiness(peer).durable == before.durable);
    finish_catalog_sync(&gate, thread);
    if (edit.action == STRUCTURAL_EDIT_CATALOG) {
      ok(ps_catalog_get(peer, 88, PS_CATALOG_BODY, "stream", 6, &view, &e), &e);
      REQUIRE((view != NULL) == rollover);
      if (view) {
        ok(ps_catalog_view_info(view, &binding, &e), &e);
        REQUIRE(binding.target.incarnation == edit.target.incarnation);
        ps_catalog_view_close(view);
      }
      ok(ps_catalog_visit_orphans(peer, find_orphan, &orphan, &e), &e);
      REQUIRE(orphan.found);
      ok(ps_owned_open(peer, NULL, tail_stream(), 0, &ref, &observer, &info,
                       &e),
         &e);
    }
    ok(ps_owned_status_capture(observer, &info, &e), &e);
    REQUIRE(info.sealed && info.retired == (edit.action == STRUCTURAL_RETIRE));
    if (edit.action == STRUCTURAL_REPACK) {
      ps_owned_run_close(edit.old);
      edit.old = edit.replacement;
      edit.replacement = NULL;
      ok(ps_owned_grant(observer, NULL, 0, 0, &current, &found, &e), &e);
      REQUIRE(found && ps_owned_run_mapped(current));
      ok(ps_owned_read(current, 100, bytes, 100, &e), &e);
      REQUIRE(bytes[0] == 's' && bytes[99] == 's');
      ps_owned_run_close(current);
      current = NULL;
    }
  }
  ok(ps_checkpoint(peer, &e), &e);
  ok(ps_owned_read(captured, 999, bytes, 1, &e), &e);
  REQUIRE(bytes[0] == 's');
  ps_owned_run_close(captured);
  ps_owned_run_close(edit.old);
  ps_owned_close(observer);
  ps_owned_close(edit.head);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(owner, &e), &e);
  ok(ps_open(path, 0, NULL, &peer, &e), &e);
  for (unsigned int n = 10; n < 15; ++n)
    expect_value(peer, key(n), 65, n);
  ok(ps_close(peer, &e), &e);
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
typedef struct marker_edit {
  ps_space *space;
  const unsigned char *bytes;
  size_t length;
  ps_catalog_info current;
} marker_edit;
static void *publish_marker_edit(void *context) {
  marker_edit *edit = context;
  lc_error e = {0};
  ps_catalog_info result;
  const ps_ref *prior = edit->current.id ? &edit->current.revision : NULL;
  if (edit->bytes) {
    ok(ps_catalog_put_marker(edit->space, NULL, 88, "marker", 6, prior,
                             edit->bytes, edit->length, &result, &e),
       &e);
    edit->current = result;
  } else {
    ok(ps_catalog_remove(edit->space, 88, PS_CATALOG_MARKER, "marker", 6, prior,
                         &e),
       &e);
  }
  return NULL;
}
static void expect_marker_view(ps_catalog_view *view,
                               const unsigned char *expected, size_t length,
                               lc_error *e) {
  const unsigned char *bytes;
  size_t size;
  ok(ps_catalog_view_marker(view, &bytes, &size, e), e);
  REQUIRE(size == length && !memcmp(bytes, expected, length));
}
static ps_catalog_view *expect_marker(ps_space *space,
                                      const unsigned char *expected,
                                      size_t length, lc_error *e) {
  ps_catalog_view *view = NULL;
  ok(ps_catalog_get(space, 88, PS_CATALOG_MARKER, "marker", 6, &view, e), e);
  if (expected)
    expect_marker_view(view, expected, length, e);
  else
    REQUIRE(!view);
  return view;
}
static void inline_catalog_markers(void) {
  lc_error e = {0};
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  marker_edit edit = {0};
  pthread_t thread;
  unsigned char one = 17, *large = malloc(PS_CATALOG_MARKER_MAX);
  ps_catalog_view *old, *latest;
  ps_catalog_info info;
  REQUIRE(large);
  memset(large, 51, PS_CATALOG_MARKER_MAX);
  options.io = &io;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_open(path, 0, &options, &b, &e), &e);
  REQUIRE(ps_catalog_put_marker(a, NULL, 88, "marker", 6, NULL, large,
                                PS_CATALOG_MARKER_MAX + 1, &info,
                                &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  edit.space = a;
  edit.bytes = &one;
  edit.length = 1;
  gate.paused = 1;
  REQUIRE(!pthread_create(&thread, NULL, publish_marker_edit, &edit));
  wait_catalog_sync(&gate);
  expect_marker(b, NULL, 0, &e);
  capture_put(b, 11, 65);
  finish_catalog_sync(&gate, thread);
  old = expect_marker(b, &one, 1, &e);
  ok(ps_catalog_view_info(old, &info, &e), &e);
  REQUIRE(info.id == edit.current.id && !info.stream.kind);

  gate.entered = gate.released = 0;
  gate.paused = 1;
  edit.space = b;
  edit.bytes = large;
  edit.length = PS_CATALOG_MARKER_MAX;
  REQUIRE(!pthread_create(&thread, NULL, publish_marker_edit, &edit));
  wait_catalog_sync(&gate);
  ps_catalog_view_close(expect_marker(a, &one, 1, &e));
  REQUIRE(ps_catalog_remove(a, 88, PS_CATALOG_MARKER, "marker", 6,
                            &info.revision, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  capture_put(a, 12, 65);
  finish_catalog_sync(&gate, thread);
  REQUIRE(info.id == edit.current.id &&
          info.revision.incarnation != edit.current.revision.incarnation);
  ok(ps_checkpoint(a, &e), &e);
  latest = expect_marker(b, large, PS_CATALOG_MARKER_MAX, &e);
  expect_marker_view(old, &one, 1, &e);

  gate.entered = gate.released = 0;
  gate.paused = 1;
  edit.space = a;
  edit.bytes = NULL;
  REQUIRE(!pthread_create(&thread, NULL, publish_marker_edit, &edit));
  wait_catalog_sync(&gate);
  ps_catalog_view_close(expect_marker(b, large, PS_CATALOG_MARKER_MAX, &e));
  capture_put(b, 13, 65);
  finish_catalog_sync(&gate, thread);
  expect_marker(b, NULL, 0, &e);
  ok(ps_checkpoint(b, &e), &e);
  expect_marker(a, NULL, 0, &e);
  expect_marker_view(latest, large, PS_CATALOG_MARKER_MAX, &e);
  expect_marker_view(old, &one, 1, &e);
  ps_catalog_view_close(latest);
  ps_catalog_view_close(old);
  ok(ps_catalog_put_marker(b, NULL, 88, "marker", 6, NULL, large,
                           PS_CATALOG_MARKER_MAX, &info, &e),
     &e);
  REQUIRE(info.id != edit.current.id);
  ok(ps_close(a, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  ps_catalog_view_close(expect_marker(a, large, PS_CATALOG_MARKER_MAX, &e));
  expect_value(a, key(11), 65, 11);
  expect_value(a, key(12), 65, 12);
  expect_value(a, key(13), 65, 13);
  ok(ps_close(a, &e), &e);
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  free(large);
}
static void catalog_workspace_with_peer_writer(void) {
  lc_error e = {0};
  ps_space *writer = NULL, *catalog = NULL;
  ps_catalog_info info;
  ps_writer *body;
  unsigned char *value = malloc(PS_CATALOG_MARKER_MAX);
  uint64_t offset, first, last, available;
  REQUIRE(value);
  memset(value, 19, PS_CATALOG_MARKER_MAX);
  ok(ps_open(path, 1, NULL, &writer, &e), &e);
  ok(ps_open(path, 0, NULL, &catalog, &e), &e);
  REQUIRE(catalog->free);
  offset = catalog->free->offset;
  available = catalog->metadata_free_bytes;
  body = replay_value(writer, key(21), 21, PS_CHUNK, &e);
  ok(ps_coordination_conflict(catalog->coordination, offset, PS_PAGE, &first,
                              &last, &e),
     &e);
  REQUIRE(first == offset && last == offset + PS_PAGE);
  /* The peer's writer holds a private range absent from the journal. Growing
   * the catalog's protected workspace must exclude it before choosing pages. */
  ok(ps_catalog_put_marker(catalog, NULL, 88, "marker", 6, NULL, value,
                           PS_CATALOG_MARKER_MAX, &info, &e),
     &e);
  REQUIRE(catalog->metadata_free_bytes > available);
  ok(ps_publish(body, NULL, &e), &e);
  ps_writer_close(body);
  expect_value(catalog, key(21), PS_CHUNK, 21);
  ps_catalog_view_close(
      expect_marker(catalog, value, PS_CATALOG_MARKER_MAX, &e));
  ok(ps_checkpoint(catalog, &e), &e);
  expect_value(writer, key(21), PS_CHUNK, 21);
  ok(ps_close(catalog, &e), &e);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  free(value);
}
static void journal_replay_structural(void) {
  replay_structural(0);
  replay_structural(1);
  additive_catalog();
  ready_structural_stream(0);
  ready_structural_stream(1);
  inline_catalog_markers();
  catalog_workspace_with_peer_writer();
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
typedef struct header_probe {
  off_t offsets[16];
  size_t count, combined, identities, footers;
  int armed;
} header_probe;
static ssize_t header_read(void *context, int fd, void *out, size_t n,
                           off_t at) {
  header_probe *probe = context;
  if (probe->armed)
    for (size_t i = 0; i < probe->count; ++i) {
      if (at == probe->offsets[i] && n == PS_RUN_HEADER)
        ++probe->combined;
      if (at == probe->offsets[i] && n == PS_RUN_IDENTITY_BYTES)
        ++probe->identities;
      if (at == probe->offsets[i] + PS_RUN_IDENTITY_BYTES)
        ++probe->footers;
    }
  return pread(fd, out, n, at);
}
static void stream_header_capture(int durable) {
  lc_error e = {0};
  header_probe probe = {0};
  ps_io io = {&probe, header_read, durable_write, sync_io, reserve_io, NULL};
  ps_options options = {0};
  ps_space *writer = NULL, *peer = NULL;
  ps_stream *stream = NULL;
  ps_stream_writer *append = NULL;
  ps_stream_window *view = NULL;
  ps_stream_info info;
  stream_recovery recovery = {0};
  char *body = malloc(PS_CHUNK);
  char last;
  off_t partial = 0;
  REQUIRE(body);
  memset(body, 'a', PS_CHUNK);
  options.durable_sync = durable;
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  ok(ps_stream_open(writer, NULL, tail_stream(), 1, NULL, recover_stream,
                    &recovery, &stream, &e),
     &e);
  ok(ps_stream_begin(stream, &append, &e), &e);
  ok(ps_stream_write(append, body, PS_CHUNK, &e), &e);
  ok(ps_stream_write(append, body, PS_CHUNK, &e), &e);
  ok(ps_stream_commit(append, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(append);
  ok(ps_stream_seal(stream, &e), &e);
  ok(ps_stream_status(stream, &info, &e), &e);
  for (ps_entry *p = writer->entries; p; p = p->all_next)
    if (p->state == 1 && !p->deleted && p->key.kind == PS_OWNED_RUN &&
        p->key.owner == info.ref.incarnation) {
      REQUIRE(!p->owned_mapped && p->count == 1 &&
              probe.count < sizeof(probe.offsets) / sizeof(probe.offsets[0]));
      probe.offsets[probe.count++] = (off_t)p->extents[0].offset;
      if (p->key.key < info.tail && p->stream_capacity > info.tail - p->key.key)
        partial = (off_t)p->extents[0].offset;
    }
  REQUIRE(probe.count == info.runs && probe.count > 1 && partial);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_checkpoint(writer, &e), &e);

  options.io = &io;
  ok(ps_open(path, 0, &options, &peer, &e), &e);
  probe.armed = 1;
  ok(ps_stream_open(peer, NULL, tail_stream(), 0, &info.ref, NULL, NULL,
                    &stream, &e),
     &e);
  REQUIRE(probe.combined == probe.count && !probe.identities && !probe.footers);
  probe.armed = 0;
  ok(ps_stream_window_open(stream, 2 * PS_CHUNK - 1, 1, &view, &e), &e);
  ok(ps_stream_window_read(view, 2 * PS_CHUNK - 1, &last, 1, &e), &e);
  REQUIRE(last == 'a');
  ps_stream_window_close(view);
  ok(ps_stream_close(stream, &e), &e);

  /* Both checks still apply to a live join, including the sealed partial run.
   */
  for (size_t i = 0; i < 2; ++i) {
    off_t at = partial + (i ? PS_RUN_IDENTITY_BYTES : 0);
    unsigned char original, damaged;
    REQUIRE(pread(writer->fd, &original, 1, at) == 1);
    damaged = original ^ 1;
    REQUIRE(pwrite(writer->fd, &damaged, 1, at) == 1);
    REQUIRE(ps_stream_open(peer, NULL, tail_stream(), 0, &info.ref, NULL, NULL,
                           &stream, &e) == LC_ERR_PROTOCOL &&
            !stream);
    lc_error_cleanup(&e);
    REQUIRE(pwrite(writer->fd, &original, 1, at) == 1);
  }
  ok(ps_stream_open(peer, NULL, tail_stream(), 0, &info.ref, NULL, NULL,
                    &stream, &e),
     &e);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  free(body);
  lc_error_cleanup(&e);
}
static void *finish_stream_append(void *context) {
  lc_error e = {0};
  ps_stream_writer *w = context;
  ok(ps_stream_commit(w, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(w);
  return NULL;
}
typedef struct piece_handoff {
  ps_coordination_guard *peer_edit;
  int entered;
} piece_handoff;
static int admit_peer_before_piece_selection(void *context,
                                             ps_piece_publish_fn publish,
                                             void *publication, lc_error *e) {
  piece_handoff *handoff = context;
  REQUIRE(handoff->peer_edit);
  ps_coordination_unlock(handoff->peer_edit);
  handoff->peer_edit = NULL;
  ++handoff->entered;
  return publish(publication, e);
}
static int replace_piece_before_selection(void *context,
                                          ps_piece_publish_fn publish,
                                          void *publication, lc_error *e) {
  ps_stream *peer = context;
  ps_owned_range live = {0, 5};
  ok(ps_stream_piece_repack(peer, 0, NULL, &live, 1, NULL, NULL, e), e);
  return publish(publication, e);
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
          LC_ERR_INVALID);
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
  piece_handoff handoff = {0};
  /* The peer holds edit authority until the private copy reaches selection. */
  ok(ps_coordination_lock(ps_space_coordination(a), PS_COORDINATION_STREAM,
                          info.ref.incarnation, 0, &handoff.peer_edit, &e),
     &e);
  ok(ps_stream_piece_repack(y, 0, NULL, &retained, 1,
                            admit_peer_before_piece_selection, &handoff, &e),
     &e);
  REQUIRE(handoff.entered == 1 && !handoff.peer_edit);
  REQUIRE(ps_stream_piece_repack(y, 0, NULL, &retained, 1,
                                 replace_piece_before_selection, x,
                                 &e) == LC_ERR_PENDING);
  lc_error_cleanup(&e);
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
typedef struct piece_barrier_work {
  ps_space *space;
  ps_stream *stream;
  int discard;
} piece_barrier_work;
static int select_piece_under_namespace(void *context,
                                        ps_piece_publish_fn publish,
                                        void *publication, lc_error *e) {
  piece_barrier_work *work = context;
  ps_coordination_guard *guard = NULL;
  int rc =
      ps_coordination_lock(ps_space_coordination(work->space),
                           PS_COORDINATION_NAMESPACE_STATE, 31, 0, &guard, e);
  if (rc == LC_OK)
    rc = publish(publication, e);
  ps_coordination_unlock(guard);
  return rc;
}
static void *replace_piece_at_barrier(void *context) {
  piece_barrier_work *work = context;
  ps_owned_range retained = {0, 1};
  lc_error e = {0};
  ok(ps_stream_piece_repack(
         work->stream, 0, NULL, work->discard ? NULL : &retained,
         work->discard ? 0 : 1, select_piece_under_namespace, work, &e),
     &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void capacity_checkpoint(int durable, int streaming, int resize,
                                size_t fail_sync) {
  lc_error e = {0};
  readiness_fault fault = {6, EIO, 0, 0, fail_sync};
  ps_io io = {&fault,     read_io, durable_write, readiness_sync_fault,
              reserve_io, NULL};
  ps_options options = {0};
  ps_space *writer = NULL, *peer = NULL;
  ps_reader *pin = NULL;
  ps_budget *padding = NULL, *admission = NULL;
  ps_budget_plan plan = {PS_CHUNK, 0, 0, 0}, fill = {0};
  stream_recovery recovery = {0};
  ps_stream *stream = NULL;
  ps_stream_writer *append = NULL;
  ps_stream_window *window = NULL;
  unsigned char *body = streaming ? malloc(2 * PS_CHUNK) : NULL;
  uint64_t available, checkpoints;
  int needed;
  options.durable_sync = durable;
  options.io = &io;
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  capture_put(writer, 1, 4 * PS_CHUNK);
  capture_put(writer, 2, 41);
  if (streaming) {
    REQUIRE(body);
    memset(body, 0x37, PS_CHUNK);
    memset(body + PS_CHUNK, 0x91, PS_CHUNK);
    ok(ps_stream_open(writer, NULL, tail_stream(), 1, NULL, recover_stream,
                      &recovery, &stream, &e),
       &e);
    ok(ps_stream_begin(stream, &append, &e), &e);
    ok(ps_stream_write(append, body, PS_CHUNK, &e), &e);
  }
  ok(ps_checkpoint(writer, &e), &e);
  options.io = NULL;
  ok(ps_open(path, 0, &options, &peer, &e), &e);
  ok(ps_read_open(peer, key(1), NULL, &pin, &e), &e);
  if (resize) {
    ps_budget_plan initial = {PS_PAGE, 0, 0, 0};
    ok(ps_budget_open(writer, &initial, &admission, &e), &e);
  }
  ok(ps_maintenance_pressure(writer, 1, &needed, &available, &e), &e);
  REQUIRE(available > PS_MIB && available % PS_PAGE == 0);
  fill.payload_bytes = available;
  ok(ps_budget_open(writer, &fill, &padding, &e), &e);
  ok(ps_delete(writer, key(1), &e), &e);
  ok(ps_flush(writer, &e), &e);
  checkpoints = status(writer).checkpoints;
  if (resize) {
    ps_budget_plan retained;
    REQUIRE(ps_budget_resize(admission, &plan, &e) == LC_ERR_INVALID);
    lc_error_cleanup(&e);
    ok(ps_budget_status(admission, &retained, &e), &e);
    REQUIRE(retained.payload_bytes == PS_PAGE);
  } else {
    REQUIRE(ps_budget_open(writer, &plan, &admission, NULL) == LC_ERR_INVALID &&
            !admission);
  }
  REQUIRE(status(writer).checkpoints == checkpoints);
  captured_expect(pin, 1, 4 * PS_CHUNK);
  ps_reader_close(pin);
  if (streaming) {
    ok(ps_stream_write(append, body + PS_CHUNK, PS_CHUNK, &e), &e);
    ok(ps_stream_commit(append, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
    ps_stream_writer_close(append);
    ok(ps_stream_seal(stream, &e), &e);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_flush(writer, &e), &e);
  } else if (fail_sync) {
    fault.armed = 1;
    REQUIRE(ps_budget_open(writer, &plan, &admission, &e) == LC_ERR_TRANSPORT &&
            !admission);
    REQUIRE(fault.calls == fail_sync);
    lc_error_cleanup(&e);
    REQUIRE(status(writer).reserved_payload_bytes == fill.payload_bytes);
  } else if (resize) {
    ok(ps_budget_resize(admission, &plan, &e), &e);
  } else {
    ok(ps_budget_open(writer, &plan, &admission, &e), &e);
  }
  if (!fail_sync)
    REQUIRE(status(writer).checkpoints == checkpoints + 1);
  (void)ps_budget_close(admission, NULL);
  (void)ps_budget_close(padding, NULL);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_open(path, 0, &options, &writer, &e), &e);
  REQUIRE(status(writer).capacity == PS_DEFAULT_CAPACITY);
  expect_value(writer, key(1), 0, 0);
  expect_value(writer, key(2), 41, 2);
  if (streaming) {
    unsigned char recovered[997];
    ok(ps_stream_open(writer, NULL, tail_stream(), 0, NULL, recover_stream,
                      &recovery, &stream, &e),
       &e);
    ok(ps_stream_window_open(stream, 0, 2 * PS_CHUNK, &window, &e), &e);
    for (size_t offset = 0; offset < 2 * PS_CHUNK;) {
      size_t n = 2 * PS_CHUNK - offset;
      if (n > sizeof(recovered))
        n = sizeof(recovered);
      ok(ps_stream_window_read(window, offset, recovered, n, &e), &e);
      REQUIRE(!memcmp(recovered, body + offset, n));
      offset += n;
    }
    ps_stream_window_close(window);
    ok(ps_stream_close(stream, &e), &e);
  }
  ok(ps_close(writer, &e), &e);
  free(body);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  lc_error_cleanup(&e);
}
static void pressure_reclaim(int durable, size_t fail_sync) {
  lc_error e = {0};
  readiness_fault fault = {6, EIO, 0, 0, fail_sync};
  ps_io io = {&fault,     read_io, durable_write, readiness_sync_fault,
              reserve_io, NULL};
  ps_options options = {0};
  ps_space *writer = NULL, *maintenance = NULL;
  ps_reader *pin = NULL;
  ps_budget *padding = NULL;
  ps_budget_plan plan = {0};
  ps_coordination_guard *guard = NULL;
  uint64_t available, checkpoints, low;
  int needed;
  options.durable_sync = durable;
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  capture_put(writer, 1, 4 * PS_CHUNK);
  capture_put(writer, 2, 41);
  ok(ps_checkpoint(writer, &e), &e);
  ok(ps_read_open(writer, key(1), NULL, &pin, &e), &e);
  options.io = &io;
  ok(ps_open(path, 0, &options, &maintenance, &e), &e);
  options.io = NULL;
  ok(ps_maintenance_pressure(maintenance, 1, &needed, &available, &e), &e);
  REQUIRE(available > PS_MIB && available % PS_PAGE == 0);
  plan.payload_bytes = available - PS_PAGE;
  ok(ps_budget_open(maintenance, &plan, &padding, &e), &e);
  ok(ps_delete(writer, key(1), &e), &e);
  ok(ps_flush(writer, &e), &e);

  checkpoints = status(maintenance).checkpoints;
  ok(ps_maintenance_pressure(maintenance, 0, &needed, &low, &e), &e);
  REQUIRE(needed && low < PS_CHUNK &&
          status(maintenance).checkpoints == checkpoints);
  captured_expect(pin, 1, 4 * PS_CHUNK);
  ps_reader_close(pin);

  /* A worker cannot start a checkpoint by waiting on a live allocator. */
  ok(ps_coordination_lock(ps_space_coordination(writer),
                          PS_COORDINATION_ALLOCATION, 0, 1, &guard, &e),
     &e);
  REQUIRE(ps_maintenance_pressure(maintenance, 0, &needed, &available, &e) ==
              LC_ERR_PENDING &&
          needed && !available &&
          status(maintenance).checkpoints == checkpoints);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);

  if (fail_sync) {
    fault.armed = 1;
    REQUIRE(ps_maintenance_pressure(maintenance, 0, &needed, &available, &e) ==
                LC_ERR_TRANSPORT &&
            fault.calls == fail_sync);
    lc_error_cleanup(&e);
    REQUIRE(status(maintenance).reserved_payload_bytes == plan.payload_bytes);
  } else {
    ok(ps_maintenance_pressure(maintenance, 0, &needed, &available, &e), &e);
    REQUIRE(needed && available >= low + 4 * PS_CHUNK &&
            status(maintenance).checkpoints == checkpoints + 1);
    ok(ps_maintenance_pressure(maintenance, 0, &needed, &available, &e), &e);
    REQUIRE(status(maintenance).checkpoints == checkpoints + 1);
    checkpoints = status(writer).checkpoints;
    ok(ps_maintenance_pressure(writer, 0, &needed, &available, &e), &e);
    REQUIRE(status(writer).checkpoints == checkpoints);
  }
  (void)ps_budget_close(padding, NULL);
  ok(ps_close(maintenance, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_open(path, 0, &options, &writer, &e), &e);
  REQUIRE(status(writer).capacity == PS_DEFAULT_CAPACITY);
  expect_value(writer, key(1), 0, 0);
  expect_value(writer, key(2), 41, 2);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
  lc_error_cleanup(&e);
}
static void packed_capacity_reclaim(int durable, int background) {
  lc_error e = {0};
  ps_options options = {.durable_sync = durable};
  ps_space *writer = NULL, *peer = NULL;
  ps_reader *pin = NULL;
  ps_budget *padding = NULL, *budget = NULL;
  const uint64_t bytes = background ? PS_CHUNK : PS_PAGE;
  const uint64_t values = bytes / 1024;
  ps_budget_plan plan = {bytes, 0, 0, 0}, fill = {0};
  uint64_t available, checkpoints;
  int needed;
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  for (uint64_t id = 1; id <= values + 1; ++id)
    capture_put(writer, id, 1024);
  REQUIRE(status(writer).packed_pages == bytes / PS_PAGE + 1);
  ok(ps_checkpoint(writer, &e), &e);
  ok(ps_open(path, 0, &options, &peer, &e), &e);
  ok(ps_maintenance_pressure(writer, 1, &needed, &available, &e), &e);
  REQUIRE(available > PS_MIB && available % PS_PAGE == 0);
  fill.payload_bytes = available;
  ok(ps_budget_open(writer, &fill, &padding, &e), &e);
  checkpoints = status(writer).checkpoints;
  for (uint64_t id = 2; id <= values; ++id)
    ok(ps_delete(writer, key(id), &e), &e);
  ok(ps_flush(writer, &e), &e);
  /* A selected live cell and then a retained cell both prevent reclaiming
   * the full request. Exercise a local pin and a kernel-witnessed peer pin. */
  for (int retired = 0; retired <= 1; ++retired) {
    if (background) {
      ok(ps_maintenance_pressure(writer, 0, &needed, &available, &e), &e);
      REQUIRE(needed && available < bytes);
    } else {
      REQUIRE(ps_budget_open(writer, &plan, &budget, &e) == LC_ERR_INVALID &&
              !budget);
      lc_error_cleanup(&e);
    }
    REQUIRE(status(writer).checkpoints == checkpoints);
    if (!retired) {
      ok(ps_read_open(durable ? peer : writer, key(1), NULL, &pin, &e), &e);
      ok(ps_delete(writer, key(1), &e), &e);
      ok(ps_flush(writer, &e), &e);
    }
  }
  captured_expect(pin, 1, 1024);
  ps_reader_close(pin);
  if (background) {
    ok(ps_maintenance_pressure(writer, 0, &needed, &available, &e), &e);
    REQUIRE(needed && available >= bytes);
  } else {
    ok(ps_budget_open(writer, &plan, &budget, &e), &e);
    REQUIRE(status(writer).reserved_payload_bytes ==
            fill.payload_bytes + bytes);
    ok(ps_budget_close(budget, &e), &e);
  }
  REQUIRE(status(writer).checkpoints == checkpoints + 1 &&
          status(writer).packed_pages == 1);
  ok(ps_budget_close(padding, &e), &e);
  expect_value(peer, key(values + 1), 1024, (int)values + 1);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_open(path, 0, &options, &writer, &e), &e);
  REQUIRE(status(writer).capacity == PS_DEFAULT_CAPACITY);
  for (uint64_t id = 1; id <= values; ++id)
    expect_value(writer, key(id), 0, 0);
  expect_value(writer, key(values + 1), 1024, (int)values + 1);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void stream_capacity_reclaim(int durable, int background,
                                    size_t fail_sync) {
  lc_error e = {0};
  readiness_fault fault = {6, EIO, 0, 0, fail_sync};
  ps_io io = {&fault,     read_io, durable_write, readiness_sync_fault,
              reserve_io, NULL};
  ps_options options = {.durable_sync = durable};
  ps_space *writer = NULL, *peer = NULL, *requester;
  ps_owned_head *head = NULL;
  ps_owned_run *pin = NULL;
  ps_owned_status selected;
  ps_budget *padding = NULL, *budget = NULL;
  ps_writer *value = NULL;
  ps_budget_plan plan = {PS_CHUNK, 128, 1, 0}, fill = {0};
  unsigned char body[PS_CHUNK], captured = 0;
  uint64_t available, checkpoints;
  int found, needed;
  memset(body, 0x51, sizeof(body));
  ok(ps_open(path, 1, &options, &writer, &e), &e);
  ok(ps_owned_open(writer, NULL, tail_stream(), 1, NULL, &head, &selected, &e),
     &e);
  ok(ps_owned_grant(head, NULL, 0, 4 * PS_CHUNK, &pin, &found, &e), &e);
  REQUIRE(found && ps_owned_run_capacity(pin) == 4 * PS_CHUNK - PS_RUN_HEADER);
  ok(ps_owned_write(pin, 0, body, 1, &e), &e);
  ok(ps_owned_commit(head, 1, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ok(ps_owned_seal(head, &e), &e);
  capture_put(writer, 1, 65);
  ok(ps_checkpoint(writer, &e), &e);
  if (fail_sync)
    options.io = &io;
  ok(ps_open(path, 0, &options, &peer, &e), &e);
  options.io = NULL;
  requester = durable ? peer : writer;
  ok(ps_maintenance_pressure(requester, 1, &needed, &available, &e), &e);
  REQUIRE(available > PS_MIB && available % PS_PAGE == 0);
  fill.payload_bytes = available;
  ok(ps_budget_open(requester, &fill, &padding, &e), &e);
  checkpoints = status(requester).checkpoints;
  ok(ps_owned_retire(head, &e), &e);
  ps_owned_close(head);
  head = NULL;
  if (background) {
    ok(ps_maintenance_pressure(requester, 0, &needed, &available, &e), &e);
    REQUIRE(needed && available < PS_CHUNK);
  } else {
    REQUIRE(ps_budget_open(requester, &plan, &budget, &e) == LC_ERR_INVALID &&
            !budget);
    lc_error_cleanup(&e);
  }
  REQUIRE(status(requester).checkpoints == checkpoints);
  ok(ps_owned_read(pin, 0, &captured, 1, &e), &e);
  REQUIRE(captured == body[0]);
  ps_owned_run_close(pin);
  /* The removed stream's runs still belong to the retained root. The first
   * admission that can use them must select the next checkpoint. */
  if (fail_sync) {
    fault.armed = 1;
    REQUIRE(ps_maintenance_pressure(requester, 0, &needed, &available, &e) ==
                LC_ERR_TRANSPORT &&
            fault.calls == fail_sync);
    lc_error_cleanup(&e);
    REQUIRE(status(requester).reserved_payload_bytes == fill.payload_bytes);
  } else if (background) {
    ok(ps_maintenance_pressure(requester, 0, &needed, &available, &e), &e);
    REQUIRE(needed && available >= PS_CHUNK);
    ok(ps_maintenance_pressure(requester, 0, &needed, &available, &e), &e);
    REQUIRE(needed && available >= PS_CHUNK);
  } else {
    ok(ps_budget_open(requester, &plan, &budget, &e), &e);
    ok(ps_begin(requester, budget, key(2), &value, &e), &e);
    ok(ps_write(value, body, sizeof(body), &e), &e);
    ok(ps_publish(value, NULL, &e), &e);
    ps_writer_close(value);
    ok(ps_budget_close(budget, &e), &e);
    ok(ps_flush(requester, &e), &e);
  }
  if (!fail_sync)
    REQUIRE(status(requester).checkpoints == checkpoints + 1);
  (void)ps_budget_close(padding, NULL);
  ok(ps_close(peer, &e), &e);
  ok(ps_close(writer, &e), &e);
  ok(ps_open(path, 0, &options, &writer, &e), &e);
  REQUIRE(status(writer).capacity == PS_DEFAULT_CAPACITY);
  REQUIRE(ps_owned_open(writer, NULL, tail_stream(), 0, NULL, &head, &selected,
                        &e) == LC_ERR_INVALID &&
          !head);
  lc_error_cleanup(&e);
  expect_value(writer, key(1), 65, 1);
  if (!background)
    expect_value(writer, key(2), PS_CHUNK, body[0]);
  ok(ps_close(writer, &e), &e);
  REQUIRE(!coverage(NULL, 0) && !unlink(path));
}
static void piece_barrier_handoff(int durable, int discard) {
  durable_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, read_io, durable_write, durable_sync, reserve_io, NULL};
  ps_options options = {0};
  ps_space *a = NULL, *b = NULL;
  ps_stream *x = NULL, *y = NULL;
  ps_stream_window *original = NULL, *current = NULL;
  ps_coordination_guard *state = NULL, *edit = NULL;
  ps_readiness_certificate certificate;
  ps_stream_info info;
  ps_stream_piece_info piece;
  pthread_t thread;
  int found = 0;
  char byte;
  lc_error e = {0};
  options.durable_sync = durable;
  options.io = &io;
  ok(ps_open(path, 1, &options, &a, &e), &e);
  ok(ps_stream_open(a, NULL, tail_stream(), 1, NULL, NULL, NULL, &x, &e), &e);
  append_byte(x, 0, "a", &e);
  ok(ps_stream_seal(x, &e), &e);
  ok(ps_stream_status(x, &info, &e), &e);
  REQUIRE(info.runs == 1);
  ok(ps_stream_window_open(x, 0, 1, &original, &e), &e);
  options.io = NULL;
  ok(ps_open(path, 0, &options, &b, &e), &e);
  piece_barrier_work work = {a, x, discard};
  gate.paused = 1;
  REQUIRE(!pthread_create(&thread, NULL, replace_piece_at_barrier, &work));
  wait_catalog_sync(&gate);
  certificate = readiness(a);
  REQUIRE(certificate.durable < certificate.ready);
  ok(ps_coordination_lock(ps_space_coordination(b),
                          PS_COORDINATION_NAMESPACE_STATE, 31, 0, &state, &e),
     &e);
  ok(ps_coordination_lock(ps_space_coordination(b), PS_COORDINATION_STREAM,
                          info.ref.incarnation, 0, &edit, &e),
     &e);
  ps_coordination_unlock(edit);
  ps_coordination_unlock(state);
  ok(ps_stream_open(b, NULL, tail_stream(), 0, NULL, NULL, NULL, &y, &e), &e);
  ok(ps_stream_piece_next(y, 0, &piece, &found, &e), &e);
  REQUIRE(found == !discard);
  if (!discard) {
    REQUIRE(piece.mapped);
    ok(ps_stream_window_open(y, 0, 1, &current, &e), &e);
    ok(ps_stream_window_read(current, 0, &byte, 1, &e), &e);
    REQUIRE(byte == 'a');
    ps_stream_window_close(current);
  }
  ok(ps_stream_window_read(original, 0, &byte, 1, &e), &e);
  REQUIRE(byte == 'a');
  finish_catalog_sync(&gate, thread);
  certificate = readiness(a);
  REQUIRE(certificate.durable == certificate.ready);
  ps_stream_window_close(original);
  ok(ps_stream_close(y, &e), &e);
  ok(ps_stream_close(x, &e), &e);
  ok(ps_close(b, &e), &e);
  ok(ps_close(a, &e), &e);
  REQUIRE(!pthread_cond_destroy(&gate.cond) &&
          !pthread_mutex_destroy(&gate.mutex));
  REQUIRE(!unlink(path));
  lc_error_cleanup(&e);
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
      lock->l_start >= (off_t)PS_MAX_CAPACITY) {
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
  else if (!strcmp(argv[2], "captures")) {
    captures();
    capture_identities();
  } else if (!strcmp(argv[2], "capture-faults"))
    capture_faults();
  else if (!strcmp(argv[2], "capture-owned"))
    capture_owned();
  else if (!strcmp(argv[2], "capture-race"))
    capture_race();
  else if (!strcmp(argv[2], "projection")) {
    projection_lifecycle();
    directory_reopen_locality(DIRECTORY_SMALL);
    directory_reopen_locality(DIRECTORY_INTERLEAVED);
    directory_reopen_locality(DIRECTORY_ORDERED);
    for (int durable = 0; durable <= 1; ++durable)
      projection_quarantine(durable, 0);
  } else if (!strcmp(argv[2], "projection-faults"))
    projection_faults();
  else if (!strcmp(argv[2], "projection-recovery"))
    projection_recovery();
  else if (!strcmp(argv[2], "projection-threads"))
    projection_threads();
  else if (!strcmp(argv[2], "projection-boundaries")) {
    projection_boundaries();
    for (int durable = 0; durable <= 1; ++durable)
      projection_quarantine(durable, 1);
  } else if (!strcmp(argv[2], "projection-pressure"))
    projection_pressure();
  else if (!strcmp(argv[2], "capacity-checkpoint")) {
    for (int durable = 0; durable <= 1; ++durable) {
      capacity_checkpoint(durable, 0, 0, 0);
      capacity_checkpoint(durable, 0, 1, 0);
      capacity_checkpoint(durable, 1, 0, 0);
      packed_capacity_reclaim(durable, 0);
      stream_capacity_reclaim(durable, 0, 0);
    }
    for (size_t sync = 1; sync <= 3; ++sync)
      capacity_checkpoint(1, 0, 0, sync);
  } else if (!strcmp(argv[2], "pressure-reclaim")) {
    pressure_reclaim(0, 0);
    pressure_reclaim(1, 0);
    packed_capacity_reclaim(0, 1);
    packed_capacity_reclaim(1, 1);
    stream_capacity_reclaim(0, 1, 0);
    stream_capacity_reclaim(1, 1, 0);
    for (size_t sync = 1; sync <= 3; ++sync) {
      pressure_reclaim(1, sync);
      stream_capacity_reclaim(1, 1, sync);
    }
  } else if (!strcmp(argv[2], "projection-tail"))
    projection_tail();
  else if (!strcmp(argv[2], "shared-durable"))
    shared_durability();
  else if (!strcmp(argv[2], "shared-preparation")) {
    shared_preparation(READY_BEFORE_SYNC);
    shared_preparation(READY_DURING_SYNC);
    shared_preparation(FAILED_SYNC);
    shared_preparation(READY_BEFORE_CHECKPOINT);
    shared_preparation_wait(0);
    shared_preparation_wait(1);
    preparation_leaves_peer_headroom();
  } else if (!strcmp(argv[2], "readiness-cohort"))
    readiness_cohort();
  else if (!strcmp(argv[2], "reopen-cohort"))
    reopen_cohort();
  else if (!strcmp(argv[2], "readiness-faults"))
    readiness_faults();
  else if (!strcmp(argv[2], "certified-prefix")) {
    certified_prefix_reads();
    certified_prefix_interruption();
    certified_resident_observation(0);
    certified_resident_observation(1);
  } else if (!strcmp(argv[2], "quarantine")) {
    quarantine_capacity();
    quarantine_packed();
    witness_filter();
  } else if (!strcmp(argv[2], "journal-replay"))
    journal_replay();
  else if (!strcmp(argv[2], "journal-replay-retired"))
    journal_replay_retired();
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
    fragmented_preparation();
    prepared_grants();
    prepared_grant_fault();
    checkpoint_preparation(CHECKPOINT_PREPARE);
    checkpoint_preparation(CHECKPOINT_ZERO_FAILURE);
    checkpoint_preparation(CHECKPOINT_BARRIER_FAILURE);
  } else if (!strcmp(argv[2], "stream-header-capture")) {
    stream_header_capture(0);
    stream_header_capture(1);
  } else if (!strcmp(argv[2], "stream-handoff")) {
    stream_handoff(0);
    stream_handoff(1);
    stream_abort_and_cold_open();
  } else if (!strcmp(argv[2], "packed-replay-locality"))
    packed_replay_locality();
  else if (!strcmp(argv[2], "piece-barrier-handoff")) {
    for (int durable = 0; durable <= 1; ++durable)
      for (int discard = 0; discard <= 1; ++discard)
        piece_barrier_handoff(durable, discard);
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
