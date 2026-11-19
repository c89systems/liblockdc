#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "lc_api_internal.h"
#include "space_internal.h"
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static size_t checks, allocations, allocation_fail;
static int measuring;
static int cooperative_wait;
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
  size_t count = 2;
  pending[0].first = 2 * PS_PAGE;
  pending[0].last = PS_JOURNAL_OFFSET;
  pending[1].first = PS_DATA;
  pending[1].last = PS_DEFAULT_CAPACITY;
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
  REQUIRE(ps_budget_open(b, &plan, &budget, &e) == LC_ERR_PENDING && !budget);
  lc_error_cleanup(&e);
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
  REQUIRE(pwrite(a->fd, rejected, sizeof(rejected), PS_JOURNAL_OFFSET) ==
          (ssize_t)sizeof(rejected));
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
  REQUIRE(g->rc == LC_ERR_PENDING);
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
  ok(ps_checkpoint(a, &e), &e);
  ok(ps_close(a, &e), &e);
  ok(ps_open(path, 0, NULL, &a, &e), &e);
  for (uint64_t id = 1; id <= 1000; ++id) {
    if (id > 64 && id < 1000)
      continue;
    ps_reader *r = NULL;
    int rc = ps_read_open(a, key(id), NULL, &r, &e);
    if (id == 64) {
      REQUIRE(rc == LC_ERR_INVALID && !r);
      lc_error_cleanup(&e);
      continue;
    }
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
int main(int argc, char **argv) {
  if (argc == 5 && !strcmp(argv[1], "--child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    child(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  REQUIRE(argc == 3 || (argc == 4 && !strcmp(argv[3], "--cooperative-wait")));
  cooperative_wait = argc == 4;
  if (cooperative_wait) {
    REQUIRE(!strcmp(argv[2], "threads") || !strcmp(argv[2], "serialized") ||
            !strcmp(argv[2], "packed"));
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
