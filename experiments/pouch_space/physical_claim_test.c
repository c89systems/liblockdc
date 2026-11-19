#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "budget.h"
#include "coordination.h"
#include "lc_api_internal.h"
#include "owned.h"
#include "revision.h"
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static size_t checks, allocations, allocation_fail;
static int measuring;
static char path[1500], directory[1400], executable[1600];
int __real_ps_coordination_open(int, const ps_coordination_options *,
                                ps_coordination **, lc_error *);
int __wrap_ps_coordination_open(int fd, const ps_coordination_options *options,
                                ps_coordination **out, lc_error *e) {
  ps_coordination_options selected = {0};
  if (options)
    selected = *options;
  /* One controlled canonical allocator, using the real shared authority mode
   * to exercise its range protocol. This fixture does not admit concurrent
   * canonical writers or add a product selector before refresh is complete. */
  selected.shared = 1;
  return __real_ps_coordination_open(fd, &selected, out, e);
}
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
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
    fprintf(stderr, "physical claim %d: %s\n", rc,
            e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static ps_key key(uint64_t id) {
  ps_key k = {81, id, PS_STATE};
  return k;
}
static int publish(ps_space *s, ps_budget *b, uint64_t id, size_t bytes,
                   lc_error *e) {
  unsigned char buffer[1024];
  memset(buffer, (int)(id % 251), sizeof(buffer));
  ps_writer *w = NULL;
  int rc = ps_begin(s, b, key(id), &w, e);
  while (rc == LC_OK && bytes) {
    size_t n = bytes < sizeof(buffer) ? bytes : sizeof(buffer);
    rc = ps_write(w, buffer, n, e);
    bytes -= n;
  }
  if (rc == LC_OK)
    rc = ps_publish(w, NULL, e);
  ps_writer_close(w);
  return rc;
}
static void verify(ps_space *s, uint64_t id, size_t bytes, lc_error *e) {
  unsigned char buffer[257];
  ps_reader *r = NULL;
  ok(ps_read_open(s, key(id), NULL, &r, e), e);
  REQUIRE(ps_read_size(r) == bytes);
  while (bytes) {
    size_t got, n = bytes < sizeof(buffer) ? bytes : sizeof(buffer);
    ok(ps_read(r, buffer, n, &got, e), e);
    REQUIRE(got == n);
    for (size_t j = 0; j < n; ++j)
      REQUIRE(buffer[j] == id % 251);
    bytes -= n;
  }
  ps_reader_close(r);
}
typedef struct span {
  uint64_t start, end;
} span;
static uint64_t kernel_bytes(void) {
  span *pending = calloc(4096, sizeof(*pending));
  REQUIRE(pending);
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(fd >= 0);
  struct stat st;
  REQUIRE(fstat(fd, &st) == 0);
  size_t count = 1;
  uint64_t total = 0;
  pending[0].start = PS_DATA;
  pending[0].end = (uint64_t)st.st_size;
  while (count) {
    span current = pending[--count];
    struct flock lock = {0};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = (off_t)current.start;
    lock.l_len = (off_t)(current.end - current.start);
    REQUIRE(fcntl(fd, F_OFD_GETLK, &lock) == 0);
    if (lock.l_type == F_UNLCK)
      continue;
    REQUIRE(lock.l_type == F_RDLCK && lock.l_len > 0 && lock.l_start >= 0);
    uint64_t first = (uint64_t)lock.l_start,
             last = first + (uint64_t)lock.l_len;
    if (first < current.start)
      first = current.start;
    if (last > current.end)
      last = current.end;
    REQUIRE(first < last && count + 2 <= 4096);
    total += last - first;
    if (first > current.start) {
      pending[count].start = current.start;
      pending[count++].end = first;
    }
    if (last < current.end) {
      pending[count].start = last;
      pending[count++].end = current.end;
    }
  }
  REQUIRE(close(fd) == 0);
  free(pending);
  return total;
}
typedef struct lock_fault {
  size_t calls, fail, fail2;
  int reason;
} lock_fault;
static int lock_call(void *context, int fd, int command, struct flock *lock) {
  lock_fault *f = context;
  if (command == F_OFD_SETLK) {
    ++f->calls;
    if (f->calls == f->fail || f->calls == f->fail2) {
      errno = f->reason;
      return -1;
    }
  }
  return fcntl(fd, command, lock);
}
static ps_stats status(ps_space *s, lc_error *e) {
  ps_stats st;
  ok(ps_status(s, &st, e), e);
  return st;
}
static void one_file(void) {
  DIR *d = opendir(directory);
  REQUIRE(d);
  struct dirent *entry;
  size_t count = 0;
  while ((entry = readdir(d)))
    if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) {
      REQUIRE(!strcmp(entry->d_name, "store"));
      ++count;
    }
  REQUIRE(closedir(d) == 0 && count == 1);
}
static void lifecycle(void) {
  for (int sync = 0; sync < 2; ++sync) {
    lc_error e = {0};
    ps_space *s = NULL;
    ps_budget *a = NULL, *b = NULL;
    lock_fault f = {0};
    ps_coordination_io io = {&f, lock_call};
    ps_options options = {0};
    options.durable_sync = sync;
    options.coordination_io = &io;
    ps_budget_plan plan = {16 * PS_PAGE, 4096, 4, 0};
    ok(ps_open(path, 1, &options, &s, &e), &e);
    ok(ps_budget_open(s, &plan, &a, &e), &e);
    REQUIRE(kernel_bytes() == plan.payload_bytes);
    ps_budget_plan second = {8 * PS_PAGE, 4096, 4, 0};
    ok(ps_budget_open(s, &second, &b, &e), &e);
    REQUIRE(kernel_bytes() == 24 * PS_PAGE);
    ps_budget_plan move = {PS_PAGE, 0, 0, 0};
    allocations = 0;
    allocation_fail = 1;
    measuring = 1;
    size_t before = f.calls;
    int rc = ps_budget_transfer(a, b, &move, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations && f.calls == before);
    ok(ps_budget_status(a, &plan, &e), &e);
    REQUIRE(plan.payload_bytes == 15 * PS_PAGE);
    plan.payload_bytes += 2 * PS_PAGE;
    ok(ps_budget_resize(a, &plan, &e), &e);
    REQUIRE(kernel_bytes() == 26 * PS_PAGE);
    before = f.calls;
    allocations = 0;
    measuring = 1;
    rc = ps_budget_resize(a, &plan, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations && f.calls == before);
    allocations = 0;
    measuring = 1;
    rc = ps_budget_close(b, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations);
    REQUIRE(kernel_bytes() == 17 * PS_PAGE);
    ps_writer *w = NULL;
    unsigned char *body = malloc(PS_CHUNK);
    REQUIRE(body);
    memset(body, 1, PS_CHUNK);
    ok(ps_begin(s, a, key(1), &w, &e), &e);
    ok(ps_write(w, body, PS_CHUNK, &e), &e);
    ps_stats st = status(s, &e);
    REQUIRE(st.claimed_payload_bytes == 17 * PS_PAGE &&
            st.private_bytes == PS_CHUNK);
    REQUIRE(st.reserved_payload_bytes == 13 * PS_PAGE &&
            kernel_bytes() == 17 * PS_PAGE);
    ok(ps_checkpoint(s, &e), &e);
    REQUIRE(kernel_bytes() == 17 * PS_PAGE);
    ok(ps_publish(w, NULL, &e), &e);
    ps_writer_close(w);
    REQUIRE(kernel_bytes() == 13 * PS_PAGE);
    ok(ps_budget_close(a, &e), &e);
    REQUIRE(kernel_bytes() == 0);
    verify(s, 1, PS_CHUNK, &e);
    ok(ps_begin(s, NULL, key(2), &w, &e), &e);
    ok(ps_write(w, body, PS_CHUNK, &e), &e);
    REQUIRE(kernel_bytes() == PS_CHUNK);
    ps_writer_close(w);
    REQUIRE(kernel_bytes() == 0);
    free(body);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_close(s, &e), &e);
    ok(ps_open(path, 0, &options, &s, &e), &e);
    verify(s, 1, PS_CHUNK, &e);
    ok(ps_close(s, &e), &e);
    one_file();
    REQUIRE(unlink(path) == 0);
  }
}
static void fragmented_faults(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *b = NULL;
  lock_fault f = {0};
  ps_coordination_io io = {&f, lock_call};
  ps_options options = {0};
  options.coordination_io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  for (uint64_t id = 1; id <= 32; ++id)
    ok(publish(s, NULL, id, PS_PAGE, &e), &e);
  for (uint64_t id = 1; id <= 32; id += 2)
    ok(ps_delete(s, key(id), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ps_budget_plan plan = {20 * PS_PAGE, 4096, 4, 0};
  allocations = allocation_fail = f.calls = 0;
  measuring = 1;
  int rc = ps_budget_open(s, &plan, &b, &e);
  measuring = 0;
  size_t sites = allocations, locks = f.calls;
  ok(rc, &e);
  REQUIRE(locks >= 17 && sites >= 4);
  REQUIRE(kernel_bytes() == plan.payload_bytes);
  ok(ps_budget_close(b, &e), &e);
  b = NULL;
  for (size_t site = 1; site <= sites; ++site) {
    allocations = 0;
    allocation_fail = site;
    measuring = 1;
    rc = ps_budget_open(s, &plan, &b, &e);
    measuring = 0;
    REQUIRE(rc == LC_ERR_NOMEM && !b && allocations >= site);
    lc_error_cleanup(&e);
    ok(ps_check(s, &e), &e);
    REQUIRE(kernel_bytes() == 0);
  }
  for (size_t site = 1; site <= locks; ++site) {
    f.calls = 0;
    f.fail = site;
    f.reason = EIO;
    REQUIRE(ps_budget_open(s, &plan, &b, &e) == LC_ERR_TRANSPORT && !b);
    f.fail = 0;
    lc_error_cleanup(&e);
    ok(ps_check(s, &e), &e);
    REQUIRE(kernel_bytes() == 0);
  }
  for (uint64_t id = 2; id <= 32; id += 2)
    verify(s, id, PS_PAGE, &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void faults(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *b = NULL;
  lock_fault f = {0};
  ps_coordination_io io = {&f, lock_call};
  ps_options options = {0};
  options.coordination_io = &io;
  ps_budget_plan plan = {4 * PS_PAGE, 4096, 4, 0};
  ok(ps_open(path, 1, &options, &s, &e), &e);
  allocations = allocation_fail = 0;
  measuring = 1;
  int rc = ps_budget_open(s, &plan, &b, &e);
  measuring = 0;
  size_t sites = allocations;
  ok(rc, &e);
  ok(ps_budget_close(b, &e), &e);
  b = NULL;
  REQUIRE(sites >= 4);
  for (int output = 0; output < 2; ++output)
    for (size_t site = 1; site <= sites; ++site) {
      allocations = 0;
      allocation_fail = site;
      measuring = 1;
      rc = ps_budget_open(s, &plan, &b, output ? &e : NULL);
      measuring = 0;
      REQUIRE(rc == LC_ERR_NOMEM && !b && allocations >= site);
      lc_error_cleanup(&e);
      ok(ps_check(s, &e), &e);
      REQUIRE(kernel_bytes() == 0);
    }
  for (size_t site = 1; site <= 2; ++site)
    for (int reason = 0; reason < 3; ++reason) {
      f.calls = 0;
      f.fail = site;
      f.reason = reason == 0 ? EINTR : reason == 1 ? EAGAIN : EIO;
      rc = ps_budget_open(s, &plan, &b, &e);
      f.fail = 0;
      if (!reason) {
        ok(rc, &e);
        ok(ps_budget_close(b, &e), &e);
        b = NULL;
      } else {
        REQUIRE(rc == (reason == 1 ? LC_ERR_PENDING : LC_ERR_TRANSPORT) && !b);
        lc_error_cleanup(&e);
      }
      ok(ps_check(s, &e), &e);
      REQUIRE(kernel_bytes() == 0);
    }
  ok(ps_budget_open(s, &plan, &b, &e), &e);
  f.calls = 0;
  f.fail = 1;
  f.reason = EIO;
  ok(ps_budget_close(b, &e), &e);
  f.fail = 0;
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_bytes() == 0);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  f.calls = 0;
  f.fail = 2;
  f.fail2 = 3;
  REQUIRE(ps_budget_open(s, &plan, &b, &e) == LC_ERR_TRANSPORT && !b);
  lc_error_cleanup(&e);
  f.fail = f.fail2 = 0;
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_bytes() == 0);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ps_writer *writers[2] = {NULL, NULL};
  unsigned char *body = malloc(PS_CHUNK);
  REQUIRE(body);
  for (size_t i = 0; i < 2; ++i) {
    memset(body, (int)(10 + i), PS_CHUNK);
    ok(ps_begin(s, NULL, key(10 + i), &writers[i], &e), &e);
    ok(ps_write(writers[i], body, PS_CHUNK, &e), &e);
  }
  f.calls = 0;
  f.fail = 1;
  REQUIRE(ps_revision_publish(writers, 2, 1, PS_COMMIT_DEFAULT, NULL, NULL,
                              &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  f.fail = 0;
  ps_writer_close(writers[0]);
  ps_writer_close(writers[1]);
  free(body);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_bytes() == 0);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  verify(s, 10, PS_CHUNK, &e);
  verify(s, 11, PS_CHUNK, &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
typedef struct gate {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int armed, entered, released;
  size_t bytes;
} gate;
static ssize_t read_io(void *c, int fd, void *p, size_t n, off_t o) {
  (void)c;
  return pread(fd, p, n, o);
}
static ssize_t write_io(void *c, int fd, const void *p, size_t n, off_t o) {
  gate *g = c;
  pthread_mutex_lock(&g->mutex);
  if (g->armed && n == g->bytes && o >= PS_DATA) {
    g->armed = 0;
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  return pwrite(fd, p, n, o);
}
static int sync_io(void *c, int fd) {
  (void)c;
  return fdatasync(fd);
}
static int reserve_io(void *c, int fd, off_t o, off_t n) {
  (void)c;
  return fallocate(fd, 0, o, n);
}
static void *producer(void *context) {
  lc_error e = {0};
  ok(publish(context, NULL, 1, 65, &e), &e);
  return NULL;
}
static void producers(void) {
  lc_error e = {0};
  gate g = {0};
  pthread_t thread;
  REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
          pthread_cond_init(&g.cond, NULL) == 0);
  ps_io io = {&g, read_io, write_io, sync_io, reserve_io, NULL};
  ps_options options = {0};
  options.io = &io;
  ps_space *s = NULL;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  g.armed = 1;
  g.bytes = 65;
  REQUIRE(pthread_create(&thread, NULL, producer, s) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.entered)
    pthread_cond_wait(&g.cond, &g.mutex);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(kernel_bytes() == 128 && status(s, &e).claimed_payload_bytes == 128);
  ok(publish(s, NULL, 2, 66, &e), &e);
  verify(s, 2, 66, &e);
  REQUIRE(kernel_bytes() == 128 && status(s, &e).claimed_payload_bytes == 128);
  ok(ps_checkpoint(s, &e), &e);
  REQUIRE(kernel_bytes() == 128);
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_join(thread, NULL) == 0);
  REQUIRE(kernel_bytes() == 0 && status(s, &e).claimed_payload_bytes == 0);
  verify(s, 1, 65, &e);
  verify(s, 2, 66, &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, NULL, &s, &e), &e);
  verify(s, 1, 65, &e);
  verify(s, 2, 66, &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
          pthread_mutex_destroy(&g.mutex) == 0);
}
typedef struct grant_work {
  ps_owned_head *head;
  ps_budget *budget;
  ps_owned_run *run;
} grant_work;
static void *grant_producer(void *context) {
  grant_work *work = context;
  lc_error e = {0};
  int found = 0;
  ok(ps_owned_grant(work->head, work->budget, 0, PS_PAGE, &work->run, &found,
                    &e),
     &e);
  REQUIRE(found);
  return NULL;
}
static void owned_producers(void) {
  lc_error e = {0};
  gate g = {0};
  pthread_t thread;
  ps_space *s = NULL;
  REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
          pthread_cond_init(&g.cond, NULL) == 0);
  ps_io io = {&g, read_io, write_io, sync_io, reserve_io, NULL};
  ps_options options = {0};
  options.io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e),
     &e); /* all free pages now uncertified */
  ps_key stream_key = {83, 1, PS_STREAM};
  ps_owned_status info;
  grant_work work = {0};
  ps_budget_plan plan = {PS_PAGE, PS_OWNED_DESCRIPTOR_BYTES, 1, 0};
  ok(ps_owned_open(s, NULL, stream_key, 1, NULL, &work.head, &info, &e), &e);
  ok(ps_budget_open(s, &plan, &work.budget, &e), &e);
  g.armed = 1;
  g.bytes = PS_RUN_HEADER;
  REQUIRE(pthread_create(&thread, NULL, grant_producer, &work) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.entered)
    pthread_cond_wait(&g.cond, &g.mutex);
  pthread_mutex_unlock(&g.mutex);
  /* The funded page and borrowed zero-preparation suffix remain one private
   * physical owner until the preparation barrier permits suffix reuse. */
  REQUIRE(kernel_bytes() == PS_MIB &&
          status(s, &e).claimed_payload_bytes == PS_MIB);
  REQUIRE(ps_budget_close(work.budget, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  ok(publish(s, NULL, 4, 32, &e), &e);
  verify(s, 4, 32, &e);
  REQUIRE(kernel_bytes() == PS_MIB);
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_join(thread, NULL) == 0);
  REQUIRE(kernel_bytes() == 0 && status(s, &e).claimed_payload_bytes == 0);
  ok(ps_budget_close(work.budget, &e), &e);
  ps_owned_run_close(work.run);
  ps_owned_close(work.head);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
  REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
          pthread_mutex_destroy(&g.mutex) == 0);
}
static void child_writer(int ready) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *b = NULL;
  ps_writer *w = NULL;
  ok(ps_open(path, 1, NULL, &s, &e), &e);
  ok(publish(s, NULL, 2, 81, &e), &e);
  ps_budget_plan plan = {PS_MIB, 4096, 4, 0};
  ok(ps_budget_open(s, &plan, &b, &e), &e);
  unsigned char *body = malloc(PS_CHUNK);
  REQUIRE(body);
  memset(body, 1, PS_CHUNK);
  ok(ps_begin(s, b, key(1), &w, &e), &e);
  ok(ps_write(w, body, PS_CHUNK, &e), &e);
  REQUIRE(write(ready, "R", 1) == 1);
  for (;;)
    pause();
}
static void processes(void) {
  int ready[2];
  REQUIRE(pipe(ready) == 0);
  pid_t pid = fork();
  REQUIRE(pid >= 0);
  if (!pid) {
    char fd[32];
    close(ready[0]);
    snprintf(fd, sizeof(fd), "%d", ready[1]);
    execl(executable, executable, "--child", path, fd, (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(ready[1]) == 0);
  char reply;
  REQUIRE(read(ready[0], &reply, 1) == 1 && reply == 'R');
  REQUIRE(kernel_bytes() == PS_MIB);
  REQUIRE(kill(pid, SIGKILL) == 0);
  int state;
  REQUIRE(waitpid(pid, &state, 0) == pid && WIFSIGNALED(state) &&
          WTERMSIG(state) == SIGKILL);
  REQUIRE(close(ready[0]) == 0 && kernel_bytes() == 0);
  lc_error e = {0};
  ps_space *s = NULL;
  ps_reader *r = NULL;
  ok(ps_open(path, 0, NULL, &s, &e), &e);
  verify(s, 2, 81, &e);
  REQUIRE(ps_read_open(s, key(1), NULL, &r, &e) == LC_ERR_INVALID && !r);
  lc_error_cleanup(&e);
  REQUIRE(status(s, &e).claimed_payload_bytes == 0);
  ok(publish(s, NULL, 3, PS_MIB, &e), &e);
  verify(s, 3, PS_MIB, &e);
  ok(ps_close(s, &e), &e);
  one_file();
  REQUIRE(unlink(path) == 0);
}
int main(int argc, char **argv) {
  if (argc == 4 && !strcmp(argv[1], "--child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    child_writer(atoi(argv[3]));
    return 0;
  }
  REQUIRE(argc == 3);
  ssize_t n = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
  REQUIRE(n > 0 && (size_t)n < sizeof(executable) - 1);
  executable[n] = 0;
  REQUIRE(snprintf(directory, sizeof(directory), "%s/physical-claims-XXXXXX",
                   argv[1]) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  if (!strcmp(argv[2], "lifecycle"))
    lifecycle();
  else if (!strcmp(argv[2], "faults")) {
    faults();
    fragmented_faults();
  } else if (!strcmp(argv[2], "producers")) {
    producers();
    owned_producers();
  } else {
    REQUIRE(!strcmp(argv[2], "processes"));
    processes();
  }
  REQUIRE(rmdir(directory) == 0);
  fprintf(stderr, "physical claims %s: %zu checks\n", argv[2], checks);
  return 0;
}
