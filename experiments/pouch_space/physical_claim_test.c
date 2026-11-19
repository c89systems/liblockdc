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
typedef struct held_image {
  span range;
  unsigned char *bytes;
  struct held_image *next;
} held_image;
static held_image *images;
static int capturing;
static uint64_t kernel_region(uint64_t start, uint64_t end) {
  span *pending = calloc(4096, sizeof(*pending));
  REQUIRE(pending);
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(fd >= 0);
  struct stat st;
  REQUIRE(fstat(fd, &st) == 0);
  size_t count = 1;
  uint64_t total = 0;
  pending[0].start = start;
  pending[0].end = end ? end : (uint64_t)st.st_size;
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
    if (capturing) {
      held_image *saved = calloc(1, sizeof(*saved));
      REQUIRE(saved);
      saved->range.start = first;
      saved->range.end = last;
      saved->bytes = malloc((size_t)(last - first));
      REQUIRE(saved->bytes);
      REQUIRE(pread(fd, saved->bytes, (size_t)(last - first), (off_t)first) ==
              (ssize_t)(last - first));
      saved->next = images;
      images = saved;
    }
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
static uint64_t kernel_bytes(void) { return kernel_region(PS_DATA, 0); }
static uint64_t kernel_metadata(void) {
  return kernel_region(2 * PS_PAGE, PS_JOURNAL_OFFSET) + kernel_bytes();
}
static void verify_images(void) {
  int fd = open(path, O_RDONLY | O_CLOEXEC);
  REQUIRE(fd >= 0);
  while (images) {
    held_image *saved = images;
    images = saved->next;
    size_t bytes = (size_t)(saved->range.end - saved->range.start);
    unsigned char *current = malloc(bytes);
    REQUIRE(current);
    REQUIRE(pread(fd, current, bytes, (off_t)saved->range.start) ==
            (ssize_t)bytes);
    REQUIRE(!memcmp(current, saved->bytes, bytes));
    free(current);
    free(saved->bytes);
    free(saved);
  }
  REQUIRE(close(fd) == 0);
}
typedef struct lock_fault {
  size_t calls, fail, fail2;
  int reason, failed_release;
} lock_fault;
static int lock_call(void *context, int fd, int command, struct flock *lock) {
  lock_fault *f = context;
  if (command == F_OFD_SETLK) {
    ++f->calls;
    if (f->calls == f->fail || f->calls == f->fail2) {
      f->failed_release = lock->l_type == F_UNLCK;
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
  f.calls = 0;
  allocations = allocation_fail = 0;
  measuring = 1;
  int rc = ps_budget_open(s, &plan, &b, &e);
  measuring = 0;
  size_t sites = allocations, locks = f.calls;
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
  for (size_t site = 1; site <= locks; ++site)
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
  /* Metadata now retains the presence witness. Use fragmented payload claims
   * to reach a second acquisition and its rollback release independently. */
  for (uint64_t id = 1; id <= 8; ++id)
    ok(publish(s, NULL, id, PS_PAGE, &e), &e);
  for (uint64_t id = 1; id <= 8; id += 2)
    ok(ps_delete(s, key(id), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
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
static void metadata_lifecycle(void) {
  for (int sync = 0; sync < 2; ++sync) {
    lc_error e = {0};
    ps_space *s = NULL;
    ps_budget *budget = NULL, *peer = NULL;
    ps_options options = {0};
    lock_fault f = {0};
    ps_coordination_io io = {&f, lock_call};
    options.coordination_io = &io;
    options.durable_sync = sync;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    ps_stats st = status(s, &e);
    REQUIRE(st.claimed_metadata_bytes == kernel_metadata() &&
            st.claimed_payload_bytes == 0);
    /* Admission crosses the fixed-region capacity and several directory
     * levels. All promised metadata is observable from an independent OFD. */
    ps_budget_plan plan = {0, 512 * 1024, 4096, 0};
    ok(ps_budget_open(s, &plan, &budget, &e), &e);
    ps_budget_plan empty = {0};
    ok(ps_budget_open(s, &empty, &peer, &e), &e);
    st = status(s, &e);
    REQUIRE(st.metadata_available_bytes > PS_METADATA_RESERVE &&
            st.claimed_metadata_bytes == kernel_metadata() &&
            st.claimed_metadata_bytes == st.metadata_available_bytes);
    size_t before = f.calls;
    allocations = 0;
    allocation_fail = 1;
    measuring = 1;
    int rc = ps_budget_transfer(budget, peer, &plan, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations && f.calls == before);
    /* Selections and deletions around 31/32 and 961/962 entries exercise
     * image growth, shrink, COW reuse and the forecast's lifetime. */
    for (uint64_t id = 1; id <= 994; ++id) {
      ok(publish(s, peer, id, 0, &e), &e);
      if (id == 31 || id == 32 || id == 961 || id == 962 || id == 994) {
        ok(ps_checkpoint(s, &e), &e);
        st = status(s, &e);
        REQUIRE(st.claimed_metadata_bytes == kernel_metadata() &&
                st.metadata_available_bytes >= st.metadata_workspace_bytes &&
                st.claimed_metadata_bytes == st.metadata_available_bytes);
      }
    }
    for (uint64_t id = 32; id <= 994; ++id)
      ok(ps_delete(s, key(id), &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    before = f.calls;
    allocations = 0;
    measuring = 1;
    rc = ps_budget_close(peer, &e);
    measuring = 0;
    ok(rc, &e);
    REQUIRE(!allocations && f.calls > before);
    ok(ps_budget_close(budget, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    st = status(s, &e);
    REQUIRE(st.claimed_metadata_bytes == kernel_metadata() &&
            st.claimed_payload_bytes == 0 && !st.budgets &&
            !st.reserved_metadata_bytes && !st.reserved_entries &&
            st.claimed_metadata_bytes < 512 * 1024);
    ok(ps_close(s, &e), &e);
    REQUIRE(kernel_metadata() == 0);
    ok(ps_open(path, 0, &options, &s, &e), &e);
    for (uint64_t id = 1; id <= 31; ++id)
      verify(s, id, 0, &e);
    ok(ps_checkpoint(s, &e), &e);
    REQUIRE(status(s, &e).claimed_metadata_bytes == kernel_metadata());
    ok(ps_close(s, &e), &e);
    one_file();
    REQUIRE(unlink(path) == 0);
  }
}
static void metadata_faults(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *budget = NULL;
  ps_options options = {0};
  lock_fault f = {0};
  ps_coordination_io io = {&f, lock_call};
  options.coordination_io = &io;
  /* Creation failures consume no FD authority. Initial metadata ownership
   * installs a presence witness followed by its fixed-region witness. */
  for (size_t site = 1; site <= 2; ++site) {
    f.calls = 0;
    f.fail = site;
    f.reason = EIO;
    REQUIRE(ps_open(path, 1, &options, &s, &e) == LC_ERR_TRANSPORT && !s);
    f.fail = 0;
    lc_error_cleanup(&e);
    REQUIRE(kernel_metadata() == 0 && unlink(path) == 0);
  }
  ok(ps_open(path, 1, &options, &s, &e), &e);
  for (uint64_t id = 1; id <= 32; ++id)
    ok(publish(s, NULL, id, 0, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ps_stats prior = status(s, &e);
  ps_budget_plan plan = {0, 512 * 1024, 4096, 0};
  for (int output = 0; output < 2; ++output) {
    f.calls = 0;
    f.fail = 1;
    f.reason = EIO;
    REQUIRE(ps_budget_open(s, &plan, &budget, output ? &e : NULL) ==
                LC_ERR_TRANSPORT &&
            !budget);
    f.fail = 0;
    lc_error_cleanup(&e);
    ok(ps_check(s, &e), &e);
    REQUIRE(status(s, &e).claimed_metadata_bytes ==
                prior.claimed_metadata_bytes &&
            kernel_metadata() == prior.claimed_metadata_bytes);
  }
  allocations = allocation_fail = 0;
  measuring = 1;
  int rc = ps_budget_open(s, &plan, &budget, &e);
  measuring = 0;
  size_t sites = allocations;
  ok(rc, &e);
  ok(ps_budget_close(budget, &e), &e);
  for (size_t site = 1; site <= sites; ++site) {
    allocations = 0;
    allocation_fail = site;
    measuring = 1;
    rc = ps_budget_open(s, &plan, &budget, &e);
    measuring = 0;
    REQUIRE(rc == LC_ERR_NOMEM && !budget);
    lc_error_cleanup(&e);
    ok(ps_check(s, &e), &e);
    REQUIRE(kernel_metadata() == status(s, &e).claimed_metadata_bytes);
  }
  /* A clean acquisition error leaves a healthy owner; an uncertain release
   * ends admission. Close releases all remaining authority and reopen keeps
   * the acknowledged directory. */
  ok(ps_budget_open(s, &plan, &budget, &e), &e);
  f.calls = 0;
  f.fail = 1;
  f.reason = EIO;
  ok(ps_budget_close(budget, &e), &e);
  f.fail = 0;
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_metadata() == 0);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  for (uint64_t id = 1; id <= 32; ++id)
    verify(s, id, 0, &e);
  ok(ps_close(s, &e), &e);
  f.calls = 0;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  size_t reopen_locks = f.calls;
  ok(ps_close(s, &e), &e);
  for (size_t site = 1; site <= reopen_locks; ++site) {
    f.calls = 0;
    f.fail = site;
    f.reason = EIO;
    REQUIRE(ps_open(path, 0, &options, &s, &e) == LC_ERR_TRANSPORT && !s);
    f.fail = 0;
    lc_error_cleanup(&e);
    REQUIRE(kernel_metadata() == 0);
  }
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ok(ps_budget_open(s, &plan, &budget, &e), &e);
  ps_budget_plan empty = {0};
  f.calls = 0;
  f.fail = 1;
  REQUIRE(ps_budget_resize(budget, &empty, &e) == LC_ERR_TRANSPORT);
  f.fail = 0;
  lc_error_cleanup(&e);
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  ok(ps_budget_close(budget, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  verify(s, 1, 0, &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_metadata() == 0 && unlink(path) == 0);
}
static void metadata_fragmented_faults(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *budget = NULL;
  ps_options options = {0};
  lock_fault f = {0};
  ps_coordination_io io = {&f, lock_call};
  options.coordination_io = &io;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  for (uint64_t id = 1; id <= 48; ++id)
    ok(publish(s, NULL, id, 128 * 1024, &e), &e);
  for (uint64_t id = 1; id <= 48; id += 2)
    ok(ps_delete(s, key(id), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  uint64_t initial = kernel_metadata();
  ps_budget_plan plan = {0, PS_MIB, 8192, 0};
  f.calls = allocations = allocation_fail = 0;
  measuring = 1;
  int rc = ps_budget_open(s, &plan, &budget, &e);
  measuring = 0;
  size_t sites = allocations, locks = f.calls;
  ok(rc, &e);
  REQUIRE(locks > 2);
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(kernel_metadata() == initial);
  for (size_t site = 1; site <= sites; ++site) {
    allocations = 0;
    allocation_fail = site;
    measuring = 1;
    rc = ps_budget_open(s, &plan, &budget, &e);
    measuring = 0;
    REQUIRE(rc == LC_ERR_NOMEM && !budget);
    lc_error_cleanup(&e);
    ok(ps_check(s, &e), &e);
    REQUIRE(kernel_metadata() == initial);
  }
  for (size_t site = 1; site <= locks; ++site)
    for (int reason = 0; reason < 3; ++reason) {
      f.calls = 0;
      f.fail = site;
      f.reason = reason == 0 ? EINTR : reason == 1 ? EAGAIN : EIO;
      rc = ps_budget_open(s, &plan, &budget, &e);
      f.fail = 0;
      if (!reason) {
        ok(rc, &e);
        ok(ps_budget_close(budget, &e), &e);
      } else {
        REQUIRE(rc == (reason == 1 ? LC_ERR_PENDING : LC_ERR_TRANSPORT) &&
                !budget);
        lc_error_cleanup(&e);
      }
      ok(ps_check(s, &e), &e);
      REQUIRE(kernel_metadata() == initial);
    }
  f.calls = 0;
  f.fail = 2;
  f.fail2 = 3;
  f.reason = EIO;
  REQUIRE(ps_budget_open(s, &plan, &budget, &e) == LC_ERR_TRANSPORT && !budget);
  f.fail = f.fail2 = 0;
  lc_error_cleanup(&e);
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  ok(ps_close(s, &e), &e);
  REQUIRE(kernel_metadata() == 0);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  for (uint64_t id = 2; id <= 48; id += 2)
    verify(s, id, 128 * 1024, &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void metadata_checkpoint_faults(void) {
  size_t locks = 0, sites = 0;
  /* Learn the actual checkpoint's acquisition/release and allocation sites,
   * then fault each once from the same canonical directory and dirty update.
   * The old value's descriptor spans several pages; its first page retains
   * other selected entries. This covers both retirement and partial-page
   * cleanup without leaving selected pages privately claimed. */
  for (size_t attempt = 0; attempt <= locks + sites; ++attempt) {
    lc_error e = {0};
    ps_space *s = NULL;
    ps_budget *budget = NULL;
    ps_reader *captured = NULL;
    ps_options options = {0};
    lock_fault f = {0};
    ps_coordination_io io = {&f, lock_call};
    options.coordination_io = &io;
    ok(ps_open(path, 1, &options, &s, &e), &e);
    for (uint64_t id = 1; id <= 32; ++id)
      ok(publish(s, NULL, id, id == 32 ? 4 * PS_MIB : 0, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    /* Keep body reclamation outside the faulted publication operation. The
     * captured value remains valid while its old metadata pages retire. */
    ok(ps_read_open(s, key(32), NULL, &captured, &e), &e);
    REQUIRE(ps_read_size(captured) == 4 * PS_MIB);
    ps_budget_plan plan = {0, 512 * 1024, 4096, 0};
    ok(ps_budget_open(s, &plan, &budget, &e), &e);
    ok(publish(s, NULL, 32, 0, &e), &e);
    f.calls = 0;
    f.reason = EIO;
    f.fail = attempt <= locks ? attempt : 0;
    allocations = 0;
    allocation_fail = attempt > locks ? attempt - locks : 0;
    measuring = 1;
    int rc = ps_checkpoint(s, &e);
    measuring = 0;
    f.fail = 0;
    if (!attempt) {
      locks = f.calls;
      sites = allocations;
      REQUIRE(locks >= 4 && sites > 0);
      ok(rc, &e);
    } else {
      REQUIRE(rc == (attempt <= locks ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
      lc_error_cleanup(&e);
      int check = ps_check(s, &e);
      REQUIRE(check == (f.failed_release ? LC_ERR_TRANSPORT : LC_OK));
      lc_error_cleanup(&e);
      if (!f.failed_release) {
        ps_stats st = status(s, &e);
        /* A failed checkpoint may retain its optional incremental spare;
         * every witness must still belong to free workspace, and the full
         * admitted forecast remains protected. */
        REQUIRE(kernel_metadata() == st.claimed_metadata_bytes &&
                st.claimed_metadata_bytes == st.metadata_available_bytes &&
                st.metadata_available_bytes >= st.metadata_workspace_bytes);
        ok(ps_checkpoint(s, &e), &e);
      }
    }
    ps_reader_close(captured);
    ok(ps_budget_close(budget, &e), &e);
    ok(ps_close(s, &e), &e);
    REQUIRE(kernel_metadata() == 0);
    ok(ps_open(path, 0, &options, &s, &e), &e);
    for (uint64_t id = 1; id <= 32; ++id)
      verify(s, id, 0, &e);
    ok(ps_close(s, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
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
typedef struct funded_work {
  ps_space *space;
  ps_budget *budget;
} funded_work;
static void *funded_producer(void *context) {
  funded_work *work = context;
  lc_error e = {0};
  ok(publish(work->space, work->budget, 1, 65, &e), &e);
  return NULL;
}
static void metadata_producers(void) {
  lc_error e = {0};
  gate g = {0};
  pthread_t thread;
  funded_work work = {0};
  REQUIRE(pthread_mutex_init(&g.mutex, NULL) == 0 &&
          pthread_cond_init(&g.cond, NULL) == 0);
  ps_io io = {&g, read_io, write_io, sync_io, reserve_io, NULL};
  ps_options options = {0};
  options.io = &io;
  ok(ps_open(path, 1, &options, &work.space, &e), &e);
  ps_budget_plan plan = {PS_PAGE, 512 * 1024, 4096, 0};
  ok(ps_budget_open(work.space, &plan, &work.budget, &e), &e);
  g.armed = 1;
  g.bytes = 65;
  REQUIRE(pthread_create(&thread, NULL, funded_producer, &work) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.entered)
    pthread_cond_wait(&g.cond, &g.mutex);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(ps_budget_close(work.budget, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  for (uint64_t id = 2; id <= 64; ++id) {
    ok(publish(work.space, NULL, id, 0, &e), &e);
    ok(ps_checkpoint(work.space, &e), &e);
    ps_stats st = status(work.space, &e);
    REQUIRE(st.metadata_available_bytes >= st.metadata_workspace_bytes &&
            st.claimed_metadata_bytes + st.claimed_payload_bytes ==
                kernel_metadata() &&
            st.claimed_payload_bytes == 128);
  }
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.cond);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_join(thread, NULL) == 0);
  ok(ps_budget_close(work.budget, &e), &e);
  verify(work.space, 1, 65, &e);
  ok(ps_checkpoint(work.space, &e), &e);
  ps_stats st = status(work.space, &e);
  REQUIRE(!st.claimed_payload_bytes &&
          st.claimed_metadata_bytes == kernel_metadata());
  ok(ps_close(work.space, &e), &e);
  REQUIRE(kernel_metadata() == 0 && unlink(path) == 0);
  REQUIRE(pthread_cond_destroy(&g.cond) == 0 &&
          pthread_mutex_destroy(&g.mutex) == 0);
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
static void child_metadata(int ready, int control) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_budget *budget = NULL;
  ok(ps_open(path, 1, NULL, &s, &e), &e);
  ok(publish(s, NULL, 2, 81, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ps_budget_plan plan = {0, 512 * 1024, 4096, 0};
  ok(ps_budget_open(s, &plan, &budget, &e), &e);
  ps_stats st = status(s, &e);
  REQUIRE(write(ready, &st, sizeof(st)) == (ssize_t)sizeof(st));
  char command;
  REQUIRE(read(control, &command, 1) == 1 && command == 'B');
  ok(ps_budget_close(budget, &e), &e);
  REQUIRE(write(ready, "B", 1) == 1);
  REQUIRE(read(control, &command, 1) == 1 && command == 'C');
  ok(ps_close(s, &e), &e);
  REQUIRE(write(ready, "C", 1) == 1);
  REQUIRE(close(ready) == 0 && close(control) == 0);
}
static void metadata_processes(void) {
  /* Publication has exactly one active canonical allocator at a time. The
   * other process retains real metadata credits but
   * performs no further publication. This isolates peer resource exclusion
   * from the pending incremental projection/allocator-refresh integration. */
  for (int killed = 0; killed < 2; ++killed) {
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
      execl(executable, executable, "--metadata-child", path, a, b,
            (char *)NULL);
      _exit(127);
    }
    REQUIRE(close(ready[1]) == 0 && close(control[0]) == 0);
    ps_stats foreign;
    REQUIRE(read(ready[0], &foreign, sizeof(foreign)) ==
            (ssize_t)sizeof(foreign));
    REQUIRE(!foreign.claimed_payload_bytes &&
            foreign.claimed_metadata_bytes > PS_METADATA_RESERVE);
    capturing = 1;
    REQUIRE(kernel_metadata() == foreign.claimed_metadata_bytes);
    capturing = 0;
    lc_error e = {0};
    ps_space *s = NULL;
    ok(ps_open(path, 0, NULL, &s, &e), &e);
    ps_stats initial = status(s, &e);
    REQUIRE(initial.retired_bytes >= foreign.metadata_available_bytes &&
            initial.metadata_available_bytes >=
                initial.metadata_workspace_bytes);
    for (uint64_t id = 3; id <= 1000; ++id)
      ok(publish(s, NULL, id, 0, &e), &e);
    ok(publish(s, NULL, 1001, 65536, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    verify(s, 2, 81, &e);
    verify(s, 1001, 65536, &e);
    /* Churn the selected directory while the private workspace remains held.
     * Canonical pages can be retired without taking the peer's future pool. */
    for (int round = 0; round < 8; ++round) {
      ok(publish(s, NULL, 1003, 0, &e), &e);
      ok(ps_checkpoint(s, &e), &e);
    }
    verify_images();
    char reply;
    int state;
    if (killed) {
      REQUIRE(kill(pid, SIGKILL) == 0);
      REQUIRE(waitpid(pid, &state, 0) == pid && WIFSIGNALED(state) &&
              WTERMSIG(state) == SIGKILL);
    } else {
      REQUIRE(write(control[1], "B", 1) == 1 &&
              read(ready[0], &reply, 1) == 1 && reply == 'B');
      ok(ps_checkpoint(s, &e), &e);
      REQUIRE(status(s, &e).retired_bytes < initial.retired_bytes);
      REQUIRE(write(control[1], "C", 1) == 1 &&
              read(ready[0], &reply, 1) == 1 && reply == 'C');
      REQUIRE(waitpid(pid, &state, 0) == pid && WIFEXITED(state) &&
              WEXITSTATUS(state) == 0);
    }
    REQUIRE(close(ready[0]) == 0 && close(control[1]) == 0);
    ok(ps_checkpoint(s, &e), &e);
    ps_stats drained = status(s, &e);
    REQUIRE(drained.retired_bytes == 0 &&
            kernel_metadata() == drained.claimed_metadata_bytes);
    ok(publish(s, NULL, 1002, 2 * PS_MIB, &e), &e);
    verify(s, 1002, 2 * PS_MIB, &e);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_close(s, &e), &e);
    REQUIRE(kernel_metadata() == 0);
    ok(ps_open(path, 0, NULL, &s, &e), &e);
    verify(s, 2, 81, &e);
    verify(s, 1001, 65536, &e);
    verify(s, 1002, 2 * PS_MIB, &e);
    ok(ps_close(s, &e), &e);
    one_file();
    REQUIRE(unlink(path) == 0);
  }
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
  if (argc == 5 && !strcmp(argv[1], "--metadata-child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    child_metadata(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
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
  else if (!strcmp(argv[2], "metadata-lifecycle"))
    metadata_lifecycle();
  else if (!strcmp(argv[2], "metadata-faults")) {
    metadata_faults();
    metadata_fragmented_faults();
    metadata_checkpoint_faults();
  } else if (!strcmp(argv[2], "metadata-producers"))
    metadata_producers();
  else if (!strcmp(argv[2], "metadata-processes"))
    metadata_processes();
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
