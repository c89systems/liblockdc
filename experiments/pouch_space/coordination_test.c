#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "budget.h"
#include "coordination.h"
#include "lc_api_internal.h"
#include "namespace_metadata.h"
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static size_t checks, allocations, allocation_fail;
static int measuring;
static size_t mutex_calls, mutex_fail;
static int mutex_armed;
static char path[1400];
#define REQUIRE(x) do { \
  __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED); \
  if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); abort(); } \
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
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n, size_t b) {
  return allocation_fails() ? NULL : __real_lc_calloc_with_allocator(a, n, b);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p, size_t n) {
  return allocation_fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return allocation_fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
int __real_pthread_mutex_init(pthread_mutex_t *, const pthread_mutexattr_t *);
int __wrap_pthread_mutex_init(pthread_mutex_t *mutex,
                               const pthread_mutexattr_t *attributes) {
  if (mutex_armed && ++mutex_calls == mutex_fail)
    return EAGAIN;
  return __real_pthread_mutex_init(mutex, attributes);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "coordination %d: %s\n", rc, e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static int file_open(void) {
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(fd >= 0);
  return fd;
}
static void file_create(void) {
  REQUIRE(unlink(path) == 0 || errno == ENOENT);
  int fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  REQUIRE(fd >= 0 && fallocate(fd, 0, 0, PS_DEFAULT_CAPACITY) == 0);
  REQUIRE(close(fd) == 0);
}
static ps_coordination *owner_open(int shared, const ps_coordination_io *io,
                                   lc_error *e) {
  ps_coordination_options options = {0};
  ps_coordination *owner = NULL;
  options.shared = shared;
  options.io = io;
  ok(ps_coordination_open(file_open(), &options, &owner, e), e);
  return owner;
}
static void pipe_write(int fd, char value) {
  ssize_t n;
  do { n = write(fd, &value, 1); } while (n < 0 && errno == EINTR);
  REQUIRE(n == 1);
}
static char pipe_read(int fd) {
  char value;
  ssize_t n;
  do { n = read(fd, &value, 1); } while (n < 0 && errno == EINTR);
  REQUIRE(n == 1);
  return value;
}
typedef struct syscall_fault {
  size_t calls, fail;
  int reason;
} syscall_fault;
static int guarded_fcntl(void *context, int fd, int command, struct flock *lock) {
  syscall_fault *fault = context;
  if (++fault->calls == fault->fail) {
    errno = fault->reason;
    return -1;
  }
  return fcntl(fd, command, lock);
}
static ssize_t storage_read(void *context, int fd, void *bytes, size_t n,
                               off_t at) {
  (void)context;
  return pread(fd, bytes, n, at);
}
static ssize_t storage_write(void *context, int fd, const void *bytes, size_t n,
                                off_t at) {
  int *reject = context;
  if (*reject) {
    errno = EIO;
    return -1;
  }
  return pwrite(fd, bytes, n, at);
}
static int storage_sync(void *context, int fd) {
  (void)context;
  return fdatasync(fd);
}
static int storage_reserve(void *context, int fd, off_t at, off_t n) {
  (void)context;
  return fallocate(fd, 0, at, n);
}
static void large_offsets(void) {
  lc_error e = {0};
  uint64_t first, last;
  const uint64_t offset = PS_MAX_CAPACITY + PS_DATA;
  file_create();
  ps_coordination *a = owner_open(1, NULL, &e);
  ps_coordination *b = owner_open(1, NULL, &e);

  ok(ps_coordination_range(a, offset, PS_PAGE, 1, &e), &e);
  ok(ps_coordination_conflict(b, offset, PS_PAGE, &first, &last, &e), &e);
  REQUIRE(first == offset && last == offset + PS_PAGE);
  ok(ps_coordination_conflict(b, PS_DATA, PS_PAGE, &first, &last, &e), &e);
  REQUIRE(!first && !last);
  ok(ps_coordination_range(a, offset, PS_PAGE, 0, &e), &e);
  ok(ps_coordination_conflict(b, offset, PS_PAGE, &first, &last, &e), &e);
  REQUIRE(!first && !last);
  ok(ps_coordination_close(b, &e), &e);
  ok(ps_coordination_close(a, &e), &e);
  lc_error_cleanup(&e);
}
static void lifecycle(void) {
  lc_error e;
  lc_error_init(&e);
  REQUIRE(unlink(path) == 0 || errno == ENOENT);
  ps_space *space = NULL, *other = NULL;
  ok(ps_open(path, 1, NULL, &space, &e), &e);
  ps_coordination *owner = ps_space_coordination(space);
  ps_coordination_guard *guard = NULL, *busy = NULL;
  allocations = 0;
  allocation_fail = 1;
  measuring = 1;
  int rc =
      ps_coordination_lock(owner, PS_COORDINATION_ALLOCATION, 0, 0, &guard, &e);
  measuring = 0;
  ok(rc, &e);
  REQUIRE(!allocations && ps_coordination_users(owner) == 1);
  REQUIRE(ps_close(space, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  REQUIRE(ps_open(path, 0, NULL, &other, &e) == LC_ERR_INVALID && !other);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(owner, PS_COORDINATION_ALLOCATION, 0, 0, &busy,
                               &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  ok(ps_close(space, &e), &e);
  space = NULL;

  syscall_fault exclusive_calls = {0};
  ps_coordination_io exclusive_io = {&exclusive_calls, guarded_fcntl};
  ps_coordination *exclusive = owner_open(0, &exclusive_io, &e);
  ok(ps_coordination_range(exclusive, PS_DATA, PS_PAGE, 1, &e), &e);
  ok(ps_coordination_range(exclusive, PS_DATA, PS_PAGE, 0, &e), &e);
  REQUIRE(exclusive_calls.calls == 0);
  ok(ps_coordination_close(exclusive, &e), &e);

  int invalid_fd = file_open();
  ps_coordination_options invalid = {0};
  invalid.shared = 2;
  REQUIRE(ps_coordination_open(invalid_fd, &invalid, &owner, &e) ==
              LC_ERR_INVALID && !owner);
  lc_error_cleanup(&e);
  REQUIRE(fcntl(invalid_fd, F_GETFD) >= 0);
  REQUIRE(!flock(invalid_fd, LOCK_EX | LOCK_NB));
  REQUIRE(!close(invalid_fd));
  REQUIRE(ps_coordination_open(-1, NULL, &owner, &e) == LC_ERR_INVALID &&
          !owner);
  lc_error_cleanup(&e);

  ps_coordination *a = owner_open(1, NULL, &e);
  ps_coordination *b = owner_open(1, NULL, &e);
  int fd = file_open();
  REQUIRE(ps_coordination_open(fd, NULL, &owner, &e) == LC_ERR_PENDING && !owner);
  REQUIRE(fcntl(fd, F_GETFD) >= 0 && close(fd) == 0);
  lc_error_cleanup(&e);
  for (int domain = PS_COORDINATION_PUBLICATION;
       domain <= PS_COORDINATION_NAMESPACE; ++domain) {
    uint64_t identity = domain == PS_COORDINATION_TRANSACTION ? 255 :
                        domain == PS_COORDINATION_NAMESPACE ? UINT64_MAX : 0;
    guard = busy = NULL;
    ok(ps_coordination_lock(a, domain, identity, 0, &guard, &e), &e);
    REQUIRE(ps_coordination_close(a, &e) == LC_ERR_INVALID);
    lc_error_cleanup(&e);
    /* Closing an unrelated description must not drop OFD authority. */
    REQUIRE(close(file_open()) == 0);
    REQUIRE(ps_coordination_lock(b, domain, identity, 0, &busy, &e) ==
                LC_ERR_PENDING && !busy);
    lc_error_cleanup(&e);
    ps_coordination_unlock(guard);
    ok(ps_coordination_lock(b, domain, identity, 0, &busy, &e), &e);
    ps_coordination_unlock(busy);
  }
  /* Deliberately colliding namespace identities must share the local gate as
   * well as the kernel byte; one must never release the other's OFD lock. */
  ok(ps_coordination_lock(a, PS_COORDINATION_NAMESPACE, 0, 0, &guard, &e), &e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_NAMESPACE, UINT64_C(7317), 0,
                               &busy, &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_NAMESPACE, 0, 0, &busy, &e) ==
              LC_ERR_PENDING && !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  ok(ps_coordination_lock(a, PS_COORDINATION_CONTROL_STATE, 0, 0, &guard, &e),
     &e);
  ok(ps_coordination_lock(b, PS_COORDINATION_CONTROL_STATE, 1, 0, &busy, &e),
     &e);
  ps_coordination_unlock(busy);
  ok(ps_coordination_lock(b, PS_COORDINATION_NAMESPACE_STATE, 0, 0, &busy, &e),
     &e);
  ps_coordination_unlock(busy);
  ps_coordination_unlock(guard);
  ps_coordination_guard *second = NULL, *foreign = NULL;
  uint64_t collision = 1 + PS_COORDINATION_STREAM_CLAIM_SLOTS;
  ok(ps_coordination_stream_prepare(a, 1, &guard, &e), &e);
  ok(ps_coordination_stream_prepare(a, collision, &second, &e), &e);
  ok(ps_coordination_stream_prepare(b, collision, &foreign, &e), &e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_STREAM_CLAIM, 1, 0, &busy,
                               &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  ps_coordination_unlock(foreign);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_STREAM_CLAIM, 1, 0, &busy,
                               &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(second);
  ok(ps_coordination_lock(b, PS_COORDINATION_STREAM_CLAIM, 1, 0, &busy, &e),
     &e);
  REQUIRE(ps_coordination_stream_prepare(b, collision, &guard, &e) ==
              LC_ERR_PENDING &&
          !guard);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_stream_prepare(a, collision, &guard, &e) ==
              LC_ERR_PENDING &&
          !guard);
  lc_error_cleanup(&e);
  ps_coordination_unlock(busy);
  guard = (ps_coordination_guard *)1;
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_CONTROL_STATE, 2, 0, &guard,
                               &e) == LC_ERR_INVALID &&
          !guard);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION,
                               PS_TRANSACTION_GUARD_SLOTS, 0, &guard,
                               &e) == LC_ERR_INVALID &&
          !guard);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_PUBLICATION, 1, 0, &guard,
                                &e) == LC_ERR_INVALID && !guard);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_NAMESPACE, 0, 2, &guard,
                                &e) == LC_ERR_INVALID && !guard);
  lc_error_cleanup(&e);
  ok(ps_coordination_close(a, &e), &e);
  ok(ps_coordination_close(b, &e), &e);
  ok(ps_open(path, 0, NULL, &space, &e), &e);
  ps_stats status;
  ok(ps_status(space, &status, &e), &e);
  REQUIRE(status.capacity == PS_DEFAULT_CAPACITY);
  ok(ps_close(space, &e), &e);
  int reject = 0;
  ps_io io = {&reject, storage_read, storage_write, storage_sync,
               storage_reserve, NULL};
  ps_options options = {0};
  options.io = &io;
  ok(ps_open(path, 0, &options, &space, &e), &e);
  ps_writer *writer = NULL;
  ps_key key = {1, 1, PS_STATE};
  ok(ps_begin(space, NULL, key, &writer, &e), &e);
  unsigned char chunk[2048] = {0};
  reject = 1;
  int result = LC_OK;
  for (size_t i = 0; result == LC_OK && i < PS_CHUNK / sizeof(chunk); ++i)
    result = ps_write(writer, chunk, sizeof(chunk), &e);
  REQUIRE(result == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_check(ps_space_coordination(space), &e) ==
          LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  guard = NULL;
  REQUIRE(ps_coordination_lock(ps_space_coordination(space),
                                PS_COORDINATION_TRANSACTION, 0, 0, &guard,
                                &e) == LC_ERR_TRANSPORT && !guard);
  lc_error_cleanup(&e);
  ps_writer_close(writer);
  ok(ps_close(space, &e), &e);
  ok(ps_open(path, 0, NULL, &space, &e), &e);
  ok(ps_close(space, &e), &e);
  lc_error_cleanup(&e);
}
typedef struct root_records {
  ps_checkpoint_record records[2];
  lc_pouch_record_header headers[2];
} root_records;
static int root_prefix(void *context, size_t at, lc_pouch_record_header *header,
                         const void **key, const void **metadata, lc_error *e) {
  root_records *r = context;
  (void)e;
  *header = r->headers[at];
  *key = header->key_len ? r->records[at].bytes + 32 : NULL;
  *metadata = r->records[at].bytes + 32 + header->key_len;
  return LC_OK;
}
static void root_lifetime(void) {
  lc_error e;
  lc_error_init(&e);
  REQUIRE(unlink(path) == 0 || errno == ENOENT);
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_metadata *metadata = NULL;
  root_records records = {0};
  uint64_t bytes = 0;
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ok(ps_root_namespace_acquire(root, "lease-owner", 1, &ns, &e), &e);
  for (size_t i = 0; i < 2; ++i) {
    lc_pouch_state_entry entry = {0};
    entry.seen = 1;
    entry.index_seq = i + 1;
    entry.record_type = i ? LC_POUCH_STATE_RECORD_HIGH_WATER
                          : LC_POUCH_STATE_RECORD_STATE_META;
    entry.control = i != 0;
    entry.found = !i;
    entry.key = i ? NULL : "live-lease";
    entry.version = i ? 0 : 1;
    entry.etag = i ? NULL : "empty";
    entry.content_type = i ? NULL : "application/test";
    ok(ps_checkpoint_record_encode(NULL, &entry, &records.records[i], &e), &e);
    ok(lc_pouch_record_header_decode(records.records[i].bytes,
                                      &records.headers[i], &e), &e);
    bytes += records.records[i].length;
  }
  lc_pouch_storage_batch batch = {2, root_prefix, &records};
  ok(ps_namespace_metadata_begin(ns, 0, &batch, bytes, &metadata, &e), &e);
  ok(ps_namespace_metadata_commit(metadata, &batch, PS_COMMIT_DEFAULT, &e), &e);
  ps_namespace_metadata_close(metadata);
  for (size_t i = 0; i < 2; ++i)
    ps_checkpoint_record_close(&records.records[i]);
  ps_namespace_release(ns);
  ps_coordination_guard *guard = NULL;
  ok(ps_coordination_lock(ps_space_coordination(ps_root_space(root)),
                            PS_COORDINATION_TRANSACTION, 8, 0, &guard, &e), &e);
  ps_stats before, after;
  ok(ps_status(ps_root_space(root), &before, &e), &e);
  REQUIRE(before.reserve_owners && before.reserve_entries);
  REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  ok(ps_status(ps_root_space(root), &after, &e), &e);
  REQUIRE(after.reserve_owners == before.reserve_owners &&
          after.reserve_entries == before.reserve_entries &&
          after.reserve_payload_bytes == before.reserve_payload_bytes &&
          after.reserve_metadata_bytes == before.reserve_metadata_bytes);
  ok(ps_root_namespace_acquire(root, "lease-owner", 0, &ns, &e), &e);
  ps_namespace_status status;
  ok(ps_namespace_status_get(ns, &status, &e), &e);
  REQUIRE(status.checkpoint_keys == 1);
  ps_namespace_release(ns);
  ps_coordination_unlock(guard);
  for (int kind = 0; kind < 2; ++kind) {
    ps_budget *budget = NULL;
    ps_writer *writer = NULL;
    if (!kind) {
      ps_budget_plan plan = {PS_PAGE, 1024, 1, 0};
      ok(ps_budget_open(ps_root_space(root), &plan, &budget, &e), &e);
    } else {
      ps_key private_key = {UINT64_MAX, 1, PS_STATE};
      ok(ps_begin(ps_root_space(root), NULL, private_key, &writer, &e), &e);
    }
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
    lc_error_cleanup(&e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.reserve_owners == before.reserve_owners &&
            after.reserve_entries == before.reserve_entries &&
            after.reserve_payload_bytes == before.reserve_payload_bytes &&
            after.reserve_metadata_bytes == before.reserve_metadata_bytes);
    ok(ps_budget_close(budget, &e), &e);
    ps_writer_close(writer);
  }
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  ok(ps_root_namespace_acquire(root, "lease-owner", 0, &ns, &e), &e);
  ok(ps_namespace_status_get(ns, &status, &e), &e);
  REQUIRE(status.checkpoint_keys == 1);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_error_cleanup(&e);
}
typedef struct waiter {
  ps_coordination *owner;
  uint64_t identity;
  int held, release;
} waiter;
static void *wait_guard(void *context) {
  waiter *w = context;
  lc_error e;
  ps_coordination_guard *guard = NULL;
  lc_error_init(&e);
  ok(ps_coordination_lock(w->owner, PS_COORDINATION_NAMESPACE, w->identity,
                           1, &guard, &e), &e);
  __atomic_store_n(&w->held, 1, __ATOMIC_RELEASE);
  while (!__atomic_load_n(&w->release, __ATOMIC_ACQUIRE))
    sched_yield();
  ps_coordination_unlock(guard);
  lc_error_cleanup(&e);
  return NULL;
}
static void counter_step(ps_coordination *owner, int fd,
                          enum ps_coordination_domain domain, uint64_t identity,
                          lc_error *e) {
  ps_coordination_guard *guard = NULL;
  uint64_t value;
  ok(ps_coordination_lock(owner, domain, identity, 1, &guard, e), e);
  REQUIRE(pread(fd, &value, sizeof(value), 0) == (ssize_t)sizeof(value));
  sched_yield();
  ++value;
  REQUIRE(pwrite(fd, &value, sizeof(value), 0) == (ssize_t)sizeof(value));
  ps_coordination_unlock(guard);
}
typedef struct counter_worker {
  ps_coordination *owner;
  int fd;
  size_t rounds;
} counter_worker;
static void *counter_thread(void *context) {
  counter_worker *w = context;
  lc_error e;
  lc_error_init(&e);
  for (size_t i = 0; i < w->rounds; ++i)
    counter_step(w->owner, w->fd, PS_COORDINATION_NAMESPACE, 19, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void threads(void) {
  file_create();
  lc_error e;
  lc_error_init(&e);
  ps_coordination *a = owner_open(1, NULL, &e);
  ps_coordination *b = owner_open(1, NULL, &e);
  ps_coordination_guard *held = NULL, *peer = NULL;
  ok(ps_coordination_lock(b, PS_COORDINATION_NAMESPACE, 19, 0, &held, &e), &e);
  waiter w = {a, 19, 0, 0};
  pthread_t thread;
  REQUIRE(!pthread_create(&thread, NULL, wait_guard, &w));
  while (!ps_coordination_users(a))
    sched_yield();
  REQUIRE(!__atomic_load_n(&w.held, __ATOMIC_ACQUIRE));
  REQUIRE(ps_coordination_close(a, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  ok(ps_coordination_lock(a, PS_COORDINATION_NAMESPACE, 20, 0, &peer, &e), &e);
  ps_coordination_unlock(peer);
  ps_coordination_unlock(held);
  while (!__atomic_load_n(&w.held, __ATOMIC_ACQUIRE))
    sched_yield();
  REQUIRE(ps_coordination_users(a) == 1);
  __atomic_store_n(&w.release, 1, __ATOMIC_RELEASE);
  REQUIRE(!pthread_join(thread, NULL));
  REQUIRE(!ps_coordination_users(a));
  int fd = file_open();
  pthread_t workers[4];
  counter_worker contexts[4];
  for (size_t i = 0; i < 4; ++i) {
    contexts[i] = (counter_worker){i % 2 ? a : b, fd, 200};
    REQUIRE(!pthread_create(&workers[i], NULL, counter_thread, &contexts[i]));
  }
  for (size_t i = 0; i < 4; ++i)
    REQUIRE(!pthread_join(workers[i], NULL));
  uint64_t value;
  REQUIRE(pread(fd, &value, sizeof(value), 0) == (ssize_t)sizeof(value));
  REQUIRE(value == 800 && close(fd) == 0);
  ok(ps_coordination_close(a, &e), &e);
  ok(ps_coordination_close(b, &e), &e);
  lc_error_cleanup(&e);
}
typedef struct handoff_probe {
  ps_coordination_guard *release;
  size_t conflicts;
} handoff_probe;
static int observe_handoff(void *context, int fd, int command,
                           struct flock *lock) {
  handoff_probe *probe = context;
  int rc = fcntl(fd, command, lock);
  int reason = errno;
  if (rc < 0 && reason == EAGAIN && command == F_OFD_SETLK &&
      lock->l_type == F_WRLCK) {
    ++probe->conflicts;
    ps_coordination_unlock(probe->release);
    probe->release = NULL;
  }
  errno = reason;
  return rc;
}
static void namespace_handoff(void) {
  const enum ps_coordination_domain domains[] = {
      PS_COORDINATION_NAMESPACE, PS_COORDINATION_NAMESPACE_STATE};
  lc_error e = {0};
  handoff_probe probe = {0};
  ps_coordination_io io = {&probe, observe_handoff};
  file_create();
  ps_coordination *owner = owner_open(1, NULL, &e);
  ps_coordination *peer = owner_open(1, &io, &e);
  for (size_t i = 0; i < sizeof(domains) / sizeof(*domains); ++i) {
    ps_coordination_guard *held = NULL, *acquired = NULL;
    ok(ps_coordination_lock(owner, domains[i], 0, 0, &held, &e), &e);
    probe.release = held;
    probe.conflicts = 0;
    ok(ps_coordination_namespace_handoff(peer, domains[i], 1, 0, &acquired, &e),
       &e);
    REQUIRE(!probe.conflicts && probe.release == held);
    ps_coordination_unlock(acquired);
    ok(ps_coordination_namespace_handoff(peer, domains[i], 0, 1, &acquired, &e),
       &e);
    REQUIRE(probe.conflicts == 1 && !probe.release);
    ps_coordination_unlock(acquired);

    ok(ps_coordination_lock(owner, domains[i], 0, 0, &held, &e), &e);
    REQUIRE(ps_coordination_namespace_handoff(peer, domains[i], 0, 0, &acquired,
                                              &e) == LC_ERR_PENDING &&
            !acquired);
    lc_error_cleanup(&e);
    REQUIRE(ps_coordination_users(owner) == 1 &&
            ps_coordination_users(peer) == 0);
    ps_coordination_unlock(held);
  }
  ok(ps_coordination_close(peer, &e), &e);
  ok(ps_coordination_close(owner, &e), &e);
}
typedef struct priority_handoff {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  ps_coordination *background, *foreground;
  int intercepted, registered, background_rc, foreground_rc;
} priority_handoff;
static int namespace_waiter_marker(const struct flock *lock) {
  return lock->l_type == F_RDLCK &&
         (uint64_t)lock->l_start >= PS_CREDIT_RECORD_BASE - PS_PAGE &&
         (uint64_t)lock->l_start < PS_CREDIT_RECORD_BASE;
}
static int pause_background_handoff(void *context, int fd, int command,
                                    struct flock *lock) {
  priority_handoff *gate = context;
  int rc = fcntl(fd, command, lock);
  if (rc != 0 || command != F_OFD_SETLK)
    return rc;
  pthread_mutex_lock(&gate->mutex);
  if (lock->l_type == F_WRLCK && !gate->intercepted) {
    /* Background holds the state byte but has yet to check for waiters. */
    gate->intercepted = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->registered)
      pthread_cond_wait(&gate->changed, &gate->mutex);
  } else if (gate->intercepted && namespace_waiter_marker(lock)) {
    gate->registered = 1;
    pthread_cond_broadcast(&gate->changed);
  }
  pthread_mutex_unlock(&gate->mutex);
  return rc;
}
static void *priority_background(void *context) {
  priority_handoff *gate = context;
  ps_coordination_guard *guard = NULL;
  gate->background_rc = ps_coordination_namespace_maintenance(
      gate->background, 19, 1, &guard, NULL);
  REQUIRE(!guard);
  return NULL;
}
static void *priority_foreground(void *context) {
  priority_handoff *gate = context;
  ps_coordination_guard *guard = NULL;
  gate->foreground_rc = ps_coordination_namespace_handoff(
      gate->foreground, PS_COORDINATION_NAMESPACE_STATE, 19, 1, &guard, NULL);
  ps_coordination_unlock(guard);
  return NULL;
}
static void namespace_handoff_priority(int same_owner) {
  lc_error e = {0};
  priority_handoff gate = {0};
  ps_coordination_io io = {&gate, pause_background_handoff};
  ps_coordination_guard *other = NULL;
  pthread_t background, foreground;
  file_create();
  REQUIRE(!pthread_mutex_init(&gate.mutex, NULL) &&
          !pthread_cond_init(&gate.changed, NULL));
  gate.background = owner_open(1, &io, &e);
  gate.foreground = same_owner ? gate.background : owner_open(1, &io, &e);
  REQUIRE(!pthread_create(&background, NULL, priority_background, &gate));
  pthread_mutex_lock(&gate.mutex);
  while (!gate.intercepted)
    pthread_cond_wait(&gate.changed, &gate.mutex);
  pthread_mutex_unlock(&gate.mutex);
  ok(ps_coordination_namespace_maintenance(gate.background, 20, 0, &other, &e),
     &e);
  ps_coordination_unlock(other);
  REQUIRE(!pthread_create(&foreground, NULL, priority_foreground, &gate));
  REQUIRE(!pthread_join(background, NULL) && !pthread_join(foreground, NULL));
  REQUIRE(gate.background_rc == LC_ERR_PENDING && gate.foreground_rc == LC_OK &&
          gate.registered);
  REQUIRE(!ps_coordination_users(gate.background) &&
          !ps_coordination_users(gate.foreground));
  ok(ps_coordination_namespace_maintenance(gate.background, 19, 0, &other, &e),
     &e);
  ps_coordination_unlock(other);
  if (!same_owner)
    ok(ps_coordination_close(gate.foreground, &e), &e);
  ok(ps_coordination_close(gate.background, &e), &e);
  REQUIRE(!pthread_cond_destroy(&gate.changed) &&
          !pthread_mutex_destroy(&gate.mutex));
  lc_error_cleanup(&e);
}
static void faults(void) {
  file_create();
  lc_error e;
  lc_error_init(&e);
  size_t sites = 0;
  for (size_t cut = 0; cut <= sites; ++cut) {
    int fd = file_open();
    ps_coordination_options options = {0};
    ps_coordination *owner = NULL;
    ps_coordination_guard *guard = NULL;
    options.shared = 1;
    allocations = 0;
    allocation_fail = cut ? cut : (size_t)-1;
    measuring = 1;
    int rc = ps_coordination_open(fd, &options, &owner, &e);
    if (rc == LC_OK)
      rc = ps_coordination_lock(owner, PS_COORDINATION_NAMESPACE, 7, 0,
                                 &guard, &e);
    measuring = 0;
    if (!cut) {
      sites = allocations;
      ok(rc, &e);
    } else
      REQUIRE(rc == LC_ERR_NOMEM && !guard);
    lc_error_cleanup(&e);
    ps_coordination_unlock(guard);
    if (owner) {
      ok(ps_coordination_check(owner, &e), &e);
      ok(ps_coordination_lock(owner, PS_COORDINATION_TRANSACTION, 0, 0,
                               &guard, &e), &e);
      ps_coordination_unlock(guard);
      ok(ps_coordination_close(owner, &e), &e);
    } else {
      REQUIRE(!flock(fd, LOCK_EX | LOCK_NB));
      REQUIRE(!close(fd));
    }
  }
  REQUIRE(sites >= 2);
  size_t mutex_sites = 0;
  for (size_t cut = 0; cut <= mutex_sites; ++cut)
    for (int no_error = 0; no_error < (cut ? 2 : 1); ++no_error) {
      int fd = file_open();
      ps_coordination_options options = {0};
      ps_coordination *owner = NULL;
      ps_coordination_guard *guard = NULL;
      options.shared = 1;
      mutex_calls = 0;
      mutex_fail = cut;
      mutex_armed = 1;
      int rc = ps_coordination_open(fd, &options, &owner, no_error ? NULL : &e);
      if (rc == LC_OK)
        rc = ps_coordination_lock(owner, PS_COORDINATION_NAMESPACE, 9, 0,
                                   &guard, no_error ? NULL : &e);
      mutex_armed = 0;
      if (!cut) {
        mutex_sites = mutex_calls;
        ok(rc, &e);
      } else
        REQUIRE(mutex_calls >= cut && rc == LC_ERR_TRANSPORT && !guard);
      lc_error_cleanup(&e);
      ps_coordination_unlock(guard);
      if (owner) {
        ok(ps_coordination_check(owner, &e), &e);
        ok(ps_coordination_close(owner, &e), &e);
      } else {
        REQUIRE(!flock(fd, LOCK_EX | LOCK_NB));
        REQUIRE(!close(fd));
      }
    }
  REQUIRE(mutex_sites > 4);
  for (int reason_index = 0; reason_index < 4; ++reason_index) {
    static const int reasons[] = {EINTR, EAGAIN, EACCES, EIO};
    syscall_fault fault = {0, 1, reasons[reason_index]};
    ps_coordination_io io = {&fault, guarded_fcntl};
    ps_coordination *owner = owner_open(1, &io, &e);
    ps_coordination_guard *guard = NULL;
    int rc = ps_coordination_lock(owner, PS_COORDINATION_NAMESPACE, 8, 0,
                                   &guard, &e);
    if (!reason_index) {
      ok(rc, &e);
      REQUIRE(fault.calls == 2);
    } else {
      REQUIRE(rc == (reason_index == 3 ? LC_ERR_TRANSPORT : LC_ERR_PENDING));
      REQUIRE(!guard && !ps_coordination_users(owner));
      lc_error_cleanup(&e);
      ok(ps_coordination_check(owner, &e), &e);
      ok(ps_coordination_lock(owner, PS_COORDINATION_NAMESPACE, 8, 0, &guard,
                               &e), &e);
    }
    fault.fail = fault.calls + 1;
    fault.reason = EINTR;
    ps_coordination_unlock(guard);
    ok(ps_coordination_check(owner, &e), &e);
    ok(ps_coordination_close(owner, &e), &e);
  }
  syscall_fault fault = {0, 0, EIO};
  ps_coordination_io io = {&fault, guarded_fcntl};
  int failed = 0;
  ps_coordination_options linked = {0};
  linked.shared = 1;
  linked.io = &io;
  linked.failure_flag = &failed;
  ps_coordination *a = NULL;
  ok(ps_coordination_open(file_open(), &linked, &a, &e), &e);
  ps_coordination *b = owner_open(1, NULL, &e);
  ps_coordination_guard *guard = NULL, *busy = NULL;
  ok(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 0, 0, &guard, &e), &e);
  fault.fail = fault.calls + 1;
  ps_coordination_unlock(guard);
  REQUIRE(__atomic_load_n(&failed, __ATOMIC_ACQUIRE) == 1);
  REQUIRE(ps_coordination_check(a, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 1, 0, &guard,
                                &e) == LC_ERR_TRANSPORT && !guard);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_TRANSACTION, 0, 0, &busy,
                                &e) == LC_ERR_PENDING && !busy);
  lc_error_cleanup(&e);
  ok(ps_coordination_close(a, &e), &e);
  ok(ps_coordination_lock(b, PS_COORDINATION_TRANSACTION, 0, 0, &busy, &e), &e);
  ps_coordination_unlock(busy);
  ok(ps_coordination_close(b, &e), &e);
  /* Exclusive guards must never call the shared-mode syscall seam. */
  fault = (syscall_fault){0, 1, EIO};
  a = owner_open(0, &io, &e);
  allocations = 0;
  allocation_fail = 1;
  measuring = 1;
  int rc =
      ps_coordination_lock(a, PS_COORDINATION_ALLOCATION, 0, 0, &guard, &e);
  ps_coordination_unlock(guard);
  measuring = 0;
  ok(rc, &e);
  REQUIRE(!allocations && !fault.calls);
  ok(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 0, 0, &guard, &e),
     &e);
  ps_coordination_unlock(guard);
  REQUIRE(!fault.calls);
  ok(ps_coordination_close(a, &e), &e);
  lc_error_cleanup(&e);
}
static void independent_stream_authority(void) {
  const uint64_t first = 5, second = 13; /* Both used to map to byte 18. */
  lc_error e = {0};
  ps_coordination *a = owner_open(1, NULL, &e);
  ps_coordination *b = owner_open(1, NULL, &e);
  ps_coordination_guard *one = NULL, *two = NULL, *busy = NULL;
  ok(ps_coordination_lock(a, PS_COORDINATION_STREAM, first, 0, &one, &e), &e);
  ok(ps_coordination_lock(b, PS_COORDINATION_STREAM, second, 0, &two, &e), &e);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_STREAM, first, 0, &busy,
                               &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(one);
  ok(ps_coordination_lock(b, PS_COORDINATION_STREAM, first, 0, &one, &e), &e);
  ps_coordination_unlock(one);
  ps_coordination_unlock(two);
  ok(ps_coordination_lock(a, PS_COORDINATION_STREAM, PS_MAX_INCARNATION, 0,
                          &one, &e),
     &e);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_STREAM, PS_MAX_INCARNATION, 0,
                               &busy, &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  /* Both ends of the transaction band are separate from stream authority. */
  ok(ps_coordination_lock(b, PS_COORDINATION_TRANSACTION, 0, 0, &two, &e), &e);
  REQUIRE(ps_coordination_transaction_covers(two, 0) &&
          !ps_coordination_transaction_covers(one, 0));
  ok(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION,
                          PS_TRANSACTION_GUARD_SLOTS - 1, 0, &busy, &e),
     &e);
  REQUIRE(
      ps_coordination_transaction_covers(busy, PS_TRANSACTION_GUARD_SLOTS - 1));
  ps_coordination_unlock(busy);
  ps_coordination_unlock(one);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 0, 0, &busy,
                               &e) == LC_ERR_PENDING &&
          !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(two);
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_STREAM,
                               PS_MAX_INCARNATION + 1, 0, &busy,
                               &e) == LC_ERR_INVALID &&
          !busy);
  lc_error_cleanup(&e);
  ok(ps_coordination_close(b, &e), &e);
  ok(ps_coordination_close(a, &e), &e);
  lc_error_cleanup(&e);
}
static void child_hold(int ready, int command,
                        enum ps_coordination_domain domain, uint64_t identity) {
  lc_error e;
  lc_error_init(&e);
  ps_coordination *owner = owner_open(1, NULL, &e);
  ps_coordination_guard *guard = NULL, *peer = NULL;
  REQUIRE(ps_coordination_lock(owner, domain, identity, 0, &guard, &e) ==
              LC_ERR_PENDING && !guard);
  lc_error_cleanup(&e);
  /* A different domain makes progress while the selected byte is held. */
  enum ps_coordination_domain independent = domain == PS_COORDINATION_PUBLICATION
                                              ? PS_COORDINATION_RECOVERY
                                              : PS_COORDINATION_PUBLICATION;
  ok(ps_coordination_lock(owner, independent, 0, 0, &peer, &e), &e);
  ps_coordination_unlock(peer);
  pipe_write(ready, 'P');
  ok(ps_coordination_lock(owner, domain, identity, 1, &guard, &e), &e);
  pipe_write(ready, 'H');
  REQUIRE(pipe_read(command) == 'C');
  ps_coordination_unlock(guard);
  ok(ps_coordination_close(owner, &e), &e);
  lc_error_cleanup(&e);
}
static void child_counter(int ready, int command) {
  lc_error e;
  lc_error_init(&e);
  ps_coordination *owner = owner_open(1, NULL, &e);
  int fd = file_open();
  pipe_write(ready, 'R');
  REQUIRE(pipe_read(command) == 'S');
  for (size_t i = 0; i < 200; ++i)
    counter_step(owner, fd, PS_COORDINATION_PUBLICATION, 0, &e);
  REQUIRE(!close(fd));
  ok(ps_coordination_close(owner, &e), &e);
  lc_error_cleanup(&e);
}
typedef struct queued_process {
  int ready, command;
} queued_process;
static int hold_waiter_marker(void *context, int fd, int command,
                              struct flock *lock) {
  queued_process *gate = context;
  int rc = fcntl(fd, command, lock);
  if (rc == 0 && command == F_OFD_SETLK && namespace_waiter_marker(lock)) {
    pipe_write(gate->ready, 'W');
    REQUIRE(pipe_read(gate->command) == 'C');
  }
  return rc;
}
static void child_handoff(int ready, int command, uint64_t identity) {
  lc_error e = {0};
  queued_process gate = {ready, command};
  ps_coordination_io io = {&gate, hold_waiter_marker};
  ps_coordination *owner = owner_open(1, &io, &e);
  ps_coordination_guard *guard = NULL;
  ok(ps_coordination_namespace_handoff(owner, PS_COORDINATION_NAMESPACE_STATE,
                                       identity, 1, &guard, &e),
     &e);
  ps_coordination_unlock(guard);
  ok(ps_coordination_close(owner, &e), &e);
  lc_error_cleanup(&e);
}
static pid_t spawn(const char *self, const char *mode, int ready[2], int command[2],
                     enum ps_coordination_domain domain, uint64_t identity) {
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char r[32], c[32], d[32], id[32];
    snprintf(r, sizeof(r), "%d", ready[1]);
    snprintf(c, sizeof(c), "%d", command[0]);
    snprintf(d, sizeof(d), "%d", (int)domain);
    snprintf(id, sizeof(id), "%llu", (unsigned long long)identity);
    (void)close(ready[0]);
    (void)close(command[1]);
    execl(self, self, "--child", path, mode, r, c, d, id, (char *)NULL);
    _exit(127);
  }
  return child;
}
static void dead_namespace_waiter(const char *self) {
  lc_error e = {0};
  ps_coordination *owner = owner_open(1, NULL, &e);
  ps_coordination_guard *guard = NULL;
  int ready[2], command[2], status;
  ok(ps_coordination_lock(owner, PS_COORDINATION_NAMESPACE_STATE, 19, 0, &guard,
                          &e),
     &e);
  REQUIRE(!pipe(ready) && !pipe(command));
  pid_t child = spawn(self, "handoff", ready, command,
                      PS_COORDINATION_NAMESPACE_STATE, 19);
  REQUIRE(!close(ready[1]) && !close(command[0]) && pipe_read(ready[0]) == 'W');
  ps_coordination_unlock(guard);
  REQUIRE(ps_coordination_namespace_maintenance(owner, 19, 1, &guard, &e) ==
              LC_ERR_PENDING &&
          !guard);
  lc_error_cleanup(&e);
  REQUIRE(!kill(child, SIGKILL) && waitpid(child, &status, 0) == child &&
          WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
  ok(ps_coordination_namespace_maintenance(owner, 19, 0, &guard, &e), &e);
  ps_coordination_unlock(guard);
  ok(ps_coordination_close(owner, &e), &e);
  REQUIRE(!close(ready[0]) && !close(command[1]));
  lc_error_cleanup(&e);
}
static void processes(const char *self) {
  file_create();
  lc_error e;
  const enum ps_coordination_domain domains[] = {
      PS_COORDINATION_PUBLICATION,   PS_COORDINATION_RECOVERY,
      PS_COORDINATION_ALLOCATION,    PS_COORDINATION_SYNC,
      PS_COORDINATION_TRANSACTION,   PS_COORDINATION_NAMESPACE,
      PS_COORDINATION_CONTROL_STATE, PS_COORDINATION_COLLECTION_CATALOG,
      PS_COORDINATION_STREAM_CLAIM};
  lc_error_init(&e);
  for (size_t i = 0; i < sizeof(domains) / sizeof(*domains); ++i)
    for (int abrupt = 0; abrupt < 2; ++abrupt) {
      enum ps_coordination_domain domain = domains[i];
      ps_coordination *owner = owner_open(1, NULL, &e);
      ps_coordination_guard *guard = NULL, *busy = NULL;
      uint64_t identity = domain == PS_COORDINATION_NAMESPACE ||
                                  domain == PS_COORDINATION_COLLECTION_CATALOG
                              ? UINT64_MAX
                          : domain == PS_COORDINATION_STREAM_CLAIM ? 1
                                                                   : 0;
      ok(ps_coordination_lock(owner, domain, identity, 0, &guard, &e), &e);
      int ready[2], command[2], status;
      REQUIRE(!pipe(ready) && !pipe(command));
      pid_t child = spawn(self, "hold", ready, command, domain, identity);
      REQUIRE(!close(ready[1]) && !close(command[0]));
      REQUIRE(pipe_read(ready[0]) == 'P');
      ps_coordination_unlock(guard);
      REQUIRE(pipe_read(ready[0]) == 'H');
      REQUIRE(ps_coordination_lock(owner, domain, identity, 0, &busy, &e) ==
                  LC_ERR_PENDING && !busy);
      lc_error_cleanup(&e);
      /* Closing another root owner must not release the child's authority. */
      ok(ps_coordination_close(owner, &e), &e);
      owner = owner_open(1, NULL, &e);
      REQUIRE(ps_coordination_lock(owner, domain, identity, 0, &busy, &e) ==
                  LC_ERR_PENDING && !busy);
      lc_error_cleanup(&e);
      if (abrupt)
        REQUIRE(!kill(child, SIGKILL));
      else
        pipe_write(command[1], 'C');
      REQUIRE(waitpid(child, &status, 0) == child);
      REQUIRE(abrupt ? WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL
                     : WIFEXITED(status) && WEXITSTATUS(status) == 0);
      ok(ps_coordination_lock(owner, domain, identity, 0, &busy, &e), &e);
      ps_coordination_unlock(busy);
      ok(ps_coordination_close(owner, &e), &e);
      REQUIRE(!close(ready[0]) && !close(command[1]));
    }
  pid_t children[4];
  int ready[2], command[2], status;
  REQUIRE(!pipe(ready) && !pipe(command));
  for (size_t i = 0; i < 4; ++i)
    children[i] = spawn(self, "counter", ready, command,
                         PS_COORDINATION_PUBLICATION, 0);
  REQUIRE(!close(ready[1]) && !close(command[0]));
  for (size_t i = 0; i < 4; ++i)
    REQUIRE(pipe_read(ready[0]) == 'R');
  for (size_t i = 0; i < 4; ++i)
    pipe_write(command[1], 'S');
  for (size_t i = 0; i < 4; ++i) {
    REQUIRE(waitpid(children[i], &status, 0) == children[i]);
    REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
  uint64_t value;
  int fd = file_open();
  REQUIRE(pread(fd, &value, sizeof(value), 0) == (ssize_t)sizeof(value));
  REQUIRE(value == 800 && !close(fd));
  REQUIRE(!close(ready[0]) && !close(command[1]));
  dead_namespace_waiter(self);
  lc_error_cleanup(&e);
}
int main(int argc, char **argv) {
  if (argc == 8 && !strcmp(argv[1], "--child")) {
    REQUIRE(snprintf(path, sizeof(path), "%s", argv[2]) < (int)sizeof(path));
    if (!strcmp(argv[3], "hold"))
      child_hold(atoi(argv[4]), atoi(argv[5]), atoi(argv[6]),
                   strtoull(argv[7], NULL, 10));
    else if (!strcmp(argv[3], "handoff"))
      child_handoff(atoi(argv[4]), atoi(argv[5]), strtoull(argv[7], NULL, 10));
    else
      child_counter(atoi(argv[4]), atoi(argv[5]));
    return 0;
  }
  if (argc != 3)
    return 2;
  char directory[1300];
  REQUIRE(snprintf(directory, sizeof(directory), "%s/coordination-%ld-XXXXXX",
                   argv[1], (long)getpid()) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) < (int)sizeof(path));
  if (!strcmp(argv[2], "lifecycle"))
    lifecycle();
  else if (!strcmp(argv[2], "root-lifetime"))
    root_lifetime();
  else if (!strcmp(argv[2], "large-offsets"))
    large_offsets();
  else if (!strcmp(argv[2], "threads")) {
    threads();
    namespace_handoff();
    namespace_handoff_priority(0);
    namespace_handoff_priority(1);
    independent_stream_authority();
  } else if (!strcmp(argv[2], "faults"))
    faults();
  else {
    REQUIRE(!strcmp(argv[2], "processes"));
    processes(argv[0]);
  }
  struct stat status;
  REQUIRE(!stat(path, &status) && status.st_size == PS_DEFAULT_CAPACITY);
  DIR *contents = opendir(directory);
  struct dirent *entry;
  size_t files = 0;
  REQUIRE(contents);
  while ((entry = readdir(contents)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    REQUIRE(!strcmp(entry->d_name, "store"));
    ++files;
  }
  REQUIRE(files == 1 && !closedir(contents));
  REQUIRE(!unlink(path) && !rmdir(directory));
  printf("coordination %s: %zu checks passed\n", argv[2], checks);
  return 0;
}
