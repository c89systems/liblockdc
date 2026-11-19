#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "coordination.h"
#include "lc_api_internal.h"
#include "namespace_metadata.h"
#include <errno.h>
#include <dirent.h>
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
  int rc = ps_coordination_lock(owner, PS_COORDINATION_TRANSACTION, 3, 0,
                                &guard, &e);
  measuring = 0;
  ok(rc, &e);
  REQUIRE(!allocations && ps_coordination_users(owner) == 1);
  REQUIRE(ps_close(space, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
  REQUIRE(ps_open(path, 0, NULL, &other, &e) == LC_ERR_INVALID && !other);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(owner, PS_COORDINATION_TRANSACTION, 3, 0,
                                &busy, &e) == LC_ERR_PENDING && !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  ok(ps_close(space, &e), &e);
  space = NULL;

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
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_NAMESPACE,
                                UINT64_C(7073558525344295424), 0, &busy, &e) ==
              LC_ERR_PENDING && !busy);
  lc_error_cleanup(&e);
  REQUIRE(ps_coordination_lock(b, PS_COORDINATION_NAMESPACE, 0, 0, &busy, &e) ==
              LC_ERR_PENDING && !busy);
  lc_error_cleanup(&e);
  ps_coordination_unlock(guard);
  guard = (ps_coordination_guard *)1;
  REQUIRE(ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 256, 0, &guard,
                                &e) == LC_ERR_INVALID && !guard);
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
  REQUIRE(mutex_sites > PS_COORDINATION_TXN_STRIPES);
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
  int rc = ps_coordination_lock(a, PS_COORDINATION_TRANSACTION, 0, 0, &guard,
                                &e);
  ps_coordination_unlock(guard);
  measuring = 0;
  ok(rc, &e);
  REQUIRE(!allocations && !fault.calls);
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
static void processes(const char *self) {
  file_create();
  lc_error e;
  lc_error_init(&e);
  for (int domain = PS_COORDINATION_PUBLICATION;
       domain <= PS_COORDINATION_NAMESPACE; ++domain)
    for (int abrupt = 0; abrupt < 2; ++abrupt) {
      ps_coordination *owner = owner_open(1, NULL, &e);
      ps_coordination_guard *guard = NULL, *busy = NULL;
      uint64_t identity = domain == PS_COORDINATION_NAMESPACE ? UINT64_MAX : 0;
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
  lc_error_cleanup(&e);
}
int main(int argc, char **argv) {
  if (argc == 8 && !strcmp(argv[1], "--child")) {
    REQUIRE(snprintf(path, sizeof(path), "%s", argv[2]) < (int)sizeof(path));
    if (!strcmp(argv[3], "hold"))
      child_hold(atoi(argv[4]), atoi(argv[5]), atoi(argv[6]),
                   strtoull(argv[7], NULL, 10));
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
  else if (!strcmp(argv[2], "threads"))
    threads();
  else if (!strcmp(argv[2], "faults"))
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
