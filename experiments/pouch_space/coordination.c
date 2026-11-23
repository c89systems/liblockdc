#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "coordination.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/file.h>
#include <time.h>
#include <unistd.h>

#define FIXED (PS_COORDINATION_SYNC + 1U)
#define BUCKETS 67U
#define INITIALIZE_OFFSET 6U
/* Darwin shares the advisory lock range between flock and OFD locks.
 * Dedicate a byte outside the existing coordination ranges to admission. */
#define LIFETIME_OFFSET 9U
#define NS_BASE ((lc_u64)512)
#define COLLECTION_SLOTS 512U
#define COLLECTION_BASE (2U * PS_PAGE - 6U * COLLECTION_SLOTS)
#define NS_SLOTS (COLLECTION_BASE - NS_BASE)
#define VIEW_SLOTS (PS_PAGE / 2U)
#define VIEW_BASE (PS_METADATA_START - VIEW_SLOTS)
#define STATE_BASE (PS_READINESS_OFFSET + 256U)
#define STREAM_CLAIM_BASE (VIEW_BASE - PS_COORDINATION_STREAM_CLAIM_SLOTS)
#define STATE_SLOTS (STREAM_CLAIM_BASE - STATE_BASE)
#define COLLECTION_CATALOG_SLOTS (STATE_BASE - PS_READINESS_OFFSET - 2U)
#define NAMESPACE_WAITER_BASE (PS_MAX_CAPACITY + PS_MAX_INCARNATION)
#if STATE_SLOTS > PS_PAGE
#error "namespace waiters must fit below the credit record range"
#endif
struct ps_coordination_guard {
  ps_coordination *owner;
  pthread_mutex_t mutex;
  uint64_t offset;
  size_t users;
  int fixed, held;
  struct ps_coordination_guard *next;
};
typedef struct ps_keyed_guard {
  ps_coordination_guard guard;
  pthread_cond_t available;
  size_t views; /* OFD locks merge, so only the last local view releases it. */
} ps_keyed_guard;
struct ps_coordination {
  int fd, shared, poisoned;
  int *failure_flag;
  size_t users;
  lc_allocator allocator;
  ps_coordination_io io;
  pthread_mutex_t registry;
  ps_coordination_guard fixed[FIXED], *buckets[BUCKETS];
};
static int fail(lc_error *e, int code, int reason, const char *message) {
  return lc_error_set(e, code, 0, message, reason ? strerror(reason) : NULL,
                      NULL, "pouch");
}
static int native_lock(void *context, int fd, int command, struct flock *lock) {
  (void)context;
  return fcntl(fd, command, lock);
}
static int lock_lifetime(int fd, int shared, int wait) {
  int rc;
#ifdef __APPLE__
  struct flock lock = {0};
  lock.l_type = shared ? F_RDLCK : F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = LIFETIME_OFFSET;
  lock.l_len = 1;
  do {
    rc = native_lock(NULL, fd, wait ? F_OFD_SETLKW : F_OFD_SETLK, &lock);
  } while (rc < 0 && errno == EINTR);
#else
  int mode = shared ? LOCK_SH : LOCK_EX;
  if (!wait)
    mode |= LOCK_NB;
  do {
    rc = flock(fd, mode);
  } while (rc < 0 && errno == EINTR);
#endif
  return rc;
}
int ps_coordination_check(ps_coordination *owner, lc_error *e) {
  if (!owner)
    return fail(e, LC_ERR_INVALID, 0, "coordination requires an owner");
  return (__atomic_load_n(&owner->poisoned, __ATOMIC_ACQUIRE) ||
          (owner->failure_flag &&
           __atomic_load_n(owner->failure_flag, __ATOMIC_ACQUIRE)))
             ? fail(e, LC_ERR_TRANSPORT, 0,
                    "container coordination uncertain; close and reopen")
             : LC_OK;
}
int ps_coordination_shared(const ps_coordination *owner) {
  return owner && owner->shared;
}
int ps_coordination_initialize(ps_coordination *owner, int create, int acquire,
                               lc_error *e) {
  struct flock lock = {0};
  int rc, reason;
  if (!owner->shared)
    return LC_OK;
  lock.l_type = !acquire ? F_UNLCK : create ? F_WRLCK : F_RDLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = INITIALIZE_OFFSET;
  lock.l_len = 1;
  do {
    rc = owner->io.lock(owner->io.context, owner->fd,
                        acquire && create ? F_OFD_SETLKW : F_OFD_SETLK, &lock);
  } while (rc < 0 && errno == EINTR);
  if (rc == 0)
    return LC_OK;
  reason = errno;
  if (acquire && !create && (reason == EAGAIN || reason == EACCES))
    return fail(e, LC_ERR_PENDING, reason,
                "container initialization in progress");
  return fail(e, LC_ERR_TRANSPORT, reason,
              acquire ? "cannot acquire container initialization authority"
                      : "cannot release container initialization authority");
}
size_t ps_coordination_users(ps_coordination *owner) {
  return owner ? __atomic_load_n(&owner->users, __ATOMIC_ACQUIRE) : 0;
}
int ps_coordination_conflict(ps_coordination *owner, uint64_t offset,
                             uint64_t bytes, uint64_t *first, uint64_t *last,
                             lc_error *e) {
  struct flock lock;
  uint64_t end;
  int rc;
  if (!first || !last || !bytes || offset > LC_I64_MAX ||
      bytes > (uint64_t)LC_I64_MAX - offset)
    return fail(e, LC_ERR_INVALID, 0, "invalid physical pin query");
  *first = *last = 0;
  rc = ps_coordination_check(owner, e);
  if (rc != LC_OK)
    return rc;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = (off_t)offset;
  lock.l_len = (off_t)bytes;
  do {
    rc = owner->io.lock(owner->io.context, owner->fd, F_OFD_GETLK, &lock);
  } while (rc < 0 && errno == EINTR);
  if (rc == 0 && lock.l_type == F_UNLCK)
    return LC_OK;
  if (rc == 0 && lock.l_whence == SEEK_SET && lock.l_start >= 0 &&
      lock.l_len >= 0 && (lock.l_type == F_RDLCK || lock.l_type == F_WRLCK)) {
    end = !lock.l_len || (uint64_t)lock.l_len >
                             (uint64_t)LC_I64_MAX - (uint64_t)lock.l_start
              ? (uint64_t)LC_I64_MAX
              : (uint64_t)lock.l_start + (uint64_t)lock.l_len;
    *first = (uint64_t)lock.l_start < offset ? offset : (uint64_t)lock.l_start;
    *last = end > offset + bytes ? offset + bytes : end;
    if (*first < *last)
      return LC_OK;
  }
  if (owner->failure_flag)
    __atomic_store_n(owner->failure_flag, 1, __ATOMIC_RELEASE);
  __atomic_store_n(&owner->poisoned, 1, __ATOMIC_RELEASE);
  return fail(e, LC_ERR_TRANSPORT, rc < 0 ? errno : 0,
              "cannot establish physical pin ownership; close and reopen");
}
int ps_coordination_range(ps_coordination *owner, uint64_t offset,
                          uint64_t bytes, int acquire, lc_error *e) {
  struct flock lock;
  int rc;
  if (!owner || !bytes || offset > LC_I64_MAX ||
      bytes > (uint64_t)LC_I64_MAX - offset || (acquire != 0 && acquire != 1))
    return fail(e, LC_ERR_INVALID, 0, "invalid physical ownership range");
  if (acquire && (rc = ps_coordination_check(owner, e)) != LC_OK)
    return rc;
  /* Exclusive lifetime authority already covers the whole container. Only
   * shared participants need separately observable range witnesses. */
  if (!owner->shared)
    return LC_OK;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = acquire ? F_RDLCK : F_UNLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = (off_t)offset;
  lock.l_len = (off_t)bytes;
  do {
    rc = owner->io.lock(owner->io.context, owner->fd, F_OFD_SETLK, &lock);
  } while (rc < 0 && errno == EINTR);
  if (rc == 0)
    return LC_OK;
  if (!acquire) {
    if (owner->failure_flag)
      __atomic_store_n(owner->failure_flag, 1, __ATOMIC_RELEASE);
    __atomic_store_n(&owner->poisoned, 1, __ATOMIC_RELEASE);
  }
  return fail(e,
              acquire && (errno == EACCES || errno == EAGAIN)
                  ? LC_ERR_PENDING
                  : LC_ERR_TRANSPORT,
              errno,
              acquire ? "cannot claim physical ownership"
                      : "physical release uncertain; close and reopen");
}
int ps_coordination_open(int fd, const ps_coordination_options *options,
                         ps_coordination **out, lc_error *e) {
  ps_coordination_options defaults = {0};
  ps_coordination *owner;
  size_t i;
  int rc;
  if (out)
    *out = NULL;
  if (!options)
    options = &defaults;
  if (fd < 0 || !out || (options->shared != 0 && options->shared != 1) ||
      (options->io && !options->io->lock))
    return fail(e, LC_ERR_INVALID, 0, "invalid container coordination open");
  owner = lc_calloc_with_allocator(options->allocator, 1, sizeof(*owner));
  if (!owner)
    return fail(e, LC_ERR_NOMEM, 0, "cannot allocate container coordination");
  if (options->allocator)
    owner->allocator = *options->allocator;
  owner->fd = fd;
  owner->shared = options->shared;
  owner->failure_flag = options->failure_flag;
  owner->io.lock = native_lock;
  if (options->io)
    owner->io = *options->io;
  rc = pthread_mutex_init(&owner->registry, NULL);
  if (rc) {
    lc_free_with_allocator(&owner->allocator, owner);
    return fail(e, LC_ERR_TRANSPORT, rc,
                "cannot initialize coordination registry");
  }
  for (i = 0; i < FIXED; ++i) {
    owner->fixed[i].owner = owner;
    owner->fixed[i].fixed = 1;
    owner->fixed[i].offset = i < 2 ? i : i + 2;
    rc = pthread_mutex_init(&owner->fixed[i].mutex, NULL);
    if (rc)
      break;
  }
  if (!rc) {
    rc = lock_lifetime(fd, owner->shared, options->wait_lifetime);
    if (rc < 0)
      rc = fail(e,
                errno == EAGAIN || errno == EWOULDBLOCK || errno == EACCES
                    ? LC_ERR_PENDING
                    : LC_ERR_TRANSPORT,
                errno, "container already open; another owner is active");
  } else
    rc = fail(e, LC_ERR_TRANSPORT, rc,
              "cannot initialize container coordination guard");
  if (rc != LC_OK) {
    while (i)
      pthread_mutex_destroy(&owner->fixed[--i].mutex);
    pthread_mutex_destroy(&owner->registry);
    lc_free_with_allocator(&owner->allocator, owner);
    return rc;
  }
  *out = owner;
  return LC_OK;
}
int ps_coordination_close(ps_coordination *owner, lc_error *e) {
  size_t i;
  int rc;
  if (!owner)
    return LC_OK;
  if (ps_coordination_users(owner))
    return fail(e, LC_ERR_INVALID, 0,
                "release coordination guards and waiters before close");
  /* No new calls may enter after the zero-user observation. */
  rc = close(owner->fd);
  if (rc)
    rc = fail(e, LC_ERR_TRANSPORT, errno, "cannot close coordination owner");
  for (i = 0; i < FIXED; ++i)
    pthread_mutex_destroy(&owner->fixed[i].mutex);
  pthread_mutex_destroy(&owner->registry);
  lc_free_with_allocator(&owner->allocator, owner);
  return rc;
}
static void drop(ps_coordination_guard *guard) {
  ps_coordination *owner = guard->owner;
  ps_coordination_guard **link;
  if (!guard->fixed) {
    pthread_mutex_lock(&owner->registry);
    if (!--guard->users) {
      link = &owner->buckets[guard->offset % BUCKETS];
      while (*link != guard)
        link = &(*link)->next;
      *link = guard->next;
      pthread_mutex_unlock(&owner->registry);
      pthread_cond_destroy(&((ps_keyed_guard *)guard)->available);
      pthread_mutex_destroy(&guard->mutex);
      lc_free_with_allocator(&owner->allocator, guard);
    } else
      pthread_mutex_unlock(&owner->registry);
  }
  __atomic_sub_fetch(&owner->users, 1, __ATOMIC_RELEASE);
}
static uint64_t keyed_offset(enum ps_coordination_domain domain,
                             uint64_t identity) {
  if (domain == PS_COORDINATION_STREAM)
    return PS_MAX_CAPACITY + identity - 1;
  if (domain == PS_COORDINATION_TRANSACTION)
    return PS_TRANSACTION_GUARD_BASE + identity;
  if (domain == PS_COORDINATION_CONTROL_STATE)
    return STATE_BASE - 2 + identity;
  if (domain == PS_COORDINATION_STREAM_CLAIM) {
    /* Consecutive incarnations in one allocation grant cover the table. */
    --identity;
    return STREAM_CLAIM_BASE +
           (identity ^ (identity >> 32)) % PS_COORDINATION_STREAM_CLAIM_SLOTS;
  }
  identity ^= identity >> 33;
  identity *= LC_U64_WORDS(0xff51afd7UL, 0xed558ccdUL);
  identity ^= identity >> 33;
  if (domain == PS_COORDINATION_COLLECTION_CATALOG)
    return PS_READINESS_OFFSET + identity % COLLECTION_CATALOG_SLOTS;
  if (domain == PS_COORDINATION_NAMESPACE_VIEW)
    return VIEW_BASE + identity % VIEW_SLOTS;
  if (domain == PS_COORDINATION_NAMESPACE_STATE)
    return STATE_BASE + identity % STATE_SLOTS;
  if (domain == PS_COORDINATION_NAMESPACE_WAITER)
    return NAMESPACE_WAITER_BASE + identity % STATE_SLOTS;
  if (domain >= PS_COORDINATION_COLLECTION_APPEND)
    return COLLECTION_BASE +
           (domain - PS_COORDINATION_COLLECTION_APPEND) * COLLECTION_SLOTS +
           identity % COLLECTION_SLOTS;
  return NS_BASE + identity % NS_SLOTS;
}
int ps_coordination_transaction_covers(const ps_coordination_guard *guard,
                                       uint64_t slot) {
  return guard && slot < PS_TRANSACTION_GUARD_SLOTS &&
         guard->offset == keyed_offset(PS_COORDINATION_TRANSACTION, slot);
}
int ps_coordination_namespace_view_conflict(ps_coordination *owner,
                                            uint64_t identity, int *present,
                                            lc_error *e) {
  uint64_t first, last;
  int rc;
  *present = 0;
  if (!owner->shared)
    return LC_OK;
  rc = ps_coordination_conflict(
      owner, keyed_offset(PS_COORDINATION_NAMESPACE_VIEW, identity), 1, &first,
      &last, e);
  if (rc == LC_OK)
    *present = first != last;
  return rc;
}
static int dynamic_guard(ps_coordination *owner, uint64_t offset,
                         ps_coordination_guard **out, lc_error *e) {
  ps_coordination_guard *guard, *candidate;
  ps_keyed_guard *keyed;
  size_t bucket;
  int rc;
  /* The registry is keyed by the actual lock byte, including collisions. */
  bucket = (size_t)(offset % BUCKETS);
  pthread_mutex_lock(&owner->registry);
  for (guard = owner->buckets[bucket]; guard; guard = guard->next)
    if (guard->offset == offset) {
      ++guard->users;
      pthread_mutex_unlock(&owner->registry);
      *out = guard;
      return LC_OK;
    }
  pthread_mutex_unlock(&owner->registry);
  keyed = lc_calloc_with_allocator(&owner->allocator, 1, sizeof(*keyed));
  if (!keyed) {
    return fail(e, LC_ERR_NOMEM, 0, "cannot allocate keyed coordination guard");
  }
  candidate = &keyed->guard;
  candidate->owner = owner;
  candidate->offset = offset;
  candidate->users = 1;
  rc = pthread_mutex_init(&candidate->mutex, NULL);
  if (rc) {
    lc_free_with_allocator(&owner->allocator, candidate);
    return fail(e, LC_ERR_TRANSPORT, rc,
                "cannot initialize keyed coordination");
  }
  rc = pthread_cond_init(&keyed->available, NULL);
  if (rc) {
    pthread_mutex_destroy(&candidate->mutex);
    lc_free_with_allocator(&owner->allocator, candidate);
    return fail(e, LC_ERR_TRANSPORT, rc,
                "cannot initialize keyed coordination waiters");
  }
  pthread_mutex_lock(&owner->registry);
  for (guard = owner->buckets[bucket]; guard; guard = guard->next)
    if (guard->offset == offset)
      break;
  if (guard)
    ++guard->users;
  else {
    candidate->next = owner->buckets[bucket];
    owner->buckets[bucket] = candidate;
    guard = candidate;
    candidate = NULL;
  }
  pthread_mutex_unlock(&owner->registry);
  if (candidate) {
    pthread_cond_destroy(&keyed->available);
    pthread_mutex_destroy(&candidate->mutex);
    lc_free_with_allocator(&owner->allocator, candidate);
  }
  *out = guard;
  return LC_OK;
}
static int kernel_lock(ps_coordination_guard *guard, short kind, int wait) {
  ps_coordination *owner = guard->owner;
  struct flock lock;
  int rc;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = kind;
  lock.l_whence = SEEK_SET;
  lock.l_start = (off_t)guard->offset;
  lock.l_len = 1;
  do {
    rc = owner->io.lock(owner->io.context, owner->fd,
                        wait ? F_OFD_SETLKW : F_OFD_SETLK, &lock);
  } while (rc < 0 && errno == EINTR);
  return rc;
}
static int local_lock(ps_coordination_guard *guard, int wait) {
  int rc = wait ? pthread_mutex_lock(&guard->mutex)
                : pthread_mutex_trylock(&guard->mutex);
  if (!rc && !guard->fixed) {
    ps_keyed_guard *keyed = (ps_keyed_guard *)guard;
    while (!rc && (guard->held || keyed->views))
      rc = wait ? pthread_cond_wait(&keyed->available, &guard->mutex) : EBUSY;
    if (!rc)
      guard->held = 1;
    pthread_mutex_unlock(&guard->mutex);
  }
  return rc;
}
static void local_unlock(ps_coordination_guard *guard) {
  if (!guard->fixed) {
    pthread_mutex_lock(&guard->mutex);
    guard->held = 0;
    pthread_cond_signal(&((ps_keyed_guard *)guard)->available);
  }
  pthread_mutex_unlock(&guard->mutex);
}
static int pin_view(ps_coordination_guard *guard, int wait, lc_error *e) {
  ps_keyed_guard *keyed = (ps_keyed_guard *)guard;
  int rc = 0, reason;
  pthread_mutex_lock(&guard->mutex);
  while (!rc && guard->held)
    rc = wait ? pthread_cond_wait(&keyed->available, &guard->mutex) : EBUSY;
  rc = rc ? fail(e, rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT, rc,
                 "cannot acquire local container read authority")
          : ps_coordination_check(guard->owner, e);
  if (rc == LC_OK && !keyed->views && guard->owner->shared &&
      kernel_lock(guard, F_RDLCK, wait)) {
    reason = errno;
    rc = fail(e,
              reason == EAGAIN || reason == EACCES ? LC_ERR_PENDING
                                                   : LC_ERR_TRANSPORT,
              reason, "cannot acquire container read authority");
  }
  if (rc == LC_OK)
    ++keyed->views;
  pthread_cond_broadcast(&keyed->available);
  pthread_mutex_unlock(&guard->mutex);
  return rc;
}
static int unpin_view(ps_coordination_guard *guard) {
  ps_keyed_guard *keyed = (ps_keyed_guard *)guard;
  int pinned;
  pthread_mutex_lock(&guard->mutex);
  pinned = keyed->views != 0;
  if (pinned && !--keyed->views) {
    (void)ps_coordination_range(guard->owner, guard->offset, 1, 0, NULL);
    pthread_cond_signal(&keyed->available);
  }
  pthread_mutex_unlock(&guard->mutex);
  return pinned;
}
static int lock_guard(ps_coordination *owner,
                      enum ps_coordination_domain domain, uint64_t identity,
                      int wait, int view, ps_coordination_guard **out,
                      lc_error *e) {
  ps_coordination_guard *guard = NULL;
  int rc, reason;
  if (out)
    *out = NULL;
  if (!owner || !out || (wait != 0 && wait != 1) ||
      domain < PS_COORDINATION_PUBLICATION ||
      domain > PS_COORDINATION_NAMESPACE_WAITER ||
      (domain == PS_COORDINATION_NAMESPACE_WAITER && (!view || wait)) ||
      (domain >= PS_COORDINATION_STREAM &&
       domain <= PS_COORDINATION_NAMESPACE_STATE && wait) ||
      (domain <= PS_COORDINATION_SYNC && identity) ||
      (domain == PS_COORDINATION_TRANSACTION &&
       identity >= PS_TRANSACTION_GUARD_SLOTS) ||
      (domain == PS_COORDINATION_STREAM &&
       (!identity || identity > PS_MAX_INCARNATION)) ||
      (domain == PS_COORDINATION_STREAM_CLAIM && !identity) ||
      (domain == PS_COORDINATION_CONTROL_STATE && identity >= 2))
    return fail(e, LC_ERR_INVALID, 0, "invalid container coordination guard");
  rc = ps_coordination_check(owner, e);
  if (rc != LC_OK)
    return rc;
  __atomic_add_fetch(&owner->users, 1, __ATOMIC_ACQ_REL);
  if (domain > PS_COORDINATION_SYNC)
    rc = dynamic_guard(owner, keyed_offset(domain, identity), &guard, e);
  else
    guard = &owner->fixed[domain];
  if (rc != LC_OK) {
    __atomic_sub_fetch(&owner->users, 1, __ATOMIC_RELEASE);
    return rc;
  }
  if (view || domain == PS_COORDINATION_NAMESPACE_VIEW) {
    rc = pin_view(guard, wait, e);
    if (rc == LC_OK)
      *out = guard;
    else
      drop(guard);
    return rc;
  }
  rc = local_lock(guard, wait);
  if (rc) {
    drop(guard);
    return fail(e, rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT, rc,
                "cannot acquire local container authority");
  }
  rc = ps_coordination_check(owner, e);
  if (rc == LC_OK && owner->shared && kernel_lock(guard, F_WRLCK, wait)) {
    reason = errno;
    rc = fail(e,
              reason == EAGAIN || reason == EACCES ? LC_ERR_PENDING
                                                   : LC_ERR_TRANSPORT,
              reason, "cannot acquire container file authority");
  }
  if (rc != LC_OK) {
    local_unlock(guard);
    drop(guard);
    return rc;
  }
  *out = guard;
  return LC_OK;
}
int ps_coordination_lock(ps_coordination *owner,
                         enum ps_coordination_domain domain, uint64_t identity,
                         int wait, ps_coordination_guard **out, lc_error *e) {
  return lock_guard(owner, domain, identity, wait, 0, out, e);
}
static int namespace_waiters(ps_coordination *owner, uint64_t identity,
                             int *waiting, lc_error *e) {
  uint64_t offset = keyed_offset(PS_COORDINATION_NAMESPACE_WAITER, identity);
  uint64_t first, last;
  ps_coordination_guard *guard;
  int rc;
  *waiting = 0;
  /* GETLK ignores the querying OFD. Its local waiters are registered as
   * compatible views, so one departing thread cannot clear another. */
  pthread_mutex_lock(&owner->registry);
  for (guard = owner->buckets[offset % BUCKETS]; guard; guard = guard->next)
    if (guard->offset == offset) {
      pthread_mutex_lock(&guard->mutex);
      *waiting = ((ps_keyed_guard *)guard)->views != 0;
      pthread_mutex_unlock(&guard->mutex);
      break;
    }
  pthread_mutex_unlock(&owner->registry);
  if (*waiting || !owner->shared)
    return LC_OK;
  rc = ps_coordination_conflict(owner, offset, 1, &first, &last, e);
  *waiting = first != last;
  return rc;
}
static int namespace_handoff_try(ps_coordination *owner,
                                 enum ps_coordination_domain domain,
                                 uint64_t identity, int background, int *wait,
                                 ps_coordination_guard **out, lc_error *e) {
  int waiting, rc = ps_coordination_lock(owner, domain, identity, 0, out, e);
  if (rc != LC_OK || !background)
    return rc;
  /* Observe intent after acquiring state. An operation already admitted
   * may finish while a foreground waiter registers. Subsequent background
   * attempts must yield while that waiter owns its witness. */
  rc = namespace_waiters(owner, identity, &waiting, e);
  if (rc != LC_OK || waiting) {
    ps_coordination_unlock(*out);
    *out = NULL;
    *wait = 0;
    if (rc == LC_OK)
      rc = ps_coordination_check(owner, e);
    if (rc == LC_OK)
      rc = fail(e, LC_ERR_PENDING, 0, "namespace state has waiting work");
  }
  return rc;
}
static int namespace_handoff(ps_coordination *owner,
                             enum ps_coordination_domain domain,
                             uint64_t identity, int pressured, int background,
                             ps_coordination_guard **out, lc_error *e) {
  const struct timespec pause = {0, 1000000L};
  struct timespec now, deadline;
  ps_coordination_guard *intent = NULL;
  int wait = !background || pressured;
  int rc =
      namespace_handoff_try(owner, domain, identity, background, &wait, out, e);
  if (rc != LC_ERR_PENDING || !wait ||
      clock_gettime(CLOCK_MONOTONIC, &deadline))
    return rc;
  deadline.tv_nsec += pressured ? 250000000L : 64000000L;
  if (deadline.tv_nsec >= 1000000000L) {
    ++deadline.tv_sec;
    deadline.tv_nsec -= 1000000000L;
  }
  if (!background && domain == PS_COORDINATION_NAMESPACE_STATE) {
    int registered = lock_guard(owner, PS_COORDINATION_NAMESPACE_WAITER,
                                identity, 0, 1, &intent, e);
    if (registered != LC_OK)
      return registered;
  }
  do {
    (void)nanosleep(&pause, NULL);
    if (clock_gettime(CLOCK_MONOTONIC, &now) || now.tv_sec > deadline.tv_sec ||
        (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec))
      break;
    lc_error_cleanup(e);
    rc = namespace_handoff_try(owner, domain, identity, background, &wait, out,
                               e);
  } while (rc == LC_ERR_PENDING && wait);
  if (intent) {
    ps_coordination_unlock(intent);
    if (ps_coordination_check(owner, NULL) != LC_OK) {
      ps_coordination_unlock(*out);
      *out = NULL;
      lc_error_cleanup(e);
      return ps_coordination_check(owner, e);
    }
  }
  return rc;
}
int ps_coordination_namespace_handoff(ps_coordination *owner,
                                      enum ps_coordination_domain domain,
                                      uint64_t identity, int pressured,
                                      ps_coordination_guard **out,
                                      lc_error *e) {
  return namespace_handoff(owner, domain, identity, pressured, 0, out, e);
}
int ps_coordination_namespace_maintenance(ps_coordination *owner,
                                          uint64_t identity, int pressured,
                                          ps_coordination_guard **out,
                                          lc_error *e) {
  return namespace_handoff(owner, PS_COORDINATION_NAMESPACE_STATE, identity,
                           pressured, 1, out, e);
}
int ps_coordination_stream_prepare(ps_coordination *owner, uint64_t identity,
                                   ps_coordination_guard **out, lc_error *e) {
  return lock_guard(owner, PS_COORDINATION_STREAM_CLAIM, identity, 0, 1, out,
                    e);
}
int ps_coordination_collection_lock(ps_coordination *owner,
                                    enum ps_coordination_domain domain,
                                    uint64_t identity, int exclusive, int wait,
                                    ps_coordination_guard **out, lc_error *e) {
  if (domain < PS_COORDINATION_COLLECTION_APPEND ||
      domain > PS_COORDINATION_COLLECTION_READERS)
    return fail(e, LC_ERR_INVALID, 0, "invalid collection authority");
  return lock_guard(owner, domain, identity, wait, !exclusive, out, e);
}
void ps_coordination_unlock(ps_coordination_guard *guard) {
  ps_coordination *owner;
  if (!guard)
    return;
  if (((guard->offset >= COLLECTION_BASE && guard->offset < 2U * PS_PAGE) ||
       (guard->offset >= STREAM_CLAIM_BASE &&
        guard->offset < PS_METADATA_START) ||
       (guard->offset >= NAMESPACE_WAITER_BASE &&
        guard->offset < PS_CREDIT_RECORD_BASE)) &&
      unpin_view(guard)) {
    drop(guard);
    return;
  }
  owner = guard->owner;
  if (owner->shared && kernel_lock(guard, F_UNLCK, 0)) {
    if (owner->failure_flag)
      __atomic_store_n(owner->failure_flag, 1, __ATOMIC_RELEASE);
    __atomic_store_n(&owner->poisoned, 1, __ATOMIC_RELEASE);
  }
  local_unlock(guard);
  drop(guard);
}
