#include "lc_pouch_txn_guard.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static pthread_mutex_t
    lc_pouch_txn_guard_mutexes[LC_POUCH_STORAGE_TXN_GUARD_STRIPES];
static pthread_once_t lc_pouch_txn_guard_once = PTHREAD_ONCE_INIT;

static void lc_pouch_txn_guard_mutexes_init(void) {
  size_t i;

  for (i = 0U; i < LC_POUCH_STORAGE_TXN_GUARD_STRIPES; ++i) {
    (void)pthread_mutex_init(&lc_pouch_txn_guard_mutexes[i], NULL);
  }
}

/* The endpoint-minted XID is also the implicit transaction admission key.
 * In shared mode the root durable-mutation guard is acquired first, then this
 * root-local transaction lock coordinates joins and terminal releases. The
 * ordering prevents a transaction guard/root cycle across processes. */
static size_t lc_pouch_txn_guard_stripe(const char *txn_id) {
  uint64_t hash = ((uint64_t)0xcbf29ce4UL << 32U) | (uint64_t)0x84222325UL;
  const unsigned char *cursor;
  for (cursor = (const unsigned char *)txn_id; *cursor != '\0'; ++cursor) {
    hash ^= (uint64_t)*cursor;
    hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
  }
  return (size_t)(hash % LC_POUCH_STORAGE_TXN_GUARD_STRIPES);
}

int lc_pouch_txn_guard_lock(lc_pouch *pouch, const char *txn_id, int wait,
                            lc_pouch_txn_guard *guard, lc_error *error) {
  char leaf[sizeof("pouch.implicit-txn-00.lock")];
  char *path;
  struct flock lock;
  size_t stripe;
  int pthread_rc;
  int rc;

  if (guard != NULL) {
    memset(guard, 0, sizeof(*guard));
    guard->fd = -1;
  }
  if (pouch == NULL || txn_id == NULL || txn_id[0] == '\0' || guard == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch transaction guard requires pouch, xid, and output", NULL, NULL,
        "pouch");
  }
  guard->stripe = lc_pouch_txn_guard_stripe(txn_id);
  rc = lc_pouch_state_shared_mutation_enter(pouch, &guard->shared_mutation,
                                            error);
  if (rc != LC_OK) {
    return rc;
  }
  if (pouch->storage.ops != NULL) {
    guard->storage = pouch->storage;
    rc = guard->storage.ops->txn_guard_lock(guard->storage.context,
                                            guard->stripe, wait,
                                            &guard->typed_handle, error);
    if (rc != LC_OK) {
      memset(&guard->storage, 0, sizeof(guard->storage));
      lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    }
    return rc;
  }
  stripe = guard->stripe;
  pthread_rc =
      pthread_once(&lc_pouch_txn_guard_once, lc_pouch_txn_guard_mutexes_init);
  if (pthread_rc != 0) {
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch transaction guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  guard->mutex = &lc_pouch_txn_guard_mutexes[stripe];
  pthread_rc = wait ? pthread_mutex_lock(guard->mutex)
                    : pthread_mutex_trylock(guard->mutex);
  if (pthread_rc != 0) {
    guard->mutex = NULL;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error,
                        pthread_rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT,
                        0L, "failed to lock pouch transaction guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  guard->mutex_locked = 1;
  if (snprintf(leaf, sizeof(leaf), "pouch.implicit-txn-%02lx.lock",
               (unsigned long)stripe) < 0) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch transaction guard", NULL, NULL,
                        "pouch");
  }
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path, leaf);
  if (path == NULL) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction guard path", NULL,
                        NULL, "pouch");
  }
  guard->fd = open(path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, path);
  if (guard->fd < 0) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch transaction guard",
                        strerror(errno), NULL, "pouch");
  }
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  while (fcntl(guard->fd, wait ? F_SETLKW : F_SETLK, &lock) != 0) {
    int lock_error = errno;
    if (errno == EINTR) {
      continue;
    }
    (void)close(guard->fd);
    guard->fd = -1;
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error,
                        !wait && (lock_error == EAGAIN || lock_error == EACCES)
                            ? LC_ERR_PENDING
                            : LC_ERR_TRANSPORT,
                        0L, "failed to lock pouch transaction guard",
                        strerror(lock_error), NULL, "pouch");
  }
  return LC_OK;
}

int lc_pouch_txn_guard_acquire(lc_pouch *pouch, const char *txn_id,
                               lc_pouch_txn_guard *guard, lc_error *error) {
  return lc_pouch_txn_guard_lock(pouch, txn_id, 1, guard, error);
}

void lc_pouch_txn_guard_release(lc_pouch_txn_guard *guard) {
  struct flock lock;

  if (guard == NULL) {
    return;
  }
  if (guard->typed_handle != NULL) {
    guard->storage.ops->txn_guard_release(guard->storage.context,
                                          guard->typed_handle);
    guard->typed_handle = NULL;
    memset(&guard->storage, 0, sizeof(guard->storage));
  }
  if (guard->fd >= 0) {
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    (void)fcntl(guard->fd, F_SETLK, &lock);
    (void)close(guard->fd);
    guard->fd = -1;
  }
  if (guard->mutex_locked && guard->mutex != NULL) {
    (void)pthread_mutex_unlock(guard->mutex);
  }
  guard->mutex = NULL;
  guard->mutex_locked = 0;
  lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
}

int lc_pouch_txn_guard_covers(const lc_pouch_txn_guard *guard,
                              const char *txn_id) {
  return guard != NULL && txn_id != NULL &&
         (guard->mutex_locked || guard->typed_handle != NULL) &&
         guard->stripe == lc_pouch_txn_guard_stripe(txn_id);
}
