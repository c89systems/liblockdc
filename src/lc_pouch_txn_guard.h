#ifndef LC_POUCH_TXN_GUARD_H
#define LC_POUCH_TXN_GUARD_H
#include "lc_pouch.h"
#include "lc_pouch_storage.h"
#include <pthread.h>

/* Private logical transaction authority. Initialize unused guards with zeroes
 * and fd = -1. Acquisition initializes the output; release is idempotent.
 * Root mutation authority precedes transaction authority, released in reverse.
 * The runtime remains live during acquisition and throughout protected work.
 * A typed owner retains its own lifetime until release, including after close.
 * Guards are released by the acquiring thread. No payload producer is invoked
 * while acquiring authority. Stripes may serialize unrelated xids. */
typedef struct lc_pouch_txn_guard {
  int fd;
  pthread_mutex_t *mutex;
  int mutex_locked;
  struct lc_pouch_state_shared_mutation_guard *shared_mutation;
  lc_pouch_storage storage;
  void *typed_handle;
  size_t stripe;
} lc_pouch_txn_guard;
int lc_pouch_txn_guard_lock(lc_pouch *, const char *txn_id, int wait,
                            lc_pouch_txn_guard *, lc_error *);
int lc_pouch_txn_guard_acquire(lc_pouch *, const char *txn_id,
                               lc_pouch_txn_guard *, lc_error *);
void lc_pouch_txn_guard_release(lc_pouch_txn_guard *);
/* Used to avoid reacquiring an already-held nonrecursive stripe in cleanup. */
int lc_pouch_txn_guard_covers(const lc_pouch_txn_guard *, const char *txn_id);
#endif
