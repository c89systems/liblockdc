#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_collection_json.h"
#include "lc_collection_maintenance.h"
#include "lc_log.h"
#include "lc_pouch_collection_record.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"
#include "lc_pouch_recode.h"
#include "lc_pouch_record.h"
#include "lc_pouch_retention.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#define LC_POUCH_STATE_RECORD_HEADER_BYTES LC_POUCH_RECORD_HEADER_BYTES
#define LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES 74U
#define LC_POUCH_STATE_PAYLOAD_FLAG_HAS_QUERY_HIDDEN 1U
#define LC_POUCH_STATE_PAYLOAD_FLAG_QUERY_HIDDEN 2U
#define LC_POUCH_STATE_PAYLOAD_FLAG_STAGED_DELETE 4U
#define LC_POUCH_STATE_PAYLOAD_FLAG_OBJECT_LINK 8U
#define LC_POUCH_STATE_PAYLOAD_FLAG_RETENTION 16U
#define LC_POUCH_STATE_RECORD_META_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_RECORD_KEY_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE 256U
#define LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS 64UL
#define LC_POUCH_STATE_BODY_CACHE_MAX_BYTES (16U * 1024U * 1024U)
#define LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_SOURCE_CACHE_MAX_FILES 64U
#define LC_POUCH_STATE_WRITEV_MAX_PARTS 15
#define LC_POUCH_STATE_WRITEV_BATCH_RECORDS 5U
#define LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES (256U * 1024U)
#define LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX 128U
#define LC_POUCH_STATE_BODY_APPEND_BATCH_MAX 16U
#define LC_POUCH_STATE_DECISION_COMMITTED "committed"
#define LC_POUCH_STATE_DECISION_DISCARDED "discarded"
#define LC_POUCH_STATE_HIGH_WATER_KEY ".lockd/high-water"
#define LC_POUCH_STATE_INDEX_TRAILER_BYTES 12U
#define LC_POUCH_STATE_INDEX_TRAILER_MAGIC 0x4c435349UL
/* A capture owns one heap buffer for candidate checksums and payload copying.
 * Keep large I/O chunks without consuming default musl thread stacks or
 * allocating scratch space per record. Never materialize a whole payload. */
#define LC_POUCH_STATE_FILE_CHUNK_BYTES (128U * 1024U)
/* Snapshot records retain their per-key query freshness by retaining the
 * legacy trailer and appending a second, distinct trailer. Normal log records
 * keep the compact legacy form and derive freshness as they are applied. */
#define LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES 24U
#define LC_POUCH_STATE_QUERY_INDEX_TRAILER_MAGIC 0x4c435351UL
#define LC_POUCH_STATE_CLEAN_INDEX_CHECKPOINT_LEAF "sequence.clean"
/* The clean projection is intentionally outside the manifest's snapshot
 * sequence: id zero is never generated for a committed snapshot. It still
 * uses the validated snapshot-leaf grammar because projection records retain
 * normal payload and record references. */
#define LC_POUCH_STATE_CLEAN_PROJECTION_LEAF "snapshot-00000000000000000000.log"

int lc_pouch_retention_resolve(lc_pouch_unix_seconds retention,
                               lc_pouch_unix_seconds commit_at_unix,
                               lc_pouch_unix_seconds *out, lc_error *error) {
  if (out == NULL || retention == LC_I64_MIN ||
      (retention < 0 &&
       (commit_at_unix < 0 || commit_at_unix > LC_I64_MAX + retention))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention exceeds supported timestamp range",
                        NULL, NULL, "pouch");
  }
  *out = retention < 0 ? commit_at_unix - retention : retention;
  return LC_OK;
}

static int lc_pouch_state_key_is_staged(const char *key);

static int lc_pouch_state_retention_prepare(const char *key,
                                            lc_pouch_unix_seconds requested,
                                            lc_pouch_unix_seconds *out,
                                            lc_error *error) {
  lc_pouch_unix_seconds now;
  lc_pouch_unix_seconds deadline = 0;
  int rc;

  if (requested >= 0) {
    *out = requested;
    return LC_OK;
  }
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_resolve(requested, now, &deadline, error);
  if (rc == LC_OK)
    *out = lc_pouch_state_key_is_staged(key) ? requested : deadline;
  return rc;
}

static int lc_pouch_state_retention_publish(lc_pouch *pouch, const char *ns,
                                            const char *key,
                                            lc_pouch_unix_seconds requested,
                                            lc_pouch_unix_seconds *out,
                                            lc_error *error) {
  int rc = lc_pouch_state_retention_prepare(key, requested, out, error);
  if (rc == LC_OK && *out > 0 && !lc_pouch_state_key_is_staged(key))
    rc = lc_pouch_retention_hint_ensure(pouch, ns, error);
  return rc;
}

static int lc_pouch_state_retention_commit(lc_pouch_unix_seconds retention,
                                           lc_pouch_unix_seconds commit_at,
                                           lc_pouch_unix_seconds *out,
                                           lc_error *error) {
  int rc = LC_OK;
  if (retention < 0 && commit_at < 0)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "retention-bearing commit is missing its timestamp",
                        NULL, NULL, "pouch");
  if (retention < 0 && commit_at == 0)
    rc = lc_pouch_now_unix(&commit_at, error);
  return rc == LC_OK
             ? lc_pouch_retention_resolve(retention, commit_at, out, error)
             : rc;
}

#ifdef LOCKDC_TEST_BUILD
lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook = NULL;
void *lc_pouch_test_after_snapshot_write_context = NULL;
lc_pouch_test_metadata_append_hook_fn lc_pouch_test_metadata_append_hook = NULL;
void *lc_pouch_test_metadata_append_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_metadata_append_entry_copy_hook = NULL;
void *lc_pouch_test_before_metadata_append_entry_copy_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_metadata_batch_append_hook = NULL;
void *lc_pouch_test_after_metadata_batch_append_context = NULL;
lc_pouch_test_body_append_hook_fn lc_pouch_test_body_append_hook = NULL;
void *lc_pouch_test_body_append_context = NULL;
lc_pouch_test_tail_repair_hook_fn lc_pouch_test_tail_repair_hook = NULL;
void *lc_pouch_test_tail_repair_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_terminal_reclaim_marker_remove_hook =
    NULL;
void *lc_pouch_test_before_terminal_reclaim_marker_remove_context = NULL;
lc_pouch_test_hook lc_pouch_test_cleanup_fsync_hook = NULL;
void *lc_pouch_test_cleanup_fsync_context = NULL;
#endif

typedef struct lc_pouch_state_namespace_lock {
  int fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  struct lc_pouch_state_process_namespace_guard *maintenance_guard;
  struct lc_pouch_state_shared_mutation_guard *shared_mutation;
  pthread_mutex_t *exclusive_append_gate;
  int shared_projection_locked;
  int borrowed;
  lc_pouch *pouch;
  const char *ns;
  struct lc_pouch_state_namespace_lock *previous_namespace_lock;
} lc_pouch_state_namespace_lock;

typedef struct lc_pouch_state_key_lock {
  int fd;
  int maintenance_fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  struct lc_pouch_state_process_namespace_guard *maintenance_guard;
  struct lc_pouch_state_shared_mutation_guard *shared_mutation;
  pthread_mutex_t *exclusive_key_mutex;
  lc_pouch *pouch;
  const char *ns;
  int projection_uses_namespace_mutex;
  int writer_mode_operation_active;
  struct lc_pouch_state_key_lock *previous_projection_lock;
} lc_pouch_state_key_lock;

typedef struct lc_pouch_state_append_lock {
  int fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  pthread_mutex_t *exclusive_gate;
  lc_pouch *pouch;
  const char *ns;
  struct lc_pouch_state_append_lock *previous_append_lock;
  int borrowed;
} lc_pouch_state_append_lock;

typedef struct lc_pouch_state_deferred_fsync {
  int fd;
  dev_t device;
  ino_t inode;
  struct lc_pouch_state_deferred_fsync *next;
} lc_pouch_state_deferred_fsync;

struct lc_pouch_state_commit_group {
  lc_pouch *pouch;
  struct lc_pouch_state_commit_group *parent;
  lc_pouch_state_deferred_fsync *head;
  lc_pouch_state_deferred_fsync *tail;
  int force_sync;
};

typedef struct lc_pouch_state_process_namespace_mutex {
  char *identity;
  pthread_mutex_t mutex;
  unsigned long refcount;
  struct lc_pouch_state_process_namespace_mutex *next;
} lc_pouch_state_process_namespace_mutex;

typedef struct lc_pouch_state_process_namespace_guard {
  char *identity;
  pthread_rwlock_t lock;
  pthread_mutex_t writer_mutex;
  pthread_t writer;
  unsigned long writer_depth;
  int writer_active;
  unsigned long refcount;
  struct lc_pouch_state_process_namespace_guard *next;
} lc_pouch_state_process_namespace_guard;

struct lc_pouch_state_shared_mutation_guard {
  char *identity;
  pthread_mutex_t mutex;
  uint64_t root_device;
  uint64_t root_inode;
  int fd;
  unsigned long depth;
  unsigned long refcount;
  struct lc_pouch_state_shared_mutation_guard *next;
};

static pthread_mutex_t lc_pouch_state_process_mutex_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_process_namespace_mutex *lc_pouch_state_process_mutexes;
static pthread_mutex_t lc_pouch_state_process_guard_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_process_namespace_guard *lc_pouch_state_process_guards;
static pthread_mutex_t lc_pouch_state_shared_mutation_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_shared_mutation_guard
    *lc_pouch_state_shared_mutation_guards;
static pthread_once_t lc_pouch_state_shared_mutation_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_shared_mutation_key;
static int lc_pouch_state_shared_mutation_key_status = -1;
static pthread_once_t lc_pouch_state_commit_group_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_commit_group_key;
static int lc_pouch_state_commit_group_key_status = -1;
static pthread_once_t lc_pouch_state_namespace_lock_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_namespace_lock_key;
static int lc_pouch_state_namespace_lock_key_status = -1;
static pthread_once_t lc_pouch_state_projection_lock_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_projection_lock_key;
static int lc_pouch_state_projection_lock_key_status = -1;
static pthread_once_t lc_pouch_state_append_lock_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_append_lock_key;
static int lc_pouch_state_append_lock_key_status = -1;
static pthread_once_t lc_pouch_state_body_append_worker_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_body_append_worker_key;
static int lc_pouch_state_body_append_worker_key_status = -1;

/* These keys only borrow active operation frames. All operations/workers are
 * closed before unload; return the key slots as well as their heap owners. */
static void lc_pouch_state_cleanup_keys(void) __attribute__((destructor));
static void lc_pouch_state_cleanup_keys(void) {
  if (lc_pouch_state_shared_mutation_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_shared_mutation_key);
  if (lc_pouch_state_commit_group_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_commit_group_key);
  if (lc_pouch_state_namespace_lock_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_namespace_lock_key);
  if (lc_pouch_state_projection_lock_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_projection_lock_key);
  if (lc_pouch_state_append_lock_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_append_lock_key);
  if (lc_pouch_state_body_append_worker_key_status == 0)
    (void)pthread_key_delete(lc_pouch_state_body_append_worker_key);
}

typedef struct lc_pouch_state_entry lc_pouch_state_entry;

static int lc_pouch_state_manifest_leaf_obsolete(
    const lc_pouch_namespace_manifest *manifest, const char *leaf,
    int snapshot);

static int lc_pouch_state_manifest_segment_visible(
    const lc_pouch_namespace_manifest *manifest, const char *leaf) {
  if (manifest == NULL || leaf == NULL ||
      lc_pouch_state_manifest_leaf_obsolete(manifest, leaf, 0)) {
    return 0;
  }
  return 1;
}

static int
lc_pouch_state_manifest_has_segment(const lc_pouch_namespace_manifest *manifest,
                                    const char *leaf) {
  unsigned long index;

  if (manifest == NULL || leaf == NULL || manifest->segment_leaves == NULL) {
    return 0;
  }
  for (index = 0UL; index < manifest->segment_count; ++index) {
    if (manifest->segment_leaves[index] != NULL &&
        strcmp(manifest->segment_leaves[index], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_state_manifest_segment_allows_tail_repair(
    const lc_pouch_namespace_manifest *manifest, const char *leaf) {
  if (manifest == NULL || leaf == NULL) {
    return 0;
  }
  return manifest->active_segment != NULL &&
         strcmp(leaf, manifest->active_segment) == 0;
}

static int lc_pouch_state_meta_set_index_seq(unsigned char *meta,
                                             size_t meta_len,
                                             lc_pouch_generation index_seq,
                                             lc_error *error);
static int lc_pouch_state_meta_index_seq(const unsigned char *meta,
                                         size_t meta_len,
                                         lc_pouch_generation *out,
                                         lc_error *error);
static void
lc_pouch_state_namespace_lock_release(lc_pouch_state_namespace_lock *lock);
static int lc_pouch_state_manifest_max_version(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_generation *max_version, lc_error *error);
static int lc_pouch_state_repair_active_tail_locked(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, const char *segment_path,
    const char *segment_leaf, lc_error *error);
static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error);
static int lc_pouch_state_cache_set_active_segment(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, uint64_t offset,
    lc_error *error);
static int lc_pouch_state_cache_active_append_fd(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int *out, lc_error *error);
static int
lc_pouch_state_cache_lookup(lc_pouch *pouch, const char *ns,
                            const lc_pouch_namespace_manifest *manifest,
                            const char *key, lc_pouch_state_entry *out,
                            lc_pouch_generation *max_version_out,
                            lc_error *error);
static int lc_pouch_state_cache_refresh_for_mode(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, lc_error *error);
static int lc_pouch_state_cache_matches_manifest(
    const lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest);
static void lc_pouch_state_cache_invalidate_namespace(lc_pouch *pouch,
                                                      const char *ns);
static int lc_pouch_state_body_append_schedule_locked(
    lc_pouch *pouch, const char *ns, const char *key, lc_source *body,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error);
static int
lc_pouch_state_manifest_materialize(lc_pouch *pouch, const char *ns,
                                    lc_pouch_namespace_manifest *manifest,
                                    int *from_cache, lc_error *error);
static int lc_pouch_state_process_namespace_mutex_lock(
    lc_pouch *pouch, const char *ns,
    lc_pouch_state_process_namespace_mutex **out, lc_error *error);
static void lc_pouch_state_process_namespace_mutex_unlock(
    lc_pouch_state_process_namespace_mutex **mutex);
static int lc_pouch_state_shared_mutation_acquire(
    lc_pouch *pouch, lc_pouch_state_shared_mutation_guard **out,
    lc_error *error);
static void lc_pouch_state_shared_mutation_release(
    lc_pouch_state_shared_mutation_guard **guard);
static int lc_pouch_state_shared_mutation_is_held(const lc_pouch *pouch);
static int lc_pouch_state_exclusive_append_gate_lock(lc_pouch *pouch,
                                                     const char *ns,
                                                     pthread_mutex_t **out,
                                                     lc_error *error);
static void lc_pouch_state_exclusive_append_gate_unlock(pthread_mutex_t **gate);

static void lc_pouch_state_commit_group_key_init(void) {
  lc_pouch_state_commit_group_key_status =
      pthread_key_create(&lc_pouch_state_commit_group_key, NULL);
}

static void lc_pouch_state_namespace_lock_key_init(void) {
  lc_pouch_state_namespace_lock_key_status =
      pthread_key_create(&lc_pouch_state_namespace_lock_key, NULL);
}

static void lc_pouch_state_projection_lock_key_init(void) {
  lc_pouch_state_projection_lock_key_status =
      pthread_key_create(&lc_pouch_state_projection_lock_key, NULL);
}

static void lc_pouch_state_append_lock_key_init(void) {
  lc_pouch_state_append_lock_key_status =
      pthread_key_create(&lc_pouch_state_append_lock_key, NULL);
}

static void lc_pouch_state_body_append_worker_key_init(void) {
  lc_pouch_state_body_append_worker_key_status =
      pthread_key_create(&lc_pouch_state_body_append_worker_key, NULL);
}

static void lc_pouch_state_shared_mutation_key_init(void) {
  lc_pouch_state_shared_mutation_key_status =
      pthread_key_create(&lc_pouch_state_shared_mutation_key, NULL);
}

static int
lc_pouch_state_namespace_lock_track(lc_pouch_state_namespace_lock *lock,
                                    lc_pouch *pouch, const char *ns,
                                    lc_error *error) {
  int pthread_rc;

  pthread_once(&lc_pouch_state_namespace_lock_key_once,
               lc_pouch_state_namespace_lock_key_init);
  if (lc_pouch_state_namespace_lock_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch namespace lock tracking",
                        strerror(lc_pouch_state_namespace_lock_key_status),
                        NULL, "pouch");
  }
  lock->pouch = pouch;
  lock->ns = ns;
  lock->previous_namespace_lock =
      (lc_pouch_state_namespace_lock *)pthread_getspecific(
          lc_pouch_state_namespace_lock_key);
  pthread_rc = pthread_setspecific(lc_pouch_state_namespace_lock_key, lock);
  if (pthread_rc != 0) {
    lock->pouch = NULL;
    lock->ns = NULL;
    lock->previous_namespace_lock = NULL;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to track pouch namespace lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_projection_lock_track(lc_pouch_state_key_lock *lock,
                                                lc_error *error) {
  int pthread_rc;

  if (lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch projection lock tracking requires lock", NULL,
                        NULL, "pouch");
  }
  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch projection lock tracking",
                        strerror(lc_pouch_state_projection_lock_key_status),
                        NULL, "pouch");
  }
  lock->previous_projection_lock =
      (lc_pouch_state_key_lock *)pthread_getspecific(
          lc_pouch_state_projection_lock_key);
  pthread_rc = pthread_setspecific(lc_pouch_state_projection_lock_key, lock);
  if (pthread_rc != 0) {
    lock->previous_projection_lock = NULL;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to track pouch projection lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void
lc_pouch_state_projection_lock_untrack(lc_pouch_state_key_lock *lock) {
  if (lock == NULL) {
    return;
  }
  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status == 0) {
    (void)pthread_setspecific(lc_pouch_state_projection_lock_key,
                              lock->previous_projection_lock);
  }
  lock->previous_projection_lock = NULL;
}

static lc_pouch_state_key_lock *
lc_pouch_state_projection_lock_current(lc_pouch *pouch, const char *ns) {
  lc_pouch_state_key_lock *lock;

  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status != 0) {
    return NULL;
  }
  lock = (lc_pouch_state_key_lock *)pthread_getspecific(
      lc_pouch_state_projection_lock_key);
  if (lock == NULL || lock->pouch != pouch || lock->ns == NULL ||
      lock->ns[0] == '\0' || pouch == NULL || ns == NULL ||
      strcmp(lock->ns, ns) != 0) {
    return NULL;
  }
  return lock;
}

static int lc_pouch_state_namespace_lock_is_held(lc_pouch *pouch,
                                                 const char *ns) {
  lc_pouch_state_namespace_lock *lock;

  pthread_once(&lc_pouch_state_namespace_lock_key_once,
               lc_pouch_state_namespace_lock_key_init);
  if (lc_pouch_state_namespace_lock_key_status != 0) {
    return 0;
  }
  for (lock = (lc_pouch_state_namespace_lock *)pthread_getspecific(
           lc_pouch_state_namespace_lock_key);
       lock != NULL; lock = lock->previous_namespace_lock) {
    if (lock->pouch == pouch && lock->ns != NULL && strcmp(lock->ns, ns) == 0) {
      return 1;
    }
  }
  return 0;
}

/* Closing a client from an application source or visitor callback is valid.
 * Such callbacks can run under this authority while an outbox recovery
 * thread is waiting to acquire it.  Lifecycle code uses this narrow
 * thread-local query to defer a dispatcher join until that callback unwinds,
 * avoiding a wait cycle without weakening the namespace lock itself. */
int lc_pouch_state_namespace_lock_held_by_current_thread(lc_pouch *pouch) {
  lc_pouch_state_namespace_lock *lock;

  if (pouch == NULL) {
    return 0;
  }
  pthread_once(&lc_pouch_state_namespace_lock_key_once,
               lc_pouch_state_namespace_lock_key_init);
  if (lc_pouch_state_namespace_lock_key_status != 0) {
    return 0;
  }
  for (lock = (lc_pouch_state_namespace_lock *)pthread_getspecific(
           lc_pouch_state_namespace_lock_key);
       lock != NULL; lock = lock->previous_namespace_lock) {
    if (lock->pouch == pouch) {
      return 1;
    }
  }
  return 0;
}

static void
lc_pouch_state_namespace_lock_untrack(lc_pouch_state_namespace_lock *lock) {
  if (lock == NULL || lock->pouch == NULL) {
    return;
  }
  if (pthread_getspecific(lc_pouch_state_namespace_lock_key) == lock) {
    (void)pthread_setspecific(lc_pouch_state_namespace_lock_key,
                              lock->previous_namespace_lock);
  }
  lock->pouch = NULL;
  lock->ns = NULL;
  lock->previous_namespace_lock = NULL;
}

static int lc_pouch_state_append_lock_track(lc_pouch_state_append_lock *lock,
                                            lc_pouch *pouch, const char *ns,
                                            lc_error *error) {
  int pthread_rc;

  pthread_once(&lc_pouch_state_append_lock_key_once,
               lc_pouch_state_append_lock_key_init);
  if (lc_pouch_state_append_lock_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch append lock tracking",
                        strerror(lc_pouch_state_append_lock_key_status), NULL,
                        "pouch");
  }
  lock->pouch = pouch;
  lock->ns = ns;
  lock->previous_append_lock =
      (lc_pouch_state_append_lock *)pthread_getspecific(
          lc_pouch_state_append_lock_key);
  pthread_rc = pthread_setspecific(lc_pouch_state_append_lock_key, lock);
  if (pthread_rc != 0) {
    lock->pouch = NULL;
    lock->ns = NULL;
    lock->previous_append_lock = NULL;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to track pouch append lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static lc_pouch_state_append_lock *
lc_pouch_state_append_lock_current(lc_pouch *pouch, const char *ns) {
  lc_pouch_state_append_lock *lock;

  pthread_once(&lc_pouch_state_append_lock_key_once,
               lc_pouch_state_append_lock_key_init);
  if (lc_pouch_state_append_lock_key_status != 0) {
    return NULL;
  }
  for (lock = (lc_pouch_state_append_lock *)pthread_getspecific(
           lc_pouch_state_append_lock_key);
       lock != NULL; lock = lock->previous_append_lock) {
    if (lock->pouch == pouch && lock->ns != NULL && ns != NULL &&
        strcmp(lock->ns, ns) == 0) {
      return lock;
    }
  }
  return NULL;
}

static void
lc_pouch_state_append_lock_untrack(lc_pouch_state_append_lock *lock) {
  if (lock == NULL || lock->borrowed || lock->pouch == NULL) {
    return;
  }
  pthread_once(&lc_pouch_state_append_lock_key_once,
               lc_pouch_state_append_lock_key_init);
  if (lc_pouch_state_append_lock_key_status == 0 &&
      pthread_getspecific(lc_pouch_state_append_lock_key) == lock) {
    (void)pthread_setspecific(lc_pouch_state_append_lock_key,
                              lock->previous_append_lock);
  }
  lock->pouch = NULL;
  lock->ns = NULL;
  lock->previous_append_lock = NULL;
}

static int lc_pouch_state_body_append_worker_active(void) {
  pthread_once(&lc_pouch_state_body_append_worker_key_once,
               lc_pouch_state_body_append_worker_key_init);
  return lc_pouch_state_body_append_worker_key_status == 0 &&
         pthread_getspecific(lc_pouch_state_body_append_worker_key) != NULL;
}

static void lc_pouch_state_body_append_worker_set_active(int active) {
  pthread_once(&lc_pouch_state_body_append_worker_key_once,
               lc_pouch_state_body_append_worker_key_init);
  if (lc_pouch_state_body_append_worker_key_status == 0) {
    (void)pthread_setspecific(lc_pouch_state_body_append_worker_key,
                              active ? (void *)1 : NULL);
  }
}

static lc_pouch_state_commit_group *lc_pouch_state_commit_group_current(void) {
  pthread_once(&lc_pouch_state_commit_group_key_once,
               lc_pouch_state_commit_group_key_init);
  /* Failed key creation leaves no owned slot: key zero may belong to the
   * application. Never interpret that thread-local value as a group. */
  if (lc_pouch_state_commit_group_key_status != 0) {
    return NULL;
  }
  return (lc_pouch_state_commit_group *)pthread_getspecific(
      lc_pouch_state_commit_group_key);
}

static void
lc_pouch_state_commit_group_set(lc_pouch_state_commit_group *group) {
  pthread_once(&lc_pouch_state_commit_group_key_once,
               lc_pouch_state_commit_group_key_init);
  if (lc_pouch_state_commit_group_key_status == 0) {
    (void)pthread_setspecific(lc_pouch_state_commit_group_key, group);
  }
}

static int lc_pouch_state_commit_group_begin(lc_pouch *pouch,
                                             lc_pouch_state_commit_group **out,
                                             int *owned, lc_error *error) {
  lc_pouch_state_commit_group *current;
  lc_pouch_state_commit_group *group;

  if (pouch == NULL || out == NULL || owned == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit group requires pouch and outputs", NULL,
                        NULL, "pouch");
  }
  if (pouch->aborted) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutations are closed after abort", NULL, NULL,
                        "pouch");
  }
  current = lc_pouch_state_commit_group_current();
  if (lc_pouch_state_commit_group_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch commit group tracking",
                        strerror(lc_pouch_state_commit_group_key_status), NULL,
                        "pouch");
  }
  if (current != NULL && current->pouch == pouch) {
    *out = current;
    *owned = 0;
    return LC_OK;
  }
  group = (lc_pouch_state_commit_group *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*group));
  if (group == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch commit group", NULL, NULL,
                        "pouch");
  }
  group->pouch = pouch;
  group->parent = current;
  lc_pouch_state_commit_group_set(group);
  *out = group;
  *owned = 1;
  return LC_OK;
}

static void
lc_pouch_state_commit_group_free(lc_pouch_state_commit_group *group) {
  lc_pouch_state_deferred_fsync *item;
  lc_pouch_state_deferred_fsync *next;

  if (group == NULL) {
    return;
  }
  item = group->head;
  while (item != NULL) {
    next = item->next;
    if (item->fd >= 0) {
      close(item->fd);
    }
    lc_free_with_allocator(NULL, item);
    item = next;
  }
  lc_free_with_allocator(NULL, group);
}

static int lc_pouch_state_commit_group_sync(lc_pouch_state_commit_group *group,
                                            int fd, lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  if (group->force_sync && lc_pouch_test_cleanup_fsync_hook != NULL) {
    int rc = lc_pouch_test_cleanup_fsync_hook(
        lc_pouch_test_cleanup_fsync_context, error);
    if (rc != LC_OK)
      return rc;
  }
#endif
  if (!group->force_sync || group->pouch->durable_sync)
    return lc_pouch_fsync_commit(group->pouch, fd, error);
  if (fsync(fd) != 0)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to sync pouch cleanup durability barrier",
                        strerror(errno), NULL, "pouch");
  return LC_OK;
}

int lc_pouch_state_durable_scope_begin(lc_pouch *pouch,
                                       lc_pouch_state_commit_group **out,
                                       lc_error *error) {
  lc_pouch_state_commit_group *scope;
  int owned = 0;
  int rc;
  if (out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cleanup durability requires output", NULL, NULL,
                        "pouch");
  *out = NULL;
  rc = lc_pouch_state_commit_group_begin(pouch, &scope, &owned, error);
  if (rc != LC_OK)
    return rc;
  if (!owned)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch cleanup durability must begin outside a mutation", NULL, NULL,
        "pouch");
  scope->force_sync = 1;
  *out = scope;
  return LC_OK;
}

int lc_pouch_state_durable_scope_barrier(lc_pouch_state_commit_group *scope,
                                         lc_error *error) {
  lc_pouch_state_deferred_fsync *item;
  int rc;
  if (scope == NULL || !scope->force_sync ||
      lc_pouch_state_commit_group_current() != scope)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cleanup barrier requires its active scope", NULL,
                        NULL, "pouch");
  while ((item = scope->head) != NULL) {
    rc = lc_pouch_state_commit_group_sync(scope, item->fd, error);
    if (rc != LC_OK)
      return rc;
    scope->head = item->next;
    if (scope->head == NULL)
      scope->tail = NULL;
    (void)close(item->fd);
    lc_free_with_allocator(NULL, item);
  }
  return LC_OK;
}

static int lc_pouch_state_commit_group_end(lc_pouch_state_commit_group *group,
                                           int owned, lc_error *error) {
  lc_pouch_state_deferred_fsync *item;
  int rc;

  if (!owned) {
    return LC_OK;
  }
  if (group == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit group end requires group", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_commit_group_set(group->parent);
  rc = LC_OK;
  for (item = group->head; item != NULL; item = item->next) {
    if (item->fd >= 0) {
      rc = lc_pouch_state_commit_group_sync(group, item->fd, error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  lc_pouch_state_commit_group_free(group);
  return rc;
}

static uint64_t lc_pouch_state_clean_identity_mix(uint64_t value,
                                                  uint64_t item) {
  value ^= item;
  value *= (uint64_t)16777619U;
  return value;
}

static int lc_pouch_state_clean_checkpoint_identity(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    uint64_t *identity, lc_error *error) {
  uint64_t value;
  unsigned long index;

  if (pouch == NULL || manifest == NULL || manifest->namespace_path == NULL ||
      identity == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch clean checkpoint identity requires namespace",
                        NULL, NULL, "pouch");
  }
  value = (uint64_t)2166136261U;
  for (index = 0UL; index < manifest->segment_count; ++index) {
    char *dir;
    char *path;
    struct stat st;

    dir = lc_pouch_path_join(&pouch->allocator, manifest->namespace_path,
                             "segments");
    path = dir != NULL ? lc_pouch_path_join(&pouch->allocator, dir,
                                            manifest->segment_leaves[index])
                       : NULL;
    lc_free_with_allocator(&pouch->allocator, dir);
    if (path == NULL || stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
      lc_free_with_allocator(&pouch->allocator, path);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to stat pouch clean checkpoint segment",
                          strerror(errno), NULL, "pouch");
    }
    value = lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_dev);
    value = lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_ino);
    value = lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_size);
#if defined(__APPLE__)
    value = lc_pouch_state_clean_identity_mix(value,
                                              (uint64_t)st.st_mtimespec.tv_sec);
    value = lc_pouch_state_clean_identity_mix(
        value, (uint64_t)st.st_mtimespec.tv_nsec);
    value = lc_pouch_state_clean_identity_mix(value,
                                              (uint64_t)st.st_ctimespec.tv_sec);
    value = lc_pouch_state_clean_identity_mix(
        value, (uint64_t)st.st_ctimespec.tv_nsec);
#else
    value =
        lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_mtim.tv_sec);
    value =
        lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_mtim.tv_nsec);
    value =
        lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_ctim.tv_sec);
    value =
        lc_pouch_state_clean_identity_mix(value, (uint64_t)st.st_ctim.tv_nsec);
#endif
    lc_free_with_allocator(&pouch->allocator, path);
  }
  *identity = value;
  return LC_OK;
}

static int lc_pouch_state_clean_checkpoint_read(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    int *state_valid, int *query_valid, lc_pouch_generation *value,
    lc_pouch_generation *query_value, uint64_t *identity, lc_error *error) {
  char *path;
  char line[96];
  lc_u64 parsed_max;
  lc_u64 parsed_query;
  FILE *fp;
  int rc;

  if (pouch == NULL || manifest == NULL || manifest->namespace_path == NULL ||
      state_valid == NULL || query_valid == NULL || value == NULL ||
      query_value == NULL || identity == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch clean checkpoint requires namespace outputs",
                        NULL, NULL, "pouch");
  }
  *state_valid = 0;
  *query_valid = 0;
  *value = 0UL;
  *query_value = 0UL;
  *identity = 0U;
  path = lc_pouch_path_join(&pouch->allocator, manifest->namespace_path,
                            LC_POUCH_STATE_CLEAN_INDEX_CHECKPOINT_LEAF);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch clean checkpoint path", NULL,
                        NULL, "pouch");
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      lc_free_with_allocator(&pouch->allocator, path);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch clean checkpoint", strerror(errno),
                      NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  if (fgets(line, sizeof(line), fp) != NULL && strncmp(line, "max=", 4U) == 0) {
    if (lc_parse_u64_base10_range_checked(line + 4U, strlen(line + 4U),
                                          &parsed_max)) {
      *value = (lc_pouch_generation)parsed_max;
      *state_valid = 1;
    }
    if (fgets(line, sizeof(line), fp) != NULL &&
        strncmp(line, "query_max=", 10U) == 0 &&
        lc_parse_u64_base10_range_checked(line + 10U, strlen(line + 10U),
                                          &parsed_query)) {
      *query_value = (lc_pouch_generation)parsed_query;
      *query_valid = 1;
    }
    if (*state_valid && *query_valid && fgets(line, sizeof(line), fp) != NULL &&
        strncmp(line, "identity=", 9U) == 0 &&
        lc_parse_u64_base10_range_checked(line + 9U, strlen(line + 9U),
                                          &parsed_query)) {
      *identity = (uint64_t)parsed_query;
    } else {
      *state_valid = 0;
      *query_valid = 0;
    }
  }
  if (*state_valid && *query_valid && *query_value > *value) {
    *state_valid = 0;
    *query_valid = 0;
    *value = 0UL;
    *query_value = 0UL;
  }
  if (fclose(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch clean checkpoint", strerror(errno),
                      NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  lc_free_with_allocator(&pouch->allocator, path);
  return LC_OK;
}

static int lc_pouch_state_defer_fsync(lc_pouch *pouch, int fd,
                                      lc_error *error) {
  lc_pouch_state_commit_group *group;
  lc_pouch_state_deferred_fsync *item;
  lc_pouch_state_deferred_fsync *existing;
  struct stat st;
  int dup_fd;

  if (pouch == NULL) {
    return LC_OK;
  }
  group = lc_pouch_state_commit_group_current();
  if (!pouch->durable_sync &&
      (group == NULL || group->pouch != pouch || !group->force_sync))
    return LC_OK;
  if (group == NULL || group->pouch != pouch) {
    return lc_pouch_fsync_commit(pouch, fd, error);
  }
  if (fstat(fd, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch deferred fsync file",
                        strerror(errno), NULL, "pouch");
  }
  for (existing = group->head; existing != NULL; existing = existing->next) {
    if (existing->device == st.st_dev && existing->inode == st.st_ino) {
      return LC_OK;
    }
  }
  dup_fd = dup(fd);
  if (dup_fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to duplicate pouch state segment fd",
                        strerror(errno), NULL, "pouch");
  }
  item = (lc_pouch_state_deferred_fsync *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*item));
  if (item == NULL) {
    close(dup_fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch deferred fsync", NULL, NULL,
                        "pouch");
  }
  item->fd = dup_fd;
  item->device = st.st_dev;
  item->inode = st.st_ino;
  if (group->tail != NULL) {
    group->tail->next = item;
  } else {
    group->head = item;
  }
  group->tail = item;
  return LC_OK;
}

static int
lc_pouch_state_finish_commit_group_common(lc_pouch_state_commit_group *group,
                                          int owned, int rc, lc_error *error) {
  int commit_rc;

  commit_rc = LC_OK;
  if (group != NULL) {
    commit_rc = lc_pouch_state_commit_group_end(group, owned,
                                                rc == LC_OK ? error : NULL);
  }
  if (commit_rc != LC_OK && rc == LC_OK) {
    rc = commit_rc;
  }
  return rc;
}

int lc_pouch_state_durable_scope_end(lc_pouch_state_commit_group *scope,
                                     int operation_rc, lc_error *error) {
  if (scope == NULL)
    return operation_rc;
  return lc_pouch_state_finish_commit_group_common(scope, 1, operation_rc,
                                                   error);
}

static int
lc_pouch_state_finish_commit_group(lc_pouch_state_commit_group *group,
                                   int owned, int rc, lc_error *error) {
  return lc_pouch_state_finish_commit_group_common(group, owned, rc, error);
}

static int lc_pouch_state_mutex_init_recursive(pthread_mutex_t *mutex,
                                               lc_error *error) {
  pthread_mutexattr_t attr;
  int pthread_rc;

  pthread_rc = pthread_mutexattr_init(&attr);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch mutex attributes",
                        strerror(pthread_rc), NULL, NULL);
  }
  pthread_rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (pthread_rc == 0) {
    pthread_rc = pthread_mutex_init(mutex, &attr);
  }
  (void)pthread_mutexattr_destroy(&attr);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch namespace mutex",
                        strerror(pthread_rc), NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_state_process_mutex_identity(lc_pouch *pouch,
                                                 const char *ns, char **out,
                                                 lc_error *error) {
  char root_identity[34];
  char device_hex[17];
  char inode_hex[17];
  size_t namespace_len;
  char *identity;

  if (pouch == NULL || ns == NULL || out == NULL ||
      !pouch->root_identity_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity requires pouch, "
                        "namespace, and output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (lc_u64_format_base16_padded((lc_u64)pouch->root_device, 16U, device_hex,
                                  sizeof(device_hex)) < 0 ||
      lc_u64_format_base16_padded((lc_u64)pouch->root_inode, 16U, inode_hex,
                                  sizeof(inode_hex)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity is too large", NULL,
                        NULL, "pouch");
  }
  memcpy(root_identity, device_hex, 16U);
  root_identity[16] = ':';
  memcpy(root_identity + 17U, inode_hex, 16U);
  root_identity[33] = '\0';
  namespace_len = strlen(ns);
  identity = (char *)lc_alloc_with_allocator(NULL, sizeof(root_identity) +
                                                       namespace_len + 1U);
  if (identity == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace mutex identity",
                        NULL, NULL, NULL);
  }
  memcpy(identity, root_identity, sizeof(root_identity) - 1U);
  identity[sizeof(root_identity) - 1U] = '\n';
  memcpy(identity + sizeof(root_identity), ns, namespace_len + 1U);
  *out = identity;
  return LC_OK;
}

static int lc_pouch_state_process_namespace_mutex_lock(
    lc_pouch *pouch, const char *ns,
    lc_pouch_state_process_namespace_mutex **out, lc_error *error) {
  lc_pouch_state_process_namespace_mutex *entry;
  lc_pouch_state_process_namespace_mutex *created;
  char *identity;
  int pthread_rc;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex requires pouch, namespace, and "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  identity = NULL;
  rc = lc_pouch_state_process_mutex_identity(pouch, ns, &identity, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
  if (pthread_rc != 0) {
    lc_free_with_allocator(NULL, identity);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace mutex registry",
                        strerror(pthread_rc), NULL, NULL);
  }
  for (entry = lc_pouch_state_process_mutexes; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->identity, identity) == 0) {
      break;
    }
  }
  if (entry == NULL) {
    created =
        (lc_pouch_state_process_namespace_mutex *)lc_calloc_with_allocator(
            NULL, 1U, sizeof(*created));
    if (created == NULL) {
      pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
      lc_free_with_allocator(NULL, identity);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch namespace mutex", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_mutex_init_recursive(&created->mutex, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return rc;
    }
    created->identity = identity;
    created->next = lc_pouch_state_process_mutexes;
    lc_pouch_state_process_mutexes = created;
    entry = created;
    identity = NULL;
  }
  entry->refcount += 1UL;
  pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
  lc_free_with_allocator(NULL, identity);
  pthread_rc = pthread_mutex_lock(&entry->mutex);
  if (pthread_rc != 0) {
    pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
    entry->refcount -= 1UL;
    pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace mutex",
                        strerror(pthread_rc), NULL, NULL);
  }
  *out = entry;
  return LC_OK;
}

static void lc_pouch_state_process_namespace_mutex_unlock(
    lc_pouch_state_process_namespace_mutex **mutex) {
  lc_pouch_state_process_namespace_mutex *entry;

  if (mutex == NULL || *mutex == NULL) {
    return;
  }
  entry = *mutex;
  *mutex = NULL;
  pthread_mutex_unlock(&entry->mutex);
  pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
  if (entry->refcount > 0UL) {
    entry->refcount -= 1UL;
  }
  pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
}

/* Default exclusive mode protects one namespace projection with the same
 * in-process namespace mutex used by readers and maintenance. Shared mode
 * holds the global projection mutex under its root mutation authority. */
static void lc_pouch_state_projection_mutex_unlock(lc_pouch *pouch,
                                                   const char *ns) {
  lc_pouch_state_key_lock *lock;

  if (lc_pouch_single_writer_enabled(pouch)) {
    lock = lc_pouch_state_projection_lock_current(pouch, ns);
    if (lock != NULL && lock->process_mutex != NULL) {
      lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    }
    return;
  }
  (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
}

static int lc_pouch_state_projection_mutex_lock(lc_pouch *pouch, const char *ns,
                                                lc_error *error) {
  lc_pouch_state_key_lock *lock;
  int pthread_rc;

  if (lc_pouch_single_writer_enabled(pouch)) {
    lock = lc_pouch_state_projection_lock_current(pouch, ns);
    if (lock == NULL || lock->process_mutex != NULL) {
      return LC_OK;
    }
    return lc_pouch_state_process_namespace_mutex_lock(
        pouch, ns, &lock->process_mutex, error);
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to restore pouch mutation state",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

typedef struct lc_pouch_state_cache_guard {
  lc_pouch_state_process_namespace_mutex *namespace_mutex;
  lc_pouch_state_shared_mutation_guard *shared_mutation;
  int shared_projection_locked;
} lc_pouch_state_cache_guard;

/* Cache records are mutable projection state. Exclusive Pouch owns one
 * namespace projection, while shared Pouch deliberately serializes every
 * projection reader and writer through state_mutation_mutex. Keeping this at
 * the cache boundary prevents a shared reader from traversing records while a
 * local append worker rebuilds or extends the cache. */
static int lc_pouch_state_cache_guard_lock(lc_pouch *pouch, const char *ns,
                                           lc_pouch_state_cache_guard *guard,
                                           lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || guard == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache guard requires namespace", NULL, NULL,
                        "pouch");
  }
  memset(guard, 0, sizeof(*guard));
  /* Namespace callbacks and exact-key mutations already own the projection
   * authority for this thread. The shared projection mutex is non-recursive,
   * so a nested lease validation/read must reuse that authority rather than
   * attempting to lock it again. */
  if (lc_pouch_state_namespace_lock_is_held(pouch, ns) ||
      lc_pouch_state_projection_lock_current(pouch, ns) != NULL) {
    return LC_OK;
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    return lc_pouch_state_process_namespace_mutex_lock(
        pouch, ns, &guard->namespace_mutex, error);
  }
  /* In shared mode every cache refresh shares the root durable authority with
   * an append, eliminating a reader/writer filesystem lock cycle. */
  if (!lc_pouch_state_shared_mutation_is_held(pouch) &&
      lc_pouch_state_shared_mutation_acquire(pouch, &guard->shared_mutation,
                                             error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
  if (pthread_rc != 0) {
    lc_pouch_state_shared_mutation_release(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch shared projection",
                        strerror(pthread_rc), NULL, "pouch");
  }
  guard->shared_projection_locked = 1;
  return LC_OK;
}

static void
lc_pouch_state_cache_guard_unlock(lc_pouch *pouch,
                                  lc_pouch_state_cache_guard *guard) {
  if (guard == NULL) {
    return;
  }
  if (guard->shared_projection_locked) {
    guard->shared_projection_locked = 0;
    (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  lc_pouch_state_shared_mutation_release(&guard->shared_mutation);
  lc_pouch_state_process_namespace_mutex_unlock(&guard->namespace_mutex);
}

static int lc_pouch_state_process_namespace_guard_lock(
    lc_pouch *pouch, const char *ns, int exclusive,
    lc_pouch_state_process_namespace_guard **out, lc_error *error) {
  lc_pouch_state_process_namespace_guard *entry;
  lc_pouch_state_process_namespace_guard *created;
  char *identity;
  int pthread_rc;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch maintenance guard requires namespace and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  identity = NULL;
  rc = lc_pouch_state_process_mutex_identity(pouch, ns, &identity, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
  if (pthread_rc != 0) {
    lc_free_with_allocator(NULL, identity);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch maintenance guard registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  for (entry = lc_pouch_state_process_guards; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->identity, identity) == 0) {
      break;
    }
  }
  if (entry == NULL) {
    created =
        (lc_pouch_state_process_namespace_guard *)lc_calloc_with_allocator(
            NULL, 1U, sizeof(*created));
    if (created == NULL) {
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch maintenance guard", NULL,
                          NULL, "pouch");
    }
    pthread_rc = pthread_rwlock_init(&created->lock, NULL);
    if (pthread_rc != 0) {
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch maintenance guard",
                          strerror(pthread_rc), NULL, "pouch");
    }
    pthread_rc = pthread_mutex_init(&created->writer_mutex, NULL);
    if (pthread_rc != 0) {
      (void)pthread_rwlock_destroy(&created->lock);
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch maintenance writer state",
                          strerror(pthread_rc), NULL, "pouch");
    }
    created->identity = identity;
    created->next = lc_pouch_state_process_guards;
    lc_pouch_state_process_guards = created;
    entry = created;
    identity = NULL;
  }
  entry->refcount += 1UL;
  pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
  lc_free_with_allocator(NULL, identity);
  if (exclusive) {
    pthread_rc = pthread_mutex_lock(&entry->writer_mutex);
    if (pthread_rc != 0) {
      pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
      entry->refcount -= 1UL;
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch maintenance writer state",
                          strerror(pthread_rc), NULL, "pouch");
    }
    if (entry->writer_active && pthread_equal(entry->writer, pthread_self())) {
      entry->writer_depth += 1UL;
      (void)pthread_mutex_unlock(&entry->writer_mutex);
      *out = entry;
      return LC_OK;
    }
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  pthread_rc = exclusive ? pthread_rwlock_wrlock(&entry->lock)
                         : pthread_rwlock_rdlock(&entry->lock);
  if (pthread_rc != 0) {
    pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
    entry->refcount -= 1UL;
    pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch maintenance guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  if (exclusive) {
    pthread_rc = pthread_mutex_lock(&entry->writer_mutex);
    if (pthread_rc != 0) {
      (void)pthread_rwlock_unlock(&entry->lock);
      pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
      entry->refcount -= 1UL;
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to record pouch maintenance writer",
                          strerror(pthread_rc), NULL, "pouch");
    }
    entry->writer = pthread_self();
    entry->writer_active = 1;
    entry->writer_depth = 1UL;
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  *out = entry;
  return LC_OK;
}

static void lc_pouch_state_process_namespace_guard_unlock(
    lc_pouch_state_process_namespace_guard **guard) {
  lc_pouch_state_process_namespace_guard *entry;
  int unlock_rwlock;

  if (guard == NULL || *guard == NULL) {
    return;
  }
  entry = *guard;
  *guard = NULL;
  unlock_rwlock = 1;
  if (pthread_mutex_lock(&entry->writer_mutex) == 0) {
    if (entry->writer_active && pthread_equal(entry->writer, pthread_self())) {
      if (entry->writer_depth > 0UL) {
        entry->writer_depth -= 1UL;
      }
      if (entry->writer_depth == 0UL) {
        entry->writer_active = 0;
      } else {
        unlock_rwlock = 0;
      }
    }
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  if (unlock_rwlock) {
    (void)pthread_rwlock_unlock(&entry->lock);
  }
  pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
  if (entry->refcount > 0UL) {
    entry->refcount -= 1UL;
  }
  pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
}

/* Shared Pouch has no resident cross-process logstore writer. Serialize its
 * complete durable mutation at the Pouch root, including `.txns`, so XA
 * participant work cannot invert a data-namespace lock with a transaction
 * namespace lock. This lock is intentionally absent from single-writer mode. */
static int lc_pouch_state_shared_mutation_acquire(
    lc_pouch *pouch, lc_pouch_state_shared_mutation_guard **out,
    lc_error *error) {
  lc_pouch_state_shared_mutation_guard *entry;
  lc_pouch_state_shared_mutation_guard *created;
  char *identity;
  char *lock_path;
  struct flock fl;
  int pthread_rc;
  int rc;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch shared mutation lock requires output", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  pthread_once(&lc_pouch_state_shared_mutation_key_once,
               lc_pouch_state_shared_mutation_key_init);
  if (lc_pouch_state_shared_mutation_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch shared mutation tracking",
                        strerror(lc_pouch_state_shared_mutation_key_status),
                        NULL, "pouch");
  }
  rc = lc_pouch_state_process_mutex_identity(pouch, ".pouch-shared-mutation",
                                             &identity, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
  if (pthread_rc != 0) {
    lc_free_with_allocator(NULL, identity);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch shared mutation registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  for (entry = lc_pouch_state_shared_mutation_guards; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->identity, identity) == 0) {
      break;
    }
  }
  if (entry == NULL) {
    created = (lc_pouch_state_shared_mutation_guard *)lc_calloc_with_allocator(
        NULL, 1U, sizeof(*created));
    if (created == NULL) {
      pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
      lc_free_with_allocator(NULL, identity);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch shared mutation guard",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_mutex_init_recursive(&created->mutex, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return rc;
    }
    created->identity = identity;
    created->root_device = pouch->root_device;
    created->root_inode = pouch->root_inode;
    created->fd = -1;
    created->next = lc_pouch_state_shared_mutation_guards;
    lc_pouch_state_shared_mutation_guards = created;
    entry = created;
    identity = NULL;
  }
  ++entry->refcount;
  pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
  lc_free_with_allocator(NULL, identity);
  pthread_rc = pthread_mutex_lock(&entry->mutex);
  if (pthread_rc != 0) {
    pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
    --entry->refcount;
    pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch shared mutation guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  if (entry->depth == 0UL) {
    lock_path = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                                   "pouch.mutation.lock");
    if (lock_path == NULL) {
      pthread_mutex_unlock(&entry->mutex);
      pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
      --entry->refcount;
      pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch shared mutation path", NULL,
                          NULL, "pouch");
    }
    entry->fd = open(lock_path, O_CREAT | O_RDWR, 0666);
    lc_free_with_allocator(&pouch->allocator, lock_path);
    if (entry->fd < 0) {
      pthread_mutex_unlock(&entry->mutex);
      pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
      --entry->refcount;
      pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to open pouch shared mutation lock",
                          strerror(errno), NULL, "pouch");
    }
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    while (fcntl(entry->fd, F_SETLKW, &fl) != 0) {
      if (errno == EINTR) {
        continue;
      }
      close(entry->fd);
      entry->fd = -1;
      pthread_mutex_unlock(&entry->mutex);
      pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
      --entry->refcount;
      pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch shared mutation file",
                          strerror(errno), NULL, "pouch");
    }
  }
  entry->depth += 1UL;
  (void)pthread_setspecific(lc_pouch_state_shared_mutation_key, entry);
  *out = entry;
  return LC_OK;
}

static int lc_pouch_state_shared_mutation_is_held(const lc_pouch *pouch) {
  lc_pouch_state_shared_mutation_guard *entry;

  if (pouch == NULL) {
    return 0;
  }
  pthread_once(&lc_pouch_state_shared_mutation_key_once,
               lc_pouch_state_shared_mutation_key_init);
  entry = lc_pouch_state_shared_mutation_key_status == 0
              ? (lc_pouch_state_shared_mutation_guard *)pthread_getspecific(
                    lc_pouch_state_shared_mutation_key)
              : NULL;
  return entry != NULL && entry->root_device == pouch->root_device &&
         entry->root_inode == pouch->root_inode;
}

static void lc_pouch_state_shared_mutation_release(
    lc_pouch_state_shared_mutation_guard **guard) {
  lc_pouch_state_shared_mutation_guard *entry;

  if (guard == NULL || *guard == NULL) {
    return;
  }
  entry = *guard;
  *guard = NULL;
  if (entry->depth > 0UL) {
    entry->depth -= 1UL;
  }
  if (entry->depth == 0UL && entry->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(entry->fd, F_SETLK, &fl);
    (void)close(entry->fd);
    entry->fd = -1;
    pthread_once(&lc_pouch_state_shared_mutation_key_once,
                 lc_pouch_state_shared_mutation_key_init);
    if (lc_pouch_state_shared_mutation_key_status == 0) {
      (void)pthread_setspecific(lc_pouch_state_shared_mutation_key, NULL);
    }
  }
  (void)pthread_mutex_unlock(&entry->mutex);
  pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
  --entry->refcount;
  pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
}

int lc_pouch_state_shared_mutation_enter(
    lc_pouch *pouch, lc_pouch_state_shared_mutation_guard **out,
    lc_error *error) {
  if (out != NULL) {
    *out = NULL;
  }
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch shared mutation lock requires output", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    return LC_OK;
  }
  return lc_pouch_state_shared_mutation_acquire(pouch, out, error);
}

void lc_pouch_state_shared_mutation_leave(
    lc_pouch_state_shared_mutation_guard **guard) {
  lc_pouch_state_shared_mutation_release(guard);
}

typedef struct lc_pouch_state_payload_span {
  char *container_leaf;
  uint64_t record_offset;
  uint64_t payload_offset;
  uint64_t payload_length;
  unsigned long payload_crc;
  int present;
} lc_pouch_state_payload_span;

struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  char *decision;
  lc_pouch_generation index_seq;
  /* HIGH_WATER records persist the last public-query mutation independently
   * from their state sequence so delete-only snapshots retain freshness. */
  lc_pouch_generation query_index_seq;
  /* Snapshot records carry the per-key value explicitly. Live records derive
   * it from their predecessor so ordinary appends retain their compact format.
   */
  int has_query_index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  int object_link;
  int seen;
  int found;
  int control;
  int has_record_ref;
  unsigned char record_type;
};

static void lc_pouch_state_precondition_view_from_entry(
    const lc_pouch_state_entry *entry, lc_pouch_state_precondition_view *out) {
  memset(out, 0, sizeof(*out));
  if (entry == NULL || !entry->found) {
    return;
  }
  out->found = 1;
  out->version = entry->version;
  out->retention = entry->retention;
  out->metadata = entry->metadata;
  out->metadata_length = entry->metadata_length;
  out->has_query_hidden = entry->has_query_hidden;
  out->query_hidden = entry->query_hidden;
  out->has_body = entry->payload_span.present;
}

static void lc_pouch_state_metadata_view_from_read_result(
    const lc_pouch_state_read_result *read_result,
    lc_pouch_state_metadata_view *out) {
  memset(out, 0, sizeof(*out));
  if (read_result == NULL || !read_result->found) {
    return;
  }
  out->found = 1;
  out->etag = read_result->etag;
  out->content_type = read_result->content_type;
  out->version = read_result->version;
  out->retention = read_result->retention;
  out->metadata = read_result->metadata;
  out->metadata_length = read_result->metadata_length;
  out->has_query_hidden = read_result->has_query_hidden;
  out->query_hidden = read_result->query_hidden;
  out->has_body = read_result->has_body;
  out->is_delete_marker = read_result->staged_delete_marker;
}

static void
lc_pouch_state_metadata_view_from_entry(const lc_pouch_state_entry *entry,
                                        lc_pouch_state_metadata_view *out) {
  memset(out, 0, sizeof(*out));
  if (entry == NULL || !entry->found) {
    return;
  }
  out->found = 1;
  out->etag = entry->etag;
  out->content_type = entry->content_type;
  out->version = entry->version;
  out->retention = entry->retention;
  out->metadata = entry->metadata;
  out->metadata_length = entry->metadata_length;
  out->has_query_hidden = entry->has_query_hidden;
  out->query_hidden = entry->query_hidden;
  out->has_body = entry->payload_span.present;
  out->is_delete_marker = entry->staged_delete_marker;
}

typedef enum lc_pouch_state_record_type {
  LC_POUCH_STATE_RECORD_STATE_PUT = 1,
  LC_POUCH_STATE_RECORD_STATE_DELETE = 2,
  LC_POUCH_STATE_RECORD_STATE_LINK = 3,
  LC_POUCH_STATE_RECORD_STATE_META = 4,
  LC_POUCH_STATE_RECORD_DECISION = 5,
  LC_POUCH_STATE_RECORD_HIGH_WATER = 6,
  LC_POUCH_STATE_RECORD_OBJECT_PUT = 7,
  LC_POUCH_STATE_RECORD_OBJECT_DELETE = 8
} lc_pouch_state_record_type;

static int lc_pouch_state_record_type_is_object(unsigned char record_type) {
  return record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
         record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE;
}

static int lc_pouch_state_key_is_staged(const char *key) {
  return key != NULL &&
         (strncmp(key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
          strstr(key, "/.staging/") != NULL);
}

static unsigned char lc_pouch_state_payload_class(unsigned char record_type,
                                                  int object_link) {
  return lc_pouch_state_record_type_is_object(record_type) || object_link ? 2U
                                                                          : 1U;
}

typedef struct lc_pouch_state_binary_append_item {
  unsigned char record_type;
  const void *key;
  size_t key_len;
  unsigned char *meta;
  size_t meta_len;
  /* The durable query watermark is either inherited from the current public
   * projection or assigned from this batch's reserved state sequence. */
  lc_pouch_generation query_index_seq;
  int query_index_from_index;
} lc_pouch_state_binary_append_item;

typedef struct lc_pouch_state_metadata_append_request {
  const char *ns;
  const char *key;
  lc_pouch_state_entry *current;
  lc_pouch_state_write_options options;
  lc_pouch_generation version;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  lc_pouch_state_write_result *out;
  lc_error *error;
  int rc;
  int done;
  pthread_cond_t cond;
  struct lc_pouch_state_metadata_append_request *next;
} lc_pouch_state_metadata_append_request;

typedef struct lc_pouch_state_body_append_request {
  const char *ns;
  const char *key;
  lc_source *body;
  const lc_pouch_state_write_options *options;
  lc_pouch_state_write_result *out;
  lc_error *error;
  lc_pouch_state_commit_group *commit_group;
  int rc;
  int done;
  pthread_cond_t cond;
  struct lc_pouch_state_body_append_request *next;
} lc_pouch_state_body_append_request;

struct lc_pouch_state_metadata_append_batcher {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pthread_t thread;
  lc_pouch *pouch;
  char *ns;
  lc_pouch_state_metadata_append_request *head;
  lc_pouch_state_metadata_append_request *tail;
  int stop;
  struct lc_pouch_state_metadata_append_batcher *next;
};

typedef struct lc_pouch_state_body_append_batcher {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pthread_t thread;
  lc_pouch *pouch;
  char *ns;
  lc_pouch_state_body_append_request *head;
  lc_pouch_state_body_append_request *tail;
  int stop;
  struct lc_pouch_state_body_append_batcher *next;
} lc_pouch_state_body_append_batcher;

typedef struct lc_pouch_state_body_cache_entry {
  unsigned char *bytes;
  size_t length;
  lc_pouch_generation version;
  pthread_mutex_t ref_mutex;
  unsigned long refcount;
  int retired;
  int complete;
  int ref_mutex_initialized;
} lc_pouch_state_body_cache_entry;

struct lc_pouch_source_cache_entry {
  char *path;
  int fd;
  unsigned long last_used;
  struct lc_pouch_source_cache_entry *next;
};

typedef struct lc_pouch_state_decision {
  char *staged_key;
  char *etag;
  char *decision;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  struct lc_pouch_state_decision *next;
} lc_pouch_state_decision;

typedef struct lc_pouch_state_cache_record {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  /* Last sequence whose record changed query candidates for this key. Lease
   * metadata may advance index_seq without changing the indexed body. */
  lc_pouch_generation query_index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  lc_pouch_state_body_cache_entry *body_cache;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  int found;
  int has_record_ref;
  unsigned char record_type;
  struct lc_pouch_state_cache_record *next;
  struct lc_pouch_state_cache_record *bucket_next;
} lc_pouch_state_cache_record;

static lc_pouch_namespace_logstore *
lc_pouch_namespace_logstore_find(lc_pouch *pouch, const char *ns, int create,
                                 lc_error *error);
static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_namespace_logstore *ns,
                                 const char *key);
static int lc_pouch_state_compaction_record_ref_matches(
    const lc_pouch_state_cache_record *record, const char *container,
    uint64_t offset);
static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *binding_context, const char *payload_span_path,
    uint64_t payload_offset, uint64_t payload_length,
    const lc_pouch_state_entry *current, int copy_cached_body, lc_source **out,
    lc_error *error);

static int lc_pouch_state_payload_container_is_valid(const char *leaf);
static void
lc_pouch_state_payload_span_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_payload_span *span);
static int lc_pouch_state_payload_span_copy(
    const lc_allocator *allocator, const lc_pouch_state_payload_span *src,
    lc_pouch_state_payload_span *dst, lc_error *error);
static int lc_pouch_state_payload_span_set(
    const lc_allocator *allocator, lc_pouch_state_payload_span *span,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc, lc_error *error);
static char *lc_pouch_state_payload_span_path(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_state_payload_span *span, lc_error *error);
static int lc_pouch_state_source_from_span(
    lc_pouch *pouch, const char *crypto_context, const char *binding_context,
    const char *path, uint64_t payload_offset, uint64_t payload_length,
    const char *descriptor, lc_source **out, lc_error *error);

typedef struct lc_pouch_state_visit_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int has_payload;
  int staged_delete_marker;
  int found;
  unsigned char record_type;
} lc_pouch_state_visit_snapshot;

typedef struct lc_pouch_state_read_many_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  char *crypto_context;
  char *binding_context;
  lc_source *body;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  int found;
  unsigned char record_type;
} lc_pouch_state_read_many_snapshot;

typedef struct lc_pouch_state_scan_body_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_state_body_cache_entry *body_cache;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  int found;
  unsigned char record_type;
} lc_pouch_state_scan_body_snapshot;

typedef struct lc_pouch_state_hash_source {
  lc_source pub;
  lc_source *inner;
  EVP_MD_CTX *ctx;
  int failed;
} lc_pouch_state_hash_source;

typedef struct lc_pouch_state_body_cache_source {
  lc_source pub;
  lc_allocator allocator;
  lc_pouch_state_body_cache_entry *entry;
  size_t offset;
} lc_pouch_state_body_cache_source;

typedef struct lc_pouch_state_body_cache_tee_source {
  lc_source pub;
  lc_allocator allocator;
  lc_source *inner;
  lc_pouch_state_body_cache_entry *entry;
  size_t offset;
  int failed;
} lc_pouch_state_body_cache_tee_source;

static int lc_pouch_state_read_text_file(lc_pouch *pouch, const char *path,
                                         char **out, size_t *out_length,
                                         lc_error *error);
static char *lc_pouch_state_child_path(const lc_allocator *allocator,
                                       const char *namespace_path,
                                       const char *child_dir, const char *leaf);
static int
lc_pouch_state_body_cache_entry_init(lc_pouch_state_body_cache_entry *entry,
                                     lc_error *error);
static int
lc_pouch_state_body_cache_entry_retain(lc_pouch_state_body_cache_entry *entry,
                                       lc_error *error);
static int lc_pouch_state_body_cache_entry_retain_for_fill(
    lc_pouch_state_body_cache_entry *entry, lc_error *error);
static void
lc_pouch_state_body_cache_entry_release(const lc_allocator *allocator,
                                        lc_pouch_state_body_cache_entry *entry);
static void
lc_pouch_state_body_cache_entry_retire(const lc_allocator *allocator,
                                       lc_pouch_state_body_cache_entry *entry);
static void
lc_pouch_state_body_cache_entry_destroy(const lc_allocator *allocator,
                                        lc_pouch_state_body_cache_entry *entry);
static int lc_pouch_state_recover_staged_decisions_locked(lc_pouch *pouch,
                                                          const char *ns,
                                                          lc_error *error);
static int
lc_pouch_state_delete_locked(lc_pouch *pouch, const char *ns, const char *key,
                             const lc_pouch_state_write_options *options,
                             lc_pouch_state_write_result *out, lc_error *error);
static int
lc_pouch_state_lock_namespace_for_mutation(lc_pouch *pouch, const char *ns,
                                           lc_pouch_state_namespace_lock *lock,
                                           lc_error *error);
static int lc_pouch_state_visit_internal(lc_pouch *pouch, const char *ns,
                                         const char *prefix,
                                         lc_pouch_state_visit_fn visitor,
                                         void *context, int locked,
                                         lc_error *error);

static size_t lc_pouch_state_hash_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error) {
  lc_pouch_state_hash_source *source;
  size_t got;

  source = (lc_pouch_state_hash_source *)self->impl;
  got = source->inner->read(source->inner, buffer, count, error);
  if (got > 0U &&
      EVP_DigestUpdate(source->ctx, (const unsigned char *)buffer, got) != 1) {
    source->failed = 1;
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to hash pouch state payload", NULL, NULL,
                       "pouch");
    return 0U;
  }
  return got;
}

static int lc_pouch_state_hash_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_state_hash_source *source;
  int rc;

  source = (lc_pouch_state_hash_source *)self->impl;
  if (source->inner->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state hash source cannot reset inner source",
                        NULL, NULL, "pouch");
  }
  rc = source->inner->reset(source->inner, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_DigestInit_ex(source->ctx, EVP_sha256(), NULL) != 1) {
    source->failed = 1;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to reset pouch state payload hash", NULL, NULL,
                        "pouch");
  }
  source->failed = 0;
  return LC_OK;
}

static int
lc_pouch_state_body_cache_entry_ready(lc_pouch_state_body_cache_entry *entry) {
  int ready;

  if (entry == NULL || !entry->ref_mutex_initialized ||
      pthread_mutex_lock(&entry->ref_mutex) != 0) {
    return 0;
  }
  ready = !entry->retired && entry->complete;
  (void)pthread_mutex_unlock(&entry->ref_mutex);
  return ready;
}

static void lc_pouch_state_hash_source_close(lc_source *self) { (void)self; }

static size_t lc_pouch_state_body_cache_source_read(lc_source *self,
                                                    void *buffer, size_t count,
                                                    lc_error *error) {
  lc_pouch_state_body_cache_source *source;
  size_t available;
  size_t take;

  (void)error;
  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL || source->entry == NULL ||
      source->offset >= source->entry->length) {
    return 0U;
  }
  available = source->entry->length - source->offset;
  take = count < available ? count : available;
  memcpy(buffer, source->entry->bytes + source->offset, take);
  source->offset += take;
  return take;
}

static int lc_pouch_state_body_cache_source_reset(lc_source *self,
                                                  lc_error *error) {
  lc_pouch_state_body_cache_source *source;

  (void)error;
  if (self == NULL) {
    return LC_ERR_INVALID;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL) {
    return LC_ERR_INVALID;
  }
  source->offset = 0U;
  return LC_OK;
}

static void lc_pouch_state_body_cache_source_close(lc_source *self) {
  lc_pouch_state_body_cache_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL) {
    return;
  }
  if (source->entry != NULL) {
    lc_pouch_state_body_cache_entry_release(&source->allocator, source->entry);
  }
  lc_free_with_allocator(&source->allocator, source);
}

static int
lc_pouch_state_body_cache_source_open(const lc_allocator *allocator,
                                      lc_pouch_state_body_cache_entry *entry,
                                      lc_source **out, lc_error *error) {
  lc_pouch_state_body_cache_source *source;

  if (entry == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache source requires entry and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  source = (lc_pouch_state_body_cache_source *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch body cache source", NULL,
                        NULL, "pouch");
  }
  if (allocator != NULL) {
    source->allocator = *allocator;
  } else {
    lc_allocator_init(&source->allocator);
  }
  if (lc_pouch_state_body_cache_entry_retain(entry, error) != LC_OK) {
    lc_free_with_allocator(&source->allocator, source);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  source->entry = entry;
  source->pub.read = lc_pouch_state_body_cache_source_read;
  source->pub.reset = lc_pouch_state_body_cache_source_reset;
  source->pub.close = lc_pouch_state_body_cache_source_close;
  source->pub.impl = source;
  *out = &source->pub;
  return LC_OK;
}

static size_t lc_pouch_state_body_cache_tee_source_read(lc_source *self,
                                                        void *buffer,
                                                        size_t count,
                                                        lc_error *error) {
  lc_pouch_state_body_cache_tee_source *source;
  lc_error local_error;
  lc_error *read_error;
  size_t nread;
  int own_local_error;
  int pthread_rc;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_state_body_cache_tee_source *)self->impl;
  if (source == NULL || source->inner == NULL || source->entry == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch body cache tee source is unavailable", NULL, NULL,
                       "pouch");
    return 0U;
  }
  own_local_error = error == NULL;
  if (own_local_error) {
    lc_error_init(&local_error);
    read_error = &local_error;
  } else {
    read_error = error;
  }
  nread = source->inner->read(source->inner, buffer, count, read_error);
  if (read_error->code != LC_OK) {
    source->failed = 1;
  }
  pthread_rc = pthread_mutex_lock(&source->entry->ref_mutex);
  if (pthread_rc != 0) {
    (void)lc_error_set(read_error, LC_ERR_TRANSPORT, 0L,
                       "failed to lock pouch body cache reference",
                       strerror(pthread_rc), NULL, "pouch");
    if (own_local_error) {
      lc_error_cleanup(&local_error);
    }
    return 0U;
  }
  if (nread > 0U) {
    /* A complete entry may already serve independent cache sources.  A reset
     * of this original tee must therefore become pass-through: published
     * cache bytes are immutable for their remaining lifetime. */
    if (!source->entry->retired && !source->entry->complete) {
      if (source->offset > source->entry->length ||
          nread > source->entry->length - source->offset) {
        source->entry->complete = 0;
        source->failed = 1;
        (void)pthread_mutex_unlock(&source->entry->ref_mutex);
        (void)lc_error_set(read_error, LC_ERR_PROTOCOL, 0L,
                           "pouch payload plaintext byte count changed", NULL,
                           NULL, "pouch");
        if (own_local_error) {
          lc_error_cleanup(&local_error);
        }
        return 0U;
      }
      memcpy(source->entry->bytes + source->offset, buffer, nread);
      source->offset += nread;
    }
  } else if (!source->failed && read_error->code == LC_OK &&
             !source->entry->retired && !source->entry->complete) {
    if (source->offset == source->entry->length) {
      source->entry->complete = 1;
    } else {
      source->entry->complete = 0;
      (void)pthread_mutex_unlock(&source->entry->ref_mutex);
      (void)lc_error_set(read_error, LC_ERR_PROTOCOL, 0L,
                         "pouch payload plaintext byte count changed", NULL,
                         NULL, "pouch");
      if (own_local_error) {
        lc_error_cleanup(&local_error);
      }
      return 0U;
    }
  }
  (void)pthread_mutex_unlock(&source->entry->ref_mutex);
  if (own_local_error) {
    lc_error_cleanup(&local_error);
  }
  return nread;
}

static int lc_pouch_state_body_cache_tee_source_reset(lc_source *self,
                                                      lc_error *error) {
  lc_pouch_state_body_cache_tee_source *source;
  int pthread_rc;
  int rc;

  if (self == NULL) {
    return LC_ERR_INVALID;
  }
  source = (lc_pouch_state_body_cache_tee_source *)self->impl;
  if (source == NULL || source->inner == NULL || source->inner->reset == NULL ||
      source->entry == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache tee source is unavailable", NULL,
                        NULL, "pouch");
  }
  rc = source->inner->reset(source->inner, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&source->entry->ref_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch body cache reference",
                        strerror(pthread_rc), NULL, "pouch");
  }
  /* Do not revoke completion here. Once published, independent cache readers
   * access the bytes without this lock and must never race a refill. Before
   * publication `complete` is already false, so resetting still permits the
   * original tee to refill its private in-progress entry. */
  source->offset = 0U;
  source->failed = 0;
  (void)pthread_mutex_unlock(&source->entry->ref_mutex);
  return LC_OK;
}

static void lc_pouch_state_body_cache_tee_source_close(lc_source *self) {
  lc_pouch_state_body_cache_tee_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_state_body_cache_tee_source *)self->impl;
  if (source == NULL) {
    return;
  }
  if (source->inner != NULL) {
    source->inner->close(source->inner);
  }
  if (source->entry != NULL) {
    lc_pouch_state_body_cache_entry_release(&source->allocator, source->entry);
  }
  lc_free_with_allocator(&source->allocator, source);
}

static int lc_pouch_state_body_cache_tee_source_open(
    const lc_allocator *allocator, lc_source *inner,
    lc_pouch_state_body_cache_entry *entry, lc_source **out, lc_error *error) {
  lc_pouch_state_body_cache_tee_source *source;

  if (inner == NULL || entry == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache tee requires source, entry, and "
                        "output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  source = (lc_pouch_state_body_cache_tee_source *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch body cache tee", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    source->allocator = *allocator;
  } else {
    lc_allocator_init(&source->allocator);
  }
  if (lc_pouch_state_body_cache_entry_retain_for_fill(entry, error) != LC_OK) {
    lc_free_with_allocator(&source->allocator, source);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  source->inner = inner;
  source->entry = entry;
  source->pub.read = lc_pouch_state_body_cache_tee_source_read;
  source->pub.reset = lc_pouch_state_body_cache_tee_source_reset;
  source->pub.close = lc_pouch_state_body_cache_tee_source_close;
  source->pub.impl = source;
  *out = &source->pub;
  return LC_OK;
}

static int lc_pouch_state_hash_source_init(lc_pouch_state_hash_source *source,
                                           lc_source *inner, EVP_MD_CTX *ctx,
                                           lc_error *error) {
  if (source == NULL || inner == NULL || ctx == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch state hash source requires source, inner and ctx", NULL, NULL,
        "pouch");
  }
  memset(source, 0, sizeof(*source));
  source->pub.read = lc_pouch_state_hash_source_read;
  source->pub.reset = lc_pouch_state_hash_source_reset;
  source->pub.close = lc_pouch_state_hash_source_close;
  source->pub.impl = source;
  source->inner = inner;
  source->ctx = ctx;
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state payload hash", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static char *lc_pouch_state_hash_final(const lc_allocator *allocator,
                                       EVP_MD_CTX *ctx, lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len;
  char *etag;
  unsigned int i;

  digest_len = 0U;
  etag = NULL;
  if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to finalize pouch state payload hash", NULL,
                       NULL, "pouch");
    return NULL;
  }
  if (digest_len != 32U) {
    OPENSSL_cleanse(digest, sizeof(digest));
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "pouch state payload hash length is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  etag = (char *)lc_alloc_with_allocator(allocator,
                                         ((size_t)digest_len * 2U) + 1U);
  if (etag == NULL) {
    OPENSSL_cleanse(digest, sizeof(digest));
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload etag", NULL,
                       NULL, "pouch");
    return NULL;
  }
  for (i = 0U; i < digest_len; ++i) {
    etag[i * 2U] = hex[(digest[i] >> 4) & 0x0FU];
    etag[(i * 2U) + 1U] = hex[digest[i] & 0x0FU];
  }
  etag[digest_len * 2U] = '\0';
  OPENSSL_cleanse(digest, sizeof(digest));
  return etag;
}

static char *lc_pouch_state_hash_bytes(const lc_allocator *allocator,
                                       const unsigned char *bytes,
                                       size_t length, lc_error *error) {
  EVP_MD_CTX *ctx;
  char *etag;

  if (length > 0U && bytes == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch inline payload hash requires bytes", NULL, NULL,
                       "pouch");
    return NULL;
  }
  ctx = EVP_MD_CTX_new();
  if (ctx == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload hash context",
                       NULL, NULL, "pouch");
    return NULL;
  }
  etag = NULL;
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      (length > 0U && EVP_DigestUpdate(ctx, bytes, length) != 1)) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to hash pouch state payload", NULL, NULL,
                       "pouch");
  } else {
    etag = lc_pouch_state_hash_final(allocator, ctx, error);
  }
  EVP_MD_CTX_free(ctx);
  return etag;
}

static char *lc_pouch_state_empty_etag(const lc_allocator *allocator,
                                       lc_error *error) {
  EVP_MD_CTX *ctx;
  char *etag;

  ctx = EVP_MD_CTX_new();
  if (ctx == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload hash context",
                       NULL, NULL, "pouch");
    return NULL;
  }
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
    EVP_MD_CTX_free(ctx);
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to initialize pouch state empty hash", NULL,
                       NULL, "pouch");
    return NULL;
  }
  etag = lc_pouch_state_hash_final(allocator, ctx, error);
  EVP_MD_CTX_free(ctx);
  return etag;
}

static int
lc_pouch_state_namespace_lock_acquire(lc_pouch *pouch, const char *ns,
                                      lc_pouch_state_namespace_lock *lock,
                                      lc_error *error) {
  char *namespace_path;
  char *lock_path;
  struct flock fl;
  int fd;
  int single_writer;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation lock requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  lock->fd = -1;
  lock->process_mutex = NULL;
  lock->maintenance_guard = NULL;
  lock->shared_mutation = NULL;
  lock->exclusive_append_gate = NULL;
  lock->shared_projection_locked = 0;
  lock->borrowed = 0;
  lock->pouch = NULL;
  lock->ns = NULL;
  lock->previous_namespace_lock = NULL;
  single_writer = lc_pouch_single_writer_enabled(pouch);
  if (lc_pouch_state_namespace_lock_is_held(pouch, ns)) {
    /* Nested recovery/query work reuses the outer namespace callback's
     * physical and projection authority. In shared mode that also avoids
     * recursively acquiring state_mutation_mutex. */
    lock->borrowed = 1;
    if (lc_pouch_state_namespace_lock_track(lock, pouch, ns, error) != LC_OK) {
      lock->borrowed = 0;
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }
  if (!single_writer && lc_pouch_state_shared_mutation_acquire(
                            pouch, &lock->shared_mutation, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (!single_writer) {
    int pthread_rc;

    /* Shared Pouch has no resident distributed logstore writer. Its root
     * mutation guard is therefore the only cross-process durability authority:
     * do not combine it with independent namespace/key/append file locks,
     * which have no global acquisition order. */
    pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
    if (pthread_rc != 0) {
      lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch shared projection",
                          strerror(pthread_rc), NULL, "pouch");
    }
    lock->shared_projection_locked = 1;
    if (lc_pouch_state_namespace_lock_track(lock, pouch, ns, error) != LC_OK) {
      lock->shared_projection_locked = 0;
      (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
      lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }
  if (lc_pouch_state_process_namespace_guard_lock(
          pouch, ns, 1, &lock->maintenance_guard, error) != LC_OK) {
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  /* Namespace callbacks retain projection authority across multiple writes.
   * Claim the physical gate before that projection so a metadata worker cannot
   * hold the gate while waiting for this callback's namespace mutex. */
  if (single_writer) {
    if (lc_pouch_state_exclusive_append_gate_lock(
            pouch, ns, &lock->exclusive_append_gate, error) != LC_OK) {
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (lc_pouch_state_process_namespace_mutex_lock(
          pouch, ns, &lock->process_mutex, error) != LC_OK) {
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (single_writer) {
    if (lc_pouch_state_namespace_lock_track(lock, pouch, ns, error) != LC_OK) {
      lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
      lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }

  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  lock_path =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "write.lock")
          : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (lock_path == NULL) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutation lock path", NULL,
                        NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch mutation lock", strerror(errno),
                        NULL, NULL);
  }

  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch mutation file", strerror(errno),
                        NULL, NULL);
  }
  lock->fd = fd;
  if (lc_pouch_state_namespace_lock_track(lock, pouch, ns, error) != LC_OK) {
    if (lock->shared_projection_locked) {
      lock->shared_projection_locked = 0;
      (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
    }
    if (lock->fd >= 0) {
      struct flock unlock_fl;

      memset(&unlock_fl, 0, sizeof(unlock_fl));
      unlock_fl.l_type = F_UNLCK;
      unlock_fl.l_whence = SEEK_SET;
      (void)fcntl(lock->fd, F_SETLK, &unlock_fl);
      (void)close(lock->fd);
      lock->fd = -1;
    }
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static void
lc_pouch_state_namespace_lock_release(lc_pouch_state_namespace_lock *lock) {
  if (lock == NULL) {
    return;
  }
  if (lock->borrowed) {
    lock->borrowed = 0;
    lc_pouch_state_namespace_lock_untrack(lock);
    return;
  }
  if (lock->shared_projection_locked && lock->pouch != NULL) {
    lock->shared_projection_locked = 0;
    (void)pthread_mutex_unlock(&lock->pouch->state_mutation_mutex);
  }
  lc_pouch_state_namespace_lock_untrack(lock);
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    close(lock->fd);
    lock->fd = -1;
  }
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
  lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
  lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
  lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
}

/* Every exclusive Pouch owns stable per-namespace physical append gates for
 * its full lifetime. Shared roots instead use one root mutation authority.
 * This gives the exclusive path the same split as Go disk:
 * callers may prepare a value concurrently, but only one finalized byte range
 * is appended at a time, without filesystem discovery on a healthy mutation.
 */
static int lc_pouch_state_append_lock_acquire(lc_pouch *pouch, const char *ns,
                                              lc_pouch_state_append_lock *lock,
                                              lc_error *error) {
  int rc;
  int single_writer;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch append lock requires namespace", NULL, NULL,
                        "pouch");
  }
  lock->fd = -1;
  lock->process_mutex = NULL;
  lock->exclusive_gate = NULL;
  lock->pouch = NULL;
  lock->ns = NULL;
  lock->previous_append_lock = NULL;
  lock->borrowed = 0;
  single_writer = lc_pouch_single_writer_enabled(pouch);
  if (lc_pouch_state_append_lock_current(pouch, ns) != NULL) {
    lock->borrowed = 1;
    return LC_OK;
  }
  if (single_writer) {
    rc = lc_pouch_state_exclusive_append_gate_lock(
        pouch, ns, &lock->exclusive_gate, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_append_lock_track(lock, pouch, ns, error);
    }
    if (rc != LC_OK) {
      lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_gate);
    }
    return rc;
  }
  if (lc_pouch_state_shared_mutation_is_held(pouch)) {
    /* The shared root guard serializes every durable append.  Keeping only
     * that lock avoids an append-vs-root lock-order cycle across processes. */
    lock->borrowed = 1;
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "shared pouch append requires root mutation authority",
                      NULL, NULL, "pouch");
}

static void
lc_pouch_state_append_lock_release(lc_pouch_state_append_lock *lock) {
  if (lock == NULL) {
    return;
  }
  if (lock->borrowed) {
    lock->borrowed = 0;
    return;
  }
  lc_pouch_state_append_lock_untrack(lock);
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    (void)close(lock->fd);
    lock->fd = -1;
  }
  lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_gate);
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
}

/* Normal exact-key mutations arrive with the projection mutex held. Take the
 * physical appender first, then restore the projection mutex before reading or
 * publishing cache state. This ordering lets a streaming owner drop only the
 * projection mutex while retaining its exact-key and append ownership; other
 * writers prepare or wait at the append gate without deadlocking publication.
 * Namespace callbacks already own stronger namespace authority and never own
 * the projection mutex through this helper. */
static int lc_pouch_state_append_lock_enter_after_mutation(
    lc_pouch *pouch, const char *ns, lc_pouch_state_append_lock *lock,
    lc_error *error) {
  int namespace_locked;
  int rc;
  int restore_rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch append transition requires namespace", NULL,
                        NULL, "pouch");
  }
  namespace_locked = lc_pouch_state_namespace_lock_is_held(pouch, ns);
  if (!namespace_locked) {
    lc_pouch_state_projection_mutex_unlock(pouch, ns);
  }
  rc = lc_pouch_state_append_lock_acquire(pouch, ns, lock, error);
  if (!namespace_locked) {
    restore_rc = lc_pouch_state_projection_mutex_lock(pouch, ns, error);
    if (restore_rc != LC_OK && rc == LC_OK) {
      lc_pouch_state_append_lock_release(lock);
      return restore_rc;
    }
  }
  return rc;
}

static int lc_pouch_state_key_lock_acquire(lc_pouch *pouch, const char *ns,
                                           const char *key,
                                           lc_pouch_state_key_lock *lock,
                                           lc_error *error) {
  uint64_t hash;
  const unsigned char *cursor;
  const char *canonical_key;
  size_t key_len;
  int rc;
  int pthread_rc;
  int single_writer;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key lock requires namespace and key", NULL, NULL,
                        "pouch");
  }
  lock->fd = -1;
  lock->maintenance_fd = -1;
  lock->process_mutex = NULL;
  lock->maintenance_guard = NULL;
  lock->shared_mutation = NULL;
  lock->exclusive_key_mutex = NULL;
  lock->pouch = pouch;
  lock->ns = ns;
  lock->projection_uses_namespace_mutex = 0;
  lock->writer_mode_operation_active = 0;
  lock->previous_projection_lock = NULL;
  single_writer = lc_pouch_single_writer_enabled(pouch);
  if (lc_pouch_state_namespace_lock_is_held(pouch, ns)) {
    /* A namespace callback already owns the active projection: its namespace
     * mutex in exclusive mode or state_mutation_mutex in shared mode. Mark
     * this nested key lock as borrowed so it neither relocks nor releases the
     * outer projection authority. */
    lock->projection_uses_namespace_mutex = 1;
    return LC_OK;
  }
  canonical_key = strstr(key, "/.staging/");
  key_len = canonical_key != NULL ? (size_t)(canonical_key - key) : strlen(key);
  if (key_len == 0U) {
    canonical_key = key;
    key_len = strlen(key);
  } else {
    canonical_key = key;
  }
  if (single_writer) {
    hash = ((uint64_t)0xcbf29ce4UL << 32U) | (uint64_t)0x84222325UL;
    for (cursor = (const unsigned char *)ns; *cursor != '\0'; ++cursor) {
      hash ^= (uint64_t)*cursor;
      hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    }
    hash ^= (uint64_t)'\n';
    hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    for (cursor = (const unsigned char *)canonical_key;
         (size_t)(cursor - (const unsigned char *)canonical_key) < key_len;
         ++cursor) {
      hash ^= (uint64_t)*cursor;
      hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    }
    if (pouch->exclusive_key_mutex_count == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch exclusive key mutexes are unavailable", NULL,
                          NULL, "pouch");
    }
    lock->exclusive_key_mutex = &pouch->exclusive_key_mutexes[(
        size_t)(hash % pouch->exclusive_key_mutex_count)];
    pthread_rc = pthread_mutex_lock(lock->exclusive_key_mutex);
    if (pthread_rc != 0) {
      lock->exclusive_key_mutex = NULL;
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch exclusive key mutex",
                          strerror(pthread_rc), NULL, "pouch");
    }
    /*
     * Maintenance takes its write guard before namespace projection.  Keep
     * regular mutations in that order as well, otherwise recovery can hold
     * the write guard while waiting for a projection mutex held by a writer
     * waiting for the read guard.
     */
    rc = lc_pouch_state_process_namespace_guard_lock(
        pouch, ns, 0, &lock->maintenance_guard, error);
    if (rc != LC_OK) {
      (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
      lock->exclusive_key_mutex = NULL;
      return rc;
    }
    rc = lc_pouch_state_process_namespace_mutex_lock(
        pouch, ns, &lock->process_mutex, error);
    if (rc != LC_OK) {
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
      lock->exclusive_key_mutex = NULL;
    }
    if (rc == LC_OK) {
      lock->projection_uses_namespace_mutex = 1;
    }
    return rc;
  }
  if (lc_pouch_state_shared_mutation_acquire(pouch, &lock->shared_mutation,
                                             error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  /* The shared root guard serializes all mutations across processes. Per-key
   * file locks would reintroduce an incomparable root/key/append order. */
  return LC_OK;
}

static void lc_pouch_state_key_lock_release(lc_pouch_state_key_lock *lock) {
  if (lock == NULL) {
    return;
  }
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    (void)close(lock->fd);
    lock->fd = -1;
  }
  if (lock->maintenance_fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->maintenance_fd, F_SETLK, &fl);
    (void)close(lock->maintenance_fd);
    lock->maintenance_fd = -1;
  }
  lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
  lc_pouch_state_shared_mutation_release(&lock->shared_mutation);
  if (lock->exclusive_key_mutex != NULL) {
    (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
    lock->exclusive_key_mutex = NULL;
  }
  lock->pouch = NULL;
  lock->ns = NULL;
  lock->projection_uses_namespace_mutex = 0;
}

static int lc_pouch_state_key_mutation_begin(lc_pouch *pouch, const char *ns,
                                             const char *key,
                                             lc_pouch_state_key_lock *lock,
                                             lc_error *error) {
  int namespace_locked;
  int pthread_rc;
  int rc;
  int writer_mode_operation_active;

  if (pouch == NULL || !pouch->state_mutation_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation requires an open pouch", NULL, NULL,
                        "pouch");
  }
  /* Recovery has its own mutation authority. Complete it first so this
   * operation can pin one writer mode through append and durable completion. */
  namespace_locked = lc_pouch_state_namespace_lock_is_held(pouch, ns);
  rc = namespace_locked
           ? LC_OK
           : lc_pouch_state_recover_staged_decisions(pouch, ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  writer_mode_operation_active = 0;
  if (!namespace_locked) {
    rc = lc_pouch_writer_mode_operation_begin(pouch, error);
    if (rc != LC_OK) {
      return rc;
    }
    writer_mode_operation_active = 1;
  }
  /* Exact-key ownership is independent of the resident projection. Take it
   * first so a conflicting key/file lock never occupies the short projection
   * mutex; this also gives maintenance a clear key/namespace barrier. */
  rc = lc_pouch_state_key_lock_acquire(pouch, ns, key, lock, error);
  if (rc != LC_OK) {
    if (writer_mode_operation_active) {
      lc_pouch_writer_mode_operation_end(pouch);
    }
    return rc;
  }
  lock->writer_mode_operation_active = writer_mode_operation_active;
  rc = lc_pouch_state_projection_lock_track(lock, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_lock_release(lock);
    if (writer_mode_operation_active) {
      lc_pouch_writer_mode_operation_end(pouch);
    }
    return rc;
  }
  if (lock->projection_uses_namespace_mutex) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
  if (pthread_rc != 0) {
    lc_pouch_state_projection_lock_untrack(lock);
    lc_pouch_state_key_lock_release(lock);
    if (writer_mode_operation_active) {
      lc_pouch_writer_mode_operation_end(pouch);
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch mutation state",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_state_key_mutation_end(lc_pouch *pouch,
                                            lc_pouch_state_key_lock *lock) {
  int writer_mode_operation_active;

  writer_mode_operation_active =
      lock != NULL && lock->writer_mode_operation_active;
  if (pouch != NULL && lock != NULL && !lock->projection_uses_namespace_mutex &&
      pouch->state_mutation_mutex_initialized) {
    pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  lc_pouch_state_projection_lock_untrack(lock);
  lc_pouch_state_key_lock_release(lock);
  if (writer_mode_operation_active) {
    lc_pouch_writer_mode_operation_end(pouch);
  }
}

/* Keep the per-key lock through durable completion, but let independent
 * namespace projections proceed while this request waits for its root-scoped
 * fsync group. Shared mode retains its global projection mutex. */
static int lc_pouch_state_finish_commit_group_after_mutation(
    lc_pouch *pouch, lc_pouch_state_key_lock *lock,
    lc_pouch_state_commit_group *group, int owned, int rc, lc_error *error) {
  int projection_released;
  int writer_mode_operation_active;

  writer_mode_operation_active =
      lock != NULL && lock->writer_mode_operation_active;
  projection_released = 0;
  if (pouch != NULL && lock != NULL && lock->projection_uses_namespace_mutex &&
      lock->process_mutex != NULL) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    projection_released = 1;
  } else if (pouch != NULL && lock != NULL &&
             !lock->projection_uses_namespace_mutex &&
             pouch->state_mutation_mutex_initialized) {
    pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  rc = lc_pouch_state_finish_commit_group_common(group, owned, rc, error);
  if (rc != LC_OK && pouch != NULL && lock != NULL) {
    if (lock->projection_uses_namespace_mutex) {
      if (projection_released) {
        if (lc_pouch_state_process_namespace_mutex_lock(
                pouch, lock->ns, &lock->process_mutex, NULL) != LC_OK) {
          lc_pouch_state_projection_lock_untrack(lock);
          lc_pouch_state_key_lock_release(lock);
          if (writer_mode_operation_active) {
            lc_pouch_writer_mode_operation_end(pouch);
          }
          return rc;
        }
      }
      lc_pouch_state_cache_invalidate_namespace(pouch, lock->ns);
    } else if (pouch->state_mutation_mutex_initialized &&
               pthread_mutex_lock(&pouch->state_mutation_mutex) == 0) {
      lc_pouch_state_cache_invalidate_namespace(pouch, lock->ns);
      pthread_mutex_unlock(&pouch->state_mutation_mutex);
    }
  }
  lc_pouch_state_projection_lock_untrack(lock);
  lc_pouch_state_key_lock_release(lock);
  if (writer_mode_operation_active) {
    lc_pouch_writer_mode_operation_end(pouch);
  }
  return rc;
}

int lc_pouch_state_with_namespace_lock(lc_pouch *pouch, const char *ns,
                                       lc_pouch_state_precondition_fn callback,
                                       void *context, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_namespace_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (callback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace lock requires callback", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, ns, &lock, error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = callback(context, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  if (rc != LC_OK) {
    lc_pouch_state_cache_invalidate_namespace(pouch, ns);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  lc_pouch_writer_mode_operation_end(pouch);
  return rc;
}

int lc_pouch_state_with_key_lock(lc_pouch *pouch, const char *ns,
                                 const char *key,
                                 lc_pouch_state_precondition_fn callback,
                                 void *context, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  if (callback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key lock requires callback", NULL, NULL, NULL);
  }
  commit_group = NULL;
  owns_commit_group = 0;
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc == LC_OK) {
    rc = callback(context, error);
    rc = lc_pouch_state_finish_commit_group_after_mutation(
        pouch, &lock, commit_group, owns_commit_group, rc, error);
  } else {
    lc_pouch_state_key_mutation_end(pouch, &lock);
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 0);
  }
  return rc;
}

static int
lc_pouch_state_lock_namespace_for_mutation(lc_pouch *pouch, const char *ns,
                                           lc_pouch_state_namespace_lock *lock,
                                           lc_error *error) {
  int rc;

  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_namespace_lock_acquire(pouch, ns, lock, error);
}

static int lc_pouch_state_ensure_namespace_locked(lc_pouch *pouch,
                                                  const char *ns,
                                                  lc_error *error) {
  int rc;

  rc =
      lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path, ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_recover_staged_decisions_locked(pouch, ns, error);
}

static char *lc_pouch_state_crypto_context(const lc_allocator *allocator,
                                           const char *ns, const char *key,
                                           lc_pouch_generation version,
                                           unsigned char payload_class) {
  static const char prefix[] = "lc-pouch-state-aad-v1|";
  char class_text[8];
  char key_length[32];
  char namespace_length[32];
  char version_text[32];
  size_t ns_len;
  size_t key_len;
  size_t total;
  char *context;
  int written;

  if (ns == NULL || key == NULL || payload_class == 0U ||
      lc_u64_format_base10((lc_u64)strlen(ns), namespace_length,
                           sizeof(namespace_length)) < 0 ||
      lc_u64_format_base10((lc_u64)strlen(key), key_length,
                           sizeof(key_length)) < 0 ||
      lc_u64_format_base10((lc_u64)version, version_text,
                           sizeof(version_text)) < 0) {
    return NULL;
  }
  written = snprintf(class_text, sizeof(class_text), "%u",
                     (unsigned int)payload_class);
  if (written <= 0 || (size_t)written >= sizeof(class_text)) {
    return NULL;
  }
  key_len = strlen(key);
  ns_len = strlen(ns);
  total = sizeof(prefix) - 1U + strlen(namespace_length) + 1U + ns_len + 1U +
          strlen(key_length) + 1U + key_len + 1U + strlen(version_text) + 1U +
          strlen(class_text) + 1U;
  if (total < ns_len || total < key_len) {
    return NULL;
  }
  context = (char *)lc_alloc_with_allocator(allocator, total);
  if (context == NULL) {
    return NULL;
  }
  written =
      snprintf(context, total, "%s%s:%s|%s:%s|%s|%s", prefix, namespace_length,
               ns, key_length, key, version_text, class_text);
  if (written < 0 || (size_t)written + 1U != total) {
    lc_free_with_allocator(allocator, context);
    return NULL;
  }
  return context;
}

#ifdef LOCKDC_TEST_BUILD
char *lc_pouch_state_test_crypto_context(const lc_allocator *allocator,
                                         const char *ns, const char *key,
                                         lc_pouch_generation version,
                                         int object_payload) {
  return lc_pouch_state_crypto_context(allocator, ns, key, version,
                                       object_payload ? 2U : 1U);
}
#endif

static char *lc_pouch_state_payload_context_for_read(
    const lc_allocator *allocator, const char *ns, const char *logical_key,
    lc_pouch_generation version, unsigned char payload_class,
    const char *payload_context) {
  if (payload_context != NULL && payload_context[0] != '\0') {
    return lc_strdup_with_allocator(allocator, payload_context);
  }
  return lc_pouch_state_crypto_context(allocator, ns, logical_key, version,
                                       payload_class);
}

static void
lc_pouch_state_payload_span_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_payload_span *span) {
  if (span == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, span->container_leaf);
  memset(span, 0, sizeof(*span));
}

static int lc_pouch_state_payload_span_set(
    const lc_allocator *allocator, lc_pouch_state_payload_span *span,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc, lc_error *error) {
  char *container_copy;

  if (span == NULL ||
      !lc_pouch_state_payload_container_is_valid(container_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span requires a valid container", NULL,
                        NULL, "pouch");
  }
  container_copy = lc_strdup_with_allocator(allocator, container_leaf);
  if (container_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch payload span container", NULL,
                        NULL, NULL);
  }
  lc_pouch_state_payload_span_cleanup(allocator, span);
  span->container_leaf = container_copy;
  span->record_offset = record_offset;
  span->payload_offset = payload_offset;
  span->payload_length = payload_length;
  span->payload_crc = payload_crc;
  span->present = 1;
  return LC_OK;
}

static int lc_pouch_state_payload_span_copy(
    const lc_allocator *allocator, const lc_pouch_state_payload_span *src,
    lc_pouch_state_payload_span *dst, lc_error *error) {
  if (dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span copy requires output", NULL, NULL,
                        "pouch");
  }
  memset(dst, 0, sizeof(*dst));
  if (src == NULL || !src->present) {
    return LC_OK;
  }
  return lc_pouch_state_payload_span_set(
      allocator, dst, src->container_leaf, src->record_offset,
      src->payload_offset, src->payload_length, src->payload_crc, error);
}

static void lc_pouch_state_record_ref_cleanup(const lc_allocator *allocator,
                                              char **container_leaf,
                                              uint64_t *record_offset,
                                              int *present) {
  if (container_leaf != NULL) {
    lc_free_with_allocator(allocator, *container_leaf);
    *container_leaf = NULL;
  }
  if (record_offset != NULL) {
    *record_offset = 0UL;
  }
  if (present != NULL) {
    *present = 0;
  }
}

static int lc_pouch_state_record_ref_set(const lc_allocator *allocator,
                                         char **container_leaf,
                                         uint64_t *record_offset, int *present,
                                         const char *leaf, uint64_t offset,
                                         lc_error *error) {
  char *copy;

  if (container_leaf == NULL || record_offset == NULL || present == NULL ||
      !lc_pouch_state_payload_container_is_valid(leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record ref requires a valid container", NULL,
                        NULL, "pouch");
  }
  copy = lc_strdup_with_allocator(allocator, leaf);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch record ref container", NULL, NULL,
                        NULL);
  }
  lc_pouch_state_record_ref_cleanup(allocator, container_leaf, record_offset,
                                    present);
  *container_leaf = copy;
  *record_offset = offset;
  *present = 1;
  return LC_OK;
}

static int lc_pouch_state_record_ref_copy(
    const lc_allocator *allocator, const char *src_container_leaf,
    uint64_t src_record_offset, int src_present, char **dst_container_leaf,
    uint64_t *dst_record_offset, int *dst_present, lc_error *error) {
  if (dst_container_leaf == NULL || dst_record_offset == NULL ||
      dst_present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record ref copy requires outputs", NULL, NULL,
                        "pouch");
  }
  *dst_container_leaf = NULL;
  *dst_record_offset = 0UL;
  *dst_present = 0;
  if (!src_present) {
    return LC_OK;
  }
  return lc_pouch_state_record_ref_set(
      allocator, dst_container_leaf, dst_record_offset, dst_present,
      src_container_leaf, src_record_offset, error);
}

typedef struct lc_pouch_state_compaction_capture {
  char *manifest_text;
  unsigned char *io_buffer;
  char **candidate_leaves;
  int *candidate_snapshots;
  uint64_t *candidate_sizes;
  unsigned long *candidate_checksums;
  size_t candidate_count;
  size_t candidate_capacity;
  char **captured_keys;
  char **captured_record_containers;
  uint64_t *captured_record_offsets;
  size_t captured_count;
  size_t captured_capacity;
  uint64_t candidate_bytes;
} lc_pouch_state_compaction_capture;

struct lc_pouch_namespace_logstore {
  char *ns;
  char *retention_cursor;
  unsigned long namespace_hash;
  pthread_mutex_t exclusive_append_gate;
  int exclusive_append_gate_initialized;
  lc_pouch_state_metadata_append_batcher *metadata_append_batcher;
  lc_pouch_state_body_append_batcher *body_append_batcher;
  char *namespace_path;
  char *active_segment_leaf;
  char *latest_snapshot_leaf;
  int active_append_fd;
  int active_append_fd_owned;
  uint64_t active_segment_offset;
  uint64_t active_segment_id;
  uint64_t max_segment_id;
  unsigned long segment_count;
  lc_pouch_generation max_version;
  lc_pouch_generation max_query_index_seq;
  uint64_t writer_mode_epoch;
  int initialized;
  int clean_checkpoint_invalidated;
  int clean_query_checkpoint_invalidated;
  int decision_recovery_checked;
  lc_pouch_state_cache_record *records;
  lc_pouch_state_cache_record **record_buckets;
  size_t record_bucket_count;
  size_t record_count;
  size_t body_cache_bytes;
  struct lc_pouch_namespace_logstore *hash_next;
  struct lc_pouch_namespace_logstore *next;
};

static int lc_pouch_state_cache_has_live_staged_records(
    const lc_pouch_namespace_logstore *cache) {
  const lc_pouch_state_cache_record *record;

  if (cache == NULL) {
    return 1;
  }
  for (record = cache->records; record != NULL; record = record->next) {
    if (record->found && lc_pouch_state_key_is_staged(record->key)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_state_write_snapshot(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, lc_pouch_namespace_logstore *cache,
    const lc_pouch_state_compaction_capture *capture, lc_error *error);

#ifdef LOCKDC_TEST_BUILD
size_t lc_pouch_test_resident_descriptor_count(lc_pouch *pouch) {
  lc_pouch_namespace_logstore *ns;
  lc_pouch_source_cache_entry *source;
  size_t count;

  if (pouch == NULL) {
    return 0U;
  }
  count = 0U;
  if (pouch->state_cache_mutex_initialized) {
    (void)pthread_mutex_lock(&pouch->state_cache_mutex);
  }
  for (ns = pouch->namespace_logstores; ns != NULL; ns = ns->next) {
    if (ns->active_append_fd_owned && ns->active_append_fd >= 0) {
      ++count;
    }
  }
  if (pouch->state_cache_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  }
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_lock(&pouch->source_cache_mutex);
  }
  for (source = pouch->source_cache_entries; source != NULL;
       source = source->next) {
    if (source->fd >= 0) {
      ++count;
    }
  }
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->source_cache_mutex);
  }
  return count;
}
#endif

static unsigned long lc_pouch_state_namespace_hash(const char *ns) {
  const unsigned char *cursor;
  unsigned long hash;

  hash = 2166136261UL;
  for (cursor = (const unsigned char *)ns; cursor != NULL && *cursor != '\0';
       ++cursor) {
    hash ^= (unsigned long)*cursor;
    hash *= 16777619UL;
  }
  return hash;
}

static void lc_pouch_state_entry_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->content_type);
  lc_free_with_allocator(allocator, entry->etag);
  lc_pouch_state_payload_span_cleanup(allocator, &entry->payload_span);
  lc_pouch_state_record_ref_cleanup(allocator, &entry->record_container_leaf,
                                    &entry->record_offset,
                                    &entry->has_record_ref);
  lc_free_with_allocator(allocator, entry->payload_context);
  lc_free_with_allocator(allocator, entry->descriptor);
  lc_free_with_allocator(allocator, entry->metadata);
  lc_free_with_allocator(allocator, entry->decision);
  memset(entry, 0, sizeof(*entry));
}

/* The metadata append worker only needs fields that describe the existing
 * durable value. Copy these while the caller still holds mutation authority;
 * the worker then owns no borrowed projection pointers. */
static int lc_pouch_state_metadata_append_entry_copy(
    lc_pouch *pouch, const lc_pouch_state_entry *source,
    lc_pouch_state_entry *destination, lc_error *error) {
  int rc;

  if (pouch == NULL || source == NULL || destination == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append entry copy requires inputs",
                        NULL, NULL, "pouch");
  }
  memset(destination, 0, sizeof(*destination));
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_metadata_append_entry_copy_hook != NULL) {
    rc = lc_pouch_test_before_metadata_append_entry_copy_hook(
        lc_pouch_test_before_metadata_append_entry_copy_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
#endif
  destination->found = source->found;
  destination->version = source->version;
  destination->bytes = source->bytes;
  destination->cipher_bytes = source->cipher_bytes;
  destination->retention = source->retention;
  destination->has_query_hidden = source->has_query_hidden;
  destination->query_hidden = source->query_hidden;
  destination->staged_delete_marker = source->staged_delete_marker;
  if (source->content_type != NULL) {
    destination->content_type =
        lc_strdup_with_allocator(&pouch->allocator, source->content_type);
  }
  if (source->etag != NULL) {
    destination->etag =
        lc_strdup_with_allocator(&pouch->allocator, source->etag);
  }
  if (source->payload_context != NULL) {
    destination->payload_context =
        lc_strdup_with_allocator(&pouch->allocator, source->payload_context);
  }
  if (source->descriptor != NULL) {
    destination->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, source->descriptor);
  }
  if ((source->content_type != NULL && destination->content_type == NULL) ||
      (source->etag != NULL && destination->etag == NULL) ||
      (source->payload_context != NULL &&
       destination->payload_context == NULL) ||
      (source->descriptor != NULL && destination->descriptor == NULL)) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to copy pouch metadata append entry", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  rc =
      lc_pouch_state_payload_span_copy(&pouch->allocator, &source->payload_span,
                                       &destination->payload_span, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (source->metadata_length > 0U && source->metadata == NULL) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch metadata append source is malformed", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  if (source->metadata_length > 0U) {
    destination->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, source->metadata_length);
    if (destination->metadata == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch metadata append blob", NULL, NULL,
                        "pouch");
      goto cleanup;
    }
    memcpy(destination->metadata, source->metadata, source->metadata_length);
    destination->metadata_length = source->metadata_length;
  }
  return LC_OK;

cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, destination);
  return rc;
}

static void lc_pouch_state_decision_cleanup(const lc_allocator *allocator,
                                            lc_pouch_state_decision *decision) {
  if (decision == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, decision->staged_key);
  lc_free_with_allocator(allocator, decision->etag);
  lc_free_with_allocator(allocator, decision->decision);
  lc_free_with_allocator(allocator, decision);
}

static void
lc_pouch_state_decisions_cleanup(const lc_allocator *allocator,
                                 lc_pouch_state_decision *decision) {
  while (decision != NULL) {
    lc_pouch_state_decision *next;

    next = decision->next;
    lc_pouch_state_decision_cleanup(allocator, decision);
    decision = next;
  }
}

static int lc_pouch_state_string_equal(const char *left, const char *right) {
  if (left == NULL || right == NULL) {
    return left == right;
  }
  return strcmp(left, right) == 0;
}

static void lc_pouch_state_body_cache_entry_destroy(
    const lc_allocator *allocator, lc_pouch_state_body_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  if (entry->bytes != NULL) {
    OPENSSL_cleanse(entry->bytes, entry->length);
    lc_free_with_allocator(allocator, entry->bytes);
  }
  if (entry->ref_mutex_initialized) {
    (void)pthread_mutex_destroy(&entry->ref_mutex);
  }
  lc_free_with_allocator(allocator, entry);
}

static int
lc_pouch_state_body_cache_entry_init(lc_pouch_state_body_cache_entry *entry,
                                     lc_error *error) {
  int pthread_rc;

  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache entry requires storage", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_init(&entry->ref_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch body cache reference lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  entry->ref_mutex_initialized = 1;
  return LC_OK;
}

static int
lc_pouch_state_body_cache_entry_retain(lc_pouch_state_body_cache_entry *entry,
                                       lc_error *error) {
  int pthread_rc;

  if (entry == NULL || !entry->ref_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache entry is unavailable", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_lock(&entry->ref_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch body cache reference",
                        strerror(pthread_rc), NULL, "pouch");
  }
  if (entry->retired || !entry->complete) {
    (void)pthread_mutex_unlock(&entry->ref_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache entry is unavailable", NULL, NULL,
                        "pouch");
  }
  entry->refcount += 1UL;
  (void)pthread_mutex_unlock(&entry->ref_mutex);
  return LC_OK;
}

static int lc_pouch_state_body_cache_entry_retain_for_fill(
    lc_pouch_state_body_cache_entry *entry, lc_error *error) {
  int pthread_rc;

  if (entry == NULL || !entry->ref_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache entry is unavailable", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_lock(&entry->ref_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch body cache reference",
                        strerror(pthread_rc), NULL, "pouch");
  }
  if (entry->retired) {
    (void)pthread_mutex_unlock(&entry->ref_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache entry was retired", NULL, NULL,
                        "pouch");
  }
  entry->refcount += 1UL;
  (void)pthread_mutex_unlock(&entry->ref_mutex);
  return LC_OK;
}

static void lc_pouch_state_body_cache_entry_release(
    const lc_allocator *allocator, lc_pouch_state_body_cache_entry *entry) {
  int destroy;

  if (entry == NULL || !entry->ref_mutex_initialized ||
      pthread_mutex_lock(&entry->ref_mutex) != 0) {
    return;
  }
  if (entry->refcount > 0UL) {
    entry->refcount -= 1UL;
  }
  destroy = entry->retired && entry->refcount == 0UL;
  (void)pthread_mutex_unlock(&entry->ref_mutex);
  if (destroy) {
    lc_pouch_state_body_cache_entry_destroy(allocator, entry);
  }
}

static void
lc_pouch_state_body_cache_entry_retire(const lc_allocator *allocator,
                                       lc_pouch_state_body_cache_entry *entry) {
  int destroy;

  if (entry == NULL || !entry->ref_mutex_initialized ||
      pthread_mutex_lock(&entry->ref_mutex) != 0) {
    return;
  }
  entry->retired = 1;
  destroy = entry->refcount == 0UL;
  (void)pthread_mutex_unlock(&entry->ref_mutex);
  if (destroy) {
    lc_pouch_state_body_cache_entry_destroy(allocator, entry);
  }
}

static void
lc_pouch_state_cache_record_body_clear(const lc_allocator *allocator,
                                       lc_pouch_namespace_logstore *ns,
                                       lc_pouch_state_cache_record *record) {
  lc_pouch_state_body_cache_entry *entry;

  if (record == NULL || record->body_cache == NULL) {
    return;
  }
  entry = record->body_cache;
  record->body_cache = NULL;
  if (ns != NULL) {
    if (ns->body_cache_bytes >= entry->length) {
      ns->body_cache_bytes -= entry->length;
    } else {
      ns->body_cache_bytes = 0U;
    }
  }
  lc_pouch_state_body_cache_entry_retire(allocator, entry);
}

static void
lc_pouch_namespace_logstore_clear_body_cache(const lc_allocator *allocator,
                                             lc_pouch_namespace_logstore *ns) {
  lc_pouch_state_cache_record *record;

  if (ns == NULL) {
    return;
  }
  for (record = ns->records; record != NULL; record = record->next) {
    lc_pouch_state_cache_record_body_clear(allocator, NULL, record);
  }
  ns->body_cache_bytes = 0U;
}

static void
lc_pouch_state_cache_record_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_cache_record *record) {
  if (record == NULL) {
    return;
  }
  lc_pouch_state_cache_record_body_clear(allocator, NULL, record);
  lc_free_with_allocator(allocator, record->key);
  lc_free_with_allocator(allocator, record->content_type);
  lc_free_with_allocator(allocator, record->etag);
  lc_pouch_state_payload_span_cleanup(allocator, &record->payload_span);
  lc_pouch_state_record_ref_cleanup(allocator, &record->record_container_leaf,
                                    &record->record_offset,
                                    &record->has_record_ref);
  lc_free_with_allocator(allocator, record->payload_context);
  lc_free_with_allocator(allocator, record->descriptor);
  lc_free_with_allocator(allocator, record->metadata);
  lc_free_with_allocator(allocator, record);
}

static void
lc_pouch_state_cache_records_cleanup(const lc_allocator *allocator,
                                     lc_pouch_state_cache_record *record) {
  while (record != NULL) {
    lc_pouch_state_cache_record *next;

    next = record->next;
    lc_pouch_state_cache_record_cleanup(allocator, record);
    record = next;
  }
}

static void
lc_pouch_namespace_logstore_clear_records(const lc_allocator *allocator,
                                          lc_pouch_namespace_logstore *ns) {
  if (ns == NULL) {
    return;
  }
  if (ns->active_append_fd_owned && ns->active_append_fd >= 0) {
    (void)close(ns->active_append_fd);
  }
  ns->active_append_fd = -1;
  ns->active_append_fd_owned = 0;
  lc_pouch_state_cache_records_cleanup(allocator, ns->records);
  lc_free_with_allocator(allocator, ns->record_buckets);
  ns->records = NULL;
  ns->record_buckets = NULL;
  ns->record_bucket_count = 0U;
  ns->record_count = 0U;
  ns->body_cache_bytes = 0U;
  lc_free_with_allocator(allocator, ns->active_segment_leaf);
  lc_free_with_allocator(allocator, ns->latest_snapshot_leaf);
  lc_free_with_allocator(allocator, ns->namespace_path);
  ns->active_segment_leaf = NULL;
  ns->latest_snapshot_leaf = NULL;
  ns->namespace_path = NULL;
  ns->active_segment_offset = 0U;
  ns->active_segment_id = 0U;
}

/* The caller already owns the namespace projection or shared-mode mutation
 * authority. Keep the namespace node stable so unrelated exclusive namespace
 * operations cannot observe a freed cache-list entry after a failed commit. */
static void lc_pouch_state_cache_invalidate_namespace(lc_pouch *pouch,
                                                      const char *ns) {
  lc_pouch_namespace_logstore *cache;

  if (pouch == NULL || ns == NULL) {
    return;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache == NULL) {
    return;
  }
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
  cache->max_segment_id = 0U;
  cache->max_version = 0UL;
  cache->max_query_index_seq = 0UL;
  cache->writer_mode_epoch = 0U;
  cache->initialized = 0;
  cache->decision_recovery_checked = 0;
}

static void
lc_pouch_state_visit_snapshot_cleanup(const lc_allocator *allocator,
                                      lc_pouch_state_visit_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void
lc_pouch_state_visit_snapshots_cleanup(const lc_allocator *allocator,
                                       lc_pouch_state_visit_snapshot *snapshots,
                                       size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static int lc_pouch_state_visit_snapshot_append(
    const lc_allocator *allocator, lc_pouch_state_visit_snapshot **snapshots,
    size_t *count, size_t *capacity, const lc_pouch_state_cache_record *record,
    lc_error *error) {
  lc_pouch_state_visit_snapshot *next;
  lc_pouch_state_visit_snapshot *snapshot;
  size_t next_capacity;

  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 16U : *capacity * 2U;
    next = (lc_pouch_state_visit_snapshot *)lc_calloc_with_allocator(
        allocator, next_capacity, sizeof(**snapshots));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state visit snapshot", NULL,
                          NULL, NULL);
    }
    if (*snapshots != NULL) {
      memcpy(next, *snapshots, *count * sizeof(**snapshots));
      lc_free_with_allocator(allocator, *snapshots);
    }
    *snapshots = next;
    *capacity = next_capacity;
  }
  snapshot = &(*snapshots)[*count];
  snapshot->key = lc_strdup_with_allocator(allocator, record->key);
  snapshot->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(allocator, record->content_type)
          : NULL;
  snapshot->etag = record->etag != NULL
                       ? lc_strdup_with_allocator(allocator, record->etag)
                       : NULL;
  snapshot->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        allocator, record->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, record->metadata, record->metadata_length);
      snapshot->metadata_length = record->metadata_length;
    }
  }
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL) ||
      (record->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state visit entry", NULL,
                        NULL, NULL);
  }
  snapshot->index_seq = record->index_seq;
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->cipher_bytes = record->cipher_bytes;
  snapshot->updated_at_unix = record->updated_at_unix;
  snapshot->retention = record->retention;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  snapshot->has_payload = record->payload_span.present;
  snapshot->staged_delete_marker = record->staged_delete_marker;
  snapshot->found = record->found;
  snapshot->record_type = record->record_type;
  ++(*count);
  return LC_OK;
}

static void lc_pouch_state_read_many_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_read_many_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  if (snapshot->body != NULL) {
    snapshot->body->close(snapshot->body);
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->payload_span_path);
  lc_pouch_state_payload_span_cleanup(allocator, &snapshot->payload_span);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  lc_free_with_allocator(allocator, snapshot->crypto_context);
  lc_free_with_allocator(allocator, snapshot->binding_context);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void lc_pouch_state_read_many_snapshots_cleanup(
    const lc_allocator *allocator, lc_pouch_state_read_many_snapshot *snapshots,
    size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_read_many_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static void lc_pouch_state_scan_body_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_body_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->payload_span_path);
  lc_pouch_state_payload_span_cleanup(allocator, &snapshot->payload_span);
  lc_free_with_allocator(allocator, snapshot->payload_context);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  if (snapshot->body_cache != NULL) {
    lc_pouch_state_body_cache_entry_release(allocator, snapshot->body_cache);
  }
  memset(snapshot, 0, sizeof(*snapshot));
}

static void lc_pouch_state_scan_body_snapshots_cleanup(
    const lc_allocator *allocator, lc_pouch_state_scan_body_snapshot *snapshots,
    size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_scan_body_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static int lc_pouch_state_scan_body_snapshot_append(
    lc_pouch *pouch, lc_pouch_state_scan_body_snapshot **snapshots,
    size_t *count, size_t *capacity,
    const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  lc_pouch_state_scan_body_snapshot *next;
  lc_pouch_state_scan_body_snapshot *snapshot;
  size_t next_capacity;

  if (pouch == NULL || snapshots == NULL || count == NULL || capacity == NULL ||
      manifest == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan snapshot requires pouch, snapshots, "
                        "count, capacity, and record",
                        NULL, NULL, "pouch");
  }
  if (!record->found) {
    return LC_OK;
  }
  if (!record->payload_span.present) {
    return LC_OK;
  }
  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 64U : *capacity * 2U;
    next = (lc_pouch_state_scan_body_snapshot *)lc_calloc_with_allocator(
        &pouch->allocator, next_capacity, sizeof(**snapshots));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan snapshots", NULL, NULL,
                          NULL);
    }
    if (*snapshots != NULL) {
      memcpy(next, *snapshots, *count * sizeof(**snapshots));
      lc_free_with_allocator(&pouch->allocator, *snapshots);
    }
    *snapshots = next;
    *capacity = next_capacity;
  }
  snapshot = &(*snapshots)[*count];
  snapshot->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
  snapshot->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  snapshot->etag =
      record->etag != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
          : NULL;
  if (lc_pouch_state_payload_span_copy(&pouch->allocator, &record->payload_span,
                                       &snapshot->payload_span,
                                       error) != LC_OK) {
    lc_pouch_state_scan_body_snapshot_cleanup(&pouch->allocator, snapshot);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  snapshot->payload_context =
      record->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_context)
          : NULL;
  snapshot->payload_span_path = lc_pouch_state_payload_span_path(
      &pouch->allocator, manifest->namespace_path, &record->payload_span,
      error);
  snapshot->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, record->metadata, record->metadata_length);
      snapshot->metadata_length = record->metadata_length;
    }
  }
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      snapshot->payload_span_path == NULL || !snapshot->payload_span.present ||
      (record->payload_context != NULL && snapshot->payload_context == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL) ||
      (record->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_scan_body_snapshot_cleanup(&pouch->allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch scan metadata", NULL, NULL,
                        NULL);
  }
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->cipher_bytes = record->cipher_bytes;
  if (record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes &&
      lc_pouch_state_body_cache_entry_ready(record->body_cache)) {
    if (lc_pouch_state_body_cache_entry_retain(record->body_cache, error) !=
        LC_OK) {
      lc_pouch_state_scan_body_snapshot_cleanup(&pouch->allocator, snapshot);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    snapshot->body_cache = record->body_cache;
  }
  snapshot->updated_at_unix = record->updated_at_unix;
  snapshot->retention = record->retention;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  snapshot->staged_delete_marker = record->staged_delete_marker;
  snapshot->found = 1;
  snapshot->record_type = record->record_type;
  ++(*count);
  return LC_OK;
}

static int lc_pouch_state_scan_body_snapshot_open_result(
    lc_pouch *pouch, const char *ns,
    lc_pouch_state_scan_body_snapshot *snapshot, int cache_completed_body,
    lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_state_cache_guard cache_guard;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  char *crypto_context;
  char *binding_context;
  int rc;

  if (pouch == NULL || ns == NULL || snapshot == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan body requires pouch, namespace, snapshot, "
                        "and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (!snapshot->found) {
    return LC_OK;
  }
  if (!snapshot->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan payload span is invalid", NULL, NULL,
                        "pouch");
  }
  if (snapshot->body_cache != NULL &&
      snapshot->body_cache->version == snapshot->version &&
      snapshot->body_cache->length == (size_t)snapshot->bytes) {
    rc = lc_pouch_state_body_cache_source_open(
        &pouch->allocator, snapshot->body_cache, &out->body, error);
  } else {
    rc = LC_OK;
  }
  crypto_context = NULL;
  binding_context = NULL;
  if (rc == LC_OK && out->body == NULL && snapshot->descriptor != NULL) {
    crypto_context = lc_pouch_state_payload_context_for_read(
        &pouch->allocator, ns, snapshot->key, snapshot->version,
        lc_pouch_state_payload_class(snapshot->record_type, 0),
        snapshot->payload_context);
    binding_context = lc_pouch_state_crypto_context(
        &pouch->allocator, ns, snapshot->key, snapshot->version,
        lc_pouch_state_payload_class(snapshot->record_type, 0));
    if (crypto_context == NULL || binding_context == NULL) {
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      lc_free_with_allocator(&pouch->allocator, binding_context);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan crypto context", NULL,
                          NULL, NULL);
    }
  }
  /* Most scan predicates stop as soon as their selector is decided. Do not
   * allocate a cache tee for those paths: it would retain a partial body that
   * cannot be reused, turning a scan larger than the bounded cache into
   * repeated allocation, crypto setup, and eviction. The caller may opt in
   * only when it will consume the complete body, such as an any-text scan. */
  memset(&cache_guard, 0, sizeof(cache_guard));
  if (rc == LC_OK && out->body == NULL && cache_completed_body &&
      snapshot->bytes <= LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
    rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
    if (rc == LC_OK) {
      cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, error);
      record = cache != NULL
                   ? lc_pouch_state_cache_record_find(cache, snapshot->key)
                   : NULL;
      if (record != NULL && record->found &&
          record->version == snapshot->version &&
          record->bytes == snapshot->bytes) {
        memset(&current, 0, sizeof(current));
        current.key = snapshot->key;
        current.content_type = snapshot->content_type;
        current.etag = snapshot->etag;
        current.payload_span = snapshot->payload_span;
        current.payload_context = snapshot->payload_context;
        current.descriptor = snapshot->descriptor;
        current.metadata = snapshot->metadata;
        current.metadata_length = snapshot->metadata_length;
        current.version = snapshot->version;
        current.bytes = snapshot->bytes;
        current.cipher_bytes = snapshot->cipher_bytes;
        current.updated_at_unix = snapshot->updated_at_unix;
        current.retention = snapshot->retention;
        current.has_query_hidden = snapshot->has_query_hidden;
        current.query_hidden = snapshot->query_hidden;
        current.staged_delete_marker = snapshot->staged_delete_marker;
        current.seen = 1;
        current.found = 1;
        rc = lc_pouch_state_read_many_snapshot_body_from_cache(
            pouch, cache, record, crypto_context, binding_context,
            snapshot->payload_span_path, snapshot->payload_span.payload_offset,
            snapshot->payload_span.payload_length, &current, 0, &out->body,
            error);
      }
      lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
    }
  }
  if (rc == LC_OK && out->body == NULL) {
    rc = lc_pouch_state_source_from_span(
        pouch, crypto_context, binding_context, snapshot->payload_span_path,
        snapshot->payload_span.payload_offset,
        snapshot->payload_span.payload_length, snapshot->descriptor, &out->body,
        error);
  }
  lc_free_with_allocator(&pouch->allocator, crypto_context);
  lc_free_with_allocator(&pouch->allocator, binding_context);
  if (rc != LC_OK) {
    return rc;
  }
  out->content_type = snapshot->content_type;
  out->etag = snapshot->etag;
  out->descriptor = snapshot->descriptor;
  out->metadata = snapshot->metadata;
  out->metadata_length = snapshot->metadata_length;
  out->version = snapshot->version;
  out->bytes = snapshot->bytes;
  out->cipher_bytes = snapshot->cipher_bytes;
  out->updated_at_unix = snapshot->updated_at_unix;
  out->retention = snapshot->retention;
  out->has_query_hidden = snapshot->has_query_hidden;
  out->query_hidden = snapshot->query_hidden;
  out->staged_delete_marker = snapshot->staged_delete_marker;
  out->has_body = snapshot->payload_span.present;
  out->found = 1;
  snapshot->content_type = NULL;
  snapshot->etag = NULL;
  snapshot->descriptor = NULL;
  snapshot->metadata = NULL;
  snapshot->metadata_length = 0U;
  return LC_OK;
}

static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *binding_context, const char *payload_span_path,
    uint64_t payload_offset, uint64_t payload_length,
    const lc_pouch_state_entry *current, int copy_cached_body, lc_source **out,
    lc_error *error) {
  lc_source *source;
  lc_pouch_state_body_cache_entry *entry;
  lc_error cache_error;
  int rc;

  entry = NULL;
  if (pouch == NULL || cache == NULL || record == NULL ||
      payload_span_path == NULL || current == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many body cache requires pouch, cache, "
                        "record, path, entry, and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes &&
      lc_pouch_state_body_cache_entry_ready(record->body_cache)) {
    if (copy_cached_body) {
      return lc_source_from_memory(record->body_cache->bytes,
                                   record->body_cache->length, out, error);
    }
    return lc_pouch_state_body_cache_source_open(
        &pouch->allocator, record->body_cache, out, error);
  }
  if (record->body_cache != NULL) {
    lc_pouch_state_cache_record_body_clear(&pouch->allocator, cache, record);
  }
  if (current->bytes > LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
    return lc_pouch_state_source_from_span(
        pouch, crypto_context, binding_context, payload_span_path,
        payload_offset, payload_length, current->descriptor, out, error);
  }
  source = NULL;
  rc = lc_pouch_state_source_from_span(
      pouch, crypto_context, binding_context, payload_span_path, payload_offset,
      payload_length, current->descriptor, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_error_init(&cache_error);
  if (cache->body_cache_bytes + (size_t)current->bytes >
      LC_POUCH_STATE_BODY_CACHE_MAX_BYTES) {
    lc_pouch_namespace_logstore_clear_body_cache(&pouch->allocator, cache);
  }
  entry = (lc_pouch_state_body_cache_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL ||
      lc_pouch_state_body_cache_entry_init(entry, &cache_error) != LC_OK) {
    lc_error_cleanup(&cache_error);
    lc_pouch_state_body_cache_entry_destroy(&pouch->allocator, entry);
    *out = source;
    return LC_OK;
  }
  entry->length = (size_t)current->bytes;
  entry->version = record->version;
  if (entry->length > 0U) {
    entry->bytes = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                            entry->length);
  }
  if (entry->length > 0U && entry->bytes == NULL) {
    lc_error_cleanup(&cache_error);
    lc_pouch_state_body_cache_entry_destroy(&pouch->allocator, entry);
    *out = source;
    return LC_OK;
  }
  record->body_cache = entry;
  cache->body_cache_bytes += entry->length;
  rc = lc_pouch_state_body_cache_tee_source_open(&pouch->allocator, source,
                                                 entry, out, &cache_error);
  if (rc == LC_OK) {
    lc_error_cleanup(&cache_error);
    return LC_OK;
  }
  lc_pouch_state_cache_record_body_clear(&pouch->allocator, cache, record);
  lc_error_cleanup(&cache_error);
  *out = source;
  return LC_OK;
}

static int lc_pouch_state_read_many_snapshot_from_entry(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, const char *requested_key,
    lc_pouch_namespace_logstore *cache, lc_pouch_state_cache_record *record,
    const lc_pouch_state_entry *current, int include_body, int copy_cached_body,
    lc_pouch_state_read_many_snapshot *snapshot, lc_error *error) {
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL ||
      requested_key == NULL || current == NULL || snapshot == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many snapshot requires pouch, namespace, "
                        "manifest, key, entry, and snapshot",
                        NULL, NULL, NULL);
  }
  memset(snapshot, 0, sizeof(*snapshot));
  snapshot->key = lc_strdup_with_allocator(&pouch->allocator, requested_key);
  if (snapshot->key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch read-many key", NULL, NULL,
                        NULL);
  }
  if (!current->found) {
    return LC_OK;
  }
  if (!current->payload_span.present && include_body) {
    return LC_OK;
  }
  if (!current->payload_span.present && !include_body) {
    snapshot->content_type =
        current->content_type != NULL
            ? lc_strdup_with_allocator(&pouch->allocator, current->content_type)
            : NULL;
    snapshot->etag =
        current->etag != NULL
            ? lc_strdup_with_allocator(&pouch->allocator, current->etag)
            : NULL;
    if (current->metadata_length > 0U) {
      snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
          &pouch->allocator, current->metadata_length);
      if (snapshot->metadata != NULL) {
        memcpy(snapshot->metadata, current->metadata, current->metadata_length);
        snapshot->metadata_length = current->metadata_length;
      }
    }
    if ((current->content_type != NULL && snapshot->content_type == NULL) ||
        (current->etag != NULL && snapshot->etag == NULL) ||
        (current->metadata_length > 0U && snapshot->metadata == NULL)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many metadata", NULL,
                          NULL, NULL);
    }
    snapshot->index_seq = current->index_seq;
    snapshot->version = current->version;
    snapshot->bytes = current->bytes;
    snapshot->cipher_bytes = current->cipher_bytes;
    snapshot->updated_at_unix = current->updated_at_unix;
    snapshot->retention = current->retention;
    snapshot->has_query_hidden = current->has_query_hidden;
    snapshot->query_hidden = current->query_hidden;
    snapshot->staged_delete_marker = current->staged_delete_marker;
    snapshot->found = 1;
    snapshot->record_type = current->record_type;
    return LC_OK;
  }
  if (!current->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
  }
  snapshot->content_type =
      current->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->content_type)
          : NULL;
  snapshot->etag =
      current->etag != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->etag)
          : NULL;
  snapshot->descriptor =
      current->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->descriptor)
          : NULL;
  if (current->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, current->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, current->metadata, current->metadata_length);
      snapshot->metadata_length = current->metadata_length;
    }
  }
  snapshot->crypto_context =
      current->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator,
                                     current->payload_context)
          : NULL;
  if (lc_pouch_state_payload_span_copy(
          &pouch->allocator, &current->payload_span, &snapshot->payload_span,
          error) != LC_OK) {
    lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if ((current->content_type != NULL && snapshot->content_type == NULL) ||
      (current->etag != NULL && snapshot->etag == NULL) ||
      (current->payload_context != NULL && snapshot->crypto_context == NULL) ||
      (current->descriptor != NULL && snapshot->descriptor == NULL) ||
      (current->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch read-many metadata", NULL,
                        NULL, NULL);
  }
  if (include_body) {
    snapshot->payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest->namespace_path, &current->payload_span,
        error);
    if (snapshot->payload_span_path == NULL) {
      lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many payload span",
                          NULL, NULL, NULL);
    }
    if (current->descriptor != NULL && snapshot->crypto_context == NULL) {
      snapshot->crypto_context = lc_pouch_state_crypto_context(
          &pouch->allocator, ns, requested_key, current->version,
          lc_pouch_state_payload_class(current->record_type,
                                       current->object_link));
      if (snapshot->crypto_context == NULL) {
        lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch read-many crypto context",
                            NULL, NULL, NULL);
      }
    }
    if (current->descriptor != NULL) {
      snapshot->binding_context = lc_pouch_state_crypto_context(
          &pouch->allocator, ns, requested_key, current->version,
          lc_pouch_state_payload_class(current->record_type,
                                       current->object_link));
      if (snapshot->binding_context == NULL) {
        lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
        return lc_error_set(
            error, LC_ERR_NOMEM, 0L,
            "failed to allocate pouch read-many binding context", NULL, NULL,
            NULL);
      }
    }
    if (cache != NULL && record != NULL) {
      rc = lc_pouch_state_read_many_snapshot_body_from_cache(
          pouch, cache, record, snapshot->crypto_context,
          snapshot->binding_context, snapshot->payload_span_path,
          current->payload_span.payload_offset,
          current->payload_span.payload_length, current, copy_cached_body,
          &snapshot->body, error);
    } else {
      rc = lc_pouch_state_source_from_span(
          pouch, snapshot->crypto_context, snapshot->binding_context,
          snapshot->payload_span_path, current->payload_span.payload_offset,
          current->payload_span.payload_length, snapshot->descriptor,
          &snapshot->body, error);
    }
    if (rc != LC_OK) {
      lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  snapshot->index_seq = current->index_seq;
  snapshot->version = current->version;
  snapshot->bytes = current->bytes;
  snapshot->cipher_bytes = current->cipher_bytes;
  snapshot->updated_at_unix = current->updated_at_unix;
  snapshot->retention = current->retention;
  snapshot->has_query_hidden = current->has_query_hidden;
  snapshot->query_hidden = current->query_hidden;
  snapshot->staged_delete_marker = current->staged_delete_marker;
  snapshot->found = 1;
  snapshot->record_type = current->record_type;
  return LC_OK;
}

static void lc_pouch_state_process_registry_cleanup(lc_pouch *pouch) {
  char prefix[35];
  lc_pouch_state_process_namespace_mutex **mutex_link;
  lc_pouch_state_process_namespace_guard **guard_link;
  lc_pouch_state_shared_mutation_guard **mutation_link;

  if (!pouch->root_identity_initialized ||
      lc_u64_format_base16_padded((lc_u64)pouch->root_device, 16U, prefix,
                                  17U) < 0 ||
      lc_u64_format_base16_padded((lc_u64)pouch->root_inode, 16U, prefix + 17U,
                                  17U) < 0)
    return;
  prefix[16] = ':';
  prefix[33] = '\n';
  prefix[34] = '\0';
  /* Registry references include holders and waiters from every open handle.
   * Closing one handle can reclaim idle entries without disturbing a sibling
   * handle's current mutation, recursive callback, reader or compaction. */
  pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
  mutex_link = &lc_pouch_state_process_mutexes;
  while (*mutex_link != NULL) {
    lc_pouch_state_process_namespace_mutex *entry = *mutex_link;
    if (entry->refcount == 0UL && strncmp(entry->identity, prefix, 34U) == 0) {
      *mutex_link = entry->next;
      pthread_mutex_destroy(&entry->mutex);
      lc_free_with_allocator(NULL, entry->identity);
      lc_free_with_allocator(NULL, entry);
    } else {
      mutex_link = &entry->next;
    }
  }
  pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
  pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
  guard_link = &lc_pouch_state_process_guards;
  while (*guard_link != NULL) {
    lc_pouch_state_process_namespace_guard *entry = *guard_link;
    if (entry->refcount == 0UL && strncmp(entry->identity, prefix, 34U) == 0) {
      *guard_link = entry->next;
      pthread_rwlock_destroy(&entry->lock);
      pthread_mutex_destroy(&entry->writer_mutex);
      lc_free_with_allocator(NULL, entry->identity);
      lc_free_with_allocator(NULL, entry);
    } else {
      guard_link = &entry->next;
    }
  }
  pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
  pthread_mutex_lock(&lc_pouch_state_shared_mutation_registry);
  mutation_link = &lc_pouch_state_shared_mutation_guards;
  while (*mutation_link != NULL) {
    lc_pouch_state_shared_mutation_guard *entry = *mutation_link;
    if (entry->refcount == 0UL && strncmp(entry->identity, prefix, 34U) == 0) {
      *mutation_link = entry->next;
      pthread_mutex_destroy(&entry->mutex);
      lc_free_with_allocator(NULL, entry->identity);
      lc_free_with_allocator(NULL, entry);
    } else {
      mutation_link = &entry->next;
    }
  }
  pthread_mutex_unlock(&lc_pouch_state_shared_mutation_registry);
}

void lc_pouch_state_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_namespace_logstore *ns;

  if (pouch == NULL) {
    return;
  }
  ns = pouch->namespace_logstores;
  while (ns != NULL) {
    lc_pouch_namespace_logstore *next;

    next = ns->next;
    if (ns->exclusive_append_gate_initialized) {
      (void)pthread_mutex_destroy(&ns->exclusive_append_gate);
    }
    lc_free_with_allocator(&pouch->allocator, ns->ns);
    lc_free_with_allocator(&pouch->allocator, ns->retention_cursor);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, ns);
    lc_free_with_allocator(&pouch->allocator, ns);
    ns = next;
  }
  pouch->namespace_logstores = NULL;
  memset(pouch->namespace_logstore_buckets, 0,
         sizeof(pouch->namespace_logstore_buckets));
  lc_pouch_state_process_registry_cleanup(pouch);
}

/* A clean checkpoint is only a cold-start hint. State and public-query
 * watermarks have different invalidation boundaries: every append invalidates
 * the state projection, while lease-only and private staging appends leave
 * the public query projection unchanged. Keeping the latter proof prevents a
 * dispatcher claim from turning an otherwise clean restart into a historical
 * log replay. */
static int lc_pouch_state_clean_checkpoint_invalidate(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const char *namespace_path, int invalidate_query, lc_error *error) {
  char *path;
  struct stat st;
  char query_generation[32];
  char line[160];
  int rc;

  if (pouch == NULL || cache == NULL || namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch clean checkpoint requires namespace state", NULL,
                        NULL, "pouch");
  }
  if (cache->clean_checkpoint_invalidated &&
      (!invalidate_query || cache->clean_query_checkpoint_invalidated)) {
    return LC_OK;
  }
  path = lc_pouch_path_join(&pouch->allocator, namespace_path,
                            LC_POUCH_STATE_CLEAN_INDEX_CHECKPOINT_LEAF);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch clean checkpoint path", NULL,
                        NULL, "pouch");
  }
  if (lstat(path, &st) != 0) {
    if (errno == ENOENT) {
      cache->clean_checkpoint_invalidated = 1;
      if (invalidate_query) {
        cache->clean_query_checkpoint_invalidated = 1;
      }
      lc_free_with_allocator(&pouch->allocator, path);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to inspect pouch clean checkpoint",
                      strerror(errno), NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  if (invalidate_query) {
    rc = lc_pouch_path_write_text_file(path, "max=invalid\nquery_max=invalid\n",
                                       error);
  } else if (lc_u64_format_base10((lc_u64)cache->max_query_index_seq,
                                  query_generation,
                                  sizeof(query_generation)) < 0 ||
             snprintf(line, sizeof(line), "max=invalid\nquery_max=%s\n",
                      query_generation) < 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "failed to format pouch clean query checkpoint", NULL,
                      NULL, "pouch");
  } else {
    rc = lc_pouch_path_write_text_file(path, line, error);
  }
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc == LC_OK) {
    cache->clean_checkpoint_invalidated = 1;
    if (invalidate_query) {
      cache->clean_query_checkpoint_invalidated = 1;
    }
  }
  return rc;
}

void lc_pouch_state_checkpoint_clean_close(lc_pouch *pouch) {
  lc_pouch_namespace_logstore *ns;

  if (pouch == NULL || !lc_pouch_single_writer_enabled(pouch) ||
      lc_pouch_crypto_enabled(pouch->crypto) ||
      lc_pouch_crypto_compression_enabled(pouch->crypto)) {
    return;
  }
  for (ns = pouch->namespace_logstores; ns != NULL; ns = ns->next) {
    lc_pouch_namespace_manifest manifest;
    char *path;
    char generation[32];
    char query_generation[32];
    char identity_text[32];
    char line[224];
    uint64_t identity;
    int projection_written;
    lc_error error;

    /* A clean projection is a compact, self-contained state snapshot. It is
     * written only during orderly exclusive shutdown, after every writer has
     * stopped; the first later reservation invalidates its checkpoint before
     * appending. This keeps cold restarts off the historical state log without
     * adding work to the single-writer mutation path. */
    memset(&manifest, 0, sizeof(manifest));
    identity = 0U;
    projection_written = 0;
    if (ns->initialized && ns->namespace_path != NULL) {
      lc_error_init(&error);
      if (lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                           ns->ns, &manifest, NULL, NULL,
                                           &error) == LC_OK) {
        if (lc_pouch_state_write_snapshot(pouch, &manifest,
                                          LC_POUCH_STATE_CLEAN_PROJECTION_LEAF,
                                          ns, NULL, &error) == LC_OK &&
            lc_pouch_state_clean_checkpoint_identity(
                pouch, &manifest, &identity, &error) == LC_OK) {
          projection_written = 1;
        }
      }
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      lc_error_cleanup(&error);
    }

    if (!projection_written || !ns->initialized || ns->namespace_path == NULL ||
        lc_pouch_state_cache_has_live_staged_records(ns) ||
        lc_u64_format_base10((lc_u64)ns->max_version, generation,
                             sizeof(generation)) < 0 ||
        lc_u64_format_base10((lc_u64)ns->max_query_index_seq, query_generation,
                             sizeof(query_generation)) < 0 ||
        lc_u64_format_base10((lc_u64)identity, identity_text,
                             sizeof(identity_text)) < 0 ||
        snprintf(line, sizeof(line), "max=%s\nquery_max=%s\nidentity=%s\n",
                 generation, query_generation, identity_text) < 0) {
      continue;
    }
    path = lc_pouch_path_join(&pouch->allocator, ns->namespace_path,
                              LC_POUCH_STATE_CLEAN_INDEX_CHECKPOINT_LEAF);
    if (path == NULL) {
      continue;
    }
    lc_error_init(&error);
    (void)lc_pouch_path_write_text_file(path, line, &error);
    lc_error_cleanup(&error);
    lc_free_with_allocator(&pouch->allocator, path);
  }
}

void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_source_cache_entry *entry;

  if (pouch == NULL) {
    return;
  }
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_lock(&pouch->source_cache_mutex);
  }
  entry = pouch->source_cache_entries;
  while (entry != NULL) {
    lc_pouch_source_cache_entry *next;

    next = entry->next;
    if (entry->fd >= 0) {
      close(entry->fd);
    }
    lc_free_with_allocator(&pouch->allocator, entry->path);
    lc_free_with_allocator(&pouch->allocator, entry);
    entry = next;
  }
  pouch->source_cache_entries = NULL;
  pouch->source_cache_count = 0U;
  pouch->source_cache_tick = 0UL;
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->source_cache_mutex);
  }
}

static void lc_pouch_state_source_cache_evict_one(lc_pouch *pouch) {
  lc_pouch_source_cache_entry **cursor;
  lc_pouch_source_cache_entry **oldest_cursor;
  unsigned long oldest_tick;

  if (pouch == NULL || pouch->source_cache_entries == NULL) {
    return;
  }
  cursor = &pouch->source_cache_entries;
  oldest_cursor = cursor;
  oldest_tick = (*cursor)->last_used;
  while (*cursor != NULL) {
    if ((*cursor)->last_used <= oldest_tick) {
      oldest_tick = (*cursor)->last_used;
      oldest_cursor = cursor;
    }
    cursor = &(*cursor)->next;
  }
  if (*oldest_cursor != NULL) {
    lc_pouch_source_cache_entry *evicted;

    evicted = *oldest_cursor;
    *oldest_cursor = evicted->next;
    if (evicted->fd >= 0) {
      close(evicted->fd);
    }
    lc_free_with_allocator(&pouch->allocator, evicted->path);
    lc_free_with_allocator(&pouch->allocator, evicted);
    if (pouch->source_cache_count > 0U) {
      --pouch->source_cache_count;
    }
  }
}

static int lc_pouch_state_source_cache_dup_fd(lc_pouch *pouch, const char *path,
                                              int *out_fd, lc_error *error) {
  lc_pouch_source_cache_entry *entry;
  int fd;
  int pthread_rc;
  int rc;

  if (pouch == NULL || path == NULL || out_fd == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source cache requires pouch, path, and output",
                        NULL, NULL, "pouch");
  }
  *out_fd = -1;
  if (!pouch->source_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source cache mutex is unavailable", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_lock(&pouch->source_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch source cache",
                        strerror(pthread_rc), NULL, "pouch");
  }
  rc = LC_OK;
  for (entry = pouch->source_cache_entries; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->path, path) == 0) {
      entry->last_used = ++pouch->source_cache_tick;
      /* Span sources use pread, so duplicated descriptors do not share a
       * stream position. */
      fd = dup(entry->fd);
      if (fd < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to duplicate pouch source descriptor",
                          strerror(errno), NULL, "pouch");
      } else {
        *out_fd = fd;
      }
      goto cleanup;
    }
  }
  while (pouch->source_cache_count >= LC_POUCH_STATE_SOURCE_CACHE_MAX_FILES) {
    lc_pouch_state_source_cache_evict_one(pouch);
  }
  entry = (lc_pouch_source_cache_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch source cache entry", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  if (entry->path == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to copy pouch source cache path", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  entry->fd = open(path, O_RDONLY);
  if (entry->fd < 0) {
    lc_free_with_allocator(&pouch->allocator, entry->path);
    lc_free_with_allocator(&pouch->allocator, entry);
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch source cache file", strerror(errno),
                      NULL, "pouch");
    goto cleanup;
  }
  entry->last_used = ++pouch->source_cache_tick;
  entry->next = pouch->source_cache_entries;
  pouch->source_cache_entries = entry;
  ++pouch->source_cache_count;
  fd = dup(entry->fd);
  if (fd < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to duplicate pouch source descriptor",
                      strerror(errno), NULL, "pouch");
    goto cleanup;
  }
  *out_fd = fd;
cleanup:
  (void)pthread_mutex_unlock(&pouch->source_cache_mutex);
  return rc;
}

static int lc_pouch_state_source_from_span(
    lc_pouch *pouch, const char *crypto_context, const char *binding_context,
    const char *path, uint64_t payload_offset, uint64_t payload_length,
    const char *descriptor, lc_source **out, lc_error *error) {
  int fd;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span source requires pouch, path, and output",
                        NULL, NULL, "pouch");
  }
  fd = -1;
  rc = lc_pouch_state_source_cache_dup_fd(pouch, path, &fd, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_crypto_source_from_fd_span(
      pouch->crypto, crypto_context, binding_context, fd, payload_offset,
      payload_length, descriptor, out, error);
}

static lc_pouch_namespace_logstore *
lc_pouch_namespace_logstore_find(lc_pouch *pouch, const char *ns, int create,
                                 lc_error *error) {
  lc_pouch_namespace_logstore *logstore;
  unsigned long namespace_hash;
  size_t bucket_index;
  int pthread_rc;

  if (pouch == NULL || ns == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch state cache namespace requires context", NULL,
                       NULL, "pouch");
    return NULL;
  }
  namespace_hash = lc_pouch_state_namespace_hash(ns);
  bucket_index =
      (size_t)(namespace_hash %
               (unsigned long)LC_POUCH_NAMESPACE_REGISTRY_BUCKET_COUNT);
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to lock pouch state cache registry",
                       strerror(pthread_rc), NULL, "pouch");
    return NULL;
  }
  for (logstore = pouch->namespace_logstore_buckets[bucket_index];
       logstore != NULL; logstore = logstore->hash_next) {
    if (logstore->namespace_hash == namespace_hash &&
        strcmp(logstore->ns, ns) == 0) {
      (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
      return logstore;
    }
  }
  if (!create) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  logstore = (lc_pouch_namespace_logstore *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*logstore));
  if (logstore == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace", NULL, NULL,
                 NULL);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  logstore->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (logstore->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, logstore);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace name", NULL,
                 NULL, NULL);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  logstore->active_append_fd = -1;
  logstore->namespace_hash = namespace_hash;
  logstore->hash_next = pouch->namespace_logstore_buckets[bucket_index];
  pouch->namespace_logstore_buckets[bucket_index] = logstore;
  logstore->next = pouch->namespace_logstores;
  pouch->namespace_logstores = logstore;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  return logstore;
}

static int lc_pouch_state_exclusive_append_gate_lock(lc_pouch *pouch,
                                                     const char *ns,
                                                     pthread_mutex_t **out,
                                                     lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  pthread_mutex_t *gate;
  int pthread_rc;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL ||
      !pouch->state_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exclusive append gate requires namespace", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace owner registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  rc = LC_OK;
  if (!cache->exclusive_append_gate_initialized) {
    rc = lc_pouch_state_mutex_init_recursive(&cache->exclusive_append_gate,
                                             error);
    if (rc == LC_OK) {
      cache->exclusive_append_gate_initialized = 1;
    }
  }
  gate = rc == LC_OK ? &cache->exclusive_append_gate : NULL;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(gate);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch exclusive append gate",
                        strerror(pthread_rc), NULL, "pouch");
  }
  *out = gate;
  return LC_OK;
}

static void
lc_pouch_state_exclusive_append_gate_unlock(pthread_mutex_t **gate) {
  if (gate == NULL || *gate == NULL) {
    return;
  }
  (void)pthread_mutex_unlock(*gate);
  *gate = NULL;
}

static unsigned long lc_pouch_state_cache_key_hash(const char *key) {
  unsigned long hash;
  const unsigned char *cursor;

  hash = 2166136261UL;
  cursor = (const unsigned char *)key;
  while (cursor != NULL && *cursor != '\0') {
    hash ^= (unsigned long)*cursor++;
    hash *= 16777619UL;
  }
  return hash;
}

static size_t lc_pouch_state_cache_bucket_count_for(size_t record_count) {
  size_t bucket_count;

  bucket_count = 1024U;
  while (bucket_count < record_count * 2U) {
    bucket_count *= 2U;
  }
  return bucket_count;
}

static int lc_pouch_state_cache_record_index_rebuild(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns, size_t next_record_count,
    lc_error *error) {
  lc_pouch_state_cache_record **buckets;
  lc_pouch_state_cache_record *record;
  size_t bucket_count;

  if (pouch == NULL || ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache index requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  bucket_count = lc_pouch_state_cache_bucket_count_for(next_record_count);
  buckets = (lc_pouch_state_cache_record **)lc_calloc_with_allocator(
      &pouch->allocator, bucket_count, sizeof(*buckets));
  if (buckets == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cache lookup index",
                        NULL, NULL, NULL);
  }
  for (record = ns->records; record != NULL; record = record->next) {
    size_t bucket;

    bucket =
        (size_t)(lc_pouch_state_cache_key_hash(record->key) % bucket_count);
    record->bucket_next = buckets[bucket];
    buckets[bucket] = record;
  }
  lc_free_with_allocator(&pouch->allocator, ns->record_buckets);
  ns->record_buckets = buckets;
  ns->record_bucket_count = bucket_count;
  return LC_OK;
}

static int lc_pouch_state_cache_record_index_ensure(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns, size_t next_record_count,
    lc_error *error) {
  if (ns->record_buckets != NULL &&
      next_record_count * 2U <= ns->record_bucket_count) {
    return LC_OK;
  }
  return lc_pouch_state_cache_record_index_rebuild(pouch, ns, next_record_count,
                                                   error);
}

static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_namespace_logstore *ns,
                                 const char *key) {
  lc_pouch_state_cache_record *record;

  if (ns == NULL || key == NULL) {
    return NULL;
  }
  if (ns->record_buckets != NULL && ns->record_bucket_count > 0U) {
    size_t bucket;

    bucket =
        (size_t)(lc_pouch_state_cache_key_hash(key) % ns->record_bucket_count);
    for (record = ns->record_buckets[bucket]; record != NULL;
         record = record->bucket_next) {
      if (strcmp(record->key, key) == 0) {
        return record;
      }
    }
    return NULL;
  }
  for (record = ns->records; record != NULL; record = record->next) {
    if (strcmp(record->key, key) == 0) {
      return record;
    }
  }
  return NULL;
}

static int lc_pouch_state_cache_record_ptr_compare(const void *left,
                                                   const void *right) {
  const lc_pouch_state_cache_record *a;
  const lc_pouch_state_cache_record *b;

  a = *(const lc_pouch_state_cache_record *const *)left;
  b = *(const lc_pouch_state_cache_record *const *)right;
  if (a == NULL && b == NULL) {
    return 0;
  }
  if (a == NULL) {
    return -1;
  }
  if (b == NULL) {
    return 1;
  }
  return strcmp(a->key, b->key);
}

static int lc_pouch_state_cache_record_key_compare(const void *left,
                                                   const void *right) {
  const char *key;
  const lc_pouch_state_cache_record *record;

  key = (const char *)left;
  record = *(const lc_pouch_state_cache_record *const *)right;
  if (record == NULL) {
    return key == NULL ? 0 : 1;
  }
  if (key == NULL) {
    return -1;
  }
  return strcmp(key, record->key);
}

static int lc_pouch_state_cache_record_index_build(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns,
    lc_pouch_state_cache_record ***records_out, size_t *count_out,
    lc_error *error) {
  lc_pouch_state_cache_record **records;
  lc_pouch_state_cache_record *record;
  size_t count;
  size_t index;

  if (records_out == NULL || count_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache record index requires outputs", NULL,
                        NULL, NULL);
  }
  *records_out = NULL;
  *count_out = 0U;
  if (pouch == NULL || ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache record index requires pouch and "
                        "namespace cache",
                        NULL, NULL, NULL);
  }
  count = 0U;
  for (record = ns->records; record != NULL; record = record->next) {
    ++count;
  }
  if (count == 0U) {
    return LC_OK;
  }
  records = (lc_pouch_state_cache_record **)lc_alloc_with_allocator(
      &pouch->allocator, count * sizeof(*records));
  if (records == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cache record index",
                        NULL, NULL, NULL);
  }
  index = 0U;
  for (record = ns->records; record != NULL; record = record->next) {
    records[index++] = record;
  }
  qsort(records, count, sizeof(*records),
        lc_pouch_state_cache_record_ptr_compare);
  *records_out = records;
  *count_out = count;
  return LC_OK;
}

static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_index_find(lc_pouch_state_cache_record **records,
                                       size_t count, const char *key) {
  lc_pouch_state_cache_record **found;

  if (records == NULL || count == 0U || key == NULL) {
    return NULL;
  }
  found = (lc_pouch_state_cache_record **)bsearch(
      key, records, count, sizeof(*records),
      lc_pouch_state_cache_record_key_compare);
  return found != NULL ? *found : NULL;
}

static size_t lc_pouch_state_cache_record_index_start_after(
    lc_pouch_state_cache_record **records, size_t count,
    const char *start_after) {
  size_t low;
  size_t high;

  if (records == NULL || count == 0U || start_after == NULL ||
      start_after[0] == '\0') {
    return 0U;
  }
  low = 0U;
  high = count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + (high - low) / 2U;
    cmp = records[mid] != NULL && records[mid]->key != NULL
              ? strcmp(records[mid]->key, start_after)
              : -1;
    if (cmp <= 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int
lc_pouch_state_entry_supersedes(const lc_pouch_state_entry *candidate,
                                const lc_pouch_state_entry *current) {
  if (candidate == NULL) {
    return 0;
  }
  if (current == NULL || !current->seen) {
    return 1;
  }
  if (candidate->record_type == LC_POUCH_STATE_RECORD_STATE_META) {
    /* Metadata is ordered independently from payload state. */
    return candidate->index_seq > current->index_seq;
  }
  return candidate->version > current->version ||
         (candidate->version == current->version &&
          candidate->index_seq > current->index_seq);
}

static int lc_pouch_state_cache_apply_entry(lc_pouch *pouch,
                                            lc_pouch_namespace_logstore *ns,
                                            const lc_pouch_state_entry *entry,
                                            lc_error *error) {
  lc_pouch_state_cache_record *record;
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  int has_record_ref;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  lc_pouch_generation query_index_seq;
  int entry_object_record;
  int prior_object_record;
  int rc;

  memset(&payload_span, 0, sizeof(payload_span));
  record_container_leaf = NULL;
  record_offset = 0U;
  has_record_ref = 0;
  metadata = NULL;
  query_index_seq = 0UL;
  entry_object_record = 0;
  prior_object_record = 0;
  if (entry == NULL || !entry->seen || entry->key == NULL) {
    return LC_OK;
  }
  if (entry->metadata_length > 0U && entry->metadata == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata blob requires bytes", NULL, NULL,
                        "pouch");
  }
  if (entry->control) {
    if (entry->index_seq > ns->max_version) {
      ns->max_version = entry->index_seq;
    }
    if (entry->query_index_seq > ns->max_query_index_seq) {
      ns->max_query_index_seq = entry->query_index_seq;
    }
    return LC_OK;
  }
  record = lc_pouch_state_cache_record_find(ns, entry->key);
  entry_object_record =
      lc_pouch_state_record_type_is_object(entry->record_type) ||
      entry->object_link;
  prior_object_record = record != NULL && lc_pouch_state_record_type_is_object(
                                              record->record_type);
  if (record != NULL) {
    lc_pouch_state_entry current;

    memset(&current, 0, sizeof(current));
    current.seen = 1;
    current.version = record->version;
    current.index_seq = record->index_seq;
    current.record_type = record->record_type;
    if (!lc_pouch_state_entry_supersedes(entry, &current)) {
      return LC_OK;
    }
  }
  /* Internal objects never become public query state. STATE_META only changes
   * lease/application metadata. A metadata-only record is the durable
   * placeholder for a newly acquired lease, not a document: it remains
   * query-neutral until a later state put supplies a body. Once a body exists,
   * retain the prior public generation across lease metadata changes. */
  if (lc_pouch_state_key_is_staged(entry->key)) {
    query_index_seq = 0UL;
  } else if (entry->has_query_index_seq) {
    query_index_seq = entry->query_index_seq;
  } else {
    query_index_seq = entry->index_seq;
    if (entry_object_record && record != NULL) {
      query_index_seq = record->query_index_seq;
    } else if (entry_object_record) {
      query_index_seq = 0UL;
    } else if ((entry->record_type == LC_POUCH_STATE_RECORD_STATE_META ||
                entry->record_type == LC_POUCH_STATE_RECORD_STATE_DELETE) &&
               prior_object_record) {
      query_index_seq = record->query_index_seq;
    } else if (entry->record_type == LC_POUCH_STATE_RECORD_STATE_META &&
               !entry->payload_span.present &&
               (record == NULL || !record->payload_span.present)) {
      query_index_seq = record != NULL ? record->query_index_seq : 0UL;
    } else if (entry->record_type == LC_POUCH_STATE_RECORD_STATE_META &&
               record != NULL && record->found == entry->found &&
               record->has_query_hidden == entry->has_query_hidden &&
               record->query_hidden == entry->query_hidden &&
               record->query_index_seq != 0UL) {
      query_index_seq = record->query_index_seq;
    }
  }
  if (record == NULL) {
    rc = lc_pouch_state_cache_record_index_ensure(pouch, ns,
                                                  ns->record_count + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    record = (lc_pouch_state_cache_record *)lc_calloc_with_allocator(
        &pouch->allocator, 1U, sizeof(*record));
    if (record == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache record", NULL,
                          NULL, NULL);
    }
    record->key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    if (record->key == NULL) {
      lc_pouch_state_cache_record_cleanup(&pouch->allocator, record);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache key", NULL,
                          NULL, NULL);
    }
    record->next = ns->records;
    ns->records = record;
    ++ns->record_count;
    if (ns->record_buckets != NULL && ns->record_bucket_count > 0U) {
      size_t bucket;

      bucket = (size_t)(lc_pouch_state_cache_key_hash(record->key) %
                        ns->record_bucket_count);
      record->bucket_next = ns->record_buckets[bucket];
      ns->record_buckets[bucket] = record;
    }
  }
  key = record->key;
  content_type =
      entry->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->content_type)
          : NULL;
  etag = entry->etag != NULL
             ? lc_strdup_with_allocator(&pouch->allocator, entry->etag)
             : NULL;
  rc = lc_pouch_state_payload_span_copy(&pouch->allocator, &entry->payload_span,
                                        &payload_span, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    return rc;
  }
  rc = lc_pouch_state_record_ref_copy(
      &pouch->allocator, entry->record_container_leaf, entry->record_offset,
      entry->has_record_ref, &record_container_leaf, &record_offset,
      &has_record_ref, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    return rc;
  }
  payload_context =
      entry->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->payload_context)
          : NULL;
  descriptor =
      entry->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->descriptor)
          : NULL;
  if (entry->metadata_length > 0U) {
    metadata = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        entry->metadata_length);
    if (metadata != NULL) {
      memcpy(metadata, entry->metadata, entry->metadata_length);
    }
  }
  if ((entry->content_type != NULL && content_type == NULL) ||
      (entry->etag != NULL && etag == NULL) ||
      (entry->payload_span.present && !payload_span.present) ||
      (entry->has_record_ref && !has_record_ref) ||
      (entry->payload_context != NULL && payload_context == NULL) ||
      (entry->descriptor != NULL && descriptor == NULL) ||
      (entry->metadata_length > 0U && metadata == NULL)) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_pouch_state_record_ref_cleanup(&pouch->allocator, &record_container_leaf,
                                      &record_offset, &has_record_ref);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, descriptor);
    lc_free_with_allocator(&pouch->allocator, metadata);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache record", NULL, NULL,
                        NULL);
  }
  if (record->body_cache != NULL &&
      (!entry->found || !record->found || record->version != entry->version ||
       record->bytes != entry->bytes ||
       record->cipher_bytes != entry->cipher_bytes ||
       !lc_pouch_state_string_equal(record->etag, entry->etag) ||
       record->payload_span.present != entry->payload_span.present ||
       (record->payload_span.present &&
        (strcmp(record->payload_span.container_leaf,
                entry->payload_span.container_leaf) != 0 ||
         record->payload_span.record_offset !=
             entry->payload_span.record_offset ||
         record->payload_span.payload_offset !=
             entry->payload_span.payload_offset ||
         record->payload_span.payload_length !=
             entry->payload_span.payload_length ||
         record->payload_span.payload_crc !=
             entry->payload_span.payload_crc)) ||
       record->has_record_ref != entry->has_record_ref ||
       (record->has_record_ref &&
        (strcmp(record->record_container_leaf, entry->record_container_leaf) !=
             0 ||
         record->record_offset != entry->record_offset)) ||
       !lc_pouch_state_string_equal(record->payload_context,
                                    entry->payload_context) ||
       !lc_pouch_state_string_equal(record->descriptor, entry->descriptor) ||
       record->metadata_length != entry->metadata_length ||
       (record->metadata_length > 0U &&
        memcmp(record->metadata, entry->metadata, record->metadata_length) !=
            0))) {
    lc_pouch_state_cache_record_body_clear(&pouch->allocator, ns, record);
  }
  lc_free_with_allocator(&pouch->allocator, record->content_type);
  lc_free_with_allocator(&pouch->allocator, record->etag);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &record->payload_span);
  lc_pouch_state_record_ref_cleanup(
      &pouch->allocator, &record->record_container_leaf, &record->record_offset,
      &record->has_record_ref);
  lc_free_with_allocator(&pouch->allocator, record->payload_context);
  lc_free_with_allocator(&pouch->allocator, record->descriptor);
  lc_free_with_allocator(&pouch->allocator, record->metadata);
  record->key = key;
  record->content_type = content_type;
  record->etag = etag;
  record->payload_span = payload_span;
  memset(&payload_span, 0, sizeof(payload_span));
  record->record_container_leaf = record_container_leaf;
  record->record_offset = record_offset;
  record->has_record_ref = has_record_ref;
  record_container_leaf = NULL;
  record_offset = 0UL;
  has_record_ref = 0;
  record->payload_context = payload_context;
  record->descriptor = descriptor;
  record->metadata = metadata;
  record->metadata_length = entry->metadata_length;
  record->index_seq = entry->index_seq;
  record->query_index_seq = query_index_seq;
  record->version = entry->version;
  record->bytes = entry->bytes;
  record->cipher_bytes = entry->cipher_bytes;
  record->updated_at_unix = entry->updated_at_unix;
  record->retention = entry->retention;
  record->has_query_hidden = entry->has_query_hidden;
  record->query_hidden = entry->query_hidden;
  record->staged_delete_marker = entry->staged_delete_marker;
  record->found = entry->found;
  if (entry_object_record) {
    record->record_type = entry->found ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                                       : LC_POUCH_STATE_RECORD_OBJECT_DELETE;
  } else if ((entry->record_type == LC_POUCH_STATE_RECORD_STATE_META ||
              entry->record_type == LC_POUCH_STATE_RECORD_STATE_DELETE) &&
             prior_object_record) {
    record->record_type = entry->found ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                                       : LC_POUCH_STATE_RECORD_OBJECT_DELETE;
  } else if (entry->record_type == LC_POUCH_STATE_RECORD_STATE_META) {
    record->record_type = LC_POUCH_STATE_RECORD_STATE_META;
  } else {
    record->record_type = entry->found ? LC_POUCH_STATE_RECORD_STATE_PUT
                                       : LC_POUCH_STATE_RECORD_STATE_DELETE;
  }
  if (entry->index_seq > ns->max_version) {
    ns->max_version = entry->index_seq;
  }
  if (query_index_seq > ns->max_query_index_seq) {
    ns->max_query_index_seq = query_index_seq;
  }
  return LC_OK;
}

static int lc_pouch_state_entry_from_cache_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record,
    lc_pouch_state_entry *out, lc_error *error) {
  memset(out, 0, sizeof(*out));
  if (record == NULL) {
    return LC_OK;
  }
  out->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
  out->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  out->etag = record->etag != NULL
                  ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
                  : NULL;
  if (lc_pouch_state_payload_span_copy(&pouch->allocator, &record->payload_span,
                                       &out->payload_span, error) != LC_OK) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (lc_pouch_state_record_ref_copy(
          &pouch->allocator, record->record_container_leaf,
          record->record_offset, record->has_record_ref,
          &out->record_container_leaf, &out->record_offset,
          &out->has_record_ref, error) != LC_OK) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  out->payload_context =
      record->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_context)
          : NULL;
  out->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (out->metadata != NULL) {
      memcpy(out->metadata, record->metadata, record->metadata_length);
      out->metadata_length = record->metadata_length;
    }
  }
  if (out->key == NULL ||
      (record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->payload_span.present && !out->payload_span.present) ||
      (record->has_record_ref && !out->has_record_ref) ||
      (record->payload_context != NULL && out->payload_context == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL) ||
      (record->metadata_length > 0U && out->metadata == NULL)) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache entry", NULL, NULL,
                        NULL);
  }
  out->index_seq = record->index_seq;
  out->query_index_seq = record->query_index_seq;
  out->has_query_index_seq = 1;
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->retention = record->retention;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->staged_delete_marker = record->staged_delete_marker;
  out->seen = 1;
  out->found = record->found;
  out->record_type = record->record_type;
  return LC_OK;
}

/* The caller owns the cache lifetime and must not clean up this view. */
static void lc_pouch_state_entry_borrow_cache_record(
    const lc_pouch_state_cache_record *record, lc_pouch_state_entry *out) {
  memset(out, 0, sizeof(*out));
  if (record == NULL) {
    return;
  }
  out->key = record->key;
  out->content_type = record->content_type;
  out->etag = record->etag;
  out->payload_span = record->payload_span;
  out->record_container_leaf = record->record_container_leaf;
  out->record_offset = record->record_offset;
  out->has_record_ref = record->has_record_ref;
  out->payload_context = record->payload_context;
  out->descriptor = record->descriptor;
  out->metadata = record->metadata;
  out->metadata_length = record->metadata_length;
  out->index_seq = record->index_seq;
  out->query_index_seq = record->query_index_seq;
  out->has_query_index_seq = 1;
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->retention = record->retention;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->staged_delete_marker = record->staged_delete_marker;
  out->seen = 1;
  out->found = record->found;
  out->record_type = record->record_type;
}

static char *lc_pouch_state_child_path(const lc_allocator *allocator,
                                       const char *namespace_path,
                                       const char *child, const char *leaf) {
  char *dir;
  char *path;

  dir = lc_pouch_path_join(allocator, namespace_path, child);
  path = dir != NULL ? lc_pouch_path_join(allocator, dir, leaf) : NULL;
  lc_free_with_allocator(allocator, dir);
  return path;
}

static char *lc_pouch_state_payload_span_path(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_state_payload_span *span, lc_error *error) {
  const char *child;

  if (namespace_path == NULL || span == NULL || !span->present ||
      !lc_pouch_state_payload_container_is_valid(span->container_leaf)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch payload span path requires a valid span", NULL,
                       NULL, "pouch");
    return NULL;
  }
  child = strncmp(span->container_leaf, "snapshot-", 9U) == 0 ? "snapshots"
                                                              : "segments";
  return lc_pouch_state_child_path(allocator, namespace_path, child,
                                   span->container_leaf);
}

static int lc_pouch_state_write_all(int fd, const void *bytes, size_t count,
                                    lc_error *error) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    ssize_t written;

    written = write(fd, cursor, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state bytes", strerror(errno),
                          NULL, NULL);
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "short write while writing pouch state bytes", NULL,
                          NULL, NULL);
    }
    cursor += written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_throttle(lc_pouch *pouch, size_t bytes,
                                              lc_error *error) {
  struct timespec delay;
  struct timespec remaining;
  uint64_t rate;
  uint64_t seconds;
  uint64_t remainder;
  uint64_t nanoseconds;

  if (pouch == NULL || bytes == 0U ||
      pouch->compaction_max_io_bytes_per_sec == 0U) {
    return LC_OK;
  }
  rate = pouch->compaction_max_io_bytes_per_sec;
  seconds = (uint64_t)bytes / rate;
  remainder = (uint64_t)bytes % rate;
  if (rate <= LC_U64_MAX / 1000000000U) {
    nanoseconds = remainder * 1000000000U / rate;
  } else {
    nanoseconds = remainder / (rate / 1000000000U);
  }
  delay.tv_sec = (time_t)seconds;
  delay.tv_nsec = (long)nanoseconds;
  if (delay.tv_sec == 0 && delay.tv_nsec == 0L) {
    return LC_OK;
  }
  while (nanosleep(&delay, &remaining) != 0) {
    if (errno != EINTR) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to throttle pouch compaction",
                          strerror(errno), NULL, "pouch");
    }
    delay = remaining;
  }
  return LC_OK;
}

static int lc_pouch_state_copy_file_span_to_fd_crc(
    lc_pouch *pouch, const char *path, uint64_t offset, uint64_t length, int fd,
    unsigned char *buffer, unsigned long *stored_crc, lc_error *error) {
  uint64_t remaining;
  int in_fd;
  int rc;

  if (path == NULL || buffer == NULL || stored_crc == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span copy requires path, buffer and crc output",
                        NULL, NULL, "pouch");
  }
  if ((off_t)offset < 0 || (uint64_t)(off_t)offset != offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span copy offset exceeds local limit", NULL,
                        NULL, "pouch");
  }
  in_fd = open(path, O_RDONLY);
  if (in_fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch payload span", strerror(errno),
                        path, "pouch");
  }
  if (lseek(in_fd, (off_t)offset, SEEK_SET) < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to seek pouch payload span", strerror(errno),
                      path, "pouch");
    close(in_fd);
    return rc;
  }
  remaining = length;
  *stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
  rc = LC_OK;
  while (remaining > 0UL) {
    size_t want;
    ssize_t got;

    want = remaining < (uint64_t)LC_POUCH_STATE_FILE_CHUNK_BYTES
               ? (size_t)remaining
               : LC_POUCH_STATE_FILE_CHUNK_BYTES;
    got = read(in_fd, buffer, want);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch payload span", strerror(errno),
                        path, "pouch");
      break;
    }
    if (got == 0) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch payload span is truncated", NULL, path, "pouch");
      break;
    }
    *stored_crc = (unsigned long)crc32((uLong)*stored_crc, buffer, (uInt)got);
    rc = lc_pouch_state_write_all(fd, buffer, (size_t)got, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_compaction_throttle(pouch, (size_t)got, error);
    }
    if (rc != LC_OK) {
      break;
    }
    remaining -= (uint64_t)got;
  }
  if (close(in_fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch payload span", strerror(errno),
                      path, "pouch");
  }
  return rc;
}

static void lc_pouch_state_put16(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
}

static void lc_pouch_state_put32(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
  out[2] = (unsigned char)((value >> 16U) & 0xFFUL);
  out[3] = (unsigned char)((value >> 24U) & 0xFFUL);
}

static void lc_pouch_state_put64(unsigned char *out, uint64_t value) {
  size_t i;

  for (i = 0U; i < 8U; ++i) {
    out[i] = (unsigned char)((value >> (i * 8U)) & 0xFFU);
  }
}

static unsigned long lc_pouch_state_get16(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U);
}

static unsigned long lc_pouch_state_get32(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U) |
         ((unsigned long)in[2] << 16U) | ((unsigned long)in[3] << 24U);
}

static uint64_t lc_pouch_state_get64(const unsigned char *in) {
  uint64_t value;
  size_t i;

  value = 0U;
  for (i = 0U; i < 8U; ++i) {
    value |= ((uint64_t)in[i]) << (i * 8U);
  }
  return value;
}

static int lc_pouch_state_decode_generation(uint64_t value,
                                            lc_pouch_generation *out,
                                            lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation decode requires output", NULL, NULL,
                        "pouch");
  }
  *out = value;
  return LC_OK;
}

static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error);

static lc_pouch_unix_seconds
lc_pouch_state_decode_unix_seconds(uint64_t value) {
  if (value <= (uint64_t)LC_I64_MAX) {
    return (lc_pouch_unix_seconds)value;
  }
  return -1 - (lc_pouch_unix_seconds)(LC_U64_MAX - value);
}

static int lc_pouch_state_record_header_encode(
    unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags,
    unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES],
    lc_error *error) {
  lc_pouch_record_header header;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      key_len > 0xFFFFFFFFUL || meta_len > 0xFFFFFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record prefix is invalid", NULL, NULL,
                        "pouch");
  }
  memset(&header, 0, sizeof(header));
  header.type = type;
  header.flags = flags;
  header.key_len = (unsigned long)key_len;
  header.meta_len = (unsigned long)meta_len;
  header.payload_len = payload_len;
  header.payload_crc = payload_crc;
  lc_pouch_record_header_encode(&header, encoded);
  return LC_OK;
}

static int lc_pouch_state_record_write_header(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  int rc;

  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           payload_len, payload_crc, flags,
                                           encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_write_all(fd, encoded, sizeof(encoded), error);
}

static int lc_pouch_state_writev_all(int fd, const struct iovec *parts,
                                     int part_count, lc_error *error) {
  struct iovec pending[LC_POUCH_STATE_WRITEV_MAX_PARTS];
  int pending_count;
  int index;

  if (parts == NULL || part_count <= 0 ||
      part_count > LC_POUCH_STATE_WRITEV_MAX_PARTS) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch vectored state write requires valid parts", NULL,
                        NULL, "pouch");
  }
  for (index = 0; index < part_count; ++index) {
    pending[index] = parts[index];
  }
  index = 0;
  pending_count = part_count;
  while (pending_count > 0) {
    ssize_t written;
    size_t consumed;

    written = writev(fd, pending + index, pending_count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state record", strerror(errno),
                          NULL, "pouch");
    }
    if (written == 0) {
      errno = EIO;
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state record", strerror(errno),
                          NULL, "pouch");
    }
    consumed = (size_t)written;
    while (pending_count > 0 && consumed >= pending[index].iov_len) {
      consumed -= pending[index].iov_len;
      ++index;
      --pending_count;
    }
    if (pending_count > 0 && consumed > 0U) {
      pending[index].iov_base = (char *)pending[index].iov_base + consumed;
      pending[index].iov_len -= consumed;
    }
  }
  return LC_OK;
}

static int lc_pouch_state_record_write_prefix(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[3];
  int part_count;
  int rc;

  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           payload_len, payload_crc, flags,
                                           encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  part_count = 0;
  parts[part_count].iov_base = encoded;
  parts[part_count].iov_len = sizeof(encoded);
  ++part_count;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  return lc_pouch_state_writev_all(fd, parts, part_count, error);
}

static int lc_pouch_state_record_write_complete(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, const unsigned char *payload,
    size_t payload_len, unsigned long payload_crc, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[4];
  int part_count;
  int rc;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      (payload_len > 0U && payload == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch inline state record is invalid", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           (uint64_t)payload_len, payload_crc,
                                           0UL, encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  part_count = 0;
  parts[part_count].iov_base = encoded;
  parts[part_count].iov_len = sizeof(encoded);
  ++part_count;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  if (payload_len > 0U) {
    parts[part_count].iov_base = (void *)payload;
    parts[part_count].iov_len = payload_len;
    ++part_count;
  }
  return lc_pouch_state_writev_all(fd, parts, part_count, error);
}

/* Batch only already-encoded inline metadata records. A bounded aggregate
 * uses one contiguous append to avoid a syscall per five records; larger
 * batches retain the vectored fallback. Streaming payload records retain their
 * pending-header lifecycle and never enter either path. Fifteen iovecs stays
 * below the POSIX minimum IOV_MAX. */
static int lc_pouch_state_record_write_prefix_batch(
    int fd, const lc_pouch_state_binary_append_item *items, size_t item_count,
    lc_error *error) {
  unsigned char headers[LC_POUCH_STATE_WRITEV_BATCH_RECORDS]
                       [LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[LC_POUCH_STATE_WRITEV_MAX_PARTS];
  unsigned char *inline_bytes;
  size_t inline_length;
  size_t inline_offset;
  size_t item_size;
  size_t batch_count;
  size_t index;
  size_t item_index;
  int part_count;
  int rc;

  if (items == NULL || item_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch vectored record batch requires items", NULL,
                        NULL, "pouch");
  }
  inline_bytes = NULL;
  inline_length = 0U;
  if (item_count > 1U) {
    for (index = 0U; index < item_count; ++index) {
      if (items[index].key_len >
              (size_t)-1 - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
          items[index].meta_len > (size_t)-1 -
                                      LC_POUCH_STATE_RECORD_HEADER_BYTES -
                                      items[index].key_len) {
        inline_length = LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES + 1U;
        break;
      }
      item_size = LC_POUCH_STATE_RECORD_HEADER_BYTES + items[index].key_len +
                  items[index].meta_len;
      if (item_size >
          LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES - inline_length) {
        inline_length = LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES + 1U;
        break;
      }
      inline_length += item_size;
    }
  }
  if (inline_length > 0U &&
      inline_length <= LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES) {
    inline_bytes =
        (unsigned char *)lc_alloc_with_allocator(NULL, inline_length);
    if (inline_bytes != NULL) {
      inline_offset = 0U;
      for (index = 0U; index < item_count; ++index) {
        const lc_pouch_state_binary_append_item *item;

        item = &items[index];
        rc = lc_pouch_state_record_header_encode(
            item->record_type, item->key, item->key_len, item->meta,
            item->meta_len, 0U, 0UL, 0UL, inline_bytes + inline_offset, error);
        if (rc != LC_OK) {
          lc_free_with_allocator(NULL, inline_bytes);
          return rc;
        }
        inline_offset += LC_POUCH_STATE_RECORD_HEADER_BYTES;
        if (item->key_len > 0U) {
          memcpy(inline_bytes + inline_offset, item->key, item->key_len);
          inline_offset += item->key_len;
        }
        if (item->meta_len > 0U) {
          memcpy(inline_bytes + inline_offset, item->meta, item->meta_len);
          inline_offset += item->meta_len;
        }
      }
      rc = lc_pouch_state_write_all(fd, inline_bytes, inline_length, error);
      lc_free_with_allocator(NULL, inline_bytes);
      return rc;
    }
  }
  index = 0U;
  while (index < item_count) {
    batch_count = item_count - index;
    if (batch_count > LC_POUCH_STATE_WRITEV_BATCH_RECORDS) {
      batch_count = LC_POUCH_STATE_WRITEV_BATCH_RECORDS;
    }
    part_count = 0;
    for (item_index = 0U; item_index < batch_count; ++item_index) {
      const lc_pouch_state_binary_append_item *item;

      item = &items[index + item_index];
      rc = lc_pouch_state_record_header_encode(
          item->record_type, item->key, item->key_len, item->meta,
          item->meta_len, 0U, 0UL, 0UL, headers[item_index], error);
      if (rc != LC_OK) {
        return rc;
      }
      parts[part_count].iov_base = headers[item_index];
      parts[part_count].iov_len = LC_POUCH_STATE_RECORD_HEADER_BYTES;
      ++part_count;
      if (item->key_len > 0U) {
        parts[part_count].iov_base = (void *)item->key;
        parts[part_count].iov_len = item->key_len;
        ++part_count;
      }
      if (item->meta_len > 0U) {
        parts[part_count].iov_base = item->meta;
        parts[part_count].iov_len = item->meta_len;
        ++part_count;
      }
    }
    rc = lc_pouch_state_writev_all(fd, parts, part_count, error);
    if (rc != LC_OK) {
      return rc;
    }
    index += batch_count;
  }
  return LC_OK;
}

/* Publish the final header last so active-log readers only see complete data.
 */
static int lc_pouch_state_record_finalize(
    int fd, uint64_t record_offset, unsigned char type, const void *key,
    size_t key_len, const unsigned char *meta, size_t meta_len,
    uint64_t payload_len, unsigned long payload_crc, lc_error *error) {
  struct iovec parts[2];
  int part_count;
  int rc;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      key_len > 0xFFFFFFFFUL || meta_len > 0xFFFFFFFFUL ||
      record_offset > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record finalization is invalid", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_fd_seek(
      fd, record_offset + LC_POUCH_STATE_RECORD_HEADER_BYTES,
      "failed to rewrite pouch state record metadata", error);
  part_count = 0;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  if (rc == LC_OK && part_count > 0) {
    rc = lc_pouch_state_writev_all(fd, parts, part_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_fd_seek(fd, record_offset,
                                "failed to publish pouch state record header",
                                error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_header(fd, type, key, key_len, meta,
                                            meta_len, payload_len, payload_crc,
                                            0UL, error);
  }
  return rc;
}

static int lc_pouch_state_file_size(const char *path, uint64_t *size,
                                    lc_error *error) {
  struct stat st;

  if (stat(path, &st) != 0) {
    if (errno == ENOENT) {
      *size = 0U;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (!S_ISREG(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state segment path is not a file", NULL, NULL,
                        NULL);
  }
  if (st.st_size < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "pouch state segment size is invalid", NULL, path,
                        "pouch");
  }
  *size = (uint64_t)st.st_size;
  return LC_OK;
}

static int lc_pouch_state_file_checksum(const char *path,
                                        uint64_t expected_size,
                                        unsigned char *buffer,
                                        unsigned long *checksum,
                                        lc_error *error) {
  struct stat st;
  uint64_t read_total;
  unsigned long value;
  ssize_t got;
  int fd;

  if (path == NULL || buffer == NULL || checksum == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state checksum requires path, buffer and output",
                        NULL, NULL, NULL);
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch compaction candidate",
                        strerror(errno), NULL, "pouch");
  }
  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  value = (unsigned long)crc32(0L, Z_NULL, 0);
  read_total = 0U;
  for (;;) {
    got = read(fd, buffer, LC_POUCH_STATE_FILE_CHUNK_BYTES);
    if (got < 0) {
      int saved_errno;

      saved_errno = errno;
      (void)close(fd);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch compaction candidate",
                          strerror(saved_errno), NULL, "pouch");
    }
    if (got == 0) {
      break;
    }
    if ((uint64_t)got > expected_size - read_total) {
      (void)close(fd);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction candidate changed", NULL, NULL,
                          "pouch");
    }
    value = (unsigned long)crc32((uLong)value, buffer, (uInt)got);
    read_total += (uint64_t)got;
  }
  if (fstat(fd, &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size || read_total != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch compaction candidate",
                        strerror(errno), NULL, "pouch");
  }
  *checksum = value;
  return LC_OK;
}

static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error) {
  off_t target;

  target = (off_t)offset;
  if (target < 0 || (uint64_t)target != offset) {
    errno = EINVAL;
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  if (lseek(fd, target, SEEK_SET) < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_fd_end(int fd, uint64_t *offset, const char *message,
                                 lc_error *error) {
  off_t end;

  if (offset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch file end requires an output", NULL, NULL,
                        "pouch");
  }
  end = lseek(fd, 0, SEEK_END);
  if (end < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  *offset = (uint64_t)end;
  return LC_OK;
}

static void lc_pouch_state_truncate_fd_best_effort(int fd, uint64_t size) {
  int ignored;

  if ((off_t)size < 0 || (uint64_t)(off_t)size != size) {
    return;
  }
  ignored = ftruncate(fd, (off_t)size);
  (void)ignored;
}

static void lc_pouch_state_append_fd_rollback(int fd, uint64_t size,
                                              int retain_fd) {
  lc_pouch_state_truncate_fd_best_effort(fd, size);
  if (retain_fd) {
    if ((off_t)size >= 0 && (uint64_t)(off_t)size == size) {
      (void)lseek(fd, (off_t)size, SEEK_SET);
    }
  } else {
    (void)close(fd);
  }
}

static void lc_pouch_state_truncate_path_best_effort(const char *path,
                                                     uint64_t size) {
  int ignored;

  if (path == NULL) {
    return;
  }
  if ((off_t)size < 0 || (uint64_t)(off_t)size != size) {
    return;
  }
  ignored = truncate(path, (off_t)size);
  (void)ignored;
}

static int lc_pouch_state_truncate_path(const char *path, uint64_t size,
                                        lc_error *error) {
  off_t offset;

  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair requires a path", NULL, NULL,
                        "pouch");
  }
  offset = (off_t)size;
  if (offset < 0 || (uint64_t)offset != size) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair exceeds local offset range",
                        NULL, NULL, "pouch");
  }
  if (truncate(path, offset) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to repair pouch active segment tail",
                        strerror(errno), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_meta_set_index_sequences(
    const lc_allocator *allocator, unsigned char **meta_io,
    size_t *meta_length_io, lc_pouch_generation index_seq,
    lc_pouch_generation query_index_seq, lc_error *error);
static int lc_pouch_state_meta_trailer(const unsigned char *meta,
                                       size_t meta_len, size_t *trailer_offset,
                                       int *has_query_index_seq,
                                       lc_error *error);

static int lc_pouch_state_reserve_index_records(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned long count, int query_index_changed,
    lc_pouch_generation *first_index_out, lc_error *error) {
  char *lock_path;
  char *sequence_path;
  struct flock fl;
  char line[96];
  char end_index_text[32];
  FILE *fp;
  lc_u64 parsed;
  lc_pouch_generation base;
  lc_pouch_generation end_index;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_process_namespace_mutex *sequence_mutex;
  int fd;
  int scan_required;
  int shared_mutation_held;
  int single_writer;
  int rc;

  if (first_index_out != NULL) {
    *first_index_out = 0UL;
  }
  if (pouch == NULL || ns == NULL || manifest == NULL ||
      manifest->namespace_path == NULL || count == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state index reservation requires context", NULL,
                        NULL, "pouch");
  }
  single_writer = lc_pouch_single_writer_enabled(pouch);
  if (single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
    if (cache == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_state_clean_checkpoint_invalidate(
        pouch, cache, manifest->namespace_path, query_index_changed, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (cache != NULL && cache->initialized) {
      base = manifest->state_max_version;
      if (cache->max_version > base) {
        base = cache->max_version;
      }
      if (base > LC_U64_MAX - (uint64_t)count) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch state index sequence overflow", NULL, NULL,
                            "pouch");
      }
      end_index = base + (uint64_t)count;
      cache->max_version = end_index;
      manifest->state_max_version = end_index;
      if (first_index_out != NULL) {
        *first_index_out = base + 1UL;
      }
      /* The exclusive owner has one resident allocator, so its projection is
       * authoritative while live. A normal close checkpoints this value for
       * the next cold index read; abort or recovery replays finalized records.
       * The sequence file remains a shared-root allocator. */
      return LC_OK;
    }
  }
  shared_mutation_held =
      !single_writer && lc_pouch_state_shared_mutation_is_held(pouch);
  if (!single_writer && !shared_mutation_held) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "shared pouch index reservation requires root mutation "
                        "authority",
                        NULL, NULL, "pouch");
  }
  sequence_mutex = NULL;
  lock_path = NULL;
  fd = -1;
  sequence_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "sequence");
  if (sequence_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, sequence_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state sequence path", NULL,
                        NULL, "pouch");
  }
  if (!shared_mutation_held) {
    rc = lc_pouch_state_process_namespace_mutex_lock(pouch, ns, &sequence_mutex,
                                                     error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, sequence_path);
      return rc;
    }
    lock_path = lc_pouch_path_join(&pouch->allocator, manifest->namespace_path,
                                   "sequence.lock");
    if (lock_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, sequence_path);
      lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state sequence lock path",
                          NULL, NULL, "pouch");
    }
    fd = open(lock_path, O_CREAT | O_RDWR, 0666);
    lc_free_with_allocator(&pouch->allocator, lock_path);
    if (fd < 0) {
      lc_free_with_allocator(&pouch->allocator, sequence_path);
      lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to open pouch state sequence lock",
                          strerror(errno), NULL, "pouch");
    }
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    while (fcntl(fd, F_SETLKW, &fl) != 0) {
      if (errno == EINTR) {
        continue;
      }
      close(fd);
      lc_free_with_allocator(&pouch->allocator, sequence_path);
      lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch state sequence",
                          strerror(errno), NULL, "pouch");
    }
  }
  base = manifest->state_max_version;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  scan_required = cache == NULL || !cache->initialized;
  if (cache != NULL && cache->initialized && cache->max_version > base) {
    base = cache->max_version;
  }
  fp = fopen(sequence_path, "rb");
  if (fp != NULL) {
    int sequence_valid;

    sequence_valid =
        fgets(line, sizeof(line), fp) != NULL && strncmp(line, "max=", 4U) == 0;
    if (sequence_valid) {
      sequence_valid = lc_parse_u64_base10_range_checked(
          line + 4U, strlen(line + 4U), &parsed);
    }
    if (fclose(fp) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch state sequence", strerror(errno),
                        NULL, "pouch");
      goto cleanup;
    }
    if (!sequence_valid) {
      /* The sequence file is advisory and may be torn after a crash. */
      scan_required = 1;
    } else if ((lc_pouch_generation)parsed > base) {
      base = (lc_pouch_generation)parsed;
    }
  } else if (errno != ENOENT) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch state sequence", strerror(errno),
                      NULL, "pouch");
    goto cleanup;
  } else {
    scan_required = 1;
  }
  if (scan_required) {
    lc_pouch_generation durable_max;

    rc = lc_pouch_state_manifest_max_version(pouch, manifest, &durable_max,
                                             error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    if (durable_max > base) {
      base = durable_max;
    }
  }
  if (base > LC_U64_MAX - (uint64_t)count) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch state index sequence overflow", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  end_index = base + (uint64_t)count;
  if (lc_u64_format_base10((lc_u64)end_index, end_index_text,
                           sizeof(end_index_text)) < 0 ||
      snprintf(line, sizeof(line), "max=%s\n", end_index_text) < 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "failed to format pouch state sequence", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  {
    int sequence_fd;

    sequence_fd = open(sequence_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (sequence_fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch state sequence", strerror(errno),
                        NULL, "pouch");
      goto cleanup;
    }
    rc = lc_pouch_state_write_all(sequence_fd, line, strlen(line), error);
    if (close(sequence_fd) != 0 && rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch state sequence", strerror(errno),
                        NULL, "pouch");
    }
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  manifest->state_max_version = end_index;
  if (first_index_out != NULL) {
    *first_index_out = base + 1UL;
  }
  rc = LC_OK;
cleanup:
  if (fd >= 0) {
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(fd, F_SETLK, &fl);
    (void)close(fd);
  }
  lc_free_with_allocator(&pouch->allocator, sequence_path);
  lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
  return rc;
}

static int lc_pouch_state_append_binary_records_locked(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_binary_append_item *items, size_t item_count,
    lc_error *error) {
  char *segment_path;
  lc_pouch_namespace_logstore *cache;
  int fd;
  int rc;
  int manifest_from_cache;
  int append_started;
  int retain_active_append_fd;
  int single_writer;
  lc_pouch_generation first_index;
  lc_pouch_generation reserved_state_max_version;
  uint64_t record_size;
  uint64_t segment_size;
  uint64_t appended_end;
  size_t index;
  int query_index_changed;

  if (pouch == NULL || ns == NULL || manifest == NULL || items == NULL ||
      item_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary record batch append requires inputs",
                        NULL, NULL, "pouch");
  }
  segment_path = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_enabled(pouch);
  manifest_from_cache =
      single_writer && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL && manifest->active_segment != NULL &&
      manifest->segment_leaves == NULL &&
      strcmp(cache->active_segment_leaf, manifest->active_segment) == 0;
  retain_active_append_fd = 0;
  append_started = 0;
  first_index = 0UL;
  reserved_state_max_version = 0UL;
  record_size = 0U;
  segment_size = 0U;
  appended_end = 0U;
  query_index_changed = 0;
  rc = LC_OK;
  for (index = 0U; index < item_count; ++index) {
    size_t trailer_offset;
    int has_query_index_seq;

    rc = lc_pouch_state_meta_trailer(items[index].meta, items[index].meta_len,
                                     &trailer_offset, &has_query_index_seq,
                                     error);
    if (rc != LC_OK || !has_query_index_seq) {
      if (rc == LC_OK) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch binary record lacks query index sequence",
                          NULL, NULL, "pouch");
      }
      return rc;
    }
    if ((items[index].key_len > 0U && items[index].key == NULL) ||
        (items[index].meta_len > 0U && items[index].meta == NULL) ||
        items[index].key_len > 0xFFFFFFFFUL ||
        items[index].meta_len > 0xFFFFFFFFUL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch item is invalid", NULL,
                          NULL, "pouch");
    }
    if (record_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                          (uint64_t)items[index].key_len -
                          (uint64_t)items[index].meta_len) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch exceeds local limit", NULL,
                          NULL, "pouch");
    }
    record_size += LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (uint64_t)items[index].key_len +
                   (uint64_t)items[index].meta_len;
    if (items[index].query_index_from_index) {
      query_index_changed = 1;
    }
  }
  if (rc == LC_OK && !manifest_from_cache) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
    memset(manifest, 0, sizeof(*manifest));
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          ns, manifest, NULL, NULL, error);
  }
  if (rc == LC_OK && !manifest_from_cache) {
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    if (segment_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK && !single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
    }
  }
  if (rc == LC_OK &&
      !(single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0)) {
    rc = lc_pouch_state_repair_active_tail_locked(
        pouch, ns, manifest, segment_path, manifest->active_segment, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_reserve_index_records(
        pouch, ns, manifest, (unsigned long)item_count, query_index_changed,
        &first_index, error);
    if (rc == LC_OK) {
      reserved_state_max_version = manifest->state_max_version;
    }
  }
  for (index = 0U; index < item_count; ++index) {
    if (rc == LC_OK) {
      lc_pouch_generation record_index;
      lc_pouch_generation query_index_seq;

      record_index = first_index + (lc_pouch_generation)index;
      query_index_seq = items[index].query_index_from_index
                            ? record_index
                            : items[index].query_index_seq;
      rc = lc_pouch_state_meta_set_index_sequences(
          &pouch->allocator, &items[index].meta, &items[index].meta_len,
          record_index, query_index_seq, error);
    }
  }
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
      segment_size = cache->active_segment_offset;
    } else {
      rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
    }
  }
  if (rc == LC_OK && segment_size > 0U &&
      (record_size > LC_U64_MAX - segment_size ||
       segment_size + record_size > pouch->segment_target_bytes)) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    segment_path = NULL;
    rc = lc_pouch_state_manifest_materialize(pouch, ns, manifest,
                                             &manifest_from_cache, error);
    /* Reservation advances the active in-memory projection before a segment
     * rotation persists the manifest. Materializing here reloads the older
     * durable manifest, so retain the reservation high-water before writing
     * the new topology. Otherwise the following cache publication can carry
     * an older sequence than the active lease record and be ignored. */
    if (rc == LC_OK &&
        manifest->state_max_version < reserved_state_max_version) {
      manifest->state_max_version = reserved_state_max_version;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, ns, manifest,
                                              manifest->active_segment_id + 1U,
                                              error);
    }
    if (rc == LC_OK) {
      segment_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                    "segments", manifest->active_segment);
      if (segment_path == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK && cache != NULL && cache->initialized) {
      rc = lc_pouch_state_cache_set_active_segment(pouch, cache, manifest, 0U,
                                                   error);
    }
    segment_size = 0U;
  }
  fd = -1;
  if (rc == LC_OK) {
    if (single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
      rc = lc_pouch_state_cache_active_append_fd(pouch, cache, manifest, &fd,
                                                 error);
      retain_active_append_fd = rc == LC_OK ? 1 : 0;
    } else {
      fd = open(segment_path, O_WRONLY | O_CREAT | O_APPEND, 0666);
    }
    if (rc == LC_OK && fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    /* Finalized metadata records are replayable immediately. Arm rollback
     * before any write so partial writev failures discard this batch too. */
    append_started = 1;
    rc = lc_pouch_state_record_write_prefix_batch(fd, items, item_count, error);
  }
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && lc_pouch_test_after_metadata_batch_append_hook != NULL) {
    rc = lc_pouch_test_after_metadata_batch_append_hook(
        lc_pouch_test_after_metadata_batch_append_context, error);
  }
#endif
  if (rc == LC_OK) {
    rc = lc_pouch_state_defer_fsync(pouch, fd, error);
  }
  if (rc == LC_OK && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
    lc_error cache_error;

    lc_error_init(&cache_error);
    if (lc_pouch_state_fd_end(fd, &appended_end,
                              "failed to read pouch state segment append end",
                              &cache_error) == LC_OK) {
      cache->active_segment_offset = appended_end;
    } else {
      lc_pouch_state_cache_invalidate_namespace(pouch, ns);
    }
    lc_error_cleanup(&cache_error);
  }
  if (!retain_active_append_fd && fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc != LC_OK && append_started) {
    if (retain_active_append_fd) {
      lc_pouch_state_append_fd_rollback(fd, segment_size, 1);
    } else {
      lc_pouch_state_truncate_path_best_effort(segment_path, segment_size);
    }
  }
  lc_free_with_allocator(&pouch->allocator, segment_path);
  return rc;
}

/* The physical append gate must precede the projection mutex. Every ordinary
 * caller enters under an exact-key mutation and therefore returns with that
 * mutex restored; namespace callbacks retain their stronger authority and use
 * the same in-process append gate recursively. */
static int
lc_pouch_state_append_binary_records(lc_pouch *pouch, const char *ns,
                                     lc_pouch_namespace_manifest *manifest,
                                     lc_pouch_state_binary_append_item *items,
                                     size_t item_count, lc_error *error) {
  lc_pouch_state_append_lock append_lock;
  int rc;

  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  rc = lc_pouch_state_append_lock_enter_after_mutation(pouch, ns, &append_lock,
                                                       error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_binary_records_locked(pouch, ns, manifest, items,
                                                     item_count, error);
  }
  lc_pouch_state_append_lock_release(&append_lock);
  return rc;
}

static int lc_pouch_state_append_binary_record(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned char record_type, const void *key, size_t key_len,
    unsigned char *meta, size_t meta_len, lc_pouch_generation query_index_seq,
    int query_index_from_index, lc_error *error) {
  lc_pouch_state_binary_append_item item;

  memset(&item, 0, sizeof(item));
  item.record_type = record_type;
  item.key = key;
  item.key_len = key_len;
  item.meta = meta;
  item.meta_len = meta_len;
  item.query_index_seq = query_index_seq;
  item.query_index_from_index = query_index_from_index;
  return lc_pouch_state_append_binary_records(pouch, ns, manifest, &item, 1U,
                                              error);
}

static char *lc_pouch_state_bytes_to_string(const lc_allocator *allocator,
                                            const unsigned char *bytes,
                                            size_t length, lc_error *error) {
  char *out;

  if (length > 0U && bytes == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch state string bytes are invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  if (length > 0U && memchr(bytes, '\0', length) != NULL) {
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "pouch state metadata string contains NUL", NULL, NULL,
                       "pouch");
    return NULL;
  }
  out = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (out == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state metadata string", NULL,
                       NULL, NULL);
    return NULL;
  }
  if (length > 0U) {
    memcpy(out, bytes, length);
  }
  out[length] = '\0';
  return out;
}

static int lc_pouch_state_meta_new(const lc_allocator *allocator, size_t length,
                                   unsigned char **out, lc_error *error) {
  *out = NULL;
  if (length > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata exceeds local limit", NULL, NULL,
                        "pouch");
  }
  *out = (unsigned char *)lc_calloc_with_allocator(allocator, length, 1U);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state metadata", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int lc_pouch_state_meta_trailer(const unsigned char *meta,
                                       size_t meta_len, size_t *trailer_offset,
                                       int *has_query_index_seq,
                                       lc_error *error) {
  size_t legacy_trailer_offset;
  size_t query_trailer_offset;

  if (meta == NULL || trailer_offset == NULL || has_query_index_seq == NULL ||
      meta_len < LC_POUCH_STATE_INDEX_TRAILER_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata has no index trailer", NULL, NULL,
                        "pouch");
  }
  if (meta_len >= LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES) {
    legacy_trailer_offset = meta_len - LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES;
    query_trailer_offset = meta_len - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
    if (lc_pouch_state_get32(meta + legacy_trailer_offset) ==
            LC_POUCH_STATE_INDEX_TRAILER_MAGIC &&
        lc_pouch_state_get32(meta + query_trailer_offset) ==
            LC_POUCH_STATE_QUERY_INDEX_TRAILER_MAGIC) {
      *trailer_offset = legacy_trailer_offset;
      *has_query_index_seq = 1;
      return LC_OK;
    }
  }
  *trailer_offset = meta_len - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  if (lc_pouch_state_get32(meta + *trailer_offset) !=
      LC_POUCH_STATE_INDEX_TRAILER_MAGIC) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index trailer is invalid", NULL,
                        NULL, "pouch");
  }
  *has_query_index_seq = 0;
  return LC_OK;
}

static int lc_pouch_state_meta_set_index_seq(unsigned char *meta,
                                             size_t meta_len,
                                             lc_pouch_generation index_seq,
                                             lc_error *error) {
  size_t trailer_offset;
  int has_query_index_seq;
  int rc;

  rc = lc_pouch_state_meta_trailer(meta, meta_len, &trailer_offset,
                                   &has_query_index_seq, error);
  if (rc != LC_OK) {
    return rc;
  }
  (void)has_query_index_seq;
  lc_pouch_state_put64(meta + trailer_offset + 4U, (uint64_t)index_seq);
  return LC_OK;
}

static int lc_pouch_state_meta_index_seq(const unsigned char *meta,
                                         size_t meta_len,
                                         lc_pouch_generation *out,
                                         lc_error *error) {
  size_t trailer_offset;
  int has_query_index_seq;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index trailer is missing", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_meta_trailer(meta, meta_len, &trailer_offset,
                                   &has_query_index_seq, error);
  if (rc != LC_OK) {
    return rc;
  }
  (void)has_query_index_seq;
  rc = lc_pouch_state_decode_generation(
      lc_pouch_state_get64(meta + trailer_offset + 4U), out, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (*out == 0UL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index sequence is invalid", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_meta_query_index_seq(const unsigned char *meta,
                                               size_t meta_len,
                                               lc_pouch_generation *out,
                                               int *present, lc_error *error) {
  size_t trailer_offset;
  int has_query_index_seq;
  int rc;

  if (out == NULL || present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index metadata requires outputs", NULL,
                        NULL, "pouch");
  }
  *out = 0UL;
  *present = 0;
  rc = lc_pouch_state_meta_trailer(meta, meta_len, &trailer_offset,
                                   &has_query_index_seq, error);
  if (rc != LC_OK || !has_query_index_seq) {
    return rc;
  }
  rc = lc_pouch_state_decode_generation(
      lc_pouch_state_get64(meta + trailer_offset + 16U), out, error);
  if (rc == LC_OK) {
    *present = 1;
  }
  return rc;
}

static int lc_pouch_state_meta_set_index_sequences(
    const lc_allocator *allocator, unsigned char **meta_io,
    size_t *meta_length_io, lc_pouch_generation index_seq,
    lc_pouch_generation query_index_seq, lc_error *error) {
  unsigned char *extended;
  size_t trailer_offset;
  int has_query_index_seq;
  int rc;

  trailer_offset = 0U;
  has_query_index_seq = 0;

  if (meta_io == NULL || meta_length_io == NULL || *meta_io == NULL ||
      *meta_length_io > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata cannot retain query sequence",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_state_meta_trailer(*meta_io, *meta_length_io, &trailer_offset,
                                   &has_query_index_seq, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (has_query_index_seq) {
    rc = lc_pouch_state_meta_set_index_seq(*meta_io, *meta_length_io, index_seq,
                                           error);
    if (rc == LC_OK) {
      lc_pouch_state_put64(*meta_io + trailer_offset + 16U,
                           (uint64_t)query_index_seq);
    }
    return rc;
  }
  if (*meta_length_io > LC_POUCH_STATE_RECORD_META_MAX_BYTES -
                            LC_POUCH_STATE_INDEX_TRAILER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata cannot retain query sequence",
                        NULL, NULL, "pouch");
  }
  extended = (unsigned char *)lc_alloc_with_allocator(
      allocator, *meta_length_io + LC_POUCH_STATE_INDEX_TRAILER_BYTES);
  if (extended == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to extend pouch state metadata", NULL, NULL,
                        NULL);
  }
  if (trailer_offset + LC_POUCH_STATE_INDEX_TRAILER_BYTES != *meta_length_io) {
    lc_free_with_allocator(allocator, extended);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch snapshot metadata legacy trailer is invalid",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_state_meta_set_index_seq(*meta_io, *meta_length_io, index_seq,
                                         error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, extended);
    return rc;
  }
  memcpy(extended, *meta_io, *meta_length_io);
  lc_pouch_state_put32(extended + *meta_length_io,
                       LC_POUCH_STATE_QUERY_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(extended + *meta_length_io + 4U,
                       (uint64_t)query_index_seq);
  lc_free_with_allocator(allocator, *meta_io);
  *meta_io = extended;
  *meta_length_io += LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  return LC_OK;
}

static int lc_pouch_state_encode_payload_meta_into(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, uint64_t plain_bytes,
    uint64_t stored_bytes, const char *content_type, const char *etag,
    const char *descriptor, const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, size_t descriptor_reserve, int has_query_hidden,
    int query_hidden, int staged_delete_marker, lc_pouch_unix_seconds retention,
    unsigned char *reserved, size_t reserved_length, unsigned char **out,
    size_t *out_length, lc_error *error) {
  char ref_container[64];
  uint64_t ref_record_offset;
  uint64_t ref_offset;
  uint64_t ref_length;
  unsigned long ref_crc;
  size_t content_type_len;
  size_t etag_len;
  size_t descriptor_len;
  size_t descriptor_capacity;
  size_t ref_container_len;
  size_t payload_context_len;
  size_t metadata_trailer_len;
  size_t length;
  unsigned char *meta;
  unsigned char *cursor;

  if (content_type == NULL || etag == NULL || out == NULL ||
      out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload metadata requires inputs", NULL,
                        NULL, "pouch");
  }
  ref_container[0] = '\0';
  ref_record_offset = 0U;
  ref_offset = 0U;
  ref_length = 0U;
  ref_crc = 0UL;
  if (payload_span != NULL && payload_span->present) {
    if (!lc_pouch_state_payload_container_is_valid(
            payload_span->container_leaf)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state payload span is invalid", NULL, NULL,
                          "pouch");
    }
    if (strlen(payload_span->container_leaf) >= sizeof(ref_container)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state payload container exceeds local limit",
                          NULL, NULL, "pouch");
    }
    strcpy(ref_container, payload_span->container_leaf);
    ref_record_offset = payload_span->record_offset;
    ref_offset = payload_span->payload_offset;
    ref_length = payload_span->payload_length;
    ref_crc = payload_span->payload_crc;
  }
  content_type_len = strlen(content_type);
  etag_len = strlen(etag);
  descriptor_len = descriptor != NULL ? strlen(descriptor) : 0U;
  descriptor_capacity =
      descriptor_reserve > descriptor_len ? descriptor_reserve : descriptor_len;
  ref_container_len = ref_container[0] != '\0' ? strlen(ref_container) : 0U;
  payload_context_len = payload_context != NULL && payload_context[0] != '\0'
                            ? strlen(payload_context)
                            : 0U;
  if (metadata_length > 0U && metadata == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state side metadata requires bytes", NULL, NULL,
                        "pouch");
  }
  if (content_type_len > 0xFFFFU || etag_len > 0xFFFFU ||
      descriptor_capacity > 0xFFFFFFFFUL || ref_container_len > 0xFFFFU ||
      payload_context_len > 0xFFFFU || metadata_length > 0xFFFFFFFFUL ||
      ref_crc > 0xFFFFFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata field exceeds local limit", NULL,
                        NULL, "pouch");
  }
  metadata_trailer_len = 4U + metadata_length + (retention != 0 ? 8U : 0U);
  length = LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_len +
           etag_len + descriptor_capacity + ref_container_len +
           payload_context_len + metadata_trailer_len +
           LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  if (reserved != NULL) {
    if (length != reserved_length)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state metadata reservation changed size", NULL,
                          "collection_corrupt", "pouch");
    meta = reserved;
    memset(meta, 0, length);
  } else if (lc_pouch_state_meta_new(allocator, length, &meta, error) !=
             LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put64(meta + 16, plain_bytes);
  lc_pouch_state_put64(meta + 24, stored_bytes);
  lc_pouch_state_put64(meta + 32, ref_record_offset);
  lc_pouch_state_put64(meta + 40, ref_offset);
  lc_pouch_state_put64(meta + 48, ref_length);
  lc_pouch_state_put32(meta + 56, ref_crc);
  meta[60] =
      has_query_hidden ? LC_POUCH_STATE_PAYLOAD_FLAG_HAS_QUERY_HIDDEN : 0U;
  if (query_hidden) {
    meta[60] |= LC_POUCH_STATE_PAYLOAD_FLAG_QUERY_HIDDEN;
  }
  if (staged_delete_marker) {
    meta[60] |= LC_POUCH_STATE_PAYLOAD_FLAG_STAGED_DELETE;
  }
  if (retention != 0) {
    meta[60] |= LC_POUCH_STATE_PAYLOAD_FLAG_RETENTION;
  }
  lc_pouch_state_put16(meta + 62, (unsigned long)content_type_len);
  lc_pouch_state_put16(meta + 64, (unsigned long)etag_len);
  lc_pouch_state_put32(meta + 66, (unsigned long)descriptor_len);
  lc_pouch_state_put16(meta + 70, (unsigned long)ref_container_len);
  lc_pouch_state_put16(meta + 72, (unsigned long)payload_context_len);
  cursor = meta + LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES;
  memcpy(cursor, content_type, content_type_len);
  cursor += content_type_len;
  memcpy(cursor, etag, etag_len);
  cursor += etag_len;
  if (descriptor_len > 0U) {
    memcpy(cursor, descriptor, descriptor_len);
  }
  cursor += descriptor_len;
  if (ref_container_len > 0U) {
    memcpy(cursor, ref_container, ref_container_len);
  }
  cursor += ref_container_len;
  if (payload_context_len > 0U) {
    memcpy(cursor, payload_context, payload_context_len);
  }
  cursor += payload_context_len;
  lc_pouch_state_put32(cursor, (unsigned long)metadata_length);
  cursor += 4U;
  if (metadata_length > 0U) {
    memcpy(cursor, metadata, metadata_length);
    cursor += metadata_length;
  }
  if (retention != 0) {
    lc_pouch_state_put64(cursor, (uint64_t)retention);
  }
  lc_pouch_state_put32(meta + length - LC_POUCH_STATE_INDEX_TRAILER_BYTES,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + length - 8U, 0U);
  *out = meta;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_state_encode_payload_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, uint64_t plain_bytes,
    uint64_t stored_bytes, const char *content_type, const char *etag,
    const char *descriptor, const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, size_t descriptor_reserve, int has_query_hidden,
    int query_hidden, int staged_delete_marker, lc_pouch_unix_seconds retention,
    unsigned char **out, size_t *out_length, lc_error *error) {
  return lc_pouch_state_encode_payload_meta_into(
      allocator, version, updated_at_unix, plain_bytes, stored_bytes,
      content_type, etag, descriptor, payload_span, payload_context, metadata,
      metadata_length, descriptor_reserve, has_query_hidden, query_hidden,
      staged_delete_marker, retention, NULL, 0, out, out_length, error);
}

static int lc_pouch_state_encode_delete_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, const char *etag,
    unsigned char **out, size_t *out_length, lc_error *error) {
  size_t etag_len;
  unsigned char *meta;

  if (etag == NULL || out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete metadata requires etag and outputs", NULL,
                        NULL, "pouch");
  }
  etag_len = strlen(etag);
  if (etag_len > 0xFFFFU) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete metadata etag exceeds local limit", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(
          allocator, 18U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES, &meta,
          error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  memcpy(meta + 18, etag, etag_len);
  lc_pouch_state_put32(meta + 18U + etag_len,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + 22U + etag_len, 0U);
  *out = meta;
  *out_length = 18U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  return LC_OK;
}

static int lc_pouch_state_encode_decision_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    const char *etag, const char *decision, unsigned char **out,
    size_t *out_length, lc_error *error) {
  size_t etag_len;
  unsigned char decision_code;
  unsigned char *meta;

  if (etag == NULL || decision == NULL || out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata requires inputs", NULL, NULL,
                        "pouch");
  }
  if (strcmp(decision, LC_POUCH_STATE_DECISION_COMMITTED) == 0) {
    decision_code = 1U;
  } else if (strcmp(decision, LC_POUCH_STATE_DECISION_DISCARDED) == 0) {
    decision_code = 2U;
  } else {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata decision is invalid", NULL,
                        NULL, "pouch");
  }
  etag_len = strlen(etag);
  if (etag_len > 0xFFFFU) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata etag exceeds local limit",
                        NULL, NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(
          allocator, 19U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES, &meta,
          error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, 0U);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  meta[18] = decision_code;
  memcpy(meta + 19, etag, etag_len);
  lc_pouch_state_put32(meta + 19U + etag_len,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + 23U + etag_len, 0U);
  *out = meta;
  *out_length = 19U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  return LC_OK;
}

/* Both ordinary and collection-owned states use this retention trailer.
 * The caller supplies the end of canonical side metadata, before padding. */
static int lc_pouch_state_decode_retention_meta(const unsigned char *meta,
                                                size_t meta_len,
                                                size_t consumed_len, int staged,
                                                lc_pouch_unix_seconds *out,
                                                lc_error *error) {
  size_t trailer_offset = 0U;
  int has_query_sequence;
  int rc;
  *out = 0;
  if ((meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_RETENTION) == 0U)
    return LC_OK;
  rc = lc_pouch_state_meta_trailer(meta, meta_len, &trailer_offset,
                                   &has_query_sequence, error);
  if (rc != LC_OK)
    return rc;
  if (consumed_len > trailer_offset || trailer_offset - consumed_len < 8U)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state retention metadata is truncated", NULL,
                        NULL, "pouch");
  *out = lc_pouch_state_decode_unix_seconds(
      lc_pouch_state_get64(meta + consumed_len));
  if (*out == LC_I64_MIN || (*out < 0 && !staged))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state retention metadata is invalid", NULL, NULL,
                        "pouch");
  return LC_OK;
}

static int lc_pouch_state_encode_high_water_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_generation query_version, unsigned char **out, size_t *out_length,
    lc_error *error) {
  unsigned char *meta;

  if (out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch high-water metadata requires outputs", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(allocator, 16U, &meta, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)query_version);
  *out = meta;
  *out_length = 16U;
  return LC_OK;
}

static int lc_pouch_state_decode_payload_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, unsigned char record_type,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc,
    lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_generation version;
  uint64_t plain_bytes;
  uint64_t stored_bytes;
  uint64_t ref_record_offset;
  uint64_t ref_offset;
  uint64_t ref_length;
  unsigned long ref_crc;
  unsigned long content_type_len;
  unsigned long etag_len;
  unsigned long descriptor_len;
  unsigned long ref_container_len;
  unsigned long payload_context_len;
  unsigned long side_metadata_len;
  unsigned long consumed_len;
  const unsigned char *cursor;
  char *ref_container;
  uint64_t value;
  int rc;

  version = 0UL;
  plain_bytes = 0U;
  stored_bytes = 0U;
  ref_record_offset = 0U;
  ref_offset = 0U;
  ref_length = 0U;
  ref_crc = 0UL;
  content_type_len = 0UL;
  etag_len = 0UL;
  descriptor_len = 0UL;
  ref_container_len = 0UL;
  payload_context_len = 0UL;
  side_metadata_len = 0UL;
  consumed_len = 0UL;
  ref_container = NULL;
  if (meta_len < LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata is truncated", NULL, NULL,
                        "pouch");
  }
  value = lc_pouch_state_get64(meta);
  rc = lc_pouch_state_decode_generation(value, &version, error);
  if (rc != LC_OK) {
    return rc;
  }
  plain_bytes = lc_pouch_state_get64(meta + 16);
  stored_bytes = lc_pouch_state_get64(meta + 24);
  ref_record_offset = lc_pouch_state_get64(meta + 32);
  ref_offset = lc_pouch_state_get64(meta + 40);
  ref_length = lc_pouch_state_get64(meta + 48);
  ref_crc = lc_pouch_state_get32(meta + 56);
  content_type_len = lc_pouch_state_get16(meta + 62);
  etag_len = lc_pouch_state_get16(meta + 64);
  descriptor_len = lc_pouch_state_get32(meta + 66);
  ref_container_len = lc_pouch_state_get16(meta + 70);
  payload_context_len = lc_pouch_state_get16(meta + 72);
  if ((uint64_t)LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_len +
          etag_len + descriptor_len + ref_container_len + payload_context_len >
      (uint64_t)meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata variable fields are truncated",
                        NULL, NULL, "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  cursor = meta + LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES;
  entry->content_type = lc_pouch_state_bytes_to_string(
      allocator, cursor, (size_t)content_type_len, error);
  cursor += content_type_len;
  entry->etag = lc_pouch_state_bytes_to_string(allocator, cursor,
                                               (size_t)etag_len, error);
  cursor += etag_len;
  entry->descriptor =
      descriptor_len > 0UL
          ? lc_pouch_state_bytes_to_string(allocator, cursor,
                                           (size_t)descriptor_len, error)
          : NULL;
  cursor += descriptor_len;
  ref_container = NULL;
  ref_container = lc_pouch_state_bytes_to_string(
      allocator, cursor, (size_t)ref_container_len, error);
  if (ref_container == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload ref", NULL,
                        NULL, "pouch");
  }
  if (ref_container[0] == '\0' &&
      record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
      record_type != LC_POUCH_STATE_RECORD_OBJECT_PUT &&
      ref_record_offset == 0UL && ref_offset == 0UL && ref_length == 0UL &&
      ref_crc == 0UL && stored_bytes == 0UL) {
    cursor += ref_container_len;
    lc_free_with_allocator(allocator, ref_container);
    ref_container = NULL;
    goto payload_ref_decoded;
  }
  if (ref_container[0] == '\0' ||
      !lc_pouch_state_payload_container_is_valid(ref_container) ||
      ref_length != stored_bytes) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    lc_free_with_allocator(allocator, ref_container);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state payload ref metadata is invalid", NULL,
                        NULL, "pouch");
  }
  if (record_type == LC_POUCH_STATE_RECORD_STATE_PUT ||
      record_type == LC_POUCH_COLLECTION_EDIT_BODY ||
      record_type == LC_POUCH_COLLECTION_EDIT ||
      record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT) {
    if (container_leaf == NULL || strcmp(container_leaf, ref_container) != 0 ||
        ref_record_offset != record_offset || payload_offset != ref_offset ||
        payload_length != ref_length || payload_length != stored_bytes ||
        payload_crc != ref_crc) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state put payload metadata is invalid", NULL,
                          NULL, "pouch");
    }
    rc = lc_pouch_state_payload_span_set(
        allocator, &entry->payload_span, ref_container, ref_record_offset,
        ref_offset, ref_length, ref_crc, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return rc;
    }
  } else {
    rc = lc_pouch_state_payload_span_set(
        allocator, &entry->payload_span, ref_container, ref_record_offset,
        ref_offset, ref_length, ref_crc, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return rc;
    }
  }
  lc_free_with_allocator(allocator, ref_container);
  cursor += ref_container_len;
payload_ref_decoded:
  entry->payload_context =
      payload_context_len > 0UL
          ? lc_pouch_state_bytes_to_string(allocator, cursor,
                                           (size_t)payload_context_len, error)
          : NULL;
  cursor += payload_context_len;
  consumed_len = (unsigned long)(cursor - meta);
  if (meta_len > consumed_len) {
    if (meta_len - consumed_len < 4U) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state side metadata is truncated", NULL, NULL,
                          "pouch");
    }
    side_metadata_len = lc_pouch_state_get32(cursor);
    cursor += 4U;
    consumed_len += 4U;
    if (side_metadata_len > meta_len - consumed_len) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state side metadata payload is truncated",
                          NULL, NULL, "pouch");
    }
    if (side_metadata_len > 0UL) {
      entry->metadata = (unsigned char *)lc_alloc_with_allocator(
          allocator, (size_t)side_metadata_len);
      if (entry->metadata == NULL) {
        lc_pouch_state_entry_cleanup(allocator, entry);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch state side metadata",
                            NULL, NULL, NULL);
      }
      memcpy(entry->metadata, cursor, (size_t)side_metadata_len);
      entry->metadata_length = (size_t)side_metadata_len;
    }
    cursor += side_metadata_len;
    consumed_len += side_metadata_len;
  }
  rc = lc_pouch_state_decode_retention_meta(
      meta, meta_len, consumed_len, lc_pouch_state_key_is_staged(entry->key),
      &entry->retention, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  if (entry->key == NULL || entry->content_type == NULL ||
      entry->etag == NULL ||
      (record_type != LC_POUCH_STATE_RECORD_STATE_META &&
       !entry->payload_span.present) ||
      (descriptor_len > 0UL && entry->descriptor == NULL) ||
      (payload_context_len > 0UL && entry->payload_context == NULL)) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  rc = lc_pouch_state_meta_query_index_seq(meta, meta_len,
                                           &entry->query_index_seq,
                                           &entry->has_query_index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->bytes = plain_bytes;
  entry->cipher_bytes = stored_bytes;
  entry->updated_at_unix =
      lc_pouch_state_decode_unix_seconds(lc_pouch_state_get64(meta + 8));
  entry->has_query_hidden =
      (meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_HAS_QUERY_HIDDEN) != 0U;
  entry->query_hidden =
      (meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_QUERY_HIDDEN) != 0U;
  entry->staged_delete_marker =
      (meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_STAGED_DELETE) != 0U;
  entry->object_link =
      (meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_OBJECT_LINK) != 0U;
  if (entry->object_link && record_type != LC_POUCH_STATE_RECORD_STATE_LINK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch object-link metadata has invalid record type",
                        NULL, NULL, "pouch");
  }
  entry->seen = 1;
  entry->found = 1;
  entry->record_type = record_type;
  rc = lc_pouch_state_record_ref_set(
      allocator, &entry->record_container_leaf, &entry->record_offset,
      &entry->has_record_ref, container_leaf, record_offset, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  return LC_OK;
}

static int lc_pouch_state_decode_delete_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, unsigned char record_type,
    const char *container_leaf, uint64_t record_offset,
    lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_generation version;
  unsigned long etag_len;
  int rc;

  version = 0UL;
  etag_len = 0UL;
  if (meta_len < 18U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state delete metadata is truncated", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  etag_len = lc_pouch_state_get16(meta + 16);
  if (18U + etag_len > meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state delete etag is truncated", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  entry->etag =
      lc_pouch_state_bytes_to_string(allocator, meta + 18, etag_len, error);
  if (entry->key == NULL || entry->etag == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  rc = lc_pouch_state_meta_query_index_seq(meta, meta_len,
                                           &entry->query_index_seq,
                                           &entry->has_query_index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->updated_at_unix =
      lc_pouch_state_decode_unix_seconds(lc_pouch_state_get64(meta + 8));
  entry->seen = 1;
  entry->found = 0;
  entry->record_type = record_type;
  rc = lc_pouch_state_record_ref_set(
      allocator, &entry->record_container_leaf, &entry->record_offset,
      &entry->has_record_ref, container_leaf, record_offset, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  return LC_OK;
}

static int lc_pouch_state_decode_decision_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, lc_pouch_state_entry *entry,
    lc_error *error) {
  lc_pouch_generation version;
  unsigned long etag_len;
  const char *decision;
  int rc;

  version = 0UL;
  etag_len = 0UL;
  decision = NULL;
  if (meta_len < 19U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision metadata is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  etag_len = lc_pouch_state_get16(meta + 16);
  if (19U + etag_len > meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision etag is truncated", NULL, NULL,
                        "pouch");
  }
  if (meta[18] == 1U) {
    decision = LC_POUCH_STATE_DECISION_COMMITTED;
  } else if (meta[18] == 2U) {
    decision = LC_POUCH_STATE_DECISION_DISCARDED;
  } else {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision value is invalid", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  entry->etag =
      lc_pouch_state_bytes_to_string(allocator, meta + 19, etag_len, error);
  entry->decision = lc_strdup_with_allocator(allocator, decision);
  if (entry->key == NULL || entry->etag == NULL || entry->decision == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  entry->record_type = LC_POUCH_STATE_RECORD_DECISION;
  return LC_OK;
}

static int lc_pouch_state_decode_high_water_meta(const lc_allocator *allocator,
                                                 const unsigned char *meta,
                                                 size_t meta_len,
                                                 lc_pouch_state_entry *entry,
                                                 lc_error *error) {
  lc_pouch_generation version;
  lc_pouch_generation query_version;
  int rc;

  version = 0UL;
  query_version = 0UL;
  if (meta_len != 8U && meta_len != 16U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state high-water metadata is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  if (meta_len == 16U) {
    rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta + 8),
                                          &query_version, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    /* Pre-query-freshness snapshots have only a state high water. Preserve
     * safety over precision until their next compaction rewrites the record. */
    query_version = version;
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key =
      lc_strdup_with_allocator(allocator, LC_POUCH_STATE_HIGH_WATER_KEY);
  if (entry->key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to decode pouch high-water record", NULL, NULL,
                        NULL);
  }
  entry->version = version;
  entry->index_seq = version;
  entry->query_index_seq = query_version;
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  entry->record_type = LC_POUCH_STATE_RECORD_HIGH_WATER;
  return LC_OK;
}

static int lc_pouch_state_decode_record(
    const lc_allocator *allocator, const lc_pouch_record_header *header,
    const unsigned char *key, const unsigned char *meta,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc,
    lc_pouch_state_entry *entry, lc_error *error) {
  switch (header->type) {
  case LC_POUCH_COLLECTION_EDIT_BODY:
  case LC_POUCH_COLLECTION_EDIT:
  case LC_POUCH_STATE_RECORD_STATE_PUT:
  case LC_POUCH_STATE_RECORD_OBJECT_PUT:
  case LC_POUCH_STATE_RECORD_STATE_LINK:
  case LC_POUCH_STATE_RECORD_STATE_META:
    return lc_pouch_state_decode_payload_meta(
        allocator, key, header->key_len, meta, header->meta_len, header->type,
        container_leaf, record_offset, payload_offset, payload_length,
        payload_crc, entry, error);
  case LC_POUCH_STATE_RECORD_STATE_DELETE:
  case LC_POUCH_STATE_RECORD_OBJECT_DELETE:
    return lc_pouch_state_decode_delete_meta(
        allocator, key, header->key_len, meta, header->meta_len, header->type,
        container_leaf, record_offset, entry, error);
  case LC_POUCH_STATE_RECORD_DECISION:
    return lc_pouch_state_decode_decision_meta(
        allocator, key, header->key_len, meta, header->meta_len, entry, error);
  case LC_POUCH_STATE_RECORD_HIGH_WATER:
    return lc_pouch_state_decode_high_water_meta(
        allocator, meta, header->meta_len, entry, error);
  default:
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record type is invalid", NULL,
                        NULL, "pouch");
  }
}

static int lc_pouch_state_read_next_record(
    FILE *fp, const lc_allocator *allocator, const char *container_leaf,
    int allow_truncated_tail, int verify_payload, uint64_t file_size,
    lc_pouch_state_entry *entry, int *found, int *truncated_tail,
    lc_error *error) {
  lc_pouch_record_header header;
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  unsigned char *key;
  unsigned char *meta;
  unsigned char payload_buffer[16384];
  uint64_t remaining;
  uint64_t payload_offset;
  uint64_t payload_length;
  uint64_t record_start;
  unsigned long crc;
  size_t got;
  off_t position;
  int rc;

  key = NULL;
  meta = NULL;
  payload_offset = 0U;
  payload_length = 0U;
  record_start = 0U;
  crc = 0UL;
  if (fp == NULL || allocator == NULL || entry == NULL || found == NULL ||
      truncated_tail == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record read requires inputs", NULL, NULL,
                        "pouch");
  }
  *found = 0;
  *truncated_tail = 0;
  position = ftello(fp);
  if (position < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state record offset",
                        strerror(errno), NULL, "pouch");
  }
  record_start = (uint64_t)position;
  got = fread(encoded, 1U, sizeof(encoded), fp);
  if (got == 0U) {
    if (ferror(fp)) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch state segment", strerror(errno),
                          NULL, "pouch");
    }
    return LC_OK;
  }
  if (got != sizeof(encoded)) {
    if (allow_truncated_tail && !ferror(fp)) {
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record header is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_record_header_decode(encoded, &header, error);
  if (rc != LC_OK) {
    return rc;
  }
  if ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record flags are invalid", NULL,
                        NULL, "pouch");
  }
  if ((header.flags & LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
    if (allow_truncated_tail) {
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment contains pending record", NULL,
                        NULL, "pouch");
  }
  if (header.key_len > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES ||
      header.meta_len > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record exceeds local limits", NULL,
                        NULL, "pouch");
  }
  if (header.key_len > 0UL) {
    key = (unsigned char *)lc_alloc_with_allocator(allocator, header.key_len);
    if (key == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state record key", NULL,
                          NULL, NULL);
    }
    got = fread(key, 1U, header.key_len, fp);
    if (got != header.key_len) {
      lc_free_with_allocator(allocator, key);
      if (allow_truncated_tail && !ferror(fp)) {
        *truncated_tail = 1;
        return LC_OK;
      }
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment record key is truncated", NULL,
                          NULL, "pouch");
    }
  }
  if (header.meta_len > 0UL) {
    meta = (unsigned char *)lc_alloc_with_allocator(allocator, header.meta_len);
    if (meta == NULL) {
      lc_free_with_allocator(allocator, key);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state record metadata",
                          NULL, NULL, NULL);
    }
    got = fread(meta, 1U, header.meta_len, fp);
    if (got != header.meta_len) {
      lc_free_with_allocator(allocator, key);
      lc_free_with_allocator(allocator, meta);
      if (allow_truncated_tail && !ferror(fp)) {
        *truncated_tail = 1;
        return LC_OK;
      }
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment record metadata is truncated",
                          NULL, NULL, "pouch");
    }
  }
  position = ftello(fp);
  if (position < 0) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state payload offset",
                        strerror(errno), NULL, "pouch");
  }
  payload_offset = (uint64_t)position;
  payload_length = header.payload_len;
  if (payload_length > file_size ||
      payload_offset > file_size - payload_length) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    if (allow_truncated_tail) {
      /* The active file can grow after fstat but before this header read. */
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment payload is truncated", NULL, NULL,
                        "pouch");
  }
  if (verify_payload) {
    crc = (unsigned long)crc32(0L, Z_NULL, 0);
    remaining = header.payload_len;
    while (remaining > 0U) {
      size_t want;

      want = remaining > sizeof(payload_buffer) ? sizeof(payload_buffer)
                                                : (size_t)remaining;
      got = fread(payload_buffer, 1U, want, fp);
      if (got != want) {
        lc_free_with_allocator(allocator, key);
        lc_free_with_allocator(allocator, meta);
        OPENSSL_cleanse(payload_buffer, sizeof(payload_buffer));
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "pouch state segment payload is truncated", NULL,
                            NULL, "pouch");
      }
      crc = (unsigned long)crc32((uLong)crc, payload_buffer, (uInt)got);
      remaining -= (uint64_t)got;
    }
    OPENSSL_cleanse(payload_buffer, sizeof(payload_buffer));
    if (crc != header.payload_crc) {
      lc_free_with_allocator(allocator, key);
      lc_free_with_allocator(allocator, meta);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment payload CRC mismatch", NULL,
                          NULL, "pouch");
    }
  } else if (payload_length > LC_U64_MAX - payload_offset ||
             (off_t)(payload_offset + payload_length) < 0 ||
             (uint64_t)(off_t)(payload_offset + payload_length) !=
                 payload_offset + payload_length ||
             fseeko(fp, (off_t)(payload_offset + payload_length), SEEK_SET) !=
                 0) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to skip pouch state payload", strerror(errno),
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_record(
      allocator, &header, key, meta, container_leaf, record_start,
      payload_offset, payload_length, header.payload_crc, entry, error);
  lc_free_with_allocator(allocator, key);
  lc_free_with_allocator(allocator, meta);
  if (rc == LC_OK) {
    *found = 1;
  }
  return rc;
}

static void lc_pouch_state_log_tail_repair(lc_pouch *pouch, const char *reason,
                                           const char *segment,
                                           uint64_t offset) {
  pslog_field fields[3];

#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_tail_repair_hook != NULL) {
    lc_pouch_test_tail_repair_hook(lc_pouch_test_tail_repair_context, reason,
                                   segment);
  }
#endif

  fields[0] = lc_log_str_field("reason", reason);
  fields[1] = lc_log_str_field("segment", segment);
  fields[2] = lc_log_u64_field("record_offset", offset);
  lc_log_warn(pouch->logger, "logstore.tail.repair", fields, 3U);
}

static int
lc_pouch_state_scan_file(lc_pouch *pouch, const char *segment_path,
                         const char *container_leaf, int allow_truncated_tail,
                         int repair_truncated_tail, uint64_t start_offset,
                         const char *key, lc_pouch_state_entry *current,
                         lc_pouch_generation *max_version, lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int repair_tail;
  uint64_t repair_offset;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  if (start_offset > file_size) {
    start_offset = 0U;
  }
  if (start_offset > 0U) {
    off_t offset;

    offset = (off_t)start_offset;
    if (offset < 0 || (uint64_t)offset != start_offset ||
        fseeko(fp, offset, SEEK_SET) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch state active-tail repair offset",
                        strerror(errno), NULL, "pouch");
      (void)fclose(fp);
      return rc;
    }
  }

  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  repair_offset = 0U;
  for (;;) {
    off_t good_position;

    good_position = ftello(fp);
    if (good_position < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state scan offset",
                        strerror(errno), NULL, "pouch");
      break;
    }
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      repair_tail = allow_truncated_tail && repair_truncated_tail;
      repair_offset = (uint64_t)good_position;
      break;
    }
    if (!found) {
      break;
    }
    if (!entry.seen) {
      continue;
    }
    if (max_version != NULL && entry.version > *max_version) {
      *max_version = entry.version;
    }
    if (!entry.control && key != NULL && strcmp(entry.key, key) == 0) {
      if (lc_pouch_state_entry_supersedes(&entry, current)) {
        lc_pouch_state_entry_cleanup(&pouch->allocator, current);
        *current = entry;
        memset(&entry, 0, sizeof(entry));
      } else {
        lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
      }
    } else {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && repair_tail) {
    rc = lc_pouch_state_truncate_path(segment_path, repair_offset, error);
    if (rc == LC_OK) {
      lc_pouch_state_log_tail_repair(pouch, "append", container_leaf,
                                     repair_offset);
    }
  }
  return rc;
}

static int lc_pouch_state_repair_active_tail_locked(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, const char *segment_path,
    const char *segment_leaf, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_entry current;
  lc_pouch_generation max_version;
  uint64_t start_offset;
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL || segment_path == NULL ||
      segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair requires inputs", NULL, NULL,
                        "pouch");
  }
  start_offset = 0U;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, segment_leaf) == 0 &&
      lc_pouch_state_cache_matches_manifest(cache, manifest)) {
    start_offset = cache->active_segment_offset;
  }
  memset(&current, 0, sizeof(current));
  max_version = 0UL;
  rc = lc_pouch_state_scan_file(pouch, segment_path, segment_leaf, 1, 1,
                                start_offset, NULL, &current, &max_version,
                                error);
  if (rc == LC_ERR_PROTOCOL && start_offset > 0U) {
    /* A projection cursor is an acceleration, never a durable boundary. The
     * append gate makes this active segment stable, so retry from its start
     * before classifying bytes as corruption. A full replay still rejects a
     * malformed log; this only repairs an obsolete in-memory cursor. */
    lc_error_cleanup(error);
    lc_error_init(error);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    memset(&current, 0, sizeof(current));
    max_version = 0UL;
    rc = lc_pouch_state_scan_file(pouch, segment_path, segment_leaf, 1, 1, 0U,
                                  NULL, &current, &max_version, error);
    if (rc == LC_OK) {
      lc_pouch_state_cache_invalidate_namespace(pouch, ns);
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  return rc;
}

static int lc_pouch_state_skip_file_bytes(FILE *fp, uint64_t length,
                                          lc_error *error) {
  while (length > 0U) {
    const uint64_t max_seek_chunk = 2147483647U;
    off_t chunk;

    chunk = (off_t)(length > max_seek_chunk ? max_seek_chunk : length);
    if (fseeko(fp, chunk, SEEK_CUR) != 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to skip pouch state record bytes",
                          strerror(errno), NULL, "pouch");
    }
    length -= (uint64_t)chunk;
  }
  return LC_OK;
}

static int lc_pouch_state_scan_file_max_version(
    lc_pouch *pouch, const char *segment_path, int allow_truncated_tail,
    lc_pouch_generation *max_version, lc_error *error) {
  FILE *fp;
  int rc;

  if (pouch == NULL || segment_path == NULL || max_version == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state sequence scan requires inputs", NULL, NULL,
                        "pouch");
  }
  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state sequence file",
                        strerror(errno), NULL, NULL);
  }
  rc = LC_OK;
  for (;;) {
    lc_pouch_record_header header;
    unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
    unsigned char index_meta[LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES];
    lc_pouch_generation index_seq;
    size_t got;
    size_t trailer_len;

    index_seq = 0UL;
    trailer_len = 0U;
    got = fread(encoded, 1U, sizeof(encoded), fp);
    if (got == 0U) {
      if (ferror(fp)) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch state sequence file",
                          strerror(errno), NULL, "pouch");
      }
      break;
    }
    if (got != sizeof(encoded)) {
      if (allow_truncated_tail && !ferror(fp)) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence header is truncated", NULL, NULL,
                        "pouch");
      break;
    }
    rc = lc_pouch_record_header_decode(encoded, &header, error);
    if (rc != LC_OK) {
      break;
    }
    if ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record flags are invalid", NULL,
                        NULL, "pouch");
      break;
    }
    if ((header.flags & LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
      if (allow_truncated_tail) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence contains pending record", NULL,
                        NULL, "pouch");
      break;
    }
    if (header.key_len > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES ||
        header.meta_len > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record exceeds local limits",
                        NULL, NULL, "pouch");
      break;
    }
    switch (header.type) {
    case LC_POUCH_STATE_RECORD_STATE_PUT:
    case LC_POUCH_STATE_RECORD_OBJECT_PUT:
    case LC_POUCH_STATE_RECORD_STATE_DELETE:
    case LC_POUCH_STATE_RECORD_OBJECT_DELETE:
    case LC_POUCH_STATE_RECORD_STATE_LINK:
    case LC_POUCH_STATE_RECORD_STATE_META:
    case LC_POUCH_STATE_RECORD_DECISION:
    case LC_POUCH_STATE_RECORD_HIGH_WATER:
      break;
    default:
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record type is invalid", NULL,
                        NULL, "pouch");
      break;
    }
    if (rc != LC_OK) {
      break;
    }
    if ((header.type == LC_POUCH_STATE_RECORD_HIGH_WATER &&
         header.meta_len != 8U && header.meta_len != 16U) ||
        (header.type != LC_POUCH_STATE_RECORD_HIGH_WATER &&
         header.meta_len < LC_POUCH_STATE_INDEX_TRAILER_BYTES)) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence metadata is invalid", NULL, NULL,
                        "pouch");
      break;
    }
    rc = lc_pouch_state_skip_file_bytes(fp, (uint64_t)header.key_len, error);
    if (rc != LC_OK) {
      break;
    }
    if (header.type == LC_POUCH_STATE_RECORD_HIGH_WATER) {
      got = fread(index_meta, 1U, 8U, fp);
    } else {
      trailer_len = header.meta_len >= LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES
                        ? LC_POUCH_STATE_QUERY_INDEX_TRAILER_BYTES
                        : LC_POUCH_STATE_INDEX_TRAILER_BYTES;
      rc = lc_pouch_state_skip_file_bytes(
          fp, (uint64_t)(header.meta_len - trailer_len), error);
      if (rc != LC_OK) {
        break;
      }
      got = fread(index_meta, 1U, trailer_len, fp);
    }
    if (got !=
        (header.type == LC_POUCH_STATE_RECORD_HIGH_WATER ? 8U : trailer_len)) {
      if (allow_truncated_tail && !ferror(fp)) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence metadata is truncated", NULL,
                        NULL, "pouch");
      break;
    }
    if (header.type == LC_POUCH_STATE_RECORD_HIGH_WATER) {
      rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(index_meta),
                                            &index_seq, error);
    } else {
      rc = lc_pouch_state_meta_index_seq(index_meta, trailer_len, &index_seq,
                                         error);
    }
    if (rc != LC_OK) {
      break;
    }
    if (index_seq > *max_version) {
      *max_version = index_seq;
    }
    if (header.type == LC_POUCH_STATE_RECORD_HIGH_WATER &&
        header.meta_len > 8U) {
      rc = lc_pouch_state_skip_file_bytes(fp, (uint64_t)(header.meta_len - 8U),
                                          error);
      if (rc != LC_OK) {
        break;
      }
    }
    rc = lc_pouch_state_skip_file_bytes(fp, header.payload_len, error);
    if (rc != LC_OK) {
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state sequence file",
                      strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_manifest_max_version(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_generation *max_version, lc_error *error) {
  unsigned long segment_index;
  int rc;

  if (pouch == NULL || manifest == NULL || max_version == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state sequence requires pouch, manifest, and "
                        "output",
                        NULL, NULL, "pouch");
  }
  *max_version = 0UL;
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file_max_version(pouch, snapshot_path, 0,
                                              max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;
    int allow_truncated_tail;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    allow_truncated_tail = lc_pouch_state_manifest_segment_allows_tail_repair(
        manifest, segment_leaf);
    rc = lc_pouch_state_scan_file_max_version(
        pouch, segment_path, allow_truncated_tail, max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_scan(lc_pouch *pouch,
                               const lc_pouch_namespace_manifest *manifest,
                               const char *key, lc_pouch_state_entry *current,
                               lc_pouch_generation *max_version,
                               lc_error *error) {
  unsigned long segment_index;
  int rc;

  memset(current, 0, sizeof(*current));
  if (max_version != NULL) {
    *max_version = 0UL;
  }
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(pouch, snapshot_path,
                                  manifest->latest_snapshot, 0, 0, 0U, key,
                                  current, max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(
        pouch, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        0, 0U, key, current, max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_decision_apply(lc_pouch *pouch,
                                         lc_pouch_state_decision **decisions,
                                         const lc_pouch_state_entry *entry,
                                         lc_error *error) {
  lc_pouch_state_decision *decision;
  char *staged_key;
  char *etag;
  char *decision_value;

  if (entry == NULL || !entry->control || entry->key == NULL ||
      entry->etag == NULL || entry->decision == NULL) {
    return LC_OK;
  }
  for (decision = *decisions; decision != NULL; decision = decision->next) {
    if (strcmp(decision->staged_key, entry->key) != 0) {
      continue;
    }
    if (entry->version < decision->version ||
        (entry->version == decision->version &&
         entry->index_seq <= decision->index_seq)) {
      return LC_OK;
    }
    staged_key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
    decision_value =
        lc_strdup_with_allocator(&pouch->allocator, entry->decision);
    if (staged_key == NULL || etag == NULL || decision_value == NULL) {
      lc_free_with_allocator(&pouch->allocator, staged_key);
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_free_with_allocator(&pouch->allocator, decision_value);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch state decision", NULL, NULL,
                          NULL);
    }
    lc_free_with_allocator(&pouch->allocator, decision->staged_key);
    lc_free_with_allocator(&pouch->allocator, decision->etag);
    lc_free_with_allocator(&pouch->allocator, decision->decision);
    decision->staged_key = staged_key;
    decision->etag = etag;
    decision->decision = decision_value;
    decision->index_seq = entry->index_seq;
    decision->version = entry->version;
    return LC_OK;
  }

  decision = (lc_pouch_state_decision *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*decision));
  if (decision == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->staged_key =
      lc_strdup_with_allocator(&pouch->allocator, entry->key);
  decision->etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
  decision->decision =
      lc_strdup_with_allocator(&pouch->allocator, entry->decision);
  if (decision->staged_key == NULL || decision->etag == NULL ||
      decision->decision == NULL) {
    lc_pouch_state_decision_cleanup(&pouch->allocator, decision);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->index_seq = entry->index_seq;
  decision->version = entry->version;
  decision->next = *decisions;
  *decisions = decision;
  return LC_OK;
}

static int lc_pouch_state_collect_decisions_file(
    lc_pouch *pouch, const char *segment_path, const char *container_leaf,
    int allow_truncated_tail, lc_pouch_state_decision **decisions,
    lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  for (;;) {
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      break;
    }
    if (!found) {
      break;
    }
    rc = lc_pouch_state_decision_apply(pouch, decisions, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_collect_decisions(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_decision **decisions, lc_error *error) {
  unsigned long segment_index;
  int rc;

  *decisions = NULL;
  rc = LC_OK;
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_collect_decisions_file(
        pouch, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        decisions, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_replay_file(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const char *segment_path, const char *container_leaf,
    int allow_truncated_tail, uint64_t start_offset, uint64_t *end_offset_out,
    lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int truncated_tail_seen;
  int repair_tail;
  uint64_t repair_offset;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  if (start_offset > file_size ||
      (start_offset > 0U && ((off_t)start_offset < 0 ||
                             (uint64_t)(off_t)start_offset != start_offset ||
                             fseeko(fp, (off_t)start_offset, SEEK_SET) != 0))) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch active segment offset is invalid", NULL, NULL,
                      "pouch");
    (void)fclose(fp);
    return rc;
  }
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  truncated_tail_seen = 0;
  repair_offset = 0U;
  for (;;) {
    off_t good_position;

    good_position = ftello(fp);
    if (good_position < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state replay offset",
                        strerror(errno), NULL, "pouch");
      break;
    }
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      /* A reader must never repair the tail of an active writer's log. */
      repair_tail = 0;
      truncated_tail_seen = 1;
      repair_offset = (uint64_t)good_position;
      break;
    }
    if (!found) {
      break;
    }
    if (entry.seen && entry.index_seq > cache->max_version) {
      cache->max_version = entry.index_seq;
    }
    rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && repair_tail) {
    lc_pouch_state_truncate_path_best_effort(segment_path, repair_offset);
    lc_pouch_state_log_tail_repair(pouch, "replay", container_leaf,
                                   repair_offset);
  }
  if (rc == LC_OK && end_offset_out != NULL) {
    *end_offset_out = repair_offset;
    if (!truncated_tail_seen) {
      off_t end_position;

      end_position = (off_t)file_size;
      if (end_position < 0 || (uint64_t)end_position != file_size) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "pouch state segment exceeds local offset range",
                            NULL, NULL, "pouch");
      }
      *end_offset_out = (uint64_t)end_position;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_matches_manifest(
    const lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest) {
  if (cache == NULL || manifest == NULL || !cache->initialized ||
      cache->max_segment_id != manifest->max_segment_id ||
      cache->active_segment_leaf == NULL || manifest->active_segment == NULL ||
      strcmp(cache->active_segment_leaf, manifest->active_segment) != 0) {
    return 0;
  }
  if (cache->latest_snapshot_leaf == NULL ||
      manifest->latest_snapshot == NULL) {
    if (cache->latest_snapshot_leaf != NULL ||
        manifest->latest_snapshot != NULL) {
      return 0;
    }
  } else if (strcmp(cache->latest_snapshot_leaf, manifest->latest_snapshot) !=
             0) {
    return 0;
  }
  if (cache->segment_count == manifest->segment_count) {
    return 1;
  }
  /* A namespace starts with no physical active segment. Its first append makes
   * only the already-known active leaf visible to manifest discovery. The
   * cache can safely tail that leaf from its verified cursor; any other
   * topology change remains a full projection rebuild. */
  return cache->segment_count < ULONG_MAX &&
         manifest->segment_count == cache->segment_count + 1UL &&
         lc_pouch_state_manifest_has_segment(manifest,
                                             manifest->active_segment);
}

static int
lc_pouch_state_cache_tail_active(lc_pouch *pouch,
                                 lc_pouch_namespace_logstore *cache,
                                 const lc_pouch_namespace_manifest *manifest,
                                 int *rebuild, lc_error *error) {
  char *segment_path;
  uint64_t file_size;
  uint64_t end_offset;
  int rc;

  *rebuild = 0;
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment path", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_file_size(segment_path, &file_size, error);
  if (rc == LC_OK && file_size < cache->active_segment_offset) {
    *rebuild = 1;
  }
  if (rc == LC_OK && !*rebuild && file_size > cache->active_segment_offset) {
    end_offset = cache->active_segment_offset;
    rc = lc_pouch_state_cache_replay_file(
        pouch, cache, segment_path, manifest->active_segment, 1,
        cache->active_segment_offset, &end_offset, error);
    if (rc == LC_ERR_PROTOCOL && cache->active_segment_offset > 0U) {
      /* The active cursor is an in-memory acceleration only. A mode change,
       * recovery path, or interrupted peer append must never turn an
       * unverified cursor into durable corruption. Rebuild from the segment
       * boundary; a genuinely malformed active log is then still rejected by
       * the full replay, while a stale cursor is repaired without dropping
       * any records. */
      lc_error_cleanup(error);
      lc_error_init(error);
      *rebuild = 1;
      rc = LC_OK;
    }
    if (rc == LC_OK && !*rebuild) {
      cache->active_segment_offset = end_offset;
    }
  }
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (rc != LC_OK || *rebuild) {
    return rc;
  }
  if (manifest->state_max_version > cache->max_version) {
    cache->max_version = manifest->state_max_version;
  }
  return LC_OK;
}

static int lc_pouch_state_cache_restore_clean_projection(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int *restored,
    lc_error *error) {
  char *active_path;
  char *projection_path;
  lc_pouch_generation checkpoint;
  lc_pouch_generation checkpoint_query;
  uint64_t checkpoint_identity;
  uint64_t current_identity;
  uint64_t active_size;
  int checkpoint_valid;
  int checkpoint_query_valid;
  int rc;

  if (restored != NULL) {
    *restored = 0;
  }
  if (pouch == NULL || cache == NULL || manifest == NULL ||
      !lc_pouch_single_writer_enabled(pouch) ||
      lc_pouch_crypto_enabled(pouch->crypto) ||
      lc_pouch_crypto_compression_enabled(pouch->crypto)) {
    return LC_OK;
  }
  checkpoint = 0UL;
  checkpoint_query = 0UL;
  checkpoint_identity = 0U;
  current_identity = 0U;
  checkpoint_valid = 0;
  checkpoint_query_valid = 0;
  rc = lc_pouch_state_clean_checkpoint_read(
      pouch, manifest, &checkpoint_valid, &checkpoint_query_valid, &checkpoint,
      &checkpoint_query, &checkpoint_identity, error);
  if (rc != LC_OK || !checkpoint_valid) {
    return rc;
  }
  rc = lc_pouch_state_clean_checkpoint_identity(pouch, manifest,
                                                &current_identity, error);
  if (rc != LC_OK || current_identity != checkpoint_identity) {
    return rc;
  }
  projection_path = lc_pouch_state_child_path(
      &pouch->allocator, manifest->namespace_path, "snapshots",
      LC_POUCH_STATE_CLEAN_PROJECTION_LEAF);
  if (projection_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch clean projection path", NULL,
                        NULL, "pouch");
  }
  if (access(projection_path, R_OK) != 0) {
    lc_free_with_allocator(&pouch->allocator, projection_path);
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch clean projection",
                        strerror(errno), NULL, "pouch");
  }
  rc = lc_pouch_state_cache_replay_file(pouch, cache, projection_path,
                                        LC_POUCH_STATE_CLEAN_PROJECTION_LEAF, 0,
                                        0U, NULL, error);
  lc_free_with_allocator(&pouch->allocator, projection_path);
  if (rc != LC_OK) {
    /* The clean projection is an acceleration artifact. A damaged one falls
     * back to the durable state log rather than making the namespace
     * unreadable.
     */
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    lc_error_cleanup(error);
    lc_error_init(error);
    return LC_OK;
  }
  active_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (active_path == NULL) {
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment path", NULL,
                        NULL, "pouch");
  }
  active_size = 0U;
  rc = lc_pouch_state_file_size(active_path, &active_size, error);
  lc_free_with_allocator(&pouch->allocator, active_path);
  if (rc != LC_OK) {
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    return rc;
  }
  cache->namespace_path =
      lc_strdup_with_allocator(&pouch->allocator, manifest->namespace_path);
  cache->active_segment_leaf =
      lc_strdup_with_allocator(&pouch->allocator, manifest->active_segment);
  cache->latest_snapshot_leaf =
      manifest->latest_snapshot != NULL
          ? lc_strdup_with_allocator(&pouch->allocator,
                                     manifest->latest_snapshot)
          : NULL;
  if (cache->namespace_path == NULL || cache->active_segment_leaf == NULL ||
      (manifest->latest_snapshot != NULL &&
       cache->latest_snapshot_leaf == NULL)) {
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain pouch clean projection manifest",
                        NULL, NULL, "pouch");
  }
  cache->active_segment_offset = active_size;
  cache->active_segment_id = manifest->active_segment_id;
  cache->max_segment_id = manifest->max_segment_id;
  cache->segment_count = manifest->segment_count;
  if (checkpoint > cache->max_version) {
    cache->max_version = checkpoint;
  }
  if (checkpoint_query > cache->max_query_index_seq) {
    cache->max_query_index_seq = checkpoint_query;
  }
  cache->initialized = 1;
  /* A clean projection is a complete view of the namespace after every local
   * writer has stopped. If no staged key survives in that view, historical
   * transaction decisions cannot have an effect: recovery only acts on a
   * decision whose staged key is still live. Mark the one-time decision scan
   * complete so ordinary clean restarts do not replay an unbounded log. Live
   * XA staging deliberately retains the conservative recovery scan. */
  if (!lc_pouch_state_cache_has_live_staged_records(cache)) {
    cache->decision_recovery_checked = 1;
  }
  if (restored != NULL) {
    *restored = 1;
  }
  return LC_OK;
}

static int lc_pouch_state_cache_refresh(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int force, lc_error *error) {
  unsigned long segment_index;
  int rebuild;
  int restored_clean_projection;
  int rc;

  /* A writer-mode epoch change cannot reuse an exclusive append descriptor.
   * The projection remains valid until the normal manifest/tail validation
   * below says otherwise, so this does not turn a mode transition into a full
   * namespace replay. */
  if (force && cache->active_append_fd_owned && cache->active_append_fd >= 0) {
    (void)close(cache->active_append_fd);
    cache->active_append_fd = -1;
    cache->active_append_fd_owned = 0;
  }

  if (lc_pouch_state_cache_matches_manifest(cache, manifest)) {
    rc = lc_pouch_state_cache_tail_active(pouch, cache, manifest, &rebuild,
                                          error);
    if (rc != LC_OK || !rebuild) {
      return rc;
    }
  }
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
  cache->max_version = 0UL;
  cache->max_query_index_seq = 0UL;
  cache->max_segment_id = 0UL;
  cache->segment_count = 0UL;
  restored_clean_projection = 0;
  rc = LC_OK;
  /* Force-refresh callers need record references from the authoritative
   * manifest topology. In particular, compaction compares those references
   * with its candidate set; a clean-reopen projection intentionally contains
   * link records and is therefore only valid for ordinary restart recovery. */
  if (!force) {
    rc = lc_pouch_state_cache_restore_clean_projection(
        pouch, cache, manifest, &restored_clean_projection, error);
    if (rc != LC_OK || restored_clean_projection) {
      return rc;
    }
  }
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_cache_replay_file(pouch, cache, snapshot_path,
                                          manifest->latest_snapshot, 0, 0U,
                                          NULL, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;
    uint64_t *end_offset;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    end_offset = strcmp(segment_leaf, manifest->active_segment) == 0
                     ? &cache->active_segment_offset
                     : NULL;
    rc = lc_pouch_state_cache_replay_file(
        pouch, cache, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        0U, end_offset, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  if (rc == LC_OK) {
    cache->namespace_path =
        lc_strdup_with_allocator(&pouch->allocator, manifest->namespace_path);
    cache->active_segment_leaf =
        lc_strdup_with_allocator(&pouch->allocator, manifest->active_segment);
    cache->latest_snapshot_leaf =
        manifest->latest_snapshot != NULL
            ? lc_strdup_with_allocator(&pouch->allocator,
                                       manifest->latest_snapshot)
            : NULL;
    if (cache->namespace_path == NULL || cache->active_segment_leaf == NULL ||
        (manifest->latest_snapshot != NULL &&
         cache->latest_snapshot_leaf == NULL)) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain pouch cache manifest state", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    cache->active_segment_id = manifest->active_segment_id;
    cache->max_segment_id = manifest->max_segment_id;
    cache->segment_count = manifest->segment_count;
    if (manifest->state_max_version > cache->max_version) {
      cache->max_version = manifest->state_max_version;
    }
    cache->initialized = 1;
    /* This is a complete, authoritative replay. If it contains no live
     * staged participant, no historical XA decision can still be applied.
     * This is equally true for shared-root recovery, where the clean
     * single-writer projection is deliberately unavailable. Retaining the
     * one-time scan in that state makes the first post-restart claim scale
     * with every historical decision despite there being nothing to recover. */
    if (!lc_pouch_state_cache_has_live_staged_records(cache)) {
      cache->decision_recovery_checked = 1;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_refresh_for_mode(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, lc_error *error) {
  lc_pouch_state_append_lock append_lock;
  uint64_t mode_epoch;
  int append_locked;
  int force_refresh;
  int single_writer;
  int rc;

  if (pouch == NULL || cache == NULL || manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache mode refresh requires inputs", NULL, NULL,
                        "pouch");
  }
  single_writer = lc_pouch_single_writer_snapshot(pouch, &mode_epoch);
  /* An uninitialized cache has no prior writer epoch to invalidate. Treating
   * that first materialization as a forced refresh bypasses the verified clean
   * projection, then makes the first post-restart acquire run an unbounded
   * staged-decision scan before it can mark recovery complete. A real mode
   * transition from an initialized projection remains forced. */
  force_refresh = cache->initialized && cache->writer_mode_epoch != mode_epoch;
  if (single_writer && cache->initialized && !force_refresh) {
    return LC_OK;
  }
  append_locked = 0;
  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  append_lock.pouch = NULL;
  append_lock.ns = NULL;
  append_lock.previous_append_lock = NULL;
  append_lock.borrowed = 0;
  if (!single_writer && !lc_pouch_state_shared_mutation_is_held(pouch)) {
    /* Shared cache refreshes must already own the root mutation authority.
     * Requiring it here prevents a projection reader from parsing a peer's
     * in-flight record as durable corruption. */
    rc = lc_pouch_state_append_lock_enter_after_mutation(pouch, cache->ns,
                                                         &append_lock, error);
    if (rc != LC_OK) {
      return rc;
    }
    append_locked = 1;
  }
  rc = lc_pouch_state_cache_refresh(pouch, cache, manifest, force_refresh,
                                    error);
  if (rc == LC_OK) {
    cache->writer_mode_epoch = mode_epoch;
  }
  if (append_locked) {
    lc_pouch_state_append_lock_release(&append_lock);
  }
  return rc;
}

static int lc_pouch_state_cache_set_active_segment(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, uint64_t offset,
    lc_error *error) {
  char *active_segment_leaf;

  if (pouch == NULL || cache == NULL || manifest == NULL ||
      manifest->active_segment == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache active segment requires context", NULL,
                        NULL, "pouch");
  }
  active_segment_leaf =
      lc_strdup_with_allocator(&pouch->allocator, manifest->active_segment);
  if (active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain pouch active segment", NULL, NULL,
                        "pouch");
  }
  if (cache->active_append_fd_owned && cache->active_append_fd >= 0) {
    (void)close(cache->active_append_fd);
  }
  cache->active_append_fd = -1;
  cache->active_append_fd_owned = 0;
  lc_free_with_allocator(&pouch->allocator, cache->active_segment_leaf);
  cache->active_segment_leaf = active_segment_leaf;
  cache->active_segment_offset = offset;
  cache->active_segment_id = manifest->active_segment_id;
  cache->max_segment_id = manifest->max_segment_id;
  cache->segment_count = manifest->segment_count;
  return LC_OK;
}

static int lc_pouch_state_cache_active_append_fd(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int *out, lc_error *error) {
  char *segment_path;
  int fd;
  int rc;

  if (pouch == NULL || cache == NULL || manifest == NULL || out == NULL ||
      !cache->initialized || cache->active_segment_leaf == NULL ||
      manifest->active_segment == NULL ||
      strcmp(cache->active_segment_leaf, manifest->active_segment) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active append cache is not current", NULL, NULL,
                        "pouch");
  }
  if (cache->active_append_fd_owned) {
    rc = lc_pouch_state_fd_seek(
        cache->active_append_fd, cache->active_segment_offset,
        "failed to seek pouch cached state segment append offset", error);
    if (rc != LC_OK) {
      (void)close(cache->active_append_fd);
      cache->active_append_fd = -1;
      cache->active_append_fd_owned = 0;
      return rc;
    }
    *out = cache->active_append_fd;
    return LC_OK;
  }
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment path", NULL,
                        NULL, "pouch");
  }
  fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch cached state segment",
                        strerror(errno), NULL, "pouch");
  }
  rc = lc_pouch_state_fd_seek(fd, cache->active_segment_offset,
                              "failed to seek pouch cached state segment "
                              "append offset",
                              error);
  if (rc != LC_OK) {
    (void)close(fd);
    return rc;
  }
  cache->active_append_fd = fd;
  cache->active_append_fd_owned = 1;
  *out = fd;
  return LC_OK;
}

/* The caller holds the namespace projection and append gate. The resident
 * fields therefore remain stable for the lifetime of this read-only view.
 * Durable manifest changes must first call manifest_materialize(), which
 * replaces the view with an owned manifest before any mutation. */
static int lc_pouch_state_cache_manifest_borrow(
    lc_pouch *pouch, const lc_pouch_namespace_logstore *cache,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  if (pouch == NULL || cache == NULL || manifest == NULL ||
      !cache->initialized || cache->namespace_path == NULL ||
      cache->active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache manifest is not current", NULL, NULL,
                        "pouch");
  }
  (void)pouch;
  memset(manifest, 0, sizeof(*manifest));
  manifest->namespace_path = cache->namespace_path;
  manifest->active_segment = cache->active_segment_leaf;
  manifest->active_segment_id = cache->active_segment_id;
  manifest->max_segment_id = cache->max_segment_id;
  manifest->segment_count = cache->segment_count;
  manifest->state_max_version = cache->max_version;
  manifest->borrowed = 1;
  return LC_OK;
}

/* Public read paths can release projection coordination before consuming their
 * result. Keep their manifest independent of the resident owner. */
static int lc_pouch_state_cache_manifest_copy(
    lc_pouch *pouch, const lc_pouch_namespace_logstore *cache,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  if (pouch == NULL || cache == NULL || manifest == NULL ||
      !cache->initialized || cache->namespace_path == NULL ||
      cache->active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache manifest is not current", NULL, NULL,
                        "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  manifest->namespace_path =
      lc_strdup_with_allocator(&pouch->allocator, cache->namespace_path);
  manifest->active_segment =
      lc_strdup_with_allocator(&pouch->allocator, cache->active_segment_leaf);
  if (manifest->namespace_path == NULL || manifest->active_segment == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch cache manifest", NULL, NULL,
                        "pouch");
  }
  manifest->active_segment_id = cache->active_segment_id;
  manifest->max_segment_id = cache->max_segment_id;
  manifest->segment_count = cache->segment_count;
  manifest->state_max_version = cache->max_version;
  return LC_OK;
}

/* Metadata batches materialize their own append manifest. While exclusive
 * mutation authority is held, the resident record can therefore be borrowed
 * directly instead of allocating an otherwise-unused manifest copy. */
static int
lc_pouch_state_cache_borrow_exclusive_record(lc_pouch *pouch, const char *ns,
                                             const char *key,
                                             lc_pouch_state_entry *current) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;

  if (pouch == NULL || ns == NULL || key == NULL || current == NULL ||
      !lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch)) {
    return 0;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache == NULL || !cache->initialized ||
      cache->writer_mode_epoch != writer_mode_epoch ||
      cache->namespace_path == NULL || cache->active_segment_leaf == NULL) {
    return 0;
  }
  lc_pouch_state_entry_borrow_cache_record(
      lc_pouch_state_cache_record_find(cache, key), current);
  return 1;
}

static int
lc_pouch_state_manifest_materialize(lc_pouch *pouch, const char *ns,
                                    lc_pouch_namespace_manifest *manifest,
                                    int *from_cache, lc_error *error) {
  lc_pouch_namespace_manifest loaded;
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL || from_cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest materialization requires context", NULL,
                        NULL, "pouch");
  }
  if (!*from_cache) {
    return LC_OK;
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  memset(&loaded, 0, sizeof(loaded));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &loaded, NULL, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  *manifest = loaded;
  *from_cache = 0;
  return LC_OK;
}

static int lc_pouch_state_manifest_lookup_cached(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || manifest == NULL ||
      current == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  memset(current, 0, sizeof(*current));
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_copy(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    return lc_pouch_state_entry_from_cache_record(
        pouch, lc_pouch_state_cache_record_find(cache, key), current, error);
  }
  rc =
      lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path, ns, error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          ns, manifest, NULL, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_lookup(pouch, ns, manifest, key, current,
                                     max_version_out, error);
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  }
  return rc;
}

/* Returns a borrowed record only while exclusive mutation serialization holds.
 */
static int lc_pouch_state_manifest_lookup_cached_borrowed(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, int *current_is_borrowed,
    lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || manifest == NULL ||
      current == NULL || current_is_borrowed == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch borrowed manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  *current_is_borrowed = 0;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    lc_pouch_state_entry_borrow_cache_record(
        lc_pouch_state_cache_record_find(cache, key), current);
    *current_is_borrowed = 1;
    return LC_OK;
  }
  return lc_pouch_state_manifest_lookup_cached(pouch, ns, key, manifest,
                                               current, max_version_out, error);
}

/* Locked mutation callers retain their existing owned record cleanup, but do
 * not need to allocate a manifest copy while the resident owner is stable. */
static int lc_pouch_state_manifest_lookup_cached_for_mutation(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || manifest == NULL ||
      current == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  memset(current, 0, sizeof(*current));
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    return lc_pouch_state_entry_from_cache_record(
        pouch, lc_pouch_state_cache_record_find(cache, key), current, error);
  }
  return lc_pouch_state_manifest_lookup_cached(pouch, ns, key, manifest,
                                               current, max_version_out, error);
}

/* Return a projection-backed manifest in exclusive mode; otherwise refresh. */
static int lc_pouch_state_manifest_view(lc_pouch *pouch, const char *ns,
                                        lc_pouch_namespace_manifest *manifest,
                                        lc_pouch_namespace_logstore **cache_out,
                                        lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL || cache_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest view requires context", NULL, NULL,
                        "pouch");
  }
  *cache_out = NULL;
  memset(manifest, 0, sizeof(*manifest));
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc == LC_OK) {
      *cache_out = cache;
    }
    return rc;
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          ns, manifest, NULL, NULL, error);
  }
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
    }
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  } else {
    *cache_out = cache;
  }
  return rc;
}

int lc_pouch_state_warm_namespace(lc_pouch *pouch, const char *ns,
                                  lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace warm requires pouch and namespace",
                        NULL, NULL, "pouch");
  }
  memset(&cache_guard, 0, sizeof(cache_guard));
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, NULL, NULL, error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  } else {
    rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, &manifest, error);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  return rc;
}

static int
lc_pouch_state_cache_lookup(lc_pouch *pouch, const char *ns,
                            const lc_pouch_namespace_manifest *manifest,
                            const char *key, lc_pouch_state_entry *out,
                            lc_pouch_generation *max_version_out,
                            lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  int rc;

  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (max_version_out != NULL) {
    *max_version_out = cache->max_version;
  }
  record = lc_pouch_state_cache_record_find(cache, key);
  return lc_pouch_state_entry_from_cache_record(pouch, record, out, error);
}

static int lc_pouch_state_read_result_from_entry(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    int include_body, lc_pouch_state_read_result *out, lc_error *error) {
  char *payload_span_path;
  char *crypto_context;
  char *binding_context;
  int rc;

  if (pouch == NULL || manifest == NULL || current == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state read result requires pouch, manifest, "
                        "entry, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (!current->found) {
    return LC_OK;
  }
  if (include_body && !current->payload_span.present) {
    return LC_OK;
  }
  if (!include_body && !current->payload_span.present) {
    rc = LC_OK;
  } else if (!current->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
  } else {
    rc = LC_OK;
  }
  if (include_body) {
    payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest->namespace_path, &current->payload_span,
        error);
    if (payload_span_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state payload span", NULL,
                          NULL, NULL);
    }
    crypto_context = lc_pouch_state_payload_context_for_read(
        &pouch->allocator, ns, current->key, current->version,
        lc_pouch_state_payload_class(current->record_type,
                                     current->object_link),
        current->payload_context);
    binding_context = lc_pouch_state_crypto_context(
        &pouch->allocator, ns, current->key, current->version,
        lc_pouch_state_payload_class(current->record_type,
                                     current->object_link));
    if (crypto_context == NULL || binding_context == NULL) {
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      lc_free_with_allocator(&pouch->allocator, binding_context);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state crypto context", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_source_from_span(
        pouch, crypto_context, binding_context, payload_span_path,
        current->payload_span.payload_offset,
        current->payload_span.payload_length, current->descriptor, &out->body,
        error);
    lc_free_with_allocator(&pouch->allocator, crypto_context);
    lc_free_with_allocator(&pouch->allocator, binding_context);
    lc_free_with_allocator(&pouch->allocator, payload_span_path);
  }
  if (rc == LC_OK) {
    out->content_type = current->content_type;
    out->etag = current->etag;
    out->descriptor = current->descriptor;
    out->metadata = current->metadata;
    out->metadata_length = current->metadata_length;
    out->index_seq = current->index_seq;
    out->version = current->version;
    out->bytes = current->bytes;
    out->cipher_bytes = current->cipher_bytes;
    out->updated_at_unix = current->updated_at_unix;
    out->retention = current->retention;
    out->has_query_hidden = current->has_query_hidden;
    out->query_hidden = current->query_hidden;
    out->has_body = current->payload_span.present;
    out->staged_delete_marker = current->staged_delete_marker;
    out->found = 1;
    current->content_type = NULL;
    current->etag = NULL;
    current->descriptor = NULL;
    current->metadata = NULL;
    current->metadata_length = 0U;
  }
  return rc;
}

static int lc_pouch_state_read_result_from_cache_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record,
    lc_pouch_state_read_result *out, lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached read result requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (record == NULL || !record->found) {
    return LC_OK;
  }
  if (!record->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached state payload span is invalid", NULL,
                        NULL, "pouch");
  }
  out->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  out->etag = record->etag != NULL
                  ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
                  : NULL;
  out->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (out->metadata != NULL) {
      memcpy(out->metadata, record->metadata, record->metadata_length);
      out->metadata_length = record->metadata_length;
    }
  }
  if ((record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL) ||
      (record->metadata_length > 0U && out->metadata == NULL)) {
    lc_pouch_state_read_result_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch cached read metadata", NULL,
                        NULL, NULL);
  }
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->retention = record->retention;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->staged_delete_marker = record->staged_delete_marker;
  out->index_seq = record->index_seq;
  out->has_body = record->payload_span.present;
  out->found = 1;
  return LC_OK;
}

static int lc_pouch_state_snapshot_write_record(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    int fd, const lc_pouch_state_cache_record *record, unsigned char *io_buffer,
    lc_error *error) {
  char *old_path;
  lc_pouch_state_payload_span snapshot_span;
  unsigned char *meta;
  unsigned char *final_meta;
  uint64_t cipher_bytes;
  unsigned long stored_crc;
  uint64_t record_start;
  uint64_t payload_offset;
  size_t meta_len;
  size_t final_meta_len;
  int rc;
  unsigned char put_record_type;
  unsigned char delete_record_type;

  (void)ns;
  memset(&snapshot_span, 0, sizeof(snapshot_span));
  old_path = NULL;
  meta = NULL;
  final_meta = NULL;
  record_start = 0U;
  stored_crc = 0UL;
  put_record_type = record->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT
                        ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                        : LC_POUCH_STATE_RECORD_STATE_PUT;
  delete_record_type =
      record->record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE
          ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
          : LC_POUCH_STATE_RECORD_STATE_DELETE;
  if (record->found) {
    rc = lc_pouch_state_fd_end(fd, &record_start,
                               "failed to seek pouch snapshot append offset",
                               error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!record->payload_span.present) {
      put_record_type = LC_POUCH_STATE_RECORD_STATE_META;
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, record->version, record->updated_at_unix,
          record->bytes, record->cipher_bytes, record->content_type,
          record->etag, record->descriptor, NULL, record->payload_context,
          record->metadata, record->metadata_length, 0U,
          record->has_query_hidden, record->query_hidden,
          record->staged_delete_marker, record->retention, &meta, &meta_len,
          error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_sequences(
            &pouch->allocator, &meta, &meta_len, record->index_seq,
            record->query_index_seq, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_state_record_write_prefix(
            fd, put_record_type, record->key, strlen(record->key), meta,
            meta_len, 0U, 0UL, 0UL, error);
      }
      lc_free_with_allocator(&pouch->allocator, meta);
      return rc;
    }
    rc = lc_pouch_state_payload_span_set(&pouch->allocator, &snapshot_span,
                                         snapshot_leaf, record_start, 0UL, 0UL,
                                         0UL, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, record->version, record->updated_at_unix, 0UL, 0UL,
        record->content_type, record->etag, NULL, &snapshot_span,
        record->payload_context, record->metadata, record->metadata_length,
        LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, record->has_query_hidden,
        record->query_hidden, record->staged_delete_marker, record->retention,
        &meta, &meta_len, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &meta, &meta_len, record->index_seq,
        record->query_index_seq, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      return rc;
    }
    if (record_start > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
        (uint64_t)strlen(record->key) >
            LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - record_start ||
        (uint64_t)meta_len > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                                 record_start - (uint64_t)strlen(record->key)) {
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot record offset exceeds u64", NULL,
                          NULL, "pouch");
    }
    payload_offset = record_start + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                     (uint64_t)strlen(record->key) + (uint64_t)meta_len;
    old_path = lc_pouch_state_payload_span_path(&pouch->allocator,
                                                manifest->namespace_path,
                                                &record->payload_span, error);
    if (old_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_free_with_allocator(&pouch->allocator, old_path);
      return error != NULL && error->code != LC_OK
                 ? error->code
                 : lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to prepare pouch snapshot payload ref",
                                NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_record_write_prefix(
        fd, put_record_type, record->key, strlen(record->key), meta, meta_len,
        0U, 0UL, LC_POUCH_RECORD_FLAG_PENDING, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_copy_file_span_to_fd_crc(
          pouch, old_path, record->payload_span.payload_offset,
          record->payload_span.payload_length, fd, io_buffer, &stored_crc,
          error);
    }
    cipher_bytes = record->payload_span.payload_length;
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &snapshot_span, snapshot_leaf, record_start,
          payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, record->version, record->updated_at_unix,
          record->bytes, cipher_bytes, record->content_type, record->etag,
          record->descriptor, &snapshot_span, record->payload_context,
          record->metadata, record->metadata_length,
          LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, record->has_query_hidden,
          record->query_hidden, record->staged_delete_marker, record->retention,
          &final_meta, &final_meta_len, error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_sequences(
            &pouch->allocator, &final_meta, &final_meta_len, record->index_seq,
            record->query_index_seq, error);
      }
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot metadata reservation changed size",
                          NULL, NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_finalize(
          fd, record_start, put_record_type, record->key, strlen(record->key),
          final_meta, final_meta_len, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_fd_end(
          fd, &payload_offset, "failed to restore pouch snapshot append offset",
          error);
    }
    if (rc != LC_OK) {
      lc_pouch_state_truncate_fd_best_effort(fd, record_start);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_free_with_allocator(&pouch->allocator, final_meta);
      lc_free_with_allocator(&pouch->allocator, old_path);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      return rc;
    }
  } else {
    old_path = NULL;
    cipher_bytes = 0UL;
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, record->version, record->updated_at_unix,
        record->etag, &meta, &meta_len, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_sequences(
          &pouch->allocator, &meta, &meta_len, record->index_seq,
          record->query_index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_write_prefix(
          fd, delete_record_type, record->key, strlen(record->key), meta,
          meta_len, 0U, 0UL, 0UL, error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  lc_free_with_allocator(&pouch->allocator, final_meta);
  lc_free_with_allocator(&pouch->allocator, old_path);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
  return rc;
}

static int lc_pouch_state_snapshot_write_high_water(
    int fd, lc_pouch_generation high_water_version,
    lc_pouch_generation query_high_water_version, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  meta = NULL;
  meta_len = 0U;
  rc = lc_pouch_state_encode_high_water_meta(NULL, high_water_version,
                                             query_high_water_version, &meta,
                                             &meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_prefix(
        fd, LC_POUCH_STATE_RECORD_HIGH_WATER, NULL, 0U, meta, meta_len, 0U, 0UL,
        0UL, error);
  }
  lc_free_with_allocator(NULL, meta);
  return rc;
}

static int lc_pouch_state_clean_projection_write_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record, int fd,
    lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  unsigned char record_type;
  unsigned long payload_flags;
  int rc;

  if (pouch == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch clean projection record requires inputs", NULL,
                        NULL, "pouch");
  }
  meta = NULL;
  meta_len = 0U;
  payload_flags = 0UL;
  if (record->found) {
    record_type = record->payload_span.present
                      ? LC_POUCH_STATE_RECORD_STATE_LINK
                      : LC_POUCH_STATE_RECORD_STATE_META;
    if (record->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT) {
      payload_flags |= LC_POUCH_STATE_PAYLOAD_FLAG_OBJECT_LINK;
    }
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, record->version, record->updated_at_unix,
        record->bytes, record->cipher_bytes, record->content_type, record->etag,
        record->descriptor,
        record->payload_span.present ? &record->payload_span : NULL,
        record->payload_context, record->metadata, record->metadata_length,
        payload_flags, record->has_query_hidden, record->query_hidden,
        record->staged_delete_marker, record->retention, &meta, &meta_len,
        error);
  } else {
    record_type = record->record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE
                      ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                      : LC_POUCH_STATE_RECORD_STATE_DELETE;
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, record->version, record->updated_at_unix,
        record->etag, &meta, &meta_len, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &meta, &meta_len, record->index_seq,
        record->query_index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_prefix(fd, record_type, record->key,
                                            strlen(record->key), meta, meta_len,
                                            0U, 0UL, 0UL, error);
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_write_snapshot(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, lc_pouch_namespace_logstore *cache,
    const lc_pouch_state_compaction_capture *capture, lc_error *error) {
  lc_pouch_state_cache_record **records;
  char tmp_leaf[128];
  char *snapshot_path;
  char *tmp_path;
  size_t record_count;
  size_t index;
  int fd;
  int rc;
  int len;

  len = snprintf(tmp_leaf, sizeof(tmp_leaf), "%s.tmp", snapshot_leaf);
  if (len < 0 || (size_t)len >= sizeof(tmp_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state snapshot temp path exceeds limit", NULL,
                        NULL, NULL);
  }
  snapshot_path = lc_pouch_state_child_path(
      &pouch->allocator, manifest->namespace_path, "snapshots", snapshot_leaf);
  tmp_path = snapshot_path != NULL
                 ? lc_pouch_state_child_path(&pouch->allocator,
                                             manifest->namespace_path,
                                             "snapshots", tmp_leaf)
                 : NULL;
  if (snapshot_path == NULL || tmp_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot path", NULL,
                        NULL, NULL);
  }
  fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state snapshot",
                        strerror(errno), NULL, NULL);
  }
  rc = LC_OK;
  records = NULL;
  record_count = 0U;
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  if (rc != LC_OK) {
    (void)close(fd);
    unlink(tmp_path);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return rc;
  }
  rc = lc_pouch_state_snapshot_write_high_water(
      fd, cache->max_version, cache->max_query_index_seq, error);
  for (index = 0U;
       rc == LC_OK &&
       index < (capture != NULL ? capture->captured_count : record_count);
       ++index) {
    lc_pouch_state_cache_record *record;

    if (capture != NULL) {
      record = lc_pouch_state_cache_record_index_find(
          records, record_count, capture->captured_keys[index]);
      if (!lc_pouch_state_compaction_record_ref_matches(
              record, capture->captured_record_containers[index],
              capture->captured_record_offsets[index])) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction captured ref drifted", NULL, NULL,
                          "pouch");
        break;
      }
    } else {
      record = records[index];
    }
    if (rc != LC_OK) {
      break;
    }
    rc = capture != NULL
             ? lc_pouch_state_snapshot_write_record(pouch, cache->ns, manifest,
                                                    snapshot_leaf, fd, record,
                                                    capture->io_buffer, error)
             : lc_pouch_state_clean_projection_write_record(pouch, record, fd,
                                                            error);
  }
  lc_free_with_allocator(&pouch->allocator, records);
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && rename(tmp_path, snapshot_path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to install pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc != LC_OK) {
    unlink(tmp_path);
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_path);
  lc_free_with_allocator(&pouch->allocator, tmp_path);
  return rc;
}

static int lc_pouch_state_read_text_file(lc_pouch *pouch, const char *path,
                                         char **out, size_t *out_length,
                                         lc_error *error) {
  char buffer[4096];
  char *bytes;
  FILE *fp;
  size_t capacity;
  size_t length;
  int rc;

  *out = NULL;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch text file", strerror(errno), path,
                        NULL);
  }
  bytes = NULL;
  capacity = 0U;
  length = 0U;
  rc = LC_OK;
  while (!feof(fp)) {
    size_t got;

    got = fread(buffer, 1U, sizeof(buffer), fp);
    if (got != 0U) {
      if (length + got + 1U < length) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
        break;
      }
      if (length + got + 1U > capacity) {
        char *grown;
        size_t next_capacity;

        next_capacity = capacity == 0U ? 4096U : capacity;
        while (next_capacity < length + got + 1U) {
          if (next_capacity > ((size_t)-1) / 2U) {
            rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch text file", NULL, path,
                              NULL);
            break;
          }
          next_capacity *= 2U;
        }
        if (rc != LC_OK) {
          break;
        }
        grown = (char *)lc_realloc_with_allocator(&pouch->allocator, bytes,
                                                  next_capacity);
        if (grown == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch text file", NULL, path,
                            NULL);
          break;
        }
        bytes = grown;
        capacity = next_capacity;
      }
      memcpy(bytes + length, buffer, got);
      length += got;
    }
    if (ferror(fp)) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch text file", strerror(errno), path,
                        NULL);
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch text file", strerror(errno), path,
                      NULL);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return rc;
  }
  if (bytes == NULL) {
    bytes = lc_strdup_with_allocator(&pouch->allocator, "");
    if (bytes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
    }
  } else {
    bytes[length] = '\0';
  }
  *out = bytes;
  if (out_length != NULL) {
    *out_length = length;
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_capture_cleanup(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture) {
  size_t index;

  if (capture == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->manifest_text);
  lc_free_with_allocator(&pouch->allocator, capture->io_buffer);
  for (index = 0U; index < capture->candidate_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_snapshots);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_sizes);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_checksums);
  for (index = 0U; index < capture->captured_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->captured_keys[index]);
    lc_free_with_allocator(&pouch->allocator,
                           capture->captured_record_containers[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->captured_keys);
  lc_free_with_allocator(&pouch->allocator,
                         capture->captured_record_containers);
  lc_free_with_allocator(&pouch->allocator, capture->captured_record_offsets);
  memset(capture, 0, sizeof(*capture));
}

static int lc_pouch_state_manifest_leaf_obsolete(
    const lc_pouch_namespace_manifest *manifest, const char *leaf,
    int snapshot) {
  char **items;
  unsigned long count;
  unsigned long index;

  if (manifest == NULL || leaf == NULL) {
    return 0;
  }
  items = snapshot ? manifest->obsolete_snapshots : manifest->obsolete_segments;
  count = snapshot ? manifest->obsolete_snapshot_count
                   : manifest->obsolete_segment_count;
  for (index = 0UL; index < count; ++index) {
    if (items[index] != NULL && strcmp(items[index], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_state_compaction_candidate_index(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  size_t index;

  if (capture == NULL || leaf == NULL) {
    return -1;
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_leaves[index] != NULL &&
        strcmp(capture->candidate_leaves[index], leaf) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static int lc_pouch_state_compaction_candidate_contains(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  return lc_pouch_state_compaction_candidate_index(capture, leaf) >= 0;
}

static int lc_pouch_state_compaction_candidate_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_namespace_manifest *manifest, const char *leaf, int snapshot,
    lc_error *error) {
  char *path;
  char *leaf_copy;
  uint64_t size = 0U;
  int rc;

  if (capture == NULL || leaf == NULL || leaf[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_state_compaction_candidate_contains(capture, leaf)) {
    return LC_OK;
  }
  if (capture->candidate_count >= capture->candidate_capacity) {
    char **next_leaves;
    int *next_snapshots;
    uint64_t *next_sizes;
    unsigned long *next_checksums;
    size_t next_capacity;

    next_capacity = capture->candidate_capacity == 0U
                        ? 8U
                        : capture->candidate_capacity * 2U;
    if (next_capacity <= capture->candidate_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction candidate list exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_leaves = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_leaves,
        next_capacity * sizeof(capture->candidate_leaves[0]));
    if (next_leaves == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidates",
                          NULL, NULL, NULL);
    }
    capture->candidate_leaves = next_leaves;
    next_snapshots = (int *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_snapshots,
        next_capacity * sizeof(capture->candidate_snapshots[0]));
    if (next_snapshots == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "types",
                          NULL, NULL, NULL);
    }
    capture->candidate_snapshots = next_snapshots;
    next_sizes = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_sizes,
        next_capacity * sizeof(capture->candidate_sizes[0]));
    if (next_sizes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "sizes",
                          NULL, NULL, NULL);
    }
    capture->candidate_sizes = next_sizes;
    next_checksums = (unsigned long *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_checksums,
        next_capacity * sizeof(capture->candidate_checksums[0]));
    if (next_checksums == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "checksums",
                          NULL, NULL, NULL);
    }
    capture->candidate_checksums = next_checksums;
    capture->candidate_capacity = next_capacity;
  }
  leaf_copy = lc_strdup_with_allocator(&pouch->allocator, leaf);
  if (leaf_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction candidate", NULL, NULL,
                        NULL);
  }
  path = lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                   snapshot ? "snapshots" : "segments", leaf);
  if (path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction candidate path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_file_size(path, &size, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_file_checksum(
        path, size, capture->io_buffer,
        &capture->candidate_checksums[capture->candidate_count], error);
  }
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return rc;
  }
  if (size > LC_U64_MAX - capture->candidate_bytes) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate bytes overflow", NULL, NULL,
                        "pouch");
  }
  capture->candidate_leaves[capture->candidate_count] = leaf_copy;
  capture->candidate_snapshots[capture->candidate_count] = snapshot ? 1 : 0;
  capture->candidate_sizes[capture->candidate_count] = size;
  ++capture->candidate_count;
  capture->candidate_bytes += size;
  return LC_OK;
}

static int lc_pouch_state_compaction_build_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  unsigned long segment_index;
  int rc;

  if (manifest->latest_snapshot != NULL &&
      !lc_pouch_state_manifest_leaf_obsolete(manifest,
                                             manifest->latest_snapshot, 1)) {
    rc = lc_pouch_state_compaction_candidate_add(
        pouch, capture, manifest, manifest->latest_snapshot, 1, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf = manifest->segment_leaves[segment_index];

    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) ||
        (manifest->active_segment != NULL &&
         strcmp(segment_leaf, manifest->active_segment) == 0)) {
      continue;
    }
    rc = lc_pouch_state_compaction_candidate_add(pouch, capture, manifest,
                                                 segment_leaf, 0, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_candidate_remove_at(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture, size_t index) {
  if (capture == NULL || index >= capture->candidate_count) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  if (capture->candidate_bytes >= capture->candidate_sizes[index]) {
    capture->candidate_bytes -= capture->candidate_sizes[index];
  } else {
    capture->candidate_bytes = 0UL;
  }
  if (index + 1U < capture->candidate_count) {
    memmove(capture->candidate_leaves + index,
            capture->candidate_leaves + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_leaves[0]));
    memmove(capture->candidate_snapshots + index,
            capture->candidate_snapshots + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_snapshots[0]));
    memmove(capture->candidate_sizes + index,
            capture->candidate_sizes + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_sizes[0]));
    memmove(capture->candidate_checksums + index,
            capture->candidate_checksums + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_checksums[0]));
  }
  --capture->candidate_count;
}

static int lc_pouch_state_compaction_protect_live_links(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, int *blocked, lc_error *error) {
  lc_pouch_state_cache_record *record;

  (void)error;
  if (blocked != NULL) {
    *blocked = 0;
  }
  for (record = cache->records; record != NULL; record = record->next) {
    int payload_candidate;
    int record_candidate;

    if (!record->payload_span.present ||
        !lc_pouch_state_compaction_candidate_contains(
            capture, record->payload_span.container_leaf)) {
      continue;
    }
    record_candidate =
        record->has_record_ref && lc_pouch_state_compaction_candidate_contains(
                                      capture, record->record_container_leaf);
    if (record_candidate) {
      continue;
    }
    payload_candidate = lc_pouch_state_compaction_candidate_index(
        capture, record->payload_span.container_leaf);
    if (payload_candidate < 0) {
      continue;
    }
    if (capture->candidate_snapshots[payload_candidate]) {
      if (blocked != NULL) {
        *blocked = 1;
      }
      return LC_OK;
    }
    lc_pouch_state_compaction_candidate_remove_at(pouch, capture,
                                                  (size_t)payload_candidate);
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_record_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  char *key_copy;
  char *container_copy;

  if (capture->captured_count >= capture->captured_capacity) {
    char **next_keys;
    char **next_containers;
    uint64_t *next_offsets;
    size_t next_capacity;

    next_capacity = capture->captured_capacity == 0U
                        ? 32U
                        : capture->captured_capacity * 2U;
    if (next_capacity <= capture->captured_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction captured refs exceed local limit",
                          NULL, NULL, NULL);
    }
    next_keys = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_keys,
        next_capacity * sizeof(capture->captured_keys[0]));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured keys",
                          NULL, NULL, NULL);
    }
    capture->captured_keys = next_keys;
    next_containers = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_containers,
        next_capacity * sizeof(capture->captured_record_containers[0]));
    if (next_containers == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "containers",
                          NULL, NULL, NULL);
    }
    capture->captured_record_containers = next_containers;
    next_offsets = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_offsets,
        next_capacity * sizeof(capture->captured_record_offsets[0]));
    if (next_offsets == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "offsets",
                          NULL, NULL, NULL);
    }
    capture->captured_record_offsets = next_offsets;
    capture->captured_capacity = next_capacity;
  }
  key_copy = lc_strdup_with_allocator(&pouch->allocator, record->key);
  container_copy = lc_strdup_with_allocator(&pouch->allocator,
                                            record->record_container_leaf);
  if (key_copy == NULL || container_copy == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_copy);
    lc_free_with_allocator(&pouch->allocator, container_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction captured ref", NULL,
                        NULL, NULL);
  }
  capture->captured_keys[capture->captured_count] = key_copy;
  capture->captured_record_containers[capture->captured_count] = container_copy;
  capture->captured_record_offsets[capture->captured_count] =
      record->record_offset;
  ++capture->captured_count;
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_records(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  lc_pouch_state_cache_record **records;
  size_t record_count;
  size_t index;
  int rc;

  records = NULL;
  record_count = 0U;
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  for (index = 0U; rc == LC_OK && index < record_count; ++index) {
    lc_pouch_state_cache_record *record;

    record = records[index];
    if (!record->found || !record->has_record_ref) {
      continue;
    }
    if (!lc_pouch_state_compaction_candidate_contains(
            capture, record->record_container_leaf)) {
      continue;
    }
    rc = lc_pouch_state_compaction_capture_record_add(pouch, capture, record,
                                                      error);
  }
  lc_free_with_allocator(&pouch->allocator, records);
  return rc;
}

static int lc_pouch_state_compaction_record_ref_matches(
    const lc_pouch_state_cache_record *record, const char *container,
    uint64_t offset) {
  return record != NULL && record->has_record_ref &&
         record->record_container_leaf != NULL &&
         strcmp(record->record_container_leaf, container) == 0 &&
         record->record_offset == offset;
}

static int lc_pouch_state_compaction_capture_now(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  char *manifest_path;
  int rc;

  memset(capture, 0, sizeof(*capture));
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path,
                                     &capture->manifest_text, NULL, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  capture->io_buffer = (unsigned char *)lc_alloc_with_allocator(
      &pouch->allocator, LC_POUCH_STATE_FILE_CHUNK_BYTES);
  if (capture->io_buffer == NULL) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction I/O buffer", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_compaction_build_candidates(pouch, manifest, capture,
                                                  error);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
  }
  return rc;
}

static int lc_pouch_state_compaction_validate_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *capture, lc_error *error) {
  size_t index;

  if (pouch == NULL || manifest == NULL || capture == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate validation requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    char *path;
    unsigned long checksum;
    int rc;

    path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path,
        capture->candidate_snapshots[index] ? "snapshots" : "segments",
        capture->candidate_leaves[index]);
    if (path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate path",
                          NULL, NULL, NULL);
    }
    checksum = 0UL;
    rc = lc_pouch_state_file_checksum(path, capture->candidate_sizes[index],
                                      capture->io_buffer, &checksum, error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc != LC_OK) {
      return rc;
    }
    if (checksum != capture->candidate_checksums[index]) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_validate_capture(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *before,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  lc_pouch_state_cache_record **records;
  char *manifest_path;
  char *manifest_text;
  size_t record_count;
  size_t index;
  int rc;

  records = NULL;
  record_count = 0U;
  manifest_text = NULL;
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path, &manifest_text, NULL,
                                     error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (before->manifest_text == NULL || manifest_text == NULL ||
      strcmp(before->manifest_text, manifest_text) != 0) {
    lc_free_with_allocator(&pouch->allocator, manifest_text);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction validation drift", NULL, NULL,
                        "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_text);
  rc = lc_pouch_state_compaction_validate_candidates(pouch, manifest, before,
                                                     error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; index < before->captured_count; ++index) {
    lc_pouch_state_cache_record *record;

    record = lc_pouch_state_cache_record_index_find(
        records, record_count, before->captured_keys[index]);
    if (!lc_pouch_state_compaction_record_ref_matches(
            record, before->captured_record_containers[index],
            before->captured_record_offsets[index])) {
      lc_free_with_allocator(&pouch->allocator, records);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  lc_free_with_allocator(&pouch->allocator, records);
  return LC_OK;
}

static int lc_pouch_maintenance_set_string(lc_pouch *pouch, char **target,
                                           const char *value, lc_error *error) {
  char *copy;

  if (target == NULL) {
    return LC_OK;
  }
  copy =
      lc_strdup_with_allocator(&pouch->allocator, value != NULL ? value : "");
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch maintenance diagnostic", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(&pouch->allocator, *target);
  *target = copy;
  return LC_OK;
}

static int lc_pouch_maintenance_set_diagnostic(lc_pouch *pouch,
                                               lc_pouch_maintenance_result *out,
                                               const char *diagnostic,
                                               lc_error *error) {
  if (out == NULL) {
    return LC_OK;
  }
  return lc_pouch_maintenance_set_string(pouch, &out->diagnostic, diagnostic,
                                         error);
}

static void lc_pouch_maintenance_set_diagnostic_best_effort(
    lc_pouch *pouch, lc_pouch_maintenance_result *out, const char *diagnostic) {
  char *copy;

  if (out == NULL) {
    return;
  }
  copy = lc_strdup_with_allocator(&pouch->allocator,
                                  diagnostic != NULL ? diagnostic : "");
  if (copy == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, out->diagnostic);
  out->diagnostic = copy;
}

static void lc_pouch_maintenance_mark_aborted(lc_pouch *pouch,
                                              lc_pouch_maintenance_result *out,
                                              const char *diagnostic) {
  if (out == NULL) {
    return;
  }
  out->aborted = 1;
  out->compacted = 0;
  out->skipped = 0;
  lc_pouch_maintenance_set_diagnostic_best_effort(
      pouch, out, diagnostic != NULL ? diagnostic : "compaction-aborted");
}

static lc_pouch_unix_seconds lc_pouch_maintenance_now_seconds(void) {
  time_t now;

  now = time(NULL);
  if (now <= 0) {
    return 0;
  }
  if ((uintmax_t)now > (uintmax_t)LC_I64_MAX) {
    return LC_I64_MAX;
  }
  return (lc_pouch_unix_seconds)now;
}

typedef struct lc_pouch_retention_key {
  char *key;
  lc_pouch_generation version;
} lc_pouch_retention_key;

typedef struct lc_pouch_retention_scan {
  lc_pouch *pouch;
  lc_pouch_unix_seconds cutoff_unix;
  lc_pouch_retention_key *keys;
  size_t count;
  size_t capacity;
  unsigned long scanned_count;
  unsigned long expired_count;
} lc_pouch_retention_scan;

static void lc_pouch_retention_scan_cleanup(lc_pouch_retention_scan *scan) {
  size_t index;

  if (scan == NULL || scan->pouch == NULL) {
    return;
  }
  for (index = 0U; index < scan->count; ++index) {
    lc_free_with_allocator(&scan->pouch->allocator, scan->keys[index].key);
  }
  lc_free_with_allocator(&scan->pouch->allocator, scan->keys);
  memset(scan, 0, sizeof(*scan));
}

static int lc_pouch_retention_scan_add(lc_pouch_retention_scan *scan,
                                       const char *key,
                                       lc_pouch_generation version,
                                       lc_error *error) {
  lc_pouch_retention_key *next_keys;
  size_t next_capacity;
  char *copy;

  if (scan->count >= scan->capacity) {
    next_capacity = scan->capacity == 0U ? 16U : scan->capacity * 2U;
    if (next_capacity <= scan->capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch retention key list exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_keys = (lc_pouch_retention_key *)lc_realloc_with_allocator(
        &scan->pouch->allocator, scan->keys,
        next_capacity * sizeof(*next_keys));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch retention key list", NULL,
                          NULL, NULL);
    }
    scan->keys = next_keys;
    scan->capacity = next_capacity;
  }
  copy = lc_strdup_with_allocator(&scan->pouch->allocator, key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch retention key", NULL, NULL, NULL);
  }
  scan->keys[scan->count].key = copy;
  scan->keys[scan->count].version = version;
  ++scan->count;
  ++scan->expired_count;
  return LC_OK;
}

static int
lc_pouch_retention_scan_visit(const lc_pouch_state_visit_entry *entry,
                              void *context, lc_error *error) {
  lc_pouch_retention_scan *scan;

  scan = (lc_pouch_retention_scan *)context;
  if (scan == NULL || entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  ++scan->scanned_count;
  /* Stateful outbox checkpoints outlive individual attempts. Their parent
   * controls the retry/dead-letter lifecycle, so timestamp retention cannot
   * safely decide whether an otherwise old checkpoint remains recoverable. */
  if (strncmp(entry->key, "__lockdc_io/v1/outbox-state/",
              sizeof("__lockdc_io/v1/outbox-state/") - 1U) == 0) {
    return LC_OK;
  }
  if (entry->updated_at_unix > 0L &&
      entry->updated_at_unix < scan->cutoff_unix) {
    return lc_pouch_retention_scan_add(scan, entry->key, entry->version, error);
  }
  return LC_OK;
}

static int lc_pouch_state_retention_sweep(lc_pouch *pouch, const char *ns,
                                          lc_pouch_unix_seconds cutoff_unix,
                                          lc_pouch_maintenance_result *out,
                                          lc_error *error) {
  lc_pouch_retention_scan scan;
  unsigned long deleted_count;
  unsigned long failed_count;
  size_t index;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || cutoff_unix <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retention sweep requires namespace and positive "
                        "cutoff",
                        NULL, NULL, NULL);
  }
  if (out != NULL) {
    rc = lc_pouch_maintenance_set_string(pouch, &out->ns, ns, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(&scan, 0, sizeof(scan));
  scan.pouch = pouch;
  scan.cutoff_unix = cutoff_unix;
  rc = lc_pouch_state_visit_internal(
      pouch, ns, NULL, lc_pouch_retention_scan_visit, &scan, 1, error);
  if (rc != LC_OK) {
    lc_pouch_retention_scan_cleanup(&scan);
    return rc;
  }
  deleted_count = 0UL;
  failed_count = 0UL;
  for (index = 0U; index < scan.count; ++index) {
    lc_pouch_state_write_options delete_options;
    lc_pouch_state_write_result delete_result;
    lc_error delete_error;

    memset(&delete_options, 0, sizeof(delete_options));
    memset(&delete_result, 0, sizeof(delete_result));
    lc_error_init(&delete_error);
    delete_options.has_expected_version = 1;
    delete_options.expected_version = scan.keys[index].version;
    rc = lc_pouch_state_delete_locked(pouch, ns, scan.keys[index].key,
                                      &delete_options, &delete_result,
                                      &delete_error);
    if (rc == LC_OK) {
      ++deleted_count;
    } else {
      ++failed_count;
    }
    lc_error_cleanup(&delete_error);
    lc_pouch_state_write_result_cleanup(&pouch->allocator, &delete_result);
  }
  if (out != NULL) {
    out->retention_scanned_count = scan.scanned_count;
    out->retention_expired_count = scan.expired_count;
    out->retention_deleted_state_count = deleted_count;
    out->retention_failed_count = failed_count;
  }
  lc_pouch_retention_scan_cleanup(&scan);
  return lc_pouch_maintenance_set_diagnostic(
      pouch, out,
      failed_count != 0UL ? "retention-failures" : "retention-complete", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    const char **abort_diagnostic, int *compacted_out, lc_error *error);

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest, const char *ns,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error);

static int lc_pouch_state_payload_container_is_valid(const char *leaf) {
  size_t len;
  size_t i;
  uint64_t segment_id;

  if (leaf == NULL) {
    return 0;
  }
  len = strlen(leaf);
  if (lc_pouch_namespace_parse_segment_leaf(leaf, &segment_id)) {
    return 1;
  }
  if (len == 33U && strncmp(leaf, "snapshot-", 9U) == 0 &&
      strcmp(leaf + 29U, ".log") == 0) {
    for (i = 9U; i < 29U; ++i) {
      if (leaf[i] < '0' || leaf[i] > '9') {
        return 0;
      }
    }
    return 1;
  }
  return 0;
}

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest, const char *ns,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error) {
  size_t index;
  uint64_t now_unix;
  int rc;

  if (capture == NULL) {
    return LC_OK;
  }
  now_unix = (uint64_t)lc_pouch_maintenance_now_seconds();
  if (now_unix == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete file timestamp is invalid", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_snapshots[index]) {
      rc = lc_pouch_namespace_manifest_mark_obsolete_snapshot(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      rc = lc_pouch_namespace_manifest_mark_obsolete_segment(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  rc = lc_pouch_namespace_manifest_save(&pouch->allocator, ns, manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_state_source_cache_cleanup(pouch);
  return lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, manifest, now_unix,
      pouch->compaction_delete_grace_seconds, cleanup_deleted_count,
      cleanup_pending_count, error);
}

static int lc_pouch_state_compact_namespace_if_needed(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    int force, int terminal_reclaim, lc_pouch_maintenance_result *out,
    lc_error *error) {
  lc_pouch_generation oldest_history_acknowledged;
  lc_pouch_generation current_history_index_seq;
  unsigned long candidate_count;
  uint64_t candidate_bytes;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t compacted_segment_id;
  uint64_t now_seconds;
  uint64_t minimum_reclaimable_bytes;
  unsigned long minimum_segment_count;
  lc_pouch_state_compaction_capture capture;
  lc_pouch_namespace_logstore candidate_cache;
  int protected_snapshot_blocked;
  int has_history_consumers;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
    rc = lc_pouch_maintenance_set_string(pouch, &out->ns, ns, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  minimum_segment_count =
      terminal_reclaim ? 1UL : pouch->compaction_min_segment_count;
  minimum_reclaimable_bytes = terminal_reclaim
                                  ? pouch->terminal_reclaim_min_bytes
                                  : pouch->compaction_min_reclaimable_bytes;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (!force && !pouch->background_compaction_enabled) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "disabled");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "disabled", error);
  }
  has_history_consumers = 0;
  oldest_history_acknowledged = 0U;
  current_history_index_seq = 0U;
  rc = lc_pouch_history_oldest_acknowledged(
      pouch, ns, &has_history_consumers, &oldest_history_acknowledged, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (has_history_consumers) {
    rc = lc_pouch_state_index_seq(pouch, ns, &current_history_index_seq, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (has_history_consumers &&
      oldest_history_acknowledged < current_history_index_seq) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_u64_field("ack", oldest_history_acknowledged);
      fields[2] = lc_log_u64_field("current", current_history_index_seq);
      lc_log_trace(pouch->logger, "compaction.skip.history_consumer", fields,
                   3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "history-consumer-behind", error);
  }
  if (terminal_reclaim && manifest->active_segment_id != LC_U64_MAX) {
    char *active_path;
    uint64_t active_size;

    active_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    active_size = 0U;
    if (active_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch terminal reclaim segment",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_file_size(active_path, &active_size, error);
    lc_free_with_allocator(&pouch->allocator, active_path);
    if (rc != LC_OK) {
      return rc;
    }
    if (active_size >= pouch->terminal_reclaim_min_bytes) {
      lc_pouch_namespace_logstore *cache;
      char *next_active_path;
      int next_active_fd;

      rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, ns, manifest,
                                              manifest->active_segment_id + 1U,
                                              error);
      if (rc != LC_OK) {
        return rc;
      }
      next_active_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                    "segments", manifest->active_segment);
      if (next_active_path == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch terminal active segment",
                            NULL, NULL, "pouch");
      }
      next_active_fd = open(next_active_path, O_WRONLY | O_CREAT, 0666);
      if (next_active_fd < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch terminal active segment",
                          strerror(errno), next_active_path, "pouch");
      } else if (close(next_active_fd) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to close pouch terminal active segment",
                          strerror(errno), next_active_path, "pouch");
      }
      lc_free_with_allocator(&pouch->allocator, next_active_path);
      if (rc != LC_OK) {
        return rc;
      }
      cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
      if (cache != NULL && cache->initialized &&
          lc_pouch_state_cache_set_active_segment(pouch, cache, manifest, 0U,
                                                  error) != LC_OK) {
        lc_pouch_state_cache_invalidate_namespace(pouch, ns);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
    }
  }
  candidate_count = 0UL;
  {
    unsigned long segment_index;

    for (segment_index = 0UL; segment_index < manifest->segment_count;
         ++segment_index) {
      const char *segment_leaf = manifest->segment_leaves[segment_index];

      if (lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) &&
          (manifest->active_segment == NULL ||
           strcmp(segment_leaf, manifest->active_segment) != 0)) {
        ++candidate_count;
      }
    }
  }
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  memset(&capture, 0, sizeof(capture));
  memset(&candidate_cache, 0, sizeof(candidate_cache));
  protected_snapshot_blocked = 0;
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_cache.ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (candidate_cache.ns == NULL) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction namespace name",
                        NULL, NULL, NULL);
  }
  rc =
      lc_pouch_state_cache_refresh(pouch, &candidate_cache, manifest, 1, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_compaction_protect_live_links(
        pouch, &capture, &candidate_cache, &protected_snapshot_blocked, error);
  }
  lc_free_with_allocator(&pouch->allocator, candidate_cache.ns);
  candidate_cache.ns = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                            &candidate_cache);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_bytes = capture.candidate_bytes;
  candidate_count = (unsigned long)capture.candidate_count;
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
    out->candidate_bytes = candidate_bytes;
  }
  if (protected_snapshot_blocked) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "protected-live-links");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out,
                                               "protected-live-links", error);
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  if (!force && candidate_count < minimum_segment_count) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "below-segment-threshold");
      fields[2] = lc_log_u64_field("count", candidate_count);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-segment-threshold", error);
  }
  if (!force && candidate_bytes < minimum_reclaimable_bytes) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "below-reclaimable-threshold");
      fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-reclaimable-threshold", error);
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("count", candidate_count);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_bool_field("force", force);
    lc_log_debug(pouch->logger, "compaction.start", fields, 4U);
  }
  {
    const char *abort_diagnostic;

    abort_diagnostic = NULL;
    rc = lc_pouch_state_compact_namespace(
        pouch, ns, manifest, &cleanup_deleted_count, &cleanup_pending_count,
        &abort_diagnostic, out != NULL ? &out->compacted : NULL, error);
    if (rc != LC_OK) {
      lc_pouch_maintenance_mark_aborted(pouch, out, abort_diagnostic);
      {
        pslog_field fields[4];

        fields[0] = lc_log_str_field("ns", ns);
        fields[1] = lc_log_str_field("reason", abort_diagnostic);
        fields[2] = lc_log_error_field("error", error);
        fields[3] = lc_log_code_field(error);
        lc_log_error(pouch->logger, "compaction.error", fields, 4U);
      }
      return rc;
    }
    if (out != NULL && !out->compacted) {
      out->skipped = 1;
      return lc_pouch_maintenance_set_diagnostic(
          pouch, out,
          abort_diagnostic != NULL ? abort_diagnostic : "no-candidates", error);
    }
  }
  if (out != NULL) {
    out->compacted = 1;
    out->compacted_segment_id = compacted_segment_id;
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("segment", compacted_segment_id);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_u64_field("cleanup_deleted", cleanup_deleted_count);
    fields[4] = lc_log_u64_field("pending", cleanup_pending_count);
    lc_log_debug(pouch->logger, "compaction.complete", fields, 5U);
  }
  return lc_pouch_maintenance_set_diagnostic(pouch, out, "compacted", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    const char **abort_diagnostic, int *compacted_out, lc_error *error) {
  lc_pouch_namespace_logstore snapshot_cache;
  lc_pouch_state_compaction_capture capture;
  char *snapshot_leaf;
  uint64_t compacted_segment_id;
  int snapshot_installed;
  int protected_snapshot_blocked;
  int rc;

  snapshot_installed = 0;
  protected_snapshot_blocked = 0;
  if (compacted_out != NULL) {
    *compacted_out = 0;
  }
  if (abort_diagnostic != NULL) {
    *abort_diagnostic = NULL;
  }
  if (manifest->segment_count == 0UL) {
    return LC_OK;
  }
  memset(&snapshot_cache, 0, sizeof(snapshot_cache));
  snapshot_cache.ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (snapshot_cache.ns == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshot namespace name",
                        NULL, NULL, NULL);
  }
  memset(&capture, 0, sizeof(capture));
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-read-aborted";
    }
    return rc;
  }
  rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-refresh-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  rc = lc_pouch_state_compaction_protect_live_links(
      pouch, &capture, &snapshot_cache, &protected_snapshot_blocked, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "candidate-protection-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  if (protected_snapshot_blocked || capture.candidate_count == 0U) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic =
          protected_snapshot_blocked ? "protected-live-links" : "no-candidates";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return LC_OK;
  }
  /* An exclusive writer with live index maintenance carries a complete
   * in-memory capture across each mutation, so its current manifest remains
   * coupled to canonical state. A shared root, or any root opened with index
   * maintenance disabled, can discard mutations that an existing manifest has
   * not observed. Retire that derived manifest before compaction replaces the
   * topology, so a later indexed reopen rebuilds from canonical state. */
  if (!lc_pouch_single_writer_enabled(pouch) ||
      !pouch->query_indexing_enabled) {
    rc = lc_pouch_query_index_invalidate_namespace(pouch, ns, error);
    if (rc != LC_OK) {
      if (abort_diagnostic != NULL) {
        *abort_diagnostic = "query-index-invalidation-aborted";
      }
      lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
      lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
      lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                                &snapshot_cache);
      return rc;
    }
  }
  rc = lc_pouch_state_compaction_capture_records(pouch, &capture,
                                                 &snapshot_cache, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "capture-records-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-sequence-overflow";
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  snapshot_leaf =
      lc_pouch_namespace_snapshot_leaf(&pouch->allocator, compacted_segment_id);
  if (snapshot_leaf == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-prepare-aborted";
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot name", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_snapshot(pouch, manifest, snapshot_leaf,
                                     &snapshot_cache, &capture, error);
  if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_after_snapshot_write_hook != NULL) {
      rc = lc_pouch_test_after_snapshot_write_hook(
          lc_pouch_test_after_snapshot_write_context, error);
    }
#endif
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1,
                                      error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_compaction_validate_capture(pouch, manifest, &capture,
                                                      &snapshot_cache, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_install_snapshot(
          &pouch->allocator, ns, manifest, snapshot_leaf, compacted_segment_id,
          error);
      if (rc == LC_OK) {
        snapshot_installed = 1;
      } else if (abort_diagnostic != NULL) {
        *abort_diagnostic = "snapshot-install-aborted";
      }
    } else if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-drift-aborted";
    }
  } else if (abort_diagnostic != NULL) {
    *abort_diagnostic = "snapshot-write-aborted";
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_queue_compacted_files(pouch, manifest, ns, &capture,
                                              cleanup_deleted_count,
                                              cleanup_pending_count, error);
    if (rc != LC_OK && abort_diagnostic != NULL) {
      *abort_diagnostic = "obsolete-cleanup-aborted";
    }
  }
  if (rc == LC_OK) {
    lc_pouch_namespace_logstore *live_cache;

    live_cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (live_cache != NULL) {
      lc_pouch_namespace_logstore_clear_records(&pouch->allocator, live_cache);
      live_cache->initialized = 0;
      live_cache->max_segment_id = 0UL;
      live_cache->segment_count = 0UL;
      live_cache->max_version = 0UL;
      live_cache->max_query_index_seq = 0UL;
    }
  }
  if (rc == LC_OK) {
    if (compacted_out != NULL) {
      *compacted_out = 1;
    }
  } else if (!snapshot_installed) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", snapshot_leaf);
    if (snapshot_path != NULL) {
      (void)unlink(snapshot_path);
      lc_free_with_allocator(&pouch->allocator, snapshot_path);
    }
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
  snapshot_cache.ns = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, &snapshot_cache);
  return rc;
}

static int lc_pouch_maintenance_run_locked(
    lc_pouch *pouch, const lc_pouch_maintenance_options *options,
    lc_pouch_maintenance_result *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  const char *ns;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t now_seconds;
  int force;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
  }
  if (pouch == NULL || options == NULL || options->ns == NULL ||
      options->ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch maintenance requires a namespace", NULL, NULL,
                        NULL);
  }
  ns = options->ns;
  force = options->force ? 1 : 0;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_bool_field("force", force);
    fields[2] = lc_log_bool_field("cleanup_only", options->cleanup_only);
    lc_log_debug(pouch->logger, "maintenance.start", fields, 3U);
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, &cleanup_deleted_count,
                                        &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, &manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (options->retention_updated_before_unix > 0L) {
    rc = lc_pouch_state_retention_sweep(
        pouch, ns, options->retention_updated_before_unix, out, error);
  } else if (options->cleanup_only) {
    if (out != NULL) {
      out->skipped = 1;
      rc = lc_pouch_maintenance_set_string(pouch, &out->ns, ns, error);
      if (rc != LC_OK) {
        lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
        return rc;
      }
    }
    rc = lc_pouch_maintenance_set_diagnostic(
        pouch, out,
        cleanup_pending_count != 0UL ? "cleanup-pending" : "cleanup-complete",
        error);
  } else {
    rc = lc_pouch_state_compact_namespace_if_needed(
        pouch, ns, &manifest, force, options->terminal_reclaim, out, error);
    if (out != NULL) {
      out->cleanup_deleted_count += cleanup_deleted_count;
      out->cleanup_pending_count += cleanup_pending_count;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (rc == LC_OK) {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] =
        lc_log_bool_field("compacted", out != NULL ? out->compacted : 0);
    fields[2] = lc_log_bool_field("skipped", out != NULL ? out->skipped : 0);
    fields[3] =
        lc_log_str_field("reason", out != NULL ? out->diagnostic : NULL);
    fields[4] = lc_log_u64_field("reclaim_bytes",
                                 out != NULL ? out->candidate_bytes : 0UL);
    fields[5] = lc_log_u64_field(
        "cleanup_deleted", out != NULL ? out->cleanup_deleted_count : 0UL);
    fields[6] = lc_log_u64_field(
        "pending", out != NULL ? out->cleanup_pending_count : 0UL);
    lc_log_debug(pouch->logger, "maintenance.complete", fields, 7U);
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "maintenance.error", fields, 3U);
  }
  return rc;
}

int lc_pouch_state_durable_scope_include_namespace_locked(lc_pouch *pouch,
                                                          const char *ns,
                                                          lc_error *error) {
  lc_pouch_state_commit_group *scope = lc_pouch_state_commit_group_current();
  lc_pouch_namespace_manifest manifest;
  unsigned long i;
  int rc;
  if (scope == NULL || scope->pouch != pouch || !scope->force_sync ||
      !lc_pouch_state_namespace_lock_is_held(pouch, ns))
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "cleanup recovery durability requires scope and namespace authority",
        NULL, NULL, "pouch");
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, NULL, NULL, error);
  if (rc != LC_OK)
    return rc;
  for (i = 0; rc == LC_OK && i <= manifest.segment_count; ++i) {
    const char *leaf = i == manifest.segment_count ? manifest.latest_snapshot
                                                   : manifest.segment_leaves[i];
    char *path;
    int fd;
    if (leaf == NULL || leaf[0] == '\0')
      continue;
    path = lc_pouch_state_child_path(
        &pouch->allocator, manifest.namespace_path,
        i == manifest.segment_count ? "snapshots" : "segments", leaf);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate cleanup recovery durability path",
                        NULL, NULL, "pouch");
      break;
    }
    fd = open(path, O_RDWR);
    if (fd < 0)
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open cleanup recovery durability container",
                        strerror(errno), path, "pouch");
    else {
      rc = lc_pouch_state_defer_fsync(pouch, fd, error);
      if (close(fd) != 0 && rc == LC_OK)
        rc = lc_error_set(
            error, LC_ERR_TRANSPORT, 0L,
            "failed to close cleanup recovery durability container",
            strerror(errno), path, "pouch");
    }
    lc_free_with_allocator(&pouch->allocator, path);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_retention_retire_hint_locked(lc_pouch *pouch, const char *ns,
                                                lc_error *error) {
  lc_pouch_state_commit_group *scope = lc_pouch_state_commit_group_current();
  int force_sync;
  int rc;
  if (scope == NULL || scope->pouch != pouch ||
      !lc_pouch_state_namespace_lock_is_held(pouch, ns))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "retention retirement requires namespace authority",
                        NULL, NULL, "pouch");
  force_sync = scope->force_sync;
  scope->force_sync = 1;
  /* The clear/delete can predate this mutation group, or a peer can have
   * appended it after cleanup's last barrier. Keep namespace authority from
   * the no-expiry observation through synchronization and hint removal. */
  rc = lc_pouch_state_durable_scope_include_namespace_locked(pouch, ns, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_durable_scope_barrier(scope, error);
  scope->force_sync = force_sync;
  if (rc == LC_OK)
    rc = lc_pouch_retention_hint_remove(pouch, ns, error);
  return rc;
}

static int lc_pouch_state_retention_inspect_locked(
    lc_pouch *pouch, const char *ns, lc_pouch_unix_seconds now, int select_due,
    lc_pouch_retention_candidate *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache = NULL;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_cache_record *first = NULL;
  lc_pouch_state_cache_record *next = NULL;
  char *cursor = NULL;
  int rc;

  memset(out, 0, sizeof(*out));
  if (!lc_pouch_state_namespace_lock_is_held(pouch, ns))
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "state retention selection requires namespace authority", NULL, NULL,
        "pouch");
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  if (rc != LC_OK)
    return rc;
  for (record = cache->records; record != NULL; record = record->next) {
    ++out->scanned;
    if (!record->found || !record->payload_span.present ||
        record->retention <= 0 || lc_pouch_state_key_is_staged(record->key) ||
        lc_pouch_state_record_type_is_object(record->record_type))
      continue;
    out->has_expiring = 1;
    if (!select_due || record->retention > now)
      continue;
    if (first == NULL || strcmp(record->key, first->key) < 0)
      first = record;
    if (cache->retention_cursor != NULL &&
        strcmp(record->key, cache->retention_cursor) > 0 &&
        (next == NULL || strcmp(record->key, next->key) < 0))
      next = record;
  }
  record = next != NULL ? next : first;
  if (record != NULL) {
    out->wrapped = next == NULL && cache->retention_cursor != NULL;
    out->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
    cursor = lc_strdup_with_allocator(&pouch->allocator, record->key);
    if (out->key == NULL || cursor == NULL) {
      lc_free_with_allocator(&pouch->allocator, out->key);
      lc_free_with_allocator(&pouch->allocator, cursor);
      out->key = NULL;
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to select state retention parent", NULL, NULL,
                        "pouch");
    } else {
      out->version = record->version;
      out->deadline = record->retention;
      lc_free_with_allocator(&pouch->allocator, cache->retention_cursor);
      cache->retention_cursor = cursor;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_retention_select_locked(lc_pouch *pouch, const char *ns,
                                           lc_pouch_unix_seconds now,
                                           lc_pouch_retention_candidate *out,
                                           lc_error *error) {
  return lc_pouch_state_retention_inspect_locked(pouch, ns, now, 1, out, error);
}

int lc_pouch_state_retention_has_expiring_locked(lc_pouch *pouch,
                                                 const char *ns, int *out,
                                                 lc_error *error) {
  lc_pouch_retention_candidate projection;
  int rc = lc_pouch_state_retention_inspect_locked(pouch, ns, 0, 0, &projection,
                                                   error);
  if (rc == LC_OK)
    *out = projection.has_expiring;
  return rc;
}

int lc_pouch_maintenance_run(lc_pouch *pouch,
                             const lc_pouch_maintenance_options *options,
                             lc_pouch_maintenance_result *out,
                             lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  const char *ns;
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_maintenance_result retention = {0};
  int owns_commit_group;
  int rc;

  ns = options != NULL ? options->ns : NULL;
  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_maintenance_run_locked(pouch, options, out, error);
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!options->cleanup_only) {
    rc = lc_pouch_retention_run(pouch, ns, LC_POUCH_RETENTION_ATTEMPT_LIMIT,
                                &retention, error);
    if (out != NULL)
      *out = retention;
    if (rc != LC_OK) {
      lc_pouch_writer_mode_operation_end(pouch);
      return rc;
    }
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, ns, &lock, error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_maintenance_run_locked(pouch, options, out, error);
  if (out != NULL) {
    out->retention_scanned_count += retention.retention_scanned_count;
    out->retention_expired_count += retention.retention_expired_count;
    out->retention_deleted_state_count +=
        retention.retention_deleted_state_count;
    out->retention_failed_count += retention.retention_failed_count;
  }
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  if (rc == LC_OK && options->terminal_reclaim &&
      (out == NULL || out->cleanup_pending_count == 0UL)) {
    lc_error marker_error;

    lc_error_init(&marker_error);
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_before_terminal_reclaim_marker_remove_hook != NULL) {
      (void)lc_pouch_test_before_terminal_reclaim_marker_remove_hook(
          lc_pouch_test_before_terminal_reclaim_marker_remove_context,
          &marker_error);
    }
#endif
    if (marker_error.code == LC_OK && lc_pouch_terminal_reclaim_marker_remove(
                                          pouch, ns, &marker_error) != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &marker_error);
      fields[2] = lc_log_code_field(&marker_error);
      lc_log_warn(pouch->logger, "compaction.terminal.marker.error", fields,
                  3U);
    }
    lc_error_cleanup(&marker_error);
  }
  if (rc != LC_OK) {
    lc_pouch_state_cache_invalidate_namespace(pouch, ns);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  lc_pouch_writer_mode_operation_end(pouch);
  if (rc == LC_OK)
    rc = lc_collection_maintenance_run(pouch, options, out, error);
  return rc;
}

void lc_pouch_maintenance_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_maintenance_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->ns);
  lc_free_with_allocator(allocator, result->diagnostic);
  memset(result, 0, sizeof(*result));
}

static int lc_pouch_state_cache_apply_write(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_namespace_manifest *manifest, const char *key,
    const char *content_type, const char *etag,
    const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, lc_pouch_generation version, uint64_t bytes,
    uint64_t cipher_bytes, const char *descriptor,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, int staged_delete_marker, int found,
    unsigned char record_type, lc_pouch_unix_seconds retention) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_entry entry;
  lc_error ignored;
  int rc;

  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache == NULL || !cache->initialized) {
    return LC_OK;
  }
  memset(&entry, 0, sizeof(entry));
  entry.key = (char *)key;
  entry.content_type = (char *)content_type;
  entry.etag = (char *)etag;
  if (payload_span != NULL) {
    entry.payload_span = *payload_span;
  }
  entry.payload_context = (char *)payload_context;
  entry.descriptor = (char *)descriptor;
  entry.metadata = (unsigned char *)metadata;
  entry.metadata_length = metadata_length;
  entry.index_seq = manifest->state_max_version;
  entry.version = version;
  entry.bytes = bytes;
  entry.cipher_bytes = cipher_bytes;
  entry.updated_at_unix = updated_at_unix;
  entry.retention = retention;
  entry.has_query_hidden = has_query_hidden;
  entry.query_hidden = query_hidden;
  entry.staged_delete_marker = staged_delete_marker;
  entry.seen = 1;
  entry.found = found;
  entry.record_type = record_type;
  lc_error_init(&ignored);
  rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, &ignored);
  lc_error_cleanup(&ignored);
  if (rc != LC_OK) {
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    cache->initialized = 0;
    cache->max_segment_id = 0UL;
    cache->segment_count = 0UL;
    cache->max_version = 0UL;
    cache->max_query_index_seq = 0UL;
    return LC_OK;
  }
  if (rc == LC_OK && manifest->max_segment_id > cache->max_segment_id) {
    cache->max_segment_id = manifest->max_segment_id;
  }
  if (rc == LC_OK) {
    cache->segment_count = manifest->segment_count;
  }
  return rc;
}

static int lc_pouch_state_append_tombstone(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    const char *key, const char *etag, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, unsigned char record_type,
    int query_index_from_index, lc_pouch_generation *index_seq_out,
    lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  lc_pouch_generation index_seq;
  int rc;
  unsigned char delete_record_type;

  if (index_seq_out != NULL) {
    *index_seq_out = 0UL;
  }
  meta = NULL;
  meta_len = 0U;
  index_seq = 0UL;
  delete_record_type = record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE ||
                               record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT
                           ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                           : LC_POUCH_STATE_RECORD_STATE_DELETE;
  rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, version,
                                         updated_at_unix, etag, &meta,
                                         &meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(&pouch->allocator, &meta,
                                                 &meta_len, 0UL, 0UL, error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_append_binary_record(
      pouch, ns, manifest, delete_record_type, key, strlen(key), meta, meta_len,
      0UL, query_index_from_index, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_index_seq(meta, meta_len, &index_seq, error);
  }
  if (rc == LC_OK) {
    /* An exclusive rotation materializes the on-disk manifest, whose
     * checkpoint can intentionally lag the resident sequence. The finalized
     * record trailer is the mutation's authoritative index sequence. */
    if (index_seq > manifest->state_max_version) {
      manifest->state_max_version = index_seq;
    }
    if (index_seq_out != NULL) {
      *index_seq_out = index_seq;
    }
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_append_record(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned char record_type, const char *key, const char *content_type,
    const char *etag, const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, lc_pouch_generation version, uint64_t bytes,
    uint64_t cipher_bytes, const char *descriptor,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, int staged_delete_marker, lc_pouch_unix_seconds retention,
    lc_pouch_generation query_index_seq, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  if (record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
      record_type != LC_POUCH_STATE_RECORD_OBJECT_PUT &&
      record_type != LC_POUCH_STATE_RECORD_STATE_LINK &&
      record_type != LC_POUCH_STATE_RECORD_STATE_META) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record type is invalid", NULL, NULL,
                        "pouch");
  }
  meta = NULL;
  meta_len = 0U;
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
      content_type, etag, descriptor, payload_span, payload_context, metadata,
      metadata_length, 0U, has_query_hidden, query_hidden, staged_delete_marker,
      retention, &meta, &meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &meta, &meta_len, 0UL, query_index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_binary_record(pouch, ns, manifest, record_type,
                                             key, strlen(key), meta, meta_len,
                                             query_index_seq, 0, error);
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_append_staged_commit_batch(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    const char *key, const char *staged_key, const lc_pouch_state_entry *staged,
    const unsigned char *metadata, size_t metadata_length,
    lc_pouch_generation version, lc_pouch_generation decision_version,
    lc_pouch_generation discard_version, lc_pouch_unix_seconds updated_at_unix,
    unsigned char committed_record_type, char **committed_descriptor_out,
    lc_pouch_generation *committed_index_seq_out, lc_error *error) {
  lc_pouch_state_binary_append_item items[3];
  unsigned char *link_meta;
  unsigned char *decision_meta;
  unsigned char *delete_meta;
  char *old_binding_context;
  char *new_binding_context;
  char *committed_descriptor;
  size_t link_meta_len;
  size_t decision_meta_len;
  size_t delete_meta_len;
  int object_record;
  int rc;

  if (staged == NULL || !staged->found || committed_descriptor_out == NULL ||
      committed_index_seq_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged commit batch requires staged state", NULL,
                        NULL, "pouch");
  }
  *committed_descriptor_out = NULL;
  *committed_index_seq_out = 0UL;
  if (committed_record_type != LC_POUCH_STATE_RECORD_STATE_LINK &&
      committed_record_type != LC_POUCH_STATE_RECORD_STATE_META) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged commit batch record type is invalid",
                        NULL, NULL, "pouch");
  }
  link_meta = NULL;
  decision_meta = NULL;
  delete_meta = NULL;
  old_binding_context = NULL;
  new_binding_context = NULL;
  committed_descriptor = NULL;
  link_meta_len = 0U;
  decision_meta_len = 0U;
  delete_meta_len = 0U;
  object_record = lc_pouch_state_record_type_is_object(staged->record_type);
  if (staged->descriptor != NULL) {
    old_binding_context = lc_pouch_state_crypto_context(
        &pouch->allocator, ns, staged_key, staged->version,
        lc_pouch_state_payload_class(staged->record_type, staged->object_link));
    new_binding_context = lc_pouch_state_crypto_context(
        &pouch->allocator, ns, key, version,
        lc_pouch_state_payload_class(staged->record_type, staged->object_link));
    if (old_binding_context == NULL || new_binding_context == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch committed binding context",
                        NULL, NULL, "pouch");
    } else {
      rc = lc_pouch_crypto_rebind_descriptor(
          pouch->crypto, staged->descriptor, old_binding_context,
          new_binding_context, &committed_descriptor, error);
    }
  } else {
    rc = LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, version, updated_at_unix, staged->bytes,
        staged->cipher_bytes, staged->content_type, staged->etag,
        committed_descriptor, &staged->payload_span, staged->payload_context,
        metadata, metadata_length, 0U, staged->has_query_hidden,
        staged->query_hidden, 0, staged->retention, &link_meta, &link_meta_len,
        error);
  }
  if (rc == LC_OK && object_record) {
    link_meta[60] |= LC_POUCH_STATE_PAYLOAD_FLAG_OBJECT_LINK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_decision_meta(
        &pouch->allocator, decision_version, staged->etag,
        LC_POUCH_STATE_DECISION_COMMITTED, &decision_meta, &decision_meta_len,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, discard_version, updated_at_unix, staged->etag,
        &delete_meta, &delete_meta_len, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &link_meta, &link_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &decision_meta, &decision_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &delete_meta, &delete_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    memset(items, 0, sizeof(items));
    items[0].record_type = committed_record_type;
    items[0].key = key;
    items[0].key_len = strlen(key);
    items[0].meta = link_meta;
    items[0].meta_len = link_meta_len;
    items[0].query_index_from_index = object_record ? 0 : 1;
    items[1].record_type = LC_POUCH_STATE_RECORD_DECISION;
    items[1].key = staged_key;
    items[1].key_len = strlen(staged_key);
    items[1].meta = decision_meta;
    items[1].meta_len = decision_meta_len;
    items[2].record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    items[2].key = staged_key;
    items[2].key_len = strlen(staged_key);
    items[2].meta = delete_meta;
    items[2].meta_len = delete_meta_len;
    rc = lc_pouch_state_append_binary_records(pouch, ns, manifest, items, 3U,
                                              error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_index_seq(link_meta, link_meta_len,
                                       committed_index_seq_out, error);
  }
  lc_free_with_allocator(&pouch->allocator, link_meta);
  lc_free_with_allocator(&pouch->allocator, decision_meta);
  lc_free_with_allocator(&pouch->allocator, delete_meta);
  lc_free_with_allocator(&pouch->allocator, old_binding_context);
  lc_free_with_allocator(&pouch->allocator, new_binding_context);
  if (rc == LC_OK) {
    *committed_descriptor_out = committed_descriptor;
  } else {
    lc_free_with_allocator(&pouch->allocator, committed_descriptor);
  }
  return rc;
}

static int
lc_pouch_state_is_staged_delete_marker(const lc_pouch_state_entry *entry) {
  return entry != NULL && entry->staged_delete_marker;
}

/* A transaction decision must retire its staged marker together with the
 * committed tombstone. For an already-absent document the decision only
 * retires the marker, preserving ordinary delete's no-op result. */
static int lc_pouch_state_append_staged_delete_commit_batch(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    const char *key, const char *staged_key, const char *committed_etag,
    lc_pouch_generation committed_version, int delete_committed,
    const char *staged_etag, lc_pouch_generation decision_version,
    lc_pouch_generation discard_version, lc_pouch_unix_seconds updated_at_unix,
    int object_record, lc_pouch_generation *committed_index_seq_out,
    lc_error *error) {
  lc_pouch_state_binary_append_item items[3];
  unsigned char *committed_delete_meta;
  unsigned char *decision_meta;
  unsigned char *staged_delete_meta;
  size_t committed_delete_meta_len;
  size_t decision_meta_len;
  size_t staged_delete_meta_len;
  size_t item_count;
  int rc;

  committed_delete_meta = NULL;
  decision_meta = NULL;
  staged_delete_meta = NULL;
  committed_delete_meta_len = 0U;
  decision_meta_len = 0U;
  staged_delete_meta_len = 0U;
  item_count = 0U;
  rc = LC_OK;
  if (committed_index_seq_out != NULL) {
    *committed_index_seq_out = 0UL;
  }
  if (delete_committed) {
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, committed_version, updated_at_unix, committed_etag,
        &committed_delete_meta, &committed_delete_meta_len, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_decision_meta(
        &pouch->allocator, decision_version, staged_etag,
        LC_POUCH_STATE_DECISION_COMMITTED, &decision_meta, &decision_meta_len,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, discard_version, updated_at_unix, staged_etag,
        &staged_delete_meta, &staged_delete_meta_len, error);
  }
  if (rc == LC_OK && delete_committed) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &committed_delete_meta, &committed_delete_meta_len,
        0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &decision_meta, &decision_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &staged_delete_meta, &staged_delete_meta_len, 0UL,
        0UL, error);
  }
  if (rc == LC_OK) {
    memset(items, 0, sizeof(items));
    if (delete_committed) {
      items[item_count].record_type = object_record
                                          ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                                          : LC_POUCH_STATE_RECORD_STATE_DELETE;
      items[item_count].key = key;
      items[item_count].key_len = strlen(key);
      items[item_count].meta = committed_delete_meta;
      items[item_count].meta_len = committed_delete_meta_len;
      items[item_count].query_index_from_index = object_record ? 0 : 1;
      ++item_count;
    }
    items[item_count].record_type = LC_POUCH_STATE_RECORD_DECISION;
    items[item_count].key = staged_key;
    items[item_count].key_len = strlen(staged_key);
    items[item_count].meta = decision_meta;
    items[item_count].meta_len = decision_meta_len;
    ++item_count;
    items[item_count].record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    items[item_count].key = staged_key;
    items[item_count].key_len = strlen(staged_key);
    items[item_count].meta = staged_delete_meta;
    items[item_count].meta_len = staged_delete_meta_len;
    ++item_count;
    rc = lc_pouch_state_append_binary_records(pouch, ns, manifest, items,
                                              item_count, error);
  }
  if (rc == LC_OK && delete_committed && committed_index_seq_out != NULL) {
    rc = lc_pouch_state_meta_index_seq(committed_delete_meta,
                                       committed_delete_meta_len,
                                       committed_index_seq_out, error);
  }
  lc_free_with_allocator(&pouch->allocator, committed_delete_meta);
  lc_free_with_allocator(&pouch->allocator, decision_meta);
  lc_free_with_allocator(&pouch->allocator, staged_delete_meta);
  return rc;
}

static int lc_pouch_state_commit_staged_delete_locked(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    const char *key, const char *staged_key,
    const lc_pouch_state_entry *committed, const lc_pouch_state_entry *staged,
    lc_pouch_state_write_result *out, int operation_active, lc_error *error) {
  char *etag;
  lc_pouch_generation version;
  lc_pouch_generation decision_version;
  lc_pouch_generation discard_version;
  lc_pouch_generation committed_index_seq;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_namespace_manifest cache_manifest;
  int delete_committed;
  int object_record;
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL || key == NULL ||
      staged_key == NULL || committed == NULL || staged == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged delete commit requires state context",
                        NULL, NULL, "pouch");
  }
  etag = NULL;
  committed_index_seq = 0UL;
  memset(&cache_manifest, 0, sizeof(cache_manifest));
  delete_committed = committed->found && committed->payload_span.present;
  object_record = lc_pouch_state_record_type_is_object(staged->record_type);
  version = delete_committed ? committed->version + 1UL : 0UL;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  decision_version = staged->version + 1UL;
  discard_version = decision_version + 1UL;
  if (delete_committed) {
    etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
    if (etag == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  rc = lc_pouch_state_append_staged_delete_commit_batch(
      pouch, ns, manifest, key, staged_key, etag, version, delete_committed,
      staged->etag, decision_version, discard_version, updated_at_unix,
      object_record, &committed_index_seq, error);
  if (rc == LC_OK && delete_committed) {
    cache_manifest = *manifest;
    cache_manifest.state_max_version = committed_index_seq;
    (void)lc_pouch_state_cache_apply_write(
        pouch, ns, &cache_manifest, key, NULL, etag, NULL, NULL, NULL, 0U,
        version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
        object_record ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                      : LC_POUCH_STATE_RECORD_STATE_DELETE,
        0);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, ns, manifest, staged_key, NULL, staged->etag, NULL, NULL, NULL,
        0U, discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
        LC_POUCH_STATE_RECORD_STATE_DELETE, 0);
  }
  if (rc == LC_OK && delete_committed) {
    out->etag = etag;
    etag = NULL;
    out->index_seq = committed_index_seq;
    out->version = version;
    out->updated_at_unix = updated_at_unix;
    if (pouch->query_indexing_enabled && !object_record) {
      lc_pouch_query_index_note_state_delete(pouch, ns, key, out,
                                             operation_active);
    }
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  return rc;
}

static int lc_pouch_state_append_staged_discard_batch(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    const char *staged_key, const char *etag,
    lc_pouch_generation decision_version, lc_pouch_generation tombstone_version,
    lc_pouch_unix_seconds updated_at_unix, lc_error *error) {
  lc_pouch_state_binary_append_item items[2];
  unsigned char *decision_meta;
  unsigned char *delete_meta;
  size_t decision_meta_len;
  size_t delete_meta_len;
  int rc;

  decision_meta = NULL;
  delete_meta = NULL;
  decision_meta_len = 0U;
  delete_meta_len = 0U;
  rc = lc_pouch_state_encode_decision_meta(
      &pouch->allocator, decision_version, etag,
      LC_POUCH_STATE_DECISION_DISCARDED, &decision_meta, &decision_meta_len,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, tombstone_version,
                                           updated_at_unix, etag, &delete_meta,
                                           &delete_meta_len, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &decision_meta, &decision_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &delete_meta, &delete_meta_len, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    memset(items, 0, sizeof(items));
    items[0].record_type = LC_POUCH_STATE_RECORD_DECISION;
    items[0].key = staged_key;
    items[0].key_len = strlen(staged_key);
    items[0].meta = decision_meta;
    items[0].meta_len = decision_meta_len;
    items[1].record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    items[1].key = staged_key;
    items[1].key_len = strlen(staged_key);
    items[1].meta = delete_meta;
    items[1].meta_len = delete_meta_len;
    rc = lc_pouch_state_append_binary_records(pouch, ns, manifest, items, 2U,
                                              error);
  }
  lc_free_with_allocator(&pouch->allocator, decision_meta);
  lc_free_with_allocator(&pouch->allocator, delete_meta);
  return rc;
}

static char *lc_pouch_state_staged_key(const lc_allocator *allocator,
                                       const char *key, const char *txn_id) {
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;
  char *staged;

  if (key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0') {
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  suffix_len = sizeof("/.staging/") - 1U;
  staged = (char *)lc_alloc_with_allocator(allocator,
                                           key_len + suffix_len + txn_len + 1U);
  if (staged == NULL) {
    return NULL;
  }
  memcpy(staged, key, key_len);
  memcpy(staged + key_len, "/.staging/", suffix_len);
  memcpy(staged + key_len + suffix_len, txn_id, txn_len);
  staged[key_len + suffix_len + txn_len] = '\0';
  return staged;
}

static int lc_pouch_state_recover_staged_decisions_locked(lc_pouch *pouch,
                                                          const char *ns,
                                                          lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_decision *decisions;
  lc_pouch_state_decision *decision;
  unsigned long recovered_count;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_recover_staged_decisions requires "
                        "pouch and namespace",
                        NULL, NULL, NULL);
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (cache->decision_recovery_checked) {
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, NULL, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  decisions = NULL;
  recovered_count = 0UL;
  rc = lc_pouch_state_collect_decisions(pouch, &manifest, &decisions, error);
  for (decision = decisions; rc == LC_OK && decision != NULL;
       decision = decision->next) {
    lc_pouch_state_entry staged;
    lc_pouch_generation max_version;
    lc_pouch_generation tombstone_version;
    lc_pouch_unix_seconds updated_at_unix;

    memset(&staged, 0, sizeof(staged));
    /* The namespace projection already resolves each key's latest record.
     * Scanning the entire durable history for every old decision makes one
     * surviving staged key turn recovery into quadratic log reads. Use the
     * normal mode-aware lookup so shared writers and invalidated projections
     * still refresh under the mutation authority held by this caller. */
    rc = lc_pouch_state_cache_lookup(pouch, ns, &manifest, decision->staged_key,
                                     &staged, &max_version, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      break;
    }
    if (!staged.found) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      continue;
    }
    (void)max_version;
    tombstone_version = staged.version + 1UL;
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    rc = lc_pouch_state_append_tombstone(
        pouch, ns, &manifest, decision->staged_key, decision->etag,
        tombstone_version, updated_at_unix, LC_POUCH_STATE_RECORD_STATE_DELETE,
        0, NULL, error);
    if (rc == LC_OK) {
      (void)lc_pouch_state_cache_apply_write(
          pouch, ns, &manifest, decision->staged_key, NULL, decision->etag,
          NULL, NULL, NULL, 0U, tombstone_version, 0UL, 0UL, NULL,
          updated_at_unix, 0, 0, 0, 0, LC_POUCH_STATE_RECORD_STATE_DELETE, 0);
      recovered_count++;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  }
  if (rc == LC_OK) {
    cache->decision_recovery_checked = 1;
    if (recovered_count != 0UL) {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_u64_field("count", recovered_count);
      lc_log_debug(pouch->logger, "recovery.staged.complete", fields, 2U);
    }
  }
  lc_pouch_state_decisions_cleanup(&pouch->allocator, decisions);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch, const char *ns,
                                            lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  lc_pouch_namespace_logstore *cache;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_recover_staged_decisions_locked(pouch, ns, error);
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache != NULL && cache->decision_recovery_checked) {
    return LC_OK;
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, ns, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_recover_staged_decisions_locked(pouch, ns, error);
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

static int lc_pouch_state_write_resolved_locked(
    lc_pouch *pouch, const char *ns, const char *key, lc_source *body,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_entry *prepared_current,
    lc_pouch_namespace_manifest *prepared_manifest,
    int prepared_manifest_from_cache, lc_pouch_state_write_result *out,
    lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_hash_source hash_source;
  const char *content_type;
  EVP_MD_CTX *hash_ctx;
  char *segment_path;
  lc_pouch_state_append_lock append_lock;
  char *descriptor;
  char *crypto_context;
  char *payload_context;
  lc_pouch_state_payload_span payload_span;
  char *etag;
  unsigned char *meta;
  unsigned char *final_meta;
  const unsigned char *materialized_plain;
  unsigned char *materialized_stored;
  const char placeholder_etag[] =
      "0000000000000000000000000000000000000000000000000000000000000000";
  int fd;
  lc_pouch_generation max_version;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  unsigned long stored_crc;
  uint64_t segment_size;
  uint64_t payload_offset;
  uint64_t record_end;
  size_t meta_len;
  size_t final_meta_len;
  size_t materialized_plain_len;
  size_t materialized_stored_len;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  const unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  lc_pouch_generation query_index_seq;
  int rc;
  int manifest_from_cache;
  int retain_active_append_fd;
  int single_writer;
  int use_materialized_record;
  int materialized_transform_ready;
  int namespace_locked;
  int streaming_projection_mutex_released;
  int projection_rc;
  int use_prepared_current;
  int query_index_guard_started;
  int use_body_batcher;
  uint64_t writer_mode_epoch;
  unsigned char put_record_type;
  lc_pouch_state_precondition_view precondition_view;
  const unsigned char *body_batch_plain;
  size_t body_batch_plain_len;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_write requires pouch, namespace, key, "
                        "body and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  index_seq = 0UL;
  query_index_seq = 0UL;
  memset(&manifest, 0, sizeof(manifest));
  memset(&current, 0, sizeof(current));
  use_prepared_current = prepared_current != NULL && prepared_manifest != NULL;
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  namespace_locked = lc_pouch_state_namespace_lock_is_held(pouch, ns);
  body_batch_plain = NULL;
  body_batch_plain_len = 0U;
  query_index_guard_started = 0;
  /* Shared writes must retain the root durable authority until publication;
   * their historical append worker is therefore disabled. */
  use_body_batcher = 0;
  if (use_body_batcher && !namespace_locked &&
      !lc_pouch_state_body_append_worker_active() &&
      lc_source_memory_view(body, &body_batch_plain, &body_batch_plain_len) &&
      body_batch_plain_len <= LC_POUCH_CRYPTO_MEMORY_TRANSFORM_MAX_BYTES) {
    return lc_pouch_state_body_append_schedule_locked(pouch, ns, key, body,
                                                      options, out, error);
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (use_prepared_current) {
    current = *prepared_current;
    memset(prepared_current, 0, sizeof(*prepared_current));
    manifest = *prepared_manifest;
    memset(prepared_manifest, 0, sizeof(*prepared_manifest));
    manifest_from_cache = prepared_manifest_from_cache;
    max_version = 0UL;
    rc = LC_OK;
  } else {
    manifest_from_cache =
        single_writer && cache != NULL && cache->initialized &&
        cache->writer_mode_epoch == writer_mode_epoch &&
        cache->namespace_path != NULL && cache->active_segment_leaf != NULL;
    if (manifest_from_cache) {
      rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, &manifest, error);
      if (rc == LC_OK) {
        max_version = cache->max_version;
        rc = lc_pouch_state_entry_from_cache_record(
            pouch, lc_pouch_state_cache_record_find(cache, key), &current,
            error);
      }
    } else {
      rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path, ns,
                                     error);
      if (rc == LC_OK) {
        rc = lc_pouch_namespace_manifest_open(&pouch->allocator,
                                              pouch->root_path, ns, &manifest,
                                              NULL, NULL, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_state_cache_lookup(pouch, ns, &manifest, key, &current,
                                         &max_version, error);
      }
      cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
      single_writer =
          lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
    }
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  retain_active_append_fd = 0;
  if (options != NULL && options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(&current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state etag precondition failed", NULL, NULL,
                          NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (options->expected_version == 0UL) {
      if (current.found && current.payload_span.present) {
        lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
        lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch state version precondition failed", NULL,
                            NULL, NULL);
      }
    } else if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state version precondition failed", NULL, NULL,
                          NULL);
    }
  }
  if (options != NULL && options->create_if_absent && current.found &&
      current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state create-if-absent precondition failed",
                        NULL, NULL, NULL);
  }
  if (options != NULL && options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  (void)max_version;
  if (options != NULL && options->has_logical_version) {
    version = options->logical_version;
  } else {
    version = current.found && current.payload_span.present
                  ? current.version + 1UL
                  : 1UL;
  }
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  etag = NULL;
  retention = options != NULL && options->has_retention ? options->retention
                                                        : current.retention;
  if (retention < 0) {
    lc_pouch_unix_seconds checked;
    rc = lc_pouch_state_retention_prepare(key, retention, &checked, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  descriptor = NULL;
  segment_path = NULL;
  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  payload_context = NULL;
  memset(&payload_span, 0, sizeof(payload_span));
  meta = NULL;
  final_meta = NULL;
  materialized_plain = NULL;
  materialized_stored = NULL;
  materialized_plain_len = 0U;
  materialized_stored_len = 0U;
  record_end = 0UL;
  use_materialized_record =
      lc_source_memory_view(body, &materialized_plain,
                            &materialized_plain_len) &&
      materialized_plain_len <= LC_POUCH_CRYPTO_MEMORY_TRANSFORM_MAX_BYTES;
  materialized_transform_ready = 0;
  streaming_projection_mutex_released = 0;
  content_type = options != NULL && options->content_type != NULL
                     ? options->content_type
                     : "application/octet-stream";
  staged_delete_marker =
      options != NULL && options->staged_delete_marker ? 1 : 0;
  put_record_type = options != NULL && options->object_record
                        ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                        : LC_POUCH_STATE_RECORD_STATE_PUT;
  index_seq = 0UL;
  has_query_hidden = current.has_query_hidden;
  query_hidden = current.query_hidden;
  if (options != NULL && options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  } else if (current.record_type == LC_POUCH_STATE_RECORD_STATE_META &&
             !current.payload_span.present) {
    /* A first lease claim is metadata-only and must not hide its first body. */
    has_query_hidden = 0;
    query_hidden = 0;
  }
  metadata = current.metadata;
  metadata_length = current.metadata_length;
  if (options != NULL && options->has_metadata) {
    metadata = options->metadata;
    metadata_length = options->metadata_length;
  }
  if (options != NULL &&
      (options->suppress_query_index || options->object_record)) {
    query_index_seq = current.query_index_seq;
  }
  payload_context = lc_pouch_state_crypto_context(
      &pouch->allocator, ns, key, version,
      lc_pouch_state_payload_class(put_record_type, 0));
  if (payload_context == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload context", NULL,
                        NULL, NULL);
  }
  /* A bounded SDK memory source has already been materialized by its caller.
   * Its compression/encryption and etag work neither observes nor mutates the
   * resident projection, so exclusive exact-key mutations perform it before
   * claiming the physical appender. Streaming sources deliberately stay out of
   * this path and retain direct source-to-segment flow below. */
  if (use_materialized_record && single_writer && !namespace_locked) {
    bytes = (uint64_t)materialized_plain_len;
    cipher_bytes = 0UL;
    stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
    lc_pouch_state_projection_mutex_unlock(pouch, ns);
    rc = lc_pouch_crypto_transform_memory(
        &pouch->allocator, pouch->crypto, payload_context, materialized_plain,
        materialized_plain_len,
        options == NULL || !options->disable_compression, &materialized_stored,
        &materialized_stored_len, &stored_crc, &descriptor, error);
    if (rc == LC_OK) {
      cipher_bytes = (uint64_t)materialized_stored_len;
      etag = lc_pouch_state_hash_bytes(&pouch->allocator, materialized_plain,
                                       materialized_plain_len, error);
      if (etag == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    projection_rc = lc_pouch_state_projection_mutex_lock(pouch, ns, error);
    if (projection_rc != LC_OK && rc == LC_OK) {
      rc = projection_rc;
    }
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_free_with_allocator(&pouch->allocator, descriptor);
      lc_free_with_allocator(&pouch->allocator, materialized_stored);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    materialized_transform_ready = 1;
  }
  rc = lc_pouch_state_append_lock_enter_after_mutation(pouch, ns, &append_lock,
                                                       error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_free_with_allocator(&pouch->allocator, descriptor);
    lc_free_with_allocator(&pouch->allocator, materialized_stored);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  /* Waiting behind an append may have advanced or rotated the active segment.
   * The exact-key precondition remains protected by its key lock, but the
   * physical manifest captured before the append authority is no longer safe to
   * use. Exclusive Pouch can borrow its resident manifest; shared Pouch must
   * reopen the durable manifest after acquiring root mutation authority
   * so it never tails or appends at a stale segment offset. */
  if (single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (cache != NULL && cache->initialized &&
        cache->writer_mode_epoch == writer_mode_epoch &&
        cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      memset(&manifest, 0, sizeof(manifest));
      rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, &manifest, error);
      manifest_from_cache = rc == LC_OK ? 1 : 0;
    } else {
      manifest_from_cache = 0;
    }
  } else {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    segment_path = NULL;
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    memset(&manifest, 0, sizeof(manifest));
    manifest_from_cache = 0;
  }
  if (rc == LC_OK && !manifest_from_cache) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    memset(&manifest, 0, sizeof(manifest));
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          ns, &manifest, NULL, NULL, error);
  }
  if (rc == LC_OK && !manifest_from_cache) {
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                  "segments", manifest.active_segment);
    if (segment_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK &&
      !(single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0)) {
    rc = lc_pouch_state_repair_active_tail_locked(
        pouch, ns, &manifest, segment_path, manifest.active_segment, error);
  }
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
      segment_size = cache->active_segment_offset;
    } else {
      rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
    }
  }
  if (rc == LC_OK &&
      (segment_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
       (uint64_t)strlen(key) >
           LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - segment_size)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch state record offset exceeds u64", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && segment_size > 0U &&
      segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES +
              (uint64_t)strlen(key) >
          pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    segment_path = NULL;
    rc = lc_pouch_state_manifest_materialize(pouch, ns, &manifest,
                                             &manifest_from_cache, error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, ns, &manifest,
                                              manifest.active_segment_id + 1U,
                                              error);
    }
    if (rc == LC_OK) {
      segment_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                    "segments", manifest.active_segment);
      if (segment_path == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK && cache != NULL && cache->initialized) {
      rc = lc_pouch_state_cache_set_active_segment(pouch, cache, &manifest, 0U,
                                                   error);
    }
    segment_size = 0U;
  }
  if (rc != LC_OK) {
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  fd = -1;
  if (single_writer && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
    rc = lc_pouch_state_cache_active_append_fd(pouch, cache, &manifest, &fd,
                                               error);
    retain_active_append_fd = rc == LC_OK ? 1 : 0;
  } else {
    fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  }
  if (rc == LC_OK && fd < 0) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (rc == LC_OK && retain_active_append_fd) {
    rc = lc_pouch_state_fd_seek(
        fd, segment_size, "failed to seek pouch state segment append offset",
        error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_state_fd_end(
        fd, &segment_size, "failed to seek pouch state segment append offset",
        error);
  }
  if (rc != LC_OK) {
    if (!retain_active_append_fd) {
      (void)close(fd);
    }
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  rc = lc_pouch_state_payload_span_set(&pouch->allocator, &payload_span,
                                       manifest.active_segment, segment_size,
                                       0UL, 0UL, 0UL, error);
  if (rc != LC_OK) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, 0UL, 0UL, content_type,
      placeholder_etag, NULL, &payload_span, payload_context, metadata,
      metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
      has_query_hidden, query_hidden, staged_delete_marker, retention, &meta,
      &meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_meta_set_index_sequences(
        &pouch->allocator, &meta, &meta_len, 0UL, query_index_seq, error);
  }
  if (rc != LC_OK) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (segment_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
      (uint64_t)strlen(key) >
          LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - segment_size ||
      (uint64_t)meta_len > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                               segment_size - (uint64_t)strlen(key)) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record offset exceeds u64", NULL, NULL,
                        "pouch");
  }
  payload_offset = segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (uint64_t)strlen(key) + (uint64_t)meta_len;
  /* A bounded in-memory source is already materialized by its caller. Both
   * writer modes hold append authority here, so they can publish one complete
   * record without the pending streaming prefix/finalization protocol. Other
   * sources retain the real streaming path below. */
  if (use_materialized_record) {
    if (!materialized_transform_ready) {
      bytes = (uint64_t)materialized_plain_len;
      cipher_bytes = 0UL;
      stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
      rc = lc_pouch_crypto_transform_memory(
          &pouch->allocator, pouch->crypto, payload_context, materialized_plain,
          materialized_plain_len,
          options == NULL || !options->disable_compression,
          &materialized_stored, &materialized_stored_len, &stored_crc,
          &descriptor, error);
      if (rc == LC_OK) {
        cipher_bytes = (uint64_t)materialized_stored_len;
        etag = lc_pouch_state_hash_bytes(&pouch->allocator, materialized_plain,
                                         materialized_plain_len, error);
        if (etag == NULL) {
          rc = error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
        }
      }
    }
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    if (rc == LC_OK) {
      rc = lc_pouch_state_retention_publish(pouch, ns, key, retention,
                                            &retention, error);
    }
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &payload_span, manifest.active_segment,
          segment_size, payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
          content_type, etag, descriptor, &payload_span, payload_context,
          metadata, metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
          has_query_hidden, query_hidden, staged_delete_marker, retention,
          &final_meta, &final_meta_len, error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_sequences(
            &pouch->allocator, &final_meta, &final_meta_len, 0UL,
            query_index_seq, error);
      }
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state metadata reservation changed size", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_reserve_index_records(
          pouch, ns, &manifest, 1UL,
          !(options != NULL &&
            (options->suppress_query_index || options->object_record)),
          &index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_sequences(
          &pouch->allocator, &final_meta, &final_meta_len, index_seq,
          options != NULL &&
                  (options->suppress_query_index || options->object_record)
              ? query_index_seq
              : index_seq,
          error);
    }
    if (rc == LC_OK && cipher_bytes > LC_U64_MAX - payload_offset) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record offset exceeds u64", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK) {
      record_end = payload_offset + cipher_bytes;
      rc = lc_pouch_state_record_write_complete(
          fd, put_record_type, key, strlen(key), final_meta, final_meta_len,
          materialized_stored, materialized_stored_len, stored_crc, error);
    }
    if (rc == LC_OK) {
      lc_source_memory_consume(body);
      payload_offset = record_end;
      rc = lc_pouch_state_defer_fsync(pouch, fd, error);
    }
  } else {
    rc = lc_pouch_state_record_write_prefix(
        fd, put_record_type, key, strlen(key), meta, meta_len, 0U, 0UL,
        LC_POUCH_RECORD_FLAG_PENDING, error);
    if (rc != LC_OK) {
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }

    bytes = 0UL;
    cipher_bytes = 0UL;
    stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
    descriptor = NULL;
    hash_ctx = EVP_MD_CTX_new();
    if (hash_ctx == NULL) {
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state payload hash context",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_hash_source_init(&hash_source, body, hash_ctx, error);
    if (rc != LC_OK) {
      EVP_MD_CTX_free(hash_ctx);
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    crypto_context =
        lc_strdup_with_allocator(&pouch->allocator, payload_context);
    if (crypto_context == NULL) {
      EVP_MD_CTX_free(hash_ctx);
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state crypto context", NULL,
                          NULL, NULL);
    }
    /* The append gate owns this byte range. Let independent keys capture
     * metadata and prepare values while this source flows directly into the
     * segment; publication reacquires the projection mutex below. */
    if (!namespace_locked) {
      lc_pouch_state_projection_mutex_unlock(pouch, ns);
      streaming_projection_mutex_released = 1;
    }
    rc = lc_pouch_crypto_stream_to_fd_crc_with_compression(
        pouch->crypto, crypto_context, fd, &hash_source.pub, &bytes,
        &cipher_bytes, &stored_crc, &descriptor,
        options == NULL || !options->disable_compression, error);
    lc_free_with_allocator(&pouch->allocator, crypto_context);
    if (rc == LC_OK && hash_source.failed) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to hash pouch state payload", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK) {
      etag = lc_pouch_state_hash_final(&pouch->allocator, hash_ctx, error);
      if (etag == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    EVP_MD_CTX_free(hash_ctx);
    if (streaming_projection_mutex_released) {
      projection_rc = lc_pouch_state_projection_mutex_lock(pouch, ns, error);
      streaming_projection_mutex_released = 0;
      if (projection_rc != LC_OK && rc == LC_OK) {
        rc = projection_rc;
      }
    }
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    if (rc == LC_OK) {
      rc = lc_pouch_state_retention_publish(pouch, ns, key, retention,
                                            &retention, error);
    }
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &payload_span, manifest.active_segment,
          segment_size, payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
          content_type, etag, descriptor, &payload_span, payload_context,
          metadata, metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
          has_query_hidden, query_hidden, staged_delete_marker, retention,
          &final_meta, &final_meta_len, error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_sequences(
            &pouch->allocator, &final_meta, &final_meta_len, 0UL,
            query_index_seq, error);
      }
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state metadata reservation changed size", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_reserve_index_records(
          pouch, ns, &manifest, 1UL,
          !(options != NULL &&
            (options->suppress_query_index || options->object_record)),
          &index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_sequences(
          &pouch->allocator, &final_meta, &final_meta_len, index_seq,
          options != NULL &&
                  (options->suppress_query_index || options->object_record)
              ? query_index_seq
              : index_seq,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_finalize(
          fd, segment_size, put_record_type, key, strlen(key), final_meta,
          final_meta_len, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_fd_end(
          fd, &payload_offset,
          "failed to restore pouch state segment append offset", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_defer_fsync(pouch, fd, error);
    }
  }
  if (rc == LC_OK && options != NULL && options->query_index_operation_active &&
      !options->object_record) {
    lc_pouch_unix_seconds expires_at_unix;

    expires_at_unix = options->query_index_operation_expires_at_unix != NULL
                          ? *options->query_index_operation_expires_at_unix
                          : 0L;
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, options->query_index_operation_id, expires_at_unix,
        &query_index_guard_started, error);
  }
  if (!retain_active_append_fd && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (!retain_active_append_fd) {
    fd = -1;
  }
  if (rc != LC_OK) {
    if (retain_active_append_fd) {
      lc_pouch_state_append_fd_rollback(fd, segment_size, 1);
    } else {
      lc_pouch_state_truncate_path_best_effort(segment_path, segment_size);
    }
  }
  lc_pouch_state_append_lock_release(&append_lock);
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
      cache->active_segment_offset = payload_offset;
    }
    (void)lc_pouch_state_cache_apply_write(
        pouch, ns, &manifest, key, content_type, etag, &payload_span,
        payload_context, metadata, metadata_length, version, bytes,
        cipher_bytes, descriptor, updated_at_unix, has_query_hidden,
        query_hidden, staged_delete_marker, 1, put_record_type, retention);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->index_seq = index_seq;
    out->version = version;
    out->bytes = bytes;
    out->cipher_bytes = cipher_bytes;
    out->descriptor = descriptor;
    if (metadata_length > 0U) {
      out->metadata = (unsigned char *)lc_alloc_with_allocator(
          &pouch->allocator, metadata_length);
      if (out->metadata == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state result metadata",
                          NULL, NULL, NULL);
      } else {
        memcpy(out->metadata, metadata, metadata_length);
        out->metadata_length = metadata_length;
      }
    }
    out->updated_at_unix = updated_at_unix;
    out->retention = retention;
    out->has_body = options == NULL || !options->staged_delete_marker;
    out->has_query_hidden = has_query_hidden;
    out->query_hidden = query_hidden;
    out->query_index_operation_guard_started = query_index_guard_started;
    etag = NULL;
    descriptor = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  lc_free_with_allocator(&pouch->allocator, materialized_stored);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
  lc_free_with_allocator(&pouch->allocator, payload_context);
  lc_free_with_allocator(&pouch->allocator, meta);
  lc_free_with_allocator(&pouch->allocator, final_meta);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (rc != LC_OK && query_index_guard_started) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
  }
  return rc;
}

int lc_pouch_state_write_locked(lc_pouch *pouch, const char *ns,
                                const char *key, lc_source *body,
                                const lc_pouch_state_write_options *options,
                                lc_pouch_state_write_result *out,
                                lc_error *error) {
  return lc_pouch_state_write_resolved_locked(pouch, ns, key, body, options,
                                              NULL, NULL, 0, out, error);
}

typedef struct lc_pouch_state_prepared_write_context {
  lc_pouch *pouch;
  const char *ns;
  const char *key;
  const lc_pouch_state_write_options *options;
  lc_pouch_state_body_prepare_fn prepare;
  void *prepare_context;
  lc_pouch_state_write_result *out;
  lc_source *body;
} lc_pouch_state_prepared_write_context;

/* Prepare the transformed body after the current projection is stable under
 * exact-key authority, then let the normal locked writer finalize it. */
static int lc_pouch_state_write_prepared_locked(void *context,
                                                lc_error *error) {
  lc_pouch_state_prepared_write_context *prepared;
  lc_pouch_state_read_result current;
  int rc;

  prepared = (lc_pouch_state_prepared_write_context *)context;
  if (prepared == NULL || prepared->pouch == NULL || prepared->ns == NULL ||
      prepared->key == NULL || prepared->options == NULL ||
      prepared->prepare == NULL || prepared->out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared write requires complete context", NULL,
                        NULL, "pouch");
  }
  memset(&current, 0, sizeof(current));
  prepared->body = NULL;
  rc = lc_pouch_state_read_locked(prepared->pouch, prepared->ns, prepared->key,
                                  &current, error);
  if (rc == LC_OK) {
    rc = prepared->prepare(&current, prepared->prepare_context, &prepared->body,
                           error);
  }
  if (rc == LC_OK && prepared->body == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch prepared write callback returned no body", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_write_locked(prepared->pouch, prepared->ns,
                                     prepared->key, prepared->body,
                                     prepared->options, prepared->out, error);
  }
  lc_pouch_state_read_result_cleanup(&prepared->pouch->allocator, &current);
  return rc;
}

static void lc_pouch_state_log_write_result(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_options *options, lc_source *body,
    const lc_pouch_state_write_result *out, int rc, lc_error *error) {
  if (rc == LC_OK) {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] =
        lc_log_u64_field("generation", out != NULL ? out->version : 0UL);
    fields[3] =
        lc_log_u64_field("payload_bytes", out != NULL ? out->bytes : 0UL);
    fields[4] =
        lc_log_u64_field("stored_bytes", out != NULL ? out->cipher_bytes : 0UL);
    fields[5] = lc_log_str_field("content_type",
                                 options != NULL ? options->content_type
                                                 : "application/octet-stream");
    fields[6] = lc_log_bool_field("query_hidden",
                                  options != NULL && options->has_query_hidden
                                      ? options->query_hidden
                                      : 0);
    lc_log_trace(pouch->logger, "logstore.write", fields, 7U);
    if (pouch->query_indexing_enabled &&
        (options == NULL || !options->object_record) &&
        (options == NULL || !options->suppress_query_index)) {
      lc_pouch_query_index_note_state_write(
          pouch, ns, key,
          options != NULL && options->content_type != NULL
              ? options->content_type
              : "application/octet-stream",
          body, out, options != NULL && options->query_index_operation_active);
    }
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.write.error", fields, 4U);
  }
}

int lc_pouch_state_write_prepared(lc_pouch *pouch, const char *ns,
                                  const char *key,
                                  const lc_pouch_state_write_options *options,
                                  lc_pouch_state_body_prepare_fn prepare,
                                  void *prepare_context,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error) {
  lc_pouch_state_prepared_write_context context;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || options == NULL || prepare == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared write requires pouch, namespace, key, "
                        "options, callback and output",
                        NULL, NULL, "pouch");
  }
  memset(&context, 0, sizeof(context));
  context.pouch = pouch;
  context.ns = ns;
  context.key = key;
  context.options = options;
  context.prepare = prepare;
  context.prepare_context = prepare_context;
  context.out = out;
  rc = lc_pouch_state_with_key_lock(
      pouch, ns, key, lc_pouch_state_write_prepared_locked, &context, error);
  if (rc != LC_OK && out->query_index_operation_guard_started &&
      options->query_index_operation_id != NULL) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
    out->query_index_operation_guard_started = 0;
  }
  lc_pouch_state_log_write_result(pouch, ns, key, options, context.body, out,
                                  rc, error);
  if (context.body != NULL) {
    lc_source_close(context.body);
  }
  return rc;
}

int lc_pouch_state_write(lc_pouch *pouch, const char *ns, const char *key,
                         lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_write_locked(pouch, ns, key, body, options, out,
                                       error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_write_locked(pouch, ns, key, body, options, out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc != LC_OK && out->query_index_operation_guard_started &&
      options != NULL && options->query_index_operation_id != NULL) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
    out->query_index_operation_guard_started = 0;
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 0);
  }
  lc_pouch_state_log_write_result(pouch, ns, key, options, body, out, rc,
                                  error);
  return rc;
}

void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->etag);
  lc_free_with_allocator(allocator, result->descriptor);
  lc_free_with_allocator(allocator, result->metadata);
  memset(result, 0, sizeof(*result));
}

static int lc_pouch_state_metadata_write_result_build(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_entry *current,
    const lc_pouch_state_write_options *options, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, lc_pouch_state_write_result *out, lc_error *error) {
  const unsigned char *metadata;
  size_t metadata_length;

  out->etag = lc_strdup_with_allocator(
      &pouch->allocator, current->etag != NULL ? current->etag : "");
  if (out->etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata state etag", NULL,
                        NULL, NULL);
  }
  out->index_seq = manifest->state_max_version;
  out->version = version;
  out->bytes = current->bytes;
  out->cipher_bytes = current->cipher_bytes;
  if (current->descriptor != NULL) {
    out->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, current->descriptor);
    if (out->descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch metadata descriptor", NULL,
                          NULL, NULL);
    }
  }
  metadata = options->has_metadata ? options->metadata : current->metadata;
  metadata_length = options->has_metadata ? options->metadata_length
                                          : current->metadata_length;
  if (metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                             metadata_length);
    if (out->metadata == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch metadata result blob", NULL,
                          NULL, NULL);
    }
    memcpy(out->metadata, metadata, metadata_length);
    out->metadata_length = metadata_length;
  }
  out->updated_at_unix = updated_at_unix;
  out->retention =
      options->has_retention ? options->retention : current->retention;
  out->has_body =
      current->payload_span.present && !current->staged_delete_marker;
  out->has_query_hidden = has_query_hidden;
  out->query_hidden = query_hidden;
  return LC_OK;
}

static void lc_pouch_state_metadata_append_complete(
    lc_pouch_state_metadata_append_batcher *batcher,
    lc_pouch_state_metadata_append_request **requests, size_t count) {
  size_t index;

  if (batcher == NULL || requests == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  for (index = 0U; index < count; ++index) {
    if (requests[index] != NULL) {
      requests[index]->done = 1;
      pthread_cond_signal(&requests[index]->cond);
    }
  }
  pthread_mutex_unlock(&batcher->mutex);
}

static void lc_pouch_state_metadata_append_fail(
    lc_pouch_state_metadata_append_request *request, int rc,
    const lc_error *cause) {
  const char *message;

  if (request == NULL) {
    return;
  }
  message = cause != NULL && cause->message != NULL
                ? cause->message
                : "pouch metadata append batch failed";
  request->rc =
      lc_error_set(request->error, rc, 0L, message, NULL, NULL, "pouch");
}

static void lc_pouch_state_metadata_append_process(
    lc_pouch *pouch, lc_pouch_state_metadata_append_request **requests,
    size_t count, int projection_held) {
  lc_pouch_state_binary_append_item
      items[LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX];
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_append_lock append_lock;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_process_namespace_mutex *projection_mutex;
  lc_pouch_state_shared_mutation_guard *shared_mutation;
  size_t index;
  int cache_cleared;
  int projection_locked;
  int rc;
  int single_writer;
  lc_error batch_error;

  if (pouch == NULL || requests == NULL || count == 0U) {
    return;
  }
  memset(items, 0, sizeof(items));
  memset(&manifest, 0, sizeof(manifest));
  memset(&append_lock, 0, sizeof(append_lock));
  append_lock.fd = -1;
  lc_error_init(&batch_error);
  cache_cleared = 0;
  cache = NULL;
  projection_mutex = NULL;
  shared_mutation = NULL;
  projection_locked = 0;
  single_writer = lc_pouch_single_writer_enabled(pouch);
  rc = LC_OK;
  if (!projection_held && single_writer) {
    rc = lc_pouch_state_append_lock_acquire(pouch, requests[0]->ns,
                                            &append_lock, &batch_error);
  }
  if (!projection_held && rc == LC_OK && single_writer) {
    rc = lc_pouch_state_process_namespace_mutex_lock(
        pouch, requests[0]->ns, &projection_mutex, &batch_error);
    if (rc == LC_OK) {
      projection_locked = 1;
    }
  } else if (!projection_held && !single_writer) {
    rc = lc_pouch_state_shared_mutation_acquire(pouch, &shared_mutation,
                                                &batch_error);
  }
  if (!projection_held && !single_writer && rc == LC_OK) {
    rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
    if (rc != 0) {
      rc = LC_ERR_TRANSPORT;
    }
  }
  if (rc != LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_fail(requests[index], LC_ERR_TRANSPORT,
                                          &batch_error);
    }
    lc_pouch_state_append_lock_release(&append_lock);
    lc_pouch_state_shared_mutation_release(&shared_mutation);
    goto cleanup;
  }
  rc = lc_pouch_state_manifest_view(pouch, requests[0]->ns, &manifest, &cache,
                                    &batch_error);
  if (rc == LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_request *request;
      const unsigned char *metadata;
      size_t metadata_length;

      request = requests[index];
      request->updated_at_unix = lc_pouch_maintenance_now_seconds();
      request->retention = request->options.has_retention
                               ? request->options.retention
                               : request->current->retention;
      rc = lc_pouch_state_retention_publish(pouch, request->ns, request->key,
                                            request->retention,
                                            &request->retention, &batch_error);
      if (rc != LC_OK)
        break;
      request->options.has_retention = 1;
      request->options.retention = request->retention;
      metadata = request->options.has_metadata ? request->options.metadata
                                               : request->current->metadata;
      metadata_length = request->options.has_metadata
                            ? request->options.metadata_length
                            : request->current->metadata_length;
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, request->version, request->updated_at_unix,
          request->current->bytes, request->current->cipher_bytes,
          request->current->content_type != NULL
              ? request->current->content_type
              : "application/octet-stream",
          request->current->etag != NULL ? request->current->etag : "",
          request->current->descriptor, &request->current->payload_span,
          request->current->payload_context, metadata, metadata_length, 0U,
          request->has_query_hidden, request->query_hidden,
          request->current->staged_delete_marker, request->retention,
          &items[index].meta, &items[index].meta_len, &batch_error);
      if (rc != LC_OK) {
        break;
      }
      rc = lc_pouch_state_meta_set_index_sequences(
          &pouch->allocator, &items[index].meta, &items[index].meta_len, 0UL,
          request->current->query_index_seq, &batch_error);
      if (rc != LC_OK) {
        break;
      }
      items[index].record_type = LC_POUCH_STATE_RECORD_STATE_META;
      items[index].key = request->key;
      items[index].key_len = strlen(request->key);
      items[index].query_index_seq = request->current->query_index_seq;
    }
  }
  if (rc == LC_OK) {
    if (single_writer) {
      rc = lc_pouch_state_append_binary_records_locked(
          pouch, requests[0]->ns, &manifest, items, count, &batch_error);
    } else {
      rc = lc_pouch_state_append_binary_records(
          pouch, requests[0]->ns, &manifest, items, count, &batch_error);
    }
  }
  if (rc == LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_request *request;
      lc_pouch_namespace_manifest result_manifest;
      lc_pouch_generation index_seq;
      int result_rc;

      request = requests[index];
      index_seq = 0UL;
      result_rc = lc_pouch_state_meta_index_seq(
          items[index].meta, items[index].meta_len, &index_seq, request->error);
      if (result_rc == LC_OK) {
        result_manifest = manifest;
        result_manifest.state_max_version = index_seq;
        result_rc = lc_pouch_state_metadata_write_result_build(
            pouch, &result_manifest, request->current, &request->options,
            request->version, request->updated_at_unix,
            request->has_query_hidden, request->query_hidden, request->out,
            request->error);
      }
      if (result_rc == LC_OK) {
        const unsigned char *metadata;
        size_t metadata_length;

        metadata = request->options.has_metadata ? request->options.metadata
                                                 : request->current->metadata;
        metadata_length = request->options.has_metadata
                              ? request->options.metadata_length
                              : request->current->metadata_length;
        result_manifest = manifest;
        result_manifest.state_max_version = index_seq;
        (void)lc_pouch_state_cache_apply_write(
            pouch, request->ns, &result_manifest, request->key,
            request->current->content_type != NULL
                ? request->current->content_type
                : "application/octet-stream",
            request->current->etag != NULL ? request->current->etag : "",
            &request->current->payload_span, request->current->payload_context,
            metadata, metadata_length, request->version,
            request->current->bytes, request->current->cipher_bytes,
            request->current->descriptor, request->out->updated_at_unix,
            request->has_query_hidden, request->query_hidden,
            request->current->staged_delete_marker, 1,
            LC_POUCH_STATE_RECORD_STATE_META, request->retention);
        request->rc = LC_OK;
      } else {
        lc_pouch_state_write_result_cleanup(&pouch->allocator, request->out);
        if (!cache_cleared) {
          lc_pouch_state_cache_invalidate_namespace(pouch, request->ns);
          cache_cleared = 1;
        }
        request->rc = result_rc;
      }
    }
  } else {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_fail(requests[index], rc, &batch_error);
    }
  }
  if (projection_locked) {
    lc_pouch_state_process_namespace_mutex_unlock(&projection_mutex);
  } else if (!single_writer && !projection_held) {
    (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  lc_pouch_state_append_lock_release(&append_lock);
  lc_pouch_state_shared_mutation_release(&shared_mutation);

cleanup:
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(&pouch->allocator, items[index].meta);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_error_cleanup(&batch_error);
}

static void *lc_pouch_state_metadata_append_worker(void *context) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_request
      *requests[LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX];
  size_t count;

  batcher = (lc_pouch_state_metadata_append_batcher *)context;
  pthread_mutex_lock(&batcher->mutex);
  for (;;) {
    while (batcher->head == NULL && !batcher->stop) {
      pthread_cond_wait(&batcher->cond, &batcher->mutex);
    }
    if (batcher->head == NULL && batcher->stop) {
      pthread_mutex_unlock(&batcher->mutex);
      return NULL;
    }
    count = 0U;
    while (batcher->head != NULL &&
           count < LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX) {
      requests[count] = batcher->head;
      batcher->head = batcher->head->next;
      requests[count]->next = NULL;
      ++count;
    }
    if (batcher->head == NULL) {
      batcher->tail = NULL;
    }
    pthread_mutex_unlock(&batcher->mutex);
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_metadata_append_hook != NULL) {
      lc_pouch_test_metadata_append_hook(lc_pouch_test_metadata_append_context,
                                         batcher->ns);
    }
#endif
    lc_pouch_state_metadata_append_process(batcher->pouch, requests, count, 0);
    lc_pouch_state_metadata_append_complete(batcher, requests, count);
    pthread_mutex_lock(&batcher->mutex);
  }
}

static void lc_pouch_state_metadata_append_batcher_stop(
    lc_pouch_state_metadata_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  batcher->stop = 1;
  pthread_cond_broadcast(&batcher->cond);
  pthread_mutex_unlock(&batcher->mutex);
}

static void lc_pouch_state_metadata_append_batcher_destroy(
    lc_pouch_state_metadata_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  lc_pouch_state_metadata_append_batcher_stop(batcher);
  (void)pthread_join(batcher->thread, NULL);
  (void)pthread_cond_destroy(&batcher->cond);
  (void)pthread_mutex_destroy(&batcher->mutex);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher->ns);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher);
}

static void lc_pouch_state_body_append_complete(
    lc_pouch_state_body_append_batcher *batcher,
    lc_pouch_state_body_append_request **requests, size_t count) {
  size_t index;

  if (batcher == NULL || requests == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  for (index = 0U; index < count; ++index) {
    if (requests[index] != NULL) {
      requests[index]->done = 1;
      pthread_cond_signal(&requests[index]->cond);
    }
  }
  pthread_mutex_unlock(&batcher->mutex);
}

static void
lc_pouch_state_body_append_fail(lc_pouch_state_body_append_request *request,
                                int rc, const lc_error *cause) {
  const char *message;

  if (request == NULL) {
    return;
  }
  message = cause != NULL && cause->message != NULL
                ? cause->message
                : "pouch body append batch failed";
  request->rc =
      lc_error_set(request->error, rc, 0L, message, NULL, NULL, "pouch");
}

/* A queued body retains its caller's exact-key lock and commit group. The
 * worker owns one append authority window, but invokes the existing
 * write path for each request so record finalization, index reservation,
 * tail-recovery, cache publication, and source consumption remain identical
 * to a direct bounded-memory write. */
static void lc_pouch_state_body_append_process(
    lc_pouch *pouch, lc_pouch_state_body_append_request **requests,
    size_t count) {
  lc_pouch_state_append_lock append_lock;
  lc_pouch_state_commit_group *previous_group;
  size_t index;
  int pthread_rc;
  int rc;
  lc_error batch_error;

  if (pouch == NULL || requests == NULL || count == 0U) {
    return;
  }
  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  append_lock.pouch = NULL;
  append_lock.ns = NULL;
  append_lock.previous_append_lock = NULL;
  append_lock.borrowed = 0;
  lc_error_init(&batch_error);
  rc = lc_pouch_state_append_lock_acquire(pouch, requests[0]->ns, &append_lock,
                                          &batch_error);
  pthread_rc = 0;
  if (rc == LC_OK) {
    pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
    if (pthread_rc != 0) {
      rc = lc_error_set(&batch_error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch shared mutation state",
                        strerror(pthread_rc), NULL, "pouch");
    }
  }
  if (rc != LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_body_append_fail(requests[index], rc, &batch_error);
    }
    lc_pouch_state_append_lock_release(&append_lock);
    lc_error_cleanup(&batch_error);
    return;
  }
  lc_pouch_state_body_append_worker_set_active(1);
  for (index = 0U; index < count; ++index) {
    previous_group = lc_pouch_state_commit_group_current();
    lc_pouch_state_commit_group_set(requests[index]->commit_group);
    requests[index]->rc = lc_pouch_state_write_locked(
        pouch, requests[index]->ns, requests[index]->key, requests[index]->body,
        requests[index]->options, requests[index]->out, requests[index]->error);
    lc_pouch_state_commit_group_set(previous_group);
  }
  lc_pouch_state_body_append_worker_set_active(0);
  (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
  lc_pouch_state_append_lock_release(&append_lock);
  lc_error_cleanup(&batch_error);
}

static void *lc_pouch_state_body_append_worker(void *context) {
  lc_pouch_state_body_append_batcher *batcher;
  lc_pouch_state_body_append_request
      *requests[LC_POUCH_STATE_BODY_APPEND_BATCH_MAX];
  size_t count;

  batcher = (lc_pouch_state_body_append_batcher *)context;
  pthread_mutex_lock(&batcher->mutex);
  for (;;) {
    while (batcher->head == NULL && !batcher->stop) {
      pthread_cond_wait(&batcher->cond, &batcher->mutex);
    }
    if (batcher->head == NULL && batcher->stop) {
      pthread_mutex_unlock(&batcher->mutex);
      return NULL;
    }
    count = 0U;
    while (batcher->head != NULL &&
           count < LC_POUCH_STATE_BODY_APPEND_BATCH_MAX) {
      requests[count] = batcher->head;
      batcher->head = batcher->head->next;
      requests[count]->next = NULL;
      ++count;
    }
    if (batcher->head == NULL) {
      batcher->tail = NULL;
    }
    pthread_mutex_unlock(&batcher->mutex);
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_body_append_hook != NULL) {
      lc_pouch_test_body_append_hook(lc_pouch_test_body_append_context,
                                     batcher->ns);
    }
#endif
    lc_pouch_state_body_append_process(batcher->pouch, requests, count);
    lc_pouch_state_body_append_complete(batcher, requests, count);
    pthread_mutex_lock(&batcher->mutex);
  }
}

static void lc_pouch_state_body_append_batcher_stop(
    lc_pouch_state_body_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  batcher->stop = 1;
  pthread_cond_broadcast(&batcher->cond);
  pthread_mutex_unlock(&batcher->mutex);
}

static void lc_pouch_state_body_append_batcher_destroy(
    lc_pouch_state_body_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  lc_pouch_state_body_append_batcher_stop(batcher);
  (void)pthread_join(batcher->thread, NULL);
  (void)pthread_cond_destroy(&batcher->cond);
  (void)pthread_mutex_destroy(&batcher->mutex);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher->ns);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher);
}

static int
lc_pouch_state_body_append_batcher_get(lc_pouch *pouch, const char *ns,
                                       lc_pouch_state_body_append_batcher **out,
                                       lc_error *error) {
  lc_pouch_state_body_append_batcher *batcher;
  lc_pouch_namespace_logstore *cache;
  int cond_initialized;
  int mutex_initialized;
  int pthread_rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL ||
      !pouch->state_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body append queue requires namespace", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace owner registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  batcher = cache->body_append_batcher;
  if (batcher != NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    *out = batcher;
    return LC_OK;
  }
  batcher = (lc_pouch_state_body_append_batcher *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*batcher));
  if (batcher == NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch body append queue", NULL,
                        NULL, "pouch");
  }
  batcher->pouch = pouch;
  batcher->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (batcher->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch body append namespace", NULL,
                        NULL, "pouch");
  }
  mutex_initialized = 0;
  cond_initialized = 0;
  pthread_rc = pthread_mutex_init(&batcher->mutex, NULL);
  if (pthread_rc == 0) {
    mutex_initialized = 1;
    pthread_rc = pthread_cond_init(&batcher->cond, NULL);
    if (pthread_rc == 0) {
      cond_initialized = 1;
      pthread_rc = pthread_create(&batcher->thread, NULL,
                                  lc_pouch_state_body_append_worker, batcher);
    }
  }
  if (pthread_rc != 0) {
    if (cond_initialized) {
      (void)pthread_cond_destroy(&batcher->cond);
    }
    if (mutex_initialized) {
      (void)pthread_mutex_destroy(&batcher->mutex);
    }
    lc_free_with_allocator(&pouch->allocator, batcher->ns);
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch body append queue",
                        strerror(pthread_rc), NULL, "pouch");
  }
  cache->body_append_batcher = batcher;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  *out = batcher;
  return LC_OK;
}

static int lc_pouch_state_body_append_schedule_locked(
    lc_pouch *pouch, const char *ns, const char *key, lc_source *body,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_body_append_batcher *batcher;
  lc_pouch_state_body_append_request request;
  int pthread_rc;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || body == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body append scheduling requires inputs", NULL,
                        NULL, "pouch");
  }
  memset(&request, 0, sizeof(request));
  request.ns = ns;
  request.key = key;
  request.body = body;
  request.options = options;
  request.out = out;
  request.error = error;
  request.commit_group = lc_pouch_state_commit_group_current();
  pthread_rc = pthread_cond_init(&request.cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch body append request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  rc = lc_pouch_state_body_append_batcher_get(pouch, ns, &batcher, error);
  if (rc != LC_OK) {
    (void)pthread_cond_destroy(&request.cond);
    return rc;
  }
  /* Exact-key ownership remains with the caller while the worker serializes
   * only the physical append and shared projection window. */
  lc_pouch_state_projection_mutex_unlock(pouch, ns);
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->stop) {
    pthread_mutex_unlock(&batcher->mutex);
    rc = lc_pouch_state_projection_mutex_lock(pouch, ns, error);
    (void)pthread_cond_destroy(&request.cond);
    if (rc != LC_OK) {
      return rc;
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body append worker is stopping", NULL, NULL,
                        "pouch");
  }
  if (batcher->tail != NULL) {
    batcher->tail->next = &request;
  } else {
    batcher->head = &request;
  }
  batcher->tail = &request;
  pthread_cond_signal(&batcher->cond);
  while (!request.done) {
    pthread_cond_wait(&request.cond, &batcher->mutex);
  }
  pthread_mutex_unlock(&batcher->mutex);
  rc = lc_pouch_state_projection_mutex_lock(pouch, ns, error);
  (void)pthread_cond_destroy(&request.cond);
  if (rc != LC_OK) {
    return rc;
  }
  return request.rc;
}

static int lc_pouch_state_metadata_append_batcher_get(
    lc_pouch *pouch, const char *ns,
    lc_pouch_state_metadata_append_batcher **out, lc_error *error) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_namespace_logstore *cache;
  int cond_initialized;
  int mutex_initialized;
  int pthread_rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL ||
      !pouch->state_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append queue requires namespace", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace owner registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  batcher = cache->metadata_append_batcher;
  if (batcher != NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    *out = batcher;
    return LC_OK;
  }
  batcher = (lc_pouch_state_metadata_append_batcher *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*batcher));
  if (batcher == NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata append queue", NULL,
                        NULL, "pouch");
  }
  batcher->pouch = pouch;
  batcher->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (batcher->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata append namespace",
                        NULL, NULL, "pouch");
  }
  mutex_initialized = 0;
  cond_initialized = 0;
  pthread_rc = pthread_mutex_init(&batcher->mutex, NULL);
  if (pthread_rc == 0) {
    mutex_initialized = 1;
    pthread_rc = pthread_cond_init(&batcher->cond, NULL);
    if (pthread_rc == 0) {
      cond_initialized = 1;
      pthread_rc =
          pthread_create(&batcher->thread, NULL,
                         lc_pouch_state_metadata_append_worker, batcher);
    }
  }
  if (pthread_rc != 0) {
    if (cond_initialized) {
      (void)pthread_cond_destroy(&batcher->cond);
    }
    if (mutex_initialized) {
      (void)pthread_mutex_destroy(&batcher->mutex);
    }
    lc_free_with_allocator(&pouch->allocator, batcher->ns);
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch metadata append queue",
                        strerror(pthread_rc), NULL, "pouch");
  }
  cache->metadata_append_batcher = batcher;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  *out = batcher;
  return LC_OK;
}

int lc_pouch_state_metadata_append_worker_init(lc_pouch *pouch,
                                               lc_error *error) {
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append worker requires pouch", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

void lc_pouch_state_metadata_append_worker_close(lc_pouch *pouch) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_batcher *next;
  lc_pouch_state_body_append_batcher *body_batcher;
  lc_pouch_state_body_append_batcher *body_next;
  lc_pouch_namespace_logstore *cache;

  if (pouch == NULL || !pouch->state_cache_mutex_initialized) {
    return;
  }
  batcher = NULL;
  body_batcher = NULL;
  pthread_mutex_lock(&pouch->state_cache_mutex);
  for (cache = pouch->namespace_logstores; cache != NULL; cache = cache->next) {
    if (cache->metadata_append_batcher != NULL) {
      cache->metadata_append_batcher->next = batcher;
      batcher = cache->metadata_append_batcher;
      cache->metadata_append_batcher = NULL;
    }
    if (cache->body_append_batcher != NULL) {
      cache->body_append_batcher->next = body_batcher;
      body_batcher = cache->body_append_batcher;
      cache->body_append_batcher = NULL;
    }
  }
  pthread_mutex_unlock(&pouch->state_cache_mutex);
  for (next = batcher; next != NULL; next = next->next) {
    lc_pouch_state_metadata_append_batcher_stop(next);
  }
  while (batcher != NULL) {
    next = batcher->next;
    lc_pouch_state_metadata_append_batcher_destroy(batcher);
    batcher = next;
  }
  for (body_next = body_batcher; body_next != NULL;
       body_next = body_next->next) {
    lc_pouch_state_body_append_batcher_stop(body_next);
  }
  while (body_batcher != NULL) {
    body_next = body_batcher->next;
    lc_pouch_state_body_append_batcher_destroy(body_batcher);
    body_batcher = body_next;
  }
}

static int lc_pouch_state_metadata_append_submit_locked(
    lc_pouch *pouch, lc_pouch_state_metadata_append_request *request,
    lc_error *error) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_request *inline_request[1];
  int pthread_rc;
  int rc;

  if (pouch == NULL || request == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append worker is unavailable", NULL,
                        NULL, "pouch");
  }
  pthread_rc = pthread_cond_init(&request->cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch metadata append request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  request->next = NULL;
  request->done = 0;
  request->rc = LC_OK;
  /* Namespace callbacks retain a stronger atomic authority. Their metadata
   * records must stay on this thread so a worker cannot wait on that authority
   * while the callback waits for its completion. */
  if (lc_pouch_state_namespace_lock_is_held(pouch, request->ns)) {
    inline_request[0] = request;
    lc_pouch_state_metadata_append_process(pouch, inline_request, 1U, 1);
    pthread_cond_destroy(&request->cond);
    return request->rc;
  }
  rc = lc_pouch_state_metadata_append_batcher_get(pouch, request->ns, &batcher,
                                                  error);
  if (rc != LC_OK) {
    pthread_cond_destroy(&request->cond);
    return rc;
  }
  /* The caller still owns its exact-key lock. Release projection ownership
   * while the bounded inline batch is appended and published by the worker. */
  lc_pouch_state_projection_mutex_unlock(pouch, request->ns);
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->tail != NULL) {
    batcher->tail->next = request;
  } else {
    batcher->head = request;
  }
  batcher->tail = request;
  pthread_cond_signal(&batcher->cond);
  while (!request->done) {
    pthread_cond_wait(&request->cond, &batcher->mutex);
  }
  pthread_mutex_unlock(&batcher->mutex);
  rc = lc_pouch_state_projection_mutex_lock(pouch, request->ns, error);
  pthread_cond_destroy(&request->cond);
  if (rc != LC_OK) {
    return rc;
  }
  return request->rc;
}

static int lc_pouch_state_update_metadata_from_current_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_generation logical_version;
  lc_pouch_generation version;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int query_index_guard_started;
  int rc;
  lc_pouch_state_precondition_view precondition_view;
  lc_pouch_state_write_options resolved_options;

  if (pouch == NULL || ns == NULL || key == NULL || manifest == NULL ||
      current == NULL || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update current state is invalid", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  query_index_guard_started = 0;
  rc = LC_OK;
  if (options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!current->found && !options->has_metadata) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  if (options->has_retention && !current->payload_span.present)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention requires an existing body", NULL, NULL,
                        "pouch");
  logical_version =
      current->found && current->payload_span.present ? current->version : 0UL;
  if (options->has_expected_version &&
      logical_version != options->expected_version) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  if (options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  resolved_options = *options;
  resolved_options.retention =
      options->has_retention ? options->retention : current->retention;
  rc = lc_pouch_state_retention_publish(pouch, ns, key,
                                        resolved_options.retention,
                                        &resolved_options.retention, error);
  if (rc != LC_OK)
    return rc;
  resolved_options.has_retention = 1;
  options = &resolved_options;
  has_query_hidden = current->has_query_hidden;
  query_hidden = current->query_hidden;
  if (options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  }
  version = logical_version;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  if (options->query_index_operation_active && !options->object_record) {
    lc_pouch_unix_seconds expires_at_unix;

    expires_at_unix = options->query_index_operation_expires_at_unix != NULL
                          ? *options->query_index_operation_expires_at_unix
                          : 0L;
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, options->query_index_operation_id, expires_at_unix,
        &query_index_guard_started, error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_append_record(
      pouch, ns, manifest, LC_POUCH_STATE_RECORD_STATE_META, key,
      current->content_type != NULL ? current->content_type
                                    : "application/octet-stream",
      current->etag != NULL ? current->etag : "", &current->payload_span,
      current->payload_context,
      options->has_metadata ? options->metadata : current->metadata,
      options->has_metadata ? options->metadata_length
                            : current->metadata_length,
      version, current->bytes, current->cipher_bytes, current->descriptor,
      updated_at_unix, has_query_hidden, query_hidden,
      current->staged_delete_marker,
      options->has_retention ? options->retention : current->retention,
      current->query_index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_metadata_write_result_build(
        pouch, manifest, current, options, version, updated_at_unix,
        has_query_hidden, query_hidden, out, error);
    if (rc == LC_OK) {
      out->query_index_operation_guard_started = query_index_guard_started;
    }
    (void)lc_pouch_state_cache_apply_write(
        pouch, ns, manifest, key,
        current->content_type != NULL ? current->content_type
                                      : "application/octet-stream",
        current->etag != NULL ? current->etag : "", &current->payload_span,
        current->payload_context,
        options->has_metadata ? options->metadata : current->metadata,
        options->has_metadata ? options->metadata_length
                              : current->metadata_length,
        version, current->bytes, current->cipher_bytes, current->descriptor,
        updated_at_unix, has_query_hidden, query_hidden,
        current->staged_delete_marker, 1, LC_POUCH_STATE_RECORD_STATE_META,
        out->retention);
  }
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
    if (query_index_guard_started) {
      lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                            options->query_index_operation_id);
    }
  }
  return rc;
}

static int lc_pouch_state_metadata_batching_allowed(lc_pouch *pouch,
                                                    const char *ns) {
  lc_pouch_state_commit_group *group;
  if (!lc_pouch_single_writer_enabled(pouch) ||
      lc_pouch_state_namespace_lock_is_held(pouch, ns))
    return 0;
  group = lc_pouch_state_commit_group_current();
  /* Forced cleanup barriers belong to this thread. Ordinary metadata batches
   * run on a separate worker with its own publication/durability boundary. */
  return group == NULL || !group->force_sync;
}

/* Queue only complete metadata records. The caller keeps its exact-key lock
 * while waiting, so the copied current entry remains the CAS decision for this
 * request even though independent keys may advance the append projection. */
static int lc_pouch_state_metadata_append_schedule_from_current_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_state_entry *current, int current_is_borrowed,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_metadata_append_request request;
  lc_pouch_state_entry scheduled_current;
  lc_pouch_generation logical_version;
  int has_query_hidden;
  int query_hidden;
  int query_index_guard_started;
  int rc;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || ns == NULL || key == NULL || current == NULL ||
      options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append scheduling requires inputs",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  query_index_guard_started = 0;
  if (options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!current->found && !options->has_metadata) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  if (options->has_retention && !current->payload_span.present)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention requires an existing body", NULL, NULL,
                        "pouch");
  logical_version =
      current->found && current->payload_span.present ? current->version : 0UL;
  if (options->has_expected_version &&
      logical_version != options->expected_version) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  if (options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  has_query_hidden = current->has_query_hidden;
  query_hidden = current->query_hidden;
  if (options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  }
  if (options->query_index_operation_active && !options->object_record) {
    lc_pouch_unix_seconds expires_at_unix;

    expires_at_unix = options->query_index_operation_expires_at_unix != NULL
                          ? *options->query_index_operation_expires_at_unix
                          : 0L;
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, options->query_index_operation_id, expires_at_unix,
        &query_index_guard_started, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(&request, 0, sizeof(request));
  memset(&scheduled_current, 0, sizeof(scheduled_current));
  if (current_is_borrowed) {
    rc = lc_pouch_state_metadata_append_entry_copy(pouch, current,
                                                   &scheduled_current, error);
    if (rc != LC_OK) {
      if (query_index_guard_started) {
        lc_pouch_query_index_operation_cancel(
            pouch, ns, key, options->query_index_operation_id);
      }
      return rc;
    }
  }
  request.ns = ns;
  request.key = key;
  request.current = current_is_borrowed ? &scheduled_current : current;
  request.options = *options;
  request.version = logical_version;
  request.has_query_hidden = has_query_hidden;
  request.query_hidden = query_hidden;
  request.out = out;
  request.error = error;
  rc = lc_pouch_state_metadata_append_submit_locked(pouch, &request, error);
  if (rc == LC_OK) {
    out->query_index_operation_guard_started = query_index_guard_started;
  } else if (query_index_guard_started) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &scheduled_current);
  return rc;
}

int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_generation max_version;
  int use_metadata_batcher;
  int current_is_borrowed;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_update_metadata requires pouch, "
                        "namespace, key, options, and out",
                        NULL, NULL, NULL);
  }
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  current_is_borrowed = 0;
  /* A shared-root key mutation retains root-wide durable authority, so only
   * the exclusive fast path may transfer metadata publication to its
   * namespace-local worker. */
  use_metadata_batcher = lc_pouch_state_metadata_batching_allowed(pouch, ns);
  if (use_metadata_batcher &&
      lc_pouch_state_cache_borrow_exclusive_record(pouch, ns, key, &current)) {
    current_is_borrowed = 1;
    rc = LC_OK;
  } else {
    rc = lc_pouch_state_manifest_lookup_cached_borrowed(
        pouch, ns, key, &manifest, &current, &max_version, &current_is_borrowed,
        error);
  }
  if (rc == LC_OK) {
    if (use_metadata_batcher) {
      rc = lc_pouch_state_metadata_append_schedule_from_current_locked(
          pouch, ns, key, &current, current_is_borrowed, options, out, error);
    } else {
      rc = lc_pouch_state_update_metadata_from_current_locked(
          pouch, ns, key, &manifest, &current, options, out, error);
    }
  }
  if (!current_is_borrowed) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_update_metadata_prepared_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_state_metadata_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_state_metadata_view view;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_write_options options;
  lc_pouch_generation max_version;
  int apply;
  int use_metadata_batcher;
  int current_is_borrowed;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || prepare == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared metadata update requires valid inputs",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  memset(&view, 0, sizeof(view));
  memset(&options, 0, sizeof(options));
  current_is_borrowed = 0;
  use_metadata_batcher = lc_pouch_state_metadata_batching_allowed(pouch, ns);
  if (use_metadata_batcher &&
      lc_pouch_state_cache_borrow_exclusive_record(pouch, ns, key, &current)) {
    current_is_borrowed = 1;
    rc = LC_OK;
  } else {
    rc = lc_pouch_state_manifest_lookup_cached_borrowed(
        pouch, ns, key, &manifest, &current, &max_version, &current_is_borrowed,
        error);
  }
  if (rc == LC_OK) {
    view.found = current.found;
    view.etag = current.etag;
    view.version = current.version;
    view.retention = current.retention;
    view.metadata = current.metadata;
    view.metadata_length = current.metadata_length;
    view.has_query_hidden = current.has_query_hidden;
    view.query_hidden = current.query_hidden;
    view.has_body = current.payload_span.present;
    apply = 1;
    rc = prepare(&view, prepare_context, &options, &apply, error);
    if (rc == LC_OK && apply) {
      if (use_metadata_batcher) {
        rc = lc_pouch_state_metadata_append_schedule_from_current_locked(
            pouch, ns, key, &current, current_is_borrowed, &options, out,
            error);
      } else {
        rc = lc_pouch_state_update_metadata_from_current_locked(
            pouch, ns, key, &manifest, &current, &options, out, error);
      }
    }
  }
  if (!current_is_borrowed) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_update_metadata(lc_pouch *pouch, const char *ns,
                                   const char *key,
                                   const lc_pouch_state_write_options *options,
                                   lc_pouch_state_write_result *out,
                                   lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_update_metadata_locked(pouch, ns, key, options, out,
                                                 error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_update_metadata_locked(pouch, ns, key, options, out,
                                             error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc != LC_OK && out->query_index_operation_guard_started &&
      options != NULL && options->query_index_operation_id != NULL) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
    out->query_index_operation_guard_started = 0;
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 0);
  }
  return rc;
}

static int
lc_pouch_state_delete_locked(lc_pouch *pouch, const char *ns, const char *key,
                             const lc_pouch_state_write_options *options,
                             lc_pouch_state_write_result *out,
                             lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  char *etag;
  lc_pouch_generation max_version;
  lc_pouch_generation version;
  lc_pouch_generation index_seq;
  lc_pouch_unix_seconds updated_at_unix;
  int query_index_guard_started;
  int rc;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_delete requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  index_seq = 0UL;
  query_index_guard_started = 0;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &current, &max_version, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (options != NULL && options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(&current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete etag precondition failed", NULL,
                          NULL, NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete version precondition failed",
                          NULL, NULL, NULL);
    }
  }
  if (options != NULL && options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (!current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return LC_OK;
  }
  /* A successful acquire creates a lease-only metadata projection. Removing
   * that projection is a no-op: it must preserve the active lease and report
   * the same no-state result as lockd disk. */
  if (!current.payload_span.present) {
    out->index_seq = manifest.state_max_version;
    out->version = current.version;
    out->updated_at_unix = current.updated_at_unix;
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return LC_OK;
  }
  (void)max_version;
  version = current.version + 1UL;
  etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
  if (etag == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  if (options != NULL && options->query_index_operation_active &&
      !options->object_record) {
    lc_pouch_unix_seconds expires_at_unix;

    expires_at_unix = options->query_index_operation_expires_at_unix != NULL
                          ? *options->query_index_operation_expires_at_unix
                          : 0L;
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, options->query_index_operation_id, expires_at_unix,
        &query_index_guard_started, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  rc = lc_pouch_state_append_tombstone(
      pouch, ns, &manifest, key, etag, version, updated_at_unix,
      options != NULL && options->object_record
          ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
          : current.record_type,
      options == NULL || !options->object_record, &index_seq, error);
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, ns, &manifest, key, NULL, etag, NULL, NULL, NULL, 0U, version,
        0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
        options != NULL && options->object_record
            ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
            : current.record_type,
        0);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->index_seq = index_seq;
    out->version = version;
    out->bytes = 0UL;
    out->updated_at_unix = updated_at_unix;
    out->query_index_operation_guard_started = query_index_guard_started;
    etag = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (rc != LC_OK && query_index_guard_started) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
  }
  return rc;
}

int lc_pouch_state_delete(lc_pouch *pouch, const char *ns, const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_delete_locked(pouch, ns, key, options, out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_delete_locked(pouch, ns, key, options, out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc != LC_OK && out->query_index_operation_guard_started &&
      options != NULL && options->query_index_operation_id != NULL) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key,
                                          options->query_index_operation_id);
    out->query_index_operation_guard_started = 0;
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 1);
  }
  if (rc == LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] =
        lc_log_u64_field("generation", out != NULL ? out->version : 0UL);
    lc_log_trace(pouch->logger, "logstore.delete", fields, 3U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.delete.error", fields, 4U);
  }
  if (rc == LC_OK && pouch->query_indexing_enabled && out != NULL &&
      out->version > 0UL && (options == NULL || !options->object_record)) {
    lc_pouch_query_index_note_state_delete(
        pouch, ns, key, out,
        options != NULL && options->query_index_operation_active);
  }
  return rc;
}

int lc_pouch_state_stage_write(lc_pouch *pouch, const char *ns, const char *key,
                               const char *txn_id, lc_source *body,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_state_write_result *out,
                               lc_error *error) {
  char *staged_key;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' || body == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_stage_write requires pouch, "
                        "namespace, key, txn_id, body and out",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_state_write(pouch, ns, staged_key, body, options, out, error);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

typedef struct lc_pouch_state_prepared_stage_context {
  lc_pouch *pouch;
  const char *ns;
  const char *key;
  const char *txn_id;
  const char *lease_key;
  lc_source *body;
  lc_pouch_state_write_options *options;
  lc_pouch_state_stage_prepare_fn prepare;
  void *prepare_context;
  lc_pouch_state_stage_body_prepare_fn body_prepare;
  lc_pouch_state_write_result *out;
} lc_pouch_state_prepared_stage_context;

/* The namespace callback is the stage mutation authority: no client-side
 * read can become stale between its lease/CAS decision and the staged append.
 */
static int lc_pouch_state_stage_write_prepared_locked(void *context,
                                                      lc_error *error) {
  lc_pouch_state_prepared_stage_context *stage;
  lc_pouch_state_read_result committed;
  lc_pouch_state_read_result lease;
  lc_pouch_state_read_result committed_body;
  lc_pouch_state_read_result staged_body;
  lc_pouch_state_entry staged_entry;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_write_options staged_write_options;
  lc_pouch_state_metadata_view committed_view;
  lc_pouch_state_metadata_view staged_view;
  lc_pouch_state_metadata_view lease_view;
  lc_pouch_state_read_result *lease_source;
  lc_source *body;
  lc_source *prepared_body;
  char *staged_key;
  lc_pouch_unix_seconds operation_expires_at_unix;
  int apply;
  int query_index_guard_started;
  int staged_entry_transferred;
  int rc;

  stage = (lc_pouch_state_prepared_stage_context *)context;
  if (stage == NULL || stage->pouch == NULL || stage->ns == NULL ||
      stage->key == NULL || stage->txn_id == NULL || stage->lease_key == NULL ||
      ((stage->body == NULL && stage->body_prepare == NULL) ||
       (stage->body != NULL && stage->body_prepare != NULL)) ||
      stage->options == NULL || stage->prepare == NULL || stage->out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared stage requires complete context", NULL,
                        NULL, "pouch");
  }
  memset(&committed, 0, sizeof(committed));
  memset(&lease, 0, sizeof(lease));
  memset(&committed_body, 0, sizeof(committed_body));
  memset(&staged_body, 0, sizeof(staged_body));
  memset(&staged_entry, 0, sizeof(staged_entry));
  memset(&manifest, 0, sizeof(manifest));
  memset(&staged_write_options, 0, sizeof(staged_write_options));
  memset(&committed_view, 0, sizeof(committed_view));
  memset(&staged_view, 0, sizeof(staged_view));
  memset(&lease_view, 0, sizeof(lease_view));
  body = NULL;
  prepared_body = NULL;
  staged_key = NULL;
  operation_expires_at_unix = 0L;
  apply = 1;
  query_index_guard_started = 0;
  staged_entry_transferred = 0;
  rc = lc_pouch_state_read_metadata_locked(stage->pouch, stage->ns, stage->key,
                                           &committed, error);
  if (rc == LC_OK) {
    staged_key = lc_pouch_state_staged_key(&stage->pouch->allocator, stage->key,
                                           stage->txn_id);
    if (staged_key == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
        stage->pouch, stage->ns, staged_key, &manifest, &staged_entry, NULL,
        error);
  }
  lease_source = &committed;
  if (rc == LC_OK && strcmp(stage->lease_key, stage->key) != 0) {
    rc = lc_pouch_state_read_metadata_locked(stage->pouch, stage->ns,
                                             stage->lease_key, &lease, error);
    lease_source = &lease;
  }
  if (rc == LC_OK) {
    lc_pouch_state_metadata_view_from_read_result(&committed, &committed_view);
    lc_pouch_state_metadata_view_from_entry(&staged_entry, &staged_view);
    lc_pouch_state_metadata_view_from_read_result(lease_source, &lease_view);
    rc = stage->prepare(&committed_view, &staged_view, &lease_view,
                        stage->prepare_context, stage->options, &apply, error);
  }
  if (rc == LC_OK && apply) {
    body = stage->body;
    if (stage->body_prepare != NULL) {
      rc = lc_pouch_state_read_locked(stage->pouch, stage->ns, stage->key,
                                      &committed_body, error);
    }
    if (rc == LC_OK && stage->body_prepare != NULL) {
      rc = lc_pouch_state_read_locked(stage->pouch, stage->ns, staged_key,
                                      &staged_body, error);
    }
    if (rc == LC_OK && stage->body_prepare != NULL) {
      rc = stage->body_prepare(&committed_body, &staged_body,
                               stage->prepare_context, &prepared_body, error);
      body = prepared_body;
    }
    if (rc == LC_OK && body == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared stage callback returned no body", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK && apply) {
    staged_entry_transferred = 1;
    if (stage->options->query_index_operation_active &&
        !stage->options->object_record) {
      operation_expires_at_unix =
          stage->options->query_index_operation_expires_at_unix != NULL
              ? *stage->options->query_index_operation_expires_at_unix
              : 0L;
      rc = lc_pouch_query_index_operation_begin(
          stage->pouch, stage->ns, stage->key, stage->txn_id,
          operation_expires_at_unix, &query_index_guard_started, error);
    }
    /* The transaction owns a public-key guard while its private staging
     * record advances the durable freshness frontier. */
    if (rc == LC_OK) {
      staged_write_options = *stage->options;
      staged_write_options.query_index_operation_active = 0;
      staged_write_options.query_index_operation_id = NULL;
      staged_write_options.query_index_operation_expires_at_unix = NULL;
      staged_write_options.suppress_query_index = 1;
      rc = lc_pouch_state_write_resolved_locked(
          stage->pouch, stage->ns, staged_key, body, &staged_write_options,
          &staged_entry, &manifest, manifest.borrowed, stage->out, error);
    }
    if (rc == LC_OK) {
      stage->out->query_index_operation_guard_started =
          query_index_guard_started;
    }
  }
  if (prepared_body != NULL) {
    lc_source_close(prepared_body);
  }
  lc_free_with_allocator(&stage->pouch->allocator, staged_key);
  lc_pouch_state_read_result_cleanup(&stage->pouch->allocator, &lease);
  lc_pouch_state_read_result_cleanup(&stage->pouch->allocator, &staged_body);
  lc_pouch_state_read_result_cleanup(&stage->pouch->allocator, &committed_body);
  if (!staged_entry_transferred) {
    lc_pouch_state_entry_cleanup(&stage->pouch->allocator, &staged_entry);
    lc_pouch_namespace_manifest_cleanup(&stage->pouch->allocator, &manifest);
  }
  lc_pouch_state_read_result_cleanup(&stage->pouch->allocator, &committed);
  if (rc != LC_OK && query_index_guard_started) {
    lc_pouch_query_index_operation_cancel(stage->pouch, stage->ns, stage->key,
                                          stage->txn_id);
  }
  return rc;
}

int lc_pouch_state_stage_write_prepared(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *lease_key, lc_source *body,
    lc_pouch_state_write_options *options,
    lc_pouch_state_stage_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_stage_body_prepare_fn body_prepare,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_prepared_stage_context context;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      lease_key == NULL || lease_key[0] == '\0' ||
      ((body == NULL && body_prepare == NULL) ||
       (body != NULL && body_prepare != NULL)) ||
      options == NULL || prepare == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared stage requires pouch, namespace, key, "
                        "transaction, lease key, body callback, options, "
                        "callback and output",
                        NULL, NULL, "pouch");
  }
  memset(&context, 0, sizeof(context));
  context.pouch = pouch;
  context.ns = ns;
  context.key = key;
  context.txn_id = txn_id;
  context.lease_key = lease_key;
  context.body = body;
  context.options = options;
  context.prepare = prepare;
  context.prepare_context = prepare_context;
  context.body_prepare = body_prepare;
  context.out = out;
  return lc_pouch_state_with_namespace_lock(
      pouch, ns, lc_pouch_state_stage_write_prepared_locked, &context, error);
}

typedef struct lc_pouch_state_prepared_metadata_stage_context {
  lc_pouch *pouch;
  const char *ns;
  const char *key;
  const char *txn_id;
  const char *lease_key;
  lc_pouch_state_write_options *options;
  lc_pouch_state_stage_prepare_fn prepare;
  void *prepare_context;
  lc_pouch_state_write_result *out;
} lc_pouch_state_prepared_metadata_stage_context;

/* Metadata staging cannot use the regular source writer: a lease may protect
 * an absent value, and materializing an empty source would turn that absence
 * into a zero-byte state body. Preserve the committed or transaction-local
 * payload span directly in a staged metadata record instead. */
static int lc_pouch_state_stage_metadata_prepared_locked(void *context,
                                                         lc_error *error) {
  lc_pouch_state_prepared_metadata_stage_context *stage;
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_read_result lease;
  lc_pouch_state_metadata_view committed_view;
  lc_pouch_state_metadata_view staged_view;
  lc_pouch_state_metadata_view lease_view;
  lc_pouch_state_entry *current;
  char *staged_key;
  char *old_binding_context;
  char *new_binding_context;
  char *staged_descriptor;
  lc_pouch_generation logical_version;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_unix_seconds operation_expires_at_unix;
  int apply;
  int has_query_hidden;
  int query_hidden;
  int query_index_guard_started;
  int rc;

  stage = (lc_pouch_state_prepared_metadata_stage_context *)context;
  if (stage == NULL || stage->pouch == NULL || stage->ns == NULL ||
      stage->key == NULL || stage->txn_id == NULL || stage->lease_key == NULL ||
      stage->options == NULL || stage->prepare == NULL || stage->out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared metadata stage requires complete "
                        "context",
                        NULL, NULL, "pouch");
  }
  memset(stage->out, 0, sizeof(*stage->out));
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  memset(&manifest, 0, sizeof(manifest));
  memset(&lease, 0, sizeof(lease));
  memset(&committed_view, 0, sizeof(committed_view));
  memset(&staged_view, 0, sizeof(staged_view));
  memset(&lease_view, 0, sizeof(lease_view));
  staged_key = NULL;
  old_binding_context = NULL;
  new_binding_context = NULL;
  staged_descriptor = NULL;
  current = NULL;
  logical_version = 0UL;
  updated_at_unix = 0L;
  operation_expires_at_unix = 0L;
  has_query_hidden = 0;
  query_hidden = 0;
  apply = 1;
  query_index_guard_started = 0;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      stage->pouch, stage->ns, stage->key, &manifest, &committed, NULL, error);
  if (rc == LC_OK) {
    staged_key = lc_pouch_state_staged_key(&stage->pouch->allocator, stage->key,
                                           stage->txn_id);
    if (staged_key == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        "pouch");
    }
  }
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(stage->pouch, stage->ns, 0, NULL);
    if (cache != NULL && cache->initialized) {
      rc = lc_pouch_state_entry_from_cache_record(
          stage->pouch, lc_pouch_state_cache_record_find(cache, staged_key),
          &staged, error);
    } else {
      rc = lc_pouch_state_scan(stage->pouch, &manifest, staged_key, &staged,
                               NULL, error);
    }
  }
  if (rc == LC_OK && strcmp(stage->lease_key, stage->key) != 0) {
    rc = lc_pouch_state_read_metadata_locked(stage->pouch, stage->ns,
                                             stage->lease_key, &lease, error);
  }
  if (rc == LC_OK) {
    lc_pouch_state_metadata_view_from_entry(&committed, &committed_view);
    lc_pouch_state_metadata_view_from_entry(&staged, &staged_view);
    if (strcmp(stage->lease_key, stage->key) == 0) {
      lease_view = committed_view;
    } else {
      lc_pouch_state_metadata_view_from_read_result(&lease, &lease_view);
    }
    rc = stage->prepare(&committed_view, &staged_view, &lease_view,
                        stage->prepare_context, stage->options, &apply, error);
  }
  if (rc == LC_OK && apply) {
    if (staged.found && staged.staged_delete_marker) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update conflicts with staged delete",
                        NULL, NULL, "pouch");
    } else {
      current = staged.found ? &staged : &committed;
    }
  }
  if (rc == LC_OK && apply) {
    if (!current->found && !stage->options->has_metadata) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
    }
    if (rc == LC_OK && stage->options->has_retention &&
        !current->payload_span.present)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention requires an existing body", NULL, NULL,
                        "pouch");
  }
  if (rc == LC_OK && apply) {
    logical_version = current->payload_span.present ? current->version : 0UL;
    has_query_hidden = current->has_query_hidden;
    query_hidden = current->query_hidden;
    if (stage->options->has_query_hidden) {
      has_query_hidden = 1;
      query_hidden = stage->options->query_hidden;
    }
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    if (current->descriptor != NULL) {
      old_binding_context = lc_pouch_state_crypto_context(
          &stage->pouch->allocator, stage->ns,
          staged.found ? staged_key : stage->key, current->version,
          lc_pouch_state_payload_class(current->record_type,
                                       current->object_link));
      new_binding_context = lc_pouch_state_crypto_context(
          &stage->pouch->allocator, stage->ns, staged_key, logical_version,
          lc_pouch_state_payload_class(current->record_type,
                                       current->object_link));
      if (old_binding_context == NULL || new_binding_context == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch staged binding context",
                          NULL, NULL, "pouch");
      } else {
        rc = lc_pouch_crypto_rebind_descriptor(
            stage->pouch->crypto, current->descriptor, old_binding_context,
            new_binding_context, &staged_descriptor, error);
      }
    }
  }
  if (rc == LC_OK && apply) {
    if (stage->options->query_index_operation_active &&
        !stage->options->object_record) {
      operation_expires_at_unix =
          stage->options->query_index_operation_expires_at_unix != NULL
              ? *stage->options->query_index_operation_expires_at_unix
              : 0L;
      rc = lc_pouch_query_index_operation_begin(
          stage->pouch, stage->ns, stage->key, stage->txn_id,
          operation_expires_at_unix, &query_index_guard_started, error);
    }
  }
  if (rc == LC_OK && apply) {
    rc = lc_pouch_state_append_record(
        stage->pouch, stage->ns, &manifest, LC_POUCH_STATE_RECORD_STATE_META,
        staged_key,
        current->content_type != NULL ? current->content_type
                                      : "application/octet-stream",
        current->etag != NULL ? current->etag : "", &current->payload_span,
        current->payload_context,
        stage->options->has_metadata ? stage->options->metadata
                                     : current->metadata,
        stage->options->has_metadata ? stage->options->metadata_length
                                     : current->metadata_length,
        logical_version, current->bytes, current->cipher_bytes,
        staged_descriptor, updated_at_unix, has_query_hidden, query_hidden,
        current->staged_delete_marker,
        stage->options->has_retention ? stage->options->retention
                                      : current->retention,
        0UL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_metadata_write_result_build(
          stage->pouch, &manifest, current, stage->options, logical_version,
          updated_at_unix, has_query_hidden, query_hidden, stage->out, error);
      if (rc == LC_OK && staged_descriptor != NULL) {
        lc_free_with_allocator(&stage->pouch->allocator,
                               stage->out->descriptor);
        stage->out->descriptor = lc_strdup_with_allocator(
            &stage->pouch->allocator, staged_descriptor);
        if (stage->out->descriptor == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy pouch staged metadata descriptor",
                            NULL, NULL, "pouch");
        }
      }
    }
    if (rc == LC_OK) {
      stage->out->query_index_operation_guard_started =
          query_index_guard_started;
      (void)lc_pouch_state_cache_apply_write(
          stage->pouch, stage->ns, &manifest, staged_key,
          current->content_type != NULL ? current->content_type
                                        : "application/octet-stream",
          current->etag != NULL ? current->etag : "", &current->payload_span,
          current->payload_context,
          stage->options->has_metadata ? stage->options->metadata
                                       : current->metadata,
          stage->options->has_metadata ? stage->options->metadata_length
                                       : current->metadata_length,
          logical_version, current->bytes, current->cipher_bytes,
          staged_descriptor, updated_at_unix, has_query_hidden, query_hidden,
          current->staged_delete_marker, 1, LC_POUCH_STATE_RECORD_STATE_META,
          stage->out->retention);
    }
  }
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&stage->pouch->allocator, stage->out);
  }
  lc_pouch_state_read_result_cleanup(&stage->pouch->allocator, &lease);
  lc_pouch_state_entry_cleanup(&stage->pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&stage->pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&stage->pouch->allocator, &manifest);
  lc_free_with_allocator(&stage->pouch->allocator, staged_key);
  lc_free_with_allocator(&stage->pouch->allocator, old_binding_context);
  lc_free_with_allocator(&stage->pouch->allocator, new_binding_context);
  lc_free_with_allocator(&stage->pouch->allocator, staged_descriptor);
  if (rc != LC_OK && query_index_guard_started) {
    lc_pouch_query_index_operation_cancel(stage->pouch, stage->ns, stage->key,
                                          stage->txn_id);
  }
  return rc;
}

int lc_pouch_state_stage_metadata_prepared(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *lease_key, lc_pouch_state_write_options *options,
    lc_pouch_state_stage_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_prepared_metadata_stage_context context;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      lease_key == NULL || lease_key[0] == '\0' || options == NULL ||
      prepare == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared metadata stage requires pouch, "
                        "namespace, key, transaction, lease key, options, "
                        "callback and output",
                        NULL, NULL, "pouch");
  }
  memset(&context, 0, sizeof(context));
  context.pouch = pouch;
  context.ns = ns;
  context.key = key;
  context.txn_id = txn_id;
  context.lease_key = lease_key;
  context.options = options;
  context.prepare = prepare;
  context.prepare_context = prepare_context;
  context.out = out;
  return lc_pouch_state_with_namespace_lock(
      pouch, ns, lc_pouch_state_stage_metadata_prepared_locked, &context,
      error);
}

static int lc_pouch_state_promote_staged_locked(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *expected_committed_etag, lc_pouch_state_write_result *out,
    int operation_active, lc_pouch_unix_seconds operation_expires_at_unix,
    lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_manifest cache_manifest;
  char *staged_key;
  char *committed_descriptor;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_generation version;
  lc_pouch_generation decision_version;
  lc_pouch_generation discard_version;
  lc_pouch_generation committed_index_seq;
  const unsigned char *promoted_metadata;
  size_t promoted_metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  unsigned char committed_record_type;
  int query_index_guard_started;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_promote_staged requires pouch, "
                        "namespace, key, txn_id and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&cache_manifest, 0, sizeof(cache_manifest));
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  committed_descriptor = NULL;
  committed_index_seq = 0UL;
  query_index_guard_started = 0;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &committed, NULL, error);
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (cache != NULL && cache->initialized) {
      rc = lc_pouch_state_entry_from_cache_record(
          pouch, lc_pouch_state_cache_record_find(cache, staged_key), &staged,
          error);
    } else {
      rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged, NULL,
                               error);
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!staged.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion source is missing", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  if (operation_active &&
      !lc_pouch_state_record_type_is_object(staged.record_type)) {
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, txn_id, operation_expires_at_unix,
        &query_index_guard_started, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  if (lc_pouch_state_is_staged_delete_marker(&staged)) {
    rc = lc_pouch_state_commit_staged_delete_locked(
        pouch, ns, &manifest, key, staged_key, &committed, &staged, out,
        operation_active, error);
    if (rc == LC_OK) {
      out->query_index_operation_guard_started = query_index_guard_started;
    }
    goto cleanup;
  }
  if (expected_committed_etag != NULL) {
    if (!committed.found ||
        strcmp(committed.etag, expected_committed_etag) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged promotion committed etag precondition "
                        "failed",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  } else if (committed.found && committed.payload_span.present) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion committed state already exists",
                      NULL, NULL, NULL);
    goto cleanup;
  }
  promoted_metadata = staged.metadata;
  promoted_metadata_length = staged.metadata_length;
  if (staged.record_type != LC_POUCH_STATE_RECORD_STATE_META &&
      promoted_metadata_length == 0U && committed.metadata_length > 0U) {
    promoted_metadata = committed.metadata;
    promoted_metadata_length = committed.metadata_length;
  }
  committed_record_type = staged.record_type == LC_POUCH_STATE_RECORD_STATE_META
                              ? LC_POUCH_STATE_RECORD_STATE_META
                              : LC_POUCH_STATE_RECORD_STATE_LINK;
  /* A staged metadata record may overlay a staged payload. Its version is
   * therefore the transaction's logical version, not the previously
   * committed document version. */
  version = staged.version;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_retention_commit(staged.retention, 0, &staged.retention,
                                       error);
  if (rc != LC_OK)
    goto cleanup;
  if (staged.retention > 0) {
    rc = lc_pouch_retention_hint_ensure(pouch, ns, error);
    if (rc != LC_OK)
      goto cleanup;
  }
  decision_version = staged.version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, ns, &manifest, key, staged_key, &staged, promoted_metadata,
      promoted_metadata_length, version, decision_version, discard_version,
      updated_at_unix, committed_record_type, &committed_descriptor,
      &committed_index_seq, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  cache_manifest = manifest;
  cache_manifest.state_max_version = committed_index_seq;
  (void)lc_pouch_state_cache_apply_write(
      pouch, ns, &cache_manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, promoted_metadata,
      promoted_metadata_length, version, staged.bytes, staged.cipher_bytes,
      committed_descriptor, updated_at_unix, staged.has_query_hidden,
      staged.query_hidden, 0, 1,
      committed_record_type == LC_POUCH_STATE_RECORD_STATE_META
          ? LC_POUCH_STATE_RECORD_STATE_META
      : lc_pouch_state_record_type_is_object(staged.record_type)
          ? LC_POUCH_STATE_RECORD_OBJECT_PUT
          : LC_POUCH_STATE_RECORD_STATE_PUT,
      staged.retention);
  (void)lc_pouch_state_cache_apply_write(
      pouch, ns, &manifest, staged_key, NULL, staged.etag, NULL, NULL, NULL, 0U,
      discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
      LC_POUCH_STATE_RECORD_STATE_DELETE, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged promotion etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  out->index_seq = committed_index_seq;
  out->version = version;
  out->bytes = staged.bytes;
  out->cipher_bytes = staged.cipher_bytes;
  out->descriptor = committed_descriptor;
  committed_descriptor = NULL;
  out->updated_at_unix = updated_at_unix;
  out->retention = staged.retention;
  out->has_body = staged.payload_span.present && !staged.staged_delete_marker;
  out->query_index_operation_guard_started = query_index_guard_started;
  if (pouch->query_indexing_enabled &&
      committed_record_type != LC_POUCH_STATE_RECORD_STATE_META &&
      !lc_pouch_state_record_type_is_object(staged.record_type)) {
    lc_pouch_query_index_note_state_write(
        pouch, ns, key,
        staged.content_type != NULL ? staged.content_type : "application/json",
        NULL, out, operation_active);
  }
cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  lc_free_with_allocator(&pouch->allocator, committed_descriptor);
  if (rc != LC_OK) {
    if (query_index_guard_started) {
      lc_pouch_query_index_operation_cancel(pouch, ns, key, txn_id);
    }
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

static int lc_pouch_state_promote_staged_with_operation(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *expected_committed_etag, lc_pouch_state_write_result *out,
    int operation_active, lc_pouch_unix_seconds operation_expires_at_unix,
    lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_promote_staged_locked(
        pouch, ns, key, txn_id, expected_committed_etag, out, operation_active,
        operation_expires_at_unix, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_promote_staged_locked(
      pouch, ns, key, txn_id, expected_committed_etag, out, operation_active,
      operation_expires_at_unix, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc != LC_OK && out->query_index_operation_guard_started) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key, txn_id);
    out->query_index_operation_guard_started = 0;
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 1);
  }
  return rc;
}

int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *ns,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error) {
  return lc_pouch_state_promote_staged_with_operation(
      pouch, ns, key, txn_id, expected_committed_etag, out, 0, 0L, error);
}

int lc_pouch_state_promote_staged_for_active_operation(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *expected_committed_etag,
    lc_pouch_unix_seconds operation_expires_at_unix,
    lc_pouch_state_write_result *out, lc_error *error) {
  return lc_pouch_state_promote_staged_with_operation(
      pouch, ns, key, txn_id, expected_committed_etag, out, 1,
      operation_expires_at_unix, error);
}

int lc_pouch_state_commit_staged_locked(lc_pouch *pouch, const char *ns,
                                        const char *key, const char *txn_id,
                                        lc_pouch_state_write_result *out,
                                        int operation_active,
                                        lc_pouch_unix_seconds commit_at_unix,
                                        lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_manifest cache_manifest;
  char *staged_key;
  char *committed_descriptor;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_generation version;
  lc_pouch_generation decision_version;
  lc_pouch_generation discard_version;
  lc_pouch_generation committed_index_seq;
  const unsigned char *promoted_metadata;
  size_t promoted_metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  unsigned char committed_record_type;
  int query_index_guard_started;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_commit_staged requires pouch, "
                        "namespace, key, txn_id and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&cache_manifest, 0, sizeof(cache_manifest));
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  committed_descriptor = NULL;
  committed_index_seq = 0UL;
  query_index_guard_started = 0;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &committed, NULL, error);
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (cache != NULL && cache->initialized) {
      rc = lc_pouch_state_entry_from_cache_record(
          pouch, lc_pouch_state_cache_record_find(cache, staged_key), &staged,
          error);
    } else {
      rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged, NULL,
                               error);
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!staged.found) {
    goto cleanup;
  }
  if (operation_active &&
      !lc_pouch_state_record_type_is_object(staged.record_type)) {
    rc = lc_pouch_query_index_operation_begin(
        pouch, ns, key, txn_id, 0L, &query_index_guard_started, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  if (lc_pouch_state_is_staged_delete_marker(&staged)) {
    rc = lc_pouch_state_commit_staged_delete_locked(
        pouch, ns, &manifest, key, staged_key, &committed, &staged, out,
        operation_active, error);
    if (rc == LC_OK) {
      out->query_index_operation_guard_started = query_index_guard_started;
    }
    goto cleanup;
  }
  promoted_metadata = staged.metadata;
  promoted_metadata_length = staged.metadata_length;
  if (staged.record_type != LC_POUCH_STATE_RECORD_STATE_META &&
      promoted_metadata_length == 0U && committed.metadata_length > 0U) {
    promoted_metadata = committed.metadata;
    promoted_metadata_length = committed.metadata_length;
  }

  committed_record_type = staged.record_type == LC_POUCH_STATE_RECORD_STATE_META
                              ? LC_POUCH_STATE_RECORD_STATE_META
                              : LC_POUCH_STATE_RECORD_STATE_LINK;
  /* A staged metadata record may overlay a staged payload. Its version is
   * therefore the transaction's logical version, not the previously
   * committed document version. */
  version = staged.version;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_retention_commit(staged.retention, commit_at_unix,
                                       &staged.retention, error);
  if (rc != LC_OK)
    goto cleanup;
  if (staged.retention > 0) {
    rc = lc_pouch_retention_hint_ensure(pouch, ns, error);
    if (rc != LC_OK)
      goto cleanup;
  }
  decision_version = staged.version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, ns, &manifest, key, staged_key, &staged, promoted_metadata,
      promoted_metadata_length, version, decision_version, discard_version,
      updated_at_unix, committed_record_type, &committed_descriptor,
      &committed_index_seq, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  cache_manifest = manifest;
  cache_manifest.state_max_version = committed_index_seq;
  (void)lc_pouch_state_cache_apply_write(
      pouch, ns, &cache_manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, promoted_metadata,
      promoted_metadata_length, version, staged.bytes, staged.cipher_bytes,
      committed_descriptor, updated_at_unix, staged.has_query_hidden,
      staged.query_hidden, 0, 1,
      committed_record_type == LC_POUCH_STATE_RECORD_STATE_META
          ? LC_POUCH_STATE_RECORD_STATE_META
      : lc_pouch_state_record_type_is_object(staged.record_type)
          ? LC_POUCH_STATE_RECORD_OBJECT_PUT
          : LC_POUCH_STATE_RECORD_STATE_PUT,
      staged.retention);
  (void)lc_pouch_state_cache_apply_write(
      pouch, ns, &manifest, staged_key, NULL, staged.etag, NULL, NULL, NULL, 0U,
      discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
      LC_POUCH_STATE_RECORD_STATE_DELETE, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged commit etag", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  out->index_seq = committed_index_seq;
  out->version = version;
  out->bytes = staged.bytes;
  out->cipher_bytes = staged.cipher_bytes;
  out->descriptor = committed_descriptor;
  committed_descriptor = NULL;
  out->updated_at_unix = updated_at_unix;
  out->retention = staged.retention;
  out->has_body = staged.payload_span.present && !staged.staged_delete_marker;
  out->has_query_hidden = staged.has_query_hidden;
  out->query_hidden = staged.query_hidden;
  out->query_index_operation_guard_started = query_index_guard_started;
  if (pouch->query_indexing_enabled &&
      committed_record_type != LC_POUCH_STATE_RECORD_STATE_META &&
      !lc_pouch_state_record_type_is_object(staged.record_type)) {
    lc_pouch_query_index_note_state_write(
        pouch, ns, key,
        staged.content_type != NULL ? staged.content_type : "application/json",
        NULL, out, operation_active);
  }
cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  lc_free_with_allocator(&pouch->allocator, committed_descriptor);
  if (rc != LC_OK) {
    if (query_index_guard_started) {
      lc_pouch_query_index_operation_cancel(pouch, ns, key, txn_id);
    }
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

static int lc_pouch_state_commit_staged_with_operation(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    lc_pouch_state_write_result *out, int operation_active, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_commit_staged_locked(pouch, ns, key, txn_id, out,
                                               operation_active, 0, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_commit_staged_locked(pouch, ns, key, txn_id, out,
                                           operation_active, 0, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc != LC_OK && out->query_index_operation_guard_started) {
    lc_pouch_query_index_operation_cancel(pouch, ns, key, txn_id);
    out->query_index_operation_guard_started = 0;
  }
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 1);
  }
  return rc;
}

int lc_pouch_state_commit_staged(lc_pouch *pouch, const char *ns,
                                 const char *key, const char *txn_id,
                                 lc_pouch_state_write_result *out,
                                 lc_error *error) {
  return lc_pouch_state_commit_staged_with_operation(pouch, ns, key, txn_id,
                                                     out, 0, error);
}

int lc_pouch_state_commit_staged_for_active_operation(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    lc_pouch_state_write_result *out, lc_error *error) {
  return lc_pouch_state_commit_staged_with_operation(pouch, ns, key, txn_id,
                                                     out, 1, error);
}

int lc_pouch_state_discard_staged_locked(lc_pouch *pouch, const char *ns,
                                         const char *key, const char *txn_id,
                                         int *discarded, lc_error *error) {
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  char *etag;
  lc_pouch_generation decision_version;
  lc_pouch_generation tombstone_version;
  lc_pouch_unix_seconds updated_at_unix;
  int rc;

  if (discarded != NULL) {
    *discarded = 0;
  }
  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_discard_staged requires pouch, "
                        "namespace, key and txn_id",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&staged, 0, sizeof(staged));
  etag = NULL;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, staged_key, &manifest, &staged, NULL, error);
  if (rc == LC_OK && staged.found) {
    decision_version = staged.version + 1UL;
    tombstone_version = decision_version + 1UL;
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
    if (etag == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_append_staged_discard_batch(
          pouch, ns, &manifest, staged_key, etag, decision_version,
          tombstone_version, updated_at_unix, error);
      if (rc == LC_OK) {
        (void)lc_pouch_state_cache_apply_write(
            pouch, ns, &manifest, staged_key, NULL, etag, NULL, NULL, NULL, 0U,
            tombstone_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0, 0,
            LC_POUCH_STATE_RECORD_STATE_DELETE, 0);
      }
      if (rc == LC_OK && discarded != NULL) {
        *discarded = 1;
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *ns,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_state_discard_staged_locked(pouch, ns, key, txn_id,
                                                discarded, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, ns, key, &lock, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_discard_staged_locked(pouch, ns, key, txn_id, discarded,
                                            error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 1);
  }
  return rc;
}

static int lc_pouch_state_read_internal(lc_pouch *pouch, const char *ns,
                                        const char *key, int include_body,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&cache_guard, 0, sizeof(cache_guard));
  /* Decision records are durable transaction state. Recover them before the
   * read mutex so a direct read observes the same committed view as scans and
   * mutations, without recursively acquiring the read coordination. */
  rc = lc_pouch_ensure_namespace(pouch, ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (include_body && !current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (include_body && lc_pouch_single_writer_enabled(pouch) && cache != NULL &&
      record != NULL) {
    char *payload_span_path;
    char *crypto_context;
    char *binding_context;

    payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest.namespace_path, &current.payload_span,
        error);
    crypto_context =
        payload_span_path != NULL
            ? lc_pouch_state_payload_context_for_read(
                  &pouch->allocator, ns, current.key, current.version,
                  lc_pouch_state_payload_class(current.record_type,
                                               current.object_link),
                  current.payload_context)
            : NULL;
    binding_context =
        payload_span_path != NULL
            ? lc_pouch_state_crypto_context(
                  &pouch->allocator, ns, current.key, current.version,
                  lc_pouch_state_payload_class(current.record_type,
                                               current.object_link))
            : NULL;
    if (payload_span_path == NULL || crypto_context == NULL ||
        binding_context == NULL) {
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      lc_free_with_allocator(&pouch->allocator, binding_context);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cached read context",
                        NULL, NULL, NULL);
    } else {
      memset(out, 0, sizeof(*out));
      rc = lc_pouch_state_read_many_snapshot_body_from_cache(
          pouch, cache, record, crypto_context, binding_context,
          payload_span_path, current.payload_span.payload_offset,
          current.payload_span.payload_length, &current, 0, &out->body, error);
      if (rc == LC_OK) {
        out->content_type = current.content_type;
        out->etag = current.etag;
        out->descriptor = current.descriptor;
        out->metadata = current.metadata;
        out->metadata_length = current.metadata_length;
        out->index_seq = current.index_seq;
        out->version = current.version;
        out->bytes = current.bytes;
        out->cipher_bytes = current.cipher_bytes;
        out->updated_at_unix = current.updated_at_unix;
        out->retention = current.retention;
        out->has_query_hidden = current.has_query_hidden;
        out->query_hidden = current.query_hidden;
        out->has_body = current.payload_span.present;
        out->staged_delete_marker = current.staged_delete_marker;
        out->found = 1;
        current.content_type = NULL;
        current.etag = NULL;
        current.descriptor = NULL;
        current.metadata = NULL;
        current.metadata_length = 0U;
      }
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      lc_free_with_allocator(&pouch->allocator, binding_context);
    }
  } else {
    rc = lc_pouch_state_read_result_from_entry(pouch, ns, &manifest, &current,
                                               include_body, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  if (rc == LC_OK) {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_bool_field("found", out != NULL ? out->found : 0);
    fields[4] =
        lc_log_u64_field("payload_bytes", out != NULL ? out->bytes : 0UL);
    fields[5] =
        lc_log_u64_field("stored_bytes", out != NULL ? out->cipher_bytes : 0UL);
    lc_log_trace(pouch->logger,
                 include_body ? "logstore.read" : "logstore.read.meta", fields,
                 6U);
  } else {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_error_field("error", error);
    fields[4] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.read.error", fields, 5U);
  }
  return rc;
}

int lc_pouch_state_read_metadata_locked(lc_pouch *pouch, const char *ns,
                                        const char *key,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_metadata_locked requires pouch, "
                        "namespace, key and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &current, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (rc == LC_OK && current.found) {
    rc = lc_pouch_state_read_result_from_entry(pouch, ns, &manifest, &current,
                                               0, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_read_locked(lc_pouch *pouch, const char *ns, const char *key,
                               lc_pouch_state_read_result *out,
                               lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_locked requires pouch, namespace, "
                        "key and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, ns, key, &manifest, &current, NULL, error);
  if (rc == LC_OK && current.found && current.payload_span.present) {
    rc = lc_pouch_state_read_result_from_entry(pouch, ns, &manifest, &current,
                                               1, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_read_metadata_view_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_state_metadata_view *out,
    lc_pouch_state_read_result *owned_fallback, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL || owned_fallback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata view requires pouch, namespace, "
                        "key, view and fallback",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(owned_fallback, 0, sizeof(*owned_fallback));
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    record = lc_pouch_state_cache_record_find(cache, key);
    if (record != NULL && record->found) {
      out->found = 1;
      out->etag = record->etag;
      out->content_type = record->content_type;
      out->version = record->version;
      out->metadata = record->metadata;
      out->metadata_length = record->metadata_length;
      out->has_query_hidden = record->has_query_hidden;
      out->query_hidden = record->query_hidden;
      out->has_body = record->payload_span.present;
      out->is_delete_marker = record->staged_delete_marker;
    }
    return LC_OK;
  }
  rc = lc_pouch_state_read_metadata_locked(pouch, ns, key, owned_fallback,
                                           error);
  if (rc == LC_OK && owned_fallback->found) {
    out->found = 1;
    out->etag = owned_fallback->etag;
    out->content_type = owned_fallback->content_type;
    out->version = owned_fallback->version;
    out->metadata = owned_fallback->metadata;
    out->metadata_length = owned_fallback->metadata_length;
    out->has_query_hidden = owned_fallback->has_query_hidden;
    out->query_hidden = owned_fallback->query_hidden;
    out->has_body = owned_fallback->has_body;
    out->is_delete_marker = owned_fallback->staged_delete_marker;
  }
  return rc;
}

int lc_pouch_state_read(lc_pouch *pouch, const char *ns, const char *key,
                        lc_pouch_state_read_result *out, lc_error *error) {
  return lc_pouch_state_read_internal(pouch, ns, key, 1, out, error);
}

int lc_pouch_state_copy(lc_pouch *pouch, const char *ns, const char *key,
                        lc_sink *dst, lc_pouch_state_read_result *out,
                        lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_guard cache_guard;
  lc_source *source;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_copy requires pouch, namespace, key, "
                        "sink, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  memset(&cache_guard, 0, sizeof(cache_guard));
  source = NULL;
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    record =
        cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
    if (record != NULL && record->found && record->body_cache != NULL &&
        record->body_cache->version == record->version &&
        record->body_cache->length == (size_t)record->bytes &&
        lc_pouch_state_body_cache_entry_ready(record->body_cache)) {
      rc = lc_pouch_state_read_result_from_cache_record(pouch, record, out,
                                                        error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_body_cache_source_open(
            &pouch->allocator, record->body_cache, &source, error);
      }
      goto cleanup_unlocked;
    }
  }
  rc = lc_pouch_state_manifest_lookup_cached(pouch, ns, key, &manifest,
                                             &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (!current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (cache != NULL && record != NULL && record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes &&
      lc_pouch_state_body_cache_entry_ready(record->body_cache)) {
    if (rc == LC_OK) {
      rc = lc_pouch_state_read_result_from_cache_record(pouch, record, out,
                                                        error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_body_cache_source_open(
          &pouch->allocator, record->body_cache, &source, error);
    }
  } else {
    rc = lc_pouch_state_read_result_from_entry(pouch, ns, &manifest, &current,
                                               1, out, error);
    if (rc == LC_OK && out->found) {
      source = out->body;
      out->body = NULL;
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  /* A returned source owns either a duplicated segment descriptor or a body
   * cache reference. Stream only after releasing namespace coordination so a
   * slow caller cannot serialize unrelated reads or projection snapshots. */
  if (rc == LC_OK && source != NULL && out->found && out->bytes > 0UL &&
      out->bytes <= (uint64_t)(size_t)-1) {
    rc = lc_sink_memory_reserve(dst, (size_t)out->bytes, error);
  }
  if (rc == LC_OK && source != NULL) {
    rc = lc_copy(source, dst, NULL, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  return rc;
}

int lc_pouch_state_read_metadata(lc_pouch *pouch, const char *ns,
                                 const char *key,
                                 lc_pouch_state_read_result *out,
                                 lc_error *error) {
  return lc_pouch_state_read_internal(pouch, ns, key, 0, out, error);
}

static int lc_pouch_state_read_many_internal(
    lc_pouch *pouch, const char *ns, const char *const *keys, size_t key_count,
    lc_pouch_state_read_many_fn visitor, void *context, int include_body,
    int copy_cached_body, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_read_many_snapshot *snapshots;
  size_t record_index_count;
  size_t index;
  size_t snapshot_count;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' ||
      (keys == NULL && key_count != 0U) || visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_many requires pouch, namespace, "
                        "keys, and visitor",
                        NULL, NULL, NULL);
  }
  snapshots = NULL;
  snapshot_count = 0U;
  record_index = NULL;
  record_index_count = 0U;
  if (key_count > 0U) {
    snapshots = (lc_pouch_state_read_many_snapshot *)lc_calloc_with_allocator(
        &pouch->allocator, key_count, sizeof(*snapshots));
    if (snapshots == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many snapshots", NULL,
                          NULL, NULL);
    }
  }
  memset(&cache_guard, 0, sizeof(cache_guard));
  rc = lc_pouch_ensure_namespace(pouch, ns, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                               snapshot_count);
    return rc;
  }
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                               snapshot_count);
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  if (rc == LC_OK && key_count > 8U) {
    rc = lc_pouch_state_cache_record_index_build(pouch, cache, &record_index,
                                                 &record_index_count, error);
  }
  for (index = 0U; rc == LC_OK && index < key_count; ++index) {
    lc_pouch_state_entry current;
    lc_pouch_state_cache_record *record;

    if (keys[index] == NULL || keys[index][0] == '\0') {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many requires non-empty keys", NULL, NULL,
                        NULL);
      break;
    }
    memset(&current, 0, sizeof(current));
    record = record_index != NULL
                 ? lc_pouch_state_cache_record_index_find(
                       record_index, record_index_count, keys[index])
                 : lc_pouch_state_cache_record_find(cache, keys[index]);
    rc = lc_pouch_state_entry_from_cache_record(pouch, record, &current, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_read_many_snapshot_from_entry(
          pouch, ns, &manifest, keys[index], cache, record, &current,
          include_body, copy_cached_body, &snapshots[index], error);
      if (rc == LC_OK) {
        snapshot_count = index + 1U;
      }
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_free_with_allocator(&pouch->allocator, record_index);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  if (rc == LC_OK) {
    for (index = 0U; rc == LC_OK && index < snapshot_count; ++index) {
      lc_pouch_state_read_result read_result;

      memset(&read_result, 0, sizeof(read_result));
      if (snapshots[index].found) {
        if (include_body) {
          read_result.body = snapshots[index].body;
          snapshots[index].body = NULL;
        }
        read_result.content_type = snapshots[index].content_type;
        read_result.etag = snapshots[index].etag;
        read_result.descriptor = snapshots[index].descriptor;
        read_result.metadata = snapshots[index].metadata;
        read_result.metadata_length = snapshots[index].metadata_length;
        read_result.index_seq = snapshots[index].index_seq;
        read_result.version = snapshots[index].version;
        read_result.bytes = snapshots[index].bytes;
        read_result.cipher_bytes = snapshots[index].cipher_bytes;
        read_result.updated_at_unix = snapshots[index].updated_at_unix;
        read_result.retention = snapshots[index].retention;
        read_result.has_query_hidden = snapshots[index].has_query_hidden;
        read_result.query_hidden = snapshots[index].query_hidden;
        read_result.found = 1;
        snapshots[index].content_type = NULL;
        snapshots[index].etag = NULL;
        snapshots[index].descriptor = NULL;
        snapshots[index].metadata = NULL;
        snapshots[index].metadata_length = 0U;
      }
      rc = visitor(snapshots[index].key, &read_result, context, error);
      lc_pouch_state_read_result_cleanup(&pouch->allocator, &read_result);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        break;
      }
    }
  }
  lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                             snapshot_count);
  if (rc == LC_OK) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("count", key_count);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_bool_field("copy_cached_body", copy_cached_body);
    lc_log_trace(pouch->logger, "logstore.read.many", fields, 4U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("count", key_count);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.read.many.error", fields, 4U);
  }
  return rc;
}

int lc_pouch_state_read_many(lc_pouch *pouch, const char *ns,
                             const char *const *keys, size_t key_count,
                             lc_pouch_state_read_many_fn visitor, void *context,
                             lc_error *error) {
  return lc_pouch_state_read_many_internal(pouch, ns, keys, key_count, visitor,
                                           context, 1, 1, error);
}

int lc_pouch_state_read_many_cached(lc_pouch *pouch, const char *ns,
                                    const char *const *keys, size_t key_count,
                                    lc_pouch_state_read_many_fn visitor,
                                    void *context, lc_error *error) {
  return lc_pouch_state_read_many_internal(pouch, ns, keys, key_count, visitor,
                                           context, 1, 0, error);
}

int lc_pouch_state_read_many_metadata(lc_pouch *pouch, const char *ns,
                                      const char *const *keys, size_t key_count,
                                      lc_pouch_state_read_many_fn visitor,
                                      void *context, lc_error *error) {
  return lc_pouch_state_read_many_internal(pouch, ns, keys, key_count, visitor,
                                           context, 0, 1, error);
}

int lc_pouch_state_scan_summary_read_body(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_state_scan_summary_entry *entry, int cache_completed_body,
    lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_state_scan_body_snapshot *snapshot;

  if (entry == NULL || entry->opaque == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan summary body read requires a summary row",
                        NULL, NULL, "pouch");
  }
  snapshot = (lc_pouch_state_scan_body_snapshot *)entry->opaque;
  return lc_pouch_state_scan_body_snapshot_open_result(
      pouch, ns, snapshot, cache_completed_body, out, error);
}

void lc_pouch_state_scan_summaries_result_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_summaries_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->next_start_after);
  memset(result, 0, sizeof(*result));
}

int lc_pouch_state_scan_summaries(lc_pouch *pouch, const char *ns,
                                  const char *start_after, size_t limit,
                                  lc_pouch_state_scan_summary_visit_fn visitor,
                                  void *context,
                                  lc_pouch_state_scan_summaries_result *out,
                                  lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_scan_body_snapshot *snapshots;
  size_t record_index_count;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t index;
  size_t next_index;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || visitor == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_scan_summaries requires pouch, "
                        "namespace, visitor, and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (limit == 0U) {
    limit = 2048U;
  }
  record_index = NULL;
  record_index_count = 0U;
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  memset(&cache_guard, 0, sizeof(cache_guard));
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_record_index_build(pouch, cache, &record_index,
                                                 &record_index_count, error);
  }
  index = rc == LC_OK ? lc_pouch_state_cache_record_index_start_after(
                            record_index, record_index_count, start_after)
                      : 0U;
  for (; rc == LC_OK && index < record_index_count && snapshot_count < limit;
       ++index) {
    lc_pouch_state_cache_record *record;

    record = record_index[index];
    if (record == NULL || !record->found) {
      continue;
    }
    rc = lc_pouch_state_scan_body_snapshot_append(
        pouch, &snapshots, &snapshot_count, &snapshot_capacity, &manifest,
        record, error);
  }
  if (rc == LC_OK && snapshot_count > 0U) {
    next_index = index;
    while (next_index < record_index_count &&
           (record_index[next_index] == NULL ||
            !record_index[next_index]->found)) {
      ++next_index;
    }
    if (next_index < record_index_count) {
      out->truncated = 1;
      out->next_start_after = lc_strdup_with_allocator(
          &pouch->allocator, snapshots[snapshot_count - 1U].key);
      if (out->next_start_after == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan cursor", NULL, NULL,
                          NULL);
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, record_index);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  for (index = 0U; rc == LC_OK && index < snapshot_count; ++index) {
    lc_pouch_state_scan_summary_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[index].key;
    entry.content_type = snapshots[index].content_type;
    entry.etag = snapshots[index].etag;
    entry.descriptor = snapshots[index].descriptor;
    entry.version = snapshots[index].version;
    entry.bytes = snapshots[index].bytes;
    entry.cipher_bytes = snapshots[index].cipher_bytes;
    entry.updated_at_unix = snapshots[index].updated_at_unix;
    entry.retention = snapshots[index].retention;
    entry.has_query_hidden = snapshots[index].has_query_hidden;
    entry.query_hidden = snapshots[index].query_hidden;
    entry.opaque = &snapshots[index];
    rc = visitor(&entry, context, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_state_scan_body_snapshots_cleanup(&pouch->allocator, snapshots,
                                             snapshot_count);
  if (rc != LC_OK) {
    lc_pouch_state_scan_summaries_result_cleanup(&pouch->allocator, out);
  }
  if (rc == LC_OK) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("cursor", start_after);
    fields[2] = lc_log_u64_field("limit", limit);
    fields[3] = lc_log_u64_field("count", snapshot_count);
    fields[4] =
        lc_log_bool_field("truncated", out != NULL ? out->truncated : 0);
    lc_log_trace(pouch->logger, "scan.summaries", fields, 5U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("limit", limit);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "scan.summaries.error", fields, 4U);
  }
  return rc;
}

static int lc_pouch_state_visit_internal(lc_pouch *pouch, const char *ns,
                                         const char *prefix,
                                         lc_pouch_state_visit_fn visitor,
                                         void *context, int locked,
                                         lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t prefix_length;
  size_t i;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit requires pouch, namespace, and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  memset(&cache_guard, 0, sizeof(cache_guard));
  if (locked) {
    rc = lc_pouch_state_ensure_namespace_locked(pouch, ns, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    rc = lc_pouch_ensure_namespace(pouch, ns, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  prefix_length = prefix != NULL ? strlen(prefix) : 0U;
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (!record->found ||
        (prefix != NULL && strncmp(record->key, prefix, prefix_length) != 0)) {
      continue;
    }
    rc = lc_pouch_state_visit_snapshot_append(
        &pouch->allocator, &snapshots, &snapshot_count, &snapshot_capacity,
        record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (!locked) {
    lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  }
  for (i = 0U; rc == LC_OK && i < snapshot_count; ++i) {
    lc_pouch_state_visit_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[i].key;
    entry.content_type = snapshots[i].content_type;
    entry.etag = snapshots[i].etag;
    entry.version = snapshots[i].version;
    entry.bytes = snapshots[i].bytes;
    entry.cipher_bytes = snapshots[i].cipher_bytes;
    entry.descriptor = snapshots[i].descriptor;
    entry.metadata = snapshots[i].metadata;
    entry.metadata_length = snapshots[i].metadata_length;
    entry.updated_at_unix = snapshots[i].updated_at_unix;
    entry.retention = snapshots[i].retention;
    entry.has_query_hidden = snapshots[i].has_query_hidden;
    entry.query_hidden = snapshots[i].query_hidden;
    entry.has_payload = snapshots[i].has_payload;
    entry.object_record =
        snapshots[i].record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
        snapshots[i].record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE;
    rc = visitor(&entry, context, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  if (rc == LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("count", snapshot_count);
    lc_log_trace(pouch->logger, "scan.visit", fields, 2U);
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "scan.visit.error", fields, 3U);
  }
  return rc;
}

int lc_pouch_state_visit(lc_pouch *pouch, const char *ns,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error) {
  return lc_pouch_state_visit_internal(pouch, ns, NULL, visitor, context, 0,
                                       error);
}

int lc_pouch_state_visit_prefix(lc_pouch *pouch, const char *ns,
                                const char *prefix,
                                lc_pouch_state_visit_fn visitor, void *context,
                                lc_error *error) {
  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit_prefix requires prefix", NULL,
                        NULL, NULL);
  }
  return lc_pouch_state_visit_internal(pouch, ns, prefix, visitor, context, 0,
                                       error);
}

int lc_pouch_state_visible_count(lc_pouch *pouch, const char *ns, size_t *count,
                                 lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_cache_guard cache_guard;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visible_count requires pouch, "
                        "namespace, and count",
                        NULL, NULL, NULL);
  }
  *count = 0U;
  memset(&cache_guard, 0, sizeof(cache_guard));
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  if (rc == LC_OK) {
    for (record = cache->records; record != NULL; record = record->next) {
      if (!record->found || !record->payload_span.present ||
          (record->has_query_hidden && record->query_hidden)) {
        continue;
      }
      if (strncmp(record->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
          strstr(record->key, "/.staging/") != NULL) {
        continue;
      }
      ++*count;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);

  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  return rc;
}

int lc_pouch_state_visit_since(lc_pouch *pouch, const char *ns,
                               lc_pouch_generation after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  lc_pouch_state_cache_guard cache_guard;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit_since requires pouch, "
                        "namespace, and visitor",
                        NULL, NULL, NULL);
  }
  memset(&cache_guard, 0, sizeof(cache_guard));
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  rc = lc_pouch_ensure_namespace(pouch, ns, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_manifest_view(pouch, ns, &manifest, &cache, error);
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (record->index_seq <= after_version) {
      continue;
    }
    rc = lc_pouch_state_visit_snapshot_append(
        &pouch->allocator, &snapshots, &snapshot_count, &snapshot_capacity,
        record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  for (i = 0U; rc == LC_OK && i < snapshot_count; ++i) {
    lc_pouch_state_change_visit_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[i].key;
    entry.content_type = snapshots[i].content_type;
    entry.etag = snapshots[i].etag;
    entry.version = snapshots[i].version;
    entry.bytes = snapshots[i].bytes;
    entry.cipher_bytes = snapshots[i].cipher_bytes;
    entry.descriptor = snapshots[i].descriptor;
    entry.updated_at_unix = snapshots[i].updated_at_unix;
    entry.retention = snapshots[i].retention;
    entry.has_query_hidden = snapshots[i].has_query_hidden;
    entry.query_hidden = snapshots[i].query_hidden;
    entry.has_payload = snapshots[i].has_payload;
    entry.object_record =
        snapshots[i].record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
        snapshots[i].record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE;
    entry.found = snapshots[i].found;
    rc = visitor(&entry, context, error);
  }
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  return rc;

cleanup:
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  return rc;
}

int lc_pouch_state_index_seq(lc_pouch *pouch, const char *ns,
                             lc_pouch_generation *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_guard cache_guard;
  lc_pouch_generation checkpoint;
  lc_pouch_generation checkpoint_query;
  uint64_t checkpoint_identity;
  uint64_t writer_mode_epoch;
  int checkpoint_valid;
  int checkpoint_query_valid;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_index_seq requires pouch, namespace, "
                        "and out",
                        NULL, NULL, NULL);
  }
  *out = 0UL;
  checkpoint = 0UL;
  checkpoint_query = 0UL;
  checkpoint_identity = 0U;
  checkpoint_valid = 0;
  checkpoint_query_valid = 0;
  memset(&cache_guard, 0, sizeof(cache_guard));
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch) {
    *out = cache->max_version;
    return LC_OK;
  }
  if (!single_writer) {
    rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, NULL, NULL, error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (single_writer) {
    if (cache != NULL && cache->initialized) {
      *out = cache->max_version > manifest.state_max_version
                 ? cache->max_version
                 : manifest.state_max_version;
    } else {
      rc = lc_pouch_state_clean_checkpoint_read(
          pouch, &manifest, &checkpoint_valid, &checkpoint_query_valid,
          &checkpoint, &checkpoint_query, &checkpoint_identity, error);
      if (rc == LC_OK && checkpoint_valid) {
        *out = checkpoint > manifest.state_max_version
                   ? checkpoint
                   : manifest.state_max_version;
      } else if (rc == LC_OK) {
        /* A manifest high-water is a cache of the resident projection. Without
         * a clean checkpoint it may lag a durable exclusive append, so only a
         * finalized-record scan is safe after abort or crash recovery. */
        rc = lc_pouch_state_manifest_max_version(pouch, &manifest, out, error);
      }
    }
  } else {
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc =
          lc_pouch_state_cache_refresh_for_mode(pouch, cache, &manifest, error);
      if (rc == LC_OK) {
        *out = cache->max_version;
      }
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  return rc;
}

int lc_pouch_state_query_index_seq(lc_pouch *pouch, const char *ns,
                                   lc_pouch_generation *out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_guard cache_guard;
  lc_pouch_generation checkpoint;
  lc_pouch_generation checkpoint_query;
  uint64_t checkpoint_identity;
  uint64_t writer_mode_epoch;
  int checkpoint_valid;
  int checkpoint_query_valid;
  int single_writer;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index sequence requires pouch, "
                        "namespace, and output",
                        NULL, NULL, "pouch");
  }
  *out = 0UL;
  memset(&cache_guard, 0, sizeof(cache_guard));
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  /* The exclusive owner updates this projection in the same mutation path
   * that changes public query candidates. Once it is initialized under the
   * current ownership epoch, its watermark is authoritative; reopening the
   * clean checkpoint on every query only repeats durable I/O. Shared Pouch
   * deliberately retains the recovery path below because another process may
   * have advanced the namespace. The resident fields are mutable projection
   * state, so snapshot them under the same namespace authority as writers. */
  if (single_writer) {
    rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
    if (rc != LC_OK) {
      return rc;
    }
    cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (cache != NULL && cache->initialized &&
        cache->writer_mode_epoch == writer_mode_epoch) {
      *out = cache->max_query_index_seq;
      lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
      return LC_OK;
    }
    lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  }
  /* An exclusive owner writes this checkpoint only after its append workers
   * have stopped and every state record is durable. Its public-query
   * watermark stays valid across lease-only and private staging writes, which
   * cannot change an indexed document. A public mutation invalidates it
   * before append, preserving crash recovery correctness. */
  if (single_writer) {
    checkpoint = 0UL;
    checkpoint_query = 0UL;
    checkpoint_identity = 0U;
    checkpoint_valid = 0;
    checkpoint_query_valid = 0;
    memset(&manifest, 0, sizeof(manifest));
    rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                          ns, error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                            ns, &manifest, NULL, NULL, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_clean_checkpoint_read(
          pouch, &manifest, &checkpoint_valid, &checkpoint_query_valid,
          &checkpoint, &checkpoint_query, &checkpoint_identity, error);
    }
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    if (rc != LC_OK) {
      return rc;
    }
    if (checkpoint_query_valid) {
      *out = checkpoint_query;
      return LC_OK;
    }
  }
  /* Query freshness excludes lease-only and internal-object mutations. That
   * distinction exists only in the state projection, so a first query after
   * reopening must warm it even on the exclusive fast path. */
  rc = lc_pouch_state_warm_namespace(pouch, ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_cache_guard_lock(pouch, ns, &cache_guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
  if (cache == NULL || !cache->initialized) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch query freshness projection is unavailable", NULL,
                      NULL, "pouch");
  } else {
    *out = cache->max_query_index_seq;
  }
  lc_pouch_state_cache_guard_unlock(pouch, &cache_guard);
  return rc;
}

void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->content_type);
  lc_free_with_allocator(allocator, result->etag);
  lc_free_with_allocator(allocator, result->descriptor);
  lc_free_with_allocator(allocator, result->metadata);
  if (result->body != NULL) {
    result->body->close(result->body);
  }
  memset(result, 0, sizeof(*result));
}

/* Collection records use this file's canonical state codec without creating
 * a current-state map or namespace runtime for every journal. */
#define LC_POUCH_COLLECTION_BINDING_MAGIC 0x4c434254UL
#define LC_POUCH_COLLECTION_BINDING_BYTES 20U
#define LC_POUCH_COLLECTION_META_BYTES 1024U
#define LC_POUCH_COLLECTION_STATE_BINDING_BYTES 32U
#define LC_POUCH_COLLECTION_PARENT_BINDING_BYTES 40U

static size_t lc_pouch_collection_state_binding(
    unsigned char *binding, uint64_t published_at, uint64_t previous_segment,
    uint64_t previous_offset, int has_parent, lc_pouch_unix_seconds parent) {
  size_t size = has_parent ? LC_POUCH_COLLECTION_PARENT_BINDING_BYTES
                           : LC_POUCH_COLLECTION_STATE_BINDING_BYTES;
  memset(binding, 0, size);
  lc_pouch_state_put32(binding, LC_POUCH_COLLECTION_BINDING_MAGIC);
  lc_pouch_state_put32(binding + 4U, has_parent ? 1U : 0U);
  lc_pouch_state_put64(binding + 8U, published_at);
  lc_pouch_state_put64(binding + 16U, previous_segment);
  lc_pouch_state_put64(binding + 24U, previous_offset);
  if (has_parent)
    lc_pouch_state_put64(binding + 32U, (uint64_t)parent);
  return size;
}

typedef struct lc_pouch_collection_body {
  lc_sink hash_sink;
  lc_sink *encoded;
  EVP_MD_CTX *hash;
} lc_pouch_collection_body;

static int lc_pouch_collection_hash_write(lc_sink *self, const void *bytes,
                                          size_t length, lc_error *error) {
  lc_pouch_collection_body *body;
  body = (lc_pouch_collection_body *)self;
  if (EVP_DigestUpdate(body->hash, bytes, length) != 1) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                 "failed to hash collection state payload", NULL, NULL,
                 "pouch");
    return 0;
  }
  return body->encoded == NULL ||
         body->encoded->write(body->encoded, bytes, length, error);
}

static int lc_pouch_collection_id_valid(const char *id) {
  size_t i;
  size_t length;
  if (id == NULL) {
    return 0;
  }
  length = strlen(id);
  if (length == 0U || length > 128U) {
    return 0;
  }
  for (i = 0U; i < length; ++i) {
    unsigned char c;
    c = (unsigned char)id[i];
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
          c == '~')) {
      return 0;
    }
  }
  return 1;
}

struct lc_pouch_collection_writer {
  lc_pouch *pouch;
  int fd;
  int started;
  int finished;
  lc_pouch_unix_seconds retention;
  lc_pouch_unix_seconds parent;
  char collection_id[LC_XID_STRING_SIZE];
  lc_pouch_collection_record record;
  lc_pouch_collection_body body;
  lc_collection_json_writer *json;
  lc_pouch_crypto_writer *codec;
  /* The PENDING reservation is reused when final metadata becomes known. */
  unsigned char *metadata;
};

void lc_pouch_collection_writer_kind(lc_pouch_collection_writer *writer,
                                     unsigned char kind) {
  writer->record.kind = kind;
}

void lc_pouch_collection_writer_close(lc_pouch_collection_writer *writer) {
  lc_pouch *pouch;
  if (writer == NULL)
    return;
  pouch = writer->pouch;
  lc_collection_json_writer_close(writer->json);
  lc_pouch_crypto_writer_close(writer->codec);
  EVP_MD_CTX_free(writer->body.hash);
  lc_free_with_allocator(&pouch->allocator, writer->metadata);
  if (writer->started && !writer->record.published)
    lc_pouch_state_append_fd_rollback(writer->fd, writer->record.record_offset,
                                      1);
  lc_free_with_allocator(&pouch->allocator, writer);
}

static int lc_pouch_collection_writer_start(lc_pouch_collection_writer *writer,
                                            const char *container_leaf,
                                            lc_error *error) {
  static const char placeholder[] =
      "0000000000000000000000000000000000000000000000000000000000000000";
  lc_pouch_collection_record *record = &writer->record;
  lc_pouch_state_payload_span span;
  unsigned char binding[LC_POUCH_COLLECTION_PARENT_BINDING_BYTES];
  lc_pouch_unix_seconds now;
  size_t binding_length, meta_length = 0U;
  int rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_fd_end(writer->fd, &record->record_offset,
                               "failed to seek collection segment tail", error);
  memset(&span, 0, sizeof(span));
  span.present = 1;
  span.container_leaf = (char *)container_leaf;
  span.record_offset = record->record_offset;
  record->payload_record_offset = record->record_offset;
  strcpy(record->payload_container, container_leaf);
  binding_length = lc_pouch_collection_state_binding(
      binding, record->generation, 0, 0, record->has_collection_retention, 0);
  if (rc == LC_OK)
    rc = lc_pouch_state_encode_payload_meta(
        &writer->pouch->allocator, record->position, now, 0, 0,
        "application/json", placeholder, NULL, &span, record->context, binding,
        binding_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, 0, 0, 0,
        writer->retention != 0, &writer->metadata, &meta_length, error);
  if (rc == LC_OK &&
      record->record_offset > (uint64_t)LC_I64_MAX -
                                  LC_POUCH_STATE_RECORD_HEADER_BYTES -
                                  strlen(record->append_id) - meta_length)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "collection record exceeds file offset range", NULL, NULL,
                      "pouch");
  if (rc == LC_OK) {
    record->metadata_length = meta_length;
    record->payload_offset = record->record_offset +
                             LC_POUCH_STATE_RECORD_HEADER_BYTES +
                             strlen(record->append_id) + meta_length;
    writer->started = 1;
    rc = lc_pouch_state_record_write_prefix(
        writer->fd, LC_POUCH_STATE_RECORD_STATE_PUT, record->append_id,
        strlen(record->append_id), writer->metadata, meta_length, 0, 0,
        LC_POUCH_RECORD_FLAG_PENDING, error);
  }
  return rc;
}

static int lc_pouch_collection_writer_open_output(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const char *collection_id, const char *append_id, uint64_t position,
    uint64_t generation, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_sink *destination, lc_pouch_collection_writer **out, lc_error *error) {
  lc_pouch_collection_writer *writer;
  lc_pouch_collection_record *record;
  char version[32];
  int n, rc;
  if (out != NULL)
    *out = NULL;
  if (pouch == NULL || out == NULL || fd < -1 ||
      !lc_pouch_collection_id_valid(append_id) ||
      !lc_xid_is_valid(collection_id) || retention == LC_I64_MIN ||
      (has_collection_retention && collection_retention == LC_I64_MIN) ||
      (fd >= 0 && !lc_pouch_state_payload_container_is_valid(container_leaf)))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection writer arguments are invalid", NULL, NULL,
                        "pouch");
  writer = lc_calloc_with_allocator(&pouch->allocator, 1U, sizeof(*writer));
  if (writer == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection record writer", NULL,
                        NULL, "pouch");
  writer->pouch = pouch;
  writer->fd = fd;
  writer->retention = retention;
  writer->parent = collection_retention;
  strcpy(writer->collection_id, collection_id);
  record = &writer->record;
  record->position = position;
  record->published_at = record->generation = generation;
  record->has_collection_retention = has_collection_retention != 0;
  strcpy(record->append_id, append_id);
  if (position == 0U)
    strcpy(version, "upload");
  else if (lc_u64_format_base10(position, version, sizeof(version)) < 0) {
    lc_pouch_collection_writer_close(writer);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection position could not be formatted", NULL,
                        NULL, "pouch");
  }
  n = snprintf(record->context, sizeof(record->context), "collection:%s:%s:%s",
               collection_id, append_id, version);
  rc = n < 0 || (size_t)n >= sizeof(record->context)
           ? lc_error_set(error, LC_ERR_INVALID, 0L,
                          "collection payload context exceeds its bound", NULL,
                          NULL, "pouch")
           : LC_OK;
  writer->body.hash_sink.write = lc_pouch_collection_hash_write;
  writer->body.hash = EVP_MD_CTX_new();
  if (rc == LC_OK && writer->body.hash == NULL)
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate collection payload hash", NULL, NULL,
                      "pouch");
  if (rc == LC_OK &&
      EVP_DigestInit_ex(writer->body.hash, EVP_sha256(), NULL) != 1)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to initialize collection payload hash", NULL,
                      NULL, "pouch");
  if (rc == LC_OK && destination != NULL)
    rc = lc_pouch_crypto_writer_open_sink(pouch->crypto, record->context,
                                          destination, &writer->codec, error);
  else if (rc == LC_OK && fd >= 0)
    rc = lc_pouch_crypto_writer_open(pouch->crypto, record->context, fd, 1,
                                     &writer->codec, error);
  writer->body.encoded = lc_pouch_crypto_writer_sink(writer->codec);
  if (rc == LC_OK)
    rc = lc_collection_json_writer_open(
        &writer->body.hash_sink, &pouch->allocator, &writer->json, error);
  if (rc == LC_OK && fd >= 0)
    rc = lc_pouch_collection_writer_start(writer, container_leaf, error);
  if (rc != LC_OK)
    lc_pouch_collection_writer_close(writer);
  else
    *out = writer;
  return rc;
}

int lc_pouch_collection_writer_open(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const char *collection_id, const char *append_id, uint64_t position,
    uint64_t generation, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_pouch_collection_writer **out, lc_error *error) {
  return lc_pouch_collection_writer_open_output(
      pouch, fd, container_leaf, collection_id, append_id, position, generation,
      retention, has_collection_retention, collection_retention, NULL, out,
      error);
}

int lc_pouch_collection_writer_open_staged(
    lc_pouch *pouch, lc_sink *destination, const char *collection_id,
    const char *append_id, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_pouch_collection_writer **out, lc_error *error) {
  return lc_pouch_collection_writer_open_output(
      pouch, -1, NULL, collection_id, append_id, 0, 0, retention,
      has_collection_retention, collection_retention, destination, out, error);
}

int lc_pouch_collection_writer_copy_staged(lc_pouch_collection_writer *writer,
                                           int fd, const char *leaf,
                                           lc_source *source, lc_error *error) {
  unsigned char *buffer;
  uint64_t remaining;
  unsigned long crc = (unsigned long)crc32(0L, Z_NULL, 0);
  size_t got, want;
  int rc;
  if (writer == NULL || !writer->finished || writer->started || fd < 0 ||
      source == NULL || source->read == NULL ||
      !lc_pouch_state_payload_container_is_valid(leaf))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection staged payload cannot be copied", NULL,
                        NULL, "pouch");
  buffer = lc_alloc_with_allocator(&writer->pouch->allocator,
                                   LC_COLLECTION_JSON_CHUNK_BYTES);
  if (buffer == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection payload copy buffer",
                        NULL, NULL, "pouch");
  writer->fd = fd;
  rc = lc_pouch_collection_writer_start(writer, leaf, error);
  remaining = writer->record.stored_bytes;
  while (rc == LC_OK && remaining != 0U) {
    want = remaining < LC_COLLECTION_JSON_CHUNK_BYTES
               ? (size_t)remaining
               : LC_COLLECTION_JSON_CHUNK_BYTES;
    got = source->read(source, buffer, want, error);
    if (error != NULL && error->code != LC_OK)
      rc = error->code;
    else if (got == 0U || got > want)
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "collection staged payload is incomplete", NULL, NULL,
                        "pouch");
    else {
      crc = (unsigned long)crc32((uLong)crc, buffer, (uInt)got);
      rc = lc_pouch_state_write_all(fd, buffer, got, error);
      remaining -= got;
    }
  }
  if (rc == LC_OK && crc != writer->record.stored_crc)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "collection staged payload checksum mismatch", NULL, NULL,
                      "pouch");
  lc_free_with_allocator(&writer->pouch->allocator, buffer);
  return rc;
}

int lc_pouch_collection_writer_write(lc_pouch_collection_writer *writer,
                                     const void *bytes, size_t count,
                                     lc_error *error) {
  if (writer == NULL || writer->finished)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection record writer is not writable", NULL, NULL,
                        "pouch");
  return lc_collection_json_writer_write(writer->json, bytes, count, error);
}

int lc_pouch_collection_writer_finish(lc_pouch_collection_writer *writer,
                                      lc_pouch_collection_record *out,
                                      lc_error *error) {
  char *etag = NULL, *descriptor = NULL;
  lc_u64 written;
  int rc;
  if (writer == NULL || out == NULL || writer->finished)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection record writer cannot finish", NULL, NULL,
                        "pouch");
  writer->finished = 1;
  rc = lc_collection_json_writer_finish(writer->json, &written, error);
  if (rc == LC_OK) {
    writer->record.bytes = written;
    etag = lc_pouch_state_hash_final(&writer->pouch->allocator,
                                     writer->body.hash, error);
    if (etag == NULL)
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (rc == LC_OK && writer->codec != NULL)
    rc = lc_pouch_crypto_writer_finish(
        writer->codec, &writer->record.bytes, &writer->record.stored_bytes,
        &writer->record.stored_crc, &descriptor, error);
  if (rc == LC_OK && descriptor != NULL &&
      strlen(descriptor) >= sizeof(writer->record.descriptor))
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "collection crypto descriptor exceeds reserved metadata",
                      NULL, NULL, "pouch");
  if (rc == LC_OK) {
    strcpy(writer->record.etag, etag);
    if (descriptor != NULL)
      strcpy(writer->record.descriptor, descriptor);
    *out = writer->record;
  }
  lc_free_with_allocator(&writer->pouch->allocator, etag);
  lc_free_with_allocator(&writer->pouch->allocator, descriptor);
  lc_collection_json_writer_close(writer->json);
  writer->json = NULL;
  lc_pouch_crypto_writer_close(writer->codec);
  writer->codec = NULL;
  EVP_MD_CTX_free(writer->body.hash);
  writer->body.hash = NULL;
  return rc;
}

int lc_pouch_collection_writer_publish(lc_pouch_collection_writer *writer,
                                       const char *container_leaf,
                                       uint64_t position, uint64_t generation,
                                       lc_pouch_collection_record *out,
                                       lc_error *error) {
  lc_pouch_collection_record *record;
  lc_pouch_state_payload_span span;
  unsigned char binding[LC_POUCH_COLLECTION_PARENT_BINDING_BYTES];
  unsigned char *meta = NULL;
  char version[32], context[256];
  char *descriptor = NULL;
  lc_pouch_unix_seconds now = 0;
  size_t binding_length, meta_length;
  int n, rc;
  if (writer == NULL || out == NULL || !writer->finished || !writer->started ||
      writer->fd < 0 || writer->record.etag[0] == '\0' ||
      writer->record.published || position == 0U || generation == 0U ||
      !lc_pouch_state_payload_container_is_valid(container_leaf))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection record writer cannot publish", NULL, NULL,
                        "pouch");
  record = &writer->record;
  if (lc_u64_format_base10(position, version, sizeof(version)) < 0)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection position could not be formatted", NULL,
                        NULL, "pouch");
  n = snprintf(context, sizeof(context), "collection:%s:%s:%s",
               writer->collection_id, record->append_id, version);
  if (n < 0 || (size_t)n >= sizeof(context))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection binding context exceeds its bound", NULL,
                        NULL, "pouch");
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_resolve(writer->retention, now, &record->retention,
                                    error);
  if (rc == LC_OK && record->has_collection_retention)
    rc = lc_pouch_retention_resolve(writer->parent, now,
                                    &record->collection_retention, error);
  if (rc == LC_OK && record->descriptor[0] != '\0' &&
      strcmp(context, record->context) != 0) {
    rc = lc_pouch_crypto_rebind_descriptor(writer->pouch->crypto,
                                           record->descriptor, record->context,
                                           context, &descriptor, error);
    if (rc == LC_OK && strlen(descriptor) >= sizeof(record->descriptor))
      rc = lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "collection binding descriptor exceeds reserved metadata", NULL, NULL,
          "pouch");
    if (rc == LC_OK)
      strcpy(record->descriptor, descriptor);
  }
  record->position = position;
  record->published_at = record->generation = generation;
  record->updated_at_unix = now;
  strcpy(record->payload_container, container_leaf);
  memset(&span, 0, sizeof(span));
  span.present = 1;
  span.container_leaf = (char *)container_leaf;
  span.record_offset = record->record_offset;
  span.payload_offset = record->payload_offset;
  span.payload_length = record->stored_bytes;
  span.payload_crc = record->stored_crc;
  binding_length = lc_pouch_collection_state_binding(
      binding, generation, 0, 0, record->has_collection_retention,
      record->collection_retention);
  meta_length = 0U;
  if (rc == LC_OK)
    rc = lc_pouch_state_encode_payload_meta_into(
        &writer->pouch->allocator, position, now, record->bytes,
        record->stored_bytes, "application/json", record->etag,
        record->descriptor[0] != '\0' ? record->descriptor : NULL, &span,
        record->context, binding, binding_length,
        LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, 0, 0, 0, record->retention,
        writer->metadata, record->metadata_length, &meta, &meta_length, error);
  if (rc == LC_OK && meta_length != record->metadata_length)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "collection state metadata reservation changed size",
                      NULL, "collection_corrupt", "pouch");
  if (rc == LC_OK) {
    rc =
        lc_pouch_state_meta_set_index_seq(meta, meta_length, generation, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_record_finalize(
          writer->fd, record->record_offset,
          record->kind ? record->kind : LC_POUCH_STATE_RECORD_STATE_PUT,
          record->append_id, strlen(record->append_id), meta, meta_length,
          record->stored_bytes, record->stored_crc, error);
    record->published = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_pouch_state_fd_end(writer->fd, &record->next_offset,
                               "failed to restore collection append offset",
                               error);
  *out = *record;
  lc_free_with_allocator(&writer->pouch->allocator, descriptor);
  return rc;
}

int lc_pouch_collection_writer_consume(lc_pouch_collection_writer *writer,
                                       lc_pouch *pouch, lc_source *source,
                                       lc_pouch_collection_record *out,
                                       lc_error *error) {
  unsigned char *input = NULL;
  size_t got;
  int rc = LC_OK;
  {
    input = lc_alloc_with_allocator(&pouch->allocator,
                                    LC_COLLECTION_JSON_CHUNK_BYTES);
    if (input == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection input chunk", NULL, NULL,
                        "pouch");
  }
  while (rc == LC_OK) {
    got = source->read(source, input, LC_COLLECTION_JSON_CHUNK_BYTES, error);
    if (error != NULL && error->code != LC_OK) {
      rc = error->code;
      break;
    }
    if (got > LC_COLLECTION_JSON_CHUNK_BYTES) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection source exceeded requested chunk size", NULL,
                        NULL, "pouch");
      break;
    }
    if (got == 0U) {
      rc = lc_pouch_collection_writer_finish(writer, out, error);
      break;
    }
    rc = lc_pouch_collection_writer_write(writer, input, got, error);
  }
  lc_free_with_allocator(&pouch->allocator, input);
  return rc;
}

int lc_pouch_collection_record_append(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const char *collection_id, const char *append_id, uint64_t position,
    uint64_t generation, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_source *source, lc_pouch_collection_record *out, lc_error *error) {
  lc_pouch_collection_writer *writer = NULL;
  int rc;
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (pouch == NULL || source == NULL || source->read == NULL || out == NULL ||
      fd < 0 || position == 0U || generation == 0U)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection state append arguments are invalid", NULL,
                        NULL, "pouch");
  rc = lc_pouch_collection_writer_open(pouch, fd, container_leaf, collection_id,
                                       append_id, position, generation,
                                       retention, has_collection_retention,
                                       collection_retention, &writer, error);
  if (rc == LC_OK)
    rc = lc_pouch_collection_writer_consume(writer, pouch, source, out, error);
  if (rc == LC_OK)
    rc = lc_pouch_collection_writer_publish(writer, container_leaf, position,
                                            generation, out, error);
  lc_pouch_collection_writer_close(writer);
  return rc;
}

int lc_pouch_collection_record_metadata(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const lc_pouch_collection_record *current, uint64_t previous_segment,
    uint64_t generation, lc_pouch_unix_seconds retention,
    lc_pouch_collection_record *out, lc_error *error) {
  lc_pouch_state_payload_span span;
  unsigned char binding[LC_POUCH_COLLECTION_PARENT_BINDING_BYTES];
  unsigned char *meta;
  lc_pouch_unix_seconds now;
  uint64_t start;
  size_t length;
  size_t binding_length;
  int rc, started;
  if (pouch == NULL || fd < 0 || current == NULL || out == NULL ||
      current == out || !current->published || current->deleted ||
      current->position == 0U || previous_segment == 0U ||
      generation <= current->generation || retention == LC_I64_MIN ||
      !lc_pouch_collection_id_valid(current->append_id) ||
      !lc_pouch_state_payload_container_is_valid(container_leaf) ||
      !lc_pouch_state_payload_container_is_valid(current->payload_container))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection state metadata arguments are invalid", NULL,
                        NULL, "pouch");
  *out = *current;
  out->published = 0;
  out->metadata_only = 1;
  out->generation = generation;
  out->previous_segment = previous_segment;
  out->previous_offset = current->record_offset;
  out->retention = retention;
  memset(&span, 0, sizeof(span));
  span.present = 1;
  span.container_leaf = (char *)current->payload_container;
  span.record_offset = current->payload_record_offset;
  span.payload_offset = current->payload_offset;
  span.payload_length = current->stored_bytes;
  span.payload_crc = current->stored_crc;
  binding_length = lc_pouch_collection_state_binding(
      binding, current->published_at, previous_segment, current->record_offset,
      current->has_collection_retention, current->collection_retention);
  meta = NULL;
  length = 0U;
  start = 0U;
  started = 0;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_resolve(retention, now, &out->retention, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, current->position, now, current->bytes,
        current->stored_bytes, "application/json", current->etag,
        current->descriptor, &span, current->context, binding, binding_length,
        0U, 0, 0, 0, out->retention, &meta, &length, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_meta_set_index_seq(meta, length, generation, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_fd_end(
        fd, &start, "failed to seek collection metadata tail", error);
  if (rc == LC_OK) {
    out->record_offset = start;
    out->metadata_length = length;
    out->updated_at_unix = now;
    started = 1;
    rc = lc_pouch_state_record_write_prefix(
        fd, LC_POUCH_STATE_RECORD_STATE_META, current->append_id,
        strlen(current->append_id), meta, length, 0U, 0U,
        LC_POUCH_RECORD_FLAG_PENDING, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_finalize(
        fd, start, LC_POUCH_STATE_RECORD_STATE_META, current->append_id,
        strlen(current->append_id), meta, length, 0U, 0U, error);
    out->published = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_pouch_state_fd_end(fd, &out->next_offset,
                               "failed to restore collection metadata offset",
                               error);
  if (rc != LC_OK && started && !out->published)
    lc_pouch_state_append_fd_rollback(fd, start, 1);
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

int lc_pouch_collection_record_delete(lc_pouch *pouch, int fd,
                                      const lc_pouch_collection_record *current,
                                      uint64_t generation,
                                      lc_pouch_collection_record *out,
                                      lc_error *error) {
  unsigned char *meta;
  unsigned char *grown;
  lc_pouch_unix_seconds now;
  uint64_t start;
  size_t length;
  size_t trailer;
  size_t binding_length;
  int rc;
  int started;
  if (pouch == NULL || fd < 0 || current == NULL || out == NULL ||
      !current->published || current->position == 0U ||
      !lc_pouch_collection_id_valid(current->append_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection state deletion arguments are invalid", NULL,
                        NULL, "pouch");
  }
  if (current->deleted) {
    *out = *current;
    return LC_OK;
  }
  if (generation <= current->generation) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection deletion generation is not increasing",
                        NULL, NULL, "pouch");
  }
  *out = *current;
  out->published = 0;
  out->deleted = 1;
  out->generation = generation;
  out->metadata_only = 0;
  out->previous_segment = 0U;
  out->previous_offset = 0U;
  out->payload_record_offset = 0U;
  out->payload_container[0] = '\0';
  out->retention = 0;
  out->stored_bytes = 0U;
  out->payload_offset = 0U;
  out->stored_crc = 0U;
  out->descriptor[0] = '\0';
  out->context[0] = '\0';
  meta = NULL;
  length = 0U;
  started = 0;
  start = 0U;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_fd_end(
        fd, &start, "failed to seek collection deletion tail", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, current->position,
                                           now, current->etag, &meta, &length,
                                           error);
  }
  if (rc == LC_OK) {
    binding_length = LC_POUCH_COLLECTION_BINDING_BYTES +
                     (current->has_collection_retention ? 8U : 0U);
    grown = (unsigned char *)lc_realloc_with_allocator(&pouch->allocator, meta,
                                                       length + binding_length);
    if (grown == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain collection deletion binding", NULL,
                        NULL, "pouch");
    } else {
      meta = grown;
      trailer = length - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
      memmove(meta + trailer + binding_length, meta + trailer,
              LC_POUCH_STATE_INDEX_TRAILER_BYTES);
      lc_pouch_state_put32(meta + trailer, LC_POUCH_COLLECTION_BINDING_MAGIC);
      lc_pouch_state_put64(meta + trailer + 4U, current->bytes);
      lc_pouch_state_put64(meta + trailer + 12U, current->published_at);
      if (current->has_collection_retention)
        lc_pouch_state_put64(meta + trailer + 20U,
                             (uint64_t)current->collection_retention);
      length += binding_length;
      out->metadata_length = length;
      rc = lc_pouch_state_meta_set_index_seq(meta, length, generation, error);
    }
  }
  if (rc == LC_OK) {
    out->record_offset = start;
    out->updated_at_unix = now;
    started = 1;
    rc = lc_pouch_state_record_write_prefix(
        fd, LC_POUCH_STATE_RECORD_STATE_DELETE, current->append_id,
        strlen(current->append_id), meta, length, 0U, 0U,
        LC_POUCH_RECORD_FLAG_PENDING, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_finalize(
        fd, start, LC_POUCH_STATE_RECORD_STATE_DELETE, current->append_id,
        strlen(current->append_id), meta, length, 0U, 0U, error);
    out->published = rc == LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_fd_end(fd, &out->next_offset,
                               "failed to restore collection deletion offset",
                               error);
  }
  if (rc != LC_OK && started && !out->published) {
    lc_pouch_state_append_fd_rollback(fd, start, 1);
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_collection_record_pread(int fd, uint64_t offset,
                                            unsigned char *bytes, size_t length,
                                            lc_error *error) {
  size_t done;
  ssize_t got;
  if (offset > (uint64_t)LC_I64_MAX || length > (uint64_t)LC_I64_MAX - offset) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "collection record offset exceeds file range", NULL,
                        "collection_corrupt", "pouch");
  }
  done = 0U;
  while (done < length) {
    got = pread(fd, bytes + done, length - done, (off_t)(offset + done));
    if (got < 0 && errno == EINTR) {
      continue;
    }
    if (got <= 0) {
      return lc_error_set(error, got == 0 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
                          0L, "failed to read collection state bytes",
                          got < 0 ? strerror(errno) : NULL,
                          got == 0 ? "collection_corrupt" : NULL, "pouch");
    }
    done += (size_t)got;
  }
  return LC_OK;
}

static int lc_pouch_collection_record_text(char *out, size_t capacity,
                                           const unsigned char *bytes,
                                           size_t length) {
  if (length >= capacity || memchr(bytes, '\0', length) != NULL) {
    return 0;
  }
  memcpy(out, bytes, length);
  out[length] = '\0';
  return 1;
}

int lc_pouch_collection_record_read(int fd, uint64_t offset, uint64_t file_end,
                                    const char *container_leaf,
                                    lc_pouch_collection_record *out,
                                    lc_error *error) {
  unsigned char prefix[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  unsigned char bytes[128U + LC_POUCH_COLLECTION_META_BYTES];
  const unsigned char *meta;
  const unsigned char *cursor;
  lc_pouch_record_header header;
  size_t content_type_length;
  size_t etag_length;
  size_t descriptor_length;
  size_t container_length;
  size_t context_length;
  size_t used;
  size_t binding_length;
  size_t i;
  uint64_t metadata_end;
  int rc;
  if (fd < 0 || out == NULL ||
      !lc_pouch_state_payload_container_is_valid(container_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection record read arguments are invalid", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (offset > file_end || file_end - offset < sizeof(prefix)) {
    goto corrupt;
  }
  rc = lc_pouch_collection_record_pread(fd, offset, prefix, sizeof(prefix),
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_record_header_decode(prefix, &header, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (header.flags == LC_POUCH_RECORD_FLAG_PENDING) {
    return lc_error_set(error, LC_ERR_PENDING, 0L,
                        "collection record is not published", NULL, NULL,
                        "pouch");
  }
  if (header.flags != 0U || header.key_len == 0U || header.key_len > 128U ||
      header.meta_len < 18U + LC_POUCH_STATE_INDEX_TRAILER_BYTES ||
      header.meta_len > LC_POUCH_COLLECTION_META_BYTES ||
      (header.type != LC_POUCH_STATE_RECORD_STATE_PUT &&
       header.type != LC_POUCH_STATE_RECORD_STATE_META &&
       header.type != LC_POUCH_STATE_RECORD_STATE_DELETE &&
       header.type != LC_POUCH_COLLECTION_EDIT_BODY &&
       header.type != LC_POUCH_COLLECTION_EDIT)) {
    goto corrupt;
  }
  if (file_end - offset - sizeof(prefix) < header.key_len + header.meta_len) {
    goto corrupt;
  }
  metadata_end = offset + sizeof(prefix) + header.key_len + header.meta_len;
  if (header.payload_len > file_end - metadata_end) {
    goto corrupt;
  }
  rc =
      lc_pouch_collection_record_pread(fd, offset + sizeof(prefix), bytes,
                                       header.key_len + header.meta_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_collection_record_text(out->append_id, sizeof(out->append_id),
                                       bytes, header.key_len) ||
      !lc_pouch_collection_id_valid(out->append_id)) {
    goto corrupt;
  }
  meta = bytes + header.key_len;
  out->position = lc_pouch_state_get64(meta);
  out->updated_at_unix =
      lc_pouch_state_decode_unix_seconds(lc_pouch_state_get64(meta + 8U));
  out->kind = header.type;
  out->metadata_length = header.meta_len;
  out->record_offset = offset;
  out->next_offset = metadata_end + header.payload_len;
  rc = lc_pouch_state_meta_index_seq(meta, header.meta_len, &out->generation,
                                     error);
  if (rc != LC_OK) {
    return rc;
  }
  if (out->position == 0U || out->generation == 0U) {
    goto corrupt;
  }
  if (header.type == LC_POUCH_STATE_RECORD_STATE_DELETE) {
    etag_length = lc_pouch_state_get16(meta + 16U);
    binding_length = header.meta_len - 18U - etag_length -
                     LC_POUCH_STATE_INDEX_TRAILER_BYTES;
    if (etag_length != 64U || header.payload_len != 0U ||
        header.payload_crc != 0U ||
        (binding_length != LC_POUCH_COLLECTION_BINDING_BYTES &&
         binding_length != LC_POUCH_COLLECTION_BINDING_BYTES + 8U)) {
      goto corrupt;
    }
    cursor = meta + 18U + etag_length;
    if (lc_pouch_state_get32(cursor) != LC_POUCH_COLLECTION_BINDING_MAGIC ||
        !lc_pouch_collection_record_text(out->etag, sizeof(out->etag),
                                         meta + 18U, etag_length)) {
      goto corrupt;
    }
    out->bytes = lc_pouch_state_get64(cursor + 4U);
    out->published_at = lc_pouch_state_get64(cursor + 12U);
    out->has_collection_retention =
        binding_length > LC_POUCH_COLLECTION_BINDING_BYTES;
    if (out->has_collection_retention) {
      if (lc_pouch_state_get64(cursor + 20U) > (uint64_t)LC_I64_MAX)
        goto corrupt;
      out->collection_retention =
          (lc_pouch_unix_seconds)lc_pouch_state_get64(cursor + 20U);
    }
    if (out->published_at == 0U || out->published_at >= out->generation) {
      goto corrupt;
    }
    out->deleted = 1;
  } else {
    if (header.meta_len < LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES ||
        (meta[60] & ~LC_POUCH_STATE_PAYLOAD_FLAG_RETENTION) != 0U ||
        meta[61] != 0U) {
      goto corrupt;
    }
    content_type_length = lc_pouch_state_get16(meta + 62U);
    etag_length = lc_pouch_state_get16(meta + 64U);
    descriptor_length = lc_pouch_state_get32(meta + 66U);
    container_length = lc_pouch_state_get16(meta + 70U);
    context_length = lc_pouch_state_get16(meta + 72U);
    used = LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_length +
           etag_length + descriptor_length + container_length + context_length;
    if (content_type_length != sizeof("application/json") - 1U ||
        etag_length != 64U || descriptor_length >= sizeof(out->descriptor) ||
        context_length == 0U || context_length >= sizeof(out->context) ||
        container_length == 0U ||
        container_length >= sizeof(out->payload_container) ||
        used + 4U + LC_POUCH_COLLECTION_STATE_BINDING_BYTES +
                LC_POUCH_STATE_INDEX_TRAILER_BYTES >
            header.meta_len ||
        lc_pouch_state_get64(meta + 48U) != lc_pouch_state_get64(meta + 24U)) {
      goto corrupt;
    }
    cursor = meta + LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES;
    if (memcmp(cursor, "application/json", content_type_length) != 0) {
      goto corrupt;
    }
    cursor += content_type_length;
    if (!lc_pouch_collection_record_text(out->etag, sizeof(out->etag), cursor,
                                         etag_length)) {
      goto corrupt;
    }
    cursor += etag_length;
    if (!lc_pouch_collection_record_text(out->descriptor,
                                         sizeof(out->descriptor), cursor,
                                         descriptor_length)) {
      goto corrupt;
    }
    cursor += descriptor_length;
    if (!lc_pouch_collection_record_text(out->payload_container,
                                         sizeof(out->payload_container), cursor,
                                         container_length) ||
        !lc_pouch_state_payload_container_is_valid(out->payload_container)) {
      goto corrupt;
    }
    cursor += container_length;
    binding_length = lc_pouch_state_get32(meta + used);
    if (!lc_pouch_collection_record_text(out->context, sizeof(out->context),
                                         cursor, context_length) ||
        (binding_length != LC_POUCH_COLLECTION_STATE_BINDING_BYTES &&
         binding_length != LC_POUCH_COLLECTION_PARENT_BINDING_BYTES) ||
        used + 4U + binding_length + LC_POUCH_STATE_INDEX_TRAILER_BYTES >
            header.meta_len) {
      goto corrupt;
    }
    cursor = meta + used + 4U;
    if (lc_pouch_state_get32(cursor) != LC_POUCH_COLLECTION_BINDING_MAGIC ||
        lc_pouch_state_get32(cursor + 4U) !=
            (binding_length == LC_POUCH_COLLECTION_PARENT_BINDING_BYTES ? 1U
                                                                        : 0U))
      goto corrupt;
    out->published_at = lc_pouch_state_get64(cursor + 8U);
    out->previous_segment = lc_pouch_state_get64(cursor + 16U);
    out->previous_offset = lc_pouch_state_get64(cursor + 24U);
    out->has_collection_retention =
        binding_length == LC_POUCH_COLLECTION_PARENT_BINDING_BYTES;
    if (out->has_collection_retention) {
      if (lc_pouch_state_get64(cursor + 32U) > (uint64_t)LC_I64_MAX)
        goto corrupt;
      out->collection_retention =
          (lc_pouch_unix_seconds)lc_pouch_state_get64(cursor + 32U);
    }
    if (out->published_at == 0U || out->published_at > out->generation)
      goto corrupt;
    used += 4U + binding_length;
    rc = lc_pouch_state_decode_retention_meta(meta, header.meta_len, used, 0,
                                              &out->retention, error);
    if (rc != LC_OK)
      goto corrupt;
    if ((meta[60] & LC_POUCH_STATE_PAYLOAD_FLAG_RETENTION) != 0U)
      used += 8U;
    for (i = used; i < header.meta_len - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
         ++i) {
      if (meta[i] != 0U) {
        goto corrupt;
      }
    }
    out->bytes = lc_pouch_state_get64(meta + 16U);
    out->payload_record_offset = lc_pouch_state_get64(meta + 32U);
    out->payload_offset = lc_pouch_state_get64(meta + 40U);
    out->stored_bytes = lc_pouch_state_get64(meta + 24U);
    out->stored_crc = lc_pouch_state_get32(meta + 56U);
    out->metadata_only = header.type == LC_POUCH_STATE_RECORD_STATE_META;
    if (out->metadata_only) {
      if (header.payload_len != 0U || header.payload_crc != 0U ||
          out->previous_segment == 0U || out->generation <= out->published_at ||
          out->payload_record_offset >= out->payload_offset ||
          out->payload_offset > (uint64_t)LC_I64_MAX ||
          out->stored_bytes > (uint64_t)LC_I64_MAX - out->payload_offset)
        goto corrupt;
    } else if (strcmp(out->payload_container, container_leaf) != 0 ||
               out->previous_segment != 0U || out->previous_offset != 0U ||
               out->payload_record_offset != offset ||
               out->payload_offset != metadata_end ||
               out->stored_bytes != header.payload_len ||
               out->stored_crc != header.payload_crc)
      goto corrupt;
  }
  for (i = 0U; i < 64U; ++i) {
    if (!((out->etag[i] >= '0' && out->etag[i] <= '9') ||
          (out->etag[i] >= 'a' && out->etag[i] <= 'f'))) {
      goto corrupt;
    }
  }
  if (out->bytes < 2U) {
    goto corrupt;
  }
  out->published = 1;
  return LC_OK;
corrupt:
  memset(out, 0, sizeof(*out));
  return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "collection canonical state record is inconsistent", NULL,
                      "collection_corrupt", "pouch");
}

int lc_pouch_collection_record_copy(
    lc_pouch *pouch, int source_fd, int payload_fd, const char *source_leaf,
    const lc_pouch_collection_record *current, int destination_fd,
    const char *destination_leaf, unsigned char *buffer, size_t capacity,
    lc_pouch_collection_record *out, lc_error *error) {
  lc_pouch_collection_record source;
  lc_pouch_collection_record body;
  struct stat body_stat;
  unsigned char meta[LC_POUCH_COLLECTION_META_BYTES];
  uint64_t start;
  uint64_t metadata_offset;
  uint64_t remaining;
  uint64_t offset;
  size_t meta_length;
  size_t container_offset;
  size_t old_length;
  size_t new_length;
  size_t want;
  unsigned long crc;
  unsigned char type;
  int rc;
  if (pouch == NULL || source_fd < 0 || destination_fd < 0 ||
      source_fd == destination_fd || current == NULL || out == NULL ||
      current == out || !current->published || buffer == NULL ||
      capacity == 0U ||
      (!current->deleted && (payload_fd < 0 || payload_fd == destination_fd)) ||
      !lc_pouch_state_payload_container_is_valid(destination_leaf))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection relocation requires distinct files, record "
                        "and bounded buffer",
                        NULL, NULL, "pouch");
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_collection_record_read(source_fd, current->record_offset,
                                       current->next_offset, source_leaf,
                                       &source, error);
  if (rc != LC_OK)
    return rc;
  if (source.position != current->position ||
      source.generation != current->generation ||
      source.published_at != current->published_at ||
      source.deleted != current->deleted || source.bytes != current->bytes ||
      source.retention != current->retention ||
      strcmp(source.append_id, current->append_id) != 0 ||
      strcmp(source.etag, current->etag) != 0)
    return lc_error_set(
        error, LC_ERR_PROTOCOL, 0L,
        "collection relocation source differs from its committed identity",
        NULL, "collection_corrupt", "pouch");
  if (source.metadata_only) {
    if (fstat(payload_fd, &body_stat) != 0 || body_stat.st_size < 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to inspect collection payload reference",
                          NULL, NULL, "pouch");
    rc = lc_pouch_collection_record_read(
        payload_fd, source.payload_record_offset, (uint64_t)body_stat.st_size,
        source.payload_container, &body, error);
    if (rc != LC_OK)
      return rc;
    if (body.deleted || body.metadata_only ||
        body.position != source.position ||
        body.published_at != source.published_at ||
        body.generation > source.generation || body.bytes != source.bytes ||
        body.payload_offset != source.payload_offset ||
        body.stored_bytes != source.stored_bytes ||
        body.stored_crc != source.stored_crc ||
        strcmp(body.append_id, source.append_id) != 0 ||
        strcmp(body.etag, source.etag) != 0 ||
        strcmp(body.context, source.context) != 0 ||
        strcmp(body.descriptor, source.descriptor) != 0)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "collection metadata refers to another payload", NULL,
                          "collection_corrupt", "pouch");
  }
  *out = source;
  out->published = 0;
  metadata_offset = source.record_offset + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                    strlen(source.append_id);
  meta_length = source.metadata_length;
  if (meta_length > sizeof(meta))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "collection relocation metadata exceeds its bound",
                        NULL, "collection_corrupt", "pouch");
  rc = lc_pouch_collection_record_pread(source_fd, metadata_offset, meta,
                                        meta_length, error);
  if (rc != LC_OK)
    return rc;
  start = 0U;
  rc = lc_pouch_state_fd_end(destination_fd, &start,
                             "failed to seek collection relocation output",
                             error);
  if (rc != LC_OK)
    return rc;
  out->record_offset = start;
  type = source.deleted ? LC_POUCH_STATE_RECORD_STATE_DELETE
                        : (source.kind == LC_POUCH_COLLECTION_EDIT
                               ? LC_POUCH_COLLECTION_EDIT
                               : LC_POUCH_STATE_RECORD_STATE_PUT);
  if (!source.deleted) {
    /* Keep all metadata bytes, including the original timestamp, crypto
     * descriptor and immutable context. Only the container/span can change. */
    container_offset = LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES +
                       lc_pouch_state_get16(meta + 62U) +
                       lc_pouch_state_get16(meta + 64U) +
                       lc_pouch_state_get32(meta + 66U);
    old_length = lc_pouch_state_get16(meta + 70U);
    new_length = strlen(destination_leaf);
    if (container_offset > meta_length ||
        old_length > meta_length - container_offset ||
        new_length > sizeof(meta) - (meta_length - old_length))
      return lc_error_set(
          error, LC_ERR_PROTOCOL, 0L,
          "collection relocation container exceeds metadata bound", NULL,
          "collection_corrupt", "pouch");
    memmove(meta + container_offset + new_length,
            meta + container_offset + old_length,
            meta_length - container_offset - old_length);
    memcpy(meta + container_offset, destination_leaf, new_length);
    meta_length = meta_length - old_length + new_length;
    lc_pouch_state_put16(meta + 70U, (unsigned int)new_length);
    lc_pouch_collection_state_binding(meta + container_offset + new_length +
                                          lc_pouch_state_get16(meta + 72U) + 4U,
                                      source.published_at, 0U, 0U,
                                      source.has_collection_retention,
                                      source.collection_retention);
    if (start > (uint64_t)LC_I64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                    strlen(source.append_id) - meta_length)
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "collection relocation exceeds file offset range",
                          NULL, NULL, "pouch");
    out->payload_offset = start + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                          strlen(source.append_id) + meta_length;
    lc_pouch_state_put64(meta + 32U, start);
    lc_pouch_state_put64(meta + 40U, out->payload_offset);
    out->payload_record_offset = start;
    strcpy(out->payload_container, destination_leaf);
    out->metadata_only = 0;
    out->previous_segment = 0U;
    out->previous_offset = 0U;
  }
  out->metadata_length = meta_length;
  rc = lc_pouch_state_record_write_prefix(
      destination_fd, type, source.append_id, strlen(source.append_id), meta,
      meta_length, source.stored_bytes, source.stored_crc,
      LC_POUCH_RECORD_FLAG_PENDING, error);
  remaining = source.stored_bytes;
  offset = source.payload_offset;
  crc = (unsigned long)crc32(0L, Z_NULL, 0);
  if (capacity > LC_POUCH_STATE_FILE_CHUNK_BYTES)
    capacity = LC_POUCH_STATE_FILE_CHUNK_BYTES;
  while (rc == LC_OK && remaining != 0U) {
    want = remaining < (uint64_t)capacity ? (size_t)remaining : capacity;
    rc = lc_pouch_collection_record_pread(payload_fd, offset, buffer, want,
                                          error);
    if (rc == LC_OK) {
      crc = (unsigned long)crc32((uLong)crc, buffer, (uInt)want);
      rc = lc_pouch_state_write_all(destination_fd, buffer, want, error);
    }
    if (rc == LC_OK)
      rc = lc_pouch_state_compaction_throttle(pouch, want, error);
    remaining -= (uint64_t)want;
    offset += (uint64_t)want;
  }
  if (rc == LC_OK && crc != source.stored_crc)
    rc =
        lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                     "collection relocation source payload checksum is invalid",
                     NULL, "collection_corrupt", "pouch");
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_finalize(
        destination_fd, start, type, source.append_id, strlen(source.append_id),
        meta, meta_length, source.stored_bytes, source.stored_crc, error);
    out->published = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_pouch_state_fd_end(
        destination_fd, &out->next_offset,
        "failed to restore collection relocation append offset", error);
  if (rc != LC_OK && !out->published)
    lc_pouch_state_append_fd_rollback(destination_fd, start, 1);
  return rc;
}

/* Root compression conversion shares canonical parsing and encoding. Physical
 * offset maps contain metadata only; every body crosses the codec once. */
static char *lc_ps_recode_binding(lc_pouch_recode *work,
                                  lc_pouch_recode_file *file,
                                  const lc_pouch_state_entry *entry) {
  char version[32], *context;
  size_t size;
  if (file->collection_id[0]) {
    if (lc_u64_format_base10(entry->version, version, sizeof(version)) < 0)
      return NULL;
    size =
        strlen(file->collection_id) + strlen(entry->key) + strlen(version) + 14;
    context = lc_alloc_with_allocator(&work->pouch->allocator, size);
    if (context)
      (void)snprintf(context, size, "collection:%s:%s:%s", file->collection_id,
                     entry->key, version);
    return context;
  }
  return lc_pouch_state_crypto_context(
      &work->pouch->allocator, file->ns, entry->key, entry->version,
      lc_pouch_state_payload_class(entry->record_type, entry->object_link));
}

static int lc_ps_recode_meta(lc_pouch_recode *work,
                             const lc_pouch_state_entry *entry,
                             const lc_pouch_state_payload_span *span,
                             const char *descriptor, unsigned char **meta,
                             size_t *length, lc_error *error) {
  int rc = lc_pouch_state_encode_payload_meta(
      &work->pouch->allocator, entry->version, entry->updated_at_unix,
      entry->bytes,
      span && span->present ? span->payload_length : entry->cipher_bytes,
      entry->content_type, entry->etag, descriptor, span,
      entry->payload_context, entry->metadata, entry->metadata_length,
      LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, entry->has_query_hidden,
      entry->query_hidden, entry->staged_delete_marker, entry->retention, meta,
      length, error);
  if (rc == LC_OK) {
    if (entry->object_link)
      (*meta)[60] |= LC_POUCH_STATE_PAYLOAD_FLAG_OBJECT_LINK;
    rc = entry->has_query_index_seq
             ? lc_pouch_state_meta_set_index_sequences(
                   &work->pouch->allocator, meta, length, entry->index_seq,
                   entry->query_index_seq, error)
             : lc_pouch_state_meta_set_index_seq(*meta, *length,
                                                 entry->index_seq, error);
  }
  return rc;
}

static int lc_ps_recode_body(lc_pouch_recode *work, lc_pouch_recode_file *file,
                             const lc_pouch_state_entry *entry, int fd,
                             lc_error *error) {
  lc_pouch_state_payload_span span = entry->payload_span;
  lc_pouch_collection_body body;
  lc_pouch_crypto_writer *codec = NULL;
  lc_source *source = NULL;
  char *binding = NULL, *descriptor = NULL, *rebound = NULL, *etag = NULL;
  const char *context;
  unsigned char *meta = NULL, *chunk = NULL;
  size_t length = 0, reserved = 0, got;
  lc_u64 start = 0, plain = 0, stored = 0;
  unsigned long crc = 0;
  int rc;
  memset(&body, 0, sizeof(body));
  binding = lc_ps_recode_binding(work, file, entry);
  if (!binding)
    return LC_ERR_NOMEM;
  context = entry->payload_context && *entry->payload_context
                ? entry->payload_context
                : binding;
  rc = lc_pouch_state_fd_end(fd, &start, "failed to locate converted payload",
                             error);
  span.container_leaf = file->leaf;
  span.record_offset = start;
  span.payload_offset = 0;
  span.payload_length = 0;
  span.payload_crc = 0;
  if (rc == LC_OK)
    rc = lc_ps_recode_meta(work, entry, &span, NULL, &meta, &reserved, error);
  if (rc == LC_OK) {
    span.payload_offset =
        start + LC_POUCH_RECORD_HEADER_BYTES + strlen(entry->key) + reserved;
    rc = lc_pouch_state_record_write_prefix(
        fd, entry->record_type, entry->key, strlen(entry->key), meta, reserved,
        0, 0, LC_POUCH_RECORD_FLAG_PENDING, error);
  }
  lc_free_with_allocator(&work->pouch->allocator, meta);
  meta = NULL;
  if (rc == LC_OK)
    rc = lc_pouch_crypto_source_from_file_span(
        work->pouch->crypto, context, binding, file->source,
        entry->payload_span.payload_offset, entry->payload_span.payload_length,
        entry->descriptor, &source, error);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_writer_open(work->pouch->crypto, context, fd, 1,
                                     &codec, error);
  body.hash = EVP_MD_CTX_new();
  body.hash_sink.write = lc_pouch_collection_hash_write;
  body.encoded = lc_pouch_crypto_writer_sink(codec);
  if (rc == LC_OK &&
      (!body.hash || EVP_DigestInit_ex(body.hash, EVP_sha256(), NULL) != 1))
    rc = lc_error_set(error, LC_ERR_NOMEM, 0,
                      "failed to initialize conversion hash", NULL, NULL,
                      "pouch");
  if (rc == LC_OK) {
    chunk = lc_alloc_with_allocator(&work->pouch->allocator, 65536);
    if (!chunk)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0,
                        "failed to allocate conversion input chunk", NULL, NULL,
                        "pouch");
  }
  while (rc == LC_OK) {
    got = source->read(source, chunk, 65536, error);
    if (error && error->code != LC_OK) {
      rc = error->code;
      break;
    }
    if (got == 0)
      break;
    if (!body.hash_sink.write(&body.hash_sink, chunk, got, error))
      rc = error && error->code != LC_OK ? error->code : LC_ERR_TRANSPORT;
  }
  if (rc == LC_OK)
    rc = lc_pouch_crypto_writer_finish(codec, &plain, &stored, &crc,
                                       &descriptor, error);
  if (rc == LC_OK) {
    etag = lc_pouch_state_hash_final(&work->pouch->allocator, body.hash, error);
    if (!etag)
      rc = LC_ERR_NOMEM;
    else if (plain != entry->bytes || strcmp(etag, entry->etag))
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                        "conversion payload integrity check failed", entry->key,
                        NULL, "pouch");
  }
  if (rc == LC_OK && descriptor && strcmp(context, binding)) {
    rc = lc_pouch_crypto_rebind_descriptor(work->pouch->crypto, descriptor,
                                           context, binding, &rebound, error);
    if (rc == LC_OK) {
      lc_free_with_allocator(&work->pouch->allocator, descriptor);
      descriptor = rebound;
      rebound = NULL;
    }
  }
  span.payload_length = stored;
  span.payload_crc = crc;
  if (rc == LC_OK)
    rc = lc_ps_recode_meta(work, entry, &span, descriptor, &meta, &length,
                           error);
  if (rc == LC_OK && length != reserved)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "converted metadata changed reservation size", NULL, NULL,
                      "pouch");
  if (rc == LC_OK)
    rc = lc_pouch_state_record_finalize(fd, start, entry->record_type,
                                        entry->key, strlen(entry->key), meta,
                                        length, stored, crc, error);
  lc_source_close(source);
  lc_pouch_crypto_writer_close(codec);
  EVP_MD_CTX_free(body.hash);
  lc_free_with_allocator(&work->pouch->allocator, binding);
  lc_free_with_allocator(&work->pouch->allocator, descriptor);
  lc_free_with_allocator(&work->pouch->allocator, rebound);
  lc_free_with_allocator(&work->pouch->allocator, etag);
  lc_free_with_allocator(&work->pouch->allocator, meta);
  lc_free_with_allocator(&work->pouch->allocator, chunk);
  return rc;
}

int lc_pouch_state_recode_file(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_error *error) {
  lc_pouch_state_entry entry;
  lc_pouch_record_header header;
  struct stat st;
  FILE *fp;
  unsigned char prefix[LC_POUCH_RECORD_HEADER_BYTES], *meta = NULL;
  size_t length;
  lc_u64 before, after = 0, limit;
  int fd, found, truncated, rc = LC_OK;
  memset(&entry, 0, sizeof(entry));
  fp = fopen(file->source, "rb");
  fd =
      fp ? open(file->target, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600) : -1;
  if (!fp || fd < 0 || fstat(fileno(fp), &st) != 0 || st.st_size < 0)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0,
                      "failed to open canonical conversion file",
                      strerror(errno), file->source, "pouch");
  limit = rc == LC_OK ? (lc_u64)st.st_size : 0;
  while (rc == LC_OK) {
    before = (lc_u64)ftello(fp);
    rc = lc_pouch_state_read_next_record(fp, &work->pouch->allocator,
                                         file->leaf, file->active, 1, limit,
                                         &entry, &found, &truncated, error);
    if (rc == LC_OK && !found && truncated && before < limit) {
      /* Collection replay permits incomplete headers and PENDING tails only.
       * The generic state parser also tolerates truncated published records;
       * rejecting those here prevents conversion from discarding live entries.
       * Inspect only discarded tails, without adding work to healthy records.
       */
      if (file->collection_id[0] && limit - before >= sizeof(prefix)) {
        rc = lc_pouch_collection_record_pread(fileno(fp), before, prefix,
                                              sizeof(prefix), error);
        if (rc == LC_OK)
          rc = lc_pouch_record_header_decode(prefix, &header, error);
        if (rc == LC_OK && header.flags != LC_POUCH_RECORD_FLAG_PENDING)
          rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                            "published collection record is truncated",
                            file->source, "collection_corrupt", "pouch");
      }
      /* A collection's retained tail may be the last committed end, just
       * before an unpublished torn append. Preserve that boundary as well as
       * physical EOF when dropping the incomplete suffix. */
      if (rc == LC_OK)
        rc = lc_pouch_state_fd_end(
            fd, &after, "failed to locate converted tail boundary", error);
      if (rc == LC_OK)
        rc = lc_pouch_recode_offset_add(work, file, before, after, error);
    }
    if (rc != LC_OK || !found)
      break;
    rc = lc_pouch_state_fd_end(fd, &after, "failed to locate converted record",
                               error);
    if (rc == LC_OK)
      rc = lc_pouch_recode_offset_add(work, file, before, after, error);
    if (rc == LC_OK && (entry.record_type == LC_POUCH_COLLECTION_EDIT_BODY ||
                        entry.record_type == LC_POUCH_COLLECTION_EDIT ||
                        entry.record_type == LC_POUCH_STATE_RECORD_STATE_PUT ||
                        entry.record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT))
      rc = lc_ps_recode_body(work, file, &entry, fd, error);
    else if (rc == LC_OK &&
             (entry.record_type == LC_POUCH_STATE_RECORD_STATE_LINK ||
              entry.record_type == LC_POUCH_STATE_RECORD_STATE_META)) {
      /* Reserve final descriptor room now; a second pass resolves old spans
       * to their already encoded bodies, including forward references. */
      rc = lc_ps_recode_meta(work, &entry, &entry.payload_span,
                             entry.descriptor, &meta, &length, error);
      if (rc == LC_OK)
        rc = lc_pouch_state_record_write_prefix(fd, entry.record_type,
                                                entry.key, strlen(entry.key),
                                                meta, length, 0, 0, 0, error);
    } else if (rc == LC_OK) {
      rc = lc_pouch_collection_record_pread(fileno(fp), before, prefix,
                                            sizeof(prefix), error);
      if (rc == LC_OK)
        rc = lc_pouch_record_header_decode(prefix, &header, error);
      length = rc == LC_OK ? (size_t)header.meta_len + header.key_len : 0;
      meta = rc == LC_OK ? lc_alloc_with_allocator(&work->pouch->allocator,
                                                   length ? length : 1)
                         : NULL;
      if (rc == LC_OK && !meta)
        rc = LC_ERR_NOMEM;
      if (rc == LC_OK)
        rc = lc_pouch_collection_record_pread(
            fileno(fp), before + sizeof(prefix), meta, length, error);
      if (rc == LC_OK)
        rc = lc_pouch_state_record_write_prefix(
            fd, header.type, (const char *)meta, header.key_len,
            meta + header.key_len, header.meta_len, 0, 0, header.flags, error);
    }
    lc_free_with_allocator(&work->pouch->allocator, meta);
    meta = NULL;
    lc_pouch_state_entry_cleanup(&work->pouch->allocator, &entry);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_fd_end(fd, &after, "failed to locate converted end",
                               error);
    if (rc == LC_OK)
      rc = lc_pouch_recode_offset_add(work, file, limit, after, error);
    if (rc == LC_OK) {
      work->input_bytes += limit;
      work->output_bytes += after;
    }
  }
  if (fp)
    (void)fclose(fp);
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0,
                      "failed to close converted canonical file",
                      strerror(errno), NULL, "pouch");
  lc_pouch_state_entry_cleanup(&work->pouch->allocator, &entry);
  lc_free_with_allocator(&work->pouch->allocator, meta);
  return rc;
}

int lc_pouch_state_recode_refs(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_error *error) {
  lc_pouch_state_entry entry, body;
  lc_pouch_recode_file *owner;
  lc_pouch_state_payload_span span;
  FILE *fp, *body_fp = NULL;
  struct stat st;
  lc_u64 translated, start, limit;
  unsigned char *meta = NULL;
  size_t length, reserved;
  char *old_binding = NULL, *new_binding = NULL, *descriptor = NULL,
       *verified = NULL;
  int fd, found, truncated, rc = LC_OK;
  memset(&entry, 0, sizeof(entry));
  memset(&body, 0, sizeof(body));
  fp = fopen(file->target, "rb");
  fd = fp ? open(file->target, O_RDWR | O_CLOEXEC) : -1;
  if (!fp || fd < 0 || fstat(fd, &st) != 0 || st.st_size < 0)
    rc = LC_ERR_TRANSPORT;
  limit = rc == LC_OK ? (lc_u64)st.st_size : 0;
  while (rc == LC_OK) {
    start = (lc_u64)ftello(fp);
    rc = lc_pouch_state_read_next_record(fp, &work->pouch->allocator,
                                         file->leaf, 0, 0, limit, &entry,
                                         &found, &truncated, error);
    if (rc != LC_OK || !found)
      break;
    if ((entry.record_type == LC_POUCH_STATE_RECORD_STATE_LINK ||
         entry.record_type == LC_POUCH_STATE_RECORD_STATE_META) &&
        entry.payload_span.present) {
      reserved = (size_t)((lc_u64)ftello(fp) - start -
                          LC_POUCH_RECORD_HEADER_BYTES - strlen(entry.key));
      rc = lc_pouch_recode_find(
          work, file->scope, entry.payload_span.container_leaf,
          entry.payload_span.record_offset, &owner, &translated, error);
      if (rc == LC_OK) {
        body_fp = fopen(owner->target, "rb");
        if (!body_fp || fseeko(body_fp, (off_t)translated, SEEK_SET) != 0 ||
            fstat(fileno(body_fp), &st) != 0)
          rc = LC_ERR_TRANSPORT;
      }
      if (rc == LC_OK)
        rc = lc_pouch_state_read_next_record(
            body_fp, &work->pouch->allocator, owner->leaf, 0, 0,
            (lc_u64)st.st_size, &body, &found, &truncated, error);
      if (body_fp) {
        (void)fclose(body_fp);
        body_fp = NULL;
      }
      if (rc == LC_OK &&
          (!found || !body.payload_span.present ||
           (body.record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
            body.record_type != LC_POUCH_COLLECTION_EDIT_BODY &&
            body.record_type != LC_POUCH_STATE_RECORD_OBJECT_PUT)))
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                          "conversion reference does not name a payload record",
                          NULL, NULL, "pouch");
      if (rc == LC_OK) {
        old_binding = lc_ps_recode_binding(work, owner, &body);
        new_binding = lc_ps_recode_binding(work, file, &entry);
        if (!old_binding || !new_binding)
          rc = LC_ERR_NOMEM;
      }
      if (rc == LC_OK && entry.descriptor)
        rc = lc_pouch_crypto_rebind_descriptor(work->pouch->crypto,
                                               entry.descriptor, new_binding,
                                               new_binding, &verified, error);
      /* References with the same binding must share the payload's exact
       * descriptor. Rebinding would generate a different authentication nonce
       * and break collection metadata/body identity checks. */
      if (rc == LC_OK && body.descriptor &&
          strcmp(old_binding, new_binding) != 0)
        rc = lc_pouch_crypto_rebind_descriptor(work->pouch->crypto,
                                               body.descriptor, old_binding,
                                               new_binding, &descriptor, error);
      if (rc == LC_OK) {
        span = body.payload_span;
        rc = lc_ps_recode_meta(work, &entry, &span,
                               descriptor ? descriptor : body.descriptor, &meta,
                               &length, error);
        if (rc == LC_OK && length != reserved)
          rc = LC_ERR_PROTOCOL;
        if (rc == LC_OK)
          rc = lc_pouch_state_record_finalize(fd, start, entry.record_type,
                                              entry.key, strlen(entry.key),
                                              meta, length, 0, 0, error);
      }
    }
    lc_pouch_state_entry_cleanup(&work->pouch->allocator, &entry);
    lc_pouch_state_entry_cleanup(&work->pouch->allocator, &body);
    lc_free_with_allocator(&work->pouch->allocator, meta);
    meta = NULL;
    lc_free_with_allocator(&work->pouch->allocator, descriptor);
    descriptor = NULL;
    lc_free_with_allocator(&work->pouch->allocator, verified);
    verified = NULL;
    lc_free_with_allocator(&work->pouch->allocator, old_binding);
    old_binding = NULL;
    lc_free_with_allocator(&work->pouch->allocator, new_binding);
    new_binding = NULL;
  }
  if (rc == LC_OK && (stat(file->source, &st) != 0 ||
                      fchmod(fd, st.st_mode & 0777) != 0 || fsync(fd) != 0))
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0,
                      "failed to preserve and sync converted canonical file",
                      strerror(errno), file->target, "pouch");
  if (fp)
    (void)fclose(fp);
  if (fd >= 0)
    (void)close(fd);
  lc_pouch_state_entry_cleanup(&work->pouch->allocator, &entry);
  lc_pouch_state_entry_cleanup(&work->pouch->allocator, &body);
  lc_free_with_allocator(&work->pouch->allocator, meta);
  lc_free_with_allocator(&work->pouch->allocator, descriptor);
  lc_free_with_allocator(&work->pouch->allocator, verified);
  lc_free_with_allocator(&work->pouch->allocator, old_binding);
  lc_free_with_allocator(&work->pouch->allocator, new_binding);
  return rc;
}
