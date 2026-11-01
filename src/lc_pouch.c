#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_collection_maintenance.h"
#include "lc_collection_migration.h"
#include "lc_intcompat.h"
#include "lc_log.h"
#include "lc_pouch_format.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"
#include "lc_pouch_recode.h"
#include "lc_pouch_retention.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <poll.h>
#include <sys/inotify.h>
#include <sys/vfs.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__) || defined(__DragonFly__)
#include <strings.h>
#include <sys/mount.h>
#endif

#include <openssl/rand.h>

static uint64_t lc_pouch_next_writer_marker_id;
static pthread_mutex_t lc_pouch_writer_marker_mutex = PTHREAD_MUTEX_INITIALIZER;
static int lc_pouch_resolve_compression(lc_pouch *pouch, const char *stored,
                                        lc_error *error);
static int lc_pouch_prepare_collection_heads(lc_pouch *pouch, int stored_bytes,
                                             lc_error *error);
static int lc_pouch_writer_root_lock_change(lc_pouch *pouch, int mode,
                                            lc_error *error);
static int lc_pc_exists(const char *path, int *exists, lc_error *error);
static pthread_mutex_t lc_pouch_writer_root_lock_mutex =
    PTHREAD_MUTEX_INITIALIZER;

/*
 * POSIX record locks belong to a process, rather than an individual file
 * descriptor. One process therefore needs one retained descriptor and a
 * local holder registry for each Pouch root; otherwise a second local open
 * can accidentally upgrade a shared lock or release every lock on close.
 */
struct lc_pouch_writer_root_lock_entry {
  lc_allocator allocator;
  dev_t device;
  ino_t inode;
  int fd;
  pthread_mutex_t manifest_mutex;
  unsigned long shared_holders;
  unsigned long exclusive_holders;
  struct lc_pouch_writer_root_lock_entry *next;
};

static lc_pouch_writer_root_lock_entry *lc_pouch_writer_root_locks;

#define LC_POUCH_EXCLUSIVE_WRITER_TOUCH_NS 1000000000L
#define LC_POUCH_EXCLUSIVE_WRITER_TTL_NS ((int64_t)3 * (int64_t)1000000000)
#define LC_POUCH_WRITER_PRESENCE_MISSING (-1001)
#define LC_POUCH_WRITER_ROOT_LOCK_NONE 0
#define LC_POUCH_WRITER_ROOT_LOCK_SHARED 1
#define LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE 2
/* Match Go disk's disk-store index writer batching policy. */
#define LC_POUCH_DEFAULT_INDEXER_FLUSH_DOCS 2000U
#define LC_POUCH_DEFAULT_INDEXER_FLUSH_INTERVAL_SECONDS 10U

struct lc_pouch_indexer_pending_namespace {
  char *ns;
  uint64_t write_count;
  struct lc_pouch_indexer_pending_namespace *next;
};

static const uint64_t
    lc_pouch_fsync_batch_bounds[LC_POUCH_FSYNC_BATCH_BOUND_COUNT] = {
        1U, 2U, 4U, 8U, 16U, 32U, 64U, 128U, 256U, 512U, 1024U, 2048U, 4096U,
};

struct lc_pouch_fsync_request {
  int fd;
  int done;
  int errnum;
  pthread_cond_t cond;
  struct lc_pouch_fsync_request *next;
};

typedef struct lc_pouch_fsync_batch_file {
  dev_t dev;
  ino_t ino;
  int fd;
} lc_pouch_fsync_batch_file;

typedef struct lc_pouch_fsync_batcher_ref {
  uint64_t batch_max_ops;
  struct lc_pouch_fsync_batcher_ref *next;
} lc_pouch_fsync_batcher_ref;

struct lc_pouch_fsync_batcher {
  dev_t device;
  ino_t inode;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pthread_t thread;
  lc_pouch_fsync_request *head;
  lc_pouch_fsync_request *tail;
  size_t queue_count;
  uint64_t batch_max_ops;
  lc_pouch_fsync_stats stats;
  lc_pouch_fsync_batcher_ref *refs;
  unsigned long refcount;
  int stop;
  struct lc_pouch_fsync_batcher *next;
};

static pthread_mutex_t lc_pouch_fsync_batcher_registry_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_fsync_batcher *lc_pouch_fsync_batchers;

#ifdef LOCKDC_TEST_BUILD
long lc_pouch_test_fsync_batch_delay_ns;
int (*lc_pouch_test_sync_fd)(int fd);
void (*lc_pouch_test_fsync_enqueued)(void);
#endif

static int lc_pouch_mutex_init_recursive(pthread_mutex_t *mutex) {
  pthread_mutexattr_t attr;
  int rc;

  rc = pthread_mutexattr_init(&attr);
  if (rc != 0) {
    return rc;
  }
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (rc == 0) {
    rc = pthread_mutex_init(mutex, &attr);
  }
  (void)pthread_mutexattr_destroy(&attr);
  return rc;
}

static int lc_pouch_sync_fd(int fd) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_sync_fd != NULL) {
    return lc_pouch_test_sync_fd(fd);
  }
#endif
#ifdef __linux__
  return fdatasync(fd);
#else
  return fsync(fd);
#endif
}

#ifdef LOCKDC_TEST_BUILD
static void lc_pouch_fsync_deadline(struct timespec *deadline, long delay_ns) {
  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  deadline->tv_nsec += delay_ns;
  if (deadline->tv_nsec >= 1000000000L) {
    deadline->tv_sec += deadline->tv_nsec / 1000000000L;
    deadline->tv_nsec %= 1000000000L;
  }
}
#endif

static uint64_t lc_pouch_elapsed_ns(const struct timespec *started,
                                    const struct timespec *finished) {
  uint64_t seconds;
  uint64_t nanos;

  if (started == NULL || finished == NULL ||
      finished->tv_sec < started->tv_sec ||
      (finished->tv_sec == started->tv_sec &&
       finished->tv_nsec < started->tv_nsec)) {
    return 0U;
  }
  seconds = (uint64_t)(finished->tv_sec - started->tv_sec);
  if (finished->tv_nsec < started->tv_nsec) {
    --seconds;
    nanos = (uint64_t)(1000000000L + finished->tv_nsec - started->tv_nsec);
  } else {
    nanos = (uint64_t)(finished->tv_nsec - started->tv_nsec);
  }
  if (seconds > LC_U64_MAX / 1000000000U) {
    return LC_U64_MAX;
  }
  seconds *= 1000000000U;
  if (nanos > LC_U64_MAX - seconds) {
    return LC_U64_MAX;
  }
  return seconds + nanos;
}

static void lc_pouch_fsync_record_batch_locked(lc_pouch_fsync_batcher *batcher,
                                               size_t count, uint64_t sync_ns) {
  size_t bucket;
  size_t index;

  if (batcher == NULL || count == 0U) {
    return;
  }
  ++batcher->stats.total_batches;
  batcher->stats.total_requests += (uint64_t)count;
  if ((uint64_t)count > batcher->stats.max_batch_size) {
    batcher->stats.max_batch_size = (uint64_t)count;
  }
  batcher->stats.total_sync_ns += sync_ns;
  if (sync_ns > batcher->stats.max_sync_ns) {
    batcher->stats.max_sync_ns = sync_ns;
  }
  bucket = LC_POUCH_FSYNC_BATCH_BOUND_COUNT;
  for (index = 0U; index < LC_POUCH_FSYNC_BATCH_BOUND_COUNT; ++index) {
    if ((uint64_t)count <= lc_pouch_fsync_batch_bounds[index]) {
      bucket = index;
      break;
    }
  }
  ++batcher->stats.counts[bucket];
}

#ifdef LOCKDC_TEST_BUILD
static int
lc_pouch_fsync_batch_limit_reached(const lc_pouch_fsync_batcher *batcher) {
  if (batcher == NULL || batcher->batch_max_ops == 0U) {
    return 0;
  }
  return (uint64_t)batcher->queue_count >= batcher->batch_max_ops;
}
#endif

static void
lc_pouch_fsync_batcher_recompute_limit_locked(lc_pouch_fsync_batcher *batcher) {
  lc_pouch_fsync_batcher_ref *entry;
  uint64_t batch_max_ops;

  if (batcher == NULL) {
    return;
  }
  batch_max_ops = 0U;
  for (entry = batcher->refs; entry != NULL; entry = entry->next) {
    if (entry->batch_max_ops != 0U &&
        (batch_max_ops == 0U || entry->batch_max_ops < batch_max_ops)) {
      batch_max_ops = entry->batch_max_ops;
    }
  }
  batcher->batch_max_ops = batch_max_ops;
}

static int
lc_pouch_fsync_batcher_add_ref_locked(lc_pouch_fsync_batcher *batcher,
                                      uint64_t batch_max_ops, lc_error *error) {
  lc_pouch_fsync_batcher_ref *entry;

  entry = (lc_pouch_fsync_batcher_ref *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to track pouch fsync batch limit", NULL, NULL,
                        "pouch");
  }
  entry->batch_max_ops = batch_max_ops;
  entry->next = batcher->refs;
  batcher->refs = entry;
  lc_pouch_fsync_batcher_recompute_limit_locked(batcher);
  return LC_OK;
}

static void
lc_pouch_fsync_batcher_remove_ref_locked(lc_pouch_fsync_batcher *batcher,
                                         uint64_t batch_max_ops) {
  lc_pouch_fsync_batcher_ref *entry;
  lc_pouch_fsync_batcher_ref *previous;

  if (batcher == NULL) {
    return;
  }
  previous = NULL;
  entry = batcher->refs;
  while (entry != NULL) {
    if (entry->batch_max_ops == batch_max_ops) {
      if (previous != NULL) {
        previous->next = entry->next;
      } else {
        batcher->refs = entry->next;
      }
      lc_free_with_allocator(NULL, entry);
      break;
    }
    previous = entry;
    entry = entry->next;
  }
  lc_pouch_fsync_batcher_recompute_limit_locked(batcher);
}

static void
lc_pouch_fsync_batcher_refs_cleanup(lc_pouch_fsync_batcher_ref *entry) {
  while (entry != NULL) {
    lc_pouch_fsync_batcher_ref *next;

    next = entry->next;
    lc_free_with_allocator(NULL, entry);
    entry = next;
  }
}

static int lc_pouch_fsync_batch_seen(lc_pouch_fsync_batch_file *files,
                                     size_t count, const struct stat *st) {
  size_t index;

  if (files == NULL || st == NULL) {
    return 0;
  }
  for (index = 0U; index < count; ++index) {
    if (files[index].dev == st->st_dev && files[index].ino == st->st_ino) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_fsync_process_batch(lc_pouch_fsync_request *batch,
                                         size_t count, uint64_t *sync_ns_out) {
  lc_pouch_fsync_batch_file inline_files[64];
  lc_pouch_fsync_batch_file *files;
  lc_pouch_fsync_request *request;
  struct stat st;
  size_t file_count;
  int errnum;
  struct timespec started;
  struct timespec finished;
  int clock_started;

  if (sync_ns_out != NULL) {
    *sync_ns_out = 0U;
  }
  clock_started = clock_gettime(CLOCK_MONOTONIC, &started) == 0;

  files = inline_files;
  if (count > sizeof(inline_files) / sizeof(inline_files[0])) {
    files = (lc_pouch_fsync_batch_file *)lc_calloc_with_allocator(
        NULL, count, sizeof(lc_pouch_fsync_batch_file));
    if (files == NULL) {
      errnum = ENOMEM;
      goto finish;
    }
  }
  file_count = 0U;
  errnum = 0;
  for (request = batch; request != NULL; request = request->next) {
    if (request->fd < 0) {
      continue;
    }
    if (fstat(request->fd, &st) != 0) {
      if (errnum == 0) {
        errnum = errno != 0 ? errno : EIO;
      }
      continue;
    }
    if (lc_pouch_fsync_batch_seen(files, file_count, &st)) {
      continue;
    }
    files[file_count].dev = st.st_dev;
    files[file_count].ino = st.st_ino;
    files[file_count].fd = request->fd;
    ++file_count;
    if (lc_pouch_sync_fd(request->fd) != 0 && errnum == 0) {
      errnum = errno != 0 ? errno : EIO;
    }
  }
  if (files != inline_files) {
    lc_free_with_allocator(NULL, files);
  }

finish:
  if (clock_started && clock_gettime(CLOCK_MONOTONIC, &finished) == 0 &&
      sync_ns_out != NULL) {
    *sync_ns_out = lc_pouch_elapsed_ns(&started, &finished);
  }
  for (request = batch; request != NULL; request = request->next) {
    request->errnum = errnum;
  }
}

static lc_pouch_fsync_request *
lc_pouch_fsync_take_batch(lc_pouch_fsync_batcher *batcher, size_t *out_count) {
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *tail;
  size_t count;

  batch = batcher->head;
  tail = NULL;
  count = 0U;
  while (batcher->head != NULL && (batcher->batch_max_ops == 0U ||
                                   (uint64_t)count < batcher->batch_max_ops)) {
    tail = batcher->head;
    batcher->head = batcher->head->next;
    ++count;
  }
  if (tail != NULL) {
    tail->next = NULL;
  }
  if (batcher->head == NULL) {
    batcher->tail = NULL;
  }
  if (batcher->queue_count >= count) {
    batcher->queue_count -= count;
  } else {
    batcher->queue_count = 0U;
  }
  if (out_count != NULL) {
    *out_count = count;
  }
  return batch;
}

static void *lc_pouch_fsync_worker(void *arg) {
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *request;
  size_t batch_count;
  uint64_t sync_ns;
#ifdef LOCKDC_TEST_BUILD
  struct timespec deadline;
  long batch_delay_ns;
#endif

  batcher = (lc_pouch_fsync_batcher *)arg;
  pthread_mutex_lock(&batcher->mutex);
  for (;;) {
    while (batcher->head == NULL && !batcher->stop) {
      pthread_cond_wait(&batcher->cond, &batcher->mutex);
    }
    if (batcher->head == NULL && batcher->stop) {
      pthread_mutex_unlock(&batcher->mutex);
      return NULL;
    }
#ifdef LOCKDC_TEST_BUILD
    /* Tests can hold this window open to coordinate exact group membership. */
    batch_delay_ns = lc_pouch_test_fsync_batch_delay_ns;
    if (!batcher->stop && batch_delay_ns > 0L &&
        !lc_pouch_fsync_batch_limit_reached(batcher)) {
      lc_pouch_fsync_deadline(&deadline, batch_delay_ns);
      while (!batcher->stop && !lc_pouch_fsync_batch_limit_reached(batcher)) {
        if (pthread_cond_timedwait(&batcher->cond, &batcher->mutex,
                                   &deadline) == ETIMEDOUT) {
          break;
        }
      }
    }
#endif
    /* Flush the ready group immediately. Requests arriving during I/O form
     * the next group; a sequential producer cannot fill a deliberate wait. */
    batch = lc_pouch_fsync_take_batch(batcher, &batch_count);
    pthread_mutex_unlock(&batcher->mutex);
    sync_ns = 0U;
    lc_pouch_fsync_process_batch(batch, batch_count, &sync_ns);
    pthread_mutex_lock(&batcher->mutex);
    lc_pouch_fsync_record_batch_locked(batcher, batch_count, sync_ns);
    for (request = batch; request != NULL; request = request->next) {
      request->done = 1;
      pthread_cond_signal(&request->cond);
    }
  }
}

int lc_pouch_fsync_batcher_init(lc_pouch *pouch, lc_error *error) {
  struct stat st;
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_batcher *created;
  int pthread_rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher requires pouch", NULL, NULL,
                        "pouch");
  }
  if (stat(pouch->root_path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to identify pouch fsync root", strerror(errno),
                        NULL, "pouch");
  }
  pthread_mutex_lock(&lc_pouch_fsync_batcher_registry_mutex);
  for (batcher = lc_pouch_fsync_batchers; batcher != NULL;
       batcher = batcher->next) {
    if (batcher->device == st.st_dev && batcher->inode == st.st_ino) {
      break;
    }
  }
  if (batcher != NULL) {
    pthread_mutex_lock(&batcher->mutex);
    pthread_rc = lc_pouch_fsync_batcher_add_ref_locked(
        batcher, pouch->fsync_batch_max_ops, error);
    if (pthread_rc == LC_OK) {
      ++batcher->refcount;
      pthread_cond_broadcast(&batcher->cond);
    }
    pthread_mutex_unlock(&batcher->mutex);
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    if (pthread_rc != LC_OK) {
      return pthread_rc;
    }
    pouch->fsync_batcher = batcher;
    return LC_OK;
  }
  created = (lc_pouch_fsync_batcher *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*created));
  if (created == NULL) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch fsync batcher", NULL, NULL,
                        "pouch");
  }
  created->device = st.st_dev;
  created->inode = st.st_ino;
  pthread_rc = pthread_mutex_init(&created->mutex, NULL);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_rc = pthread_cond_init(&created->cond, NULL);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    pthread_mutex_destroy(&created->mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_rc = lc_pouch_fsync_batcher_add_ref_locked(
      created, pouch->fsync_batch_max_ops, error);
  if (pthread_rc != LC_OK) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    pthread_cond_destroy(&created->cond);
    pthread_mutex_destroy(&created->mutex);
    lc_free_with_allocator(NULL, created);
    return pthread_rc;
  }
  created->refcount = 1UL;
  pthread_rc =
      pthread_create(&created->thread, NULL, lc_pouch_fsync_worker, created);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    lc_pouch_fsync_batcher_refs_cleanup(created->refs);
    pthread_cond_destroy(&created->cond);
    pthread_mutex_destroy(&created->mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch fsync worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  created->next = lc_pouch_fsync_batchers;
  lc_pouch_fsync_batchers = created;
  pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
  pouch->fsync_batcher = created;
  return LC_OK;
}

void lc_pouch_fsync_batcher_close(lc_pouch *pouch) {
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_batcher **cursor;

  if (pouch == NULL) {
    return;
  }
  pthread_mutex_lock(&lc_pouch_fsync_batcher_registry_mutex);
  batcher = pouch->fsync_batcher;
  pouch->fsync_batcher = NULL;
  if (batcher == NULL) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  lc_pouch_fsync_batcher_remove_ref_locked(batcher, pouch->fsync_batch_max_ops);
  if (batcher->refcount > 0UL) {
    --batcher->refcount;
  }
  if (batcher->refcount != 0UL) {
    pthread_cond_broadcast(&batcher->cond);
    pthread_mutex_unlock(&batcher->mutex);
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return;
  }
  for (cursor = &lc_pouch_fsync_batchers; *cursor != NULL;
       cursor = &(*cursor)->next) {
    if (*cursor == batcher) {
      *cursor = batcher->next;
      break;
    }
  }
  batcher->stop = 1;
  pthread_cond_broadcast(&batcher->cond);
  pthread_mutex_unlock(&batcher->mutex);
  pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
  pthread_join(batcher->thread, NULL);
  pthread_cond_destroy(&batcher->cond);
  pthread_mutex_destroy(&batcher->mutex);
  lc_pouch_fsync_batcher_refs_cleanup(batcher->refs);
  lc_free_with_allocator(NULL, batcher);
}

static void lc_pouch_compaction_deadline(lc_pouch *pouch,
                                         struct timespec *deadline) {
  uint64_t seconds;

  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  seconds = pouch->compaction_interval_seconds;
  if (seconds > (uint64_t)(LONG_MAX - deadline->tv_sec)) {
    deadline->tv_sec = LONG_MAX;
  } else {
    deadline->tv_sec += (time_t)seconds;
  }
}

static int lc_pouch_compaction_track_namespace(lc_pouch *pouch, const char *ns,
                                               int terminal, int *queued,
                                               lc_error *error) {
  char *name_copy;
  size_t index;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction namespace requires inputs", NULL,
                        NULL, "pouch");
  }
  if (queued != NULL) {
    *queued = 0;
  }
  if (!pouch->compaction_mutex_initialized) {
    return LC_OK;
  }
  pthread_mutex_lock(&pouch->compaction_mutex);
  for (index = 0U; index < pouch->compaction_namespace_count; ++index) {
    if (strcmp(pouch->compaction_namespaces[index], ns) == 0) {
      if (terminal) {
        pouch->compaction_namespace_terminal[index] = 1U;
      }
      pthread_mutex_unlock(&pouch->compaction_mutex);
      if (queued != NULL) {
        *queued = 1;
      }
      return LC_OK;
    }
  }
  if (pouch->compaction_namespace_capacity == 0U) {
    pouch->compaction_namespaces = (char **)lc_alloc_with_allocator(
        &pouch->allocator, LC_POUCH_COMPACTION_QUEUE_MAX_NAMESPACES *
                               sizeof(*pouch->compaction_namespaces));
    pouch->compaction_namespace_terminal =
        (unsigned char *)lc_alloc_with_allocator(
            &pouch->allocator,
            LC_POUCH_COMPACTION_QUEUE_MAX_NAMESPACES *
                sizeof(*pouch->compaction_namespace_terminal));
    if (pouch->compaction_namespaces == NULL ||
        pouch->compaction_namespace_terminal == NULL) {
      lc_free_with_allocator(&pouch->allocator, pouch->compaction_namespaces);
      lc_free_with_allocator(&pouch->allocator,
                             pouch->compaction_namespace_terminal);
      pouch->compaction_namespaces = NULL;
      pouch->compaction_namespace_terminal = NULL;
      pthread_mutex_unlock(&pouch->compaction_mutex);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction queue", NULL,
                          NULL, NULL);
    }
    pouch->compaction_namespace_capacity =
        LC_POUCH_COMPACTION_QUEUE_MAX_NAMESPACES;
  }
  if (pouch->compaction_namespace_count >=
      pouch->compaction_namespace_capacity) {
    pthread_mutex_unlock(&pouch->compaction_mutex);
    return LC_OK;
  }
  name_copy = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (name_copy == NULL) {
    pthread_mutex_unlock(&pouch->compaction_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction namespace", NULL, NULL,
                        NULL);
  }
  pouch->compaction_namespaces[pouch->compaction_namespace_count] = name_copy;
  pouch->compaction_namespace_terminal[pouch->compaction_namespace_count] =
      terminal ? 1U : 0U;
  ++pouch->compaction_namespace_count;
  pthread_mutex_unlock(&pouch->compaction_mutex);
  if (queued != NULL) {
    *queued = 1;
  }
  return LC_OK;
}

static int lc_pouch_terminal_reclaim_directory(lc_pouch *pouch, int create,
                                               char **out, lc_error *error) {
  char *control_path;
  char *marker_path;
  int rc;

  *out = NULL;
  control_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  marker_path = control_path != NULL
                    ? lc_pouch_path_join(&pouch->allocator, control_path,
                                         "terminal-reclaim")
                    : NULL;
  if (control_path == NULL || marker_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, control_path);
    lc_free_with_allocator(&pouch->allocator, marker_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch terminal reclaim path", NULL,
                        NULL, "pouch");
  }
  rc = LC_OK;
  if (create) {
    rc = lc_pouch_path_ensure_directory(
        control_path, "failed to create pouch control directory", error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          marker_path, "failed to create pouch terminal reclaim directory",
          error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, control_path);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, marker_path);
    return rc;
  }
  *out = marker_path;
  return LC_OK;
}

static int lc_pouch_terminal_reclaim_marker_path(lc_pouch *pouch,
                                                 const char *ns, int create,
                                                 char **directory, char **path,
                                                 lc_error *error) {
  char *leaf;
  int rc;

  *directory = NULL;
  *path = NULL;
  rc = lc_pouch_terminal_reclaim_directory(pouch, create, directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  if (leaf == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory);
    *directory = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch terminal reclaim namespace",
                        NULL, NULL, "pouch");
  }
  *path = lc_pouch_path_join(&pouch->allocator, *directory, leaf);
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (*path == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory);
    *directory = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch terminal reclaim marker",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_terminal_reclaim_marker_write(lc_pouch *pouch,
                                                  const char *ns,
                                                  lc_error *error) {
  char *directory;
  char *path;
  int fd;
  int rc;

  directory = NULL;
  path = NULL;
  rc = lc_pouch_terminal_reclaim_marker_path(pouch, ns, 1, &directory, &path,
                                             error);
  if (rc == LC_OK) {
    fd = open(path, O_WRONLY | O_CREAT, 0666);
    if (fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch terminal reclaim marker",
                        strerror(errno), path, "pouch");
    } else {
      if (fsync(fd) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to sync pouch terminal reclaim marker",
                          strerror(errno), path, "pouch");
      }
      if (close(fd) != 0 && rc == LC_OK) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to close pouch terminal reclaim marker",
                          strerror(errno), path, "pouch");
      }
      if (rc == LC_OK) {
        rc = lc_pouch_path_fsync_directory(
            directory, "failed to sync pouch terminal reclaim directory",
            error);
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

static int lc_pouch_terminal_reclaim_marker_take(lc_pouch *pouch, char **ns,
                                                 lc_error *error) {
  char *directory;
  char *after_cursor_leaf;
  char *after_cursor_namespace;
  char *first_leaf;
  char *first_namespace;
  DIR *dir;
  struct dirent *entry;
  int rc;

  *ns = NULL;
  directory = NULL;
  first_leaf = NULL;
  first_namespace = NULL;
  after_cursor_leaf = NULL;
  after_cursor_namespace = NULL;
  rc = lc_pouch_terminal_reclaim_directory(pouch, 0, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(directory);
  if (dir == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to open pouch terminal reclaim directory",
                            strerror(errno), directory, "pouch");
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      char *entry_namespace;
      char *entry_leaf;
      int select_after_cursor;
      int select_first;

      select_first =
          first_leaf == NULL || strcmp(entry->d_name, first_leaf) < 0;
      select_after_cursor =
          pouch->terminal_reclaim_marker_cursor != NULL &&
          strcmp(entry->d_name, pouch->terminal_reclaim_marker_cursor) > 0 &&
          (after_cursor_leaf == NULL ||
           strcmp(entry->d_name, after_cursor_leaf) < 0);
      if (!select_first && !select_after_cursor) {
        continue;
      }
      entry_namespace =
          lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
      if (entry_namespace == NULL) {
        continue;
      }
      if (select_first) {
        entry_leaf = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
        if (entry_leaf != NULL) {
          lc_free_with_allocator(&pouch->allocator, first_leaf);
          lc_free_with_allocator(&pouch->allocator, first_namespace);
          first_leaf = entry_leaf;
          first_namespace = entry_namespace;
          entry_namespace = NULL;
        }
      }
      if (select_after_cursor) {
        if (entry_namespace == NULL) {
          entry_namespace =
              lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
        }
        entry_leaf = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
        if (entry_namespace != NULL && entry_leaf != NULL) {
          lc_free_with_allocator(&pouch->allocator, after_cursor_leaf);
          lc_free_with_allocator(&pouch->allocator, after_cursor_namespace);
          after_cursor_leaf = entry_leaf;
          after_cursor_namespace = entry_namespace;
          entry_namespace = NULL;
        } else {
          lc_free_with_allocator(&pouch->allocator, entry_leaf);
          lc_free_with_allocator(&pouch->allocator, entry_namespace);
          entry_namespace = NULL;
        }
      }
      lc_free_with_allocator(&pouch->allocator, entry_namespace);
    }
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch terminal reclaim directory",
                      strerror(errno), directory, "pouch");
  }
  if (rc == LC_OK) {
    if (after_cursor_namespace != NULL) {
      *ns = after_cursor_namespace;
      after_cursor_namespace = NULL;
      lc_free_with_allocator(&pouch->allocator,
                             pouch->terminal_reclaim_marker_cursor);
      pouch->terminal_reclaim_marker_cursor = after_cursor_leaf;
      after_cursor_leaf = NULL;
    } else if (first_namespace != NULL) {
      *ns = first_namespace;
      first_namespace = NULL;
      lc_free_with_allocator(&pouch->allocator,
                             pouch->terminal_reclaim_marker_cursor);
      pouch->terminal_reclaim_marker_cursor = first_leaf;
      first_leaf = NULL;
    }
  }
  lc_free_with_allocator(&pouch->allocator, after_cursor_leaf);
  lc_free_with_allocator(&pouch->allocator, after_cursor_namespace);
  lc_free_with_allocator(&pouch->allocator, first_leaf);
  lc_free_with_allocator(&pouch->allocator, first_namespace);
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}

int lc_pouch_terminal_reclaim_marker_remove(lc_pouch *pouch, const char *ns,
                                            lc_error *error) {
  char *directory;
  char *path;
  int rc;

  directory = NULL;
  path = NULL;
  rc = lc_pouch_terminal_reclaim_marker_path(pouch, ns, 0, &directory, &path,
                                             error);
  if (rc == LC_OK) {
    if (unlink(path) != 0 && errno != ENOENT) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to remove pouch terminal reclaim marker",
                        strerror(errno), path, "pouch");
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          directory, "failed to sync pouch terminal reclaim directory", error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

#define LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC "LOR1"
#define LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES 12U
#define LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES 4096U

static int lc_pouch_outbox_state_reclaim_marker_id_valid(const char *id) {
  const unsigned char *cursor;

  if (id == NULL || id[0] == '\0') {
    return 0;
  }
  cursor = (const unsigned char *)id;
  while (*cursor != '\0') {
    if (!((*cursor >= (unsigned char)'A' && *cursor <= (unsigned char)'Z') ||
          (*cursor >= (unsigned char)'a' && *cursor <= (unsigned char)'z') ||
          (*cursor >= (unsigned char)'0' && *cursor <= (unsigned char)'9') ||
          *cursor == (unsigned char)'_' || *cursor == (unsigned char)'-')) {
      return 0;
    }
    ++cursor;
  }
  return 1;
}

static int lc_pouch_outbox_state_reclaim_directory(lc_pouch *pouch,
                                                   const char *ns, int create,
                                                   char **out,
                                                   lc_error *error) {
  char *control;
  char *root;
  char *leaf;
  char *directory;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim requires namespace and "
                        "output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  control = lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  root = control != NULL ? lc_pouch_path_join(&pouch->allocator, control,
                                              "outbox-state-reclaim")
                         : NULL;
  leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  directory = root != NULL && leaf != NULL
                  ? lc_pouch_path_join(&pouch->allocator, root, leaf)
                  : NULL;
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (control == NULL || root == NULL || directory == NULL) {
    lc_free_with_allocator(&pouch->allocator, control);
    lc_free_with_allocator(&pouch->allocator, root);
    lc_free_with_allocator(&pouch->allocator, directory);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox state reclaim path",
                        NULL, NULL, "pouch");
  }
  rc = LC_OK;
  if (create) {
    rc = lc_pouch_path_ensure_directory(
        control, "failed to create pouch control directory", error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          root, "failed to create pouch outbox state reclaim directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          directory, "failed to create pouch outbox state reclaim namespace",
          error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, control);
  lc_free_with_allocator(&pouch->allocator, root);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  *out = directory;
  return LC_OK;
}

static int lc_pouch_outbox_state_reclaim_marker_path(
    lc_pouch *pouch, const char *ns, const char *marker_id, int create,
    char **directory_out, char **path_out, lc_error *error) {
  char *directory;
  char *path;
  int rc;

  if (directory_out == NULL || path_out == NULL ||
      !lc_pouch_outbox_state_reclaim_marker_id_valid(marker_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox state reclaim marker", NULL, NULL,
                        "pouch");
  }
  *directory_out = NULL;
  *path_out = NULL;
  directory = NULL;
  path = NULL;
  rc = lc_pouch_outbox_state_reclaim_directory(pouch, ns, create, &directory,
                                               error);
  if (rc == LC_OK) {
    path = lc_pouch_path_join(&pouch->allocator, directory, marker_id);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  *directory_out = directory;
  *path_out = path;
  return LC_OK;
}

static int lc_pouch_outbox_state_reclaim_write_all(int fd,
                                                   const unsigned char *bytes,
                                                   size_t length,
                                                   lc_error *error) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    ssize_t written = write(fd, bytes + offset, length - offset);

    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch outbox state reclaim marker",
                          strerror(errno), NULL, "pouch");
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "pouch outbox state reclaim marker short write", NULL,
                          NULL, "pouch");
    }
    offset += (size_t)written;
  }
  return LC_OK;
}

static unsigned long
lc_pouch_outbox_state_reclaim_u32_read(const unsigned char *bytes) {
  return ((unsigned long)bytes[0] << 24U) | ((unsigned long)bytes[1] << 16U) |
         ((unsigned long)bytes[2] << 8U) | (unsigned long)bytes[3];
}

static void lc_pouch_outbox_state_reclaim_u32_write(unsigned char *bytes,
                                                    unsigned long value) {
  bytes[0] = (unsigned char)((value >> 24U) & 0xffUL);
  bytes[1] = (unsigned char)((value >> 16U) & 0xffUL);
  bytes[2] = (unsigned char)((value >> 8U) & 0xffUL);
  bytes[3] = (unsigned char)(value & 0xffUL);
}

int lc_pouch_outbox_state_reclaim_marker_write(lc_pouch *pouch, const char *ns,
                                               const char *marker_id,
                                               const char *outbox_key,
                                               const char *state_key,
                                               lc_error *error) {
  unsigned char *contents;
  char *directory;
  char *path;
  char *temporary;
  size_t outbox_length;
  size_t state_length;
  size_t contents_length;
  size_t temporary_length;
  unsigned long attempt;
  int fd;
  int rc;

  if (outbox_key == NULL || outbox_key[0] == '\0' || state_key == NULL ||
      state_key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim marker requires keys", NULL,
                        NULL, "pouch");
  }
  outbox_length = strlen(outbox_key);
  state_length = strlen(state_key);
  if (outbox_length > 0xffffffffUL || state_length > 0xffffffffUL ||
      outbox_length + state_length >
          LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES -
              LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim marker is too large", NULL,
                        NULL, "pouch");
  }
  directory = NULL;
  path = NULL;
  temporary = NULL;
  contents = NULL;
  fd = -1;
  rc = lc_pouch_outbox_state_reclaim_marker_path(pouch, ns, marker_id, 1,
                                                 &directory, &path, error);
  contents_length =
      LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES + outbox_length + state_length;
  if (rc == LC_OK) {
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        contents_length);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(contents, LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC, 4U);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 4U,
                                            (unsigned long)outbox_length);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 8U,
                                            (unsigned long)state_length);
    memcpy(contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES, outbox_key,
           outbox_length);
    memcpy(contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES +
               outbox_length,
           state_key, state_length);
    temporary_length = strlen(path) + 64U;
    temporary =
        (char *)lc_alloc_with_allocator(&pouch->allocator, temporary_length);
    if (temporary == NULL) {
      rc = lc_error_set(
          error, LC_ERR_NOMEM, 0L,
          "failed to allocate pouch outbox state reclaim temporary", NULL, NULL,
          "pouch");
    }
  }
  for (attempt = 0UL; rc == LC_OK && attempt < 32UL; ++attempt) {
    snprintf(temporary, temporary_length, "%s.tmp.%ld.%lu", path,
             (long)getpid(), attempt);
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd >= 0) {
      break;
    }
    if (errno != EEXIST) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch outbox state reclaim marker",
                        strerror(errno), temporary, "pouch");
    }
  }
  if (rc == LC_OK && fd < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to allocate pouch outbox state reclaim temporary",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_outbox_state_reclaim_write_all(fd, contents, contents_length,
                                                 error);
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to sync pouch outbox state reclaim marker",
                      strerror(errno), temporary, "pouch");
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch outbox state reclaim marker",
                      strerror(errno), temporary, "pouch");
  }
  fd = -1;
  if (rc == LC_OK && rename(temporary, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to publish pouch outbox state reclaim marker",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch outbox state reclaim directory",
        error);
  }
  if (rc != LC_OK && temporary != NULL) {
    (void)unlink(temporary);
  }
  lc_free_with_allocator(&pouch->allocator, contents);
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  lc_free_with_allocator(&pouch->allocator, temporary);
  return rc;
}

static int lc_pouch_outbox_state_reclaim_read_all(int fd, unsigned char *bytes,
                                                  size_t length,
                                                  lc_error *error) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    ssize_t got = read(fd, bytes + offset, length - offset);

    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch outbox state reclaim marker",
                          strerror(errno), NULL, "pouch");
    }
    if (got == 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "truncated pouch outbox state reclaim marker", NULL,
                          NULL, "pouch");
    }
    offset += (size_t)got;
  }
  return LC_OK;
}

int lc_pouch_outbox_state_reclaim_marker_take(lc_pouch *pouch, const char *ns,
                                              const char *after_marker_id,
                                              char **marker_id_out,
                                              char **outbox_key_out,
                                              char **state_key_out,
                                              lc_error *error) {
  char *directory;
  char *first;
  char *after;
  char *selected;
  char *path;
  unsigned char *contents;
  struct dirent *entry;
  struct stat st;
  DIR *dir;
  size_t contents_length;
  unsigned long outbox_length;
  unsigned long state_length;
  int fd;
  int rc;

  if (marker_id_out == NULL || outbox_key_out == NULL ||
      state_key_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim take requires outputs",
                        NULL, NULL, "pouch");
  }
  *marker_id_out = NULL;
  *outbox_key_out = NULL;
  *state_key_out = NULL;
  directory = NULL;
  first = NULL;
  after = NULL;
  selected = NULL;
  path = NULL;
  contents = NULL;
  dir = NULL;
  fd = -1;
  rc = lc_pouch_outbox_state_reclaim_directory(pouch, ns, 0, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(directory);
  if (dir == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_error_set(
                   error, LC_ERR_TRANSPORT, 0L,
                   "failed to open pouch outbox state reclaim directory",
                   strerror(errno), directory, "pouch");
    goto cleanup;
  }
  while ((entry = readdir(dir)) != NULL) {
    if (!lc_pouch_outbox_state_reclaim_marker_id_valid(entry->d_name)) {
      continue;
    }
    if (first == NULL || strcmp(entry->d_name, first) < 0) {
      char *copy = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);

      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain pouch outbox state reclaim marker",
                          NULL, NULL, "pouch");
        goto cleanup;
      }
      lc_free_with_allocator(&pouch->allocator, first);
      first = copy;
    }
    if (after_marker_id != NULL && after_marker_id[0] != '\0' &&
        strcmp(entry->d_name, after_marker_id) > 0 &&
        (after == NULL || strcmp(entry->d_name, after) < 0)) {
      char *copy = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);

      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain pouch outbox state reclaim marker",
                          NULL, NULL, "pouch");
        goto cleanup;
      }
      lc_free_with_allocator(&pouch->allocator, after);
      after = copy;
    }
  }
  selected = after != NULL ? after : first;
  if (selected != NULL) {
    if (selected == after) {
      after = NULL;
    } else {
      first = NULL;
    }
    path = lc_pouch_path_join(&pouch->allocator, directory, selected);
    if (path == NULL || lstat(path, &st) != 0 || !S_ISREG(st.st_mode) ||
        st.st_size < (off_t)LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES ||
        st.st_size > (off_t)LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES) {
      rc = path == NULL
               ? lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch outbox state reclaim "
                              "marker path",
                              NULL, NULL, "pouch")
               : lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                              "invalid pouch outbox state reclaim marker", NULL,
                              NULL, "pouch");
      goto cleanup;
    }
    contents_length = (size_t)st.st_size;
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        contents_length);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to read pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
      goto cleanup;
    }
    fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      rc =
          errno == ENOENT
              ? LC_OK
              : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                             "failed to open pouch outbox state reclaim marker",
                             strerror(errno), path, "pouch");
      goto cleanup;
    }
    rc = lc_pouch_outbox_state_reclaim_read_all(fd, contents, contents_length,
                                                error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    if (memcmp(contents, LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC, 4U) != 0) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox state reclaim marker", NULL, NULL,
                        "pouch");
      goto cleanup;
    }
    outbox_length = lc_pouch_outbox_state_reclaim_u32_read(contents + 4U);
    state_length = lc_pouch_outbox_state_reclaim_u32_read(contents + 8U);
    if (outbox_length == 0UL || state_length == 0UL ||
        outbox_length >
            contents_length - LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES ||
        state_length != contents_length -
                            LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES -
                            outbox_length) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox state reclaim marker lengths",
                        NULL, NULL, "pouch");
      goto cleanup;
    }
    *marker_id_out = selected;
    selected = NULL;
    *outbox_key_out = (char *)lc_alloc_with_allocator(
        &pouch->allocator, (size_t)outbox_length + 1U);
    *state_key_out = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                                     (size_t)state_length + 1U);
    if (*outbox_key_out == NULL || *state_key_out == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
      goto cleanup;
    }
    memcpy(*outbox_key_out,
           contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES,
           (size_t)outbox_length);
    (*outbox_key_out)[outbox_length] = '\0';
    memcpy(*state_key_out,
           contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES +
               outbox_length,
           (size_t)state_length);
    (*state_key_out)[state_length] = '\0';
  }

cleanup:
  if (fd >= 0) {
    (void)close(fd);
  }
  if (dir != NULL) {
    (void)closedir(dir);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, *marker_id_out);
    lc_free_with_allocator(&pouch->allocator, *outbox_key_out);
    lc_free_with_allocator(&pouch->allocator, *state_key_out);
    *marker_id_out = NULL;
    *outbox_key_out = NULL;
    *state_key_out = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, first);
  lc_free_with_allocator(&pouch->allocator, after);
  lc_free_with_allocator(&pouch->allocator, selected);
  lc_free_with_allocator(&pouch->allocator, path);
  lc_free_with_allocator(&pouch->allocator, contents);
  return rc;
}

int lc_pouch_outbox_state_reclaim_marker_remove(lc_pouch *pouch, const char *ns,
                                                const char *marker_id,
                                                lc_error *error) {
  char *directory;
  char *path;
  int rc;

  directory = NULL;
  path = NULL;
  rc = lc_pouch_outbox_state_reclaim_marker_path(pouch, ns, marker_id, 0,
                                                 &directory, &path, error);
  if (rc == LC_OK && unlink(path) != 0) {
    if (errno == ENOENT) {
      /* The marker directory is created only for stateful completion. A
       * normal stateless dead-letter has nothing to retire, so there is no
       * directory entry to persist. */
      goto cleanup;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to remove pouch outbox state reclaim marker",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch outbox state reclaim directory",
        error);
  }
cleanup:
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

/* Each outbox retention family has its own small control directory. It is
 * deliberately outside the namespace log so reclaim never needs the query
 * engine, a namespace visit, or a derived-index warmup. */
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC "LDR1"
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES 24U
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES 8192U
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING (-1002)

static void lc_pouch_outbox_dead_letter_reclaim_u64_write(unsigned char *bytes,
                                                          uint64_t value) {
  unsigned int index;

  for (index = 0U; index < 8U; ++index) {
    bytes[7U - index] = (unsigned char)(value & 0xffU);
    value >>= 8U;
  }
}

static uint64_t
lc_pouch_outbox_dead_letter_reclaim_u64_read(const unsigned char *bytes) {
  uint64_t value;
  unsigned int index;

  value = 0U;
  for (index = 0U; index < 8U; ++index)
    value = (value << 8U) | (uint64_t)bytes[index];
  return value;
}

static int lc_pouch_outbox_retention_directory(lc_pouch *pouch, const char *ns,
                                               int kind, int create, char **out,
                                               lc_error *error) {
  static const char *const names[] = {
      "outbox-dead-letter-reclaim", "outbox-completed-reclaim",
      "outbox-inbox-reclaim", "outbox-command-reclaim"};
  char *control;
  char *root;
  char *leaf;
  char *directory;
  int rc;

  if (kind < 0 || kind >= LC_POUCH_RETENTION_KIND_COUNT || pouch == NULL ||
      ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention requires namespace", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  control = lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  root = control != NULL
             ? lc_pouch_path_join(&pouch->allocator, control, names[kind])
             : NULL;
  leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  directory = root != NULL && leaf != NULL
                  ? lc_pouch_path_join(&pouch->allocator, root, leaf)
                  : NULL;
  if (control == NULL || root == NULL || leaf == NULL || directory == NULL) {
    lc_free_with_allocator(&pouch->allocator, control);
    lc_free_with_allocator(&pouch->allocator, root);
    lc_free_with_allocator(&pouch->allocator, leaf);
    lc_free_with_allocator(&pouch->allocator, directory);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox retention path", NULL,
                        NULL, "pouch");
  }
  rc = LC_OK;
  if (create) {
    rc = lc_pouch_path_ensure_directory(
        control, "failed to create pouch control directory", error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          pouch->root_path,
          "failed to sync pouch root outbox retention directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          root, "failed to create pouch outbox retention directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          control,
          "failed to sync pouch outbox retention control "
          "directory",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          directory,
          "failed to create pouch outbox retention namespace directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          root, "failed to sync pouch outbox retention namespace root", error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, control);
  lc_free_with_allocator(&pouch->allocator, root);
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  *out = directory;
  return LC_OK;
}

static int lc_pouch_outbox_retention_marker_path(
    lc_pouch *pouch, const char *ns, int kind, const char *marker_id,
    lc_pouch_unix_seconds due, int create, char **directory_out,
    char **path_out, lc_error *error) {
  char filename[17U + LC_XID_STRING_SIZE];
  unsigned int index;
  uint64_t deadline;
  char *directory;
  char *path;
  int rc;

  if (marker_id == NULL || !lc_xid_is_valid(marker_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
  }
  *directory_out = NULL;
  *path_out = NULL;
  directory = NULL;
  path = NULL;
  rc = lc_pouch_outbox_retention_directory(pouch, ns, kind, create, &directory,
                                           error);
  if (rc == LC_OK) {
    if (kind != LC_POUCH_RETENTION_DEAD_LETTER) {
      if (due <= 0) {
        lc_free_with_allocator(&pouch->allocator, directory);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "terminal retention requires a positive deadline",
                            NULL, NULL, "pouch");
      }
      deadline = (uint64_t)due;
      for (index = 0U; index < 16U; ++index) {
        filename[15U - index] = "0123456789abcdef"[deadline & 15U];
        deadline >>= 4U;
      }
      filename[16] = '-';
      strcpy(filename + 17U, marker_id);
      marker_id = filename;
    }
    path = lc_pouch_path_join(&pouch->allocator, directory, marker_id);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox retention marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  *directory_out = directory;
  *path_out = path;
  return LC_OK;
}

void lc_pouch_outbox_retention_marker_cleanup(
    lc_pouch *pouch, lc_pouch_outbox_retention_marker *marker) {
  if (pouch == NULL || marker == NULL)
    return;
  lc_free_with_allocator(&pouch->allocator, marker->marker_id);
  lc_free_with_allocator(&pouch->allocator, marker->outbox_key);
  memset(marker, 0, sizeof(*marker));
}

static int lc_pouch_outbox_retention_marker_clone(
    lc_pouch *pouch, const lc_pouch_outbox_retention_marker *source,
    lc_pouch_outbox_retention_marker *out, lc_error *error) {
  if (pouch == NULL || source == NULL || out == NULL ||
      source->marker_id == NULL || source->outbox_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
  }
  memset(out, 0, sizeof(*out));
  out->marker_id =
      lc_strdup_with_allocator(&pouch->allocator, source->marker_id);
  out->outbox_key =
      lc_strdup_with_allocator(&pouch->allocator, source->outbox_key);
  if (out->marker_id == NULL || out->outbox_key == NULL) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox retention marker", NULL,
                        NULL, "pouch");
  }
  out->due_at_unix = source->due_at_unix;
  out->retained_bytes = source->retained_bytes;
  return LC_OK;
}

static int lc_pouch_outbox_retention_marker_keep_lexical_first(
    lc_pouch *pouch, const lc_pouch_outbox_retention_marker *candidate,
    lc_pouch_outbox_retention_marker *selected, lc_error *error) {
  int rc;

  if (selected->marker_id != NULL &&
      strcmp(selected->marker_id, candidate->marker_id) <= 0) {
    return LC_OK;
  }
  lc_pouch_outbox_retention_marker_cleanup(pouch, selected);
  rc =
      lc_pouch_outbox_retention_marker_clone(pouch, candidate, selected, error);
  return rc;
}

static int lc_pouch_outbox_retention_marker_read(
    lc_pouch *pouch, const char *directory, const char *filename,
    lc_pouch_outbox_retention_marker *out, lc_error *error) {
  const char *marker_id =
      strlen(filename) == LC_XID_STRING_SIZE - 1U ? filename : filename + 17U;
  unsigned char *contents;
  char *path;
  struct stat st;
  uint64_t due_bits;
  uint64_t retained_bytes;
  unsigned long key_length;
  int fd;
  int rc;

  if (pouch == NULL || directory == NULL || marker_id == NULL || out == NULL ||
      !lc_xid_is_valid(marker_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
  }
  memset(out, 0, sizeof(*out));
  contents = NULL;
  path = lc_pouch_path_join(&pouch->allocator, directory, filename);
  fd = -1;
  rc = path == NULL
           ? lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch outbox retention path",
                          NULL, NULL, "pouch")
           : LC_OK;
  if (rc == LC_OK && lstat(path, &st) != 0) {
    rc = errno == ENOENT
             ? LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING
             : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to inspect pouch outbox retention "
                            "marker",
                            strerror(errno), path, "pouch");
  }
  if (rc == LC_OK &&
      (!S_ISREG(st.st_mode) ||
       st.st_size < (off_t)LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES ||
       st.st_size > (off_t)LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES)) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "invalid pouch outbox retention marker", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        (size_t)st.st_size);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to read pouch outbox retention marker", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      rc = errno == ENOENT
               ? LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING
               : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                              "failed to open pouch outbox retention marker",
                              strerror(errno), path, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_outbox_state_reclaim_read_all(fd, contents,
                                                (size_t)st.st_size, error);
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch outbox retention marker",
                      strerror(errno), path, "pouch");
  }
  fd = -1;
  if (rc == LC_OK &&
      memcmp(contents, LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC, 4U) != 0) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "invalid pouch outbox retention marker", NULL, NULL,
                      "pouch");
  }
  due_bits = rc == LC_OK
                 ? lc_pouch_outbox_dead_letter_reclaim_u64_read(contents + 4U)
                 : 0U;
  retained_bytes =
      rc == LC_OK ? lc_pouch_outbox_dead_letter_reclaim_u64_read(contents + 12U)
                  : 0U;
  key_length = rc == LC_OK
                   ? lc_pouch_outbox_state_reclaim_u32_read(contents + 20U)
                   : 0UL;
  if (rc == LC_OK &&
      (key_length == 0UL ||
       key_length !=
           (unsigned long)((size_t)st.st_size -
                           LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES))) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "invalid pouch outbox retention marker", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    out->marker_id = lc_strdup_with_allocator(&pouch->allocator, marker_id);
    out->outbox_key = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                                      (size_t)key_length + 1U);
    if (out->marker_id == NULL || out->outbox_key == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox retention marker", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(out->outbox_key,
           contents + LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES,
           (size_t)key_length);
    out->outbox_key[key_length] = '\0';
    out->due_at_unix = (lc_pouch_unix_seconds)(int64_t)due_bits;
    out->retained_bytes = retained_bytes;
    if (out->due_at_unix <= 0 || out->outbox_key[0] == '\0') {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
    }
  }
  if (rc != LC_OK)
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
  lc_free_with_allocator(&pouch->allocator, contents);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

int lc_pouch_outbox_retention_marker_write(lc_pouch *pouch, const char *ns,
                                           int kind, const char *marker_id,
                                           const char *outbox_key,
                                           lc_pouch_unix_seconds due_at_unix,
                                           uint64_t retained_bytes,
                                           lc_error *error) {
  unsigned char *contents;
  char *directory;
  char *path;
  char *temporary;
  size_t key_length;
  size_t contents_length;
  size_t temporary_length;
  unsigned long attempt;
  int fd;
  int rc;

  if (outbox_key == NULL || outbox_key[0] == '\0' || due_at_unix <= 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention marker requires key and "
                        "deadline",
                        NULL, NULL, "pouch");
  }
  key_length = strlen(outbox_key);
  if (key_length > 0xffffffffUL ||
      key_length > LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES -
                       LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention marker is too large", NULL,
                        NULL, "pouch");
  }
  directory = NULL;
  path = NULL;
  temporary = NULL;
  contents = NULL;
  fd = -1;
  rc = lc_pouch_outbox_retention_marker_path(
      pouch, ns, kind, marker_id, due_at_unix, 1, &directory, &path, error);
  contents_length =
      LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES + key_length;
  if (rc == LC_OK) {
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        contents_length);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox retention marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(contents, LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC, 4U);
    lc_pouch_outbox_dead_letter_reclaim_u64_write(
        contents + 4U, (uint64_t)(int64_t)due_at_unix);
    lc_pouch_outbox_dead_letter_reclaim_u64_write(contents + 12U,
                                                  retained_bytes);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 20U,
                                            (unsigned long)key_length);
    memcpy(contents + LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES,
           outbox_key, key_length);
    temporary_length = strlen(path) + 64U;
    temporary =
        (char *)lc_alloc_with_allocator(&pouch->allocator, temporary_length);
    if (temporary == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox retention temporary",
                        NULL, NULL, "pouch");
    }
  }
  for (attempt = 0UL; rc == LC_OK && attempt < 32UL; ++attempt) {
    snprintf(temporary, temporary_length, "%s.tmp.%ld.%lu", path,
             (long)getpid(), attempt);
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd >= 0)
      break;
    if (errno != EEXIST) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch outbox retention marker",
                        strerror(errno), temporary, "pouch");
    }
  }
  if (rc == LC_OK && fd < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to allocate pouch outbox retention temporary",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_outbox_state_reclaim_write_all(fd, contents, contents_length,
                                                 error);
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to sync pouch outbox retention marker",
                      strerror(errno), temporary, "pouch");
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch outbox retention marker",
                      strerror(errno), temporary, "pouch");
  }
  fd = -1;
  if (rc == LC_OK && rename(temporary, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to publish pouch outbox retention marker",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch outbox retention directory", error);
  }
  if (rc != LC_OK && temporary != NULL)
    (void)unlink(temporary);
  lc_free_with_allocator(&pouch->allocator, contents);
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  lc_free_with_allocator(&pouch->allocator, temporary);
  return rc;
}

static int lc_pouch_terminal_retention_filename(const char *name,
                                                lc_pouch_unix_seconds *due) {
  static const char hex[] = "0123456789abcdef";
  uint64_t value;
  unsigned int index;
  if (strlen(name) != 17U + LC_XID_STRING_SIZE - 1U || name[16] != '-' ||
      !lc_xid_is_valid(name + 17U))
    return 0;
  value = 0U;
  for (index = 0U; index < 16U; ++index) {
    const char *digit = strchr(hex, name[index]);
    if (digit == NULL)
      return 0;
    value = (value << 4U) | (uint64_t)(digit - hex);
  }
  if (value == 0U || value > (uint64_t)LC_I64_MAX)
    return 0;
  *due = (lc_pouch_unix_seconds)value;
  return 1;
}

static void lc_pouch_terminal_retention_keep_name(char *selected,
                                                  const char *candidate) {
  if (selected[0] == '\0' || strcmp(candidate + 17U, selected + 17U) < 0)
    strcpy(selected, candidate);
}

/* Directory names provide deadlines and counts without parsing retained
 * envelopes or warming any index. Keep a constant number of candidate names,
 * and read exactly one selected marker body. The turn cursor ensures that a
 * busy parent cannot indefinitely starve another writer's expired receipt. */
static int lc_pouch_terminal_retention_take(
    lc_pouch *pouch, const char *ns, int kind, const char *after,
    lc_pouch_unix_seconds now, size_t max_count,
    lc_pouch_outbox_retention_marker *out, lc_pouch_unix_seconds *next_due,
    lc_error *error) {
  char first[17U + LC_XID_STRING_SIZE];
  char later[17U + LC_XID_STRING_SIZE];
  char other[17U + LC_XID_STRING_SIZE];
  char due_first[17U + LC_XID_STRING_SIZE];
  char due_later[17U + LC_XID_STRING_SIZE];
  char *directory;
  const char *selected;
  DIR *dir;
  struct dirent *entry;
  size_t count;
  int rc;
  first[0] = later[0] = other[0] = due_first[0] = due_later[0] = '\0';
  count = 0U;
  directory = NULL;
  rc = lc_pouch_outbox_retention_directory(pouch, ns, kind, 0, &directory,
                                           error);
  if (rc != LC_OK)
    return rc;
  dir = opendir(directory);
  if (dir == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to open terminal retention directory",
                            strerror(errno), directory, "pouch");
    goto cleanup;
  }
  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    lc_pouch_unix_seconds due;
    const char *id;
    if (!lc_pouch_terminal_retention_filename(entry->d_name, &due))
      continue;
    id = entry->d_name + 17U;
    if (count != SIZE_MAX)
      ++count;
    if (*next_due == 0 || due < *next_due)
      *next_due = due;
    lc_pouch_terminal_retention_keep_name(first, entry->d_name);
    if (after != NULL && strcmp(id, after) > 0)
      lc_pouch_terminal_retention_keep_name(later, entry->d_name);
    if (after == NULL || strcmp(id, after) != 0)
      lc_pouch_terminal_retention_keep_name(other, entry->d_name);
    if (due <= now) {
      lc_pouch_terminal_retention_keep_name(due_first, entry->d_name);
      if (after != NULL && strcmp(id, after) > 0)
        lc_pouch_terminal_retention_keep_name(due_later, entry->d_name);
    }
  }
  if (errno != 0)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read terminal retention directory",
                      strerror(errno), directory, "pouch");
  if (closedir(dir) != 0 && rc == LC_OK)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close terminal retention directory",
                      strerror(errno), directory, "pouch");
  if (rc != LC_OK)
    goto cleanup;
  selected = max_count != SIZE_MAX && count > max_count
                 ? (later[0] != '\0'   ? later
                    : other[0] != '\0' ? other
                                       : first)
                 : (due_later[0] != '\0' ? due_later : due_first);
  if (selected[0] != '\0') {
    lc_pouch_unix_seconds expected_due = 0;
    lc_pouch_terminal_retention_filename(selected, &expected_due);
    rc = lc_pouch_outbox_retention_marker_read(pouch, directory, selected, out,
                                               error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    } else if (rc == LC_OK && out->due_at_unix != expected_due) {
      lc_pouch_outbox_retention_marker_cleanup(pouch, out);
      rc =
          lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "terminal retention filename and marker deadline differ",
                       NULL, selected, "pouch");
    }
    /* A body failure retains durable work but must not pin the advisory turn
     * forever. The validated filename supplies identity only, never a parent
     * key or authority to delete a receipt. */
    strcpy(out->selected_id, selected + 17U);
    out->queued_count = count;
  }
cleanup:
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}

/* Order identities within one advisory turn, wrapping only after all lexical
 * successors. An unreadable body participates by filename without supplying
 * a parent key or deletion authority. */
static int lc_pouch_retention_turn_precedes(const char *candidate,
                                            const char *selected,
                                            const char *after) {
  if (selected == NULL || selected[0] == '\0')
    return 1;
  if (after != NULL) {
    int candidate_after = strcmp(candidate, after) > 0;
    int selected_after = strcmp(selected, after) > 0;
    if (candidate_after != selected_after)
      return candidate_after;
  }
  return strcmp(candidate, selected) < 0;
}

static int lc_pouch_retention_marker_failure_turn(
    lc_pouch *pouch, lc_pouch_outbox_retention_marker *marker,
    const char *filename, const char *after, char *failed_name, int rc,
    lc_error *error) {
  if (rc != LC_ERR_PROTOCOL && rc != LC_ERR_TRANSPORT)
    return rc;
  if (lc_pouch_retention_turn_precedes(filename, failed_name, after))
    strcpy(failed_name, filename);
  lc_pouch_outbox_retention_marker_cleanup(pouch, marker);
  lc_error_cleanup(error);
  lc_error_init(error);
  return LC_OK;
}

int lc_pouch_outbox_retention_marker_take(lc_pouch *pouch, const char *ns,
                                          int kind, const char *after_marker_id,
                                          lc_pouch_unix_seconds now_unix,
                                          size_t max_count, uint64_t max_bytes,
                                          lc_pouch_outbox_retention_marker *out,
                                          lc_pouch_unix_seconds *next_due_unix,
                                          lc_error *error) {
  char *directory;
  DIR *dir;
  struct dirent *entry;
  lc_pouch_outbox_retention_marker oldest;
  lc_pouch_outbox_retention_marker oldest_after;
  lc_pouch_outbox_retention_marker oldest_other;
  lc_pouch_outbox_retention_marker earliest;
  lc_pouch_outbox_retention_marker due_first;
  lc_pouch_outbox_retention_marker due_after;
  char later_name[LC_XID_STRING_SIZE];
  char other_name[LC_XID_STRING_SIZE];
  char failed_name[LC_XID_STRING_SIZE];
  size_t count;
  uint64_t bytes;
  int capacity_exceeded;
  int rc;

  if (out == NULL || next_due_unix == NULL || now_unix <= 0 ||
      (after_marker_id != NULL && !lc_xid_is_valid(after_marker_id))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention take requires outputs", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  *next_due_unix = 0;
  if (kind != LC_POUCH_RETENTION_DEAD_LETTER)
    return lc_pouch_terminal_retention_take(pouch, ns, kind, after_marker_id,
                                            now_unix, max_count, out,
                                            next_due_unix, error);
  memset(&oldest, 0, sizeof(oldest));
  memset(&oldest_after, 0, sizeof(oldest_after));
  memset(&oldest_other, 0, sizeof(oldest_other));
  memset(&earliest, 0, sizeof(earliest));
  memset(&due_first, 0, sizeof(due_first));
  memset(&due_after, 0, sizeof(due_after));
  later_name[0] = '\0';
  other_name[0] = '\0';
  failed_name[0] = '\0';
  directory = NULL;
  dir = NULL;
  count = 0U;
  bytes = 0U;
  capacity_exceeded = 0;
  rc = lc_pouch_outbox_retention_directory(pouch, ns, kind, 0, &directory,
                                           error);
  if (rc != LC_OK)
    goto cleanup;
  dir = opendir(directory);
  if (dir == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to open pouch outbox retention directory",
                            strerror(errno), directory, "pouch");
    goto cleanup;
  }
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    lc_pouch_outbox_retention_marker candidate;

    if (!lc_xid_is_valid(entry->d_name))
      continue;
    /* The capacity decision needs only a bounded prefix of marker bodies.
     * Finish the directory-name walk to find the true lexical successor;
     * choosing the first successor in readdir order can cycle among blocked
     * parents and starve a marker later in that order. */
    if (capacity_exceeded && after_marker_id != NULL) {
      if (strcmp(entry->d_name, after_marker_id) > 0 &&
          (later_name[0] == '\0' || strcmp(entry->d_name, later_name) < 0))
        strcpy(later_name, entry->d_name);
      if (strcmp(entry->d_name, after_marker_id) != 0 &&
          (other_name[0] == '\0' || strcmp(entry->d_name, other_name) < 0))
        strcpy(other_name, entry->d_name);
      continue;
    }
    memset(&candidate, 0, sizeof(candidate));
    rc = lc_pouch_outbox_retention_marker_read(pouch, directory, entry->d_name,
                                               &candidate, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      continue;
    }
    if (rc != LC_OK) {
      rc = lc_pouch_retention_marker_failure_turn(
          pouch, &candidate, entry->d_name, after_marker_id, failed_name, rc,
          error);
      lc_pouch_outbox_retention_marker_cleanup(pouch, &candidate);
      if (rc != LC_OK)
        break;
      /* Damaged markers still consume count capacity. Their byte accounting
       * and parent identity are untrusted until a later successful read. */
      if (count != SIZE_MAX)
        ++count;
      continue;
    }
    if (count != SIZE_MAX)
      ++count;
    if (~(uint64_t)0U - bytes < candidate.retained_bytes)
      bytes = ~(uint64_t)0U;
    else
      bytes += candidate.retained_bytes;
    rc = lc_pouch_outbox_retention_marker_keep_lexical_first(pouch, &candidate,
                                                             &oldest, error);
    if (rc == LC_OK && after_marker_id != NULL &&
        strcmp(candidate.marker_id, after_marker_id) > 0) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &oldest_after, error);
    }
    if (rc == LC_OK && after_marker_id != NULL &&
        strcmp(candidate.marker_id, after_marker_id) != 0) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &oldest_other, error);
    }
    if (rc == LC_OK && (earliest.marker_id == NULL ||
                        candidate.due_at_unix < earliest.due_at_unix)) {
      lc_pouch_outbox_retention_marker_cleanup(pouch, &earliest);
      rc = lc_pouch_outbox_retention_marker_clone(pouch, &candidate, &earliest,
                                                  error);
    }
    if (rc == LC_OK && candidate.due_at_unix <= now_unix) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &due_first, error);
      if (rc == LC_OK && after_marker_id != NULL &&
          strcmp(candidate.marker_id, after_marker_id) > 0) {
        rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
            pouch, &candidate, &due_after, error);
      }
    }
    lc_pouch_outbox_retention_marker_cleanup(pouch, &candidate);
    /* A capacity breach is a service-protection event, not an inventory
     * report. Read no more marker bodies after this bounded prefix; when a
     * cursor exists, finish only the directory-name walk to select its true
     * lexical successor, then yield after one reclaim attempt. */
    if (rc == LC_OK && ((max_count != SIZE_MAX && count > max_count) ||
                        (max_bytes != ~(uint64_t)0U && bytes > max_bytes))) {
      capacity_exceeded = 1;
      /* A new turn without a cursor can yield immediately. With a cursor,
       * inspect the remaining names without reading their marker bodies. */
      if (after_marker_id == NULL) {
        break;
      }
    }
  }
  if (dir != NULL && closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch outbox retention directory",
                      strerror(errno), directory, "pouch");
  }
  dir = NULL;
  if (rc != LC_OK)
    goto cleanup;
  if (later_name[0] != '\0' &&
      (oldest_after.marker_id == NULL ||
       strcmp(later_name, oldest_after.marker_id) < 0)) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_after);
    rc = lc_pouch_outbox_retention_marker_read(pouch, directory, later_name,
                                               &oldest_after, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    rc = lc_pouch_retention_marker_failure_turn(pouch, &oldest_after,
                                                later_name, after_marker_id,
                                                failed_name, rc, error);
  }
  if (rc == LC_OK && other_name[0] != '\0' &&
      (oldest_other.marker_id == NULL ||
       strcmp(other_name, oldest_other.marker_id) < 0)) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_other);
    rc = lc_pouch_outbox_retention_marker_read(pouch, directory, other_name,
                                               &oldest_other, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    rc = lc_pouch_retention_marker_failure_turn(pouch, &oldest_other,
                                                other_name, after_marker_id,
                                                failed_name, rc, error);
  }
  if (rc != LC_OK)
    goto cleanup;
  capacity_exceeded = capacity_exceeded ||
                      (max_count != SIZE_MAX && count > max_count) ||
                      (max_bytes != ~(uint64_t)0U && bytes > max_bytes);
  if (earliest.marker_id != NULL)
    *next_due_unix = earliest.due_at_unix;
  if (capacity_exceeded &&
      (oldest_after.marker_id != NULL || oldest_other.marker_id != NULL ||
       oldest.marker_id != NULL)) {
    if (oldest_after.marker_id != NULL) {
      *out = oldest_after;
      memset(&oldest_after, 0, sizeof(oldest_after));
    } else if (oldest_other.marker_id != NULL) {
      *out = oldest_other;
      memset(&oldest_other, 0, sizeof(oldest_other));
    } else {
      *out = oldest;
      memset(&oldest, 0, sizeof(oldest));
    }
  } else if (due_after.marker_id != NULL || due_first.marker_id != NULL) {
    if (due_after.marker_id != NULL) {
      *out = due_after;
      memset(&due_after, 0, sizeof(due_after));
    } else {
      *out = due_first;
      memset(&due_first, 0, sizeof(due_first));
    }
  }
  if (failed_name[0] != '\0' &&
      lc_pouch_retention_turn_precedes(failed_name, out->marker_id,
                                       after_marker_id)) {
    /* Re-read only the failed identity selected for this turn. Preserve its
     * actual error for the caller; other damaged bodies cannot pin that turn.
     * A concurrent repair must still satisfy expiry/capacity before deletion.
     */
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    rc = lc_pouch_outbox_retention_marker_read(pouch, directory, failed_name,
                                               out, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    if (rc == LC_OK && out->marker_id != NULL && !capacity_exceeded &&
        out->due_at_unix > now_unix) {
      if (*next_due_unix == 0 || out->due_at_unix < *next_due_unix)
        *next_due_unix = out->due_at_unix;
      lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    }
    strcpy(out->selected_id, failed_name);
  } else if (out->marker_id != NULL) {
    strcpy(out->selected_id, out->marker_id);
  }
  out->queued_count = count;
  out->queued_bytes = bytes;

cleanup:
  if (dir != NULL)
    (void)closedir(dir);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_after);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_other);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &earliest);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &due_first);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &due_after);
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}

int lc_pouch_outbox_retention_marker_remove(lc_pouch *pouch, const char *ns,
                                            int kind, const char *marker_id,
                                            lc_pouch_unix_seconds due_at_unix,
                                            lc_error *error) {
  char *directory;
  char *path;
  int rc;

  directory = NULL;
  path = NULL;
  rc = lc_pouch_outbox_retention_marker_path(
      pouch, ns, kind, marker_id, due_at_unix, 0, &directory, &path, error);
  if (rc == LC_OK && unlink(path) != 0 && errno != ENOENT) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to remove pouch outbox retention marker",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch outbox retention directory", error);
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

static int lc_pouch_compaction_take_namespace(lc_pouch *pouch,
                                              char **out_namespace,
                                              int *terminal_out) {
  char *ns;

  *out_namespace = NULL;
  *terminal_out = 0;
  pthread_mutex_lock(&pouch->compaction_mutex);
  if (pouch->compaction_namespace_count == 0U) {
    pthread_mutex_unlock(&pouch->compaction_mutex);
    return LC_OK;
  }
  ns = pouch->compaction_namespaces[0];
  *terminal_out = pouch->compaction_namespace_terminal[0] != 0U;
  if (pouch->compaction_namespace_count > 1U) {
    memmove(pouch->compaction_namespaces, pouch->compaction_namespaces + 1U,
            (pouch->compaction_namespace_count - 1U) *
                sizeof(pouch->compaction_namespaces[0]));
    memmove(pouch->compaction_namespace_terminal,
            pouch->compaction_namespace_terminal + 1U,
            (pouch->compaction_namespace_count - 1U) *
                sizeof(pouch->compaction_namespace_terminal[0]));
  }
  --pouch->compaction_namespace_count;
  pthread_mutex_unlock(&pouch->compaction_mutex);
  *out_namespace = ns;
  return LC_OK;
}

enum {
  LC_POUCH_COMPACTION_QUEUED,
  LC_POUCH_COMPACTION_TERMINAL,
  LC_POUCH_COMPACTION_RETENTION,
  LC_POUCH_COMPACTION_COLLECTION,
  LC_POUCH_COMPACTION_SOURCE_COUNT
};

static int lc_pouch_compaction_take_source_turn(lc_pouch *pouch) {
  int source;

  pthread_mutex_lock(&pouch->compaction_mutex);
  source = pouch->compaction_source_turn;
  pouch->compaction_source_turn =
      (source + 1) % LC_POUCH_COMPACTION_SOURCE_COUNT;
  pthread_mutex_unlock(&pouch->compaction_mutex);
  return source;
}

static int lc_pouch_compaction_take_retention_namespace(lc_pouch *pouch,
                                                        char **ns,
                                                        lc_error *error) {
  char *cursor = NULL;
  int rc = lc_pouch_retention_hint_next(
      pouch, pouch->state_retention_marker_cursor, ns, &cursor, error);
  if (rc == LC_OK && cursor != NULL) {
    lc_free_with_allocator(&pouch->allocator,
                           pouch->state_retention_marker_cursor);
    pouch->state_retention_marker_cursor = cursor;
  } else
    lc_free_with_allocator(&pouch->allocator, cursor);
  return rc;
}

static int lc_pouch_compaction_take_collection_namespace(lc_pouch *pouch,
                                                         char **ns,
                                                         lc_error *error) {
  char *cursor = NULL;
  int rc = lc_collection_maintenance_next(
      pouch, pouch->collection_namespace_cursor, ns, &cursor, error);
  if (rc == LC_OK && cursor != NULL) {
    lc_free_with_allocator(&pouch->allocator,
                           pouch->collection_namespace_cursor);
    pouch->collection_namespace_cursor = cursor;
  } else
    lc_free_with_allocator(&pouch->allocator, cursor);
  return rc;
}

static void lc_pouch_compaction_run_pass(lc_pouch *pouch) {
  char *ns;
  int terminal_reclaim;
  int attempt;
  lc_error error;
  int rc;

  if (pouch == NULL || !pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U) {
    return;
  }
  lc_error_init(&error);
  ns = NULL;
  terminal_reclaim = 0;
  rc = LC_OK;
  /* One fair, work-conserving rotation. Advance before discovery so a failed
   * source cannot keep the next pass; inspect each source at most once. */
  for (attempt = 0;
       attempt < LC_POUCH_COMPACTION_SOURCE_COUNT && rc == LC_OK && ns == NULL;
       ++attempt) {
    switch (lc_pouch_compaction_take_source_turn(pouch)) {
    case LC_POUCH_COMPACTION_QUEUED:
      rc = lc_pouch_compaction_take_namespace(pouch, &ns, &terminal_reclaim);
      break;
    case LC_POUCH_COMPACTION_TERMINAL:
      rc = lc_pouch_terminal_reclaim_marker_take(pouch, &ns, &error);
      if (rc == LC_OK && ns != NULL)
        terminal_reclaim = 1;
      break;
    case LC_POUCH_COMPACTION_RETENTION:
      rc = lc_pouch_compaction_take_retention_namespace(pouch, &ns, &error);
      break;
    case LC_POUCH_COMPACTION_COLLECTION:
      rc = lc_pouch_compaction_take_collection_namespace(pouch, &ns, &error);
      break;
    }
  }
  if (rc != LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_error_field("error", &error);
    fields[1] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "compaction.background.namespaces.error", fields,
                2U);
    lc_error_cleanup(&error);
    return;
  }
  lc_error_cleanup(&error);
  if (ns != NULL) {
    lc_pouch_maintenance_options options;
    lc_pouch_maintenance_result result;
    lc_error maintenance_error;

    if (terminal_reclaim) {
      lc_error marker_error;

      lc_error_init(&marker_error);
      rc = lc_pouch_terminal_reclaim_marker_write(pouch, ns, &marker_error);
      if (rc != LC_OK) {
        pslog_field fields[3];

        fields[0] = lc_log_str_field("ns", ns);
        fields[1] = lc_log_error_field("error", &marker_error);
        fields[2] = lc_log_code_field(&marker_error);
        lc_log_warn(pouch->logger, "compaction.terminal.marker.error", fields,
                    3U);
        lc_error_cleanup(&marker_error);
        lc_free_with_allocator(&pouch->allocator, ns);
        return;
      }
      lc_error_cleanup(&marker_error);
    }
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.ns = ns;
    options.terminal_reclaim = terminal_reclaim;
    lc_error_init(&maintenance_error);
    rc = lc_pouch_maintenance_run(pouch, &options, &result, &maintenance_error);
    if (rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &maintenance_error);
      fields[2] = lc_log_code_field(&maintenance_error);
      lc_log_warn(pouch->logger, "compaction.background.error", fields, 3U);
    } else if (result.cleanup_pending_count != 0UL) {
      lc_error queue_error;
      int queued;

      lc_error_init(&queue_error);
      queued = 0;
      if (lc_pouch_compaction_track_namespace(pouch, ns, terminal_reclaim,
                                              &queued, &queue_error) != LC_OK) {
        pslog_field fields[3];

        fields[0] = lc_log_str_field("ns", ns);
        fields[1] = lc_log_error_field("error", &queue_error);
        fields[2] = lc_log_code_field(&queue_error);
        lc_log_warn(pouch->logger, "compaction.background.queue.error", fields,
                    3U);
      }
      lc_error_cleanup(&queue_error);
    }
    lc_error_cleanup(&maintenance_error);
    lc_pouch_maintenance_result_cleanup(&pouch->allocator, &result);
    lc_free_with_allocator(&pouch->allocator, ns);
  }
}

#ifdef LOCKDC_TEST_BUILD
void lc_pouch_test_compaction_run_pass(lc_pouch *pouch) {
  lc_pouch_compaction_run_pass(pouch);
}

size_t lc_pouch_test_compaction_queue_count(lc_pouch *pouch) {
  size_t count;

  if (pouch == NULL || !pouch->compaction_mutex_initialized) {
    return 0U;
  }
  pthread_mutex_lock(&pouch->compaction_mutex);
  count = pouch->compaction_namespace_count;
  pthread_mutex_unlock(&pouch->compaction_mutex);
  return count;
}
#endif

static void *lc_pouch_compaction_worker(void *arg) {
  lc_pouch *pouch;
  int armed;

  pouch = (lc_pouch *)arg;
  armed = 0;
  pthread_mutex_lock(&pouch->compaction_mutex);
  while (!pouch->compaction_stop) {
    struct timespec deadline;
    int wait_rc;

    /* Ordinary opens stay idle; existing work or a failed hint discovery arms
     * the same bounded cadence so transient I/O cannot lose durable work. */
    if (!armed) {
      while (!pouch->compaction_stop && !pouch->compaction_pending) {
        (void)pthread_cond_wait(&pouch->compaction_cond,
                                &pouch->compaction_mutex);
      }
      if (pouch->compaction_stop) {
        break;
      }
      pouch->compaction_pending = 0;
      armed = 1;
    }
    lc_pouch_compaction_deadline(pouch, &deadline);
    wait_rc = 0;
    while (!pouch->compaction_stop && wait_rc != ETIMEDOUT) {
      wait_rc = pthread_cond_timedwait(&pouch->compaction_cond,
                                       &pouch->compaction_mutex, &deadline);
      if (pouch->compaction_pending) {
        /* Work is coalesced, but later writes never postpone an already
         * scheduled bounded maintenance turn. */
        pouch->compaction_pending = 0;
      }
    }
    if (pouch->compaction_stop) {
      break;
    }
    pthread_mutex_unlock(&pouch->compaction_mutex);
    lc_pouch_compaction_run_pass(pouch);
    pthread_mutex_lock(&pouch->compaction_mutex);
  }
  pthread_mutex_unlock(&pouch->compaction_mutex);
  return NULL;
}

static int lc_pouch_compaction_worker_init(lc_pouch *pouch, lc_error *error) {
  char *pending_namespace;
  char *retention_cursor;
  lc_error marker_error;
  int pthread_rc;
  int queued;
  int rc;

  if (pouch == NULL) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_init(&pouch->compaction_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch compaction mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->compaction_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch compaction condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_cond_initialized = 1;
  if (!pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U) {
    return LC_OK;
  }
  pending_namespace = NULL;
  queued = 0;
  lc_error_init(&marker_error);
  rc = lc_pouch_terminal_reclaim_marker_take(pouch, &pending_namespace,
                                             &marker_error);
  if (rc != LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_error_field("error", &marker_error);
    fields[1] = lc_log_code_field(&marker_error);
    lc_log_warn(pouch->logger, "compaction.terminal.marker.scan.error", fields,
                2U);
  } else if (pending_namespace != NULL) {
    rc = lc_pouch_compaction_track_namespace(pouch, pending_namespace, 1,
                                             &queued, &marker_error);
    if (rc != LC_OK) {
      pslog_field fields[2];

      fields[0] = lc_log_error_field("error", &marker_error);
      fields[1] = lc_log_code_field(&marker_error);
      lc_log_warn(pouch->logger, "compaction.terminal.marker.queue.error",
                  fields, 2U);
    } else if (queued) {
      pouch->compaction_pending = 1;
    }
  }
  lc_error_cleanup(&marker_error);
  lc_free_with_allocator(&pouch->allocator, pending_namespace);
  pending_namespace = NULL;
  retention_cursor = NULL;
  rc = lc_pouch_retention_hint_next(pouch, NULL, &pending_namespace,
                                    &retention_cursor, &marker_error);
  if (rc != LC_OK) {
    pslog_field fields[2];
    fields[0] = lc_log_error_field("error", &marker_error);
    fields[1] = lc_log_code_field(&marker_error);
    lc_log_warn(pouch->logger, "compaction.retention.hint.scan.error", fields,
                2);
  }
  if (rc != LC_OK || pending_namespace != NULL) {
    pouch->compaction_pending = 1;
#ifdef LOCKDC_TEST_BUILD
    (void)lc_pouch_retention_note_phase(LC_POUCH_RETENTION_DISCOVERED, NULL);
#endif
  }
  lc_error_cleanup(&marker_error);
  lc_free_with_allocator(&pouch->allocator, pending_namespace);
  lc_free_with_allocator(&pouch->allocator, retention_cursor);
  pending_namespace = retention_cursor = NULL;
  rc = lc_collection_maintenance_next(pouch, NULL, &pending_namespace,
                                      &retention_cursor, &marker_error);
  if (rc != LC_OK || pending_namespace != NULL)
    pouch->compaction_pending = 1;
  if (rc != LC_OK) {
    pslog_field field;
    field = lc_log_error_field("error", &marker_error);
    lc_log_warn(pouch->logger, "compaction.collection.discovery.error", &field,
                1U);
  }
  lc_error_cleanup(&marker_error);
  lc_free_with_allocator(&pouch->allocator, pending_namespace);
  lc_free_with_allocator(&pouch->allocator, retention_cursor);
  pthread_rc = pthread_create(&pouch->compaction_thread, NULL,
                              lc_pouch_compaction_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch compaction worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_compaction_worker_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->compaction_thread_started) {
    pthread_mutex_lock(&pouch->compaction_mutex);
    pouch->compaction_stop = 1;
    pthread_cond_broadcast(&pouch->compaction_cond);
    pthread_mutex_unlock(&pouch->compaction_mutex);
    pthread_join(pouch->compaction_thread, NULL);
    pouch->compaction_thread_started = 0;
  }
  if (pouch->compaction_cond_initialized) {
    pthread_cond_destroy(&pouch->compaction_cond);
    pouch->compaction_cond_initialized = 0;
  }
  if (pouch->compaction_mutex_initialized) {
    pthread_mutex_destroy(&pouch->compaction_mutex);
    pouch->compaction_mutex_initialized = 0;
  }
  while (pouch->compaction_namespace_count > 0U) {
    lc_free_with_allocator(
        &pouch->allocator,
        pouch->compaction_namespaces[--pouch->compaction_namespace_count]);
  }
  lc_free_with_allocator(&pouch->allocator, pouch->compaction_namespaces);
  pouch->compaction_namespaces = NULL;
  lc_free_with_allocator(&pouch->allocator,
                         pouch->compaction_namespace_terminal);
  pouch->compaction_namespace_terminal = NULL;
  lc_free_with_allocator(&pouch->allocator,
                         pouch->terminal_reclaim_marker_cursor);
  pouch->terminal_reclaim_marker_cursor = NULL;
  lc_free_with_allocator(&pouch->allocator,
                         pouch->state_retention_marker_cursor);
  pouch->state_retention_marker_cursor = NULL;
  lc_free_with_allocator(&pouch->allocator, pouch->collection_namespace_cursor);
  pouch->collection_namespace_cursor = NULL;
  pouch->compaction_namespace_count = 0U;
  pouch->compaction_namespace_capacity = 0U;
}

void lc_pouch_compaction_note_mutation(lc_pouch *pouch, const char *ns,
                                       int terminal) {
  lc_error error;
  int queued;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return;
  }
  lc_error_init(&error);
  queued = 0;
  if (!pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U || pouch->aborted ||
      !pouch->compaction_thread_started) {
    lc_error_cleanup(&error);
    return;
  }
  if (lc_pouch_compaction_track_namespace(pouch, ns, terminal, &queued,
                                          &error) != LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", &error);
    fields[2] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "compaction.background.queue.error", fields, 3U);
  } else if (terminal && !queued &&
             lc_pouch_terminal_reclaim_marker_write(pouch, ns, &error) !=
                 LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", &error);
    fields[2] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "compaction.terminal.marker.error", fields, 3U);
  }
  lc_error_cleanup(&error);
  pthread_mutex_lock(&pouch->compaction_mutex);
  pouch->compaction_pending = 1;
  pthread_cond_signal(&pouch->compaction_cond);
  pthread_mutex_unlock(&pouch->compaction_mutex);
}

int lc_pouch_compaction_note_history_advanced(lc_pouch *pouch, const char *ns,
                                              lc_error *error) {
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history compaction notification requires pouch and "
                        "namespace",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_terminal_reclaim_marker_write(pouch, ns, error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(pouch, ns, 1);
  }
  return rc;
}

static void lc_pouch_indexer_deadline(const lc_pouch *pouch,
                                      struct timespec *deadline) {
  uint64_t seconds;

  if (pouch == NULL || deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  seconds = pouch->indexer_flush_interval_seconds;
  if (seconds > (uint64_t)(LONG_MAX - deadline->tv_sec)) {
    deadline->tv_sec = LONG_MAX;
  } else {
    deadline->tv_sec += (time_t)seconds;
  }
}

/* The idle debounce remains anchored to the first pending mutation. A live
 * lease guard can only shorten that wait so expiration resumes publication
 * promptly; it never resets the batching boundary on later signals. */
static void lc_pouch_indexer_cap_deadline_for_operation_expiry_locked(
    lc_pouch *pouch, struct timespec *deadline) {
  lc_pouch_indexer_pending_namespace *entry;

  if (pouch == NULL || deadline == NULL) {
    return;
  }
  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    lc_pouch_unix_seconds expires_at_unix;

    expires_at_unix =
        lc_pouch_query_index_next_operation_expiry_locked(pouch, entry->ns);
    if (expires_at_unix <= 0L || (lc_u64)expires_at_unix > (lc_u64)LONG_MAX) {
      continue;
    }
    if ((time_t)expires_at_unix < deadline->tv_sec) {
      deadline->tv_sec = (time_t)expires_at_unix;
      deadline->tv_nsec = 0L;
    }
  }
}

#ifdef LOCKDC_TEST_BUILD
void lc_pouch_test_indexer_deadline(lc_pouch *pouch,
                                    struct timespec *deadline) {
  lc_pouch_indexer_deadline(pouch, deadline);
}
#endif

static void lc_pouch_indexer_pending_namespaces_cleanup(
    lc_pouch *pouch, lc_pouch_indexer_pending_namespace *namespaces) {
  while (namespaces != NULL) {
    lc_pouch_indexer_pending_namespace *next;

    next = namespaces->next;
    lc_free_with_allocator(&pouch->allocator, namespaces->ns);
    lc_free_with_allocator(&pouch->allocator, namespaces);
    namespaces = next;
  }
}

static int lc_pouch_indexer_queue_namespace_locked(lc_pouch *pouch,
                                                   const char *ns) {
  lc_pouch_indexer_pending_namespace *entry;

  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->ns, ns) == 0) {
      if (entry->write_count != LC_U64_MAX) {
        ++entry->write_count;
      }
      return LC_OK;
    }
  }
  entry = (lc_pouch_indexer_pending_namespace *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return LC_ERR_NOMEM;
  }
  entry->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (entry->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return LC_ERR_NOMEM;
  }
  entry->write_count = 1U;
  entry->next = pouch->indexer_pending_namespaces;
  pouch->indexer_pending_namespaces = entry;
  return LC_OK;
}

static int
lc_pouch_indexer_namespace_flush_limit_reached_locked(lc_pouch *pouch,
                                                      const char *ns) {
  lc_pouch_indexer_pending_namespace *entry;

  if (pouch == NULL || ns == NULL || pouch->indexer_flush_docs == 0U) {
    return 0;
  }
  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->ns, ns) == 0) {
      return entry->write_count >= pouch->indexer_flush_docs &&
             (!lc_pouch_single_writer_enabled(pouch) ||
              lc_pouch_query_index_pending_document_limit_reached_locked(
                  pouch, ns, pouch->indexer_flush_docs));
    }
  }
  return 0;
}

/* Moves one foreground threshold batch out of the worker queue. New writes
 * then establish their own debounce window while publication runs. A failed
 * or deferred attempt requeues this entry under the same mutex. */
static lc_pouch_indexer_pending_namespace *
lc_pouch_indexer_take_namespace_locked(lc_pouch *pouch, const char *ns) {
  lc_pouch_indexer_pending_namespace **link;

  if (pouch == NULL || ns == NULL) {
    return NULL;
  }
  link = &pouch->indexer_pending_namespaces;
  while (*link != NULL) {
    lc_pouch_indexer_pending_namespace *entry;

    entry = *link;
    if (strcmp(entry->ns, ns) == 0) {
      *link = entry->next;
      entry->next = NULL;
      return entry;
    }
    link = &entry->next;
  }
  return NULL;
}

/* Shared roots do not have a local mutation authority, so their threshold
 * stays on the asynchronous replay worker.  An exclusive writer has a
 * complete body-free projection and publishes at the threshold in the
 * foreground, matching Go disk's bounded memtable boundary. */
static int lc_pouch_indexer_flush_limit_reached_locked(lc_pouch *pouch) {
  lc_pouch_indexer_pending_namespace *entry;

  if (pouch == NULL || lc_pouch_single_writer_enabled(pouch)) {
    return 0;
  }
  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_indexer_namespace_flush_limit_reached_locked(pouch,
                                                              entry->ns)) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_indexer_requeue(lc_pouch *pouch,
                                     lc_pouch_indexer_pending_namespace *list) {
  while (list != NULL) {
    lc_pouch_indexer_pending_namespace *next;
    lc_pouch_indexer_pending_namespace *entry;

    next = list->next;
    for (entry = pouch->indexer_pending_namespaces; entry != NULL;
         entry = entry->next) {
      if (strcmp(entry->ns, list->ns) == 0) {
        if (entry->write_count > LC_U64_MAX - list->write_count) {
          entry->write_count = LC_U64_MAX;
        } else {
          entry->write_count += list->write_count;
        }
        lc_free_with_allocator(&pouch->allocator, list->ns);
        lc_free_with_allocator(&pouch->allocator, list);
        list = next;
        break;
      }
    }
    if (entry == NULL) {
      list->next = pouch->indexer_pending_namespaces;
      pouch->indexer_pending_namespaces = list;
      list = next;
    }
  }
}

static void
lc_pouch_indexer_run_batch(lc_pouch *pouch,
                           lc_pouch_indexer_pending_namespace *batch) {
  lc_pouch_indexer_pending_namespace *failed;
  lc_pouch_indexer_pending_namespace *entry;

  failed = NULL;
  entry = batch;
  while (entry != NULL) {
    lc_pouch_indexer_pending_namespace *next;
    lc_pouch_query_index_flush_result flush_result;
    lc_pouch_generation state_index_seq;
    lc_error error;
    int deferred_for_active_operation;
    int rc;

    next = entry->next;
    entry->next = NULL;
    memset(&flush_result, 0, sizeof(flush_result));
    state_index_seq = 0UL;
    deferred_for_active_operation = 0;
    lc_error_init(&error);
    rc = lc_pouch_state_query_index_seq(pouch, entry->ns, &state_index_seq,
                                        &error);
    if (rc == LC_OK) {
      if (lc_pouch_single_writer_enabled(pouch)) {
        /* An exclusive worker must preserve the same final-version-only
         * boundary as foreground threshold publication. A zero result with a
         * nonzero public sequence means active keys deferred this batch. */
        rc = lc_pouch_query_index_flush_threshold(
            pouch, entry->ns, state_index_seq, &flush_result, &error);
        deferred_for_active_operation = rc == LC_OK && state_index_seq != 0UL &&
                                        flush_result.index_seq == 0UL;
      } else {
        rc = lc_pouch_query_index_flush(pouch, entry->ns, state_index_seq,
                                        &flush_result, &error);
      }
    }
    if (rc != LC_OK || deferred_for_active_operation) {
      if (rc != LC_OK) {
        pslog_field fields[3];

        fields[0] = lc_log_str_field("ns", entry->ns);
        fields[1] = lc_log_error_field("error", &error);
        fields[2] = lc_log_code_field(&error);
        lc_log_warn(pouch->logger, "index.background.error", fields, 3U);
      }
      entry->next = failed;
      failed = entry;
    } else {
      lc_free_with_allocator(&pouch->allocator, entry->ns);
      lc_free_with_allocator(&pouch->allocator, entry);
    }
    lc_error_cleanup(&error);
    entry = next;
  }
  if (failed != NULL) {
    pthread_mutex_lock(&pouch->indexer_mutex);
    lc_pouch_indexer_requeue(pouch, failed);
    pthread_cond_signal(&pouch->indexer_cond);
    pthread_mutex_unlock(&pouch->indexer_mutex);
  }
}

static void *lc_pouch_indexer_worker(void *arg) {
  lc_pouch *pouch;

  pouch = (lc_pouch *)arg;
  pthread_mutex_lock(&pouch->indexer_mutex);
  while (!pouch->indexer_stop) {
    lc_pouch_indexer_pending_namespace *batch;
    struct timespec idle_deadline;
    struct timespec deadline;
    int wait_rc;

    while (!pouch->indexer_stop && pouch->indexer_pending_namespaces == NULL) {
      (void)pthread_cond_wait(&pouch->indexer_cond, &pouch->indexer_mutex);
    }
    if (pouch->indexer_stop) {
      break;
    }
    lc_pouch_indexer_deadline(pouch, &idle_deadline);
    wait_rc = 0;
    while (!pouch->indexer_stop &&
           !lc_pouch_indexer_flush_limit_reached_locked(pouch) &&
           wait_rc != ETIMEDOUT) {
      deadline = idle_deadline;
      lc_pouch_indexer_cap_deadline_for_operation_expiry_locked(pouch,
                                                                &deadline);
      wait_rc = pthread_cond_timedwait(&pouch->indexer_cond,
                                       &pouch->indexer_mutex, &deadline);
    }
    if (pouch->indexer_stop) {
      break;
    }
    batch = pouch->indexer_pending_namespaces;
    pouch->indexer_pending_namespaces = NULL;
    pthread_mutex_unlock(&pouch->indexer_mutex);
    lc_pouch_indexer_run_batch(pouch, batch);
    pthread_mutex_lock(&pouch->indexer_mutex);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return NULL;
}

static int lc_pouch_indexer_worker_init(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_init(&pouch->indexer_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch indexer mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->indexer_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch indexer condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_cond_initialized = 1;
  pthread_rc = pthread_create(&pouch->indexer_thread, NULL,
                              lc_pouch_indexer_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch indexer worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_indexer_worker_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->indexer_thread_started) {
    pthread_mutex_lock(&pouch->indexer_mutex);
    pouch->indexer_stop = 1;
    pthread_cond_broadcast(&pouch->indexer_cond);
    pthread_mutex_unlock(&pouch->indexer_mutex);
    pthread_join(pouch->indexer_thread, NULL);
    pouch->indexer_thread_started = 0;
  }
  if (pouch->indexer_cond_initialized) {
    pthread_cond_destroy(&pouch->indexer_cond);
    pouch->indexer_cond_initialized = 0;
  }
  if (pouch->indexer_mutex_initialized) {
    lc_pouch_indexer_pending_namespaces_cleanup(
        pouch, pouch->indexer_pending_namespaces);
    pouch->indexer_pending_namespaces = NULL;
    pthread_mutex_destroy(&pouch->indexer_mutex);
    pouch->indexer_mutex_initialized = 0;
  }
}

static int lc_pouch_indexer_publish_threshold(lc_pouch *pouch, const char *ns) {
  lc_pouch_query_index_flush_result flush_result;
  lc_pouch_generation state_index_seq;
  lc_error error;
  int published;
  int rc;

  /* State durability was already acknowledged before this derived-artifact
   * publication.  Preserve that boundary: a failed index write is retained
   * in the local queue for recovery instead of turning a successful mutation
   * into an error. */
  memset(&flush_result, 0, sizeof(flush_result));
  state_index_seq = 0UL;
  published = 0;
  lc_error_init(&error);
  rc = lc_pouch_state_query_index_seq(pouch, ns, &state_index_seq, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_flush_threshold(pouch, ns, state_index_seq,
                                              &flush_result, &error);
  }
  if (rc == LC_OK && flush_result.index_seq != 0UL) {
    published = 1;
  }
  if (rc != LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", &error);
    fields[2] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "index.threshold.error", fields, 3U);
  }
  lc_error_cleanup(&error);
  return published;
}

void lc_pouch_indexer_note_mutation(lc_pouch *pouch, const char *ns) {
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || pouch->aborted ||
      !pouch->indexer_thread_started) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  rc = pouch->indexer_stop ? LC_ERR_INVALID
                           : lc_pouch_indexer_queue_namespace_locked(pouch, ns);
  if (rc == LC_OK) {
    pthread_cond_signal(&pouch->indexer_cond);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  if (rc != LC_OK && rc != LC_ERR_INVALID) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("ns", ns);
    lc_log_warn(pouch->logger, "index.background.queue.error", fields, 1U);
  }
}

void lc_pouch_indexer_note_operation_complete(lc_pouch *pouch, const char *ns,
                                              const char *key,
                                              const char *operation_id) {
  lc_pouch_indexer_pending_namespace *threshold_batch;
  int eager_publish;
  int published;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || pouch->aborted || operation_id == NULL ||
      operation_id[0] == '\0' || !pouch->indexer_thread_started) {
    return;
  }
  eager_publish = 0;
  threshold_batch = NULL;
  pthread_mutex_lock(&pouch->indexer_mutex);
  if (!pouch->indexer_stop && lc_pouch_single_writer_enabled(pouch)) {
    if (lc_pouch_query_index_pending_operation_complete_locked(pouch, ns, key,
                                                               operation_id)) {
      eager_publish =
          lc_pouch_indexer_namespace_flush_limit_reached_locked(pouch, ns);
      if (eager_publish) {
        threshold_batch = lc_pouch_indexer_take_namespace_locked(pouch, ns);
        eager_publish = threshold_batch != NULL;
      }
    }
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  if (eager_publish) {
    published = lc_pouch_indexer_publish_threshold(pouch, ns);
    if (published) {
      lc_pouch_indexer_pending_namespaces_cleanup(pouch, threshold_batch);
    } else {
      pthread_mutex_lock(&pouch->indexer_mutex);
      lc_pouch_indexer_requeue(pouch, threshold_batch);
      pthread_cond_signal(&pouch->indexer_cond);
      pthread_mutex_unlock(&pouch->indexer_mutex);
    }
  }
}

int lc_pouch_fsync_commit(lc_pouch *pouch, int fd, lc_error *error) {
  lc_pouch_fsync_request request;
  lc_pouch_fsync_batcher *batcher;
  int pthread_rc;

  if (pouch == NULL || fd < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync commit requires pouch and fd", NULL, NULL,
                        "pouch");
  }
  if (!pouch->durable_sync) {
    return LC_OK;
  }
  if (pouch->aborted) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed after abort", NULL, NULL,
                        "pouch");
  }
  batcher = pouch->fsync_batcher;
  if (batcher == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed", NULL, NULL, "pouch");
  }
  memset(&request, 0, sizeof(request));
  request.fd = fd;
  pthread_rc = pthread_cond_init(&request.cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->stop) {
    pthread_mutex_unlock(&batcher->mutex);
    pthread_cond_destroy(&request.cond);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed", NULL, NULL, "pouch");
  }
  if (batcher->tail != NULL) {
    batcher->tail->next = &request;
  } else {
    batcher->head = &request;
  }
  batcher->tail = &request;
  ++batcher->queue_count;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_fsync_enqueued != NULL)
    lc_pouch_test_fsync_enqueued();
#endif
  pthread_cond_signal(&batcher->cond);
  while (!request.done) {
    pthread_cond_wait(&request.cond, &batcher->mutex);
  }
  pthread_mutex_unlock(&batcher->mutex);
  pthread_cond_destroy(&request.cond);
  if (request.errnum != 0) {
    pslog_field fields[2];

    fields[0] = lc_log_i64_field("fd", fd);
    fields[1] = lc_log_str_field("error", strerror(request.errnum));
    lc_log_error(pouch->logger, "logstore.fsync.error", fields, 2U);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to sync pouch state segment",
                        strerror(request.errnum), NULL, "pouch");
  }
  {
    pslog_field fields[2];

    fields[0] = lc_log_i64_field("fd", fd);
    fields[1] = lc_log_bool_field("batched", 1);
    lc_log_trace(pouch->logger, "logstore.fsync", fields, 2U);
  }
  return LC_OK;
}

int lc_pouch_queue_watch_wait(lc_pouch *pouch, const char *ns,
                              const char *queue, uint64_t timeout_ms) {
#if defined(__linux__)
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  struct pollfd pollfd;
  union {
    unsigned char bytes[4096];
    struct inotify_event alignment;
  } events;
  size_t leaf_len;
  size_t offset;
  ssize_t bytes_read;
  int fd;
  int watch;
  int timeout;
  int ready;
  int result;
  uint64_t timeout_ns;
  uint64_t elapsed_ns;
  uint64_t remaining_ns;
  struct timespec started;
  struct timespec now;
  int clock_started;

  if (pouch == NULL || !pouch->queue_watch_enabled || pouch->aborted ||
      ns == NULL || ns[0] == '\0' || queue == NULL || queue[0] == '\0') {
    return -1;
  }
  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&pouch->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&pouch->allocator, queue);
  notify_leaf = NULL;
  result = -1;
  if (notify_dir == NULL || escaped_queue == NULL ||
      strlen(escaped_queue) > SIZE_MAX - strlen(".notify") - 1U) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&pouch->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  fd = inotify_init();
  if (fd < 0) {
    goto cleanup;
  }
  watch = inotify_add_watch(fd, notify_dir,
                            IN_MOVED_TO | IN_CREATE | IN_CLOSE_WRITE |
                                IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);
  if (watch < 0) {
    (void)close(fd);
    goto cleanup;
  }
  result = 0;
  timeout_ns = timeout_ms > LC_U64_MAX / (uint64_t)1000000UL
                   ? LC_U64_MAX
                   : timeout_ms * (uint64_t)1000000UL;
  clock_started = clock_gettime(CLOCK_MONOTONIC, &started) == 0;
  memset(&pollfd, 0, sizeof(pollfd));
  pollfd.fd = fd;
  pollfd.events = POLLIN;
  for (;;) {
    if (clock_started && clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
      elapsed_ns = lc_pouch_elapsed_ns(&started, &now);
      if (elapsed_ns >= timeout_ns) {
        break;
      }
      remaining_ns = timeout_ns - elapsed_ns;
      timeout = remaining_ns / (uint64_t)1000000UL >= (uint64_t)INT_MAX
                    ? INT_MAX
                    : (int)((remaining_ns + (uint64_t)999999UL) /
                            (uint64_t)1000000UL);
    } else {
      timeout = timeout_ms > (uint64_t)INT_MAX ? INT_MAX : (int)timeout_ms;
    }
    pollfd.revents = 0;
    ready = poll(&pollfd, 1U, timeout);
    if (ready == 0) {
      break;
    }
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if ((pollfd.revents & POLLIN) == 0) {
      break;
    }
    bytes_read = read(fd, events.bytes, sizeof(events.bytes));
    if (bytes_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    offset = 0U;
    while (offset + sizeof(struct inotify_event) <= (size_t)bytes_read) {
      const struct inotify_event *event;
      size_t event_size;

      event = (const struct inotify_event *)(events.bytes + offset);
      event_size = sizeof(*event) + event->len;
      if (event_size > (size_t)bytes_read - offset) {
        break;
      }
      if ((event->mask & (IN_Q_OVERFLOW | IN_DELETE_SELF | IN_MOVE_SELF)) !=
              0U ||
          (event->len > 0U && strcmp(event->name, notify_leaf) == 0)) {
        result = 1;
        break;
      }
      offset += event_size;
    }
    if (result != 0) {
      break;
    }
  }
  (void)inotify_rm_watch(fd, watch);
  (void)close(fd);

cleanup:
  lc_free_with_allocator(&pouch->allocator, notify_leaf);
  lc_free_with_allocator(&pouch->allocator, escaped_queue);
  lc_free_with_allocator(&pouch->allocator, notify_dir);
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  return result;
#else
  (void)pouch;
  (void)ns;
  (void)queue;
  (void)timeout_ms;
  return -1;
#endif
}

static const char *lc_pouch_option_string(const char *value,
                                          const char *fallback) {
  if (value != NULL && value[0] != '\0') {
    return value;
  }
  return fallback;
}

static const char *
lc_pouch_compression_option(const lc_pouch_open_options *options) {
  const char *value;

  value = options != NULL ? options->compression : NULL;
  if (value == NULL || value[0] == '\0') {
    return "none";
  }
  return value;
}

static void lc_pouch_init_options(lc_pouch *pouch,
                                  const lc_pouch_open_options *options) {
  pouch->segment_target_bytes =
      options != NULL && options->segment_target_bytes != 0U
          ? options->segment_target_bytes
          : LC_POUCH_DEFAULT_SEGMENT_TARGET_BYTES;
  pouch->indexer_flush_docs =
      options != NULL && options->indexer_flush_docs != 0U
          ? options->indexer_flush_docs
          : (uint64_t)LC_POUCH_DEFAULT_INDEXER_FLUSH_DOCS;
  pouch->indexer_flush_interval_seconds =
      options != NULL && options->indexer_flush_interval_seconds != 0U
          ? options->indexer_flush_interval_seconds
          : (uint64_t)LC_POUCH_DEFAULT_INDEXER_FLUSH_INTERVAL_SECONDS;
  pouch->fsync_batch_max_ops =
      options != NULL ? options->fsync_batch_max_ops : 0U;
  pouch->durable_sync = options != NULL && options->durable_sync ? 1 : 0;
  pouch->compaction_min_segment_count =
      options != NULL && options->compaction_min_segment_count != 0UL
          ? options->compaction_min_segment_count
          : LC_POUCH_DEFAULT_COMPACTION_MIN_SEGMENT_COUNT;
  pouch->compaction_min_reclaimable_bytes =
      options != NULL && options->compaction_min_reclaimable_bytes != 0U
          ? options->compaction_min_reclaimable_bytes
          : LC_POUCH_DEFAULT_COMPACTION_MIN_RECLAIMABLE_BYTES;
  pouch->compaction_interval_seconds =
      options != NULL && options->compaction_interval_seconds != 0U
          ? options->compaction_interval_seconds
          : LC_POUCH_DEFAULT_COMPACTION_INTERVAL_SECONDS;
  pouch->compaction_delete_grace_seconds =
      options != NULL && options->compaction_delete_grace_seconds != 0U
          ? options->compaction_delete_grace_seconds
          : LC_POUCH_DEFAULT_COMPACTION_DELETE_GRACE_SECONDS;
  pouch->compaction_throttling_disabled =
      options != NULL && options->compaction_throttling_disabled ? 1 : 0;
  pouch->compaction_max_io_bytes_per_sec =
      pouch->compaction_throttling_disabled
          ? 0U
          : (options != NULL && options->compaction_max_io_bytes_per_sec != 0U
                 ? options->compaction_max_io_bytes_per_sec
                 : LC_POUCH_DEFAULT_COMPACTION_MAX_IO_BYTES_PER_SEC);
  pouch->background_compaction_enabled =
      options != NULL && options->background_compaction_enabled_set
          ? (options->background_compaction_enabled ? 1 : 0)
          : 1;
  pouch->terminal_reclaim_min_bytes =
      options != NULL && options->terminal_reclaim_min_bytes != 0U
          ? options->terminal_reclaim_min_bytes
          : LC_POUCH_DEFAULT_TERMINAL_RECLAIM_MIN_BYTES;
  pouch->single_writer = options != NULL && options->single_writer_set
                             ? (options->single_writer != 0 ? 1 : 0)
                             : 1;
  pouch->queue_watch_enabled = options != NULL ? options->queue_watch : 0;
  pouch->queue_watch_mode = "polling";
  pouch->queue_watch_reason = "config_disabled";
  pouch->query_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(options != NULL ? options->query_engine : NULL,
                             "index"));
  pouch->query_fallback_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(
          options != NULL ? options->query_fallback_engine : NULL, ""));
  pouch->query_indexing_enabled =
      options != NULL && options->query_indexing_enabled_set
          ? (options->query_indexing_enabled != 0 ? 1 : 0)
          : 1;
  pouch->compression = lc_strdup_with_allocator(
      &pouch->allocator, lc_pouch_compression_option(options));
  pouch->compression_explicit =
      options && options->compression && *options->compression;
}

static void lc_pouch_detect_filesystem_capabilities(lc_pouch *pouch) {
#if defined(__linux__)
  struct statfs st;

  if (pouch != NULL && pouch->root_path != NULL &&
      statfs(pouch->root_path, &st) == 0) {
    pouch->filesystem_capabilities_known = 1;
    pouch->filesystem_is_nfs = st.f_type == 0x6969;
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__) || defined(__DragonFly__)
  struct statfs st;

  if (pouch != NULL && pouch->root_path != NULL &&
      statfs(pouch->root_path, &st) == 0) {
    pouch->filesystem_capabilities_known = 1;
    pouch->filesystem_is_nfs = strcasecmp(st.f_fstypename, "nfs") == 0 ||
                               strcasecmp(st.f_fstypename, "nfs4") == 0;
  }
#else
  (void)pouch;
#endif
}

static int lc_pouch_queue_watch_supported(const lc_pouch *pouch) {
#if defined(__linux__)
  return pouch != NULL && pouch->filesystem_capabilities_known &&
         !pouch->filesystem_is_nfs;
#else
  (void)pouch;
  return 0;
#endif
}

static void lc_pouch_configure_filesystem_capabilities(lc_pouch *pouch) {
  int requested;

  if (pouch == NULL) {
    return;
  }
  pouch->filesystem_capabilities_known = 0;
  pouch->filesystem_is_nfs = 0;
  lc_pouch_detect_filesystem_capabilities(pouch);
  requested = pouch->queue_watch_enabled;
  pouch->queue_watch_enabled = 0;
  pouch->queue_watch_mode = "polling";
  pouch->queue_watch_reason = "config_disabled";
  if (requested) {
    if (lc_pouch_queue_watch_supported(pouch)) {
      pouch->queue_watch_enabled = 1;
      pouch->queue_watch_mode = "fsnotify";
      pouch->queue_watch_reason = "filesystem_watch_enabled";
    } else {
      pouch->queue_watch_reason = "filesystem_not_supported";
    }
  }
}

static int lc_pouch_write_root_manifest(lc_pouch *pouch, lc_error *error) {
  char manifest[512];
  struct stat st;
  char *crypto_key_id;
  char *manifest_path;
  int rc;

  crypto_key_id = NULL;
  if (lc_pouch_crypto_enabled(pouch->crypto)) {
    rc = lc_pouch_crypto_key_id(pouch->crypto, &crypto_key_id, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    lc_free_with_allocator(NULL, crypto_key_id);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  snprintf(manifest, sizeof(manifest),
           "layout=%s\nversion=%lu\ncompression=%s\ncrypto=%s\n"
           "crypto_key_id=%s\ncollection_head_bytes=%u\n",
           LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION, pouch->compression,
           lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext",
           crypto_key_id != NULL ? crypto_key_id : "",
           LC_COLLECTION_HEAD_BYTES);
  if (lstat(manifest_path, &st) == 0)
    rc = lc_pouch_path_write_bytes_file_mode(manifest_path, manifest,
                                             strlen(manifest),
                                             (unsigned int)st.st_mode, error);
  else if (errno == ENOENT)
    rc = lc_pouch_path_write_text_file(manifest_path, manifest, error);
  else
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to inspect pouch root manifest permissions",
                      strerror(errno), NULL, "pouch");
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  lc_free_with_allocator(NULL, crypto_key_id);
  return rc;
}

static int lc_pouch_root_manifest_read(
    lc_pouch *pouch, char *layout, size_t layout_size, unsigned long *version,
    char *mode, size_t mode_size, char *key_id, size_t key_id_size,
    int *found_manifest, int *found_crypto_mode, char *compression,
    size_t compression_size, int *found_compression, int *collection_head_bytes,
    lc_error *error) {
  char line[512];
  char *manifest_path;
  FILE *fp;
  int found_key_id;
  int found_layout;
  int found_mode;
  int found_compression_mode;
  int found_version;
  int rc;

  if (layout == NULL || layout_size == 0U || version == NULL || mode == NULL ||
      mode_size == 0U || key_id == NULL || key_id_size == 0U ||
      found_manifest == NULL || found_crypto_mode == NULL ||
      compression == NULL || compression_size == 0U ||
      found_compression == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest read requires outputs", NULL, NULL,
                        NULL);
  }
  layout[0] = '\0';
  *version = 0UL;
  mode[0] = '\0';
  key_id[0] = '\0';
  compression[0] = '\0';
  *found_manifest = 0;
  *found_crypto_mode = 0;
  *found_compression = 0;
  *collection_head_bytes = 0;
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    rc = errno == ENOENT ? LC_OK
                         : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                        "failed to open pouch root manifest",
                                        strerror(errno), NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, manifest_path);
    return rc;
  }
  *found_manifest = 1;
  found_layout = 0;
  found_version = 0;
  found_mode = 0;
  found_compression_mode = 0;
  found_key_id = 0;
  rc = LC_OK;
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *value;
    size_t len;

    if (strncmp(line, "layout=", 7U) == 0) {
      if (found_layout) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate layout", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len == 0U || len + 1U > layout_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest layout is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      memcpy(layout, value, len + 1U);
      found_layout = 1;
      continue;
    }
    if (strncmp(line, "version=", 8U) == 0) {
      char *end;
      unsigned long parsed;

      if (found_version) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate version", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 8U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      end = NULL;
      parsed = strtoul(value, &end, 10);
      if (end == value || (end != NULL && *end != '\0') || parsed == 0UL) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest version is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      *version = parsed;
      found_version = 1;
      continue;
    }
    if (strncmp(line, "crypto=", 7U) == 0) {
      if (found_mode) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (strcmp(value, "plaintext") != 0 && strcmp(value, "encrypted") != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has invalid crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      if (len + 1U > mode_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto mode is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(mode, value, len + 1U);
      found_mode = 1;
      continue;
    }
    if (strncmp(line, "compression=", 12U) == 0) {
      if (found_compression_mode) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate compression mode",
                          NULL, NULL, "pouch");
        break;
      }
      value = line + 12U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (strcmp(value, "none") != 0 && strcmp(value, "zlib") != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has invalid compression mode",
                          NULL, NULL, "pouch");
        break;
      }
      if (len + 1U > compression_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest compression mode is too long",
                          NULL, NULL, "pouch");
        break;
      }
      memcpy(compression, value, len + 1U);
      found_compression_mode = 1;
      continue;
    }
    if (strncmp(line, "collection_head_bytes=", 22U) == 0) {
      char expected[16];
      (void)snprintf(expected, sizeof(expected), "%u",
                     LC_COLLECTION_HEAD_BYTES);
      value = line + 22U;
      value[strcspn(value, "\r\n")] = '\0';
      if (*collection_head_bytes != 0 || strcmp(value, expected) != 0) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "invalid or unsupported collection head layout", NULL,
                          NULL, "pouch");
        break;
      }
      *collection_head_bytes = (int)LC_COLLECTION_HEAD_BYTES;
      continue;
    }
    if (strncmp(line, "crypto_key_id=", 14U) == 0) {
      if (found_key_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto key id",
                          NULL, NULL, "pouch");
        break;
      }
      value = line + 14U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len + 1U > key_id_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto key id is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(key_id, value, len + 1U);
      found_key_id = 1;
    }
  }
  if (ferror(fp) && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc == LC_OK && !found_layout) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires layout", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_version) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires version", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_mode) {
    (void)snprintf(mode, mode_size, "plaintext");
  }
  if (rc == LC_OK && !found_compression_mode) {
    (void)snprintf(compression, compression_size, "none");
  }
  if (rc == LC_OK) {
    *found_crypto_mode = found_mode;
    *found_compression = found_compression_mode;
  }
  return rc;
}

static int lc_pouch_root_has_namespace_entries(lc_pouch *pouch, int *out,
                                               lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  int saved_errno;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root namespace scan requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = 0;
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    *out = 1;
    break;
  }
  saved_errno = errno;
  if (closedir(dir) != 0 && saved_errno == 0) {
    saved_errno = errno != 0 ? errno : EIO;
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (saved_errno != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  return LC_OK;
}

/*
 * Exclusive Pouch pays this bounded, read-only cache construction at open so
 * its first query does not decode immutable segment readers. Plain shared
 * roots remain lazy to keep their optional multi-writer resident footprint
 * bounded. Transformed roots retain their existing eager warming in either
 * writer mode.
 */
static int lc_pouch_warm_open_namespaces(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  unsigned long namespace_count;
  int saved_errno;
  int rc;

  if (pouch == NULL || (!lc_pouch_single_writer_enabled(pouch) &&
                        !lc_pouch_crypto_enabled(pouch->crypto) &&
                        !lc_pouch_crypto_compression_enabled(pouch->crypto))) {
    return LC_OK;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("reason", "namespaces-missing");
      lc_log_debug(pouch->logger, "cache.load.skip", fields, 1U);
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  rc = LC_OK;
  saved_errno = 0;
  namespace_count = 0UL;
  {
    pslog_field fields[2];

    fields[0] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[1] = lc_log_bool_field(
        "compression", lc_pouch_crypto_compression_enabled(pouch->crypto));
    lc_log_debug(pouch->logger, "cache.load.start", fields, 2U);
  }
  while (rc == LC_OK) {
    char *ns;
    lc_error warm_error;
    int index_rc;
    int state_rc;

    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      saved_errno = errno;
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    ns = lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    if (ns == NULL) {
      continue;
    }
    lc_error_init(&warm_error);
    state_rc = lc_pouch_state_warm_namespace(pouch, ns, &warm_error);
    if (state_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.state.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    lc_error_init(&warm_error);
    index_rc = LC_OK;
    if (pouch->query_indexing_enabled) {
      index_rc = lc_pouch_query_index_warm_namespace(pouch, ns, &warm_error);
    }
    if (index_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.index.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_bool_field("state_ok", state_rc == LC_OK);
      fields[2] = lc_log_bool_field("index_ok", index_rc == LC_OK);
      lc_log_debug(pouch->logger, "cache.load.namespace", fields, 3U);
    }
    namespace_count++;
    lc_free_with_allocator(&pouch->allocator, ns);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces",
                      strerror(errno != 0 ? errno : EIO), NULL, "pouch");
  } else if (rc == LC_OK && saved_errno != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces", strerror(saved_errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc == LC_OK) {
    pslog_field fields[1];

    fields[0] = lc_log_u64_field("count", namespace_count);
    lc_log_debug(pouch->logger, "cache.load.complete", fields, 1U);
  }
  return rc;
}

static int lc_pouch_validate_root_crypto_mode(lc_pouch *pouch,
                                              int *write_manifest,
                                              lc_error *error) {
  char stored_key_id[128];
  char stored_layout[64];
  char stored_mode[sizeof("encrypted")];
  char stored_compression[sizeof("zlib")];
  char *requested_key_id;
  const char *requested_mode;
  unsigned long stored_version;
  int found_crypto_mode;
  int found_compression;
  int found_manifest;
  int has_namespace_entries;
  int collection_head_bytes;
  int rc;

  requested_key_id = NULL;
  *write_manifest = 1;
  requested_mode =
      lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext";
  stored_version = 0UL;
  found_crypto_mode = 0;
  found_compression = 0;
  rc = lc_pouch_root_manifest_read(
      pouch, stored_layout, sizeof(stored_layout), &stored_version, stored_mode,
      sizeof(stored_mode), stored_key_id, sizeof(stored_key_id),
      &found_manifest, &found_crypto_mode, stored_compression,
      sizeof(stored_compression), &found_compression, &collection_head_bytes,
      error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!found_manifest) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest is required for populated root",
                          NULL, NULL, "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_layout, LC_POUCH_LAYOUT_NAME) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest layout is unsupported", NULL, NULL,
                        "pouch");
  }
  if (stored_version != LC_POUCH_LAYOUT_VERSION) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest version is unsupported", NULL,
                        NULL, "pouch");
  }
  if (!found_crypto_mode) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest requires crypto mode for populated root", NULL,
          NULL, "pouch");
    }
    if (stored_key_id[0] != '\0') {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest crypto key id requires crypto mode", NULL, NULL,
          "pouch");
    }
    return LC_OK;
  }
  if (!found_compression) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest requires compression mode for populated root",
          NULL, NULL, "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_mode, requested_mode) != 0) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto mode cannot be changed after initialization", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_mode, "plaintext") == 0 && stored_key_id[0] != '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch plaintext root manifest cannot contain crypto key id", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_mode, "encrypted") != 0) {
    rc = lc_pouch_prepare_collection_heads(pouch, collection_head_bytes, error);
    if (rc != LC_OK)
      return rc;
    *write_manifest = collection_head_bytes != LC_COLLECTION_HEAD_BYTES;
    return lc_pouch_resolve_compression(pouch, stored_compression, error);
  }
  if (stored_key_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted root manifest requires crypto key id",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_crypto_key_id(pouch->crypto, &requested_key_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (strcmp(stored_key_id, requested_key_id) != 0) {
    lc_free_with_allocator(NULL, requested_key_id);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto key does not match initialized encrypted root", NULL,
        NULL, "pouch");
  }
  lc_free_with_allocator(NULL, requested_key_id);
  rc = lc_pouch_prepare_collection_heads(pouch, collection_head_bytes, error);
  if (rc != LC_OK)
    return rc;
  *write_manifest = collection_head_bytes != LC_COLLECTION_HEAD_BYTES;
  return lc_pouch_resolve_compression(pouch, stored_compression, error);
}

static int lc_pouch_root_manifest_lock(lc_pouch *pouch, int *out,
                                       lc_error *error) {
  char *lock_path;
  struct flock fl;
  int fd;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest lock requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = -1;
  lock_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest.lock");
  if (lock_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest lock path",
                        NULL, NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch root manifest lock",
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
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest", strerror(errno),
                        NULL, "pouch");
  }
  *out = fd;
  return LC_OK;
}

static void lc_pouch_root_manifest_unlock(int fd) {
  struct flock fl;

  if (fd < 0) {
    return;
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_UNLCK;
  fl.l_whence = SEEK_SET;
  (void)fcntl(fd, F_SETLK, &fl);
  close(fd);
}

static int lc_pouch_ensure_root(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  int manifest_mutex_locked;
  int write_manifest;
  int lock_fd;
  int pthread_rc;
  int rc;
  int shared;

  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  manifest_mutex_locked = 0;
  lock_fd = -1;
  pthread_rc = pthread_mutex_lock(&pouch->writer_root_lock->manifest_mutex);
  if (pthread_rc != 0) {
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  manifest_mutex_locked = 1;
  shared = pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED;
  rc = lc_pouch_root_manifest_lock(pouch, &lock_fd, error);
  if (rc == LC_OK) {
    rc = lc_pouch_validate_root_crypto_mode(pouch, &write_manifest, error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_path_ensure_directory(
        namespaces_path, "failed to create pouch namespaces directory", error);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc == LC_OK && write_manifest) {
    rc = lc_pouch_write_root_manifest(pouch, error);
  }
  if (rc == LC_OK && shared &&
      pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE)
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, error);
  lc_pouch_root_manifest_unlock(lock_fd);
  if (manifest_mutex_locked) {
    pthread_mutex_unlock(&pouch->writer_root_lock->manifest_mutex);
  }
  return rc;
}

static int lc_pouch_init_writer_marker(lc_pouch *pouch, lc_error *error) {
  unsigned char writer_random[16];
  char marker_leaf[128];
  char presence_leaf[128];
  char writer_hex[33];
  char writer_marker_text[32];
  uint64_t writer_marker_id;
  size_t index;

  if (RAND_bytes(writer_random, (int)sizeof(writer_random)) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch writer identity", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < sizeof(writer_random); ++index) {
    (void)snprintf(writer_hex + (index * 2U), 3U, "%02x", writer_random[index]);
  }
  writer_hex[sizeof(writer_hex) - 1U] = '\0';
  pthread_mutex_lock(&lc_pouch_writer_marker_mutex);
  writer_marker_id = ++lc_pouch_next_writer_marker_id;
  pthread_mutex_unlock(&lc_pouch_writer_marker_mutex);
  if (lc_u64_format_base10_padded((lc_u64)writer_marker_id, 20U,
                                  writer_marker_text,
                                  sizeof(writer_marker_text)) < 0 ||
      snprintf(marker_leaf, sizeof(marker_leaf), "writer-%s-%s.marker",
               writer_hex, writer_marker_text) < 0 ||
      snprintf(presence_leaf, sizeof(presence_leaf), "writer-%s-%s.presence",
               writer_hex, writer_marker_text) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch writer marker", NULL, NULL,
                        "pouch");
  }
  pouch->writer_marker_leaf =
      lc_strdup_with_allocator(&pouch->allocator, marker_leaf);
  pouch->writer_presence_leaf =
      lc_strdup_with_allocator(&pouch->allocator, presence_leaf);
  pouch->writer_presence_dir = lc_pouch_path_join(
      &pouch->allocator, pouch->root_path, "exclusive-writers");
  pouch->writer_presence_path =
      pouch->writer_presence_dir != NULL
          ? lc_pouch_path_join(&pouch->allocator, pouch->writer_presence_dir,
                               presence_leaf)
          : NULL;
  if (pouch->writer_marker_leaf == NULL ||
      pouch->writer_presence_leaf == NULL ||
      pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer marker paths", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static lc_pouch_writer_root_lock_entry *
lc_pouch_writer_root_lock_find(dev_t device, ino_t inode) {
  lc_pouch_writer_root_lock_entry *entry;

  entry = lc_pouch_writer_root_locks;
  while (entry != NULL) {
    if (entry->device == device && entry->inode == inode) {
      return entry;
    }
    entry = entry->next;
  }
  return NULL;
}

static void lc_pouch_writer_root_lock_release(lc_pouch *pouch) {
  lc_pouch_writer_root_lock_entry *entry;
  lc_pouch_writer_root_lock_entry **link;
  struct flock fl;

  if (pouch == NULL || pouch->writer_root_lock == NULL) {
    return;
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  entry = pouch->writer_root_lock;
  if (pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    if (entry->exclusive_holders > 0UL) {
      --entry->exclusive_holders;
    }
  } else if (pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
             entry->shared_holders > 0UL) {
    --entry->shared_holders;
  }
  pouch->writer_root_lock = NULL;
  pouch->writer_root_lock_mode = LC_POUCH_WRITER_ROOT_LOCK_NONE;
  if (entry->exclusive_holders == 0UL && entry->shared_holders == 0UL) {
    link = &lc_pouch_writer_root_locks;
    while (*link != NULL && *link != entry) {
      link = &(*link)->next;
    }
    if (*link == entry) {
      *link = entry->next;
    }
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(entry->fd, F_SETLK, &fl);
    (void)close(entry->fd);
    (void)pthread_mutex_destroy(&entry->manifest_mutex);
    lc_free_with_allocator(&entry->allocator, entry);
  }
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
}

static int lc_pouch_writer_root_lock_acquire(lc_pouch *pouch, int mode,
                                             lc_error *error) {
  char *path;
  lc_pouch_writer_root_lock_entry *entry;
  struct flock fl;
  struct stat st;
  int fd;
  int saved_errno;

  if (pouch == NULL || (mode != LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
                        mode != LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock requires a valid mode", NULL,
                        NULL, "pouch");
  }
  if (pouch->writer_root_lock != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock is already held", NULL, NULL,
                        "pouch");
  }
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                            "writer-mode.lock");
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer root lock path", NULL,
                        NULL, "pouch");
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  if (stat(path, &st) == 0) {
    entry = lc_pouch_writer_root_lock_find(st.st_dev, st.st_ino);
    if (entry != NULL) {
      if ((mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE &&
           (entry->exclusive_holders != 0UL || entry->shared_holders != 0UL)) ||
          (mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
           entry->exclusive_holders != 0UL)) {
        pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
        lc_free_with_allocator(&pouch->allocator, path);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch root writer mode is already owned", NULL,
                            NULL, "pouch");
      }
      if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
        ++entry->exclusive_holders;
      } else {
        ++entry->shared_holders;
      }
      pouch->writer_root_lock = entry;
      pouch->writer_root_lock_mode = mode;
      pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
      lc_free_with_allocator(&pouch->allocator, path);
      return LC_OK;
    }
  } else if (errno != ENOENT) {
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    lc_free_with_allocator(&pouch->allocator, path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  fd = open(path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, path);
  if (fd < 0) {
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (fstat(fd, &st) != 0) {
    saved_errno = errno;
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE ? F_WRLCK : F_RDLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLK, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    saved_errno = errno;
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    if (saved_errno == EACCES || saved_errno == EAGAIN) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root writer mode is already owned", NULL, NULL,
                          "pouch");
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer root",
                        strerror(saved_errno), NULL, "pouch");
  }
  entry = (lc_pouch_writer_root_lock_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    fl.l_type = F_UNLCK;
    (void)fcntl(fd, F_SETLK, &fl);
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer root lock", NULL, NULL,
                        "pouch");
  }
  entry->allocator = pouch->allocator;
  saved_errno = pthread_mutex_init(&entry->manifest_mutex, NULL);
  if (saved_errno != 0) {
    fl.l_type = F_UNLCK;
    (void)fcntl(fd, F_SETLK, &fl);
    (void)close(fd);
    lc_free_with_allocator(&pouch->allocator, entry);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                        "failed to initialize root manifest mutex",
                        strerror(saved_errno), NULL, "pouch");
  }
  entry->device = st.st_dev;
  entry->inode = st.st_ino;
  entry->fd = fd;
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    entry->exclusive_holders = 1UL;
  } else {
    entry->shared_holders = 1UL;
  }
  entry->next = lc_pouch_writer_root_locks;
  lc_pouch_writer_root_locks = entry;
  pouch->writer_root_lock = entry;
  pouch->writer_root_lock_mode = mode;
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
  return LC_OK;
}

static int lc_pouch_writer_root_lock_change(lc_pouch *pouch, int mode,
                                            lc_error *error) {
  lc_pouch_writer_root_lock_entry *entry;
  struct flock fl;
  int saved_errno;

  if (pouch == NULL || pouch->writer_root_lock == NULL ||
      (mode != LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
       mode != LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock cannot change mode", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  entry = pouch->writer_root_lock;
  if (pouch->writer_root_lock_mode == mode) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return LC_OK;
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE &&
      (entry->exclusive_holders != 0UL || entry->shared_holders != 1UL)) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root has active shared writers", NULL, NULL,
                        "pouch");
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
      (entry->exclusive_holders != 1UL || entry->shared_holders != 0UL)) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root writer lock state is inconsistent", NULL,
                        NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE ? F_WRLCK : F_RDLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(entry->fd, F_SETLK, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    if (saved_errno == EACCES || saved_errno == EAGAIN) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root writer mode is already owned", NULL, NULL,
                          "pouch");
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to change pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    entry->shared_holders = 0UL;
    entry->exclusive_holders = 1UL;
  } else {
    entry->exclusive_holders = 0UL;
    entry->shared_holders = 1UL;
  }
  pouch->writer_root_lock_mode = mode;
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
  return LC_OK;
}

static int lc_pouch_prepare_collection_heads(lc_pouch *pouch, int stored_bytes,
                                             lc_error *error) {
  char *intent;
  int rc, pending;
  if (stored_bytes == LC_COLLECTION_HEAD_BYTES)
    return LC_OK;
  rc = lc_pouch_writer_root_lock_change(
      pouch, LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE, error);
  if (rc == LC_ERR_INVALID)
    return lc_error_set(error, LC_ERR_PENDING, 0L,
                        "collection upgrade requires exclusive root access; "
                        "close other clients and retry",
                        NULL, "collection_upgrade_busy", "pouch");
  if (rc != LC_OK)
    return rc;
  intent = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                              ".compression-ready");
  if (intent == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to inspect collection upgrade recovery", NULL,
                        NULL, "pouch");
  rc = lc_pc_exists(intent, &pending, error);
  lc_free_with_allocator(&pouch->allocator, intent);
  /* A released converter may have staged 128-byte replacement heads. Its
   * ready tree is authoritative: upgrade after installation, not the source
   * tree that recovery is about to retire. */
  if (rc == LC_OK && !pending)
    rc = lc_collection_migrate_heads(pouch, error);
  return rc;
}

static int lc_pouch_writer_root_lock_check_presence(lc_pouch *pouch,
                                                    lc_error *error) {
  lc_pouch_exclusive_writer_presence presence;
  int rc;

  memset(&presence, 0, sizeof(presence));
  rc = lc_pouch_probe_exclusive_writer(pouch, &presence, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (presence.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root has an unexpired exclusive writer", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

int lc_pouch_single_writer_snapshot(lc_pouch *pouch, uint64_t *epoch_out) {
  int enabled;

  if (pouch == NULL) {
    if (epoch_out != NULL) {
      *epoch_out = 0U;
    }
    return 0;
  }
  if (!pouch->single_writer_mutex_initialized) {
    if (epoch_out != NULL) {
      *epoch_out = pouch->single_writer_epoch;
    }
    return pouch->single_writer;
  }
  pthread_mutex_lock(&pouch->single_writer_mutex);
  enabled = pouch->single_writer;
  if (epoch_out != NULL) {
    *epoch_out = pouch->single_writer_epoch;
  }
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  return enabled;
}

int lc_pouch_single_writer_enabled(lc_pouch *pouch) {
  return lc_pouch_single_writer_snapshot(pouch, NULL);
}

int lc_pouch_writer_mode_operation_begin(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || !pouch->writer_mode_guard_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer mode operation requires an open pouch",
                        NULL, NULL, "pouch");
  }
  pthread_rc = pthread_rwlock_rdlock(&pouch->writer_mode_guard);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer mode operation",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

void lc_pouch_writer_mode_operation_end(lc_pouch *pouch) {
  if (pouch != NULL && pouch->writer_mode_guard_initialized) {
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
  }
}

static int lc_pouch_writer_presence_now_ns(int64_t *out, lc_error *error) {
  struct timespec now;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence requires output storage", NULL,
                        NULL, "pouch");
  }
  if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch writer presence clock",
                        strerror(errno), NULL, "pouch");
  }
  if (now.tv_sec < 0 ||
      (uintmax_t)now.tv_sec >
          ((uintmax_t)LC_I64_MAX - (uintmax_t)now.tv_nsec) / 1000000000U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence clock is out of range", NULL,
                        NULL, "pouch");
  }
  *out = (int64_t)now.tv_sec * (int64_t)1000000000 + (int64_t)now.tv_nsec;
  return LC_OK;
}

static int lc_pouch_writer_presence_touch(lc_pouch *pouch, lc_error *error) {
  char payload[64];
  int64_t now_ns = 0;
  int rc;

  if (pouch == NULL || pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence is not initialized", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_writer_presence_now_ns(&now_ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_i64_format_base10((lc_i64)now_ns, payload, sizeof(payload)) < 0 ||
      strlen(payload) + 2U > sizeof(payload) ||
      snprintf(payload + strlen(payload), sizeof(payload) - strlen(payload),
               "\n") < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to format pouch writer heartbeat", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_path_ensure_directory(
      pouch->writer_presence_dir,
      "failed to create pouch exclusive-writer directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_path_write_text_file_relaxed(pouch->writer_presence_path,
                                               payload, error);
}

static void *lc_pouch_writer_presence_main(void *context) {
  lc_pouch *pouch;

  pouch = (lc_pouch *)context;
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  while (!pouch->writer_presence_stop) {
    struct timespec deadline;
    int wait_rc;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += LC_POUCH_EXCLUSIVE_WRITER_TOUCH_NS;
    if (deadline.tv_nsec >= 1000000000L) {
      deadline.tv_sec += deadline.tv_nsec / 1000000000L;
      deadline.tv_nsec %= 1000000000L;
    }
    wait_rc = pthread_cond_timedwait(&pouch->writer_presence_cond,
                                     &pouch->writer_presence_mutex, &deadline);
    if (pouch->writer_presence_stop) {
      break;
    }
    if (wait_rc == 0 || wait_rc == ETIMEDOUT) {
      lc_error error;

      pthread_mutex_unlock(&pouch->writer_presence_mutex);
      lc_error_init(&error);
      (void)lc_pouch_writer_presence_touch(pouch, &error);
      lc_error_cleanup(&error);
      pthread_mutex_lock(&pouch->writer_presence_mutex);
    }
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  return NULL;
}

static int lc_pouch_writer_presence_start(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;
  int rc;

  if (pouch == NULL || !pouch->writer_presence_mutex_initialized ||
      !pouch->writer_presence_cond_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence is not initialized", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  if (pouch->writer_presence_thread_started) {
    pthread_mutex_unlock(&pouch->writer_presence_mutex);
    return LC_OK;
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  rc = lc_pouch_writer_presence_touch(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  pouch->writer_presence_stop = 0;
  pthread_rc = pthread_create(&pouch->writer_presence_thread, NULL,
                              lc_pouch_writer_presence_main, pouch);
  if (pthread_rc == 0) {
    pouch->writer_presence_thread_started = 1;
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  if (pthread_rc != 0) {
    (void)unlink(pouch->writer_presence_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch writer presence worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_writer_presence_stop_internal(lc_pouch *pouch,
                                                   int remove_marker) {
  int join_thread;

  if (pouch == NULL || !pouch->writer_presence_mutex_initialized) {
    return;
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  join_thread = pouch->writer_presence_thread_started;
  if (join_thread) {
    pouch->writer_presence_stop = 1;
    pthread_cond_signal(&pouch->writer_presence_cond);
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  if (join_thread) {
    (void)pthread_join(pouch->writer_presence_thread, NULL);
    pthread_mutex_lock(&pouch->writer_presence_mutex);
    pouch->writer_presence_thread_started = 0;
    pouch->writer_presence_stop = 0;
    pthread_mutex_unlock(&pouch->writer_presence_mutex);
  }
  if (remove_marker && pouch->writer_presence_path != NULL) {
    (void)unlink(pouch->writer_presence_path);
  }
}

static void lc_pouch_writer_presence_stop(lc_pouch *pouch) {
  lc_pouch_writer_presence_stop_internal(pouch, 1);
}

static void lc_pouch_writer_presence_stop_abrupt(lc_pouch *pouch) {
  lc_pouch_writer_presence_stop_internal(pouch, 0);
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_crypto_open_options crypto_options;
  int requested_single_writer;
  int pthread_rc;
  int rc;
  size_t key_mutex_index;
  struct stat root_stat;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_open requires root_path and out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  pouch = (lc_pouch *)lc_calloc_with_allocator(allocator, 1U, sizeof(*pouch));
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch handle", NULL, NULL, NULL);
  }
  if (allocator != NULL) {
    pouch->allocator = *allocator;
  } else {
    lc_allocator_init(&pouch->allocator);
  }
  pthread_rc = pthread_mutex_init(&pouch->single_writer_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch single-writer mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->single_writer_mutex_initialized = 1;
  pthread_rc = pthread_rwlock_init(&pouch->writer_mode_guard, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer mode guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_mode_guard_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->writer_presence_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer presence mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_presence_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->state_mutation_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state mutation mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->state_mutation_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->state_cache_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state cache mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->state_cache_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->query_flush_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch query flush mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->query_flush_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->query_doc_table_cache_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch doc-table cache mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->query_doc_table_cache_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->source_cache_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch source cache mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->source_cache_mutex_initialized = 1;
  for (key_mutex_index = 0U;
       key_mutex_index < LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT;
       ++key_mutex_index) {
    pthread_rc = lc_pouch_mutex_init_recursive(
        &pouch->exclusive_key_mutexes[key_mutex_index]);
    if (pthread_rc != 0) {
      lc_pouch_close(pouch);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch exclusive key mutex",
                          strerror(pthread_rc), NULL, "pouch");
    }
    pouch->exclusive_key_mutex_count += 1U;
  }
  pthread_rc = pthread_cond_init(&pouch->writer_presence_cond, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer presence condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_presence_cond_initialized = 1;
  pouch->root_path = lc_strdup_with_allocator(&pouch->allocator, root_path);
  if (pouch->root_path == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch root path", NULL, NULL, NULL);
  }
  lc_pouch_init_options(pouch, options);
  requested_single_writer = pouch->single_writer;
  pouch->single_writer = 0;
  pouch->single_writer_epoch = 1U;
  pouch->base_logger = options != NULL && options->logger != NULL
                           ? options->logger
                           : lc_log_noop_logger();
  pouch->logger = lc_log_pouch_logger(pouch->base_logger);
  if (pouch->logger == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch logger", NULL, NULL,
                        "pouch");
  }
  pouch->owns_logger = pouch->logger != pouch->base_logger;
  if (pouch->query_engine == NULL || pouch->query_fallback_engine == NULL ||
      pouch->compression == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch open options", NULL, NULL,
                        NULL);
  }
  if (strcmp(pouch->query_engine, "index") != 0 &&
      strcmp(pouch->query_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_engine must be index or scan", NULL, NULL,
                        "pouch");
  }
  if (pouch->query_fallback_engine[0] != '\0' &&
      strcmp(pouch->query_fallback_engine, "index") != 0 &&
      strcmp(pouch->query_fallback_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_fallback_engine must be index or scan",
                        NULL, NULL, "pouch");
  }
  if (strcmp(pouch->compression, "none") != 0 &&
      strcmp(pouch->compression, "zlib") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compression must be none or zlib", NULL, NULL,
                        "pouch");
  }
  memset(&crypto_options, 0, sizeof(crypto_options));
  if (options != NULL) {
    crypto_options.key_string = options->crypto_key;
    crypto_options.key_file = options->crypto_key_file;
    crypto_options.generate_key_file = options->crypto_generate_key_file;
  }
  crypto_options.compression_enabled =
      strcmp(pouch->compression, "zlib") == 0 ? 1 : 0;
  rc = lc_pouch_crypto_open(&pouch->allocator, &crypto_options, &pouch->crypto,
                            &pouch->crypto_key_file, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_init_writer_marker(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_set_single_writer(pouch, requested_single_writer, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_ensure_root(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  if (stat(pouch->root_path, &root_stat) != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to identify pouch root", strerror(errno), NULL,
                        "pouch");
  }
  pouch->root_device = (uint64_t)root_stat.st_dev;
  pouch->root_inode = (uint64_t)root_stat.st_ino;
  pouch->root_identity_initialized = 1;
  lc_pouch_configure_filesystem_capabilities(pouch);
  if (pouch->durable_sync) {
    rc = lc_pouch_fsync_batcher_init(pouch, error);
    if (rc != LC_OK) {
      lc_pouch_close(pouch);
      return rc;
    }
  }
  rc = lc_pouch_state_metadata_append_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_control_migration_run(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_warm_open_namespaces(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_compaction_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  if (pouch->query_indexing_enabled) {
    rc = lc_pouch_indexer_worker_init(pouch, error);
    if (rc != LC_OK) {
      lc_pouch_close(pouch);
      return rc;
    }
  }
  *out = pouch;
  {
    pslog_field fields[12];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    fields[1] = lc_log_str_field("query_engine", pouch->query_engine);
    fields[2] =
        lc_log_str_field("query_fallback_engine", pouch->query_fallback_engine);
    fields[3] = lc_log_str_field("compression", pouch->compression);
    fields[4] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[5] =
        lc_log_u64_field("segment_target_bytes", pouch->segment_target_bytes);
    fields[6] = lc_log_bool_field("single_writer",
                                  lc_pouch_single_writer_enabled(pouch));
    fields[7] = lc_log_bool_field("background_compaction",
                                  pouch->background_compaction_enabled);
    fields[8] = lc_log_bool_field("durable_sync", pouch->durable_sync);
    fields[9] =
        lc_log_u64_field("indexer_flush_docs", pouch->indexer_flush_docs);
    fields[10] = lc_log_u64_field("indexer_flush_interval_seconds",
                                  pouch->indexer_flush_interval_seconds);
    fields[11] =
        lc_log_bool_field("query_indexing", pouch->query_indexing_enabled);
    lc_log_info(pouch->logger, "open", fields, 12U);
  }
  return LC_OK;
}

void lc_pouch_close(lc_pouch *pouch) {
  lc_allocator allocator;

  if (pouch == NULL) {
    return;
  }
  allocator = pouch->allocator;
  if (pouch->logger != NULL) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    lc_log_debug(pouch->logger, "close", fields, 1U);
  }
  if (pouch->aborted) {
    lc_pouch_writer_presence_stop_abrupt(pouch);
  } else {
    lc_pouch_writer_presence_stop(pouch);
  }
  lc_pouch_indexer_worker_close(pouch);
  lc_pouch_compaction_worker_close(pouch);
  lc_pouch_state_metadata_append_worker_close(pouch);
  lc_pouch_fsync_batcher_close(pouch);
  if (!pouch->aborted) {
    lc_pouch_state_checkpoint_clean_close(pouch);
  }
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_state_source_cache_cleanup(pouch);
  lc_pouch_query_index_cache_cleanup(pouch);
  lc_pouch_writer_root_lock_release(pouch);
  lc_pouch_crypto_close(pouch->crypto);
  lc_free_with_allocator(&allocator, pouch->crypto_key_file);
  lc_free_with_allocator(&allocator, pouch->writer_presence_path);
  lc_free_with_allocator(&allocator, pouch->writer_presence_leaf);
  lc_free_with_allocator(&allocator, pouch->writer_presence_dir);
  lc_free_with_allocator(&allocator, pouch->writer_marker_leaf);
  lc_free_with_allocator(&allocator, pouch->compression);
  lc_free_with_allocator(&allocator, pouch->query_fallback_engine);
  lc_free_with_allocator(&allocator, pouch->query_engine);
  lc_free_with_allocator(&allocator, pouch->root_path);
  if (pouch->owns_logger && pouch->logger != NULL) {
    pouch->logger->destroy(pouch->logger);
  }
  if (pouch->writer_presence_cond_initialized) {
    pthread_cond_destroy(&pouch->writer_presence_cond);
  }
  if (pouch->writer_presence_mutex_initialized) {
    pthread_mutex_destroy(&pouch->writer_presence_mutex);
  }
  if (pouch->state_mutation_mutex_initialized) {
    pthread_mutex_destroy(&pouch->state_mutation_mutex);
  }
  if (pouch->state_cache_mutex_initialized) {
    pthread_mutex_destroy(&pouch->state_cache_mutex);
  }
  if (pouch->query_flush_mutex_initialized) {
    pthread_mutex_destroy(&pouch->query_flush_mutex);
  }
  if (pouch->query_doc_table_cache_mutex_initialized) {
    pthread_mutex_destroy(&pouch->query_doc_table_cache_mutex);
  }
  if (pouch->source_cache_mutex_initialized) {
    pthread_mutex_destroy(&pouch->source_cache_mutex);
  }
  while (pouch->exclusive_key_mutex_count > 0U) {
    pouch->exclusive_key_mutex_count -= 1U;
    (void)pthread_mutex_destroy(
        &pouch->exclusive_key_mutexes[pouch->exclusive_key_mutex_count]);
  }
  if (pouch->writer_mode_guard_initialized) {
    (void)pthread_rwlock_destroy(&pouch->writer_mode_guard);
  }
  if (pouch->single_writer_mutex_initialized) {
    pthread_mutex_destroy(&pouch->single_writer_mutex);
  }
  lc_free_with_allocator(&allocator, pouch);
}

int lc_pouch_abort(lc_pouch *pouch, lc_error *error) {
  if (pouch == NULL || !pouch->single_writer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch abort requires an open pouch", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&pouch->single_writer_mutex);
  if (pouch->aborted) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    return LC_OK;
  }
  pouch->aborted = 1;
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  lc_pouch_writer_presence_stop_abrupt(pouch);
  lc_pouch_indexer_worker_close(pouch);
  lc_pouch_compaction_worker_close(pouch);
  lc_pouch_state_metadata_append_worker_close(pouch);
  lc_pouch_fsync_batcher_close(pouch);
  /* Abort mirrors Go disk logstore close before ownership is released: no
   * resident append/read descriptor or derived cache may outlive this writer.
   */
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_state_source_cache_cleanup(pouch);
  lc_pouch_query_index_cache_cleanup(pouch);
  lc_pouch_writer_root_lock_release(pouch);
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    lc_log_info(pouch->logger, "abort", fields, 1U);
  }
  return LC_OK;
}

int lc_pouch_supports_concurrent_writes(const lc_pouch *pouch) {
  return pouch != NULL &&
                 !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, NULL)
             ? 1
             : 0;
}

int lc_pouch_fsync_stats_read(lc_pouch *pouch, lc_pouch_fsync_stats *out,
                              lc_error *error) {
  lc_pouch_fsync_batcher *batcher;
  size_t index;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync stats requires pouch and output", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  batcher = pouch->fsync_batcher;
  if (batcher != NULL) {
    pthread_mutex_lock(&batcher->mutex);
    *out = batcher->stats;
    pthread_mutex_unlock(&batcher->mutex);
  }
  for (index = 0U; index < LC_POUCH_FSYNC_BATCH_BOUND_COUNT; ++index) {
    out->bounds[index] = lc_pouch_fsync_batch_bounds[index];
  }
  return LC_OK;
}

static int
lc_pouch_backend_hash_derived(lc_pouch *pouch,
                              char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                              lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  const char *root;
  char current_directory[PATH_MAX];
  char *descriptor;
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len;
  size_t descriptor_len;
  size_t index;
  size_t root_len;
  size_t current_directory_len;
  int rc;

  if (pouch == NULL || out == NULL || pouch->root_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash requires pouch and output", NULL,
                        NULL, "pouch");
  }
  out[0] = '\0';
  root = pouch->root_path;
  root_len = strlen(root);
  if (root[0] != '/' &&
      getcwd(current_directory, sizeof(current_directory)) != NULL) {
    current_directory_len = strlen(current_directory);
    if (current_directory_len > SIZE_MAX - sizeof("pouch|/") ||
        root_len > SIZE_MAX - current_directory_len - sizeof("pouch|/")) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch backend hash descriptor exceeds limit", NULL,
                          NULL, "pouch");
    }
    descriptor_len = current_directory_len + root_len + sizeof("pouch|/");
    descriptor =
        (char *)lc_alloc_with_allocator(&pouch->allocator, descriptor_len);
    if (descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch backend hash descriptor",
                          NULL, NULL, "pouch");
    }
    snprintf(descriptor, descriptor_len, "pouch|%s/%s", current_directory,
             root);
  } else {
    if (root_len > SIZE_MAX - sizeof("pouch|")) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch backend hash descriptor exceeds limit", NULL,
                          NULL, "pouch");
    }
    descriptor_len = root_len + sizeof("pouch|");
    descriptor =
        (char *)lc_alloc_with_allocator(&pouch->allocator, descriptor_len);
    if (descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch backend hash descriptor",
                          NULL, NULL, "pouch");
    }
    snprintf(descriptor, descriptor_len, "pouch|%s", root);
  }
  ctx = EVP_MD_CTX_new();
  digest_len = 0U;
  rc = LC_OK;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      EVP_DigestUpdate(ctx, descriptor, strlen(descriptor)) != 1 ||
      EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1 || digest_len != 32U) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to calculate pouch backend hash", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    for (index = 0U; index < digest_len; ++index) {
      out[index * 2U] = hex[(digest[index] >> 4U) & 0x0fU];
      out[index * 2U + 1U] = hex[digest[index] & 0x0fU];
    }
    out[digest_len * 2U] = '\0';
  }
  OPENSSL_cleanse(digest, sizeof(digest));
  EVP_MD_CTX_free(ctx);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  return rc;
}

static int lc_pouch_backend_hash_marker_read(
    lc_pouch *pouch, char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U], int *found,
    lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t index;
  int rc;

  if (pouch == NULL || out == NULL || found == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash marker requires inputs", NULL, NULL,
                        "pouch");
  }
  out[0] = '\0';
  *found = 0;
  memset(&read_result, 0, sizeof(read_result));
  sink = NULL;
  rc = lc_pouch_state_read(pouch, ".lockd", "backend-id", &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && read_result.found) {
    if (length != LC_POUCH_BACKEND_HASH_HEX_BYTES) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch backend hash marker has invalid length", NULL,
                        NULL, "pouch");
    }
    for (index = 0U; rc == LC_OK && index < length; ++index) {
      unsigned char value;

      value = ((const unsigned char *)bytes)[index];
      if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f'))) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch backend hash marker is not lowercase hex",
                          NULL, NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      memcpy(out, bytes, length);
      out[length] = '\0';
      *found = 1;
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &read_result);
  return rc;
}

static int lc_pouch_backend_hash_marker_write(
    lc_pouch *pouch, const char value[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
    lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  rc = lc_source_from_memory(value, LC_POUCH_BACKEND_HASH_HEX_BYTES, &source,
                             error);
  if (rc == LC_OK) {
    options.content_type = "text/plain";
    options.create_if_absent = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(pouch, ".lockd", "backend-id", source, &options,
                              &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
  return rc;
}

static int lc_pouch_backend_hash_create_collision(const lc_error *error) {
  return error != NULL && error->code == LC_ERR_INVALID &&
         error->message != NULL &&
         strstr(error->message, "create-if-absent precondition failed") != NULL;
}

int lc_pouch_backend_hash(lc_pouch *pouch,
                          char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                          lc_error *error) {
  char derived[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  int found;
  int rc;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash requires pouch and output", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_backend_hash_derived(pouch, derived, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_backend_hash_marker_read(pouch, out, &found, error);
  if (rc != LC_OK || found) {
    return rc;
  }
  rc = lc_pouch_backend_hash_marker_write(pouch, derived, error);
  if (rc == LC_OK) {
    memcpy(out, derived, sizeof(derived));
    return LC_OK;
  }
  if (!lc_pouch_backend_hash_create_collision(error)) {
    return rc;
  }
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lc_pouch_backend_hash_marker_read(pouch, out, &found, error);
  if (rc != LC_OK || found) {
    return rc;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch backend hash marker disappeared after collision",
                      NULL, NULL, "pouch");
}

int lc_pouch_set_single_writer(lc_pouch *pouch, int enabled, lc_error *error) {
  lc_error restart_error;
  int had_root_lock;
  int mode_changed;
  int normalized;
  int rc;

  if (pouch == NULL || !pouch->single_writer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control requires an open pouch",
                        NULL, NULL, "pouch");
  }
  normalized = enabled != 0 ? 1 : 0;
  if (!pouch->writer_mode_guard_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control requires a mode guard",
                        NULL, NULL, "pouch");
  }
  rc = pthread_rwlock_wrlock(&pouch->writer_mode_guard);
  if (rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer mode transition",
                        strerror(rc), NULL, "pouch");
  }
  mode_changed = 0;
  pthread_mutex_lock(&pouch->single_writer_mutex);
  if (pouch->aborted) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control is closed after abort",
                        NULL, NULL, "pouch");
  }
  had_root_lock = pouch->writer_root_lock != NULL;
  if (pouch->single_writer == normalized && had_root_lock) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
    return LC_OK;
  }
  if (!had_root_lock) {
    rc = lc_pouch_writer_root_lock_acquire(
        pouch,
        normalized ? LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE
                   : LC_POUCH_WRITER_ROOT_LOCK_SHARED,
        error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_root_lock_check_presence(pouch, error);
    if (rc != LC_OK) {
      lc_pouch_writer_root_lock_release(pouch);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    if (normalized) {
      rc = lc_pouch_writer_presence_start(pouch, error);
      if (rc != LC_OK) {
        lc_pouch_writer_root_lock_release(pouch);
        pthread_mutex_unlock(&pouch->single_writer_mutex);
        (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
        return rc;
      }
    }
    pouch->single_writer = normalized;
    mode_changed = 1;
  } else if (normalized) {
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_root_lock_check_presence(pouch, error);
    if (rc != LC_OK) {
      lc_error downgrade_error;

      lc_error_init(&downgrade_error);
      (void)lc_pouch_writer_root_lock_change(
          pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, &downgrade_error);
      lc_error_cleanup(&downgrade_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_presence_start(pouch, error);
    if (rc != LC_OK) {
      lc_error downgrade_error;

      lc_error_init(&downgrade_error);
      (void)lc_pouch_writer_root_lock_change(
          pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, &downgrade_error);
      lc_error_cleanup(&downgrade_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    pouch->single_writer = 1;
    mode_changed = 1;
  } else {
    lc_pouch_writer_presence_stop(pouch);
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, error);
    if (rc != LC_OK) {
      lc_error_init(&restart_error);
      (void)lc_pouch_writer_presence_start(pouch, &restart_error);
      lc_error_cleanup(&restart_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    pouch->single_writer = 0;
    mode_changed = 1;
  }
  if (mode_changed && pouch->single_writer_epoch != LC_U64_MAX) {
    ++pouch->single_writer_epoch;
  }
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
  {
    pslog_field fields[1];

    fields[0] = lc_log_bool_field("single_writer", normalized);
    lc_log_info(pouch->logger, "single_writer.set", fields, 1U);
  }
  return LC_OK;
}

static int lc_pouch_writer_presence_read(const char *path,
                                         int64_t *heartbeat_ns,
                                         int *has_heartbeat, lc_error *error) {
  char buffer[128];
  size_t length;
  int fd;

  if (path == NULL || heartbeat_ns == NULL || has_heartbeat == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer heartbeat requires path and outputs",
                        NULL, NULL, "pouch");
  }
  *heartbeat_ns = 0;
  *has_heartbeat = 0;
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      return LC_POUCH_WRITER_PRESENCE_MISSING;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch writer heartbeat",
                        strerror(errno), NULL, "pouch");
  }
  length = 0U;
  for (;;) {
    ssize_t got;

    if (length + 1U >= sizeof(buffer)) {
      break;
    }
    got = read(fd, buffer + length, sizeof(buffer) - length - 1U);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      close(fd);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch writer heartbeat",
                          strerror(errno), NULL, "pouch");
    }
    if (got == 0) {
      break;
    }
    length += (size_t)got;
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch writer heartbeat",
                        strerror(errno), NULL, "pouch");
  }
  buffer[length] = '\0';
  if (length > 0U) {
    char *end;
    lc_i64 value;

    while (*buffer == ' ' || *buffer == '\t' || *buffer == '\r' ||
           *buffer == '\n') {
      memmove(buffer, buffer + 1U, strlen(buffer));
    }
    end = buffer + strlen(buffer);
    while (end > buffer && (end[-1] == ' ' || end[-1] == '\t' ||
                            end[-1] == '\r' || end[-1] == '\n')) {
      *--end = '\0';
    }
    if (lc_i64_parse_base10(buffer, &value) && value > 0) {
      *heartbeat_ns = (int64_t)value;
      *has_heartbeat = 1;
    }
  }
  return LC_OK;
}

static int lc_pouch_writer_presence_mtime_ns(const struct stat *st,
                                             int64_t *out) {
  int64_t seconds;
  long nanos;

  if (st == NULL || out == NULL) {
    return 0;
  }
#if defined(__APPLE__)
  seconds = (int64_t)st->st_mtimespec.tv_sec;
  nanos = st->st_mtimespec.tv_nsec;
#else
  seconds = (int64_t)st->st_mtim.tv_sec;
  nanos = st->st_mtim.tv_nsec;
#endif
  if (seconds < 0 || nanos < 0 ||
      (uintmax_t)seconds >
          ((uintmax_t)LC_I64_MAX - (uintmax_t)nanos) / 1000000000U) {
    return 0;
  }
  *out = seconds * (int64_t)1000000000 + (int64_t)nanos;
  return 1;
}

int lc_pouch_probe_exclusive_writer(lc_pouch *pouch,
                                    lc_pouch_exclusive_writer_presence *out,
                                    lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  int64_t now_ns = 0;
  int rc;

  if (pouch == NULL || out == NULL || pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer probe requires an open pouch and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_writer_presence_now_ns(&now_ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(pouch->writer_presence_dir);
  if (dir == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch exclusive-writer directory",
                        strerror(errno), NULL, "pouch");
  }
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *path;
    struct stat st;
    int64_t heartbeat_ns;
    int64_t expires_ns;
    int has_heartbeat;

    if (entry->d_name[0] == '\0' ||
        strcmp(entry->d_name, pouch->writer_presence_leaf) == 0) {
      continue;
    }
    path = lc_pouch_path_join(&pouch->allocator, pouch->writer_presence_dir,
                              entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer presence path", NULL,
                        NULL, "pouch");
      break;
    }
    if (stat(path, &st) != 0) {
      int saved_errno;

      saved_errno = errno;
      lc_free_with_allocator(&pouch->allocator, path);
      if (saved_errno == ENOENT) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer heartbeat",
                        strerror(saved_errno), NULL, "pouch");
      break;
    }
    if (!S_ISREG(st.st_mode)) {
      lc_free_with_allocator(&pouch->allocator, path);
      continue;
    }
    rc = lc_pouch_writer_presence_read(path, &heartbeat_ns, &has_heartbeat,
                                       error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc == LC_POUCH_WRITER_PRESENCE_MISSING) {
      rc = LC_OK;
      continue;
    }
    if (rc != LC_OK) {
      break;
    }
    if (!has_heartbeat &&
        !lc_pouch_writer_presence_mtime_ns(&st, &heartbeat_ns)) {
      continue;
    }
    expires_ns = heartbeat_ns > LC_I64_MAX - LC_POUCH_EXCLUSIVE_WRITER_TTL_NS
                     ? LC_I64_MAX
                     : heartbeat_ns + LC_POUCH_EXCLUSIVE_WRITER_TTL_NS;
    if (expires_ns > now_ns) {
      out->present = 1;
      out->expires_at_unix = expires_ns / (int64_t)1000000000;
      break;
    }
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch exclusive-writer directory",
                      strerror(errno), NULL, "pouch");
  }
  return rc;
}

int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_status_read requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->root_path =
      lc_strdup_with_allocator(&pouch->allocator, pouch->root_path);
  out->layout_name =
      lc_strdup_with_allocator(&pouch->allocator, LC_POUCH_LAYOUT_NAME);
  if (out->root_path == NULL || out->layout_name == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy pouch status",
                        NULL, NULL, NULL);
  }
  out->layout_version = LC_POUCH_LAYOUT_VERSION;
  out->segment_target_bytes = pouch->segment_target_bytes;
  out->indexer_flush_docs = pouch->indexer_flush_docs;
  out->indexer_flush_interval_seconds = pouch->indexer_flush_interval_seconds;
  out->compaction_min_segment_count = pouch->compaction_min_segment_count;
  out->compaction_min_reclaimable_bytes =
      pouch->compaction_min_reclaimable_bytes;
  out->compaction_interval_seconds = pouch->compaction_interval_seconds;
  out->compaction_delete_grace_seconds = pouch->compaction_delete_grace_seconds;
  out->compaction_max_io_bytes_per_sec = pouch->compaction_max_io_bytes_per_sec;
  out->terminal_reclaim_min_bytes = pouch->terminal_reclaim_min_bytes;
  out->background_compaction_enabled = pouch->background_compaction_enabled;
  out->compaction_throttling_disabled = pouch->compaction_throttling_disabled;
  out->single_writer = lc_pouch_single_writer_enabled(pouch);
  out->supports_concurrent_writes = lc_pouch_supports_concurrent_writes(pouch);
  out->aborted = pouch->aborted;
  out->durable_sync = pouch->durable_sync;
  out->fsync_batch_max_ops = pouch->fsync_batch_max_ops;
  out->queue_watch_enabled = pouch->queue_watch_enabled;
  out->filesystem_capabilities_known = pouch->filesystem_capabilities_known;
  out->filesystem_is_nfs = pouch->filesystem_is_nfs;
  out->queue_watch_mode =
      lc_strdup_with_allocator(&pouch->allocator, pouch->queue_watch_mode);
  out->queue_watch_reason =
      lc_strdup_with_allocator(&pouch->allocator, pouch->queue_watch_reason);
  out->query_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_engine);
  out->query_fallback_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_fallback_engine);
  out->query_indexing_enabled = pouch->query_indexing_enabled;
  out->compression =
      lc_strdup_with_allocator(&pouch->allocator, pouch->compression);
  out->crypto_enabled = lc_pouch_crypto_enabled(pouch->crypto);
  out->crypto_key_file =
      pouch->crypto_key_file != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, pouch->crypto_key_file)
          : NULL;
  if (out->queue_watch_mode == NULL || out->queue_watch_reason == NULL ||
      out->query_engine == NULL || out->query_fallback_engine == NULL ||
      out->compression == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch status options", NULL, NULL,
                        NULL);
  }
  if (pouch->crypto_key_file != NULL && out->crypto_key_file == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch crypto key file status", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("path", out->root_path);
    fields[1] = lc_log_str_field("layout", out->layout_name);
    fields[2] = lc_log_u64_field("version", out->layout_version);
    fields[3] = lc_log_bool_field("crypto", out->crypto_enabled);
    fields[4] = lc_log_str_field("compression", out->compression);
    lc_log_trace(pouch->logger, "status.read", fields, 5U);
  }
  return LC_OK;
}

void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status) {
  if (status == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, status->root_path);
  lc_free_with_allocator(allocator, status->layout_name);
  lc_free_with_allocator(allocator, status->queue_watch_mode);
  lc_free_with_allocator(allocator, status->queue_watch_reason);
  lc_free_with_allocator(allocator, status->query_engine);
  lc_free_with_allocator(allocator, status->query_fallback_engine);
  lc_free_with_allocator(allocator, status->compression);
  lc_free_with_allocator(allocator, status->crypto_key_file);
  memset(status, 0, sizeof(*status));
}

int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *ns,
                              lc_error *error) {
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_ensure_namespace requires pouch", NULL, NULL,
                        NULL);
  }
  rc =
      lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path, ns, error);
  if (rc != LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "manifest.ensure_namespace.error", fields, 3U);
    return rc;
  }
  rc = lc_pouch_state_recover_staged_decisions(pouch, ns, error);
  if (rc == LC_OK) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("ns", ns);
    lc_log_trace(pouch->logger, "manifest.ensure_namespace", fields, 1U);
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "manifest.recover_staged.error", fields, 3U);
  }
  return rc;
}

#ifdef LOCKDC_TEST_BUILD
lc_pouch_recode_hook_fn lc_pouch_recode_test_hook;
void *lc_pouch_recode_test_context;
#endif

const char *lc_pouch_compression_mode(const lc_pouch *pouch) {
  return pouch->compression;
}

static int lc_pc_phase(int phase, lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_recode_test_hook)
    return lc_pouch_recode_test_hook(lc_pouch_recode_test_context, phase,
                                     error);
#else
  (void)phase;
  (void)error;
#endif
  return LC_OK;
}

static int lc_pc_mode(lc_pouch *pouch, const char *mode, lc_error *error) {
  lc_pouch_crypto_open_options options;
  char *copy;
  int rc = LC_OK;
  if (!strcmp(pouch->compression, mode))
    return LC_OK;
  copy = lc_strdup_with_allocator(&pouch->allocator, mode);
  if (!copy)
    return lc_error_set(error, LC_ERR_NOMEM, 0,
                        "failed to select root compression", NULL, NULL,
                        "pouch");
  if (!pouch->crypto && !strcmp(mode, "zlib")) {
    memset(&options, 0, sizeof(options));
    options.compression_enabled = 1;
    rc = lc_pouch_crypto_open(&pouch->allocator, &options, &pouch->crypto, NULL,
                              error);
  }
  if (rc == LC_OK) {
    lc_pouch_crypto_set_compression(pouch->crypto, !strcmp(mode, "zlib"));
    lc_free_with_allocator(&pouch->allocator, pouch->compression);
    pouch->compression = copy;
  } else
    lc_free_with_allocator(&pouch->allocator, copy);
  return rc;
}

static int lc_pc_exists(const char *path, int *exists, lc_error *error) {
  struct stat st;
  *exists = 0;
  if (lstat(path, &st) == 0) {
    *exists = 1;
    return LC_OK;
  }
  return errno == ENOENT
             ? LC_OK
             : lc_error_set(error, LC_ERR_TRANSPORT, 0,
                            "failed to inspect compression conversion",
                            strerror(errno), path, "pouch");
}

static void lc_pc_log(lc_pouch *pouch, const char *event, int warning,
                      const char *from, const char *to, lc_u64 input,
                      lc_u64 output, lc_u64 elapsed, int recovered) {
  pslog_field fields[7];
  fields[0] = lc_log_str_field("path", pouch->root_path);
  fields[1] = lc_log_str_field("from", from);
  fields[2] = lc_log_str_field("to", to);
  fields[3] = lc_log_u64_field("input_bytes", input);
  fields[4] = lc_log_u64_field("output_bytes", output);
  fields[5] = lc_log_u64_field("elapsed_ms", elapsed);
  fields[6] = lc_log_bool_field("recovered", recovered);
  if (warning)
    lc_log_warn(pouch->logger, event, fields, 7);
  else
    lc_log_info(pouch->logger, event, fields, 7);
}

/* Ready means every replacement file and directory is durable. The intent is
 * removed only after the new manifest and old-tree reclamation are durable. */
static int lc_pc_publish(lc_pouch *pouch, const char *intent, const char *fresh,
                         const char *old, const char *current,
                         const char *stored, const char *target, int recovering,
                         lc_error *error) {
  int has_current, has_old, has_fresh, rc;
  rc = lc_pc_exists(current, &has_current, error);
  if (rc == LC_OK)
    rc = lc_pc_exists(old, &has_old, error);
  if (rc == LC_OK)
    rc = lc_pc_exists(fresh, &has_fresh, error);
  if (rc != LC_OK)
    return rc;
  if (has_fresh && !has_old && has_current) {
    if (rename(current, old) != 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                          "failed to preserve source namespaces",
                          strerror(errno), NULL, "pouch");
    has_current = 0;
    has_old = 1;
    rc = lc_pouch_path_fsync_directory(
        pouch->root_path, "failed to sync preserved namespaces", error);
    if (rc == LC_OK)
      rc = lc_pc_phase(2, error);
  }
  if (rc == LC_OK && has_fresh && has_old && !has_current) {
    if (rename(fresh, current) != 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                          "failed to install converted namespaces",
                          strerror(errno), NULL, "pouch");
    has_fresh = 0;
    has_current = 1;
    rc = lc_pouch_path_fsync_directory(
        pouch->root_path, "failed to sync converted namespaces", error);
    if (rc == LC_OK)
      rc = lc_pc_phase(3, error);
  }
  if (rc == LC_OK &&
      (!has_current || has_fresh || (!has_old && strcmp(stored, target))))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "compression conversion publication is incomplete", NULL,
                      NULL, "pouch");
  if (rc == LC_OK && recovering)
    rc = lc_collection_migrate_heads(pouch, error);
  if (rc == LC_OK)
    rc = lc_pc_mode(pouch, target, error);
  if (rc == LC_OK)
    rc = lc_pouch_write_root_manifest(pouch, error);
  if (rc == LC_OK)
    rc = lc_pc_phase(4, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_remove_tree(old, error);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        pouch->root_path, "failed to sync conversion reclamation", error);
  if (rc == LC_OK)
    rc = lc_pc_phase(5, error);
  if (rc == LC_OK && unlink(intent) != 0)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0,
                      "failed to retire compression intent", strerror(errno),
                      NULL, "pouch");
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        pouch->root_path, "failed to sync compression completion", error);
  return rc;
}

static int lc_pc_read_intent(const char *path, char from[5], char to[5],
                             lc_u64 *input, lc_u64 *output, lc_error *error) {
  char bytes[128], *token, *save = NULL, *in, *out;
  FILE *fp = fopen(path, "rb");
  size_t count;
  int rc = LC_OK;
  if (!fp)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                        "failed to read compression intent", strerror(errno),
                        NULL, "pouch");
  count = fread(bytes, 1, sizeof(bytes), fp);
  if (ferror(fp))
    rc = LC_ERR_TRANSPORT;
  (void)fclose(fp);
  if (rc != LC_OK || count == sizeof(bytes))
    goto invalid;
  bytes[count] = '\0';
  token = strtok_r(bytes, "\n", &save);
  if (!token || strcmp(token, "pouch-compression-v1"))
    goto invalid;
  token = strtok_r(NULL, "\n", &save);
  if (!token || (strcmp(token, "none") && strcmp(token, "zlib")))
    goto invalid;
  strcpy(from, token);
  token = strtok_r(NULL, "\n", &save);
  if (!token || (strcmp(token, "none") && strcmp(token, "zlib")))
    goto invalid;
  strcpy(to, token);
  in = strtok_r(NULL, "\n", &save);
  out = strtok_r(NULL, "\n", &save);
  if (!in || !out || !strcmp(from, to) || !lc_u64_parse_base10(in, input) ||
      !lc_u64_parse_base10(out, output) || strtok_r(NULL, "\n", &save))
    goto invalid;
  return LC_OK;
invalid:
  return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "invalid Pouch compression conversion intent", NULL, NULL,
                      "pouch");
}

static int lc_pouch_resolve_compression(lc_pouch *pouch, const char *stored,
                                        lc_error *error) {
  char requested[5], previous[5], target[5], text[128], in[32], out[32];
  char *intent, *fresh, *old, *current;
  struct timespec started = {0, 0}, ended = {0, 0};
  lc_u64 input = 0, output = 0, elapsed = 0;
  int pending, shared, rc;
  strcpy(requested, pouch->compression);
  strcpy(previous, stored);
  intent = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                              ".compression-ready");
  fresh = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                             ".compression-new");
  old = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                           ".compression-old");
  current =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  rc = intent && fresh && old && current ? lc_pc_exists(intent, &pending, error)
                                         : LC_ERR_NOMEM;
  shared = pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED;
  if (rc == LC_OK && !pending &&
      (!pouch->compression_explicit || !strcmp(requested, stored))) {
    rc = lc_pc_mode(pouch, stored, error);
    goto done;
  }
  if (rc == LC_OK && shared) {
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE, error);
    if (rc == LC_ERR_INVALID) {
      lc_pc_log(pouch, "compression.convert.refused", 1, stored, requested, 0,
                0, 0, pending);
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = pending
               ? lc_error_set(
                     error, LC_ERR_PENDING, 0,
                     "compression recovery requires exclusive root ownership",
                     NULL, NULL, "pouch")
               : lc_pc_mode(pouch, stored, error);
      goto done;
    }
  }
  if (rc == LC_OK && pending) {
    rc = lc_pc_read_intent(intent, previous, target, &input, &output, error);
    if (rc == LC_OK) {
      (void)clock_gettime(CLOCK_MONOTONIC, &started);
      lc_pc_log(pouch, "compression.convert.start", 1, previous, target, input,
                output, 0, 1);
      rc = lc_pc_publish(pouch, intent, fresh, old, current, stored, target, 1,
                         error);
      (void)clock_gettime(CLOCK_MONOTONIC, &ended);
      elapsed = (lc_u64)(ended.tv_sec - started.tv_sec) * 1000 +
                (ended.tv_nsec - started.tv_nsec) / 1000000;
      if (rc == LC_OK) {
        lc_pc_log(pouch, "compression.convert.complete", 0, previous, target,
                  input, output, elapsed, 1);
        strcpy(previous, target);
      }
    }
  }
  if (rc == LC_OK && pouch->compression_explicit &&
      strcmp(requested, previous)) {
    (void)clock_gettime(CLOCK_MONOTONIC, &started);
    lc_pc_log(pouch, "compression.convert.start", 1, previous, requested, 0, 0,
              0, 0);
    rc = lc_pc_mode(pouch, requested, error);
    if (rc == LC_OK)
      rc = lc_pouch_recode_build(pouch, current, fresh, &input, &output, error);
    if (rc == LC_OK)
      rc = lc_pc_phase(0, error);
    if (rc == LC_OK) {
      (void)lc_u64_format_base10(input, in, sizeof(in));
      (void)lc_u64_format_base10(output, out, sizeof(out));
      (void)snprintf(text, sizeof(text),
                     "pouch-compression-v1\n%s\n%s\n%s\n%s\n", previous,
                     requested, in, out);
      rc = lc_pouch_path_write_text_file(intent, text, error);
    }
    if (rc == LC_OK)
      rc = lc_pc_phase(1, error);
    if (rc == LC_OK)
      rc = lc_pc_publish(pouch, intent, fresh, old, current, previous,
                         requested, 0, error);
    (void)clock_gettime(CLOCK_MONOTONIC, &ended);
    elapsed = (lc_u64)(ended.tv_sec - started.tv_sec) * 1000 +
              (ended.tv_nsec - started.tv_nsec) / 1000000;
    if (rc == LC_OK)
      lc_pc_log(pouch, "compression.convert.complete", 0, previous, requested,
                input, output, elapsed, 0);
  }
  if (rc == LC_OK && shared)
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, error);
done:
  lc_free_with_allocator(&pouch->allocator, intent);
  lc_free_with_allocator(&pouch->allocator, fresh);
  lc_free_with_allocator(&pouch->allocator, old);
  lc_free_with_allocator(&pouch->allocator, current);
  if (rc != LC_OK && error && error->code == LC_OK)
    lc_error_set(error, rc, 0, "failed to resolve Pouch root compression", NULL,
                 NULL, "pouch");
  return rc;
}
