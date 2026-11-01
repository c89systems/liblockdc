#include "lc_collection_internal.h"
#include "lc_collection_json.h"
#include "lc_collection_query.h"
#include "lc_log.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_recode.h"
#include "lc_pouch_record.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define LC_CC_HEAD_MAGIC 0x4c434348UL
#define LC_CC_CLEAN_MAGIC 0x4c43434cUL

static pthread_mutex_t lc_collection_lifecycle_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static lc_collection_call *lc_collection_calls;
#ifdef LOCKDC_TEST_BUILD
lc_collection_root_hook_fn lc_collection_root_test_hook;
void *lc_collection_root_test_context;
lc_collection_commit_hook_fn lc_collection_commit_wait_test_hook;
void *lc_collection_commit_wait_test_context;
#endif
static void lc_collection_destroy(lc_collection_handle *collection);
#ifdef LOCKDC_TEST_BUILD
lc_collection_append_hook_fn lc_collection_append_test_hook = NULL;
void *lc_collection_append_test_context = NULL;
#endif
static int lc_collection_reconcile(lc_collection_handle *collection,
                                   lc_error *error);

int lc_collection_failure(lc_error *error, int code, const char *reason,
                          const char *message) {
  return lc_error_set(error, code, 0L, message,
                      code == LC_ERR_TRANSPORT ? strerror(errno) : NULL, reason,
                      "pouch");
}

int lc_collection_hash(const void *bytes, size_t length, unsigned char out[32],
                       lc_error *error) {
  unsigned int size;
  size = 0U;
  if (EVP_Digest(bytes, length, out, &size, EVP_sha256(), NULL) != 1 ||
      size != 32U) {
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to hash collection identity");
  }
  return LC_OK;
}

static void lc_cc_put(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)value;
    value >>= 8U;
  }
}

static lc_u64 lc_cc_get(const unsigned char *bytes) {
  lc_u64 value;
  unsigned int i;
  value = 0U;
  for (i = 8U; i > 0U; --i)
    value = (value << 8U) | bytes[i - 1U];
  return value;
}

static void lc_cc_seal(unsigned char *bytes, size_t length) {
  unsigned long crc;
  crc = (unsigned long)crc32(0L, bytes, (uInt)(length - 4U));
  bytes[length - 4U] = (unsigned char)crc;
  bytes[length - 3U] = (unsigned char)(crc >> 8U);
  bytes[length - 2U] = (unsigned char)(crc >> 16U);
  bytes[length - 1U] = (unsigned char)(crc >> 24U);
}

static int lc_cc_valid(const unsigned char *bytes, size_t length) {
  unsigned long stored;
  stored = (unsigned long)bytes[length - 4U] |
           ((unsigned long)bytes[length - 3U] << 8U) |
           ((unsigned long)bytes[length - 2U] << 16U) |
           ((unsigned long)bytes[length - 1U] << 24U);
  return stored == (unsigned long)crc32(0L, bytes, (uInt)(length - 4U));
}

static int lc_cc_read_file(const char *path, unsigned char *bytes,
                           size_t length, int optional, int *found, int *pin,
                           lc_error *error) {
  size_t done;
  ssize_t got;
  unsigned char extra;
  int fd;
  int rc;
  if (found != NULL)
    *found = 0;
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (optional && errno == ENOENT)
      return LC_OK;
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection metadata");
  }
  done = 0U;
  rc = LC_OK;
  while (done < length) {
    got = read(fd, bytes + done, length - done);
    if (got < 0 && errno == EINTR)
      continue;
    if (got <= 0) {
      rc = lc_collection_failure(
          error, got == 0 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
          "collection_corrupt", "collection metadata is truncated");
      break;
    }
    done += (size_t)got;
  }
  if (rc == LC_OK) {
    do {
      got = read(fd, &extra, 1U);
    } while (got < 0 && errno == EINTR);
    if (got != 0)
      rc = lc_collection_failure(
          error, got < 0 ? LC_ERR_TRANSPORT : LC_ERR_PROTOCOL,
          "collection_corrupt", "collection metadata has trailing bytes");
  }
  if (rc == LC_OK && pin != NULL) {
    if (*pin >= 0)
      (void)close(*pin);
    *pin = fd;
  } else
    (void)close(fd);
  if (rc == LC_OK && found != NULL)
    *found = 1;
  return rc;
}

static int lc_cc_lock(int fd, int operation, lc_error *error) {
  while (flock(fd, operation) != 0) {
    if (errno == EINTR)
      continue;
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to lock collection authority");
  }
  return LC_OK;
}

static char *lc_cc_path(lc_collection_handle *collection, const char *leaf) {
  return lc_pouch_path_join(&collection->allocator, collection->directory,
                            leaf);
}

/* Distinct open descriptions are required: flock on a receiver-owned fd would
 * let concurrent calls on that receiver upgrade each other's shared pins. */
static int lc_cc_commit_enter(lc_collection_handle *collection, int exclusive,
                              int *pin, lc_error *error) {
  char *path;
  int rc;
  *pin = -1;
  path = lc_cc_path(collection, ".commit.lock");
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection commit pin");
  *pin = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  lc_free_with_allocator(&collection->allocator, path);
  if (*pin < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection commit pin");
  rc = lc_cc_lock(*pin, exclusive ? LOCK_EX : LOCK_SH, error);
  if (rc != LC_OK) {
    (void)close(*pin);
    *pin = -1;
  }
  return rc;
}

static void lc_cc_commit_leave(int *pin) {
  if (*pin >= 0)
    (void)close(*pin);
  *pin = -1;
}

static int lc_cc_order_refresh(lc_collection_handle *, lc_error *);

int lc_collection_capture_enter(lc_collection_handle *collection, int *pin,
                                lc_error *error) {
  int rc;
  *pin = -1;
  /* Prepare can need append authority. Never acquire it behind the exclusive
   * commit pin: an appender may already own it and be waiting for a shared pin.
   */
  for (;;) {
    rc = lc_collection_prepare(collection, error);
    if (rc == LC_OK)
      rc = lc_cc_commit_enter(collection, 1, pin, error);
    if (rc == LC_OK)
      rc = lc_collection_metadata_enter(collection, 0, error);
    if (rc != LC_OK) {
      lc_cc_commit_leave(pin);
      break;
    }
    rc = lc_cc_order_refresh(collection, error);
    if (rc != LC_OK || !collection->order.edit_in_progress) {
      if (rc != LC_OK) {
        lc_collection_metadata_leave(collection);
        lc_cc_commit_leave(pin);
      }
      break;
    }
    /* An upload may have owned append authority during prepare. Its failed
     * publication is now behind our exclusive commit pin. Release the pin
     * before acquiring append authority to repair, then capture afresh. */
    lc_collection_metadata_leave(collection);
    lc_cc_commit_leave(pin);
  }
  return rc;
}

void lc_collection_capture_leave(lc_collection_handle *collection, int *pin) {
  lc_collection_metadata_leave(collection);
  lc_cc_commit_leave(pin);
}

static int lc_cc_open_file(lc_collection_handle *collection, const char *leaf,
                           int flags, int *out, lc_error *error) {
  char *path;
  path = lc_cc_path(collection, leaf);
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection path");
  *out = open(path, flags, 0600);
  lc_free_with_allocator(&collection->allocator, path);
  if (*out < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection file");
  return LC_OK;
}

static int lc_cc_same_collection(lc_collection_handle *a,
                                 lc_collection_handle *b) {
  return a->pouch->root_device == b->pouch->root_device &&
         a->pouch->root_inode == b->pouch->root_inode &&
         strcmp(a->id, b->id) == 0;
}

int lc_collection_read_pin(lc_collection_handle *collection, lc_u64 snapshot,
                           int *fd, lc_error *error) {
  char number[32];
  char leaf[64];
  int rc;
  if (lc_u64_format_base10_padded(snapshot, 20U, number, sizeof(number)) < 0)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, NULL,
                                 "failed to format collection read generation");
  snprintf(leaf, sizeof(leaf), ".readers-%s.lock", number);
  rc = lc_cc_open_file(collection, leaf, O_RDWR | O_CREAT | O_CLOEXEC, fd,
                       error);
  if (rc == LC_OK)
    rc = lc_cc_lock(*fd, LOCK_SH, error);
  return rc;
}

static int lc_cc_call_begin(lc_collection *self, int mutating, int held,
                            lc_collection_call *call, lc_error *error) {
  lc_collection_handle *collection;
  lc_collection_call *active;
  pthread_t thread;
  if (self == NULL || self->impl == NULL || call == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection receiver is required");
  collection = (lc_collection_handle *)self->impl;
  thread = pthread_self();
  memset(call, 0, sizeof(*call));
  lc_error_init(&call->local_error);
  call->error = error != NULL ? error : &call->local_error;
  lc_error_cleanup(call->error);
  lc_error_init(call->error);
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  if (collection->closed && !held) {
    pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection receiver is closed");
  }
  for (active = lc_collection_calls; active != NULL; active = active->next) {
    if (pthread_equal(active->thread, thread) &&
        ((active->collection == collection && active->callback) ||
         (mutating && active->source_callback &&
          lc_cc_same_collection(active->collection, collection)))) {
      pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
      return lc_collection_failure(
          error, LC_ERR_INVALID, NULL,
          "collection operation would re-enter its streaming callback");
    }
  }
  ++collection->references;
  call->collection = collection;
  call->pouch = collection->pouch;
  call->thread = thread;
  call->next = lc_collection_calls;
  lc_collection_calls = call;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  return LC_OK;
}

int lc_collection_call_begin(lc_collection *self, int mutating,
                             lc_collection_call *call, lc_error *error) {
  return lc_cc_call_begin(self, mutating, 0, call, error);
}

int lc_collection_call_begin_held(lc_collection_handle *collection,
                                  int mutating, lc_collection_call *call,
                                  lc_error *error) {
  return lc_cc_call_begin(&collection->pub, mutating, 1, call, error);
}

void lc_collection_handle_hold(lc_collection_handle *collection) {
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  ++collection->references;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
}

void lc_collection_handle_release(lc_collection_handle *collection) {
  int release;
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  release = --collection->references == 0UL;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  if (release)
    lc_collection_destroy(collection);
}

void lc_collection_call_callback(lc_collection_call *call, int enabled,
                                 int source) {
  /* Only this call's thread writes or reads these flags. Taking the registry
   * mutex here needlessly serializes otherwise independent stream callbacks. */
  call->callback = enabled;
  call->source_callback = enabled && source;
}

void lc_collection_call_end(lc_collection_call *call) {
  lc_collection_call **link;
  lc_collection_handle *collection;
  int release;
  collection = call->collection;
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  for (link = &lc_collection_calls; *link != NULL && *link != call;
       link = &(*link)->next) {
  }
  if (*link == call)
    *link = call->next;
  release = collection != NULL && --collection->references == 0UL;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  lc_error_cleanup(&call->local_error);
  if (release)
    lc_collection_destroy(collection);
}

static int lc_cc_root_enter(lc_pouch *pouch, lc_collection_call *call,
                            lc_collection_guard *guard, lc_error *error) {
  lc_collection_call *active;
  int borrowed;
  int rc;
  memset(guard, 0, sizeof(*guard));
  guard->commit_fd = -1;
  guard->pouch = pouch;
  guard->call = call;
  borrowed = 0;
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  for (active = lc_collection_calls; active != NULL; active = active->next) {
    if (active->mode_guard && pthread_equal(active->thread, pthread_self()) &&
        active->pouch == pouch)
      borrowed = 1;
  }
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  rc = borrowed ? LC_OK : lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK)
    return rc;
  guard->mode = !borrowed;
  if (guard->mode && call != NULL) {
    pthread_mutex_lock(&lc_collection_lifecycle_mutex);
    call->mode_guard = 1;
    pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  }
  /* Collection storage is isolated and never enlists in ordinary-state/XA
   * transactions. Its catalog/session/append/commit/metadata locks own its
   * mutations. Taking the ordinary-state root transaction lock here would
   * serialize independent collections and block lease renewal during uploads.
   * The root mode guard still excludes configuration/conversion cutovers. */
  return LC_OK;
}

static void lc_cc_root_leave(lc_collection_guard *guard) {
  lc_cc_commit_leave(&guard->commit_fd);
  if (guard->mode) {
    if (guard->call != NULL) {
      pthread_mutex_lock(&lc_collection_lifecycle_mutex);
      guard->call->mode_guard = 0;
      pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
    }
    lc_pouch_writer_mode_operation_end(guard->pouch);
  }
}

int lc_collection_root_begin(lc_pouch *pouch, lc_collection_call *scope,
                             lc_collection_guard *guard, lc_error *error) {
  int rc;
  memset(scope, 0, sizeof(*scope));
  lc_error_init(&scope->local_error);
  scope->error = error != NULL ? error : &scope->local_error;
  scope->pouch = pouch;
  scope->thread = pthread_self();
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  scope->next = lc_collection_calls;
  lc_collection_calls = scope;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
#ifdef LOCKDC_TEST_BUILD
  if (lc_collection_root_test_hook != NULL)
    lc_collection_root_test_hook(lc_collection_root_test_context, pouch);
#endif
  rc = lc_cc_root_enter(pouch, scope, guard, scope->error);
  if (rc != LC_OK)
    lc_collection_call_end(scope);
  return rc;
}

void lc_collection_root_end(lc_collection_call *scope,
                            lc_collection_guard *guard) {
  lc_cc_root_leave(guard);
  lc_collection_call_end(scope);
}

int lc_collection_metadata_enter(lc_collection_handle *collection,
                                 int exclusive, lc_error *error) {
  int rc;
  rc = pthread_mutex_lock(&collection->metadata_mutex);
  if (rc != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to lock collection metadata mutex");
  rc =
      lc_cc_lock(collection->metadata_fd, exclusive ? LOCK_EX : LOCK_SH, error);
  if (rc != LC_OK)
    pthread_mutex_unlock(&collection->metadata_mutex);
  return rc;
}

void lc_collection_metadata_leave(lc_collection_handle *collection) {
  (void)flock(collection->metadata_fd, LOCK_UN);
  pthread_mutex_unlock(&collection->metadata_mutex);
}

static int lc_cc_head_prefix_valid(const unsigned char *bytes, size_t length,
                                   lc_error *error) {
  if (!lc_cc_valid(bytes, length) || lc_cc_get(bytes) != LC_CC_HEAD_MAGIC)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection head checksum is invalid");
  if (lc_cc_get(bytes + 72U) > (lc_u64)LC_I64_MAX)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection retention deadline is invalid");
  if (lc_cc_get(bytes + 8U) == 0U ||
      lc_cc_get(bytes + 8U) >= LC_COLLECTION_SNAPSHOT_BIT ||
      lc_cc_get(bytes + 16U) >= LC_COLLECTION_SNAPSHOT_BIT ||
      lc_cc_get(bytes + 32U) == 0U ||
      lc_cc_get(bytes + 32U) > lc_cc_get(bytes + 8U) ||
      lc_cc_get(bytes + 16U) > lc_cc_get(bytes + 48U))
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection head bounds are invalid");
  return LC_OK;
}

int lc_collection_head_write(lc_collection_handle *collection,
                             lc_error *error) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  char *path;
  int rc;
  memset(bytes, 0, sizeof(bytes));
  lc_cc_put(bytes, LC_CC_HEAD_MAGIC);
  lc_cc_put(bytes + 8U, collection->head.active);
  lc_cc_put(bytes + 16U, collection->head.snapshot);
  lc_cc_put(bytes + 24U, collection->head.snapshot_generation);
  lc_cc_put(bytes + 32U, collection->head.tail_segment);
  lc_cc_put(bytes + 40U, collection->head.tail_offset);
  lc_cc_put(bytes + 48U, collection->head.next_snapshot);
  lc_cc_put(bytes + 56U, collection->head.dropped ? 1U : 0U);
  lc_cc_put(bytes + 64U, collection->head.published_at);
  lc_cc_put(bytes + 72U, (lc_u64)collection->head.retention);
  lc_cc_put(bytes + 80U, collection->head.max_entries);
  lc_cc_put(bytes + 88U, collection->head.retention_generation);
  lc_cc_put(bytes + 96U, collection->head.lease.fencing_token);
  lc_cc_put(bytes + 104U, (lc_u64)collection->head.lease.expires_at_unix);
  lc_cc_put(bytes + 112U, collection->head.lease.private_read ? 1U : 0U);
  memcpy(bytes + 120U, collection->head.lease.lease_id, LC_XID_STRING_SIZE);
  memcpy(bytes + 144U, collection->head.lease.owner, 129U);
  lc_cc_seal(bytes, sizeof(bytes));
  path = lc_cc_path(collection, "head");
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection head path");
  rc = lc_pouch_path_write_bytes_file(path, bytes, sizeof(bytes), error);
  lc_free_with_allocator(&collection->allocator, path);
  return rc;
}

int lc_collection_head_read(lc_collection_handle *collection, lc_error *error) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  struct stat identity;
  size_t i;
  int rc;
  if (stat(collection->head_path, &identity) != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect collection head");
  if (collection->head_loaded &&
      collection->head_device == (uint64_t)identity.st_dev &&
      collection->head_inode == (uint64_t)identity.st_ino) {
    return collection->head.dropped
               ? lc_collection_failure(error, LC_ERR_INVALID,
                                       "collection_stale",
                                       "collection has been dropped")
               : LC_OK;
  }
  collection->head_loaded = 0;
  rc = lc_cc_read_file(collection->head_path, bytes, sizeof(bytes), 0, NULL,
                       &collection->head_fd, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_cc_head_prefix_valid(bytes, sizeof(bytes), error);
  if (rc != LC_OK)
    return rc;
  for (i = 273U; i < sizeof(bytes) - 4U; ++i)
    if (bytes[i] != 0U)
      return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                   "collection head has unknown fields");
  collection->head.active = lc_cc_get(bytes + 8U);
  collection->head.snapshot = lc_cc_get(bytes + 16U);
  collection->head.snapshot_generation = lc_cc_get(bytes + 24U);
  collection->head.tail_segment = lc_cc_get(bytes + 32U);
  collection->head.tail_offset = lc_cc_get(bytes + 40U);
  collection->head.next_snapshot = lc_cc_get(bytes + 48U);
  collection->head.dropped = lc_cc_get(bytes + 56U) != 0U;
  collection->head.published_at = lc_cc_get(bytes + 64U);
  collection->head.retention = (lc_pouch_unix_seconds)lc_cc_get(bytes + 72U);
  collection->head.max_entries = lc_cc_get(bytes + 80U);
  collection->head.retention_generation = lc_cc_get(bytes + 88U);
  if (lc_cc_get(bytes + 104U) > (lc_u64)LC_I64_MAX ||
      lc_cc_get(bytes + 112U) > 1U ||
      memchr(bytes + 120U, 0, LC_XID_STRING_SIZE) == NULL ||
      memchr(bytes + 144U, 0, 129U) == NULL)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection ownership metadata is invalid");
  collection->head.lease.fencing_token = lc_cc_get(bytes + 96U);
  collection->head.lease.expires_at_unix =
      (lc_pouch_unix_seconds)lc_cc_get(bytes + 104U);
  collection->head.lease.private_read = (int)lc_cc_get(bytes + 112U);
  memcpy(collection->head.lease.lease_id, bytes + 120U, LC_XID_STRING_SIZE);
  memcpy(collection->head.lease.owner, bytes + 144U, 129U);
  if ((collection->head.lease.expires_at_unix == 0) !=
          (collection->head.lease.lease_id[0] == '\0') ||
      (collection->head.lease.lease_id[0] != '\0' &&
       (!lc_xid_is_valid(collection->head.lease.lease_id) ||
        collection->head.lease.fencing_token == 0U ||
        collection->head.lease.owner[0] == '\0')))
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection ownership binding is invalid");
  collection->head_device = (uint64_t)identity.st_dev;
  collection->head_inode = (uint64_t)identity.st_ino;
  collection->head_loaded = 1;
  if (collection->head.dropped)
    return lc_collection_failure(error, LC_ERR_INVALID, "collection_stale",
                                 "collection has been dropped");
  return LC_OK;
}

int lc_collection_migrate_head(lc_pouch *pouch, const char *directory,
                               lc_error *error) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  lc_collection_disk_head head;
  struct stat st;
  char *path;
  size_t i;
  int rc;
  path = lc_pouch_path_join(&pouch->allocator, directory, "head");
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection upgrade path");
  if (lstat(path, &st) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to inspect collection upgrade head");
  else if (!S_ISREG(st.st_mode) ||
           (st.st_size != 128 && st.st_size != LC_COLLECTION_HEAD_BYTES))
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection upgrade head has invalid size or type");
  else if (st.st_size == LC_COLLECTION_HEAD_BYTES)
    rc = lc_collection_recode_head(pouch, directory, &head, error);
  else {
    memset(bytes, 0, sizeof(bytes));
    rc = lc_cc_read_file(path, bytes, 128U, 0, NULL, NULL, error);
    if (rc == LC_OK)
      rc = lc_cc_head_prefix_valid(bytes, 128U, error);
    for (i = 96U; rc == LC_OK && i < 124U; ++i)
      if (bytes[i] != 0U)
        rc =
            lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                  "collection upgrade head has unknown fields");
    if (rc == LC_OK) {
      /* The released head's complete semantic prefix is unchanged. Its CRC
       * and reserved bytes become zeroed ownership metadata, never a body. */
      memset(bytes + 96U, 0, sizeof(bytes) - 96U);
      lc_cc_seal(bytes, sizeof(bytes));
      rc = lc_pouch_path_write_bytes_file_mode(path, bytes, sizeof(bytes),
                                               (unsigned int)st.st_mode, error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

int lc_collection_record_leaf(lc_u64 segment, char leaf[64], lc_error *error) {
  char digits[32];
  int n;
  if (segment == 0U || (segment & (lc_u64)LC_I64_MAX) == 0U ||
      lc_u64_format_base10_padded(segment & (lc_u64)LC_I64_MAX, 20U, digits,
                                  sizeof(digits)) < 0)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection segment identity is invalid");
  n = snprintf(leaf, 64U, "%s%s.log",
               segment & LC_COLLECTION_SNAPSHOT_BIT ? "snapshot-" : "seg-",
               digits);
  return n >= 0 && n < 64
             ? LC_OK
             : lc_collection_failure(error, LC_ERR_PROTOCOL,
                                     "collection_corrupt",
                                     "collection segment leaf is invalid");
}

int lc_collection_record_path(lc_collection_handle *collection, lc_u64 segment,
                              char **out, char leaf[64], lc_error *error) {
  int rc;
  *out = NULL;
  rc = lc_collection_record_leaf(segment, leaf, error);
  if (rc != LC_OK)
    return rc;
  *out = lc_cc_path(collection, leaf);
  return *out != NULL ? LC_OK
                      : lc_collection_failure(
                            error, LC_ERR_NOMEM, NULL,
                            "failed to allocate collection segment path");
}

int lc_collection_record_ref(lc_collection_handle *collection, lc_u64 segment,
                             lc_u64 offset, int *fd, lc_u64 *fd_segment,
                             lc_u64 *fd_end, lc_pouch_collection_record *out,
                             lc_error *error) {
  char leaf[64];
  char *path;
  struct stat st;
  int rc;
  path = NULL;
  rc = lc_collection_record_leaf(segment, leaf, error);
  if (rc != LC_OK)
    return rc;
  if (*fd < 0 || *fd_segment != segment) {
    path = lc_cc_path(collection, leaf);
    if (path == NULL)
      return lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate referenced collection segment path");
    if (*fd >= 0)
      (void)close(*fd);
    *fd = open(path, O_RDONLY | O_CLOEXEC);
    *fd_segment = segment;
    if (*fd < 0 || fstat(*fd, &st) != 0 || st.st_size < 0) {
      lc_free_with_allocator(&collection->allocator, path);
      return lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to inspect referenced collection segment");
    }
    *fd_end = (lc_u64)st.st_size;
  }
  lc_free_with_allocator(&collection->allocator, path);
  rc = lc_pouch_collection_record_read(*fd, offset, *fd_end, leaf, out, error);
  /* A metadata projection may advance after the view was captured. Its
   * backward link is safe, but the shared append file may have grown since
   * this reader's previous stat. Never treat that stale bound as corruption. */
  if (rc == LC_ERR_PROTOCOL && fstat(*fd, &st) == 0 && st.st_size >= 0 &&
      (lc_u64)st.st_size > *fd_end) {
    *fd_end = (lc_u64)st.st_size;
    lc_error_cleanup(error);
    lc_error_init(error);
    rc =
        lc_pouch_collection_record_read(*fd, offset, *fd_end, leaf, out, error);
  }
  return rc;
}

int lc_collection_record_at(lc_collection_handle *collection, lc_u64 generation,
                            int *fd, lc_u64 *fd_segment, lc_u64 *fd_end,
                            lc_pouch_collection_record *record,
                            lc_error *error) {
  lc_pouch_collection_record newer;
  int rc;
  while (record->generation > generation) {
    if (!record->metadata_only || record->previous_segment == 0U ||
        (record->previous_segment == *fd_segment &&
         record->previous_offset >= record->record_offset))
      return lc_collection_failure(
          error, LC_ERR_PROTOCOL, "collection_corrupt",
          "collection metadata history does not precede its captured view");
    newer = *record;
    rc = lc_collection_record_ref(collection, newer.previous_segment,
                                  newer.previous_offset, fd, fd_segment, fd_end,
                                  record, error);
    if (rc != LC_OK)
      return rc;
    if (record->deleted || record->generation >= newer.generation ||
        record->position != newer.position ||
        record->published_at != newer.published_at ||
        record->bytes != newer.bytes ||
        strcmp(record->append_id, newer.append_id) != 0 ||
        strcmp(record->etag, newer.etag) != 0)
      return lc_collection_failure(
          error, LC_ERR_PROTOCOL, "collection_corrupt",
          "collection metadata history changed its immutable state");
  }
  return LC_OK;
}

int lc_collection_record_body(lc_collection_handle *collection,
                              const lc_pouch_collection_record *record, int *fd,
                              lc_u64 *fd_segment, lc_u64 *fd_end,
                              lc_error *error) {
  lc_pouch_collection_record body;
  lc_u64 segment;
  size_t prefix;
  int snapshot, rc;
  snapshot = strncmp(record->payload_container, "snapshot-", 9U) == 0;
  prefix = snapshot ? 9U : 4U;
  if ((!snapshot && strncmp(record->payload_container, "seg-", 4U) != 0) ||
      strlen(record->payload_container) != prefix + 24U ||
      strcmp(record->payload_container + prefix + 20U, ".log") != 0 ||
      !lc_parse_u64_base10_range_checked(record->payload_container + prefix,
                                         20U, &segment) ||
      segment == 0U || segment >= LC_COLLECTION_SNAPSHOT_BIT)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection payload container is invalid");
  if (snapshot)
    segment |= LC_COLLECTION_SNAPSHOT_BIT;
  rc = lc_collection_record_ref(collection, segment,
                                record->payload_record_offset, fd, fd_segment,
                                fd_end, &body, error);
  if (rc == LC_OK &&
      (body.deleted || body.metadata_only ||
       body.position != record->position ||
       body.published_at != record->published_at ||
       body.generation > record->generation || body.bytes != record->bytes ||
       body.payload_offset != record->payload_offset ||
       body.stored_bytes != record->stored_bytes ||
       body.stored_crc != record->stored_crc ||
       strcmp(body.append_id, record->append_id) != 0 ||
       strcmp(body.etag, record->etag) != 0 ||
       strcmp(body.context, record->context) != 0 ||
       strcmp(body.descriptor, record->descriptor) != 0))
    rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                               "collection metadata refers to another payload");
  return rc;
}

static int lc_cc_active(lc_collection_handle *collection, lc_error *error) {
  char leaf[64];
  char *path;
  int rc;
  if (collection->active_fd >= 0 &&
      collection->active_fd_segment == collection->head.active)
    return LC_OK;
  rc = lc_collection_record_path(collection, collection->head.active, &path,
                                 leaf, error);
  if (rc != LC_OK)
    return rc;
  if (collection->active_fd >= 0)
    (void)close(collection->active_fd);
  collection->active_fd = open(path, O_RDWR);
  collection->active_fd_segment = collection->head.active;
  lc_free_with_allocator(&collection->allocator, path);
  return collection->active_fd >= 0
             ? LC_OK
             : lc_collection_failure(
                   error, LC_ERR_TRANSPORT, NULL,
                   "failed to open active collection segment");
}

static int lc_cc_order_refresh(lc_collection_handle *collection,
                               lc_error *error) {
  struct stat named;
  struct stat opened;
  if (collection->order.fd < 0)
    return lc_collection_order_open(collection->order_path, 0,
                                    &collection->order, error);
  if (stat(collection->order_path, &named) != 0 ||
      fstat(collection->order.fd, &opened) != 0)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to inspect collection ordered projection");
  if (named.st_dev != opened.st_dev || named.st_ino != opened.st_ino) {
    lc_collection_order_close(&collection->order);
    return lc_collection_order_open(collection->order_path, 0,
                                    &collection->order, error);
  }
  return lc_collection_order_refresh(&collection->order, error);
}

int lc_collection_project_record(lc_collection_order *order,
                                 lc_collection_ids *ids,
                                 const lc_pouch_collection_record *record,
                                 lc_u64 segment, int snapshot,
                                 lc_error *error) {
  lc_collection_order_row row;
  lc_collection_order_row current;
  lc_collection_id_ref id;
  int found;
  int rc;
  memset(&row, 0, sizeof(row));
  row.position = record->position;
  row.published_at = record->published_at;
  row.deleted_at = record->deleted ? record->generation : 0U;
  row.segment = segment;
  row.record_offset = record->record_offset;
  row.bytes = record->bytes;
  row.stored_bytes = record->stored_bytes;
  if (record->retention > 0)
    order->retention_present = 1;
  if (record->has_collection_retention &&
      record->published_at > order->collection_retention_generation) {
    order->collection_retention_generation = record->published_at;
    order->collection_retention = record->collection_retention;
  }
  rc = lc_collection_order_find(order, record->position, &current, &found,
                                error);
  if (rc != LC_OK)
    return rc;
  if (!found) {
    if (record->deleted && !snapshot)
      return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                   "collection delete has no committed append");
    rc = snapshot ? lc_collection_order_append_recovered(
                        order, &row, 1U, record->next_offset, error)
                  : lc_collection_order_append(order, &row, segment,
                                               record->next_offset, error);
  } else if (current.published_at != row.published_at ||
             current.bytes != row.bytes) {
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection identity changed during canonical replay");
  } else if (record->deleted && current.deleted_at < record->generation) {
    rc = lc_collection_order_delete(order, record->position, record->generation,
                                    segment, record->next_offset, error);
  }
  if (rc != LC_OK)
    return rc;
  if (!record->deleted && record->generation > record->published_at) {
    rc = lc_collection_order_metadata(order, &row, record->generation,
                                      snapshot ? 1U : segment,
                                      record->next_offset, error);
    if (rc != LC_OK)
      return rc;
  }
  memset(&id, 0, sizeof(id));
  rc = lc_collection_hash(record->append_id, strlen(record->append_id),
                          id.digest, error);
  if (rc != LC_OK)
    return rc;
  id.position = record->position;
  id.generation = record->generation;
  id.segment = segment;
  id.offset = record->record_offset;
  return lc_collection_ids_put(ids, &id, error);
}

int lc_collection_refresh_view(lc_collection_handle *collection,
                               lc_error *error) {
  int rc;
  rc = lc_collection_head_read(collection, error);
  if (rc == LC_OK)
    rc = lc_cc_order_refresh(collection, error);
  if (rc == LC_OK && collection->order.snapshot != collection->head.snapshot)
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection projection belongs to another snapshot");
  return rc;
}

int lc_collection_replay_into(lc_collection_handle *collection,
                              lc_collection_order *order,
                              lc_collection_ids *ids, lc_u64 segment,
                              lc_u64 offset, int snapshot, lc_u64 cutoff,
                              lc_error *error) {
  lc_pouch_collection_record record;
  lc_pouch_collection_record prior;
  char leaf[64];
  char *path;
  struct stat st;
  lc_u64 limit;
  int fd;
  int rc;
  int changed;
  rc = lc_collection_record_path(collection, segment, &path, leaf, error);
  if (rc != LC_OK)
    return rc;
  fd = open(path, O_RDWR);
  lc_free_with_allocator(&collection->allocator, path);
  if (fd < 0 || fstat(fd, &st) != 0 || st.st_size < 0) {
    if (fd >= 0)
      (void)close(fd);
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to open collection canonical replay segment");
  }
  limit = (lc_u64)st.st_size;
  changed = offset != limit || order->replay_segment != segment ||
            order->replay_offset != limit || ids->replay_segment != segment ||
            ids->replay_offset != limit;
  if (offset > limit) {
    (void)close(fd);
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection canonical tail is shorter than its projection");
  }
  memset(&prior, 0, sizeof(prior));
  while (offset < limit) {
    /* Torn pending headers have no published outcome. Complete malformed
     * headers and truncated published bodies remain explicit corruption. */
    rc = limit - offset < LC_POUCH_RECORD_HEADER_BYTES
             ? LC_ERR_PENDING
             : lc_pouch_collection_record_read(fd, offset, limit, leaf, &record,
                                               error);
    if (rc == LC_ERR_PENDING && !snapshot &&
        segment == collection->head.active) {
      lc_error_cleanup(error);
      lc_error_init(error);
      if (ftruncate(fd, (off_t)offset) != 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to repair collection pending tail");
      else
        rc = LC_OK;
      limit = offset;
      break;
    }
    if (rc != LC_OK)
      break;
    if (record.kind == LC_POUCH_COLLECTION_EDIT) {
      if (snapshot || record.generation > cutoff)
        rc = lc_collection_edit_project(collection, order, ids, fd, &record,
                                        &prior, segment, snapshot, error);
    } else if (record.kind != LC_POUCH_COLLECTION_EDIT_BODY &&
               (snapshot || record.generation > cutoff))
      rc = lc_collection_project_record(order, ids, &record, segment, snapshot,
                                        error);
    prior = record;
    if (rc != LC_OK)
      break;
    offset = record.next_offset;
  }
  if (rc == LC_OK && !snapshot && segment == collection->head.active &&
      prior.kind == LC_POUCH_COLLECTION_EDIT_BODY &&
      prior.next_offset == limit) {
    /* A finalized provisional body is still unpublished without its following
     * edit record. A crash can leave exactly this append-authority-owned tail.
     * Never discard a malformed record or a body followed by a committed edit.
     */
    if (ftruncate(fd, (off_t)prior.record_offset) != 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to reclaim unpublished edit tail");
    else {
      limit = prior.record_offset;
      changed = 1;
    }
  }
  (void)close(fd);
  if (rc == LC_OK && !snapshot && changed) {
    rc = lc_collection_order_checkpoint(order, segment, limit, error);
    if (rc == LC_OK)
      rc = lc_collection_ids_checkpoint(ids, lc_collection_binding_count(order),
                                        order->generation, segment, limit,
                                        error);
  }
  return rc;
}

static int lc_cc_rebuild(lc_collection_handle *collection, lc_error *error) {
  lc_collection_order previous_order;
  lc_collection_ids previous_ids;
  char id[LC_XID_STRING_SIZE];
  char *order_temp;
  char *ids_temp;
  size_t size;
  lc_u64 segment;
  int rc;
  int order_created;
  int ids_created;
  rc = lc_xid_new(id, error);
  if (rc != LC_OK)
    return rc;
  size = strlen(collection->directory) + sizeof(id) + 32U;
  order_temp = (char *)lc_alloc_with_allocator(&collection->allocator, size);
  ids_temp = (char *)lc_alloc_with_allocator(&collection->allocator, size);
  if (order_temp == NULL || ids_temp == NULL) {
    lc_free_with_allocator(&collection->allocator, order_temp);
    lc_free_with_allocator(&collection->allocator, ids_temp);
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection recovery paths");
  }
  snprintf(order_temp, size, "%s/order.rebuild-%s", collection->directory, id);
  snprintf(ids_temp, size, "%s/ids.rebuild-%s", collection->directory, id);
  previous_order = collection->order;
  previous_ids = collection->ids;
  memset(&collection->order, 0, sizeof(collection->order));
  collection->order.fd = -1;
  memset(&collection->ids, 0, sizeof(collection->ids));
  collection->ids.fd = -1;
  order_created = 0;
  ids_created = 0;
  rc = lc_collection_order_open(order_temp, 1, &collection->order, error);
  order_created = rc == LC_OK;
  if (rc == LC_OK) {
    rc = lc_collection_ids_open(ids_temp, 1, &collection->allocator,
                                &collection->ids, error);
    ids_created = rc == LC_OK;
  }
  collection->order.snapshot = collection->ids.snapshot =
      collection->head.snapshot;
  if (rc == LC_OK && collection->head.snapshot != 0U)
    rc = lc_collection_replay_into(
        collection, &collection->order, &collection->ids,
        collection->head.snapshot | LC_COLLECTION_SNAPSHOT_BIT, 0U, 1,
        collection->head.snapshot_generation, error);
  if (rc == LC_OK)
    rc = lc_collection_order_checkpoint(&collection->order,
                                        collection->head.tail_segment,
                                        collection->head.tail_offset, error);
  for (segment = collection->head.tail_segment;
       rc == LC_OK && segment <= collection->head.active; ++segment)
    rc = lc_collection_replay_into(
        collection, &collection->order, &collection->ids, segment,
        segment == collection->head.tail_segment ? collection->head.tail_offset
                                                 : 0U,
        0, collection->head.snapshot_generation, error);
  if (rc == LC_OK &&
      (fsync(collection->order.fd) != 0 || fsync(collection->ids.fd) != 0))
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to sync rebuilt collection projections");
  if (rc == LC_OK && rename(order_temp, collection->order_path) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to publish rebuilt collection order");
  else if (rc == LC_OK)
    order_created = 0;
  if (rc == LC_OK && rename(ids_temp, collection->ids_path) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to publish rebuilt append identities");
  else if (rc == LC_OK)
    ids_created = 0;
  lc_collection_order_close(&collection->order);
  lc_collection_ids_close(&collection->ids);
  lc_collection_order_close(&previous_order);
  lc_collection_ids_close(&previous_ids);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        collection->directory, "failed to sync collection recovery publication",
        error);
  if (rc == LC_OK)
    rc = lc_collection_order_open(collection->order_path, 0, &collection->order,
                                  error);
  if (rc == LC_OK)
    rc = lc_collection_ids_open(collection->ids_path, 0, &collection->allocator,
                                &collection->ids, error);
  if (order_created)
    (void)unlink(order_temp);
  if (ids_created)
    (void)unlink(ids_temp);
  lc_free_with_allocator(&collection->allocator, order_temp);
  lc_free_with_allocator(&collection->allocator, ids_temp);
  collection->trusted = rc == LC_OK;
  return rc;
}

static int lc_collection_reconcile(lc_collection_handle *collection,
                                   lc_error *error) {
  lc_u64 segment;
  lc_u64 offset;
  int rc;
  rc = lc_collection_head_read(collection, error);
  if (rc == LC_OK)
    rc = lc_cc_active(collection, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_cc_order_refresh(collection, error);
  if (rc == LC_OK) {
    if (collection->ids.fd < 0)
      rc = lc_collection_ids_open(collection->ids_path, 0,
                                  &collection->allocator, &collection->ids,
                                  error);
    else
      rc = lc_collection_ids_refresh(&collection->ids, error);
  }
  if (rc != LC_OK || !collection->trusted ||
      collection->order.edit_in_progress ||
      collection->order.snapshot != collection->head.snapshot ||
      collection->ids.snapshot != collection->head.snapshot) {
    if (rc != LC_OK && rc != LC_ERR_PROTOCOL &&
        !(rc == LC_ERR_TRANSPORT && errno == ENOENT))
      return rc;
    lc_error_cleanup(error);
    lc_error_init(error);
    return lc_cc_rebuild(collection, error);
  }
  segment = collection->order.replay_segment;
  offset = collection->order.replay_offset;
  if (collection->ids.replay_segment < segment ||
      (collection->ids.replay_segment == segment &&
       collection->ids.replay_offset < offset)) {
    segment = collection->ids.replay_segment;
    offset = collection->ids.replay_offset;
  }
  if (segment == 0U)
    segment = collection->head.tail_segment;
  for (; rc == LC_OK && segment <= collection->head.active; ++segment) {
    rc = lc_collection_replay_into(collection, &collection->order,
                                   &collection->ids, segment, offset, 0,
                                   collection->head.snapshot_generation, error);
    offset = 0U;
  }
  return rc;
}

int lc_collection_prepare(lc_collection_handle *collection, lc_error *error) {
  lc_collection_guard guard;
  int lock_errno;
  int rc;
  /* Never wait on a source callback's own append. The metadata lock below
   * still excludes an in-progress projection publication. */
  rc = pthread_mutex_trylock(&collection->append_mutex);
  if (rc == EBUSY)
    return LC_OK;
  if (rc != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to probe collection append mutex");
  rc = flock(collection->append_fd, LOCK_EX | LOCK_NB);
  lock_errno = errno;
  if (rc == 0)
    (void)flock(collection->append_fd, LOCK_UN);
  pthread_mutex_unlock(&collection->append_mutex);
  errno = lock_errno;
  if (rc != 0 && (lock_errno == EWOULDBLOCK || lock_errno == EAGAIN))
    return LC_OK;
  if (rc != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to probe collection appender");
  rc = lc_cc_root_enter(collection->pouch, NULL, &guard, error);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&collection->append_mutex);
  rc = lc_cc_lock(collection->append_fd, LOCK_EX, error);
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(collection, 1, error);
    if (rc == LC_OK) {
      rc = lc_collection_reconcile(collection, error);
      lc_collection_metadata_leave(collection);
    }
    (void)flock(collection->append_fd, LOCK_UN);
  }
  pthread_mutex_unlock(&collection->append_mutex);
  lc_cc_root_leave(&guard);
  return rc;
}

static int lc_cc_checkpoint(lc_collection_handle *collection, int write_marker,
                            int *valid, lc_error *error) {
  unsigned char bytes[64];
  unsigned char expected[64];
  struct stat st;
  char *path;
  int found;
  int rc;
  *valid = 0;
  rc = lc_cc_active(collection, error);
  if (rc != LC_OK)
    return rc;
  if (fstat(collection->active_fd, &st) != 0 || st.st_size < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect collection clean boundary");
  memset(expected, 0, sizeof(expected));
  lc_cc_put(expected, LC_CC_CLEAN_MAGIC);
  lc_cc_put(expected + 8U, collection->order.count);
  lc_cc_put(expected + 16U, collection->order.generation);
  lc_cc_put(expected + 24U, collection->order.upper_position);
  lc_cc_put(expected + 32U, collection->head.active);
  lc_cc_put(expected + 40U, (lc_u64)st.st_size);
  lc_cc_put(expected + 48U, collection->head.snapshot);
  lc_cc_seal(expected, sizeof(expected));
  path = lc_cc_path(collection, "clean");
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection checkpoint path");
  if (write_marker) {
    if (fsync(collection->active_fd) != 0 || fsync(collection->order.fd) != 0 ||
        fsync(collection->ids.fd) != 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync collection clean checkpoint");
    else
      rc = lc_pouch_path_write_bytes_file(path, expected, sizeof(expected),
                                          error);
    *valid = rc == LC_OK;
  } else {
    rc = lc_cc_read_file(path, bytes, sizeof(bytes), 1, &found, NULL, error);
    if (rc == LC_ERR_PROTOCOL) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      found = 0;
    }
    if (rc == LC_OK && found && memcmp(bytes, expected, sizeof(bytes)) == 0 &&
        collection->order.snapshot == collection->head.snapshot &&
        collection->ids.snapshot == collection->head.snapshot &&
        collection->order.replay_segment == collection->head.active &&
        collection->order.replay_offset == (lc_u64)st.st_size &&
        collection->ids.count ==
            lc_collection_binding_count(&collection->order) &&
        collection->ids.generation == collection->order.generation &&
        collection->ids.replay_segment == collection->head.active &&
        collection->ids.replay_offset == (lc_u64)st.st_size)
      *valid = 1;
  }
  lc_free_with_allocator(&collection->allocator, path);
  return rc;
}

static int lc_cc_invalidate_clean(lc_collection_handle *collection,
                                  lc_error *error) {
  char *path;
  int removed;
  path = lc_cc_path(collection, "clean");
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection checkpoint path");
  removed = unlink(path) == 0;
  lc_free_with_allocator(&collection->allocator, path);
  if (!removed && errno != ENOENT)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to invalidate collection clean checkpoint");
  return removed
             ? lc_pouch_path_fsync_directory(
                   collection->directory,
                   "failed to sync collection checkpoint invalidation", error)
             : LC_OK;
}

static void lc_collection_destroy(lc_collection_handle *collection) {
  lc_client_handle *client;
  lc_allocator allocator;
  lc_collection_guard guard;
  lc_collection_call scope;
  lc_error error;
  int valid;
  int rc;
  int entered;
  client = collection->client;
  allocator = collection->allocator;
  lc_error_init(&error);
  rc = lc_collection_root_begin(collection->pouch, &scope, &guard, &error);
  entered = rc == LC_OK;
  if (rc == LC_OK && collection->join_fd >= 0)
    rc = lc_cc_lock(collection->join_fd, LOCK_EX, &error);
  /* Upgrade is nonblocking: other live handles retain the shared session.
   * Only the last handle can publish a clean-close proof. */
  if (rc == LC_OK && collection->session_fd >= 0 && collection->trusted &&
      flock(collection->session_fd, LOCK_EX | LOCK_NB) == 0) {
    rc = lc_cc_lock(collection->append_fd, LOCK_EX, &error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, &error);
      if (rc == LC_OK) {
        rc = lc_collection_reconcile(collection, &error);
        if (rc == LC_OK)
          rc = lc_cc_checkpoint(collection, 1, &valid, &error);
        lc_collection_metadata_leave(collection);
      }
      (void)flock(collection->append_fd, LOCK_UN);
    }
  }
  if (entered)
    lc_collection_root_end(&scope, &guard);
  if (rc != LC_OK) {
    pslog_field field;
    field = lc_log_error_field("error", &error);
    lc_log_warn(
        collection->pouch->logger,
        "collection clean checkpoint failed; next open requires recovery",
        &field, 1U);
  }
  lc_error_cleanup(&error);
  if (collection->active_fd >= 0)
    (void)close(collection->active_fd);
  if (collection->head_fd >= 0)
    (void)close(collection->head_fd);
  lc_collection_order_close(&collection->order);
  lc_collection_ids_close(&collection->ids);
  if (collection->session_fd >= 0)
    (void)close(collection->session_fd);
  if (collection->join_fd >= 0)
    (void)close(collection->join_fd);
  if (collection->metadata_fd >= 0)
    (void)close(collection->metadata_fd);
  if (collection->append_fd >= 0)
    (void)close(collection->append_fd);
  if (collection->metadata_mutex_initialized)
    (void)pthread_mutex_destroy(&collection->metadata_mutex);
  if (collection->append_mutex_initialized)
    (void)pthread_mutex_destroy(&collection->append_mutex);
  lc_free_with_allocator(&allocator, collection->ns);
  lc_free_with_allocator(&allocator, collection->name);
  lc_free_with_allocator(&allocator, collection->directory);
  lc_free_with_allocator(&allocator, collection->head_path);
  lc_free_with_allocator(&allocator, collection->catalog_path);
  lc_free_with_allocator(&allocator, collection->catalog_base);
  lc_free_with_allocator(&allocator, collection->order_path);
  lc_free_with_allocator(&allocator, collection->ids_path);
  lc_free_with_allocator(&allocator, collection);
  if (client != NULL)
    lc_client_handle_release(client);
}

static void lc_cc_close_method(lc_collection *self) {
  lc_collection_handle *collection;
  int release;
  if (self == NULL || self->impl == NULL)
    return;
  collection = (lc_collection_handle *)self->impl;
  pthread_mutex_lock(&lc_collection_lifecycle_mutex);
  if (collection->closed) {
    pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
    return;
  }
  collection->closed = 1;
  release = --collection->references == 0UL;
  pthread_mutex_unlock(&lc_collection_lifecycle_mutex);
  if (release)
    lc_collection_destroy(collection);
}

static int lc_cc_write_enter(lc_collection_call *call,
                             lc_collection_guard *guard, int appending,
                             lc_error *error) {
  lc_collection_handle *collection;
  int rc;
  collection = call->collection;
  rc = lc_cc_root_enter(collection->pouch, call, guard, error);
  if (rc != LC_OK)
    return rc;
  guard->collection = collection;
  rc = pthread_mutex_lock(&collection->append_mutex);
  if (rc == 0) {
    rc = lc_cc_lock(collection->append_fd, LOCK_EX, error);
    if (rc == LC_OK) {
      guard->append = 1;
      if (!appending)
        rc = lc_cc_commit_enter(collection, 1, &guard->commit_fd, error);
      if (rc == LC_OK)
        rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        rc = lc_collection_reconcile(collection, error);
        if (rc != LC_OK)
          lc_collection_metadata_leave(collection);
      }
    }
    if (rc != LC_OK) {
      (void)flock(collection->append_fd, LOCK_UN);
      pthread_mutex_unlock(&collection->append_mutex);
      guard->append = 0;
    }
  } else
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to enter collection appender");
  if (rc != LC_OK)
    lc_cc_root_leave(guard);
  return rc;
}

static int lc_cc_checked_write_enter(lc_collection_call *call,
                                     lc_collection_guard *guard, int appending,
                                     lc_error *error) {
  int rc = lc_cc_write_enter(call, guard, appending, error);
  if (rc == LC_OK && !call->maintenance) {
    rc = lc_collection_lease_check(call->collection, 1, error);
    if (rc != LC_OK) {
      lc_collection_metadata_leave(call->collection);
      lc_collection_write_leave(guard);
    }
  }
  return rc;
}

int lc_collection_write_enter(lc_collection_call *call,
                              lc_collection_guard *guard, lc_error *error) {
  return lc_cc_checked_write_enter(call, guard, 0, error);
}

int lc_collection_append_enter(lc_collection_call *call,
                               lc_collection_guard *guard, lc_error *error) {
  return lc_cc_checked_write_enter(call, guard, 1, error);
}

int lc_collection_edit_barrier(lc_collection_guard *guard, lc_error *error) {
  return lc_cc_commit_enter(guard->collection, 1, &guard->commit_fd, error);
}

int lc_collection_control_enter(lc_collection_call *call,
                                lc_collection_guard *guard, lc_error *error) {
  /* Collection control is not an ordinary-state transaction participant. It
   * must progress while another thread/process owns a streamed append's
   * root-wide transaction guard. Mode and commit/metadata authority suffice. */
  int rc = lc_cc_root_enter(call->collection->pouch, call, guard, error);
  if (rc != LC_OK)
    return rc;
  guard->collection = call->collection;
  rc = lc_cc_commit_enter(call->collection, 1, &guard->commit_fd, error);
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(call->collection, 1, error);
    if (rc == LC_OK) {
      rc = lc_collection_head_read(call->collection, error);
      if (rc != LC_OK)
        lc_collection_metadata_leave(call->collection);
    }
  }
  if (rc != LC_OK)
    lc_collection_write_leave(guard);
  return rc;
}

void lc_collection_control_leave(lc_collection_guard *guard) {
  lc_collection_metadata_leave(guard->collection);
  lc_collection_write_leave(guard);
}

static void lc_cc_write_unlock(lc_collection_guard *guard) {
  if (guard->append) {
    (void)flock(guard->collection->append_fd, LOCK_UN);
    pthread_mutex_unlock(&guard->collection->append_mutex);
    guard->append = 0;
  }
}

void lc_collection_write_leave(lc_collection_guard *guard) {
  lc_cc_write_unlock(guard);
  lc_cc_root_leave(guard);
}

int lc_collection_rotate(lc_collection_handle *collection, int force,
                         lc_error *error) {
  char leaf[64];
  char *path;
  struct stat st;
  lc_u64 next;
  int fd;
  int rc;
  if (fstat(collection->active_fd, &st) != 0 || st.st_size < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect collection segment size");
  if (!force && (lc_u64)st.st_size < collection->pouch->segment_target_bytes)
    return LC_OK;
  next = collection->head.active + 1U;
  if (next >= LC_COLLECTION_SNAPSHOT_BIT)
    return lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                 "collection segment identities are exhausted");
  /* Seal canonical history before naming another segment. Projections remain
   * rebuildable from that history; only a clean checkpoint promises their
   * persistence and syncs them before publishing its marker. */
  if (fsync(collection->active_fd) != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync collection segment boundary");
  rc = lc_collection_record_path(collection, next, &path, leaf, error);
  if (rc != LC_OK)
    return rc;
  fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0 && errno == EEXIST) {
    fd = open(path, O_RDWR);
    /* A push commit may have named its complete inode immediately before a
     * crash prevented head publication. This next segment is outside every
     * committed topology; current append authority can safely reclaim it. */
    if (fd >= 0 && ftruncate(fd, 0) != 0) {
      (void)close(fd);
      fd = -1;
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to reclaim unpublished collection segment");
    }
  }
  if (fd < 0 && rc == LC_OK)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to create collection segment");
  if (rc == LC_OK && fsync(fd) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to sync new collection segment");
  if (rc == LC_OK) {
    collection->head.active = next;
    rc = lc_collection_head_write(collection, error);
  }
  if (rc == LC_OK) {
    (void)close(collection->active_fd);
    collection->active_fd = fd;
    collection->active_fd_segment = next;
    fd = -1;
  }
  if (fd >= 0)
    (void)close(fd);
  lc_free_with_allocator(&collection->allocator, path);
  if (rc != LC_OK)
    collection->head_loaded = 0;
  return rc;
}

typedef struct lc_cc_lookup {
  lc_collection_handle *collection;
  const char *append_id;
  lc_pouch_collection_record record;
  int fd;
  lc_u64 segment;
  lc_u64 end;
} lc_cc_lookup;

static int lc_cc_match(void *context, const lc_collection_id_ref *ref,
                       int *matched, lc_error *error) {
  lc_cc_lookup *lookup;
  int rc;
  lookup = (lc_cc_lookup *)context;
  rc = lc_collection_record_ref(lookup->collection, ref->segment, ref->offset,
                                &lookup->fd, &lookup->segment, &lookup->end,
                                &lookup->record, error);
  if (rc != LC_OK)
    return rc;
  if (lookup->record.kind == LC_POUCH_COLLECTION_EDIT) {
    *matched = 0;
    return LC_OK;
  }
  if (lookup->record.position != ref->position ||
      lookup->record.generation != ref->generation)
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection identity reference disagrees with canonical state");
  *matched = strcmp(lookup->record.append_id, lookup->append_id) == 0;
  return LC_OK;
}

int lc_collection_lookup_id(lc_collection_handle *collection,
                            const char *append_id,
                            lc_pouch_collection_record *out, lc_u64 *segment,
                            int *found, lc_error *error) {
  lc_cc_lookup lookup;
  lc_collection_id_ref ref;
  unsigned char digest[32];
  int rc;
  memset(&lookup, 0, sizeof(lookup));
  lookup.collection = collection;
  lookup.append_id = append_id;
  lookup.fd = -1;
  *found = 0;
  rc = lc_collection_hash(append_id, strlen(append_id), digest, error);
  if (rc == LC_OK)
    rc = lc_collection_ids_find(&collection->ids, digest, lc_cc_match, &lookup,
                                &ref, found, error);
  if (rc == LC_OK && *found) {
    lc_collection_order_row row;
    int row_found;
    *out = lookup.record;
    rc = lc_collection_order_find(&collection->order, out->position, &row,
                                  &row_found, error);
    if (rc == LC_OK && row_found && row.deleted_at != 0U)
      out->deleted = 1;
  }
  if (rc == LC_OK && *found && segment != NULL)
    *segment = lookup.segment;
  if (lookup.fd >= 0)
    (void)close(lookup.fd);
  return rc;
}

static int lc_cc_id_valid(const char *id) {
  size_t i;
  size_t length;
  if (id == NULL)
    return 0;
  length = strlen(id);
  if (length == 0U || length > 128U)
    return 0;
  for (i = 0U; i < length; ++i) {
    unsigned char c;
    c = (unsigned char)id[i];
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
          c == '~'))
      return 0;
  }
  return 1;
}

static void lc_cc_append_result(const lc_pouch_collection_record *record,
                                int duplicate, lc_collection_append_res *out) {
  memset(out, 0, sizeof(*out));
  out->position = record->position;
  out->bytes = record->bytes;
  memcpy(out->etag, record->etag, sizeof(out->etag));
  out->duplicate = duplicate;
  out->deleted = record->deleted;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix = !record->deleted;
  out->retention_expires_at_unix = record->deleted ? 0 : record->retention;
  out->has_collection_retention_expires_at_unix =
      record->has_collection_retention;
  out->collection_retention_expires_at_unix = record->collection_retention;
}

lc_pouch_unix_seconds
lc_collection_retention_deadline(const lc_collection_handle *collection) {
  return collection->order.collection_retention_generation >
                 collection->head.retention_generation
             ? collection->order.collection_retention
             : collection->head.retention;
}

static void lc_cc_describe_result(lc_collection_handle *collection,
                                  lc_collection_info *out) {
  memset(out, 0, sizeof(*out));
  out->ns = collection->ns;
  out->name = collection->name;
  out->collection_id = collection->id;
  out->upper_position = collection->order.upper_position;
  out->live_count = collection->order.live_count;
  out->query_indexing = collection->pouch->query_indexing_enabled;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix = 1;
  out->retention_expires_at_unix = lc_collection_retention_deadline(collection);
  out->max_entries = collection->head.max_entries;
  out->lease = collection->head.lease;
}

static int lc_cc_describe(lc_collection *self, lc_collection_info *out,
                          lc_error *error) {
  lc_collection_call call;
  int rc, pin;
  if (out == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection description requires output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_capture_enter(call.collection, &pin, error);
  if (rc == LC_OK) {
    rc = lc_collection_head_read(call.collection, error);
    if (rc == LC_OK)
      rc = lc_cc_order_refresh(call.collection, error);
    if (rc == LC_OK) {
      lc_cc_describe_result(call.collection, out);
      rc = lc_collection_lease_observe(call.collection, &out->lease, error);
    }
    lc_collection_capture_leave(call.collection, &pin);
  }
  lc_collection_call_end(&call);
  return rc;
}

static int lc_cc_metadata(lc_collection *self,
                          const lc_collection_metadata_req *req,
                          lc_collection_info *out, lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_pouch_unix_seconds now, deadline;
  int rc;
  if (req == NULL || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection metadata requires request and output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_state_retention_validate(req->has_retention_seconds,
                                   req->retention_seconds, call.collection->ns,
                                   NULL, error);
  if (rc == LC_OK)
    rc = lc_collection_write_enter(&call, &guard, error);
  if (rc == LC_OK) {
    if (req->has_retention_seconds) {
      rc = lc_pouch_now_unix(&now, error);
      if (rc == LC_OK)
        rc = lc_pouch_retention_resolve(
            -(lc_pouch_unix_seconds)req->retention_seconds, now, &deadline,
            error);
      if (rc == LC_OK) {
        /* The durable override boundary cannot name a generation that a
         * NoSync power loss could discard and then reuse for a new append. */
        if (fsync(call.collection->active_fd) != 0)
          rc = lc_collection_failure(
              error, LC_ERR_TRANSPORT, NULL,
              "failed to sync collection policy boundary");
      }
      if (rc == LC_OK) {
        call.collection->head.retention = deadline;
        call.collection->head.retention_generation =
            call.collection->order.generation;
      }
    }
    if (rc == LC_OK && req->has_max_entries)
      call.collection->head.max_entries = req->max_entries;
    if (req->has_retention_seconds || req->has_max_entries) {
      if (rc == LC_OK)
        rc = lc_collection_head_write(call.collection, error);
      /* Also discard speculative values after an unsuccessful write. */
      call.collection->head_loaded = 0;
    }
    if (rc == LC_OK)
      lc_cc_describe_result(call.collection, out);
    if (rc == LC_OK)
      rc = lc_collection_lease_observe(call.collection, &out->lease, error);
    lc_collection_metadata_leave(call.collection);
    lc_collection_write_leave(&guard);
  }
  if (rc == LC_OK && (req->has_retention_seconds || req->has_max_entries))
    lc_pouch_compaction_note_mutation(call.collection->pouch,
                                      call.collection->ns, 0);
  lc_collection_call_end(&call);
  return rc;
}

static int lc_cc_status(lc_collection *self, const char *append_id,
                        lc_collection_append_status_res *out, lc_error *error) {
  lc_collection_call call;
  lc_pouch_collection_record record;
  int found, pin;
  int rc;
  if (!lc_cc_id_valid(append_id) || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "append status requires a valid append ID and output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_capture_enter(call.collection, &pin, error);
  if (rc == LC_OK) {
    rc = lc_collection_refresh_view(call.collection, error);
    if (rc == LC_OK)
      rc = lc_collection_ids_refresh(&call.collection->ids, error);
    if (rc == LC_OK)
      rc = lc_collection_lookup_id(call.collection, append_id, &record, NULL,
                                   &found, error);
    if (rc == LC_OK && found) {
      out->state = record.deleted ? LC_COLLECTION_APPEND_DELETED
                                  : LC_COLLECTION_APPEND_COMMITTED;
      lc_cc_append_result(&record, 0, &out->entry);
    }
    lc_collection_capture_leave(call.collection, &pin);
  }
  lc_collection_call_end(&call);
  return rc;
}

typedef struct lc_cc_source {
  lc_source pub;
  lc_source *inner;
  lc_collection_call *call;
} lc_cc_source;

static size_t lc_cc_source_read(lc_source *self, void *bytes, size_t count,
                                lc_error *error) {
  lc_cc_source *source;
  size_t got;
  source = (lc_cc_source *)self;
  lc_collection_call_callback(source->call, 1, 1);
  got = source->inner->read(source->inner, bytes, count, error);
  lc_collection_call_callback(source->call, 0, 0);
  return got;
}

typedef struct lc_cc_hash_sink {
  lc_sink pub;
  EVP_MD_CTX *hash;
} lc_cc_hash_sink;

static int lc_cc_hash_write(lc_sink *self, const void *bytes, size_t length,
                            lc_error *error) {
  lc_cc_hash_sink *sink;
  sink = (lc_cc_hash_sink *)self;
  if (EVP_DigestUpdate(sink->hash, bytes, length) == 1)
    return 1;
  lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                        "failed to hash collection retry body");
  return 0;
}

static int lc_cc_verify_retry(lc_collection_handle *collection,
                              lc_source *source,
                              const lc_pouch_collection_record *record,
                              lc_error *error) {
  lc_cc_hash_sink sink;
  unsigned char digest[32];
  unsigned int length;
  char etag[65];
  lc_u64 written;
  size_t i;
  int rc;
  memset(&sink, 0, sizeof(sink));
  sink.pub.write = lc_cc_hash_write;
  sink.hash = EVP_MD_CTX_new();
  if (sink.hash == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection retry hash");
  rc =
      EVP_DigestInit_ex(sink.hash, EVP_sha256(), NULL) == 1
          ? LC_OK
          : lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                  "failed to initialize collection retry hash");
  if (rc == LC_OK)
    rc = lc_collection_json_copy(source, &sink.pub, &collection->allocator,
                                 &written, error);
  length = 0U;
  if (rc == LC_OK && (EVP_DigestFinal_ex(sink.hash, digest, &length) != 1 ||
                      length != sizeof(digest)))
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to finalize collection retry hash");
  EVP_MD_CTX_free(sink.hash);
  if (rc != LC_OK)
    return rc;
  for (i = 0U; i < sizeof(digest); ++i) {
    static const char hex[] = "0123456789abcdef";
    etag[i * 2U] = hex[digest[i] >> 4U];
    etag[i * 2U + 1U] = hex[digest[i] & 15U];
  }
  etag[64] = '\0';
  return strcmp(etag, record->etag) == 0 && written == record->bytes
             ? LC_OK
             : lc_collection_failure(
                   error, LC_ERR_INVALID, "append_id_conflict",
                   "append ID is bound to different compact JSON bytes");
}

static int lc_cc_pin_segment(lc_collection_handle *collection, lc_u64 segment,
                             int *fd, lc_error *error) {
  char *path;
  char leaf[64];
  int rc;
  *fd = -1;
  if (!collection->pouch->durable_sync)
    return LC_OK;
  rc = LC_OK;
  /* Another call may rotate this receiver as soon as append authority leaves.
   * Pin the exact canonical inode, including a duplicate's older binding. */
  if (segment == collection->active_fd_segment)
    *fd = fcntl(collection->active_fd, F_DUPFD_CLOEXEC, 0);
  else {
    path = NULL;
    rc = lc_collection_record_path(collection, segment, &path, leaf, error);
    if (rc == LC_OK)
      *fd = open(path, O_RDONLY | O_CLOEXEC);
    lc_free_with_allocator(&collection->allocator, path);
  }
  if (rc == LC_OK && *fd < 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to pin collection commit segment");
  return rc;
}

static int lc_cc_commit_pin(lc_collection_handle *collection,
                            lc_collection_guard *guard, lc_u64 segment, int *fd,
                            lc_error *error) {
  int rc;
  *fd = -1;
  if (!collection->pouch->durable_sync)
    return LC_OK;
  rc = lc_cc_commit_enter(collection, 0, &guard->commit_fd, error);
  return rc == LC_OK ? lc_cc_pin_segment(collection, segment, fd, error) : rc;
}

int lc_collection_append_validate(lc_collection_handle *collection,
                                  const lc_collection_append_req *req,
                                  lc_error *error) {
  int rc;
  if (req == NULL || !lc_cc_id_valid(req->append_id))
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection append requires a valid append ID");
  rc = lc_state_retention_validate(req->has_retention_seconds,
                                   req->retention_seconds, collection->ns, NULL,
                                   error);
  if (rc == LC_OK)
    rc = lc_state_retention_validate(req->has_collection_retention_seconds,
                                     req->collection_retention_seconds,
                                     collection->ns, NULL, error);
  return rc;
}

static int lc_cc_append(lc_collection *self,
                        const lc_collection_append_req *req, lc_source *source,
                        lc_collection_append_res *out, lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_pouch_collection_record record;
  lc_pouch_collection_writer *writer = NULL;
  lc_cc_source guarded;
  lc_collection_handle *collection;
  lc_u64 segment;
  lc_u64 position;
  lc_u64 generation;
  lc_u64 query_ordinal;
  lc_u64 ownership_generation;
  char leaf[64];
  int found;
  int rc, sync_fd;
  if (req == NULL || source == NULL || source->read == NULL || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection append requires identity, source, and output");
  memset(out, 0, sizeof(*out));
  query_ordinal = LC_U64_MAX;
  sync_fd = -1;
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  memset(&record, 0, sizeof(record));
  rc = lc_collection_append_validate(collection, req, error);
  if (rc != LC_OK) {
    lc_collection_call_end(&call);
    return rc;
  }
  rc = lc_cc_write_enter(&call, &guard, 1, error);
  if (rc != LC_OK) {
    lc_collection_call_end(&call);
    return rc;
  }
  found = 0;
  ownership_generation = collection->head.lease.fencing_token;
  rc = lc_collection_lease_check(collection, 1, error);
  if (rc == LC_OK)
    rc = lc_collection_lookup_id(collection, req->append_id, &record, &segment,
                                 &found, error);
  if (rc == LC_OK && !found &&
      (collection->order.upper_position == LC_U64_MAX ||
       collection->order.generation == LC_U64_MAX))
    rc = lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                               "collection sequence is exhausted");
  if (rc == LC_OK && !found)
    rc = lc_collection_rotate(collection, 0, error);
  if (!found)
    segment = collection->head.active;
  position = collection->order.upper_position + (found ? 0U : 1U);
  generation = collection->order.generation + (found ? 0U : 1U);
  if (rc == LC_OK)
    rc = lc_collection_record_leaf(segment, leaf, error);
  lc_collection_metadata_leave(collection);
  memset(&guarded, 0, sizeof(guarded));
  guarded.pub.read = lc_cc_source_read;
  guarded.inner = source;
  guarded.call = &call;
  if (rc == LC_OK && found) {
    rc = lc_cc_verify_retry(collection, &guarded.pub, &record, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_collection_writer_open(
        collection->pouch, collection->active_fd, leaf, collection->id,
        req->append_id, position, generation,
        req->has_retention_seconds
            ? -(lc_pouch_unix_seconds)req->retention_seconds
            : 0,
        req->has_collection_retention_seconds,
        req->has_collection_retention_seconds
            ? -(lc_pouch_unix_seconds)req->collection_retention_seconds
            : 0,
        &writer, error);
    if (rc == LC_OK)
      rc = lc_pouch_collection_writer_consume(writer, collection->pouch,
                                              &guarded.pub, &record, error);
  }
  if (rc == LC_OK)
    rc = lc_cc_commit_pin(collection, &guard, segment, &sync_fd, error);
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(collection, 1, error);
    if (rc == LC_OK) {
      rc = lc_collection_head_read(collection, error);
      if (rc == LC_OK)
        rc = lc_collection_lease_check(collection, 1, error);
      if (rc == LC_OK &&
          ownership_generation != collection->head.lease.fencing_token)
        rc =
            lc_collection_failure(error, LC_ERR_INVALID, "lease_changed",
                                  "collection ownership changed during upload");
      if (rc == LC_OK && !found)
        rc = lc_pouch_collection_writer_publish(writer, leaf, position,
                                                generation, &record, error);
      if (record.published || (rc == LC_OK && found))
        lc_cc_append_result(&record, found, out);
      if (rc == LC_OK && !found)
        rc = lc_collection_project_record(&collection->order, &collection->ids,
                                          &record, segment, 0, error);
      if (rc == LC_OK && !found)
        rc = lc_collection_ids_checkpoint(
            &collection->ids, lc_collection_binding_count(&collection->order),
            collection->order.generation, segment, record.next_offset, error);
      if (rc == LC_OK && !found && collection->pouch->query_indexing_enabled &&
          collection->order.count % LC_COLLECTION_ORDER_PAGE_ROWS == 0U)
        query_ordinal = collection->order.count - LC_COLLECTION_ORDER_PAGE_ROWS;
      lc_collection_metadata_leave(collection);
    }
  }
  lc_pouch_collection_writer_close(writer);
  if (rc != LC_OK && !found && record.published)
    collection->trusted = 0;
  if (rc == LC_OK && sync_fd >= 0) {
    /* The commit pin keeps capture/compaction behind this group. Appenders may
     * now stage the next complete record while this caller waits for sync. Keep
     * the root mode pin until the durable outcome is known. */
    lc_cc_write_unlock(&guard);
#ifdef LOCKDC_TEST_BUILD
    if (lc_collection_commit_wait_test_hook != NULL)
      rc = lc_collection_commit_wait_test_hook(
          lc_collection_commit_wait_test_context, self, error);
#endif
    if (rc == LC_OK)
      rc = lc_pouch_fsync_commit(collection->pouch, sync_fd, error);
  }
  if (sync_fd >= 0)
    (void)close(sync_fd);
  lc_collection_write_leave(&guard);
  if (rc == LC_OK && !found)
    lc_pouch_compaction_note_mutation(collection->pouch, collection->ns, 0);
  if (rc == LC_OK && query_ordinal != LC_U64_MAX) {
    lc_error index_error;
    int indexed;
    lc_error_init(&index_error);
    indexed = lc_collection_query_build(&call, query_ordinal, &index_error);
    if (indexed != LC_OK) {
      pslog_field field;
      field = lc_log_error_field("error", &index_error);
      lc_log_warn(collection->pouch->logger,
                  "collection append committed; query page will use streaming "
                  "evaluation",
                  &field, 1U);
    }
    lc_error_cleanup(&index_error);
  }
  lc_collection_call_end(&call);
  return rc;
}

int lc_collection_publish_staged(lc_collection_call *call,
                                 lc_pouch_collection_writer *writer,
                                 lc_source *source, lc_u64 ownership_generation,
                                 const lc_pouch_collection_record *body,
                                 lc_collection_append_res *out,
                                 lc_error *error) {
  lc_collection_handle *collection = call->collection;
  lc_collection_guard guard;
  lc_pouch_collection_record current;
  lc_u64 segment = 0U, query_ordinal = LC_U64_MAX;
  char leaf[64];
  int found = 0, rc, sync_fd = -1;
  memset(&current, 0, sizeof(current));
  memset(out, 0, sizeof(*out));
  rc = lc_cc_write_enter(call, &guard, 1, error);
  if (rc != LC_OK) {
    lc_pouch_collection_writer_close(writer);
    return rc;
  }
  rc = lc_collection_lease_check(collection, 1, error);
  if (rc == LC_OK &&
      ownership_generation != collection->head.lease.fencing_token)
    rc = lc_collection_failure(error, LC_ERR_INVALID, "lease_changed",
                               "collection ownership changed during upload");
  if (rc == LC_OK)
    rc = lc_collection_lookup_id(collection, body->append_id, &current,
                                 &segment, &found, error);
  if (rc == LC_OK && found &&
      (body->bytes != current.bytes || strcmp(body->etag, current.etag) != 0))
    rc = lc_collection_failure(
        error, LC_ERR_INVALID, "append_id_conflict",
        "append ID is bound to different compact JSON bytes");
  if (rc == LC_OK && !found) {
    if (source == NULL || source->read == NULL)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection retry binding disappeared");
    else if (collection->order.upper_position == LC_U64_MAX ||
             collection->order.generation == LC_U64_MAX)
      rc = lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                 "collection sequence is exhausted");
    if (rc == LC_OK)
      rc = lc_collection_rotate(collection, 0, error);
    segment = collection->head.active;
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(segment, leaf, error);
  }
  lc_collection_metadata_leave(collection);
  if (rc == LC_OK && !found)
    rc = lc_pouch_collection_writer_copy_staged(writer, collection->active_fd,
                                                leaf, source, error);
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && !found && lc_collection_append_test_hook != NULL)
    rc = lc_collection_append_test_hook(lc_collection_append_test_context,
                                        &collection->pub,
                                        LC_COLLECTION_APPEND_STAGED, error);
#endif
  if (rc == LC_OK)
    rc = lc_cc_commit_pin(collection, &guard, segment, &sync_fd, error);
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(collection, 1, error);
    if (rc == LC_OK) {
      rc = lc_collection_head_read(collection, error);
      if (rc == LC_OK)
        rc = lc_collection_lease_check(collection, 1, error);
      if (rc == LC_OK &&
          ownership_generation != collection->head.lease.fencing_token)
        rc =
            lc_collection_failure(error, LC_ERR_INVALID, "lease_changed",
                                  "collection ownership changed during upload");
      if (rc == LC_OK && !found)
        rc = lc_pouch_collection_writer_publish(
            writer, leaf, collection->order.upper_position + 1U,
            collection->order.generation + 1U, &current, error);
#ifdef LOCKDC_TEST_BUILD
      if (rc == LC_OK && !found && lc_collection_append_test_hook != NULL)
        rc = lc_collection_append_test_hook(
            lc_collection_append_test_context, &collection->pub,
            LC_COLLECTION_APPEND_PUBLISHED, error);
#endif
      if (rc == LC_OK && !found)
        rc = lc_collection_project_record(&collection->order, &collection->ids,
                                          &current, segment, 0, error);
      if (rc == LC_OK && !found)
        rc = lc_collection_ids_checkpoint(
            &collection->ids, lc_collection_binding_count(&collection->order),
            collection->order.generation, segment, current.next_offset, error);
      if (rc == LC_OK && !found && collection->pouch->query_indexing_enabled &&
          collection->order.count % LC_COLLECTION_ORDER_PAGE_ROWS == 0U)
        query_ordinal = collection->order.count - LC_COLLECTION_ORDER_PAGE_ROWS;
#ifdef LOCKDC_TEST_BUILD
      if (rc == LC_OK && !found && lc_collection_append_test_hook != NULL)
        rc = lc_collection_append_test_hook(
            lc_collection_append_test_context, &collection->pub,
            LC_COLLECTION_APPEND_PROJECTED, error);
#endif
      lc_collection_metadata_leave(collection);
    }
  }
  if (rc == LC_OK || (!found && current.published))
    lc_cc_append_result(&current, found, out);
  if (rc != LC_OK && !found && current.published)
    collection->trusted = 0;
  /* Roll back unpublished bytes while append authority still excludes peers. */
  lc_pouch_collection_writer_close(writer);
  if (rc == LC_OK && sync_fd >= 0) {
    lc_cc_write_unlock(&guard);
#ifdef LOCKDC_TEST_BUILD
    if (lc_collection_commit_wait_test_hook != NULL)
      rc = lc_collection_commit_wait_test_hook(
          lc_collection_commit_wait_test_context, &collection->pub, error);
#endif
    if (rc == LC_OK)
      rc = lc_pouch_fsync_commit(collection->pouch, sync_fd, error);
  }
  if (sync_fd >= 0)
    (void)close(sync_fd);
  lc_collection_write_leave(&guard);
  if (rc == LC_OK && !found)
    lc_pouch_compaction_note_mutation(collection->pouch, collection->ns, 0);
  if (rc == LC_OK && query_ordinal != LC_U64_MAX) {
    lc_error index_error;
    lc_error_init(&index_error);
    if (lc_collection_query_build(call, query_ordinal, &index_error) != LC_OK) {
      pslog_field field = lc_log_error_field("error", &index_error);
      lc_log_warn(collection->pouch->logger,
                  "collection append committed; query page will use streaming "
                  "evaluation",
                  &field, 1U);
    }
    lc_error_cleanup(&index_error);
  }
  return rc;
}

static int lc_cc_entry_metadata(lc_collection *self, uint64_t position,
                                const lc_collection_metadata_req *req,
                                lc_collection_append_res *out,
                                lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_handle *collection;
  lc_collection_order_row row;
  lc_pouch_collection_record current, changed;
  lc_u64 file_segment, file_end, segment, generation;
  char leaf[64];
  int fd, found, rc;
  if (position == 0U || req == NULL || out == NULL || req->has_max_entries)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "entry metadata requires position, expiry-only request and output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  rc = lc_state_retention_validate(req->has_retention_seconds,
                                   req->retention_seconds, collection->ns, NULL,
                                   error);
  if (rc == LC_OK)
    rc = lc_collection_write_enter(&call, &guard, error);
  if (rc != LC_OK) {
    lc_collection_call_end(&call);
    return rc;
  }
  fd = -1;
  file_segment = file_end = segment = generation = 0U;
  memset(&changed, 0, sizeof(changed));
  rc = lc_collection_order_find(&collection->order, position, &row, &found,
                                error);
  if (rc == LC_OK && (!found || row.deleted_at != 0U))
    rc = lc_collection_failure(error, LC_ERR_INVALID, "entry_not_found",
                               "entry metadata requires a live state object");
  if (rc == LC_OK)
    rc = lc_collection_record_ref(collection, row.segment, row.record_offset,
                                  &fd, &file_segment, &file_end, &current,
                                  error);
  if (rc == LC_OK &&
      (current.deleted || current.position != position ||
       current.published_at != row.published_at || current.bytes != row.bytes))
    rc =
        lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                              "entry metadata identity differs from its order");
  if (rc == LC_OK && req->has_retention_seconds) {
    if (collection->order.generation == LC_U64_MAX)
      rc = lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                 "collection mutation sequence is exhausted");
    if (rc == LC_OK)
      rc = lc_collection_rotate(collection, 0, error);
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(collection->head.active, leaf, error);
    segment = collection->head.active;
    generation = collection->order.generation + 1U;
    lc_collection_metadata_leave(collection);
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_metadata(
          collection->pouch, collection->active_fd, leaf, &current, row.segment,
          generation, -(lc_pouch_unix_seconds)req->retention_seconds, &changed,
          error);
    if (rc == LC_OK)
      rc = lc_pouch_fsync_commit(collection->pouch, collection->active_fd,
                                 error);
    if (changed.published)
      lc_cc_append_result(&changed, 0, out);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        rc = lc_collection_project_record(&collection->order, &collection->ids,
                                          &changed, segment, 0, error);
        if (rc == LC_OK)
          rc = lc_collection_ids_checkpoint(
              &collection->ids, lc_collection_binding_count(&collection->order),
              collection->order.generation, segment, changed.next_offset,
              error);
        lc_collection_metadata_leave(collection);
      }
    }
  } else {
    if (rc == LC_OK)
      lc_cc_append_result(&current, 0, out);
    lc_collection_metadata_leave(collection);
  }
  if (fd >= 0)
    (void)close(fd);
  if (rc != LC_OK && changed.published)
    collection->trusted = 0;
  lc_collection_write_leave(&guard);
  if (rc == LC_OK && req->has_retention_seconds)
    lc_pouch_compaction_note_mutation(collection->pouch, collection->ns, 0);
  lc_collection_call_end(&call);
  return rc;
}

static int lc_cc_delete_current(lc_collection *self, uint64_t position,
                                int only_due, lc_pouch_unix_seconds now,
                                lc_collection_delete_res *out,
                                lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_order_row row;
  lc_pouch_collection_record record;
  lc_pouch_collection_record deleted;
  lc_collection_handle *collection;
  lc_u64 file_segment;
  lc_u64 file_end;
  lc_u64 segment;
  lc_u64 generation;
  int fd;
  int found;
  int changed;
  int rc;
  if ((position == 0U && only_due != 2) || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection deletion requires position and output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  call.maintenance = only_due != 0;
  rc = lc_collection_write_enter(&call, &guard, error);
  if (rc != LC_OK) {
    lc_collection_call_end(&call);
    return rc;
  }
  fd = -1;
  file_segment = 0U;
  file_end = 0U;
  changed = 0;
  memset(&deleted, 0, sizeof(deleted));
  if (only_due == 2) {
    if (collection->head.max_entries == 0U ||
        collection->order.live_count <= collection->head.max_entries) {
      lc_collection_metadata_leave(collection);
      goto finish_delete;
    }
    rc = lc_collection_order_oldest(&collection->order, &row, &found, error);
    if (rc == LC_OK && !found)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection live count has no oldest entry");
    if (rc != LC_OK) {
      lc_collection_metadata_leave(collection);
      goto finish_delete;
    }
    position = row.position;
  }
  rc = lc_collection_order_find(&collection->order, position, &row, &found,
                                error);
  if (rc == LC_OK && !found)
    rc = lc_collection_failure(error, LC_ERR_INVALID, "entry_not_found",
                               "collection position was never assigned");
  if (rc == LC_OK && row.deleted_at != 0U) {
    out->position = position;
    out->deleted = 1;
    out->already_deleted = 1;
  } else if (rc == LC_OK) {
    rc =
        lc_collection_record_ref(collection, row.segment, row.record_offset,
                                 &fd, &file_segment, &file_end, &record, error);
    if (rc == LC_OK &&
        (record.deleted || record.position != position ||
         record.published_at != row.published_at || record.bytes != row.bytes))
      rc = lc_collection_failure(
          error, LC_ERR_PROTOCOL, "collection_corrupt",
          "collection deletion identity differs from its ordered state");
    if (rc == LC_OK && only_due == 1 &&
        (record.retention == 0 || record.retention > now)) {
      lc_collection_metadata_leave(collection);
      goto finish_delete;
    }
    if (rc == LC_OK && collection->order.generation == LC_U64_MAX)
      rc = lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                 "collection mutation sequence is exhausted");
    if (rc == LC_OK)
      rc = lc_collection_rotate(collection, 0, error);
    segment = collection->head.active;
    generation = collection->order.generation + 1U;
    lc_collection_metadata_leave(collection);
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_delete(collection->pouch,
                                             collection->active_fd, &record,
                                             generation, &deleted, error);
    if (rc == LC_OK)
      rc = lc_pouch_fsync_commit(collection->pouch, collection->active_fd,
                                 error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        rc = lc_collection_project_record(&collection->order, &collection->ids,
                                          &deleted, segment, 0, error);
        if (rc == LC_OK)
          rc = lc_collection_ids_checkpoint(
              &collection->ids, lc_collection_binding_count(&collection->order),
              collection->order.generation, segment, deleted.next_offset,
              error);
        lc_collection_metadata_leave(collection);
      }
    }
    if (rc == LC_OK) {
      out->position = position;
      out->deleted = 1;
      changed = 1;
    }
    goto finish_delete;
  }
  lc_collection_metadata_leave(collection);
finish_delete:
  if (fd >= 0)
    (void)close(fd);
  if (rc != LC_OK && deleted.published)
    collection->trusted = 0;
  lc_collection_write_leave(&guard);
  if (changed)
    lc_pouch_compaction_note_mutation(collection->pouch, collection->ns, 1);
  lc_collection_call_end(&call);
  return rc;
}

static int lc_cc_delete(lc_collection *self, uint64_t position,
                        lc_collection_delete_res *out, lc_error *error) {
  return lc_cc_delete_current(self, position, 0, 0, out, error);
}

int lc_collection_expire_entry(lc_collection *self, uint64_t position,
                               lc_pouch_unix_seconds now,
                               lc_collection_delete_res *out, lc_error *error) {
  return lc_cc_delete_current(self, position, 1, now, out, error);
}

int lc_collection_trim_entry(lc_collection *self, lc_collection_delete_res *out,
                             lc_error *error) {
  return lc_cc_delete_current(self, 0, 2, 0, out, error);
}

static int lc_cc_drop_current(lc_collection *self, int only_due,
                              lc_pouch_unix_seconds now, int *removed,
                              lc_error *error) {
  lc_collection_call call;
  lc_collection_call scope;
  lc_collection_guard guard;
  lc_collection_guard root_guard;
  lc_collection_handle *collection;
  int catalog_fd;
  int rc;
  *removed = 0;
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  call.maintenance = only_due != 0;
  collection = call.collection;
  catalog_fd = -1;
  rc = lc_collection_root_begin(collection->pouch, &scope, &root_guard, error);
  if (rc != LC_OK) {
    lc_collection_call_end(&call);
    return rc;
  }
  rc = lc_collection_catalog_lock(collection->catalog_base, &catalog_fd, error);
  if (rc == LC_OK) {
    rc = lc_collection_write_enter(&call, &guard, error);
    if (rc == LC_OK) {
      if (only_due && (lc_collection_retention_deadline(collection) == 0 ||
                       lc_collection_retention_deadline(collection) > now))
        goto leave_drop;
      rc = lc_collection_catalog_bound(collection, error);
      if (rc == LC_OK) {
        collection->head.dropped = 1;
        rc = lc_collection_head_write(collection, error);
        collection->head_loaded = 0;
        /* Logical deletion is the authority even if catalog cleanup fails. */
        collection->trusted = 0;
      }
      if (rc == LC_OK && unlink(collection->catalog_path) != 0 &&
          errno != ENOENT)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to remove collection name");
      if (rc == LC_OK) {
        char *names;
        names = lc_pouch_path_join(&collection->allocator,
                                   collection->catalog_base, "names");
        if (names == NULL)
          rc = lc_collection_failure(
              error, LC_ERR_NOMEM, NULL,
              "failed to allocate collection catalog path");
        else
          rc = lc_pouch_path_fsync_directory(
              names, "failed to sync collection drop", error);
        lc_free_with_allocator(&collection->allocator, names);
      }
      if (rc == LC_OK)
        *removed = 1;
    leave_drop:
      lc_collection_metadata_leave(collection);
      lc_collection_write_leave(&guard);
    }
    (void)close(catalog_fd);
  }
  lc_collection_root_end(&scope, &root_guard);
  lc_collection_call_end(&call);
  return rc;
}

static int lc_cc_drop(lc_collection *self, lc_error *error) {
  int removed;
  return lc_cc_drop_current(self, 0, 0, &removed, error);
}

int lc_collection_expire(lc_collection *self, lc_pouch_unix_seconds now,
                         int *removed, lc_error *error) {
  return lc_cc_drop_current(self, 1, now, removed, error);
}

int lc_collection_open_bound(lc_client_handle *client, const char *ns,
                             const char *name, const char *id, const char *base,
                             const char *catalog, int create,
                             lc_pouch_unix_seconds retention,
                             lc_u64 max_entries, lc_collection **out,
                             lc_error *error) {
  return lc_collection_storage_open(client->pouch, client, ns, name, id, base,
                                    catalog, create, retention, max_entries,
                                    out, error);
}

int lc_collection_storage_open(lc_pouch *pouch, lc_client_handle *client,
                               const char *ns, const char *name, const char *id,
                               const char *base, const char *catalog,
                               int create, lc_pouch_unix_seconds retention,
                               lc_u64 max_entries, lc_collection **out,
                               lc_error *error) {
  lc_collection_handle *collection;
  const lc_allocator *allocator;
  lc_collection_guard guard;
  lc_collection_guard root_guard;
  lc_collection_call scope;
  lc_pouch_unix_seconds now;
  char *data;
  int first;
  int valid;
  int rc;
  int entered;
  allocator = &pouch->allocator;
  collection = (lc_collection_handle *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*collection));
  if (collection == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection receiver");
  collection->client = client;
  collection->pouch = pouch;
  collection->allocator = *allocator;
  if (client != NULL)
    lc_client_handle_retain(client);
  collection->session_fd = collection->join_fd = collection->metadata_fd =
      collection->append_fd = collection->active_fd = collection->head_fd = -1;
  collection->order.fd = collection->ids.fd = -1;
  collection->references = 1UL;
  memcpy(collection->id, id, sizeof(collection->id));
  collection->ns = lc_strdup_with_allocator(allocator, ns);
  collection->name = lc_strdup_with_allocator(allocator, name);
  collection->catalog_path = lc_strdup_with_allocator(allocator, catalog);
  collection->catalog_base = lc_strdup_with_allocator(allocator, base);
  data = lc_pouch_path_join(allocator, base, "data");
  if (data != NULL)
    collection->directory = lc_pouch_path_join(allocator, data, id);
  lc_free_with_allocator(allocator, data);
  if (collection->directory != NULL) {
    collection->head_path = lc_cc_path(collection, "head");
    collection->order_path = lc_cc_path(collection, "order");
    collection->ids_path = lc_cc_path(collection, "ids");
  }
  rc = LC_OK;
  entered = 0;
  if (collection->ns == NULL || collection->name == NULL ||
      collection->directory == NULL || collection->catalog_path == NULL ||
      collection->catalog_base == NULL || collection->head_path == NULL ||
      collection->order_path == NULL || collection->ids_path == NULL)
    rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                               "failed to allocate collection identity paths");
  if (rc == LC_OK) {
    rc = lc_collection_root_begin(pouch, &scope, &root_guard, error);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK && create)
    rc = lc_collection_maintenance_note(pouch, ns, error);
  if (rc == LC_OK && create && mkdir(collection->directory, 0700) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to create collection partition");
  if (rc == LC_OK && create)
    lc_pouch_compaction_note_mutation(pouch, ns, 0);
  if (rc == LC_OK) {
    if (pthread_mutex_init(&collection->metadata_mutex, NULL) != 0)
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to initialize collection metadata mutex");
    else
      collection->metadata_mutex_initialized = 1;
  }
  if (rc == LC_OK) {
    if (pthread_mutex_init(&collection->append_mutex, NULL) != 0)
      rc =
          lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                "failed to initialize collection append mutex");
    else
      collection->append_mutex_initialized = 1;
  }
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".session.lock", O_RDWR | O_CREAT,
                         &collection->session_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".join.lock", O_RDWR | O_CREAT,
                         &collection->join_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".metadata.lock", O_RDWR | O_CREAT,
                         &collection->metadata_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".append.lock", O_RDWR | O_CREAT,
                         &collection->append_fd, error);
  first = 0;
  if (rc == LC_OK)
    rc = lc_cc_lock(collection->join_fd, LOCK_EX, error);
  if (rc == LC_OK) {
    if (flock(collection->session_fd, LOCK_EX | LOCK_NB) == 0)
      first = 1;
    else if (errno == EWOULDBLOCK || errno == EAGAIN)
      rc = lc_cc_lock(collection->session_fd, LOCK_SH, error);
    else
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to enter collection session");
  }
  if (rc == LC_OK && (first || create))
    rc = lc_cc_root_enter(collection->pouch, NULL, &guard, error);
  if (rc == LC_OK && (first || create)) {
    rc = lc_cc_lock(collection->append_fd, LOCK_EX, error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        if (create) {
          collection->head.max_entries = max_entries;
          collection->head.active = collection->head.tail_segment = 1U;
          rc = lc_cc_open_file(collection, "seg-00000000000000000001.log",
                               O_RDWR | O_CREAT | O_EXCL,
                               &collection->active_fd, error);
          collection->active_fd_segment = 1U;
          if (rc == LC_OK && fsync(collection->active_fd) != 0)
            rc = lc_collection_failure(
                error, LC_ERR_TRANSPORT, NULL,
                "failed to sync initial collection segment");
          if (rc == LC_OK)
            rc = lc_pouch_now_unix(&now, error);
          if (rc == LC_OK)
            rc = lc_pouch_retention_resolve(retention, now,
                                            &collection->head.retention, error);
          if (rc == LC_OK)
            rc = lc_collection_head_write(collection, error);
          if (rc == LC_OK)
            rc = lc_collection_order_open(collection->order_path, 1,
                                          &collection->order, error);
          if (rc == LC_OK)
            rc = lc_collection_ids_open(collection->ids_path, 1, allocator,
                                        &collection->ids, error);
          if (rc == LC_OK)
            rc = lc_collection_order_checkpoint(&collection->order, 1U, 0U,
                                                error);
          if (rc == LC_OK)
            rc = lc_collection_ids_checkpoint(&collection->ids, 0U, 0U, 1U, 0U,
                                              error);
          if (rc == LC_OK)
            collection->trusted = 1;
        } else {
          rc = lc_collection_head_read(collection, error);
          valid = 0;
          if (rc == LC_OK && first) {
            rc = lc_cc_order_refresh(collection, error);
            if (rc == LC_OK)
              rc = lc_collection_ids_open(collection->ids_path, 0, allocator,
                                          &collection->ids, error);
            if (rc == LC_OK)
              rc = lc_cc_checkpoint(collection, 0, &valid, error);
            else if (rc == LC_ERR_PROTOCOL ||
                     (rc == LC_ERR_TRANSPORT && errno == ENOENT)) {
              lc_error_cleanup(error);
              lc_error_init(error);
              rc = LC_OK;
            }
          }
          collection->trusted = !first || valid;
          if (rc == LC_OK)
            rc = lc_collection_reconcile(collection, error);
        }
        if (rc == LC_OK && first)
          rc = lc_cc_invalidate_clean(collection, error);
        lc_collection_metadata_leave(collection);
      }
      (void)flock(collection->append_fd, LOCK_UN);
    }
    lc_cc_root_leave(&guard);
  }
  if (rc == LC_OK && !first && !create) {
    collection->trusted = 1;
    rc = lc_collection_prepare(collection, error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 0, error);
      if (rc == LC_OK) {
        rc = lc_collection_refresh_view(collection, error);
        if (rc == LC_OK && collection->ids.fd < 0)
          rc = lc_collection_ids_open(collection->ids_path, 0, allocator,
                                      &collection->ids, error);
        lc_collection_metadata_leave(collection);
      }
    }
  }
  if (rc == LC_OK && first)
    rc = lc_cc_lock(collection->session_fd, LOCK_SH, error);
  if (collection->join_fd >= 0)
    (void)flock(collection->join_fd, LOCK_UN);
  if (rc == LC_OK && create) {
    data = lc_pouch_path_join(allocator, base, "data");
    if (data == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection partition parent");
    else
      rc = lc_pouch_path_fsync_directory(
          data, "failed to sync collection partition creation", error);
    lc_free_with_allocator(allocator, data);
    if (rc == LC_OK) {
      data = lc_cc_path(collection, "identity");
      if (data == NULL)
        rc = lc_collection_failure(
            error, LC_ERR_NOMEM, NULL,
            "failed to allocate collection identity path");
      else
        rc = lc_collection_catalog_write(data, id, name, error);
      lc_free_with_allocator(allocator, data);
    }
  }
  if (entered)
    lc_collection_root_end(&scope, &root_guard);
  if (rc != LC_OK) {
    collection->trusted = 0;
    lc_collection_destroy(collection);
    return rc;
  }
  collection->pub.head = lc_collection_head_method;
  collection->pub.edit = lc_collection_edit_method;
  collection->pub.edit_status = lc_collection_edit_status_method;
  collection->pub.describe = lc_cc_describe;
  collection->pub.acquire = lc_collection_acquire_method;
  collection->pub.takeover = lc_collection_takeover_method;
  collection->pub.keepalive = lc_collection_keepalive_method;
  collection->pub.release = lc_collection_release_method;
  collection->pub.metadata = lc_cc_metadata;
  collection->pub.append = lc_cc_append;
  collection->pub.append_staged = lc_collection_append_staged_method;
  collection->pub.append_begin = lc_collection_append_begin_method;
  collection->pub.append_status = lc_cc_status;
  collection->pub.entry_metadata = lc_cc_entry_metadata;
  collection->pub.read = lc_collection_read_method;
  collection->pub.delete_entry = lc_cc_delete;
  collection->pub.drop = lc_cc_drop;
  collection->pub.close = lc_cc_close_method;
  collection->pub.ns = collection->ns;
  collection->pub.name = collection->name;
  collection->pub.collection_id = collection->id;
  collection->pub.impl = collection;
  *out = &collection->pub;
  return LC_OK;
}

int lc_collection_recode_head(lc_pouch *pouch, const char *directory,
                              lc_collection_disk_head *head, lc_error *error) {
  lc_collection_handle collection;
  int rc;
  memset(&collection, 0, sizeof(collection));
  collection.pouch = pouch;
  collection.allocator = pouch->allocator;
  collection.directory = (char *)directory;
  collection.head_fd = -1;
  collection.head_path = lc_cc_path(&collection, "head");
  rc = collection.head_path ? lc_collection_head_read(&collection, error)
                            : LC_ERR_NOMEM;
  if (collection.head_fd >= 0)
    (void)close(collection.head_fd);
  lc_free_with_allocator(&collection.allocator, collection.head_path);
  if (rc == LC_ERR_INVALID && collection.head.dropped) {
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
  }
  if (rc == LC_OK)
    *head = collection.head;
  return rc;
}

int lc_collection_recode_finish(lc_pouch_recode *work, const char *source,
                                const char *target, const char *id,
                                lc_error *error) {
  lc_collection_handle collection;
  lc_collection_order previous;
  lc_pouch_recode_file *file;
  lc_u64 translated;
  char leaf[64], query_id[LC_XID_STRING_SIZE];
  char *old_order;
  int rc, valid;
  memset(&collection, 0, sizeof(collection));
  memset(&previous, 0, sizeof(previous));
  previous.fd = -1;
  query_id[0] = '\0';
  collection.pouch = work->pouch;
  collection.allocator = work->pouch->allocator;
  collection.directory = (char *)target;
  collection.active_fd = collection.head_fd = collection.order.fd =
      collection.ids.fd = -1;
  strcpy(collection.id, id);
  collection.order_path = lc_cc_path(&collection, "order");
  collection.ids_path = lc_cc_path(&collection, "ids");
  collection.head_path = lc_cc_path(&collection, "head");
  old_order = lc_pouch_path_join(&collection.allocator, source, "order");
  rc = collection.order_path && collection.ids_path && collection.head_path &&
               old_order
           ? LC_OK
           : LC_ERR_NOMEM;
  if (rc == LC_OK) {
    lc_error ignored;
    lc_error_init(&ignored);
    if (lc_collection_order_open(old_order, 0, &previous, &ignored) == LC_OK)
      strcpy(query_id, previous.query_id);
    lc_collection_order_close(&previous);
    lc_error_cleanup(&ignored);
    rc = lc_collection_head_read(&collection, error);
  }
  if (rc == LC_OK)
    rc = lc_collection_record_leaf(collection.head.tail_segment, leaf, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_find(work, source, leaf, collection.head.tail_offset,
                              &file, &translated, error);
  if (rc == LC_OK) {
    collection.head.tail_offset = translated;
    rc = lc_collection_head_write(&collection, error);
  }
  if (rc == LC_OK)
    rc = lc_cc_rebuild(&collection, error);
  if (rc == LC_OK && query_id[0]) {
    strcpy(collection.order.query_id, query_id);
    rc = lc_collection_order_checkpoint(&collection.order,
                                        collection.order.replay_segment,
                                        collection.order.replay_offset, error);
  }
  if (rc == LC_OK)
    rc = lc_cc_checkpoint(&collection, 1, &valid, error);
  lc_collection_order_close(&collection.order);
  lc_collection_ids_close(&collection.ids);
  if (collection.active_fd >= 0)
    (void)close(collection.active_fd);
  if (collection.head_fd >= 0)
    (void)close(collection.head_fd);
  lc_free_with_allocator(&collection.allocator, collection.order_path);
  lc_free_with_allocator(&collection.allocator, collection.ids_path);
  lc_free_with_allocator(&collection.allocator, collection.head_path);
  lc_free_with_allocator(&collection.allocator, old_order);
  return rc;
}
