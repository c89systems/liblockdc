#include "lc_collection_query.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

static int lc_qgc_unlink(const char *path, int *changed,
                         lc_pouch_maintenance_result *out, lc_error *error) {
  if (unlink(path) == 0) {
    *changed = 1;
    if (out != NULL)
      ++out->cleanup_deleted_count;
    return LC_OK;
  }
  return errno == ENOENT
             ? LC_OK
             : lc_collection_failure(
                   error, LC_ERR_TRANSPORT, NULL,
                   "failed to remove obsolete collection query artifact");
}

static int lc_qgc_temporary(const char *leaf) {
  char id[LC_XID_STRING_SIZE];
  if (strlen(leaf) != 6U + LC_XID_STRING_SIZE - 1U + 4U ||
      strncmp(leaf, ".page-", 6U) != 0 ||
      strcmp(leaf + 6U + LC_XID_STRING_SIZE - 1U, ".tmp") != 0)
    return 0;
  memcpy(id, leaf + 6U, LC_XID_STRING_SIZE - 1U);
  id[LC_XID_STRING_SIZE - 1U] = '\0';
  return lc_xid_is_valid(id);
}

/* Current publishers acquire their file lock under shared metadata authority.
 * The exclusive probe cannot unlink the file between create and lock. */
static int lc_qgc_temporary_remove(lc_collection_handle *collection,
                                   const char *path, int *changed,
                                   lc_pouch_maintenance_result *out,
                                   lc_error *error) {
  struct stat st;
  int fd, locked, rc;
  rc = lc_collection_metadata_enter(collection, 1, error);
  if (rc != LC_OK)
    return rc;
  fd = -1;
  if (lstat(path, &st) != 0) {
    if (errno != ENOENT)
      rc =
          lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                "failed to inspect collection query temporary");
  } else if (!S_ISREG(st.st_mode)) {
    rc = lc_qgc_unlink(path, changed, out, error);
  } else {
    fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0 && errno != ENOENT)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection query temporary");
    else if (fd >= 0) {
      do {
        locked = flock(fd, LOCK_EX | LOCK_NB);
      } while (locked != 0 && errno == EINTR);
      if (locked == 0)
        rc = lc_qgc_unlink(path, changed, out, error);
      else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (out != NULL)
          ++out->cleanup_pending_count;
      } else
        rc =
            lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                  "failed to probe collection query publisher");
    }
  }
  if (fd >= 0)
    (void)close(fd);
  lc_collection_metadata_leave(collection);
  return rc;
}

static int lc_qgc_epoch(lc_collection_handle *collection, const char *base,
                        const char *identity, int *base_changed,
                        lc_pouch_maintenance_result *out, lc_error *error) {
  char *directory, *path;
  struct stat st;
  struct dirent *entry;
  DIR *dir;
  int obsolete, changed, rc;
  directory = lc_pouch_path_join(&collection->allocator, base, identity);
  if (directory == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection query cleanup path");
  dir = NULL;
  obsolete = changed = 0;
  rc = lc_collection_metadata_enter(collection, 1, error);
  if (rc == LC_OK) {
    rc = lc_collection_refresh_view(collection, error);
    if (rc == LC_OK) {
      obsolete = strcmp(identity, collection->order.query_id) != 0;
      if (lstat(directory, &st) != 0) {
        if (errno != ENOENT)
          rc =
              lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                    "failed to inspect collection query epoch");
      } else if (!S_ISDIR(st.st_mode))
        rc = lc_qgc_unlink(directory, base_changed, out, error);
      else {
        dir = opendir(directory);
        if (dir == NULL)
          rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                     "failed to open collection query epoch");
      }
    }
    lc_collection_metadata_leave(collection);
  }
  /* Recovery never reuses an epoch. Store revalidates identity before creating
   * directories and before publishing, so an obsolete path cannot be revived.
   * Readers of a retired index simply fall back to their pinned state spans. */
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to enumerate collection query artifacts");
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        (!obsolete && !lc_qgc_temporary(entry->d_name)))
      continue;
    path = lc_pouch_path_join(&collection->allocator, directory, entry->d_name);
    if (path == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection query artifact path");
    else if (obsolete)
      rc = lc_qgc_unlink(path, &changed, out, error);
    else
      rc = lc_qgc_temporary_remove(collection, path, &changed, out, error);
    lc_free_with_allocator(&collection->allocator, path);
  }
  if (dir != NULL) {
    if (rc == LC_OK && changed && collection->pouch->durable_sync &&
        fsync(dirfd(dir)) != 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync collection query cleanup");
    (void)closedir(dir);
    if (rc == LC_OK && obsolete) {
      if (rmdir(directory) == 0)
        *base_changed = 1;
      else if (errno == ENOTEMPTY) {
        if (out != NULL)
          ++out->cleanup_pending_count;
      } else if (errno != ENOENT)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to retire collection query epoch");
    }
  }
  lc_free_with_allocator(&collection->allocator, directory);
  return rc;
}

int lc_collection_file_query_cleanup(void *context,
                                     lc_pouch_maintenance_result *out,
                                     lc_error *error) {
  lc_collection_handle *collection = context;
  struct dirent *entry;
  DIR *dir;
  char *base;
  int changed, rc = LC_OK;
  base = lc_pouch_path_join(&collection->allocator, collection->directory,
                            "query");
  dir = NULL;
  changed = 0;
  if (base == NULL)
    rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                               "failed to allocate collection query directory");
  if (rc == LC_OK) {
    dir = opendir(base);
    if (dir == NULL && errno != ENOENT)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to enumerate collection query epochs");
  }
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to read collection query directory");
      break;
    }
    if (lc_xid_is_valid(entry->d_name))
      rc = lc_qgc_epoch(collection, base, entry->d_name, &changed, out, error);
  }
  if (dir != NULL) {
    if (rc == LC_OK && changed && collection->pouch->durable_sync &&
        fsync(dirfd(dir)) != 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync collection query epochs");
    (void)closedir(dir);
  }
  lc_free_with_allocator(&collection->allocator, base);
  return rc;
}

int lc_collection_query_cleanup(lc_collection *self,
                                lc_pouch_maintenance_result *out,
                                lc_error *error) {
  lc_collection_call call, scope;
  lc_collection_guard guard;
  lc_collection_handle *c;
  int rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  c = call.collection;
  rc = lc_collection_root_begin(c->pouch, &scope, &guard, call.error);
  if (rc == LC_OK) {
    rc = c->queries.cleanup(c->queries.context, out, call.error);
    lc_collection_root_end(&scope, &guard);
  }
  lc_collection_call_end(&call);
  return rc;
}
