#include "lc_collection_internal.h"
#include "lc_collection_query.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

static int lc_mc_has_partitions(const char *path, int *found, lc_error *error);

static int lc_mc_hint_paths(lc_pouch *pouch, const char *ns, char **directory,
                            char **path, lc_error *error) {
  char *escaped;
  *path = NULL;
  *directory = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                                  "collection-work");
  escaped = lc_pouch_path_escape_name(&pouch->allocator, ns);
  if (*directory != NULL && escaped != NULL)
    *path = lc_pouch_path_join(&pouch->allocator, *directory, escaped);
  lc_free_with_allocator(&pouch->allocator, escaped);
  if (*directory != NULL && *path != NULL)
    return LC_OK;
  lc_free_with_allocator(&pouch->allocator, *directory);
  lc_free_with_allocator(&pouch->allocator, *path);
  *directory = *path = NULL;
  return lc_collection_failure(
      error, LC_ERR_NOMEM, NULL,
      "failed to allocate collection maintenance hint path");
}

int lc_collection_maintenance_note(lc_pouch *pouch, const char *ns,
                                   lc_error *error) {
  char *directory;
  char *path;
  struct stat st;
  int fd;
  int rc;
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->hint_ensure(
        pouch->storage.context, ns, LC_POUCH_STORAGE_WORK_COLLECTION, error);
  rc = lc_mc_hint_paths(pouch, ns, &directory, &path, error);
  if (rc != LC_OK)
    return rc;
  if (lstat(path, &st) == 0) {
    rc = S_ISREG(st.st_mode)
             ? LC_OK
             : lc_collection_failure(
                   error, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection maintenance hint is not a regular file");
    if (rc == LC_OK) {
      fd = open(path, O_RDONLY | O_CLOEXEC);
      if (fd < 0 || fsync(fd) != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to confirm collection maintenance hint durability");
      if (fd >= 0)
        (void)close(fd);
      if (rc == LC_OK)
        rc = lc_pouch_path_fsync_directory(
            directory, "failed to confirm collection maintenance hint name",
            error);
    }
  } else if (errno != ENOENT)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to inspect collection maintenance hint");
  else {
    rc = lc_pouch_path_ensure_directory(
        directory, "failed to create collection maintenance work directory",
        error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          pouch->root_path, "failed to sync collection work directory", error);
    if (rc == LC_OK) {
      fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
      if (fd < 0 && errno == EEXIST)
        fd = open(path, O_RDONLY | O_CLOEXEC);
      if (fd < 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to create collection namespace work reference");
      else if (fd >= 0) {
        if (fsync(fd) != 0)
          rc = lc_collection_failure(
              error, LC_ERR_TRANSPORT, NULL,
              "failed to sync collection namespace work reference");
        (void)close(fd);
      }
    }
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          directory, "failed to sync collection maintenance hint", error);
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

static int lc_mc_retire_hint(lc_pouch *pouch, const char *ns, const char *base,
                             const char *data, lc_error *error) {
  char *directory;
  char *path;
  lc_collection_call scope;
  lc_collection_guard guard;
  int fd;
  int found;
  int rc;
  fd = -1;
  rc = lc_collection_root_begin(pouch, &scope, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_collection_catalog_lock(base, &fd, error);
  if (rc != LC_OK) {
    lc_collection_root_end(&scope, &guard);
    return rc;
  }
  rc = lc_mc_has_partitions(data, &found, error);
  if (rc == LC_OK && !found) {
    rc = lc_mc_hint_paths(pouch, ns, &directory, &path, error);
    if (rc == LC_OK) {
      if (unlink(path) != 0 && errno != ENOENT)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to retire empty collection namespace work");
      if (rc == LC_OK)
        rc = lc_pouch_path_fsync_directory(
            directory, "failed to sync retired collection namespace work",
            error);
      lc_free_with_allocator(&pouch->allocator, directory);
      lc_free_with_allocator(&pouch->allocator, path);
    }
  }
  (void)close(fd);
  lc_collection_root_end(&scope, &guard);
  return rc;
}

static int lc_mc_missing(const lc_error *error) {
  return error->code == LC_ERR_INVALID && error->server_code != NULL &&
         strcmp(error->server_code, "collection_not_found") == 0;
}

static int lc_mc_tree(lc_pouch *pouch, const char *path, unsigned int depth,
                      lc_error *error) {
  struct stat st;
  struct dirent *entry;
  DIR *dir;
  char *child;
  int rc;
  if (lstat(path, &st) != 0)
    return errno == ENOENT ? LC_OK
                           : lc_collection_failure(
                                 error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect orphaned collection path");
  if (!S_ISDIR(st.st_mode))
    return unlink(path) == 0 || errno == ENOENT
               ? LC_OK
               : lc_collection_failure(
                     error, LC_ERR_TRANSPORT, NULL,
                     "failed to remove orphaned collection file");
  if (depth >= 32U)
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection cleanup directory nesting exceeds its bound");
  dir = opendir(path);
  if (dir == NULL)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to open orphaned collection directory");
  rc = LC_OK;
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to enumerate orphaned collection files");
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    child = lc_pouch_path_join(&pouch->allocator, path, entry->d_name);
    if (child == NULL)
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection cleanup path");
    else
      rc = lc_mc_tree(pouch, child, depth + 1U, error);
    lc_free_with_allocator(&pouch->allocator, child);
    if (rc != LC_OK)
      break;
  }
  (void)closedir(dir);
  if (rc == LC_OK && rmdir(path) != 0 && errno != ENOENT)
    rc =
        lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                              "failed to remove orphaned collection partition");
  return rc;
}

static int lc_mc_try_lock(lc_pouch *pouch, const char *directory,
                          const char *leaf, int *fd, int *busy,
                          lc_error *error) {
  char *path;
  int rc;
  path = lc_pouch_path_join(&pouch->allocator, directory, leaf);
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection reclamation lock path");
  *fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  lc_free_with_allocator(&pouch->allocator, path);
  if (*fd < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection reclamation lock");
  do {
    rc = flock(*fd, LOCK_EX | LOCK_NB);
  } while (rc != 0 && errno == EINTR);
  *busy = rc != 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
  return rc == 0 || *busy
             ? LC_OK
             : lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                     "failed to lock collection reclamation");
}

/* The catalog remains authoritative if a derived partition identity is lost.
 * This slow metadata-only search is confined to that repair/orphan case. */
static int lc_mc_identity(lc_pouch *pouch, const char *base,
                          const char *partition, const char *id, char name[256],
                          int *found, lc_error *error) {
  char read_id[LC_XID_STRING_SIZE];
  char read_name[256];
  char *identity;
  char *names;
  char *path;
  struct dirent *entry;
  DIR *dir;
  int rc;
  int damaged;
  *found = 0;
  identity = lc_pouch_path_join(&pouch->allocator, partition, "identity");
  if (identity == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection identity path");
  rc = lc_collection_catalog_read(identity, read_id, name, error);
  if (rc == LC_OK) {
    if (strcmp(id, read_id) != 0)
      rc =
          lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                "collection partition identity has another ID");
    else
      *found = 1;
    lc_free_with_allocator(&pouch->allocator, identity);
    return rc;
  }
  damaged = rc == LC_ERR_PROTOCOL;
  if (!damaged && !lc_mc_missing(error)) {
    lc_free_with_allocator(&pouch->allocator, identity);
    return rc;
  }
  lc_error_cleanup(error);
  lc_error_init(error);
  names = lc_pouch_path_join(&pouch->allocator, base, "names");
  if (names == NULL) {
    lc_free_with_allocator(&pouch->allocator, identity);
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection catalog repair path");
  }
  dir = opendir(names);
  rc = LC_OK;
  if (dir == NULL)
    rc = lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to inspect collection catalog during identity repair");
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to enumerate collection catalog repair");
      break;
    }
    if (strlen(entry->d_name) != 64U ||
        strspn(entry->d_name, "0123456789abcdef") != 64U)
      continue;
    path = lc_pouch_path_join(&pouch->allocator, names, entry->d_name);
    if (path == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection catalog repair entry");
    else
      rc = lc_collection_catalog_read(path, read_id, read_name, error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc == LC_OK && strcmp(id, read_id) == 0) {
      strcpy(name, read_name);
      *found = 1;
      rc = lc_collection_catalog_write(identity, id, name, error);
      break;
    }
  }
  if (dir != NULL)
    (void)closedir(dir);
  lc_free_with_allocator(&pouch->allocator, names);
  lc_free_with_allocator(&pouch->allocator, identity);
  if (rc == LC_OK && damaged && !*found)
    rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                               "unbound collection identity is corrupt");
  return rc;
}

static int lc_mc_partition(lc_pouch *pouch, const char *ns, const char *base,
                           const char *data, const char *id,
                           const lc_pouch_maintenance_options *options,
                           lc_pouch_maintenance_result *out, lc_error *error) {
  lc_collection_handle view;
  lc_collection_call scope;
  lc_collection_guard guard;
  struct stat identity;
  lc_collection *collection;
  char name[256];
  char read_name[256];
  char read_id[LC_XID_STRING_SIZE];
  char *partition;
  char *catalog;
  char *head;
  char *names;
  int catalog_fd;
  int join_fd;
  int session_fd;
  int found;
  int bound;
  int busy;
  int rc;
  int entered;
  int dropped;
  collection = NULL;
  catalog = head = names = NULL;
  catalog_fd = join_fd = session_fd = -1;
  partition = lc_pouch_path_join(&pouch->allocator, data, id);
  if (partition == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection maintenance partition");
  rc = lc_collection_root_begin(pouch, &scope, &guard, error);
  entered = rc == LC_OK;
  if (rc == LC_OK)
    rc = lc_collection_catalog_lock(base, &catalog_fd, error);
  if (rc == LC_OK && lstat(partition, &identity) != 0) {
    if (errno != ENOENT)
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to inspect collection maintenance partition");
    if (catalog_fd >= 0)
      (void)close(catalog_fd);
    if (entered)
      lc_collection_root_end(&scope, &guard);
    lc_free_with_allocator(&pouch->allocator, partition);
    return rc;
  }
  if (rc == LC_OK && !S_ISDIR(identity.st_mode))
    rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                               "collection partition is not a directory");
  found = bound = 0;
  if (rc == LC_OK)
    rc = lc_mc_identity(pouch, base, partition, id, name, &found, error);
  if (rc == LC_OK && found) {
    rc = lc_collection_catalog_name_path(&pouch->allocator, base, name,
                                         &catalog, error);
    if (rc == LC_OK)
      rc = lc_collection_catalog_read(catalog, read_id, read_name, error);
    if (rc == LC_OK)
      bound = strcmp(read_id, id) == 0 && strcmp(name, read_name) == 0;
    else if (lc_mc_missing(error)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
  }
  if (rc == LC_OK && bound) {
    head = lc_pouch_path_join(&pouch->allocator, partition, "head");
    if (head == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection maintenance head");
    else {
      memset(&view, 0, sizeof(view));
      view.head_path = head;
      view.head_fd = -1;
      view.control = lc_collection_file_control(&view);
      rc = lc_collection_head_read(&view, error);
      if (view.head_fd >= 0)
        (void)close(view.head_fd);
      if (rc == LC_ERR_INVALID && view.head.dropped) {
        lc_error_cleanup(error);
        lc_error_init(error);
        rc = LC_OK;
        if (unlink(catalog) != 0 && errno != ENOENT)
          rc =
              lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                    "failed to finish collection catalog drop");
        names = lc_pouch_path_join(&pouch->allocator, base, "names");
        if (rc == LC_OK && names == NULL)
          rc = lc_collection_failure(
              error, LC_ERR_NOMEM, NULL,
              "failed to allocate collection catalog drop path");
        if (rc == LC_OK)
          rc = lc_pouch_path_fsync_directory(
              names, "failed to sync recovered collection drop", error);
        bound = 0;
      }
    }
  }
  if (rc == LC_OK && bound) {
    rc = lc_collection_storage_open(pouch, NULL, ns, name, id, base, catalog, 0,
                                    0, 0, &collection, error);
    /* Bulk copying runs after catalog authority is released. */
  } else if (rc == LC_OK) {
    busy = 0;
    rc = lc_mc_try_lock(pouch, partition, ".join.lock", &join_fd, &busy, error);
    if (rc == LC_OK && !busy)
      rc = lc_mc_try_lock(pouch, partition, ".session.lock", &session_fd, &busy,
                          error);
    if (rc == LC_OK && busy) {
      if (out != NULL)
        ++out->cleanup_pending_count;
    } else if (rc == LC_OK) {
      rc = lc_mc_tree(pouch, partition, 0U, error);
      if (rc == LC_OK)
        rc = lc_pouch_path_fsync_directory(
            data, "failed to sync collection partition reclamation", error);
      if (rc == LC_OK && out != NULL)
        ++out->cleanup_deleted_count;
    }
  }
  if (session_fd >= 0)
    (void)close(session_fd);
  if (join_fd >= 0)
    (void)close(join_fd);
  if (catalog_fd >= 0)
    (void)close(catalog_fd);
  if (entered)
    lc_collection_root_end(&scope, &guard);
  if (rc == LC_OK && collection != NULL) {
    dropped = 0;
    if (!options->cleanup_only)
      rc = lc_collection_retention_maintain(collection, &dropped, out, error);
    if (rc == LC_OK && !dropped)
      rc = lc_collection_compact(collection, options->force,
                                 options->cleanup_only, out, error);
    if (rc == LC_OK && !dropped)
      rc = lc_collection_query_cleanup(collection, out, error);
    if (rc == LC_OK && !dropped && !options->cleanup_only)
      rc = lc_collection_query_maintain(collection, error);
    if (rc == LC_ERR_INVALID && error->server_code != NULL &&
        strcmp(error->server_code, "collection_stale") == 0) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      if (out != NULL)
        ++out->cleanup_pending_count;
    }
  }
  lc_collection_close(collection);
  lc_free_with_allocator(&pouch->allocator, partition);
  lc_free_with_allocator(&pouch->allocator, catalog);
  lc_free_with_allocator(&pouch->allocator, head);
  lc_free_with_allocator(&pouch->allocator, names);
  return rc;
}

int lc_collection_maintenance_run(lc_pouch *pouch,
                                  const lc_pouch_maintenance_options *options,
                                  lc_pouch_maintenance_result *out,
                                  lc_error *error) {
  char *namespace_path;
  char *base;
  char *data;
  struct dirent *entry;
  DIR *dir;
  lc_error failure;
  lc_error local;
  int rc;
  int first_rc;
  if (pouch == NULL || options == NULL || options->ns == NULL ||
      options->ns[0] == '\0')
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection maintenance requires a root and namespace");
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->collections_run(
        pouch->storage.context, pouch, options, out, error);
  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, options->ns);
  base =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "collections")
          : NULL;
  data =
      base != NULL ? lc_pouch_path_join(&pouch->allocator, base, "data") : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (base == NULL || data == NULL) {
    lc_free_with_allocator(&pouch->allocator, base);
    lc_free_with_allocator(&pouch->allocator, data);
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate namespace collection maintenance path");
  }
  dir = opendir(data);
  if (dir == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_collection_failure(
                   error, LC_ERR_TRANSPORT, NULL,
                   "failed to open namespace collection maintenance");
    lc_free_with_allocator(&pouch->allocator, base);
    lc_free_with_allocator(&pouch->allocator, data);
    return rc;
  }
  lc_error_init(&failure);
  lc_error_init(&local);
  first_rc = LC_OK;
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0 && first_rc == LC_OK)
        first_rc =
            lc_collection_failure(&failure, LC_ERR_TRANSPORT, NULL,
                                  "failed to enumerate collection maintenance");
      break;
    }
    if (!lc_xid_is_valid(entry->d_name))
      continue;
    rc = lc_mc_partition(pouch, options->ns, base, data, entry->d_name, options,
                         out, &local);
    if (rc != LC_OK && first_rc == LC_OK) {
      first_rc = rc;
      failure = local;
      lc_error_init(&local);
    }
    lc_error_cleanup(&local);
    lc_error_init(&local);
  }
  (void)closedir(dir);
  if (first_rc == LC_OK)
    first_rc = lc_mc_retire_hint(pouch, options->ns, base, data, &failure);
  if (first_rc != LC_OK && error != NULL) {
    lc_error_cleanup(error);
    *error = failure;
    lc_error_init(&failure);
  }
  lc_error_cleanup(&local);
  lc_error_cleanup(&failure);
  lc_free_with_allocator(&pouch->allocator, base);
  lc_free_with_allocator(&pouch->allocator, data);
  return first_rc;
}

static int lc_mc_has_partitions(const char *path, int *found, lc_error *error) {
  struct dirent *entry;
  DIR *dir;
  int rc;
  *found = 0;
  dir = opendir(path);
  if (dir == NULL)
    return errno == ENOENT
               ? LC_OK
               : lc_collection_failure(
                     error, LC_ERR_TRANSPORT, NULL,
                     "failed to inspect collection maintenance work");
  rc = LC_OK;
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to enumerate collection maintenance work");
      break;
    }
    if (lc_xid_is_valid(entry->d_name)) {
      *found = 1;
      break;
    }
  }
  (void)closedir(dir);
  return rc;
}

int lc_collection_maintenance_next(lc_pouch *pouch, const char *after,
                                   char **ns, char **cursor, lc_error *error) {
  char *namespaces;
  char *first;
  char *next;
  struct dirent *entry;
  DIR *dir;
  int rc;
  *ns = *cursor = NULL;
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->hint_next(
        pouch->storage.context, LC_POUCH_STORAGE_WORK_COLLECTION, after, ns,
        cursor, error);
  namespaces = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                                  "collection-work");
  if (namespaces == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection namespace discovery path");
  dir = opendir(namespaces);
  if (dir == NULL) {
    rc =
        errno == ENOENT
            ? LC_OK
            : lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                    "failed to discover collection namespaces");
    lc_free_with_allocator(&pouch->allocator, namespaces);
    return rc;
  }
  first = next = NULL;
  rc = LC_OK;
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to enumerate collection namespaces");
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    if (first == NULL || strcmp(entry->d_name, first) < 0) {
      lc_free_with_allocator(&pouch->allocator, first);
      first = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
      if (first == NULL) {
        rc = LC_ERR_NOMEM;
        break;
      }
    }
    if (after != NULL && strcmp(entry->d_name, after) > 0 &&
        (next == NULL || strcmp(entry->d_name, next) < 0)) {
      lc_free_with_allocator(&pouch->allocator, next);
      next = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
      if (next == NULL) {
        rc = LC_ERR_NOMEM;
        break;
      }
    }
  }
  (void)closedir(dir);
  lc_free_with_allocator(&pouch->allocator, namespaces);
  if (rc == LC_OK && (first != NULL || next != NULL)) {
    *cursor = next != NULL ? next : first;
    if (next != NULL)
      next = NULL;
    else
      first = NULL;
    *ns = lc_pouch_path_unescape_name(&pouch->allocator, *cursor);
    if (*ns == NULL)
      rc = LC_ERR_NOMEM;
  }
  lc_free_with_allocator(&pouch->allocator, first);
  lc_free_with_allocator(&pouch->allocator, next);
  if (rc == LC_ERR_NOMEM)
    rc = lc_collection_failure(
        error, rc, NULL, "failed to allocate collection discovery cursor");
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, *ns);
    lc_free_with_allocator(&pouch->allocator, *cursor);
    *ns = *cursor = NULL;
  }
  return rc;
}
