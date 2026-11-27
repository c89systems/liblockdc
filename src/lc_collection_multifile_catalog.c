#include "lc_collection_catalog_record.h"
#include "lc_collection_multifile.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

int lc_collection_catalog_paths(lc_client_handle *client, const char *ns,
                                const char *name, int create, char **base,
                                char **catalog, lc_error *error) {
  char *namespace_path;
  char *names;
  char *data;
  int rc;
  *base = *catalog = NULL;
  if (ns == NULL || ns[0] == '\0' ||
      (name != NULL && !lc_collection_catalog_name_valid(name)))
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection namespace and valid UTF-8 name are required");
  rc = create ? lc_pouch_namespace_ensure_layout(
                    &client->allocator, client->pouch->root_path, ns, error)
              : LC_OK;
  if (rc != LC_OK)
    return rc;
  namespace_path =
      lc_pouch_namespace_path(&client->allocator, client->pouch->root_path, ns);
  if (namespace_path != NULL)
    *base =
        lc_pouch_path_join(&client->allocator, namespace_path, "collections");
  names = *base != NULL ? lc_pouch_path_join(&client->allocator, *base, "names")
                        : NULL;
  data = *base != NULL ? lc_pouch_path_join(&client->allocator, *base, "data")
                       : NULL;
  if (namespace_path == NULL || *base == NULL || names == NULL || data == NULL)
    rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                               "failed to allocate collection catalog paths");
  if (rc == LC_OK && create) {
    rc = lc_pouch_path_ensure_directory(
        *base, "failed to create collection catalog", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(
          names, "failed to create collection names", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(
          data, "failed to create collection partitions", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          *base, "failed to sync collection layout", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          namespace_path, "failed to sync collection namespace", error);
  }
  if (rc == LC_OK && name != NULL) {
    rc = lc_collection_catalog_name_path(&client->allocator, *base, name,
                                         catalog, error);
  }
  lc_client_free(client, namespace_path);
  lc_client_free(client, names);
  lc_client_free(client, data);
  if (rc != LC_OK) {
    lc_client_free(client, *base);
    lc_client_free(client, *catalog);
    *base = *catalog = NULL;
  }
  return rc;
}

int lc_collection_catalog_lock(const char *base, int *fd, lc_error *error) {
  char *path;
  path = lc_pouch_path_join(NULL, base, ".catalog.lock");
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate catalog lock path");
  *fd = open(path, O_RDWR | O_CREAT, 0600);
  lc_free_with_allocator(NULL, path);
  if (*fd < 0) {
    if (errno == ENOENT)
      return lc_collection_failure(error, LC_ERR_INVALID,
                                   "collection_not_found",
                                   "collection does not exist");
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection catalog lock");
  }
  while (flock(*fd, LOCK_EX) != 0) {
    if (errno == EINTR)
      continue;
    (void)close(*fd);
    *fd = -1;
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to lock collection catalog");
  }
  return LC_OK;
}

int lc_collection_catalog_write(const char *path, const char *id,
                                const char *name, lc_error *error) {
  unsigned char bytes[LC_COLLECTION_CATALOG_BYTES];
  int rc = lc_collection_catalog_encode(id, name, bytes, error);
  return rc == LC_OK
             ? lc_pouch_path_write_bytes_file(path, bytes, sizeof(bytes), error)
             : rc;
}

int lc_collection_file_catalog_bound(lc_collection_handle *collection,
                                     lc_error *error) {
  char id[LC_XID_STRING_SIZE];
  char name[256];
  int rc;
  rc = lc_collection_catalog_read(collection->catalog_path, id, name, error);
  if (rc == LC_OK &&
      (strcmp(id, collection->id) != 0 || strcmp(name, collection->name) != 0))
    rc =
        lc_collection_failure(error, LC_ERR_INVALID, "collection_stale",
                              "collection name belongs to another incarnation");
  return rc;
}

int lc_collection_file_catalog_remove(lc_collection_handle *collection,
                                      lc_error *error) {
  char *names;
  int rc;
  if (unlink(collection->catalog_path) != 0 && errno != ENOENT)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to remove collection name");
  names = lc_pouch_path_join(&collection->allocator, collection->catalog_base,
                             "names");
  if (!names)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection catalog path");
  rc = lc_pouch_path_fsync_directory(names, "failed to sync collection drop",
                                     error);
  lc_free_with_allocator(&collection->allocator, names);
  return rc;
}

int lc_collection_multifile_catalog_open(lc_client_handle *client,
                                         const char *ns,
                                         const lc_collection_config *config,
                                         int create, lc_collection **out,
                                         lc_error *error) {
  lc_collection *existing;
  char id[LC_XID_STRING_SIZE], name[256];
  char *base = NULL, *catalog = NULL;
  int fd = -1, found, rc = LC_OK;
  if (rc == LC_OK)
    rc = lc_collection_catalog_paths(client, ns, config->name, create, &base,
                                     &catalog, error);
  if (rc == LC_OK)
    rc = lc_collection_catalog_lock(base, &fd, error);
  if (rc == LC_OK) {
    rc = lc_collection_catalog_read(catalog, id, name, error);
    found = rc == LC_OK;
    if (found && strcmp(name, config->name) != 0)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection catalog digest collision");
    if (found && rc == LC_OK) {
      /* A durable logical drop may precede removal of the old name. Opening
       * its head distinguishes that case from a damaged live partition. */
      existing = NULL;
      rc = lc_collection_open_bound(client, ns, name, id, base, catalog, 0, 0,
                                    0, &existing, error);
      if (rc == LC_ERR_INVALID && error->server_code != NULL &&
          strcmp(error->server_code, "collection_stale") == 0) {
        if (unlink(catalog) != 0 && errno != ENOENT)
          rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                     "failed to remove stale collection name");
        else {
          found = 0;
          lc_error_cleanup(error);
          lc_error_init(error);
          rc = LC_OK;
        }
      } else if (rc == LC_OK && !create)
        *out = existing;
      else if (existing != NULL)
        existing->close(existing);
      if (rc == LC_OK && found && create)
        rc = lc_collection_failure(error, LC_ERR_INVALID, "collection_exists",
                                   "collection name already exists");
    } else if (!found && rc == LC_ERR_INVALID && error->server_code != NULL &&
               strcmp(error->server_code, "collection_not_found") == 0) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    if (rc == LC_OK && !found && !create)
      rc = lc_collection_failure(error, LC_ERR_INVALID, "collection_not_found",
                                 "collection does not exist");
    if (rc == LC_OK && !found && create) {
      rc = lc_xid_new(id, error);
      if (rc == LC_OK)
        rc = lc_collection_open_bound(
            client, ns, config->name, id, base, catalog, 1,
            config->has_retention_seconds
                ? -(lc_pouch_unix_seconds)config->retention_seconds
                : 0,
            config->has_max_entries ? config->max_entries : 0U, out, error);
      if (rc == LC_OK)
        if (config->lease != NULL)
          rc = lc_collection_lease_initialize((*out)->impl, config->lease,
                                              error);
      if (rc == LC_OK)
        rc = lc_collection_catalog_write(catalog, id, config->name, error);
      if (rc != LC_OK && *out != NULL) {
        (*out)->close(*out);
        *out = NULL;
      }
    }
  }
  if (fd >= 0)
    (void)close(fd);
  lc_client_free(client, base);
  lc_client_free(client, catalog);
  return rc;
}

typedef struct lc_catalog_item {
  char name[256];
  char id[LC_XID_STRING_SIZE];
  char leaf[65];
} lc_catalog_item;

static int lc_catalog_compare(const void *a, const void *b) {
  return strcmp(((const lc_catalog_item *)a)->name,
                ((const lc_catalog_item *)b)->name);
}

int lc_collection_multifile_catalog_visit(lc_client_handle *client,
                                          const char *ns,
                                          lc_collection_visit_fn visitor,
                                          void *context, lc_error *error) {
  lc_catalog_item *items;
  lc_catalog_item *grown;
  lc_collection *collection;
  lc_collection_info info;
  lc_collection_call scope;
  lc_collection_guard guard;
  struct dirent *entry;
  DIR *dir;
  char *base;
  char *unused;
  char *names;
  char *path;
  size_t count;
  size_t capacity;
  size_t i;
  int fd;
  int rc;
  int entered;
  base = unused = names = NULL;
  items = NULL;
  count = capacity = 0U;
  fd = -1;
  dir = NULL;
  rc = lc_collection_root_begin(client->pouch, &scope, &guard, error);
  entered = rc == LC_OK;
  if (rc == LC_OK)
    rc =
        lc_collection_catalog_paths(client, ns, NULL, 0, &base, &unused, error);
  if (rc == LC_OK)
    rc = lc_collection_catalog_lock(base, &fd, error);
  if (rc == LC_ERR_INVALID && error->server_code != NULL &&
      strcmp(error->server_code, "collection_not_found") == 0) {
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
  } else if (rc == LC_OK) {
    names = lc_pouch_path_join(&client->allocator, base, "names");
    if (names == NULL)
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection listing path");
    else {
      dir = opendir(names);
      if (dir == NULL)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to enumerate collection names");
    }
    while (rc == LC_OK && dir != NULL) {
      errno = 0;
      entry = readdir(dir);
      if (entry == NULL) {
        if (errno != 0)
          rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                     "failed to enumerate collection catalog");
        break;
      }
      if (strlen(entry->d_name) != 64U ||
          strspn(entry->d_name, "0123456789abcdef") != 64U)
        continue;
      if (count == capacity) {
        size_t next;
        next = capacity == 0U ? 8U : capacity * 2U;
        if (next < capacity || next > (size_t)-1 / sizeof(*items)) {
          rc = LC_ERR_NOMEM;
          break;
        }
        grown = (lc_catalog_item *)lc_client_realloc(client, items,
                                                     next * sizeof(*items));
        if (grown == NULL) {
          rc = LC_ERR_NOMEM;
          break;
        }
        items = grown;
        capacity = next;
      }
      path = lc_pouch_path_join(&client->allocator, names, entry->d_name);
      if (path == NULL) {
        rc = LC_ERR_NOMEM;
        break;
      }
      rc = lc_collection_catalog_read(path, items[count].id, items[count].name,
                                      error);
      lc_client_free(client, path);
      if (rc == LC_OK) {
        memcpy(items[count].leaf, entry->d_name, 65U);
        ++count;
      }
    }
  }
  if (dir != NULL)
    (void)closedir(dir);
  if (fd >= 0)
    (void)close(fd);
  if (entered)
    lc_collection_root_end(&scope, &guard);
  if (rc == LC_ERR_NOMEM)
    rc = lc_collection_failure(error, rc, NULL,
                               "failed to allocate collection listing");
  if (rc == LC_OK && count > 1U)
    qsort(items, count, sizeof(*items), lc_catalog_compare);
  for (i = 0U; rc == LC_OK && i < count; ++i) {
    path = lc_pouch_path_join(&client->allocator, names, items[i].leaf);
    if (path == NULL) {
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection catalog path");
      break;
    }
    collection = NULL;
    rc = lc_collection_open_bound(client, ns, items[i].name, items[i].id, base,
                                  path, 0, 0, 0, &collection, error);
    lc_client_free(client, path);
    if (rc == LC_ERR_INVALID && error->server_code != NULL &&
        strcmp(error->server_code, "collection_stale") == 0) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      continue;
    }
    if (rc == LC_OK)
      rc = collection->describe(collection, &info, error);
    if (rc == LC_OK)
      rc = visitor(context, &info, error);
    lc_collection_close(collection);
  }
  lc_client_free(client, items);
  lc_client_free(client, names);
  lc_client_free(client, base);
  lc_client_free(client, unused);
  return rc;
}
