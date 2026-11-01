#include "lc_collection_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_utf8.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>
#include <zlib.h>

/* Catalog metadata is bounded and contains no application document. Hashing
 * the filename permits every supported UTF-8 name within NAME_MAX. The name
 * itself remains authoritative and detects an accidental digest collision. */
#define LC_CATALOG_BYTES 320U
static const unsigned char lc_catalog_magic[8] = {'L', 'C', 'C', 'A',
                                                  'T', 0,   0,   0};

static int lc_catalog_name_valid(const char *name) {
  lc_utf8_state utf8;
  size_t size;
  if (name == NULL)
    return 0;
  size = strlen(name);
  memset(&utf8, 0, sizeof(utf8));
  return size != 0U && size <= 255U &&
         lc_utf8_push(&utf8, (const unsigned char *)name, size) &&
         utf8.remaining == 0;
}

static int lc_catalog_endpoint(lc_client *self, lc_client_handle **out,
                               lc_error *error) {
  *out = NULL;
  if (self == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection operation requires a client");
  *out = (lc_client_handle *)self;
  if (!(*out)->is_pouch)
    return lc_error_set(error, LC_ERR_UNSUPPORTED, 0L,
                        "endpoint does not support collections", NULL,
                        "collections_unsupported", "lockd");
  return LC_OK;
}

int lc_collection_catalog_name_path(const lc_allocator *allocator,
                                    const char *base, const char *name,
                                    char **out, lc_error *error) {
  unsigned char digest[32];
  static const char hex[] = "0123456789abcdef";
  char leaf[65];
  char *names;
  size_t i;
  int rc;
  *out = NULL;
  rc = lc_collection_hash(name, strlen(name), digest, error);
  if (rc != LC_OK)
    return rc;
  for (i = 0U; i < sizeof(digest); ++i) {
    leaf[i * 2U] = hex[digest[i] >> 4U];
    leaf[i * 2U + 1U] = hex[digest[i] & 15U];
  }
  leaf[64] = '\0';
  names = lc_pouch_path_join(allocator, base, "names");
  if (names != NULL)
    *out = lc_pouch_path_join(allocator, names, leaf);
  lc_free_with_allocator(allocator, names);
  return *out != NULL
             ? LC_OK
             : lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                     "failed to allocate collection name path");
}

int lc_collection_catalog_paths(lc_client_handle *client, const char *ns,
                                const char *name, int create, char **base,
                                char **catalog, lc_error *error) {
  char *namespace_path;
  char *names;
  char *data;
  int rc;
  *base = *catalog = NULL;
  if (ns == NULL || ns[0] == '\0' ||
      (name != NULL && !lc_catalog_name_valid(name)))
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

int lc_collection_catalog_read(const char *path, char id[LC_XID_STRING_SIZE],
                               char name[256], lc_error *error) {
  unsigned char bytes[LC_CATALOG_BYTES + 1U];
  unsigned long expected;
  unsigned long actual;
  size_t total;
  size_t length;
  ssize_t got;
  int fd;
  fd = open(path, O_RDONLY);
  if (fd < 0)
    return lc_collection_failure(
        error, errno == ENOENT ? LC_ERR_INVALID : LC_ERR_TRANSPORT,
        errno == ENOENT ? "collection_not_found" : NULL,
        "failed to open collection name");
  total = 0U;
  while (total < sizeof(bytes)) {
    got = read(fd, bytes + total, sizeof(bytes) - total);
    if (got < 0 && errno == EINTR)
      continue;
    if (got < 0) {
      (void)close(fd);
      return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to read collection name");
    }
    if (got == 0)
      break;
    total += (size_t)got;
  }
  (void)close(fd);
  if (total != LC_CATALOG_BYTES || memcmp(bytes, lc_catalog_magic, 8U) != 0)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection catalog metadata is invalid");
  actual = (unsigned long)bytes[316] | ((unsigned long)bytes[317] << 8U) |
           ((unsigned long)bytes[318] << 16U) |
           ((unsigned long)bytes[319] << 24U);
  expected = (unsigned long)crc32(0L, bytes, 316U);
  length = (size_t)bytes[32];
  if (actual != expected || length == 0U || bytes[28] != 0U ||
      bytes[40U + length] != 0U)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection catalog identity is invalid");
  memcpy(id, bytes + 8U, LC_XID_STRING_SIZE);
  memcpy(name, bytes + 40U, length + 1U);
  if (!lc_catalog_name_valid(name) || strlen(name) != length ||
      strlen(id) != LC_XID_STRING_SIZE - 1U ||
      strspn(id, "0123456789abcdefghijklmnopqrstuv") != LC_XID_STRING_SIZE - 1U)
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection catalog contains an invalid name or ID");
  return LC_OK;
}

int lc_collection_catalog_write(const char *path, const char *id,
                                const char *name, lc_error *error) {
  unsigned char bytes[LC_CATALOG_BYTES];
  unsigned long crc;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, lc_catalog_magic, 8U);
  memcpy(bytes + 8U, id, LC_XID_STRING_SIZE);
  bytes[32] = (unsigned char)strlen(name);
  memcpy(bytes + 40U, name, strlen(name));
  crc = (unsigned long)crc32(0L, bytes, 316U);
  bytes[316] = (unsigned char)crc;
  bytes[317] = (unsigned char)(crc >> 8U);
  bytes[318] = (unsigned char)(crc >> 16U);
  bytes[319] = (unsigned char)(crc >> 24U);
  return lc_pouch_path_write_bytes_file(path, bytes, sizeof(bytes), error);
}

int lc_collection_catalog_bound(lc_collection_handle *collection,
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

static int lc_catalog_open(lc_client *self, const lc_collection_config *config,
                           int create, lc_collection **out, lc_error *error) {
  lc_client_handle *client;
  lc_collection *existing;
  lc_error local;
  lc_collection_call scope;
  lc_collection_guard guard;
  const char *ns;
  char id[LC_XID_STRING_SIZE];
  char name[256];
  char *base;
  char *catalog;
  int fd;
  int found;
  int rc;
  int entered;
  lc_error_init(&local);
  if (error == NULL)
    error = &local;
  lc_error_cleanup(error);
  lc_error_init(error);
  if (out != NULL)
    *out = NULL;
  rc = lc_catalog_endpoint(self, &client, error);
  if (rc == LC_OK &&
      (config == NULL || !lc_catalog_name_valid(config->name) || out == NULL))
    rc = lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection operation requires a valid name and output");
  if (rc != LC_OK) {
    lc_error_cleanup(&local);
    return rc;
  }
  rc = lc_pouch_client_public_namespace(client, config->ns, &ns, error);
  if (rc == LC_OK && !create &&
      (config->has_retention_seconds || config->has_max_entries ||
       config->lease != NULL))
    rc = lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "opening a collection cannot set creation policies or acquire a lease");
  if (rc == LC_OK)
    rc =
        lc_state_retention_validate(config->has_retention_seconds,
                                    config->retention_seconds, ns, NULL, error);
  if (rc != LC_OK) {
    lc_error_cleanup(&local);
    return rc;
  }
  base = catalog = NULL;
  fd = -1;
  lc_client_handle_retain(client);
  rc = lc_collection_root_begin(client->pouch, &scope, &guard, error);
  entered = rc == LC_OK;
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
  if (entered)
    lc_collection_root_end(&scope, &guard);
  lc_client_free(client, base);
  lc_client_free(client, catalog);
  lc_client_handle_release(client);
  lc_error_cleanup(&local);
  return rc;
}

int lc_client_create_collection_method(lc_client *self,
                                       const lc_collection_config *config,
                                       lc_collection **out, lc_error *error) {
  return lc_catalog_open(self, config, 1, out, error);
}

int lc_client_open_collection_method(lc_client *self,
                                     const lc_collection_config *config,
                                     lc_collection **out, lc_error *error) {
  return lc_catalog_open(self, config, 0, out, error);
}

int lc_client_collection_capabilities_method(lc_client *self,
                                             lc_collection_capabilities *out,
                                             lc_error *error) {
  lc_client_handle *client;
  if (self == NULL || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection capabilities require a client and output");
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  if (client->is_pouch) {
    out->collections = out->lql_filter = out->resume = out->checkpoint = 1;
    out->ranges = out->first_n = 1;
    out->count_retention = 1;
    out->append_collection_retention = 1;
    out->append_writer = 1;
    out->append_staged = 1;
    out->leases = out->lease_takeover = out->private_reads = 1;
    out->atomic_edit = 1;
  }
  return LC_OK;
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

int lc_client_list_collections_method(lc_client *self, const char *ns,
                                      lc_collection_visit_fn visitor,
                                      void *context, lc_error *error) {
  lc_client_handle *client;
  lc_catalog_item *items;
  lc_catalog_item *grown;
  lc_collection *collection;
  lc_collection_info info;
  lc_error local;
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
  lc_error_init(&local);
  if (error == NULL)
    error = &local;
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lc_catalog_endpoint(self, &client, error);
  if (rc == LC_OK && visitor == NULL)
    rc = lc_collection_failure(error, LC_ERR_INVALID, NULL,
                               "collection listing requires a visitor");
  if (rc != LC_OK) {
    lc_error_cleanup(&local);
    return rc;
  }
  lc_client_handle_retain(client);
  rc = lc_pouch_client_public_namespace(client, ns, &ns, error);
  if (rc != LC_OK) {
    lc_client_handle_release(client);
    lc_error_cleanup(&local);
    return rc;
  }
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
  lc_client_handle_release(client);
  lc_error_cleanup(&local);
  return rc;
}
