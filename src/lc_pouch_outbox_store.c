#include "lc_pouch_outbox_store.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct file_store {
  lc_allocator allocator;
  char *directory;
  DIR *cursor;
  int done;
  int state_reclaim;
} file_store;
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message,
                      code == LC_ERR_TRANSPORT ? strerror(errno) : NULL, NULL,
                      "pouch");
}
static int next(void *context, const char **out, lc_error *e) {
  file_store *store = context;
  struct dirent *entry;
  *out = NULL;
  if (store->done)
    return LC_OK;
  if (!store->cursor) {
    store->cursor = opendir(store->directory);
    if (!store->cursor) {
      if (errno != ENOENT)
        return failure(e, LC_ERR_TRANSPORT, "cannot enumerate outbox markers");
      store->done = 1;
      return LC_OK;
    }
  }
  errno = 0;
  entry = readdir(store->cursor);
  if (entry)
    *out = entry->d_name;
  else {
    int saved = errno;
    store->done = 1;
    if (saved)
      return failure(e, LC_ERR_TRANSPORT, "cannot enumerate outbox markers");
  }
  return LC_OK;
}
static int read_marker(void *context, const char *name, const lc_allocator *a,
                       size_t minimum, size_t maximum, unsigned char **out,
                       size_t *length, lc_error *e) {
  file_store *store = context;
  char *path;
  unsigned char *bytes = NULL;
  struct stat status;
  size_t offset = 0U, size = 0U;
  int fd = -1, rc = LC_OK;
  *out = NULL;
  *length = 0U;
  path = lc_pouch_path_join(&store->allocator, store->directory, name);
  if (!path)
    return failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker path");
  if (lstat(path, &status) != 0)
    rc = errno == ENOENT
             ? LC_POUCH_OUTBOX_MARKER_MISSING
             : failure(e, LC_ERR_TRANSPORT, "cannot inspect outbox marker");
  if (rc == LC_OK &&
      (!S_ISREG(status.st_mode) || status.st_size < 0 ||
       (uint64_t)status.st_size < minimum ||
       (uint64_t)status.st_size > maximum ||
       (uint64_t)status.st_size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES))
    rc = failure(e, LC_ERR_PROTOCOL, "invalid outbox marker size or type");
  if (rc == LC_OK) {
    size = (size_t)status.st_size;
    bytes = lc_alloc_with_allocator(a, size ? size : 1U);
    if (!bytes)
      rc = failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker metadata");
  }
  if (rc == LC_OK) {
    fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
      rc = errno == ENOENT
               ? LC_POUCH_OUTBOX_MARKER_MISSING
               : failure(e, LC_ERR_TRANSPORT, "cannot open outbox marker");
  }
  while (rc == LC_OK && offset < size) {
    ssize_t got = read(fd, bytes + offset, size - offset);
    if (got < 0 && errno == EINTR)
      continue;
    if (got <= 0)
      rc = failure(e, got < 0 ? LC_ERR_TRANSPORT : LC_ERR_PROTOCOL,
                   "cannot read complete outbox marker");
    else
      offset += (size_t)got;
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = failure(e, LC_ERR_TRANSPORT, "cannot close outbox marker");
  lc_free_with_allocator(&store->allocator, path);
  if (rc == LC_OK) {
    *out = bytes;
    *length = size;
  } else
    lc_free_with_allocator(a, bytes);
  return rc;
}
#ifdef LC_POUCH_MULTIFILE_RUNTIME
#include "lc_pouch_multifile_outbox.h"
#endif

static int close_store(void *context, lc_error *e) {
  file_store *store = context;
  lc_allocator allocator = store->allocator;
  int rc = LC_OK;
  if (store->cursor && closedir(store->cursor) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, "cannot close outbox marker enumeration");
  lc_free_with_allocator(&allocator, store->directory);
  lc_free_with_allocator(&allocator, store);
  return rc;
}
static const lc_pouch_outbox_store_ops operations = {next, read_marker,
#ifdef LC_POUCH_MULTIFILE_RUNTIME
                                                     write_marker,
                                                     remove_marker,
#else
                                                     NULL, NULL,
#endif
                                                     close_store};
static int marker_store_open(lc_pouch *pouch, const char *ns, int kind,
                             int create, lc_pouch_outbox_store *out,
                             lc_error *e) {
  static const char *const names[] = {
      "outbox-state-reclaim", "outbox-dead-letter-reclaim",
      "outbox-completed-reclaim", "outbox-inbox-reclaim",
      "outbox-command-reclaim"};
  file_store *store;
  char *control = NULL, *root = NULL, *leaf = NULL;
  int rc = LC_OK;
  if (out)
    memset(out, 0, sizeof(*out));
  if (!pouch || !ns || !ns[0] || !out || kind < -1 ||
      kind >= LC_POUCH_RETENTION_KIND_COUNT)
    return failure(e, LC_ERR_INVALID, "invalid outbox marker store");
  if (pouch->storage.ops) {
    if (!pouch->storage.ops->outbox_markers_open)
      return failure(e, LC_ERR_UNSUPPORTED,
                     "storage owner has no outbox markers");
    return pouch->storage.ops->outbox_markers_open(pouch->storage.context, ns,
                                                   kind, create, out, e);
  }
#ifndef LC_POUCH_MULTIFILE_RUNTIME
  if (create)
    return failure(e, LC_ERR_UNSUPPORTED,
                   "legacy outbox markers are read-only");
#endif
  store = lc_calloc_with_allocator(&pouch->allocator, 1U, sizeof(*store));
  if (!store)
    return failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker store");
  store->allocator = pouch->allocator;
  store->state_reclaim = kind == -1;
  control = lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  if (control)
    root = lc_pouch_path_join(&pouch->allocator, control, names[kind + 1]);
  leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  if (root && leaf)
    store->directory = lc_pouch_path_join(&pouch->allocator, root, leaf);
  if (!control || !root || !leaf || !store->directory)
    rc = failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker directory");
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  if (rc == LC_OK && create) {
    rc = lc_pouch_path_ensure_directory(control, "cannot create pouch control",
                                        e);
    if (rc == LC_OK && !store->state_reclaim)
      rc = lc_pouch_path_fsync_directory(pouch->root_path,
                                         "cannot sync pouch control", e);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(root, "cannot create outbox markers",
                                          e);
    if (rc == LC_OK && !store->state_reclaim)
      rc = lc_pouch_path_fsync_directory(control, "cannot sync outbox markers",
                                         e);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(
          store->directory, "cannot create outbox marker namespace", e);
    if (rc == LC_OK && !store->state_reclaim)
      rc = lc_pouch_path_fsync_directory(
          root, "cannot sync outbox marker namespace", e);
  }
#endif
  lc_free_with_allocator(&pouch->allocator, control);
  lc_free_with_allocator(&pouch->allocator, root);
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (rc == LC_OK) {
    out->ops = &operations;
    out->context = store;
  } else
    (void)close_store(store, NULL);
  return rc;
}
int lc_pouch_outbox_store_open(lc_pouch *pouch, const char *ns, int kind,
                               int create, lc_pouch_outbox_store *out,
                               lc_error *e) {
  lc_error local;
  int rc;
#ifndef LC_POUCH_MULTIFILE_RUNTIME
  if (pouch && !pouch->storage.ops) {
    if (out)
      memset(out, 0, sizeof(*out));
    return failure(e, LC_ERR_UNSUPPORTED,
                   "legacy outbox markers are read-only");
  }
#endif
  lc_error_init(&local);
  rc = marker_store_open(pouch, ns, kind, create, out, e ? e : &local);
  lc_error_cleanup(&local);
  return rc;
}

int lc_pouch_outbox_import_source(lc_pouch *pouch, const char *ns, int kind,
                                  lc_pouch_outbox_store *out, lc_error *e) {
  return marker_store_open(pouch, ns, kind, 0, out, e);
}
int lc_pouch_outbox_store_close(lc_pouch_outbox_store *store, int rc,
                                lc_error *e) {
  lc_error ignored;
  int closed;
  if (!store || !store->ops)
    return rc;
  lc_error_init(&ignored);
  closed = store->ops->close(store->context, rc == LC_OK ? e : &ignored);
  memset(store, 0, sizeof(*store));
  lc_error_cleanup(&ignored);
  return rc == LC_OK ? closed : rc;
}
