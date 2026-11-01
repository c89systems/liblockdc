#include "lc_pouch_recode.h"
#include "lc_collection_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int rc_error(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_TRANSPORT, 0, message, strerror(errno),
                      NULL, "pouch");
}

int lc_pouch_recode_remove_tree(const char *path, lc_error *error) {
  struct stat st;
  DIR *dir;
  struct dirent *entry;
  char *child;
  int rc = LC_OK;
  if (lstat(path, &st) != 0)
    return errno == ENOENT
               ? LC_OK
               : rc_error(error, "failed to inspect conversion tree");
  if (!S_ISDIR(st.st_mode))
    return unlink(path) == 0
               ? LC_OK
               : rc_error(error, "failed to remove conversion file");
  dir = opendir(path);
  if (dir == NULL)
    return rc_error(error, "failed to open conversion tree");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = rc_error(error, "failed to read conversion tree");
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    child = lc_pouch_path_join(NULL, path, entry->d_name);
    if (child == NULL)
      rc = LC_ERR_NOMEM;
    else
      rc = lc_pouch_recode_remove_tree(child, error);
    lc_free_with_allocator(NULL, child);
  }
  (void)closedir(dir);
  if (rc == LC_OK && rmdir(path) != 0)
    rc = rc_error(error, "failed to remove conversion directory");
  return rc;
}

int lc_pouch_recode_offset_add(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_u64 before,
                               lc_u64 after, lc_error *error) {
  lc_pouch_recode_offset *next;
  size_t capacity;
  if (file->count == file->capacity) {
    capacity = file->capacity == 0 ? 64 : file->capacity * 2;
    if (capacity < file->capacity || capacity > (size_t)-1 / sizeof(*next))
      return lc_error_set(error, LC_ERR_NOMEM, 0,
                          "conversion offset table exceeds address space", NULL,
                          NULL, "pouch");
    next = lc_realloc_with_allocator(&work->pouch->allocator, file->offsets,
                                     capacity * sizeof(*next));
    if (next == NULL)
      return lc_error_set(error, LC_ERR_NOMEM, 0,
                          "failed to allocate conversion offsets", NULL, NULL,
                          "pouch");
    file->offsets = next;
    file->capacity = capacity;
  }
  file->offsets[file->count].before = before;
  file->offsets[file->count++].after = after;
  return LC_OK;
}

static int rc_compare(const void *a, const void *b) {
  const lc_pouch_recode_file *x = a, *y = b;
  int order = strcmp(x->scope, y->scope);
  return order != 0 ? order : strcmp(x->leaf, y->leaf);
}

int lc_pouch_recode_find(lc_pouch_recode *work, const char *scope,
                         const char *leaf, lc_u64 offset,
                         lc_pouch_recode_file **file, lc_u64 *translated,
                         lc_error *error) {
  lc_pouch_recode_file key, *found;
  size_t first = 0, last, mid;
  memset(&key, 0, sizeof(key));
  key.scope = (char *)scope;
  key.leaf = (char *)leaf;
  found = bsearch(&key, work->files, work->count, sizeof(*found), rc_compare);
  if (found != NULL) {
    last = found->count;
    while (first < last) {
      mid = first + (last - first) / 2;
      if (found->offsets[mid].before < offset)
        first = mid + 1;
      else
        last = mid;
    }
    if (first < found->count && found->offsets[first].before == offset) {
      *file = found;
      *translated = found->offsets[first].after;
      return LC_OK;
    }
  }
  return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "conversion reference has no retained canonical record",
                      leaf, NULL, "pouch");
}

static int rc_copy(lc_pouch_recode *work, const char *source,
                   const char *target, lc_error *error) {
  unsigned char *buffer;
  ssize_t got, written;
  struct stat st;
  size_t offset;
  int in, out, rc = LC_OK;
  buffer = lc_alloc_with_allocator(&work->pouch->allocator, 65536);
  if (buffer == NULL)
    return LC_ERR_NOMEM;
  in = open(source, O_RDONLY | O_CLOEXEC);
  out = in >= 0 ? open(target, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600)
                : -1;
  if (in < 0 || out < 0)
    rc = rc_error(error, "failed to open conversion metadata");
  if (rc == LC_OK && fstat(in, &st) != 0)
    rc = rc_error(error, "failed to inspect conversion metadata mode");
  while (rc == LC_OK) {
    do {
      got = read(in, buffer, 65536);
    } while (got < 0 && errno == EINTR);
    if (got == 0)
      break;
    if (got < 0) {
      rc = rc_error(error, "failed to read conversion metadata");
      break;
    }
    offset = 0;
    while (offset < (size_t)got) {
      do {
        written = write(out, buffer + offset, (size_t)got - offset);
      } while (written < 0 && errno == EINTR);
      if (written <= 0) {
        rc = rc_error(error, "failed to copy conversion metadata");
        break;
      }
      offset += (size_t)written;
    }
  }
  if (rc == LC_OK && fchmod(out, st.st_mode & 0777) != 0)
    rc = rc_error(error, "failed to preserve conversion metadata mode");
  if (rc == LC_OK && fsync(out) != 0)
    rc = rc_error(error, "failed to sync conversion metadata");
  if (in >= 0)
    (void)close(in);
  if (out >= 0 && close(out) != 0 && rc == LC_OK)
    rc = rc_error(error, "failed to close conversion metadata");
  lc_free_with_allocator(&work->pouch->allocator, buffer);
  return rc;
}

static int rc_file(lc_pouch_recode *work, const char *source,
                   const char *target, const char *scope, const char *ns,
                   const char *leaf, const char *id, int active,
                   lc_error *error) {
  lc_pouch_recode_file *file, *next;
  size_t capacity;
  if (work->count == work->capacity) {
    capacity = work->capacity == 0 ? 16 : work->capacity * 2;
    if (capacity < work->capacity || capacity > (size_t)-1 / sizeof(*next))
      return LC_ERR_NOMEM;
    next = lc_realloc_with_allocator(&work->pouch->allocator, work->files,
                                     capacity * sizeof(*next));
    if (next == NULL)
      return LC_ERR_NOMEM;
    work->files = next;
    work->capacity = capacity;
  }
  file = &work->files[work->count++];
  memset(file, 0, sizeof(*file));
  file->source = lc_strdup_with_allocator(&work->pouch->allocator, source);
  file->target = lc_strdup_with_allocator(&work->pouch->allocator, target);
  file->scope = lc_strdup_with_allocator(&work->pouch->allocator, scope);
  file->ns = lc_strdup_with_allocator(&work->pouch->allocator, ns);
  file->leaf = lc_strdup_with_allocator(&work->pouch->allocator, leaf);
  file->active = active;
  if (id != NULL)
    strcpy(file->collection_id, id);
  if (!file->source || !file->target || !file->scope || !file->ns ||
      !file->leaf)
    return LC_ERR_NOMEM;
  return lc_pouch_state_recode_file(work, file, error);
}

/* Only authoritative manifest/head files enter the converted canonical tree;
 * derived indexes and obsolete grace-period inputs need no physical rewrite. */
static int rc_tree(lc_pouch_recode *work, const char *source,
                   const char *target, const char *ns, int kind,
                   lc_error *error);

static int rc_collection(lc_pouch_recode *work, const char *source,
                         const char *target, const char *ns, const char *id,
                         lc_error *error) {
  lc_collection_disk_head head;
  char leaf[64], *from = NULL, *to = NULL;
  struct stat st;
  lc_u64 segment;
  int rc;
  if (lstat(source, &st) != 0)
    return rc_error(error, "failed to inspect collection conversion partition");
  if (!S_ISDIR(st.st_mode))
    return lc_error_set(error, LC_ERR_INVALID, 0,
                        "collection conversion partition is not a directory",
                        source, NULL, "pouch");
  rc = lc_collection_recode_head(work->pouch, source, &head, error);
  if (rc != LC_OK)
    return rc;
  rc = rc_tree(work, source, target, ns, 4, error);
  if (rc == LC_OK && head.dropped)
    return LC_OK;
  for (segment = head.tail_segment; rc == LC_OK && segment <= head.active;
       ++segment) {
    rc = lc_collection_record_leaf(segment, leaf, error);
    if (rc == LC_OK) {
      from = lc_pouch_path_join(&work->pouch->allocator, source, leaf);
      to = lc_pouch_path_join(&work->pouch->allocator, target, leaf);
      rc = from && to ? rc_file(work, from, to, source, ns, leaf, id,
                                segment == head.active, error)
                      : LC_ERR_NOMEM;
      lc_free_with_allocator(&work->pouch->allocator, from);
      lc_free_with_allocator(&work->pouch->allocator, to);
    }
  }
  if (rc == LC_OK && head.snapshot != 0) {
    rc = lc_collection_record_leaf(head.snapshot | LC_COLLECTION_SNAPSHOT_BIT,
                                   leaf, error);
    if (rc == LC_OK) {
      from = lc_pouch_path_join(&work->pouch->allocator, source, leaf);
      to = lc_pouch_path_join(&work->pouch->allocator, target, leaf);
      rc = from && to ? rc_file(work, from, to, source, ns, leaf, id, 0, error)
                      : LC_ERR_NOMEM;
      lc_free_with_allocator(&work->pouch->allocator, from);
      lc_free_with_allocator(&work->pouch->allocator, to);
    }
  }
  return rc;
}

/* Catalog membership is authoritative. Unbound partitions can lack a head
 * after interrupted creation or reclamation; they need no replacement. Walk
 * bindings once rather than searching the catalog separately per partition. */
static int rc_collection_data(lc_pouch_recode *work, const char *source,
                              const char *target, const char *ns,
                              lc_error *error) {
  char id[LC_XID_STRING_SIZE], name[256];
  char *names, *data, *converted, *catalog, *from, *to;
  DIR *dir = NULL;
  struct dirent *entry;
  struct stat st;
  int rc;
  names = lc_pouch_path_join(&work->pouch->allocator, source, "names");
  data = lc_pouch_path_join(&work->pouch->allocator, source, "data");
  converted = lc_pouch_path_join(&work->pouch->allocator, target, "data");
  rc = names && data && converted ? LC_OK : LC_ERR_NOMEM;
  if (rc == LC_OK) {
    if (lstat(data, &st) == 0) {
      if (!S_ISDIR(st.st_mode))
        rc = lc_error_set(error, LC_ERR_INVALID, 0,
                          "collection conversion data is not a directory", data,
                          NULL, "pouch");
    } else if (errno != ENOENT)
      rc = rc_error(error, "failed to inspect collection conversion data");
  }
  if (rc == LC_OK)
    rc = lc_pouch_path_ensure_directory(
        converted, "failed to create converted collection partitions", error);
  if (rc == LC_OK) {
    dir = opendir(names);
    if (dir == NULL && errno != ENOENT)
      rc = rc_error(error, "failed to read collection conversion catalog");
  }
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = rc_error(error,
                      "failed to enumerate collection conversion catalog");
      break;
    }
    if (strlen(entry->d_name) != 64U ||
        strspn(entry->d_name, "0123456789abcdef") != 64U)
      continue;
    catalog = lc_pouch_path_join(&work->pouch->allocator, names, entry->d_name);
    rc = catalog ? lc_collection_catalog_read(catalog, id, name, error)
                 : LC_ERR_NOMEM;
    from = rc == LC_OK ? lc_pouch_path_join(&work->pouch->allocator, data, id)
                       : NULL;
    to = rc == LC_OK
             ? lc_pouch_path_join(&work->pouch->allocator, converted, id)
             : NULL;
    if (rc == LC_OK)
      rc = from && to ? rc_collection(work, from, to, ns, id, error)
                      : LC_ERR_NOMEM;
    lc_free_with_allocator(&work->pouch->allocator, catalog);
    lc_free_with_allocator(&work->pouch->allocator, from);
    lc_free_with_allocator(&work->pouch->allocator, to);
  }
  if (dir != NULL)
    (void)closedir(dir);
  lc_free_with_allocator(&work->pouch->allocator, names);
  lc_free_with_allocator(&work->pouch->allocator, data);
  lc_free_with_allocator(&work->pouch->allocator, converted);
  return rc;
}

static int rc_namespace(lc_pouch_recode *work, const char *source,
                        const char *target, const char *ns, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  char *from_dir = NULL, *to_dir = NULL, *from, *to;
  const char *leaf;
  unsigned long i, obsolete;
  int rc;
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&work->pouch->allocator,
                                        work->pouch->root_path, ns, &manifest,
                                        NULL, NULL, error);
  if (rc == LC_OK)
    rc = rc_tree(work, source, target, ns, 1, error);
  for (i = 0; rc == LC_OK && i < 2; ++i) {
    to_dir = lc_pouch_path_join(&work->pouch->allocator, target,
                                i == 0 ? "segments" : "snapshots");
    rc = to_dir ? lc_pouch_path_ensure_directory(
                      to_dir, "failed to create converted log directory", error)
                : LC_ERR_NOMEM;
    lc_free_with_allocator(&work->pouch->allocator, to_dir);
  }
  for (i = 0; rc == LC_OK && i <= manifest.segment_count; ++i) {
    leaf = i == manifest.segment_count ? manifest.latest_snapshot
                                       : manifest.segment_leaves[i];
    if (leaf == NULL || *leaf == '\0')
      continue;
    if (i < manifest.segment_count) {
      /* Match replay's canonical boundary. Grace-period segments can still
       * reference superseded snapshots that are intentionally not retained. */
      for (obsolete = 0; obsolete < manifest.obsolete_segment_count; ++obsolete)
        if (strcmp(leaf, manifest.obsolete_segments[obsolete]) == 0)
          break;
      if (obsolete < manifest.obsolete_segment_count)
        continue;
    }
    from_dir = lc_pouch_path_join(&work->pouch->allocator, source,
                                  i == manifest.segment_count ? "snapshots"
                                                              : "segments");
    to_dir = lc_pouch_path_join(&work->pouch->allocator, target,
                                i == manifest.segment_count ? "snapshots"
                                                            : "segments");
    if (!from_dir || !to_dir)
      rc = LC_ERR_NOMEM;
    else
      rc = lc_pouch_path_ensure_directory(
          to_dir, "failed to create converted log directory", error);
    from = rc == LC_OK
               ? lc_pouch_path_join(&work->pouch->allocator, from_dir, leaf)
               : NULL;
    to = rc == LC_OK ? lc_pouch_path_join(&work->pouch->allocator, to_dir, leaf)
                     : NULL;
    if (rc == LC_OK)
      rc = from && to ? rc_file(work, from, to, source, ns, leaf, NULL,
                                manifest.active_segment &&
                                    !strcmp(leaf, manifest.active_segment),
                                error)
                      : LC_ERR_NOMEM;
    lc_free_with_allocator(&work->pouch->allocator, from);
    lc_free_with_allocator(&work->pouch->allocator, to);
    lc_free_with_allocator(&work->pouch->allocator, from_dir);
    lc_free_with_allocator(&work->pouch->allocator, to_dir);
  }
  lc_pouch_namespace_manifest_cleanup(&work->pouch->allocator, &manifest);
  return rc;
}

/* kind: namespaces=0, namespace=1, collection catalog=2,
 * collection partition=4, other structural metadata=5. */
static int rc_tree(lc_pouch_recode *work, const char *source,
                   const char *target, const char *ns, int kind,
                   lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  char *from, *to, *decoded;
  int next_kind, rc;
  rc = lc_pouch_path_ensure_directory(
      target, "failed to create conversion directory", error);
  if (rc != LC_OK)
    return rc;
  dir = opendir(source);
  if (!dir)
    return rc_error(error, "failed to read source namespace directory");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = rc_error(error, "failed to enumerate conversion source");
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    if (kind == 2 && !strcmp(entry->d_name, "data"))
      continue;
    if (kind == 1 && (!strcmp(entry->d_name, "index") ||
                      !strcmp(entry->d_name, "segments") ||
                      !strcmp(entry->d_name, "snapshots") ||
                      !strcmp(entry->d_name, "sequence.clean")))
      continue;
    if (kind == 4 &&
        (!strcmp(entry->d_name, "order") || !strcmp(entry->d_name, "ids") ||
         !strcmp(entry->d_name, "clean") || strstr(entry->d_name, ".log") ||
         !strncmp(entry->d_name, "query", 5) ||
         !strncmp(entry->d_name, ".obsolete-", 10)))
      continue;
    from = lc_pouch_path_join(&work->pouch->allocator, source, entry->d_name);
    to = lc_pouch_path_join(&work->pouch->allocator, target, entry->d_name);
    decoded = NULL;
    if (!from || !to)
      rc = LC_ERR_NOMEM;
    else if (lstat(from, &st) != 0)
      rc = rc_error(error, "failed to inspect conversion source");
    else if (S_ISDIR(st.st_mode)) {
      if (kind == 0) {
        decoded =
            lc_pouch_path_unescape_name(&work->pouch->allocator, entry->d_name);
        rc = decoded ? rc_namespace(work, from, to, decoded, error)
                     : LC_ERR_PROTOCOL;
      } else {
        next_kind = kind == 1 && !strcmp(entry->d_name, "collections") ? 2 : 5;
        rc = rc_tree(work, from, to, ns, next_kind, error);
      }
    } else if (S_ISREG(st.st_mode))
      rc = rc_copy(work, from, to, error);
    else
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0,
                       "conversion source contains a non-regular storage entry",
                       entry->d_name, NULL, "pouch");
    lc_free_with_allocator(&work->pouch->allocator, from);
    lc_free_with_allocator(&work->pouch->allocator, to);
    lc_free_with_allocator(&work->pouch->allocator, decoded);
  }
  (void)closedir(dir);
  if (rc == LC_OK && kind == 2)
    rc = rc_collection_data(work, source, target, ns, error);
  return rc;
}

static int rc_sync_tree(const char *source, const char *path, lc_error *error) {
  DIR *dir = opendir(path);
  struct dirent *entry;
  struct stat st;
  char *child, *original;
  int rc = LC_OK;
  if (!dir)
    return rc_error(error, "failed to open completed conversion tree");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = rc_error(error, "failed to read completed conversion tree");
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    child = lc_pouch_path_join(NULL, path, entry->d_name);
    original = lc_pouch_path_join(NULL, source, entry->d_name);
    if (!child || !original)
      rc = LC_ERR_NOMEM;
    else if (lstat(child, &st) != 0)
      rc = rc_error(error, "failed to inspect completed conversion tree");
    else if (S_ISDIR(st.st_mode))
      rc = rc_sync_tree(original, child, error);
    lc_free_with_allocator(NULL, child);
    lc_free_with_allocator(NULL, original);
  }
  (void)closedir(dir);
  if (rc == LC_OK) {
    if (stat(source, &st) == 0) {
      if (chmod(path, st.st_mode & 0777) != 0)
        rc = rc_error(error, "failed to preserve conversion directory mode");
    } else if (errno != ENOENT)
      rc = rc_error(error, "failed to inspect conversion directory mode");
  }
  return rc == LC_OK ? lc_pouch_path_fsync_directory(
                           path, "failed to sync conversion directory", error)
                     : rc;
}

int lc_pouch_recode_build(lc_pouch *pouch, const char *source,
                          const char *target, lc_u64 *input_bytes,
                          lc_u64 *output_bytes, lc_error *error) {
  lc_pouch_recode work;
  lc_pouch_recode_file *file;
  const char *slash;
  char *parent;
  size_t i;
  int rc;
  memset(&work, 0, sizeof(work));
  work.pouch = pouch;
  rc = lc_pouch_recode_remove_tree(target, error);
  /* Copies can have public file modes beneath private source directories.
   * Keep the entire replacement inaccessible to peers until rc_sync_tree has
   * restored descendant modes and finally the staging root's source mode. */
  if (rc == LC_OK && mkdir(target, 0700) != 0)
    rc = rc_error(error, "failed to create private conversion directory");
  if (rc == LC_OK)
    rc = rc_tree(&work, source, target, NULL, 0, error);
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && lc_pouch_recode_test_hook != NULL)
    rc = lc_pouch_recode_test_hook(lc_pouch_recode_test_context,
                                   LC_POUCH_RECODE_COPIED, error);
#endif
  if (rc == LC_OK && work.count > 1)
    qsort(work.files, work.count, sizeof(*work.files), rc_compare);
  for (i = 0; rc == LC_OK && i < work.count; ++i)
    rc = lc_pouch_state_recode_refs(&work, &work.files[i], error);
  for (i = 0; rc == LC_OK && i < work.count; ++i) {
    file = &work.files[i];
    if (!file->collection_id[0] ||
        (i && !strcmp(file->scope, work.files[i - 1].scope)))
      continue;
    slash = strrchr(file->target, '/');
    parent = slash ? lc_alloc_with_allocator(&pouch->allocator,
                                             (size_t)(slash - file->target) + 1)
                   : NULL;
    if (!parent) {
      rc = LC_ERR_NOMEM;
      break;
    }
    memcpy(parent, file->target, (size_t)(slash - file->target));
    parent[slash - file->target] = '\0';
    rc = lc_collection_recode_finish(&work, file->scope, parent,
                                     file->collection_id, error);
    lc_free_with_allocator(&pouch->allocator, parent);
  }
  if (rc == LC_OK)
    rc = rc_sync_tree(source, target, error);
  *input_bytes = work.input_bytes;
  *output_bytes = work.output_bytes;
  for (i = 0; i < work.count; ++i) {
    file = &work.files[i];
    lc_free_with_allocator(&pouch->allocator, file->source);
    lc_free_with_allocator(&pouch->allocator, file->target);
    lc_free_with_allocator(&pouch->allocator, file->scope);
    lc_free_with_allocator(&pouch->allocator, file->ns);
    lc_free_with_allocator(&pouch->allocator, file->leaf);
    lc_free_with_allocator(&pouch->allocator, file->offsets);
  }
  lc_free_with_allocator(&pouch->allocator, work.files);
  if (rc != LC_OK && error && error->code == LC_OK)
    lc_error_set(error, rc, 0, "failed to prepare Pouch compression conversion",
                 NULL, NULL, "pouch");
  return rc;
}
