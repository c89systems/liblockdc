#include "lc_pouch_recode.h"
#include "lc_collection_internal.h"
#include "lc_collection_recode.h"
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
  struct remove_frame {
    const char *path;
    DIR *dir;
    struct remove_frame *parent;
  } root, *frame = &root, *next;
  struct stat st;
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
  root.path = path;
  root.parent = NULL;
  root.dir = opendir(path);
  if (root.dir == NULL)
    return rc_error(error, "failed to open conversion tree");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(frame->dir);
    if (entry == NULL) {
      if (errno != 0) {
        rc = rc_error(error, "failed to read conversion tree");
        break;
      }
      (void)closedir(frame->dir);
      frame->dir = NULL;
      if (rmdir(frame->path) != 0) {
        rc = rc_error(error, "failed to remove conversion directory");
        break;
      }
      if (frame == &root)
        break;
      next = frame->parent;
      lc_free_with_allocator(NULL, (void *)frame->path);
      lc_free_with_allocator(NULL, frame);
      frame = next;
      continue;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    child = lc_pouch_path_join(NULL, frame->path, entry->d_name);
    if (child == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate conversion cleanup path", NULL,
                        NULL, "pouch");
      break;
    }
    if (lstat(child, &st) != 0) {
      if (errno != ENOENT)
        rc = rc_error(error, "failed to inspect conversion tree");
    } else if (S_ISDIR(st.st_mode)) {
      next = lc_alloc_with_allocator(NULL, sizeof(*next));
      if (next == NULL)
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate conversion cleanup traversal",
                          NULL, NULL, "pouch");
      else {
        next->dir = opendir(child);
        if (next->dir == NULL) {
          lc_free_with_allocator(NULL, next);
          rc = rc_error(error, "failed to open conversion tree");
        } else {
          next->path = child;
          next->parent = frame;
          frame = next;
          continue;
        }
      }
    } else if (unlink(child) != 0)
      rc = rc_error(error, "failed to remove conversion file");
    lc_free_with_allocator(NULL, child);
  }
  for (;;) {
    if (frame->dir != NULL)
      (void)closedir(frame->dir);
    if (frame == &root)
      break;
    next = frame->parent;
    lc_free_with_allocator(NULL, (void *)frame->path);
    lc_free_with_allocator(NULL, frame);
    frame = next;
  }
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
  memset(&file->offsets[file->count], 0, sizeof(*file->offsets));
  file->offsets[file->count].before = before;
  file->offsets[file->count++].after = after;
  return LC_OK;
}

static int rc_compare(const void *a, const void *b) {
  const lc_pouch_recode_file *x = a, *y = b;
  int order = strcmp(x->scope, y->scope);
  return order != 0 ? order : strcmp(x->leaf, y->leaf);
}

int lc_pouch_recode_lookup(lc_pouch_recode *work, const char *scope,
                           const char *leaf, lc_u64 offset,
                           lc_pouch_recode_file **file,
                           lc_pouch_recode_offset **translated,
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
      *translated = &found->offsets[first];
      return LC_OK;
    }
  }
  return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "conversion reference has no retained canonical record",
                      leaf, NULL, "pouch");
}

int lc_pouch_recode_find(lc_pouch_recode *work, const char *scope,
                         const char *leaf, lc_u64 offset,
                         lc_pouch_recode_file **file, lc_u64 *translated,
                         lc_error *error) {
  lc_pouch_recode_offset *location;
  int rc =
      lc_pouch_recode_lookup(work, scope, leaf, offset, file, &location, error);
  if (rc == LC_OK)
    *translated = location->after;
  return rc;
}
char *lc_pouch_recode_location(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_u64 segment,
                               char leaf[64], lc_error *error) {
  if (!segment) {
    if (strlen(file->leaf) >= 64U)
      return NULL;
    strcpy(leaf, file->leaf);
    return lc_strdup_with_allocator(&work->pouch->allocator, file->target);
  }
  if (lc_collection_record_leaf(segment, leaf, error) != LC_OK)
    return NULL;
  return lc_pouch_path_join(&work->pouch->allocator, file->scope, leaf);
}

static int rc_copy(lc_pouch_recode *work, const char *source,
                   const char *target, int writable, lc_error *error) {
  unsigned char *buffer;
  ssize_t got, written;
  struct stat st;
  size_t offset;
  int in, out, rc = LC_OK;
  if (!strcmp(source, target))
    return LC_OK;
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
  if (rc == LC_OK && fchmod(out, writable ? 0600 : st.st_mode & 0777) != 0)
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
  (void)error;
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
  return LC_OK;
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
      if (rc == LC_OK)
        work->files[work->count - 1U].output_floor = head.active;
      lc_free_with_allocator(&work->pouch->allocator, from);
      lc_free_with_allocator(&work->pouch->allocator, to);
    }
  }
  for (segment = head.snapshot;
       rc == LC_OK && segment != 0 &&
       segment <= head.snapshot + head.snapshot_extra_parts;
       ++segment) {
    rc = lc_collection_record_leaf(segment | LC_COLLECTION_SNAPSHOT_BIT, leaf,
                                   error);
    if (rc == LC_OK) {
      from = lc_pouch_path_join(&work->pouch->allocator, source, leaf);
      to = lc_pouch_path_join(&work->pouch->allocator, target, leaf);
      rc = from && to ? rc_file(work, from, to, source, ns, leaf, id, 0, error)
                      : LC_ERR_NOMEM;
      if (rc == LC_OK)
        work->files[work->count - 1U].output_floor = head.next_snapshot;
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
  struct tree_frame {
    const char *source, *target, *ns;
    int kind;
    DIR *dir;
    struct tree_frame *parent;
  } root, *frame = &root, *next;
  struct dirent *entry;
  struct stat st;
  char *from, *to, *decoded;
  int rc = LC_OK;
  memset(&root, 0, sizeof(root));
  root.source = source;
  root.target = target;
  root.ns = ns;
  root.kind = kind;
  while (rc == LC_OK) {
    if (frame->dir == NULL) {
      rc = lc_pouch_path_ensure_directory(
          frame->target, "failed to create conversion directory", error);
      if (rc != LC_OK)
        break;
      frame->dir = opendir(frame->source);
      if (frame->dir == NULL) {
        rc = rc_error(error, "failed to read source namespace directory");
        break;
      }
    }
    errno = 0;
    entry = readdir(frame->dir);
    if (entry == NULL) {
      if (errno != 0) {
        rc = rc_error(error, "failed to enumerate conversion source");
        break;
      }
      (void)closedir(frame->dir);
      frame->dir = NULL;
      if (frame->kind == 2)
        rc = rc_collection_data(work, frame->source, frame->target, frame->ns,
                                error);
      if (rc != LC_OK || frame == &root)
        break;
      next = frame->parent;
      lc_free_with_allocator(&work->pouch->allocator, (void *)frame->source);
      lc_free_with_allocator(&work->pouch->allocator, (void *)frame->target);
      lc_free_with_allocator(&work->pouch->allocator, frame);
      frame = next;
      continue;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    if (frame->kind == 2 && !strcmp(entry->d_name, "data"))
      continue;
    if (frame->kind == 1 && (!strcmp(entry->d_name, "index") ||
                             !strcmp(entry->d_name, "segments") ||
                             !strcmp(entry->d_name, "snapshots") ||
                             !strcmp(entry->d_name, "sequence.clean")))
      continue;
    if (frame->kind == 4 &&
        (!strcmp(entry->d_name, "order") || !strcmp(entry->d_name, "ids") ||
         !strcmp(entry->d_name, "clean") || strstr(entry->d_name, ".log") ||
         !strncmp(entry->d_name, "query", 5) ||
         !strncmp(entry->d_name, ".obsolete-", 10)))
      continue;
    from = lc_pouch_path_join(&work->pouch->allocator, frame->source,
                              entry->d_name);
    to = lc_pouch_path_join(&work->pouch->allocator, frame->target,
                            entry->d_name);
    decoded = NULL;
    if (!from || !to)
      rc = LC_ERR_NOMEM;
    else if (lstat(from, &st) != 0)
      rc = rc_error(error, "failed to inspect conversion source");
    else if (S_ISDIR(st.st_mode)) {
      if (frame->kind == 0) {
        decoded =
            lc_pouch_path_unescape_name(&work->pouch->allocator, entry->d_name);
        rc = decoded ? rc_namespace(work, from, to, decoded, error)
                     : LC_ERR_PROTOCOL;
      } else {
        next = lc_calloc_with_allocator(&work->pouch->allocator, 1U,
                                        sizeof(*next));
        if (next == NULL)
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate conversion inventory traversal",
                            NULL, NULL, "pouch");
        else {
          next->source = from;
          next->target = to;
          next->ns = frame->ns;
          next->kind =
              frame->kind == 1 && !strcmp(entry->d_name, "collections") ? 2 : 5;
          next->parent = frame;
          frame = next;
          continue;
        }
      }
    } else if (S_ISREG(st.st_mode))
      rc = rc_copy(work, from, to, 0, error);
    else
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0,
                       "conversion source contains a non-regular storage entry",
                       entry->d_name, NULL, "pouch");
    lc_free_with_allocator(&work->pouch->allocator, from);
    lc_free_with_allocator(&work->pouch->allocator, to);
    lc_free_with_allocator(&work->pouch->allocator, decoded);
  }
  for (;;) {
    if (frame->dir != NULL)
      (void)closedir(frame->dir);
    if (frame == &root)
      break;
    next = frame->parent;
    lc_free_with_allocator(&work->pouch->allocator, (void *)frame->source);
    lc_free_with_allocator(&work->pouch->allocator, (void *)frame->target);
    lc_free_with_allocator(&work->pouch->allocator, frame);
    frame = next;
  }
  return rc;
}

/* The private journal contains inventory, reachability and relocation metadata.
 * Ordinary files share one staged payload; collection units write directly to
 * final canonical parts. Durable receipts precede replacement or retirement,
 * so restart never translates already converted offsets a second time. */
int lc_pouch_recode_phase(int phase, lc_error *error) {
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

int lc_pouch_recode_put64(FILE *fp, lc_u64 value) {
  unsigned char bytes[8];
  unsigned int i;
  for (i = 0; i < 8; ++i) {
    bytes[7 - i] = (unsigned char)value;
    value >>= 8;
  }
  return fwrite(bytes, 1, 8, fp) == 8;
}

int lc_pouch_recode_get64(FILE *fp, lc_u64 *value) {
  unsigned char bytes[8];
  unsigned int i;
  if (fread(bytes, 1, 8, fp) != 8)
    return 0;
  *value = 0;
  for (i = 0; i < 8; ++i)
    *value = (*value << 8) | bytes[i];
  return 1;
}

static int rc_put_string(FILE *fp, const char *text) {
  size_t length = strlen(text);
  return lc_pouch_recode_put64(fp, (lc_u64)length) &&
         fwrite(text, 1, length, fp) == length;
}

static int rc_get_string(lc_pouch_recode *work, FILE *fp, char **out, int *rc) {
  lc_u64 length;
  if (!lc_pouch_recode_get64(fp, &length) || length > 1048576)
    return 0;
  *out = lc_alloc_with_allocator(&work->pouch->allocator, (size_t)length + 1);
  if (!*out) {
    *rc = LC_ERR_NOMEM;
    return 0;
  }
  if (fread(*out, 1, (size_t)length, fp) != length ||
      memchr(*out, 0, (size_t)length))
    return 0;
  (*out)[length] = 0;
  return 1;
}

int lc_pouch_recode_journal_error(lc_error *error) {
  return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "invalid compression conversion journal", NULL, NULL,
                      "pouch");
}

int lc_pouch_recode_commit(FILE *fp, const char *tmp, const char *path,
                           const char *directory, int valid, lc_error *error) {
  int rc =
      valid ? LC_OK : rc_error(error, "failed to write conversion journal");
  if (rc == LC_OK && (fflush(fp) != 0 || fsync(fileno(fp)) != 0))
    rc = rc_error(error, "failed to sync conversion journal");
  if (fclose(fp) != 0 && rc == LC_OK)
    rc = rc_error(error, "failed to close conversion journal");
  if (rc == LC_OK && rename(tmp, path) != 0)
    rc = rc_error(error, "failed to publish conversion journal");
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync conversion journal directory", error);
  return rc;
}

FILE *lc_pouch_recode_create(const char *path) {
  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  FILE *fp = fd >= 0 ? fdopen(fd, "wb") : NULL;
  if (!fp && fd >= 0)
    (void)close(fd);
  return fp;
}

static int rc_inventory(lc_pouch_recode *work, const char *source,
                        const char *directory, lc_error *error) {
  char *path = lc_pouch_path_join(&work->pouch->allocator, directory, "plan");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  char *relative = NULL, *scope = NULL, *ns = NULL, *id = NULL;
  lc_pouch_recode_file *file;
  lc_u64 count, active, floor;
  FILE *fp;
  size_t i, base = strlen(source) + 1;
  int valid, rc = LC_OK;
  if (!path || !tmp) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  fp = fopen(path, "rb");
  if (fp) {
    valid = lc_pouch_recode_get64(fp, &count) &&
            count <= (size_t)-1 / sizeof(*work->files);
    for (i = 0; valid && i < count; ++i) {
      valid = rc_get_string(work, fp, &relative, &rc) &&
              rc_get_string(work, fp, &scope, &rc) &&
              rc_get_string(work, fp, &ns, &rc) &&
              rc_get_string(work, fp, &id, &rc) &&
              lc_pouch_recode_get64(fp, &active) &&
              lc_pouch_recode_get64(fp, &floor) && active <= 1 &&
              floor < LC_COLLECTION_SNAPSHOT_BIT &&
              strlen(id) < LC_XID_STRING_SIZE;
      if (valid) {
        char *from =
            lc_pouch_path_join(&work->pouch->allocator, source, relative);
        char *owner =
            lc_pouch_path_join(&work->pouch->allocator, source, scope);
        const char *leaf = strrchr(relative, '/');
        rc = from && owner && leaf ? rc_file(work, from, from, owner, ns,
                                             leaf + 1, id, (int)active, error)
                                   : LC_ERR_NOMEM;
        lc_free_with_allocator(&work->pouch->allocator, from);
        lc_free_with_allocator(&work->pouch->allocator, owner);
        valid = rc == LC_OK;
        if (valid)
          work->files[work->count - 1U].output_floor = floor;
      }
      lc_free_with_allocator(&work->pouch->allocator, relative);
      relative = NULL;
      lc_free_with_allocator(&work->pouch->allocator, scope);
      scope = NULL;
      lc_free_with_allocator(&work->pouch->allocator, ns);
      ns = NULL;
      lc_free_with_allocator(&work->pouch->allocator, id);
      id = NULL;
    }
    if (!valid || fgetc(fp) != EOF || ferror(fp))
      rc = rc != LC_OK ? rc : lc_pouch_recode_journal_error(error);
    (void)fclose(fp);
    goto done;
  }
  if (errno != ENOENT) {
    rc = rc_error(error, "failed to read conversion inventory");
    goto done;
  }
  /* No canonical file is modified until the complete inventory is durable. */
  rc = rc_tree(work, source, source, NULL, 0, error);
  if (rc == LC_OK && work->count > 1)
    qsort(work->files, work->count, sizeof(*work->files), rc_compare);
  if (rc != LC_OK)
    goto done;
  fp = lc_pouch_recode_create(tmp);
  if (!fp) {
    rc = rc_error(error, "failed to create conversion inventory");
    goto done;
  }
  valid = lc_pouch_recode_put64(fp, (lc_u64)work->count);
  for (i = 0; valid && i < work->count; ++i) {
    file = &work->files[i];
    valid = rc_put_string(fp, file->source + base) &&
            rc_put_string(fp, file->scope + base) &&
            rc_put_string(fp, file->ns) &&
            rc_put_string(fp, file->collection_id) &&
            lc_pouch_recode_put64(fp, (lc_u64)file->active) &&
            lc_pouch_recode_put64(fp, file->output_floor);
  }
  rc = lc_pouch_recode_commit(fp, tmp, path, directory, valid, error);
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

char *lc_pouch_recode_progress(lc_pouch_recode *work, const char *directory,
                               size_t index, const char *suffix) {
  char leaf[64];
  (void)snprintf(leaf, sizeof(leaf), "%lu.%s", (unsigned long)index, suffix);
  return lc_pouch_path_join(&work->pouch->allocator, directory, leaf);
}

static int rc_install(const char *directory, const char *payload,
                      const char *source, lc_u64 before, lc_u64 after,
                      int phase, lc_error *error) {
  struct stat st, staged;
  char *parent;
  int rc = LC_OK;
  if (stat(source, &st) != 0)
    return rc_error(error, "failed to inspect conversion segment");
  if ((lc_u64)st.st_ino != after) {
    if ((lc_u64)st.st_ino != before || stat(payload, &staged) != 0 ||
        (lc_u64)staged.st_ino != after)
      return lc_pouch_recode_journal_error(error);
    if (rename(payload, source) != 0)
      return rc_error(error, "failed to install converted segment");
    rc = lc_pouch_recode_phase(phase, error);
    if (rc != LC_OK)
      return rc;
  }
  /* Also sync after a recovered rename: it may have reached either directory
   * before power loss, even when the current in-memory lookup sees `after`. */
  parent = lc_strdup_with_allocator(NULL, source);
  if (parent && strrchr(parent, '/'))
    *strrchr(parent, '/') = 0;
  rc = parent ? lc_pouch_path_fsync_directory(
                    parent, "failed to sync converted segment directory", error)
              : LC_ERR_NOMEM;
  lc_free_with_allocator(NULL, parent);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync conversion staging directory", error);
  return rc;
}

static int rc_segment(lc_pouch_recode *work, size_t index,
                      const char *directory, const char *payload,
                      lc_error *error) {
  lc_pouch_recode_file *file = &work->files[index];
  char *path = lc_pouch_recode_progress(work, directory, index, "map");
  char *refs = lc_pouch_recode_progress(work, directory, index, "refs");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  char *target;
  struct stat source_st, target_st, journal_st;
  FILE *fp;
  lc_u64 before = 0, after = 0, count = 0, input = 0, output = 0, has_refs = 0;
  size_t i;
  int valid, rc = LC_OK;
  if (!path || !refs || !tmp) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  fp = fopen(path, "rb");
  if (fp) {
    valid = lc_pouch_recode_get64(fp, &before) &&
            lc_pouch_recode_get64(fp, &after) &&
            lc_pouch_recode_get64(fp, &input) &&
            lc_pouch_recode_get64(fp, &output) &&
            lc_pouch_recode_get64(fp, &has_refs) && has_refs <= 1 &&
            lc_pouch_recode_get64(fp, &count) &&
            fstat(fileno(fp), &journal_st) == 0 && journal_st.st_size >= 48 &&
            count == (lc_u64)(journal_st.st_size - 48) / 56 &&
            (journal_st.st_size - 48) % 56 == 0 &&
            count <= (size_t)-1 / sizeof(*file->offsets);
    for (i = 0; valid && i < count; ++i) {
      lc_u64 old_offset, new_offset;
      valid = lc_pouch_recode_get64(fp, &old_offset) &&
              lc_pouch_recode_get64(fp, &new_offset);
      if (valid) {
        rc = lc_pouch_recode_offset_add(work, file, old_offset, new_offset,
                                        error);
        valid =
            rc == LC_OK &&
            lc_pouch_recode_get64(fp,
                                  &file->offsets[file->count - 1U].segment) &&
            lc_pouch_recode_get64(fp,
                                  &file->offsets[file->count - 1U].stored) &&
            lc_pouch_recode_get64(fp, &file->offsets[file->count - 1U].units) &&
            lc_pouch_recode_get64(
                fp, &file->offsets[file->count - 1U].first_segment) &&
            lc_pouch_recode_get64(
                fp, &file->offsets[file->count - 1U].first_offset);
      }
    }
    if (!valid || fgetc(fp) != EOF || ferror(fp))
      rc = rc != LC_OK ? rc : lc_pouch_recode_journal_error(error);
    (void)fclose(fp);
    if (rc == LC_OK) {
      file->has_refs = (int)has_refs;
      work->input_bytes += input;
      work->output_bytes += output;
    }
    /* A reference receipt supersedes the first replacement's inode. Its
     * installation is completed by rc_references before translating anything.
     */
    if (rc == LC_OK && lstat(refs, &journal_st) != 0) {
      if (errno != ENOENT)
        rc = rc_error(error, "failed to inspect reference progress");
      else
        rc = rc_install(directory, payload, file->source, before, after,
                        LC_POUCH_RECODE_SEGMENT_RENAMED, error);
    }
    goto done;
  }
  if (errno != ENOENT) {
    rc = rc_error(error, "failed to read conversion offset map");
    goto done;
  }
  if (unlink(payload) != 0 && errno != ENOENT) {
    rc = rc_error(error, "failed to discard unfinished segment");
    goto done;
  }
  if (stat(file->source, &source_st) != 0) {
    rc = rc_error(error, "failed to inspect original segment");
    goto done;
  }
  input = work->input_bytes;
  output = work->output_bytes;
  target = file->target;
  file->target = (char *)payload;
  rc = lc_pouch_state_recode_file(work, file, error);
  file->target = target;
  if (rc != LC_OK)
    goto done;
  if (stat(payload, &target_st) != 0) {
    rc = rc_error(error, "failed to inspect converted segment");
    goto done;
  }
  before = (lc_u64)source_st.st_ino;
  after = (lc_u64)target_st.st_ino;
  fp = lc_pouch_recode_create(tmp);
  if (!fp) {
    rc = rc_error(error, "failed to create conversion offset map");
    goto done;
  }
  valid = lc_pouch_recode_put64(fp, before) &&
          lc_pouch_recode_put64(fp, after) &&
          lc_pouch_recode_put64(fp, work->input_bytes - input) &&
          lc_pouch_recode_put64(fp, work->output_bytes - output) &&
          lc_pouch_recode_put64(fp, (lc_u64)file->has_refs) &&
          lc_pouch_recode_put64(fp, (lc_u64)file->count);
  for (i = 0; valid && i < file->count; ++i)
    valid = lc_pouch_recode_put64(fp, file->offsets[i].before) &&
            lc_pouch_recode_put64(fp, file->offsets[i].after) &&
            lc_pouch_recode_put64(fp, file->offsets[i].segment) &&
            lc_pouch_recode_put64(fp, file->offsets[i].stored) &&
            lc_pouch_recode_put64(fp, file->offsets[i].units) &&
            lc_pouch_recode_put64(fp, file->offsets[i].first_segment) &&
            lc_pouch_recode_put64(fp, file->offsets[i].first_offset);
  rc = lc_pouch_recode_commit(fp, tmp, path, directory, valid, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_SEGMENT_READY, error);
  if (rc == LC_OK)
    rc = rc_install(directory, payload, file->source, before, after,
                    LC_POUCH_RECODE_SEGMENT_RENAMED, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_SEGMENT_INSTALLED, error);
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, refs);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

static int rc_references(lc_pouch_recode *work, size_t index,
                         const char *directory, const char *payload,
                         lc_error *error) {
  lc_pouch_recode_file *file = &work->files[index];
  char *path = lc_pouch_recode_progress(work, directory, index, "refs");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  char *target;
  struct stat source_st, target_st;
  lc_u64 before = 0, after = 0;
  FILE *fp;
  int valid, rc = LC_OK;
  if (!file->has_refs)
    goto done;
  if (!path || !tmp) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  fp = fopen(path, "rb");
  if (fp) {
    valid = lc_pouch_recode_get64(fp, &before) &&
            lc_pouch_recode_get64(fp, &after) && fgetc(fp) == EOF &&
            !ferror(fp);
    (void)fclose(fp);
    rc = valid ? rc_install(directory, payload, file->source, before, after,
                            LC_POUCH_RECODE_REFS_RENAMED, error)
               : lc_pouch_recode_journal_error(error);
    goto done;
  }
  if (errno != ENOENT) {
    rc = rc_error(error, "failed to read reference progress");
    goto done;
  }
  if (unlink(payload) != 0 && errno != ENOENT) {
    rc = rc_error(error, "failed to discard unfinished reference rewrite");
    goto done;
  }
  if (stat(file->source, &source_st) != 0) {
    rc = rc_error(error, "failed to inspect reference segment");
    goto done;
  }
  /* The private scratch copy must be writable even for sealed read-only
   * segments. recode_refs restores the original mode before publication. */
  rc = rc_copy(work, file->source, payload, 1, error);
  target = file->target;
  file->target = (char *)payload;
  if (rc == LC_OK)
    rc = lc_pouch_state_recode_refs(work, file, error);
  file->target = target;
  if (rc != LC_OK)
    goto done;
  if (stat(payload, &target_st) != 0) {
    rc = rc_error(error, "failed to inspect rewritten reference segment");
    goto done;
  }
  before = (lc_u64)source_st.st_ino;
  after = (lc_u64)target_st.st_ino;
  fp = lc_pouch_recode_create(tmp);
  if (!fp) {
    rc = rc_error(error, "failed to create reference receipt");
    goto done;
  }
  valid = lc_pouch_recode_put64(fp, before) && lc_pouch_recode_put64(fp, after);
  rc = lc_pouch_recode_commit(fp, tmp, path, directory, valid, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_REFS_READY, error);
  if (rc == LC_OK)
    rc = rc_install(directory, payload, file->source, before, after,
                    LC_POUCH_RECODE_REFS_RENAMED, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_REFS_INSTALLED, error);
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

static int rc_query_id(lc_pouch_recode *work, const char *directory,
                       size_t index, const char *scope,
                       char id[LC_XID_STRING_SIZE], lc_error *error) {
  char *saved = lc_pouch_recode_progress(work, directory, index, "query-id");
  char *path = lc_pouch_path_join(&work->pouch->allocator, scope, "order");
  lc_collection_order order;
  FILE *fp;
  lc_error ignored;
  int rc = LC_OK;
  memset(id, 0, LC_XID_STRING_SIZE);
  if (!saved || !path) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  fp = fopen(saved, "rb");
  if (fp) {
    if (fread(id, 1, LC_XID_STRING_SIZE, fp) != LC_XID_STRING_SIZE ||
        fgetc(fp) != EOF || ferror(fp) || id[LC_XID_STRING_SIZE - 1] ||
        (id[0] && !lc_xid_is_valid(id)))
      rc = lc_pouch_recode_journal_error(error);
    (void)fclose(fp);
    goto done;
  }
  if (errno != ENOENT) {
    rc = rc_error(error, "failed to read saved collection query identity");
    goto done;
  }
  memset(&order, 0, sizeof(order));
  order.fd = -1;
  lc_error_init(&ignored);
  if (lc_collection_order_open(path, 0, &order, &ignored) == LC_OK)
    strcpy(id, order.query_id);
  lc_collection_order_close(&order);
  lc_error_cleanup(&ignored);
  rc = lc_pouch_path_write_bytes_file_mode(saved, id, LC_XID_STRING_SIZE, 0600,
                                           error);
done:
  lc_free_with_allocator(&work->pouch->allocator, saved);
  lc_free_with_allocator(&work->pouch->allocator, path);
  return rc;
}

static int rc_reset_projection(lc_pouch_recode *work, const char *scope,
                               lc_error *error) {
  DIR *dir = opendir(scope);
  struct dirent *entry;
  char *path;
  int rc =
      dir ? LC_OK
          : rc_error(error, "failed to inspect converted collection indexes");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc =
            rc_error(error, "failed to enumerate converted collection indexes");
      break;
    }
    if (strcmp(entry->d_name, "order") && strcmp(entry->d_name, "ids") &&
        strcmp(entry->d_name, "clean") && strcmp(entry->d_name, "query") &&
        strncmp(entry->d_name, "order.rebuild-", 14) &&
        strncmp(entry->d_name, "ids.rebuild-", 12))
      continue;
    path = lc_pouch_path_join(&work->pouch->allocator, scope, entry->d_name);
    rc = path ? lc_pouch_recode_remove_tree(path, error) : LC_ERR_NOMEM;
    lc_free_with_allocator(&work->pouch->allocator, path);
  }
  if (dir)
    (void)closedir(dir);
  return rc == LC_OK ? lc_pouch_path_fsync_directory(
                           scope, "failed to sync converted index reset", error)
                     : rc;
}

static int rc_heads(lc_pouch_recode *work, const char *directory,
                    lc_error *error) {
  lc_pouch_recode_file *file;
  struct stat st;
  size_t i;
  char *saved, *head, *tmp;
  char query_id[LC_XID_STRING_SIZE];
  int rc = LC_OK;
  for (i = 0; rc == LC_OK && i < work->count; ++i) {
    file = &work->files[i];
    if (!file->collection_id[0] ||
        (i && !strcmp(file->scope, work->files[i - 1].scope)))
      continue;
    rc = rc_query_id(work, directory, i, file->scope, query_id, error);
    if (rc != LC_OK)
      break;
    saved = lc_pouch_recode_progress(work, directory, i, "head");
    head = lc_pouch_path_join(&work->pouch->allocator, file->scope, "head");
    tmp = lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
    if (!saved || !head || !tmp)
      rc = LC_ERR_NOMEM;
    else if (stat(saved, &st) != 0) {
      if (errno != ENOENT)
        rc = rc_error(error, "failed to inspect original collection head");
      else {
        if (unlink(tmp) != 0 && errno != ENOENT)
          rc = rc_error(error, "failed to discard unfinished head backup");
        if (rc == LC_OK)
          rc = rc_copy(work, head, tmp, 0, error);
        if (rc == LC_OK && rename(tmp, saved) != 0)
          rc = rc_error(error, "failed to save original collection head");
        if (rc == LC_OK)
          rc = lc_pouch_path_fsync_directory(
              directory, "failed to sync saved collection head", error);
      }
    }
    if (rc == LC_OK)
      rc = rc_reset_projection(work, file->scope, error);
    /* Always restart head/index publication from the original small head.
     * Reapplying the offset translation to an already translated head is wrong.
     */
    if (rc == LC_OK) {
      if (unlink(tmp) != 0 && errno != ENOENT)
        rc = rc_error(error, "failed to discard unfinished head restore");
      if (rc == LC_OK)
        rc = rc_copy(work, saved, tmp, 0, error);
      if (rc == LC_OK && rename(tmp, head) != 0)
        rc = rc_error(error, "failed to restore original collection head");
      if (rc == LC_OK)
        rc = lc_collection_migrate_head(work->pouch, file->scope, error);
      if (rc == LC_OK)
        rc = lc_collection_recode_finish(work, file->scope, file->scope,
                                         file->collection_id, query_id, error);
      if (rc == LC_OK)
        rc = lc_pouch_recode_phase(LC_POUCH_RECODE_HEAD, error);
    }
    lc_free_with_allocator(&work->pouch->allocator, saved);
    lc_free_with_allocator(&work->pouch->allocator, head);
    lc_free_with_allocator(&work->pouch->allocator, tmp);
  }
  return rc;
}

/* Derived offsets must not survive the physical rewrite. Reclaim only files
 * already retired by the authoritative manifest; no live record is deleted. */
static int rc_cleanup(lc_pouch_recode *work, const char *source,
                      lc_error *error) {
  DIR *dir = opendir(source);
  struct dirent *entry;
  lc_pouch_namespace_manifest manifest;
  char *ns, *base, *path, *logs;
  unsigned long i;
  size_t j;
  int rc =
      dir ? LC_OK : rc_error(error, "failed to open conversion cleanup root");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = rc_error(error, "failed to enumerate conversion cleanup");
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    ns = lc_pouch_path_unescape_name(&work->pouch->allocator, entry->d_name);
    memset(&manifest, 0, sizeof(manifest));
    rc = ns ? lc_pouch_namespace_manifest_open(&work->pouch->allocator,
                                               work->pouch->root_path, ns,
                                               &manifest, NULL, NULL, error)
            : LC_ERR_NOMEM;
    base = manifest.namespace_path;
    if (rc == LC_OK) {
      const char *derived[] = {"index", "sequence.clean"};
      for (j = 0; rc == LC_OK && j < 2; ++j) {
        path = lc_pouch_path_join(&work->pouch->allocator, base, derived[j]);
        rc = path ? lc_pouch_recode_remove_tree(path, error) : LC_ERR_NOMEM;
        lc_free_with_allocator(&work->pouch->allocator, path);
      }
    }
    for (j = 0; rc == LC_OK && j < 2; ++j) {
      unsigned long count = j ? manifest.obsolete_snapshot_count
                              : manifest.obsolete_segment_count;
      logs = lc_pouch_path_join(&work->pouch->allocator, base,
                                j ? "snapshots" : "segments");
      if (!logs)
        rc = LC_ERR_NOMEM;
      for (i = 0; rc == LC_OK && i < count; ++i) {
        path = lc_pouch_path_join(&work->pouch->allocator, logs,
                                  j ? manifest.obsolete_snapshots[i]
                                    : manifest.obsolete_segments[i]);
        rc = path ? lc_pouch_recode_remove_tree(path, error) : LC_ERR_NOMEM;
        lc_free_with_allocator(&work->pouch->allocator, path);
      }
      if (rc == LC_OK)
        rc = lc_pouch_path_fsync_directory(
            logs, "failed to sync obsolete conversion cleanup", error);
      lc_free_with_allocator(&work->pouch->allocator, logs);
    }
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          base, "failed to sync derived index invalidation", error);
    lc_pouch_namespace_manifest_cleanup(&work->pouch->allocator, &manifest);
    lc_free_with_allocator(&work->pouch->allocator, ns);
  }
  if (dir)
    (void)closedir(dir);
  return rc;
}

int lc_pouch_recode_build(lc_pouch *pouch, const char *source,
                          const char *directory, lc_u64 *input_bytes,
                          lc_u64 *output_bytes, lc_error *error) {
  lc_pouch_recode work;
  lc_pouch_recode_file *file;
  struct stat st;
  char *payload;
  size_t i;
  int rc = LC_OK;
  memset(&work, 0, sizeof(work));
  work.pouch = pouch;
  if (mkdir(directory, 0700) != 0 && errno != EEXIST)
    rc = rc_error(error, "failed to create private conversion journal");
  if (rc == LC_OK && (lstat(directory, &st) != 0 || !S_ISDIR(st.st_mode) ||
                      chmod(directory, 0700) != 0))
    rc = rc_error(error, "failed to secure conversion journal");
  payload = lc_pouch_path_join(&pouch->allocator, directory, "payload");
  if (rc == LC_OK && !payload)
    rc = LC_ERR_NOMEM;
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        pouch->root_path, "failed to sync conversion journal creation", error);
  /* A previous publication may be visible after its directory sync failed.
   * Make all recovered receipts/backups durable before replacing canonical
   * files or discarding the originals they describe. */
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync recovered conversion journal", error);
  if (rc == LC_OK)
    rc = rc_inventory(&work, source, directory, error);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_COPIED, error);
  if (rc == LC_OK)
    rc = lc_collection_recode_references(&work, directory, error);
  for (i = work.count; rc == LC_OK && i > 0; --i)
    if (!work.files[i - 1U].collection_id[0])
      rc = rc_segment(&work, i - 1U, directory, payload, error);
  /* A metadata alias can reference the snapshot body. Within each class,
   * physical children precede their unit indexes and publication records. */
  for (i = 0; rc == LC_OK && i < work.count; ++i)
    if (work.files[i].collection_id[0] &&
        !strncmp(work.files[i].leaf, "snapshot-", 9))
      rc = lc_collection_recode_segment(&work, i, directory, error);
  for (i = 0; rc == LC_OK && i < work.count; ++i)
    if (work.files[i].collection_id[0] &&
        !strncmp(work.files[i].leaf, "seg-", 4))
      rc = lc_collection_recode_segment(&work, i, directory, error);
  /* Prepared reference replacements must be installed before another file
   * reads their payload descriptors. rc_references installs its own receipt
   * before doing new work; at most one prepared payload exists. */
  for (i = 0; rc == LC_OK && i < work.count; ++i)
    rc = rc_references(&work, i, directory, payload, error);
  if (rc == LC_OK)
    rc = rc_heads(&work, directory, error);
  if (rc == LC_OK)
    rc = rc_cleanup(&work, source, error);
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
    lc_free_with_allocator(&pouch->allocator, file->references);
  }
  lc_free_with_allocator(&pouch->allocator, work.files);
  lc_free_with_allocator(&pouch->allocator, payload);
  lc_free_with_allocator(&pouch->allocator, work.state_scratch);
  if (rc != LC_OK && error && error->code == LC_OK)
    lc_error_set(error, rc, 0, "failed to resume Pouch compression conversion",
                 NULL, NULL, "pouch");
  return rc;
}
