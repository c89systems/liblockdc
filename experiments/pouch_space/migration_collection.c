#include "collections.h"
#include "lc_api_internal.h"
#include "lc_collection_catalog_record.h"
#include "lc_collection_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "pouch_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct collection_directory {
  const lc_collection_disk_head *head;
  int fd;
} collection_directory;

static int corruption(const char *message, const char *path, lc_error *e) {
  return lc_error_set(e, LC_ERR_PROTOCOL, 0L, message, path,
                      "collection_corrupt", "pouch");
}

static int part_end(lc_pouch_record_io io, const char *leaf, uint64_t start,
                    uint64_t *end, int active, lc_error *e) {
  lc_pouch_collection_record record;
  uint64_t at = start, accepted = start;
  int rc = LC_OK;

  if (start > *end)
    return corruption("legacy collection tail refers beyond its segment", leaf,
                      e);
  while (at < *end) {
    rc = *end - at < LC_POUCH_RECORD_HEADER_BYTES
             ? LC_ERR_PENDING
             : lc_pouch_collection_record_read(io, at, *end, leaf, &record, e);
    if (rc == LC_ERR_PENDING) {
      if (!active)
        return corruption("legacy collection segment is incomplete", leaf, e);
      lc_error_cleanup(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK)
      return rc;
    at = record.next_offset;
    if (record.kind != LC_POUCH_COLLECTION_EDIT_BODY)
      accepted = at;
  }
  if (!active && accepted != *end)
    return corruption("legacy collection segment has an unfinished edit", leaf,
                      e);
  *end = accepted;
  return rc;
}

static int visit_part(collection_directory *source, lc_u64 id,
                      ps_collection_import_part_fn visit, void *context,
                      lc_error *e) {
  char leaf[64];
  struct stat info;
  lc_pouch_record_io io;
  uint64_t end, start;
  int fd, rc = lc_collection_record_leaf(id, leaf, e);

  if (rc != LC_OK)
    return rc;
  fd = openat(source->fd, leaf, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "cannot open legacy collection segment", leaf, NULL,
                        "pouch");
  if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 0)
    rc = corruption("invalid legacy collection segment", leaf, e);
  if (rc == LC_OK) {
    io = lc_pouch_record_file_io(fd);
    start = id == source->head->tail_segment ? source->head->tail_offset : 0;
    end = (uint64_t)info.st_size;
    rc = part_end(io, leaf, start, &end, id == source->head->active, e);
  }
  if (rc == LC_OK)
    rc = visit(context, id, io, end, e);
  close(fd);
  return rc;
}

static int visit_parts(void *context, ps_collection_import_part_fn visit,
                       void *target, lc_error *e) {
  collection_directory *source = context;
  const lc_collection_disk_head *head = source->head;
  lc_u64 n, id;
  int rc = LC_OK;
  if (head->snapshot) {
    for (n = 0; rc == LC_OK && n <= head->snapshot_extra_parts; ++n)
      rc = visit_part(source, (head->snapshot + n) | LC_COLLECTION_SNAPSHOT_BIT,
                      visit, target, e);
  }
  for (id = head->tail_segment; rc == LC_OK && id <= head->active; ++id)
    rc = visit_part(source, id, visit, target, e);
  return rc;
}

static int import_collection(lc_pouch *source, lc_pouch *target,
                             ps_namespace *ns, const char *base,
                             const char *names, const char *data,
                             const char *leaf, lc_error *e) {
  const lc_allocator *allocator = &source->allocator;
  lc_collection_disk_head head;
  collection_directory partitions;
  char id[LC_XID_STRING_SIZE], name[LC_COLLECTION_NAME_BYTES];
  char *entry, *expected = NULL, *path = NULL;
  int rc;

  entry = lc_pouch_path_join(allocator, names, leaf);
  rc = entry ? lc_collection_catalog_read(entry, id, name, e)
             : lc_error_set(e, LC_ERR_NOMEM, 0L,
                            "cannot allocate legacy collection name path", NULL,
                            NULL, "pouch");
  if (rc == LC_OK)
    rc = lc_collection_catalog_name_path(allocator, base, name, &expected, e);
  if (rc == LC_OK && strcmp(entry, expected))
    rc = corruption("legacy collection name has the wrong digest", entry, e);
  if (rc == LC_OK) {
    path = lc_pouch_path_join(allocator, data, id);
    if (!path)
      rc = lc_error_set(e, LC_ERR_NOMEM, 0L,
                        "cannot allocate legacy collection partition path",
                        NULL, NULL, "pouch");
  }
  if (rc == LC_OK)
    rc = lc_collection_recode_head(source, path, &head, e);
  if (rc == LC_OK && !head.dropped) {
    partitions.head = &head;
    partitions.fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (partitions.fd < 0)
      rc = lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "cannot open legacy collection partition", path, NULL,
                        "pouch");
    else {
      rc = ps_collection_import(target, ns, name, id, &head, visit_parts,
                                &partitions, e);
      close(partitions.fd);
    }
  }
  lc_free_with_allocator(allocator, path);
  lc_free_with_allocator(allocator, expected);
  lc_free_with_allocator(allocator, entry);
  return rc;
}

int ps_pouch_import_legacy_collections(lc_pouch *source, lc_pouch *target,
                                       ps_namespace *ns, const char *name,
                                       lc_error *e) {
  const lc_allocator *allocator = &source->allocator;
  char *path, *base = NULL, *names = NULL, *data = NULL;
  DIR *dir = NULL;
  struct dirent *entry;
  int rc = LC_OK;

  path = lc_pouch_namespace_path(allocator, source->root_path, name);
  if (path)
    base = lc_pouch_path_join(allocator, path, "collections");
  if (base) {
    names = lc_pouch_path_join(allocator, base, "names");
    data = lc_pouch_path_join(allocator, base, "data");
  }
  if (!names || !data)
    rc = lc_error_set(e, LC_ERR_NOMEM, 0L,
                      "cannot allocate legacy collection catalog paths", NULL,
                      NULL, "pouch");
  if (rc == LC_OK) {
    dir = opendir(names);
    if (!dir && errno != ENOENT)
      rc = lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "cannot open legacy collection catalog", names, NULL,
                        "pouch");
  }
  while (rc == LC_OK && dir) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                          "cannot read legacy collection catalog", names, NULL,
                          "pouch");
      break;
    }
    if (strlen(entry->d_name) != 64 ||
        strspn(entry->d_name, "0123456789abcdef") != 64)
      continue;
    rc = import_collection(source, target, ns, base, names, data, entry->d_name,
                           e);
  }
  if (dir)
    closedir(dir);
  lc_free_with_allocator(allocator, data);
  lc_free_with_allocator(allocator, names);
  lc_free_with_allocator(allocator, base);
  lc_free_with_allocator(allocator, path);
  return rc;
}
