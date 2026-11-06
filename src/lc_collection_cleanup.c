#include "lc_collection_internal.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

static int lc_cl_number(const char *name, const char *prefix,
                        const char *suffix, lc_u64 *value) {
  size_t start;
  start = strlen(prefix);
  return strlen(name) == start + 20U + strlen(suffix) &&
         strncmp(name, prefix, start) == 0 &&
         strcmp(name + start + 20U, suffix) == 0 &&
         lc_parse_u64_base10_range_checked(name + start, 20U, value);
}

/* 0 is unrelated, 1 referenced canonical storage, 2 obsolete canonical
 * storage, 3 an interrupted derived build, 4 an unpublished append. The
 * metadata lock freezes head; pending appends additionally own a file lock. */
static int lc_cl_kind(lc_collection_handle *collection, const char *name) {
  lc_u64 id;
  char xid[LC_XID_STRING_SIZE];
  if (strlen(name) == 8U + LC_XID_STRING_SIZE - 1U + 4U &&
      strncmp(name, "pending-", 8U) == 0 &&
      strcmp(name + 8U + LC_XID_STRING_SIZE - 1U, ".log") == 0) {
    memcpy(xid, name + 8U, LC_XID_STRING_SIZE - 1U);
    xid[LC_XID_STRING_SIZE - 1U] = '\0';
    if (lc_xid_is_valid(xid))
      return 4;
  }
  if (lc_cl_number(name, "seg-", ".log", &id) && id != 0U &&
      id < LC_COLLECTION_SNAPSHOT_BIT)
    return id >= collection->head.tail_segment && id <= collection->head.active
               ? 1
               : 2;
  if (lc_cl_number(name, "snapshot-", ".log", &id) && id != 0U &&
      id < LC_COLLECTION_SNAPSHOT_BIT)
    return id >= collection->head.snapshot &&
                   id - collection->head.snapshot <=
                       collection->head.snapshot_extra_parts
               ? 1
               : 2;
  if (strncmp(name, "order.rebuild-", 14U) == 0 ||
      strncmp(name, "ids.rebuild-", 12U) == 0 ||
      strncmp(name, "order.compact-", 14U) == 0 ||
      strncmp(name, "ids.compact-", 12U) == 0)
    return 3;
  return 0;
}

static int lc_cl_readers(lc_collection_handle *collection, int *busy,
                         lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  char *path;
  lc_u64 generation;
  int fd;
  int rc;
  int lock_rc;
  *busy = 0;
  dir = opendir(collection->directory);
  if (dir == NULL)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to enumerate collection read generations");
  rc = LC_OK;
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to read collection generation directory");
      break;
    }
    if (!lc_cl_number(entry->d_name, ".readers-", ".lock", &generation) ||
        generation == collection->head.snapshot)
      continue;
    path = lc_pouch_path_join(&collection->allocator, collection->directory,
                              entry->d_name);
    if (path == NULL) {
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection read generation path");
      break;
    }
    fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0)
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to open old collection read generation");
    else {
      do {
        lock_rc = flock(fd, LOCK_EX | LOCK_NB);
      } while (lock_rc != 0 && errno == EINTR);
      if (lock_rc != 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
        *busy = 1;
      else if (lock_rc != 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to inspect old collection readers");
      else if (unlink(path) != 0 && errno != ENOENT)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to retire old collection read generation");
      (void)close(fd);
    }
    lc_free_with_allocator(&collection->allocator, path);
    if (rc != LC_OK)
      break;
  }
  (void)closedir(dir);
  return rc;
}

/* A first-obsolete timestamp belongs to each file, so continuing compactions
 * cannot restart its grace period forever. Markers contain no state values. */
static int lc_cl_obsolete_at(const char *path, lc_u64 now, lc_u64 *marked,
                             lc_error *error) {
  unsigned char bytes[17];
  unsigned long crc;
  unsigned long stored;
  size_t done;
  size_t i;
  ssize_t got;
  int fd;
  int rc;
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0 && errno == ENOENT) {
    memset(bytes, 0, sizeof(bytes));
    *marked = now;
    for (i = 0U; i < 8U; ++i) {
      bytes[i] = (unsigned char)now;
      now >>= 8U;
    }
    crc = (unsigned long)crc32(0L, bytes, 12U);
    for (i = 0U; i < 4U; ++i)
      bytes[12U + i] = (unsigned char)(crc >> (i * 8U));
    return lc_pouch_path_write_bytes_file(path, bytes, 16U, error);
  }
  if (fd < 0)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to read collection obsolete-file marker");
  done = 0U;
  rc = LC_OK;
  while (done < sizeof(bytes)) {
    got = read(fd, bytes + done, sizeof(bytes) - done);
    if (got < 0 && errno == EINTR)
      continue;
    if (got < 0) {
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to read collection reclamation time");
      break;
    }
    if (got == 0)
      break;
    done += (size_t)got;
  }
  (void)close(fd);
  if (rc != LC_OK)
    return rc;
  if (done != 16U)
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection reclamation timestamp is truncated");
  crc = (unsigned long)crc32(0L, bytes, 12U);
  stored = (unsigned long)bytes[12] | ((unsigned long)bytes[13] << 8U) |
           ((unsigned long)bytes[14] << 16U) |
           ((unsigned long)bytes[15] << 24U);
  if (done != 16U || bytes[8] != 0U || bytes[9] != 0U || bytes[10] != 0U ||
      bytes[11] != 0U || crc != stored)
    return lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection reclamation timestamp is corrupt");
  *marked = 0U;
  for (i = 8U; i > 0U; --i)
    *marked = (*marked << 8U) | bytes[i - 1U];
  return LC_OK;
}

int lc_collection_cleanup_files(lc_collection_handle *collection,
                                lc_pouch_maintenance_result *out,
                                lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  lc_pouch_unix_seconds instant;
  lc_u64 marked = 0U;
  char *path;
  char *marker;
  char leaf[80];
  int rc;
  int busy;
  int kind;
  int remove;
  int pending_fd;
  rc = lc_cl_readers(collection, &busy, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_now_unix(&instant, error);
  if (rc != LC_OK)
    return rc;
  dir = opendir(collection->directory);
  if (dir == NULL)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to enumerate collection obsolete files");
  for (;;) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to read collection obsolete-file directory");
      break;
    }
    kind = lc_cl_kind(collection, entry->d_name);
    if (kind == 0 && strncmp(entry->d_name, ".obsolete-", 10U) == 0) {
      path = lc_pouch_path_join(&collection->allocator, collection->directory,
                                entry->d_name + 10U);
      marker = lc_pouch_path_join(&collection->allocator, collection->directory,
                                  entry->d_name);
      if (path == NULL || marker == NULL)
        rc = lc_collection_failure(
            error, LC_ERR_NOMEM, NULL,
            "failed to allocate collection reclamation paths");
      else if (lc_cl_kind(collection, entry->d_name + 10U) == 1 ||
               (lstat(path, &st) != 0 && errno == ENOENT)) {
        if (unlink(marker) != 0 && errno != ENOENT)
          rc = lc_collection_failure(
              error, LC_ERR_TRANSPORT, NULL,
              "failed to remove stale collection reclamation marker");
      }
      lc_free_with_allocator(&collection->allocator, path);
      lc_free_with_allocator(&collection->allocator, marker);
      if (rc != LC_OK)
        break;
      continue;
    }
    if (kind != 2 && kind != 3 && kind != 4)
      continue;
    path = lc_pouch_path_join(&collection->allocator, collection->directory,
                              entry->d_name);
    marker = NULL;
    if (path == NULL) {
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection obsolete-file path");
      break;
    }
    remove = kind == 3;
    pending_fd = -1;
    if (kind == 4) {
      int locked;
      pending_fd = open(path, O_RDWR | O_CLOEXEC);
      if (pending_fd < 0 && errno == ENOENT) {
        lc_free_with_allocator(&collection->allocator, path);
        continue;
      }
      if (pending_fd < 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to inspect pending collection append");
      else {
        do {
          locked = flock(pending_fd, LOCK_EX | LOCK_NB);
        } while (locked != 0 && errno == EINTR);
        if (locked == 0)
          remove = 1;
        else if (errno != EWOULDBLOCK && errno != EAGAIN)
          rc = lc_collection_failure(
              error, LC_ERR_TRANSPORT, NULL,
              "failed to lock abandoned collection append");
      }
    }
    if (kind == 2) {
      remove = !busy;
      if (strlen(entry->d_name) + 11U > sizeof(leaf))
        rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                   "collection obsolete filename is too long");
      else {
        memcpy(leaf, ".obsolete-", 10U);
        strcpy(leaf + 10U, entry->d_name);
        marker = lc_pouch_path_join(&collection->allocator,
                                    collection->directory, leaf);
        if (marker == NULL)
          rc = lc_collection_failure(
              error, LC_ERR_NOMEM, NULL,
              "failed to allocate collection obsolete timestamp path");
        else if (collection->pouch->compaction_delete_grace_seconds != 0U)
          rc = lc_cl_obsolete_at(marker, (lc_u64)instant, &marked, error);
        if (rc == LC_OK &&
            collection->pouch->compaction_delete_grace_seconds != 0U &&
            ((lc_u64)instant < marked ||
             (lc_u64)instant - marked <
                 collection->pouch->compaction_delete_grace_seconds))
          remove = 0;
      }
    }
    if (rc == LC_OK && remove) {
      if (unlink(path) != 0 && errno != ENOENT)
        rc =
            lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                  "failed to reclaim collection obsolete file");
      if (rc == LC_OK && marker != NULL && unlink(marker) != 0 &&
          errno != ENOENT)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to remove collection reclamation marker");
      if (rc == LC_OK && out != NULL)
        ++out->cleanup_deleted_count;
    } else if (rc == LC_OK && out != NULL)
      ++out->cleanup_pending_count;
    lc_free_with_allocator(&collection->allocator, path);
    lc_free_with_allocator(&collection->allocator, marker);
    if (pending_fd >= 0)
      (void)close(pending_fd);
    if (rc != LC_OK)
      break;
  }
  (void)closedir(dir);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        collection->directory, "failed to sync collection reclamation", error);
  return rc;
}
