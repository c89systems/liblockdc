#include "lc_collection_ids.h"
#include "lc_api_internal.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define LC_CI_HEADER_BYTES 64U
#define LC_CI_ENTRY_BYTES 80U
#define LC_CI_MAGIC 0x4c434944UL
#define LC_CI_INITIAL_CAPACITY 64U
#define LC_CI_GROW_REQUIRED (-1)

static int lc_ci_error(lc_error *error, int code, const char *message) {
  return lc_error_set(error, code, 0L, message,
                      code == LC_ERR_TRANSPORT ? strerror(errno) : NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}

static void lc_ci_put64(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)value;
    value >>= 8U;
  }
}

static lc_u64 lc_ci_get64(const unsigned char *bytes) {
  unsigned int i;
  lc_u64 value;
  value = 0U;
  for (i = 8U; i > 0U; --i) {
    value = (value << 8U) | bytes[i - 1U];
  }
  return value;
}

static void lc_ci_seal(unsigned char *bytes, size_t length) {
  unsigned long crc;
  crc = (unsigned long)crc32(0L, bytes, (uInt)(length - 4U));
  bytes[length - 4U] = (unsigned char)crc;
  bytes[length - 3U] = (unsigned char)(crc >> 8U);
  bytes[length - 2U] = (unsigned char)(crc >> 16U);
  bytes[length - 1U] = (unsigned char)(crc >> 24U);
}

static int lc_ci_valid(const unsigned char *bytes, size_t length) {
  unsigned char copy[LC_CI_ENTRY_BYTES];
  memcpy(copy, bytes, length);
  lc_ci_seal(copy, length);
  return memcmp(copy + length - 4U, bytes + length - 4U, 4U) == 0;
}

static int lc_ci_io(int fd, lc_u64 offset, unsigned char *bytes, size_t length,
                    int writing, lc_error *error) {
  size_t done;
  ssize_t got;
  if (offset > (lc_u64)LC_I64_MAX || length > (lc_u64)LC_I64_MAX - offset) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity index offset exceeds file range");
  }
  done = 0U;
  while (done < length) {
    got = writing
              ? pwrite(fd, bytes + done, length - done, (off_t)(offset + done))
              : pread(fd, bytes + done, length - done, (off_t)(offset + done));
    if (got < 0 && errno == EINTR) {
      continue;
    }
    if (got <= 0) {
      return lc_ci_error(
          error, got == 0 && !writing ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
          "failed to access collection identity index");
    }
    done += (size_t)got;
  }
  return LC_OK;
}

static int lc_ci_header_write(lc_collection_ids *ids, lc_error *error) {
  unsigned char bytes[LC_CI_HEADER_BYTES];
  memset(bytes, 0, sizeof(bytes));
  lc_ci_put64(bytes, LC_CI_MAGIC);
  lc_ci_put64(bytes + 8U, ids->capacity);
  lc_ci_put64(bytes + 16U, ids->count);
  lc_ci_put64(bytes + 24U, ids->generation);
  lc_ci_put64(bytes + 32U, ids->replay_segment);
  lc_ci_put64(bytes + 40U, ids->replay_offset);
  lc_ci_put64(bytes + 48U, ids->snapshot);
  lc_ci_seal(bytes, sizeof(bytes));
  return lc_ci_io(ids->fd, 0U, bytes, sizeof(bytes), 1, error);
}

static int lc_ci_capacity_valid(lc_u64 capacity) {
  return capacity >= LC_CI_INITIAL_CAPACITY &&
         (capacity & (capacity - 1U)) == 0U &&
         capacity <=
             ((lc_u64)LC_I64_MAX - LC_CI_HEADER_BYTES) / LC_CI_ENTRY_BYTES;
}

int lc_collection_ids_refresh(lc_collection_ids *ids, lc_error *error) {
  unsigned char bytes[LC_CI_HEADER_BYTES];
  struct stat st;
  struct stat named;
  int replacement;
  int rc;
  if (ids == NULL || ids->fd < 0) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity index is closed");
  }
  if (stat(ids->path, &named) != 0 || fstat(ids->fd, &st) != 0) {
    return lc_ci_error(error, LC_ERR_TRANSPORT,
                       "failed to inspect collection identity index identity");
  }
  if (named.st_dev != st.st_dev || named.st_ino != st.st_ino) {
    replacement = open(ids->path, O_RDWR);
    if (replacement < 0) {
      return lc_ci_error(error, LC_ERR_TRANSPORT,
                         "failed to reopen replaced collection identity index");
    }
    (void)close(ids->fd);
    ids->fd = replacement;
    if (fstat(ids->fd, &st) != 0) {
      return lc_ci_error(error, LC_ERR_TRANSPORT,
                         "failed to stat replaced collection identity index");
    }
  }
  rc = lc_ci_io(ids->fd, 0U, bytes, sizeof(bytes), 0, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_ci_valid(bytes, sizeof(bytes)) || lc_ci_get64(bytes) != LC_CI_MAGIC ||
      bytes[56] != 0U || bytes[57] != 0U || bytes[58] != 0U ||
      bytes[59] != 0U) {
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection identity index header is invalid");
  }
  ids->capacity = lc_ci_get64(bytes + 8U);
  ids->count = lc_ci_get64(bytes + 16U);
  ids->generation = lc_ci_get64(bytes + 24U);
  ids->replay_segment = lc_ci_get64(bytes + 32U);
  ids->replay_offset = lc_ci_get64(bytes + 40U);
  ids->snapshot = lc_ci_get64(bytes + 48U);
  if (!lc_ci_capacity_valid(ids->capacity) || ids->count > ids->capacity) {
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection identity index bounds are invalid");
  }
  /* The caller holds metadata authority throughout refresh; reuse the stat of
   * this descriptor rather than issuing it again after the header read. */
  if (!S_ISREG(st.st_mode) || st.st_size < 0 ||
      (lc_u64)st.st_size !=
          LC_CI_HEADER_BYTES + ids->capacity * LC_CI_ENTRY_BYTES) {
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection identity index length is inconsistent");
  }
  return LC_OK;
}

int lc_collection_ids_open(const char *path, int create,
                           const lc_allocator *allocator,
                           lc_collection_ids *out, lc_error *error) {
  struct stat st;
  int rc;
  if (out == NULL) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity index requires output");
  }
  memset(out, 0, sizeof(*out));
  out->fd = -1;
  if (allocator != NULL) {
    out->allocator = *allocator;
  } else {
    lc_allocator_init(&out->allocator);
  }
  if (path == NULL || path[0] == '\0') {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity index requires path");
  }
  out->path = lc_strdup_with_allocator(&out->allocator, path);
  if (out->path == NULL) {
    return lc_ci_error(error, LC_ERR_NOMEM,
                       "failed to retain collection identity index path");
  }
  out->fd = open(path, O_RDWR | (create ? O_CREAT : 0), 0600);
  if (out->fd < 0) {
    rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                     "failed to open collection identity index");
    lc_collection_ids_close(out);
    return rc;
  }
  rc = LC_OK;
  if (fstat(out->fd, &st) != 0) {
    rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                     "failed to stat collection identity index");
  } else if (create && st.st_size == 0) {
    out->capacity = LC_CI_INITIAL_CAPACITY;
    if (ftruncate(out->fd, LC_CI_HEADER_BYTES +
                               out->capacity * LC_CI_ENTRY_BYTES) != 0) {
      rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                       "failed to size collection identity index");
    } else {
      rc = lc_ci_header_write(out, error);
    }
  }
  if (rc == LC_OK) {
    rc = lc_collection_ids_refresh(out, error);
  }
  if (rc != LC_OK) {
    lc_collection_ids_close(out);
  }
  return rc;
}

void lc_collection_ids_close(lc_collection_ids *ids) {
  if (ids != NULL) {
    if (ids->fd >= 0) {
      (void)close(ids->fd);
    }
    lc_free_with_allocator(&ids->allocator, ids->path);
    memset(ids, 0, sizeof(*ids));
    ids->fd = -1;
  }
}

static int lc_ci_entry_read(lc_collection_ids *ids, lc_u64 slot,
                            lc_collection_id_ref *ref, int *occupied,
                            lc_error *error) {
  unsigned char bytes[LC_CI_ENTRY_BYTES];
  size_t i;
  int rc;
  rc = lc_ci_io(ids->fd, LC_CI_HEADER_BYTES + slot * LC_CI_ENTRY_BYTES, bytes,
                sizeof(bytes), 0, error);
  if (rc != LC_OK) {
    return rc;
  }
  *occupied = 0;
  ref->position = lc_ci_get64(bytes + 32U);
  if (ref->position == 0U) {
    for (i = 0U; i < sizeof(bytes); ++i) {
      if (bytes[i] != 0U) {
        return lc_ci_error(error, LC_ERR_PROTOCOL,
                           "collection identity index empty slot is corrupt");
      }
    }
    return LC_OK;
  }
  if (!lc_ci_valid(bytes, sizeof(bytes))) {
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection identity index entry checksum is invalid");
  }
  for (i = 64U; i < sizeof(bytes) - 4U; ++i) {
    if (bytes[i] != 0U) {
      return lc_ci_error(error, LC_ERR_PROTOCOL,
                         "collection identity index entry has unknown fields");
    }
  }
  memcpy(ref->digest, bytes, sizeof(ref->digest));
  ref->generation = lc_ci_get64(bytes + 40U);
  ref->segment = lc_ci_get64(bytes + 48U);
  ref->offset = lc_ci_get64(bytes + 56U);
  if (ref->generation == 0U || ref->segment == 0U) {
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection identity index reference is invalid");
  }
  *occupied = 1;
  return LC_OK;
}

static int lc_ci_entry_write(lc_collection_ids *ids, lc_u64 slot,
                             const lc_collection_id_ref *ref, lc_error *error) {
  unsigned char bytes[LC_CI_ENTRY_BYTES];
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, ref->digest, sizeof(ref->digest));
  lc_ci_put64(bytes + 32U, ref->position);
  lc_ci_put64(bytes + 40U, ref->generation);
  lc_ci_put64(bytes + 48U, ref->segment);
  lc_ci_put64(bytes + 56U, ref->offset);
  lc_ci_seal(bytes, sizeof(bytes));
  return lc_ci_io(ids->fd, LC_CI_HEADER_BYTES + slot * LC_CI_ENTRY_BYTES, bytes,
                  sizeof(bytes), 1, error);
}

int lc_collection_ids_find(lc_collection_ids *ids,
                           const unsigned char digest[32],
                           lc_collection_id_match_fn match, void *context,
                           lc_collection_id_ref *out, int *found,
                           lc_error *error) {
  lc_collection_id_ref candidate;
  lc_u64 slot;
  lc_u64 examined;
  int occupied;
  int matched;
  int rc;
  if (found != NULL) {
    *found = 0;
  }
  if (ids == NULL || ids->fd < 0 || digest == NULL || match == NULL ||
      out == NULL || found == NULL) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity lookup arguments are invalid");
  }
  slot = lc_ci_get64(digest) & (ids->capacity - 1U);
  for (examined = 0U; examined < ids->capacity; ++examined) {
    rc = lc_ci_entry_read(ids, slot, &candidate, &occupied, error);
    if (rc != LC_OK || !occupied) {
      return rc;
    }
    if (memcmp(candidate.digest, digest, 32U) == 0) {
      matched = 0;
      rc = match(context, &candidate, &matched, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (matched) {
        *out = candidate;
        *found = 1;
        return LC_OK;
      }
    }
    slot = (slot + 1U) & (ids->capacity - 1U);
  }
  return lc_ci_error(error, LC_ERR_PROTOCOL,
                     "collection identity index has no terminating empty slot");
}

static int lc_ci_insert(lc_collection_ids *ids, const lc_collection_id_ref *ref,
                        int resize_if_new, lc_error *error) {
  lc_collection_id_ref candidate;
  lc_u64 slot;
  lc_u64 examined;
  int occupied;
  int rc;
  slot = lc_ci_get64(ref->digest) & (ids->capacity - 1U);
  for (examined = 0U; examined < ids->capacity; ++examined) {
    rc = lc_ci_entry_read(ids, slot, &candidate, &occupied, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!occupied || (candidate.position == ref->position &&
                      memcmp(candidate.digest, ref->digest, 32U) == 0)) {
      if (!occupied && resize_if_new && ids->count >= ids->capacity / 2U) {
        return LC_CI_GROW_REQUIRED;
      }
      if (occupied && candidate.generation > ref->generation) {
        return LC_OK;
      }
      rc = lc_ci_entry_write(ids, slot, ref, error);
      if (rc == LC_OK && !occupied) {
        ++ids->count;
      }
      return rc;
    }
    slot = (slot + 1U) & (ids->capacity - 1U);
  }
  return lc_ci_error(error, LC_ERR_PROTOCOL,
                     "collection identity index has no insertion slot");
}

int lc_collection_ids_slot(lc_collection_ids *ids, lc_u64 slot,
                           lc_collection_id_ref *ref, int *occupied,
                           lc_error *error) {
  if (slot >= ids->capacity)
    return lc_ci_error(error, LC_ERR_INVALID, "identity slot outside capacity");
  return lc_ci_entry_read(ids, slot, ref, occupied, error);
}

static int lc_ci_grow(lc_collection_ids *ids, lc_error *error) {
  lc_collection_ids grown;
  lc_collection_id_ref ref;
  char id[LC_XID_STRING_SIZE];
  char *temporary;
  char *directory;
  char *slash;
  size_t size;
  lc_u64 slot;
  int occupied;
  int rc;
  int renamed;
  int created;
  if (ids->capacity > LC_U64_MAX / 2U ||
      !lc_ci_capacity_valid(ids->capacity * 2U)) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity index has exhausted file capacity");
  }
  rc = lc_xid_new(id, error);
  if (rc != LC_OK) {
    return rc;
  }
  size = strlen(ids->path) + sizeof(id) + sizeof(".rebuild-");
  temporary = (char *)lc_alloc_with_allocator(&ids->allocator, size);
  directory = lc_strdup_with_allocator(&ids->allocator, ids->path);
  if (temporary == NULL || directory == NULL) {
    lc_free_with_allocator(&ids->allocator, temporary);
    lc_free_with_allocator(&ids->allocator, directory);
    return lc_ci_error(error, LC_ERR_NOMEM,
                       "failed to allocate collection identity rebuild paths");
  }
  snprintf(temporary, size, "%s.rebuild-%s", ids->path, id);
  slash = strrchr(directory, '/');
  if (slash == NULL) {
    strcpy(directory, ".");
  } else if (slash == directory) {
    slash[1] = '\0';
  } else {
    *slash = '\0';
  }
  memset(&grown, 0, sizeof(grown));
  grown.fd = open(temporary, O_RDWR | O_CREAT | O_EXCL, 0600);
  created = grown.fd >= 0;
  grown.capacity = ids->capacity * 2U;
  grown.generation = ids->generation;
  grown.replay_segment = ids->replay_segment;
  grown.replay_offset = ids->replay_offset;
  grown.snapshot = ids->snapshot;
  renamed = 0;
  if (grown.fd < 0 ||
      ftruncate(grown.fd, (off_t)(LC_CI_HEADER_BYTES +
                                  grown.capacity * LC_CI_ENTRY_BYTES)) != 0) {
    rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                     "failed to create collection identity rebuild");
  } else {
    rc = LC_OK;
  }
  for (slot = 0U; rc == LC_OK && slot < ids->capacity; ++slot) {
    rc = lc_ci_entry_read(ids, slot, &ref, &occupied, error);
    if (rc == LC_OK && occupied) {
      rc = lc_ci_insert(&grown, &ref, 0, error);
    }
  }
  if (rc == LC_OK) {
    rc = lc_ci_header_write(&grown, error);
  }
  if (rc == LC_OK && fsync(grown.fd) != 0) {
    rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                     "failed to sync collection identity rebuild");
  }
  if (rc == LC_OK && rename(temporary, ids->path) != 0) {
    rc = lc_ci_error(error, LC_ERR_TRANSPORT,
                     "failed to publish collection identity rebuild");
  } else if (rc == LC_OK) {
    renamed = 1;
    (void)close(ids->fd);
    ids->fd = grown.fd;
    ids->capacity = grown.capacity;
    ids->count = grown.count;
    grown.fd = -1;
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync collection identity index directory", error);
  }
  if (grown.fd >= 0) {
    (void)close(grown.fd);
  }
  if (created && !renamed) {
    (void)unlink(temporary);
  }
  lc_free_with_allocator(&ids->allocator, temporary);
  lc_free_with_allocator(&ids->allocator, directory);
  return rc;
}

int lc_collection_ids_put(lc_collection_ids *ids,
                          const lc_collection_id_ref *ref, lc_error *error) {
  int rc;
  if (ids == NULL || ids->fd < 0 || ref == NULL || ref->position == 0U ||
      ref->generation == 0U || ref->segment == 0U) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity update arguments are invalid");
  }
  rc = lc_ci_insert(ids, ref, 1, error);
  if (rc == LC_CI_GROW_REQUIRED) {
    rc = lc_ci_grow(ids, error);
    if (rc == LC_OK) {
      rc = lc_ci_insert(ids, ref, 0, error);
    }
  }
  if (rc == LC_OK) {
    if (ref->generation > ids->generation) {
      ids->generation = ref->generation;
    }
  }
  return rc;
}

int lc_collection_ids_checkpoint(lc_collection_ids *ids, lc_u64 count,
                                 lc_u64 generation, lc_u64 replay_segment,
                                 lc_u64 replay_offset, lc_error *error) {
  if (ids == NULL || ids->fd < 0 || count > ids->capacity ||
      generation < ids->generation || replay_segment == 0U) {
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection identity checkpoint arguments are invalid");
  }
  ids->count = count;
  ids->generation = generation;
  ids->replay_segment = replay_segment;
  ids->replay_offset = replay_offset;
  return lc_ci_header_write(ids, error);
}
