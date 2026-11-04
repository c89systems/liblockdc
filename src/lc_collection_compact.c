#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_pouch_path.h"
#include "lc_pouch_record.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>
#include <zlib.h>

#define LC_CM_BUFFER_BYTES (128U * 1024U)

#ifdef LOCKDC_TEST_BUILD
lc_collection_compact_hook_fn lc_collection_compact_test_hook;
void *lc_collection_compact_test_context;
#endif

typedef struct lc_compact {
  lc_collection_call call;
  lc_collection_handle *collection;
  lc_collection_order captured;
  lc_collection_order order;
  lc_collection_ids ids;
  lc_collection_ids captured_ids;
  lc_u64 count;
  lc_u64 generation;
  lc_u64 metadata_generation;
  lc_u64 previous_snapshot;
  lc_u64 snapshot;
  lc_u64 part_snapshot;
  lc_u64 parts;
  lc_collection_chunks *chunks;
  lc_u64 tail;
  char leaf[64];
  char *path;
  char *order_path;
  char *ids_path;
  char *stable_ids_path;
  unsigned char *buffer;
  int lock_fd;
  int pin_fd;
  int snapshot_fd;
  int path_owned;
  int order_owned;
  int ids_owned;
  int commit_attempted;
} lc_compact;

static int lc_cm_phase(lc_compact *work, int phase, lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  int rc;
  if (lc_collection_compact_test_hook != NULL) {
    rc = lc_collection_compact_test_hook(lc_collection_compact_test_context,
                                         &work->collection->pub, phase, error);
    if (rc != LC_OK && error->code == LC_OK)
      lc_collection_failure(error, rc, NULL,
                            "collection compaction hook stopped publication");
    return rc;
  }
#else
  (void)work;
  (void)phase;
  (void)error;
#endif
  return LC_OK;
}

static int lc_cm_lock(lc_compact *work, int *busy, lc_error *error) {
  char *path;
  int rc;
  path = lc_pouch_path_join(&work->collection->allocator,
                            work->collection->directory, ".compaction.lock");
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection compaction lock path");
  work->lock_fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  lc_free_with_allocator(&work->collection->allocator, path);
  if (work->lock_fd < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection compaction lock");
  do {
    rc = flock(work->lock_fd, LOCK_EX | LOCK_NB);
  } while (rc != 0 && errno == EINTR);
  *busy = rc != 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
  return rc == 0 || *busy
             ? LC_OK
             : lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                     "failed to lock collection compaction");
}

static int lc_cm_capture(lc_compact *work, int force, int *selected,
                         lc_error *error) {
  lc_collection_handle *collection;
  lc_collection_guard guard;
  int rc, stream_fd = -1, busy = 0;
  char *stream_path;
  collection = work->collection;
  *selected = 0;
  rc = lc_collection_write_enter(&work->call, &guard, error);
  if (rc != LC_OK)
    return rc;
  stream_path = lc_pouch_path_join(&collection->allocator,
                                   collection->directory, ".streams.lock");
  if (stream_path == NULL)
    rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                               "failed to allocate stream gate");
  else {
    stream_fd = open(stream_path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    lc_free_with_allocator(&collection->allocator, stream_path);
    if (stream_fd < 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open stream gate");
    else if (flock(stream_fd, LOCK_EX | LOCK_NB) != 0) {
      busy = errno == EAGAIN || errno == EWOULDBLOCK;
      if (!busy)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to inspect live streams");
    }
  }
  if (rc == LC_OK && !busy &&
      (collection->order.count != 0U || collection->order.edit_count != 0U ||
       collection->order.reclaimable_bytes != 0U) &&
      (collection->order.generation != collection->head.snapshot_generation ||
       collection->order.reclaimable_bytes != 0U) &&
      (force || (collection->order.reclaimable_bytes != 0U &&
                 collection->order.reclaimable_bytes >=
                     collection->pouch->compaction_min_reclaimable_bytes))) {
    if (collection->head.next_snapshot >= LC_COLLECTION_SNAPSHOT_BIT - 1U)
      rc =
          lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                "collection snapshot identities are exhausted");
    if (rc == LC_OK)
      rc = lc_collection_read_pin(collection, collection->head.snapshot,
                                  &work->pin_fd, error);
    if (rc == LC_OK) {
      work->captured = collection->order;
      work->captured.fd = dup(collection->order.fd);
      if (work->captured.fd < 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to capture collection compaction order");
    }
    if (rc == LC_OK) {
      work->captured_ids = collection->ids;
      work->captured_ids.fd = dup(collection->ids.fd);
      if (work->captured_ids.fd < 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to pin edit receipts");
      work->count = collection->order.count;
      work->generation = collection->order.generation;
      work->metadata_generation = collection->order.metadata_generation;
      work->previous_snapshot = collection->head.snapshot;
      work->snapshot = ++collection->head.next_snapshot;
      /* Rotation seals the captured inputs and durably reserves this snapshot
       * ID. Producers can append to the new tail throughout the bulk copy. */
      rc = lc_collection_rotate(collection, 1, error);
      work->tail = collection->head.active;
      *selected = rc == LC_OK;
    }
  }
  if (stream_fd >= 0)
    (void)close(stream_fd);
  lc_collection_metadata_leave(collection);
  lc_collection_write_leave(&guard);
  return rc;
}

static int lc_cm_create_projection(lc_compact *work, lc_error *error) {
  char xid[LC_XID_STRING_SIZE];
  char leaf[64];
  int fd;
  int rc;
  rc = lc_collection_record_path(work->collection,
                                 work->snapshot | LC_COLLECTION_SNAPSHOT_BIT,
                                 &work->path, work->leaf, error);
  if (rc != LC_OK)
    return rc;
  work->part_snapshot = work->snapshot;
  work->parts = 1;
  work->snapshot_fd =
      open(work->path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  work->path_owned = work->snapshot_fd >= 0;
  if (work->snapshot_fd < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to create collection snapshot");
  rc = lc_xid_new(xid, error);
  if (rc != LC_OK)
    return rc;
  snprintf(leaf, sizeof(leaf), "order.compact-%s", xid);
  work->order_path = lc_pouch_path_join(&work->collection->allocator,
                                        work->collection->directory, leaf);
  snprintf(leaf, sizeof(leaf), "ids.compact-%s", xid);
  work->ids_path = lc_pouch_path_join(&work->collection->allocator,
                                      work->collection->directory, leaf);
  work->stable_ids_path = lc_strdup_with_allocator(&work->collection->allocator,
                                                   work->collection->ids_path);
  work->buffer = (unsigned char *)lc_alloc_with_allocator(
      &work->collection->allocator, LC_CM_BUFFER_BYTES);
  work->chunks = lc_calloc_with_allocator(&work->collection->allocator, 1,
                                          sizeof(*work->chunks));
  if (work->order_path == NULL || work->ids_path == NULL ||
      work->stable_ids_path == NULL || work->buffer == NULL ||
      work->chunks == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate bounded collection compaction resources");
  fd = open(work->order_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to create compacted collection order");
  work->order_owned = 1;
  (void)close(fd);
  rc = lc_collection_order_open(work->order_path, 1, &work->order, error);
  if (rc != LC_OK)
    return rc;
  memcpy(work->order.query_id, work->captured.query_id, LC_XID_STRING_SIZE);
  fd = open(work->ids_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to create compacted collection identities");
  work->ids_owned = 1;
  (void)close(fd);
  rc = lc_collection_ids_open(work->ids_path, 1, &work->collection->allocator,
                              &work->ids, error);
  work->order.snapshot = work->ids.snapshot = work->snapshot;
  return rc;
}

/* Snapshot parts are final canonical files. Only the active part and the
 * caller's bounded copy buffer are writable; no whole-entry output exists. */
static int lc_cm_roll(lc_compact *work, lc_u64 required, lc_error *error) {
  lc_collection_handle *c = work->collection;
  lc_collection_guard guard;
  lc_u64 next = work->part_snapshot + 1U;
  char *path = NULL, leaf[64];
  off_t end;
  int fd = -1, rc;
  end = lseek(work->snapshot_fd, 0, SEEK_END);
  if (end < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect snapshot part");
  if (!end || ((lc_u64)end < c->pouch->segment_target_bytes &&
               required <= c->pouch->segment_target_bytes - (lc_u64)end))
    return LC_OK;
  if (fsync(work->snapshot_fd) != 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync snapshot part");
  rc = lc_collection_write_enter(&work->call, &guard, error);
  if (rc != LC_OK)
    return rc;
  if (c->head.next_snapshot != work->part_snapshot)
    rc =
        lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                              "snapshot reservation changed during compaction");
  else if (next >= LC_COLLECTION_SNAPSHOT_BIT)
    rc = lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                               "collection snapshot identities are exhausted");
  if (rc == LC_OK) {
    c->head.next_snapshot = next;
    rc = lc_collection_head_write(c, error);
  }
  lc_collection_metadata_leave(c);
  lc_collection_write_leave(&guard);
  if (rc == LC_OK)
    rc = lc_collection_record_path(c, next | LC_COLLECTION_SNAPSHOT_BIT, &path,
                                   leaf, error);
  if (rc == LC_OK) {
    fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to create snapshot part");
  }
  if (rc == LC_OK) {
    (void)close(work->snapshot_fd);
    lc_free_with_allocator(&c->allocator, work->path);
    work->path = path;
    path = NULL;
    strcpy(work->leaf, leaf);
    work->snapshot_fd = fd;
    work->part_snapshot = next;
    ++work->parts;
  }
  lc_free_with_allocator(&c->allocator, path);
  return rc;
}

static int lc_cm_output(void *context, lc_u64 required, int *fd,
                        const char **leaf, lc_u64 *identity, lc_error *error) {
  lc_compact *work = context;
  int rc = lc_cm_roll(work, required, error);
  if (rc == LC_OK) {
    *fd = work->snapshot_fd;
    *leaf = work->leaf;
    *identity = work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT;
  }
  return rc;
}

static int lc_cm_index(void *context, unsigned int level, const void *tuples,
                       size_t count, lc_collection_chunk_ref *ref,
                       lc_error *error) {
  lc_compact *work = context;
  int rc = lc_cm_roll(work, 40U + count, error);
  if (rc == LC_OK)
    rc = lc_collection_chunks_write_index(
        work->snapshot_fd, work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT,
        level, tuples, count, ref, error);
  return rc;
}

static int lc_cm_write(int fd, const void *bytes, size_t size,
                       lc_error *error) {
  const unsigned char *p = bytes;
  ssize_t n;
  while (size) {
    n = write(fd, p, size);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to copy collection codec unit");
    p += (size_t)n;
    size -= (size_t)n;
  }
  return LC_OK;
}

static int lc_cm_stream(lc_compact *work, lc_collection_chunk_reader *reader,
                        const lc_pouch_collection_record *source,
                        lc_pouch_collection_record *relocated,
                        lc_error *error) {
  lc_collection_chunk_ref ref, copied;
  lc_collection_unit unit;
  lc_pouch_record_header header;
  unsigned char prefix[LC_COLLECTION_UNIT_HEADER];
  lc_u64 remaining, offset;
  off_t output;
  unsigned long crc;
  ssize_t n;
  size_t want;
  int found, fd = -1, rc;
  memset(work->chunks, 0, sizeof(*work->chunks));
  work->chunks->append_index = lc_cm_index;
  work->chunks->index_context = work;
  for (;;) {
    rc =
        lc_collection_chunks_next_unit(reader, &found, &fd, &ref, &unit, error);
    if (rc != LC_OK || !found)
      break;
    rc = lc_cm_roll(work, LC_COLLECTION_UNIT_HEADER + unit.stored, error);
    if (rc != LC_OK)
      break;
    output = lseek(work->snapshot_fd, 0, SEEK_END);
    if (output < 0) {
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to seek snapshot codec unit");
      break;
    }
    /* The finalized metadata is installed only after every encoded byte and
     * its checksum have been copied. A failed build never becomes
     * authoritative. */
    memset(&header, 0, sizeof(header));
    header.type = LC_POUCH_COLLECTION_CHUNK;
    header.flags = LC_POUCH_RECORD_FLAG_PENDING;
    header.meta_len = LC_COLLECTION_UNIT_META;
    header.payload_len = unit.stored;
    header.payload_crc = unit.crc;
    memset(prefix, 0, sizeof(prefix));
    lc_pouch_record_header_encode(&header, prefix);
    rc = lc_cm_write(work->snapshot_fd, prefix, sizeof(prefix), error);
    remaining = unit.stored;
    offset = ref.offset + LC_COLLECTION_UNIT_HEADER;
    crc = (unsigned long)crc32(0L, Z_NULL, 0);
    while (rc == LC_OK && remaining) {
      want = remaining < LC_CM_BUFFER_BYTES ? (size_t)remaining
                                            : LC_CM_BUFFER_BYTES;
      do {
        n = pread(fd, work->buffer, want, (off_t)offset);
      } while (n < 0 && errno == EINTR);
      if (n <= 0) {
        rc = lc_collection_failure(error,
                                   n < 0 ? LC_ERR_TRANSPORT : LC_ERR_PROTOCOL,
                                   n < 0 ? NULL : "collection_corrupt",
                                   "failed to read collection codec unit");
        break;
      }
      crc = (unsigned long)crc32((uLong)crc, work->buffer, (uInt)n);
      rc = lc_cm_write(work->snapshot_fd, work->buffer, (size_t)n, error);
      if (rc == LC_OK)
        rc = lc_pouch_state_compaction_throttle(work->collection->pouch,
                                                (size_t)n, error);
      offset += (lc_u64)n;
      remaining -= (lc_u64)n;
    }
    if (rc == LC_OK && crc != unit.crc)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection codec unit checksum is invalid");
    if (rc == LC_OK)
      rc = lc_collection_unit_write_meta(work->snapshot_fd, (lc_u64)output,
                                         &unit, error);
    if (rc != LC_OK)
      break;
    copied.segment = work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT;
    copied.offset = (lc_u64)output;
    copied.bytes = unit.stored;
    rc = lc_collection_chunks_link(NULL, work->chunks, &copied, error);
    if (rc != LC_OK)
      break;
  }
  if (rc == LC_OK)
    rc = lc_collection_chunks_finish(NULL, work->chunks, error);
  if (rc == LC_OK) {
    *relocated = *source;
    relocated->stream_segment = work->chunks->root.segment;
    relocated->stream_offset = work->chunks->root.offset;
    relocated->stream_root_bytes = work->chunks->root.bytes;
    relocated->stream_height = work->chunks->height;
  }
  return rc;
}

static int lc_cm_copy(lc_compact *work, lc_error *error) {
  lc_collection_chunk_reader chunks = {0};
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_pouch_collection_record source;
  lc_pouch_collection_record copied;
  lc_pouch_collection_record relocated;
  lc_collection_handle *collection;
  lc_u64 ordinal;
  lc_u64 file_segment;
  lc_u64 file_end;
  lc_u64 body_segment, body_end;
  lc_u64 segment;
  char source_leaf[64];
  size_t count;
  size_t capacity;
  size_t i;
  int fd;
  int body_fd;
  int rc;
  collection = work->collection;
  ordinal = 0U;
  file_segment = file_end = 0U;
  body_segment = body_end = 0U;
  fd = -1;
  body_fd = -1;
  rc = LC_OK;
  while (rc == LC_OK && ordinal < work->count) {
    rc = lc_collection_metadata_enter(collection, 0, error);
    count = 0U;
    if (rc == LC_OK) {
      rc = lc_collection_order_refresh(&work->captured, error);
      capacity = work->count - ordinal < LC_COLLECTION_ORDER_PAGE_ROWS
                     ? (size_t)(work->count - ordinal)
                     : LC_COLLECTION_ORDER_PAGE_ROWS;
      if (rc == LC_OK)
        rc = lc_collection_order_rows(&work->captured, ordinal, rows, capacity,
                                      &count, error);
      lc_collection_metadata_leave(collection);
    }
    if (rc == LC_OK && count == 0U)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "captured collection order is incomplete");
    for (i = 0U; rc == LC_OK && i < count; ++i) {
      segment = rows[i].segment;
      rc = lc_collection_record_ref(collection, segment, rows[i].record_offset,
                                    &fd, &file_segment, &file_end, &source,
                                    error);
      if (rc == LC_OK)
        rc = lc_collection_record_at(collection, work->generation, &fd,
                                     &file_segment, &file_end, &source, error);
      segment = file_segment;
      if (rc != LC_OK)
        break;
      if (source.position != rows[i].position ||
          source.published_at != rows[i].published_at ||
          source.bytes != rows[i].bytes ||
          source.published_at > work->generation) {
        rc = lc_collection_failure(
            error, LC_ERR_PROTOCOL, "collection_corrupt",
            "collection compaction source differs from captured order");
        break;
      }
      if (rows[i].deleted_at != 0U && rows[i].deleted_at <= work->generation &&
          !source.deleted) {
        rc = lc_cm_roll(work, 2048U, error);
        if (rc == LC_OK)
          rc = lc_pouch_collection_record_delete(
              collection->pouch, work->snapshot_fd, &source, rows[i].deleted_at,
              &copied, error);
        if (rc == LC_OK)
          rc = lc_collection_project_record(
              &work->order, &work->ids, &copied,
              work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT, 1, error);
        continue;
      }
      if (rc == LC_OK && source.deleted &&
          (rows[i].deleted_at == 0U || rows[i].deleted_at > work->generation))
        rc =
            lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                  "live captured collection entry has no body");
      if (rc == LC_OK)
        rc = lc_collection_record_leaf(segment, source_leaf, error);
      if (rc == LC_OK && source.metadata_only)
        rc = lc_collection_record_body(collection, &source, &body_fd,
                                       &body_segment, &body_end, error);
      if (rc == LC_OK && source.stream_extents)
        rc = lc_collection_chunks_open(
            &chunks, &collection->allocator, collection->pouch->crypto,
            collection->id, collection->directory,
            source.metadata_only ? body_fd : fd, &source, error);
      if (rc == LC_OK && source.stream_extents)
        rc = lc_cm_stream(work, &chunks, &source, &relocated, error);
      if (rc == LC_OK)
        rc = lc_cm_roll(
            work, source.stream_extents ? 2048U : source.stored_bytes + 2048U,
            error);
      if (rc == LC_OK)
        rc = lc_pouch_collection_record_copy_source(
            collection->pouch, fd, source.metadata_only ? body_fd : fd,
            source.stream_extents ? &relocated : NULL, source_leaf, &source,
            work->snapshot_fd, work->leaf, work->buffer, LC_CM_BUFFER_BYTES,
            &copied, error);
      if (rc == LC_OK)
        rc = lc_collection_project_record(
            &work->order, &work->ids, &copied,
            work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT, 1, error);
    }
    ordinal += (lc_u64)count;
  }
  if (fd >= 0)
    (void)close(fd);
  if (body_fd >= 0)
    (void)close(body_fd);
  lc_collection_chunks_reader_close(&chunks);
  if (rc == LC_OK && work->captured_ids.count > work->count)
    rc = lc_collection_edit_compact(
        collection, &work->captured_ids, work->generation, lc_cm_output, work,
        &work->order, &work->ids, work->buffer, LC_CM_BUFFER_BYTES, error);
  if (rc == LC_OK && fsync(work->snapshot_fd) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to sync collection snapshot");
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        collection->directory, "failed to sync collection snapshot name",
        error);
  return rc;
}

static int lc_cm_publish(lc_compact *work, lc_error *error) {
  lc_collection_handle *collection;
  lc_collection_guard guard;
  lc_pouch_unix_seconds now;
  lc_u64 segment;
  int rc;
  collection = work->collection;
  rc = lc_collection_write_enter(&work->call, &guard, error);
  if (rc != LC_OK)
    return rc;
  if (collection->head.snapshot != work->previous_snapshot)
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "collection topology changed during exclusive compaction");
  if (rc == LC_OK &&
      strcmp(collection->order.query_id, work->captured.query_id) != 0)
    rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_recovered",
                               "collection recovery changed during compaction; "
                               "retry with the recovered view");
  if (rc == LC_OK &&
      collection->order.metadata_generation != work->metadata_generation)
    rc = lc_collection_failure(
        error, LC_ERR_PENDING, "collection_changed",
        "collection retention metadata changed during compaction; retry later");
  if (rc == LC_OK)
    rc = lc_collection_order_checkpoint(&work->order, work->tail, 0U, error);
  if (rc == LC_OK)
    rc = lc_collection_ids_checkpoint(
        &work->ids, lc_collection_binding_count(&work->order),
        work->order.generation, work->tail, 0U, error);
  for (segment = work->tail; rc == LC_OK && segment <= collection->head.active;
       ++segment)
    rc = lc_collection_replay_into(collection, &work->order, &work->ids,
                                   segment, 0U, 0, work->generation, 1, error);
  if (rc == LC_OK &&
      (work->order.edit_count != collection->order.edit_count ||
       work->order.count != collection->order.count ||
       work->order.generation != collection->order.generation ||
       work->order.live_count != collection->order.live_count ||
       work->order.upper_position != collection->order.upper_position))
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "compacted collection does not cover its committed tail");
  if (rc == LC_OK && (fsync(collection->active_fd) != 0 ||
                      fsync(work->order.fd) != 0 || fsync(work->ids.fd) != 0))
    rc = lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to sync collection compaction publication");
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    collection->head.snapshot = work->snapshot;
    collection->head.snapshot_extra_parts = work->parts - 1U;
    collection->head.snapshot_generation = work->generation;
    collection->head.tail_segment = work->tail;
    collection->head.tail_offset = 0U;
    collection->head.published_at = (lc_u64)now;
    collection->head_loaded = 0;
    work->commit_attempted = 1;
    rc = lc_collection_head_write(collection, error);
  }
  if (rc == LC_OK)
    rc = lc_cm_phase(work, LC_COLLECTION_COMPACT_HEAD_PUBLISHED, error);
  if (rc == LC_OK) {
    if (rename(work->order_path, collection->order_path) != 0)
      rc =
          lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                "failed to publish compacted collection order");
    else
      work->order_owned = 0;
  }
  if (rc == LC_OK)
    rc = lc_cm_phase(work, LC_COLLECTION_COMPACT_ORDER_PUBLISHED, error);
  if (rc == LC_OK) {
    if (rename(work->ids_path, collection->ids_path) != 0)
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to publish compacted collection identities");
    else
      work->ids_owned = 0;
  }
  if (rc == LC_OK)
    rc = lc_cm_phase(work, LC_COLLECTION_COMPACT_IDS_PUBLISHED, error);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        collection->directory,
        "failed to sync compacted collection projections", error);
  if (rc == LC_OK) {
    lc_collection_order_close(&collection->order);
    lc_collection_ids_close(&collection->ids);
    collection->order = work->order;
    work->order.fd = -1;
    lc_free_with_allocator(&work->ids.allocator, work->ids.path);
    work->ids.path = work->stable_ids_path;
    work->stable_ids_path = NULL;
    collection->ids = work->ids;
    memset(&work->ids, 0, sizeof(work->ids));
    work->ids.fd = -1;
  } else if (work->commit_attempted)
    collection->trusted = 0;
  lc_collection_metadata_leave(collection);
  lc_collection_write_leave(&guard);
  return rc;
}

static void lc_cm_release(lc_compact *work) {
  const lc_allocator *allocator;
  lc_u64 part;
  char *path, leaf[64];
  lc_error ignored;
  allocator = &work->collection->allocator;
  if (work->captured_ids.fd >= 0)
    close(work->captured_ids.fd);
  lc_collection_order_close(&work->captured);
  lc_collection_order_close(&work->order);
  lc_collection_ids_close(&work->ids);
  if (work->snapshot_fd >= 0)
    (void)close(work->snapshot_fd);
  if (work->pin_fd >= 0)
    (void)close(work->pin_fd);
  if (work->path_owned && !work->commit_attempted) {
    lc_error_init(&ignored);
    for (part = 0; part < work->parts; ++part) {
      path = NULL;
      if (lc_collection_record_path(work->collection,
                                    (work->snapshot + part) |
                                        LC_COLLECTION_SNAPSHOT_BIT,
                                    &path, leaf, &ignored) == LC_OK)
        (void)unlink(path);
      lc_free_with_allocator(allocator, path);
      lc_error_cleanup(&ignored);
    }
  }
  if (work->order_owned)
    (void)unlink(work->order_path);
  if (work->ids_owned)
    (void)unlink(work->ids_path);
  lc_free_with_allocator(allocator, work->chunks);
  lc_free_with_allocator(allocator, work->buffer);
  lc_free_with_allocator(allocator, work->path);
  lc_free_with_allocator(allocator, work->order_path);
  lc_free_with_allocator(allocator, work->ids_path);
  lc_free_with_allocator(allocator, work->stable_ids_path);
}

int lc_collection_compact(lc_collection *self, int force, int cleanup_only,
                          lc_pouch_maintenance_result *out, lc_error *error) {
  lc_compact work;
  lc_collection_guard guard;
  int busy;
  int selected;
  int rc;
  memset(&work, 0, sizeof(work));
  work.lock_fd = work.pin_fd = work.snapshot_fd = -1;
  work.captured_ids.fd = -1;
  work.captured.fd = work.order.fd = work.ids.fd = -1;
  rc = lc_collection_call_begin(self, 1, &work.call, error);
  if (rc != LC_OK)
    return rc;
  work.collection = work.call.collection;
  work.call.maintenance = 1;
  error = work.call.error;
  busy = selected = 0;
  rc = lc_cm_lock(&work, &busy, error);
  if (rc == LC_OK && !busy && !cleanup_only)
    rc = lc_cm_capture(&work, force, &selected, error);
  if (rc == LC_OK && selected)
    rc = lc_cm_phase(&work, LC_COLLECTION_COMPACT_CAPTURED, error);
  if (rc == LC_OK && selected)
    rc = lc_cm_create_projection(&work, error);
  if (rc == LC_OK && selected)
    rc = lc_cm_copy(&work, error);
  if (rc == LC_OK && selected)
    rc = lc_cm_phase(&work, LC_COLLECTION_COMPACT_COPIED, error);
  if (rc == LC_OK && selected)
    rc = lc_cm_publish(&work, error);
  if (rc == LC_ERR_PENDING && error->server_code != NULL &&
      strcmp(error->server_code, "collection_changed") == 0) {
    if (out != NULL)
      ++out->cleanup_pending_count;
    selected = 0;
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
  }
  if (rc == LC_OK && selected && out != NULL) {
    ++out->compacted_collection_count;
    out->compacted = 1;
    out->skipped = 0;
  }
  lc_cm_release(&work);
  if (rc == LC_OK && !busy) {
    rc = lc_collection_write_enter(&work.call, &guard, error);
    if (rc == LC_OK) {
      rc = lc_collection_cleanup_files(work.collection, out, error);
      lc_collection_metadata_leave(work.collection);
      lc_collection_write_leave(&guard);
    }
  } else if (busy && out != NULL)
    ++out->cleanup_pending_count;
  if (work.lock_fd >= 0)
    (void)close(work.lock_fd);
  lc_collection_call_end(&work.call);
  return rc;
}
