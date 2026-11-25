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
  lc_collection_projection_build build;
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
  unsigned char *buffer;
  lc_collection_lock lock_pin, view_pin;
  lc_collection_segment output;
  int created;
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
  int rc = lc_collection_lock_enter(work->collection->authority,
                                    LC_COLLECTION_AUTH_COMPACTION, 0U, 1, 0,
                                    &work->lock_pin, error);
  *busy = rc == LC_ERR_PENDING;
  if (*busy) {
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
  }
  return rc;
}

static int lc_cm_capture(lc_compact *work, int force, int *selected,
                         lc_error *error) {
  lc_collection_handle *collection;
  lc_collection_guard guard;
  int rc, busy = 0;
  lc_collection_lock stream_pin;
  memset(&stream_pin, 0, sizeof(stream_pin));
  collection = work->collection;
  *selected = 0;
  rc = lc_collection_write_enter(&work->call, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_collection_lock_enter(collection->authority,
                                LC_COLLECTION_AUTH_STREAMS, 0U, 1, 0,
                                &stream_pin, error);
  if (rc == LC_ERR_PENDING) {
    busy = 1;
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
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
                                  &work->view_pin, error);
    if (rc == LC_OK)
      rc = lc_collection_order_capture(&collection->order, &work->captured,
                                       error);
    if (rc == LC_OK)
      rc = lc_collection_ids_capture(&collection->ids, &work->captured_ids,
                                     error);
    if (rc == LC_OK) {
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
  lc_collection_lock_leave(&stream_pin);
  lc_collection_metadata_leave(collection);
  lc_collection_write_leave(&guard);
  return rc;
}

static int lc_cm_projection_phase(void *context, int phase, lc_error *error) {
  return lc_cm_phase(context,
                     phase == LC_COLLECTION_PROJECTION_ORDER
                         ? LC_COLLECTION_COMPACT_ORDER_PUBLISHED
                         : LC_COLLECTION_COMPACT_IDS_PUBLISHED,
                     error);
}
static int lc_cm_create_projection(lc_compact *work, lc_error *error) {
  lc_collection_handle *c = work->collection;
  int rc = lc_collection_record_leaf(
      work->snapshot | LC_COLLECTION_SNAPSHOT_BIT, work->leaf, error);
  if (rc == LC_OK)
    rc = c->segments.open(c->segments.context,
                          work->snapshot | LC_COLLECTION_SNAPSHOT_BIT, 1,
                          &work->output, error);
  if (rc != LC_OK)
    return rc;
  work->created = 1;
  work->part_snapshot = work->snapshot;
  work->parts = 1U;
  work->buffer = lc_alloc_with_allocator(&c->allocator, LC_CM_BUFFER_BYTES);
  work->chunks =
      lc_calloc_with_allocator(&c->allocator, 1U, sizeof(*work->chunks));
  if (!work->buffer || !work->chunks)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate bounded compaction resources");
  rc = c->projections.begin(c->projections.context, &work->build, error);
  if (rc == LC_OK) {
    memcpy(work->build.order.query_id, work->captured.query_id,
           LC_XID_STRING_SIZE);
    work->build.order.snapshot = work->build.ids.snapshot = work->snapshot;
    work->build.on_publish = lc_cm_projection_phase;
    work->build.publish_context = work;
  }
  return rc;
}

/* Snapshot parts are final canonical files. Only the active part and the
 * caller's bounded copy buffer are writable; no whole-entry output exists. */
static int lc_cm_roll(lc_compact *work, lc_u64 required, lc_error *error) {
  lc_collection_handle *c = work->collection;
  lc_collection_guard guard;
  lc_collection_segment fresh;
  lc_u64 next = work->part_snapshot + 1U, end;
  char leaf[64];
  int rc = lc_pouch_record_io_tail(work->output.io, &end, error);
  if (rc != LC_OK || !end ||
      (end < c->pouch->segment_target_bytes &&
       required <= c->pouch->segment_target_bytes - end))
    return rc;
  rc = lc_collection_segment_seal(&work->output, error);
  if (rc != LC_OK)
    return rc;
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
  memset(&fresh, 0, sizeof(fresh));
  if (rc == LC_OK)
    rc = lc_collection_record_leaf(next | LC_COLLECTION_SNAPSHOT_BIT, leaf,
                                   error);
  if (rc == LC_OK)
    rc = c->segments.open(c->segments.context,
                          next | LC_COLLECTION_SNAPSHOT_BIT, 1, &fresh, error);
  if (rc == LC_OK) {
    lc_collection_segment_close(&work->output);
    work->output = fresh;
    strcpy(work->leaf, leaf);
    work->part_snapshot = next;
    ++work->parts;
  }
  return rc;
}

static int lc_cm_output(void *context, lc_u64 required,
                        lc_pouch_record_io *output, const char **leaf,
                        lc_u64 *identity, lc_error *error) {
  lc_compact *work = context;
  int rc = lc_cm_roll(work, required, error);
  if (rc == LC_OK) {
    *output = work->output.io;
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
        work->output.unit_io, work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT,
        level, tuples, count, ref, error);
  return rc;
}

static int lc_cm_write(lc_compact *work, const void *bytes, size_t size,
                       lc_error *error) {
  struct iovec part;
  part.iov_base = (void *)bytes;
  part.iov_len = size;
  return lc_pouch_record_io_append(work->output.io, &part, 1, error);
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
  lc_u64 output;
  unsigned long crc;
  lc_pouch_record_io input;
  struct iovec part;
  size_t want;
  int found, rc;
  memset(work->chunks, 0, sizeof(*work->chunks));
  work->chunks->append_index = lc_cm_index;
  work->chunks->index_context = work;
  for (;;) {
    rc = lc_collection_chunks_next_unit(reader, &found, &input, &ref, &unit,
                                        error);
    if (rc != LC_OK || !found)
      break;
    rc = lc_cm_roll(work, LC_COLLECTION_UNIT_HEADER + unit.stored, error);
    if (rc != LC_OK)
      break;
    rc = lc_pouch_record_io_tail(work->output.io, &output, error);
    if (rc != LC_OK)
      break;
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
    rc = lc_cm_write(work, prefix, sizeof(prefix), error);
    remaining = unit.stored;
    offset = ref.offset + LC_COLLECTION_UNIT_HEADER;
    crc = (unsigned long)crc32(0L, Z_NULL, 0);
    while (rc == LC_OK && remaining) {
      want = remaining < LC_CM_BUFFER_BYTES ? (size_t)remaining
                                            : LC_CM_BUFFER_BYTES;
      part.iov_base = work->buffer;
      part.iov_len = want;
      rc = lc_pouch_record_io_read(input, offset, &part, 1, error);
      if (rc != LC_OK)
        break;
      crc = (unsigned long)crc32((uLong)crc, work->buffer, (uInt)want);
      rc = lc_cm_write(work, work->buffer, want, error);
      if (rc == LC_OK)
        rc = lc_pouch_state_compaction_throttle(work->collection->pouch, want,
                                                error);
      offset += want;
      remaining -= want;
    }
    if (rc == LC_OK && crc != unit.crc)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "collection codec unit checksum is invalid");
    if (rc == LC_OK)
      rc = lc_collection_unit_write_meta(work->output.unit_io, (lc_u64)output,
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
  lc_collection_order_row *rows;
  lc_pouch_collection_record source;
  lc_pouch_collection_record copied;
  lc_pouch_collection_record relocated;
  lc_collection_handle *collection;
  lc_u64 ordinal;
  lc_collection_input input, body_input;
  lc_u64 segment;
  char source_leaf[64];
  size_t count;
  size_t capacity;
  size_t i;
  int rc;
  collection = work->collection;
  rows = lc_alloc_with_allocator(&collection->allocator,
                                 LC_COLLECTION_ORDER_PAGE_ROWS * sizeof(*rows));
  if (rows == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate compaction row page");
  ordinal = 0U;
  memset(&input, 0, sizeof(input));
  memset(&body_input, 0, sizeof(body_input));
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
                                      &count, NULL, error);
      lc_collection_metadata_leave(collection);
    }
    if (rc == LC_OK && count == 0U)
      rc = lc_collection_failure(error, LC_ERR_PROTOCOL, "collection_corrupt",
                                 "captured collection order is incomplete");
    for (i = 0U; rc == LC_OK && i < count; ++i) {
      segment = rows[i].segment;
      rc = lc_collection_record_ref(collection, segment, rows[i].record_offset,
                                    &input, &source, error);
      if (rc == LC_OK)
        rc = lc_collection_record_at(collection, work->generation, &input,
                                     &source, error);
      segment = input.segment;
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
              collection->pouch, work->output.io, &source, rows[i].deleted_at,
              &copied, error);
        if (rc == LC_OK)
          rc = lc_collection_project_record(
              &work->build.order, &work->build.ids, &copied,
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
        rc = lc_collection_record_body(collection, &source, &body_input, error);
      if (rc == LC_OK && source.stream_extents)
        rc = lc_collection_chunks_open(
            &chunks, &collection->allocator, collection->pouch->crypto,
            collection->id, collection->reads,
            source.metadata_only ? body_input.io : input.io, &source, error);
      if (rc == LC_OK && source.stream_extents)
        rc = lc_cm_stream(work, &chunks, &source, &relocated, error);
      if (rc == LC_OK)
        rc = lc_cm_roll(
            work, source.stream_extents ? 2048U : source.stored_bytes + 2048U,
            error);
      if (rc == LC_OK)
        rc = lc_pouch_collection_record_copy_source(
            collection->pouch, input.io,
            source.metadata_only ? body_input.io : input.io,
            source.stream_extents ? &relocated : NULL, source_leaf, &source,
            work->output.io, work->leaf, work->buffer, LC_CM_BUFFER_BYTES,
            &copied, error);
      if (rc == LC_OK)
        rc = lc_collection_project_record(
            &work->build.order, &work->build.ids, &copied,
            work->part_snapshot | LC_COLLECTION_SNAPSHOT_BIT, 1, error);
    }
    ordinal += (lc_u64)count;
  }
  lc_collection_input_close(&input);
  lc_collection_input_close(&body_input);
  lc_free_with_allocator(&collection->allocator, rows);
  lc_collection_chunks_reader_close(&chunks);
  if (rc == LC_OK && work->captured_ids.count > work->count)
    rc = lc_collection_edit_compact(collection, &work->captured_ids,
                                    work->generation, lc_cm_output, work,
                                    &work->build.order, &work->build.ids,
                                    work->buffer, LC_CM_BUFFER_BYTES, error);
  if (rc == LC_OK)
    rc = lc_collection_segment_seal(&work->output, error);
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
    rc = lc_collection_order_checkpoint(&work->build.order, work->tail, 0U,
                                        error);
  if (rc == LC_OK)
    rc = lc_collection_ids_checkpoint(
        &work->build.ids, lc_collection_binding_count(&work->build.order),
        work->build.order.generation, work->tail, 0U, error);
  for (segment = work->tail; rc == LC_OK && segment <= collection->head.active;
       ++segment)
    rc = lc_collection_replay_into(collection, &work->build.order,
                                   &work->build.ids, segment, 0U, 0,
                                   work->generation, 1, error);
  if (rc == LC_OK &&
      (work->build.order.edit_count != collection->order.edit_count ||
       work->build.order.count != collection->order.count ||
       work->build.order.generation != collection->order.generation ||
       work->build.order.live_count != collection->order.live_count ||
       work->build.order.upper_position != collection->order.upper_position))
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, "collection_corrupt",
        "compacted collection does not cover its committed tail");
  if (rc == LC_OK)
    rc = lc_collection_segment_force(&collection->active, error);
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
  if (rc == LC_OK)
    rc = collection->projections.publish(collection->projections.context,
                                         &work->build, error);
  if (rc == LC_OK) {
    lc_collection_order_close(&collection->order);
    lc_collection_ids_close(&collection->ids);
    collection->order = work->build.order;
    collection->ids = work->build.ids;
    memset(&work->build.order, 0, sizeof(work->build.order));
    memset(&work->build.ids, 0, sizeof(work->build.ids));
    work->build.order.fd = work->build.ids.fd = -1;
  } else if (work->commit_attempted)
    collection->trusted = 0;
  lc_collection_metadata_leave(collection);
  lc_collection_write_leave(&guard);
  return rc;
}

static void lc_cm_release(lc_compact *work) {
  lc_collection_handle *c = work->collection;
  lc_error ignored;
  lc_u64 part;
  lc_collection_ids_close(&work->captured_ids);
  lc_collection_order_close(&work->captured);
  lc_collection_segment_close(&work->output);
  lc_collection_lock_leave(&work->view_pin);
  if (work->created && !work->commit_attempted) {
    lc_error_init(&ignored);
    for (part = 0U; part < work->parts; ++part) {
      (void)c->layout.discard(
          c->layout.context,
          (work->snapshot + part) | LC_COLLECTION_SNAPSHOT_BIT, &ignored);
      lc_error_cleanup(&ignored);
      lc_error_init(&ignored);
    }
    lc_error_cleanup(&ignored);
  }
  c->projections.cleanup(c->projections.context, &work->build);
  lc_free_with_allocator(&c->allocator, work->chunks);
  lc_free_with_allocator(&c->allocator, work->buffer);
}

int lc_collection_compact(lc_collection *self, int force, int cleanup_only,
                          lc_pouch_maintenance_result *out, lc_error *error) {
  lc_compact work;
  lc_collection_guard guard;
  int busy;
  int selected;
  int rc;
  memset(&work, 0, sizeof(work));
  work.captured_ids.fd = -1;
  work.captured.fd = work.build.order.fd = work.build.ids.fd = -1;
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
  lc_collection_lock_leave(&work.lock_pin);
  lc_collection_call_end(&work.call);
  return rc;
}
