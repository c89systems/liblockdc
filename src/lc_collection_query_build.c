#include "lc_collection_payload.h"
#include "lc_collection_query.h"

#include <string.h>
#include <unistd.h>
#include <zlib.h>

unsigned long lc_collection_query_row_hash(const lc_collection_order_row *rows,
                                           size_t count) {
  unsigned char bytes[24];
  lc_u64 values[3];
  unsigned long crc;
  size_t i;
  unsigned int field, byte;
  crc = 0UL;
  for (i = 0U; i < count; ++i) {
    values[0] = rows[i].position;
    values[1] = rows[i].published_at;
    values[2] = rows[i].bytes;
    for (field = 0U; field < 3U; ++field) {
      for (byte = 0U; byte < 8U; ++byte) {
        bytes[field * 8U + byte] = (unsigned char)values[field];
        values[field] >>= 8U;
      }
    }
    crc = (unsigned long)crc32(crc, bytes, sizeof(bytes));
  }
  return crc;
}

int lc_collection_query_build(lc_collection_call *call, lc_u64 ordinal,
                              lc_error *error) {
  lc_collection_call root_scope;
  lc_collection_guard root_guard;
  lc_collection_handle *collection;
  lc_collection_index index;
  lc_collection_payload payload;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_pouch_collection_record record;
  lc_u64 generation;
  lc_collection_input record_input;
  char query_id[LC_XID_STRING_SIZE];
  size_t count, i;
  int rooted, rc;
  lc_collection_lock view_pin, commit_pin;
  if (ordinal % LC_COLLECTION_ORDER_PAGE_ROWS != 0U)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection query build requires a physical page boundary");
  collection = call->collection;
  memset(&index, 0, sizeof(index));
  memset(&payload, 0, sizeof(payload));
  memset(&view_pin, 0, sizeof(view_pin));
  memset(&record_input, 0, sizeof(record_input));
  generation = 0U;
  count = 0U;
  rc = lc_collection_root_begin(collection->pouch, &root_scope, &root_guard,
                                error);
  rooted = rc == LC_OK;
  if (rooted)
    error = root_scope.error;
  if (rc == LC_OK) {
    rc = lc_collection_capture_enter(collection, &commit_pin, error);
    if (rc == LC_OK) {
      rc = lc_collection_refresh_view(collection, error);
      if (rc == LC_OK && ordinal < collection->order.count) {
        generation = collection->order.generation;
        memcpy(query_id, collection->order.query_id, sizeof(query_id));
        rc = lc_collection_order_rows(&collection->order, ordinal, rows,
                                      LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                      error);
        if (rc == LC_OK)
          rc = lc_collection_read_pin(collection, collection->head.snapshot,
                                      &view_pin, error);
      }
      lc_collection_capture_leave(collection, &commit_pin);
    }
  }
  if (rc == LC_OK && count != 0U)
    rc = lc_collection_index_init(
        &index, &collection->allocator, collection->id, query_id, ordinal,
        lc_collection_query_row_hash(rows, count), error);
  for (i = 0U; rc == LC_OK && i < count; ++i) {
    if (rows[i].deleted_at != 0U && rows[i].deleted_at <= generation) {
      rc = lc_collection_index_add(&index, NULL, error);
      continue;
    }
    rc = lc_collection_record_ref(collection, rows[i].segment,
                                  rows[i].record_offset, &record_input, &record,
                                  error);
    if (rc == LC_OK)
      rc = lc_collection_record_at(collection, generation, &record_input,
                                   &record, error);
    if (rc == LC_OK && (record.deleted || record.position != rows[i].position ||
                        record.published_at != rows[i].published_at ||
                        record.bytes != rows[i].bytes))
      rc = lc_collection_failure(
          error, LC_ERR_PROTOCOL, "collection_corrupt",
          "collection query order points to a different canonical entry");
    if (rc == LC_OK)
      rc = lc_collection_payload_open(&payload, collection, record_input.io,
                                      &record, error);
    if (rc == LC_OK)
      rc = lc_collection_index_add(&index, &payload.source, error);
    if (rc == LC_OK)
      rc = lc_collection_payload_finish(&payload, error);
    lc_collection_payload_reset(&payload);
  }
  if (rc == LC_OK && count != 0U)
    rc = lc_collection_query_store(collection, &index, error);
  lc_collection_payload_cleanup(&payload);
  lc_collection_index_cleanup(&index);
  lc_collection_input_close(&record_input);
  lc_collection_lock_leave(&view_pin);
  if (rooted)
    lc_collection_root_end(&root_scope, &root_guard);
  return rc;
}
