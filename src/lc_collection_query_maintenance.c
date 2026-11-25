#include "lc_collection_query.h"

#include <string.h>

int lc_collection_query_maintain(lc_collection *self, lc_error *error) {
  lc_collection_call call, root_scope;
  lc_collection_guard guard;
  lc_collection_handle *collection;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_collection_index index;
  char query_id[LC_XID_STRING_SIZE];
  unsigned char proof[LC_COLLECTION_PAGE_PROOF_BYTES];
  lc_u64 captured, ordinal, generation, deleted;
  size_t count, i;
  int rooted, found, rc;
  lc_collection_lock commit_pin;
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  collection = call.collection;
  error = call.error;
  if (!collection->pouch->query_indexing_enabled) {
    lc_collection_call_end(&call);
    return LC_OK;
  }
  rc = lc_collection_root_begin(collection->pouch, &root_scope, &guard, error);
  rooted = rc == LC_OK;
  captured = generation = 0U;
  if (rc == LC_OK) {
    rc = lc_collection_capture_enter(collection, &commit_pin, error);
    if (rc == LC_OK) {
      captured = collection->order.count;
      lc_collection_capture_leave(collection, &commit_pin);
    }
  }
  for (ordinal = 0U; rc == LC_OK && ordinal < captured;
       ordinal += LC_COLLECTION_ORDER_PAGE_ROWS) {
    count = 0U;
    rc = lc_collection_metadata_enter(collection, 0, error);
    if (rc == LC_OK) {
      rc = lc_collection_refresh_view(collection, error);
      if (rc == LC_OK) {
        generation = collection->order.generation;
        memcpy(query_id,
               lc_collection_query_epoch(collection, &collection->order),
               sizeof(query_id));
        rc = lc_collection_order_rows(&collection->order, ordinal, rows,
                                      LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                      proof, error);
      }
      lc_collection_metadata_leave(collection);
    }
    if (rc != LC_OK || count == 0U)
      continue;
    deleted = 0U;
    for (i = 0U; i < count; ++i)
      if (rows[i].deleted_at != 0U && rows[i].deleted_at <= generation)
        deleted |= (lc_u64)1U << i;
    rc = lc_collection_query_load(collection, query_id, ordinal,
                                  lc_collection_query_row_hash(rows, count),
                                  proof, &index, &found, error);
    if (rc == LC_OK && (!found || index.rows != count)) {
      lc_collection_index_cleanup(&index);
      rc = lc_collection_query_build(&call, ordinal, error);
    } else {
      if (rc == LC_OK && (index.covered & deleted) != 0U) {
        rc = lc_collection_index_prune(&index, deleted, error);
        if (rc == LC_OK)
          rc = lc_collection_query_store(collection, &index, error);
      }
      lc_collection_index_cleanup(&index);
    }
  }
  if (rooted)
    lc_collection_root_end(&root_scope, &guard);
  lc_collection_call_end(&call);
  return rc;
}
