#include "lc_collection_internal.h"

#include <string.h>

/* Runs on the root's existing maintenance cadence. Metadata-only batches do
 * not open a body source or hide an expired entry from ordinary reads. Each
 * selected deletion rechecks its deadline under current mutation authority. */
int lc_collection_retention_maintain(lc_collection *self, int *dropped,
                                     lc_pouch_maintenance_result *out,
                                     lc_error *error) {
  lc_collection_call call, scope;
  lc_collection_guard guard;
  lc_collection_handle *collection;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_u64 due[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_pouch_collection_record record;
  lc_collection_delete_res deleted;
  lc_pouch_unix_seconds now, parent;
  lc_u64 captured, ordinal, trim_budget;
  lc_collection_input input;
  size_t count, selected, i;
  int rooted, rc;
  lc_collection_lock commit_pin;
  *dropped = 0;
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  collection = call.collection;
  error = call.error;
  rc = lc_collection_root_begin(collection->pouch, &scope, &guard, error);
  rooted = rc == LC_OK;
  captured = 0U;
  trim_budget = 0U;
  parent = 0;
  memset(&input, 0, sizeof(input));
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_collection_capture_enter(collection, &commit_pin, error);
    if (rc == LC_OK) {
      rc = lc_collection_refresh_view(collection, error);
      if (rc == LC_OK) {
        captured =
            collection->order.retention_present ? collection->order.count : 0U;
        parent = lc_collection_retention_deadline(collection);
        if (collection->head.max_entries != 0U &&
            collection->order.live_count > collection->head.max_entries) {
          trim_budget =
              collection->order.live_count - collection->head.max_entries;
        }
      }
      lc_collection_capture_leave(collection, &commit_pin);
    }
  }
  if (rc == LC_OK && parent > 0 && parent <= now) {
    rc = lc_collection_expire(self, now, dropped, error);
    if (rc == LC_OK && *dropped && out != NULL) {
      ++out->retention_expired_count;
      ++out->cleanup_pending_count;
    }
  }
  for (ordinal = 0U; rc == LC_OK && !*dropped && ordinal < captured;
       ordinal += LC_COLLECTION_ORDER_PAGE_ROWS) {
    count = selected = 0U;
    rc = lc_collection_metadata_enter(collection, 0, error);
    if (rc == LC_OK) {
      rc = lc_collection_refresh_view(collection, error);
      if (rc == LC_OK)
        rc = lc_collection_order_rows(&collection->order, ordinal, rows,
                                      LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                      error);
      for (i = 0U; rc == LC_OK && i < count; ++i) {
        if (rows[i].deleted_at != 0U)
          continue;
        rc = lc_collection_record_ref(collection, rows[i].segment,
                                      rows[i].record_offset, &input, &record,
                                      error);
        if (rc == LC_OK &&
            (record.deleted || record.position != rows[i].position ||
             record.published_at != rows[i].published_at ||
             record.bytes != rows[i].bytes))
          rc = lc_collection_failure(
              error, LC_ERR_PROTOCOL, "collection_corrupt",
              "collection retention metadata differs from its ordered state");
        if (rc == LC_OK) {
          if (out != NULL)
            ++out->retention_scanned_count;
          if (record.retention > 0 && record.retention <= now)
            due[selected++] = record.position;
        }
      }
      lc_collection_metadata_leave(collection);
    }
    for (i = 0U; rc == LC_OK && i < selected; ++i) {
      rc = lc_collection_expire_entry(self, due[i], now, &deleted, error);
      if (rc == LC_OK && deleted.deleted && !deleted.already_deleted &&
          out != NULL) {
        ++out->retention_expired_count;
        ++out->retention_deleted_state_count;
      }
    }
  }
  lc_collection_input_close(&input);
  /* Age deletion runs first so it can satisfy the cap without unnecessarily
   * deleting an older, unexpired entry. Each trim selects the current oldest
   * and rechecks policy/count under mutation authority, releasing it after
   * each deletion. The captured surplus bounds work even if new appends keep
   * arriving, without spreading an existing backlog over many worker ticks. */
  for (ordinal = 0U; rc == LC_OK && !*dropped && ordinal < trim_budget;
       ++ordinal) {
    rc = lc_collection_trim_entry(self, &deleted, error);
    if (rc == LC_OK && !deleted.deleted)
      break;
    if (rc == LC_OK && !deleted.already_deleted && out != NULL)
      ++out->retention_deleted_state_count;
  }
  if (rooted)
    lc_collection_root_end(&scope, &guard);
  lc_collection_call_end(&call);
  return rc;
}
