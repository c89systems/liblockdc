#ifndef LC_COLLECTION_ORDER_H
#define LC_COLLECTION_ORDER_H

#include "lc/lc.h"
#include "lc_intcompat.h"

/* Derived, seekable collection order. All calls require the owning collection's
 * metadata authority. No payload, per-entry allocation, or process-local cache
 * is stored here. Canonical replay repairs an interrupted projection update. */
#define LC_COLLECTION_ORDER_PAGE_ROWS 63U
#define LC_COLLECTION_ORDER_PAGE_BYTES 4096U
#define LC_COLLECTION_ORDER_ROOT_BYTES 160U

typedef struct lc_collection_order_row {
  lc_u64 position;
  lc_u64 published_at;
  lc_u64 deleted_at;
  lc_u64 segment;
  lc_u64 record_offset;
  lc_u64 bytes;
  lc_u64 stored_bytes;
} lc_collection_order_row;

typedef struct lc_collection_order {
  int fd;
  lc_u64 count;
  lc_u64 live_count;
  /* Conservative ordinal lower bound for the oldest live row. Canonical
   * replay rebuilds it; advancing it never makes a canonical deletion durable.
   */
  lc_u64 first_live;
  lc_u64 collection_retention_generation;
  lc_unix_seconds collection_retention;
  lc_u64 generation;
  lc_u64 upper_position;
  lc_u64 replay_segment;
  lc_u64 replay_offset;
  lc_u64 snapshot;
  lc_u64 reclaimable_bytes;
  /* Last entry metadata mutation. Compaction retries if it changes during
   * copying, so its new topology never leaves references to retired inputs. */
  lc_u64 metadata_generation;
  lc_u64 edit_count;
  /* A canonical edit exists, but its derived page updates may be incomplete. */
  int edit_in_progress;
  /* Conservative derived hint. False proves no entry expiry needs scanning;
   * canonical rebuild/compaction recomputes it without a separate TTL store. */
  int retention_present;
  /* Fresh for canonical recovery, preserved by compaction. Derived query
   * artifacts from lost NoSync history must never match reused positions. */
  char query_id[LC_XID_STRING_SIZE];
} lc_collection_order;

typedef struct lc_collection_order_iter {
  lc_u64 next;
  lc_u64 count;
  lc_u64 generation;
  lc_u64 after;
  lc_u64 through;
  int reverse;
  int done;
  /* One read's bounded metadata page, not a collection cache. It also gives
   * the caller a short authority scope independent of payload callbacks. */
  int page_loaded;
  lc_u64 page_number;
  unsigned char page[LC_COLLECTION_ORDER_PAGE_BYTES];
} lc_collection_order_iter;

#ifdef LOCKDC_TEST_BUILD
int lc_collection_edit_page_test(lc_error *);
#endif

/* Opens an owned descriptor. A missing file is created only with create != 0.
 * A corrupt existing index is reported, never silently replaced. */
int lc_collection_order_open(const char *path, int create,
                             lc_collection_order *out, lc_error *error);
void lc_collection_order_close(lc_collection_order *order);
int lc_collection_order_refresh(lc_collection_order *order, lc_error *error);
int lc_collection_order_checkpoint(lc_collection_order *order,
                                   lc_u64 replay_segment, lc_u64 replay_offset,
                                   lc_error *error);
/* Caller has already published the referenced canonical mutation. The replay
 * cursor points immediately after it. These functions do not fsync derived
 * bytes or claim durability independently of the canonical log. */
int lc_collection_order_append(lc_collection_order *order,
                               const lc_collection_order_row *row,
                               lc_u64 replay_segment, lc_u64 replay_offset,
                               lc_error *error);
/* Rebuilds a newly owned projection from canonical snapshot records in
 * position order. Deletion generations need not follow that order; they are
 * retained in each row and the projection watermark takes their maximum.
 * Never use this to publish a new application append. */
int lc_collection_order_append_recovered(lc_collection_order *order,
                                         const lc_collection_order_row *row,
                                         lc_u64 replay_segment,
                                         lc_u64 replay_offset, lc_error *error);
int lc_collection_order_delete(lc_collection_order *order, lc_u64 position,
                               lc_u64 generation, lc_u64 replay_segment,
                               lc_u64 replay_offset, lc_error *error);
/* Caller owns metadata authority; dirty publication precedes all page writes.
 * A failed mutation requires canonical rebuild, never best-effort rollback. */
int lc_collection_order_edit_begin(lc_collection_order *, lc_u64, lc_error *);
int lc_collection_order_cut(lc_collection_order *, const lc_allocator *,
                            const lc_collection_edit_range *, size_t, lc_u64,
                            lc_u64 *, lc_error *);
/* Counts the current view without modifying it. Whole selected pages use
 * their live summary; only range boundaries require decoding individual rows.
 */
int lc_collection_order_count_ranges(lc_collection_order *,
                                     const lc_allocator *,
                                     const lc_collection_edit_range *, size_t,
                                     lc_u64 *, lc_error *);
/* Repoints a live row to canonical STATE_META without changing order, body
 * identity, page counts or deletion visibility. Snapshot replay can encounter
 * metadata generations out of position order; the watermark takes their max. */
int lc_collection_order_metadata(lc_collection_order *order,
                                 const lc_collection_order_row *row,
                                 lc_u64 generation, lc_u64 replay_segment,
                                 lc_u64 replay_offset, lc_error *error);
int lc_collection_order_find(lc_collection_order *order, lc_u64 position,
                             lc_collection_order_row *row, int *found,
                             lc_error *error);
/* Current oldest live row, skipping deleted pages from a derived lower bound.
 * Caller holds exclusive metadata authority. A subsequent delete/checkpoint
 * persists the improved hint along with the existing projection watermark. */
int lc_collection_order_oldest(lc_collection_order *order,
                               lc_collection_order_row *row, int *found,
                               lc_error *error);
/* Captures current cardinality, not a body snapshot. The caller pins canonical
 * files and supplies its captured mutation generation for deletion visibility.
 * Bounds are after-exclusive and through-inclusive in either direction. */
int lc_collection_order_seek(lc_collection_order *order, lc_u64 after,
                             lc_u64 through, lc_u64 generation, int reverse,
                             lc_collection_order_iter *iter, lc_error *error);
int lc_collection_order_next(lc_collection_order *order,
                             lc_collection_order_iter *iter,
                             lc_collection_order_row *row, int *found,
                             lc_error *error);
/* Visits at most one physical page, returning visible rows in traversal order.
 * An empty batch can still advance examined across a deleted page. Callers can
 * release metadata authority and observe cancellation between these batches.
 * examined is zero when no in-range boundary was examined. */
int lc_collection_order_batch(lc_collection_order *order,
                              lc_collection_order_iter *iter,
                              lc_collection_order_row *rows, size_t capacity,
                              size_t *count, lc_u64 *examined, lc_error *error);
/* Bounded physical-order metadata batch, including tombstones. Caller holds
 * metadata authority and pins the referenced canonical generation. */
int lc_collection_order_rows(lc_collection_order *order, lc_u64 first,
                             lc_collection_order_row *rows, size_t capacity,
                             size_t *count, lc_error *error);

#endif
