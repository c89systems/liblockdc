#ifndef LC_COLLECTION_QUERY_H
#define LC_COLLECTION_QUERY_H

#include "lc_collection_index.h"
#include "lc_collection_internal.h"
#include <lql/lql.h>

typedef struct lc_collection_query_plan {
  lc_allocator allocator;
  lc_pouch_index_term_key *keys;
  size_t count;
  int presence;
  int prefix;
} lc_collection_query_plan;
/* Optional acceleration for a complete supported path/predicate. A zero-count
 * plan preserves full LQL evaluation, including unsupported index shapes. */
int lc_collection_query_plan_init(lc_collection_query_plan *plan,
                                  const lc_allocator *allocator, lql *runtime,
                                  const lql_selector *selector,
                                  lc_error *error);
void lc_collection_query_plan_cleanup(lc_collection_query_plan *plan);

/* Derived, bounded metadata; the application state is never copied here.
 * Store publishes one complete protected artifact atomically. Load validates
 * the canonical page binding; absent/corrupt/incompatible artifacts are misses
 * requiring a state scan. Allocation failure remains explicit. Both require
 * a retained collection/root operation, outside metadata authority. */
int lc_collection_query_store(lc_collection_handle *collection,
                              lc_collection_index *index, lc_error *error);
int lc_collection_query_load(lc_collection_handle *collection,
                             const char *query_id, lc_u64 ordinal,
                             unsigned long row_hash, const unsigned char *proof,
                             lc_collection_index *index, int *found,
                             lc_error *error);
const char *lc_collection_query_epoch(const lc_collection_handle *,
                                      const lc_collection_order *);
unsigned long lc_collection_query_row_hash(const lc_collection_order_row *rows,
                                           size_t count);
/* Captures and indexes one physical page outside mutation authority. Validates
 * complete canonical bodies before publishing derived terms. Appends/deletes
 * may proceed while it reads; the captured canonical generation remains pinned.
 * This is bounded maintenance, never a collection-wide read prerequisite. */
int lc_collection_query_build(lc_collection_call *call, lc_u64 ordinal,
                              lc_error *error);
int lc_collection_query_maintain(lc_collection *collection, lc_error *error);
/* Reclaims optional index epochs and abandoned builds through their physical
 * owner. Native publishers retain temporary-file locks; sealed container
 * builders retain owned-stream pins. Missing artifacts remain state-scan
 * fallbacks for readers. */
int lc_collection_query_cleanup(lc_collection *collection,
                                lc_pouch_maintenance_result *out,
                                lc_error *error);
#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_collection_query_hook_fn)(void *, lc_collection_handle *,
                                           const char *, lc_error *);
extern lc_collection_query_hook_fn lc_collection_query_test_hook;
extern void *lc_collection_query_test_context;
#endif

#endif
