#ifndef POUCH_SPACE_QUERY_INDEX_H
#define POUCH_SPACE_QUERY_INDEX_H
#include "lc_pouch_index_store.h"
#include "root.h"
/* Borrowed ps_root context. Namespace revisions select their generation
 * and manifest together. */
extern const lc_pouch_index_store_ops ps_query_index_store_ops;
/* Captured collection pages retain their exact immutable extents and root. */
int ps_query_page_open(ps_namespace *, uint64_t, uint64_t,
                       lc_pouch_index_input *, lc_error *);
void ps_query_page_close(lc_pouch_index_input *);
int ps_query_page_replace(ps_namespace *, uint64_t, uint64_t, uint64_t,
                          lc_pouch_index_produce_fn, void *,
                          lc_pouch_index_select_fn, void *, lc_error *);
int ps_query_catalog_reclaim(ps_space *, uint64_t, unsigned long *, lc_error *);
int ps_query_artifacts_reclaim(ps_space *, uint64_t, lc_error *);
int ps_query_values_reclaim(ps_space *, uint64_t, lc_error *);
#endif
