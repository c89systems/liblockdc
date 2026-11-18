#ifndef POUCH_SPACE_QUERY_INDEX_H
#define POUCH_SPACE_QUERY_INDEX_H
#include "lc_pouch_index_store.h"
#include "root.h"
/* Borrowed ps_root context. Namespace revisions use immutable typed values;
 * generation and manifest are prepared privately and selected together. */
extern const lc_pouch_index_store_ops ps_query_index_store_ops;
/* Shared sealed-artifact owner. Catalog scope is the actual namespace or
 * collection owner; the binary domain identity is never a synthetic namespace.
 * Open retains the namespace/root independently until close. */
int ps_query_artifact_open(ps_namespace *, uint64_t, const void *, size_t,
                           lc_pouch_index_input *, lc_error *);
void ps_query_artifact_close(ps_root *, lc_pouch_index_input *);
int ps_query_artifact_replace(ps_namespace *, uint64_t, const void *, size_t,
                              uint64_t, lc_pouch_index_produce_fn, void *,
                              lc_pouch_index_select_fn, void *, lc_error *);
int ps_query_artifact_remove(ps_space *, uint64_t, const void *, size_t,
                             const ps_ref *, lc_error *);
int ps_query_artifacts_reclaim(ps_space *, uint64_t, lc_error *);
int ps_query_values_reclaim(ps_space *, uint64_t, lc_error *);
#endif
