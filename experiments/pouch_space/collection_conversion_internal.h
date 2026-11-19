#ifndef POUCH_SPACE_COLLECTION_CONVERSION_INTERNAL_H
#define POUCH_SPACE_COLLECTION_CONVERSION_INTERNAL_H
#include "collection_conversion.h"
/* Durable planning slots are disjoint from append-only relocation pages.
 * Incomplete plans can be replaced until a final control selects them. Slots
 * are below SNAPSHOT_BIT, values at most one page. Owner cleanup removes all.
 */
int ps_collection_conversion_meta_read(ps_collection_conversion *, lc_u64,
                                       void *, size_t, int *, lc_error *);
int ps_collection_conversion_meta_write(ps_collection_conversion *, lc_u64,
                                        const void *, size_t, lc_error *);
void ps_collection_conversion_context(ps_collection_conversion *, ps_space **,
                                      uint64_t *);
int ps_collection_conversion_candidates(ps_collection_conversion *,
                                        ps_catalog_visit_fn, void *,
                                        lc_error *);
int ps_collection_conversion_target_matches(ps_collection_conversion *,
                                            lc_pouch_crypto *, lc_error *);
/* Sealing an append source is itself irreversible, before byte disposal. */
int ps_collection_conversion_require_completion(ps_collection_conversion *,
                                                lc_error *);
#endif
