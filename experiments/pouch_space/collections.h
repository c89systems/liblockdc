#ifndef POUCH_SPACE_COLLECTIONS_H
#define POUCH_SPACE_COLLECTIONS_H
#include "root.h"
struct ps_collection_owner;
/* Catalog admission binds the existing production collection engine. */
int ps_collection_client_open(ps_root *, lc_client *, const char *,
                              const lc_collection_config *, int,
                              lc_collection **, lc_error *);
int ps_collection_client_visit(ps_root *, lc_client *, const char *,
                               lc_collection_visit_fn, void *, lc_error *);
/* Root close has excluded external namespace/receiver pins. No payload I/O. */
void ps_collection_cache_destroy(struct ps_collection_owner *);
/* Caller holds an orphan claim on PS_STREAM{owner,0}. Retire every captured
 * child, including uncached segments, before the control and identity. A
 * failed pass leaves the unbound identity present for recovery/retry. */
int ps_collection_reclaim_children(ps_space *, uint64_t, lc_error *);
#endif
