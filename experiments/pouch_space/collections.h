#ifndef POUCH_SPACE_COLLECTIONS_H
#define POUCH_SPACE_COLLECTIONS_H
#include "lc_collection_head.h"
#include "lc_pouch.h"
#include "lc_pouch_record_io.h"
#include "root.h"
struct ps_collection_owner;
/* Offline import preserves the released collection ID and byte coordinates.
 * The source calls visit once per retained canonical segment. The destination
 * remains private to the root import until every collection has verified. */
typedef int (*ps_collection_import_part_fn)(void *, lc_u64, lc_pouch_record_io,
                                            uint64_t, lc_error *);
typedef int (*ps_collection_import_parts_fn)(void *,
                                             ps_collection_import_part_fn,
                                             void *, lc_error *);
int ps_collection_import(lc_pouch *, ps_namespace *, const char *, const char *,
                         const lc_collection_disk_head *,
                         ps_collection_import_parts_fn, void *, lc_error *);
int ps_pouch_import_legacy_collections(lc_pouch *source, lc_pouch *target,
                                       ps_namespace *, const char *,
                                       lc_error *);
/* Catalog admission binds the existing production collection engine. */
int ps_collection_client_open(ps_root *, lc_client *, const char *,
                              const lc_collection_config *, int,
                              lc_collection **, lc_error *);
int ps_collection_client_visit(ps_root *, lc_client *, const char *,
                               lc_collection_visit_fn, void *, lc_error *);
/* Root close has excluded external namespace/receiver pins. No payload I/O. */
void ps_collection_cache_destroy(struct ps_collection_owner *);
void ps_collection_maintenance_cache_clear(ps_root *);
/* A durable conversion workspace already blocks new admission. Release this
 * collection's cached byte owners only after all existing receivers drain.
 * Keep the owner's logical query/head-token epoch and sibling owners intact.
 */
int ps_collection_conversion_release_cache(ps_root *, uint64_t, lc_error *);
int ps_collection_canonical_recover(lc_pouch_record_io, uint64_t, uint64_t,
                                    uint64_t *, void *, lc_error *);
/* Caller holds an orphan claim on PS_STREAM{owner,0}. Retire every captured
 * child, including uncached segments, before the control and identity. A
 * failed pass leaves the unbound identity present for recovery/retry. */
int ps_collection_reclaim_children(ps_space *, uint64_t, lc_error *);
int ps_collection_maintenance_run(ps_root *, lc_pouch *,
                                  const lc_pouch_maintenance_options *,
                                  lc_pouch_maintenance_result *, lc_error *);
#endif
