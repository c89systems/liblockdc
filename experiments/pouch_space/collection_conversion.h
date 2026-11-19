#ifndef POUCH_SPACE_COLLECTION_CONVERSION_H
#define POUCH_SPACE_COLLECTION_CONVERSION_H
#include "catalog.h"
#include "collection_segment.h"
#include "lc_collection_head.h"
#include "lc_pouch_crypto.h"

#define PS_COLLECTION_CONVERSION_BATCH 64U
typedef struct ps_collection_conversion ps_collection_conversion;
typedef struct ps_collection_relocation {
  lc_u64 source_segment, source_offset, target_segment, target_offset;
} ps_collection_relocation;
typedef struct ps_collection_conversion_progress {
  lc_u64 pages, records, candidates;
  lc_u64 source_segment, source_offset;
} ps_collection_conversion_progress;

/* Offline collection coordination. The caller excludes logical mutation of
 * this collection, retains the space and keeps its encryption identity. Other
 * owners remain independent. Creation captures the canonical head and target
 * codec identity before any candidates are admitted. Opening existing work
 * requires the same target; a different request never replaces its intent.
 * One exclusive anchor owner admits one converter, including across close/open.
 * Close preserves durable work; it never selects a collection or root policy.
 */
int ps_collection_conversion_open(ps_space *, uint64_t collection,
                                  lc_pouch_crypto *target, int create,
                                  ps_collection_conversion **, lc_error *);
void ps_collection_conversion_close(ps_collection_conversion *);
void ps_collection_conversion_status(const ps_collection_conversion *,
                                     ps_collection_conversion_progress *);
void ps_collection_conversion_source_head(const ps_collection_conversion *,
                                          lc_collection_disk_head *, ps_ref *);
/* Source aliases protect exact captured incarnations across original catalog
 * changes. No body bytes are read. Rebinding an ID to a different source fails.
 */
int ps_collection_conversion_source_bind(ps_collection_conversion *, lc_u64,
                                         const ps_catalog_info *, lc_error *);
int ps_collection_conversion_source_get(ps_collection_conversion *, lc_u64,
                                        ps_catalog_info *, lc_error *);
/* Logical IDs belong to the collection plan; physical keys use fresh reserved
 * identities, so speculative rollback/retry cannot collide with old owners.
 * At most BATCH new candidates may precede a checkpoint. The caller owns and
 * closes each returned segment. All new candidates must be sealed before a
 * checkpoint; prior candidates are immutable. Completed parts may close before
 * publication: their catalog protection is already durable.
 */
int ps_collection_conversion_candidate_create(ps_collection_conversion *,
                                              lc_u64, ps_collection_segment **,
                                              lc_error *);
int ps_collection_conversion_candidate_open(ps_collection_conversion *, lc_u64,
                                            ps_collection_segment **,
                                            lc_error *);
/* At most BATCH strictly ordered record mappings per page. Snapshot source
 * IDs precede normal segment IDs, matching canonical history traversal. The
 * cursor follows every entry and never moves backward. Bytes and the immutable
 * page precede an always-durable progress replacement, even on NoSync roots.
 * Failed checkpoints leave the prior prefix selected. Uncertain I/O requires
 * reopen. Reopen drops speculative candidate bindings/pages beyond that prefix.
 */
int ps_collection_conversion_checkpoint(ps_collection_conversion *, lc_u64,
                                        lc_u64,
                                        const ps_collection_relocation *,
                                        size_t, lc_error *);
/* Bounded scratch and binary page search; no whole-history relocation array.
 * Missing is success with present=0. Outputs change only on success.
 */
int ps_collection_conversion_lookup(ps_collection_conversion *, lc_u64, lc_u64,
                                    ps_collection_relocation *, int *,
                                    lc_error *);
/* Explicit cancellation/finished-work cleanup. Remove workspace aliases and
 * metadata before its anchor. Actual unreferenced streams are reclaimed by the
 * ordinary orphan pass; streams selected elsewhere remain protected. Error
 * preserves a retry anchor. Cancellation is durably marked before cleanup;
 * reopen finishes it before admitting new work. A missing-work open after
 * cancellation returns INVALID; create admits a fresh workspace. Close the
 * handle after successful discard.
 */
int ps_collection_conversion_discard(ps_collection_conversion *, lc_error *);
/* Collection/orphan cleanup hook. Active anchors exclude cascading cleanup. */
int ps_collection_conversion_reclaim(ps_space *, uint64_t, lc_error *);
#endif
