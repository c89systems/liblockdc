#ifndef POUCH_SPACE_COLLECTION_SEGMENT_H
#define POUCH_SPACE_COLLECTION_SEGMENT_H

#include "lc_collection_storage.h"
#include "lc_pouch_record_io.h"
#include "owned.h"

typedef struct ps_collection_segment ps_collection_segment;
typedef int (*ps_collection_segment_recover_fn)(lc_pouch_record_io,
                                                uint64_t checkpoint,
                                                uint64_t capacity,
                                                uint64_t *accepted, void *,
                                                lc_error *);

/* Collection canonical storage is seekable because independent uploads own
 * disjoint REGION reservations inside a segment. REGION headers are complete
 * allocation metadata; their private units become application data only when
 * a canonical publication refers to them. No producer owns allocator or root
 * publication authority. The collection owner serializes allocation/append,
 * validates private patches, and caches one segment owner per typed key.
 * Captured readers and private ranges retain that owner until their I/O ends.
 * The caller retains the root for the complete owner lifetime. */
int ps_collection_segment_open(ps_space *, ps_key, int create, const ps_ref *,
                               ps_collection_segment_recover_fn, void *,
                               ps_collection_segment **, lc_error *);
/* Stable peer-selected mappings for readers. Refresh is for the active
 * writer under logical append and metadata authority. */
int ps_collection_segment_view(ps_collection_segment *,
                               ps_collection_segment **, lc_error *);
int ps_collection_segment_refresh(ps_collection_segment *,
                                  ps_collection_segment_recover_fn, void *,
                                  lc_error *);
void ps_collection_segment_retain(ps_collection_segment *);
void ps_collection_segment_close(ps_collection_segment *);
lc_pouch_record_io ps_collection_segment_io(ps_collection_segment *);
/* Retained logical-engine capability; its caller still retains the root. */
void ps_collection_segment_bind(ps_collection_segment *,
                                lc_collection_segment *);
int ps_collection_segment_status(ps_collection_segment *, ps_owned_status *,
                                 uint64_t *working_tail, lc_error *);
/* Extends the logical canonical range with already prepared zero ownership.
 * No implicit container growth. A failed reservation can retain reusable
 * grants, but does not advance the logical range or publish an outcome. */
int ps_collection_segment_reserve(ps_collection_segment *, uint64_t,
                                  lc_error *);
/* Only an unpublished active suffix may be discarded. Clearing is bounded
 * and stabilized before reuse; live REGION units outside it are untouched. */
int ps_collection_segment_truncate(ps_collection_segment *, uint64_t,
                                   lc_error *);
/* Call after complete canonical records and private unit fixups. Equal tails
 * still register fresh readiness, so a patch is covered by its publication's
 * acknowledgement. Unfinished uploads need no separate completion barrier. */
int ps_collection_segment_publish(ps_collection_segment *, enum ps_commit_mode,
                                  lc_error *);
/* Force current bytes, including finalized private units in an older segment.
 * This does not publish an application outcome or wait for producer EOF. */
int ps_collection_segment_sync(ps_collection_segment *, lc_error *);
int ps_collection_segment_seal(ps_collection_segment *, lc_error *);
int ps_collection_segment_claim_orphan(ps_collection_segment *, lc_error *);
int ps_collection_segment_retire(ps_collection_segment *, lc_error *);
/* Offline completed-prefix disposal. Caller excludes byte I/O and holds
 * canonical liveness authority through select. Only sealed sources qualify.
 * Drop whole grants before through; the crossing grant stays unchanged. When
 * through reaches the final tail, unused capacity can go too. No retained
 * bytes are copied or newly checksummed. Captured physical run pins survive;
 * a later open reconstructs the sparse directory. Error can leave an already
 * completed prefix disposed; retry is idempotent. removed changes on success.
 */
int ps_collection_segment_discard_prefix(ps_collection_segment *, uint64_t,
                                         ps_piece_select_fn, void *,
                                         uint64_t *removed, lc_error *);

#endif
