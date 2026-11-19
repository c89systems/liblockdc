#ifndef POUCH_SPACE_COLLECTION_WALK_H
#define POUCH_SPACE_COLLECTION_WALK_H
#include "collection_conversion.h"
#include "lc_collection_storage.h"

typedef struct ps_collection_walk ps_collection_walk;
/* END (kind zero) retains a source's exact final boundary, including empty
 * segments. Unit/index bytes exclude framing. Other bytes cover the original
 * complete record. EDIT_BODY is a paired token: bytes ends its body record,
 * next_offset ends the adjacent EDIT receipt. Process both before checkpoint.
 */
typedef struct ps_collection_walk_item {
  lc_u64 segment, offset, next_offset, bytes, height, kind;
} ps_collection_walk_item;
/* Capture the selected canonical topology once, validate metadata dependencies
 * and persist a sorted reachability plan before any source disposal. All input
 * sources must be sealed and excluded from mutation by the caller. input opens
 * retained byte capabilities, including for the currently scanned source.
 * Payload bytes are neither decoded nor staged. Build memory follows canonical
 * framing/referenced-unit metadata, never payload bytes or reserved capacity.
 * Partial preparation is replaceable; completion is published last. Existing
 * completed plans reject replacement. No copying, head/policy selection or
 * source disposal occurs here.
 */
int ps_collection_walk_prepare(ps_collection_conversion *, const char *,
                               lc_pouch_crypto *, lc_collection_read_storage,
                               lc_error *);
/* Borrow work until close. Open and lookup use bounded page scratch and need
 * no original source bytes; they work after a completed prefix is reclaimed.
 */
int ps_collection_walk_open(ps_collection_conversion *, const char *,
                            ps_collection_walk **, lc_error *);
void ps_collection_walk_close(ps_collection_walk *);
/* First item at/after cursor, in snapshot-before-normal order. found=0 is EOF.
 * next cursor is the following item's coordinates, or (segment,offset+1) after
 * the final END. Results are unchanged on error. The caller converts a token,
 * seals its candidates, publishes mappings/cursor, then may prune its source.
 */
int ps_collection_walk_next(ps_collection_walk *, lc_u64, lc_u64,
                            ps_collection_walk_item *, lc_u64 *, lc_u64 *,
                            int *, lc_error *);
#endif
