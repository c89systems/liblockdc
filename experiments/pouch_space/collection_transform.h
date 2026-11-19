#ifndef POUCH_SPACE_COLLECTION_TRANSFORM_H
#define POUCH_SPACE_COLLECTION_TRANSFORM_H
#include "collection_conversion.h"
#include "root.h"
typedef struct ps_collection_transform ps_collection_transform;
typedef struct ps_collection_transform_result {
  size_t tokens;
  lc_u64 units, publications, reclaimed;
} ps_collection_transform_result;
/* Offline per-collection controller. Borrow root, logical codec context,
 * target and workspace through close. The caller excludes target-collection
 * receivers/writers. Cache release refuses outstanding receivers; sibling
 * collections remain independent. Preparation seals canonical sources and
 * persists their plan before copying. Resume uses the stored packing target.
 * Sealing the append source requires completion; cancellation cannot expose a
 * sealed old append head. Encryption identity is unchanged. No root policy is
 * selected here.
 */
int ps_collection_transform_open(ps_root *, lc_pouch *, lc_pouch_crypto *,
                                 ps_collection_conversion *, const char *,
                                 lc_u64 target_bytes,
                                 ps_collection_transform **, lc_error *);
/* Visit at most limit plan tokens. Unit I/O is truly streaming. Checkpoint
 * output/mappings before reclaiming old source grants. Edit body/receipt pairs
 * stay adjacent and indivisible. Completion durably selects the new collection
 * head after all dependencies/bindings, invalidates derived query artifacts,
 * removes obsolete bindings and detaches the workspace last. A failed call may
 * have durable progress; close both handles and reopen the workspace to retry.
 * Uncertain I/O also requires root reopen. Outputs change only on success.
 */
int ps_collection_transform_step(ps_collection_transform *, size_t, int *,
                                 ps_collection_transform_result *, lc_error *);
void ps_collection_transform_close(ps_collection_transform *);
#endif
