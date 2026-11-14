#ifndef POUCH_SPACE_CONTROL_H
#define POUCH_SPACE_CONTROL_H

#include "lc_collection_head.h"
#include "space.h"

/* Collection IDs come from the root's reserved identity allocator. The caller
 * retains collection metadata authority across read/modify/store; this boundary
 * neither queues writers nor allocates public collection positions. Old readers
 * remain pinned across replacement. Every store waits for durability, including
 * NoSync roots. A failed I/O has the usual uncertain/poisoned-root semantics.
 * The caller stabilizes referenced canonical dependencies before publishing a
 * head that can recover independently. Numeric segment/snapshot IDs are the
 * collection owner's authority; this codec does not validate those bindings.
 * This is the canonical head, not a second application log or projection. */
int ps_collection_head_store(ps_space *, uint64_t collection,
                             const lc_collection_disk_head *, ps_ref *,
                             lc_error *);
/* NULL expected selects the current completed head. An exact reference rejects
 * stale/foreign identities. Decode is atomic; dropped remains observable data
 * so the collection owner can enforce its existing admission policy. */
int ps_collection_head_load(ps_space *, uint64_t collection, const ps_ref *,
                            lc_collection_disk_head *, lc_error *);

#endif
