#ifndef POUCH_SPACE_REVISION_H
#define POUCH_SPACE_REVISION_H
#include "space.h"

/* Capture the identity and complete byte count of an unpublished immutable
 * writer, including its bounded pending tail. It does not publish or flush.
 * The identity can be embedded in another member of the same revision. */
int ps_revision_writer_info(const ps_writer *, ps_ref *, uint64_t *,
                            lc_error *);
/* Publish a complete set of distinct immutable values in one checksummed
 * physical journal frame. Writers belong to the same space and capacity
 * budget (which may be NULL). No producer executes under publication authority.
 * Admission accounts for the whole revision; no application value is buffered
 * or concatenated. The frame contains only ownership descriptors.
 *
 * Recovery admits every member or none, including candidate frames whose
 * individual entries have valid checksums. A durable root (or any CONTROL
 * member) exposes and acknowledges only a completed shared durability prefix
 * covering the whole revision, including non-CONTROL members on NoSync roots.
 * NoSync follows ordinary value publication policy. Automatic journal rollover
 * can additionally checkpoint before selection. Existing captured readers pin
 * their old incarnations. Invalid invocations leave private writers untouched.
 * Once preparation starts, failure makes participating writers unusable; close
 * them and rebuild. Uncertain I/O poisons the space as for ps_publish.
 * Outputs, when supplied, are valid only on success. One caller owns each
 * writer throughout the operation; stop concurrent use before publishing. */
int ps_revision_publish(ps_writer *const *, size_t, ps_ref *, lc_error *);
#endif
