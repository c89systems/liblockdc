#ifndef POUCH_SPACE_READINESS_H
#define POUCH_SPACE_READINESS_H
#include "space.h"
typedef struct ps_readiness ps_readiness;
#define PS_READINESS_WORDS 13U
#define PS_READINESS_CREDIT_SLOTS 32U
enum ps_readiness_change {
  PS_READINESS_ROOT = 1,
  PS_READINESS_JOURNAL = 2,
  PS_READINESS_RECOVERY = 4,
  /* The new root passed its selection barrier. Its covered journal and
   * predecessor may be erased and stabilized by another owner. */
  PS_READINESS_CHECKPOINT_RETIRE = 8
};
typedef struct ps_readiness_certificate {
  uint64_t ready, durable, root, origin, pending;
  size_t journal_bytes; /* Relative to the selected root's journal start. */
  uint64_t grant_used, grant_limit;
  uint64_t prepared_durable;
} ps_readiness_certificate;
/* Certificate updates and authoritative selection use allocation authority.
 * Membership is an OFD read witness at byte 3, separate from the reader
 * presence witness at byte 2. Certificates are volatile cohort state.
 */
int ps_readiness_attach(ps_space *, lc_error *);
int ps_readiness_joined(const ps_space *);
void ps_readiness_close(ps_space *);
/* The credit slot lives with the owner's OFD. Acquisition and credit
 * increases require allocator authority. A zero balance only clears a bit. */
int ps_readiness_credit_reserve(ps_space *, lc_error *);
int ps_readiness_credit_registered(const ps_space *);
int ps_readiness_credit_publish(ps_space *);
int ps_readiness_credit_collect(ps_space *, uint64_t *, uint64_t *, lc_error *);
int ps_readiness_credit_spilled(const ps_space *);
void ps_readiness_credit_note_spill(ps_space *);
int ps_readiness_validate(ps_space *, lc_error *);
int ps_readiness_opened(ps_space *, lc_error *);
int ps_readiness_read(ps_space *, ps_readiness_certificate *, lc_error *);
/* Under the local space mutex, prove the resident canonical projection at
 * a checked instant. A miss needs ordinary allocator capture. No physical
 * ownership or allocator permission follows from a match. */
int ps_readiness_resident(ps_space *);
/* After retaining extents from a certified resident prefix, check that a
 * checkpoint or recovery cannot already have made those extents reusable.
 * Journal and durability advances can retire values within the same root;
 * reclamation of their storage still requires a checkpoint transition. */
int ps_readiness_retains_capture(ps_space *);
/* A newer certified root can seed private preparation. Installation still
 * needs the allocator and a fresh certificate. */
int ps_readiness_preview(ps_space *, ps_readiness_certificate *);
/* Advisory capacity scheduling. Workers can sample without allocator
 * authority. Updating the level requires the allocator scope. */
int ps_readiness_pressure(ps_space *);
void ps_readiness_set_pressure(ps_space *, int);
uint64_t ps_readiness_activity(ps_space *);
void ps_readiness_note_mutation(ps_space *);
/* Mark an in-flight disk transition before its first canonical write.
 * A rejected suffix being erased keeps its own fence until a sync proves
 * retirement. Reading back a matching journal header cannot clear it. */
void ps_readiness_begin(ps_space *, unsigned int changes);
void ps_readiness_finish(ps_space *, unsigned int changes);
int ps_readiness_publish(ps_space *, lc_error *);
/* Register completed private writes before waiting for sync leadership.
 * Leaders capture the issued prefix before I/O and certify it afterward.
 * These tickets carry no journal visibility or allocation rights. */
int ps_readiness_prepare_ticket(ps_space *, uint64_t *, lc_error *);
uint64_t ps_readiness_issued_tickets(ps_space *);
int ps_readiness_stabilize(ps_space *, uint64_t wire, uint64_t root,
                           size_t journal_bytes, uint64_t prepared, lc_error *);
/* A stable checked snapshot can prove a published prefix or private
 * preparation ticket without allocation authority. Zero omits that bound.
 * Notifications carry no proof or authority. */
int ps_readiness_durability_covers(ps_space *, uint64_t wire, uint64_t ticket);
uint32_t ps_readiness_sync_observe(ps_space *);
int ps_readiness_sync_wait(ps_space *, uint32_t observed);
void ps_readiness_sync_wake(ps_space *);
/* Prepared pages are durably zeroed free payload capacity. The allocator
 * checks proofs under allocation authority and revokes them before spending
 * pages. A capacity reservation can retain the proof while bytes stay free. */
uint64_t ps_readiness_zero_coverage(ps_space *, uint64_t offset,
                                    uint64_t bytes);
/* Return the first span that fits wanted, or the largest shorter span, within
 * a page-aligned free range. The output is the physical start of that span. */
uint64_t ps_readiness_zero_find(ps_space *, uint64_t offset, uint64_t bytes,
                                uint64_t wanted, uint64_t *selected);
uint64_t ps_readiness_zero_prefix(ps_space *, uint64_t offset, uint64_t bytes);
void ps_readiness_zero_pages(ps_space *, uint64_t offset, uint64_t bytes,
                             int prepared);
/* Possible cohort witnesses, recorded before the kernel claim under allocator
 * authority. Clear only whole common free pages after a negative kernel query.
 * Read-only imports and uncovered ranges still need kernel queries. */
void ps_readiness_witness_pages(ps_space *, uint64_t offset, uint64_t bytes,
                                int possible);
int ps_readiness_witness_possible(ps_space *, uint64_t offset, uint64_t bytes);
#endif
