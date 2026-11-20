#ifndef POUCH_SPACE_READINESS_H
#define POUCH_SPACE_READINESS_H
#include "space.h"
typedef struct ps_readiness ps_readiness;
#define PS_READINESS_WORDS 11U
enum ps_readiness_change {
  PS_READINESS_ROOT = 1,
  PS_READINESS_JOURNAL = 2,
  PS_READINESS_RECOVERY = 4
};
typedef struct ps_readiness_certificate {
  uint64_t ready, durable, root, origin, pending;
  uint64_t grant_used, grant_limit;
} ps_readiness_certificate;
/* All access is under container allocation authority. Membership is an OFD
 * read witness at byte 3, separate from physical-reader presence at byte 2.
 * Certificates are volatile cohort state, not replay authority after reopen.
 */
int ps_readiness_attach(ps_space *, lc_error *);
void ps_readiness_close(ps_space *);
int ps_readiness_validate(ps_space *, lc_error *);
int ps_readiness_opened(ps_space *, lc_error *);
int ps_readiness_read(ps_space *, ps_readiness_certificate *, lc_error *);
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
int ps_readiness_stabilize(ps_space *, uint64_t wire, uint64_t root,
                           lc_error *);
/* Prepared pages are durably zeroed free payload capacity. The allocator
 * checks proofs under allocation authority and revokes them before spending
 * pages. A capacity reservation can retain the proof while bytes stay free. */
/* Return the first span that fits wanted, or the largest shorter span, within
 * a page-aligned free range. The output is the physical start of that span. */
uint64_t ps_readiness_zero_find(ps_space *, uint64_t offset, uint64_t bytes,
                                uint64_t wanted, uint64_t *selected);
uint64_t ps_readiness_zero_prefix(ps_space *, uint64_t offset, uint64_t bytes);
void ps_readiness_zero_pages(ps_space *, uint64_t offset, uint64_t bytes,
                             int prepared);
#endif
