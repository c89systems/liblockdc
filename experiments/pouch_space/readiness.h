#ifndef POUCH_SPACE_READINESS_H
#define POUCH_SPACE_READINESS_H
#include "space.h"
typedef struct ps_readiness ps_readiness;
#define PS_READINESS_WORDS 9U
enum ps_readiness_change { PS_READINESS_ROOT = 1, PS_READINESS_JOURNAL = 2 };
typedef struct ps_readiness_certificate {
  uint64_t ready, durable, root, origin, pending;
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
/* Mark an in-flight disk transition before its first canonical write. Only
 * publication or a checked recovery boundary can clear that transition. */
void ps_readiness_begin(ps_space *, unsigned int changes);
void ps_readiness_finish(ps_space *, unsigned int changes);
int ps_readiness_publish(ps_space *, lc_error *);
int ps_readiness_stabilize(ps_space *, uint64_t wire, uint64_t root,
                           lc_error *);
/* Prepared pages are durably zeroed common payload capacity. Claims revoke
 * the proof before writing. Publish only fully zeroed pages under allocation
 * authority while returning a continuously owned range to the common pool. */
uint64_t ps_readiness_zero_prefix(ps_space *, uint64_t offset, uint64_t bytes);
void ps_readiness_zero_pages(ps_space *, uint64_t offset, uint64_t bytes,
                             int prepared);
#endif
