#ifndef POUCH_SPACE_OWNED_H
#define POUCH_SPACE_OWNED_H

#include "space.h"

/* Private bridge for the canonical stream owner, not a pathname/fd API.
 * One head owner may be open per typed stream in a process. Run handles pin
 * physical ownership; their immutable mapping survives append and checkpoint.
 * The owner serializes its appenders and writes only unpublished byte ranges.
 * Logical record validation/publication belongs to the canonical layer. */
#define PS_OWNED_RUN 4U
#define PS_RUN_HEADER 64U
typedef struct ps_owned_head ps_owned_head;
typedef struct ps_owned_run ps_owned_run;
typedef struct ps_owned_status {
  ps_ref ref;
  uint64_t tail, capacity, runs;
  int sealed, retired;
} ps_owned_status;

/* The optional budget funds creation only; an open cached owner retains no
 * claim. */
int ps_owned_open(ps_space *, ps_budget *, ps_key, int create, const ps_ref *,
                  ps_owned_head **, ps_owned_status *, lc_error *);
void ps_owned_close(ps_owned_head *);
int ps_owned_status_get(ps_owned_head *, ps_owned_status *, lc_error *);
/* Capture head metadata without acquiring its exclusive writer owner, reading
 * payloads, allocating handles or changing pins. A retained catalog/read pin
 * protects the caller's subsequent use; this scalar observation alone does not.
 * Exact references reject stale/foreign incarnations, including during reads.
 */
int ps_owned_inspect(ps_space *, ps_key, const ps_ref *, ps_owned_status *,
                     lc_error *);
/* desired==0 looks up a grant at the exact logical base (found==0 is EOF).
 * Otherwise allocate a new contiguous grant at the current capacity; desired
 * is a whole-page physical size, at most 1 MiB. Fragmentation may reduce it.
 * New grants contain only zeros after their immutable identity header.
 * Reused/uncertified space is initialized and synced before its grant enters
 * the journal. Already durable zero space needs no preparation barrier. */
int ps_owned_grant(ps_owned_head *, ps_budget *, uint64_t base,
                   uint64_t desired, ps_owned_run **, int *found, lc_error *);
uint64_t ps_owned_run_capacity(const ps_owned_run *);
void ps_owned_run_close(ps_owned_run *);
int ps_owned_read(ps_owned_run *, uint64_t offset, void *, size_t, lc_error *);
int ps_owned_write(ps_owned_run *, uint64_t offset, const void *, size_t,
                   lc_error *);
/* Clear an owned unpublished suffix, preserving reservation and bytes outside
 * the range. scratch points to PS_CHUNK bytes for the bounded fallback. */
int ps_owned_clear(ps_owned_run *, uint64_t offset, uint64_t bytes,
                   void *scratch, lc_error *);
/* Complete canonical bytes precede this call. Head tail checkpoints are
 * structural hints, not a second per-record commit journal. Default durable
 * success waits for the shared root readiness prefix, independently of journal
 * seq. DEFER_SYNC requires the caller's later flush; publisher callbacks still
 * have to establish the durable dependency coverage on durable roots. */
int ps_owned_commit(ps_owned_head *, uint64_t tail, enum ps_commit_mode,
                    ps_commit_publish_fn, void *, lc_error *);
int ps_owned_barrier(ps_owned_head *, lc_error *);
/* Canonical recovery has validated [checkpoint tail, tail) and cleared the
 * rejected suffix. Stabilize it before accepting the recovered tail hint.
 * Complete repairs join root readiness and may share a durability barrier. */
int ps_owned_recovered(ps_owned_head *, uint64_t tail, lc_error *);
/* Structural transitions always synchronize. Seal first stabilizes canonical
 * bytes, then publishes the immutable final tail. Retirement journals one
 * head removal and atomically retires its complete run directory. Held handles
 * retain payload pins through durable root retirement. */
int ps_owned_seal(ps_owned_head *, lc_error *);
/* Revalidate absence of durable catalog references/views and exclude new
 * bindings until this exclusive owner closes. Used before cascading a closed
 * logical owner's child metadata; failure never authorizes child removal.
 * This process-local claim leaves the identity present for retry after a
 * failed cleanup or process death. It writes no format or payload bytes. */
int ps_owned_claim_orphan(ps_owned_head *, lc_error *);
int ps_owned_retire(ps_owned_head *, lc_error *);

#endif
