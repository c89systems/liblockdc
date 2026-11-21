#ifndef POUCH_SPACE_OWNED_H
#define POUCH_SPACE_OWNED_H

#include "coordination.h"

/* Private bridge for the canonical stream owner, not a pathname/fd API.
 * One head owner may be open per typed stream in a process. Run handles pin
 * physical ownership; their immutable mapping survives append and checkpoint.
 * The owner serializes its appenders and writes only unpublished byte ranges.
 * Logical record validation/publication belongs to the canonical layer. */
#define PS_OWNED_RUN 4U
#define PS_RUN_IDENTITY_BYTES 64U
#define PS_RUN_HEADER 96U
/* Required root geometry includes the coordination page and grant slots.
 * An opener that expects journal grants cannot recover this layout. */
#define PS_ROOT_LAYOUT_FIXED_GRANTS UINT32_C(0x20000)
#define PS_ROOT_LAYOUT_JOURNAL_WINDOW UINT32_C(0x40000)
#define PS_ROOT_LAYOUT                                                         \
  (PS_RUN_HEADER | UINT32_C(0x10000) | PS_ROOT_LAYOUT_FIXED_GRANTS |           \
   PS_ROOT_LAYOUT_JOURNAL_WINDOW)
#define PS_OWNED_DESCRIPTOR_BYTES 104U
typedef struct ps_owned_head ps_owned_head;
typedef struct ps_owned_run ps_owned_run;
typedef struct ps_owned_footer {
  unsigned char bytes[PS_RUN_HEADER - PS_RUN_IDENTITY_BYTES];
} ps_owned_footer;
typedef struct ps_owned_status {
  ps_ref ref;
  uint64_t tail, capacity, runs;
  uint64_t stored_bytes; /* selected payload ownership, excluding old pins */
  uint64_t edit_wire;
  int sealed, retired, complete;
} ps_owned_status;

/* The optional budget funds creation only. Shared creation retains an orphan
 * liveness claim until first durable binding or final close. */
int ps_owned_open(ps_space *, ps_budget *, ps_key, int create, const ps_ref *,
                  ps_owned_head **, ps_owned_status *, lc_error *);
/* Shared canonical stream views can overlap during staged cache replacement.
 * All their byte and mapping mutations use the same edit guard. */
int ps_owned_open_guarded(ps_space *, ps_budget *, ps_key, int create,
                          const ps_ref *, ps_owned_head **, ps_owned_status *,
                          lc_error *);
/* Stage an empty head for a following durable catalog binding in this space.
 * The binding's barrier covers both records. Keep this handle until the
 * binding returns so the shared preparation claim protects the head. */
int ps_owned_stage_empty(ps_space *, ps_budget *, ps_key, ps_owned_head **,
                         ps_ref *, lc_error *);
void ps_owned_close(ps_owned_head *);
/* Reopen an unpublished stream for preparation, such as a reserved successor
 * left by an interrupted publisher. Retain its claim until binding or close. */
int ps_owned_open_prepared(ps_space *, ps_key, ps_owned_head **,
                           ps_owned_status *, lc_error *);
int ps_owned_status_get(ps_owned_head *, ps_owned_status *, lc_error *);
/* Check durable retirement when admitting a new read window. A previously
 * captured reader reads its pinned mappings directly. */
int ps_owned_status_capture(ps_owned_head *, ps_owned_status *, lc_error *);
/* Acquire shared edit authority and refresh the selected head. The continued
 * flag covers the current checkpoint generation and previous owner; the
 * caller must also have a previously validated local view. Exclusive mode
 * returns a NULL guard. The guard spans recovery, producer I/O and cleanup.
 * Append admission reports the prior state and revokes its completion before
 * returning write authority. */
int ps_owned_edit_begin(ps_owned_head *, int append, ps_coordination_guard **,
                        ps_owned_status *, int *continued, lc_error *);
int ps_owned_edit_end(ps_owned_head *, ps_coordination_guard *, int,
                      lc_error *);
/* A logical collection appender already owns its cross-process edit scope. */
int ps_owned_mark_edit(ps_owned_head *, lc_error *);
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
/* Capture the exact mapping for remap or removal. A removal uses the
 * certified descriptor alone. Keeping bytes verifies the run identity. */
int ps_owned_capture_piece(ps_owned_head *, uint64_t base, int keep_bytes,
                           ps_owned_run **, lc_error *);
uint64_t ps_owned_run_capacity(const ps_owned_run *);
uint64_t ps_owned_run_base(const ps_owned_run *);
int ps_owned_run_mapped(const ps_owned_run *);
void ps_owned_run_retain(ps_owned_run *);
/* Capture only present mappings in logical order. Each handle pins that exact
 * incarnation; caller closes handles and frees the array with the default
 * allocator. Caller excludes mapping mutations until capture returns (stream
 * initialization owns that exclusion). Logical holes do not consume array
 * entries or read buffers. */
int ps_owned_runs_capture(ps_owned_head *, ps_owned_run ***, size_t *,
                          lc_error *);
/* Read raw identities and adjacent footers together for stream initialization.
 * The caller holds stream edit authority through the inspection and frees the
 * parallel footer array before repair or release of that authority. */
int ps_owned_runs_capture_initial(ps_owned_head *, ps_owned_run ***, size_t *,
                                  ps_owned_footer **, lc_error *);
int ps_owned_runs_capture_status(ps_owned_head *, ps_owned_status *,
                                 ps_owned_run ***, size_t *, lc_error *);
int ps_owned_runs_key_status(ps_space *, ps_key, const ps_ref *,
                             ps_owned_status *, ps_owned_run ***, size_t *,
                             lc_error *);
int ps_owned_runs_range(ps_space *, ps_key, const ps_ref *, uint64_t start,
                        uint64_t end, ps_owned_run ***, size_t *, lc_error *);
/* Select and pin from a held canonical allocator scope. Check identities
 * and optional footers after releasing that scope. */
int ps_owned_runs_pin_range(ps_space *, ps_key, const ps_ref *, uint64_t start,
                            uint64_t end, ps_owned_run ***, size_t *,
                            lc_error *);
int ps_owned_runs_verify(ps_owned_run *const *, size_t, ps_owned_footer *,
                         lc_error *);
/* Capture the writable suffix, including grants beyond the published tail.
 * The caller owns stream edit authority through use of the mappings. */
int ps_owned_runs_edit_range(ps_owned_head *, uint64_t start, uint64_t end,
                             ps_owned_run ***, size_t *, lc_error *);
typedef struct ps_owned_range {
  uint64_t offset, bytes;
} ps_owned_range;
uint64_t ps_owned_run_storage(const ps_owned_run *);
int ps_owned_run_matches(const ps_owned_run *, const ps_owned_range *, size_t);
typedef int (*ps_piece_publish_fn)(void *, lc_error *);
/* Runs the supplied selection exactly once while holding canonical liveness
 * authority, after copying has finished. Reject stale captured liveness before
 * invoking it, then return its result unchanged. The repack waits for
 * durability after the selector returns. NULL selects directly. */
typedef int (*ps_piece_select_fn)(void *, ps_piece_publish_fn, void *,
                                  lc_error *);
/* Replace one completed bounded piece with its sorted coalesced live ranges.
 * Empty ranges remove its ownership entirely, including larger grants and
 * unused grants at or
 * beyond a sealed head's committed tail. Those grants need no payload/footer
 * read and cannot be replaced with live ranges. Source checks and copying
 * happen outside publication authority; selection revalidates the captured
 * mapping. Always durable. Existing handles retain old bytes. Caller owns
 * canonical liveness authority and excludes new references to discarded ranges.
 */
int ps_owned_run_repack(ps_owned_head *, ps_owned_run *, ps_budget *,
                        const ps_owned_range *, size_t, ps_piece_select_fn,
                        void *, ps_owned_run **, lc_error *);
void ps_owned_run_close(ps_owned_run *);
int ps_owned_read(ps_owned_run *, uint64_t offset, void *, size_t, lc_error *);
int ps_owned_write(ps_owned_run *, uint64_t offset, const void *, size_t,
                   lc_error *);
/* Independently checked finalized bytes for bounded relocation. The identity
 * header is immutable; its separate completion footer joins canonical byte
 * readiness, without a per-record ownership journal event. Caller excludes
 * writes to [0, bytes) while completing/checking it. scratch has PS_CHUNK
 * bytes. Canonical grants can span up to PS_STREAM_PIECE_BYTES physical bytes.
 * Other owned users may retain larger grants and do not use this
 * capability. Inspect validates the footer only; verify also reads and checks
 * those bytes. Uncovered recovery may rebuild a footer after canonical
 * validation. A reconstructed inherited prefix requires canonical body proof
 * before relocation; inspection exposes that persistent obligation, and verify
 * returns pouch_piece_needs_validation rather than blessing those bytes. */
typedef struct ps_piece_checksum {
  uint32_t crc;
  int reconstructed;
} ps_piece_checksum;
/* Extend an unchanged checked prefix after private fixups. Only new bytes are
 * read; CRC combination retains detection of damage to the previous prefix.
 * Caller owns the prior CRC and supplies PS_CHUNK scratch. */
int ps_owned_piece_extend(ps_owned_run *, uint64_t prior_bytes,
                          const ps_piece_checksum *prior, uint64_t bytes,
                          void *scratch, ps_piece_checksum *, lc_error *);
/* Checksum state output is optional. */
int ps_owned_piece_inspect(ps_owned_run *, uint64_t bytes, ps_piece_checksum *,
                           lc_error *);
int ps_owned_piece_inspect_initial(ps_owned_run *, uint64_t bytes,
                                   const ps_owned_footer *, ps_piece_checksum *,
                                   lc_error *);
int ps_owned_piece_verify(ps_owned_run *, uint64_t bytes, void *scratch,
                          lc_error *);
/* Clear an owned unpublished suffix, preserving reservation and bytes outside
 * the range. scratch points to PS_CHUNK bytes for the bounded fallback. */
int ps_owned_clear(ps_owned_run *, uint64_t offset, uint64_t bytes,
                   void *scratch, lc_error *);
/* Complete canonical bytes precede this call. Shared participants certify a
 * tail by incarnation for live peers. Cold owners still validate the suffix
 * through canonical recovery. The caller excludes competing writes to the same
 * stream for the whole append. Default durable success waits for publication
 * coverage. DEFER_SYNC requires the caller's later flush. Publisher callbacks
 * establish durable dependency coverage before the shared hint is exposed. */
int ps_owned_commit(ps_owned_head *, uint64_t tail, enum ps_commit_mode,
                    ps_commit_publish_fn, void *, lc_error *);
/* A canonical appender holding its edit guard has finalized the piece
 * footers and has no rejected suffix. Publish both facts with the tail. */
int ps_owned_commit_canonical(ps_owned_head *, uint64_t tail,
                              enum ps_commit_mode, ps_commit_publish_fn, void *,
                              lc_error *);
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
 * The lifetime claim leaves the identity present for retry after a
 * failed cleanup or process death. It writes no format or payload bytes. */
int ps_owned_claim_orphan(ps_owned_head *, lc_error *);
int ps_owned_retire(ps_owned_head *, lc_error *);

#endif
