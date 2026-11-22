#ifndef POUCH_SPACE_NAMESPACE_H
#define POUCH_SPACE_NAMESPACE_H
#include "lc_pouch_retention.h"
#include "lc_pouch_state_record.h"
#include "owned.h"
#include "root.h"
#include "stream.h"

typedef struct ps_namespace_append ps_namespace_append;
typedef struct ps_namespace_window ps_namespace_window;
typedef struct ps_namespace_view ps_namespace_view;
typedef struct ps_namespace_body ps_namespace_body;
typedef struct ps_namespace_recode ps_namespace_recode;
typedef struct ps_namespace_conversion ps_namespace_conversion;
typedef struct ps_ns_resource ps_namespace_retirement;
struct lc_pouch_storage_batch;
struct lc_pouch_storage_namespace_info;
struct lc_pouch_storage_state_refresh;
enum ps_namespace_append_mode {
  PS_APPEND_PREFIX = 0,
  PS_APPEND_COMPLETE,
  PS_APPEND_MAINTENANCE,
  PS_APPEND_MAINTENANCE_PREFIX,
  PS_APPEND_FRAMED_PREFIX
};
typedef struct ps_namespace_status {
  uint64_t active_segment, segments, tail, runs;
  uint64_t checkpoint_epoch, covered_segment, covered_offset;
  uint64_t checkpoint_keys, pending_keys, pending_metadata_bytes;
  uint64_t bound_segments, bound_bodies, pending_retirements;
} ps_namespace_status;
typedef struct ps_namespace_source {
  const char *leaf;
  unsigned int kind;
  uint64_t id, tail, capacity, stored_bytes, runs;
  int closed; /* An inactive segment or a sealed immutable body. */
} ps_namespace_source;
typedef int (*ps_namespace_source_fn)(void *, const ps_namespace_source *,
                                      lc_error *);
/* Captured bound-source metadata, excluding unpublished builds. No canonical
 * or body reads; callbacks run outside owner locks. Labels are borrowed while
 * the namespace capability remains retained. */
int ps_namespace_sources_visit(ps_namespace *, ps_namespace_source_fn, void *,
                               lc_error *);

/* Namespace-owned canonical segments. Caller retains its namespace pin during
 * synchronous calls. Returned append/view/window handles retain another pin.
 * One cached storage owner per namespace, never a pathname/virtual filesystem.
 * An empty namespace has no head/segment until its first append or rotation. */
int ps_namespace_status_get(ps_namespace *, ps_namespace_status *, lc_error *);
/* Current route and selected checkpoint, with no per-source enumeration. */
int ps_namespace_projection(ps_namespace *,
                            struct lc_pouch_storage_namespace_info *,
                            lc_error *);
struct ps_checkpoint_observer;
int ps_namespace_checkpoint_observe(ps_namespace *,
                                    const struct ps_checkpoint_observer *,
                                    lc_error *);
/* Flushes only changed canonical metadata, retaining uncovered history until
 * every bounded batch is selected. Caller holds logical namespace authority. */
int ps_namespace_checkpoint_flush(ps_namespace *, lc_error *);
/* Replays the canonical suffix and reinstalls its outstanding checkpoint
 * capacity claim without publishing a new key checkpoint. */
int ps_namespace_checkpoint_recover(ps_namespace *, lc_error *);
/* Check the physical coverage and ready tail in one live-cohort capture. */
int ps_namespace_admission_proven(ps_space *, uint64_t owner, int *,
                                  lc_error *);
/* Check coverage at a caller-selected prefix under the local space mutex. */
int ps_namespace_admission_proven_locked(ps_space *, uint64_t owner,
                                         uint64_t prefix, int *proven,
                                         int *private_failure, lc_error *);
/* Before exposing the root, recover state claims and release empty caches. */
int ps_namespace_recover_on_open(ps_namespace *, lc_error *);
uint64_t ps_namespace_source_refs(ps_namespace *, unsigned int, uint64_t);
/* Upper bound on live encoded bytes, or UINT64_MAX without a current
 * projection. The caller holds logical namespace mutation authority. */
uint64_t ps_namespace_source_bytes(ps_namespace *, unsigned int, uint64_t);
/* Conservative durable work marker, independent of the segment head/tail.
 * Ensure precedes expiry-bearing publication, including for NoSync roots. A
 * failed mutation may leave a harmless positive hint. */
int ps_namespace_retention_hint_ensure(ps_namespace *, lc_error *);
/* Metadata-only: no segment opening, canonical replay or body reads. */
int ps_namespace_retention_hint_get(ps_namespace *, int *, lc_error *);
/* Presence only, without reading the control value. Consumption validates the
 * selected namespace so one malformed control cannot starve sibling names. */
int ps_namespace_retention_hint_present(ps_namespace *, int *, lc_error *);
/* Native immutable work identity plus a small incarnation-bound progress
 * record. Load allocates structured fields; a marker alone is idle. Store
 * borrows its input and requires an existing hint. Active identity admission
 * protects its claim and finish publications until finished/idle, including
 * across root reopen. Callers hold logical namespace mutation authority while
 * changing controls. */
int ps_namespace_retention_work_load(ps_namespace *, const lc_allocator *,
                                     lc_pouch_retention_work *, lc_error *);
int ps_namespace_retention_work_store(ps_namespace *,
                                      const lc_pouch_retention_work *,
                                      lc_error *);
/* Always durably clears work before retiring its marker. */
int ps_namespace_retention_hint_remove(ps_namespace *, lc_error *);
/* One active producer per namespace, explicit BUSY on contention. Unrelated
 * namespaces and captured readers progress independently. target==0 disables
 * automatic rotation; otherwise rotate before the next append when the active
 * committed tail has reached target. PREFIX admits a known minimum (or one
 * byte when zero), with later unknown-size growth using ordinary admission.
 * COMPLETE requires exactly minimum positive bytes and preserves the standing
 * floor; MAINTENANCE is complete bounded work permitted to spend that floor.
 * FRAMED_PREFIX grows like PREFIX and commits a single checksummed frame.
 * Framed appends are indivisible recovery units. Storage reserves the frame
 * in addition to minimum canonical bytes; returned offsets address the actual
 * inner records. An invalid candidate frame contributes no records, while
 * corruption in required checkpoint metadata or an uncovered trusted prefix
 * is an error. Covered history is not replayed; referenced bodies are checked
 * when consumed.
 * A non-NULL batch enumerates known key/metadata ceilings during admission;
 * checkpoint debt and the canonical span share one compound capacity claim.
 * Actual prefixes must stay within those ceilings. The callback is borrowed
 * only until this call returns. NULL retains incremental prefix admission for
 * producers whose prefixes are not known ahead of time.
 * Plan successor, binding, grants and head selection before staging. Preferred
 * geometric growth falls back to an exact claim near capacity. Commit selects
 * its head only after canonical
 * bytes are complete; abort leaves the predecessor selected. No lock spans the
 * producer callback. Routing follows root durability; a predecessor seal waits
 * for durable routing before the next mutation proceeds. */
int ps_namespace_append_begin(ps_namespace *, uint64_t target, uint64_t minimum,
                              enum ps_namespace_append_mode,
                              const struct lc_pouch_storage_batch *,
                              ps_namespace_append **, lc_error *);
lc_pouch_record_io ps_namespace_append_output(ps_namespace_append *);
/* Label remains stable after append close while a namespace pin is held. */
const char *ps_namespace_append_leaf(const ps_namespace_append *);
uint64_t ps_namespace_append_segment(const ps_namespace_append *);
uint64_t ps_namespace_append_segments(const ps_namespace_append *);
uint64_t ps_namespace_append_incarnation(const ps_namespace_append *);
/* Caller has finalized complete canonical records and retains logical mutation
 * authority. Commit publishes the tail through the existing sync coordinator;
 * it does not allocate versions/index sequences or apply namespace policy.
 * DEFER_SYNC leaves canonical acknowledgement to the caller's root barrier;
 * namespace selection retains its existing dependency barrier. */
int ps_namespace_append_commit(ps_namespace_append *, enum ps_commit_mode,
                               lc_error *);
void ps_namespace_append_close(ps_namespace_append *);
/* Rotation is durable even for NoSync roots. A staged empty successor can be
 * reused after failure. The head commits cutover before sealing its
 * predecessor. An explicit retry can finish a seal interrupted by failure.
 * expected is the caller's observed active segment (zero for initialization).
 * An already selected expected+1 is a successful retry, even after reopen. */
int ps_namespace_rotate(ps_namespace *, uint64_t expected, lc_error *);
/* Unlink the active source after durable cutover if the checkpoint covers
 * its entire tail and retains no bodies. The caller holds logical namespace
 * mutation authority. An ineligible source leaves the retirement NULL. */
int ps_namespace_unlink_active(ps_namespace *, uint64_t expected,
                               ps_namespace_retirement **, lc_error *);
enum { PS_NAMESPACE_RETIRE_LIMIT = PS_STREAM_RETIRE_LIMIT };
/* Unlink a batch of two or more covered inactive segments with no remaining
 * references under logical namespace authority. The caller consumes all
 * returned retirements even when an error follows a durable prefix. */
int ps_namespace_unlink_covered(ps_namespace *, uint64_t minimum_bytes,
                                ps_namespace_retirement **, size_t *,
                                lc_error *);
/* Consume a returned retirement under stream authority alone. If the stream
 * is still busy, the namespace's regular reaper retains the pending work. */
int ps_namespace_retire_unlinked(ps_namespace *, ps_namespace_retirement *,
                                 lc_error *);
/* Consume all returned retirements. The mask reports the streams confirmed
 * durable. Busy members stay on the namespace's retirement list. */
int ps_namespace_retire_unlinked_batch(ps_namespace *,
                                       ps_namespace_retirement *const *,
                                       size_t count, unsigned int *retired,
                                       lc_error *);
int ps_namespace_parse_source_leaf(const char *, unsigned int *, uint64_t *);
int ps_namespace_read_open(ps_namespace *, unsigned int kind, uint64_t id,
                           uint64_t offset, uint64_t bytes,
                           ps_namespace_window **, lc_error *);
lc_pouch_record_io ps_namespace_read_input(ps_namespace_window *);
/* Share an immutable captured range, without re-admitting its stream. */
void ps_namespace_read_retain(ps_namespace_window *);
void ps_namespace_read_close(ps_namespace_window *);
/* Captures segment identities/tails once. Callbacks run outside owner locks,
 * may append/rotate, and observe only this fixed prefix. Memory follows segment
 * participation. Point reads resolve one cached segment directly. Record
 * entries and their fields are borrowed for each callback. Captured typed key
 * metadata precedes uncovered canonical suffixes and uses segment zero. A
 * callback error stops a visit without consuming the view; the next visit
 * starts at the captured beginning again. */
int ps_namespace_view_open(ps_namespace *, ps_namespace_view **, lc_error *);
int ps_namespace_state_view_open(ps_namespace *, ps_namespace_view **,
                                 lc_error *);
int ps_namespace_refresh_state(ps_namespace *,
                               struct lc_pouch_storage_namespace_info *,
                               const struct lc_pouch_storage_state_refresh *,
                               lc_error *);
/* Admit only the selected active range after matching the expected boundary. */
int ps_namespace_view_suffix_open(
    ps_namespace *, const struct lc_pouch_storage_namespace_info *,
    uint64_t start, ps_namespace_view **, lc_error *);
void ps_namespace_view_projection(const ps_namespace_view *,
                                  struct lc_pouch_storage_namespace_info *);
typedef int (*ps_namespace_record_fn)(void *, uint64_t segment,
                                      const lc_pouch_state_entry *, lc_error *);
int ps_namespace_view_visit(ps_namespace_view *, int verify_payload,
                            ps_namespace_record_fn, void *, lc_error *);
void ps_namespace_view_close(ps_namespace_view *);
/* Sealed relocation packs contain canonical encoded body records, not a replay
 * prefix. Their bindings are durable before a key checkpoint can refer to
 * them. Unreferenced packs require logical reclamation after interrupted work.
 * Exact known-span payload and the metadata needed to publish its head, grants
 * and catalog binding are admitted before stream creation. This ordinary claim
 * preserves the maintenance floor. Producers hold no owner lock. The caller
 * still supplies the measured span; this is not a complete compaction budget.
 */
int ps_namespace_body_begin(ps_namespace *, uint64_t bytes,
                            ps_namespace_body **, lc_error *);
/* Admit a complete conservative span before production, then allocate only
 * the bytes actually emitted. Commit permits a shorter nonempty result;
 * close releases unused admission. No output-sized buffer is retained. */
int ps_namespace_body_begin_bound(ps_namespace *, uint64_t maximum,
                                  ps_namespace_body **, lc_error *);
lc_pouch_record_io ps_namespace_body_output(ps_namespace_body *);
const char *ps_namespace_body_leaf(const ps_namespace_body *);
int ps_namespace_body_commit(ps_namespace_body *, lc_error *);
void ps_namespace_body_close(ps_namespace_body *);
/* Offline conversion preparation, not logical reference selection. The caller
 * excludes namespace mutation/reclamation throughout preparation and handoff.
 * Produce a sealed final body pack from one immutable source, retaining all
 * logical facts and verifying integrity. A successful unselected pack is
 * reclaimed through normal namespace maintenance after interrupted handoff.
 * Result owns its decoded fields in the root crypto allocator. */
int ps_namespace_body_recode(ps_namespace *, lc_pouch_crypto *,
                             const lc_pouch_state_entry *, const char *binding,
                             lc_pouch_state_entry *, lc_error *);
/* Offline handoff of a verified body_recode result. Match the entire expected
 * selected record, retain its logical identity, and durably replace its body
 * reference and checkpoint coverage together, including on NoSync roots.
 * Old metadata views/readers remain valid; reclamation is a separate step.
 * An attached logical checkpoint owner rejects this offline operation. */
int ps_namespace_body_select(ps_namespace *,
                             const lc_pouch_state_entry *expected,
                             const lc_pouch_state_entry *converted, lc_error *);
/* Complete offline conversion: begin captures the selected source metadata and
 * admits body construction plus durable key/coverage replacement before body
 * input. Namespace mutation/reclamation exclusion survives until close, with
 * no mutex held over production. Crypto is borrowed until close. Run is single
 * use and replaces the caller's result only after durable publication. Close
 * refunds unused credits; a sealed unselected pack from an interrupted handoff
 * remains eligible for ordinary maintenance. No implicit container growth. */
int ps_namespace_recode_begin(ps_namespace *, lc_pouch_crypto *,
                              const lc_pouch_state_entry *, const char *binding,
                              ps_namespace_recode **, lc_error *);
int ps_namespace_recode_run(ps_namespace_recode *, lc_pouch_state_entry *,
                            lc_error *);
void ps_namespace_recode_close(ps_namespace_recode *);
typedef struct ps_namespace_conversion_result {
  uint64_t visited, converted, matching, maintenance, reclaimed;
  int finished, waiting;
} ps_namespace_conversion_result;
/* Offline bounded namespace conversion, including initial/final source cleanup
 * and reclamation between bodies. Each step visits at most records key slots
 * and maintenance source/piece units. It retains one owned metadata record,
 * never a namespace-wide historical view. Matching descriptors are binding-
 * verified and skipped, so reopening/restarting resumes durable work. Logical
 * mutation invalidates traversal; crypto is borrowed until close. Root policy,
 * collections and root-wide orphan cleanup remain the coordinator's concern. */
int ps_namespace_conversion_open(ps_namespace *, lc_pouch_crypto *,
                                 ps_namespace_conversion **, lc_error *);
int ps_namespace_conversion_step(ps_namespace_conversion *, size_t records,
                                 size_t maintenance,
                                 ps_namespace_conversion_result *, lc_error *);
void ps_namespace_conversion_close(ps_namespace_conversion *);
/* Only fully covered sealed segments and unreferenced sealed body packs are
 * eligible. Captured metadata views defer source retirement with BUSY until
 * their callbacks can no longer open a historical body reference.
 * Removal is durable and idempotent. Captured readers remain valid. Reap closes
 * retired owners after catalog/read pins drain; checkpoint makes space
 * reusable. These calls do not invalidate a logical owner's cached projection.
 */
int ps_namespace_reclaim(ps_namespace *, unsigned int kind, uint64_t id,
                         lc_error *);
#define PS_NAMESPACE_REPACK_BYTES (256U * 1024U)
typedef struct ps_namespace_repack_result {
  uint64_t visited, replaced;
  uint64_t body_bytes, mapping_bytes; /* retained encoded bytes selected */
  int pending, finished_tail;
} ps_namespace_repack_result;
/* Reclaims at most limit pieces: completed pieces behind selected coverage and
 * wholly unused canonical grants beyond the committed tail. Includes covered
 * pieces of the active source. Captures scalar liveness, copies at
 * most one bounded piece without owner locks, then revalidates before durable
 * selection. Existing body readers retain their exact mapping; metadata views
 * defer selection. The optional gate reacquires logical namespace mutation
 * authority for selection after the copy; NULL requires caller exclusion. A
 * cached cursor makes successive bounded calls progress through present
 * mappings. Flush key metadata before starting; newly accepted dirty keys defer
 * the next capture until the next flush. A profitable covered partial tail
 * reserves rotation and replacement together, durably selects an empty
 * successor and seals the old source before copying. The gate also serializes
 * this bounded structural transition. An already selected successor retries
 * its pending seal without another rotation. Wholly unused grants require no
 * checkpoint coverage or body copy; rotation durably fixes their predecessor's
 * tail before deletion. An empty selected head rotates only when retiring its
 * allocated unused grants. Pending references and changed tails defer
 * selection. A sweep revisits earlier pieces after reaching the tail, including
 * bodies superseded between sweeps. */
int ps_namespace_repack(ps_namespace *, unsigned int, uint64_t, size_t limit,
                        ps_piece_select_fn gate, void *,
                        ps_namespace_repack_result *, lc_error *);
/* Drop an abandoned unselected successor before draining retired sources.
 * Mutation exclusion protects live producers; catalog/read pins may defer
 * physical retirement. A failed first append needs no canonical checkpoint
 * before its zero-tail unselected binding can be discarded. */
int ps_namespace_reap(ps_namespace *, uint64_t *closed, uint64_t *pending,
                      lc_error *);
#endif
