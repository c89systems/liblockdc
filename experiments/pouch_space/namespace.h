#ifndef POUCH_SPACE_NAMESPACE_H
#define POUCH_SPACE_NAMESPACE_H
#include "lc_pouch_state_record.h"
#include "root.h"

typedef struct ps_namespace_append ps_namespace_append;
typedef struct ps_namespace_window ps_namespace_window;
typedef struct ps_namespace_view ps_namespace_view;
typedef struct ps_namespace_snapshot ps_namespace_snapshot;
typedef struct ps_namespace_body ps_namespace_body;
enum ps_namespace_append_mode {
  PS_APPEND_PREFIX = 0,
  PS_APPEND_COMPLETE,
  PS_APPEND_MAINTENANCE
};
typedef struct ps_namespace_status {
  uint64_t active_segment, segments, tail, runs;
  uint64_t snapshot, snapshot_boundary;
  uint64_t bound_segments, bound_snapshots, bound_bodies, pending_retirements;
} ps_namespace_status;
typedef struct ps_namespace_source {
  const char *leaf;
  unsigned int kind;
  uint64_t id, tail, capacity;
  int sealed;
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
/* Conservative durable work marker, independent of the segment head/tail.
 * Ensure precedes expiry-bearing publication, including for NoSync roots. A
 * failed mutation may leave a harmless positive hint. */
int ps_namespace_retention_hint_ensure(ps_namespace *, lc_error *);
/* Metadata-only: no segment opening, canonical replay or body reads. */
int ps_namespace_retention_hint_get(ps_namespace *, int *, lc_error *);
/* One active producer per namespace, explicit BUSY on contention. Unrelated
 * namespaces and captured readers progress independently. target==0 disables
 * automatic rotation; otherwise rotate before the next append when the active
 * committed tail has reached target. PREFIX admits a known minimum (or one
 * byte when zero), with later unknown-size growth using ordinary admission.
 * COMPLETE requires exactly minimum positive bytes and preserves the standing
 * floor; MAINTENANCE is complete bounded work permitted to spend that floor.
 * Plan successor, binding, grants and head selection before staging. Preferred
 * geometric growth falls back to an exact claim near capacity. Commit selects
 * its head only after canonical
 * bytes are complete; abort leaves the predecessor selected. No lock spans the
 * producer callback. Routing follows root durability; a predecessor seal waits
 * for durable routing before the next mutation proceeds. */
int ps_namespace_append_begin(ps_namespace *, uint64_t target, uint64_t minimum,
                              enum ps_namespace_append_mode,
                              ps_namespace_append **, lc_error *);
lc_pouch_record_io ps_namespace_append_output(ps_namespace_append *);
/* Label remains stable after append close while a namespace pin is held. */
const char *ps_namespace_append_leaf(const ps_namespace_append *);
uint64_t ps_namespace_append_segment(const ps_namespace_append *);
uint64_t ps_namespace_append_segments(const ps_namespace_append *);
/* Caller has finalized complete canonical records and retains logical mutation
 * authority. Commit publishes the tail through the existing sync coordinator;
 * it does not allocate versions/index sequences or apply namespace policy. */
int ps_namespace_append_commit(ps_namespace_append *, lc_error *);
void ps_namespace_append_close(ps_namespace_append *);
/* Rotation is durable even for NoSync roots. A staged empty successor can be
 * reused after failure. The head commits cutover before sealing its
 * predecessor; recovery finishes that seal before admitting mutations.
 * expected is the caller's observed active segment (zero for initialization).
 * An already selected expected+1 is a successful retry, even after reopen. */
int ps_namespace_rotate(ps_namespace *, uint64_t expected, lc_error *);
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
 * entries and their fields are borrowed for each callback. A selected snapshot
 * precedes its newer segments and is reported with segment zero. A callback
 * error stops a visit without consuming the view; the next visit starts at the
 * captured beginning again. */
int ps_namespace_view_open(ps_namespace *, ps_namespace_view **, lc_error *);
typedef int (*ps_namespace_record_fn)(void *, uint64_t segment,
                                      const lc_pouch_state_entry *, lc_error *);
int ps_namespace_view_visit(ps_namespace_view *, int verify_payload,
                            ps_namespace_record_fn, void *, lc_error *);
void ps_namespace_view_close(ps_namespace_view *);
typedef struct ps_namespace_snapshot_plan {
  uint64_t metadata_bytes, body_bytes;
} ps_namespace_snapshot_plan;
typedef int (*ps_namespace_snapshot_plan_fn)(void *, ps_namespace_view *,
                                             uint64_t boundary,
                                             ps_namespace_snapshot_plan *,
                                             lc_error *);
/* Capture the current prefix and invoke the metadata-only planner before any
 * rotation or output creation. Namespace mutation authority covers planning
 * and preparation, never the subsequent producer. The plan declares exact
 * metadata and optional body-pack sizes. One maintenance claim funds rotation,
 * both outputs and final head selection; failed admission stages nothing.
 * Only one builder per namespace; ordinary appends may proceed during output.
 * The caller owns logical completeness, high-water marks and reference
 * protection. A selected snapshot can be rewritten without advancing the
 * covered prefix; a namespace without a snapshot still needs a nonempty sealed
 * prefix. The view is borrowed until close. */
int ps_namespace_snapshot_begin(ps_namespace *, ps_namespace_snapshot_plan_fn,
                                void *, ps_namespace_snapshot **, lc_error *);
ps_namespace_view *ps_namespace_snapshot_view(ps_namespace_snapshot *);
lc_pouch_record_io ps_namespace_snapshot_output(ps_namespace_snapshot *);
const char *ps_namespace_snapshot_leaf(const ps_namespace_snapshot *);
uint64_t ps_namespace_snapshot_boundary(const ps_namespace_snapshot *);
/* Borrowed output of the optional body pack, owned by the snapshot builder.
 * Commit stabilizes and binds it before selecting the metadata snapshot. */
lc_pouch_record_io ps_namespace_snapshot_body_output(ps_namespace_snapshot *);
const char *ps_namespace_snapshot_body_leaf(const ps_namespace_snapshot *);
/* Sealed relocation packs contain canonical encoded body records, not a replay
 * prefix. Their bindings are durable before a metadata snapshot can refer to
 * them. Unreferenced packs require logical reclamation after interrupted work.
 * Exact known-span payload and the metadata needed to publish its head, grants
 * and catalog binding are admitted before stream creation. This ordinary claim
 * preserves the maintenance floor. Producers hold no owner lock. The caller
 * still supplies the measured span; this is not a complete compaction budget.
 */
int ps_namespace_body_begin(ps_namespace *, uint64_t bytes,
                            ps_namespace_body **, lc_error *);
lc_pouch_record_io ps_namespace_body_output(ps_namespace_body *);
const char *ps_namespace_body_leaf(const ps_namespace_body *);
int ps_namespace_body_commit(ps_namespace_body *, lc_error *);
void ps_namespace_body_close(ps_namespace_body *);
/* Durable selection under either root policy. Busy mutation admission can be
 * retried; after admission this is a single publication attempt. On failure,
 * close and rebuild. No reclamation runs after successful head publication. */
int ps_namespace_snapshot_commit(ps_namespace_snapshot *, lc_error *);
void ps_namespace_snapshot_close(ps_namespace_snapshot *);
/* Caller proves no current logical references need this source. Only segments
 * covered by the selected snapshot, older selected snapshots, and sealed body
 * packs are eligible.
 * Removal is durable and idempotent. Captured readers remain valid. Reap closes
 * retired owners after catalog/read pins drain; checkpoint makes space
 * reusable. These calls do not invalidate a logical owner's cached projection.
 */
int ps_namespace_reclaim(ps_namespace *, unsigned int kind, uint64_t id,
                         lc_error *);
int ps_namespace_reap(ps_namespace *, uint64_t *closed, uint64_t *pending,
                      lc_error *);
#endif
