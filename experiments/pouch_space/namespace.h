#ifndef POUCH_SPACE_NAMESPACE_H
#define POUCH_SPACE_NAMESPACE_H
#include "lc_pouch_state_record.h"
#include "root.h"

typedef struct ps_namespace_append ps_namespace_append;
typedef struct ps_namespace_window ps_namespace_window;
typedef struct ps_namespace_view ps_namespace_view;
typedef struct ps_namespace_snapshot ps_namespace_snapshot;
typedef struct ps_namespace_status {
  uint64_t active_segment, segments, tail, runs;
  uint64_t snapshot, snapshot_boundary;
  uint64_t bound_segments, bound_snapshots, pending_retirements;
} ps_namespace_status;

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
 * committed tail has reached target. Reserve minimum bytes (or one byte when
 * zero) in a private successor. Commit selects its head only after canonical
 * bytes are complete; abort leaves the predecessor selected. No lock spans the
 * producer callback. Routing follows root durability; a predecessor seal waits
 * for durable routing before the next mutation proceeds. */
int ps_namespace_append_begin(ps_namespace *, uint64_t target, uint64_t minimum,
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
/* Capture the selected snapshot and its sealed log prefix, rotating a nonempty
 * active segment first. Only one builder per namespace; ordinary appends may
 * proceed while the caller writes complete canonical snapshot records. The
 * caller owns logical completeness, high-water marks and reference protection.
 * An empty/unadvanced prefix is invalid. The view is borrowed until close. */
int ps_namespace_snapshot_begin(ps_namespace *, ps_namespace_snapshot **,
                                lc_error *);
ps_namespace_view *ps_namespace_snapshot_view(ps_namespace_snapshot *);
lc_pouch_record_io ps_namespace_snapshot_output(ps_namespace_snapshot *);
const char *ps_namespace_snapshot_leaf(const ps_namespace_snapshot *);
uint64_t ps_namespace_snapshot_boundary(const ps_namespace_snapshot *);
/* Durable selection under either root policy. Busy mutation admission can be
 * retried; after admission this is a single publication attempt. On failure,
 * close and rebuild. No reclamation runs after successful head publication. */
int ps_namespace_snapshot_commit(ps_namespace_snapshot *, lc_error *);
void ps_namespace_snapshot_close(ps_namespace_snapshot *);
/* Caller proves no current logical references need this source. Only segments
 * covered by the selected snapshot or older selected snapshots are eligible.
 * Removal is durable and idempotent. Captured readers remain valid. Reap closes
 * retired owners after catalog/read pins drain; checkpoint makes space
 * reusable. These calls do not invalidate a logical owner's cached projection.
 */
int ps_namespace_reclaim(ps_namespace *, unsigned int kind, uint64_t id,
                         lc_error *);
int ps_namespace_reap(ps_namespace *, uint64_t *closed, uint64_t *pending,
                      lc_error *);
#endif
