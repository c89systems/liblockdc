# Per-object state retention

Implementation contract for per-object state retention.

## Contract

An application may optionally set retention when creating or updating a state
object. The same public C and Lua APIs apply to local Pouch and remote lockd.
Retention is independent of lease TTL, outbox receipt retention, dead-letter
retention, and completed XA history retirement.

Collection-owned state objects use this same canonical state retention metadata
and body-preserving update behavior through the
[collection metadata methods](collections.md#retention-metadata). Collection
ownership adds identity and ordering metadata. A collection's separate own
retention drops that collection with its remaining entries; it does not assign
an age limit to each child.

| Write option | Meaning |
| --- | --- |
| Omitted | Preserve the existing deadline; a new object has no expiry |
| `retention_seconds = 0` | Remove expiry |
| Positive seconds | Replace expiry with commit time plus that duration |
| Negative, fractional, or out-of-range value | Reject before mutation |

Expiry is an eligibility boundary, not an exact deletion time. Automatic
retention must not delete an object before its deadline. After that deadline,
the application has neither a continued-retention guarantee nor a guarantee
that deletion has already occurred. Explicit application deletion remains
available at any time.

Until maintenance actually deletes it, an expired object is ordinary state:
reads, queries, lease acquisition, updates, and attachments keep their existing
semantics. Reads do not hide it or trigger cleanup. Updating it without a
retention option preserves its expired deadline; explicitly setting a new
positive duration can extend its lifetime if the update wins the deletion race.
Deleting and recreating the same key starts a new object with no inherited
retention unless the creating write explicitly sets it.
With a healthy store and recurring maintenance passes, unprotected expired
objects must eventually be deleted. The lack of a deadline is not permission
to lose scheduled work or starve an object indefinitely.

## Public API

State update, mutation, and metadata request/options structures expose
`has_retention_seconds` and `retention_seconds`. The duration uses the
existing public `long` seconds convention. A zero-initialized request omits
retention; the presence flag distinguishes omission from an explicit zero.
Initializers must clear both fields.
When the presence flag is clear, ignore the duration field entirely, including
validation. A non-zero presence flag selects the option.

The affected C inputs are `lc_update_opts`, `lc_update_req`, `lc_mutate_req`,
`lc_mutate_op`, `lc_metadata_req`, and `lc_metadata_op`;
`lc_mutate_local_req.update` inherits the update options. Metadata validation
must accept retention alone, without requiring `has_query_hidden`.

Cover client operations, lease receivers, streamed/buffered/mapped updates,
local mutation, and business-state participants bound to outbox or managed XA
transactions. Local mutation carries retention through its existing
`lc_update_opts`. Acquire continues to configure a lease, not state retention.
Metadata-only operations may change retention without replacing the body;
selecting retention requires an existing state body or a staged replacement,
and must not turn a lease-only placeholder into a retained object.

Both `lc_lease_save()` and the `lc_lease.save` receiver method accept
`const lc_update_opts *opts` immediately before `lc_error *error`. Preserve
the existing fetched-CAS and JSON content-type defaults when `opts` is `NULL`.
Existing call sites pass `NULL`; there is no parallel `save_ex` variant.
The mapped implementation forwards retention in its single streamed
update. An `acquire_for_update()` handler uses its borrowed lease's normal
write/metadata methods; it needs no separate retention setting on acquire.

Lua uses an optional integer `retention_seconds` on the corresponding request
tables. Omission or `nil` preserves retention; numeric zero clears it. Booleans,
fractions, negatives, and values outside the C range are invalid. For example,
the Lua surface accepts:

```lua
assert(lease:update_json({ status = "complete" }, {
  retention_seconds = 7 * 24 * 60 * 60,
}))
```

State read/describe and update/mutation/metadata results, including
`lc_state_snapshot`, expose:

- `retention_support`: `LC_STATE_RETENTION_UNKNOWN`,
  `LC_STATE_RETENTION_SUPPORTED`, or `LC_STATE_RETENTION_UNSUPPORTED` in C;
  values 0, 1, and 2, respectively; `"unknown"`, `"supported"`, or
  `"unsupported"` in Lua. Zero initialization therefore means unknown.
- `has_retention_expires_at_unix` and `retention_expires_at_unix` in C. The
  timestamp uses `lc_unix_seconds`. Lua represents absence by omitting the
  timestamp field. A present zero means confirmed absence of expiry.

Pouch reports support directly. Support means the backend understands and
persists the policy, not that its maintenance worker is currently enabled.
Remote results report only what that response advertises; HTTP success alone
does not confirm support or application of the option. A staged positive
duration has no final deadline until commit, so its private result omits the
deadline while still reporting known support. A staged clear can report zero;
an inherited absolute deadline remains reportable.
Retention metadata follows the same view as the returned body/version/etag.
Public reads report committed retention, never staged changes. `describe()`
already describes the primary/private view when a live lease has staged state;
its retention follows `state_etag` and `version` rather than silently mixing in
the public deadline. Preserve describe's existing staged-delete view selection.
Use a public read to inspect committed retention.
An absent body, including a lease-only placeholder or staged delete, has no
deadline field; confirmed no-expiry zero is for an existing state body.
Commit/release itself need not change its return type: normal public metadata
reads expose the resulting deadline.
Receiver methods that return only status publish the same information as
receiver properties, without changing their return shape, in both C and Lua.
These properties describe the latest primary/private state observation;
public-read results do not overwrite them. A new state observation replaces
the previous support/deadline information, including clearing it when the
response omits it. A mutating operation with an uncertain outcome invalidates
the receiver's retention observation rather than leaving a stale deadline.
No automatic follow-up read is required.

Update the matching facade comments, public declaration comments, examples,
and [Lua documentation](lua.md) together. There is one backend-neutral API.

## Commit and recovery

Retention changes obey the same lease, fencing, CAS, transaction, and durability
rules as the state write. Rejected preconditions or a write aborted before
COMMIT cannot change the deadline. A lost acknowledgement after COMMIT keeps
the normal uncertain operation outcome; it does not undo committed retention.
Keepalive, attachment-only writes, release, ordinary reads, and metadata changes
that omit retention do not renew it.

For staged work, persist the requested duration or clear directive with the
staged state. Later writes in that transaction inherit the staged directive
when they omit retention; an explicit later directive replaces it. Rollback
preserves the original committed deadline. A remove followed by a new create
must not inherit the removed object's deadline.

For a direct write, commit time is its publication timestamp. For an implicit
single-participant transaction, it is the first durable commit/promotion
timestamp. For coordinated transactions, persist a single commit timestamp
with the existing immutable global COMMIT decision and derive participant
deadlines from that timestamp. Add no second decision log. Recovery and
idempotent completion preserve that timestamp and the resulting deadlines;
replaying a decision must not grant another full retention period. If recovery
finishes after expiry, it still applies the committed transaction before
ordinary retention may delete the published state.

Use checked arithmetic before accepting a duration, and again when choosing
the commit timestamp. Validate every participant's selected positive duration
before global COMMIT, not just the last state written. All commit paths,
including implicit vote completion and explicit decisions, must persist the
timestamp once; neither generic metadata-update time nor last-replay time is
an authoritative substitute. A timestamp-overflow failure must be detected
before durable COMMIT; a chosen COMMIT cannot later be converted into ABORT.
Persist absolute Unix deadlines. Clock rollback can delay eligibility, but does
not rewrite deadlines or resurrect a deleted object. Old committed transactions
with no selected retention need no timestamp backfill. A retention-bearing
COMMIT missing its required timestamp must not be repaired by guessing replay
time; report that inconsistent state and preserve it for recovery.

Successful release of an implicit, single-key native lease retires the decision
created for its retention change after applying commit or rollback. Normal
renewals therefore leave no terminal transaction backlog for the next client
open. In NoSync mode, retirement first synchronizes the participant namespace;
the cleanup cannot outlive the state it proves applied. Cleanup failure remains
an uncertain release error with a recoverable decision, never proof of rollback.
Explicit decisions, explicitly acquired transactions, joined transactions and
managed/external XA retain their existing outcome and recovery rules. Startup
still recovers interrupted work and decisions left by older clients.

## Pouch persistence and maintenance

Store retention in the canonical state-record metadata, separately from the
opaque lease metadata that acquire, keepalive, and release reconstruct.
Preserve it through staging, promotion, replay, metadata-only writes, snapshots,
compaction, compression, and encryption. Records without retention metadata
mean no expiry and require no backfill or rewrite on open.

Reuse Pouch's compaction worker and its configured cadence. The current default
interval is 30 minutes. Add no per-object timers, dedicated janitor thread, or
foreground expiry checks. Disabling background compaction also disables these
automatic maintenance passes; deadlines remain durable and eligible work is
rediscovered when maintenance is enabled again.

The existing worker removes ordinary namespaces from its work queue after an
attempt. Therefore a one-time mutation notification is insufficient: an object
can expire after that attempt, without another write. Keep a coalesced durable
retention-work hint per affected namespace until no expiring objects remain.
Durably establish the hint before committing its first retention-bearing state,
including publication during transaction recovery. A failed write may leave a
harmless extra hint; committed state must not lose cleanup scheduling after a
crash, even if its acknowledgement was lost. Hint creation, publication, and
hint removal must use the same namespace authority: a worker cannot retire
the hint between its creation and the write that needs it. Before removing a
hint, refresh the authoritative shared-writer view and confirm no committed
expiring state or unfinished cleanup/retirement remains. A future deadline or
temporarily protected object keeps its hint. Existing transaction recovery
remains responsible for pending application stages; promotion establishes the
hint before publishing them. No per-object sidecar is needed.

Before durably retiring the hint, synchronize the canonical namespace containers
under that same authority, even with NoSync durability. This includes an idle
cleanup finishing after a concurrent retention clear or delete. If synchronization
fails, preserve discovery for retry; ordinary foreground NoSync writes remain
unsynchronized until a durability boundary requires them.

For a namespace whose hint already exists, retention does not rewrite/fsync
that control file on every state write. A bounded existence/creation check
under namespace authority is allowed, especially for shared writers; do not
promise zero filesystem operations while requiring crash-safe scheduling.

At startup and subsequent maintenance turns, discover these hints without
enumerating all application namespaces. Visit selected namespaces fairly and
use their canonical metadata projection, without fetching/decoding state bodies
or using the query index to decide expiry. Ordinary cold replay, integrity
validation, and compaction can still read record payload bytes; do not weaken
those checks or claim that cold maintenance requires no payload I/O.
Walking the selected namespace's resident metadata is allowed and scales with
that namespace. Do not allocate a list of every expired object before starting
deletion. A pass has a finite parent-object attempt budget; protected objects
and failures consume that budget too, and unfinished work remains scheduled
for a later normal pass. This bounds deletion work, not the cost of replay or
a selected-namespace metadata walk. Keep advisory cursors for fair traversal,
and share turns fairly with queued compaction, durable terminal-reclaim markers,
and collection discovery through one source rotation. Empty sources fall
through within the same pass; discovery errors advance the rotation before
being reported. Continuously ready sources each receive an attempt within
four passes, while their namespace cursors preserve fair traversal within
each source. This is a scheduling bound, not an expiry deadline. Do not
add a busy catch-up loop, shorten the interval because work is overdue, or
introduce a separate copy of the state cache for retention.

Perform eligible logical deletion during this maintenance pass even if the
namespace is too small to qualify for segment compaction. Append ordinary
durable tombstones and reuse normal query-index invalidation and terminal
reclamation. Physical space reclamation follows normal snapshot installation,
compaction thresholds, history pins, and obsolete-file deletion grace. Retention
is neither a secure-erasure guarantee nor a disk-space reclamation deadline.

Run the retention turn outside the namespace guard already held by
`lc_pouch_maintenance_run()`. Its cleanup operation must follow the existing
transaction-guard/namespace authority order; do not call a locking client
facade from inside the locked compaction body. The same worker can perform
retention first and then ordinary compaction without nesting these lifecycles.
Use synchronous root-owned storage operations, extracting the small shared
lease/transaction policy helpers where needed. Do not open a public
`lc_xa_coordinator` or retain its `lc_client` from this worker: coordinator open
starts another journal thread and its store retains the client whose teardown
joins the compaction worker. Do not construct a fake client, open another Pouch
root, or invoke application update handlers to bridge that ownership boundary.
The root owns the maintenance context, and joins the worker before freeing it.

The existing `retention_updated_before_unix` sweep is a separate explicit
namespace-wide age policy. Do not enable it as the automatic implementation:
`updated_at` changes for reasons other than an explicit retention reset, and
its direct state-delete path does not provide the required object-lifecycle
checks and attachment cleanup.

## Deletion safety and ownership

Selection grants no right to delete. Attempt the existing exact-key cleanup
lease/transaction without waiting for an application lease. Compare the state
observed before installing that claim with the selection; skip and reschedule
a changed candidate. Then reread the authoritative state, verify its body
exists and its current deadline is due, and use the fresh version/CAS under
this authority. Acquiring the cleanup lease itself can advance physical
metadata versions; its own version change must not make every cleanup fail.
A stale selection must not delete a replacement or bypass a renewed/cleared
deadline, and later publication must not reuse the cleanup operation's authority.

Skip foreign live leases and unresolved coordinated transaction reservations,
including XA work whose lease has expired and COMMIT decisions still awaiting
application. Check recovery authority, not just whether a lease ID is nonempty.
An expired unmanaged implicit lease is not a permanent retention pin: use the
existing abandoned-lease/staging cleanup rules before the fresh eligibility
check, including its staged attachments. Do not wait indefinitely for an
application to revisit that key. Retention must never invent a coordinated
decision or treat lease expiry as permission to roll back prepared XA work.

Expire the object and its committed, owned attachments through the existing
transactional state/attachment deletion machinery. A failed cleanup must roll
back before COMMIT or remain durably recoverable after COMMIT. It must not
leave an acknowledged partial deletion or let an old cleanup delete attachments
belonging to a recreated object. Preserve monotonic fencing and ordering
high-water values, and close maintenance-owned handles on every exit path.
An fsync commit group alone is not transactional rollback. Use the existing
coordinated Pouch decision/replay path for cleanup that must survive a crash
between parent and attachment application; bare implicit lease release is not
proof of that guarantee. Keep an unfinished cleanup's transaction ID reachable
from the namespace work hint until its application is complete, and resume it
before selecting fresh work for that namespace. The hint is a work reference,
never another decision journal. After COMMIT is durable, complete that deletion
idempotently on recovery; do not abort it because wall time moved back or the
cleanup lease expired. Protect the object from recreation until attachment and
parent application finishes. An expired cleanup lease must not let another
writer bypass that unfinished transaction, even if the parent body is already
absent. Apply the reservation check at the existing acquisition/mutation
authority boundary; it is a transaction check, not foreground expiry cleanup.
Do not call the public cleanup facade while holding an inverse-order lock.

The existing namespace hint also carries the bounded cleanup work reference:
one active transaction ID, its target key, and an active/finished phase. Its
bounded claim metadata also preserves the cleanup fencing token and state
ordering high-water before acquiring the claim. Recovery can then restore its
reservation after a crash has deleted the parent but not finished its owned
attachments, without resetting fencing or consulting application data. These
are claim facts, not a second transaction outcome or a saved application body.
The phase records work completion, not a competing COMMIT/ROLLBACK decision. Use
this ordering:

1. Claim the namespace work slot under the normal authority order, and durably
   record its fresh transaction ID and target before creating cleanup work.
   Workers encountering an occupied slot resume it under its transaction
   guard, or skip it without waiting if another worker holds that guard.
2. Prepare the existing `.txns` record with the known local participant before
   acquiring/staging its lease. Do not enter implicit-leader discovery: joining
   a previously unknown transaction currently walks all application namespaces.
   A crash before prepare can abandon the reference only if no owned claim or
   stage exists; missing decision data with owned work is an error to preserve.
3. Validate the candidate, stage its owned deletions, and use the existing
   decision/application machinery. An abandoned undecided cleanup follows
   normal rollback rules; a decided cleanup replays only its recorded outcome.
   Retry only its original staged work, never fresh attachments found under a
   reused key. A missing parent alone is not proof that application finished.
4. After every owned stage, parent/attachment change, and lease finalization is
   applied, establish a durability barrier for that work before durably marking
   the hint's reference finished. An fsynced control file must not get ahead
   of the journal/data it claims are complete, including with `durable_sync=0`.
5. Retire that internal transaction's journal/staging records, then durably
   clear its finished work reference. A restart with a finished reference and
   an already-retired journal completes retirement; it must not create a new
   transaction or guess a decision from absence. Never retire decision data
   while the reference is active. Finally, remove an otherwise empty hint.

These are ordered uses of one namespace work item and the existing transaction
journal, not a new coordinator or recovery service. Do not assume raw `.txns`
cleanup history is automatically retired by the public managed-XA policy:
that policy tracks managed `.xa` completion records. Cleanup must retire its
own completed internal history through the ordering above, so expiry does not
replace state growth with permanent maintenance-journal growth.

Normal cleanup barriers synchronize the files touched by its existing commit
group. When resuming an earlier cleanup, include the referenced namespace's
existing canonical containers as well: a new thread or process cannot infer
durability from its own list of writes. Confirm the existing decision's
durability before applying it, and all prior partial data application before
marking work finished. This recovery-only synchronization does not enumerate
other application namespaces or decode state bodies.

Library-owned queue, outbox, inbox, command, checkpoint, and coordinator records
keep their existing lifecycle policies. Reject explicit generic retention on
reserved records before mutation, consistently across endpoints; business state
inside an outbox/XA transaction remains eligible for this feature. Retention
must not expire recovery checkpoints independently of their owning job.

Failures remain observable through the existing structured logging and
maintenance result paths. Keep failed work for retry and allow other eligible
objects to progress. Fair traversal must prevent a busy object or an isolated
cleanup failure from starving other eligible objects. If canonical corruption
prevents a trustworthy namespace projection, skip that namespace and report
the error; continue other namespaces rather than reconstructing around corrupt
records or deleting uncertain data. A committed cleanup that cannot finish may
hold its namespace's single work slot; report it and continue other namespaces.
Do not overwrite that active reference to simulate fairness. Once the error
clears, resume it before advancing the namespace cursor. A history pin may
preserve physical history but does not prevent valid logical expiry deletion.

## Remote lockd protocol and compatibility

Use the same requests for Pouch and remote lockd. For explicitly selected
retention, send the decimal `X-State-Retention-Seconds` header on `/v1/update`,
`/v1/mutate`, and `/v1/metadata`, including zero. Keep existing state and mutation
request bodies unchanged and preserve true streaming. Do not set retention by
following a body update with a separate metadata update: both changes must
share one transaction and failure boundary.

When retention is omitted, send no new headers, body members, discovery calls,
or additional requests. Existing calls must keep working against older lockd
versions without retention support. Parse legacy responses successfully when
all retention response metadata is absent. Require no minimum server version
for the normal path.
Conversely, a retention-aware server must preserve an object's deadline when
an older client omits the option; an ordinary update must not silently clear
expiry or renew it from `updated_at`.

The proposed server response contract uses optional headers on successful
state operations:

| Header | Meaning |
| --- | --- |
| `X-State-Retention-Supported: 1` | Server advertises this retention contract |
| `X-State-Retention-Supported: 0` | Server explicitly reports no support |
| Support header and deadline both absent | Support is unknown for this response |
| `X-State-Retention-Expires-At-Unix: N` | Actual deadline of the reported state view; zero means no expiry |

A positive staged duration can advertise support while omitting its unresolved
deadline. A confirmed existing committed body advertises support and includes
its deadline or zero; staged-absent and no-content responses omit the deadline.
Header names are case-insensitive and HTTP whitespace follows the existing
parser rules. Support accepts exactly `0` or `1`; deadlines are non-negative
decimal values representable by `lc_unix_seconds`. Accept identical repeated
values, but reject conflicting duplicates. A valid deadline also confirms
support when the separate support header is absent; a deadline together with
explicit unsupported status is contradictory. Malformed, contradictory, or
out-of-range retention headers are protocol errors. The client never fills a
missing deadline from its request or local clock. Support observations belong
to individual responses and must not be cached as a guarantee for every server
behind a failover client.

Use only the final successful response's metadata. Clear fields between retry
attempts and HTTP response blocks so interim responses, redirects, errors, or
another endpoint cannot supply a deadline absent from the final response.
Apply the same rules to common JSON and streaming/multipart response parsers.
A metadata parse error after a successful remote mutation is an uncertain
outcome, not proof of rollback, and must not trigger a new downgrade retry.

An older server may reject an explicit retention request or silently ignore
its header. Return its error normally, or preserve its successful result with
unknown support and no confirmed deadline. Do not retry a failed mutation with
the option removed, synthesize enforcement, or introduce remote-only APIs.
An explicit unsupported response likewise does not replace the server's own
operation outcome with a client-side capability error.

This defines the client and future server contract. Implementation here does
not modify lockd or promise cleanup by a server that lacks support. Server-side
deletion follows the same eligibility, preservation, and transaction rules;
its own maintenance cadence may differ from Pouch's.

The compatibility requirement concerns existing calls and older remote
servers. Extending transparent public C structures changes the shared-library
ABI. State retention introduced core ABI 6 in v0.22.0, after ABI 5 in v0.21.0.
Subsequent collection layout changes use core ABI 9; see
[current ABI compatibility](../README.md#c-abi-and-storage-compatibility).
Package metadata and provider linkage must agree. Recompile C consumers
and rebuild the optional PostgreSQL provider against the matching core. Existing
initialized request calls retain their behavior after recompilation. Callers
upgrading from before v0.22.0 must additionally supply the mapped C save options
argument (`NULL` preserves the previous behavior). This does not
promise that old Pouch storage engines can share retention-bearing stores with
new writers.
The optional provider's own ABI is independent; updating its core-ABI
dependency alone does not require advancing its own SOVERSION.
Do not add storage migrations, compatibility shims, or a format-version bump
merely to track the SDK release.

## Acceptance gates

Use an injected clock and explicit test maintenance passes for deterministic
expiry boundaries. Avoid sleeping for retention periods or the default worker
interval. Required coverage includes:

1. Create/update/mutate/metadata set, preserve, replace, and clear; multiple
   staged writes; retention-only metadata updates; mapped save with and without
   options; absent-body errors; ignored unselected C values; invalid selected
   values and arithmetic overflow across every commit path/participant.
2. Exact deadline eligibility, continued normal reads/queries/writes before
   cleanup, explicit extension of an already expired object, clock rollback,
   and deletion only when a maintenance pass performs it.
3. Idle namespaces, restart and crash recovery, disabled/re-enabled maintenance,
   below-compaction-threshold deletion, bounded work, and fairness across busy
   objects and namespaces. Cover publication racing hint removal and unrelated
   maintenance sharing turns. Prove no root-wide application scan, decoded-body
   reads for eligibility, per-object control files, or repeated control-file
   writes/fsyncs once a namespace hint exists. Separate cold replay/integrity I/O
   from additional retention work. Cover a root opened with existing hints
   before client construction completes, and close/unload during maintenance
   without extra worker threads or retained-client shutdown cycles.
4. CAS failures, single/shared writers, renewal/clear/delete/recreate races,
   active and abandoned implicit leases, attachment ownership, internal cleanup
   lease version changes, and fencing high-water preservation after deletion
   and compaction. Assert no foreground lease wait or reversed lock order.
5. Implicit rollback/commit, coordinated Pouch and PostgreSQL commit, lost
   acknowledgements, delayed application, and repeated recovery preserving one
   commit timestamp. Protect prepared work and recovery checkpoints; resume a
   decided cleanup from its namespace hint after crash, lease expiry, and
   backward clock movement, before allowing recreation or retiring its work.
   Place crash cuts before prepare, after staging, after decision, between
   parent/attachment application, after durable completion, after journal
   retirement, and before clearing the hint. Prove bounded journal growth,
   durability ordering with `durable_sync=0`, and no implicit-leader root scan.
6. Marker, append, allocation, snapshot, and partial attachment-cleanup failures;
   leak tracking and restart convergence without partial or stale deletion.
7. C and Lua parity for client, lease, participant, mapped, callback, and
   streaming paths; describe following its primary view; public/private
   isolation; no-content versus confirmed zero versus pending/unknown deadline;
   receiver metadata invalidation after an unconfirmed/uncertain operation.
8. HTTP fixtures for a strict legacy server with retention omitted, explicit
   retention ignored or rejected, older-client updates preserving expiry on a
   retention-aware server, advertised support/unsupported responses,
   deadline-only confirmation, malformed/duplicate metadata, interim response
   blocks, and differing failover responses. Assert unchanged normal request
   counts/bodies and no downgrade retry. Use fixtures for
   retention-capable lockd until a real server implements the contract; retain
   normal compatibility E2E against the existing server.
9. Relevant GNU/musl and ARM runtime checks, public ABI/export/package metadata,
   and real installed static/shared C and Lua consumers. Compare ordinary
   no-retention writes and reads with the released baseline; separately measure
   retention-enabled writes and a maintenance batch to expose additional I/O
   and prevent new full-state materialization in retention bookkeeping and
   streamed updates, or root-wide scans. Preserve the existing callback snapshot
   contract. Document that a bounded deletion batch does not make namespace
   replay/selection constant time.

## Implementation boundaries

The integration points are the request/result structures in
[`lc.h`](../include/lc/lc.h), the client/lease/Lua forwarding paths, HTTP request
construction in [`lc_state_api.c`](../src/lc_state_api.c), and both common and
streaming response-header parsers. Pouch state persistence and promotion live
in [`lc_pouch_state.c`](../src/lc_pouch_state.c); lease/transaction/attachment
policy lives in [`lc_pouch_client.c`](../src/lc_pouch_client.c). The existing
worker and durable maintenance scheduling are in
[`lc_pouch.c`](../src/lc_pouch.c). Extend those boundaries rather than adding a
parallel storage engine, transaction coordinator, or scheduler.
