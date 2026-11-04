# Collection leases

Collections remain multiwriter whenever no live lease exists. Acquiring a lease
grants exclusive application mutation authority over one namespace/collection
incarnation. Entries have no individual leases. Every append commits separately
under the root's configured durability policy, without renewing or releasing
ownership. Release, expiry and takeover never roll back committed history.

## Ownership and API

`collection.acquire({owner, ttl_seconds, private_read=false})` returns another
owned collection receiver, bound to fresh lease credentials. The original
receiver remains uncredentialed. Equal owner labels do not share ownership.
Acquisition fails immediately with `collection_leased` if ownership is occupied.
`collection.takeover(request, observed_lease_id)` conditionally replaces exactly
that live lease; a changed observation fails with `lease_changed`.

The credentialed receiver supports the ordinary collection methods, keepalive
and release. Successful release closes it; failed release leaves it open for
inspection/retry or local close. Local close never releases durable ownership.
Describe/list expose bounded lease metadata without credentials being logged.
Creation accepts an optional initial lease, published before the catalog name,
so initialization and acquisition have no intervening writer race.

Control write errors can have an uncertain outcome after head publication.
Inspect current ownership before deciding to retry or take over. A failed
release never authorizes clearing a replacement owner's lease. Defaults are a
30-second TTL and public visibility; renew comfortably before expiry. Neither
the receiver nor its close method starts an automatic renewal worker.

While leased, append, deletion, entry metadata, collection metadata and drop
require its current credentials. Expired/replaced credentialed receivers cannot
fall back to unleased mutations, even after ownership becomes free. An upload
captures the ownership generation at admission and checks it again immediately
before publication. An intervening acquisition/release also invalidates an
uncredentialed upload. A rejected append does not bind its ID to new bytes.
Append identity, positions and retry retention survive all ownership changes.

Keepalive extends only lease expiry. It does not alter collection/entry retention
or mutation generations. Maintenance remains authorized to compact, expire and
trim under existing policies; collection removal invalidates every lease.
The fencing high-water mark survives release and restart and never wraps.

## Reads and visibility

By default committed entries remain publicly readable during ownership.
Private acquisition instead permits payload reads only through the current
credentialed receiver until release or expiry. Description, listing and append
status remain available for ownership diagnosis and uncertain-outcome recovery.
Expiry restores public readability without waiting for maintenance.

Visibility is checked while capturing each read, including cursor resumes.
Already admitted reads finish their pinned view even if ownership later changes.
No lock is held during delivery callbacks, and no per-chunk ownership lookup is
needed. Privacy changes neither payload bytes nor the streaming format.

## Persistence and performance

Keep a bounded ownership record in the existing atomically published collection
head, refreshed under metadata authority. Ownership transitions serialize with
final publication, not with the entire lifetime of an upload or lease. Direct
pull encodes under append authority; push and explicit staged pull write bounded
encoded chunks into shared canonical segments and publish metadata at commit. Ownership checks add no per-entry lease update,
extra client/worker per lease, or full history scan.

Acquisition, keepalive, release and takeover durably replace control metadata.
Append does not rewrite the lease or add an ownership sync. Existing canonical
sync fences and batch durability remain effective. Recovery, compaction and
compression conversion preserve ownership; acquisition alone does not force
unsynced entries to become durable.

Collection operations use the root mode guard and their own catalog/session/
append/commit/metadata locks. They do not acquire the ordinary-state/XA root
transaction guard: collection storage is isolated and appends do not enlist in
those transactions. This permits independent collections and lease control to
progress while a producer is paused in an upload callback.

The direct pull path retains per-collection append authority while consuming
its source. Renewal/takeover from other threads, peer committed reads and other
collections can progress, but another append to that same collection waits for
the source to return.
Takeover fences publication; it does not cancel application callbacks. Process
death releases the physical lock. Use `append_staged` for a source that may
wait on external input, or push writers for fragment producers. Staged source
reads and pauses between push writes retain no append lock. Source callbacks
keep the ordinary reentry restrictions described in
[the append contract](collection-writer.md).

## Conversation journals

Acquire before restoring writable runtime state, then use the returned owned
receiver for both event and checkpoint appends. Keep it for the conversation's
lifetime and renew on a separate cadence, including while waiting for model or
tool work. Renewal must not depend on acquiring the application's journal-write
mutex. Stop further dispatch and writes on ownership loss; stop all writers
before releasing. Child conversations acquire their own collections.

Finite JSON generators over already available event data or a captured history
delta fit the pull fast path. They do not wait for future model output. A single
collection lease covers both kinds of record without per-record lease renewal
or release, and parent-retention directives remain atomic with each new append.
Uncredentialed receivers freshly opened by other code paths cannot substitute
for the owned receiver while a lease is active. Private restore/read operations
also use the owned receiver. Consumer integration is a separate deliverable.

The initial backend is Pouch. Capabilities separately advertise leases,
takeover and private reads; remote collection operations remain unsupported.
C89 and Lua expose the same lifecycle and failure behavior. Public layout
changes advance the core ABI once from the latest released ABI.

## Storage upgrade

Lease support introduced the extended canonical head in v0.25.0 with ABI 8.
Current receivers use ABI 9 for [atomic edits](collection-edits.md). On Pouch
root open, released 128-byte collection
heads are automatically upgraded to the 384-byte ownership layout, with no
active lease and an initial fencing token of zero. Positions, identities,
retention and canonical entry files are preserved; no entry replay or body
rewrite is needed for the upgrade.

The upgrade requires exclusive root access, including when the requested
runtime is shared. Stop all existing clients before first opening the root with
the new library. Contention returns `LC_ERR_PENDING` with
`collection_upgrade_busy`; close the other clients and retry. Do not run older
binaries against an upgraded root or downgrade its storage.

Each catalog-bound head is validated, then atomically replaced with its
permissions preserved and its file and directory synced, even for NoSync
roots. Unbound partitions are ignored. Corruption and I/O errors cause open to
fail without marking completion. An interrupted upgrade can leave a mix of old and
new heads; the next open resumes safely. Only after every bound head succeeds
does the root manifest durably record `collection_head_bytes=384`. Later opens
skip the upgrade walk. Upgrade precedes any newly requested compression
conversion. If a previous release left a ready conversion, recovery first
installs that tree and upgrades its heads before recording the new manifest.
Ordinary state record and collection entry formats are unchanged.

## Future server boundary

The server must implement acquisition and conditional takeover as atomic
collection-control operations. Every mutation carries the collection incarnation
and, when leased, its lease ID and fencing token. An uncredentialed mutation is
valid only when no live lease exists. Keepalive/release require current
credentials; readers capture visibility before delivery. Control observations
return owner, ID, fencing token, expiry and private visibility. Append outcome
lookup remains independent of ownership, and the wire payload remains raw JSONL
with separate metadata. The client never emulates remote ownership locally or
silently permits an unsupported lease operation.

## Verification

Use controlled clocks and callback/barrier interleavings for competing owners,
equal labels, expiry, release, conditional takeover, stale keepalive/release,
direct/staged pull and push uploads crossing ownership transitions,
private/captured/resumed reads, duplicate IDs, drop/recreate, restart,
compression and maintenance. Exercise allocation/I/O failures and owner close,
with allocation tracking enabled.
CAI acquires before restoring a writable journal, stops dispatch on ownership
loss, and releases only after its writers stop; consumer integration is separate.

Benchmark matching leased/unleased appends against the preceding implementation:
wall/process CPU, one writer and independent collections, plain/transformed
payloads, NoSync/durable sync, and actual filesystem storage. Count syscalls and
syncs separately from physical IOPS. Host timing is diagnostic, not a flaky test.
