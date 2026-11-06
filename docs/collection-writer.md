# Incremental collection appends

Implementation contract extending [ordered collections](collections.md).
The initial backend is Pouch, with matching C and Lua surfaces. Chunk/index
records add no public ABI change; older libraries cannot read these new record
kinds, while existing contiguous records remain readable. Remote
collections remain unsupported. Collection receiver and capability additions
now use core ABI 9, advancing once from ABI 8 in v0.25.0 for atomic edits and
read preconditions. Rebuild C consumers and the optional PostgreSQL provider.
For an atomic cut/replacement/recovery marker, use an [edit](collection-edits.md);
ordinary append and push writers remain independent atomic appends.

## Scope and publication

A bound append writer validates, compacts, hashes and encodes pushed JSON
fragments once. Bounded codec chunks go directly into shared rolling canonical
segments. A small final state record publishes the complete entry; commit does
not read back or copy its payload. No whole-entry buffer, spool, producer thread
or body cache is used. Pull and push share codecs, projection publication and
the durable group-sync protocol.

This does not enlist collections in XA or add append batches. A downstream
repository may publish immutable collection-position references through its
existing commit marker. Its readers must select committed references before
forwarding body bytes. Physically committed but unreferenced entries are not a
downstream publication and may require downstream garbage collection.

## C and Lua API

`lc_collection_append_begin(collection, request, &writer, error)` returns an
owned `lc_collection_writer`. It borrows no request strings after returning.
The request is the same initialized `lc_collection_append_req` used by pull
append, including the required append ID and optional entry/parent retention.

The writer has matching receiver methods and free functions:

- `write(bytes, count, error)` accepts arbitrary fragments of one JSON object.
  Bytes are borrowed only for that call; zero bytes do not signal EOF.
- `commit(result, error)` finishes validation and publishes one complete entry.
  Its result and retry rules match pull append. Position is assigned at
  publication, so concurrent writers are ordered by commit rather than begin.
- `abort(error)` abandons unpublished chunks for maintenance. It is idempotent and cannot
  undo a committed or uncertain published append.
- `close()` releases the writer, aborting any unfinished append. Cleanup never
  publishes. Callers close successful and failed writers as well.

Lua exposes `collection:append_begin(request)` and writer `write`, `commit`,
`abort`, and `close`, without decoding or encoding application JSON. `write`
and `abort` return true or the normal structured SDK error; `commit` returns
the append result or an error. C returns normal `LC_OK`/SDK status codes.

For a source that already supplies a pull interface, select
`lc_collection_append_staged(collection, request, source, result, error)` or
the matching receiver method; Lua exposes `collection:append_staged(request,
source)`. This consumes the source once through the same writer, using a
bounded 16 KiB heap input chunk. It introduces no second staging engine and no
automatic path selection. The `append_staged` capability reports availability.
Ordinary `append` retains its direct path and can be cheaper for fast inputs.

The C source remains borrowed: the helper never resets or closes it. Lua owns
its normal source adapter and closes that adapter on every outcome, as for
ordinary append. A zero-length read without error supplies EOF and attempts
commit; a source error, malformed input or oversized read discards unpublished
chunks. Signal cancellation through the source's error convention. A commit
error may still represent publication; reconcile with append status and a fresh
source, never by implicitly replaying an exhausted source.

Staged pull keeps ordinary append's callback rules. Operations on the same
receiver and mutations through peer receivers of this collection fail during
the source callback. Peer reads and independent collections may progress.
Closing public owners from a callback defers reclamation until the call ends.
No publication locks span source reads, allowing slow concurrent producers and
maintenance to progress. Source errors preserve their original SDK error.

A writer accepts sequential calls from different threads; overlapping calls
are rejected. Closing an active writer defers reclamation until that operation
unwinds. It retains its collection/client independently, so closing their
public receivers does not invalidate the owned writer. A write/validation/I/O
failure during encoding or publication leaves a terminal writer that can always
be aborted and closed. Argument checks before starting an operation need not
consume the writer. A successful commit is terminal too; retry recovery uses
append status or a new writer/source with the same append ID. Cancellation is
explicit abort between fragment calls, or close. No cancellation worker is created.

## Pending storage and concurrency

Public push writers keep no root, namespace, catalog, append, commit or metadata
publication lock while waiting for the producer. Parser and codec scratch are
bounded independently of entry size; producer fragments are borrowed only for
the call. Encoding can retain one bounded codec chunk before emitting it.

All append forms use shared rolling canonical segments. Direct pull validates
and encodes into an unpublished contiguous range while retaining append
authority. Push and staged pull encode independent units directly into exclusive
ranges under short append authority. A unit covers at most 8 MiB of plaintext,
but consumes only bounded codec scratch, never an 8 MiB input buffer. A shorter
available range reduces that unit's input limit before it fills. Ranges share
segment inodes; neither units nor entries require an inode of their own.
Reservations include bounded framing headroom, so a target-sized plaintext
value does not need another codec unit just for its metadata or authentication
tag. This headroom is canonical address space, not an input buffer.
Payload writes within a reserved unit run without append authority, so slow
storage does not retain that lock. Ownership is checked when reserving and
finalizing units and again at publication.

A compact immutable index records canonical unit offsets. Full index pages are
appended as input arrives. Its 64-way carry stack is fixed in size, and
finalization writes only the remaining bounded index pages. The final state
record assigns a position and publishes the index root. Each upload has its own
identity and each unit its own ordinal and codec context. Unit metadata is
immutable. The publication records a logical manifest covering committed entry
identity, whole-body fingerprint and unit count. Its descriptor authenticates
that manifest while retaining the first unit's exact cipher core. Root metadata
references reuse the publication's exact descriptor. Retention changes leave
this logical evidence unchanged.

Concurrent uploads may interleave units and publish in either order. No
entry-sized offset array or commit-time payload copy is needed. Streaming
input and publication allow one additional target's worth of reserved bytes
before sealing. A producer can
still finish its exclusively owned range after a peer rotates; those late
writes are synced before publication from another inode, including in NoSync
mode. Completed immutable first-inode data is synced outside append authority
when already rotated. Publication checks again and retains a synchronous seal
if a rotation raced preparation. Same-inode durable publication shares the
ordinary commit sync.
Final compression and private-file seals run before acquiring publication
authority. A bounded final inline chunk acquires that authority only when its
encoded bytes are ready, then retains it through publication. An exactly full
codec frame waits for either the next input byte or commit, within the same
fixed chunk bound.
Rotation always seals the old segment before advancing the durable head,
including unpublished reservations: their framing must survive recovery even
when the producer aborts. Owners still sync any later private writes before
publishing a reference from another inode.
Units finalized before rotation are already covered by that seal; later writes
invalidate this proof and require their own seal before publication.
Range allocation owns rotation; the bounded index carry and publication that
follow it do not rotate again merely for metadata overhead.

Compaction copies encoded units unchanged, rebuilds their physical index and
preserves the stream publication. Its output rolls into a snapshot group at the
segment target. It does not concatenate a large entry or the collection into one
oversized snapshot file. The head names the whole group, and recovery and reader
protection cover every part. Deleting a derived projection rebuilds it from all
retained parts before replaying the newer append tail.
An indivisible codec unit may exceed the target by its bounded framing overhead;
that part contains only that unit. Very small targets also retain the minimum
unit budget. Neither exception grows with the total entry or collection size.

If no codec chunk was emitted before commit, its final bounded chunk writes
directly into a normal contiguous record under the existing publication lock.
This preserves the small-entry path without an entry-sized buffer or spool.
Parser/codec chunk capacities remain independent of the complete entry size.
Reads start with bounded 16 KiB heap scratch and grow at most once to 64 KiB
for larger entries. Cancellation remains cooperative between bounded chunks;
callback lengths may follow codec-frame boundaries.

For an unchanged single-unit entry without an entry deadline, the derived
ordered projection also records its unit location. Reads can validate the unit
metadata and begin consuming its encoded body together, using the existing
bounded output scratch for that first input chunk. Identity validation precedes
output; complete length and fingerprint validation precede accepting the entry
and its cursor. An LQL pass consumes that input once; a subsequent presentation
pass reads the canonical body again. No payload cache is retained. Other entries
follow their canonical publication and unit index normally. Entry metadata
updates clear the direct location; compaction can restore it after folding the
metadata. All these locations are rebuilt from canonical records with the rest
of the derived projection.
Contiguous records also read metadata and their bounded first body chunk
together into the existing consumer scratch. Historical metadata references
discard that prefix before following the visible version; LQL replay still
reads canonical bytes again after consuming the prefix.

An advisory `.streams.lock` pin protects unfinished canonical extents. Compaction
defers capture while a producer is live, then seals the capture boundary under
append authority. Producers admitted after capture can append to its retained
tail. Abandoned chunks are reclaimable; process exit releases the pin.

Both paths use the same incremental JSON validator and compression/encryption
encoder. Fixed chunk/parser scratch limits remain independent of item size.
Compression precedes encryption. Plaintext fingerprints cover compact JSON,
not compressed bytes or semantic key ordering.
The validator coalesces tiny parser emissions into at most 4096 bytes before
hashing/encoding, flushing the tail before each public write returns. This is
bounded chunk scratch, not a whole-value buffer or a body cache.
Push and staged pull reserve their 64 KiB codec chunk at admission (plus fixed
frame/tag storage for encryption), so payload progress and segment rollover do
not need further SDK allocations. Direct pull retains its small
plain-output optimization. Encryption and compression retain their 64 KiB
frame/workspace behavior; large writes keep the same write cadence.

Commit obtains normal collection authority, rechecks the incarnation and append
identity, assigns position/generation, finalizes metadata, and publishes through
the existing head/projection/commit-barrier protocol. Duplicate identity and
fingerprint return the original binding without renewing retention; conflicts
fail. A peer may publish the same ID while a writer is producing, so identity
is checked again at commit.

An optional [collection lease](collection-leases.md) is checked at admission
and final publication. The writer retains its original credentials and ownership
generation, including after public receiver close. Takeover, release or expiry
cannot let an obsolete writer publish or bind a new append ID. Each successful
append commits while that same lease remains held; it does not renew ownership.

Uncommitted extents are invisible to reads. Recovery skips complete extents
without a publication record and repairs incomplete active tails. A finalized
publication remains authoritative if projection updates or acknowledgement were
interrupted. Maintenance reclaims abandoned extents after their producer pin
has been released. Readers traverse the immutable index with bounded scratch and
verify the committed body; they never reconstruct a complete entry in memory.
On Linux, the reader disables lookahead on sparse stream-metadata descriptors.
Direct single-unit reads use sequential advice on their contiguous payload;
other payload descriptors retain the normal policy. Contiguous records restore
that policy when sharing the metadata descriptor.
Segment transitions sync the preceding canonical history. They do not separately
sync the rebuildable order and identity projections. A clean-close checkpoint
syncs both projections before publishing its marker; an unclean open repairs
them from canonical history, including missing or torn projection files.
Reserved addresses can extend a replay hint beyond an interrupted active file.
Recovery reconstructs canonical metadata and checks the existing published
positions, generations and counts before replacing projections or trimming
that unfinished tail. Truncated finalized records remain corruption errors.

The selected NoSync or durable-sync policy still applies. Durable commits pin
their exact canonical inode and use the existing root fsync batcher and commit
barrier. All forms retain a shared commit pin, allowing same-collection callers
to join a durable group. Writer admission does not drain that group. Canonical
rotation still applies the normal topology fences; ordinary pushes do not
rotate solely because they use incremental input.
A paused push/staged-pull producer holds no publication lock, allowing other
appends and retention/control maintenance to progress. Compaction capture waits
for uploads with unpublished canonical chunks to finish. Direct pull retains append authority while
reading its source; use staged pull or push for input that may wait externally.
If collection deletion wins before commit, the writer fails without
reviving that incarnation. Captured readers retain their existing protection.

## Atomic parent-retention renewal

Append requests additionally accept `has_collection_retention_seconds` and
`collection_retention_seconds`; Lua uses `collection_retention_seconds`.
These are separate from the entry's `retention_seconds`.

Omission preserves the current parent deadline. Selected zero clears it.
Positive seconds replace it with the exact append commit timestamp plus the
checked duration. Only a newly published append applies the directive. Aborted,
conflicting, identical duplicate and failed-before-publication appends do not
renew the parent. An error after publication can still have applied it; status
reconciles this uncertain outcome.

The directive and resolved absolute deadline belong to the canonical entry's
metadata. Deletion, metadata-only entry changes and compaction preserve this
small publication fact along with the existing retry binding. Recovery applies
surviving canonical facts without granting another lifetime. The derived order
projection tracks the latest renewal; it is not another transaction authority.

Explicit collection expiry updates record the current collection generation as
their override boundary. Earlier append directives cannot override that update
during replay; a later append directive can. Count-only metadata changes do not
renew expiry or turn unsynced append facts into durable authority. Losing an
unsynced append must also lose its renewal, even if newer projections survived.

Append results/status expose the original append's parent deadline when that
append selected a directive. This is a retry observation, not necessarily the
current parent deadline; `describe` reports the latter. Parent renewal and entry
expiry remain independent. Reads/listing do not renew either.

Generic liblockdc collections have no default expiry. A 90-day inactivity policy
is a downstream choice: configure creation and send 7,776,000 seconds on new
appends. Expiry remains lazy in Pouch: objects behave normally until maintenance
actually removes them. A downstream logical API may enforce its own visibility
policy, without changing native read semantics.

## Verification

Prove fragmented keys, escapes, UTF-8 and nesting; invalid/truncated/trailing
input; allocation-free payload progress for generated one-megabyte strings;
canonical payload writes before commit and bounded commit I/O;
and no canonical publication before commit. Cover codecs, abort/close/owner-close,
allocation/I/O failure, concurrent writers, duplicate/conflicting IDs,
captured readers, rotation, restart and compaction. Use controlled clocks for
renewal, overflow, clearing, duplicate/failed append, and janitor races.
Simulate loss of unsynced canonical
tails while preserving projections. Verify C89/C++ headers, ABI/export, Lua
parity and installed consumers. Keep performance measurements separate from
host-sensitive correctness assertions.
