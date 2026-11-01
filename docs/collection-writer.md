# Incremental collection appends

Implementation contract extending [ordered collections](collections.md).
The initial backend is Pouch, with matching C and Lua surfaces. Remote
collections remain unsupported. Collection receiver and capability additions
now use core ABI 9, advancing once from ABI 8 in v0.25.0 for atomic edits and
read preconditions. Rebuild C consumers and the optional PostgreSQL provider.
For an atomic cut/replacement/recovery marker, use an [edit](collection-edits.md);
ordinary append and staged/push writer behavior is unchanged.

## Scope and publication

A bound append writer validates, compacts, hashes and encodes pushed JSON
fragments once. Pouch stages at most 64 KiB of encoded payload on the heap,
then spills to an anonymous file. Commit copies those encoded bytes into the
shared active segment without reparsing or re-encoding. No producer thread or
body cache is introduced. Pull and push share the canonical record format,
codecs, projection publication and durable group-sync protocol.

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
- `abort(error)` discards unpublished storage. It is idempotent and cannot
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
staging. Signal cancellation through the source's error convention. A commit
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
publication lock while waiting for the producer. Small entries allocate no
staging file. Staging grows from 2 KiB up to a 64 KiB cap per live writer,
measured in encoded bytes. Larger payloads spill the prefix and subsequent
encoded bytes into a private mode-0600 file, immediately unlinked. The writer
owns its descriptor; close, abort and process exit reclaim it. Compression may
keep a large JSON body within the memory cap; encrypted roots never write a
plaintext spool. Parser and codec scratch are additional bounded allocations.

All append forms use shared rolling canonical segments. Direct pull validates
and encodes into an unpublished canonical range while retaining append
authority. Push and staged pull validate/encode before acquiring that authority;
commit copies the staged payload, checks its stored checksum, then finalizes
the canonical record. Failed copies roll back only the unpublished range while
append authority still excludes peers. There is no segment per small push commit.

Both paths use the same incremental JSON validator and compression/encryption
encoder. Fixed chunk/parser scratch limits remain independent of item size.
Compression precedes encryption. Plaintext fingerprints cover compact JSON,
not compressed bytes or semantic key ordering.
The validator coalesces tiny parser emissions into at most 4096 bytes before
hashing/encoding, flushing the tail before each public write returns. This is
bounded chunk scratch, not a whole-value buffer or a body cache.
Plain output uses a 2 KiB chunk inside the allocated writer and grows to the
existing 64 KiB chunk only when needed. Encryption and compression retain their
64 KiB frame/workspace behavior; large writes keep the same write cadence.

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

Uncommitted staging is invisible to reads and recovery. Recovery discards a
canonical PENDING tail left by interruption during the commit copy; a finalized
canonical record remains authoritative even when its projection publication
or acknowledgement was interrupted. Maintenance also reclaims any abandoned
named pending files left by an interrupted spool creation/unlink, while live
anonymous spools remain owned solely by their descriptors.
Segment transitions sync the preceding canonical history. They do not separately
sync the rebuildable order and identity projections. A clean-close checkpoint
syncs both projections before publishing its marker; an unclean open repairs
them from canonical history, including missing or torn projection files.

The selected NoSync or durable-sync policy still applies. Durable commits pin
their exact canonical inode and use the existing root fsync batcher and commit
barrier. All forms retain a shared commit pin, allowing same-collection callers
to join a durable group. Writer admission does not drain that group. Canonical
rotation still applies the normal topology fences; ordinary pushes do not
rotate solely because they use incremental input.
A paused push/staged-pull producer holds no publication lock, allowing other
appends and maintenance to progress. Direct pull retains append authority while
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
input; bounded memory for multi-megabyte strings; exact staging boundaries;
and no canonical publication before commit. Cover codecs, abort/close/owner-close,
allocation/I/O failure, concurrent writers, duplicate/conflicting IDs,
captured readers, rotation, restart and compaction. Use controlled clocks for
renewal, overflow, clearing, duplicate/failed append, and janitor races.
Simulate loss of unsynced canonical
tails while preserving projections. Verify C89/C++ headers, ABI/export, Lua
parity and installed consumers. Keep performance measurements separate from
host-sensitive correctness assertions.
