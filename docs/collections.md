# Ordered collections

Contract for the complete collection surface.
The implementation belongs to liblockdc/Pouch with C and Lua parity.
The HTTP interface below is the corresponding future lockd server contract;
this work does not implement or modify lockd.

Existing storage/durability policy is documented in [Pouch storage](pouch-storage.md)
and language ownership conventions in [the Lua SDK guide](lua.md).

## Purpose and first scope

A collection is an ordered owner of state objects with immutable JSON bodies
within a namespace.
It supports journals and event stores with fast append and ordered streaming
reads. Position and append identity are collection metadata, outside the
application's JSON object. Corrections append new entries; individual entries
may be deleted, but their positions never change.

The public surface provides collection creation/open/list/description/drop,
direct and staged pull append, push-fed append writers, append-status lookup,
individual deletion, and ordered streaming with an optional LQL predicate and
opaque resume cursors. Appends are independently atomic; they do not enlist in
implicit or managed XA transactions. Collections
and their entries both support optional retention metadata, including later
metadata-only changes.

[Collection leases](collection-leases.md) optionally grant exclusive ownership
of the entire collection while entries continue to commit individually.
Unleased collections retain multiwriter behavior. Public committed reads remain
available by default; private acquisition hides payload reads until release or
expiry. C and Lua use the same collection receiver methods in either mode.

Collections also support an optional `max_entries` live-entry limit. Maintenance
removes oldest positions until the newest N live states remain; append remains
independently atomic and never synchronously trims the collection.

Position ranges and first-N input windows are implemented on that same read
operation, before LQL filtering. Last-N selection, explicit position sets,
payload projection, body mutation,
subscriptions, consumer groups,
and CAI integration remain outside the current implementation. Do not
expose inactive selection options or silently accept them. Ordered seek and
reverse traversal are required storage primitives now, so last-N
can later extend the same read operation without changing stored entries.

## Raw state bytes and separate metadata

The payload output is an ordered stream of stored compact JSON objects, each
followed by LF. For example, the application's sink receives only:

```json
{"role":"user","text":"hello"}
{"role":"assistant","text":"hi"}
```

Position, append ID, ETag, byte count, and cursor belong to separate metadata.
They never become members of an application object or a JSON envelope around
it. The native API pairs an ordinary byte sink with optional metadata callbacks;
the future HTTP transport carries metadata in multipart headers. Both present
the same raw JSONL and metadata contract to C and Lua callers.

For each match, the reader locates its stored payload span, copies its bytes
from offset zero to the end through the existing crypto/decompression/integrity
path, writes LF, and reports completed-entry metadata. It then advances the
ordered iterator. An unfiltered read does not parse or reconstruct JSON for
presentation. An exact indexed predicate can use that same payload path;
residual LQL evaluation is the explicitly additional work described below.

Reuse bounded per-read metadata/chunk buffers. Metadata delivery must not
introduce per-entry clients, threads, network requests, whole-object buffers,
or mandatory per-entry heap allocation on the native path. This is direct
stored-byte delivery, not a claim of kernel zero-copy with crypto enabled.

## Identity, ownership, and order

- `(namespace, name)` locates a collection. Creation assigns an opaque,
  immutable collection ID. Opening has no create side effect. Concurrent
  creation of the same name has exactly one winner.
- A collection handle binds its ID, not only its name. Dropping/recreating a
  name gives the new collection a different ID; old handles and cursors cannot
  address the replacement. Collection names are nonempty valid UTF-8, at most
  255 bytes; namespace validation follows the existing SDK contract.
- An entry is a collection-owned state object using Pouch's canonical state
  record representation and existing payload/crypto/compression machinery.
  Ownership does not make a second application document type or inject a
  wrapper, ID, or position into its JSON body.
- Ordinary state keys, listings, queries, leases, metadata updates, and
  attachment methods cannot address or mutate collection entries. Collection
  reads expose ordinary state payload/ETag facts through collection methods.
  Entry retention changes use the same state metadata behavior as ordinary
  objects. There is no entry body update, body mutation, or attachment API in v1.
- Positions are unsigned 64-bit values. Zero is the boundary before the first
  entry. Each published append receives a unique, increasing positive position
  within its collection ID. Positions are not timestamps, array indexes, file
  offsets, or Lucene-style transient document numbers.
- Gaps are valid. Deletes, failed reservations, and internal mutations may
  leave gaps. Allocation must not wrap; exhaustion rejects further appends.
  There is no ordering guarantee between different collections, and concurrent
  callers are ordered by publication rather than call-start time.

For example, deleting position 42 from `41, 42, 43` leaves `41, 43`; the next
append has a position greater than 43. Deleting every entry does not reset the
high-water mark. Compaction, clean restart, and recovery preserve positions and
that high-water mark. Power-loss durability follows the selected Pouch storage
policy, including the existing distinction between NoSync and strict sync.

## Pouch storage boundary

Store collection data in an isolated area beneath its namespace, addressed by
collection ID. Ordinary namespace replay and document scans must not enumerate
collection entries. Collection enumeration reads the name/ID catalog, not all
entry bodies. Name-to-ID catalog publication and collection drop are durable
metadata operations under namespace catalog authority.

Namespace deletion includes logical removal of its collections and respects
their active-reader protection before physical reclamation. Recreating a
namespace cannot revive old collection IDs. Whole-store backup/recovery must
include collection partitions; ordinary document JSONL export remains separate
from explicit collection export through `read`. Existing namespace
history-consumer registrations do not become collection subscriptions.

Use one shared Pouch root, allocator, writer policy, descriptor infrastructure,
and maintenance scheduler. Do not instantiate a client/Pouch root, worker,
timer, or writer-presence heartbeat for each collection. Both exclusive-root
and shared-writer modes must preserve the same public contract.

Keep root, catalog, session, append, commit, and metadata authority in that order,
including handle creation, deferred close, and maintenance. Nested callback
operations borrow an already held root operation rather than reacquiring its
reader gate behind a waiting writer.

The necessary storage extensions are collection-aware addressing, an ordered
position projection, and read-generation protection. Reuse the existing record
encoder, pending-record publication, recovery, crypto, compression, group-sync,
and compaction primitives. A private append-new operation may bypass generic
current-state lookup when collection authority has proved identity uniqueness.
Do not route healthy collection reads through a namespace-wide metadata replay
and collect/sort path merely to reuse a public query wrapper.

The canonical state records, collection identity/catalog facts, and surviving
sequence high-water facts are the durable authority. The ordered and LQL
indexes are derived projections. A canonical deletion must preserve the small
binding of append identity, fingerprint, position, and original byte count
needed for retry reconciliation; it must not survive only in a discardable
query index. Reuse or narrowly extend state tombstone metadata rather than
creating another transaction outcome log.

## Append and retry

An append accepts exactly one JSON object through a direct/staged `lc_source`
or an owned push-fed writer. [Incremental appends](collection-writer.md) describe
the writer lifecycle, bounded canonical chunks, and atomic parent renewal.
Arrays, scalars, empty input, trailing non-whitespace, malformed JSON, source failure, and limit
violations fail before publication. Validation and compaction of structural
whitespace are incremental. Preserve JSON values and member order; this is not
semantic canonicalization or object-key sorting.

The initial validator supports nesting through 128 levels, decoded object keys
through 64 KiB, and number tokens through 4096 bytes. String values and total
document size are not constrained by a whole-value buffer; normal storage and
checked byte-counter limits still apply. Input chunks are bounded, and parser
scratch has a fixed heap ceiling. Test keys larger than an input chunk and
string values much larger than the parser's scratch ceiling.

Store compact JSON once so reads can stream the stored body and a newline
without reparsing or materializing the document for presentation. Each read
operation reuses bounded heap storage for decoding and hashing across entries;
it borrows the pinned segment file descriptor instead of reopening it for each
body. Every entry verifies its identity, decoded length, and committed
fingerprint. Encrypted entries also verify descriptor bindings and frame
authentication. Decoder buffers hold bounded chunks and are released when the
read ends.

The decoder can read several authentication frames into its bounded input
buffer together, while read-ahead stays within the current entry's encoded
span. Key derivation and binding contexts belong to one read operation; every
entry still derives its own keys and starts independent authentication.

Use lonejson's public incremental parser/writer facilities. Its push-fed value
stream is a sink interface. Push writers validate and encode incrementally into
bounded canonical extents in shared rolling segments. Commit publishes metadata
without rereading or copying the payload. No whole-entry staging is used.
Readers follow the stored extents and stream raw JSON without reconstruction.

Each request requires a caller-supplied `append_id`: 1 to 128 ASCII characters
from letters, digits, `-`, `_`, `.`, and `~`. It is scoped to the collection ID.
Callers can generate one with the existing XID helper and retain it before
starting a single-pass upload. Initializers do not generate IDs implicitly.

While streaming, compute the stored-body ETag/fingerprint through the existing
hash path. The binding is to the compact stored JSON bytes, not semantic JSON
equivalence. The same append ID with the same fingerprint returns the original
position and `duplicate=true`; different content returns `append_id_conflict`.
Duplicate verification may consume the caller's source to compute its hash,
without writing another entry. An append-status lookup requires no body.

Publication has one per-collection linearization point. Readers see both a
complete entry and its ordered membership, or neither. Publish through the
existing pending-record protocol and update projections under the same
authority; do not independently commit an entry and a list of references.
Successful acknowledgement meets the configured durability policy. An error
after canonical publication may have an uncertain outcome and must not be
reported as proof that nothing was appended.

With `durable_sync=1`, a pull append pins its exact canonical file and holds a
shared commit barrier while waiting for the existing root fsync batcher. It
releases append authority before that wait,
so independent callers can append and join the same durable group. Handles in
one process share the root batcher; separate processes retain separate batchers
but can progress concurrently while synchronization is pending.

Public snapshots, descriptions and status observations wait for the commit
barrier before capturing metadata. Other mutations and compaction also drain
pending appends before changing their records or canonical topology. Thus
readers cannot bypass a pending durability wait, and rotation cannot redirect
a waiting caller's sync to a different inode. A failed sync returns an error
to its waiters; complete canonical records may still be observed afterward,
have the uncertain outcome described above, and retain their retry identity.

Source callbacks run before acquiring the shared commit pin, allowing a peer
receiver to read the preceding committed view during upload. Snapshot capture
releases its exclusive pin before delivery callbacks. The commit barrier uses
an advisory `.commit.lock` file with an independent descriptor per call; it
stores no state or transaction outcomes and introduces no worker or body
buffer. Push commit publishes its canonical extent index and uses the same
shared commit pin; no publication lock is held between push calls. A separate
`.streams.lock` pin defers compaction capture until active uploads finish.
NoSync appends bypass the synchronization wait.

Keep the canonical binding after individual deletion until collection drop.
Status lookup reports `absent`, `committed`, or `deleted`, with the original
position/fingerprint when known. An absent lookup observes one instant; it does
not cancel an earlier request still in transit. Concurrent same-ID requests
must converge to one binding. Retrying a deleted binding never recreates it.
The storage cost of retaining small retry tombstones is deliberate; deleting
the JSON body does not promise erasure of identity/hash metadata.

Never automatically replay a non-rewindable source, including during endpoint
failover. After an ambiguous outcome, callers use append-status and, when
necessary, explicitly supply a fresh source with the same append ID. A known
unsupported backend must fail before consuming that source.

## Read selection and LQL

The selection request accepts optional `selector_lql`, opaque `cursor`,
`after`, `through`, and `first_n`. Delivery additionally accepts a sink, an
optional metadata callback, and local cooperative cancellation. Empty/absent
LQL selects every live entry. Scalar/callback options are captured at entry;
borrowed strings and contexts must remain valid through the call.
`after` is an exclusive permanent position (default zero); `through` is an
inclusive permanent position (default the captured committed end). Explicit
`after > through` is invalid; equal bounds and a start beyond the current end
select nothing. The reported `upper_position` is the captured end capped by
`through`, so it can be below the starting boundary for an empty future range.
Without a cursor, a read starts after its requested lower bound.

`first_n` selects at most N live input entries after applying the position
bounds, before applying LQL, in increasing position order. Filtered-out inputs
consume N; deleted entries do not. A window may emit fewer than N states,
including none, and never searches later entries to fill its output. Omission
is unlimited; explicit zero selects an empty window. Reaching N completes the
selected window without examining or advancing over the remaining entries.
Its checkpoint continues after the last safely examined selected input,
including filtered-out inputs, with a fresh N-input budget and captured end.
An empty zero-count window's checkpoint does not advance. Exhausting the range
before N live inputs advances over the safely examined nonmatches/deletion gaps.

Cancellation and errors retain the original captured upper boundary, predicate
instant, and remaining input count in the resume cursor. Repeat all selection
options when resuming or using a checkpoint; changing bounds, count, or predicate
is a cursor mismatch. A cursor captured after the Nth accepted input can finish
its window without emitting another entry. Result counts remain per invocation.
`complete` describes selection completion; with first-N, it need not mean the
collection's captured end was reached. Offset/skip, reverse output, last-N and
explicit position lists remain unavailable.

At invocation start, capture a committed upper sequence boundary and a
consistent visibility/index view. An upper boundary may itself be a gap.
Appends after this boundary do not extend the invocation. Existing deletions
are excluded. Entries deleted after this read captures its view remain
available to that invocation until it ends or is cancelled.

Capture a mutation generation as well as the upper position: appends and
deletions advance the generation, but deletion does not allocate an entry
position. A deletion projection records its generation. A read excludes that
entry only when the deletion belongs to its captured generation or earlier.
An upper position alone is insufficient to provide this visibility rule.

Emit matches in increasing permanent position, regardless of LQL candidate
order. Filtering never renumbers entries. Unsupported syntax/plans fail
explicitly; never silently weaken the predicate or use a materializing/spooled
compatibility path to claim streaming support. Preflight selector syntax and
streaming-plan support before starting delivery.

Inherit the existing Pouch `query_indexing` policy; v1 adds no per-field or
per-collection indexing configuration. The ordered position projection is
always required, including when LQL indexing is disabled. Reuse the existing
LQL language, selector inspection/planning, field extraction, and supported
index representations within the collection's isolated scope.
Use normal-state LQL predicate semantics; projection and mutation requests are
not filters and are rejected. Preserve the existing at-rest/index protection
policy without duplicating complete plaintext bodies into auxiliary indexes.

Use indexed candidates when supported. Residual predicates and scan-only
collections use bounded streaming evaluation over stored entries. A predicate
that depends on the document's end can require one evaluation pass and another
stored-body pass for a match. This is additional storage I/O, not whole-record
buffering or an additional client request. Reuse authenticated/decompressed
stream primitives and validate the entire referenced record before accepting
an evaluation or completed output record.

Index lag cannot hide committed entries. A read combines its persisted index
generation with committed unindexed changes through its captured boundary,
including deletion overlays affecting earlier positions. Alternatively, an
already-current compatible generation can supply the whole view. Do not force
a full collection rebuild/flush for every read. Index and scan execution must
produce identical positions, bodies, and cursor progress.

Bind persisted query pages to a recovery identity as well as the collection
and their ordered metadata. After NoSync data loss, a later append can reuse a
lost position, publication generation, and byte count for a different body.
Canonical recovery therefore starts a fresh query identity; metadata equality
alone cannot validate an index from the lost history. Compaction preserves
this identity because it relocates the same committed entries. Retire obsolete
query generations and abandoned publication files through maintenance.
A build finishing after canonical recovery must not recreate a retired query
identity. Revalidate its identity and physical page binding before publication;
a stale partial page must not replace a newer complete page. Cleanup preserves
active current-generation publishers while reclaiming abandoned temporaries.

Bound extracted index metadata independently of document and collection size.
An entry exceeding that budget remains uncovered and is evaluated from state;
partial extraction cannot establish a nonmatch. Numeric normalization,
long-string hashes, and recursive-presence candidates require residual
verification wherever their index representation cannot prove the predicate.
Discard provisional terms for budget-exceeded or unrepresentable-number states
after validating their complete JSON, so they neither retain partial values nor
consume the rest of a page's indexing budget. Maintenance removes deleted slots
from bounded index metadata without rereading surviving bodies. Those slots
become uncovered so an active earlier visibility view still evaluates its
original states correctly.

Capture LQL's relative-time evaluation instant once per logical read.
Continuation retains that instant so reconnecting does not change the predicate
halfway through its bounded read. A new logical read captures a new instant.
An incremental checkpoint never revisits earlier examined entries, including
ones that a later evaluation of a time-relative selector could match.

## Streaming delivery and cursor lifetime

`read` writes raw compact JSONL directly to the caller's existing `lc_sink`.
Sink chunks may split anywhere within an object; they are not record boundaries.
Emit LF only after complete payload validation/authentication. A failing last
record may leave partial JSON bytes, but no completed JSONL record. No read
returns a fully buffered document under a streaming name, and no second read
method is required to remove metadata from the payload.

An optional synchronous `on_metadata` callback receives one of two borrowed
metadata views, outside the byte sink:

| Kind | Facts and delivery point |
| --- | --- |
| `entry` | Position, append ID, ETag, JSON byte count excluding LF, and resume cursor; after the complete object and LF have been accepted by the sink |
| `progress` | Last safely examined position and resume cursor; after proven nonmatches/deletion gaps, without writing payload bytes |

The callback returns `LC_OK` or a normal SDK error. When present, it must also
succeed before the proposed cursor becomes the SDK's accepted progress. Callback
failure stops the read and preserves the previously accepted cursor, even if
that entry's bytes have reached the sink. Without a callback, successful sink
delivery and validation accept the entry. Metadata is borrowed only for the
callback; a caller retaining a cursor must copy it. Do not expose provisional
remote part headers as a completed-entry callback before its body is complete.

Offer progress metadata at bounded traversal intervals, with at most 256
examined candidates between progress opportunities, and after a final skipped
suffix. This also supports reads with no matching entries. Progress never
skips an undelivered match. A callback may synchronously accept and persist a
completed entry's cursor; interruption does not force whole-read replay.

The SDK does not roll back a sink. After a partial object or a metadata-callback
failure, a caller resuming into the same file must first remove any unaccepted
suffix, or use another sink. There is no automatic byte replay into a sink that
already contains partial output. Completed-entry cursor metadata avoids
replaying earlier accepted objects; it does not make external side effects
and checkpoint persistence atomic.

Check cancellation before starting another object and during bounded chunk
delivery. Cancellation observed before acceptance of an object leaves its
cursor unaccepted, including when all its JSON bytes but not its LF have been
written. A callback cannot re-enter the same collection receiver; reject such
calls instead of waiting on its active operation. Calls through another handle
remain possible. An active call retains its client and collection owners;
close requests release ownership without waiting on their own callbacks.

An append source callback runs while that collection's append authority is
held. It may read the prior committed view, but attempting another append,
deletion, or drop of the same collection from that callback must fail explicitly,
including through another handle. Otherwise a synchronous callback could wait
on the append operation that is waiting for it. Calls on other collections and
deferred owner close remain supported.

One invocation traverses its selection with bounded chunk/page memory, bounded
open descriptors, and no per-entry thread or transport request. Reuse segment
read resources across adjacent entries. Payload size, total collection size,
and the number of matches cannot require buffering the full result or opening
all collection segments at startup. Read sink, metadata, and cancellation
callbacks execute outside mutation locks; their backpressure reaches
storage/transport directly.

Pin the read's index/data generation, using transient cross-process reader
protection where necessary. Appenders, logical deletes, and snapshot
publication can continue; reclamation of files needed by an active read waits.
Process exit releases transient pins. A slow reader may delay disk-space
reclamation, not collection append. The obsolete-file grace period alone is
insufficient protection. Do not create a durable history-consumer registration
for every read or retain an old generation indefinitely after disconnect.
Pins identify a canonical snapshot generation. Readers of a later generation
must not keep obsolete files from an earlier generation alive.

A cursor is a nonempty, unpadded base64url ASCII token of at most 4096 bytes,
bound to the collection ID and exact selection identity; absent and empty
selectors are the same identity. The caller repeats its selection options when
resuming. A mismatch, malformed token, stale collection ID, or unavailable
required history fails explicitly. Tokens are opaque to applications and
strictly validated; decoded fields never become unchecked filesystem paths.
Tokens survive clean reopen/restart and are portable between server endpoints
that serve the same collection identity and retained coverage. They contain no
required process-local pointer, file descriptor, or permanent reader ticket.

Distinguish two purposes:

| Token | Meaning when supplied as `cursor` |
| --- | --- |
| Resume cursor | Continue after the last safe examined position, preserving the original upper boundary, remaining first-N budget and predicate evaluation instant |
| Checkpoint cursor | Start a new selected window after the completed prior window, capturing a fresh upper boundary, count budget and evaluation instant within the repeated explicit bounds |

A read result contains `upper_position`, completed-record count,
`last_examined_position`, `complete`, `stop_reason`, an owned `resume_cursor`
when incomplete, and an owned `checkpoint_cursor` when complete. The checkpoint
is useful for later incremental journal reads; it captures a fresh end while
continuing to respect any explicit `through` bound supplied by the caller.

The safe examined boundary includes proven nonmatches and known deletion gaps,
not just emitted entries. A fully filtered read can complete and advance its
checkpoint without emitting an object. Errors and cancellation preserve the
last accepted metadata boundary even when no entry matched. A partially
delivered entry never advances it. Establish an initial resume cursor when the
view is captured, before the first match. Populate partial results before
returning an error; their normal cleanup functions remain required and safe.
Result counts and `last_examined_position` describe accepted progress, not
provisional bytes or unaccepted metadata.

Callbacks report errors through the normal SDK status/error contract. Local
cooperative cancellation returns success with `complete=false`,
`stop_reason=cancelled`, and safe resume progress. Transport loss without
caller-requested cancellation is an error. Cancellation releases read pins and
does not undo previously completed records.

Across separate invocations a continuation retains its bounds, not an
indefinitely pinned historical snapshot. Its new visibility view may exclude
entries explicitly deleted or removed by retention maintenance since the prior
call. Individual deletion does not invalidate otherwise valid cursors.
Dropping/recreating the collection, including expiry of the collection itself,
does. Future bulk history trimming must maintain a coverage floor and reject older
cursors as expired instead of silently claiming complete retained history.

## Retention metadata

Use the same optional retention metadata and commit semantics as
[ordinary state objects](state-retention.md). Collection ownership changes
addressing, not the representation or meaning of an entry's state retention.
The JSON body, fingerprint, append identity, and position remain unchanged by
a metadata update. Reuse the canonical state metadata record and payload-span
reference; changing retention must not read, copy, parse, or rewrite the body.

There are two independent targets:

- Collection retention is the collection's own expiry. When maintenance
  removes it, the collection and all remaining entries become inaccessible,
  through the same catalog/drop authority as an explicit drop. It is not a
  maximum age assigned to each entry. An append preserves it unless that new
  append explicitly selects atomic parent-retention renewal.
- Entry retention is that state object's own expiry. Maintenance deletes the
  entry through collection deletion authority, preserving its position gap
  and append retry binding. It does not remove or renew the collection.

Both default to no expiry. An empty collection remains until explicitly
dropped or removed because its own retention has expired. Changing collection
retention applies to the collection with all its existing and future entries;
it does not visit or rewrite their metadata. Clearing an entry's expiry does
not exempt it from deletion with its parent collection.

Creation and append accept optional `has_retention_seconds` and
`retention_seconds`. The corresponding collection and entry `metadata`
operations use the same fields. Omission preserves an existing deadline and
defaults a new object to no expiry. Explicit zero removes the selected
object's expiry. A positive duration replaces its deadline with commit time
plus that duration, using checked arithmetic. Reject negative selected
durations and overflow before publication. Ignore the duration when its
presence flag is zero. An omitted metadata change is an observation/no-op,
not a renewal. Opening an existing collection never changes retention;
creation-only retention options on open are rejected.

All append forms additionally accept `has_collection_retention_seconds` and
`collection_retention_seconds` for the parent. Omission preserves its deadline;
zero clears it; positive seconds reset it from the same publication timestamp
as the entry. Only a newly committed append applies this directive. Duplicate,
conflicting, aborted and failed-before-publication attempts never renew it.
The directive is recorded with the canonical retry binding, including after
entry deletion and compaction. Results/status expose
`has_collection_retention_expires_at_unix` and
`collection_retention_expires_at_unix` when the original append selected it.
This original observation may differ from the parent's current deadline;
use `describe` for the current value. Generic collections still default to no
expiry; an inactivity policy is explicitly supplied by the application.

Choose a new append deadline at publication after consuming and validating its
source, not at upload start. A duplicate append returns the original binding
and currently known retention without changing any deadline, even when the
retry supplies a different valid duration. After retention deletion, retrying
the same append ID still reports a deleted duplicate and cannot resurrect it.
Metadata changes target a live entry; a never-assigned or already deleted
position reports `entry_not_found`. Concurrent metadata updates and deletion
serialize through the same collection authority. A metadata operation cannot
restore a deleted entry or a dropped collection. Recovery and compaction
preserve absolute deadlines; neither grants a fresh retention period.

Observation uses the ordinary state fields `retention_support`,
`has_retention_expires_at_unix`, and `retention_expires_at_unix`, including the
distinction between unknown and confirmed zero. Collection descriptions and
collection metadata results describe the parent's deadline. Append outcomes,
append status, entry metadata results, and read entry metadata describe the
entry's deadline. A deleted binding has no current state deadline. Progress
frames carry no entry retention facts. Pouch reports
support and a known deadline, including zero. Remote capability observations
remain endpoint-local and follow the ordinary state retention protocol; do not
silently emulate server retention with client timers.

Entry metadata updates advance the collection mutation generation without
allocating a position or changing its payload ETag. Read entry metadata belongs to the
invocation's captured visibility generation, just like deletion visibility.
A later retention update or janitor deletion cannot change a live reader's
captured object or metadata. A separate resumed invocation may observe newer
retention metadata or deletion. Existing read pins protect any canonical
metadata and payload spans required by that captured view.

Expiry makes an object eligible for maintenance; it is not a read-time filter
or a promise of deletion at the deadline. Until maintenance actually removes
it, an expired collection or entry behaves normally and its retention can be
changed or cleared. Use the existing root janitor/compaction cadence. Do not
add foreground scans, a per-collection worker, or a timer for each deadline.
Check deadlines using bounded metadata traversal. A conservative derived
presence hint skips entry traversal when no entry has retention; canonical
rebuild and compaction recompute it. Only actual deletion creates
a tombstone or drop; physical reclamation continues to respect reader pins,
grace periods, and the configured durability policy. Entry expiry leaves the
cursor coverage floor and high-water mark unchanged.

### Count retention

Collection creation and collection `metadata` accept `has_max_entries` and
uint64 `max_entries`. Omission creates an unlimited collection or preserves an
existing setting. Explicit zero disables count retention; positive N keeps the
newest N live entries by permanent position. The setting applies to existing
and future entries. Opening cannot set it, even to zero. Entry metadata rejects
this collection-only setting. Descriptions and list callbacks report the
configured `max_entries`; zero means unlimited. The `count_retention` capability
is true for Pouch and false for currently unsupported remote collections.

Count retention is eventual, like age retention. Appends and reads can observe
more than N entries before maintenance catches up. The root's existing worker
cadence drives cleanup; disabling background compaction also disables automatic
count cleanup. Explicit non-cleanup-only maintenance can drive it immediately.
Each collection visit deletes at most the surplus captured at its start,
releasing mutation authority between individual deletions. New appends cannot
keep a pass running indefinitely, while an existing backlog can be removed
without waiting for many maintenance intervals. Cleanup-only passes perform no
logical deletion.
Maintenance rechecks the current limit and live count under mutation authority
before selecting each oldest entry. A raised/disabled limit or concurrent
explicit deletion cannot make a previously captured surplus over-delete.

An entry's age expiry is independent of the count cap. Maintenance processes
due entry expirations before count trimming, avoiding unnecessary trimming when
expiry already satisfies the cap. Parent collection expiry takes precedence
and drops the entire collection. LQL, read ranges and first-N do not affect
retention selection. Deleted entries do not count; positions are never reused
or renumbered. Increasing/disabling a limit never restores deleted entries.

Reuse canonical entry deletion, ordered visibility, retry tombstones, compaction
and reader protection. The live count determines whether trimming is needed;
an ordinal hint in the derived ordered index avoids repeatedly traversing an
already trimmed prefix. It is reconstructed from canonical records after an
unclean restart and is not a durable deletion watermark. No body parsing,
collection-wide sorting, new timer or append-time trim is needed.
Count trimming increments `retention_deleted_state_count`; only age expiration
increments `retention_expired_count`. The cap bounds live states, not physical
disk use or retained append-identity tombstones. Reclamation still waits for
compaction and active readers; retry bindings persist until collection drop.

Lua uses canonical uint64 decimal strings for `max_entries`, including `"0"`
and `"1000"`, with the same omission semantics. Future lockd creation and
collection metadata requests use `X-Collection-Max-Entries`, also a canonical
uint64 decimal string. Descriptions report `max_entries` as a decimal string;
entry metadata and append reject that header. There is no client-side remote
retention emulation.

## C and Lua client surface

The public-only [C journal example](../examples/collection_journal.c) creates
a temporary named collection, uses pull and push appends with atomic renewal,
retries entries, changes retention
metadata, streams raw bodies with separate boundary callbacks, and verifies
deletion before dropping the collection. Build `lc_example_collection_journal`
and pass a `pouch:///absolute/path` endpoint. SDK tests compile and execute the
same example with both extracted static and shared libraries and allocation
tracking enabled.

Use a bound `lc_collection` receiver with matching free functions, following
the existing public SDK style. Declare public structs/types in `lc/lc.h`;
all owning results have initializers/cleanup and all input options have
zero/default initializers. Keep physical storage and query-plan types private.

| Client/receiver operation | Contract |
| --- | --- |
| `client.create_collection` / `lc_client_create_collection` | Create and return a bound handle; existing name is a conflict |
| `client.open_collection` / `lc_client_open_collection` | Open an existing name and bind its collection ID |
| `client.list_collections` / `lc_client_list_collections` | Visit collection descriptions in namespace/name order without entry bodies |
| `client.collection_capabilities` / `lc_client_collection_capabilities` | Report endpoint-local collection/filter/index/resume support and later selection capabilities |
| `collection.head` / `lc_collection_head` | Observe identity, opaque head token, counts and bounded recovery metadata coherently |
| `collection.edit` / `lc_collection_edit` | Atomically remove position ranges and optionally append a streamed replacement and update recovery metadata |
| `collection.edit_status` / `lc_collection_edit_status` | Observe a persistent original edit outcome; absence does not cancel an in-flight operation |
| `collection.describe` / `lc_collection_describe` | Return ID, name, namespace, current upper boundary, live count, indexing availability, and collection retention |
| `collection.metadata` / `lc_collection_metadata` | Change collection expiry and/or max_entries and return its description |
| `collection.append` / `lc_collection_append` | Consume `lc_source`, with required append ID and optional entry/parent retention; return position, ETag, byte count, duplicate/deleted and retention facts |
| `collection.append_begin` / `lc_collection_append_begin` | Return an owned writer accepting bounded raw JSON fragments through `write`, then `commit` or `abort`; always `close` it |
| `collection.append_staged` / `lc_collection_append_staged` | Consume a borrowed single-pass source through the same writer; source reads hold no publication locks |
| `collection.append_status` / `lc_collection_append_status` | Resolve an append ID without supplying its body |
| `collection.entry_metadata` / `lc_collection_entry_metadata` | Change retention metadata for a live position, preserving its body and identity; return entry facts |
| `collection.read` / `lc_collection_read` | Write raw JSONL to `lc_sink`, with optional separate metadata callback, LQL, cursor, and cancellation |
| `collection.delete_entry` / `lc_collection_delete_entry` | Delete one position; repeat deletion of a known tombstone succeeds |
| `collection.drop` / `lc_collection_drop` | Logically remove the bound collection, invalidate future calls/cursors, and schedule physical reclamation |
| `collection.close` / `lc_collection_close` | Release the handle without deleting the collection |

Every receiver spelling above has the corresponding matching free function;
the client methods take namespace/name configuration and bound methods use the
handle's collection identity. Source/sink arguments remain the existing
`lc_source` and `lc_sink` types, not a second collection-specific byte API.
Use `delete_entry` in both languages: `delete` is a C++ keyword and cannot be
a public C receiver field while preserving the SDK's C++ header contract.
Use `lc_collection_read_metadata` for borrowed delivery frames, leaving
`lc_collection_metadata` available for the receiver's metadata operation.
Retention requests and results follow the same presence/deadline conventions
as ordinary states. Lua uses the ordinary retention option and observation
spellings, including metadata-only changes on both collection and entry.

A delete of a never-assigned position reports `entry_not_found`; a missing body
behind an allegedly live index reference is corruption, not a normal deletion.
Open bound handles permit concurrent operations through shared collection
authority. Apply the SDK's ownership discipline: active calls pin required
owners, callback errors/cancellation unwind cleanly, and closing an owner must
not cause use-after-free or deadlock waiting for its own callback.

Lua exposes the same client/collection methods and lifecycle. Represent all
unsigned 64-bit positions, boundaries, byte counts, and record counts as
canonical decimal strings, including small values; do not round through
floating point or vary the type by magnitude. Lua uses the existing streaming
sink adapters and an optional `on_metadata` function with the same entry/progress
facts. It receives small metadata tables, not decoded or reconstructed state
objects. No unrequested buffered convenience method is part of v1. Validation
returns `nil, error, code` rather than forcing longjmp across owned native
resources.
Streaming reads return `nil, result` on success and `nil, error, code, result`
on failure so partial progress remains available, matching the facade's
streaming and error conventions. Metadata callback success is `true` or no
return; `false, message`, `nil, message`, or a raised error fails the read.

Illustrative Lua flow, with a caller-provided streaming source and sink:

```lua
local collection = assert(client:create_collection({
  namespace = "sessions", name = "conversation-123",
}))
local appended = assert(collection:append({ append_id = event_id }, source))
local streamed, result, code, partial = collection:read({
  selector_lql = "eq{field=/role,value=assistant}",
  on_metadata = function(meta)
    -- The corresponding raw JSON has already reached sink for an entry.
    return accept_metadata(meta) -- true only after accepting this boundary
  end,
}, sink)
assert(code == nil, result)
assert(streamed == nil)
assert(result.complete)
save_checkpoint(result.checkpoint_cursor)
collection:close()
```

This example does not imply an atomic transaction with `save_checkpoint`.
An application stores a checkpoint only after accepting and durably handling
the referenced completed records. Replaying after a lost application checkpoint
can repeat delivery; collection append idempotency is not exactly-once consumer
side-effect processing.

## Future lockd server API

Use the same identities, ordering, filtering, cursor, error, and durability
semantics. Existing namespace authorization, authentication, request deadlines,
and endpoint selection apply. Path components are independently percent-encoded;
positions use canonical unsigned decimal text. IDs and cursors are bounded,
validated data, never storage paths. All sequence values in JSON are decimal
strings. Return collection ID on create/open and require it on bound operations
to prevent a stale handle from touching a recreated name.

Let `B` denote `/v1/namespaces/{namespace}/collections` and `C` denote
`B/{name}`. These are new operations inside the existing versioned API, not a
new version inferred from the liblockdc release number.

| Method/path | Request and successful response |
| --- | --- |
| `GET /v1/collections/capabilities` | JSON collection/filter/resume support; indexing availability is additionally reported by collection description |
| `POST B` | JSON `{"name":"..."}`; 201 description including new collection ID |
| `GET B` | Stream bounded-size collection descriptions as NDJSON; require successful HTTP message completion, without entry bodies or control records |
| `GET C` | 200 description; with `X-Collection-ID` validates a bound handle |
| `PATCH C/metadata` | Required `X-Collection-ID`; empty body and optional retention and/or max-entries headers; 200 updated description |
| `DELETE C` | Required `X-Collection-ID`; 204 logical drop |
| `POST C/entries` | Required `X-Collection-ID` and `Idempotency-Key`; `application/json` streaming object; 201 append result, or 200 for an identical retry |
| `GET C/appends/{append_id}` | Required `X-Collection-ID`; 200 append status, including `state=absent` when no binding is committed |
| `PATCH C/entries/{position}/metadata` | Required `X-Collection-ID`; empty body and optional retention header; 200 entry metadata |
| `DELETE C/entries/{position}` | Required `X-Collection-ID`; 200 deletion result including whether it was already deleted |
| `POST C/entries/read` | Required `X-Collection-ID`; JSON with optional `selector_lql`, `cursor`, `after`, `through`, `first_n`, `expected_head`; one streaming multipart response with raw state bodies and separate metadata headers |

See [atomic edit wire bindings](collection-edits.md#future-lockd-wire-binding) for
head, edit and edit-status routes. These server bindings remain planned; the
current remote client reports collection capabilities as false.

The capabilities response uses boolean fields `collections`, `lql_filter`,
`resume`, `checkpoint`, `ranges`, `first_n`, `last_n`, `position_selection`,
`count_retention`, `append_writer`, `append_staged`, `append_collection_retention`,
`leases`, `lease_takeover`, `private_reads`, and `atomic_edit`.
Wire bounds/counts use canonical uint64 decimal strings, preserving exact values
through JSON runtimes with limited numeric precision. Omission, zero, ranges,
input limits and both cursor purposes have the same semantics as the C/Lua
read options above.
Pouch currently supports all except `last_n` and `position_selection`.
Descriptions use `namespace`, `name`, `collection_id`, `upper_position`,
`live_count`, `max_entries`, boolean `query_indexing`, and the `lease` observation
described in [collection ownership](collection-leases.md#future-server-boundary).
Append results use `collection_id`, `append_id`, `position`, `etag`, `bytes`,
`duplicate`, and `deleted`; append-status adds `state` and omits unassigned
entry facts when absent. Deletion results identify the position and carry
boolean `deleted` and `already_deleted` facts. Counts/byte lengths, like
positions, use canonical unsigned decimal strings in JSON.

Create, append, and the two metadata operations accept the ordinary
`X-State-Retention-Seconds` request header, with the same omission/zero/positive
semantics. Descriptions and entry observations expose the ordinary support and
absolute-deadline fields. Their responses also use
`X-State-Retention-Supported` and `X-State-Retention-Expires-At-Unix` with the
ordinary state meanings. Entry multipart headers carry those same retention
observations; they never change the raw JSON body. No extra request is needed
when retention is omitted. Collection methods remain unsupported on remote
lockd until its collection capability is implemented; normal state calls to
older servers retain their existing compatibility behavior.

Append also accepts `X-Collection-Retention-Seconds` to apply the parent
directive atomically with new membership. Results/status optionally report
`collection_retention_expires_at_unix` for the original directive, including
zero for clearing. Unsupported directives fail explicitly. A future remote
push writer sends one streaming request body to the normal append route:
successful request EOF commits, while abort closes the unfinished request.
Explicit staged pull is a client input policy using that same append route;
it adds no server-side spool or writer session. Neither form needs a batch/XA
endpoint. Lost responses still require append-status reconciliation rather than
assuming rollback.

Creation/drop are catalog-atomic. A repeated drop of the already-dropped bound
ID may succeed, but it never removes a replacement collection with another ID.
Unknown read-option names, including future bounds/counts/position sets on an
old implementation, are rejected rather than ignored.

### Raw bodies with transport metadata

Read responses use `multipart/mixed` with the standard
[MIME multipart syntax](https://www.rfc-editor.org/rfc/rfc2046#section-5.1.1).
This is transport framing, not JSON wrapping: each entry part contains exactly
the stored compact JSON bytes. Its metadata is in part headers. The client
strips transport framing, forwards body chunks directly to `lc_sink`, and adds
LF at successful entry completion. Metadata never reaches that sink.

The successful final response header block supplies `X-Collection-ID`,
`X-Collection-Upper-Position`, `X-Collection-Last-Examined-Position` and
`X-Collection-Resume-Cursor` for the initial safe boundary. A first-N window's
completed boundary may not be known yet, so its checkpoint belongs to a final
empty completion part rather than an initial response header. Reset response
metadata on interim responses, redirects,
and any retry; never mix headers from different attempts.

Each entry part requires these headers:

| Header | Meaning |
| --- | --- |
| `Content-Type: application/json` | Raw compact UTF-8 JSON state object |
| `Content-Length` | Exact JSON byte count, excluding multipart delimiters and the sink's LF |
| `Content-Collection-Kind: entry` | Entry metadata discriminator |
| `Content-Collection-Position` | Permanent position, as canonical unsigned decimal text |
| `Content-Collection-Append-ID` | Caller-supplied append identity |
| `Content-Collection-ETag` | Stored-body fingerprint, using the append result's representation |
| `Content-Collection-Resume-Cursor` | Proposed continuation after this entry, subject to completed delivery |

Use `Content-Transfer-Encoding: binary`; no base64 encoding, JSON escaping,
or reconstruction of the state body. Multipart lengths must fit the public
parser's signed length type and all size arithmetic must be checked. Existing
state size limits apply; full-range unsigned positions remain supported.

Illustrative wire body below uses displayed line breaks for CRLF delimiters.
The JSON line is the unchanged 35-byte stored object; its following delimiter
CRLF is transport syntax, not part of the state:

```text
--collection-read
Content-Type: application/json
Content-Transfer-Encoding: binary
Content-Length: 35
Content-Collection-Kind: entry
Content-Collection-Position: 123
Content-Collection-Append-ID: event_123
Content-Collection-ETag: opaque_fingerprint
Content-Collection-Resume-Cursor: opaque_resume_at_123

{"role":"assistant","text":"hello"}
--collection-read
Content-Type: application/octet-stream
Content-Transfer-Encoding: binary
Content-Length: 0
Content-Collection-Kind: progress
Content-Collection-Last-Examined-Position: 900
Content-Collection-Resume-Cursor: opaque_resume_at_900


--collection-read
Content-Type: application/octet-stream
Content-Transfer-Encoding: binary
Content-Length: 0
Content-Collection-Kind: complete
Content-Collection-Last-Examined-Position: 900
Content-Collection-Checkpoint-Cursor: opaque_checkpoint_at_900


--collection-read--
```

A progress part has an empty body and only metadata: the type/encoding/length,
kind, last examined position, and resume cursor shown above. It makes safe
filtered/deletion progress observable through the same `on_metadata` callback,
without introducing fake state objects or JSON control records. Emit progress
at the native reader's bounded opportunities. Emit a final progress part when
it advances the accepted boundary, including filtered/deleted ranges. Every
successful read then has exactly one final `complete` part: zero length, the
type/encoding headers above, the final examined position, and checkpoint cursor.
It is mandatory even for zero-count windows and empty collections. No later
entry/progress part is allowed. The adapter uses it to construct the result,
without forwarding a duplicate progress callback or any payload bytes. There
are no error-document parts, per-entry download requests, or separate metadata
connections. Generate a valid boundary of at most 70 ASCII characters; compact
state bodies contain no literal CR/LF, so no payload scan/escape pass is needed
to avoid delimiter-line collisions. Do not emit a preamble or epilogue.

Reuse lonejson's public incremental multipart parser, already used by queue
subscription transport. Its length-delimited parts stream directly through
`part_data`, and its public header views expose these metadata fields. Retain
only bounded headers and chunk/boundary scratch; do not use JSON parsing or a
whole-part capture mode. Reuse the payload hash/integrity path to compare the
received body with its declared fingerprint. The server withholds at most one
final bounded chunk until stored-record validation/authentication succeeds;
it must not send the full declared body length before that validation. A
validation failure aborts the part short of its declared length. This prevents
remote acknowledgement of an object whose final storage integrity check failed,
without staging the entire object.

Require valid collection identity, bounded header/token sizes, unique required
headers, canonical numeric fields, increasing entry positions within the
captured bound, exact body lengths, and monotonic safe progress. For an empty
future range, its initial lower boundary may exceed the captured end; it must
equal the requested `after` and cannot justify an out-of-bound entry. Reject unknown
part kinds, unexpected bodies on progress parts, or unsupported transfer
encodings. Never infer positions from line numbers. Entry metadata remains
provisional until exactly the declared body length has arrived and its
fingerprint has been validated. Only then write the sink LF, invoke the entry
callback, and accept its cursor. Progress metadata is accepted only after its
zero-length part has passed validation. Later delimiter/header errors fail the
read but do not undo already accepted entries or progress; a truncated next
part must not discard completed preceding parts.
Completion-part metadata remains provisional until multipart and HTTP completion.

Before response streaming starts, failures use the ordinary non-2xx JSON error
envelope, which is never forwarded to the payload sink. After streaming starts,
a storage/filter/integrity failure aborts the HTTP message without a successful
multipart closing delimiter. Already completed entries retain their accepted
cursors. Do not repair an interrupted object by appending JSON error content.

Success requires both the multipart closing delimiter and normal HTTP message
completion, not just status 200 or an EOF. HTTP/1.1 streaming uses chunked
framing with its required terminating chunk, as defined by
[HTTP/1.1 message framing](https://httpwg.org/specs/rfc9112.html#chunked.encoding);
close-delimited success is not allowed. HTTP/2 requires successful stream
completion; server failure resets the stream. Missing multipart completion,
truncated parts, or transport failure returns an error with accepted partial
progress. A missing or duplicate completion part also fails. Only full success
accepts the completion part's checkpoint cursor. First-N completion stops at its
selected boundary; an exhausted range includes its final gaps/nonmatches.
No correctness-critical HTTP trailers are required.

Capability reporting is endpoint-local. In the initial Pouch implementation,
collections/filter/resume, ranges and first-N are supported; last-N and explicit
position selection are not. Remote lockd operations initially return an explicit unsupported
result; do not emulate collections using ordinary remote state keys. Defining
this server API does not require remote implementation in the same change.
Older remote servers continue to support their existing ordinary state API.
Later clients may not assume support across failover endpoints or retry an
unsupported selection with its options removed.

## Error contract

Add `LC_ERR_UNSUPPORTED=8` after the existing public status values, without
renumbering them, and expose its Lua equivalent. Use it for known unsupported
backends, unavailable collection features, and valid LQL plans outside the
streaming implementation. Source/I/O, allocation, protocol, and ordinary
validation errors retain their existing status classes.
Recognized remote `collections_unsupported`, `filter_unsupported`, and
`selection_unsupported` responses map to `LC_ERR_UNSUPPORTED` as well.

Use stable reason codes in the existing `lc_error.server_code` field for both
backend kinds; its public comment must acknowledge local backend reasons.
Pouch leaves `http_status=0`. The future server uses these HTTP mappings;
ordinary remote non-2xx handling remains consistent with the SDK, while callers
can distinguish reasons independently of human-readable messages.

| Reason | HTTP status | Meaning |
| --- | --- | --- |
| `collections_unsupported` | 501 | Backend has no collection capability |
| `collection_exists` | 409 | Create encountered an existing name |
| `collection_not_found` | 404 | Open/operation has no such collection |
| `collection_stale` | 409 | Bound ID does not identify the active collection |
| `entry_not_found` | 404 | Delete requested a never-assigned position |
| `append_id_conflict` | 409 | Append identity is bound to different content |
| `cursor_invalid` | 400 | Malformed or collection/selection-incompatible cursor |
| `selection_invalid` | 400 | Invalid selection flags or reversed explicit bounds |
| `cursor_expired` | 410 | Required retained coverage is no longer available |
| `filter_unsupported` | 422 | Valid LQL cannot execute under the streaming contract |
| `selection_unsupported` | 422 | A requested later selection mode is unavailable |
| `sequence_exhausted` | 409 | No further position can be allocated |
| `collection_corrupt` | 500 | Canonical/index reference or payload integrity is inconsistent |

An old server's absent capability route can establish unsupported collections;
authentication/authorization failures, network failures, and arbitrary missing
collection names must not be misclassified as unsupported capability.

## Ordered index, recovery, and later selection

The position index is persistent, compact, and append-friendly, with bounded
page access and deletion-aware counts/summaries. It supports lower/upper-bound
seek and forward/backward traversal, including gaps, without decoding bodies.
Provide equivalent seekable collection-order iteration for indexed LQL
candidates; collecting and sorting the entire match set is not a small-tail
implementation. Reuse existing LQL planning/index primitives where they fit.

Use an append-friendly fixed-width ordered projection, with positions stored as
keys rather than used as byte offsets. Binary-searchable position rows and
fixed-size page summaries provide bounded seek, traversal in either direction,
and skipping of wholly deleted pages. Each row references its canonical record
and records append/deletion generations. An old read skips a deleted page only
when all its deletions are visible in that read's generation. Full unsigned
positions do not require gigantic sparse files or an array indexed by position.

The projection carries a checked canonical replay watermark. Validate its
header and the pages/rows actually touched, not every page at open. Append
rows and update deletion projections under collection mutation authority;
capture/read a bounded metadata batch under that authority, then release it
before storage payload delivery or user callbacks. A crash during a derived
update requires tail reconciliation or a diagnosed rebuild before serving that
projection. It must not turn an unverified row into a committed object.

A watermark alone does not prove that derived pages survived a power failure.
Under NoSync an index update can survive while its canonical mutation does not;
replaying only newer canonical records cannot undo that false projection.
Live collection handles therefore hold a process-released session lock,
separate from active-read reclamation pins. The last clean close synchronizes
canonical data and projections before atomically publishing a clean checkpoint.
The first handle after an idle period validates that checkpoint, or rebuilds
the projections from canonical records after an unclean end. Remove the clean
checkpoint durably before allowing that session's first mutation. Handles
joining a verified live session reconcile only unseen peer mutations.
Serialize the join/last-close handshake so an initializer that fails or dies
cannot be mistaken for a verified live session by another opener.

This adds no heartbeat, worker, payload cache, or per-entry recovery map. A
clean cold open retains indexed seek; the diagnosed unclean recovery may walk
retained canonical history. Never preserve a projection-only append/deletion
as a committed result. Synchronize completed canonical/projection boundaries
before publishing segment topology that relies on them.

Healthy cold reads, seek, and append must not depend on constructing a metadata
object for every historical entry. Maintain generation/watermark checks and
bounded canonical-tail reconciliation. Losing or corrupting a derived index
may require a diagnosed rebuild; that recovery case is not a claim that every
cold open is constant-time. Index validation checks pages as needed rather
than hashing every historical payload at each open.

Compaction installs replacement canonical storage and matching projection
generations through existing manifest publication authority. It cannot publish
references to missing bodies, lose retry bindings/high-water facts, or reclaim
files protected by a read. A crash between canonical publication and a derived
update converges by replay; no derived index invents a committed outcome.
Each derived projection identifies its canonical snapshot, so an interrupted
replacement cannot make an older, checksum-valid projection appear current.
Capture a committed boundary and seal its input tail under append authority,
copy raw stored records outside mutation locks, then reconcile newer committed
tail records before publishing. Preserve ciphertext/compressed payload bytes,
timestamps, fingerprints, positions, and tombstone retry bindings.

Streamed entries retain independently encoded units during compaction; only
their physical index is rebuilt. Replacement snapshots roll into a group of
segment-sized files under one head publication and read-generation pin.
Recovery replays the complete named group before its append tail. Cleanup must
retain every current part, including parts containing only payload units or
index nodes, until the next snapshot publication makes the group obsolete.

Automatic compaction selects reclaimable deleted payload according to the
existing root threshold; a pure append journal does not need repeated copying
of live history. Explicit forced maintenance can consolidate it. Track the
first-obsolete time per file so later compactions do not indefinitely restart
its grace period. Reclaim only after both that grace and active read protection
allow it.

Entry retention changes during a compaction copy defer that replacement to a
later pass. Capture an immutable metadata-mutation marker and compare it under
publication authority. This avoids publishing a new topology whose concurrent
metadata records still reference retired input files. Append/delete tail
reconciliation remains unchanged; producer calls do not wait for the bulk copy.

Dropped collections become inaccessible before background physical cleanup.
Cleanup uses the existing cadence and must converge after restart without
leaking process-owned resources or retaining orphaned collection history.
Keep one durable, coalesced namespace work reference, published before creating
its first partition. Discovery walks those work references rather than ordinary
namespace histories. Retire the reference under catalog authority after all
partitions are gone. Catalog identity remains authoritative when a partition's
small name/ID identity copy needs repair; an unbound partition can be reclaimed
only after its process-released session protection is gone.

Retention maintenance uses the shared state retention metadata while applying
collection deletion/drop authority. The ordinary namespace state janitor must
not remove bodies behind live collection references. Expire the collection
itself before scanning its entries when the parent deadline is due. An empty
collection still retains its durable maintenance work reference so its own
deadline remains discoverable after restart. Entry expiry updates ordered/LQL
visibility exactly as explicit deletion, and never renumbers surviving entries.

Current and later selection modes extend the same `read` request and server
operation. Their semantic order is fixed:

1. Establish a captured view and position bounds: `after` exclusive,
   `through` inclusive; defaults are the beginning and captured upper bound.
2. Apply any explicit position membership restriction.
3. Select the first or last N live entries; specifying both is invalid.
4. Apply LQL only to those selected entries and emit matches in increasing
   permanent position.

Thus last-N with LQL filters the last N selected live states within the bounds,
and can return fewer than N results. It never searches older states for more
matches. A last-N implementation finds its lower boundary using only ordered
metadata, then streams forward through the selected window. It does not retain
N complete bodies to reverse them or evaluate LQL outside that window.
Later explicit membership selection preserves collection order, not the order
of a supplied ID list. Offset/skip-by-current-row-number is not a stable cursor.

These later modes require read-planning/API additions, not renumbering entries,
rewriting payloads, or migrating a forward-only collection representation.
Future request fields must have explicit capability checks and cursor binding;
there is no second backend-specific collection API or replacement read method.

## Performance checks

For the separate, optional raw JSONL baseline, see
[the page-cache-cold read comparison](collection-read-comparison.md). It is
invoked with `make compare-collection-read` and is never a gate.

The native benchmark includes a public-API `collections` workload:

```sh
bash scripts/build.sh x86_64-linux-gnu-release lockdc_bench
build/x86_64-linux-gnu-release/bench/lockdc_bench 8192 collections
LOCKDC_COLLECTION_BENCH_INDEX=0 \
  build/x86_64-linux-gnu-release/bench/lockdc_bench 8192 collections
```

Each run owns a fresh root and reports append time separately from raw,
selective equality, and zero-match reads. Reads repeat on the existing handle
and after a complete client reopen; reopen does not imply a cold OS page cache.
Every read checks completed record and byte counts without collecting output.
Compare identical entry counts and body sizes with indexing on and off. The
indexed cases must preserve scan results and avoid body reads for covered
nonmatches; unfiltered delivery must not acquire an LQL parsing cost. Small
timing differences are diagnostic, not a portable absolute latency guarantee.

`LOCKDC_COLLECTION_BENCH_PAYLOAD` selects string padding bytes (default 1024,
maximum 1048576). `LOCKDC_COLLECTION_BENCH_SYNC`,
`LOCKDC_COLLECTION_BENCH_CRYPTO`, and `LOCKDC_COLLECTION_BENCH_COMPRESS` select
strict synchronization, encryption, and zlib respectively when set to `1`;
they default to `0`. Keep strict-sync runs small and compare durability modes
separately. No benchmark introduces a body cache or buffers streamed output.

`LOCKDC_COLLECTION_BENCH_PUSH=1` selects the public append writer and feeds
4096-byte fragments; the default uses pull append. Both publish into shared
rolling segments and use the same durability batcher. Push writes bounded
canonical extents as input arrives and commits metadata without a payload copy.
Both workloads verify the same raw results.

`LOCKDC_COLLECTION_BENCH_WRITERS` selects 1..32 concurrent append callers
(default 1), each using its own receiver and input buffer. Set
`LOCKDC_COLLECTION_BENCH_SHARED=1` for shared-writer root policy; the default
is exclusive writer policy. For a bounded durable group-commit comparison:

```sh
LOCKDC_COLLECTION_BENCH_SYNC=1 LOCKDC_COLLECTION_BENCH_WRITERS=8 \
LOCKDC_COLLECTION_BENCH_SHARED=1 LOCKDC_COLLECTION_BENCH_INDEX=0 \
  build/x86_64-linux-gnu-release/bench/lockdc_bench 512 collections
```

Compare with the same command and one writer, keeping body size, indexing,
encryption, compression and root policy fixed. Parallel append timing includes
receiver/thread setup and teardown. Read counts, raw byte counts and final
collection cardinality/high-water position are checked in every run.

## Compatibility and verification

`lc_unit_mixed_storage` checks ordinary states and collections on one Pouch
root. Its three writer processes reopen over three coordinated rounds, writing
216 ordinary states and 216 corresponding entries, plus conflicting CAS writes,
conflicting append IDs and identical retries. Writers run inside compaction's
capture/copy window. Assertions require segment rotation, append-ID table growth,
more than three order pages, a complete query-index page, and consistent filtered
and unfiltered reads after reopening. The workload is bounded by these paths,
not a long stress-test duration.

The same executable tests independent loss or seven-byte tearing of each
unsynced canonical tail while newer projections survive (six combinations),
mixed synchronization failures and retries, and clock-driven state/entry renewal,
expiry, compaction, parent collection expiry and recreation. Successful durable
prefixes are preserved in the loss fixtures. These are explicit storage-loss
models; process exit alone does not discard dirty filesystem pages.

An immutable head descriptor pins each receiver's cached file identity until
refresh or close. This prevents inode reuse after atomic replacement from making
a stale head appear current; a focused regression checks pin lifetime and peer
refresh after compaction. It adds one metadata descriptor per open receiver.

The `test_crash_*` collection cases intentionally terminate children with live
handles, without running shutdown cleanup. Retention's power-loss/failure cases
also terminate children and discard only canonical bytes not covered by a
successful sync. Run the registered crash cases under native sanitizers; the
portability selections retain publication/recovery cuts on the other Linux
targets. The complete retention suite runs on every Linux target.

For clean-shutdown memory checking, select the complementary collection cases
explicitly. The same command also runs retention's fault/recovery coverage:

```sh
LOCKDC_VALGRIND_PRESET=x86_64-linux-gnu-release \
LOCKDC_VALGRIND_TESTS='lc_unit_collection_index lc_unit_collection_sync lc_unit_collections lc_unit_mixed_storage lc_unit_state_retention' \
CMOCKA_SKIP_FILTER='test_crash_*' bash scripts/valgrind.sh
```

Follow the public C ABI policy when adding receiver fields. Collection leases,
receiver methods, atomic edits and read preconditions now use core ABI 9,
advancing once from ABI 8 in v0.25.0. Rebuild consumers and optional providers.
See [atomic edits](collection-edits.md) for range cuts, recovery descriptors and
persistent retry outcomes. Ordinary appends remain independent operations.
Count-retention layouts were introduced with ABI 7. Do not tie
Pouch record or HTTP protocol version markers to the repository release tag.

Document the final C declarations, Lua facade, ownership, errors, and server
framing together. Add executable public-only examples and installed static,
shared, and Lua consumer proof. Native/storage/C/Lua cases below apply to the
first Pouch implementation. The
multipart/HTTP cases additionally gate enabling the future remote capability;
they do not require implementing or modifying lockd in the Pouch change.

- Empty, large, pretty-printed, chunk-split, invalid/trailing JSON and source
  failures; bounded memory with keys/strings/objects larger than the chunk size.
- Unique ordered positions under threads/processes; delete first/middle/last/
  all, compaction and restart; no reuse or renumbering and checked overflow.
- Same-ID retries, conflicting bodies, lost acknowledgements, deleted retries,
  partial publication, dropped/recreated names, and recovery at each canonical/
  projection/manifest boundary in both configured durability modes.
- Optional parent/entry retention, omitted preservation, explicit clear,
  renewal after expiry but before maintenance, checked deadline arithmetic,
  and publication-time deadlines after slow uploads. Verify empty-parent
  expiry, independent child expiry, parent policy changes with existing and
  future children, and no resurrection on append retries or metadata races.
  Prove metadata-only updates never consume or rewrite payload bytes, including
  encrypted/compressed bodies. Cover restart, compaction, dropped/recreated
  identities, captured read metadata, resumed reads, janitor races, and C/Lua
  parity using the common state retention representation.
- Identical LQL results with indexing enabled/disabled, residual predicates,
  lagging-index appends/deletes, selective and zero-match filters, frozen
  relative time, and explicit unsupported plans without a spool fallback.
- Exact equality between compact stored body bytes and emitted object bytes,
  with one LF added per completed object and all cursor/position metadata
  excluded from the sink. Instrument the unfiltered/exact-indexed path to prove
  there is no presentation parse/serialization or whole-object staging.
- One captured upper boundary under continuing writers; concurrent deletion/
  compaction/namespace drop during backpressured reads; process-crashed readers
  releasing protection; cancellation, callback failure, and corrupt/missing
  payloads.
- Per-object checkpoint/resume after every completed object, callback failure
  after sink acceptance, partial body/LF failures, filtered-only progress,
  both cursor purposes, selector/collection mismatches, and expired cursors.
  Verify a failed object never advances progress and accepted earlier objects
  are not replayed when their cursor is used. Cover lost application checkpoints
  separately; this does not promise exactly-once external side effects.
- Public multipart parser fixtures with arbitrarily split headers/delimiters,
  large bodies, zero-length progress parts, duplicate/missing/oversized headers,
  incorrect lengths/hashes, nonmonotonic positions, and uint64 positions above
  Lua's exact numeric range. Verify direct chunk forwarding without JSON
  parsing and identical native/remote-adapter metadata ordering.
- Errors after 200, server integrity failure after sending almost all payload
  bytes, truncated next-part headers, missing multipart closing delimiter,
  missing HTTP chunked terminator, missing/duplicate completion part, and HTTP/2
  reset. A provisional completion checkpoint must never be accepted after
  these failures.
  Previously accepted per-object cursors must remain usable. No per-entry
  network request or thread is permitted.
- Bidirectional ordered-index and indexed-candidate seek now, including sparse
  deleted regions and large positions. Verify healthy cold range seek without
  full metadata replay/sort; last-N and position sets remain deferred.
- Allocation tracking, fault injection, relevant sanitizer/Valgrind checks,
  GNU/musl and ARM runtimes, SDK ABI/export checks, and C/Lua parity.

Measure append and cold/warm ordered/filter reads at increasing collection
sizes with plaintext, crypto, and compression, under NoSync and strict sync.
Report bytes, entries, examined candidates, file opens, memory, worker count,
and elapsed time. Use structural assertions rather than host-sensitive latency
thresholds in regression tests. "Best in class" requires measured evidence.
Ordinary state operations must retain their existing performance envelope.

The separate CAI adoption must demonstrate its real resume/export workload.
v1 checkpoints allow incremental recovery reads; efficient last-turn console
selection remains a later collection-read feature and must not be claimed by
merely adding filtered full-history replay.
