# Atomic collection edits

An edit atomically removes explicit position ranges and optionally publishes one
replacement state and a small recovery descriptor. It affects one collection;
it is independent of implicit/managed XA. Pouch provides the implementation.
Remote capabilities currently report false; this release has no remote
collection transport. Future wire bindings are specified below.
The C receiver, plain C functions and Lua facade provide the same behavior.
Public layouts use core ABI 9, advancing once from ABI 8 in v0.25.0; rebuild
consumers, facades and optional providers against the same core.

## Public contract

`head` observes collection identity, an opaque head ETag, committed upper
position, live count and recovery descriptor together. The token covers entry
publication, deletion, entry metadata, collection retention/count policy and
descriptor edits. It is not an ownership credential. Lease renewal alone does
not change it. Physical compaction preserves it; canonical recovery may
invalidate it conservatively. A token is scoped to the collection incarnation.

`edit` requires an operation ID and an expected head ETag. IDs use the existing
1..128 byte append-ID alphabet, in a separate operation namespace. The request
contains at most 64 sorted, disjoint `(after, through]` ranges. Bounds are finite,
strictly increasing within each range, and cannot exceed the observed committed
upper position. Adjacent ranges are allowed. Holes and already deleted entries
are harmless. Only newly removed live entries count as deleted.

Bounded request options, ranges, IDs, token and recovery text are copied at
entry, before calling the source. Producer callbacks cannot change the operation
being committed. Keep original retry metadata independently of result storage.

Replacement input, when present, is exactly one JSON object supplied as a
borrowed source and paired with the ordinary append request. For a new
operation its append ID must be unused. The JSON validator/codec consumes bounded
chunks with backpressure; neither the replacement nor the retained prefix is
materialized or spooled.
It gets the next permanent position; cuts never renumber entries. Entry and
parent retention directives use ordinary append semantics, measured at commit.

An optional recovery descriptor is opaque UTF-8 application text of at most
1024 bytes, conventionally JSON. Omission preserves; an explicit empty value
clears. A separately managed anchor is either zero, an existing live position,
or the replacement published by this edit. Descriptor changes and the assigned
anchor publish together. Omitting recovery preserves both fields. Zero anchor
means no required entry and reports available. Descriptors are encoded/encrypted
using the root's normal payload codec and are visible only according to payload
lease rules.
They must not contain a full context or journal. Deleting/expiring an anchor
makes it explicitly unavailable; the descriptor never pins its payload.

Empty range lists permit append-only and descriptor-only edits. Even a no-op
edit records its identity and outcome. Ordinary appends keep their existing
API and fast path; applications needing an atomic recovery marker use an
append-only edit. There is no automatic rebase on conflict.

The result includes the original resulting head token, upper position, live
count, newly deleted count, optional replacement result and duplicate flag.
`edit_status` observes the original committed result without scanning entries
or opening replacement payloads.
An absent status reserves nothing and does not cancel an in-flight edit.

Identical retries return the original result before checking the now-stale
expected head. Request identity binds the expected token, exact ranges,
replacement identity/compact JSON bytes/retention directives and descriptor
update/anchor directive. Changed requests under a committed ID fail explicitly.
Receipts survive later writes, entry deletion/expiry and compaction, until the
collection is dropped. They retain small outcome metadata, never deleted bodies.

Validation and lease fencing are rechecked after input callbacks, including
when an identical request matches a committed receipt. An identical retry
bypasses only the stale-head precondition; an expired, released or replaced
writer lease still rejects the call. Such a rejection leaves the original
commit intact and returns no acknowledgement or uncertain-publication result.
An authorized receiver can inspect the receipt or retry the original request.
A conflict or prepublication failure changes no visible collection state. On
`collection_changed`, C output and Lua `error.current_head` contain bounded
current token/count facts.
An error after publication is uncertain and must be reconciled through status.
The C result's `published` flag distinguishes this from a prepublication failure
or a conflict observation. Lua errors include `uncertain=true` and the original
`result` in this case. A successful identical retry also waits for selected
root durability before acknowledging its original outcome.
Durability follows the root's configured sync policy, including its documented
NoSync power-loss limitations. Cancellation cannot undo publication.

An optional expected head on `read` validates admission before delivering bytes.
An admitted read retains its captured generation even if an edit publishes
later. Cursor resumption still captures fresh visibility: it does not replay
deleted entries or restore an earlier snapshot. Source/sink callbacks remain
outside metadata/commit authority. Direct edit input holds append authority,
like ordinary direct append; it is not intended for externally blocked sources.

## Canonical publication and derived indexes

An optional replacement uses a distinct provisional-body record kind. Its
finalized bytes alone are invisible. One following edit record contains bounded
range metadata, the request fingerprint, original outcome and resulting recovery
descriptor. That record is the publication point for the complete edit. Both
records reside in the same rolling segment and sync as one commit. Recovery
requires a replacement to immediately precede its edit, with matching identity,
generation and digest. Interrupted provisional bodies remain invisible. Errors
before edit publication roll their bytes back under append authority; recovery
truncates a valid unpaired provisional body at the active tail after a crash.
Compaction also omits
unreferenced bodies. An incomplete header or PENDING suffix is discarded; a
truncated finalized record remains corruption, including a provisional body.

The existing order index receives one bulk mutation generation. Touched metadata
pages are updated in batches; retained payloads are neither read nor rewritten.
A persisted in-progress projection boundary, written before edit publication,
forces canonical reconstruction after an interrupted update. Publication holds
metadata authority until all derived facts are consistent. Already admitted
readers keep their old generation
and process-held snapshot pins, so future deletion stamps cannot remove their
entries. New readers repair incomplete projections before capturing a view.

Range counting uses live summaries for fully selected metadata pages and decodes
rows for partially selected pages. Mutation still stamps individual rows to
preserve older readers. Dense positions allow a direct ordinal seek with row
validation; sparse positions use unsigned-key binary search. Publication projects
the writer's bounded control directly, while recovery and status observations
decode and validate canonical receipts from disk.

Operation receipts and the latest recovery descriptor use domain-separated keys
in the existing derived ID index. Its cardinality includes entry bindings,
operation bindings and one latest-edit binding. No per-edit file, independent
authoritative outcome log or general transaction manager is introduced.

Compaction materializes ordinary entry tombstones for range-deleted bindings
without opening deleted payloads. It preserves operation receipts and the latest
descriptor alongside retained entries. Compression conversion preserves record
kinds and uses the ordinary codec/reference conversion rules. Query projections
remain derived and obey the same deletion generation as ordered reads.

Edit cost is affected metadata rows/pages plus replacement bytes and bounded
control metadata, with the existing ID hash table's amortized growth cost.
Large cuts are not claimed to be constant time. There is no
per-entry canonical delete or per-entry fsync. Allocation uses the library
allocator; multi-KiB aggregates belong on the heap, respecting default musl
worker stacks.

## Future lockd wire binding

These are planned bindings within the existing `/v1` API. Neither remote
collection transport nor a lockd server implementation ships here. Advertise
`atomic_edit` only with head, edit/status, recovery metadata and read admission
preconditions available together. Namespace authorization and collection lease
headers retain the existing collection contracts. Require `X-Collection-ID` on
all bound calls; operation identities are scoped to that incarnation.

Let `C` be `/v1/namespaces/{namespace}/collections/{name}`, with independently
percent-encoded components. Head tags are opaque 64-character lowercase hex
values, carried as quoted HTTP ETags; clients must not construct or parse them.

| Method/path | Contract |
| --- | --- |
| `GET C/head` | One JSON head observation, including identity, ETag, decimal upper/live counts, recovery text/revision/anchor and anchor availability; enforce payload visibility |
| `POST C/edits` | Required `Idempotency-Key` and a single strong `If-Match` head token; 201 for a new edit, 200 for an identical committed retry |
| `GET C/edits/{operation_id}` | Persistent original outcome or `found=false`, without replacement bytes; absence does not cancel another call |
| `POST C/entries/read` | Add optional JSON `expected_head`; reject a stale observation before response payload bytes |

A removal/marker-only edit uses bounded `application/json` control metadata.
An edit with a replacement uses `multipart/mixed`: one JSON control part of at
most 32 KiB, then exactly one raw `application/json` object part. Parse and
validate the control before consuming the replacement. Stream the second part
with backpressure; never embed, base64-encode or materialize the state in a JSON
envelope. Reject extra parts. The controls contain `ranges`, optional ordinary
`replacement` append options and optional `recovery`/`recovery_anchor` directives.
Omitted recovery preserves both marker fields, an empty string clears them,
and an anchor of `"replacement"` refers to this operation's new entry. All
positions/counts use canonical uint64 decimal strings, matching Lua; bounds and
size limits match the C contract.

A stale token returns HTTP 412 with `collection_changed` and a bounded current
head summary. Check a known committed operation before rejecting its original
stale precondition, while still verifying request identity. A changed request
under a committed ID returns 409 `operation_id_conflict`. Apply existing lease
and input error mappings. A known postpublication error carries
`uncertain=true` and its result; connection loss can always require reconciliation
through status or an identical retry. These are additions within the existing
protocol version, independent of the repository's release tag.

## Verification

Focused tests cover suffix/disjoint cuts, holes, original positions and bytes,
all request forms, invalid bounds/limits, stale heads after each mutation kind,
ownership changes during input, retries after later mutations/compaction,
descriptor clearing and unavailable anchors, and head-validated read admission.
Fault injection covers input, canonical publication, projection and sync errors;
reopen must reveal exactly an old or new complete view. Deterministic barriers
cover pinned readers and peer writers without timed sleeps. Codec and conversion
tests exercise encrypted/compressed roots, malformed finalized records and
interrupted bodies. C and Lua both exercise failures and cleanup with allocator
tracking. Final target coverage includes a default-stack musl worker.

Disposable benchmarks compare ordinary appends before/after, append-only edits,
suffix and disjoint cuts at increasing collection sizes, and head/tail reads.
Report CPU, wall time, bytes written and retained-prefix independence; avoid
noisy timing thresholds in CI. CAI's released 0.25.0 read report predates the
current performance changes and is not a matched baseline for this work.

## Disposable performance comparison

Build `lockdc_collection_edit_probe` in a native Release build. Its arguments are
`FRESH_ROOT ENTRIES CUT JSON_BYTES plain|zlib|encrypted-zlib nosync|durable [suffix|scatter]`.
Scatter removes at most 64 evenly spaced entries. The synthetic body uses a
deterministic ASCII pseudorandom sequence.
The root must not exist and remains caller-owned after the run. Seeding uses
NoSync followed by a clean close; the measured append/edit/read operations use
selected durability. Output is CSV: codec, durability, selection style, seed entries, cut size,
JSON bytes, average ordinary-append wall/CPU seconds, edit wall/CPU seconds,
edit write characters, tail-read wall/CPU seconds, read characters and delivered
bytes, followed by average post-edit append wall/CPU seconds (16 appends in
each append phase), then edit read characters, read calls and write calls.
Process I/O snapshots include a small fixed measurement overhead; character
counts measure syscall traffic, not physical disk
I/O. Compare matched fixed cuts at increasing prefix sizes and fixed prefixes
at increasing cut sizes; repeat samples and report medians. Tail bounds use
permanent positions, so cuts can reduce the number of live tail entries.
