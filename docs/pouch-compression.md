# Pouch compression changes on open

New roots default to `compression=none`. On an existing root, an omitted
compression option inherits the root manifest. Typed C settings and Lua's
`pouch.compression`, the legacy C field, and the endpoint query retain their
existing precedence. An explicit `none` is a request, not omission.

A matching mode opens normally. A differing explicit mode requests synchronous
conversion before the client becomes available. Compression uses zlib before
encryption; decompression preserves the root's encryption policy and key
identity. Conversion never changes state bytes, ETags, versions, fencing tokens,
transaction decisions, retention deadlines, collection IDs or positions, history
boundaries, or valid collection continuations.

Conversion requires the existing root writer lock exclusively, including
same-process clients. A conflicting writer prevents a new conversion: warn and
adopt the stored mode for this open. Normal single-writer admission rules still
apply. An unfinished conversion always requires exclusive recovery; another
opener cannot bypass it by inheriting the stored mode or requesting that mode.

## Bounded payload space

Conversion replaces canonical segments and snapshots individually inside the
existing namespace tree. It never builds a second root. Each payload passes
through the existing decoder and encoder using bounded chunk scratch; no
complete document or root is buffered in memory. Only retained canonical
history, live collection records and staging are converted. Structural metadata
is not a compressed archive of the directory.

Temporary duplicated payload space is bounded by one replacement canonical file
at a time, which can be a segment or snapshot. A large record can exceed the
configured segment target, and snapshots can be larger than a segment. Allow
space for the largest converted file, the conversion journal and rebuilt derived
indexes. Decompression also needs the unavoidable space increase in the final
root; this is not temporary duplication.

The private `.compression-work/` directory contains an immutable file inventory,
per-file offset maps and replacement receipts, small original collection heads
and continuation identities, and at most one staged payload file. Maps use space
proportional to record count; they do not contain document bodies. The directory
remains mode `0700` until reclamation. Namespace directories and catalogs stay
in place with their existing permissions; each replacement preserves its source
file's mode before installation.

## Durable progress and recovery

Persist `.compression-ready` with the source and target modes before modifying
any canonical file. Hold exclusive root ownership throughout conversion. The
physical root may contain both encodings while this intent exists, but no client
is admitted until recovery and cutover finish.

For each file, durably write its converted replacement and offset map before
renaming the replacement over the source. Synchronize both affected directories
before proceeding. Replacing the source releases its old payload storage;
completed files are not recompressed on restart. A partial replacement without a
durable receipt is discarded and retried for that file only. Replacement receipts
identify the original and replacement files, allowing recovery to finish an
interrupted rename without applying conversion twice.

On retry, synchronize the journal directory before using any recovered receipt
or backup. A prior publication can remain visible after its directory sync
failed; it must become durable before a canonical replacement can become
durable. If that recovery sync fails, stop before installing replacements.

After encoding all payload files, repair references using the durable offset
maps. Files without references need no second rewrite. A reference-bearing file
is copied individually, repaired and durably replaced with the same protocol,
so temporary payload duplication remains limited to one file. Original
collection heads and continuation identities are saved before translating tail
boundaries and rebuilding indexes. Recovery can repeat that step without
translating a translated offset or changing an existing continuation identity.
Discard original derived collection indexes and interrupted rebuild files before
rebuilding, so retries do not accumulate full index copies.
Invalidate ordinary derived indexes and collection query artifacts whose
physical offsets changed. Reclaim manifest-listed obsolete segments and
snapshots; they need no conversion.

Only after all files, references, heads and index invalidations are durable,
persist the final root `compression=` setting. Remove the work directory and
then the intent durably. A restart after manifest publication only finishes
cleanup. A restart at an earlier point resumes the remaining work, even when
compression is omitted. An explicit request for the opposite mode first finishes
the pending cutover, then performs the newly requested conversion. Publication
left by the previously released whole-tree converter is also completed before
clients are admitted; new conversions never create those payload trees.

## Canonical boundaries and failures

Ordinary conversion excludes manifest-listed obsolete segments, including files
still present during the deletion grace period. Only the latest snapshot and
non-obsolete segments enter the inventory; retired references to superseded
snapshots are not replayed. Live references and metadata after compaction remain
intact.

The collection name catalog determines which partitions are converted. Unbound
partitions left by interrupted creation or reclamation are skipped, even when
their head is missing, and remain eligible for ordinary orphan maintenance.
Missing or corrupt heads of catalog-bound collections remain errors.

Collection retention updates use the converted payload's exact descriptor when
the immutable binding is unchanged. This preserves metadata/body identity for
encrypted reads and later compaction; a changed binding still requires
authentication and rebinding.

Conversion follows collection replay's publication rules: incomplete active
headers and PENDING tails can be discarded while preserving the committed
prefix. A finalized record with a truncated key, metadata or payload is
corruption and fails conversion. The damaged source file remains available for
repair; other segments may already have been converted. A pending root cannot
be reopened normally until that repair and conversion succeed.

I/O, allocation, integrity, key-validation or space failures fail the open with
an actionable SDK error. They never masquerade as a successful conversion.
After correcting the cause, opening the root resumes conversion automatically.
Do not delete the intent, work directory or offset maps to bypass recovery.

Use libpslog warning events for conversion start and writer-conflict refusal,
and an info event after durable completion. Include path, source/target modes,
elapsed time, recovery status and useful byte counts without payloads or key
material. Ordinary matching/inherited opens without a pending intent do not scan
or rewrite the root for conversion.

Verification covers both directions, omission versus explicit none, encryption,
retained history and staging, ordinary states, attachments, collections and
continuations, process and same-process writer conflicts, partial output writes,
allocation failures, repeated resumptions, crashes around segment/reference
replacement and index/head publication, private staging, and bounded payload
space with multiple namespaces and segments.

Conversion durability tests also model lost unsynced bytes and directory
updates: discard or tear an unflushed staged payload, restore a reference copy
to its last successfully synced bytes, undo an unsynced rename, or retain the
staging name when only the destination directory was synced. The tests preserve
successfully synced journal/data state and assert recovery through ordinary
open. They also leave offset-map and reference receipts visible after a failed
publication sync, then interrupt a retry after its canonical-directory sync,
discarding only receipt names that were not made durable. Recovery sync errors
must preserve the prepared replacement and original canonical file. Together
with injected fsync failure, these cover write-loss cases that
process exit alone cannot reproduce; they are a targeted durability model,
not a hardware power-cut test or a complete filesystem emulator.
