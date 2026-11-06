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

Temporary duplication is limited to the current source file and its converted
output. Ordinary state files stage one replacement. Indexed collection data goes
directly into final canonical parts, rolling at the soft segment target plus
one independently encoded unit; conversion never concatenates a large streamed
entry into one file. Decompression needs the unavoidable increase in final
payload space, plus headroom for the old source until that step is durable.
Legacy contiguous records or snapshots can exceed the configured segment target;
allow for the largest retained source file, journal metadata and rebuilt indexes.

The private `.compression-work/` directory contains an immutable file inventory,
per-file offset maps, collection reachability tables and replacement receipts,
small original collection heads and continuation identities, and at most one
staged ordinary payload file. Maps contain offsets, output identities and tree
totals, never document bodies. Their space scales with canonical record count. The directory
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

Ordinary state references are repaired after encoding their payload files, using
the durable offset maps. Files without references need no second rewrite. A
reference-bearing ordinary file is copied individually, repaired and durably
replaced with the same protocol.

Collection conversion first validates the published roots and saves durable
metadata-only reachability tables. Unpublished private ranges need not contain
parseable units. Referenced units and index pages must validate, and their source
files are synced before the reachability boundary becomes durable. Units precede
their index pages and publications during conversion; snapshots precede segment
aliases. Unit data crosses the codec once through bounded scratch. New index
references and subtree totals live in the relocation maps, so reads need no new
index format or prefetch cache. Unit zero retains the logical manifest binding;
its converted descriptor is reused exactly by its publication and retention
aliases.

All output parts and their names are synced before a source-retirement receipt
becomes durable. Only then is that source removed. After interruption, completed
parts are reused and only the unfinished source step is regenerated. Head
publication translates segment identities and the complete snapshot group.
Collection aliases resolve inline and need no second payload rewrite.

 Original
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
open. Indexed collection tests also fail the final unit-output sync, or discard
its unsynced name or bytes, and require the original source to remain available
for a successful retry. They also leave offset-map and reference receipts visible after a failed
publication sync, then interrupt a retry after its canonical-directory sync,
discarding only receipt names that were not made durable. Recovery sync errors
must preserve the prepared replacement and original canonical file. Together
with injected fsync failure, these cover write-loss cases that
process exit alone cannot reproduce; they are a targeted durability model,
not a hardware power-cut test or a complete filesystem emulator.
