# Pouch segment space: future storage direction

Status: **draft for investigation and prototyping**. This is a possible future
physical-storage refactor, not an implemented feature or authorization to change
existing roots. The [current storage contract](pouch-storage.md) remains the
implementation authority. Public settings, layout constants and allocation sizes
below are not frozen interfaces.

A [native container-foundation experiment](pouch-space-experiment.md) now
implements a limited physical storage slice outside normal Pouch open behavior.
It uses 10 MiB initial capacity and explicit 1 MiB growth steps on native x86_64
Linux. Its measurements and correctness envelope do not establish a production
cutover or freeze the policy choices in this draft.

Full native x86_64 implementation is now authorized in verified stages under the
[implementation contract](pouch-space-implementation.md). This does not
authorize conversion of existing roots or a production cutover. The growable
checkpoint directory is being implemented separately before replacing the
foundation's fixed metadata banks and integrating normal Pouch behavior.

## Intent

Store a Pouch root in one preallocated, extensible file, called its segment
space. Retain logical namespaces, collections, segments and snapshots, while
allocating their physical storage inside that file. Target fewer inodes and
filesystem metadata operations, predictable capacity management, and equal or
better read/write performance.

The physical container must cover ordinary states, objects, attachments,
leases, queue/outbox records, XA coordinator/participant records, collections,
catalogs and indexes. Preserve the existing distinction between authoritative
records and rebuildable projections. This proposal adds no transaction semantics,
SQL layer, payload cache, point-in-time recovery or log-archive product.

Single-file storage means that all persistent store state, rebuildable indexes,
coordination metadata and journals reside in the container, with no persistent
sidecars. Transient scratch files are permitted for explicitly documented
operations, including mutation processing, private snapshots and lonejson
spool-backed fields. They are disposable working state: losing them may abort
an in-flight operation, but must never prevent recovery of committed data.
Define their ownership and cleanup on success, failure and process death.

Use a narrow typed storage boundary for allocation, offset I/O, publication,
retirement and growth. Do not implement a general filesystem inside the file or
translate every existing pathname operation into a virtual pathname operation.

## Existing foundations and affected boundaries

Pouch already retains numeric segment identities, typed payload references,
resident namespace projections and reusable descriptors. Collection readers
already stream stored JSON through bounded decoding buffers. Preserve these
properties; eliminating filename lookup is not, by itself, evidence of a faster
hot path.

The refactor replaces real responsibilities currently supplied by the filesystem:

| Current mechanism | Candidate replacement |
| --- | --- |
| Segment/snapshot files | Owned extents in the segment space |
| Namespace and collection directories/catalog files | Typed catalog records and metadata blocks |
| Manifest/head replacement by rename | Durable metadata publication inside the container |
| File creation and unlink | Extent reservation and safe reuse |
| Per-file locks and reader-generation files | Container coordination ranges and shared reader ownership |
| File length as append-tail observation | Explicit published logical tails |
| Derived index files | Rebuildable index regions with canonical-generation bindings |

An allocated extent's end and the container's physical EOF are not committed
record boundaries. Existing `ftruncate`, rename, unlink, descriptor-lifetime and
`flock` assumptions need explicit replacements. A shared file descriptor alone
cannot reproduce them.

## Candidate physical organization

Keep bounded bootstrap metadata at known offsets: container identity, committed
capacity and generation, and independently checksummed checkpoint roots. Reserve
coordination ranges separately from payload and metadata storage.

A typed directory resolves a namespace/collection and logical segment identity
to its physical extents, generation and published length. Resolve hot references
through compact metadata, without pathname construction, scanning all segments,
or consulting a global tree for every payload chunk. Metadata working sets are
permitted; document-body caching is not part of this design.

Use suitably aligned allocation units, with large contiguous reservations for
sequential payloads and smaller allocations for metadata and small streams.
Extent size and directory representation are prototype decisions. Do not reserve
a full large segment for every tiny collection or require RAM proportional to
preallocated capacity.

Keep logical segment identity separate from reusable physical location. A
reference must identify the container, logical owner, incarnation/generation and
checked offset/span as needed to reject stale or cross-owner references. Reusing
space must never silently make an old reference address a different object.
Persist fixed-width fields with explicit encoding, not process pointers or
architecture-sized structs.

Read and write by explicit offsets through bounded buffers. Whole-file mapping
is not required. An entry larger than one extent consumes additional extents as
input arrives; entry size must not determine buffer size. Preserve incremental
JSON validation, compression, encryption, hashing and backpressure. Preserve
existing direct streaming paths; the physical-storage refactor must not add a
whole-entry buffer, temporary payload spool or payload-copy commit to those
paths. Operations that explicitly use transient scratch remain spooled or
file-backed operations and must be described and measured as such.

Published bodies and their descriptors retain their existing validation rules.
Keep references to metadata-only retention changes bound to the correct stored
body. Physical relocation must preserve logical identities, collection positions,
retry receipts, versions and transaction outcomes.

## Allocation and independent writers

Keep independent append ownership for namespaces and collections. One file does
not imply one global append cursor or a global writer lock held across input
callbacks. Writers reserve bounded runs of space, then write within those runs.
Serialize shared allocation metadata briefly and amortize it across multiple
records. Slow producers must not hold allocator or root-publication ownership
while waiting for input.

Retain exact-key exclusion, collection lease fencing, collection publication
ordering and existing transaction decision authority. Physical reservations do
not publish a state, collection entry or transaction result. Failed or cancelled
writers may leave reclaimable private space, but cannot expose partial values or
erase an already published outcome.

The default exclusive-root runtime must retain its inexpensive process-local
coordination. Explicit shared-root mode must coordinate independent processes,
including process death and owner takeover. File byte-range locks and process
mutexes are candidates; the exact portable protocol needs proof on supported
platforms. Do not substitute whole-file `flock` for every existing independent
lock or add one persistent sidecar per namespace, collection or reader.

Reuse the existing maintenance ownership mechanisms where possible. Any shared
reader/session registry must grow with active participation rather than impose
an arbitrary permanent reader limit, and must distinguish a dead process from a
slow live one.

## Publication, durability and recovery

Retain the configured NoSync and durable-sync contracts, uncertain-publication
errors, and idempotent outcome/status reconciliation. Preserve append grouping
and the root durability batcher. A structural allocation record is not evidence
that its payload or transaction committed.

A candidate protocol uses a small metadata journal inside the file and periodic
checkpoints with independently validated roots. It must define ordering for
reservation, body completion, logical publication, checkpoint installation,
retirement and reuse. Existing canonical logs remain authoritative for logical
operations; do not add a second full-payload WAL or replay application SQL.

Do not assume that overwriting a header, sector or checkpoint pointer is atomic.
Use checksums, generations and synchronization ordering to identify recoverable
states after torn, missing or reordered writes. Every checkpoint that recovery
may select must retain its referenced extents until the protocol durably retires
that checkpoint. A checksum-valid but obsolete checkpoint cannot authorize
fallback into reused storage.

The essential reuse rule is:

> An extent becomes writable by a new owner only after no current reader and no
> recoverable durable publication can still reference its previous contents.

Generation checks detect stale references; they do not replace this protection.
An open descriptor for the container does not pin an individual extent.

In particular, a NoSync deletion or replacement cannot durably free storage while
a restart could still select the older durable reference. Make the retirement
boundary durable before permitting overwrite. Structural durability must prevent
allocation overlap and preserve previously durable data even when newer NoSync
operations are lost.

Recovery starts from bounded bootstrap metadata, a valid checkpoint and its
necessary journal/canonical tails. It must not scan unused preallocated capacity
or parse all historical bodies on each open. Track abandoned reservations so
owner recovery can reclaim them without inventing committed logical outcomes.
Losing a rebuildable index may require its documented reconstruction; a healthy
open must not take that path routinely.

Single-file synchronization changes the interference boundary: `fsync` and
`fdatasync` operate on a file rather than one namespace's logical segment. Measure
whether unrelated dirty writers increase durable-commit latency. Do not weaken
durability or use range writeback as an unsupported substitute for a durable
commit barrier.

## Capacity policy and growth

Separate reserved physical capacity, committed usable capacity, live allocation,
reclaimable space and maintenance reserve. Capacity must mean real backing-space
reservation where supported; extending a sparse file alone does not meet that
contract. Native preallocation support and failure behavior require verification
on Linux glibc/musl and Darwin. Never silently fall back to a concurrent
zero-filling implementation with different correctness or latency properties.

The proposed policy exposes these concepts; names and numeric defaults remain
unselected:

- initial reserved capacity;
- growth increment and optional maximum capacity;
- free-space threshold for proactive growth;
- a protected reserve for metadata, recovery, compaction and conversion;
- optional proactive and synchronous emergency growth, disabled unless selected.

The intended operational behavior is proactive reservation during a configured
maintenance window, such as overnight, with an optional synchronous growth path
when an operation cannot obtain sufficient space. Calendar/time-zone policy
belongs to the embedding application. It can invoke the capacity-maintenance
entrypoint in that window; capacity work should reuse existing Pouch maintenance
coordination rather than introduce a general scheduler. This is in-process work,
not a promise that a closed database schedules itself overnight.

Coordinate growth across all participating processes. Reserve the new file range,
make the required file/capacity metadata durable, then publish it as usable.
After interruption, uncommitted trailing capacity is unavailable until recovery
validates or completes the growth. Existing offsets remain stable.

A conservative first implementation may pause new writer admission for emergency
growth. Existing readers should remain usable where possible. Online extension
can be evaluated because offset-based I/O does not require remapping old ranges;
its publication and allocator barriers still need proof. Do not hold a global
mutation lock across unbounded producer callbacks merely to reach a resize point.

When growth is disabled, exceeds its limit or fails, return a clear capacity/I/O
error. Preserve existing uncertain-outcome rules if logical publication already
occurred. The protected maintenance reserve is not ordinary foreground capacity.
Compaction must be able to make progress before that reserve is exhausted.

Automatic shrinking is outside this draft. Freed space is reused internally;
returning capacity to the filesystem would need a separate relocation contract.

## Maintenance, transforms and backups

Compaction captures a committed boundary, writes replacement extents outside
ordinary mutation ownership, validates intervening changes, publishes replacement
references and retires old extents after reader protection permits it. Preserve
raw stored payload bytes where the operation does not change their encoding.
Pure append history must not acquire repeated live-payload copying as a cost of
space management.

Retention and deletion keep their existing lazy semantics. Preserve ordinary
state and collection retention metadata, count retention, tombstones, permanent
positions and the maintenance scheduler's fairness across work sources.

Compression conversion remains resumable and bounded to a source unit plus
required metadata/index work. Stage converted data in free extents inside the
space, durably redirect references, then reclaim the source. Do not recreate the
previous whole-root duplication design. Account for decompression expansion and
the largest replacement unit when deciding whether free capacity is sufficient.

One live file does not make an arbitrary online file copy a consistent backup.
Capture a valid database boundary and retain its referenced extents until copying
finishes. A future backup operation may copy only allocated/reachable regions
into one backup artifact, avoiding a copy of all unused reserved capacity. This
draft does not define a backup format, archive-log service or recovery timeline.

Container filesystem permissions apply to its contents collectively. Existing
lease visibility and encryption rules must remain intact. A deployment relying
on separate Unix permissions for namespace directories needs an explicit support
decision before moving those namespaces into one file.

## Prototype and acceptance evidence

Build a vertical storage slice before changing normal Pouch open behavior:
allocation, bounded streaming append, indexed reads, independent writers,
publication, retirement/reuse, compaction, recovery and growth. Exercise both
ordinary-state and collection paths before claiming root-wide feasibility.
The prototype must not introduce a permanent alternate production backend.

Compare against the current segmented implementation using the same hardware,
input, compression/encryption, durability and concurrency settings. Retain the
transform-equivalent raw JSONL comparison as a reference, not a release gate.
Measure throughput, CPU per byte/operation, bytes written, syscall and sync counts,
open/recovery time, inode use, allocation fragmentation, working memory and
p95/p99 operation latency. Include small entries, large bounded-chunk streams,
many small collections, pure append, deletion/retention churn and slow producers.

Include temporary-file I/O in operation timings, bytes written and syscall
counts. Report peak scratch-space use separately from container capacity and
working memory, along with peak temporary-file count. A small preallocated
container must not conceal additional disk-space or inode requirements from
spooled operations.

Establish measurement noise before acceptance. Require preserved or improved
throughput and latency outside that noise, bounded metadata overhead and a
material reduction in inode/metadata lifecycle costs. Report any competing result
for a maintainer decision; an aggregate throughput win cannot hide a material
durable-commit or concurrent-reader regression. No performance result is claimed
by this draft.

Use small files, small allocation units and deterministic barriers to force the
interesting boundaries quickly. A roughly 1 MiB streamed value with tiny input
chunks can prove chunking and allocation ceilings; testing a terabyte value is
unnecessary. Include mixed state/collection churn and multiple live processes.
Cover at least:

- simultaneous reservation, growth and publication without overlapping ownership;
- slow/failed producers and cancellation before and after logical publication;
- a reader held across compaction, deletion and attempted reuse, including a
  reader in another process and recovery after a writer/reader process dies;
- torn publication/checkpoint records, failed writes/syncs and interruption at
  each allocation, retirement and growth boundary;
- deterministic loss/reordering of unsynced writes, especially NoSync retirement
  followed by reuse; a process kill alone does not simulate power loss;
- full capacity, fragmented free space, exhausted growth limits and failed growth,
  while preserving the maintenance reserve and prior durable contents;
- plaintext, compression, encryption and their combination, including retention
  aliases, incremental conversion and independent large-entry extents;
- existing leases, atomic edits, append/edit idempotency, XA decisions and outbox
  recovery across checkpoint/restart;
- scratch-file failure and loss, including process death during a spooled
  operation, without loss of committed data or a recovery dependency on scratch;
- very large reserved but mostly unused capacity without proportional startup
  scanning or memory allocation;
- the supported integer widths, musl default 128 KiB worker stacks and portable
  locking/preallocation behavior, without VLAs or large stack workspaces.

Implement coherent prototype chunks and use focused Debug tests during iteration.
Run the supported cross-target matrix once the design and implementation have
stabilized. Reuse existing behavior suites where possible rather than duplicating
every public API test for a physical-layout change.

## Decisions before production implementation

The prototype must settle allocation granularity/directory layout, metadata
journal/checkpoint ordering, portable multi-process locking and reader pins,
fragmentation behavior, growth policy/defaults and actual performance. Those are
unproven design decisions, not implementation details already resolved here.

Decide how existing file-per-segment roots are deployed or converted, including
temporary space and interruption handling, before authorizing a production
cutover. This draft introduces no automatic migration, compatibility shim,
parallel runtime or format-version increment. Storage compatibility and any
public C ABI change must be assessed independently against the released contract.

## References

- [Current Pouch storage contract](pouch-storage.md)
- [Collection streaming and recovery](collections.md)
- [Resumable compression conversion](pouch-compression.md)
- [SQLite file format: allocation and free pages](https://www.sqlite.org/fileformat.html)
- [SQLite WAL: publication, readers and checkpointing](https://www.sqlite.org/wal.html)
- [Linux file synchronization semantics](https://man7.org/linux/man-pages/man2/fsync.2.html)
- [Preallocation semantics and portability caveats](https://man7.org/linux/man-pages/man3/posix_fallocate.3.html)

External designs inform the investigation; they do not establish Pouch's
performance or prescribe SQLite's writer-concurrency model.
