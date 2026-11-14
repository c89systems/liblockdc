# Native container implementation contract

The full native x86_64 backend is authorized for staged implementation on
`experiment/pouch-space`. Normal Pouch still uses the multifile backend until
integration and acceptance are complete. The measured foundation remains the
comparison reference, not a second permanent production backend. Existing-root
conversion is outside this implementation; production deployment still requires
an explicit decision. Darwin, QEMU, 250 MiB and larger measurements remain
deferred. Initial capacity stays 10 MiB, with explicit 1 MiB growth tests.

## Physical ownership and directory

Physical pages are 4 KiB. Logical records may share pages through append-owned
runs; publishing one record never makes another writer's unfinished bytes
visible. A slow producer owns private reservations, not allocator or root locks.
Large streams use bounded buffers and acquire further runs as input arrives.
Canonical Pouch records retain their logical authority and transform rules.
Document bodies are not cached or duplicated in a second full-payload WAL.

Use a copy-on-write B+tree for the typed checkpoint directory. Its keys identify
record kind, logical owner and numeric identity. Values identify immutable
metadata records by checked offset, span and incarnation. Logical namespace and
segment catalogs map public names onto those identities. This is a typed storage
boundary, not an emulation of filesystem pathnames.

Each directory page encodes its container identity, physical offset, generation,
level and count with a CRC32 over the complete page. Parent references bind both
offset and generation. Explicit little-endian fixed-width scalars are used;
native struct layout and pointers never reach disk. A page contains up to 63
slots. Non-root nodes retain at least 31 slots through split, redistribution
and merge. Point lookup and replacement follow a bounded tree path; depth is
validated before descent. Page workspaces live on the heap. Metadata memory
follows visited pages and changed paths, never reserved container capacity.

Checkpoint directory work copies only changed paths and metadata records. An
unchanged subtree retains its immutable physical pages. A fixed journal window
must not cause a full live-directory rewrite at each rollover. Directory flush
produces private pages and reports separate allocated and retired ownership
sets. It neither publishes a root nor synchronizes or reclaims disk pages.
Open a fresh directory owner for each checkpoint's changed set; close recovery
walk views after rebuilding ownership. This keeps checkpoint caches and cleanup
work tied to that checkpoint's participation.

The container coordinator must order publication as follows:

1. Reserve sufficient private metadata and payload capacity.
2. Complete payload/descriptor writes and append checksummed publication records.
3. Build changed directory pages and synchronize their dependencies.
4. Publish an independently checksummed alternate root and synchronize it.
5. Invalidate the old recoverable root and synchronize that retirement.
6. Reclaim unpinned retired directory pages and payload runs.

An aborted directory build may release its new pages only before root
publication. After uncertain root publication, recovery determines ownership.
Unchanged pages shared by old and new roots never enter the retirement set.
Active directory views also retain their page ownership until they quiesce.
Recovery explicitly walks verified reachable pages and typed entries to rebuild
ownership. Read callbacks do not implicitly mutate the allocator. Ownership
walks require an unmodified directory view.
Journal verification dependencies also remain owned across reopen, including
superseded bodies, until a durable checkpoint retires that journal history.
Corruption of the selected root's directory is an error; recovery cannot fall
back to an obsolete root whose pages may already have been reused.

Reserve enough capacity for the next checkpoint's metadata, journal progress,
and bounded maintenance replacement work before admitting foreground payload
allocation. Full capacity must not prevent deletion, durable retirement or
recovery. Publication never exposes backing capacity before growth commits.
The experimental container now uses the growable directory and packed immutable
descriptor pages. Its roots bind directory offset/generation and the reserved
incarnation limit; retired metadata pages are released after root retirement.
It reserves 16 KiB of payload capacity and a conservative full-directory allowance
before foreground admission. Tests cover 12,000 entries in 10 MiB, incremental
checkpoint I/O, spanning descriptors, private-build allocation failures and
torn/missing writes at multi-page checkpoint boundaries. Small payload tails now
share independently reserved 64-byte cells within 4 KiB pages, with bounded
free-span selection and ownership reconstructed from all replay dependencies.
Tests cover fragmented spans, every cell boundary, pinned retirement, independent
publication/checkpoint during a held neighbouring write, malformed ownership,
and each publication I/O/heap failure, including mixed whole-run/cell cleanup.
Canonical append-owned streams are now implemented in the experimental target.
An immutable identity header binds each whole-page grant to its stream and
logical base; a typed head checkpoints its tail and grant directory. Appending
inside an existing grant adds no container journal entry. The existing canonical
record owner supplies validation and recovery, including PENDING-header fixups;
the physical boundary does not parse JSON or introduce another document WAL.
Read handles snapshot committed ranges, while append/patch and physical mapping
use bounded buffers outside the allocator mutex.

Reused, reopened and grown free intervals carry no zero certificate. Before a
grant becomes recoverable, initialize and sync at most 1 MiB of private space;
only then return its unused zeroed spare pages to the allocator. Preparation
uses the same caller-led readiness coordinator as commits and may share a
barrier. Linux zero-range allocation preserves backing reservation; bounded
offset writes provide the unsupported-filesystem fallback. Recovery clears the
complete rejected suffix and syncs before admitting appenders. Tests cover
persisted grant metadata ahead of body writes, held preparation concurrent with
unrelated publication/checkpoint, all preparation/recovery I/O and allocation
failures, and pre-existing bytes beyond the committed capacity during growth.

Stream sealing and atomic retirement are now implemented. Seal freezes the
canonical tail in an 80-byte structural event. A prerequisite barrier stabilizes
unsynced canonical bytes before that event can recover; an already durable tail
needs only the marker barrier. Reopening a sealed stream validates its owned-run
headers without candidate-tail recovery or suffix repair. Canonical projections
can read the committed prefix through the ordinary bounded reader.

Retirement emits one 64-byte head-removal event and retires every grant belonging
to that incarnation. It adds no per-run publication records or allocations.
Both transitions require an idle appender and wait for durability even in NoSync
mode. Existing readers retain their pins; retirement rejects new read admission
after success. Physical reuse still waits for checkpoint root retirement and
pins to drain. The same typed key may then bind a fresh incarnation; old refs
cannot resolve it. Tests cover both durability modes, each transition/checkpoint
fault, malformed checksummed events, held barriers and full-journal rebinding.

These streams do not yet implement public collection positions, receipts or
shared-root ownership. Metadata repacking and production integration also remain
to be implemented. See the
[stream design](../experiments/pouch_space/stream.h) and
[measured scope and regressions](pouch-space-experiment.md).

## Typed name catalogs

The internal catalog boundary now maps `(owner, surface, exact name bytes)` to
a stable binding identity, revision and live stream incarnation. Surfaces cover
namespaces, collections, segments, snapshots, projections and control metadata;
their public Pouch behavior still belongs to the pending adapter. Empty and
binary names are accepted without pathname interpretation. A dynamic hash index
compares full names; its memory follows metadata participation, not file capacity.

Create is conditional on absence; replacement and removal require the exact
current revision and container identity. Replacement retains the binding ID;
recreation after removal allocates a fresh ID. Owners, bindings and revisions
share the durably reserved incarnation allocator without a per-ID journal write.
Binding records occupy the existing metadata journal/checkpoint pages, contain
no payload extents and require no second WAL. They use 104 fixed bytes plus the
name, bounded by the 128 KiB journal record limit.

Catalog changes always wait for structural durability, including NoSync stores.
A concurrent change to the same name returns busy; independent names may share
the existing commit barrier. Get and enumeration capture one completed durable
prefix. Views pin both the binding and target until actual stream/read handles
are admitted. Stream retirement rejects live bindings, catalog pins and releases
whose durability is pending. Recovery rebuilds the name index and live target
references, rejecting stale targets and conflicting name identities.

Enumeration captures metadata under the allocator mutex and invokes callbacks
outside it, permitting checkpointing and catalog changes. Unbound closed stream
enumeration exposes orphan candidates without reading payloads or implicitly
collecting them. A caller must revalidate each candidate before retirement;
concurrent publication can bind it after capture. A deterministic test proves
that commit completion during snapshot allocation cannot change the captured
prefix or overrun the counted vector.

## Integration and acceptance

Complete storage integration covers states, objects, attachments, leases,
collections and permanent positions, retry receipts, catalogs, indexes, codecs,
queues/outbox and XA authority. Preserve public semantics through the existing
behavior suites. Remove filesystem assumptions at their owning typed boundary;
do not create hidden persistent sidecars. Explicitly accepted disposable scratch
retains bounded ownership, cleanup and measured I/O, including process death.

Maintenance includes compaction, retention and bounded resumable codec conversion.
Shared-root coordination must protect publication and reader generations across
process death; an exclusive whole-file open alone does not implement it.

Each integrated stage requires deterministic capacity, allocation, I/O-failure,
torn-write and concurrency proof; native glibc/musl and sanitizer checks; and
matched performance comparisons. Preserve grouped durability and test unrelated
dirty-writer interference. Measure single-writer sync, warm/cold reads, concurrent
scans, append and deletion churn, checkpoint/growth latency and scratch footprint.
Report material regressions before production cutover. Native integration and
these acceptance gates remain required even after individual components pass.
