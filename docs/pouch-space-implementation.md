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
a stable binding identity, revision and, for stream bindings, live stream
incarnation. Surfaces cover
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
prefix. Views pin the binding and any target until actual stream/read handles
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

## Bounded canonical controls

Collection heads now share one production codec between the native file owner
and the container's typed control adapter. The existing 384-byte representation
retains segment/snapshot facts, retention, dropped state and lease fencing.
Decode validates before replacing the caller's value. The adapter stores these
bytes directly in packed immutable cells; it creates no head file or second log.

The new `PS_CONTROL` kind supports values up to 4 KiB. Publication and deletion
always wait for the existing caller-led durability coordinator, including NoSync
roots; reads select its completed prefix. Independent control writers can publish
while another waits for sync and share subsequent barriers. Metadata authority
still belongs to the caller. Referenced canonical dependencies must be stabilized
before publishing a head that can recover independently; encoding numeric IDs
does not establish those cross-stream bindings.

Existing control replacements may use one 4 KiB page of the protected 16 KiB
maintenance allowance. New controls retain ordinary foreground admission limits.
Checkpoint admission preserves metadata shadow capacity without reserving that
same maintenance payload twice. It reclaims superseded controls after root
retirement and reader pins drain. Tests prove repeated replacement/checkpoint
after foreground exhaustion, without growth or shrinking.

This completes control encoding and physical persistence, not public collection
ownership. Public lease operations still exercise the native owner. Namespace
data manifests, dependency ordering, guard lifetime and control cleanup must be
joined to the container before the public Pouch cutover.

## Namespace segment ownership

`ps_namespace` now caches its canonical segment owner under the real root.
The namespace has no segment/head until its first append or explicit rotation.
A 128-byte `PS_CONTROL` head identifies the namespace owner, active segment,
rotation epoch and canonical record width, with checked reserved bytes and CRC.
Typed segment catalog entries hold the growing segment set and exact stream
incarnations. Numeric segment labels remain canonical metadata, never files.
No growing manifest value or per-record head publication is introduced.

Rotation prepares an empty successor and commits its catalog binding, durably
publishes the head cutover, then seals the predecessor. A caller-supplied
expected active segment fences retries: an already selected successor succeeds
without another rotation, including after reopen. Recovery recognizes a staged
empty successor and a cutover whose predecessor still needs sealing. It completes
that seal before admitting another producer. A nonempty unpublished successor,
missing segment, wrong binding or sealed active segment is a protocol error.
Uncertain physical I/O retains the container's poison/reopen rule.

One producer/rotation owns a namespace at a time; contention is explicit. Producer
callbacks hold neither registry nor namespace mutexes. Other namespaces and
captured readers continue independently. The cached committed tail is updated
only after successful publication under that mutation authority; it avoids a
duplicate physical status query before each record. Versions, query sequences,
preconditions and resident projections remain the logical mutation owner's job.

Captured views retain exact segment identities and tails with lightweight stream
windows. Buffered cursors use the same admission and release code. Windows keep
old ranges readable across append, seal, retirement and checkpoint without a
16 KiB buffer per captured segment. Replay shares one bounded readahead buffer
and optional CRC scratch across segments. Callbacks run outside locks, may
append/rotate, and cannot extend the captured prefix. Callback failure leaves
the view reusable. Namespace/window/append pins protect root lifetime. Root close
releases cached segment owners without an implicit NoSync barrier; after a raw
child rejects close, the cached owners can be reconstructed.

This is segment storage and canonical replay, not complete public namespace
policy. Snapshot installation, obsolete ownership/reclamation, public collection
ownership and process-shared coordination remain required. Normal
`lc_pouch_open` still opens multifile roots; there is no deployment cutover.

## Production state integration

A private `ps_pouch_state_open` harness now connects ordinary production state
PUT, point read, metadata read, copy and namespace creation to the container.
Common Pouch runtime initialization owns options, logging and coordination;
the storage owner supplies typed namespace admission, canonical replay, private
append/commit and bounded encoded sources. It supplies no pathname operations.
Native `lc_pouch_open` remains multifile until full integration and acceptance;
there is no SDK backend selector.

The existing state engine retains preconditions, generations, index sequence
allocation, metadata inheritance, retention publication, SHA-256, codecs and
projection application. Timestamp, retention and sequence publication remain
after the streaming producer. Caller-owned small memory sources retain the
existing bounded memory transform optimization. Other producers flow directly
through bounded codec/append buffers; container reads populate no body cache.

Replay uses the production projection application rules, with no manufactured
namespace path. Failed replay clears the partial projection before retry.
Hot operations check physical poison. One appender per namespace retains the
existing logical append authority; independent namespaces share the physical
coordinator. Producer callbacks and durable commit waits release the projection
mutex while exact-key and append authority remain held. Competing stale
preconditions fail before consuming their source.

Encoded read windows retain the namespace and storage owner. Their decoded
sources retain their original body and crypto binding across replacement,
rotation and Pouch close. The last source releases exclusive container ownership.
Crypto outputs use the logical owner's allocator; default-allocated key IDs
retain their original allocation domain. Shared append completion cleanup also
releases reserved metadata and precomputed transforms after failed reservation
or finalization.

This harness explicitly requires disabled query indexing, background maintenance
and queue watches, exclusive root ownership and no generated external key file.
State PUT staged/lease publication requests and staged decision replay return
unsupported. Other Pouch APIs are outside this harness's current call contract.
Metadata-only mutation/delete, read-many/scans, collections, queues, XA, indexes,
snapshot/obsolete ownership, maintenance and shared-root operation remain
integration work. This is a verified ordinary-state slice, not full backend
acceptance or deployment authorization.

## Integration and acceptance

The experimental root owner now owns the physical container, crypto provider and
namespace registry. A 128-byte `PS_CONTROL` value binds encryption mode, root key
identity, compression policy and canonical record/head widths. Creation commits
it durably; reopen rejects missing/corrupt policy or a mismatched key/mode before
namespace admission. Implicit compression inherits the stored policy, allocating
a plaintext transform provider when necessary. An explicit compression change
currently returns unsupported; resumable container conversion remains required.

Namespaces now own their catalog metadata directly. `ps_catalog_create_namespace`
creates an immutable global namespace owner with `id == revision.incarnation`,
zero stream/target fields and one durable publication. The owning record has no
payload stream or extents. Name removal/recreation allocates a fresh owner;
pinned catalog views survive checkpoint and recreation. Ordinary stream bindings
retain their target references and retirement guards. Root namespace names are
nonempty C strings with exact bytes, without pathname interpretation.

A dynamic registry caches one namespace owner per name. Same-name initialization
waits for that owner; unrelated names publish outside the registry mutex and can
share physical barriers. A failed initializer releases its pending ownership so
waiting callers can retry. Namespace pins, active calls and physical child
handles reject root close without destroying the root. Enumeration captures one
durable prefix and invokes callbacks outside locks. Cached admission checks the
physical poison state without allocation or I/O; an uncertain write cannot be
bypassed through a cached namespace.

The stream comparison now opens through this root owner and obtains its actual
namespace IDs and crypto provider. Stream catalogs are scoped to those IDs.
Namespace heads, segment ownership/rotation and bounded canonical replay now use
this owner too. The private ordinary-state harness now uses the production
projection and PUT/read policy. Remaining state mutation/scan paths,
snapshot/obsolete sets and public collection ownership still require
integration. Normal `lc_pouch_open` uses
multifile roots. The experimental owner retains exclusive physical open
ownership; it does not implement shared-root coordination.

The production canonical record I/O boundary is now shared with the container
experiment. `lc_pouch_record_io` borrows an owner and provides exact vectored
reads, append, private-range patching, tail observation and discard. Canonical
prefix/finalization and vectored writes in normal Pouch use this boundary;
collection writers and decoders accept it directly. Existing segmented owners
use the native file adapter, retaining descriptor authority/cursor rules and
their previous cleanup policy. There is no backend-selection flag or public ABI
change. The canonical wire format is unchanged.

The experimental bridge owns one private stream append, checks patch bounds,
and admits reads through an already pinned window. Offset reads preserve that
window across append/retirement without changing the sequential cursor. Recovery
uses a separate input capability restricted to the canonical recovery callback.
Finalizing the canonical header does not publish the stream tail or synchronize
it; the owner must commit through the existing container readiness coordinator.
Discard clears the private suffix through the stream's existing stabilization
protocol and poisons storage if cleanup fails.

The same production collection writer now validates JSON, hashes compact bytes,
encodes transforms, reserves/finalizes real metadata and writes canonical spans
inside the container. Its decoder and reusable crypto reader consume those
bytes directly, without a payload spool or another WAL. Tests cover all four
transform combinations, both durability modes, output before EOF, failed
producers, every canonical callback failure before/after I/O, pinned windows,
checkpoint/reopen and retirement. The comparison uses this real collection
codec. The ordinary state variant now uses complete production metadata too.

`lc_pouch_state_record_read` is the shared bounded canonical reader for native
segmented replay and container streams. Native replay uses borrowed stdio I/O
to preserve readahead. It validates captured bounds before allocating, retains
the production metadata/control dispatch, optionally checks stored payload CRC
with reusable 16 KiB scratch, and replaces a decoded entry only after success.
Incomplete/PENDING tails are distinct from corruption and transport errors.

The ordinary container byte writer uses the existing state metadata encoders,
SHA-256 and bounded pull encoder with an encoded-byte sink. It supports state
and object PUTs with all four codec combinations and compression override.
Metadata/result allocations finish before PENDING is cleared; failure discards
the owner's private append. It owns neither namespace mutation policy nor
version/index allocation, publication or sync. Logical segment labels remain
metadata; no file is created for a label. Native Pouch's mutation authority and
write sequencing are unchanged. The shared hash source rejects oversized
producer returns before hashing.

This is partial integration, not root-wide adoption. Namespace data and collection
owners, ordinary namespace mutation/replay policy, metadata mutation/copy paths,
control ownership, position/receipt authority, projections and public root lifetime still
need conversion to container ownership. Normal public Pouch tests continue to
exercise the segmented backend through the shared record I/O boundary.

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
