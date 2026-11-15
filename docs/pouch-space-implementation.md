# Native container implementation contract

The full native x86_64 backend is authorized for staged implementation on
`experiment/pouch-space`. Normal Pouch still uses the multifile backend until
integration and acceptance are complete. The measured foundation remains the
comparison reference, not a second permanent production backend. Existing-root
conversion is outside this implementation; production deployment still requires
an explicit decision. Darwin, QEMU, 250 MiB and larger measurements remain
deferred. Initial capacity stays 10 MiB, with explicit 1 MiB growth tests.

## Compound append recovery

Staged state integration exposed a recovery gap in the experimental
container. A deterministic process-death probe wrote the first complete LINK
of a promotion into an existing active segment, then exits before writing its
DECISION and staged tombstone. Unframed recovery accepted that complete LINK beyond the
stream's previous published tail. It exposed public generation 2 while the
generation-2 stage remains live. The retained failing probe is
`build/pouch-space-staged/active-batch-crash.log`; its diagnostic mode is
`staging-crash` in the private Pouch test executable. It exits during the write,
before append cleanup. This is process-death evidence, not a power-loss model.

Known-size reservation and one append commit do not by themselves make a
compound canonical batch an indivisible recovery unit. Selecting a new segment
protects an unpublished successor, but recovery of an already selected active
segment accepts complete individual records in its candidate suffix. Earlier
allocation/I/O/partial-write sweeps exercised orderly failure cleanup and
rotation; their passes do not establish crash atomicity for this active case.
The checksummed framing implementation below fixes this failure and completes
the private harness's staged mutation/recovery slice. Normal public Pouch still
uses its existing multifile owner.

The approved correction is implemented as **checksummed framing for complete canonical
batches**. Preserve the existing logical records inside one container-owned
frame. Its header binds the exact encoded batch length and a CRC32 over every
inner canonical byte. Recover and validate the complete frame before admitting
any inner record or advancing the accepted tail. An incomplete or damaged
candidate frame contributes no records; damage within the selected published
prefix remains a hard corruption error. A checkpoint must not split a frame.
Projection replay still receives ordinary canonical entries with their actual
inner offsets, existing index sequences, lifetimes and body references.

Apply this contract to typed COMPLETE and MAINTENANCE batches. Keep the existing
bounded streaming PUT path and its payload checks unchanged. Add the framing
bytes to the complete append's reservation and rotation calculation before any
write. Finalize indices/lifetimes before computing the checksum. Walk the
already encoded metadata vectors to compute it: no additional body copy, whole
batch concatenation, spool, sidecar, writer queue or durable barrier is needed.
The frame header reuses the existing fixed-width record-header encoding, with
private type 9. No public Pouch record type or SDK backend selector is introduced.
The overhead is one 32-byte header per complete batch plus checksum CPU and
recovery validation. The normal metadata path accumulates CRC over borrowed
vectors and finalizes the header in the existing stream buffer. A known-size
canonical producer that patches private bytes uses a bounded CRC reread instead;
when the header has already flushed, its final patch is an additional write.
Ordinary unknown-size streaming PUTs remain unframed.

This changes the experimental segment representation. It does not change the
multifile representation or public API/ABI. It carries no existing-fixture
compatibility or migration promise; recreate experimental fixtures, retain the
current independent format marker, and add no legacy parser. This persistence
change is approved for the experimental implementation and verified below.
It is separate from the standing
maintenance-capacity decision below.

The acceptance checks prove promotion, discard and staged-delete
groups on both an active segment and rotation. Cut process execution across
frame header/body/publication writes, including each inner-record boundary;
model loss of unsynced writes separately. Recovery may select the old state or
the complete new group, never an inner prefix. Cover both sync policies, codecs,
allocation/I/O/torn writes, recovery retry, pinned readers, later ordinary
appends, snapshots and compaction. Durable acknowledgement must cover the
complete group. Verify unchanged barrier counts, independent producers, and
measure framing/checksum cost against both the frozen container checkpoint and
the current multifile implementation before claiming the staged path complete.

The framing implementation now passes the focused ASan/UBSan checks with
128 KiB glibc worker stacks. The frame codec/writer/recovery tests include
bounded reads, allocation/I/O failures, canonical fixups beyond the stream
buffer, captured old views, invalid candidate checksums, a malformed final
inner record with a valid outer CRC, checkpoint splits, and corruption inside
the committed prefix. Rejected candidate batches admit no inner records and
can be retried before a successful cold readback.

The staged suite passes 23 groups in 1784.34 s, alongside the separately passed
plain NoSync active-segment partition. It covers all four codecs, both root
policies and active/rotated batches, with over 6,000 process-death/lost-write
cases and 3,992 allocation/I/O/partial-write failure sites. The crash cases cut
every physical write/sync/reserve operation before/after/partway, and explicitly
cut each frame header and inner-record boundary. Each case checks the real
file and an independent successful-sync image, retries old state, compacts and
reopens. Durable acknowledgement requires the full group in both images.
The allocation/I/O sweeps use plain and combined encryption/compression;
functional and crash coverage exercise all four codec combinations.
The passing logs are `build/pouch-space-batch-frame/debug-partition-probe.log`,
`debug-staging.log` and `debug-staging-details.log` in that directory. The
initial aggregate crash run reached its last configuration without an
assertion failure but exceeded 600 s; the retained `debug-crash-matrix.log`
records that timeout. Splitting by codec/policy/rotation preserves its coverage
and extends it to every codec, with a 240 s limit per partition.

All **400 Debug groups**, including **348 unit groups**, pass across the focused
and remaining standard runs. The remaining 373 groups take 1616.18 s; their
summary/details are `debug-standard-remaining.log`/`debug-standard-details.log`
in the same stage. Native GNU/musl each pass 24 affected groups in
86.86/77.34 s, including two crash partitions and production staged-recovery
regressions; summaries/details are `gnu.log`/`gnu-details.log` and
`musl.log`/`musl-details.log`.

ThreadSanitizer with the checked fully instrumented OpenSSL archives passes
80 four-writer comparisons and 16 focused groups, without suppressions. The
first run passed the 80 comparisons, then found a race in the new frame test's
assertion counter. That test-only counter now uses a relaxed atomic, matching
the other harnesses. The isolated failing test, the remaining focused race
checks and all three frame groups under Debug/GNU/musl pass after that repair.
The retained logs are `tsan.log`, `tsan/frame-codec-fixed.log`,
`tsan-focused.log` and `{debug,gnu,musl}-counter.log`. Five Memcheck runs cover
frame codec/writer/recovery and staged lifecycle/recovery: 235,145 allocations
and frees, zero live allocations, errors or suppressions (`memcheck.log` and
`memcheck/`).

The [600-trial matched comparison](pouch-space-experiment.md#checksummed-batch-framing)
retains all medians, ranges and counters. Measured small complete batches add
exactly 32 syscall bytes each, with no extra write calls or single-writer
barriers. Timing medians are mixed, and all 40 before/after primary ranges
overlap; this establishes the observed branch comparison rather than an
isolated checksum latency estimate.

The private harness now supports staged PUT/prepared mutations, commit,
promotion, discard and cold decision recovery for state/object records. Typed
decision recovery uses the resident key index and a hash table, allocating
decision entries only for still-live participants older than the decision.
Historical completed attempts cannot retire a recreated stage. Ordinary
compaction resolves the captured prefix and permits settled staged history,
but rejects any remaining live stage before planning or publication. Live
staged maintenance, other owners, protected maintenance admission at capacity,
shared-root integration and production cutover remain outside this completed
slice.

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
It preserves 16 KiB of payload headroom and a checkpoint-only free-page pool
before foreground admission. If F is the conservative complete directory and
packed-descriptor bound, and A is allocated metadata bytes, the pool retains at
least `max(F, 2F - A)` bytes. Payloads cannot allocate these pages. Recovery
rebuilds this volatile reservation from the free-space complement inside the
same file. With spare common capacity, checkpoints stay incremental. Near
capacity, a dense metadata rewrite and old-root retirement restore enough free
workspace for the next checkpoint. Excess dynamic workspace returns to common
allocation; the fixed 128 KiB region remains protected.
The experimental private budget boundary now claims payload pages, positive
descriptor growth and additional live entries before dependent publication.
Checkpoint bounds include outstanding future credits; private writers retain
descriptor credits until publication, while physical allocation spends payload
credits. Ordinary admission and checkpoint top-ups cannot consume those claims.
The maintenance floor applies at admission, so funded work remains possible when
a peer subsequently claims that floor. Handles pin their space and active writers
pin their budget. Claim/release uses no I/O or persisted format; it neither
isolates keys nor rolls back publications. Explicit namespace rotation now
reserves its successor, binding and routing replacement before staging.
Canonical grants and explicit stream/record reservation now spend claims. Grant
preparation pins the budget and retains future descriptor credits until actual
installation. Extra zero preparation uses only unclaimed headroom; cached streams
do not retain the reservation's budget. Typed scalar planners compose head,
binding, routing and worst-fragmentation exact-span costs. Known-size body packs
admit head, grants and final catalog binding before stream creation, preserving
the standing floor through an ordinary claim. Namespace appends now plan their
known span, possible successor/binding and head selection before staging, then
hold the claim through production and publication. Complete metadata batches
preserve the floor; complete delete-only batches may spend it. Unknown-size PUTs
claim their header/key prefix and grow further through ordinary admission.
Snapshot planning now measures both outputs before rotation or stream creation.
One maintenance claim funds rotation, snapshot/body heads, grants, bindings and
final head selection. The builder owns both outputs and closes them before
releasing the claim. Checkpoint workspace funds seal and reclamation progress.
A bounded-work policy must also cover accepted keys up to 1 MiB;
the fixed 16 KiB floor alone cannot establish that guarantee.
Tests cover 12,000 entries in 10 MiB, incremental
checkpoint I/O, spanning descriptors, private-build allocation failures and
torn/missing writes at multi-page checkpoint boundaries. Small payload tails now
share independently reserved 64-byte cells within 4 KiB pages, with bounded
free-span selection and ownership reconstructed from all replay dependencies.
Tests cover fragmented spans, every cell boundary, pinned retirement, independent
publication/checkpoint during a held neighbouring write, malformed ownership,
and each publication I/O/heap failure, including mixed whole-run/cell cleanup.

Explicit `ps_metadata_repack` promotes current descriptors into the ordinary
checkpoint's dirty ownership set and rewrites them densely through the same
directory and root-retirement protocol. It does not copy payloads, relocate
stream mappings or change binding identities. Existing body, stream and catalog
pins remain valid. Spare-capacity checkpoints still rewrite only changed records
and paths. A clean checkpoint writes no new directory/descriptor pages and can
release the excess forecast of an aborted private writer. A healthy failed
repack retains its dirty plan for a later checkpoint or repack retry.

Growth reserves backing first, then admits the new range privately under the
allocator mutex so it can fund its own metadata checkpoint. Capacity and new
directory pages publish in one root. Healthy failure aborts private pages,
returns growth intervals from both pools and trims excess old-capacity
workspace before excluding the new range. Preallocated rollback/trim nodes
preserve the old capacity and payload headroom without a new allocation;
uncertain structural I/O still requires reopen/reconciliation. Tests cover
fragmented near-full metadata, repeated 1 MiB growth, every growth/repack heap
and I/O failure, held pins and concurrent durable publications. Whole-operation
maintenance admission remains required; explicit repacking and successful
growth do not establish that guarantee for a fixed-capacity full store.

Immutable replacement forecasts account for the replaced descriptor, and catalog
replacement retains its existing name/entry count. A prepared grant rechecks
descriptor admission after I/O releases allocator authority. Tests hold that I/O
while a peer grows metadata and exhausts payload admission: the pending grant
fails before installation, then succeeds after checkpoint/reclamation without
growth. Removing that final recheck makes the test fail. Existing value refs and
captured catalog views remain valid throughout.

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

Unsealed stream open validates the canonical prefix and admits readers without
clearing unused capacity. Newly recovered complete bytes join root readiness;
an explicit barrier, peer publication or checkpoint can stabilize them before
suffix repair. First write admission clears the complete rejected suffix and
waits for a readiness unit registered after that clearing completes. Independent
repairs can share the existing coordinator's barrier. The writer's existing
heap buffer supplies bounded fallback zero writes. Failed repair returns no
writer and retains uncertain-I/O poison semantics. Sealing and retirement keep
their existing prefix/dependency barriers without clearing unused capacity.
Physical root recovery still performs its own journal/incarnation work.

The [reopen measurements](pouch-space-experiment.md#deferred-stream-repair-and-reopen-readiness)
show approximately three times faster four-namespace readiness in the small
native fixture. Twenty deterministic recovery/concurrency variants and the
extended first-write fault sweep prove the admission boundary. The complete
362-check Debug preset passes, including 310 unit groups; GNU/musl each pass
73 affected groups, instrumented-dependency ThreadSanitizer passes 80 comparisons
and eleven groups, and four Memcheck runs have zero live blocks/errors.

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
shared-root ownership. Complete production integration remains ahead. See the
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

`PS_ROUTING` has the same one-page bound and replacement allowance, but its
publication, reads and deletion follow root durability. It holds logical routing
decisions that may roll back with NoSync canonical data. Allocation/ownership
authority remains in always-durable controls, catalogs and grants. `ps_flush`
completes the ready publication prefix through the existing sync coordinator,
without a checkpoint or another sync for an already covered prefix.

This completes control encoding and physical persistence, not public collection
ownership. Public lease operations still exercise the native owner. Namespace
data manifests, dependency ordering, guard lifetime and control cleanup must be
joined to the container before the public Pouch cutover.

## Namespace segment ownership

`ps_namespace` now caches its canonical segment owner under the real root.
The namespace has no segment/head until its first append or explicit rotation.
A 128-byte `PS_ROUTING` head identifies the namespace owner, active segment,
rotation epoch, canonical record width, selected snapshot and covered segment
boundary, with checked reserved bytes and CRC.
Typed segment catalog entries hold the growing segment set and exact stream
incarnations. Numeric segment labels remain canonical metadata, never files.
No growing manifest value or per-record head publication is introduced.

Rotation prepares an empty successor and commits its catalog binding. Automatic
append rotation reserves its known private span, finishes its canonical bytes,
then publishes head cutover before making the new stream tail visible.
The head follows root durability: durable publication covers the canonical
bytes in the same barrier; NoSync does not force a whole large value to disk.
Predecessor sealing is deferred until the next mutation, which first flushes
the routing decision. No fallible seal follows successful append publication.
The private append boundary distinguishes a known prefix, a complete positive
span and a complete maintenance span. Metadata batches declare their exact
canonical size; only all-delete batches request maintenance admission. A
streaming PUT declares its known header/key prefix and acquires further body
capacity as input arrives. Zero minimum in prefix mode reserves one byte of
logical capacity. Complete modes reject oversized vectored writes before any
part is written, reject commit after an overrun attempt and require the declared
byte count before publication. An underfilled append can be completed and retried.
Explicit empty structural rotation has no append span to reserve and remains
durable under either root policy. A caller-supplied expected active segment
fences retries: an already selected successor succeeds
without another rotation, including after reopen. Recovery recognizes a staged
empty successor and a cutover whose predecessor still needs sealing. It completes
that seal before admitting another producer. A staged successor with checkpoint
tail zero is discarded even if complete private canonical bytes survived a peer
checkpoint. A published tail in an unselected successor, missing segment, wrong
binding or sealed active segment is a protocol error.
Uncertain physical I/O retains the container's poison/reopen rule.

`ps_stream_reserve` and its canonical record adapter acquire bounded zero-owned
grants without advancing a tail or emitting canonical bytes. Namespace planning
subtracts capacity already owned by the selected stream; an append needing no new
grant or head selection allocates no budget handle. Ordinary admission funds a
conservative geometric-grant envelope and falls back to exact reservation when
that envelope cannot fit. It does not retry allocation or I/O failures as capacity
failures. Maintenance uses exact reservation. A clipped grant does not reset the
stream's growth preference: the largest observed grant determines the next
preferred size, capped at 1 MiB and reconstructed on open. Scalar planning creates
no capacity-sized workspace. Valid allocation/preparation failure fails the writer,
which must be closed; staged zero grants can be reused. Failed capacity admission
precedes successor staging and preserves the selected head. Admission protects
the declared dependencies, while unknown-size continuation remains ordinary
foreground work. Complete parent snapshot/compaction admission is still required.

Logical append manifests remain private until physical publication succeeds.
Their segment labels remain stable while the namespace capability is held,
including across source-directory growth, retirement and projection invalidation.
The prototype uses ROUTING for the namespace head. Its width remains 128 bytes;
snapshot identity and covered boundary occupy previously reserved fields. There
is no legacy reader or migration.

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

This is typed source storage and canonical replay, not complete public namespace
policy. Logical compaction, public collection ownership and process-shared
coordination remain required. Normal
`lc_pouch_open` still opens multifile roots; there is no deployment cutover.

## Namespace snapshot and obsolete ownership

The namespace head stores its selected snapshot ID at byte 40 and its covered
logical segment boundary at byte 48. Both are zero or both positive; a selected
snapshot must cover a boundary strictly below the active segment. Bytes 56–119
remain reserved, with the existing CRC at byte 120. Snapshot IDs come from the
durably reserved global incarnation allocator. Their stream keys bind the build
ID as owner and namespace ID as key, preventing a foreign namespace from
adopting an otherwise valid snapshot binding. Canonical
`snapshot-<20 decimal digits>.log` labels are catalog metadata, never files.

Snapshot begin captures the selected snapshot plus current tail, then invokes a
metadata-only planner before rotation, identity allocation or output creation.
The planner declares exact metadata and optional body-pack sizes. One maintenance
claim includes any required rotation, both output heads/grants/bindings and final
routing replacement. Failed capacity admission leaves the topology unchanged.
After admission, pending seals and abandoned bindings are cleaned up, a nonempty
active segment rotates, and both outputs receive exact grants. An already empty
active segment needs no extra rotation. A selected snapshot can be rewritten at
the same covered boundary. Namespace mutation authority covers planning and
preparation, then releases before copying. One builder owns the capture and its
optional body pack; ordinary appends and other namespaces continue during output.
The caller supplies logical completeness, high-water marks, tombstones and any
required history or body references.

Commit checks both declared byte counts and failed-output flags before publishing
either output. It reacquires namespace mutation authority and checks the captured
base. The body pack publishes first, allowing its dependency barrier to stabilize
deferred routing before predecessor-seal cleanup. Metadata publication then seals
and binds the snapshot before head selection. All dependent allocations spend
the same claim. Head selection waits for durability under both root policies.
Newer appended segments remain the replay tail.
There is no fallible reclamation after successful selection. Before a higher
snapshot ID can be selected, any bindings left by earlier unpublished builds
are durably removed. This ensures that a retained lower ID identifies a
previously selected snapshot, including after interrupted publication.

Replay reads only the selected snapshot, reported with callback segment zero,
then the dense segment tail above its covered boundary. A sorted source index
supports reclaimed gaps without allocating slots for every historical numeric
ID. Older bound segments and snapshots remain available for body references.
The internal append result reports live replay-segment count separately from
active segment identity. Interned labels retain their existing namespace-owner
lifetime; closing obsolete stream owners does not invalidate a borrowed label.
Private unbound snapshot labels last only until builder close.

Explicit reclamation accepts covered segments, older snapshots and sealed body
packs. The logical caller must first prove that current records, links and
history no longer need the source; a covered boundary alone does not prove that.
Catalog removal is durable before stream retirement. New namespace read admission then
fails, while captured windows and their clones retain the old bytes. Reap
defers retirement for catalog views and closes retired owners only after read
pins drain. Physical reuse still waits for checkpoint retirement. Source
objects follow bound or pending resources; the retained label pool is separate.

Root orphan reclamation is also explicit. It captures closed, unbound stream
incarnations, rechecks ownership before retirement, and skips candidates claimed
concurrently. It opens no canonical reader and reads no bodies, so incomplete
private builds left by process death need no application-record recovery.
No persistent sidecar or second payload log is introduced.

These physical publication and reclamation capabilities now join the production
ordinary-state harness through the forced pass below. It performs logical
publication, projection invalidation and current-source reclamation. Automatic
policy, history protection, retention scheduling and standing capacity for all
admitted maintenance work remain required.
The fixed-capacity churn proof uses a caller-supplied complete one-key snapshot;
it does not establish full-store maintenance admission.

## Canonical snapshot serialization

The state codec now supplies `lc_pouch_state_record_write_snapshot` and
`lc_pouch_state_record_write_high_water` through the existing borrowed record-I/O
boundary. Native compaction and clean-close projections use this same writer.
The caller supplies resolved logical entries, selection/history policy, physical
commit and durability. A raw namespace view alone does not resolve inherited
per-key query generations; the logical producer must do that before writing.
An absent explicit query sequence defaults to the entry's index sequence.
`lc_pouch_state_record_measure_snapshot` sizes the same high-water, reference,
inline encoded-body, placeholder or tombstone records using metadata only.
Aggregate snapshot/body planning reuses it. It validates input and local offset
bounds, clears the output size on failure and performs no body I/O. Tests compare
its sizes with actual decoded boundaries across the existing transform and
object/state matrix.

With no encoded source, the writer emits a reference, metadata-only placeholder
or tombstone without reading a body. With an encoded source, it copies exactly
the declared stored span into the destination label, retaining the descriptor,
immutable crypto context, versions, timestamps, metadata and both sequences.
Caller-owned scratch bounds each chunk to at most 128 KiB; smaller buffers are
supported. The source and output advance together, without decoding,
re-encryption, payload spooling or a whole-body buffer. Empty encoded bodies
need no scratch.

Copied bodies retain PENDING until their original stored CRC has been checked
and final metadata has been patched. Source errors, short input, invalid input,
allocation failures and output failures prevent completion; an output failure
after writing begins invokes the owner's private discard boundary. No logical
snapshot is selected by this byte writer.

Object references explicitly retain the existing OBJECT_LINK flag. This also
corrects native clean-close projection writing, which previously passed that
flag as unused descriptor reserve instead of setting the payload flag.
Native reopen tests now assert that internal objects remain objects.

The ordinary-state pass below supplies current-source liveness decisions;
history protection and complete maintenance-capacity admission remain
integration work. These
helpers introduce no public ABI or persistent format/version change.

## Captured logical snapshots

`lc_pouch_state_snapshot_capture` now builds an immutable logical projection
from a caller-owned captured canonical prefix. It reuses production replay,
supersession, object classification and query-generation inheritance. It does
not consult or replace the live namespace cache. Replay uses a temporary hash
index; capture sorts the surviving keys once and releases the lookup buckets.
Memory follows current key metadata, including tombstones, without body
materialization. The Pouch runtime is borrowed only during capture; the capture
owns its metadata and allocator value. Its allocator context and any physical
source pins remain caller responsibilities.

`lc_pouch_state_snapshot_visit` emits HIGH_WATER followed by one resolved entry
per key in byte order, including tombstones and placeholders, with both index
sequences explicit. Visits allocate nothing, can run concurrently and restart
from HIGH_WATER after a visitor error. Callbacks retain no engine coordination.
This separates metadata capture and inspection from output allocation and body
selection, so maintenance can calculate complete workspace before copying.
Close releases the owned projection independently of runtime lifetime.

The private Pouch snapshot tests now use this producer for initial and replacement
snapshots. They keep an old covered body bound when a newer META still refers
to it. Capture does not itself publish snapshots, invalidate live projections,
protect history consumers, select victims or admit complete maintenance work.
Capture now resolves staged history and decisions, then rejects remaining live
staged records until their maintenance policy is integrated. Completed staged
history can be omitted from a resolved ordinary snapshot.

This work also fixes queued metadata-entry copying to retain the prior query
sequence and its presence flag. Without that copy, a metadata update could
persist query generation zero while the live cache retained its prior public
generation. Captured canonical replay now agrees with the live projection.

## Forced ordinary-state maintenance

`ps_pouch_state_compact` now captures a prefix and measures its canonical metadata
snapshot and one selected source's body frames without body I/O, before rotation
or output creation. The snapshot planner admits the complete known publication
dependency set through one claim, then copying runs outside logical namespace
authority. Its forced victim
score is source capacity minus current live encoded-body bytes. Complete
metadata remains in each snapshot; untouched bodies retain their existing
references. The initial capture is released before the publication-time capture
so both metadata workspaces need not coexist.

Selected bodies use a separate sealed stream bound under `PS_CATALOG_BODY`
(surface 6), named `body-<20 decimal digits>.log`. The globally unique build ID
is its stream owner, and the namespace ID is its stream key. These are typed
labels inside the container, not files. Packs contain canonical body frames but
never participate in logical replay. Namespace load rejects foreign ownership,
invalid labels and unsealed packs. This extends the prototype's catalog kind
set without an external compatibility/version commitment.

Exact sealed-build reservation retains full geometric grants and clips the last
grant to the page-rounded remaining span. It advances neither canonical cursor
nor published tail; completed body/snapshot tails must equal measured sizes.
The joint claim protects payload and positive descriptor/entry growth through
body binding, metadata binding and final head selection. Both outputs reject
oversized vectored writes before any part is written. An incomplete or failed
producer is rejected before either binding publishes. A later allocation or I/O
failure can still leave bound, unselected output for recovery or reclamation.
Reclamation and checkpoint use the separately protected metadata pool.
Admission guarantees the declared capacity after it is granted; it does not
guarantee that every compaction fits when the store is
already full. The fixed 16 KiB floor still needs a standing-work policy covering
all admitted key sizes and required maintenance progress.

The pass durably binds its body pack before selecting its metadata snapshot.
Under the same logical namespace authority, it then captures the latest snapshot
plus newer tail and protects all current body sources before retirement. A
newer META can therefore retain the original copied source. Publication
invalidates the resident cache before releasing authority, including on failure.
Source removal, reap, closed-unbound orphan collection and checkpoint reuse the
existing physical owners and retain caller-held read pins. A late failure can
leave a selected new physical topology; reopen/retry preserves logical values.

Encoded sources check the stored CRC incrementally, including covered original
segments which metadata-only snapshot replay no longer visits. Mismatch
withholds the final chunk and stays an error until reset. Reset and clones start
at zero with independent checksum state; their shared immutable window and
storage owner can outlive Pouch. Copy scratch stays bounded at 128 KiB, without
body materialization or payload spooling.

This remains a private forced ordinary-state operation. History consumers,
staged/XA decisions, collections, indexes, retention scheduling, shared-root
coordination and complete protected capacity remain integration work. Current
measurements and their observed read regression are recorded in the
[experiment results](pouch-space-experiment.md#forced-ordinary-state-compaction).

## Crypto initialization before writer admission

Runtime admission explicitly initializes libcrypto through
`OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CRYPTO_STRINGS, NULL)`. Plaintext roots
also hash their bodies; they must complete the library's first allocation
before admitting concurrent record writers. This uses OpenSSL's initialization
coordination at open. Failed initialization returns a transport error with a
null handle before physical-root creation; a healthy retry remains possible.

The dependency investigation uses the checksum-verified pinned OpenSSL 3.6.5
source, built with ThreadSanitizer in
`build/pouch-space-durable-scope/openssl-tsan/`. The prepared uninstrumented
library reported an RCU hashtable publication race that requires instrumented
dependency verification. Full instrumentation separately exposed a first-use
race on `CRYPTO_malloc`'s `allow_customize` flag. A diagnostic explicit
initialization before writers passed forty concurrent scope runs and three
focused groups without suppressions. That diagnostic uses a test constructor;
the product change initializes during runtime admission. The actual product
change, linked with the instrumented dependency and without the diagnostic
constructor, passes all forty concurrent comparisons plus four focused groups,
including initialization failure and retry. The strict runner stops on any
failure and uses no suppressions. Its log is
`build/pouch-space-durable-scope/tsan-runtime-init.log`.

## Cleanup durability scopes

The logical cleanup scope uses a typed root `flush` capability for container
storage. Scoped canonical commits select an explicit per-call `DEFER_SYNC` mode;
the mode belongs to the current thread's forced scope for that Pouch owner.
Ordinary commits retain their root policy, including durable acknowledgement
outside a scope. This adds no writer queue or global durability toggle.

Completed mutations remain individually visible before the scope barrier. An
explicit barrier and scope completion force the completed publication prefix
durable even under NoSync. Namespace inclusion retains its authority check and
checks storage health without opening namespace paths or reading bodies. The
physical readiness coordinator excludes unfinished private producers and skips
a file sync when the prefix is already covered. Structural publication and grant
preparation retain their barriers; publisher callbacks must still establish
their existing durability dependency. An ordinary peer writer can complete
while a scope stays open, and its barrier may cover that scope's completed
prefix. A scope groups durability without making its mutations atomic.

Scope completion still attempts durability after an operation error, preserves
that original error and releases its thread-local ownership on every path. A
failed sync retains the physical owner's uncertain-outcome/poison behavior.
This integrates cleanup's durability boundary and scoped canonical commit
deferral; transaction work references, retention scheduling and complete cleanup
ownership remain separate work.

The focused fixture exercises all four codec combinations and both root
policies, a mutation completed before scope creation, grouped PUT/META/DELETE,
explicit and completion barriers, repeated covered barriers, allocation failure,
sync failure and error precedence. It checks reads before the barrier, a pinned
old body across peer publication, peer durability while the scope stays open,
and successful earlier mutations after a later producer failure. An unpublished
append in another namespace keeps its prior selected tail through all barriers.
Twenty-four sync-failure cases cover barrier/end/error completion across codecs
and policies. Another 24 process-death cases reopen images captured only at
successful syncs, proving prior state before the barrier and completed state
after explicit/completion barriers without relying on surviving page cache.

The complete standard Debug preset passes **361 checks**, including **309 unit
groups**, with ASan/UBSan and constrained workers in **1327.32 s**. The fourteen
affected GNU/musl groups pass in **13.75/11.78 s**. Instrumented-dependency
ThreadSanitizer passes forty concurrent comparisons and six focused groups;
four Memcheck runs complete 58,244 allocations/frees with zero leaks/errors.
The comparison modes `runtime-scope` and `pouch-scope` put both write rounds
inside one forced scope per writer. Aggregate elapsed time includes scope
completion; individual PUT samples exclude that final group barrier.
The [matched measurements](pouch-space-experiment.md#scoped-canonical-commit-deferral)
remove the prior per-mutation sync slowdown in this small workload. Reopen
readiness and broader backend acceptance remain open.

## Known canonical recreation defect

A separate probe exposed a shared logical defect before this repair: PUT, DELETE, then
PUT of the same key reports successful recreation at public version 1 and index
sequence 3, while immediate and reopened reads still report absence. This
reproduces in both the container and multifile implementation under both sync
policies, using GNU release libraries from before the scope change. The
reproducer and its four logs are under `build/pouch-space-durable-scope/` as
`recreation-probe.c` and `recreation-{container,multifile}-{0,1}.log`.

The preceding mutation writer reset the public generation after a tombstone,
while `lc_pouch_state_entry_supersedes` rejected lower payload generations
before considering the newer index sequence. The earlier recreation fixture
checked the mutation result but did not read the recreated body. The lifetime
repair below reconciles generation-1 recreation with stale-generation replay
protection and proves immediate, reopened and compacted state/object readback.

### Selected key lifetime ordering

The `45da71c` GNU libraries reproduced the same failure in both backends
under both root policies. The unchanged probe is rebuilt and retained under
`build/pouch-space-key-recreation/`, with `build-probe.sh` and
`{container,multifile}-{0,1}.log`. Recreation returns version 1/index 3, but both
immediate and reopened reads report absence. Those logs retain the pre-repair
counterexample; the selected lifetime implementation is now verified below.

The selected design uses a durable, internal per-key lifetime identity. Use the
namespace index sequence of the body creation as a `uint64_t` lifetime value;
the first body after missing, tombstoned or metadata-only state starts a fresh
lifetime while retaining the specified public generation behavior. Ordinary
body updates, metadata overlays and deletion retain that identity. A placeholder
before the first body has lifetime zero. Metadata after deletion must retain the
previous lifetime until a new body starts another one.

Replay first compares lifetimes. Within one lifetime, preserve the existing
payload-generation ordering and equal-generation index tie-breaker; metadata
overlays remain ordered by index sequence. Carry the lifetime through canonical
PUT/link/META/delete records, resident and captured projections, clean-close
projections, snapshots and compaction. A link uses its destination key's lifetime
while retaining the existing encoded payload context. Snapshotting only the
current row must preserve its lifetime so an older, higher-generation record
cannot displace a recreated value after replay. Index allocation and recovery
must not reuse an identity while a recoverable reference can still retain it.

The design adds another eight-byte scalar per affected canonical record and
snapshot row. Encoding/decoding, exact size planners and corruption checks must
change together. The user delegated the technical choice; preserving existing
generation-1 and replay semantics through durable lifetimes is now selected and
authorized for implementation in both backends. It is now implemented through
ordinary writes, metadata/deletes, staging, snapshots and native recoding.
Existing-root conversion, compatibility parsing and a format-version increment
remain outside this decision. No payload copy or separate journal is introduced.

Acceptance must prove state/object recreation immediately, after reopen, after
repeated deletion/recreation, and after compaction with the old tombstone gone.
Include older-lifetime payloads and deletes with higher public generations,
same-lifetime stale/equal generations, metadata-only lease transitions, staged
promotion, pinned old readers, both policies/all codecs, and allocation/I/O/torn
write failures. Existing stale-generation and independent metadata ordering
tests must remain valid before this correctness repair can be accepted.

The allocation-recovery prerequisite is implemented: every valid canonical
record advances the namespace index high-water before per-key supersession.
An ignored older payload generation therefore cannot make its allocated index
reusable after replay or snapshot replacement. Public-query freshness still
follows accepted projection changes. Native replay uses this common rule;
typed container replay and logical capture now retain the same high-water.
Cold and warm container index/query reads use namespace projection authority,
storage health checks and typed recovery instead of falling through to native
filesystem paths.

The `sequence-recovery` test covers 96 state/object cases across both backends,
all four codec combinations, both policies and compaction before/after reopen
or without compaction. It checks empty namespaces, either sequence API as the
first cold operation, repeated reopen, preserved body/version/query freshness
and the next unique allocated index. Two logical-capture cases prove that an
ignored record's higher index survives without a HIGH_WATER record or with a
lower one. The existing cold/warm allocation and I/O sweeps now include both
sequence APIs, including zero output on error and successful retry.

Verification under `build/pouch-space-key-sequence/` passes the complete
363-check Debug preset (311 unit groups), 38 GNU release groups, 14 native musl
groups, five ThreadSanitizer groups with instrumented OpenSSL and three Memcheck
runs with zero live blocks/errors/suppressions. The full Debug read sweeps
exercise 4,228 allocation and 1,298 I/O failure sites. That prerequisite added
no record fields.

Canonical metadata now ends with a mandatory 20-byte lifetime/index trailer:
`[uint64 lifetime][LCSI][uint64 index]`. Snapshots and explicit query overlays
append the existing distinct 12-byte `[LCSQ][uint64 query-index]` extension,
giving 32 bytes in total. The terminal marker stays twelve bytes from the end,
so marker-shaped scalar values cannot masquerade as another trailer form.
The fixed 74-byte payload prefix and crypto binding/context remain unchanged.
Controls and collection-owned records carry zero in the shared lifetime slot.
Exact size planning includes the scalar. Mutable body admission rejects zero
lifetimes; decoding rejects a lifetime beyond its record index and side fields
overlapping the trailer. New body identities are assigned from the final
reserved index before clearing PENDING. No payload copy, extra allocation,
separate journal, writer queue, public API or shared-library ABI change is added.
Existing-root conversion and compatibility parsing remain excluded; recreate
experimental fixtures made with the preceding canonical metadata layout.

The `lifetimes` group covers 32 fixtures across both backends, all four codecs,
both policies and state/object classes. Each performs four recreations with
metadata-only transitions, compaction before/after creation, repeated reopen,
both input paths and pinned old readers. Sixteen native fixtures additionally
prove staged updates retain the live destination lifetime, promotion into a
deleted destination starts a fresh one, and two compression-policy recodings
preserve state. Every fixture checks two exhausted-index recovery/rejection
attempts without wraparound or query-generation changes. Captured-prefix tests
apply stale former-lifetime PUT/DELETE/META records after both complete history
and a snapshot containing only the recreated value. Existing same-lifetime
version/index and independent metadata ordering remain intact.

Codec tests retain lifetime values in live records and snapshots, distinguish
zero placeholders from nonzero tombstones, exercise marker-shaped scalar values
and reject invalid inputs before output. A corrupt lifetime fails both strict
and repairable reads without replacing a prior decoded result. The dedicated
`lifetime-faults` group passes 1,272 allocation/I/O/partial-write failure sites
around recreation after compaction has removed the prior tombstone. Recovery
retains either the metadata-only placeholder or the new generation-1 body;
retry and another compaction/reopen must preserve the new value.

The completed repair passes the full 365-check Debug preset (313 unit groups),
20 GNU release and 20 native musl groups, 80 concurrent comparisons plus eleven
strict ThreadSanitizer groups with instrumented OpenSSL, and four Memcheck runs
with 858,328 allocations/frees and zero live blocks/errors/suppressions. Evidence
is under `build/pouch-space-key-lifetime/`. The
[720-trial comparison and focused read follow-up](pouch-space-experiment.md#durable-key-lifetimes)
retain all samples and describe remaining performance limits. Normal public
Pouch open still selects the multifile backend; full backend integration remains
open.

## Standing maintenance capacity: decision pending

The existing joint claim protects a particular admitted operation. A standing
reserve must additionally make required work admissible after foreground space
is exhausted. Raising the fixed floor alone does not establish that property
with the current logical snapshot layout.

There are two distinct obligations. The production delete path repeats the key
in its tombstone, but deletion of a metadata-only placeholder is deliberately a
no-op. The tested 60 KiB body keys therefore demonstrate the deletion obligation;
the accepted 1 MiB metadata-only keys demonstrate the snapshot obligation. Do
not infer that deleting a 1 MiB placeholder writes a 1 MiB tombstone.

The canonical record header is 32 bytes. Delete metadata occupies at least
38 bytes before the etag and any extended query-index trailer. The exact-span
planner allows 4,032 canonical bytes per 4 KiB page, accounting for the 64-byte
physical run header, and 88 descriptor bytes per page in its fragmentation
bound. These give the following lower bounds, excluding routing, stream heads,
catalog entries and checkpoint workspace:

| Obligation | Canonical bytes | Planned payload bytes | Planned descriptor bytes |
| --- | ---: | ---: | ---: |
| One 60 KiB body-key tombstone | at least 61,510 | at least 65,536 | at least 1,408 |
| One 1 MiB placeholder in a snapshot | more than 1,048,608 | at least 1,069,056 | at least 22,968 |

These are conservative admission bounds from `ps_budget_plan_span`, not measured
physical consumption or a complete reserve recommendation. The public key-bound
fixture covers both backends and durability policies, plaintext and combined
compression/encryption. It also checks placeholder deletion is a no-op and
preserves that placeholder and the distinct deleted body key across initial and
replacement container snapshots and reopen.
The expanded `pouch_space_pouch_key-bounds` test passes under Debug ASan/UBSan
with a 128 KiB worker stack in 2.24 s; the log is
`build/pouch-space-durable-scope/debug-init-focused.log`. Earlier GNU release
and native musl runs passed in 1.80 s and 1.79 s, recorded under
`build/pouch-space-standing-capacity/` as `gnu-key-bounds.log` and
`musl-key-bounds.log`. Debug CTest also applies its worker stack policy through
the generated `CTestCustom.cmake`; explicit environment settings in later runs
reinforce the same policy.

Every current snapshot serializes the complete resolved key metadata, including
keys whose bodies remain elsewhere. Protecting another complete snapshot
therefore requires workload-sized spare capacity. In a metadata-dominated
namespace, old and new metadata must coexist until durable selection and
retirement; the resulting upper bound on usable capacity can approach one half
of the container before other overhead. This is a design bound, not a measured
typical utilization figure. The current source-sized body copy is a separate
workload-sized obligation.

The recommendation is to design bounded incremental metadata maintenance and
body relocation before selecting a standing reserve. This is a proposed change
to logical persistence and maintenance architecture, not an implemented or
approved format change. The design must define foreground publication as well
as recovery: splitting only the output of a complete snapshot does not remove
its bootstrap requirement to retain all prior metadata. It must specify the
authoritative representation, atomic multi-key/high-water publication, the
largest indivisible work unit, and progress without a full-namespace rewrite or
a complete source-body copy. Preserve canonical behavior, true streaming,
independent producers, pinned readers and the existing durable commit contract.

Before implementing that change, select the architecture explicitly. Retaining
complete snapshots instead requires accepting and exposing a workload-sized
reserve and its usable-capacity cost. Neither choice authorizes silent growth,
external persistent staging, reduced key limits or weaker durability.

Whichever architecture is selected, verification must fill foreground capacity
at fixed size, then prove deletion and reclaimable maintenance can proceed once
reader pins drain. An all-live store may remain full. Include maximum accepted
keys and metadata, multiple namespaces, repeated maintenance/deletion cycles,
fragmentation and allocation/I/O/torn-write failures. Reserve accounting must
protect every still-recoverable state under NoSync rollback as well as durable
publication, and must recover after process death before readmission. Measure
usable capacity, write amplification, CPU, durable barriers and maintenance
latency against the frozen multifile reference. No reserve number is selected
by this analysis.

## Production state integration

A private `ps_pouch_state_open` harness now connects ordinary production state
PUT, metadata update, delete, point read, metadata read, copy, read-many,
metadata visits, paged scan summaries/body access and namespace creation to the
container.
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
coordinator. Ordinary PUT producer callbacks and durable commit waits release
the projection mutex while exact-key and append authority remain held. Competing
stale preconditions fail before consuming their source.

Metadata updates retain the existing per-namespace batching worker and bounded
metadata aggregation. Its canonical vectored append now uses the typed record
I/O boundary; tombstones use that boundary too. Neither operation copies body
payloads. Rotation accounts for the complete metadata batch size. This worker
retains its existing projection coordination during durable commit; it does not
inherit ordinary PUT's projection-unlock behavior or add a root-wide writer queue.

Mutation admission now rejects state keys beyond the decoder's existing 1 MiB
limit before consuming a producer. Header/batch encoding applies matching key
and metadata bounds. This fixes durable metadata-only writes that could report
success for an oversized key and disappear after replay. Exact-limit metadata
keys remain supported; existing individual metadata-field limits still apply.

Encoded read windows retain the namespace and storage owner. Their decoded
sources retain their original body and crypto binding across replacement,
rotation and Pouch close. The last source releases exclusive container ownership.
Crypto outputs use the logical owner's allocator; default-allocated key IDs
retain their original allocation domain. Shared append completion cleanup also
releases reserved metadata and precomputed transforms after failed reservation
or finalization.

Read-many and scan summaries capture lightweight encoded read pins under the
projection guard, without payload I/O or a decoder buffer per row. Read-many
decodes one captured source immediately before its callback, after releasing
projection coordination. Scan body access clones the captured immutable window
with an independent cursor; it never looks the value up again in the current
directory. A clone retains the window, namespace, root and crypto lifetime even
after the scan and Pouch runtime close. This pin protocol also supplies the read
side needed for future relocation/reclamation.

The container does not populate a body cache, including through the existing
cached read-many entrypoint or a scan's cache-completed-body hint. These hints
remain optimizations in the multifile owner. Metadata visits and paged summaries
retain their existing ordering/filtering/cursor semantics. Callback mutation
cannot change the captured rows or their body references. Scan body results now
copy their metadata rather than consuming the borrowed row, preserving its
fields through result cleanup and repeated body access in both backends.
Read-many results also report their captured has-body and staged-delete fields.
There is no public ABI or persistent format change.

A conservative namespace retention marker occupies control slot 2, separate
from the segment head in slot 1. Its 32 bytes bind a magic, namespace owner,
present flag, zero reserved fields and CRC32. Ensure publishes it durably even
under NoSync before an expiry-bearing record. Failed mutation may leave a
harmless positive hint. Reading the hint does not open segments or replay
bodies. Marker removal, retention scheduling and reclamation remain pending.

Protected physical capacity does not yet provide end-to-end maintenance
admission. At a fixed 10 MiB capacity, another namespace can consume the space
needed to rotate before deletion. The capacity probe checks the prior value,
selected head and tail across failure/reopen, not guaranteed deletion or
reclamation. Reserving the complete
maintenance dependency set before cutover remains an acceptance requirement.

This harness explicitly requires disabled query indexing, background maintenance
and queue watches, exclusive root ownership and no generated external key file.
State staged/lease publication requests and staged decision replay return
unsupported. Other Pouch APIs are outside this harness's current call contract.
Prepared/namespace mutation APIs, public query-client ownership, collections,
queues, XA, indexes,
logical snapshot/obsolete policy, maintenance and shared-root operation remain
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
projection, ordinary mutation policy, read-many, visits and paged scan/body
snapshots. Advanced mutation paths, public query-client ownership,
logical snapshot/obsolete policy and public collection ownership still require
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

This is partial integration, not root-wide adoption. Collection owners, advanced
namespace mutation policy, public query-client ownership, logical snapshot/obsolete
policy, position/receipt
authority and remaining control/projection owners still need conversion to
container ownership. Normal public Pouch tests continue to
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
