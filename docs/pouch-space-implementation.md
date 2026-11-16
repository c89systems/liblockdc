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

Unbudgeted `PS_CONTROL` values support up to 4 KiB. Publication and deletion
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

### Budgeted multi-page controls

Larger controls now use the same immutable value writer when supplied an
explicit capacity claim. `ps_budget_plan_value` funds one new binding, payload
rounded up to physical pages, and the worst case of one extent descriptor per
page. Empty values still need their 64-byte descriptor. The planner rejects
values whose worst-case extent list cannot fit one journal publication, and
leaves the plan unchanged on overflow or invalid input. Replacements spend only
positive descriptor growth; unused credits return when the claim closes.

This supports the existing retention journal's bounded metadata size without
putting application bodies into controls. The writer still takes producer
chunks through its fixed 16 KiB buffer. It neither materializes the complete
value nor introduces a staging file. Explicitly funded controls retain
always-durable publication, deletion and completed-prefix reads on both root
policies. Routing values remain limited to one page, including with a claim.

There is no format-marker change, larger automatic replacement allowance,
standing reserve selection or implicit growth. The caller must admit the
complete replacement while its predecessor and any pinned readers still own
their capacity. Repeated replacements may require explicit checkpoint/reclaim
before another claim is admissible. The [typed retention control owner](#typed-retention-control-ownership)
now uses this support. The [typed transaction authority](#typed-transaction-authority)
is integrated too; complete public lease/expiry acceptance and the pending
maintenance architecture decision remain required.

Five new test groups cover planning overflow and journal bounds, page/chunk
boundaries, zero-length values, replacements and pinned readers, underfunded
claims, process exit without root close, and held payload/publication/deletion
barriers on both durability policies. A 3 MiB + 128-byte fixture stays within
32 KiB per individual internal allocation during its write. Fixed 10 MiB
capacity tests admit the claim, exhaust foreground space, take the remaining
maintenance floor with a peer claim, then publish across a metadata repack.
The fragmented cases consume the planner's full one-extent-per-page bound.

The existing deterministic file-image model uses reduced 1 MiB fixtures for
28 write/barrier and 46 allocation fault sites across replacement, checkpoint
and deletion. It restores stable bytes after lost or persisted torn writes,
verifies the complete recovered old/new value, and retries on the reopened
owner. Optional allocation failures may succeed through the existing fallback;
they must still recover the acknowledged complete value.

Verification covers 430 distinct Debug groups: all 425 existing standard checks
plus the five new groups. GNU and native musl pass 25 affected groups each.
Strict TSan passes all five new groups plus existing budget and commit coverage
with instrumented OpenSSL, `halt_on_error=1` and no suppressions. Four normal-exit
Memcheck groups report 109,190 allocations and matching frees, zero live heap,
zero errors/suppressions and only three inherited descriptors at exit. The
deliberately unclosed process-exit fixture runs separately from Memcheck's
normal-cleanup checks. GNU/Debug checks use the 128 KiB worker-stack policy;
musl retains its native defaults. Formatting and whitespace checks pass.

Evidence is under `build/pouch-space-value-budget/`: `debug-standard.log`,
`debug-final-new-tests.log`, GNU/musl test logs, `tsan/`, `tsan-relink.sh` and
`memcheck/`. The final test-only split separates deliberate process exit from
normal lifecycle cleanup; production inputs are unchanged. Existing bounded
head comparisons also pass. Larger retention records have no matched native
timing claim yet; that comparison belongs to the typed journal integration.

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

### Recommended architecture for the pending decision

This is a **proposal**, not approval or an implementation claim. Select the
direction before changing persistence. Replace complete experimental namespace
snapshots with incremental canonical metadata checkpoints, and replace
source-sized body relocation with bounded relocation of encoded ranges. Keep
one allocator, one physical publication coordinator and the existing typed
directory. Do not build a pathname layer or a second database inside the file.

The inspected implementation establishes the reasons for both changes:
`maintenance.c:plan_snapshot` measures a complete resolved namespace and all
live bodies in one selected source; `namespace_snapshot.c` funds those complete
outputs before staging. Splitting that output into writes would leave its
capacity requirement unchanged. The existing directory already supports
copy-on-write paths over fixed-width typed identities, while the canonical
snapshot encoder can write one complete metadata entry with a borrowed body
reference and no body I/O.

#### Canonical authority and incremental checkpoints

Keep canonical metadata records authoritative. A checkpoint selects complete
canonical entries through the existing typed physical directory; it is not an
independently writable projection or a duplicate source of logical truth.
Each namespace selection binds its checkpoint coverage boundary and both state
and query high-water values. Replay starts with the selected canonical entries
and applies complete newer canonical groups using the production resolver.
Derived query indexes remain rebuildable.

Give each retained logical key slot a durable internal numeric identity, scoped
to its namespace. State/object classification stays in the canonical entry;
changing that classification must not create a second slot for the same key.
Store the full exact key in its canonical entry; a hash is never identity.
Rebuild the resident name-to-slot lookup when
opening the namespace. This uses the directory's numeric keys without adding
an on-disk variable-length name tree. Slot identity is distinct from public
generation, log index and body lifetime: metadata-only placeholders currently
have lifetime zero, and body recreation must still get a fresh lifetime while
retaining the existing generation-one semantics. Preserve required tombstones,
live stages and decisions according to their logical policy; do not discard
them merely because they have no body.

Foreground mutations keep the bounded canonical append path. Completed frames
are the indivisible replay units for compound mutations; streaming PUTs retain
their current completion rules. Track changed slots and their resolved metadata
under the existing namespace authority. A checkpoint captures a complete prefix
and writes only its changed canonical metadata entries and changed directory
paths. Unchanged metadata and body references remain shared. Publication selects
that changed set, its exact coverage boundary and its high-water values together
through the physical root protocol. A newer foreground suffix is replayed after
that boundary; it cannot be accidentally included in an older capture.

Maintain the incremental checkpoint from the first admitted mutation in a new
experimental store. Bound the outstanding changed set and fund its complete
checkpoint before admitting work that enlarges it. Finish a funded checkpoint
before admitting a group that would exceed the current bound. A single compound
group may exceed the usual target only with its complete capacity claim; never
split its semantics to satisfy the target. This removes the bootstrap dependency
on retaining an entire log until a first full namespace snapshot can fit.
Capturing only changed slots is required; calling the current whole-namespace
snapshot capture on every incremental checkpoint would preserve its CPU/memory
cost even if fewer bytes were eventually written.

Old canonical history remains owned until the selected checkpoint covers it,
the superseded recoverable root is durably retired, and reader pins permit reuse.
NoSync must protect both selected checkpoint data and the still-recoverable
newer history. Checkpoint admission may force structural synchronization as it
does today; measure that frequency and latency instead of assuming NoSync work
has no barriers.

#### Bounded encoded-range relocation

Separate stable logical source offsets from their current physical ranges.
Preserve existing payload references and crypto binding contexts while replacing
only the physical mapping of a bounded range. A read window pins its mapping
generation; it must not follow a replacement midway through a read. Source names
remain logical labels, with no external file or persistent sidecar.

Use at most the existing 16 KiB stream chunk as one relocation data unit, with
page-granular fallback under fragmentation. Persist checksums for independently
relocatable encoded pieces when foreground bytes become complete. Private
canonical fixups must finalize the affected piece checksums before publication.
This gives relocation an integrity check for the bytes it moves without reading
the rest of a large body. Keep the canonical whole-payload CRC and transform
validation too; a piece checksum is not a replacement for either.

One relocation step reads and checks one piece, writes its replacement outside
publication authority, validates the captured mapping, selects the new mapping
durably and retires the old one through the root protocol. A concurrent change
invalidates that candidate; it cannot overwrite a newer mapping. Physical piece
ownership must participate in the same allocator/recovery walk as all other
typed records. Do not hide pages behind an opaque pointer that recovery cannot
account for.

After checkpoint coverage makes old log records unnecessary, reclaim ranges
with no remaining canonical, body, stage, decision or reader dependency. A
source may become sparse only behind a prefix that replay no longer needs.
Reading a required unmapped range is corruption, not EOF or zero-filled data.
Relocate live ranges out of mixed pages so dead bytes can actually return
capacity. Merely copying each partly live page unchanged would not establish
reclaimable progress. Never require a replacement copy of the complete source
or complete body to start this work.

Readers may temporarily retain old pieces and delay progress; their close must
make those pieces eligible without another namespace-sized rewrite. No producer
holds namespace, allocator or root publication authority while supplying input
or copying a body. A maintenance capacity claim does not grant a lock over
unrelated producers.

#### Standing admission and control owners

The reserve is a computed obligation, not another fixed floor selected by
guesswork. Conservatively account for the physical checkpoint workspace, the
funded changed-slot checkpoint, a deletion with the largest retained required
key/metadata, one relocation piece, and their routing, descriptor, publication
and retirement dependencies. Count distinct simultaneously required ownership;
share credits between phases only after the earlier dependency is durably
retired and unpinned. Existing `ps_budget_plan_*` span and fragmentation bounds
are inputs, not a proof of the complete new obligation.

Foreground admission must preserve this free/claimed workspace before accepting
a larger key, larger metadata, larger indivisible group or further private
payload allocation. Failure preserves the selected logical state. Persist enough
canonical information to reconstruct the obligation before recovery readmission;
a lost process-local claim cannot turn protected workspace into foreground
capacity. Release excess protection only after every state requiring it has
been durably retired. Reader-held space is still owned and is not available
reserve.

Large indivisible transactions may still require operation-sized capacity.
Reserve the completion obligations of admitted live participants; do not claim
that incremental checkpoints make arbitrary atomic groups constant-sized. The
design removes mandatory whole-namespace metadata and whole-source body copies
from ordinary maintenance. It does not remove the storage lower bound of an
indivisible operation. An all-live store can remain full; reclaimable work must
make progress after obstructing pins drain. No silent growth, lower public key
limit or external persistent staging is authorized.

Complete the remaining logical control ownership through typed capabilities.
The inspected public lease path required retention-work reservation checks and
transaction guard ownership at this boundary. The implemented
[namespace enumeration boundary](#typed-namespace-enumeration) now supplies
names from the existing container catalog to implicit transaction leader
discovery. That independent integration changes no persistence layout and does
not approve this maintenance proposal. [Retention control ownership](#typed-retention-control-ownership)
now uses the same boundary. [Transaction authority](#typed-transaction-authority)
now uses typed capabilities too; complete public lease/expiry behavior still
requires its own acceptance proof. Keep their policy in the
production engine. Do not treat the
current exclusive open as proof of the final shared-root contract. Interprocess
publication and reader-generation recovery remain required within the same file.

#### Acceptance and cutover sequence

Implementation would first replace experimental metadata checkpoint/replay from
store creation, then add independently checked encoded mappings and bounded
reclamation, then complete standing admission and control owners. Each stage
needs its own failure proof before the next depends on it. Normal multifile
Pouch remains the semantic and performance reference. Recreate experimental
fixtures at the private representation change; add no legacy parser, migration,
parallel experimental snapshot path or automatic format-version increment.
Production deployment and existing-root conversion remain separate decisions.

Acceptance must establish all of the following, beyond the current tests:

- Atomic key-set, checkpoint-boundary and high-water selection, including
  tombstones, placeholders, recreation, live stages and decisions. Reject
  incomplete frames as a group and preserve corruption errors in selected data.
- Checkpoint cost follows changed entries and paths, including the first
  checkpoint and repeated one-key churn as total namespace metadata grows.
  Neither the planning pass nor output can secretly walk/rewrite every key.
- Bodies larger than the free maintenance workspace remain readable while
  individual encoded pieces move. Verify piece corruption, private header
  fixups, all codecs, pinned old mappings and partial-page reclamation.
- Fill a fixed 10 MiB store through foreground admission, then delete and
  reclaim across multiple namespaces after pins drain. Include the existing
  60 KiB body keys and 1 MiB metadata-only keys, mixed live/dead pages, repeated
  cycles and explicit 1 MiB growth tests. Measure actual recovered free space.
- Cut allocation, I/O, sync and process execution across preparation, selection,
  retirement and retry. Model selectively persisted writes separately from
  process death, under both policies. Recover reserve accounting before allowing
  any new producer, and keep unrelated dirty writers out of acknowledged work.
- Reuse existing public behavior suites as each remaining owner is integrated.
  Assert exactly one persistent file after success, failure and recovery, and
  verify process-shared publication/reader generations before claiming the
  shared-root portion of the full-backend goal complete.
- Compare verified native GNU/musl, sanitizer and concurrency results and matched
  performance with the frozen container and multifile baselines. Report usable
  capacity, changed-entry work, mapping/checksum overhead, write amplification,
  CPU, memory, barriers and worst maintenance latency. The current 240-trial
  recovery comparison establishes none of these new maintenance claims.

Approving this proposal would select the incremental architecture and authorize
its experimental persistence changes. It would not select an unmeasured reserve
number, waive the acceptance gates or authorize production cutover.

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
Staged writes, prepared mutations, promotion/commit/discard and cold decision
replay are covered by the framing slice above. Requests that mark a query-index
operation active still return unsupported. Live-stage maintenance, public
leases/queue/XA ownership, collections, indexes, retention work references and
shared-root operation remain integration work. Full backend acceptance and
deployment authorization remain separate.

## SDK client runtime admission

The private `ps_pouch_client_open` harness admits the existing container owner
through the common SDK client constructor. `lc_client_open` retains its normal
multifile admission. A private factory receives the resolved endpoint path,
allocator and effective Pouch options after the normal endpoint/settings
validation. The constructor invokes it once and owns every returned runtime,
including during failed initialization. It retains neither the factory nor its
context. The common constructor still installs the complete production method
table, copies configuration, initializes logging/lifecycle ownership and enters
transaction recovery. There is no separate container method table, public
backend selector, persistent format change or ABI change.

The private wrapper adds only create/open intent, initial/maximum physical
capacity and the existing I/O fault seam. Logical settings keep their ordinary
SDK precedence and validation; unsupported indexing/background/watch/shared
settings are rejected by the existing container owner. Creation requires a new
file; it cannot adopt or overwrite another store. Native root conversion and
production cutover remain outside this admission seam.

The implemented SDK call envelope covers streamed get/get-in-namespace, mapped
load, scan queries and namespace query configuration. It reuses production
handlers over the typed canonical state boundary. Seeded state tests compare
native and container owners across all four codecs and both policies, warm and
after reopen. Generic object records remain query-visible when not hidden;
attachment visibility follows the SDK's explicit query-hidden flag. Pure scan
queries retain the existing zero index-freshness sequence. Namespace config
updates retain etag preconditions, inherited settings and durable readback under
the root policy. Public leases, attachments, queues, history consumers, XA and
indexed query ownership still require their remaining integrations.

Receiver tests close the client from a get sink callback while the operation's
pin protects response cleanup. Query tests cover one-row pagination, callback
error and retry. The fixture checks custom allocation domains, releases every
client-owned allocation and enumerates its parent directory for unexpected
persistent files. Factory validation covers rejection before admission, success
without a runtime, and failure after a real owner has been returned; the latter
must release its file lock and permit reopen. Initial fault sweeps pass all 344
allocation and 176 I/O sites across create/reopen, both policies, plain/combined
codecs and calls with/without an error object. Positive short reads must complete;
partial writes followed by EIO retain the normal uncertain-store behavior.

The comparison adds `runtime-sdk-read-points`/`pouch-sdk-read-points` and
`runtime-sdk-scan-metadata`/`pouch-sdk-scan-metadata`. They seed ordinary committed
records through the same logical state engine, then time public SDK streamed
gets or paged key scans. Gets verify bytes and versions; key scans verify exact
membership, with full canonical readback outside that timed phase. Warm and
reopened CTest fixtures include 64 KiB bodies.

The SDK slice passes the complete 420-group Debug CTest run (3,503.25 seconds),
including the existing crash matrix, public method contracts and shared-library
symbol boundary. Focused native GNU and musl release runs each pass 25 groups
(45.12 and 43.72 seconds). GNU tests use 128 KiB default worker stacks with the
glibc stack cache disabled; musl uses its ordinary defaults. Formatting and
whitespace checks pass. Logs and complete test output are retained under
`build/pouch-space-client/`: `debug-standard.log`,
`debug-standard-details.log`, `gnu-tests.log`, `gnu-test-details.log`,
`musl-tests.log` and `musl-test-details.log`.

Strict ThreadSanitizer passes all four SDK test modes and 40 four-worker SDK
comparisons across both backends, both sync policies and both read profiles,
five samples each. The binaries are relinked using the retained fully
instrumented OpenSSL archives, with no suppressions and `halt_on_error=1`.
`tsan-build.log`, `tsan-relink.sh`, `tsan-check.sh`, `tsan.log` and the per-case
`tsan/` logs retain the exact inputs and output. Memcheck passes all four SDK
modes: 334,619 allocations and matching frees, zero live blocks, zero errors
and zero suppressions. `memcheck.sh`, `memcheck.log` and `memcheck/` retain its
commands and detailed reports.

All 80 matched SDK performance trials verify their results and retain exactly
one 10 MiB container file. Seven of eight warm read/scan range pairs overlap;
the four-worker Durable get ranges are narrowly separated in the container's
favor. Reopen plus first read remains clearly slower in every configuration:
container medians 7.520–10.175 ms versus multifile 1.064–1.799 ms. The
[SDK performance results](pouch-space-experiment.md#sdk-client-read-and-scan-comparison)
record every configuration, ranges, CPU, workload limits and frozen input
identity. These measurements cover SDK reads/scans over canonical seeded data;
they do not price the remaining public leased-write integration.

## Recovery cleanup write bounds

Physical reopen now leaves already-zero recovery bytes alone. Recovery already
holds the complete journal image and both root headers. It finds the first and
last nonzero byte of the rejected suffix and zeroes that complete span. Zero
gaps and trailing zero bytes need no write. A nonzero sibling root is still
retired in full; an already-zero sibling needs no rewrite. Every rejected
journal byte is therefore zero before sequence reuse, including stale valid
events beyond an invalid or empty prefix.

Recovery synchronizes rejected-tail and sibling-root retirement **before**
writing the durable incarnation grant, then synchronizes that grant separately.
Both barriers are mandatory under both policies, even when cleanup reads only
zeros: an earlier failed recovery may have left those zeros in the file cache
without retiring the stable stale events. Accepted-record verification, root
selection, token fencing, poison/retry behavior and the persistent format are
unchanged. This is a bounded write reduction with corrected recovery ordering,
without a deferred recovery policy or storage-format commitment.

The registered `pouch_space_reopen-sanitize` test covers both policies and six
layouts: a clean journal, one distant dirty byte, a valid stale event after a
zero gap, that event plus a distant dirty byte, a valid older sibling root and
a corrupt sibling. Valid stale payloads remain on disk; a second reopen must
not resurrect their events after the opening grant reuses a sequence. Every
successful fixture checks the complete rejected suffix and sibling are zero,
the committed value is intact, capacity is unchanged and returned incarnations
remain fenced after a simulated loss of later writes. The 46 actual recovery
write/sync failure sites are swept with lost writes and persisted torn prefixes:
92 failure cases plus 12 successful baselines. Clean opens issue one 64-byte
write and two syncs; the distant-byte fixture issues 65 total bytes. Wide dirty
suffixes still receive their complete required cleanup.

The separate `pouch_space_reopen-ordering` test places a rejected PUT or DELETE
on the physical page after the next opening grant. Eight cases cover both
policies and ordinary/cached-zero retries. They persist the grant independently,
fail its final sync and lose other unsynced writes. Reopen must preserve the
committed key and must not resurrect a rejected key. Rebuilding physical storage
from `7ce0381` against this same test loses the committed key, so this ordering
failure predates the cleanup optimization. Its retained failing log is
`build/pouch-space-reopen-fenced/prior-ordering.log`.

The cleanup test first failed against the previous implementation's clean-open
I/O bound. An existing stream fault test initially rejected the lower call count;
its fixed minimum is replaced with required observed failures in both opening
and stream repair, retaining all dynamically counted fault sites and existing
poison/cold-readback/retry assertions. After correcting retirement ordering,
focused Debug checks pass seven groups (50.32 seconds); native GNU and musl
each pass 34 affected groups (162.86 and 169.14 seconds). The SDK constructor
sweeps 344 allocation sites and 168 actual I/O sites. The remaining 415 standard
Debug groups pass in 3,373.80 seconds, completing the 422-group standard gate
with the seven focused groups above. The summary and complete output are
`debug-standard-remaining.log` and `debug-standard-details.log` in the final
evidence directory below.

Strict TSan passes both recovery tests, all four SDK modes and 40 concurrent
SDK comparisons using fully instrumented OpenSSL, no suppressions and
`halt_on_error=1`. Memcheck passes all six modes with 367,397 allocations and
matching frees, zero live heap, errors or suppressions. Final logs, exact
scripts and detailed reports are retained under
`build/pouch-space-reopen-fenced/`, including `debug-focused-details.log`,
`gnu-test-details.log`, `musl-test-details.log`, `tsan/` and `memcheck/`.
Earlier cleanup-bound failures and the isolated stream-repair pass remain under
`build/pouch-space-reopen/`. Formatting and whitespace checks pass.

The [matched recovery comparison](pouch-space-experiment.md#avoiding-clean-recovery-rewrites)
retains all 240 verified trials of the corrected protocol. Durable readiness
medians improve 25.6–59.5%; six of eight before/after range pairs are disjoint.
NoSync results remain mixed with overlapping ranges. All 16 warm-workload
range pairs overlap. Separate traces verify that clean reopen uses one
64-byte write and two syncs, compared with three writes/130,168 bytes and
one sync in the prior implementation.
Neither this change nor those measurements settle the standing maintenance
capacity decision or complete the remaining typed public owners.

## Typed namespace enumeration

`lc_pouch_storage_namespaces_visit` now supplies exact logical namespace names
through their owning storage. The native owner retains its directory walk and
name unescaping. The container adapter uses `ps_root_namespace_visit` over one
durable catalog prefix and checks poisoned admission before invoking callbacks.
There is no new public API/ABI, persistent format, backend selector or sidecar.

The implicit-transaction leader lookup now uses this capability; its old
filesystem loop is removed from the logical client. Callback names are borrowed
for the call, no ordering/filtering is imposed, and callbacks can open/create
namespaces or perform nested visits outside owner locks. Native walks retain
their existing directory semantics; container walks retain the captured prefix.
Valid native name allocation failure now returns `LC_ERR_NOMEM` rather than
silently omitting that namespace. Malformed native encodings are still skipped.

The three registered diagnostic modes cover exact names, empty and reopened
stores, callback error/retry, nested creation, four concurrent create-and-walk
workers, zero physical I/O for resident container enumeration, poisoned
admission, native directory errors and one-file/sibling-artifact checks.
Fault sweeps cover 48 warm and 56 cold allocation sites with non-NULL and NULL
error pointers: 208 injected failures. Cold cases prove same-owner retry and
immediate close/reopen after failure. The existing shared-root native
cross-namespace transaction-enrollment regression also passes in isolation.

All 425 standard Debug groups pass across the three focused groups and the
remaining 422-group gate (2,940.62 seconds). The additional cold namespace
supplement passes in Debug/GNU/musl in 2.62/2.40/3.55 seconds. Native GNU and
musl each pass 13 affected groups, including SDK/root behavior, namespace
concurrency, native Pouch and header/boundary checks; the updated normal-allocator
comparison is separately verified in both. The native shared-library exports
exclude the new internal helper.

Strict TSan passes all three modes and the cold supplement with instrumented
OpenSSL, no suppressions and `halt_on_error=1`. Four Memcheck runs pass with
165,767 allocations and matching frees, zero live heap, zero errors/suppressions
and only three inherited descriptors at exit. The final cold supplement changes
test coverage only; production source and both GNU library archives remain
unchanged. Formatting and whitespace checks pass.

Evidence is under `build/pouch-space-namespace-visit/`: the
`debug-standard-remaining.log`/`debug-standard-details.log` pair,
`debug-final-details.log`, `debug-native-leader.log`, GNU/musl test logs,
`*-cold-*-details.log`/`*-cold-details.log`, `tsan-final/` and `memcheck/`.
The [40-case matched comparison](pouch-space-experiment.md#typed-namespace-enumeration-comparison)
prices the enumeration itself over 64 empty namespaces. Its repeated-walk
medians favor the container with disjoint ranges; combined reopen-readiness
ranges overlap. The later [retention control owner](#typed-retention-control-ownership)
completes journal storage. Complete public leased-write acceptance remains
required beyond the [typed transaction authority](#typed-transaction-authority).

## Typed retention control ownership

The logical retention facade now dispatches discovery, work load/store and hint
retirement through typed storage capabilities. Native Pouch keeps its existing
atomic file owner. Both owners share the same LPR1 codec, preserving its idle,
active and finished representations, transaction ID, fencing token, state
generation, percent-encoded key and bounded size. This is intentionally
materialized control metadata, with no application body or transaction outcome.
Malformed encoding, embedded NUL identities and bad fields return protocol
errors; allocation failure while decoding an identity or native hint cursor
returns `NOMEM` and leaves empty outputs. There is no public API/ABI or format
marker change.

The container retains namespace control slot 2's existing durable 32-byte
discovery marker and stores non-idle LPR1 work in control slot 3. Reading or
retiring a missing namespace creates nothing. A store requires an existing
hint. The marker precedes expiry-bearing canonical publication and remains
present while work is idle; clear durably removes only work. Retirement durably
clears work before removing the marker, and updates the resident marker cache.
A failure between those steps leaves a harmless idle positive hint. Controls
always use the completed durable prefix, including under NoSync. The logical
caller retains namespace mutation authority; outcomes remain solely in `.txns`.

Every work replacement admits its complete immutable-value claim before writing
payload. On capacity exhaustion for this known valid bounded plan, the owner
checkpoints once and retries admission, allowing superseded journal values to
be reclaimed. It never grows the file. Reader pins and all-live/claimed capacity
may still make admission fail. The existing best-effort post-checkpoint collect
can defer reuse if allocating a free-run node fails; the following admission
may therefore report capacity rather than `NOMEM`. Tests require the prior
work to remain selected and retry to reclaim successfully. This selects no
standing reserve or incremental metadata/body maintenance architecture.

Discovery scans namespace catalog identities and their fixed markers only.
It does not open canonical streams, work records or bodies. The cursor is
advisory and its format belongs to the storage owner. Native uses escaped
filename order; the container uses exact namespace byte order. Both select after the cursor
and wrap fairly. Returned namespace and cursor strings use the logical
allocator and have separate ownership. No path interpretation or extra files
are introduced in the container.

Public lease enrollment/release and expiry maintenance still need complete
public integration tests beyond the typed transaction guard owner. Root
admission remains exclusive. Shared-root coordination/recovery, the pending maintenance
architecture selection, and explicit production cutover remain open requirements.

The first standard run exposed a pre-existing staged-retention test assumption:
it bounded a `CLOCK_REALTIME` commit deadline using the independent maintenance
`time()` timestamp. A test-only clock seam reproduces that mismatch reliably.
The repaired assertion brackets the requested TTL with commit-clock samples,
including a deliberately advanced, stepping clock, across both owners and all
existing transform/durability cases. Production clock and TTL behavior are
unchanged. The failing assertion and its deterministic reproduction are retained
in `debug-standard.log` and `debug-staged-clock-negative.log`; the repaired case
passes in `debug-final-new-tests-and-clock.log` under the evidence directory.

Nine retention test groups cover exact codec bytes and bounds, both owners and
durability policies, idle/active/finished work, missing-namespace behavior,
retirement/re-ensure, metadata-only discovery and cursor fairness, malformed
logical records, completed-prefix visibility and an independent public body
writer during held journal payload I/O. The maximum-key fixture repeats large
work replacements in a fixed 10 MiB file. Capacity tests keep a peer claim and
a historical reader pin: failure preserves the selected work, and releasing
the pin makes retry succeed without growth.

Fault coverage includes 908 warm/cold allocation cuts across both owners and
policies, including omitted error objects, plus 120 allocation cuts through the
capacity/checkpoint retry. Every failed read leaves empty outputs. The
deterministic 10 MiB dirty-page file-image model covers 108 read, write and
barrier failures with lost or selected persisted torn writes. It verifies
complete recovered work and persistent discovery, then retries. Poisoned
admission cannot be bypassed through cached owners and performs no further I/O.

Verification covers 439 distinct Debug groups: the final 430 existing standard
checks plus nine new retention groups. GNU and native musl each pass 58 affected
groups, including native state retention, cleanup, Pouch/XA, compression and
the repaired staging clock test. Strict TSan passes the nine new groups, SDK
lifecycle and staging with instrumented OpenSSL, `halt_on_error=1`, no race
reports and no suppressions. Nine Memcheck groups report 433,574 allocations
and matching frees, zero live heap, zero errors/suppressions and only three
inherited descriptors at exit. GNU/Debug checks use 128 KiB default worker
stacks; musl uses its native defaults. New codec helpers remain hidden in GNU
and musl shared libraries. Formatting and whitespace checks pass.

Evidence is under `build/pouch-space-retention-journal/`:
`debug-standard-final.log`/`debug-standard-final-details.log`,
`debug-final-new-nine-tests.log`, `gnu-tests.log`, `musl-tests.log`, `tsan/`
(including `lifecycle-final.log` and `staging-clock.log`), the two explicit
TSan relink scripts and `memcheck/`. The final discovery benchmark addition
changes only its excluded test family; product inputs are unchanged from the
430-group standard run. The [160-case matched comparison](pouch-space-experiment.md#typed-retention-control-comparison)
reports faster small-control cycles and repeated hint selection, overlapping
elapsed ranges and higher CPU at the maximum-key stress point, and slower
combined reopen/discovery readiness. It does not price the complete public
retention transaction path or authorize production cutover.

## Typed transaction authority

Transaction guard ownership now crosses the typed storage boundary. The private
`lc_pouch_txn_guard` facade owns xid striping, root mutation ordering and cleanup.
The production client no longer opens transaction lock files. Native ownership
retains its existing 256 process mutexes, stripe hash, lock-file names and
`fcntl` locks. Shared native roots still acquire root durable mutation authority
before the stripe and release it last. Blocking and nonblocking admission retain
their prior behavior; nonblocking contention returns `LC_ERR_PENDING`.

The currently exclusive container owner supplies 256 owner-local mutexes. Its
capability accepts a numeric stripe and returns an opaque handle, retaining the
physical owner through release. An admitted guard may therefore outlive logical
runtime close without releasing exclusive file ownership early. Runtime lifetime
is required throughout acquisition and protected engine work; the acquiring
thread releases the guard. Separate roots have separate stripes. Acquisition
checks physical health before and after waiting, performs no allocation or I/O,
and creates no persistent control or lock file. Root initialization failure
unwinds only initialized mutexes, closes physical ownership and leaves retry
possible. The root's heap allocation contains the mutex table.

Retention cleanup now asks whether an acquired guard covers another xid rather
than inspecting a native mutex address. This preserves the collision case where
an abandoned foreign xid already protects the new cleanup xid's stripe. It
avoids reacquiring a nonrecursive mutex through either storage owner.

This boundary adds no public API/ABI or persistence layout. It does not implement
shared-root container publication or reader generations. Shared-root admission,
complete public lease/expiry/XA behavior and the pending maintenance-capacity
decision remain required before full backend acceptance. The private SDK probe
now covers leased retained save, release/commit, rollback and cold readback
through both owners and policies; it does not extend the harness's acceptance
to every public lease operation.

The five new `pouch_space_client_*` groups are `guards`, `guard-concurrency`,
`guard-faults`, `guard-public` and `guard-compare`. They cover invalid inputs,
stripe collisions, nonblocking contention, wait/release, independent stripes
and roots, native process locking, owner lifetime, idempotent release, cold
retry and strict single-file container artifacts. Fault checks cut all 256
container mutex initializations under both policies and with/without an error
object (1,024 failed opens), all four native path-allocation cuts, native file
open failure and poisoned container admission without I/O. The pure guard
microcomparison uses the same native/container facade and executable, separates
first admission from 1,024 repeated acquisitions and excludes root open/close.
The full Debug build and all **444 standard ASan/UBSan groups** pass in
3488.42 s, including 392 unit groups and all five new guard groups. Native GNU
and musl builds each pass 46 selected existing/new groups (380.34/365.66 s),
including native Pouch/cleanup/retention/XA/compression suites and Lua retention. All five
new groups pass strict TSan with instrumented OpenSSL and no suppressions. Four
normal-exit Memcheck groups have zero live heap, errors or owned descriptors at
exit, covering 331,256 allocations and matching frees. The native fork probe is
covered by ASan/UBSan and TSan; its intentional child `_exit` inherits the parent
heap and is outside the normal-exit leak gate. Private guard symbols remain
hidden in both GNU/musl shared libraries. GNU Debug and TSan workers use 128 KiB
stacks with the glibc stack cache disabled; musl uses its native defaults.
GNU Release omits the test stack initializer, so setting
`LOCKDC_TEST_STACK_BYTES=131072` alone does not constrain Release workers.
Those Release checks use ordinary platform worker stacks.

Evidence is in `build/pouch-space-txn-guards/`: `debug-final-build.log`,
`debug-five-final.log`, `debug-standard.log`/`debug-standard-details.log`,
`{gnu,musl}-{build,tests}.log`, `tsan/` and `memcheck/`.
The [40-case comparison](pouch-space-experiment.md#typed-transaction-guard-comparison)
retains all five samples per case, min/median/max elapsed/CPU/first-admission
statistics and executable/source hashes. It observes lower uncontended guard
overhead; complete transaction and shared-root performance remain unmeasured.

## Leased state and expiry ordering

The private SDK harness now exercises public leased updates, retention metadata,
keepalive, release, attachments, explicit participant votes and bounded
on-demand cleanup through the same logical engine with both storage owners.
The new `lease_test.c` uses the existing test library's logical expiry clock;
filesystem heartbeat timers and operational clocks remain real. This adds no
public backend selector, API/ABI or persistence format change. Background
scheduling, complete external XA ownership and guaranteed maintenance admission
at capacity remain outside this stage.

An independent syscall durability model exposed a production cleanup-ordering
bug. On a NoSync root, a complete commit-decision write could reach storage yet
report an error while its earlier participant stages remained unsynced. After
discarding those unsynced stages, recovery selected the commit, treated absent
stages as already applied, and cleared cleanup work while retaining the expired
parent and attachment. The retained failing case is plain NoSync, physical cut
14, lost acknowledgement: `debug-io-repro14.log` and `io-recovery-gdb.log` under
`build/pouch-space-leased-expiry/`.

Cleanup now forces participant stages durable before writing any terminal
decision bytes. Both owners and both root policies share this ordering. A later
decision barrier cannot establish that prerequisite after the fact. The same
isolated failure passes after this correction (`debug-stage-fence-repro14.log`).

The formatted harness passes all eight physical I/O profiles, totaling 2,304
cuts across both policies, four codecs and warm/reopened owners. Each profile
injects hard errors, lost torn writes, persisted torn writes and complete writes
whose acknowledgement fails. Every trial independently checks its durable seed
image, freezes persistence before owner close, restores only selected durable
bytes, and verifies recovery with the logical clock moved backward. Committed
cleanup must finish; an undecided cleanup must preserve the original parent and
attachment before their deadline. Later expiry removes both, retires work and
permits recreation with a larger fence. Poisoned admission performs no I/O.
All development containers remain exactly 10 MiB.

The concurrency probe passes 32 cases across both owners, policies and codecs,
pausing cleanup at STAGED or DECIDED. An independent namespace writer commits
while cleanup is held. A second writer supplies a 512 KiB JSON value from a
bounded producer and pauses halfway through; cleanup, including the new stage
barrier, completes before that producer reaches EOF. Physical payload progress
before the pause proves container streaming. A bounded checking sink verifies
every decoded byte afterward. The test explicitly excludes documented xid stripe
collisions and leaves unrelated writer heartbeat clocks real.

The coordinated probe passes 64 cases: two participant commit votes, rollback
votes, lost decision acknowledgement, and recovery with forward/backward logical
clocks. It checks staged attachment visibility, immutable commit-derived
retention deadlines, and expiry's ownership of only its own cleanup journal.
The existing native-only startup policy applies and retires terminal transaction
journals; managed external outcomes have separate policy and remain outside
this probe. The normal-allocator comparison also passes all 160 paired Debug
cases; Release performance qualification is still pending.

Evidence is `debug-fence-regression.log`, `debug-final-harness.log`,
`debug-final-io-compare.log`, `debug-concurrency.log` and `debug-coordinated.log`
in `build/pouch-space-leased-expiry/`. The first comparison attempt exposed a
fixture allocator mismatch, corrected by cleaning read results with their
store's actual allocator. The failed log is retained; it is not a passing gate.
The complete Debug build and all **528 standard ASan/UBSan groups**, including
476 unit groups and all 84 new lease groups, pass in **8857.58 seconds**.
The allocation matrix covers 64 profiles and **30,124 injected cuts** across
both owners, both policies, all four codecs, warm/reopened stores and
nullable/non-null error output. Every selected cut must fire; each trial verifies
both warm and cold retry. The full run also passes all 2,304 physical I/O cuts,
88 process-death cases, 32 concurrency cases and 64 coordinated transaction
cases. GNU Debug workers use 128 KiB stacks with the glibc stack cache disabled.
The complete build, summary and detailed output are `debug-standard-build.log`,
`debug-standard.log` and `debug-standard-details.log` in the same directory.
Native GNU/musl, TSan, Memcheck and Release comparisons remain required for the
completed integration batch. GNU Release uses ordinary platform worker stacks; the
Debug-only initializer does not enforce a Release stack size.

## Public collection owners

The experimental SDK now admits collection create/open/list, heads,
direct/staged/push append, streaming scan/LQL reads and cursors, deletion,
retention metadata, atomic edits and retry receipts, leases/takeover/private
reads, and drop/recreate. Positions, generations, ownership rules and receipts
remain in the production logical engine. The native file adapter uses the same
engine. Normal public Pouch still opens the multifile backend; this stage adds
no public backend selector or deployment cutover.

Catalog names select immutable collection incarnations. Within a root, stream
key zero holds the shared 320-byte catalog identity; control key one holds the
shared 384-byte collection head. Segment and snapshot child bindings protect
their canonical owners. Dependencies and any initial lease precede publication
of the namespace's collection name. Missing namespaces list as empty, and old
receivers cannot address a recreated namesake. One cached physical owner admits
each incarnation; snapshot, publication and upload pins use counted gates.
A pin can outlive an authority scope and move between threads without retaining
a thread-owned pthread lock.

The shared engine uses typed canonical bytes for metadata changes, deletion,
encoded relocation and codec units; body/history/query/retention/edit replay and
compaction inputs are pinned by segment identity. Native pins retain file
descriptors without heap allocation during streaming rollover. Disposable memfd
order/ID projections use the existing index algorithms, including bounded
entry-at-a-time growth. No application body is buffered or spooled into them.
Receivers sharing a live physical owner share its projection epoch. Reopening
the physical owner reconstructs these projections with a new epoch, invalidating
prior cursors and head preconditions.

Private ranges map directly onto owned runs and survive segment rotation.
Container reservations are bounded to 1 MiB; values can span multiple ranges.
Completed-tail readiness is registered under append authority; its later
durability wait does not sample a subsequent producer's working tail. Final
private bytes in an older sealed segment are stabilized before publishing their
reference. Recovery validates the owned checkpoint prefix before clearing
anything, then applies the existing container policy to an incomplete or invalid
uncheckpointed suffix. Checkpoint-prefix corruption remains an explicit error.

Dropping or abandoning a closed owner reclaims all child catalog bindings,
including segments absent from its cache. An orphan claim excludes a new parent
name binding while reclamation runs. Captured views close before target
retirement; children precede control and identity retirement. A failed pass
retains the unbound identity as the retry anchor. All development containers
remain exactly 10 MiB; growth stays explicit.

Qualification evidence is in `build/pouch-space-collections/`:

- The complete Debug build passes. The retained `surface-debug-gate.log`
  records 82 passes and two native allocation regressions; it is not a passing
  gate. Both isolated repairs pass in `surface-native-pin-repair.log`.
  All 30 affected native/SDK/Lua groups subsequently pass in
  `surface-debug-affected.log`. The unchanged catalog/root/namespace and lease
  groups retain their passing results from the broader run.
- All 18 final container groups pass in `surface-debug-container-final.log`
  (259.76 seconds). They cover every codec and both policies, generated
  1.25 MiB values, actual payload output before producer EOF, positions,
  duplicate/deleted retry identities, edits, recovery descriptors, retention,
  private leases/takeover, cursors/LQL, abort, drop/recreate and reopen.
  Eight process-death cases, 48 interrupted-edit cases, independent producers
  across rotation, and upload handoff across threads are included.
- The final matrices inject **2,016 physical I/O cuts** and **1,274 allocation
  cuts**, with every selected cut required to fire. I/O covers create,
  streamed append/rotation, edit and drop, including hard errors, lost and
  persisted torn writes, and complete writes with lost acknowledgement.
  Each trial restores only the modeled durable bytes before reopening through
  the SDK. Allocation coverage includes create, append, edit, acquire, read,
  cold owner admission and listing, with warm/cold reconciliation and retry.
  Detailed output is `surface-debug-container-final-details.log`.
- GNU and musl each pass 16 selected groups, including the native allocation-free
  streaming/conversion regressions (`surface-gnu.log`, `surface-musl.log`).
  After the pin-lifetime correction, both pass the 10 affected container and
  comparison groups again (`surface-{gnu,musl}-container-final.log`).
  Debug and TSan GNU workers use 128 KiB stacks with the stack cache disabled;
  musl uses its default worker stack. GNU Release uses ordinary worker stacks.
- TSan passes the final SDK probe, the canonical storage probe and 16 matched
  four-writer comparisons, without suppressions. The binaries use the checked
  instrumented OpenSSL 3.6.5 archives. Logs are
  `surface-tsan-complete-sdk.log`, `surface-tsan-pins-storage.log` and
  `surface-tsan-comparisons.log`.
- Memcheck passes the complete live SDK surface (excluding intentional process
  death) and canonical storage probe: **59,447 allocations/frees**, zero live
  allocations, zero errors/suppressions, and only the three inherited file
  descriptors at exit. Logs are `surface-memcheck-{sdk,storage}.log`.

The normal-allocator GNU Release comparison has **96 rows / 48 matched pairs**:
three samples of 24 x 4 KiB or 8 x 64 KiB entries, one/four independent
collections, both policies, and newly created/reopened owners. OS page cache is
retained; these are development measurements, not cold-device or production
performance claims. Raw results are `surface-paired-perf-final.log`.
Per-configuration medians of paired write speed ratios span **0.66–1.86x**
native for Durable (median 1.54x); four-writer sync counts fall from 24 to
12–14, or from 8 to 4–6. Reopened NoSync writes are **3.29–15.06x slower**,
with one/two syncs versus native's zero. Read speed ratios span 1.02–2.02x
(median 1.31x); reopen costs vary. Container projections allocate 12 KiB per
collection / 48 KiB for four in these workloads, reported separately from the
single persistent file; native stores have 23–61 files. The reopened NoSync and
projection-rebuild costs remain optimization work.

The owner-admission batch above did not yet admit collection maintenance; the
following stage adds explicit passes. Indexed queries, background collection
retention and shared-root operation remain outside the admitted call contract.
The standing maintenance-capacity proposal still awaits an explicit decision.
Existing-root conversion, Darwin/QEMU and large-machine qualification remain
deferred, and production cutover still requires a deployment decision.
The additional GNU lease sweep was stopped at 31 passing groups when development
changed to larger integration batches; it is not a completed gate.

## Collection maintenance

The private `ps_pouch_collection_maintain` entry now runs the production
collection retention engine, compactor and obsolete-storage cleanup, followed
by the existing container metadata checkpoint. It supports forced compaction
and cleanup-only passes. Normal public Pouch still selects the multifile
backend. No public selector, application outcome log, persistence-version bump
or deployment cutover is introduced.

The common compactor now obtains output, sealing, projection publication,
discard and cleanup through typed storage owners. Encoded bodies and codec
units are copied with the existing bounded 128 KiB heap buffer; receipt,
generation, position, query-identity and cardinality rules remain in the common
engine. Snapshot parts are stabilized before canonical head selection.
Disposable order/ID projections retain the common recovery algorithm. Native
compaction retains independent stream-lock descriptions, including when the
receiver itself has an admitted upload.

The container owner serializes compaction with a lifetime gate. Capture rotates
the tail and pins its read generation; copying proceeds outside append
authority. Foreground appends enter the new tail, and publication replays that
tail before selecting the snapshot. An upload that starts during copying can
remain unfinished through snapshot publication and the physical checkpoint,
then commit normally. An upload already holding private storage excludes initial
capture. Concurrent entry-retention changes defer the attempt for retry.

Obsolete segments retain their original grace timestamp through repeated
compactions, clock rollback and reopen. A `PS_CATALOG_CONTROL` alias names the
same canonical stream with 16 bytes: the segment identity and first-obsolete
Unix timestamp, each little-endian 64-bit. Existing catalog checksums protect
the metadata. Removal retires the canonical binding, then its alias, then the
physical stream. A marker-only interrupted removal remains discoverable, without
an independent marker control that could be stranded. Closed collection-owner
reclamation captures both regular bindings and aliases, closes catalog views,
and reclaims their unique targets before the control/identity anchor.

Old captured read generations defer obsolete storage retirement; current
generation readers do not pin unrelated old storage. Raw catalog views also
defer retirement. Failed physical retirements retain an owner-cache identity
for retry after the last name is removed. Uncached obsolete targets admit only
their physical identity and do not replay bodies. The final metadata checkpoint
releases extents retained by the previous recoverable root. Repeated compaction
tests assert actual retired-byte clearance and reuse within the unchanged
10 MiB container, including private leases and an all-deleted journal.

Qualification artifacts are in `build/pouch-space-collection-maintenance/`:

- The Debug build and 38 affected native/container/SDK/Lua groups passed. The
  checkpoint-inclusive surface then passed 32 groups: eight native/container
  comparison cases and 24 maintenance behavior/fault groups. This includes
  **2,672 I/O cuts and 1,004 allocation cuts**, all four codec combinations,
  both durability policies, and failure/process-death recovery at all five
  publication phases. `surface-debug.log`, `surface-final-debug.log` and their
  `-details.log` files retain the results. An additional eight behavior groups
  passed after adding the unfinished-upload overlap and competing-compactor
  checks (`surface-overlap-debug.log`); product sources were unchanged.
- GNU Release and musl Release each passed 20 affected groups, including the
  native collection regressions and maintenance/comparison cases
  (`surface-gnu.log`, `surface-musl.log`). Debug used the actual 128 KiB worker
  initializer with the glibc stack cache disabled; musl used default worker
  stacks. GNU Release used ordinary platform worker stacks.
- ThreadSanitizer passed all eight live maintenance configurations, including
  peer appends during copying and upload overlap, with linked instrumented
  OpenSSL 3.6.5 and no suppressions (`surface-tsan.log`, `relink-tsan.sh`).
  Memcheck passed plain and encrypted/compressed maintenance under both policies:
  **73,296 allocations/frees**, zero remaining blocks/errors/suppressions, and
  only three inherited descriptors (`surface-memcheck.log`, `memcheck-*.log`).
  Intentional process-death cases are excluded from these live memory checks.

The normal GNU Release comparison records 96 verified rows: 48 matched pairs,
three samples over 16 configurations. Workloads are 24 x 4 KiB or 8 x 64 KiB,
one/four independent collections, both policies, and new/preopened owners.
Both engines use 64 KiB segment targets, zero deletion grace and no throttle;
the container timing includes its final physical checkpoint. Engine order
alternates in the middle sample. OS cache is retained, so these are development
observations rather than cold-device or production timing claims. Raw results
are `surface-paired-performance.log` and the driver is `paired-performance.sh`.
Per-configuration medians of paired native/container compaction-time ratios
span **1.15-1.76x** (median **1.44x**); corresponding CPU ratios span 1.07-1.72x.
The container makes **20-104 sync calls**, versus native's 17-68, and retains
one persistent store file versus 27-77 native files. Higher barrier counts remain
optimization work despite the favorable timings in this workload.

This stage preserves the existing full-snapshot compaction algorithm. Copying
still needs foreground free capacity; there is no implicit growth or guarantee
of maintenance admission at full capacity. The incremental-checkpoint and
protected-capacity proposal above still awaits a decision. Background discovery
and retention are integrated in the following stage. Indexed queries,
shared-root operation and the remaining queue/external-XA integration remain
unfinished. The complete backend goal is not achieved by this maintenance stage.

## Background and namespace maintenance

The experimental owner now admits the common background scheduler and public
`lc_pouch_maintenance_run` namespace pass. Typed storage callbacks own durable
work discovery and physical state/collection maintenance. The common engine
retains retention policy, explicit age sweeping, force/cleanup-only semantics,
cadence, coalescing, fairness and worker lifetime. Native roots retain their
four existing work sources; container roots add a separate durable ordinary-state
source, so continuous volatile queued writes cannot starve cold ordinary work.
Opening arms existing work without waiting for a new mutation. Close joins the
worker before releasing physical ownership; maintenance receivers retain their
physical owner without retaining the SDK client being closed.

Ordinary, terminal and collection hints use `PS_CONTROL` slots 4, 5 and 6 under
the immutable namespace owner. Each value is exactly 32 bytes: `LCPSWORK`, owner
identity, work kind and a CRC32 stored in a 64-bit field. Hints precede physical
work and are durable under both root policies. They are advisory discovery
metadata, not decision authority or another outcome log. Discovery selects
metadata presence without reading hint values, ordinary segments, collection
bodies or resident projections. Consumption validates the selected hint.
Malformed contents therefore fail their namespace pass while the lexical
cursor can continue to healthy namespaces. Retention discovery uses the same
presence-only policy; its existing consuming codec is unchanged.

The namespace pass shares ordinary expiry/age policy with the common engine,
then applies physical compaction thresholds and common collection retention and
compaction. Ordinary copying uses bounded buffers outside namespace authority;
live staged records and unfinished physical producers defer ordinary compaction
while collection work can progress. Terminal hint retirement rechecks the
current tail under namespace authority. Collection discovery captures durable
catalog names and identities, then revalidates revision/target before admitting
one receiver at a time. Dropped or replaced names cannot redirect captured work.
An empty namespace retains its hint while an unbound target remains pinned or
open. Reclamation and the final checkpoint release retired physical capacity.

Two integration defects are corrected in this surface. Orphan cleanup now
accepts an already absent collection head, allowing retry when head deletion
succeeds but identity retirement fails. A peer's disposable projection can also
lag a compactor's selected snapshot, including when an active upload prevents
ordinary preparation. The container refreshes that receiver's projections from
the captured canonical topology under metadata authority. This uses the common
bounded replay engine without acquiring append authority, repairing/truncating
canonical storage, or changing the producer's private suffix. Deterministic
tests hold append authority and repeat peer head/body/status reads inside a real
producer callback; the completed upload and prior body survive reopen.

Qualification artifacts are in `build/pouch-space-background/`:

- The initial 95-group Debug qualification and targeted test-expectation
  corrections have passing evidence in `surface-debug.log`,
  `surface-debug-corrections.log`, `surface-debug-retention.log` and their
  `-details.log` files. The peer refresh repair then passed **55 affected groups**
  across `surface-peer-focused.log` and `surface-peer-debug.log`, including
  **3,808 I/O cuts and 1,954 allocation cuts**. Their detail logs retain per-codec
  counts. This covers namespace passes, first hint publication, interrupted
  drops, peer refresh failures, native/SDK/Lua collection behavior, cold
  discovery, malformed hints, expiry during continuous writes and close during
  active copying. Debug used ASan/UBSan, the actual 128 KiB worker initializer
  and disabled glibc stack caching.
- GNU Release and musl Release initially passed 23 groups each; after the peer
  repair each passed the 20 affected collection/maintenance/comparison groups
  (`surface-final-gnu.log`, `surface-final-musl.log`). GNU used ordinary platform
  worker stacks; musl used its default worker stacks.
- ThreadSanitizer passed all eight live codec/durability configurations after
  the repair, with linked instrumented OpenSSL 3.6.5, actual 128 KiB workers and
  no suppressions (`surface-final-tsan.log`, `relink-tsan.sh`). Its earlier run
  exposed the stale-peer projection defect above; the deterministic regression
  is now included in every configuration.
- Memcheck passed the plain and encrypted/compressed live paths under both
  policies: **81,629 allocations/frees**, zero remaining blocks, errors or
  suppressions, and only three inherited descriptors (`surface-memcheck.log`,
  `memcheck-*.log`). Intentional process-death tests are excluded from live
  sanitizer/memory checks; cold interrupted-drop recovery runs in Debug and
  release behavior checks.

The normal GNU Release comparison records **96 verified rows / 48 matched
pairs**, three samples over 16 configurations. Both owners run the same public
namespace pass over one/four independent collections, with 24 x 4 KiB or
8 x 64 KiB workloads, both root policies and new/preopened owners. Both use
64 KiB segment targets, zero deletion grace and no throttle; container timing
includes the final physical checkpoint. The middle sample reverses engine order
and OS cache is retained. Raw results and driver are
`surface-paired-performance.log` and `paired-performance.sh`. Per-configuration
medians of paired native/container maintenance-time ratios span **1.13-2.68x**
(median **1.44x**); CPU ratios span 1.00-1.96x (median 1.34x). The container uses
**20-95 sync calls**, versus native's 17-70, and retains one persistent file
versus 27-77 native files. These are development observations, not cold-device
or production timing claims. Higher barrier counts remain optimization work.

This surface does not change full-snapshot capacity requirements: maintenance
may still need foreground free space, and there is no implicit growth or
guaranteed admission at full capacity. The incremental-checkpoint/protected
capacity proposal remains unapproved. Indexed queries, queues/outbox, complete
external XA, resumable codec conversion, shared-root coordination/recovery and
deployment acceptance remain outside this completed maintenance surface. The
complete backend goal remains active.

## Queue and outbox ownership

The private container SDK now runs public queue enqueue/dequeue, ack/nack,
visibility and abandoned-delivery recovery, TTL/retry terminals, batch/FIFO and
cursor behavior, paired state leases, transaction decisions/recovery,
subscriptions and queue watches through the existing logical engine. Outbox
integration includes duplicate validation, transactional effects, stateful
delivery/checkpoints, completion and deferred checkpoint cleanup,
dead-letter/replay/delete, inbox and command receipts, terminal retention and
scan-based dispatcher recovery. This surface admits exclusive physical
ownership and explicitly selected scan queries. It establishes no shared-root
or indexed-query support, and normal public open still selects multifile Pouch.

`lc_pouch_outbox_store` is the narrow physical boundary for five bounded marker
families: state reclaim and dead-letter/completed/inbox/command retention.
Common code retains the existing `LOR1`/`LDR1` codecs, lexical cursor/wrap policy,
deadline filenames, bounded dead-letter inspection, metadata-only terminal
inventory and generation checks. State markers are at most 4 KiB; retention
markers are at most 8 KiB. These values contain cleanup keys and policy metadata,
never application payloads or transaction decisions. The native adapter retains
its existing directories, atomic temporary-file publication and family-specific
sync/absent-directory behavior. The previous filesystem-specific marker block
has been removed from `lc_pouch.c`.

Container names are namespace-owned `PS_CATALOG_CONTROL` entries: one binary
kind byte followed by the exact marker filename. Replacement allocates a fresh
immutable owner and empty sealed stream identity with key `UINT64_MAX`. Its
bounded body lives in `PS_CONTROL` slot 1. A separate 32-byte owner tag in slot
`UINT64_MAX` contains `LCPSOBX1`, owner, identity incarnation and CRC32 stored in
a 64-bit field. Publication orders identity, tag, body and catalog binding;
replacement checks the prior catalog revision. Marker publication/removal is
durable under both root policies. Ordinary namespace work is anchored before
resource creation so cold maintenance can discover interrupted cleanup.

Catalog inventory captures names without admitting logical namespace state or
reading marker bodies; the common selector controls bounded body admission.
Reads pin the selected binding. Removed/replaced owners remain available while
catalog views are pinned, then ordinary orphan cleanup retires body, tag and
identity. Tags distinguish these children from ordinary canonical stream owners
whose numeric stream key happens to match. Interrupted construction and cleanup
remain retryable. Same-name writers use a short registry admission plus a
per-marker wait; unrelated marker identities and namespaces can publish while a
same-name writer is blocked. No registry mutex spans physical publication.

Queue watches use per-waiter monotonic conditions scoped to the physical owner,
namespace and queue. Notifications wake all matching waiters without files or
unrelated-queue wakes. The common 100 ms canonical rescan fallback covers a
notification arriving before registration; notifications remain advisory.
Status reports `inprocess` / `container_notifications` when enabled.

Qualification also corrected two lifecycle issues. An existing test changed
`client->is_pouch` while its dispatcher read that immutable identity; the
rejection check now uses a real remote client without requesting network work.
ThreadSanitizer then exposed a real worker-exit race: dispatcher wait could
finish before the detached worker's JSON TLS destructor. The worker now drains
that runtime before publishing exit, so successful wait covers its owned JSON
resource. The existing worker-release wait regression is included in the
container behavior matrix.

Evidence is in `build/pouch-space-queue-outbox/`:

- The initial **75-group Debug** affected pass includes native queue/outbox
  controls, all codec/durability combinations, collection/background and lease
  regressions, **554 marker I/O cuts, 568 allocation cuts and 538 real process
  exits**. Failure cases verify cold old/new/absent outcomes, successful durable
  outcomes, retry and orphan-cleanup convergence. Stable-image tests freeze
  before close so teardown cannot repair the modeled failed prefix. The TLS
  repair subsequently passed **19 affected groups**, including the full native
  outbox suite, eight outbox configurations and lifecycle comparisons. Logs are
  `surface-debug.log`, `surface-final-debug.log` and their detail logs. Debug
  used ASan/UBSan, actual 128 KiB workers and disabled glibc stack caching.
- GNU and musl Release each passed **49 groups**, then **18 affected groups**
  after the TLS repair, plus two native shutdown cases. The current SDK matrix
  reuses 20 original queue cases and 22 original outbox cases across eight
  codec/durability configurations, with two additional notification cases per
  configuration. GNU uses ordinary platform worker stacks; musl uses its default
  worker stacks. See `surface-{gnu,musl}.log`, `surface-final-*.log` and
  `native-shutdown-*.log`.
- ThreadSanitizer passed three direct marker/notification/concurrency modes and
  all eight live SDK configurations, including the worker-release regression
  and notifications: **27 invocations**, with linked instrumented OpenSSL 3.6.5,
  actual 128 KiB workers and no suppressions. `surface-tsan.log`,
  `check-tsan.sh` and `relink-tsan-drain.log` retain the final evidence; earlier
  logs retain the fixture and TLS findings above.
- Memcheck passed **11 selected live invocations / 605,570 allocations and
  frees**, with zero remaining blocks, errors or suppressions and only three
  inherited descriptors. These cover direct marker ownership, notification and
  writer concurrency, plus representative plain and encrypted/compressed queue
  and stateful outbox paths under both policies (`surface-memcheck.log`,
  `memcheck-*.log`, `check-memcheck.sh`). Intentional process-exit cases are
  separate from live memory/sanitizer checks.

The normal GNU Release comparison records **192 verified rows / 96 matched
pairs**, three samples over 32 configurations: queue or stateful outbox,
24 x 4 KiB or 8 x 64 KiB, one/four independent worker namespaces, both root
policies and new/preopened owners. Both use scan queries, disabled background
maintenance/watches and 1 MiB segment targets; the container starts at 10 MiB.
Producers emit bounded 4 KiB chunks and consumers verify every byte. Queue
workflows enqueue, deliver and ack. Outbox workflows append/commit, dispatch,
update paired checkpoints and complete. Timing includes joined workers,
dispatcher stop and matching public cleanup-only namespace passes; container
passes include the final physical checkpoint. Close is reported separately.
Reopen verification checks drained queues and duplicate completed effects.
Creation and reopen verification are outside the completion measurement. The
middle sample reverses engine order; OS cache is retained. These are development
observations, not cold-device or production timing claims. Raw results and
driver are `surface-paired-performance.log` and `paired-performance.sh`.

The primary completion comparison adds reported `lifecycle_ms` and `close_ms`,
because the owners distribute cleanup costs differently. Per-configuration
medians of paired native/container ratios are:

| Workflow | Completion including close | CPU of work/cleanup phase | Native persistent files |
| --- | --- | --- | --- |
| Queue | 1.23-2.53x, median 1.75x | 1.05-1.78x, median 1.35x | 11-23 |
| Stateful outbox | 0.66-1.60x, median 1.16x | 1.18-1.66x, median 1.42x | 42-99 |

The container retains one persistent file in every measurement. CPU excludes
close. Work/cleanup-phase wall ratios alone span 0.34-1.77x for queues and
0.54-1.55x for outbox; the separate close cost matters, especially for NoSync
queues. **NoSync outbox completion regresses in seven of eight configurations**:
the median native/container ratio is 0.82x and the worst is 0.66x, approximately
52% more container time. Durable-sync outbox completion ratios are 1.12-1.60x
(median 1.35x). Measured work/cleanup-phase sync calls are container 9-179 versus
native 2-152 for queues, and 77-915 versus 72-661 for outbox; these counts exclude
close. The NoSync outbox regression and marker barrier costs remain optimization
work before deployment acceptance.

This surface does not change the unapproved incremental-checkpoint/protected
capacity proposal or promise maintenance admission at full capacity. Indexed
queries, complete external XA, resumable codec conversion, shared-root
coordination/recovery and deployment acceptance remain unfinished. Existing-root
conversion remains outside the initial implementation. The backend goal stays
active.

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
snapshots, staged/prepared mutations and the SDK call envelope above. Leased
state, expiry, public collection ownership and explicit/background maintenance
use the typed owners described above. Live stages defer ordinary compaction;
indexed queries and complete external XA still require
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

Remaining integration includes indexed-query owners, complete
external XA ownership and shared-root coordination. Core collection positions
and retry receipts now
run through the container owner above. Normal public Pouch tests continue to
exercise the segmented backend through the shared typed storage boundaries.

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
