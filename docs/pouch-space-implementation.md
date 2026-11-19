# Native container implementation contract

The full native x86_64 backend is authorized for staged implementation on
`experiment/pouch-space`. Normal Pouch still uses the multifile backend until
integration and acceptance are complete. The measured foundation remains the
comparison reference, not a second permanent production backend. Existing-root
conversion is outside this implementation; production deployment still requires
an explicit decision. Darwin, QEMU, 250 MiB and larger measurements remain
deferred. Initial capacity stays 10 MiB, with explicit 1 MiB growth tests.

Current milestones are the [incremental canonical key checkpoint cutover](#incremental-canonical-key-checkpoint-cutover)
and [bounded namespace piece integrity](#bounded-namespace-piece-integrity).
Sparse mapping/reclamation, standing admission with root-wide reconstruction,
codec conversion and process-shared roots remain unfinished. Earlier snapshot
sections below retain the evidence and constraints of their implementation stages.

## Single-file architecture direction

The renewed implementation mandate authorizes redesigning all experimental
internals to fit one physical container. The priorities are Pouch and lockd's
lease-and-lock, coordination and transactional-messaging workloads, with
correctness, bounded resource use and measured performance governing the
design. The multifile implementation remains a behavioral and performance
reference; its file organization, indexing algorithms and maintenance schedule
are not requirements for the new backend. Production cutover still requires
an explicit deployment decision.

The selected maintenance direction is incremental canonical metadata
checkpoints, bounded relocation of encoded body ranges and a computed standing
capacity obligation, as specified in
[the maintenance architecture](#selected-maintenance-architecture). The earlier
request for separate architecture approval is superseded by this mandate.
This authorizes experimental persistence changes; it does not claim that these
mechanisms are implemented or their capacity proof is complete.

### Preserved behavior and permissible tradeoffs

Preserve lease ownership and expiry, fencing/preconditions, transaction and
staged-decision atomicity, retry/idempotency receipts, queue ordering and
acknowledgement rules, outbox/inbox and XA recovery. Preserve public generation,
key identity, visibility, cursor and query-result semantics unless a specific
observable change has been explained and accepted. A durable acknowledgement
must survive recovery; NoSync cannot expose an invalid partial canonical group.
Pinned reads retain their captured bytes through replacement and reclamation.
Application values remain genuinely streamed, including codec and relocation
paths. Producers do not run under root or allocator publication authority.

Derived query state is rebuildable, but this does not permit a stale explicit
indexed query, silently replacing its engine with a scan, or indexing an
intermediate version inside an active public lease/transaction. The existing
state-index sequence and public completion boundaries remain the initial
correctness contract. Optional indexing failure cannot revoke an already
committed canonical mutation. Any proposed change to freshness, flush modes,
index scheduling promises, cursor lifetime or failure behavior must identify
the affected use case and its measured benefit before adoption.

Internal structures, layout, algorithms, batching, cache ownership and work
scheduling can change directly within those invariants. Recreate private
experimental fixtures when necessary; do not add compatibility layers or bump
an independent format marker merely because internals change. No implicit
growth, external persistent staging, reduced accepted key limits or production
backend selector is introduced by this direction.

### Publication and indexing

Use the container's physical publication domain to select a complete logical
revision. Canonical authority and rebuildable query state must remain distinct;
derived indexing must not become a second transactional database or force a
canonical operation through a simulated filesystem. Prepare private outputs
outside publication authority, validate their captured canonical boundary, then
select the complete revision. Independent namespaces must be able to prepare
work concurrently and share durability work without waiting for unrelated
producers or acknowledging incomplete prefixes.

The namespace writer packs document/posting components into one generation.
The original container adapter selected that generation and its manifest
separately through stream sealing and catalog selection, with 6-9 measured
barriers per writer and root-wide flush serialization. The implemented
[namespace revision owner](#namespace-index-revision-owner) now prepares both
values privately and selects them together. The measurements isolated this
publication cost; they did not establish that inverted indexes are incompatible
with one file.

Choose further generation bundling, incremental postings and cache reuse from
measured build, selection and read costs. Retaining current artifact interfaces
is not an acceptance condition. Preserve the matched baseline and separate
flush, first query pass, repeated query pass, close and cold reopen so that
changing the algorithm cannot hide deferred work.

### Implementation and acceptance order

1. Separate the existing performance evidence into flush/publication and query
   read phases. Establish which costs follow barriers, root serialization,
   encoding or loading before attributing the full gap to one algorithm.
2. Land one complete indexing publication/reader/recovery surface, qualifying
   its observable behavior and comparing the same verified workloads. Include
   steady repeated updates and multiple namespaces, not just initial builds.
3. Replace full namespace snapshots with incremental canonical checkpoints,
   then add bounded encoded-range relocation and standing admission. Admission
   must fund outstanding work from store creation; it cannot depend on finding
   room for a first full snapshot after the store fills.
4. Complete resumable codec conversion and process-shared publication,
   ownership and reader recovery against the same canonical/physical authority.
   Exercise actual lease, coordination and messaging workflows across processes.
5. Qualify the resulting backend as a whole and present the deployment decision.

Use coherent implementation batches and run the applicable qualification once
per completed surface. After failures, prove the repair narrowly before the
affected broader rerun; retain unrelated valid evidence. Existing correctness
suites constrain behavior, not the internal layout they originally exercised.

Performance acceptance must report foreground latency/throughput, explicit index
readiness, first and repeated query costs, CPU, write and sync amplification,
usable capacity and worst maintenance pauses. Report close and reopen separately.
The working target is comparable performance on the core coordination workloads
with no unexplained order-of-magnitude indexing penalty. No numerical production
SLO or blanket permission to trade away behavior is implied. A measured remaining
regression needs an explicit use-case tradeoff before deployment acceptance;
faster close cannot compensate for slow foreground work. Include failure/retry,
fixed-capacity churn and shared-root operation before calling the backend ready.

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

## Standing maintenance capacity: selected direction, implementation pending

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
planner at the whole-snapshot milestone allowed 4,032 canonical bytes per 4 KiB
page, accounting for the then-64-byte physical run header, and 88 descriptor
bytes per page in its fragmentation bound. These gave the following historical
lower bounds, excluding routing, stream heads,
catalog entries and checkpoint workspace:

| Obligation | Canonical bytes | Planned payload bytes | Planned descriptor bytes |
| --- | ---: | ---: | ---: |
| One 60 KiB body-key tombstone | at least 61,510 | at least 65,536 | at least 1,408 |
| One 1 MiB placeholder in a snapshot | more than 1,048,608 | at least 1,069,056 | at least 22,968 |

These were conservative admission bounds from `ps_budget_plan_span`, not measured
physical consumption or a complete reserve recommendation. The current 96-byte
prologue permits 4,000 canonical bytes per page. Typed checkpoint metadata now
uses `ps_budget_plan_value`; these historical snapshot figures are not the
current standing-reserve proof. The public key-bound
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

At that milestone, every snapshot serialized the complete resolved key metadata,
including keys whose bodies remained elsewhere. Protecting another complete
snapshot therefore required workload-sized spare capacity. In a metadata-dominated
namespace, old and new metadata must coexist until durable selection and
retirement; the resulting upper bound on usable capacity can approach one half
of the container before other overhead. This is a design bound, not a measured
typical utilization figure. The source-sized body copy was a separate
workload-sized obligation.

The selected direction is bounded incremental metadata maintenance and body
relocation before selecting a standing reserve. The renewed single-file mandate
authorizes this internal persistence redesign. Incremental checkpoints are now
implemented below; bounded relocation and standing admission remain unfinished.
The design must define foreground publication as well as recovery: splitting
only the output of a complete snapshot does not remove
its bootstrap requirement to retain all prior metadata. It must specify the
authoritative representation, atomic multi-key/high-water publication, the
largest indivisible work unit, and progress without a full-namespace rewrite or
a complete source-body copy. Preserve canonical behavior, true streaming,
independent producers, pinned readers and the existing durable commit contract.

Retaining complete snapshots would instead require accepting and exposing a
workload-sized reserve and its usable-capacity cost. That is not the selected
direction. No silent growth, external persistent staging, reduced key limits or
weaker durability is authorized.

Verification must fill foreground capacity
at fixed size, then prove deletion and reclaimable maintenance can proceed once
reader pins drain. An all-live store may remain full. Include maximum accepted
keys and metadata, multiple namespaces, repeated maintenance/deletion cycles,
fragmentation and allocation/I/O/torn-write failures. Reserve accounting must
protect every still-recoverable state under NoSync rollback as well as durable
publication, and must recover after process death before readmission. Measure
usable capacity, write amplification, CPU, durable barriers and maintenance
latency against the frozen multifile reference. No reserve number is selected
by this analysis.

### Selected maintenance architecture

This is the selected internal architecture, not an implementation claim.
Replace complete experimental namespace snapshots with incremental canonical
metadata checkpoints, and replace
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

##### Checkpoint representation and publication

The implementation cutover uses `PS_KEY_METADATA` values for resolved ordinary
and staged key metadata, `PS_KEY_DECISION` values for retained decisions and a
`PS_NAMESPACE_CHECKPOINT` coverage/high-water control. Slot IDs come from the
existing root ID allocator and remain distinct from public generations and body
lifetimes. Exact key lookup is resident; checkpoint values retain complete keys.
Small values already share the physical allocator's 64-byte cells, so this uses
the existing typed directory and packed allocator rather than another page tree.
Metadata encoding may materialize its bounded canonical fields; application
bodies remain references to their streamed encoded spans.

Use bounded publication batches of changed slots. Intermediate batches retain
the previous coverage boundary and all uncovered canonical history. Replay seeds
those advanced per-key entries and still replays complete uncovered groups using
the production supersession and query-generation rules. Only the final batch
advances coverage and both high waters after every changed slot in that capture
is durable. This avoids imposing the physical revision frame's descriptor limit
as a new public transaction limit. Large indivisible groups still require their
complete funded metadata obligation. No history may be reclaimed merely because
one intermediate batch selected some of its keys.

Namespace views capture immutable checkpoint incarnations and canonical tails
coherently: no checkpoint entry may seed a view whose captured canonical prefix
precedes that entry. A metadata-only value capture pins entry descriptors without
allocating a 16 KiB payload buffer for every key. Replay opens one captured value
reader at a time, decodes its canonical metadata and releases that reader; the
capture continues to retain the old incarnation through replacement/removal.
Stable `snapshot-<slot ID>.log` labels address these typed metadata capabilities,
not files or catalog aliases. The namespace read boundary supplies their captured
value input. Canonical body source labels and crypto contexts retain their
existing meaning until the separate bounded mapping cutover.

Fund a delta before its body producer runs. The canonical prefix writer already
supplies header, key and metadata together. Its private append admission hook
uses that structured prefix to reserve the checkpoint bound, deduplicating keys
within a group. PENDING finalization determines the next record boundary;
application body chunks are never interpreted as records. Before canonical
commit, decode the completed private metadata, resolve it with the same pure
production projection used by the resident cache and prepare the dirty update.
Accepting the canonical append must not introduce a new allocation failure that
revokes an already committed mutation.

A namespace budget retains the unspent checkpoint obligation. Atomic resize
preserves existing claims on failed growth, permits growth alongside funded
writers and forbids shrinking their live claim. After a batch, settle the claim
to remaining changed work so replacement churn cannot accumulate unused credits.
Recovery reconstructs the outstanding delta and its capacity claim before write
readmission. This is separate from the still-required standing deletion and
relocation reserve; the existing fixed floor is not its proof.

Publishing a checkpoint updates only matching changed keys' resident metadata
references, using plans allocated before physical selection. A newer canonical
suffix must retain its own reference. Do not invalidate/replay the complete
resident namespace after every checkpoint. Retained stages and decisions remain
recoverable through the same common resolver, rather than globally deferring
checkpoints while a transaction is live.

The namespace owner now implements these mechanics, including bounded batches,
private preflight, per-key cache-reference updates and recovery from coverage.
Automatic preparation flushes after 64 dirty keys, 256 KiB of pending metadata
or 1 MiB of uncovered canonical history. The experimental maintenance path uses
these key checkpoints and retires fully covered, unreferenced sealed sources.
Active and mixed sources remain pending for bounded range reclamation; no
whole-source body copy or whole-namespace snapshot planner remains in that path.

Covered historical records are not parsed again during stream recovery. Required
checkpoint metadata retains strict physical CRC validation, uncovered canonical
records retain their normal validation, and referenced bodies are checked when
consumed. A captured namespace metadata view defers source retirement until its
callbacks finish admitting historical body reads. Already admitted metadata and
body windows continue to pin their immutable bytes across replacement and root
checkpointing.

Per-namespace write admission reconstructs its outstanding checkpoint claim.
Root-wide reconstruction before any other owner can consume capacity remains
part of the standing-reserve gate, together with future deletion/relocation
obligations. These local claims and the fixed floor
do not establish full-store maintenance progress.

#### Bounded encoded-range relocation

Separate stable logical source offsets from their current physical ranges.
Preserve existing payload references and crypto binding contexts while replacing
only the physical mapping of a bounded range. A read window pins its mapping
generation; it must not follow a replacement midway through a read. Source names
remain logical labels, with no external file or persistent sidecar.

Logical source high waters must be independent of physical file capacity after
old ranges retire. Validate logical offsets against their 64-bit offset bound
and actual owned mappings. Enumerate present mappings by typed owner/key order;
do not walk every historical hole or allocate tables sized by logical address.
This is required for repeated fixed-capacity churn, not only the first reclaim.

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
discovery. That independent integration changes no persistence layout.
[Retention control ownership](#typed-retention-control-ownership)
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

The renewed mandate selects this incremental architecture and authorizes its
experimental persistence changes. It does not select an unmeasured reserve
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

This section records the initial shared text representation. The later
[native retention identity and progress owner](#native-retention-identity-and-progress-capacity)
supersedes its container representation and capacity policy. The multifile owner
continues using LPR1.

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
capacity redesign was not part of this maintenance surface. Indexed queries,
queues/outbox, complete
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

This surface does not implement the incremental-checkpoint/protected-capacity
architecture or promise maintenance admission at full capacity. Indexed
queries, complete external XA, resumable codec conversion, shared-root
coordination/recovery and deployment acceptance remain unfinished. Existing-root
conversion remains outside the initial implementation. The backend goal stays
active.

## External XA journal and coordinator ownership

The exclusive container SDK now runs resource registration, branch enrollment,
prepare intent/observation/votes, COMMIT/ABORT resolution, native participant
application, managed coordinator claims/renewal/sealing/release, fenced takeover,
snapshots, outbox adoption and history retirement through the common XA engine.
The existing `.txns` record remains the sole decision authority. Resource,
branch, owner and completion objects remain in `.xa`, stored through the typed
state boundary with no persistent sidecars. Public APIs, ABI and canonical
formats are unchanged. Normal `lc_pouch_open` still opens multifile Pouch.

Critical handoffs force completed dependencies under both durability policies.
Native staged data precedes vote/terminal-decision bytes, even if a decision
write survives a failed acknowledgement. Owner and branch changes, resource
registration and recovery snapshots stabilize their authority before returning;
applied native data precedes the completion marker. History retirement orders
owner removal, branch removal, decision removal and completion removal through
separate barriers. A borrowed mutation group regains its prior durability policy
after the handoff. Ordinary NoSync writes retain that policy.

The shared namespace helper pins native canonical files under namespace
authority and waits after releasing newly acquired authority. Container barriers
use the completed root prefix and exclude unfinished producers. A paused prepare
sync permits an unrelated ordinary write in the same `.xa` namespace to finish;
same-xid authority remains excluded while an independent xid remains available.
Borrowed-scope tests distinguish the required earlier handoff from a later
ordinary NoSync write. No whole-value payload materialization was added.

Failure qualification exposed two common-engine defects, both repaired here.
The transaction decoder now retains ownership of participant strings until
insertion succeeds, eliminating a leak under allocation failure. Native NoSync
COMMIT/ABORT retries could previously observe an unacknowledged decision and
advance an external branch without re-syncing `.txns`. Two native regressions
fail against that ordering and pass with the explicit prerequisite barrier.

Evidence is in `build/pouch-space-xa/`:

- The affected Debug qualification preserves **44 passing groups** from the
  initial run and completes its **30 remaining groups** after the allocation
  repair. The final handoff changes pass **39 Debug groups**, including all
  eight codec/policy SDK configurations, all four live model configurations,
  lifecycle comparisons, API/symbol boundaries and the **43-case native XA
  suite**. The container reuses **39 original XA cases**; the two original
  simultaneous shared-client cases remain native controls. Logs are
  `surface-debug*.log` and their preserved `*-details.log` files.
- Plain and encrypted/compressed failure profiles execute **17,184 allocation
  and I/O cuts** and **5,984 deliberate process exits**, including focused
  requalification of the changed handoffs and registration. Stable-image I/O
  models cover lost writes, persisted torn writes and complete writes with lost
  acknowledgements. Real exec children interrupt recovery and critical journal
  operations before/after observed I/O boundaries. Allocation-only profiles
  use immediate successful sync to isolate ownership/retry; persistence is
  established by the separate stable-image and real-I/O profiles. Logs are
  `surface-debug-initial-details.log`, `surface-debug-remaining-details.log`
  and `handoff-delta-debug.log`.
- Capacity models cover **38 raw/managed handoffs per codec**, preserve root
  health on denial, retry after releasing peer reservations and verify exactly
  **10 MiB** without implicit growth. Final denial counts are **14/12/10/12**
  for plain/encrypted/compressed/combined storage. These prove clean denial and
  retry, not guaranteed maintenance admission at full capacity.
- GNU and musl Release each pass **27 affected groups**, with the native suite,
  eight SDK configurations, eight comparisons and representative live models
  (`surface-gnu-final.log`, `surface-musl-final.log`). GNU Release uses ordinary
  worker defaults; musl uses its native defaults. Debug uses actual 128 KiB
  workers with the glibc stack cache disabled.
- **18 ThreadSanitizer invocations** pass with instrumented OpenSSL **3.6.5**,
  128 KiB workers and no suppressions. All eight SDK configurations run the
  **37 live cases**; two wrong-process fork cases and deliberate process deaths
  remain outside this live race profile (`surface-tsan-final.log`).
- **11 Memcheck invocations** pass with **334,625 allocations/frees**, no live
  allocations at exit, zero errors, no suppressions and only three inherited
  descriptors. Coverage includes representative coordinator/outbox/retirement
  cases under both policies, plain/combined journal and concurrency/scope
  models, and both native decision-retry regressions
  (`surface-memcheck-final.log`, `memcheck-*.log`).

The GNU Release journal comparison records **96 verified rows / 48 matched
pairs**, three samples over **16 configurations**: 24 x 4 KiB or 8 x 64 KiB,
one/four independent writers, both policies and new/preopened roots. It uses a
bounded 4 KiB producer and verifies every streamed value. Both engines perform
claim/enroll, native lease staging, seal/prepare/vote, COMMIT, external completion,
native application, release, history retirement and the same public cleanup
passes. Container physical checkpoint work is included. Close is separate, and
cold reopen verifies applied bodies and retired history. Setup is excluded;
OS cache is retained and the middle sample reverses engine order. This measures
Pouch journal/control work, not PostgreSQL query, network or server latency.
Raw rows are in `paired-performance.log`; the shared benchmark target is now
`pouch_space_lifecycle_compare` and retains the earlier queue/outbox modes.

Ratios below are native/container configuration medians of three paired samples;
values above one favor the container. Completion includes lifecycle work plus
close; CPU covers the work/cleanup phase only.

| Policy | Completion time ratio | CPU time ratio |
| --- | --- | --- |
| NoSync | 1.23-2.76x, median 2.03x | 1.92-2.95x, median 2.47x |
| Durable sync | 1.43-2.59x, median 1.70x | 1.94-3.06x, median 2.55x |

All 16 completion medians favor the container; the overall median is **1.84x**.
Work-phase-only ratios span **0.99-2.65x**: the preopened, large-value,
four-writer NoSync configuration spends about **1.3% more** before close.
Measured work-phase sync calls span **147-653 container / 302-1497 native**;
close calls are excluded. The container uses **one file**, versus **28-56**
native files. These observations do not establish cold-device or production
performance.

This surface qualifies exclusive XA journal/coordinator behavior. The optional
PostgreSQL provider is OFF in these builds; real SQL-driver/server qualification
remains part of deployment acceptance. Indexed queries are integrated below;
resumable codec conversion and shared-root coordination/recovery remain unfinished. The
incremental-checkpoint/protected-capacity architecture remains unimplemented; full
snapshots can still require foreground free space. Existing-root conversion
remains outside the initial implementation, and production cutover still
requires an explicit deployment decision. The complete backend goal stays active.

## Namespace and collection query-index owners

The exclusive container SDK now accepts normal indexing defaults and runs the
existing query engines, explicit flush, worker thresholds/deadlines, operation
guards, freshness and caches through typed immutable artifact owners. The
namespace query algorithms and generation/manifest formats are unchanged.
Native files have their own adapter; the common engine no longer interprets
artifact paths or filesystem timestamps. Container signatures use immutable
value or stream identity/incarnation facts in the existing manifest witness fields.
Bindings use binary domain identities under the actual namespace or collection
owner, with no synthetic namespaces or persistent sidecars.

Namespace generations and collection pages share the existing metadata crypto
codec, AAD and footer. Derived metadata retains its existing materialization
policy; collection pages retain their explicit size ceiling and 38-slot bound.
Application bodies still flow through bounded producer/consumer buffers. An
owner admits the complete encoded bound before invoking its producer and never
selects a partial artifact or grows the container implicitly. Captured inputs
remain readable across replacement, pruning and checkpoint and retain their
namespace/root until close. Namespace indexes now use the typed value owner
described below. Collection pages retain their sealed-stream owner: a publisher
shares it before catalog visibility so an immediate reader can open it before
publication returns. A collection reader racing removal rechecks its catalog
revision and falls back to canonical evaluation. Allocation and transport
failures remain errors.

Collection publication rechecks the recovered epoch and canonical page rows
under metadata authority. Receiver refresh observes a peer's committed tail,
preventing an older partial page from replacing a completed page. Explicit and
background maintenance clean obsolete epochs and repair/prune pages through
the same common engine. Physical-owner recovery still changes the accepted
disposable order epoch: prior query pages become scan fallbacks until repaired.
Collection retirement removes its query bindings before its identity. Failure
of optional indexing preserves a committed append and its retry receipt.

Allocation qualification also found a common manifest-copy leak: cleanup could
not see already copied segment IDs if a later copy failed. Ownership now
advances after each successful copy. Deterministic regressions separately fail
before and pass after the publication-owner and retirement-admission fixes.

Evidence is in `build/pouch-space-index/`:

- The initial **60 Debug groups** include **356 native Pouch cases**, **104
  native collection cases**, all **50 namespace SDK cases x eight codec/policy
  configurations**, and **16 collection SDK cases x eight configurations**.
  The final ownership changes pass **42 affected Debug groups**, including all
  SDK configurations, capacity/lifetime/concurrency models and both complete
  allocation profiles (`surface-debug*.log`, `ownership-final-debug*.log`).
  Three native-only namespace controls retain simultaneous shared-client,
  shared-compaction flag and transformed-body-cache assumptions; they remain
  in the native suite. Physical path corruption/cleanup controls remain native
  collection tests rather than fabricated container paths.
- Namespace publisher models pass **2,784 I/O cuts** and **3,712 allocation
  cuts**. Collection publisher models pass **944 I/O cuts** and **1,388
  allocation cuts**, including bootstrap, pruning and epoch cleanup. Another
  **204 startup allocation/mutex cuts** prove failed index-worker admission
  releases ownership. Collection trials use fresh workers so the JSON parser's
  first-use cache allocation is included consistently. Stable-image models
  cover lost/torn writes and complete writes with lost acknowledgements.
  Namespace allocation-only profiles use immediate successful sync to isolate
  ownership/retry; separate I/O and real-process profiles establish durability.
- Real exec children pass **2,664 process exits**, before/after observed I/O
  boundaries in bootstrap, incremental publication and index invalidation
  before compaction. Rebuild also has complete I/O/allocation profiles. Logs
  are `faults-debug*-details.log` and
  `collection-allocation-workers-details.log`. Final publication changes add
  volatile ownership/admission only; the durable byte sequence is unchanged.
- Capacity models record **32 namespace denials** across all four codecs and
  both policies; collection models also deny and retry after releasing peer
  reservations. They check root health and exactly **10 MiB** without implicit
  growth. This proves denial/retry, not guaranteed maintenance admission at
  full capacity. Producer and durability gates prove unrelated writes progress;
  paused post-selection and retirement admission tests prove reader behavior.
- Full GNU and musl Release builds each pass **40 selected runtime groups**,
  including all SDK configurations and native controls
  (`surface-gnu-release*.log`, `surface-musl-release*.log`). GNU Release uses
  ordinary defaults; musl uses its native defaults. Debug and race profiles
  use 128 KiB workers with the glibc stack cache disabled. API/symbol, storage,
  allocator and editor boundary checks also pass (`contracts-debug.log`).
- **28 ThreadSanitizer invocations** pass with instrumented OpenSSL **3.6.5**
  and no suppressions: all SDK configurations, all eight collection ownership
  profiles, and plain/combined artifact and concurrency models
  (`surface-tsan.log`). Intentional process deaths remain separate.
- **11 Memcheck invocations** pass with **210,185 allocations/frees**, zero
  errors, no live allocations, no suppressions and only three inherited
  descriptors. Coverage includes selected SDK/cache/guard cases, producer and
  reader lifetime models, collection append failure/repair, and native footer,
  binding, streaming and cleanup controls (`surface-memcheck.log`,
  `memcheck-*.log`).

The plaintext GNU Release comparison records **96 verified rows / 48 matched
pairs**, three samples over **16 configurations**: 24 x 4 KiB or 8 x 64 KiB,
one/four independent writers, both policies and new/preopened roots. Each row
uses public lease acquire/update/release and a streamed point read; each writer
then flushes and verifies six indexed queries. Work includes the common cleanup and
the container checkpoint. Close is separate; cold reopen verifies the persisted
manifest sequence before any flush, queries it and verifies streamed bodies.
The p50/p95 fields cover those per-row operations; `index_worker_max_ms` covers
the largest writer flush-plus-six-query phase. Setup is excluded, OS cache is
retained and the middle sample reverses engine order. Raw rows and derived
configuration medians are `paired-performance.log`
and `paired-summary.json`; the shared lifecycle benchmark retains earlier
queue/outbox/XA modes.

Ratios are native/container medians of three paired samples; values above one
favor the container. Completion includes work plus close; CPU covers work only.

| Policy | Completion ratio | Work-only ratio | CPU ratio |
| --- | --- | --- | --- |
| NoSync | 0.69-1.32x, median 0.89x | 0.15-0.51x, median 0.25x | 0.77-1.22x, median 0.98x |
| Durable sync | 1.00-1.63x, median 1.20x | 0.63-1.44x, median 1.07x | 0.92-1.46x, median 1.19x |

These measurements expose an unresolved performance gap. **Five completion
medians and twelve work-only medians regress**. The flush-plus-six-query phase
is **9.62-66.75x slower** for the container, median **23.28x**; cold-reopen
verification is **1.66-4.21x slower**, median **2.44x**. Faster close masks some
of the work cost. Work-phase sync calls span **15-139 container / 2-104 native**,
excluding close; storage remains **one file versus 19-55**. Publication batching
is the next measured optimization target, with completed-prefix durability,
immutable inputs and publication visibility preserved. The results do not
establish cold-device or production performance or satisfy deployment acceptance.

This is an exclusive indexing integration surface. Resumable codec conversion,
protected maintenance admission, shared-root coordination/recovery and deployment
acceptance remain required. The selected incremental-checkpoint/protected-capacity
architecture is still unimplemented; full snapshots can require foreground
free space. Existing-root conversion remains outside the initial implementation,
and production cutover still requires an explicit deployment decision. The
complete backend goal stays active.

### Publication and query cost baseline

The renewed architecture work separates the previously combined index phase
without changing product code. The comparison now times explicit validating
flush, the first three indexed selectors and their repeated pass separately.
It records CPU on the executing thread and attributes actual sync calls through
a thread-specific phase pointer, avoiding peer-writer calls in overlapping
measurements. Worker phase wall times are reported as separate maxima; CPU,
sync count and sync duration are sums across workers. Separate maxima need not
belong to the same worker and must not be added as a lifecycle duration.
Reopen pass durations are sums of the sequential namespace verifications and
remain inside the existing open-plus-query-plus-body verification interval.
CPU and sync attribution exclude helper/background threads; whole-process
work CPU and sync counters remain available separately.

The same three-sample plaintext GNU Release matrix produces **96 verified rows
/ 48 matched pairs**, with identical payloads, lease/update/read operations,
explicit flush and six indexed queries. Ratios below are **container/native**,
using each configuration's median of three timings; values above one are slower.

| Phase | Ratio range | Median across 16 configurations |
| --- | ---: | ---: |
| Validating flush wall time | 10.79-62.80x | 22.68x |
| First three-query pass | 0.74-1.35x | 1.05x |
| Repeated three-query pass | 0.29-1.49x | 0.84x |
| Flush CPU on executing threads | 1.06-2.06x | 1.54x |

The container spends **98.92-99.86%** of its combined worker index interval in
flush (range of configuration medians). In single-writer configurations,
**91.98-96.67%** of flush wall time is measured inside sync syscalls. Container
flushes execute 6-9 sync calls per writer in these samples; four-writer durable
samples occasionally share one call. Neither query pass executes a sync call.
This isolates the large observed gap to flush/publication for this workload;
it does not establish that all query algorithms, workloads or devices perform
equally. Root-wide flush serialization remains a separate inspected constraint.
These results support container revision publication as the next implementation
surface, rather than replacing the selector algorithms without evidence.

Evidence is `build/pouch-space-architecture/paired-performance.log` and
`paired-summary.json`. All 32 existing lifecycle comparison groups pass in
Debug, GNU Release and native musl Release (`tests-debug.log`, `tests-gnu.log`,
`tests-musl.log`), including queue, outbox and XA controls as well as indexes,
both policies and reopen modes. Release runs use ordinary platform stacks.
The benchmark checks phase/query coverage, phase timer containment, attributed
sync totals, exact streamed bodies, persistent index freshness, actual indexed
execution and the one-file container invariant. Debug uses 128 KiB workers and
the disabled glibc stack cache. These are benchmark/contract changes; no product
persistence or public behavior has changed yet.

## Integration and acceptance

### Atomic immutable revision publication

The physical container now provides `ps_revision_writer_info` and
`ps_revision_publish` through the private `revision.h` capability. The first
captures an unpublished writer's immutable identity and exact encoded length,
including its bounded pending tail, without publishing or flushing it. A logical
revision can embed that identity in another member before either becomes visible.
The second publishes distinct immutable keys in one physical revision. Members
share a space and capacity budget; NULL retains ordinary admission. A supplied
joint budget protects the complete payload/descriptor obligation while unrelated
producers consume unclaimed capacity. Selection checks aggregate descriptor
growth and entry credits, so separately affordable members cannot overdraw one
claim. Private producers and payload I/O run outside publication authority.

One checksummed `PSR1` frame in the existing physical journal binds all ordinary
PUT descriptors and their consecutive journal sequences. It contains metadata,
never concatenated application values. The complete descriptor frame must fit
the existing 128 KiB journal; this is an internal publication-primitive bound,
not a new public transaction limit. Existing canonical compound-stream framing
remains in use. Recovery validates every member descriptor and every canonical
payload before admitting any member. Derived query payloads use the separate
validation/rebuild boundary described below. An incomplete or invalid candidate admits
none, even when its first inner PUT is complete and valid. Checkpointing selects
the resulting entries through the existing directory/root protocol; there is
no second log, allocator, decision authority or pathname layer.

Runtime selection installs the whole revision under metadata authority and
advances readiness once. Durable roots, explicitly durable revisions and
revisions containing an always-durable CONTROL member expose and acknowledge
only the completed shared prefix. This
also covers non-CONTROL members on a NoSync root: their volatile visibility
obligation prevents a partially visible revision while the barrier is pending.
Plain NoSync revisions expose complete selection without an added sync. A
non-rollover revision uses exactly one shared durability barrier when required;
journal rollover may additionally checkpoint first. Captured old readers retain
their bytes through replacement and checkpoint retirement. Invalid invocations
leave private writers usable; failures after preparation starts require closing
and rebuilding them, with ordinary uncertain-I/O poison/recovery semantics.

The scoped qualification is in `build/pouch-space-revision/`:

- All **31 affected Debug storage groups** pass, including existing allocator,
  capacity, checkpoint/repack, stream, catalog, recovery and commit controls
  (`qualification-debug.log`, `qualification-debug-details.log`).
- GNU and native musl Release each pass **10 groups**, comprising the four new
  revision groups and six portability controls (`qualification-{gnu,musl}*.log`).
  Debug uses actual 128 KiB workers with the glibc stack cache disabled; Release
  uses ordinary platform defaults.
- New models check **265 byte prefixes**, **eight invalid complete candidates**,
  **19 publication I/O cuts**, **36 publication allocation cuts**, **eight
  recovery read cuts**, **24 recovery allocation cuts**, and **18 exec process
  exits**. Lost/torn persistence and process exit are separate models. Complete
  frames with lost acknowledgements may recover; complete inner prefixes may
  not. Every recovered pair is checked against exact old/new bodies and an
  unaffected neighboring value, then retried and checkpointed.
- Capacity checks reject insufficient joint entry/descriptor claims without
  selecting either member, then publish funded work after a peer exhausts
  ordinary capacity. Lifetime and concurrency cases retain old readers, cross
  journal rollover, and perform independent payload I/O while revision payload
  I/O or sync is paused. Mixed CONTROL/STATE revisions on NoSync roots are
  checked before and after their held barrier.
- Three ThreadSanitizer invocations pass with instrumented OpenSSL **3.6.5**,
  128 KiB workers and no suppressions (`qualification-tsan.log`). Three Memcheck
  invocations pass with **26,747 allocations/frees**, no live heap, errors,
  suppressions or owned descriptors (`qualification-memcheck.log`,
  `memcheck-*.log`). Intentional process deaths remain separate.

This completes the physical publication primitive. The namespace integration
below now funds generation plus manifest together and selects them through this
protocol. Collection pages retain their sealed-stream/catalog owner.
Incremental canonical checkpoints, bounded relocation,
standing maintenance admission, codec conversion, shared-root coordination and
deployment acceptance remain unfinished. No public API/ABI or production-open
selection changes; the experimental format marker is unchanged.

### Namespace index revision owner

Namespace generations and manifests now occupy typed immutable values scoped to
the actual namespace owner. Preparing a generation reserves its complete encoded
bound plus a conservative manifest bound before running its producer. Its exact
private signature enters the manifest without selecting either value. One
`ps_revision_publish` selects both. Failed preparation disposes private writers
and their budget; pruning removes exact captured incarnations and cannot delete
a replacement selected during traversal. Captured range readers retain their
old immutable bytes across replacement, pruning and checkpoint.

Publication authority is per namespace for the container; native multifile
publication retains its root-local mutex. Independent namespaces can prepare
and issue payload I/O while another namespace waits at its durability barrier.
Successful index flush still stabilizes its captured canonical state on NoSync
roots. The revision therefore explicitly requests durability under both policies;
removing that barrier lost acknowledged state in the deterministic lost-payload
model. Ordinary canonical NoSync mutations keep their existing policy.

Derived payload corruption must not invalidate later canonical journal records.
Recovery checks query descriptors and frame integrity and claims their physical
ownership, but defers their payload CRCs to the query-value validation/reader
boundary. The first recovered signature check validates immutable payloads and
memoizes the result. Bad derived bytes appear absent to manifest validation and
are rebuilt from canonical state, including when the manifest and watermark
themselves are unchanged. Canonical payload verification is unchanged. Scratch
allocation failure remains retryable; transport failure retains ordinary poison
semantics. Repeated warm signatures do not reread the entire generation.

Concurrent qualification exposed an unsynchronized shared generation-cache LRU:
four query workers could create a cycle during resident promotion. Packed cache
access now has an ownership mutex and copies components before releasing that
ownership. Parsed generation misses load privately, recheck admission under the
mutex, and retain a borrow until posting evaluation finishes. Eviction skips
borrowed generations. A deterministic regression pauses one query, completes
peer queries beyond cache capacity, resumes the pinned query and then exercises
parallel misses/promotions/evictions. It runs against both backends. Namespace
build/publication and posting evaluation remain outside this cache mutex.

The matched plaintext GNU Release initial-build matrix completes **96 verified
rows / 48 pairs**. Ratios are **container/native**, using the median of three
samples for each configuration, then the median across configurations:

| Phase | Ratio range | Median |
| --- | ---: | ---: |
| Validating flush wall time | 1.77-12.32x | 5.49x |
| First three-query pass | 0.67-2.05x | 1.17x |
| Repeated three-query pass | 0.50-3.05x | 0.95x |
| Flush CPU on executing threads | 0.75-1.07x | 0.94x |
| Whole work, NoSync | 1.65-3.29x | 2.53x |
| Whole work, durable | 0.64-1.28x | 0.78x |
| Cold reopen and verification | 1.53-3.44x | 2.48x |

Median flush barrier counts are one for one writer and two across four writers,
with neither query pass issuing a barrier. This substantially reduces the prior
22.68x median flush ratio while leaving a real NoSync and reopen gap. Work includes
the existing common cleanup and container checkpoint; close remains separate.
The result does not establish large-store scaling or production acceptance.
Raw evidence is `build/pouch-space-index-revision/paired-performance.log` and
`paired-summary.json`; the interrupted pre-repair run and parent-debugger stacks
are retained there too. The reference workload, selectors and streaming body
checks are unchanged.

The separate repeated-update matrix also completes **96 verified rows / 48
pairs**, with four lease/update/read and validating-flush rounds on the same
keys. Every round must advance the published index sequence and return exact
indexed results; final cold reopen verifies that selected sequence and every
streamed body. Phase times accumulate across the four rounds before taking
worker maxima. Both engines use the same loop and fixed 10 MiB capacity bound.
Whole-work container/native ratios are **1.51-3.78x, median 2.50x** under NoSync
and **0.74-0.97x, median 0.83x** under durable policy. Flush ratios are
**2.51-19.14x, median 7.47x**; first/repeated query pass medians are **1.13x /
1.04x**. This is repeated small-store replacement, not long-duration or
large-store qualification. Evidence is `paired-churn-performance.log` and
`paired-churn-summary.json` in the same stage. All 12 initial/churn Debug
comparison groups pass (`churn-debug.log`).

The complete pre-cache-repair Debug gate passed **70 groups**, including **1,770
I/O cuts**, **3,060 allocation cuts**, **1,276 exec process deaths**, all codec/
policy SDK combinations and the physical revision models
(`qualification-debug.log`, `qualification-debug-test.log`). The affected
post-repair gate passed **28 groups**, including **357 native Pouch cases**, all
**51 namespace SDK cases x eight configurations**, startup failure/retry,
allocation and publication concurrency controls, comparisons and five boundary/
API/editor checks (`cache-repair-debug.log`, `cache-repair-debug-test.log`).
Unchanged physical/recovery and process-death proof is retained from the first
gate. Debug uses 128 KiB workers with the glibc stack cache disabled.

GNU and native musl Release each pass **53 selected groups**, including all
eight namespace and collection SDK configurations, initial/churn comparisons,
typed-owner capacity/concurrency/recovery cases, five revision groups and native
portability controls (`qualification-{gnu,musl}.log` and their `-test.log`
readbacks). Release uses ordinary platform worker stacks. Seven targeted
ThreadSanitizer invocations pass with instrumented OpenSSL **3.6.5**, 128 KiB
workers and no suppressions (`qualification-tsan.log`). Nine Memcheck invocations
pass with **166,235 allocations/frees**, zero live heap, errors, suppressions or
owned descriptors (`qualification-memcheck.log`, `memcheck-[1-9].log`). These
checks cover the new generation-cache borrow/eviction regression, parallel
canonical writes/queries/publication, invalidation, operation watermarks and
lost/corrupt derived generations.

This is the namespace-index publication/reader/recovery cutover. It adds no
public API or production backend selector, changes no independently versioned
format marker and supplies no migration; recreate private experimental fixtures.
Collection pages still require their sealed owner. Typed-value traversal still
scans the resident physical bucket table, and the revision descriptor has its
existing bounded size; this evidence does not qualify unbounded generations.
At this index-publication milestone, incremental canonical checkpoints, bounded
relocation, standing maintenance admission, codec conversion and process-shared
roots remained unfinished. The complete backend goal remains active.

### Incremental canonical key checkpoint cutover

The namespace append path now funds and prepares changed canonical key metadata
before acknowledging its original append. Pure common projection rules drive
both resident state and checkpoint summaries, including cold object body links,
metadata-only lease placeholders, query sequences, lifetimes, stages and retained
decisions. Small memory bodies and metadata batches retain structured prefix
admission. Budget reuse allocates nothing; shrinking a funded claim requires no
new capacity even while a maintenance peer holds the remaining floor.

The old namespace snapshot builder and source-sized body-copy maintenance path
are removed. Typed metadata slots and one coverage/high-water value replace
their replay role. Matching resident references update through a preallocated
plan; checkpoint publication preserves the resident logical projection. An
unchanged repeat checkpoint selects no new generation and copies no body bytes.
Active or mixed sources are reported as pending work for range reclamation.

Debug qualification uses ASan/UBSan and 128 KiB workers. The queue, outbox, XA
and namespace-query SDK suites passed all eight codec/durability configurations,
alongside all 357 native Pouch cases. Mutation allocation/I/O gates and staged
recovery passed. The checkpoint atomic-group fixture passed all 140 write/sync
cuts and 406 allocation cuts across both root policies, including interrupted
partial selection and loss of unsynchronized writes. Maintenance passed 112
allocation and 120 I/O cuts. The 1,500-key/3,000-record group exceeds one physical
revision's descriptor capacity and recovers through bounded batches at 10 MiB.
The final Debug gate passed 14 groups, including the checkpoint fault fixture,
projection, staged recovery, concurrent maintenance and five API/boundary/editor
checks (`qualification-final-debug.log`).

Focused regressions cover cold object metadata updates, captured metadata and
body windows, retirement deferred by metadata views, sibling-writer progress
during held checkpoint I/O and cold namespace admission reading less than
512 KiB for a fixture with multiple MiB of referenced bodies. A generated 1 MiB
body proves that the history trigger selects its funded metadata before the
next body producer, under both root policies.

GNU Release passed 57 selected groups; native musl Release passed 58, including
the final history policy fixture. Both cover the complete queue, outbox, XA and
namespace-query SDK matrix and selected native portability cases. The GNU matrix
preceded the history-threshold tuning; eight focused GNU groups passed afterward.
Release retains ordinary platform worker stacks. Ten ThreadSanitizer invocations
passed with instrumented OpenSSL 3.6.5 and 128 KiB workers, without suppressions.
Eleven Memcheck invocations passed, including captured windows, the 1,500-key
group, cold object projection and concurrent maintenance; each finished with
zero live heap, errors, suppressions or owned descriptors. Together they account
for 514,892 matched allocations/frees.

Matched GNU Release comparisons verified all 96 runs: 16 configurations, three
samples per engine, four rounds over the same keys, 4 KiB/64 KiB bodies, one/four
writers, both durability policies and cold/preopened namespace ownership.
These are process-reopen/cache-warm host measurements, not cold-device or
large-store qualification. Ratios below are container/native medians of the three
samples per configuration, then medians across configurations:

| Measurement | NoSync | Durable |
| --- | ---: | ---: |
| Whole measured work | 2.57x | 0.84x |
| Work CPU | 0.90x | 0.88x |
| p50 write latency | 0.81x | 0.75x |
| Index flush wall time | 9.44x | 7.47x |
| Index flush thread CPU | 0.92x | 0.91x |
| First indexed query pass | 1.55x | 1.08x |
| Repeated indexed query pass | 1.18x | 0.97x |
| Close | 0.011x | 0.010x |
| Reopen and verify | 3.14x | 2.88x |

The initial 256 KiB history cadence produced a 3.46x NoSync work ratio. Selecting
1 MiB reduced absolute NoSync work time by a median 18% across those
configurations; durable work time changed by about +1.5%. Metadata obligations
remain funded and checkpoint selection remains durable under both policies.
Structural barriers
and reopen overhead remain material performance work. This establishes the
incremental checkpoint architecture; it does not establish performance parity.
Create, close and reopen are recorded separately from measured work. Evidence
is under `build/pouch-space-checkpoint/`: `qualification-{gnu,musl}.log`,
`qualification-final-gnu.log`, `qualification-{tsan,memcheck}.log`,
`memcheck-*.log`, `paired-performance.log` and `paired-summary.json`.

Bounded range relocation, a computed standing reserve with root-wide recovery,
codec conversion and process-shared roots remain separate unfinished gates.

### Bounded namespace piece integrity

The canonical namespace stream now caps each physical grant at 16 KiB, retaining
page-granular fallback and geometric growth from 4 KiB. Its prologue contains
the original 64-byte immutable identity and a separate 32-byte completion
footer. The footer binds the grant incarnation, completed prefix length and
encoded-byte CRC, with its own metadata CRC. The physical root binds the
96-byte prologue width at byte 72; incompatible private fixtures are rejected.
There is no independent format-marker increment or legacy parser.

After private canonical header/metadata fixups, completion checksums join the
same readiness prefix as the canonical append. There is no extra ownership
journal event or durability barrier per record. CRC combination extends a
previously checked partial piece using only newly appended bytes. The writer's
existing bounded buffer supplies scratch; failure restores its prior checksum
state before retry. A regression changes older bytes before a later append and
proves that extension cannot silently certify that damage. The canonical body
CRC and codec validation remain authoritative.

Recovery inspects completion metadata for full pieces within its protected
prefix and for a sealed final partial piece. Covered bodies are not read merely
to open the namespace. A valid footer is reused without repair writes. Missing
uncovered completion metadata may be rebuilt after canonical recovery accepts
the prefix. Rebuilt metadata registers fresh readiness even when the accepted
tail does not advance; a peer barrier covers it exactly once. The retained
`repair-red-debug.log` reproduces that missing-readiness error, and the final
`stream-piece-repair` fixture proves its correction, including torn repair I/O.

If reconstruction includes an inherited prefix that recovery did not revisit,
the footer retains a reconstruction bit. Appending and reopening preserve that
obligation. Independent piece verification returns
`pouch_piece_needs_validation`; a relocation owner must validate the inherited
live bodies against their canonical CRCs before using those bytes. Rehashing an
inherited prefix is not itself proof of its original contents. A fresh piece
fully validated by canonical replay does not acquire this obligation.

Debug qualification passed 27 groups: the complete eight-configuration query
SDK matrix, plaintext/NoSync and combined-codec/durable queue/outbox/XA controls,
projection, stages, namespace admission, checkpoint fault cuts, large groups,
stream lifecycle and recovery. Ten focused groups passed after the final
reconstruction/readiness changes. GNU and native musl Release each passed twelve
final groups, including large keys and concurrent maintenance. Five targeted
ThreadSanitizer invocations passed with instrumented OpenSSL 3.6.5, 128 KiB
workers and no suppressions. Four Memcheck invocations finished with no live
heap, errors, suppressions or owned descriptors, with 48,714 matched
allocations/frees. Logs are under
`build/pouch-space-pieces/` as `qualification-debug.log`, `proof-debug.log`,
`qualification-final-gnu.log`, `qualification-musl.log`,
`qualification-tsan.log`, `qualification-memcheck.log` and `memcheck-*.log`.
Seven final Debug groups covering large keys, checksum readers and the standard
API/boundary/editor checks passed (`final-gates-debug.log`). Changed C/headers
pass `clang-format --dry-run --Werror`; `git diff --check` is clean.

The final matched GNU Release comparison verified all 48 runs: eight
configurations, three samples per build, 4 KiB/64 KiB bodies, one/four writers,
both durability policies, preopened namespaces and four rounds over the same
keys. This compares the new pieces with the frozen checkpoint implementation,
not directly with multifile. Median-of-three ratios per configuration, then
medians across configurations, are:

| Measurement, pieces/checkpoint build | NoSync | Durable |
| --- | ---: | ---: |
| Whole measured work | 1.04x | 1.01x |
| Work CPU | 1.09x | 1.02x |
| p50 write latency | 1.16x | 1.00x |
| Reopen and verify | 1.01x | 1.10x |

Whole-work ratios range from 0.83-1.21x for NoSync and 0.90-1.13x for durable
work. These small cache-warm host fixtures establish neither cold-device
performance nor a production latency commitment. The NoSync write-latency/CPU
cost remains optimization work; the earlier multifile comparison is still a
separate checkpoint-stage baseline. Raw rows and the complete summaries are
`paired-performance.log` and `paired-summary.json` in the same stage directory.

This milestone established independently checked namespace pieces. The following
stage adds sparse mappings, reader generations and bounded range reclamation.
Collection REGION owners permit independent uploads inside allocated ranges and
still require their own completed-unit integration; they are not forced through
the namespace stream's single-appender policy. Computed standing admission and
recovery of all outstanding root obligations remain required work.

### Sparse mappings and bounded namespace reclamation

Completed namespace pieces can now become immutable packed values containing
only their live encoded body ranges, or disappear when no live ranges remain.
The same typed owned-run capability describes raw append grants and immutable
replacement maps. Its descriptor explicitly records logical span and mapping
form; a raw grant descriptor is now 104 bytes. Old private fixtures must be
recreated. There is no migration, alternate legacy reader, public ABI change or
change to the default multifile backend.

Logical source offsets and high waters no longer depend on physical container
capacity. Heads index only present mappings, including during checkpoint
recovery and source retirement. A captured read window retains the exact mapping
handles it admitted. Replacing the current map cannot redirect an existing
reader. Durable selection and the root checkpoint retirement protocol govern
reuse; closing the final old reader releases protected pages without another
body rewrite. Required holes produce protocol errors. Body labels, offsets,
encoded bytes, canonical CRCs and crypto binding contexts remain stable.

The checkpoint owner maintains an intrusive augmented AVL interval index for
each referenced source. Accepted scalar key summaries update it without
allocation. Piece capture visits overlapping body references and returns their
coalesced union, including live stage aliases. Metadata-only lease changes with
the same body do not change the source's body generation. Prepared ownership and
generation checks reject stale selection. A new capture defers while dirty key
metadata could still supersede bodies in the selected checkpoint.

One candidate verifies and copies at most one existing bounded piece, then
reacquires logical namespace authority and checks its captured liveness before
durable publication. The production maintenance path releases its outer logical
lock during copying. Tests hold an actual encoded-map write while a metadata
update in the same namespace, a sibling publication and an admitted body reader
finish. A separate test changes body ownership during copying, checkpoints a
peer and rejects the stale candidate. Metadata views continue to defer
reclamation until they can no longer admit historical body references.

Maps choose the smaller of a compact range table and a byte bitmap. Fragmented
maps retain the bitmap with a small rank directory in memory, rather than
expanding every live byte into a range object. Header/table checksums protect
metadata admission; immutable extent CRCs protect the encoded replacement.
Recovery rejects a candidate whose complete descriptor persisted without its
payload and retains the raw predecessor. Reconstructed raw checksums still
require canonical body validation; they remain deferred rather than becoming
independent evidence for a replacement.

The cell allocator can split its final subpage tail once to use existing slack,
then fit the remainder in another interval or one new page. This adds at most one
extent to the immutable-value budget bound. Three 4 KiB raw pages with 2,000 live
bytes each now pack into two physical payload pages. The alternating-byte case
retains thousands of disjoint ranges with a bitmap under 2 KiB and no individual
replacement allocation larger than the existing 16 KiB chunk plus its control
structure. Several bounded buffers may coexist; no complete body or source is
materialized.

A forced namespace pass visits at most sixteen present pieces per source. Its
cached cursor advances successive passes and restarts after the unfinished tail,
so later body replacement can make earlier maps reclaimable. Unchanged ranges
and replacements that would not reduce owned allocation are skipped. Whole
unreferenced sealed sources still retire through their existing owner. Private
maintenance results now report selected retained encoded-body bytes and complete
map-value bytes; rejected candidates are excluded, while I/O probes account for
attempted work. The comparison harness verifies the checkpoint's key set and
bounded relocation instead of requiring a complete source copy.

The fixed-capacity namespace fixture streams a 1 MiB stable body and replaces a
192 KiB neighbor 52 times while retaining the original stable reader. Both sync
policies finish with logical tail 11,299,172, physical capacity 10,485,760,
1,290,240 bytes of physical payload reservation and 79 present mappings. Across
the complete run, selected replacement map values total 447,166 bytes. Payload
reservation excludes root/metadata workspace and includes old payload held by
the reader at that observation. No growth is performed. Both bodies pass byte
and canonical CRC validation after reopen. The lower owner also passes 660
append/remove cycles at 10 MiB with logical offsets beyond physical capacity.

The final ASan/UBSan Debug gate passes 25 groups, including 292 maintenance
allocation cuts, 336 maintenance I/O cuts, actual-copy concurrency, large keys,
compaction comparisons, queue/outbox/XA controls and standard API/editor/boundary
checks. Additional gates cover the complete eight-configuration query SDK matrix,
public lease lifecycle/protection/concurrency, canonical checkpoint fault cuts,
large atomic groups and stream lifecycle/recovery. Replacement fixtures exercise
all 70 write/sync, allocation and read failure sites across both map encodings
and both durability policies, plus selectively persisted candidate payload loss.
GNU and native musl Release each pass fifteen core groups and eleven final
publication/compaction groups with normal worker defaults. Six ThreadSanitizer
invocations pass using instrumented OpenSSL 3.6.5, 128 KiB workers and no
suppressions. Four strict Memcheck invocations finish with 69,562 matched
allocations/frees, zero live heap, errors, suppressions or owned descriptors.
Evidence is retained under `build/pouch-space-reclaim/`: `final-debug.log`,
`final-debug-details.log`, `qualification-gnu.log`, `final-gnu.log`,
`qualification-musl.log`, `final-musl.log`, `qualification-tsan.log`,
`qualification-memcheck.log` and `memcheck-*.log`.

The matched GNU Release comparison verifies all 72 runs: eight configurations,
three samples each of the frozen piece-integrity build, this implementation and
the current multifile reference. It uses 24 x 4 KiB or 8 x 64 KiB bodies, one/four
writers, both policies, preopened namespaces and four rounds over the same keys.
The following are median-of-three ratios per configuration, then medians across
configurations; lower is faster. Work, readiness, reopen and close are separate
measurements.

| Measurement | NoSync | Durable |
| --- | ---: | ---: |
| Measured work / frozen container | 0.96x | 0.95x |
| Work CPU / frozen container | 0.96x | 1.07x |
| p50 write / frozen container | 0.91x | 0.98x |
| Reopen and verify / frozen container | 0.92x | 0.99x |
| Measured work / multifile | 2.81x | 0.92x |
| Work CPU / multifile | 1.00x | 0.97x |
| p50 write / multifile | 0.94x | 0.79x |
| Indexed worker / multifile | 11.13x | 5.87x |
| Reopen and verify / multifile | 2.10x | 2.62x |
| Close / multifile | 0.012x | 0.011x |

The new/frozen work ratios range from 0.84-1.23x for NoSync and 0.89-1.17x for
durable work. These small host samples support comparable container cost, not a
statistical speedup or production latency commitment. Against multifile, the
remaining NoSync workflow cost is visible despite comparable write latency and
CPU. Median indexed-worker measurements across the configurations are about
61.2/35.7 ms for this backend versus 4.9/6.0 ms for multifile (NoSync/durable).
Median close is about 0.5 ms versus 39.4/47.6 ms; reopen/verify is about 5.5 ms
versus 2.3-2.6 ms. Indexed publication/readiness and cold admission remain
optimization work. Raw measurements and complete per-configuration medians are
`paired-performance.log` and `paired-summary.json` in the same stage directory.

This completes bounded namespace range replacement, not the full backend or its
standing-capacity proof. Unfinished active tails, captured metadata views,
changing body ownership and inherited checksum obligations can retain pending
work. Threshold/completion scheduling still needs to follow physical and deferred
obligations as logical high waters grow. At this milestone, computed standing
reserve and root-wide reconstruction of outstanding capacity obligations before
any producer remained required. The following recovery change restores
outstanding canonical checkpoint claims; the fixed floor still does not prove
the complete standing-capacity property. Collection REGION relocation, codec
conversion and shared-root coordination/recovery remain separate required
surfaces. Production cutover still requires an explicit deployment decision.
The full goal remains active.

### Root admission restores outstanding checkpoint claims

Reopening the exclusive container now recovers every catalogued namespace before
returning a writable root capability. Each namespace loads its selected key
summaries, recovers its canonical source prefix and replays the uncovered suffix
to restore its outstanding checkpoint budget. Cached namespace ownership retains
those volatile claims for the root lifetime. A producer cannot consume a cold
sibling's completion workspace simply because nobody has acquired that sibling
since reopening. Creation of an empty root needs no census.

The census uses existing canonical authority and adds no durable debt ledger or
producer durability barrier. Namespace checkpoint publication remains explicit.
This deliberately moves recovery into root readiness: startup retains storage
owners for all namespaces and pays their metadata/suffix recovery cost. Errors
in a cold namespace now reject root open; no writer capability is exposed with
partially restored claims. Failed initialization destroys partial namespace
owners, budgets, crypto and physical ownership through the normal root close
path. A same-path retry needs no intervening process exit or extra capacity.

An interrupted bounded checkpoint may have already selected a large key value
while its coverage control still names an older prefix. Replay now applies the
existing canonical supersession rules before funding an entry. Superseded
records need coverage-control credit, not a second copy of an already-selected
value. This is necessary at full capacity: refunding a temporary large claim
after projection is too late. The suffix replay also skips decoding selected
metadata values that the fresh checkpoint owner already loaded. Public view
visits still include selected metadata and high waters.

The fixed 10 MiB fixture covers both sync policies, complete/interrupted key
checkpoints and spare/full foreground capacity: eight combinations. The
interrupted case selects 64 of 65 keys, including a 1 MiB metadata-only key,
before rejecting the next batch. After root return, before acquiring that cold
namespace, its outstanding payload credits exactly match the pre-close claim.
A peer then exhausts ordinary capacity and the cold checkpoint still completes
without growth. A separate two-namespace model cuts all 179 allocation,
4 write/sync and 16 read failure sites per policy, then retries the same path and
checks both canonical values and both restored claims. That model uses an
explicit 1 MiB fixture; the product default remains 10 MiB.

The final ASan/UBSan Debug gate passes 15 groups, including the new capacity and
cleanup cases, checkpoint publication fault cuts, root/namespace lifecycle and
concurrency, public snapshots/sequence recovery, source-manifest and Pouch
boundary checks. GNU and native musl Release each pass twelve affected groups
with normal worker defaults. Strict Memcheck passes the complete bootstrap fault sweep with
132,600 matched allocations/frees, zero live heap, errors, suppressions or owned
descriptors. Logs are in `build/pouch-space-obligations/`: `final-debug.log`,
`qualification-gnu.log`, `qualification-musl.log`, `qualification-memcheck.log`
and `memcheck-bootstrap.log`.

The paired GNU Release comparison verifies 48 runs: eight configurations,
three samples each of the frozen sparse-reclamation build and this change,
using the same preopened four-round indexed workloads as the preceding stage.
Ratios below are medians of each configuration's median-of-three ratio to that
frozen container; lower is faster. Readiness and reopen are reported separately.

| Measurement / prior container | NoSync | Durable |
| --- | ---: | ---: |
| Measured work | 1.13x | 0.97x |
| Work CPU | 0.98x | 0.97x |
| p50 write | 0.98x | 0.96x |
| Indexed worker | 1.11x | 1.16x |
| Reopen and verify | 1.23x | 1.21x |
| Close | 1.09x | 0.97x |

Median reopen/verify across configurations is about 8.0/6.8 ms versus the
prior 6.1/6.0 ms (NoSync/durable). NoSync work ratios range from 1.02-1.27x;
durable ratios range from 0.78-1.10x. NoSync sync-call medians are unchanged in
all four configurations. These small host samples do not establish a speedup,
a cause for the measured work variation or a production latency commitment.
They expose the added cold admission cost. This comparison does not replace
the preceding multifile baseline or resolve its NoSync indexed-readiness gap.
Raw data and per-configuration medians are `paired-performance.log` and
`paired-summary.json` in the same stage directory.

This recovery milestone restores existing checkpoint debt before admission.
The fixed 16 KiB floor still needed a computed compound obligation for deletion,
retention control phases, bounded relocation and their metadata/publication
dependencies. Retention work can exceed a tombstone's size, and replacing an
active control may require both predecessor and successor ownership until
durable retirement. Those credits cannot be assumed to be interchangeable.
Active partial tails, inherited checksum proof, collection REGION relocation,
codec conversion, shared-root coordination/recovery and the broader capacity
progress proof remain required. The following surface adds canonical deletion
protection; it does not complete expiry or the full capacity proof.

### Computed canonical deletion reserve

Ordinary allocation now protects a computed future maintenance plan in addition
to summed credits for admitted work. Each live canonical key owns an unclaimed
deletion requirement. An augmented AVL tree caches the component maxima of owner
payload, descriptor and entry plans. Reading the aggregate is constant time;
changing or removing one owner is logarithmic. Equal requirements need no tree
update. This reserves one bounded future operation rather than one deletion
copy per live key. Already admitted independent participants still retain their
separate summed completion claims.

The space-owned base plan covers a bounded encoded piece, optional successor
head/catalog/routing and one work hint. A key's compound plan adds its framed
canonical deletion, changed-key checkpoint value and coverage control. Plans
include page fragmentation and positive descriptor/entry growth. Ordinary
admission also preserves the incremental root checkpoint workspace required by
those future descriptors and entries. Optional checkpoint cache expansion honors
that same floor. Mandatory checkpoints continue using actual funded workspace
without requiring another hypothetical future unit. The lower physical owner's
existing 16 KiB control allowance remains an additional foundation; it is no
longer the whole native namespace maintenance reserve.

The common canonical codec measures future tombstones from key and retained etag
sizes without allocation or body reads. Prefix admission protects a larger
requirement before application input. Side metadata and body length do not
inflate a future tombstone. Decoded projections use the same codec calculation,
so storage policy does not duplicate canonical field offsets. Reopen reconstructs
requirements from selected slots and accepted suffix records before exposing
writers. Recovery can register a requirement while its future floor is occupied;
ordinary admission then waits for capacity while mandatory completion remains
possible. No second persistent ledger or per-producer durability barrier is added.

Requirements retain the largest accepted or prepared state until its replacement
slot is durably selected. Shrinking then visits only those changed slots, using
the same forward-slot supersession rule as recovery. Old pinned storage stays
physically owned and is never counted as free reserve. Aborting a private append
clears its physical suffix before releasing grown requirements. Checkpoint
budgets are mandatory completion claims; an ordinary prefix separately verifies
that its newly funded claim still leaves the computed future floor intact.

Exact-length value plans and upper-bound value plans are now distinct. A shorter
value under an aligned byte limit can fill an old packed tail and use another
extent in a new page. Upper-bound plans fund that extra extent even when their
limit is page aligned. Checkpoint claims and future checkpoint plans use those
monotonic bounds, including when a key's requirement shrinks.

Work-hint identity is preserved. Native deletion appends ensure the existing
ordinary hint using explicit maintenance admission when it needs creation.
Ordinary producers preserve the floor, and terminal maintenance callbacks may
spend it. All internal callers now pass their admission class explicitly. This
avoids introducing a separate first-deletion terminal publication/barrier merely
to make capacity available.

The fixed 10 MiB deletion fixture covers 3,491-byte, 60 KiB and 1 MiB metadata-only
keys under both policies. The smallest case aligns the checkpoint admission
bound to a page. Each fixture reopens before a peer fills ordinary capacity,
creates its required hint, appends a deletion and publishes its checkpoint while
an old view remains readable. After the view drains, bounded reclamation frees
4,096, 118,784 and 2,101,248 bytes of canonical history respectively. Capacity
stays 10 MiB. These are metadata-key/history tests, not a complete public expiry
or large-body-key capacity qualification. Active incomplete tail grants may
still remain pending.

Other new cases cover 127 owners with independent maxima, failed growth without
state change, allocation-free shrink/removal, an aligned value limit using two
packed extents, four independent writers changing requirements, and producer
rejection without input consumption. A successful private body is also discarded
to prove requirement rollback after suffix invalidation. At full capacity, a
funded synthetic operation publishes 1,024 empty values and 12 KiB of payload
while peer checkpoints repeatedly rewrite the growing metadata. This exercises
future descriptor/entry workspace rather than just payload protection.

Debug gates pass the new cases plus public mutation/capacity/key bounds,
concurrent projection and relocation, staging recovery faults, large groups,
checkpoint/history faults, churn and the standard Pouch/source/header boundaries.
The checkpoint sweep covers 203 allocation and 70 write/sync cuts per policy;
bootstrap recovery covers 181 allocation, 4 write/sync and 16 read cuts per policy.
GNU and native musl each pass the twelve-group gate and subsequent affected
hint-admission checks with default workers. Three affected concurrency modes pass
ThreadSanitizer with instrumented OpenSSL 3.6.5, 128 KiB workers and no
suppressions. Four strict Memcheck cases finish with 171,459 matched
allocations/frees, zero live heap, errors, suppressions or owned descriptors.
Evidence is under `build/pouch-space-standing/`: `final-debug.log`,
`qualification-debug.log`, `admission-debug.log`, `qualification-gnu.log`,
`qualification-musl.log`, `admission-gnu.log`, `admission-musl.log`,
`qualification-tsan.log`, `admission-tsan.log` and `memcheck-1.log` through
`memcheck-4.log`.

The final paired GNU indexed comparison verifies 48 runs: eight configurations
and three samples per engine against the frozen root-admission build. Ratios are
medians of per-configuration median-of-three ratios; lower is faster.

| Measurement / prior container | NoSync | Durable |
| --- | ---: | ---: |
| Measured work | 1.04x | 0.99x |
| Work CPU | 1.00x | 0.99x |
| p50 write | 1.00x | 1.05x |
| Indexed worker | 0.98x | 1.17x |
| Reopen and verify | 1.00x | 1.13x |
| Close | 1.36x | 1.01x |

Work ratios range from 0.98-1.06x for NoSync and 0.90-1.20x for durable work.
Median close across configurations remains below 0.7 ms; median reopen/verify
is about 6.9/7.2 ms (NoSync/durable). The additional 24 queue/outbox/XA probes
verify both policies and one/four writers, with one sample per engine and
configuration.
All single-writer workflow sync counts match the frozen build; four-writer
counts vary with batching. These are small host probes, not statistical speedup
or production latency commitments. They do not resolve the earlier multifile
NoSync indexed-readiness gap. Raw results are `paired-performance.log`,
`workflow-performance.log` and `paired-summary.json` in the same directory.

Canonical deletion protection is now integrated. Retention's two control phases
and transaction participants still need their complete mandatory-work plan and
capacity proof. Automatic completion/reclamation of active partial tails,
inherited checksum validation, collection REGION relocation, codec conversion,
shared-root coordination/recovery and the full public lease/queue capacity matrix
remain required. The full goal remains active, the multifile default is retained,
and production cutover still requires an explicit decision.

### Covered partial tails and physical maintenance scheduling

Native maintenance now completes a profitable, fully covered partial tail rather
than waiting indefinitely for another producer to fill its raw grant. It reserves
successor routing and bounded piece replacement together, validates captured
coverage and the body-source epoch under namespace mutation authority, durably
selects an empty successor and seals the predecessor. Copying the retained encoded
ranges then runs outside logical publication authority. Selection still rejects
changed ownership or a newly admitted metadata view. A rejected candidate keeps
the piece cursor at that generation for retry instead of skipping it.

The structural transition uses the existing successor/select/seal protocol. If a
failed attempt already selected its successor, retry finishes the pending seal
without creating another successor. A nearly full, live tail whose replacement
would not reduce rounded ownership stays active. An empty active head does not
rotate. Uncovered bytes, zero-completed abandoned grants and inherited checksum
obligations continue to defer reclamation. No whole body is materialized, and no
new on-disk representation or public ABI is introduced.

Each physical stream head now maintains the selected payload ownership of its
present mappings, including page/cell rounding. This derived counter is rebuilt
during recovery and checked against the existing child-directory walk. Namespace
source capture exposes that value and the present mapping count. Old reader pins
remain separately owned by the physical retirement protocol; they are excluded
from the current-source metric and never treated as free space.

Maintenance candidates and terminal thresholds use selected physical ownership,
including sources older than canonical checkpoint coverage. Historical logical
offsets no longer inflate the candidate byte estimate. This estimate describes
stored candidates, not guaranteed reclaimable bytes. After a completed repack,
a sealed predecessor with no canonical body references can retire in the same
pass. Admitted readers retain its exact old incarnations until they close. Key
coverage may still name the retired predecessor; the existing successor recovery
contract handles that state without an extra empty checkpoint publication.

The public full-capacity fixture streams a 1 MiB + 37 byte body under a 60 KiB
key, pins a real body reader, fills ordinary capacity with unrelated ownership,
then deletes the key and runs bounded maintenance. Plaintext and combined-codec
configurations, each with both durability policies, complete the old tail and
retire its source at 10 MiB without growth. The pinned body still streams with
valid bytes and CRC. Closing it releases 1,064,960 physical payload bytes in each
case. Maintenance drains pending work, a new foreground write succeeds, and
reopen preserves both the tombstone and the new value. A separate metadata-only
fixture leaves over 1 MiB of logical history with one small physical grant:
nonforced terminal maintenance respects the physical threshold, while forced
maintenance completes the tail and reports its actual selected ownership.

Lower cases cover metadata-view exclusion, a live body pin across raw-to-map
replacement, append and reopen through the successor, repeated empty-head sweeps,
and an almost entirely live tail that must not rotate. The combined rotation/map
fault sweep exercises 95 allocation, write/sync and read sites across both
policies, including torn writes and same-process allocation retry. Every cut
reopens with the accepted bodies intact and converges on exactly one successor.
A concurrency case pauses an actual replacement-map write after sealing: a
same-namespace replacement, sibling publication and admitted old reader finish
before the copy resumes. The stale map is rejected and a fresh checkpoint/retry
completes without another rotation.

The ASan/UBSan Debug gate passes eighteen groups covering standard Pouch/header/
source boundaries, compaction comparisons, checksum validation, lower physical
replacement and the new tail cases. Its public maintenance sweeps cover 354
allocation and 420 I/O cuts. Additional affected public key-bound, maintenance,
concurrency, full-capacity and namespace churn gates pass. GNU and native musl
Release each pass seventeen affected groups with default workers. Four
ThreadSanitizer invocations pass with instrumented OpenSSL 3.6.5, 128 KiB workers
and no suppressions. Four strict Memcheck invocations cover lower tail faults,
copy concurrency, churn and public full-capacity tails: 196,516 matched
allocations/frees, zero live heap, errors, suppressions or owned descriptors.
Evidence is retained under `build/pouch-space-tails/`: `final-debug.log`,
`expectations-debug.log`, `public-debug.log`, `threshold-debug.log`,
`qualification-gnu.log`, `qualification-musl.log`, `qualification-tsan.log` and
`memcheck-*.log`.

The matched GNU comparison verifies 48 indexed runs against the frozen computed
reserve build (`a151b354`): eight configurations, three samples per engine,
24 x 4 KiB or 8 x 64 KiB bodies, one/four writers, both policies, preopened
namespaces and four rounds. Ratios below are medians across per-configuration
median-of-three ratios; lower is faster. Work, indexed readiness, reopen and close
remain separately measured.

| Measurement / prior container | NoSync | Durable |
| --- | ---: | ---: |
| Measured work | 0.92x | 0.94x |
| Work CPU | 1.06x | 1.01x |
| p50 write | 1.03x | 1.05x |
| Indexed worker | 0.84x | 1.08x |
| Reopen and verify | 1.08x | 1.07x |
| Close | 0.99x | 1.04x |

Per-configuration work ratios range from 0.88-1.02x for NoSync and 0.85-1.11x for
durable work. Median reopen/verify is about 7.1/6.2 ms, close about 0.48/0.46 ms
and indexed worker about 40.6/37.8 ms (NoSync/durable). Another 24 queue/outbox/XA
probes verify both policies and one/four writers with one sample per engine and
configuration. All single-writer workflow sync counts match; concurrent batching
varies. These small host samples support comparable foreground cost, not a
statistical speedup or a production latency commitment. They do not resolve the
earlier indexed-readiness and NoSync work gap against multifile. Raw results and
per-configuration medians are `paired-performance.log`, `workflow-performance.log`
and `paired-summary.json` in the same directory.

The matched churn probe keeps the stable 1 MiB body pin and replaces its 192 KiB
neighbor 52 times with no growth. The new namespace sweep visits present sources
and retires dead predecessors rather than assuming one forever-active source.
Both policies finish at 1,282,048 payload bytes and 81 present mappings, versus
1,290,240 and 79 previously. Completing tails increases selected map bytes over
the run from 447,166 to 891,128. Logical history is 11,299,172 encoded bytes; for
this implementation that is aggregate emitted history across source generations,
whereas the frozen fixture reports one source's tail. Independent lower tests
still cover sparse single-source offsets beyond physical capacity. Three
alternating-order samples have median combined-policy fixture wall time
3.37 seconds versus 3.48 seconds, including byte/CRC assertions and reopen.
This is a bounded ownership and cleanup result with a measured copy cost, not
a per-operation latency claim. See `churn-performance.log` and `churn-times.log`.

Covered profitable partial-tail completion is now integrated. Retention control
phases and transaction participants still need complete mandatory-work plans and
public capacity proof. Inherited checksum validation, abandoned private-grant
cleanup, collection REGION relocation, codec conversion and shared-root
coordination/recovery remain required. Source labels also retain their existing
namespace-lifetime intern contract; this batch does not prove a bound on all
resident metadata over indefinite rotation history. The full goal remains active,
the multifile default is retained and production cutover requires an explicit
deployment decision.

### Native retention identity and progress capacity

The storage boundary now exchanges structured retention facts rather than LPR1
text. The multifile owner retains its existing codec and atomic file protocol.
The container stores key bytes directly, so percent encoding and filename
conventions no longer determine its work representation. This remains bounded,
intentionally materialized control metadata; no application body or transaction
outcome is copied into it. Public C APIs and their ABI are unchanged.

Control slot 2 remains the durable discovery marker. Slot 3 owns one immutable
identity: an 80-byte header containing the XID and initial facts, followed by the
raw key. The complete value is exactly 80 bytes plus key length. Slot 7 owns
an 80-byte progress record naming that identity's exact container/incarnation,
with active/finished phase, fencing token and state version. Slots 4 through 6
remain the ordinary, terminal and collection maintenance hints. Both the first
claim and the finish now replace only progress, without rewriting a large key.
Checksums and owner/length/field validation cover both representations. Exact
key comparison uses bounded read buffers rather than treating a key CRC as
identity authority.

Reads capture identity and progress from one completed physical prefix before
reading payloads outside publication authority. A completion for an older
incarnation cannot finish a later work item, even when every logical fact and
the XID/key are reused. Returning from finished to active creates a new identity.
Idle durably removes the identity and releases its credits. Its obsolete small
progress value can remain until hint retirement; absence of identity means idle,
and the old incarnation cannot affect a later identity. This avoids a second
idle durability barrier on every work cycle. Hint retirement clears progress
before removing discovery. Controls remain durable on NoSync roots too.

An initial zero-fenced reference admits its identity and two progress publications
together: one for claim facts and one for finish. After claim publication, it
retains exactly the final publication's capacity. Already claimed work needs
only that final credit. Unused identity allocation slack returns after durable
selection. A further active fact change must admit its replacement while keeping
the remaining mandatory credits; rejection leaves accepted facts intact. Finished
or idle work retains no completion claim. These are capacity claims under the
existing allocator contract, not guarantees against memory or I/O failure;
spent credits and failed private attempts retain the existing retry rules.

Root admission reconstructs these summed claims before exposing ordinary
writers. A scalar absence check avoids work payload I/O for namespaces without
an identity. Existing work needs only a bounded header/progress read, without
materializing its key or reading application bodies. Selected progress resolves
whether to restore two publications, one or none. Malformed work remains a
namespace-local consumption error with discovery intact; it does not prevent
opening unrelated namespaces. The standing key maintenance plan also includes
a bounded raw-key identity and both progress publications, in addition to its
existing deletion, key-checkpoint, coverage, routing and relocation allowances.

The 1 MiB percent-heavy key fixture holds a raw identity reader and lets peers
claim all remaining capacity. Both warm and reopened owners publish claim and
finish with 168 bytes of actual payload/journal writes each, under both policies.
The identity's incarnation and original bytes remain unchanged. A further fact
change rejected for capacity leaves the funded finish available. Reusing the
facts after finish creates a different identity; stale progress cannot complete
it. Idle uses one durability barrier. Bootstrap exercises 56 allocation cuts per
policy, including partial-owner cleanup and subsequent successful admission.

Another fixture starts 24 independent zero-fenced work references, fills ordinary
capacity at 10 MiB, reopens and observes all 48 progress-page credits restored.
While a maintenance peer holds the remaining capacity, every reference publishes
its claim. Ordinary writers then consume released slack. All 24 finishes still
succeed. After ordinary writers fill again, reopen restores no claims for finished
work and all accepted facts survive. No file growth is performed. A paused-read
case also advances both selected controls while an old captured read is suspended:
the old complete tuple and the new complete tuple each remain coherent.

The final ASan/UBSan Debug gate passes seventeen groups: public header/Pouch/source
boundaries, all seven lease/expiry groups, retention fault sweeps, staging recovery,
full-capacity tail reclamation, large-key deletion and root obligation admission.
The control gate and subsequent raw-identity pin readback also pass. Allocation
sweeps exercise 1,072 warm/cold cuts across both owners/policies, including NULL
error outputs; physical fault sweeps exercise 148 lost/torn write, barrier and
read cuts; full-capacity retry exercises another 108 allocation cuts. Those
sweeps include active claim-fact updates as well as completion and hint retirement.
GNU and native musl Release each pass all seventeen final affected groups with
normal workers. Two ThreadSanitizer invocations pass the control concurrency and
capacity/recovery fixtures using instrumented OpenSSL 3.6.5, 128 KiB workers and
no suppressions. Three strict Memcheck invocations cover progress/capacity,
concurrency and capacity faults: 53,533 matched allocations/frees, zero live heap,
errors, suppressions or owned descriptors.

Evidence is under `build/pouch-space-retention/`: `complete-debug.log`,
`complete-debug-details.log`, `complete-controls-debug.log`, `proof-debug.log`,
`qualified-gnu.log`, `qualified-musl.log`, `qualified-tsan.log` and
`memcheck-*.log`. Earlier prototype/iteration logs in that directory are not the
final qualification results. The private control representation changes without
a format-marker bump or automatic existing-root conversion; these fixtures use
fresh experimental roots.

The matched GNU control comparison records 72 measurements: three samples of
both current owners, both policies, warm/reopened ownership and 32-byte, 8 KiB or
1 MiB percent-heavy keys. Each measurement verifies eight four-step sequences:
initial zero-fenced reference, claim facts, finished and idle. Engine/policy order
reverses for the middle sample. The following ratios are medians across
per-configuration median-of-three ratios; lower is faster.

| Key bytes | Control work / multifile | Control CPU / multifile |
| --- | ---: | ---: |
| 32 | 0.20x | 0.29x |
| 8,192 | 0.31x | 0.34x |
| 1,048,576 | 0.25x | 0.23x |

Median container/file elapsed measurements are about 58.6/287.8, 93.7/289.1 and
185.0/736.4 ms respectively. These cover all eight control sequences and their
readback assertions, not one full expiry transaction. Transaction journals,
leases, parent deletion and attachments are outside this timing. Active 1 MiB
work has median owner-reopen time 3.92 ms versus 0.43 ms for multifile, and close
0.04 ms versus 0.23 ms. Reopen measures admission before subsequent work validation;
the container restores mandatory credits there. These are reopened owners with
ordinary OS caching, not cold-disk measurements.

A separate 48-run indexed comparison uses the frozen covered-tail build
(`b692b873`) with the unchanged eight configurations and three samples per engine:
24 x 4 KiB or 8 x 64 KiB bodies, one/four writers, both policies, preopened
namespaces and four rounds. Work, readiness, reopen and close stay separate.

| Measurement / prior container | NoSync | Durable |
| --- | ---: | ---: |
| Measured work | 0.97x | 1.03x |
| Work CPU | 0.99x | 1.10x |
| p50 write | 0.99x | 1.01x |
| Indexed worker | 1.02x | 0.99x |
| Reopen and verify | 0.99x | 0.92x |
| Close | 1.09x | 1.09x |

Per-configuration work ratios range from 0.81-1.12x for NoSync and 1.00-1.06x for
durable work. Median current indexed-worker time is about 47.5/38.9 ms,
reopen/verify 8.6/7.9 ms and close 0.93/0.82 ms (NoSync/durable). All single-writer
sync-call medians match the frozen build; concurrent batching varies. The durable
CPU increase is recorded rather than treated as a demonstrated no-regression
result. These small host samples establish measured component costs, not
production latency commitments. The earlier NoSync work/indexed-readiness gap
against multifile remains unresolved. Raw measurements and complete medians are
`retention-performance.log`, `paired-index-performance.log` and
`performance-summary.json` in the same directory. The earlier three-step prototype
probe is not used in these final comparisons.

This completes the native retention reference and protected claim/finish control
sequence. It does not fund the transaction journal and participant mutations
started after reference admission. Complete public expiry and transactional
messaging at capacity still require admission and recovery of those compound
operations, including their staging, lease and attachment dependencies. Inherited
checksum proof, abandoned private-grant cleanup, collection REGION relocation,
codec conversion and shared-root coordination/recovery also remain. The full goal
is active, multifile remains the default/reference, and production cutover still
requires an explicit deployment decision.

### Compound canonical batch admission

The production binary-record path now supplies its known key and metadata
prefixes through the typed storage boundary. The container plans all changed-key
checkpoint and coverage costs before creating the private source/frame, and
combines the additional credits with its source, grant, binding and routing
plan. One parent budget admits those costs together. Future deletion requirements
are raised during planning and unused or rejected changes restore their previous
requirements. Planning visits the declared prefixes and their key slots, without
walking unrelated keys or reading application bodies.

The space owner can transfer unspent credits from that parent into the existing
checkpoint completion owner. This transfer changes no aggregate root claim or
checkpoint workspace, allocates nothing and performs no I/O, trimming or second
capacity admission. The source cannot have active writers; the destination may.
Ordinary credits can fund completion, while maintenance credits cannot become
ordinary credits. Handle lifetime and failed-attempt accounting retain the
existing budget contract.

Actual prefixes must belong to the admitted key set and stay inside its metadata
and future-deletion ceilings. They cannot introduce additional checkpoint debt
after peers consume the remaining ordinary capacity. A declared span mismatch,
provider error or later undeclared key fails without selecting a partial frame.
The prefix provider is borrowed only until admission returns. Query/index facts
can still change during ordinary generation assignment within the admitted
bounds. Even when existing grants and funded key debt make the additional plan
empty, a new ordinary frame checks the foreground floor. Already admitted work
does not pay that floor again.

The common path borrows its existing binary item array; this adds no body buffer,
spool or application serialization pass. Unknown prefixes, including the normal
body-producing streaming path, retain incremental prefix admission through the
same append owner. Public C APIs, ABI and persistence representations are
unchanged. This completes admission for a known canonical participant frame;
it does not yet admit an entire transaction journal and all participants as one
compound operation.

Qualification also found a routing capacity omission: a 128-byte replacement
head can split across two packed tails even when its predecessor has one extent.
Routing plans now allow 112 metadata bytes for creation and 24 positive-growth
bytes for replacement. The regression fixture leaves exactly one cell in a packed
page, then creates or replaces the head through its budget and checks readback
after reopen, under both policies.

Evidence is in ignored `build/pouch-space-batches/`:

- 28 targeted Debug ASan/UBSan groups pass, including public staging, metadata
  mutation/capacity, lease phases and coordinated expiry, checkpoint/deletion
  ownership, frame recovery, budget primitives and four standard source/API
  boundary checks. Two fixtures still assumed lazy namespace recovery; their
  focused rerun passes after asserting trusted-prefix rejection at root open and
  independent readers/writers while a source read is blocked. See
  `qualified-debug.log` and `fixtures-debug.log`. Two collection background
  groups additionally pass under both policies (`collection-debug.log`), with
  discovery preserving the recovered owner and live staged bodies surviving
  maintenance through the current key-checkpoint representation.
- GNU Release passes 15 selected groups, including durable outbox and
  cross-namespace transaction workflows; musl Release passes 13. These are
  `qualified-gnu.log` and `qualified-musl.log`.
- The new planned-batch fault mode covers 246 allocation and 144 write/sync cuts
  across both policies and initial/rotating sources. Each path checks retry and
  reopen authority. The incremental mode independently covers the same cut
  counts. The default-capacity batch fixture holds remaining capacity in a peer,
  checkpoints while the frame is private, finishes two keys and their checkpoint
  under that peer, and verifies bodies before and after reopen. It also checks
  a blocked prefix provider, independent writer/reader progress, undeclared-key
  rejection before prefix advancement, and retry. Cut counts are retained in
  `fault-cuts.log`.
- Three TSan scenarios pass with instrumented OpenSSL 3.6.5 and no suppressions:
  batch preflight concurrency, blocked-reader concurrency and public staging
  concurrency. Two strict Memcheck probes cover batch admission and credit
  transfer: 15,105 allocations and frees, zero errors, zero heap at exit and only
  the three inherited file descriptors.

The matched performance probe alternates the frozen retention build (`ed37a4e3`)
and this batch across eight configurations, with three samples each: 24 x 4 KiB
or 8 x 64 KiB bodies, one/four writers and both policies, using preopened owners.
The indexed workload has four rounds (48 runs). The staged workload has two
rounds and also measures the current multifile owner (72 runs). Every run checks
readback and reopen. The table reports the median of the four per-configuration
ratios for each policy, after taking each configuration's three-sample median.
Values below one mean less elapsed time or CPU. Work excludes close and reopen.

| Measurement / frozen container | NoSync | Durable |
| --- | ---: | ---: |
| Staged work | 1.04x | 1.02x |
| Staged work CPU | 1.02x | 1.07x |
| Staged reopen | 1.02x | 1.11x |
| Staged reopen plus readback | 1.02x | 1.02x |
| Staged close | 0.93x | 1.07x |
| Indexed work | 0.92x | 0.96x |
| Indexed work CPU | 1.00x | 0.96x |
| Indexed worker | 0.76x | 0.95x |
| Indexed reopen plus verification | 0.97x | 1.00x |
| Indexed close | 0.94x | 0.94x |

Staged work ratios range from 0.74-1.11x for NoSync and 0.88-1.09x for durable
work against the frozen container. Its durable CPU ratios range from
0.87-1.37x; this is not proof of no regression. Current median staged work is
35.0/263.5 ms, reopen is 9.35/3.16 ms, reopen plus readback is 10.03/3.74 ms,
and close is 0.071/0.072 ms (NoSync/durable). Indexed-worker medians are
42.9/41.3 ms, reopen plus verification 7.31/6.92 ms and close 0.52/0.61 ms.
These are small-host observations with ordinary OS caching, not cold-disk or
production latency commitments.

Against multifile, current staged work is 2.06x/0.70x and CPU 1.43x/1.01x;
reopen is 14.15x/3.69x and reopen plus readback 4.13x/1.43x. Container close is
much shorter, with ratios 0.002x/0.004x, and is kept separate rather than used to
hide work or readiness cost. The NoSync work and reopen gaps remain unresolved.
The complete measurements and calculation are `paired-performance.log`,
`paired-performance.sh`, `performance-summary.tsv` and
`summarize-performance.awk` in the same evidence directory.

Full public expiry and transactional messaging at capacity still need admission
and recovery of the journal/participant/lease/attachment operation as a whole.
Inherited checksum proof, abandoned private-grant cleanup, collection REGION
relocation, codec conversion and shared-root coordination/recovery also remain.
The full goal stays active. Multifile remains the default/reference, and
production cutover requires an explicit deployment decision.

### Abandoned canonical capacity recovery

Bounded maintenance now retires wholly unused grants beyond a canonical source's
committed tail. Previously, a zero-completed grant stopped the sweep; failed or
over-reserved appends could retain physical capacity indefinitely. The owner now
durably rotates and seals a selected source before discarding its unused suffix.
These deletions need neither checkpoint coverage of uncommitted bytes nor
payload/footer reads or body copies. Completed pieces retain their existing
coverage and checksum requirements. Logical offsets and the source's capacity
high-water mark remain stable; admitted readers retain their physical mapping
until release.

Captured source epochs, committed tails, metadata views and mutation exclusion
protect selection. A writer changing the old tail defers the stale operation;
after rotation, successor writers, old-body readers and unrelated writers can
progress. A sweep reports pending grants even when the same bounded pass finishes
the tail, so the scheduler continues beyond its piece limit. Reaching the actual
end resets the cursor for the next liveness sweep.

An empty selected head can publish coverage without manufacturing a state/query
record. Existing keys and watermarks are preserved and obsolete empty predecessors
can retire. The computed maintenance plan includes both coverage publications,
rather than increasing a guessed fixed reserve. A failed first append's
unselected zero-tail binding is removed under producer exclusion; catalog removal
precedes physical retirement. Root orphan recovery completes interrupted
retirement, and the existing final physical checkpoint makes the pages reusable.
Persistence representations, public C APIs and ABI are unchanged.

Evidence is in ignored `build/pouch-space-abandoned/`:

- 32 selected Debug ASan/UBSan groups are covered by passing runs
  (`qualified-debug.log` plus the two repaired fixture checks in
  `fixtures-debug.log`). GNU Release passes 17 groups and musl Release 13.
  Existing fixtures now observe empty-head coverage advancement and drive
  advertised pending work through bounded passes instead of assuming a single
  pass completes every grant.
- Twenty selected-source cases cover both policies, reserve-only and flushed
  abandoned bytes, warm/reopened owners, full 10 MiB stores and a pinned 1 MiB
  live body. Cleanup issues zero body reads/copies; a later ordinary write and
  reopen verify reusable capacity and preserved data. Another 24 full-store
  empty-head cycles per policy keep the entry count stable and state/query
  watermarks zero, with periodic reopen. The initial abandoned half-MiB reservation
  supplies the fragmentation margin for subsequent quarter-MiB admissions;
  other capacity stays live and the file never grows.
- Deterministic faults cover 84 allocation, 49 write/sync and two read cuts
  across both policies and selected/unselected sources, followed by retry,
  crash/reopen and a successful ordinary write. Two best-effort allocation cuts
  complete successfully; the rest return the injected error. Counts and raw
  results are in `qualified-debug-details.log`.
- Public maintenance covers failed streaming producers and repeated cleanup of
  selected/unselected sources under all four codecs and both policies. A live
  producer defers cleanup; after close, bounded passes drain the work, retain
  one physical file and permit new writes. Two TSan scenarios pass with
  instrumented OpenSSL 3.6.5 and no suppressions. Two strict Memcheck probes
  account for 43,281 allocations/frees, zero errors, no retained heap and only
  the three inherited descriptors.

The cleanup comparison relinks the same current fixture against the frozen
`9c01fe6d` backend and alternates old/new order over three samples, two body sizes
and both policies (24 verified observations). Each current case reclaims 17
unused grants, or 278,528 bytes, with zero retained unused bytes. The frozen
backend retains all 278,528 bytes; its no-op timing is not useful reclamation
performance. Current medians keep cleanup and the final physical checkpoint
separate:

| Policy / retained body | Cleanup | CPU | Syncs | Physical checkpoint |
| --- | ---: | ---: | ---: | ---: |
| NoSync / 19,775 bytes | 23.17 ms | 1.21 ms | 20 | 7.56 ms |
| NoSync / 1,062,207 bytes | 20.83 ms | 1.51 ms | 20 | 6.81 ms |
| Durable / 19,775 bytes | 26.00 ms | 1.42 ms | 21 | 7.14 ms |
| Durable / 1,062,207 bytes | 26.15 ms | 1.51 ms | 21 | 7.13 ms |

All four cases write 1,596 bytes in 22 calls during cleanup. It still selects
each deletion durably, exposing a per-grant barrier cost that future batching
can reduce. The three-sample checkpoint ranges extend to 84.62 ms; these cached,
small-host observations are not production latency guarantees. Exact counts,
ranges and calculations are in `paired-performance.log`, `cleanup-summary.tsv`
and `summarize-cleanup.awk`.

A separate matched indexed workload runs the same eight configurations and
three samples per backend as the preceding milestone (48 verified runs). After
taking each configuration's sample median, the median current/prior ratio is
1.05x/0.89x for measured work, 1.02x/0.92x for CPU, 0.91x/0.94x for indexed worker,
1.05x/1.10x for reopen plus verification and 1.04x/1.14x for close
(NoSync/durable). Work ratios range 0.93-1.07x and 0.84-0.99x respectively.
This does not establish a universal no-regression result. Work, readiness,
reopen and close remain separate in `performance-summary.tsv`; the earlier
NoSync and reopen gaps against multifile remain unresolved.

Abandoned canonical grant and first-append binding cleanup are now implemented.
Full public expiry and transactional messaging at capacity still need admission
and recovery of the journal/participant/lease/attachment operation as a whole.
Inherited checksum proof, collection REGION relocation, codec conversion and
shared-root coordination/recovery remain. Interned source labels still live for
the namespace lifetime; stable live entry counts do not prove bounded resident
label history. The full goal stays active, multifile remains the default/reference,
and production cutover requires an explicit deployment decision.

### Protected terminal transaction application

Terminal staged-decision records now share maintenance admission with their
tombstones. Previously, adding a terminal decision to an otherwise deleting
batch changed its admission class to ordinary, so a durable cleanup decision
could stop at its first participant despite protected capacity being available.
Payload links and ordinary metadata still retain ordinary admission. The common
binary batch path continues to admit the complete frame and its checkpoint
dependencies before selecting any record.

Validated lease clearing and restoration of a committed cleanup's fenced lease
now explicitly classify their bodyless metadata updates as maintenance
completion. The private write-options field defaults to false. It conveys
capacity class only: the existing namespace/transaction authority, fencing,
preconditions, canonical validation and durability barriers still apply. Such
updates stay on their caller instead of entering the ordinary metadata batcher.
The restored lease retains the work reference's original fencing/state facts
after a parent tombstone has removed the prior metadata. Public APIs, shared
library ABI and persistence representations are unchanged.

The new `pouch_space_lease_capacity-completion` group proves 56 workflows across
all four codecs and both policies: 24 successful expiry workflows and 32 injected
failures from decision through data durability. Half of those failure cases
reopen before retry, reacquire an ordinary peer claim and observe the still-ACTIVE
reservation. Every retry preserves the committed outcome with the peer
held even after the wall clock retreats. A reader admitted before expiry still
reads the original body; later reads and reopen see the deleted parent and
attachments. Each fixture retains a fixed 10 MiB file.

The peer holds a large ordinary page claim near the admission boundary and
preserves the computed maintenance floor. This is a capacity-pressure probe,
not proof that every usable cell or all-live physical capacity is exhausted.
Its binary search does not forbid subsequent small ordinary work from fitting
in existing grants or released checkpoint credits.

The broader manual `capacity-profile` mode runs 96 public workflows: two policies,
four codecs, 5-byte/8 KiB parent keys, zero/16 attachments, and peer admission
before cleanup or after staging, plus a no-peer control. It records live and
allocated bytes, metadata, credits, standing floor, checkpoint workspace,
entries and barriers at each observed phase. Both versions verify every final
parent/attachment outcome after releasing the peer and reopening.

| Workflow / eight codec-policy combinations | Prior algorithms | Current |
| --- | ---: | ---: |
| No peer, all four key/attachment profiles | 32/32 | 32/32 |
| Peer before cleanup, short key without attachments | 0/8 | 8/8 |
| Peer before cleanup, other three profiles | 0/24 | 0/24 |
| Peer after staging, short key or no attachments | 0/24 | 24/24 |
| Peer after staging, 8 KiB key with 16 attachments | 0/8 | 0/8 |

The current operation therefore completes 64/96 cases while the peer is held,
versus 32/96 previously. This is not complete compound admission. The 24 early
failures stop at journal preparation and unwind their reference; the remaining
eight reach parent application but cannot finish attachment application until
capacity returns. In the plaintext/NoSync large-key example, live bytes grow
from 1,246,066 after claiming to 1,662,334 after staging, while the standing
payload floor is 81,920 bytes. The per-key maximum does not represent that
operation's cumulative dependencies. Completing a funded checkpoint and retrying
the next append did not repair this case; that attempted retry was removed.

`capacity-required` runs the same matrix with a strict completion requirement
and currently fails. `capacity-tail-probe` isolates the remaining post-staging
case. These intentional manual acceptance gates are not registered as green
CTest groups. Their remaining obligation is to admit the journal, lease,
participant, attachment and checkpoint work together, with recovery reconstructing
unfinished obligations before foreground readmission. A volatile live-call
budget alone, or charging the entire original plan again after partial completion,
would not establish that property. No second outcome journal or fixed-floor
increase was introduced by this change.

Evidence is in ignored `build/pouch-space-transaction-capacity/`. Twelve selected
Debug ASan/UBSan groups pass, including lease phase failures, concurrency,
coordinated transactions, combined-codec process cuts, stage recovery and four
standard source/API boundary checks. GNU Release passes six groups, including
plaintext and combined-codec XA capacity/retry, and musl Release passes four.
Two TSan scenarios pass with instrumented OpenSSL 3.6.5 and no suppressions.
Strict Memcheck covers the 56 new workflows with 418,008 allocations/frees, zero
errors, no retained heap and only the three inherited descriptors.

The comparison control recompiles the `927688d4` state/client algorithms against
the current private layouts and dependencies, then relinks the same current
fixture and physical backend (`build-baseline.sh`). The old algorithms fail the
new strict completion fixture; the whole-operation gate still fails currently.
`profile-before.log`, `profile-gnu.log`, `baseline-required.log`,
`acceptance-required.log` and `required-status.log` retain those results.

The separate uncontended comparison alternates algorithms over three samples,
plaintext/combined codecs and both policies (24 verified runs), using a preopened
root, a short parent key and one attachment. The median of the two configuration
ratios after taking each configuration's three-sample median is:

| Current / prior algorithms | NoSync | Durable |
| --- | ---: | ---: |
| Expiry work | 1.08x | 0.99x |
| Work CPU | 0.95x | 0.95x |
| Close | 0.84x | 1.08x |
| Reopen | 1.03x | 1.14x |
| Reopen plus verification | 1.02x | 1.15x |

Current work medians are 22.92/21.16 ms, CPU 1.32/1.48 ms, close 0.044/0.076 ms,
reopen 1.53/1.70 ms and reopen plus verification 1.59/1.79 ms (NoSync/durable).
The 32 no-peer profiles also retain identical per-operation barrier counts in
the broader comparison. These cached small-host samples do not establish a
universal no-regression result or production latency guarantee. Query indexing
is disabled in this expiry probe; it does not measure indexed readiness. The
earlier indexing and multifile gaps remain open. Raw measurements and derivation
are `paired-performance.log`, `paired-performance.sh`, `performance-summary.tsv`
and `summarize-performance.awk`.

The full backend goal remains active, including compound admission/recovery,
inherited checksum proof, collection REGION relocation, codec conversion and
shared-root coordination/recovery. Multifile remains the default/reference;
production cutover still needs an explicit deployment decision.

### Native canonical metadata publication

The typed owner now publishes bodyless mutations directly into canonical key
values. One physical revision selects every changed key, retained decision and
namespace coverage/high-water value together. Common Pouch uses a required
`begin`/`commit`/`close` metadata capability; its typed bodyless path no longer
appends a canonical byte stream and then creates another key checkpoint.
Application bodies retain their existing producer-to-stream path and encoded
span references. Independent metadata requests coalesce within an 8 MiB window
and the existing request-count bound; individual key/metadata limits remain.

Begin admits the complete borrowed-prefix plan before generation finalization.
Commit rejects undeclared keys, producer bytes and prefixes exceeding the plan,
prepares immutable metadata values, then publishes their ownership descriptors
atomically. It does not concatenate application bodies or invoke a producer
under physical publication authority. Metadata-only namespaces can have selected
coverage with segment and offset both zero, without manufacturing a log source.

Before a metadata mutation, already-funded stream checkpoint debt is selected
in deferred mode. The following commit or caller's durability scope supplies the
required completed prefix. This preserves folding PUT, META and DELETE work into
one explicit barrier. CONTROL members and explicitly forced revisions still
wait on NoSync roots. Future-deletion credits remain at their older bound until
the replacement prefix is durable; an intrusive per-key retirement list observes
the root's scalar publication/durability prefixes. A newer streaming mutation
takes over that obligation until its checkpoint completes. There is no second
persistent outcome ledger.

Metadata captures now select key values and coverage at one visibility prefix.
They use the captured control's physical incarnation to distinguish an older
durable view from the owner's newer deferred revision. Captured readers retain
their original values through replacement and physical checkpoints. Borrowed
prefix providers run outside the metadata owner's mutex, so a paused provider
allows readers and another namespace's writer to finish.

Staged delete intents now contain metadata and a lifetime without an encoded
empty application body. They validate an empty producer before selection,
including propagating producer errors when the caller omits its error object.
Attachment application reads metadata first and opens a body only for a real
staged PUT. Explicit intents can be retired; deleting an ordinary lease-only
placeholder retains its existing no-state behavior. Object classification is
preserved on bodyless META records through the existing canonical object flag.
This is an internal representation cutover with no public API/ABI change or
independent format-marker increment. Existing-root conversion remains deferred.

Maintenance now recognizes canonical metadata even when no log source exists.
A forced public pass checkpoints the physical prefix and makes superseded
metadata reusable. The new maintenance fixture replaces 16 MiB of metadata in
a fixed 10 MiB container, periodically drives public maintenance, observes
physical allocation fall, and verifies the final value after reopen. Plain and
combined codecs run under both policies. No source or capacity growth is needed.
Source-tail threshold tests now use actual streamed records; their physical
reclamation and pinned-body checks remain.

The three new native metadata groups prove source-less reopen, multi-key and
decision atomicity, finalized-plan rejection, deferred visibility, old captured
values, future-deletion credit retirement, and paused-provider concurrency.
They exercise 129 allocation/write/sync/captured-read failure points across both
policies. The main Debug ASan/UBSan qualification passes 59 selected groups,
including all eight query SDK configurations, queue/outbox SDK controls,
lease policy/phase/concurrency/coordinated workflows, 1,878 staging failure
sites, and 324 allocation plus 396 I/O maintenance failure sites. Supplementary
staging/crash checks pass 208 process-death/lost-write scenarios, targeting the
native revision header and incomplete member prefixes. Their durable seed is
explicit, including for NoSync bodyless intents. Focused final checks also prove
the metadata-only maintenance repair and captured values across root checkpoint.

GNU Release passes ten selected groups, including shared public-symbol and API
boundaries; musl Release passes six. Both additionally pass the final canonical
metadata/maintenance checks. Five TSan scenarios pass with instrumented OpenSSL
and no suppressions. Strict Memcheck passes native metadata failure injection,
the 56-workflow capacity-completion fixture and maintenance: 409,622 allocations
and matching frees, zero errors, no retained heap, and only three inherited
descriptors in each process.

The broader pressure matrix remains **64/96** completed with the ordinary peer
claim held. All 96 still recover after release. The standing floor is unchanged;
this is not complete compound admission. In the plaintext/NoSync 8 KiB-key,
16-attachment case, live bytes after staging fall from 1,662,334 to 1,189,036 and
allocated bytes from 1,994,752 to 1,597,440, while the payload floor remains
81,920 bytes. The operation still cannot finish attachment application with that
peer held. The earlier 24 journal-prepare failures also remain. The strict manual
`capacity-required` gate still fails and is not registered as a green test.

The matched uncontended expiry comparison uses the frozen `aa60a5c6` GNU fixture,
the same one-parent/one-attachment workflow, plaintext/combined codecs and both
policies. Three alternating samples per configuration produce 24 verified runs.
Taking each configuration's median, then the median of its two codec ratios:

| Native metadata / prior single-file algorithms | NoSync | Durable |
| --- | ---: | ---: |
| Expiry work | 1.29x | 1.14x |
| Work CPU | 1.27x | 1.17x |
| Close | 1.15x | 1.01x |
| Reopen | 0.97x | 1.08x |
| Reopen plus verification | 0.95x | 1.05x |

Current work medians are 32.74/30.00 ms and CPU 2.04/1.89 ms
(NoSync/durable). This is a measured regression in that small workflow. Eager
canonical key publication replaces pending log updates; the zero-attachment
NoSync phase probe actually uses two fewer barriers, so extra forced barriers
do not explain its cost. These expiry probes disable query indexing.

Separate current single-file/multifile comparisons use three alternating
samples, plaintext/no compression, preopened roots and two independent writers.
The metadata probe seeds 48 bodies of 4 KiB before measuring two update rounds.
The table gives single-file / multifile median times in milliseconds:

| Metadata phase | NoSync | Durable |
| --- | ---: | ---: |
| Update work | 4.82 / 15.34 | 314.73 / 321.61 |
| Work CPU | 7.33 / 4.84 | 23.44 / 22.04 |
| Close | 0.15 / 26.49 | 0.22 / 24.85 |
| Reopen | 8.62 / 0.73 | 2.01 / 0.59 |
| Reopen plus read verification | 9.25 / 2.79 | 2.49 / 2.15 |

The indexed lifecycle probe uses 16 records of 1 KiB, two rounds, 24 verified
indexed queries per run, and explicit flush/first/repeat query phases:

| Indexed phase, single-file / multifile milliseconds | NoSync | Durable |
| --- | ---: | ---: |
| Lifecycle work including indexed readiness | 58.47 / 22.78 | 637.47 / 440.69 |
| Work CPU | 15.20 / 15.57 | 48.35 / 49.83 |
| Maximum worker index flush | 18.74 / 1.47 | 19.66 / 1.49 |
| Close | 0.43 / 34.72 | 0.43 / 38.76 |
| Reopen plus verification | 4.08 / 1.12 | 4.24 / 1.44 |

The smaller footprint and faster NoSync metadata wall time do not establish a
general performance acceptance. Indexed readiness and reopen remain substantial
targets, and the small expiry regression remains open. These are cached
small-host samples; work, readiness, close and reopen are kept separate.

Evidence is in ignored `build/pouch-space-native-metadata/`: `qualified-debug*`,
`crash-debug*`, `metadata-maintenance-debug*`, `qualified-gnu*`, `qualified-musl*`,
`final-*`, `qualified-tsan.log`, `qualified-memcheck.log`, `memcheck-*`,
`profile-gnu.log`, `acceptance-required.log`, `acceptance-status.log`,
`paired-performance.log`, `performance-summary.tsv`, `reference-performance.log`
and `reference-summary.tsv`. The paired/reference scripts and summary derivations
are retained there. The full backend goal remains active: compound admission
and recovery, inherited checksum proof, collection REGION relocation, codec
conversion, shared-root coordination/recovery, and performance qualification.
Multifile remains the default/reference; production cutover still requires an
explicit deployment decision.

### Parent-owned attachment cleanup and complete control admission

Retention cleanup now treats the fenced parent as the owner of its committed
attachment set. The existing `.txns` journal remains the sole outcome authority;
cleanup stages one parent delete intent, without creating a separate intent for
every attachment. After a durable commit it applies the parent, deletes the live
attachment prefix, clears the lease, forces application durability, records
FINISHED, retires the journal and finally releases the work reference. Rollback
discards the parent intent and leaves committed attachments alone. Ordinary user
transactions and abandoned-holder attachment rollback retain their own staging
protocol. No second outcome ledger or plaintext journal representation is added.

Application copies at most 16 payload keys per window, retaining a cursor and
prefix alongside them. It holds namespace mutation authority, retains no cache
record pointers across mutations, and rediscovers remaining live children after
interruption. ACTIVE work continues to block a replacement lease even after the
parent tombstone is visible. This bounds temporary key copies, not total scan
work: each window scans the resident namespace metadata, and one attempt still
processes the entire parent. Large-fan-out scan complexity remains a target.

The private maintenance-completion classification now admits the complete frame
and changed-key checkpoint for a control body that its caller already holds in
memory, within the existing 64 KiB memory-transform limit. The actual compressed
or encrypted representation, CRC, descriptor and body length are known before
admission. Arbitrary producers and larger memory sources retain their streaming
append path; no application body is newly materialized or spooled. Journal JSON,
crypto contexts and forced decision ordering remain unchanged.

Completion can introduce bounded control slots after spending the standing
floor. Those slots still retain future-deletion obligations, while the complete
frame/checkpoint plan owns immediate admission. Ordinary writes continue to
preserve the floor. Before parent application and each child deletion, the typed
owner probes a conservative completion-space hint and checkpoints a completed
physical prefix only when that hint cannot fit. Actual begin remains the
authoritative admission boundary. Private producers and readers retaining old
body incarnations preserve their claims through reclamation.

The previous 64/96 public pressure result is now **96/96**, without container
growth. `capacity-required` is registered as an acceptance check. Its matrix
covers both durability policies, all four codecs, 5-byte and 8 KiB parent keys,
zero or 16 attachments, and ordinary peer claims established before preparation
or after staging, alongside uncontended cases. Each case checks a previously
opened body reader, fixed 10 MiB size, physical consistency and repeated reopen.
This is a workload-specific proof: it does not qualify every legal key size,
control body above 64 KiB, or every possible collection of concurrent claims.

`attachment-windows` adds 48 workflows across both backends, both policies and
all four codecs, with 33 attachments. Cuts after child 1 and child 17 prove
partial deletion, reopen, clock retreat, fencing and completion; native cases
retain an ordinary peer during application and retry. An unrelated parent and
its attachment must survive. Child application is also a deterministic failure
and abrupt-process cut in the existing phase suites.

Qualification for this surface:

- 46 Debug checks, including query, queue, outbox, leases, transaction recovery,
  native metadata, ordinary capacity, root obligations and API/source boundaries;
  glibc workers use 128 KiB stacks with the stack cache disabled.
- 11 GNU release and 11 musl release checks, including both new acceptance modes,
  phase/process recovery and complete batch admission.
- One combined-codec NoSync allocation sweep: 523 injected failures with both
  retry paths. One combined-codec durable I/O sweep: 480 cuts across warm/cold
  roots, including torn writes and lost acknowledgements.
- Three ThreadSanitizer scenarios with instrumented OpenSSL and no suppressions:
  native metadata concurrency, lease concurrency and attachment windows.
- Strict Memcheck for attachment windows and metadata faults: 1,127,918 matched
  allocations/frees, zero remaining heap, zero errors and zero owned descriptors.

Matched performance uses the frozen `014f350a` GNU lease binary and this surface,
three alternating samples, fixed 10 MiB roots and preopened workloads. Plaintext
is codec 0; combined encryption/compression is codec 3. For an 8 KiB parent with
16 attachments, before / after median milliseconds are:

| Policy | Codec | Cleanup work | Work CPU | Close | Reopen | Reopen plus verification |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 0 | 54.34 / 48.59 | 9.81 / 4.85 | 0.192 / 0.113 | 6.55 / 5.84 | 8.02 / 6.96 |
| NoSync | 3 | 51.60 / 42.71 | 9.49 / 4.88 | 0.160 / 0.133 | 6.70 / 6.24 | 8.25 / 7.31 |
| Durable | 0 | 50.71 / 64.33 | 10.08 / 5.42 | 0.224 / 0.132 | 7.35 / 5.73 | 8.79 / 6.73 |
| Durable | 3 | 47.56 / 38.18 | 9.61 / 5.47 | 0.170 / 0.188 | 7.02 / 6.15 | 8.53 / 7.17 |

Cleanup CPU falls 43–51%, but durable plaintext wall time rises 27%. Both variants
report 15 physical syncs through completion in that workload; the wall-time cause
is unresolved. In the smaller 5-byte-parent/one-attachment probe, wall time falls
1–15%, while combined-codec NoSync CPU rises 13% and reopen plus verification
rises from 1.53 to 2.73 ms. These mixed, small-host samples do not establish a
general latency acceptance. Query indexing is disabled in these expiry probes;
the prior indexed-readiness comparison remains a separate open target.

For the plaintext 8 KiB/16-attachment case, staging now selects 99 entries instead
of 115, with live bytes 1,052,912 instead of 1,189,036. At completion, allocation
is 1,638,400 instead of 1,912,832 bytes and live bytes are 919,453 instead of
1,053,881. This reduces retained metadata without copying application bodies.

Evidence is retained in ignored `build/pouch-space-prefix-cleanup/`:
`qualified-debug.log`, `allocation-debug.log`, `io-debug.log`,
`qualified-*-release.log`, `qualified-tsan.log`, `qualified-memcheck.log`,
`memcheck-*`, `paired-performance.log`, `performance-summary.tsv`,
`fanout-performance.log`, `fanout-summary.tsv` and the raw `fanout-*.log` runs.
Qualification and benchmark scripts, summary derivation and the frozen baseline
are retained there. Format markers and the public ABI are unchanged. Existing
in-flight experimental roots are not qualified for cross-build recovery; existing
root conversion remains deferred. The full backend goal remains active for
general capacity progress, scan complexity, latency and indexed readiness,
codec conversion and shared-root coordination/recovery. Multifile remains the
default/reference; production cutover still requires an explicit deployment
decision.

### Resident key order and bounded ordered reads

The logical projection now maintains an intrusive AVL index over the same cache
records and owned keys used by its hash index. Exact-key reads and mutations
retain hash lookup. Ordered pages, prefix visits and attachment key windows seek
directly to their lower bound and traverse successors. A window no longer scans
every namespace record to select its next 16 keys. Across contiguous windows,
work follows the matching range plus a logarithmic seek per window, including
matching tombstones that must be skipped. One maintenance attempt still processes
the entire parent; this does not introduce a per-parent time quota.

Record insertion is the single update point for both indexes. Replacements,
tombstones and resurrection keep their key position; cache invalidation clears
both indexes and replay reconstructs them. Rotations and traversal are iterative,
with no new node allocation, key copy or recursive worker-stack use. Existing
projection authority protects the topology. Visitors still receive owned
snapshots after that authority is released, including readers retained across
mutations and close. The additional struct storage is 32 bytes per cached key
and one root pointer per namespace on native x86_64. Total projection memory
still scales with namespace size.

Read-many now uses the existing hash index for every requested key, removing its
whole-namespace sort for requests above eight keys. Compaction capture, snapshot
writing and captured-reference validation also use resident order or direct
lookup. The temporary sorted-pointer/binary-search helpers are removed. A vector
remains only where an immutable replay capture explicitly transfers ownership of
all captured records; it is filled in key order without sorting. Small scan pages
also cap snapshot-array growth at the requested limit instead of initially
allocating 64 entries. Application body transport and persistent formats are
unchanged.

The new `ordered-reads` gate seeds 1,024 body keys in ascending, descending,
alternating-extreme and permuted order. It exercises tombstones, resurrection,
metadata-only keys, seven-row pagination, exact membership and order, prefix
boundaries, reopen, and deletion through 16-key windows while another key is
inserted. Plaintext and combined codecs run on both backends. A warmed seven-row
page must avoid a namespace-wide allocation; its largest allocation through the
Pouch allocator is **1,344 bytes**, against **12,288 bytes** in the prior fixture.
The inherited readers also cover all four codecs and both durability policies.

Qualification comprises 52 Debug checks, 9 GNU release checks and 9 musl release
checks. The broader batch includes query, queue, outbox, leases, pressure,
attachment windows, reads, immutable snapshots, projection, lifetimes, staging,
compaction and source/API boundaries. Reported deterministic fault sites include
3,598 read allocations, 1,281 read I/O operations, 737 logical-capture allocations,
1,388 lifetime allocation/I/O/torn operations, 324 maintenance allocations and
396 maintenance I/O operations. All passed. Three ThreadSanitizer scenarios use
instrumented OpenSSL with no suppressions: read concurrency, maintenance
concurrency and ordered reads. Strict Memcheck for ordered reads and projection
faults reports 1,093,469 matched allocations/frees, zero remaining heap, zero
errors and zero owned descriptors. Constrained glibc workers and musl workers
retain the 128 KiB stack qualification.

The page comparison uses the same GNU driver against frozen `f30a4a19` and current
test libraries, with matching build flags and three alternating samples. It
measures 128 seven-row reads from a preloaded projection of the 1,025-key fixture.
Reopen CPU is measured separately through the first verified body read, excluding
the preceding close. Median before / after thread CPU milliseconds are:

| Backend | Codec | Repeated pages, warm | Repeated pages after reopen | Reopen plus first read |
| --- | ---: | ---: | ---: | ---: |
| Single-file | 0 | 9.650 / 0.658 | 10.400 / 0.565 | 14.296 / 11.421 |
| Single-file | 3 | 8.903 / 0.689 | 10.476 / 0.616 | 11.053 / 11.834 |
| Multifile | 0 | 8.306 / 0.495 | 6.831 / 0.501 | 2.695 / 3.200 |
| Multifile | 3 | 9.269 / 0.638 | 8.412 / 0.608 | 2.752 / 3.090 |

Repeated-page CPU falls 13–18 times. Reopen CPU is mixed: combined-codec
single-file rises 7%, and multifile rises 12–19%, reflecting a cost that must not
be hidden by the repeated-read result. These samples do not establish a general
cold-start or point-mutation performance acceptance.

A separate production-binary comparison retains the 16-record, 1 KiB,
two-independent-writer, two-round indexed workload and verifies 24 indexed
queries per run. Three alternating samples compare the prior and current
binaries and both backends. Current single-file / multifile median milliseconds
keep work, indexed readiness, close and reopen separate:

| Phase | NoSync | Durable |
| --- | ---: | ---: |
| Lifecycle work including readiness | 62.13 / 20.62 | 596.79 / 495.60 |
| Work CPU | 16.99 / 13.58 | 45.92 / 46.39 |
| Maximum worker index flush | 18.02 / 0.87 | 13.98 / 1.15 |
| Sum of index-flush thread CPU | 2.32 / 1.52 | 2.20 / 2.23 |
| Close | 0.35 / 36.57 | 0.35 / 33.98 |
| Reopen plus verification | 3.25 / 1.53 | 3.93 / 1.25 |

The native index still performs four forced readiness syncs under either policy;
the multifile index-flush phase reports zero. Native index-flush CPU decreases
modestly, while lifecycle wall time changes in both directions. The ordered-read
improvement does not resolve that durability/readiness gap or the prior durable
plaintext expiry wall-time regression.

Evidence is in ignored `build/pouch-space-ordered-keys/`: `qualified-debug*`,
`qualified-*-release*`, `qualified-tsan.log`, `qualified-memcheck.log`,
`memcheck-*`, `ordered-*.log`, `paired-performance.log`, `performance-summary.tsv`,
`lifecycle-performance.log` and `lifecycle-summary.tsv`, alongside the frozen
libraries/binaries, exact relink commands and summary scripts. Multifile remains
the default/reference. The full goal stays active for general maintenance and
capacity qualification, larger control records, native readiness and reopen,
codec conversion and shared-root coordination/recovery; existing-root conversion
and the deployment decision remain outside this cutover.

### Streaming codec conversion into bounded native body packs

Canonical body conversion now runs through `lc_pouch_record_io`, shared by the
existing multifile root converter and native pack preparation. The old
file-specific body codec loop is removed. Input stays a borrowed immutable span;
the decoder feeds the hash and output codec directly into final private canonical
storage. There is no whole-body buffer, spool, or second encoded-body copy.
Encoded CRC, plaintext byte count and SHA-256 etag must agree before the output
writer clears PENDING. Versions, lifetime, update time, retention, side metadata,
query visibility and both index sequences survive. Descriptor binding follows
the original body context and the current logical binding, including linked
objects. A resolved META/LINK body becomes a physical PUT/OBJECT_PUT; reference
selection remains the logical owner's responsibility.

The shared writer reserves the explicit query-sequence extension before emitting
the body. It also preserves a zero lifetime on existing collection-owned
snapshots; ordinary namespace append still requires a positive lifetime. Failure
discards private output and leaves the caller's result unchanged. The multifile
converter therefore may leave an empty private payload file after a short write;
its recovery test now accepts either empty or partial private output and still
requires successful resume and complete data verification.

Native `ps_namespace_body_begin_bound` admits the entire conservative encoded
record, stream, catalog and checkpoint claim before reading the source. It then
funds actual physical grants from that claim as bytes arrive. Commit permits a
shorter nonempty output; close releases unused admission. Exact-size body
construction retains its existing measured-size contract. A peer can claim the
remaining free capacity after bounded admission without preventing the admitted
writer from completing. Rejection of an unfunded conversion happens before any
source read and leaves no converted result.

`ps_namespace_body_recode` prepares and durably binds one sealed final body pack.
It preserves the root's encoding policy and does not select a key reference.
Callers must exclude namespace mutation and reclamation throughout preparation
and handoff. Interrupted handoff leaves an unreferenced pack eligible for normal
maintenance. This is an internal conversion stage: native root policy changes
remain unsupported until physical reference replacement, collection-unit
relocation, derived-index invalidation and resumable root orchestration are
implemented and proven together.

Qualification covers 48 native round trips across empty, one-byte and 65,537-byte
bodies, both compression directions, plaintext/encrypted descriptors, ordinary
and linked bodies, and both durability policies. All 446 injected allocation,
read/write/sync and torn-publication cuts pass crash/reopen and retry checks.
CRC and etag mismatches reject output, including calls without an error object.
The 54 multifile compression, durability and recovery cases pass after the
short-write assertion adjustment. Affected native canonical writes, staging,
snapshots, capacity, publication and concurrency checks pass as well. Two older
namespace assertions were repaired after reproducing their failures with the
previous binary: corrupt namespaces now fail during eager root admission, and
body-builder cleanup must restore the initial checkpoint claims rather than
assert that the entire root has no claims.

Generated 2 MiB inputs exercise complete streaming and reopen in a fixed 10 MiB
container, with no growth. The measured maximum single Pouch allocator request
is 65,536 bytes for plaintext or 131,400 bytes for encryption's fixed decryptor;
these are not peak aggregate-memory measurements. Physical allocation follows
actual output, not the conservative admission bound:

| Generated 2 MiB input | Encoded body bytes | New physical bytes |
| --- | ---: | ---: |
| Repeating, plaintext | 5,126 | 8,192 |
| Repeating, encrypted | 5,198 | 8,192 |
| Incompressible, plaintext | 2,097,798 | 2,113,536 |
| Incompressible, encrypted | 2,098,614 | 2,113,536 |

GNU and musl release checks pass, including the expanded streaming cases and
partial-write resume. Three TSan checks pass with instrumented OpenSSL and no
suppressions. Strict Memcheck passes both native conversion modes with zero
remaining allocations, zero errors and only three inherited file descriptors.
Debug and TSan use 128 KiB workers; musl also exercises its native worker-stack
contract. Source-manifest and stack-allocation policy checks pass.

Three alternating matched indexed-workload pairs compare the preceding binary
with this implementation: 16 rows, 1 KiB bodies, two writers and two rounds, with
reopen enabled. Native work CPU is essentially unchanged; this is a normal-write
regression check, not full native root-conversion performance qualification.
Median milliseconds are separated below; multifile columns show current values.

| Phase | Native NoSync, before → after | Native durable, before → after | Multifile NoSync / durable |
| --- | ---: | ---: | ---: |
| Work including readiness | 50.11 → 53.33 | 460.92 → 403.05 | 17.62 / 358.06 |
| Work CPU | 13.04 → 13.09 | 36.23 → 35.82 | 14.21 / 33.20 |
| Maximum index flush | 18.27 → 18.97 | 13.03 → 12.56 | 1.07 / 1.05 |
| Close | 0.27 → 0.31 | 0.35 → 0.34 | 35.18 / 30.36 |
| Reopen plus verification | 3.42 → 3.04 | 3.27 → 3.30 | 0.92 / 1.05 |

The native index still performs four forced readiness syncs under both policies;
the multifile index-flush phase performs zero. That latency gap, full native
codec conversion, broader bounded-maintenance qualification and shared-root
coordination/recovery remain required work. Multifile stays the default and
measured reference; no format marker or public ABI changes here, and no
production cutover is authorized by this milestone.

Evidence is in ignored `build/pouch-space-record-recode/`: `native-recode-all.log`,
`native-empty.log`, `repaired-expectations.log`, `qualified-debug*`,
`qualified-*-release*`, `partial-write-*`, `tsan-*`, `memcheck-*`,
`paired-performance.log` and `performance-summary.tsv`, with the exact build,
relink and qualification commands. `baseline-body-tests.log` records the two
pre-existing namespace assertion failures; the initial failing conversion logs
are retained separately from the repaired proof.

### Durable selection of converted native namespace bodies

`ps_namespace_body_select` now makes a verified converted pack authoritative for
an existing namespace key. It compares the converted pack's sealed record with
the supplied result, then compares the complete selected canonical metadata with
the expected source. Only the descriptor, encoded length and body span may
change. Canonical normalization permits a physical PUT/OBJECT_PUT to replace a
resolved LINK/META without changing logical identity. Changed versions,
lifetimes, either index sequence, timestamps, retention, query visibility,
metadata, content types or keys are rejected. The in-memory key summary retains
its original object/state classification, so later lease metadata updates keep
their established query and object semantics.

Selection reuses the native metadata publisher. The replacement key value and
checkpoint coverage are members of one physical revision, forced durable even
under NoSync. Physical checkpoint epochs advance; logical versions, watermarks
and canonical suffix coverage do not. Source ownership and interval indexes move
only after publication succeeds. Fault recovery must select the complete old or
new key/coverage pair. A stale expected source is rejected rather than silently
rewiring a newer value; a coordinator must inspect the selected record after an
uncertain outcome before retrying.

This operation is offline. An attached logical checkpoint observer rejects it,
preventing stale logical-client record references. Body production remains the
separate streaming preparation stage. Selection reads canonical metadata without
another full encoded-body integrity pass; preparation already verified CRC,
plaintext length and etag. Namespace mutation exclusion spans the handoff, with
no root publication lock or namespace mutex held over those metadata reads.
The blocked-I/O test proves that a sibling namespace can commit and readers can
still capture the old value while selection is paused; competing mutation in the
same namespace receives BUSY.

Existing metadata views remain valid across replacement and defer reclamation.
After views close, catalog removal can retire an unreferenced old pack while an
already captured body reader retains its bytes. Closing the last reader permits
reaping; a subsequent checkpoint makes released space reusable. Root compression
policy is unchanged throughout both
directions of conversion.

The dedicated suite covers all four codec modes, both durability policies,
ordinary and linked objects, stale sources, foreign namespaces, observer
exclusion and eleven changes to logical fields. All 416 allocation, I/O and
torn-publication cuts pass, including lost-write reopen, complete old/new
key-and-coverage correlation and retry. Allocation cuts also exercise omitted
error objects. A subsequent ordinary metadata mutation verifies object
classification before reopening. A 2 MiB incompressible body with 32,769 bytes
of metadata selects and reopens within the fixed 10 MiB container in each
encryption/durability combination. Its largest single Pouch allocator request
during selection is 33,276 bytes; this is not a peak aggregate-memory measure.

A second suite starts with values written by the production Pouch engine,
including state, internal objects, an empty body, a tombstone and a metadata-only
placeholder. It performs 64 body handoffs across both compression directions,
encryption modes and durability policies, reopening the production engine after
each direction. Body bytes, versions, metadata, object classification, state
watermarks and query freshness survive. Later object metadata updates preserve
the body version and public query sequence. Each fixture still contains only
the native container file.

The affected Debug batch passes 38 cases covering native metadata/checkpoints,
canonical snapshots, staging, maintenance, publication, body budgets and
concurrency. Four additional lease lifecycle, protection, coordinated transaction
and attachment-window checks pass. GNU and musl release each pass seven selected
checks, including the complete new fault suite. Three TSan checks pass with
instrumented OpenSSL and no suppressions. Strict Memcheck passes both namespace
and production-engine selection suites with zero remaining allocations, zero
errors and only three inherited file descriptors. Formatting, source-manifest
and stack-allocation checks pass.

Preparation and selection still have separate capacity claims. The integrated
root converter must fund both stages before production, support bounded progress
and reclaim obsolete sources between batches. Collection-unit conversion,
derived-index invalidation, a durable conversion intent and resumable root policy
cutover remain required. Explicit root compression changes therefore continue to
return unsupported. Full conversion performance is not qualified by these
metadata handoff tests; the previously measured readiness latency gap also
remains. Multifile remains the default/reference and deployment remains a
separate decision. This milestone changes no format marker or public ABI.

Evidence is in ignored `build/pouch-space-body-selection/`:
`qualified-selection.log`, `qualified-debug.log`, `client-selection.log`,
`qualified-leases.log`, `qualified-*-release*`, `qualified-tsan.log`, `tsan-*`,
`qualified-memcheck.log` and `memcheck-*`, with exact native build, instrumented
relink and memory-check commands.

### Compound admission for native body conversion

Native conversion now has one operation owner:
`ps_namespace_recode_begin/run/close`. Begin validates the selected source,
captures its canonical metadata and binding, and admits the complete body pack,
replacement key value and checkpoint coverage before body input. The target
crypto provider is borrowed until close; the caller can release the original
entry after begin. Run is single-use and returns an owned result only after the
new reference is durably authoritative. Close releases namespace mutation
exclusion and unused credits. A sealed unselected pack after an interrupted
handoff remains normal maintenance work.

The plan separates conservative canonical body and key-metadata bounds. Both
include a complete explicit query trailer, including when the input used the
implicit live-record form. The body still allocates physical grants only as the
codec emits bytes. The compound ordinary claim preserves the maintenance floor;
before creating its body writer, the owner transfers the metadata share to the
checkpoint completion budget without another admission. Publication consumes
that funded share instead of asking for space after the body has been read.
Budget planning and the existing handoff primitive use the same replacement
owner and publisher; logical mutation paths retain their existing semantics.

The owner holds namespace mutation/reclamation exclusion through preparation and
publication, with no mutex over producer I/O. A paused-input test proves sibling
namespace commits and reads of the old value continue, while mutation in the
same namespace receives BUSY. The peer-capacity test admits conversion, lets a
maintenance peer claim the remaining available space, and then completes both
body construction and durable reference replacement. Cancellation before input
returns the original claims, and the caller's result remains unchanged after
failure. Invalid result arguments do not consume the operation; a completed run
cannot be repeated.

All 784 integrated allocation, I/O and torn-publication cuts pass, including
omitted error objects, lost-write reopen, old/new key-and-coverage correlation
and retry. Their source fixture explicitly stabilizes NoSync metadata before
injecting conversion faults. Without that barrier, the first read failure can
legitimately lose the preceding unsynced seed at simulated power loss; that was
a fixture precondition error, not a conversion rollback guarantee. The reused
handoff suite also passes its expanded 502 fault sites after the admission
refactor.

Generated 2 MiB incompressible bodies with 32,769-byte metadata complete under
the peer claim in a fixed 10 MiB container, in each encryption/durability
combination. The maximum single Pouch allocator request during admission is
33,276 bytes; during production it is 65,536 bytes for plaintext or 131,400 bytes
for the fixed decryptor. These are not aggregate peak-memory measurements.
The production-engine suite now routes all 64 body handoffs through the compound
operation and still verifies state, objects, empty bodies, tombstones,
placeholders, metadata updates, versions and query freshness after reopen. It
continues to leave only the native container file.

The affected Debug batch passes 37 checks covering both conversion primitives,
native metadata, snapshots, staging, checkpoint faults/ranges, body and append
budgets, budget transfer and four multifile conversion cases. GNU and musl
release each pass nine selected checks, including the complete integrated fault
suite. Four TSan checks pass with instrumented OpenSSL and no suppressions.
Strict Memcheck passes compound conversion, the handoff/large-body suite and the
production-engine suite with zero remaining allocations, zero errors and only
three inherited file descriptors. Debug/TSan use 128 KiB workers; musl exercises
its native stack contract. Source-manifest, stack-allocation and formatting
checks pass.

This closes compound admission for one native namespace body, not the full root
codec feature. The coordinator still needs bounded key traversal that releases
historical view ownership between batches, allowing obsolete sources to be
reclaimed during conversion. Collection-unit conversion, derived-index
invalidation, durable conversion intent and resumable root policy cutover remain
required. Explicit root compression changes remain unsupported. Full codec
performance qualification and the existing indexed-readiness latency gap also
remain open. Multifile stays the default/reference; this adds no format-marker
or public ABI change and authorizes no deployment.

Evidence is in ignored `build/pouch-space-compound-recode/`:
`compound-recovery.log`, `client.log`, `qualified-debug*`,
`qualified-*-release*`, `qualified-tsan.log`, `tsan-*`,
`qualified-memcheck.log` and `memcheck-*`, with exact build, instrumented relink
and memory-check commands. The initial fixture failure and its isolated fault
location are retained separately in `compound-faults.log` and `fault-abort.log`.

### Bounded namespace conversion with reclamation

Native namespace conversion now owns the complete bounded pass through
`ps_namespace_conversion_open/step/close`. A step limits both key visits and
source/piece maintenance. Initial cleanup recovers source workspace, each
successful body handoff schedules cleanup of its old source, and final cleanup
finishes the remaining physical work. The namespace owner prevents overlapping
conversion owners and independent raw body handoffs. The target provider is
borrowed until close. Root policy and collection conversion remain separate
coordinator responsibilities.

Traversal uses the checkpoint owner's stable key slots directly. Opening a
cursor takes constant workspace; each next call pins and decodes one metadata
value, releasing that reader before conversion. It retains no historical
namespace-wide value/body view. A volatile logical epoch invalidates traversal
after ordinary canonical publication or metadata mutation, including a mutation
during an unlocked metadata read. Physical body replacement, piece repacking and
coverage flush preserve traversal. Failed next calls leave the caller's owned
metadata and cursor position unchanged. Epoch saturation rejects traversal until
the owner is reopened, without wrapping. This adds no persistent-format field.

Source scans use the existing ordered resource index and a scalar kind/id token;
removing the last visited source does not lose the next position. Cleanup
returns a waiting result when retained views defer reclamation. It holds no
mutex over producer input. Sibling namespace commits continue during conversion
and during a paused metadata read. Logical publication between steps invalidates
the pass; restarting observes the newly committed state.

Each key retains the funded body-plus-metadata handoff from the preceding
milestone. Conversion derives its binding from the real namespace, key, version
and body class. Already matching representations verify encrypted binding
identity and skip rewriting. A descriptor bound to another key is rejected even
when its codec flags match. Closing and reopening a pass resumes from durable
selected references. Failed candidate packs become cleanup work; recovery after
an uncertain I/O outcome still requires reopening the root and running root-wide
orphan reclamation before resuming. Completed retired physical space is
checkpointed on an admission retry when needed for capacity progress.

The capacity fixture converts three distinct incompressible 2 MiB values inside
a fixed 10 MiB container, closing/reopening after the first conversion. Across
all four codec combinations and both durability policies, it converts exactly
three bodies, verifies every decoded byte and logical metadata, and finishes
with 6,340,608--6,447,104 allocated bytes. Each step admits one key and at most
three maintenance units. The largest measured allocation request is 131,400
bytes. A completed authenticated repeat pass reports three matches, zero
conversions, zero additional container allocation and zero additional syncs.
These results establish bounded progress without holding a duplicate of the
whole namespace; the source and replacement for an individual body still need
temporary space together.

The production Pouch fixture now uses this pass for its 64 state/object handoffs
across codec/durability combinations and both conversion directions. Its
metadata, empty-body, tombstone, placeholder and later metadata-update checks
continue to pass, including a second matching pass. The full namespace failure
matrix recovers across 2,630 allocation, I/O and torn-write sites, with acknowledged
results surviving simulated power loss under both durability policies. Ownership
and mutation tests cover deferred views, sibling progress, competing owners,
failed cursor reads, publication during a paused metadata read and binding
verification on the matching path.

Qualification passes 33 selected Debug checks, including maintenance and
checkpoint fault/concurrency regressions, source-manifest and stack-allocation
checks. GNU and musl release each pass nine selected checks, including the full
new failure matrix. Four TSan scenarios pass with instrumented OpenSSL and no
suppressions. Three strict Memcheck scenarios pass with zero remaining
allocations, zero errors and only three inherited file descriptors. Product
sources compile under the existing strict C89 policy and formatting checks pass.

Durable root conversion intent, collection-unit conversion, derived-index
invalidation and resumable root policy publication remain required. Explicit
root compression changes still return unsupported. Full conversion performance,
shared-root qualification and the indexed-readiness latency gap remain open.
Multifile remains the default/reference and production cutover still requires an
explicit deployment decision. This milestone changes no format marker or public
ABI.

Evidence is in ignored `build/pouch-space-namespace-conversion/`: `pressure.log`,
`faults.log`, `guards.log`, `qualified-debug*`, `qualified-*-release*`,
`qualified-native.log`, `qualified-tsan.log`, `tsan-*`,
`qualified-memcheck.log` and `memcheck-*`, together with exact build, instrumented
relink and memory-check commands.

### Root and namespace ownership

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
indexed queries use the owners described above too. Exclusive external XA journal and
coordinator behavior now use the common engine described above. Normal
`lc_pouch_open` uses
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

Remaining integration includes resumable codec conversion, protected maintenance
admission and shared-root coordination/recovery. Core collection positions
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
