# Native Pouch container foundation

This is an experimental container foundation, not a complete Pouch backend or a
replacement public API. Its immutable-value fixtures use STATE keys to replace
immutable values; COLLECTION keys
reject overwrite of a live binding. Deleted keys can be rebound: permanent
position allocation, tombstones and retry authority belong to a future adapter.
Neither implements public leases, JSON validation, codecs, indexes, XA or
receipts. Comparisons must identify these differences rather than claim feature
parity. The comparison adapter separately applies the existing collection JSON
validator before publishing collection-shaped values.
The canonical stream variants now use production collection/state metadata,
SHA-256 and bounded codecs, separately from those immutable-value fixtures.
Ordinary segmented and container replay share one bounded reader; borrowed
stdio preserves native readahead. State/object PUT byte output accepts borrowed
record I/O and caller-owned versions, sequences and timestamps. It finalizes
metadata before clearing PENDING and never commits or syncs its owner's stream.
The caller retains namespace policy and append/publication authority. No payload
spool is introduced. Normal public Pouch still opens multifile roots.

The private `ps_pouch_state_open` harness now runs ordinary production state PUT,
point read, metadata read, copy and namespace creation through typed storage
capabilities. It reuses the real projection and mutation policy, with no fake
filesystem paths or container body cache. Read sources retain their original
body/crypto binding across replacement, rotation and Pouch close. Indexing,
background maintenance and watches must be explicitly disabled; other APIs are
outside this harness's current call contract. The `runtime-state` comparison
mode uses these production calls for both writes and reads, separately from the
older codec-only stream fixtures. Full public backend integration remains ahead.

One exclusive Linux `flock` protects the container lifetime. Independent threads
reserve private extents under a short mutex; producer callbacks run outside it.
A second open, including another process, fails. After the exclusive owner
exits, another process can recover and open normally. Shared-root coordination
and process-shared reader pins are not implemented. Fork before open or exec
before using the library in a child. Stop concurrent calls and close all readers
and writers before closing their space.

The initial capacity defaults to 10 MiB with real Linux `fallocate`. Growth is
explicit in 1 MiB multiples; fixed-capacity failure is the default. No sparse or
zero-filling fallback exists. Two 4 KiB root pages, a protected 128 KiB metadata
allocation region and a 128 KiB publication journal occupy 264 KiB. The typed
copy-on-write directory and packed descriptor pages start in that metadata
region and can allocate further pages from the common pool. The global 64 KiB
checkpoint ceiling is removed. An immutable prototype value's publication
descriptor still fits the 128 KiB journal. Canonical streams instead publish
one fixed-size descriptor per owned run, without a growing single manifest.
No 250 MiB or 100 GB test is claimed.

Foreground admission preserves 16 KiB of payload capacity for maintenance and
a separate pool of free pages that only metadata checkpoints can allocate.
Let F be the conservative complete directory/packed-descriptor bound in bytes,
and A the bytes of allocated metadata pages. Admission protects at least
`max(F, 2F - A)` free metadata bytes, including the original fixed region.
The bound follows logical metadata and the directory's minimum occupancy,
independently of reserved container capacity. Unused dynamic pages return to
the common pool; the original 128 KiB metadata region stays protected.

With spare common capacity, checkpoints copy changed descriptors and tree paths.
Near capacity they can instead rewrite all current metadata densely using the
protected pool. Old-root retirement restores space for another complete
checkpoint, including when ordinary payload admission has stopped. No payload
is copied by this fallback. Capacity failures preserve prior publications.
Explicit namespace rotation now admits an operation budget before staging its
successor. Complete canonical append/delete/compaction admission remains required.

The private `ps_budget` boundary reserves unspent physical payload pages,
positive descriptor growth and additional live entries. Ordinary admission and
checkpoint top-ups exclude claimed payload capacity. The checkpoint bound
includes current live metadata plus every outstanding descriptor/entry credit;
private writers retain those credits through payload I/O until publication.
Payload credits are spent on physical allocation, metadata credits on publication.
The maintenance floor governs opening a claim, not spending already funded work.
Claims are volatile under the existing exclusive root owner; claim/release
performs no I/O, introduces no persisted representation and copies no payload.
Writers pin their budget; space close rejects outstanding claims. Release returns
unspent credits and excess dynamic metadata workspace using a preallocated split
node. A budget provides capacity admission, not key isolation, rollback, or
protection from allocation/I/O errors. Failed attempts do not refund spent credits.

Typed planning helpers now compose costs for new stream heads, name bindings,
routing replacement/creation and an exact unowned canonical span. Successful
composition uses scalar arithmetic; overflow leaves the plan unchanged. A span
uses a conservative one-page-grant bound, including every 64-byte run header,
without allocating an array per future page.

Canonical grant allocation and explicit stream/record reservation now accept a
budget. Grant preparation pins that handle, spends physical grant pages when
taken, and retains its descriptor/entry credits through private I/O and peer
checkpoints until grant installation. Extra zero preparation retains the existing
optimization but may borrow only unclaimed common headroom, preserving the
maintenance floor. Prepared spare pages become reusable only after their sync.
The reservation call funds grants, not later automatic growth; cached streams
retain no budget. Geometric over-reservation needs an additional allowance;
the exact span planner is intended for `ps_stream_reserve_exact`.

A known-size body-pack builder now takes an ordinary budget for its head, exact
span grants and final catalog binding before stream creation. It holds that
budget through the producer and publication, then releases it on close. The
ordinary claim preserves the standing maintenance floor. This completes body-pack
admission, not admission for the parent compaction sequence.

Private payloads flow through a 16 KiB buffer into page-aligned runs. Allocation
uses 4 KiB physical pages, matching the measurement host's filesystem blocks;
its device reports 512-byte sectors. Whole-page portions retain whole runs;
subpage tails own contiguous 64-byte cells within shared physical pages. Each
extent encodes its ownership kind and CRC. Writes cover only the caller's actual
byte range; no page image is read and rewritten to append a neighbour. Payload
I/O remains outside the allocator mutex, including independent writers sharing
one physical page.

Each participating packed page has a 64-bit occupancy mask. Free-span bins
classify pages by their largest contiguous free span: reservation probes at most
64 bins and 64 cells, rather than scanning every partially occupied page.
Recovery derives masks from every accepted checkpoint and journal body,
including superseded entries; it rejects overlapping cell/whole-page ownership.
There is no separately persisted payload bitmap. Retirement follows the same
checkpoint and reader-pin rules as whole runs. A page returns to the common
pool only after its last cell owner is released. Mixed whole-run/cell cleanup
prepares fallible allocations before returning any ownership; failure during
private abort poisons the handle until reopen. `allocated_bytes` counts physical
payload reservations once per page; attributed retired/private bytes and
`packed_bytes` count rounded cells, excluding unused shared-page slack.
Allocation uses sorted free intervals, split/merged on demand, and metadata
grows with actual extents and participation, not reserved capacity. Each run
records its offset, byte length and CRC32. References bind a random container
identity and a monotonically allocated incarnation; keys bind type, owner and
numeric key. Each open durably reserves a range of 2^32 incarnation numbers
before admission, so a
returned NoSync reference cannot alias a later value even if its publication is
lost. Reserving ranges amortizes durability without a per-writer sync or entropy
syscall. Unused numbers are abandoned on restart; exhaustion fails explicitly.
The 80-byte root header checkpoints the incarnation limit alongside capacity,
sequence, directory offset/generation and container identity, with an independent
checksum. Further grants use 64-byte journal events. Payload reads
use a pinned extent vector and a bounded verification buffer, without consulting
the directory on every chunk. Hash buckets resolve keys. CRC32 detects accidental
damage and torn writes; this is not authenticated storage.

Publication appends a checksummed metadata event only after payload writes
complete. The directory mutex serializes this short append/install step. A
caller then leads synchronization of the ready sequence prefix, or waits for
the active leader. The directory and commit coordination mutexes are released
throughout the file sync: other producers, allocations, publications and read
admission can progress. Arrivals during a barrier form the next group, without
an artificial collection delay or a background worker. One writer still issues
one file sync per successful commit.

Durable success requires a completed file sync covering that caller's sequence.
Readers select the completed durable prefix: pending replacements and deletes
leave the prior committed value visible. In-memory version links and temporary
delete markers retain that view until checkpoint; they add no payload copies
and do not change the disk format. NoSync mode exposes complete publications
without waiting for a commit barrier. Read handles retain immutable pins.
Checkpoint holds the directory mutex, shares a separate sync I/O mutex with
commit barriers, and advances durability only after its structural barriers.
Commit completion never acquires the directory mutex, avoiding a lock cycle.
Failed publication I/O poisons the handle: no further operations can reuse space
or overwrite an uncertain journal suffix. Reopen reconciles the outcome.
Status counters describe in-memory accounting; reopen to reconcile durable state
after an I/O error.

Recovery selects the highest valid root, validates its bound directory and
packed descriptors, then replays the contiguous checksummed journal suffix.
Journal bodies are verified
with bounded buffers, allowing torn or reordered unsynced tails to be discarded.
Checkpoint bodies are verified when read; open does not scan them or unused
capacity. A valid newest root with a damaged checkpoint is an error, not grounds
to fall back into possibly reused extents. An invalid/torn root can fall back
only while the older root is still valid. Recovery stabilizes the selected root
and accepted journal, invalidates its sibling, and clears the rejected journal
suffix durably before admitting allocation. Arbitrary corruption of previously
durable journal bytes is outside the torn-tail recovery guarantee.

Reopening retains ownership of every accepted checkpoint and journal body,
including superseded values. The next replay still verifies older PUT bodies
before reaching later replacements or deletions. Only a durable checkpoint
retires that dependency and permits reuse; an opening sync alone cannot do so.

Retired payload runs remain unavailable until a checkpoint has synchronized
payloads and metadata, published the alternate root durably, and invalidated the
previous root durably. Only then can unpinned runs be reused. Active readers pin
old entries even after that barrier. This makes NoSync deletion/replacement safe:
the old durable publication cannot recover after its runs become writable.
Private abandoned reservations need no durable ownership record: recovery finds
only published extents, so their complement is free after its opening barrier.

Descriptors share immutable 4 KiB pages with checksummed identity, location and
generation headers. A descriptor may span pages; its checked span determines
ownership. An unused continuation pointer does not retain a page after the
spanning record is retired. Pages are reclaimed after their last current record
and old recoverable root are retired. Reader extent vectors retain payload pins
independently of descriptor-page lifetime. Explicit `ps_metadata_repack` promotes
all current live descriptors into the existing dirty set and rewrites them
densely through ordinary checkpoint/root retirement. Dirty replacements and
tombstones retain their existing ownership transfer; unpublished writers stay
private. Payloads, stream mappings, binding identities and held readers do not
change. Tests reduce 185 fragmented metadata pages to 10 at the same 10 MiB
capacity. Ordinary checkpoints keep their incremental path away from capacity;
the protected-pool fallback provides progress near capacity. A clean checkpoint
allocates no new metadata pages and trims excess workspace left by an aborted
private writer. Healthy repack failure retains its dirty plan for a later
checkpoint or repack retry.

Growth reserves backing space first, then privately admits the new range under
the allocator mutex so it can fund the checkpoint that publishes capacity and
metadata together. Healthy failure aborts private metadata pages, returns free
growth intervals from both allocation pools and trims excess old-capacity
workspace before excluding the new range. Rollback split nodes are allocated
before growth, so restoring old capacity and payload headroom needs no new
allocation. Uncommitted trailing space is ignored on open. Any failed structural
I/O poisons the handle;
reopen reconciles the selected root. Allocation failure before root publication
permits retry on the same owner. Journal exhaustion uses the same checkpoint
policy; no live payload is copied. Checkpoint/growth serialize publication and
allocation, but never wait on a producer callback or
on readers to finish.

## Canonical append-owned streams

`stream.[ch]` adds one cached owner per typed logical stream. A stream head is an
80-byte metadata record with stable incarnation, checkpoint tail, capacity and
grant count. Each grant has an 88-byte descriptor and owns whole physical pages.
Its immutable 64-byte header binds container identity, head and grant
incarnations, logical base, physical offset and size. Its descriptor checksum
covers that header; the canonical record owner checks its mutable record bytes.
The head/grant chain is validated on recovery, including orphan and capacity
mismatches. Normal immutable-value APIs reject these types.

Grants normally grow from 4 KiB through 16, 64 and 256 KiB to 1 MiB, then remain
at 1 MiB; fragmentation or protected-capacity admission can reduce a grant. A
grant's header is hidden from logical offsets. The heap run vector follows
actual grants and supports binary lookup; readers capture immutable mappings
under a short stream mutex. The 16 KiB producer buffer flushes directly into
owned ranges, with no document materialization or spool. Private-range patching
supports canonical metadata and PENDING-header finalization, including records
crossing run boundaries. A second appender on the same owner gets a busy error;
other streams and readers remain independent.

Only creation and run grants add structural journal events during normal active
stream append. A complete canonical
append advances an in-memory readiness sequence and tail hint; durable commit
waits for the root barrier covering that sequence before exposing its new tail
through the stream owner. Ready sequences are separate from structural journal
LSNs, including mixed immutable/stream groups. They use the existing caller-led
coordinator without a timer or background writer. A lone steady-state durable
appender still syncs each commit. Creating a head and preparing uncertified
space can require additional barriers. Read handles snapshot committed ranges
and retain the owner across later appends and checkpoints.

Fresh creation certifies initially zero free pages. Reopen, retirement and growth
do not: growth can encounter an existing ignored file tail. Before publishing a
grant from uncertified space, reserve a private batch of at most 1 MiB, initialize
it, write the first grant header and wait for preparation durability. Only after
that barrier may spare zeroed pages return to the common free list. Concurrent
preparations may share a commit barrier; no other allocator may use an in-flight
spare range. The zero certificate exists only in memory and disappears on reopen.
Admission preserves full checkpoint/maintenance allowance even while the whole
batch is private. Unsupported allocation and I/O failures follow the normal
cleanup or poisoned-handle rules.

Native initialization uses Linux `FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE`:
the range reads as zero while remaining allocated and retaining file size
([Linux fallocate contract](https://www.man7.org/linux/man-pages/man2/fallocate.2.html)).
Missing/unsupported zero-range operations fall back to 16 KiB offset writes;
initial creation and growth still require real preallocation. Tests cover both
paths, interrupted zero-range calls, backing reservation, partial failures and
durable zero provenance. This is initialization of private/free space, not a
hole-punching storage policy.

The canonical owner's recovery callback validates records from the trusted
checkpoint tail, using bounded read-ahead and its existing record checksums.
Only this callback may inspect candidate bytes beyond the published tail. The
stream then clears the entire rejected suffix and synchronizes before write
admission, preventing later valid-looking debris from resurrecting. An abandoned
written append is likewise cleared and synced before another appender; its
existing buffer supplies the bounded fallback. Failed cleanup poisons the space.
Opening a checkpointed stream does not scan its acknowledged bodies.

## Stream lifecycle

An active stream may be sealed once its appender is idle. The 80-byte seal event
binds its existing incarnation, final canonical tail and complete grant shape.
The head's reserved word at offset 56 carries the sealed bit instead of an extent
count; other bits are rejected. A checkpoint preserves the same sealed head.
Sealing is irreversible and idempotent. New appenders are rejected, while readers
may open immutable ranges through the final tail.

The last canonical commit records its ready sequence in memory. If that sequence
is not already covered by completed root durability, sealing first synchronizes
canonical data outside the allocator mutex. Only then may it append its seal
marker and wait for the shared readiness barrier. This prerequisite matters
even for an unacknowledged seal: a valid marker must never recover ahead of its
body. A last-commit sequence already covered by completed root durability avoids
the extra data barrier.
Structural seal/retirement wait for durability in both sync modes. Journal
rollover may additionally require the ordinary checkpoint barriers.

A sealed open verifies its grant headers but neither calls active-tail recovery
nor clears/synchronizes unused capacity. The final tail is authoritative because
its dependencies precede its structural marker. Payload checksums and projection
reconstruction remain the canonical owner's responsibility through bounded reads
of that committed prefix. Sealing retains unused capacity until retirement;
there is no implicit truncation or relocation.

Retirement appends one 64-byte head-removal event, bound to its incarnation.
Validate the run chain before publication, then mark that head and all its grants
deleted without further allocation or per-run journal actions. Replay performs
the same complete transition. Descriptors, payloads and journal-verification
dependencies remain owned until a durable checkpoint retires their history.
Pins can retain them longer. Existing stream readers keep working; after
retirement succeeds, the owner rejects new readers and appenders. Close still
requires its readers to drain. Repeating retirement adds no journal event.

Rebinding a retired typed key requires its retirement sequence to be durable
and allocates a fresh incarnation. Existing readers of the old owner remain
independent. The new head inherits any prior checkpoint reference for replacement
accounting; refresh that lookup after automatic journal-rollover checkpointing,
which may have collected the old tombstone. A deterministic sanitizer regression
reproduces the stale-pointer failure before its correction. Another full-capacity
test retires over 100 grants using one 64-byte allocation/event and proves that
checkpointing restores capacity without copying payloads.

## Typed name catalogs

`catalog.[ch]` maps `(owner, surface, exact name bytes)` to a binding ID, revision
and live stream reference. Surfaces distinguish namespaces, collections,
segments, snapshots, projections, control metadata and sealed body packs.
These are internal typed bindings; their public Pouch semantics are not
implemented by this layer.
Names may be empty or binary, including NUL, and are not filesystem paths.
The name index grows at 75% occupancy and compares the full byte string after
hashing. Memory follows metadata participation rather than reserved file size.

Catalog records use directory kind 5, keyed by owner and stable binding ID.
They are inline metadata, with no payload extents: the 64-byte common event
header is followed by surface (32 bits), name length (32), target kind (32),
reserved zero (32), target owner (64), target key (64), target incarnation (64),
then the raw name. The existing record CRC covers the entire representation.
Its size is 104 plus name bytes, at most the 128 KiB journal size. Checkpoint
packing supports records spanning metadata pages. Normal immutable-value APIs
reject this kind, preventing bypass of catalog validation and target retention.

Create requires an absent name. Replacement and removal require the current
revision, including this container's identity. Replacement retains the binding
ID and allocates a new revision; removed/recreated names get fresh IDs. Explicit
owner allocation, binding IDs and revisions share the reserved incarnation
allocator without a per-ID write. Exhaustion fails explicitly. Unused IDs are
abandoned on reopen, so lost NoSync work cannot alias a later identity.

Targets must be live stream incarnations in the same container. Mutations
always wait for structural durability, even in NoSync stores. The same name
cannot have two pending mutations; independent names use the existing grouped
commit coordinator. Runtime reference counts prevent retiring a bound target.
Removing or replacing its last binding records the release sequence; retirement
also waits for that release to be durable. Recovery reconstructs these counts
from checkpoint bindings and journal deltas, validates targets after loading
checkpoint heads, and rejects a journal that retires a still-bound head.

Get and enumeration select one completed durable prefix in both sync modes.
Pending changes preserve the old visible binding. A view pins that metadata and
its target; close it after admitting the actual stream/read handles. Enumeration
pins a metadata snapshot and invokes callbacks outside the allocator mutex, so
callbacks may checkpoint or mutate bindings without deadlock. Both count and
capture passes use one saved durable watermark: commit completion intentionally
does not acquire the allocator mutex. Deterministic paused-allocation tests
reproduce a pre-fix vector overflow from an advancing watermark and prove stable
binding and orphan snapshots after its correction.

Orphan enumeration captures unbound, closed heads with no catalog views or
pending catalog release. It reads no payloads and performs no implicit GC.
Candidates require revalidation before retirement because another thread may
publish a binding after capture; stream retirement rechecks these guards.
Recovered staged streams are therefore discoverable without inventing a second
authoritative manifest or scanning document bodies.

## Production record I/O

`src/lc_pouch_record_io.[ch]` is a borrowed canonical byte boundary: exact
vectored reads, append, private patching, tail observation and discard. It has no
pathname, directory, rename, lock or allocation API. The owning namespace or
collection retains those typed responsibilities. Normal Pouch's canonical
prefix/finalization and vectored writes use the same implementation; collection
writers and bounded decoders accept this I/O directly. All existing callers now
use the new internal signature. Public headers and canonical wire bytes are
unchanged. The temporary native file adapter retains existing descriptor cursor
and cleanup semantics; it introduces no permanent backend option.

Single-buffer native operations use `pread`/`write`; multi-part operations retain
the bounded vector path. Dispatch occurs before entering its vector workspace.
This keeps the original scalar syscalls and avoids paying for the larger stack
frame on small metadata reads. Both paths retry EINTR and partial transfers,
preserve caller vectors and reject invalid ranges before I/O. Zero/error and
short-transfer tests exercise the actual libc symbols, including glibc's
64-bit aliases.

`record_io.[ch]` bridges a private append to that production boundary. It records
the private logical start/tail, permits patches only inside that range, and
discards the whole append on failure. Canonical finalization writes key/metadata
before the final non-PENDING header; it neither commits the container tail nor
adds synchronization. The caller commits through the existing readiness group
only after its canonical records finish. Failed discard uses the stream's
existing suffix clearing, synchronization and poisoned-handle behavior.

Input borrows an already admitted stream reader. Exact offset reads stay inside
its captured logical window, preserve its sequential cursor and require no new
allocation/admission. They retain the same stream/run pins across later append,
retirement and checkpoints. A distinct recovery input permits candidate-tail
reads only during the owner's recovery callback. No body cache, payload spool,
second WAL or filesystem emulation is introduced.

The comparison creates/reopens real catalog bindings. Collection-shaped streams
now use production JSON validation, SHA-256, transforms, full canonical metadata
and span finalization. The benchmark's record-codec context is memory-only; it
does not open a second Pouch root. Positions/generations are supplied by the
fixture, not by an integrated public collection owner. State-shaped streams
still use the real header codec with minimal metadata. Public retry/position
authority and process-shared ownership remain unimplemented, so neither fixture
establishes full public semantics or performance parity.

## Bounded canonical control values

`PS_CONTROL` is an immutable, replaceable value of at most 4 KiB. Its publication,
read visibility and deletion follow the completed durable prefix in both root
modes. It reuses packed payload cells, checksummed descriptors, journal replay,
pins and the caller-led readiness coordinator. It adds no writer queue or
per-payload-record synchronization. Capture its durability policy before releasing
the allocator mutex; a subsequent replacement/checkpoint can retire the entry.

`control.[ch]` stores collection heads under `(PS_CONTROL, collection owner, 1)`.
`src/lc_collection_head.[ch]` owns the production 384-byte codec, also called by
the existing native file head owner. Failed decode leaves output unchanged;
dropped is data for the owning admission policy. Callers retain metadata
authority and establish durability of referenced canonical dependencies first.
This adapter neither allocates public positions nor validates snapshot bindings.

Existing control replacements may consume one page of the 16 KiB maintenance
allowance across the root. New controls cannot consume it. Both cases retain the
conservative metadata shadow allowance. Checkpoint can spend that shadow without
charging the maintenance payload floor again, then releases superseded values
after root retirement and pins drain. A full-capacity test fills the initial head
page and foreground space, requires a fresh replacement page, rejects a new
control and proves repeated checkpoint/replacement and reopen without growth.

The component comparison uses actual native atomic head replacement and the
container adapter with the same codec and caller authority. Its complete head
reload phase bypasses the native owner's inode cache. Neither it nor the stream
fixture establishes integrated public collection ownership or query performance.

## Root policy and namespace ownership

`root.[ch]` owns one physical space, its production crypto provider and a dynamic
namespace registry. Its 128-byte control at `(PS_CONTROL, 0, 1)` contains a magic,
encryption/compression flags, a bounded nonsecret key ID, canonical head/record
widths, zero reserved fields and CRC32. It is always durable. Reopen validates
this policy before admitting namespaces; a bare or interrupted physical space
cannot silently adopt another policy. A wrong key/mode fails closed. Compression
inherits stored policy unless explicit. A different explicit setting requires
the still-pending container conversion, rather than changing interpretation.

Namespace creation is one self-contained catalog record. Its stable ID equals
its initial revision, identifying the logical owner and incarnation. Stream and
target fields are zero. This variant is valid only at the global namespace
surface, with zero target facts; it cannot be replaced by a stream binding.
Removal/recreation allocates a fresh ID. Catalog views pin the metadata itself;
ordinary stream bindings still pin targets and retain their retirement guards.
No empty stream, grant, payload page or second publication is needed for a
namespace identity. The initial measured empty-stream representation is retained
only as an archived benchmark executable, not an implementation path.

Root namespace names are nonempty C strings, retaining exact bytes including
slashes and non-UTF-8 bytes. The registry grows with visited names; failed misses
do not remain cached. Same-name initialization waits on that owner, while other
names can prepare and publish independently. Registry ownership never spans
storage I/O or user callbacks. Namespace pins and active calls protect root
lifetime; physical child handles also reject close. Callers stop new calls before
final close, and do not reuse physical/root handles after fork.

`ps_check` provides the atomic admission check for cached logical owners. Success
allocates nothing, locks nothing and performs no I/O. After uncertain physical
I/O, new namespace admission fails even when its identity is cached. Existing
pins and diagnostic status can still be released/inspected before reopen.

The comparison's stream modes now use this owner for root creation/reopen,
namespace identities and the production crypto provider. Child stream catalogs
use their real namespace scope; the memory-only record context borrows that
provider. Public collection IDs, positions, receipts, namespace mutation policy,
maintenance and process-shared ownership remain pending integration. Normal
public Pouch roots still use files and directories.

Ordinary stream runs now use a cached namespace segment owner. A small routing
head selects the active numeric segment; typed catalog entries own the segment
set. Initialization and rotation publish structural metadata, while normal
record commits only publish the canonical tail. Rotation prepares/binds a new
empty stream, reserves the known span for an automatic append, completes its
canonical bytes, then commits head cutover before exposing its tail. The bounded
`PS_ROUTING` head follows root durability and shares CONTROL's replacement
allowance. Durable publication covers the first record in that same barrier;
NoSync does not force a whole value to disk. The next mutation flushes the ready
routing prefix before durably sealing the predecessor. Explicit empty rotation
reserves no append span and remains durable under either policy. Expected-active
fences make interrupted rotations retryable across reopen. Unpublished empty
successors are reusable; complete private bytes with checkpoint tail zero are
discarded if their head was never selected. A published tail in an unselected
successor is rejected. Recovery rejects inconsistent membership. Logical
mutation authority still owns versions, index sequences and public policy.

Namespace views capture identities/tails using small stream-window pins, with
one operation-local readahead buffer and optional CRC scratch during replay.
They retain no per-segment body buffer. Buffered sequential readers share the
same pin admission/release code. Read windows and views retain their namespace;
root close releases cached segment owners only after those pins drain. Producer
callbacks and replay callbacks hold no namespace/registry mutex. Independent
namespaces progress during a blocked producer or initialization. Cached storage
admission checks poison before its atomic pointer lookup. The owner caches its
committed tail under append authority to avoid a duplicate metadata query per
record.

Namespace routing now also selects a sealed canonical snapshot and the logical
segment boundary it covers. Its private builder captures a fixed prefix and
releases mutation authority across producer calls. Selection durably binds the
sealed stream before replacing the head, under either root policy; newer log
segments remain replayable. A build ID and namespace ID jointly bind its stream.
Unpublished snapshot bindings are removed before a higher ID can be selected.
Replay visits the selected snapshot followed by its newer segment tail.

A sparse sorted source index keeps older bound sources addressable for body
references without making them part of replay. Explicit obsolete-source removal
requires the logical owner's liveness proof. Catalog removal precedes physical
retirement; captured windows remain readable, while catalog/read pins defer
closing and reuse. Closed unbound builders have explicit metadata-only orphan
reclamation, with incarnation and catalog ownership rechecked before retirement.
Interned labels retain namespace-owner lifetime even after source objects close.
The forced ordinary-state pass described below now supplies logical liveness
and reclamation. Automatic policy and complete maintenance admission remain
pending.

`ps_stream_reserve` acquires enough zero-owned grants from the append start for
a caller's known minimum. It changes neither the private record cursor nor the
published tail and does not buffer a body. The record adapter exposes the same
operation. Automatic namespace append reserves at least one logical byte before
selecting a successor. Pouch metadata/delete supply their complete canonical
batch size; ordinary streaming PUT supplies only its header/key prefix. Later
unknown producer bytes still acquire bounded grants as they arrive.

Allocation failure may retain empty grants for retry, but a failed reservation
does not cut over or seal the predecessor. Reopen accepts this staged empty
owner, and a 1 MiB growth can satisfy a later retry. Committing within the
reserved span needs no additional grant. Complete maintenance dependency
admission remains separate work; no per-root writer queue is introduced.

The owned commit boundary accepts an optional logical publisher outside the
allocator/commit mutexes. Its canonical tail stays private through publication;
durable roots require that publisher to cover the prepared ready sequence.
Ordinary within-segment commits retain their existing single-lock fast path.
Production state appends prepare a private prospective manifest and update the
committed cache only after physical success. Interned segment labels remain
stable until namespace-owner destruction, including across array growth and
append close. No public ABI or canonical record payload changes. The namespace
head's prototype typed key changes directly from CONTROL to ROUTING without
legacy parsing, migration or a format-version increment.

The private Pouch harness now also admits ordinary metadata updates and delete
tombstones through the production canonical batch writer and typed appender.
Logical preconditions, versions, sequences and body references remain owned by
the state engine. The existing metadata worker batches within a namespace;
independent namespaces retain their own append authority. Metadata-only changes
and tombstones do not rewrite body bytes.

Namespace control slot 2 holds a conservative retention work marker. Its 32-byte
payload contains `LCPSRETN`, the little-endian namespace owner at byte 8, a
present byte of 1 at byte 16, seven zero reserved bytes, and a little-endian
64-bit slot at byte 24 containing CRC32 of bytes 0 through 23 (upper half zero).
Ensure validates an existing marker and publishes a missing marker durably,
including under NoSync, before expiry-bearing canonical publication. A failed
mutation can leave a harmless positive marker. Get reads only that control;
it neither initializes segment ownership nor replays body records. Clearing
markers and executing retention maintenance remain pending.

The physical reserve is not yet connected to the complete logical maintenance
dependency set. A peer namespace can exhaust foreground capacity needed for
rotation before a delete, despite protected physical capacity. The fixed-size
capacity probe verifies preservation of the prior value across failure/reopen;
it also checks selected-head/tail preservation. It does not prove full-capacity
deletion. Guaranteed admission and reclamation
remain requirements of the full backend.

State read-many, metadata visits and paged scan summaries now use the same
production projection with typed encoded read pins. A metadata scan opens only
small captured windows, without body reads or per-row decoder buffers.
Read-many decodes one captured value at a time immediately before its visitor,
outside projection coordination. Scan body access clones a captured source
cursor, sharing an immutable reference-counted namespace window; it does not
re-resolve the current segment directory. Each cursor retains the root/crypto
owner and can outlive the scan and Pouch runtime. Window release drops its
physical read pin only after the last shared cursor closes.

Container reads ignore body-cache optimization hints and never populate that
cache. Scan body results copy their metadata so result cleanup cannot invalidate
the visitor's borrowed row; both backends support repeated body reads from the
same captured row. Read-many also returns its captured has-body and staged-delete
fields. All of these changes are internal memory ownership or read routing;
canonical bytes, prototype format and public ABI are unchanged. Full public
query-client ownership remains integration work; forced ordinary-state
maintenance publication is described below.

Canonical snapshot serialization now also uses borrowed record I/O. Native
compaction and clean-close projections delegate to the same state codec as
container snapshots. A resolved entry can retain its old body reference without
body I/O, or copy an encoded source with caller-owned scratch capped at 128 KiB.
Copying checks the original stored CRC before completing PENDING, retaining
crypto descriptors, immutable context, metadata, versions and both sequences.
Failure after output begins calls the owner's private discard boundary. The
codec owns no logical selection, history protection or physical publication.
Native clean-close object references now preserve their OBJECT_LINK flag.

Logical snapshot capture now reuses production projection semantics over a
captured canonical prefix. It retains only current key metadata, including
tombstones and placeholders; hash lookup storage is released after one key sort.
Immutable visits emit HIGH_WATER first and explicit per-key query sequences.
They allocate nothing and hold no engine coordination. The capture owns metadata
but borrows the allocator context; physical body pins stay with its caller.
Ordinary appends can commit during capture and during a held immutable visit.
Queued metadata-entry copying now also retains its query generation, preventing
canonical replay from losing a generation which the live cache preserved.
The private `ps_pouch_state_compact` pass now measures complete metadata output
and a selected source's canonical body frames before copying. Known spans are
reserved with page-rounded final grants. The forced victim score uses capacity
minus current live encoded body bytes. Copying runs outside logical namespace
authority with bounded heap scratch.

Bodies relocate into a separate sealed stream, bound under `PS_CATALOG_BODY`
(surface 6). Its global build ID and namespace ID jointly bind stream ownership.
`body-<20 decimal digits>.log` labels are typed metadata, never files. Packs do
not participate in replay. Metadata snapshots retain references to untouched
bodies, so later snapshots need not copy every live value. A selected snapshot
may be rewritten at its existing covered boundary without another empty log
rotation.

The pass binds/seals the pack before durable snapshot selection, then captures
the latest logical tail under publication authority before deciding source
liveness. It invalidates the resident cache before releasing that authority,
including on uncertain failure. Covered source retirement, reap, closed-unbound
orphan collection and checkpoint retain existing read-pin guarantees. Encoded
sources validate stored CRCs as consumed; reset and clone maintain independent
cursor/checksum state and can outlive the logical runtime.

Known canonical output spans and checkpoint workspace have separate protection.
Explicit empty rotation now claims successor head, name binding and routing
replacement before staging or head selection. Seal and rollover use protected
checkpoint workspace. Canonical grants can now spend explicit claims and known
body packs reserve their complete publication. Namespace appends now admit their
declared span, successor/binding and head selection before staging, retaining
the claim through producer completion and publication. Metadata batches declare an exact
span and preserve the standing floor; delete-only batches may use it. Unknown-size
PUTs declare a header/key prefix and grow through ordinary admission. Existing
owned capacity avoids a budget handle when no head selection is needed. Ordinary
grants retain geometric growth with exact fallback near capacity; maintenance
uses exact grants, and a small clipped grant preserves the larger growth
preference across reopen. Complete spans are bounded before vectored output and
checked before commit. Snapshot builders and parent compaction still need a
joint claim for their full dependency set. The fixed 16 KiB floor needs a
bounded-work policy covering admitted keys up to 1 MiB. Automatic
compaction/retention policy and history protection remain pending.
The current label pool still follows namespace-owner lifetime.

The protected metadata region cannot be consumed by payloads. Complete
maintenance admission, resumable root transform conversion,
multi-process coordination and full Pouch integration remain future work. The
foundation now implements forced ordinary-state relocation and makes no claim
that all draft acceptance criteria are already satisfied.

See [measurements and reproduction](../../docs/pouch-space-experiment.md) for
the verified scope, observed regressions and commands.
