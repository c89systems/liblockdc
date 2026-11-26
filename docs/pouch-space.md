# Pouch Space

## Scope

Pouch stores a root in one persistent file on Linux x86_64, AArch64, and ARM
hard-float with GNU libc and musl, and on Apple Silicon with macOS 15.0 or
later. New roots start at 10 MiB and grow in whole 1 MiB increments as needed.
The implementation retains the multifile reader for migration and fixtures.

An eligible multifile directory migrates on open to a container at the same
configured path. Migration requires exclusive ownership of the legacy root
and the key needed to read it. Stop applications with that root open before
starting an upgrade. The migrator streams into a verified, durable candidate
on the same filesystem, then atomically exchanges it with the legacy
directory. Legacy files are reclaimed only after publication is durable.
Reopening after interruption either rebuilds an unpublished candidate from
the legacy root or finishes reclamation for the published container.
Canonical records and retained history carry forward, including saved consumer
positions, pending transaction decisions, and collection retry receipts.
Migration requires a dedicated root directory. Move any key file or unrelated
files outside the root before opening it with the new library.

`disable_migration=true` makes a legacy directory return
`pouch_migration_disabled`. New and previously migrated roots can still open.
See [Pouch open settings](pouch-open-settings-api.md).

## Observable behavior

- Preserve lease ownership, expiry, fencing, and preconditions. Preserve
  transaction-local visibility, staged decisions, atomic commit, and retry
  receipts, including after recovery.
- Preserve queue ordering and acknowledgements, outbox and inbox idempotency,
  and XA decision and recovery semantics. Independent namespaces and
  transaction owners can prepare work concurrently.
- Preserve public key identity, collection positions, generations, and cursors.
  A captured read retains its selected bytes through replacement, rotation,
  and reclamation.
- Preserve indexed-query visibility and the existing explicit index completion
  boundaries. Rebuildable query artifacts have separate authority from
  canonical state. A derived-index failure cannot revoke a canonical commit.
- Stream bodies end to end through bounded buffers, including codecs and
  relocation. Any explicitly admitted disposable scratch has bounded capacity,
  recoverable ownership, and measured I/O.

The public storage semantics are defined in the
[Pouch storage specification](pouch-storage.md).

## Physical ownership and recovery

Checksummed root selectors and a bounded rolling journal choose the accepted
physical prefix. A copy-on-write directory binds typed identities to extents.
Each revision batch has one checksum and one starting sequence. Its members
retain typed ownership, extent checksums, and explicit lengths. The required
root layout bits identify these encodings before journal replay. Use a freshly
created experimental container with this layout. Namespace checkpoint controls
carry five variable-width integers in the journal or checkpoint metadata.
Controls that fit in a directory binding stay in the leaf. Larger controls use
the existing packed descriptor pages.
Small canonical key records are stored with their revision and checkpoint
descriptors. Larger records and transactions that need the journal space
retain separately allocated extents.
Acknowledged durable operations survive recovery. NoSync work without a
durability barrier can be lost. Recovery checks framing, ownership, and commit
boundaries before admitting complete canonical publication groups. Before
admitting a cold namespace for writes, recovery checks for complete records past
the selected stream tail and restores their outstanding capacity obligations.
After validating the recovered prefix, the first owner can reuse proof from
a previous durability barrier. A clean exclusive close can record that
proof for the exact journal and a bounded set of stream tails. Restoring a
covered tail reuses its durable sequence. Recovery syncs newly discovered
records outside the proof before exposing them. Shared reopen can reuse a
durable namespace checkpoint for a recovered tail within its coverage when
the piece footers are intact. Reconstructed footers keep the recovery sync
requirement, including on retry after an interrupted repair.
When a partial piece's footer fits after the completed bytes on the last
written page, the append can avoid rewriting the piece's first page. A later
append can replace that footer. Recovery accepts the last complete canonical
tail and rebuilds its footer if interrupted. Full pieces retain fixed footers.
Sealing stores the final footer in its fixed position so a sealed stream
normally reads only the piece headers. An interrupted seal can use the
tail-adjacent footer if the fixed copy was lost.
Owners that reopen using a proof acquire an incarnation range on first use.
On an exclusive root, the first namespace creation can share a barrier with
its new incarnation range. Recovery admits the catalog entry only when the
covering range is present. On NoSync roots with native zero-range support,
that barrier can also prepare a bounded interval for subsequent stream grants.
Fresh shared roots make their already durable free-page zeros available to
all owners in the first live cohort. Cold reopen clears that proof.
After a clean exclusive NoSync close, a checked journal can serve reads.
The first persistent mutation or explicit flush stabilizes that recovered view.
Collection retention can write tombstones in the checked unused suffix of an
existing grant and stabilize them with the recovered view at the final
maintenance barrier.
Replayed keys acquire a physical snapshot identity when a write or checkpoint
needs it. Exclusive collection recovery verifies unused suffix bytes and repairs
any that are nonzero. An unchanged suffix can reuse the existing tail proof.

Private preparation reserves payload and descriptor capacity before spending it.
Publication revalidates its logical authority. Producers run outside
allocator publication authority. Independent owners can share durability work.
Shared durability waiters use completion notifications. Quiet retries back
off to a 64 ms interval so a waiter detects an owner exit that released its
lock without sending a notification.
On exclusive durable roots, a canonical append taking over after a slow sync
can briefly wait for the previous writer's next append. One barrier covers
the completed prefix captured after that wait.
Protected maintenance capacity funds checkpoint and completion work when
foreground admission exhausts common space. A failed capacity admission
preserves the previous selection.

Creation persists the empty selector and initial incarnation grant together.
Recovery requires both before it can open the container. The parent directory
is synced before initialization releases ownership.

Cleanup syncs the replacement checkpoint pages and selector together. When
both selectors are valid, cold recovery uses the preceding selector and its
intact journal. A live owner can finish an interrupted retirement after the
shared readiness record confirms the first sync. A second sync makes the
predecessor's retirement durable before the owner clears its covered journal
and reuses its pages. Recovery discards older frames if their clear was lost.
During ordinary exclusive maintenance, an append-only prefix can stay in
the journal and canonical streams when there is no physical retirement,
metadata trimming, or allocation pressure. Maintenance still syncs that
prefix before returning. Ordinary exclusive NoSync maintenance may also
leave replaced collection index pages owned by the preceding checkpoint.
Cleanup-only and forced maintenance reclaim them after retiring that
checkpoint and releasing any readers. A cleanup with no payload retirement
can leave durable inline records and tombstones in the journal. Capacity
admission and journal rollover can checkpoint the prefix earlier.
Standalone namespace checkpoints persist their canonical history before
publishing replacement key snapshots. This also applies when an exclusive
NoSync namespace reaches its pending metadata or replay byte limit.

A selected namespace key checkpoint makes metadata-only work discoverable by
maintenance. The first namespace index publication can include a maintenance
hint in the same durability barrier as its generation, manifest, and
completed canonical prefix.
An exclusive namespace can answer an index freshness check from the
watermark recovered at open while its logical epoch and tail match. Queries
that need canonical keys still populate the state projection.
An exclusive NoSync owner can discover a new namespace as soon as its name
is journaled. The next canonical durability barrier covers the name and its
completed records. A newly reserved incarnation range becomes durable before
dependent records use it.
Exclusive NoSync foreground appends frame a successor binding and maintenance
hint before the route can select that segment. The route makes the hint
discoverable to live maintenance. The next canonical durability barrier
covers the binding, hint, and selected records in journal order.

Collection order and append-ID projections use process-local memory pages
allocated on write. Captured views retain their backing while in use.
Recovery rebuilds these projections from canonical records. Within a live
cohort, a certified collection tail stays admitted across physical
checkpoints. An interrupted edit or a cold open still requires recovery.
Collection query pages are immutable values keyed by collection owner and row
ordinal. Their publication follows the root's sync policy. Readers retain the
selected incarnation across replacement. An ordered digest of canonical
positions, publication generations, sizes, and payload ETags binds the postings
to the recovered rows. A missing or changed page uses canonical scans until
rebuilt. Maintenance removes older projection stream bindings for reclamation.

Reclamation waits for durable retirement and release of outstanding physical
ownership. OFD range locks retain reader extents and private writer
workspace across independent opens. Shared admission validates its view against
the selected root and accepted journal bounds before installing it.
Darwin uses a dedicated OFD byte for lifetime admission. Growth refreshes
capacity and holds allocator authority through preallocation and root selection.
A NoSync join of a fully certified live root inherits its durability watermark
when structural publications are already durable. Records that require a
durability barrier keep their preceding visible values until it completes.
Namespace routing selects the sole append head. Dense NoSync predecessors can
remain unsealed. Repacking a closed source records its final tail before removing
grants. Unlinking an old segment requires its durable route and checkpoint coverage.

On durable roots, transaction application batches each local participant's
effects under namespace authority. The decision is durable before application.
The participant's changes share a durability wait, and queue notifications
follow successful application.

Queue watchers on a shared root observe a sequence before reading queue
statistics, then wait for the sequence to change. Publishers advance it and
wake waiters through the mapped readiness page. Each wake prompts a fresh
queue scan. The timed rescan also observes changes when a publisher exits
before notifying or a visibility timeout expires.

## Verification and performance

Test observable behavior through the production Pouch and lockd paths. Linux
ARM functional checks use QEMU, including cold reopen in both directions with
x86_64. On ARM hard-float they also cover the futex retry timeout and OFD
lock ranges above the 32-bit offset limit. Readiness tests exercise simulated
16 KiB and 64 KiB mapping alignment. Performance comparisons use
native hardware. Cover process and I/O failure, concurrent owners, retained readers,
durable and NoSync recovery, fixed-capacity reclamation, explicit growth, and
transaction completion. Use the applicable Pouch Space CTest groups and the
[lifecycle comparison](../experiments/pouch_space/lifecycle_compare.c).
On Linux, the `pouch_space_smoke` target builds all executables selected by
the `pouch-space` CTest label. On Apple Silicon it builds the platform,
coordination, and physical storage executables. The platform probe checks
owner death before and after publication, retained reads, growth from
independently opened owners, lease fencing, and durable queue and collection
behavior. The coordination suite checks lock lifetimes, large offsets,
concurrent threads and processes, and injected I/O and lock failures.
The physical storage suite covers protected completion at fixed capacity,
concurrent namespace repack, canonical stream and piece repair after modeled
write loss, and recovery of streamed values after owner exit. It uses the
native storage callbacks.

Configure the Darwin smoke build with `LOCKDC_BUILD_TESTS=ON` to include
`pouch_space_lease_test`. It covers shared lease fencing and expiry, queue
commit and rollback, outbox owner death and replay, and lease and messaging
completion at the 10 MiB capacity in durable and NoSync modes.
The SDK index suite verifies shared visibility, concurrent publication,
recovery of derived indexes from canonical state, and retry at fixed capacity.
It runs with both plain and encrypted, compressed payloads.

With tests enabled, the Darwin `pouch_space_smoke_bundle` target builds
`experiments/pouch_space/pouch-space-apfs-smoke.tar.gz` under the build
directory. Extract it on Apple Silicon running macOS 15 or later and run
`./run-apfs-smoke.sh DIRECTORY` with a writable directory on APFS. The runner
uses the same case list as native `ctest -L pouch-space` and records page size,
available volume blocks, and per-case logs beneath the supplied directory.
Record available volume space alongside any capacity failure so container
pressure can be distinguished from filesystem exhaustion.

The GitHub Actions workflow on `experiment/pouch-space` builds the native
suite, runs it on Apple Silicon with APFS, and checks cold reopen with Linux
in both directions. All 69 cases passed on macOS 15.8.2 with 16 KiB pages.
[Native APFS run](https://github.com/c89systems/liblockdc/actions/runs/45243737998)

For cold cross-platform file exchange, `pouch_space_test FILE wire-create`
writes a checkpointed value and a later journal publication.
`pouch_space_test FILE wire-verify` cold-opens the copy and verifies
both values. For an SDK collection image, run `pouch_space_platform_test FILE`
with `--write-collection` on the source host and `--verify-collection` on the
destination. Use a new image in each direction.

The lifecycle comparison's `--parallel-joins` option measures a cohort
starting together after first-owner recovery. It reports elapsed time,
join-thread CPU, and the slowest individual join. Indexed verification reads
and queries through each joined client.

The regular compaction tests hold a collection reader pin while the real
worker retries. They check retry cadence, progress in another namespace,
and reclamation after release. One case holds a capacity claim in the
fixed-size container.

Under advisory pressure the worker samples ordinary headroom independently
of its maintenance cadence. Advisory passes occur at most once per second
and share the regular worker's schedule. While draining advisory pressure,
the allocator can checkpoint unpinned retirements that cover at least an
eighth of usable capacity, with a one-stream-chunk minimum. The worker can
still reclaim a stream chunk during its remaining drain turns after clearing
the pressure hint. Low verified headroom enables smaller batches and urgent
retries, with backoff when work stalls.

Shared single-file background maintenance retains one metadata projection per
open root and releases its receiver and physical pins after each pass. It
validates the collection, snapshot, active stream incarnation, and ready tail
before reuse. The recovery case keeps that projection resident across peer
exit before and after snapshot publication, then checks retention, append-ID
receipts, and reclamation in both sync modes.

For a native Linux comparison, build `pouch_space_compaction_test` in
Release and run it with an absolute scratch directory, a backend argument
(`multifile` or `single-file`), and `soak` or `soak-durable`. Use
`soak-mixed` or `soak-mixed-durable` for four collections across two
namespaces. Both backends use the same collection churn and one-second
maintenance interval. The probe reports compactor CPU during writes and
drain, append and delete pair latency, peak RSS, and occupied storage.

Compare matched builds and multifile on representative lease, coordination,
messaging, and indexed workloads. Record work time, CPU, mutation p95 and
late-window latency, index readiness, first-owner and live-join reopen,
post-open verification, and close. Preserve durable throughput and indexed
readiness advantages.

On native x86_64 with ext4, matched runs at 10 MiB showed lower elapsed work
and CPU across the tested indexed and messaging profiles. At 192 MiB of
indexed churn, median NoSync late-window p95 was 49 ms on both backends and
durable work took 18 s on single-file and 66 s on multifile. Indexed reopen
through verification was faster on single-file. Standalone opens and final
maintenance can cost more, and sustained indexed runs used about 6 MiB more
peak resident memory. Review these costs before deployment.

## Historical evidence

- [Original proposal and design rationale](archive/pouch-space-proposal.md)
- [Implementation history, tests, and benchmark results](archive/pouch-space-implementation.md)
- [Foundation measurements and reproduction](archive/pouch-space-experiment.md)
- [Original foundation design](archive/pouch-space-foundation-design.md)
- [Physical interface](../experiments/pouch_space/space.h)

Later implementation commits record their verification and measurements.
