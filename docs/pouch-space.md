# Pouch Space

## Scope

The experimental backend stores a Pouch root in one persistent file. It runs
on Linux x86_64, AArch64, and ARM hard-float with GNU libc and musl.
The Apple Silicon build targets macOS 15.0 or later and is ready for native
APFS verification. The private
`ps_pouch_client_open` factory supports independently opened owners sharing
that root. Normal Pouch opens multifile roots. Production cutover requires an
explicit deployment decision. Existing-root migration and large-machine
qualification are deferred.

Development uses a fixed 10 MiB container with explicit 1 MiB growth. The
layout, indexing, and maintenance schedule can evolve within this contract.

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
Acknowledged durable operations survive recovery. NoSync work without a
durability barrier can be lost. Recovery checks framing, ownership, and commit
boundaries before admitting complete canonical publication groups. Before
admitting a cold namespace for writes, recovery checks for complete records past
the selected stream tail and restores their outstanding capacity obligations.
After validating the recovered prefix, the first owner can reuse proof from
a previous durability barrier. A clean exclusive close can record that
proof for the exact journal and a bounded set of stream tails. When reusing
that durability proof, recovery syncs newly discovered records outside its
coverage before exposing them.
Owners that reopen using a proof acquire an incarnation range on first use.
After a clean exclusive NoSync close, a checked journal can serve reads.
The first persistent mutation or explicit flush stabilizes that recovered view.

Private preparation reserves payload and descriptor capacity before spending it.
Publication revalidates its logical authority. Producers run outside
allocator publication authority. Independent owners can share durability work.
Protected maintenance capacity funds checkpoint and completion work when
foreground admission exhausts common space. A failed capacity admission
preserves the previous selection.

Collection order and append-ID projections use process-local memory pages
allocated on write. Captured views retain their backing while in use.
Recovery rebuilds these projections from canonical records.

Reclamation waits for durable retirement and release of outstanding physical
ownership. OFD range locks retain reader extents and private writer
workspace across independent opens. Shared admission validates its view against
the selected root and accepted journal bounds before installing it.
A NoSync join of a fully certified live root inherits its durability watermark
when structural publications are already durable. Records that require a
durability barrier keep their preceding visible values until it completes.
Namespace routing selects the sole append head. Dense NoSync predecessors can
remain unsealed. Repacking a closed source records its final tail before removing
grants. Unlinking an old segment requires its durable route and checkpoint coverage.

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
the `pouch-space` CTest label. On Apple Silicon it builds
`pouch_space_platform_test`, which checks process-death recovery, retained
reader data, physical growth, wait timeouts, lease fencing, and whether durable
collection retries and queue acknowledgements survive cold reopen. Pass the
probe a writable directory on an APFS volume.

The lifecycle comparison's `--parallel-joins` option measures a cohort
starting together after first-owner recovery. It reports elapsed time,
join-thread CPU, and the slowest individual join. Indexed verification reads
and queries through each joined client.

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
