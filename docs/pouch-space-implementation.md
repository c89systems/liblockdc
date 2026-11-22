# Native Pouch implementation contract

## Scope

The experimental native Linux x86_64 backend opens through the private
`ps_pouch_client_open` factory. It supports independently opened shared owners
over one persistent file. Normal Pouch opens multifile roots. Production cutover
requires an explicit deployment decision. Existing-root migration, Darwin,
QEMU, and large-machine qualification are outside the current experiment.

The development baseline is a fixed 10 MiB container. Growth is explicit in
1 MiB increments. The experimental layout can evolve within the behavioral
contract below. The project records layout and capacity constants in the
[physical storage interface](../experiments/pouch_space/space.h).

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

The [Pouch storage specification](pouch-storage.md) governs public storage
semantics. Matching the multifile layout or internal indexing algorithm is
outside the native contract.

## Physical ownership and recovery

Checksummed root selectors and a bounded rolling journal choose the accepted
physical prefix. A copy-on-write directory binds typed identities to extents.
Its leaves store ownership descriptors directly for small key metadata and
transaction decisions. Other descriptors reside in packed metadata pages.
Acknowledged durable operations survive recovery.
NoSync work without a durability barrier can be lost. Recovery checks framing,
ownership, and commit boundaries before admitting complete canonical publication
groups.

Private preparation reserves payload and descriptor capacity before spending it.
Publication revalidates its logical authority. Producers run outside
allocator publication authority. Independent owners can share durability work.
Protected maintenance capacity funds checkpoint and completion work when
foreground admission exhausts common space. A failed capacity admission
preserves the previous selection.

Reclamation follows durable root retirement and outstanding physical ownership.
Linux OFD range witnesses retain reader extents and private writer workspace
across independent opens. The shared readiness certificate identifies the
root and accepted journal sequence and byte bounds. It can validate
speculative adoption or private metadata reads before installation. Startup
namespace recovery validates privately decoded checkpoint metadata against the
physical root and reloads through witnessed reads on a root transition.
Shared readers retain exact physical witnesses for their lifetimes.

## Verification and performance

Test observable behavior through the production Pouch and lockd paths on native
x86_64. Cover process and I/O failure, concurrent owners, retained readers,
durable and NoSync recovery, fixed-capacity reclamation, explicit growth, and
transaction completion. Use the applicable Pouch Space CTest groups and the
[lifecycle comparison](../experiments/pouch_space/lifecycle_compare.c).

Compare matched builds and multifile on representative lease, coordination,
messaging, and indexed workloads. Record work time, CPU, mutation p95 and
late-window latency, index readiness, first-owner and live-join reopen,
post-open verification, and close. Preserve durable throughput and indexed
readiness advantages. Indexed CPU, NoSync tail latency, and live-join reopen
remain open performance work. Review material tradeoffs before deployment.

## Historical evidence

The [implementation history](archive/pouch-space-implementation.md) and
[foundation experiments](archive/pouch-space-experiment.md) preserve earlier
designs, test matrices, and benchmark results. Subsequent changes record their
verification and measurements in commit messages. The [original proposal](pouch-space.md)
contains the design rationale.
