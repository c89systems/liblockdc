# Native single-file foundation experiment

The [container foundation](../experiments/pouch_space/space.h) implements one
preallocated file, typed owner/key bindings, bounded streaming, independent
thread writers, checksummed publication/recovery, reader pins, retirement/reuse
and explicit growth. Default capacity is 10 MiB; tests grow 1 to 2 to 3 MiB.
Its [design and ordering contract](../experiments/pouch_space/DESIGN.md) defines
the failure model and limits. It is not connected to normal Pouch open behavior.

All persistent prototype metadata and payloads occupy that file. The comparison
audits the store and its parent for sidecars, excluding the test helper's owned
temporary-directory marker. The foundation does not spool payloads. The broader
single-file proposal permits explicitly documented disposable scratch, whose
I/O and peak space must be included when those operations are integrated.

## Correctness evidence

Native x86_64 verification on 2026-11-13:

- 75 selected Debug unit/contract tests passed, including existing Pouch,
  collection, compression durability/recovery, retention, XA and allocator cases.
- All nine experiment tests passed under Debug sanitizers and glibc Release.
- All nine passed under native musl Release, including default-stack workers.
- Valgrind reported zero errors and no lost blocks; the parent freed every heap
  allocation. Child processes intentionally exit with private reservations.
- Every one of 160 untraced comparison runs verified all payloads before and
  after reopening, with exact entry counts and single-file artifact checks.

The foundation test injects failed allocation, preallocation, writes and syncs;
persists torn write prefixes; discards unsynced writes; and persists valid
publication metadata ahead of missing bodies. It exercises each publication,
checkpoint and growth I/O boundary, stale/foreign references, payload/checkpoint
corruption, short I/O/EINTR, full/fragmented capacity, journal rollover, metadata
exhaustion, held readers across retirement/reuse, independent writers during an
unfinished producer and growth, process exclusion/death, and 3,000 deterministic
reference-model operations across reopen cycles. Durable incarnation ranges
reject references to lost NoSync publications; interrupted opens and a completely
full journal that checkpoints during recovery are covered too. A value over
1 MiB with 7-byte input chunks asserts that individual storage allocations stay
below 17 KiB.
These are deterministic protocol tests, not a hardware power-loss certification.

## Measurement contract

Measurements use native x86_64 Linux, ext4 on `/dev/sda1`, pinned Bootlin GCC
15.3.0, and the glibc Release preset. Five independent samples alternate engine
order. All commands run serially. Reads use the ordinary page cache; no cache
drop or device isolation is claimed. Raw logs are under
`build/pouch-space-comparison/`, with consolidated output in `results.txt`.

The baseline is the actual current segmented Pouch implementation:

- State uses `lc_pouch_state_write/read`, with no lease wrapper or CAS condition.
- Collections use public create/append/read APIs. The container comparison uses
  the same incremental collection JSON validator/compactor, then publishes a
  typed physical binding. It does not implement Pouch catalogs, receipts,
  SHA-256 identities, permanent-position/tombstone authority or lease semantics.

Both sides use identical generated JSON, 4 KiB producer chunks, no compression,
no encryption, and matching NoSync/durable and writer counts. Query indexing and
background maintenance are disabled in Pouch. Each writer has an independent
namespace/collection; Pouch rolls at 1 MiB. State writes every key twice;
collections append once. Small profiles use 1 KiB values and 256 rows for NoSync,
128 for durable; large profiles use 64 KiB values and 32 rows. Every container
starts with the same genuinely reserved 10 MiB file, without automatic growth.

Write timing excludes store creation and thread startup, but includes namespace
warmup, source setup and all publications. Creation and close are reported
separately. Read timing includes byte verification. `ready_ms` includes open
plus the first complete verified read after reopen, accounting for Pouch's lazy
namespace recovery. Container reads are individual physical reads; Pouch's
collection read is one JSONL operation per collection. Neither is a filtered
query benchmark. No production performance acceptance is claimed.

## Observed results

Median write-phase milliseconds, **container / current Pouch**. State phases
contain twice as many writes as collection phases; compare within each cell.

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-nosync-1w | 2.11 / 15.35 | 1.90 / 30.85 |
| small-nosync-4w | 3.61 / 16.16 | 2.82 / 19.27 |
| small-durable-1w | 1814.98 / 1518.97 | 865.19 / 738.32 |
| small-durable-4w | 1405.85 / 1067.79 | 525.68 / 549.63 |
| large-nosync-1w | 5.16 / 61.67 | 3.99 / 29.81 |
| large-nosync-4w | 3.48 / 19.25 | 2.01 / 2.38 |
| large-durable-1w | 369.78 / 410.68 | 180.75 / 196.82 |
| large-durable-4w | 313.44 / 370.59 | 144.93 / 165.31 |

NoSync state throughput is promising: its write-time ranges are disjoint across
all four profiles. Durable outcomes are mixed and noisy. For small durable state
with four writers, container samples span 1362–1494 ms versus 1022–1118 ms for
Pouch; the container median is 32% slower, outside the observed noise. Even the
apparent large NoSync four-writer collection gain has overlapping ranges:
1.94–2.55 ms versus 2.01–2.53 ms. These observations do not establish a durable
or general win.

Tail latency also exposes regressions despite throughput gains. Median observed
per-run p99 for small NoSync state with four writers is 211 us versus 74 us;
small durable state with four writers is 71.7 ms versus 52.4 ms. Large profiles
have only 32/64 operations, so their reported p99 is the sample maximum, not an
estimate of a stable production tail.

Recovery and shutdown must be considered together. These are medians of each
sample's open-plus-first-read and close-plus-open-plus-first-read sums:

| State profile | Open + first read, container / Pouch ms | Including close, container / Pouch ms |
| --- | ---: | ---: |
| small-nosync-1w | 14.90 / 5.20 | 14.94 / 17.93 |
| small-durable-4w | 6.53 / 2.93 | 6.56 / 62.78 |
| large-nosync-1w | 21.67 / 4.03 | 21.67 / 19.77 |
| large-durable-4w | 12.21 / 4.03 | 12.22 / 65.09 |

The foundation closes without an implicit checkpoint. It verifies the journal
tail and performs a structural sync at open before reuse. Pouch spends much
more time closing, and defers some recovery to first access. Changing checkpoint
scheduling needs a separate measured decision; reporting open alone would hide
part of that tradeoff.

The persistent file count is exactly one, versus 9–21 for state and 23–59 for
collections in these profiles. Reserved capacity is always 10 MiB; Pouch's
filesystem allocation is smaller for these fixtures. Small-value page allocation
also costs writes: the small NoSync one-writer state phase accounts for
2,142,208 Linux process `write_bytes` versus 811,008 for Pouch. These counters
are kernel accounting, not physical-device IOPS or device write amplification.
Raw logs also report CPU time, syscall counts, p50/p95/p99, read/creation/close
times, filesystem bytes and peak process RSS.

Separate `strace -f -c` diagnostics used 32 rows, 1 KiB values and four writers.
Counts below include creation, writes, reads, close, reopen and cleanup. Traced
latencies are excluded from the comparison above. Each container diagnostic
also performs exactly one `fallocate`.

| Workload | Container fdatasync / fsync | Pouch fdatasync / fsync |
| --- | ---: | ---: |
| State NoSync | 4 / 1 | 0 / 36 |
| State durable | 68 / 1 | 64 / 36 |
| Collection NoSync | 4 / 1 | 0 / 110 |
| Collection durable | 36 / 1 | 32 / 110 |

## Reproduce

```sh
bash scripts/compare_pouch_space.sh smoke
bash scripts/compare_pouch_space.sh measure "$PWD/build/pouch-space-comparison"
```

`POUCH_SPACE_RUNS` accepts 3, 5 (default), 7 or 9. An optional absolute directory
selects the backing device. The runner builds only native glibc targets and
removes each owned database fixture, retaining per-sample logs. It never invokes
QEMU or Darwin builds. The test helper's transient owner marker is outside the
store and excluded from reported store inode/byte counts.

The broader Debug check used:

```sh
cmake --preset debug -DLOCKDC_BUILD_EXAMPLES=OFF -DLOCKDC_BUILD_E2E_TESTS=OFF
cmake --build --preset debug
ctest --test-dir build/debug -L unit -LE lifecycle-host \
  --output-on-failure --no-tests=error --parallel 1 --stop-on-failure
```

For native musl, provision `deps-x86_64-linux-musl`, configure the
`x86_64-linux-musl-release` preset with benchmarks enabled, build
`pouch_space_test pouch_space_compare`, then run CTest with `-R '^pouch_space_'`.
Valgrind output is retained in `build/pouch-space-comparison/valgrind.log`.

## Next commitments

Before larger-scale tests, replace the fixed 64 KiB checkpoint directory ceiling
with growable typed metadata. Before a production adapter, resolve small-value
packing, grouped durability/publication contention, checkpoint scheduling,
relocating compaction and maintenance payload reserve, process-shared reader
ownership, and complete public Pouch semantics. The current foundation permits
rebinding a deleted physical key; it does not supply permanent collection
position or retry authority. Full codec/index/XA/outbox integration and migration
remain outside this first milestone. Production adoption still requires the
broader [draft acceptance evidence](pouch-space.md).
