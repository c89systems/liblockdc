# Native single-file foundation experiment

The [container foundation](../experiments/pouch_space/space.h) implements one
preallocated file, typed owner/key bindings, bounded streaming, independent
thread writers, checksummed publication/recovery, reader pins, retirement/reuse
and explicit growth. Default capacity is 10 MiB; tests grow 1 to 2 to 3 MiB.
The later stages add canonical append-owned streams, whole-stream lifecycle
operations and typed name catalogs; their verification and measurements appear
below in implementation order.
Its [design and ordering contract](../experiments/pouch_space/DESIGN.md) defines
the failure model and limits. It is not connected to normal Pouch open behavior.

All persistent prototype metadata and payloads occupy that file. The comparison
audits the store and its parent for sidecars, excluding the test helper's owned
temporary-directory marker. The foundation does not spool payloads. The broader
single-file proposal permits explicitly documented disposable scratch, whose
I/O and peak space must be included when those operations are integrated.

## Correctness evidence

Initial foundation verification on native x86_64, 2026-11-13:

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

## Commit coordination follow-up

The original implementation held the directory mutex throughout each commit's
`fdatasync`. That serialized durable writers and blocked read admission and
allocation during the barrier. The revised implementation appends/installs under
the directory mutex, then releases it before callers coordinate a sync of the
ready sequence prefix. Arrivals during one barrier form the next group. There
is no fill delay or background worker. Readers retain the prior committed view
until the appropriate barrier completes, including pending deletes and multiple
replacements of the same key. Checkpoint and commit barriers share a separate
I/O mutex; commit completion never acquires the directory mutex.

A second series on the same host compared the original `fcdde32` executable,
the revised container, and current Pouch. Five samples rotate these three
executables in order, with identical workloads and serial execution. All 240
runs verified complete payloads before and after reopen and checked store
artifacts. Logs and consolidated medians/ranges are in `build/pouch-space-fix/`
and `results.json`. The original executable is retained there as
`pouch_space_compare-before`. The revised benchmark times real `fdatasync` and
`fsync` calls for both engines, outside the library; the original executable
does not include those new timing counters. No tracing is enabled in these runs.

Median durable write-phase milliseconds:

| Profile | Kind | Original container | Revised container | Current Pouch |
| --- | --- | ---: | ---: | ---: |
| small-durable-1w | State | 1539.16 | 1576.89 | 1469.46 |
| small-durable-1w | Collection | 754.96 | 724.49 | 723.54 |
| small-durable-4w | State | 1399.77 | 708.14 | 849.55 |
| small-durable-4w | Collection | 623.29 | 385.30 | 454.02 |
| large-durable-1w | State | 337.81 | 365.26 | 349.74 |
| large-durable-1w | Collection | 204.75 | 174.86 | 200.26 |
| large-durable-4w | State | 340.85 | 180.65 | 366.13 |
| large-durable-4w | Collection | 172.62 | 93.13 | 166.66 |

Four-writer phases use half as many container commit barriers: 128 for 256
small state writes, 64 for 128 small collection writes, 32 for 64 large state
writes, and 16 for 32 large collection writes. The largest observed group is
three. Compared with the original container, the four-writer medians improve
by 49%, 38%, 47% and 46%, respectively; all four observed before/after ranges
are disjoint. Small state samples span 610–852 ms after the change versus
1089–1516 ms before it. Current Pouch spans 819–954 ms, overlapping the revised
container range, so the 17% lower median does not establish a universal win.
Median small state p99 falls from 67.39 ms to 29.63 ms, versus 39.10 ms for Pouch.

Single-writer durability is unchanged: every successful commit still waits for
one sync. Small state uses 256 barriers and spends a median 1541 ms inside sync
calls out of a 1577 ms write phase, approximately 98%. Its revised median is 7%
above Pouch, with overlapping ranges. The coordinator removes contention and
amortizes concurrent barriers; it does not solve single-writer storage latency.
`sync_ms` sums call durations across threads and is not device busy time.

Physical pages remain 4 KiB, matching this filesystem's block size. The device
reports 512-byte logical and physical sectors. Payloads allocate whole pages;
small payload writes and their separate journal records can dirty separate
filesystem blocks. Kernel write accounting remains about 2 MiB for the small
single-writer durable state phase. Packing small logical records into aligned
pages is a separate layout problem; neither its performance effect nor a
single-writer improvement has been demonstrated here.

NoSync write medians remain within 0.7 ms of the original container in every
profile. Small state warm reads are 0.27–0.57 ms for the revised container across
the profiles versus 2.19–4.12 ms for Pouch; large state reads are 1.90–2.07 ms
versus 3.42–3.96 ms. These remain serial, verified physical reads after writers
join. Concurrent scan performance is not measured by this comparison.

The follow-up adds a deterministic commit test that holds sync while proving
reader/allocation/publication progress, prior committed visibility for writes
and deletes, grouped acknowledgments, failed-barrier wakeups, checkpoint lock
ordering and same-key version selection between successive barriers. All ten
experiment tests pass under Debug ASan/UBSan, native glibc Release and native
musl Release. Memcheck reports zero errors and no lost allocations for both the
foundation and commit tests. The parent process frees every allocation.
Both tests also pass with ThreadSanitizer and `halt_on_error=1`, without race
diagnostics. Helgrind reports one conflict on the existing `__atomic` poison
flag; every access is atomic, including the reported read and write, whose
relaxed operations compile to plain x86 instructions. No Helgrind clean-pass
claim is made. Logs for all three tools are retained beside the paired runs.
The broader Debug unit/contract gate passes all 76 selected tests with the
documented native configuration.

## Growable metadata checkpoint follow-up

The foundation now checkpoints through the typed copy-on-write directory and
packed immutable descriptor pages. Metadata can grow into the common pool;
the fixed 64 KiB snapshot ceiling is gone. Foreground admission reserves space
for a complete replacement metadata tree plus 16 KiB of maintenance payload
capacity. This is a fresh experimental format: recreate prototype fixtures.
Normal Pouch files and open behavior are unchanged.

The 12,000-entry regression crosses the old metadata limit and the protected
metadata region. Updating one entry writes at most four directory pages and
one descriptor page. Tests also cover shared descriptor pages, records spanning
pages, old reader pins across reuse, rebinding at implicit journal rollover,
every checkpoint I/O/allocation failure, and recovery allocation failures.
Opening the large-value fixture reads at most the journal, two root headers
and six metadata pages; it does not read the checkpointed 3 MiB payload.

The broader Debug unit/contract gate passed 79 tests. After the final metadata
changes, all 13 affected experiment tests passed under Debug ASan/UBSan,
native glibc Release and native musl Release. ThreadSanitizer passed the
directory, commit and metadata tests; the final recovery-cache change was
rechecked with the metadata test. Memcheck reports zero errors and no lost
allocations for foundation and metadata tests, including the final metadata
recheck. Logs are retained under `build/pouch-space-integrated/`.

Five alternating samples of the original eight profiles produced these write
medians, in milliseconds, **container / current Pouch**:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-nosync-1w | 1.98 / 14.30 | 1.97 / 30.73 |
| small-nosync-4w | 3.51 / 15.76 | 2.34 / 13.18 |
| small-durable-1w | 1484.92 / 1312.22 | 725.11 / 743.40 |
| small-durable-4w | 822.43 / 964.22 | 395.04 / 483.11 |
| large-nosync-1w | 4.59 / 48.61 | 4.02 / 25.94 |
| large-nosync-4w | 2.95 / 16.12 | 1.89 / 2.22 |
| large-durable-1w | 331.72 / 354.33 | 170.80 / 190.18 |
| large-durable-4w | 163.22 / 326.63 | 94.21 / 167.02 |

Single-writer durable state remains slower in the small profile, with
overlapping sample ranges. The large durable four-writer ranges are disjoint:
151–210 ms versus 297–788 ms for state, and 76–111 ms versus 159–260 ms for
collections. Small NoSync state with four writers still has a worse median p99:
148 us versus 84 us. Serial verified warm reads take 0.28–0.59 ms for small
state versus 2.37–4.23 ms for Pouch; large state takes 2.02–2.09 ms versus
3.33–3.68 ms. These are physical reads, without a concurrent query/scan claim.

A further 80 runs force journal rollover: each writes 2,048 publications of
1 KiB into the same preallocated 10 MiB container. State uses 1,024 keys written
twice; collections use 2,048 keys appended once. Every container run checkpoints
exactly once during its write phase. Median write-phase milliseconds:

| Profile | State, container / Pouch | Collection-shaped, container / Pouch |
| --- | ---: | ---: |
| rollover-nosync-1w | 38.24 / 59.06 | 46.29 / 188.70 |
| rollover-nosync-4w | 39.83 / 24.76 | 47.20 / 73.62 |
| rollover-durable-1w | 10410.92 / 10760.75 | 11656.87 / 11053.77 |
| rollover-durable-4w | 5063.87 / 7953.45 | 5611.59 / 8067.42 |

The four-writer NoSync state median regresses by 61%. Sample ranges overlap
(37–67 ms versus 23–82 ms); four of five Pouch samples are below 32 ms.
The container issues three structural checkpoint syncs even in NoSync mode and
accounts for about 8.8 MB of kernel writes versus 3.2 MB for Pouch. Its summed
sync durations are 23–48 ms; Pouch's namespace syncs overlap across writers, so
summed sync durations alone cannot identify the critical path. Payload packing
and checkpoint scheduling remain performance work. The rare checkpoint also
falls outside p99 in this workload; p99 cannot establish checkpoint latency.

Durable four-writer rollover ranges are disjoint: 4.56–5.70 s versus
7.11–8.60 s for state, and 4.80–5.88 s versus 7.01–8.25 s for collections.
Container state uses a median 1,020 data syncs versus 2,048 for Pouch, including
its three checkpoint barriers. Single-writer durable ranges overlap; these
measurements establish no single-writer advantage. Verified warm state reads
take about 2–2.6 ms versus 16–19 ms, and collection-shaped reads about
4.4–5.1 ms versus 7.3–8.3 ms. Durably written state reopens and completes its
first verified read in about 8 ms versus 18 ms; NoSync recovery remains mixed.

All 240 runs verified payloads before and after reopen, entry counts and store
artifacts. Raw standard and rollover logs are in
`build/pouch-space-integrated/measure/` and
`build/pouch-space-integrated/rollover/`; `results.json` retains the raw rows and
summary ranges. These are foundation measurements, with the semantic scope
differences described above, rather than full-backend acceptance evidence.

## Payload packing follow-up

Small payload tails now reserve 64-byte cells inside shared, aligned 4 KiB pages.
Whole-page portions remain whole runs. Writers reserve disjoint ranges under
the allocator mutex and perform payload I/O outside it. Free-span bins bound
selection work independently of the number of participating pages. Recovery
reconstructs ownership from the existing descriptors; cell reuse still waits
for journal/root retirement and reader pins. No payload cache or spool is added.

All 14 experiment tests pass under Debug ASan/UBSan and native glibc/musl
Release. Five storage tests pass with ThreadSanitizer. The extended packing test
also passes in each configuration and Memcheck, with 26,403 observable checks,
zero errors and every heap allocation freed. It covers both sides of every
cell boundary, fragmented spans, return of completely freed pages, mixed
whole-run/tail allocation failures and failed abort cleanup, persistent torn
write prefixes, checksummed invalid ownership, and a blocked payload write
while a neighbour commits and checkpoints, in both durability modes.

The standard comparison repeats all 160 verified runs. Median write-phase
milliseconds, **packed container / current Pouch**:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-nosync-1w | 2.11 / 16.08 | 2.01 / 49.46 |
| small-nosync-4w | 3.69 / 20.62 | 3.11 / 24.50 |
| small-durable-1w | 1016.82 / 1540.54 | 484.22 / 893.51 |
| small-durable-4w | 598.30 / 1081.97 | 327.82 / 507.72 |
| large-nosync-1w | 5.13 / 44.90 | 4.41 / 29.27 |
| large-nosync-4w | 3.68 / 18.29 | 2.29 / 2.67 |
| large-durable-1w | 367.15 / 463.99 | 202.82 / 209.90 |
| large-durable-4w | 187.12 / 414.98 | 93.34 / 168.08 |

The 80-run rollover comparison also repeats, with exactly one checkpoint in
each container write phase:

| Profile | State, container / Pouch ms | Collection-shaped, container / Pouch ms |
| --- | ---: | ---: |
| rollover-nosync-1w | 22.29 / 59.37 | 28.72 / 195.95 |
| rollover-nosync-4w | 30.78 / 24.23 | 34.95 / 76.45 |
| rollover-durable-1w | 6889.49 / 10607.43 | 6402.42 / 11018.26 |
| rollover-durable-4w | 3846.47 / 7588.40 | 4048.93 / 7687.74 |

All four durable rollover container/Pouch sample ranges are disjoint. This
does not establish a general win: four-writer NoSync state rollover remains
slower, and the standard large four-writer collection ranges overlap despite
the lower median. Small NoSync state p99 with four writers remains worse:
245 us versus 73 us. Serial verified warm state reads take 0.27–0.48 ms versus
2.49–5.86 ms in small profiles, and 2.07–2.20 ms versus 3.71–4.16 ms in large
profiles. Concurrent scans and cold-cache behavior remain unmeasured.

A separate 60-run check rotates the packed executable, the preserved
pre-packing executable corresponding to `e0c19a2`, and Pouch across five samples:

| Profile | Kind | Before packing, ms | Packed, ms | Pouch, ms |
| --- | --- | ---: | ---: | ---: |
| small-durable-1w | State | 1326.56 | 857.03 | 1274.69 |
| small-durable-1w | Collection | 654.07 | 452.42 | 673.81 |
| rollover-nosync-4w | State | 37.94 | 26.00 | 22.69 |
| rollover-nosync-4w | Collection | 48.23 | 31.86 | 66.53 |

Packing reduces the paired single-writer durable medians by 35% for state and
31% for collections. Their before/after ranges are disjoint, as are packed/Pouch
ranges: state takes 701–966 ms versus 1193–1686 ms for Pouch, and collections
383–477 ms versus 554–769 ms. Each engine still issues one data sync per
successful publication: 256 state syncs or 128 collection syncs. Kernel write
accounting is unchanged by packing in these single-writer durable fixtures
(2,117,632 bytes for state, 1,056,768 for collections), and remains above Pouch.
The measured latency gain does not imply fewer barriers or fewer durable bytes.

For paired four-writer NoSync state rollover, packing cuts kernel writes from
8,802,304 to 2,596,864 bytes, below Pouch's 3,244,032 bytes, and write time by
31%. The remaining 15% median slowdown versus Pouch and overlapping ranges
remain explicit. Container checkpoint sync time sums fall from a 23.79 ms
median to 12.42 ms, but three structural barriers still lie on the write path.
P99 again excludes the rare checkpoint. Canonical append-owned streams and
checkpoint scheduling remain necessary integration/performance work.

All 300 runs verify payloads before and after reopen and audit file artifacts.
Logs, the paired runner, and `packing-results.json` are retained in
`build/pouch-space-integrated/`, under `packing-measure/`, `packing-rollover/`
and `packing-paired/`. The pre-packing executable is retained as
`pouch_space_compare-multifile-baseline`; its normal Pouch implementation is
unchanged. The physical foundation still lacks the public semantics described
in the measurement contract, so these results are not full-backend acceptance.

## Canonical stream follow-up

The experimental target now has append-owned canonical streams, with fixed-size
run grants and checkpoint tail hints. Appending inside a grant emits no
per-document container journal event. Producers and readers use 16 KiB buffers;
the record owner supplies canonical validation, metadata/header patches and
tail recovery. Reused space is durably initialized before grant publication.
Preparation batches at most 1 MiB, returns spare zeroed pages only after its
barrier, and can share that barrier with other ready work. The
[design contract](../experiments/pouch_space/DESIGN.md#canonical-append-owned-streams)
describes ownership, suffix repair and the native/fallback initialization paths.

The stream fixture uses the actual Pouch binary header codec, payload CRC and
PENDING-finalization rules, with a key and minimal 16-byte row/round metadata.
Collection-shaped input also passes the existing incremental JSON validator.
It lacks public catalogs, SHA-256 identities, receipts, permanent positions,
leases, codecs, indexes and XA/outbox semantics. Normal Pouch still uses its
multifile implementation. These comparisons measure the physical foundation
and do not establish feature parity or production-backend acceptance.

All 31 experiment tests pass under Debug ASan/UBSan and native glibc/musl
Release; affected stream additions were rerun after the final test edits.
All seven storage tests pass under ThreadSanitizer. Final stream Memcheck
reports 3,257,946 observable checks, zero errors and every heap allocation freed.
Coverage includes every append, preparation, checkpoint and open/repair I/O or
heap failure; persisted torn prefixes; grant-before-body persistence; complete
rejected-suffix clearing; fixed reader views; fragmented grants; full capacity
and growth; and held sync/preparation concurrent with independent publication
and checkpoint. Mixed immutable/stream groups prove exact durable-prefix
visibility. Stale/foreign references, failed-open cleanup, invalid read ranges
and empty reads at the committed tail are covered too.

A growth regression was reproduced before its correction: an ignored existing
file tail could contain valid-looking canonical records, and preallocation did
not erase them. Grown free intervals now remain uncertified until preparation.
The regression verifies both native zero-range allocation and bounded-write
fallback, durable zero provenance and successful append/reopen afterward.
These are deterministic storage-protocol tests, not hardware power-loss proof.

### Fresh stores

The final comparison contains 240 fresh-store runs, alternating all six variants
over five samples and the same eight profiles. Each cell lists median write
milliseconds as **stream / packed immutable prototype / current Pouch**:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-nosync-1w | 1.35 / 1.58 / 13.08 | 1.39 / 1.64 / 29.39 |
| small-nosync-4w | 1.33 / 2.71 / 15.77 | 1.12 / 2.08 / 14.49 |
| small-durable-1w | 624.17 / 811.62 / 1248.50 | 318.97 / 392.11 / 628.90 |
| small-durable-4w | 509.48 / 516.71 / 939.37 | 218.52 / 231.42 / 414.41 |
| large-nosync-1w | 5.29 / 4.51 / 39.04 | 3.58 / 3.69 / 24.39 |
| large-nosync-4w | 2.79 / 3.20 / 15.70 | 1.78 / 2.04 / 2.26 |
| large-durable-1w | 322.63 / 353.48 / 352.19 | 157.35 / 189.90 / 158.87 |
| large-durable-4w | 174.61 / 184.55 / 338.63 | 102.56 / 83.42 / 158.11 |

Small single-writer durable state ranges are disjoint: stream 610–653 ms,
packed 775–932 ms and Pouch 1119–1290 ms. Its 256 record publications still use
256 steady-state data syncs, plus one stream-head creation sync. Stream kernel
write accounting is 1,343,488 bytes versus packed 2,117,632 and Pouch 1,458,176.
This result does not come from weakening sync-on-commit.

There is no universal win. Large single-writer durable ranges overlap across
all three variants. Large durable four-writer collections are slower than the
packed prototype: 98–107 ms versus 80–94 ms, although Pouch takes 125–237 ms.
Large single-writer NoSync state has a worse median close/open/first-read cycle:
23.90 ms versus Pouch 15.91 ms, with overlapping ranges of 20.17–36.18 and
14.03–33.55 ms. Warm verified state reads take about 0.2–0.5 ms for small
profiles versus Pouch 1.9–4.8 ms, and 1.9–2.9 ms for large profiles versus
2.7–3.9 ms. These remain cached physical reads, not concurrent scans or
cold-cache query measurements.

Geometric grants reserve slack: the fresh large four-writer collection profile
reserves 5,586,944 payload bytes for 2 MiB of application payload. Grant policy,
many-small-owner behavior and maintenance reclamation still need measurement
and tuning before production integration.

### Longer append histories

Another 120 runs publish 2,048 records of 1 KiB each, with five samples. State
writes 1,024 keys twice; collections append 2,048 keys once. The packed prototype
checkpoints once in every write phase; streams checkpoint zero times:

| Profile | State, stream / packed / Pouch ms | Collection, stream / packed / Pouch ms |
| --- | ---: | ---: |
| long-nosync-1w | 4.46 / 23.24 / 51.03 | 8.18 / 24.31 / 160.15 |
| long-nosync-4w | 2.98 / 26.22 / 21.51 | 4.22 / 32.79 / 79.12 |
| long-durable-1w | 5203.32 / 6446.37 / 9907.10 | 4909.55 / 5517.78 / 10129.48 |
| long-durable-4w | 3742.90 / 3746.34 / 6260.15 | 3792.48 / 3634.73 / 7040.06 |

Four-writer NoSync state stream ranges are disjoint from both references:
2.23–3.40 ms versus packed 23.10–28.98 ms and Pouch 20.18–50.62 ms.
The median per-run maximum operation is 0.325 ms versus 14.44 ms for each
reference; p99 alone would hide the packed prototype's checkpoint stall.
A single stream uses six grants and 608 journal bytes for all 2,048 records;
four streams use 20 grants and 2,080 journal bytes. Fresh NoSync streams issue
no write-phase syncs, while the packed prototype issues three structural barriers.

All four long durable stream/Pouch write-time ranges are disjoint. A lone
durable state writer uses 2,049 data syncs including head creation, versus
2,048 for Pouch. Four state writers use a median 1,003 versus 2,048. Tail
latency is still noisy: the largest observed single-writer durable stream state
operation is 224 ms versus Pouch's 54 ms in that profile. A lower total time
or p99 does not establish a bound on the worst operation.

Verified long warm state reads take about 1.4–2.1 ms versus Pouch 13.7–20.7 ms.
The four-writer NoSync close/open/first-read cycle is 32.82 ms versus Pouch
74.78 ms. The packed prototype's corresponding cycle is faster, at 12.07 ms;
canonical tail recovery and rejected-suffix stabilization remain real costs.

### Writing after reopen

The initial unbatched stream implementation exposed a material regression.
Four-writer NoSync state after reopening took a median 138.46 ms, with a
98.53–151.58 ms range, and performed 20 separate preparation syncs. Kernel
write accounting was 7,991,296 bytes. This prompted private preparation batches,
shared readiness barriers and the native zero-range operation, with bounded
writes retained as its fallback.

The corrected comparison reopens each empty store before writing, then repeats
the long workload across all six variants and three samples (72 runs). The
pre-write close/open cost is reported separately as `preopen_ms` for every
engine. Median write-phase milliseconds:

| Profile | State, stream / packed / Pouch ms | Collection, stream / packed / Pouch ms |
| --- | ---: | ---: |
| long-nosync-1w | 30.62 / 19.86 / 52.53 | 37.59 / 23.76 / 143.96 |
| long-nosync-4w | 19.19 / 30.38 / 21.35 | 13.28 / 30.09 / 77.11 |
| long-durable-1w | 4527.27 / 5014.84 / 9718.41 | 5153.06 / 7067.01 / 9951.51 |
| long-durable-4w | 3891.64 / 3806.71 / 6913.02 | 3699.47 / 3276.16 / 6950.86 |

Four-writer NoSync state now takes 14.80–20.71 ms, with 2–3 preparation syncs
and median kernel writes of 2,252,800 bytes. Pouch takes 19.57–24.54 ms, so
their ranges overlap. Stream collections take 12.41–14.13 ms and two syncs,
versus Pouch 60.76–78.94 ms. State's median maximum operation falls from
35.36 ms before batching to 9.91 ms afterward. Preparation remains a visible
cost relative to the fresh state median of 2.98 ms; reopened single-writer
NoSync streams also remain slower than the packed prototype.

Across all 432 corrected runs, payloads verify before and after reopen, record
counts match, each container is exactly one store file with reserved backing,
and every stream write phase has zero metadata checkpoints. The 240 fresh and
120 long runs exercise unchanged fixed-capacity paths; the subsequent growth
certificate correction is covered separately by the final verification and
the reopened matrix. Structural LSNs and runtime readiness now differ, so
`max_sync_batch` counts readiness units, including grants/preparation and prior
NoSync work, rather than a comparable number of application commits.

Raw corrected logs are in `build/pouch-space-integrated/streams-batched-measure/`,
`streams-batched-long/` and `streams-batched-reopen/`. Earlier unbatched logs
remain in `streams-measure/`, `streams-long/` and `streams-reopen/`.
`streams-results.json` retains all 864 raw rows and their median/min/max
summaries. The unbatched executable is retained as
`pouch_space_compare-streams-unbatched`. No source or artifact of the normal
Pouch backend was changed by this stream stage.

## Stream lifecycle follow-up

Native x86_64 verification on 2026-11-14 adds immutable sealing and atomic
whole-stream retirement. These are internal physical ownership operations;
at this stage typed name catalogs and public integration were outstanding.
Normal Pouch remains multifile.

Seal records the final tail in one 80-byte structural event. It first stabilizes
unsynced canonical bytes; an already durable tail skips that prerequisite.
The marker itself always waits for durability. Sealed streams reopen with
only owned-header validation, without scanning bodies or repairing unused
suffix capacity. Retirement uses one 64-byte head-removal event for the complete
run set, while existing readers retain their immutable pins. Reuse still waits
for durable checkpoint retirement and pin release. Rebinding uses a fresh
incarnation and rejects stale references. These operations are idempotent and
reject an active appender. Journal rollover retains its existing checkpoint cost.

All 32 experiment tests pass under Debug ASan/UBSan and native glibc/musl
Release; eight storage groups pass under ThreadSanitizer. Lifecycle Memcheck
reports 22,378 observable checks, zero errors and every heap allocation freed.
Deterministic tests cover each transition and checkpoint I/O/heap failure,
persisted torn events, valid seal-marker persistence ahead of its failed final
sync, both durability modes, held barriers with independent ready producers,
reader pins across retirement/rebinding, zero sealed-body scanning, empty
streams and full-capacity reclamation. Checksummed invalid flags, tails, run
counts and identities are also covered.

The full-journal rebinding regression reproduced a use-after-free before its
fix: automatic checkpointing could collect the old tombstone before the
replacement head used it. The implementation now refreshes that lookup after
checkpoint completion. Retirement of over 100 grants fits one 64-byte
allocation/event when no journal rollover is needed; no allocation per grant
occurs in the retirement operation.

A 90-run paired check rotates the preserved `46d07ad` executable, the lifecycle
implementation and current Pouch across five samples. The same unsealed append
fixtures verify payloads before/after reopen and audit reserved backing and
single-file artifacts. Median write milliseconds are **before / lifecycle /
Pouch**:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-durable-1w | 623.78 / 614.47 / 1309.55 | 294.72 / 314.32 / 611.87 |
| long-nosync-4w | 2.65 / 2.16 / 20.87 | 3.88 / 3.57 / 66.19 |
| reopened-long-nosync-4w | 21.74 / 25.03 / 20.77 | 26.74 / 26.64 / 77.04 |

Before/after ranges overlap in every cell; these samples establish no append
regression or improvement from the lifecycle change. Small single-writer durable
state spans 587–716 ms before and 605–718 ms afterward, versus Pouch 1180–1599 ms.
Fresh four-writer NoSync state spans 1.93–3.29 ms before and 1.84–3.72 ms afterward,
versus Pouch 17.69–22.12 ms.

Reopened state remains variable and has a worse median than Pouch in this run:
12.09–42.25 ms afterward versus 12.94–26.76 ms before and Pouch 15.33–22.49 ms.
The existing preparation cost and device/scheduling noise remain visible; these
results do not establish a general reopened-state win. Public semantic parity,
cold-cache reads, concurrent public scans, compaction and shared-root acceptance
are still required by the full implementation goal.

Raw logs and the paired runner are retained under
`build/pouch-space-integrated/lifecycle-paired/` and `lifecycle-paired.sh`;
`lifecycle-results.json` retains all rows and median/min/max summaries.
Verification logs use the `lifecycle-*` prefix in the same directory. This
comparison measures ordinary unsealed appends; transition barrier/allocation
costs above are proven separately by deterministic tests, not presented as a
public Pouch deletion benchmark.

## Typed catalog follow-up

The 2026-11-14 catalog stage adds exact-byte name bindings to the experimental
container, including conditional create/replace/remove, stable binding IDs,
revision/container checks, stream target retention, pinned views and metadata-only
enumeration. Binding records use 104 bytes plus name bytes in the existing
journal/checkpoint pages; they add no payload copy or second WAL. Their dynamic
hash index follows actual metadata participation. Catalog publication waits for
durability even in NoSync mode. Same-name concurrent mutation returns busy;
independent names may share a barrier.

Retirement rejects live bindings, held catalog views and releases not yet
durable. Recovery rebuilds the name index and target counts and rejects stale
targets, conflicting identities and attempts to retire bound heads. Snapshot
callbacks run outside the allocator mutex. Orphan enumeration discovers unbound
closed heads without reading payloads; candidates must be revalidated before
retirement and are never collected implicitly. These capabilities support the
pending public adapter; they do not implement public namespace/collection
behavior by themselves.

All 33 experiment tests pass under Debug ASan/UBSan and native glibc/musl
Release. Nine storage groups pass under ThreadSanitizer. Catalog Memcheck
reports 45,171 observable checks, zero errors and all 68,631 heap allocations
freed. Tests cover 2,000 names across owners, binary and empty names, spanning
metadata, the maximum 130,968-byte name, full-journal rollover/rebinding, pinned
old revisions, exact conditional failures, callback mutation/checkpointing,
orphan rescue, malformed checksummed events and both durability modes. Fault
injection covers publication/removal/checkpoint writes, syncs, allocations and
recovery reads, including valid metadata persisted ahead of a failed sync.

One deterministic race test reproduced a heap overflow before its fix: sync
completion can advance the durable watermark while the allocator mutex is held.
A two-pass snapshot that reloads that watermark can capture more entries than
it counted. Both binding and orphan snapshots now save a single prefix for
both passes. Pausing the vector allocation while an unrelated publication or
release completes proves that the captured set stays consistent. The pre-fix
sanitizer trace is retained in `catalog-prefix-repro.log`.

The stream comparison now creates and reopens actual catalog bindings. State
creates its namespace binding during timed warmup; collection creation and its
binding precede timed append, matching the existing Pouch fixture. Reopen
resolves and pins the catalog target before admitting the stream. Recovered
entry counts include heads, bindings and every owned-run grant. The preserved
`c659990` executable supplies the catalog-free stream baseline. The normal
multifile Pouch implementation is unchanged.

Another 90-run paired series rotates those three engines across five samples,
both workloads and three profiles. Small durable uses one writer and 128 rows
(state has two rounds); long NoSync uses four writers and 2,048 total operations.
Values are 1 KiB with 4 KiB producer chunks and a genuinely reserved 10 MiB
container. The reopened profile closes/opens an empty store before writing,
exercising uncertified free-space preparation. All 90 runs verify payloads
before and after reopen, exact physical counts and single-file backing.
Median write milliseconds are **before / catalog / Pouch**:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-durable-1w | 459.21 / 425.55 / 1061.29 | 239.50 / 250.93 / 629.39 |
| long-nosync-4w | 3.44 / 8.50 / 19.07 | 3.81 / 3.73 / 62.95 |
| reopened-long-nosync-4w | 18.53 / 23.65 / 21.35 | 14.24 / 20.57 / 55.19 |

Fresh NoSync state now includes two grouped catalog syncs in timed namespace
startup, versus zero in the catalog-free fixture: 6.10–9.63 ms afterward versus
2.92–3.87 ms before. This is a measured startup cost, not a steady-state
per-record barrier. Its median CPU time is 8.46 ms versus Pouch 26.92 ms; the
median per-run worst operation rises from 0.35 to 6.07 ms. Do not weaken
structural durability to remove this cost. A later real adapter may combine
related ready metadata with its canonical initialization under the same commit
protocol, but that optimization is not implemented or measured here.

Small durable state uses 258 timed `fdatasync` calls versus 257 before; its
before/after time ranges overlap (394–485 ms versus 413–466 ms), as do collection
ranges (222–253 ms versus 213–268 ms). Collection binding setup is outside timed
append, whose 128 per-record barriers are unchanged. Fresh long NoSync
collection ranges also overlap. Reopened state still has a worse median than
Pouch and overlapping ranges: 13.58–27.80 ms with catalogs versus Pouch
18.44–23.06 ms. Reopened collection ranges overlap before/after (10.89–22.39 ms
versus 17.59–24.22 ms); preparation used two or three barriers before and three
afterward. State adds two catalog barriers to its two or three preparations.
This series establishes no general reopened-state win.

Warm verified read medians, **catalog / Pouch**, are:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| small-durable-1w | 0.21 / 2.10 | 0.23 / 0.65 |
| long-nosync-4w | 1.52 / 16.62 | 2.94 / 8.40 |
| reopened-long-nosync-4w | 1.52 / 14.72 | 3.07 / 6.73 |

The long fresh NoSync close/open/first-verified-read cycle is 31.27 ms for state
and 29.29 ms for collection-shaped streams, versus Pouch 59.60 and 113.44 ms.
Opening alone remains slower for the container; the cycle includes Pouch's
deferred recovery and close work. Kernel timed-write accounting for fresh long
state is 2,224,128 bytes versus 2,215,936 before and Pouch 3,244,032; collection
is 2,232,320 versus 2,228,224 and Pouch 4,800,512. The container stays one file,
10 MiB logical, fully allocated; names add 444/464 journal bytes in these
four-writer fixtures without checkpointing.

The fixtures still omit public SHA-256/UUID identities, leases, receipts,
permanent positions, retry/XA authority, indexes and codecs. Warm physical reads
are not cold-cache reads or concurrent public scans. These results are evidence
for a storage boundary, not full backend performance acceptance.

Raw logs and runner are retained in
`build/pouch-space-integrated/catalog-paired/` and `catalog-paired.sh`.
`catalog-results.json` contains every row and median/min/max summaries.
Final verification logs are `catalog-final-{debug,release,musl,tsan}-tests.log`
and `catalog-memcheck.log` in the same parent directory. No Darwin, QEMU,
250 MiB or larger tests were run.

## Production collection record follow-up

On 2026-11-14 the container experiment starts using the production collection
record writer and bounded decoder through a shared, borrowed record I/O boundary.
Normal Pouch uses that same boundary with its existing file owners. Canonical
prefix/finalization and vectored writes are shared too. Public headers and the
canonical wire format are unchanged; normal Pouch roots are still multifile.

Collection-shaped stream writes now include real JSON validation, SHA-256,
collection XID/context, timestamps, metadata reservation, stored CRC/span facts
and pending-header finalization. The comparison no longer substitutes 16-byte
metadata for these collection records. Its state-shaped stream fixture still
does. The benchmark supplies collection positions/generations and a memory-only
codec context; public collection ownership, receipts, leases and index authority
are not integrated by this change. No second root, payload spool or document WAL
is introduced.

The bridge writes through one private append, bounds patches to that append and
commits separately from canonical header finalization. Reads borrow a pinned
window and preserve its sequential cursor across exact offset reads. Recovery
has a separate candidate-tail input capability. The existing stream stabilization
protocol handles failed/discarded suffixes. Native file operations retain their
original scalar `pread`/`write` calls for a single buffer and bounded vectors for
multiple parts; scalar dispatch avoids entering the vector stack workspace.

Verification on the final executable source includes:

- The selected Debug unit gate: 151 registered jobs, including 87 reusable
  passes from the same final code and 64 passing remaining jobs. All 34
  experiment tests and existing public Pouch behavior cases are included.
- All 110 selected native musl Release jobs, including default-stack workers,
  and 65 affected glibc Release jobs passed. Optimized builds use the project's
  existing portability case selections.
- All ten storage groups passed under ThreadSanitizer.
- Record integration Memcheck: 165,301 observable checks, zero errors, all
  12,192 heap allocations freed. The updated comparison is also clean, with
  all 6,934 allocations freed.

The new integration test runs a real producer through the production JSON/hash/
codec pipeline into container extents before EOF. It covers plain, zlib,
encrypted and combined transforms in both durability modes; verifies decoded
bytes, SHA-256 and retention facts; rebinds reusable readers; checks fixed read
windows after append/retirement; and recovers across checkpoint/reopen. Every
canonical append/patch/tail callback is failed before and after its operation.
Those fixtures exceed the private buffer, so finalization/cleanup also touches
physical grants. Failed producers and valid final-header writes followed by
reported failure leave the old committed prefix readable. Native transport tests
exercise scalar/vector short I/O, EINTR, zero/error returns, immutable caller
vectors and invalid ranges, using the actual glibc/musl syscall symbols.

The final gate exposed cumulative timeouts in the existing compression suite.
Its original 30-case sequence passes untraced in 149.31 seconds with 2.01 seconds
of CPU time. Compression scenarios now run one case per invocation of the same
test executable, retaining the 60-second limit. Configuration readback proves
that the same 54 Debug cases and 12 optimized cases run exactly once; none are
dropped. This changes registered job counts, not behavioral coverage. The
explicit case-selection contract test passes as well.

A final 120-run paired series rotates four engines across five samples:
preserved `02b7fc3` streams, current streams, untouched multifile Pouch from that
executable, and current multifile Pouch after the I/O refactor. This controls for
cost added to the baseline itself. Profiles retain 1 KiB values, 4 KiB producer
chunks and a genuinely reserved 10 MiB container. Small durable uses one writer
and 128 rows; state writes two rounds. Long NoSync uses four writers and 2,048
total operations. The reopened profile opens an empty store again before
writing; it exercises uncertified free space, not a cold page cache. All 120
runs verify payloads before/after reopen, recovered counts and one-file backing.
Median write milliseconds are:

| Workload/profile | Earlier stream | Current stream | Untouched Pouch | Current Pouch |
| --- | ---: | ---: | ---: | ---: |
| State, small durable, 1 writer | 551.84 | 653.39 | 1156.01 | 1158.78 |
| State, long NoSync, 4 writers | 8.01 | 7.49 | 28.12 | 24.81 |
| State, reopened long NoSync, 4 writers | 23.61 | 18.58 | 27.99 | 26.18 |
| Collection, small durable, 1 writer | 234.20 | 345.13 | 529.29 | 536.35 |
| Collection, long NoSync, 4 writers | 6.17 | 11.61 | 69.99 | 79.95 |
| Collection, reopened long NoSync, 4 writers | 16.98 | 29.08 | 87.91 | 77.83 |

Real collection encoding has a measurable cost. Fresh NoSync spans 9.46–13.81 ms
versus the earlier minimal fixture's 5.41–7.84 ms. Its median CPU time rises from
20.42 to 38.19 ms, still below untouched Pouch's 97.72 ms. Timed kernel write
accounting rises from 2,232,320 to 3,280,896 bytes, versus Pouch 4,800,512. The
same 20 grants and 2,544 journal bytes cover the fresh workload: records do not
acquire per-record container journal entries or syncs. Small durable collection
kernel writes rise from 675,840 to 741,376 bytes versus Pouch 815,104; its 128
per-record barriers are unchanged. Collection creation/binding remains outside
timed append on both sides.

State-shaped append ranges overlap before/after in all three profiles, with the
same bytes/barrier counts except preparation grouping. Small durable spans
512–729 ms before and 468–754 ms afterward. Reopened state uses four or five
barriers, including catalog startup. Its lower median than Pouch in this series
does not overturn earlier mixed results or establish a general reopened-state
win. Full state metadata/semantics are still missing from this stream fixture.

The file adapter is not proven free of performance cost. All multifile Pouch
write ranges overlap before/after, but fresh NoSync collection median CPU time
is 110.87 ms versus untouched Pouch 97.72 ms; reopened is 111.92 versus 103.21 ms.
A focused 30-run original/vector/scalar check likewise leaves roughly 10–14%
higher scalar-adapter CPU medians, with overlapping ranges. Scalar dispatch
preserves the old syscalls and removes the vector stack frame, but does not
establish elimination of that overhead. Retain the untouched executable as the
performance reference during root integration; no zero-cost abstraction claim
or production performance acceptance follows from these measurements.

Current warm verified read medians, **stream / untouched Pouch**, are:

| Profile | State, ms | Collection-shaped workload, ms |
| --- | ---: | ---: |
| Small durable, 1 writer | 0.31 / 4.00 | 0.39 / 0.85 |
| Long NoSync, 4 writers | 2.30 / 27.53 | 4.81 / 10.17 |
| Reopened long NoSync, 4 writers | 2.25 / 29.63 | 4.69 / 9.88 |

The fresh long collection close/open/first-verified-read cycle is 39.10 ms versus
untouched Pouch 132.02 ms; reopened is 35.80 versus 123.51 ms. The stream fixture
still lacks public ownership and index/receipt work. Reads are cached physical
spans versus Pouch's public operations, not concurrent public scans or cold-cache
acceptance. Full root ownership, ordinary states/objects/attachments, logical
position and retry authority, indexes, XA/outbox, maintenance and shared-root
integration remain required by the active goal.

Raw final logs are in `build/pouch-space-integrated/record-io-scalar-paired/`;
`record-io-paired.sh` reproduces that series and `record-io-final-results.json`
retains every row and median/min/max summaries. The first 120-run vector-only
series is retained in `record-io-paired/` and `record-io-vector-results.json`.
Focused ablation data is in `record-io-scalar-final-ablation/` and its results
JSON. Debug proof combines 87 passes from
`record-io-scalar-final-debug-tests.log` (which also retains the pre-split timeout)
with 64 passes in `record-io-single-case-debug-tests.log`. Other final passing
verification logs use `record-io-scalar-final-*` and
`record-io-case-selection-tests.log`.
No Darwin, QEMU, 250 MiB or larger measurements were run.

## Canonical control follow-up

On 2026-11-14 collection heads gain a shared production codec and a container
control adapter. The 384-byte canonical bytes retain lease fencing, retention,
snapshot groups and dropped state. Normal Pouch's existing atomic file owner uses
the same codec; public headers and format markers are unchanged. Container heads
occupy packed immutable cells with always-durable replacement/deletion and
completed-prefix reads, even in NoSync roots. Caller-owned metadata authority and
durable canonical dependencies remain prerequisites. Root/namespace ownership,
public collection admission and dependency bindings are still pending.

Control values are bounded to 4 KiB. Existing replacements can use one page of
the protected maintenance allowance, while new controls retain foreground
limits. Checkpoint keeps the metadata shadow allowance and can reclaim obsolete
controls after root retirement and pins drain. Tests exhaust foreground capacity,
force a new head page, reject a new control and repeat replacement/checkpoint
without growth. No writer queue, background sync timer, sidecar or payload WAL
is introduced.

Verification on the executable source passed:

- 124 selected Debug jobs and 82 native musl Release jobs, including production
  Pouch/collection, compression, XA, retention, outbox and Lua behavior.
- 68 affected GNU Release jobs and all eleven ThreadSanitizer storage groups.
- Over 11,900 control assertions: fixed wire bytes, every single-bit CRC
  mutation, checksummed semantic corruption, atomic failed decode, exact/stale
  references, pinned replacement, replay/delete, a 500-head set, capacity,
  every creation/replacement/deletion/checkpoint write or barrier failure,
  volatile/persisted torn writes with crash restoration, and read I/O failure.
- Held creation/replacement/deletion barriers in both root modes, independent
  NoSync publication during a held control sync, and four independent control
  writers sharing two barriers after all four publications reach the journal.
- Memcheck: zero errors/leaks, all 27,707 control-test allocations freed. Both
  component comparison engines also pass Memcheck. Formatting and diff checks
  pass. Allocation failures in the reused physical primitives remain covered by
  the existing foundation suites.

Five alternating paired samples compare 128 durable head replacements and 1,024
warm complete-head reloads. The native side calls the actual atomic file head
owner; the container uses its typed slot in a 10 MiB file. Both use the production
codec under caller metadata authority. The reload phase bypasses the native
inode cache on every call; these are complete control reloads, not public reads.

| Component | Container median (range), ms | Atomic file head median (range), ms |
| --- | ---: | ---: |
| 128 replacements | 256.31 (195.89–284.24) | 815.73 (742.48–942.49) |
| Replacement CPU | 9.51 (7.98–11.11) | 25.36 (20.59–26.01) |
| 1,024 complete reloads | 1.74 (1.70–2.07) | 4.15 (3.53–5.51) |

Every container sample uses 128 write barriers; every file sample uses 256.
This measures bounded head persistence, not complete root/namespace integration.

A further 150 verified runs repeat the three earlier state/collection profiles
with five samples, alternating engine order. References include the immediate
previous stage (`ccb242d`) for both streams and actual multifile Pouch, plus the
untouched multifile implementation (`02b7fc3`). Row counts, 1 KiB values, writer
counts, 4 KiB chunks and reopened-empty-store procedure match the previous
follow-up. Median write times are:

| Profile | Previous stream, ms | Current stream, ms | Untouched Pouch, ms | Current Pouch, ms |
| --- | ---: | ---: | ---: | ---: |
| State, small durable, one writer | 575.04 | 562.63 | 977.86 | 1,075.66 |
| Collection, small durable, one writer | 337.13 | 260.05 | 577.80 | 609.91 |
| State, fresh NoSync, four writers | 7.49 | 8.73 | 22.50 | 25.07 |
| Collection, fresh NoSync, four writers | 12.33 | 11.03 | 75.64 | 80.79 |
| State, reopened NoSync, four writers | 17.58 | 20.51 | 23.37 | 22.40 |
| Collection, reopened NoSync, four writers | 28.13 | 27.99 | 82.99 | 80.35 |

Every current/previous stream write range overlaps. For example, fresh collection
is 10.61–14.26 ms versus 9.13–12.97 ms; reopened state is 16.78–26.57 versus
14.96–24.84 ms. This series demonstrates no new stream speed change. Journal
usage and grant counts match the prior stage for every profile. Durable/fresh
kernel write counts match exactly; reopened counts have overlapping ranges.
Every stream store remains one file, without persistent directories.

Current warm stream versus untouched public Pouch read medians are 2.49/27.68 ms
for fresh state and 6.93/9.35 ms for fresh collection; reopened results are
2.81/34.65 and 4.96/10.54 ms. These remain physical cached spans versus public
operations, without full index/receipt authority, cold reads or concurrent scans.
The earlier native file-adapter CPU concern is unresolved: fresh/reopened actual
Pouch collection CPU medians are 111.97/124.88 ms versus untouched
110.19/105.90 ms, with overlapping sample ranges. This change does not establish
that the adapter overhead has disappeared.

Raw samples are in `build/pouch-space-integrated/control-paired/`;
`control-results.json` retains all 160 rows and median/min/max summaries.
`control-paired.sh` reproduces the local archived-reference series. Exact ELF,
results and runner checksums are in `control-artifacts.sha256`; current ELF
archives are `pouch_space_compare-controls-current` and
`pouch_space_control_compare-current`. Final verification logs use
`control-final-{debug,musl,release,tsan}-tests.log`,
`control-final-memcheck.log` and `control-compare-*-memcheck.log`.

The component comparison can be repeated directly after a native Release build:

```sh
build/x86_64-linux-gnu-release/experiments/pouch_space/pouch_space_control_compare space "$PWD/build/pouch-space-integrated" 128
build/x86_64-linux-gnu-release/experiments/pouch_space/pouch_space_control_compare file "$PWD/build/pouch-space-integrated" 128
```

No Darwin, QEMU or 250 MiB-and-larger tests were run. The full implementation goal
remains active; this stage completes physical control persistence and shared
encoding. Normal public Pouch roots remain multifile.

## Container root and namespace follow-up

On 2026-11-14 the stream experiment gains a root owner for the single container,
production crypto provider and namespace identities. Its 128-byte root control
binds encryption mode/key ID, compression and canonical head/record widths.
Creation is durable; reopen rejects missing/corrupt policy or a wrong key/mode
before admitting namespaces. Implicit compression inherits stored policy,
including allocating the plaintext transform provider when needed. Explicit
compression changes currently return unsupported: resumable container conversion
remains part of the active goal.

Namespace identities are now self-contained catalog metadata. They have a
stable owner ID/revision, no stream target and no payload extents. Creation needs
one durable publication. They cannot be replaced by stream bindings; deletion
and recreation produce a fresh ID. Catalog views pin the metadata across
checkpoint/recreation. Existing stream bindings retain target/retirement guards.
The root registry caches one owner per exact nonempty C-string name, grows with
metadata participation, and does not retain failed misses. Same-name callers
wait for initialization; unrelated names publish outside the registry mutex.

Namespace pins, active calls and physical child handles reject root close while
leaving the owner usable. Enumeration permits reentrant acquisition/creation
outside locks. Cached namespace admission now checks the physical poison state;
an uncertain write cannot be bypassed by a cache hit. This check performs no
allocation or I/O on success.

The stream comparison now creates/reopens this root, uses its real namespace
IDs and scopes child catalogs accordingly. Its record-codec context borrows the
root's production crypto provider. Normal public `lc_pouch_open` still uses the
multifile backend. Ordinary namespace data/manifests/replay, public collection
ownership, positions, receipts, indexes, maintenance and shared-root coordination
remain pending. Namespace identity metadata is not a complete namespace store.

Final-source verification passed all 40 affected storage jobs on Debug, native
GNU Release and native musl Release, plus all fourteen ThreadSanitizer storage
groups. Production sources/public headers are unchanged in this stage, so the
preceding full Pouch/collection/outbox/XA/compression/Lua gates are reused.
New proof covers:

- Four codec combinations through 1 MiB-plus bounded streaming before EOF,
  read/reopen, policy inheritance/mismatch, and retained child/root lifetimes.
- Missing root initialization, each control byte's CRC corruption, correctly
  checksummed invalid fields/lengths, invalid namespace bindings and names.
- 500 namespaces, the maximum catalog name, registry growth, immutable identity,
  stale references after recreation, pinned views across checkpoint and
  reentrant enumeration with a fixed captured prefix.
- Every creation/reopen/namespace publication write, barrier and read failure;
  volatile and selectively persisted torn writes with durable-image restoration;
  every framework allocation failure in plain/encrypted open, compression
  inheritance, namespace initialization, registry growth and enumeration.
- Independent/same-name initialization in both modes, held barriers, a failed
  initializer followed by waiting callers' successful retry, and uncertain sync
  failure/reopen. Complete unacknowledged metadata may recover after uncertain
  I/O; incomplete namespace state is never admitted. A killed owning child
  releases exclusive file ownership and its durable namespace reopens.

Memcheck reports zero errors/leaks and frees all 131,173 allocations across the
three root groups. Both stream comparison paths also pass Memcheck. A regression
first demonstrated cached admission after a poisoned write and then proved the
fix. Formatting, configuration builds and diff checks pass.

An initial root design represented namespace identity with an empty sealed
stream. Its first 150 matched runs exposed fresh four-writer state startup cost:
17.18 ms versus pre-root streams 7.84 ms and untouched Pouch 25.14 ms. The final
design removes that empty stream and its publication barrier. That implementation
is absent from current source; its exact executable and samples remain archived
for comparison.

The final series contains 180 verified runs: five alternating samples of six
engines across the same three state/collection profiles. Engines include the
previous stage (`1ccf226`) for streams and actual Pouch, the archived empty-stream
root design, the final root, and untouched multifile Pouch (`02b7fc3`). Parameters
remain 10 MiB reservation, 1 KiB values, 4 KiB chunks; small durable uses one writer
and 128 rows (state has two rounds), long NoSync uses four writers with 1,024
two-round state rows or 2,048 collection entries. Reopened means writing after
closing/reopening an empty store, not dropping the OS cache.

| Write profile | Pre-root streams, ms | Empty-stream root, ms | Final root, ms | Untouched Pouch, ms | Current Pouch, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| State, small durable | 599.47 | 629.22 | 585.30 | 1,097.80 | 1,102.63 |
| Collection, small durable | 288.40 | 352.55 | 323.12 | 584.98 | 602.00 |
| State, fresh NoSync | 8.15 | 16.90 | 11.84 | 26.19 | 24.10 |
| Collection, fresh NoSync | 11.72 | 8.94 | 10.21 | 70.62 | 78.91 |
| State, reopened NoSync | 19.04 | 29.66 | 27.76 | 26.56 | 26.05 |
| Collection, reopened NoSync | 29.38 | 31.63 | 30.80 | 72.51 | 79.89 |

Fresh state final/empty-stream ranges are 7.50–15.04 / 6.98–18.41 ms. The median
improves, but ranges overlap. The structural change is deterministic: namespace
birth needs one record/barrier, and that profile uses four write barriers versus
six or seven with empty streams. Its journal is 3,052 versus 3,692 bytes, with the
same 20 data grants. A root control and real namespace creation still add fixed
cost versus the earlier partial stream fixture; this is not a free integration.

Collection creation is outside the write interval on all sides. Its fresh final
root creation median is 23.54 ms versus pre-root 26.97 ms and untouched Pouch
141.08 ms, with overlapping pre-root/final ranges. Fresh state root creation
(before lazy namespace creation) is 17.15 versus pre-root 11.98 ms and untouched
Pouch 19.02 ms. These fixed costs must be retained in deployment comparisons.
Reopened state write ranges overlap untouched Pouch, so this series shows no
reliable write advantage there. All collection write ranges
also overlap their pre-root/empty-stream counterparts; no further body-codec
optimization is established.

| Warm read profile | Final root median, ms | Untouched public Pouch median, ms |
| --- | ---: | ---: |
| State, small | 0.34 | 4.39 |
| Collection, small | 0.32 | 0.77 |
| State, fresh | 2.64 | 28.87 |
| Collection, fresh | 5.15 | 9.95 |
| State, reopened | 2.33 | 31.67 |
| Collection, reopened | 5.01 | 11.12 |

These remain cached physical stream reads versus public Pouch operations, without
full namespace replay/index/receipt authority, cold reads or concurrent public
scans. Collection close/open/first-verified-read medians are 37.20 ms fresh and
29.77 ms reopened versus untouched 115.10/122.24 ms. Every final stream store
remains exactly one file with no persistent directories or sidecars. The earlier
native file-adapter CPU concern is unresolved: current/untouched actual Pouch
collection CPU medians are 104.65/95.50 ms fresh and 116.45/105.57 ms reopened,
with overlapping ranges. This stage changes no production Pouch implementation.

Final raw samples are in `build/pouch-space-integrated/root-paired/`; all 180
selected numeric rows and median/min/max summaries are in `root-results.json`.
`root-paired.sh` repeats the local archived-reference series. The initial 150
empty-stream samples are in `root-anchors-paired/` and `root-anchor-results.json`,
with their archived executable/runner. `root-artifacts.sha256` binds the current,
empty-stream, previous-stage and untouched executables, results and runners.
Final test logs use `root-direct-final-{debug,musl,release,tsan}-tests.log`;
Memcheck logs use `root-direct-*-memcheck.log`. The cached-poison falsifier and
repair are retained in `root-poison-{before,after}-tests.log`.

The full implementation goal remains active. No Darwin, QEMU or 250 MiB-and-larger
tests were run. Existing-root conversion remains excluded; production cutover
requires its separate decision after full integration and acceptance.

## Reproduce

The current `streams` modes include the production collection codec described in
the follow-up above; earlier tables identify the preserved implementation they
measured.

```sh
bash scripts/compare_pouch_space.sh smoke
bash scripts/compare_pouch_space.sh measure "$PWD/build/pouch-space-comparison"
bash scripts/compare_pouch_space.sh rollover "$PWD/build/pouch-space-rollover"
bash scripts/compare_pouch_space.sh streams "$PWD/build/pouch-space-streams"
bash scripts/compare_pouch_space.sh streams-long "$PWD/build/pouch-space-streams-long"
POUCH_SPACE_RUNS=3 bash scripts/compare_pouch_space.sh streams-long-reopen \
  "$PWD/build/pouch-space-streams-reopened"
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
`pouch_space_test pouch_space_directory_test pouch_space_compare`, then run
CTest with `-R '^pouch_space_'`.
Valgrind output is retained in `build/pouch-space-comparison/valgrind.log`.

## Canonical ordinary state records

The ordinary stream comparison now writes the production state record format:
versions, timestamps, SHA-256 etags, payload references/CRC, content type,
descriptor reservation, payload context, side metadata and index trailer. The
previous 16-byte metadata fixture is removed from the current implementation.
Both native segmented replay and container replay now use the same bounded
reader and existing production metadata decoders. A borrowed stdio adapter keeps
the native owner's readahead; container access remains offset based. There is
no whole-payload staging. The codec leaves namespace policy, sequence allocation,
publication and durability with its caller.

The shared pull encoder now accepts an encoded-byte sink with an explicit
compression choice. The ordinary container writer hashes plaintext while
producing encoded bytes, reserves canonical metadata, and clears PENDING only
after the final metadata and result allocations succeed. Errors discard the
owner's private append. State/object PUT output supports plain, compressed,
encrypted and combined records. The shared decoder also retains the existing
metadata, delete, decision and high-water dispatch used by native Pouch.

Tests cover empty, one-byte and 1 MiB-plus payloads, staged and deadline retention,
compression override, exact plaintext/hash/metadata recovery, trusted checkpoint
prefixes and verified candidate tails. Encoded output must flow before producer
EOF. Every captured-end cut, pending/invalid flags, wrong payload identities,
partial stdio reads/EINTR, transport errors, record I/O/allocation failures and
oversized producer returns are exercised. Failed reads preserve a previously
decoded entry; failed appends preserve earlier records. Four independent state
streams share one crypto provider in the concurrency test. That retention stress
fixture reserves 32 MiB for sixteen 1 MiB records; performance fixtures remain
at the 10 MiB default.

Verification passed the standard Debug gate (233 tests), GNU Release unit gate
(137), musl Release unit gate (138), and all seventeen ThreadSanitizer storage
checks. After the final writer-only input-contract tightening, all seven affected
state/benchmark checks passed again on Debug, GNU and musl, and all three state
record checks passed again under ThreadSanitizer. The three final state record
groups passed Memcheck with zero errors, no suppressions and all 25,982 allocations
freed. Formatting and diff checks apply to the final sources. These are native
x86_64 checks; no Darwin, QEMU or 250 MiB-and-larger execution is claimed.

The final matched series contains 150 verified cases: five samples, three
profiles, both record kinds, and five engines. Engine order reverses between
samples. The pre-stage executable is `026fd33`; the untouched multifile baseline
is `02b7fc3`. Every sample verifies values before and after reopening. All stream
cases have exactly one physical file and no store directories. Write medians
are milliseconds:

| Workload | Pre-stage stream | Complete stream | Untouched multifile | Pre-stage multifile | Current multifile |
| --- | ---: | ---: | ---: | ---: | ---: |
| Durable, one writer, state | 702.85 | 715.75 | 1207.96 | 1249.72 | 1231.89 |
| Durable, one writer, collection | 348.47 | 386.42 | 703.39 | 654.52 | 730.39 |
| Fresh NoSync, four writers, state | 13.87 | 15.42 | 32.01 | 29.26 | 30.29 |
| Fresh NoSync, four writers, collection | 13.32 | 14.33 | 92.18 | 94.39 | 92.64 |
| Preopened NoSync, four writers, state | 34.25 | 38.65 | 28.43 | 28.23 | 28.44 |
| Preopened NoSync, four writers, collection | 33.72 | 33.31 | 86.91 | 90.99 | 91.72 |

All cases use 1 KiB values and 4 KiB source chunks. Durable state writes perform
128 records twice; collection appends perform 128 records once. Long state runs
perform 1,024 records twice; long collection runs append 2,048 records. The
preopened profile closes/reopens an empty root before writing; it is neither a
cold-cache test nor writing again to an already populated store.

The final state stream ranges were 638.46–936.88 ms durable, 14.80–18.25 ms fresh
NoSync and 33.65–58.46 ms preopened NoSync. Current multifile ranges were
1,156.05–1,364.85, 29.52–33.04 and 24.01–33.03 ms respectively. The preopened
NoSync slowdown remains. Corrected syscall accounting: the stream performs seven
`fdatasync` calls at the median (range seven to eight); multifile Pouch performs
zero `fdatasync` calls **and eight `fsync` calls**. Its structural metadata still
synchronizes in NoSync mode. Fresh NoSync uses four container `fdatasync` calls
versus eight multifile `fsync` calls. Durable state uses 259 container
`fdatasync` calls versus 256 `fdatasync` plus two `fsync` calls in multifile Pouch.
Different calls cover different files and dependencies; counts alone do not
explain latency. Durable namespace metadata and safe preparation of untrusted
free extents still have costs. No writer queue or weaker commit guarantee was
introduced.

Complete metadata also makes the fresh state kernel-write counts comparable:
3,248,128 bytes for the stream versus 3,244,032 for current multifile Pouch,
instead of roughly 2.23 MB for the old stream fixture. The added format/hash
work is included in these measurements. Absolute timings shifted across both
engines between measurement rounds; use matched results and ranges, not a
cross-round causal claim.

The same long state runs measured these read medians in milliseconds. Reopen +
read includes the run's final filled-store recovery and payload verification:

| State profile | Stream warm read | Multifile warm read | Stream reopen + read | Multifile reopen + read |
| --- | ---: | ---: | ---: | ---: |
| Fresh NoSync, four writers | 3.38 | 35.38 | 47.99 | 53.61 |
| Preopened NoSync, four writers | 2.95 | 47.76 | 35.44 | 37.59 |

These remain warm physical-span reads against public Pouch reads. They do not
establish public scan/index performance or complete namespace-policy parity.
Total reopen readiness remains variable; the experiment has no general startup
advantage. Current collection warm reads measured 5.57/7.11 ms for fresh/preopened
roots, versus 10.84/10.87 ms for current multifile Pouch.

Local evidence is retained under `build/pouch-space-integrated/`: the pinned
`pouch_space_compare-state-current`, previous/root and untouched/catalog
executables, `state-paired.sh`, all 150 `state-paired/*.log` files,
`state-results.json` with samples/ranges, and `state-artifacts.sha256`. Verification
logs use the `state-*` prefix; `state-last-*` and `state-final-release-*` record
the final focused checks. Normal `lc_pouch_open` still uses multifile storage.
The full implementation goal remains active: namespace mutation/manifests,
snapshot/obsolete ownership, public collection integration, maintenance and
shared-root coordination are still required.

## Namespace segment ownership

Ordinary stream runs now use a namespace storage owner instead of the benchmark's
single `"active"` binding. The owner lazily loads its head and segment catalog,
recovers canonical records, serializes append/rotation authority within one
namespace, captures immutable read prefixes and owns its segment handles through
root close. It creates no segment/head for an untouched empty namespace.

A 128-byte head selects the active segment and rotation epoch. Segment catalog
entries bind numeric canonical labels to exact stream incarnations. Rotation
prepares and binds an empty successor, durably commits head cutover, then seals
its predecessor. An expected-active fence makes retries safe across reopen,
including a committed cutover whose seal still needs completion. Empty staged
successors can be reused; inconsistent membership is rejected. Ordinary commits
only publish their canonical tail, without another namespace head write.

Producer and replay callbacks run without namespace/registry locks. A blocked
producer leaves captured readers and another namespace able to progress. Lazy
initialization coalesces same-namespace callers while another namespace proceeds
through a held read. Cached storage admission checks poison before using its
atomic pointer. The committed tail is cached under append authority, avoiding
one duplicate physical metadata/status query per record.

Captured segments now use lightweight stream-window pins. Buffered sequential
readers share their admission/release implementation. A warm view over 89
segments makes no allocation as large as the 16 KiB body buffer that each
captured reader previously inherited. Replay instead shares one bounded
readahead buffer and optional CRC scratch across segments. Views preserve their
original prefix across append/rotation; callback cancellation leaves them
reusable. Windows remain readable after retirement/checkpoint and prevent stream
close until released. They and append/view handles retain the namespace/root.

All 47 storage checks passed on Debug, GNU Release and native musl Release;
all 21 storage checks passed under ThreadSanitizer. After final replay/tail-cache
work, the eight affected namespace/stream-state checks passed on Debug, GNU and
musl, and the four namespace groups passed again under ThreadSanitizer. Final
Memcheck runs for lifecycle, faults, concurrency and validation reported zero
errors, no suppressions and all 238,517 allocations freed. Musl uses the default
128 KiB worker stacks.

The fault suite fails every observed I/O/allocation point during initialization,
rotation and load/replay in both durability modes. It restores a simulated
durable image after failure, including partial writes, partial persistence on
failed barriers and publications that persisted despite an error. Established
durable records survive; a first NoSync write is allowed to be lost. Recovered
owners accept further appends and retry rotations without an extra cutover.
Validation covers all 128 head bytes, checksummed semantic corruption, wrong
length/labels/bindings, captured bounds, window allocation failure and retirement.
Cached owners cannot bypass a poisoned root. Four transform combinations,
automatic rotation, 89-segment growth/checkpoint/reopen, reentrant callbacks and
root/child lifetime are exercised.

### Matched measurements

The final series has **210 verified cases**: five samples, seven workloads and
six executables/modes, with reversed engine order between samples. Values are
1 KiB, source chunks 4 KiB and container capacity 10 MiB. The codec-only
predecessor is `375849c`; the same-stage uncached-tail executable isolates that
implementation choice. The series also retains untouched multifile `02b7fc3`,
multifile from `375849c`, and current multifile samples. No product source
changes were made to the multifile backend in this stage.

Write medians are milliseconds:

| Workload | Codec-only stream | Namespace, uncached tail | Namespace, cached tail | Current multifile |
| --- | ---: | ---: | ---: | ---: |
| Durable, one writer, state | 635.77 | 881.38 | 621.47 | 1119.68 |
| Durable, one writer, collection | 368.41 | 337.72 | 351.48 | 596.00 |
| Fresh NoSync, four writers, state | 14.90 | 17.64 | 20.16 | 28.32 |
| Fresh NoSync, four writers, collection | 12.29 | 12.52 | 12.27 | 86.61 |
| Preopened NoSync, four writers, state | 36.62 | 45.46 | 43.08 | 30.63 |
| Preopened NoSync, four writers, collection | 30.61 | 33.70 | 30.78 | 90.95 |
| Rotating NoSync, one writer, state | 27.85 | 79.33 | 68.62 | 65.09 |

Durable state runs write 128 records twice; durable collections append 128 once.
Long state runs write 1,024 records twice and long collection runs append 2,048.
The extra rotating case uses one state writer and **four actual namespace
segments**, checked by the executable's `logical_segments` metric. Its
codec-only predecessor kept one unbounded logical stream; that predecessor
does not include namespace rotation work. The preopened profile reopens an
empty root before writing, not a populated store or cold cache.

Final namespace state-write ranges were 564.83–667.67 ms durable, 14.22–21.07 ms
fresh NoSync, 37.14–46.94 ms preopened NoSync, and 63.05–74.78 ms rotating.
Current multifile ranges were 1,004.66–1,326.34, 22.98–43.07, 23.84–36.78 and
62.07–67.42 ms respectively. Preopened NoSync remains slower. Rotating write
ranges overlap: its median is slightly slower than current multifile. There is
no claim that the new namespace owner beats its earlier codec-only fixture.

The tail cache removes an unnecessary metadata query by construction. It does
not establish a uniform measured speedup: multiwriter CPU medians are essentially
flat, and durable wall-time differences are dominated by sync-time variance.
The uncached samples remain available rather than attributing the entire
durable difference to this small cache.

Both synchronization APIs must be counted. State-write median call counts:

| State profile | Codec-only fdatasync / fsync | Namespace fdatasync / fsync | Multifile fdatasync / fsync |
| --- | ---: | ---: | ---: |
| Durable, one writer | 259 / 0 | 260 / 0 | 256 / 2 |
| Fresh NoSync, four writers | 4 / 0 | 6 / 0 | 0 / 8 |
| Preopened NoSync, four writers | 7 / 0 | 9 / 0 | 0 / 8 |
| Rotating NoSync, one writer | 2 / 0 | 12 / 0 | 0 / 8 |

The earlier canonical-state paragraph above is corrected accordingly: multifile
NoSync still synchronizes structural metadata. Calls cover different dependencies
and, for multifile stores, different inodes/directories; counts are not equivalent
latency units. Summed syscall time can overlap across writers. Container
preparation, head publication and sealing retain their durability guarantees;
no global writer queue or weakened commit was introduced.

Warm state-read and final filled-store reopen + read medians, in milliseconds:

| State profile | Stream warm read | Multifile warm read | Stream reopen + read | Multifile reopen + read |
| --- | ---: | ---: | ---: | ---: |
| Durable, one writer | 0.35 | 6.27 | 10.37 | 5.67 |
| Fresh NoSync, four writers | 2.51 | 31.76 | 41.28 | 33.57 |
| Preopened NoSync, four writers | 2.50 | 36.91 | 33.89 | 32.03 |
| Rotating NoSync, one writer | 2.30 | 32.01 | 15.24 | 32.54 |

Rotating/sealing old segments moves durability work into the write interval and
allows sealed prefixes to skip candidate recovery. Its faster reopen is not
free end-to-end speed. Read figures still compare warm physical-span reads with
public Pouch reads, not full public scan/index/policy parity. Startup remains
variable and has no general container advantage. Current collection warm reads
are 5.13/4.93 ms for fresh/preopened roots versus 10.11/10.76 ms for multifile.

Local evidence is in `build/pouch-space-integrated/`: pinned
`pouch_space_compare-namespace-current` and
`pouch_space_compare-namespace-uncached-tail`, the unchanged state/catalog
baselines, `namespace-paired.sh`, all 210 `namespace-paired/*.log` files,
`namespace-results.json` (samples and ranges), and
`namespace-artifacts.sha256`. The final series log is
`namespace-final-paired-run.log`; focused final checks use `namespace-tail-*`
and final memory reports `namespace-final-*-memcheck.log`.
Exactly one container file and no store directories are verified in every
stream case. No Darwin, QEMU or 250 MiB-and-larger execution was run.

This completed namespace segment storage and bounded canonical replay in the
experimental owner. The following integration now connects ordinary production
state projection/mutation policy. Normal public `lc_pouch_open` remains
multifile; remaining public semantics, snapshot/obsolete ownership, collections,
maintenance and shared-root coordination remain part of the active goal.

## Production ordinary-state integration

The private `ps_pouch_state_open` harness now connects production ordinary-state
PUT, point read, metadata read, copy and namespace creation to the container's
namespace owner. This uses the real precondition, version, metadata, sequence,
retention, SHA-256, codec and projection rules. Timestamp, retention and sequence
publication remain after the producer. Replay rebuilds the production projection
without a manufactured namespace pathname; failed replay cannot admit a partial
projection. Encoded read windows retain their storage owner and original crypto
binding across replacement, rotation and Pouch close. The container path creates
no body cache or payload spool.

The internal capability table supplies namespace admission/info, canonical
replay, private append/commit and bounded encoded sources. Common runtime
initialization owns Pouch coordination and options. Native public
`lc_pouch_open` remains multifile; the integration harness is not installed or
selected through an SDK flag. It requires disabled indexing, background
maintenance and watches, exclusive root ownership and no generated external key
file. Other Pouch APIs are outside its current call contract. State PUT
staged/lease publication and staged decision replay explicitly return
unsupported. At this PUT/read stage, metadata-only mutation/delete, read-many/scans,
collections, queues, indexes, XA, maintenance, snapshot/obsolete ownership and
shared-root semantics still require integration.

### Verification

The native Debug preset passed all 247 checks, with ASan/UBSan and constrained
worker stacks. GNU and musl release each passed all 151 selected unit checks,
including all 56 pouch-space checks. The extended held-read lifecycle passed
again in Debug/GNU and in the full musl run. Fifteen affected
Pouch/root/namespace/state-codec groups passed ThreadSanitizer.

The new integration groups cover all four crypto/compression combinations,
both sync policies, 1 MiB streaming bodies, small caller-owned memory bodies,
empty bodies, version rejection before source consumption, failed-producer
rollback, metadata inheritance, namespace names containing slashes, rotation,
populated-root reopen and inherited compression policy. Read sources retain
their old body across replacement/rotation, reset after Pouch close, keep the
root exclusively owned until their last close, and then permit reopening.
A held producer allows a reader in its namespace and a writer in another
namespace to finish. A same-key contender fails its stale version precondition
without consuming its source.

Allocation tests inject all 790 observed positions across creation, streaming
and memory PUT, cold replay and warm reads in plaintext and encrypted/compressed
modes, under both sync policies. I/O tests inject all 58 observed write,
reservation and synchronization positions, including a written prefix followed
by failure. They verify that the anchor survives, an update is absent or fully
committed, and reopen admits another write. Producer failure preserves the
previous value. A failed publication/result allocation can have a complete
committed outcome, consistent with the existing mutation engine; an error is
not treated as an instruction to undo durable publication. Cached metadata
admission after physical poison is separately asserted in Debug, GNU, musl and
TSAN. These integration I/O tests use real native files; the foundation's
existing deterministic durable-image tests remain the power-loss model.

Memcheck passed all five new groups: 396,026 allocations and frees, zero bytes
or blocks remaining, zero errors and zero suppressions. Crypto outputs use the
logical owner's allocation domain. Tests found and fixed a root key-ID cleanup
domain mismatch and shared append reservation cleanup leaks.

Logs are under `build/pouch-space-integrated/runtime-*`: Debug is split between
`runtime-debug-tests.log` (the first 16 checks) and
`runtime-debug-remaining-tests.log` (the remaining 231 selected checks).
GNU/musl logs, focused held-read/admission checks, TSAN and the five
`runtime-*-memcheck.log` files retain their respective evidence.

### Matched production-state measurements

Native GNU release, 10 MiB initial container, 1 KiB bodies delivered through
4 KiB source callbacks, two PUT rounds, no crypto/compression, indexing or
background maintenance. Five samples per profile/engine alternate execution
order. Both integrated and current multifile modes call production
`lc_pouch_state_write` and `lc_pouch_state_read`; the older stream fixture is
not substituted for the integrated engine. Each writer owns a distinct
namespace. The rotating profile uses one namespace and four logical segments;
same-namespace contention is covered by the correctness tests.

Every run verifies the current bodies before close and after reopening the
populated root. All 20 integrated runs also audit the parent directory and
require exactly one store file and no store sidecars. The “empty-root reopen”
profile reopens an empty root before writes; populated reopen plus verification
is measured separately in every profile. OS page cache is retained. The point
read column is a hot-projection first body pass, including any multifile body
cache population; it is not a repeated body-cache hit or a public scan.

Median milliseconds, container versus current multifile:

| Workload | Container PUT | Multifile PUT | Container point read | Multifile point read |
| --- | ---: | ---: | ---: | ---: |
| Durable, 128 keys, 1 writer | 687.52 | 1233.71 | 0.52 | 3.46 |
| Fresh NoSync, 1,024 keys, 4 writers | 21.85 | 26.20 | 3.65 | 30.60 |
| Empty-root reopen, NoSync, 4 writers | 46.74 | 24.78 | 3.59 | 30.51 |
| Rotating NoSync, 1,024 keys, 1 writer | 89.62 | 68.91 | 3.68 | 29.08 |

Durable PUT is about 1.8 times faster here. Fresh NoSync PUT is faster, while
empty-root-reopened NoSync is about 1.9 times slower and rotating NoSync about
1.3 times slower. Point-read passes are roughly 7–8 times faster. Public scan
performance is still unmeasured through this integration.

The container does not yet publish Pouch's clean projection hint at close and
replays canonical records when building the next projection. Multifile pays
for that hint during close. Include that work when comparing restart:

| Workload | Close C / MF | Reopen + verify C / MF | Close + reopen + verify C / MF |
| --- | ---: | ---: | ---: |
| Durable, 128 keys, 1 writer | 0.04 / 11.88 | 11.98 / 3.87 | 12.06 / 16.69 |
| Fresh NoSync, 1,024 keys, 4 writers | 0.15 / 64.11 | 46.74 / 31.70 | 46.89 / 99.21 |
| Empty-root reopen, NoSync, 4 writers | 0.16 / 46.89 | 44.00 / 30.20 | 44.19 / 78.05 |
| Rotating NoSync, 1,024 keys, 1 writer | 0.15 / 15.27 | 19.05 / 31.74 | 19.18 / 47.53 |

The combined column is the median of per-sample sums. Container reopen alone is
slower in the first three profiles, while its close work is much smaller. Sealed
segments avoid physical candidate-tail recovery; logical projection replay
still verifies canonical records. Typed snapshot/clean-projection integration
remains necessary before full backend acceptance.

Median write CPU times C/MF are 32.50/47.30 ms (durable), 50.44/49.27 ms
(fresh four-writer), 49.56/46.20 ms (reopened four-writer), and 33.20/34.43 ms
(rotating single-writer). Similar NoSync CPU with larger wall time suggests
waiting/coordination as the next performance target, but the counters do not
isolate the critical path.

Count both synchronization APIs. During PUT, container fdatasync/fsync counts
are 260/0, 6/0, 9/0 and 12/0 respectively; multifile counts are 256/2, 0/8, 0/8
and 0/8. They cover different dependency sets. Timed synchronization sums can
overlap across workers and cannot be subtracted from wall time as a causal
breakdown. Reopened-space preparation and rotation remain the NoSync targets.

The preserved `02b7fc3` multifile reference was measured in the same run;
its PUT medians were 1243.33, 31.96, 29.46 and 67.39 ms respectively. Wall
times varied materially: rotating PUT ranged 68.25–149.42 ms for the container
and 57.04–110.00 ms for current multifile. Do not infer a shared-code speedup
from current/reference wall differences. These are local measurements at the
specified scale, not evidence for larger deployments.

The 60 complete logs, paired Bash runner, JSON metric rows/group summaries,
two preserved executables and their SHA-256 manifest are in
`build/pouch-space-integrated/runtime-paired/`,
`runtime-paired.sh`, `runtime-results.json`,
`pouch_space_compare-runtime-current`,
`pouch_space_compare-catalog-02b7fc3` and
`runtime-artifacts.sha256`. The earlier namespace artifact manifest also
passed verification before this run; those reference files were not replaced.

For a current-source comparison after the normal native GNU configuration:

```sh
cmake --build build/x86_64-linux-gnu-release --target pouch_space_compare
build/x86_64-linux-gnu-release/experiments/pouch_space/pouch_space_compare \
  runtime-state "$PWD/build/pouch-space-integrated" 1024 1024 0 4 4096 --reopen
build/x86_64-linux-gnu-release/experiments/pouch_space/pouch_space_compare \
  pouch-state "$PWD/build/pouch-space-integrated" 1024 1024 0 4 4096 --reopen
```

## Production metadata and deletion integration

The private harness now admits ordinary `lc_pouch_state_update_metadata` and
`lc_pouch_state_delete` through the same production policy engine. Metadata
batching retains its existing per-namespace worker, bounded aggregation and
canonical vectored writer. The production state engine reserves index sequences
and uses the typed appender to publish the complete canonical batch, without
rewriting bodies. Tombstones
use the same boundary. Borrowed manifests are refreshed under append authority
before rotation. Old body sources remain readable after deletion and Pouch close.

Positive expiry now publishes a conservative durable marker inside the file,
rather than attempting to create the native retention hint sidecar. Namespace
control slot 2 contains the checked 32-byte marker described in the
[foundation design](../experiments/pouch_space/DESIGN.md). It is durable before
expiry-bearing publication even under NoSync. A failed mutation may leave a
positive hint. Get reads only metadata; marker clearing and retention execution
are not implemented yet. No SDK selector or public-open cutover is introduced.

### Verification and remaining capacity gap

Debug passed all 269 selected checks under ASan/UBSan. Native GNU and musl release
each passed all 173 selected unit checks, including all 78 pouch-space checks.
Twenty-one affected integration/root/namespace/canonical-record groups passed
ThreadSanitizer. Native musl workers retain their default 128 KiB stacks.

New lifecycle cases cover all four transform combinations and both durability
policies, metadata-only placeholders, metadata inheritance, version/etag/view
preconditions, hidden/expiry fields, index sequence preservation, forced
rotation, no-op repeated deletion, recreation, pinned bodies and populated-root
reopen. Eight concurrent callers update distinct keys in one namespace, preserving
their bodies and producing 64 unique sequences under both sync policies.

Fault matrices exercise all 558 observed allocation and 173 I/O positions for
metadata over an existing body, deletion and metadata-only creation, using plain
and combined encryption/compression under both sync policies. A 20,001-byte
metadata value crosses append-buffer boundaries. I/O failures include written
prefixes. Recovery requires the old or complete new outcome, intact anchor
bytes, poison admission and a successful retry. Faults found and fixed shared
state-engine bugs: metadata-worker setup masked allocation errors as transport
errors, and tombstone sequence-allocation failure leaked encoded metadata.

Retention tests corrupt every marker byte plus short/long size, wrong owner,
flag and reserved-field cases with valid outer storage checksums. Both get and
ensure fail closed. Reading the marker of a namespace with a 1 MiB body performs
at most 16 KiB of physical reads without initializing its segment owner.
Memcheck passed all six new groups: 432,836 allocations and frees, no remaining
blocks, errors or suppressions.

The fixed 10 MiB capacity probe fills 117 keys after seeding a peer namespace.
Deletion in the filling namespace succeeds; deletion in the peer can fail when
rotation needs foreground capacity. The prior value survives failure and reopen.
This passing probe proves safety, **not full-capacity deletion admission**.
Connecting protected capacity to the complete maintenance dependency set, then
reclamation, remains an unmet requirement of the active full-backend goal.

Logs and full CTest details are under
`build/pouch-space-integrated/mutations-final-{debug,gnu,musl,tsan}-*`.
The six `mutations-*-memcheck.log` files preserve leak/error evidence. No Darwin,
QEMU or large-machine execution was run.

### Matched mutation measurements

Sixty serial GNU release runs use five alternating-order samples for each
operation/profile/engine. Both engines call the real production APIs. Ordinary
PUT seeds 1 KiB bodies before the mutation timer; `seed_ms` records that cost
separately. Metadata performs two rounds with 32-byte application metadata;
delete performs one. Indexing, transforms, background maintenance and positive
expiry are disabled. Each writer owns a distinct namespace. Every run verifies
the resulting bodies/metadata or absence before close and after populated-root
reopen. All 30 container runs require one store file without sidecars.

Median mutation milliseconds, container / current multifile:

| Workload | Metadata update | Delete |
| --- | ---: | ---: |
| Durable, 128 keys, 1 writer | 553.63 / 1308.48 | 323.18 / 769.57 |
| Hot seeded root, NoSync, 1,024 keys, 4 writers | 38.40 / 27.13 | 3.46 / 3.87 |
| Populated-root reopen, NoSync, 1,024 keys, 4 writers | 45.57 / 46.85 | 14.79 / 18.89 |

Durable mutation medians are about 2.4 times faster. Hot NoSync metadata is
about 1.4 times slower and remains a performance target: CPU medians are
92.90 / 71.64 ms, while both issue 2,048 write calls and 683,688 syscall bytes.
Those counters do not isolate the cause. Hot NoSync delete is close. Five-sample
wall ranges overlap materially: hot metadata spans 22.40–43.12 / 25.54–44.21 ms;
durable metadata spans 447.95–657.27 / 1111.95–4003.57 ms. These are local
measurements, not a statistical acceptance claim.

Unlike the preceding PUT benchmark's empty-root reopen, the new preopened modes
close/reopen a **populated seeded root** before mutation. Container projection
replay is lazy and can fall within the first mutation, while multifile pays for
its clean projection hint during close. Median preopen-close/open plus mutation
is 59.13 / 133.40 ms for metadata and 25.82 / 101.27 ms for delete. These are
medians of per-sample sums; seeding remains separate. OS page cache is retained.

After mutation, median close + reopen + verification milliseconds:

| Workload | Metadata update | Delete |
| --- | ---: | ---: |
| Durable, 128 keys, 1 writer | 17.07 / 18.63 | 15.19 / 21.40 |
| Hot seeded root, NoSync, 4 writers | 50.29 / 104.76 | 44.11 / 88.02 |
| Populated-root reopen, NoSync, 4 writers | 47.95 / 108.79 | 41.29 / 100.13 |

Container reopen alone remains slower: for the hot NoSync profile, reopen plus
verification takes 49.87 / 33.44 ms after metadata and 43.86 / 24.58 ms after
delete. Container close is much cheaper. The metadata hot-projection first body
pass takes 4.34 / 30.95 ms. Delete verification checks absence and is not a body
read benchmark. Neither mode measures public scans.

Durable mutation sync counts match: 256 fdatasync calls for metadata and 128 for
delete, with no fsync calls in either engine. Hot NoSync mutation uses neither;
populated-root-reopened NoSync uses container 4/0 versus multifile 0/8
fdatasync/fsync calls for both operations. The logical container remains 10 MiB;
filesystem-accounted allocated bytes are 10 MiB or 10 MiB + 4 KiB. The common
probe's `MiB_s` field uses reference body size and is not metadata/delete byte
throughput; use mutation timings and actual byte counters for these modes.

The earlier runtime reference manifest passed verification and its executables
were preserved. New raw logs, runner, JSON rows/group medians and preserved ELF
are `mutations-paired/`, `mutations-paired.sh`, `mutations-results.json` and
`pouch_space_compare-mutations-current` under `build/pouch-space-integrated/`.
`mutations-artifacts.sha256` verifies all 63 artifacts. Reproduce the paired
series with `bash build/pouch-space-integrated/mutations-paired.sh`, or substitute
`runtime-metadata`/`pouch-metadata` and `runtime-delete`/`pouch-delete` in the
current-source comparison command above.

## Reserved spans before cutover and bounded key admission

Automatic append rotation now prepares/binds its successor and reserves the
known canonical span before selecting its head and sealing the predecessor.
`ps_stream_reserve` and `ps_record_reserve` acquire existing zero-owned grants
without publishing a tail or writing canonical bytes. Metadata/delete reserve
their complete batch; ordinary streaming PUT reserves its header/key prefix,
with unknown body bytes still flowing through bounded buffers. A zero minimum
reserves one logical byte. Explicit empty structural rotation has no append span.

A failed reservation can retain reusable empty grants, but leaves the prior
head selected and appendable. The new 1 MiB-capacity test requires failure
before cutover, appends to the predecessor, closes/reopens, grows by 1 MiB and
retries the prepared successor. Captured readers survive growth and cutover;
reserved bytes are unreadable until publication. Publishing within the reserved
span must not allocate another grant or emit another grant journal event.
Raw stream tests also extend a reservation after buffering a prefix and check
cursor/tail stability, invalid arguments and both durability policies.

This does **not** complete protected maintenance admission. The fixed 10 MiB
probe still fills 117 keys and can reject peer-namespace deletion for lack of
rotation capacity. It now additionally asserts selected head and tail remain
unchanged across that capacity failure and reopen. Complete operation budgets,
reclamation and NoSync performance remain work in the full native goal.

### A reproduced write/replay boundary defect

Checking the largest reservation exposed a shared state-engine defect. A durable
metadata-only write with a 1 MiB + 1-byte key returned success; after container
reopen the key was absent. The decoder already rejects keys above 1 MiB, while
mutation/header admission permitted that metadata record. The reproduced output
is in `build/pouch-space-integrated/admission-key-bounds-repro2.log`.

Shared mutation admission now rejects oversized keys before producer input;
header/batch encoding applies the decoder's key/metadata limits too. No encoding,
version, compatibility path or public backend selector changes. The regression
tests both current multifile and container roots, plain and combined codecs,
under both sync policies. It rejects oversized PUT/metadata/delete requests,
requires no source consumption, preserves an exact-limit metadata-only key
across reopen, and verifies body/update/delete plus sequences using a 60 KiB
key. Body keys also occupy the existing 16-bit payload-context field.

### Verification

After that repair, the full Debug preset passed 271 checks under ASan/UBSan.
GNU and native musl release each passed all 98 affected checks: all 80 pouch-space
checks plus 18 existing Pouch groups. Twenty-three affected integration,
namespace, root and canonical-record groups passed ThreadSanitizer. Musl workers
retain their default 128 KiB stacks. No Darwin, QEMU or large-machine tests ran.

The namespace durable-image fault matrix now includes pre-reserved automatic
rotation, covering every observed allocation/I/O point under both sync policies,
missing writes and torn synchronization models. It requires the old canonical
prefix and a usable retry after recovery. The existing PUT and mutation fault
matrices also pass with the new admission order. Six affected Memcheck groups
passed: 786,869 allocations/frees, no remaining blocks, errors or suppressions.

Final logs and CTest details are `admission-verified-{debug,gnu,musl,tsan}-*`
under `build/pouch-space-integrated/`; the six
`admission-{namespace,pouch}-*-memcheck.log` files retain memory evidence.
Earlier `admission-final-*` logs predate the key-boundary repair; the final
counts above refer to `admission-verified-*`.

### Matched current and prior container measurements

The 180 serial GNU release runs use five alternating-order samples per
operation/profile/engine. Current container and current multifile call production
APIs from the same executable. Prior container uses the preserved `91632cc`
executable from the preceding mutation stage. Inputs remain 1 KiB bodies,
32-byte application metadata for metadata updates, 4 KiB source chunks, no
transforms/indexing, background maintenance or positive expiry, and a fixed
10 MiB container. NoSync profiles use 1,024 keys. Each writer owns a distinct
namespace. Every run verifies after mutation and after populated-root reopen;
all 120 container runs audit one file and no
sidecars. Metadata/delete seeding is outside the mutation timer and reported as
`seed_ms`; PUT and metadata have two rounds, deletion one. OS page cache remains.

Median mutation milliseconds:

| Operation / workload | Current container | Prior container | Current multifile |
| --- | ---: | ---: | ---: |
| PUT, durable, 128 keys, 1 writer | 738.47 | 831.30 | 1143.44 |
| PUT, fresh NoSync, 1,024 keys, 4 writers | 33.23 | 26.43 | 27.04 |
| PUT, empty-root reopen, NoSync, 4 writers | 52.96 | 48.84 | 28.80 |
| PUT, NoSync, 1,024 keys, 1 writer | 86.63 | 82.00 | 71.63 |
| Metadata, durable, 128 keys, 1 writer | 369.03 | 417.42 | 1168.81 |
| Metadata, hot seeded NoSync, 4 writers | 24.25 | 22.48 | 21.66 |
| Metadata, populated-root reopen, NoSync, 4 writers | 35.08 | 34.63 | 43.28 |
| Metadata, seeded NoSync, 1,024 keys, 1 writer | 90.21 | 88.57 | 83.06 |
| Delete, durable, 128 keys, 1 writer | 160.71 | 174.95 | 537.25 |
| Delete, hot seeded NoSync, 4 writers | 4.77 | 3.34 | 3.90 |
| Delete, populated-root reopen, NoSync, 4 writers | 10.33 | 13.42 | 15.50 |
| Delete, seeded NoSync, 1,024 keys, 1 writer | 6.21 | 5.58 | 6.38 |

Durable medians remain faster than multifile. NoSync is not accepted as solved:
fresh four-writer PUT is about 26% slower than prior container, metadata 8%
slower and deletion 43% slower by these medians. Five-sample ranges overlap:
PUT is 28.28–39.83 / 20.21–30.93 ms (current/prior); metadata
21.35–32.01 / 21.80–31.55 ms; deletion 2.64–7.12 / 2.59–6.60 ms. Fresh PUT
CPU medians are nearly equal, 56.68 / 56.62 ms, while other CPU ranges also
overlap. These measurements do not establish a causal timing breakdown or a
performance acceptance result. Raw rows, CPU/latency ranges and phase metrics
remain in the JSON.

Median syscall write counts/bytes and sync counts match prior container in each
profile. One-writer PUT uses four logical segments and accounts for 16 KiB more
kernel writes; one-writer metadata ends with three segments and accounts for
4 KiB more during its timed phase. This is consistent with stabilizing a new
grant-header page at head cutover before canonical writes dirty that page again.
It is a concrete cost to investigate, not proof that it explains every timing
regression. Both PUT variants have 12 fdatasync calls in that one-writer profile;
metadata has 3; delete has none. Sync-duration sums may overlap across workers.

For the fresh/hot NoSync four-writer profiles, current-container / multifile
verification passes take 3.82 / 43.30 ms after PUT and 3.77 / 28.65 ms after
metadata. Delete verification checks absence, not bodies. Reopen plus verification
alone remains slower for container: 45.22 / 33.07 ms after PUT, 50.60 / 34.27 ms
after metadata and 44.63 / 28.08 ms after delete. Including the preceding close,
per-sample sum medians are 45.41 / 105.46, 50.94 / 95.81 and 44.93 / 88.66 ms.
Public scan performance remains unmeasured.

Preopened PUT closes/reopens an empty root before work; metadata/delete do so
with a populated seeded root. Including that preopen close/open, current / MF
mutation medians are 59.47 / 29.12 ms for PUT, 49.23 / 106.70 ms for metadata
and 20.76 / 74.09 ms for delete. Lazy container replay and multifile close hints
must remain visible in that comparison. Do not treat metadata/delete `MiB_s`
as actual byte throughput; the common field uses reference body size.

The prior mutation manifest passed verification without replacing its ELF.
New artifacts under `build/pouch-space-integrated/` are `admission-paired/`,
`admission-paired.sh`, `admission-results.json`,
`pouch_space_compare-admission-current` and `admission-artifacts.sha256`.
The manifest checks all 184 artifacts, including both preserved executables.
The paired runner reproduces the complete series with Bash. Normal public
`lc_pouch_open` still uses multifile roots; the complete backend goal stays active.

## Private first records and root-durable routing

Automatic append now prepares/reserves its successor, completes the first
canonical record, then publishes the namespace head before exposing that stream's
tail. A failed producer or healthy publication failure leaves the predecessor
selected. An owned-commit callback performs logical publication outside allocator
and commit mutexes; ordinary within-segment commits retain their existing path.
The production logical manifest also stays private until physical success.
Interned segment labels survive array growth, append close and projection
invalidation while the namespace capability is retained.

Making that late head always durable would force the whole first value to disk
at EOF even for NoSync. The bounded `PS_ROUTING` kind instead follows root
durability while retaining CONTROL's one-page replacement allowance. Durable
head publication covers the completed first record in the same barrier.
NoSync publication need not do so. Before predecessor sealing, `ps_flush`
stabilizes the ready routing prefix; the durable seal cannot recover ahead of
its replacement head. Seal cleanup runs before the next mutation, without
fallible work after successful append publication. Explicit empty rotation
remains durable under both policies. Ownership grants, catalogs, root policy
and retention markers retain their existing durability protocols.

A peer checkpoint can flush complete private successor bytes while retaining
their checkpoint tail of zero. Recovery discards those bytes when the containing
head was never selected or its NoSync selection was lost. A published checkpoint
tail in an unselected successor is rejected. The namespace head payload remains
128 bytes, but its prototype typed key changes directly from CONTROL to ROUTING.
No legacy parser, migration, format-version increment or public ABI change.

### Verification

The full Debug preset passed 272 checks under ASan/UBSan, including all 81
container checks. GNU and native musl release each passed 99 affected checks:
81 container checks plus 18 existing Pouch groups. All 24 affected integration,
namespace, root and canonical-record groups passed ThreadSanitizer. Musl uses
its default worker stacks; no Darwin, QEMU or large-machine runs were added.

The namespace matrix covers every observed allocation/I/O failure point in
first-record automatic publication under both sync policies, including missing
writes and torn synchronization models. The held-publication test lets another
namespace append and checkpoint while complete first-record bytes remain private,
then proves recovery and retry with either successful or failed head publication.
It also rejects an authoritative tail in an unselected successor. Generic routing
tests prove replacement/deletion rollback, explicit flush, pinned prior values
and already-covered flushes. Capacity tests prove both bounded metadata kinds
can replace at foreground exhaustion and reject oversized writes.

A reserved first-record commit adds zero syncs under NoSync and one under durable
sync. Eight Memcheck groups passed with 948,597 allocations/frees and no remaining
blocks, errors or suppressions. They cover namespace publication, reservation and
faults; PUT allocation/I/O faults; mutation allocation/I/O faults; and controls.
Final evidence is `publication-verified-{debug,gnu,musl,tsan}-*` and
`publication-*-memcheck.log` under `build/pouch-space-integrated/`.

### Matched current, prior and multifile measurements

The primary series has 210 serial GNU release runs: five alternating-order
samples per operation/profile/engine. Current container and multifile call the
production APIs from the same executable; prior container uses the preserved
`59a5218` executable. Original profiles retain 1 KiB bodies, 32-byte application
metadata for metadata updates, 4 KiB source chunks and fixed 10 MiB containers.
Two additional PUT profiles use two keys with 1 MiB values and one/two writers.
Each writer owns a distinct namespace. PUT and metadata have two rounds; delete
one. Metadata/delete seeding is outside mutation timing. Transforms, indexing,
background maintenance and positive expiry are disabled. OS page cache remains.
All runs verify after mutation and populated-root reopen; all 140 container
runs audit one file and no sidecars.

Median mutation milliseconds:

| Operation / workload | Current container | Prior container | Current multifile |
| --- | ---: | ---: | ---: |
| PUT, durable, 128 keys, 1 writer | 798.01 | 812.31 | 1253.99 |
| PUT, fresh NoSync, 1,024 keys, 4 writers | 19.72 | 30.10 | 28.85 |
| PUT, empty-root reopen, NoSync, 4 writers | 52.24 | 57.73 | 25.88 |
| PUT, NoSync, 1,024 keys, 1 writer | 88.43 | 99.62 | 73.62 |
| Metadata, durable, 128 keys, 1 writer | 431.97 | 437.54 | 1162.52 |
| Metadata, hot seeded NoSync, 4 writers | 27.57 | 29.69 | 24.15 |
| Metadata, populated-root reopen, NoSync, 4 writers | 33.01 | 33.27 | 41.04 |
| Metadata, seeded NoSync, 1,024 keys, 1 writer | 94.20 | 98.29 | 85.14 |
| Delete, durable, 128 keys, 1 writer | 174.46 | 175.99 | 536.50 |
| Delete, hot seeded NoSync, 4 writers | 5.28 | 3.36 | 3.40 |
| Delete, populated-root reopen, NoSync, 4 writers | 12.28 | 14.05 | 24.27 |
| Delete, seeded NoSync, 1,024 keys, 1 writer | 5.54 | 5.73 | 6.63 |
| PUT, NoSync, 2 keys, 1 MiB values, 1 writer | 62.37 | 72.66 | 42.81 |
| PUT, NoSync, 2 keys, 1 MiB values, 2 writers | 38.84 | 61.26 | 28.14 |

Fresh four-writer PUT improves from 30.10 to
19.72 ms, versus
28.85 ms for multifile. Its five-sample ranges
are 15.88–31.41 / 23.88–37.31 / 22.28–33.16 ms
(current/prior/multifile), so timing attribution remains limited. Sync calls
fall from six to four; kernel-accounted writes fall by 28 KiB. One-writer
1 KiB PUT uses 11 rather than 12 syncs and writes 8 KiB less; 1 MiB-value PUT
uses nine rather than 12 syncs and writes 28 KiB less. Two-writer large-value
PUT uses six rather than 11 syncs. These are measured structural reductions,
not a complete explanation of every elapsed-time difference.

Durable mutation medians remain faster than multifile. NoSync is not accepted
as solved: empty-root reopened PUT, one-writer PUT/metadata, large-value PUT and
hot metadata/delete still trail multifile in the primary series.

Hot delete's higher primary median prompted a separate 60-run diagnostic:
ten alternating-order samples for seeded NoSync metadata/delete with four
writers and unchanged executables/inputs. Delete medians were
9.87 / 10.27 / 9.20 ms;
metadata 43.16 / 37.29 /
35.19 ms (current/prior/multifile). Absolute timings
shifted across all engines; delete did not reproduce a consistent current/prior
penalty and metadata stayed mixed. This diagnostic is retained separately in
the JSON and does not replace the primary samples or establish causal attribution.

For fresh four-writer PUT, current/multifile live verification takes
4.16 /
32.50 ms. Reopen plus verification
alone takes 57.25 /
37.59 ms; including preceding close,
per-sample sum medians are 57.53 /
103.82 ms. Hot metadata live
verification takes 4.95 /
30.44 ms, with close/reopen/verification
sum medians 53.25 /
84.66 ms. These are point-read
verification passes; public scan performance remains unmeasured.

Preopened PUT reopens an empty root; metadata/delete reopen a populated seed.
Including the preopen close/open, current/multifile mutation sum medians are
58.40 /
26.26 ms for PUT,
52.84 /
88.51 ms for metadata
and 22.76 /
79.20 ms for delete.
Lazy replay and multifile close work remain visible in that comparison.
Metadata/delete `MiB_s` remains a reference-body-size statistic, not actual
mutation byte throughput.

Artifacts under `build/pouch-space-integrated/` are
`publication-paired/`, `publication-paired.sh`,
`publication-hot-mutations/`, `publication-hot-mutations.sh`,
`publication-results.json`, `pouch_space_compare-publication-current` and
`publication-artifacts.sha256`. The manifest covers 275 artifacts: both frozen
executables, both Bash runners, the JSON and all 270 logs. The preceding admission
manifest is checked without replacing either preserved executable.
Whole-operation maintenance admission, reclamation, remaining public semantics
and shared-root coordination remain in the active full-backend goal.

## Captured state reads and paged scans

The private production-state integration now supports both read-many entrypoints,
metadata visits/prefix visits, and paged scan summaries with optional body access.
Read-many captures encoded read pins while holding projection coordination and
decodes one value immediately before its visitor, after releasing that guard.
It does not create a decoder buffer per captured row. Metadata-only summaries
capture small read windows without reading bodies. Scan body access clones the
captured window with an independent cursor, without resolving the current
directory again. Clones share a reference-counted namespace window and retain
root/crypto ownership through the last source close.

Callback mutation cannot substitute newer body bytes into the captured view.
A source obtained from a scan can outlive both that scan and Pouch close;
exclusive root ownership stays held until its last source closes. The container
does not populate a body cache, including for cached read-many and a scan's
cache-completed-body hint. This establishes a read-pin boundary for future
relocation; maintenance publication and reclamation remain unimplemented.

Two shared state-engine behaviors also change. Scan body results copy metadata
instead of consuming the visitor's borrowed row, so result cleanup leaves row
fields valid and repeated body opens work in both backends. Read-many results
now populate their captured has-body and staged-delete fields. No canonical
bytes, persistent format or public ABI change.

### Verification

The full Debug preset passed 308 checks under ASan/UBSan, including 117 container
checks. GNU and native musl release each passed 135 affected checks: all 117
container checks and 18 existing Pouch groups. Twenty-eight affected integration,
namespace, root and canonical-record groups passed ThreadSanitizer. No Darwin,
QEMU or large-machine run was added.

New read behavior tests cover both backends, all four codec combinations and
both sync policies: callback order, duplicates, missing/deleted/metadata-only
keys, empty bodies, early stop/retry, invalid inputs, pagination, prefix visits,
mutation inside a callback, repeated independent body opens, and retained
sources after Pouch close. A warmed container metadata-scan fixture requires
zero payload I/O and no allocation as large as a 16 KiB decoder buffer.

The read matrices inject failures at 3,640 observed allocation sites and 1,282
I/O sites across warm and reopened namespaces, plain and combined codecs, both
read-many variants, metadata/body scans and prefix visits. They require correct
cleanup, unchanged stored values and successful retry or reopen after poison.
Held callbacks in another thread permit same-namespace overwrite/delete and
peer-namespace publication while retaining the original rows and bodies.

Five Memcheck groups passed: 4,162,472 allocations/frees, no remaining blocks,
errors or suppressions. They cover normal reads, allocation/I/O read matrices,
read concurrency and held namespace publication. Evidence is
`reads-verified-{debug,gnu,musl,tsan}-*`, `reads-focused-tests.log`,
`reads-compare-tests.log` and `reads-*-memcheck.log` under
`build/pouch-space-integrated/`. The exhaustive allocation group's bounded
timeout is 600 seconds to cover sanitizer runs over thousands of fresh roots.

### Matched read measurements

The primary series has 120 serial GNU release runs: five alternating-order
samples × four read methods × three workloads × current container/multifile.
Both backends use production state APIs from the same executable. Two ordinary
PUT rounds precede the read phases. The writer-count argument also selects the
concurrent reader count, with one namespace per writer/reader. Read-many batches
64 keys; scans use 256-row pages. Points, read-many and body scans verify every
body byte. Metadata-only scan timings exclude body access; full seed bodies are
verified afterward, outside both measured read phases.

Inputs use 4 KiB source chunks, NoSync seed writes, a fixed 10 MiB container,
no transforms, indexing, background maintenance or positive expiry. OS page cache
is retained. The reopened phase recreates resident projections; these are not
cold-disk measurements. Read intervals include worker creation, membership and
version checks, and fixture byte verification for body modes. The current
multifile owner retains its existing cache/projection optimizations; the
container uses no body cache or persistent clean projection. Peak RSS covers
the entire process, including seeding and untimed validation.

Wall-clock median milliseconds; reopened columns include root open and the
first complete read pass:

| Method / workload | Live container | Live multifile | Reopen + read container | Reopen + read multifile |
| --- | ---: | ---: | ---: | ---: |
| Points, 1,024 × 1 KiB, 1 reader | 4.359 | 31.277 | 20.563 | 32.067 |
| Read-many, 1,024 × 1 KiB, 1 reader | 4.235 | 6.284 | 20.348 | 9.474 |
| Scan bodies, 1,024 × 1 KiB, 1 reader | 3.620 | 5.222 | 20.900 | 7.344 |
| Scan metadata, 1,024 × 1 KiB, 1 reader | 1.182 | 1.290 | 18.372 | 3.316 |
| Points, 1,024 × 1 KiB, 4 readers | 1.975 | 13.036 | 28.100 | 16.929 |
| Read-many, 1,024 × 1 KiB, 4 readers | 1.469 | 3.160 | 29.660 | 5.508 |
| Scan bodies, 1,024 × 1 KiB, 4 readers | 1.365 | 1.827 | 25.090 | 4.251 |
| Scan metadata, 1,024 × 1 KiB, 4 readers | 0.549 | 0.573 | 27.303 | 3.410 |
| Points, 64 × 32 KiB, 1 reader | 2.594 | 6.365 | 16.931 | 6.884 |
| Read-many, 64 × 32 KiB, 1 reader | 2.891 | 4.167 | 15.964 | 3.540 |
| Scan bodies, 64 × 32 KiB, 1 reader | 2.792 | 3.465 | 21.037 | 3.353 |
| Scan metadata, 64 × 32 KiB, 1 reader | 0.228 | 0.152 | 15.819 | 0.575 |

Live point, read-many and body-scan medians are lower for container in all three
profiles. Metadata-only scans are near parity in the two 1 KiB profiles and
slower for container in the 64-key profile; their ranges overlap. Four-reader
point-read ranges are 1.76–2.74 / 8.45–14.77 ms (container/multifile).
Body-scan ranges overlap at 0.92–2.01 / 1.31–2.94 ms; metadata-scan ranges are
0.54–0.93 / 0.45–0.65 ms. Five samples do not establish a complete causal
breakdown.

Four-reader aggregate CPU medians (container/multifile):
Points: 7.03 / 39.85 ms;
Read-many: 5.05 / 10.13 ms;
Scan bodies: 3.87 / 5.24 ms;
Scan metadata: 1.78 / 1.63 ms.
CPU sums across simultaneous readers can exceed wall time.

Bare reopen/read remains a material container limitation. Namespace admission
still reconstructs the canonical projection and verifies stored payload CRCs;
multifile can reuse its close-generated clean projection. Those mechanisms do
not isolate all measured latency components. Preceding close work also matters.
For four readers, per-sample close + reopen + read sum medians are:

| Method, 1,024 × 1 KiB | Container | Multifile |
| --- | ---: | ---: |
| Points | 28.560 | 67.511 |
| Read-many | 30.006 | 55.958 |
| Scan bodies | 25.224 | 62.507 |
| Scan metadata | 27.470 | 57.748 |

That combined interval is still worse for container in the 64 × 32 KiB
read-many, body-scan and metadata-scan profiles (container/multifile):
Read-many: 16.033 / 15.682 ms;
Scan bodies: 21.157 / 18.433 ms;
Scan metadata: 15.866 / 11.815 ms.
These regressions remain visible in the full-backend goal.

### Legacy PUT check and retained artifacts

Fifteen additional fresh four-writer PUT runs compare the current container,
current multifile and the frozen `acd24b1` container, with the original serial
point-verification phases. Mutation medians are 18.80 / 24.17 / 16.66 ms in that
order. Current container is higher than prior; ranges overlap at
15.28–21.46 / 14.27–20.96 ms (current/prior). Aggregate CPU medians are
47.50 / 42.72 ms and ranges overlap. Both container executables make four syncs,
write 3,228,404 syscall bytes and account for 3,252,224 kernel write bytes in
every sample. This series does not isolate a cause for the higher current median.

The 135 runs all verify values after populated-root reopen; all 70 container
runs audit one file and no sidecars. Legacy `MiB_s`, latency, syscall and sync
fields still describe the two-round mutation phase, not read throughput.
`read_cpu_ms` and `reopen_read_cpu_ms` are the new read-phase CPU measurements.

Artifacts under `build/pouch-space-integrated/` are `reads-paired/`,
`reads-paired.sh`, `reads-results.json`, `pouch_space_compare-reads-current`
and `reads-artifacts.sha256`. The manifest covers 139 benchmark artifacts:
both frozen executables, the Bash runner, JSON and all 135 raw logs. The preceding
275-artifact publication manifest is checked without replacing its executables.
Normal public open remains multifile. Atomic maintenance admission, snapshot and
obsolete publication/reclamation, advanced mutation policy, public query-client
and collection ownership, indexes, queues/XA/outbox and shared-root coordination
remain part of the active full native implementation goal.

## Namespace snapshots and obsolete-source reclamation

The namespace owner now installs sealed canonical snapshots and explicitly
retires obsolete sources. Snapshot begin captures a fixed prefix and releases
mutation authority across producer calls, allowing same-namespace appends,
peer publication and checkpoints to continue. Commit stabilizes the complete
stream and catalog binding before durably selecting the snapshot and its covered
segment boundary in the existing head. Newer segments remain the replay tail.
This applies under both sync policies; successful selection has no fallible
reclamation afterward.

Replay visits only the selected snapshot plus newer segments. Older bound
sources remain addressable for body references, through a sparse sorted index.
The logical caller must prove that a source has no remaining live references
before requesting reclamation. Catalog removal precedes stream retirement.
Captured readers and clones keep their bytes; catalog/read pins defer closing,
and checkpoint retirement still precedes physical reuse. Explicit root orphan
reclamation rechecks closed, unbound incarnations without reading canonical
records or bodies. No additional file or payload log is introduced.

Build IDs and namespace IDs jointly bind snapshot streams. Unpublished snapshot
bindings are removed before selecting a higher ID, so a failed build cannot
later masquerade as a historical selected snapshot. The 128-byte routing head
uses previously reserved fields for snapshot ID and covered boundary. Canonical
source labels remain metadata. The private state harness can reopen snapshot
bodies, apply metadata updates referencing those bodies, reopen again and
continue versions/query sequences.

This is physical snapshot publication and reclamation. The logical owner still
must supply complete records, high-water marks, tombstones and protected history.
Automatic compaction, retention scheduling and full maintenance admission remain
integration work. Source objects follow bound/pending participation, while
interned labels retain their existing namespace-owner lifetime. This stage does
not establish bounded metadata memory for an indefinitely lived owner.

### Verification and a validation correction

The full Debug preset executed 313 checks: 312 passed, including all 122
container checks. The timeout-contract inspector exceeded its unchanged
ten-second limit; an isolated repeat passed in 9.46 seconds. Extracting each
test's JSON once, rather than reparsing the full listing for every property,
reduced the corrected check to 1.91 seconds. A valid fixture passes and a fixture
without a timeout still fails. The contract and timeout were not relaxed.

GNU and native musl release each passed all 140 affected checks; 33 affected
Pouch, namespace, root and canonical-record groups passed ThreadSanitizer.
Review then found a mixed-source validation regression: variable-width segment
labels could load under a regenerated canonical label, preventing later catalog
removal. A catalog replacement fixture reproduced the error before correction.
Restoring the original 28-byte segment-label width rejects both short and
overpadded names. The final correction passed 14 focused namespace/Pouch groups
under Debug, GNU, musl and TSAN. Snapshot labels retain their exact 33-byte width.

The new behavior checks cover all four codecs and both durability policies:
capture boundaries, later appends, busy commit retry, repeat selection,
historical-source reads, cross-namespace binding rejection, catalog/read pins,
cloned windows, checkpoint/reopen and metadata references into snapshots.
A held snapshot producer permits same-namespace and peer writes plus checkpoint.

There are 658 modeled baseline/failure cases: 328 for an initial snapshot and
330 for replacement of a prior selected snapshot. They inject allocation, I/O,
torn writes and partial barrier persistence during begin, canonical production,
selection, source reclamation and orphan cleanup. Recovery retains the latest
logical value, and a later higher-ID selection removes failed staged bindings.
The existing read sweeps also pass at 3,670 allocation sites and 1,282 I/O sites.

Fixed-capacity churn completes 192 cycles under each durability policy in
10 MiB. Each run writes over 24 MiB of canonical bodies and snapshot copies;
payload reservations remain below 1 MiB after each checkpoint. Periodic reopen
recovers the latest value. Distinct payload bytes per cycle make stale reused
grants fail CRC verification; the strengthened fixture also passes focused
Debug, GNU, musl, TSAN and Memcheck reruns. This one-key workload proves reuse,
not complete maintenance admission for a store dominated by live metadata.

Six final Memcheck groups pass: 445,559 allocations and frees, zero remaining
blocks, errors or suppressions. They cover namespace validation, snapshot
lifecycle, failure cases, held producers, churn and production-state snapshot
reopen. Evidence under `build/pouch-space-integrated/` includes
`snapshots-verified-{debug,gnu,musl,tsan}-*`,
`snapshots-label-{debug,gnu,musl,tsan}-*`,
`snapshots-replacement-faults-debug.log`, `snapshots-debug-timeout-*`,
`snapshots-timeout-negative.log` and `snapshots-*-memcheck.log`.

### Matched ordinary-path regression measurements

These measurements do not run compaction or install snapshots. They check the
ordinary PUT/read paths after the owner changes. Five independent samples use
the GNU release build, plaintext NoSync roots, 4 KiB source chunks, fixed 10 MiB
container capacity and two PUT rounds. Engine order alternates. The OS cache is
retained: reopen rebuilds a cold resident projection, not a cold disk cache.

The primary series has 120 paired read runs plus 15 four-writer PUT runs
(current container, prior read-stage container and multifile). All values verify
after populated-root reopen. Medians below are container/multifile milliseconds.
Close-plus-ready sums the two phases per sample before taking the median.

| Profile / method | Live read C / MF (ms) | Open + read C / MF (ms) | Close + open + read C / MF (ms) |
| --- | ---: | ---: | ---: |
| 1,024 × 1 KiB, 1 reader: Point | 6.547 / 39.643 | 27.833 / 36.109 | 28.564 / 54.516 |
| 1,024 × 1 KiB, 1 reader: Read-many | 4.858 / 7.020 | 20.498 / 10.644 | 20.721 / 28.136 |
| 1,024 × 1 KiB, 1 reader: Body scan | 4.137 / 4.720 | 20.242 / 7.616 | 20.757 / 23.640 |
| 1,024 × 1 KiB, 1 reader: Metadata scan | 1.382 / 1.413 | 21.011 / 4.137 | 21.553 / 20.257 |
| 1,024 × 1 KiB, 4 readers: Point | 3.186 / 13.531 | 30.594 / 17.302 | 30.819 / 85.007 |
| 1,024 × 1 KiB, 4 readers: Read-many | 1.666 / 3.544 | 25.295 / 7.487 | 25.649 / 77.100 |
| 1,024 × 1 KiB, 4 readers: Body scan | 1.586 / 2.099 | 29.005 / 5.367 | 29.283 / 61.824 |
| 1,024 × 1 KiB, 4 readers: Metadata scan | 1.194 / 0.786 | 26.698 / 3.961 | 27.417 / 59.834 |
| 64 × 32 KiB, 1 reader: Point | 2.908 / 7.672 | 24.964 / 8.309 | 25.058 / 22.147 |
| 64 × 32 KiB, 1 reader: Read-many | 3.263 / 5.026 | 20.895 / 4.392 | 20.982 / 18.686 |
| 64 × 32 KiB, 1 reader: Body scan | 3.490 / 3.492 | 23.558 / 3.798 | 23.610 / 19.456 |
| 64 × 32 KiB, 1 reader: Metadata scan | 0.309 / 0.189 | 17.561 / 0.933 | 17.605 / 14.428 |

Live point and batched reads remain faster in these samples; body scans are
faster or near equal. Metadata-only scans are mixed. Bare open-plus-read is
slower in eleven of the twelve primary profiles: the container replays canonical
history while multifile can reuse a close-generated clean projection. Including
close changes the comparison:
the four-reader container profiles are substantially faster overall, while the
large-body profiles remain slower. The single-reader metadata profile is also
slightly slower over the complete close/reopen lifecycle.

The primary four-reader metadata median is 1.194 / 0.786 ms. A subsequent
45-run diagnostic interleaves current container, frozen prior container and
multifile for the affected metadata profiles and four-reader point reads.
The primary results remain above; they are not replaced by the diagnostic.

| Diagnostic live read | Current container (ms) | Prior container (ms) | Multifile (ms) |
| --- | ---: | ---: | ---: |
| 1,024 × 1 KiB, 4 readers: point | 1.929 | 1.853 | 12.354 |
| 1,024 × 1 KiB, 4 readers: metadata | 0.724 | 0.675 | 0.705 |
| 64 × 32 KiB, 1 reader: metadata | 0.182 | 0.194 | 0.165 |

Current/prior ranges overlap: four-reader points 1.655–3.144 / 1.846–2.298 ms,
four-reader metadata 0.586–0.940 / 0.650–0.785 ms, and large-body metadata
0.150–0.291 / 0.166–0.285 ms. This does not isolate a new read regression.
It also does not establish a consistent metadata-scan advantage over multifile.

The primary four-writer PUT medians are 21.919 / 28.756 / 26.255 ms for
current container / multifile / prior container. Current/prior ranges overlap
at 18.903–27.992 / 24.700–77.536 ms. Both container executables make four syncs
and write exactly 3,228,404 syscall bytes and 3,252,224 kernel-accounted bytes in
every sample. These timings do not isolate a cause for the different medians.

All 180 runs verify values after reopen; all 100 container runs audit one file
and no sidecars. Raw logs are `snapshots-paired/` and `snapshots-diagnostics/`,
with corresponding Bash runners and `snapshots-results.json`. The
`snapshots-artifacts.sha256` manifest covers 185 benchmark artifacts: two frozen
executables, two runners, JSON and all 180 logs. The prior 139-artifact read
manifest remains unchanged and verifies.

The measured current executable is `pouch_space_compare-snapshots-validated`,
SHA-256 `067723c41b1a437e54ba8ca26a8d68e8cef589f7a47766fd06cf855f26e18db0`.
The prior read-stage executable remains
`f0fcd389311221cfe6b482bf3313a1bc85119897d7ff93a9dc621da5bf667cda`.
Actual maintenance performance, complete capacity admission, advanced public
semantics and shared-root ownership remain in the active full-backend goal.
Normal public open remains multifile.

## Shared snapshot serialization and regression checks

The production state codec now writes canonical snapshot records through
borrowed record I/O. Native compaction and clean-close projection writing use
the same implementation. A caller supplies resolved entries and either keeps
an existing body reference with zero body I/O or copies the encoded body
incrementally using borrowed scratch capped at 128 KiB. The original stored CRC
must match before PENDING is completed. Descriptors, immutable crypto context,
metadata, flags, timestamps, retention, versions and both index sequences are
preserved. This stage adds no decode/re-encrypt pass, payload spool or whole-body
buffer, public backend selector, ABI change or persistent format/version change.

The writer also fixes an existing native clean-close projection defect: the
object-link flag was passed as descriptor reserve rather than written to the
payload flags. A native close/reopen regression now checks object classification
under both sync policies.

Verification on native x86_64:

- Full Debug ASan/UBSan: all 346 CTest groups passed.
- GNU release and musl release: all 21 selected groups passed in each build,
  including native Pouch, compression/durability/recovery, background compaction,
  small-stack compaction, all state-record tests and the Pouch snapshot test.
- ThreadSanitizer: all eight selected state-record and Pouch snapshot/concurrency
  groups passed.
- The new codec matrix has 96 cases: four codec modes, two durability policies,
  file/container outputs, state/object records and empty/one-byte/large bodies.
  Readback checks copied ciphertext byte for byte as well as decoded plaintext.
  It also checks references, high-water marks, tombstones, placeholders and
  explicit/default query sequences. Large-body cases assert interleaved input
  and output and the chunk cap.
- Fault coverage includes 41 injected allocation/output failures with 15
  successful baselines, ten source-failure cases with and without an error
  object, eleven invalid inputs and two offset-limit cases. Failed output
  retains its pre-existing prefix. Pouch tests build snapshots from captured
  encoded sources, then reopen, reclaim the old segment, update metadata and
  reopen again across all eight codec/durability combinations.
- Three Memcheck groups completed 41,342 matched allocations/frees with zero
  errors, leaks or suppressions. Changed-line clang-format checks and
  `git diff --check` passed.

Logs use the `build/pouch-space-integrated/snapshot-codec-` prefix:
`debug-build.log`, `debug-tests.log`, `gnu-build.log`, `gnu-tests.log`,
`musl-build.log`, `musl-tests.log`, `tsan-build.log`, `tsan-tests.log`
and the three `*-memcheck.log` files.

### Matched ordinary-path performance

Five alternating samples compare current/prior container and current/prior
multifile executables. The 280 runs comprise 240 read profiles and 40
four-writer PUT profiles. All values verify after populated-root reopen; all
140 container runs audit one fixed 10 MiB file without sidecars. Inputs are
plaintext with 4 KiB producer chunks and two PUT rounds. Reads use NoSync; PUT
runs cover both durability policies. The OS cache remains warm; reopened resident
projections are rebuilt. These runs do not execute logical compaction or install
snapshots, so they are ordinary-path regression evidence.

Read medians follow. C / MF means container / multifile. Close-plus-readiness
sums close, open and reopened read for each sample before taking the median.

| Profile / method | Live read C / MF (ms) | Close + open + read C / MF (ms) | Prior close + open + read C / MF (ms) |
| --- | ---: | ---: | ---: |
| 1,024 × 1 KiB, 1 reader: Point | 3.984 / 33.931 | 22.727 / 51.341 | 23.035 / 55.167 |
| 1,024 × 1 KiB, 1 reader: Read-many | 4.475 / 7.623 | 25.713 / 24.115 | 24.274 / 28.661 |
| 1,024 × 1 KiB, 1 reader: Body scan | 4.097 / 3.449 | 21.986 / 22.587 | 21.527 / 25.421 |
| 1,024 × 1 KiB, 1 reader: Metadata scan | 1.360 / 1.150 | 19.670 / 21.790 | 20.620 / 20.818 |
| 1,024 × 1 KiB, 4 readers: Point | 1.957 / 12.543 | 28.498 / 71.618 | 25.722 / 77.544 |
| 1,024 × 1 KiB, 4 readers: Read-many | 1.546 / 2.692 | 28.973 / 62.961 | 27.779 / 67.933 |
| 1,024 × 1 KiB, 4 readers: Body scan | 1.675 / 1.621 | 27.314 / 56.007 | 27.591 / 71.046 |
| 1,024 × 1 KiB, 4 readers: Metadata scan | 0.668 / 0.597 | 34.780 / 68.449 | 26.988 / 67.302 |
| 64 × 32 KiB, 1 reader: Point | 2.853 / 6.895 | 21.898 / 25.787 | 21.570 / 21.004 |
| 64 × 32 KiB, 1 reader: Read-many | 2.928 / 4.353 | 18.876 / 24.063 | 17.933 / 17.535 |
| 64 × 32 KiB, 1 reader: Body scan | 2.754 / 2.704 | 22.457 / 15.754 | 20.465 / 17.794 |
| 64 × 32 KiB, 1 reader: Metadata scan | 0.178 / 0.177 | 20.749 / 12.948 | 16.948 / 15.672 |

Live point and read-many medians favor the container in all six corresponding
profiles. Body and metadata scans are mixed. Bare open-plus-read remains slower
for the container in eleven of twelve profiles. Including close makes the
container faster in nine of twelve profiles in this series; the single-reader
small-body read-many and large-body scan profiles remain slower.
These lifecycle comparisons vary with close cost and must not be substituted
for the live-read comparison.

| Four-writer PUT, 1,024 × 1 KiB | Current C (ms) | Prior C (ms) | Current MF (ms) | Prior MF (ms) |
| --- | ---: | ---: | ---: | ---: |
| NoSync | 20.734 | 19.612 | 28.713 | 29.526 |
| Durable commit | 5,293.460 | 5,799.223 | 8,687.485 | 8,295.727 |

Current/prior live-read, PUT and complete close-plus-readiness ranges overlap in
every matched profile. Multifile close ranges also overlap in every profile.
The NoSync PUT ranges are 16.863–24.019 / 15.988–32.633 ms for current/prior
container and 23.304–41.472 / 25.169–75.937 ms for current/prior multifile.
Durable ranges are 4.647–6.712 / 4.729–6.313 s and
7.969–8.981 / 7.682–8.810 s respectively. These samples do not isolate a
regression or improvement caused by the shared writer.

Both container executables write exactly 3,228,404 syscall bytes under either
policy. NoSync uses four syncs and 3,252,224 kernel-accounted bytes per run.
Durable container runs make 1,038–1,048 sync calls across the current/prior
samples; multifile makes 2,048 fdatasync calls plus eight fsync calls.
Group-commit scheduling affects these counts; no writer queue was added.

Raw logs are `snapshot-codec-paired/`, with Bash runner
`snapshot-codec-paired.sh`, `snapshot-codec-results.json` and
`snapshot-codec-benchmark.log`. The `snapshot-codec-artifacts.sha256` manifest
covers 285 artifacts, including both frozen executables. The current executable,
`pouch_space_compare-snapshot-codec-current`, has SHA-256
`a82c2175c391debc6c4987677e0ef6f71a611d9307f96d5b3a3803b4ec8b0ccf`.
The prior `pouch_space_compare-snapshots-validated` remains
`067723c41b1a437e54ba8ca26a8d68e8cef589f7a47766fd06cf855f26e18db0`.

Automatic logical projection production, safe reclamation policy and complete
maintenance admission remain required by the active full-backend goal. A full
metadata projection and any relocated live bodies must fit the maintenance
workspace; the existing physical floor alone does not establish that guarantee.

## Captured logical snapshots and metadata regression checks

The logical engine now captures an immutable projection from a selected snapshot
and captured newer log prefix. It reuses production supersession, query-generation
inheritance and object classification. Capture retains one metadata entry per
current key, including tombstones and placeholders; a temporary hash index is
released after sorting. Visits emit HIGH_WATER first and both sequences explicitly,
allocate nothing, and operate outside engine coordination. Capture does not read
or retain body values, publish a snapshot, invalidate the live cache or authorize
reclamation. The caller retains physical source pins and allocator context.

The new replay test found an existing metadata-worker defect: its queued entry
copy omitted the prior query generation. A canonical META could therefore carry
zero while the live cache retained the correct prior generation. The copy now
preserves that generation and presence flag. The integration test failed with
query generation zero before this fix and passes with generation five afterward.

Verification on native x86_64:

- All 349 standard Debug ASan/UBSan tests passed. A subsequent test-only addition
  deterministically holds replay while a same-namespace PUT commits; that focused
  Debug test passed too. No production code changed after the standard run.
- GNU release and musl release each passed all 24 selected native Pouch,
  compression/durability/recovery, stack, state-codec and snapshot/projection tests.
- All 11 selected ThreadSanitizer groups passed with the final concurrency test.
- The eight codec/durability combinations each install two logical snapshots,
  preserve tombstones, placeholders, object classification and both watermarks,
  retain newer tail updates and reopen after explicit obsolete-source removal.
  A newer object META keeps its original covered source bound until replacement
  no longer needs it. Captured old readers survive removal.
- The capture fixture has more than 2 MiB of historical bodies. Its largest
  capture allocation stays below 64 KiB; plaintext metadata replay reads less
  than 512 KiB. These are fixture bounds, not fixed limits on key metadata.
- All 737 capture allocation sites fail cleanly both with and without an error
  object: 1,474 injected allocation failures. Replay errors at the beginning,
  middle and end, staged/decision rejection, malformed metadata, invalid arguments,
  an empty prefix and failure at every visitor position are also checked.
  Source keys and metadata can change after capture without changing visits.
  Superseded versions and stale sequences do not replace newer entries.
- Held replay permits a same-namespace PUT before capture completes while the
  result retains the old prefix. A held immutable visitor permits another reader
  to complete 16 visits and another same-namespace PUT to commit. These run under
  plaintext NoSync and encrypted/compressed durable policy.
- Three Memcheck groups completed 589,405 matched allocations/frees with zero
  errors, leaks or suppressions. Changed-line formatting and
  `git diff --check` passed.

Logs use `build/pouch-space-integrated/projection-` prefixes for Debug, GNU,
musl and TSAN builds/tests. `projection-debug-latch-tests.log` records the final
test-only addition. `projection-memcheck.log`,
`projection-faults-memcheck.log` and `projection-concurrency-memcheck.log`
contain the three memory checks.

### Matched metadata-update performance

Five alternating samples compare current/prior container and current/prior
multifile executables. Each run seeds 512 one-KiB bodies outside the timed
mutation phase, then performs two metadata-update rounds with one or four writers.
The 80 runs cover NoSync and durable commits, plaintext inputs and 4 KiB producer
chunks. All verify bodies and metadata after populated-root reopen; all 40
container runs audit one fixed 10 MiB file with no sidecars. This measures the
metadata-worker fix; these runs do not execute logical compaction.

| Metadata updates, 512 × 1 KiB | Current C (ms) | Prior C (ms) | Current MF (ms) | Prior MF (ms) |
| --- | ---: | ---: | ---: | ---: |
| NoSync, 1 writer | 57.349 | 53.378 | 44.191 | 45.770 |
| NoSync, 4 writers | 13.841 | 16.975 | 14.002 | 13.479 |
| Durable, 1 writer | 1669.193 | 1583.268 | 4144.871 | 4249.858 |
| Durable, 4 writers | 1329.259 | 1147.226 | 2452.402 | 2429.410 |

Single-writer NoSync remains slower than multifile in these samples; four-writer
NoSync is near parity. Durable container medians are lower under both writer
counts. Current/prior ranges overlap in every profile, so these samples do not
isolate a performance change caused by preserving the query generation.
For example, four-writer durable current/prior container ranges are
1.180–2.301 / 1.031–1.970 s, and multifile ranges are
2.276–2.517 / 2.320–2.733 s.

Current and prior container syscall byte counts are identical in every profile:
342,692 bytes for one writer and 341,576 for four writers. NoSync uses three
sync calls with one writer and zero with four; durable uses 1,027 and 516–527
respectively across both executables. Durable multifile uses 1,024 fdatasync
calls, with two additional fsyncs in the one-writer profile. The scalar-copy fix
adds no allocation, body I/O or durability barrier. These observations do not
attribute the timing differences to a particular storage or scheduler cost.

Raw logs are `projection-metadata-paired/`, with runner
`projection-metadata-paired.sh`, `projection-metadata-results.json` and
`projection-metadata-benchmark.log`. The `projection-artifacts.sha256` manifest
covers 85 artifacts including both frozen executables; the previous
285-artifact snapshot-codec manifest remains unchanged and verifies.
The measured current executable is `pouch_space_compare-projection-current`,
SHA-256 `d0a9475454c63fab72dc81230e74a6b6c709fd55320d76c4b216e53152b2a643`.
The prior `pouch_space_compare-snapshot-codec-current` remains
`a82c2175c391debc6c4987677e0ef6f71a611d9307f96d5b3a3803b4ec8b0ccf`.

At this checkpoint, the capture supplied logical metadata for calculating
workspace without establishing that the physical reserve could fit it. These
measurements did not execute maintenance; end-to-end measurements follow below.

## Forced ordinary-state compaction

The private container harness now exposes `ps_pouch_state_compact`: one forced
ordinary-state pass that publishes a complete metadata snapshot and relocates
live bodies from at most one sealed source. It preserves current state/object
values, metadata, tombstones, placeholders, versions and both sequence high-water
marks. It is not the public maintenance API or an automatic background policy. History
consumers, staged/XA records, collections, indexes and retention scheduling
remain outside this harness's maintenance contract.

The pass captures the selected prefix under logical namespace authority, counts
live encoded bytes by source, and selects a sealed source using capacity minus
live body bytes. Shared canonical encoders measure the metadata snapshot and
selected body's canonical frames without body I/O. Known spans are reserved
before copying. Full grants retain geometric growth; the final grant is clipped
to the page-rounded need. New sealed streams leave less than one 4 KiB page of
logical capacity unused.

Relocated bodies go into a separate sealed pack bound under `PS_CATALOG_BODY`
(surface 6). Its stream owner is the globally unique build ID; its stream key
binds the namespace identity. `body-<20 decimal digits>.log` is metadata inside
the container, never another file. Packs do not participate in logical replay.
The metadata snapshot contains references, placeholders, tombstones and HIGH_WATER.
Separating these owners lets later metadata snapshots retain untouched bodies
without copying every live value again.

Encoded copying runs outside logical namespace authority with at most 128 KiB
of heap scratch. It retains stored bytes, descriptors and immutable crypto
contexts without decoding, re-encoding, payload spooling or whole-body buffering.
Publication durably binds/seals the pack before selecting the metadata snapshot.
It then captures the latest snapshot plus tail under the same logical authority
used for publication, protects every current body source, and reclaims only
eligible unreferenced sources. This protects a newer META that still refers to
the original copied source. The resident logical projection is invalidated
before that authority is released, including on an uncertain publication error.
Old read pins survive retirement. Closed unbound streams from interrupted work
are reclaimed through the existing root orphan API, followed by checkpoint.

A selected snapshot may be rewritten at the same covered boundary; no artificial
empty log rotation is needed to relocate a body. Encoded sources now validate
their stored CRC as bytes are consumed, including covered original segments
which are no longer replayed. A mismatch withholds the final chunk and remains
an error until reset. Reset and clone start independent checksum/cursor state;
pins and their storage owner can outlive Pouch.

This is still **partial admission**: known canonical payload grants are reserved,
but the complete rotation, catalog, head, checkpoint and concurrent-foreground
budget is not yet protected. The fixed physical reserve does not establish
full-store maintenance admission. Label interning still follows namespace-owner
lifetime. Normal public open remains multifile, with exclusive ownership in the
experimental harness and no existing-root conversion or production cutover.

### Verification

- All 363 Debug ASan/UBSan tests have passing evidence: 332 standard tests and
  31 additional example/install checks. The first standard run had one
  20-second `release_test_selection_test` timeout; its isolated retry took
  18.82 seconds. The fixture now extracts each test's local JSON object before
  checking properties, reducing the same check to 3.84 seconds. No production
  C code changed after the broad run.
- GNU release and native musl release each passed all 56 selected read,
  namespace, state-codec, compaction, stream/catalog and native Pouch checks.
  Debug uses the constrained worker-stack policy; musl uses its default stack.
- All 19 selected ThreadSanitizer groups passed.
- Repeated compaction across all four codec combinations and both durability
  policies preserves tombstones, placeholders, object classification, newer
  tail changes and both sequences through reopen. Metadata-only and initialized
  empty namespaces and invalid entry points are covered too.
- All 744 allocation-failure sites and 552 I/O-failure sites are injected,
  including failed reads and writes that persist a partial prefix. Every attempt
  is followed by reopen, value/version checks, retry and another readback.
  Failure may leave a newer physical topology; logical values remain intact.
- A deterministic held copy permits same-namespace META, a peer PUT and reads
  before release. Reclamation retains the original source needed by that META.
  The next pass copies only its 65,569-byte live body and retains the other pack.
- Original and relocated sources across all four codecs detect corruption and
  wrong CRCs, retain a sticky error, reset and clone correctly, and drain after
  Pouch close. Opening and cloning encoded sources perform no payload I/O.
- Body-pack tests reject malformed/overflow labels, foreign bindings and
  unsealed recovery. Packs stay out of replay; source-enumeration callbacks can
  append without deadlock and see their captured source set. Retirement retains
  old pins. Exact reservation tests straddle the first three grant boundaries,
  preserve the published tail during reservation, and reopen correctly.
- Six Memcheck groups completed 1,716,224 matched allocations/frees with zero
  errors, leaks or suppressions, including both failure campaigns.
  Changed-line clang-format verification and `git diff --check` passed.

Evidence uses `build/pouch-space-integrated/maintenance-` prefixes:
`debug-tests.log`, `debug-selection-fixed.log`,
`debug-additional-tests.log`, `gnu-tests.log`, `musl-tests.log` and
`tsan-tests.log`, with matching build logs.
The five `maintenance*-memcheck.log` groups and
`maintenance-bodies-memcheck.log` contain memory-check output.

### Matched compaction and read performance

The 300 serial GNU release runs use five alternating-order samples: 60 actual
compaction runs and 240 ordinary read runs. All container runs audit one fixed
10 MiB file with no sidecars. Inputs are plaintext, delivered in 4 KiB chunks;
OS page cache is retained. Every run verifies values after populated-root reopen.

Compaction fixtures perform two PUT rounds into one source per namespace using
an 8 MiB segment target on both engines. One forced pass per namespace is then
timed separately from PUTs. Native terminal rotation is explicit, its minimum
is one byte, and its throttle and obsolete-file grace are disabled for this
comparison. The four-namespace rows measure four **sequential** compaction
passes after four writers seeded the fixture, not parallel compaction throughput.
Values below are medians.

| Fixture | Policy | C (ms) | MF (ms) | C CPU (ms) | MF CPU (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| 512 × 1 KiB, 1 namespace | NoSync | 44.887 | 54.603 | 6.156 | 12.704 |
| 512 × 1 KiB, 1 namespace | Durable | 40.290 | 55.457 | 5.765 | 13.130 |
| 512 × 1 KiB, 4 namespaces | NoSync | 149.115 | 154.936 | 10.799 | 16.833 |
| 512 × 1 KiB, 4 namespaces | Durable | 174.966 | 178.854 | 10.996 | 21.806 |
| 64 × 32 KiB, 1 namespace | NoSync | 57.182 | 54.797 | 6.307 | 13.143 |
| 64 × 32 KiB, 1 namespace | Durable | 46.592 | 58.735 | 6.666 | 12.691 |

Container CPU medians are lower in all six fixtures. Wall-time ranges overlap
in five of six; in the large durable fixture, container samples span
38.564–48.424 ms and multifile samples 52.956–87.741 ms. This is a small-store
characterization, not a general throughput guarantee.

A container pass performs 15 sync calls under NoSync and 18 under durable
policy, versus 10 multifile. Four sequential passes perform 60/72 versus 40.
Structural maintenance remains durable under either policy. For the small
single-namespace fixture, syscall writes total 990,392 bytes in the container
versus 1,092,930 multifile; for large bodies, 2,197,174 versus 2,168,658.
Extra barriers remain a concrete compaction cost despite the lower CPU time.

Ordinary read runs compare current/prior container and current/prior multifile
executables without invoking maintenance. The prior is the captured-projection
checkpoint. Warm and reopened columns include each method's complete verification
phase. Reopened measurements retain the OS cache and include initial logical
projection loading; they are not cold-device measurements.

| Fixture | Read method | Current C (ms) | Prior C (ms) | Current MF (ms) | Reopened C (ms) | Reopened MF (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1024 × 1 KiB, 1 reader | read-points | 4.448 | 3.792 | 31.353 | 15.749 | 27.716 |
| 1024 × 1 KiB, 1 reader | read-many | 5.747 | 4.182 | 6.287 | 13.873 | 5.880 |
| 1024 × 1 KiB, 1 reader | scan-bodies | 4.218 | 4.133 | 4.592 | 13.735 | 3.496 |
| 1024 × 1 KiB, 1 reader | scan-metadata | 1.367 | 1.314 | 1.135 | 12.180 | 0.933 |
| 1024 × 1 KiB, 4 readers | read-points | 2.160 | 2.078 | 13.489 | 10.635 | 11.862 |
| 1024 × 1 KiB, 4 readers | read-many | 1.639 | 1.366 | 2.370 | 10.160 | 2.481 |
| 1024 × 1 KiB, 4 readers | scan-bodies | 1.593 | 1.389 | 2.473 | 10.064 | 2.225 |
| 1024 × 1 KiB, 4 readers | scan-metadata | 0.706 | 0.651 | 0.610 | 9.195 | 0.605 |
| 64 × 32 KiB, 1 reader | read-points | 4.220 | 3.428 | 6.719 | 9.414 | 6.767 |
| 64 × 32 KiB, 1 reader | read-many | 3.366 | 2.892 | 4.425 | 11.854 | 3.359 |
| 64 × 32 KiB, 1 reader | scan-bodies | 4.036 | 2.637 | 3.149 | 12.553 | 2.619 |
| 64 × 32 KiB, 1 reader | scan-metadata | 0.182 | 0.152 | 0.175 | 7.399 | 0.125 |

The new checksum path adds body-read work. Several warm container medians rise,
including large-body scans from 2.637 to 4.036 ms; current multifile is 3.149 ms.
Current/prior sample ranges overlap, including 2.963–4.679 versus
2.428–3.205 ms for that scan, so these samples do not attribute every timing
change to checksum calculation. Metadata-only visits do not consume body bytes.
The prior reopened-projection deficit remains visible. Neither it nor the
previous single-writer NoSync mutation deficit is declared solved here.

Raw logs are `maintenance-paired/`, with runner `maintenance-paired.sh` and
`maintenance-results.json`. The 305-artifact
`maintenance-artifacts.sha256` manifest verifies; previous 85-artifact projection
and 285-artifact snapshot-codec manifests remain intact and verify.
The measured current executable is `pouch_space_compare-maintenance-current`,
SHA-256 `d6f686b1a9b0540a20e8f737c9996b6d71d6c66a15eaff2acee422042d6d4eda`.
The prior `pouch_space_compare-projection-current` remains
`d0a9475454c63fab72dc81230e74a6b6c709fd55320d76c4b216e53152b2a643`.

## Metadata repacking and near-full growth

Incremental checkpoints leave unchanged descriptors on their original immutable
pages. Replacing hot entries one at a time can therefore retain many sparsely
occupied pages, even when current logical metadata is small. The explicit
`ps_metadata_repack` operation promotes current descriptors into the existing
dirty ownership set and checkpoints them densely. It reuses the directory,
alternate-root publication and old-root retirement protocol; no new persisted
representation, body copy or public ABI is introduced. Ordinary checkpoints
remain incremental. Clean checkpoints no longer require the full metadata
shadow allowance when they allocate no new descriptor or directory pages.

The fragmented fixture keeps 180 small keys and checkpoints their hot
replacements separately. Repack reduces **185 metadata pages to 10**, under both
sync policies, at the same 10 MiB capacity. It includes pending replacements and
tombstones, excludes an unpublished writer, and preserves an old captured body.
A second fixture holds a 3 MiB + 7 byte body, a spanning 16 KiB + 17 byte catalog
name and a sealed stream. Measured repack writes are exactly the new directory
and descriptor pages plus two 80-byte root headers. Reads are bounded by the
directory page allowance; no body is read or copied. Its largest allocation is
bounded by 16 KiB + 256 bytes. Payload allocation, stream/binding identities and
held body, stream and catalog pins remain unchanged by repack. Populated reopen
verifies the same bindings and bytes.

The real Pouch compaction test also repacks its root with captured readers held,
for all four codecs and both sync policies.

Repack exposed a near-full growth bug: the old implementation published its
capacity checkpoint before adding the new free range, so growth could fail
because that checkpoint lacked space. The regression first failed at a 1 MiB
growth with the checkpoint-capacity error. Growth now reserves backing and
privately admits the range under allocator authority before checkpointing. New
metadata may use those pages; the root commits capacity and metadata together.
A healthy failure aborts private pages and removes the range before restoring
old capacity. Uncertain I/O retains the poison/reopen rule. The test proves
metadata writes actually enter the new range; separate checks verify repeated
3→4→5→6 MiB growth and populated reopen after each step.

A healthy failed repack deliberately retains its promoted dirty plan for a later
checkpoint or repack retry. The growth fixture exercises precisely that state.
This is explicit physical maintenance, not automatic scheduling or protected
admission for an entire compaction/delete operation. The fixed maintenance floor
and known-span reservations still do not guarantee full-capacity deletion.

### Verification

- Full Debug ASan/UBSan: **367/367** CTest groups passed in 1310.26 seconds.
- Native GNU release: **19/19** affected groups passed in 173.82 seconds.
- Native musl release: **19/19** affected groups passed in 177.92 seconds.
- ThreadSanitizer: **13/13** affected groups passed in 355.78 seconds, including
  concurrent repack and four ready durable publications.
- Six Memcheck groups passed with **9,276,522 allocations and frees**, no live
  blocks, errors or suppressions, including both complete new failure sweeps.

Repack covers 34 write/sync, 580 allocation and 17 read failures (**631 cuts**).
Growth covers 42 write/sync/reserve, 767 allocation and 21 read failures
(**830 cuts**). Recovery verifies all seeded values and continued publication;
allocation failures also prove retry on the same live owner. Intervening replace
and delete before repack retry exercise dirty-reference ownership. Successful
clean checkpoints add no directory/descriptor page writes. The growth tests
accept either old or new committed capacity after uncertain I/O, with all data
intact. Logs are `repack-debug-tests.log`, `repack-{gnu,musl,tsan}-tests.log` and
the six `repack-*-memcheck.log` files under `build/pouch-space-integrated/`.
The original failing growth check is `repack-growth-before.log`; the repaired
isolated run is `repack-growth-fixed.log`.

### Matched ordinary metadata updates

The 80 serial GNU release runs compare current/prior container and
current/prior multifile executables with five alternating-order samples. Each
seeds 512 plaintext 1 KiB states, times two rounds of ordinary metadata updates,
then verifies values after populated reopen. Four writers use separate namespaces.
Every container run audits one 10 MiB file and no sidecars. OS cache is retained.
These are ordinary-update regression measurements; they do not invoke explicit
repack or measure its latency. Values below are medians in milliseconds.

| Policy | Writers | Current C | Prior C | Current MF | Prior MF | Current C CPU | Current MF CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 63.315 | 65.482 | 50.770 | 54.791 | 26.384 | 29.514 |
| NoSync | 4 | 12.733 | 12.869 | 14.633 | 13.808 | 33.373 | 36.202 |
| Durable | 1 | 1869.695 | 1874.288 | 4677.070 | 4931.556 | 121.560 | 196.709 |
| Durable | 4 | 1501.266 | 1269.218 | 2752.566 | 2852.351 | 173.185 | 217.859 |

Single-writer NoSync remains about 25% slower than current multifile; its sample
ranges overlap (48.032–71.478 versus 39.106–79.344 ms). Current/prior container
ranges overlap in all four fixtures. The four-writer durable median rises about
18%, with broad ranges of 1139.917–1697.949 versus 1086.898–1451.704 ms. These
samples do not establish a cause or justify declaring the regression solved.
Container CPU medians are lower than current multifile in all four fixtures.

Current/prior container syscall-write totals match in each fixture: 342,692
bytes for one writer and 341,576 for four. One writer makes three structural
sync calls in NoSync and 1027 in durable mode, versus zero/1024 multifile. Four
writers make zero NoSync syncs and median 518 durable syncs (516–522), versus
1024 multifile; the prior container median is 519. Extra structural barriers and
the earlier body-read/reopened-projection costs remain open performance work.

Raw logs are `repack-metadata-paired/`, with runner
`repack-metadata-paired.sh`, `repack-metadata-results.json` and
`repack-metadata-benchmark.log`. The 85-artifact
`repack-metadata-artifacts.sha256` manifest verifies. Previous maintenance,
projection and snapshot-codec manifests remain intact and verify. The new
executable is `pouch_space_compare-repack-current`, SHA-256
`33aa8cf62b7c5d8ec29b9db0e799b56b2ddc16b850cedfa5664b83182876a5c4`;
the prior is the maintenance executable recorded above.

## Protected checkpoint workspace at capacity

Incremental checkpoints can retain sparse descriptor pages even after their
logical contents shrink. The previous admission allowance did not guarantee
another complete metadata rewrite after foreground exhaustion: the original
`metadata-capacity` regression failed at explicit repack in a fixed 3 MiB store.
The allocator now keeps a separate free-page pool for checkpoint metadata.
All stored bytes remain inside the same container file.

Let F be the conservative complete packed-descriptor/directory bound in bytes,
and A the bytes of allocated metadata pages. Before admitting payload work,
the pool contains at least `max(F, 2F - A)` free bytes. This protects one complete
rewrite and two metadata images counting the current allocation. The bound
follows logical metadata participation, independent of preallocated capacity.
Free intervals represent the reservation; it creates no capacity-sized bitmap
or buffer. Recovery reconstructs the pool from unowned intervals. The persisted
root/directory/record representation and installed public ABI are unchanged.

When spare common capacity can temporarily fund two free metadata images while
preserving the 16 KiB payload floor, checkpoints use changed records and paths.
Otherwise they promote current descriptors into a dense rewrite through the
existing alternate-root and old-root retirement protocol. Retiring the prior
image restores free workspace for the next checkpoint. Excess dynamic pages
return to the common pool; the fixed 128 KiB metadata region remains protected.
`metadata_available_bytes`, `metadata_workspace_bytes` and `metadata_fallbacks`
expose the free workspace, conservative bound and committed dense fallbacks.

### Capacity and failure evidence

The capacity fixture fragments 180 hot/cold keys, fills unrelated 4 KiB values
until admission stops, then checkpoints/reclaims and fills again. At the same
3 MiB capacity it replaces a spanning 16 KiB + 17 byte catalog name while an old
catalog view remains held, performs 32 appends within an existing stream grant,
and checkpoints after each append with periodic explicit repack. It requires a
dense fallback on the first normal checkpoint, verifies the protected free bound
after every checkpoint, checks held readers and all filled values, and verifies
the stream contents after populated reopen.
Both sync policies pass. Replacing a name previously forecast an extra live
binding; the corresponding red test fails before the accounting correction.
Immutable replacement forecasts likewise subtract the replaced descriptor.

A separate fixture aborts a 1 MiB private writer over 2,000 live keys. A clean
checkpoint returns its excess workspace forecast, restores prior payload
allocation accounting and writes no directory/descriptor page. Another test
holds private grant I/O while a peer fills a descriptor page with a large inline
catalog name, checkpoints, and consumes payload headroom through replacements.
The delayed producer must recheck metadata admission before installing its grant.
It fails cleanly under that pressure; checkpoint/reclamation then permits a
successful retry without growth. Latest value refs survive populated reopen.
Temporarily removing the final recheck makes the test fail at the expected
producer result; the restored implementation passes.

Growth rollback also returns free growth intervals from the protected pool and
trims excess old-capacity workspace before excluding the new range. The stronger
allocation-failure probe first exposed retained old-capacity quota. Two split
nodes are now allocated before growth for rollback and trimming. Both the
post-repack and filled-again profiles prove that healthy allocation failure
preserves old capacity, entries, metadata pages and journal state, permits an
immediate existing-value replacement, and permits growth retry on the same owner.
Uncertain I/O still reconciles through reopen; repeated 1 MiB growth and populated
reopen remain covered. Trailing physical bytes never become admitted capacity
without root publication.

Failure sweeps cover **800 cuts at capacity** (41 write/sync, 738 allocation,
21 read), **633 repack cuts** (34 write/sync, 582 allocation, 17 read), and
**58 growth cuts** (9 write/sync/reserve, 18 allocation and 2 read per profile):
**1,491 cuts** across these fixtures. Recovery checks cover seeded payloads
and continued operation. Complete logical deletion at capacity still requires
atomic reservation of the rotation/catalog/head/payload sequence. Automatic
logical maintenance and history protection remain pending.

Verification includes a full **369-group Debug ASan/UBSan** pass in 1313.50 s,
followed by **23 affected Debug groups** on the final helper initialization and
growth rollback implementation in 206.71 s. Final GNU release **23/23** passes
in 197.60 s; musl release **23/23** in 194.17 s; ThreadSanitizer **17/17** in
340.45 s. Nine Memcheck groups pass with **9,521,363 allocations and frees**,
zero live blocks, errors or suppressions. The final stream checks include the
held-I/O capacity case and its successful retry.

Logs under `build/pouch-space-integrated/` are `workspace-debug-tests.log`,
`workspace-debug-affected-tests.log`, `workspace-gnu-final-tests.log`,
`workspace-musl-tests.log`, `workspace-tsan-tests.log`, their captured `*-details.log`
files and nine `workspace-*-memcheck.log` files. The original capacity/catalog
failures are `workspace-red-tests.log` and `workspace-catalog-red-tests.log`;
`workspace-rollback-red-tests.log` captures the stronger rollback failure.
`workspace-preparation-negative-tests.log` records the deliberate removal of the
publication recheck; `workspace-preparation-final-tests.log` records restoration.

### Matched ordinary metadata performance

The 80 serial GNU release runs use five alternating-order samples of
current/prior container and current/prior multifile. Each seeds 512 plaintext
1 KiB states, times two metadata-update rounds, and verifies values after
populated reopen. Four writers use four namespaces. Every container run audits
one fixed 10 MiB file and no sidecars. OS cache is retained. These ordinary-update
runs characterize regression behavior; they do not invoke explicit repack or
measure the emergency dense-rewrite latency. Values below are medians in ms;
C denotes container and MF denotes multifile.

| Policy | Writers | Current C | Prior C | Current MF | Prior MF | Current C CPU | Current MF CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 47.927 | 52.635 | 42.024 | 42.649 | 23.678 | 22.196 |
| NoSync | 4 | 12.520 | 12.348 | 12.055 | 11.257 | 34.595 | 32.240 |
| Durable | 1 | 1412.175 | 1687.246 | 4050.824 | 3988.425 | 106.093 | 160.875 |
| Durable | 4 | 1405.035 | 1111.451 | 2374.585 | 2599.852 | 143.466 | 161.677 |

Container durable medians remain below current multifile. Single-writer NoSync
is about 14% slower; its ranges overlap (44.813–59.955 versus 38.206–49.738 ms).
Current/prior container write-time ranges overlap in every fixture. The
four-writer durable median rises about 26%, with ranges 1040.453–1538.058 versus
1042.955–1287.586 ms. Recorded `fdatasync` time accounts for almost all of that
median difference: 1386.590 versus 1092.795 ms, while CPU is 143.466 versus
141.966 ms. This series does not isolate the cause of the timing variation.

Current/prior container syscall writes match exactly: 342,692 bytes with one
writer and 341,576 with four. One writer makes three NoSync syncs and 1027 durable
syncs versus zero/1024 multifile. Four writers make zero NoSync syncs and median
518 durable syncs (516–520), versus 1024 multifile; prior container median is 519.
The single-writer NoSync syncs take median 15.184 ms in this series. The structural
barriers remain a concrete performance cost.

The reopened ready phase includes root open plus the first value verification
and logical projection load. These medians retain the OS cache. The container
phase with four namespaces remains slower than multifile; no reopened-projection fix is
claimed here.

| Policy | Writers | Current C ready | Prior C ready | Current MF ready | Prior MF ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 16.734 | 16.579 | 16.566 | 15.886 |
| NoSync | 4 | 37.084 | 34.887 | 14.504 | 14.606 |
| Durable | 1 | 15.064 | 17.608 | 14.557 | 15.866 |
| Durable | 4 | 29.729 | 28.028 | 15.375 | 16.052 |

Raw logs are `workspace-metadata-paired/`, with runner
`workspace-metadata-paired.sh`, `workspace-metadata-results.json` and
`workspace-metadata-benchmark.log`. The **85-artifact**
`workspace-metadata-artifacts.sha256` manifest verifies. All four earlier
repack/maintenance/projection/snapshot-codec manifests remain intact and verify.
The current executable is `pouch_space_compare-workspace-current`, SHA-256
`535221931ba552ba92a8e2d9f7518f6439046c4d0360748564283843d1c5af3f`; prior is
`pouch_space_compare-repack-current`, SHA-256
`33aa8cf62b7c5d8ec29b9db0e799b56b2ddc16b850cedfa5664b83182876a5c4`.

## Operation capacity claims and explicit rotation

The private [budget boundary](../experiments/pouch_space/budget.h) now admits
complete declared capacity before dependent publication. A plan reserves
page-aligned payload bytes, positive descriptor growth and additional live
entries. Ordinary claims preserve the maintenance floor; maintenance claims may
spend it. Let Q be unspent payload claims and D/E be outstanding descriptor/entry
credits. Ordinary payload admission and checkpoint top-ups exclude Q, while the
protected checkpoint bound F includes live metadata plus D/E. This keeps future
metadata protected through peer checkpoints, repack and growth, including while
a private writer has released allocator authority for payload I/O.

Payload credits are spent when physical pages are taken. Descriptor and entry
credits remain outstanding until actual publication under allocator authority;
their removal then offsets the corresponding live metadata increase. Spent
credits are not automatically refunded after a failed or abandoned attempt.
Release returns unspent quota and excess dynamic metadata workspace, using a
preallocated interval split node. Active immutable writers pin their budget;
space close rejects outstanding handles. Claims are process-local and volatile
under the current exclusive root owner. Claim/release performs no I/O and changes
neither the persisted representation nor installed ABI. It provides capacity
admission, not key isolation, rollback, or immunity to memory/I/O failures.
Producers continue outside allocator locks.

Explicit empty namespace rotation now claims a possible routing payload page,
positive routing descriptor growth and the new successor head/name binding
before staging that successor. Existing staged successors avoid those extra
descriptor charges. Predecessor sealing and journal rollover use the protected
checkpoint pool. Failed admission leaves the selected head and successor
directory unchanged. Canonical run grants still use ordinary admission: this
stage does not establish complete automatic append/delete or compaction
admission. The 16 KiB floor also cannot cover every accepted key up to 1 MiB;
protecting the complete bounded canonical work remains required.

### Capacity, concurrency and failure evidence

The new budget fixtures cover typed quota exhaustion, same-shape replacement
without metadata growth, cross-container rejection, busy close, aggregate
overflow and populated reopen with no surviving claims. At 3 MiB, an ordinary
budget reserves a 32 KiB value, a stream head/catalog binding and 512 empty
values. Peers fill foreground capacity, checkpoint, fill again and repack. A
separate maintenance claim then takes the 16 KiB floor. The original owner must
still complete every funded publication and survive reopen under both sync
policies. A private write is checkpointed before its descriptor publishes.

A held-I/O producer lets peers create 512 entries, open/release another claim,
repack and grow by 1 MiB before the original writer completes. Its future
descriptor credits must remain intact through that interval. A fragmented
admission fixture covers **49 allocation failure cuts**, including failures
after partial transfers into the metadata pool, exact quota/workspace cleanup
and retry on the same owner. The namespace fixture fills a fixed 1 MiB store,
rotates, claims remaining free pages, requires another rotation to fail before
staging a successor, releases those claims, retries and verifies populated
reopen. Both root sync policies pass. Existing namespace rotation and snapshot
allocation/write/sync/torn-write sweeps remain enabled.

Two falsification checks distinguish these guarantees. Ignoring held payload
claims makes the capacity fixture fail at the second private 16 KiB write.
Requiring the maintenance floor again when spending a funded ordinary claim
fails at successor head creation after the peer takes that floor. The final
implementation applies the floor only at claim admission and both regressions
pass.

The full **149-group Debug ASan/UBSan** experimental suite passes in 715.11 s
before the final funded-floor refinement. On the final executable behavior,
**27 affected Debug groups** pass in 213.30 s, **27 GNU release groups** in
201.79 s, **27 musl release groups** in 220.02 s and **17 ThreadSanitizer groups**
in 177.50 s. Final assertions additionally exercise overflow of each aggregate
against an existing claim; the budget group passes again in all four
configurations. Four Memcheck runs (budget, budget-capacity and namespace
reservation/faults) complete **301,983 allocations and frees**, with zero live
blocks, errors or suppressions. Debug uses the established 128 KiB worker policy;
musl uses its native worker default.

Logs are under `build/pouch-space-budget/`: `debug-tests.log`,
`debug-final-tests.log`, `gnu-tests.log`, `musl-tests.log`, `tsan-tests.log`, final
`*-test-output.log` captures, and four `memcheck-*.log`/`*-output.log` pairs.
`negative-capacity-tests.log` and `funded-floor-red-tests.log` retain the
deliberate removal and the reproduced funded-floor defect. Final tests cover
the restored protections.

### Matched ordinary metadata performance

The 80 serial release runs repeat five alternating-order samples of current/prior
container and current/prior multifile, each with 512 seeded 1 KiB states and two
metadata-update rounds. Writers use independent namespaces. Cache is retained;
every run verifies values after reopen, and container runs audit one fixed
10 MiB file with no sidecars. This measures ordinary update regression behavior,
not emergency rewrite or capacity-claim latency. Medians below are ms.

| Policy | Writers | Current container | Prior container | Current multifile | Prior multifile | Current C CPU | Prior C CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 55.804 | 47.872 | 44.894 | 45.175 | 26.811 | 23.949 |
| NoSync | 4 | 12.652 | 12.483 | 11.701 | 12.261 | 33.917 | 32.492 |
| Durable | 1 | 1651.678 | 1341.924 | 4049.839 | 4023.512 | 109.907 | 113.168 |
| Durable | 4 | 1196.815 | 1097.344 | 2342.664 | 2505.798 | 146.706 | 141.843 |

Container durable medians remain below current multifile. NoSync is about 24%
slower with one writer and 8% with four. Current/prior container timing ranges
overlap in every fixture. One-writer NoSync rises about 17% versus prior;
CPU ranges overlap at 23.241–33.335 versus 23.278–29.245 ms, but the higher
current median remains an unresolved observation. Durable one-writer time rises
about 23%, with recorded sync medians 1571.945 versus 1260.620 ms and lower
current CPU. Durable four-writer time rises about 9%, with sync medians
1178.093 versus 1078.352 ms. These samples do not isolate a cause or establish
that the remaining performance deficits are solved.

Current/prior container syscall write volumes match exactly: 342,692 bytes
with one writer and 341,576 with four. One writer retains three NoSync barriers
and 1027 durable barriers. Four writers use zero NoSync barriers and median
520 durable barriers (518–524), versus prior median 519 and current multifile
1024. The single-writer NoSync structural barriers remain a concrete cost.

The reopened ready phase includes root open plus first verification/projection
load; four-namespace container medians remain slower than multifile.

| Policy | Writers | Current C ready | Prior C ready | Current MF ready | Prior MF ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 17.478 | 16.054 | 16.140 | 14.054 |
| NoSync | 4 | 36.236 | 33.887 | 16.415 | 15.181 |
| Durable | 1 | 14.564 | 14.514 | 18.171 | 14.947 |
| Durable | 4 | 30.130 | 28.590 | 16.691 | 14.947 |

Raw logs are `build/pouch-space-budget/metadata-paired/`, with runner
`metadata-paired.sh`, `metadata-results.json` and `metadata-benchmark.log`.
The **85-artifact** `metadata-artifacts.sha256` manifest verifies. The current
executable `pouch_space_compare-current` has SHA-256
`e2e306e94836895a95f9166eb2d8e6995b6b765790264fabeffbe1fe4fc211a5`;
prior is the previous stage's frozen `pouch_space_compare-workspace-current`,
SHA-256 `535221931ba552ba92a8e2d9f7518f6439046c4d0360748564283843d1c5af3f`.
The previous workspace manifest also verifies unchanged.

## Funded canonical grants and known body packs

Canonical owned grants, explicit stream reservation and record reservation now
accept the operation budget directly. A grant pins that handle through private
preparation, charges its physical pages when taken, and retains its 88-byte
descriptor and live-entry credits until installation. The final admission check
spends the funded claim instead of trying to admit it again as foreground work.
Peer checkpoints, repacking and payload pressure cannot remove those future
credits. No allocator lock spans producer or preparation I/O.

The preparation-pool optimization remains available. Additional zero preparation
may borrow only unclaimed common space while preserving the maintenance floor;
it cannot borrow another operation's claim or the original operation's remaining
credits. Its spare pages enter the reusable clean pool only after synchronization.
The physical grant charges its own pages, not those temporary spare pages.

Typed helpers now compose new-head, catalog, routing and exact-span costs without
physical-format arithmetic in the namespace coordinator. Successful composition
uses scalar arithmetic and no workspace allocation. Failed composition leaves
the plan unchanged and uses ordinary error reporting. For an exact fresh span,
the conservative bound allows every grant to fragment down to one 4 KiB page,
including its 64-byte identity header. It reserves one descriptor/entry per
possible page without creating a capacity-sized array. Existing grants can reduce
actual spending. Ordinary geometric over-reservation needs additional allowance;
the span helper funds `ps_stream_reserve_exact`. A cached stream retains no
budget: further automatic growth still uses ordinary admission.

A measured body-pack build now admits its complete head, exact payload span,
future grant descriptors and final catalog binding before creating the stream.
The builder holds that ordinary claim through production and publication and
releases it after abort/close. It preserves the standing maintenance floor.
This completes admission for a known body pack; it does not yet fund its parent
compaction's earlier rotation, metadata snapshot, head selection or reclamation.
Automatic append/delete admission and a bounded maintenance-work policy for
accepted 1 MiB keys remain required. Neither persisted format nor installed ABI
changes in this stage.

### Correctness and failure evidence

The grant fixtures reject undersized payload, descriptor and entry quotas,
foreign-container claims and invalid grant bases without allocation/publication.
Planner tests cover composition, zero spans, bounds and atomic overflow failure.
At fixed 1/3 MiB capacities, both sync policies complete a 98,321-byte exact span
and catalog publication after peers exhaust foreground capacity and claim the
16 KiB floor. The fragmented profile requires more than ten grants and verifies
all bytes after populated reopen. A held-I/O grant keeps its descriptor/entry
credits visible while peers publish 512 entries, repack, fill and checkpoint.
Close rejects that in-flight budget, and the grant then completes successfully.

The body fixture reserves a 65,537-byte pack, rejects a further oversized build
before creating its stream, then lets peers fill the store and claim the floor.
The admitted pack must still publish its binding and remain readable after
reopen, while the namespace's earlier canonical record stays unchanged.
Temporarily removing the budget from the final catalog call makes publication
fail under this pressure; restoring it passes.

Failure sweeps cover **204 funded grant cuts** (164 write/sync, 40 allocation)
across fallback/native zero preparation and both sync policies, plus **187 body
build cuts** (87 write/sync, 100 allocation): **391 cuts**. They exercise partial
preparation, header/publication/torn writes, seals and catalog publication,
old-prefix preservation, uncertain recovery and continued operation. Healthy
allocation failures additionally admit another reservation/private build on the
same open owner before close; populated reopen and subsequent publication remain
checked separately.

The implementation passes **34 affected Debug ASan/UBSan groups** in 247.31 s,
**34 GNU release groups** in 239.94 s, **34 musl release groups** in 231.34 s and
**23 ThreadSanitizer groups** in 222.87 s. After adding the same-owner retry
assertions, the two fault groups pass again in Debug/GNU/musl/TSAN in
17.77/15.29/15.99/42.16 s. Four final Memcheck groups complete **119,213
allocations and frees**, with zero live blocks, errors or suppressions. Debug
retains the established 128 KiB worker policy and musl uses its native default.

Logs under `build/pouch-space-grant-budget/` include `debug-tests.log`,
`gnu-tests.log`, `musl-tests.log`, `tsan-tests.log`, captured `*-test-output.log`,
the final `*-retry-tests.log`/`*-retry-output.log` runs and four `memcheck-*.log`
pairs. `negative-tests.log` preserves the removed catalog-budget regression.
Changed-line formatting and local documentation links verify.

### Matched forced-compaction performance

The 80 serial GNU release runs use five alternating-order samples of
current/prior container and current/prior multifile. Each writes 512 plaintext
1 KiB states over two PUT rounds, then forces one covered prefix per namespace.
Both backends use an 8 MiB segment target, disabled throttling and completed
reclamation. One or four writer threads prepare one or four namespaces;
**the compaction phase visits those namespaces serially**, not as concurrent
compactors. Cache is retained. Every run verifies values after compaction and
reopen; container runs audit one fixed 10 MiB file with no sidecars. This does not
measure emergency admission latency or establish complete-at-capacity compaction.
Medians are ms; C denotes container and MF denotes multifile.

| Root policy | Namespaces | Current C compact | Prior C compact | Current MF compact | Prior MF compact | Current C CPU | Prior C CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 50.468 | 46.559 | 49.993 | 48.294 | 5.952 | 5.481 |
| NoSync | 4 | 126.511 | 140.885 | 144.228 | 155.600 | 9.976 | 9.649 |
| Durable | 1 | 40.216 | 44.518 | 61.672 | 49.080 | 6.236 | 6.551 |
| Durable | 4 | 154.768 | 151.413 | 162.741 | 185.369 | 12.182 | 10.915 |

Current/prior container compaction ranges overlap in every fixture. NoSync
one-namespace median rises about 8%, with ranges 43.764–63.766 versus
38.205–55.398 ms and overlapping CPU ranges. Four-namespace durable median rises
about 2%; its CPU medians rise about 12%, with ranges 9.576–14.516 versus
9.287–15.259 ms. Other container wall medians decrease. Multifile durable
one-namespace time also varies substantially between control builds, with sync
medians 49.466 versus 34.262 ms. These samples do not isolate a timing cause or
establish that earlier NoSync, checksum/read or reopened-projection deficits are
resolved.

Current/prior container compaction I/O matches exactly in every run: **990,392
bytes and 250 writes/626 reads** for one namespace; **1,025,532 bytes and
355 writes/645 reads** for four. The container retains 15/60 compaction barriers
under NoSync and 18/72 under durable roots, versus multifile 10/40 in either
policy. Claims introduce no additional measured I/O or barriers. Current MF
compaction writes median 1,092,930/1,095,216 bytes. Container time remains largely
sync time; root NoSync policy does not remove structural compaction barriers.

Post-compaction ready includes root open plus first verification/projection load.
The current fixture benefits from its published compacted snapshot; it is distinct
from the earlier ordinary-metadata reopened-projection measurements.

| Root policy | Namespaces | Current C ready | Prior C ready | Current MF ready | Prior MF ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 9.549 | 7.342 | 14.339 | 14.513 |
| NoSync | 4 | 8.079 | 8.368 | 14.180 | 14.737 |
| Durable | 1 | 7.705 | 9.618 | 14.839 | 14.211 |
| Durable | 4 | 10.739 | 8.796 | 15.354 | 17.529 |

Raw logs are `build/pouch-space-grant-budget/compaction-paired/`, with runner
`compaction-paired.sh`, `compaction-results.json` and `compaction-benchmark.log`.
The **85-artifact** `compaction-artifacts.sha256` manifest verifies, as does the
previous budget-stage metadata manifest. Frozen current executable
`pouch_space_compare-current` has SHA-256
`9681ddeafe57c4dfbeb43cca1cabcb08d1ab581e18e43d825e349bb538a13711`;
prior is `build/pouch-space-budget/pouch_space_compare-current`, SHA-256
`e2e306e94836895a95f9166eb2d8e6995b6b765790264fabeffbe1fe4fc211a5`.

## Funded namespace append dependencies

Namespace append admission now plans the known canonical span, a possible new
successor/head binding, and routing selection before staging that successor.
The append owns its capacity claim until its private writer closes, including
through final head publication. Peer payload pressure and future metadata growth
cannot consume those declared dependencies. Producers continue outside allocator
and root locks. Neither the persisted format nor installed ABI changes.

The private boundary distinguishes a known prefix, a complete positive span and
a complete maintenance span. Production metadata batches declare their complete
size and preserve the standing floor. Only delete-only batches request maintenance
admission. Streaming PUTs declare their header/key prefix and acquire further
grants through ordinary admission as input arrives. Complete modes reject an
oversized vectored call before writing any part, reject commit after an overrun
attempt and require the exact declared byte count before publication. An
underfilled append can still be completed and committed.

Planning subtracts capacity already owned by the selected stream. A cached append
that needs neither a new grant nor head selection allocates no budget handle.
Ordinary claims fund a conservative geometric-growth envelope, with exact-span
fallback on insufficient capacity; allocation and I/O errors are not treated as
capacity retries. Maintenance reserves exact grants. A stream remembers the
largest grant's growth preference, capped at 1 MiB and reconstructed on open, so
a small clipped maintenance grant does not reset subsequent ordinary growth.
Typed planning uses scalar arithmetic and no capacity-sized workspace. Its
descriptor/entry bound still permits worst-case one-page fragmentation; only the
payload allowance grows to cover possible geometric overshoot.

### Correctness and failure evidence

The new namespace fixture covers invalid modes and zero complete spans,
all-or-nothing overrun rejection, an extra write after an exactly filled span,
underfill/continue/commit, and the zero-budget path for existing owned capacity.
The existing reservation test admits a 1 MiB prefix after growth to 2 MiB,
forcing the conservative preferred envelope to fall back to exact admission.
At fixed 1 MiB capacity under both root policies, peers exhaust foreground space
and claim the standing floor after an append is admitted. That append must still
publish its head. A new ordinary append fails before staging; a subsequent
bounded maintenance rotation succeeds and remains readable after reopen.
Temporarily removing the head publication's budget makes this pressure test
fail; restoring it passes.

The public mutation-capacity fixture now requires both small-key tombstones to
rotate, commit and survive reopen after filling foreground capacity in the
10 MiB store with **115 pressure-namespace keys and one peer key**. It no longer
accepts capacity rejection for that bounded profile. This
does not guarantee deletion for every accepted 1 MiB key, arbitrary future
metadata thresholds or a complete parent compaction; the standing-work policy
and joint snapshot/compaction admission remain required.

Failure sweeps cover **356 cuts: 146 write/sync and 210 allocation failures**,
across both root policies and initial complete/rotating maintenance appends.
They include uncertified zero preparation, torn writes, producer output and
tail/head publication. Every cut releases its quotas and recovers a whole prior
or newly published prefix, then permits another append/checkpoint. Healthy
allocation failures also preserve the old prefix and admit another attempt on
the same open owner before close. Raw planner tests cover atomic overflow and
dense/fragmented funded preferred allocation, while a clipped-grant test fills,
checkpoints, reopens and verifies the next grant still grows to 1 MiB.

The full native **295-group Debug unit suite** passes in **1261.98 s**, with
ASan/UBSan and the established constrained worker-stack checks. After tightening
the public capacity assertion, that group passes again in 1.22 s. The final
**47 affected GNU release groups** pass in **128.41 s**, **47 musl groups** in
**129.61 s** and **19 ThreadSanitizer groups** in **104.76 s**. Musl uses its
native default worker stack. Five Memcheck runs (raw planning, namespace
admission/faults and public mutation/capacity) complete **206,700 allocations and
frees**, with zero live blocks, errors or suppressions.

Logs under `build/pouch-space-append-budget/` include `debug-tests.log`,
`debug-capacity-tests.log`, `gnu-tests.log`, `musl-tests.log`, `tsan-tests.log`,
their captured `*-output.log` files and five `memcheck-*.log`/output pairs.
`negative-tests.log` preserves the removed head-budget regression.

### Matched ordinary metadata-update performance

The 80 serial GNU release runs use five alternating-order samples of
current/prior container and current/prior multifile. Each seeds 512 plaintext
1 KiB states, measures two metadata-update rounds with one/four writers under
NoSync/durable roots and verifies values after reopen. Cache is retained. All
container runs audit one fixed 10 MiB file with no sidecars. This measures
ordinary updates, not emergency admission latency. Medians are ms; C denotes
container and MF denotes multifile.

| Root policy | Writers | Current C write | Prior C write | Current MF write | Prior MF write | Current C CPU | Prior C CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 52.846 | 52.082 | 42.283 | 44.254 | 27.141 | 26.712 |
| NoSync | 4 | 12.723 | 12.337 | 12.147 | 13.233 | 34.294 | 33.554 |
| Durable | 1 | 1541.543 | 1497.416 | 3913.366 | 4164.097 | 111.939 | 111.985 |
| Durable | 4 | 1034.767 | 1111.457 | 2115.277 | 2324.844 | 143.197 | 144.947 |

Current/prior container write medians change about +1.5%, +3.1%, +2.9% and
-6.9%; their wall and CPU ranges overlap in every fixture. The one-writer
NoSync ranges are 47.573–64.711 versus 50.440–88.273 ms; CPU ranges are
24.315–32.825 versus 25.147–29.261 ms. These samples do not isolate a new timing
cost. Current NoSync remains about **25%/5% slower than current MF** with
one/four writers; durable container medians remain lower than MF.

Current/prior container syscall write volumes match exactly: **342,692 bytes
and 1040 writes** with one writer; **341,576 bytes and 1024 writes** with four.
One-writer container retains three NoSync and 1027 durable `fdatasync` calls;
MF uses two `fsync` calls plus zero/1024 `fdatasync` calls. Four-writer NoSync
uses no barriers in either backend. Durable container batches median **519**
barriers (518–521), versus prior 519 (517–520) and MF 1024. Claims add no
measured write volume or fixed barrier. Structural NoSync costs remain open.

Ready includes root open plus the first verification/projection load. The
four-namespace container remains slower than MF. One-writer durable ready median
increases about **31%** versus prior; ranges 14.498–21.623 versus
13.743–16.975 ms overlap, but this remains an unresolved observation. These
results do not establish that reopened-projection or body-read costs are solved.

| Root policy | Writers | Current C ready | Prior C ready | Current MF ready | Prior MF ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 16.029 | 14.277 | 15.556 | 15.767 |
| NoSync | 4 | 33.632 | 38.968 | 16.833 | 15.517 |
| Durable | 1 | 20.094 | 15.378 | 14.576 | 16.579 |
| Durable | 4 | 26.956 | 28.739 | 17.454 | 16.483 |

Raw logs are `build/pouch-space-append-budget/metadata-paired/`, with runner
`metadata-paired.sh`, aggregation script `metadata-summary.pl`,
`metadata-results.json` and `metadata-benchmark.log`. The **86-artifact**
`metadata-artifacts.sha256` manifest verifies, as does the prior stage's
85-artifact compaction manifest. Frozen current executable
`pouch_space_compare-current` has SHA-256
`3359eb73cfd6de024ecd98327163fd79853616f123efc6d0d1d7aa7abeaf6dff`;
prior is `build/pouch-space-grant-budget/pouch_space_compare-current`, SHA-256
`9681ddeafe57c4dfbeb43cca1cabcb08d1ab581e18e43d825e349bb538a13711`.

## Joint snapshot and body publication

Snapshot begin now captures the current prefix and runs a metadata-only planning
callback before rotating or creating outputs. Production maintenance captures
the resolved projection once, selects a source from the logically covered prefix
and measures its metadata/body frames without reading bodies. A source about to
be sealed is eligible without staging the rotation first. One maintenance claim
then funds any rotation, both stream heads and exact grants, their catalog
bindings and final routing replacement. Checkpoint workspace protects seals and
reclamation. Capacity rejection precedes topology changes.

The builder owns its optional body pack and retains the joint claim through
publication and writer close. It checks both exact byte counts and failed-output
flags before publishing either binding. Producers continue outside namespace
mutation authority and allocator/root locks. Encoded bodies still flow through
bounded scratch without decoding, re-encryption, spooling or materialization.
The private snapshot API now requires a planner; the former separate span-reserve
call is removed and all callers migrate together. Standalone body builds remain
ordinary positive-span claims and now reject oversized output before writing.
Neither persisted format nor installed ABI changes.

The codec supplies exact per-record snapshot sizing, reused by aggregate
snapshot/body measurement. It handles high-water records, references, inline
encoded bodies, objects, placeholders and tombstones using metadata only. It
validates field/offset bounds and clears its size output on failure. Existing
codec tests compare measured sizes with actual decoded record boundaries across
96 transform/durability/file-or-stream/object-or-state/size configurations,
with six records per configuration.

### Correctness and failure evidence

At fixed 1 MiB capacity under both root policies, a joint metadata/body build is
admitted before peers fill foreground space and claim the standing 16 KiB floor.
Its **65,537-byte opaque-body fixture**, both catalog bindings and final head
selection must still complete. Reclamation/checkpoint and populated reopen
preserve both outputs. An oversized plan and a subsequent unadmitted build leave
entry/journal counts and the rejected namespace's topology unchanged.

Boundary tests reject zero/overflow plans and zero standalone body spans. Both
outputs reject an oversized vectored call atomically and reject an extra write
after exactly filling the declared span. The other output is completed too, so
rejection cannot merely be an underfill check; neither body nor snapshot may
publish. An underfilled joint producer is rejected and can complete successfully.
Temporarily removing the final head publication's budget makes the pressure
fixture fail; restoring it passes.

The new joint-operation sweep covers **668 cuts: 262 write/sync and 406 allocation
failures**, across both root policies and initial/replacement snapshots. It
reopens before the attempt to exercise uncertified zero preparation, checks
producer and body/catalog/head publication failures, releases all quotas, and
recovers a whole old or newly selected prefix. Successful selection is durable
under both policies. Healthy allocation failures additionally admit another
attempt on the same open owner before close. Recovered selected bodies remain
readable, and subsequent append/checkpoint succeeds. Codec sizing adds **18
allocation cuts**, including null error outputs, plus invalid/overflow checks.
Existing public maintenance recovery/concurrency tests exercise real canonical
body references and liveness-based reclamation.

A sync-ordering regression was found before finalizing the stage: finishing a
deferred predecessor seal before body publication used **nine barriers** for a
NoSync snapshot with a newer tail rotation. Restoring body publication before
seal cleanup lets its dependency barrier stabilize pending routing too. The
regression now measures **eight barriers under both policies** and verifies
replay after reopen. This retains the previous publication order while using the
joint claim; it does not remove the underlying structural barriers.

The full native **297-group Debug unit suite** passes in **1039.82 s**, with
ASan/UBSan and constrained worker-stack checks. After the experimental-only
barrier-order repair, all **36 affected Debug groups** pass in **215.79 s**;
the unchanged shared codec/native-library checks retain the full-suite result.
Both public API/shared-symbol boundary checks pass in **0.09 s**. Final
**36 GNU release groups** pass in **163.11 s**, **36 musl groups** in
**162.41 s**, and **19 ThreadSanitizer groups** in **179.30 s**. After strengthening
the other-output completion assertion, that group passes again in
Debug/GNU/musl/TSAN in 1.51/1.45/1.45/1.51 s. Musl retains its native default
worker stack. Five final Memcheck runs complete **409,909 allocations and frees**,
with zero live blocks, errors or suppressions.

Logs under `build/pouch-space-snapshot-budget/` include `debug-tests.log`,
`debug-final-tests.log`, `api-tests.log`, `gnu-final-tests.log`,
`musl-final-tests.log`, `tsan-tests.log`, the four `*-limits-tests.log` runs,
captured output logs and five `memcheck-*.log`/output pairs.
`negative-tests.log` records the removed head-budget control;
`barrier-before-tests.log` records the nine-barrier regression.

### Matched forced-compaction performance

The main 80 serial GNU release runs use five alternating-order samples of
current/prior container and current/prior multifile. Each writes 512 plaintext
1 KiB states over two PUT rounds, then forces compaction with an 8 MiB segment
target, disabled throttling and completed reclamation. One/four writer threads
prepare one/four namespaces; **compaction visits those namespaces serially after
the writers join**. Cache is retained. Every run verifies values after compaction
and reopen; container runs audit one fixed 10 MiB file without sidecars. This
does not measure emergency admission latency or prove that every compaction fits
when starting at full capacity. Medians are ms; C denotes container and MF
denotes multifile.

| Root policy | Namespaces | Current C compact | Prior C compact | Current MF compact | Prior MF compact | Current C CPU | Prior C CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 52.483 | 53.053 | 51.937 | 51.586 | 7.497 | 7.472 |
| NoSync | 4 | 141.892 | 141.587 | 180.226 | 155.989 | 12.893 | 11.036 |
| Durable | 1 | 47.753 | 48.685 | 56.259 | 55.162 | 8.386 | 7.387 |
| Durable | 4 | 166.297 | 159.817 | 174.463 | 173.311 | 13.484 | 13.351 |

The main durable four-namespace container median increases about **4.1%**;
ranges **162.916–189.445 versus 142.422–161.503 ms** do not overlap. Recorded
sync medians also rise, from 150.090 to 159.214 ms, while CPU ranges overlap.
One focused 20-run follow-up repeats only that profile with the same frozen
executables and all four controls, reversing the starting order of odd samples.
All 20 runs verify. The wall-time difference does **not** repeat: container
ranges 141.230–204.200 versus 148.528–177.977 ms overlap. Both series are retained.

| Follow-up engine | Current compact | Prior compact | Current CPU | Prior CPU |
| --- | ---: | ---: | ---: | ---: |
| Container | 165.588 | 171.172 | 13.908 | 11.169 |
| Multifile | 164.577 | 163.983 | 19.933 | 18.850 |

CPU medians are higher in some profiles: about 17% for main NoSync/four
namespaces, 14% for main durable/one namespace and 25% for follow-up
durable/four namespaces. Their ranges overlap; these samples do not isolate a
cause or establish that earlier performance deficits are resolved. Compaction
wall time remains largely sync time.

Current/prior container compaction I/O matches across both series' **50 container
runs** within the 100-run comparison:
**990,392 bytes, 250 writes and 626 reads** with one namespace;
**1,025,532 bytes, 355 writes and 645 reads** with four. Container retains
15/60 NoSync and 18/72 durable compaction barriers, versus MF 10/40 under either
root policy. The joint claim adds no measured I/O or fixed barrier here.
Current MF writes 1,092,930/1,095,216 bytes in the main series.

Ready includes root open plus the first verification/projection load after
compaction. Main current/prior ranges overlap; the follow-up four-namespace
container medians are 7.761/9.881 ms. This fixture benefits from a compacted
snapshot and does not replace the earlier ordinary-metadata reopen measurements.

| Root policy | Namespaces | Current C ready | Prior C ready | Current MF ready | Prior MF ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 11.514 | 9.715 | 16.328 | 16.204 |
| NoSync | 4 | 9.291 | 8.446 | 16.102 | 16.178 |
| Durable | 1 | 10.400 | 8.697 | 25.875 | 16.910 |
| Durable | 4 | 10.794 | 9.084 | 16.840 | 16.052 |

Raw series are `build/pouch-space-snapshot-budget/compaction-paired/` and
`compaction-durable4-paired/`, with matching runner, result JSON and benchmark
logs. `compaction-summary.pl` aggregates either series. The **109-artifact**
`compaction-artifacts.sha256` manifest verifies, as does the prior stage's
86-artifact metadata manifest. Frozen current executable
`pouch_space_compare-current` has SHA-256
`5df62ab9bf7b7e6261e38b8405a8df23c258643b692c526b79e8bc338d764a90`;
prior is `build/pouch-space-append-budget/pouch_space_compare-current`, SHA-256
`3359eb73cfd6de024ecd98327163fd79853616f123efc6d0d1d7aa7abeaf6dff`.

## Next commitments

The [integrated foundation](../experiments/pouch_space/DESIGN.md) now replaces
the fixed 64 KiB checkpoint banks with growable typed metadata, packed descriptor
pages, protected checkpoint workspace and a fixed payload reserve for maintenance.
Earlier
measurement tables above identify their implementation stages, including the
current payload packing, canonical-stream, lifecycle, typed catalog and production
collection and ordinary state record follow-ups. Namespace segments now have a real
container owner. Ordinary production state PUT/projection/point-read, metadata
update, deletion, read-many, metadata visits and paged scan/body snapshots now
join it through the private integration harness. Physical namespace snapshot
publication and protected obsolete-source reclamation are now available too.
Forced ordinary-state compaction now joins these owners, with separate sealed
body packs and a current-tail liveness check before reclamation. Its first
matched performance series and checksum/read tradeoff are recorded above.
Explicit metadata repacking and near-full checkpoint funding during growth now
join that foundation, with the failure and ordinary-update measurements above.
The checkpoint-only pool and dense fallback now preserve metadata progress after
foreground exhaustion, with final capacity/recovery/race/memory checks above.
Volatile capacity claims and explicit rotation now join that foundation, with
funded-writer, quota rollback, concurrency and matched performance evidence above.
Funded canonical grants, typed planning and complete known body-pack admission
now join those claims, with same-owner retry, capacity, failure and forced-compaction
comparison evidence above. Namespace appends now fund their declared spans and
head selection before staging, with bounded-key full-capacity deletion and
matched ordinary-update evidence above. Snapshot and body publication now use a
joint measured claim before topology changes, with capacity, failure and matched
compaction evidence above. The next work must protect standing maintenance
capacity for all admitted key sizes and required progress at full capacity,
address remaining NoSync, checksum/body-read and reopened-projection costs, and
connect advanced mutations, public query-client and collection ownership, then
complete the public behavior suites. Full native integration still requires
checkpoint scheduling, automatic compaction/retention policy,
process-shared reader ownership, and complete public Pouch semantics. The current
foundation permits rebinding a deleted physical key; it does not supply permanent collection
position or retry authority. Full codec/index/XA/outbox integration remains part
of the authorized native implementation; existing-root conversion is excluded.
Production adoption still requires the
broader [draft acceptance evidence](pouch-space.md).

## Cleanup durability scopes and cold crypto initialization

This checkpoint is `92df5ee`. The [following deferral stage](#scoped-canonical-commit-deferral)
addresses the durable scope slowdown measured here.

The private storage boundary now supplies a root flush capability. Explicit
cleanup barriers and scope completion stabilize the completed container prefix
under either policy, including mutations completed before the scope began.
Namespace inclusion checks authority and storage health without opening a
namespace path. The physical readiness coordinator excludes private producers;
an already covered prefix needs no further file sync. Scope completion preserves
an earlier operation error while still attempting durability and releasing its
thread-local ownership. This supplies cleanup's durability boundary, not complete
retention/transaction cleanup or deferred durable container commits.

The concurrent benchmark exposed OpenSSL initialization behavior that the
earlier fixtures, which generated a crypto key before worker startup, did not
exercise. Prepared uninstrumented OpenSSL reported an RCU hashtable access race.
Building the checksum-verified pinned 3.6.5 source with ThreadSanitizer separately
exposed a first-allocation race on `CRYPTO_malloc`'s `allow_customize` flag.
Pouch runtime admission now calls the normal explicit initialization API before
admitting record writers, including plaintext roots which still hash bodies.
Initialization failure returns a null handle before root creation. A diagnostic
constructor established the candidate repair; the product initializes at runtime
admission and contains no such constructor.

### Correctness evidence and remaining defects

All **12 focused groups** pass under Debug ASan/UBSan in **4.27 s**, GNU release
in **3.59 s**, and native musl release in **4.14 s**. Debug uses 128 KiB worker
stacks with the glibc stack cache disabled; musl uses its default worker stack.
The fixtures cover all four codec combinations, both policies, pre-scope
mutations, grouped PUT/META/DELETE, explicit/completion/covered barriers, scope
allocation failure, allocation-free flush, initialization failure/retry and
sync-error precedence. An unpublished append in another namespace remains open
through all barriers without changing its selected tail. Populated reopen
checks bodies, metadata and the deleted key. The new scope test linked against
the preceding integration fails at its namespace-file synchronization path.

The complete **308-group Debug unit suite** passes in **1038.71 s**. Nine
additional public API, shared-symbol, library-unload and worker-stack lifecycle
checks pass in **7.95 s**. The full-suite log is `debug-full-unit.log`, and
`api-lifecycle.log` records the boundary/lifecycle checks. The installed API and
canonical record format are unchanged.

With the actual product initialization change and fully instrumented OpenSSL,
**40 concurrent comparison runs plus four focused groups** pass without
suppressions. These use five repetitions of both engines, both policies and
fresh/reopened roots, with two writers. The strict runner stops on the first
failure. Four Memcheck runs complete **50,302 allocations and frees** with zero
live blocks, errors or suppressions. Logs and diagnostic controls remain under
`build/pouch-space-durable-scope/`, including `debug-init-focused.log`,
`gnu-init-focused.log`, `musl-init-focused.log`, `tsan-runtime-init.log` and
`memcheck-*.log`.

A separate probe found an existing shared canonical defect: PUT, DELETE, then
PUT reports successful recreation at version 1/index sequence 3, but immediate
and reopened reads remain absent. It reproduces in both backends under both
policies using the preceding release libraries. The prior recreation fixture
checked the mutation result without reading that body. The scope fixture keeps
its deleted key absent and uses another key for later writes. Recreation
readback, including compaction and objects, remains a required correctness fix;
the [implementation contract](pouch-space-implementation.md#known-canonical-recreation-defect)
records the conflict with stale-generation replay protection.

### Matched scope and ordinary-write measurements

The final GNU binary and frozen preceding binary run **120 verified trials**:
five alternating-order samples, both policies, one/four namespace writers,
128 one-KiB states and two PUT rounds with 4 KiB producer chunks. Each fixture
compares current container/multifile scopes and current/prior ordinary writes
in both engines. A scope encloses both rounds in each writer; total elapsed time
includes its final barrier, while individual PUT samples exclude that barrier.
Every container trial retains one 10 MiB file, with no persistent sidecars.
Caches are retained; these are small native fixtures, not cold-device or large
machine acceptance measurements.

| Policy | Writers | Container scope ms | Multifile scope ms | Container CPU ms | Multifile CPU ms | Container / multifile syncs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 15.401 | 22.424 | 5.914 | 5.831 | 3 / 3 |
| NoSync | 4 | 21.889 | 21.781 | 9.590 | 11.004 | 6 / 12 |
| Durable | 1 | 832.383 | 22.368 | 31.022 | 7.520 | 259 / 3 |
| Durable | 4 | 606.924 | 31.649 | 47.473 | 11.630 | 137 / 12 |

These expose a required integration gap: the container retains per-mutation
durable synchronization inside a scope, while native multifile groups defer it
until scope completion. At one writer, 259 versus three barriers makes the
container about 37 times slower; at four writers the measured ratio is about
19. This stage supplies forced NoSync durability but does not satisfy grouped
durable performance acceptance. Implementing deferred group publication without
weakening unrelated commits or blocking private producers remains required.
Four-writer durable container scope barriers range from 135 to 140 as the
existing coordinator coalesces ready publications.

Ordinary container controls, shown as current / prior medians, retain exactly
403,803 syscall-written bytes with one namespace and 407,092 with four:

| Policy | Writers | Write ms | CPU ms | Create plus write ms |
| --- | ---: | ---: | ---: | ---: |
| NoSync | 1 | 8.521 / 9.592 | 6.131 / 6.025 | 27.137 / 26.794 |
| NoSync | 4 | 7.140 / 7.086 | 9.478 / 10.051 | 26.572 / 23.944 |
| Durable | 1 | 806.150 / 716.895 | 32.970 / 33.414 | 833.224 / 731.219 |
| Durable | 4 | 491.221 / 620.048 | 43.888 / 45.388 | 517.910 / 646.116 |

Current/prior ranges overlap for write time, CPU and create-plus-write time in
all four container fixtures. Normal NoSync barriers remain two/four; durable
one-writer remains 259, and four-writer medians are 135/137 with overlapping
135–137/135–138 ranges. Explicit crypto initialization moves first-use work into
open, so create-plus-write is included rather than interpreting write time alone
as a gain. Two scope trials also have large creation stalls: create-plus-write
maxima reach 669.790/573.475 ms for NoSync one/four writers. These samples do not
establish an ordinary-write timing regression or improvement.

Ordinary container peak-RSS medians in table order are 8,220 / 9,036 KiB,
8,588 / 9,440 KiB, 8,244 / 8,784 KiB and 8,404 / 9,308 KiB for current / prior.
These process measurements include libraries and worker stacks; this small
fixture does not establish large-store memory scaling.

Reopened ready time includes root open and the first full verification of the
latest values. Its scope medians still show a container deficit:

| Policy | Writers | Container ready ms | Multifile ready ms |
| --- | ---: | ---: | ---: |
| NoSync | 1 | 11.563 | 5.246 |
| NoSync | 4 | 27.610 | 4.848 |
| Durable | 1 | 12.434 | 4.305 |
| Durable | 4 | 24.865 | 4.689 |

The current ELF is `build/pouch-space-durable-scope/pouch_space_compare-current`,
SHA-256 `d948e25939a9ada5e2ef0ea4847d483fc86a1bd45b0809959f646d91f686a1b0`.
The prior is the frozen joint-snapshot ELF with SHA-256
`5df62ab9bf7b7e6261e38b8405a8df23c258643b692c526b79e8bc338d764a90`.
The stage retains `scope-paired.sh`, `scope-summary.pl`, `results.json`,
`benchmark.log` and all 120 raw logs under `paired/`. The performance checksum
manifest covers both ELFs, runner, summarizer, summary, benchmark log and raw
trials. These results do not close full backend correctness or performance
acceptance; maintenance capacity architecture, grouped durable commits,
recreation and the broader integration requirements remain open.

## Scoped canonical commit deferral

This checkpoint is `45da71c`. The [following readiness stage](#deferred-stream-repair-and-reopen-readiness)
addresses its remaining stream-open repair cost.

Canonical append acknowledgement now has an explicit per-call deferral mode.
The logical owner selects it only inside the current thread's forced cleanup
scope for that Pouch instance. Completed bytes enter the existing readiness
prefix and become individually visible; the scope's explicit/completion barrier
forces that prefix durable under either root policy. Ordinary commits retain
their policy, and structural publication/preparation barriers remain enforced.
An independent ordinary writer still completes while a scope stays open. Its
barrier may also cover the scope's completed prefix. There is no new writer
queue, root-wide policy toggle, persisted record or public API change.

### Verification

The full standard Debug preset passes **361/361 checks** in **1327.32 s**,
including **309 unit groups**, public API/shared-symbol boundaries and worker
stack checks. ASan/UBSan workers use 128 KiB stacks with the glibc stack cache
disabled. The fourteen affected GNU release and native musl groups pass in
**13.75 s** and **11.78 s**; musl uses its default worker stack.

The scope fixture covers all four codec combinations and both root policies:
PUT/META/DELETE visibility before a barrier, pre-scope mutations, explicit and
completion barriers, already covered work, allocation-free flush, invalid commit
modes, a pinned old body across peer publication and ordinary peer durability
while the scope stays open. A later producer failure preserves earlier complete
appends, leaves the failed mutation unapplied and retains its original error at
scope completion. An unpublished producer keeps its old selected tail through
all barriers. Sync-failure coverage expands to **24 codec/policy/phase cases**.

Another **24 process-death cases** reopen a separate test image updated only by
successful root sync callbacks. Before a scope barrier, that image retains the
prior state; after explicit or completion barriers, it retains completed PUT,
metadata and deletion state. This models loss of unsynced writes without relying
on kernel page-cache survival. The image belongs to the test fixture, not the
store layout; production still writes one container. These deterministic
recovery checks supplement existing torn-write tests, not hardware power-loss
certification.

Strict ThreadSanitizer checks with the pinned, instrumented OpenSSL pass **40
concurrent comparisons plus six focused groups**, including crash recovery,
publication dependencies, record I/O and lifecycle. Four Memcheck runs complete
**58,244 allocations and frees**, with zero live blocks, errors or suppressions.
Logs and reproduction scripts are under `build/pouch-space-deferred-scope/`:
`debug-full.log`, `gnu-focused.log`, `musl-focused.log`, `tsan-scopes.log` and
`memcheck-*.log`. Source and configuration remained unchanged throughout checks.

### Matched scope and ordinary-write measurements

The final GNU binary and frozen `92df5ee` binary run **160 verified trials**:
five alternating-order samples of current/prior scoped and ordinary writes in
both engines, both policies and one/four namespace writers. Each trial writes
128 one-KiB states twice with 4 KiB producer chunks. Scoped elapsed time includes
the final barrier. Every container trial retains exactly one reserved 10 MiB
file with no persistent sidecars. Caches are retained and device scheduling is
not isolated; these are small native comparisons.

Median scope elapsed and CPU milliseconds; prior is the preceding container:

| Policy | Writers | Prior container ms | Container ms | Multifile ms | Container / multifile CPU ms | Container / multifile syncs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 16.130 | 17.626 | 19.076 | 5.555 / 6.450 | 3 / 3 |
| NoSync | 4 | 22.174 | 21.220 | 25.188 | 12.186 / 12.175 | 6 / 12 |
| Durable | 1 | 827.317 | 22.677 | 20.791 | 5.869 / 6.980 | 5 / 3 |
| Durable | 4 | 548.815 | 35.644 | 37.183 | 10.635 / 14.067 | 10 / 12 |

Durable container scope time improves **36.5 times** at one writer and **15.4
times** at four. Barriers fall from 259 to five and median 136 to ten; remaining
structural and completion barriers are retained. Container/multifile durable
timing ranges overlap: **21.180–30.750 / 20.103–23.098 ms** at one writer and
**31.873–42.622 / 30.598–50.205 ms** at four. This removes the measured scope
gap without establishing a general backend performance win.

Ordinary-write controls retain per-mutation policy. Median milliseconds are
**current / prior**:

| Policy | Writers | Container ms | Multifile ms |
| --- | ---: | ---: | ---: |
| NoSync | 1 | 8.350 / 7.581 | 13.588 / 12.764 |
| NoSync | 4 | 7.031 / 8.521 | 16.098 / 12.570 |
| Durable | 1 | 806.567 / 832.852 | 1299.278 / 1434.864 |
| Durable | 4 | 566.408 / 615.466 | 844.212 / 852.556 |

All ordinary before/after elapsed ranges overlap. Single-writer durable
container controls retain exactly 259 barriers; four-writer current/prior
medians are 137/135 with overlapping 134–139/134–138 ranges. Container bytes
written remain exactly **403,803** at one namespace and **407,092** at four
for both scoped and ordinary modes, before and after the change.

Populated reopen-to-ready remains slower. Median scoped milliseconds:

| Policy | Writers | Container ready ms | Multifile ready ms |
| --- | ---: | ---: | ---: |
| NoSync | 1 | 12.414 | 6.419 |
| NoSync | 4 | 23.697 | 5.421 |
| Durable | 1 | 12.358 | 4.114 |
| Durable | 4 | 21.874 | 6.643 |

The new frozen ELF is
`build/pouch-space-deferred-scope/pouch_space_compare-current`, SHA-256
`c49c9759dbf2f22e791dbf81bde2c15e7df4d134ea0da23250b7470ee01c0b6f`.
The frozen prior ELF remains under `build/pouch-space-durable-scope/`, SHA-256
`d948e25939a9ada5e2ef0ea4847d483fc86a1bd45b0809959f646d91f686a1b0`.
The new stage retains `scope-paired.sh`, `scope-summary.pl`, `results.json`,
`benchmark.log`, all 160 raw trials in `paired/`, and a performance checksum
manifest covering both ELFs and those inputs/results. This closes the measured
scoped-sync gap. Maintenance capacity architecture, canonical recreation,
remaining Pouch integration, shared-root coordination and full performance
acceptance remain open; normal public Pouch open still uses the multifile backend.

## Deferred stream repair and reopen readiness

This is the `8c7b75c` checkpoint; the later
[lifetime repair and comparison](#durable-key-lifetimes) follows below.

Opening an unsealed stream now validates its canonical prefix and admits readers
without clearing unused suffix capacity. Newly accepted complete bytes enter
root readiness, so explicit barriers, peer publications and checkpoints still
cover them. The first append clears the complete rejected suffix and waits for
its own completed-repair readiness unit before returning a writer. This reuses
the writer's bounded heap buffer and the existing caller-led sync coordinator.
There is no batching delay, background worker, new writer queue, record-format
change or public API change. Failed repair returns no writer and retains the
existing poisoned-storage behavior. Sealing/retirement keep their dependency
barriers and can avoid unused-suffix clearing altogether.

Physical root admission still performs journal/incarnation durability work.
A four-namespace populated-reopen trace has **one sync and no suffix zero calls**,
versus **five syncs and four zero calls** at the preceding checkpoint. Both have
three root offset writes. These diagnostic traces are `container-4w.strace` and
`coordinated-ready-4w.strace` under `build/pouch-space-readiness/`; their timings
are not used for performance comparisons. Repair work remains required before
a subsequent append.

### Verification

The complete standard Debug preset passes **362/362 checks** in **1295.17 s**,
including **310 unit groups**, public API/shared-symbol boundaries and worker
stack checks. ASan/UBSan workers use 128 KiB stacks with the glibc stack cache
disabled. GNU release and native musl each pass **73 affected groups**, in
**64.84 s / 70.24 s**; musl uses its default worker stack.

The new stream-recovery group covers **20 deterministic variants**. Twelve
volatile/durable-model cases combine both policies, native/fallback zeroing and
append/seal/retire. A valid stale record beyond a PENDING gap must remain hidden
after a shorter append and another crash. Readers cause no stream writes/syncs;
explicit flush, peer publication and checkpoint cover newly accepted bytes;
allocation failure admits no writer; first-write repair occurs once; held old
readers remain valid. Four paused-clearing cases prove reader, peer-writer and
checkpoint progress. Four held-barrier cases force four repairs into exactly
two readiness groups and prove that subsequent admissions need no repair sync.
The existing heap/read/repair-I/O sweep now includes first-write admission and
checks poisoned-state rejection and null writer output after repair failure.

Strict ThreadSanitizer runs with the pinned instrumented OpenSSL pass **80
concurrent comparisons plus eleven groups**, including reads, recovery, scope
crashes, publication dependencies and maintenance concurrency. Four Memcheck
runs complete **52,211 allocations and frees**, with zero live blocks, errors
or suppressions. Logs are under `build/pouch-space-readiness/` as `debug-full.log`,
`gnu-coordinated-focused.log`, `musl-coordinated-focused.log`,
`tsan-readiness.log` and `memcheck-*.log`.

### Matched performance

The final binary and frozen `45da71c` binary run **240 verified trials**: five
alternating-order samples of current/prior container/multifile engines, both
policies, one/four namespace writers and three profiles. Each writes 128 one-KiB
states twice with 4 KiB producer chunks. Profiles are ordinary writes, forced
durability scopes, and metadata updates after populated reopen. Scope time
includes completion. Metadata seeding is outside its measured phase; the
reopen/mutation total includes closing/opening that populated store and both
metadata rounds. Every container trial retains one reserved 10 MiB file with no
persistent sidecars. Caches remain populated and device scheduling is not
isolated; these are small native fixtures.

Median populated reopen-to-ready milliseconds for the scope profile:

| Policy | Writers | Container ms | Prior container ms | Multifile ms |
| --- | ---: | ---: | ---: | ---: |
| NoSync | 1 | 6.806 | 12.035 | 4.340 |
| NoSync | 4 | 7.233 | 22.801 | 5.018 |
| Durable | 1 | 7.676 | 11.267 | 4.889 |
| Durable | 4 | 7.468 | 22.817 | 5.463 |

Four-namespace readiness improves about **3.2 times under NoSync** and **3.1
times under Durable**. Container/multifile readiness is still 7.2–7.5 / 5.0–5.5 ms
at four namespaces, so this does not close the full read-performance gap.
Reopen-read CPU, excluding initial root open, has container medians of
2.18–2.42 ms versus 2.49–3.57 ms before the change.

Fresh-write controls retain ordinary and scoped policy. Median milliseconds
are **current / prior**; C is container and M is multifile:

| Policy | Writers | Ordinary C ms | Ordinary M ms | Scope C ms | Scope M ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 8.699 / 7.398 | 12.614 / 11.532 | 13.239 / 15.001 | 19.009 / 19.813 |
| NoSync | 4 | 6.410 / 6.849 | 16.297 / 13.303 | 17.897 / 19.280 | 21.032 / 21.716 |
| Durable | 1 | 673.996 / 636.677 | 1272.388 / 1286.770 | 20.800 / 21.245 | 18.670 / 20.008 |
| Durable | 4 | 562.619 / 540.110 | 802.303 / 817.746 | 32.981 / 32.377 | 29.875 / 31.128 |

Median populated reopen plus metadata-update milliseconds; sync counts cover
the mutation phase and exclude root reopening:

| Policy | Writers | Container ms | Prior container ms | Multifile ms | Container / prior syncs |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 21.751 | 21.479 | 30.255 | 1 / 1 |
| NoSync | 4 | 16.978 | 18.630 | 69.231 | 2 / 4 |
| Durable | 1 | 380.333 | 468.863 | 1095.234 | 257 / 257 |
| Durable | 4 | 343.391 | 297.646 | 604.052 | 131 / 135 |

All fresh-write and reopen/mutation control ranges overlap before/after.
Some current medians are higher: single-writer ordinary NoSync is 8.699 versus
7.398 ms, and four-writer durable reopen/mutation is 343.391 versus 297.646 ms.
The latter ranges are **301.153–359.613 / 278.360–387.512 ms**. These samples
establish the readiness gain, not a general write-performance win or the absence
of smaller regressions. Write-phase `syscall_written` remains exactly 403,803
bytes at one namespace and 407,092 at four for ordinary/scoped writes, and
85,064 bytes for metadata rounds, before/after. This excludes zero-range work.

The initial implementation deferred repair but retained unconditional recovery
syncs. Its separate 240-run series exposed a four-writer NoSync metadata
regression: **16.118 versus 9.122 ms**, with non-overlapping **13.369–19.745 /
8.637–13.004 ms** ranges. Traces showed concurrent clears followed by repeated
syncs flushing newly appended NoSync metadata. Completed repairs now join the
existing readiness coordinator. A targeted five-sample, three-engine comparison
then measures **10.236 ms coordinated / 9.699 ms prior / 17.693 ms unconditional**,
with two/four/four barriers. The final full series above retains the readiness
gain with overlapping reopen/mutation ranges. Both initial and final series,
and all fifteen diagnostic trials, remain available; no trials were discarded.

The final ELF is `build/pouch-space-readiness/pouch_space_compare-coordinated`,
SHA-256 `437181b4e02373c2760d840392f9095746a3d4fd3d991258654d36ab24064b92`.
The prior ELF is `build/pouch-space-deferred-scope/pouch_space_compare-current`,
SHA-256 `c49c9759dbf2f22e791dbf81bde2c15e7df4d134ea0da23250b7470ee01c0b6f`.
The initial candidate is `pouch_space_compare-current` in the new stage, SHA-256
`79afb9a950919709648b738e49e7e665f856a14260104d26211dc0cbe3ce60fe`.
The stage retains both runners, `readiness-summary.pl`, initial `paired/` and
`results.json`, final `coordinated/paired/` and `coordinated/results.json`, and
`coordinated-check/`. Each full series has a verified 246-artifact performance
manifest. Normal public Pouch remains multifile. Full integration, selected
state/object lifetime repair, maintenance-capacity policy, shared-root operation
and broader performance acceptance remain open.


## Durable key lifetimes

Generation-1 state/object recreation now remains visible immediately, after
reopen and after compaction. Canonical records and snapshots carry the namespace
index of the key's first body as a durable lifetime. Replay compares that
identity before preserving the existing version/index ordering within it.
Metadata-only state after deletion retains the old identity until another body
starts a fresh one. Links use their destination key's lifetime and retain the
encoded payload context. The mandatory common trailer is now
`[uint64 lifetime][LCSI][uint64 index]` (20 bytes), with the distinct 12-byte
query-index extension when needed. Controls/collection records use zero in the
shared slot. Size planning and corruption validation change together. No extra
payload copy, allocation, journal, writer queue or public ABI change is added.
Recreate experimental fixtures from the preceding canonical metadata layout;
existing-root conversion and compatibility parsing remain excluded.

### Verification

The complete Debug preset passes **365/365 checks**, including **313 unit
groups**, in **1470.75 s**, with ASan/UBSan, 128 KiB glibc workers and the stack
cache disabled. Native GNU release and musl each pass **20 focused groups** in
**220.91 / 232.16 s**; musl uses default worker stacks. The 32-fixture lifetime
matrix covers both backends, all four codecs, both policies and state/object
classes, four recreations per fixture, metadata-only transitions, pre/post-create
compaction, repeated reopen, both input paths and pinned old readers. Sixteen
native fixtures also verify destination lifetime selection during staged commit
and promotion, plus two compression-policy recodings. Each fixture checks two
exhausted-index recovery/rejection attempts with query freshness preserved.

Captured-prefix replay covers stale former-lifetime payloads, deletes and
metadata after both full history and a snapshot containing only the recreated
value. Same-lifetime stale/equal generations and independent metadata ordering
remain valid. Codec tests round-trip lifetimes, reject invalid values before
output, preserve prior decoded output on corrupt reads, and distinguish compact
and extended trailers with marker-shaped scalar values. The dedicated
recreation sweep passes **1,272 allocation/I/O/partial-write failure sites**,
including retry and compaction after recovery. Metadata-only placeholders are
checked through metadata reads; ordinary body reads retain their existing
absent-body behavior.

Strict ThreadSanitizer with the pinned instrumented OpenSSL passes **80
concurrent comparisons plus eleven groups**, including lifetime faults, staging,
codec concurrency, publication and maintenance. Four Memcheck runs complete
**858,328 allocations and frees**, with zero live blocks, errors or suppressions.
Evidence is under `build/pouch-space-key-lifetime/`: `debug-full.log`,
`gnu-focused.log`, `musl-focused.log`, `tsan-lifetimes.log` and
`memcheck-*.log`.

### Matched writes, reads and compaction

The final and frozen `8c7b75c` executables complete **720 verified trials**:
five alternating-order samples, current/prior container/multifile engines,
both policies, one/four namespace writers and nine profiles. Each uses 128
one-KiB states and 4 KiB producer chunks. Writes run twice; deletion runs once.
Metadata/deletion seed before closing/reopening their populated root, outside
the measured mutation phase. Their reported total includes that reopen and the
mutation. Read profiles measure their named read separately after writing;
compaction measures its maintenance phase separately. Every container trial
retains one reserved **10 MiB** file with zero persistent sidecars. Caches remain
populated and device scheduling is not isolated. These small native fixtures
establish comparisons here, not large-store acceptance.

Median fresh-write milliseconds. C is container and M is multifile:

| Policy | Writers | Ordinary C | Prior C | Ordinary M | Scoped C | Prior scoped C | Scoped M |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NoSync | 1 | 6.793 | 6.837 | 10.440 | 14.510 | 13.616 | 17.927 |
| NoSync | 4 | 7.257 | 7.054 | 15.813 | 16.753 | 20.145 | 21.533 |
| Durable | 1 | 862.073 | 711.060 | 1230.798 | 22.184 | 19.848 | 22.417 |
| Durable | 4 | 552.946 | 538.389 | 795.310 | 31.206 | 29.796 | 28.577 |

Median populated reopen plus mutation milliseconds:

| Profile | Policy | Writers | Container | Prior container | Multifile |
| --- | --- | ---: | ---: | ---: | ---: |
| Metadata | NoSync | 1 | 22.105 | 21.030 | 25.914 |
| Metadata | NoSync | 4 | 15.453 | 16.089 | 71.773 |
| Metadata | Durable | 1 | 432.830 | 378.271 | 1076.657 |
| Metadata | Durable | 4 | 308.377 | 327.579 | 584.863 |
| Delete | NoSync | 1 | 11.936 | 11.710 | 21.238 |
| Delete | NoSync | 4 | 11.543 | 13.194 | 51.195 |
| Delete | Durable | 1 | 183.538 | 180.384 | 544.096 |
| Delete | Durable | 4 | 138.573 | 139.619 | 316.812 |

Median read milliseconds for the complete 128-row named access pattern:

| Profile | Policy | Writers | Container | Prior container | Multifile |
| --- | --- | ---: | ---: | ---: | ---: |
| Point | NoSync | 1 | 0.969 | 0.710 | 4.361 |
| Point | NoSync | 4 | 0.605 | 0.487 | 1.451 |
| Point | Durable | 1 | 0.690 | 0.683 | 3.872 |
| Point | Durable | 4 | 0.563 | 0.429 | 1.835 |
| Read-many | NoSync | 1 | 0.556 | 0.557 | 0.896 |
| Read-many | NoSync | 4 | 0.324 | 0.363 | 0.503 |
| Read-many | Durable | 1 | 0.635 | 0.650 | 1.157 |
| Read-many | Durable | 4 | 0.334 | 0.395 | 0.662 |
| Metadata scan | NoSync | 1 | 0.215 | 0.207 | 0.211 |
| Metadata scan | NoSync | 4 | 0.215 | 0.198 | 0.205 |
| Metadata scan | Durable | 1 | 0.230 | 0.204 | 0.200 |
| Metadata scan | Durable | 4 | 0.272 | 0.294 | 0.192 |
| Body scan | NoSync | 1 | 0.622 | 0.543 | 0.505 |
| Body scan | NoSync | 4 | 0.348 | 0.363 | 0.435 |
| Body scan | Durable | 1 | 0.555 | 0.585 | 0.620 |
| Body scan | Durable | 4 | 0.480 | 0.390 | 0.663 |

Median compaction milliseconds:

| Policy | Writers | Container | Prior container | Multifile |
| --- | ---: | ---: | ---: | ---: |
| NoSync | 1 | 41.025 | 38.026 | 47.702 |
| NoSync | 4 | 123.728 | 123.388 | 148.593 |
| Durable | 1 | 40.912 | 39.631 | 40.525 |
| Durable | 4 | 138.421 | 147.856 | 169.756 |

Median populated reopen-to-ready milliseconds for the scope profile:

| Policy | Writers | Container | Prior container | Multifile |
| --- | ---: | ---: | ---: | ---: |
| NoSync | 1 | 9.007 | 7.014 | 4.307 |
| NoSync | 4 | 7.033 | 8.046 | 4.181 |
| Durable | 1 | 7.751 | 6.976 | 4.010 |
| Durable | 4 | 9.672 | 8.033 | 4.371 |

The container remains faster than current multifile for ordinary writes,
metadata/deletion, point reads and read-many in these medians. Scoped durable
writes are close at one namespace and 9.2% slower at four. Metadata scans are
slower in every current pair; the four-writer durable case is 0.272 / 0.192 ms.
The single-writer NoSync body scan is also slower, 0.622 / 0.505 ms. Single-writer
durable compaction is about even; other compaction medians favor the container.
Reopen-to-ready remains slower than multifile. These read/maintenance limits
leave complete performance acceptance open.

All 36 container before/after ranges overlap for each profile's primary metric.
Some medians nevertheless rise: ordinary single-writer durable writes are
**862.073 / 711.060 ms**, with **584.622–1110.388 / 587.872–1280.997 ms** ranges;
CPU medians are **29.290 / 29.238 ms**. Single-writer durable reopen/metadata is
**432.830 / 378.271 ms**, with **410.238–455.304 / 340.418–415.808 ms** ranges.
Four-writer durable point reads are **0.563 / 0.429 ms**, with **0.441–0.608 /
0.335–0.548 ms** ranges. The samples do not establish a general performance win
or exclude smaller regressions.

The new field adds eight canonical bytes per record. Observed write-phase
`syscall_written` for 256 ordinary/scoped writes increases by **2,048 bytes in
the container** and **4,096 in multifile**: current container totals are
405,851 / 409,140 bytes at one/four namespaces. Metadata's 256 records add
2,048 bytes in either engine; 128 deletes add 1,024. No additional per-record
barrier is introduced. Single-writer sync counts match the checkpoint; timing
changes the coalescing of some four-writer groups (durable metadata has median
133 / 131 container barriers). Compaction container barriers remain
15 / 60 under NoSync and 18 / 72 under Durable at one/four namespaces;
multifile has 10 / 40 under either policy.

### Focused point-read follow-up

The higher point-read medians prompted a separate **160 verified trials**:
ten alternating-order samples, 128/1,024 rows, one/four writers, NoSync and all
four current/prior engines. Each larger workload still fits one reserved
10 MiB container. Median point-read milliseconds:

| Rows | Writers | Container | Prior container | Multifile |
| --- | ---: | ---: | ---: | ---: |
| 128 | 1 | 0.659 | 0.660 | 3.709 |
| 128 | 4 | 0.466 | 0.416 | 1.598 |
| 1024 | 1 | 4.015 | 4.226 | 29.242 |
| 1024 | 4 | 1.758 | 1.947 | 12.275 |

All before/after ranges overlap in this follow-up too. The higher primary
point-read medians (24–36.5% in three cases) vary across sizes/repeats:
the 128-row single-writer result is flat; the four-writer median is still 12.2%
higher. At 1,024 rows both container medians and CPU medians are lower than the
checkpoint. Some multifile medians also rise. Smaller performance effects remain
uncertain. Both series retain every trial.

The current frozen ELF is
`build/pouch-space-key-lifetime/pouch_space_compare-current`, SHA-256
`43928cf31d5447fe1f4ff76ee49fa58aa7261cdc0aeff5fa41604a7fbf5a29fe`.
The prior ELF is `build/pouch-space-readiness/pouch_space_compare-coordinated`,
SHA-256 `437181b4e02373c2760d840392f9095746a3d4fd3d991258654d36ab24064b92`.
The stage retains `lifetime-paired.sh`, `lifetime-summary.pl`, all 720
`paired/` logs and `results.json`, plus `read-check.sh`,
`read-check-summary.pl`, all 160 `read-check/` logs and
`read-check-results.json`. The performance manifest verifies **888 artifacts**,
including both executables and all inputs/results. Normal public Pouch remains
multifile; protected maintenance capacity, remaining owners/shared-root
integration and broader performance acceptance remain open.
