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

## Reproduce

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

## Next commitments

The [integrated foundation](../experiments/pouch_space/DESIGN.md) now replaces
the fixed 64 KiB checkpoint banks with growable typed metadata, packed descriptor
pages and protected checkpoint/maintenance capacity. Earlier measurement tables
above identify their implementation stages, including the current payload
packing and canonical-stream follow-ups. Before a production adapter, complete
stream sealing/retirement, typed catalogs, checkpoint scheduling,
relocating compaction, metadata repacking, process-shared reader
ownership, and complete public Pouch semantics. The current foundation permits
rebinding a deleted physical key; it does not supply permanent collection
position or retry authority. Full codec/index/XA/outbox integration remains part
of the authorized native implementation; existing-root conversion is excluded.
Production adoption still requires the
broader [draft acceptance evidence](pouch-space.md).
