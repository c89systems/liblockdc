# liblockdc Pouch benchmarks

## Single-file versus multifile

Run the native Linux comparison on the filesystem you want to measure.

```sh
POUCH_SPACE_RUNS=5 bash scripts/compare_pouch_space.sh coverage "$PWD/build/pouch-comparison"
```

Each profile runs both backends on fresh roots with matching options and
alternating execution order. The runner saves the raw logs, individual samples,
and per-phase medians in a new directory. Times in `medians.tsv` are in
milliseconds and ratios greater than one favor multifile. The NoSync and
durable runs are separate. Reopen retains the OS page cache.

| Area | Workloads |
| --- | --- |
| State and coordination | Acquire, held-key refusal, renewal, release and peer handoff, leased update and mutation, describe, query visibility and remove, point and bulk reads, scans, staging, durable scopes, compaction, namespace index publication and queries |
| Attachments | 64 KiB upload, list, byte-checked download and delete |
| Queues and transactions | Enqueue, stats, batch dequeue, payload read, visibility extension, deferral, redelivery with state, acknowledgement, watch observation from the same or a separate owner, joint state update and queue acknowledgement through prepare, commit, rollback and replay, and XA lifecycle |
| Outbox | Dispatch, reconciliation, warm stateful delivery, and terminal command receipts |
| Collections | Catalog, lease and metadata operations, direct pull, staged pull and push with independent owners of one collection, raw and selective reads, index build and reopen rebuild, encrypted/compressed append, suffix and scattered edits, append-only edits, retention and compaction |

The bounded profiles measure operation costs and verify their results.
Continuous background compaction and worker CPU use the separate
[mixed collection soak](../docs/pouch-space.md#verification-and-performance).
The paired suite reports timings without pass/fail performance thresholds.

## Native collections

The native collection workload checks append results, raw and filtered JSONL
record/byte counts, and reads after reopening. It reports wall time and process
CPU time separately for append/read phases. Build it with:

```sh
cmake --build --preset x86_64-linux-gnu-release --target lockdc_bench
./build/x86_64-linux-gnu-release/bench/lockdc_bench 4096 collections
```

The default storage parent is `/tmp`, which may be RAM-backed. For filesystem
and sync measurements, choose an existing absolute directory on the intended
storage device. Each run creates and removes only its owned temporary root:

```sh
mkdir -p build/collection-perf/storage
LOCKDC_COLLECTION_BENCH_DIRECTORY="$PWD/build/collection-perf/storage" \
LOCKDC_COLLECTION_BENCH_PUSH=1 LOCKDC_COLLECTION_BENCH_SYNC=1 \
  ./build/x86_64-linux-gnu-release/bench/lockdc_bench 256 collections
```

Additional settings are `LOCKDC_COLLECTION_BENCH_PAYLOAD` (padding bytes,
default 1024), `WRITERS` (default 1), `SHARED`, `INDEX` (default 1), `CRYPTO`,
and `COMPRESS`, all with the `LOCKDC_COLLECTION_BENCH_` prefix. Boolean settings
default off unless noted. Set `MAX_ENTRIES` to a positive value to include a
controlled maintenance pass after the initial reads, then check retained counts
and reopened streaming output. This invokes the actual maintenance path directly
rather than waiting for its background cadence. Zero leaves retention unlimited.

Compare identical workloads in paired runs. Use `strace -f -c` to attribute
syscalls and sync calls; these counts are not physical device IOPS. Keep
RAM-backed CPU measurements separate from device-dependent durability latency.

`LOCKDC_COLLECTION_BENCH_LEASE=1` creates the collection with a single lease
covering every append. It does not renew on each event. With several callers,
they share that owned receiver. Set `LOCKDC_COLLECTION_BENCH_INDEPENDENT=1` to
give each caller its own collection instead; this supports leased/unleased
comparisons without contention on one journal. Append timing covers all entries,
including caller setup/teardown; reads check the first caller's partition, and
every partition's committed count/high-water mark is verified. Both flags default
off. Compare matching `LEASE=0/1` runs with the same inputs and storage directory.
`LOCKDC_COLLECTION_BENCH_PEERS=1` with `SHARED=1` opens another client for each
additional writer. Use `STAGED=1` for staged pull, or `PUSH=1` for push append.
`BUILD_INDEX=1` with `INDEX=1` times the explicit index maintenance pass before
the first read and again after reopening.

For optional diagnostics outside the benchmark gates, use
`make compare-collection-layouts` to compare producer layouts, inode growth
and compaction, or `make compare-collection-pull` to compare direct and staged
pull with identical fast/slow sources and concurrent writers. Their `-smoke`
targets check correctness under Debug with allocation tracking. See
[layout and pull comparisons](../docs/collection-layout-experiment.md).
`make compare-collection-read` separately compares collection output with raw
JSONL; see [read comparison limits](../docs/collection-read-comparison.md).

## Go comparisons

This module is intentionally outside the CMake release gate. It is used by the
root `benchmark-pouch-go*` Makefile targets for opt-in pouch performance and
stress work.

`make benchmarks-go` runs the complete Go benchmark suite, including
compression, compaction, concurrency, and query scale workloads. Its defaults
are `POUCH_GO_BENCH=.`, `POUCH_GO_BENCHTIME=3s`,
`POUCH_GO_SEED_ROWS=10000`, and `POUCH_GO_TEST_TIMEOUT=10m`; increase the
timeout for a full run when necessary. This command reports measurements;
`make bench-gate` separately runs all native cases and enforces the default
and strict-durability parity budgets described below.

The pouch benchmark path calls into C through cgo and reports C-measured query
time as `c-ns/op`, so the cgo bridge is not the metric used for pouch query
latency. The Go benchmark `ns/op` still includes harness/setup work and is only
useful as a coarse runner signal.

For pouch-only performance iteration, use the native `bench/lockdc_bench`
surface through the root `benchmark-pouch-perf*` targets instead of the Go
comparison matrix. These targets rebuild only `bench/lockdc_bench`, seed a
bounded public-API pouch fixture, warm the index when needed, then print one
isolated `metric=...` line for the phase under repair. The default timeout is
60 seconds including rebuild. The fast cases are:

- `make benchmark-pouch-perf-index-docs`
- `make benchmark-pouch-perf-full-text-keys`
- `make benchmark-pouch-perf-full-text-reopen-keys`
- `make benchmark-pouch-perf-scan-keys`
- `make benchmark-pouch-perf-flush-intermediate`
- `make benchmark-pouch-perf-flush-reopen`

Set `POUCH_PERF_ROWS`, `POUCH_PERF_PAYLOAD_BYTES`, or `POUCH_PERF_CRYPTO=1` to
adjust the local fixture without switching to the broad parity suite.

`make benchmark-pouch-routine` is the bounded development suite. It runs the
six native phase probes at 12 rows and 128 KiB payloads, the 12-row segmented
production matrix, and the two-writer/eight-write shared-root concurrency
matrix. A single 90-second outer timeout covers the complete command;
`POUCH_GO_ROUTINE_TIMEOUT`, `POUCH_PERF_ROUTINE_ROWS`,
`POUCH_PERF_ROUTINE_PAYLOAD_BYTES`, and the
`POUCH_GO_ROUTINE_CONCURRENCY_*` variables adjust its profile. The broader
medium, acceptance, compaction, and parity-gate commands remain opt-in.

`BenchmarkFastLockdDisk` and `BenchmarkMediumLockdDisk*` launch a real pinned
`pkt.systems/lockd` binary with a disk backend rooted in an automatically
removed `/tmp/liblockdc-lockd-disk-bench-*` directory. Both engines explicitly
use `/tmp` regardless of `TMPDIR`, so redirecting Go build scratch cannot put
their storage fixtures on different filesystems. The disk side uses the
exported Go client and full-form LQL expressions. The pouch side measures the
C query path directly through `selector_lql` and reports C-side query time so
the cgo bridge is excluded from pouch latency metrics.

`make benchmark-pouch-go-production` runs the default aligned-`NoSync`
production workload for six
explicit variants by default: `ProductionPouchPT`, `ProductionPouchCrypto`,
`ProductionPouchCompression`, `ProductionPouchCryptoCompression`, and
`ProductionLockdDiskNoCrypto`, plus `ProductionLockdDiskCrypto`. Pouch crypto
is enabled through the public `crypto_key` endpoint option; the Go disk
variants run with storage encryption disabled and enabled respectively. The
parity gate compares plaintext and crypto runs only to their matching Go disk
configuration. Go disk has no matching compression mode, so compression runs
remain reported Pouch diagnostics rather than mismatched parity inputs. The
production target reports split flush metrics so index flush work can be
attributed to intermediate write-churn flushes, final flush, no-op flush, and
post-reopen flush.

`make benchmark-pouch-go-parity-gate` uses the median from
`POUCH_GO_PARITY_COUNT=5` same-run production samples for each engine. Its
exclusive release budget is `POUCH_GO_PARITY_MIN_SPEEDUP=1.25`: Pouch must be
no slower than 80% of the matching Go disk latency on every comparable core
metric. Set the variable only to make an intentional release-policy change;
the gate reports the measured speedup for every budget miss. Compression has no
matching Go disk transform, so the release gate runs only the four comparable
variants selected by the full-name `POUCH_GO_PARITY_BENCH` expression;
compression remains reported evidence in the complete six-mode production
matrix rather than a synthetic cross-engine ratio. The release comparison uses
the existing bounded production profile: 12 records, two updates, 128 KiB
payloads and 16 KiB segments, adjustable with
`POUCH_GO_BOUNDED_PRODUCTION_*`. `POUCH_GO_PARITY_TIMEOUT=90s` bounds the
five-sample measurement; preparation is separate. The three large scenarios
remain available through `make benchmark-pouch-go-production` for scale and
stress investigation.
Preparation builds the native static library and pinned Go server, without
building other architectures or unrelated test executables.
The isolated server fixture uses the supported five-second minimum HA lease,
zero shutdown drain grace, and graceful cancellation with a two-second
forced-stop fallback.
This bounds restart handoff without extending client request deadlines.
The contract is per operation: acquire, update, stale-precondition rejection,
release, public and leased reads, attachment write/read, queue delivery,
intermediate/final/no-op/
post-reopen index publication, indexed and scan queries, full-text search, and
restart recovery must all satisfy the same budget. A faster aggregate cannot
hide a slower lock or index invariant.

`make benchmark-pouch-go-durable` is the separate strict-durability matrix.
It compares Pouch `durable_sync=true` with a one-server Go disk `--ha auto`
run: Go core applies `NoSync` in `failover` and `single`, but not `auto`.
The command measures plaintext and storage-crypto configurations across the
same production operation metrics. Its default 12-row, two-update, 128 KiB,
16 KiB-segment profile is bounded at 90 seconds and retains rollover. Set the
`POUCH_GO_DURABLE_*` variables for another profile. `make
benchmark-pouch-go-durable-gate` uses the same five-sample and 1.25x policy
when that opt-in durability mode must meet the release performance budget.
Its five-sample measurement is bounded by `POUCH_GO_DURABLE_GATE_TIMEOUT=90s`.
Strict sync is not Go disk's default disk policy, but it is a first-class Pouch
mode: `make perf-gate` runs the default and durable comparison gates
separately, each against its matching Go disk durability boundary.
Attachment output separates `attachment-write-ns/op` from
`attachment-read-ns/op`; the legacy combined `attachment-ns/op` remains a
diagnostic only.

The fixed production profiles use deterministic, JSON-safe high-entropy payload
tails and size each profile above the 64 MiB segment target after compression.
This keeps the required multi-segment replay and rollover assertions meaningful
for plaintext, crypto, compression, and crypto-plus-compression runs. Custom
profiles remain responsible for supplying enough incompressible historical data
to exceed their configured segment target.

`make benchmark-pouch-go-production-bounded` is the routine full-matrix
coverage target. It uses one iteration of 12 rows, two updates per key, a
128 KiB historical payload, and a shared 16 KiB segment target. The target
forces rolling and still validates stale ETag rejection, attachments, queue
round trips, replay, indexed and scan queries, full-text queries, and public
reads. The common target is passed to Pouch through `segment_target_bytes` and
to Go disk through `--logstore-segment-size`; this is necessary because Pouch
compression changes stored bytes and therefore rollover frequency. Override
the bounded values with the `POUCH_GO_BOUNDED_PRODUCTION_*` variables, or use
the corresponding `POUCH_GO_PRODUCTION_*` variables for a custom production
profile.

Production output reports `restart-recovery-ns/op`, the complete close/reopen
plus first post-reopen index-flush path. This is the cross-engine recovery
metric: lockd disk eagerly restores state during server startup, while Pouch
loads the namespace lazily when the first operation needs it. The existing
`reopen-ns/op` remains a diagnostic sub-phase. `flush-reopen-ns/op` is the
post-recovery index-publication operation and is independently gated.
Both engines also flush midway through every production workload, including
the bounded profile, so intermediate publication has an actual measurement.

The production indexed-query pair distinguishes reader readiness from steady
state. `index-query-keys-ns/op` is the first indexed `RangeHalf` query after a
reopen followed by a wait flush; `index-query-keys-warm-ns/op` is its immediate
repeat. The first metric catches deferred reader recovery or stale index work
that would otherwise be hidden by a warmed cache, while the second measures the
resident query path.

`make benchmark-pouch-go-concurrency` is the bounded lock and shared-root
comparison matrix. It runs Pouch and Go lockd disk with crypto disabled and
enabled, over a contended single key and independent keys. Pouch opens the
requested number of distinct liblockdc clients with `single_writer=false`
against one root. The same-key case uses public acquire/update/release calls and
asserts the final version equals every completed write; the independent-key case
reads back every key at version one. This confirms both key-lock serialization
and multi-client shared-root mutation behavior.

Go lockd disk is measured through its supported topology: the same number of
independent Go clients connect to one disk-backed lockd server. The Go disk
backend itself remains a single disk writer and does not support several disk
server instances mutating one root. The target reports `write-wall-ns/op`,
`write-mean-ns/op`, `max-write-ns/op`, and `writes/s`; readback validation is
outside the timed region. Defaults are two writers, 32 writes per writer, and a
256-byte payload. Override them with `POUCH_GO_CONCURRENCY_WRITERS`,
`POUCH_GO_CONCURRENCY_WRITES_PER_WRITER`, and
`POUCH_GO_CONCURRENCY_PAYLOAD_BYTES`.

The default Go harness passes `--ha failover` explicitly.
Go disk rejects `concurrent` for disk roots, and Go core marks failover writes
`NoSync`. Pouch defaults to `durable_sync=false`, so this matrix uses the same
power-loss durability boundary while comparing contention, shared-root,
key-lock, and observed throughput. Run `durable_sync=true` separately when a
stronger Pouch `fdatasync` group-commit boundary is required; use the explicit
`benchmark-pouch-go-durable` pair rather than comparing that strict mode to
default Go failover numbers.

Crypto mode measures the operational overhead of each engine's enabled
at-rest-encryption configuration, not a byte-for-byte cryptographic-format
comparison: Pouch uses one generated `crypto_key` across its clients, and
the bootstrapped Go lockd disk server enables its storage encryption.

`make benchmark-pouch-go-medium` mirrors the Go lockd disk comparison shape over
both key and document result modes for scan and indexed engines. The default
matrix includes sparse/dense equality, numeric range, `in`, array membership,
case-sensitive and case-insensitive text, date, root OR, recursive exists,
tenant/outbox/amount predicates, narrative text, and full-document text search
over production-shaped nested documents. `make benchmark-pouch-go-acceptance`
keeps the same 4096-document cap and runs the broader indexed comparison plus a
bounded scan subset.

`make benchmark-pouch-go-compaction` runs opt-in pouch-only compaction
benchmarks. It reports forced maintenance compaction time as `compaction-ns/op`
and background-scheduled compaction impact through `write-ns/op` plus
`max-write-ns/op`. A Pouch pthread worker performs scheduled work after a
mutation signals it, so foreground writes do not execute the compaction pass.
The scheduled profile waits for an actual idle-debounced compaction snapshot;
it cannot pass merely because it queued background work before close.
Forced compaction keeps both default profiles; scheduled compaction uses a
bounded default profile so the target is usable as a routine gate while still
exercising an early compaction threshold. Setting any
`POUCH_GO_COMPACTION_*` value replaces the defaults with a single env-driven
scenario. Use these environment knobs to simulate different schedules:

- `POUCH_GO_COMPACTION_ROWS`
- `POUCH_GO_COMPACTION_UPDATES`
- `POUCH_GO_COMPACTION_PAYLOAD_BYTES`
- `POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES`
- `POUCH_GO_COMPACTION_MIN_SEGMENTS`
- `POUCH_GO_COMPACTION_MIN_RECLAIMABLE_BYTES`

`make benchmark-pouch-go-core-soak` is a finite Pouch-only churn run for
prerelease hardening. It repeats the full production operation set on one root
under plaintext, crypto, compression, and crypto-plus-compression. The
hardening graph follows it with forced and scheduled reclaim and with
same-key/independent-key shared-root contention. Its ten-minute outer budget
and workload controls are intentionally separate from the normal release
gate: it is a sustained-invariant proof, not a release-time benchmark tax.
