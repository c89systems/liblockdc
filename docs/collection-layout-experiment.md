# Collection layout experiment

This manual diagnostic compares inode use, append cost, sequential reads and
compaction through the real Pouch engine. It is excluded from normal builds,
bench gates and releases. It changes no production API or storage format.

Run on the filesystem used by the application:

```sh
make compare-collection-layouts-smoke
make compare-collection-layouts
```

`COLLECTION_LAYOUT_DIR` selects the fixture/log directory, defaulting to
`build/collection-layout`. `COLLECTION_LAYOUT_RUNS` selects an odd sample count
from 1 through 9, default 3. Each sample has its own fresh root and removes that
fixture on completion. Logs and median `summary.tsv` remain in that directory.
The diagnostic currently requires native Linux and `/proc/self/io`.

## Layouts

- `raw`: compact JSONL, one write per complete entry, with a mutex around
  record append when using multiple producers. This is a storage lower bound;
  it has no identity index, lease fencing, retention or payload integrity checks.
- `pull`: the existing public atomic append into shared rolling segments.
- `push`: the public incremental writer, encoding into bounded chunks written
  directly into shared rolling segments. Commit publishes a small immutable
  index without reading or copying the payload.
- `buffered`: an experimental producer adapter. It accepts 4096-byte input
  chunks into a heap buffer, growing only up to the configured limit. Crossing
  that limit spills the buffered prefix and subsequent input to a private
  temporary file. Commit supplies the buffered or spooled input to the existing
  atomic pull append. The spool is copied into the canonical log, then removed.

The buffered adapter's experimental 64 KiB cap is per active producer. Its
oversized entries incur an explicit spool write/read pass. Public push uses
canonical chunk publication instead. Neither has a buffer pool or whole-history
cache. The synthetic input buffer is separate from the adapter's measured
staging bound; the benchmark creates that same input for every layout.
The `adapter_spooled` and `adapter_stage_capacity_bound` counters describe only
the experimental adapter; public push writes canonical chunks directly.

This is a producer/layout experiment, **not a replacement public push API**.
The existing atomic append validates JSON and fences publication at commit.
The adapter does not preserve public push admission-time fencing or write-time
validation. The public `push` path preserves both and supports configured
codecs; its canonical chunks contain ciphertext on encrypted roots. The runner
defaults to plaintext, uncompressed roots. The adapter's CLI buffer limit controls only
the adapter; public push uses bounded codec chunks with no whole-entry spool.

## Workloads and accounting

The runner pairs small entries, four-producer durable appends, forced spill,
and a larger read fixture. An indexed profile separately exposes LQL page-file
growth; that profile runs once rather than reporting a paired median. The
experiment deliberately retains the existing order/identity indexes,
publication protocol and group-sync machinery; comparisons against raw JSONL
include those additional SDK responsibilities.

Durable mode waits for an fsync covering each completed entry in every layout.
Raw writers release the append mutex before fsync so concurrent writes may
share a filesystem flush. Pouch retains its real batcher. NoSync mode excludes
the later checkpoint from append timing; checkpoint time and I/O are reported
separately. `per_op_us` is amortized throughput time, not a per-caller latency
percentile when producers run concurrently.

Read timing uses a counting sink. Separate, untimed verification checks every
body byte, unique producer IDs, complete output and monotonic commit positions.
It never accumulates the output. Both pre/post-compaction cold-read attempts
follow close and reopen, then fsync and per-file `POSIX_FADV_DONTNEED` so recovery
does not give either layout warmer payload pages. This is
**advised page-cache eviction**, not proof of physical-media reads; it does not
evict device caches or use global cache-dropping operations. Warm reads follow
immediately. Use the stricter `compare-collection-read` diagnostic when verified
page residency is required.

File inventories count regular files, directories, canonical segments/snapshots,
LQL page files, apparent bytes and allocated blocks. Process I/O counters report
read/write calls and kernel-accounted storage bytes. These are not physical
device IOPS and exclude filesystem journal/metadata traffic not charged there.

## Compaction accounting

Forced collection compaction copies retained records into a rolling snapshot
group, keeping their stored payload bytes and building matching projections.
Indexed entries retain their independently encoded units and get rebuilt
physical indexes. An active tail remains for new appends. Old files are
reclaimed only after reader protection and the deletion grace period permit it.
The experiment sets that grace to zero
only for its owned fixtures, with no outstanding readers.

Automatic selection requires reclaimable deleted payload or abandoned chunks. A pure
append journal therefore reports no automatic consolidation. Forced compaction
can reduce the inode count, but rewrites the retained history; repeating whole
collection compaction is not evidence of sustainable incremental consolidation.
The probe verifies the data again after forced compaction and reports its
time, I/O, resulting file count and sequential read costs.

Public push packs canonical chunks into rolling segments. LQL pages use one
artifact per covered ordered-index page. The current storage and concurrency
contract is documented in [incremental appends](collection-writer.md); this
diagnostic measures it rather than proposing a replacement implementation.
Results do not establish huge-history scalability or journalctl-equivalent
performance.

## Direct versus staged pull

```sh
make compare-collection-pull-smoke
COLLECTION_LAYOUT_RUNS=5 make compare-collection-pull
```

`COLLECTION_PULL_DIR` selects the fixture/log directory, defaulting to
`build/collection-pull`. `pull-summary.tsv` contains medians from fresh paired
runs with alternating order. This comparison adds no production configuration,
automatic path selection or second staging implementation.

Both variants consume the same single-pass callback with the same chunk size:

- `direct-pull` passes it to public `lc_collection_append`.
- `staged-pull` passes it to public `lc_collection_append_staged`, which consumes
  the source with a bounded 16 KiB heap input chunk through the existing writer.
  Production validation, callback guards, canonical chunk writes and publication
  handle the entry.

The probe verifies that every input byte was consumed once with the expected
number of source calls, including EOF. It also checks append outcomes, closes
and reopens storage, and verifies every output byte, identity and position.
No compaction runs in this comparison. The synthetic input exists in memory for
both variants; the comparison performs no preparatory whole-value copy.

Profiles cover 1 KiB and 128 KiB entries, one/four producer threads sharing a
collection receiver under shared-root policy, NoSync and selected durable cases.
This does not measure independent-process scheduling. Fast sources use 16 KiB
reads. Controlled slow sources sleep 100 microseconds before each nonempty
256-byte or 4096-byte read. This models waiting on input, not an actual disk or
network transport. Scheduler overhead
can lengthen each sleep; both paths use the identical source routine.

Alongside amortized append time, process CPU and kernel-accounted write bytes,
the summary reports medians of each run's per-entry p50/p95/p99 latency.
Per-entry latency starts immediately before the SDK call and includes source
consumption, validation and commit; input generation is excluded. Amortized
append timing also includes producer startup and input generation. Sample
quantiles are descriptive and have limited tail resolution for small profiles.

For direct invocation, `LOCKDC_LAYOUT_SOURCE_CHUNK` selects 1..16384 bytes and
`LOCKDC_LAYOUT_SOURCE_DELAY_US` selects 0..10000 microseconds, for these two modes.
`LOCKDC_LAYOUT_COMPACT=0` skips compaction while retaining reopen/read checks.
Defaults are 16384, 0 and 1 respectively. The pull smoke target covers small,
threshold-sized, multi-chunk, delayed and one-byte-fragment sources, with concurrent
and durable cases under sanitizers and allocation tracking.

These measurements compare the public direct and explicitly staged APIs.
Both preserve callback/retry semantics; staged input uses writer admission
separately from publication. Source errors, owner closure, fencing, codecs and
slow/concurrent writers are covered by the collection C and Lua test suites.

For codec comparisons, `LOCKDC_LAYOUT_COMPRESS=1` enables zlib and
`LOCKDC_LAYOUT_CRYPTO=1` enables encryption in collection modes. Both default
to zero. `LOCKDC_LAYOUT_ENTROPY=1` replaces repeated padding with deterministic
pseudo-random ASCII, forcing larger encoded values through the chunk path.
The raw mode remains plaintext JSONL and rejects codec options. Compare the
same codec, entropy, durability, input and writer settings on both builds;
these diagnostic measurements are not a gate.

`LOCKDC_LAYOUT_EXTRA_READS` adds 0..1000 repeated warm reads, default zero,
reported as `read_repeat`. Read phases also report process user and system CPU
seconds. Use repeated reads to separate payload processing from kernel I/O cost;
they do not replace the advised-cold measurement.
