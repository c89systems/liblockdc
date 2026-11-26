# Pouch Space host handover

Continue on `experiment/pouch-space`. The implementation at handoff is
`455daf1e` (`perf(pouch): keep NoSync metadata changes in canonical history`).
The worktree was clean before this handover. On a fresh checkout, use
`git fetch origin` followed by `git switch --track origin/experiment/pouch-space`.

Performance work stopped for the host move. The VPS filesystem was 96%
full and I/O contention was severe. The full native run on `455daf1e` was
interrupted by request after 9m 21s, while linking step 177 of 203 in the Debug
build. Neither CTest nor service E2E had started.

## Latest verified results

At `455daf1e`, five matched Release runs on native Linux x86_64/ext4 used
192 rows, 4 KiB bodies, two writers, and NoSync. Median elapsed time covers the
mutation phase. The prior single-file executable was built at `8b2f6fa8`.

| Workload | Prior single-file | Current single-file | Multifile |
| --- | ---: | ---: | ---: |
| Delete | 548.83 ms | 1.73 ms | 1.10 ms |
| Metadata update | 1,128.60 ms | 6.47 ms | 4.60 ms |

Every prior single-file run issued three foreground `fdatasync` calls. Every
current run issued zero. Current deletes and metadata updates are still about
57% and 41% slower than multifile in the mutation phase. Median single-file
reopen rose from 0.37 to 1.32 ms for deletes and from 0.64 to 1.67 ms for
metadata updates. Recheck wall times on the destination host because the VPS
was contended.

Current behavior appends eligible standalone edits to the active canonical
segment using ordinary capacity. Typed metadata handles staged work, protected
lease completion, and edits without ordinary space in that segment. Funded
namespace snapshots can accumulate across batches within the existing byte
limits. Recovery replays the accepted canonical suffix. The underlying design
idea was to keep log publication independent of projection checkpoint cadence.
[FASTER checkpointing and recovery](https://microsoft.github.io/FASTER/docs/fasterkv-recovery/) · [LeanStore logging and checkpointing paper](https://db.in.tum.de/~leis/papers/rethinkingLogging.pdf)

All 19 focused tests passed in 86.68 s. The separate mutation fault sweep
exercised 198 allocation and I/O failure sites in 9.13 s. The power-loss image
test verified the 192-row mutation workload on both sides of an explicit
durability barrier. The full-capacity test verified that deletes can complete
without moving the active stream.

The last completed full native run was at `97af32b4`. It passed 1,509 Debug
tests and 61 service E2E tests in 11m 32s. The last complete benchmark matrix was
at `3ae13f2c`, with five matched runs per profile. Of 1,092 metric medians, 958
were within 10% of multifile or faster. In 880 metrics that held in at least
four of five pairs. Of the 84 initially tracked gaps, 26 met that repeatability
criterion. The newer commits have targeted measurements only.

Two targeted results worth carrying forward are the stable NoSync index flush
(317 ms single-file vs. 337 ms multifile when both persist their captured
canonical state) and contended two-writer durable appends (37 syncs instead of
50–52 and 11.30 s median write time vs. 18.30 s multifile). Those wall times were
also observed under VPS I/O contention. Keep the raw and equivalent-durability
index profiles visible because their completion boundaries differ.

## Resume on the destination host

1. Run `make test-all` to completion and record Debug CTest, service E2E, and
   build-inclusive wall times. The targets are under 15 minutes for full native
   testing and under 3 minutes for each fault sweep. The prior successful run used
   `LOCKDC_CTEST_PARALLEL_LEVEL=24`. Set parallelism for the destination host.
2. Gather a fresh five-pair matrix on that same host. Keep the emitted
   `run.txt`, `samples.tsv`, and `medians.tsv` together.

   ```bash
   POUCH_SPACE_RUNS=5 bash scripts/compare_pouch_space.sh coverage "$PWD/build/pouch-space-parity"
   ```

3. Reproduce the larger NoSync mutation cases with the Release driver built
   by the matrix. Run each pair five times, alternating backend order and using
   fresh stores. Record write, CPU, sync, and reopen times.

   ```bash
   probe="$PWD/build/x86_64-linux-gnu-release/experiments/pouch_space/pouch_space_compare"
   stores="$PWD/build/pouch-space-parity"
   "$probe" runtime-delete "$stores" 192 4096 0 2 4096
   "$probe" pouch-delete "$stores" 192 4096 0 2 4096
   "$probe" runtime-metadata "$stores" 192 4096 0 2 4096
   "$probe" pouch-metadata "$stores" 192 4096 0 2 4096
   ```

4. Rank repeatable misses by absolute latency or CPU first and report the
   ratio alongside it. Keep the 84-metric cohort below as the fixed denominator.
   The older matrix points to queue cleanup, index completion, outbox
   reconciliation, and collection append CPU. Retest them before choosing work
   because intervening commits changed all four surfaces. The confirmed
   192-row residuals are per-record mutation cost and reopen replay. The goal
   is to close the bulk of the original gaps to within 10% and quantify the
   remainder.

Use targeted correctness checks between coherent changes and rerun the full
native suite for final verification. Keep QEMU to short smoke tests. Preserve
canonical durability and recovery, independent owners, captured-reader
lifetimes, protected maintenance capacity, and true streaming. Production
cutover remains an explicit deployment decision. [Pouch Space contract](docs/pouch-space.md) · [Pouch storage specification](docs/pouch-storage.md)

The old host's raw logs are under `build/pouch-space-parity` and are ignored
by Git. The current mutation runs and interrupted suite are under
`nosync-ns/verified-canonical-checkpoint-WmnEHi` within that directory. The
focused test log is `nosync-ns/canonical-checkpoint-9IWVYe/verified-targeted-tests.log`.

<details>
<summary>Original 84 benchmark metrics</summary>

Use the profile and metric columns from the coverage output. A metric meets
the repeatability criterion when the single-file time is at most 1.10 times
the multifile time in at least four of five pairs.

| Profile | Metrics |
| --- | --- |
| durable-attachment | attachment-delete/p95_ms |
| durable-collection-edit-suffix | edit_ms |
| durable-collection-filter-index-1 | collection-index-build/initial/elapsed_ms |
| durable-collection-pull-contended | collection-append/cpu_ms |
| durable-collection-push-contended | collection-append/cpu_ms |
| durable-collection-staged-contended | collection-append/cpu_ms |
| durable-delete | cpu_ms |
| durable-index | index_flush_max_ms, maintenance_ms |
| durable-metadata | cpu_ms |
| durable-namespace-full-text | pouch-perf-full-text-reopen-keys/elapsed_ms |
| durable-namespace-index-reopen | pouch-perf-flush-reopen/elapsed_ms |
| durable-outbox | maintenance_ms |
| durable-queue | maintenance_ms |
| durable-read-points | reopen_ms |
| durable-scan-metadata | reopen_read_ms, reopen_ms |
| durable-scope | write_ms, cpu_ms, p95_ms, reopen_ms |
| durable-sdk-scan-metadata | reopen_read_ms, read_ms |
| durable-shared-index | maintenance_ms |
| durable-shared-outbox | maintenance_ms |
| durable-shared-queue | close_ms, maintenance_ms |
| durable-staged | cpu_ms |
| durable-state | reopen_ms |
| nosync-attachment | attachment-put/p95_ms, attachment-put/elapsed_ms |
| nosync-collection-filter-index-1 | collection-index-build/initial/cpu_ms, collection-index-build/initial/elapsed_ms, collection-append/elapsed_ms |
| nosync-collection-push-contended | collection-append/cpu_ms |
| nosync-collection-staged-contended | collection-append/cpu_ms |
| nosync-compact | p95_ms |
| nosync-delete | cpu_ms, write_ms, p95_ms |
| nosync-delete-compact | cpu_ms, write_ms, p95_ms |
| nosync-index | index_flush_max_ms, work_ms, lifecycle_ms, maintenance_ms, p95_ms |
| nosync-metadata | cpu_ms, write_ms, p95_ms |
| nosync-namespace-full-text | pouch-perf-full-text-reopen-keys/elapsed_ms |
| nosync-namespace-index-reopen | pouch-perf-flush-reopen/elapsed_ms |
| nosync-outbox | maintenance_ms |
| nosync-outbox-reconcile | outbox-reconcile/next_ms, outbox-reconcile/first_delivery_ms |
| nosync-queue | maintenance_ms, lifecycle_ms, p95_ms, work_ms |
| nosync-read-many | cpu_ms, p95_ms |
| nosync-read-points | reopen_ms |
| nosync-scan-bodies | p95_ms, reopen_read_ms |
| nosync-scan-metadata | reopen_read_ms, cpu_ms, reopen_ms |
| nosync-scope | reopen_ms, write_ms |
| nosync-sdk-read-points | p95_ms |
| nosync-sdk-scan-metadata | reopen_read_ms, reopen_ms |
| nosync-shared-index | index_flush_max_ms, p95_ms, close_ms, maintenance_ms |
| nosync-shared-outbox | close_ms, maintenance_ms |
| nosync-shared-queue | maintenance_ms, close_ms |
| nosync-staged | cpu_ms, p95_ms |
| nosync-state | p95_ms |

</details>
