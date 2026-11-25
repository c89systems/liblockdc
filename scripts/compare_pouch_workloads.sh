#!/usr/bin/env bash
# Invoked by compare_pouch_space.sh coverage after building the native drivers.
set -euo pipefail
export LC_ALL=C

probe=$1
storage=$2
runs=$3
SECONDS=0
results=$(mktemp -d "$storage/coverage-XXXXXX")
work=$(mktemp -d "$results/stores-XXXXXX")
trap 'rm -rf -- "$work"' EXIT
stamp=$(git rev-parse HEAD)
dirty=0
if [[ -n "$(git status --porcelain)" ]]; then dirty=1; fi
printf 'source=%s dirty=%s native=%s filesystem=%s samples=%s\n' \
  "$stamp" "$dirty" "$(uname -m)" "$(findmnt -n -o FSTYPE -T "$work")" "$runs" |
  tee "$results/run.txt"
printf 'results=%s\n' "$results"

export LOCKDC_BENCH_DIRECTORY="$work" LOCKDC_COLLECTION_BENCH_DIRECTORY="$work"
# Local comparison, using fresh roots and deterministic inputs.
export LOCKDC_TMP_AUTO_CLEANUP_STALE_SECONDS=-1
unset LOCKDC_OUTBOX_BENCH_ENDPOINT LOCKDC_OUTBOX_BENCH_FAILOVER_ENDPOINT
export LOCKDC_OUTBOX_BENCH_TERMINAL_ROWS=32 LOCKDC_OUTBOX_BENCH_CHURN_UPDATES=1
export LOCKDC_OUTBOX_BENCH_PAYLOAD_BYTES=4096 LOCKDC_OUTBOX_BENCH_PAGE_CAPACITY=8
export LOCKDC_OUTBOX_BENCH_DEAD_LETTER_EVERY=0 LOCKDC_OUTBOX_BENCH_CHECKPOINT_BYTES=0
export LOCKDC_POUCH_PERF_PAYLOAD_BYTES=4096 LOCKDC_POUCH_PERF_CRYPTO=0
export LOCKDC_COLLECTION_BENCH_PAYLOAD=1024 LOCKDC_COLLECTION_BENCH_WRITERS=1
export LOCKDC_COLLECTION_BENCH_INDEX=0 LOCKDC_COLLECTION_BENCH_BUILD_INDEX=0
export LOCKDC_COLLECTION_BENCH_CRYPTO=0 LOCKDC_COLLECTION_BENCH_COMPRESS=0
export LOCKDC_COLLECTION_BENCH_PUSH=0 LOCKDC_COLLECTION_BENCH_STAGED=0
export LOCKDC_COLLECTION_BENCH_SHARED=0 LOCKDC_COLLECTION_BENCH_PEERS=0
export LOCKDC_COLLECTION_BENCH_LEASE=0 LOCKDC_COLLECTION_BENCH_INDEPENDENT=0
export LOCKDC_COLLECTION_BENCH_MAX_ENTRIES=0

extract_metrics() {
  awk -v profile="$1" -v backend="$2" -v sample="$sample" -v driver="$3" '
    function emit(metric, value) {
      printf "%s\t%s\t%s\t%d\t%.9f\n", profile, metric, backend, sample, value
      ++observations
    }
    driver == "edit" && /^[^ ]+,[^ ]+,/ {
      split($0, csv, ",")
      emit("append_ms", csv[7] * 1000)
      emit("edit_ms", csv[9] * 1000)
      emit("edit_cpu_ms", csv[10] * 1000)
      emit("tail_read_ms", csv[12] * 1000)
      emit("post_edit_append_ms", csv[16] * 1000)
      next
    }
    /^metric=/ || /^mode=/ {
      for (key in fields) delete fields[key]
      for (i = 1; i <= NF; ++i) {
        separator = index($i, "=")
        if (separator)
          fields[substr($i, 1, separator - 1)] = substr($i, separator + 1)
      }
      metric = fields["metric"]
      if (fields["phase"] != "") metric = metric "/" fields["phase"]
      if (fields["filter"] != "") metric = metric "/" fields["filter"]
      if ("seconds" in fields) emit(metric "/elapsed_ms", fields["seconds"] * 1000)
      if ("cpu_seconds" in fields) emit(metric "/cpu_ms", fields["cpu_seconds"] * 1000)
      for (key in fields) {
        if (key ~ /^(write|work|cpu|read|close|reopen|reopen_read|create|lifecycle|index_flush_max|index_first_pass_max|index_repeat_pass_max|first_delivery|drain|next|checkpoint|completion|terminal_read|compact|maintenance|p95)_ms$/) {
          if ((key == "compact_ms" || key == "maintenance_ms" || key ~ /^index_/) && fields[key] + 0 == 0)
            continue
          emit((metric == "" ? "" : metric "/") key, fields[key])
        }
        if (key == "p95_us")
          emit((metric == "" ? "" : metric "/") "p95_ms", fields[key] / 1000)
      }
    }
    END { if (!observations) exit 1 }
  ' "$4"
}

pair() {
  local profile=$1 driver=$2 workload=$3 backend mode log
  shift 3
  local -a order=(single-file multifile) command settings
  if ((sample % 2 == 0)); then order=(multifile single-file); fi
  for backend in "${order[@]}"; do
    case "$driver" in
      sdk)
        command=("$probe/pouch_space_sdk_bench" "$workload" "$@") ;;
      edit)
        command=("$probe/pouch_space_edit_probe" "$work/edit-$profile-$backend" "$@") ;;
      state|lifecycle)
        mode=pouch-$workload
        if [[ "$backend" == single-file ]]; then mode=runtime-$workload; fi
        if [[ "$driver" == state ]]; then
          command=("$probe/pouch_space_compare" "$mode" "$work" "$@" 4096)
        else
          command=("$probe/pouch_space_lifecycle_compare" "$mode" "$work" "$@")
        fi ;;
    esac
    log="$results/$profile-$backend-$sample.log"
    settings=(env -u LOCKDC_BENCH_SYNC "LOCKDC_BENCH_BACKEND=$backend")
    if [[ "$driver" == sdk ]]; then settings+=("LOCKDC_BENCH_SYNC=$sync"); fi
    if ! "${settings[@]}" timeout 120 "${command[@]}" > "$log" 2>&1; then
      printf 'failed profile=%s backend=%s sample=%s\n' "$profile" "$backend" "$sample" >&2
      cat "$log" >&2
      exit 1
    fi
    if [[ "$driver" == edit ]]; then rm -rf -- "$work/edit-$profile-$backend"; fi
    extract_metrics "$profile" "$backend" "$driver" "$log" >> "$results/samples.tsv"
  done
}

for ((sample = 1; sample <= runs; ++sample)); do
  for policy in nosync durable; do
    sync=0
    if [[ "$policy" == durable ]]; then sync=1; fi
    export LOCKDC_COLLECTION_BENCH_SYNC=$sync
    for workload in state metadata delete compact delete-compact scope staged \
      sdk-read-points sdk-scan-metadata read-many read-points scan-bodies scan-metadata \
      collection collection-compact collection-maintain; do
      pair "$policy-$workload" state "$workload" 24 4096 "$sync" 2
    done
    for workload in queue shared-queue outbox shared-outbox xa index shared-index; do
      pair "$policy-$workload" lifecycle "$workload" 12 4096 "$sync" 2 --reopen
    done
    if [[ "$policy" == nosync ]]; then
      for workload in index shared-index; do
        pair "$policy-$workload-stable" lifecycle "$workload" \
          12 4096 "$sync" 2 --reopen --stable-index
      done
    fi
    pair "$policy-lease" sdk 32 pouch-lease
    pair "$policy-state-api" sdk 24 pouch-state-api
    pair "$policy-attachment" sdk 16 pouch-attachment
    pair "$policy-transaction" sdk 12 pouch-transaction
    pair "$policy-queue-delivery" sdk 12 pouch-queue-delivery
    pair "$policy-queue-watch" sdk 6 pouch-queue-watch
    pair "$policy-collection-control" sdk 12 collection-control
    pair "$policy-outbox-command-wait" sdk 16 outbox-command-wait
    pair "$policy-outbox-reconcile" sdk 16 outbox-reconcile
    pair "$policy-outbox-stateful-warm" sdk 16 outbox-stateful-delivery-warm
    pair "$policy-namespace-index-reopen" sdk 96 pouch-perf-flush-reopen
    pair "$policy-namespace-full-text" sdk 96 pouch-perf-full-text-reopen-keys
    for index in 0 1; do
      LOCKDC_COLLECTION_BENCH_INDEX=$index LOCKDC_COLLECTION_BENCH_BUILD_INDEX=$index \
        pair "$policy-collection-filter-index-$index" sdk 128 collections
    done
    for append in pull staged push; do
      push=0 staged=0
      case "$append" in
        push) push=1 ;;
        staged) staged=1 ;;
      esac
      LOCKDC_COLLECTION_BENCH_PUSH=$push LOCKDC_COLLECTION_BENCH_STAGED=$staged \
        LOCKDC_COLLECTION_BENCH_SHARED=1 LOCKDC_COLLECTION_BENCH_PEERS=1 \
        LOCKDC_COLLECTION_BENCH_WRITERS=4 \
        pair "$policy-collection-$append-contended" sdk 96 collections
    done
    LOCKDC_COLLECTION_BENCH_CRYPTO=1 LOCKDC_COLLECTION_BENCH_COMPRESS=1 \
      LOCKDC_COLLECTION_BENCH_PAYLOAD=4096 LOCKDC_COLLECTION_BENCH_PUSH=1 \
      pair "$policy-collection-encrypted-zlib" sdk 48 collections
    LOCKDC_COLLECTION_BENCH_MAX_ENTRIES=32 \
      pair "$policy-collection-retention" sdk 128 collections
    LOCKDC_COLLECTION_BENCH_MAX_ENTRIES=32 LOCKDC_COLLECTION_BENCH_INDEX=1 \
      LOCKDC_COLLECTION_BENCH_BUILD_INDEX=1 \
      pair "$policy-collection-retention-indexed" sdk 128 collections
    for style in suffix scatter; do
      pair "$policy-collection-edit-$style" edit edit 256 32 4096 plain "$policy" "$style"
    done
    pair "$policy-collection-edit-append" edit edit 256 0 4096 plain "$policy" suffix
  done
  printf 'completed sample %d/%d\n' "$sample" "$runs"
done

# The input is ordered by workload, metric, backend, then numerical timing.
sort -t $'\t' -k1,1 -k2,2 -k3,3 -k5,5g "$results/samples.tsv" |
  awk -F '\t' -v runs="$runs" '
    BEGIN { print "profile\tmetric\tsingle_file_ms\tmultifile_ms\tsingle_file_over_multifile" }
    function flush(    f, s, i) {
      if (profile == "") return
      if (count["multifile"] != runs || count["single-file"] != runs) {
        printf "incomplete comparison for %s %s\n", profile, metric > "/dev/stderr"
        failed = 1
        return
      }
      i = (runs + 1) / 2
      f = timing["multifile", i]
      s = timing["single-file", i]
      printf "%s\t%s\t%.6f\t%.6f\t%s\n", profile, metric, s, f,
        (f > 0 ? sprintf("%.3f", s / f) : "n/a")
    }
    $1 != profile || $2 != metric {
      flush()
      delete count
      delete timing
      profile = $1
      metric = $2
    }
    { timing[$3, ++count[$3]] = $5 }
    END { flush(); exit failed }
  ' > "$results/medians.tsv"
printf 'paired medians=%s\n' "$results/medians.tsv"
printf 'elapsed_seconds=%d\n' "$SECONDS" | tee -a "$results/run.txt"
