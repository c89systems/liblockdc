#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
mode=${1:-measure}
storage=${2:-"$root/build/pouch-space-comparison"}
runs=${POUCH_SPACE_RUNS:-5}
preopen_args=()
if [[ "$mode" == *-reopen ]]; then preopen_args=(--reopen); fi
case "$mode" in
  smoke) preset=debug ;;
  measure|rollover|streams|streams-long|streams-reopen|streams-long-reopen) preset=x86_64-linux-gnu-release ;;
  *) echo 'usage: compare_pouch_space.sh [smoke|measure|rollover|streams|streams-long|streams-reopen|streams-long-reopen] [ABSOLUTE_DIRECTORY]' >&2; exit 2 ;;
esac
case "$storage" in /*) ;; *) echo 'storage directory must be absolute' >&2; exit 2 ;; esac
case "$runs" in 3|5|7|9) ;; *) echo 'POUCH_SPACE_RUNS must be 3, 5, 7, or 9' >&2; exit 2 ;; esac
cd "$root"
# Only native glibc dependencies; do not invoke the broader release matrix.
bash scripts/deps.sh deps-x86_64-linux-gnu
cmake --preset "$preset" -DLOCKDC_BUILD_BENCHMARKS=ON \
  -DLOCKDC_BUILD_EXAMPLES=OFF -DLOCKDC_BUILD_E2E_TESTS=OFF
cmake --build --preset "$preset" --target pouch_space_test pouch_space_directory_test pouch_space_compare
if [ "$mode" = smoke ]; then
  ctest --test-dir "$root/build/$preset" -R '^pouch_space_' \
    --output-on-failure --no-tests=error --parallel 1
  exit 0
fi
mkdir -p -- "$storage"
probe="$root/build/$preset/experiments/pouch_space/pouch_space_compare"
printf 'native=%s filesystem=%s directory=%s runs=%s\n' \
  "$(uname -m)" "$(stat -f -c %T "$storage")" "$storage" "$runs"

profile() {
  local name=$1 rows=$2 bytes=$3 sync=$4 writers=$5 sample=$6 variant log count
  local variants=(space-state pouch-state space-collection pouch-collection)
  if [[ "$mode" == streams* ]]; then
    variants=(stream-state space-state pouch-state stream-collection space-collection pouch-collection)
  fi
  if ((sample % 2 == 0)); then
    variants=(pouch-collection space-collection pouch-state space-state)
    if [[ "$mode" == streams* ]]; then
      variants=(pouch-collection space-collection stream-collection pouch-state space-state stream-state)
    fi
  fi
  for variant in "${variants[@]}"; do
    count=$rows
    # Both rollover workloads publish 2,048 records: state writes twice,
    # collections append once. Immutable variants cross the metadata journal
    # boundary; canonical streams amortize their journal entries over grants.
    if { [ "$mode" = rollover ] || [[ "$mode" == streams-long* ]]; } && [[ "$variant" == *collection ]]; then
      count=$((rows * 2))
    fi
    log="$storage/$name-$variant-$sample.log"
    printf 'profile=%s sample=%s ' "$name" "$sample"
    if ! "$probe" "$variant" "$storage" "$count" "$bytes" "$sync" "$writers" 4096 "${preopen_args[@]}" > "$log" 2>&1; then
      cat "$log" >&2
      exit 1
    fi
    if rg -q 'liblockdc allocator:|ERROR: AddressSanitizer|runtime error:' "$log"; then
      cat "$log" >&2
      exit 1
    fi
    if { [ "$mode" = rollover ] || [[ "$mode" == streams-long* ]]; } && [[ "$variant" == space-* ]]; then
      if ! rg -q ' checkpoints=[1-9][0-9]* ' "$log"; then
        echo 'rollover workload did not checkpoint during writes' >&2
        cat "$log" >&2
        exit 1
      fi
    fi
    if [[ "$variant" == stream-* ]] && ! rg -q ' checkpoints=0 ' "$log"; then
      echo 'stream workload unexpectedly required a metadata checkpoint' >&2
      cat "$log" >&2
      exit 1
    fi
    cat "$log"
  done
}
for ((sample = 1; sample <= runs; ++sample)); do
  if [[ "$mode" == streams-long* ]]; then
    profile long-nosync-1w 1024 1024 0 1 "$sample"
    profile long-nosync-4w 1024 1024 0 4 "$sample"
    profile long-durable-1w 1024 1024 1 1 "$sample"
    profile long-durable-4w 1024 1024 1 4 "$sample"
    continue
  fi
  if [ "$mode" = rollover ]; then
    profile rollover-nosync-1w 1024 1024 0 1 "$sample"
    profile rollover-nosync-4w 1024 1024 0 4 "$sample"
    profile rollover-durable-1w 1024 1024 1 1 "$sample"
    profile rollover-durable-4w 1024 1024 1 4 "$sample"
    continue
  fi
  profile small-nosync-1w 256 1024 0 1 "$sample"
  profile small-nosync-4w 256 1024 0 4 "$sample"
  profile small-durable-1w 128 1024 1 1 "$sample"
  profile small-durable-4w 128 1024 1 4 "$sample"
  profile large-nosync-1w 32 65536 0 1 "$sample"
  profile large-nosync-4w 32 65536 0 4 "$sample"
  profile large-durable-1w 32 65536 1 1 "$sample"
  profile large-durable-4w 32 65536 1 4 "$sample"
done
