#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
timed_bin="$script_dir/run_timed.sh"
workspace="$repo_root/build/devenv"
native=false
if [ "${1:-}" = --native ] && [ "$#" -eq 2 ]; then
  native=true
  mode=all
  e2e_build_dir=$(CDPATH= cd -- "$2" && pwd -P)
  fixture_bin="$repo_root/build/e2e-fixtures"
  for fixture in lockd minio mc; do
    if [ ! -x "$fixture_bin/$fixture" ]; then
      echo "Missing $fixture_bin/$fixture. Run bash scripts/build-e2e-fixtures.sh first." >&2
      exit 1
    fi
  done
  # shellcheck source=test-e2e-native.sh
  source "$script_dir/test-e2e-native.sh"
else
  mode=${1:-all}
  if { [ "$mode" != all ] && [ "$mode" != examples ]; } || [ "$#" -gt 1 ]; then
    echo 'usage: scripts/test-e2e.sh [all|examples|--native BUILD_DIR]' >&2
    exit 2
  fi
  e2e_preset=${LOCKDC_E2E_PRESET:-e2e}
  case "$e2e_preset" in debug|e2e) ;; *) echo 'LOCKDC_E2E_PRESET must be debug or e2e' >&2; exit 2 ;; esac
  e2e_build_dir="$repo_root/build/$e2e_preset"
fi

if [ "$mode" = "examples" ]; then
  e2e_build_target=lockdc_e2e_example_tests
else
  e2e_build_target=lockdc_e2e_tests
fi

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}
ctest_parallel_level=${LOCKDC_CTEST_PARALLEL_LEVEL:-4}

# E2E links and loads against the configured lifecycle dependency roots only.
unset LD_LIBRARY_PATH
export LOCKDC_TEST_ALLOCATOR_TRACKING=1

wait_for_file() {
  path=$1
  label=$2
  count=0
  while [ ! -s "$path" ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $path" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_socket() {
  path=$1
  label=$2
  count=0
  while [ ! -S "$path" ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $path" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_tcp() {
  host=$1
  port=$2
  label=$3
  count=0
  while ! (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $host:$port" >&2
      exit 1
    fi
    sleep 1
  done
  exec 3>&-
}

probe_lockd_endpoint() {
  local probe_output probe_status=0
  endpoint=$1
  bundle=$2
  example_bin=$3

  probe_output=$(env \
    LOCKDC_URL="$endpoint" \
    LOCKDC_CLIENT_PEM="$bundle" \
    LOCKDC_KEY="e2e-readiness-$$-$(date +%s)" \
    LOCKDC_OWNER="e2e-readiness" \
    "$example_bin" 2>&1) || probe_status=$?
  if [[ "$probe_output" == *"liblockdc allocator:"* ]]; then
    printf '%s\n' "$probe_output" >&2
    exit 1
  fi
  return "$probe_status"
}

wait_for_lockd_probe() {
  endpoint=$1
  bundle=$2
  label=$3
  example_bin=$4
  count=0

  while :; do
    count=$((count + 1))
    if probe_lockd_endpoint "$endpoint" "$bundle" "$example_bin"; then
      return 0
    fi
    if [ "$count" -ge 30 ]; then
      printf '%s\n' "Timed out waiting for $label lockd API readiness at $endpoint" >&2
      exit 1
    fi
    sleep 1
  done
}

select_active_disk_endpoint() {
  disk_a_endpoint=$1
  disk_b_endpoint=$2
  bundle=$3
  example_bin=$4
  count=0

  while :; do
    count=$((count + 1))
    if probe_lockd_endpoint "$disk_a_endpoint" "$bundle" "$example_bin"; then
      printf '%s\n' "$disk_a_endpoint"
      return 0
    fi
    if probe_lockd_endpoint "$disk_b_endpoint" "$bundle" "$example_bin"; then
      printf '%s\n' "$disk_b_endpoint"
      return 0
    fi
    if [ "$count" -ge 30 ]; then
      printf '%s\n' "Timed out waiting for an active disk lockd endpoint" >&2
      exit 1
    fi
    sleep 1
  done
}

cd "$repo_root"
# One checkout has one service graph. A second E2E runner must not stop the
# first runner's services or rewrite its configuration or logs.
command -v flock >/dev/null || { echo 'flock is required to serialize E2E runs' >&2; exit 1; }
source "$script_dir/assert_generated_path.sh"
lockdc_assert_generated_path "$repo_root" "$workspace"
[ ! -L "$workspace" ] || { echo 'build/devenv must not be a symlink' >&2; exit 1; }
mkdir -p -m 700 "$workspace"
exec 9> "$workspace/e2e.lock"
flock -n 9 || { echo 'E2E is already running in this checkout' >&2; exit 1; }
"$script_dir/devenv.sh" render
source "$workspace/env.sh"
export TMPDIR="$workspace/tmp"
failed_command=
trap 'failed_command=$BASH_COMMAND' ERR
cleanup() {
  rc=$?
  trap - EXIT
  if [ "$rc" -ne 0 ]; then
    printf 'E2E failed (%s): %s\nDisk: %s\nS3: %s\n' "$rc" "${failed_command:-$BASH_COMMAND}" "$LOCKDC_E2E_DISK_ENDPOINT" "$LOCKDC_E2E_S3_ENDPOINT" >&2
    if "$native"; then
      native_service_logs
    else
      "$script_dir/devenv.sh" ps >&2 || true
      "$script_dir/devenv.sh" logs > "$workspace/logs/e2e-services.log" 2>&1 || true
      tail -n 200 "$workspace/logs/e2e-services.log" >&2
      echo "Full service logs: $workspace/logs/e2e-services.log" >&2
    fi
  fi
  if "$native"; then
    stop_native_services
  elif [ "${LOCKDC_E2E_KEEP_DEVSERVICES:-0}" = 1 ]; then
    echo 'Keeping E2E services: LOCKDC_E2E_KEEP_DEVSERVICES=1' >&2
  elif ! "$script_dir/devenv.sh" down; then
    echo 'E2E service teardown failed; inspect make dev-ps and make dev-logs' >&2
    # Preserve a test failure or interruption; cleanup failure alone fails
    # an otherwise successful run.
    if [ "$rc" = 0 ]; then rc=1; fi
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if "$native"; then
  start_native_services
else
  "$timed_bin" "e2e build" "$script_dir/build.sh" "$e2e_preset" "$e2e_build_target"
  if ! "$script_dir/devenv.sh" running; then
    "$timed_bin" "e2e start" "$script_dir/devenv.sh" up
  fi
fi

wait_for_file "$LOCKDC_E2E_DISK_BUNDLE" "disk client bundle"
wait_for_file "$LOCKDC_E2E_S3_BUNDLE" "s3 client bundle"
wait_for_socket "$LOCKDC_E2E_MEM_SOCKET" "mem uds socket"
wait_for_tcp "127.0.0.1" "$LOCKDC_DISK_A_PORT" "disk lockd tcp listener"
wait_for_tcp "127.0.0.1" "$LOCKDC_DISK_B_PORT" "disk-b lockd tcp listener"
wait_for_tcp "127.0.0.1" "$LOCKDC_S3_PORT" "s3 lockd tcp listener"

LOCKDC_E2E_DISK_ENDPOINT="$(select_active_disk_endpoint \
  "$LOCKDC_E2E_DISK_ENDPOINT" \
  "${LOCKDC_E2E_DISK_B_ENDPOINT:-https://localhost:$LOCKDC_DISK_B_PORT}" \
  "$LOCKDC_E2E_DISK_BUNDLE" \
  "$e2e_build_dir/examples/lc_example_acquire_lease_lifecycle")"
export LOCKDC_E2E_DISK_ENDPOINT
export LOCKDC_E2E_DISK_A_ENDPOINT="$LOCKDC_E2E_DISK_ENDPOINT"
export LOCKDC_E2E_DISK_A_BUNDLE="$LOCKDC_E2E_DISK_BUNDLE"
if [ "$LOCKDC_E2E_DISK_ENDPOINT" = "$LOCKDC_E2E_DISK_B_ENDPOINT" ]; then
  export LOCKDC_FALLBACK_URL="https://localhost:$LOCKDC_DISK_A_PORT"
else
  export LOCKDC_FALLBACK_URL="$LOCKDC_E2E_DISK_B_ENDPOINT"
fi
export LOCKDC_URL="$LOCKDC_E2E_DISK_ENDPOINT"
export LOCKDC_CLIENT_PEM="$LOCKDC_E2E_DISK_BUNDLE"

wait_for_lockd_probe \
  "$LOCKDC_E2E_DISK_ENDPOINT" \
  "$LOCKDC_E2E_DISK_BUNDLE" \
  "disk" \
  "$e2e_build_dir/examples/lc_example_acquire_lease_lifecycle"
wait_for_lockd_probe \
  "$LOCKDC_E2E_S3_ENDPOINT" \
  "$LOCKDC_E2E_S3_BUNDLE" \
  "s3" \
  "$e2e_build_dir/examples/lc_example_acquire_lease_lifecycle"

if [ "$mode" = "examples" ]; then
  "$timed_bin" "e2e examples" ctest --test-dir "$e2e_build_dir" --output-on-failure --progress --stop-on-failure \
    --output-log "$workspace/logs/ctest.log" \
    --timeout "$ctest_timeout" --parallel "$ctest_parallel_level" --no-tests=error -L examples
else
  ctest_args=()
  if ! "$native"; then ctest_args+=(--stop-on-failure); fi
  "$timed_bin" "e2e ctest" ctest --test-dir "$e2e_build_dir" --output-on-failure --progress \
    "${ctest_args[@]}" --output-log "$workspace/logs/ctest.log" \
    --timeout "$ctest_timeout" --parallel "$ctest_parallel_level" --no-tests=error -L e2e
fi
