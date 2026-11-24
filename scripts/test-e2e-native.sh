#!/usr/bin/env bash
# Sourced by test-e2e.sh with the same checkout lock and rendered endpoints
# used by the Podman fixture. Keep all listeners on the loopback interface.
native_pids=()
native_logs=()
native_mem_started=false

start_native_service() {
  local name=$1
  shift
  local log="$workspace/logs/native-$name.log"
  "$@" >"$log" 2>&1 9>&- &
  native_pids+=("$!")
  native_logs+=("$log")
}

native_credentials() {
  local config=$1
  local lockd="$fixture_bin/lockd"
  [ -s "$config/ca.pem" ] ||
    "$lockd" auth new ca --cn lockd-root --out "$config/ca.pem"
  [ -s "$config/server.pem" ] ||
    "$lockd" auth new server --ca-in "$config/ca.pem" --cn lockd-anywhere \
      --hosts localhost,127.0.0.1 --out "$config/server.pem"
  if [ "$2" = server ]; then return; fi
  [ -s "$config/client.pem" ] ||
    "$lockd" auth new client --ca-in "$config/ca.pem" --cn lockd-client \
      -n default=rw --out "$config/client.pem"
  [ -s "$config/tc-client.pem" ] ||
    "$lockd" auth new tcclient --ca-in "$config/ca.pem" --cn lockd-tc-client \
      --out "$config/tc-client.pem"
}

start_native_services() {
  local port attempt
  local lockd="$fixture_bin/lockd"
  local minio_url="http://127.0.0.1:$LOCKDC_MINIO_API_PORT"
  local access_key=lockddev secret_key=lockddevpass
  local disk_config="$workspace/credentials/disk"
  local s3_config="$workspace/credentials/s3"
  local mem_config="$workspace/credentials/mem"
  local mc_config="$workspace/state/mc"

  for port in "$LOCKDC_DISK_A_PORT" "$LOCKDC_DISK_B_PORT" "$LOCKDC_S3_PORT" \
    "$LOCKDC_MINIO_API_PORT" "$LOCKDC_MINIO_CONSOLE_PORT"; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "Native E2E needs port $port. Stop the existing checkout services first." >&2
      return 1
    fi
  done
  if [ -e "$LOCKDC_E2E_MEM_SOCKET" ]; then
    echo "Native E2E needs the Unix socket path $LOCKDC_E2E_MEM_SOCKET" >&2
    return 1
  fi

  native_credentials "$disk_config" clients
  native_credentials "$s3_config" clients
  native_credentials "$mem_config" server
  start_native_service minio env MINIO_ROOT_USER="$access_key" MINIO_ROOT_PASSWORD="$secret_key" \
    "$fixture_bin/minio" server "$workspace/state/minio" \
    --address "127.0.0.1:$LOCKDC_MINIO_API_PORT" \
    --console-address "127.0.0.1:$LOCKDC_MINIO_CONSOLE_PORT"
  for ((attempt=0; attempt<60; attempt++)); do
    if curl --fail --silent --max-time 2 "$minio_url/minio/health/ready" >/dev/null; then
      break
    fi
    sleep 1
  done
  if [ "$attempt" -eq 60 ]; then
    echo "Timed out waiting for MinIO at $minio_url" >&2
    return 1
  fi
  "$fixture_bin/mc" --config-dir "$mc_config" alias set local "$minio_url" "$access_key" "$secret_key"
  "$fixture_bin/mc" --config-dir "$mc_config" mb --ignore-existing local/lockd-client-s3
  "$fixture_bin/mc" --config-dir "$mc_config" anonymous set none local/lockd-client-s3

  start_native_service lockd-disk-a env LOCKD_CONFIG_DIR="$disk_config" LOCKD_OTLP_ENDPOINT= \
    "$lockd" --store "disk://$workspace/state/disk" --listen-proto tcp \
    --listen "127.0.0.1:$LOCKDC_DISK_A_PORT"
  start_native_service lockd-disk-b env LOCKD_CONFIG_DIR="$disk_config" LOCKD_OTLP_ENDPOINT= \
    "$lockd" --store "disk://$workspace/state/disk" --listen-proto tcp \
    --listen "127.0.0.1:$LOCKDC_DISK_B_PORT"
  start_native_service lockd-s3 env LOCKD_CONFIG_DIR="$s3_config" LOCKD_OTLP_ENDPOINT= \
    LOCKD_S3_ACCESS_KEY_ID="$access_key" LOCKD_S3_SECRET_ACCESS_KEY="$secret_key" \
    "$lockd" --store "s3://127.0.0.1:$LOCKDC_MINIO_API_PORT/lockd-client-s3/liblockdc?insecure=1&path-style=1" \
    --listen-proto tcp --listen "127.0.0.1:$LOCKDC_S3_PORT"
  start_native_service lockd-mem env LOCKD_CONFIG_DIR="$mem_config" LOCKD_OTLP_ENDPOINT= \
    "$lockd" --disable-mtls --connguard-enabled=false --store mem:// \
    --listen-proto unix --listen "$LOCKDC_E2E_MEM_SOCKET"
  native_mem_started=true
}

native_service_logs() {
  if [ "${#native_logs[@]}" -gt 0 ]; then tail -n 80 "${native_logs[@]}" >&2 || true; fi
}

stop_native_services() {
  if [ "${#native_pids[@]}" -gt 0 ]; then
    kill "${native_pids[@]}" 2>/dev/null || true
    wait "${native_pids[@]}" 2>/dev/null || true
  fi
  if "$native_mem_started"; then rm -f -- "$LOCKDC_E2E_MEM_SOCKET"; fi
}
