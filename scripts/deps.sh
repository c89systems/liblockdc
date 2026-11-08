#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
preset=${1:-deps-x86_64-linux-gnu}
postgres_setting=${LOCKDC_BUILD_POSTGRES:-OFF}
case "${2:-}" in
  --with-postgres) postgres_setting=ON ;;
  --without-postgres) postgres_setting=OFF ;;
  '') ;;
  *) echo 'expected --with-postgres or --without-postgres after the preset' >&2; exit 2 ;;
esac
if [ "$#" -gt 2 ]; then echo 'too many dependency arguments' >&2; exit 2; fi
case "$postgres_setting" in
  ON|on|TRUE|true|1) build_postgres=ON ;;
  OFF|off|FALSE|false|0) build_postgres=OFF ;;
  *) echo 'LOCKDC_BUILD_POSTGRES must be ON or OFF' >&2; exit 2 ;;
esac

unset LD_LIBRARY_PATH
dry_run=${LOCKDC_DEPS_DRY_RUN:-0}
download_timeout=${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT:-300}
local_download_root=${LOCKDC_DOWNLOAD_ROOT:-$repo_root/.cache/downloads}
lonejson_abi_version=${LOCKDC_LONEJSON_ABI_VERSION:-27}
liblql_abi_version=${LOCKDC_LIBLQL_ABI_VERSION:-0}

# shellcheck source=assert_generated_path.sh
source "$script_dir/assert_generated_path.sh"

case "$preset" in
  deps-x86_64-linux-gnu)
    cmake_preset="x86_64-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/x86_64-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/x86_64-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-x86_64-linux-musl)
    cmake_preset="x86_64-linux-musl-release"
    deps_root="$repo_root/.cache/deps/x86_64-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/x86_64-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-aarch64-linux-gnu)
    cmake_preset="aarch64-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/aarch64-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/aarch64-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-aarch64-linux-musl)
    cmake_preset="aarch64-linux-musl-release"
    deps_root="$repo_root/.cache/deps/aarch64-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/aarch64-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-armhf-linux-gnu)
    cmake_preset="armhf-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/armhf-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/armhf-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-armhf-linux-musl)
    cmake_preset="armhf-linux-musl-release"
    deps_root="$repo_root/.cache/deps/armhf-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/armhf-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-arm64-apple-darwin)
    cmake_preset="arm64-apple-darwin-release"
    deps_root="$repo_root/.cache/deps/arm64-apple-darwin"
    deps_build_root="$repo_root/.cache/deps-build/arm64-apple-darwin"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_TESTS=OFF
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
      -DLOCKDC_BUILD_LUA_BINDINGS=OFF
    )
    ;;
  *)
    echo "usage: scripts/deps.sh [deps-x86_64-linux-gnu|deps-x86_64-linux-musl|deps-aarch64-linux-gnu|deps-aarch64-linux-musl|deps-armhf-linux-gnu|deps-armhf-linux-musl|deps-arm64-apple-darwin]" >&2
    echo "       append --with-postgres to provision the optional cpktdb SDK" >&2
    exit 2
    ;;
esac
cmake_extra_args+=("-DLOCKDC_BUILD_POSTGRES=$build_postgres")

lockdc_assert_generated_path "$repo_root" "$local_download_root"
lockdc_assert_generated_path "$repo_root" "$deps_root"
lockdc_assert_generated_path "$repo_root" "$deps_build_root"

if [ "$dry_run" = "1" ]; then
  printf 'preset=%s\n' "$preset"
  printf 'cmake_preset=%s\n' "$cmake_preset"
  printf 'deps_root=%s\n' "$deps_root"
  printf 'deps_build_root=%s\n' "$deps_build_root"
  printf 'postgres=%s\n' "$build_postgres"
  exit 0
fi

manifest_path="$deps_root/manifest.txt"
mkdir -p "$deps_root"

resolve_cmake_cache_string() {
  local var_name=$1
  local override_value=$2
  local resolved_value=

  if [ -n "$override_value" ]; then
    printf '%s\n' "$override_value"
    return 0
  fi

  resolved_value=$(
    sed -n "s/^set(${var_name} \"\\(.*\\)\" CACHE STRING.*/\\1/p" \
      "$repo_root/CMakeLists.txt" | head -n1
  )
  if [ -z "$resolved_value" ]; then
    printf 'failed to resolve %s from CMakeLists.txt\n' "$var_name" >&2
    exit 1
  fi

  printf '%s\n' "$resolved_value"
}

cpkt_version=$(resolve_cmake_cache_string LOCKDC_CPKT_VERSION "${LOCKDC_CPKT_VERSION:-}")
openssl_version=$(resolve_cmake_cache_string LOCKDC_OPENSSL_VERSION "${LOCKDC_OPENSSL_VERSION:-}")
zlib_version=$(resolve_cmake_cache_string LOCKDC_ZLIB_VERSION "${LOCKDC_ZLIB_VERSION:-}")
curl_version=$(resolve_cmake_cache_string LOCKDC_CURL_VERSION "${LOCKDC_CURL_VERSION:-}")
nghttp2_version=$(resolve_cmake_cache_string LOCKDC_NGHTTP2_VERSION "${LOCKDC_NGHTTP2_VERSION:-}")
libssh2_version=$(resolve_cmake_cache_string LOCKDC_LIBSSH2_VERSION "${LOCKDC_LIBSSH2_VERSION:-}")
lonejson_version=$(resolve_cmake_cache_string LOCKDC_LONEJSON_VERSION "${LOCKDC_LONEJSON_VERSION:-}")
cmocka_version=$(resolve_cmake_cache_string LOCKDC_CMOCKA_VERSION "${LOCKDC_CMOCKA_VERSION:-}")
pslog_version=$(resolve_cmake_cache_string LOCKDC_PSLOG_VERSION "${LOCKDC_PSLOG_VERSION:-}")

cpkt_asset_name="cpkt-$cpkt_version-${preset#deps-}.tar.gz"
cpkt_download_url="https://github.com/c89systems/cpkt/releases/download/v$cpkt_version/$cpkt_asset_name"
case "$cpkt_asset_name" in
  cpkt-0.6.1-x86_64-linux-gnu.tar.gz)
    cpkt_asset_hash=99625b7889f78b6e467d38e7dbec08d9d0ed6f599ff4571244060ce67c6ca62f
    ;;
  cpkt-0.6.1-x86_64-linux-musl.tar.gz)
    cpkt_asset_hash=f4b1b0b2b257d2347be7d2b437dbb84891c19e39067bba24464b415dd76eb67e
    ;;
  cpkt-0.6.1-aarch64-linux-gnu.tar.gz)
    cpkt_asset_hash=3c5f172a50b7df7e37e99dbf52014d3b09114cbc143339f1109c23715fa117a1
    ;;
  cpkt-0.6.1-aarch64-linux-musl.tar.gz)
    cpkt_asset_hash=bf39981f557b1ed10940ee376bb81d0fa734d3d290e5d95cbc4f786ac466cb93
    ;;
  cpkt-0.6.1-armhf-linux-gnu.tar.gz)
    cpkt_asset_hash=c2200e3a4a05ed8807d0bb19b0042179bd47c7ce83174932f521d36aee11b935
    ;;
  cpkt-0.6.1-armhf-linux-musl.tar.gz)
    cpkt_asset_hash=eb1ed255f3ea60c82c3973f1246571872b4f0fd7866364e544b5627e5cd3ee98
    ;;
  cpkt-0.6.1-arm64-apple-darwin.tar.gz)
    cpkt_asset_hash=8f32fe4a9e477196d75a927af329dafcc91eda5e4b505012a7319edcf92e8579
    ;;
  *)
    printf 'unsupported cpkt release asset: %s\n' "$cpkt_asset_name" >&2
    exit 1
    ;;
esac

if [ "$build_postgres" = ON ]; then
  cpktdb_version=$(resolve_cmake_cache_string LOCKDC_CPKTDB_VERSION "${LOCKDC_CPKTDB_VERSION:-}")
  cpktdb_asset_name="cpktdb-$cpktdb_version-${preset#deps-}.tar.gz"
  cpktdb_download_url="https://github.com/c89systems/cpktdb/releases/download/v$cpktdb_version/$cpktdb_asset_name"
  case "$cpktdb_asset_name" in
    cpktdb-0.3.1-x86_64-linux-gnu.tar.gz)
      cpktdb_asset_hash=e18d96e0c7cc5456ec963a068075a2e3dc25966e81d11cb9beea6faf11273f3a
      ;;
    cpktdb-0.3.1-x86_64-linux-musl.tar.gz)
      cpktdb_asset_hash=d2299b9cd5c66037c68875ae4bf0b77ef89c22fb6f3372d9c08ff7f9d988fa90
      ;;
    cpktdb-0.3.1-aarch64-linux-gnu.tar.gz)
      cpktdb_asset_hash=08614da32caca445c3813c60a6e0ea81dad54467b8539adb213e61fefbea2db3
      ;;
    cpktdb-0.3.1-aarch64-linux-musl.tar.gz)
      cpktdb_asset_hash=d027eb9f77e37c84c835172a535a2fabbd01e59124a88237ab7fbc85f045dc61
      ;;
    cpktdb-0.3.1-armhf-linux-gnu.tar.gz)
      cpktdb_asset_hash=5215fffc8009019f748a60d51fc5e85f9f476c2c9b55a55ee444ec1bf43c0b51
      ;;
    cpktdb-0.3.1-armhf-linux-musl.tar.gz)
      cpktdb_asset_hash=33e47b891988f638433fcc65eb79e621999396eba902d9927eb26eb851dd8c6c
      ;;
    cpktdb-0.3.1-arm64-apple-darwin.tar.gz)
      cpktdb_asset_hash=f6483a79d768eb09e3258b404b6e5ced84f5b5b5e3c4be6500834f79957be919
      ;;
    *)
      printf 'unsupported cpktdb release asset: %s\n' "$cpktdb_asset_name" >&2
      exit 1
      ;;
  esac
fi

lonejson_asset_name="liblonejson-$lonejson_version-${preset#deps-}.tar.gz"
lonejson_download_url="https://github.com/c89systems/lonejson/releases/download/v$lonejson_version/$lonejson_asset_name"
case "$lonejson_asset_name" in
  liblonejson-0.47.0-x86_64-linux-gnu.tar.gz)
    lonejson_asset_hash=a2dbc1db962f8dcdf8398435043a85031c4f195d7e3d4ec9ee27b985a2f7c353
    ;;
  liblonejson-0.47.0-x86_64-linux-musl.tar.gz)
    lonejson_asset_hash=4f91b5d601f7264487ff6f3b103d15d8a954a94eb79cf515aea2dd16971b9784
    ;;
  liblonejson-0.47.0-aarch64-linux-gnu.tar.gz)
    lonejson_asset_hash=d6d6c98ec9e86a7f57bbf1f748976048bb8f99a069eb3ffed7e0f16e3223eae7
    ;;
  liblonejson-0.47.0-aarch64-linux-musl.tar.gz)
    lonejson_asset_hash=dc6ec6c4e694ede21f54a0b7fb8197df122f5b052441b9c4862dde10842d13ef
    ;;
  liblonejson-0.47.0-armhf-linux-gnu.tar.gz)
    lonejson_asset_hash=90bd654dda6461be943ebb9e198d43af489b2501502f02a421589968d78d7eaf
    ;;
  liblonejson-0.47.0-armhf-linux-musl.tar.gz)
    lonejson_asset_hash=fb4ac6dfaff38cdc229feb074e594b4eb1b5e30a5448906e0c91cf31d9d8924c
    ;;
  liblonejson-0.47.0-arm64-apple-darwin.tar.gz)
    lonejson_asset_hash=35d4fa113896fbcc3f8d5095a3e859fecf6b14423460541b8326aa32d9dcaeb8
    ;;
  *)
    printf 'unsupported lonejson release asset: %s\n' "$lonejson_asset_name" >&2
    exit 1
    ;;
esac

liblql_version=$(resolve_cmake_cache_string LOCKDC_LIBLQL_VERSION "${LOCKDC_LIBLQL_VERSION:-}")
liblql_asset_name="liblql-$liblql_version-${preset#deps-}.tar.gz"
liblql_download_url="https://github.com/c89systems/liblql/releases/download/v$liblql_version/$liblql_asset_name"
case "$liblql_asset_name" in
  liblql-0.4.1-x86_64-linux-gnu.tar.gz)
    liblql_asset_hash=d6b3d12afcf6ba2582cb9103cc6a6a97a7378870d969e0755d467d72b9f43030
    ;;
  liblql-0.4.1-x86_64-linux-musl.tar.gz)
    liblql_asset_hash=0379c745309924310155f0270d4199263ba8273ae138d7e5d45c956588aa608a
    ;;
  liblql-0.4.1-aarch64-linux-gnu.tar.gz)
    liblql_asset_hash=664a9c52ca606caac3bc4421a5ff24c403cb49f4b21f3394f51af98e21998e14
    ;;
  liblql-0.4.1-aarch64-linux-musl.tar.gz)
    liblql_asset_hash=bac01b66e77e10fd5b7b4fcfbf4f05b2e3950b18af93cf5aa7e0e680323c66a6
    ;;
  liblql-0.4.1-armhf-linux-gnu.tar.gz)
    liblql_asset_hash=e35bff9e4ca505707ca8ba4e9a9852c0e4cee7791c392bd97fff028898624372
    ;;
  liblql-0.4.1-armhf-linux-musl.tar.gz)
    liblql_asset_hash=84e244c555f879637f913697a1cf203f5260b22714e19f0dc40d40e405646c25
    ;;
  liblql-0.4.1-arm64-apple-darwin.tar.gz)
    liblql_asset_hash=2a387edea27c628ca575d8ee4ce77c6647708e0375498d12c5c86d86d0bcc70f
    ;;
  *)
    printf 'unsupported liblql release asset: %s\n' "$liblql_asset_name" >&2
    exit 1
    ;;
esac

pslog_asset_name="libpslog-$pslog_version-${preset#deps-}.tar.gz"
pslog_download_url="https://github.com/c89systems/libpslog/releases/download/v$pslog_version/$pslog_asset_name"
case "$pslog_asset_name" in
  libpslog-0.13.0-x86_64-linux-gnu.tar.gz)
    pslog_asset_hash=3fa68da666c70470f0f09041b05561f807cd241fae30421410033932d2b8c1c4
    ;;
  libpslog-0.13.0-x86_64-linux-musl.tar.gz)
    pslog_asset_hash=c195881181b1cbe08acb705ab91a8cd0e8cabe470c9fdb391b40820529836f80
    ;;
  libpslog-0.13.0-aarch64-linux-gnu.tar.gz)
    pslog_asset_hash=d15b172c076324c05d7596cb7b01ba036c2d1aeb0eb7264bd4e77cc6c103c71f
    ;;
  libpslog-0.13.0-aarch64-linux-musl.tar.gz)
    pslog_asset_hash=44c295a5ade15ce30dcbca6ba7775b6ea521560941306312e33bbbfade2f8059
    ;;
  libpslog-0.13.0-armhf-linux-gnu.tar.gz)
    pslog_asset_hash=55bfb03089697d01abfad14bb631c6cc69bfc361f09e8e0064bba1f48c086290
    ;;
  libpslog-0.13.0-armhf-linux-musl.tar.gz)
    pslog_asset_hash=5ba1d3c16f864a3c6010ece642b6ecaa85273c2ba282d72856c9e7af6ac6320e
    ;;
  libpslog-0.13.0-arm64-apple-darwin.tar.gz)
    pslog_asset_hash=7f3b8e89f7f1a3ac27d47cb92aa68c4989ed4c8cdb51c80c81692412ba75cacb
    ;;
  *)
    printf 'unsupported libpslog release asset: %s\n' "$pslog_asset_name" >&2
    exit 1
    ;;
esac

dependency_target_id=${preset#deps-}

toolchain_value() {
  local key=$1
  printf '%s\n' "$toolchain_description" | sed -n "s/^${key}=//p" | head -n1
}

case "$dependency_target_id" in
  *-linux-*)
    toolchain_description=$("$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id")
    if ! printf '%s\n' "$toolchain_description" | grep -q '^status=ready$'; then
      "$repo_root/scripts/cpkt-toolchains.sh" ensure "$dependency_target_id" >/dev/null
      toolchain_description=$("$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id")
    fi
    if ! printf '%s\n' "$toolchain_description" | grep -q '^status=ready$'; then
      printf 'Bootlin toolchain is not ready for %s\n%s\n' "$dependency_target_id" "$toolchain_description" >&2
      exit 1
    fi
    toolchain_source=$(toolchain_value source)
    toolchain_archive=$(toolchain_value archive)
    toolchain_root=$(toolchain_value root)
    toolchain_prefix=$(toolchain_value prefix)
    toolchain_sysroot=$(toolchain_value sysroot)
    toolchain_target_triple=$(toolchain_value target_triple)
    compiler_path=$(toolchain_value cc)
    compiler_machine=$("$compiler_path" -dumpmachine 2>/dev/null || echo unknown)
    compiler_version=$("$compiler_path" --version 2>/dev/null | head -n1 || echo unknown)
    compiler=$(basename -- "$compiler_path")
    ;;
  *)
    if [ -z "${CPKT_OSXCROSS_HOST:-}" ] && [ -n "${LOCKDC_OSXCROSS_HOST:-}" ]; then
      export CPKT_OSXCROSS_HOST=$LOCKDC_OSXCROSS_HOST
    fi
    "$repo_root/scripts/cpkt-toolchains.sh" ensure "$dependency_target_id" >/dev/null
    toolchain_description=$("$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id")
    toolchain_source=$(toolchain_value source)
    toolchain_archive=$(toolchain_value archive)
    toolchain_root=$(toolchain_value root)
    toolchain_prefix=$(toolchain_value prefix)
    toolchain_sysroot=$(toolchain_value sysroot)
    toolchain_target_triple=$(toolchain_value target_triple)
    compiler_path=$(toolchain_value cc)
    if [ -z "$compiler_path" ] || ! printf '%s\n' "$toolchain_description" | grep -q '^status=ready$'; then
      printf 'Darwin toolchain is not ready; run scripts/cpkt-toolchains.sh ensure arm64-apple-darwin\n' >&2
      exit 1
    fi
    compiler_machine=$("$compiler_path" -dumpmachine 2>/dev/null || echo unknown)
    compiler_version=$("$compiler_path" --version 2>/dev/null | head -n1 || echo unknown)
    compiler=$(basename -- "$compiler_path")
    ;;
esac
toolchain_root_identity=$(basename -- "$toolchain_root")
toolchain_sysroot_identity=$toolchain_sysroot
case "$toolchain_sysroot_identity" in
  "$toolchain_root"/*)
    toolchain_sysroot_identity=${toolchain_sysroot_identity#"$toolchain_root"/}
    ;;
esac

fingerprint=$(
  {
    cat "$repo_root/CMakePresets.json"
    cat "$repo_root/cmake/LcDependencies.cmake"
    if [ -f "$repo_root/cmake/prune_dependency_install_tree.cmake" ]; then
      cat "$repo_root/cmake/prune_dependency_install_tree.cmake"
    fi
    cat "$repo_root/scripts/deps.sh"
    if [ -d "$repo_root/cmake/toolchains" ]; then
      find "$repo_root/cmake/toolchains" -maxdepth 1 -type f | sort | while read -r file; do
        cat "$file"
      done
    fi
  } | sha256sum | awk '{print $1}'
)

manifest="compiler=$compiler
machine=$compiler_machine
version=$compiler_version
fingerprint=$fingerprint
preset=$preset
toolchain_target_id=$dependency_target_id
toolchain_source=$toolchain_source
toolchain_archive=$toolchain_archive
toolchain_root=$toolchain_root_identity
toolchain_prefix=$toolchain_prefix
toolchain_sysroot=$toolchain_sysroot_identity
toolchain_target_triple=$toolchain_target_triple
cpkt_version=$cpkt_version
cpkt_asset_name=$cpkt_asset_name
cpkt_asset_hash=$cpkt_asset_hash
postgres=$build_postgres
openssl_version=$openssl_version
zlib_version=$zlib_version
curl_version=$curl_version
nghttp2_version=$nghttp2_version
libssh2_version=$libssh2_version
lonejson_version=$lonejson_version
lonejson_asset_name=$lonejson_asset_name
lonejson_asset_hash=$lonejson_asset_hash
liblql_version=$liblql_version
liblql_asset_name=$liblql_asset_name
liblql_asset_hash=$liblql_asset_hash
cmocka_version=$cmocka_version
pslog_version=$pslog_version
pslog_asset_name=$pslog_asset_name
pslog_asset_hash=$pslog_asset_hash"
if [ "$build_postgres" = ON ]; then
  manifest="$manifest
cpktdb_version=$cpktdb_version
cpktdb_asset_name=$cpktdb_asset_name
cpktdb_asset_hash=$cpktdb_asset_hash"
fi

manifest_value() {
  local key=$1
  local manifest_file=$2
  sed -n "s/^${key}=//p" "$manifest_file" | head -n1
}

stage_dependency_license() {
  local install_subdir=$1
  local package_name=$2
  local source_path=$3
  local destination_path="$deps_root/$install_subdir/install/share/doc/liblockdc-third-party/$package_name/LICENSE.txt"

  if [ ! -f "$source_path" ]; then
    printf 'missing license file for %s: %s\n' "$package_name" "$source_path" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$destination_path")"
  cp "$source_path" "$destination_path"
}

copy_matching_files() {
  local destination=$1
  shift
  local copied=0
  local candidate

  mkdir -p "$destination"
  for candidate in "$@"; do
    if [ -e "$candidate" ] || [ -L "$candidate" ]; then
      cp -a "$candidate" "$destination/"
      copied=1
    fi
  done
  if [ "$copied" -eq 0 ]; then
    printf 'no cpkt files matched for destination: %s\n' "$destination" >&2
    exit 1
  fi
}

stage_cpkt_license() {
  local package_name=$1
  local source_path="$deps_root/cpkt/install/share/doc/cpkt/core/third_party/$package_name/LICENSE"
  local destination_dir="$deps_root/$package_name/install/share/doc/liblockdc-third-party/$package_name"

  if [ ! -f "$source_path" ]; then
    printf 'missing cpkt license for %s: %s\n' "$package_name" "$source_path" >&2
    exit 1
  fi

  mkdir -p "$destination_dir"
  cp "$source_path" "$destination_dir/LICENSE.txt"
}

acquire_verified_archive() {
  local component=$1
  local url=$2
  local expected_hash=$3
  local archive_name=$4
  local output_path=$5
  local cache_args=()

  if [ "${CPKT_DEPENDENCY_CACHE+x}" ]; then
    cache_args=(-DCPKT_DEPENDENCY_CACHE="$CPKT_DEPENDENCY_CACHE")
  fi

  cmake \
    -DLOCKDC_ARCHIVE_COMPONENT="$component" \
    -DLOCKDC_ARCHIVE_URL="$url" \
    -DLOCKDC_ARCHIVE_SHA256="$expected_hash" \
    -DLOCKDC_ARCHIVE_NAME="$archive_name" \
    -DLOCKDC_ARCHIVE_OUTPUT="$output_path" \
    -DLOCKDC_ARCHIVE_TIMEOUT="$download_timeout" \
    "${cache_args[@]}" \
    -P "$repo_root/cmake/acquire_verified_archive.cmake"
}

download_cpkt_bundle() {
  local archive_path="$local_download_root/$cpkt_asset_name"
  local extract_root="$deps_root/cpkt/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "cpkt" "$cpkt_download_url" "$cpkt_asset_hash" "$cpkt_asset_name" "$archive_path"

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$cpkt_asset_hash" ]; then
    printf 'cpkt checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$cpkt_asset_name" "$cpkt_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_cpktdb_bundle() {
  local archive_path="$local_download_root/$cpktdb_asset_name"
  local extract_root="$deps_root/cpktdb/install"

  acquire_verified_archive "cpktdb" "$cpktdb_download_url" "$cpktdb_asset_hash" "$cpktdb_asset_name" "$archive_path"
  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_lonejson_bundle() {
  local archive_path="$local_download_root/$lonejson_asset_name"
  local extract_root="$deps_root/lonejson/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "lonejson" "$lonejson_download_url" "$lonejson_asset_hash" "$lonejson_asset_name" "$archive_path"

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$lonejson_asset_hash" ]; then
    printf 'lonejson checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$lonejson_asset_name" "$lonejson_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_liblql_bundle() {
  local archive_path="$local_download_root/$liblql_asset_name"
  local extract_root="$deps_root/liblql/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "liblql" "$liblql_download_url" "$liblql_asset_hash" "$liblql_asset_name" "$archive_path"

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$liblql_asset_hash" ]; then
    printf 'liblql checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$liblql_asset_name" "$liblql_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_pslog_bundle() {
  local archive_path="$local_download_root/$pslog_asset_name"

  mkdir -p "$local_download_root"
  acquire_verified_archive "libpslog" "$pslog_download_url" "$pslog_asset_hash" "$pslog_asset_name" "$archive_path"
}

stage_cpkt_component_layout() {
  local cpkt_root="$deps_root/cpkt/install"

  download_cpkt_bundle
  rm -rf \
    "$deps_root/openssl/install" \
    "$deps_root/curl/install" \
    "$deps_root/nghttp2/install" \
    "$deps_root/libssh2/install" \
    "$deps_root/zlib/install"

  copy_matching_files "$deps_root/openssl/install/include" "$cpkt_root/include/openssl"
  copy_matching_files "$deps_root/openssl/install/lib" "$cpkt_root"/lib/libssl* "$cpkt_root"/lib/libcrypto*
  copy_matching_files "$deps_root/openssl/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libssl.pc "$cpkt_root"/lib/pkgconfig/libcrypto.pc "$cpkt_root"/lib/pkgconfig/openssl.pc
  copy_matching_files "$deps_root/openssl/install/lib/cmake/OpenSSL" "$cpkt_root"/lib/cmake/OpenSSL/*.cmake
  stage_cpkt_license openssl

  copy_matching_files "$deps_root/curl/install/include" "$cpkt_root/include/curl"
  copy_matching_files "$deps_root/curl/install/lib" "$cpkt_root"/lib/libcurl*
  copy_matching_files "$deps_root/curl/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libcurl.pc
  copy_matching_files "$deps_root/curl/install/lib/cmake/CURL" "$cpkt_root"/lib/cmake/CURL/*.cmake
  stage_cpkt_license curl

  copy_matching_files "$deps_root/nghttp2/install/include" "$cpkt_root/include/nghttp2"
  copy_matching_files "$deps_root/nghttp2/install/lib" "$cpkt_root"/lib/libnghttp2*
  copy_matching_files "$deps_root/nghttp2/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libnghttp2.pc
  copy_matching_files "$deps_root/nghttp2/install/lib/cmake/nghttp2" "$cpkt_root"/lib/cmake/nghttp2/*.cmake
  stage_cpkt_license nghttp2

  copy_matching_files "$deps_root/libssh2/install/include" \
    "$cpkt_root/include/libssh2.h" \
    "$cpkt_root/include/libssh2_publickey.h" \
    "$cpkt_root/include/libssh2_sftp.h"
  copy_matching_files "$deps_root/libssh2/install/lib" "$cpkt_root"/lib/libssh2*
  copy_matching_files "$deps_root/libssh2/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libssh2.pc
  copy_matching_files "$deps_root/libssh2/install/lib/cmake/libssh2" "$cpkt_root"/lib/cmake/libssh2/*.cmake
  stage_cpkt_license libssh2

  copy_matching_files "$deps_root/zlib/install/include" \
    "$cpkt_root/include/zlib.h" \
    "$cpkt_root/include/zconf.h"
  copy_matching_files "$deps_root/zlib/install/lib" "$cpkt_root"/lib/libz*
  copy_matching_files "$deps_root/zlib/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/zlib.pc
  copy_matching_files "$deps_root/zlib/install/lib/cmake/zlib" "$cpkt_root"/lib/cmake/zlib/*.cmake
  stage_cpkt_license zlib
}

prune_dependency_install_trees() {
  cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_EXTERNAL_ROOT="$deps_root" \
    -P "$repo_root/cmake/prune_dependency_install_tree.cmake"
}

assert_dependency_install_tree_privacy() {
  cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_SCAN_LABEL="dependency install tree $deps_root" \
    -DLOCKDC_SCAN_PATHS="$deps_root" \
    -P "$repo_root/tests/release_privacy_scan.cmake"
}

reset_dependency_build_root() {
  local attempt=1

  while [ "$attempt" -le 5 ]; do
    rm -rf "$deps_build_root"
    if [ ! -e "$deps_build_root" ]; then
      return 0
    fi
    sleep 1
    attempt=$((attempt + 1))
  done

  printf 'failed to reset dependency build root: %s\n' "$deps_build_root" >&2
  exit 1
}

case "$preset" in
  deps-arm64-apple-darwin)
    shared_ext=dylib
    libssh2_shared_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    libssh2_shared_soname_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    libssh2_shared_versioned_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    zlib_shared_path="$deps_root/zlib/install/lib/libz.${shared_ext}"
    zlib_shared_soname_path="$deps_root/zlib/install/lib/libz.1.${shared_ext}"
    zlib_shared_versioned_path="$deps_root/zlib/install/lib/libz.$zlib_version.${shared_ext}"
    pslog_shared_path="$deps_root/pslog/install/lib/libpslog.0.${shared_ext}"
    lonejson_shared_path="$deps_root/lonejson/install/lib/liblonejson.${lonejson_abi_version}.${shared_ext}"
    liblql_shared_path="$deps_root/liblql/install/lib/liblql.${liblql_abi_version}.${shared_ext}"
    ;;
  *)
    shared_ext=so
    libssh2_shared_path="$deps_root/libssh2/install/lib/libssh2.so"
    libssh2_shared_soname_path="$deps_root/libssh2/install/lib/libssh2.so.1"
    libssh2_shared_versioned_path="$deps_root/libssh2/install/lib/libssh2.so.1.0.1"
    zlib_shared_path="$deps_root/zlib/install/lib/libz.so"
    zlib_shared_soname_path="$deps_root/zlib/install/lib/libz.so.1"
    zlib_shared_versioned_path="$deps_root/zlib/install/lib/libz.so.$zlib_version"
    pslog_shared_path="$deps_root/pslog/install/lib/libpslog.so.0"
    lonejson_shared_path="$deps_root/lonejson/install/lib/liblonejson.so.${lonejson_abi_version}"
    liblql_shared_path="$deps_root/liblql/install/lib/liblql.so.${liblql_abi_version}"
    ;;
esac
curl_shared_path="$deps_root/curl/install/lib/libcurl.${shared_ext}"
openssl_ssl_shared_path="$deps_root/openssl/install/lib/libssl.${shared_ext}"
openssl_crypto_shared_path="$deps_root/openssl/install/lib/libcrypto.${shared_ext}"
nghttp2_shared_path="$deps_root/nghttp2/install/lib/libnghttp2.${shared_ext}"

deps_ready=1
required_paths=(
  "$deps_root/openssl/install/lib/libssl.a"
  "$deps_root/openssl/install/lib/libcrypto.a"
  "$openssl_ssl_shared_path"
  "$openssl_crypto_shared_path"
  "$deps_root/nghttp2/install/lib/libnghttp2.a"
  "$nghttp2_shared_path"
  "$deps_root/libssh2/install/lib/libssh2.a"
  "$libssh2_shared_path"
  "$libssh2_shared_soname_path"
  "$libssh2_shared_versioned_path"
  "$deps_root/libssh2/install/include/libssh2.h"
  "$deps_root/libssh2/install/include/libssh2_publickey.h"
  "$deps_root/libssh2/install/include/libssh2_sftp.h"
  "$deps_root/zlib/install/lib/libz.a"
  "$zlib_shared_path"
  "$zlib_shared_soname_path"
  "$zlib_shared_versioned_path"
  "$deps_root/zlib/install/include/zlib.h"
  "$deps_root/zlib/install/include/zconf.h"
  "$deps_root/curl/install/lib/libcurl.a"
  "$curl_shared_path"
  "$deps_root/cpkt/install/lib/cmake/CURL/CURLConfig.cmake"
  "$deps_root/cpkt/install/lib/cmake/OpenSSL/OpenSSLConfig.cmake"
  "$deps_root/cpkt/install/lib/cmake/libssh2/libssh2-config.cmake"
  "$deps_root/cpkt/install/lib/cmake/nghttp2/nghttp2Config.cmake"
  "$deps_root/cpkt/install/lib/cmake/zlib/ZLIBConfig.cmake"
  "$deps_root/cpkt/install/lib/pkgconfig/libcurl.pc"
  "$deps_root/cpkt/install/lib/pkgconfig/openssl.pc"
  "$deps_root/cpkt/install/lib/pkgconfig/libssh2.pc"
  "$deps_root/cpkt/install/lib/pkgconfig/libnghttp2.pc"
  "$deps_root/cpkt/install/lib/pkgconfig/zlib.pc"
  "$deps_root/cpkt/install/share/cpkt/packages/core.json"
  "$deps_root/pslog/install/lib/libpslog.a"
  "$deps_root/pslog/install/include/pslog.h"
  "$pslog_shared_path"
  "$deps_root/lonejson/install/lib/liblonejson.a"
  "$deps_root/lonejson/install/include/lonejson.h"
  "$lonejson_shared_path"
  "$deps_root/lonejson/install/lib/pkgconfig/lonejson.pc"
  "$deps_root/lonejson/install/lib/cmake/lonejson/lonejsonConfig.cmake"
  "$deps_root/lonejson/install/lib/cmake/lonejson/lonejsonConfigVersion.cmake"
  "$deps_root/liblql/install/lib/liblql.a"
  "$deps_root/liblql/install/include/lql/lql.h"
  "$liblql_shared_path"
  "$deps_root/liblql/install/lib/pkgconfig/liblql.pc"
  "$deps_root/liblql/install/lib/cmake/liblql/liblqlConfig.cmake"
  "$deps_root/liblql/install/lib/cmake/liblql/liblqlConfigVersion.cmake"
)
if [ "$build_postgres" = ON ]; then
  required_paths+=(
    "$deps_root/cpktdb/install/share/cpkt/packages/db.json"
    "$deps_root/cpktdb/install/lib/cmake/CpktPostgres/CpktPostgresConfig.cmake"
    "$deps_root/cpktdb/install/include/cpkt/postgres.h"
    "$deps_root/cpktdb/install/lib/libcpkt_postgres.a"
  )
fi
if [ "$preset" != "deps-arm64-apple-darwin" ]; then
  required_paths+=("$deps_root/cpkt/install/lib/libcmocka.a")
fi

for path in "${required_paths[@]}"; do
  if [ ! -f "$path" ]; then
    deps_ready=0
    break
  fi
done

if [ "$deps_ready" -eq 1 ] && [ -f "$manifest_path" ]; then
  existing_manifest=$(cat "$manifest_path")
  if [ "$existing_manifest" = "$manifest" ]; then
    prune_dependency_install_trees
    assert_dependency_install_tree_privacy
    exit 0
  fi

  if ! grep -q '^openssl_version=' "$manifest_path"; then
    if [ "$(manifest_value compiler "$manifest_path")" = "$compiler" ] \
      && [ "$(manifest_value machine "$manifest_path")" = "$compiler_machine" ] \
      && [ "$(manifest_value version "$manifest_path")" = "$compiler_version" ] \
      && [ "$(manifest_value preset "$manifest_path")" = "$preset" ] \
      && [ "$(manifest_value zlib_version "$manifest_path")" = "$zlib_version" ]; then
      prune_dependency_install_trees
      printf '%s\n' "$manifest" > "$manifest_path"
      assert_dependency_install_tree_privacy
      exit 0
    fi
  fi
fi

reset_dependency_build_root
stage_cpkt_component_layout
if [ "$build_postgres" = ON ]; then download_cpktdb_bundle; fi
download_lonejson_bundle
download_liblql_bundle
download_pslog_bundle
cmake_extra_args+=("-DLOCKDC_PSLOG_ARCHIVE_PATH=$local_download_root/$pslog_asset_name")
cmake_extra_args+=("-DLOCKDC_ZLIB_VERSION=$zlib_version")
cmake_extra_args+=("-DLOCKDC_CPKT_VERSION=$cpkt_version")
if [ "$build_postgres" = ON ]; then
  cmake_extra_args+=("-DLOCKDC_CPKTDB_VERSION=$cpktdb_version")
fi
cmake --preset "$cmake_preset" --fresh "${cmake_extra_args[@]}"
cmake --build --preset "$cmake_preset" --target lc_deps
stage_dependency_license "pslog" "libpslog" "$deps_root/pslog/install/share/doc/libpslog/LICENSE"
stage_dependency_license "lonejson" "lonejson" "$deps_root/lonejson/install/share/doc/liblonejson/LICENSE"
stage_dependency_license "liblql" "liblql" "$deps_root/liblql/install/share/doc/liblql/LICENSE"
prune_dependency_install_trees
printf '%s\n' "$manifest" > "$manifest_path"
assert_dependency_install_tree_privacy
