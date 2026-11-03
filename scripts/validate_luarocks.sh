#!/usr/bin/env bash

set -euo pipefail

mode=all
case "${1:-}" in
  --prepare|--run) mode=${1#--}; shift ;;
esac
if [ "$#" -ne 4 ]; then
  printf 'usage: %s [--prepare|--run] TREE_DIR SDK_PREFIX LUA_PACKAGE_PATH LUA_SCRIPT\n' "$0" >&2
  exit 1
fi

tree_dir="$1"
sdk_prefix="$2"
lua_package_path="$3"
lua_script="$4"

lua_bin="${LOCKDC_LUA_BIN:?LOCKDC_LUA_BIN must name the project-built Bootlin Lua runner}"
luarocks_bin="${LOCKDC_LUAROCKS_BIN:-luarocks}"
lua_version="${LOCKDC_LUA_VERSION:-5.5}"
lonejson_src_rock="${LOCKDC_LONEJSON_SRC_ROCK:-https://github.com/c89systems/lonejson/releases/download/v0.45.0/lonejson-0.45.0-1.src.rock}"
luarocks_build_root="${LOCKDC_LUAROCKS_BUILD_ROOT:-${tree_dir}/.luarocks-build}"
luarocks_workdir="${LOCKDC_LUAROCKS_WORKDIR:-$PWD}"
run_lua_smoke="${LOCKDC_RUN_LUA_SMOKE:-1}"

# shellcheck source=assert_generated_path.sh
source "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/assert_generated_path.sh"

require_path() {
  if [ ! -e "$1" ]; then
    printf 'missing required path: %s\n' "$1" >&2
    exit 1
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

require_command "$lua_bin"
require_command "$luarocks_bin"
require_path "$sdk_prefix"
require_path "$lua_script"
require_path "$luarocks_workdir"
lockdc_assert_generated_path "$luarocks_workdir" "$tree_dir"
lockdc_assert_generated_path "$luarocks_workdir" "$luarocks_build_root"
if [ "$mode" != run ]; then
  luarocks_cc="${LOCKDC_LUAROCKS_CC:?LOCKDC_LUAROCKS_CC must name the selected Bootlin C compiler}"
  luarocks_cflags="${LOCKDC_LUAROCKS_CFLAGS:?LOCKDC_LUAROCKS_CFLAGS must contain the selected Bootlin C flags}"
  luarocks_lua_incdir="${LOCKDC_LUAROCKS_LUA_INCDIR:?LOCKDC_LUAROCKS_LUA_INCDIR must name the cpkt Lua headers}"
  require_path "$lua_package_path"
  require_path "$luarocks_cc"
  require_path "$luarocks_lua_incdir/lua.h"
else
  require_path "$tree_dir/lib/lua/$lua_version/lockdc/core.so"
  require_path "$tree_dir/share/lua/$lua_version/lockdc/init.lua"
fi

lua_runtime_version="$($lua_bin -e 'io.write(_VERSION)' 2>/dev/null || true)"
if [ "$lua_runtime_version" != "Lua 5.5" ]; then
  printf 'Lua 5.5 is required, got %s from %s\n' "${lua_runtime_version:-unknown}" "$lua_bin" >&2
  exit 1
fi

install_lonejson_dependency() (
  # LoneJSON 0.44 selects its native Bootlin toolchain through CMake.  Do not
  # preload liblockdc's vendored libraries into that host-tool invocation:
  # loader diagnostics on stderr are part of the target-discovery output.
  unset LD_LIBRARY_PATH
  unset LD_PRELOAD

  case "$lonejson_src_rock" in
    *.src.rock)
      if [ -f "$lonejson_src_rock" ]; then
        require_command unzip
        require_command tar

        lonejson_unpack_dir="${tree_dir}-lonejson-src"
        lockdc_assert_generated_path "$luarocks_workdir" "$lonejson_unpack_dir"
        rm -rf "$lonejson_unpack_dir"
        mkdir -p "$lonejson_unpack_dir"
        (
          cd "$lonejson_unpack_dir"
          unzip -q "$lonejson_src_rock"
          lonejson_archive="$(find . -maxdepth 1 -type f -name '*.tar.gz' | sed 's#^\./##' | head -n 1)"
          lonejson_rockspec="$(find . -maxdepth 1 -type f -name '*.rockspec' | sed 's#^\./##' | head -n 1)"
          if [ -z "$lonejson_archive" ] || [ -z "$lonejson_rockspec" ]; then
            printf 'invalid lonejson source rock layout: %s\n' "$lonejson_src_rock" >&2
            exit 1
          fi
          tar -xzf "$lonejson_archive"
          lonejson_source_dir="$(find . -mindepth 1 -maxdepth 1 -type d | sed 's#^\./##' | head -n 1)"
          if [ -z "$lonejson_source_dir" ]; then
            printf 'lonejson source rock has no extracted source directory: %s\n' "$lonejson_src_rock" >&2
            exit 1
          fi
          cp "$lonejson_rockspec" "$lonejson_source_dir/"
          cd "$lonejson_source_dir"
          "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" make "$lonejson_rockspec"
        )
        return 0
      fi
      ;;
  esac

  "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" install "$lonejson_src_rock"
)

unset LD_LIBRARY_PATH
unset LD_PRELOAD
export LOCKDC_PREFIX="$sdk_prefix"
: "${LONEJSON_LIBDIR:=$sdk_prefix/lib}"
export LONEJSON_LIBDIR

if [ "$mode" != run ]; then
  rm -rf "$tree_dir" "$luarocks_build_root"
  mkdir -p "$tree_dir"
  cd "$luarocks_workdir"

  install_lonejson_dependency
  luarocks_target_variables=(
    "CC=${luarocks_cc}"
    "CFLAGS=${luarocks_cflags}"
    "LUA_INCDIR=${luarocks_lua_incdir}"
  )
  case "$lua_package_path" in
    *.rockspec)
      LOCKDC_LUAROCKS_BUILD_ROOT="$luarocks_build_root" \
        "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" make "$lua_package_path" "${luarocks_target_variables[@]}"
      ;;
    *.src.rock|*.rock)
      "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" install "$lua_package_path" "${luarocks_target_variables[@]}"
      ;;
    *)
      printf 'unsupported Lua package path: %s\n' "$lua_package_path" >&2
      exit 1
      ;;
  esac

  if [ -n "${LOCKDC_POSTGRES_ROCK:-}" ]; then
    require_path "$LOCKDC_POSTGRES_ROCK"
    "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" \
      install "$LOCKDC_POSTGRES_ROCK" "${luarocks_target_variables[@]}"
  fi
fi

if [ "$mode" = prepare ]; then
  printf 'Prepared LuaRocks fixture: %s\n' "$tree_dir"
  exit 0
fi

lua_paths=$("$luarocks_bin" --tree "$tree_dir" path --lua-version "$lua_version")
eval "$lua_paths"

if [ "$run_lua_smoke" = "0" ] || [ "$run_lua_smoke" = "OFF" ] || [ "$run_lua_smoke" = "FALSE" ]; then
  printf '%s\n' "Skipping Lua runtime smoke: target module is not runnable on this host"
  exit 0
fi

if [ "${LOCKDC_TEST_ALLOCATOR_TRACKING:-0}" = 1 ]; then
  LOCKDC_TRACKED_LUA_SCRIPT="$lua_script" "$lua_bin" -e \
    'assert(require("lockdc").allocator_tracking_configure(true)); dofile(os.getenv("LOCKDC_TRACKED_LUA_SCRIPT")); collectgarbage("collect")'
else
  "$lua_bin" "$lua_script"
fi
