#!/usr/bin/env bash
set -euo pipefail

# Implement GNU ld --wrap for Mach-O test executables. Rewrite undefined
# object symbols only, including references inside static archives. The
# original libraries and production link commands never pass through here.
for tool in llvm-nm llvm-objcopy llvm-ar; do
  command -v "$tool" >/dev/null || { echo "$tool is required for Darwin unit tests" >&2; exit 1; }
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/lockdc-test-link.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
mapping="$scratch/symbols.map"
touch "$mapping"
command=()
for argument in "$@"; do
  case "$argument" in
    -Wl,--wrap=*)
      symbol=${argument#-Wl,--wrap=}
      printf '_%s ___wrap_%s\n___real_%s _%s\n' "$symbol" "$symbol" "$symbol" "$symbol" >>"$mapping"
      ;;
    *) command+=("$argument") ;;
  esac
done

map_undefined_symbols() {
  llvm-nm --undefined-only --just-symbol-name "$1" |
    awk 'NR==FNR { names[$1]=$2; next } $1 in names { print $1, names[$1] }' "$mapping" -
}

for index in "${!command[@]}"; do
  input=${command[index]}
  case "$input" in
    *.a|*.o) [ -f "$input" ] || continue ;;
    *) continue ;;
  esac
  rewritten="$scratch/$index"
  mkdir "$rewritten"
  map_undefined_symbols "$input" >"$rewritten/symbols.map"
  if [ ! -s "$rewritten/symbols.map" ]; then continue; fi
  case "$input" in
    *.o)
      output="$rewritten/${input##*/}"
      llvm-objcopy --redefine-syms="$rewritten/symbols.map" "$input" "$output"
      ;;
    *.a)
      archive=$(CDPATH= cd -- "$(dirname -- "$input")" && pwd -P)/${input##*/}
      output="$rewritten/rewritten.a"
      cp "$archive" "$output"
      (
        cd "$rewritten"
        llvm-ar t "$archive" >members
        if [ -n "$(sort members | uniq -d)" ]; then
          echo "Cannot wrap an archive with duplicate member names: $archive" >&2
          exit 1
        fi
        mkdir objects
        cd objects
        llvm-ar x "$archive"
        changed=()
        while IFS= read -r member; do
          map_undefined_symbols "$member" >../symbols.map
          if [ -s ../symbols.map ]; then
            llvm-objcopy --redefine-syms=../symbols.map "$member"
            changed+=("$member")
          fi
        done <../members
        llvm-ar r "$output" "${changed[@]}"
      )
      ;;
  esac
  command[index]=$output
done
"${command[@]}"
