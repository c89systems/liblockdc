#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
fixtures="$repo_root/build/e2e-fixtures"
source "$script_dir/assert_generated_path.sh"
lockdc_assert_generated_path "$repo_root" "$fixtures"

# Match the fixture releases in devenv.yaml.in. MinIO ships native release
# binaries, and the lockd CLI can be built without its container entry point.
lockd_ref=b871aa324cbca4815c207b651a0de0d876910f7d # v0.8.1
minio_release=RELEASE.2025-09-07T16-13-09Z
mc_release=RELEASE.2025-08-13T08-35-41Z
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    platform=linux-amd64
    minio_sha=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
    mc_sha=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891
    ;;
  Linux:aarch64)
    platform=linux-arm64
    minio_sha=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d
    mc_sha=14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c
    ;;
  Darwin:arm64)
    platform=darwin-arm64
    minio_sha=7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4
    mc_sha=a877fd0c183409da9f20f9d6e1811987298bbbca1aa03428eebdffba79fb9445
    ;;
  *)
    echo 'Native E2E fixtures require Linux x86_64, Linux aarch64, or macOS arm64' >&2
    exit 2
    ;;
esac

mkdir -p "$fixtures"
scratch=$(mktemp -d "$fixtures/.build.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
sha256=(sha256sum)
if ! command -v sha256sum >/dev/null; then sha256=(shasum -a 256); fi

download_fixture() {
  local name=$1 release=$2 hash=$3 url
  if [ -f "$fixtures/$name" ] &&
     printf '%s  %s\n' "$hash" "$fixtures/$name" | "${sha256[@]}" -c - >/dev/null; then
    return
  fi
  url="https://github.com/minio/$name/releases/download/$release/$name.$platform.$release"
  curl -fLsS --retry 3 --connect-timeout 20 --max-time 180 "$url" -o "$scratch/$name"
  printf '%s  %s\n' "$hash" "$scratch/$name" | "${sha256[@]}" -c -
  chmod 755 "$scratch/$name"
  mv -f -- "$scratch/$name" "$fixtures/$name"
}

download_fixture minio "$minio_release" "$minio_sha"
download_fixture mc "$mc_release" "$mc_sha"
git init -q "$scratch/lockd-src"
git -C "$scratch/lockd-src" fetch --quiet --depth=1 https://github.com/sa6mwa/lockd.git "$lockd_ref"
git -C "$scratch/lockd-src" checkout --quiet --detach "$lockd_ref"
(
  cd "$scratch/lockd-src"
  export CGO_ENABLED=0 GOTOOLCHAIN=local
  go build -mod=readonly -tags no_psi -o "$scratch/lockd" ./cmd/lockd
)
mv -f -- "$scratch/lockd" "$fixtures/lockd"
"$fixtures/lockd" version
"$fixtures/minio" --version
"$fixtures/mc" --version
