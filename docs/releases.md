# Releases and SDK archives

For prerequisites, dependency selection and everyday verification, see
[development and verification](development.md).

## Supported targets

Release archives are produced for:

- `x86_64-linux-gnu`
- `x86_64-linux-musl`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `armhf-linux-gnu`
- `armhf-linux-musl`
- `arm64-apple-darwin` (built locally with osxcross)

The library itself is delivered as:

- `liblockdc.so` on Linux or `liblockdc.dylib` on Darwin
- `liblockdc.a`

## Release process

The project ships a core SDK archive for each supported target. Selecting
PostgreSQL adds a separate provider SDK archive for each target.

Complete implementation, verification and review on one development branch.
Before squashing, exercise the entire release build and test graph from clean
generated state with the candidate version and intended optional capabilities:

```bash
: "${LOCKDC_VERSION_OVERRIDE:?Set this to the intended candidate version}"
make clean
LOCKDC_VERSION_OVERRIDE="$LOCKDC_VERSION_OVERRIDE" LOCKDC_BUILD_POSTGRES=ON \
  make prerelease release-matrix
```

Use the intended candidate version; select PostgreSQL only when shipping its
provider SDKs. Fresh CMake configurations take the candidate version from the
environment; an explicit CMake selection or existing cache takes precedence.
Require the whole exercise to pass before squashing. These candidate artifacts
are preparation evidence. The signed, tagged commit still requires its final
clean release gate.

Then squash the verified tree onto local `trunk`. The engineer amends/signs that
default-branch commit. Verify its signature and unchanged tree, then create the
matching local lightweight version tag. Signing and tagging use `trunk`.

Create the complete release set from that signed, tagged commit:

```bash
make release
```

Keep remote release refs unchanged until the clean local release gate passes.

`make release` is the final clean-slate release pipeline. It verifies release
tag semantics, removes generated state, then runs the same proof graph as
`make prerelease`: formatting, debug sanitizer tests including Lua coverage,
Valgrind, full deterministic fuzzing, lockd e2e, and the release matrix. The
longer performance, churn, and compaction campaigns remain the explicit
`make prerelease-hardening` layer. Use `make release-matrix` when you explicitly
want to reuse existing build and dependency caches for a faster release
matrix/package rerun.

## Verification ownership

During a full release, Debug owns the `lifecycle-host` source and host-fixture
checks and complete behavioral tables. The three large Pouch, outbox and
collection suites use explicit portability cases in optimized GNU/musl and QEMU
lanes; their lists live in `tests/unit/portability.cmake`. These retain
storage-format and integer-width boundaries, crash/restart recovery, compaction,
durable synchronization, shared writers, retention, collection selection and
ownership regressions. Smaller unit suites run in full on every Linux target,
including mixed state/collection churn, allocator probes and the small-stack
compaction regression. Every target retains its SDK and ABI checks. Executing a
storage test binary without case arguments still runs its full table, including
on a release target.

Every configuration partitions collection checks into `lc_unit_collections`,
`lc_unit_collections_append` and `lc_unit_collections_recovery`, sharing one
executable. Debug runs the complete table; optimized GNU/musl and QEMU lanes
partition their declared portability cases. A configuration-specific selection
contract runs in each runtime lane and checks that every case runs exactly once.
Select all three with `ctest --test-dir build/debug -R '^lc_unit_collections'`.

Collection and staged-writer groups reserve two CTest slots each, limiting
concurrent fsync-heavy groups within the default four-job lane. Their 60-second
timeouts and complete case selections remain unchanged.

Debug runs the complete PostgreSQL 14–18 matrix, with two servers, Pouch and the
C/Lua driver variants. Optimized native GNU repeats PostgreSQL 14; the
additional server versions carry the `native-behavior` label and run once. Lua
E2E prepares one fresh installed rock through a CTest fixture per invocation;
each scenario still runs in its own Lua process and retains allocator failure
checks. Selecting an individual scenario automatically includes its setup.
Standalone and final-artifact Lua package tests retain their own installations.

Prerelease compiles its AFL configuration before long service/memory checks; the
later full fuzz campaign reuses those compiled outputs. Lease-expiry semantics
use controlled clocks where possible; real worker timing remains tested. This
changes coverage ownership, not clean-release or signing rules.

The cpkt compatibility fixture consumes the same GNU dependency SDK in Debug and
Release, so Debug owns it; the distinct musl SDK is checked in its Release lane.
The `release-artifact` checks have standalone CTest entrypoints; the release
pipeline verifies those contracts against the final archives and checksum
inventory during packaging. Native archive consumers build and run in one pass
per target, while cross consumers retain target-specific link checks. Valgrind
builds only its selected test targets and their prerequisites. It runs the full
Pouch suite and five focused outbox shutdown/allocator cases; the complete
outbox inventory and timing assertions run under native sanitizers.
`CMOCKA_TEST_FILTER` can select another case for Valgrind diagnosis.
`make test-all` reuses the Debug sanitizer build for E2E; standalone
`make test-e2e` retains its own preset. Static and shared core libraries share
compiled implementation objects. SDK configuration permutations run once per
distinct extracted package configuration within the packaging invocation; every
target still builds its own consumers against its final archives.

## SDK archives

Create only the `x86_64-linux-gnu` package:

```bash
make package
```

Package archive names follow this pattern:

- release archive:
  - `liblockdc-<version>-<target>.tar.gz`
- optional PostgreSQL provider SDK:
  - `liblockdc-postgres-<version>-<target>.tar.gz`
- checksum manifest:
  - `liblockdc-<version>-CHECKSUMS`
- C source package:
  - `liblockdc-<version>.tar.gz`
- standalone Lua source package:
  - `liblockdc-lua-<version>.tar.gz`
- rendered Lua release artifacts:
  - `lockdc-<version>-1.rockspec`
  - `lockdc-<version>-1.src.rock`
- optional PostgreSQL Lua registration artifacts (when PostgreSQL is selected):
  - `lockdc-postgres-<version>-1.rockspec`
  - `lockdc-postgres-<version>-1.src.rock`

The Lua source archive supplies the complete public XA facade for both rocks.
The optional `lockdc.postgres` module registers the linked driver; static
embeddings preload the same entry point. See the
[Lua XA guide](lua.md#optional-postgresql-participation).

### Release archive contents

The release archive contains:

- `liblockdc` public headers
- bundled dependency headers for `libpslog`, `curl`, `OpenSSL`, `nghttp2`, and
  `lonejson`
- `liblockdc.so*`
- `liblockdc.a`
- the static archives for the bundled third-party dependencies
- bundled shared runtime dependencies
- `pkg-config` metadata
- CMake package metadata
- project documentation and license files
- bundled third-party license files

Archive layout and contents are regression-tested. The release verification
suite asserts the complete shipped matrix, expected archive names, checksum
manifest, and package contents.
