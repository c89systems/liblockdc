# Development and verification

Run these commands from the repository root. `make help` is the authoritative
command index. For release preparation and signing, see
[the release guide](releases.md).

## Build system

The repository (<https://github.com/c89systems/liblockdc>) uses a Makefile-first
lifecycle with CMake as the build backend:

- `Makefile`
  - primary developer entry point
  - build, test, package, clean, and release orchestration
- `CMake`
  - target graph
  - install graph
  - exported package metadata
  - test registration

Normal host and release Make targets provision the required dependency trees
automatically. The low-level `scripts/build.sh` helper assumes that the matching
dependency root already exists.

PostgreSQL's client/facade SDK is optional. Core builds default to
`-DLOCKDC_BUILD_POSTGRES=OFF` and require no cpktdb installation. To prepare the
optional provider SDK, native participant and its validation targets, use
`LOCKDC_BUILD_POSTGRES=ON make deps` or
`scripts/deps.sh deps-x86_64-linux-gnu --with-postgres`, then configure with
`-DLOCKDC_BUILD_POSTGRES=ON`. The [C participant guide](postgres.md) describes
its explicit static/shared linkage, registration, borrows and recovery. The core
library binaries are identical with the optional provider present or absent.
PostgreSQL 14 through 18 are covered by mixed-transaction integration tests
using two PostgreSQL servers and one Pouch store. For a clean release that
includes provider SDKs, set `LOCKDC_BUILD_POSTGRES=ON`; fresh library
configurations use the same selection as dependency provisioning. An existing
CMake cache or explicit `-DLOCKDC_BUILD_POSTGRES=OFF` takes precedence over the
environment default.

## Build prerequisites

Normal development expects:

- CMake 3.24 or later
- Ninja
- GNU Make
- host `clang-format` for `make format`
- host `clangd` for editor diagnostics of the public C API
- host Valgrind for the native Memcheck gate
- `qemu-aarch64` and `qemu-arm` for the non-host release test matrix
- rootless Podman and Python 3.9 or newer for the local Podman Kube development
  environment

Every Linux build uses its matching pinned Bootlin GCC collection, including the
compiler, linker, binutils, sysroot, headers, and runtime. The Make and CMake
configurations provision those collections automatically; do not substitute host
or distro cross compilers. Toolchains are shared under
`${CPKT_TOOLCHAIN_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cpkt/toolchains}` and
verified dependency archives under
`${CPKT_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cpkt/deps}`.
Repository-local `.cache/` directories are disposable build and staging state.

Darwin cross builds use a developer-supplied osxcross SDK under `OSXCROSS_ROOT`.
The lifecycle selects the newest complete `arm64-apple-darwin25.x` collection;
set `CPKT_OSXCROSS_HOST` to pin an installed prefix. Prepare it with
`scripts/cpkt-toolchains.sh ensure arm64-apple-darwin` before configuring. That
command provisions the pinned Linux host MIG helper in the shared toolchain
cache, without obtaining Apple content. Darwin configure requires both tools;
MIG is build tooling and is excluded from SDK packages.

The pinned `cpkt` 0.7.0 GNU SDKs require glibc 2.43 or newer at deployment. This
does not change the `liblockdc` public ABI, but deployments using the shared GNU
SDK must upgrade their runtime accordingly. Development executables, tests,
examples, Lua module checks, and SDK consumer probes run directly with their
selected Bootlin ELF interpreter and private runtime lookup paths; they do not
use `LD_LIBRARY_PATH` or a host Lua interpreter.

## Dependencies

| Dependency | Version | Use |
| --- | --- | --- |
| [cpkt](https://github.com/c89systems/cpkt) | 0.7.0 | Core SDK dependencies and Lua runtime |
| [cpktdb](https://github.com/c89systems/cpktdb) | 0.4.0 | Optional PostgreSQL C89 facade and database SDK |
| lonejson | 0.50.0 (ABI 29) | JSON parsing, mapping, and streaming |
| libpslog | 0.13.0 | SDK diagnostics |
| liblql | 0.4.1 | Pouch query support |
| cmocka | 2.0.2 | Unit tests only; target-specific static library from cpkt |

Lonejson 0.50.0 uses ABI 29, introduced in 0.49.0 with the push-fed value visitor
on the runtime receiver. Consumers upgrading from ABI 28 must rebuild against
the matching SDK, including Lua native modules. Check the status of mapped-record
init/reset/cleanup calls. See the [upgrade notes](../CHANGELOG.md#upgrading-from-030x).

OpenSSL, curl, zlib, nghttp2, libssh2, and Lua come from cpkt. cmocka comes from
cpkt's target-specific SDK and remains a test-only dependency.

Core and database SDKs remain in separate `cpkt/install` and `cpktdb/install`
prefixes under each target's dependency root. cpktdb records its exact core
build pin as provenance and requests a compatible minimum core through CMake.
Core's declared compatibility floor governs independent upgrades; matching
major/minor versions alone do not establish compatibility. cpkt 0.7.0 accepts
baselines from 0.2.0 through 0.7.0; cpktdb 0.4.0 requires at least 0.7.0. Target
and payload integrity checks remain enforced. `scripts/deps.sh` pins each
provider release and checksum independently; cpktdb is acquired/discovered only
when PostgreSQL is enabled. Consumers use the official imported targets.
pkg-config consumers must also run the provider's CMake compatibility preflight.
cpktmisc is not required by liblockdc. Only cpktdb's PostgreSQL facade/client
is selected; its SQLite and iODBC facades are not used. PostgreSQL applications
use the checked result retrieval API to distinguish drainage from local
retrieval failures.

## Common commands

Build the normal host development preset:

```bash
make build
```

`make build` and `make test-debug` use the sanitizer-instrumented debug preset.
Third-party dependencies are still reused from the release-built host dependency
cache.

This native build also prepares clangd: `.clangd` discovers
`build/debug/compile_commands.json`, and standalone C89 header checks supply
exact core/provider commands when opening public headers directly. Run
`make build` after `make clean` to restore the dependency headers, generated
headers, and compilation database. For the optional PostgreSQL header, prepare
its SDK and enable `LOCKDC_BUILD_POSTGRES=ON` as described above; keep that
selection when running `make build`. No separate editor SDK is required.

Run the host release suites for the shipped x86_64 GNU and musl builds:

```bash
make test-host
```

`make test` is the short alias for the sanitizer-instrumented debug suite.

Run the non-host cross release suites:

```bash
make test-cross
```

Run the complete local confidence path:

```bash
make test-all
```

`make test-all` runs the sanitizer-instrumented debug suite and deterministic
local e2e. It is the complete fast functional edit-loop gate, not an alias for
the release matrix. Its CTest suites use a bounded four-job default
(`LOCKDC_CTEST_PARALLEL_LEVEL` overrides it), while tests marked serial remain
serial. Cross-target testing, Valgrind, and fuzzing are release confidence work:
`make prerelease` runs the native debug/e2e suite, Valgrind, and full fuzzing,
while `make release-matrix` builds, tests, packages, and verifies the shipped
Bootlin GNU/musl and QEMU cross matrix. The AFL++ compiler bootstrap is
hardening work, not an everyday functional invariant. Performance workloads
belong to the explicit `make bench-gate` command. The signed, tagged release
gate is `make release`; see [release preparation](releases.md#release-process)
for the clean exercise before squashing.

`make prerelease-hardening` runs ordinary prerelease, including full fuzzing,
then adds native benchmarks, the Pouch-vs-lockd performance thresholds, the
finite multi-mode Pouch core churn soak, reclaim proof, and outbox
reconciliation hardening. Run `make prerelease-hardening release-matrix` when
the same exercise also needs the shipped target/package matrix. The
Pouch-vs-lockd performance comparison measures every core storage operation
separately: acquire, update and stale-precondition rejection, release, public
and leased reads, attachment write/read, queue delivery, all index-publication
phases, indexed/scan/full-text queries, and restart recovery.

Run focused verification layers:

```bash
make test-e2e
make test-debug
make coverage
make fuzz
make pouch-integration-fuzz
make benchmarks
```

`make build-fuzz` and the fuzz runner prepare the pinned AFL++ toolchain before
CMake configures instrumentation. Resolver discovery remains read-only and
reports a preparation command when the collection is missing.

`make pouch-integration-fuzz` runs the production-code Pouch LQL and lifecycle
mutation runners without AFL++ instrumentation. Each corpus seed and mutation
executes in a fresh process; failures retain the reproducing input artifact.

`make benchmarks` uses tuned per-case defaults; set `BENCH_ITERS=<n>` to force
the same iteration count across all benchmark cases.

`make bench-gate` runs the complete native benchmark matrix and both production
parity gates: default durability and strict durability. Each gate uses five
same-run samples and requires a median Pouch speedup of at least 1.25x on every
comparable core operation against the matching Go disk configuration. The
comparisons use a bounded 12-record, two-update, 128 KiB-payload profile with 16
KiB segments to exercise rollover and recovery. Each comparison has a 90-second
measurement limit; preparation is separate. Large production profiles remain
available through `make benchmark-pouch-go-production`. The broader Go suite
also includes compression, compaction, concurrency, and query scale benchmarks;
see [the benchmark guide](../benchmark/README.md) for its profiles and command
options.

All significant Make targets print total elapsed time on completion.

`make format` runs `clang-format` over the C source/header tree and is also part
of the clean-slate release pipeline.

### Worker stack budget

Design library code for musl's nominal 128 KiB default worker stack. Keep
input-dependent traversal state on the project allocator, with allocation
errors and cleanup covered by tests. Keep large buffers and aggregates off the
native stack; owned C library and test targets reject VLAs and `alloca()` with
compiler errors.

Linux glibc Debug CTest runs set default pthread stacks to 128 KiB, including
nested workers and SDK-linked child applications. CTest disables glibc's thread
stack cache at process startup with
`GLIBC_TUNABLES=glibc.pthread.stack_cache_size=0`, so a default worker cannot reuse
a previously larger stack. Main-thread limits, explicit pthread stack requests,
and build-tool defaults remain unchanged. The initializer is confined to Debug
test builds and requires CTest's `LOCKDC_TEST_STACK_BYTES=131072` environment;
Release SDKs contain no initializer.

The `stack`-labelled regressions check actual worker sizes, guard faults, child
inheritance, deep traversal, malformed input, and allocation/read failures.
Run `ctest --test-dir build/debug -L stack --output-on-failure` after building.
The traversal regression also runs on the Linux cross-runtime matrix; native
musl runs verify the actual default worker stack without overriding its size.

## Local development environment

The rootless Podman Kube environment contains a shared-disk lockd pair, an
S3-backed lockd with MinIO, and a `mem://` server on a Unix socket.

```bash
make dev-up
source build/devenv/env.sh
make dev-down
make dev-reset
```

`make test-e2e` and `make example-smoke-local` start or reuse the stack, probe
readiness, and stop and verify removal of the owned pod and containers on
completion, failure or interruption. Overlapping runs in one checkout are
rejected. Set `LOCKDC_E2E_KEEP_DEVSERVICES=1` explicitly to retain services for
debugging. Direct lockd CTest or binary invocation requires manual service
management; PostgreSQL XA CTest checks manage their separate two-server stack.

Generated state and credentials live under `build/devenv/`. Reset stops the pod
before removing that directory. The [environment guide](../devenv/README.md)
documents images, ports, endpoint exports, overrides and cleanup behavior.

## Repository layout

| Path | Purpose |
| --- | --- |
| `include/lc/` | Public C API and version headers |
| `src/` | Library implementation and native Lua binding |
| `lua/` | Public Lua facade |
| `examples/` | Runnable C and Lua examples |
| `tests/unit/` | Unit and transport checks |
| `tests/e2e/` | Service-backed integration tests |
| `tests/fuzz/` | AFL++ and isolated-process Pouch mutation harnesses |
| `scripts/` | Build, test, package and environment commands |
| `devenv/` | Podman Kube service configuration and guidance |
| `docs/` | API, storage and lifecycle guides |

## Direct CMake use

`make help` is the authoritative command index. For direct control over a
configured CMake preset, use CMake after the normal lifecycle entrypoint has
provisioned its inputs:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

The CMake presets resolve the matching pinned Bootlin collection and dependency
root. Use the Make targets for normal builds, package production, verification,
and release orchestration.
