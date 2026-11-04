# liblockdc

`liblockdc` is a C89/C90 client library for `lockd`, providing persistent state,
leases, queues, attachments, queries, and built-in implicit XA transactions
with coordinated commit, rollback, and recovery. It also includes Pouch, an
embedded storage engine that makes these capabilities available directly
inside your application without a separate `lockd` server.

Its installed header is also usable from C++98-or-later applications through C
linkage. It provides a receiver-function public API for client, lease, queue
delivery, attachment, management, and consumer-service handles, plus stream-based
JSON and payload I/O. The project ships both static and shared libraries, a
local development environment, a cross-architecture release pipeline, and
dependency-backed unit, e2e, sanitizer, coverage, fuzz, and benchmark targets.

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

## Capabilities

- built-in implicit XA transactions for state and attachment changes, with
  coordinated commit, rollback, and recovery
- mTLS client authentication through a combined `client.pem` bundle
- HTTP/1.1 and HTTP/2 transport through bundled `libcurl` and `nghttp2`
- Unix domain socket transport support for local `mem://` deployments
- stream-oriented state and payload upload/download with `lc_source` and `lc_sink`
- receiver-function `lc_client`, `lc_lease`, `lc_message`, and `lc_consumer_service` APIs
- lease, queue, attachment, namespace/index, transaction-coordinator, and resource-manager management APIs
- mapped JSON state load/save through `lonejson`
- streamed query-key callbacks and streaming queue subscribe/watch flows
- managed consumer support with blocking and explicit start/stop/wait service modes
- durable command-receipt, inbox, outbox, dispatcher, retry, reconciliation,
  and dead-letter outbox receivers
- integrated SDK logging through `libpslog`
- optional PostgreSQL participation in the existing XA coordinator, with native
  two-phase completion and recovery

The durable transactional messaging model and endpoint constraints are
specified in [the outbox design](docs/inbox-outbox.md). The threadless
producer, explicit dispatcher, and Vectis integration contract are specified
in [the outbox dispatch architecture](docs/outbox-dispatch-architecture.md).
The Lua outbox facade is documented in [the Lua SDK guide](docs/lua.md).
The optional PostgreSQL participant provider joins ordinary Pouch state and
attachment transactions, with optional inbox/outbox composition. See the
[XA contract](docs/xa.md), [C participant guide](docs/postgres.md), and
[Lua transaction facade](docs/lua.md#optional-postgresql-participation). The coordinator
uses a local Pouch journal; remote lockd participation is unsupported.
See [release notes](CHANGELOG.md) for changes and ABI requirements.
[Ordered collections](docs/collections.md) provide Pouch journals with immutable
JSON bodies, permanent positions, LQL filtering, raw ordered streaming, and
optional collection/entry age retention and per-collection live-entry limits
through the C and Lua APIs. Count limits lazily retain the newest N entries
without trimming on append. [Push-fed append writers](docs/collection-writer.md)
accept bounded raw JSON fragments; all append forms can renew parent retention
atomically with a new entry. Explicit staged pull feeds a source through the
same writer for slow or concurrent transfers. [Atomic collection edits](docs/collection-edits.md)
remove position ranges with an optional streamed replacement and recovery marker,
using head preconditions and durable retry receipts.
Optional [collection leases](docs/collection-leases.md)
grant exclusive ownership across many committed appends, with keepalive,
conditional takeover and optional private reads. Unleased collections retain
multiwriter behavior. The same contract defines a future lockd server interface;
remote collections are currently unsupported. Pouch automatically upgrades
released collection heads on root open without rewriting entry bodies. First
upgrade requires all other
clients to be closed; later opens skip migration. See
[storage upgrade](docs/collection-leases.md#storage-upgrade).
The executable direct deployment pair is
[`examples/outbox_producer.c`](examples/outbox_producer.c) and
[`examples/lua/outbox_dispatcher.lua`](examples/lua/outbox_dispatcher.lua):
the producer commits and exits, while the dedicated Lua process performs the
foreign-effect dispatch.

## Pouch storage

Pouch is liblockdc's C-native local storage engine. It follows Go lockd disk
storage semantics while intentionally using its own binary records, manifests,
`uint64_t` payload/file-size, generation/index, and coordinator-term
accounting, and `int64_t` Unix timestamps. The current semantic alignment scope
and every remaining format, operational, API, and portability divergence are
recorded in [the Pouch storage specification](docs/pouch-storage.md).

Select Pouch through a single absolute `pouch://` endpoint, for example
`pouch:///var/lib/my-service/lockd-root`. The default is exclusive
single-writer mode: one live process owns the root append path and normal
acquire, update, release, queue, attachment, and query operations use the
resident logstore fast path. Opening a second default writer for the same root
fails instead of silently downgrading. Explicit shared-root writing remains
available for callers that need multiple active local writers by adding
`?single_writer=false`; that mode preserves
correctness and process fencing but is not the primary performance target.

New C and Lua applications should configure Pouch root policy with the typed
`lc_pouch_settings` / `lockdc.open({ pouch = ... })` API. The endpoint options
below remain fully supported for deployment compatibility and are overridden
per field by typed settings. See [typed Pouch open settings](docs/pouch-open-settings-api.md).
Common legacy options are:

- `compression=zlib` for streaming at-rest zlib compression. Omission inherits
  an existing root; explicitly changing mode converts it before open returns
  when no other writers are active. Conversion resumes after interruption and
  replaces one segment or snapshot at a time without a second root. See
  [compression changes](docs/pouch-compression.md).
- `crypto_key_file=/path/to/pouch.key` with
  `crypto_generate_key_file=true` for encrypted local roots
- `durable_sync=true` and `fsync_batch_max_ops=<u64>` for root-scoped durable
  group commit
- `segment_target_bytes=<u64>` for rolling segment sizing
- `indexer_flush_docs=<u64>` and `indexer_flush_interval_seconds=<u64>` for
  bounded query-index publication: exclusive roots publish at the document
  threshold after all pending leases or transactions have completed, while
  shared roots publish asynchronously
- `background_compaction=false` to disable the default cadence-based
  compaction worker, and `disable_compaction_throttling=true` to remove its
  default throughput bound
- `terminal_reclaim_min_bytes=<u64>` to set the minimum active-segment size
  before bounded terminal-history compaction seals it (default 1 MiB)
- `queue_watch=true` to request filesystem-assisted queue wake-up where the
  local filesystem supports it, with polling fallback otherwise
- `query_engine=index|scan` and `query_fallback_engine=index|scan` for the
  namespace query preference used at open
- `query_indexing=false` for roots that never use indexed queries. This
  disables local index maintenance and makes implicit queries use scans;
  explicit indexed queries and `flush_index` are unavailable. Pouch outbox
  recovery and dead-letter management continue through bounded scan queries.

State writes and metadata updates accept optional per-object retention through
the same C and Lua APIs for Pouch and remote lockd. Omission preserves the
policy, explicit zero clears it, and positive seconds start at commit. Expired
Pouch objects remain ordinary state until the existing maintenance worker
deletes them. The default maintenance cadence is 30 minutes; disabling background
compaction also disables automatic expiry cleanup. Remote results advertise
support when available; a successful write without retention metadata does not
confirm that an older server applied the option. Older servers remain usable
without retention. See [state retention](docs/state-retention.md).

The public API remains the same receiver-function SDK surface for remote and
Pouch clients. State bodies, queue payloads, attachments, scan output,
query-document output, crypto, and compression use real streaming paths unless
the caller explicitly chooses an in-memory source or sink.
Collection push and staged-pull appends write bounded canonical chunks as described
in [the append contract](docs/collection-writer.md); collection reads remain raw
ordered streams.

## C ABI and storage compatibility

Core ABI **9**, introduced in v0.26.0 from ABI **8** in v0.25.0, remains
unchanged in v0.27.0. ABI 9 consumers remain compatible; the PostgreSQL
provider's own ABI remains **0**. When upgrading from ABI 8, rebuild C consumers,
Lua native bindings and the optional PostgreSQL provider together.
That ABI change covers atomic collection edits and recovery heads;
see [the edit contract](docs/collection-edits.md) and
[upgrade instructions](CHANGELOG.md#upgrading-from-025x).

Pouch upgrades released collection heads on open under exclusive root access,
preserving positions, retry identities, retention and entry bodies. Close other
clients before the first upgrade and keep older binaries away from the upgraded
root. Shared-library ABI and on-disk format versions are independent. Earlier
source migrations, including the mapped `save()` options argument introduced
with state retention, remain documented in the [release notes](CHANGELOG.md).

## Build system

The repository (<https://github.com/c89systems/liblockdc>) uses a Makefile-first lifecycle with CMake as the build backend:

- `Makefile`
  - primary developer entry point
  - build, test, package, clean, and release orchestration
- `CMake`
  - target graph
  - install graph
  - exported package metadata
  - test registration

Normal host and release Make targets provision the required dependency trees automatically. The low-level `scripts/build.sh` helper assumes that the matching dependency root already exists.

PostgreSQL's client/facade SDK is optional. Core builds default to
`-DLOCKDC_BUILD_POSTGRES=OFF` and require no cpktdb installation. To prepare the
optional provider SDK, native participant and its validation targets, use
`LOCKDC_BUILD_POSTGRES=ON make deps` or
`scripts/deps.sh deps-x86_64-linux-gnu --with-postgres`, then configure with
`-DLOCKDC_BUILD_POSTGRES=ON`. The [C participant guide](docs/postgres.md) describes
its explicit static/shared linkage, registration, borrows and recovery. The
core library binaries are identical with the optional provider present or
absent. PostgreSQL 14 through 18 are covered by mixed-transaction integration
tests using two PostgreSQL servers and one Pouch store.
For a clean release that includes provider SDKs, set `LOCKDC_BUILD_POSTGRES=ON`;
fresh library configurations use the same selection as dependency provisioning.
An existing CMake cache or explicit `-DLOCKDC_BUILD_POSTGRES=OFF` takes precedence
over the environment default.

## Build prerequisites

Normal development expects:

- CMake 3.24 or later
- Ninja
- GNU Make
- host `clang-format` for `make format`
- host `clangd` for editor diagnostics of the public C API
- host Valgrind for the native Memcheck gate
- `qemu-aarch64` and `qemu-arm` for the non-host release test matrix
- rootless Podman and Python 3.9 or newer for the local Podman Kube development environment

Every Linux build uses its matching pinned Bootlin GCC collection, including
the compiler, linker, binutils, sysroot, headers, and runtime. The Make and
CMake configurations provision those collections automatically; do not substitute
host or distro cross compilers. Toolchains are shared under
`${CPKT_TOOLCHAIN_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cpkt/toolchains}`
and verified dependency archives under
`${CPKT_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cpkt/deps}`.
Repository-local `.cache/` directories are disposable build and staging state.

Darwin cross builds use a developer-supplied osxcross SDK under `OSXCROSS_ROOT`.
The lifecycle selects the newest complete `arm64-apple-darwin25.x` collection;
set `CPKT_OSXCROSS_HOST` to pin an installed prefix. Prepare it with
`scripts/cpkt-toolchains.sh ensure arm64-apple-darwin` before configuring.
That command provisions the pinned Linux host MIG helper in the shared toolchain
cache, without obtaining Apple content. Darwin configure requires both tools;
MIG is build tooling and is excluded from SDK packages.

The pinned `cpkt` 0.6.1 GNU SDKs require glibc 2.43 or newer at
deployment. This does not change the `liblockdc` public ABI, but deployments
using the shared GNU SDK must upgrade their runtime accordingly. Development
executables, tests, examples, Lua module checks, and SDK consumer probes run
directly with their selected Bootlin ELF interpreter and private runtime lookup
paths; they do not use `LD_LIBRARY_PATH` or a host Lua interpreter.

The separately pinned dependencies are:

| Dependency | Version | Use |
| --- | --- | --- |
| [cpkt](https://github.com/c89systems/cpkt) | 0.6.1 | Core SDK dependencies and Lua runtime |
| [cpktdb](https://github.com/c89systems/cpktdb) | 0.3.1 | Optional PostgreSQL C89 facade and database SDK |
| lonejson | 0.45.0 (ABI 26) | JSON parsing, mapping, and streaming |
| libpslog | 0.12.0 | SDK diagnostics |
| liblql | 0.4.1 | Pouch query support |
| cmocka | 2.0.2 | Unit tests only; target-specific static library from cpkt |

OpenSSL, curl, zlib, nghttp2, libssh2, and Lua come from cpkt.
cmocka comes from cpkt's target-specific SDK and remains a test-only dependency.

Core and database SDKs remain in separate `cpkt/install` and `cpktdb/install`
prefixes under each target's dependency root. cpktdb records its exact core build
pin as provenance and requests a compatible minimum core through CMake. Core's
declared compatibility floor governs independent upgrades; matching major/minor
versions alone do not establish compatibility. cpkt 0.6.1 accepts baselines from
0.2.0 through 0.6.1; cpktdb 0.3.1 requires at least 0.5.1. Target and payload
integrity checks remain enforced. `scripts/deps.sh` pins each provider release and
checksum independently; cpktdb is acquired/discovered only when PostgreSQL is
enabled. Consumers use the official imported targets. pkg-config consumers must
also run the provider's CMake compatibility preflight. cpktmisc is not required
by liblockdc. PostgreSQL applications use cpktdb's checked result
retrieval API to distinguish drainage from local retrieval failures.

## Common commands

Build the normal host development preset:

```bash
make build
```

`make build` and `make test-debug` use the sanitizer-instrumented debug
preset. Third-party dependencies are still reused from the release-built host
dependency cache.

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
serial. Cross-target testing, Valgrind, and fuzzing are release confidence
work: `make prerelease` runs the native debug/e2e suite, Valgrind, and full
fuzzing, while `make release-matrix` builds, tests, packages, and verifies the
shipped Bootlin GNU/musl and QEMU cross matrix. The AFL++ compiler bootstrap is
hardening work, not an everyday functional invariant. Performance workloads
belong to the explicit `make bench-gate` command. The complete artifact
rehearsal remains `make release`.

`make prerelease-hardening` runs ordinary prerelease, including full fuzzing,
then adds native benchmarks, the Pouch-vs-lockd performance thresholds, the
finite multi-mode Pouch core churn soak, reclaim proof, and outbox reconciliation
hardening. Run `make prerelease-hardening release-matrix` when the same exercise
also needs the shipped target/package matrix.
The Pouch-vs-lockd performance comparison measures every core storage
operation separately: acquire, update and stale-precondition rejection,
release, public and leased reads, attachment write/read, queue delivery, all
index-publication phases, indexed/scan/full-text queries, and restart recovery.

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
comparisons use a bounded 12-record, two-update, 128 KiB-payload profile with
16 KiB segments to exercise rollover and recovery. Each comparison has a
90-second measurement limit; preparation is separate. Large production
profiles remain available through `make benchmark-pouch-go-production`. The
broader Go suite also includes compression, compaction, concurrency, and query
scale benchmarks; see [the benchmark guide](benchmark/README.md) for its
profiles and command options.

All significant Make targets print total elapsed time on completion.

`make format` runs `clang-format` over the C source/header tree and is also
part of the clean-slate release pipeline.

## Local development environment

The repository includes a local `lockd` environment for integration testing and example execution. The stack includes:

- a shared-disk `lockd` pair
- an S3-backed `lockd`
- a `mem://` `lockd` exposed over a Unix domain socket
- MinIO

Start the environment:

```bash
make dev-up
```

Reset generated environment state:

```bash
make dev-reset
```

Stop the environment:

```bash
make dev-down
```

The e2e environment is self-contained. `make test-e2e` starts or reuses the rootless Podman Kube services, probes the active disk and S3 APIs, and runs the e2e CTest preset. Once the full run finishes, it stops the stack and verifies that the pod and all declared containers, including bootstrap containers, are removed. This also applies after failures or interruption and when the stack was already running before E2E began. `make example-smoke-local` uses the same lifecycle. Overlapping E2E runs in one checkout are rejected. Set `LOCKDC_E2E_KEEP_DEVSERVICES=1` explicitly to retain the stack for debugging. Use these Make runners for automatic lockd service management; direct lockd CTest or binary invocation requires manual `dev-up`/`dev-down`. PostgreSQL XA CTest checks manage their separate two-server Podman Kube stack automatically; see [the participant guide](docs/postgres.md#recovery-and-observation).

All service data, credentials, and test temporary files live under `build/devenv/`; `make dev-reset` stops the pod before deleting that directory. Host ports are stable per checkout and can be overridden with the `LOCKDC_*_PORT` variables shown by `make help`. Run `source build/devenv/env.sh` after `make dev-up` to use the effective endpoints in examples.

Additional development-environment notes are available in the repository at `devenv/README.md`.

## Packaging and release archives

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
Require the whole exercise to pass before squashing. These
candidate artifacts are preparation evidence. The signed, tagged commit still
requires its final clean release gate.

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
`make prerelease-hardening` layer. Use
`make release-matrix` when you explicitly want to reuse existing build and
dependency caches for a faster release matrix/package rerun.

During a full release, Debug owns the `lifecycle-host` source and host-fixture
checks and complete behavioral tables. The three large Pouch, outbox and
collection suites use explicit portability cases in optimized GNU/musl and
QEMU lanes; their lists live in `tests/unit/portability.cmake`. These retain
storage-format and integer-width boundaries, crash/restart recovery, compaction,
durable synchronization, shared writers, retention, collection selection and
ownership regressions. Smaller unit suites run in full on every Linux target,
including mixed state/collection churn, allocator probes and the small-stack
compaction regression. Every target retains its SDK and ABI checks.
Executing a storage test binary without case arguments still runs its full
table, including on a release target.

Every configuration partitions collection checks into `lc_unit_collections`,
`lc_unit_collections_append` and `lc_unit_collections_recovery`, sharing one
executable. Debug runs the complete table; optimized GNU/musl and QEMU lanes
partition their declared portability cases. A configuration-specific selection
contract runs in each runtime lane and checks that every case runs exactly once.
Select all three with `ctest --test-dir build/debug -R '^lc_unit_collections'`.

Debug runs the complete PostgreSQL 14–18 matrix, with two servers, Pouch and
the C/Lua driver variants. Optimized native GNU repeats PostgreSQL 14; the
additional server versions carry the `native-behavior` label and run once.
Lua E2E prepares one fresh installed rock through a CTest fixture per invocation;
each scenario still runs in its own Lua process and retains allocator failure
checks. Selecting an individual scenario automatically includes its setup.
Standalone and final-artifact Lua package tests retain their own installations.

Prerelease compiles its AFL configuration before long service/memory checks;
the later full fuzz campaign reuses those compiled outputs. Lease-expiry
semantics use controlled clocks where possible; real worker timing remains
tested. This changes coverage ownership, not clean-release or signing rules.

The cpkt compatibility fixture consumes the same GNU dependency
SDK in Debug and Release, so Debug owns it; the distinct musl SDK is checked
in its Release lane. The `release-artifact` checks have standalone CTest
entrypoints; the release pipeline verifies those contracts against the final
archives and checksum inventory during packaging. Native archive consumers build and run
in one pass per target, while cross consumers retain target-specific link
checks. Valgrind builds only its selected test targets and their prerequisites.
It runs the full Pouch suite and five focused outbox shutdown/allocator cases;
the complete outbox inventory and timing assertions run under native sanitizers.
`CMOCKA_TEST_FILTER` can select another case for Valgrind diagnosis.
`make test-all` reuses the Debug sanitizer build for E2E; standalone
`make test-e2e` retains its own preset. Static and shared core libraries share
compiled implementation objects. SDK configuration permutations run once per
distinct extracted package configuration within the packaging invocation;
every target still builds its own consumers against its final archives.

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
embeddings preload the same entry point. See the [Lua XA guide](docs/lua.md#optional-postgresql-participation).

### Release archive contents

The release archive contains:

- `liblockdc` public headers
- bundled dependency headers for `libpslog`, `curl`, `OpenSSL`, `nghttp2`, and `lonejson`
- `liblockdc.so*`
- `liblockdc.a`
- the static archives for the bundled third-party dependencies
- bundled shared runtime dependencies
- `pkg-config` metadata
- CMake package metadata
- project documentation and license files
- bundled third-party license files

Archive layout and contents are regression-tested. The release verification suite asserts the complete shipped matrix, expected archive names, checksum manifest, and package contents.

## Public API model

The public API is intentionally receiver-function based rather than a flat RPC
wrapper. `lc_client_open()` returns an `lc_client *`; methods then live on the
returned handle as function pointers such as `client->acquire(...)`,
`lease->update(...)`, `message->ack(...)`, and `service->run(...)`.

The standalone `lc_*` functions remain available as compatibility wrappers and
as useful entry points for callers that prefer a flat symbol lookup, but the
primary public surface and examples use the receiver-function form. New method
slots are appended to preserve layout stability within the current
shared-library ABI line.

The installed [public header](include/lc/lc.h) is the detailed API reference;
the generated `lc/version.h` also documents the compile-time semantic-version
and ABI macros. Their Doxygen contracts cover every public handle,
request/result type, field, callback, helper, and compatibility function. The
conventions are:

- initialize transparent config and request structs with their matching
  `*_init()` helper before setting fields;
- input pointers are borrowed for the call unless their documentation says
  ownership transfers;
- returned handles and heap-backed result fields are caller-owned and are
  released with the matching `close()` or `*_cleanup()` helper;
- close and cleanup helpers accept `NULL`, and cleanup helpers zero their
  object after releasing nested ownership;
- fallible calls return `LC_OK` or an `LC_ERR_*` status and may populate an
  `lc_error`, which is released with `lc_error_cleanup()`;
- `lc_source_from_fd()` and `lc_sink_to_fd()` borrow the descriptor and do not
  close it; path-backed constructors own the descriptor they open;
- streaming APIs use bounded producer-to-consumer buffers. Mapped lonejson
  loads normally materialize mapped fields, while spool-backed mappings may
  keep large fields file-backed.

Portable widths are part of that contract: versions and Unix timestamps are
signed 64-bit values, index sequences and transaction-coordinator terms are
unsigned 64-bit values, and legacy public `long` byte/count fields reject
values that cannot be represented on the calling architecture.

- `lc_client`
  - root client handle
- `lc_lease`
  - lease and state handle
- `lc_message`
  - queue delivery handle
- `lc_source`, `lc_sink`
  - streaming state and payload abstractions

Typical flow:

1. initialize an `lc_client_config`
2. open an `lc_client`
3. acquire a lease or dequeue a message
4. operate on the returned handle
5. finish with `release()`, `ack()`, or `close()`

This keeps lease identity, transaction identifiers, and related lifecycle state on the handle instead of forcing callers to pass those values through every operation manually.

### JSON and lonejson

`liblockdc` depends on `lonejson 0.45.0` with shared-library ABI `26`.
`lonejson` is used for:

- typed JSON response parsing for management, attachment, queue, namespace,
  transaction, and state metadata paths
- mapped state `load()` and `save()` through caller-provided
  `LONEJSON_FIELD_*` maps
- streaming JSON request serialization through lonejson curl upload adapters
- query-key streaming and queue subscribe multipart/metadata parsing
- Lua JSON encode/decode helpers through the `lockdc` Lua rock dependency

The client config field `http_json_response_limit_bytes` caps typed JSON
responses parsed through `lonejson`. Zero uses
`LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT`, currently 100 MiB. The cap applies to
dynamic strings, JSON value capture, and spool-backed mapped fields on response
parsing. It does not cap request-side JSON serialization; large mapped saves
should use lonejson source-backed fields when the source value is naturally
file-backed or stream-backed.

Mapped `load()` destinations may be reused. The SDK prepares destinations with
the runtime `reset` path when they already own lonejson-managed storage and
with `init` on first use, while preserving preconfigured `lonejson_json_value`
capture sinks. Callers still own final cleanup of lonejson-owned mapped fields
with a compatible lonejson runtime after the loaded value is no longer needed.

## Examples

The repository includes standalone example programs under `examples/`.

- C examples: `examples/*.c`
- Lua examples: `examples/lua/*.lua`

The Lua examples assume the `lockdc` and `lonejson` rocks are installed and
show the intended Lua DX directly without wrapper helper modules. Additional
Lua example notes live in `examples/lua/README.md`.

The Lua consumer API is intentionally single-threaded and blocking. Lua
handlers run one message at a time on the calling Lua state; the binding does
not expose the native threaded C callback model into the same Lua VM.

The Lua SDK reference and dependency policy are documented in
`docs/lua.md`.

## Lua SDK

`liblockdc` ships a Lua frontend for the public client API as the `lockdc`
module.

The intended ownership model is:

- `liblockdc` owns the Lua-facing `lockd` client
- `liblockdc` owns the Lua-facing `lonejson` dependency boundary for what it
  exposes by declaring the supported `lonejson` Lua rock version
- downstream components such as `vectis` should consume that shipped Lua
  client distribution instead of maintaining a second `lockd` Lua client or an
  incompatible JSON binding layout

This keeps one coherent SDK import path for downstream Lua outbox runtimes.

For the Lua public surface, consumer behavior, and packaging model, see:

- `docs/lua.md`
- `examples/lua/README.md`

The following C snippets show the expected calling style directly.

Open a client:

```c
lc_client_config config;
lc_client *client;
lc_source *client_bundle;
lc_error error;
const char *endpoints[] = { "https://localhost:19441" };

lc_client_config_init(&config);
lc_error_init(&error);
client_bundle = NULL;

if (lc_source_from_file("./client.pem", &client_bundle, &error) != LC_OK) {
  fprintf(stderr, "failed to open client bundle: %s\n", error.message);
  lc_error_cleanup(&error);
  return 1;
}

config.endpoints = endpoints;
config.endpoint_count = 1;
config.client_bundle_source = client_bundle;
config.default_namespace = "default";

if (lc_client_open(&config, &client, &error) != LC_OK) {
  fprintf(stderr, "lc_client_open failed: %s\n", error.message);
  lc_source_close(client_bundle);
  lc_error_cleanup(&error);
  return 1;
}
lc_source_close(client_bundle);
```

`client_bundle_path` remains available for existing C callers, but new code
should prefer `client_bundle_source` so PEM bundles can come from files, memory,
file descriptors, or callback-backed sources.

Allocation tracking is **off by default**. To enable it for the process-wide
built-in allocator, call `lc_allocator_tracking_configure(1, &error)` once at
startup, before creating SDK objects or starting concurrent SDK use. Pass `0`
to explicitly keep it off; omitting the call has the same default. Successful
configuration cannot be repeated or changed. Custom allocators never participate
in tracking. Provide malloc/free hooks together; realloc is optional. A full
custom hook set is called directly with the requested sizes and original
pointers. Without a realloc hook, the SDK retains size-only metadata to emulate
resizing through your malloc/free pair.

The setting selects plain libc or tracking callbacks once. Disabled built-in
allocation has no tracking-flag test, header, counter update, lock, or lazy
initialization. When enabled, after closing all handles and cleaning up errors,
ordinary process exit is silent. Unreleased allocations produce a count and byte
total on stderr, for example `liblockdc allocator: 1 live allocations (37 bytes)
at exit`. Shared-library unload also reports outstanding allocations. This covers
the built-in allocator interfaces, excluding raw facade metadata and allocations
made directly by your application or dependencies; abrupt termination does not
run the report. Tracking uses an aligned header inside each allocation and a
fixed table of counters, with no separate metadata allocations or pointer
registry. Reported bytes count only the requested payload. Each tracked
operation updates counters; include this overhead when measuring
allocation-heavy workloads.

E2E enables this tracker for C tests/examples and Lua consumers, including the
PostgreSQL integration tests. A leak diagnostic fails the test even when its
consumer exits successfully. Dependency allocations made outside the built-in
allocator require the separate Valgrind lifetime checks: these exercise logger
ownership, failed opens, encrypted Pouch, repeated library unload/reload, idle
application threads, and static process shutdown. Close all SDK handles and
finish concurrent SDK calls before unloading the library. Library-owned fallback
loggers and thread runtimes are released before the allocator exit report.

Acquire a lease and update JSON state:

```c
lc_acquire_req acquire;
lc_release_req release;
lc_lease *lease;
lc_source *src;

lc_acquire_req_init(&acquire);
lc_release_req_init(&release);

acquire.key = "orders/42";
acquire.owner = "payments";
acquire.ttl_seconds = 60L;

if (client->acquire(client, &acquire, &lease, &error) != LC_OK) {
  fprintf(stderr, "acquire failed: %s\n", error.message);
  lc_error_cleanup(&error);
  client->close(client);
  return 1;
}

if (lc_source_from_memory("{\"status\":\"processing\"}",
                          strlen("{\"status\":\"processing\"}"),
                          &src, &error) != LC_OK) {
  fprintf(stderr, "source failed: %s\n", error.message);
  lc_error_cleanup(&error);
  lease->close(lease);
  client->close(client);
  return 1;
}

if (lease->update(lease, src, NULL, &error) != LC_OK) {
  fprintf(stderr, "update failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

lc_source_close(src);

if (lease->release(lease, &release, &error) != LC_OK) {
  fprintf(stderr, "release failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

client->close(client);
```

For callback-style update flows, `lc_acquire_for_update()` acquires the lease,
captures the complete private snapshot in a temporary file, invokes the handler,
closes the snapshot, and always attempts to release the lease before returning.
The lease and snapshot reader are borrowed and valid only during the callback;
the helper owns the final release. Handler success commits staged changes;
handler failure releases
with rollback so partial callback updates are not published. The callback reads
a file-backed snapshot after capture completes. For direct transfer without
snapshot staging, use `lc_acquire()` followed by `lc_lease_get()` with a sink.
The callback helper is used as follows:

```c
static int update_order(void *ctx, lc_acquire_for_update_context *af,
                        lc_error *error) {
  lc_source *src;
  int rc;

  (void)ctx;
  rc = lc_source_from_memory("{\"status\":\"processing\"}",
                             strlen("{\"status\":\"processing\"}"),
                             &src, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = af->lease->update(af->lease, src, NULL, error);
  lc_source_close(src);
  return rc;
}

if (lc_acquire_for_update(client, &acquire, update_order, NULL, &error) !=
    LC_OK) {
  fprintf(stderr, "acquire_for_update failed: %s\n", error.message);
  lc_error_cleanup(&error);
}
```

Queue APIs follow the same handle-oriented pattern: dequeue or subscribe, operate on the returned `lc_message`, and then finish with `ack()`, `nack()`, or `close()`.

The examples in the repository at <https://github.com/c89systems/liblockdc/tree/trunk/examples> are complete programs intended to be runnable from the build tree and illustrate how an application would wire client configuration, logging, and request structures directly.

## Repository layout

- `include/lc/lc.h`
  - public API
- `src/`
  - library implementation
- `examples/`
  - standalone example programs
- `tests/unit/`
  - unit and transport tests
- `tests/e2e/`
  - e2e tests against the local `lockd` environment
- `tests/fuzz/`
  - AFL++ unit harnesses and isolated-process Pouch integration mutation
    harnesses
- `scripts/`
  - build, release, and environment scripts
- `devenv/`
  - local environment notes

## Low-level entry points

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

See <https://github.com/c89systems/liblockdc> for the full source code.
