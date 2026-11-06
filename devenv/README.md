# liblockdc devenv

The local E2E environment uses rootless Podman, `podman kube play`, Python 3.9 or newer,
curl, and `flock` (util-linux). `scripts/devenv.sh` owns the lifecycle; there is one tracked service
graph in `devenv.yaml.in` (JSON syntax, which is valid YAML). No container-engine
fallback is used.

```bash
make dev-up
source build/devenv/env.sh
make dev-ps
make dev-logs
make test-e2e
make dev-down
make dev-reset
```

`dev-up` renders a private manifest to `build/devenv/devenv.yaml`, creates the
pod without starting its containers, starts MinIO, checks its HTTP health, and
runs the declared bucket bootstrap container. Repeated `dev-up` resumes
initialization or restarts stopped containers while retaining generated data. It then starts the pod's TLS init
containers and lockd servers and checks the TCP listeners, generated bundles,
and Unix socket. It selects the active shared-disk node with a bounded
acquire/rollback probe using the declared image's CLI, then exports that node
in `env.sh` for both C and Lua clients. `test-e2e` additionally exercises the
disk and S3 APIs through an acquire/update/release probe.

The pod uses a separate rootless `pasta` network. All services share pod-local localhost. Disk-a listens on 9341, disk-b on 9342,
S3 on 9343, and MinIO on 9000/9001 inside the pod. Both disk servers share the
same disk state and TLS configuration. The S3 server uses the local
`lockd-client-s3` bucket. The memory server exposes HTTP over
`build/devenv/state/mem-run/lockd.sock`, with mTLS deliberately disabled to
exercise that transport. Each lockd server retains its one-CPU, 150 MB limit.

Images are pinned to `docker.io/pktsystems/lockd:v0.8.1`,
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`, and
`quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z`. Certificate init jobs use the exact lockd image's static CLI through the
shell-capable mc image and preserve existing bundles; bucket initialization
is repeatable. Retaining the CA is required to reopen encrypted lockd data. The pod uses `keep-id:uid=0,gid=0`; the container
writers map to the invoking ordinary host user. Generated data is bind-mounted,
so reset requires no recursive ownership changes or elevated permissions.

Pod names and default high host ports derive from the canonical checkout path.
Published ports bind to loopback. `dev-up` prints the effective endpoints,
images, pod, credentials, state root, and environment file. If a port is already
occupied, choose overrides before starting:

```bash
LOCKDC_DISK_A_PORT=20441 LOCKDC_DISK_B_PORT=20442 LOCKDC_S3_PORT=20443 \
LOCKDC_MINIO_API_PORT=20000 LOCKDC_MINIO_CONSOLE_PORT=20001 make dev-up
```

Run `make dev-reset` before changing ports on an existing rendered environment.
Later commands reuse the saved ports when overrides are omitted.
`dev-down` allows up to 60 seconds for graceful pod shutdown, uses the same
rendered manifest, verifies that the pod and all declared containers are gone,
and retains generated data;
`dev-reset` tears down first and then removes `build/devenv/`. Static tracked
files in `devenv/` are preserved. A teardown failure retains the manifest and
state for diagnosis.

PostgreSQL XA checks use a separate Podman Kube manifest with two PostgreSQL
instances and a local Pouch store per test consumer. With PostgreSQL selected
and the integration binaries built,
`ctest --test-dir build/debug -L postgres -j1 --output-on-failure` runs PostgreSQL
14 through 18. These CTest wrappers
start their own stack, probe readiness and assert that the pod and both containers
are absent after every run, including failure. They do not require `dev-up`.
See [the participant guide](../docs/postgres.md#recovery-and-observation).

Generated bundles are `build/devenv/credentials/disk/client.pem` and
`build/devenv/credentials/s3/client.pem`; each includes its CA, certificate,
and private key. The local-only MinIO credentials are `lockddev` /
`lockddevpass`. They are declared in the private rendered manifest and are
unsuitable for external services.

`test-e2e` reuses services already running from `dev-up`. If any long-lived
service is stopped or missing, it starts or resumes the stack. After all selected
tests finish, it tears down the entire stack, including services that were
already running. Teardown also runs after a build/startup/test failure or
SIGINT/SIGTERM, and a teardown or resource-removal verification failure fails
the command. `example-smoke-local` follows the same lifecycle. A checkout lock
rejects overlapping E2E runs without stopping the active run's stack.
`LOCKDC_E2E_KEEP_DEVSERVICES=1 make test-e2e` explicitly keeps the stack for
inspection. Test scratch and timing logs use `build/devenv/tmp/`. Failures print
the failed command, effective endpoints, pod state, and pod logs. The generated
`env.sh` exports endpoints, client bundles, the Unix socket, and primary/failover
URLs for the C and Lua examples. Source it before invoking CTest directly.

## Build and test

Normal local flows:

```bash
make build
make test
make test-e2e
make test-all
make coverage
make fuzz-smoke
```

Equivalent CMake preset flows:

```bash
cmake --preset debug
cmake --build --preset debug

cmake --preset e2e
cmake --build --preset e2e
ctest --preset debug
ctest --preset e2e
```

The normal Make and CMake flows provision the pinned Bootlin compiler
collection and matching dependency roots automatically. Linux builds never
fall back to host or distro compilers. Toolchains and verified dependency
archives are shared outside the checkout under `CPKT_TOOLCHAIN_CACHE` and
`CPKT_DEPENDENCY_CACHE`; repository-local `.cache/` remains disposable build
and staging state. Low-level scripts are implementation helpers, not the
normal developer command surface.

## Packaging

Release SDKs are split by target, with the shared-library ABI recorded inside
each bundle:

- `liblockdc-<version>-<target-id>.tar.gz` contains public headers, bundled
  dependency headers and libraries, `liblockdc.so*`, `liblockdc.a`, and package
  metadata.
- `liblockdc-<version>.tar.gz` contains the C source release.
- `liblockdc-postgres-<version>-<target-id>.tar.gz` contains the optional native
  PostgreSQL provider SDK when selected, separate from the core SDK.
- `liblockdc-lua-<version>.tar.gz`, the rendered rockspec and `.src.rock`
  contain the separate Lua facade sources and documentation.
- `lockdc-postgres-<version>-1.rockspec` and its `.src.rock` supply the optional
  Lua registration module alongside the matching core Lua rock.

Normal packaging flows:

Follow the [release/signing sequence](../docs/releases.md#release-process)
on local `trunk` before invoking the final release gate.

```bash
make release
make package-verify
```

`make release` is the clean-slate release gate. It checks the release version contract, removes generated state, and runs
formatting, native sanitizer and lockd E2E tests, Valgrind, full fuzzing,
and the release matrix. `make prerelease-hardening` adds benchmark and
stress gates; `make bench-gate` runs the benchmark suite and both parity gates. The primary
Makefile release flow provisions the dependency roots it needs.

`make package-verify` builds and verifies packages, source/SDK/Lua artifacts,
checksums, and recursive privacy for the shipped targets without rerunning
tests. `make release-matrix` rebuilds, tests, packages, and verifies that matrix:

- `x86_64-linux-gnu`
- `x86_64-linux-musl`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `armhf-linux-gnu`
- `armhf-linux-musl`
- `arm64-apple-darwin` (local osxcross build; see the root README prerequisites)

## Fuzzing

The repository uses the lifecycle-owned fuzz build for parser, record, stream,
and storage-state hardening:

```bash
make fuzz-smoke
```

Or via the fuzz preset:

```bash
cmake --preset fuzz
cmake --build --preset fuzz
ctest --preset fuzz
```

The fuzz build uses pinned AFL++ GCC-plugin instrumentation over the native
x86_64 Bootlin collection. It remains host-native only, keeps fuzzing outside
ordinary package/test presets, and runs bounded committed-corpus smoke jobs
through `make fuzz-smoke`.

Current fuzz targets cover:

- streamed source/sink/json surfaces
- client bundle parsing during `lc_client_open()`
- attachment response decoding
- queue subscribe meta decoding
- streamed query-key decoding
- local mutate parsing and application

## Direct E2E execution

Prefer `make test-e2e` or `make example-smoke-local` for automatic startup and
verified teardown. Direct CTest/binary invocation requires manual service
management:

```bash
make dev-up
source build/devenv/env.sh
cmake --preset e2e
cmake --build --preset e2e
ctest --preset e2e
make dev-down
```

The CTest scratch fixture also creates its workspace for direct service-independent
Pouch test selections after `dev-reset`; Podman is not required for those tests.

Unit coverage excludes the `e2e` label. The TLS/client-bundle/libcurl path is
covered by the separate local-service suite. Its local TLS tests explicitly
allow development certificates; the normal client default remains strict.
