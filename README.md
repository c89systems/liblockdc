# liblockdc

`liblockdc` is a C89/C90 client library for `lockd`, providing persistent state,
ordered collections, leases, queues, attachments, queries, and built-in implicit
XA transactions with coordinated commit, rollback, and recovery. It also
includes Pouch, an embedded storage engine that makes these capabilities
available directly inside your application without a separate `lockd` server.

It ships static and shared libraries, C++98-compatible public headers, and a Lua
facade.

## Capabilities

- Built-in implicit XA for state and attachment changes, including recovery.
- Ordered collections with immutable JSON entries, retention, LQL filtering,
  incremental appends, collection leases, and atomic edits.
- Leased state, queue delivery, attachments, and bounded byte streaming.
- Mapped JSON through lonejson, streamed queries, and managed consumers.
- Durable command receipts, inbox/outbox processing, retries, and dead letters.
- Remote HTTP/1.1, HTTP/2, mTLS, and Unix domain socket transports.
- Optional PostgreSQL participation in the existing XA coordinator.
- C and Lua APIs with integrated libpslog diagnostics.

Collections and transactional messaging currently require Pouch. The managed XA
coordinator uses a local Pouch journal; remote lockd participation in that
coordinator is unsupported. See the feature guides below for backend limits.

## Getting started

Use a [target SDK archive](docs/releases.md#sdk-archives), or build from source:

```bash
make build
make test
```

These commands prepare the native sanitizer build and run its debug tests. Make
provisions the pinned compiler and dependency SDKs. See
[development and verification](docs/development.md) for prerequisites, clangd
setup, optional PostgreSQL builds, and the test layers. `make help` lists the
available commands.

For local storage, select an absolute endpoint such as
`pouch:///var/lib/my-service/lockd-root`. Configure it through
[`lc_pouch_settings` or Lua's `pouch` table](docs/pouch-open-settings-api.md).
For Lua installation and usage, see [the Lua SDK guide](docs/lua.md).

## Supported targets

| Platform | Architectures | Libraries |
| --- | --- | --- |
| Linux, GNU or musl | `x86_64`, `aarch64`, `armhf` | Static `.a` and shared `.so` |
| macOS, built with osxcross | `arm64` | Static `.a` and shared `.dylib` |

Shared GNU SDKs require glibc 2.43 or newer. Core shared-library ABI is **9**;
the optional PostgreSQL provider ABI is **0**. See
[ABI and storage compatibility](docs/c-api.md#abi-and-storage-compatibility) and
the [release notes](CHANGELOG.md) before upgrading.

## Documentation

| Topic | Guide |
| --- | --- |
| C handles, ownership, streaming, JSON, allocation tracking | [C API](docs/c-api.md) and [public header](include/lc/lc.h) |
| Lua API, installation, and embedding | [Lua SDK](docs/lua.md) |
| Pouch endpoints, defaults, and configuration | [Opening Pouch](docs/pouch-open-settings-api.md) |
| Persistence, recovery, and compression changes | [Pouch storage](docs/pouch-storage.md), [compression](docs/pouch-compression.md) |
| Ordered journals, selection, and retention | [Collections](docs/collections.md) |
| Incremental writes, ownership, and atomic edits | [Append writers](docs/collection-writer.md), [leases](docs/collection-leases.md), [edits](docs/collection-edits.md) |
| Per-object expiry | [State retention](docs/state-retention.md) |
| Transactional messaging and dispatch | [Inbox/outbox](docs/inbox-outbox.md), [dispatcher](docs/outbox-dispatch-architecture.md) |
| XA coordination and PostgreSQL participation | [XA contract](docs/xa.md), [PostgreSQL driver](docs/postgres.md) |
| Builds, dependencies, tests, and local services | [Development](docs/development.md) |
| Release preparation and SDK archives | [Releases](docs/releases.md) |
| Performance workloads and comparisons | [Benchmark guide](benchmark/README.md) |

## Examples

- [Client and lease methods](examples/client_and_lease_methods.c)
- [Callback-style state updates](examples/acquire_for_update.c)
- [Collection journal](examples/collection_journal.c)
- [Outbox producer](examples/outbox_producer.c) and
  [Lua dispatcher](examples/lua/outbox_dispatcher.lua)
- [Lua examples and setup](examples/lua/README.md)

The examples are complete programs. The [development guide](docs/development.md)
explains how to run the local service environment they use.

## License

See [LICENSE](LICENSE).
