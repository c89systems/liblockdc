# C API, ownership and streaming

Include `<lc/lc.h>` from the target SDK. The public API supports C89/C90 and
C++98-or-later consumers through C linkage. PostgreSQL participation uses the
separate optional [provider header and library](postgres.md).

## Receiver handles

The public API is intentionally receiver-function based rather than a flat RPC
wrapper. `lc_client_open()` returns an `lc_client *`; methods then live on the
returned handle as function pointers such as `client->acquire(...)`,
`lease->update(...)`, `message->ack(...)`, and `service->run(...)`.

The standalone `lc_*` functions remain available as compatibility wrappers and
as useful entry points for callers that prefer a flat symbol lookup, but the
primary public surface and examples use the receiver-function form. Receiver
layout changes follow the
[shared-library ABI contract](#abi-and-storage-compatibility).

The installed [public header](../include/lc/lc.h) is the detailed API reference;
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
- close and cleanup helpers accept `NULL`, and cleanup helpers zero their object
  after releasing nested ownership;
- fallible calls return `LC_OK` or an `LC_ERR_*` status and may populate an
  `lc_error`, which is released with `lc_error_cleanup()`;
- `lc_source_from_fd()` and `lc_sink_to_fd()` borrow the descriptor and do not
  close it; path-backed constructors own the descriptor they open;
- streaming APIs use bounded producer-to-consumer buffers. Mapped lonejson loads
  normally materialize mapped fields, while spool-backed mappings may keep large
  fields file-backed.

Portable widths are part of that contract: versions and Unix timestamps are
signed 64-bit values, index sequences and transaction-coordinator terms are
unsigned 64-bit values, and legacy public `long` byte/count fields reject values
that cannot be represented on the calling architecture.

| Handle | Purpose |
| --- | --- |
| `lc_client` | Root client and management operations |
| `lc_lease` | Leased state and attachment operations |
| `lc_message` | Queue delivery and payload ownership |
| `lc_collection`, `lc_collection_writer` | Ordered journals and incremental appends |
| `lc_outbox`, `lc_outbox_dispatcher` | Transactional production and explicit dispatch |
| `lc_source`, `lc_sink` | Byte input/output abstractions |

Typical flow:

1. initialize an `lc_client_config`
2. open an `lc_client`
3. acquire a lease or dequeue a message
4. operate on the returned handle
5. finish with `release()`, `ack()`, or `close()`

This keeps lease identity, transaction identifiers, and related lifecycle state
on the handle instead of forcing callers to pass those values through every
operation manually.

## JSON and lonejson

`liblockdc` uses the pinned lonejson SDK listed in the
[dependency table](development.md#dependencies). `lonejson` is used for:

- typed JSON response parsing for management, attachment, queue, namespace,
  transaction, and state metadata paths
- mapped state `load()` and `save()` through caller-provided `LONEJSON_FIELD_*`
  maps
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
the runtime `reset` path when they already own lonejson-managed storage and with
`init` on first use, while preserving preconfigured `lonejson_json_value`
capture sinks. Callers still own final cleanup of lonejson-owned mapped fields
with a compatible lonejson runtime after the loaded value is no longer needed.

## Client configuration and credentials

Initialize `lc_client_config` and `lc_error`, supply endpoints and a default
namespace, then call `lc_client_open()`. Select local storage with an absolute
`pouch://` endpoint and [typed Pouch settings](pouch-open-settings-api.md).

For an authenticated remote connection, prefer `client_bundle_source` so a
combined PEM bundle can come from a file, memory, descriptor or callback source.
The source remains caller-owned: close it after `lc_client_open()` returns,
whether open succeeds or fails. `client_bundle_path` remains supported. The
returned client is owned and must be closed independently of its sources.

## Allocation tracking

Allocation tracking is **off by default**. To enable it for the process-wide
built-in allocator, call `lc_allocator_tracking_configure(1, &error)` once at
startup, before creating SDK objects or starting concurrent SDK use. Pass `0` to
explicitly keep it off; omitting the call has the same default. Successful
configuration cannot be repeated or changed. Custom allocators never participate
in tracking. Provide malloc/free hooks together; realloc is optional. A full
custom hook set is called directly with the requested sizes and original
pointers. Without a realloc hook, the SDK retains size-only metadata to emulate
resizing through your malloc/free pair.

The setting selects plain libc or tracking callbacks once. Disabled built-in
allocation has no tracking-flag test, header, counter update, lock, or lazy
initialization. When enabled, after closing all handles and cleaning up errors,
ordinary process exit is silent. Unreleased allocations produce a count and byte
total on stderr, for example:

```text
liblockdc allocator: 1 live allocations (37 bytes) at exit
```

Shared-library unload also reports outstanding allocations. This covers the
built-in allocator interfaces, excluding raw facade metadata and allocations
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

## Callback snapshots

For callback-style update flows, `lc_acquire_for_update()` acquires the lease,
captures the complete private snapshot in a temporary file, invokes the handler,
closes the snapshot, and always attempts to release the lease before returning.
The lease and snapshot reader are borrowed and valid only during the callback;
the helper owns the final release. Handler success commits staged changes;
handler failure releases with rollback so partial callback updates are not
published. The callback reads a file-backed snapshot after capture completes.
For direct transfer without snapshot staging, use `lc_acquire()` followed by
`lc_lease_get()` with a sink.

Use the [complete callback example](../examples/acquire_for_update.c) for
request initialization, error handling and cleanup. Queue APIs follow the same
handle model: dequeue or subscribe, operate on the returned `lc_message`, then
acknowledge, negatively acknowledge, or close the local handle explicitly.

## ABI and storage compatibility

Core ABI **9**, introduced in v0.26.0 from ABI **8** in v0.25.0, remains
unchanged through v0.29.0. ABI 9 consumers remain compatible; the PostgreSQL
provider's own ABI remains **0**. When upgrading from ABI 8, rebuild C
consumers, Lua native bindings and the optional PostgreSQL provider together.
That ABI change covers atomic collection edits and recovery heads; see
[the edit contract](collection-edits.md) and
[upgrade instructions](../CHANGELOG.md#upgrading-from-025x).

The dependency ABI is separate: v0.29.0 uses lonejson 0.47.0 with ABI **27**.
Its public receiver layouts changed from ABI 26, so rebuild applications using
those receivers and Lua native modules against the matching SDK. Liblockdc's
mapped load/save boundary uses unchanged lonejson map layouts. See
[the dependency upgrade notes](../CHANGELOG.md#upgrading-from-028x).

Pouch upgrades released collection heads on open under exclusive root access,
preserving positions, retry identities, retention and entry bodies. Close other
clients before the first upgrade and keep older binaries away from the upgraded
root. Shared-library ABI and on-disk format versions are independent. Earlier
source migrations, including the mapped `save()` options argument introduced
with state retention, remain documented in the [release notes](../CHANGELOG.md).

## Runnable examples

These complete programs include configuration, logging, errors and cleanup:

| Example | Demonstrates |
| --- | --- |
| [Client and lease methods](../examples/client_and_lease_methods.c) | Client credentials, lease acquisition and state updates |
| [Acquire for update](../examples/acquire_for_update.c) | Callback-scoped snapshot and commit/rollback |
| [State stream roundtrip](../examples/state_stream_roundtrip.c) | Explicit byte sources and sinks |
| [Collection journal](../examples/collection_journal.c) | Ordered entries, selection and journal operations |
| [Outbox producer](../examples/outbox_producer.c) | Transactional messaging with an independent dispatcher |

The [Lua examples](../examples/lua/README.md) use the shipped `lockdc` facade.
Lua consumers run handlers serially on the calling Lua state; they do not expose
the native threaded C callback model to that VM.
