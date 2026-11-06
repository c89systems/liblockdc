# Lua SDK

`liblockdc` ships a Lua frontend for the public `liblockdc` client surface.
The Lua package namespace is `lockdc`.

This binding is intended to be the Lua-facing `lockd` client used by downstream
components such as `vectis`. `vectis` should consume the Lua modules shipped by
`liblockdc` rather than carrying a separate `lockd` Lua client.

## Distribution model

The Lua binding is distributed as a LuaRocks package that links against an
already-built `liblockdc` SDK installation.

- the Lua rock builds only the Lua extension module
- the C SDK and bundled native dependencies come from the installed
  `liblockdc` release
- the required `liblockdc` version is an exact match for the Lua rock version
- supported runtime architectures are the same ones shipped by the `liblockdc`
  binary SDK bundle

Normal LuaRocks installs are expected to be a two-step flow:

1. install or unpack the matching `liblockdc` SDK release for the target host
2. install the `lockdc` Lua rock against that SDK

The rock build checks the installed `liblockdc` version and fails early if it is
missing or does not match. Normal LuaRocks installs also require a shared
`liblockdc` SDK; static-only SDK bundles are reserved for `vectis` or other
in-tree embedded-Lua builds.

The failure message points at the matching GitHub release tarball URL for the
current rock version and tells the user to set `LOCKDC_PREFIX` to the extracted
SDK root or make `lockdc.pc` visible to `pkg-config`.

GNU/Linux shared SDKs and the Lua extension modules linked against them require
glibc 2.43 or newer. Use a Lua 5.5 runtime compatible with the SDK's target and
libc; repository checks and examples use the project-built Bootlin runner
described below.

The generated rockspec expects:

- package `lockdc`
- `lonejson == 0.46.0-1`
- Lua `>= 5.5, < 5.6`

The lifecycle executes and packages only Lua 5.5. Each release includes the
standalone `liblockdc-lua-<version>.tar.gz` source package, the rendered
`lockdc-<version>-1.rockspec`, and the matching `.src.rock`. The source rock
embeds that exact standalone archive; all three artifacts are checksum-listed
and recursively privacy-scanned before release.
The Lua source payload includes this API guide, the Pouch/outbox retention and
integration guides it links to, and the Lua examples. Its explicit release
manifest excludes investigation notes and generated service state.

For local examples and test modules, use the `LOCKDC_LUA_BIN` value printed by
`make lua-env`. It names the project-built Lua 5.5 runner linked to the selected
Bootlin runtime. LuaRocks remains a packaging tool; it is not used as the
interpreter for a Bootlin-built native module.

## Local runner and search paths

`make lua-env` prints shell-evaluable exports for the project-built Bootlin Lua
runner and the repository's local Lua module paths. It does not install either
Lua rock. After installing `lockdc` and `lonejson` into a local LuaRocks tree,
load that tree's complete paths after the project exports, then run examples
with the Bootlin runner:

```bash
eval "$(make -s lua-env)"
eval "$(luarocks --tree /path/to/lua-tree --lua-version 5.5 path)"
"$LOCKDC_LUA_BIN" examples/lua/acquire_update_json.lua
```

The LuaRocks path export supplies the installed `lonejson` module as well as
`lockdc`. Do not replace this setup with `LD_LIBRARY_PATH` or a host Lua
interpreter: prefix-selected `lockdc` modules carry their SDK runtime path and
the lifecycle checks them through the Bootlin runner.

The C SDK is pinned to the matching `lonejson 0.46.0` native dependency for
mapped state load/save and internal typed JSON parsing. The Lua rock declares
the corresponding Lua-facing `lonejson` rock so Lua JSON behavior and the C
SDK JSON boundary stay in the same release line.

## Dependency ownership

`liblockdc` owns the Lua dependency boundary for the APIs it exposes.

That means:

- `liblockdc` ships the `lockdc` Lua module
- the `lockdc` rock declares the supported `lonejson` Lua rock version
- downstream consumers such as `vectis` should use the same `lonejson` Lua
  dependency declared by `lockdc` instead of bundling a competing copy

This keeps the Lua dependency graph coherent:

- one SDK owner for the public lock client and JSON contract
- one Lua JSON binding version aligned with the public `liblockdc` API surface
- one installation/import path for downstream outbox runtimes

The Lua binding does not expose `pslog` configuration. Native logging is a
`liblockdc` SDK concern; normal Lua rock users get the SDK no-op logger
default. Embedded hosts that need a shared framework logger should create the
`lc_client` in C with their own `pslog_logger *` and wrap that client for Lua
through a host-side integration layer.

The host owns that base logger and its output context. Keep them alive until
the client and all derived handles, consumer services, and dispatchers have
closed. SDK workers may log concurrently from native threads; output callbacks
must support that execution context and must not enter a Lua state owned by
another thread. The consumer callback's logger is borrowed and must not be
destroyed by the handler.

Native allocation tracking is off by default. Call
`lockdc.allocator_tracking_configure(true)` (or `1`) once at process startup,
before creating SDK objects or starting concurrent SDK use, to enable it. Pass
`false` (or `0`) to explicitly keep it off. The setting is shared by all clients
and Lua states using the native library; repeated configuration returns an error.
The native C host can instead call `lc_allocator_tracking_configure()` before
starting Lua. Custom allocators are excluded, and disabled allocation uses the
plain callbacks without a tracking-flag check, header, or counter update.

When enabled, ordinary process exit or native-library unload reports outstanding
counts and bytes on stderr; a clean allocator shutdown is silent. Close SDK
handles and close the Lua state before the host exits. This diagnostic covers
the native allocator, not Lua's own heap, and does not run after abrupt
termination.

The E2E harness enables native tracking before running Lua examples and fails on
native leak diagnostics, including diagnostics captured by package validation.
Dependency heaps require the separate Valgrind lifetime regressions; native
tracking does not replace those checks. Close Lua states and all SDK handles,
and finish SDK calls on other threads, before unloading the native library.

## Public entrypoints

The top-level module is:

```lua
local lockdc = require("lockdc")
```

`lockdc` is the supported package. `lockdc.core` is an internal native module
used to implement it; applications must not require it or depend on its
methods, result shapes, or convenience behavior.

Primary entrypoints:

- `lockdc.open(config)`
- `lockdc.allocator_tracking_configure(enabled)` before creating SDK objects
- `lockdc.version_string()`
- `lockdc.xid_new()`
- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lockdc.json_null`
- `lockdc.OK`, `lockdc.ERR_*`, and `lockdc.NACK_*` C status/intent constants
- `lockdc.pouch_crypto_generate_key()`
- `lockdc.pouch_crypto_default_key_file()`
- `lockdc.pouch_crypto_generate_key_file(path[, overwrite])`

`lockdc.ERR_PENDING` can report incomplete work or temporary contention; it does
not by itself prove a durable transaction decision. For asynchronous operations,
inspect `result().done` and drive the wait/advance contract. For a recorded
transaction decision whose application is pending, preserve its ID and decision
and continue recovery rather than replaying application work. See
[the XA participation contract](xa.md) for PostgreSQL coordination and recovery.

Unsupported collection operations return numeric status `8`
(`LC_ERR_UNSUPPORTED` in C). The Lua facade currently exports `ERR_INVALID`
through `ERR_PENDING`, but has no `lockdc.ERR_UNSUPPORTED` constant. Prefer the
collection capability query when deciding which features to offer.

Primary handle types:

- `Client`
- `Lease`
- `Collection`
- `CollectionWriter`
- `Message`
- `Outbox`
- `OutboxDispatcher`
- `OutboxTransaction`
- `OutboxParticipant`
- `OutboxJob`
- `HistoryConsumer`
- `Service`
- `XACoordinator`, `XATransaction`, and `XAOperation`
- `XAStore`, `XAResource`, `XABranch`, and `XAResourceOperation`
- `XABorrow` for scoped native connection access

## Byte sources, sinks, and explicit materialization

The Lua façade keeps the C `lc_source`/`lc_sink` distinction visible. Methods
that send bytes accept a source string, `{ bytes = ... }`, `{ path = ... }`,
`{ fd = ... }`, or a callback source with `read(max_bytes)`, optional `reset`,
and optional `close`. A callback source returns a byte string, `nil` at EOF,
or `nil, message` on failure.

Methods that receive bytes require a sink. A sink is a path string, file
descriptor number, `{ path = ... }`, `{ fd = ... }`, or a callback table:

```lua
local streamed, written_or_err = job:write_payload({
  write = function(chunk)
    foreign_request:write(chunk) -- receives bounded chunks
    return true                  -- nil/no return is also accepted
  end,
  close = function()
    foreign_request:finish()
  end,
})
assert(streamed == nil)
assert(type(written_or_err) == "number")
```

`write` runs synchronously and must either accept the complete supplied chunk
or return `false, message` / `nil, message` (or raise) to fail the operation.
There are no partial writes. `close` is optional, runs once when liblockdc
releases the sink, and cannot report an operation error. Do not call methods
on the receiver that started a callback-sink operation; that receiver is
deliberately protected against re-entry until streaming returns. In particular,
terminal job methods are unavailable from a job payload sink.

Streaming methods never silently buffer their output. Their first result is
`nil` and their second result reports bytes written or operation metadata. The
matching `read_*` method deliberately materializes bytes into a Lua string;
`read_*_json` additionally decodes that string. Use a `read_*` method only
when the value is deliberately bounded in process memory.

## Client API

Open a Lua client:

```lua
local client, err = lockdc.open({
  endpoints = { "https://localhost:19441" },
  client_bundle_source = { path = "./client.pem" },
  default_namespace = "default",
})
```

`client_bundle_source` accepts the same source-shaped values used by payload
uploads: a PEM string, `{ bytes = pem }`, `{ path = "./client.pem" }`,
`{ fd = fd }`, or a callback source:

```lua
local client, err = lockdc.open({
  endpoints = { "https://localhost:19441" },
  client_bundle_source = {
    read = function(max_bytes)
      return next_pem_chunk(max_bytes) -- return nil for EOF
    end,
  },
})
```

`client_bundle_path` is not a Lua API. Use `client_bundle_source` for every
Lua configuration, including a path-shaped source.

### Local Pouch storage

Use exactly one absolute, option-free `pouch://` endpoint for local storage.
Put all root-open policy in the nested `pouch` table; it includes crypto,
compression, writer, durability, maintenance, queue-watch, and query-index
settings. Presence is significant: `false` and `0` are explicit settings.
Top-level `pouch_crypto_*`/`pouch_compression` fields and Pouch endpoint query
options are not accepted by the Lua façade.

```lua
local client, err = lockdc.open({
  endpoints = { "pouch:///var/lib/my-service/lockd-root" },
  default_namespace = "default",
  pouch = {
    crypto_key_file = "/var/lib/my-service/lockd-root/pouch.key",
    crypto_generate_key_file = true,
    compression = "zlib",
    query_indexing = false,
    query_engine = "scan",
  },
})
```

The supported compression values are `"none"` and `"zlib"`. Omitting
`pouch.compression` inherits the existing root's mode; a new root defaults to
none. Explicitly selecting a different mode converts the entire root before
`open` returns, with warning/info logs at start/completion. Competing shared
writers prevent conversion: open warns and uses the stored mode. Conversion
errors return the normal structured open error. Conversion replaces one
canonical segment or snapshot at a time without duplicating the root. A failed
or interrupted conversion resumes before the next open returns, even when
`pouch.compression` is omitted. See
[compression changes on open](pouch-compression.md) for recovery and disk-space
requirements. Pouch remains
exclusive single-writer by default; opening another writer for the same root
returns the normal structured `lockdc.open` error. Set
`pouch = { single_writer = false }` where shared writers are explicitly
required.

Opening a Pouch root also upgrades released collection control metadata before
returning the client. First upgrade requires all other clients to be closed,
even for shared mode; contention returns status `LC_ERR_PENDING` and an error
table with `server_code = "collection_upgrade_busy"`. Entry bodies are not
rewritten. The upgrade is resumable after interruption and later opens skip
the migration walk. See [collection storage upgrade](collection-leases.md#storage-upgrade).

Typed Pouch settings are local-only: supplying a non-empty `pouch` table to a
remote or Unix-socket client returns the normal structured open error. See
[typed Pouch open settings](pouch-open-settings-api.md) for every key and its
precedence rule.

The explicit Pouch crypto helpers mirror the safe C utility workflows:
`lockdc.pouch_crypto_generate_key()` returns a new key string,
`lockdc.pouch_crypto_default_key_file()` returns the platform default path, and
`lockdc.pouch_crypto_generate_key_file(path[, overwrite])` creates a mode-0600
key file and returns its generated key. Treat returned keys as secrets and use
the nested `pouch.crypto_key_file` setting for long-lived process configuration.

Common client methods:

- `client:info()`
- `client:close()`
- `client:acquire(req)`
- `client:acquire_for_update(req, handler)`
- `client:describe(req)`
- `client:get(req, sink)` and `client:read(req)` / `client:read_json(req)`
  (`req.namespace` selects an explicit namespace; omitted uses the client
  default)
- `client:update(req, body)`
- `client:update_json(req, value)`
- `client:mutate(req)`
- `client:metadata(req)`
- `client:remove(req)`
- `client:keepalive(req)`
- `client:release(req)`
- `client:attach(req, body)`
- `client:list_attachments(req)`
- `client:get_attachment(req, sink)` and `client:read_attachment(req)`
- `client:delete_attachment(req)`
- `client:delete_all_attachments(req)`
- `client:queue_stats(req)`
- `client:enqueue(req, body)`
- `client:dequeue(req)`
- `client:dequeue_batch(req)`
- `client:dequeue_with_state(req)`
- `client:queue_ack(message_or_req)`
- `client:queue_nack(req)`
- `client:queue_extend(req)`
- `client:query(req, sink)` and `client:read_query(req)` (`req.engine` and
  `req.refresh` may select the query engine/refresh mode; document-query
  trailer metadata is returned as `metadata_json`)
- `client:query_keys(req, handler)` streams decoded key bytes to `handler`
- `client:get_namespace_config(req)`
- `client:update_namespace_config(req)`
- `client:flush_index(req)`
- `client:txn_replay(req)`
- `client:txn_prepare(req)`
- `client:txn_commit(req)`
- `client:txn_rollback(req)`
- `client:tc_lease_acquire(req)`
- `client:tc_lease_renew(req)`
- `client:tc_lease_release(req)`
- `client:tc_leader()`
- `client:tc_cluster_announce(req)`
- `client:tc_cluster_leave()`
- `client:tc_cluster_list()`
- `client:tc_rm_register(req)`
- `client:tc_rm_unregister(req)`
- `client:tc_rm_list()`
- `client:new_outbox(config)`
- `client:new_history_consumer(config)`
- `client:create_collection(config)`, `client:open_collection(config)`
- `client:list_collections(options, visitor)`, `client:collection_capabilities()`
- `client:subscribe(req, handler)` and `client:subscribe_with_state(req, handler)`
- `client:watch_queue(req, handler)`
- `client:new_consumer_service(config)`

## Ordered collections

Collections are isolated, ordered journals of state objects with immutable JSON
bodies. Pouch implements them locally; remote lockd currently reports
numeric status `8` (`LC_ERR_UNSUPPORTED` in C). The
[collection contract](collections.md) also defines the
future server interface. Collections do not enlist in XA transactions.

`client:create_collection({namespace=..., name=..., retention_seconds=..., max_entries="1000"})`
returns a new bound receiver; an existing name is a conflict.
`client:open_collection({namespace=..., name=...})` opens without creating or
changing retention. Omit `namespace` to select the client default. Creation
assigns a collection ID; drop/recreation invalidates old receivers and cursors.
`client:list_collections({namespace=...}, visitor)` visits descriptions in name
order without entry bodies. The visitor accepts a description by returning
`true` or nothing. `client:collection_capabilities()` reports supported collection
operations. Pouch currently enables `collections`, `lql_filter`, `resume`,
`checkpoint`, `ranges`, `first_n`, `count_retention`, `append_writer`,
`append_staged`, `append_collection_retention`, `leases`, `lease_takeover`, and
`private_reads`, and `atomic_edit`; `last_n` and `position_selection` are false.
A remote client's capability query returns all false without a network request;
other collection operations are unsupported. Unknown selection options are
rejected rather than ignored.

| Collection method | Result |
| --- | --- |
| `acquire({owner=..., ttl_seconds=30, private_read=false})` | Another owned collection receiver bound to fresh lease credentials |
| `takeover(request, observed_lease_id)` | Owned receiver replacing exactly the observed live lease |
| `keepalive({ttl_seconds=30})` | Refreshed ownership metadata, without changing retention |
| `release()` | True; releases ownership and closes this receiver on success |
| `head()` | Coherent identity, opaque ETag, counts and bounded recovery descriptor/anchor availability |
| `edit(request, source?)` | Atomic range cut, optional streamed replacement and recovery descriptor; original result on an identical retry |
| `edit_status(operation_id)` | `found` and the original committed `result`; absence does not cancel an in-flight edit |
| `describe()` | Namespace, name, collection ID, upper position, live count, indexing, retention and lease metadata |
| `metadata({retention_seconds=..., max_entries="1000"})` | Updated collection description |
| `append({append_id=..., retention_seconds=..., collection_retention_seconds=...}, source)` | Position, ETag, bytes, duplicate/deleted and retention facts |
| `append_staged(request, source)` | Same request, sources and result as append, using the existing writer's bounded canonical chunks and commit path |
| `append_begin(request)` | Owned raw-JSON writer; bounded chunks go into canonical segments and commit publishes metadata |
| `append_status(append_id)` | `state="absent"`, `"committed"`, or `"deleted"`; assigned identities include an `entry` table |
| `entry_metadata(position, {retention_seconds=...})` | Entry facts after a body-preserving retention change |
| `read(options, sink)` | `nil, result`; on failure `nil, error, code, partial_result` |
| `delete_entry(position)` | Position, `deleted`, and `already_deleted` |
| `drop()` | `true` after logical removal; physical cleanup follows maintenance |
| `close()` | Release only this local receiver; repeat close is harmless |

Atomic [edits](collection-edits.md) require `operation_id` and `expected_head`
from `head().etag`. Ranges are sorted disjoint `{after="...", through="..."}`
intervals (at most 64). Optional `replacement` contains ordinary append options;
pass its raw JSON source as the second argument. Its append ID must be unused.
Optional `recovery` is at most 1024 UTF-8 bytes: omit to preserve, `""` to clear.
A selected `recovery_anchor="replacement"` binds the newly assigned position;
otherwise use a live decimal position or omit it for no anchor. The marker does
not retain that entry. `head().recovery_anchor_available` becomes false after
entry removal. `read({expected_head=token}, sink)` rejects a changed head before
any output. A stale edit returns `error.current_head` with the observed token
and counts. Errors after publication carry `uncertain=true` and the original
`result`; reconcile them through `edit_status` or an identical retry. Do not automatically rebase a changed request under its old ID.

```lua
local observed = assert(collection:head())
local edited = assert(collection:edit({
  operation_id = "trim-command-42", expected_head = observed.etag,
  ranges = {{after="100", through=observed.upper_position}},
  replacement = {append_id="baseline-42"},
  recovery = '{"replay":"baseline"}', recovery_anchor="replacement",
}, source))
-- Persist the request/operation ID before attempting it. After an uncertain
-- error, inspect edit_status or retry the identical request and compact bytes.
```

All uint64 positions, byte counts and record counts are canonical decimal
strings, including small values. Position inputs use those strings too; Lua
numbers are rejected. Retention uses the ordinary state option convention:
omitted/`nil` preserves an existing deadline, zero clears it, and positive
integer seconds replace it from commit. New objects default to no expiry.
Observations return `retention_support` and, when known,
`retention_expires_at_unix`, including zero for no expiry.

All append forms accept optional `collection_retention_seconds`: omission
preserves the parent deadline, zero clears it, and positive integer seconds
reset it from the new append's commit timestamp. Only new membership renews;
identical retries, reads and failures before publication do not. Results/status
include `collection_retention_expires_at_unix` when the original append selected
the directive, even if the parent has since changed. Use `describe()` for its
current deadline. There is no default inactivity duration in liblockdc.

An append writer accepts `writer:write(raw_json_string)` repeatedly, then
`writer:commit()` or `writer:abort()`, and always `writer:close()`. `write` and
`abort` return true on success; `commit` returns the normal append result.
Failures return `nil, error, code`. Empty writes are not EOF. Strings are
borrowed for each call; the binding does not decode JSON or introduce a producer
thread. Native Pouch writes bounded chunks directly into shared canonical
segments. Commit publishes metadata without copying the payload. No publication
lock is held between calls; live uploads defer compaction capture.
Writers retain their owners independently of public collection/client close.
Commit is terminal, abort is idempotent, and close/GC never publishes. After
an uncertain commit error, use `append_status`, including when constructing
the Lua result failed after native publication. See
[incremental appends](collection-writer.md) for lifecycle and storage costs.

`collection:append_staged(request, source)` consumes a source through that same
writer and closes its internal writer on every outcome. It does not hold
publication locks while invoking the source's read callback. Callback reentry,
owner closure, retry identity and retention rules match ordinary append; the Lua
source adapter is closed as usual and is never automatically reset or replayed.
Source failure/cancellation publishes nothing; a commit or result-allocation
error can have a committed outcome. Fast inputs may cost less with direct
`append`; staging is an explicit caller choice.

Collection retention expires the collection itself with all remaining entries.
Entry retention expires only that state. An empty collection remains until its
own expiry or an explicit drop. Appends without an explicit parent directive
preserve parent expiry; retries preserve both policies.

Optional collection `max_entries` limits live states by permanent position:
`"1000"` keeps the newest 1,000, `"0"` disables the limit, and omission preserves
an existing setting (new collections default to unlimited). Creation and
collection `metadata` accept it; opening and entry metadata reject it.
`describe` and list callbacks return the configured limit as a decimal string.
Age expiry is independent. Maintenance removes due entries first, then trims
oldest surplus states up to the surplus captured when the visit started. Until
that pass, reads and appends may observe more entries than the limit. Disabling
background compaction disables automatic trimming too. This is a live-state
limit, not a disk-space or retry-tombstone bound; increasing it cannot restore
deleted states. See [count retention](collections.md#count-retention).
Expiry permits later maintenance deletion; reads and metadata changes continue
normally until that deletion occurs. Metadata changes preserve the body, ETag,
append identity, and position. Retrying a deleted append ID cannot resurrect it.

`source` accepts the normal bytes/file/FD/callback adapters. Callback and file
sources supply bounded chunks. `read` requires a sink and forwards original
compact JSON objects with LF; it has no buffered read convenience or JSON
conversion step. Its options are `selector_lql`, `cursor`, `after`, `through`,
`first_n`, `on_metadata`, and `cancelled`. Bounds and counts are canonical uint64
decimal strings, including on 32-bit Lua builds. `after` is exclusive and
`through` inclusive; omitted bounds use zero and captured EOD. Equal bounds or
a start beyond current EOD select nothing; reversed explicit bounds fail.
`first_n` selects live inputs before LQL filtering; filtered-out inputs consume
N and deleted entries do not. A window may therefore emit fewer than N states,
including none. Omission is unlimited, while `"0"` completes an empty window
without advancing its checkpoint. Last-N,
projection and position-list options remain unavailable.

```lua
local journal = assert(client:create_collection({ name = "events" }))
local event = assert(journal:append({ append_id = event_id }, event_source))
local _, result, code, partial = journal:read({
  selector_lql = "eq{field=/kind,value=message}",
  after = "0",
  first_n = "100",
  on_metadata = function(meta)
    if meta.kind == "entry" then
      -- This entry's raw JSON and LF have already reached event_sink.
      accept_entry(meta.position, meta.resume_cursor)
    end
    return true
  end,
}, event_sink)
assert(code == nil, result and result.message)
assert(result.complete)
save_checkpoint(result.checkpoint_cursor)
journal:close()
```

Entry metadata includes `kind="entry"`, `position`, `append_id`, `etag`, `bytes`,
retention, `last_examined_position`, and `resume_cursor`. Progress frames have
`kind="progress"` and only the examined boundary/cursor, with no payload.
Metadata stays outside the JSON sink. Callback success is `true` or no return;
false/error rejects that boundary and preserves the preceding accepted cursor.
Cancellation returns success with `complete=false`, `stop_reason="cancelled"`,
and safe progress. A raised cancellation callback is an error.

Incomplete results carry `resume_cursor`; complete results carry
`checkpoint_cursor` for a new selected window after the prior window's last
safely examined position. Reaching first-N completes that selection and its
checkpoint starts a fresh N-entry budget without skipping remaining entries.
A resume cursor retains the unconsumed budget and captured upper boundary.
Repeat the same selector, bounds and count with either cursor purpose; changing
selection is rejected. Discard any incomplete final object from the sink
before resume. Store cursors only after accepting the corresponding output
durably. Sink acceptance followed by callback rejection may repeat that entry.
Closing a receiver or client inside a callback defers native cleanup until the
call unwinds; re-entry on the same active collection returns a status error.

## Pouch durable-history consumers

`client:new_history_consumer(config)` registers or reopens a durable Pouch
retention cursor. It is a compaction-safety boundary, not a history-query API:
the application remains responsible for obtaining and applying records through
its own replication, backup, or resume protocol. Remote lockd clients reject
this Pouch-only operation.

`config.namespace` and `config.consumer_id` form the
stable durable identity. A new identity begins at
`initial_acknowledged_index_seq` (zero by default), or set
`start_at_current = true` to retain only future history. Existing identities
always keep their recorded acknowledgement; the supplied initial position is
ignored. `start_at_current` and `initial_acknowledged_index_seq` are mutually
exclusive.

The returned receiver provides:

- `history:position()` → `{ acknowledged_index_seq, current_index_seq }`
- `history:advance(acknowledged_index_seq)` → the updated position; it may
  only move forward and cannot exceed the current sequence.
- `history:unregister()` → removes the durable retention pin.
- `history:close()` → releases only the local handle; it deliberately leaves
  the durable pin in place.

Use a stable consumer ID and register it before producing history that must be
retained. A slow cursor can retain more on-disk history, but normal Pouch open,
read, query, and mutation paths never enumerate consumer records.

## Inbox/outboxes

`client:new_outbox(config)` creates a threadless inbox/outbox producer. Its
`namespace` contains both durable inbox and outbox keys.
It never starts a dispatcher, claims a job, performs recovery, or invokes a
foreign effect. Use `outbox:dispatcher()` only in the dedicated worker or
service domain that owns delivery. The complete lifecycle and host-integration
contract is in [the outbox dispatch architecture](outbox-dispatch-architecture.md).

Outbox support is currently Pouch-only. Remote lockd outboxes are unsupported
because of [defects in the remote lockd server](inbox-outbox.md#remote-lockd):
implicit-XA enrollment and public-state isolation.

```lua
-- Request/producer domain: this is safe to construct without creating a
-- background dispatcher.
local outbox = assert(client:new_outbox({
  namespace = "orders-outbox",
  owner = "orders-api",
  max_attempts = 100,
  recovery_interval_seconds = 5,
}))

local txn = assert(outbox:append({
  operation_id = order_id,
  effect_id = "charge-card",
  effect_key = "charge:" .. order_id,
  payload_digest = payload_digest,
  kind = "http",
  destination = "https://payments.example/charges",
  headers_json = lockdc.encode_json({ ["idempotency-key"] = "charge:" .. order_id }),
}, payload_source))

local order = assert(txn:acquire({ namespace = "orders", key = order_id }))
assert(order:update_json({ status = "payment_pending" }))
order:close()
local commit = assert(txn:commit())
txn:close()

-- `commit.outbox_receipts` exists only after the durable commit. A host with a
-- bounded worker/supervisor wake channel may copy those keys now, never before.

-- worker.lua, in a dedicated worker/service process. This opens its own client
-- and outbox, then returns the one compatible local dispatcher or starts it
-- lazily. It does not run Lua on its private C thread.
-- For a Pouch root shared with the producer process, both client configurations
-- must explicitly select the supported `single_writer=false` shared-root mode.
local worker_client = assert(lockdc.open(worker_client_config))
local worker_outbox = assert(worker_client:new_outbox({
  namespace = "orders-outbox",
  owner = "orders-api",
  max_attempts = 100,
  recovery_interval_seconds = 5,
}))
local dispatcher = assert(worker_outbox:dispatcher())
assert(dispatcher:run({
  handlers = {
    http = function(job)
      assert(job:write_payload(foreign_request_body_sink))
      return job:complete()
    end,
  },
}))
```

`headers_json` is the durable serialized header envelope. Build it explicitly
with `lockdc.encode_json` when the headers originate as a Lua table.
The example selects a positive recovery cadence for shared-root delivery:
Pouch defaults to no routine recovery scans, so a completely lost cross-process
wake otherwise needs explicit reconciliation. Both processes use identical
outbox settings, including all retention policies.
`outbox:accept_inbox(message)`
returns `nil, result` on an accepted duplicate, where `result.duplicate` is
true. Otherwise it returns a `OutboxTransaction` and `result.accepted` is
true.

Outbox `payload_digest` is required immutable metadata binding the supplied
payload bytes. It is host-generated and opaque to liblockdc: use the same value
for an identical retry and a different value for changed bytes. This preserves
one-pass streaming, because the binding does not require the façade to consume
or buffer the payload before it stages the attachment.

`max_attempts` must fit the C API's signed 32-bit integer range; values outside
that range are rejected instead of being narrowed or defaulted.

Pouch outboxes automatically retain a dead letter for 30 days by default and
also cap retained dead letters at 1,024 envelopes or 64 MiB per namespace.
Set `dead_letter_retention_seconds` to a positive override or `-1` to disable
automatic retention. `dead_letter_max_count` and `dead_letter_max_bytes` set
the two capacity limits; use `false` for either Lua field to disable that one
capacity bound. This policy is local-Pouch-only—an explicit retention setting
on a remote lockd outbox is rejected rather than silently ignored. Automatic
reclamation requires a live dispatcher and is exact-key maintenance, never a
query or a side effect of `dispatcher:next()`. See
[Pouch dead-letter retention](pouch-dead-letter-retention.md).

Completed outbox envelopes, accepted inbox receipts, and completed/failed
command receipts each default to 30 days independently. Configure
`completed_retention_seconds`, `inbox_retention_seconds`, and
`command_retention_seconds`: zero selects the default, positive values select
seconds, and `-1` disables that type's reclamation, including its count cap.
`completed_max_count`, `inbox_max_count`, and `command_max_count` are optional
caps; zero, omission, or `false` leaves them unbounded. A positive cap permits
early reclamation. Reclamation ends duplicate/idempotency protection and command
status lookup; owned command results are removed with the receipt. Pending
work is excluded. These settings are Pouch-only and require a live dispatcher;
see [Pouch terminal receipt retention](pouch-terminal-retention.md).

Numeric retention counts must be non-negative integers representable by the C
`size_t` and Lua integer types; `true`, negative and fractional values are
rejected. Durations must fit a C `long`. Time-policy changes affect future
terminal transitions only, and records created before retention or while a type
was disabled are not backfilled. Receipt reads, `wait_command()`, and
`resume_command_by_id()` return their normal missing-record errors after
cleanup; they never recreate the receipt.

Outbox receivers are explicit and owned. Parent acceptance and append
operations return a new `OutboxTransaction` only when they created fresh
durable work:

- `outbox:append(entry, payload)` returns `txn, nil` for a fresh
  append. Its receipt appears only in the successful `txn:commit()` result. A
  matching committed effect returns `nil, receipt` with `receipt.duplicate`.
- `outbox:accept_inbox(message)` returns `txn, result`; a matching source
  message returns `nil, result` with `result.duplicate`.
- `outbox:accept_command(request)` returns `txn, receipt`; a matching
  command returns `nil, receipt` with `receipt.duplicate`. Use
  `outbox:get_command_receipt(identity)` for a direct durable status read,
  `outbox:write_command_result(identity, sink)` to stream a completed result
  body, `outbox:read_command_result(identity)` to materialize it, and
  `outbox:resume_command(identity)` to obtain a transaction for a pending
  command. A terminal command resumes as `nil, receipt`.
  `outbox:get_command_receipt_by_id(command_id)` is suitable for an
  authenticated status resource. `outbox:wait_command(command_id, timeout_ms)`
  only waits for that durable receipt; it never runs a dispatcher handler or
  starts a worker. Use `0` for one read, `-1` for no deadline, or a positive
  monotonic deadline that also bounds each remote receipt read. On timeout it
  returns `receipt, error`, where receipt
  remains pending and `error.code` is `ERR_TIMEOUT`. Set
  `generate_idempotency_key = true` and omit
  `idempotency_key` in `accept_command` only when returning the generated key
  or status reference to the caller.
  Invalid or unknown command IDs use the normal Lua failure result
  `nil, error, code`; only a timeout has a pending receipt to return.
- `outbox:transaction(fn)` provides a transaction proxy with a stable reserved
  global ID; its first
  participant may be `acquire`, `append`, `accept_inbox`, or
  `accept_command`; it commits on normal callback return and rolls back on an
  error or any failed staged operation, even when the callback elects to
  inspect and return normally from that structured error. It starts no durable
  marker by itself, so an empty transaction is invalid. Its successful result
  includes `outbox_receipts`, so only that result contains fresh keys safe to
  forward. A pre-existing duplicate found after a domain participant is staged
  makes the transaction rollback-only, so no domain change can commit without
  its outbox/idempotency boundary. A duplicate first record returns its durable
  receipt even when the callback does not return it; a duplicate before a
  domain participant is enrolled may still allow an independently fresh
  outbox receipt to commit.
- `outbox:begin()` returns the same transaction receiver for advanced
  code that needs explicit commit/rollback control. Its first participant has
  the same domain-acquire/command/inbox/outbox choices as `transaction(fn)`.
- `outbox:dispatcher()` acquires the compatible local dispatcher. It has no
  configuration argument because dispatch policy comes from the outbox's
  canonical configuration.
- `client:new_outbox(config, { dispatcher = dispatcher })` creates another
  threadless producer attached to an already-compatible dispatcher. It is the
  local fast wake path only: a committed receipt remains the durable source of
  truth and may still be forwarded to a supervisor.
- `dispatcher:next(timeout_ms)` returns a claimed job for advanced pull-based
  consumers and claims only on that demand. `dispatcher:next_with_state(timeout_ms)`
  is the explicit Pouch-only stateful variant: it returns the same job shape
  with `job:state()` available as an ordinary lease facade. It does not fall
  back to stateless delivery when the selected backend cannot provide a paired
  checkpoint claim. `dispatcher:next(0)` is strictly an in-memory, non-query
  probe; a blocking call may request durable recovery. A stateful pull may
  reclaim one exact completed-checkpoint marker before its probe; it never
  scans or queries the namespace for that housekeeping.
  `dispatcher:run({handlers = ..., with_state = true})` is the blocking
  dedicated worker loop; bounded `dispatcher:pump({ ..., with_state = true })`
  is for hosts that own their event loop. `with_state` has the same paired Pouch
  claim semantics as `next_with_state`; omitting it preserves the stateless
  path. `pump(options)`
  is for hosts that own their event loop;
  its `timeout_ms` is non-negative. Like `next(timeout_ms)`, it bounds only
  this pull attempt; it is independent of the remote request
  `shutdown_timeout_ms` configured on the outbox.
  The first consumption choice binds a dispatcher to either raw pull or its
  caller Lua state's handler table; the other mode, another handler table, or
  another Lua state is rejected. A handler cannot recursively call `pump()` or
  `run()` on that dispatcher (including through an alias), nor call
  `stop()` or `wait()` while it owns the active job. It must return its terminal
  outcome before another job is consumed. Neither should run foreign effects in
  an HTTP route.
- `dispatcher:notify_outbox_key(outbox_key)` accepts a key from a successful
  commit receipt as a bounded, process-local latency hint. It does not query
  the durable namespace; duplicate, unavailable, and overflowed hints are
  repaired by ordinary durable reconciliation.
- `dispatcher:stats()` returns process-local counters, including
  `terminal_reclaims` (retired markers, including stale ones) and
  `terminal_reclaim_failures` (cleanup failures preserved for retry);
  `dispatcher:reconcile()` requests durable recovery.
- `dispatcher:replay_dead_letter(outbox_key)` returns one dead-lettered effect
  to pending; `dispatcher:delete_dead_letter(outbox_key)` permanently deletes
  it and its payload. `dispatcher:export_dead_letters(options, sink)` exports
  envelopes as `"json"` or `"jsonl"` to a required sink;
  `dispatcher:read_dead_letters(options)` is the explicit materializer.
- `outbox:close()` releases only the producer. `dispatcher:stop()` and
  `dispatcher:wait()` control the explicitly acquired dispatcher;
  `dispatcher:close()` releases a handle and does not stop shared work.
  Stop/wait accept omitted or `-1` for an indefinite wait, `0` for a poll, or
  positive milliseconds for a bounded wait. A timeout leaves it stopping;
  `wait()` before stop is invalid. Finish or close jobs already returned to Lua
  before waiting indefinitely: claim expiry alone does not close their local
  handles. All stop/wait calls, including polls, are rejected inside a dispatcher
  handler.
- `client:close()` stops and joins private dispatcher workers but does not wait
  for a job already returned to Lua. That job remains usable for its terminal
  decision or close; stopped dispatcher wrappers reject new work and remain
  closeable.

Transactions provide `acquire`, `append`, `accept_command`, `accept_inbox`,
`complete_command`, `fail_command`, `commit`, `rollback`, and `close`.
`accept_command` permits at most one command receipt per transaction.
`txn:commit()` returns `{ outbox_receipts = { ... } }` only after a durable
commit; it returns no fresh outbox key on failure or rollback.
`txn:append()` may return an existing duplicate receipt, which is
already safe to forward; fresh appended keys remain commit-published.
Participants provide `info`, `describe`, `get`, `read`, `read_json`, `update`,
`update_json`, `mutate`, `mutate_local`, `metadata`, `remove`, `keepalive`, and
the full attachment surface. Jobs provide `info`, `write_payload`,
`read_payload`, `read_payload_json`, `state`, `renew`, `complete`, `retry`,
`dead_letter`, and `close`. `write_payload` requires a sink, including a
callback sink; `read_payload_json` deliberately materializes and decodes the
complete body, so use it only when that size is deliberately bounded.

Participants deliberately have no release or terminal-decision method. Closing
or deciding a transaction invalidates its participants, so close each view when
finished. A dispatcher job is the only handoff to host effect execution; keep
its claim alive with `job:renew()` for longer foreign operations, then select
exactly one terminal operation. A successful `complete`, `retry`, or
`dead_letter` consumes the job. If a terminal operation returns an error, the
job remains active and the same terminal operation may be retried until its
claim expires. Use `job:close()` only when abandoning a non-terminal local
handle; it does not retry or complete the durable job.

`job:state()` returns `nil` for a normal stateless pull. For a stateful pull it
returns an ordinary Lua lease facade over the job's paired checkpoint. Use its
normal streaming, JSON, metadata, mutation, and attachment methods. It is a
borrowed view: do not close it. The job retains the authoritative claim, and
after a successful job terminal operation or `job:close()` a previously
returned checkpoint wrapper is invalid. Checkpoints survive `retry`, expiry
recovery, and dead-letter replay: every successful checkpoint or attachment
mutation commits under the paired lease before the Lua method returns, rather
than remaining as a claim-local stage that expiry can discard. The parent
remembers that a stateful checkpoint may exist, so a later ordinary
`dispatcher:next()` claimant also reclaims the checkpoint and attachments on
completion. `state:remove()` removes its body but completion still retires any
paired attachments. If a process stops after the parent terminal record is
durable but before that cleanup, a later stateful pull reclaims the one durably
named checkpoint before claiming work. Completion has already succeeded in
that case: cleanup is deferred to the durable marker rather than risking a
redelivery that lacks its checkpoint. If the terminal decision was not yet
durable, normal redelivery keeps the checkpoint instead. A checkpoint captured
by a handler is also invalid after
that handler returns without a valid terminal outcome: its automatic retry
consumes the native job. While a checkpoint `get()` or attachment read is
streaming into a sink, `job:close()` and job terminal methods are rejected;
the sink must return before the job can release its paired lease. Stateless
pulls remain unaffected. The checkpoint is query-hidden and its key is
deliberately not a Lua configuration or lookup surface.

Inside a `dispatcher:run()` or `dispatcher:pump()` handler,
`job:complete()`, `job:retry(options)`, and `job:dead_letter(diagnostic)`
construct an outcome that the façade applies after the handler returns. The
same methods on a job returned by raw `dispatcher:next()` perform the direct
terminal operation. `retry({ delay_seconds = n, diagnostic = message })`
durably reschedules the job within the configured retry bounds. `retry()` takes
no argument or that options table; a diagnostic string by itself is invalid.

## Raw XA and transaction-coordinator APIs

The outbox API is the normal Lua transactional-outbox path. It creates and
recovers durable decisions without requiring the application to manage an XID.
For a coordinator, resource manager, or recovery tool that must operate at the
same level as the C API, the raw XA surface is also available on `client`.

Use `lockdc.xid_new()` to mint a valid transaction identifier for raw XA work.

`txn_prepare`, `txn_commit`, and `txn_rollback` accept a table with `txn_id`,
`participants`, optional `expires_at_unix`, optional `tc_term`, and optional
`target_backend_hash`. Each participant has `namespace`, `key`, and an
optional `backend_hash`. `txn_replay` accepts `{ txn_id = ... }`. Decision and
replay results include `txn_id`, `state`, and `correlation_id`.
When C supplies a result while returning an error such as `ERR_PENDING`, Lua
returns `nil, error, code, result`. That fourth value preserves the recorded
decision; pending participant application does not erase it or imply rollback.
An expired raw prepare can become rollback on replay. Managed undecided work
requires fenced coordinator recovery; raw replay does not take over its owner.

```lua
local txn_id = assert(lockdc.xid_new())
local participant = { namespace = "orders", key = "order-42" }

assert(client:txn_prepare({
  txn_id = txn_id,
  participants = { participant },
  expires_at_unix = 2147483647,
  tc_term = 1,
}))
assert(client:txn_commit({
  txn_id = txn_id,
  participants = { participant },
  tc_term = 1,
}))
```

The `tc_lease_*`, `tc_leader`, `tc_cluster_*`, and `tc_rm_*` methods map their
C requests and results directly to Lua tables. Terms must fit Lua's
non-negative integer range. Result lists are ordinary dense Lua arrays in
`endpoints` or `backends`.

For Pouch, `tc_leader()` retains the last fencing `term` when `leader_id` is
empty. Replacing an expired or released leader requires a strictly newer term;
only the live current owner can reacquire its same term. Release and client
restart do not reset this high-water mark. Check the Lua integer limit before
advancing a term.

`client:query_keys(req, handler)` has the same query request fields as
`client:query`, but sends each decoded key directly to Lua instead of materializing
the query response. Selectors are optional, so `engine = "scan"` with a cursor
can enumerate every key in a namespace. Pass either a function for key chunks
or a table with a required `chunk(bytes)` function and optional `begin()` and
`finish()` hooks.
A key can arrive in multiple chunks, including inside a UTF-8 sequence; collect
only the current key between `begin` and `finish` if complete strings are needed.
An empty result, including a server response that omits `keys`, succeeds without
invoking the key handlers.

`client:acquire_for_update(req, handler)` wraps the common acquire, snapshot,
update, release workflow. C captures the complete private snapshot in a temporary
file before invoking the binding, which then materializes it into the Lua
`state` string. `load_json()` additionally decodes that string into Lua values.
Use `client:acquire()` followed by `lease:get(opts, sink)` for direct bounded
transfer without capturing a whole snapshot. The handler receives a context
table with:

- `lease`
- `state`
- `state_meta`
- `load_json()`
- `update(body, req)`
- `update_json(value, req)`
- `mutate(req)`
- `mutate_local(req)`
- `metadata(req)`
- `remove(req)`
- `keepalive(req)`

The helper always attempts to release the lease after the handler returns.
Handler success commits staged changes; handler failure releases with rollback
so partial callback updates are not published. Return `nil` for success or
forward the complete return values of an SDK call, as below. To fail and roll
back, return `nil, error`, a string or `false`, or raise an error. An error table
belongs in the second return value.

`af.lease` is a scoped lease: it is valid only while this handler is running.
Do not retain it, pass it to another coroutine, or use it after the handler
returns. This preserves Pouch's single staged-update boundary.

```lua
local ok, err = client:acquire_for_update({
  key = "orders/42",
  owner = "worker-1",
  ttl_seconds = 60,
}, function(af)
  local state, meta = af:load_json()
  if state == nil and (meta == nil or not meta.no_content) then
    return "failed to load state"
  end
  state = state or {}
  state.status = "processing"
  return af:update_json(state)
end)
```

## Lease API

State updates, mutations, local mutations, and metadata updates accept an
optional integer `retention_seconds`, through client, lease, outbox participant,
and managed XA lease APIs. Omission or `nil` preserves the existing policy;
zero removes expiry; positive seconds replace it with commit time plus the
duration. New objects default to no expiry. Metadata may specify retention alone,
but requires an existing body or staged replacement. Booleans, fractions,
negative values, strings, and values outside a C `long` return
`nil, error, ERR_INVALID` before mutation.

```lua
assert(lease:update_json({ status = "complete" }, {
  retention_seconds = 30 * 24 * 60 * 60,
}))
assert(lease:metadata({ retention_seconds = 0 })) -- Keep indefinitely.
```

State results, lease/participant `info()`, and callback `state_meta` expose
`retention_support` (`"unknown"`, `"supported"`, or `"unsupported"`). A known
deadline appears as `retention_expires_at_unix`; zero confirms no expiry.
Pending positive durations have no final deadline until commit. Public reads
report committed policy and leave the receiver's private observation unchanged.

Pouch retires a fully applied single-key implicit retention decision on lease
release, so successful renewals do not accumulate startup recovery work. This
also covers metadata-only renewals and rollback. Cleanup can fail after the
decision has applied: a release error is not evidence of rollback. Explicit,
joined and managed/external XA retain their normal outcome rules.

Expiry makes an object eligible for cleanup; it remains ordinary readable and
writable state until maintenance deletes it. Pouch uses its existing compaction
worker, normally on a 30-minute cadence. Setting `pouch.background_compaction=false`
disables automatic state and collection expiry, without affecting their stored
deadlines. This root worker does not require an outbox dispatcher. Remote lockd
must implement cleanup itself. Older servers remain usable when retention is
omitted; an explicit request may be rejected or ignored. Success alone does not
confirm support, so inspect the returned observation. This policy is separate from
outbox and dead-letter retention. See [state retention](state-retention.md).

Returned by `client:acquire(...)` and by `message:state()`. The latter is an
independently closable lease view copied from the message's borrowed state
lease; it does not extend the delivery's server-side lease lifetime. Complete
the message's `ack()` or `nack()` decision only after state work has finished.

Common lease methods:

- `lease:info()`
- `lease:close()`
- `lease:describe()`
- `lease:get(req, sink)` and `lease:read(req)` / `lease:read_json(req)`
- `lease:update(body, req)`
- `lease:update_json(value, req)`
- `lease:mutate(req)`
- `lease:mutate_local(req)`
- `lease:metadata(req)`
- `lease:remove(req)`
- `lease:keepalive(req)`
- `lease:release(req)`
- `lease:attach(req, body)`
- `lease:list_attachments()`
- `lease:get_attachment(req, sink)` and `lease:read_attachment(req)`
- `lease:delete_attachment(selector)`
- `lease:delete_all_attachments()`

## Message API

Returned by dequeue operations.

Common message methods:

- `message:info()`
- `message:is_open()`
- `message:close()`
- `message:ack()`
- `message:nack(req)`
- `message:extend(req)`
- `message:state()`
- `message:rewind_payload()`
- `message:write_payload(sink)`
- `message:read_payload()` / `message:read_payload_json()`

## JSON helpers

The binding uses `lonejson` internally for JSON encode/decode helpers.

Use:

- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lease:read_json()`
- `lease:update_json(value, req)`
- `message:read_payload_json()`

These helpers are for idiomatic Lua outbox code. They do not replace the
mapped `lonejson` APIs in the C SDK; those C APIs use caller-defined
`LONEJSON_FIELD_*` maps for typed state load/save. The Lua helpers sit on top
of the public `liblockdc` JSON transport surface and use the `lonejson`
dependency version declared by the `lockdc` rock.

Top-level JSON `null` is returned as `lockdc.json_null`. That keeps successful
JSON `null` payloads distinct from the binding's existing `nil, err` and
`nil, meta` return conventions.

## Consumer model

`client:subscribe(req, handler)` and `client:subscribe_with_state(req, handler)`
are direct bindings to the C streaming subscription operations. Their handlers
run synchronously on the calling Lua state and receive the normal public
`Message` and optional `Lease` receivers, not raw native userdata. A subscription handler must
explicitly `ack()` or `nack()` its borrowed message before returning; returning
`false, message`, `nil, message`, or raising stops the subscription with a
structured error. A message (and the optional state lease) is invalid once its
handler returns, even if Lua retains the wrapper. The client remains usable in
the handler, matching C subscription semantics, but it cannot be closed until
the native callback returns.

`client:watch_queue(req, handler)` is likewise a direct C queue watch, not a
polling loop. The handler receives a borrowed event table; return `false` to
stop cleanly, return `nil, message` or `false, message` to fail, and otherwise
return normally to continue.

`client:new_consumer_service(config)` is the Lua-specific threadless managed
consumer adaptation. Native `lc_consumer_service` workers cannot invoke a Lua
state from their private threads, so this surface deliberately supports one
blocking consumer at a time:

```lua
local service = assert(client:new_consumer_service({
  name = "orders-worker",
  request = { namespace = "orders", queue = "events", owner = "orders-worker" },
  with_state = true,
  handle = function(message, state)
    -- Return normally to acknowledge. Return nil, err (or false, err) to nack.
    return nil
  end,
}))
assert(service:run())
```

It uses the C-shaped `name`, `request`, and `with_state` configuration fields.
`handle` is the necessary Lua callback spelling. `run()` blocks; `stop()` is
safe from that handler and `wait()` reports after `run()` returns, whether it
succeeded or failed. There is no Lua
`start()` or `start_consumer()` alias and no multi-worker/multi-config service
because neither can safely call one Lua VM concurrently. Start separate Lua
states or processes when concurrency is needed. A normal handler return
acknowledges an open message. A handler that has already acknowledged,
negatively acknowledged, or closed its message may also return normally; the
service does not apply a second terminal operation. `nil, err`, `false, err`,
or a raised error nacks a still-open message and returns that failure from
`run()`; a failed nack is returned instead.

## Examples

Pedagogic examples live in:

- `examples/lua/acquire_update_json.lua`
- `examples/lua/acquire_for_update.lua`
- `examples/lua/queue_roundtrip.lua`
- `examples/lua/namespace_config.lua`
- `examples/lua/pouch_local_storage.lua`
- `examples/lua/consumer_handler.lua`

See also:

- [examples/lua/README.md](../examples/lua/README.md)

## Optional PostgreSQL participation

The base Lua module and core libraries require no PostgreSQL client or cpktdb
SDK. The optional [C participant provider](postgres.md) uses explicit application
linkage and registration; enabling its CMake build does not load it into Lua.
`client:xa_coordinator(config)` exposes the existing C coordinator. Its config
accepts `owner_ttl_seconds`, `completed_retention_seconds` and
`completed_max_records`, with the [C defaults and retention rules](postgres.md).
The receiver has `register_start(name, configuration, deadline_ms)`,
`begin_start(deadline_ms)`, `adopt_start(id, deadline_ms)`,
`recover_start(id, deadline_ms)`, `retire_start(deadline_ms)` and
`close_blocking()`. A C embedding explicitly registers linked drivers through
[`lc_lua_xa_register_driver`](../src/lua/lockdc_lua.h); missing registration
returns `nil, error, ERR_INVALID`. Configuration conversion is protected against
Lua exceptions and its temporary is released before native operation advancement.

Every start returns an operation. `advance()` returns `false` while pending,
`true` on success, or `nil, error, code` on terminal failure. A terminal
`ERR_PENDING` is distinguishable through `result().done`. `wait()` returns
`{fd, events, deadline_ms}` for the application's event loop; READ=1, WRITE=2,
and the deadline is absolute monotonic milliseconds. Start's nil/zero deadline
selects 30 seconds; -1 means infinity. The facade performs no readiness polling.
`result()` copies IDs, `done`, numeric `decision` (0 undecided, 1 commit, 2 abort),
`applied`, `sqlstate` and `retired_count`. IDs are available before durable work.
Close an operation with `close_blocking()` after observing/taking its result;
`cancel()` preserves potentially prepared work for recovery.

Successful begin/recovery transfers a transaction once through
`operation:take_transaction()`. It has `id()`, `enroll_start(resource_id,
deadline_ms)`, `acquire_start(request, deadline_ms)`, `renew_start(deadline_ms)`,
`commit_start(deadline_ms)`, `rollback_start(deadline_ms)`, `borrow(resource_id)`
and `close_blocking()`. Managed acquire supplies the transaction ID and rejects
blocking waits; use `operation:take_lease()` once to obtain its lease. The request
accepts the ordinary boolean `if_not_exists` create-only precondition; an existing
record rejects acquisition without changing its contents. Operations
and child handles retain native owners even after a parent wrapper closes. GC
releases references and hands unfinished work to recovery; it never invents COMMIT.
Managed acquisition requires namespace, key and owner strings without NUL bytes.

For mixed outbox work, bind `outbox:begin()` with
`box:bind_xa(transaction)`. An empty box selects the managed ID; existing work
requires adopting `box:id()` first. Stage through the same box and drive
`box:commit_start()` or `rollback_start()`. After applied COMMIT, ordinary
`box:commit()` collects receipts once. It returns pending before application.
Caught staged errors still mark the Lua transaction rollback-only; failures from
direct XA operations or any bound box block commit through every other box.
Callback-owned boxes reject terminal starts until their transaction callback
has returned. Ordinary
unbound Lua outbox callbacks retain their existing behavior.

A borrow's `view()` exposes identity/kind metadata, and `close()` releases its
reference. C query adapters consume either facade or core borrow values through
[`lc_lua_xa_borrow_retain`](../src/lua/lockdc_lua.h), retain the token while results
or callbacks use it, and revalidate with `lc_xa_borrow_view` before every native
access. No native pointer or private userdata layout is part of the Lua API.
Prepare requires every borrower/result to release protocol ownership first.

`coordinator:transaction(callback, await)` owns begin, the final decision and
cleanup. Supply an await function explicitly or as the coordinator config's
`await` field. It must drive the passed operation to completion using its
readiness/deadline; it may yield. The helper verifies the actual C terminal status
before trusting a scheduler return. Callback-owned transactions offer `enroll(id)`,
`acquire(request)`, `renew()`, `outbox(producer)` and
`with_connection(id, adapter, callback)` conveniences. They use that same await
function and perform no polling inside the library. Each adapter factory receives
a checked borrow and returns a session with `close()`; its query/cursor methods
use the public C interop token. The helper closes the session and borrow before
prepare, including when a callback throws or yields. The query adapter owns
protocol/result cleanup, rejects local transaction helpers, and never frees the
borrowed native connection.

Normal return commits and returns `{txn_id, decision, applied, outboxes, values}`;
`values` is a packed callback-result array preserving nils. Callback exceptions
or caught failures from staged work choose ABORT. The callback cannot manually
commit, roll back or close its managed transaction, vote through a returned
lease's `release()`, or start a terminal operation through a bound outbox.
These lease guards and rollback-only tracking also apply to leases transferred
through `acquire_start()` and `operation:take_lease()` inside the callback.
Failure returns
`nil, error, code, state`, preserving the global ID and last observed decision/
application evidence. An await exception after COMMIT keeps COMMIT evidence;
pending or cancelled phase-two work goes to recovery without SQL replay.
Caught failures in reentrant staging calls remain rollback-only, including
calls through another outbox bound to the same transaction. Successful nesting
does not prevent COMMIT.

The optional `lockdc.postgres` module registers `"postgres"` when required:
`local postgres = require("lockdc.postgres")`. Use `postgres.register(name)` for
an additional explicit alias. Build it with PostgreSQL and Lua bindings enabled,
or install the separate `lockdc-postgres` rock alongside the matching `lockdc`
rock. The rock uses the public shared provider target and requires
`LOCKDC_PREFIX`, `LOCKDC_POSTGRES_PREFIX` and `CMAKE_PREFIX_PATH` to select the
core/provider, compatible cpkt/cpktdb SDKs, and the core's public dependency SDKs
(pslog, lonejson and liblql). A cross compiler additionally
selects `LOCKDC_LUA_TOOLCHAIN_FILE`; headers come from the selected Lua 5.5 SDK.

Fully static embeddings compile the facade sources, link the public static
provider target, and preload `luaopen_lockdc_postgres` with `luaL_requiref`.
The [`lc_lua_postgres_register`](../src/lua/lockdc_postgres_lua.h) C helper also
remains available. This capability is absent from the base core module; requiring
the optional module is explicit Lua module loading, not core plugin discovery.
Provider callbacks and query adapters must remain linked for all retained tokens.
Its
configuration is `{resource_id=stable_id, connection_info=connection_string}`;
the helper copies both strings and does not log connection configuration.
An embedding can replace the default registry name by supplying another name.

### Public resource and branch parity

The Lua receivers delegate to the public C XA APIs, including PostgreSQL's
native primitives through its `lc_xa_driver` table. There is one resource and
branch interface for both manual and managed use; provider kind is metadata.

| Receiver | Public operations |
| --- | --- |
| `client:xa_store()` | Open the retained Pouch journal |
| Journal | `resource(name, configuration)`, `retain()`, `register_blocking(record)`, `enroll_blocking(record)`, `progress_blocking(record)`, `read_blocking(txn_id, branch_id)`, `close()` |
| Resource | `identity()`, `register_start(deadline_ms)`, `branch_start(txn_id, deadline_ms)`, `recover_start(txn_id, branch_id, commit, deadline_ms)`, `status_start(txn_id, branch_id, deadline_ms)`, `inventory_start(after_gid, limit, deadline_ms)`, `close_blocking()` |
| Branch | `record()`, `prepare_start(deadline_ms)`, `resolve_start(commit, deadline_ms)`, `borrow()`, `close()` |
| Resource operation | `advance()`, `wait()`, `result()`, `cancel()`, `take_branch()`, `close_blocking()` |

`coordinator:resource(resource_id)` exposes its registered manual receiver and
retains the coordinator. Closing that view releases the reference, never closes
the coordinator-owned native resource independently. Do not overlap manual and
managed operations on the same resource. Independently opened resources retain
their journal, and operations retain their resource/branch, even after a parent
Lua receiver closes. C retain/release for coordinator, transaction and borrow
lifetimes is represented by Lua owner references and checked C adapter retention;
callback tables and opaque native/thread pointers remain C interop.

Manual operations use the same advance/wait/deadline and error conventions as
managed operations. `result().done` is false while pending; terminal results
contain `record`, `native_outcome`, `sqlstate`, `row_count` and copied `rows`.
Successful branch start/recovery transfers a branch exactly once using
`take_branch()`. The creating operation retains a native lifetime pin even after
transfer: closing the branch first defers destruction until operation close.
An independently opened resource's close prevents new starts/borrows through its
retained branches; existing operations can finish and branches remain inspectable
and closeable. A coordinator-owned resource view instead releases its coordinator
reference, as described above.
Inventory is GID-ordered, limited to 64 rows, and diagnostic:
nil/zero limit selects 64; the last returned GID is the next cursor. It never
authorizes resolving foreign prepared transactions. Resolution's `commit`
argument is boolean and must agree with the immutable global decision.
Enroll every member before the first prepare seals the participant set. Prepare
requires all borrows/results to close. Raw implicit XA can decide after all YES
votes; an unvoted Pouch member holds the decision while application work stages.

Branch records copy every public C field. GID, full XID, system/database/role
identities are strings; `prepared_observed` is boolean; phase/evidence and
completion time are integers. Journal reads additionally return the authoritative
coordinator YES flag; `prepared_observed` only records native preparation evidence,
including observation after ABORT without a YES vote.
`borrow:view()` provides size, ABI/kind and resource/transaction/branch IDs,
plus exact decimal `owner_process` and `generation` strings; native connection
and opaque thread identity are available only to the public checked C adapter.
`XA_*` constants mirror the C phases, evidence sources, outcomes and decisions.
The optional PostgreSQL module additionally exports its C `OUTCOME_*` constants.
Journal writes are explicitly blocking and retain the C validation/fencing rules.
SQL execution belongs to the query adapter, as in the C API.
Raw `txn_prepare`, `txn_commit`, `txn_rollback` and `txn_replay` require an exact
string `txn_id` without NUL bytes; malformed identities return `ERR_INVALID`.

```lua
local coordinator = assert(client:xa_coordinator({ await = event_loop.await }))
-- Drive/close register_start("postgres", config) for each stable resource ID.
local receipt, err, code, state = coordinator:transaction(function(tx)
  assert(tx:enroll(first_id))
  assert(tx:enroll(second_id))
  local box = assert(tx:outbox(outbox))
  local order = assert(box:acquire({ namespace = "orders", key = order_key }))
  assert(order:update(order_bytes))
  tx:with_connection(first_id, query_adapter.borrow, function(sql)
    sql:execute(statement, parameters)
  end)
  tx:with_connection(second_id, query_adapter.borrow, function(sql)
    sql:execute(other_statement, other_parameters)
  end)
  assert(box:append(effect_identity, payload_source))
end)
```

The [compiled embedding](../tests/integration/test_lua_postgres_xa.c) uses only
this public bridge and cpktdb's public C89 facade. Its explicitly named blocking
query helper is a bounded test adapter; application adapters may expose their
own asynchronous operations. The [Lua E2E](../tests/lua/test_lockdc_postgres_xa.lua)
checks real COMMIT and ABORT outcomes across two independent PostgreSQL instances,
Pouch and outbox, including SQL failure and borrow release/GC. An ordinary
unbound Lua outbox transaction does not enroll SQL automatically.
