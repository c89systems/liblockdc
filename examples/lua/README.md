# Lua Examples

These examples are written to show the intended Lua DX directly. They do not
hide request shaping, result handling, or JSON/state operations behind reusable
helpers.

## Prerequisites

Install the `lockdc` and `lonejson` Lua rocks against a released or staged
`liblockdc` SDK bundle.

For normal LuaRocks use, install the matching shared `liblockdc` binary SDK
release first, then install the Lua rock. The `lockdc` rock does not build the
full C SDK for you; it builds only the Lua binding and links it against the
matching installed `liblockdc` release.

Static-only SDK bundles are for `vectis` or other embedded-Lua build graphs,
not for a normal `luarocks install` flow.

`liblockdc` owns this Lua dependency layout. Downstream consumers such as
`vectis` should use the `lockdc` Lua module and the `lonejson` Lua rock version
declared by `lockdc` rather than repackaging a separate `lockd` Lua client or a
competing JSON binding.

PostgreSQL XA additionally uses the optional `lockdc-postgres` rock and the
native provider/cpktdb SDKs. Core Lua remains usable without them. Static hosts
preload the linked registration entry point. See the [Lua XA guide](../../docs/lua.md#optional-postgresql-participation)
for managed transactions, manual participant APIs and checked query adapters.

For this release line, the `lockdc` rock declares `lonejson == 0.48.0-1`. That
matches the native `lonejson 0.48.0` dependency used by the C SDK for mapped
state load/save and internal JSON response parsing.

When running from this repository after installing `lockdc` and `lonejson` into
a local LuaRocks tree, run examples with the project-built Bootlin Lua runner:

```bash
eval "$(make -s lua-env)"
eval "$(luarocks --tree /path/to/lua-tree --lua-version 5.5 path)"
"$LOCKDC_LUA_BIN" examples/lua/acquire_update_json.lua
```

`make lua-env` does not install rocks; it supplies the Bootlin runner and
repository paths. The LuaRocks export supplies the installed `lonejson` module.
Do not use `LD_LIBRARY_PATH` or a host Lua interpreter for this flow.

## Common environment

The examples read these variables when present:

- `LOCKDC_URL`
- `LOCKDC_CLIENT_PEM`
- `LOCKDC_NAMESPACE`
- `LOCKDC_KEY`
- `LOCKDC_OWNER`
- `LOCKDC_QUEUE`

Source the generated environment to use the checkout-specific endpoints and
client bundles from the local Podman Kube environment.

Start it with:

```bash
make dev-up
source build/devenv/env.sh
```

## Examples

- `acquire_update_json.lua`
  acquire a lease, read JSON state, update JSON state, and release
- `acquire_for_update.lua`
  wraps the same acquire, snapshot, update, and release lifecycle for callback
  code; successful handlers commit and failed handlers roll back. The complete
  snapshot is staged in a temporary file, then copied into a Lua string before
  the callback; `load_json()` decodes that string
- `queue_roundtrip.lua`
  enqueue a JSON payload, dequeue it, inspect it, and acknowledge it
- `namespace_config.lua`
  read namespace engine configuration and trigger an index flush
- `pouch_local_storage.lua`
  open encrypted, compressed local Pouch storage with first-run key generation
  through Lua's typed `pouch` configuration table
- `consumer_handler.lua`
  run a stateful queue consumer with explicit message and lease handling
- `../outbox_producer.c` and `outbox_dispatcher.lua`
  demonstrate the transactional-outbox deployment boundary: the C producer
  commits an effect into a shared-root Pouch store and exits; a dedicated Lua
  dispatcher process later claims and completes it. Set `LOCKDC_POUCH_ROOT` to
  the same absolute root for both commands. The producer prints its
  commit-published outbox key; a real supervisor may forward that key as a
  bounded latency hint, but restart reconciliation remains authoritative.

For a one-shot local demonstration, run the producer first and then the Lua
dispatcher with `LOCKDC_OUTBOX_ONCE=1`. Without that variable,
`outbox_dispatcher.lua` is the blocking dedicated worker process. Both
processes deliberately use `single_writer=false`; default Pouch mode permits
only one live writer and is the preferred setting when producer and dispatcher
are in one process.

## Consumer model

The Lua consumer path is intentionally simple and single-threaded.

- `client:subscribe(req, handler)` and `client:subscribe_with_state(req, handler)`
  directly use the C streaming subscriptions; handlers must terminalize each
  borrowed message before returning
- `client:watch_queue(req, handler)` directly uses the C streaming queue watch
- `client:new_consumer_service(config):run()` is the single-consumer, blocking
  Lua-managed adaptation; a normal handler return acknowledges, while
  `nil, err`, `false, err`, or an exception nacks and stops it
- each Lua managed service takes exactly one C-shaped config with `name`,
  `request`, optional `with_state`, and `handle`
- one message is consumed at a time
- the Lua handler runs to completion on the calling Lua state
- after the handler completes, the next message is consumed

A handler may explicitly `ack()`, `nack()`, or `close()` its message and then
return normally. The managed service only acknowledges an still-open message;
it never applies a second terminal operation.

This is deliberate. The Lua binding does not expose the native threaded C
consumer callback model because calling back into the same Lua state from
multiple native threads would be unsafe.

In practical terms, the intended Lua DX is:

1. construct one blocking consumer service or subscription
2. handle one message
3. update state or attachments if needed
4. ack or nack
5. continue to the next message

For the broader Lua SDK surface and distribution policy, see `docs/lua.md`.
