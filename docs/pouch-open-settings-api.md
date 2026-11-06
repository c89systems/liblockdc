# Typed Pouch open settings

`lc_pouch_settings` is the typed, local-storage policy portion of
`lc_client_config`. It keeps Pouch root policy out of `pouch://` query strings
while retaining those URLs as a supported compatibility mechanism.

## Endpoint and writer policy

Select Pouch through a single absolute `pouch://` endpoint, for example
`pouch:///var/lib/my-service/lockd-root`. The default is exclusive single-writer
mode: one live process owns the root append path and normal acquire, update,
release, queue, attachment, and query operations use the resident logstore fast
path. Opening a second default writer for the same root fails instead of
silently downgrading. Explicit shared-root writing remains available for callers
that need multiple active local writers by adding `?single_writer=false`; that
mode preserves correctness and process fencing but is not the primary
performance target.

See the [storage specification](pouch-storage.md) for on-disk representation,
recovery and maintenance semantics.

## C API

Initialize both transparent structures, set a bit for every field supplied, and
borrow the settings only for `lc_client_open()`:

```c
lc_client_config config;
lc_pouch_settings pouch;

lc_client_config_init(&config);
lc_pouch_settings_init(&pouch);
pouch.set_mask = LC_POUCH_SETTING_QUERY_INDEXING |
                 LC_POUCH_SETTING_QUERY_ENGINE |
                 LC_POUCH_SETTING_INDEXER_FLUSH_DOCS;
pouch.query_indexing_enabled = 0;
pouch.query_engine = "scan";
pouch.indexer_flush_docs = 128U;
config.endpoints = endpoints; /* one pouch:///absolute/root endpoint */
config.endpoint_count = 1U;
config.pouch_settings = &pouch;
```

`set_mask` is necessary because `false` and zero are meaningful values. The
public bit constants are macros rather than a C enum so the public API remains
C89-compatible while using the full `uint64_t` mask. Unknown bits, invalid
booleans, invalid engines, invalid compression, and selected empty strings are
rejected by `lc_client_open()`.

The settings object is valid only for exactly one local `pouch://` endpoint. A
non-empty mask with a remote TCP/TLS or Unix-socket client returns
`LC_ERR_INVALID` before the transport opens; the library never serializes
local-root settings onto a remote request.

## Lua API

Lua supplies the same fields through `lockdc.open({ pouch = { ... } })`.
Presence sets the corresponding C mask bit, including `false` and `0`:

```lua
local client, err = lockdc.open({
  endpoints = { "pouch:///var/lib/my-service/lockd-root" },
  pouch = {
    query_indexing = false,
    query_engine = "scan",
    indexer_flush_docs = 128,
    durable_sync = false,
  },
})
```

Supported Lua keys map to the C settings: `single_writer`, `durable_sync`,
`fsync_batch_max_ops`, `segment_target_bytes`, `indexer_flush_docs`,
`indexer_flush_interval_seconds`, `background_compaction`,
`disable_compaction_throttling`, `terminal_reclaim_min_bytes`, `queue_watch`,
`query_engine`, `query_fallback_engine`, `query_indexing`, `crypto_key`,
`crypto_key_file`, `crypto_generate_key_file`, and `compression`. Unknown keys
and wrong Lua types are rejected. Backend and value failures return the normal
structured `lockdc.open` error.

Three Lua names are shorter deployment names: `background_compaction` maps to C
`background_compaction_enabled`, `disable_compaction_throttling` maps to
`compaction_throttling_disabled`, and `query_indexing` maps to
`query_indexing_enabled`. Numeric settings require exact nonnegative Lua
integers; unlike collection positions, these settings do not accept decimal
strings.

Root defaults are exclusive writing and NoSync (`single_writer=true`,
`durable_sync=false`). Enabling durable sync waits for the root's shared commit
group before acknowledging a mutation. NoSync does not guarantee persistence
across power loss, and does not disable durability barriers needed for recovery
or safe retirement of retention work. Background compaction defaults to enabled
and also drives state/collection expiry and dropped-partition cleanup, normally
on a 30-minute cadence. Outbox receipt/dead-letter reclamation instead belongs
to its explicitly acquired dispatcher.

The durable-sync worker dispatches ready requests without a batching delay.
Requests arriving during I/O form the next group. `fsync_batch_max_ops` is an
upper bound, not a minimum group size; zero is unbounded. Handles in one process
share the root's batcher and use their lowest nonzero bound. Separate processes
have separate SDK queues.

## Resolution rules

Each field resolves once during client construction, in this order:

1. a selected field in `lc_pouch_settings` / the Lua `pouch` table;
2. legacy dedicated `lc_client_config` crypto and compression fields;
3. the equivalent `pouch://` endpoint query option;
4. the established Pouch default.

Compression omission inherits the existing root's stored mode; only new roots
default to `none`. Explicit `none` or `zlib` requests synchronous whole-root
conversion when different. Competing writers cause a warning and use of the
stored mode, while conversion errors fail the open. See
[compression changes on open](pouch-compression.md). Conversion retires one
source segment or snapshot at a time; indexed collections roll their converted
output into final canonical parts. Interrupted work resumes even if the next
open omits compression; no client is admitted to a partly converted root.

The selected typed setting always wins. For C callers,
`pouch_settings.compression = "none"` overrides both the legacy dedicated field
and `?compression=zlib`. Lua exposes only its nested `pouch` table and rejects
those legacy inputs. No caller-owned settings pointer is retained after opening,
and no settings precedence is evaluated in Pouch mutation, replay, query,
compaction, or dispatcher paths.

## C endpoint query options

Typed settings are preferred and override endpoint options per field. These
query options remain supported for C deployments; Lua accepts only the typed
`pouch` table.

Common legacy options are:

- `compression=zlib` for streaming at-rest zlib compression. Omission inherits
  an existing root; explicitly changing mode converts it before open returns
  when no other writers are active. Conversion resumes after interruption and
  replaces one segment or snapshot at a time without a second root. See
  [compression changes](pouch-compression.md).
- `crypto_key_file=/path/to/pouch.key` with `crypto_generate_key_file=true` for
  encrypted local roots
- `durable_sync=true` and `fsync_batch_max_ops=<u64>` for root-scoped durable
  group commit
- `segment_target_bytes=<u64>` for rolling segment sizing
- `indexer_flush_docs=<u64>` and `indexer_flush_interval_seconds=<u64>` for
  bounded query-index publication: exclusive roots publish at the document
  threshold after all pending leases or transactions have completed, while
  shared roots publish asynchronously
- `background_compaction=false` to disable the default cadence-based compaction
  worker, and `disable_compaction_throttling=true` to remove its default
  throughput bound
- `terminal_reclaim_min_bytes=<u64>` to set the minimum active-segment size
  before bounded terminal-history compaction seals it (default 1 MiB)
- `queue_watch=true` to request filesystem-assisted queue wake-up where the
  local filesystem supports it, with polling fallback otherwise
- `query_engine=index|scan` and `query_fallback_engine=index|scan` for the
  namespace query preference used at open
- `query_indexing=false` for roots that never use indexed queries. This disables
  local index maintenance and makes implicit queries use scans; explicit indexed
  queries and `flush_index` are unavailable. Pouch outbox recovery and
  dead-letter management continue through bounded scan queries.

## State retention

State writes and metadata updates accept optional per-object retention through
the same C and Lua APIs for Pouch and remote lockd. Omission preserves the
policy, explicit zero clears it, and positive seconds start at commit. Expired
Pouch objects remain ordinary state until the existing maintenance worker
deletes them. The default maintenance cadence is 30 minutes; disabling
background compaction also disables automatic expiry cleanup. Remote results
advertise support when available; a successful write without retention metadata
does not confirm that an older server applied the option. Older servers remain
usable without retention. See [state retention](state-retention.md).

## Boundaries

These are root-open policy settings. They are not per-query controls:
`lc_query_req.engine` and `refresh` retain their existing single-query meaning.
They are also not durable namespace preferences, which remain managed through
`lc_namespace_config`. Applying settings at open can create control metadata,
upgrade collection heads or convert the durable compression mode as described
above. These local effects do not create a remote lockd configuration protocol.

Legacy endpoint query options and `lc_pouch_endpoint_build()` remain supported
for C deployment compatibility. Lua deliberately rejects Pouch endpoint query
options and exposes only the typed settings table, which is discoverable,
type-checked, and makes the local-only boundary explicit.
