# Release notes

## Unreleased

## 0.28.0

- Stream pushed and staged-pull collection entries directly into shared
  canonical segments using bounded codec chunks and immutable offset indexes.
  Publish each entry atomically without a whole-entry buffer, temporary spool,
  commit-time payload copy, or segment inode per entry. Preserve concurrent
  producers, lease fencing, retry identities and durable group commits.
- Read, compact, recover and convert compression for chunked entries with
  bounded memory. Roll compacted output across snapshot parts rather than
  collecting a large entry or collection into one oversized file. Preserve
  metadata references, retention, atomic edits and pinned-reader visibility.
- Reject entry-sized allocation regressions and cover interrupted uploads,
  publication failures, lost unsynced writes, concurrent producers, rotation,
  projection recovery and encrypted/compressed storage.
- Remove unused private transport, codec and index helpers. Complete public C
  and Lua contract comments, retire superseded implementation notes, and include
  the linked collection guides in Lua source packages.
- Decrypt uncompressed collection frames directly into bounded read scratch,
  avoiding an intermediate plaintext copy. Retain authentication before delivery
  and wipe tentative output on authentication failure.
- Reuse per-operation HMAC key setup and the direct reader's manifest binding;
  avoid repeating identity construction and descriptor comparison for a unit
  whose publication descriptor was constructed from that same unit.
- Deliver large collection reads in bounded 64 KiB heap chunks matching the
  codec transfer size, retaining the 16 KiB allocation for small reads.
  Preserve retry cursors, filtering and integrity checks after read failures.
- Upgrade lonejson to 0.46.0 and libpslog to 0.13.0, including all target SDK
  checksums and the matching lonejson Lua dependency. Lonejson retains ABI 26.
- Upgrade liblql to 0.4.1 from c89systems, with verified SDK checksums for all
  supported targets. Its public C API and shared-library ABI remain unchanged.

### Upgrading from 0.27.x

Core shared-library ABI remains **9**; the optional PostgreSQL provider's ABI
remains **0**. Public C layouts and existing append APIs are unchanged.

Existing contiguous collection records remain readable without rewriting their
payloads. Push and staged-pull appends can now publish new chunk/index record
kinds; older libraries cannot read those records. Upgrade every client of a
shared Pouch root before using these append paths, and do not downgrade after
writing chunked entries. See
[incremental collection appends](docs/collection-writer.md).

The `append_staged` name is retained, but its implementation now feeds the same
bounded canonical writer as push without staging a complete entry. Direct pull
remains available. Source and writer ownership, atomic publication and the
selected NoSync or durable-sync policy retain their existing contracts.

## 0.27.0

- Convert Pouch compression on open using resumable segment/snapshot
  replacements instead of duplicating the retained root. Resume after partial
  writes or crashes before admitting clients, including opens without an
  explicit compression option. Preserve payload references, collection
  continuations, permissions and logical identities through cutover.
- Keep obsolete-file cleanup and compaction candidate views coherent, including
  partial failures and retries. Sync deleted-file directories before retiring
  obsolete markers and retain candidate path, errno and phase in diagnostics.
- Cover compression recovery after loss of unsynced staging bytes and directory
  updates, in both directions with plaintext and encrypted storage.
- Use the `c89systems` GitHub organization for liblockdc, liblql, lonejson and
  libpslog source, dependency and release references.

### Upgrading from 0.26.x

Core shared-library ABI remains **9**; the PostgreSQL provider's ABI remains
**0**. Public C layouts and canonical storage formats are unchanged.

Explicit compression changes still complete synchronously before open returns,
under exclusive root ownership. Temporary payload duplication is now limited to
one converted segment or snapshot, plus recovery metadata and index scratch;
snapshots can exceed the normal segment target. Decompression also needs space
for the final root's growth. See [compression conversion](docs/pouch-compression.md).

An interrupted conversion resumes even when the next open omits compression.
Correct the underlying space, I/O or integrity problem and reopen with this
release. Keep using this release until conversion finishes; do not remove its
intent or recovery metadata to bypass it. Pending publication from the previous
whole-tree converter is completed automatically before clients are admitted.

## 0.26.0

- Add atomic collection range edits with an optional streaming replacement,
  bounded recovery descriptor, head preconditions and persistent retry receipts.
  Preserve entry positions, pinned readers, lease fencing, retention and codec
  conversion through matching C and Lua APIs. Edits are collection-local and
  supported by Pouch; remote capability discovery reports unsupported.
- Advance the core shared-library ABI once from released ABI 8 to ABI 9 for the
  collection receiver, capability and read-option layouts. Rebuild C consumers,
  Lua native bindings and the optional PostgreSQL provider together.
- Reuse bounded decoding, key-derivation and binding contexts across collection
  entries, coalesce encrypted frame reads, and reduce metadata decoding and
  callback-lock overhead. Preserve independent entry authentication, payload
  fingerprints and streaming backpressure without caching document bodies.
- Encrypt streaming frames in place, removing a separate 64 KiB plaintext
  allocation, copy and wipe per encrypted writer. Keep key/nonce handling,
  frame boundaries and complete scratch cleanup on failure.
- Coalesce residual encrypted output and its terminal frame at EOF, preserving
  immediate full-frame delivery and byte-identical framing. Reuse each
  collection writer's metadata reservation through publication, eliminating
  a second metadata allocation without changing durability or storage format.
- Reduce collection edit CPU and read calls with page-level range counting,
  direct validated seeks for dense positions and projection from the writer's
  bounded control metadata. Preserve canonical receipt validation on recovery,
  sparse-position lookup, pinned-reader visibility and configured durability.
- Dispatch ready Pouch durable-sync groups without the fixed two-millisecond
  coalescing delay. Preserve per-file deduplication, arrivals grouped during
  I/O, live request-count bounds and shared failure outcomes. Keep durable
  acknowledgement, collection publication and NoSync policy unchanged.

### Upgrading from 0.25.x

Rebuild C/C++ consumers, Lua native bindings and the optional PostgreSQL provider
against core ABI **9** (`liblockdc.so.9` or `liblockdc.9.dylib`). Collection
receivers, capabilities and read-option layouts changed; the provider's own ABI
remains **0**. Existing append calls and reads configured through
`lc_collection_read_opts_init` retain their behavior when optional edit/head
preconditions are unused.

Existing 0.25.x Pouch roots require no conversion or entry-body rewrite. Upgrade
all clients before using atomic edits: older binaries cannot replay the new
edit records. Do not mix older clients with collections that contain edits.
Receipts remain until the collection is dropped, including after payload
deletion, expiry and compaction. See the [atomic edit contract](docs/collection-edits.md)
for retry, reconciliation and NoSync durability semantics. Remote collections
remain unsupported.

## 0.25.0

- Add explicit staged pull through `lc_collection_append_staged` and the Lua
  `collection:append_staged` facade. Consume sources once through the existing
  push writer, preserving callback, ownership, retry and retention semantics
  without holding publication locks during input. Direct pull remains available.
- Pack push-fed collection appends into shared rolling segments. Validate and
  encode once, stage up to 64 KiB of encoded bytes on the heap, then spill to
  an anonymous file. Preserve encrypted spools, lease fencing, atomic retention
  renewal, recovery and durable group commits without a segment per small entry.
- Add optional collection-wide leases, atomic creation with initial ownership,
  conditional takeover and separate keepalive/release. Entries commit under the
  same held lease; unleased collections retain multiwriter behavior. Default
  reads remain public, with optional private visibility until release/expiry.
  Expose the lifecycle through matching C and Lua receivers and capabilities.
- Advance the core shared-library ABI from 7 to 8 for collection layouts.
  Extend the collection control-head layout for bounded ownership. Upgrade
  released heads automatically on Pouch open under exclusive root access,
  preserving entries, retention and positions without rewriting bodies.
- Remove ordinary-state transaction serialization from isolated collection
  operations, allowing renewal/takeover during uploads and independent
  collections to progress under their existing storage locks.

- Reduce collection append allocation and clearing costs with a per-writer
  2 KiB plain-output chunk that grows only when needed. Large and transformed
  bodies retain the existing 64 KiB frame/write cadence.
- Publish the identity-index header once with its replay checkpoint, reuse
  metadata stats within each refresh, and reuse the read's selected digest
  implementation across entries while checking every payload fingerprint.
- Seal canonical collection history without redundant projection syncs at
  segment transitions. Clean checkpoints still sync both projections before
  publishing their marker; unclean recovery rebuilds from canonical records.
- Extend collection diagnostics with CPU measurements, an explicit storage
  directory, and optional count-retention maintenance/reopen checks.

### Upgrading from 0.24.x

Rebuild C consumers and the optional PostgreSQL provider against core ABI **8**
(`liblockdc.so.8` or `liblockdc.8.dylib`). Collection receiver and capability
layouts changed; the provider's own ABI remains **0**. Existing direct pull calls
retain their behavior. Staged pull is explicit through `append_staged`, and push
writers now pack entries into shared rolling segments with bounded encoded
staging instead of adopting one segment per commit.

Stop all other clients before first opening an older collection root. Pouch
automatically upgrades collection heads without rewriting entry bodies; a busy
root returns `LC_ERR_PENDING` / `collection_upgrade_busy`. Interrupted upgrades
resume on the next open. Do not downgrade or use older binaries on upgraded
roots. See [collection storage upgrade](docs/collection-leases.md#storage-upgrade).

## 0.24.0

- Upgrade cpkt to 0.6.1, including its libssh2 AES-GCM key cleanup fix. Keep
  cpktdb at 0.3.1 and use the published core compatibility floor.

- Upgrade liblql to 0.4.0 for allocator-owned workspaces and default musl
  worker-thread stacks. Its public API and shared-library ABI remain unchanged.

- Keep stream-copy, mutation and Pouch query scratch buffers off worker stacks.
  Use small bounded chunks for short copies and allocator-owned 16 KiB bulk
  copy/mutation scratch. Reuse query scratch within each request and release
  scratch on success and failure.

- Convert Pouch roots synchronously on an explicit compression change, in both
  directions, preserving states, attachments, collections, cursors and retained
  transaction history. Omission inherits the stored mode; new roots still use
  none. Competing writers cause a warning and adoption of the stored mode.
  Publication is durable and recoverable; payload transformation streams in
  bounded chunks. Log warning on start/refusal and info on completion.

- Retire fully applied single-key implicit retention transactions on release,
  preventing successful state renewals from building a synchronous recovery
  backlog for the next open. Preserve explicit/joined XA outcomes and fence
  NoSync cleanup behind participant durability.

- Add push-fed collection append writers in C and Lua, sharing the incremental
  validator and codecs with pull append. Publish the pending canonical inode
  without a body copy, support explicit abort/close, and recover interrupted
  publication. Both append forms can atomically renew parent retention only
  for new membership; retries retain the original deadline observation.
  These public layout additions share core ABI **7** below.

- Add optional per-collection `max_entries` retention in C and Lua. Keep the
  newest N live entries using bounded lazy maintenance, preserving positions,
  retry identities, streaming reads, and existing age-retention behavior.
  Appends do not trim synchronously. Core ABI advances from released ABI 6 in
  v0.23.0 to **7** for public collection request/result fields; PostgreSQL
  provider ABI remains 0. Rebuild C consumers and the provider.

### Upgrading from 0.23.x

Rebuild C consumers and the optional PostgreSQL provider against core ABI **7**
(`liblockdc.so.7` or `liblockdc.7.dylib`). Collection request/result layouts and
the client receiver surface have changed; the provider's own ABI remains **0**.
Existing collections have no count limit unless explicitly configured. Lua
callers can use the new optional fields without changing existing calls.

Existing Pouch roots retain their stored compression when the option is omitted.
Explicitly requesting a different mode now converts the root before open returns;
allow time and disk space for the replacement tree. See
[compression conversion](docs/pouch-compression.md) for writer exclusion,
failure recovery and logging behavior.

## 0.22.0

- Add ordered Pouch collections to C and Lua: immutable JSON bodies, permanent
  positions, idempotent append/status, individual deletion, indexed LQL filters,
  raw JSONL delivery with separate metadata, resume and incremental checkpoints.
  Permanent-position ranges and first-N input windows select entries before
  LQL; resume preserves the unconsumed input budget and checkpoints start the
  next window after its accepted boundary.
  Optional collection and entry retention use metadata-only changes and lazy
  maintenance cleanup. Remote collection operations report unsupported.
  Durable concurrent appends share fsync groups without retaining append or
  shared-root mutation locks during synchronization; a commit barrier preserves
  snapshot visibility and compaction safety while callers wait.

- Add optional per-object state retention across C and Lua state APIs. Preserve
  omitted policy, support explicit clearing, and anchor staged durations to the
  durable commit time. Reclaim expired Pouch parents and owned attachments in
  bounded, recoverable passes on the existing maintenance worker. Forward
  explicit retention to remote lockd and expose response-local capability and
  deadline observations. Core shared-library ABI advances from 5 to **6**;
  mapped C `save` now accepts update options. PostgreSQL provider ABI stays 0.

- Preserve automatic retention discovery across NoSync power loss by syncing
  canonical state before retiring an empty namespace hint. Cover retention
  clears, deletes, rotation, independent writers and synchronization failures.

- Reduce repeated prerelease checks with explicit portability selections,
  shared Lua E2E setup, controlled expiry clocks and an early AFL compiler check.

- Upgrade cpkt to 0.5.2 and cpktdb to 0.3.1 for faster SDK validation. The
  database SDK requires core 0.5.1 within its declared compatibility span.

- Compile public headers independently as C89 in normal test builds, giving
  clangd exact core/provider commands when opening headers directly. Verify
  native editor discovery through the normal Debug compilation database.

- Report deferred Pouch obsolete-file cleanup in completion diagnostics instead
  of duplicate warnings. Label deleted artifacts correctly and distinguish
  deliberate query-index invalidation from unexpected index corruption.

### Upgrading from 0.21.x

Recompile all C consumers against the new headers and core library: public
request, result, and receiver structures have changed layout. Core shared
libraries now use ABI **6** (`liblockdc.so.6` or `liblockdc.6.dylib`). Rebuild
the optional PostgreSQL provider against that core; its own ABI remains **0**.

Both `lc_lease_save()` and `lease->save()` take `const lc_update_opts *opts`
immediately before the error argument. Pass `NULL` to retain the previous
mapped-save behavior. Lua call signatures remain unchanged; retention is an
optional request-table field.

Existing state has no expiry by default. Omitted retention preserves the
current policy; explicit zero clears it. Expiry only makes Pouch state eligible
for deletion by maintenance, whose default cadence is 30 minutes. Disabling
background compaction also disables automatic state expiry cleanup. Older
remote lockd servers remain supported for ordinary calls; successful explicit
retention requests require support/deadline observations to confirm application.
See [state retention](docs/state-retention.md) for the complete contract.

## 0.21.0

- Upgrade cpkt to 0.5.0 with OpenSSL 3.6.5 initialization-failure and
  static-plugin pthread-key cleanup fixes; keep cpktdb at 0.3.0. Cover failed
  TLS-key creation and repeated encrypted-Pouch static-plugin unload with
  key-capacity checks and Valgrind.
- Reject Pouch mutations when required thread-local tracking keys cannot be
  initialized. Preserve application-owned thread-local data and unwind locks
  safely across repeated failures in shared and single-writer modes.
- Give Lua stateful-outbox fixtures at least a full second for checkpoint I/O
  while retaining short expiry-recovery coverage across wall-clock boundaries.
  Keep auxiliary Pouch stores inside the cleared test root for repeatable runs.
- Release the disabled fallback logger on SDK unload or process exit, and avoid
  creating it while checking ownership of an application-supplied logger.
- Reclaim thread JSON runtimes and Pouch TLS keys on unload. Report outstanding
  tracked allocations after library-owned lifetime cleanup.
- Enable allocation tracking throughout C/Lua E2E and PostgreSQL integration
  tests. Fail on captured leak diagnostics and cover library unload/reload,
  borrowed logger ownership, failed opens, and static shutdown with Valgrind.

The core shared-library ABI remains **5**; the PostgreSQL provider ABI remains
**0**. The Pouch storage format is unchanged.

## 0.20.0

- Add opt-in process-wide runtime tracking for the built-in allocator, disabled
  by default. Select plain or tracking callbacks once at startup; custom hooks
  bypass tracking and disabled allocation has no tracking-flag check.
- Track live built-in allocations and report remaining
  counts and bytes at ordinary exit or shared-library unload. Use inline headers
  and fixed counters without separate metadata allocations. Reclaim idle Pouch
  process lock entries when closing a store.
- Complete dispatcher worker ownership release before shutdown reports success,
  preventing allocator use after the caller has closed its client.
- Compile core objects once for static/shared libraries and the HTTPS fixture once
  per configuration. Reuse the sanitizer build for E2E, build only selected fuzz
  targets, and avoid repeating identical SDK configuration checks. Acknowledge
  HTTPS fixture upload expectations immediately to eliminate fallback waits.
- Fix background Pouch compaction exhausting musl thread stacks. Reuse a bounded
  heap buffer for payload copying and candidate checksums, preserving large I/O
  chunks and incremental CRC validation without per-record scratch allocation.
- Let source-tree static consumers select the static libatomic archive instead
  of inheriting an absolute shared-library dependency.
- Include integration executables in the cross-runtime build target, so clean
  cross-release builds produce both compaction regression tests before CTest.
- Avoid repeated host fixtures and preliminary package checks during release.
  Build and run native archive consumers together, and build only the selected
  Valgrind test targets. Final artifact verification remains mandatory.
- Stop native checks at the first failure and reject empty test selections.
- Make the local E2E interrupt fixture deterministic during startup and verify
  that interruption tears down the owned Podman stack.

The core shared-library ABI remains **5**; the PostgreSQL provider ABI remains
**0**. The Pouch storage format is unchanged.

## 0.19.0

- Add managed XA coordination across local Pouch and PostgreSQL participants.
  Ordinary state and attachments, SQL, and optional inbox/outbox work share one
  durable decision. Recovery resolves prepared branches without replaying SQL.
- Add the separately linked `liblockdc_postgres` static/shared provider using
  cpktdb's public C89 facade. PostgreSQL remains optional: core artifacts and
  their dependency requirements are identical with the capability on or off.
- Add public scoped connection borrows and Lua transaction orchestration,
  including rollback on callback failure and guarded lease/outbox completion.
- Expose public XA journal, resource and branch operations in Lua, including
  native status, bounded prepared inventory, manual prepare/resolution and
  recovery. Add optional `lockdc.postgres` registration for module loading or
  static preloading, and a separate PostgreSQL Lua rock. Pending raw decisions
  and replay preserve the C result as a fourth Lua return value.
- Add independently configurable completed-record and coordinator-history
  retention, with 30-day age defaults and optional count limits. Incomplete
  transactions and live coordinator ownership remain protected.
- Harden coordinator fencing, abandoned-owner recovery, terminal replay,
  outbox publication gates, receipt ownership and Lua borrow cleanup.
- Verify mixed transactions across two PostgreSQL servers and Pouch on
  PostgreSQL 14–18. Service tests use rootless Podman Kube and assert teardown.
- Replace c.pkt.systems with cpkt 0.3.0 and optional cpktdb 0.3.0. Upgrade to
  lonejson 0.45.0 and libpslog 0.12.0; liblql remains at 0.3.0. The PostgreSQL
  provider ships in a separate SDK archive.
- Honor PostgreSQL provider selection in fresh configurations, so clean release
  builds include the selected provider SDKs. Explicit CMake selections take
  precedence; unselected fresh builds remain PostgreSQL-free.
- Fix strict C89 64-bit limit expressions and large-file access on 32-bit Linux.
- Bound release performance comparisons to smaller fixtures and 90-second
  measurement limits, retaining five samples, all core metrics and the 1.25x
  speedup requirement. Reduce benchmark preparation and server restart waits.

The core shared-library ABI advances from **4 to 5**. Rebuild/relink downstream
consumers for this release; the PostgreSQL provider has its own ABI 0.

PostgreSQL participation uses the local Pouch coordinator. Remote lockd
participation and remote lockd outboxes are unsupported.
