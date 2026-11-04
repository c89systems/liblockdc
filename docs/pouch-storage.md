# Pouch Storage Implementation Specification

This document is the implementation authority for Pouch storage. Pouch first
ships in v0.13.0, and the record format specified here is its initial durable
contract. Rejected development layouts, runtime paths, benchmark fixtures, and
transition names have no compatibility obligation. The current cutover retains
the authoritative Pouch record format while replacing the writer runtime
beneath it.

The implementation target is a real segmented logstore. Every namespace has a
single numeric rolling active segment and one Pouch replay format. The target
default runtime is an exclusive root writer with a resident namespace
logstore. An explicit shared-root runtime is supported over that same format.
This is not Go-disk interoperability or a parallel durable format.

## Reference Principle

Go lockd disk is the reference for storage semantics and operational model
because it has already solved append-only segment lifecycle, resident namespace
indexes, active-writer ownership, replay, manifest state, staged state
promotion, payload span ownership, append/fsync grouping, scan summaries,
compaction, and encryption placement.

Pouch is not Go lockd disk. It must remain a C storage engine with Pouch names,
C-native structs, C error handling, and the dependencies available in
liblockdc. It does not use protobuf and does not need byte-compatible `LOGD`
records. Divergence is acceptable only when it preserves the same storage
feature, durability property, and performance intent, and is a better local C
implementation.

Relevant Go disk files:

- `internal/storage/disk/logstore_record.go`
- `internal/storage/disk/logstore.go`
- `internal/storage/disk/logstore_support.go`
- `internal/storage/disk/logstore_compaction.go`
- `internal/storage/disk/staging.go`
- `internal/storage/disk/disk.go`
- `internal/storage/storage.go`
- `internal/storage/attachments.go`
- `internal/queue/service.go`
- `internal/queue/keys.go`
- `internal/core/locks.go`
- `internal/core/update.go`
- `internal/core/txn_marker_apply.go`
- `namespaces/config_store.go`

### Alignment Record

The 2026-08-04 source, behaviour, production-benchmark, and lifecycle sweep
completed the exclusive-writer cutover. Pouch now matches the Go disk storage
contract and operational model for namespace locality, record families,
fixed-width durable scalars, streamed payload spans, staged links,
lease/queue metadata, capture/validate/install compaction, recovery, grouped
durability, and public state/object/attachment/queue/query operations. Normal
exclusive state, lease, object, attachment, queue, and staged-transaction
mutations use the resident projection, active segment offset, and retained
append descriptor. Direct and scan-oriented reads use that projection after
the first namespace warm.

There are no known semantic or operational alignment gaps in the supported
exclusive-writer contract. The deliberate differences are limited to Pouch's
C-native binary record and derived query-artifact formats rather than
protobuf/Go `LOGD` bytes, fixed-width `uint64_t` durable sizes and offsets,
and liblockdc-native public C APIs, error handling, and logging. Explicit
shared-root writing is a Pouch-specific, correctness-tested extension beyond
Go disk's single-writer topology; it is deliberately lower-throughput and is
not used as the exclusive-path performance baseline. The source comparison
does not claim Go/Pouch byte compatibility or interchangeable public storage
APIs.

## Go Disk Alignment Contract

This section is a mandatory implementation checklist. Every item is either
copied from Go disk semantics or is an explicitly accepted Pouch divergence.
Broad implementation resemblance is not alignment. Pouch is aligned only when
the same storage semantics, failure behavior, metadata authority, transaction
participant model, compaction safety properties, and exclusive-writer
operational intent are preserved. C-native representation choices are allowed
only when this document names them and explains why the Go disk property is
still preserved.

Pouch has no Go-disk compatibility contract. It must not keep Go record
readers or Go/C format dispatch. Before exposing an opened Pouch handle, it
automatically migrates every supported historical Pouch binary control record
to the current layout under root mutation authority; normal runtime readers
then accept only the current layout. Numeric rolling segments are the sole
Pouch segment format.

Lease renewal preserves the higher of the committed body version and the
version snapshot retained in lease metadata. Historical extended lease records
can retain a higher snapshot; immediate writes can advance the body beyond that
snapshot. An existing transaction-local stage supplies its own visible version,
including version zero after a staged deletion. Renewal changes lease metadata
while preserving the body and its CAS version.

### Storage API Boundary

Go disk exposes the storage backend in terms of `(namespace, key)`. The
namespace selects a per-namespace logstore. The key is relative within that
namespace. Go disk does not encode user namespace names into object keys under
a global storage namespace for queue, attachments, leases, or namespace
configuration.

Pouch must preserve that boundary:

- namespace selection happens before segment/snapshot/projection lookup;
- per-namespace manifests, segments, snapshots, locks, and projections
  are independent;
- user namespace surfaces never move to a global `.lockd/*` durable namespace;
- if a logical object is internal, it is hidden by metadata and key policy
  inside the caller namespace, not hidden by being stored globally.

Rejected Pouch layout:

- `.lockd/queue`;
- `.lockd/attachments`;
- `.lockd/leases`;
- `.lockd/namespace-config`.

Reserved namespaces are true control stores, rather than relocated user data.
Pouch transaction records use
`.txns`, and decision marker support, when present, uses `.txn-decisions`.
Pouch stores the C-native transaction record at key `<txn_id>` instead of a
protobuf/JSON Go payload; a `txn/` prefix or `.lockd/txn` namespace is rejected.
Transaction-coordinator control records follow Go's `.lockd` control namespace:

- leader lease: `.lockd` / `tc/leader`;
- cluster membership lease: `.lockd` / `tc-cluster/leases/self`;
- RM membership registry: `.lockd` / `tc-rm-members`.

The C cluster API currently has no explicit Go-style node identity or
membership TTL field, so Pouch maps the single local membership surface to the
reserved identity `self` and stores no cluster expiry. That is an accepted
public-API divergence from Go's internal `tccluster.Store`, not permission to
use Pouch-only `.lockd/tc-cluster` namespaces. Pouch stores C-native binary
control values at these keys; Go stores JSON/protobuf-supported values. Any
additional Pouch-only reserved control store must be named here before
implementation.

PostgreSQL XA uses `.xa` for external branch descriptors and resource
registration. A branch descriptor uses `branch/<txn_id>/<branch_id>` and a bounded
`LPXB` binary record with native identity, phase and completion evidence. It
contains no SQL or credentials. The existing `.txns` participant/vote record
remains the sole global decision journal; an external prepare-intent vote seals
enrollment without voting YES. The application gate checks durable external
completion before publishing Pouch changes. `.xa` and its child namespaces are
reserved from public state/lease access. See [the XA contract](xa.md).

### Logstore Records

Go disk record families are:

- meta put;
- meta delete;
- state put;
- state delete;
- object put;
- object delete;
- state link.

Pouch may use different C enum values and a different binary header, but it
must preserve those logical families. Flattening all features into ordinary
state rows is not accepted when it loses object/meta/state distinction,
metadata hot-path behavior, or compaction semantics.

Object records are mandatory. Queue message payloads, queue message metadata
objects, attachments, staged attachments, and every Pouch operation that maps to
Go disk `PutObject`/`GetObject`/`DeleteObject` semantics must replay and compact
as object put/delete records, not as ordinary state put/delete records hidden by
key policy. This keeps state and object generation, metadata, visibility, and
compaction behavior separate in the same way Go disk does.

Go disk record metadata carries:

- generation;
- modified timestamp;
- etag;
- content type for object records;
- transform descriptor bytes;
- plaintext size;
- cipher or stored size where relevant;
- payload CRC in the physical header.

Pouch must carry the same logical facts in C-native binary metadata and refs.
Durable generations and index high-water values use
`lc_pouch_generation` (`uint64_t`); durable Unix timestamps use
`lc_pouch_unix_seconds` (`int64_t`). The generic C API follows Go's public
boundary with `lc_version` (`int64_t`) for object versions,
`lc_unix_seconds` (`int64_t`) for timestamps, and `lc_index_seq` (`uint64_t`)
for query/index sequences. Transaction-coordinator terms use `lc_tc_term`
(`uint64_t`), and queue enqueue ordering tokens remain `uint64_t` across their
binary encode/decode path. A physical generation is range-checked before it is
exposed as an API version, preserving Go's signed public version contract
without narrowing the durable counter on ILP32.

Timestamp width is invariant across response channels: Pouch durable metadata,
transport JSON fields, and streamed attachment timestamp headers all preserve
the full signed `lc_unix_seconds` range. They never use an architecture-width
`long` intermediary.

Pouch additionally uses `uint64_t` payload lengths and offsets. This is an
accepted divergence from Go's 32-bit physical payload length because Pouch's
large-payload/file-size invariant is stronger.

The generic C API retains its established `long` byte-count fields for update,
mutate, attachment, and queue-enqueue responses. Pouch client update and
mutate operations reject a streamed state write before it commits when its
payload would exceed `LONG_MAX` on the calling architecture. Pouch and the
remote transport reject attachment sizes and enqueue payload or visibility
metadata outside the public `long` range. Remote values are protocol errors;
Pouch rejects impossible public results. No path narrows a durable `uint64_t`
count into an incorrect public result.

All authoritative Pouch storage identities that may outlive a process are
also fixed-width: numeric segment and snapshot identities,
queue-notification sequences, and namespace index high-water values are
`uint64_t`. A 20-digit decimal filename or marker field is therefore never a
machine-word counter.

Pouch must not use protobuf for durable metadata. Pouch must not use Go's
`LOGD` byte format. Those are accepted divergences; the semantic payload of the
metadata is not optional.

### Per-Key Metadata And Leases

Go disk stores active lease state in `storage.Meta` for the target key. The
lease fields are part of metadata mutation and are persisted through
`StoreMeta(namespace, key, meta, expectedETag)`. Important fields include:

- `Lease.ID`;
- `Lease.Owner`;
- `Lease.ExpiresAtUnix`;
- `Lease.FencingToken`;
- `Lease.TxnID`;
- `Lease.TxnExplicit`;
- key `FencingToken`;
- staged transaction fields;
- state etag/version/published version;
- state descriptor and plaintext byte count;
- attachment metadata.

Pouch must do the same at the logical model level. The active lease for
`(namespace, key)` is stored with that key's metadata in that namespace.
A separate global lease table keyed by encoded namespace/key is rejected.

Implementation obligations:

- acquire reads the target key metadata, validates expiry/if-not-exists, mints
  lease id/txn id/fencing token, and writes updated target-key metadata;
- keepalive reads and updates the same metadata record;
- release reads the same metadata, validates lease id/txn id/fencing token,
  commits or rolls back staged fields, clears the lease, and writes the same
  target metadata record;
- acquire, keepalive, release's lease-metadata replacement, and every queue
  delivery lease claim, renewal, or clear evaluate current metadata and publish
  their replacement while holding that exact key's mutation authority. A
  separate read followed by a later key mutation is not permitted because it
  can grant conflicting shared-root leases when a metadata-only record has
  generation zero, renew an expired or replaced delivery lease, or clear a
  newer delivery lease;
- update/mutate, metadata, and delete validate the target metadata lease once
  from the resident projection while exact-key mutation authority is held,
  before staging or writing state. They do not pre-read the lease and then
  validate it again in the mutation path;
- every public state or attachment mutation requires a non-empty lease id and
  its matching fencing token. A missing lease reference is an invalid request,
  never an unfenced mutation request;
- attachment upload, delete, and delete-all validate the parent key's lease
  while namespace mutation authority encloses both that validation and the
  attachment object/metadata mutation. A preflight lease read followed by a
  later attachment write is not valid because expiry, release, or takeover can
  occur between those steps;
- metadata CAS must use etag/generation behavior matching Go disk for missing
  keys, stale etags, pending same-group writes, and conflicting pending writes;
- projections cache hot lease and summary fields so lock paths do not parse
  state JSON.

If a C helper struct internally separates hot lease fields from other metadata,
that is an in-memory organization detail only. It must replay from target-key
metadata records and must not be a separate durable namespace.

Pouch encodes active lease fields as a C-native side-metadata blob attached to
the target key metadata record. A target with no state payload may still have a
metadata-only record so acquire, keepalive, and release can preserve fencing
and expiry without creating a public document. The placeholder is query-hidden
only while it has no body: its first body write, including a transaction-staged
write promoted on commit, is query-visible unless the body itself explicitly
sets query-hidden metadata. Body reads treat metadata-only records as
no-content; metadata reads return the lease metadata.

Metadata-only rows do not consume the public state generation for the key. A
live body write after a missing/tombstoned/metadata-only row starts at public
generation `1`, and subsequent live body writes/deletes advance from the
current live body generation. Lease metadata CAS is lease-record scoped: the
first lease write for a target with no lease metadata observes expected lease
version `0` even when the target already has a body, while keepalive/release
validate the stored lease metadata version.

Pouch also keeps a namespace log high-water sequence separate from public
per-key generations. Every appended namespace log record advances the
high-water sequence used by query-index incremental flush, transaction
correlation ids, projection refresh, and compaction validation. Derived indexes
must filter changed records by this log/index sequence, not by public state
generation, because metadata-only records and independent keys may share public
generation values.

Every state, object, staged-link, delete, and decision record ends with a fixed
binary index trailer containing a magic and the `uint64_t` sequence. The
sequence allocator is durable and namespace-scoped, and serializes the short
reservation path across processes and separate Pouch handles in one process.
A complete record without that trailer is rejected as corruption; replay never
synthesizes ordering from physical file position. The high-water control record
carries its sequence as its sole generation field.

Snapshots retain the legacy `[LCSI][state-sequence]` trailer and append a
second `[LCSQ][query-sequence]` trailer for each state or delete record. This
unambiguous 24-byte tail preserves the distinction between a document mutation
and a later lease-only metadata mutation across compaction and reopen:
query-index flushes do not republish metadata-only changes. Replay continues to
accept earlier snapshot trailers without that extension and derives their
per-record query freshness conservatively; newly written snapshots always use
the appended form. The `LCSQ` marker is valid only after a complete legacy
`LCSI` trailer, so caller metadata cannot be mistaken for snapshot freshness
metadata.

### State Records

Go disk stores JSON state payloads as state records under the caller namespace
and relative key. `WriteState` performs key normalization, per-key locking,
refresh, pending/CAS checks, generation increment, streaming append, etag
generation, byte counting, and projection publication after commit.

Pouch obligations:

- state payloads are segment/snapshot payload spans, never external payload
  files;
- writes stream from caller reader through transforms and hashing;
- expected-etag and if-not-exists behavior match Go disk outcomes;
- generation increments from the current live state ref;
- read returns metadata from projections without opening payload bytes;
- payload readers are bounded to the recorded span;
- delete appends a state delete/tombstone record and advances generation.

### Object Records

Go disk uses object records for non-state payloads: attachments, queue payloads,
queue metadata objects, namespace config, transaction payloads, and other
object-like data. Object records carry content type, etag, descriptor,
plaintext size, and generation.

Pouch may encode object-class records in its state/logstore module, but it must
preserve object semantics:

- object keys are relative to the caller namespace;
- list order is lexical by key;
- `GetObject` streams a bounded payload span;
- `PutObject` supports CAS/if-not-exists and content type;
- `DeleteObject` supports CAS/ignore-not-found;
- object metadata is available without payload reads;
- object rows used by higher-level internal features are query-hidden.

### Attachments

Go disk attachment helpers define exact relative object keys:

- committed: `state/<key>/attachments/<attachment_id>`;
- staged: `state/<key>/.staging/<txn_id>/attachments/<attachment_id>`;
- committed prefix: `state/<key>/attachments/`;
- staged prefix: `state/<key>/.staging/<txn_id>/attachments/`.

Pouch must use this namespace-local key model. The attachment id may remain a
Pouch-generated id if public liblockdc APIs require that, but the durable
relative key must live under the Go-shaped attachment prefix.

Implementation obligations:

- attachment payload writes call the namespace logstore with the caller
  namespace;
- committed attachment metadata is stored in the target key metadata;
- staged attachment metadata is stored in the target key staged metadata;
- staged attachment promotion/discard follows release/transaction outcome;
- attachment creation time is preserved in compact attachment-owned metadata,
  while the object record's persisted update timestamp is the authority for
  `updated_at_unix`. Missing or malformed attachment-owned metadata is a
  corrupt Pouch record, not a legacy layout to infer;
- attachment bytes are exact on readback;
- attachment rows are hidden from public state scans/query/index/get-public.

### Queue

Go disk queue relative keys are:

- queue base: `q/<queue>`;
- message metadata: `q/<queue>/msg/<id>.pb`;
- message payload: `q/<queue>/msg/<id>.bin`;
- outbox state: `q/<queue>/state/<id>.json`;
- DLQ metadata: `q/<queue>/dlq/msg/<id>.pb`;
- DLQ payload: `q/<queue>/dlq/msg/<id>.bin`;
- DLQ state: `q/<queue>/dlq/state/<id>.json`.

Go queue lease keys use relative keys without file extensions:

- message lease: `q/<queue>/msg/<id>`;
- outbox state lease: `q/<queue>/state/<id>`.

Pouch must use the same namespace-local layout family. Because Pouch does not
use protobuf, the C-native queue metadata object key is
`q/<queue>/msg/<id>.meta` instead of Go's `.pb`. Payload and outbox state
keys stay Go-shaped: `q/<queue>/msg/<id>.bin` and
`q/<queue>/state/<id>.json`. Message and state lease metadata keys remain
extensionless: `q/<queue>/msg/<id>` and `q/<queue>/state/<id>`.

Queue key material must remain parseable by the same logical rules as Go
`queue.ParseMessageLeaseKey` and `queue.ParseStateLeaseKey`. A Pouch-internal
encoding that prevents transaction participant code from pairing
`q/<queue>/msg/<id>` and `q/<queue>/state/<id>` is not an accepted divergence.
If Pouch sanitizes user-provided queue names, the stored key still has to be
the normalized logical queue name and message id, not an unrelated hex encoding
that changes participant semantics.

Queue message leases are target-key metadata. The message document may carry
delivery fields for API responses and CAS validation, but the authoritative
lease, fencing token, transaction id, and lease expiry are stored and mutated on
metadata key `q/<queue>/msg/<id>`. Outbox state leases use metadata key
`q/<queue>/state/<id>`. Transaction commit/rollback must validate and clear
those metadata leases the way Go disk does; scanning staged queue rows is not a
substitute for participant semantics.

Single-message and batch dequeue use the same target-key lease acquisition
semantics. A delivered message is not valid unless the lease metadata write has
advanced the fencing token on `q/<queue>/msg/<id>` and the returned message
carries that token for ack, nack, extend, and transaction validation.

Pouch transaction application routes queue participants by exact lease-key
shape. `q/<queue>/msg/<id>` applies the staged `.meta` queue message decision
and clears message lease metadata. `q/<queue>/state/<id>` applies outbox
state cleanup against `q/<queue>/state/<id>.json` and clears state lease
metadata. A `.meta`, `.bin`, `.json`, staging, or scan-discovered key is not a
queue transaction participant.

The queue payload object is written as its own logstore object before the
metadata object. The `.meta` record contains the C-native queue header and the
counted payload length; it does not concatenate queue payload bytes. Reads
derive the payload object key from the `.meta` key by replacing `.meta` with
`.bin`.

Queue enqueue obligations:

- normalize namespace and queue name;
- generate a message id;
- write the payload object in the caller namespace first;
- count payload bytes while streaming;
- build queue message metadata with queue, id, enqueue time, updated time,
  attempts, failure attempts, visibility, TTL, attributes, content type,
  descriptors, correlation id, and lease fields;
- write metadata object in the caller namespace second;
- if metadata write fails, delete the payload object;
- update ready cache or equivalent notification state only after metadata is
  durable;
- notify watchers through namespace/queue scoped notification behavior.

Queue delivery obligations:

- candidate ordering is by enqueue time and deterministic sequence/id
  tie-breaker, not by payload content;
- visibility timeout, TTL, max attempts, attempts, failure attempts, last
  error, and DLQ behavior match the supported Go queue behavior;
- ack/nack/extend validate active lease id, fencing token, txn id, status, and
  expiry;
- extend persists the message metadata CAS decision before renewing the
  message lease and then the optional state lease. These are distinct durable
  metadata updates, so a later lease validation failure may leave the earlier
  message visibility update, matching Go disk's write ordering; Pouch must not
  renew a delivery lease before the message CAS succeeds;
- non-transactional ack deletes queue state when a state lease/state etag is
  present, deletes message metadata, deletes message payload, clears message
  lease metadata, and clears state lease metadata; it must not leave an
  authoritative `acked` message metadata row behind;
- transaction commit of a staged queue ack deletes message metadata and payload
  before clearing message lease metadata; transaction rollback discards the
  staged ack and makes the message visible again;
- before a direct transaction decision changes a queue participant, Pouch holds
  namespace mutation authority and verifies that the current message or state
  lease still belongs to that transaction. A direct decision rejects a newer
  transaction's lease rather than deleting its queue document or clearing its
  metadata. Replay and startup recovery instead skip that stale participant,
  preserving the newer lease while continuing the durable decision for the
  remaining participants, matching Go disk's idempotent replay behavior;
- a dequeue whose durable claim succeeds but whose queue-record update or
  public message-handle construction fails clears that message lease before it
  returns an error. Once the delivery record is published, Pouch also restores
  it to available state and reverses the unobserved attempt;
- `dequeue_batch` applies the same rule to every message it claimed. If any
  member cannot be transferred into the returned batch, Pouch restores that
  member and every earlier unreturned batch member before returning the error;
- stateful queue operations acquire paired message and state leases;
- `dequeue_with_state` is all-or-nothing from the caller's perspective. If
  state-lease setup or message-handle construction fails after a message lease
  is durable, Pouch directly restores the message's available queue record and
  clears the message and any newly acquired state lease before returning the
  error. This rollback is direct even for a transaction-tagged request because
  no message handle escaped to let a transaction resolve a partial delivery;
- transaction marker application pairs `q/<queue>/msg/<id>` and
  `q/<queue>/state/<id>` the same way Go `queue.ParseMessageLeaseKey` and
  `ParseStateLeaseKey` do.

Pouch uses a C-native transient `acked` status only as a staged transaction
decision marker before commit. It is not a durable terminal message state after
non-transactional ack or after transaction commit. Go disk has no durable
`acked` queue metadata row; it deletes the message objects.

### Namespace Configuration

Go disk namespace config is an object in the configured namespace:

- key: `config/namespace.pb`;
- missing object returns the default config;
- load/save use the backend object API;
- save uses expected etag/CAS;
- cache TTL is short and invalidated on save;
- crypto encrypts metadata when enabled.

Pouch must store namespace config in the configured namespace, not in a global
namespace. The Pouch key is `config/namespace` unless a C-native suffix is
explicitly selected before implementation. The object is internal/query-hidden.
Pouch returns the config object's etag on get/update and enforces
`lc_namespace_config_req.if_etag` on update with the same stale-etag failure
shape as other Pouch CAS writes. The missing-config default response returns an
empty etag.

### Staging

Go disk staged state key shape is `<key>/.staging/<txn>`.
Promotion:

- locks destination and staged keys in sorted order;
- refreshes namespace projections;
- verifies destination CAS or if-not-exists;
- verifies staged ref exists and is not deleted;
- appends a state link at the destination pointing at the staged payload span;
- appends a state delete for the staged key;
- returns staged etag, descriptor, and plaintext bytes without copying payload.

Pouch must preserve this. For staged attachments, use the Go attachment helper
shape `state/<key>/.staging/<txn>/attachments/<id>`, not the generic staged
state key helper unless the resulting key is exactly the same.

A transaction-bound public update or mutate takes namespace mutation authority
before it reads the committed key metadata. Within that one authority window it
validates the lease, reads any existing staged metadata, derives the staged
CAS/query-visibility decision, and appends the staged record. It must not
perform a client-side lease pre-read and then re-read the lease from an
unrelated staging callback. This wider namespace boundary is deliberate: the
committed key and its `<key>/.staging/<txn>` companion form one transaction
decision even though ordinary non-transactional writes retain exact-key
concurrency.

The state layer owns that full stage decision. It passes the resolved staged
projection and manifest directly to its append path, so the append does not
perform a second staged-key lookup after CAS and lease validation. Client code
may provide lease-policy validation only; it must not read Pouch state to
prepare a staged write. This includes the local staging scope used by
`acquire_for_update`.

A stateful queue delivery is the deliberate distinct-key exception. Its lease
metadata lives at `q/<queue>/state/<id>.lease`, while the state body lives at
`q/<queue>/state/<id>`. A transaction-bound update reads both records under
the same namespace authority: the lease record authorizes the caller, and the
state record plus its staged companion determine CAS and query visibility. It
must not treat the empty state object as a missing lease or use the lease
record as the staged object precondition.

### Transactions

Go disk uses reserved control namespaces for transaction records/decision
markers, but transaction participants are ordinary `(namespace, key)` pairs.
Applying a decision mutates the participant's namespace-local metadata/state.
Queue participant lease markers are recognized by the queue message/state key
shape and applied to the paired key.

A nonempty participant backend hash identifies one resource manager. Pouch
applies that participant only when it equals the root's persisted
`.lockd/backend-id`; a foreign participant remains staged for delivery to its
own resource manager. An empty hash is the compatibility wildcard and is
applied locally. Decision records retain every participant regardless of its
backend so the coordinator can deliver the same durable decision to each
resource manager. Pouch trims outer whitespace from participant hashes and
rejects a nonempty value that becomes empty, matching Go lockd's transport
validation.

A nonempty decision `target_backend_hash` scopes the whole decision to one
receiving resource manager. Pouch trims its outer whitespace and requires it
to equal the receiving root's persisted `.lockd/backend-id` before creating,
merging, or applying the record. An empty target is deliberately unscoped for
compatibility. Replay and open-time recovery revalidate persisted targets;
they reject a foreign or malformed scoped record without applying or deleting
it. This preserves coordinator routing rather than treating a durable decision
from another resource manager as local work.

Pouch obligations:

- keep transaction decision/control records only in documented control
  namespaces: `.txns` for transaction records and `.txn-decisions` for
  decision markers;
- do not use transaction control namespaces to hide queue or attachment data;
- transaction commit/rollback mutates participant metadata in participant
  namespaces;
- queue message/state paired marker application is preserved;
- transaction records containing production data are transformed at rest on
  crypto/compression roots.

`txn_replay` parses and applies the stored participant list, not caller-supplied
participant data. A durable `commit` or `rollback` is reapplied idempotently.
An expired `prepare` is first converted to a durable `rollback`, then applied.
Non-expired prepares are reported without changing participants. Queue lease
mismatches during replay or startup recovery mean that a newer delivery has
superseded the old participant: Pouch skips only that participant and does not
delete or clear the newer queue state.

`txn_prepare`, `txn_commit`, and `txn_rollback` read and update the transaction
control record while holding that record's exact key mutation authority. Each
request merges new participants into the durable participant set, retains the
later expiry, requires a monotonic TC term, and applies the resulting durable
decision. A direct decision must never replace an already-prepared participant
list with the smaller list supplied by a later request.

Generic state participants execute under the namespace mutation authority.
Pouch applies their staged state and attachments, then clears the matching
transaction lease before releasing that authority. A current active lease with
another transaction id supersedes the old participant, so Pouch skips the state
and attachment decision without disturbing that newer lease. Consequently, a
lease used by a successful transaction decision is no longer releasable;
callers close its local handle rather than issuing a second release.

### Compaction

Go disk compaction captures live refs from per-namespace projections, excludes
its active segment, protects live links into candidate files, streams payload
spans into a temp snapshot, validates captured refs before install, renames the
snapshot, updates manifest state, and deletes obsolete files only after grace
and live-ref checks. Pouch has the same active-segment exclusion: the rolling
active segment is never a candidate.

Obsolete cleanup removes each deleted segment from the current manifest's
candidate view immediately, including after an earlier unlink in the same
batch succeeded. It synchronizes deletions once per affected directory before
retiring their obsolete markers. An unlink or directory-sync failure leaves
markers available for retry; already missing obsolete files are handled
idempotently. A missing live compaction candidate remains an error. Candidate
open failures retain the filename, numeric errno and capture/validation phase
in the SDK error detail and maintenance/background logs.

Pouch must compact with the same lifecycle. It must not compact by dumping an
entire cache, reparsing payload JSON, or materializing large values. Object,
state, metadata, queue, attachment, transaction, and lease metadata refs must
all be represented in capture/validation.

Pouch cache entries retain the current log record container and record offset
separately from payload spans. Compaction candidate selection uses the installed
snapshot plus sealed non-obsolete segments, then filters protected live-link
targets before applying thresholds. Snapshot emission is limited to captured
current refs whose record location is inside the filtered candidate set, and
validation rechecks those exact refs before install. Whole-cache snapshot dumps,
file fingerprints as the only validation authority, and range-obsoleting all
segments through a maximum id are rejected.

The [ordered collection contract](collections.md) defines an isolated Pouch
journal extension. Collection catalogs live under
`namespaces/<escaped-namespace>/collections/names/`, and each collection ID owns
its canonical records and projections under `collections/data/<collection-id>/`.
Whole-root backup includes these partitions and the coalesced namespace work
references under `collection-work/`. Ordinary document export enumerates the
ordinary state projection; collection export uses its ordered read API.

Collection compaction covers the complete captured position projection,
including tombstones, and merges its newer canonical tail before publication.
The snapshot and each derived projection carry a matching topology identity.
This provides ownership proof for the collection's sealed input range while
preserving permanent positions and append retry bindings. Raw payload spans
retain their compressed/encrypted bytes and immutable crypto context. Snapshot
generation pins protect active reads; per-file first-obsolete timestamps keep
the root's grace policy stable across continuing compactions. The existing root
maintenance worker discovers coalesced collection work and reclaims dropped or
unfinished partitions after their session owners have gone.

Collection entries use ordinary canonical state retention metadata and
`STATE_META` payload-span references for body-preserving metadata changes.
Collection expiry removes the parent and its remaining entries through catalog
authority. Entry expiry leaves permanent position gaps and retry tombstones.
Both run on the existing maintenance cadence; journals without entry retention
skip expiry traversal through a conservative derived presence hint. A metadata
change during a compaction copy defers that replacement, preserving its input
dependencies until a later pass can publish a consistent topology.

### Query And Scan Visibility

Go disk keeps internal object surfaces out of public state/query results
through storage API boundaries and metadata/query-exclusion behavior. Pouch's
single C logstore representation must preserve that result:

- internal queue rows are hidden;
- attachment object rows are hidden;
- namespace config rows are hidden;
- staged rows are hidden;
- transaction control rows are hidden from user namespace scans;
- lease metadata is metadata, not a query-visible state document.

Hidden means all of the following:

- skipped by state scans before payload open;
- excluded from indexed document tables;
- excluded from exact/presence/range/text/trigram/temporal/full-text postings;
- excluded from get-public;
- rejected or protected against direct public state-key collision where a user
  key could address an internal row.

Direct public state APIs must reject every `q/` key and every
`state/<key>/attachments/<id>` attachment-object key shape. Queue state lease
keys such as `q/<queue>/state/<id>` are accepted only through internal
lease-ref validation for queue state operations, not as public user document
keys. Lease acquisition is the exception: it accepts an exact queue-state
lease key and defaults its metadata to query-hidden unless that key already
has an explicit visibility preference, matching Go's `ForceQueryHidden` path.

## Divergence Register

Each difference from Go disk is classified below as representation/local
implementation, operational/public API behavior, or a removed/rejected
divergence.

### Resolved Exclusive Hot-Path Gap

The rejected pre-cutover path opened and parsed the namespace manifest,
checked or repaired the active tail, opened and closed the active segment, and
followed shared-root append coordination for each ordinary mutation. That is
not used by a healthy default-exclusive namespace. Its stable
`namespace_logstore` owns the projection, logical-key index, active segment
identity and offset, reusable append descriptor, namespace append gate, and
metadata worker.

While the namespace projection and append gate are held, exclusive mutations
use a read-only borrowed manifest view of that owner instead of allocating a
copy or rereading storage. A durable manifest mutator rejects such a view; the
caller must first materialize an owned manifest at rotation, recovery,
maintenance, takeover, or I/O invalidation. Public read results remain
independent copies before releasing projection coordination. This preserves the
Go-disk resident-logstore property without exposing Pouch runtime ownership in
the public API or durable format.

### Representation And Local Implementation

- Physical record bytes:
  Go disk uses `LOGD` records. Pouch uses Pouch-specific binary record magic,
  C structs, and Pouch file naming. Reason: Pouch is a separate C storage
  engine and must not imply byte compatibility with Go disk.

- Metadata encoding:
  Go disk serializes rich metadata payloads through Go/protobuf-supported
  structures. Pouch uses C-native binary metadata. Reason: liblockdc does not
  use protobuf for pouch, and binary metadata is faster and easier to fuzz in C.

- Payload length width:
  Go disk's physical record header uses a smaller payload length field. Pouch
  uses `uint64_t` stored payload length, plaintext length, stored length,
  record offset, payload offset, and compaction byte accounting. Local `off_t`
  conversions are range-checked. Reason: Pouch must preserve large-payload and
  large-file invariants.

- Physical layout and manifest protocol:
  Go keeps a namespace at `<root>/<namespace>/logstore/` with an append-only
  manifest log. Pouch keeps escaped namespace directories below `namespaces/`
  and uses a root and per-namespace text manifest. Its active file is
  `seg-<20-digit-u64-id>.log`, and snapshots use numeric ids. The manifest
  records the active segment, installed snapshot, and obsolete files. A short
  append gate serializes physical writes and rotation; it does not widen
  logical-key ownership. Reason: Pouch owns its recovery format and does not
  promise cross-engine file interoperability.

- Query-index artifact format:
  Go disk query/index internals are Go-native. Pouch uses C-native packed
  derived artifacts. Reason: indexes are derived from the authoritative
  logstore and may use a local format as long as query semantics, rebuild, and
  performance intent are preserved. Pouch writes `LPITGEN1` term-generation
  artifacts at format version 2: a sorted field dictionary is emitted once per
  generation and terms refer to it by id; normalized three-byte trigrams are
  encoded as three bytes rather than six hex characters. Version 1 artifacts,
  which repeat the field and store every value as hex text, remain readable.
  A failed or interrupted artifact upgrade is harmless because every index
  artifact is derived and rebuilt from the authoritative state log.

- TC cluster identity and TTL API:
  Go's internal TC cluster store takes an explicit identity and TTL. The current
  liblockdc Pouch client API exposes only `self_endpoint` for cluster announce
  and no TTL, so Pouch persists the singleton membership at
  namespace `.lockd`, key `tc-cluster/leases/self`, with no expiry. Reason:
  preserving the public C API avoids inventing a hidden identity source while
  still using Go's control namespace/key topology.

- Queue DLQ timing:
  Go queue moves max-attempt messages to DLQ when the ready cache observes a
  descriptor. Pouch has no Go ready-cache worker, so non-transactional terminal
  failure nacks synchronously move message metadata, payload, and outbox
  state to the DLQ keys. Reason: this preserves the same durable DLQ end state
  without adding a background cache layer; callers observe the terminal message
  removed from the live queue immediately.

- Namespace config cache:
  Go wraps namespace config loads in a short TTL cache above the storage
  backend. Pouch reads the namespace-local config object through its in-process
  logstore projection and does not add a separate config TTL cache. Reason: the
  Pouch projection already keeps the hot object metadata in memory, while
  preserving the same durable object, default-on-missing behavior, and update
  CAS semantics.

- Public names and diagnostics:
  Pouch uses Pouch terminology in files, errors, events, and durable metadata.
  Reason: Pouch is not disk and must not expose Go disk identity.

### Runtime Contract And Cutover Status

- Exclusive writer is the post-cutover default contract:
  a successful ordinary Pouch open will own one logical writer for the root. It
  establishes root ownership, then initializes each namespace's resident state
  lazily on first use. The initialized namespace keeps a projection, active
  segment identity and offset, reusable append descriptor, and bounded
  append/commit pipeline until close, abort, takeover, rotation, maintenance,
  or I/O invalidation. An exclusive open that cannot obtain ownership fails
  with an actionable error and must not silently use shared-root behavior.
  `lc_pouch_abort` stops Pouch-owned workers and releases the process-bound
  root lock as a crash would. Before that release it closes resident append and
  source descriptors, bounded authenticated body-cache entries, and clears
  derived query state, while retaining the
  durable heartbeat marker as takeover evidence until it expires. An explicit
  shared-root abort can reopen immediately and replays the last finalized
  record; an exclusive-root abort instead requires clean handoff or heartbeat
  expiry before another exclusive writer opens. A resident source descriptor is
  an open-file cache only: every returned body source reads its payload span by
  absolute offset, so overlapping plaintext, compressed, and encrypted streams
  remain independent even when they originate in the same segment.

- Post-cutover default HA is active/passive:
  one process owns the root; another process takes over only after clean
  handoff or validated expiry/failure. It is not active-active writing. Go
  disk's public storage backend likewise has one append owner.

- Shared-root capability is an explicit post-cutover contract:
  callers that intentionally place two or more active Pouch instances on one
  root will select shared-writer mode. Each process retains its local
  projection, but every cache refresh and durable mutation acquires one
  root-scoped mutation guard before recovery, delta tail, append, publication,
  and requested durability. It covers every participating namespace, including
  `.txns`. A healthy writer does not
  reread or revalidate bytes at or before that verified cursor on each normal
  mutation; recovery, manifest lifecycle invalidation, or a new projection
  performs that historical validation. Shared mode deliberately combines no
  independent namespace, key, or append file locks beneath the root guard:
  that removes cross-process lock-order cycles. It is a correctness-first
  serialization mode; default exclusive mode keeps its concurrent hot path.
  Namespace sequence allocation, the local mode epoch, and maintenance fencing
  remain required. Compound staged decisions retain their atomic 3-record
  promotion or 2-record discard group while holding the same guard. Transaction
  coordination acquires that guard before its per-XID linearization guard. A
  shared handle
  retains a root-wide process read lock for its lifetime; exclusive mode
  requires the conflicting write lock. A live shared writer therefore cannot
  be overtaken, and a crashed writer loses its kernel lock before
  recovery/takeover. Pouch deliberately does not add a second durable writer
  epoch as data authority. This is a supported Pouch extension, not the
  default Go-disk-aligned performance path.
  Direct internal callers set `single_writer_set=1` and `single_writer=0`;
  public clients select the same policy with
  `LC_POUCH_SETTING_SINGLE_WRITER` or the compatible
  `?single_writer=false` endpoint option.

- Mode transitions are lifecycle transitions. Pouch takes a writer-mode
  transition barrier that stops new append-capable operations and waits for
  existing ones through their durable completion before changing root
  ownership. The incremented writer-mode epoch fences an exclusive append
  descriptor immediately; the next projection validation closes it and then
  tails or rebuilds only when the manifest requires it. Runtime mode is never
  inferred from workload and is never encoded in user records. The barrier is
  local to one Pouch handle. Cross-process handoff is fenced by the root
  process lock:
  exclusive mode cannot take ownership while any live shared handle retains its
  read lock, and a crashed handle's kernel lock is released before recovery.

- Fork lifecycle: an open Pouch handle owns pthread workers. A process must
  fork before opening Pouch, or `exec` before the child uses Pouch. Calling a
  Pouch API on an inherited open handle is unsupported because a child retains
  only the calling thread while worker-owned mutexes and condition variables
  may have been held at the fork boundary. This does not restrict separate
  parent and child handles opened after a pre-open fork; shared-root process
  coordination continues to apply to those independent handles.

- Fsync batching and diagnostics:
  Pouch defaults to `durable_sync=0`, the same `NoSync` mutation boundary used
  by Go disk's default `failover` mode. Finalized records are immediately
  visible and replay-safe in either policy. `durable_sync=1` enables Pouch's
  stronger root-scoped `fdatasync` group-commit policy. In that mode,
  `fsync_batch_max_ops` is a `uint64_t` Pouch open setting and a compatible
  `pouch://...?fsync_batch_max_ops=<u64>` endpoint option; zero is unbounded,
  matching Go's `LogstoreCommitMaxOps`. `durable_sync` is available through
  `lc_pouch_settings` and `pouch://...?durable_sync=true`. The worker dispatches
  ready requests without a coalescing timer. Requests arriving during I/O form
  the next group; the count limit is an upper bound, never a minimum to await.
  Each process has one batcher per root: when multiple durable-sync handles are
  live, its effective bound is the lowest nonzero `fsync_batch_max_ops` among
  them (zero contributes no bound). Opening and closing a handle recomputes
  that live-root bound. Every caller waits for its group's shared sync result.
  `lc_pouch_fsync_stats_read` reports fixed-width aggregate batch,
  request, sync-I/O latency, bound, and bucket counters using Go's 1 through 4096
  histogram boundaries; they remain zero when durable sync is disabled.

- Segment sizing:
  `segment_target_bytes` is a fixed-width `uint64_t` direct Pouch open option
  and a `pouch://...?segment_target_bytes=<u64>` endpoint option. Zero selects
  the 64 MiB resolved default. It is a stored-record rolling threshold, so
  compression and encryption can change the segment count for the same input
  document workload.

- Indexer batching:
  `indexer_flush_docs` and `indexer_flush_interval_seconds` are fixed-width
  direct Pouch open options and `pouch://` endpoint options. Zero selects the
  Go disk-store defaults of 2,000 distinct documents and ten seconds. In
  exclusive Pouch, completed public lease releases or transaction decisions
  synchronously publish the bounded, body-free index segment when its final
  ready projection reaches the distinct-document limit and no other pending
  key is still active. This preserves final-version-only publication across
  overlapping leases, matching Go disk's memtable boundary without emitting
  an intermediate in-lease version.
  Repeated updates to one pending key do not advance that document budget.
  The exclusive worker never lets the idle deadline publish a body from an
  active public lease or transaction; it retains that batch until the final
  completion boundary, then retries under the normal interval policy. Direct
  storage mutations without a public completion boundary publish at the idle
  deadline or an explicit `flush_index(mode=wait)`. Shared-root handles
  retain asynchronous, namespace-locked durable replay at either bound because
  no handle owns the complete local mutation stream. Index-artifact failure is
  recoverable worker work and never changes the success of an already durable
  state mutation.

- Non-query roots:
  `query_indexing=false` is a root-open setting for append-heavy Pouch roots
  that do not need derived query indexes. It prevents index warming, indexer
  startup, and write-side index extraction or publication. Implicit queries
  use scans even if a namespace's durable preference is `index`; an explicit
  `engine=index` request and `flush_index` fail clearly. The setting is local
  to the Pouch endpoint and does not modify durable namespace configuration.
  Deployments that tune Go disk's index writer may set comparable bounds on
  Pouch, for example
  `pouch://...?indexer_flush_docs=64&indexer_flush_interval_seconds=1`.

- Filesystem capability policy and queue wake-up:
  Pouch detects NFS on Linux and BSD-family targets and exposes both detection
  state and queue-watch status through `lc_pouch_status`. The exclusive writer
  retains its active append descriptor; a shared writer retains descriptors
  only while its ownership/cursor remains valid. Rotation, recovery, takeover,
  maintenance, mode transition, close, and abort invalidate them. The
  `queue_watch` setting and compatible `pouch://...?queue_watch=true` enable
  Linux inotify wake-ups only on a known
  non-NFS filesystem; unsupported or unknown filesystems report polling and
  retain the 100 ms polling fallback.

- Backend lifecycle and identity:
  `lc_pouch_backend_hash` derives a SHA-256 identity from the absolute Pouch
  root, then stores it with create-or-read semantics in the Pouch
  `.lockd/backend-id` control record. The persisted marker keeps the identity
  stable across later root-path changes. Its `pouch|` descriptor intentionally
  differs from Go disk's `disk|` descriptor because the two file formats are
  not interoperable.

- Background compaction defaults:
  Pouch now applies Go disk's resolved defaults when callers leave compaction
  controls unset: enabled, a 30-minute interval, two sealed files, a 64 MiB
  reclaim threshold, a 15-minute deletion grace, and an 8 MiB/s throttle.
  `background_compaction_enabled_set` distinguishes an explicit disable from
  the default, and `compaction_throttling_disabled` explicitly selects an
  unlimited throttle. Typed client settings expose the public enable/throttle
  choices; endpoint options remain a compatibility input and direct Pouch open
  options retain the internal full tuning surface.

- Background compaction scheduling:
  Go disk runs a pass at open and then on a fixed periodic timer. Pouch does
  not compact at open. A successful mutation only coalesces its namespace in a
  fixed-size, root-local work queue; it does not reset a global idle timer or
  perform maintenance I/O. The worker makes one namespace-local attempt per
  configured interval. Continuous successful mutation can therefore add work,
  but cannot postpone an already scheduled turn indefinitely. If the bounded
  queue is full, ordinary compaction work is dropped; terminal work falls back
  to a durable marker for a later turn. This does not affect replay or
  durability; it only delays space reclamation. Callers that require immediate
  reclamation can invoke explicit namespace maintenance.

  Each compaction capture owns one 128 KiB heap buffer, reused for payload-span
  copying and candidate checksums and freed when that capture ends. Preliminary
  candidate scanning and snapshot creation use successive captures, so at most
  one such buffer is live per namespace maintenance operation.
  Compaction streams stored bytes directly and updates CRC incrementally;
  scratch space stays independent of record size, avoids per-record allocation,
  and keeps large buffers off small default pthread stacks, including those in
  fully static musl applications. Allocation failure aborts maintenance without
  publishing a partial snapshot; existing records remain readable and a later
  maintenance attempt can retry.

  Regression tests exercise both the background worker's default thread stack
  and an explicitly constrained stack that respects the running platform's
  minimum. These thread-stack limits are independent of the 128 KiB heap I/O
  buffer; the tests report the configured thread-stack size.

- Terminal retention and reclamation:
  A completed lease, released queue delivery, deleted state, and resolved
  transaction must disappear from the live logical projection immediately.
  Their physical records remain available until an immutable checkpoint covers
  the resulting projection and Pouch can reclaim its obsolete segments.

  `lc_client_new_history_consumer()` adds an explicit durable retention pin for
  a Pouch namespace. Its `(ns, consumer_id)` identity and
  acknowledgement are stored under `.lockd/history-consumers/`; acknowledge
  positions are monotonic and bounded by the current namespace sequence.
  Cursor replacements stage beneath that namespace's private `.staging/`
  directory and become registered only after an atomic rename into the final
  consumer-record directory. A later cursor operation or compaction attempt
  removes abandoned staged writes under the same namespace lock; staging files
  never act as retention pins and cannot corrupt a registered record.
  `LC_HISTORY_CONSUMER_START_AT_CURRENT` registers only future retention.
  The public C receiver has matching `lc_history_consumer_get_position()`,
  `lc_history_consumer_advance()`, `lc_history_consumer_unregister()`, and
  `lc_history_consumer_close()` forms. Closing a local handle preserves its
  pin; only `unregister()` removes it.
  This is the retention substrate for a watcher-resume, replication, backup,
  or PITR implementation, but it intentionally does **not** itself expose a
  chronological history reader or make a remote lockd promise.

  The initial implementation is conservative: while any registered consumer
  is behind the namespace's durable current sequence, compaction does not
  replace that namespace's candidate segments. Once all pins are current (or
  have been unregistered), ordinary compaction can proceed. This is a safe
  whole-compaction barrier, not a fine-grained segment-prefix collector; a
  deliberately slow consumer therefore grows retained disk history. It does
  not introduce a history scan, replay allocation, or consumer-directory I/O
  on normal Pouch open, read, query, or mutation paths. Consumer records are
  inspected only inside the already-selected namespace compaction attempt.

  A consumer must be registered before it relies on historical recovery:
  Pouch cannot recreate data compacted before a pin existed. Corrupt or
  unreadable consumer control records fail that namespace's compaction rather
  than weakening retention.

  Checkpoints retain the monotonic fencing high-water mark even after a lease
  terminal record is reclaimed, so no future acquire can reuse a fencing token.
  Unresolved XA decisions are retained indefinitely. Fully applied managed XA
  histories have separate [coordinator retirement](postgres.md#completed-coordinator-history)
  with a 30-day default and optional count cap; native branch completion alone
  does not make a global history eligible. Outbox envelopes and receipt families
  use independent [terminal retention](pouch-terminal-retention.md) and
  [dead-letter retention](pouch-dead-letter-retention.md). History pins
  conservatively prevent the selected namespace compaction while behind; they
  do not extend these explicit record-retirement policies. Fine-grained consumer
  boundaries for individual record families remain future work.

  Pouch implements this through terminal-reclaim work markers and compaction,
  not a TTL sweeper. Once a terminal transition commits, its namespace is
  coalesced in the bounded worker queue outside the mutation authority. Before
  the worker starts its terminal compaction attempt, it records a best-effort
  durable work marker. The marker is only a liveness hint: losing it cannot
  resurrect a record, weaken a fence, or make an incomplete XA decision
  reclaimable. A later terminal transition recreates it, and an operator can
  always request namespace maintenance explicitly.

  The root-local worker takes at most one queued namespace or marker per pass.
  It never enumerates the root's namespaces, visits every state key, or
  allocates a root-wide namespace snapshot. When the marked namespace has
  accumulated at least the
  configured terminal-reclaim byte threshold in its active segment, the worker
  seals that segment, builds and fsyncs an immutable snapshot, installs the
  snapshot in the manifest, and only then consumes the work marker. A marker
  that finds too little work is likewise consumed; a later terminal transition
  schedules the next bounded attempt. The snapshot contains the current
  logical projection and the durable high-water values;
  terminal tombstones therefore disappear from the new snapshot while their
  fencing and ordering facts remain available. Superseded segment and snapshot
  files still obey `compaction_delete_grace_seconds` before physical unlink.

  This makes work bounded at the root scheduler level: one namespace and one
  compaction attempt per worker turn. Snapshot construction remains bounded by
  the selected segment/snapshot range rather than the root, and its byte
  threshold prevents tiny terminal transitions from causing rewrite churn.
  Continuous application writes do not restart a global idle timer or defer an
  already queued terminal reclaim forever. Normal non-terminal compaction is
  scheduled only for namespaces touched by the current handle and is discarded
  from its in-memory queue after an attempt; it is never reconstructed by an
  open-time namespace scan.

  Per-object state expiry uses `has_retention_seconds`/`retention_seconds` on
  C write options, or optional `retention_seconds` in Lua request tables. New
  objects have no expiry by default; omitted updates preserve the policy,
  explicit zero clears it, and positive durations start at commit. Expired
  objects remain ordinary state until maintenance deletes them. The existing
  worker uses its compaction cadence, with at most 64 parent attempts per
  namespace pass, independently of the threshold for rewriting segments.
  Namespace work references survive reopen, and failed startup discovery arms
  retry at that same cadence. Disabling background compaction disables automatic
  expiry; an explicit namespace maintenance pass can still reclaim due objects.
  Before durably removing an empty retention hint, synchronize the canonical
  namespace containers under the same mutation authority that established no
  expiring state remains. This covers NoSync clears/deletes from earlier or
  independent commit groups, including final idle-work cleanup. Sync failure
  leaves the hint available for retry. Ordinary NoSync writes do not gain a
  per-write sync from this maintenance barrier.
  See [per-object state retention](state-retention.md).

  Neither `retention_seconds` nor `janitor_interval_seconds` is a Pouch endpoint
  control. The explicit
  `lc_pouch_maintenance_options.retention_updated_before_unix` operation
  remains a separate deliberate age-based deletion tool for callers that
  need that distinct policy. It is intentionally opt-in, namespace-scoped, and
  may scan that named namespace; it is not used by normal Pouch initialization
  or background maintenance.

### Remaining Operational And Public API Differences

- Sync policy:
  The Go comparison harness launches one disk-backed server in its default
  `failover` HA mode. Go disk rejects `concurrent` for a disk root and core
  marks `failover` writes `NoSync`; Pouch now does the same by default.
  `durable_sync=1` is an intentional stronger Pouch opt-in for callers that
  require a root-scoped durable group-commit boundary. It is not the default
  benchmark or compatibility policy.
  The separate strict-durability benchmark compares that Pouch opt-in with one
  Go disk server in `auto` HA mode, because Go core uses `NoSync` in both
  `failover` and `single` but not `auto`. `make benchmark-pouch-go-durable`
  reports this equal-sync-boundary production pair, and the optional
  `make benchmark-pouch-go-durable-gate` applies the same documented 1.25x
  core-metric budget. It is deliberately distinct from the default release
  gate, which tests the default compatible `NoSync` modes.

- Raw storage surface and empty staged key:
  Go exposes a generic backend with raw object list/get/put/delete operations
  and permits staging the empty key as `.staging/<txn>`. Pouch exposes
  state-oriented C primitives; object typing is an internal option used by the
  liblockdc queue, attachment, transaction, and configuration paths. Pouch
  requires non-empty state keys, including staged keys. These are public API
  differences, not a change to the namespace-local object semantics used by
  supported client operations.

- Snapshot-only rewrite:
  Go may compact an installed snapshot even when no later sealed segment
  exists, producing a new snapshot identity. Pouch compacts only when at least
  one sealed segment follows the installed snapshot and retains the coverage
  segment id as snapshot identity. Rewriting a snapshot alone has no data or
  durability effect, so Pouch deliberately skips that physical churn.

### Removed Rejected Divergences

The source sweep found no durable use of `.lockd/queue`, `.lockd/attachments`,
`.lockd/leases`, or `.lockd/namespace-config`. Queue, attachment, lease, and
namespace-config data are namespace-local; object rows retain object record
families and query-hidden visibility; and lease hot paths use target-key
metadata rather than parsing user JSON. Reintroducing any of those layouts is
an unaccepted divergence.

Architecture-dependent durable scalar widths are also removed: Pouch no longer
uses `unsigned long` or `long` for persisted generations, index high-water
values, Unix timestamps, TC terms, queue enqueue ordering tokens, writer
identities, or marker/notification sequences, and no longer rejects valid
64-bit generations at an `ULONG_MAX` boundary. The Pouch direct API,
authoritative namespace manifests and records, projection cache, queue/TC
binary records, and generic client boundary use the fixed-width types stated
above. Query-index artifacts remain derived and rebuildable; their in-memory
row counts are bounded by the host's address space rather than defining an
authoritative storage counter.

## Non-Negotiable Model

Pouch stores production data inside namespace log records. It is not a
metadata-only log with separate durable payload files.

Each namespace owns append-only rolling segment files and installed snapshot
files. Every mutation publishes a typed binary record in the active
segment or in a compaction snapshot. Records contain:

- a fixed binary header for physical navigation and integrity;
- a normalized key;
- compact binary type-specific metadata;
- an optional payload span stored in the same segment or snapshot file.

In-memory projections and query indexes are derived accelerators. They are
rebuilt from the durable logstore. They must never become the authoritative
storage format.

There must be no `payloads/` directory for state or object durability. Any
remaining payload-file implementation is rejected code, not an alternate Pouch
format.

## Module Boundaries

The implementation keeps Pouch as a receiver shell over private storage
boundaries. The current C boundaries are:

- `lc_pouch_namespace.[ch]`: namespace lifecycle, root/namespace manifest
  handling, numeric rolling-segment discovery and rotation,
  snapshot install, obsolete cleanup, and repair from discovered
  segments/snapshots.
- `lc_pouch_record.[ch]`: binary record header, validation, CRC, and fuzzable
  decode helpers.
- `lc_pouch_state.c`: the namespace logstore writer/replay/projection core for
  metadata/state/object records, structured refs, scan summaries, staged links,
  and compaction capture/install.
- `lc_pouch_crypto.[ch]`: streaming transform wrappers plus descriptor
  encode/decode for crypto and compression.
- `lc_pouch_query_index.[ch]` and index helpers: derived query/full-text
  artifacts rebuilt from state projections.

Queue, attachment, lease, namespace configuration, and transaction modules call
this storage core through state operations and the same logical keyspace model
as Go lockd disk. They must not parse log files directly or create alternate
durable formats.

Reserved namespaces are not a general escape hatch. Pouch may use reserved
namespaces only for storage-engine control data whose Go reference equivalent
is also out-of-band control data, or where this document explicitly accepts a
C-local control artifact. User-namespace surfaces must remain anchored in the
caller namespace:

- queue message metadata and payload records live under the caller namespace
  with `q/<queue>/msg/<id>`-style keys;
- queue outbox state lives under the caller namespace with
  `q/<queue>/state/<id>`-style keys;
- attachments live under the caller namespace with
  `state/<key>/attachments/<id>` and staged attachment keys under
  `state/<key>/.staging/<txn>/attachments/<id>`;
- namespace configuration lives inside the configured namespace under
  `config/namespace`;
- lease state is metadata for the target logical key in the target namespace,
  not a separate global lease namespace keyed by encoded namespace/key.

Any internal row stored in a user namespace must be explicitly marked
query-hidden and must be excluded from public scans, indexed queries, and
document emission. This preserves Go disk's locality and visibility model
without exposing internal storage rows as user state.

## Logging

Pouch logs through pslog using the liblockdc logging contract in
`docs/logging.md`.

Every Pouch log uses `sys=storage.pouch`, including logstore, manifest, scan,
index, crypto, compression, compaction, queue-backed storage,
attachment/object storage, and maintenance internals. Internal areas are
expressed in event names such as `logstore.write`, `index.flush`, and
`compaction.start`; they must not be represented by changing `sys`.

Pouch logs must use short, readable fields such as `ns`, `key`, `segment`,
`record_offset`, `payload_bytes`, `stored_bytes`, `plaintext_bytes`,
`generation`, `reason`, and `elapsed_ms`. Pouch logs must never include state
JSON, queue payloads, attachment/object bodies, full document text, crypto key
material, transform secrets, or credentials.

## Storage Layout

Representative root layout:

```text
root/
  manifest
  .lockdc-control-migration-v1              # completed control-record migration
  .lockd/
    state-retention/
      <escaped-namespace>/
        work                                # coalesced expiry work / cleanup XID
  namespaces/
    <escaped-namespace>/
      manifest
      segments/
        seg-<20-digit-u64-id>.log          # rolling active or sealed
      snapshots/
        snapshot-<20-digit-u64-id>.log
      sequence                              # durable advisory allocator
      sequence.lock                         # exclusive cold allocator fence
      write.lock
      locks/
      index/
      queue-notify/
  locks/
```

The root `manifest` records the root mode: plaintext, crypto, compression, and
crypto+compression. Encryption policy and key identity cannot change on open.
Omitted compression inherits the stored mode (none for a new root). Explicit
`none` or `zlib` converts the entire root synchronously when exclusive ownership
is available; other active writers cause a warning and use of the stored mode.
Conversion preserves logical identities and retained history, streams payloads
in bounded chunks, retires one source segment or snapshot at a time, and rolls
indexed collection output into final canonical parts. It resumes any
interrupted conversion before opening, even when compression is omitted. It
does not build a second payload tree. See
[compression changes on open](pouch-compression.md) for space and recovery rules.

When a root is first opened by a release that supports historical binary
control records, Pouch completes the required migration before becoming
available and durably writes `.lockdc-control-migration-v1`. Migration writes
and existing replay files are synced before this marker is published, including
records converted by an earlier failed attempt, regardless of the runtime
`durable_sync` setting. If unrelated corruption prevents a complete scan, open
can still succeed, but no completion marker is published; later opens retry.
Later opens with a completion marker retain normal lazy namespace loading.
Both historical `LPL1` lease layouts are supported. Extended records preserve
their encoded state version and explicit-transaction flag; only short records
infer these fields from the backing state and transaction records.

## Binary Record Header

Pouch uses a C-native binary header. It is not `LOGD`, not protobuf, and not a
version lineage for rejected layouts.

Required fields:

- `magic`: fixed Pouch log magic, for example `PCHL`;
- `format`: initial Pouch log format discriminator;
- `type`: record family enum;
- `flags`: record-local flags, currently reserved except for documented
  transform or link bits if needed;
- `key_len`: `uint32_t`;
- `meta_len`: `uint32_t`;
- `stored_payload_len`: `uint64_t`;
- `payload_crc32`: CRC of stored payload bytes after compression/encryption;
- `header_crc32`: CRC32 over the first 28 header bytes.

The header is for physical traversal and corruption detection. Logical facts
belong in record metadata, where replay can inspect them without opening the
payload.

`flags` currently defines `LC_POUCH_RECORD_FLAG_PENDING`. A payload write
appends a pending record to the active segment, streams the payload, rewrites
the final key/metadata while the header remains pending, then publishes the
final header last. Replay never applies a pending record. An active-segment
reader that races a writer stops at the pending or incomplete tail; recovery
defers physical repair to the next appender while it holds the append gate,
then continues from the last complete record. A pending or truncated sealed
segment or snapshot is corruption because those files are complete before they
become immutable or atomically installed.

Record decoding must reject impossible lengths, overflow, unknown record
types, short reads, bad CRC, invalid metadata length, invalid descriptor length,
and invalid link payloads. Crash-tail handling is defined in the replay
section.

## Record Families

Pouch supports the same durable behavior as Go disk through C-local record
families:

- metadata put/delete;
- state put/delete;
- object put/delete;
- state link;
- decision/control records;
- high-water records.

Higher-level Pouch features map onto those families:

- attachments are object records in the caller namespace under
  `state/<key>/attachments/<id>`-style keys;
- queue message metadata and payloads are object records in the caller
  namespace under `q/<queue>/msg/<id>`-style keys, with documented Pouch
  extension choices if C-native records do not use `.pb`/`.bin`;
- queue outbox state is an object record or metadata-backed queue state
  record in the caller namespace under `q/<queue>/state/<id>`-style keys;
- lease state is target-key metadata in the caller namespace;
- transaction decisions, participants, retention markers, tombstones, and
  explicit Pouch control records use reserved namespaces only where the
  reference model also uses reserved control stores or the C-local divergence is
  documented.

Pouch has one segment/snapshot physical container, but it must preserve the
logical record-family distinction that Go disk relies on. Object-like payloads
must not be flattened into user state documents. The accepted C-local
divergence is physical encoding, not loss of meta/state/object semantics and
not logical namespace/key placement.

Queue records persist enqueue seconds, enqueue nanoseconds, and an enqueue
sequence in binary metadata. Dequeue and stats ordering is FIFO by that tuple,
then message id as a deterministic final tie-breaker. Queue scans must not sort
same-second bursts by variable-width textual ids or by payload content.

Do not create text pseudo-records for hot storage facts. Human-readable strings
are allowed for content type, key names, and diagnostics, not as the primary
format for generation, byte counts, refs, queue state, transaction state, or
crypto/compression descriptors.

## Binary Metadata

Metadata is type-specific and binary. Pouch does not use protobuf.

All metadata that affects replay, scan, query, list, CAS, compaction, crypto,
or cleanup must be available without reading or parsing the payload.

Required metadata fields by family:

- state put/link: generation `uint64_t`, modified timestamp, state etag,
  content type or state kind if needed, plaintext byte count `uint64_t`,
  stored byte count `uint64_t`, transform descriptor length and bytes,
  query-hidden flag, staged/internal flag, and any summary fields needed to
  scan without opening hidden rows. A staged-delete decision is this dedicated
  binary flag, never a sentinel content type: caller-provided content types
  remain opaque and are committed unchanged.
- state delete: generation `uint64_t`, modified timestamp, tombstone marker.
- state metadata: generation `uint64_t`, modified timestamp, current etag,
  byte counts, descriptor, content type, and query visibility without replacing
  payload bytes.
- decision/control: generation `uint64_t`, etag where applicable, decision
  code, and high-water sequence data.
- state link: the state put metadata above plus a binary link payload or link
  metadata containing segment/snapshot identity, payload offset `uint64_t`,
  payload length `uint64_t`, and stored-payload CRC or equivalent validation.

Go disk keeps plaintext/cipher sizes, descriptor bytes, generation, modified
time, etag, and content type in metadata because those values are needed by hot
paths. Pouch must preserve that property.

## Payload Refs

Live projections store typed refs, not strings.

A ref contains:

- record family and key;
- segment or snapshot identity;
- record offset `uint64_t`;
- payload offset `uint64_t`;
- stored payload length `uint64_t`;
- plaintext byte count `uint64_t`;
- stored byte count `uint64_t`;
- payload CRC;
- generation, modified timestamp, etag, content type, flags, and descriptor;
- optional link target, represented as a validated structured ref.

String forms such as `container@offset:length` are allowed only for diagnostics
or tests. Durable refs and in-memory refs must be structured and overflow-safe.

Link targets must be restricted to manifested segment/snapshot names. Absolute
paths, traversal, unknown files, stale obsolete files, negative values, and
integer overflow must fail.

## Append And Commit Pipeline

Pouch writes through a rolling per-namespace logstore with a common resident
namespace core. The private `namespace_logstore` is the stable owner for the
default-exclusive state path: a resident normalized logical-key projection,
durable index high-water sequence, active segment and snapshot lifecycle,
active segment identity/offset, bounded source cache, and active append
descriptor. State, leases, objects, attachments, queues, and staged
transactions all reach authoritative storage through that owner; query
artifacts remain derived and rebuildable from its projection. Shared-root,
recovery, takeover, rotation, maintenance, and I/O invalidation deliberately
leave that path to coordinate or rebuild durable state.

Each resident namespace owner also retains its exclusive append gate, bounded
metadata append worker, and shared-mode bounded-body append worker for the
Pouch lifetime. This keeps the projection, descriptor, byte-range gate, and
append queues in one namespace ownership domain. The body worker accepts only
an already bounded SDK memory source and owns no full-document staging buffer.
The process-level namespace guard remains separate because it intentionally
coordinates Pouch handles that resolve to the same root.

The bounded source cache retains at most 64 open segment descriptors. A public
stream receives a duplicate descriptor with its own read cursor, so compaction
or LRU invalidation can close only the cache entry without invalidating an
already returned source. Source-cache lookup, eviction, and invalidation are
serialized independently of state mutation so concurrent reads and maintenance
cannot race the descriptor list.

Required behavior:

- lock the canonical logical key for its full CAS/read/append/publication
  window; staged and destination keys for the same logical key share that
  lock;
- take the shared maintenance barrier while a mutation is in progress;
- reserve one or more `uint64_t` namespace index sequences before final record
  headers are written. Default exclusive mode reserves from its resident
  projection high-water mark and does not read, write, or lock the advisory
  `sequence` file on healthy mutations. Explicit shared-root mode reserves
  under the root durable-mutation guard, so independent processes cannot reuse
  a sequence. A
  normal exclusive close writes a durable per-namespace clean checkpoint after
  pending append syncs. A later cold index read may use that checkpoint instead
  of replaying the namespace. The first subsequent mutation atomically marks
  it invalid before reserving a record, and abort, crash recovery, a missing
  checkpoint, or malformed checkpoint rebuilds from finalized durable records.
  The checkpoint is therefore a performance hint only; it cannot change
  exclusive record ordering;
- in default exclusive mode, read current metadata from the resident
  projection, append through the resident writer, and retain the active segment
  descriptor across normal public operations. It must not rescan the manifest,
  stat/repair the active tail, acquire a cross-process append gate, or reopen
  the segment on a healthy normal mutation. A metadata-only batch whose
  headers, keys, and metadata total at most 256 KiB may use one bounded
  contiguous append buffer; it never contains document, queue-payload, or
  attachment bytes. Larger metadata batches retain the vectored append path;
- in explicit shared-root mode, take one root-scoped durable-mutation guard
  before reading a projection or mutating any namespace. That guard serializes
  the complete recovery/read-or-write/append/publication window across
  processes, including `.txns`; shared mode must not combine independent
  namespace, key, or append file locks with that root guard because they have
  no global acquisition order. This is deliberately a correctness-first
  deployment mode, while the default single-writer mode retains per-key and
  append concurrency;
- shared bounded-memory body writes retain the caller's commit group under the
  root guard and use the normal complete-record path for finalization, index
  reservation, cache publication, and source consumption;
- staged promotion and discard encode their complete decision group into one
  binary append batch. They retain one authority window and are not coalesced
  with unrelated body or metadata requests, preserving transaction order;
- rotate at the stored-byte target under the appropriate writer/maintenance
  coordination, atomically publish the new numeric active segment, and replace
  only the affected active descriptor/cursor;
- append small records to that rolling active file. In default exclusive mode,
  complete metadata-only state mutations (including lease acquire, keepalive,
  release, visibility updates, and queue delivery state) enter one resident,
  bounded append queue per Pouch handle and namespace. Each queue preserves
  submission order within its namespace, processes at most 128 records in one
  append batch,
  publishes each resulting projection entry before its caller is acknowledged,
  and never accepts a payload source. A namespace-scoped compound operation
  already owns stronger namespace authority, so its metadata records execute
  inline on that caller after it has claimed the namespace append gate; it
  never waits for a worker that would need the same authority;
- default-exclusive physical appends use one Pouch-handle/namespace-local gate
  that is separate from resident projection ownership. An exact-key mutation
  takes its maintenance read guard before the root-local namespace projection
  mutex, then captures its precondition from that namespace's projection. This
  order is also used by maintenance (write guard, then namespace projection),
  so recovery cannot invert the two locks. A conflicting exact-key wait
  therefore never occupies projection ownership. The mutation releases the
  namespace projection while it waits at the physical gate, while a bounded
  SDK-memory body transforms, and while a true streaming source writes its
  pending record directly to the active segment. It reacquires projection only
  to reserve the final index sequence, finalize the record, and publish the
  new reference. Exact-key, maintenance, and writer-mode guards remain held
  throughout. This is Pouch's C implementation of Go disk's namespace
  metadata/write-gate split, not a second writer or a buffered payload path.
  The gate is allocated once per Pouch handle and namespace, survives
  projection invalidation until close, and does not perform a root stat,
  shared-mode process registry lookup, or file-lock operation on a healthy
  exclusive mutation. The namespace-logstore registry has its own short-lived
  mutex and does not use shared-root mutation ownership;
- for an SDK-owned `lc_source_from_memory` state or object body no larger than
  64 KiB, Pouch may hash and transform the already-materialized unread range
  in bounded memory, then append one complete finalized record with one
  vectored write. This includes supported zlib and AES-GCM transforms. It is
  an explicit materialized-value optimization, not streaming: callback, file,
  fd, externally implemented, and larger memory sources never enter it;
- stream large payload records directly from the caller-provided reader through
  transforms, hash/etag, and CRC into the active writer file without full
  materialization;
- for streaming records, write a `PENDING` header first, write finalized key
  and metadata while that header remains pending, then publish the finalized
  header last after final stored lengths, descriptor, hash/etag, and CRC are
  known. A materialized complete-record append needs no pending rewrite: a
  partial `writev` is an incomplete active tail because its declared record
  length exceeds the verified file size, so normal tail repair discards it;
- when `durable_sync=1`, group independent commit requests through a
  root-scoped fsync batcher. Direct mutation paths defer active-file syncs
  through duplicate fds held by the current state commit group; the exclusive
  metadata append queue for each namespace owns the same sync boundary in its
  worker, and every waiting public caller drains that result before
  completion;
- make the finalized active-record header the shared-reader publication point;
  shared readers compare the active file size with their verified offset and
  replay only the new complete tail;
- treat an active pending or incomplete tail, including a truncated encrypted
  payload frame, as an unpublished crash tail: readers stop before it and a
  later appender repairs only the unseen suffix after its verified cursor while
  holding the shared root mutation guard (or the exclusive append gate). Sealed
  segments and snapshots do not
  receive this recovery treatment and reject truncation or authentication
  failure;
- make refs visible in projections after finalized-record publication and, when
  `durable_sync=1`, only after the commit group succeeds at the public boundary;
  explicit same-operation staged visibility remains supported;
- make staged/pending refs visible only through explicit same-operation
  promotion paths, matching the supported C-local state-link semantics;
- propagate fsync/write failure to every operation in a durable-sync group;
  otherwise, propagate write and finalization failures. A failed append,
  including a fully encoded metadata batch whose fsync or close then fails,
  truncates its active suffix before releasing append authority. It neither
  replaces a previously published projection entry nor becomes visible after
  replay.

The public mutation completion rule is invariant in both modes: a caller sees
success only after its record is finalized, its commit group has reached the
requested sync boundary, and the resident projection has accepted the new
ref. A writer pipeline may batch independent operations but may not
acknowledge, reorder, or expose an operation before that point.

Pouch follows Go disk's bounded index-writer boundary. After a successful
state mutation, the exclusive foreground path captures a compact, body-free
derived projection while the source is still available. It never retains the
source body. A staged transaction promotion has no caller-owned source at its
decision boundary, so its pending row is deferred and extracted from the
already-finalized durable payload span during that foreground publication.
When completed public lease releases or transaction decisions
leave that projection at the configured distinct-document bound (2,000 by
default), with no other pending key still active, it publishes the immutable
index segment synchronously, just as Go disk flushes its full memtable.
Deferring the threshold check until every captured operation has reached its
completion boundary deliberately coalesces multiple updates under overlapping
leases into their final document versions. The batch take repeats the
active-key check while it claims the pending projection; if another operation
became active, foreground publication defers without reading the live tail. A
successful foreground publication consumes its worker queue entry, so a later
below-threshold mutation begins a full fresh worker interval rather than
inheriting the earlier batch deadline. The exclusive worker applies that same
active-operation guard at its idle deadline: it keeps an active public lease
or transaction batch pending until its final completion boundary, then retries
under the normal interval policy. Direct storage mutations have no public
completion boundary and still publish at the idle deadline. In particular,
the low-level `lc_pouch_state_*` APIs leave their durable writes ready for the
idle worker; only client lease and transaction orchestration marks a pending
key active until its explicit completion callback. Repeated updates to one key
replace that key's pending projection and do not advance the exclusive-writer
document bound. The deadline is not restarted by later writes. One-shot,
oversized, or otherwise non-replayable sources explicitly mark the pending
projection incomplete; that fallback retains every active operation key until
its completion callback, and its next eligible flush incrementally reads the
authoritative state log. Once a pending exclusive projection contains a
deferred body, every subsequent row in that projection uses the same durable
extraction path, preserving one coherent final-version segment. Shared-root
handles can each
run an indexer, but retain asynchronous, namespace-locked durable replay
because no one handle owns every writer's projection. Index publication is
derived work: a publication failure remains queued for retry and cannot revoke
an already durable state mutation.

The durable state-index sequence is the sole index freshness boundary. A query
or explicit `flush_index` compares it with the manifest sequence; on a
mismatch, it performs the same incremental replay synchronously when the
background indexer has not yet published. Process exit can therefore leave
only derived artifacts behind: the next synchronous query/flush repairs them
from durable state without any lost document or in-memory body dependency.
For an exclusive writer, a `flush_index(mode=wait)` may satisfy an equal
sequence from the manifest snapshot that this ownership epoch already
published; it does not reopen derived artifacts merely to validate a no-op.
`flush_index(mode=sync)` and recovery paths remain the explicit validating
operations and rebuild damaged artifacts from durable state.

`flush_index(mode=wait)` publishes all state accepted by the indexer but does
not retain or adopt a just-written derived artifact in a handle-local query
cache. This matches Go disk's flush boundary and keeps durable publication out
of the query-cache hot path. Query representations load lazily from the
validated packed artifact on first use, while open-time cache warming remains
best effort. The packed binary artifact remains the sole durable source:
reopened handles, shared roots, and cache-allocation failure use the normal
validated packed-artifact decoder. No cache retains source JSON or full
document bodies.

Exclusive roots serialize derived artifact publication with a root-local flush
mutex. Shared roots instead hold the root durable-mutation guard from the
high-water sequence sample through artifact and manifest
publication. This serializes competing Pouch handles and processes, so their
read-modify-write manifest updates and derived artifact writes cannot race.
The indexer's queue is local scheduling state only and never participates in
public mutation completion.

Go disk's default failover path marks writes `NoSync` because a per-write fsync
can dominate core lockd workloads. Pouch has the same default boundary:
finalized records are visible to shared readers and safe to replay after a
cleanly flushed filesystem cache, but an unexpected power loss follows the
platform's `NoSync` semantics. Pouch preserves deterministic append order and
finalized-record publication. Callers that require a stronger boundary opt in
to `durable_sync=1`; on Linux that mode uses `fdatasync` with per-file
deduplication inside a root-scoped commit group.

Pouch's durable-sync worker immediately dispatches the ready group and collects
arrivals during I/O for the next group. A sequential caller does not pay a
coalescing timer, and every caller still waits for its group's sync outcome.
The process-local `fsync_batch_max_ops` upper bound is a `uint64_t`, defaults
to zero for an unbounded group, and is available on the direct Pouch open
options and the `pouch://` endpoint. Separate processes do not share the SDK's
queue, though the filesystem can coalesce their I/O. The resident writer
pipeline retains its descriptors and healthy namespace discovery state.
`lc_pouch_fsync_stats_read` exposes aggregate batch-size and sync-I/O latency
counters, excluding queue waiting time, with fixed-width counters on every
supported C ABI.

`segment_target_bytes` is a `uint64_t` direct open option and an endpoint
option (`pouch://...?segment_target_bytes=<u64>`). Zero resolves to the 64 MiB
default. It applies to stored record bytes, not source payload bytes.

Initial constants should mirror Go disk unless profiling proves a C-local
change is better:

- materialized complete-record threshold: 64 KiB, and only for values already
  owned by `lc_source_from_memory`;
- payload streaming buffer: 128 KiB;
- read file cache: 64 open segment/snapshot files;
- append batch buffer cap for grouped inline records: 1 MiB.

## Streaming Requirement

Streaming means real producer-to-consumer flow. Pouch must not serialize,
encrypt, compress, or concatenate a complete large document into memory behind
a streaming-looking API.

Bounded chunk buffers are acceptable. Full-message buffering, temporary files
as an implicit staging substitute, and whole-payload materialization are not.

This applies to:

- state writes;
- object/attachment writes;
- queue payload writes;
- reads;
- scan/query document emission;
- compaction;
- crypto;
- compression.

## Replay And Refresh

Replay rebuilds projections from installed snapshots and non-obsolete segments.

Required behavior:

- recover and construct the namespace projection at exclusive open, shared
  writer attachment, takeover, or explicit invalidation; do not reconstruct it
  before an ordinary exclusive operation;
- exclusive mode reuses its projection and verified active append offset until
  rotation, maintenance, handoff, close/abort, or I/O failure;
- shared-root mode validates and refreshes its projection under the root
  durable-mutation guard. It replays only bytes beyond its verified committed
  tail offset; the initial directory discovery of that known active leaf is a
  cursor-preserving tail transition rather than a historic replay;
- order installed snapshot first, then live non-obsolete segments;
- apply state/object records by public generation so stale payload writes
  cannot resurrect older state; use the durable namespace index sequence as
  the tie-breaker for equal versions. `STATE_META` overlays are ordered by that
  sequence independently of payload generation, matching Go disk's separate
  state and metadata indexes; physical filename order is never commit order;
- require the durable per-record index sequence for every state, object,
  staged-link, delete, and decision record;
- stop at an incomplete, pending, or truncated tail in the manifest's active
  rolling segment, then repair it once during writer acquisition/recovery
  while holding the root mutation guard in shared mode or append authority in
  exclusive mode; reject the same condition in a sealed
  non-active segment or installed snapshot as corruption;
- never apply a partial record or bad-CRC payload;
- validate link targets against manifested segment/snapshot state before
  installing linked refs;
- rebuild metadata, state, object, queue, transaction, lease, retention, and
  query-visible projections from log records without depending on global side
  namespaces for user data surfaces.

Open/corrupt-tail semantics must be explicit in tests. The implementation
should follow Go disk's operational intent: a crash tail must not make the
whole namespace unreadable, but corrupt data must not be silently applied.

## Manifest And Refresh

Each namespace has a manifest recording at least:

- snapshot install;
- obsolete segment;
- obsolete snapshot.

The current C manifest is a small rewritten text file with the installed
snapshot, obsolete file sets, and numeric active-segment field. Validated
numeric segment names are discovered from `segments/` at recovery or lifecycle
transitions; ordinary active appends do not scan or rewrite the manifest.
Rotation, snapshot install, and obsolete changes update it while the
appropriate coordination gate is held. Snapshot install or obsolete changes
invalidate only affected resident replay state so projections cannot keep stale
refs. The manifest is namespace lifecycle metadata; hot
state/object/queue/lease facts remain binary log records.

Malformed manifest entries are ignored only where Go disk intentionally treats
them as non-authoritative append noise. Any behavior here must be tested and
documented in code comments because manifest policy is a durability decision.

The active segment and writer epoch are the shared-root refresh source.
Exclusive mode does not poll either while it owns the root. Pouch does not use
per-writer state markers as data authority. Queue wake-up notifications remain
separate, advisory files under `queue-notify/`; they never establish state
durability or projection visibility.

## Read Path

Reads open bounded sources over segment/snapshot payload spans. The read path
uses an LRU cache for open segment/snapshot file descriptors and dup-backed
bounded sources for independent reader lifetimes. Projection creation,
metadata-only reads, and writes never open, decrypt, decompress, or
materialize a live payload merely to prefill the body cache. A requested body
source streams directly from its payload span on its first consumption while a
bounded tee copies delivered plaintext into a provisional cache entry. Only a
source that reaches successful authenticated EOF publishes its entry; partial,
failed, and tampered reads never become cache hits. The cache is capped at 16
MiB per resident namespace, and cached entries preserve an already-returned
source across concurrent projection invalidation.

Required behavior:

- read by structured ref, not by reparsing a string path;
- never read past the stored payload span;
- expose payload readers that stream transforms in the correct order;
- return metadata from projections without opening payload bytes;
- when a public copy operation needs a payload, capture its result metadata and
  a dup-backed segment source or retained, authenticated bounded body-cache
  source while coordinated, then release namespace coordination before copying
  into the caller sink. A slow or blocked caller sink must not serialize
  another read;
- avoid opening hidden, staged, reserved, and internal-prefix rows for scan
  summaries;
- keep public state, private state, attachment/object, queue, and transaction
  reads on the same logstore primitives.

Public state scans must also exclude internal user-namespace key prefixes such
as `q/`, `state/<key>/attachments/`, `state/<key>/.staging/`, and
`config/namespace` unless a future public API deliberately exposes those
surfaces. This applies equally to derived and persisted query-index rows, so a
transaction's staged record cannot become queryable before its decision.
Hiding must be enforced by metadata and prefix policy, not merely by placing
records in a different namespace.

## Scan And Query

Scan and indexed query operate over logstore projections and payload spans.

Scan requirements:

- walk sorted metadata summaries;
- skip hidden, staged, reserved, internal-prefix, and deleted rows before
  opening payloads;
- open a payload span only when selector evaluation or document emission
  requires it;
- stream selector evaluation through `liblql`;
- prove `has_more` cursor state by checking whether another matching row exists;
- never materialize all candidate documents.

Indexed query requirements:

- indexable selectors use indexes when the selected engine is index;
- indexed execution must not silently fall back to scan;
- strict JSON Pointer paths preserve decoded object-member names, including
  numeric and empty names and `~0`/`~1` escapes. Exact array elements retain
  their numeric JSON Pointer paths, while Pouch's `/field[]` shorthand is an
  additional posting only for values whose parent is an actual JSON array;
- a query-index format revision invalidates older derived artifacts and
  rebuilds them from the authoritative logstore before serving the new field
  semantics;
- query-index artifacts are derived from logstore projections and are
  rebuildable after corruption or loss;
- query-index manifests carry each segment's sequence, row count, row hash,
  delete summary, posting-completeness flags, and exactly one signature for
  that segment's packed artifact;
- every query-index segment component is stored in one packed binary
  artifact named `query.<segment>.query.index.lcpseg`. The packed artifact
  contains the document table, exact/presence/range/text/trigram/temporal term
  generations, and the delete set. The manifest has one `artifact` record per
  segment (artifact index `0`) for this file; logical component paths are
  in-memory decoder identifiers only;
- encrypted packed query-index artifacts store ciphertext followed by
  descriptor bytes and a fixed binary footer. Pouch reads the footer, bounds
  decryption to the ciphertext span, and does not create separate descriptor
  companion files for derived query-index artifacts;
- an empty delete set is represented by a zero-length delete component inside
  the packed artifact. The manifest's `delete_count=0` and empty-set hash are
  the authoritative empty value; non-empty delete components must match the
  manifest count and hash;
- query-index manifests and packed segments are installed by same-directory
  temporary-file rename without fsync because they are derived files. A new
  packed segment whose path is proven newer than a valid current manifest may
  be written directly: an interrupted write leaves only an unreachable
  artifact, and the manifest switch remains the atomic publication point.
  Recovery validates the manifest metadata and packed artifact signature, and
  rebuilds from the logstore if any derived write was interrupted or torn;
- normal append flushes do not sweep the index directory for orphaned derived
  artifacts. Initial manifest bootstrap also skips a sweep because no artifact
  can be referenced before that manifest is published; an interrupted
  bootstrap can leave only unreachable derived files, which a later validated
  repair or rebuild from an existing manifest reclaims. Full rebuilds from an
  existing manifest, repair/validated flushes, and retired-segment cleanup
  paths perform orphan cleanup, so foreground append flush latency is not tied
  to directory size;
- indexed queries validate segment completeness from the durable manifest and
  validate the packed artifact against its recorded signature; they do not
  depend on a standalone segment-header file;
- a successful manifest sequence read records per-client manifest trust for
  that namespace/index sequence. A later non-validating ensure-current call may
  skip rereading the manifest when the state index sequence is unchanged;
- shared-writer compaction replaces canonical-state topology under the
  namespace write authority and first retires its derived query manifest. The
  next foreground `flush_index` (including the outbox recovery `wait`
  boundary) rebuilds from the installed snapshot, publishes its replacement
  atomically, and reclaims the now-unreferenced artifacts.
  This is a cold maintenance boundary for peer-owned asynchronous publication;
  exclusive-writer compaction retains its complete in-memory index capture and
  does not add a query rebuild to its normal compaction path;
- indexed document queries with a discard sink may bulk-count exact candidates
  only when there is no input cursor and all matches fit below the requested
  limit, so cursor behavior and candidate verification semantics remain
  unchanged;
- packed query-index artifacts are cached per client by file signature so one
  physical read/decrypt can serve document table, delete set, and term
  generation components for the same segment;
- exclusive-writer roots eagerly construct immutable state and query-index
  readers for existing namespaces at client open. This read-only derived cache
  removes first-query decoding from the default fast path without publishing or
  flushing data. Plain shared-root handles remain lazy to bound their optional
  multi-writer memory footprint; encrypted or compressed roots retain eager
  warming in either writer mode;
- field-specific `contains` lookups report whether all text generations for
  the field are complete while collecting indexed candidates. The planner must
  not perform a second completeness-only artifact walk after a successful
  field-specific contains lookup;
- index flush must be incremental and generation-aware, matching Go disk's
  performance intent rather than rebuilding the full corpus on each flush;
- full-text search must cover text in the full JSON document, including nested
  fields and long text fields, through the selected indexed engine;
- string values of at most 256 bytes receive complete trigram postings. Longer
  strings retain their exact hash and first 256-byte prefix and receive a
  fallback candidate posting, so exact, prefix, and contains results remain
  complete through body-level verification without creating blob-sized
  posting lists;
- `/...` is a logical whole-document text selector, not a public state field.
  Pouch resolves it by unioning the concrete string fields, matching Go disk's
  index contract. It never duplicates each raw-text or trigram posting into a
  synthetic all-text field, so index publication remains bounded by the real
  document projections. It may additionally emit a bounded private posting for
  each ASCII letter-or-digit token from a short retained text value. These
  postings prove positive case-insensitive whole-token `icontains` matches and
  let an all-text query bypass raw-term verification when they cover every
  trigram candidate. They never prove a negative: punctuation, non-ASCII and
  partial-token needles, unindexed long values, and partially covered
  candidates retain the normal trigram-plus-verifier path. Trigrams remain a
  candidate filter and text terms reject false positives.

## Staged State

Staged state uses logstore records, not separate payload files.

Promotion must follow Go disk's solved shape:

- staged payload writes create normal state records under a staging key;
- promotion CAS-checks the destination and staged keys;
- promotion appends a state link at the destination pointing at the staged
  payload span;
- promotion appends a delete/tombstone for the staging key;
- compaction treats live links as protected until they are rewritten safely;
- link refs preserve etag, descriptor, plaintext byte count, stored byte count,
  and transform state.

This avoids copying staged payload bytes during promotion and preserves
large-payload performance.

## Crypto And Compression

Encryption is optional and disabled by default. Encryption policy and key
identity cannot change on open: an encrypted root requires its matching key,
and an existing unencrypted root cannot be opened as encrypted. Compression
defaults to none for new roots; omission inherits an existing root's mode.
An explicit compression change converts the root under exclusive ownership,
preserving its encryption policy. See
[compression changes on open](pouch-compression.md).

Transforms belong at the log payload storage boundary. They are streaming
wrappers around payload bytes written into segment/snapshot records.

Required behavior:

- derive per-record material from the root key and a stable logical context;
- do not include physical segment offset in the authenticated context unless
  compaction deliberately remints descriptors;
- preserve plaintext byte count, stored byte count, descriptor bytes, etag, and
  payload CRC;
- authenticate record class, namespace, key, generation, and transform metadata
  as associated data where the provider supports it;
- fail closed on descriptor corruption, wrong key, tampered ciphertext, or
  invalid transform ordering;
- encrypt production data at rest: state payloads, attachment/object payloads,
  queue message payloads, and transaction payloads where they contain user or
  production data;
- keep searchable metadata and query artifacts free of plaintext user payloads
  unless the root mode explicitly defines and accepts that leakage. Query-index
  segment headers may be plaintext because they contain only non-secret format,
  sequence, count, and hash metadata.

Compression runs before encryption on writes and after decryption on reads.
zlib may be used. Compression must be streaming and bounded. Small payloads may
skip compression when the descriptor records that no compression was applied,
or may omit the descriptor only when no transform was applied. Encrypted records
are always descriptor-required. Tiny control records, including lease records,
are compression-ineligible on compression-enabled roots because zlib overhead
dominates those hot paths; on crypto roots they are still encrypted and still
preserve plaintext and stored byte counts.

Compaction must copy stored payload bytes when the descriptor remains valid.
It must not decrypt/re-encrypt or decompress/recompress every live record merely
because its physical segment changes.

## Compaction

Compaction is a namespace lifecycle operation. It is not a full-cache dump.

Required behavior:

- load manifest, snapshots, and segments before capture;
- choose candidates from installed snapshot plus sealed non-obsolete segments;
- treat every non-active segment as sealed and exclude the manifest's active
  rolling segment from candidates;
- compute reclaimable bytes;
- enforce configurable `min_segments`, `min_reclaimable_bytes`, interval,
  delete grace, and optional IO throttle;
- when background compaction is enabled with a non-zero interval, schedule a
  bounded turn after a mutation or discovery of durable work at open; wait
  the configured interval before the first turn and between later turns;
  coalesce later mutations without postponing already scheduled work;
- rotate fairly across queued namespace work, durable terminal-reclaim markers,
  [per-object state-retention hints](state-retention.md), and collection discovery;
  skip empty sources within the same turn, inspect each source at most once,
  and maintain at most one selected namespace per turn; advance the rotation
  before discovery so errors cannot pin a source's priority;
- perform eligible logical expiry outside the compaction namespace guard,
  even below snapshot thresholds;
- run explicit namespace maintenance immediately rather than delaying it for
  the background interval;
- honor `compaction_max_io_bytes_per_sec` while copying snapshot payload
  chunks; `0` leaves compaction unthrottled;
- detect live state links that point into candidate files and protect those
  files until the links can be rewritten safely;
- capture current refs from metadata, committed state, staged state, object,
  and attachment projections in deterministic key order; staged and attachment
  payload spans are live until their durable decision or deletion record makes
  them unreachable;
- build a temp snapshot from captured refs using streaming payload readers;
- copy stored payload bytes where transform descriptors remain valid;
- fsync the snapshot file;
- validate that captured refs are still current before install;
- rename temp snapshot into place;
- append manifest entries for snapshot install and obsolete files;
- update projections to new refs only after manifest install succeeds;
- delete obsolete files only after delete grace and only if no live refs or
  protected links remain.

Compaction takes the exclusive namespace maintenance barrier before capture,
validation, install, or retention deletion. Normal writers hold the shared
side, so an installed snapshot cannot obsolete a file that a writer will later
append. Pouch must not range-unlink files through a maximum id. It may use a
C-native manifest and snapshot format, but it must preserve Go disk's candidate
selection, drift validation, live-link protection, and obsolete cleanup timing.

Validation drift must abandon the snapshot without installing it. Cleanup must
be retryable and idempotent.

## Pouch Endpoint Construction

Applications must not assemble `pouch://` strings by concatenating a root and
query text. `lc_pouch_endpoint_build()` takes an absolute, decoded root plus an
array of decoded `lc_pouch_endpoint_option` values and returns an owned,
percent-encoded endpoint. It preserves `/` in the root, encodes query names and
values, and represents a `NULL` option value as a bare option. Release the
result with `lc_pouch_endpoint_free()`.

`lc_pouch_endpoint_has_option()` is the complementary inspection API for
configuration layering. It compares decoded query names only, never returns
values, and treats duplicate or bare options as present. It uses the same
percent-decoding as Pouch open; query processing ends before a URL fragment,
and `+` remains a literal plus rather than being form-decoded. Consumers can
therefore decide whether to apply a local default such as
`crypto_key_file` without reimplementing Pouch URL parsing or accidentally
overriding an encoded explicit option.

## Public API Coverage

Every Pouch behavior is exercised through the public Pouch API or public
storage API boundary, matching how Go lockd disk is benchmarked and tested.
Tests and benchmarks must not use background magic or private mutation helpers
to make Pouch look faster or more correct than the public engine.

The installed C header is part of this public contract. Doxygen comments for
public Pouch-facing configuration and APIs must document the same behavior
described here: `pouch://` uses one absolute local root, exclusive single-writer
mode is the default, explicit shared-root writing requires
`LC_POUCH_SETTING_SINGLE_WRITER` with zero (or compatible
`single_writer=false`), typed and endpoint values are copied at open, public
`long` fields are range-checked before narrowing on
32-bit targets, and state bodies, queue payloads, attachments, scan output,
query-document output, crypto, and compression remain real streaming paths
unless the caller explicitly chooses a memory-backed source or sink.

The header documentation audit is structural, not a sampling exercise. Every
public typedef, enum value, transparent struct field, receiver method,
callback, constructor, initializer, cleanup helper, and compatibility wrapper
must have an adjacent Doxygen comment. Those comments must state the applicable
ownership/lifetime rule, zero/default behavior, terminal-handle behavior,
streaming or materialization boundary, representable-width restriction, and
error contract. In particular, descriptor adapters document whether they
borrow or own the descriptor, response comments identify owned fields, and
callback comments identify which arguments may be retained after return.

Coverage must include:

- acquire, release, update, mutate, get, and get public;
- state write/read/reopen/delete;
- staged state promote/discard;
- attachments and object payloads;
- queue publish/claim/ack/retry/dead-letter where supported by liblockdc;
- transaction prepare/commit/discard paths;
- query keys, query documents, scan, indexed query, full-text query, and index
  flush;
- maintenance and compaction;
- crypto, compression, and crypto+compression roots;
- multi-segment production-size datasets using default segment size.

The focused exclusive-mode transform matrix runs acquire, keepalive, update,
mutate, lease and public get, attachment roundtrip, queue enqueue/dequeue/ack,
transaction commit, scan, indexed query, and full-text query against plaintext,
crypto, compression, and crypto+compression roots. Focused transition tests
separately cover ownership, shared mode, takeover, tail recovery, rotation,
durable grouping, and descriptor lifecycle.

## Benchmarks

Production benchmarks compare the following modes separately:

- Pouch plaintext;
- Pouch crypto;
- Pouch compression where relevant;
- Pouch crypto+compression where relevant;
- Go lockd disk plaintext for Pouch plaintext, and Go lockd disk crypto for
  Pouch crypto. Go disk has no matching compression mode, so Pouch compression
  and crypto+compression are reported separately and are not cross-engine
  parity-gated against a mismatched transform configuration.

Benchmarks must include realistic and abusive workloads:

- deep nested JSON documents;
- long summary/description/body text fields;
- mixed small, medium, and large state payloads;
- repeated acquire/release/update loops;
- get/get-public/read-many;
- scan selectors;
- indexed selectors;
- full-text search over entire documents;
- queue roundtrips;
- attachment/object writes and reads;
- staged state promotion;
- compaction over many default-sized segments;
- reopen and multi-segment replay;
- overcapacity patterns with churn, deletes, updates, and stale history;
- shared-root single-segment terminal reclaim and forced multi-segment
  compaction while an outbox dispatcher holds a real claim, followed by
  retention cleanup, reopen, exact survivor assertions, and cold outbox
  recovery after a forced multi-segment compaction.

Document-returning production query metrics measure steady-state public API
throughput. The benchmark first runs the same document query once outside the
timed region and verifies the exact match count for both Pouch and Go lockd
disk, then records the timed query. Key-only query metrics are not warmed this
way because they do not exercise document streaming.

The fixture must use deterministic incompressible data whenever a scenario
asserts multi-segment rollover for a compression mode. It must not infer a
format failure merely because compression correctly reduces stored bytes below
the rollover threshold.

The exclusive-writer comparison gate has an explicit allowlist of comparable
end-to-end core metrics: acquire, update and stale-precondition rejection,
lease/public get, release, queue, attachment write and attachment retrieve,
intermediate/final/no-op/post-reopen index publication, cold and warm indexed
key queries, indexed document queries, scan/full-text queries, and restart
recovery. Each metric must be reported by both engines. Aggregate `ns/op` and
Pouch-only C timing remain diagnostics, not independent cross-engine parity
metrics. Aggregate timing must not hide a slower core operation.

Acceptance target: exclusive Pouch must materially outperform the matching Go
lockd disk plaintext or crypto configuration on every gated core metric. The
numeric hardening budget is a minimum `1.25x` Pouch speedup: every Pouch latency
must be at most 80% of the matching Go disk median. The stable baseline is the
median of five same-run production samples for each engine, preventing isolated
sub-millisecond filesystem and scheduler outliers from redefining a performance
threshold. `make
benchmark-pouch-go-parity-gate` enforces this with
`POUCH_GO_PARITY_MIN_SPEEDUP=1.25`; strict-but-undefined "faster" is
insufficient. Pouch compression variants retain their own reported performance
evidence in the complete production matrix, but are excluded from the release
performance gate because Go disk has no transform-equivalent compression mode.
The performance gate runs only plaintext and crypto Pouch/Go pairs, with five
production samples each and a default `POUCH_GO_PARITY_TIMEOUT=90s` budget.
Shared root has
separate correctness, contention, handoff, and bounded-performance coverage and
does not dilute the exclusive performance comparison.

Strict durable sync uses the same complete core-metric contract in `make
benchmark-pouch-go-durable-gate`; it is part of `make perf-gate` rather than an
opt-in diagnostic, and runs in `make prerelease-hardening`. Compression has no
format-equivalent Go disk counterpart, so its core-operation churn remains
measured by the hardening soak rather than a synthetic cross-engine threshold.

`make prerelease-hardening` runs the Pouch-vs-lockd performance thresholds and
additionally runs a finite ten-minute Pouch core soak outside the normal release
gate. It repeats an exclusive root's acquire,
update, stale-precondition, leased/public read, attachment, queue, indexing,
query, reopen, and recovery cycles under plaintext, crypto, compression, and
crypto-plus-compression. It then proves forced and idle-debounced background
reclaim on rolling segments and exercises same-key and independent-key
shared-root contention. The soak has fixed workload and timeout controls in
the root Makefile; it is deliberate release hardening, not an unbounded burn-in
or a normal release prerequisite.

The same lane also runs the bounded outbox reconciliation hardening suite:
preflushed and persisted indexes, a forced-compaction reopen, and shared-root
dispatchers before and after compaction. Its cases are serial and use two
shared-root dispatchers by default, so it exercises recovery correctness
without turning release hardening into an unbounded host load.

## Fuzzing And Failure Modes

Fuzzing and failure tests must cover:

- record header decode;
- metadata decode for every family;
- state link decode and target validation;
- manifest replay;
- crash-truncated active tails and rejected truncated sealed segments or
  snapshots;
- bad CRC and malformed lengths;
- replay generation ordering and equal-generation index-sequence ordering
  across numeric rolling segments;
- compaction capture/install/cleanup metadata;
- transform descriptor decode;
- crypto authentication failure;
- compression corruption;
- scan/query selector paths;
- query-index artifact corruption and rebuild.

Fuzzing must run with plaintext and transformed roots.

## Cleanup Requirements

The implementation cutover must remove rejected-code paths in the same slice:

- external state/object payload durability helpers;
- text hot-metadata parsers for storage facts;
- string payload refs as durable/in-memory authority;
- full-cache compaction dump logic;
- per-mutation fsync-only append paths where batching is required;
- hidden scan materialization paths;
- index rebuild-on-every-flush paths;
- compatibility branches for rejected development Pouch layouts;
- stale tests, fixtures, benchmarks, docs, and names such as
  `pouch-redesign`, compatibility layers, company layers, and disk-conflated
  terminology.

Pouch may mention Go disk in docs and comments only as a reference. Public API,
file names, errors, and durable Pouch metadata must use Pouch terminology.
