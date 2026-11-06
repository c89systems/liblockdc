# Managed participation in implicit XA

This document defines the implemented Pouch and PostgreSQL participation
contract and acceptance gates. See [the driver guide](postgres.md) for C usage,
outbox binding and completed-history retirement, and [the Lua guide](lua.md) for
Lua participation and callback orchestration.

## Scope and responsibilities

Extend liblockdc's existing transaction coordinator with a PostgreSQL participant
driver. PostgreSQL, Pouch state, attachments, inbox/command records and outbox
effects participate in the same global transaction and immutable decision.
The integrated coordinator store is Pouch. Remote lockd participation and
SQLite distributed participation are unsupported. Existing remote lockd outbox
limitations still apply; this work does not modify lockd.

| Component | Responsibility |
| --- | --- |
| Coordinator | Registration, enrollment, durable votes and decision, fencing, recovery, completion and journal retirement |
| PostgreSQL driver | Branch connections, native prepare/resolution, outcome queries, scoped connection borrows and prepared inventory |
| Pouch participant | Durable staging, transaction locks, state/attachment application or rollback |
| Outbox facade | Staged effects, completion receipts, publication and dispatcher notification |
| Query adapter | Application query/cursor interface on the exact borrowed branch connection |

There is one coordinator implementation. State-only transactions, SQL-only
transactions and mixed transactions use it; no second coordinator exists in the
PostgreSQL driver, outbox facade, Lua binding or Vectis adapter. Generic SQL/state
transactions do not need a dummy outbox effect to obtain coordination.

Use the public C89 `cpkt/postgres.h` facade from cpktdb and its official imported
targets. No direct libpq calls or dependency-private headers are permitted.
Server two-phase capacity and ordinary connection/role permissions are explicit
prerequisites. Registration validates them; it does not configure PostgreSQL.
There is no PostgreSQL metadata schema, custom function, extension, additional
empty prepared transaction, history-pinning mechanism or SQL replay log.

## Optional PostgreSQL capability

The core `liblockdc.a` and `liblockdc.so` remain independent of PostgreSQL and
the cpkt PostgreSQL facade. Enabling or disabling this capability must use the
same core binaries. Core public headers and CMake/pkg-config imports must work
without cpktdb headers, metadata, archives or shared libraries. Do not put native
PostgreSQL symbols, a PostgreSQL `DT_NEEDED` entry, or a PostgreSQL build-mode
definition in the core library.

Implement native PostgreSQL operations in an optional `liblockdc_postgres`
provider. Its static/shared artifacts consume the official cpktdb facade targets
and own that dependency closure. Both static and shared applications enable
PostgreSQL by explicitly linking the provider and registering its function table
through the same C interface. Applications without PostgreSQL omit that link
group and registration call, using the exact same core library. Resource
registration does not discover or dynamically load libraries.

The final executable differs according to its linked capabilities. A fully
static executable cannot acquire an omitted provider without relinking. An
application linked to the shared provider requires that shared library and its
native client dependencies at startup; skipping registration does not remove
those loader dependencies. Omitting PostgreSQL therefore means omitting its
application link group, rather than removing required shared files afterward.

The provider uses a public C89 resource contract with the core coordinator.
Registration uses functions already linked into the application. Do not add a
runtime plugin loader, require a second core instance, or export private core
symbols to support the provider.
Keep provider/native objects alive while operations, borrows or results refer
to them. Disablement prevents new PostgreSQL registration; existing unfinished
branches remain durable pending recovery and cannot authorize Pouch publication.
Ordinary Pouch capabilities remain available without the provider.

`LOCKDC_BUILD_POSTGRES` defaults to OFF. Only ON discovers/requires cpktdb and
builds provider-specific validation. Dependency provisioning follows the same
selection, through `LOCKDC_BUILD_POSTGRES=ON` or `scripts/deps.sh <preset>
--with-postgres`. OFF does not fetch, require or validate cpktdb. A missing SDK
when ON is an explicit configure error. Provider metadata and exported targets
remain separate from core metadata/imports, including after SDK relocation.
Lua follows the same explicit provider registration through C; it does not link
the optional native client into the base Lua/core artifacts.

## Identity and enrollment

The coordinator creates a stable global transaction ID when the transaction
starts or its first participant is enrolled. SQL may be the first or only
participant: identity must not depend on acquiring a Pouch business-record lease.
All subsequent participants use that same ID. Enrollment closes before the final
prepare vote set is evaluated; late participants are rejected.

Keep one authoritative decision in the existing reserved `.txns` namespace.
Resource registrations and durable coordinator identity belong in `.xa`; this
namespace is not a second decision log. Extend participant descriptors with
resource kind, stable resource identity, branch identity, prepare status,
resolution intent, completion and its evidence source. A native prepared
observation is distinct from a coordinator YES vote: recovery may discover and
durably record prepared work after ABORT without changing the vote or decision.

The Pouch storage adapter represents an external branch in the existing
participant set by the reserved `.xa` namespace and its branch descriptor key.
That descriptor holds native identity and progress; `.txns` holds its prepare
vote and the global decision. A prepare-intent marker seals enrollment without
counting as a YES vote. Application waits for every external descriptor to record
completion in the decision's direction. `LC_ERR_PENDING` reports that wait and
preserves the durable decision in TC/replay responses.

Each PostgreSQL branch records:

- Coordinator, global transaction, resource-manager and branch identities.
- A unique generated native GID and exact full PostgreSQL XID.
- Backend identity: cluster system identifier, database OID and role OID.
- Durable enrollment, prepare intent/vote, resolution intent and completion.

The coordinator supplies the GID; PostgreSQL does not register other resources
or choose the global decision. Generate bounded ASCII GIDs from durable
identities, shorter than 200 bytes, without credentials. Do not reuse a branch
GID or adopt a colliding/foreign branch from its prefix alone.

Before application work becomes preparable, obtain the top-level native ID on
its branch connection:

```sql
SELECT pg_current_xact_id()::text;
```

Persist that exact decimal string, including the epoch, in the enrollment
descriptor. It is distinct from the GID and the global ID. Never truncate it to
a 32-bit value or round it through a Lua number. Database/role names are
diagnostics; recorded backend identity is checked on recovery connections.

Persist enrollment and prepare/resolve transmission intent before transmitting
the corresponding operation. Coordinator records contain no application SQL,
bound values, connection secrets or raw provider diagnostics. Recovery obtains
credentials from the registered resource configuration independently of the
original application process.

## Ordinary two-phase flow

1. Enroll PostgreSQL and execute `BEGIN` on its branch connection.
2. Execute application SQL on that same connection. Pouch participants stage
   their changes under the same global ID.
3. End application access, seal enrollment and prepare every participant.
   PostgreSQL receives `PREPARE TRANSACTION 'branch-gid'`.
4. Persist every YES vote. Only an all-YES set permits a durable COMMIT decision.
   A NO, or fenced abandoned work without a durable COMMIT, selects durable ABORT.
5. Persist the immutable decision before phase-two commands. The driver sends
   `COMMIT PREPARED` or `ROLLBACK PREPARED` accordingly.
6. Durably record participant completion, apply Pouch changes and publish
   completion receipts through the common publication gate.

A successfully prepared branch is durable and detached from its original
session. Another appropriately privileged session can resolve it. A
server-confirmed failed prepare cancels the local transaction; transport loss or
retrieval failure does not establish a failed prepare. Prepared work survives
ordinary server restart and is not aborted by closing a client connection.
[Native preparation](https://www.postgresql.org/docs/18/sql-prepare-transaction.html),
[native resolution](https://www.postgresql.org/docs/18/sql-commit-prepared.html).

Once COMMIT is durable, timeout, cancellation, owner expiry, lost response and
failover never change it to ABORT. Recovery repeats the recorded decision,
never application statements. Matching phase-two retries may come from successive
owners; no owner may send the opposite decision. PostgreSQL commands carry no
coordinator term, so a term check before sending is not sufficient arbitration.
Journal mutations are fenced; terminal decisions are immutable.

An active owner is not abandoned just because another client starts recovery.
Participant locks and staged Pouch data survive owner-lease expiry while the
global transaction awaits application. Startup recovery, ordinary replay, lease
acquisition and record cleanup must respect that same application gate.

Managed ownership uses a lease per global transaction in `.xa/owner/<txn-id>`.
It records the generated owner identity, a monotonically advancing epoch, expiry
and enrollment sealing; `.txns` remains the sole decision log. Independent
transactions can have different owners in a shared Pouch root. Claims, enrollment,
votes and progress share the existing transaction mutation guard. A raw/manual
journal cannot advance a managed branch, and a stale owner cannot renew, release
or progress its replacement's work.

Persist a first owner before the initial `.txns` record, so a failed begin retains
recoverable identity. For takeover, persist ABORT for abandoned undecided work,
or the existing terminal direction, before publishing a newer owner epoch.
A crash between those writes leaves mismatched epochs that block progress;
the next recovery advances again and preserves the decision. Sealing is durable
before prepare votes, including when a later vote write fails. Passive startup
and replay preserve managed undecided work for fenced recovery rather than
choosing an outcome from a business lease's expiry.

## Lost acknowledgements and reconciliation

PostgreSQL removes a prepared GID after either commit or rollback. A repeated
resolution can therefore return SQLSTATE `42704`, corresponding to XA's
unknown-transaction case. This is not by itself evidence of failure or success.
[Prepared inventory](https://www.postgresql.org/docs/18/view-pg-prepared-xacts.html),
[XA commit contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.transaction.xa/javax/transaction/xa/XAResource.html).

After reconnecting to the verified resource, query the saved full native ID:

```sql
SELECT pg_xact_status($1::xid8);
```

Native results are `committed`, `aborted`, `in progress`, or SQL NULL if historical
status has been discarded. Prepared branches report `in progress`; inspect
`pg_prepared_xacts` to distinguish them from active or resolving work. A query
error is not SQL NULL. PostgreSQL documents this function specifically for
confirming outcomes after a lost commit connection.
[Native status contract](https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-PG-SNAPSHOT).

| Observation | Required action |
| --- | --- |
| Exact owned GID is prepared and matches backend/role/database/XID | Apply the immutable recorded decision |
| GID absent, native status matches decision | Durably acknowledge native completion |
| GID absent, native status contradicts decision | Record an explicit integrity/heuristic failure; block publication |
| GID absent, native status is in progress | Reconcile active or in-flight work; do not acknowledge completion |
| GID absent, native status NULL, completion already journaled | Return the journaled result |
| GID absent, native status NULL, durable prepare/decision/resolution intent recorded | Reconcile previous completion under the ownership protocol; record protocol reconciliation as the evidence source |
| GID absent, native status NULL, no valid recovery history or identity | Integrity failure; do not manufacture success |
| Prepare submission or checked result retrieval is uncertain | Preserve enrollment/intent; establish native state before reporting a vote |
| Resource unavailable, status query fails or operation deadline expires | Preserve identity and decision for retry; do not invent completion |
| Foreign/colliding GID or mismatched registered resource | Reject resolution; never touch foreign work |

Native status history is finite, with no promised number of days. The NULL
fallback is standard protocol reconciliation, not independent historical proof:
under the supported contract a durably prepared branch can only be removed by a
resolver following its immutable decision. Established coordinators also handle
unknown branches during recovery as previous completion.
[Narayana recovery handling](https://github.com/jbosstm/narayana/blob/main/ArjunaJTA/jta/classes/com/arjuna/ats/internal/jta/resources/arjunacore/XAResourceRecord.java),
[Atomikos redundant commit retries](https://www.atomikos.com/Blog/ExtremeTransactions5dot0dot102).

Required preconditions are a valid durable coordinator journal, verified backend
identity, preserved acknowledged WAL state, non-reused GIDs and exclusive TM
authority over its branches. Manual contrary resolution, independent data-loss
restore and journal corruption violate that contract. An arbitrary missing GID
or initial unknown transaction is never reconciled as success. When native
evidence contradicts the journal, the driver reports the contradiction rather
than overriding it with the coordinator's intended outcome.

Completion evidence distinguishes a received acknowledgement, a native outcome
observation and protocol reconciliation. Persist it before opening publication.
The API distinguishes the durable decision from completed participant application.

An uncertain prepare/abort must not be forgotten while its original session can
still finish preparing. Preserve the identity and abort intent, quiesce owned
work, resolve late prepared branches and retire the record only when they cannot
reappear. Close only owned unprepared sessions; do not kill an arbitrary PID using
a stale identity check. Bound active-session operations with deadlines.

Inventory is bounded, ordered by GID and scoped to the registered database,
role and coordinator namespace, with continuation. Cross-check every destructive
operation against enrollment. Prepared branches hold locks and require prompt
recovery; report inventory, resolution lag and capacity exhaustion. Do not impose
an expiry that rolls back prepared work after durable COMMIT.

## Pouch and outbox completion

For COMMIT, complete/reconcile PostgreSQL participants first, then apply Pouch
state, attachments, inbox/command receipts and outbox effects. Publish success
only after every participant is applied. Effects remain undispatchable until
that gate opens. Decision and application are separate durable phases, not a
single vote-triggered application followed by immediate transaction deletion.

Atomicity means a common irreversible outcome. Visibility advances as each
resource applies the decision; this design does not provide a shared
instantaneous read snapshot across independent databases.

Preallocate receipt/handoff storage before the irreversible boundary where
possible. Later allocation or reporting failure preserves the transaction ID
and recovery path. Effect/command identities and completion receipts remain
stable across retries. Direct and recovery completion use the same existing
dispatcher wake path; failed notification coalesces durable rediscovery and
cannot undo commit.

Only completed records are eligible for coordinator history retirement.
Unfinished enrollment, decisions and acknowledgements never expire. The default
completed-history policy is 30 days, independently configured from outbox, inbox,
command and DLQ retention; a zero count limit means no count cap. Resource
registrations remain recoverable while unfinished work references them.
After history retirement, lookup returns expired/not-found; it never reconstructs
a decision or executes SQL again. Repeating completion for a retained global ID
returns the existing result rather than starting another transaction.

## Public operations and connection ownership

Expose minimal receiver-style resource, branch and operation handles, alongside
coordinator enrollment. The driver contract covers branch start/end, prepare,
resolution, native status and prepared inventory. It observes and applies a
decision supplied by the coordinator; it does not choose that decision.

Use driver-owned branch connections and public scoped borrows for application
query adapters. Application SQL and PREPARE use exactly that connection, not a
second connection to the same database. Independent recovery connections use
the same verified backend and role. Validate native 2PC capability; a compatible
wire protocol alone does not make another server a supported participant.

Operations expose socket/wake readiness, advancement, monotonic deadline,
cancellation and typed results. Drive public connect polling, asynchronous
submission/flush/consume and checked retrieval. Do not put synchronous native
execution behind an asynchronous API. Blocking conveniences are explicitly
named and documented. Pouch fsync work uses a serialized durability worker;
PostgreSQL connections remain on their owning process/thread. The owner receives
completion readiness and invokes callbacks; workers never invoke Lua callbacks.

Default network-operation deadline is 30 seconds. Callers may provide a shorter
absolute deadline or explicit infinity. Default inventory batches contain 64
rows. Nonblocking connection setup uses Unix sockets, numeric addresses or
prepared `hostaddr`; resolve names outside the owner loop and retain `host` when
authentication/TLS needs it. Enforce deadlines in the operation rather than
assuming connect polling honors `connect_timeout`.
[Connection requirements](https://www.postgresql.org/docs/18/libpq-connect.html).

A generic borrow has ABI size/version, resource/branch identity, exact cpkt
connection pointer, owner process/thread, validity generation and a retained
validity token. It transfers no native ownership. The token outlives every
consumer shell. Invalidate before closing native resources, unwind referenced
results/callback contexts, then reclaim the token after its last reference.

Only one facade owns a branch's protocol at a time. End/prepare requires no
active cursor, pending query, COPY or pipeline state. Outstanding results fail
the branch; cleanup has a deadline. Borrower close requests driver-controlled
abort/release and never frees the native connection. Stale, thread, process and
state mismatches return actionable errors without touching freed pointers.

Application SQL is trusted text. Parameter binding separates values from SQL
but cannot prohibit intentional transaction-control commands. Adapters reject
nested/local transaction helpers during a borrow; detect unexpected native state
at end. This is an ownership contract, not a SQL capability sandbox.

Copy configuration before startup and open connections after fork. Inherited
handles fail validation without native operations. Cancellation/close after
possible preparation hands work to recovery; it does not pretend a prepared
branch rolled back. Lua wraps the same C behavior. Vectis consumes the public
generic borrow rather than private userdata or headers. Pin owners across
callbacks, exceptions, yields and GC; prepare waits for borrow/result release.

Preserve bounded SQLSTATE/provider diagnostics separately from coordinator state
and completion evidence. Never automatically log SQL, parameters, connection
secrets or raw diagnostic bodies.

## Verification

Tests assert observable behavior using deterministic fault barriers:

- Core configure, build and installed consumers work with the entire cpktdb SDK
  absent. ON fails clearly if that provider SDK is missing. Optional provider
  selection leaves the core archive/shared-library bytes identical and introduces
  no native PostgreSQL symbols or startup dependency in core artifacts.
- Explicit provider linking/registration, absent/disabled capability, static and
  shared applications, SDK relocation and provider lifetimes are verified. Provider
  absence preserves unfinished branches and blocks their publication gate.
- SQL and PREPARE use the same physical connection; stale/thread/fork/GC and
  nested ownership failures are explicit.
- Lost prepare response, uncertain checked retrieval and allocation failure at
  journal/command boundaries never invent votes or lose enrollment.
- All YES votes without durable COMMIT recover as ABORT after fencing; durable
  COMMIT survives timeout, owner change, close and lost acknowledgement.
- Native PostgreSQL restart, lost commit/rollback acknowledgement, repeated
  unknown-GID resolution and full-XID status lookup produce correct business rows
  without rerunning SQL.
- Expired native outcome history follows the documented reconciliation rule;
  opposite observed outcomes, incomplete history, wrong backend/role and
  foreign GIDs fail explicitly.
- Late prepare, concurrent/stale matching resolution, collisions, missing
  registration and capacity exhaustion preserve the ownership protocol.
  Capacity exhaustion in the second PostgreSQL participant aborts the already
  prepared first branch and Pouch work, leaving foreign prepared branches intact.
- Pouch startup, replay, lease expiry and cleanup cannot bypass external
  application gates or delete incomplete transaction records.
- Mixed/multiple participants, stable receipts and direct/recovery wakes never
  dispatch outbox work before completion.
- Successful completion leaves no prepared branch; interruption leaves durable
  recovery work. No extra prepared branch is introduced for retention.
- Strict C89 and C++ consumers, sanitizers/Memcheck, static/shared installed and
  relocated SDKs, Lua ownership and the supported PostgreSQL versions pass.
- Rootless Podman Kube tests start their stack when needed, use real readiness
  and host-writable state, tear it down on every exit and assert owned pod/container
  absence. Generated state/credentials stay under ignored build directories.
