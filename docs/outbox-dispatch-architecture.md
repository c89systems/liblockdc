# Threadless transactional outboxes and explicit dispatchers

Status: implemented architecture authority. This document supersedes the
outbox-construction, dispatcher-lifecycle, stateful-delivery, and Lua-dispatch
parts of [the transactional messaging specification](inbox-outbox.md). The
durable record format, transaction rules, inbox/outbox identities, claim
semantics, retry policy, dead-letter representation, and endpoint limitations
in that document remain authoritative.

## Purpose

An outbox producer must not allocate a private dispatcher thread. Route-side
producers only append durable effects; requiring them to own a long-lived
worker, client/session state, notification queues, and recovery work is the
wrong model for web runtimes and for processes that only produce effects.

This cutover splits those roles:

- an **outbox** is a threadless transactional producer and receipt reader;
- an **outbox dispatcher** is an explicit, process-local durable-work
  consumer; and
- a host decides its process topology, scheduling, and language-runtime
  ownership.

The dispatcher cutover shipped in ABI generation 4 after ABI 3. The terminal
retention configuration and statistics introduced ABI generation 5, followed
by per-object state retention in ABI 6. Current collection layouts use ABI 9;
see [ABI compatibility](c-api.md#abi-and-storage-compatibility). Future
incompatible public C layout changes require a new ABI generation. There is no
compatibility constructor and no legacy implicit-thread mode.

`outbox` is deliberately the distributed-systems term for this transactional
messaging facility. It is not a business-workflow engine: activity graphs,
workflow state machines, and supervisor policy belong to hosts such as Vectis
or to a future explicitly named workflow subsystem.

## Ownership boundary

liblockdc owns durable outbox semantics:

- transaction enrollment and terminal decisions;
- command receipts, inbox deduplication, immutable outbox records and payloads;
- claims, renewals, retry scheduling, terminal completion, dead letters, and
  bounded durable recovery;
- bounded direct-key notification and process-local dispatcher coordination.

liblockdc does **not** own a generic supervisor, process manager, IPC router,
web-runtime lifecycle, foreign transport, or application handler policy. In
particular, it never calls a Lua closure from a private thread.

A host such as Vectis owns worker/supervisor process placement, its bounded
route-to-supervisor IPC, handler registration, Lua lane ownership, ingress and
shutdown policy, and any leader election used to choose one dispatcher among
several application runtimes. IPC is a latency hint only; Lockd/Pouch remains
the durable authority.

## Stateful job delivery

Some effects need a durable, fenced checkpoint while a host performs an
at-least-once foreign action. For example, a host may record an activity's
foreign receipt, a completed sub-step, or a result before it decides whether a
redelivery must repeat that action. The outbox supplies that **per-job state
lease**; it does not interpret the state body, select activities, schedule a
graph, or execute a handler.

This deliberately follows the `dequeue_with_state()` model rather than adding
an outbox-wide policy flag:

- `dispatcher->next()` remains the default, stateless pull operation. It
  creates no state sidecar and retains its current I/O, allocation, and
  recovery behaviour.
- `dispatcher->next_with_state()` is the explicit opt-in. It returns the same
  claimed `lc_outbox_job`, but that job owns a state lease available through
  `job->state()`.
- `job->state()` returns a borrowed ordinary `lc_lease *`, owned by the job,
  just as `message->state()` is owned by its queue message. The host uses the
  normal lease `get`, `update`, `mutate`, metadata, attachment, and streaming
  operations. It must not close the borrowed lease itself.
- The job owns both leases. A successful `complete`, `retry`, or
  `dead_letter` consumes the job and its state handle. `close()` invalidates
  both local handles and leaves an unfinished claim for normal expiry
  recovery.

The state key is an internal reserved key in the outbox namespace derived from
the immutable durable `outbox_key`, not from an attempt number, a lease id, an
owner, or a process-local pointer. Thus every retry, expiry recovery, and
dead-letter replay of one effect sees the same state. It is query-hidden and
is never placed in a per-job namespace, so stateful delivery neither pollutes
ordinary application queries nor creates namespace churn. The reserved key
format remains private; callers obtain the state only through the claimed job.

### Paired-claim invariant

`next_with_state()` is all-or-nothing from the caller's perspective. Before a
job is handed to a host, liblockdc must durably establish both the outbox claim
and the associated state lease with matching owner and lifetime. If state
lease acquisition, state-handle construction, verification, or local
allocation fails, no job escapes. The implementation directly compensates any
published partial claim and clears a newly acquired sidecar lease, following
the existing Pouch stateful-dequeue rollback rule. It must not leave recovery
to discover an inaccessible partial handoff.

The paired leases use the same requested claim TTL. `job->renew()` renews both
before it reports success and refreshes the public job expiry only when both
renewals are durable. A partial renewal failure is an error: the host must stop
assuming ownership and let the normal fenced expiry/recovery path decide the
next attempt. This avoids a fresh outbox claim paired with a stale state lease,
or the reverse.

Renewal preserves the current parent and checkpoint state versions. It may be
called before or after checkpoint mutations, repeatedly, without invalidating
the CAS preconditions of a later complete, retry, or dead-letter decision.

Terminal operations first persist the ordinary terminal outbox transition
under the outbox claim, then release both leases. A successful checkpoint body,
mutation, metadata operation, removal, or attachment operation is committed
immediately under the paired state lease's fencing precondition; it is never
left as a claim-transaction stage that expiry recovery could discard. This
makes a checkpoint written before a crashed handler available to the next
claimant. It is still not atomic with an arbitrary HTTP, RPC, broker, or
filesystem side effect. The correct handler protocol remains:

1. read the job state and decide whether the external action is already known
   to have completed;
2. use the immutable `effect_key` at the foreign boundary;
3. persist the resulting checkpoint or foreign receipt through `job->state()`;
   and
4. complete, retry, or dead-letter the job.

That ordering makes a crash after a foreign action recoverable by the next
stateful delivery, but does not claim impossible global exactly-once semantics.
Handlers that need one atomic state-and-outbox terminal decision must use a
future explicit transaction surface; this extension must not silently create a
long-lived XA transaction around foreign work.

State is retained across retry, expiry recovery, and dead-letter replay. A
stateful handoff persistently marks the envelope as having a possible
checkpoint before the handler receives it. That fact survives every retry, so
even a later ordinary `next()` claimant retires the exact sidecar on
completion. A successful completion does not need the private job state for
another delivery: durable terminal command results belong in the command
receipt and compact completion evidence belongs in `lc_outbox_completion`.
Completion first writes one fsynced private cleanup marker, then publishes the
terminal parent, then removes the sidecar and its attachments while it still
owns the parent claim. This also handles a valid handler-side `state:remove()`:
any remaining attachments are retired through the exact-key recovery path.
The parent decision is committed before that irreversible cleanup. Therefore a
cleanup I/O failure cannot make a completed foreign effect redeliver without
its checkpoint: completion remains successful, leaves the marker durable, and
a later bounded stateful pull completes the exact cleanup. Failures before the
parent decision remains durable leave the job retryable as usual.

The marker closes the power-loss gap between terminal publication and sidecar
removal. Every later stateful pull checks at most one marker, loads only its
named parent record, and deletes only the paired sidecar and its attachments
after confirming that the parent is `completed`. A marker whose parent remains
pending, claimed, or retryable may precede an uncommitted terminal write, so it
is retained for a later bounded revisit; a later completion overwrites the same
marker identity. A dead-letter marker is discarded without deleting its
checkpoint. An absent parent is stale control data (for example, when explicit
dead-letter deletion follows interrupted cleanup) and is retired rather than
blocking later stateful pulls. This work is bounded and has no namespace scan,
snapshot, query, or allocation on the stateless pull path. It is deliberately
stateful-pull maintenance rather than generic Pouch retention, because only
the outbox can interpret the parent/sidecar relationship.

Dead-letter remains an operator-visible replay state, so its sidecar survives
replay and remains until a later completion, `delete_dead_letter()`, or Pouch's
automatic dead-letter retention reclaims the terminal envelope. Both explicit
and automatic removal delete the sidecar and its attachments before consuming
the envelope and payload, preserving a retryable management boundary on failure. Automatic
retention is separately scheduled from a durable exact-key marker and never
performs a namespace query or scan on the delivery path; its defaults and
crash/multiwriter semantics are specified in
[Pouch dead-letter retention](pouch-dead-letter-retention.md). The generic
state-retention sweep skips reserved outbox-state keys: only parent-aware
outbox transitions may reclaim them. An application that has safely exported
or no longer needs a large in-flight checkpoint may explicitly remove its
state through the borrowed lease before terminalizing the job.

Completed envelopes, accepted inbox receipts, and completed/failed commands
have independent 30-day [terminal receipt retention](pouch-terminal-retention.md)
defaults. Optional count caps can reclaim before expiry. This is dispatcher
maintenance, separate from checkpoint cleanup on a stateful pull. Removing a
receipt ends its duplicate/idempotency protection and command-status lookup;
pending or claimed work is excluded. Producers and foreground reads remain
threadless and do not trigger this reclamation.

Stateful and stateless consumers can safely compete at the storage level, but
a deployed handler that relies on checkpoints must ensure every consumer for
that effect kind uses `next_with_state()`. Liblockdc cannot infer that host
policy from an opaque effect kind or state body, so it must document rather
than pretend to enforce it.

## Handles and lifetimes

### Threadless `lc_outbox`

`lc_client_new_outbox()` validates, canonicalizes, deep-copies, and retains
an `lc_outbox_config`. It creates no thread, private client clone,
notification queue, recovery loop, claim, or foreign-effect execution.

The outbox receiver retains the producer-side operations:

- lazy outbox transaction creation;
- command acceptance, lookup, result streaming, and resume;
- inbox acceptance;
- outbox append;
- outbox transaction participant acquisition, commit, rollback, and close;
- receipt/result cleanup and outbox close.

It deliberately has no `next`, stats, reconciliation, dead-letter, claim, or
job-terminal operation. These belong only to a dispatcher.

`lc_outbox_begin()` creates a threadless, lazy transaction receiver. It does
not create a durable transaction marker or mint an id by itself. Its first
participating operation—domain `acquire`, command acceptance, inbox acceptance,
or outbox append—lazily mints an explicit xid and prepares that participant for
one durable XA decision. Every later participant uses that xid. This lets a host
such as Vectis stage a domain update before it appends an outbox effect without
giving a domain-first transaction weaker recovery semantics.
A transaction that reaches `commit()` with no participant is invalid; closing
such an empty transaction is a local no-op.

If a command, inbox, or outbox operation later finds a pre-existing committed
duplicate after the transaction has acquired a domain participant, liblockdc
makes the whole transaction rollback-only and releases its participants. It
returns the normal duplicate result but rejects `commit()`. This prevents a
domain update from becoming visible without the required idempotency barrier or
outbox effect. A duplicate before any domain participant is enrolled leaves
the transaction usable: for example, a fresh inbox receipt may commit even
when its separately owned command is already a durable duplicate.

### Commit-published outbox receipts

An outbox key is safe to wake only after its transaction has committed. The
cutover therefore separates a staged append from its committed receipt:

- a fresh `outbox->append()` returns its transaction but no public
  outbox receipt;
- a transaction's `append()` stages another effect and returns no fresh
  outbox key; and
- `transaction->commit()` returns one owned receipt for every outbox effect
  made durable by that decision.

The C commit result is an initialized/cleanup-owned result record containing a
receipt array:

```c
typedef struct lc_outbox_commit_result {
  lc_outbox_receipt *outbox_receipts;
  size_t outbox_receipt_count;
} lc_outbox_commit_result;

void lc_outbox_commit_result_init(lc_outbox_commit_result *result);
void lc_outbox_commit_result_cleanup(lc_outbox_commit_result *result);
int lc_outbox_transaction_commit(lc_outbox_transaction *transaction,
                                   lc_outbox_commit_result *out,
                                   lc_error *error);
```

The result is populated only after the backend reports its durable terminal
commit. On failure or rollback it is empty. A duplicate top-level or later
transaction append is different: it finds an already committed effect and
returns that existing durable receipt directly. Such a receipt is already safe
to notify. An attached local dispatcher receives fresh committed keys
internally at the same boundary; it may also receive an existing duplicate key.
A separate host forwards only a successful commit result's fresh receipts or an
already committed duplicate receipt. No public API exposes a fresh staged
outbox key that could accidentally be sent before commit.

A later duplicate append after a staged domain participant follows the
rollback-only rule above: its existing receipt is safe to notify, but the
current transaction has no fresh commit result and cannot publish its earlier
staged domain work.

Commit-result storage is prepared before the terminal backend decision. If the
library cannot allocate or copy the required result, it returns before voting
commit and leaves the transaction available for rollback or retry. Once the
backend confirms a commit, returning its receipts cannot fail due to local
allocation. An indeterminate backend error returns no fresh receipt even if a
later recovery proves the commit durable; reconciliation is then the required
repair path.

Closing an outbox closes any still-open owned transaction by best-effort
rollback, releases any attached dispatcher reference, and releases its client
reference. It never stops a dispatcher.

### `lc_outbox_dispatcher`

`lc_outbox_dispatcher` is an opaque receiver with an independent explicit
lifecycle. It owns:

- bounded direct-key candidate and delayed-retry wake queues;
- a private C-only notification/recovery thread and, for remote endpoints, its
  dedicated bounded dispatcher client;
- targeted claim/read only when a consumer requests work;
- indexed reconciliation, claim-expiry recovery, retry, dead-letter controls,
  and dispatcher statistics.

It does not own application callbacks. Its private dispatcher thread queues
candidate keys and recovery intent, but never claims a job or invokes C or Lua
application code.

One live dispatcher exists at most once for a `(client instance, canonical
outbox configuration)` registry key. The key includes every configuration
field, after defaults and validation have been applied: namespace, owner,
transaction and claim TTLs, retry policy, notification capacity, recovery
cadence, shutdown request timeout, dead-letter startup policy, and every
dead-letter and terminal-retention duration/count/byte limit. Comparing
canonical owned values, rather than caller pointers, makes the result stable.

Different `lc_client` instances never share a registry entry. They can safely
compete for durable claims, but a host that needs one process-level dispatcher
must arrange that policy itself. A Vectis supervisor therefore creates its own
post-fork client and obtains one dispatcher from it; route workers use distinct
threadless producer clients and send committed receipt keys to that supervisor.

### Acquisition, attachment, and closure

The public C shape is:

```c
typedef struct lc_outbox_dispatcher lc_outbox_dispatcher;

int lc_client_new_outbox(lc_client *client,
                           const lc_outbox_config *config,
                           lc_outbox **out, lc_error *error);
int lc_outbox_begin(lc_outbox *outbox,
                      lc_outbox_transaction **out, lc_error *error);
int lc_client_new_outbox_with_dispatcher(
    lc_client *client, const lc_outbox_config *config,
    lc_outbox_dispatcher *dispatcher, lc_outbox **out, lc_error *error);
int lc_outbox_dispatcher_get_or_start(lc_outbox *outbox,
                                        lc_outbox_dispatcher **out,
                                        lc_error *error);
int lc_outbox_dispatcher_notify_outbox_key(
    lc_outbox_dispatcher *dispatcher, const char *outbox_key,
    lc_error *error);
```

The client receiver exposes both outbox constructors. The outbox receiver
exposes `get_or_start_dispatcher`; both receivers retain eight trailing
`void *` extension slots. Every fallible receiver operation has the matching
free-function form.

`get_or_start_dispatcher()` is linearizable with stop and registry cleanup:

- concurrent compatible calls obtain retained references to one live instance;
- a dispatcher that is stopping or failed is not returned as running;
- an acquisition racing stop returns a closed/busy error until cleanup permits
  a replacement; and
- no operation temporarily creates two live dispatchers for one registry key.

`new_outbox_with_dispatcher()` attaches an already acquired compatible
dispatcher to a producer. Compatibility requires the same client instance and
canonical configuration. A successful commit on an attached outbox sends
each newly committed outbox receipt key to that dispatcher's local wake queue.
The operation is only a fast path: queue overflow, an unavailable dispatcher,
or a later crash cannot invalidate the durable commit and is repaired by
reconciliation.

An unattached outbox is still complete for production. Its caller receives
outbox keys only in committed receipts and may forward them to a host-owned
dispatcher through host IPC. It must never forward a key from a failed or
rolled-back transaction.

The registry retains a started dispatcher until explicit `stop()` completes or
its client closes. `lc_outbox_dispatcher_close()` releases only one caller's
handle; it does not stop a dispatcher, including when it happens to be the last
external handle. This deliberately lets a lazily started dispatcher remain
available for later producer attachments and asynchronous work. Hosts that no
longer need it call `stop()`; `wait()` is available to a different lifecycle
observer or after a timeout.

`stop(deadline_ms)` is idempotent. It changes the dispatcher to stopping,
rejects new wake and work requests, and waits up to its deadline for private
infrastructure and handed-out jobs to become terminal. A timeout leaves it
stopping; a later `wait(deadline_ms)` observes the same shutdown and never
restarts it. Stop never invalidates an already handed-out job: it remains able
to reach a terminal outcome until its owner closes it or its claim expires.
Client close stops and joins every registered dispatcher's private worker before
releasing its client state. It does not wait for caller-owned handed-out jobs:
each such job retains the client state required to stream, renew, complete, or
close itself. Dispatcher shells remain closeable but reject new work after the
stop; any unfinished claim is left for normal expiry recovery.

## Dispatcher activation and work limits

Acquisition starts private notification/recovery infrastructure but is
**passive**: it performs no namespace query, recovery sweep, or claim merely
because a dispatcher exists or has a wake. A blocking consumer request or an
explicit `reconcile()` starts durable recovery. `next(0)` is an in-memory
non-query probe. Claiming is demand-driven: a consumer asks for one job only
when it has execution capacity. The first request selects a consumer mode:

- raw C consumption uses `dispatcher->next()`;
- Lua `dispatcher:run()` and `dispatcher:pump()` bind their owner-state sink
  before requesting work; and
- a Lua owner state cannot be replaced, while concurrent raw C `next()` calls
  share the raw pull mode.

A Lua handler cannot recursively call `run()` or `pump()` on its dispatcher,
including through an alias. The current handler must return so its outcome is
applied before the caller-owned loop consumes another job. `stop()` and
`wait()` are likewise rejected from that handler because they would wait for
the handler-owned active claim.

`notification_capacity` is one shared bound across direct-key candidates and
delayed retry wakes; it does not authorize preclaiming. `next()` removes or discovers one candidate,
claims it, and returns the owned job to the caller. A Lua lane requests one
job only when it has an idle handler slot. A Vectis source router does the same
for an available logical-supervisor lane. Raw C callers choose their own bounded
worker capacity by issuing only that many concurrent or outstanding `next()`
requests. No unbounded ready queue or library-owned claim set exists.

On a Pouch root opened with `query_indexing=false`, dispatcher recovery and
dead-letter management use the explicit `scan` query engine. They never call
`flush_index` or silently re-enable index maintenance; this preserves the
root's non-query opt-out while retaining durable outbox recovery.

Stopping first prevents new claims and wake acceptance. It then waits only up
to its deadline for active jobs to reach a terminal outcome. Remaining claims
are released where safe or allowed to expire and are repaired by normal durable
recovery. A committed outbox record is never reported as failed because its
wake arrived after shutdown began.

## Direct-key notifications and recovery

`lc_outbox_dispatcher_notify_outbox_key()` accepts only a non-empty durable
outbox key already known by the caller to have committed. It copies and
deduplicates the key in the existing bounded candidate queue and wakes waiting
consumers. The next consumer request uses a targeted claim/read path. It does
not query or scan the outbox namespace solely because it received a key.

Duplicate notifications coalesce. A full queue increments observable overflow
state and schedules one indexed reconciliation candidate; it never drops
durable work.

Reconciliation discovers candidates but never claims them. It uses a bounded
streaming query and a resumable cursor: when the shared candidate budget is
full, it retains enough cursor state to resume later rather than materializing
the remaining namespace keys. A consumer request drains one candidate before
continuing that cursor. This holds for indexed and explicit scan recovery, so a
large retained outbox population cannot turn a wake or recovery pass into an
unbounded allocation.
Unknown, already-terminal, not-yet-eligible, or actively claimed keys are
normal no-work candidates, not evidence that the durable outbox was lost.

Recovery remains mandatory and is the correctness path for:

- process crash or restart;
- a full or closed host IPC channel;
- direct-notification overflow;
- lease expiry or a failed terminal action;
- missed cross-process or cross-host wakeups; and
- dead-letter startup replay when configured.

Pouch local recovery uses its efficient configured query/index policy. Remote
Lockd and shared Pouch may later supply cross-process change notifications, but
those can only request reconciliation and cannot replace durable recovery.

## C receiver contract

The dispatcher receiver provides the following operations, with free-function
equivalents and public API comments:

```c
struct lc_outbox_dispatcher {
  int (*next)(lc_outbox_dispatcher *self, long timeout_ms,
              lc_outbox_job **out, lc_error *error);
  int (*next_with_state)(lc_outbox_dispatcher *self, long timeout_ms,
                         lc_outbox_job **out, lc_error *error);
  int (*notify_outbox_key)(lc_outbox_dispatcher *self,
                           const char *outbox_key, lc_error *error);
  int (*get_stats)(lc_outbox_dispatcher *self, lc_outbox_stats *out,
                   lc_error *error);
  int (*reconcile)(lc_outbox_dispatcher *self, lc_error *error);
  int (*replay_dead_letter)(lc_outbox_dispatcher *self,
                            const char *outbox_key, lc_error *error);
  int (*delete_dead_letter)(lc_outbox_dispatcher *self,
                            const char *outbox_key, lc_error *error);
  int (*export_dead_letters)(lc_outbox_dispatcher *self,
                             const lc_dead_letter_export_opts *options,
                             lc_sink *dst, lc_dead_letter_export_res *out,
                             lc_error *error);
  int (*stop)(lc_outbox_dispatcher *self, long deadline_ms, lc_error *error);
  int (*wait)(lc_outbox_dispatcher *self, long deadline_ms, lc_error *error);
  void (*close)(lc_outbox_dispatcher *self);
  void *reserved_extension_slots[8];
};
```

`next()` is the raw expert pull surface and one unit of demand. It waits for or
discovers one candidate, claims it, and returns one owned `lc_outbox_job`; the
job retains its active claim until `complete`, `retry`, `dead_letter`, or
close. Existing terminal operations and payload streaming semantics remain
unchanged. A terminal failure must leave the claim recoverable through retry or
expiry, never silently consume it.

`next_with_state()` has the same timeout, demand, recovery, and no-query
semantics, but its returned job has `job->state()` and the paired-claim
invariant described above. `lc_outbox_job` gains the matching receiver method
and a free-function wrapper:

```c
lc_lease *(*state)(lc_outbox_job *self);
lc_lease *lc_outbox_job_state(lc_outbox_job *job);
int lc_outbox_dispatcher_next_with_state(lc_outbox_dispatcher *dispatcher,
                                         long timeout_ms,
                                         lc_outbox_job **out,
                                         lc_error *error);
```

Both public methods occupy receiver-shell extension capacity, and the job
receiver will reserve eight tail extension pointers in the same cutover. The
receiver method and free-function wrapper are introduced together; neither is
a dangling one-style-only API. `job->state()` returns `NULL` for a stateless
job without treating that absence as an error.

The producer and transaction receivers' `append` operations return an
optional receipt only for an already committed duplicate. Their `commit`
operation accepts an `lc_outbox_commit_result *` as above. That result is the
only public source of **fresh** wakeable outbox keys.

`lc_outbox_stats` is dispatcher-only in this cutover. Its live gauges are
`pending_candidates`, `delayed_wakes`, and `waiting_consumers`; the old
preclaimed `ready_jobs` gauge is removed. Its monotonic direct-notification,
overflow, recovery-query, recovered-candidate, claim-loss, and payload-failure
counters retain their existing observability role. None of these reads queries
the durable namespace. Pouch additionally reports monotonic
`dead_letter_reclaims` and `dead_letter_reclaim_failures`; these are control
marker maintenance outcomes, including harmless stale-marker disposal after a
replay, rather than foreground-delivery work.
Terminal receipt maintenance similarly reports `terminal_reclaims` and
`terminal_reclaim_failures`. Reclaim counters include safely retired stale
markers and therefore are not counts of application records deleted. A damaged
marker remains retryable and reports an error while the advisory cursor allows
other eligible work to progress.

## Lua outbox and dispatcher façade

Lua mirrors the ownership split exactly.

```lua
-- Producer or request domain: no dispatcher thread is created.
local outbox = assert(client:new_outbox({
  namespace = "myapp.orders",
  max_attempts = 12,
}))

outbox:transaction(function(tx)
  tx:append(entry, payload_source)
end)

-- Worker/service domain, normally a distinct process with its own client.
local worker_client = assert(lockdc.open(worker_client_config))
local worker_outbox = assert(worker_client:new_outbox({
  namespace = "myapp.orders",
  max_attempts = 12,
}))
local dispatcher = assert(worker_outbox:dispatcher())
assert(dispatcher:run({
  handlers = {
    ["order.webhook"] = function(job)
      local info = job:info()
      -- Convenience for this deliberately small JSON example. It materializes
      -- the payload; a production transport path uses write_payload() to its
      -- streaming destination instead.
      local payload = assert(job:read_payload_json())
      local ok, err = deliver(payload, info.effect_key)
      if not ok then
        return job:retry({ diagnostic = err, delay_seconds = 30 })
      end
      return job:complete()
    end,
  },
}))
```

`client:new_outbox(config[, { dispatcher = dispatcher }])` creates a
threadless producer, optionally with a compatible dispatcher attachment.
`outbox:dispatcher()` is the Lua form of get-or-start and takes no
configuration: the canonical outbox configuration is the sole policy source.

The outbox façade includes transaction production, receipt access, and
close/garbage-collection cleanup. It has no `next`, claim, retry, dead-letter,
or dispatcher statistics methods. The dispatcher façade includes `run`,
bounded `pump`, raw `next`, `notify_outbox_key`, `stats`, `reconcile`,
dead-letter controls, `stop`, `wait`, and close. It has no producer shortcut.

`outbox:begin()` and `outbox:transaction(fn)` create a lazy transaction
receiver, not a durable transaction marker. Its first participant may be
`acquire`, `append`, `accept_inbox`, or `accept_command`; that operation
lazily mints the explicit xid that anchors every later participant. This lets a
Vectis transaction stage a domain update before its outbox append while
remaining one durable XA decision. On normal callback return, the façade
commits the concrete transaction; on a Lua error or staging failure, it rolls
back and rethrows/returns the structured failure. Callback-scoped transactions
reject explicit `commit`, `rollback`, and `close`: the façade is the sole owner
of that terminal decision. A
duplicate first command/inbox/outbox operation is returned as its ordinary
durable duplicate result. Advanced users may keep using the explicit producer
operations when they need to inspect and control each transaction step.

Returning normally without a participant is `LC_ERR_INVALID`; the façade does
not commit an empty pseudo-transaction. A duplicate first record is the sole
normal callback path without a durable participant. The callback may inspect
that duplicate result and return, after which the façade returns it without a
terminal decision. A duplicate discovered after a domain participant has
staged makes the proxy rollback-only; the façade rolls it back and returns that
duplicate result rather than committing partial work. A duplicate before any
domain participant may still let the callback commit an independently fresh
outbox receipt, such as an inbox delivery whose owned command was already
committed.

On success, `outbox:transaction(fn)` returns the same commit result as the
explicit transaction's `commit()`, including `outbox_receipts`. This gives a
host its forwardable keys only after the durable decision. A duplicate first
append instead returns its existing `receipt` and no commit result, because no
new effect was committed.

`run` is a blocking owner-state loop for a dedicated worker or service process.
`pump` invokes at most its configured bounded amount of work synchronously on
the calling Lua state, for hosts with their own event loop. Neither permits a
private liblockdc thread to access Lua. Calling either from an HTTP route is
supported only insofar as the host itself allows it, but is documented as an
incorrect deployment for foreign-effect dispatch because it puts effect latency
on the request path.

`pump(options)` defaults to one job and a zero wait. Its `max_jobs` must be a
small positive bounded integer and its `timeout_ms` is a non-negative value no
greater than the configured dispatcher shutdown timeout. It returns the number
of handler outcomes
processed and never starts a second Lua owner loop. A host schedules another
pump when it is ready; liblockdc does not create an event-loop thread.

`run`/`pump` bind exactly one Lua VM and one handler map while at least one Lua
wrapper for the dispatcher remains live. Raw `next` instead binds raw-pull
mode. The first consumption mode is permanent for that live-wrapper lifetime:
a second run/pump, raw pull after handler activation, handler activation after
raw pull, a different Lua VM, or a conflicting handler map fails
deterministically. Dropping every Lua dispatcher wrapper releases the handler
map and its Lua binding; a later wrapper establishes a new caller-owned Lua
binding for the still-live native dispatcher. Handler registrations are
validated before activation: the map is non-empty, every key is a non-empty
outbox kind, and every value is a function. An
unhandled kind leaves its claim for normal expiry recovery and returns a
structured error; liblockdc does not invent a Vectis policy for it.

A Lua handler receives a claimed job with `job:write_payload(sink)` for true
streaming to a path, file descriptor, or callback sink. `job:read_payload_json()`
materializes and decodes the full JSON payload, so it is suitable only for
intentionally bounded bodies.
It may consume that payload only during the call and must not retain the job
after returning. `job:complete()`, `job:retry(options)`, and
`job:dead_letter(diagnostic)` create typed outcomes; the façade applies the
actual terminal mutation after the handler returns. An exception or a handler
return without an outcome produces the configured retry outcome. A long foreign
operation must call `job:renew(ttl_seconds)` before its claim expires, exactly
as raw C and raw Lua consumers do; the binding never races a Lua callback with
a hidden concurrent lease operation.

`retry()` accepts no argument or an options table.
`retry({ delay_seconds = n, diagnostic = message })` supports a bounded
reschedule delay and diagnostic. It maps to the existing durable retry policy;
rescheduling is therefore a normal retry terminal outcome, not an in-memory
timer.

Lua exposes the same explicit choice rather than inventing a separate state
API: `dispatcher:next_with_state(timeout_ms)` returns a job whose
`job:state()` is an ordinary Lua lease wrapper, while `dispatcher:next()`
remains stateless. `dispatcher:run()` and `dispatcher:pump()` accept
`with_state = true` in their existing options when their handlers need it;
they acquire each job through the stateful C operation and expose the same
`job:state()` wrapper only for that invocation. The wrapper is a borrowed view
of the job-owned paired lease: do not close it and do not use it after an
outcome, handler failure, or `job:close()`. While the checkpoint is streaming,
Lua rejects job closure and terminal decisions until the sink returns, so a
callback cannot free the native lease under its active read. Lua lease
streaming and mutation remain the existing core lease operations; this facade
must not materialize state bodies or add a parallel JSON-only checkpoint
format.

## Vectis integration contract

Vectis is not implemented in this repository. Its required liblockdc contract
is nevertheless explicit. This section defines only the liblockdc outbox
contract; it does not redefine Vectis supervisor ownership or lifecycle policy:

```text
route worker                         supervisor process
------------                         ------------------
threadless outbox                  outbox dispatcher + Lua lane
  └─ durable commit ─ receipt key ─> bounded host IPC ─> notify_outbox_key()
       full ────────────────> OUTBOX_RECONCILE signal ─> reconcile()
       closed/lost ────────────────────────────> startup recovery
                                                     └──> capacity-aware next()
                                                          → claim → handler → outcome
```

1. Each application declares one canonical outbox configuration per durable
   namespace.
2. Route workers construct threadless producers using their own post-fork
   clients. They begin one lazy outbox transaction, mutate domain state, and
   append outbox effects atomically.
3. After a successful commit, a worker copies every returned receipt key to
   Vectis's bounded outbox-key channel. On its first full send it emits one
   payload-free reconciliation wake; it never blocks a route or attempts effect
   delivery. A closed or lost handoff is repaired by durable startup recovery.
4. The supervisor process owns a separate client, obtains the one local
   dispatcher, receives keys/wakes, and calls `notify_outbox_key` or
   `reconcile`.
5. The supervisor requests a job only when its chosen logical supervisor and
   Lua lane have capacity. It selects `next_with_state()` for an effect kind
   whose supervisor uses durable checkpoints, otherwise `next()`, then applies
   the job outcome after the handler returns.

Vectis uses the dispatcher's raw C pull mode, not liblockdc's dependency-native
Lua `run()`/`pump()` bridge. Its one native source router can therefore call
the stateless or stateful `next` variant for whichever of several logical
supervisor lanes has capacity, then invoke the selected Vectis-owned Lua lane.
The liblockdc dispatcher sees only claimed jobs and terminal outcomes; it does
not own Vectis handler registration or try to bind one global Lua handler map.

Vectis's high-level `tx:update_json(order)` resolves to its configured domain
participant acquisition. It may be the lazy outbox transaction's first
participant, captures the endpoint-minted xid, and lets a later
`tx:append()` join the same implicit-XA decision. If Vectis rejects an
unregistered outbox kind after staging that domain participant, it marks the
enclosing transaction failed: commit is impossible and all staged state rolls
back before the route can report success. Raw liblockdc remains available for
deliberately decoupled producers that select a different unhandled-job policy.

For a remote Lockd root, route and supervisor clients are naturally independent
connections. For local Pouch, a Vectis route-worker process and supervisor
process cannot share an exclusive single-writer session across a fork boundary.
A multi-process Pouch deployment must explicitly use the supported shared-root
mode (`single_writer=false`) in the one canonical client/root configuration.
Default exclusive mode continues to reject the second live process rather than
silently weakening its ownership guarantee. liblockdc never carries a Pouch
client or dispatcher thread across the fork boundary.

The channel carries copied receipt-key bytes only. It is neither a second queue
nor a persistence layer. A failed channel send, overflow, restart, or lost
wake changes latency only; reconciliation recovers durable work. Vectis must
reject duplicate `(outbox namespace, kind)` handler ownership at declaration
time and reject high-level unregistered kinds before committing a transaction.

## Documentation and examples

The user-facing outbox documentation must explain the complete deployment
pattern, not merely individual calls:

1. a request transaction commits domain state and durable intent together;
2. it returns immediately after commit, never after delivery;
3. receipt keys are an optional low-latency wake only after commit;
4. a dedicated dispatcher/worker performs the foreign effect with `effect_key`
   idempotency;
5. completion, retry/reschedule, and dead-lettering are durable terminal
   decisions; and
6. restart and reconciliation repair every missing wake.

For a Vectis supervisor integration review, start with the self-contained
[Vectis outbox integration handover](outbox-vectis-handover.md). It summarizes
the required host topology, IPC, recovery, ownership, and Pouch deployment
rules while this document remains the detailed architecture authority.

Examples must include a direct liblockdc deployment with a producer process
and a dedicated Lua dispatcher process, plus the Vectis-facing lifecycle
diagram above.
They must never demonstrate `dispatcher:run()` inside a route handler or an
implicit dispatcher created by producer construction.

The checked-in pair is `examples/outbox_producer.c` and
`examples/lua/outbox_dispatcher.lua`. The producer commits one effect and
exits; the Lua process owns the dispatcher and either blocks in `run()` or uses
the explicit one-shot `LOCKDC_OUTBOX_ONCE=1` demonstration mode. Their Pouch
endpoint explicitly uses `single_writer=false` because they are separate
processes. This is an executable topology example, not a recommendation to
weaken the default single-writer mode for single-process deployments.

## Required verification

Unit, integration, Lua, and end-to-end coverage must prove observable
invariants, including failure paths:

- `new_outbox()` creates no dispatcher thread, client clone, claim, or scan;
- producer transaction, command, inbox, outbox, XA, rollback, and receipt
  semantics remain unchanged for Pouch;
- lazy transaction coverage proves that `begin()` creates no durable marker,
  an update-first domain participant and later outbox append commit as one
  decision, an empty commit is invalid, and a later duplicate after a domain
  participant rolls that decision back without exposing domain state or a fresh
  receipt; a fresh inbox plus its already-committed owned command remains
  independently committable;
- compatible concurrent acquisition returns exactly one dispatcher, while a
  configuration mismatch, stop race, failed dispatcher, and client close leave
  no stale or dangling instance;
- outbox attachment performs post-commit local wake only after durable
  success; outbox close does not stop the dispatcher; uncommitted close
  cannot expose a wakeable receipt;
- commit-result allocation failure occurs before a terminal vote; an
  indeterminate terminal error exposes no fresh receipt and is repaired only
  through durable recovery;
- dispatcher acquisition and direct notification use no namespace scan and no
  claim before blocking consumer demand or explicit reconciliation; `next(0)`
  is a non-query probe; duplicate wakes coalesce, and queue overflow/restart/closed IPC
  equivalently recover through reconciliation;
- reconciliation uses bounded streamed pages and a resumable cursor under a
  full candidate budget, without materializing or preclaiming the namespace;
- raw C and Lua dispatch preserve payload streaming, claim renewal, complete,
  retry/reschedule, dead-letter, terminal failure, expiry, and startup replay;
- stateless delivery has no sidecar read, write, lease, query, allocation, or
  timing regression; stateful delivery returns one borrowed ordinary lease
  whose stable state survives retry, expiry recovery, and dead-letter replay;
- a stateful claim is all-or-nothing across state acquisition, verification,
  allocation, and caller handoff; injected failures restore the outbox
  candidate and release the state lease without an inaccessible claim;
- paired renewal is never reported successful when either lease is stale;
  terminal success and local close invalidate the borrowed state handle;
  terminal failure preserves both retryability and state contents;
- generic Pouch retention never removes a sidecar whose parent may retry;
  normal completed delivery removes the sidecar and its attachments under its
  held claim, while a
  durable one-marker-at-a-time stateful-pull reclaimer repairs any interrupted
  cleanup without a namespace scan; dead-letter deletion removes its envelope,
  payload, sidecar, and attachments through a retryable management boundary;
- two stateful Pouch dispatchers competing for one effect observe one fenced
  state owner at a time, while a stateful handler can resume after a crashed
  owner without replaying a checkpointed foreign action; and
- raw Lua `next_with_state`, Lua `run`/`pump` with `with_state`, state
  mutation, handler exception, and terminal invalidation mirror the C
  ownership and no-materialization contract;
- Lua run/pump executes only on its caller Lua state; handler exceptions,
  cancellation, renewal failure, terminal failure, empty transaction, duplicate
  anchor, stop, and duplicate binding are deterministic;
- stop timeout, later wait, and client-close invalidation preserve handed-out
  job recovery without leaving a live thread or dangling client reference;
- multiple Lua worker processes safely compete for a Pouch namespace in its
  supported shared-root configuration; and
- performance gates demonstrate bounded candidate memory and deterministic
  query/claim counts for direct wake and reconciliation, and that passive
  producers add no dispatcher/recovery overhead to request construction or
  commits.

The full release matrix, Lua SDK/package checks, and Pouch e2e coverage must
run after the cutover. Vectis-specific supervisor and IPC tests belong in
Vectis, but liblockdc supplies deterministic dispatcher-notification fixtures
that let Vectis prove its integration without timing sleeps.
