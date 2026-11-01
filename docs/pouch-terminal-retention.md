# Pouch terminal receipt retention

Pouch automatically reclaims completed outbox envelopes, accepted inbox
deduplication receipts, and completed or failed command receipts. Each has an
independent 30-day default. Pending commands and pending, retrying, or claimed
outbox effects are excluded. [Dead-letter retention](pouch-dead-letter-retention.md)
keeps its separate 30-day, 1,024-envelope, 64-MiB default.

## Configuration

The C `lc_outbox_config` and Lua `client:new_outbox()` use the same fields:

| Records | Time field | Optional count field | Time starts at |
| --- | --- | --- | --- |
| Completed outbox envelopes | `completed_retention_seconds` | `completed_max_count` | Completion |
| Accepted inbox receipts | `inbox_retention_seconds` | `inbox_max_count` | Acceptance |
| Completed/failed commands | `command_retention_seconds` | `command_max_count` | Terminal decision |

Time fields accept zero for the 30-day default, positive seconds for an
override, or `-1` to disable reclamation for that type, including its count
cap. Other negative values and unsupported future timestamps are rejected.
Count caps are **disabled by default**: zero or C `SIZE_MAX` is unbounded;
Lua also accepts `false`. A positive cap permits reclamation before the time
window expires. Caps are asynchronous service-protection limits, not admission
quotas, and a burst or held lease can temporarily exceed them.

```lua
local outbox = assert(client:new_outbox({
  namespace = "orders",
  completed_retention_seconds = 30 * 86400,
  inbox_retention_seconds = 60 * 86400,
  command_retention_seconds = 30 * 86400,
  completed_max_count = false,
  inbox_max_count = false,
  command_max_count = 100000,
}))
local dispatcher = assert(outbox:dispatcher())
```

Choose windows that cover the maximum retry, redelivery, and client-status
window for each boundary. Removing a receipt ends duplicate/idempotency
protection: the same identity can execute again. Command status and owned
result attachments disappear together. Applications needing longer audit
history should keep it in dedicated archival storage.

All producers and dispatchers for one namespace must use the same retention
configuration. Explicit settings are rejected for remote lockd; this policy
qualifies local Pouch only. The added public configuration and statistics
fields were introduced in shared-library ABI generation 5. Current collection
receiver/capability layouts use core ABI 9; rebuild C consumers and the optional
PostgreSQL provider against the same core. See
[ABI compatibility](../README.md#c-abi-and-storage-compatibility) and
[upgrade instructions](../CHANGELOG.md#upgrading-from-024x).

## Scheduling and safety

A live explicit outbox dispatcher is required. Producers stay threadless;
receipt reads and foreground pulls do not perform reclamation. Dispatchers
discover durable work on startup and at most a minute after a shared writer
publishes a previously unknown marker. Expiry makes a record eligible for
cleanup; it does not promise deletion at that exact second. Disabling a type
leaves its existing markers intact until it is re-enabled.

Before staging a terminal record, the writer publishes a small fsynced control
marker with an opaque identity and deadline. The parent stores that identity.
Its normal lease protects a transition until commit or rollback. Cleanup takes
the exact parent lease and verifies both terminal state and marker identity.
An absent, rolled-back, nonterminal, or newer parent leaves only stale control
data to retire. An old marker cannot delete a later incarnation of an identity.

Receipt attachments and the parent are removed in the same lease transaction;
a failure rolls both back. Completed envelope cleanup also uses the existing
safe ordering for claim deadlines and stateful checkpoints. Failures preserve
the marker for retry, and durable markers survive process restarts.

Each maintenance turn attempts at most eight markers per record type, then
yields. The directory walk uses fixed memory and deadlines in filenames, and
opens only the selected marker body. Its cost scales with the number of marker
names; it does not parse every retained receipt, scan application documents,
use the query index, or maintain a cache. An advisory turn cursor prevents a
busy receipt from starving other eligible receipts. A marker read or validation
failure also advances that advisory turn: the damaged marker and its parent
remain untouched and the error remains observable, while other eligible
records can be reclaimed. A later turn retries the retained marker.

Dispatcher statistics expose `terminal_reclaims` and
`terminal_reclaim_failures`. The first counts safely retired markers, including
stale markers; it is not a count of application records deleted. Failures also
appear in `last_error`. Lease contention while an application is still
publishing a terminal outcome also increments the failure counter; the marker
is preserved and retried after the lease is released. A nonzero counter alone
does not mean cleanup is permanently stuck. Dead letters retain their separate
counters.

## Existing records and policy changes

Records created before terminal retention, or while that type is disabled,
have no retention marker and are not automatically backfilled. Pouch does not
scan an entire legacy namespace at startup to retrofit markers. These records
remain available for deliberate operator cleanup.

The writer records the deadline when it makes the terminal transition.
Changing a time window affects subsequent transitions, not existing marker
deadlines. Count caps apply to the marked backlog when maintenance observes
it. Turning a policy off suspends both time and count reclamation; re-enabling
it resumes existing durable work.
