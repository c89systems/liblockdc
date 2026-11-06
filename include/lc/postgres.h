#ifndef LC_POSTGRES_H
#define LC_POSTGRES_H

/** @file lc/postgres.h
 * Optional explicitly linked PostgreSQL XA provider. Include this header only
 * when building with the PostgreSQL provider SDK and cpktdb facade available.
 * The core public API in lc/lc.h has no PostgreSQL client dependency.
 */

#include <lc/lc.h>

#ifdef __cplusplus
extern "C" {
#endif
#include <cpkt/postgres.h>
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(default)
#endif

/** Optional PostgreSQL participant, explicitly linked and registered. One
 * owning thread/process drives native connections; one serialized journal
 * worker performs Pouch durability. No SQL is saved or replayed. */
typedef struct lc_postgres lc_postgres;
/** Owned enrolled PostgreSQL branch; close its active operation before branch
 * close.
 */
typedef struct lc_postgres_branch lc_postgres_branch;
/** Owned PostgreSQL operation; drive readiness and close after terminal
 * observation.
 */
typedef struct lc_postgres_operation lc_postgres_operation;
/** Owned validity token for the branch's exact native connection; release
 * even after invalidation.
 */
typedef struct lc_sql_borrow lc_sql_borrow;

/** Copied at open. Use an explicit Unix socket host, numeric host or hostaddr;
 * resolve names outside the event loop. Resource ID is stable across recovery.
 * Store is borrowed until close_blocking returns. Registration checks
 * PostgreSQL 14+ native 2PC, writable primary, system/database/role identity
 * and capacity. The role needs EXECUTE on pg_control_system(), in addition to
 * ordinary access. No DDL or server configuration is performed. */
typedef struct lc_postgres_config {
  /** Stable resource XID, reused unchanged across restarts and recovery. */
  const char *resource_id;
  /** Copied libpq-style connection settings; credentials never enter the
   * durable journal.
   */
  const char *connection_info;
} lc_postgres_config;

/** Explicit core resource registration: pass this table and lc_postgres_config
 * to lc_xa_resource_open(). Static/shared applications use the same contract.
 * The base library and Lua binding require no PostgreSQL SDK or native client.
 */
const lc_xa_driver *lc_postgres_driver(void);

/** Native outcome, distinct from coordinator decision and completion evidence.
 */
enum {
  /** Native outcome is unavailable or ambiguous; preserve recovery intent. */
  LC_POSTGRES_OUTCOME_UNKNOWN = 0,
  /** Native transaction status proves commit. */
  LC_POSTGRES_OUTCOME_COMMITTED = 1,
  /** Native transaction status proves abort. */
  LC_POSTGRES_OUTCOME_ABORTED = 2,
  /** Native transaction remains active or prepared. */
  LC_POSTGRES_OUTCOME_IN_PROGRESS = 3,
  /** Native status history has expired; this observation alone proves no
   * terminal outcome. */
  LC_POSTGRES_OUTCOME_DISCARDED = 4
};

/** One socket or journal-wake wait. events uses READ=1, WRITE=2. Deadline is
 * absolute CLOCK_MONOTONIC milliseconds; zero at operation start selects 30s,
 * -1 selects infinity. advance never polls/sleeps or performs journal fsync.
 * On LC_ERR_PENDING, inspect lc_postgres_operation_result(): NULL needs
 * readiness/advance; a non-NULL result is terminal and requires a later
 * recovery attempt. Never wait on a terminal descriptor. Errors preserve
 * durable intent and exclude raw provider diagnostics. */
typedef struct lc_postgres_wait {
  /** Borrowed readiness descriptor; do not close it. */
  int fd;
  /** READ=1 and WRITE=2 readiness mask; do not wait when the operation is
   * terminal.
   */
  unsigned int events;
  /** Absolute CLOCK_MONOTONIC milliseconds; -1 denotes an infinite deadline. */
  int64_t deadline_ms;
} lc_postgres_wait;

/** Bounded diagnostic prepared-transaction identity; membership never
 * authorizes resolution.
 */
typedef struct lc_postgres_inventory_row {
  /** Exact native prepared-transaction identifier, NUL-terminated. */
  char gid[200];
  /** Exact native 32-bit XID as decimal text, distinct from the epoch-
   * qualified enrollment ID.
   */
  char xid[11];
  /** Exact database OID as decimal text, NUL-terminated. */
  char database_oid[11];
  /** Exact role OID as decimal text, NUL-terminated. */
  char role_oid[11];
} lc_postgres_inventory_row;

/** Terminal snapshot borrowed until operation close. Successful branch start/
 * recovery yields one owned branch; prepare/resolve only echo an existing
 * branch, transferring no additional ownership. Native branches require their
 * operation to close first and cannot outlive resource close. Inventory is
 * diagnostic only; prefix membership never authorizes destructive resolution.
 */
typedef struct lc_postgres_result {
  lc_postgres_branch *branch; /**< Owned only for successful start/recovery. */
  /** Copied durable branch identity/progress snapshot. */
  lc_xa_branch_record record;
  /** LC_POSTGRES_OUTCOME_* observation, distinct from the coordinator
   * decision.
   */
  int native_outcome;
  /** Number of populated inventory rows, at most 64. */
  size_t row_count;
  /** Bounded inventory page; only row_count entries are populated. */
  lc_postgres_inventory_row rows[64];
  /** Five-character SQLSTATE plus NUL, or empty when unavailable; never raw
   * SQL diagnostics.
   */
  char sqlstate[6];
} lc_postgres_result;

/** Copies configuration and starts its durability worker without network I/O.
 * The journal is borrowed: keep it alive until resource close returns. */
int lc_postgres_open(lc_xa_store *store, const lc_postgres_config *config,
                     lc_postgres **out, lc_error *error);
/** Starts capability/backend validation and durable identity registration.
 * Failed re-registration disables new branches without replacing identity. */
int lc_postgres_register_start(lc_postgres *resource, int64_t deadline_ms,
                               lc_postgres_operation **out, lc_error *error);
/** Starts a native transaction and durably enrolls its exact XID under the
 * supplied valid global ID. Enroll every member before preparing any branch. */
int lc_postgres_branch_start(lc_postgres *resource, const char *txn_id,
                             int64_t deadline_ms, lc_postgres_operation **out,
                             lc_error *error);
/** Release all borrows/results and drain application protocol first. Unexpected
 * transaction, COPY or pipeline state fails the branch. Prepare intent is
 * durable before PREPARE is sent; retrieval failure never produces a YES vote.
 */
int lc_postgres_prepare_start(lc_postgres_branch *branch, int64_t deadline_ms,
                              lc_postgres_operation **out, lc_error *error);
/** commit is 0 or 1. Existing .txns decision must agree. Recovery verifies
 * exact backend and journal identity, then resolves/reconciles without
 * replaying SQL. */
int lc_postgres_resolve_start(lc_postgres_branch *branch, int commit,
                              int64_t deadline_ms, lc_postgres_operation **out,
                              lc_error *error);
/** Verifies the durable decision and exact enrolled backend/branch identity,
 * then resolves/reconciles without SQL replay. Success returns an owned branch.
 */
int lc_postgres_recover_start(lc_postgres *resource, const char *txn_id,
                              const char *branch_id, int commit,
                              int64_t deadline_ms, lc_postgres_operation **out,
                              lc_error *error);
/** Observe the exact enrolled native XID after backend verification. This does
 * not choose a decision, persist intent, resolve work or authorize publication.
 */
int lc_postgres_status_start(lc_postgres *resource, const char *txn_id,
                             const char *branch_id, int64_t deadline_ms,
                             lc_postgres_operation **out, lc_error *error);
/** Bounded, GID-ordered inventory scoped to resource role/database and its
 * stable resource prefix. after_gid is NULL for the first page; last GID
 * continues it. Zero limit selects 64; valid positive limits are 1 through 64.
 */
int lc_postgres_inventory_start(lc_postgres *resource, const char *after_gid,
                                size_t limit, int64_t deadline_ms,
                                lc_postgres_operation **out, lc_error *error);
/** Advances bounded nonblocking work. LC_ERR_PENDING is nonterminal only while
 * lc_postgres_operation_result() is NULL; otherwise close and retry recovery
 * later. */
int lc_postgres_operation_advance(lc_postgres_operation *operation,
                                  lc_error *error);
/** Copies current readiness after nonterminal LC_ERR_PENDING. Honor the
 * absolute monotonic deadline; an already due deadline requires advancement. */
int lc_postgres_operation_wait(lc_postgres_operation *operation,
                               lc_postgres_wait *out, lc_error *error);
/** NULL until terminal (or for a foreign owner). The result is borrowed until
 * operation close; observe the advance status before using an owned branch. */
const lc_postgres_result *
lc_postgres_operation_result(const lc_postgres_operation *operation);
/** Stops owned native work and preserves possibly prepared work for recovery.
 * LC_ERR_PENDING means journal cleanup is still in flight; drive the operation
 * or use close_blocking() to rendezvous with that worker. */
int lc_postgres_operation_cancel(lc_postgres_operation *operation,
                                 lc_error *error);
/** Consumes the operation, waiting only for in-flight durability cleanup.
 * Successful start/recovery branches remain owned until branch/resource close.
 */
void lc_postgres_operation_close_blocking(lc_postgres_operation *operation);
/** Consumes a branch after its active operation is closed. Earlier closure
 * returns LC_ERR_INVALID and leaves the branch intact. Prepared work remains
 * durable for recovery; retained borrow tokens become invalid but releasable.
 */
int lc_postgres_branch_close(lc_postgres_branch *branch, lc_error *error);
/** Consumes the native resource and any remaining operations/branches. Their
 * pointers must not be reused; close the borrowed journal only after return.
 * May wait for journal fsync, never native network responses. */
int lc_postgres_close_blocking(lc_postgres *resource, lc_error *error);

/** Scoped exact-connection borrow. Tokens are retained beyond connection close;
 * stale, fork and thread checks precede native pointer access. Only one
 * borrower may own the protocol. Connection-associated results and copies that
 * preserve their events pin validity tokens and must be freed before prepare.
 * Releasing while protocol/results remain
 * invalidates the branch, closes owned work and leaves durable cleanup for
 * recovery. Returned cpkt pointer transfers no ownership; never free/reset it.
 */
int lc_postgres_branch_borrow(lc_postgres_branch *branch, lc_sql_borrow **out,
                              lc_error *error);
/** Generic public adapter scope backed by the same retained validity token.
 * The view's native_connection is the exact cpkt connection used by PREPARE. */
int lc_postgres_branch_borrow_generic(lc_postgres_branch *branch,
                                      lc_xa_borrow **out, lc_error *error);
/** Validates the live process/thread scope before returning a borrowed native
 * connection. Revalidate before every use; never free/reset the pointer. */
int lc_sql_borrow_postgres(lc_sql_borrow *borrow,
                           cpkt_postgres_connection **out, lc_error *error);
/** Consumes the owning-thread token, including when final protocol cleanup
 * fails; do not retry a consumed token. Foreign-owner rejection releases none.
 */
int lc_sql_borrow_release(lc_sql_borrow *borrow, lc_error *error);
/** Snapshot identity; pointer remains borrowed until branch_close. */
const lc_xa_branch_record *
lc_postgres_branch_record(const lc_postgres_branch *branch);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif
#ifdef __cplusplus
}
#endif
#endif
