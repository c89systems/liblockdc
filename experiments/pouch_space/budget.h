#ifndef POUCH_SPACE_BUDGET_H
#define POUCH_SPACE_BUDGET_H
#include "space.h"

/* Volatile capacity claims, never a second journal or an ownership record.
 * Plan the sum of physical page allocations and positive descriptor growth
 * across the complete operation. Replacements only charge positive growth.
 * Payload bytes must be page aligned. maintenance permits spending the
 * maintenance floor; ordinary claims preserve it. No capacity-sized allocation.
 *
 * Open is atomic admission: peers and checkpoints cannot spend the claim.
 * Explicitly pass it to each participating allocation. NULL uses ordinary
 * admission. Credits are spent on physical allocation / descriptor publication,
 * respectively; a failed or abandoned attempt does not refund spent credits.
 * This guarantees capacity, not memory allocation or I/O success, and does not
 * roll back publications or isolate keys; callers retain logical mutation
 * authority. Producers need no allocator lock while holding it. The admission
 * floor is not charged again when spending already funded credits.
 * A handle may serve independent writers; stop concurrent calls before close.
 */
typedef struct ps_budget_plan {
  uint64_t payload_bytes, metadata_bytes, entries;
  int maintenance;
} ps_budget_plan;
typedef struct ps_reserve ps_reserve;
/* Unclaimed future maintenance requirements. The space protects the component
 * maxima of the base plan and live owners, in addition to all admitted budgets
 * and their checkpoint workspace. Owners do not reserve one copy per key.
 * Growth uses ordinary admission, before exposing the larger logical state.
 * recover==1 only reconstructs existing canonical obligations before root
 * admission; ordinary allocation still preserves the complete recovered plan.
 * Shrink/close requires the owner to have durably retired every state needing
 * the old requirement. Updates and removal are O(log owners), without I/O or
 * allocation except the usual physical checkpoint-workspace protection.
 * These plans are not spendable: maintenance first acquires a normal explicit
 * maintenance budget. An already funded budget never pays the floor again. */
int ps_reserve_base(ps_space *, const ps_budget_plan *, int recover,
                    lc_error *);
int ps_reserve_open(ps_space *, const ps_budget_plan *, int recover,
                    ps_reserve **, lc_error *);
int ps_reserve_resize(ps_reserve *, const ps_budget_plan *, int recover,
                      lc_error *);
void ps_reserve_close(ps_reserve *);
/* Successful composition uses scalar arithmetic without allocation or I/O.
 * Failure leaves the plan unchanged and uses normal error reporting.
 * These add one new head/binding, a routing replacement
 * (or creation), or an exact canonical span with no existing grants. The span
 * bound permits every grant to fragment down to one page, including its
 * identity header. Geometric over-reservation needs a separate allowance. */
int ps_budget_plan_stream(ps_budget_plan *, lc_error *);
int ps_budget_plan_catalog(ps_budget_plan *, size_t name_bytes, lc_error *);
int ps_budget_plan_routing(ps_budget_plan *, int create, lc_error *);
/* One immutable value, including an empty value. Funds a new descriptor and
 * the worst case of one extent per payload page and one extra extent for a
 * split subpage tail. Also usable for replacement;
 * positive growth alone is spent and unused credits return on close. Rejects
 * values whose worst-case extent list cannot fit one publication event. */
int ps_budget_plan_value(ps_budget_plan *, uint64_t bytes, lc_error *);
/* Any encoded length up to this bound. Even an aligned limit can require one
 * extra extent when a shorter value splits an existing packed tail. */
int ps_budget_plan_value_bound(ps_budget_plan *, uint64_t bytes, lc_error *);
int ps_budget_plan_piece(ps_budget_plan *, uint64_t logical_bytes,
                         size_t ranges, uint64_t live_bytes, lc_error *);
/* Compound namespace workspace: one bounded piece, optional successor and
 * routing, one maintenance hint, and (when nonzero) a canonical deletion with
 * its bounded frame, changed-key checkpoint and coverage control. */
int ps_budget_plan_maintenance(ps_budget_plan *, uint64_t deletion_bytes,
                               lc_error *);
int ps_budget_plan_span(ps_budget_plan *, uint64_t bytes, lc_error *);
/* Preferred geometric reservation may over-allocate its last grant. Account
 * for that payload without inventing extra descriptor entries for the spare. */
int ps_budget_plan_preferred_span(ps_budget_plan *, uint64_t bytes,
                                  uint64_t preferred_grant, lc_error *);
int ps_budget_open(ps_space *, const ps_budget_plan *, ps_budget **,
                   lc_error *);
/* Check the ordinary floor after increasing a mandatory completion claim,
 * before accepting ordinary producer bytes. Recovery and mandatory work may
 * restore/spend completion credits while the standing floor is in use. */
int ps_budget_foreground_check(ps_space *, lc_error *);
int ps_budget_status(ps_budget *, ps_budget_plan *, lc_error *);
/* Replace the unspent claim atomically, preserving the handle's admission
 * class. Growth is permitted while writers use the budget; shrinking requires
 * all such writers to close. Reusing an unchanged healthy claim allocates
 * nothing; shrinking needs no new capacity, including while a maintenance
 * peer holds the remaining reserve. Failure retains the previous claim. Spent
 * space remains ordinary owned storage and is never refunded by resizing. */
int ps_budget_resize(ps_budget *, const ps_budget_plan *, lc_error *);
/* Fails without consuming the handle while a writer or grant preparation uses
 * it. Otherwise releases unspent credits, even after uncertain I/O. */
int ps_budget_close(ps_budget *, lc_error *);
#endif
