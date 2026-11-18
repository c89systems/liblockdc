#ifndef POUCH_SPACE_BUDGET_H
#define POUCH_SPACE_BUDGET_H
#include "space.h"

/* Volatile capacity claims, never a second journal or an ownership record.
 * Plan the sum of physical page allocations and positive descriptor growth
 * across the complete operation. Replacements only charge positive growth.
 * Payload bytes must be page aligned. maintenance permits spending the fixed
 * payload floor; ordinary claims preserve it. No capacity-sized allocation.
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
int ps_budget_plan_piece(ps_budget_plan *, uint64_t logical_bytes,
                         size_t ranges, uint64_t live_bytes, lc_error *);
int ps_budget_plan_span(ps_budget_plan *, uint64_t bytes, lc_error *);
/* Preferred geometric reservation may over-allocate its last grant. Account
 * for that payload without inventing extra descriptor entries for the spare. */
int ps_budget_plan_preferred_span(ps_budget_plan *, uint64_t bytes,
                                  uint64_t preferred_grant, lc_error *);
int ps_budget_open(ps_space *, const ps_budget_plan *, ps_budget **,
                   lc_error *);
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
