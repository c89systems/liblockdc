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
int ps_budget_open(ps_space *, const ps_budget_plan *, ps_budget **,
                   lc_error *);
int ps_budget_status(ps_budget *, ps_budget_plan *, lc_error *);
/* Fails without consuming the handle while an immutable writer uses it.
 * Otherwise releases unspent credits, even after uncertain I/O. */
int ps_budget_close(ps_budget *, lc_error *);
#endif
