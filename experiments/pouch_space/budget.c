#include "lc_api_internal.h"
#include "space_internal.h"

static int invalid(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL, "pouch-space");
}
int ps_internal_budget_check(ps_space *s, ps_budget *b, uint64_t payload,
                             uint64_t metadata, uint64_t entries, lc_error *e) {
  if (b && (b->space != s || payload > b->remaining.payload_bytes ||
            metadata > b->remaining.metadata_bytes ||
            entries > b->remaining.entries))
    return invalid(e, "container operation exceeds its capacity budget");
  return LC_OK;
}
void ps_internal_budget_spend(ps_budget *b, uint64_t payload, uint64_t metadata,
                              uint64_t entries) {
  if (!b)
    return;
  b->remaining.payload_bytes -= payload;
  b->remaining.metadata_bytes -= metadata;
  b->remaining.entries -= entries;
  b->space->reserved_payload -= payload;
  b->space->reserved_metadata -= metadata;
  b->space->reserved_entries -= entries;
}
int ps_budget_open(ps_space *s, const ps_budget_plan *plan, ps_budget **out,
                   lc_error *e) {
  ps_budget *b;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !plan || !out || plan->payload_bytes % PS_PAGE ||
      plan->maintenance < 0 || plan->maintenance > 1)
    return invalid(e,
                   "capacity budget requires a page-aligned plan and output");
  b = lc_calloc_with_allocator(NULL, 1, sizeof(*b));
  if (b)
    b->trim = lc_calloc_with_allocator(NULL, 1, sizeof(*b->trim));
  if (!b || !b->trim) {
    lc_free_with_allocator(NULL, b);
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate capacity budget",
                        NULL, NULL, "pouch-space");
  }
  b->space = s;
  b->remaining = *plan;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  if (rc == LC_OK &&
      (plan->payload_bytes > UINT64_MAX - s->reserved_payload ||
       plan->metadata_bytes > UINT64_MAX - s->reserved_metadata ||
       plan->entries > UINT64_MAX - s->reserved_entries))
    rc = invalid(e, "capacity budget totals overflow");
  if (rc == LC_OK) {
    s->reserved_payload += plan->payload_bytes;
    s->reserved_metadata += plan->metadata_bytes;
    s->reserved_entries += plan->entries;
    rc = ps_internal_budget_admit(s, plan->maintenance, e);
    if (rc != LC_OK) {
      ps_internal_budget_spend(b, plan->payload_bytes, plan->metadata_bytes,
                               plan->entries);
      ps_internal_budget_trim(s, &b->trim);
    } else
      ++s->budgets;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    *out = b;
  else {
    lc_free_with_allocator(NULL, b->trim);
    lc_free_with_allocator(NULL, b);
  }
  return rc;
}
int ps_budget_status(ps_budget *b, ps_budget_plan *out, lc_error *e) {
  if (!b || !out)
    return invalid(e, "capacity budget status requires handle and output");
  pthread_mutex_lock(&b->space->mutex);
  *out = b->remaining;
  pthread_mutex_unlock(&b->space->mutex);
  return LC_OK;
}
int ps_budget_close(ps_budget *b, lc_error *e) {
  ps_space *s;
  if (!b)
    return LC_OK;
  s = b->space;
  pthread_mutex_lock(&s->mutex);
  if (b->users) {
    pthread_mutex_unlock(&s->mutex);
    return invalid(e, "close writers using the capacity budget first");
  }
  ps_internal_budget_spend(b, b->remaining.payload_bytes,
                           b->remaining.metadata_bytes, b->remaining.entries);
  --s->budgets;
  ps_internal_budget_trim(s, &b->trim);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, b->trim);
  lc_free_with_allocator(NULL, b);
  return LC_OK;
}
