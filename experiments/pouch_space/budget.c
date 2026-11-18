#include "lc_api_internal.h"
#include "space_internal.h"

static int invalid(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL, "pouch-space");
}
static int plan_add(ps_budget_plan *p, uint64_t payload, uint64_t metadata,
                    uint64_t entries, lc_error *e) {
  if (!p || payload > UINT64_MAX - p->payload_bytes ||
      metadata > UINT64_MAX - p->metadata_bytes ||
      entries > UINT64_MAX - p->entries)
    return invalid(e, "capacity plan totals overflow or plan is missing");
  p->payload_bytes += payload;
  p->metadata_bytes += metadata;
  p->entries += entries;
  return LC_OK;
}
int ps_budget_plan_stream(ps_budget_plan *p, lc_error *e) {
  return plan_add(p, 0, 80, 1, e);
}
int ps_budget_plan_catalog(ps_budget_plan *p, size_t bytes, lc_error *e) {
  if (bytes > PS_CATALOG_NAME_MAX)
    return invalid(e, "capacity plan catalog name exceeds format bounds");
  return plan_add(p, 0, 104 + bytes, 1, e);
}
int ps_budget_plan_routing(ps_budget_plan *p, int create, lc_error *e) {
  if (create != 0 && create != 1)
    return invalid(e, "capacity plan routing creation must be zero or one");
  return plan_add(p, PS_PAGE, create ? 88 : 0, create ? 1 : 0, e);
}
int ps_budget_plan_value(ps_budget_plan *p, uint64_t bytes, lc_error *e) {
  uint64_t pages, extents;
  if (bytes > (uint64_t)INT64_MAX)
    return invalid(e, "capacity plan immutable value exceeds local bounds");
  pages = bytes / PS_PAGE + (bytes % PS_PAGE != 0);
  extents = pages + (bytes % PS_PAGE != 0);
  if (extents > (PS_JOURNAL - 64) / 24)
    return invalid(e, "capacity plan value extents exceed journal capacity");
  return plan_add(p, pages * PS_PAGE, 64 + extents * 24, 1, e);
}
int ps_budget_plan_span(ps_budget_plan *p, uint64_t bytes, lc_error *e) {
  uint64_t pages;
  if (bytes > (uint64_t)INT64_MAX)
    return invalid(e, "capacity plan canonical span exceeds local bounds");
  pages = bytes / (PS_PAGE - PS_RUN_HEADER) +
          (bytes % (PS_PAGE - PS_RUN_HEADER) != 0);
  if (pages > UINT64_MAX / PS_PAGE ||
      pages > UINT64_MAX / PS_OWNED_DESCRIPTOR_BYTES)
    return invalid(e, "capacity plan canonical span overflows physical bounds");
  return plan_add(p, pages * PS_PAGE, pages * PS_OWNED_DESCRIPTOR_BYTES, pages,
                  e);
}
int ps_budget_plan_piece(ps_budget_plan *p, uint64_t logical, size_t ranges,
                         uint64_t live, lc_error *e) {
  ps_budget_plan next;
  uint64_t table;
  int rc;
  if (!p || !logical || logical > PS_CHUNK || ranges > logical ||
      live > logical || (!!ranges != !!live))
    return invalid(e, "piece capacity plan requires bounded live ranges");
  if (!ranges)
    return LC_OK;
  next = *p;
  table = (uint64_t)ranges * 8;
  if (table > (logical + 7) / 8)
    table = (logical + 7) / 8;
  rc = ps_budget_plan_value(&next, 64 + table + live, e);
  if (rc == LC_OK)
    rc = plan_add(&next, 0, 16, 0, e);
  if (rc == LC_OK)
    *p = next;
  return rc;
}
int ps_budget_plan_preferred_span(ps_budget_plan *p, uint64_t bytes,
                                  uint64_t preferred, lc_error *e) {
  ps_budget_plan next;
  uint64_t prior, last;
  int rc;
  if (!p || preferred < PS_PAGE || preferred > PS_MIB || preferred % PS_PAGE)
    return invalid(e, "capacity plan requires a page-aligned preferred grant");
  next = *p;
  rc = ps_budget_plan_span(&next, bytes, e);
  if (rc != LC_OK || !bytes)
    return rc;
  /* Every non-final grant is needed in full. Its physical size is at most
   * floor((bytes - 1 + header) / page) pages. Geometric growth is fourfold;
   * only the final grant can exceed the exact-span bound. */
  prior = (bytes - 1 + PS_RUN_HEADER) / PS_PAGE * PS_PAGE;
  last = prior >= PS_CHUNK / 4 ? PS_CHUNK : prior * 4;
  if (last < preferred)
    last = preferred;
  rc = plan_add(&next, last - PS_PAGE, 0, 0, e);
  if (rc == LC_OK)
    *p = next;
  return rc;
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
int ps_budget_resize(ps_budget *b, const ps_budget_plan *plan, lc_error *e) {
  ps_space *s;
  ps_budget_plan prior;
  ps_run *trim = NULL;
  uint64_t payload, metadata, entries;
  int rc, grows;
  if (!b || !plan || plan->payload_bytes % PS_PAGE || plan->maintenance < 0 ||
      plan->maintenance > 1)
    return invalid(e, "capacity resize requires an aligned matching plan");
  s = b->space;
  pthread_mutex_lock(&s->mutex);
  prior = b->remaining;
  rc = ps_internal_healthy(s, e);
  if (rc == LC_OK && plan->maintenance != prior.maintenance)
    rc = invalid(e, "capacity resize cannot change admission class");
  if (rc == LC_OK && b->users &&
      (plan->payload_bytes < prior.payload_bytes ||
       plan->metadata_bytes < prior.metadata_bytes ||
       plan->entries < prior.entries))
    rc = invalid(e, "close capacity writers before shrinking their claim");
  if (rc == LC_OK && plan->payload_bytes == prior.payload_bytes &&
      plan->metadata_bytes == prior.metadata_bytes &&
      plan->entries == prior.entries) {
    pthread_mutex_unlock(&s->mutex);
    return LC_OK;
  }
  if (rc == LC_OK) {
    trim = lc_calloc_with_allocator(NULL, 1, sizeof(*trim));
    if (!trim)
      rc = lc_error_set(e, LC_ERR_NOMEM, 0,
                        "cannot allocate capacity resize workspace", NULL, NULL,
                        "pouch-space");
  }
  grows = plan->payload_bytes > prior.payload_bytes ||
          plan->metadata_bytes > prior.metadata_bytes ||
          plan->entries > prior.entries;
  payload = s->reserved_payload - prior.payload_bytes;
  metadata = s->reserved_metadata - prior.metadata_bytes;
  entries = s->reserved_entries - prior.entries;
  if (rc == LC_OK && (plan->payload_bytes > UINT64_MAX - payload ||
                      plan->metadata_bytes > UINT64_MAX - metadata ||
                      plan->entries > UINT64_MAX - entries))
    rc = invalid(e, "capacity resize totals overflow");
  if (rc == LC_OK) {
    s->reserved_payload = payload + plan->payload_bytes;
    s->reserved_metadata = metadata + plan->metadata_bytes;
    s->reserved_entries = entries + plan->entries;
    b->remaining = *plan;
    if (grows)
      rc = ps_internal_budget_admit(s, plan->maintenance, e);
    if (rc != LC_OK) {
      s->reserved_payload = payload + prior.payload_bytes;
      s->reserved_metadata = metadata + prior.metadata_bytes;
      s->reserved_entries = entries + prior.entries;
      b->remaining = prior;
    }
    ps_internal_budget_trim(s, &trim);
  }
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, trim);
  return rc;
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
