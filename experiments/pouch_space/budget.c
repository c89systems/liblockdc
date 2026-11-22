#include "frame.h"
#include "lc_api_internal.h"
#include "space_internal.h"

static int invalid(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL, "pouch-space");
}
static void plan_max(ps_budget_plan *p, const ps_budget_plan *q) {
  if (q->payload_bytes > p->payload_bytes)
    p->payload_bytes = q->payload_bytes;
  if (q->metadata_bytes > p->metadata_bytes)
    p->metadata_bytes = q->metadata_bytes;
  if (q->entries > p->entries)
    p->entries = q->entries;
}
static unsigned int reserve_height(ps_reserve *r) { return r ? r->height : 0; }
static void reserve_update(ps_reserve *r) {
  unsigned int a = reserve_height(r->left), b = reserve_height(r->right);
  r->height = 1 + (a > b ? a : b);
  r->maximum = r->plan;
  if (r->left)
    plan_max(&r->maximum, &r->left->maximum);
  if (r->right)
    plan_max(&r->maximum, &r->right->maximum);
}
static ps_reserve *reserve_left(ps_reserve *r) {
  ps_reserve *n = r->right;
  r->right = n->left;
  n->left = r;
  reserve_update(r);
  reserve_update(n);
  return n;
}
static ps_reserve *reserve_right(ps_reserve *r) {
  ps_reserve *n = r->left;
  r->left = n->right;
  n->right = r;
  reserve_update(r);
  reserve_update(n);
  return n;
}
static ps_reserve *reserve_balance(ps_reserve *r) {
  if (!r)
    return NULL;
  reserve_update(r);
  if (reserve_height(r->left) > reserve_height(r->right) + 1) {
    if (reserve_height(r->left->right) > reserve_height(r->left->left))
      r->left = reserve_left(r->left);
    return reserve_right(r);
  }
  if (reserve_height(r->right) > reserve_height(r->left) + 1) {
    if (reserve_height(r->right->left) > reserve_height(r->right->right))
      r->right = reserve_right(r->right);
    return reserve_left(r);
  }
  return r;
}
static ps_reserve *reserve_insert(ps_reserve *root, ps_reserve *r) {
  if (!root) {
    reserve_update(r);
    return r;
  }
  if (r->id < root->id)
    root->left = reserve_insert(root->left, r);
  else
    root->right = reserve_insert(root->right, r);
  return reserve_balance(root);
}
static ps_reserve *reserve_first(ps_reserve *r, ps_reserve **first) {
  if (!r->left) {
    *first = r;
    return r->right;
  }
  r->left = reserve_first(r->left, first);
  return reserve_balance(r);
}
static ps_reserve *reserve_remove(ps_reserve *root, ps_reserve *r) {
  ps_reserve *next;
  if (root == r) {
    if (!root->left)
      return root->right;
    if (!root->right)
      return root->left;
    root->right = reserve_first(root->right, &next);
    next->left = root->left;
    next->right = root->right;
    return reserve_balance(next);
  }
  if (r->id < root->id)
    root->left = reserve_remove(root->left, r);
  else
    root->right = reserve_remove(root->right, r);
  return reserve_balance(root);
}
static ps_reserve *reserve_change(ps_reserve *root, ps_reserve *r) {
  if (root != r) {
    if (r->id < root->id)
      root->left = reserve_change(root->left, r);
    else
      root->right = reserve_change(root->right, r);
  }
  reserve_update(root);
  return root;
}
static void selected_reserve_remove(ps_space *s, ps_reserve *r) {
  if (r->retiring_link) {
    *r->retiring_link = r->retiring_next;
    if (r->retiring_next)
      r->retiring_next->retiring_link = r->retiring_link;
    r->retiring_link = NULL;
    r->retiring_next = NULL;
  }
  s->selected_reserves = reserve_remove(s->selected_reserves, r);
  r->height = 0;
  --s->selected_reserve_count;
}
static void selected_reserves_prune(ps_space *s) {
  ps_reserve *r, *next;
  uint64_t durable;
  if (!s->retiring_reserves)
    return;
  durable = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  if (s->reserve_pruned_local == durable &&
      s->reserve_pruned_peer == s->peer_durable)
    return;
  s->reserve_pruned_local = durable;
  s->reserve_pruned_peer = s->peer_durable;
  for (r = s->retiring_reserves; r; r = next) {
    next = r->retiring_next;
    if (r->retire_local <= durable && r->retire_peer <= s->peer_durable)
      selected_reserve_remove(s, r);
  }
}
void ps_internal_reserve_plan(ps_space *s, ps_budget_plan *out) {
  selected_reserves_prune(s);
  *out = s->reserve_base;
  if (s->reserves)
    plan_max(out, &s->reserves->maximum);
  if (s->selected_reserves)
    plan_max(out, &s->selected_reserves->maximum);
}
/* Physical descriptors own these nodes. Decode and preparation allocate them
 * before an entry can become live; installation updates the max without I/O. */
int ps_internal_reserve_entry(ps_entry *p, uint64_t bytes, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_reserve *r, *prior = p->deletion_reserve;
  int rc;
  if (bytes == p->deletion_bytes)
    return LC_OK;
  if (!bytes) {
    lc_free_with_allocator(NULL, prior);
    p->deletion_reserve = NULL;
    p->deletion_bytes = 0;
    return LC_OK;
  }
  rc = ps_budget_plan_maintenance(&plan, bytes, e);
  if (rc != LC_OK)
    return rc;
  r = prior ? prior : lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate key slot reserve",
                        NULL, NULL, "pouch-space");
  r->id = p->token;
  r->plan = plan;
  p->deletion_reserve = r;
  ps_internal_entry_auxiliary(p);
  p->deletion_bytes = bytes;
  return LC_OK;
}
void ps_internal_reserve_install(ps_space *s, ps_entry *p, ps_entry *previous) {
  ps_reserve *old = previous ? previous->deletion_reserve : NULL;
  if (old && old->height) {
    old->retire_local = p->publication_sequence;
    old->retire_peer = p->peer_sequence;
    if (old->retire_local <=
            __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) &&
        old->retire_peer <= s->peer_durable)
      selected_reserve_remove(s, old);
    else {
      old->retiring_next = s->retiring_reserves;
      old->retiring_link = &s->retiring_reserves;
      if (old->retiring_next)
        old->retiring_next->retiring_link = &old->retiring_next;
      s->retiring_reserves = old;
    }
  }
  if (p->deletion_reserve) {
    s->selected_reserves =
        reserve_insert(s->selected_reserves, p->deletion_reserve);
    ++s->selected_reserve_count;
  }
}
void ps_internal_reserve_forget(ps_space *s, ps_entry *p) {
  if (p->deletion_reserve && p->deletion_reserve->height)
    selected_reserve_remove(s, p->deletion_reserve);
}
int ps_internal_reserve_recheck(ps_space *s, uint64_t deletion, lc_error *e) {
  ps_budget_plan base = s->reserve_base, current = base, proposed = {0};
  int rc;
  selected_reserves_prune(s);
  if (s->selected_reserves)
    plan_max(&current, &s->selected_reserves->maximum);
  rc = ps_budget_plan_maintenance(&proposed, deletion, e);
  if (rc != LC_OK || (proposed.payload_bytes <= current.payload_bytes &&
                      proposed.metadata_bytes <= current.metadata_bytes &&
                      proposed.entries <= current.entries))
    return rc;
  plan_max(&s->reserve_base, &proposed);
  rc = ps_internal_budget_admit(s, 0, NULL, NULL, e);
  s->reserve_base = base;
  return rc;
}
static int reserve_valid(const ps_budget_plan *p, int recover, lc_error *e) {
  if (!p || p->payload_bytes % PS_PAGE || p->maintenance ||
      p->payload_bytes > (uint64_t)INT64_MAX ||
      p->metadata_bytes > (uint64_t)INT64_MAX / 2 ||
      p->entries > (uint64_t)INT64_MAX / PS_PAGE || recover < 0 || recover > 1)
    return invalid(e, "maintenance reserve requires a bounded aligned plan");
  return LC_OK;
}
int ps_reserve_base(ps_space *s, const ps_budget_plan *p, int recover,
                    lc_error *e) {
  ps_budget_plan prior;
  ps_coordination_guard *guard = NULL;
  int admit, rc = reserve_valid(p, recover, e);
  if (!s || rc != LC_OK)
    return rc == LC_OK ? invalid(e, "maintenance reserve requires space") : rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  prior = s->reserve_base;
  admit = !recover && (p->payload_bytes > prior.payload_bytes ||
                       p->metadata_bytes > prior.metadata_bytes ||
                       p->entries > prior.entries);
  if (rc == LC_OK && admit)
    rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK)
    s->reserve_base = *p;
  if (rc == LC_OK && admit)
    rc = ps_internal_budget_admit(s, 0, NULL, NULL, e);
  rc = ps_internal_capture_end(s, guard, rc, e);
  if (rc != LC_OK)
    s->reserve_base = prior;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_reserve_open(ps_space *s, const ps_budget_plan *p, int recover,
                    ps_reserve **out, lc_error *e) {
  ps_reserve *r;
  ps_coordination_guard *guard = NULL;
  int rc;
  if (out)
    *out = NULL;
  rc = reserve_valid(p, recover, e);
  if (!s || !out || rc != LC_OK)
    return rc == LC_OK ? invalid(e, "maintenance reserve requires output") : rc;
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return lc_error_set(e, LC_ERR_NOMEM, 0,
                        "cannot allocate maintenance reserve owner", NULL, NULL,
                        "pouch-space");
  r->space = s;
  r->plan = *p;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  if (rc == LC_OK && s->reserve_next == UINT64_MAX)
    rc = invalid(e, "maintenance reserve owner identity exhausted");
  if (rc == LC_OK && !recover)
    rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK) {
    r->id = ++s->reserve_next;
    s->reserves = reserve_insert(s->reserves, r);
    if (!recover)
      rc = ps_internal_budget_admit(s, 0, NULL, NULL, e);
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  if (rc == LC_OK)
    ++s->reserve_count;
  else if (r->id)
    s->reserves = reserve_remove(s->reserves, r);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    *out = r;
  else
    lc_free_with_allocator(NULL, r);
  return rc;
}
int ps_reserve_resize(ps_reserve *r, const ps_budget_plan *p, int recover,
                      lc_error *e) {
  ps_budget_plan prior;
  ps_space *s;
  ps_coordination_guard *guard = NULL;
  int admit, rc = reserve_valid(p, recover, e);
  if (!r || rc != LC_OK)
    return rc == LC_OK ? invalid(e, "maintenance reserve requires owner") : rc;
  s = r->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  prior = r->plan;
  admit = !recover && (p->payload_bytes > prior.payload_bytes ||
                       p->metadata_bytes > prior.metadata_bytes ||
                       p->entries > prior.entries);
  if (rc == LC_OK && admit)
    rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK) {
    r->plan = *p;
    s->reserves = reserve_change(s->reserves, r);
    if (admit)
      rc = ps_internal_budget_admit(s, 0, NULL, NULL, e);
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  if (rc != LC_OK) {
    r->plan = prior;
    s->reserves = reserve_change(s->reserves, r);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
void ps_reserve_close(ps_reserve *r) {
  ps_space *s;
  if (!r)
    return;
  s = r->space;
  pthread_mutex_lock(&s->mutex);
  s->reserves = reserve_remove(s->reserves, r);
  --s->reserve_count;
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, r);
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
int ps_budget_plan_add(ps_budget_plan *p, const ps_budget_plan *extra,
                       lc_error *e) {
  if (!extra)
    return invalid(e, "capacity composition requires additional costs");
  return plan_add(p, extra->payload_bytes, extra->metadata_bytes,
                  extra->entries, e);
}
int ps_budget_plan_catalog(ps_budget_plan *p, size_t bytes, lc_error *e) {
  if (bytes > PS_CATALOG_NAME_MAX)
    return invalid(e, "capacity plan catalog name exceeds format bounds");
  return plan_add(p, 0, 104 + bytes, 1, e);
}
int ps_budget_plan_routing(ps_budget_plan *p, int create, lc_error *e) {
  if (create != 0 && create != 1)
    return invalid(e, "capacity plan routing creation must be zero or one");
  /* The short routing value may split across two packed tails. A valid old
   * head has at least one extent, so replacement can still grow by one. */
  return plan_add(p, PS_PAGE, create ? 112 : 24, create ? 1 : 0, e);
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
int ps_budget_plan_value_bound(ps_budget_plan *p, uint64_t bytes, lc_error *e) {
  ps_budget_plan next;
  int rc;
  if (!p)
    return invalid(e, "capacity value bound requires a plan");
  next = *p;
  rc = ps_budget_plan_value(&next, bytes, e);
  if (rc == LC_OK && bytes && !(bytes % PS_PAGE) &&
      bytes / PS_PAGE >= (PS_JOURNAL - 64) / 24)
    rc = invalid(e, "capacity value bound extents exceed journal capacity");
  if (rc == LC_OK && bytes && !(bytes % PS_PAGE))
    rc = plan_add(&next, 0, 24, 0, e);
  if (rc == LC_OK)
    *p = next;
  return rc;
}
int ps_budget_plan_key_slot(ps_budget_plan *p, unsigned int kind,
                            uint64_t bytes, int upper_bound, lc_error *e) {
  ps_budget_plan next;
  int rc;
  if (!p)
    return invalid(e, "capacity key slot requires a plan");
  next = *p;
  rc = upper_bound ? ps_budget_plan_value_bound(&next, bytes, e)
                   : ps_budget_plan_value(&next, bytes, e);
  if (rc == LC_OK && kind == PS_KEY_METADATA)
    rc = plan_add(&next, 0, PS_KEY_SLOT_DESCRIPTOR_EXTRA, 0, e);
  if (rc == LC_OK)
    *p = next;
  return rc;
}
int ps_budget_plan_piece(ps_budget_plan *p, uint64_t logical, size_t ranges,
                         uint64_t live, lc_error *e) {
  ps_budget_plan next;
  uint64_t table;
  int rc;
  if (!p || !logical || logical > PS_STREAM_PIECE_BYTES || ranges > logical ||
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
  last = prior >= PS_STREAM_PIECE_BYTES / 4 ? PS_STREAM_PIECE_BYTES : prior * 4;
  if (last < preferred)
    last = preferred;
  rc = plan_add(&next, last - PS_PAGE, 0, 0, e);
  if (rc == LC_OK)
    *p = next;
  return rc;
}
int ps_budget_plan_maintenance(ps_budget_plan *p, uint64_t deletion,
                               lc_error *e) {
  ps_budget_plan next;
  int rc;
  if (!p || deletion > (uint64_t)INT64_MAX - 512)
    return invalid(e, "maintenance plan deletion exceeds canonical bounds");
  next = *p;
  rc =
      ps_budget_plan_piece(&next, PS_STREAM_PIECE_BYTES,
                           PS_STREAM_PIECE_BYTES / 2, PS_STREAM_PIECE_BYTES, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_stream(&next, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_catalog(&next, 28, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_routing(&next, 1, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&next, 32, e);
  if (rc == LC_OK && deletion)
    rc = ps_budget_plan_span(&next, deletion + PS_FRAME_BYTES, e);
  if (rc == LC_OK && deletion)
    rc = ps_budget_plan_key_slot(&next, PS_KEY_METADATA, deletion + 512, 1, e);
  /* Empty selected heads also need coverage before their obsolete catalog
   * bindings can retire. Cover the predecessor, then the empty successor at
   * retirement. Neither publication needs a logical tombstone. */
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&next, 128, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&next, 128, e);
  /* A future retention reference owns one raw key, not its escaped filename
   * spelling. Claim facts and finish use bounded progress publications. The
   * deletion bound includes the key and therefore conservatively bounds this
   * identity.
   */
  if (rc == LC_OK && deletion)
    rc = ps_budget_plan_value_bound(&next, deletion + 80, e);
  if (rc == LC_OK && deletion)
    rc = ps_budget_plan_value(&next, 80, e);
  if (rc == LC_OK && deletion)
    rc = ps_budget_plan_value(&next, 80, e);
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
static int budget_open(ps_space *s, const ps_budget_plan *plan, int completion,
                       ps_budget **out, lc_error *e) {
  const ps_budget_plan empty = {0};
  ps_budget *b;
  ps_coordination_guard *guard = NULL;
  int receiver = completion && !plan;
  int rc, accounted = 0;
  if (out)
    *out = NULL;
  if (receiver)
    plan = &empty;
  if (!s || !plan || !out || plan->payload_bytes % PS_PAGE ||
      plan->maintenance < 0 || plan->maintenance > 1)
    return invalid(e,
                   "capacity budget requires a page-aligned plan and output");
  b = lc_calloc_with_allocator(NULL, 1, sizeof(*b));
  if (b)
    b->trim = lc_calloc_with_allocator(NULL, 1, sizeof(*b->trim));
  if (b && b->trim)
    b->payload_trim =
        lc_calloc_with_allocator(NULL, 1, sizeof(*b->payload_trim));
  if (!b || !b->trim || !b->payload_trim) {
    if (b)
      lc_free_with_allocator(NULL, b->trim);
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
  if (rc == LC_OK && !receiver)
    rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK && !receiver) {
    s->reserved_payload += plan->payload_bytes;
    s->reserved_metadata += plan->metadata_bytes;
    s->reserved_entries += plan->entries;
    accounted = 1;
    rc = ps_internal_budget_admit(s, plan->maintenance, NULL, plan, e);
    if (rc == LC_OK)
      rc = ps_internal_budget_prepare_record(s, e);
    if (rc == LC_OK)
      rc = ps_internal_budget_ranges(s, s->reserved_payload, NULL, e);
  }
  if (accounted && rc != LC_OK) {
    ps_internal_budget_spend(b, plan->payload_bytes, plan->metadata_bytes,
                             plan->entries);
    ps_internal_budget_trim(s, &b->trim);
    accounted = 0;
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  if (accounted && rc != LC_OK) {
    ps_internal_budget_spend(b, plan->payload_bytes, plan->metadata_bytes,
                             plan->entries);
    ps_internal_budget_trim(s, &b->trim);
  }
  if (rc == LC_OK) {
    if (completion)
      b->remaining.maintenance = 1;
    ++s->budgets;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    *out = b;
  else {
    lc_free_with_allocator(NULL, b->payload_trim);
    lc_free_with_allocator(NULL, b->trim);
    lc_free_with_allocator(NULL, b);
  }
  return rc;
}
int ps_budget_open(ps_space *s, const ps_budget_plan *plan, ps_budget **out,
                   lc_error *e) {
  return budget_open(s, plan, 0, out, e);
}
int ps_budget_open_completion(ps_space *s, const ps_budget_plan *plan,
                              ps_budget **out, lc_error *e) {
  return budget_open(s, plan, 1, out, e);
}
int ps_budget_status(ps_budget *b, ps_budget_plan *out, lc_error *e) {
  if (!b || !out)
    return invalid(e, "capacity budget status requires handle and output");
  pthread_mutex_lock(&b->space->mutex);
  *out = b->remaining;
  pthread_mutex_unlock(&b->space->mutex);
  return LC_OK;
}
int ps_budget_foreground_check(ps_space *s, lc_error *e) {
  const ps_budget_plan empty = {0};
  ps_coordination_guard *guard;
  int rc;
  if (!s)
    return invalid(e, "foreground capacity check requires space");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK)
    rc = ps_internal_budget_admit(s, 0, NULL, &empty, e);
  rc = ps_internal_capture_end(s, guard, rc, e);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_budget_transfer(ps_budget *from, ps_budget *to,
                       const ps_budget_plan *amount, lc_error *e) {
  ps_space *s;
  int rc;
  if (!from || !to || from == to || from->space != to->space || !amount ||
      amount->payload_bytes % PS_PAGE)
    return invalid(e, "capacity transfer requires distinct same-space handles "
                      "and aligned costs");
  s = from->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  if (rc == LC_OK && from->users)
    rc = invalid(e, "close source capacity writers before transfer");
  if (rc == LC_OK && from->remaining.maintenance && !to->remaining.maintenance)
    rc = invalid(e, "maintenance credits cannot fund an ordinary handle");
  if (rc == LC_OK && (amount->payload_bytes > from->remaining.payload_bytes ||
                      amount->metadata_bytes > from->remaining.metadata_bytes ||
                      amount->entries > from->remaining.entries))
    rc = invalid(e, "capacity transfer exceeds unspent source credits");
  if (rc == LC_OK &&
      (amount->payload_bytes > UINT64_MAX - to->remaining.payload_bytes ||
       amount->metadata_bytes > UINT64_MAX - to->remaining.metadata_bytes ||
       amount->entries > UINT64_MAX - to->remaining.entries))
    rc = invalid(e, "capacity transfer totals overflow");
  if (rc == LC_OK) {
    from->remaining.payload_bytes -= amount->payload_bytes;
    from->remaining.metadata_bytes -= amount->metadata_bytes;
    from->remaining.entries -= amount->entries;
    to->remaining.payload_bytes += amount->payload_bytes;
    to->remaining.metadata_bytes += amount->metadata_bytes;
    to->remaining.entries += amount->entries;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_budget_resize(ps_budget *b, const ps_budget_plan *plan, lc_error *e) {
  ps_space *s;
  ps_budget_plan prior;
  ps_run *trim = NULL;
  ps_coordination_guard *guard = NULL;
  uint64_t payload, metadata, entries;
  int rc, grows, accounted = 0;
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
  if (rc == LC_OK && grows)
    rc = ps_internal_admission_begin(s, &guard, e);
  if (rc == LC_OK) {
    s->reserved_payload = payload + plan->payload_bytes;
    s->reserved_metadata = metadata + plan->metadata_bytes;
    s->reserved_entries = entries + plan->entries;
    b->remaining = *plan;
    accounted = 1;
    if (grows)
      rc = ps_internal_budget_admit(s, plan->maintenance, &prior, plan, e);
    if (rc == LC_OK && grows)
      rc = ps_internal_budget_prepare_record(s, e);
    if (rc == LC_OK)
      rc = ps_internal_budget_ranges(s, s->reserved_payload, NULL, e);
  }
  if (accounted && rc != LC_OK) {
    s->reserved_payload = payload + prior.payload_bytes;
    s->reserved_metadata = metadata + prior.metadata_bytes;
    s->reserved_entries = entries + prior.entries;
    b->remaining = prior;
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  if (accounted) {
    if (rc != LC_OK) {
      s->reserved_payload = payload + prior.payload_bytes;
      s->reserved_metadata = metadata + prior.metadata_bytes;
      s->reserved_entries = entries + prior.entries;
      b->remaining = prior;
    }
    ps_internal_budget_trim(s, &trim);
    ps_internal_budget_publish_record(s);
    if (rc == LC_OK)
      rc = ps_internal_healthy(s, e);
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
  if (ps_internal_budget_ranges(
          s, s->reserved_payload - b->remaining.payload_bytes, &b->payload_trim,
          NULL) != LC_OK) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    (void)ps_internal_budget_ranges(
        s, s->reserved_payload - b->remaining.payload_bytes, &b->payload_trim,
        NULL);
  }
  ps_internal_budget_spend(b, b->remaining.payload_bytes,
                           b->remaining.metadata_bytes, b->remaining.entries);
  --s->budgets;
  ps_internal_budget_trim(s, &b->trim);
  ps_internal_budget_publish_record(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, b->trim);
  lc_free_with_allocator(NULL, b->payload_trim);
  lc_free_with_allocator(NULL, b);
  return LC_OK;
}
