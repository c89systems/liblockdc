#include "lc_api_internal.h"
#include "namespace_internal.h"
#include <string.h>

typedef struct repack_selection {
  struct ps_namespace_data *data;
  ps_ns_resource *source;
  uint64_t epoch;
  uint64_t tail;
  ps_budget *budget;
  ps_piece_select_fn gate;
  void *context, *publication;
  ps_piece_publish_fn publish;
  int unused, sealed;
} repack_selection;
static int unchanged(void *context, lc_error *e) {
  (void)context;
  (void)e;
  return LC_OK;
}
static int busy(lc_error *e) {
  return lc_error_set(e, LC_ERR_INVALID, 0,
                      "namespace body ownership is changing or captured", NULL,
                      "pouch_namespace_busy", "pouch");
}
static int current_source(struct ps_namespace_data *d, ps_ns_resource *r,
                          lc_error *e) {
  return ps_ns_find(d, r->kind, r->id) == r
             ? LC_OK
             : lc_error_set(e, LC_ERR_PENDING, 0,
                            "namespace repack source changed", NULL, NULL,
                            "pouch");
}
static int select_guarded(void *context, lc_error *e) {
  repack_selection *s = context;
  struct ps_namespace_data *d = s->data;
  ps_ns_resource *r = s->source;
  int shared =
      ps_coordination_shared(ps_space_coordination(d->ns->root->space));
  int rc = shared ? ps_ns_enter(d, e) : LC_OK;
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  rc = ps_ns_require_unpinned(d, e);
  if (rc == LC_OK)
    rc = current_source(d, r, e);
  if (rc == LC_OK && !s->sealed && r->kind == PS_CATALOG_SEGMENT &&
      r->id != d->active)
    rc = busy(e); /* Retry a newly inactive source after sealing its tail. */
  if (rc == LC_OK)
    rc = ps_key_checkpoint_ranges_select(
        d->checkpoint, r->kind, r->id, s->epoch, s->publish, s->publication, e);
  if (rc == LC_OK && !s->sealed)
    r->sparse = 1;
  pthread_mutex_unlock(&d->mutex);
  if (shared)
    ps_ns_leave(d);
  return rc;
}
static int select_piece(void *context, ps_piece_publish_fn publish,
                        void *publication, lc_error *e) {
  repack_selection *s = context;
  s->publish = publish;
  s->publication = publication;
  return s->gate ? s->gate(s->context, select_guarded, s, e)
                 : select_guarded(s, e);
}
static int finish_tail(void *context, lc_error *e) {
  repack_selection *s = context;
  struct ps_namespace_data *d = s->data;
  ps_ns_resource *r = s->source;
  ps_checkpoint_info coverage;
  ps_stream_info info;
  int rc = ps_ns_enter(d, e), rotate = 0;
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  rc = ps_ns_require_unpinned(d, e);
  if (rc == LC_OK)
    rc = current_source(d, r, e);
  if (rc == LC_OK)
    rc = (!s->unused &&
          (coverage.pending || !coverage.epoch || r->id > coverage.segment ||
           (r->id == coverage.segment && coverage.offset != s->tail)))
             ? busy(e)
             : ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK && info.tail != s->tail)
    rc = busy(e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_ranges_select(d->checkpoint, r->kind, r->id,
                                         s->epoch, unchanged, NULL, e);
  if (rc == LC_OK && !info.sealed) {
    if (r->kind != PS_CATALOG_SEGMENT || r->id > d->active)
      rc = busy(e);
    else if (d->active == r->id)
      rotate = 1;
  }
  pthread_mutex_unlock(&d->mutex);
  /* Mutation exclusion, not the data/interval mutex, spans the bounded
   * routing/seal I/O. A new view can still defer subsequent map selection.
   * No application producer or encoded-body copy runs under this authority. */
  if (rc == LC_OK && !info.sealed)
    rc = rotate ? ps_ns_rotate(d, s->budget, e)
                : ps_ns_seal_predecessor(d, r->id, e);
  ps_ns_leave(d);
  return rc;
}
static int plan_tail(repack_selection *s, ps_budget_plan *plan, lc_error *e) {
  struct ps_namespace_data *d = s->data;
  ps_ns_resource *r = s->source;
  ps_stream_info info;
  int rc;
  pthread_mutex_lock(&d->mutex);
  rc = current_source(d, r, e);
  if (rc == LC_OK)
    rc = ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK && info.tail != s->tail)
    rc = busy(e);
  if (rc == LC_OK && !info.sealed) {
    if (r->kind != PS_CATALOG_SEGMENT || r->id > d->active)
      rc = busy(e);
    else if (d->active == r->id)
      rc = ps_ns_rotation_plan(d, plan, e);
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
static int deferred(int rc, lc_error *e) {
  if (rc == LC_ERR_PENDING ||
      (rc == LC_ERR_INVALID && e && e->server_code &&
       (!strcmp(e->server_code, "pouch_piece_needs_validation") ||
        !strcmp(e->server_code, "pouch_namespace_busy")))) {
    lc_error_cleanup(e);
    return 1;
  }
  return 0;
}
static int repack_active(struct ps_namespace_data *d, ps_ns_resource *r) {
  ps_ns_resource *job;
  for (job = d->repacking; job; job = job->repack_next)
    if (job->kind == r->kind && job->id == r->id &&
        job->binding.target.incarnation == r->binding.target.incarnation)
      return 1;
  return 0;
}
int ps_namespace_repack(ps_namespace *ns, unsigned int kind, uint64_t id,
                        size_t limit, ps_piece_select_fn gate, void *context,
                        ps_namespace_repack_result *out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_ns_resource *r = NULL, **job;
  ps_stream_info info;
  ps_stream_piece_info piece;
  ps_checkpoint_info coverage;
  ps_checkpoint_ranges live = {0};
  ps_budget_plan plan;
  ps_budget *budget = NULL;
  repack_selection selection;
  uint64_t cursor = 0, cover = 0, encoded, table, footprint;
  int rc, found = 0, matches = 0, partial, unused, shared, inactive = 0;
  if (!out || !limit || !id ||
      (kind != PS_CATALOG_SEGMENT && kind != PS_CATALOG_BODY))
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "source repack requires identity, limit and output",
                        NULL, NULL, "pouch");
  memset(out, 0, sizeof(*out));
  rc = ps_ns_data_lookup(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  shared = ps_coordination_shared(ps_space_coordination(ns->root->space));
  if (shared)
    rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  r = ps_ns_find(d, kind, id);
  rc = ps_ns_require_unpinned(d, e);
  if (rc == LC_OK &&
      (!d->checkpoint_ready || !r || r->unselected || repack_active(d, r)))
    rc = busy(e);
  if (rc == LC_OK)
    rc = ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK) {
    inactive = kind == PS_CATALOG_SEGMENT && id != d->active;
    ps_key_checkpoint_info(d->checkpoint, &coverage);
    if (coverage.epoch && kind == PS_CATALOG_SEGMENT && id <= coverage.segment)
      cover = id < coverage.segment ? info.tail : coverage.offset;
    else if (coverage.epoch && kind == PS_CATALOG_BODY && info.sealed)
      cover = info.tail;
    rc = ps_stream_pin_mappings(r->stream, e);
    if (rc == LC_OK) {
      /* Hold the source and its mappings across the private copy. A shared
       * refresh can install a new owner while this job is outside authority. */
      ps_ns_resource_retain(r);
      __atomic_store_n(&r->repacking, 1, __ATOMIC_RELEASE);
      r->repack_next = d->repacking;
      d->repacking = r;
      cursor = r->reclaim_cursor;
    } else
      ps_ns_resource_park(d, r);
  }
  pthread_mutex_unlock(&d->mutex);
  if (shared)
    ps_ns_leave(d);
  if (rc != LC_OK)
    return rc;
  ps_namespace_retain(ns);
  selection.data = d;
  selection.source = r;
  selection.gate = gate;
  selection.context = context;
  selection.tail = info.tail;
  selection.budget = NULL;
  selection.unused = 0;
  selection.sealed = info.sealed;
  while (rc == LC_OK && out->visited < limit) {
    rc = ps_stream_piece_next(r->stream, cursor, &piece, &found, e);
    if (rc != LC_OK || !found) {
      cursor = 0;
      break;
    }
    unused = kind == PS_CATALOG_SEGMENT && !piece.completed_bytes &&
             piece.base >= info.tail;
    if (!unused && (!piece.completed_bytes || piece.base > cover ||
                    piece.completed_bytes > cover - piece.base)) {
      out->pending = 1;
      cursor = 0; /* revisit older bodies after the next bounded sweep */
      break;
    }
    partial = !info.sealed && (unused || piece.completed_bytes < piece.bytes);
    if (partial && !unused &&
        (kind != PS_CATALOG_SEGMENT ||
         piece.base + piece.completed_bytes != info.tail)) {
      out->pending = 1;
      cursor = 0;
      break;
    }
    ++out->visited;
    /* A new sweep starts from selected key metadata. In particular, a newly
     * superseded body may still belong to the selected checkpoint while its
     * replacement is only an uncovered canonical suffix. Captures after such
     * a mutation wait for the next key checkpoint. Changes after this capture
     * are instead rejected by the source epoch at selection. */
    pthread_mutex_lock(&d->mutex);
    ps_key_checkpoint_info(d->checkpoint, &coverage);
    rc = current_source(d, r, e);
    if (rc == LC_OK)
      rc = coverage.pending && !unused
               ? busy(e)
               : ps_key_checkpoint_ranges(
                     d->checkpoint, kind, id, piece.base,
                     unused ? piece.bytes : piece.completed_bytes, &live, e);
    pthread_mutex_unlock(&d->mutex);
    if (rc != LC_OK)
      break;
    if (unused && live.count) {
      rc = lc_error_set(e, LC_ERR_PROTOCOL, 0,
                        "canonical body extends beyond committed source tail",
                        NULL, NULL, "pouch");
      break;
    }
    selection.unused = unused;
    selection.epoch = live.epoch;
    if (piece.mapped)
      rc = ps_stream_piece_matches(r->stream, piece.base, live.ranges,
                                   live.count, &matches, e);
    else
      matches = live.count == 1 && !live.ranges[0].offset &&
                live.ranges[0].bytes == piece.bytes;
    table = (uint64_t)live.count * 8;
    if (table > (piece.bytes + 7) / 8)
      table = (piece.bytes + 7) / 8;
    encoded = 64 + table + live.bytes;
    footprint = encoded / PS_PAGE * PS_PAGE +
                (encoded % PS_PAGE + PS_CELL - 1) / PS_CELL * PS_CELL;
    /* Do not replace unchanged ranges, or grow an almost entirely live raw
     * piece just to remove its dead sliver. Zero live ranges need no copy. */
    if (rc == LC_OK && !matches &&
        (!live.count || footprint < piece.stored_bytes)) {
      memset(&plan, 0, sizeof(plan));
      plan.maintenance = 1;
      rc = ps_budget_plan_piece(&plan, piece.bytes, live.count, live.bytes, e);
      if (rc == LC_OK && !info.sealed && (partial || inactive))
        rc = plan_tail(&selection, &plan, e);
      if (rc == LC_OK &&
          (plan.payload_bytes || plan.metadata_bytes || plan.entries))
        rc = ps_budget_open(ns->root->space, &plan, &budget, e);
      /* Active full pieces can become sparse behind their checkpoint.
       * An inactive source needs a final tail before it can gain holes. */
      if (rc == LC_OK && !info.sealed && (partial || inactive)) {
        selection.budget = budget;
        rc = gate ? gate(context, finish_tail, &selection, e)
                  : finish_tail(&selection, e);
        if (rc == LC_OK) {
          info.sealed = 1;
          selection.sealed = 1;
          out->finished_tail = 1;
        }
      }
      if (rc == LC_OK)
        rc = ps_stream_piece_repack(r->stream, piece.base, budget, live.ranges,
                                    live.count, select_piece, &selection, e);
      if (rc == LC_OK) {
        ++out->replaced;
        out->body_bytes += live.bytes;
        if (live.count)
          out->mapping_bytes += encoded;
      }
      if (budget) {
        (void)ps_budget_close(budget, NULL);
        budget = NULL;
      }
      if (deferred(rc, e)) {
        out->pending = 1;
        rc = LC_OK;
        cursor = piece.base;
        lc_free_with_allocator(NULL, live.ranges);
        memset(&live, 0, sizeof(live));
        break; /* retry this generation after its canonical state settles */
      }
    }
    lc_free_with_allocator(NULL, live.ranges);
    memset(&live, 0, sizeof(live));
    if (rc == LC_OK)
      cursor = piece.base + piece.bytes;
  }
  lc_free_with_allocator(NULL, live.ranges);
  if (rc == LC_OK && out->visited == limit && cursor) {
    rc = ps_stream_piece_next(r->stream, cursor, &piece, &found, e);
    if (rc == LC_OK) {
      out->pending = found;
      if (!found)
        cursor = 0;
    }
  }
  if (deferred(rc, e)) {
    out->pending = 1;
    rc = LC_OK;
  }
  pthread_mutex_lock(&d->mutex);
  r->reclaim_cursor = cursor;
  for (job = &d->repacking; *job != r; job = &(*job)->repack_next) {
  }
  *job = r->repack_next;
  r->repack_next = NULL;
  __atomic_store_n(&r->repacking, 0, __ATOMIC_RELEASE);
  ps_ns_resource_park(d, r);
  pthread_mutex_unlock(&d->mutex);
  ps_ns_resource_release(r);
  ps_namespace_release(ns);
  return rc;
}
