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
static int select_guarded(void *context, lc_error *e) {
  repack_selection *s = context;
  struct ps_namespace_data *d = s->data;
  ps_ns_resource *r = s->source;
  int rc;
  pthread_mutex_lock(&d->mutex);
  rc = d->views || ps_ns_find(d, r->kind, r->id) != r
           ? busy(e)
           : ps_key_checkpoint_ranges_select(d->checkpoint, r->kind, r->id,
                                             s->epoch, s->publish,
                                             s->publication, e);
  pthread_mutex_unlock(&d->mutex);
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
  rc = d->views || ps_ns_find(d, r->kind, r->id) != r || coverage.pending ||
               !coverage.epoch || r->id > coverage.segment ||
               (r->id == coverage.segment && coverage.offset != s->tail)
           ? busy(e)
           : ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK && info.tail != s->tail)
    rc = busy(e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_ranges_select(d->checkpoint, r->kind, r->id,
                                         s->epoch, unchanged, NULL, e);
  if (rc == LC_OK && !info.sealed) {
    if (d->active == r->id)
      rotate = 1;
    else if (d->pending_seal != r->id)
      rc = busy(e);
  }
  pthread_mutex_unlock(&d->mutex);
  /* Mutation exclusion, not the data/interval mutex, spans the bounded
   * routing/seal I/O. A new view can still defer subsequent map selection.
   * No application producer or encoded-body copy runs under this authority. */
  if (rc == LC_OK)
    rc = ps_ns_finish_seal(d, e);
  if (rc == LC_OK && rotate)
    rc = ps_ns_rotate(d, s->budget, e);
  ps_ns_leave(d);
  return rc;
}
static int plan_tail(repack_selection *s, ps_budget_plan *plan, lc_error *e) {
  struct ps_namespace_data *d = s->data;
  ps_ns_resource *r = s->source;
  ps_stream_info info;
  int rc;
  pthread_mutex_lock(&d->mutex);
  rc = ps_ns_find(d, r->kind, r->id) != r
           ? busy(e)
           : ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK && info.tail != s->tail)
    rc = busy(e);
  if (rc == LC_OK && !info.sealed) {
    if (d->active == r->id)
      rc = ps_ns_rotation_plan(d, plan, e);
    else if (d->pending_seal != r->id)
      rc = busy(e);
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
static int deferred(int rc, lc_error *e) {
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      (!strcmp(e->server_code, "pouch_piece_needs_validation") ||
       !strcmp(e->server_code, "pouch_namespace_busy"))) {
    lc_error_cleanup(e);
    return 1;
  }
  return 0;
}
int ps_namespace_repack(ps_namespace *ns, unsigned int kind, uint64_t id,
                        size_t limit, ps_piece_select_fn gate, void *context,
                        ps_namespace_repack_result *out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_ns_resource *r = NULL;
  ps_stream_window *pin = NULL;
  ps_stream_info info;
  ps_stream_piece_info piece;
  ps_checkpoint_info coverage;
  ps_checkpoint_ranges live = {0};
  ps_budget_plan plan;
  ps_budget *budget = NULL;
  repack_selection selection;
  uint64_t cursor = 0, cover = 0, encoded, table, footprint;
  int rc, found = 0, matches = 0, partial;
  if (!out || !limit || !id ||
      (kind != PS_CATALOG_SEGMENT && kind != PS_CATALOG_BODY))
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "source repack requires identity, limit and output",
                        NULL, NULL, "pouch");
  memset(out, 0, sizeof(*out));
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  r = ps_ns_find(d, kind, id);
  if (!d->checkpoint_ready || d->views || !r || r->unselected || r->repacking)
    rc = busy(e);
  if (rc == LC_OK)
    rc = ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK) {
    ps_key_checkpoint_info(d->checkpoint, &coverage);
    if (coverage.epoch && kind == PS_CATALOG_SEGMENT && id <= coverage.segment)
      cover = id < coverage.segment ? info.tail : coverage.offset;
    else if (coverage.epoch && kind == PS_CATALOG_BODY && info.sealed)
      cover = info.tail;
    rc = ps_stream_window_open(r->stream, 0, 0, &pin, e);
    if (rc == LC_OK) {
      r->repacking = 1;
      cursor = r->reclaim_cursor;
    }
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK)
    return rc;
  ps_namespace_retain(ns);
  selection.data = d;
  selection.source = r;
  selection.gate = gate;
  selection.context = context;
  selection.tail = info.tail;
  selection.budget = NULL;
  while (rc == LC_OK && out->visited < limit) {
    rc = ps_stream_piece_next(r->stream, cursor, &piece, &found, e);
    if (rc != LC_OK || !found) {
      cursor = 0;
      break;
    }
    if (!piece.completed_bytes || piece.base > cover ||
        piece.completed_bytes > cover - piece.base) {
      out->pending = 1;
      cursor = 0; /* revisit older bodies after the next bounded sweep */
      break;
    }
    partial = !info.sealed && piece.completed_bytes < piece.bytes;
    if (partial && (kind != PS_CATALOG_SEGMENT ||
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
    rc = coverage.pending
             ? busy(e)
             : ps_key_checkpoint_ranges(d->checkpoint, kind, id, piece.base,
                                        piece.completed_bytes, &live, e);
    pthread_mutex_unlock(&d->mutex);
    if (rc != LC_OK)
      break;
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
      if (rc == LC_OK && partial)
        rc = plan_tail(&selection, &plan, e);
      if (rc == LC_OK &&
          (plan.payload_bytes || plan.metadata_bytes || plan.entries))
        rc = ps_budget_open(ns->root->space, &plan, &budget, e);
      if (rc == LC_OK && partial) {
        selection.budget = budget;
        rc = gate ? gate(context, finish_tail, &selection, e)
                  : finish_tail(&selection, e);
        if (rc == LC_OK) {
          info.sealed = 1;
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
  if (out->visited == limit && cursor && !out->finished_tail)
    out->pending = 1;
  if (deferred(rc, e)) {
    out->pending = 1;
    rc = LC_OK;
  }
  pthread_mutex_lock(&d->mutex);
  r->reclaim_cursor = cursor;
  r->repacking = 0;
  pthread_mutex_unlock(&d->mutex);
  ps_stream_window_close(pin);
  ps_namespace_release(ns);
  return rc;
}
