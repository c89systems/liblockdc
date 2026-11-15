#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "namespace.h"
#include "pouch_internal.h"
#include <stdlib.h>
#include <string.h>

typedef struct source_plan {
  ps_namespace_source source;
  uint64_t live, references;
} source_plan;
typedef struct compact {
  lc_pouch *pouch;
  const char *name, *copy_leaf;
  ps_root *root;
  ps_namespace *ns;
  ps_namespace_snapshot *snapshot;
  lc_pouch_state_snapshot *capture;
  source_plan *sources;
  size_t count, capacity;
  const char **protected;
  size_t protected_count, protected_capacity;
  unsigned char *scratch;
  int selected;
  uint64_t abandoned_body;
  ps_pouch_compaction_result result;
} compact;

static int fail(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch");
}
static int replay(void *context, lc_pouch_storage_visit_fn visit, void *visitor,
                  lc_error *e) {
  return ps_namespace_view_visit(context, 0, visit, visitor, e);
}
static int source_compare(const void *a, const void *b) {
  const source_plan *left = a, *right = b;
  return strcmp(left->source.leaf, right->source.leaf);
}
static source_plan *find_source(compact *w, const char *leaf) {
  size_t lo = 0, hi = w->count, mid;
  int cmp;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2;
    cmp = strcmp(leaf, w->sources[mid].source.leaf);
    if (!cmp)
      return w->sources + mid;
    if (cmp < 0)
      hi = mid;
    else
      lo = mid + 1;
  }
  return NULL;
}
static int collect_source(void *context, const ps_namespace_source *source,
                          lc_error *e) {
  compact *w = context;
  source_plan *grown;
  size_t capacity;
  if (w->count == w->capacity) {
    capacity = w->capacity ? w->capacity * 2 : 16;
    if (capacity < w->capacity || capacity > (size_t)-1 / sizeof(*w->sources))
      return fail(e, LC_ERR_NOMEM,
                  "maintenance source plan exceeds local limit");
    grown = lc_realloc_with_allocator(&w->pouch->allocator, w->sources,
                                      capacity * sizeof(*grown));
    if (!grown)
      return fail(e, LC_ERR_NOMEM, "cannot allocate maintenance source plan");
    w->sources = grown;
    w->capacity = capacity;
  }
  memset(w->sources + w->count, 0, sizeof(*w->sources));
  w->sources[w->count++].source = *source;
  return LC_OK;
}
static int count_live(void *context, const lc_pouch_state_entry *r,
                      lc_error *e) {
  compact *w = context;
  source_plan *source;
  if (r->control)
    return LC_OK;
  ++w->result.records;
  if (!r->found || !r->payload_span.present)
    return LC_OK;
  source = find_source(w, r->payload_span.container_leaf);
  if (!source || r->cipher_bytes > UINT64_MAX - source->live)
    return fail(e, LC_ERR_PROTOCOL,
                "logical snapshot has an unavailable body source");
  source->live += r->cipher_bytes;
  ++source->references;
  return LC_OK;
}
static int plan_snapshot(void *context, ps_namespace_view *view,
                         uint64_t boundary, ps_namespace_snapshot_plan *plan,
                         lc_error *e) {
  compact *w = context;
  source_plan *best = NULL, *source;
  uint64_t score, best_score = 0, bodies = 0;
  size_t i;
  int rc =
      lc_pouch_state_snapshot_capture(w->pouch, replay, view, &w->capture, e);
  if (rc == LC_OK)
    rc = ps_namespace_sources_visit(w->ns, collect_source, w, e);
  if (rc == LC_OK) {
    if (w->count > 1)
      qsort(w->sources, w->count, sizeof(*w->sources), source_compare);
    rc = lc_pouch_state_snapshot_visit(w->capture, count_live, w, e);
  }
  if (rc != LC_OK)
    return rc;
  for (i = 0; i < w->count; ++i) {
    source = w->sources + i;
    if ((source->source.kind == PS_CATALOG_SEGMENT
             ? source->source.id > boundary
             : !source->source.sealed) ||
        (source->source.kind == PS_CATALOG_SNAPSHOT && !source->references))
      continue;
    score = source->live < source->source.capacity
                ? source->source.capacity - source->live
                : 0;
    if (!best || score > best_score) {
      best = source;
      best_score = score;
    }
  }
  if (best && best->references) {
    w->copy_leaf = best->source.leaf;
    w->result.copied_bytes = best->live;
  }
  /* A body label has a fixed width; the actual private pack receives a fresh
   * identity after measurement. No body is read for this sizing pass. */
  rc = lc_pouch_state_snapshot_measure(
      w->capture, "body-00000000000000000001.log", w->copy_leaf,
      &w->result.metadata_bytes, &bodies, e);
  w->result.body_bytes = bodies;
  if (rc == LC_OK) {
    plan->metadata_bytes = w->result.metadata_bytes;
    plan->body_bytes = bodies;
  }
  return rc;
}
static int begin_locked(void *context, lc_error *e) {
  compact *w = context;
  ps_namespace_status status;
  int rc = w->pouch->storage.ops->namespace_open(
      w->pouch->storage.context, w->name, 0, (void **)&w->ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_status_get(w->ns, &status, e);
  if (rc == LC_OK &&
      (!status.active_segment ||
       (!status.snapshot && !status.tail && status.active_segment == 1))) {
    w->result.skipped = 1;
    return LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_namespace_snapshot_begin(w->ns, plan_snapshot, w, &w->snapshot, e);
  return rc;
}
static int write_record(void *context, const lc_pouch_state_entry *r,
                        lc_error *e) {
  compact *w = context;
  lc_source *encoded = NULL;
  lc_pouch_state_entry relocated;
  uint64_t start = 0, end = 0;
  int rc = LC_OK;
  if (r->control)
    return lc_pouch_state_record_write_high_water(
        &w->pouch->allocator, ps_namespace_snapshot_output(w->snapshot),
        r->index_seq, r->query_index_seq, e);
  relocated = *r;
  if (w->copy_leaf && r->found && r->payload_span.present &&
      !strcmp(r->payload_span.container_leaf, w->copy_leaf)) {
    if (r->cipher_bytes && !w->scratch) {
      w->scratch = lc_alloc_with_allocator(&w->pouch->allocator,
                                           LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES);
      if (!w->scratch)
        return fail(e, LC_ERR_NOMEM,
                    "cannot allocate maintenance copy scratch");
    }
    rc = w->pouch->storage.ops->source_open(
        w->pouch->storage.context, w->ns, r->payload_span.container_leaf,
        r->payload_span.payload_offset, r->payload_span.payload_length,
        r->payload_span.payload_crc, &encoded, e);
    if (rc == LC_OK)
      rc = lc_pouch_record_io_tail(
          ps_namespace_snapshot_body_output(w->snapshot), &start, e);
    if (rc == LC_OK)
      rc = lc_pouch_state_record_write_snapshot(
          &w->pouch->allocator, ps_namespace_snapshot_body_output(w->snapshot),
          ps_namespace_snapshot_body_leaf(w->snapshot), r, encoded, w->scratch,
          LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES, e);
    if (encoded)
      encoded->close(encoded);
    if (rc == LC_OK)
      rc = lc_pouch_record_io_tail(
          ps_namespace_snapshot_body_output(w->snapshot), &end, e);
    if (rc == LC_OK) {
      relocated.payload_span.container_leaf =
          (char *)ps_namespace_snapshot_body_leaf(w->snapshot);
      relocated.payload_span.record_offset = start;
      relocated.payload_span.payload_offset = end - r->cipher_bytes;
    }
  }
  if (rc == LC_OK)
    rc = lc_pouch_state_record_write_snapshot(
        &w->pouch->allocator, ps_namespace_snapshot_output(w->snapshot),
        ps_namespace_snapshot_leaf(w->snapshot), &relocated, NULL, NULL, 0, e);
  return rc;
}
static int string_compare(const void *a, const void *b) {
  return strcmp(*(const char *const *)a, *(const char *const *)b);
}
static int protect_body(void *context, const lc_pouch_state_entry *r,
                        lc_error *e) {
  compact *w = context;
  const char **grown;
  size_t capacity;
  if (!r->found || !r->payload_span.present)
    return LC_OK;
  if (w->protected_count == w->protected_capacity) {
    capacity = w->protected_capacity ? w->protected_capacity * 2 : 32;
    if (capacity < w->protected_capacity ||
        capacity > (size_t)-1 / sizeof(*grown))
      return fail(e, LC_ERR_NOMEM,
                  "maintenance reference set exceeds local limit");
    grown = lc_realloc_with_allocator(&w->pouch->allocator, w->protected,
                                      capacity * sizeof(*grown));
    if (!grown)
      return fail(e, LC_ERR_NOMEM, "cannot allocate maintenance reference set");
    w->protected = grown;
    w->protected_capacity = capacity;
  }
  w->protected[w->protected_count++] = r->payload_span.container_leaf;
  return LC_OK;
}
static int reclaim_source(void *context, const ps_namespace_source *source,
                          lc_error *e) {
  compact *w = context;
  const char *leaf = source->leaf;
  int rc;
  if ((source->kind == PS_CATALOG_SEGMENT && source->id > w->result.boundary) ||
      (source->kind == PS_CATALOG_SNAPSHOT && source->id >= w->result.snapshot))
    return LC_OK;
  if (w->protected_count && bsearch(&leaf, w->protected, w->protected_count,
                                    sizeof(*w->protected), string_compare))
    return LC_OK;
  rc = ps_namespace_reclaim(w->ns, source->kind, source->id, e);
  if (rc == LC_OK)
    ++w->result.reclaimed_sources;
  return rc;
}
static int publish_locked(void *context, lc_error *e) {
  compact *w = context;
  ps_namespace_view *view = NULL;
  lc_pouch_state_snapshot *current = NULL;
  uint64_t end, closed;
  unsigned int kind;
  int rc = lc_pouch_record_io_tail(ps_namespace_snapshot_output(w->snapshot),
                                   &end, e);
  if (rc == LC_OK && end != w->result.metadata_bytes)
    rc = fail(e, LC_ERR_PROTOCOL,
              "snapshot differs from measured canonical size");
  if (rc == LC_OK)
    rc = ps_namespace_snapshot_commit(w->snapshot, e);
  if (rc != LC_OK)
    return rc;
  w->selected = 1;
  w->result.boundary = ps_namespace_snapshot_boundary(w->snapshot);
  if (!ps_namespace_parse_source_leaf(ps_namespace_snapshot_leaf(w->snapshot),
                                      &kind, &w->result.snapshot))
    return fail(e, LC_ERR_PROTOCOL, "selected snapshot label is invalid");
  /* Drop this operation's captured pins before retirement. The logical
   * projection owns metadata independently; caller-owned read pins remain. */
  ps_namespace_snapshot_close(w->snapshot);
  w->snapshot = NULL;
  rc = ps_namespace_view_open(w->ns, &view, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_snapshot_capture(w->pouch, replay, view, &current, e);
  ps_namespace_view_close(view);
  if (rc == LC_OK)
    rc = lc_pouch_state_snapshot_visit(current, protect_body, w, e);
  if (rc == LC_OK) {
    if (w->protected_count > 1)
      qsort(w->protected, w->protected_count, sizeof(*w->protected),
            string_compare);
    rc = ps_namespace_sources_visit(w->ns, reclaim_source, w, e);
  }
  if (rc == LC_OK)
    rc = ps_namespace_reap(w->ns, &closed, &w->result.pending_sources, e);
  lc_pouch_state_snapshot_close(current);
  return rc;
}
static int cleanup_body(void *context, lc_error *e) {
  compact *w = context;
  return ps_namespace_reclaim(w->ns, PS_CATALOG_BODY, w->abandoned_body, e);
}
int ps_pouch_state_compact(lc_pouch *pouch, const char *name,
                           ps_pouch_compaction_result *out, lc_error *e) {
  compact w;
  lc_error ignored;
  unsigned int kind;
  uint64_t orphaned, skipped;
  int rc;
  memset(&w, 0, sizeof(w));
  if (out)
    memset(out, 0, sizeof(*out));
  w.root = ps_pouch_state_root(pouch);
  if (!w.root || !name || !name[0])
    return fail(e, LC_ERR_INVALID,
                "ordinary container compaction requires owner and namespace");
  w.pouch = pouch;
  w.name = name;
  rc = lc_pouch_state_with_namespace_lock(pouch, name, begin_locked, &w, e);
  if (rc == LC_OK && !w.result.skipped)
    rc = lc_pouch_state_snapshot_visit(w.capture, write_record, &w, e);
  lc_pouch_state_snapshot_close(w.capture);
  w.capture = NULL;
  if (rc == LC_OK && !w.result.skipped)
    rc = lc_pouch_state_snapshot_publish(pouch, name, publish_locked, &w, e);
  if (rc != LC_OK && !w.selected && w.snapshot &&
      ps_check(ps_root_space(w.root), NULL) == LC_OK)
    (void)ps_namespace_parse_source_leaf(
        ps_namespace_snapshot_body_leaf(w.snapshot), &kind, &w.abandoned_body);
  ps_namespace_snapshot_close(w.snapshot);
  if (w.abandoned_body) {
    lc_error_init(&ignored);
    (void)lc_pouch_state_with_namespace_lock(pouch, name, cleanup_body, &w,
                                             &ignored);
    lc_error_cleanup(&ignored);
  }
  lc_pouch_state_snapshot_close(w.capture);
  lc_free_with_allocator(&pouch->allocator, w.scratch);
  lc_free_with_allocator(&pouch->allocator, w.sources);
  lc_free_with_allocator(&pouch->allocator, w.protected);
  if (w.ns)
    pouch->storage.ops->namespace_close(pouch->storage.context, w.ns);
  if (rc == LC_OK && !w.result.skipped)
    rc = ps_root_reclaim_orphans(w.root, &orphaned, &skipped, e);
  if (rc == LC_OK && !w.result.skipped)
    rc = ps_checkpoint(ps_root_space(w.root), e);
  if (rc == LC_OK && out)
    *out = w.result;
  return rc;
}
