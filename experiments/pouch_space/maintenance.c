#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "namespace.h"
#include "namespace_work.h"
#include "pouch_internal.h"
#include <string.h>

typedef struct compact {
  lc_pouch *pouch;
  const char *name, *reason;
  ps_namespace *ns;
  ps_namespace_status before, after;
  const lc_pouch_maintenance_options *options;
  lc_pouch_maintenance_result *maintenance;
  ps_pouch_compaction_result result;
  uint64_t candidates, bytes;
} compact;
static int fail(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL, NULL, "pouch");
}
static int candidate(void *context, const ps_namespace_source *source,
                     lc_error *e) {
  compact *w = context;
  int active = !w->options || w->options->force ||
               (w->options->terminal_reclaim &&
                w->before.tail >= w->pouch->terminal_reclaim_min_bytes);
  if (source->kind != PS_CATALOG_SEGMENT ||
      source->id < w->before.covered_segment ||
      (source->id == w->before.active_segment && !active))
    return LC_OK;
  if (source->tail > UINT64_MAX - w->bytes)
    return fail(e, LC_ERR_PROTOCOL, "maintenance candidate size overflow");
  ++w->candidates;
  w->bytes += source->tail;
  return LC_OK;
}
static int retire_source(void *context, const ps_namespace_source *source,
                         lc_error *e) {
  compact *w = context;
  int rc;
  if (source->kind == PS_CATALOG_SEGMENT &&
      source->id == w->after.active_segment && source->tail &&
      source->id == w->after.covered_segment &&
      source->tail == w->after.covered_offset) {
    ++w->result.pending_sources;
    return LC_OK;
  }
  if (!source->sealed || (source->kind == PS_CATALOG_SEGMENT &&
                          (source->id == w->after.active_segment ||
                           source->id > w->after.covered_segment ||
                           (source->id == w->after.covered_segment &&
                            source->tail > w->after.covered_offset))))
    return LC_OK;
  if (ps_namespace_source_refs(w->ns, source->kind, source->id)) {
    /* Mixed live sources require the bounded encoded-range relocation stage.
     * Preserve ownership and its work marker until that stage can reclaim it.
     */
    ++w->result.pending_sources;
    return LC_OK;
  }
  rc = ps_namespace_reclaim(w->ns, source->kind, source->id, e);
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_busy")) {
    lc_error_cleanup(e);
    ++w->result.pending_sources;
    return LC_OK;
  }
  if (rc == LC_OK)
    ++w->result.reclaimed_sources;
  return rc;
}
static int compact_locked(void *context, lc_error *e) {
  compact *w = context;
  uint64_t closed, pending, minimum_bytes;
  unsigned long minimum_segments;
  int present, rc;
  rc = w->pouch->storage.ops->namespace_open(w->pouch->storage.context, w->name,
                                             0, (void **)&w->ns, e);
  if (rc == LC_OK && w->options)
    rc = ps_namespace_work_get(w->ns, LC_POUCH_STORAGE_WORK_ORDINARY, &present,
                               e);
  if (rc == LC_OK && w->options && w->options->terminal_reclaim)
    rc = ps_namespace_work_get(w->ns, LC_POUCH_STORAGE_WORK_TERMINAL, &present,
                               e);
  if (rc == LC_OK)
    rc = ps_namespace_status_get(w->ns, &w->before, e);
  if (rc == LC_OK)
    rc = ps_namespace_reap(w->ns, &closed, &pending, e);
  if (rc != LC_OK)
    return rc;
  if (w->maintenance) {
    w->maintenance->cleanup_deleted_count += (unsigned long)closed;
    w->maintenance->cleanup_pending_count += (unsigned long)pending;
  }
  if (!w->before.active_segment ||
      (!w->before.checkpoint_epoch && !w->before.tail &&
       w->before.active_segment == 1)) {
    w->result.skipped = 1;
    w->reason = "no-candidates";
  } else if (w->options && (w->options->cleanup_only ||
                            w->options->retention_updated_before_unix > 0)) {
    w->result.skipped = 1;
    w->reason = "cleanup-complete";
  }
  if (w->result.skipped)
    return LC_OK;
  rc = ps_namespace_sources_visit(w->ns, candidate, w, e);
  if (rc == LC_OK && w->maintenance) {
    w->maintenance->candidate_segment_count = (unsigned long)w->candidates;
    w->maintenance->candidate_bytes = w->bytes;
  }
  if (rc == LC_OK && w->options && !w->options->force) {
    minimum_segments = w->options->terminal_reclaim
                           ? 1UL
                           : w->pouch->compaction_min_segment_count;
    minimum_bytes = w->options->terminal_reclaim
                        ? w->pouch->terminal_reclaim_min_bytes
                        : w->pouch->compaction_min_reclaimable_bytes;
    if (!w->pouch->background_compaction_enabled)
      w->reason = "disabled";
    else if (!w->candidates)
      w->reason = "no-candidates";
    else if (w->candidates < minimum_segments)
      w->reason = "below-segment-threshold";
    else if (w->bytes < minimum_bytes)
      w->reason = "below-reclaimable-threshold";
    if (w->reason) {
      w->result.skipped = 1;
      return LC_OK;
    }
  }
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_flush(w->ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_status_get(w->ns, &w->after, e);
  if (rc == LC_OK) {
    w->result.snapshot = w->after.checkpoint_epoch;
    w->result.boundary = w->after.covered_segment;
    w->result.records = w->after.checkpoint_keys;
    w->result.metadata_bytes = w->before.pending_metadata_bytes;
    rc = ps_namespace_sources_visit(w->ns, retire_source, w, e);
  }
  if (rc == LC_OK)
    rc = ps_namespace_reap(w->ns, &closed, &pending, e);
  if (rc == LC_OK) {
    w->result.pending_sources += pending;
    if (w->maintenance)
      w->maintenance->cleanup_deleted_count += (unsigned long)closed;
  }
  if (rc == LC_OK && w->options && w->options->terminal_reclaim &&
      !w->result.pending_sources && !w->maintenance->cleanup_pending_count &&
      w->after.tail < w->pouch->terminal_reclaim_min_bytes)
    rc = lc_pouch_terminal_reclaim_marker_remove(w->pouch, w->name, e);
  return rc;
}
static int compact_run(lc_pouch *pouch, const char *name,
                       const lc_pouch_maintenance_options *options,
                       lc_pouch_maintenance_result *maintenance,
                       ps_pouch_compaction_result *out, lc_error *e) {
  compact w;
  ps_root *root = ps_pouch_state_root(pouch);
  uint64_t orphaned, skipped;
  int rc;
  memset(&w, 0, sizeof(w));
  if (out)
    memset(out, 0, sizeof(*out));
  if (!root || !name || !name[0])
    return fail(e, LC_ERR_INVALID,
                "container maintenance requires owner and namespace");
  w.pouch = pouch;
  w.name = name;
  w.options = options;
  w.maintenance = maintenance;
  rc = lc_pouch_state_with_namespace_lock(pouch, name, compact_locked, &w, e);
  if (w.ns)
    pouch->storage.ops->namespace_close(pouch->storage.context, w.ns);
  if (rc == LC_OK && !w.result.skipped && !maintenance)
    rc = ps_root_reclaim_orphans(root, &orphaned, &skipped, e);
  if (rc == LC_OK && !w.result.skipped && !maintenance)
    rc = ps_checkpoint(ps_root_space(root), e);
  if (rc == LC_OK && maintenance) {
    maintenance->skipped = w.result.skipped;
    maintenance->compacted = !w.result.skipped;
    maintenance->compacted_segment_id = w.result.snapshot;
    maintenance->cleanup_deleted_count +=
        (unsigned long)w.result.reclaimed_sources;
    maintenance->cleanup_pending_count +=
        (unsigned long)w.result.pending_sources;
    if (w.reason) {
      lc_free_with_allocator(&pouch->allocator, maintenance->diagnostic);
      maintenance->diagnostic =
          lc_strdup_with_allocator(&pouch->allocator, w.reason);
      if (!maintenance->diagnostic)
        rc = fail(e, LC_ERR_NOMEM, "cannot retain maintenance diagnostic");
    }
  }
  if (rc == LC_OK && out)
    *out = w.result;
  return rc;
}
int ps_pouch_state_compact(lc_pouch *pouch, const char *name,
                           ps_pouch_compaction_result *out, lc_error *e) {
  return compact_run(pouch, name, NULL, NULL, out, e);
}
int ps_pouch_state_maintain(lc_pouch *pouch,
                            const lc_pouch_maintenance_options *options,
                            lc_pouch_maintenance_result *out, lc_error *e) {
  int rc = compact_run(pouch, options->ns, options, out, NULL, e);
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_busy")) {
    lc_error_cleanup(e);
    lc_error_init(e);
    out->skipped = 1;
    ++out->cleanup_pending_count;
    return LC_OK;
  }
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_not_found")) {
    lc_error_cleanup(e);
    lc_error_init(e);
    out->skipped = 1;
    return LC_OK;
  }
  return rc;
}
