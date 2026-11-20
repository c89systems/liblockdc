#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "namespace.h"
#include "namespace_work.h"
#include "pouch_internal.h"
#include <string.h>

enum { MAINTENANCE_PIECES = 16 };
typedef struct compact {
  lc_pouch *pouch;
  const char *name, *reason;
  ps_namespace *ns;
  ps_namespace_status before, after;
  const lc_pouch_maintenance_options *options;
  lc_pouch_maintenance_result *maintenance;
  ps_pouch_compaction_result result;
  uint64_t candidates, bytes;
  uint64_t selected_id, selected_bytes;
  unsigned int selected_kind;
  int background;
} compact;
static int fail(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL, NULL, "pouch");
}
static int candidate(void *context, const ps_namespace_source *source,
                     lc_error *e) {
  compact *w = context;
  uint64_t bytes = source->stored_bytes;
  int active = w->background || !w->options || w->options->force ||
               (w->options->terminal_reclaim &&
                source->stored_bytes >= w->pouch->terminal_reclaim_min_bytes);
  if ((source->kind != PS_CATALOG_SEGMENT && source->kind != PS_CATALOG_BODY) ||
      !source->stored_bytes ||
      (source->kind == PS_CATALOG_SEGMENT &&
       source->id == w->before.active_segment && !active))
    return LC_OK;
  if (w->background) {
    uint64_t live = ps_namespace_source_bytes(w->ns, source->kind, source->id);
    uint64_t minimum = w->pouch->compaction_min_reclaimable_bytes;
    if (minimum > MAINTENANCE_PIECES * PS_CHUNK)
      minimum = MAINTENANCE_PIECES * PS_CHUNK;
    if (live >= bytes)
      return LC_OK;
    bytes -= live;
    /* Discount page rounding and sparse-map overhead for retained bodies.
     * This estimate only schedules work. Selection uses exact ranges. */
    if (live) {
      if (source->runs > bytes / PS_PAGE)
        return LC_OK;
      bytes -= source->runs * PS_PAGE;
    }
    if (!bytes || bytes < minimum)
      return LC_OK;
    if (bytes > w->selected_bytes) {
      w->selected_bytes = bytes;
      w->selected_kind = source->kind;
      w->selected_id = source->id;
    }
  }
  if (bytes > UINT64_MAX - w->bytes)
    return fail(e, LC_ERR_PROTOCOL, "maintenance candidate size overflow");
  ++w->candidates;
  w->bytes += bytes;
  return LC_OK;
}
static int select_logical(void *context, ps_piece_publish_fn publish,
                          void *publication, lc_error *e) {
  compact *w = context;
  return lc_pouch_state_with_namespace_lock(w->pouch, w->name, publish,
                                            publication, e);
}
typedef struct retire_task {
  compact *work;
  unsigned int kind;
  uint64_t id;
  int active, rotated;
} retire_task;
static int retire_locked(void *context, lc_error *e) {
  retire_task *task = context;
  compact *w = task->work;
  int rc;
  if (task->active) {
    rc = ps_namespace_rotate_obsolete(w->ns, task->id, &task->rotated, e);
    if (rc != LC_OK || !task->rotated)
      return rc;
  }
  if (ps_namespace_source_refs(w->ns, task->kind, task->id)) {
    ++w->result.pending_sources;
    return LC_OK;
  }
  rc = ps_namespace_reclaim(w->ns, task->kind, task->id, e);
  if (rc == LC_OK)
    ++w->result.reclaimed_sources;
  return rc;
}
static int retire_source(void *context, const ps_namespace_source *source,
                         lc_error *e) {
  compact *w = context;
  ps_namespace_repack_result repack;
  retire_task task = {0};
  int rc;
  if ((source->kind != PS_CATALOG_SEGMENT && source->kind != PS_CATALOG_BODY) ||
      (source->kind == PS_CATALOG_SEGMENT &&
       source->id > w->after.covered_segment) ||
      (w->background &&
       (source->kind != w->selected_kind || source->id != w->selected_id)))
    return LC_OK;
  task.work = w;
  task.kind = source->kind;
  task.id = source->id;
  if (source->kind == PS_CATALOG_SEGMENT &&
      source->id == w->after.active_segment && source->tail &&
      !ps_namespace_source_refs(w->ns, source->kind, source->id)) {
    task.active = 1;
    rc = lc_pouch_state_with_namespace_lock(w->pouch, w->name, retire_locked,
                                            &task, e);
    task.active = 0;
    if (rc != LC_OK || task.rotated)
      goto outcome;
  }
  if (!source->sealed ||
      (source->kind == PS_CATALOG_SEGMENT &&
       source->id == w->after.active_segment) ||
      ps_namespace_source_refs(w->ns, source->kind, source->id)) {
    rc = ps_namespace_repack(w->ns, source->kind, source->id,
                             MAINTENANCE_PIECES, select_logical, w, &repack, e);
    if (rc == LC_OK) {
      w->result.body_bytes += repack.body_bytes;
      w->result.copied_bytes += repack.mapping_bytes;
      if (repack.pending)
        ++w->result.pending_sources;
      else if ((source->sealed || repack.finished_tail) &&
               !ps_namespace_source_refs(w->ns, source->kind, source->id)) {
        rc = lc_pouch_state_with_namespace_lock(w->pouch, w->name,
                                                retire_locked, &task, e);
      }
    }
  } else {
    rc = lc_pouch_state_with_namespace_lock(w->pouch, w->name, retire_locked,
                                            &task, e);
  }
outcome:
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_busy")) {
    lc_error_cleanup(e);
    ++w->result.pending_sources;
    return LC_OK;
  }
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
  if ((!w->before.active_segment && !w->before.checkpoint_keys) ||
      (!w->before.checkpoint_epoch && !w->before.tail && !w->before.runs &&
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
  if (w->background && w->before.runs)
    rc = ps_namespace_checkpoint_recover(w->ns, e);
  if (rc == LC_OK)
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
    else if (!w->background && w->candidates < minimum_segments)
      w->reason = "below-segment-threshold";
    else if (!w->background && w->bytes < minimum_bytes)
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
  }
  return rc;
}
static int finish_locked(void *context, lc_error *e) {
  compact *w = context;
  uint64_t closed, pending;
  int rc = ps_namespace_status_get(w->ns, &w->after, e);
  if (rc == LC_OK)
    rc = ps_namespace_reap(w->ns, &closed, &pending, e);
  if (rc == LC_OK) {
    w->result.pending_sources += pending;
    if (w->maintenance)
      w->maintenance->cleanup_deleted_count += (unsigned long)closed;
  }
  if (rc == LC_OK && w->options && w->options->terminal_reclaim &&
      !w->result.pending_sources && !w->maintenance->cleanup_pending_count &&
      !w->after.pending_keys &&
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
  w.background = options && options->background && !options->force;
  rc = lc_pouch_state_with_namespace_lock(pouch, name, compact_locked, &w, e);
  /* Private encoded copying owns no logical or physical publication gate.
   * Every durable map selection reacquires logical authority and rechecks its
   * captured body generation. Whole-source retirement and final work-marker
   * decisions are likewise serialized with canonical mutations. */
  if (rc == LC_OK && !w.result.skipped)
    rc = ps_namespace_sources_visit(w.ns, retire_source, &w, e);
  if (rc == LC_OK && !w.result.skipped)
    rc = lc_pouch_state_with_namespace_lock(pouch, name, finish_locked, &w, e);
  if (w.ns)
    pouch->storage.ops->namespace_close(pouch->storage.context, w.ns);
  if (rc == LC_OK && !w.result.skipped && !maintenance)
    rc = ps_root_reclaim_orphans(root, &orphaned, &skipped, e);
  if (rc == LC_OK && !w.result.skipped &&
      (!maintenance || !w.before.active_segment))
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
