#include "collections.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_outbox_store.h"
#include "namespace.h"
#include "pouch.h"
#include "pouch_internal.h"

typedef struct state_import {
  lc_pouch *source;
  lc_pouch *target;
  ps_namespace *destination;
  int preserve_history;
} state_import;

static int import_entry(void *context, const lc_pouch_state_entry *entry,
                        lc_pouch_record_io input, const char *binding,
                        lc_error *e) {
  static const char segment_leaf[] = "seg-00000000000000000001.log";
  state_import *work = context;
  const lc_allocator *allocator = &work->target->allocator;
  lc_pouch_crypto *crypto = work->target->crypto;
  ps_namespace_append *append = NULL;
  lc_pouch_state_entry converted = {0};
  uint64_t bytes;
  int rc;

  if (binding) {
    lc_pouch_state_recode_bounds bounds;
    rc = lc_pouch_state_record_recode_bound(allocator, crypto, segment_leaf,
                                            entry, &bounds, e);
    bytes = rc == LC_OK ? bounds.record_bytes : 0;
  } else {
    rc = lc_pouch_state_record_measure_snapshot(allocator, segment_leaf, entry,
                                                0, &bytes, e);
  }
  if (rc == LC_OK)
    rc = ps_migration_fund(ps_root_space(ps_pouch_state_root(work->target)),
                           bytes, e);
  if (rc == LC_OK && entry->found && entry->retention)
    rc = ps_namespace_retention_hint_ensure(work->destination, e);
  if (rc == LC_OK)
    rc = ps_namespace_append_begin(work->destination,
                                   work->target->segment_target_bytes, bytes,
                                   PS_APPEND_FRAMED_PREFIX, NULL, &append, e);
  if (rc == LC_OK) {
    lc_pouch_record_io output = ps_namespace_append_output(append);
    const char *leaf = ps_namespace_append_leaf(append);

    if (binding && work->preserve_history)
      rc = lc_pouch_state_record_import_history(allocator, crypto, input,
                                                output, leaf, entry, binding,
                                                &converted, e);
    else if (binding)
      rc = lc_pouch_state_record_recode(allocator, crypto, input, output, leaf,
                                        entry, binding, &converted, e);
    else
      rc = lc_pouch_state_record_write_snapshot(allocator, output, leaf, entry,
                                                NULL, NULL, 0, e);
  }
  if (rc == LC_OK)
    rc = ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, e);
  lc_pouch_state_entry_cleanup(allocator, &converted);
  ps_namespace_append_close(append);
  return rc;
}

static int import_namespace(void *context, const char *ns, lc_error *e) {
  state_import *work = context;
  lc_pouch_retention_work retention = {0};
  lc_pouch_generation oldest;
  int rc = lc_pouch_history_oldest_acknowledged(
      work->source, ns, &work->preserve_history, &oldest, e);
  if (rc == LC_OK)
    rc = ps_root_namespace_acquire(ps_pouch_state_root(work->target), ns, 1,
                                   &work->destination, e);
  if (rc == LC_OK && work->preserve_history)
    rc = lc_pouch_history_import(work->source, work->target, ns, e);
  if (rc == LC_OK)
    rc = lc_pouch_legacy_state_visit(work->source, ns, work->preserve_history,
                                     import_entry, work, e);
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_flush(work->destination, e);
  if (rc == LC_OK)
    rc = lc_pouch_retention_work_read(work->source, ns, &retention, e);
  if (rc == LC_OK && retention.present)
    rc = ps_namespace_retention_hint_ensure(work->destination, e);
  if (rc == LC_OK && retention.phase != LC_POUCH_RETENTION_IDLE)
    rc = ps_namespace_retention_work_store(work->destination, &retention, e);
  lc_pouch_retention_work_cleanup(work->source, &retention);
  if (rc == LC_OK)
    rc = lc_pouch_outbox_markers_import(work->source, work->target, ns, e);
  if (rc == LC_OK)
    rc = ps_pouch_import_legacy_collections(work->source, work->target,
                                            work->destination, ns, e);
  if (work->destination) {
    ps_namespace_release(work->destination);
    work->destination = NULL;
  }
  return rc;
}

int ps_pouch_import_legacy(lc_pouch *source, lc_pouch *target, lc_error *e) {
  state_import work;
  if (!source || source->storage.ops || !source->single_writer ||
      !ps_pouch_state_root(target) || !target->single_writer)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "import requires exclusively owned legacy and "
                        "container roots",
                        NULL, NULL, "pouch");
  work.source = source;
  work.target = target;
  work.destination = NULL;
  return lc_pouch_storage_namespaces_visit(source, import_namespace, &work, e);
}
