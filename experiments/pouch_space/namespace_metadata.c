#include "namespace_metadata.h"
#include "lc_api_internal.h"
#include "namespace_internal.h"

struct ps_namespace_metadata {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_checkpoint_delta *delta;
};
int ps_namespace_metadata_begin(ps_namespace *ns, int maintenance,
                                const lc_pouch_storage_batch *batch,
                                uint64_t bytes, ps_namespace_metadata **out,
                                lc_error *e) {
  ps_namespace_metadata *metadata;
  struct ps_namespace_data *d;
  int rc;
  if (!out)
    return lc_error_set(e, LC_ERR_INVALID, 0, "native metadata requires output",
                        NULL, NULL, "pouch");
  *out = NULL;
  rc = ps_namespace_checkpoint_recover(ns, e);
  if (rc == LC_OK)
    rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  metadata = lc_calloc_with_allocator(NULL, 1, sizeof(*metadata));
  if (!metadata)
    return lc_error_set(e, LC_ERR_NOMEM, 0,
                        "cannot allocate native metadata owner", NULL, NULL,
                        "pouch");
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, metadata);
    return rc;
  }
  rc = ps_ns_finish_seal(d, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    rc = ps_key_checkpoint_flush_deferred(d->checkpoint, e);
    pthread_mutex_unlock(&d->mutex);
  }
  if (rc == LC_OK)
    rc = ps_key_metadata_begin(d->checkpoint, maintenance, batch, bytes,
                               &metadata->delta, e);
  if (rc == LC_OK) {
    metadata->ns = ns;
    metadata->data = d;
    ps_namespace_retain(ns);
    *out = metadata;
  } else {
    ps_ns_leave(d);
    lc_free_with_allocator(NULL, metadata);
  }
  return rc;
}
int ps_namespace_metadata_commit(ps_namespace_metadata *metadata,
                                 const lc_pouch_storage_batch *batch,
                                 enum ps_commit_mode mode, lc_error *e) {
  if (!metadata)
    return lc_error_set(e, LC_ERR_INVALID, 0, "native metadata is not open",
                        NULL, NULL, "pouch");
  return ps_key_metadata_commit(metadata->delta, batch, mode, e);
}
void ps_namespace_metadata_close(ps_namespace_metadata *metadata) {
  if (!metadata)
    return;
  ps_checkpoint_delta_close(metadata->delta);
  ps_ns_leave(metadata->data);
  ps_namespace_release(metadata->ns);
  lc_free_with_allocator(NULL, metadata);
}
