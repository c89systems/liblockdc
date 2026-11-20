#include "lc_api_internal.h"
#include "namespace_internal.h"
#include <string.h>

struct ps_namespace_conversion {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_reader *verifier;
  ps_key_cursor *cursor;
  lc_pouch_state_entry pending;
  unsigned int scan_kind, cleanup_kind;
  uint64_t scan_id, cleanup_id;
  int phase, has_pending, cleanup_active;
};
static int failure(lc_error *e, int code, const char *text) {
  return lc_error_set(e, code, 0, text, NULL, NULL, "pouch");
}
static int same_key(ps_namespace *ns, lc_pouch_crypto *crypto, lc_error *e) {
  return ps_root_crypto_matches(ns->root, crypto, e);
}
void ps_namespace_conversion_close(ps_namespace_conversion *work) {
  ps_namespace *ns;
  if (!work)
    return;
  ns = work->ns;
  lc_pouch_crypto_reader_close(work->verifier);
  ps_key_cursor_close(work->cursor);
  if (work->data) {
    pthread_mutex_lock(&work->data->mutex);
    if (work->data->conversion == work)
      work->data->conversion = NULL;
    pthread_mutex_unlock(&work->data->mutex);
  }
  lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, &work->pending);
  lc_free_with_allocator(&ns->root->crypto_allocator, work);
  ps_namespace_release(ns);
}
int ps_namespace_conversion_open(ps_namespace *ns, lc_pouch_crypto *crypto,
                                 ps_namespace_conversion **out, lc_error *e) {
  ps_namespace_conversion *work;
  uint64_t closed, pending;
  int rc;
  if (out)
    *out = NULL;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID, "namespace conversion requires output");
  rc = same_key(ns, crypto, e);
  if (rc != LC_OK)
    return rc;
  work =
      lc_calloc_with_allocator(&ns->root->crypto_allocator, 1, sizeof(*work));
  if (!work)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace converter");
  work->ns = ns;
  work->crypto = crypto;
  ps_namespace_retain(ns);
  rc = ps_ns_data_get(ns, &work->data, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&work->data->mutex);
    if (work->data->conversion || work->data->scope != PS_NS_IDLE)
      rc = lc_error_set(e, LC_ERR_INVALID, 0,
                        "namespace conversion already has an owner", NULL,
                        "pouch_namespace_busy", "pouch");
    else
      work->data->conversion = work;
    pthread_mutex_unlock(&work->data->mutex);
  }
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_recover(ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_flush(ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_reap(ns, &closed, &pending, e);
  if (rc == LC_OK)
    rc = ps_ns_data_get(ns, &work->data, e);
  if (rc == LC_OK)
    rc = ps_key_cursor_open(work->data->checkpoint, &work->cursor, e);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_reader_open(crypto, &work->verifier, e);
  if (rc == LC_OK)
    *out = work;
  else
    ps_namespace_conversion_close(work);
  return rc;
}
static int busy(int rc, lc_error *e) {
  if (rc == LC_ERR_INVALID && e->server_code &&
      (!strcmp(e->server_code, "pouch_namespace_busy") ||
       !strcmp(e->server_code, "pouch_piece_needs_validation"))) {
    lc_error_cleanup(e);
    return 1;
  }
  return 0;
}
static int cleanup(ps_namespace_conversion *work, size_t limit,
                   ps_namespace_conversion_result *out, int *done,
                   lc_error *e) {
  ps_namespace_source source;
  ps_namespace_repack_result repack = {0};
  uint64_t closed, pending;
  int found = 0, rc;
  *done = 0;
  rc = ps_ns_source(work->data, work->cleanup_kind, work->cleanup_id, 0,
                    &source, &found, e);
  if (rc != LC_OK)
    return rc;
  if (!found) {
    ++out->maintenance;
    *done = 1;
    return LC_OK;
  }
  if (source.stored_bytes &&
      (!source.sealed ||
       ps_namespace_source_refs(work->ns, source.kind, source.id))) {
    rc = ps_namespace_repack(work->ns, source.kind, source.id, limit, NULL,
                             NULL, &repack, e);
    out->maintenance += repack.visited ? repack.visited : 1;
    if (busy(rc, e)) {
      out->waiting = 1;
      return LC_OK;
    }
    if (rc == LC_OK && repack.finished_tail)
      rc = ps_namespace_checkpoint_flush(work->ns, e);
    if (rc == LC_OK && repack.pending) {
      out->waiting = !repack.visited;
      return LC_OK;
    }
    if (rc != LC_OK)
      return rc;
    rc =
        ps_ns_source(work->data, source.kind, source.id, 0, &source, &found, e);
    if (rc != LC_OK)
      return rc;
  } else
    ++out->maintenance;
  if (found && source.sealed &&
      !ps_namespace_source_refs(work->ns, source.kind, source.id)) {
    rc = ps_namespace_reclaim(work->ns, source.kind, source.id, e);
    if (busy(rc, e)) {
      out->waiting = 1;
      return LC_OK;
    }
    if (rc != LC_OK)
      return rc;
    ++out->reclaimed;
  }
  rc = ps_namespace_reap(work->ns, &closed, &pending, e);
  if (rc == LC_OK)
    *done = 1;
  return rc;
}
static int convert(ps_namespace_conversion *work,
                   ps_namespace_conversion_result *out, lc_error *e) {
  ps_namespace_recode *operation = NULL;
  lc_pouch_state_entry converted = {0};
  lc_pouch_state_entry *entry = &work->pending;
  char *binding = NULL;
  const char *leaf;
  unsigned int output_kind = 0, old_kind;
  uint64_t output_id = 0, old_id;
  ps_stats status;
  int matches = 0, rc;
  if (!entry->found || entry->control || !entry->payload_span.present)
    return LC_OK;
  binding = lc_pouch_state_crypto_context(
      &work->ns->root->crypto_allocator, work->ns->name, entry->key,
      entry->version,
      entry->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
              entry->object_link
          ? 2
          : 1);
  if (!binding)
    return failure(e, LC_ERR_NOMEM, "cannot allocate conversion binding");
  rc = lc_pouch_crypto_descriptor_matches(work->crypto, entry->descriptor,
                                          &matches, e);
  if (rc == LC_OK && matches) {
    rc = lc_pouch_crypto_reader_verify(work->verifier, entry->descriptor,
                                       binding, e);
    if (rc == LC_OK)
      ++out->matching;
    goto done;
  }
  if (rc == LC_OK)
    rc = ps_ns_recode_begin(work->ns, work->crypto, entry, binding, work,
                            &operation, e);
  if (rc == LC_ERR_INVALID && ps_check(work->ns->root->space, NULL) == LC_OK &&
      ps_status(work->ns->root->space, &status, NULL) == LC_OK &&
      status.retired_bytes) {
    lc_error_cleanup(e);
    rc = ps_checkpoint(work->ns->root->space, e);
    if (rc == LC_OK)
      rc = ps_ns_recode_begin(work->ns, work->crypto, entry, binding, work,
                              &operation, e);
  }
  if (rc == LC_OK) {
    leaf = ps_ns_recode_leaf(operation);
    (void)ps_namespace_parse_source_leaf(leaf, &output_kind, &output_id);
    rc = ps_namespace_recode_run(operation, &converted, e);
  }
  ps_namespace_recode_close(operation);
  operation = NULL;
  if (rc == LC_OK) {
    if (!ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                        &old_kind, &old_id))
      rc = failure(e, LC_ERR_PROTOCOL, "converted key has no canonical source");
    else {
      work->cleanup_kind = old_kind;
      work->cleanup_id = old_id;
      work->cleanup_active = 1;
      ++out->converted;
    }
  } else if (output_id) {
    work->cleanup_kind = output_kind;
    work->cleanup_id = output_id;
    work->cleanup_active = 1;
  }
done:
  lc_pouch_state_entry_cleanup(&work->ns->root->crypto_allocator, &converted);
  lc_free_with_allocator(&work->ns->root->crypto_allocator, binding);
  return rc;
}
int ps_namespace_conversion_step(ps_namespace_conversion *work, size_t records,
                                 size_t maintenance,
                                 ps_namespace_conversion_result *out,
                                 lc_error *e) {
  ps_namespace_conversion_result result = {0};
  ps_namespace_source source;
  lc_error local;
  int rc, found, done;
  if (!work || !out || !records || !maintenance)
    return failure(e, LC_ERR_INVALID,
                   "conversion step requires positive bounds");
  lc_error_init(&local);
  if (!e)
    e = &local;
  rc = ps_key_cursor_check(work->cursor, e);
  while (rc == LC_OK && work->phase < 3) {
    if (work->cleanup_active) {
      if (result.maintenance == maintenance)
        break;
      rc = cleanup(work, maintenance - (size_t)result.maintenance, &result,
                   &done, e);
      if (rc != LC_OK || result.waiting)
        break;
      if (done) {
        work->cleanup_active = 0;
        if (work->phase != 1) {
          work->scan_kind = work->cleanup_kind;
          work->scan_id = work->cleanup_id;
        }
      }
      continue;
    }
    if (work->phase != 1) {
      if (result.maintenance == maintenance)
        break;
      found = 0;
      rc = ps_ns_source(work->data, work->scan_kind, work->scan_id, 1, &source,
                        &found, e);
      if (rc == LC_OK && found) {
        work->cleanup_kind = source.kind;
        work->cleanup_id = source.id;
        work->cleanup_active = 1;
      } else if (rc == LC_OK) {
        ++work->phase;
        work->scan_kind = 0;
        work->scan_id = 0;
      }
      continue;
    }
    if (result.visited == records)
      break;
    if (!work->has_pending) {
      found = 0;
      rc = ps_key_cursor_next(work->cursor, &work->pending, &found, e);
      if (rc != LC_OK)
        break;
      if (!found) {
        work->phase = 2;
        continue;
      }
      work->has_pending = 1;
    }
    rc = convert(work, &result, e);
    if (rc == LC_OK) {
      ++result.visited;
      work->has_pending = 0;
      lc_pouch_state_entry_cleanup(&work->ns->root->crypto_allocator,
                                   &work->pending);
    }
  }
  if (rc == LC_OK) {
    rc = ps_key_cursor_check(work->cursor, e);
    result.finished = work->phase == 3;
  }
  if (rc == LC_OK)
    *out = result;
  lc_error_cleanup(&local);
  return rc;
}
