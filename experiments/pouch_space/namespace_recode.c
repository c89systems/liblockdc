#include "lc_api_internal.h"
#include "namespace_internal.h"
#include <string.h>

struct ps_namespace_recode {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  lc_pouch_crypto *crypto;
  ps_checkpoint_body *metadata;
  ps_budget *budget;
  ps_namespace_body *body;
  char *binding;
  int entered, attempted;
};
static int recode_enter(struct ps_namespace_data *data, const void *owner,
                        lc_error *e) {
  int rc = LC_OK;
  pthread_mutex_lock(&data->mutex);
  if (data->busy || (data->conversion && data->conversion != owner))
    rc = lc_error_set(e, LC_ERR_INVALID, 0,
                      "namespace conversion already has an owner or producer",
                      NULL, "pouch_namespace_busy", "pouch");
  else
    data->busy = 1;
  pthread_mutex_unlock(&data->mutex);
  return rc;
}
void ps_namespace_recode_close(ps_namespace_recode *work) {
  ps_namespace *ns;
  if (!work)
    return;
  ns = work->ns;
  ps_namespace_body_close(work->body);
  ps_key_body_close(work->metadata);
  (void)ps_budget_close(work->budget, NULL);
  if (work->entered)
    ps_ns_leave(work->data);
  lc_free_with_allocator(&ns->root->crypto_allocator, work->binding);
  lc_free_with_allocator(&ns->root->crypto_allocator, work);
  ps_namespace_release(ns);
}
const char *ps_ns_recode_leaf(const ps_namespace_recode *work) {
  return work ? ps_namespace_body_leaf(work->body) : NULL;
}
int ps_ns_recode_begin(ps_namespace *ns, lc_pouch_crypto *crypto,
                       const lc_pouch_state_entry *expected,
                       const char *binding, const void *owner,
                       ps_namespace_recode **out, lc_error *e) {
  ps_namespace_recode *work = NULL;
  lc_pouch_state_recode_bounds bounds;
  ps_budget_plan plan = {0}, metadata;
  int rc;
  if (out)
    *out = NULL;
  if (!ns || !expected || !binding || !*binding || !out)
    return lc_error_set(
        e, LC_ERR_INVALID, 0,
        "conversion admission requires source, binding and output", NULL, NULL,
        "pouch");
  work =
      lc_calloc_with_allocator(&ns->root->crypto_allocator, 1, sizeof(*work));
  if (!work)
    return lc_error_set(e, LC_ERR_NOMEM, 0,
                        "cannot allocate namespace conversion", NULL, NULL,
                        "pouch");
  work->ns = ns;
  work->crypto = crypto;
  ps_namespace_retain(ns);
  work->binding =
      lc_strdup_with_allocator(&ns->root->crypto_allocator, binding);
  rc = work->binding ? LC_OK
                     : lc_error_set(e, LC_ERR_NOMEM, 0,
                                    "cannot retain conversion binding", NULL,
                                    NULL, "pouch");
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_recover(ns, e);
  if (rc == LC_OK)
    rc = ps_ns_data_get(ns, &work->data, e);
  if (rc == LC_OK) {
    rc = recode_enter(work->data, owner, e);
    work->entered = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_ns_finish_seal(work->data, e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_flush(work->data->checkpoint, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_recode_bound(&ns->root->crypto_allocator, crypto,
                                            "body-00000000000000000001.log",
                                            expected, &bounds, e);
  if (rc == LC_OK)
    rc = ps_key_body_plan(work->data->checkpoint, expected,
                          bounds.metadata_bytes, &work->metadata, &metadata, e);
  if (rc == LC_OK)
    rc = ps_ns_body_plan(&plan, bounds.record_bytes, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_add(&plan, &metadata, e);
  if (rc == LC_OK)
    rc = ps_budget_open(ns->root->space, &plan, &work->budget, e);
  if (rc == LC_OK)
    rc = ps_key_body_fund(work->metadata, work->budget, e);
  if (rc == LC_OK)
    rc =
        ps_ns_body_begin(ns, work->budget, bounds.record_bytes, &work->body, e);
  if (rc == LC_OK)
    *out = work;
  else
    ps_namespace_recode_close(work);
  return rc;
}
int ps_namespace_recode_begin(ps_namespace *ns, lc_pouch_crypto *crypto,
                              const lc_pouch_state_entry *expected,
                              const char *binding, ps_namespace_recode **out,
                              lc_error *e) {
  return ps_ns_recode_begin(ns, crypto, expected, binding, NULL, out, e);
}
int ps_namespace_recode_run(ps_namespace_recode *work,
                            lc_pouch_state_entry *result, lc_error *e) {
  ps_namespace_window *input = NULL;
  lc_pouch_state_entry converted = {0};
  const lc_pouch_state_entry *source;
  ps_namespace *ns;
  unsigned int kind;
  uint64_t id;
  lc_error local;
  int rc;
  if (!work || work->attempted || !result)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "namespace conversion is not open", NULL, NULL,
                        "pouch");
  lc_error_init(&local);
  if (!e)
    e = &local;
  work->attempted = 1;
  ns = work->ns;
  source = ps_key_body_source(work->metadata);
  rc = ps_namespace_parse_source_leaf(source->payload_span.container_leaf,
                                      &kind, &id)
           ? LC_OK
           : lc_error_set(e, LC_ERR_INVALID, 0,
                          "conversion source has no canonical owner", NULL,
                          NULL, "pouch");
  if (rc == LC_OK)
    rc = ps_namespace_read_open(ns, kind, id,
                                source->payload_span.payload_offset,
                                source->cipher_bytes, &input, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_recode(&ns->root->crypto_allocator, work->crypto,
                                      ps_namespace_read_input(input),
                                      ps_namespace_body_output(work->body),
                                      ps_namespace_body_leaf(work->body),
                                      source, work->binding, &converted, e);
  if (rc == LC_OK)
    rc = ps_ns_body_commit(work->body, e);
  if (rc == LC_OK)
    rc = ps_key_body_commit(work->metadata, &converted, e);
  if (rc == LC_OK) {
    lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, result);
    *result = converted;
  } else
    lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, &converted);
  ps_namespace_read_close(input);
  lc_error_cleanup(&local);
  return rc;
}

int ps_namespace_body_recode(ps_namespace *ns, lc_pouch_crypto *crypto,
                             const lc_pouch_state_entry *entry,
                             const char *binding, lc_pouch_state_entry *result,
                             lc_error *e) {
  ps_namespace_window *input = NULL;
  ps_namespace_body *output = NULL;
  lc_pouch_state_entry converted = {0};
  unsigned int kind;
  uint64_t id;
  lc_pouch_state_recode_bounds bounds;
  int rc;
  if (!ns || !entry || !result || entry == result || !binding || !*binding ||
      !ps_namespace_parse_source_leaf(entry->payload_span.container_leaf, &kind,
                                      &id))
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "body conversion requires namespace, source and result",
                        NULL, NULL, "pouch");
  /* Names have a fixed-width physical ID, so admission needs no source label
   * allocation or body read. Actual output metadata uses the allocated ID. */
  rc = lc_pouch_state_record_recode_bound(&ns->root->crypto_allocator, crypto,
                                          "body-00000000000000000001.log",
                                          entry, &bounds, e);
  if (rc == LC_OK)
    rc = ps_namespace_body_begin_bound(ns, bounds.record_bytes, &output, e);
  if (rc == LC_OK)
    rc =
        ps_namespace_read_open(ns, kind, id, entry->payload_span.payload_offset,
                               entry->cipher_bytes, &input, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_recode(
        &ns->root->crypto_allocator, crypto, ps_namespace_read_input(input),
        ps_namespace_body_output(output), ps_namespace_body_leaf(output), entry,
        binding, &converted, e);
  if (rc == LC_OK)
    rc = ps_namespace_body_commit(output, e);
  if (rc == LC_OK) {
    lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, result);
    *result = converted;
  } else
    lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, &converted);
  ps_namespace_read_close(input);
  ps_namespace_body_close(output);
  return rc;
}
static int prepared_body(ps_namespace *ns,
                         const lc_pouch_state_entry *converted,
                         ps_namespace_window **window, lc_error *e) {
  lc_pouch_state_entry parsed = {0};
  ps_checkpoint_record actual = {0}, claimed = {0};
  unsigned int kind;
  uint64_t id, end, next = 0;
  int rc, found = 0, truncated = 0;
  const lc_pouch_state_payload_span *span = &converted->payload_span;
  if (!span->present ||
      !ps_namespace_parse_source_leaf(span->container_leaf, &kind, &id) ||
      kind != PS_CATALOG_BODY || span->payload_offset < span->record_offset ||
      span->payload_length != converted->cipher_bytes ||
      span->payload_length > (uint64_t)INT64_MAX ||
      span->payload_offset > (uint64_t)INT64_MAX - span->payload_length)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "physical handoff requires a final namespace body",
                        NULL, NULL, "pouch");
  end = span->payload_offset + span->payload_length;
  rc = ps_namespace_read_open(ns, kind, id, span->record_offset,
                              end - span->record_offset, window, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_read(
        ps_namespace_read_input(*window), &ns->root->crypto_allocator,
        span->container_leaf, span->record_offset, end, 0, 0, NULL, &parsed,
        &next, &found, &truncated, e);
  if (rc == LC_OK && (!found || truncated || next != end ||
                      parsed.record_type != converted->record_type))
    rc = lc_error_set(e, LC_ERR_INVALID, 0,
                      "physical handoff has no complete converted record", NULL,
                      NULL, "pouch");
  if (rc == LC_OK)
    rc = ps_checkpoint_record_encode(&ns->root->crypto_allocator, &parsed,
                                     &actual, e);
  if (rc == LC_OK)
    rc = ps_checkpoint_record_encode(&ns->root->crypto_allocator, converted,
                                     &claimed, e);
  if (rc == LC_OK && (actual.length != claimed.length ||
                      memcmp(actual.bytes, claimed.bytes, actual.length)))
    rc = lc_error_set(e, LC_ERR_INVALID, 0,
                      "physical handoff differs from its sealed record", NULL,
                      NULL, "pouch");
  ps_checkpoint_record_close(&claimed);
  ps_checkpoint_record_close(&actual);
  lc_pouch_state_entry_cleanup(&ns->root->crypto_allocator, &parsed);
  return rc;
}
int ps_namespace_body_select(ps_namespace *ns,
                             const lc_pouch_state_entry *expected,
                             const lc_pouch_state_entry *converted,
                             lc_error *e) {
  struct ps_namespace_data *data = NULL;
  ps_namespace_window *window = NULL;
  lc_error local;
  int rc, entered = 0;
  lc_error_init(&local);
  if (!e)
    e = &local;
  if (!ns || !expected || !converted) {
    rc = lc_error_set(e, LC_ERR_INVALID, 0,
                      "physical handoff requires namespace and both records",
                      NULL, NULL, "pouch");
    goto done;
  }
  rc = ps_namespace_checkpoint_recover(ns, e);
  if (rc == LC_OK)
    rc = ps_ns_data_get(ns, &data, e);
  if (rc == LC_OK) {
    rc = recode_enter(data, NULL, e);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_ns_finish_seal(data, e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_flush(data->checkpoint, e);
  if (rc == LC_OK)
    rc = prepared_body(ns, converted, &window, e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_replace_body(data->checkpoint, expected, converted,
                                        e);
done:
  ps_namespace_read_close(window);
  if (entered)
    ps_ns_leave(data);
  lc_error_cleanup(&local);
  return rc;
}
