#include "lc_api_internal.h"
#include "namespace_internal.h"
#include <string.h>

int ps_namespace_body_recode(ps_namespace *ns, lc_pouch_crypto *crypto,
                             const lc_pouch_state_entry *entry,
                             const char *binding, lc_pouch_state_entry *result,
                             lc_error *e) {
  ps_namespace_window *input = NULL;
  ps_namespace_body *output = NULL;
  lc_pouch_state_entry converted = {0};
  unsigned int kind;
  uint64_t id, bound;
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
                                          entry, &bound, e);
  if (rc == LC_OK)
    rc = ps_namespace_body_begin_bound(ns, bound, &output, e);
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
    rc = ps_ns_enter(data, e);
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
