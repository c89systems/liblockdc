#include "lc_api_internal.h"
#include "namespace.h"
#include "root_internal.h"

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
