#include "lc_collection_query.h"

int lc_collection_query_cleanup(lc_collection *self,
                                lc_pouch_maintenance_result *out,
                                lc_error *error) {
  lc_collection_call call, scope;
  lc_collection_guard guard;
  lc_collection_handle *c;
  int rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  c = call.collection;
  rc = lc_collection_root_begin(c->pouch, &scope, &guard, call.error);
  if (rc == LC_OK) {
    rc = c->queries.cleanup(c->queries.context, out, call.error);
    lc_collection_root_end(&scope, &guard);
  }
  lc_collection_call_end(&call);
  return rc;
}
