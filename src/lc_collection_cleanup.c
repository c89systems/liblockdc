#include "lc_collection_internal.h"

int lc_collection_cleanup_files(lc_collection_handle *collection,
                                lc_pouch_maintenance_result *out,
                                lc_error *error) {
  return collection->layout.cleanup(collection->layout.context, out, error);
}
