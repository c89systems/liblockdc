#include "lc_collection_multifile.h"

static int unavailable(lc_error *error) {
  return lc_collection_failure(error, LC_ERR_UNSUPPORTED, NULL,
                               "storage owner has no collection maintenance");
}

int lc_collection_maintenance_note(lc_pouch *pouch, const char *ns,
                                   lc_error *error) {
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->hint_ensure(
        pouch->storage.context, ns, LC_POUCH_STORAGE_WORK_COLLECTION, error);
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  if (!pouch->storage.ops)
    return lc_collection_multifile_maintenance_note(pouch, ns, error);
#endif
  return unavailable(error);
}

int lc_collection_maintenance_run(lc_pouch *pouch,
                                  const lc_pouch_maintenance_options *options,
                                  lc_pouch_maintenance_result *out,
                                  lc_error *error) {
  if (!pouch || !options || !options->ns || !*options->ns)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection maintenance requires a root and namespace");
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->collections_run(
        pouch->storage.context, pouch, options, out, error);
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  if (!pouch->storage.ops)
    return lc_collection_multifile_maintenance_run(pouch, options, out, error);
#endif
  return unavailable(error);
}

int lc_collection_maintenance_next(lc_pouch *pouch, const char *after,
                                   char **ns, char **cursor, lc_error *error) {
  *ns = *cursor = NULL;
  if (pouch->storage.ops && pouch->storage.ops->maintenance)
    return pouch->storage.ops->maintenance->hint_next(
        pouch->storage.context, LC_POUCH_STORAGE_WORK_COLLECTION, after, ns,
        cursor, error);
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  if (!pouch->storage.ops)
    return lc_collection_multifile_maintenance_next(pouch, after, ns, cursor,
                                                    error);
#endif
  return unavailable(error);
}
