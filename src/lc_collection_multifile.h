#ifndef LOCKDC_COLLECTION_MULTIFILE_H
#define LOCKDC_COLLECTION_MULTIFILE_H

#include "lc_collection_internal.h"

int lc_collection_multifile_catalog_open(lc_client_handle *, const char *,
                                         const lc_collection_config *, int,
                                         lc_collection **, lc_error *);
int lc_collection_multifile_catalog_visit(lc_client_handle *, const char *,
                                          lc_collection_visit_fn, void *,
                                          lc_error *);
int lc_collection_multifile_maintenance_note(lc_pouch *, const char *,
                                             lc_error *);
int lc_collection_multifile_maintenance_run(
    lc_pouch *, const lc_pouch_maintenance_options *,
    lc_pouch_maintenance_result *, lc_error *);
int lc_collection_multifile_maintenance_next(lc_pouch *, const char *, char **,
                                             char **, lc_error *);

#endif
