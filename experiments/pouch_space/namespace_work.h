#ifndef POUCH_SPACE_NAMESPACE_WORK_H
#define POUCH_SPACE_NAMESPACE_WORK_H
#include "lc_pouch_storage.h"
#include "root.h"
/* Slots 4..6 are durable metadata hints, independent of canonical outcomes.
 * Ensure precedes the operation that can create maintenance work. Discovery
 * never admits segment owners or reads application bodies. */
int ps_namespace_work_ensure(ps_namespace *, enum lc_pouch_storage_work_kind,
                             lc_error *);
int ps_namespace_work_get(ps_namespace *, enum lc_pouch_storage_work_kind,
                          int *, lc_error *);
int ps_namespace_work_remove(ps_namespace *, enum lc_pouch_storage_work_kind,
                             lc_error *);
int ps_namespace_work_next(ps_root *, const lc_allocator *,
                           enum lc_pouch_storage_work_kind, const char *,
                           char **, char **, lc_error *);
#endif
