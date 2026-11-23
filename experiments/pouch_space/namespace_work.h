#ifndef POUCH_SPACE_NAMESPACE_WORK_H
#define POUCH_SPACE_NAMESPACE_WORK_H
#include "lc_pouch_storage.h"
#include "root.h"
/* Slots 4..6 are durable metadata hints, independent of canonical outcomes.
 * Ensure precedes the operation that can create maintenance work. Discovery
 * never admits segment owners or reads application bodies. maintenance permits
 * mandatory bounded creation to spend the computed floor; ordinary creation
 * preserves it. The admission class does not change the hint identity. */
int ps_namespace_work_ensure(ps_namespace *, enum lc_pouch_storage_work_kind,
                             int maintenance, lc_error *);
/* Prepare an ordinary hint before append setup. Setup may synchronize its
 * publication while binding a stream. Complete before exposing a producer. */
int ps_namespace_work_stage_ordinary(ps_namespace *, int maintenance,
                                     int *pending, lc_error *);
int ps_namespace_work_complete_ordinary(ps_namespace *, lc_error *);
int ps_namespace_work_get(ps_namespace *, enum lc_pouch_storage_work_kind,
                          int *, lc_error *);
int ps_namespace_work_remove(ps_namespace *, enum lc_pouch_storage_work_kind,
                             lc_error *);
int ps_namespace_work_next(ps_root *, const lc_allocator *,
                           enum lc_pouch_storage_work_kind, const char *,
                           char **, char **, lc_error *);
#endif
