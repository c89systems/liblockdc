#ifndef POUCH_SPACE_MIGRATION_H
#define POUCH_SPACE_MIGRATION_H
#include "pouch.h"

#define PS_MIGRATION_RECEIPT ".pouch-space-migration"

/* Verify ownership before moving or reclaiming a legacy directory. */
int ps_pouch_migration_inventory(const char *, const lc_allocator *,
                                 lc_error *);

/* Normal path admission for container roots and same-path legacy migration. */
int ps_pouch_open_managed(const char *, const lc_allocator *,
                          const lc_pouch_open_options *, int disable_migration,
                          lc_pouch **, lc_error *);
#ifdef LOCKDC_TEST_BUILD
extern void (*ps_migration_test_hook)(const char *phase);
#endif
#endif
