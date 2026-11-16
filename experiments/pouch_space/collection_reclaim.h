#ifndef POUCH_SPACE_COLLECTION_RECLAIM_H
#define POUCH_SPACE_COLLECTION_RECLAIM_H
#include "catalog.h"
typedef struct ps_collection_resource {
  unsigned char name[16];
  size_t length;
  unsigned int kind;
  uint64_t id, marked;
  ps_catalog_info binding;
} ps_collection_resource;
typedef struct ps_collection_resources {
  uint64_t owner;
  unsigned int kind;
  ps_collection_resource *items;
  size_t count, capacity;
} ps_collection_resources;
/* Capture metadata only; all catalog views close before target retirement. */
int ps_collection_resources_capture(ps_space *, uint64_t,
                                    ps_collection_resources *, lc_error *);
void ps_collection_resources_clear(ps_collection_resources *);
/* Remove canonical bindings before their grace aliases. Exact revisions
 * prevent a captured pass from deleting a different incarnation. */
int ps_collection_resources_remove(ps_space *, ps_collection_resources *,
                                   uint64_t, int markers_only, lc_error *);
#endif
