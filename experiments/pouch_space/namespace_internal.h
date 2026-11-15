#ifndef POUCH_SPACE_NAMESPACE_INTERNAL_H
#define POUCH_SPACE_NAMESPACE_INTERNAL_H
#include "budget.h"
#include "namespace.h"
#include "record_io.h"
#include "root_internal.h"

#define PS_NS_LEAF_BYTES 34U
typedef struct ps_ns_label {
  struct ps_ns_label *next;
  char value[PS_NS_LEAF_BYTES];
} ps_ns_label;
typedef struct ps_ns_resource {
  uint64_t id;
  unsigned int kind;
  const char *leaf;
  ps_ns_label *label;
  ps_catalog_info binding;
  ps_stream *stream;
  int unselected, pooled;
  struct ps_ns_resource *next;
} ps_ns_resource;
struct ps_namespace_data {
  ps_namespace *ns;
  pthread_mutex_t mutex;
  ps_ns_resource **items, *retired;
  ps_ns_label *labels;
  size_t count, capacity;
  uint64_t active, tail, pending_seal;
  uint64_t snapshot_id, snapshot_boundary;
  int busy, snapshot_building;
  int retention_loaded, retention_present;
};

/* Index helpers run under the namespace mutex (or before owner publication).
 * Resource addresses and interned labels survive vector growth. Labels retain
 * the existing namespace-capability lifetime, including after source removal.
 */
ps_ns_resource *ps_ns_resource_new(unsigned int, uint64_t, lc_error *);
void ps_ns_resource_free(ps_ns_resource *);
int ps_ns_index_room(struct ps_namespace_data *, lc_error *);
ps_ns_resource *ps_ns_find(struct ps_namespace_data *, unsigned int, uint64_t);
void ps_ns_insert(struct ps_namespace_data *, ps_ns_resource *);
void ps_ns_remove(struct ps_namespace_data *, ps_ns_resource *);

/* Mutation authority is brief and never spans a snapshot's producer. */
int ps_ns_data_get(ps_namespace *, struct ps_namespace_data **, lc_error *);
int ps_ns_enter(struct ps_namespace_data *, lc_error *);
void ps_ns_leave(struct ps_namespace_data *);
int ps_ns_finish_seal(struct ps_namespace_data *, lc_error *);
int ps_ns_rotation_plan(struct ps_namespace_data *, ps_budget_plan *,
                        lc_error *);
int ps_ns_rotate(struct ps_namespace_data *, ps_budget *, lc_error *);
int ps_ns_head_write(struct ps_namespace_data *, ps_budget *, uint64_t active,
                     uint64_t snapshot, uint64_t boundary, lc_error *);

#endif
