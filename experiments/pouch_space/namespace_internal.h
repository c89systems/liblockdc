#ifndef POUCH_SPACE_NAMESPACE_INTERNAL_H
#define POUCH_SPACE_NAMESPACE_INTERNAL_H
#include "budget.h"
#include "checkpoint.h"
#include "namespace.h"
#include "record_io.h"
#include "root_internal.h"

#define PS_NS_LEAF_BYTES 34U
typedef struct ps_ns_label {
  struct ps_ns_label *next;
  char value[PS_NS_LEAF_BYTES];
} ps_ns_label;
typedef struct ps_ns_resource {
  uint64_t id, covered, reclaim_cursor;
  unsigned int kind;
  const char *leaf;
  ps_ns_label *label;
  ps_catalog_info binding;
  ps_stream *stream;
  int unselected, pooled, repacking;
  struct ps_ns_resource *next;
} ps_ns_resource;
struct ps_namespace_data {
  ps_namespace *ns;
  pthread_mutex_t mutex;
  ps_ns_resource **items, *retired;
  ps_ns_label *labels;
  size_t count, capacity, views;
  uint64_t active, tail, pending_seal;
  ps_key_checkpoint *checkpoint;
  ps_checkpoint_observer checkpoint_observer;
  int busy, checkpoint_ready;
  int retention_loaded, retention_present;
  ps_budget *retention_budget;
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

/* Mutation authority never spans an application producer while holding a mutex.
 */
int ps_ns_data_get(ps_namespace *, struct ps_namespace_data **, lc_error *);
int ps_ns_retention_restore(struct ps_namespace_data *, lc_error *);
int ps_ns_enter(struct ps_namespace_data *, lc_error *);
void ps_ns_leave(struct ps_namespace_data *);
int ps_ns_finish_seal(struct ps_namespace_data *, lc_error *);
int ps_ns_rotation_plan(struct ps_namespace_data *, ps_budget_plan *,
                        lc_error *);
int ps_ns_rotate(struct ps_namespace_data *, ps_budget *, lc_error *);
int ps_ns_head_write(struct ps_namespace_data *, ps_budget *, uint64_t active,
                     lc_error *);
/* Funded body construction while the conversion owner holds mutation
 * exclusion. The borrowed budget outlives the body builder. */
int ps_ns_body_plan(ps_budget_plan *, uint64_t, lc_error *);
int ps_ns_body_begin(ps_namespace *, ps_budget *, uint64_t,
                     ps_namespace_body **, lc_error *);
int ps_ns_body_commit(ps_namespace_body *, lc_error *);

#endif
