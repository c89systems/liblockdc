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
  size_t references;
  int unselected, pooled, repacking, retiring;
  struct ps_ns_resource *next, *repack_next;
} ps_ns_resource;
enum ps_namespace_scope {
  PS_NS_IDLE,
  PS_NS_OBSERVING,
  PS_NS_EDIT_PENDING,
  PS_NS_EDITING
};
struct ps_namespace_data {
  ps_namespace *ns;
  pthread_mutex_t mutex;
  pthread_cond_t scope_changed;
  ps_ns_resource **items, *retired, *repacking;
  ps_ns_label *labels;
  size_t count, capacity, views;
  uint64_t active, tail, pending_seal;
  uint64_t replay_segment, replay_offset;
  /* Decoded fixed controls, keyed by selected immutable incarnations. */
  uint64_t observed_head, observed_active;
  ps_checkpoint_info observed_coverage;
  ps_key_checkpoint *checkpoint;
  ps_checkpoint_observer checkpoint_observer;
  ps_coordination_guard *edit;
  enum ps_namespace_scope scope;
  int checkpoint_ready;
  int shared_valid;
  int retention_loaded, retention_present;
  ps_budget *retention_budget;
  const void *conversion;
};

/* Index helpers run under the namespace mutex (or before owner publication).
 * Resource addresses and interned labels survive vector growth. Labels retain
 * the existing namespace-capability lifetime, including after source removal.
 */
ps_ns_resource *ps_ns_resource_new(unsigned int, uint64_t, lc_error *);
void ps_ns_resource_retain(ps_ns_resource *);
void ps_ns_resource_release(ps_ns_resource *);
void ps_ns_resource_park(struct ps_namespace_data *, ps_ns_resource *);
int ps_ns_index_room(struct ps_namespace_data *, lc_error *);
ps_ns_resource *ps_ns_find(struct ps_namespace_data *, unsigned int, uint64_t);
int ps_ns_source(struct ps_namespace_data *, unsigned int, uint64_t, int after,
                 ps_namespace_source *, int *, lc_error *);
void ps_ns_insert(struct ps_namespace_data *, ps_ns_resource *);
void ps_ns_remove(struct ps_namespace_data *, ps_ns_resource *);

/* Mutation authority never spans an application producer while holding a mutex.
 */
int ps_ns_data_get(ps_namespace *, struct ps_namespace_data **, lc_error *);
int ps_ns_data_lookup(ps_namespace *, struct ps_namespace_data **, lc_error *);
int ps_ns_retention_restore(struct ps_namespace_data *, lc_error *);
/* Caller holds the allocator capture. A selected or pending workspace owner
 * requires ordinary namespace recovery. */
int ps_ns_retention_recovery_needed(ps_space *, uint64_t owner);
int ps_ns_enter(struct ps_namespace_data *, lc_error *);
int ps_ns_enter_checkpoint(ps_namespace *, struct ps_namespace_data **,
                           lc_error *);
void ps_ns_leave(struct ps_namespace_data *);
/* A final source retirement or mapping selection holds both namespace
 * mutation authority and the data mutex across this check and publication. */
int ps_ns_require_unpinned(struct ps_namespace_data *, lc_error *);
int ps_ns_finish_seal(struct ps_namespace_data *, lc_error *);
int ps_ns_rotation_plan(struct ps_namespace_data *, ps_budget_plan *,
                        lc_error *);
int ps_ns_rotate(struct ps_namespace_data *, ps_budget *, lc_error *);
/* Select a durable empty successor and record its predecessor for sealing
 * or direct retirement. Caller has finished any earlier pending seal. */
int ps_ns_select_successor(struct ps_namespace_data *, ps_budget *, lc_error *);
int ps_ns_head_write(struct ps_namespace_data *, ps_budget *, uint64_t active,
                     lc_error *);
/* Funded body construction while the conversion owner holds mutation
 * exclusion. The borrowed budget outlives the body builder. */
int ps_ns_body_plan(ps_budget_plan *, uint64_t, lc_error *);
int ps_ns_body_begin(ps_namespace *, ps_budget *, uint64_t,
                     ps_namespace_body **, lc_error *);
int ps_ns_body_commit(ps_namespace_body *, lc_error *);
/* Borrowed output identity for cleanup after a failed conversion handoff. */
const char *ps_ns_recode_leaf(const ps_namespace_recode *);
int ps_ns_recode_begin(ps_namespace *, lc_pouch_crypto *,
                       const lc_pouch_state_entry *, const char *, const void *,
                       ps_namespace_recode **, lc_error *);

#endif
