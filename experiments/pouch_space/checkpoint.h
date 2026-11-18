#ifndef POUCH_SPACE_CHECKPOINT_H
#define POUCH_SPACE_CHECKPOINT_H
#include "budget.h"
#include "checkpoint_record.h"
#include "lc_pouch_storage.h"
#include "namespace.h"
#include "record_io.h"

typedef struct ps_key_checkpoint ps_key_checkpoint;
typedef struct ps_checkpoint_delta ps_checkpoint_delta;
typedef struct ps_checkpoint_body ps_checkpoint_body;
typedef struct ps_checkpoint_info {
  uint64_t epoch, segment, offset, state_index, query_index;
  uint64_t slots, pending, pending_bytes;
  uint64_t
      control; /* Captured physical incarnation, never a logical generation. */
} ps_checkpoint_info;
typedef lc_pouch_storage_checkpoint_ref ps_checkpoint_ref;
typedef struct ps_checkpoint_observer {
  int (*prepare)(void *, const char *, const ps_checkpoint_ref *, size_t,
                 void **, lc_error *);
  void (*finish)(void *, void *, int published);
  void *context;
} ps_checkpoint_observer;

/* Owned by one namespace. Its caller serializes canonical appends and flushes;
 * captures/read admission may run concurrently. No mutex spans body input. */
int ps_key_checkpoint_open(ps_namespace *, ps_key_checkpoint **, lc_error *);
void ps_key_checkpoint_close(ps_key_checkpoint *);
int ps_key_checkpoint_observe(ps_key_checkpoint *,
                              const ps_checkpoint_observer *, lc_error *);
void ps_key_checkpoint_info(ps_key_checkpoint *, ps_checkpoint_info *);
/* Installs checkpoint and future-deletion admission before the producer.
 * maintenance permits mandatory completion while the standing floor is in use;
 * ordinary prefixes must preserve that floor after funding their claims.
 * Preparation decodes only completed private metadata. Accept is infallible
 * after canonical commit. */
int ps_checkpoint_delta_open(ps_key_checkpoint *, int maintenance,
                             ps_checkpoint_delta **, lc_error *);
int ps_checkpoint_delta_plan(ps_checkpoint_delta *,
                             const lc_pouch_storage_batch *, uint64_t,
                             ps_budget_plan *, lc_error *);
int ps_checkpoint_delta_fund(ps_checkpoint_delta *, ps_budget *,
                             const ps_budget_plan *, lc_error *);
int ps_checkpoint_delta_bind(ps_checkpoint_delta *, ps_record_writer *,
                             uint64_t segment, const char *leaf, lc_error *);
int ps_checkpoint_delta_prepare(ps_checkpoint_delta *, lc_error *);
void ps_checkpoint_delta_accept(ps_checkpoint_delta *);
void ps_checkpoint_delta_close(ps_checkpoint_delta *);
/* Recovery uses complete decoded canonical records, then records the exact
 * accepted suffix end. Admission must finish before another body producer. */
int ps_key_checkpoint_replay(ps_key_checkpoint *, const lc_pouch_state_entry *,
                             lc_error *);
void ps_key_checkpoint_replayed(ps_key_checkpoint *, uint64_t, uint64_t);
/* Caller excludes canonical mutation and has observed an empty selected head.
 * Preserve all prior key changes and watermarks while covering that zero-byte
 * source, so sealed empty predecessors can retire without invented records. */
int ps_key_checkpoint_cover_empty(ps_key_checkpoint *, uint64_t, lc_error *);
int ps_key_checkpoint_due(ps_key_checkpoint *);
int ps_key_checkpoint_flush(ps_key_checkpoint *, lc_error *);
/* Complete funded stream debt before a native metadata batch without forcing
 * an extra barrier. Its replacement obligations remain until prefix durability.
 */
int ps_key_checkpoint_flush_deferred(ps_key_checkpoint *, lc_error *);
/* Caller excludes canonical mutation and has completed prior stream debt. */
int ps_key_metadata_begin(ps_key_checkpoint *, int maintenance,
                          const lc_pouch_storage_batch *, uint64_t,
                          ps_checkpoint_delta **, lc_error *);
int ps_key_metadata_commit(ps_checkpoint_delta *,
                           const lc_pouch_storage_batch *, enum ps_commit_mode,
                           lc_error *);
/* Offline physical conversion handoff. Caller excludes mutation and completes
 * stream debt first. Compare the entire selected canonical metadata with the
 * expected record, preserve its logical fields, and durably select only the
 * replacement descriptor and span. Reject attached logical checkpoint owners.
 * Replacement must be a verified, committed body in this namespace. */
int ps_key_checkpoint_replace_body(ps_key_checkpoint *,
                                   const lc_pouch_state_entry *expected,
                                   const lc_pouch_state_entry *replacement,
                                   lc_error *);
/* Conversion admission owns a canonical metadata snapshot, a bounded future
 * key replacement and its coverage. Plan before body input, fund from the
 * compound parent before it has writers, then commit actual converted metadata
 * within that bound. Caller holds namespace mutation exclusion throughout. */
int ps_key_body_plan(ps_key_checkpoint *, const lc_pouch_state_entry *,
                     uint64_t, ps_checkpoint_body **, ps_budget_plan *,
                     lc_error *);
int ps_key_body_fund(ps_checkpoint_body *, ps_budget *, lc_error *);
const lc_pouch_state_entry *ps_key_body_source(const ps_checkpoint_body *);
int ps_key_body_commit(ps_checkpoint_body *, const lc_pouch_state_entry *,
                       lc_error *);
void ps_key_body_close(ps_checkpoint_body *);
/* Capture slots and coverage at one completed visibility prefix, including
 * when the owner has a newer deferred metadata revision. */
int ps_key_checkpoint_capture(ps_key_checkpoint *, ps_values **,
                              ps_checkpoint_info *, lc_error *);
int ps_key_checkpoint_read(ps_key_checkpoint *, uint64_t slot, ps_reader **,
                           lc_error *);
/* Constant-time source ownership observation for ordinary reclamation. */
uint64_t ps_key_checkpoint_source_refs(ps_key_checkpoint *, unsigned int,
                                       uint64_t);
/* Bounded coalesced body ranges relative to one logical piece. Uses only the
 * live interval index, not canonical history or body bytes. Free ranges with
 * the default allocator. Epoch selection rejects changed or prepared body
 * ownership; unchanged lease metadata does not invalidate a capture. */
typedef struct ps_checkpoint_ranges {
  ps_owned_range *ranges;
  size_t count;
  uint64_t bytes, epoch;
} ps_checkpoint_ranges;
int ps_key_checkpoint_ranges(ps_key_checkpoint *, unsigned int, uint64_t,
                             uint64_t base, uint64_t bytes,
                             ps_checkpoint_ranges *, lc_error *);
int ps_key_checkpoint_ranges_select(ps_key_checkpoint *, unsigned int, uint64_t,
                                    uint64_t epoch, ps_piece_publish_fn, void *,
                                    lc_error *);
#endif
