#ifndef POUCH_SPACE_CHECKPOINT_H
#define POUCH_SPACE_CHECKPOINT_H
#include "checkpoint_record.h"
#include "lc_pouch_storage.h"
#include "namespace.h"
#include "record_io.h"

typedef struct ps_key_checkpoint ps_key_checkpoint;
typedef struct ps_checkpoint_delta ps_checkpoint_delta;
typedef struct ps_checkpoint_info {
  uint64_t epoch, segment, offset, state_index, query_index;
  uint64_t slots, pending, pending_bytes;
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
int ps_checkpoint_delta_begin(ps_key_checkpoint *, ps_record_writer *,
                              uint64_t segment, const char *leaf,
                              int maintenance, ps_checkpoint_delta **,
                              lc_error *);
int ps_checkpoint_delta_prepare(ps_checkpoint_delta *, lc_error *);
void ps_checkpoint_delta_accept(ps_checkpoint_delta *);
void ps_checkpoint_delta_close(ps_checkpoint_delta *);
/* Recovery uses complete decoded canonical records, then records the exact
 * accepted suffix end. Admission must finish before another body producer. */
int ps_key_checkpoint_replay(ps_key_checkpoint *, const lc_pouch_state_entry *,
                             lc_error *);
void ps_key_checkpoint_replayed(ps_key_checkpoint *, uint64_t, uint64_t);
int ps_key_checkpoint_due(ps_key_checkpoint *);
int ps_key_checkpoint_flush(ps_key_checkpoint *, lc_error *);
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
