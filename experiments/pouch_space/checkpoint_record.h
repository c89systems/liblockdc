#ifndef POUCH_SPACE_CHECKPOINT_RECORD_H
#define POUCH_SPACE_CHECKPOINT_RECORD_H
#include "lc_pouch_state_record.h"
#include "space.h"

/* Materialized canonical metadata only. Application bytes remain referenced
 * by their existing encoded spans; they are never copied into this buffer. */
typedef struct ps_checkpoint_record {
  lc_allocator allocator;
  unsigned char *bytes;
  size_t length;
} ps_checkpoint_record;
int ps_checkpoint_record_encode(const lc_allocator *,
                                const lc_pouch_state_entry *,
                                ps_checkpoint_record *, lc_error *);
void ps_checkpoint_record_close(ps_checkpoint_record *);
/* Prepared output remains private; its owner selects it with the other slots
 * and checkpoint coverage through one physical publication. */
int ps_checkpoint_record_write(ps_space *, ps_budget *, ps_key,
                               const ps_checkpoint_record *, ps_writer **,
                               lc_error *);
int ps_checkpoint_record_read(ps_reader *, const lc_allocator *,
                              const char *leaf, lc_pouch_state_entry *,
                              lc_error *);
#endif
