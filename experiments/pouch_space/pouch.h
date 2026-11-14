#ifndef POUCH_SPACE_POUCH_H
#define POUCH_SPACE_POUCH_H
#include "lc_pouch.h"
#include "space.h"

typedef struct ps_pouch_state_options {
  lc_pouch_open_options logical;
  uint64_t initial_capacity, maximum_capacity;
  const ps_io *io;
} ps_pouch_state_options;

/* Private integration harness, never installed: ordinary state PUT, metadata
 * update, delete, point read/metadata/copy and namespace creation use the
 * production logical engine. Retention work hints are durable in-file markers;
 * reclamation and guaranteed maintenance admission at capacity remain pending.
 * Rotation can still require foreground capacity before a delete.
 * Other Pouch operations are outside this harness's current call contract.
 * Explicitly disable indexing, background maintenance and queue watches; shared
 * root mode and generated external key files are not admitted. No public
 * backend option or native-open cutover is introduced by this harness. */
int ps_pouch_state_open(const char *, int create, const lc_allocator *,
                        const ps_pouch_state_options *, lc_pouch **,
                        lc_error *);
#endif
