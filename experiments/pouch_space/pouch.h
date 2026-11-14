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
 * update, delete, point read/metadata/copy, read-many, metadata visits, paged
 * scan summaries/body reads and namespace creation use the
 * production logical engine. Retention work hints are durable in-file markers;
 * retention scheduling and guaranteed maintenance admission at capacity remain
 * pending. Forced ordinary-state compaction is available below.
 * Rotation can still require foreground capacity before a delete.
 * Other Pouch operations are outside this harness's current call contract.
 * Explicitly disable indexing, background maintenance and queue watches; shared
 * root mode and generated external key files are not admitted. No public
 * backend option or native-open cutover is introduced by this harness. */
int ps_pouch_state_open(const char *, int create, const lc_allocator *,
                        const ps_pouch_state_options *, lc_pouch **,
                        lc_error *);
typedef struct ps_pouch_compaction_result {
  uint64_t records, metadata_bytes, body_bytes, copied_bytes;
  uint64_t snapshot, boundary, reclaimed_sources, pending_sources;
  int skipped;
} ps_pouch_compaction_result;
/* One forced ordinary-state compaction pass. Relocates one source into a
 * sealed body pack, publishes a metadata-only logical snapshot, then rechecks
 * the current tail before retirement. Copies run outside namespace authority.
 * Staged/history/index/collection maintenance remains outside this harness.
 * Known canonical spans are reserved before copying; complete protected
 * metadata/rotation admission at full capacity is still pending. */
int ps_pouch_state_compact(lc_pouch *, const char *ns,
                           ps_pouch_compaction_result *, lc_error *);
#endif
