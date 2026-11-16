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
 * staged writes, prepared mutations, promotion/commit/discard and cold decision
 * recovery also use that engine. Complete canonical batches have checksummed
 * recovery framing. Live staged records prevent ordinary compaction; settled
 * historical stages and decisions may be omitted by the resolved snapshot.
 * Public leased state updates/metadata/keepalive/release, transactional
 * attachments, native participant votes and on-demand expiry cleanup use the
 * same logical engine. .txns remains the decision authority. Complete external
 * XA ownership and guaranteed maintenance admission at capacity remain pending.
 * Public namespace maintenance and the common background scheduler use durable
 * in-file work hints for ordinary state, terminal cleanup, expiry and
 * collections. Forced ordinary-state compaction is also available below.
 * Internal cleanup durability scopes force the completed container prefix
 * durable under either root policy; unfinished producers remain private.
 * Rotation can still require foreground capacity before a delete.
 * Public collection create/open/list, heads, direct/staged/push append,
 * streamed scan/LQL reads and cursors, deletion, retention metadata, atomic
 * edits/retry receipts, leases/takeover/private reads and drop/recreate now use
 * the production collection engine through typed container owners. Catalogs,
 * control heads and canonical bytes live in the container; order/ID metadata
 * is disposable memfd scratch. A physical-owner recovery changes the scratch
 * epoch, invalidating prior cursors/head preconditions; receivers sharing a
 * live owner have the same epoch. Explicit and background collection
 * maintenance use the common compactor and retention engine. Indexing remains
 * outside this harness.
 * Public queue delivery, batching, paired state, transaction recovery and
 * subscription/watch calls use the common engine. In-process advisory queue
 * notifications use no sidecar. Outbox delivery, stateful checkpoints,
 * completion/dead-letter/replay, inbox/command receipts and terminal retention
 * use the same owners, with bounded durable in-file cleanup markers.
 * Other Pouch operations are outside this harness's current call contract.
 * Explicitly disable indexing; shared
 * root mode and generated external key files are not admitted. No public
 * backend option or native-open cutover is introduced by this harness. */
int ps_pouch_state_open(const char *, int create, const lc_allocator *,
                        const ps_pouch_state_options *, lc_pouch **,
                        lc_error *);
typedef struct ps_pouch_client_options {
  uint64_t initial_capacity, maximum_capacity;
  const ps_io *io;
} ps_pouch_client_options;
/* Private SDK integration: common client configuration and receiver lifetime,
 * admitted through the existing container owner. Logical options come from the
 * normal config/endpoint. The state harness's operation limits still apply. */
int ps_pouch_client_open(const lc_client_config *, int create,
                         const ps_pouch_client_options *, lc_client **,
                         lc_error *);
typedef struct ps_pouch_compaction_result {
  uint64_t records, metadata_bytes, body_bytes, copied_bytes;
  uint64_t snapshot, boundary, reclaimed_sources, pending_sources;
  int skipped;
} ps_pouch_compaction_result;
/* One forced ordinary-state compaction pass. Relocates one source into a
 * sealed body pack, publishes a metadata-only logical snapshot, then rechecks
 * the current tail before retirement. Copies run outside namespace authority.
 * Live staged records defer this pass; resolved historical stages may be
 * omitted. Collection maintenance uses the separate entry point below or the
 * public namespace maintenance API. Index maintenance remains outside it.
 * Known canonical spans are reserved before copying; complete protected
 * metadata/rotation admission at full capacity is still pending. */
int ps_pouch_state_compact(lc_pouch *, const char *ns,
                           ps_pouch_compaction_result *, lc_error *);
/* Explicit collection retention, compaction and obsolete storage cleanup.
 * Snapshot copies use bounded buffers and leave foreground appends independent.
 * Existing grace periods and captured reader generations govern retirement.
 * This pass may require free capacity; it neither grows the container nor
 * promises maintenance admission at full capacity. */
int ps_pouch_collection_maintain(lc_collection *, int force, int cleanup_only,
                                 lc_pouch_maintenance_result *, lc_error *);
#endif
