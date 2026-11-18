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
 * same logical engine. .txns remains the decision authority. Guaranteed
 * maintenance admission at capacity remains pending.
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
 * maintenance use the common compactor, retention and query-page engines.
 * Query pages are bounded derived metadata in the container, bound to the
 * recovered order epoch and canonical rows. Old epochs remain scan fallbacks.
 * Public queue delivery, batching, paired state, transaction recovery and
 * subscription/watch calls use the common engine. In-process advisory queue
 * notifications use no sidecar. Outbox delivery, stateful checkpoints,
 * completion/dead-letter/replay, inbox/command receipts and terminal retention
 * use the same owners, with bounded durable in-file cleanup markers.
 * Public XA resource registration, external branch progress, managed owner
 * epochs, recovery snapshots, completion and history retirement use the common
 * journal/coordinator engine. Journal handoffs force completed dependencies
 * under either durability policy; external SQL execution is outside this
 * harness. Exclusive ownership remains required.
 * Public namespace queries, explicit flush and the existing index worker use
 * typed immutable artifacts in the container. Captured inputs survive replace,
 * pruning and checkpoint; encoded bounds are admitted before invoking
 * producers. Canonical application bodies remain streamed. Namespace index
 * publication still has a material performance gap against the native owner.
 * Other Pouch operations are outside this harness's current call contract.
 * Shared root mode and generated external key files are not admitted. No public
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
  /* Selected retained encoded body bytes and map-value bytes, respectively.
   * Rejected candidates are excluded; I/O probes measure attempted work. */
  uint64_t records, metadata_bytes, body_bytes, copied_bytes;
  uint64_t snapshot, boundary, reclaimed_sources, pending_sources;
  int skipped;
} ps_pouch_compaction_result;
/* Checkpoints changed key metadata, retires unreferenced sealed sources, and
 * reclaims at most sixteen completed pieces per live/active source. A cached
 * cursor advances later passes. Encoded copying runs outside namespace
 * authority; selection reacquires it and revalidates body ownership. Live
 * stages retain their ranges and captured readers retain old mappings. Logical
 * body references and query indexes remain stable. Collection maintenance uses
 * its separate owner below. One candidate is funded before copying; computed
 * standing capacity and reconstruction of all root obligations remain pending.
 */
int ps_pouch_state_compact(lc_pouch *, const char *ns,
                           ps_pouch_compaction_result *, lc_error *);
/* Explicit collection retention, compaction, query repair and storage cleanup.
 * Snapshot copies use bounded buffers and leave foreground appends independent.
 * Existing grace periods and captured reader generations govern retirement.
 * This pass may require free capacity; it neither grows the container nor
 * promises maintenance admission at full capacity. */
int ps_pouch_collection_maintain(lc_collection *, int force, int cleanup_only,
                                 lc_pouch_maintenance_result *, lc_error *);
#endif
