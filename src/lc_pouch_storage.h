#ifndef LC_POUCH_STORAGE_H
#define LC_POUCH_STORAGE_H
#include "lc_pouch_record.h"
#include "lc_pouch_retention.h"
#include "lc_pouch_state_record.h"

/* Internal typed storage capabilities. The logical Pouch engine retains key
 * policy, generations, projections and codecs. No pathname operations. */
typedef struct lc_pouch_storage_namespace_info {
  uint64_t active_segment, segments, tail;
  uint64_t active_incarnation, checkpoint_epoch;
} lc_pouch_storage_namespace_info;
typedef struct lc_pouch_storage_append {
  void *handle;
  lc_pouch_record_io io;
  const char *leaf; /* Stable while the namespace capability is retained. */
  uint64_t start, segment, segments, incarnation;
} lc_pouch_storage_append;
typedef int (*lc_pouch_storage_visit_fn)(void *, uint64_t,
                                         const lc_pouch_state_entry *,
                                         lc_error *);
typedef int (*lc_pouch_storage_replay_fn)(void *, lc_pouch_storage_visit_fn,
                                          void *, lc_error *);
typedef int (*lc_pouch_storage_namespace_visit_fn)(void *, const char *,
                                                   lc_error *);

typedef struct lc_pouch_storage_state_slot {
  uint64_t id, incarnation;
} lc_pouch_storage_state_slot;
/* Slots are sorted by ID. Return count when the ID is absent. */
size_t lc_pouch_storage_state_slot_find(const lc_pouch_storage_state_slot *,
                                        size_t count, uint64_t id);
typedef struct lc_pouch_storage_state_refresh {
  const lc_pouch_storage_state_slot *previous;
  size_t previous_count;
  void *context;
  /* Takes ownership of the detached slot identities from the default
   * allocator. Incremental capture covers all previous IDs, with no
   * uncovered suffix. Selection stops consulting previous before begin. */
  void (*begin)(void *, int incremental, lc_pouch_storage_state_slot *,
                size_t count);
  int (*selected)(void *, uint64_t slot, const lc_pouch_state_entry *,
                  lc_error *);
  lc_pouch_storage_visit_fn visit;
} lc_pouch_storage_state_refresh;

/* Enumerate exact logical names through their owning storage. Names are
 * borrowed for the callback. No body replay, filtering or ordering guarantee.
 * Callbacks may open namespaces and return an error to stop the walk. Container
 * owners capture a durable catalog prefix; native owners walk their directory.
 * The caller retains the runtime throughout the operation. */
int lc_pouch_storage_namespaces_visit(lc_pouch *,
                                      lc_pouch_storage_namespace_visit_fn,
                                      void *, lc_error *);

/* Immutable logical projection of one captured canonical prefix. Reuses the
 * production supersession, object and query-generation rules without changing
 * the live namespace cache. Replay must visit the selected snapshot followed
 * by its captured newer prefix and stop on callback error. Capture resolves
 * historical stages/decisions, then rejects any remaining live staged records
 * until their logical maintenance policy is integrated.
 * Capture copies metadata only, never bodies. The caller retains any physical
 * source pins and the allocator context; the Pouch runtime is borrowed only
 * during capture. No logical publication or reclamation is performed here. */
typedef struct lc_pouch_state_snapshot lc_pouch_state_snapshot;
/* Resolve one complete, non-control canonical change against the prior entry
 * for that exact key. Pure metadata projection: no allocation, body I/O or
 * publication. Returns one for an accepted change and writes a borrowed view
 * with explicit query sequence and state/object classification; zero leaves
 * output untouched. Inputs have already passed canonical decoding/validation.
 * The same policy drives the live projection and typed key checkpoints. Prior
 * may be a scalar summary: key, seen, index/version/lifetime, record_type,
 * query sequence, found, payload-span presence and query-hidden flags suffice;
 * no other prior fields are inspected. */
int lc_pouch_state_entry_project(const lc_pouch_state_entry *prior,
                                 const lc_pouch_state_entry *change,
                                 lc_pouch_state_entry *out);
/* Decisions order canonical attempts, independently of payload generations. */
int lc_pouch_state_decision_entry_accepts(int prior_present,
                                          uint64_t prior_index,
                                          const lc_pouch_state_entry *);
int lc_pouch_state_key_is_staged(const char *);
typedef struct lc_pouch_storage_checkpoint_ref {
  const char *key, *leaf;
  uint64_t index, lifetime;
} lc_pouch_storage_checkpoint_ref;
/* Caller holds logical namespace authority across prepare, physical selection
 * and finish. Preparation allocates all replacement metadata references; finish
 * only swaps matching cached revisions, without invalidating the projection. */
int lc_pouch_state_checkpoint_prepare(lc_pouch *, const char *,
                                      const lc_pouch_storage_checkpoint_ref *,
                                      size_t, void **, lc_error *);
void lc_pouch_state_checkpoint_finish(lc_pouch *, void *, int published);
typedef int (*lc_pouch_state_snapshot_visit_fn)(void *,
                                                const lc_pouch_state_entry *,
                                                lc_error *);
int lc_pouch_state_snapshot_capture(lc_pouch *, lc_pouch_storage_replay_fn,
                                    void *replay_context,
                                    lc_pouch_state_snapshot **, lc_error *);
/* Visits HIGH_WATER first, then one resolved entry per key in byte order,
 * including tombstones and metadata-only placeholders. Fields are borrowed
 * for each call. A failed visit leaves the immutable capture reusable; a new
 * visit starts at HIGH_WATER. Concurrent visits are permitted. Keep the capture
 * alive throughout every visit; callbacks run without engine coordination. */
int lc_pouch_state_snapshot_visit(const lc_pouch_state_snapshot *,
                                  lc_pouch_state_snapshot_visit_fn, void *,
                                  lc_error *);
void lc_pouch_state_snapshot_close(lc_pouch_state_snapshot *);
/* Exact metadata-snapshot and body-pack sizes without body I/O. NULL copy_leaf
 * retains every body reference. Otherwise rebind that source's bodies into
 * leaf, storing their canonical body records separately from snapshot metadata.
 * This measures bytes, not complete physical maintenance admission. */
int lc_pouch_state_snapshot_measure(const lc_pouch_state_snapshot *,
                                    const char *leaf, const char *copy_leaf,
                                    uint64_t *metadata_bytes,
                                    uint64_t *body_bytes, lc_error *);
/* Complete a typed-owner topology publication under namespace authority and
 * invalidate its resident projection before releasing that authority, even
 * on an uncertain failure. The callback owns physical durability. */
int lc_pouch_state_snapshot_publish(lc_pouch *, const char *ns,
                                    lc_pouch_state_precondition_fn, void *,
                                    lc_error *);

enum lc_pouch_storage_append_mode {
  LC_POUCH_STORAGE_APPEND_PREFIX = 0,
  LC_POUCH_STORAGE_APPEND_COMPLETE,
  LC_POUCH_STORAGE_APPEND_MAINTENANCE,
  /* A bounded pending record with a funded key prefix and codec ceiling. */
  LC_POUCH_STORAGE_APPEND_MAINTENANCE_PREFIX,
  /* Unknown remaining payload in one indivisible canonical frame. */
  LC_POUCH_STORAGE_APPEND_FRAMED_PREFIX,
  /* A complete metadata record using ordinary space in the active segment. */
  LC_POUCH_STORAGE_APPEND_METADATA
};
/* Known canonical prefixes, borrowed only during append admission. No producer
 * or payload I/O runs in this callback. Query/index values may change later;
 * the owner admits metadata/deletion ceilings for the declared key set. */
typedef int (*lc_pouch_storage_prefix_fn)(void *, size_t,
                                          lc_pouch_record_header *,
                                          const void **, const void **,
                                          lc_error *);
typedef struct lc_pouch_storage_batch {
  size_t count;
  lc_pouch_storage_prefix_fn prefix;
  void *context;
} lc_pouch_storage_batch;
enum lc_pouch_storage_commit_mode {
  LC_POUCH_STORAGE_COMMIT_DEFAULT = 0,
  LC_POUCH_STORAGE_COMMIT_DEFER_SYNC
};
typedef void (*lc_pouch_storage_metadata_retired_fn)(void *, size_t);
typedef struct lc_pouch_storage_metadata_result {
  uint64_t checkpoint_epoch;
  /* Reports the ordinal of each input whose prior key slot was removed. */
  lc_pouch_storage_metadata_retired_fn retired;
  void *context;
} lc_pouch_storage_metadata_result;
/* Atomic canonical metadata, with no application producer or byte-stream
 * append. Prefixes are borrowed separately during begin and commit. Begin
 * admits key/metadata ceilings; commit receives their finalized generations.
 * Caller holds namespace mutation authority until close. */
typedef struct lc_pouch_storage_metadata_ops {
  int (*begin)(void *, void *, enum lc_pouch_storage_append_mode,
               const lc_pouch_storage_batch *, uint64_t, void **, lc_error *);
  int (*commit)(void *, void *, const lc_pouch_storage_batch *,
                enum lc_pouch_storage_commit_mode,
                lc_pouch_storage_metadata_result *, lc_error *);
  void (*close)(void *, void *);
  /* At a committed maintenance boundary, make superseded metadata reusable
   * if the next bounded record would otherwise exhaust completion capacity.
   * This is a hint; the next actual begin still owns authoritative admission.
   */
  int (*reclaim)(void *, uint64_t, lc_error *);
  uint64_t batch_bytes; /* Independent metadata requests may be coalesced up to
                           this bound. */
} lc_pouch_storage_metadata_ops;
#define LC_POUCH_STORAGE_TXN_GUARD_STRIPES 256U
struct lc_pouch_outbox_store;
struct lc_pouch_index_store_ops;
enum lc_pouch_storage_work_kind {
  LC_POUCH_STORAGE_WORK_ORDINARY = 0,
  LC_POUCH_STORAGE_WORK_TERMINAL,
  LC_POUCH_STORAGE_WORK_COLLECTION
};
typedef struct lc_pouch_storage_capacity {
  uint64_t available;
  uint64_t activity;
  int pressured;
  int urgent; /* A successful sample found low ordinary headroom. */
} lc_pouch_storage_capacity;
/* Discovery/control metadata belongs to the physical owner. The existing
 * scheduler retains cadence, fairness, coalescing and worker lifetime. State
 * and collection passes retain their common logical policies; physical copies
 * may release namespace authority between capture and publication. */
typedef struct lc_pouch_storage_maintenance_ops {
  int (*hint_ensure)(void *, const char *, enum lc_pouch_storage_work_kind,
                     lc_error *);
  int (*hint_remove)(void *, const char *, enum lc_pouch_storage_work_kind,
                     lc_error *);
  int (*hint_next)(void *, enum lc_pouch_storage_work_kind, const char *,
                   char **, char **, lc_error *);
  int (*state_run)(void *, lc_pouch *, const lc_pouch_maintenance_options *,
                   lc_pouch_maintenance_result *, lc_error *);
  int (*collections_run)(void *, lc_pouch *,
                         const lc_pouch_maintenance_options *,
                         lc_pouch_maintenance_result *, lc_error *);
  /* During a drain, sample headroom even after the pressure hint clears.
   * PENDING preserves an advisory pressure level, without a verified
   * headroom sample. Activity spans all live owners of the root. */
  int (*capacity_pressure)(void *, int draining, lc_pouch_storage_capacity *,
                           lc_error *);
  void (*note_mutation)(void *);
  /* Observe the durable expiry hint without acquiring logical state
   * authority. A present hint is rechecked by the retention engine. */
  int (*retention_pending)(void *, const char *, int *, lc_error *);
} lc_pouch_storage_maintenance_ops;
enum lc_pouch_namespace_lock_policy {
  LC_POUCH_NAMESPACE_FOREGROUND = 0,
  LC_POUCH_NAMESPACE_BACKGROUND
};
enum lc_pouch_storage_replay_mode {
  LC_POUCH_STORAGE_REPLAY_FULL,
  LC_POUCH_STORAGE_REPLAY_STATE
};
typedef struct lc_pouch_storage_ops {
  int (*check)(void *, lc_error *);
  /* Force the completed publication prefix durable, including on NoSync
   * roots. Private producers are excluded. An already covered prefix needs no
   * new barrier. This is a root capability, never a namespace file handle. */
  int (*flush)(void *, lc_error *);
  int (*namespace_open)(void *, const char *, int, void **, lc_error *);
  void (*namespace_close)(void *, void *);
  int (*namespace_info)(void *, void *, lc_pouch_storage_namespace_info *,
                        lc_error *);
  int (*namespaces_visit)(void *, lc_pouch_storage_namespace_visit_fn, void *,
                          lc_error *);
  /* Both replays report the captured boundary. State replay can represent
   * covered decisions with the checkpoint's exact sequence watermarks. */
  int (*replay)(void *, void *, lc_pouch_storage_namespace_info *,
                enum lc_pouch_storage_replay_mode, lc_pouch_storage_visit_fn,
                void *, lc_error *);
  /* Visit an append suffix only after checking the selected incarnation,
   * checkpoint epoch and end against the expected namespace boundary. */
  int (*replay_suffix)(void *, void *, const lc_pouch_storage_namespace_info *,
                       uint64_t start, lc_pouch_storage_visit_fn, void *,
                       lc_error *);
  /* A closed checkpoint can reuse unchanged physical slots. On a capture
   * conflict the caller may request a fresh projection with no prior slots. */
  int (*refresh_state)(void *, void *, lc_pouch_storage_namespace_info *,
                       const lc_pouch_storage_state_refresh *, lc_error *);
  /* Rotation target and known span. PREFIX permits an unknown remainder;
   * COMPLETE requires the declared size, MAINTENANCE additionally permits
   * spending protected work capacity. Reserve all dependencies before staging.
   * COMPLETE, MAINTENANCE and FRAMED_PREFIX are indivisible recovery units.
   * The declared span contains canonical
   * bytes only; storage accounts for its envelope in reservation and rotation.
   */
  int (*append_begin)(void *, void *, uint64_t, uint64_t,
                      enum lc_pouch_storage_append_mode,
                      const lc_pouch_storage_batch *, lc_pouch_storage_append *,
                      lc_error *);
  /* DEFER_SYNC belongs to the calling logical cleanup scope. Its completed
   * prefix must be flushed before that scope acknowledges durability. */
  int (*append_commit)(void *, lc_pouch_storage_append *,
                       enum lc_pouch_storage_commit_mode, lc_error *);
  void (*append_close)(void *, lc_pouch_storage_append *);
  /* Encoded, resettable bounded source owns its read pin and storage lifetime.
   * It may outlive the Pouch runtime that opened it. */
  int (*source_open)(void *, void *, const char *, uint64_t, uint64_t,
                     unsigned long stored_crc, lc_source **, lc_error *);
  /* Input belongs to this owner. Independent cursor over the same immutable
   * encoded pin, starting at zero.
   * No payload I/O, decoding, or current-directory lookup. */
  int (*source_clone)(void *, lc_source *, lc_source **, lc_error *);
  int (*retention_hint_ensure)(void *, const char *, lc_error *);
  /* Bounded control metadata. Load returns owned fields using the logical
   * allocator. Store borrows structured facts; idle clears work but retains
   * discovery. Owners choose their representation and must already have a
   * hint. Changes are durable even on NoSync roots. The caller retains
   * namespace mutation authority. */
  int (*retention_work_load)(void *, const char *, lc_pouch_retention_work *,
                             lc_error *);
  int (*retention_work_store)(void *, const char *,
                              const lc_pouch_retention_work *, lc_error *);
  int (*retention_hint_remove)(void *, const char *, lc_error *);
  /* Advisory, opaque owner cursor; allocated name/cursor use logical allocator.
   * Discovery reads only hints, without opening work records or bodies. */
  int (*retention_hint_next)(void *, const char *, char **, char **,
                             lc_error *);
  /* Volatile transaction authority for the supplied XID. wait=0
   * returns PENDING on contention without waiting. Successful acquisition
   * retains the owner and returns a non-NULL opaque handle; failure leaves
   * NULL. Release on the acquiring thread, after protected work, exactly once.
   * The common facade owns root mutation ordering. Covers tests the physical
   * authority for an XID, including aliases, to avoid recursive acquisition. */
  int (*txn_guard_lock)(void *, const char *txn_id, int wait, void **,
                        lc_error *);
  int (*txn_guard_covers)(void *handle, const char *txn_id);
  void (*txn_guard_release)(void *, void *);
  void (*close)(void *);
  /* Private SDK admission. The owner supplies catalog identity and binds the
   * common collection engine; it never implements another set of methods. */
  int (*collection_open)(void *, lc_client *, const char *,
                         const lc_collection_config *, int create,
                         lc_collection **, lc_error *);
  int (*collections_visit)(void *, lc_client *, const char *,
                           lc_collection_visit_fn, void *, lc_error *);
  const lc_pouch_storage_maintenance_ops *maintenance;
  int (*outbox_markers_open)(void *, const char *, int, int,
                             struct lc_pouch_outbox_store *, lc_error *);
  void (*queue_notify)(void *, const char *, const char *);
  uint32_t (*queue_observe)(void *);
  int (*queue_wait)(void *, const char *, const char *, uint32_t, uint64_t);
  const struct lc_pouch_index_store_ops *(*query_indexes)(void *, void **);
  const lc_pouch_storage_metadata_ops *metadata;
  /* Coordinated typed owners serialize state projections and decisions per
   * logical namespace. Acquire before source or publication authority.
   * Background requests may yield to pending foreground work. */
  int (*namespace_state_lock)(void *, const char *,
                              enum lc_pouch_namespace_lock_policy, void **,
                              lc_error *);
  void (*namespace_state_unlock)(void *, void *);
  /* Optional in-memory query watermark for an exclusive namespace whose
   * admitted recovery projection still covers the current canonical tail.
   * Return nonzero only when the sequence is known, under the caller's
   * namespace projection authority. */
  int (*namespace_query_sequence)(void *, lc_pouch_generation *);
  /* Durable acknowledgement records, serialized by namespace authority. */
  int (*history_markers_open)(void *, const char *, int,
                              struct lc_pouch_outbox_store *, lc_error *);
} lc_pouch_storage_ops;
typedef struct lc_pouch_storage {
  const lc_pouch_storage_ops *ops;
  void *context;
  int coordinated_namespaces;
} lc_pouch_storage;

/* Common logical runtime initialization, without opening a physical root.
 * Internal storage owners finish root policy/ownership before exposing it. */
int lc_pouch_runtime_open(const char *, const lc_allocator *,
                          const lc_pouch_open_options *, lc_pouch **,
                          lc_error *);
/* Call once, after physical ownership and all worker callbacks are installed.
 * Close joins the existing worker before releasing the physical owner. */
int lc_pouch_runtime_start_maintenance(lc_pouch *, lc_error *);
int lc_pouch_runtime_start_indexing(lc_pouch *, lc_error *);
/* Private admission seam for a physical owner. The constructor resolves the
 * ordinary SDK options before invoking factory once. It owns any returned
 * runtime, including during failed initialization, and retains neither factory
 * nor context. A non-NULL factory requires a Pouch endpoint; NULL preserves the
 * native owner. This does not add a public backend selector. */
typedef int (*lc_pouch_runtime_factory_fn)(void *, const char *,
                                           const lc_allocator *,
                                           const lc_pouch_open_options *,
                                           lc_pouch **, lc_error *);
int lc_client_open_with_pouch_factory(const lc_client_config *,
                                      lc_pouch_runtime_factory_fn, void *,
                                      lc_client **, lc_error *);
int lc_pouch_legacy_runtime_factory(void *, const char *, const lc_allocator *,
                                    const lc_pouch_open_options *, lc_pouch **,
                                    lc_error *);
/* Exercise the legacy reader/writer in migration fixtures and comparisons. */
int lc_pouch_legacy_client_open(const lc_client_config *, lc_client **,
                                lc_error *);
#endif
