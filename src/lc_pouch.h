#ifndef LC_POUCH_H
#define LC_POUCH_H

#include "lc/lc.h"
#include "lc_intcompat.h"

#include <stddef.h>

typedef struct lc_pouch lc_pouch;

/* Durable state generations and Unix timestamps have fixed-width storage. */
typedef lc_u64 lc_pouch_generation;
typedef lc_i64 lc_pouch_unix_seconds;

typedef struct lc_pouch_open_options {
  uint64_t segment_target_bytes;
  /** Maximum distinct ready projections before an exclusive writer publishes
   * its immutable generation synchronously. Shared-root handles use the same
   * bound on their asynchronous replay indexer. Zero selects Go disk's
   * default of 2,000. */
  uint64_t indexer_flush_docs;
  /** Maximum seconds the indexer retains its first unflushed mutation before
   * asynchronous publication. Zero selects Go disk's disk-store default of
   * ten seconds. In exclusive mode, this deadline never publishes an active
   * public lease or transaction; its final body waits for that operation's
   * completion boundary. */
  uint64_t indexer_flush_interval_seconds;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  /**
   * Cadence for bounded background maintenance. A mutation or durable work
   * rediscovered at open schedules the first turn after this interval.
   * Later writes coalesce without postponing an already scheduled turn.
   * Retention uses this same worker; expiry never promises exact deletion.
   * Queued work, terminal markers, state-retention hints and collections share
   * one fair rotation, with at most one selected namespace per turn.
   * Explicit `lc_pouch_maintenance_run` starts its work immediately.
   */
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  /** Minimum active-segment bytes before a pending terminal transition seals
   * it for bounded snapshot reclamation. Zero selects the default. */
  uint64_t terminal_reclaim_min_bytes;
  int background_compaction_enabled;
  /** Distinguishes an explicit compaction setting from the Go-compatible
   * default. */
  int background_compaction_enabled_set;
  /** Leaves compaction IO unlimited instead of using the default 8 MiB/s
   * throttle. */
  int compaction_throttling_disabled;
  /**
   * Selects the writer mode explicitly. When unset, Pouch uses its default
   * exclusive-root writer mode. Set this field before using `single_writer`
   * to opt into either exclusive or shared-root operation deliberately.
   */
  int single_writer_set;
  /**
   * Writer mode selected when `single_writer_set` is nonzero. A nonzero value
   * reserves this root for one writer; zero enables the slower shared-root
   * coordination path. Shared writers retain a local projection and, under
   * append authority, replay only the peer tail after its verified cursor.
   * An incomplete unseen suffix is repaired there before append. Historical
   * bytes are revalidated on recovery or projection invalidation, not before
   * every healthy shared mutation.
   */
  int single_writer;
  const char *query_engine;
  const char *query_fallback_engine;
  /** Disables local derived query-index maintenance when set to zero. The
   * default is enabled unless `query_indexing_enabled_set` is nonzero. */
  int query_indexing_enabled;
  int query_indexing_enabled_set;
  const char *crypto_key;
  const char *crypto_key_file;
  int crypto_generate_key_file;
  /** NULL inherits an existing root's mode, or none for a new root. An explicit
   * none/zlib requests offline conversion when it differs; active writers
   * prevent conversion and the open adopts the stored mode. */
  const char *compression;
  pslog_logger *logger;
  /**
   * Wait for a durable root-scoped fsync group commit at each public mutation.
   * Defaults to zero, matching Go disk failover's NoSync policy.
   */
  int durable_sync;
  /** Maximum fsync requests per durable group commit. The worker commits when
   * this limit is reached or after its two-millisecond coalescing window.
   * Zero leaves the batch unbounded.
   */
  uint64_t fsync_batch_max_ops;
  /** Enables filesystem queue notifications where the root supports them. */
  int queue_watch;
} lc_pouch_open_options;

#define LC_POUCH_FSYNC_BATCH_BOUND_COUNT 13U
#define LC_POUCH_FSYNC_BATCH_BUCKET_COUNT                                      \
  (LC_POUCH_FSYNC_BATCH_BOUND_COUNT + 1U)
#define LC_POUCH_BACKEND_HASH_HEX_BYTES 64U

/** Aggregate group-commit diagnostics, using fixed-width counters on all
 * targets. */
typedef struct lc_pouch_fsync_stats {
  uint64_t total_batches;
  uint64_t total_requests;
  uint64_t max_batch_size;
  uint64_t total_sync_ns;
  uint64_t max_sync_ns;
  uint64_t bounds[LC_POUCH_FSYNC_BATCH_BOUND_COUNT];
  uint64_t counts[LC_POUCH_FSYNC_BATCH_BUCKET_COUNT];
} lc_pouch_fsync_stats;

typedef struct lc_pouch_status {
  char *root_path;
  char *layout_name;
  unsigned long layout_version;
  uint64_t segment_target_bytes;
  uint64_t indexer_flush_docs;
  uint64_t indexer_flush_interval_seconds;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  uint64_t terminal_reclaim_min_bytes;
  int background_compaction_enabled;
  int compaction_throttling_disabled;
  int single_writer;
  int supports_concurrent_writes;
  int aborted;
  int durable_sync;
  uint64_t fsync_batch_max_ops;
  int queue_watch_enabled;
  int filesystem_capabilities_known;
  int filesystem_is_nfs;
  char *queue_watch_mode;
  char *queue_watch_reason;
  char *query_engine;
  char *query_fallback_engine;
  int query_indexing_enabled;
  int crypto_enabled;
  char *crypto_key_file;
  char *compression;
} lc_pouch_status;

/** Root-scoped exclusive-writer presence used to fence HA auto mode. */
typedef struct lc_pouch_exclusive_writer_presence {
  int present;
  lc_pouch_unix_seconds expires_at_unix;
} lc_pouch_exclusive_writer_presence;

typedef struct lc_pouch_maintenance_options {
  const char *ns;
  int force;
  int cleanup_only;
  /** Requests a snapshot boundary for a pending terminal transition. This is
   * used by Pouch's background reclaimer; ordinary callers normally leave it
   * zero and use `force` for explicit full maintenance. */
  int terminal_reclaim;
  /** Explicit namespace age sweep using the last update time. This is distinct
   * from per-object expiry and is never enabled automatically. */
  lc_pouch_unix_seconds retention_updated_before_unix;
} lc_pouch_maintenance_options;

typedef struct lc_pouch_maintenance_result {
  char *ns;
  char *diagnostic;
  unsigned long candidate_segment_count;
  uint64_t candidate_bytes;
  uint64_t compacted_segment_id;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  unsigned long compacted_collection_count;
  unsigned long retention_scanned_count;
  unsigned long retention_expired_count;
  unsigned long retention_deleted_state_count;
  unsigned long retention_failed_count;
  int compacted;
  int skipped;
  int aborted;
} lc_pouch_maintenance_result;

typedef int (*lc_pouch_state_precondition_fn)(void *context, lc_error *error);

/* A target-key projection borrowed while a state mutation holds its authority.
 * Fields remain valid only for the duration of the precondition callback. */
typedef struct lc_pouch_state_precondition_view {
  int found;
  lc_pouch_generation version;
  lc_pouch_unix_seconds retention;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_query_hidden;
  int query_hidden;
  int has_body;
} lc_pouch_state_precondition_view;

typedef int (*lc_pouch_state_view_precondition_fn)(
    const lc_pouch_state_precondition_view *current, void *context,
    lc_error *error);

typedef struct lc_pouch_state_write_options {
  const char *content_type;
  const char *expected_etag;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_metadata;
  /* Selected retention overrides the current state policy. A negative value
   * requests a commit-relative duration; only staged records retain it. */
  int has_retention;
  lc_pouch_unix_seconds retention;
  /* Evaluates against the selected target-key projection without a reread. */
  lc_pouch_state_view_precondition_fn view_precondition;
  void *view_precondition_context;
  lc_pouch_state_precondition_fn precondition;
  void *precondition_context;
  lc_pouch_generation expected_version;
  int has_expected_version;
  /* Transaction staging can carry a durable logical version independently of
   * the staging object's physical revision. */
  lc_pouch_generation logical_version;
  int has_logical_version;
  int create_if_absent;
  int has_query_hidden;
  int query_hidden;
  /* A private staged record is not a public query-index mutation. */
  int suppress_query_index;
  int disable_compression;
  /* Stores an internal object (attachments, queues, control records) rather
   * than public document state. Object records are excluded from query-index
   * capture and never advance its freshness generation. */
  int object_record;
  /* Internal client coordination: the public lease or transaction that owns
   * this write remains active and will later signal its completion boundary.
   * Low-level lc_pouch_state_* callers leave this clear so idle indexing can
   * publish their durable writes without a separate completion callback. */
  int query_index_operation_active;
  /* Internal identity for the active public operation. It is distinct for
   * every lease or transaction so an older completion cannot retire a newer
   * operation on the same document. */
  const char *query_index_operation_id;
  /* Internal pointer populated by the validated lease precondition. A
   * positive expiration lets the idle indexer retire an abandoned guard after
   * its owning lease is no longer valid. */
  lc_pouch_unix_seconds *query_index_operation_expires_at_unix;
  /* Internal only: marks a staged transactional delete in durable metadata.
   * Public content types remain ordinary content types unless this field is
   * set by the delete path. */
  int staged_delete_marker;
} lc_pouch_state_write_options;

typedef struct lc_pouch_state_write_result {
  int has_body;
  char *etag;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  /* Internal state-layer handoff: the enclosing durable commit cancels this
   * newly installed guard if its fsync group fails. */
  int query_index_operation_guard_started;
} lc_pouch_state_write_result;

typedef struct lc_pouch_state_read_result {
  int found;
  char *content_type;
  char *etag;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int has_body;
  /* Internal transactional-delete discriminator from durable metadata. */
  int staged_delete_marker;
  lc_source *body;
} lc_pouch_state_read_result;

typedef struct lc_pouch_state_visit_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  const char *descriptor;
  const unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  /* Non-zero when this record owns a readable state payload (including an
   * empty payload). Metadata-only lease and tombstone records are not bodies.
   */
  int has_payload;
  /* Non-zero for internal object records, which are not public query state. */
  int object_record;
} lc_pouch_state_visit_entry;

typedef int (*lc_pouch_state_visit_fn)(const lc_pouch_state_visit_entry *entry,
                                       void *context, lc_error *error);
#define LC_POUCH_STATE_READ_MANY_STOP (-1000)
typedef int (*lc_pouch_state_read_many_fn)(
    const char *key, const lc_pouch_state_read_result *result, void *context,
    lc_error *error);

/**
 * Opens a Pouch root and starts its owned worker threads. Its exclusive
 * indexer publishes a bounded, body-free projection synchronously at a
 * completed public lease or transaction boundary when it reaches
 * `indexer_flush_docs` (2,000 by default) and no other pending key remains
 * active. The batch take rechecks that active-key invariant before it can
 * publish, matching Go disk's memtable threshold without indexing an
 * intermediate in-lease body version. A successful foreground publication
 * consumes its queued batch, so a later below-threshold mutation begins a
 * fresh worker interval. Below that bound, and for shared roots, the worker
 * publishes after `indexer_flush_interval_seconds` (ten seconds by default).
 * The exclusive worker defers its idle batch while a captured public operation
 * remains active, then retries after that operation's completion boundary;
 * direct storage mutations have no such boundary. Mutation completion never
 * retains source document bodies for indexing.
 *
 * A process must fork before opening Pouch, or exec before using Pouch in the
 * child. Re-entering an inherited Pouch handle after fork is unsupported:
 * worker-owned pthread state is not available in the child.
 */
int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error);
void lc_pouch_close(lc_pouch *pouch);
/**
 * Stops Pouch-owned worker loops, closes resident descriptor and derived-cache
 * state, and releases its process-bound writer lock. The durable
 * exclusive-writer heartbeat remains as crash fencing until its takeover
 * expiry, so another exclusive opener cannot bypass recovery.
 */
int lc_pouch_abort(lc_pouch *pouch, lc_error *error);
/** Returns whether this handle uses shared-root mutation coordination. */
int lc_pouch_supports_concurrent_writes(const lc_pouch *pouch);
/** Borrowed resolved storage mode, including inheritance or writer fallback. */
const char *lc_pouch_compression_mode(const lc_pouch *pouch);
/** Copies aggregate group-commit diagnostics into `out`. */
int lc_pouch_fsync_stats_read(lc_pouch *pouch, lc_pouch_fsync_stats *out,
                              lc_error *error);
/** Writes the stable SHA-256 root identity as 64 lowercase hex characters. */
int lc_pouch_backend_hash(lc_pouch *pouch,
                          char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                          lc_error *error);
int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error);
void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status);
/**
 * Changes this open handle between exclusive and shared-root operation.
 * The transition waits for this handle's active append-capable operations to
 * resolve their durable result, invalidates their append descriptors through
 * the writer-mode epoch, then changes root ownership. It fails while another
 * incompatible root writer is active.
 */
int lc_pouch_set_single_writer(lc_pouch *pouch, int enabled, lc_error *error);
int lc_pouch_probe_exclusive_writer(lc_pouch *pouch,
                                    lc_pouch_exclusive_writer_presence *out,
                                    lc_error *error);
int lc_pouch_maintenance_run(lc_pouch *pouch,
                             const lc_pouch_maintenance_options *options,
                             lc_pouch_maintenance_result *out, lc_error *error);
void lc_pouch_maintenance_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_maintenance_result *result);
int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *ns, lc_error *error);
int lc_pouch_state_write(lc_pouch *pouch, const char *ns, const char *key,
                         lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_delete(lc_pouch *pouch, const char *ns, const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_update_metadata(lc_pouch *pouch, const char *ns,
                                   const char *key,
                                   const lc_pouch_state_write_options *options,
                                   lc_pouch_state_write_result *out,
                                   lc_error *error);
int lc_pouch_state_stage_write(lc_pouch *pouch, const char *ns, const char *key,
                               const char *txn_id, lc_source *body,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_state_write_result *out,
                               lc_error *error);
int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *ns,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error);
int lc_pouch_state_commit_staged(lc_pouch *pouch, const char *ns,
                                 const char *key, const char *txn_id,
                                 lc_pouch_state_write_result *out,
                                 lc_error *error);
/** Internal client coordinator variant. The caller must later signal the
 * public operation's completion with lc_pouch_indexer_note_operation_complete.
 */
int lc_pouch_state_commit_staged_for_active_operation(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *ns,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error);
int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch, const char *ns,
                                            lc_error *error);
void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result);
int lc_pouch_state_read(lc_pouch *pouch, const char *ns, const char *key,
                        lc_pouch_state_read_result *out, lc_error *error);
int lc_pouch_state_copy(lc_pouch *pouch, const char *ns, const char *key,
                        lc_sink *dst, lc_pouch_state_read_result *out,
                        lc_error *error);
int lc_pouch_state_read_metadata(lc_pouch *pouch, const char *ns,
                                 const char *key,
                                 lc_pouch_state_read_result *out,
                                 lc_error *error);
int lc_pouch_state_read_many(lc_pouch *pouch, const char *ns,
                             const char *const *keys, size_t key_count,
                             lc_pouch_state_read_many_fn visitor, void *context,
                             lc_error *error);
int lc_pouch_state_read_many_cached(lc_pouch *pouch, const char *ns,
                                    const char *const *keys, size_t key_count,
                                    lc_pouch_state_read_many_fn visitor,
                                    void *context, lc_error *error);
int lc_pouch_state_visit(lc_pouch *pouch, const char *ns,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error);
/* Visits only live keys with `prefix` without snapshotting unrelated records.
 */
int lc_pouch_state_visit_prefix(lc_pouch *pouch, const char *ns,
                                const char *prefix,
                                lc_pouch_state_visit_fn visitor, void *context,
                                lc_error *error);
int lc_pouch_state_index_seq(lc_pouch *pouch, const char *ns,
                             lc_pouch_generation *out, lc_error *error);
/* Returns the durable sequence that can change indexed query candidates.
 * Lease-only metadata records intentionally do not advance this value. */
int lc_pouch_state_query_index_seq(lc_pouch *pouch, const char *ns,
                                   lc_pouch_generation *out, lc_error *error);
void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result);

#endif
