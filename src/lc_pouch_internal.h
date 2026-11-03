#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"

#include <pthread.h>
#include <time.h>

typedef struct lc_pouch_namespace_logstore lc_pouch_namespace_logstore;
typedef struct lc_pouch_source_cache_entry lc_pouch_source_cache_entry;
typedef struct lc_pouch_query_index_generation_cache_entry
    lc_pouch_query_index_generation_cache_entry;
typedef struct lc_pouch_query_index_doc_table_cache_entry
    lc_pouch_query_index_doc_table_cache_entry;
typedef struct lc_pouch_query_index_packed_cache_entry
    lc_pouch_query_index_packed_cache_entry;
typedef struct lc_pouch_query_index_manifest_trust_entry
    lc_pouch_query_index_manifest_trust_entry;
typedef struct lc_pouch_query_index_pending_entry
    lc_pouch_query_index_pending_entry;
typedef struct lc_pouch_query_index_active_operation
    lc_pouch_query_index_active_operation;
typedef struct lc_pouch_fsync_request lc_pouch_fsync_request;
typedef struct lc_pouch_fsync_batcher lc_pouch_fsync_batcher;
typedef struct lc_pouch_state_commit_group lc_pouch_state_commit_group;
typedef struct lc_pouch_state_metadata_append_batcher
    lc_pouch_state_metadata_append_batcher;
typedef struct lc_pouch_writer_root_lock_entry lc_pouch_writer_root_lock_entry;
typedef struct lc_pouch_indexer_pending_namespace
    lc_pouch_indexer_pending_namespace;

/* Shared wall clock for lease validation, retention publication and cleanup.
 * Failure is explicit; a retention deadline must never use a guessed clock. */
int lc_pouch_now_unix(lc_pouch_unix_seconds *out, lc_error *error);
/* Canonical nonnegative deadlines pass through; a negative staged duration
 * is resolved once against the chosen immutable commit timestamp. */
int lc_pouch_retention_resolve(lc_pouch_unix_seconds retention,
                               lc_pouch_unix_seconds commit_at_unix,
                               lc_pouch_unix_seconds *out, lc_error *error);
/* Internal cleanup reuses append commit groups, forcing their barriers even
 * for NoSync roots before fsynced work references advance or are retired.
 * A scope owns the calling thread's group and must be ended on every path. */
int lc_pouch_state_durable_scope_begin(lc_pouch *pouch,
                                       lc_pouch_state_commit_group **out,
                                       lc_error *error);
int lc_pouch_state_durable_scope_barrier(lc_pouch_state_commit_group *scope,
                                         lc_error *error);
/* Recovery includes containers written before this thread/process's group.
 * Requires namespace mutation authority; never reads application bodies. */
int lc_pouch_state_durable_scope_include_namespace_locked(lc_pouch *pouch,
                                                          const char *ns,
                                                          lc_error *error);
int lc_pouch_state_durable_scope_end(lc_pouch_state_commit_group *scope,
                                     int operation_rc, lc_error *error);

/* Exclusive roots have one in-process writer. These bounded stripes retain
 * conflicting-key serialization through durable completion without shared
 * mode's registry allocation or cross-process lock work. */
#define LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT 97U
/* Stable owner lists remain available for lifecycle traversal; hot namespace
 * lookup uses these fixed buckets instead of scanning those lists. */
#define LC_POUCH_NAMESPACE_REGISTRY_BUCKET_COUNT 127U

typedef struct lc_pouch_state_change_visit_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  const char *descriptor;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  /* A found record can still be lease metadata or a tombstone without a
   * readable public state payload. */
  int has_payload;
  int object_record;
  int found;
} lc_pouch_state_change_visit_entry;

typedef int (*lc_pouch_state_change_visit_fn)(
    const lc_pouch_state_change_visit_entry *entry, void *context,
    lc_error *error);
typedef struct lc_pouch_state_scan_summary_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  const char *descriptor;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry; positive values are Unix deadlines; negative
   * values are transaction-staged durations pending commit. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  const void *opaque;
} lc_pouch_state_scan_summary_entry;

typedef struct lc_pouch_state_scan_summaries_result {
  int truncated;
  char *next_start_after;
} lc_pouch_state_scan_summaries_result;

/* A transient state projection view used while a key mutation lock is held. */
typedef struct lc_pouch_state_metadata_view {
  int found;
  const char *etag;
  const char *content_type;
  lc_pouch_generation version;
  lc_pouch_unix_seconds retention;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_query_hidden;
  int query_hidden;
  int has_body;
  int is_delete_marker;
} lc_pouch_state_metadata_view;

/* Builds a body from one current state projection while exact-key mutation
 * authority is held. The state layer closes the returned source after it has
 * recorded the write and queued index projection work. */
typedef int (*lc_pouch_state_body_prepare_fn)(
    const lc_pouch_state_read_result *current, void *context, lc_source **body,
    lc_error *error);

/* Builds a metadata mutation from the current key projection while the key
 * mutation lock is held. Set `apply` to zero for a successful read-only
 * decision such as a lease already being held. */
typedef int (*lc_pouch_state_metadata_prepare_fn)(
    const lc_pouch_state_metadata_view *current, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error);

/* Builds one staged state write from the committed, staged, and lease
 * projections while one namespace mutation authority is held. */
typedef int (*lc_pouch_state_stage_prepare_fn)(
    const lc_pouch_state_metadata_view *committed,
    const lc_pouch_state_metadata_view *staged,
    const lc_pouch_state_metadata_view *lease_state, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error);

/* Builds a staged body from the committed and transaction-local projections
 * while namespace mutation authority is held. The state layer closes the
 * returned source after the staged record is finalized. */
typedef int (*lc_pouch_state_stage_body_prepare_fn)(
    const lc_pouch_state_read_result *committed,
    const lc_pouch_state_read_result *staged, void *context, lc_source **body,
    lc_error *error);

typedef int (*lc_pouch_state_scan_summary_visit_fn)(
    const lc_pouch_state_scan_summary_entry *entry, void *context,
    lc_error *error);

struct lc_pouch {
  lc_allocator allocator;
  char *root_path;
  uint64_t segment_target_bytes;
  uint64_t indexer_flush_docs;
  uint64_t indexer_flush_interval_seconds;
  uint64_t fsync_batch_max_ops;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  uint64_t terminal_reclaim_min_bytes;
  uint64_t marker_sequence;
  int durable_sync;
  int background_compaction_enabled;
  int compaction_throttling_disabled;
  int single_writer;
  int aborted;
  int queue_watch_enabled;
  int filesystem_capabilities_known;
  int filesystem_is_nfs;
  const char *queue_watch_mode;
  const char *queue_watch_reason;
  uint64_t single_writer_epoch;
  pthread_mutex_t single_writer_mutex;
  int single_writer_mutex_initialized;
  /* Readers cover append-capable work. A mode transition takes this writer
   * lock so no operation can append using the mode it observed before the
   * root ownership handoff. */
  pthread_rwlock_t writer_mode_guard;
  int writer_mode_guard_initialized;
  lc_pouch_writer_root_lock_entry *writer_root_lock;
  int writer_root_lock_mode;
  char *query_engine;
  char *query_fallback_engine;
  int query_indexing_enabled;
  char *compression;
  int compression_explicit;
  lc_pouch_crypto *crypto;
  char *crypto_key_file;
  pslog_logger *base_logger;
  pslog_logger *logger;
  int owns_logger;
  char *writer_marker_leaf;
  char *writer_presence_dir;
  char *writer_presence_leaf;
  char *writer_presence_path;
  pthread_mutex_t writer_presence_mutex;
  pthread_cond_t writer_presence_cond;
  pthread_t writer_presence_thread;
  int writer_presence_mutex_initialized;
  int writer_presence_cond_initialized;
  int writer_presence_thread_started;
  int writer_presence_stop;
  uint64_t root_device;
  uint64_t root_inode;
  int root_identity_initialized;
  pthread_mutex_t state_mutation_mutex;
  int state_mutation_mutex_initialized;
  pthread_mutex_t state_cache_mutex;
  int state_cache_mutex_initialized;
  /* Serializes query artifact publication without stalling pending writers. */
  pthread_mutex_t query_flush_mutex;
  int query_flush_mutex_initialized;
  /* Owns doc-table cache lookup, publication, eviction, and query borrows. */
  pthread_mutex_t query_doc_table_cache_mutex;
  int query_doc_table_cache_mutex_initialized;
  pthread_mutex_t exclusive_key_mutexes[LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT];
  size_t exclusive_key_mutex_count;
  lc_pouch_fsync_batcher *fsync_batcher;
  pthread_mutex_t compaction_mutex;
  pthread_cond_t compaction_cond;
  pthread_t compaction_thread;
  int compaction_mutex_initialized;
  int compaction_cond_initialized;
  int compaction_thread_started;
  int compaction_stop;
  int compaction_pending;
  /* Next background work source, protected by compaction_mutex. Discovery
   * cursors below retain the single maintenance worker's per-source progress.
   */
  int compaction_source_turn;
  char *collection_namespace_cursor;
  char *state_retention_marker_cursor;
  /* Last terminal-reclaim marker selected by the background worker. The
   * durable marker remains until maintenance completes; this cursor prevents
   * one failing marker from starving the other pending namespaces. */
  char *terminal_reclaim_marker_cursor;
  char **compaction_namespaces;
  unsigned char *compaction_namespace_terminal;
  size_t compaction_namespace_count;
  size_t compaction_namespace_capacity;
  /* Go disk's indexer batches durable-state replay away from mutations. */
  pthread_mutex_t indexer_mutex;
  pthread_cond_t indexer_cond;
  pthread_t indexer_thread;
  int indexer_mutex_initialized;
  int indexer_cond_initialized;
  int indexer_thread_started;
  int indexer_stop;
  lc_pouch_indexer_pending_namespace *indexer_pending_namespaces;
  lc_pouch_namespace_logstore *namespace_logstores;
  lc_pouch_namespace_logstore
      *namespace_logstore_buckets[LC_POUCH_NAMESPACE_REGISTRY_BUCKET_COUNT];
  pthread_mutex_t source_cache_mutex;
  int source_cache_mutex_initialized;
  lc_pouch_source_cache_entry *source_cache_entries;
  size_t source_cache_count;
  unsigned long source_cache_tick;
  lc_pouch_query_index_generation_cache_entry *query_generation_cache;
  size_t query_generation_cache_count;
  lc_pouch_query_index_doc_table_cache_entry *query_doc_table_cache;
  size_t query_doc_table_cache_count;
  lc_pouch_query_index_packed_cache_entry *query_packed_cache;
  size_t query_packed_cache_count;
  lc_pouch_query_index_manifest_trust_entry *query_manifest_trust;
  /* Exclusive-writer derived postings awaiting immutable publication. This is
   * protected by indexer_mutex and deliberately stores no document bodies. */
  lc_pouch_query_index_pending_entry *query_pending_index;
  /* Lease- and transaction-scoped publication guards. Kept independently of
   * a pending projection so installing a guard never performs filesystem I/O
   * on the foreground write path. Protected by indexer_mutex. */
  lc_pouch_query_index_active_operation *query_active_operations;
};

#ifdef LOCKDC_TEST_BUILD
/* Scoped by single-threaded queue tests; absent from production builds. */
extern int (*lc_pouch_test_queue_clock_gettime)(clockid_t clock_id,
                                                struct timespec *out,
                                                void *context);
extern void (*lc_pouch_test_queue_poll_delay)(const struct timespec *delay,
                                              void *context);
extern void *lc_pouch_test_queue_time_context;
typedef int (*lc_pouch_test_hook)(void *context, lc_error *error);
extern lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook;
extern void *lc_pouch_test_after_snapshot_write_context;
extern int (*lc_pouch_test_candidate_open_errno)(const char *path,
                                                 const char *phase);
/* Invoked after an indexed query borrows a document-table cache entry and
 * before it dereferences that borrowed table. */
extern lc_pouch_test_hook lc_pouch_test_after_doc_table_cache_borrow_hook;
extern void *lc_pouch_test_after_doc_table_cache_borrow_context;
/* Invoked after an indexed query has retained cache borrows for result rows,
 * before it sorts or emits those rows. */
extern lc_pouch_test_hook
    lc_pouch_test_after_doc_table_cache_rows_attached_hook;
extern void *lc_pouch_test_after_doc_table_cache_rows_attached_context;
/* A nonzero test override shortens the resident doc-table cache for eviction
 * interleavings. Production always uses its fixed cache capacity. */
extern size_t lc_pouch_test_doc_table_cache_capacity;
/* Invoked while holding the doc-table cache mutex immediately before a
 * borrowed entry lazily initializes one decoded key. */
extern lc_pouch_test_hook
    lc_pouch_test_before_doc_table_decoded_key_populate_hook;
extern void *lc_pouch_test_before_doc_table_decoded_key_populate_context;
typedef void (*lc_pouch_test_after_acquire_claim_hook_fn)(void *context);
extern lc_pouch_test_after_acquire_claim_hook_fn
    lc_pouch_test_after_acquire_claim_hook;
extern void *lc_pouch_test_after_acquire_claim_context;
/* Invoked after a held lease makes acquire enter its bounded retry path. */
typedef void (*lc_pouch_test_before_acquire_poll_hook_fn)(void *context);
extern lc_pouch_test_before_acquire_poll_hook_fn
    lc_pouch_test_before_acquire_poll_hook;
extern void *lc_pouch_test_before_acquire_poll_context;
/* Invoked after dequeue-with-state persists its state lease, before it builds
 * the returned message handle. */
typedef void (*lc_pouch_test_after_dequeue_state_lease_hook_fn)(void *context);
extern lc_pouch_test_after_dequeue_state_lease_hook_fn
    lc_pouch_test_after_dequeue_state_lease_hook;
extern void *lc_pouch_test_after_dequeue_state_lease_context;
/* Invoked after queue dequeue persists the message lease, before it publishes
 * the delivery record. */
extern lc_pouch_test_hook lc_pouch_test_after_queue_lease_claim_hook;
extern void *lc_pouch_test_after_queue_lease_claim_context;
/* Invoked after a queue delivery is durable, before its public handle is
 * constructed. */
typedef void (*lc_pouch_test_before_queue_message_build_hook_fn)(void *context);
extern lc_pouch_test_before_queue_message_build_hook_fn
    lc_pouch_test_before_queue_message_build_hook;
extern void *lc_pouch_test_before_queue_message_build_context;
/* Invoked after each zero-wait batch message is built, before ownership moves
 * into the batch result. */
extern lc_pouch_test_hook lc_pouch_test_after_queue_batch_message_build_hook;
extern void *lc_pouch_test_after_queue_batch_message_build_context;
/* Invoked immediately before a Pouch transaction decision is made durable.
 * Tests use this to prove that an outbox does not publish one participant
 * before its multi-key terminal decision exists. */
extern lc_pouch_test_hook lc_pouch_test_before_txn_decision_hook;
extern void *lc_pouch_test_before_txn_decision_context;
extern lc_pouch_test_hook lc_pouch_test_after_txn_decision_hook;
extern void *lc_pouch_test_after_txn_decision_context;
extern lc_pouch_test_hook lc_pouch_test_cleanup_fsync_hook;
extern void *lc_pouch_test_cleanup_fsync_context;
/* Crash barrier between decision fencing and replacement owner publication. */
extern lc_pouch_test_hook lc_pouch_test_before_xa_owner_write_hook;
extern lc_pouch_test_hook lc_pouch_test_after_xa_retire_owner_hook;
extern void *lc_pouch_test_after_xa_retire_owner_context;
extern void *lc_pouch_test_before_xa_owner_write_context;
/* Invoked immediately before Pouch reads a durable transaction replay record.
 * Tests use this to exercise indeterminate post-vote replay failures. */
extern lc_pouch_test_hook lc_pouch_test_before_txn_replay_hook;
extern void *lc_pouch_test_before_txn_replay_context;
typedef void (*lc_pouch_test_metadata_append_hook_fn)(void *context,
                                                      const char *ns);
extern lc_pouch_test_metadata_append_hook_fn lc_pouch_test_metadata_append_hook;
extern void *lc_pouch_test_metadata_append_context;
/* Invoked before a borrowed metadata record is copied for batch submission. */
extern lc_pouch_test_hook lc_pouch_test_before_metadata_append_entry_copy_hook;
extern void *lc_pouch_test_before_metadata_append_entry_copy_context;
/* Invoked after a finalized metadata batch reaches the active segment, before
 * its sync boundary is confirmed. */
extern lc_pouch_test_hook lc_pouch_test_after_metadata_batch_append_hook;
extern void *lc_pouch_test_after_metadata_batch_append_context;
typedef void (*lc_pouch_test_body_append_hook_fn)(void *context,
                                                  const char *ns);
extern lc_pouch_test_body_append_hook_fn lc_pouch_test_body_append_hook;
extern void *lc_pouch_test_body_append_context;
typedef void (*lc_pouch_test_tail_repair_hook_fn)(void *context,
                                                  const char *reason,
                                                  const char *segment);
extern lc_pouch_test_tail_repair_hook_fn lc_pouch_test_tail_repair_hook;
extern void *lc_pouch_test_tail_repair_context;
/* Invoked under namespace mutation authority immediately before a completed
 * terminal-reclaim pass consumes its durable marker. */
extern lc_pouch_test_hook
    lc_pouch_test_before_terminal_reclaim_marker_remove_hook;
extern void *lc_pouch_test_before_terminal_reclaim_marker_remove_context;
/* Extends only test batch coalescing, without changing production scheduling.
 */
extern long lc_pouch_test_fsync_batch_delay_ns;
extern int (*lc_pouch_test_sync_fd)(int fd);
/* Invoked under the batch mutex after enqueue; must not reenter Pouch. */
extern void (*lc_pouch_test_fsync_enqueued)(void);
size_t lc_pouch_test_resident_descriptor_count(lc_pouch *pouch);
void lc_pouch_test_indexer_deadline(lc_pouch *pouch, struct timespec *deadline);
void lc_pouch_test_compaction_run_pass(lc_pouch *pouch);
size_t lc_pouch_test_compaction_queue_count(lc_pouch *pouch);
char *lc_pouch_state_test_crypto_context(const lc_allocator *allocator,
                                         const char *ns, const char *key,
                                         lc_pouch_generation version,
                                         int object_payload);
#endif

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);
typedef struct lc_pouch_state_shared_mutation_guard
    lc_pouch_state_shared_mutation_guard;
/**
 * Enters the shared-root durable mutation authority. Transaction coordinators
 * use this before their own per-transaction guard so nested state operations
 * retain one global lock order. It is a no-op in exclusive mode.
 */
int lc_pouch_state_shared_mutation_enter(
    lc_pouch *pouch, lc_pouch_state_shared_mutation_guard **out,
    lc_error *error);
void lc_pouch_state_shared_mutation_leave(
    lc_pouch_state_shared_mutation_guard **guard);
/* Converts historical binary lease and transaction control records before a
 * Pouch handle is exposed. Runtime control decoders intentionally remain
 * current-layout-only. */
int lc_pouch_control_migration_run(lc_pouch *pouch, lc_error *error);
/** Writes best-effort exclusive-root clean checkpoints after all append fsyncs.
 * Missing or invalid checkpoints only require a cold replay; they never alter
 * durable record ordering. */
void lc_pouch_state_checkpoint_clean_close(lc_pouch *pouch);
int lc_pouch_single_writer_snapshot(lc_pouch *pouch, uint64_t *epoch_out);
int lc_pouch_single_writer_enabled(lc_pouch *pouch);
/**
 * Pins writer mode for an append-capable operation until its durable result is
 * resolved. Internal callers must pair each successful begin with end.
 */
int lc_pouch_writer_mode_operation_begin(lc_pouch *pouch, lc_error *error);
void lc_pouch_writer_mode_operation_end(lc_pouch *pouch);
/** Queues one namespace-local compaction attempt after a successful mutation.
 * Terminal transitions additionally leave durable reclaim work for a later
 * restart; neither path scans the root at open. */
void lc_pouch_compaction_note_mutation(lc_pouch *pouch, const char *ns,
                                       int terminal);
/**
 * Persists terminal-reclaim work after a durable history cursor advances so a
 * restart can reclaim newly unpinned history without an open-time sweep.
 */
int lc_pouch_compaction_note_history_advanced(lc_pouch *pouch, const char *ns,
                                              lc_error *error);
/* Consumes completed terminal-reclaim work while the caller holds the same
 * namespace mutation authority used to make the reclamation decision. */
int lc_pouch_terminal_reclaim_marker_remove(lc_pouch *pouch, const char *ns,
                                            lc_error *error);
/* Durable, bounded-work hand-off for stateful outbox checkpoint cleanup.
 * These markers live outside namespace log data: a stateful dispatcher takes
 * at most one marker per pull, validates the parent terminal record, then
 * deletes only its paired checkpoint. */
int lc_pouch_outbox_state_reclaim_marker_write(lc_pouch *pouch, const char *ns,
                                               const char *marker_id,
                                               const char *outbox_key,
                                               const char *state_key,
                                               lc_error *error);
int lc_pouch_outbox_state_reclaim_marker_take(lc_pouch *pouch, const char *ns,
                                              const char *after_marker_id,
                                              char **marker_id_out,
                                              char **outbox_key_out,
                                              char **state_key_out,
                                              lc_error *error);
int lc_pouch_outbox_state_reclaim_marker_remove(lc_pouch *pouch, const char *ns,
                                                const char *marker_id,
                                                lc_error *error);
/* One durable Pouch-only outbox dead-letter retention work item. The marker
 * lives outside namespace log data, so dispatcher maintenance can reclaim an
 * exact terminal parent without querying or visiting the namespace. */
enum {
  LC_POUCH_RETENTION_DEAD_LETTER = 0,
  LC_POUCH_RETENTION_COMPLETED,
  LC_POUCH_RETENTION_INBOX,
  LC_POUCH_RETENTION_COMMAND,
  LC_POUCH_RETENTION_KIND_COUNT
};
typedef struct lc_pouch_outbox_retention_marker {
  char *marker_id;
  /* Advisory filename identity, even when a retention marker body is
   * unreadable. Lets the maintenance turn advance without trusting or deleting
   * that body.
   */
  char selected_id[LC_XID_STRING_SIZE];
  char *outbox_key;
  lc_pouch_unix_seconds due_at_unix;
  uint64_t retained_bytes;
  size_t queued_count;
  uint64_t queued_bytes;
} lc_pouch_outbox_retention_marker;
int lc_pouch_outbox_retention_marker_write(lc_pouch *pouch, const char *ns,
                                           int kind, const char *marker_id,
                                           const char *outbox_key,
                                           lc_pouch_unix_seconds due_at_unix,
                                           uint64_t retained_bytes,
                                           lc_error *error);
int lc_pouch_outbox_retention_marker_take(lc_pouch *pouch, const char *ns,
                                          int kind, const char *after_marker_id,
                                          lc_pouch_unix_seconds now_unix,
                                          size_t max_count, uint64_t max_bytes,
                                          lc_pouch_outbox_retention_marker *out,
                                          lc_pouch_unix_seconds *next_due_unix,
                                          lc_error *error);
int lc_pouch_outbox_retention_marker_remove(lc_pouch *pouch, const char *ns,
                                            int kind, const char *marker_id,
                                            lc_pouch_unix_seconds due_at_unix,
                                            lc_error *error);
void lc_pouch_outbox_retention_marker_cleanup(
    lc_pouch *pouch, lc_pouch_outbox_retention_marker *marker);
/* Measures current logical bytes for an outbox envelope and optional stateful
 * checkpoint, including their attachment records. */
int lc_pouch_outbox_dead_letter_retained_bytes(lc_client_handle *client,
                                               const char *ns,
                                               const char *outbox_key,
                                               const char *state_key,
                                               uint64_t *out, lc_error *error);
/* Deletes every attachment belonging to a checkpoint whose paired outbox
 * parent was already verified terminal by stateful reclaim. */
int lc_pouch_outbox_state_reclaim_delete_attachments(lc_client_handle *client,
                                                     const char *ns,
                                                     const char *key,
                                                     int *deleted_count,
                                                     lc_error *error);
/**
 * Reads the oldest durable history-consumer acknowledgement for one namespace.
 * Callers that make a reclamation decision must hold that namespace's mutation
 * authority for the entire decision.
 */
int lc_pouch_history_oldest_acknowledged(lc_pouch *pouch, const char *ns,
                                         int *has_consumers,
                                         lc_pouch_generation *out,
                                         lc_error *error);
/** Schedules derived index publication after a mutation. The queue and bounded
 * normalized projection never retain a document body, and a failed derived
 * publication cannot revoke an already durable mutation. */
void lc_pouch_indexer_note_mutation(lc_pouch *pouch, const char *ns);
/** Marks a durable public operation boundary. In exclusive-writer mode this
 * synchronously publishes a queued namespace once its configured distinct
 * document threshold is reached and no pending key is still active; shared
 * roots retain asynchronous replay. */
void lc_pouch_indexer_note_operation_complete(lc_pouch *pouch, const char *ns,
                                              const char *key,
                                              const char *operation_id);
/** Removes one matching completed operation from the exclusive writer's
 * pending-operation set. The caller holds `indexer_mutex`; allocation failure
 * is conservative and leaves derived publication queued. */
int lc_pouch_query_index_pending_operation_complete_locked(
    lc_pouch *pouch, const char *ns, const char *key, const char *operation_id);
void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch);
int lc_pouch_state_metadata_append_worker_init(lc_pouch *pouch,
                                               lc_error *error);
void lc_pouch_state_metadata_append_worker_close(lc_pouch *pouch);
void lc_pouch_query_index_cache_cleanup(lc_pouch *pouch);
int lc_pouch_state_with_namespace_lock(lc_pouch *pouch, const char *ns,
                                       lc_pouch_state_precondition_fn callback,
                                       void *context, lc_error *error);
/** Non-zero when this thread currently holds a namespace lock for `pouch`. */
int lc_pouch_state_namespace_lock_held_by_current_thread(lc_pouch *pouch);
int lc_pouch_state_with_key_lock(lc_pouch *pouch, const char *ns,
                                 const char *key,
                                 lc_pouch_state_precondition_fn callback,
                                 void *context, lc_error *error);
int lc_pouch_state_read_metadata_locked(lc_pouch *pouch, const char *ns,
                                        const char *key,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error);
/**
 * Reads a full state record while the caller holds namespace mutation
 * authority. This avoids attempting to recursively acquire the namespace
 * process mutex while transaction application validates and updates paired
 * queue records.
 */
int lc_pouch_state_read_locked(lc_pouch *pouch, const char *ns, const char *key,
                               lc_pouch_state_read_result *out,
                               lc_error *error);
/** Writes a full state record while the caller holds the key mutation lock. */
int lc_pouch_state_write_locked(lc_pouch *pouch, const char *ns,
                                const char *key, lc_source *body,
                                const lc_pouch_state_write_options *options,
                                lc_pouch_state_write_result *out,
                                lc_error *error);
/**
 * Prepares and writes one state body while exact-key mutation authority is
 * held. This keeps read-transform-write mutations linearizable without
 * materializing the stored body in memory.
 */
int lc_pouch_state_write_prepared(lc_pouch *pouch, const char *ns,
                                  const char *key,
                                  const lc_pouch_state_write_options *options,
                                  lc_pouch_state_body_prepare_fn prepare,
                                  void *prepare_context,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error);
/** Commits staged state under target mutation authority. */
int lc_pouch_state_commit_staged_locked(lc_pouch *pouch, const char *ns,
                                        const char *key, const char *txn_id,
                                        lc_pouch_state_write_result *out,
                                        int operation_active,
                                        lc_pouch_unix_seconds commit_at_unix,
                                        lc_error *error);
/** Discards staged state under target mutation authority. */
int lc_pouch_state_discard_staged_locked(lc_pouch *pouch, const char *ns,
                                         const char *key, const char *txn_id,
                                         int *discarded, lc_error *error);
/**
 * Reads metadata while the caller holds the key mutation lock. On a current
 * exclusive-writer projection, `out` borrows its fields directly from that
 * projection. Those fields must be consumed before the next state mutation or
 * release of the key mutation lock. Shared and cold paths populate
 * `owned_fallback`, whose lifetime is managed by the caller.
 */
int lc_pouch_state_read_metadata_view_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_state_metadata_view *out,
    lc_pouch_state_read_result *owned_fallback, lc_error *error);
int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error);
/**
 * Prepares and applies one metadata mutation from one locked resident view.
 * The caller must already hold the key mutation lock.
 */
int lc_pouch_state_update_metadata_prepared_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    lc_pouch_state_metadata_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_write_result *out, lc_error *error);
/**
 * Stages a body after one namespace-atomic committed/staged/lease decision.
 * `lease_key` may differ from `key` for queue-state lease updates.
 */
int lc_pouch_state_stage_write_prepared(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *lease_key, lc_source *body,
    lc_pouch_state_write_options *options,
    lc_pouch_state_stage_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_stage_body_prepare_fn body_prepare,
    lc_pouch_state_write_result *out, lc_error *error);
/**
 * Stages metadata-only state while preserving the selected transaction-local
 * payload projection. The prepare callback makes the same namespace-atomic
 * lease and CAS decision as staged body writes.
 */
int lc_pouch_state_stage_metadata_prepared(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    const char *lease_key, lc_pouch_state_write_options *options,
    lc_pouch_state_stage_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_visit_since(lc_pouch *pouch, const char *ns,
                               lc_pouch_generation after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error);
int lc_pouch_state_visible_count(lc_pouch *pouch, const char *ns, size_t *count,
                                 lc_error *error);
int lc_pouch_state_warm_namespace(lc_pouch *pouch, const char *ns,
                                  lc_error *error);
int lc_pouch_state_scan_summaries(lc_pouch *pouch, const char *ns,
                                  const char *start_after, size_t limit,
                                  lc_pouch_state_scan_summary_visit_fn visitor,
                                  void *context,
                                  lc_pouch_state_scan_summaries_result *out,
                                  lc_error *error);
int lc_pouch_state_scan_summary_read_body(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_state_scan_summary_entry *entry, int cache_completed_body,
    lc_pouch_state_read_result *out, lc_error *error);
void lc_pouch_state_scan_summaries_result_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_summaries_result *result);
int lc_pouch_fsync_commit(lc_pouch *pouch, int fd, lc_error *error);
int lc_pouch_fsync_batcher_init(lc_pouch *pouch, lc_error *error);
void lc_pouch_fsync_batcher_close(lc_pouch *pouch);
int lc_pouch_queue_watch_wait(lc_pouch *pouch, const char *ns,
                              const char *queue, uint64_t timeout_ms);
/** Converts a durable Pouch byte count for generic C API response fields. */
int lc_pouch_size_to_public_long(uint64_t size, long *out, lc_error *error);

#endif
