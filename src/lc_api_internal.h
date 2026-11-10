#ifndef LC_API_INTERNAL_H
#define LC_API_INTERNAL_H

/*
 * Private SDK implementation details.
 *
 * Public callers should only include include/lc/lc.h. The structs and helpers
 * in this header are internal to the shipped HTTP/curl/lonejson implementation
 * of that public contract.
 */

#ifndef LONEJSON_WITH_CURL
#define LONEJSON_WITH_CURL
#endif
#include "lc/lc.h"

#include "lc_engine_api.h"
#include "lc_pouch.h"
#include <time.h>

#include <pslog.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct lc_stream_pipe lc_stream_pipe;

typedef struct lc_client_handle lc_client_handle;
typedef struct lc_outbox_dispatcher_handle lc_outbox_dispatcher_handle;

typedef struct lc_lease_handle lc_lease_handle;
typedef struct lc_message_handle lc_message_handle;
typedef struct lc_consumer_service_handle lc_consumer_service_handle;
typedef struct lc_history_consumer_handle lc_history_consumer_handle;

enum {
  LC_POUCH_COMMIT_STAGED = 0,
  LC_POUCH_COMMIT_STATE_IMMEDIATELY = 1,
  LC_POUCH_COMMIT_PARENT_IMMEDIATELY = 2
};

/* Test-build-only transport clock seam. The test library defines these when
 * LOCKDC_BUILD_TESTS is enabled; production code never references them. */
extern int (*lc_transport_test_clock_gettime)(clockid_t clock_id,
                                              struct timespec *out,
                                              void *context);
extern void *lc_transport_test_clock_context;

#ifdef LOCKDC_TEST_BUILD
typedef void (*lc_outbox_test_after_reconcile_query_hook_fn)(void *context);
extern lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_reconcile_query_hook;
extern void *lc_outbox_test_after_reconcile_query_context;
extern lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_recovery_overflow_hook;
extern void *lc_outbox_test_after_recovery_overflow_context;
extern lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_before_recovery_query_hook;
extern void *lc_outbox_test_before_recovery_query_context;
extern lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_recovery_capacity_pause_hook;
extern void *lc_outbox_test_after_recovery_capacity_pause_context;
typedef void (*lc_outbox_test_dead_letter_replay_client_hook_fn)(
    lc_client *client, lc_client *dispatcher_client, void *context);
extern lc_outbox_test_dead_letter_replay_client_hook_fn
    lc_outbox_test_dead_letter_replay_client_hook;
extern void *lc_outbox_test_dead_letter_replay_client_context;
typedef void (*lc_outbox_test_hook_fn)(void *context);
extern lc_outbox_test_hook_fn lc_outbox_test_after_close_requested_hook;
extern void *lc_outbox_test_after_close_requested_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_ready_job_detach_hook;
extern void *lc_outbox_test_before_ready_job_detach_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_ready_job_teardown_hook;
extern void *lc_outbox_test_before_ready_job_teardown_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_dispatcher_wait_hook;
extern void *lc_outbox_test_before_dispatcher_wait_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_next_wait_hook;
extern void *lc_outbox_test_before_next_wait_context;
extern lc_outbox_test_hook_fn
    lc_outbox_test_after_command_wait_pending_read_hook;
extern void *lc_outbox_test_after_command_wait_pending_read_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_next_release_hook;
extern void *lc_outbox_test_before_next_release_context;
extern lc_outbox_test_hook_fn lc_outbox_test_after_dispatcher_core_retain_hook;
extern void *lc_outbox_test_after_dispatcher_core_retain_context;
extern lc_outbox_test_hook_fn lc_outbox_test_before_worker_release_hook;
extern void *lc_outbox_test_before_worker_release_context;
size_t lc_outbox_test_dispatcher_ref_count(lc_outbox_dispatcher *dispatcher);
extern lc_outbox_test_hook_fn
    lc_outbox_test_before_dead_letter_export_open_hook;
extern void *lc_outbox_test_before_dead_letter_export_open_context;
void lc_outbox_test_wake_next_waiters(lc_outbox *outbox);
typedef int (*lc_outbox_test_failure_hook_fn)(void *context, lc_error *error);
extern lc_outbox_test_failure_hook_fn lc_outbox_test_before_ledger_append_hook;
extern void *lc_outbox_test_before_ledger_append_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_participant_allocation_hook;
extern void *lc_outbox_test_before_participant_allocation_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_command_receipt_copy_hook;
extern void *lc_outbox_test_before_command_receipt_copy_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_after_command_terminal_load_hook;
extern void *lc_outbox_test_after_command_terminal_load_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_receipt_copy_hook;
extern void *lc_outbox_test_before_outbox_receipt_copy_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_transaction_terminal_release_hook;
extern void *lc_outbox_test_before_transaction_terminal_release_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_after_transaction_terminal_decision_hook;
extern void *lc_outbox_test_after_transaction_terminal_decision_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_notification_copy_hook;
extern void *lc_outbox_test_before_notification_copy_context;
extern lc_outbox_test_failure_hook_fn lc_outbox_test_before_claim_outbox_hook;
extern void *lc_outbox_test_before_claim_outbox_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_handoff_reacquire_hook;
extern void *lc_outbox_test_before_outbox_handoff_reacquire_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_stateful_handoff_hook;
extern void *lc_outbox_test_before_stateful_handoff_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_stateful_terminal_cleanup_hook;
extern void *lc_outbox_test_before_stateful_terminal_cleanup_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_periodic_recovery_schedule_hook;
extern void *lc_outbox_test_before_periodic_recovery_schedule_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_dead_letter_claim_cleanup_hook;
extern void *lc_outbox_test_before_dead_letter_claim_cleanup_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_dead_letter_replay_release_hook;
extern void *lc_outbox_test_before_dead_letter_replay_release_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_renew_keepalive_hook;
extern void *lc_outbox_test_before_outbox_renew_keepalive_context;
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_renew_deadline_publish_hook;
extern void *lc_outbox_test_before_outbox_renew_deadline_publish_context;
int lc_outbox_test_retry_is_not_eligible(lc_unix_seconds not_before_unix,
                                         lc_unix_seconds now);
int lc_outbox_test_claim_failure_is_retryable(const lc_error *error);
int lc_outbox_test_recovery_is_due(int recovery_needed, int recovery_immediate,
                                   lc_unix_seconds next_recovery_unix,
                                   lc_unix_seconds now);
int lc_outbox_test_dead_letter_reclaim_is_due(int startup_replay_pending,
                                              int reclaim_needed,
                                              lc_unix_seconds next_reclaim_unix,
                                              lc_unix_seconds now);
int lc_outbox_test_schedule_dead_letter_reclaim_deadline(
    lc_unix_seconds prior_reclaim_unix,
    lc_unix_seconds prior_capacity_check_unix, lc_unix_seconds due_at_unix,
    lc_unix_seconds now, int capacity_check,
    lc_unix_seconds *next_reclaim_unix);
size_t lc_outbox_test_receipt_input_limit(int is_pouch,
                                          size_t response_limit_bytes);
long lc_outbox_test_host_job_timeout(long root_timeout_ms,
                                     long shutdown_timeout_ms);
int lc_outbox_test_claim_deadline_matches(lc_i64 deadline_attempt,
                                          lc_i64 deadline_replay,
                                          lc_i64 outbox_attempt,
                                          lc_i64 outbox_replay);
lc_i64 lc_outbox_test_select_claim_deadline(lc_i64 deadline_expires_at_unix,
                                            lc_i64 deadline_attempt,
                                            lc_i64 deadline_replay,
                                            lc_i64 outbox_attempt,
                                            lc_i64 outbox_replay,
                                            lc_i64 fallback);
int lc_outbox_test_conflicted_recovery_deadline(lc_i64 now,
                                                lc_i64 durable_deadline,
                                                long retry_delay_seconds,
                                                lc_i64 *out_deadline);
int lc_outbox_test_renew_claim_lease(lc_lease *lease, long ttl_seconds,
                                     lc_error *error);
int lc_outbox_test_delayed_recovery_deadline(lc_outbox *outbox, const char *key,
                                             lc_i64 *out_deadline);
int lc_outbox_test_dispatcher_delayed_recovery_deadline(
    lc_outbox_dispatcher *dispatcher, const char *key, lc_i64 *out_deadline);
int lc_outbox_test_dispatcher_periodic_recovery_is_armed(
    lc_outbox_dispatcher *dispatcher);
extern lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_terminal_remove_hook;
extern void *lc_outbox_test_before_terminal_remove_context;
int lc_outbox_test_reclaim_terminal(lc_outbox *outbox, int kind, lc_i64 now,
                                    size_t *reclaimed, lc_i64 *next_due,
                                    lc_error *error);
int lc_outbox_test_reclaim_dead_letters(lc_outbox *outbox, lc_i64 now_unix,
                                        size_t *reclaimed_out,
                                        lc_i64 *next_due_out, lc_error *error);
int lc_outbox_test_dead_letter_discovery_deadline(lc_i64 now_unix,
                                                  lc_i64 known_due_unix,
                                                  lc_i64 *deadline_out,
                                                  lc_error *error);
int lc_outbox_test_state_reclaim_marker_id(const char *outbox_key,
                                           char marker_id[44], lc_error *error);
void lc_lonejson_test_fail_thread_runtime_once(void);
#endif

struct lc_client_handle {
  lc_client pub;
  lc_engine_client *engine;
  lc_pouch *pouch;
  int is_pouch;
  char **endpoints;
  size_t endpoint_count;
  char *unix_socket_path;
  unsigned char *client_bundle_bytes;
  size_t client_bundle_length;
  char *client_bundle_path;
  char *default_namespace;
  long timeout_ms;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  size_t http_json_response_limit_bytes;
  char *pouch_crypto_key;
  char *pouch_crypto_key_file;
  char *pouch_compression;
  int pouch_crypto_generate_key_file;
  int pouch_crypto_generate_key_file_set;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
  int owns_logger;
  lc_allocator allocator;
  pthread_mutex_t lifecycle_mutex;
  lc_outbox_dispatcher_handle *outbox_dispatchers;
  unsigned long refcount;
  int close_requested;
  int lifecycle_mutex_initialized;
};

struct lc_lease_handle {
  lc_lease pub;
  lc_client_handle *client;
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  long fencing_token;
  lc_version version;
  lc_unix_seconds lease_expires_at_unix;
  char *state_etag;
  char *queue_state_etag;
  char *pouch_state_key;
  /* Outbox checkpoints and their parent checkpoint-presence/completion
   * records must survive a handler process dying before its terminal decision.
   * These private modes keep mutations fenced by the lease but commit them
   * instead of staging them under its txn id. */
  int pouch_commit_immediately;
  /* A direct, lease-fenced remove already cleared this immediate-mode lease.
   * Release can close it without a redundant failed replacement and read. */
  int pouch_state_removed;
  int pouch_stage_active;
  int pouch_stage_dirty;
  char *pouch_stage_etag;
  lc_version pouch_stage_version;
  int has_query_hidden;
  int query_hidden;
};

struct lc_message_handle {
  lc_message pub;
  lc_client_handle *client;
  char *ns;
  char *queue;
  char *message_id;
  int attempts;
  int max_attempts;
  int failure_attempts;
  lc_unix_seconds not_visible_until_unix;
  long visibility_timeout_seconds;
  char *payload_content_type;
  char *correlation_id;
  char *lease_id;
  lc_unix_seconds lease_expires_at_unix;
  long fencing_token;
  char *txn_id;
  char *meta_etag;
  char *next_cursor;
  lc_source *payload;
  int *terminal_flag;
  lc_lease *state_lease;
  char *state_etag;
  char *state_lease_id;
  lc_unix_seconds state_lease_expires_at_unix;
  long state_fencing_token;
  char *state_txn_id;
  int batch_owned;
};

typedef struct lc_write_bridge {
  lc_sink *sink;
} lc_write_bridge;

typedef struct lc_read_bridge {
  lc_source *source;
} lc_read_bridge;

typedef struct lc_watch_bridge {
  const lc_watch_handler *handler;
} lc_watch_bridge;

typedef struct lc_single_delivery_bridge {
  lc_client_handle *client;
  lc_message **out;
  lc_dequeue_batch_res *batch;
  lc_error *error;
  lc_engine_dequeue_response meta;
  lc_source *payload;
  lc_stream_pipe *pipe;
  int mode_batch;
} lc_single_delivery_bridge;

typedef struct lc_subscribe_bridge {
  lc_client_handle *client;
  const lc_consumer *consumer;
  lc_error *error;
  lc_engine_dequeue_response meta;
  lc_stream_pipe *pipe;
  pthread_t handler_thread;
  int handler_thread_started;
  int handler_rc;
  int terminal;
  lc_message *message;
} lc_subscribe_bridge;

int lc_state_retention_validate(int selected, long seconds, const char *ns,
                                const char *key, lc_error *error);
void lc_lease_retention_observe(lc_lease *lease,
                                lc_state_retention_support support,
                                int has_expiry, lc_unix_seconds expiry);
int lc_error_set(lc_error *error, int code, long http_status,
                 const char *message, const char *detail,
                 const char *server_code, const char *correlation_id);
int lc_error_from_engine(lc_error *error, lc_engine_error *engine);
const char *lc_nack_intent_to_string(lc_nack_intent intent);
int lc_nack_intent_to_wire_string(lc_nack_intent intent, const char **out,
                                  lc_error *error);
void *lc_alloc_with_allocator(const lc_allocator *allocator, size_t size);
void *lc_calloc_with_allocator(const lc_allocator *allocator, size_t count,
                               size_t size);
void *lc_realloc_with_allocator(const lc_allocator *allocator, void *ptr,
                                size_t size);
void lc_free_with_allocator(const lc_allocator *allocator, void *ptr);
char *lc_strdup_with_allocator(const lc_allocator *allocator,
                               const char *value);
void lc_secret_wipe(void *ptr, size_t length);
void lc_secret_free_string_with_allocator(const lc_allocator *allocator,
                                          char *value);
void *lc_client_alloc(lc_client_handle *client, size_t size);
void *lc_client_calloc(lc_client_handle *client, size_t count, size_t size);
void *lc_client_realloc(lc_client_handle *client, void *ptr, size_t size);
void lc_client_free(lc_client_handle *client, void *ptr);
char *lc_client_strdup(lc_client_handle *client, const char *value);
/* Raw libc domain for facade result metadata and third-party buffers. These
 * pointers must never be passed to the managed *_with_allocator helpers. */
void *lc_calloc_local(size_t count, size_t size);
void *lc_realloc_local(void *ptr, size_t size);
void lc_free_local(void *ptr);
char *lc_strdup_local(const char *value);
char *lc_dup_bytes_as_text(const void *bytes, size_t length);
void lc_attachment_info_copy(lc_attachment_info *dst,
                             const lc_engine_attachment_info *src);
size_t lc_engine_read_bridge(void *context, void *buffer, size_t count,
                             lc_engine_error *error);
int lc_engine_reset_bridge(void *context, lc_engine_error *error);
int lc_engine_write_bridge(void *context, const void *bytes, size_t count,
                           lc_engine_error *error);
int lc_sink_memory_reserve(lc_sink *sink, size_t capacity, lc_error *error);
lc_source *lc_source_from_open_file(FILE *fp, int close_file);
int lc_stream_pipe_open(size_t capacity, const lc_allocator *allocator,
                        lc_source **out, lc_stream_pipe **pipe,
                        lc_error *error);
int lc_stream_pipe_write(lc_stream_pipe *pipe, const void *bytes, size_t count,
                         lc_error *error);
void lc_stream_pipe_finish(lc_stream_pipe *pipe);
void lc_stream_pipe_fail(lc_stream_pipe *pipe, int code, const char *message);
int lc_source_is_resettable(const lc_source *source);
/* Returns the unread range for an SDK-owned memory source. This deliberately
 * rejects callback, file, fd, and externally implemented sources so storage
 * optimizations cannot turn a streaming call into hidden materialization. */
int lc_source_memory_view(const lc_source *source,
                          const unsigned char **bytes_out, size_t *length_out);
void lc_source_memory_consume(lc_source *source);
lc_lease *lc_lease_new(lc_client_handle *client, const char *ns,
                       const char *key, const char *owner, const char *lease_id,
                       const char *txn_id, long fencing_token,
                       lc_version version, const char *state_etag,
                       const char *queue_state_etag);
/* Creates an independently owned local view of a lease.  The clone preserves
 * backend method dispatch and private storage routing, but it does not retain
 * a client beyond the lifetime contract already shared by all lease handles. */
lc_lease *lc_lease_clone(const lc_lease *source);
lc_message *lc_message_new(lc_client_handle *client,
                           const lc_engine_dequeue_response *engine,
                           lc_source *payload, int *terminal_flag);

int lc_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                             lc_lease **out, lc_error *error);
int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error);
int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *handler_context,
    lc_error *error);
int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error);
int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error);
int lc_pouch_client_get_in_namespace(lc_client *self, const char *ns,
                                     const char *key, const lc_get_opts *opts,
                                     lc_sink *dst, lc_get_res *out,
                                     lc_error *error);
int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error);
int lc_pouch_client_load_in_namespace(lc_client *self, const char *ns,
                                      const char *key, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error);
int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error);
int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error);
int lc_pouch_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                                    lc_metadata_res *out, lc_error *error);
int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error);
int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out, lc_error *error);
int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error);
int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error);
int lc_pouch_client_list_attachments_method(lc_client *self,
                                            const lc_attachment_list_req *req,
                                            lc_attachment_list *out,
                                            lc_error *error);
int lc_pouch_client_get_attachment_method(lc_client *self,
                                          const lc_attachment_get_op *req,
                                          lc_sink *dst,
                                          lc_attachment_get_res *out,
                                          lc_error *error);
int lc_pouch_client_delete_attachment_method(lc_client *self,
                                             const lc_attachment_delete_op *req,
                                             int *deleted, lc_error *error);
int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error);
int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error);
int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error);
int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error);
int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out, lc_error *error);
int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error);
int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error);
int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error);
int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error);
int lc_pouch_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error);
int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error);
int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error);
int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error);
int lc_pouch_client_query_keys_method(lc_client *self, const lc_query_req *req,
                                      const lc_query_key_handler *handler,
                                      void *context, lc_query_res *out,
                                      lc_error *error);
int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error);
int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error);
int lc_pouch_client_flush_index_method(lc_client *self,
                                       const lc_index_flush_req *req,
                                       lc_index_flush_res *out,
                                       lc_error *error);
int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out, lc_error *error);
int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error);
int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error);
int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error);
int lc_pouch_client_recover_transactions(lc_client *self, lc_error *error);
int lc_pouch_client_tc_lease_acquire_method(lc_client *self,
                                            const lc_tc_lease_acquire_req *req,
                                            lc_tc_lease_acquire_res *out,
                                            lc_error *error);
int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error);
int lc_pouch_client_tc_lease_release_method(lc_client *self,
                                            const lc_tc_lease_release_req *req,
                                            lc_tc_lease_release_res *out,
                                            lc_error *error);
int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error);
int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error);
int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error);
int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error);
int lc_pouch_client_tc_rm_register_method(lc_client *self,
                                          const lc_tc_rm_register_req *req,
                                          lc_tc_rm_res *out, lc_error *error);
int lc_pouch_client_tc_rm_unregister_method(lc_client *self,
                                            const lc_tc_rm_unregister_req *req,
                                            lc_tc_rm_res *out, lc_error *error);
int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error);
int lc_pouch_message_ack_method(lc_message *self, lc_error *error);
int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *req,
                                 lc_error *error);
int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *req,
                                   lc_error *error);
int lc_pouch_lease_describe_method(lc_lease *self, lc_error *error);
int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error);
int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts, lc_error *error);
int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                                   lc_error *error);
int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                                 lc_error *error);
int lc_pouch_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                                    lc_error *error);
int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *req,
                                  lc_error *error);
int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error);
int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error);
int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error);
int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error);
int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error);
int lc_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *handler_context,
    lc_error *error);
int lc_client_describe_method(lc_client *self, const lc_describe_req *req,
                              lc_describe_res *out, lc_error *error);
int lc_client_get_method(lc_client *self, const char *key,
                         const lc_get_opts *opts, lc_sink *dst, lc_get_res *out,
                         lc_error *error);
int lc_client_get_in_namespace_method(lc_client *self, const char *ns,
                                      const char *key, const lc_get_opts *opts,
                                      lc_sink *dst, lc_get_res *out,
                                      lc_error *error);
int lc_client_load_method(lc_client *self, const char *key,
                          const lonejson_map *map, void *dst,
                          const lc_get_opts *opts, lc_get_res *out,
                          lc_error *error);
int lc_client_load_in_namespace_method(lc_client *self, const char *ns,
                                       const char *key, const lonejson_map *map,
                                       void *dst, const lc_get_opts *opts,
                                       lc_get_res *out, lc_error *error);
int lc_client_update_method(lc_client *self, const lc_update_req *req,
                            lc_source *src, lc_update_res *out,
                            lc_error *error);
int lc_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                            lc_mutate_res *out, lc_error *error);
int lc_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                              lc_metadata_res *out, lc_error *error);
int lc_client_remove_method(lc_client *self, const lc_remove_op *req,
                            lc_remove_res *out, lc_error *error);
int lc_client_keepalive_method(lc_client *self, const lc_keepalive_op *req,
                               lc_keepalive_res *out, lc_error *error);
int lc_client_release_method(lc_client *self, const lc_release_op *req,
                             lc_release_res *out, lc_error *error);
int lc_client_attach_method(lc_client *self, const lc_attach_op *req,
                            lc_source *src, lc_attach_res *out,
                            lc_error *error);
int lc_client_list_attachments_method(lc_client *self,
                                      const lc_attachment_list_req *req,
                                      lc_attachment_list *out, lc_error *error);
int lc_client_get_attachment_method(lc_client *self,
                                    const lc_attachment_get_op *req,
                                    lc_sink *dst, lc_attachment_get_res *out,
                                    lc_error *error);
int lc_client_delete_attachment_method(lc_client *self,
                                       const lc_attachment_delete_op *req,
                                       int *deleted, lc_error *error);
int lc_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error);
int lc_client_queue_stats_method(lc_client *self, const lc_queue_stats_req *req,
                                 lc_queue_stats_res *out, lc_error *error);
int lc_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                               lc_ack_res *out, lc_error *error);
int lc_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                lc_nack_res *out, lc_error *error);
int lc_client_queue_extend_method(lc_client *self, const lc_extend_op *req,
                                  lc_extend_res *out, lc_error *error);
int lc_client_query_method(lc_client *self, const lc_query_req *req,
                           lc_sink *dst, lc_query_res *out, lc_error *error);
int lc_client_query_keys_method(lc_client *self, const lc_query_req *req,
                                const lc_query_key_handler *handler,
                                void *context, lc_query_res *out,
                                lc_error *error);
int lc_client_get_namespace_config_method(lc_client *self,
                                          const lc_namespace_config_req *req,
                                          lc_namespace_config_res *out,
                                          lc_error *error);
int lc_client_update_namespace_config_method(lc_client *self,
                                             const lc_namespace_config_req *req,
                                             lc_namespace_config_res *out,
                                             lc_error *error);
int lc_client_flush_index_method(lc_client *self, const lc_index_flush_req *req,
                                 lc_index_flush_res *out, lc_error *error);
int lc_client_txn_replay_method(lc_client *self, const lc_txn_replay_req *req,
                                lc_txn_replay_res *out, lc_error *error);
int lc_client_txn_prepare_method(lc_client *self,
                                 const lc_txn_decision_req *req,
                                 lc_txn_decision_res *out, lc_error *error);
int lc_client_txn_commit_method(lc_client *self, const lc_txn_decision_req *req,
                                lc_txn_decision_res *out, lc_error *error);
int lc_client_txn_rollback_method(lc_client *self,
                                  const lc_txn_decision_req *req,
                                  lc_txn_decision_res *out, lc_error *error);
int lc_client_tc_lease_acquire_method(lc_client *self,
                                      const lc_tc_lease_acquire_req *req,
                                      lc_tc_lease_acquire_res *out,
                                      lc_error *error);
int lc_client_tc_lease_renew_method(lc_client *self,
                                    const lc_tc_lease_renew_req *req,
                                    lc_tc_lease_renew_res *out,
                                    lc_error *error);
int lc_client_tc_lease_release_method(lc_client *self,
                                      const lc_tc_lease_release_req *req,
                                      lc_tc_lease_release_res *out,
                                      lc_error *error);
int lc_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                               lc_error *error);
int lc_client_tc_cluster_announce_method(lc_client *self,
                                         const lc_tc_cluster_announce_req *req,
                                         lc_tc_cluster_res *out,
                                         lc_error *error);
int lc_client_tc_cluster_leave_method(lc_client *self, lc_tc_cluster_res *out,
                                      lc_error *error);
int lc_client_tc_cluster_list_method(lc_client *self, lc_tc_cluster_res *out,
                                     lc_error *error);
int lc_client_tc_rm_register_method(lc_client *self,
                                    const lc_tc_rm_register_req *req,
                                    lc_tc_rm_res *out, lc_error *error);
int lc_client_tc_rm_unregister_method(lc_client *self,
                                      const lc_tc_rm_unregister_req *req,
                                      lc_tc_rm_res *out, lc_error *error);
int lc_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                lc_error *error);
int lc_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                             lc_source *src, lc_enqueue_res *out,
                             lc_error *error);
int lc_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                             lc_message **out, lc_error *error);
int lc_client_dequeue_batch_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_dequeue_batch_res *out, lc_error *error);
int lc_client_dequeue_with_state_method(lc_client *self,
                                        const lc_dequeue_req *req,
                                        lc_message **out, lc_error *error);
int lc_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                               const lc_consumer *consumer, lc_error *error);
int lc_client_subscribe_with_state_method(lc_client *self,
                                          const lc_dequeue_req *req,
                                          const lc_consumer *consumer,
                                          lc_error *error);
int lc_client_new_consumer_service_method(
    lc_client *self, const lc_consumer_service_config *config,
    lc_consumer_service **out, lc_error *error);
int lc_client_new_outbox_method(lc_client *self, const lc_outbox_config *config,
                                lc_outbox **out, lc_error *error);
int lc_client_new_outbox_with_dispatcher_method(
    lc_client *self, const lc_outbox_config *config,
    lc_outbox_dispatcher *dispatcher, lc_outbox **out, lc_error *error);
int lc_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error);
int lc_pouch_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error);
int lc_client_clone_remote_for_outbox(lc_client_handle *source, long timeout_ms,
                                      const struct timespec *request_deadline,
                                      lc_client **out, lc_error *error);
int lc_client_watch_queue_method(lc_client *self, const lc_watch_queue_req *req,
                                 const lc_watch_handler *handler,
                                 lc_error *error);
void lc_client_handle_retain(lc_client_handle *client);
/* Resolves the default and rejects namespaces reserved for internal state. */
int lc_pouch_client_public_namespace(lc_client_handle *client, const char *ns,
                                     const char **out, lc_error *error);
int lc_client_create_collection_method(lc_client *,
                                       const lc_collection_config *,
                                       lc_collection **, lc_error *);
int lc_client_open_collection_method(lc_client *, const lc_collection_config *,
                                     lc_collection **, lc_error *);
int lc_client_list_collections_method(lc_client *, const char *,
                                      lc_collection_visit_fn, void *,
                                      lc_error *);
int lc_client_collection_capabilities_method(lc_client *,
                                             lc_collection_capabilities *,
                                             lc_error *);
void lc_client_handle_release(lc_client_handle *client);
void lc_client_close_method(lc_client *self);
void lc_outbox_dispatchers_stop_for_client(lc_client_handle *client);

int lc_lease_describe_method(lc_lease *self, lc_error *error);
int lc_lease_get_method(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
                        lc_get_res *out, lc_error *error);
int lc_lease_load_method(lc_lease *self, const lonejson_map *map, void *dst,
                         const lc_get_opts *opts, lc_get_res *out,
                         lc_error *error);
int lc_lease_save_method(lc_lease *self, const lonejson_map *map,
                         const void *src, const lc_update_opts *opts,
                         lc_error *error);
int lc_lease_update_method(lc_lease *self, lc_source *src,
                           const lc_update_opts *opts, lc_error *error);
int lc_lease_mutate_method(lc_lease *self, const lc_mutate_req *req,
                           lc_error *error);
int lc_lease_mutate_local_method(lc_lease *self, const lc_mutate_local_req *req,
                                 lc_error *error);
int lc_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                             lc_error *error);
int lc_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                           lc_error *error);
int lc_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                              lc_error *error);
int lc_lease_release_method(lc_lease *self, const lc_release_req *req,
                            lc_error *error);
int lc_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                           lc_source *src, lc_attach_res *out, lc_error *error);
int lc_lease_list_attachments_method(lc_lease *self, lc_attachment_list *out,
                                     lc_error *error);
int lc_lease_get_attachment_method(lc_lease *self,
                                   const lc_attachment_get_req *req,
                                   lc_sink *dst, lc_attachment_get_res *out,
                                   lc_error *error);
int lc_lease_delete_attachment_method(lc_lease *self,
                                      const lc_attachment_selector *selector,
                                      int *deleted, lc_error *error);
int lc_lease_delete_all_attachments_method(lc_lease *self, int *deleted_count,
                                           lc_error *error);
void lc_lease_close_method(lc_lease *self);

int lc_message_ack_method(lc_message *self, lc_error *error);
int lc_message_nack_method(lc_message *self, const lc_nack_req *req,
                           lc_error *error);
int lc_message_extend_method(lc_message *self, const lc_extend_req *req,
                             lc_error *error);
lc_lease *lc_message_state_method(lc_message *self);
lc_source *lc_message_payload_reader_method(lc_message *self);
int lc_message_rewind_payload_method(lc_message *self, lc_error *error);
int lc_message_write_payload_method(lc_message *self, lc_sink *dst,
                                    size_t *written, lc_error *error);

int lc_lonejson_error_from_status(lc_error *error, lonejson_status status,
                                  const lonejson_error *lj_error,
                                  const char *message);
size_t lc_lonejson_runtime_limit(size_t configured_limit);
void lc_lonejson_config_init(lonejson_config *config, size_t limit);
lonejson *lc_engine_lonejson_runtime(lc_engine_client *client);
lonejson *lc_thread_lonejson_runtime(void);
void lc_lonejson_cleanup_value(lonejson *runtime, const lonejson_map *map,
                               void *value);
void lc_engine_lonejson_cleanup(lc_engine_client *client,
                                const lonejson_map *map, void *value);
/* Iterative metadata walk; traversal allocation failure leaves dst intact.
 * Runtime init/reset remains the dependency's responsibility. */
int lc_lonejson_prepare_parse_destination(lonejson *runtime,
                                          const lonejson_map *map, void *value,
                                          lc_error *error);
lonejson_status lc_lonejson_parse_cstr_value(lonejson *runtime,
                                             const lonejson_map *map, void *dst,
                                             const char *json,
                                             lonejson_error *error);
int lc_lonejson_parse_file(lonejson *runtime, FILE *fp, const lonejson_map *map,
                           void *dst, lc_error *error, const char *message);
int lc_lonejson_serialize_file(lonejson *runtime, FILE *fp,
                               const lonejson_map *map, const void *src,
                               lc_error *error, const char *message);
void lc_message_close_method(lc_message *self);

int lc_consumer_service_run_method(lc_consumer_service *self, lc_error *error);
int lc_consumer_service_start_method(lc_consumer_service *self,
                                     lc_error *error);
int lc_consumer_service_stop_method(lc_consumer_service *self);
int lc_consumer_service_wait_method(lc_consumer_service *self, lc_error *error);
void lc_consumer_service_close_method(lc_consumer_service *self);

int lc_xid_is_valid(const char *value);

#endif
