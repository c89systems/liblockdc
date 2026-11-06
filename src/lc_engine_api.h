#ifndef LC_ENGINE_INTERNAL_API_H
#define LC_ENGINE_INTERNAL_API_H

/*
 * Private implementation interface for the built-in HTTP transport.
 *
 * This is not a second public API. The only supported public client contract is
 * declared in include/lc/lc.h. Everything in this header is internal plumbing
 * used by the concrete SDK implementation under src/.
 */

#include "lc/lc.h"

#include <pslog.h>
#include <stddef.h>
#include <time.h>

typedef struct lc_engine_client lc_engine_client;
typedef struct lc_engine_error lc_engine_error;

typedef void *(*lc_engine_malloc_fn)(void *context, size_t size);
typedef void *(*lc_engine_realloc_fn)(void *context, void *ptr, size_t size);
typedef void (*lc_engine_free_fn)(void *context, void *ptr);
typedef size_t (*lc_engine_read_callback)(void *context, void *buffer,
                                          size_t count, lc_engine_error *error);
typedef int (*lc_engine_reset_callback)(void *context, lc_engine_error *error);
typedef int (*lc_engine_write_callback)(void *context, const void *bytes,
                                        size_t count, lc_engine_error *error);

enum {
  LC_ENGINE_OK = 0,
  LC_ENGINE_ERROR_INVALID_ARGUMENT = 1,
  LC_ENGINE_ERROR_NO_MEMORY = 2,
  LC_ENGINE_ERROR_TRANSPORT = 3,
  LC_ENGINE_ERROR_PROTOCOL = 4,
  LC_ENGINE_ERROR_SERVER = 5
};

typedef struct lc_engine_allocator {
  lc_engine_malloc_fn malloc_fn;
  lc_engine_realloc_fn realloc_fn;
  lc_engine_free_fn free_fn;
  void *context;
} lc_engine_allocator;

typedef struct lc_engine_client_config {
  const char *const *endpoints;
  size_t endpoint_count;
  const char *unix_socket_path;
  lc_source *client_bundle_source;
  const char *client_bundle_path;
  const char *default_namespace;
  long timeout_ms;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  size_t http_json_response_limit_bytes;
  pslog_logger *logger;
  int disable_logger_sys_field;
  lc_engine_allocator allocator;
} lc_engine_client_config;

struct lc_engine_error {
  int code;
  long http_status;
  char *message;
  char *server_error_code;
  char *detail;
  char *leader_endpoint;
  char *current_etag;
  long current_version;
  long retry_after_seconds;
  char *correlation_id;
};

typedef struct lc_engine_acquire_request {
  const char *ns;
  const char *key;
  const char *owner;
  lonejson_int64 ttl_seconds;
  lonejson_int64 block_seconds;
  int if_not_exists;
  const char *txn_id;
} lc_engine_acquire_request;

typedef struct lc_engine_acquire_response {
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 fencing_token;
  char *correlation_id;
} lc_engine_acquire_response;

typedef struct lc_engine_get_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  lonejson_int64 fencing_token;
  int public_read;
} lc_engine_get_request;

typedef struct lc_engine_get_response {
  unsigned char *body;
  size_t body_length;
  int no_content;
  char *content_type;
  char *etag;
  lonejson_int64 version;
  lonejson_int64 fencing_token;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_get_response;

typedef struct lc_engine_get_stream_response {
  int no_content;
  char *content_type;
  char *etag;
  lonejson_int64 version;
  lonejson_int64 fencing_token;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_get_stream_response;

typedef struct lc_engine_keepalive_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 ttl_seconds;
  lonejson_int64 fencing_token;
} lc_engine_keepalive_request;

typedef struct lc_engine_keepalive_response {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  char *correlation_id;
} lc_engine_keepalive_response;

typedef struct lc_engine_release_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  int rollback;
} lc_engine_release_request;

typedef struct lc_engine_release_response {
  int released;
  char *correlation_id;
} lc_engine_release_response;

typedef struct lc_engine_update_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *if_state_etag;
  lonejson_int64 if_version;
  int has_if_version;
  const void *body;
  size_t body_length;
  const char *content_type;
  int has_retention_seconds;
  long retention_seconds;
} lc_engine_update_request;

typedef struct lc_engine_update_response {
  lonejson_int64 new_version;
  char *new_state_etag;
  lonejson_int64 bytes;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_update_response;

typedef struct lc_engine_mutate_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *if_state_etag;
  lonejson_int64 if_version;
  int has_if_version;
  const char *const *mutations;
  size_t mutation_count;
  int has_retention_seconds;
  long retention_seconds;
} lc_engine_mutate_request;

typedef struct lc_engine_mutate_response {
  lonejson_int64 new_version;
  char *new_state_etag;
  lonejson_int64 bytes;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_mutate_response;

typedef struct lc_engine_metadata_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  lonejson_int64 if_version;
  int has_if_version;
  int has_query_hidden;
  int query_hidden;
  int has_retention_seconds;
  long retention_seconds;
} lc_engine_metadata_request;

typedef struct lc_engine_metadata_response {
  char *ns;
  char *key;
  lonejson_int64 version;
  int has_query_hidden;
  int query_hidden;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_metadata_response;

typedef struct lc_engine_remove_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *if_state_etag;
  lonejson_int64 if_version;
  int has_if_version;
} lc_engine_remove_request;

typedef struct lc_engine_remove_response {
  int removed;
  lonejson_int64 new_version;
  char *correlation_id;
} lc_engine_remove_response;

typedef struct lc_engine_describe_request {
  const char *ns;
  const char *key;
} lc_engine_describe_request;

typedef struct lc_engine_describe_response {
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  lonejson_int64 expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  char *correlation_id;
  lc_state_retention_support retention_support;
  int has_retention_expires_at_unix;
  lc_unix_seconds retention_expires_at_unix;
} lc_engine_describe_response;

typedef struct lc_engine_query_request {
  const char *ns;
  const char *selector_json;
  lonejson_int64 limit;
  const char *cursor;
  const char *fields_json;
  const char *return_mode;
  const char *engine;
  const char *refresh;
} lc_engine_query_request;

typedef struct lc_engine_query_stream_response {
  char *cursor;
  char *correlation_id;
  char *metadata_json;
  char *return_mode;
  lc_index_seq index_seq;
  int index_seq_present;
  long http_status;
} lc_engine_query_stream_response;

typedef struct lc_engine_query_key_handler {
  int (*begin)(void *context, lc_engine_error *error);
  int (*chunk)(void *context, const char *bytes, size_t len,
               lc_engine_error *error);
  int (*end)(void *context, lc_engine_error *error);
} lc_engine_query_key_handler;

typedef struct lc_engine_enqueue_request {
  const char *ns;
  const char *queue;
  lonejson_int64 delay_seconds;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 ttl_seconds;
  int max_attempts;
  const char *payload_content_type;
} lc_engine_enqueue_request;

typedef struct lc_engine_enqueue_response {
  char *ns;
  char *queue;
  char *message_id;
  int attempts;
  int max_attempts;
  int failure_attempts;
  lonejson_int64 not_visible_until_unix;
  long visibility_timeout_seconds;
  long payload_bytes;
  char *correlation_id;
} lc_engine_enqueue_response;

typedef struct lc_engine_dequeue_request {
  const char *ns;
  const char *queue;
  const char *owner;
  const char *txn_id;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 wait_seconds;
  int page_size;
  const char *start_after;
} lc_engine_dequeue_request;

typedef struct lc_engine_dequeue_response {
  char *ns;
  char *queue;
  char *message_id;
  int attempts;
  int max_attempts;
  int failure_attempts;
  lonejson_int64 not_visible_until_unix;
  lonejson_int64 visibility_timeout_seconds;
  char *payload_content_type;
  lc_source *payload;
  size_t payload_length;
  char *correlation_id;
  char *lease_id;
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 fencing_token;
  char *txn_id;
  char *meta_etag;
  char *state_etag;
  char *state_lease_id;
  lonejson_int64 state_lease_expires_at_unix;
  lonejson_int64 state_fencing_token;
  char *state_txn_id;
  char *next_cursor;
} lc_engine_dequeue_response;

typedef struct lc_engine_queue_stats_request {
  const char *ns;
  const char *queue;
} lc_engine_queue_stats_request;

typedef struct lc_engine_queue_stats_response {
  char *ns;
  char *queue;
  int waiting_consumers;
  int pending_candidates;
  int total_consumers;
  int has_active_watcher;
  int available;
  char *head_message_id;
  lonejson_int64 head_enqueued_at_unix;
  lonejson_int64 head_not_visible_until_unix;
  lonejson_int64 head_age_seconds;
  char *correlation_id;
} lc_engine_queue_stats_response;

typedef struct lc_engine_queue_ack_request {
  const char *ns;
  const char *queue;
  const char *message_id;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *meta_etag;
  const char *state_etag;
  const char *state_lease_id;
  lonejson_int64 state_fencing_token;
} lc_engine_queue_ack_request;

typedef struct lc_engine_queue_ack_response {
  int acked;
  char *correlation_id;
} lc_engine_queue_ack_response;

typedef struct lc_engine_queue_nack_request {
  const char *ns;
  const char *queue;
  const char *message_id;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *meta_etag;
  const char *state_etag;
  lonejson_int64 delay_seconds;
  const char *intent;
  const char *last_error_json;
  const char *state_lease_id;
  lonejson_int64 state_fencing_token;
} lc_engine_queue_nack_request;

typedef struct lc_engine_queue_nack_response {
  int requeued;
  char *meta_etag;
  char *correlation_id;
} lc_engine_queue_nack_response;

typedef struct lc_engine_queue_extend_request {
  const char *ns;
  const char *queue;
  const char *message_id;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *meta_etag;
  lonejson_int64 extend_by_seconds;
  const char *state_lease_id;
  lonejson_int64 state_fencing_token;
} lc_engine_queue_extend_request;

typedef struct lc_engine_queue_extend_response {
  lc_unix_seconds lease_expires_at_unix;
  long visibility_timeout_seconds;
  char *meta_etag;
  lc_unix_seconds state_lease_expires_at_unix;
  char *correlation_id;
} lc_engine_queue_extend_response;

typedef struct lc_engine_attachment_selector {
  const char *id;
  const char *name;
} lc_engine_attachment_selector;

typedef struct lc_engine_attachment_info {
  char *id;
  char *name;
  long size;
  char *plaintext_sha256;
  char *content_type;
  lonejson_int64 created_at_unix;
  lonejson_int64 updated_at_unix;
} lc_engine_attachment_info;

typedef struct lc_engine_attach_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  const char *name;
  const char *content_type;
  lonejson_int64 max_bytes;
  int has_max_bytes;
  int prevent_overwrite;
} lc_engine_attach_request;

typedef struct lc_engine_attach_response {
  lc_engine_attachment_info attachment;
  int noop;
  lonejson_int64 version;
  char *correlation_id;
} lc_engine_attach_response;

typedef struct lc_engine_list_attachments_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  int public_read;
} lc_engine_list_attachments_request;

typedef struct lc_engine_list_attachments_response {
  char *ns;
  char *key;
  lc_engine_attachment_info *attachments;
  size_t attachment_count;
  char *correlation_id;
} lc_engine_list_attachments_response;

typedef struct lc_engine_get_attachment_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  int public_read;
  lc_engine_attachment_selector selector;
} lc_engine_get_attachment_request;

typedef struct lc_engine_get_attachment_response {
  lc_engine_attachment_info attachment;
  char *correlation_id;
} lc_engine_get_attachment_response;

typedef struct lc_engine_delete_attachment_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
  lc_engine_attachment_selector selector;
} lc_engine_delete_attachment_request;

typedef struct lc_engine_delete_attachment_response {
  int deleted;
  lonejson_int64 version;
  char *correlation_id;
} lc_engine_delete_attachment_response;

typedef struct lc_engine_delete_all_attachments_request {
  const char *ns;
  const char *key;
  const char *lease_id;
  const char *txn_id;
  lonejson_int64 fencing_token;
} lc_engine_delete_all_attachments_request;

typedef struct lc_engine_delete_all_attachments_response {
  int deleted;
  lonejson_int64 version;
  char *correlation_id;
} lc_engine_delete_all_attachments_response;

typedef struct lc_engine_watch_queue_request {
  const char *ns;
  const char *queue;
} lc_engine_watch_queue_request;

typedef struct lc_engine_queue_watch_event {
  char *ns;
  char *queue;
  int available;
  char *head_message_id;
  lonejson_int64 changed_at_unix;
  char *correlation_id;
} lc_engine_queue_watch_event;

typedef int (*lc_engine_queue_watch_handler)(
    void *context, const lc_engine_queue_watch_event *event,
    lc_engine_error *error);

typedef int (*lc_engine_queue_delivery_begin_handler)(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error);
typedef int (*lc_engine_queue_delivery_chunk_handler)(void *context,
                                                      const void *bytes,
                                                      size_t count,
                                                      lc_engine_error *error);
typedef int (*lc_engine_queue_delivery_end_handler)(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error);

typedef struct lc_engine_queue_stream_handler {
  lc_engine_queue_delivery_begin_handler begin;
  lc_engine_queue_delivery_chunk_handler chunk;
  lc_engine_queue_delivery_end_handler end;
} lc_engine_queue_stream_handler;

typedef struct lc_engine_string_array {
  char **items;
  size_t count;
} lc_engine_string_array;

typedef struct lc_engine_namespace_config_request {
  const char *ns;
  const char *preferred_engine;
  const char *fallback_engine;
  const char *if_etag;
} lc_engine_namespace_config_request;

typedef struct lc_engine_namespace_config_response {
  char *ns;
  char *preferred_engine;
  char *fallback_engine;
  char *etag;
  char *correlation_id;
} lc_engine_namespace_config_response;

typedef struct lc_engine_index_flush_request {
  const char *ns;
  const char *mode;
} lc_engine_index_flush_request;

typedef struct lc_engine_index_flush_response {
  char *ns;
  char *mode;
  char *flush_id;
  int accepted;
  int flushed;
  int pending;
  lc_index_seq index_seq;
  char *correlation_id;
} lc_engine_index_flush_response;

typedef struct lc_engine_txn_participant {
  const char *ns;
  const char *key;
  const char *backend_hash;
} lc_engine_txn_participant;

typedef struct lc_engine_txn_replay_request {
  const char *txn_id;
} lc_engine_txn_replay_request;

typedef struct lc_engine_txn_replay_response {
  char *txn_id;
  char *state;
  char *correlation_id;
} lc_engine_txn_replay_response;

typedef struct lc_engine_txn_decision_request {
  const char *txn_id;
  const char *state;
  const lc_engine_txn_participant *participants;
  size_t participant_count;
  lonejson_int64 expires_at_unix;
  lc_tc_term tc_term;
  const char *target_backend_hash;
} lc_engine_txn_decision_request;

typedef struct lc_engine_txn_decision_response {
  char *txn_id;
  char *state;
  char *correlation_id;
} lc_engine_txn_decision_response;

typedef struct lc_engine_tc_lease_acquire_request {
  const char *candidate_id;
  const char *candidate_endpoint;
  lc_tc_term term;
  lonejson_int64 ttl_ms;
} lc_engine_tc_lease_acquire_request;

typedef struct lc_engine_tc_lease_acquire_response {
  int granted;
  char *leader_id;
  char *leader_endpoint;
  lc_tc_term term;
  lonejson_int64 expires_at_unix;
  char *correlation_id;
} lc_engine_tc_lease_acquire_response;

typedef struct lc_engine_tc_lease_renew_request {
  const char *leader_id;
  lc_tc_term term;
  lonejson_int64 ttl_ms;
} lc_engine_tc_lease_renew_request;

typedef struct lc_engine_tc_lease_renew_response {
  int renewed;
  char *leader_id;
  char *leader_endpoint;
  lc_tc_term term;
  lonejson_int64 expires_at_unix;
  char *correlation_id;
} lc_engine_tc_lease_renew_response;

typedef struct lc_engine_tc_lease_release_request {
  const char *leader_id;
  lc_tc_term term;
} lc_engine_tc_lease_release_request;

typedef struct lc_engine_tc_lease_release_response {
  int released;
  char *correlation_id;
} lc_engine_tc_lease_release_response;

typedef struct lc_engine_tc_leader_response {
  char *leader_id;
  char *leader_endpoint;
  lc_tc_term term;
  lonejson_int64 expires_at_unix;
  char *correlation_id;
} lc_engine_tc_leader_response;

typedef struct lc_engine_tc_cluster_announce_request {
  const char *self_endpoint;
} lc_engine_tc_cluster_announce_request;

typedef struct lc_engine_tc_cluster_response {
  lc_engine_string_array endpoints;
  lonejson_int64 updated_at_unix;
  lonejson_int64 expires_at_unix;
  char *correlation_id;
} lc_engine_tc_cluster_response;

typedef struct lc_engine_tcrm_register_request {
  const char *backend_hash;
  const char *endpoint;
} lc_engine_tcrm_register_request;

typedef struct lc_engine_tcrm_register_response {
  char *backend_hash;
  lc_engine_string_array endpoints;
  lonejson_int64 updated_at_unix;
  char *correlation_id;
} lc_engine_tcrm_register_response;

typedef struct lc_engine_tcrm_unregister_request {
  const char *backend_hash;
  const char *endpoint;
} lc_engine_tcrm_unregister_request;

typedef struct lc_engine_tcrm_unregister_response {
  char *backend_hash;
  lc_engine_string_array endpoints;
  lonejson_int64 updated_at_unix;
  char *correlation_id;
} lc_engine_tcrm_unregister_response;

typedef struct lc_engine_tcrm_backend {
  char *backend_hash;
  lc_engine_string_array endpoints;
  lonejson_int64 updated_at_unix;
} lc_engine_tcrm_backend;

typedef struct lc_engine_tcrm_list_response {
  lc_engine_tcrm_backend *backends;
  size_t backend_count;
  lonejson_int64 updated_at_unix;
  char *correlation_id;
} lc_engine_tcrm_list_response;

const char *lc_engine_version_string(void);
void lc_engine_client_config_init(lc_engine_client_config *config);
void lc_engine_error_init(lc_engine_error *error);
void lc_engine_error_cleanup(lc_engine_error *error);
/* Applies one absolute monotonic deadline to every endpoint attempt made by
 * this private, one-shot client. A NULL value clears that constraint. */
void lc_engine_client_set_request_deadline(lc_engine_client *client,
                                           const struct timespec *deadline);
int lc_engine_client_open(const lc_engine_client_config *config,
                          lc_engine_client **out_client,
                          lc_engine_error *error);
pslog_logger *lc_engine_client_logger(lc_engine_client *client);
void lc_engine_client_set_cancel_check(lc_engine_client *client,
                                       int (*cancel_check)(void *context),
                                       void *cancel_context);
void lc_engine_client_close(lc_engine_client *client);
int lc_engine_client_set_allocator(lc_engine_client *client,
                                   const lc_engine_allocator *allocator,
                                   lc_engine_error *error);
int lc_engine_client_get_allocator(lc_engine_client *client,
                                   lc_engine_allocator *allocator,
                                   lc_engine_error *error);

void lc_engine_acquire_response_cleanup(lc_engine_acquire_response *response);
void lc_engine_get_response_cleanup(lc_engine_get_response *response);
void lc_engine_get_stream_response_cleanup(
    lc_engine_get_stream_response *response);
void lc_engine_keepalive_response_cleanup(
    lc_engine_keepalive_response *response);
void lc_engine_release_response_cleanup(lc_engine_release_response *response);
void lc_engine_update_response_cleanup(lc_engine_update_response *response);
void lc_engine_mutate_response_cleanup(lc_engine_mutate_response *response);
void lc_engine_metadata_response_cleanup(lc_engine_metadata_response *response);
void lc_engine_remove_response_cleanup(lc_engine_remove_response *response);
void lc_engine_describe_response_cleanup(lc_engine_describe_response *response);
void lc_engine_query_stream_response_cleanup(
    lc_engine_client *client, lc_engine_query_stream_response *response);
void lc_engine_enqueue_response_cleanup(lc_engine_enqueue_response *response);
void lc_engine_dequeue_response_cleanup(lc_engine_dequeue_response *response);
void lc_engine_queue_stats_response_cleanup(
    lc_engine_queue_stats_response *response);
void lc_engine_queue_ack_response_cleanup(
    lc_engine_queue_ack_response *response);
void lc_engine_queue_nack_response_cleanup(
    lc_engine_queue_nack_response *response);
void lc_engine_queue_extend_response_cleanup(
    lc_engine_queue_extend_response *response);
void lc_engine_attachment_info_cleanup(lc_engine_attachment_info *info);
void lc_engine_attach_response_cleanup(lc_engine_attach_response *response);
void lc_engine_list_attachments_response_cleanup(
    lc_engine_list_attachments_response *response);
void lc_engine_get_attachment_response_cleanup(
    lc_engine_get_attachment_response *response);
void lc_engine_delete_attachment_response_cleanup(
    lc_engine_delete_attachment_response *response);
void lc_engine_delete_all_attachments_response_cleanup(
    lc_engine_delete_all_attachments_response *response);
void lc_engine_queue_watch_event_cleanup(lc_engine_queue_watch_event *event);
void lc_engine_string_array_cleanup(lc_engine_string_array *array);
void lc_engine_namespace_config_response_cleanup(
    lc_engine_namespace_config_response *response);
void lc_engine_index_flush_response_cleanup(
    lc_engine_index_flush_response *response);
void lc_engine_txn_replay_response_cleanup(
    lc_engine_txn_replay_response *response);
void lc_engine_txn_decision_response_cleanup(
    lc_engine_txn_decision_response *response);
void lc_engine_tc_lease_acquire_response_cleanup(
    lc_engine_tc_lease_acquire_response *response);
void lc_engine_tc_lease_renew_response_cleanup(
    lc_engine_tc_lease_renew_response *response);
void lc_engine_tc_lease_release_response_cleanup(
    lc_engine_tc_lease_release_response *response);
void lc_engine_tc_leader_response_cleanup(
    lc_engine_tc_leader_response *response);
void lc_engine_tc_cluster_response_cleanup(
    lc_engine_tc_cluster_response *response);
void lc_engine_tcrm_register_response_cleanup(
    lc_engine_tcrm_register_response *response);
void lc_engine_tcrm_unregister_response_cleanup(
    lc_engine_tcrm_unregister_response *response);
void lc_engine_tcrm_list_response_cleanup(
    lc_engine_tcrm_list_response *response);

int lc_engine_client_acquire(lc_engine_client *client,
                             const lc_engine_acquire_request *request,
                             lc_engine_acquire_response *response,
                             lc_engine_error *error);
int lc_engine_client_get(lc_engine_client *client,
                         const lc_engine_get_request *request,
                         lc_engine_get_response *response,
                         lc_engine_error *error);
int lc_engine_client_get_into(lc_engine_client *client,
                              const lc_engine_get_request *request,
                              lc_engine_write_callback writer,
                              void *writer_context,
                              lc_engine_get_stream_response *response,
                              lc_engine_error *error);
int lc_engine_client_keepalive(lc_engine_client *client,
                               const lc_engine_keepalive_request *request,
                               lc_engine_keepalive_response *response,
                               lc_engine_error *error);
int lc_engine_client_release(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_release_response *response,
                             lc_engine_error *error);
int lc_engine_client_update_from(lc_engine_client *client,
                                 const lc_engine_update_request *request,
                                 lc_engine_read_callback reader,
                                 void *reader_context,
                                 lc_engine_update_response *response,
                                 lc_engine_error *error);
int lc_engine_client_mutate(lc_engine_client *client,
                            const lc_engine_mutate_request *request,
                            lc_engine_mutate_response *response,
                            lc_engine_error *error);
int lc_engine_client_update_metadata(lc_engine_client *client,
                                     const lc_engine_metadata_request *request,
                                     lc_engine_metadata_response *response,
                                     lc_engine_error *error);
int lc_engine_client_remove(lc_engine_client *client,
                            const lc_engine_remove_request *request,
                            lc_engine_remove_response *response,
                            lc_engine_error *error);
int lc_engine_client_describe(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_describe_response *response,
                              lc_engine_error *error);
int lc_engine_client_query_into(lc_engine_client *client,
                                const lc_engine_query_request *request,
                                lc_engine_write_callback writer,
                                void *writer_context,
                                lc_engine_query_stream_response *response,
                                lc_engine_error *error);
int lc_engine_client_query_keys(lc_engine_client *client,
                                const lc_engine_query_request *request,
                                const lc_engine_query_key_handler *handler,
                                void *handler_context,
                                lc_engine_query_stream_response *response,
                                lc_engine_error *error);
int lc_engine_client_enqueue_from(lc_engine_client *client,
                                  const lc_engine_enqueue_request *request,
                                  lc_engine_read_callback reader,
                                  void *reader_context,
                                  lc_engine_enqueue_response *response,
                                  lc_engine_error *error);
int lc_engine_client_dequeue_into(lc_engine_client *client,
                                  const lc_engine_dequeue_request *request,
                                  const lc_engine_queue_stream_handler *handler,
                                  void *handler_context,
                                  lc_engine_error *error);
int lc_engine_client_dequeue_with_state_into(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error);
int lc_engine_client_queue_stats(lc_engine_client *client,
                                 const lc_engine_queue_stats_request *request,
                                 lc_engine_queue_stats_response *response,
                                 lc_engine_error *error);
int lc_engine_client_queue_ack(lc_engine_client *client,
                               const lc_engine_queue_ack_request *request,
                               lc_engine_queue_ack_response *response,
                               lc_engine_error *error);
int lc_engine_client_queue_nack(lc_engine_client *client,
                                const lc_engine_queue_nack_request *request,
                                lc_engine_queue_nack_response *response,
                                lc_engine_error *error);
int lc_engine_client_queue_extend(lc_engine_client *client,
                                  const lc_engine_queue_extend_request *request,
                                  lc_engine_queue_extend_response *response,
                                  lc_engine_error *error);
int lc_engine_client_attach_from(lc_engine_client *client,
                                 const lc_engine_attach_request *request,
                                 lc_engine_read_callback reader,
                                 void *reader_context,
                                 lc_engine_attach_response *response,
                                 lc_engine_error *error);
int lc_engine_client_list_attachments(
    lc_engine_client *client, const lc_engine_list_attachments_request *request,
    lc_engine_list_attachments_response *response, lc_engine_error *error);
int lc_engine_client_get_attachment_into(
    lc_engine_client *client, const lc_engine_get_attachment_request *request,
    lc_engine_write_callback writer, void *writer_context,
    lc_engine_get_attachment_response *response, lc_engine_error *error);
int lc_engine_client_delete_attachment(
    lc_engine_client *client,
    const lc_engine_delete_attachment_request *request,
    lc_engine_delete_attachment_response *response, lc_engine_error *error);
int lc_engine_client_delete_all_attachments(
    lc_engine_client *client,
    const lc_engine_delete_all_attachments_request *request,
    lc_engine_delete_all_attachments_response *response,
    lc_engine_error *error);
int lc_engine_client_watch_queue(lc_engine_client *client,
                                 const lc_engine_watch_queue_request *request,
                                 lc_engine_queue_watch_handler handler,
                                 void *handler_context, lc_engine_error *error);
int lc_engine_client_subscribe(lc_engine_client *client,
                               const lc_engine_dequeue_request *request,
                               const lc_engine_queue_stream_handler *handler,
                               void *handler_context, lc_engine_error *error);
int lc_engine_client_subscribe_with_state(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error);
int lc_engine_client_get_namespace_config(
    lc_engine_client *client, const char *ns,
    lc_engine_namespace_config_response *response, lc_engine_error *error);
int lc_engine_client_update_namespace_config(
    lc_engine_client *client, const lc_engine_namespace_config_request *request,
    lc_engine_namespace_config_response *response, lc_engine_error *error);
int lc_engine_client_index_flush(lc_engine_client *client,
                                 const lc_engine_index_flush_request *request,
                                 lc_engine_index_flush_response *response,
                                 lc_engine_error *error);
int lc_engine_client_txn_replay(lc_engine_client *client,
                                const lc_engine_txn_replay_request *request,
                                lc_engine_txn_replay_response *response,
                                lc_engine_error *error);
int lc_engine_client_txn_decide(lc_engine_client *client,
                                const lc_engine_txn_decision_request *request,
                                lc_engine_txn_decision_response *response,
                                lc_engine_error *error);
int lc_engine_client_txn_commit(lc_engine_client *client,
                                const lc_engine_txn_decision_request *request,
                                lc_engine_txn_decision_response *response,
                                lc_engine_error *error);
int lc_engine_client_txn_rollback(lc_engine_client *client,
                                  const lc_engine_txn_decision_request *request,
                                  lc_engine_txn_decision_response *response,
                                  lc_engine_error *error);
int lc_engine_client_tc_lease_acquire(
    lc_engine_client *client, const lc_engine_tc_lease_acquire_request *request,
    lc_engine_tc_lease_acquire_response *response, lc_engine_error *error);
int lc_engine_client_tc_lease_renew(
    lc_engine_client *client, const lc_engine_tc_lease_renew_request *request,
    lc_engine_tc_lease_renew_response *response, lc_engine_error *error);
int lc_engine_client_tc_lease_release(
    lc_engine_client *client, const lc_engine_tc_lease_release_request *request,
    lc_engine_tc_lease_release_response *response, lc_engine_error *error);
int lc_engine_client_tc_leader(lc_engine_client *client,
                               lc_engine_tc_leader_response *response,
                               lc_engine_error *error);
int lc_engine_client_tc_cluster_announce(
    lc_engine_client *client,
    const lc_engine_tc_cluster_announce_request *request,
    lc_engine_tc_cluster_response *response, lc_engine_error *error);
int lc_engine_client_tc_cluster_leave(lc_engine_client *client,
                                      lc_engine_tc_cluster_response *response,
                                      lc_engine_error *error);
int lc_engine_client_tc_cluster_list(lc_engine_client *client,
                                     lc_engine_tc_cluster_response *response,
                                     lc_engine_error *error);
int lc_engine_client_tcrm_register(
    lc_engine_client *client, const lc_engine_tcrm_register_request *request,
    lc_engine_tcrm_register_response *response, lc_engine_error *error);
int lc_engine_client_tcrm_unregister(
    lc_engine_client *client, const lc_engine_tcrm_unregister_request *request,
    lc_engine_tcrm_unregister_response *response, lc_engine_error *error);
int lc_engine_client_tcrm_list(lc_engine_client *client,
                               lc_engine_tcrm_list_response *response,
                               lc_engine_error *error);
int lc_engine_parse_attach_response_json(const char *json,
                                         const char *correlation_id,
                                         lc_engine_attach_response *response,
                                         lc_engine_error *error);
int lc_engine_parse_list_attachments_response_json(
    const char *json, const char *correlation_id,
    lc_engine_list_attachments_response *response, lc_engine_error *error);
int lc_engine_parse_subscribe_meta_json(const char *json,
                                        const char *fallback_correlation_id,
                                        lc_engine_dequeue_response *response,
                                        lc_engine_error *error);

#endif
