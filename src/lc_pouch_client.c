#include "lc_api_internal.h"
#include "lc_log.h"
#include "lc_mutate_stream.h"
#include "lc_pouch.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"
#include "lc_pouch_query_plan.h"
#include "lc_pouch_retention.h"
#include "lc_pouch_xa.h"

#include "lc_internal.h"

#include <lql/lql.h>

#include "lc_xa_internal.h"
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_POUCH_QUERY_DEFAULT_LIMIT 100L
#define LC_POUCH_QUERY_MAX_LIMIT 1000L
#define LC_POUCH_QUERY_SCAN_SUMMARY_DEFAULT_PAGE 2048U
#define LC_POUCH_QUERY_SCAN_SUMMARY_MIN_PAGE 512U
#define LC_POUCH_QUERY_SCAN_SUMMARY_MAX_PAGE 4096U
#define LC_POUCH_QUERY_STREAM_READER_BUFFER_BYTES (16U * 1024U)
#define LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE                                \
  "application/x-lockdc-pouch-attachment-delete"
#define LC_POUCH_NAMESPACE_CONFIG_KEY "config/namespace"
#define LC_POUCH_NAMESPACE_CONFIG_CONTENT_TYPE                                 \
  "application/x-lockdc-pouch-namespace-config"
#define LC_POUCH_TXN_NAMESPACE ".txns"
#define LC_POUCH_CONTROL_NAMESPACE ".lockd"
#define LC_POUCH_TC_LEADER_KEY "tc/leader"
#define LC_POUCH_TC_CLUSTER_PREFIX "tc-cluster/leases/"
#define LC_POUCH_TC_RM_MEMBERS_KEY "tc-rm-members"
#define LC_POUCH_TC_CONTENT_TYPE "application/x-lockdc-pouch-tc"
#define LC_POUCH_LEASE_CONTENT_TYPE "application/x-lockdc-pouch-lease"
#define LC_POUCH_QUEUE_RECORD_MAGIC "LPQ1"
#define LC_POUCH_LEASE_RECORD_MAGIC "LPL2"
#define LC_POUCH_TXN_RECORD_MAGIC "LPT3"
#define LC_POUCH_ATTACHMENT_METADATA_MAGIC "LPA1"
#define LC_POUCH_TC_RECORD_MAGIC "LPC1"
#define LC_POUCH_TC_CLUSTER_RECORD_MAGIC "LCC1"
#define LC_POUCH_TC_RM_RECORD_MAGIC "LCR1"
#define LC_POUCH_NAMESPACE_CONFIG_MAGIC "LPN1"
#define LC_POUCH_CONTROL_STRING_MAX 65535U
#define LC_POUCH_CONTROL_HEADER_MAX (64U * 1024U)
#define LC_POUCH_ATTACHMENT_METADATA_BYTES 12U
#define LC_POUCH_TXN_GUARD_STRIPES 256U

static pthread_mutex_t lc_pouch_queue_message_id_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lc_pouch_lease_id_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t lc_pouch_lease_id_counter = 0U;
static pthread_mutex_t lc_pouch_txn_guard_mutexes[LC_POUCH_TXN_GUARD_STRIPES];
static pthread_once_t lc_pouch_txn_guard_once = PTHREAD_ONCE_INIT;

static void lc_pouch_txn_guard_mutexes_init(void) {
  size_t i;

  for (i = 0U; i < LC_POUCH_TXN_GUARD_STRIPES; ++i) {
    (void)pthread_mutex_init(&lc_pouch_txn_guard_mutexes[i], NULL);
  }
}

#ifdef LOCKDC_TEST_BUILD
int (*lc_pouch_test_queue_clock_gettime)(clockid_t clock_id,
                                         struct timespec *out,
                                         void *context) = NULL;
void (*lc_pouch_test_queue_poll_delay)(const struct timespec *delay,
                                       void *context) = NULL;
void *lc_pouch_test_queue_time_context = NULL;

static int lc_pouch_queue_clock_gettime(clockid_t clock_id,
                                        struct timespec *out) {
  if (lc_pouch_test_queue_clock_gettime != NULL) {
    return lc_pouch_test_queue_clock_gettime(clock_id, out,
                                             lc_pouch_test_queue_time_context);
  }
  return clock_gettime(clock_id, out);
}
#else
#define lc_pouch_queue_clock_gettime clock_gettime
#endif

#ifdef LOCKDC_TEST_BUILD
lc_pouch_test_after_acquire_claim_hook_fn
    lc_pouch_test_after_acquire_claim_hook = NULL;
void *lc_pouch_test_after_acquire_claim_context = NULL;
lc_pouch_test_before_acquire_poll_hook_fn
    lc_pouch_test_before_acquire_poll_hook = NULL;
void *lc_pouch_test_before_acquire_poll_context = NULL;
lc_pouch_test_after_dequeue_state_lease_hook_fn
    lc_pouch_test_after_dequeue_state_lease_hook = NULL;
void *lc_pouch_test_after_dequeue_state_lease_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_queue_lease_claim_hook = NULL;
void *lc_pouch_test_after_queue_lease_claim_context = NULL;
lc_pouch_test_before_queue_message_build_hook_fn
    lc_pouch_test_before_queue_message_build_hook = NULL;
void *lc_pouch_test_before_queue_message_build_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_queue_batch_message_build_hook = NULL;
void *lc_pouch_test_after_queue_batch_message_build_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_txn_decision_hook = NULL;
void *lc_pouch_test_before_txn_decision_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_txn_decision_hook = NULL;
void *lc_pouch_test_after_txn_decision_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_xa_owner_write_hook = NULL;
void *lc_pouch_test_before_xa_owner_write_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_xa_retire_owner_hook = NULL;
void *lc_pouch_test_after_xa_retire_owner_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_txn_replay_hook = NULL;
void *lc_pouch_test_before_txn_replay_context = NULL;
#endif

typedef struct lc_pouch_acquire_for_update_file {
  FILE *fp;
} lc_pouch_acquire_for_update_file;

typedef struct lc_pouch_lonejson_source {
  lonejson_generator generator;
  int initialized;
} lc_pouch_lonejson_source;

typedef struct lc_pouch_txn_buffer {
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_txn_buffer;

typedef struct lc_pouch_binary_cursor {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} lc_pouch_binary_cursor;

typedef struct lc_pouch_txn_key_list {
  char **keys;
  size_t count;
  size_t capacity;
} lc_pouch_txn_key_list;

static void lc_pouch_txn_buffer_cleanup(lc_pouch_txn_buffer *buffer);
static int lc_pouch_txn_buffer_append_bytes(lc_pouch_txn_buffer *buffer,
                                            const void *bytes, size_t length,
                                            lc_error *error);
static int lc_pouch_txn_buffer_append_string(lc_pouch_txn_buffer *buffer,
                                             const char *value,
                                             lc_error *error);
static int lc_pouch_txn_id_present(const char *txn_id);
static int lc_pouch_txn_metadata_has_lease_record(const unsigned char *bytes,
                                                  size_t length);
static int lc_pouch_txn_apply_state_participant(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const char *txn_id, const char *state,
    lc_pouch_unix_seconds commit_at_unix, lc_error *error);
static int lc_pouch_txn_validate_participants(const lc_txn_decision_req *req,
                                              lc_error *error);
static void lc_pouch_txn_trim_bounds(const char *value, const char **start,
                                     size_t *length);
static int
lc_pouch_txn_validate_retention(lc_client_handle *client, const char *txn_id,
                                const lc_txn_participant *participants,
                                size_t count, lc_pouch_unix_seconds commit_at,
                                lc_error *error);
static int lc_pouch_binary_cursor_magic(lc_pouch_binary_cursor *cursor,
                                        const char magic[4], lc_error *error);
static int lc_pouch_binary_cursor_string(lc_pouch_binary_cursor *cursor,
                                         char **out, lc_error *error);
static int lc_pouch_queue_is_message_lease_key(const char *key);
static char *lc_pouch_query_dup_bytes(const char *bytes, size_t length,
                                      lc_error *error);

typedef struct lc_pouch_mutate_file {
  FILE *fp;
  int found;
  char *etag;
  lc_pouch_generation version;
} lc_pouch_mutate_file;

typedef struct lc_pouch_namespace_config_record {
  int found;
  char *etag;
  char preferred_engine[sizeof("index")];
  char fallback_engine[sizeof("scan")];
} lc_pouch_namespace_config_record;

typedef struct lc_pouch_tc_lease_record {
  int found;
  char *leader_id;
  char *leader_endpoint;
  lc_tc_term term;
  lc_pouch_unix_seconds expires_at_unix;
  lc_pouch_generation version;
} lc_pouch_tc_lease_record;

static void lc_pouch_tc_lease_record_cleanup(lc_pouch_tc_lease_record *record);
static int lc_pouch_xa_owner_read(lc_client_handle *client, const char *txn_id,
                                  lc_pouch_tc_lease_record *owner,
                                  lc_error *error);
static int lc_pouch_xa_owner_check(lc_client_handle *client, const char *txn_id,
                                   lc_tc_term term, const char *owner_id,
                                   int require_owner, int *sealed,
                                   lc_error *error);
static int lc_pouch_xa_managed_term(lc_client_handle *client,
                                    const char *txn_id, lc_tc_term *term,
                                    lc_error *error);
static int lc_pouch_xa_managed(lc_client_handle *client, const char *txn_id,
                               int *managed, lc_error *error);
static int lc_pouch_xa_seal_vote(lc_client_handle *client, const char *txn_id,
                                 lc_tc_term term, lc_error *error);
static int lc_pouch_xa_completed_read(lc_client_handle *client,
                                      const char *txn_id,
                                      lc_pouch_unix_seconds *completed,
                                      lc_error *error);
static int lc_pouch_tc_delete_key(lc_client_handle *client, const char *ns,
                                  const char *key, int has_expected_version,
                                  lc_pouch_generation expected_version,
                                  lc_error *error);

typedef struct lc_pouch_lease_record {
  int found;
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  long fencing_token;
  lc_pouch_unix_seconds expires_at_unix;
  lc_pouch_generation state_version;
  int txn_explicit;
  lc_pouch_generation version;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_lease_record;

typedef struct lc_pouch_acquire_context {
  lc_client_handle *client;
  const lc_acquire_req *req;
  const char *ns;
  lc_pouch_txn_buffer lease_metadata;
  lc_pouch_state_write_result lease_write_result;
  lc_pouch_unix_seconds lease_expires_at_unix;
  lc_pouch_unix_seconds held_until_unix;
  long fencing_token;
  lc_pouch_generation version;
  int state_found;
  int has_query_hidden;
  int query_hidden;
  int metadata_prepared;
  int acquired;
  int txn_explicit;
  char lease_id[128];
} lc_pouch_acquire_context;

typedef struct lc_pouch_release_context {
  lc_client_handle *client;
  const lc_release_op *req;
  const char *ns;
  lc_pouch_txn_buffer lease_metadata;
  lc_pouch_state_write_result write_result;
} lc_pouch_release_context;

typedef struct lc_pouch_keepalive_context {
  lc_client_handle *client;
  const lc_keepalive_op *req;
  const char *ns;
  lc_pouch_unix_seconds lease_expires_at_unix;
  lc_pouch_generation state_version;
  lc_pouch_txn_buffer lease_metadata;
  lc_pouch_state_write_result write_result;
} lc_pouch_keepalive_context;

typedef struct lc_pouch_queue_lease_acquire_context {
  lc_client_handle *client;
  const lc_dequeue_req *req;
  const char *ns;
  const char *lease_key;
  const char *lease_id;
  lc_pouch_unix_seconds lease_expires_at_unix;
  lc_pouch_unix_seconds now_seconds;
  lc_pouch_txn_buffer lease_metadata;
  lc_pouch_state_write_result write_result;
  long fencing_token;
  int acquired;
  int write_prepared;
} lc_pouch_queue_lease_acquire_context;

typedef struct lc_pouch_lease_replace_context {
  lc_client_handle *client;
  const lc_lease_ref *lease;
  const char *ns;
  const char *key;
  lc_pouch_unix_seconds expires_at_unix;
  int clear_lease;
  int preserve_expiration;
  int has_state_version;
  lc_pouch_generation state_version;
  lc_pouch_txn_buffer lease_metadata;
  lc_pouch_state_write_result write_result;
} lc_pouch_lease_replace_context;

typedef struct lc_pouch_txn_queue_participant_context {
  lc_client_handle *client;
  const char *ns;
  const char *lease_key;
  const char *txn_id;
  const char *state;
  int state_participant;
} lc_pouch_txn_queue_participant_context;

typedef struct lc_pouch_txn_state_participant_context {
  lc_pouch *pouch;
  const lc_lease_ref *lease;
  const char *ns;
  const char *key;
  const char *txn_id;
  const char *state;
  lc_pouch_unix_seconds commit_at_unix;
  int retention_cleanup;
} lc_pouch_txn_state_participant_context;

typedef struct lc_pouch_tc_endpoint_list {
  char **items;
  size_t count;
  size_t capacity;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_unix_seconds expires_at_unix;
} lc_pouch_tc_endpoint_list;

typedef struct lc_pouch_tc_rm_registry {
  lc_tc_rm_backend *backends;
  size_t backend_count;
  size_t backend_capacity;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_generation version;
  int found;
} lc_pouch_tc_rm_registry;

typedef struct lc_pouch_attachment_key_ref {
  char *key;
  lc_pouch_generation version;
} lc_pouch_attachment_key_ref;

typedef struct lc_pouch_attachment_list_builder {
  char *prefix;
  size_t prefix_len;
  lc_attachment_info *items;
  size_t count;
  size_t capacity;
  lc_pouch_attachment_key_ref *keys;
  size_t key_count;
  size_t key_capacity;
} lc_pouch_attachment_list_builder;

typedef struct lc_pouch_staged_attachment_overlay {
  lc_pouch_attachment_list_builder *builder;
  const char *prefix;
  size_t prefix_len;
} lc_pouch_staged_attachment_overlay;

typedef struct lc_pouch_queue_record {
  char *storage_key;
  char *ns;
  char *queue;
  char *message_id;
  char *status;
  char *content_type;
  char *lease_id;
  char *lease_txn_id;
  char *meta_etag;
  long lease_fencing_token;
  lc_pouch_generation version;
  int attempts;
  int max_attempts;
  int failure_attempts;
  lc_pouch_unix_seconds enqueued_at_unix;
  long enqueued_at_nsec;
  uint64_t enqueue_sequence;
  lc_pouch_unix_seconds expires_at_unix;
  lc_pouch_unix_seconds not_visible_until_unix;
  long visibility_timeout_seconds;
  unsigned char *payload;
  size_t payload_length;
} lc_pouch_queue_record;

typedef struct lc_pouch_queue_scan {
  lc_client_handle *client;
  const char *ns;
  const char *prefix;
  size_t prefix_len;
  lc_pouch_queue_record *records;
  size_t count;
  size_t capacity;
} lc_pouch_queue_scan;

typedef struct lc_pouch_txn_record {
  char *state;
  lc_pouch_unix_seconds expires_at_unix;
  lc_pouch_unix_seconds commit_at_unix;
  lc_tc_term tc_term;
  char *target_backend_hash;
  lc_txn_participant *participants;
  unsigned char *votes;
  size_t participant_count;
  size_t participant_capacity;
} lc_pouch_txn_record;

typedef struct lc_pouch_txn_decision_context {
  lc_client_handle *client;
  const lc_txn_decision_req *request;
  const char *state;
  const char *key;
  const char *owner_id;
  int takeover;
  int owner_sealed;
  lc_pouch_txn_record record;
  lc_pouch_state_write_result write_result;
  lc_pouch_generation response_index;
  int apply_decision;
  lc_pouch_unix_seconds commit_at_unix;
} lc_pouch_txn_decision_context;

typedef struct lc_pouch_txn_vote_context {
  lc_client_handle *client;
  const char *txn_id;
  const char *ns;
  const char *participant_key;
  int rollback;
  int preparing;
  const char *record_key;
  lc_pouch_txn_record record;
  lc_pouch_state_write_result write_result;
  lc_pouch_generation response_index;
  int apply_decision;
  lc_pouch_unix_seconds commit_at_unix;
} lc_pouch_txn_vote_context;

typedef struct lc_pouch_txn_guard {
  int fd;
  pthread_mutex_t *mutex;
  int mutex_locked;
  lc_pouch_state_shared_mutation_guard *shared_mutation;
} lc_pouch_txn_guard;

typedef struct lc_pouch_implicit_txn_leader {
  char *ns;
  char *key;
  char *lease_id;
  long fencing_token;
  lc_pouch_unix_seconds expires_at_unix;
  size_t count;
  const char *txn_id;
  lc_pouch_unix_seconds now;
  lc_client_handle *client;
} lc_pouch_implicit_txn_leader;

static int lc_pouch_txn_record_vote(lc_client_handle *client,
                                    const char *txn_id, const char *ns,
                                    const char *participant_key, int rollback,
                                    int preparing, int *decision_ready,
                                    int *native_completion, lc_error *error);
static int lc_pouch_client_txn_decision(
    lc_client *self, const lc_txn_decision_req *req, const char *state,
    int tolerate_queue_lease_mismatch, const char *owner_id, int takeover,
    lc_txn_decision_res *out, lc_error *error);
static int lc_pouch_client_txn_replay_impl(lc_client *self,
                                           const lc_txn_replay_req *req,
                                           lc_txn_replay_res *out,
                                           lc_error *error);
static int
lc_pouch_xa_decision_authority(lc_pouch_txn_decision_context *context,
                               int *sealed, lc_error *error);
static int lc_pouch_xa_read_txn(lc_pouch *pouch, const char *txn_id,
                                lc_pouch_txn_record *record, lc_error *error);
static int
lc_pouch_retention_stage_begin(lc_client_handle *client,
                               const lc_lease_ref *lease, const char *ns,
                               const char *key, const char *txn_id,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_txn_guard *guard, lc_error *error);
static void lc_pouch_txn_guard_release(lc_pouch_txn_guard *guard);

typedef struct lc_pouch_query_source_reader {
  lc_source *source;
  unsigned char *buffer;
  size_t buffer_capacity;
  size_t buffer_offset;
  size_t buffer_length;
} lc_pouch_query_source_reader;

typedef struct lc_pouch_lease_precondition {
  lc_client_handle *client;
  const lc_lease_ref *lease;
  const char *ns;
  const char *key;
  lc_pouch_unix_seconds lease_expires_at_unix;
} lc_pouch_lease_precondition;

typedef struct lc_pouch_lease_write_context {
  lc_client_handle *client;
  const char *ns;
  const char *key;
  lc_pouch_state_write_options *options;
  lc_pouch_state_write_result *out;
} lc_pouch_lease_write_context;

typedef struct lc_pouch_txn_stage_write_context {
  lc_client_handle *client;
  const lc_lease_ref *lease;
  const char *ns;
  const char *key;
  const char *txn_id;
  lc_pouch_generation projected_version;
  int *removed;
} lc_pouch_txn_stage_write_context;

typedef struct lc_pouch_counting_source {
  lc_source *inner;
  uint64_t bytes;
  long max_bytes;
  int has_max_bytes;
} lc_pouch_counting_source;

typedef struct lc_pouch_mutation_prepare_context {
  lc_client_handle *client;
  const char *ns;
  const char *key;
  lc_mutation_plan *plan;
  lc_pouch_mutate_file mutated;
  lc_pouch_counting_source counting_source;
  lc_source *source;
} lc_pouch_mutation_prepare_context;

typedef struct lc_pouch_txn_mutation_context {
  lc_pouch_txn_stage_write_context stage;
  lc_pouch_mutation_prepare_context mutation;
} lc_pouch_txn_mutation_context;

static size_t lc_pouch_counting_source_read(void *context, void *buffer,
                                            size_t count, lc_error *error);
static int lc_pouch_counting_source_reset(void *context, lc_error *error);

typedef struct lc_pouch_enqueue_context {
  lc_client_handle *client;
  const char *ns;
  const lc_enqueue_req *req;
  lc_source *src;
  lc_enqueue_res *out;
} lc_pouch_enqueue_context;

typedef struct lc_pouch_attach_write_context {
  lc_client_handle *client;
  const lc_attach_op *req;
  const char *ns;
  const char *attachment_key;
  const char *staged_attachment_key;
  int stage_transaction;
  lc_source *source;
  lc_pouch_state_write_options *options;
  lc_pouch_state_write_result *result;
  lc_pouch_unix_seconds attachment_created_at_unix;
} lc_pouch_attach_write_context;

/* Attachment deletion changes both the parent lease projection and one or
 * more attachment rows. Keep that work under one namespace authority. */
typedef struct lc_pouch_attachment_delete_context {
  lc_client_handle *client;
  const lc_attachment_delete_op *req;
  const char *ns;
  const char *attachment_key;
  const char *staged_key;
  int stage_transaction;
  int *deleted;
} lc_pouch_attachment_delete_context;

typedef struct lc_pouch_attachment_delete_all_context {
  lc_client_handle *client;
  const lc_attachment_delete_all_op *req;
  const char *ns;
  int stage_transaction;
  int *deleted_count;
} lc_pouch_attachment_delete_all_context;

typedef struct lc_pouch_outbox_state_reclaim_attachment_context {
  lc_client_handle *client;
  const char *ns;
  const char *key;
  int *deleted_count;
} lc_pouch_outbox_state_reclaim_attachment_context;

typedef struct lc_pouch_query_match_state {
  int matched;
} lc_pouch_query_match_state;

typedef struct lc_pouch_query_any_text_match_state {
  lc_source *source;
  lc_error read_error;
  char *needle;
  size_t *prefix;
  size_t needle_len;
  size_t matched_len;
  int ignore_case;
  int matched;
} lc_pouch_query_any_text_match_state;

typedef struct lc_pouch_query_scan_scalar_match_state {
  lc_source *source;
  lc_error read_error;
  const struct lc_pouch_query_index_plan *plan;
  lc_pouch_query_any_text_match_state contains_matcher;
  char *scratch;
  size_t scratch_len;
  size_t scratch_capacity;
  char capture_type;
  int has_contains_matcher;
  int capturing;
  int matched;
  int stopped_after_match;
  int stopped_after_decision;
} lc_pouch_query_scan_scalar_match_state;

typedef struct lc_pouch_query_index_plan lc_pouch_query_index_plan;

typedef struct lc_pouch_query_scan_context {
  lc_client_handle *client;
  const char *ns;
  const lc_query_req *request;
  const lc_query_key_handler *handler;
  void *handler_context;
  lc_sink *sink;
  lql *runtime;
  const lql_selector *selector;
  /* Request-owned scratch, reused across candidates and released on return. */
  unsigned char *stream_buffer;
  const char *start_after_key;
  const char *candidate_key;
  char *last_emitted_key;
  size_t seen;
  size_t emitted;
  size_t matched;
  size_t limit;
  lc_pouch_generation index_seq;
  const char *any_text_contains_needle;
  int any_text_contains_ignore_case;
  const lc_pouch_query_index_plan *scan_scalar_plan;
  int track_index_seq;
  int page_full;
  int emit_documents;
  int indexed_candidates_exact;
} lc_pouch_query_scan_context;

static void lc_pouch_query_track_index_seq(lc_pouch_query_scan_context *context,
                                           lc_pouch_generation seq) {
  if (context != NULL && context->track_index_seq && seq > context->index_seq) {
    context->index_seq = seq;
  }
}

static int
lc_pouch_query_page_enter_candidate(lc_pouch_query_scan_context *context,
                                    int *active, lc_error *error);
static int
lc_pouch_query_page_accept_match(lc_pouch_query_scan_context *context,
                                 int *emit, int *stop, lc_error *error);
static int
lc_pouch_query_page_mark_emitted(lc_pouch_query_scan_context *context,
                                 lc_error *error);
static int
lc_pouch_query_run_index_predicate(lc_pouch_query_scan_context *scan,
                                   lc_pouch_generation *flushed_seq_out,
                                   lc_error *error);

struct lc_pouch_query_index_plan {
  char *field;
  char **values;
  char *value_types;
  size_t value_count;
  size_t value_capacity;
  lc_pouch_query_index_scalar_term *or_terms;
  size_t or_term_count;
  size_t or_term_capacity;
  int exists;
  int prefix;
  int contains;
  int ignore_case;
  int range;
  int date;
  int root_or;
  int candidates_exact;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
};

typedef struct lc_pouch_query_selector_scalar {
  char *value;
  char value_type;
} lc_pouch_query_selector_scalar;

typedef struct lc_pouch_query_selector_scalar_list {
  lc_pouch_query_selector_scalar *items;
  size_t count;
  size_t capacity;
  size_t cursor;
  char *scratch;
  size_t scratch_len;
  size_t scratch_capacity;
  int capturing;
  char capture_type;
} lc_pouch_query_selector_scalar_list;

typedef struct lc_pouch_query_index_key_set {
  lc_pouch_query_index_key_view *keys;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_key_set;

typedef struct lc_pouch_query_index_key_collect_context {
  lc_pouch_query_index_key_set *keys;
  int candidate_exact;
} lc_pouch_query_index_key_collect_context;

typedef struct lc_pouch_query_index_exact_document_page {
  lc_pouch_query_scan_context *scan;
  lc_pouch_query_index_key_set keys;
} lc_pouch_query_index_exact_document_page;

static size_t lc_pouch_lonejson_source_read(void *context, void *buffer,
                                            size_t count, lc_error *error) {
  lc_pouch_lonejson_source *source;
  lonejson_status status;
  size_t out_len;
  int out_eof;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL || !source->initialized) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch JSON generator source is closed", NULL, NULL, NULL);
    return 0U;
  }
  out_len = 0U;
  out_eof = 0;
  status = lonejson_generator_read(&source->generator, (unsigned char *)buffer,
                                   count, &out_len, &out_eof);
  if (status != LONEJSON_STATUS_OK) {
    (void)lc_lonejson_error_from_status(error, status, NULL,
                                        "failed to stream pouch JSON value");
    return 0U;
  }
  (void)out_eof;
  return out_len;
}

static void lc_pouch_lonejson_source_close(void *context) {
  lc_pouch_lonejson_source *source;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL) {
    return;
  }
  if (source->initialized) {
    lonejson_generator_cleanup(&source->generator);
  }
  lc_free_with_allocator(NULL, source);
}

static int lc_pouch_lonejson_source_open(const lonejson_map *map,
                                         const void *src, lc_source **out,
                                         lc_error *error) {
  lc_pouch_lonejson_source *context;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch JSON source requires output storage", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch JSON runtime", NULL, NULL,
                        NULL);
  }
  context = (lc_pouch_lonejson_source *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*context));
  if (context == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch JSON source", NULL, NULL,
                        NULL);
  }
  status = lonejson_generator_init(runtime, &context->generator, map, src);
  if (status != LONEJSON_STATUS_OK) {
    lc_free_with_allocator(NULL, context);
    return lc_lonejson_error_from_status(
        error, status, NULL, "failed to initialize pouch JSON generator");
  }
  context->initialized = 1;
  rc = lc_source_from_callbacks(lc_pouch_lonejson_source_read, NULL,
                                lc_pouch_lonejson_source_close, context, out,
                                error);
  if (rc != LC_OK) {
    lc_pouch_lonejson_source_close(context);
  }
  return rc;
}

static int lc_pouch_acquire_for_update_sink_write(lc_sink *self,
                                                  const void *bytes,
                                                  size_t count,
                                                  lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)self->impl;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update sink is closed", NULL, NULL,
                        NULL);
  }
  if (count > 0U && fwrite(bytes, 1U, count, file->fp) != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return 1;
}

static void lc_pouch_acquire_for_update_sink_close(lc_sink *self) {
  (void)self;
}

static size_t lc_pouch_acquire_for_update_source_read(void *context,
                                                      void *buffer,
                                                      size_t count,
                                                      lc_error *error) {
  lc_pouch_acquire_for_update_file *file;
  size_t nread;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch acquire_for_update source is closed", NULL, NULL, NULL);
    return 0U;
  }
  nread = fread(buffer, 1U, count, file->fp);
  if (nread == 0U && ferror(file->fp)) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                 "failed to read pouch acquire_for_update snapshot",
                 strerror(errno), NULL, NULL);
  }
  return nread;
}

static int lc_pouch_acquire_for_update_source_reset(void *context,
                                                    lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update source is closed", NULL, NULL,
                        NULL);
  }
  clearerr(file->fp);
  if (fseek(file->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static const char *lc_pouch_client_namespace(lc_client_handle *client,
                                             const char *ns) {
  if (ns != NULL && ns[0] != '\0') {
    return ns;
  }
  if (client->default_namespace != NULL &&
      client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

static int lc_pouch_client_namespace_reserved(const char *ns) {
  return ns != NULL &&
         (strcmp(ns, ".lockd") == 0 || strncmp(ns, ".lockd/", 7U) == 0 ||
          strcmp(ns, LC_POUCH_XA_NAMESPACE) == 0 ||
          strncmp(ns, ".xa/", 4U) == 0);
}

int lc_pouch_client_public_namespace(lc_client_handle *client, const char *ns,
                                     const char **out, lc_error *error) {
  const char *resolved;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace validation requires output", NULL,
                        NULL, NULL);
  }
  resolved = lc_pouch_client_namespace(client, ns);
  if (lc_pouch_client_namespace_reserved(resolved)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace is reserved for internal state", NULL,
                        NULL, "pouch");
  }
  *out = resolved;
  return LC_OK;
}

static const char *
lc_pouch_client_endpoint_query_engine(lc_client_handle *client) {
  if (client != NULL && client->pouch != NULL &&
      client->pouch->query_engine != NULL &&
      client->pouch->query_engine[0] != '\0') {
    return client->pouch->query_engine;
  }
  return "index";
}

static const char *
lc_pouch_client_default_fallback_engine(lc_client_handle *client) {
  if (client != NULL && client->pouch != NULL &&
      client->pouch->query_fallback_engine != NULL &&
      strcmp(client->pouch->query_fallback_engine, "scan") == 0) {
    return "scan";
  }
  return "none";
}

static int lc_pouch_namespace_config_valid_preferred(const char *engine) {
  return engine != NULL &&
         (strcmp(engine, "index") == 0 || strcmp(engine, "scan") == 0);
}

static int lc_pouch_namespace_config_valid_fallback(const char *engine) {
  return engine != NULL &&
         (strcmp(engine, "scan") == 0 || strcmp(engine, "none") == 0);
}

static const char *
lc_pouch_namespace_config_normalize_preferred(const char *engine) {
  return engine != NULL && engine[0] != '\0' ? engine : "index";
}

static const char *
lc_pouch_namespace_config_normalize_fallback(const char *engine) {
  return engine != NULL && engine[0] != '\0' ? engine : "none";
}

static char *lc_pouch_namespace_config_key(const char *ns, lc_error *error) {
  char *key;

  (void)ns;
  key = lc_strdup_with_allocator(NULL, LC_POUCH_NAMESPACE_CONFIG_KEY);
  if (key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch namespace config key", NULL, NULL,
                 NULL);
    return NULL;
  }
  return key;
}

static int lc_pouch_namespace_config_set_record(
    lc_pouch_namespace_config_record *record, const char *preferred_engine,
    const char *fallback_engine, lc_error *error) {
  if (!lc_pouch_namespace_config_valid_preferred(preferred_engine)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace preferred_engine must be index or "
                        "scan",
                        NULL, NULL, "pouch");
  }
  if (!lc_pouch_namespace_config_valid_fallback(fallback_engine)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace fallback_engine must be scan or none",
                        NULL, NULL, "pouch");
  }
  if (preferred_engine != record->preferred_engine) {
    strcpy(record->preferred_engine, preferred_engine);
  }
  if (fallback_engine != record->fallback_engine) {
    strcpy(record->fallback_engine, fallback_engine);
  }
  return LC_OK;
}

static int
lc_pouch_namespace_config_parse_body(const char *body, size_t length,
                                     lc_pouch_namespace_config_record *record,
                                     lc_error *error) {
  lc_pouch_binary_cursor cursor;
  char *preferred;
  char *fallback;
  int rc;

  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = (const unsigned char *)body;
  cursor.length = length;
  preferred = NULL;
  fallback = NULL;
  rc = lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_NAMESPACE_CONFIG_MAGIC,
                                    error);
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &preferred, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &fallback, error);
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch namespace config record has trailing bytes", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_config_set_record(record, preferred, fallback,
                                              error);
  }
  if (rc == LC_OK) {
    record->found = 1;
  }
  lc_free_with_allocator(NULL, preferred);
  lc_free_with_allocator(NULL, fallback);
  return rc;
}

static int lc_pouch_namespace_config_build_record(
    const lc_pouch_namespace_config_record *record, lc_pouch_txn_buffer *buffer,
    lc_error *error) {
  int rc;

  memset(buffer, 0, sizeof(*buffer));
  rc = lc_pouch_txn_buffer_append_bytes(buffer, LC_POUCH_NAMESPACE_CONFIG_MAGIC,
                                        strlen(LC_POUCH_NAMESPACE_CONFIG_MAGIC),
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, record->preferred_engine,
                                           error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, record->fallback_engine,
                                           error);
  }
  if (rc != LC_OK) {
    lc_pouch_txn_buffer_cleanup(buffer);
  }
  return rc;
}

static void lc_pouch_namespace_config_record_cleanup(
    lc_pouch_namespace_config_record *record) {
  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, record->etag);
  memset(record, 0, sizeof(*record));
}

static int
lc_pouch_namespace_config_read(lc_client_handle *client, const char *ns,
                               lc_pouch_namespace_config_record *record,
                               lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *key;
  int rc;

  memset(record, 0, sizeof(*record));
  rc = lc_pouch_namespace_config_set_record(
      record, lc_pouch_client_endpoint_query_engine(client),
      lc_pouch_client_default_fallback_engine(client), error);
  if (rc != LC_OK) {
    return rc;
  }
  record->found = 0;
  memset(&read_result, 0, sizeof(read_result));
  sink = NULL;
  key = lc_pouch_namespace_config_key(ns, error);
  if (key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, ns, key, &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_namespace_config_parse_body((const char *)bytes, length,
                                              record, error);
  }
  if (rc == LC_OK && read_result.found) {
    record->etag = lc_strdup_with_allocator(NULL, read_result.etag);
    if (record->etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace config etag", NULL,
                        NULL, NULL);
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  lc_free_with_allocator(NULL, key);
  return rc;
}

static int lc_pouch_namespace_config_response(
    lc_namespace_config_res *out, const char *ns,
    const lc_pouch_namespace_config_record *record, lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->ns = lc_strdup_local(ns);
  out->preferred_engine = lc_strdup_local(record->preferred_engine);
  out->fallback_engine = lc_strdup_local(record->fallback_engine);
  out->etag = lc_strdup_local(record->etag != NULL ? record->etag : "");
  out->correlation_id = lc_strdup_local("pouch-namespace-config");
  if (out->ns == NULL || out->preferred_engine == NULL ||
      out->fallback_engine == NULL || out->etag == NULL ||
      out->correlation_id == NULL) {
    lc_namespace_config_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace config response",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_client_query_engine(lc_client_handle *client,
                                        const char *ns,
                                        const char *request_engine,
                                        const char **out_engine,
                                        char **owned_engine, lc_error *error) {
  lc_pouch_namespace_config_record record;
  int rc;

  *owned_engine = NULL;
  if (client != NULL && client->pouch != NULL &&
      !client->pouch->query_indexing_enabled) {
    if (request_engine != NULL && request_engine[0] != '\0' &&
        strcmp(request_engine, "index") == 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query indexing is disabled; use engine=scan",
                          NULL, NULL, "pouch");
    }
    if (request_engine == NULL || request_engine[0] == '\0' ||
        strcmp(request_engine, "scan") == 0) {
      *out_engine = "scan";
      return LC_OK;
    }
  }
  if (request_engine != NULL && request_engine[0] != '\0') {
    *out_engine = request_engine;
    return LC_OK;
  }
  rc = lc_pouch_namespace_config_read(client, ns, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!record.found) {
    *out_engine = lc_pouch_client_endpoint_query_engine(client);
    lc_pouch_namespace_config_record_cleanup(&record);
    return LC_OK;
  }
  *owned_engine = lc_strdup_with_allocator(NULL, record.preferred_engine);
  if (*owned_engine == NULL) {
    lc_pouch_namespace_config_record_cleanup(&record);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query engine", NULL, NULL,
                        NULL);
  }
  *out_engine = *owned_engine;
  lc_pouch_namespace_config_record_cleanup(&record);
  return LC_OK;
}

static int lc_pouch_client_can_use_query_fallback(lc_client_handle *client,
                                                  const char *request_engine,
                                                  const char *fallback) {
  if (request_engine != NULL && request_engine[0] != '\0') {
    return 0;
  }
  return client != NULL && client->pouch != NULL &&
         client->pouch->query_indexing_enabled &&
         client->pouch->query_fallback_engine != NULL &&
         strcmp(client->pouch->query_fallback_engine, fallback) == 0;
}

static int lc_pouch_client_is_queue_state_key(const char *key);

static int lc_pouch_client_validate_public_key(const char *key,
                                               lc_error *error) {
  if (key == NULL || key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch operation requires a non-empty key", NULL, NULL,
                        NULL);
  }
  if (strcmp(key, LC_POUCH_NAMESPACE_CONFIG_KEY) == 0 ||
      strncmp(key, "config/", sizeof("config/") - 1U) == 0 ||
      strncmp(key, "q/", sizeof("q/") - 1U) == 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch internal keys are reserved", NULL, NULL,
                        "pouch");
  }
  if (strlen(key) >= strlen("/.lease") &&
      strcmp(key + strlen(key) - strlen("/.lease"), "/.lease") == 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch internal keys are reserved", NULL, NULL,
                        "pouch");
  }
  if (strncmp(key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(key, "/.staging/") != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staging keys are reserved for internal state",
                        NULL, NULL, "pouch");
  }
  if (strncmp(key, "state/", sizeof("state/") - 1U) == 0 &&
      strstr(key, "/attachments/") != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment keys are reserved for internal "
                        "objects",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_client_validate_acquire_key(const char *key,
                                                lc_error *error) {
  if (lc_pouch_client_is_queue_state_key(key)) {
    return LC_OK;
  }
  return lc_pouch_client_validate_public_key(key, error);
}

static int lc_pouch_query_lql_error(lc_error *error, lql_status status,
                                    const lql_error *lql_error_value,
                                    const char *fallback) {
  const char *message;

  message = fallback;
  if (lql_error_value != NULL && lql_error_value->message[0] != '\0') {
    message = lql_error_value->message;
  } else if (message == NULL || message[0] == '\0') {
    message = lql_status_string(status);
  }
  return lc_error_set(
      error, status == LQL_STATUS_NO_MEMORY ? LC_ERR_NOMEM : LC_ERR_INVALID, 0L,
      message, lql_status_string(status), NULL, "pouch-lql");
}

static int lc_pouch_query_request_has_selector(const lc_query_req *req) {
  return req != NULL &&
         ((req->selector_json != NULL && req->selector_json[0] != '\0') ||
          (req->selector_lql != NULL && req->selector_lql[0] != '\0'));
}

static int lc_pouch_query_request_validate_selector(const lc_query_req *req,
                                                    lc_error *error) {
  if (req != NULL && req->selector_json != NULL &&
      req->selector_json[0] != '\0' && req->selector_lql != NULL &&
      req->selector_lql[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "selector_json and selector_lql are mutually "
                        "exclusive",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_query_parse_selector(lql *runtime, const lc_query_req *req,
                                         lql_selector **out, lc_error *error) {
  lql_error lql_error_value;
  lql_status status;

  if (runtime == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector parse requires runtime, request, "
                        "and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (lc_pouch_query_request_validate_selector(req, error) != LC_OK) {
    return error != NULL ? error->code : LC_ERR_INVALID;
  }
  lql_error_init(&lql_error_value);
  if (req->selector_lql != NULL && req->selector_lql[0] != '\0') {
    status = runtime->selector_parse(runtime, req->selector_lql, out,
                                     &lql_error_value);
  } else if (req->selector_json != NULL && req->selector_json[0] != '\0') {
    status = runtime->selector_parse_json(runtime, req->selector_json,
                                          strlen(req->selector_json), out,
                                          &lql_error_value);
  } else {
    return LC_OK;
  }
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to parse pouch query selector");
  }
  return LC_OK;
}

static lql_status lc_pouch_query_lql_read(void *user, unsigned char *buffer,
                                          size_t capacity, size_t *out_len,
                                          lql_error *lql_error_value) {
  lc_pouch_query_source_reader *reader;
  lc_error error;
  size_t nread;

  if (user == NULL || buffer == NULL || out_len == NULL) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message),
               "pouch query reader requires user, buffer, and out_len");
      lql_error_value->code = LQL_STATUS_INVALID_ARGUMENT;
    }
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  reader = (lc_pouch_query_source_reader *)user;
  if (reader->source == NULL) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message),
               "pouch query reader has no source");
      lql_error_value->code = LQL_STATUS_INVALID_ARGUMENT;
    }
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  lc_error_init(&error);
  if (reader->buffer != NULL && reader->buffer_capacity > 0U) {
    if (reader->buffer_offset >= reader->buffer_length) {
      reader->buffer_offset = 0U;
      reader->buffer_length = reader->source->read(
          reader->source, reader->buffer, reader->buffer_capacity, &error);
      if (reader->buffer_length == 0U && error.code != LC_OK) {
        if (lql_error_value != NULL) {
          snprintf(lql_error_value->message, sizeof(lql_error_value->message),
                   "%s",
                   error.message != NULL ? error.message
                                         : "failed to read pouch query body");
          lql_error_value->code = LQL_STATUS_IO_ERROR;
        }
        lc_error_cleanup(&error);
        return LQL_STATUS_IO_ERROR;
      }
    }
    nread = reader->buffer_length - reader->buffer_offset;
    if (nread > capacity) {
      nread = capacity;
    }
    if (nread > 0U) {
      memcpy(buffer, reader->buffer + reader->buffer_offset, nread);
      reader->buffer_offset += nread;
    }
  } else {
    nread = reader->source->read(reader->source, buffer, capacity, &error);
  }
  if (nread == 0U && error.code != LC_OK) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message), "%s",
               error.message != NULL ? error.message
                                     : "failed to read pouch query body");
      lql_error_value->code = LQL_STATUS_IO_ERROR;
    }
    lc_error_cleanup(&error);
    return LQL_STATUS_IO_ERROR;
  }
  lc_error_cleanup(&error);
  *out_len = nread;
  return LQL_STATUS_OK;
}

static lql_stream_callback_result
lc_pouch_query_lql_decision(void *user, const lql_stream_decision *decision,
                            lql_error *error) {
  lc_pouch_query_match_state *state;

  (void)error;
  state = (lc_pouch_query_match_state *)user;
  if (state == NULL || decision == NULL) {
    return LQL_STREAM_CALLBACK_ERROR;
  }
  if (decision->matched) {
    state->matched = 1;
    return LQL_STREAM_CALLBACK_STOP;
  }
  return LQL_STREAM_CALLBACK_CONTINUE;
}

static unsigned char lc_pouch_query_fold_ascii(unsigned char byte) {
  return byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
}

static lonejson_read_result
lc_pouch_query_any_text_lonejson_read(void *user, unsigned char *buffer,
                                      size_t capacity) {
  lc_pouch_query_any_text_match_state *state;
  lonejson_read_result result;

  memset(&result, 0, sizeof(result));
  state = (lc_pouch_query_any_text_match_state *)user;
  if (state == NULL || state->source == NULL) {
    result.error_code = EINVAL;
    return result;
  }
  result.bytes_read =
      state->source->read(state->source, buffer, capacity, &state->read_error);
  if (result.bytes_read == 0U) {
    if (state->read_error.code != LC_OK) {
      result.error_code = EIO;
    } else {
      result.eof = 1;
    }
  }
  return result;
}

static int
lc_pouch_query_any_text_prepare(lc_pouch_query_any_text_match_state *state,
                                const char *needle, int ignore_case,
                                lc_error *error) {
  size_t index;
  size_t matched;

  if (state == NULL || needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch any-text match requires a non-empty needle",
                        NULL, NULL, NULL);
  }
  state->needle_len = strlen(needle);
  state->ignore_case = ignore_case ? 1 : 0;
  state->needle = (char *)lc_alloc_with_allocator(NULL, state->needle_len + 1U);
  state->prefix = (size_t *)lc_calloc_with_allocator(NULL, state->needle_len,
                                                     sizeof(*state->prefix));
  if (state->needle == NULL || state->prefix == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch any-text matcher", NULL, NULL,
                        NULL);
  }
  for (index = 0U; index < state->needle_len; ++index) {
    unsigned char byte;

    byte = (unsigned char)needle[index];
    state->needle[index] =
        (char)(state->ignore_case ? lc_pouch_query_fold_ascii(byte) : byte);
  }
  state->needle[state->needle_len] = '\0';
  matched = 0U;
  for (index = 1U; index < state->needle_len; ++index) {
    while (matched > 0U && state->needle[index] != state->needle[matched]) {
      matched = state->prefix[matched - 1U];
    }
    if (state->needle[index] == state->needle[matched]) {
      ++matched;
    }
    state->prefix[index] = matched;
  }
  return LC_OK;
}

static void lc_pouch_query_any_text_match_state_cleanup(
    lc_pouch_query_any_text_match_state *state) {
  if (state == NULL) {
    return;
  }
  lc_error_cleanup(&state->read_error);
  lc_free_with_allocator(NULL, state->needle);
  lc_free_with_allocator(NULL, state->prefix);
  memset(state, 0, sizeof(*state));
}

static lonejson_status lc_pouch_query_any_text_string_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_any_text_match_state *state;

  (void)path;
  (void)lj_error;
  state = (lc_pouch_query_any_text_match_state *)user;
  if (state != NULL) {
    state->matched_len = 0U;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_any_text_string_chunk(
    void *user, const lonejson_value_path *path, const char *data, size_t len,
    lonejson_error *lj_error) {
  lc_pouch_query_any_text_match_state *state;
  size_t index;

  (void)path;
  (void)lj_error;
  state = (lc_pouch_query_any_text_match_state *)user;
  if (state == NULL || state->matched || state->needle == NULL ||
      state->needle_len == 0U) {
    return LONEJSON_STATUS_OK;
  }
  for (index = 0U; index < len; ++index) {
    unsigned char byte;
    char folded;

    byte = (unsigned char)data[index];
    folded =
        (char)(state->ignore_case ? lc_pouch_query_fold_ascii(byte) : byte);
    while (state->matched_len > 0U &&
           folded != state->needle[state->matched_len]) {
      state->matched_len = state->prefix[state->matched_len - 1U];
    }
    if (folded == state->needle[state->matched_len]) {
      ++state->matched_len;
      if (state->matched_len == state->needle_len) {
        state->matched = 1;
        state->matched_len = state->prefix[state->matched_len - 1U];
        break;
      }
    }
  }
  return LONEJSON_STATUS_OK;
}

static void lc_pouch_query_scan_scalar_match_state_cleanup(
    lc_pouch_query_scan_scalar_match_state *state) {
  if (state == NULL) {
    return;
  }
  if (state->has_contains_matcher) {
    lc_pouch_query_any_text_match_state_cleanup(&state->contains_matcher);
  }
  lc_error_cleanup(&state->read_error);
  lc_free_with_allocator(NULL, state->scratch);
  memset(state, 0, sizeof(*state));
}

static lonejson_read_result
lc_pouch_query_scan_scalar_lonejson_read(void *user, unsigned char *buffer,
                                         size_t capacity) {
  lc_pouch_query_scan_scalar_match_state *state;
  lonejson_read_result result;

  memset(&result, 0, sizeof(result));
  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || state->source == NULL) {
    result.error_code = EINVAL;
    return result;
  }
  if (capacity > 4096U) {
    capacity = 4096U;
  }
  result.bytes_read =
      state->source->read(state->source, buffer, capacity, &state->read_error);
  if (result.bytes_read == 0U) {
    if (state->read_error.code != LC_OK) {
      result.error_code = EIO;
    } else {
      result.eof = 1;
    }
  }
  return result;
}

static int lc_pouch_query_scan_scalar_pointer_segment_matches_path(
    const char *encoded, size_t encoded_len,
    const lonejson_path_segment *path_segment) {
  size_t encoded_index;
  size_t decoded_index;

  if (encoded == NULL || path_segment == NULL) {
    return 0;
  }
  decoded_index = 0U;
  for (encoded_index = 0U; encoded_index < encoded_len; ++encoded_index) {
    char byte;

    byte = encoded[encoded_index];
    if (byte == '~') {
      if (encoded_index + 1U >= encoded_len) {
        return 0;
      }
      ++encoded_index;
      if (encoded[encoded_index] == '0') {
        byte = '~';
      } else if (encoded[encoded_index] == '1') {
        byte = '/';
      } else {
        return 0;
      }
    }
    if (decoded_index >= path_segment->len ||
        path_segment->data[decoded_index] != byte) {
      return 0;
    }
    ++decoded_index;
  }
  return decoded_index == path_segment->len;
}

static int
lc_pouch_query_scan_scalar_field_matches_path(const char *field,
                                              const lonejson_value_path *path) {
  const char *cursor;
  size_t segment_index;

  if (field == NULL || field[0] != '/' || path == NULL) {
    return 0;
  }
  cursor = field + 1;
  segment_index = 0U;
  for (;;) {
    const char *slash;
    size_t len;

    if (segment_index >= path->segment_count) {
      return 0;
    }
    slash = strchr(cursor, '/');
    len = slash != NULL ? (size_t)(slash - cursor) : strlen(cursor);
    if (!lc_pouch_query_scan_scalar_pointer_segment_matches_path(
            cursor, len, &path->segments[segment_index])) {
      return 0;
    }
    ++segment_index;
    if (slash == NULL) {
      return segment_index == path->segment_count;
    }
    cursor = slash + 1;
  }
}

static int
lc_pouch_query_scan_scalar_value_matches(const lc_pouch_query_index_plan *plan,
                                         const char *value, size_t value_len,
                                         char value_type) {
  char stack_value[128];
  char *value_copy;
  size_t index;
  double parsed_value;
  char *end;

  if (plan == NULL || value == NULL) {
    return 0;
  }
  if (plan->prefix || plan->contains) {
    if (value_type != 's') {
      return 0;
    }
    for (index = 0U; index < plan->value_count; ++index) {
      const char *needle;
      size_t needle_len;
      size_t offset;

      if (plan->value_types[index] != 's') {
        continue;
      }
      needle = plan->values[index];
      needle_len = needle != NULL ? strlen(needle) : 0U;
      if (needle == NULL || needle_len == 0U || needle_len > value_len) {
        continue;
      }
      if (plan->prefix) {
        int matched;
        size_t byte_index;

        matched = 1;
        for (byte_index = 0U; byte_index < needle_len; ++byte_index) {
          unsigned char actual;
          unsigned char expected;

          actual = (unsigned char)value[byte_index];
          expected = (unsigned char)needle[byte_index];
          if (plan->ignore_case) {
            actual = lc_pouch_query_fold_ascii(actual);
            expected = lc_pouch_query_fold_ascii(expected);
          }
          if (actual != expected) {
            matched = 0;
            break;
          }
        }
        if (matched) {
          return 1;
        }
        continue;
      }
      for (offset = 0U; offset + needle_len <= value_len; ++offset) {
        int matched;
        size_t byte_index;

        matched = 1;
        for (byte_index = 0U; byte_index < needle_len; ++byte_index) {
          unsigned char actual;
          unsigned char expected;

          actual = (unsigned char)value[offset + byte_index];
          expected = (unsigned char)needle[byte_index];
          if (plan->ignore_case) {
            actual = lc_pouch_query_fold_ascii(actual);
            expected = lc_pouch_query_fold_ascii(expected);
          }
          if (actual != expected) {
            matched = 0;
            break;
          }
        }
        if (matched) {
          return 1;
        }
      }
    }
    return 0;
  }
  value_copy = NULL;
  if (value_type == 'n') {
    if (value_len + 1U <= sizeof(stack_value)) {
      memcpy(stack_value, value, value_len);
      stack_value[value_len] = '\0';
      value_copy = stack_value;
    } else {
      value_copy = lc_pouch_query_dup_bytes(value, value_len, NULL);
      if (value_copy == NULL) {
        return 0;
      }
    }
    errno = 0;
    parsed_value = strtod(value_copy, &end);
    if (errno != 0 || end == value_copy || *end != '\0') {
      if (value_copy != stack_value) {
        lc_free_with_allocator(NULL, value_copy);
      }
      return 0;
    }
  } else {
    parsed_value = 0.0;
  }
  for (index = 0U; index < plan->value_count; ++index) {
    if (plan->value_types[index] != value_type) {
      continue;
    }
    if (value_type == 'n') {
      double planned_value;

      errno = 0;
      planned_value = strtod(plan->values[index], &end);
      if (errno == 0 && end != plan->values[index] && *end == '\0' &&
          planned_value == parsed_value) {
        if (value_copy != stack_value) {
          lc_free_with_allocator(NULL, value_copy);
        }
        return 1;
      }
      continue;
    }
    if (strlen(plan->values[index]) == value_len &&
        memcmp(plan->values[index], value, value_len) == 0) {
      if (value_copy != NULL && value_copy != stack_value) {
        lc_free_with_allocator(NULL, value_copy);
      }
      return 1;
    }
  }
  if (value_copy != NULL && value_copy != stack_value) {
    lc_free_with_allocator(NULL, value_copy);
  }
  return 0;
}

static int lc_pouch_query_scan_scalar_scratch_reserve(
    lc_pouch_query_scan_scalar_match_state *state, size_t extra,
    lc_error *error) {
  char *next;
  size_t needed;
  size_t next_capacity;

  if (state == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan scalar scratch requires state", NULL, NULL,
                        NULL);
  }
  if (extra > (size_t)-1 - state->scratch_len - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch scan scalar exceeds local limit", NULL, NULL,
                        NULL);
  }
  needed = state->scratch_len + extra + 1U;
  if (needed <= state->scratch_capacity) {
    return LC_OK;
  }
  next_capacity = state->scratch_capacity == 0U ? 32U : state->scratch_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch scan scalar exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next = (char *)lc_realloc_with_allocator(NULL, state->scratch, next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch scan scalar", NULL, NULL,
                        NULL);
  }
  state->scratch = next;
  state->scratch_capacity = next_capacity;
  state->scratch[state->scratch_len] = '\0';
  return LC_OK;
}

typedef struct lc_pouch_query_scan_scalar_reader {
  lc_source *source;
  unsigned char buffer[4096];
  size_t offset;
  size_t length;
  int eof;
  lc_error read_error;
} lc_pouch_query_scan_scalar_reader;

static int lc_pouch_query_scan_scalar_reader_peek(
    lc_pouch_query_scan_scalar_reader *reader, int *out, lc_error *error) {
  size_t got;

  if (reader == NULL || out == NULL || reader->source == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan scalar reader requires inputs", NULL, NULL,
                        "pouch");
  }
  if (reader->offset >= reader->length && !reader->eof) {
    lc_error_cleanup(&reader->read_error);
    lc_error_init(&reader->read_error);
    got = reader->source->read(reader->source, reader->buffer,
                               sizeof(reader->buffer), &reader->read_error);
    if (got == 0U) {
      if (reader->read_error.code != LC_OK) {
        if (error != NULL) {
          *error = reader->read_error;
          memset(&reader->read_error, 0, sizeof(reader->read_error));
        }
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_PROTOCOL;
      }
      reader->eof = 1;
    }
    reader->offset = 0U;
    reader->length = got;
  }
  if (reader->offset >= reader->length) {
    *out = -1;
  } else {
    *out = (int)reader->buffer[reader->offset];
  }
  return LC_OK;
}

static int lc_pouch_query_scan_scalar_reader_next(
    lc_pouch_query_scan_scalar_reader *reader, int *out, lc_error *error) {
  int rc;

  rc = lc_pouch_query_scan_scalar_reader_peek(reader, out, error);
  if (rc == LC_OK && *out >= 0) {
    reader->offset += 1U;
  }
  return rc;
}

static int
lc_pouch_query_scan_scalar_reader_ws(lc_pouch_query_scan_scalar_reader *reader,
                                     int *out, lc_error *error) {
  int c;
  int rc;

  c = 0;
  for (;;) {
    rc = lc_pouch_query_scan_scalar_reader_peek(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
      *out = c;
      return LC_OK;
    }
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
}

static int lc_pouch_query_scan_scalar_field_segment(
    lc_pouch_query_scan_scalar_match_state *state, const char *field,
    size_t wanted, const char **segment, size_t *len, lc_error *error) {
  const char *cursor;
  size_t index;

  if (state == NULL || field == NULL || field[0] != '/' || segment == NULL ||
      len == NULL) {
    return 0;
  }
  cursor = field + 1;
  index = 0U;
  for (;;) {
    const char *slash;
    size_t segment_len;

    slash = strchr(cursor, '/');
    segment_len = slash != NULL ? (size_t)(slash - cursor) : strlen(cursor);
    if (index == wanted) {
      size_t source_index;
      size_t decoded_len;

      if (memchr(cursor, '~', segment_len) == NULL) {
        *segment = cursor;
        *len = segment_len;
        return 1;
      }
      if (lc_pouch_query_scan_scalar_scratch_reserve(state, segment_len,
                                                     error) != LC_OK) {
        return 0;
      }
      decoded_len = 0U;
      for (source_index = 0U; source_index < segment_len; ++source_index) {
        char byte;

        byte = cursor[source_index];
        if (byte == '~') {
          if (source_index + 1U >= segment_len) {
            return 0;
          }
          ++source_index;
          if (cursor[source_index] == '0') {
            byte = '~';
          } else if (cursor[source_index] == '1') {
            byte = '/';
          } else {
            return 0;
          }
        }
        state->scratch[decoded_len++] = byte;
      }
      state->scratch[decoded_len] = '\0';
      *segment = state->scratch;
      *len = decoded_len;
      return 1;
    }
    if (slash == NULL) {
      break;
    }
    cursor = slash + 1;
    ++index;
  }
  return 0;
}

static size_t
lc_pouch_query_scan_scalar_field_segment_count(const char *field) {
  const char *cursor;
  size_t count;

  if (field == NULL || field[0] != '/') {
    return 0U;
  }
  cursor = field + 1;
  count = 0U;
  for (;;) {
    const char *slash;

    slash = strchr(cursor, '/');
    ++count;
    if (slash == NULL) {
      break;
    }
    cursor = slash + 1;
  }
  return count;
}

static int lc_pouch_query_scan_scalar_json_string(
    lc_pouch_query_scan_scalar_reader *reader,
    lc_pouch_query_scan_scalar_match_state *state, const char *compare,
    size_t compare_len, int capture, int *equal, lc_error *error) {
  size_t seen;
  int c;
  int rc;

  if (equal != NULL) {
    *equal = 1;
  }
  if (capture) {
    if (state->has_contains_matcher && state->plan != NULL &&
        state->plan->contains) {
      state->contains_matcher.matched = 0;
      state->contains_matcher.matched_len = 0U;
    } else {
      state->scratch_len = 0U;
      if (state->scratch != NULL) {
        state->scratch[0] = '\0';
      }
    }
  }
  rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (c != '"') {
    return LC_ERR_INVALID;
  }
  seen = 0U;
  for (;;) {
    char out;

    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c < 0) {
      return LC_ERR_INVALID;
    }
    if (c == '"') {
      if (equal != NULL && seen != compare_len) {
        *equal = 0;
      }
      return LC_OK;
    }
    if (c == '\\') {
      rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (c < 0) {
        return LC_ERR_INVALID;
      }
      switch (c) {
      case '"':
      case '\\':
      case '/':
        out = (char)c;
        break;
      case 'b':
        out = '\b';
        break;
      case 'f':
        out = '\f';
        break;
      case 'n':
        out = '\n';
        break;
      case 'r':
        out = '\r';
        break;
      case 't':
        out = '\t';
        break;
      default:
        return LC_ERR_INVALID;
      }
    } else {
      out = (char)c;
    }
    if (equal != NULL &&
        (seen >= compare_len || compare == NULL || compare[seen] != out)) {
      *equal = 0;
    }
    ++seen;
    if (capture && state->has_contains_matcher && state->plan != NULL &&
        state->plan->contains) {
      lonejson_status status;

      status = lc_pouch_query_any_text_string_chunk(&state->contains_matcher,
                                                    NULL, &out, 1U, NULL);
      if (status != LONEJSON_STATUS_OK) {
        return LC_ERR_INVALID;
      }
    } else if (capture) {
      rc = lc_pouch_query_scan_scalar_scratch_reserve(state, 1U, error);
      if (rc != LC_OK) {
        return rc;
      }
      state->scratch[state->scratch_len++] = out;
      state->scratch[state->scratch_len] = '\0';
    }
  }
}

static int lc_pouch_query_scan_scalar_json_literal(
    lc_pouch_query_scan_scalar_reader *reader, const char *literal,
    lc_error *error) {
  size_t index;
  int c;
  int rc;

  c = 0;
  for (index = 0U; literal[index] != '\0'; ++index) {
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c != (int)(unsigned char)literal[index]) {
      return LC_ERR_INVALID;
    }
  }
  return LC_OK;
}

static int lc_pouch_query_scan_scalar_json_number(
    lc_pouch_query_scan_scalar_reader *reader,
    lc_pouch_query_scan_scalar_match_state *state, lc_error *error) {
  int c;
  int rc;

  c = 0;
  if (state != NULL) {
    state->scratch_len = 0U;
  }
  if (state != NULL && state->scratch != NULL) {
    state->scratch[0] = '\0';
  }
  for (;;) {
    rc = lc_pouch_query_scan_scalar_reader_peek(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' ||
          c == 'e' || c == 'E')) {
      return state == NULL || state->scratch_len > 0U ? LC_OK : LC_ERR_INVALID;
    }
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (state != NULL) {
      rc = lc_pouch_query_scan_scalar_scratch_reserve(state, 1U, error);
      if (rc != LC_OK) {
        return rc;
      }
      state->scratch[state->scratch_len++] = (char)c;
      state->scratch[state->scratch_len] = '\0';
    }
  }
}

static int
lc_pouch_query_scan_scalar_skip_key(lc_pouch_query_scan_scalar_reader *reader,
                                    lc_error *error) {
  int c, equal, rc;
  rc = lc_pouch_query_scan_scalar_json_string(reader, NULL, NULL, 0U, 0, &equal,
                                              error);
  if (rc == LC_OK)
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
  if (rc == LC_OK && c != ':')
    rc = LC_ERR_INVALID;
  if (rc == LC_OK)
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
  return rc;
}

static int
lc_pouch_query_scan_scalar_skip_value(lc_pouch_query_scan_scalar_reader *reader,
                                      lc_error *error) {
  unsigned char local[32], *frames = local;
  size_t depth = 0U, capacity = sizeof(local);
  int c, equal, rc;
  for (;;) {
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
    if (rc != LC_OK)
      break;
    if (c == '{' || c == '[') {
      int object = c == '{';
      if (depth == capacity) {
        unsigned char *next;
        if (capacity > (size_t)-1 / 2U) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "scalar-query traversal is too large", NULL, NULL,
                            "pouch");
          break;
        }
        next = lc_alloc_with_allocator(NULL, capacity * 2U);
        if (next == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate scalar-query traversal", NULL,
                            NULL, "pouch");
          break;
        }
        memcpy(next, frames, depth);
        if (frames != local)
          lc_free_with_allocator(NULL, frames);
        frames = next;
        capacity *= 2U;
      }
      frames[depth++] = (unsigned char)object;
      rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
      if (rc == LC_OK)
        rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
      if (rc != LC_OK)
        break;
      if (c == (object ? '}' : ']')) {
        rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
        --depth;
      } else {
        if (object)
          rc = lc_pouch_query_scan_scalar_skip_key(reader, error);
        if (rc != LC_OK)
          break;
        continue;
      }
    } else if (c == '"') {
      rc = lc_pouch_query_scan_scalar_json_string(reader, NULL, NULL, 0U, 0,
                                                  &equal, error);
    } else if (c == 't') {
      rc = lc_pouch_query_scan_scalar_json_literal(reader, "true", error);
    } else if (c == 'f') {
      rc = lc_pouch_query_scan_scalar_json_literal(reader, "false", error);
    } else if (c == 'n') {
      rc = lc_pouch_query_scan_scalar_json_literal(reader, "null", error);
    } else {
      rc = lc_pouch_query_scan_scalar_json_number(reader, NULL, error);
    }
    /* A completed value can close many parents, without recursive returns. */
    while (rc == LC_OK && depth != 0U) {
      int object = frames[depth - 1U] != 0;
      rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
      if (rc != LC_OK)
        break;
      if (c == (object ? '}' : ']')) {
        rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
        --depth;
        continue;
      }
      if (c != ',') {
        rc = LC_ERR_INVALID;
        break;
      }
      rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
      if (rc == LC_OK)
        rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
      if (rc == LC_OK && object)
        rc = lc_pouch_query_scan_scalar_skip_key(reader, error);
      break;
    }
    if (rc != LC_OK || depth == 0U)
      break;
  }
  if (frames != local)
    lc_free_with_allocator(NULL, frames);
  return rc;
}

static int lc_pouch_query_scan_scalar_parse_target(
    lc_pouch_query_scan_scalar_reader *reader,
    lc_pouch_query_scan_scalar_match_state *state, int *matched,
    lc_error *error) {
  int c;
  int rc;
  int equal;

  *matched = 0;
  rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (c == '"') {
    rc = lc_pouch_query_scan_scalar_json_string(reader, state, NULL, 0U, 1,
                                                &equal, error);
    if (rc == LC_OK) {
      if (state->has_contains_matcher && state->plan != NULL &&
          state->plan->contains) {
        *matched = state->contains_matcher.matched ? 1 : 0;
      } else {
        *matched = lc_pouch_query_scan_scalar_value_matches(
            state->plan, state->scratch != NULL ? state->scratch : "",
            state->scratch_len, 's');
      }
    }
    return rc;
  }
  if (c == 't') {
    rc = lc_pouch_query_scan_scalar_json_literal(reader, "true", error);
    *matched = rc == LC_OK && lc_pouch_query_scan_scalar_value_matches(
                                  state->plan, "true", 4U, 'b');
    return rc;
  }
  if (c == 'f') {
    rc = lc_pouch_query_scan_scalar_json_literal(reader, "false", error);
    *matched = rc == LC_OK && lc_pouch_query_scan_scalar_value_matches(
                                  state->plan, "false", 5U, 'b');
    return rc;
  }
  if (c == 'n') {
    rc = lc_pouch_query_scan_scalar_json_literal(reader, "null", error);
    *matched = rc == LC_OK && lc_pouch_query_scan_scalar_value_matches(
                                  state->plan, "null", 4U, 'z');
    return rc;
  }
  if (c == '{' || c == '[') {
    return lc_pouch_query_scan_scalar_skip_value(reader, error);
  }
  rc = lc_pouch_query_scan_scalar_json_number(reader, state, error);
  if (rc == LC_OK) {
    *matched = lc_pouch_query_scan_scalar_value_matches(
        state->plan, state->scratch != NULL ? state->scratch : "",
        state->scratch_len, 'n');
  }
  return rc;
}

static int lc_pouch_query_scan_scalar_scan_object(
    lc_pouch_query_scan_scalar_reader *reader,
    lc_pouch_query_scan_scalar_match_state *state, size_t depth,
    size_t segment_count, int object_open, int *decided, int *matched,
    lc_error *error) {
  int c;
  int rc;
  const char *segment;
  size_t segment_len;

  if (decided != NULL) {
    *decided = 0;
  }
  if (matched != NULL) {
    *matched = 0;
  }
next_object:
  if (!object_open) {
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c != '{') {
      return LC_ERR_INVALID;
    }
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!lc_pouch_query_scan_scalar_field_segment(
          state, state->plan->field, depth, &segment, &segment_len, error)) {
    return LC_ERR_INVALID;
  }
  rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (c == '}') {
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc == LC_OK && decided != NULL) {
      *decided = 1;
    }
    return rc;
  }
  for (;;) {
    int key_equal;

    rc = lc_pouch_query_scan_scalar_json_string(
        reader, NULL, segment, segment_len, 0, &key_equal, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c != ':') {
      return LC_ERR_INVALID;
    }
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (key_equal) {
      if (depth + 1U == segment_count) {
        rc = lc_pouch_query_scan_scalar_parse_target(reader, state, matched,
                                                     error);
        if (rc == LC_OK && decided != NULL) {
          *decided = 1;
        }
        return rc;
      }
      rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (c != '{') {
        if (c == '[') {
          return LC_OK;
        }
        rc = lc_pouch_query_scan_scalar_skip_value(reader, error);
        if (rc == LC_OK && decided != NULL) {
          *decided = 1;
        }
        return rc;
      }
      rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
      if (rc != LC_OK) {
        return rc;
      }
      ++depth;
      object_open = 1;
      goto next_object;
    }
    rc = lc_pouch_query_scan_scalar_skip_value(reader, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (c == '}') {
      rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
      if (rc == LC_OK && decided != NULL) {
        *decided = 1;
      }
      return rc;
    }
    if (c != ',') {
      return LC_ERR_INVALID;
    }
    rc = lc_pouch_query_scan_scalar_reader_next(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_query_scan_scalar_reader_ws(reader, &c, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
}

static int lc_pouch_query_match_scan_scalar_body_direct(
    lc_pouch_query_scan_scalar_match_state *state, int *decided, int *matched,
    lc_error *error) {
  lc_pouch_query_scan_scalar_reader reader;
  size_t segment_count;
  int rc;

  if (decided != NULL) {
    *decided = 0;
  }
  if (matched != NULL) {
    *matched = 0;
  }
  if (state == NULL || state->source == NULL || state->plan == NULL) {
    return LC_ERR_INVALID;
  }
  segment_count =
      lc_pouch_query_scan_scalar_field_segment_count(state->plan->field);
  if (segment_count == 0U) {
    return LC_ERR_INVALID;
  }
  memset(&reader, 0, sizeof(reader));
  lc_error_init(&reader.read_error);
  reader.source = state->source;
  rc = lc_pouch_query_scan_scalar_scan_object(&reader, state, 0U, segment_count,
                                              0, decided, matched, error);
  lc_error_cleanup(&reader.read_error);
  return rc;
}

static lonejson_status
lc_pouch_query_scan_scalar_lonejson_error(lonejson_error *lj_error,
                                          const lc_error *error) {
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = error != NULL && error->code == LC_ERR_NOMEM
                         ? LONEJSON_STATUS_ALLOCATION_FAILED
                         : LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message), "%s",
             error != NULL && error->message != NULL
                 ? error->message
                 : "pouch scan scalar callback failed");
  }
  return lj_error != NULL ? lj_error->code : LONEJSON_STATUS_CALLBACK_FAILED;
}

static lonejson_status lc_pouch_query_scan_scalar_stop_after_decision(
    lc_pouch_query_scan_scalar_match_state *state, int matched,
    lonejson_error *lj_error) {
  if (state != NULL) {
    state->matched = matched ? 1 : 0;
    state->stopped_after_match = matched ? 1 : 0;
    state->stopped_after_decision = 1;
  }
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message),
             "pouch scan scalar decided");
  }
  return LONEJSON_STATUS_CALLBACK_FAILED;
}

static lonejson_status
lc_pouch_query_scan_scalar_begin(void *user, const lonejson_value_path *path,
                                 char value_type, lonejson_error *lj_error) {
  lc_pouch_query_scan_scalar_match_state *state;
  lc_error error;
  int rc;

  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || state->matched ||
      !lc_pouch_query_scan_scalar_field_matches_path(state->plan->field,
                                                     path)) {
    if (state != NULL) {
      state->capturing = 0;
    }
    return LONEJSON_STATUS_OK;
  }
  state->scratch_len = 0U;
  if (state->scratch != NULL) {
    state->scratch[0] = '\0';
  }
  state->capturing = 1;
  state->capture_type = value_type;
  lc_error_init(&error);
  rc = lc_pouch_query_scan_scalar_scratch_reserve(state, 0U, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_scan_scalar_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_scan_scalar_string_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_scan_scalar_begin(user, path, 's', lj_error);
}

static lonejson_status lc_pouch_query_scan_scalar_number_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_scan_scalar_begin(user, path, 'n', lj_error);
}

static lonejson_status
lc_pouch_query_scan_scalar_chunk(void *user, const lonejson_value_path *path,
                                 const char *data, size_t len,
                                 lonejson_error *lj_error) {
  lc_pouch_query_scan_scalar_match_state *state;
  lc_error error;
  int rc;

  (void)path;
  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || !state->capturing || state->matched) {
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_scan_scalar_scratch_reserve(state, len, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_scan_scalar_lonejson_error(lj_error, &error);
  }
  memcpy(state->scratch + state->scratch_len, data, len);
  state->scratch_len += len;
  state->scratch[state->scratch_len] = '\0';
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_scan_scalar_end(void *user, const lonejson_value_path *path,
                               lonejson_error *lj_error) {
  lc_pouch_query_scan_scalar_match_state *state;

  (void)path;
  (void)lj_error;
  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || !state->capturing) {
    return LONEJSON_STATUS_OK;
  }
  if (lc_pouch_query_scan_scalar_value_matches(
          state->plan, state->scratch != NULL ? state->scratch : "",
          state->scratch_len, state->capture_type)) {
    state->capturing = 0;
    return lc_pouch_query_scan_scalar_stop_after_decision(state, 1, lj_error);
  }
  state->capturing = 0;
  return lc_pouch_query_scan_scalar_stop_after_decision(state, 0, lj_error);
}

static lonejson_status
lc_pouch_query_scan_scalar_boolean_value(void *user,
                                         const lonejson_value_path *path,
                                         int value, lonejson_error *lj_error) {
  lc_pouch_query_scan_scalar_match_state *state;
  const char *text;

  (void)lj_error;
  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || state->matched ||
      !lc_pouch_query_scan_scalar_field_matches_path(state->plan->field,
                                                     path)) {
    return LONEJSON_STATUS_OK;
  }
  text = value ? "true" : "false";
  if (lc_pouch_query_scan_scalar_value_matches(state->plan, text, strlen(text),
                                               'b')) {
    return lc_pouch_query_scan_scalar_stop_after_decision(state, 1, lj_error);
  }
  return lc_pouch_query_scan_scalar_stop_after_decision(state, 0, lj_error);
}

static lonejson_status lc_pouch_query_scan_scalar_null_value(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_scan_scalar_match_state *state;

  (void)lj_error;
  state = (lc_pouch_query_scan_scalar_match_state *)user;
  if (state == NULL || state->matched ||
      !lc_pouch_query_scan_scalar_field_matches_path(state->plan->field,
                                                     path)) {
    return LONEJSON_STATUS_OK;
  }
  if (lc_pouch_query_scan_scalar_value_matches(state->plan, "null", 4U, 'z')) {
    return lc_pouch_query_scan_scalar_stop_after_decision(state, 1, lj_error);
  }
  return lc_pouch_query_scan_scalar_stop_after_decision(state, 0, lj_error);
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_test_scalar_scan(lc_source *source, const char *field,
                              const char *value, char type, int *decided,
                              int *matched, lc_error *error) {
  lc_pouch_query_scan_scalar_match_state state;
  lc_pouch_query_index_plan plan;
  char *values[1];
  char types[1];
  int rc;
  memset(&state, 0, sizeof(state));
  memset(&plan, 0, sizeof(plan));
  values[0] = (char *)value;
  types[0] = type;
  plan.field = (char *)field;
  plan.values = values;
  plan.value_types = types;
  plan.value_count = 1U;
  state.source = source;
  state.plan = &plan;
  rc = lc_pouch_query_match_scan_scalar_body_direct(&state, decided, matched,
                                                    error);
  lc_pouch_query_scan_scalar_match_state_cleanup(&state);
  return rc;
}
#endif

static int lc_pouch_query_scan_scalar_plan_supported(
    const lc_pouch_query_index_plan *plan) {
  return plan != NULL && plan->field != NULL && plan->field[0] == '/' &&
         strstr(plan->field, "[]") == NULL &&
         strstr(plan->field, "...") == NULL &&
         strstr(plan->field, "**") == NULL && plan->value_count > 0U &&
         !plan->exists && !plan->range && !plan->date && !plan->root_or;
}

static int
lc_pouch_query_match_scan_scalar_body(lc_pouch_query_scan_context *context,
                                      lc_source *body, int *matched,
                                      lc_error *error) {
  lc_pouch_query_scan_scalar_match_state state;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  if (context == NULL || body == NULL || matched == NULL ||
      context->scan_scalar_plan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan scalar match requires context, body, and "
                        "plan",
                        NULL, NULL, NULL);
  }
  *matched = 0;
  if (body->reset != NULL) {
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(&state, 0, sizeof(state));
  lc_error_init(&state.read_error);
  state.source = body;
  state.plan = context->scan_scalar_plan;
  if (state.plan->contains && state.plan->value_count == 1U &&
      state.plan->value_types[0] == 's') {
    lc_error_init(&state.contains_matcher.read_error);
    rc = lc_pouch_query_any_text_prepare(&state.contains_matcher,
                                         state.plan->values[0],
                                         state.plan->ignore_case, error);
    if (rc != LC_OK) {
      lc_pouch_query_scan_scalar_match_state_cleanup(&state);
      return rc;
    }
    state.has_contains_matcher = 1;
  }
  {
    int direct_decided;
    int direct_matched;
    lc_error direct_error;

    direct_decided = 0;
    direct_matched = 0;
    lc_error_init(&direct_error);
    rc = lc_pouch_query_match_scan_scalar_body_direct(
        &state, &direct_decided, &direct_matched, &direct_error);
    if (rc == LC_OK && direct_decided) {
      *matched = direct_matched ? 1 : 0;
      lc_error_cleanup(&direct_error);
      lc_pouch_query_scan_scalar_match_state_cleanup(&state);
      return LC_OK;
    }
    if (rc != LC_OK && rc != LC_ERR_INVALID) {
      if (error != NULL) {
        if (direct_error.code != LC_OK) {
          *error = direct_error;
          lc_error_init(&direct_error);
        } else {
          (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                             "pouch scan scalar selector was not decided by "
                             "the direct scanner",
                             NULL, NULL, "pouch");
        }
      }
      rc = direct_error.code != LC_OK ? direct_error.code : LC_ERR_INVALID;
      lc_error_cleanup(&direct_error);
      lc_pouch_query_scan_scalar_match_state_cleanup(&state);
      return rc;
    }
    lc_error_cleanup(&direct_error);
    if (body->reset == NULL) {
      lc_pouch_query_scan_scalar_match_state_cleanup(&state);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch scan scalar source cannot retry after direct "
                          "path evaluation",
                          NULL, NULL, "pouch");
    }
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      lc_pouch_query_scan_scalar_match_state_cleanup(&state);
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    lc_pouch_query_scan_scalar_match_state_cleanup(&state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch scan scalar JSON runtime",
                        NULL, NULL, NULL);
  }
  visitor = lonejson_default_path_value_visitor();
  visitor.string_begin = lc_pouch_query_scan_scalar_string_begin;
  visitor.string_chunk = lc_pouch_query_scan_scalar_chunk;
  visitor.string_end = lc_pouch_query_scan_scalar_end;
  visitor.number_begin = lc_pouch_query_scan_scalar_number_begin;
  visitor.number_chunk = lc_pouch_query_scan_scalar_chunk;
  visitor.number_end = lc_pouch_query_scan_scalar_end;
  visitor.boolean_value = lc_pouch_query_scan_scalar_boolean_value;
  visitor.null_value = lc_pouch_query_scan_scalar_null_value;
  lonejson_error_init(&lj_error);
  status = runtime->visit_path_value_reader(
      runtime, lc_pouch_query_scan_scalar_lonejson_read, &state, &visitor,
      &state, &lj_error);
  if (state.read_error.code != LC_OK) {
    rc = state.read_error.code;
    if (error != NULL) {
      *error = state.read_error;
      memset(&state.read_error, 0, sizeof(state.read_error));
    }
  } else if (status == LONEJSON_STATUS_CALLBACK_FAILED &&
             state.stopped_after_decision) {
    *matched = state.matched ? 1 : 0;
    rc = LC_OK;
  } else if (status != LONEJSON_STATUS_OK) {
    rc = lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to evaluate pouch scan scalar query");
  } else {
    *matched = state.matched ? 1 : 0;
    rc = LC_OK;
  }
  lc_pouch_query_scan_scalar_match_state_cleanup(&state);
  return rc;
}

static lonejson_status
lc_pouch_query_any_text_string_end(void *user, const lonejson_value_path *path,
                                   lonejson_error *lj_error) {
  return lc_pouch_query_any_text_string_begin(user, path, lj_error);
}

static int lc_pouch_query_match_any_text_contains_body(
    lc_pouch_query_scan_context *context, lc_source *body, int *matched,
    lc_error *error) {
  lc_pouch_query_any_text_match_state state;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  if (context == NULL || body == NULL || matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch any-text body match requires context, body, "
                        "and matched output",
                        NULL, NULL, NULL);
  }
  *matched = 0;
  memset(&state, 0, sizeof(state));
  lc_error_init(&state.read_error);
  rc = lc_pouch_query_any_text_prepare(
      &state, context->any_text_contains_needle,
      context->any_text_contains_ignore_case, error);
  if (rc != LC_OK) {
    lc_pouch_query_any_text_match_state_cleanup(&state);
    return rc;
  }
  if (body->reset != NULL) {
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      lc_pouch_query_any_text_match_state_cleanup(&state);
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    lc_pouch_query_any_text_match_state_cleanup(&state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch any-text JSON runtime",
                        NULL, NULL, NULL);
  }
  state.source = body;
  visitor = lonejson_default_path_value_visitor();
  visitor.string_begin = lc_pouch_query_any_text_string_begin;
  visitor.string_chunk = lc_pouch_query_any_text_string_chunk;
  visitor.string_end = lc_pouch_query_any_text_string_end;
  lonejson_error_init(&lj_error);
  status = runtime->visit_path_value_reader(
      runtime, lc_pouch_query_any_text_lonejson_read, &state, &visitor, &state,
      &lj_error);
  if (state.read_error.code != LC_OK) {
    rc = state.read_error.code;
    if (error != NULL) {
      *error = state.read_error;
      memset(&state.read_error, 0, sizeof(state.read_error));
    }
  } else if (status != LONEJSON_STATUS_OK) {
    rc = lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to evaluate pouch any-text query");
  } else {
    *matched = state.matched ? 1 : 0;
    rc = LC_OK;
  }
  lc_pouch_query_any_text_match_state_cleanup(&state);
  return rc;
}

static int lc_pouch_query_match_body(lc_pouch_query_scan_context *context,
                                     lc_source *body, int *matched,
                                     lc_error *error) {
  lc_pouch_query_source_reader reader;
  lc_pouch_query_match_state match_state;
  lql_stream_request request;
  lql_stream_result result;
  lql_error lql_error_value;
  lql_status status;

  if (context == NULL || body == NULL || matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query body match requires context, body, and "
                        "matched output",
                        NULL, NULL, NULL);
  }
  *matched = 0;
  if (context->scan_scalar_plan != NULL) {
    return lc_pouch_query_match_scan_scalar_body(context, body, matched, error);
  }
  if (context->any_text_contains_needle != NULL) {
    return lc_pouch_query_match_any_text_contains_body(context, body, matched,
                                                       error);
  }
  if (context->stream_buffer == NULL) {
    context->stream_buffer = (unsigned char *)lc_alloc_with_allocator(
        &context->client->allocator, LC_POUCH_QUERY_STREAM_READER_BUFFER_BYTES);
    if (context->stream_buffer == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query stream buffer", NULL,
                          NULL, "pouch");
    }
  }
  memset(&reader, 0, sizeof(reader));
  memset(&match_state, 0, sizeof(match_state));
  memset(&request, 0, sizeof(request));
  memset(&result, 0, sizeof(result));
  lql_error_init(&lql_error_value);
  reader.source = body;
  reader.buffer = context->stream_buffer;
  reader.buffer_capacity = LC_POUCH_QUERY_STREAM_READER_BUFFER_BYTES;
  request.reader = lc_pouch_query_lql_read;
  request.reader_user = &reader;
  request.selector = context->selector;
  request.on_decision = lc_pouch_query_lql_decision;
  request.decision_user = &match_state;
  request.limits.max_records = 1U;
  status = context->runtime->stream_apply(context->runtime, &request, &result,
                                          &lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to evaluate pouch query selector");
  }
  *matched = match_state.matched;
  return LC_OK;
}

static int lc_pouch_query_emit_key(const lc_query_key_handler *handler,
                                   void *handler_context, const char *key,
                                   lc_error *error) {
  if (handler != NULL && handler->begin != NULL) {
    if (!handler->begin(handler_context, error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (key != NULL && key[0] != '\0' && handler != NULL &&
      handler->chunk != NULL) {
    if (!handler->chunk(handler_context, key, strlen(key), error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (handler != NULL && handler->end != NULL) {
    if (!handler->end(handler_context, error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  return LC_OK;
}

static int lc_pouch_query_emit_document(lc_pouch_query_scan_context *context,
                                        lc_source *body, lc_error *error) {
  static const char newline[] = "\n";
  int rc;

  if (context == NULL || context->sink == NULL || body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch document query requires a sink and body", NULL,
                        NULL, NULL);
  }
  if (lc_sink_is_discard(context->sink)) {
    return LC_OK;
  }
  if (body->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch document query body is not resettable", NULL,
                        NULL, "pouch");
  }
  rc = body->reset(body, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_copy(body, context->sink, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!context->sink->write(context->sink, newline, sizeof(newline) - 1U,
                            error)) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static size_t lc_pouch_query_effective_limit(long requested) {
  if (requested <= 0L) {
    return (size_t)LC_POUCH_QUERY_DEFAULT_LIMIT;
  }
  if (requested > LC_POUCH_QUERY_MAX_LIMIT) {
    return (size_t)LC_POUCH_QUERY_MAX_LIMIT;
  }
  return (size_t)requested;
}

static size_t lc_pouch_query_scan_summary_page_limit(size_t result_limit) {
  size_t page;

  if (result_limit == 0U) {
    return LC_POUCH_QUERY_SCAN_SUMMARY_DEFAULT_PAGE;
  }
  page = result_limit * 2U;
  if (page < LC_POUCH_QUERY_SCAN_SUMMARY_MIN_PAGE) {
    page = LC_POUCH_QUERY_SCAN_SUMMARY_MIN_PAGE;
  }
  if (page > LC_POUCH_QUERY_SCAN_SUMMARY_MAX_PAGE) {
    page = LC_POUCH_QUERY_SCAN_SUMMARY_MAX_PAGE;
  }
  return page;
}

static int
lc_pouch_query_index_summary_visit(const lc_pouch_query_index_row_view *row,
                                   void *scan_context, lc_error *error) {
  lc_pouch_query_scan_context *context;
  int active;
  int emit;
  int rc;
  int stop;

  context = (lc_pouch_query_scan_context *)scan_context;
  if (context == NULL || row == NULL || row->key == NULL) {
    return LC_OK;
  }
  if (row->has_query_hidden && row->query_hidden) {
    return LC_OK;
  }
  if (strncmp(row->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(row->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(context, row->version);
  context->candidate_key = row->key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(context, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  emit = 0;
  stop = 0;
  rc = lc_pouch_query_page_accept_match(context, &emit, &stop, error);
  if (rc != LC_OK || !emit) {
    return rc == LC_OK && stop ? LC_POUCH_STATE_READ_MANY_STOP : rc;
  }
  rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                               row->key, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_page_mark_emitted(context, error);
  }
  return rc;
}

static int lc_pouch_query_parse_cursor(const char *cursor, const char **out,
                                       lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query cursor requires output storage", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  if (cursor == NULL || cursor[0] == '\0') {
    return LC_OK;
  }
  *out = cursor;
  return LC_OK;
}

static char *lc_pouch_query_cursor_string(const char *key, lc_error *error) {
  char *copy;

  if (key == NULL || key[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L, "pouch query cursor requires a key",
                 NULL, NULL, NULL);
    return NULL;
  }
  copy = lc_strdup_local(key);
  if (copy == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query cursor", NULL, NULL, NULL);
  }
  return copy;
}

static char *lc_pouch_query_scan_metadata_string(size_t candidates,
                                                 size_t matches,
                                                 lc_error *error) {
  char stack[160];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"scan\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static char *lc_pouch_query_index_summary_metadata_string(size_t candidates,
                                                          size_t matches,
                                                          lc_error *error) {
  char stack[192];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"index-summary\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static char *lc_pouch_query_index_metadata_string(size_t candidates,
                                                  size_t matches,
                                                  lc_error *error) {
  char stack[160];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"index\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static void lc_pouch_query_selector_scalar_list_cleanup(
    lc_pouch_query_selector_scalar_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    lc_free_with_allocator(NULL, list->items[index].value);
  }
  lc_free_with_allocator(NULL, list->items);
  lc_free_with_allocator(NULL, list->scratch);
  memset(list, 0, sizeof(*list));
}

static void lc_pouch_query_index_plan_cleanup(lc_pouch_query_index_plan *plan) {
  size_t index;

  if (plan == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, plan->field);
  for (index = 0U; index < plan->value_count; ++index) {
    lc_free_with_allocator(NULL, plan->values[index]);
  }
  for (index = 0U; index < plan->or_term_count; ++index) {
    lc_free_with_allocator(NULL, (char *)plan->or_terms[index].field);
    lc_free_with_allocator(NULL, (char *)plan->or_terms[index].value);
  }
  lc_free_with_allocator(NULL, (char *)plan->date_bounds.gt);
  lc_free_with_allocator(NULL, (char *)plan->date_bounds.gte);
  lc_free_with_allocator(NULL, (char *)plan->date_bounds.lt);
  lc_free_with_allocator(NULL, (char *)plan->date_bounds.lte);
  lc_free_with_allocator(NULL, plan->values);
  lc_free_with_allocator(NULL, plan->value_types);
  lc_free_with_allocator(NULL, plan->or_terms);
  memset(plan, 0, sizeof(*plan));
}

static int lc_pouch_query_index_plan_candidate_results_are_exact(
    const lc_pouch_query_index_plan *plan) {
  size_t index;

  if (plan == NULL || !plan->candidates_exact) {
    return 0;
  }
  if (plan->root_or) {
    for (index = 0U; index < plan->or_term_count; ++index) {
      if (!lc_pouch_query_index_scalar_candidates_exact(
              plan->or_terms[index].value, plan->or_terms[index].value_type)) {
        return 0;
      }
    }
    return 1;
  }
  if (plan->value_count > 0U && !plan->prefix && !plan->contains &&
      !plan->range && !plan->date) {
    for (index = 0U; index < plan->value_count; ++index) {
      if (!lc_pouch_query_index_scalar_candidates_exact(
              plan->values[index], plan->value_types[index])) {
        return 0;
      }
    }
  }
  return 1;
}

static char *lc_pouch_query_dup_lql_string(lql_string_view view,
                                           lc_error *error) {
  char *out;

  if (view.data == NULL && view.len > 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query selector has invalid string view", NULL, NULL,
                 "pouch");
    return NULL;
  }
  out = (char *)lc_alloc_with_allocator(NULL, view.len + 1U);
  if (out == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query selector term", NULL, NULL,
                 NULL);
    return NULL;
  }
  if (view.len > 0U) {
    memcpy(out, view.data, view.len);
  }
  out[view.len] = '\0';
  return out;
}

static char *lc_pouch_query_dup_bytes(const char *bytes, size_t length,
                                      lc_error *error) {
  char *out;

  if (bytes == NULL && length > 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query selector has invalid scalar view", NULL, NULL,
                 "pouch");
    return NULL;
  }
  out = (char *)lc_alloc_with_allocator(NULL, length + 1U);
  if (out == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query selector scalar", NULL, NULL,
                 NULL);
    return NULL;
  }
  if (length > 0U) {
    memcpy(out, bytes, length);
  }
  out[length] = '\0';
  return out;
}

static char *lc_pouch_query_dup_exists_candidate_path(lql_string_view path,
                                                      lc_error *error) {
  size_t offset;

  if (path.len >= 3U && memcmp(path.data + path.len - 3U, "/**", 3U) == 0) {
    if (path.len == 3U) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query index engine does not support root recursive "
                   "exists selectors",
                   NULL, NULL, "pouch");
      return NULL;
    }
  }
  offset = 0U;
  while (offset < path.len) {
    size_t segment_start;
    size_t segment_end;
    size_t segment_len;

    if (path.data[offset] != '/') {
      break;
    }
    segment_start = offset + 1U;
    segment_end = segment_start;
    while (segment_end < path.len && path.data[segment_end] != '/') {
      ++segment_end;
    }
    segment_len = segment_end - segment_start;
    if (segment_len == 1U && path.data[segment_start] == '*') {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query index engine does not support wildcard exists "
                   "selectors",
                   NULL, NULL, "pouch");
      return NULL;
    }
    if (segment_len == 2U && path.data[segment_start] == '*' &&
        path.data[segment_start + 1U] == '*' && segment_end != path.len) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query index engine supports recursive exists "
                   "selectors only at the end of a path",
                   NULL, NULL, "pouch");
      return NULL;
    }
    offset = segment_end;
  }
  return lc_pouch_query_dup_lql_string(path, error);
}

static int lc_pouch_query_path_segment_eq(const lonejson_value_path *path,
                                          size_t index, const char *text) {
  size_t len;

  if (path == NULL || text == NULL || index >= path->segment_count) {
    return 0;
  }
  len = strlen(text);
  return path->segments[index].len == len &&
         memcmp(path->segments[index].data, text, len) == 0;
}

static int lc_pouch_query_path_segment_is_index(const lonejson_value_path *path,
                                                size_t index) {
  size_t byte_index;

  if (path == NULL || index >= path->segment_count ||
      path->segments[index].len == 0U) {
    return 0;
  }
  for (byte_index = 0U; byte_index < path->segments[index].len; ++byte_index) {
    char byte;

    byte = path->segments[index].data[byte_index];
    if (byte < '0' || byte > '9') {
      return 0;
    }
  }
  return 1;
}

static int
lc_pouch_query_selector_scalar_path(const lonejson_value_path *path) {
  size_t count;

  if (path == NULL || path->segment_count < 2U) {
    return 0;
  }
  count = path->segment_count;
  if (lc_pouch_query_path_segment_eq(path, count - 1U, "value") &&
      lc_pouch_query_path_segment_eq(path, count - 2U, "eq")) {
    return 1;
  }
  if (count >= 3U && lc_pouch_query_path_segment_is_index(path, count - 1U) &&
      lc_pouch_query_path_segment_eq(path, count - 2U, "any") &&
      lc_pouch_query_path_segment_eq(path, count - 3U, "in")) {
    return 1;
  }
  return 0;
}

static int lc_pouch_query_selector_scalar_list_reserve(
    lc_pouch_query_selector_scalar_list *list, size_t needed, lc_error *error) {
  lc_pouch_query_selector_scalar *next_items;
  size_t next_capacity;

  if (needed <= list->capacity) {
    return LC_OK;
  }
  next_capacity = list->capacity == 0U ? 4U : list->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query selector scalar list exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_query_selector_scalar *)lc_realloc_with_allocator(
      NULL, list->items, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query selector scalars", NULL,
                        NULL, NULL);
  }
  memset(next_items + list->capacity, 0,
         (next_capacity - list->capacity) * sizeof(*next_items));
  list->items = next_items;
  list->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_selector_scalar_scratch_reserve(
    lc_pouch_query_selector_scalar_list *list, size_t extra, lc_error *error) {
  char *next;
  size_t needed;
  size_t next_capacity;

  if (extra > (size_t)-1 - list->scratch_len - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query selector scalar exceeds local limit", NULL,
                        NULL, NULL);
  }
  needed = list->scratch_len + extra + 1U;
  if (needed <= list->scratch_capacity) {
    return LC_OK;
  }
  next_capacity = list->scratch_capacity == 0U ? 32U : list->scratch_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query selector scalar exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (char *)lc_realloc_with_allocator(NULL, list->scratch, next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query selector scalar", NULL,
                        NULL, NULL);
  }
  list->scratch = next;
  list->scratch_capacity = next_capacity;
  list->scratch[list->scratch_len] = '\0';
  return LC_OK;
}

static int
lc_pouch_query_selector_scalar_add(lc_pouch_query_selector_scalar_list *list,
                                   const char *value, size_t value_len,
                                   char value_type, lc_error *error) {
  char *copy;
  int rc;

  if (value_type != 's' && value_type != 'n' && value_type != 'b' &&
      value_type != 'z') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector scalar has invalid JSON type",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_query_selector_scalar_list_reserve(list, list->count + 1U,
                                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  copy = lc_pouch_query_dup_bytes(value, value_len, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  list->items[list->count].value = copy;
  list->items[list->count].value_type = value_type;
  ++list->count;
  return LC_OK;
}

static lonejson_status
lc_pouch_query_selector_lonejson_error(lonejson_error *lj_error,
                                       const lc_error *error) {
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = error != NULL && error->code == LC_ERR_NOMEM
                         ? LONEJSON_STATUS_ALLOCATION_FAILED
                         : LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message), "%s",
             error != NULL && error->message != NULL
                 ? error->message
                 : "pouch query selector scalar callback failed");
  }
  return lj_error != NULL ? lj_error->code : LONEJSON_STATUS_CALLBACK_FAILED;
}

static lonejson_status lc_pouch_query_selector_scalar_begin(
    void *user, const lonejson_value_path *path, char value_type,
    lonejson_error *lj_error) {
  lc_pouch_query_selector_scalar_list *list;
  lc_error error;
  int rc;

  (void)lj_error;
  list = (lc_pouch_query_selector_scalar_list *)user;
  if (!lc_pouch_query_selector_scalar_path(path)) {
    list->capturing = 0;
    return LONEJSON_STATUS_OK;
  }
  list->scratch_len = 0U;
  if (list->scratch != NULL) {
    list->scratch[0] = '\0';
  }
  list->capturing = 1;
  list->capture_type = value_type;
  lc_error_init(&error);
  rc = lc_pouch_query_selector_scalar_scratch_reserve(list, 0U, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_selector_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_selector_string_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_selector_scalar_begin(user, path, 's', lj_error);
}

static lonejson_status lc_pouch_query_selector_number_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_selector_scalar_begin(user, path, 'n', lj_error);
}

static lonejson_status lc_pouch_query_selector_scalar_chunk(
    void *user, const lonejson_value_path *path, const char *data, size_t len,
    lonejson_error *lj_error) {
  lc_pouch_query_selector_scalar_list *list;
  lc_error error;
  int rc;

  (void)path;
  list = (lc_pouch_query_selector_scalar_list *)user;
  if (!list->capturing) {
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_selector_scalar_scratch_reserve(list, len, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_selector_lonejson_error(lj_error, &error);
  }
  memcpy(list->scratch + list->scratch_len, data, len);
  list->scratch_len += len;
  list->scratch[list->scratch_len] = '\0';
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_selector_scalar_end(void *user, const lonejson_value_path *path,
                                   lonejson_error *lj_error) {
  lc_pouch_query_selector_scalar_list *list;
  lc_error error;
  int rc;

  (void)path;
  list = (lc_pouch_query_selector_scalar_list *)user;
  if (!list->capturing) {
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_selector_scalar_add(
      list, list->scratch, list->scratch_len, list->capture_type, &error);
  list->capturing = 0;
  if (rc != LC_OK) {
    return lc_pouch_query_selector_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_selector_boolean_value(void *user,
                                      const lonejson_value_path *path,
                                      int value, lonejson_error *lj_error) {
  lc_pouch_query_selector_scalar_list *list;
  lc_error error;
  const char *text;
  int rc;

  list = (lc_pouch_query_selector_scalar_list *)user;
  if (!lc_pouch_query_selector_scalar_path(path)) {
    return LONEJSON_STATUS_OK;
  }
  text = value ? "true" : "false";
  lc_error_init(&error);
  rc =
      lc_pouch_query_selector_scalar_add(list, text, strlen(text), 'b', &error);
  if (rc != LC_OK) {
    return lc_pouch_query_selector_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_selector_null_value(void *user, const lonejson_value_path *path,
                                   lonejson_error *lj_error) {
  lc_pouch_query_selector_scalar_list *list;
  lc_error error;
  int rc;

  list = (lc_pouch_query_selector_scalar_list *)user;
  if (!lc_pouch_query_selector_scalar_path(path)) {
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_selector_scalar_add(list, "null", 4U, 'z', &error);
  if (rc != LC_OK) {
    return lc_pouch_query_selector_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static int lc_pouch_query_selector_json_alloc(lql *runtime,
                                              const lql_selector *selector,
                                              char **out, lc_error *error) {
  lql_error lql_error_value;
  lql_status status;
  FILE *fp;
  char *json;
  size_t json_length;

  if (runtime == NULL || selector == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector serialization requires runtime, "
                        "selector, and output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  json = NULL;
  json_length = 0U;
  fp = open_memstream(&json, &json_length);
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, (long)errno,
                        "failed to create pouch query selector serialization "
                        "stream",
                        strerror(errno), NULL, "liblql");
  }
  lql_error_init(&lql_error_value);
  status =
      runtime->selector_write_json(runtime, selector, fp, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    fclose(fp);
    lc_free_local(json);
    return lc_pouch_query_lql_error(
        error, status, &lql_error_value,
        "failed to serialize pouch query selector for index planning");
  }
  if (fclose(fp) != 0) {
    int saved_errno;

    saved_errno = errno;
    lc_free_local(json);
    return lc_error_set(error, LC_ERR_PROTOCOL, (long)saved_errno,
                        "failed to finalize pouch query selector "
                        "serialization",
                        strerror(saved_errno), NULL, "liblql");
  }
  if (json == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query selector JSON", NULL,
                        NULL, NULL);
  }
  (void)json_length;
  *out = json;
  return LC_OK;
}

static int lc_pouch_query_collect_selector_scalars(
    lql *lql_runtime, const lql_selector *selector,
    lc_pouch_query_selector_scalar_list *list, lc_error *error) {
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *json_runtime;
  lonejson_status status;
  char *json;
  int rc;

  if (lql_runtime == NULL || selector == NULL || list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector scalar collection requires "
                        "runtime, selector, and output",
                        NULL, NULL, NULL);
  }
  memset(list, 0, sizeof(*list));
  json = NULL;
  rc = lc_pouch_query_selector_json_alloc(lql_runtime, selector, &json, error);
  if (rc != LC_OK) {
    return rc;
  }
  json_runtime = lc_thread_lonejson_runtime();
  if (json_runtime == NULL) {
    lc_free_local(json);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch selector JSON runtime",
                        NULL, NULL, NULL);
  }
  memset(&visitor, 0, sizeof(visitor));
  visitor.string_begin = lc_pouch_query_selector_string_begin;
  visitor.string_chunk = lc_pouch_query_selector_scalar_chunk;
  visitor.string_end = lc_pouch_query_selector_scalar_end;
  visitor.number_begin = lc_pouch_query_selector_number_begin;
  visitor.number_chunk = lc_pouch_query_selector_scalar_chunk;
  visitor.number_end = lc_pouch_query_selector_scalar_end;
  visitor.boolean_value = lc_pouch_query_selector_boolean_value;
  visitor.null_value = lc_pouch_query_selector_null_value;
  lonejson_error_init(&lj_error);
  status = lonejson_visit_path_value_cstr(json_runtime, json, &visitor, list,
                                          &lj_error);
  lc_free_local(json);
  if (status != LONEJSON_STATUS_OK) {
    lc_pouch_query_selector_scalar_list_cleanup(list);
    return lc_error_set(
        error,
        status == LONEJSON_STATUS_ALLOCATION_FAILED ? LC_ERR_NOMEM
                                                    : LC_ERR_INVALID,
        0L, "failed to inspect pouch query selector scalars", lj_error.message,
        lonejson_status_string(status), "pouch");
  }
  return LC_OK;
}

int lc_pouch_query_selector_exact_key(lql *runtime,
                                      const lql_selector *selector,
                                      const char *field,
                                      const lc_allocator *allocator,
                                      lc_pouch_index_term_key **out,
                                      size_t *count, lc_error *error) {
  lc_pouch_query_selector_scalar_list scalars;
  lc_pouch_index_plain_term term;
  int rc;
  *out = NULL;
  *count = 0U;
  rc = lc_pouch_query_collect_selector_scalars(runtime, selector, &scalars,
                                               error);
  if (rc == LC_OK && scalars.count != 1U)
    rc = lc_error_set(
        error, LC_ERR_UNSUPPORTED, 0L,
        "collection exact lookup requires one typed selector value", NULL, NULL,
        NULL);
  if (rc == LC_OK) {
    term.field = field;
    term.value = scalars.items[0].value;
    term.value_type = scalars.items[0].value_type;
    rc = lc_pouch_query_index_build_exact_terms(&term, 1U, out, count,
                                                allocator, error);
  }
  lc_pouch_query_selector_scalar_list_cleanup(&scalars);
  return rc;
}

static const lc_pouch_query_selector_scalar *
lc_pouch_query_selector_scalar_next(lc_pouch_query_selector_scalar_list *list,
                                    lc_error *error) {
  if (list == NULL || list->cursor >= list->count) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query selector scalar type metadata is incomplete",
                 NULL, NULL, "pouch");
    return NULL;
  }
  return &list->items[list->cursor++];
}

static int lc_pouch_query_index_plan_add_value(lc_pouch_query_index_plan *plan,
                                               const char *value,
                                               size_t value_len,
                                               char value_type,
                                               lc_error *error) {
  char **next_values;
  char *next_value_types;
  size_t next_capacity;
  char *copy;

  if (value_type != 's' && value_type != 'n' && value_type != 'b' &&
      value_type != 'z') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector value requires a JSON scalar "
                        "type",
                        NULL, NULL, "pouch");
  }
  if (plan->value_count >= plan->value_capacity) {
    next_capacity = plan->value_capacity == 0U ? 4U : plan->value_capacity;
    while (next_capacity <= plan->value_count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query selector value list exceeds local "
                            "limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_values = (char **)lc_alloc_with_allocator(
        NULL, next_capacity * sizeof(*next_values));
    if (next_values == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query selector values",
                          NULL, NULL, NULL);
    }
    next_value_types = (char *)lc_alloc_with_allocator(NULL, next_capacity);
    if (next_value_types == NULL) {
      lc_free_with_allocator(NULL, next_values);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query selector value "
                          "types",
                          NULL, NULL, NULL);
    }
    if (plan->value_count > 0U) {
      memcpy(next_values, plan->values,
             plan->value_count * sizeof(*next_values));
      memcpy(next_value_types, plan->value_types, plan->value_count);
    }
    memset(next_values + plan->value_capacity, 0,
           (next_capacity - plan->value_capacity) * sizeof(*next_values));
    memset(next_value_types + plan->value_capacity, 0,
           next_capacity - plan->value_capacity);
    lc_free_with_allocator(NULL, plan->values);
    lc_free_with_allocator(NULL, plan->value_types);
    plan->values = next_values;
    plan->value_types = next_value_types;
    plan->value_capacity = next_capacity;
  }
  copy = lc_pouch_query_dup_bytes(value, value_len, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  plan->values[plan->value_count] = copy;
  plan->value_types[plan->value_count] = value_type;
  ++plan->value_count;
  return LC_OK;
}

static int lc_pouch_query_index_plan_add_or_term(
    lc_pouch_query_index_plan *plan, lql_string_view field, const char *value,
    size_t value_len, char value_type, lc_error *error) {
  lc_pouch_query_index_scalar_term *next_terms;
  size_t next_capacity;
  char *field_copy;
  char *value_copy;

  if (value_type != 's' && value_type != 'n' && value_type != 'b' &&
      value_type != 'z') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector root-or value requires a JSON "
                        "scalar type",
                        NULL, NULL, "pouch");
  }
  if (plan->or_term_count >= plan->or_term_capacity) {
    next_capacity = plan->or_term_capacity == 0U ? 4U : plan->or_term_capacity;
    while (next_capacity <= plan->or_term_count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query selector root or term list exceeds "
                            "local limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_terms = (lc_pouch_query_index_scalar_term *)lc_realloc_with_allocator(
        NULL, plan->or_terms, next_capacity * sizeof(*next_terms));
    if (next_terms == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query selector root or "
                          "terms",
                          NULL, NULL, NULL);
    }
    memset(next_terms + plan->or_term_capacity, 0,
           (next_capacity - plan->or_term_capacity) * sizeof(*next_terms));
    plan->or_terms = next_terms;
    plan->or_term_capacity = next_capacity;
  }
  field_copy = lc_pouch_query_dup_lql_string(field, error);
  if (field_copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  value_copy = lc_pouch_query_dup_bytes(value, value_len, error);
  if (value_copy == NULL) {
    lc_free_with_allocator(NULL, field_copy);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  plan->or_terms[plan->or_term_count].field = field_copy;
  plan->or_terms[plan->or_term_count].value = value_copy;
  plan->or_terms[plan->or_term_count].value_type = value_type;
  ++plan->or_term_count;
  return LC_OK;
}

static int lc_pouch_query_index_plan_add_or_child(
    lql *runtime, lql_selector_node child, lc_pouch_query_index_plan *plan,
    lc_pouch_query_selector_scalar_list *scalars, lql_error *lql_error_value,
    lc_error *error) {
  lql_selector_string_term string_term;
  const lc_pouch_query_selector_scalar *scalar;
  lql_status status;

  if (child.kind != LQL_SELECTOR_NODE_EQ) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports root or groups "
                        "whose children are exact scalar equality selectors "
                        "only",
                        NULL, NULL, "pouch");
  }
  memset(&string_term, 0, sizeof(string_term));
  status = runtime->selector_node_string_term(runtime, child, &string_term,
                                              lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(
        error, status, lql_error_value,
        "failed to inspect pouch root or equality selector");
  }
  if (!string_term.value_present || string_term.any_count != 0U ||
      string_term.field.len == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports root or exact "
                        "scalar equality selectors only",
                        NULL, NULL, "pouch");
  }
  scalar = lc_pouch_query_selector_scalar_next(scalars, error);
  if (scalar == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  return lc_pouch_query_index_plan_add_or_term(
      plan, string_term.field, scalar->value, strlen(scalar->value),
      scalar->value_type, error);
}

static int lc_pouch_query_index_plan_set_range_bound(
    lc_pouch_query_index_range_bounds *bounds,
    const lql_selector_range_bound *bound, int *has_bound, double *value,
    const char *label, lc_error *error) {
  (void)bounds;
  if (bound == NULL || has_bound == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query range planning requires bound outputs",
                        NULL, NULL, NULL);
  }
  if (bound->kind == LQL_SELECTOR_BOUND_ABSENT) {
    return LC_OK;
  }
  if (bound->kind != LQL_SELECTOR_BOUND_NUMBER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports numeric range "
                        "selectors only",
                        NULL, label, "pouch");
  }
  *has_bound = 1;
  *value = bound->number;
  return LC_OK;
}

static int lc_pouch_query_index_plan_set_date_bound(lql_string_view view,
                                                    int *has_bound,
                                                    const char **value,
                                                    lc_error *error) {
  char *copy;

  if (has_bound == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query date planning requires bound outputs",
                        NULL, NULL, NULL);
  }
  if (view.len == 0U) {
    return LC_OK;
  }
  copy = lc_pouch_query_dup_lql_string(view, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_free_with_allocator(NULL, (char *)*value);
  *value = copy;
  *has_bound = 1;
  return LC_OK;
}

static int lc_pouch_query_index_plan_from_selector(
    lql *runtime, const lql_selector *selector, lc_pouch_query_index_plan *plan,
    lc_error *error) {
  lql_selector_node root;
  lql_selector_string_term string_term;
  lql_selector_range_term range_term;
  lql_selector_date_term date_term;
  lql_selector_in_term in_term;
  lc_pouch_query_selector_scalar_list scalars;
  const lc_pouch_query_selector_scalar *scalar;
  lql_error lql_error_value;
  lql_status status;
  size_t child_count;
  size_t index;

  if (runtime == NULL || selector == NULL || plan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index planning requires runtime, "
                        "selector, and plan",
                        NULL, NULL, NULL);
  }
  memset(plan, 0, sizeof(*plan));
  memset(&scalars, 0, sizeof(scalars));
  scalar = NULL;
  lql_error_init(&lql_error_value);
  status = runtime->selector_root(runtime, selector, &root, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    int rc;

    rc = lc_pouch_query_lql_error(error, status, &lql_error_value,
                                  "failed to inspect pouch query selector");
    lc_pouch_query_selector_scalar_list_cleanup(&scalars);
    return rc;
  }
  if (root.kind == LQL_SELECTOR_NODE_OR) {
    child_count = 0U;
    status = runtime->selector_node_child_count(runtime, root, &child_count,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      int rc;

      rc = lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to inspect pouch root or selector");
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    if (child_count == 0U) {
      int rc;

      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports non-empty root "
                        "or selectors only",
                        NULL, NULL, "pouch");
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    if (lc_pouch_query_collect_selector_scalars(runtime, selector, &scalars,
                                                error) != LC_OK) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_INVALID;
    }
    for (index = 0U; index < child_count; ++index) {
      lql_selector_node child;

      memset(&child, 0, sizeof(child));
      status = runtime->selector_node_child(runtime, root, index, &child,
                                            &lql_error_value);
      if (status != LQL_STATUS_OK) {
        int rc;

        rc = lc_pouch_query_lql_error(
            error, status, &lql_error_value,
            "failed to inspect pouch root or selector child");
        lc_pouch_query_selector_scalar_list_cleanup(&scalars);
        return rc;
      }
      if (lc_pouch_query_index_plan_add_or_child(runtime, child, plan, &scalars,
                                                 &lql_error_value,
                                                 error) != LC_OK) {
        int rc;

        rc = error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_INVALID;
        lc_pouch_query_selector_scalar_list_cleanup(&scalars);
        return rc;
      }
    }
    plan->root_or = 1;
    plan->candidates_exact = 1;
    lc_pouch_query_selector_scalar_list_cleanup(&scalars);
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_EQ) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      int rc;

      rc =
          lc_pouch_query_lql_error(error, status, &lql_error_value,
                                   "failed to inspect pouch equality selector");
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U) {
      int rc;

      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports exact "
                        "scalar equality selectors only",
                        NULL, NULL, "pouch");
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      int rc;

      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    if (lc_pouch_query_collect_selector_scalars(runtime, selector, &scalars,
                                                error) != LC_OK) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_INVALID;
    }
    scalar = lc_pouch_query_selector_scalar_next(&scalars, error);
    if (scalar == NULL) {
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_INVALID;
    }
    if (lc_pouch_query_index_plan_add_value(
            plan, scalar->value, strlen(scalar->value), scalar->value_type,
            error) != LC_OK) {
      int rc;

      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      lc_pouch_query_selector_scalar_list_cleanup(&scalars);
      return rc;
    }
    plan->candidates_exact = 1;
    lc_pouch_query_selector_scalar_list_cleanup(&scalars);
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_PREFIX ||
      root.kind == LQL_SELECTOR_NODE_IPREFIX) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch prefix selector");
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U || string_term.value.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "prefix selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    plan->prefix = 1;
    plan->ignore_case =
        string_term.ignore_case || root.kind == LQL_SELECTOR_NODE_IPREFIX ? 1
                                                                          : 0;
    plan->candidates_exact = 1;
    return lc_pouch_query_index_plan_add_value(
        plan, string_term.value.data, string_term.value.len, 's', error);
  }
  if (root.kind == LQL_SELECTOR_NODE_CONTAINS ||
      root.kind == LQL_SELECTOR_NODE_ICONTAINS) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch contains selector");
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U || string_term.value.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "contains selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    plan->contains = 1;
    plan->ignore_case =
        string_term.ignore_case || root.kind == LQL_SELECTOR_NODE_ICONTAINS ? 1
                                                                            : 0;
    plan->candidates_exact = 1;
    return lc_pouch_query_index_plan_add_value(
        plan, string_term.value.data, string_term.value.len, 's', error);
  }
  if (root.kind == LQL_SELECTOR_NODE_RANGE) {
    int rc;

    memset(&range_term, 0, sizeof(range_term));
    status = runtime->selector_node_range_term(runtime, root, &range_term,
                                               &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch range selector");
    }
    if (range_term.field.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty range "
                          "selectors only",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_query_index_plan_set_range_bound(
        &plan->range_bounds, &range_term.gt, &plan->range_bounds.has_gt,
        &plan->range_bounds.gt, "gt", error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.gte, &plan->range_bounds.has_gte,
          &plan->range_bounds.gte, "gte", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.lt, &plan->range_bounds.has_lt,
          &plan->range_bounds.lt, "lt", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.lte, &plan->range_bounds.has_lte,
          &plan->range_bounds.lte, "lte", error);
    }
    if (rc != LC_OK) {
      return rc;
    }
    if (!plan->range_bounds.has_gt && !plan->range_bounds.has_gte &&
        !plan->range_bounds.has_lt && !plan->range_bounds.has_lte) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports bounded range "
                          "selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_lql_string(range_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    plan->range = 1;
    plan->candidates_exact = 1;
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_DATE) {
    int rc;

    memset(&date_term, 0, sizeof(date_term));
    status = runtime->selector_node_date_term(runtime, root, &date_term,
                                              &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch date selector");
    }
    if (date_term.field.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty date "
                          "selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_lql_string(date_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    rc = LC_OK;
    if (date_term.value.len > 0U) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.value, &plan->date_bounds.has_gte, &plan->date_bounds.gte,
          error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_plan_set_date_bound(
            date_term.value, &plan->date_bounds.has_lte, &plan->date_bounds.lte,
            error);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.after, &plan->date_bounds.has_gt, &plan->date_bounds.gt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.before, &plan->date_bounds.has_lt, &plan->date_bounds.lt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.gt, &plan->date_bounds.has_gt, &plan->date_bounds.gt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.gte, &plan->date_bounds.has_gte, &plan->date_bounds.gte,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.lt, &plan->date_bounds.has_lt, &plan->date_bounds.lt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.lte, &plan->date_bounds.has_lte, &plan->date_bounds.lte,
          error);
    }
    if (rc != LC_OK) {
      return rc;
    }
    if (plan->date_bounds.has_gt || plan->date_bounds.has_gte ||
        plan->date_bounds.has_lt || plan->date_bounds.has_lte) {
      plan->date = 1;
      plan->candidates_exact = 1;
    } else {
      plan->exists = 1;
    }
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_IN) {
    memset(&in_term, 0, sizeof(in_term));
    status = runtime->selector_node_in_term(runtime, root, &in_term,
                                            &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch in selector");
    }
    if (in_term.field.len == 0U || in_term.any_count == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "scalar in selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_lql_string(in_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (lc_pouch_query_collect_selector_scalars(runtime, selector, &scalars,
                                                error) != LC_OK) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_INVALID;
    }
    for (index = 0U; index < in_term.any_count; ++index) {
      lql_string_view value;

      status = runtime->selector_node_in_term_any(runtime, root, index, &value,
                                                  &lql_error_value);
      if (status != LQL_STATUS_OK) {
        int rc;

        rc = lc_pouch_query_lql_error(
            error, status, &lql_error_value,
            "failed to inspect pouch in selector value");
        lc_pouch_query_selector_scalar_list_cleanup(&scalars);
        return rc;
      }
      scalar = lc_pouch_query_selector_scalar_next(&scalars, error);
      if (scalar == NULL) {
        lc_pouch_query_selector_scalar_list_cleanup(&scalars);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
      (void)value;
      if (lc_pouch_query_index_plan_add_value(
              plan, scalar->value, strlen(scalar->value), scalar->value_type,
              error) != LC_OK) {
        int rc;

        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
        lc_pouch_query_selector_scalar_list_cleanup(&scalars);
        return rc;
      }
    }
    plan->candidates_exact = 1;
    lc_pouch_query_selector_scalar_list_cleanup(&scalars);
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_EXISTS) {
    lql_string_view path;

    memset(&path, 0, sizeof(path));
    status = runtime->selector_node_exists_path(runtime, root, &path,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch exists selector");
    }
    if (path.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty exists "
                          "selectors only",
                          NULL, NULL, "pouch");
    }
    plan->field = lc_pouch_query_dup_exists_candidate_path(path, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    plan->exists = 1;
    plan->candidates_exact = 1;
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query index engine supports exact scalar "
                      "equality, in, exists, prefix, contains, range, and date "
                      "selectors only",
                      NULL, NULL, "pouch");
}

static void
lc_pouch_query_index_key_set_cleanup(lc_pouch_query_index_key_set *set) {
  size_t index;

  if (set == NULL) {
    return;
  }
  for (index = 0U; index < set->count; ++index) {
    lc_free_with_allocator(NULL, (char *)set->keys[index].key);
  }
  lc_free_with_allocator(NULL, set->keys);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_key_compare(const void *left,
                                            const void *right) {
  const lc_pouch_query_index_key_view *a;
  const lc_pouch_query_index_key_view *b;
  int cmp;

  a = (const lc_pouch_query_index_key_view *)left;
  b = (const lc_pouch_query_index_key_view *)right;
  cmp = strcmp(a->key, b->key);
  if (cmp != 0) {
    return cmp;
  }
  if (a->candidate_exact != b->candidate_exact) {
    return a->candidate_exact ? -1 : 1;
  }
  return 0;
}

static int
lc_pouch_query_index_key_set_add(lc_pouch_query_index_key_set *set,
                                 const lc_pouch_query_index_key_view *key,
                                 lc_error *error) {
  lc_pouch_query_index_key_view *next_keys;
  size_t next_capacity;

  if (key == NULL || key->key == NULL || key->key[0] == '\0') {
    return LC_OK;
  }
  if (set->count >= set->capacity) {
    next_capacity = set->capacity == 0U ? 16U : set->capacity;
    while (next_capacity <= set->count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query index key set exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_keys = (lc_pouch_query_index_key_view *)lc_realloc_with_allocator(
        NULL, set->keys, next_capacity * sizeof(*next_keys));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query index key set", NULL,
                          NULL, NULL);
    }
    memset(next_keys + set->capacity, 0,
           (next_capacity - set->capacity) * sizeof(*next_keys));
    set->keys = next_keys;
    set->capacity = next_capacity;
  }
  set->keys[set->count] = *key;
  set->keys[set->count].key = lc_strdup_with_allocator(NULL, key->key);
  if (set->keys[set->count].key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query index key", NULL, NULL,
                        NULL);
  }
  ++set->count;
  return LC_OK;
}

static int
lc_pouch_query_page_enter_candidate(lc_pouch_query_scan_context *context,
                                    int *active, lc_error *error) {
  if (context == NULL || active == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query page candidate requires context and "
                        "active output",
                        NULL, NULL, NULL);
  }
  *active = 0;
  if (context->candidate_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query page candidate requires key", NULL, NULL,
                        NULL);
  }
  if (context->start_after_key != NULL &&
      strcmp(context->candidate_key, context->start_after_key) <= 0) {
    return LC_OK;
  }
  ++context->seen;
  *active = 1;
  return LC_OK;
}

static int
lc_pouch_query_page_accept_match(lc_pouch_query_scan_context *context,
                                 int *emit, int *stop, lc_error *error) {
  if (context == NULL || emit == NULL || stop == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query page match requires context and outputs",
                        NULL, NULL, NULL);
  }
  *emit = 0;
  *stop = 0;
  ++context->matched;
  if (context->emitted >= context->limit) {
    context->page_full = 1;
    *stop = 1;
    return LC_OK;
  }
  *emit = 1;
  return LC_OK;
}

static int
lc_pouch_query_page_mark_emitted(lc_pouch_query_scan_context *context,
                                 lc_error *error) {
  char *copy;

  if (context == NULL || context->candidate_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query emitted page requires candidate key", NULL,
                        NULL, NULL);
  }
  ++context->emitted;
  if (context->emitted < context->limit) {
    return LC_OK;
  }
  copy = lc_strdup_local(context->candidate_key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query emitted cursor key",
                        NULL, NULL, NULL);
  }
  lc_free_local(context->last_emitted_key);
  context->last_emitted_key = copy;
  return LC_OK;
}

static int
lc_pouch_query_index_key_collect(const lc_pouch_query_index_key_view *key,
                                 void *context, lc_error *error) {
  return lc_pouch_query_index_key_set_add(
      (lc_pouch_query_index_key_set *)context, key, error);
}

static int lc_pouch_query_index_key_collect_marked(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error) {
  lc_pouch_query_index_key_collect_context *collect;
  lc_pouch_query_index_key_view marked;

  collect = (lc_pouch_query_index_key_collect_context *)context;
  if (collect == NULL || collect->keys == NULL || key == NULL) {
    return LC_OK;
  }
  marked = *key;
  marked.candidate_exact = collect->candidate_exact ? 1 : 0;
  return lc_pouch_query_index_key_set_add(collect->keys, &marked, error);
}

static int
lc_pouch_query_index_row_collect(const lc_pouch_query_index_row_view *row,
                                 void *context, lc_error *error) {
  lc_pouch_query_index_key_view key;

  if (row == NULL) {
    return LC_OK;
  }
  memset(&key, 0, sizeof(key));
  key.key = row->key;
  key.key_hex = row->key_hex;
  key.doc_id = row->doc_id;
  key.version = row->version;
  key.bytes = row->bytes;
  key.has_query_hidden = row->has_query_hidden;
  key.query_hidden = row->query_hidden;
  return lc_pouch_query_index_key_set_add(
      (lc_pouch_query_index_key_set *)context, &key, error);
}

static int
lc_pouch_query_index_process_exact_key(lc_pouch_query_scan_context *context,
                                       const lc_pouch_query_index_key_view *key,
                                       lc_error *error) {
  int rc;
  int active;
  int emit;
  int stop;

  if (context == NULL || key == NULL || key->key == NULL) {
    return LC_OK;
  }
  if (key->has_query_hidden && key->query_hidden) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(context, key->version);
  context->candidate_key = key->key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(context, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  emit = 0;
  stop = 0;
  rc = lc_pouch_query_page_accept_match(context, &emit, &stop, error);
  if (rc != LC_OK || !emit) {
    return rc == LC_OK && stop ? LC_POUCH_STATE_READ_MANY_STOP : rc;
  }
  rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                               key->key, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_page_mark_emitted(context, error);
  }
  return rc;
}

static int lc_pouch_query_index_process_exact_key_visit(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error) {
  return lc_pouch_query_index_process_exact_key(
      (lc_pouch_query_scan_context *)context, key, error);
}

static void
lc_pouch_query_scan_reset_page_state(lc_pouch_query_scan_context *context) {
  if (context == NULL) {
    return;
  }
  context->candidate_key = NULL;
  lc_free_local(context->last_emitted_key);
  context->last_emitted_key = NULL;
  context->seen = 0U;
  context->emitted = 0U;
  context->matched = 0U;
  context->index_seq = 0UL;
  context->page_full = 0;
  context->indexed_candidates_exact = 0;
}

static int lc_pouch_query_index_process_exact_discard_document_key(
    lc_pouch_query_scan_context *context,
    const lc_pouch_query_index_key_view *key, lc_error *error) {
  int active;
  int emit;
  int rc;
  int stop;

  if (context == NULL || key == NULL || key->key == NULL) {
    return LC_OK;
  }
  if (key->has_query_hidden && key->query_hidden) {
    return LC_OK;
  }
  if (strncmp(key->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(key->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(context, key->version);
  context->candidate_key = key->key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(context, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  emit = 0;
  stop = 0;
  rc = lc_pouch_query_page_accept_match(context, &emit, &stop, error);
  if (rc != LC_OK || !emit) {
    return rc == LC_OK && stop ? LC_POUCH_STATE_READ_MANY_STOP : rc;
  }
  return lc_pouch_query_page_mark_emitted(context, error);
}

static int lc_pouch_query_index_collect_exact_document_key(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error) {
  lc_pouch_query_index_exact_document_page *page;
  lc_pouch_query_scan_context *scan;
  int active;
  int emit;
  int rc;
  int stop;

  page = (lc_pouch_query_index_exact_document_page *)context;
  if (page == NULL || page->scan == NULL || key == NULL || key->key == NULL) {
    return LC_OK;
  }
  scan = page->scan;
  if (key->has_query_hidden && key->query_hidden) {
    return LC_OK;
  }
  if (strncmp(key->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(key->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(scan, key->version);
  scan->candidate_key = key->key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(scan, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  emit = 0;
  stop = 0;
  rc = lc_pouch_query_page_accept_match(scan, &emit, &stop, error);
  if (rc != LC_OK || !emit) {
    return rc == LC_OK && stop ? LC_POUCH_STATE_READ_MANY_STOP : rc;
  }
  if (lc_sink_is_discard(scan->sink)) {
    return lc_pouch_query_page_mark_emitted(scan, error);
  }
  rc = lc_pouch_query_index_key_set_add(&page->keys, key, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_page_mark_emitted(scan, error);
  }
  return rc;
}

static int lc_pouch_query_index_emit_exact_document_read(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *read_context, lc_error *error) {
  lc_pouch_query_scan_context *context;

  (void)key;
  context = (lc_pouch_query_scan_context *)read_context;
  if (context == NULL || read_result == NULL) {
    return LC_OK;
  }
  if (!read_result->found ||
      (read_result->has_query_hidden && read_result->query_hidden)) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(context, read_result->version);
  return lc_pouch_query_emit_document(context, read_result->body, error);
}

static int lc_pouch_query_index_process_exact_document_page(
    lc_pouch_query_scan_context *context,
    lc_pouch_query_index_key_set *page_keys, lc_error *error) {
  const char **read_keys;
  size_t index;
  int rc;

  if (context == NULL || page_keys == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact indexed document page requires context "
                        "and keys",
                        NULL, NULL, NULL);
  }
  if (page_keys->count == 0U) {
    return LC_OK;
  }
  if (lc_sink_is_discard(context->sink)) {
    return LC_OK;
  }
  read_keys = (const char **)lc_calloc_with_allocator(NULL, page_keys->count,
                                                      sizeof(*read_keys));
  if (read_keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact document page keys",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < page_keys->count; ++index) {
    read_keys[index] = page_keys->keys[index].key;
  }
  rc = lc_pouch_state_read_many_cached(
      context->client->pouch, context->ns, read_keys, page_keys->count,
      lc_pouch_query_index_emit_exact_document_read, context, error);
  lc_free_with_allocator(NULL, read_keys);
  return rc;
}

static int lc_pouch_query_index_process_key_read(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *read_context, lc_error *error) {
  lc_pouch_query_scan_context *context;
  int active;
  int emit;
  int matched;
  int rc;
  int stop;

  context = (lc_pouch_query_scan_context *)read_context;
  if (context == NULL || key == NULL || read_result == NULL) {
    return LC_OK;
  }
  if (!read_result->found ||
      (read_result->has_query_hidden && read_result->query_hidden)) {
    return LC_OK;
  }
  lc_pouch_query_track_index_seq(context, read_result->version);
  context->candidate_key = key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(context, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  matched = 0;
  if (context->indexed_candidates_exact) {
    matched = 1;
    rc = LC_OK;
  } else {
    rc = read_result->body != NULL
             ? lc_pouch_query_match_body(context, read_result->body, &matched,
                                         error)
             : LC_OK;
  }
  if (rc == LC_OK && matched) {
    emit = 0;
    stop = 0;
    rc = lc_pouch_query_page_accept_match(context, &emit, &stop, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (stop) {
      return LC_POUCH_STATE_READ_MANY_STOP;
    }
    if (emit) {
      if (context->emit_documents) {
        rc = lc_pouch_query_emit_document(context, read_result->body, error);
      } else {
        rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                                     key, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_page_mark_emitted(context, error);
      }
    }
  }
  return rc;
}

static int lc_pouch_query_scan_summary_visit(
    const lc_pouch_state_scan_summary_entry *entry, void *scan_context,
    lc_error *error) {
  lc_pouch_query_scan_context *context;
  lc_pouch_state_read_result read_result;
  int active;
  int emit;
  int matched;
  int rc;
  int stop;

  context = (lc_pouch_query_scan_context *)scan_context;
  if (context == NULL || entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  if ((entry->has_query_hidden && entry->query_hidden) ||
      strncmp(entry->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(entry->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  context->candidate_key = entry->key;
  active = 0;
  rc = lc_pouch_query_page_enter_candidate(context, &active, error);
  if (rc != LC_OK || !active) {
    return rc;
  }
  memset(&read_result, 0, sizeof(read_result));
  matched = context->selector == NULL ? 1 : 0;
  if (context->selector != NULL || context->emit_documents) {
    rc = lc_pouch_state_scan_summary_read_body(
        context->client->pouch, context->ns, entry,
        context->any_text_contains_needle != NULL, &read_result, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (rc == LC_OK && context->selector != NULL) {
    rc = read_result.body != NULL
             ? lc_pouch_query_match_body(context, read_result.body, &matched,
                                         error)
             : LC_OK;
  }
  if (rc == LC_OK && matched) {
    emit = 0;
    stop = 0;
    rc = lc_pouch_query_page_accept_match(context, &emit, &stop, error);
    if (rc == LC_OK && stop) {
      rc = LC_POUCH_STATE_READ_MANY_STOP;
    }
    if (rc == LC_OK && emit) {
      if (context->emit_documents) {
        rc = lc_pouch_query_emit_document(context, read_result.body, error);
      } else {
        rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                                     entry->key, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_page_mark_emitted(context, error);
      }
    }
  }
  lc_pouch_state_read_result_cleanup(&context->client->pouch->allocator,
                                     &read_result);
  return rc;
}

static int lc_pouch_query_run_scan_predicate(lc_pouch_query_scan_context *scan,
                                             lc_error *error) {
  lc_pouch_query_index_plan plan;
  lc_pouch_state_scan_summaries_result page;
  char *owned_start_after;
  const char *start_after;
  size_t page_limit;
  int rc;

  if (scan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scanned query requires scan context", NULL, NULL,
                        NULL);
  }
  scan->indexed_candidates_exact = 0;
  memset(&plan, 0, sizeof(plan));
  rc = LC_OK;
  if (scan->selector != NULL) {
    rc = lc_pouch_query_index_plan_from_selector(scan->runtime, scan->selector,
                                                 &plan, error);
    if (rc == LC_OK && plan.contains && plan.value_count == 1U &&
        plan.field != NULL && strcmp(plan.field, "/...") == 0) {
      /* An any-text selector must consume the complete JSON document. Besides
       * avoiding LQL's general matcher for this common scan, that guarantee
       * lets scan summaries safely warm their bounded transformed-body cache.
       */
      scan->any_text_contains_needle = plan.values[0];
      scan->any_text_contains_ignore_case = plan.ignore_case;
    } else if (rc == LC_OK &&
               lc_pouch_query_scan_scalar_plan_supported(&plan)) {
      /* The scalar body matcher is a bounded streaming scan optimization, not
       * a query-index feature. In particular, non-query Pouch roots must use
       * it for outbox's durable envelope predicates: routing those simple
       * scans through the generic LQL evaluator needlessly requires its large
       * parser frame on the dispatcher's Musl thread stack. Only the optional
       * key-index candidate shortcut below depends on indexing being enabled.
       */
      scan->scan_scalar_plan = &plan;
      if (scan->client->pouch->query_indexing_enabled &&
          !scan->emit_documents) {
        lc_pouch_generation flushed_seq;

        flushed_seq = 0UL;
        scan->scan_scalar_plan = NULL;
        lc_pouch_query_index_plan_cleanup(&plan);
        return lc_pouch_query_run_index_predicate(scan, &flushed_seq, error);
      }
    } else if (rc != LC_OK) {
      if (error != NULL) {
        lc_error_cleanup(error);
        lc_error_init(error);
      }
      rc = LC_OK;
    }
  }
  owned_start_after = NULL;
  start_after = scan->start_after_key;
  page_limit = lc_pouch_query_scan_summary_page_limit(scan->limit);
  while (rc == LC_OK && !scan->page_full) {
    memset(&page, 0, sizeof(page));
    rc = lc_pouch_state_scan_summaries(
        scan->client->pouch, scan->ns, start_after, page_limit,
        lc_pouch_query_scan_summary_visit, scan, &page, error);
    if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
      rc = LC_OK;
      scan->page_full = 1;
    }
    if (rc != LC_OK || scan->page_full || !page.truncated ||
        page.next_start_after == NULL) {
      lc_pouch_state_scan_summaries_result_cleanup(
          &scan->client->pouch->allocator, &page);
      break;
    }
    lc_free_with_allocator(&scan->client->pouch->allocator, owned_start_after);
    owned_start_after = page.next_start_after;
    page.next_start_after = NULL;
    start_after = owned_start_after;
    lc_pouch_state_scan_summaries_result_cleanup(
        &scan->client->pouch->allocator, &page);
  }
  lc_free_with_allocator(&scan->client->pouch->allocator, owned_start_after);
  scan->any_text_contains_needle = NULL;
  scan->any_text_contains_ignore_case = 0;
  scan->scan_scalar_plan = NULL;
  lc_pouch_query_index_plan_cleanup(&plan);
  return rc;
}

static int lc_pouch_query_flush_summary_index(lc_client_handle *client,
                                              const char *ns,
                                              int validate_current,
                                              lc_pouch_generation *index_seq,
                                              lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  int rc;

  if (index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index flush requires index_seq output",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  rc = lc_pouch_state_query_index_seq(client->pouch, ns, index_seq, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&flush_result, 0, sizeof(flush_result));
  rc = lc_pouch_query_index_ensure_current(
      client->pouch, ns, *index_seq, validate_current, &flush_result, error);
  if (rc == LC_OK) {
    *index_seq = flush_result.index_seq;
  }
  return rc;
}

static int
lc_pouch_query_index_process_keys(lc_pouch_query_scan_context *context,
                                  lc_pouch_query_index_key_set *keys,
                                  lc_error *error) {
  const char **read_keys;
  size_t index;
  size_t write_index;
  int has_candidate_exact;
  int all_candidate_exact;
  int rc;

  if (context == NULL || keys == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch indexed query requires context and key set",
                        NULL, NULL, NULL);
  }
  read_keys = NULL;
  rc = LC_OK;
  if (keys->count > 1U) {
    qsort(keys->keys, keys->count, sizeof(keys->keys[0]),
          lc_pouch_query_index_key_compare);
  }
  write_index = 0U;
  for (index = 0U; index < keys->count; ++index) {
    if (strncmp(keys->keys[index].key, ".staging/", sizeof(".staging/") - 1U) ==
            0 ||
        strstr(keys->keys[index].key, "/.staging/") != NULL ||
        (keys->keys[index].has_query_hidden &&
         keys->keys[index].query_hidden)) {
      lc_free_with_allocator(NULL, (char *)keys->keys[index].key);
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
      continue;
    }
    if (write_index > 0U &&
        strcmp(keys->keys[write_index - 1U].key, keys->keys[index].key) == 0) {
      lc_free_with_allocator(NULL, (char *)keys->keys[index].key);
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
      continue;
    }
    if (write_index != index) {
      keys->keys[write_index] = keys->keys[index];
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
    }
    ++write_index;
  }
  keys->count = write_index;
  if (keys->count == 0U) {
    return LC_OK;
  }
  has_candidate_exact = 0;
  all_candidate_exact = 1;
  for (index = 0U; index < keys->count; ++index) {
    if (keys->keys[index].candidate_exact) {
      has_candidate_exact = 1;
    } else {
      all_candidate_exact = 0;
    }
  }
  if (context->emit_documents && lc_sink_is_discard(context->sink) &&
      (context->indexed_candidates_exact || all_candidate_exact) &&
      context->start_after_key == NULL && keys->count > 0U &&
      context->emitted <= context->limit &&
      keys->count < context->limit - context->emitted) {
    for (index = 0U; index < keys->count; ++index) {
      lc_pouch_query_track_index_seq(context, keys->keys[index].version);
    }
    context->seen += keys->count;
    context->matched += keys->count;
    context->emitted += keys->count;
    return LC_OK;
  }
  if (context->emit_documents && context->indexed_candidates_exact &&
      lc_sink_is_discard(context->sink)) {
    for (index = 0U; index < keys->count; ++index) {
      rc = lc_pouch_query_index_process_exact_discard_document_key(
          context, &keys->keys[index], error);
      if (rc != LC_OK || context->page_full) {
        return rc == LC_POUCH_STATE_READ_MANY_STOP ? LC_OK : rc;
      }
    }
    return LC_OK;
  }
  if (!context->emit_documents && context->indexed_candidates_exact) {
    for (index = 0U; index < keys->count; ++index) {
      rc = lc_pouch_query_index_process_exact_key(context, &keys->keys[index],
                                                  error);
      if (rc != LC_OK || context->page_full) {
        return rc == LC_POUCH_STATE_READ_MANY_STOP ? LC_OK : rc;
      }
    }
    return LC_OK;
  }
  if (!context->indexed_candidates_exact && has_candidate_exact) {
    int saved_indexed_candidates_exact;

    saved_indexed_candidates_exact = context->indexed_candidates_exact;
    for (index = 0U; index < keys->count; ++index) {
      const char *one_key[1];

      if (keys->keys[index].candidate_exact) {
        context->indexed_candidates_exact = 1;
        if (!context->emit_documents) {
          rc = lc_pouch_query_index_process_exact_key(
              context, &keys->keys[index], error);
        } else if (lc_sink_is_discard(context->sink)) {
          rc = lc_pouch_query_index_process_exact_discard_document_key(
              context, &keys->keys[index], error);
        } else {
          one_key[0] = keys->keys[index].key;
          rc = lc_pouch_state_read_many_cached(
              context->client->pouch, context->ns, one_key, 1U,
              lc_pouch_query_index_process_key_read, context, error);
        }
      } else {
        context->indexed_candidates_exact = 0;
        one_key[0] = keys->keys[index].key;
        rc = lc_pouch_state_read_many_cached(
            context->client->pouch, context->ns, one_key, 1U,
            lc_pouch_query_index_process_key_read, context, error);
      }
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
      }
      if (rc != LC_OK || context->page_full) {
        context->indexed_candidates_exact = saved_indexed_candidates_exact;
        return rc;
      }
    }
    context->indexed_candidates_exact = saved_indexed_candidates_exact;
    return LC_OK;
  }
  read_keys = (const char **)lc_calloc_with_allocator(NULL, keys->count,
                                                      sizeof(*read_keys));
  if (read_keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query index read keys", NULL,
                        NULL, NULL);
  }
  for (index = 0U; index < keys->count; ++index) {
    read_keys[index] = keys->keys[index].key;
  }
  rc = lc_pouch_state_read_many_cached(
      context->client->pouch, context->ns, read_keys, keys->count,
      lc_pouch_query_index_process_key_read, context, error);
  lc_free_with_allocator(NULL, read_keys);
  return rc;
}

static int
lc_pouch_query_run_index_predicate(lc_pouch_query_scan_context *scan,
                                   lc_pouch_generation *flushed_seq_out,
                                   lc_error *error) {
  lc_pouch_query_index_plan plan;
  lc_pouch_query_index_key_set keys;
  lc_pouch_query_index_exact_document_page exact_document_page;
  lc_pouch_generation flushed_seq;
  lc_pouch_generation value_seq;
  size_t value_index;
  int indexed_candidates_exact;
  int processed_exact_documents;
  int planned;
  int retried_repair;
  int validate_current;
  int rc;

  if (scan == NULL || flushed_seq_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch indexed query requires scan context and flush "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(&plan, 0, sizeof(plan));
  memset(&keys, 0, sizeof(keys));
  memset(&exact_document_page, 0, sizeof(exact_document_page));
  exact_document_page.scan = scan;
  flushed_seq = 0UL;
  processed_exact_documents = 0;
  planned = 0;
  retried_repair = 0;
  validate_current = scan->request != NULL && scan->request->refresh != NULL &&
                     strcmp(scan->request->refresh, "wait_for") == 0;
  *flushed_seq_out = 0UL;
  indexed_candidates_exact = 0;
  rc = lc_pouch_query_index_plan_from_selector(scan->runtime, scan->selector,
                                               &plan, error);
  if (rc == LC_OK) {
    planned = 1;
    plan.candidates_exact =
        lc_pouch_query_index_plan_candidate_results_are_exact(&plan);
  }

run_index_query:
  if (rc == LC_OK) {
    flushed_seq = 0UL;
    indexed_candidates_exact = plan.candidates_exact;
    processed_exact_documents = 0;
    exact_document_page.scan = scan;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_flush_summary_index(
        scan->client, scan->ns, validate_current, &flushed_seq, error);
  }
  if (rc == LC_OK && plan.root_or) {
    value_seq = 0UL;
    if (plan.candidates_exact && scan->emit_documents) {
      rc = lc_pouch_query_index_visit_scalar_terms_docids(
          scan->client->pouch, scan->ns, plan.or_terms, plan.or_term_count,
          lc_pouch_query_index_collect_exact_document_key, &exact_document_page,
          &value_seq, error);
      processed_exact_documents = 1;
    } else if (!scan->emit_documents && plan.candidates_exact) {
      rc = lc_pouch_query_index_visit_scalar_terms_docids(
          scan->client->pouch, scan->ns, plan.or_terms, plan.or_term_count,
          lc_pouch_query_index_process_exact_key_visit, scan, &value_seq,
          error);
    } else {
      rc = lc_pouch_query_index_visit_scalar_terms(
          scan->client->pouch, scan->ns, plan.or_terms, plan.or_term_count,
          lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    }
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else if (rc == LC_OK && plan.exists) {
    value_seq = 0UL;
    if (plan.candidates_exact && scan->emit_documents) {
      rc = lc_pouch_query_index_visit_exists(
          scan->client->pouch, scan->ns, plan.field,
          lc_pouch_query_index_collect_exact_document_key, &exact_document_page,
          &value_seq, error);
      processed_exact_documents = 1;
    } else if (!scan->emit_documents && plan.candidates_exact) {
      rc = lc_pouch_query_index_visit_exists(
          scan->client->pouch, scan->ns, plan.field,
          lc_pouch_query_index_process_exact_key_visit, scan, &value_seq,
          error);
    } else {
      rc = lc_pouch_query_index_visit_exists(
          scan->client->pouch, scan->ns, plan.field,
          lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    }
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && plan.date) {
    value_seq = 0UL;
    if (plan.candidates_exact && scan->emit_documents) {
      rc = lc_pouch_query_index_visit_date(
          scan->client->pouch, scan->ns, plan.field, &plan.date_bounds,
          lc_pouch_query_index_collect_exact_document_key, &exact_document_page,
          &value_seq, error);
      processed_exact_documents = 1;
    } else if (!scan->emit_documents && plan.candidates_exact) {
      rc = lc_pouch_query_index_visit_date(
          scan->client->pouch, scan->ns, plan.field, &plan.date_bounds,
          lc_pouch_query_index_process_exact_key_visit, scan, &value_seq,
          error);
    } else {
      rc = lc_pouch_query_index_visit_date(
          scan->client->pouch, scan->ns, plan.field, &plan.date_bounds,
          lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    }
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && plan.range) {
    value_seq = 0UL;
    if (plan.candidates_exact && scan->emit_documents) {
      rc = lc_pouch_query_index_visit_range(
          scan->client->pouch, scan->ns, plan.field, &plan.range_bounds,
          lc_pouch_query_index_collect_exact_document_key, &exact_document_page,
          &value_seq, error);
      processed_exact_documents = 1;
    } else if (!scan->emit_documents && plan.candidates_exact) {
      rc = lc_pouch_query_index_visit_range(
          scan->client->pouch, scan->ns, plan.field, &plan.range_bounds,
          lc_pouch_query_index_process_exact_key_visit, scan, &value_seq,
          error);
    } else {
      rc = lc_pouch_query_index_visit_range(
          scan->client->pouch, scan->ns, plan.field, &plan.range_bounds,
          lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    }
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && plan.candidates_exact && scan->emit_documents &&
      plan.value_count > 0U && !plan.prefix && !plan.contains && !plan.range &&
      !plan.date && !plan.root_or) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_scalar_any_docids(
        scan->client->pouch, scan->ns, plan.field,
        (const char *const *)plan.values, plan.value_types, plan.value_count,
        lc_pouch_query_index_collect_exact_document_key, &exact_document_page,
        &value_seq, error);
    processed_exact_documents = 1;
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else if (rc == LC_OK && !scan->emit_documents && plan.candidates_exact &&
             plan.value_count > 0U && !plan.prefix && !plan.contains &&
             !plan.range && !plan.date) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_scalar_any_docids(
        scan->client->pouch, scan->ns, plan.field,
        (const char *const *)plan.values, plan.value_types, plan.value_count,
        lc_pouch_query_index_process_exact_key_visit, scan, &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else if (rc == LC_OK && plan.value_count > 1U && !plan.prefix &&
             !plan.contains && !plan.range && !plan.date) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_scalar_any(
        scan->client->pouch, scan->ns, plan.field,
        (const char *const *)plan.values, plan.value_types, plan.value_count,
        lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else {
    for (value_index = 0U; rc == LC_OK && value_index < plan.value_count;
         ++value_index) {
      value_seq = 0UL;
      if (plan.prefix) {
        if (!lc_pouch_query_index_prefix_candidates_exact(
                plan.values[value_index])) {
          rc = lc_pouch_query_index_visit_prefix_candidates(
              scan->client->pouch, scan->ns, plan.field,
              plan.values[value_index], plan.ignore_case,
              lc_pouch_query_index_key_collect, &keys, &value_seq, error);
          indexed_candidates_exact = 0;
        } else if (plan.candidates_exact && scan->emit_documents) {
          rc = lc_pouch_query_index_visit_prefix(
              scan->client->pouch, scan->ns, plan.field,
              plan.values[value_index], plan.ignore_case,
              lc_pouch_query_index_collect_exact_document_key,
              &exact_document_page, &value_seq, error);
          processed_exact_documents = 1;
        } else {
          rc = lc_pouch_query_index_visit_prefix(
              scan->client->pouch, scan->ns, plan.field,
              plan.values[value_index], plan.ignore_case,
              !scan->emit_documents && plan.candidates_exact
                  ? lc_pouch_query_index_process_exact_key_visit
                  : lc_pouch_query_index_key_collect,
              !scan->emit_documents && plan.candidates_exact ? (void *)scan
                                                             : (void *)&keys,
              &value_seq, error);
        }
      } else if (plan.contains) {
        lc_pouch_query_index_key_collect_context collect_context;
        lc_pouch_generation candidate_seq;
        lc_pouch_generation exact_seq;
        size_t needle_len;
        int text_complete;

        memset(&collect_context, 0, sizeof(collect_context));
        collect_context.keys = &keys;
        collect_context.candidate_exact = 1;
        needle_len = strlen(plan.values[value_index]);
        exact_seq = 0UL;
        rc = lc_pouch_query_index_visit_contains_complete_at_snapshot(
            scan->client->pouch, scan->ns, plan.field, plan.values[value_index],
            plan.ignore_case, lc_pouch_query_index_key_collect_marked,
            &collect_context, &flushed_seq, &exact_seq, &text_complete, error);
        if (rc == LC_OK && exact_seq > value_seq) {
          value_seq = exact_seq;
        }
        candidate_seq = 0UL;
        collect_context.candidate_exact = 0;
        if (rc == LC_OK && !text_complete && needle_len < 3U) {
          if (strcmp(plan.field, "/...") == 0) {
            rc = lc_pouch_query_index_visit(scan->client->pouch, scan->ns,
                                            lc_pouch_query_index_row_collect,
                                            &keys, &candidate_seq, error);
          } else {
            rc = lc_pouch_query_index_visit_exists(
                scan->client->pouch, scan->ns, plan.field,
                lc_pouch_query_index_key_collect_marked, &collect_context,
                &candidate_seq, error);
          }
          indexed_candidates_exact = 0;
        } else if (rc == LC_OK && !text_complete && needle_len >= 3U) {
          rc = lc_pouch_query_index_visit_contains_candidates(
              scan->client->pouch, scan->ns, plan.field,
              plan.values[value_index], plan.ignore_case,
              lc_pouch_query_index_key_collect_marked, &collect_context,
              &candidate_seq, error);
          indexed_candidates_exact = 0;
        }
        if (rc == LC_OK && candidate_seq > value_seq) {
          value_seq = candidate_seq;
        }
      } else {
        rc = lc_pouch_query_index_visit_scalar(
            scan->client->pouch, scan->ns, plan.field, plan.values[value_index],
            plan.value_types[value_index],
            !scan->emit_documents && plan.candidates_exact
                ? lc_pouch_query_index_process_exact_key_visit
                : lc_pouch_query_index_key_collect,
            !scan->emit_documents && plan.candidates_exact ? (void *)scan
                                                           : (void *)&keys,
            &value_seq, error);
      }
      if (rc == LC_OK && value_seq > scan->index_seq) {
        scan->index_seq = value_seq;
      }
    }
  }
  if (rc == LC_ERR_INVALID && planned && !validate_current && !retried_repair) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    lc_pouch_query_index_key_set_cleanup(&exact_document_page.keys);
    lc_pouch_query_index_key_set_cleanup(&keys);
    lc_pouch_query_scan_reset_page_state(scan);
    retried_repair = 1;
    validate_current = 1;
    rc = LC_OK;
    goto run_index_query;
  }
  if (rc == LC_OK && scan->index_seq < flushed_seq) {
    scan->index_seq = flushed_seq;
  }
  if (rc == LC_OK) {
    scan->indexed_candidates_exact = indexed_candidates_exact;
    scan->any_text_contains_needle = NULL;
    scan->any_text_contains_ignore_case = 0;
    scan->scan_scalar_plan = NULL;
    if (!indexed_candidates_exact && plan.contains && plan.value_count == 1U &&
        plan.field != NULL && strcmp(plan.field, "/...") == 0) {
      scan->any_text_contains_needle = plan.values[0];
      scan->any_text_contains_ignore_case = plan.ignore_case;
    } else if (!indexed_candidates_exact &&
               lc_pouch_query_scan_scalar_plan_supported(&plan)) {
      scan->scan_scalar_plan = &plan;
    }
    if (processed_exact_documents) {
      rc = lc_pouch_query_index_process_exact_document_page(
          scan, &exact_document_page.keys, error);
    } else {
      rc = lc_pouch_query_index_process_keys(scan, &keys, error);
    }
    scan->any_text_contains_needle = NULL;
    scan->any_text_contains_ignore_case = 0;
    scan->scan_scalar_plan = NULL;
  }
  if (rc == LC_OK) {
    *flushed_seq_out = flushed_seq;
  }
  lc_pouch_query_index_key_set_cleanup(&exact_document_page.keys);
  lc_pouch_query_index_key_set_cleanup(&keys);
  lc_pouch_query_index_plan_cleanup(&plan);
  return rc;
}

static int lc_pouch_generation_to_version(lc_pouch_generation generation,
                                          lc_version *out, lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch version conversion requires output storage",
                        NULL, NULL, NULL);
  }
  if (generation > (uint64_t)LC_I64_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation exceeds lockd version range", NULL,
                        NULL, NULL);
  }
  *out = (lc_version)generation;
  return LC_OK;
}

static int lc_pouch_version_to_generation(lc_version version,
                                          lc_pouch_generation *out,
                                          lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation conversion requires output storage",
                        NULL, NULL, NULL);
  }
  if (version < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch version must be non-negative", NULL, NULL, NULL);
  }
  *out = (lc_pouch_generation)version;
  return LC_OK;
}

static int lc_pouch_lease_refresh_state(lc_lease_handle *lease,
                                        const char *etag, lc_version version,
                                        lc_error *error) {
  char *etag_copy;

  etag_copy = lc_client_strdup(lease->client, etag);
  if (etag != NULL && etag_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease state etag", NULL, NULL,
                        NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = etag_copy;
  lease->version = version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  if (etag == NULL && version == 0)
    lc_lease_retention_observe(&lease->pub, LC_STATE_RETENTION_SUPPORTED, 0, 0);
  return LC_OK;
}

static void lc_pouch_lease_refresh_query_metadata(lc_lease_handle *lease,
                                                  int has_query_hidden,
                                                  int query_hidden) {
  if (lease == NULL) {
    return;
  }
  lease->has_query_hidden = has_query_hidden;
  lease->query_hidden = query_hidden;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
}

int lc_pouch_now_unix(lc_pouch_unix_seconds *out, lc_error *error) {
  struct timespec now;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch time read requires output storage", NULL, NULL,
                        NULL);
  }
  /* Use the enqueue clock for visibility, expiry, and lease validation.
   * time() may lag CLOCK_REALTIME across a Unix-second boundary. */
  if (lc_pouch_queue_clock_gettime(CLOCK_REALTIME, &now) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch wall clock", strerror(errno),
                        NULL, NULL);
  }
  if (now.tv_sec > 0 && (uintmax_t)now.tv_sec > (uintmax_t)LC_I64_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch wall clock exceeds supported timestamp range",
                        NULL, NULL, NULL);
  }
  *out = (lc_pouch_unix_seconds)now.tv_sec;
  return LC_OK;
}

static int lc_pouch_expiration_from_ttl(long ttl_seconds,
                                        lc_pouch_unix_seconds *out,
                                        lc_error *error) {
  lc_pouch_unix_seconds now;
  int rc;

  if (ttl_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds must be positive", NULL, NULL, NULL);
  }
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc != LC_OK) {
    return rc;
  }
  if ((uintmax_t)ttl_seconds > (uintmax_t)LC_I64_MAX ||
      now > LC_I64_MAX - (lc_pouch_unix_seconds)ttl_seconds) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds exceeds supported range", NULL, NULL,
                        NULL);
  }
  *out = now + ttl_seconds;
  return LC_OK;
}

static int lc_pouch_timestamp_add(lc_pouch_unix_seconds base, long delta,
                                  const char *field, lc_pouch_unix_seconds *out,
                                  lc_error *error) {
  char message[128];

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch timestamp addition requires output", NULL, NULL,
                        NULL);
  }
  if (delta < 0L) {
    snprintf(message, sizeof(message), "pouch %s must be non-negative",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if ((uintmax_t)delta > (uintmax_t)LC_I64_MAX ||
      base > LC_I64_MAX - (lc_pouch_unix_seconds)delta) {
    snprintf(message, sizeof(message),
             "pouch %s exceeds supported timestamp range",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = base + delta;
  return LC_OK;
}

static void
lc_pouch_lease_refresh_expiration(lc_lease_handle *lease,
                                  lc_pouch_unix_seconds lease_expires_at_unix) {
  if (lease == NULL) {
    return;
  }
  lease->lease_expires_at_unix = lease_expires_at_unix;
  lease->pub.lease_expires_at_unix = lease_expires_at_unix;
}

static int lc_pouch_retention_options(lc_pouch_state_write_options *options,
                                      int selected, long seconds,
                                      const char *ns, const char *key,
                                      lc_error *error) {
  int rc = lc_state_retention_validate(selected, seconds, ns, key, error);
  if (rc == LC_OK && selected) {
    options->has_retention = 1;
    options->retention = -(lc_pouch_unix_seconds)seconds;
  }
  return rc;
}

static void lc_pouch_lease_refresh_retention(lc_lease_handle *lease,
                                             int has_body,
                                             lc_pouch_unix_seconds policy) {
  lease->pub.retention_support = LC_STATE_RETENTION_SUPPORTED;
  lease->pub.has_retention_expires_at_unix = has_body && policy >= 0;
  lease->pub.retention_expires_at_unix =
      lease->pub.has_retention_expires_at_unix ? policy : 0;
}

static int lc_pouch_client_copy_state_metadata(
    const lc_pouch_state_read_result *read_result, lc_get_res *out,
    lc_error *error) {
  char *content_type;
  char *etag;

  content_type = lc_strdup_local(read_result->content_type);
  etag = lc_strdup_local(read_result->etag);
  if ((read_result->content_type != NULL && content_type == NULL) ||
      (read_result->etag != NULL && etag == NULL)) {
    lc_free_local(content_type);
    lc_free_local(etag);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch get metadata", NULL, NULL,
                        NULL);
  }
  out->content_type = content_type;
  out->etag = etag;
  if (lc_pouch_generation_to_version(read_result->version, &out->version,
                                     error) != LC_OK) {
    lc_free_local(content_type);
    lc_free_local(etag);
    return error != NULL ? error->code : LC_ERR_INVALID;
  }
  out->no_content = !read_result->found;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix = read_result->has_body &&
                                       !read_result->staged_delete_marker &&
                                       read_result->retention >= 0;
  out->retention_expires_at_unix =
      out->has_retention_expires_at_unix ? read_result->retention : 0;
  return LC_OK;
}

static int lc_pouch_client_copy_update_metadata(
    const lc_pouch_state_write_result *write_result, lc_update_res *out,
    lc_error *error) {
  char *etag;
  long public_bytes = 0L;

  if (lc_pouch_size_to_public_long(write_result->bytes, &public_bytes, error) !=
      LC_OK) {
    return error != NULL ? error->code : LC_ERR_INVALID;
  }

  etag = lc_strdup_local(write_result->etag);
  if (write_result->etag != NULL && etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch update metadata", NULL, NULL,
                        NULL);
  }
  if (lc_pouch_generation_to_version(write_result->version, &out->new_version,
                                     error) != LC_OK) {
    lc_free_local(etag);
    return error != NULL ? error->code : LC_ERR_INVALID;
  }
  out->new_state_etag = etag;
  out->bytes = public_bytes;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix =
      write_result->has_body && write_result->retention >= 0;
  out->retention_expires_at_unix =
      out->has_retention_expires_at_unix ? write_result->retention : 0;
  return LC_OK;
}

static int lc_pouch_client_copy_mutate_metadata(
    const lc_pouch_state_write_result *write_result, lc_mutate_res *out,
    lc_error *error) {
  char *etag;
  long public_bytes = 0L;

  if (lc_pouch_size_to_public_long(write_result->bytes, &public_bytes, error) !=
      LC_OK) {
    return error != NULL ? error->code : LC_ERR_INVALID;
  }

  etag = lc_strdup_local(write_result->etag);
  if (write_result->etag != NULL && etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutate metadata", NULL, NULL,
                        NULL);
  }
  if (lc_pouch_generation_to_version(write_result->version, &out->new_version,
                                     error) != LC_OK) {
    lc_free_local(etag);
    return error != NULL ? error->code : LC_ERR_INVALID;
  }
  out->new_state_etag = etag;
  out->bytes = public_bytes;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix =
      write_result->has_body && write_result->retention >= 0;
  out->retention_expires_at_unix =
      out->has_retention_expires_at_unix ? write_result->retention : 0;
  return LC_OK;
}

static void lc_pouch_mutate_file_cleanup(lc_pouch_mutate_file *file) {
  if (file == NULL) {
    return;
  }
  if (file->fp != NULL) {
    fclose(file->fp);
  }
  lc_free_local(file->etag);
  memset(file, 0, sizeof(*file));
}

static int lc_pouch_copy_source_to_file(lc_source *source, FILE *fp,
                                        lc_error *error) {
  unsigned char *buffer;
  int rc = LC_OK;

  if (source == NULL || fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate copy requires source and file", NULL,
                        NULL, NULL);
  }
  buffer = lc_alloc_with_allocator(NULL, 8192U);
  if (buffer == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutate copy buffer", NULL,
                        NULL, "pouch");
  for (;;) {
    size_t nread;

    nread = source->read(source, buffer, 8192U, error);
    if (nread == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    if (fwrite(buffer, 1U, nread, fp) != nread) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch mutate scratch file",
                        strerror(errno), NULL, NULL);
      break;
    }
  }
  lc_free_with_allocator(NULL, buffer);
  if (rc != LC_OK)
    return rc;
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate scratch file",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

static int lc_pouch_mutate_seed_empty(FILE *fp, lc_error *error) {
  if (fwrite("{}", 1U, 2U, fp) != 2U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seed pouch mutate state", strerror(errno),
                        NULL, NULL);
  }
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate seed state",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

static int lc_pouch_state_result_is_delete_marker(
    const lc_pouch_state_read_result *read_result) {
  return read_result != NULL && read_result->found &&
         read_result->staged_delete_marker;
}

static int lc_pouch_prepare_mutation_file_from_plan(
    lc_client_handle *client, const lc_pouch_state_read_result *read_result,
    lc_mutation_plan *plan, lc_pouch_mutate_file *out, lc_error *error) {
  FILE *input_fp;
  FILE *final_fp;
  char *etag;
  int logical_found;
  int rc;

  if (client == NULL || read_result == NULL || plan == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate preparation requires client, state, "
                        "plan, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  input_fp = NULL;
  final_fp = NULL;
  etag = NULL;
  logical_found = read_result->found && read_result->has_body &&
                  !lc_pouch_state_result_is_delete_marker(read_result);
  input_fp = tmpfile();
  if (input_fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch mutate input scratch file",
                      strerror(errno), NULL, NULL);
    goto cleanup;
  }
  if (logical_found) {
    rc = lc_pouch_copy_source_to_file(read_result->body, input_fp, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    etag = lc_strdup_local(read_result->etag);
    if (read_result->etag != NULL && etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutate fetched etag", NULL,
                        NULL, NULL);
      goto cleanup;
    }
  } else {
    rc = lc_pouch_mutate_seed_empty(input_fp, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  if (rc != LC_OK) {
    goto cleanup;
  }

  out->fp = final_fp;
  out->found = logical_found;
  out->etag = etag;
  out->version = logical_found ? read_result->version : 0UL;
  final_fp = NULL;
  etag = NULL;

cleanup:
  lc_free_local(etag);
  if (final_fp != NULL) {
    fclose(final_fp);
  }
  if (input_fp != NULL) {
    fclose(input_fp);
  }
  return rc;
}

static int
lc_pouch_prepare_mutation_file(lc_client_handle *client, const char *ns,
                               const char *key, const char *const *mutations,
                               size_t mutation_count,
                               const lc_mutation_parse_options *parse_options,
                               lc_pouch_mutate_file *out, lc_error *error) {
  lc_mutation_plan *plan;
  lc_pouch_state_read_result read_result;
  int rc;

  if (client == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires client, namespace, key, and "
                        "out",
                        NULL, NULL, NULL);
  }
  plan = NULL;
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_mutation_plan_build(mutations, mutation_count, parse_options, &plan,
                              error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_read(client->pouch, ns, key, &read_result, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_prepare_mutation_file_from_plan(client, &read_result, plan,
                                                  out, error);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (plan != NULL) {
    lc_mutation_plan_close(plan);
  }
  return rc;
}

static char *lc_pouch_attachment_hex_encode(const char *value) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *out;
  size_t length;
  size_t offset;

  if (value == NULL) {
    value = "";
  }
  length = strlen(value);
  out = (char *)lc_alloc_with_allocator(NULL, length * 2U + 1U);
  if (out == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  offset = 0U;
  while (*src != '\0') {
    out[offset++] = hex[*src >> 4];
    out[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  out[offset] = '\0';
  return out;
}

static int lc_pouch_attachment_hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

static char *lc_pouch_attachment_hex_decode(const char *value) {
  char *out;
  size_t length;
  size_t i;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  if (length == 0U || length % 2U != 0U) {
    return NULL;
  }
  out = (char *)lc_alloc_with_allocator(NULL, length / 2U + 1U);
  if (out == NULL) {
    return NULL;
  }
  for (i = 0U; i < length; i += 2U) {
    int hi;
    int lo;

    hi = lc_pouch_attachment_hex_value(value[i]);
    lo = lc_pouch_attachment_hex_value(value[i + 1U]);
    if (hi < 0 || lo < 0) {
      lc_free_with_allocator(NULL, out);
      return NULL;
    }
    out[i / 2U] = (char)((hi << 4) | lo);
  }
  out[length / 2U] = '\0';
  return out;
}

static char *lc_pouch_attachment_prefix(const char *ns, const char *key,
                                        lc_error *error) {
  char *prefix;
  size_t length;

  (void)ns;
  if (key == NULL || key[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch attachment prefix requires key", NULL, NULL,
                       NULL);
    return NULL;
  }
  length = strlen("state//attachments/") + strlen(key) + 1U;
  prefix = (char *)lc_alloc_with_allocator(NULL, length);
  if (prefix == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment key prefix", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(prefix, length, "state/%s/attachments/", key);
  return prefix;
}

static char *lc_pouch_staged_attachment_prefix(const char *ns, const char *key,
                                               const char *txn_id,
                                               lc_error *error) {
  char *prefix;
  size_t length;

  (void)ns;
  if (key == NULL || key[0] == '\0' || !lc_pouch_txn_id_present(txn_id)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch staged attachment prefix requires key and txn_id",
                       NULL, NULL, NULL);
    return NULL;
  }
  length = strlen("state//.staging//attachments/") + strlen(key) +
           strlen(txn_id) + 1U;
  prefix = (char *)lc_alloc_with_allocator(NULL, length);
  if (prefix == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch staged attachment prefix",
                       NULL, NULL, NULL);
    return NULL;
  }
  snprintf(prefix, length, "state/%s/.staging/%s/attachments/", key, txn_id);
  return prefix;
}

static char *lc_pouch_attachment_key(const char *ns, const char *key,
                                     const char *name, lc_error *error) {
  char *prefix;
  char *name_hex;
  char *attachment_key;
  size_t length;

  prefix = lc_pouch_attachment_prefix(ns, key, error);
  name_hex = lc_pouch_attachment_hex_encode(name);
  if (prefix == NULL || name_hex == NULL) {
    lc_free_with_allocator(NULL, prefix);
    lc_free_with_allocator(NULL, name_hex);
    if (name_hex == NULL) {
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch attachment name", NULL, NULL,
                         NULL);
    }
    return NULL;
  }
  length = strlen(prefix) + strlen(name_hex) + 1U;
  attachment_key = (char *)lc_alloc_with_allocator(NULL, length);
  if (attachment_key == NULL) {
    lc_free_with_allocator(NULL, prefix);
    lc_free_with_allocator(NULL, name_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment key", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(attachment_key, length, "%s%s", prefix, name_hex);
  lc_free_with_allocator(NULL, prefix);
  lc_free_with_allocator(NULL, name_hex);
  return attachment_key;
}

static char *lc_pouch_staged_attachment_key(const char *ns, const char *key,
                                            const char *name,
                                            const char *txn_id,
                                            lc_error *error) {
  char *prefix;
  char *name_hex;
  char *attachment_key;
  size_t length;

  prefix = lc_pouch_staged_attachment_prefix(ns, key, txn_id, error);
  name_hex = lc_pouch_attachment_hex_encode(name);
  if (prefix == NULL || name_hex == NULL) {
    lc_free_with_allocator(NULL, prefix);
    lc_free_with_allocator(NULL, name_hex);
    if (name_hex == NULL) {
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch staged attachment name",
                         NULL, NULL, NULL);
    }
    return NULL;
  }
  length = strlen(prefix) + strlen(name_hex) + 1U;
  attachment_key = (char *)lc_alloc_with_allocator(NULL, length);
  if (attachment_key == NULL) {
    lc_free_with_allocator(NULL, prefix);
    lc_free_with_allocator(NULL, name_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch staged attachment key", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(attachment_key, length, "%s%s", prefix, name_hex);
  lc_free_with_allocator(NULL, prefix);
  lc_free_with_allocator(NULL, name_hex);
  return attachment_key;
}

static char *lc_pouch_staged_attachment_key_from_committed(
    const char *attachment_key, const char *txn_id, lc_error *error) {
  const char *marker;
  const char *attachment_id;
  size_t state_prefix_len;
  size_t length;
  char *staged_key;

  if (attachment_key == NULL || !lc_pouch_txn_id_present(txn_id)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch staged attachment conversion requires key and "
                       "txn_id",
                       NULL, NULL, NULL);
    return NULL;
  }
  marker = strstr(attachment_key, "/attachments/");
  if (marker == NULL || marker == attachment_key ||
      marker[strlen("/attachments/")] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch committed attachment key is invalid", NULL, NULL,
                       NULL);
    return NULL;
  }
  state_prefix_len = (size_t)(marker - attachment_key);
  attachment_id = marker + strlen("/attachments/");
  length = state_prefix_len + strlen("/.staging//attachments/") +
           strlen(txn_id) + strlen(attachment_id) + 1U;
  staged_key = (char *)lc_alloc_with_allocator(NULL, length);
  if (staged_key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch staged attachment key", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(staged_key, length, "%.*s/.staging/%s/attachments/%s",
           (int)state_prefix_len, attachment_key, txn_id, attachment_id);
  return staged_key;
}

static int lc_pouch_txn_id_present(const char *txn_id) {
  return txn_id != NULL && txn_id[0] != '\0';
}

static void lc_pouch_txn_trim_bounds(const char *value, const char **start,
                                     size_t *length) {
  const char *first;
  const char *last;

  first = value != NULL ? value : "";
  while (*first != '\0' && isspace((unsigned char)*first)) {
    ++first;
  }
  last = first + strlen(first);
  while (last > first && isspace((unsigned char)last[-1])) {
    --last;
  }
  if (start != NULL) {
    *start = first;
  }
  if (length != NULL) {
    *length = (size_t)(last - first);
  }
}

static char *lc_pouch_staged_storage_key(const char *key, const char *txn_id,
                                         lc_error *error) {
  char *staged_key;
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;

  if (key == NULL || key[0] == '\0' || !lc_pouch_txn_id_present(txn_id)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch staged storage key requires key and txn_id", NULL,
                       NULL, NULL);
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  suffix_len = strlen("/.staging/");
  staged_key = (char *)lc_alloc_with_allocator(NULL, key_len + suffix_len +
                                                         txn_len + 1U);
  if (staged_key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch staged storage key", NULL,
                       NULL, NULL);
    return NULL;
  }
  memcpy(staged_key, key, key_len);
  memcpy(staged_key + key_len, "/.staging/", suffix_len);
  memcpy(staged_key + key_len + suffix_len, txn_id, txn_len);
  staged_key[key_len + suffix_len + txn_len] = '\0';
  return staged_key;
}

static int lc_pouch_client_prepare_txn_mutation_file(
    lc_client_handle *client, const char *ns, const char *key,
    const char *txn_id, const char *const *mutations, size_t mutation_count,
    const lc_mutation_parse_options *parse_options, lc_pouch_mutate_file *out,
    lc_error *error) {
  lc_pouch_state_read_result staged;
  char *staged_key;
  const char *mutation_key;
  int rc;

  if (client == NULL || ns == NULL || key == NULL ||
      !lc_pouch_txn_id_present(txn_id) || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction mutation requires client, "
                        "namespace, key, txn_id, and out",
                        NULL, NULL, NULL);
  }
  memset(&staged, 0, sizeof(staged));
  staged_key = lc_pouch_staged_storage_key(key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read_metadata(client->pouch, ns, staged_key, &staged,
                                    error);
  if (rc == LC_OK) {
    /* A staged delete is the transaction's absent-value projection. Mutating
     * it must seed an empty JSON value from the committed key, not parse the
     * zero-byte delete marker as a document body. */
    mutation_key =
        staged.found && !lc_pouch_state_result_is_delete_marker(&staged)
            ? staged_key
            : key;
    rc = lc_pouch_prepare_mutation_file(client, ns, mutation_key, mutations,
                                        mutation_count, parse_options, out,
                                        error);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  lc_free_with_allocator(NULL, staged_key);
  return rc;
}

static int lc_pouch_storage_key_has_staging_suffix(const char *key) {
  return key != NULL && strstr(key, "/.staging/") != NULL;
}

static int lc_pouch_key_has_suffix(const char *key, const char *suffix) {
  size_t key_len;
  size_t suffix_len;

  if (key == NULL || suffix == NULL) {
    return 0;
  }
  key_len = strlen(key);
  suffix_len = strlen(suffix);
  return key_len >= suffix_len &&
         strcmp(key + key_len - suffix_len, suffix) == 0;
}

static int lc_pouch_attachment_is_delete_marker(const char *content_type) {
  return content_type != NULL &&
         strcmp(content_type, LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE) == 0;
}

static int lc_pouch_attachment_stage_delete(lc_client_handle *client,
                                            const char *ns,
                                            const char *staged_attachment_key,
                                            lc_error *error) {
  lc_source *source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_source_from_memory("", 0U, &source, error);
  if (rc == LC_OK) {
    options.content_type = LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(client->pouch, ns, staged_attachment_key, source,
                              &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static char *lc_pouch_attachment_id_from_name(const char *name,
                                              lc_error *error) {
  char *name_hex;
  char *id;
  size_t length;

  name_hex = lc_pouch_attachment_hex_encode(name);
  if (name_hex == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment id", NULL, NULL,
                       NULL);
    return NULL;
  }
  length = strlen("pouch-att-") + strlen(name_hex) + 1U;
  id = (char *)lc_realloc_local(NULL, length);
  if (id == NULL) {
    lc_free_with_allocator(NULL, name_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment id", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(id, length, "pouch-att-%s", name_hex);
  lc_free_with_allocator(NULL, name_hex);
  return id;
}

static char *
lc_pouch_attachment_name_from_selector(const lc_attachment_selector *selector,
                                       lc_error *error) {
  const char *id_prefix;

  if (selector == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch attachment selector is required", NULL, NULL,
                       NULL);
    return NULL;
  }
  if (selector->name != NULL && selector->name[0] != '\0') {
    return lc_strdup_with_allocator(NULL, selector->name);
  }
  id_prefix = "pouch-att-";
  if (selector->id != NULL &&
      strncmp(selector->id, id_prefix, strlen(id_prefix)) == 0) {
    char *name;

    name = lc_pouch_attachment_hex_decode(selector->id + strlen(id_prefix));
    if (name == NULL) {
      (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                         "pouch attachment id is invalid", NULL, NULL, NULL);
    }
    return name;
  }
  (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                     "pouch attachment selector requires name or pouch id",
                     NULL, NULL, NULL);
  return NULL;
}

int lc_pouch_size_to_public_long(uint64_t size, long *out, lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch size conversion requires output", NULL, NULL,
                        NULL);
  }
  if (size > (uint64_t)LONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch size exceeds public API limit", NULL, NULL,
                        NULL);
  }
  *out = (long)size;
  return LC_OK;
}

static void lc_pouch_attachment_metadata_encode(
    unsigned char metadata[LC_POUCH_ATTACHMENT_METADATA_BYTES],
    lc_pouch_unix_seconds created_at_unix) {
  uint64_t value;
  size_t i;

  memcpy(metadata, LC_POUCH_ATTACHMENT_METADATA_MAGIC, 4U);
  value = (uint64_t)created_at_unix;
  for (i = 0U; i < sizeof(value); ++i) {
    metadata[4U + i] = (unsigned char)((value >> (i * 8U)) & 0xFFU);
  }
}

static int lc_pouch_attachment_created_at_decode(
    const unsigned char *metadata, size_t metadata_length,
    lc_pouch_unix_seconds *created_at_unix, lc_error *error) {
  uint64_t value;
  size_t i;

  if (created_at_unix == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment timestamp requires output", NULL,
                        NULL, NULL);
  }
  if (metadata == NULL ||
      metadata_length != LC_POUCH_ATTACHMENT_METADATA_BYTES ||
      memcmp(metadata, LC_POUCH_ATTACHMENT_METADATA_MAGIC, 4U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment metadata is invalid", NULL, NULL,
                        NULL);
  }
  value = 0U;
  for (i = 0U; i < sizeof(value); ++i) {
    value |= ((uint64_t)metadata[4U + i]) << (i * 8U);
  }
  *created_at_unix = (lc_pouch_unix_seconds)(int64_t)value;
  return LC_OK;
}

static int lc_pouch_attachment_info_fill(lc_attachment_info *info,
                                         const char *name, uint64_t size,
                                         const char *content_type,
                                         lc_pouch_unix_seconds created_at_unix,
                                         lc_pouch_unix_seconds updated_at_unix,
                                         lc_error *error) {
  long public_size = 0L;
  int rc;

  rc = lc_pouch_size_to_public_long(size, &public_size, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(info, 0, sizeof(*info));
  info->id = lc_pouch_attachment_id_from_name(name, error);
  info->name = lc_strdup_local(name);
  info->content_type = lc_strdup_local(content_type);
  if (info->id == NULL || info->name == NULL ||
      (content_type != NULL && info->content_type == NULL)) {
    lc_attachment_info_cleanup(info);
    if (error != NULL && error->code != LC_OK) {
      return error->code;
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment metadata", NULL,
                        NULL, NULL);
  }
  info->size = public_size;
  info->created_at_unix = created_at_unix;
  info->updated_at_unix = updated_at_unix;
  return LC_OK;
}

static size_t lc_pouch_counting_source_read(void *context, void *buffer,
                                            size_t count, lc_error *error) {
  lc_pouch_counting_source *source;
  size_t nread;

  source = (lc_pouch_counting_source *)context;
  if (source == NULL || source->inner == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch counting source requires inner source", NULL,
                       NULL, NULL);
    return 0U;
  }
  nread = source->inner->read(source->inner, buffer, count, error);
  if (nread == 0U) {
    return 0U;
  }
  if (source->bytes > LC_U64_MAX - (uint64_t)nread) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch streamed payload is too large", NULL, NULL, NULL);
    return 0U;
  }
  if (source->has_max_bytes &&
      source->bytes + (uint64_t)nread > (uint64_t)source->max_bytes) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch streamed payload exceeds size limit", NULL, NULL,
                       NULL);
    return 0U;
  }
  source->bytes += (uint64_t)nread;
  return nread;
}

static int lc_pouch_counting_source_reset(void *context, lc_error *error) {
  lc_pouch_counting_source *source;

  source = (lc_pouch_counting_source *)context;
  if (source == NULL || source->inner == NULL || source->inner->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch counting source is not resettable", NULL, NULL,
                        NULL);
  }
  source->bytes = 0UL;
  return source->inner->reset(source->inner, error);
}

/* The state layer invokes this only after acquiring mutation authority, so the
 * input snapshot and the resulting body form one linearizable transform. */
static int lc_pouch_client_prepare_mutation_source(
    const lc_pouch_state_read_result *current, void *context, lc_source **out,
    lc_error *error) {
  lc_pouch_mutation_prepare_context *mutation;
  lc_source *counted_source;
  int rc;

  mutation = (lc_pouch_mutation_prepare_context *)context;
  if (mutation == NULL || mutation->client == NULL || current == NULL ||
      mutation->plan == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation source preparation requires state and "
                        "plan context",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  mutation->source = NULL;
  counted_source = NULL;
  rc = lc_pouch_prepare_mutation_file_from_plan(
      mutation->client, current, mutation->plan, &mutation->mutated, error);
  if (rc == LC_OK) {
    mutation->source = lc_source_from_open_file(mutation->mutated.fp, 0);
    if (mutation->source == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch mutate result source", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    mutation->counting_source.inner = mutation->source;
    mutation->counting_source.max_bytes = LONG_MAX;
    mutation->counting_source.has_max_bytes = 1;
    rc = lc_source_from_callbacks(
        lc_pouch_counting_source_read, lc_pouch_counting_source_reset, NULL,
        &mutation->counting_source, &counted_source, error);
  }
  if (rc != LC_OK && mutation->source != NULL) {
    lc_source_close(mutation->source);
    mutation->source = NULL;
  }
  if (rc == LC_OK) {
    *out = counted_source;
  }
  return rc;
}

static int lc_pouch_attach_write_locked(void *context, lc_error *error) {
  lc_pouch_attach_write_context *ctx;
  lc_pouch_state_read_result current;
  lc_pouch_state_write_options options;
  lc_pouch_unix_seconds created_at_unix;
  const char *write_key;
  unsigned char metadata[LC_POUCH_ATTACHMENT_METADATA_BYTES];
  int current_is_attachment;
  int transaction_bound;
  int rc;

  ctx = (lc_pouch_attach_write_context *)context;
  memset(&current, 0, sizeof(current));
  created_at_unix = 0L;
  transaction_bound =
      ctx->stage_transaction && lc_pouch_txn_id_present(ctx->req->lease.txn_id);
  write_key =
      transaction_bound ? ctx->staged_attachment_key : ctx->attachment_key;
  rc = lc_pouch_state_read(ctx->client->pouch, ctx->ns, write_key, &current,
                           error);
  current_is_attachment =
      current.found &&
      !lc_pouch_attachment_is_delete_marker(current.content_type);
  if (rc == LC_OK && ctx->req->prevent_overwrite && current.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch attachment already exists", NULL, NULL, NULL);
  }
  if (rc == LC_OK && current_is_attachment) {
    rc = lc_pouch_attachment_created_at_decode(
        current.metadata, current.metadata_length, &created_at_unix, error);
  }
  lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &current);
  memset(&current, 0, sizeof(current));
  if (rc == LC_OK && transaction_bound &&
      (ctx->req->prevent_overwrite || created_at_unix == 0L)) {
    rc = lc_pouch_state_read(ctx->client->pouch, ctx->ns, ctx->attachment_key,
                             &current, error);
    if (rc == LC_OK && ctx->req->prevent_overwrite && current.found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment already exists", NULL, NULL, NULL);
    }
    if (rc == LC_OK && current.found && created_at_unix == 0L) {
      rc = lc_pouch_attachment_created_at_decode(
          current.metadata, current.metadata_length, &created_at_unix, error);
    }
    lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &current);
  }
  if (rc == LC_OK && created_at_unix == 0L) {
    rc = lc_pouch_now_unix(&created_at_unix, error);
  }
  if (rc == LC_OK) {
    lc_pouch_attachment_metadata_encode(metadata, created_at_unix);
    options = *ctx->options;
    options.metadata = metadata;
    options.metadata_length = sizeof(metadata);
    options.has_metadata = 1;
    rc = lc_pouch_state_write(ctx->client->pouch, ctx->ns, write_key,
                              ctx->source, &options, ctx->result, error);
  }
  if (rc == LC_OK) {
    ctx->attachment_created_at_unix = created_at_unix;
  }
  return rc;
}

static void lc_pouch_attachment_list_builder_cleanup(
    lc_pouch_attachment_list_builder *builder) {
  size_t i;

  if (builder == NULL) {
    return;
  }
  for (i = 0U; i < builder->count; ++i) {
    lc_attachment_info_cleanup(&builder->items[i]);
  }
  for (i = 0U; i < builder->key_count; ++i) {
    lc_free_with_allocator(NULL, builder->keys[i].key);
  }
  lc_free_local(builder->items);
  lc_free_with_allocator(NULL, builder->keys);
  lc_free_with_allocator(NULL, builder->prefix);
  memset(builder, 0, sizeof(*builder));
}

static int
lc_pouch_attachment_append_key(lc_pouch_attachment_list_builder *builder,
                               const char *key, lc_pouch_generation version,
                               lc_error *error) {
  lc_pouch_attachment_key_ref *next;
  char *copy;
  size_t capacity;

  if (builder->key_count == builder->key_capacity) {
    capacity = builder->key_capacity == 0U ? 8U : builder->key_capacity * 2U;
    next = (lc_pouch_attachment_key_ref *)lc_realloc_with_allocator(
        NULL, builder->keys, capacity * sizeof(builder->keys[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch attachment key list", NULL,
                          NULL, NULL);
    }
    builder->keys = next;
    builder->key_capacity = capacity;
  }
  copy = lc_strdup_with_allocator(NULL, key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment key", NULL, NULL,
                        NULL);
  }
  builder->keys[builder->key_count].key = copy;
  builder->keys[builder->key_count].version = version;
  ++builder->key_count;
  return LC_OK;
}

static int lc_pouch_attachment_append_info(
    lc_pouch_attachment_list_builder *builder, const char *name, uint64_t size,
    const char *content_type, lc_pouch_unix_seconds created_at_unix,
    lc_pouch_unix_seconds updated_at_unix, lc_error *error) {
  lc_attachment_info *next;
  size_t capacity;
  int rc;

  if (builder->count == builder->capacity) {
    capacity = builder->capacity == 0U ? 4U : builder->capacity * 2U;
    next = (lc_attachment_info *)lc_realloc_local(
        builder->items, capacity * sizeof(builder->items[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch attachment list", NULL,
                          NULL, NULL);
    }
    memset(next + builder->capacity, 0,
           (capacity - builder->capacity) * sizeof(builder->items[0]));
    builder->items = next;
    builder->capacity = capacity;
  }
  rc = lc_pouch_attachment_info_fill(&builder->items[builder->count], name,
                                     size, content_type, created_at_unix,
                                     updated_at_unix, error);
  if (rc == LC_OK) {
    ++builder->count;
  }
  return rc;
}

static int lc_pouch_attachment_visit(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  lc_pouch_attachment_list_builder *builder;
  lc_pouch_unix_seconds created_at_unix;
  char *name;
  int rc;

  builder = (lc_pouch_attachment_list_builder *)context;
  created_at_unix = 0L;
  if (strncmp(entry->key, builder->prefix, builder->prefix_len) != 0) {
    return LC_OK;
  }
  if (lc_pouch_storage_key_has_staging_suffix(entry->key)) {
    return LC_OK;
  }
  name = lc_pouch_attachment_hex_decode(entry->key + builder->prefix_len);
  if (name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment key is corrupt", entry->key, NULL,
                        NULL);
  }
  rc = lc_pouch_attachment_created_at_decode(
      entry->metadata, entry->metadata_length, &created_at_unix, error);
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_append_info(builder, name, entry->bytes,
                                         entry->content_type, created_at_unix,
                                         entry->updated_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_append_key(builder, entry->key, entry->version,
                                        error);
  }
  lc_free_with_allocator(NULL, name);
  return rc;
}

static size_t
lc_pouch_attachment_info_find(const lc_pouch_attachment_list_builder *builder,
                              const char *name) {
  size_t i;

  if (builder == NULL || name == NULL) {
    return 0U;
  }
  for (i = 0U; i < builder->count; ++i) {
    if (builder->items[i].name != NULL &&
        strcmp(builder->items[i].name, name) == 0) {
      return i;
    }
  }
  return builder->count;
}

static void
lc_pouch_attachment_info_remove(lc_pouch_attachment_list_builder *builder,
                                size_t index) {
  if (builder == NULL || index >= builder->count) {
    return;
  }
  lc_attachment_info_cleanup(&builder->items[index]);
  if (index + 1U < builder->count) {
    memmove(&builder->items[index], &builder->items[index + 1U],
            (builder->count - index - 1U) * sizeof(builder->items[0]));
  }
  --builder->count;
  memset(&builder->items[builder->count], 0, sizeof(builder->items[0]));
}

static int lc_pouch_attachment_info_replace(
    lc_pouch_attachment_list_builder *builder, size_t index, const char *name,
    uint64_t size, const char *content_type,
    lc_pouch_unix_seconds created_at_unix,
    lc_pouch_unix_seconds updated_at_unix, lc_error *error) {
  lc_attachment_info replacement;
  int rc;

  if (builder == NULL || index >= builder->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment replacement requires list item", NULL,
                        NULL, "pouch");
  }
  memset(&replacement, 0, sizeof(replacement));
  rc = lc_pouch_attachment_info_fill(&replacement, name, size, content_type,
                                     created_at_unix, updated_at_unix, error);
  if (rc == LC_OK) {
    lc_attachment_info_cleanup(&builder->items[index]);
    builder->items[index] = replacement;
  }
  return rc;
}

static int
lc_pouch_staged_attachment_visit(const lc_pouch_state_visit_entry *entry,
                                 void *context, lc_error *error) {
  lc_pouch_staged_attachment_overlay *overlay;
  lc_pouch_unix_seconds created_at_unix;
  char *name;
  size_t index;
  int rc;

  overlay = (lc_pouch_staged_attachment_overlay *)context;
  if (overlay == NULL || overlay->builder == NULL || entry == NULL ||
      entry->key == NULL ||
      strncmp(entry->key, overlay->prefix, overlay->prefix_len) != 0) {
    return LC_OK;
  }
  name = lc_pouch_attachment_hex_decode(entry->key + overlay->prefix_len);
  if (name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attachment key is corrupt", entry->key,
                        NULL, NULL);
  }
  index = lc_pouch_attachment_info_find(overlay->builder, name);
  if (lc_pouch_attachment_is_delete_marker(entry->content_type)) {
    if (index < overlay->builder->count) {
      lc_pouch_attachment_info_remove(overlay->builder, index);
    }
    lc_free_with_allocator(NULL, name);
    return LC_OK;
  }
  created_at_unix = 0L;
  rc = lc_pouch_attachment_created_at_decode(
      entry->metadata, entry->metadata_length, &created_at_unix, error);
  if (rc == LC_OK && index < overlay->builder->count) {
    rc = lc_pouch_attachment_info_replace(
        overlay->builder, index, name, entry->bytes, entry->content_type,
        created_at_unix, entry->updated_at_unix, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_attachment_append_info(overlay->builder, name, entry->bytes,
                                         entry->content_type, created_at_unix,
                                         entry->updated_at_unix, error);
  }
  lc_free_with_allocator(NULL, name);
  return rc;
}

static int lc_pouch_attachment_info_compare(const void *left,
                                            const void *right) {
  const lc_attachment_info *a;
  const lc_attachment_info *b;

  a = (const lc_attachment_info *)left;
  b = (const lc_attachment_info *)right;
  return strcmp(a->name != NULL ? a->name : "", b->name != NULL ? b->name : "");
}

static int lc_pouch_collect_attachments(
    lc_client_handle *client, const char *ns, const char *key,
    lc_pouch_attachment_list_builder *builder, lc_error *error) {
  int rc;

  memset(builder, 0, sizeof(*builder));
  builder->prefix = lc_pouch_attachment_prefix(ns, key, error);
  if (builder->prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  builder->prefix_len = strlen(builder->prefix);
  rc = lc_pouch_state_visit_prefix(client->pouch, ns, builder->prefix,
                                   lc_pouch_attachment_visit, builder, error);
  if (rc == LC_OK && builder->count > 1U) {
    qsort(builder->items, builder->count, sizeof(builder->items[0]),
          lc_pouch_attachment_info_compare);
  }
  return rc;
}

static int lc_pouch_outbox_retained_bytes_add(uint64_t *total, uint64_t value,
                                              lc_error *error) {
  if (total == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retained-byte total is required", NULL, NULL,
                        "pouch");
  }
  if (~(uint64_t)0U - *total < value) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retained-byte total exceeds supported range",
                        NULL, NULL, "pouch");
  }
  *total += value;
  return LC_OK;
}

static int lc_pouch_outbox_retained_bytes_for_key(lc_client_handle *client,
                                                  const char *ns,
                                                  const char *key,
                                                  uint64_t *total,
                                                  lc_error *error) {
  lc_pouch_state_read_result state;
  lc_pouch_attachment_list_builder attachments;
  size_t index;
  int rc;

  if (client == NULL || !client->is_pouch || ns == NULL || ns[0] == '\0' ||
      key == NULL || key[0] == '\0' || total == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retained-byte measurement requires key", NULL,
                        NULL, "pouch");
  }
  memset(&state, 0, sizeof(state));
  memset(&attachments, 0, sizeof(attachments));
  rc = lc_pouch_state_read_metadata(client->pouch, ns, key, &state, error);
  if (rc == LC_OK && state.found) {
    rc = lc_pouch_outbox_retained_bytes_add(total, state.bytes, error);
  }
  lc_pouch_state_read_result_cleanup(&client->pouch->allocator, &state);
  if (rc == LC_OK) {
    rc = lc_pouch_collect_attachments(client, ns, key, &attachments, error);
  }
  for (index = 0U; rc == LC_OK && index < attachments.count; ++index) {
    if (attachments.items[index].size < 0L) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch attachment has an invalid retained size", NULL,
                        NULL, "pouch");
    } else {
      rc = lc_pouch_outbox_retained_bytes_add(
          total, (uint64_t)attachments.items[index].size, error);
    }
  }
  lc_pouch_attachment_list_builder_cleanup(&attachments);
  return rc;
}

int lc_pouch_outbox_dead_letter_retained_bytes(lc_client_handle *client,
                                               const char *ns,
                                               const char *outbox_key,
                                               const char *state_key,
                                               uint64_t *out, lc_error *error) {
  uint64_t total;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retained-byte output is required", NULL, NULL,
                        "pouch");
  }
  *out = 0U;
  total = 0U;
  rc = lc_pouch_outbox_retained_bytes_for_key(client, ns, outbox_key, &total,
                                              error);
  if (rc == LC_OK && state_key != NULL && state_key[0] != '\0') {
    rc = lc_pouch_outbox_retained_bytes_for_key(client, ns, state_key, &total,
                                                error);
  }
  if (rc == LC_OK)
    *out = total;
  return rc;
}

/* Private transaction reads see their own attachment writes and deletes. The
 * public view stays committed-only, so staged object bytes never leak. */
static int
lc_pouch_overlay_staged_attachments(lc_client_handle *client, const char *ns,
                                    const char *key, const char *txn_id,
                                    lc_pouch_attachment_list_builder *builder,
                                    lc_error *error) {
  lc_pouch_staged_attachment_overlay overlay;
  char *prefix;
  int rc;

  if (client == NULL || ns == NULL || key == NULL ||
      !lc_pouch_txn_id_present(txn_id) || builder == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attachment overlay requires transaction",
                        NULL, NULL, "pouch");
  }
  prefix = lc_pouch_staged_attachment_prefix(ns, key, txn_id, error);
  if (prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&overlay, 0, sizeof(overlay));
  overlay.builder = builder;
  overlay.prefix = prefix;
  overlay.prefix_len = strlen(prefix);
  rc = lc_pouch_state_visit_prefix(client->pouch, ns, prefix,
                                   lc_pouch_staged_attachment_visit, &overlay,
                                   error);
  if (rc == LC_OK && builder->count > 1U) {
    qsort(builder->items, builder->count, sizeof(builder->items[0]),
          lc_pouch_attachment_info_compare);
  }
  lc_free_with_allocator(NULL, prefix);
  return rc;
}

static void lc_pouch_txn_buffer_cleanup(lc_pouch_txn_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, buffer->bytes);
  memset(buffer, 0, sizeof(*buffer));
}

static void lc_pouch_txn_key_list_cleanup(lc_pouch_txn_key_list *list) {
  size_t i;

  if (list == NULL) {
    return;
  }
  for (i = 0U; i < list->count; ++i) {
    lc_free_with_allocator(NULL, list->keys[i]);
  }
  lc_free_with_allocator(NULL, list->keys);
  memset(list, 0, sizeof(*list));
}

static int lc_pouch_txn_key_list_append(lc_pouch_txn_key_list *list,
                                        const char *key, lc_error *error) {
  char **next;
  char *copy;
  size_t capacity;

  if (list->count == list->capacity) {
    capacity = list->capacity != 0U ? list->capacity * 2U : 8U;
    next = (char **)lc_realloc_with_allocator(NULL, list->keys,
                                              capacity * sizeof(*next));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction key list", NULL,
                          NULL, NULL);
    }
    list->keys = next;
    list->capacity = capacity;
  }
  copy = lc_strdup_with_allocator(NULL, key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch transaction key", NULL, NULL,
                        NULL);
  }
  list->keys[list->count++] = copy;
  return LC_OK;
}

static int lc_pouch_txn_collect_key(const lc_pouch_state_visit_entry *entry,
                                    void *context, lc_error *error) {
  if (entry->key == NULL || entry->key[0] == '\0' ||
      lc_pouch_storage_key_has_staging_suffix(entry->key)) {
    return LC_OK;
  }
  return lc_pouch_txn_key_list_append((lc_pouch_txn_key_list *)context,
                                      entry->key, error);
}

static void lc_pouch_txn_record_cleanup(lc_pouch_txn_record *record) {
  size_t i;

  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, record->state);
  lc_free_with_allocator(NULL, record->target_backend_hash);
  for (i = 0U; i < record->participant_count; ++i) {
    lc_free_with_allocator(NULL, (char *)record->participants[i].ns);
    lc_free_with_allocator(NULL, (char *)record->participants[i].key);
    lc_free_with_allocator(NULL, (char *)record->participants[i].backend_hash);
  }
  lc_free_with_allocator(NULL, record->participants);
  lc_free_with_allocator(NULL, record->votes);
  memset(record, 0, sizeof(*record));
}

static int lc_pouch_txn_record_add_participant(lc_pouch_txn_record *record,
                                               char *ns, char *key,
                                               char *backend_hash,
                                               lc_error *error) {
  lc_txn_participant *next;
  unsigned char *next_votes;
  size_t capacity;

  if (record->participant_count == record->participant_capacity) {
    capacity = record->participant_capacity != 0U
                   ? record->participant_capacity * 2U
                   : 4U;
    next = (lc_txn_participant *)lc_realloc_with_allocator(
        NULL, record->participants, capacity * sizeof(*next));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction participants",
                          NULL, NULL, NULL);
    }
    record->participants = next;
    next_votes = (unsigned char *)lc_realloc_with_allocator(
        NULL, record->votes, capacity * sizeof(*next_votes));
    if (next_votes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction votes", NULL,
                          NULL, NULL);
    }
    record->votes = next_votes;
    record->participant_capacity = capacity;
  }
  record->participants[record->participant_count].ns = ns;
  record->participants[record->participant_count].key = key;
  record->participants[record->participant_count].backend_hash = backend_hash;
  record->votes[record->participant_count] = 0U;
  ++record->participant_count;
  return LC_OK;
}

static int lc_pouch_txn_record_replace_string(char **field, const char *value,
                                              lc_error *error) {
  char *copy;

  copy = lc_strdup_with_allocator(NULL, value != NULL ? value : "");
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction field", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(NULL, *field);
  *field = copy;
  return LC_OK;
}

static int lc_pouch_txn_record_replace_trimmed_string(char **field,
                                                      const char *value,
                                                      lc_error *error) {
  const char *start;
  size_t length;
  char *copy;

  lc_pouch_txn_trim_bounds(value, &start, &length);
  copy = (char *)lc_alloc_with_allocator(NULL, length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction field", NULL,
                        NULL, NULL);
  }
  if (length != 0U) {
    memcpy(copy, start, length);
  }
  copy[length] = '\0';
  lc_free_with_allocator(NULL, *field);
  *field = copy;
  return LC_OK;
}

static int
lc_pouch_txn_record_add_participant_copy(lc_pouch_txn_record *record,
                                         const lc_txn_participant *participant,
                                         lc_error *error) {
  char *ns;
  char *key;
  char *backend_hash;
  int rc;

  ns = NULL;
  key = NULL;
  backend_hash = NULL;
  rc = lc_pouch_txn_record_replace_string(
      &ns, participant != NULL ? participant->ns : NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_record_replace_string(
        &key, participant != NULL ? participant->key : NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_record_replace_trimmed_string(
        &backend_hash, participant != NULL ? participant->backend_hash : NULL,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_record_add_participant(record, ns, key, backend_hash,
                                             error);
    if (rc == LC_OK) {
      ns = NULL;
      key = NULL;
      backend_hash = NULL;
    }
  }
  lc_free_with_allocator(NULL, ns);
  lc_free_with_allocator(NULL, key);
  lc_free_with_allocator(NULL, backend_hash);
  return rc;
}

static int lc_pouch_txn_participants_match(const lc_txn_participant *left,
                                           const lc_txn_participant *right) {
  const char *left_backend;
  const char *right_backend;

  if (left == NULL || right == NULL || left->ns == NULL || left->key == NULL ||
      right->ns == NULL || right->key == NULL ||
      strcmp(left->ns, right->ns) != 0 || strcmp(left->key, right->key) != 0) {
    return 0;
  }
  left_backend = left->backend_hash != NULL ? left->backend_hash : "";
  right_backend = right->backend_hash != NULL ? right->backend_hash : "";
  return left_backend[0] == '\0' || right_backend[0] == '\0' ||
         strcmp(left_backend, right_backend) == 0;
}

static int lc_pouch_txn_participant_compare(const void *left,
                                            const void *right) {
  const lc_txn_participant *left_participant;
  const lc_txn_participant *right_participant;
  const char *left_backend;
  const char *right_backend;
  int rc;

  left_participant = (const lc_txn_participant *)left;
  right_participant = (const lc_txn_participant *)right;
  left_backend = left_participant->backend_hash != NULL
                     ? left_participant->backend_hash
                     : "";
  right_backend = right_participant->backend_hash != NULL
                      ? right_participant->backend_hash
                      : "";
  rc = strcmp(left_backend, right_backend);
  if (rc == 0) {
    rc = strcmp(left_participant->ns, right_participant->ns);
  }
  if (rc == 0) {
    rc = strcmp(left_participant->key, right_participant->key);
  }
  return rc;
}

/* Transaction records keep votes positionally aligned with participants.
 * Reorder the two arrays as pairs whenever enrollment canonicalizes the
 * participant order, including after a participant has already voted. */
static void lc_pouch_txn_record_sort_participants(lc_pouch_txn_record *record) {
  size_t index;

  if (record == NULL || record->participants == NULL || record->votes == NULL) {
    return;
  }
  for (index = 1U; index < record->participant_count; ++index) {
    lc_txn_participant participant;
    unsigned char vote;
    size_t cursor;

    participant = record->participants[index];
    vote = record->votes[index];
    cursor = index;
    while (cursor > 0U &&
           lc_pouch_txn_participant_compare(&record->participants[cursor - 1U],
                                            &participant) > 0) {
      record->participants[cursor] = record->participants[cursor - 1U];
      record->votes[cursor] = record->votes[cursor - 1U];
      --cursor;
    }
    record->participants[cursor] = participant;
    record->votes[cursor] = vote;
  }
}

static int lc_pouch_txn_record_has_xa(const lc_pouch_txn_record *record) {
  size_t i;
  for (i = 0U; i < record->participant_count; ++i)
    if (lc_pouch_xa_participant(&record->participants[i]))
      return 1;
  return 0;
}

static int lc_pouch_txn_record_merge_request(
    lc_pouch_txn_record *record, const lc_txn_decision_req *request,
    const char *requested_state, int owner_sealed, int *write_record,
    int *apply_decision, lc_error *error) {
  size_t i;
  int rc;
  int sealed;

  if (record == NULL || request == NULL || requested_state == NULL ||
      write_record == NULL || apply_decision == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction record merge requires context", NULL,
                        NULL, NULL);
  }
  *write_record = 0;
  *apply_decision = 0;
  sealed = owner_sealed;
  if (lc_pouch_txn_record_has_xa(record) ||
      lc_pouch_xa_has_participants(request)) {
    sealed = sealed ||
             (record->state != NULL && strcmp(record->state, "prepare") != 0);
    for (i = 0U; i < record->participant_count; ++i)
      if (record->votes[i] != 0U)
        sealed = 1;
  }
  rc = lc_pouch_txn_validate_participants(request, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (record->state == NULL) {
    rc = lc_pouch_txn_record_replace_string(&record->state, requested_state,
                                            error);
    if (rc == LC_OK) {
      record->expires_at_unix = request->expires_at_unix;
      record->tc_term = request->tc_term;
      rc = lc_pouch_txn_record_replace_trimmed_string(
          &record->target_backend_hash, request->target_backend_hash, error);
    }
  } else {
    if (strcmp(requested_state, "prepare") == 0 &&
        strcmp(record->state, "prepare") != 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction is already decided", NULL, NULL,
                          NULL);
    }
    if (record->tc_term != 0U && request->tc_term == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction tc_term is required", NULL, NULL,
                          NULL);
    }
    if (request->tc_term != 0U && record->tc_term != 0U &&
        request->tc_term < record->tc_term) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction tc_term is stale", NULL, NULL,
                          NULL);
    }
    if (strcmp(record->state, "prepare") != 0 &&
        strcmp(record->state, requested_state) != 0) {
      if (request->tc_term != 0U || record->tc_term != 0U ||
          lc_pouch_txn_record_has_xa(record)) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch transaction is already decided", NULL, NULL,
                            NULL);
      }
      return LC_OK;
    }
    if (request->expires_at_unix > 0L &&
        (record->expires_at_unix <= 0L ||
         request->expires_at_unix < record->expires_at_unix)) {
      record->expires_at_unix = request->expires_at_unix;
    }
    if (request->tc_term != 0U) {
      record->tc_term = request->tc_term;
    }
    if ((record->target_backend_hash == NULL ||
         record->target_backend_hash[0] == '\0') &&
        request->target_backend_hash != NULL &&
        request->target_backend_hash[0] != '\0') {
      rc = lc_pouch_txn_record_replace_trimmed_string(
          &record->target_backend_hash, request->target_backend_hash, error);
    }
    if (rc == LC_OK && strcmp(record->state, "prepare") == 0) {
      rc = lc_pouch_txn_record_replace_string(&record->state, requested_state,
                                              error);
    }
  }
  for (i = 0U; rc == LC_OK && i < request->participant_count; ++i) {
    size_t existing;

    for (existing = 0U; existing < record->participant_count; ++existing) {
      if (lc_pouch_txn_participants_match(&record->participants[existing],
                                          &request->participants[i])) {
        const char *backend_hash = request->participants[i].backend_hash;

        if ((record->participants[existing].backend_hash == NULL ||
             record->participants[existing].backend_hash[0] == '\0') &&
            backend_hash != NULL && backend_hash[0] != '\0') {
          char *replacement;

          replacement = NULL;
          rc = lc_pouch_txn_record_replace_trimmed_string(&replacement,
                                                          backend_hash, error);
          if (rc == LC_OK) {
            lc_free_with_allocator(
                NULL, (char *)record->participants[existing].backend_hash);
            record->participants[existing].backend_hash = replacement;
          }
        }
        break;
      }
    }
    if (rc == LC_OK && existing == record->participant_count) {
      if (sealed)
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "XA transaction enrollment is sealed", NULL, NULL,
                            "pouch-xa");
      rc = lc_pouch_txn_record_add_participant_copy(
          record, &request->participants[i], error);
    }
  }
  if (rc == LC_OK && record->participant_count > 1U) {
    lc_pouch_txn_record_sort_participants(record);
  }
  if (rc == LC_OK) {
    *write_record = 1;
    *apply_decision = strcmp(requested_state, "prepare") != 0 &&
                      strcmp(record->state, requested_state) == 0;
  }
  return rc;
}

static int lc_pouch_txn_buffer_reserve(lc_pouch_txn_buffer *buffer,
                                       size_t needed, lc_error *error) {
  char *next;
  size_t capacity;

  if (needed <= buffer->capacity) {
    return LC_OK;
  }
  capacity = buffer->capacity != 0U ? buffer->capacity : 256U;
  while (capacity < needed) {
    if (capacity > ((size_t)-1) / 2U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction record is too large", NULL, NULL,
                          NULL);
    }
    capacity *= 2U;
  }
  next = (char *)lc_realloc_with_allocator(NULL, buffer->bytes, capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction record", NULL,
                        NULL, NULL);
  }
  buffer->bytes = next;
  buffer->capacity = capacity;
  return LC_OK;
}

static int lc_pouch_txn_buffer_append_bytes(lc_pouch_txn_buffer *buffer,
                                            const void *bytes, size_t length,
                                            lc_error *error) {
  int rc;

  if (length == 0U) {
    return LC_OK;
  }
  if (buffer == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary record append requires bytes", NULL, NULL,
                        "pouch");
  }
  if (length > (size_t)-1 - buffer->length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary record is too large", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_txn_buffer_reserve(buffer, buffer->length + length, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(buffer->bytes + buffer->length, bytes, length);
  buffer->length += length;
  return LC_OK;
}

static int lc_pouch_txn_buffer_append_u16(lc_pouch_txn_buffer *buffer,
                                          unsigned long value,
                                          lc_error *error) {
  unsigned char bytes[2];

  if (value > 0xFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary string is too large", NULL, NULL,
                        "pouch");
  }
  bytes[0] = (unsigned char)(value & 0xFFUL);
  bytes[1] = (unsigned char)((value >> 8U) & 0xFFUL);
  return lc_pouch_txn_buffer_append_bytes(buffer, bytes, sizeof(bytes), error);
}

static int lc_pouch_txn_buffer_append_u64(lc_pouch_txn_buffer *buffer,
                                          uint64_t value, lc_error *error) {
  unsigned char bytes[8];
  size_t i;

  for (i = 0U; i < sizeof(bytes); ++i) {
    bytes[i] = (unsigned char)((value >> (i * 8U)) & 0xFFU);
  }
  return lc_pouch_txn_buffer_append_bytes(buffer, bytes, sizeof(bytes), error);
}

static int lc_pouch_txn_buffer_append_i64(lc_pouch_txn_buffer *buffer,
                                          int64_t value, lc_error *error) {
  return lc_pouch_txn_buffer_append_u64(buffer, (uint64_t)value, error);
}

static int lc_pouch_txn_buffer_append_string(lc_pouch_txn_buffer *buffer,
                                             const char *value,
                                             lc_error *error) {
  size_t length;
  int rc;

  if (value == NULL) {
    value = "";
  }
  length = strlen(value);
  if (length > LC_POUCH_CONTROL_STRING_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary string exceeds limit", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_txn_buffer_append_u16(buffer, (unsigned long)length, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_bytes(buffer, value, length, error);
  }
  return rc;
}

static int lc_pouch_binary_cursor_read(lc_pouch_binary_cursor *cursor,
                                       void *out, size_t length,
                                       lc_error *error) {
  if (cursor == NULL || out == NULL ||
      length > cursor->length - cursor->offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary control record is truncated", NULL, NULL,
                        "pouch");
  }
  memcpy(out, cursor->bytes + cursor->offset, length);
  cursor->offset += length;
  return LC_OK;
}

static int lc_pouch_binary_cursor_magic(lc_pouch_binary_cursor *cursor,
                                        const char magic[4], lc_error *error) {
  unsigned char actual[4];
  int rc;

  rc = lc_pouch_binary_cursor_read(cursor, actual, sizeof(actual), error);
  if (rc != LC_OK) {
    return rc;
  }
  if (memcmp(actual, magic, sizeof(actual)) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary control record magic mismatch", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_binary_cursor_u16(lc_pouch_binary_cursor *cursor,
                                      unsigned long *out, lc_error *error) {
  unsigned char bytes[2];
  int rc;

  rc = lc_pouch_binary_cursor_read(cursor, bytes, sizeof(bytes), error);
  if (rc == LC_OK) {
    *out = (unsigned long)bytes[0] | ((unsigned long)bytes[1] << 8U);
  }
  return rc;
}

static int lc_pouch_binary_cursor_u64(lc_pouch_binary_cursor *cursor,
                                      uint64_t *out, lc_error *error) {
  unsigned char bytes[8] = {0U};
  uint64_t value;
  size_t i;
  int rc;

  rc = lc_pouch_binary_cursor_read(cursor, bytes, sizeof(bytes), error);
  if (rc != LC_OK) {
    return rc;
  }
  value = 0U;
  for (i = 0U; i < sizeof(bytes); ++i) {
    value |= ((uint64_t)bytes[i]) << (i * 8U);
  }
  *out = value;
  return LC_OK;
}

static int lc_pouch_binary_cursor_i64(lc_pouch_binary_cursor *cursor,
                                      int64_t *out, lc_error *error) {
  uint64_t value;
  int rc;

  rc = lc_pouch_binary_cursor_u64(cursor, &value, error);
  if (rc == LC_OK) {
    *out = (int64_t)value;
  }
  return rc;
}

static int lc_pouch_binary_cursor_string(lc_pouch_binary_cursor *cursor,
                                         char **out, lc_error *error) {
  unsigned long length;
  char *copy;
  int rc;

  *out = NULL;
  rc = lc_pouch_binary_cursor_u16(cursor, &length, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (length > cursor->length - cursor->offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary string is truncated", NULL, NULL,
                        "pouch");
  }
  copy = (char *)lc_alloc_with_allocator(NULL, (size_t)length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch binary string", NULL, NULL,
                        "pouch");
  }
  if (length > 0UL) {
    memcpy(copy, cursor->bytes + cursor->offset, (size_t)length);
  }
  copy[length] = '\0';
  cursor->offset += (size_t)length;
  *out = copy;
  return LC_OK;
}

static int lc_pouch_source_read_exact(lc_source *source, void *buffer,
                                      size_t length, lc_error *error) {
  unsigned char *out;
  size_t offset;

  if (source == NULL || buffer == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source exact read requires source and buffer",
                        NULL, NULL, "pouch");
  }
  out = (unsigned char *)buffer;
  offset = 0U;
  while (offset < length) {
    size_t got;

    got = source->read(source, out + offset, length - offset, error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        return error->code;
      }
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch source ended before binary control record",
                          NULL, NULL, "pouch");
    }
    offset += got;
  }
  return LC_OK;
}

static void lc_pouch_queue_record_cleanup(lc_pouch_queue_record *record) {
  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, record->storage_key);
  lc_free_with_allocator(NULL, record->ns);
  lc_free_with_allocator(NULL, record->queue);
  lc_free_with_allocator(NULL, record->message_id);
  lc_free_with_allocator(NULL, record->status);
  lc_free_with_allocator(NULL, record->content_type);
  lc_free_with_allocator(NULL, record->lease_id);
  lc_free_with_allocator(NULL, record->lease_txn_id);
  lc_free_with_allocator(NULL, record->meta_etag);
  lc_free_with_allocator(NULL, record->payload);
  memset(record, 0, sizeof(*record));
}

static void lc_pouch_queue_scan_cleanup(lc_pouch_queue_scan *scan) {
  size_t i;

  if (scan == NULL) {
    return;
  }
  for (i = 0U; i < scan->count; ++i) {
    lc_pouch_queue_record_cleanup(&scan->records[i]);
  }
  lc_free_with_allocator(NULL, scan->records);
  lc_free_with_allocator(NULL, (char *)scan->prefix);
  memset(scan, 0, sizeof(*scan));
}

static char *lc_pouch_queue_normalize_name(const char *queue, lc_error *error) {
  char *normalized;
  size_t len;
  size_t i;

  if (queue == NULL || queue[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue name is required", NULL, NULL, "pouch");
    return NULL;
  }
  len = strlen(queue);
  while (len > 0U && (queue[0] == ' ' || queue[0] == '\t' || queue[0] == '\r' ||
                      queue[0] == '\n')) {
    ++queue;
    --len;
  }
  while (len > 0U && (queue[len - 1U] == ' ' || queue[len - 1U] == '\t' ||
                      queue[len - 1U] == '\r' || queue[len - 1U] == '\n')) {
    --len;
  }
  if (len == 0U || len > 128U) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L, "pouch queue name is invalid",
                       NULL, NULL, "pouch");
    return NULL;
  }
  normalized = (char *)lc_alloc_with_allocator(NULL, len + 1U);
  if (normalized == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue name", NULL, NULL,
                       "pouch");
    return NULL;
  }
  for (i = 0U; i < len; ++i) {
    char c;

    c = queue[i];
    if (c >= 'A' && c <= 'Z') {
      c = (char)(c - 'A' + 'a');
    }
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' ||
          c == '_' || c == '-')) {
      lc_free_with_allocator(NULL, normalized);
      (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                         "pouch queue name is invalid", NULL, NULL, "pouch");
      return NULL;
    }
    normalized[i] = c;
  }
  normalized[len] = '\0';
  return normalized;
}

static char *lc_pouch_queue_prefix(const char *ns, const char *queue,
                                   lc_error *error) {
  char *queue_name;
  char *prefix;
  size_t length;

  (void)ns;
  queue_name = lc_pouch_queue_normalize_name(queue, error);
  if (queue_name == NULL) {
    return NULL;
  }
  length = strlen("q//msg/") + strlen(queue_name) + 1U;
  prefix = (char *)lc_alloc_with_allocator(NULL, length);
  if (prefix == NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue key prefix", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(prefix, length, "q/%s/msg/", queue_name);
  lc_free_with_allocator(NULL, queue_name);
  return prefix;
}

static char *lc_pouch_queue_key(const char *ns, const char *queue,
                                const char *message_id, lc_error *error) {
  char *prefix;
  char *key;
  size_t length;

  prefix = lc_pouch_queue_prefix(ns, queue, error);
  if (prefix == NULL) {
    lc_free_with_allocator(NULL, prefix);
    return NULL;
  }
  if (message_id == NULL || message_id[0] == '\0' ||
      strchr(message_id, '/') != NULL) {
    lc_free_with_allocator(NULL, prefix);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message id is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  length = strlen(prefix) + strlen(message_id) + strlen(".meta") + 1U;
  key = (char *)lc_alloc_with_allocator(NULL, length);
  if (key == NULL) {
    lc_free_with_allocator(NULL, prefix);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue key", NULL, NULL, NULL);
    return NULL;
  }
  snprintf(key, length, "%s%s.meta", prefix, message_id);
  lc_free_with_allocator(NULL, prefix);
  return key;
}

static char *lc_pouch_queue_payload_key_from_meta(const char *metadata_key,
                                                  lc_error *error) {
  char *key;
  size_t length;
  size_t stem_length;

  if (metadata_key == NULL || metadata_key[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue payload key requires metadata key", NULL,
                       NULL, NULL);
    return NULL;
  }
  length = strlen(metadata_key);
  if (length <= strlen(".meta") ||
      strcmp(metadata_key + length - strlen(".meta"), ".meta") != 0) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue metadata key is invalid", NULL, NULL, NULL);
    return NULL;
  }
  stem_length = length - strlen(".meta");
  key =
      (char *)lc_alloc_with_allocator(NULL, stem_length + strlen(".bin") + 1U);
  if (key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue payload key", NULL, NULL,
                       NULL);
    return NULL;
  }
  memcpy(key, metadata_key, stem_length);
  memcpy(key + stem_length, ".bin", strlen(".bin") + 1U);
  return key;
}

static char *lc_pouch_queue_dlq_meta_key(const char *queue,
                                         const char *message_id,
                                         lc_error *error) {
  char *queue_name;
  char *key;
  size_t length;

  queue_name = lc_pouch_queue_normalize_name(queue, error);
  if (queue_name == NULL) {
    return NULL;
  }
  if (message_id == NULL || message_id[0] == '\0' ||
      strchr(message_id, '/') != NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message id is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  length =
      strlen("q//dlq/msg/.meta") + strlen(queue_name) + strlen(message_id) + 1U;
  key = (char *)lc_alloc_with_allocator(NULL, length);
  if (key == NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue DLQ metadata key", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(key, length, "q/%s/dlq/msg/%s.meta", queue_name, message_id);
  lc_free_with_allocator(NULL, queue_name);
  return key;
}

static char *lc_pouch_queue_dlq_state_key(const char *queue,
                                          const char *message_id,
                                          lc_error *error) {
  char *queue_name;
  char *key;
  size_t length;

  queue_name = lc_pouch_queue_normalize_name(queue, error);
  if (queue_name == NULL) {
    return NULL;
  }
  if (message_id == NULL || message_id[0] == '\0' ||
      strchr(message_id, '/') != NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message id is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  length = strlen("q//dlq/state/.json") + strlen(queue_name) +
           strlen(message_id) + 1U;
  key = (char *)lc_alloc_with_allocator(NULL, length);
  if (key == NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue DLQ state key", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(key, length, "q/%s/dlq/state/%s.json", queue_name, message_id);
  lc_free_with_allocator(NULL, queue_name);
  return key;
}

static char *
lc_pouch_queue_message_lease_key_from_meta(const char *metadata_key,
                                           lc_error *error) {
  char *key;
  size_t length;
  size_t stem_length;

  if (metadata_key == NULL || metadata_key[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue lease key requires metadata key", NULL,
                       NULL, "pouch");
    return NULL;
  }
  length = strlen(metadata_key);
  if (length <= strlen(".meta") ||
      strcmp(metadata_key + length - strlen(".meta"), ".meta") != 0) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue metadata key is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  stem_length = length - strlen(".meta");
  key = (char *)lc_alloc_with_allocator(NULL, stem_length + 1U);
  if (key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue lease key", NULL, NULL,
                       "pouch");
    return NULL;
  }
  memcpy(key, metadata_key, stem_length);
  key[stem_length] = '\0';
  return key;
}

static char *
lc_pouch_queue_message_meta_key_from_lease_key(const char *lease_key,
                                               lc_error *error) {
  char *key;
  size_t length;

  if (!lc_pouch_queue_is_message_lease_key(lease_key)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message lease key is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  length = strlen(lease_key);
  if (length > ((size_t)-1) - strlen(".meta") - 1U) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message metadata key is too large", NULL,
                       NULL, "pouch");
    return NULL;
  }
  key = (char *)lc_alloc_with_allocator(NULL, length + strlen(".meta") + 1U);
  if (key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue metadata key", NULL,
                       NULL, "pouch");
    return NULL;
  }
  memcpy(key, lease_key, length);
  memcpy(key + length, ".meta", strlen(".meta") + 1U);
  return key;
}

static int lc_pouch_queue_has_exact_lease_key_shape(const char *key,
                                                    const char *kind) {
  const char *cursor;
  const char *sep;
  size_t kind_length;

  if (key == NULL || kind == NULL ||
      strncmp(key, "q/", sizeof("q/") - 1U) != 0 ||
      lc_pouch_key_has_suffix(key, ".meta") ||
      lc_pouch_key_has_suffix(key, ".bin") ||
      lc_pouch_key_has_suffix(key, ".json")) {
    return 0;
  }
  cursor = key + (sizeof("q/") - 1U);
  sep = strchr(cursor, '/');
  if (sep == NULL || sep == cursor) {
    return 0;
  }
  cursor = sep + 1;
  kind_length = strlen(kind);
  if (strncmp(cursor, kind, kind_length) != 0 || cursor[kind_length] != '/') {
    return 0;
  }
  cursor += kind_length + 1U;
  return cursor[0] != '\0' && strchr(cursor, '/') == NULL;
}

static int lc_pouch_queue_is_message_lease_key(const char *key) {
  return lc_pouch_queue_has_exact_lease_key_shape(key, "msg");
}

static char *lc_pouch_queue_state_lease_key(const char *queue,
                                            const char *message_id,
                                            lc_error *error) {
  char *key;
  char *queue_name;
  size_t queue_length;
  size_t message_id_length;
  size_t total_length;

  if (queue == NULL || queue[0] == '\0' || message_id == NULL ||
      message_id[0] == '\0') {
    (void)lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch queue state lease key requires queue and message id", NULL, NULL,
        NULL);
    return NULL;
  }
  queue_name = lc_pouch_queue_normalize_name(queue, error);
  if (queue_name == NULL) {
    return NULL;
  }
  queue_length = strlen(queue_name);
  message_id_length = strlen(message_id);
  if (strchr(message_id, '/') != NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state lease key component is invalid", NULL,
                       NULL, "pouch");
    return NULL;
  }
  if (queue_length >
      ((size_t)-1) - message_id_length - (sizeof("q//state/") - 1U) - 1U) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state lease key is too large", NULL, NULL,
                       NULL);
    return NULL;
  }
  total_length = (sizeof("q/") - 1U) + queue_length + (sizeof("/state/") - 1U) +
                 message_id_length + 1U;
  key = (char *)lc_alloc_with_allocator(NULL, total_length);
  if (key == NULL) {
    lc_free_with_allocator(NULL, queue_name);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue state lease key", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(key, total_length, "q/%s/state/%s", queue_name, message_id);
  lc_free_with_allocator(NULL, queue_name);
  return key;
}

static char *
lc_pouch_queue_state_object_key_from_lease_key(const char *state_lease_key,
                                               lc_error *error) {
  char *key;
  size_t length;

  if (state_lease_key == NULL || state_lease_key[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state object key requires lease key", NULL,
                       NULL, "pouch");
    return NULL;
  }
  length = strlen(state_lease_key);
  if (length > ((size_t)-1) - strlen(".json") - 1U) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state object key is too large", NULL, NULL,
                       "pouch");
    return NULL;
  }
  key = (char *)lc_alloc_with_allocator(NULL, length + strlen(".json") + 1U);
  if (key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue state object key", NULL,
                       NULL, "pouch");
    return NULL;
  }
  memcpy(key, state_lease_key, length);
  memcpy(key + length, ".json", strlen(".json") + 1U);
  return key;
}

static char *lc_pouch_queue_message_id(lc_pouch_unix_seconds *seconds_out,
                                       long *nanos_out, uint64_t *sequence_out,
                                       lc_error *error) {
  static uint64_t counter;
  struct timespec now;
  char text[128];
  char seconds_text[32];
  char sequence_text[32];
  uint64_t candidate;
  uint64_t sequence;

  if (seconds_out == NULL || nanos_out == NULL || sequence_out == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue message id requires timestamp outputs",
                       NULL, NULL, NULL);
    return NULL;
  }
  if (lc_pouch_queue_clock_gettime(CLOCK_REALTIME, &now) != 0) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to read pouch queue clock", strerror(errno),
                       NULL, NULL);
    return NULL;
  }
  if (now.tv_sec < 0 ||
      (uintmax_t)now.tv_sec >
          ((uintmax_t)LC_U64_MAX - (uintmax_t)now.tv_nsec) / 1000000000U) {
    candidate = counter < LC_U64_MAX ? counter + 1U : LC_U64_MAX;
  } else {
    candidate = ((uint64_t)now.tv_sec * 1000000000U) + (uint64_t)now.tv_nsec;
  }
  pthread_mutex_lock(&lc_pouch_queue_message_id_mutex);
  if (candidate <= counter) {
    if (counter == LC_U64_MAX) {
      pthread_mutex_unlock(&lc_pouch_queue_message_id_mutex);
      (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                         "pouch queue message id sequence overflow", NULL, NULL,
                         NULL);
      return NULL;
    }
    candidate = counter + 1U;
  }
  counter = candidate;
  sequence = counter;
  pthread_mutex_unlock(&lc_pouch_queue_message_id_mutex);
  if (now.tv_sec > 0 && (uintmax_t)now.tv_sec > (uintmax_t)LC_I64_MAX) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue clock exceeds supported timestamp range",
                       NULL, NULL, NULL);
    return NULL;
  }
  *seconds_out = (lc_pouch_unix_seconds)now.tv_sec;
  *nanos_out = (long)now.tv_nsec;
  *sequence_out = sequence;
  if (lc_i64_format_base10_padded((lc_i64)now.tv_sec, 20U, seconds_text,
                                  sizeof(seconds_text)) < 0 ||
      lc_u64_format_base10_padded((lc_u64)sequence, 20U, sequence_text,
                                  sizeof(sequence_text)) < 0 ||
      snprintf(text, sizeof(text), "pouch-msg-%ld-%s-%09ld-%s", (long)getpid(),
               seconds_text, (long)now.tv_nsec, sequence_text) < 0) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "failed to format pouch queue message id", NULL, NULL,
                       NULL);
    return NULL;
  }
  return lc_strdup_with_allocator(NULL, text);
}

static int lc_pouch_queue_retryable_create_collision(lc_error *error) {
  return error != NULL && error->code == LC_ERR_INVALID &&
         error->message != NULL &&
         strstr(error->message, "create-if-absent precondition failed") != NULL;
}

static int lc_pouch_queue_retryable_version_conflict(lc_error *error) {
  return error != NULL && error->code == LC_ERR_INVALID &&
         error->message != NULL &&
         strstr(error->message, "version precondition failed") != NULL;
}

static int lc_pouch_queue_record_header(const lc_pouch_queue_record *record,
                                        int include_payload_length,
                                        char **header_out,
                                        size_t *header_length_out,
                                        lc_error *error) {
  lc_pouch_txn_buffer buffer;
  int rc;

  if (header_out == NULL || header_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue header requires outputs", NULL, NULL,
                        NULL);
  }
  *header_out = NULL;
  *header_length_out = 0U;
  memset(&buffer, 0, sizeof(buffer));
  rc = lc_pouch_txn_buffer_append_bytes(&buffer, LC_POUCH_QUEUE_RECORD_MAGIC,
                                        strlen(LC_POUCH_QUEUE_RECORD_MAGIC),
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, record->ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, record->queue, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, record->message_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, record->status, error);
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_txn_buffer_append_string(&buffer, record->content_type, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, record->lease_id, error);
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_txn_buffer_append_string(&buffer, record->lease_txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->lease_fencing_token, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(&buffer, (int64_t)record->attempts,
                                        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(&buffer, (int64_t)record->max_attempts,
                                        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->failure_attempts, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->enqueued_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->enqueued_at_nsec, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(&buffer, record->enqueue_sequence,
                                        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->expires_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->not_visible_until_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)record->visibility_timeout_seconds, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(&buffer,
                                        include_payload_length
                                            ? (uint64_t)record->payload_length
                                            : ~(uint64_t)0U,
                                        error);
  }
  if (rc == LC_OK) {
    *header_out = (char *)buffer.bytes;
    *header_length_out = buffer.length;
    buffer.bytes = NULL;
    buffer.length = 0U;
    buffer.capacity = 0U;
  }
  lc_pouch_txn_buffer_cleanup(&buffer);
  return rc;
}

static int lc_pouch_queue_record_source(const lc_pouch_queue_record *record,
                                        lc_source **out, lc_error *error) {
  lc_pouch_txn_buffer buffer;
  char *header;
  size_t header_length;
  int rc;

  memset(&buffer, 0, sizeof(buffer));
  header = NULL;
  header_length = 0U;
  rc = lc_pouch_queue_record_header(record, 1, &header, &header_length, error);
  if (rc == LC_OK && record->payload != NULL && record->payload_length > 0U) {
    rc = lc_pouch_txn_buffer_reserve(
        &buffer, header_length + record->payload_length, error);
    if (rc == LC_OK) {
      memcpy(buffer.bytes, header, header_length);
      buffer.length = header_length;
      memcpy(buffer.bytes + buffer.length, record->payload,
             record->payload_length);
      buffer.length += record->payload_length;
    }
  } else if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_reserve(&buffer, header_length, error);
    if (rc == LC_OK && header_length > 0U) {
      memcpy(buffer.bytes, header, header_length);
      buffer.length = header_length;
    }
  }
  if (rc == LC_OK) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, out, error);
  }

  lc_free_with_allocator(NULL, header);
  lc_pouch_txn_buffer_cleanup(&buffer);
  return rc;
}

static void lc_pouch_lease_record_cleanup(lc_pouch_lease_record *record) {
  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, record->ns);
  lc_free_with_allocator(NULL, record->key);
  lc_free_with_allocator(NULL, record->owner);
  lc_free_with_allocator(NULL, record->lease_id);
  lc_free_with_allocator(NULL, record->txn_id);
  memset(record, 0, sizeof(*record));
}

static int lc_pouch_fencing_token_from_i64(lc_i64 value, long *out_value,
                                           const char *field, lc_error *error) {
  if (!lc_i64_to_long_checked(value, out_value)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease fencing_token exceeds public API range",
                        field, NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_next_fencing_token(long prior_token, long *out_value,
                                       lc_error *error) {
  if (out_value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fencing token output is required", NULL, NULL,
                        "pouch");
  }
  if (prior_token >= LONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease fencing token exceeds long range", NULL,
                        NULL, "pouch");
  }
  *out_value = prior_token > 0L ? prior_token + 1L : 1L;
  return LC_OK;
}

static void lc_pouch_generate_lease_id(char *buffer, size_t buffer_size) {
  uint64_t sequence;
  lc_pouch_unix_seconds now_seconds = 0L;
  char seconds_text[32];
  char sequence_text[32];

  pthread_mutex_lock(&lc_pouch_lease_id_mutex);
  sequence = ++lc_pouch_lease_id_counter;
  pthread_mutex_unlock(&lc_pouch_lease_id_mutex);
  now_seconds = 0;
  if (lc_pouch_now_unix(&now_seconds, NULL) != LC_OK) {
    now_seconds = 0;
  }
  if (lc_i64_format_base10(now_seconds, seconds_text, sizeof(seconds_text)) <
          0 ||
      lc_u64_format_base10((lc_u64)sequence, sequence_text,
                           sizeof(sequence_text)) < 0 ||
      snprintf(buffer, buffer_size, "pouch-lease-%s-%s", seconds_text,
               sequence_text) < 0) {
    if (buffer != NULL && buffer_size > 0U) {
      buffer[0] = '\0';
    }
  }
}

static int lc_pouch_lease_record_parse(const unsigned char *bytes,
                                       size_t length,
                                       lc_pouch_generation version,
                                       lc_pouch_lease_record *record,
                                       lc_error *error) {
  lc_pouch_binary_cursor cursor;
  uint64_t unsigned_value;
  int64_t signed_value;
  int rc;

  memset(record, 0, sizeof(*record));
  if (bytes == NULL || length == 0U) {
    return LC_OK;
  }
  signed_value = 0;
  unsigned_value = 0U;
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = bytes;
  cursor.length = length;
  rc =
      lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_LEASE_RECORD_MAGIC, error);
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->owner, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->lease_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_fencing_token_from_i64(
        (lc_i64)signed_value, &record->fencing_token,
        "lease record fencing_token is out of range", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    record->expires_at_unix = (lc_pouch_unix_seconds)signed_value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_u64(&cursor, &unsigned_value, error);
    record->state_version = (lc_pouch_generation)unsigned_value;
  }
  if (rc == LC_OK) {
    if (cursor.offset >= cursor.length || cursor.bytes[cursor.offset] > 1U) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease transaction explicit flag is invalid",
                        NULL, NULL, "pouch");
    } else {
      record->txn_explicit = cursor.bytes[cursor.offset++];
    }
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease record has trailing bytes", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK &&
      (record->ns == NULL || record->key == NULL || record->owner == NULL ||
       record->lease_id == NULL || record->txn_id == NULL)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "failed to parse pouch lease record", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    record->found = 1;
    record->version = version;
  } else {
    lc_pouch_lease_record_cleanup(record);
  }

  return rc;
}

static int lc_pouch_lease_record_build(lc_pouch_txn_buffer *buffer,
                                       const char *ns, const char *key,
                                       const char *owner, const char *lease_id,
                                       const char *txn_id, long fencing_token,
                                       lc_pouch_unix_seconds expires_at_unix,
                                       lc_pouch_generation state_version,
                                       int txn_explicit, lc_error *error) {
  unsigned char explicit_flag;
  int rc;

  explicit_flag = txn_explicit ? 1U : 0U;
  rc = lc_pouch_txn_buffer_append_bytes(buffer, LC_POUCH_LEASE_RECORD_MAGIC,
                                        strlen(LC_POUCH_LEASE_RECORD_MAGIC),
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, owner, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, lease_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(buffer, txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(buffer, (int64_t)fencing_token, error);
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_txn_buffer_append_i64(buffer, (int64_t)expires_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(buffer, (uint64_t)state_version, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_bytes(buffer, &explicit_flag,
                                          sizeof(explicit_flag), error);
  }
  return rc;
}

static int lc_pouch_read_lease_record(lc_client_handle *client, const char *ns,
                                      const char *key,
                                      lc_pouch_lease_record *record,
                                      lc_error *error) {
  lc_pouch_state_read_result read_result;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  rc =
      lc_pouch_state_read_metadata(client->pouch, ns, key, &read_result, error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_record_parse(read_result.metadata,
                                     read_result.metadata_length,
                                     read_result.version, record, error);
  }
  if (rc == LC_OK && record->found) {
    record->has_query_hidden = read_result.has_query_hidden;
    record->query_hidden = read_result.query_hidden;
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_write_lease_record_locked(void *context, lc_error *error) {
  lc_pouch_lease_write_context *ctx;

  ctx = (lc_pouch_lease_write_context *)context;
  return lc_pouch_state_update_metadata_locked(
      ctx->client->pouch, ctx->ns, ctx->key, ctx->options, ctx->out, error);
}

static int lc_pouch_write_lease_record_with_visibility(
    lc_client_handle *client, const char *ns, const char *key,
    const char *owner, const char *lease_id, const char *txn_id,
    long fencing_token, lc_pouch_unix_seconds expires_at_unix,
    lc_pouch_generation state_version, int txn_explicit,
    lc_pouch_generation expected_version, int force_query_hidden,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_txn_buffer buffer;
  lc_pouch_lease_write_context lock_context;
  lc_pouch_state_write_options options;
  int rc;

  memset(&buffer, 0, sizeof(buffer));
  memset(&options, 0, sizeof(options));
  rc = lc_pouch_lease_record_build(&buffer, ns, key, owner, lease_id, txn_id,
                                   fencing_token, expires_at_unix,
                                   state_version, txn_explicit, error);
  if (rc == LC_OK) {
    options.content_type = LC_POUCH_LEASE_CONTENT_TYPE;
    options.suppress_query_index = 1;
    options.has_expected_version = expected_version > 0UL;
    options.expected_version = expected_version;
    options.has_metadata = 1;
    options.metadata = (const unsigned char *)buffer.bytes;
    options.metadata_length = buffer.length;
    if (force_query_hidden) {
      options.has_query_hidden = 1;
      options.query_hidden = 1;
    }
    memset(&lock_context, 0, sizeof(lock_context));
    lock_context.client = client;
    lock_context.ns = ns;
    lock_context.key = key;
    lock_context.options = &options;
    lock_context.out = out;
    rc = lc_pouch_state_with_key_lock(client->pouch, ns, key,
                                      lc_pouch_write_lease_record_locked,
                                      &lock_context, error);
  }
  lc_pouch_txn_buffer_cleanup(&buffer);
  return rc;
}

static int lc_pouch_write_lease_record(
    lc_client_handle *client, const char *ns, const char *key,
    const char *owner, const char *lease_id, const char *txn_id,
    long fencing_token, lc_pouch_unix_seconds expires_at_unix,
    lc_pouch_generation state_version, int txn_explicit,
    lc_pouch_generation expected_version, lc_pouch_state_write_result *out,
    lc_error *error) {
  return lc_pouch_write_lease_record_with_visibility(
      client, ns, key, owner, lease_id, txn_id, fencing_token, expires_at_unix,
      state_version, txn_explicit, expected_version, 0, out, error);
}

static int lc_pouch_write_lease_tombstone(lc_client_handle *client,
                                          const char *ns, const char *key,
                                          const char *owner, long fencing_token,
                                          lc_pouch_generation expected_version,
                                          lc_error *error) {
  lc_pouch_state_write_result result;
  int rc;

  memset(&result, 0, sizeof(result));
  rc =
      lc_pouch_write_lease_record(client, ns, key, owner, "", "", fencing_token,
                                  0L, 0UL, 0, expected_version, &result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_lease_ref_has_credentials(const lc_lease_ref *lease) {
  return lease != NULL && lease->lease_id != NULL && lease->lease_id[0] != '\0';
}

/* Every public mutation is lease-bound. Keep this check at the entry points so
 * an absent lease cannot be interpreted as a request to skip fencing. */
static int lc_pouch_client_require_lease_credentials(const lc_lease_ref *lease,
                                                     lc_error *error) {
  if (lc_pouch_lease_ref_has_credentials(lease)) {
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease-bound mutation requires lease_id", NULL,
                      NULL, NULL);
}

static int lc_pouch_client_is_queue_state_key(const char *key) {
  return lc_pouch_queue_has_exact_lease_key_shape(key, "state");
}

static int lc_pouch_queue_is_state_lease_key(const char *key) {
  return lc_pouch_queue_has_exact_lease_key_shape(key, "state");
}

static int lc_pouch_client_validate_lease_key(const lc_lease_ref *lease,
                                              lc_error *error) {
  if (lease != NULL && lc_pouch_lease_ref_has_credentials(lease) &&
      lc_pouch_client_is_queue_state_key(lease->key)) {
    return LC_OK;
  }
  return lc_pouch_client_validate_public_key(lease != NULL ? lease->key : NULL,
                                             error);
}

/* Validate the lease against the metadata selected by an exact-key mutation.
 * This keeps the decision and the following metadata replacement under one
 * authority in both exclusive and shared-root modes. */
static int lc_pouch_validate_lease_metadata_view(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const lc_pouch_state_metadata_view *state_view,
    lc_pouch_lease_record *record, lc_error *error) {
  lc_pouch_lease_record local_record;
  lc_pouch_lease_record *target;
  const char *lease_txn_id;
  const char *stored_txn_id;
  lc_pouch_unix_seconds now_seconds = 0L;
  int rc;

  if (client == NULL || lease == NULL || ns == NULL || key == NULL ||
      state_view == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease metadata validation requires context",
                        NULL, NULL, "pouch");
  }
  memset(&local_record, 0, sizeof(local_record));
  target = record != NULL ? record : &local_record;
  memset(target, 0, sizeof(*target));
  if (!lc_pouch_lease_ref_has_credentials(lease)) {
    return LC_OK;
  }
  if (lease->fencing_token <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease validation requires fencing_token", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_lease_record_parse(state_view->metadata,
                                   state_view->metadata_length,
                                   state_view->version, target, error);
  if (rc == LC_OK) {
    rc = lc_pouch_now_unix(&now_seconds, error);
  }
  if (rc == LC_OK && (!target->found || strcmp(target->ns, ns) != 0 ||
                      strcmp(target->key, key) != 0 ||
                      strcmp(target->lease_id, lease->lease_id) != 0 ||
                      target->fencing_token != lease->fencing_token ||
                      target->expires_at_unix <= now_seconds)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease validation failed", NULL, NULL, NULL);
  }
  lease_txn_id = lease->txn_id != NULL ? lease->txn_id : "";
  stored_txn_id = target->txn_id != NULL ? target->txn_id : "";
  if (rc == LC_OK && strcmp(stored_txn_id, lease_txn_id) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease validation failed", NULL, NULL, NULL);
  }
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(target);
  } else if (record == NULL) {
    lc_pouch_lease_record_cleanup(&local_record);
  }
  return rc;
}

static int lc_pouch_validate_lease_record(lc_client_handle *client,
                                          const lc_lease_ref *lease,
                                          const char *ns, const char *key,
                                          lc_pouch_lease_record *record,
                                          lc_error *error) {
  lc_pouch_lease_record local_record;
  lc_pouch_lease_record *target;
  lc_pouch_state_metadata_view state_view;
  lc_pouch_state_read_result read_result;
  int rc;

  if (!lc_pouch_lease_ref_has_credentials(lease)) {
    if (record != NULL) {
      memset(record, 0, sizeof(*record));
    }
    return LC_OK;
  }
  if (lease->fencing_token <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease validation requires fencing_token", NULL,
                        NULL, NULL);
  }
  memset(&local_record, 0, sizeof(local_record));
  memset(&read_result, 0, sizeof(read_result));
  target = record != NULL ? record : &local_record;
  memset(target, 0, sizeof(*target));
  rc =
      lc_pouch_state_read_metadata(client->pouch, ns, key, &read_result, error);
  if (rc == LC_OK) {
    memset(&state_view, 0, sizeof(state_view));
    state_view.found = read_result.found;
    state_view.version = read_result.version;
    state_view.metadata = read_result.metadata;
    state_view.metadata_length = read_result.metadata_length;
    state_view.has_query_hidden = read_result.has_query_hidden;
    state_view.query_hidden = read_result.query_hidden;
    rc = lc_pouch_validate_lease_metadata_view(client, lease, ns, key,
                                               &state_view, target, error);
    if (rc == LC_OK && target->found) {
      target->has_query_hidden = read_result.has_query_hidden;
      target->query_hidden = read_result.query_hidden;
    }
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc == LC_OK && record == NULL) {
    lc_pouch_lease_record_cleanup(&local_record);
  }
  return rc;
}

/* Replace one verified lease while the exact target-key mutation authority is
 * held. Queue delivery leases use this instead of a stale read followed by a
 * later metadata write, which is unsafe when shared roots interleave writers.
 */
static int lc_pouch_replace_lease_prepare_metadata(
    const lc_pouch_state_metadata_view *state_view, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_lease_replace_context *ctx;
  lc_pouch_lease_record lease_record;
  const char *lease_id;
  const char *txn_id;
  lc_pouch_unix_seconds expires_at_unix;
  int rc;

  ctx = (lc_pouch_lease_replace_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->lease == NULL ||
      ctx->ns == NULL || ctx->key == NULL || state_view == NULL ||
      options == NULL || apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease replacement requires context", NULL, NULL,
                        "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(options, 0, sizeof(*options));
  *apply = 1;
  rc = lc_pouch_validate_lease_metadata_view(ctx->client, ctx->lease, ctx->ns,
                                             ctx->key, state_view,
                                             &lease_record, error);
  lease_id = ctx->clear_lease ? "" : lease_record.lease_id;
  txn_id = ctx->clear_lease ? "" : lease_record.txn_id;
  expires_at_unix = ctx->clear_lease           ? 0L
                    : ctx->preserve_expiration ? lease_record.expires_at_unix
                                               : ctx->expires_at_unix;
  if (rc == LC_OK) {
    rc = lc_pouch_lease_record_build(
        &ctx->lease_metadata, ctx->ns, ctx->key, lease_record.owner, lease_id,
        txn_id, lease_record.fencing_token, expires_at_unix,
        ctx->has_state_version ? ctx->state_version
                               : lease_record.state_version,
        ctx->clear_lease ? 0 : lease_record.txn_explicit, error);
  }
  if (rc == LC_OK) {
    options->content_type = LC_POUCH_LEASE_CONTENT_TYPE;
    options->suppress_query_index = 1;
    options->has_expected_version = lease_record.version > 0UL;
    options->expected_version = lease_record.version;
    options->has_metadata = 1;
    options->metadata = (const unsigned char *)ctx->lease_metadata.bytes;
    options->metadata_length = ctx->lease_metadata.length;
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_replace_lease_locked(void *context, lc_error *error) {
  lc_pouch_lease_replace_context *ctx;

  ctx = (lc_pouch_lease_replace_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->lease == NULL ||
      ctx->ns == NULL || ctx->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease replacement lock requires context", NULL,
                        NULL, "pouch");
  }
  return lc_pouch_state_update_metadata_prepared_locked(
      ctx->client->pouch, ctx->ns, ctx->key,
      lc_pouch_replace_lease_prepare_metadata, ctx, &ctx->write_result, error);
}

static int lc_pouch_replace_lease_record(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, lc_pouch_unix_seconds expires_at_unix, int clear_lease,
    int preserve_expiration, int has_state_version,
    lc_pouch_generation state_version, lc_pouch_state_write_result *out,
    lc_error *error) {
  lc_pouch_lease_replace_context replace_context;
  int rc;

  if (client == NULL || lease == NULL || ns == NULL || key == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease replacement requires inputs", NULL, NULL,
                        "pouch");
  }
  memset(out, 0, sizeof(*out));
  memset(&replace_context, 0, sizeof(replace_context));
  replace_context.client = client;
  replace_context.lease = lease;
  replace_context.ns = ns;
  replace_context.key = key;
  replace_context.expires_at_unix = expires_at_unix;
  replace_context.clear_lease = clear_lease;
  replace_context.preserve_expiration = preserve_expiration;
  replace_context.has_state_version = has_state_version;
  replace_context.state_version = state_version;
  rc = lc_pouch_state_with_key_lock(client->pouch, ns, key,
                                    lc_pouch_replace_lease_locked,
                                    &replace_context, error);
  if (rc == LC_OK) {
    *out = replace_context.write_result;
    memset(&replace_context.write_result, 0,
           sizeof(replace_context.write_result));
  }
  lc_pouch_txn_buffer_cleanup(&replace_context.lease_metadata);
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &replace_context.write_result);
  return rc;
}

static int lc_pouch_lease_precondition_check(void *context, lc_error *error) {
  lc_pouch_lease_precondition *precondition;

  precondition = (lc_pouch_lease_precondition *)context;
  return lc_pouch_validate_lease_record(precondition->client,
                                        precondition->lease, precondition->ns,
                                        precondition->key, NULL, error);
}

/* Ordinary state mutations already hold the target key authority. Validate the
 * lease against that projection rather than reading the same metadata again. */
static int lc_pouch_lease_view_precondition_check(
    const lc_pouch_state_precondition_view *current, void *context,
    lc_error *error) {
  lc_pouch_lease_precondition *precondition;
  lc_pouch_lease_record lease_record;
  lc_pouch_state_metadata_view state_view;
  int rc;

  precondition = (lc_pouch_lease_precondition *)context;
  if (precondition == NULL || current == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease precondition requires current state", NULL,
                        NULL, "pouch");
  }
  memset(&state_view, 0, sizeof(state_view));
  memset(&lease_record, 0, sizeof(lease_record));
  state_view.found = current->found;
  state_view.version = current->version;
  state_view.metadata = current->metadata;
  state_view.metadata_length = current->metadata_length;
  state_view.has_query_hidden = current->has_query_hidden;
  state_view.query_hidden = current->query_hidden;
  state_view.has_body = current->has_body;
  rc = lc_pouch_validate_lease_metadata_view(
      precondition->client, precondition->lease, precondition->ns,
      precondition->key, &state_view, &lease_record, error);
  if (rc == LC_OK) {
    precondition->lease_expires_at_unix = lease_record.expires_at_unix;
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

/* The state layer supplies all three resident views while namespace mutation
 * authority is held. The client owns only transport-level lease policy. */
static int lc_pouch_client_prepare_txn_stage_write(
    const lc_pouch_state_metadata_view *committed,
    const lc_pouch_state_metadata_view *staged,
    const lc_pouch_state_metadata_view *lease_state, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_txn_stage_write_context *stage;
  const lc_pouch_state_metadata_view *precondition_source;
  lc_pouch_lease_record lease_record;
  lc_pouch_generation base_version;
  int rc;

  stage = (lc_pouch_txn_stage_write_context *)context;
  if (stage == NULL || stage->client == NULL || stage->lease == NULL ||
      stage->ns == NULL || stage->key == NULL || stage->lease->key == NULL ||
      stage->lease->key[0] == '\0' || !lc_pouch_txn_id_present(stage->txn_id) ||
      committed == NULL || staged == NULL || lease_state == NULL ||
      options == NULL || apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction stage preparation requires context",
                        NULL, NULL, "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  *apply = 1;
  rc = lc_pouch_validate_lease_metadata_view(stage->client, stage->lease,
                                             stage->ns, stage->lease->key,
                                             lease_state, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_record_parse(
        lease_state->metadata, lease_state->metadata_length,
        lease_state->version, &lease_record, error);
  }
  if (rc == LC_OK && options->query_index_operation_expires_at_unix != NULL) {
    *options->query_index_operation_expires_at_unix =
        lease_record.expires_at_unix;
  }
  if (rc == LC_OK) {
    base_version = lease_record.state_version;
    if (committed->found && committed->version > base_version) {
      base_version = committed->version;
    }
    if (staged->found && staged->version > base_version) {
      base_version = staged->version;
    }
    if (base_version == LC_U64_MAX) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction state version is exhausted", NULL,
                        NULL, "pouch");
    } else {
      stage->projected_version = base_version + 1UL;
      options->has_logical_version = 1;
      options->logical_version = stage->projected_version;
    }
  }
  if (rc == LC_OK) {
    precondition_source =
        staged->found && !staged->is_delete_marker ? staged : committed;
    if (options->expected_etag != NULL &&
        (!precondition_source->found || precondition_source->etag == NULL ||
         strcmp(precondition_source->etag, options->expected_etag) != 0)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state etag precondition failed", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    precondition_source =
        staged->found && !staged->is_delete_marker ? staged : committed;
    if (options->has_expected_version &&
        (!precondition_source->found ||
         precondition_source->version != options->expected_version)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state version precondition failed", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK && stage->removed != NULL) {
    if (staged->found && staged->is_delete_marker) {
      *stage->removed = 0;
    } else {
      precondition_source = staged->found ? staged : committed;
      *stage->removed =
          precondition_source->found && precondition_source->has_body;
      if (!*stage->removed) {
        /* lockd treats a remove of an absent logical value as a no-op. */
        *apply = 0;
      }
    }
  }
  if (rc == LC_OK && !*apply) {
    /* A no-op remove has no logical state version and must not advance the
     * lease's durable state-version cursor. */
    stage->projected_version = 0UL;
  }
  if (rc == LC_OK && *apply && !options->has_retention &&
      !options->staged_delete_marker) {
    /* A private directive, including explicit clear, survives omitted writes.
     * A staged delete followed by recreation starts a fresh object. */
    const lc_pouch_state_metadata_view *policy =
        staged->found ? staged : committed;
    if (policy->found && policy->has_body && !policy->is_delete_marker) {
      options->has_retention = 1;
      options->retention = policy->retention;
    } else if (staged->found && staged->is_delete_marker) {
      options->has_retention = 1;
      options->retention = 0;
    }
  }
  /* Lease-only metadata must not hide its first staged body write. */
  if (rc == LC_OK && *apply && !options->has_query_hidden &&
      precondition_source->found && precondition_source->has_body &&
      precondition_source->has_query_hidden) {
    options->has_query_hidden = 1;
    options->query_hidden = precondition_source->query_hidden;
  }
  if (rc == LC_OK && *apply && (!staged->found || staged->is_delete_marker)) {
    options->expected_etag = NULL;
    options->has_expected_version = 0;
    options->expected_version = 0UL;
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_client_stage_transaction_write(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const char *txn_id, lc_source *source,
    lc_pouch_state_write_options *options, lc_pouch_state_write_result *out,
    int *removed, lc_error *error) {
  lc_pouch_txn_stage_write_context context;
  lc_pouch_txn_guard guard;
  lc_error discard_error;
  int discard_rc;
  int discarded;
  int rc;

  memset(&context, 0, sizeof(context));
  lc_error_init(&discard_error);
  discard_rc = LC_OK;
  discarded = 0;
  context.client = client;
  context.lease = lease;
  context.ns = ns;
  context.key = key;
  context.txn_id = txn_id;
  context.removed = removed;
  rc = lc_pouch_retention_stage_begin(client, lease, ns, key, txn_id, options,
                                      &guard, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_stage_write_prepared(
        client != NULL ? client->pouch : NULL, ns, key, txn_id,
        lease != NULL ? lease->key : NULL, source, options,
        lc_pouch_client_prepare_txn_stage_write, &context, NULL, out, error);
  if (rc == LC_OK) {
    out->version = context.projected_version;
    if (context.projected_version != 0UL && lease != NULL &&
        lease->key != NULL && strcmp(key, lease->key) == 0) {
      lc_pouch_state_write_result lease_write_result;

      memset(&lease_write_result, 0, sizeof(lease_write_result));
      rc = lc_pouch_replace_lease_record(client, lease, ns, lease->key, 0L, 0,
                                         1, 1, context.projected_version,
                                         &lease_write_result, error);
      lc_pouch_state_write_result_cleanup(&client->allocator,
                                          &lease_write_result);
      if (rc != LC_OK) {
        lc_pouch_state_write_result rollback_lease_write_result;

        /* The private staged record is not usable until the active lease
         * durably advertises its projected version. Do not return an error
         * while retaining an unacknowledged stage: a caller retry would then
         * precondition against the old lease view and fail permanently. */
        discard_rc = lc_pouch_state_discard_staged(
            client->pouch, ns, key, txn_id, &discarded, &discard_error);
        memset(&rollback_lease_write_result, 0,
               sizeof(rollback_lease_write_result));
        if (discard_rc == LC_OK && discarded) {
          if (context.projected_version > LC_U64_MAX - 2UL) {
            discard_rc =
                lc_error_set(&discard_error, LC_ERR_INVALID, 0L,
                             "pouch staged rollback state version is exhausted",
                             NULL, NULL, "pouch");
          } else {
            /* Discard appends a decision and a tombstone after the staged
             * record. Advance the durable lease cursor past both records so
             * a subsequent retry cannot be hidden by that tombstone. */
            discard_rc = lc_pouch_replace_lease_record(
                client, lease, ns, lease->key, 0L, 0, 1, 1,
                context.projected_version + 2UL, &rollback_lease_write_result,
                &discard_error);
          }
        }
        lc_pouch_state_write_result_cleanup(&client->allocator,
                                            &rollback_lease_write_result);
        if (out->query_index_operation_guard_started) {
          lc_pouch_query_index_operation_cancel(client->pouch, ns, key, txn_id);
          out->query_index_operation_guard_started = 0;
        }
        if (discard_rc != LC_OK && error != NULL) {
          lc_error_cleanup(error);
          *error = discard_error;
          lc_error_init(&discard_error);
          rc = discard_rc;
        }
      }
    }
  }
  lc_pouch_txn_guard_release(&guard);
  lc_error_cleanup(&discard_error);
  return rc;
}

static int lc_pouch_client_stage_transaction_metadata(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const char *txn_id, lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_txn_stage_write_context context;
  lc_pouch_txn_guard guard;
  int rc;

  memset(&context, 0, sizeof(context));
  context.client = client;
  context.lease = lease;
  context.ns = ns;
  context.key = key;
  context.txn_id = txn_id;
  rc = lc_pouch_retention_stage_begin(client, lease, ns, key, txn_id, options,
                                      &guard, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_stage_metadata_prepared(
        client != NULL ? client->pouch : NULL, ns, key, txn_id,
        lease != NULL ? lease->key : NULL, options,
        lc_pouch_client_prepare_txn_stage_write, &context, out, error);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}

static int lc_pouch_client_prepare_txn_mutation_stage_write(
    const lc_pouch_state_metadata_view *committed,
    const lc_pouch_state_metadata_view *staged,
    const lc_pouch_state_metadata_view *lease_state, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_txn_mutation_context *mutation;

  mutation = (lc_pouch_txn_mutation_context *)context;
  if (mutation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction mutation requires context", NULL,
                        NULL, "pouch");
  }
  return lc_pouch_client_prepare_txn_stage_write(
      committed, staged, lease_state, &mutation->stage, options, apply, error);
}

static int lc_pouch_client_prepare_txn_mutation_source(
    const lc_pouch_state_read_result *committed,
    const lc_pouch_state_read_result *staged, void *context, lc_source **out,
    lc_error *error) {
  lc_pouch_txn_mutation_context *mutation;
  const lc_pouch_state_read_result *current;

  mutation = (lc_pouch_txn_mutation_context *)context;
  if (mutation == NULL || committed == NULL || staged == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction mutation requires state context",
                        NULL, NULL, "pouch");
  }
  current = staged->found && !lc_pouch_state_result_is_delete_marker(staged)
                ? staged
                : committed;
  return lc_pouch_client_prepare_mutation_source(current, &mutation->mutation,
                                                 out, error);
}

static int lc_pouch_client_stage_transaction_mutation(
    lc_pouch_txn_mutation_context *mutation,
    lc_pouch_state_write_options *options, lc_pouch_state_write_result *out,
    lc_error *error) {
  lc_pouch_txn_guard guard;
  int rc;
  if (mutation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction mutation requires context", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_retention_stage_begin(
      mutation->stage.client, mutation->stage.lease, mutation->stage.ns,
      mutation->stage.key, mutation->stage.txn_id, options, &guard, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_stage_write_prepared(
        mutation->stage.client != NULL ? mutation->stage.client->pouch : NULL,
        mutation->stage.ns, mutation->stage.key, mutation->stage.txn_id,
        mutation->stage.lease != NULL ? mutation->stage.lease->key : NULL, NULL,
        options, lc_pouch_client_prepare_txn_mutation_stage_write, mutation,
        lc_pouch_client_prepare_txn_mutation_source, out, error);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}

static int lc_pouch_client_stage_transaction_remove(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const char *txn_id, lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, int *removed, lc_error *error) {
  lc_source *source;
  int rc;

  source = NULL;
  options->staged_delete_marker = 1;
  options->has_query_hidden = 1;
  options->query_hidden = 1;
  rc = lc_source_from_memory("", 0U, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_client_stage_transaction_write(
        client, lease, ns, key, txn_id, source, options, out, removed, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  return rc;
}

/* Preserve lockd's transport-visible failure for a required lease owner. */
static int lc_pouch_client_validate_owner(const char *owner, lc_error *error) {
  if (owner == NULL || owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L, "owner is required", NULL,
                        "missing_owner", NULL);
  }
  return LC_OK;
}

/* Queue deliveries acquire a normal lease before their transaction participant
 * is staged. Keep the identifier contract aligned with that lease and the
 * terminal transaction APIs so a delivery can always be finalized. */
static int lc_pouch_client_validate_optional_txn_id(const char *txn_id,
                                                    lc_error *error) {
  if (txn_id != NULL && txn_id[0] != '\0' && !lc_xid_is_valid(txn_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_id must be a valid xid", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_queue_lease_acquire_prepare_metadata(
    const lc_pouch_state_metadata_view *state_view, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_queue_lease_acquire_context *ctx;
  lc_pouch_lease_record lease_record;
  const char *txn_id;
  int rc;

  ctx = (lc_pouch_queue_lease_acquire_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || ctx->lease_key == NULL || ctx->lease_id == NULL ||
      state_view == NULL || options == NULL || apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue lease acquisition requires context", NULL,
                        NULL, "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(options, 0, sizeof(*options));
  ctx->write_prepared = 0;
  *apply = 1;
  rc = lc_pouch_lease_record_parse(state_view->metadata,
                                   state_view->metadata_length,
                                   state_view->version, &lease_record, error);
  if (rc == LC_OK && lease_record.found &&
      lease_record.expires_at_unix > ctx->now_seconds) {
    *apply = 0;
  }
  if (rc == LC_OK && *apply) {
    rc = lc_pouch_next_fencing_token(
        lease_record.found ? lease_record.fencing_token : 0L,
        &ctx->fencing_token, error);
  }
  if (rc == LC_OK && *apply) {
    txn_id = ctx->req->txn_id != NULL ? ctx->req->txn_id : "";
    rc = lc_pouch_lease_record_build(
        &ctx->lease_metadata, ctx->ns, ctx->lease_key, ctx->req->owner,
        ctx->lease_id, txn_id, ctx->fencing_token, ctx->lease_expires_at_unix,
        0UL, lc_pouch_txn_id_present(txn_id), error);
    if (rc == LC_OK) {
      options->content_type = LC_POUCH_LEASE_CONTENT_TYPE;
      options->has_expected_version = lease_record.version > 0UL;
      options->expected_version = lease_record.version;
      options->has_metadata = 1;
      options->metadata = (const unsigned char *)ctx->lease_metadata.bytes;
      options->metadata_length = ctx->lease_metadata.length;
      ctx->write_prepared = 1;
    }
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_queue_lease_acquire_locked(void *context, lc_error *error) {
  lc_pouch_queue_lease_acquire_context *ctx;
  int rc;

  ctx = (lc_pouch_queue_lease_acquire_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->ns == NULL ||
      ctx->lease_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue lease lock requires context", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_update_metadata_prepared_locked(
      ctx->client->pouch, ctx->ns, ctx->lease_key,
      lc_pouch_queue_lease_acquire_prepare_metadata, ctx, &ctx->write_result,
      error);
  if (rc == LC_OK && ctx->write_prepared) {
    ctx->acquired = 1;
  }
  return rc;
}

static int lc_pouch_queue_acquire_message_lease(
    lc_client_handle *client, const char *ns, const lc_dequeue_req *req,
    const char *message_lease_key, const char *lease_id,
    lc_pouch_unix_seconds lease_expires_at_unix,
    lc_pouch_unix_seconds now_seconds, long *fencing_token_out,
    int *acquired_out, lc_error *error) {
  lc_pouch_queue_lease_acquire_context acquire_context;
  int rc;

  if (fencing_token_out != NULL) {
    *fencing_token_out = 0L;
  }
  if (acquired_out != NULL) {
    *acquired_out = 0;
  }
  if (client == NULL || ns == NULL || req == NULL ||
      message_lease_key == NULL || lease_id == NULL || lease_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message lease acquire requires inputs",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_client_validate_owner(req->owner, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_validate_optional_txn_id(req->txn_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&acquire_context, 0, sizeof(acquire_context));
  acquire_context.client = client;
  acquire_context.req = req;
  acquire_context.ns = ns;
  acquire_context.lease_key = message_lease_key;
  acquire_context.lease_id = lease_id;
  acquire_context.lease_expires_at_unix = lease_expires_at_unix;
  acquire_context.now_seconds = now_seconds;
  rc = lc_pouch_state_with_key_lock(client->pouch, ns, message_lease_key,
                                    lc_pouch_queue_lease_acquire_locked,
                                    &acquire_context, error);
  if (rc == LC_OK && acquire_context.acquired) {
    if (fencing_token_out != NULL) {
      *fencing_token_out = acquire_context.fencing_token;
    }
    if (acquired_out != NULL) {
      *acquired_out = 1;
    }
  }
  lc_pouch_txn_buffer_cleanup(&acquire_context.lease_metadata);
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &acquire_context.write_result);
  return rc;
}

static char *
lc_pouch_queue_message_lease_key_from_ref(const lc_message_ref *message,
                                          lc_error *error) {
  char *metadata_key;
  char *lease_key;

  metadata_key = lc_pouch_queue_key(message->ns, message->queue,
                                    message->message_id, error);
  if (metadata_key == NULL) {
    return NULL;
  }
  lease_key = lc_pouch_queue_message_lease_key_from_meta(metadata_key, error);
  lc_free_with_allocator(NULL, metadata_key);
  return lease_key;
}

static int lc_pouch_queue_clear_message_lease(lc_client_handle *client,
                                              const lc_message_ref *message,
                                              lc_error *error) {
  lc_lease_ref lease_ref;
  lc_pouch_state_write_result write_result;
  char *message_lease_key;
  int rc;

  if (message == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message lease clear requires message",
                        NULL, NULL, "pouch");
  }
  message_lease_key = lc_pouch_queue_message_lease_key_from_ref(message, error);
  if (message_lease_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&lease_ref, 0, sizeof(lease_ref));
  memset(&write_result, 0, sizeof(write_result));
  lease_ref.ns = message->ns;
  lease_ref.key = message_lease_key;
  lease_ref.lease_id = message->lease_id;
  lease_ref.txn_id = message->txn_id;
  lease_ref.fencing_token = message->fencing_token;
  rc = lc_pouch_replace_lease_record(client, &lease_ref, message->ns,
                                     message_lease_key, 0L, 1, 0, 0, 0UL,
                                     &write_result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  lc_free_with_allocator(NULL, message_lease_key);
  return rc;
}

static int lc_pouch_queue_extend_message_lease(
    lc_client_handle *client, const lc_message_ref *message,
    lc_pouch_unix_seconds lease_expires_at_unix, lc_error *error) {
  lc_lease_ref lease_ref;
  lc_pouch_state_write_result write_result;
  char *message_lease_key;
  int rc;

  message_lease_key = lc_pouch_queue_message_lease_key_from_ref(message, error);
  if (message_lease_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&lease_ref, 0, sizeof(lease_ref));
  memset(&write_result, 0, sizeof(write_result));
  lease_ref.ns = message->ns;
  lease_ref.key = message_lease_key;
  lease_ref.lease_id = message->lease_id;
  lease_ref.txn_id = message->txn_id;
  lease_ref.fencing_token = message->fencing_token;
  rc = lc_pouch_replace_lease_record(client, &lease_ref, message->ns,
                                     message_lease_key, lease_expires_at_unix,
                                     0, 0, 0, 0UL, &write_result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  lc_free_with_allocator(NULL, message_lease_key);
  return rc;
}

static int lc_pouch_queue_source_u16(lc_source *source, unsigned long *out,
                                     lc_error *error) {
  unsigned char bytes[2];
  int rc;

  rc = lc_pouch_source_read_exact(source, bytes, sizeof(bytes), error);
  if (rc == LC_OK) {
    *out = (unsigned long)bytes[0] | ((unsigned long)bytes[1] << 8U);
  }
  return rc;
}

static int lc_pouch_queue_source_u64(lc_source *source, uint64_t *out,
                                     lc_error *error) {
  unsigned char bytes[8];
  uint64_t value;
  size_t i;
  int rc;

  rc = lc_pouch_source_read_exact(source, bytes, sizeof(bytes), error);
  if (rc != LC_OK) {
    return rc;
  }
  value = 0U;
  for (i = 0U; i < sizeof(bytes); ++i) {
    value |= ((uint64_t)bytes[i]) << (i * 8U);
  }
  *out = value;
  return LC_OK;
}

static int lc_pouch_queue_source_i64(lc_source *source, int64_t *out,
                                     lc_error *error) {
  uint64_t value;
  int rc;

  rc = lc_pouch_queue_source_u64(source, &value, error);
  if (rc == LC_OK) {
    *out = (int64_t)value;
  }
  return rc;
}

static int lc_pouch_queue_source_string(lc_source *source, char **out,
                                        size_t *header_length,
                                        lc_error *error) {
  unsigned long length;
  char *copy;
  int rc;

  *out = NULL;
  rc = lc_pouch_queue_source_u16(source, &length, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (length > LC_POUCH_CONTROL_STRING_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue binary string exceeds limit", NULL, NULL,
                        "pouch");
  }
  if (*header_length > LC_POUCH_CONTROL_HEADER_MAX - 2U - (size_t)length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue binary header exceeds limit", NULL, NULL,
                        "pouch");
  }
  *header_length += 2U + (size_t)length;
  copy = (char *)lc_alloc_with_allocator(NULL, (size_t)length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue field", NULL, NULL,
                        "pouch");
  }
  if (length > 0UL) {
    rc = lc_pouch_source_read_exact(source, copy, (size_t)length, error);
  }
  if (rc == LC_OK) {
    copy[length] = '\0';
    *out = copy;
    return LC_OK;
  }
  lc_free_with_allocator(NULL, copy);
  return rc;
}

static int lc_pouch_queue_record_parse(
    lc_client_handle *client, const lc_pouch_state_read_result *read_result,
    const char *storage_key, lc_pouch_queue_record *record, lc_error *error) {
  unsigned char magic[4];
  size_t header_length;
  uint64_t available_payload_bytes;
  uint64_t payload_bytes;
  int64_t signed_value;
  int rc;

  memset(record, 0, sizeof(*record));
  header_length = sizeof(magic);
  available_payload_bytes = 0U;
  payload_bytes = ~(uint64_t)0U;
  signed_value = 0;
  rc = lc_pouch_source_read_exact(read_result->body, magic, sizeof(magic),
                                  error);
  if (rc == LC_OK &&
      memcmp(magic, LC_POUCH_QUEUE_RECORD_MAGIC, sizeof(magic)) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record is corrupt", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    if (read_result->bytes > (uint64_t)((size_t)-1) ||
        header_length > (size_t)read_result->bytes) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record is too large", NULL, NULL, NULL);
    } else {
      available_payload_bytes = read_result->bytes - (uint64_t)header_length;
    }
  }
  if (rc == LC_OK) {
    record->storage_key = lc_strdup_with_allocator(NULL, storage_key);
    record->meta_etag = lc_strdup_with_allocator(NULL, read_result->etag);
    record->version = read_result->version;
    if (record->storage_key == NULL || record->meta_etag == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record is missing fields", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->ns,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->queue,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->message_id,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->status,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->content_type,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->lease_id,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_string(read_result->body, &record->lease_txn_id,
                                      &header_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_fencing_token_from_i64(
        (lc_i64)signed_value, &record->lease_fencing_token,
        "queue record lease_fencing_token is out of range", error);
  }
  if (rc == LC_OK &&
      (record->ns == NULL || record->queue == NULL ||
       record->message_id == NULL || record->status == NULL ||
       record->content_type == NULL || record->lease_id == NULL ||
       record->lease_txn_id == NULL)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record is missing fields", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK &&
      !lc_i64_to_int_checked((lc_i64)signed_value, &record->attempts)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record attempts is out of range", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK &&
      !lc_i64_to_int_checked((lc_i64)signed_value, &record->max_attempts)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record max_attempts is out of range", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK &&
      !lc_i64_to_int_checked((lc_i64)signed_value, &record->failure_attempts)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record failure_attempts is out of range",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    record->enqueued_at_unix = (lc_pouch_unix_seconds)signed_value;
    header_length += 8U;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK && !lc_i64_to_long_checked((lc_i64)signed_value,
                                             &record->enqueued_at_nsec)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record enqueued_at_nsec is out of range",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_u64(read_result->body, &payload_bytes, error);
    record->enqueue_sequence = payload_bytes;
    header_length += 8U;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    record->expires_at_unix = (lc_pouch_unix_seconds)signed_value;
    header_length += 8U;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    record->not_visible_until_unix = (lc_pouch_unix_seconds)signed_value;
    header_length += 8U;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_i64(read_result->body, &signed_value, error);
    header_length += 8U;
  }
  if (rc == LC_OK &&
      !lc_i64_to_long_checked((lc_i64)signed_value,
                              &record->visibility_timeout_seconds)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record visibility timeout is out of range",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_u64(read_result->body, &payload_bytes, error);
    header_length += 8U;
  }
  if (rc == LC_OK) {
    if (read_result->bytes < (uint64_t)header_length) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record is too large", NULL, NULL, NULL);
    } else {
      available_payload_bytes = read_result->bytes - (uint64_t)header_length;
    }
  }
  if (rc == LC_OK && payload_bytes == ~(uint64_t)0U) {
    payload_bytes = (uint64_t)available_payload_bytes;
  }
  if (rc == LC_OK && available_payload_bytes > 0U &&
      payload_bytes > available_payload_bytes) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue payload is truncated", NULL, NULL, NULL);
  }
  if (rc == LC_OK && payload_bytes > (uint64_t)((size_t)-1)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue payload is too large", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    record->payload_length = (size_t)payload_bytes;
  }
  if (rc != LC_OK) {
    lc_pouch_queue_record_cleanup(record);
  }
  (void)client;
  return rc;
}

static int lc_pouch_queue_scan_append(lc_pouch_queue_scan *scan,
                                      lc_pouch_queue_record *record,
                                      lc_error *error) {
  lc_pouch_queue_record *next;
  size_t capacity;

  if (scan->count == scan->capacity) {
    capacity = scan->capacity == 0U ? 8U : scan->capacity * 2U;
    next = (lc_pouch_queue_record *)lc_realloc_with_allocator(
        NULL, scan->records, capacity * sizeof(scan->records[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue scan", NULL, NULL,
                          NULL);
    }
    memset(next + scan->capacity, 0,
           (capacity - scan->capacity) * sizeof(scan->records[0]));
    scan->records = next;
    scan->capacity = capacity;
  }
  scan->records[scan->count] = *record;
  memset(record, 0, sizeof(*record));
  ++scan->count;
  return LC_OK;
}

static int lc_pouch_queue_visit(const lc_pouch_state_visit_entry *entry,
                                void *context, lc_error *error) {
  lc_pouch_queue_scan *scan;
  lc_pouch_state_read_result read_result;
  lc_pouch_queue_record record;
  int rc;

  scan = (lc_pouch_queue_scan *)context;
  if (strncmp(entry->key, scan->prefix, scan->prefix_len) != 0 ||
      !lc_pouch_key_has_suffix(entry->key, ".meta") ||
      lc_pouch_storage_key_has_staging_suffix(entry->key)) {
    return LC_OK;
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_state_read(scan->client->pouch, scan->ns, entry->key,
                           &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_queue_record_parse(scan->client, &read_result, entry->key,
                                     &record, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_queue_scan_append(scan, &record, error);
  }
  lc_pouch_queue_record_cleanup(&record);
  lc_pouch_state_read_result_cleanup(&scan->client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_record_compare(const void *left, const void *right) {
  const lc_pouch_queue_record *a;
  const lc_pouch_queue_record *b;

  a = (const lc_pouch_queue_record *)left;
  b = (const lc_pouch_queue_record *)right;
  if (a->enqueue_sequence > 0U && b->enqueue_sequence > 0U &&
      a->enqueue_sequence < b->enqueue_sequence) {
    return -1;
  }
  if (a->enqueue_sequence > 0U && b->enqueue_sequence > 0U &&
      a->enqueue_sequence > b->enqueue_sequence) {
    return 1;
  }
  if (a->enqueued_at_unix < b->enqueued_at_unix) {
    return -1;
  }
  if (a->enqueued_at_unix > b->enqueued_at_unix) {
    return 1;
  }
  if (a->enqueued_at_nsec < b->enqueued_at_nsec) {
    return -1;
  }
  if (a->enqueued_at_nsec > b->enqueued_at_nsec) {
    return 1;
  }
  return strcmp(a->message_id, b->message_id);
}

static int lc_pouch_queue_scan_load(lc_client_handle *client, const char *ns,
                                    const char *queue,
                                    lc_pouch_queue_scan *scan,
                                    lc_error *error) {
  int rc;

  memset(scan, 0, sizeof(*scan));
  scan->client = client;
  scan->ns = ns;
  scan->prefix = lc_pouch_queue_prefix(ns, queue, error);
  if (scan->prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  scan->prefix_len = strlen(scan->prefix);
  rc = lc_pouch_state_visit_prefix(client->pouch, ns, scan->prefix,
                                   lc_pouch_queue_visit, scan, error);
  if (rc == LC_OK && scan->count > 1U) {
    qsort(scan->records, scan->count, sizeof(scan->records[0]),
          lc_pouch_queue_record_compare);
  }
  return rc;
}

static int lc_pouch_queue_record_is_live(const lc_pouch_queue_record *record) {
  if (strcmp(record->status, "acked") == 0) {
    return 0;
  }
  if (strcmp(record->status, "dead") == 0 ||
      strcmp(record->status, "expired") == 0) {
    return 0;
  }
  if (record->max_attempts > 0 &&
      record->failure_attempts >= record->max_attempts) {
    return 0;
  }
  return 1;
}

static int lc_pouch_queue_record_is_live_at(const lc_pouch_queue_record *record,
                                            lc_pouch_unix_seconds now) {
  if (!lc_pouch_queue_record_is_live(record)) {
    return 0;
  }
  if (record->expires_at_unix > 0L && record->expires_at_unix <= now) {
    return 0;
  }
  return 1;
}

static int lc_pouch_queue_record_available(const lc_pouch_queue_record *record,
                                           lc_pouch_unix_seconds now) {
  if (!lc_pouch_queue_record_is_live_at(record, now) ||
      record->not_visible_until_unix > now) {
    return 0;
  }
  return strcmp(record->status, "available") == 0 ||
         strcmp(record->status, "inflight") == 0;
}

static int lc_pouch_queue_wait_deadline(long wait_seconds,
                                        struct timespec *deadline,
                                        int *has_deadline, lc_error *error) {
  if (deadline == NULL || has_deadline == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue wait deadline requires outputs", NULL,
                        NULL, NULL);
  }
  *has_deadline = 0;
  memset(deadline, 0, sizeof(*deadline));
  if (wait_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue wait_seconds must be non-negative", NULL,
                        NULL, NULL);
  }
  if (wait_seconds == 0L) {
    return LC_OK;
  }
  if (lc_pouch_queue_clock_gettime(CLOCK_MONOTONIC, deadline) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch dequeue wait clock",
                        strerror(errno), NULL, NULL);
  }
  if (wait_seconds > LONG_MAX - (long)deadline->tv_sec) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue wait_seconds is too large", NULL, NULL,
                        NULL);
  }
  deadline->tv_sec += (time_t)wait_seconds;
  *has_deadline = 1;
  return LC_OK;
}

static int
lc_pouch_queue_wait_deadline_reached(const struct timespec *deadline) {
  struct timespec now;

  if (deadline == NULL ||
      lc_pouch_queue_clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    return 1;
  }
  return now.tv_sec > deadline->tv_sec ||
         (now.tv_sec == deadline->tv_sec && now.tv_nsec >= deadline->tv_nsec);
}

static void lc_pouch_queue_dequeue_poll_delay(void) {
  struct timespec delay;

  delay.tv_sec = 0;
  delay.tv_nsec = 100L * 1000L * 1000L;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_queue_poll_delay != NULL) {
    lc_pouch_test_queue_poll_delay(&delay, lc_pouch_test_queue_time_context);
    return;
  }
#endif
  (void)nanosleep(&delay, NULL);
}

static void lc_pouch_queue_touch_notification(lc_client_handle *client,
                                              const char *ns,
                                              const char *queue) {
  lc_error ignored;
  char text[256];
  char sequence_text[32];
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  char *notify_path;
  size_t leaf_len;
  uint64_t sequence;

  if (client == NULL || client->pouch == NULL || ns == NULL || ns[0] == '\0' ||
      queue == NULL || queue[0] == '\0') {
    return;
  }
  if (!client->pouch->queue_watch_enabled) {
    return;
  }
  lc_error_init(&ignored);
  namespace_path = NULL;
  notify_dir = NULL;
  escaped_queue = NULL;
  notify_leaf = NULL;
  notify_path = NULL;
  if (lc_pouch_namespace_ensure(&client->allocator, client->pouch->root_path,
                                ns, &ignored) != LC_OK) {
    goto cleanup;
  }
  namespace_path =
      lc_pouch_namespace_path(&client->allocator, client->pouch->root_path, ns);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&client->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&client->allocator, queue);
  if (notify_dir == NULL || escaped_queue == NULL) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&client->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  notify_path = lc_pouch_path_join(&client->allocator, notify_dir, notify_leaf);
  if (notify_path == NULL) {
    goto cleanup;
  }
  sequence = ++client->pouch->marker_sequence;
  if (lc_u64_format_base10_padded((lc_u64)sequence, 20U, sequence_text,
                                  sizeof(sequence_text)) < 0) {
    goto cleanup;
  }
  snprintf(text, sizeof(text), "queue=%s\nsequence=%s\n%s", escaped_queue,
           sequence_text,
           (sequence % (uint64_t)2U) == (uint64_t)0U ? "pad=x\n" : "");
  (void)lc_pouch_path_write_text_file(notify_path, text, NULL);

cleanup:
  lc_free_with_allocator(&client->allocator, notify_path);
  lc_free_with_allocator(&client->allocator, notify_leaf);
  lc_free_with_allocator(&client->allocator, escaped_queue);
  lc_free_with_allocator(&client->allocator, notify_dir);
  lc_free_with_allocator(&client->allocator, namespace_path);
  lc_error_cleanup(&ignored);
}

static int lc_pouch_queue_payload_source(lc_client_handle *client,
                                         const char *ns,
                                         const char *storage_key,
                                         lc_source **out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  char *payload_key;
  int rc;

  if (client == NULL || ns == NULL || storage_key == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch queue payload source requires client, namespace, "
        "key, and out",
        NULL, NULL, NULL);
  }
  *out = NULL;
  payload_key = NULL;
  memset(&read_result, 0, sizeof(read_result));
  payload_key = lc_pouch_queue_payload_key_from_meta(storage_key, error);
  if (payload_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, ns, payload_key, &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue payload source record not found", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    *out = read_result.body;
    read_result.body = NULL;
  }
  lc_free_with_allocator(NULL, payload_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_open_record_source(lc_client_handle *client,
                                             lc_pouch_queue_record *record,
                                             lc_source **payload_source_out,
                                             lc_source **out, lc_error *error) {
  if (payload_source_out == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record source requires outputs", NULL,
                        NULL, NULL);
  }
  (void)client;
  *payload_source_out = NULL;
  return lc_pouch_queue_record_source(record, out, error);
}

static int lc_pouch_queue_write_record(lc_client_handle *client,
                                       lc_pouch_queue_record *record,
                                       lc_error *error) {
  lc_source *source;
  lc_source *payload_source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  payload_source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_queue_open_record_source(client, record, &payload_source,
                                         &source, error);
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    options.expected_version = record->version;
    options.has_expected_version = record->version > 0UL;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(client->pouch, record->ns, record->storage_key,
                              source, &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  if (rc == LC_OK) {
    lc_pouch_queue_touch_notification(client, record->ns, record->queue);
  }
  if (rc == LC_OK) {
    lc_free_with_allocator(NULL, record->meta_etag);
    record->meta_etag = lc_strdup_with_allocator(NULL, result.etag);
    record->version = result.version;
    if (record->meta_etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue meta etag", NULL, NULL,
                        NULL);
    }
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_write_new_record(lc_client_handle *client,
                                           lc_pouch_queue_record *record,
                                           lc_error *error) {
  lc_source *source;
  lc_source *payload_source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  payload_source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_queue_open_record_source(client, record, &payload_source,
                                         &source, error);
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    options.create_if_absent = 1;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(client->pouch, record->ns, record->storage_key,
                              source, &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  if (rc == LC_OK) {
    lc_free_with_allocator(NULL, record->meta_etag);
    record->meta_etag = lc_strdup_with_allocator(NULL, result.etag);
    record->version = result.version;
    if (record->meta_etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue meta etag", NULL, NULL,
                        NULL);
    }
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_copy_object_if_exists(lc_client_handle *client,
                                                const char *ns,
                                                const char *source_key,
                                                const char *destination_key,
                                                lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  rc = lc_pouch_state_read(client->pouch, ns, source_key, &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  if (rc == LC_OK) {
    options.content_type = read_result.content_type != NULL
                               ? read_result.content_type
                               : "application/octet-stream";
    options.metadata = read_result.metadata;
    options.metadata_length = read_result.metadata_length;
    options.has_metadata = read_result.metadata_length > 0U;
    options.create_if_absent = 1;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(client->pouch, ns, destination_key,
                              read_result.body, &options, &write_result, error);
    if (lc_pouch_queue_retryable_create_collision(error)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_delete_object(lc_client_handle *client,
                                        const char *ns, const char *key,
                                        int has_expected_version,
                                        lc_pouch_generation expected_version,
                                        lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.object_record = 1;
  if (has_expected_version) {
    options.has_expected_version = 1;
    options.expected_version = expected_version;
  }
  rc = lc_pouch_state_delete(client->pouch, ns, key, &options, &result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_move_to_dlq(lc_client_handle *client,
                                      lc_pouch_queue_record *record,
                                      lc_error *error) {
  char *original_meta_key;
  char *original_payload_key;
  char *original_state_lease_key;
  char *original_state_key;
  char *dlq_meta_key;
  char *dlq_payload_key;
  char *dlq_state_key;
  char *old_storage_key;
  char *old_meta_etag;
  lc_pouch_generation original_version;
  int rc;

  original_meta_key = NULL;
  original_payload_key = NULL;
  original_state_lease_key = NULL;
  original_state_key = NULL;
  dlq_meta_key = NULL;
  dlq_payload_key = NULL;
  dlq_state_key = NULL;
  old_storage_key = NULL;
  old_meta_etag = NULL;
  original_version = 0UL;
  if (client == NULL || record == NULL || record->ns == NULL ||
      record->queue == NULL || record->message_id == NULL ||
      record->storage_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue DLQ move requires complete record", NULL,
                        NULL, NULL);
  }
  original_meta_key = lc_strdup_with_allocator(NULL, record->storage_key);
  if (original_meta_key == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue DLQ source key", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  original_payload_key =
      lc_pouch_queue_payload_key_from_meta(original_meta_key, error);
  if (original_payload_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  original_state_lease_key =
      lc_pouch_queue_state_lease_key(record->queue, record->message_id, error);
  if (original_state_lease_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  original_state_key = lc_pouch_queue_state_object_key_from_lease_key(
      original_state_lease_key, error);
  if (original_state_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  dlq_meta_key =
      lc_pouch_queue_dlq_meta_key(record->queue, record->message_id, error);
  if (dlq_meta_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  dlq_payload_key = lc_pouch_queue_payload_key_from_meta(dlq_meta_key, error);
  if (dlq_payload_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  dlq_state_key =
      lc_pouch_queue_dlq_state_key(record->queue, record->message_id, error);
  if (dlq_state_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_queue_copy_object_if_exists(
      client, record->ns, original_payload_key, dlq_payload_key, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_copy_object_if_exists(
        client, record->ns, original_state_key, dlq_state_key, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  old_storage_key = record->storage_key;
  old_meta_etag = record->meta_etag;
  original_version = record->version;
  record->storage_key = dlq_meta_key;
  record->meta_etag = lc_strdup_with_allocator(NULL, "");
  record->version = 0UL;
  dlq_meta_key = NULL;
  if (record->meta_etag == NULL) {
    record->storage_key = old_storage_key;
    record->meta_etag = old_meta_etag;
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue DLQ etag", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  lc_free_with_allocator(NULL, old_meta_etag);
  rc = lc_pouch_queue_write_new_record(client, record, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_delete_object(client, record->ns, original_meta_key,
                                      original_version > 0UL, original_version,
                                      error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_delete_object(client, record->ns, original_payload_key,
                                      0, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_delete_object(client, record->ns, original_state_key, 0,
                                      0UL, error);
  }
  lc_free_with_allocator(NULL, old_storage_key);
  if (rc == LC_OK) {
    lc_pouch_queue_touch_notification(client, record->ns, record->queue);
  }

cleanup:
  lc_free_with_allocator(NULL, original_meta_key);
  lc_free_with_allocator(NULL, original_payload_key);
  lc_free_with_allocator(NULL, original_state_lease_key);
  lc_free_with_allocator(NULL, original_state_key);
  lc_free_with_allocator(NULL, dlq_meta_key);
  lc_free_with_allocator(NULL, dlq_payload_key);
  lc_free_with_allocator(NULL, dlq_state_key);
  return rc;
}

static int lc_pouch_queue_delete_state_object_for_ack(
    lc_client_handle *client, const lc_message_ref *message, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  char *state_lease_key;
  char *state_object_key;
  int state_required;
  int rc;

  if (client == NULL || message == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue ack state delete requires client and "
                        "message",
                        NULL, NULL, NULL);
  }
  state_required =
      message->state_lease_id != NULL && message->state_lease_id[0] != '\0';
  if (!state_required &&
      (message->state_etag == NULL || message->state_etag[0] == '\0')) {
    return LC_OK;
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  state_lease_key = NULL;
  state_object_key = NULL;
  state_lease_key = lc_pouch_queue_state_lease_key(message->queue,
                                                   message->message_id, error);
  if (state_lease_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  state_object_key =
      lc_pouch_queue_state_object_key_from_lease_key(state_lease_key, error);
  if (state_object_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, message->ns, state_object_key,
                           &read_result, error);
  if (rc == LC_OK && !read_result.found && message->state_etag != NULL &&
      message->state_etag[0] != '\0') {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue state etag precondition failed", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK && read_result.found) {
    options.object_record = 1;
    if (message->state_etag != NULL && message->state_etag[0] != '\0') {
      options.expected_etag = message->state_etag;
    }
    rc = lc_pouch_state_delete(client->pouch, message->ns, state_object_key,
                               &options, &result, error);
  }

cleanup:
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  lc_free_with_allocator(NULL, state_lease_key);
  lc_free_with_allocator(NULL, state_object_key);
  return rc;
}

static int lc_pouch_queue_delete_message_objects(lc_client_handle *client,
                                                 lc_pouch_queue_record *record,
                                                 lc_error *error) {
  char *metadata_key;
  char *payload_key;
  int rc;

  if (client == NULL || record == NULL || record->ns == NULL ||
      record->storage_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delete requires complete record", NULL,
                        NULL, NULL);
  }
  metadata_key = lc_strdup_with_allocator(NULL, record->storage_key);
  payload_key = NULL;
  if (metadata_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue delete key", NULL, NULL,
                        NULL);
  }
  payload_key = lc_pouch_queue_payload_key_from_meta(metadata_key, error);
  if (payload_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_queue_delete_object(client, record->ns, metadata_key,
                                    record->version > 0UL, record->version,
                                    error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_delete_object(client, record->ns, payload_key, 0, 0UL,
                                      error);
  }

cleanup:
  lc_free_with_allocator(NULL, metadata_key);
  lc_free_with_allocator(NULL, payload_key);
  return rc;
}

static int lc_pouch_queue_stage_record(lc_client_handle *client,
                                       lc_pouch_queue_record *record,
                                       const char *txn_id, lc_error *error) {
  lc_source *source;
  lc_source *payload_source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  payload_source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_queue_open_record_source(client, record, &payload_source,
                                         &source, error);
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_stage_write(client->pouch, record->ns,
                                    record->storage_key, txn_id, source,
                                    &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_write_or_stage_record(lc_client_handle *client,
                                                lc_pouch_queue_record *record,
                                                const char *txn_id,
                                                lc_error *error) {
  if (lc_pouch_txn_id_present(txn_id)) {
    return lc_pouch_queue_stage_record(client, record, txn_id, error);
  }
  return lc_pouch_queue_write_record(client, record, error);
}

static int lc_pouch_queue_replace_string(char **field, const char *value,
                                         lc_error *error) {
  char *copy;

  copy = lc_strdup_with_allocator(NULL, value != NULL ? value : "");
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue record field", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(NULL, *field);
  *field = copy;
  return LC_OK;
}

static int lc_pouch_queue_clear_lease(lc_pouch_queue_record *record,
                                      lc_error *error) {
  int rc;

  rc = lc_pouch_queue_replace_string(&record->lease_id, "", error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_replace_string(&record->lease_txn_id, "", error);
  }
  if (rc == LC_OK) {
    record->lease_fencing_token = 0L;
  }
  return rc;
}

static int
lc_pouch_queue_validate_active_delivery(const lc_message_ref *message,
                                        const lc_pouch_queue_record *record,
                                        lc_error *error) {
  const char *message_txn_id;
  const char *record_txn_id;
  lc_pouch_unix_seconds now_seconds = 0;
  int rc;

  if (message == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delivery validation requires message and "
                        "record",
                        NULL, NULL, NULL);
  }
  if (message->lease_id == NULL || message->lease_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message reference requires lease_id", NULL,
                        NULL, NULL);
  }
  if (message->fencing_token <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message reference requires "
                        "fencing_token",
                        NULL, NULL, NULL);
  }
  if (record->status == NULL || strcmp(record->status, "inflight") != 0 ||
      record->lease_id == NULL || record->lease_id[0] == '\0' ||
      strcmp(record->lease_id, message->lease_id) != 0 ||
      record->lease_fencing_token != message->fencing_token) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delivery lease validation failed", NULL,
                        NULL, NULL);
  }
  message_txn_id = message->txn_id != NULL ? message->txn_id : "";
  record_txn_id = record->lease_txn_id != NULL ? record->lease_txn_id : "";
  if (message_txn_id[0] != '\0' && strcmp(record_txn_id, message_txn_id) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delivery lease validation failed", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_now_unix(&now_seconds, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (record->not_visible_until_unix <= now_seconds) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delivery lease expired", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int
lc_pouch_queue_message_ref_has_state_lease(const lc_message_ref *message) {
  return message != NULL && message->state_lease_id != NULL &&
         message->state_lease_id[0] != '\0';
}

static char *
lc_pouch_queue_state_key_from_message_ref(const lc_message_ref *message,
                                          lc_error *error) {
  if (message == NULL || message->queue == NULL ||
      message->message_id == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state lease reference is incomplete", NULL,
                       NULL, NULL);
    return NULL;
  }
  return lc_pouch_queue_state_lease_key(message->queue, message->message_id,
                                        error);
}

static void
lc_pouch_queue_state_lease_ref_from_message(const lc_message_ref *message,
                                            const char *state_key,
                                            lc_lease_ref *lease_ref) {
  lc_lease_ref_init(lease_ref);
  lease_ref->ns = message->ns;
  lease_ref->key = state_key;
  lease_ref->lease_id = message->state_lease_id;
  lease_ref->txn_id = message->txn_id;
  lease_ref->fencing_token = message->state_fencing_token;
}

static int lc_pouch_queue_validate_state_lease(lc_client_handle *client,
                                               const lc_message_ref *message,
                                               lc_error *error) {
  lc_lease_ref lease_ref;
  char *state_key;
  int rc;

  if (!lc_pouch_queue_message_ref_has_state_lease(message)) {
    return LC_OK;
  }
  state_key = lc_pouch_queue_state_key_from_message_ref(message, error);
  if (state_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_queue_state_lease_ref_from_message(message, state_key, &lease_ref);
  rc = lc_pouch_validate_lease_record(client, &lease_ref, message->ns,
                                      state_key, NULL, error);
  lc_free_with_allocator(NULL, state_key);
  return rc;
}

static int lc_pouch_queue_release_state_lease(lc_client_handle *client,
                                              const lc_message_ref *message,
                                              int rollback, lc_error *error) {
  lc_lease_ref lease_ref;
  lc_pouch_lease_record lease_record;
  char *state_key;
  char *state_object_key;
  int discarded;
  int rc;

  if (!lc_pouch_queue_message_ref_has_state_lease(message)) {
    return LC_OK;
  }
  state_key = lc_pouch_queue_state_key_from_message_ref(message, error);
  if (state_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  state_object_key = NULL;
  memset(&lease_record, 0, sizeof(lease_record));
  discarded = 0;
  lc_pouch_queue_state_lease_ref_from_message(message, state_key, &lease_ref);
  rc = lc_pouch_validate_lease_record(client, &lease_ref, message->ns,
                                      state_key, &lease_record, error);
  if (rc == LC_OK && rollback && message->txn_id != NULL &&
      message->txn_id[0] != '\0') {
    state_object_key =
        lc_pouch_queue_state_object_key_from_lease_key(state_key, error);
    if (state_object_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && rollback && message->txn_id != NULL &&
      message->txn_id[0] != '\0') {
    rc = lc_pouch_state_discard_staged(client->pouch, message->ns,
                                       state_object_key, message->txn_id,
                                       &discarded, error);
  }
  if (rc == LC_OK) {
    lc_pouch_state_write_result write_result;

    memset(&write_result, 0, sizeof(write_result));
    rc = lc_pouch_replace_lease_record(client, &lease_ref, message->ns,
                                       state_key, 0L, 1, 0, 0, 0UL,
                                       &write_result, error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  lc_free_with_allocator(NULL, state_object_key);
  lc_free_with_allocator(NULL, state_key);
  return rc;
}

static int lc_pouch_queue_extend_state_lease(
    lc_client_handle *client, const lc_message_ref *message,
    lc_pouch_unix_seconds lease_expires_at_unix,
    lc_pouch_unix_seconds *state_lease_expires_at_unix, lc_error *error) {
  lc_lease_ref lease_ref;
  lc_pouch_state_write_result write_result;
  char *state_key;
  int rc;

  if (state_lease_expires_at_unix != NULL) {
    *state_lease_expires_at_unix = 0L;
  }
  if (!lc_pouch_queue_message_ref_has_state_lease(message)) {
    return LC_OK;
  }
  state_key = lc_pouch_queue_state_key_from_message_ref(message, error);
  if (state_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&write_result, 0, sizeof(write_result));
  lc_pouch_queue_state_lease_ref_from_message(message, state_key, &lease_ref);
  rc = lc_pouch_replace_lease_record(client, &lease_ref, message->ns, state_key,
                                     lease_expires_at_unix, 0, 0, 0, 0UL,
                                     &write_result, error);
  if (rc == LC_OK && state_lease_expires_at_unix != NULL) {
    *state_lease_expires_at_unix = lease_expires_at_unix;
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  lc_free_with_allocator(NULL, state_key);
  return rc;
}

static int lc_pouch_queue_copy_message_ref(const lc_message_ref *message,
                                           lc_pouch_queue_record *record,
                                           lc_client_handle *client,
                                           lc_error *error) {
  lc_lease_ref lease_ref;
  lc_pouch_state_read_result read_result;
  char *storage_key;
  char *message_lease_key;
  int rc;

  if (message == NULL || message->ns == NULL || message->queue == NULL ||
      message->message_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message reference is incomplete", NULL,
                        NULL, NULL);
  }
  message_lease_key = NULL;
  storage_key = lc_pouch_queue_key(message->ns, message->queue,
                                   message->message_id, error);
  if (storage_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, message->ns, storage_key,
                           &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue message not found", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_record_parse(client, &read_result, storage_key, record,
                                     error);
  }
  if (rc == LC_OK && message->meta_etag != NULL &&
      message->meta_etag[0] != '\0' &&
      strcmp(message->meta_etag, read_result.etag) != 0) {
    lc_pouch_queue_record_cleanup(record);
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue meta etag precondition failed", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_validate_active_delivery(message, record, error);
    if (rc != LC_OK) {
      lc_pouch_queue_record_cleanup(record);
    }
  }
  if (rc == LC_OK) {
    message_lease_key =
        lc_pouch_queue_message_lease_key_from_meta(storage_key, error);
    if (message_lease_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      lc_pouch_queue_record_cleanup(record);
    }
  }
  if (rc == LC_OK) {
    memset(&lease_ref, 0, sizeof(lease_ref));
    lease_ref.ns = message->ns;
    lease_ref.key = message_lease_key;
    lease_ref.lease_id = message->lease_id;
    lease_ref.txn_id = message->txn_id;
    lease_ref.fencing_token = message->fencing_token;
    rc = lc_pouch_validate_lease_record(client, &lease_ref, message->ns,
                                        message_lease_key, NULL, error);
    if (rc != LC_OK) {
      lc_pouch_queue_record_cleanup(record);
    }
  }
  lc_free_with_allocator(NULL, message_lease_key);
  lc_free_with_allocator(NULL, storage_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_make_message(lc_client_handle *client,
                                       const lc_pouch_queue_record *record,
                                       const char *next_cursor,
                                       int *terminal_flag, lc_message **out,
                                       lc_error *error) {
  lc_engine_dequeue_response response;
  lc_source *payload;
  int rc;

  memset(&response, 0, sizeof(response));
  payload = NULL;
  if (record->payload != NULL || record->payload_length == 0U) {
    rc = lc_source_from_memory(record->payload, record->payload_length,
                               &payload, error);
  } else {
    rc = lc_pouch_queue_payload_source(client, record->ns, record->storage_key,
                                       &payload, error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  if (payload->reset != NULL) {
    rc = payload->reset(payload, error);
    if (rc != LC_OK) {
      lc_source_close(payload);
      return rc;
    }
  }
  response.ns = record->ns;
  response.queue = record->queue;
  response.message_id = record->message_id;
  response.attempts = record->attempts;
  response.max_attempts = record->max_attempts;
  response.failure_attempts = record->failure_attempts;
  response.not_visible_until_unix = record->not_visible_until_unix;
  response.visibility_timeout_seconds = record->visibility_timeout_seconds;
  response.payload_content_type = record->content_type;
  response.payload = payload;
  response.payload_length = record->payload_length;
  response.correlation_id = "pouch-queue-dequeue";
  response.lease_id = record->lease_id;
  response.lease_expires_at_unix = record->not_visible_until_unix;
  response.fencing_token = record->lease_fencing_token;
  response.txn_id =
      record->lease_txn_id != NULL && record->lease_txn_id[0] != '\0'
          ? record->lease_txn_id
          : NULL;
  response.meta_etag = record->meta_etag;
  response.next_cursor = (char *)next_cursor;
  *out = lc_message_new(client, &response, payload, terminal_flag);
  if (*out == NULL) {
    lc_source_close(payload);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue message", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int lc_pouch_txn_parse_record(const char *bytes, size_t length,
                                     lc_pouch_txn_record *record,
                                     lc_error *error) {
  lc_pouch_binary_cursor cursor;
  unsigned long expected_participants;
  uint64_t count;
  uint64_t value;
  int64_t signed_value;
  int rc;

  memset(record, 0, sizeof(*record));
  expected_participants = 0UL;
  count = 0U;
  value = 0U;
  signed_value = 0;
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = (const unsigned char *)bytes;
  cursor.length = length;
  rc = lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_TXN_RECORD_MAGIC, error);
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    record->expires_at_unix = (lc_pouch_unix_seconds)signed_value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_u64(&cursor, &value, error);
    record->tc_term = value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, &record->target_backend_hash,
                                       error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_u64(&cursor, &count, error);
    expected_participants = (unsigned long)count;
  }
  while (rc == LC_OK && count > 0U) {
    char *ns;
    char *key;
    char *backend_hash;

    ns = NULL;
    key = NULL;
    backend_hash = NULL;
    rc = lc_pouch_binary_cursor_string(&cursor, &ns, error);
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_string(&cursor, &key, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_string(&cursor, &backend_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_record_add_participant(record, ns, key, backend_hash,
                                               error);
      ns = NULL;
      key = NULL;
      backend_hash = NULL;
    }
    if (rc == LC_OK) {
      /* External prepare intent seals enrollment but is not a YES vote. The
       * preparing marker is valid only for a resource-driver participant.
       */
      if (cursor.offset >= cursor.length || cursor.bytes[cursor.offset] > 3U ||
          (cursor.bytes[cursor.offset] == 3U &&
           !lc_pouch_xa_participant(
               &record->participants[record->participant_count - 1U]))) {
        rc =
            lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                         "pouch transaction vote is invalid", NULL, NULL, NULL);
      } else {
        record->votes[record->participant_count - 1U] =
            cursor.bytes[cursor.offset++];
      }
    }
    lc_free_with_allocator(NULL, ns);
    lc_free_with_allocator(NULL, key);
    lc_free_with_allocator(NULL, backend_hash);
    --count;
  }
  if (rc == LC_OK && record->state == NULL) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction record is missing state", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK &&
      expected_participants != (unsigned long)record->participant_count) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction participant count does not match",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK && cursor.length - cursor.offset == 8U) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    if (rc == LC_OK &&
        (signed_value <= 0 || strcmp(record->state, "commit") != 0))
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch transaction commit timestamp is invalid", NULL,
                        NULL, "pouch");
    record->commit_at_unix = signed_value;
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction record has trailing bytes", NULL, NULL,
                      NULL);
  }
  if (rc != LC_OK) {
    lc_pouch_txn_record_cleanup(record);
  }
  return rc;
}

static char *lc_pouch_txn_key(const char *txn_id, lc_error *error) {
  if (txn_id == NULL || txn_id[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L, "pouch transaction requires txn_id",
                 NULL, NULL, NULL);
    return NULL;
  }
  if (strchr(txn_id, '/') != NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch transaction txn_id must not contain slash", NULL, NULL,
                 NULL);
    return NULL;
  }
  {
    char *key;

    key = lc_strdup_with_allocator(NULL, txn_id);
    if (key == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch transaction key", NULL, NULL,
                   NULL);
    }
    return key;
  }
}

/* The endpoint-minted XID is also the implicit transaction admission key.
 * In shared mode the root durable-mutation guard is acquired first, then this
 * root-local transaction lock coordinates joins and terminal releases. The
 * ordering prevents a transaction guard/root cycle across processes. */
static size_t lc_pouch_txn_guard_stripe(const char *txn_id) {
  uint64_t hash = ((uint64_t)0xcbf29ce4UL << 32U) | (uint64_t)0x84222325UL;
  const unsigned char *cursor;
  for (cursor = (const unsigned char *)txn_id; *cursor != '\0'; ++cursor) {
    hash ^= (uint64_t)*cursor;
    hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
  }
  return (size_t)(hash % LC_POUCH_TXN_GUARD_STRIPES);
}

static int lc_pouch_txn_guard_lock(lc_pouch *pouch, const char *txn_id,
                                   int wait, lc_pouch_txn_guard *guard,
                                   lc_error *error) {
  char leaf[sizeof("pouch.implicit-txn-00.lock")];
  char *path;
  struct flock lock;
  size_t stripe;
  int pthread_rc;
  int rc;

  if (pouch == NULL || txn_id == NULL || txn_id[0] == '\0' || guard == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction guard requires pouch, xid, and "
                        "output",
                        NULL, NULL, "pouch");
  }
  guard->fd = -1;
  guard->mutex = NULL;
  guard->mutex_locked = 0;
  guard->shared_mutation = NULL;
  rc = lc_pouch_state_shared_mutation_enter(pouch, &guard->shared_mutation,
                                            error);
  if (rc != LC_OK) {
    return rc;
  }
  stripe = lc_pouch_txn_guard_stripe(txn_id);
  pthread_rc =
      pthread_once(&lc_pouch_txn_guard_once, lc_pouch_txn_guard_mutexes_init);
  if (pthread_rc != 0) {
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch transaction guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  guard->mutex = &lc_pouch_txn_guard_mutexes[stripe];
  pthread_rc = wait ? pthread_mutex_lock(guard->mutex)
                    : pthread_mutex_trylock(guard->mutex);
  if (pthread_rc != 0) {
    guard->mutex = NULL;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error,
                        pthread_rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT,
                        0L, "failed to lock pouch transaction guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  guard->mutex_locked = 1;
  if (snprintf(leaf, sizeof(leaf), "pouch.implicit-txn-%02lx.lock",
               (unsigned long)stripe) < 0) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch transaction guard", NULL, NULL,
                        "pouch");
  }
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path, leaf);
  if (path == NULL) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction guard path", NULL,
                        NULL, "pouch");
  }
  guard->fd = open(path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, path);
  if (guard->fd < 0) {
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch transaction guard",
                        strerror(errno), NULL, "pouch");
  }
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  while (fcntl(guard->fd, wait ? F_SETLKW : F_SETLK, &lock) != 0) {
    int lock_error = errno;
    if (errno == EINTR) {
      continue;
    }
    (void)close(guard->fd);
    guard->fd = -1;
    (void)pthread_mutex_unlock(guard->mutex);
    guard->mutex = NULL;
    guard->mutex_locked = 0;
    lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
    return lc_error_set(error,
                        !wait && (lock_error == EAGAIN || lock_error == EACCES)
                            ? LC_ERR_PENDING
                            : LC_ERR_TRANSPORT,
                        0L, "failed to lock pouch transaction guard",
                        strerror(lock_error), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_txn_guard_acquire(lc_pouch *pouch, const char *txn_id,
                                      lc_pouch_txn_guard *guard,
                                      lc_error *error) {
  return lc_pouch_txn_guard_lock(pouch, txn_id, 1, guard, error);
}

static void lc_pouch_txn_guard_release(lc_pouch_txn_guard *guard) {
  struct flock lock;

  if (guard == NULL) {
    return;
  }
  if (guard->fd >= 0) {
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    (void)fcntl(guard->fd, F_SETLK, &lock);
    (void)close(guard->fd);
    guard->fd = -1;
  }
  if (guard->mutex_locked && guard->mutex != NULL) {
    (void)pthread_mutex_unlock(guard->mutex);
  }
  guard->mutex = NULL;
  guard->mutex_locked = 0;
  lc_pouch_state_shared_mutation_leave(&guard->shared_mutation);
}

/* Selected private retention needs one immutable decision timestamp even for
 * an otherwise single-participant implicit transaction. Enroll only the known
 * lease, under existing xid authority; never search other namespaces. */
static int
lc_pouch_retention_stage_begin(lc_client_handle *client,
                               const lc_lease_ref *lease, const char *ns,
                               const char *key, const char *txn_id,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_txn_guard *guard, lc_error *error) {
  lc_pouch_txn_record record = {0};
  lc_pouch_lease_record current = {0};
  lc_txn_participant participant = {0};
  lc_txn_decision_req request;
  lc_txn_decision_res response = {0};
  size_t i;
  int enrolled = 0;
  int rc;

  memset(guard, 0, sizeof(*guard));
  guard->fd = -1;
  if (options == NULL || !options->has_retention)
    return LC_OK;
  if (client == NULL || lease == NULL || !lc_xid_is_valid(txn_id))
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "state retention staging requires an active transaction", NULL, NULL,
        "pouch");
  rc = lc_state_retention_validate(1, 0, ns, key, error);
  if (rc == LC_OK)
    rc = lc_pouch_txn_guard_acquire(client->pouch, txn_id, guard, error);
  if (rc == LC_OK)
    rc = lc_pouch_validate_lease_record(client, lease, ns, lease->key, &current,
                                        error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &record, error);
  if (rc == LC_OK && record.state != NULL &&
      strcmp(record.state, "prepare") != 0)
    rc =
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "state retention cannot change after transaction decision",
                     NULL, NULL, "pouch");
  for (i = 0; rc == LC_OK && i < record.participant_count; ++i) {
    if (strcmp(record.participants[i].ns, ns) == 0 &&
        strcmp(record.participants[i].key, lease->key) == 0) {
      enrolled = 1;
      if (record.votes[i] != 0)
        rc =
            lc_error_set(error, LC_ERR_INVALID, 0L,
                         "state retention cannot change after participant vote",
                         NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK && !enrolled) {
    lc_txn_decision_req_init(&request);
    participant.ns = ns;
    participant.key = lease->key;
    request.txn_id = txn_id;
    request.participants = &participant;
    request.participant_count = 1;
    request.expires_at_unix = current.expires_at_unix;
    request.tc_term = record.tc_term;
    rc = lc_pouch_client_txn_decision(&client->pub, &request, "prepare", 0,
                                      NULL, 0, &response, error);
  }
  lc_txn_decision_res_cleanup(&response);
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_lease_record_cleanup(&current);
  return rc;
}

static int lc_pouch_txn_record_exists(lc_client_handle *client,
                                      const char *txn_id, int *exists,
                                      lc_error *error) {
  lc_pouch_state_read_result read_result;
  char *key;
  int rc;

  if (client == NULL || exists == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction existence check requires client "
                        "and output",
                        NULL, NULL, "pouch");
  }
  *exists = 0;
  key = lc_pouch_txn_key(txn_id, error);
  if (key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read_metadata(client->pouch, LC_POUCH_TXN_NAMESPACE, key,
                                    &read_result, error);
  if (rc == LC_OK) {
    *exists = read_result.found;
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  lc_free_with_allocator(NULL, key);
  return rc;
}

static void
lc_pouch_implicit_txn_leader_cleanup(lc_pouch_implicit_txn_leader *leader) {
  if (leader == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, leader->ns);
  lc_free_with_allocator(NULL, leader->key);
  lc_free_with_allocator(NULL, leader->lease_id);
  memset(leader, 0, sizeof(*leader));
}

static int
lc_pouch_implicit_txn_leader_visit(const lc_pouch_state_visit_entry *entry,
                                   void *context, lc_error *error) {
  lc_pouch_implicit_txn_leader *leader;
  lc_pouch_lease_record record;
  int rc;

  leader = (lc_pouch_implicit_txn_leader *)context;
  if (leader == NULL || entry == NULL || entry->key == NULL ||
      !lc_pouch_txn_metadata_has_lease_record(entry->metadata,
                                              entry->metadata_length)) {
    return LC_OK;
  }
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_lease_record_parse(entry->metadata, entry->metadata_length,
                                   entry->version, &record, error);
  if (rc == LC_OK && record.found && !record.txn_explicit &&
      record.lease_id != NULL && record.lease_id[0] != '\0' &&
      record.txn_id != NULL && strcmp(record.txn_id, leader->txn_id) == 0 &&
      record.expires_at_unix > leader->now) {
    ++leader->count;
    if (leader->count == 1U) {
      leader->ns = lc_strdup_with_allocator(NULL, record.ns);
      leader->key = lc_strdup_with_allocator(NULL, record.key);
      leader->lease_id = lc_strdup_with_allocator(NULL, record.lease_id);
      leader->fencing_token = record.fencing_token;
      leader->expires_at_unix = record.expires_at_unix;
      if (leader->ns == NULL || leader->key == NULL ||
          leader->lease_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy implicit pouch transaction leader",
                          NULL, NULL, "pouch");
      }
    } else {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch implicit transaction has multiple leaders", NULL,
                        NULL, "pouch");
    }
  }
  lc_pouch_lease_record_cleanup(&record);
  return rc;
}

static int
lc_pouch_find_implicit_txn_leader(lc_client_handle *client, const char *txn_id,
                                  lc_pouch_implicit_txn_leader *leader,
                                  lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  int rc;

  if (client == NULL || client->pouch == NULL || txn_id == NULL ||
      leader == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch implicit transaction leader requires context",
                        NULL, NULL, "pouch");
  }
  memset(leader, 0, sizeof(*leader));
  leader->client = client;
  leader->txn_id = txn_id;
  rc = lc_pouch_now_unix(&leader->now, error);
  if (rc != LC_OK) {
    return rc;
  }
  namespaces_path = lc_pouch_path_join(&client->pouch->allocator,
                                       client->pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        "pouch");
  }
  dir = opendir(namespaces_path);
  lc_free_with_allocator(&client->pouch->allocator, namespaces_path);
  if (dir == NULL) {
    return errno == ENOENT ? LC_OK
                           : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                          "failed to scan pouch namespaces",
                                          strerror(errno), NULL, "pouch");
  }
  rc = LC_OK;
  while (rc == LC_OK) {
    char *ns;

    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to scan pouch namespaces", strerror(errno),
                          NULL, "pouch");
      }
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    ns = lc_pouch_path_unescape_name(&client->pouch->allocator, entry->d_name);
    if (ns == NULL) {
      continue;
    }
    rc = lc_pouch_state_visit(
        client->pouch, ns, lc_pouch_implicit_txn_leader_visit, leader, error);
    lc_free_with_allocator(&client->pouch->allocator, ns);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch namespaces", strerror(errno), NULL,
                      "pouch");
  }
  if (rc != LC_OK) {
    lc_pouch_implicit_txn_leader_cleanup(leader);
  }
  return rc;
}

/* The namespace walk is deliberately outside the candidate key lock: taking
 * that lock while preparing .txns would reintroduce the shared-Pouch nested
 * lock deadlock. Re-read the durable lease immediately before enrollment so a
 * leader which was released or replaced during the walk cannot be enrolled. */
static int lc_pouch_validate_implicit_txn_leader(
    lc_client_handle *client, const lc_pouch_implicit_txn_leader *leader,
    lc_error *error) {
  lc_pouch_lease_record record;
  lc_pouch_unix_seconds now;
  int rc;

  if (client == NULL || leader == NULL || leader->ns == NULL ||
      leader->key == NULL || leader->lease_id == NULL ||
      leader->txn_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch implicit transaction leader is incomplete", NULL,
                        NULL, "pouch");
  }
  memset(&record, 0, sizeof(record));
  now = 0L;
  rc = lc_pouch_read_lease_record(client, leader->ns, leader->key, &record,
                                  error);
  if (rc == LC_OK) {
    rc = lc_pouch_now_unix(&now, error);
  }
  if (rc == LC_OK && (!record.found || record.txn_explicit ||
                      record.lease_id == NULL || record.txn_id == NULL ||
                      strcmp(record.lease_id, leader->lease_id) != 0 ||
                      strcmp(record.txn_id, leader->txn_id) != 0 ||
                      record.fencing_token != leader->fencing_token ||
                      record.expires_at_unix <= now)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch implicit transaction leader is no longer active",
                      NULL, NULL, "pouch");
  }
  lc_pouch_lease_record_cleanup(&record);
  return rc;
}

/* Called with the xid guard held from before the joining lease is claimed.
 * That prevents the original normal lease from publishing in the interval
 * between the claim and durable enrollment. */
static int lc_pouch_prepare_implicit_transaction(
    lc_client *self, lc_client_handle *client, const char *txn_id,
    const char *ns, const char *key,
    lc_pouch_unix_seconds lease_expires_at_unix, lc_error *error) {
  lc_pouch_implicit_txn_leader leader;
  lc_txn_participant participants[2];
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  int record_exists;
  int rc;

  if (self == NULL || client == NULL || txn_id == NULL || ns == NULL ||
      key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch implicit transaction prepare requires context",
                        NULL, NULL, "pouch");
  }
  memset(&leader, 0, sizeof(leader));
  memset(participants, 0, sizeof(participants));
  lc_txn_decision_req_init(&request);
  memset(&result, 0, sizeof(result));
  record_exists = 0;
  rc = lc_pouch_txn_record_exists(client, txn_id, &record_exists, error);
  if (rc == LC_OK && !record_exists) {
    rc = lc_pouch_find_implicit_txn_leader(client, txn_id, &leader, error);
  }
  if (rc == LC_OK && leader.count == 1U) {
    rc = lc_pouch_validate_implicit_txn_leader(client, &leader, error);
  }
  participants[0].ns = ns;
  participants[0].key = key;
  request.txn_id = txn_id;
  request.participants = participants;
  request.participant_count = 1U;
  request.expires_at_unix = lease_expires_at_unix;
  if (rc == LC_OK)
    rc = lc_pouch_xa_managed_term(client, txn_id, &request.tc_term, error);
  if (rc == LC_OK && leader.count == 1U) {
    participants[1].ns = leader.ns;
    participants[1].key = leader.key;
    request.participant_count = 2U;
    if (leader.expires_at_unix > 0L &&
        (request.expires_at_unix <= 0L ||
         leader.expires_at_unix < request.expires_at_unix)) {
      request.expires_at_unix = leader.expires_at_unix;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_txn_decision(self, &request, "prepare", 0, NULL, 0,
                                      &result, error);
  }
  lc_txn_decision_res_cleanup(&result);
  lc_pouch_implicit_txn_leader_cleanup(&leader);
  return rc;
}

static int lc_pouch_txn_validate_participants(const lc_txn_decision_req *req,
                                              lc_error *error) {
  size_t i;

  if (req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction request is required", NULL, NULL,
                        NULL);
  }
  if (req->participant_count > 0U && req->participants == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction participants are required", NULL,
                        NULL, NULL);
  }
  for (i = 0U; i < req->participant_count; ++i) {
    size_t backend_hash_length;

    if (req->participants[i].ns == NULL || req->participants[i].ns[0] == '\0' ||
        req->participants[i].key == NULL ||
        req->participants[i].key[0] == '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction participant requires namespace "
                          "and key",
                          NULL, NULL, NULL);
    }
    lc_pouch_txn_trim_bounds(req->participants[i].backend_hash, NULL,
                             &backend_hash_length);
    if (req->participants[i].backend_hash != NULL &&
        req->participants[i].backend_hash[0] != '\0' &&
        backend_hash_length == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction participant backend hash is "
                          "invalid",
                          NULL, NULL, NULL);
    }
  }
  return LC_OK;
}

static int lc_pouch_txn_build_record(const lc_pouch_txn_record *txn_record,
                                     lc_pouch_txn_buffer *record,
                                     lc_error *error) {
  size_t i;
  int rc;

  memset(record, 0, sizeof(*record));
  if (txn_record == NULL || txn_record->state == NULL ||
      (txn_record->participant_count > 0U && txn_record->votes == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction record is incomplete", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_txn_buffer_append_bytes(record, LC_POUCH_TXN_RECORD_MAGIC,
                                        strlen(LC_POUCH_TXN_RECORD_MAGIC),
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(record, txn_record->state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(record, txn_record->expires_at_unix,
                                        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(record, txn_record->tc_term, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(
        record, txn_record->target_backend_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(
        record, (uint64_t)txn_record->participant_count, error);
  }
  for (i = 0U; rc == LC_OK && i < txn_record->participant_count; ++i) {
    rc = lc_pouch_txn_buffer_append_string(
        record, txn_record->participants[i].ns, error);
    if (rc == LC_OK) {
      rc = lc_pouch_txn_buffer_append_string(
          record, txn_record->participants[i].key, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_buffer_append_string(
          record, txn_record->participants[i].backend_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_buffer_append_bytes(record, &txn_record->votes[i], 1U,
                                            error);
    }
  }
  if (rc == LC_OK && txn_record->commit_at_unix > 0)
    rc = lc_pouch_txn_buffer_append_i64(record, txn_record->commit_at_unix,
                                        error);
  if (rc != LC_OK) {
    lc_pouch_txn_buffer_cleanup(record);
  }
  return rc;
}

static int lc_pouch_txn_decision_response(lc_txn_decision_res *out,
                                          const char *txn_id, const char *state,
                                          lc_pouch_generation version,
                                          lc_error *error) {
  char correlation[96];
  char version_text[32];

  memset(out, 0, sizeof(*out));
  if (lc_u64_format_base10_padded((lc_u64)version, 20U, version_text,
                                  sizeof(version_text)) < 0 ||
      snprintf(correlation, sizeof(correlation), "pouch-txn-%s", version_text) <
          0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch transaction correlation", NULL,
                        NULL, NULL);
  }
  out->txn_id = lc_strdup_local(txn_id);
  out->state = lc_strdup_local(state);
  out->correlation_id = lc_strdup_local(correlation);
  if (out->txn_id == NULL || out->state == NULL ||
      out->correlation_id == NULL) {
    lc_txn_decision_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_txn_replay_response(lc_txn_replay_res *out,
                                        const char *txn_id, const char *state,
                                        lc_pouch_generation version,
                                        lc_error *error) {
  char correlation[96];
  char version_text[32];

  memset(out, 0, sizeof(*out));
  if (lc_u64_format_base10_padded((lc_u64)version, 20U, version_text,
                                  sizeof(version_text)) < 0 ||
      snprintf(correlation, sizeof(correlation), "pouch-txn-%s", version_text) <
          0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch transaction correlation", NULL,
                        NULL, NULL);
  }
  out->txn_id = lc_strdup_local(txn_id);
  out->state = lc_strdup_local(state);
  out->correlation_id = lc_strdup_local(correlation);
  if (out->txn_id == NULL || out->state == NULL ||
      out->correlation_id == NULL) {
    lc_txn_replay_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction replay response",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

typedef struct lc_pouch_txn_attachment_collect {
  const char *staged_prefix;
  size_t staged_prefix_len;
  const char *committed_prefix;
  size_t committed_prefix_len;
  lc_pouch_txn_key_list base_keys;
} lc_pouch_txn_attachment_collect;

static int
lc_pouch_txn_collect_attachment_staged(const lc_pouch_state_visit_entry *entry,
                                       void *context, lc_error *error) {
  lc_pouch_txn_attachment_collect *collect;
  char *base_key;
  size_t key_len;
  size_t base_len;
  int rc;

  collect = (lc_pouch_txn_attachment_collect *)context;
  if (entry->key == NULL || strncmp(entry->key, collect->staged_prefix,
                                    collect->staged_prefix_len) != 0) {
    return LC_OK;
  }
  key_len = strlen(entry->key);
  if (key_len <= collect->staged_prefix_len) {
    return LC_OK;
  }
  base_len =
      collect->committed_prefix_len + key_len - collect->staged_prefix_len;
  base_key = (char *)lc_alloc_with_allocator(NULL, base_len + 1U);
  if (base_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction attachment key",
                        NULL, NULL, NULL);
  }
  memcpy(base_key, collect->committed_prefix, collect->committed_prefix_len);
  memcpy(base_key + collect->committed_prefix_len,
         entry->key + collect->staged_prefix_len,
         key_len - collect->staged_prefix_len);
  base_key[base_len] = '\0';
  if (lc_pouch_storage_key_has_staging_suffix(base_key)) {
    lc_free_with_allocator(NULL, base_key);
    return LC_OK;
  }
  rc = lc_pouch_txn_key_list_append(&collect->base_keys, base_key, error);
  lc_free_with_allocator(NULL, base_key);
  return rc;
}

static int lc_pouch_collect_staged_attachment_bases(
    lc_pouch *pouch, const char *ns, const char *key, const char *txn_id,
    lc_pouch_txn_key_list *out, lc_error *error) {
  lc_pouch_txn_attachment_collect collect;
  char *prefix;
  char *staged_prefix;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&collect, 0, sizeof(collect));
  prefix = lc_pouch_attachment_prefix(ns, key, error);
  if (prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  staged_prefix = lc_pouch_staged_attachment_prefix(ns, key, txn_id, error);
  if (staged_prefix == NULL) {
    lc_free_with_allocator(NULL, prefix);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  collect.committed_prefix = prefix;
  collect.committed_prefix_len = strlen(prefix);
  collect.staged_prefix = staged_prefix;
  collect.staged_prefix_len = strlen(staged_prefix);
  rc = lc_pouch_state_visit_prefix(pouch, ns, staged_prefix,
                                   lc_pouch_txn_collect_attachment_staged,
                                   &collect, error);
  if (rc == LC_OK) {
    *out = collect.base_keys;
    memset(&collect.base_keys, 0, sizeof(collect.base_keys));
  }
  lc_pouch_txn_key_list_cleanup(&collect.base_keys);
  lc_free_with_allocator(NULL, staged_prefix);
  lc_free_with_allocator(NULL, prefix);
  return rc;
}

static int lc_pouch_txn_commit_attachment_stage(lc_pouch *pouch, const char *ns,
                                                const char *base_key,
                                                const char *txn_id,
                                                lc_error *error) {
  lc_pouch_state_read_result staged;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  char *staged_key;
  int rc;

  memset(&staged, 0, sizeof(staged));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  staged_key =
      lc_pouch_staged_attachment_key_from_committed(base_key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(pouch, ns, staged_key, &staged, error);
  if (rc == LC_OK && staged.found &&
      lc_pouch_attachment_is_delete_marker(staged.content_type)) {
    options.object_record = 1;
    rc = lc_pouch_state_delete(pouch, ns, base_key, &options, &result, error);
    lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
    if (rc == LC_OK) {
      rc = lc_pouch_state_delete(pouch, ns, staged_key, &options, &result,
                                 error);
      lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
    }
  } else if (rc == LC_OK) {
    options.content_type = staged.content_type;
    options.metadata = staged.metadata;
    options.metadata_length = staged.metadata_length;
    options.has_metadata = staged.metadata_length > 0U;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(pouch, ns, base_key, staged.body, &options,
                              &result, error);
    if (rc == LC_OK) {
      lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
      rc = lc_pouch_state_delete(pouch, ns, staged_key, &options, &result,
                                 error);
    }
    lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
  }
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &staged);
  return rc;
}

static int
lc_pouch_txn_apply_attachment_participant(lc_pouch *pouch, const char *ns,
                                          const char *key, const char *txn_id,
                                          const char *state, lc_error *error) {
  lc_pouch_txn_key_list base_keys;
  size_t i;
  int rc;

  memset(&base_keys, 0, sizeof(base_keys));
  rc = lc_pouch_collect_staged_attachment_bases(pouch, ns, key, txn_id,
                                                &base_keys, error);
  for (i = 0U; rc == LC_OK && i < base_keys.count; ++i) {
    if (strcmp(state, "commit") == 0) {
      rc = lc_pouch_txn_commit_attachment_stage(pouch, ns, base_keys.keys[i],
                                                txn_id, error);
    } else if (strcmp(state, "rollback") == 0) {
      char *staged_key;

      staged_key = lc_pouch_staged_attachment_key_from_committed(
          base_keys.keys[i], txn_id, error);
      if (staged_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      } else {
        lc_pouch_state_write_result result;

        memset(&result, 0, sizeof(result));
        rc = lc_pouch_state_delete(pouch, ns, staged_key, NULL, &result, error);
        lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
      }
      lc_free_with_allocator(NULL, staged_key);
    }
  }
  lc_pouch_txn_key_list_cleanup(&base_keys);
  return rc;
}

static int lc_pouch_txn_commit_queue_stage(lc_client_handle *client,
                                           const char *ns, const char *base_key,
                                           const char *txn_id,
                                           lc_error *error) {
  lc_pouch_state_read_result staged;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_pouch_queue_record record;
  char *payload_key;
  char *staged_key;
  int rc;

  memset(&staged, 0, sizeof(staged));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  memset(&record, 0, sizeof(record));
  payload_key = NULL;
  staged_key = lc_pouch_staged_storage_key(base_key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc =
      lc_pouch_state_read_locked(client->pouch, ns, staged_key, &staged, error);
  if (rc == LC_OK && staged.found) {
    rc = lc_pouch_queue_record_parse(client, &staged, staged_key, &record,
                                     error);
  }
  if (rc == LC_OK && staged.found && strcmp(record.status, "acked") == 0) {
    options.object_record = 1;
    rc = lc_pouch_state_delete(client->pouch, ns, base_key, &options, &result,
                               error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    memset(&result, 0, sizeof(result));
    if (rc == LC_OK) {
      payload_key = lc_pouch_queue_payload_key_from_meta(base_key, error);
      if (payload_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_delete(client->pouch, ns, payload_key, &options,
                                 &result, error);
      lc_pouch_state_write_result_cleanup(&client->allocator, &result);
      memset(&result, 0, sizeof(result));
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_delete(client->pouch, ns, staged_key, &options,
                                 &result, error);
    }
  } else if (rc == LC_OK) {
    rc = lc_pouch_state_commit_staged_for_active_operation(
        client->pouch, ns, base_key, txn_id, &result, error);
  }
  if (rc == LC_OK && staged.found) {
    lc_pouch_queue_touch_notification(client, record.ns, record.queue);
  }
  lc_free_with_allocator(NULL, payload_key);
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_queue_record_cleanup(&record);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

/* The caller holds namespace mutation authority while the selected lease is
 * read and cleared. Transaction application changes the paired queue document
 * in that same authority window, so a shared writer cannot install a newer
 * delivery lease between these two operations. */
static int lc_pouch_queue_clear_lease_key_locked(lc_client_handle *client,
                                                 const char *ns,
                                                 const char *lease_key,
                                                 lc_error *error) {
  lc_pouch_lease_record lease_record;
  int rc;

  memset(&lease_record, 0, sizeof(lease_record));
  {
    lc_pouch_state_read_result read_result;

    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read_metadata_locked(client->pouch, ns, lease_key,
                                             &read_result, error);
    if (rc == LC_OK) {
      rc = lc_pouch_lease_record_parse(
          read_result.metadata, read_result.metadata_length,
          read_result.version, &lease_record, error);
    }
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  }
  if (rc == LC_OK && lease_record.found) {
    rc = lc_pouch_write_lease_tombstone(
        client, ns, lease_key, lease_record.owner, lease_record.fencing_token,
        lease_record.version, error);
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_txn_rollback_queue_message(lc_client_handle *client,
                                               const char *ns,
                                               const char *metadata_key,
                                               const char *txn_id,
                                               lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_queue_record record;
  int discarded;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  memset(&record, 0, sizeof(record));
  discarded = 0;
  rc = lc_pouch_state_discard_staged(client->pouch, ns, metadata_key, txn_id,
                                     &discarded, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_read_locked(client->pouch, ns, metadata_key,
                                    &read_result, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_queue_record_parse(client, &read_result, metadata_key,
                                     &record, error);
  }
  if (rc == LC_OK && read_result.found && record.lease_txn_id != NULL &&
      strcmp(record.lease_txn_id, txn_id) == 0) {
    rc = lc_pouch_queue_replace_string(&record.status, "available", error);
    if (rc == LC_OK) {
      record.not_visible_until_unix = 0L;
      rc = lc_pouch_queue_clear_lease(&record, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_queue_write_record(client, &record, error);
    }
    if (rc == LC_OK) {
      lc_pouch_queue_touch_notification(client, record.ns, record.queue);
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

/* Go lockd disk validates the queue document and its lease metadata together
 * before applying a transactional decision. Namespace mutation authority
 * keeps this validation and the following staged change atomic across shared
 * Pouch writers. */
static int lc_pouch_txn_validate_queue_message_lease(
    const lc_pouch_queue_record *record,
    const lc_pouch_lease_record *lease_record, const char *txn_id,
    const char *state, lc_error *error) {
  int lease_missing;

  if (record == NULL || lease_record == NULL || txn_id == NULL ||
      state == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction lease validation requires "
                        "context",
                        NULL, NULL, "pouch");
  }
  lease_missing = record->lease_id == NULL || record->lease_id[0] == '\0';
  lease_missing =
      lease_missing &&
      (record->lease_txn_id == NULL || record->lease_txn_id[0] == '\0') &&
      record->lease_fencing_token == 0L;
  if (strcmp(state, "commit") == 0 &&
      (record->lease_txn_id == NULL ||
       strcmp(record->lease_txn_id, txn_id) != 0)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction lease mismatch", NULL, NULL,
                        "pouch");
  }
  if (lease_missing) {
    if (strcmp(state, "commit") == 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch queue transaction lease missing", NULL, NULL,
                          "pouch");
    }
    return LC_OK;
  }
  if (!lease_record->found) {
    if (strcmp(state, "commit") == 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch queue transaction lease missing", NULL, NULL,
                          "pouch");
    }
    if (record->lease_txn_id == NULL || record->lease_txn_id[0] == '\0') {
      return LC_OK;
    }
    if (strcmp(record->lease_txn_id, txn_id) != 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch queue transaction lease mismatch", NULL, NULL,
                          "pouch");
    }
    return LC_OK;
  }
  if (strcmp(lease_record->txn_id, txn_id) != 0 || record->lease_id == NULL ||
      strcmp(record->lease_id, lease_record->lease_id) != 0 ||
      record->lease_fencing_token != lease_record->fencing_token ||
      record->lease_txn_id == NULL ||
      strcmp(record->lease_txn_id, txn_id) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction lease mismatch", NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_txn_apply_queue_participant_locked(void *context,
                                                       lc_error *error) {
  lc_pouch_txn_queue_participant_context *ctx;
  lc_pouch_lease_record lease_record;
  lc_pouch_queue_record queue_record;
  char *metadata_key;
  char *state_object_key;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_pouch_unix_seconds now;
  int discarded;
  int rc;

  ctx = (lc_pouch_txn_queue_participant_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->ns == NULL ||
      ctx->lease_key == NULL || ctx->txn_id == NULL || ctx->state == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction participant requires context",
                        NULL, NULL, "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(&queue_record, 0, sizeof(queue_record));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  metadata_key = NULL;
  state_object_key = NULL;
  discarded = 0;
  now = 0L;
  {
    lc_pouch_state_read_result read_result;

    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read_metadata_locked(
        ctx->client->pouch, ctx->ns, ctx->lease_key, &read_result, error);
    if (rc == LC_OK) {
      rc = lc_pouch_lease_record_parse(
          read_result.metadata, read_result.metadata_length,
          read_result.version, &lease_record, error);
    }
    lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
  }
  if (rc == LC_OK && lease_record.found &&
      strcmp(lease_record.txn_id, ctx->txn_id) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue transaction lease mismatch", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !ctx->state_participant && lease_record.found &&
      strcmp(ctx->state, "commit") == 0) {
    rc = lc_pouch_now_unix(&now, error);
    if (rc == LC_OK && lease_record.expires_at_unix <= now) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction lease expired", NULL, NULL,
                        "pouch");
    }
  }
  if (rc == LC_OK && !ctx->state_participant) {
    metadata_key =
        lc_pouch_queue_message_meta_key_from_lease_key(ctx->lease_key, error);
    if (metadata_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && !ctx->state_participant) {
    lc_pouch_state_read_result read_result;

    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read_locked(ctx->client->pouch, ctx->ns, metadata_key,
                                    &read_result, error);
    if (rc == LC_OK && read_result.found) {
      rc = lc_pouch_queue_record_parse(ctx->client, &read_result, metadata_key,
                                       &queue_record, error);
    }
    lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
  }
  if (rc == LC_OK && !ctx->state_participant &&
      queue_record.storage_key != NULL) {
    rc = lc_pouch_txn_validate_queue_message_lease(
        &queue_record, &lease_record, ctx->txn_id, ctx->state, error);
  }
  if (rc == LC_OK && !ctx->state_participant &&
      queue_record.storage_key != NULL && strcmp(ctx->state, "commit") == 0) {
    if (queue_record.not_visible_until_unix <= now) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue transaction lease expired", NULL, NULL,
                        "pouch");
    }
  }
  if (rc == LC_OK && !ctx->state_participant &&
      queue_record.storage_key != NULL && strcmp(ctx->state, "commit") == 0) {
    rc = lc_pouch_txn_commit_queue_stage(ctx->client, ctx->ns, metadata_key,
                                         ctx->txn_id, error);
  } else if (rc == LC_OK && !ctx->state_participant &&
             queue_record.storage_key != NULL &&
             strcmp(ctx->state, "rollback") == 0) {
    rc = lc_pouch_txn_rollback_queue_message(ctx->client, ctx->ns, metadata_key,
                                             ctx->txn_id, error);
  } else if (rc == LC_OK && !ctx->state_participant &&
             queue_record.storage_key != NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch transaction state is unsupported", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK && ctx->state_participant) {
    state_object_key =
        lc_pouch_queue_state_object_key_from_lease_key(ctx->lease_key, error);
    if (state_object_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && ctx->state_participant) {
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    if (strcmp(ctx->state, "commit") == 0) {
      rc = lc_pouch_state_delete(ctx->client->pouch, ctx->ns, state_object_key,
                                 &options, &result, error);
    } else if (strcmp(ctx->state, "rollback") == 0) {
      rc = lc_pouch_state_discard_staged(ctx->client->pouch, ctx->ns,
                                         state_object_key, ctx->txn_id,
                                         &discarded, error);
    } else {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction state is unsupported", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK &&
      (ctx->state_participant || queue_record.storage_key != NULL)) {
    rc = lc_pouch_queue_clear_lease_key_locked(ctx->client, ctx->ns,
                                               ctx->lease_key, error);
  }
  lc_pouch_state_write_result_cleanup(&ctx->client->allocator, &result);
  lc_free_with_allocator(NULL, state_object_key);
  lc_free_with_allocator(NULL, metadata_key);
  lc_pouch_queue_record_cleanup(&queue_record);
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_txn_apply_queue_message_participant(
    lc_client_handle *client, const char *ns, const char *message_lease_key,
    const char *txn_id, const char *state, lc_error *error) {
  lc_pouch_txn_queue_participant_context context;
  int rc;

  memset(&context, 0, sizeof(context));
  context.client = client;
  context.ns = ns;
  context.lease_key = message_lease_key;
  context.txn_id = txn_id;
  context.state = state;

  rc = lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_txn_apply_queue_participant_locked, &context,
      error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(client->pouch, ns, 1);
  }
  return rc;
}

static int lc_pouch_txn_apply_queue_state_participant(
    lc_client_handle *client, const char *ns, const char *state_lease_key,
    const char *txn_id, const char *state, lc_error *error) {
  lc_pouch_txn_queue_participant_context context;
  int rc;

  memset(&context, 0, sizeof(context));
  context.client = client;
  context.ns = ns;
  context.lease_key = state_lease_key;
  context.txn_id = txn_id;
  context.state = state;
  context.state_participant = 1;

  rc = lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_txn_apply_queue_participant_locked, &context,
      error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(client->pouch, ns, 1);
  }
  return rc;
}

static int lc_pouch_txn_metadata_has_lease_record(const unsigned char *bytes,
                                                  size_t length) {
  return bytes != NULL && length >= strlen(LC_POUCH_LEASE_RECORD_MAGIC) &&
         memcmp(bytes, LC_POUCH_LEASE_RECORD_MAGIC,
                strlen(LC_POUCH_LEASE_RECORD_MAGIC)) == 0;
}

static int lc_pouch_txn_clear_lease_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_lease_record *lease_record, lc_error *error) {
  lc_pouch_txn_buffer buffer;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || lease_record == NULL ||
      lease_record->owner == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction lease clear requires context", NULL,
                        NULL, NULL);
  }
  memset(&buffer, 0, sizeof(buffer));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_lease_record_build(&buffer, ns, key, lease_record->owner, "",
                                   "", lease_record->fencing_token, 0L,
                                   lease_record->state_version, 0, error);
  if (rc == LC_OK) {
    options.content_type = LC_POUCH_LEASE_CONTENT_TYPE;
    options.has_metadata = 1;
    options.metadata = (const unsigned char *)buffer.bytes;
    options.metadata_length = buffer.length;
    rc = lc_pouch_state_update_metadata_locked(pouch, ns, key, &options,
                                               &result, error);
  }
  lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
  lc_pouch_txn_buffer_cleanup(&buffer);
  return rc;
}

static int lc_pouch_txn_apply_state_participant_locked(void *context,
                                                       lc_error *error) {
  lc_pouch_txn_state_participant_context *ctx;
  lc_pouch_state_metadata_view state_view;
  lc_pouch_state_read_result fallback;
  lc_pouch_lease_record lease_record;
  lc_pouch_state_write_result write_result;
  lc_pouch_unix_seconds now_seconds;
  int discarded;
  int active_lease;
  int rc;

  ctx = (lc_pouch_txn_state_participant_context *)context;
  if (ctx == NULL || ctx->pouch == NULL || ctx->ns == NULL ||
      ctx->key == NULL || ctx->txn_id == NULL || ctx->state == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction state participant requires context",
                        NULL, NULL, NULL);
  }
  memset(&state_view, 0, sizeof(state_view));
  memset(&fallback, 0, sizeof(fallback));
  memset(&lease_record, 0, sizeof(lease_record));
  memset(&write_result, 0, sizeof(write_result));
  now_seconds = 0L;
  discarded = 0;
  active_lease = 0;
  rc = lc_pouch_state_read_metadata_view_locked(ctx->pouch, ctx->ns, ctx->key,
                                                &state_view, &fallback, error);
  if (rc == LC_OK && ctx->lease != NULL) {
    if (ctx->lease->fencing_token <= 0L) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease validation requires fencing_token", NULL,
                        NULL, NULL);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_lease_record_parse(
          state_view.metadata, state_view.metadata_length, state_view.version,
          &lease_record, error);
    }
    if (rc == LC_OK &&
        (!lease_record.found || strcmp(lease_record.ns, ctx->ns) != 0 ||
         strcmp(lease_record.key, ctx->key) != 0 ||
         strcmp(lease_record.lease_id, ctx->lease->lease_id) != 0 ||
         lease_record.fencing_token != ctx->lease->fencing_token)) {
      /* Release is idempotent: a missing or superseded lease is already
       * released. Never touch the replacement holder's staged state. */
      goto cleanup;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_now_unix(&now_seconds, error);
    }
    if (rc == LC_OK && lease_record.expires_at_unix <= now_seconds) {
      /* Match lockd's expiry path: discard this expired holder's staged
       * changes, clear its lease, and report an idempotent release. */
      rc = lc_pouch_state_discard_staged_locked(ctx->pouch, ctx->ns, ctx->key,
                                                lease_record.txn_id, &discarded,
                                                error);
      if (rc == LC_OK) {
        rc = lc_pouch_txn_apply_attachment_participant(
            ctx->pouch, ctx->ns, ctx->key, lease_record.txn_id, "rollback",
            error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_txn_clear_lease_locked(ctx->pouch, ctx->ns, ctx->key,
                                             &lease_record, error);
      }
      goto cleanup;
    }
    if (rc == LC_OK &&
        strcmp(lease_record.txn_id != NULL ? lease_record.txn_id : "",
               ctx->txn_id) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction lease does not match release", NULL,
                        NULL, NULL);
    }
    active_lease = rc == LC_OK;
  } else if (rc == LC_OK &&
             lc_pouch_txn_metadata_has_lease_record(
                 state_view.metadata, state_view.metadata_length)) {
    rc = lc_pouch_lease_record_parse(state_view.metadata,
                                     state_view.metadata_length,
                                     state_view.version, &lease_record, error);
    active_lease = lease_record.found && lease_record.lease_id != NULL &&
                   lease_record.lease_id[0] != '\0';
  }
  if (rc == LC_OK && active_lease &&
      strcmp(lease_record.txn_id != NULL ? lease_record.txn_id : "",
             ctx->txn_id) != 0) {
    goto cleanup;
  }
  if (rc == LC_OK && strcmp(ctx->state, "commit") == 0) {
    rc = lc_pouch_state_commit_staged_locked(ctx->pouch, ctx->ns, ctx->key,
                                             ctx->txn_id, &write_result, 1,
                                             ctx->commit_at_unix, error);
  } else if (rc == LC_OK && strcmp(ctx->state, "rollback") == 0) {
    rc = lc_pouch_state_discard_staged_locked(ctx->pouch, ctx->ns, ctx->key,
                                              ctx->txn_id, &discarded, error);
  } else if (rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch transaction state is unsupported", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK && ctx->retention_cleanup)
    rc =
        lc_pouch_retention_note_phase(LC_POUCH_RETENTION_PARENT_APPLIED, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_apply_attachment_participant(
        ctx->pouch, ctx->ns, ctx->key, ctx->txn_id, ctx->state, error);
  }
  if (rc == LC_OK && ctx->retention_cleanup)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_ATTACHMENTS_APPLIED,
                                       error);
  if (rc == LC_OK && active_lease) {
    rc = lc_pouch_txn_clear_lease_locked(ctx->pouch, ctx->ns, ctx->key,
                                         &lease_record, error);
  }

cleanup:
  if (rc != LC_OK && write_result.query_index_operation_guard_started) {
    lc_pouch_query_index_operation_cancel(ctx->pouch, ctx->ns, ctx->key,
                                          ctx->txn_id);
    write_result.query_index_operation_guard_started = 0;
  }
  lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &write_result);
  lc_pouch_lease_record_cleanup(&lease_record);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &fallback);
  return rc;
}

static int lc_pouch_txn_apply_state_participant(
    lc_client_handle *client, const lc_lease_ref *lease, const char *ns,
    const char *key, const char *txn_id, const char *state,
    lc_pouch_unix_seconds commit_at_unix, lc_error *error) {
  lc_pouch_txn_state_participant_context context;
  int rc;

  memset(&context, 0, sizeof(context));
  context.pouch = client->pouch;
  context.lease = lease;
  context.ns = ns;
  context.key = key;
  context.txn_id = txn_id;
  context.state = state;
  context.commit_at_unix = commit_at_unix;

  rc = lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_txn_apply_state_participant_locked, &context,
      error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(client->pouch, ns, 1);
  }
  return rc;
}

static int lc_pouch_txn_is_queue_lease_mismatch(const lc_error *error) {
  if (error == NULL || error->code != LC_ERR_INVALID ||
      error->message == NULL) {
    return 0;
  }
  return strcmp(error->message, "pouch queue transaction lease mismatch") ==
             0 ||
         strcmp(error->message, "pouch queue transaction lease missing") == 0 ||
         strcmp(error->message, "pouch queue transaction lease expired") == 0;
}

static void lc_pouch_txn_cancel_applied_operation_guards(
    lc_client_handle *client, const lc_txn_decision_req *req,
    const char *local_backend_hash, int requires_backend_hash,
    size_t participant_count) {
  size_t i;

  if (client == NULL || req == NULL || req->participants == NULL ||
      req->txn_id == NULL || req->txn_id[0] == '\0') {
    return;
  }
  for (i = 0U; i < participant_count; ++i) {
    const char *participant_backend_hash;
    const char *ns;
    size_t participant_backend_hash_length;

    lc_pouch_txn_trim_bounds(req->participants[i].backend_hash,
                             &participant_backend_hash,
                             &participant_backend_hash_length);
    if (participant_backend_hash_length != 0U &&
        (!requires_backend_hash ||
         strlen(local_backend_hash) != participant_backend_hash_length ||
         strncmp(local_backend_hash, participant_backend_hash,
                 participant_backend_hash_length) != 0)) {
      continue;
    }
    if (lc_pouch_queue_is_message_lease_key(req->participants[i].key) ||
        lc_pouch_queue_is_state_lease_key(req->participants[i].key)) {
      continue;
    }
    ns = lc_pouch_client_namespace(client, req->participants[i].ns);
    if (lc_pouch_client_namespace_reserved(ns)) {
      continue;
    }
    lc_pouch_query_index_operation_cancel(
        client->pouch, ns, req->participants[i].key, req->txn_id);
  }
}

/* Completion metadata is not a decision. The xid guard serializes its first
 * write with application, recovery and retirement. Never refresh this age on
 * retries. A retained completion also prevents replaying old Pouch staging. */
static int lc_pouch_xa_completed_read(lc_client_handle *client,
                                      const char *txn_id,
                                      lc_pouch_unix_seconds *completed,
                                      lc_error *error) {
  char key[sizeof("completed/") + LC_XID_STRING_SIZE];
  lc_pouch_state_read_result result;
  lc_pouch_binary_cursor cursor;
  lc_sink *sink = NULL;
  const void *bytes;
  size_t length;
  int64_t value = 0;
  int rc;
  *completed = 0;
  memset(&result, 0, sizeof(result));
  memset(&cursor, 0, sizeof(cursor));
  (void)snprintf(key, sizeof(key), "completed/%s", txn_id);
  rc = lc_pouch_state_read(client->pouch, LC_POUCH_XA_NAMESPACE, key, &result,
                           error);
  if (rc == LC_OK && result.found)
    rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK && result.found)
    rc = lc_copy(result.body, sink, NULL, error);
  if (rc == LC_OK && result.found)
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  if (rc == LC_OK && result.found) {
    cursor.bytes = (const unsigned char *)bytes;
    cursor.length = length;
    rc = lc_pouch_binary_cursor_magic(&cursor, "LPXC", error);
    if (rc == LC_OK)
      rc = lc_pouch_binary_cursor_i64(&cursor, &value, error);
    if (rc == LC_OK && (value <= 0 || cursor.offset != cursor.length))
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid XA completion timestamp", NULL, NULL,
                        "pouch-xa");
    if (rc == LC_OK)
      *completed = (lc_pouch_unix_seconds)value;
  }
  if (sink != NULL)
    lc_sink_close(sink);
  lc_pouch_state_read_result_cleanup(&client->allocator, &result);
  return rc;
}
static int lc_pouch_xa_completed_write(lc_client_handle *client,
                                       const char *txn_id, lc_error *error) {
  char key[sizeof("completed/") + LC_XID_STRING_SIZE];
  lc_pouch_unix_seconds completed = 0;
  lc_pouch_txn_buffer buffer;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source = NULL;
  int managed = 0;
  int rc = lc_pouch_xa_managed(client, txn_id, &managed, error);
  if (rc != LC_OK || !managed)
    return rc;
  rc = lc_pouch_xa_completed_read(client, txn_id, &completed, error);
  if (rc != LC_OK || completed != 0)
    return rc;
  memset(&buffer, 0, sizeof(buffer));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.object_record = 1;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.has_expected_version = 1;
  rc = lc_pouch_now_unix(&completed, error);
  if (rc == LC_OK)
    rc = lc_pouch_txn_buffer_append_bytes(&buffer, "LPXC", 4U, error);
  if (rc == LC_OK)
    rc = lc_pouch_txn_buffer_append_i64(&buffer, (int64_t)completed, error);
  if (rc == LC_OK)
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  (void)snprintf(key, sizeof(key), "completed/%s", txn_id);
  if (rc == LC_OK)
    rc = lc_pouch_state_write(client->pouch, LC_POUCH_XA_NAMESPACE, key, source,
                              &options, &result, error);
  if (source != NULL)
    lc_source_close(source);
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_txn_apply_participants(lc_client_handle *client,
                                           const lc_txn_decision_req *req,
                                           const char *state,
                                           int tolerate_queue_lease_mismatch,
                                           lc_pouch_unix_seconds commit_at_unix,
                                           lc_error *error) {
  char local_backend_hash[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  size_t i;
  int requires_backend_hash;
  int rc;
  lc_pouch_unix_seconds completed = 0;

  if (strcmp(state, "prepare") == 0) {
    return LC_OK;
  }
  if (req->tc_term != 0U) {
    rc = lc_pouch_xa_completed_read(client, req->txn_id, &completed, error);
    if (rc != LC_OK || completed != 0)
      return rc;
  }
  rc = lc_pouch_txn_validate_participants(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->participant_count == 1) {
    int retention_owned = 0;
    rc = lc_pouch_retention_owns_journal(client->pouch, req->participants[0].ns,
                                         req->participants[0].key, req->txn_id,
                                         &retention_owned, error);
    if (rc != LC_OK)
      return rc;
    if (retention_owned)
      return lc_error_set(error, LC_ERR_PENDING, 0L,
                          "state retention cleanup requires root maintenance",
                          req->txn_id, NULL, "pouch");
  }
  if (strcmp(state, "commit") == 0 && commit_at_unix <= 0) {
    rc = lc_pouch_txn_validate_retention(client, req->txn_id, req->participants,
                                         req->participant_count, -1, error);
    if (rc != LC_OK)
      return rc;
  }
  /* Validate every external completion before applying even the first native
   * participant. Pending SQL must never publish a partial Pouch/outbox result.
   */
  rc = lc_pouch_xa_application_gate(client->pouch, req, state, error);
  if (rc != LC_OK)
    return rc;
  requires_backend_hash = 0;
  for (i = 0U; i < req->participant_count; ++i) {
    const char *participant_backend_hash;
    size_t participant_backend_hash_length;

    lc_pouch_txn_trim_bounds(req->participants[i].backend_hash,
                             &participant_backend_hash,
                             &participant_backend_hash_length);
    if (participant_backend_hash_length != 0U) {
      requires_backend_hash = 1;
      break;
    }
  }
  if (requires_backend_hash) {
    rc = lc_pouch_backend_hash(client->pouch, local_backend_hash, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (i = 0U; i < req->participant_count; ++i) {
    const char *participant_backend_hash;
    size_t participant_backend_hash_length;
    const char *ns = NULL;

    lc_pouch_txn_trim_bounds(req->participants[i].backend_hash,
                             &participant_backend_hash,
                             &participant_backend_hash_length);
    if (participant_backend_hash_length != 0U &&
        (strlen(local_backend_hash) != participant_backend_hash_length ||
         strncmp(participant_backend_hash, local_backend_hash,
                 participant_backend_hash_length) != 0)) {
      continue;
    }

    if (lc_pouch_xa_participant(&req->participants[i]))
      continue;

    if (!lc_pouch_queue_is_message_lease_key(req->participants[i].key) &&
        !lc_pouch_queue_is_state_lease_key(req->participants[i].key)) {
      rc = lc_pouch_client_validate_public_key(req->participants[i].key, error);
      if (rc != LC_OK) {
        lc_pouch_txn_cancel_applied_operation_guards(
            client, req, local_backend_hash, requires_backend_hash, i);
        return rc;
      }
    }
    rc = lc_pouch_client_public_namespace(client, req->participants[i].ns, &ns,
                                          error);
    if (rc != LC_OK) {
      lc_pouch_txn_cancel_applied_operation_guards(
          client, req, local_backend_hash, requires_backend_hash, i);
      return rc;
    }
    if (lc_pouch_queue_is_message_lease_key(req->participants[i].key)) {
      rc = lc_pouch_txn_apply_queue_message_participant(
          client, ns, req->participants[i].key, req->txn_id, state, error);
      if (rc != LC_OK) {
        if (tolerate_queue_lease_mismatch &&
            lc_pouch_txn_is_queue_lease_mismatch(error)) {
          lc_error_cleanup(error);
          lc_error_init(error);
          continue;
        }
        lc_pouch_txn_cancel_applied_operation_guards(
            client, req, local_backend_hash, requires_backend_hash, i + 1U);
        return rc;
      }
      continue;
    }
    if (lc_pouch_queue_is_state_lease_key(req->participants[i].key)) {
      rc = lc_pouch_txn_apply_queue_state_participant(
          client, ns, req->participants[i].key, req->txn_id, state, error);
      if (rc != LC_OK) {
        if (tolerate_queue_lease_mismatch &&
            lc_pouch_txn_is_queue_lease_mismatch(error)) {
          lc_error_cleanup(error);
          lc_error_init(error);
          continue;
        }
        lc_pouch_txn_cancel_applied_operation_guards(
            client, req, local_backend_hash, requires_backend_hash, i + 1U);
        return rc;
      }
      continue;
    }
    if (strcmp(state, "commit") == 0) {
      rc = lc_pouch_txn_apply_state_participant(
          client, NULL, ns, req->participants[i].key, req->txn_id, state,
          commit_at_unix > 0 ? commit_at_unix : -1, error);
    } else if (strcmp(state, "rollback") == 0) {
      rc = lc_pouch_txn_apply_state_participant(
          client, NULL, ns, req->participants[i].key, req->txn_id, state,
          commit_at_unix > 0 ? commit_at_unix : -1, error);
    } else {
      lc_pouch_txn_cancel_applied_operation_guards(
          client, req, local_backend_hash, requires_backend_hash, i);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction state is unsupported", NULL, NULL,
                          NULL);
    }
    if (rc != LC_OK) {
      lc_pouch_txn_cancel_applied_operation_guards(
          client, req, local_backend_hash, requires_backend_hash, i + 1U);
      return rc;
    }
  }
  /* A transaction decision is the logical completion boundary for every
   * staged participant. Delay exclusive threshold publication until all local
   * participants have reached their committed or discarded final state. */
  for (i = 0U; i < req->participant_count; ++i) {
    const char *participant_backend_hash;
    size_t participant_backend_hash_length;
    const char *ns;

    lc_pouch_txn_trim_bounds(req->participants[i].backend_hash,
                             &participant_backend_hash,
                             &participant_backend_hash_length);
    if (participant_backend_hash_length != 0U &&
        (strlen(local_backend_hash) != participant_backend_hash_length ||
         strncmp(participant_backend_hash, local_backend_hash,
                 participant_backend_hash_length) != 0)) {
      continue;
    }
    if (lc_pouch_queue_is_message_lease_key(req->participants[i].key) ||
        lc_pouch_queue_is_state_lease_key(req->participants[i].key) ||
        lc_pouch_xa_participant(&req->participants[i])) {
      continue;
    }
    ns = NULL;
    rc = lc_pouch_client_public_namespace(client, req->participants[i].ns, &ns,
                                          error);
    if (rc != LC_OK) {
      lc_pouch_txn_cancel_applied_operation_guards(
          client, req, local_backend_hash, requires_backend_hash,
          req->participant_count);
      return rc;
    }
    lc_pouch_indexer_note_operation_complete(
        client->pouch, ns, req->participants[i].key, req->txn_id);
    if (strcmp(state, "commit") == 0)
      lc_outbox_pouch_notify(&client->pub, ns, req->participants[i].key);
  }
  return req->tc_term != 0U
             ? lc_pouch_xa_completed_write(client, req->txn_id, error)
             : LC_OK;
}

static int lc_pouch_txn_delete_recovered_record(lc_client_handle *client,
                                                const char *key,
                                                lc_error *error) {
  enum { LC_POUCH_TXN_RECOVERY_DELETE_ATTEMPTS = 3 };
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  size_t attempt;
  int rc;

  /* Every Pouch client recovers durable transaction decisions at open. Two
   * rolling instances can therefore observe the same completed record. The
   * delete is deliberately CAS-protected, but losing that cleanup race means
   * another recovery made equivalent progress, not that opening the second
   * shared-writer client is unsafe. Re-read and retry a bounded number of
   * times so a genuinely changing record still reports its real error. */
  for (attempt = 0U; attempt < LC_POUCH_TXN_RECOVERY_DELETE_ATTEMPTS;
       ++attempt) {
    memset(&read_result, 0, sizeof(read_result));
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    rc = lc_pouch_state_read(client->pouch, LC_POUCH_TXN_NAMESPACE, key,
                             &read_result, error);
    if (rc != LC_OK)
      return rc;
    if (!read_result.found) {
      lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
      return LC_OK;
    }
    options.has_expected_version = 1;
    options.expected_version = read_result.version;
    options.object_record = 1;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    rc = lc_pouch_state_delete(client->pouch, LC_POUCH_TXN_NAMESPACE, key,
                               &options, &result, error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    if (rc == LC_OK || error == NULL || error->code != LC_ERR_INVALID ||
        error->message == NULL ||
        strcmp(error->message,
               "pouch state delete version precondition failed") != 0) {
      return rc;
    }
    if (attempt + 1U == LC_POUCH_TXN_RECOVERY_DELETE_ATTEMPTS)
      return rc;
    lc_error_cleanup(error);
    lc_error_init(error);
  }
  return rc;
}

typedef struct lc_pouch_native_retirement {
  lc_pouch *pouch;
  const char *ns;
} lc_pouch_native_retirement;

static int lc_pouch_native_retirement_sync(void *context, lc_error *error) {
  lc_pouch_native_retirement *retirement = context;
  return lc_pouch_state_durable_scope_include_namespace_locked(
      retirement->pouch, retirement->ns, error);
}

/* The caller holds xid authority and has just applied an automatically chosen
 * single-key decision. Explicit/native XA outcomes remain replayable. A
 * NoSync decision must not disappear durably ahead of its applied participant.
 */
static int lc_pouch_retire_native_completion(lc_client_handle *client,
                                             const char *txn_id, const char *ns,
                                             lc_error *error) {
  lc_pouch_state_commit_group *scope = NULL;
  lc_pouch_native_retirement retirement;
  int managed = 0;
  int rc = lc_pouch_xa_managed(client, txn_id, &managed, error);
  if (rc != LC_OK || managed)
    return rc;
  if (!client->pouch->durable_sync) {
    retirement.pouch = client->pouch;
    retirement.ns = ns;
    rc = lc_pouch_state_durable_scope_begin(client->pouch, &scope, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_with_namespace_lock(client->pouch, ns,
                                              lc_pouch_native_retirement_sync,
                                              &retirement, error);
    if (scope != NULL)
      rc = lc_pouch_state_durable_scope_end(scope, rc, error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_txn_delete_recovered_record(client, txn_id, error);
  return rc;
}

static int lc_pouch_client_get_namespace(lc_client_handle *client,
                                         const char *ns, const char *key,
                                         const lc_get_opts *opts, lc_sink *dst,
                                         lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  const char *resolved_namespace = NULL;
  int rc;

  (void)opts;
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_client_public_namespace(client, ns, &resolved_namespace, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_copy(client->pouch, resolved_namespace, key, dst,
                           &read_result, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    out->retention_support = LC_STATE_RETENTION_SUPPORTED;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_client_load_namespace(lc_client_handle *client,
                                          const char *ns, const char *key,
                                          const lonejson_map *map, void *dst,
                                          const lc_get_opts *opts,
                                          lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *memory_sink;
  const void *bytes;
  size_t length;
  char *json;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;
  const char *resolved_namespace = NULL;
  int rc;

  (void)opts;
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  memory_sink = NULL;
  json = NULL;
  rc = lc_pouch_client_public_namespace(client, ns, &resolved_namespace, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_read(client->pouch, resolved_namespace, key, &read_result,
                           error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    out->retention_support = LC_STATE_RETENTION_SUPPORTED;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_sink_to_memory(&memory_sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, memory_sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(memory_sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    json = (char *)lc_alloc_with_allocator(NULL, length + 1U);
    if (json == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mapped load buffer", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(json, bytes, length);
    json[length] = '\0';
    runtime = lc_thread_lonejson_runtime();
    rc = lc_lonejson_prepare_parse_destination(runtime, map, dst, error);
    memset(&lj_error, 0, sizeof(lj_error));
    status = rc == LC_OK ? lc_lonejson_parse_cstr_value(runtime, map, dst, json,
                                                        &lj_error)
                         : LONEJSON_STATUS_OK;
    if (status != LONEJSON_STATUS_OK) {
      rc = lc_lonejson_error_from_status(error, status, &lj_error,
                                         "failed to parse pouch mapped state");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  }
  lc_free_with_allocator(NULL, json);
  if (memory_sink != NULL) {
    memory_sink->close(memory_sink);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error);
static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src,
                                      const lc_update_opts *opts,
                                      lc_error *error);
static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error);
static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error);
static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error);

static void lc_pouch_patch_lease_methods(lc_lease *lease) {
  lease->describe = lc_pouch_lease_describe_method;
  lease->get = lc_pouch_lease_get_method;
  lease->load = lc_pouch_lease_load_method;
  lease->save = lc_pouch_lease_save_method;
  lease->update = lc_pouch_lease_update_method;
  lease->mutate = lc_pouch_lease_mutate_method;
  lease->mutate_local = lc_pouch_lease_mutate_local_method;
  lease->metadata = lc_pouch_lease_metadata_method;
  lease->remove = lc_pouch_lease_remove_method;
  lease->keepalive = lc_pouch_lease_keepalive_method;
  lease->release = lc_pouch_lease_release_method;
  lease->attach = lc_pouch_lease_attach_method;
  lease->list_attachments = lc_pouch_lease_list_attachments_method;
  lease->get_attachment = lc_pouch_lease_get_attachment_method;
  lease->delete_attachment = lc_pouch_lease_delete_attachment_method;
  lease->delete_all_attachments = lc_pouch_lease_delete_all_attachments_method;
}

static void lc_pouch_acquire_context_cleanup(lc_pouch_acquire_context *ctx) {
  if (ctx == NULL) {
    return;
  }
  lc_pouch_txn_buffer_cleanup(&ctx->lease_metadata);
  lc_pouch_state_write_result_cleanup(&ctx->client->allocator,
                                      &ctx->lease_write_result);
  ctx->held_until_unix = 0L;
  ctx->fencing_token = 0L;
  ctx->version = 0UL;
  ctx->state_found = 0;
  ctx->has_query_hidden = 0;
  ctx->query_hidden = 0;
  ctx->metadata_prepared = 0;
  ctx->acquired = 0;
  ctx->lease_id[0] = '\0';
}

static void lc_pouch_acquire_poll_delay(void) {
  struct timespec delay;

#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_acquire_poll_hook != NULL) {
    lc_pouch_test_before_acquire_poll_hook(
        lc_pouch_test_before_acquire_poll_context);
  }
#endif
  delay.tv_sec = 0;
  delay.tv_nsec = 100L * 1000L * 1000L;
  (void)nanosleep(&delay, NULL);
}

static int lc_pouch_rollback_acquire_claim(lc_pouch_acquire_context *ctx,
                                           lc_error *error) {
  lc_pouch_lease_record written_record;
  int rc;

  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || ctx->lease_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire rollback requires written lease context",
                        NULL, NULL, NULL);
  }
  memset(&written_record, 0, sizeof(written_record));
  rc = lc_pouch_read_lease_record(ctx->client, ctx->ns, ctx->req->key,
                                  &written_record, error);
  if (rc == LC_OK && written_record.found &&
      strcmp(written_record.lease_id, ctx->lease_id) == 0 &&
      written_record.fencing_token == ctx->fencing_token) {
    rc = lc_pouch_write_lease_tombstone(
        ctx->client, ctx->ns, ctx->req->key, written_record.owner,
        written_record.fencing_token, written_record.version, error);
  } else if (rc == LC_OK && ctx->lease_write_result.version > 0UL) {
    rc = lc_pouch_write_lease_tombstone(ctx->client, ctx->ns, ctx->req->key,
                                        ctx->req->owner, ctx->fencing_token,
                                        ctx->lease_write_result.version, error);
  }
  lc_pouch_lease_record_cleanup(&written_record);
  return rc;
}

static int
lc_pouch_rollback_acquire_claim_after_failure(lc_pouch_acquire_context *ctx,
                                              lc_error *error) {
  lc_error rollback_error;
  int rollback_rc;

  lc_error_init(&rollback_error);
  rollback_rc = lc_pouch_rollback_acquire_claim(ctx, &rollback_error);
  if (rollback_rc != LC_OK && error != NULL) {
    lc_error_cleanup(error);
    *error = rollback_error;
    lc_error_init(&rollback_error);
  }
  lc_error_cleanup(&rollback_error);
  return rollback_rc;
}

static int lc_pouch_acquire_prepare_metadata(
    const lc_pouch_state_metadata_view *state_view, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_acquire_context *ctx;
  lc_pouch_lease_record lease_record;
  lc_pouch_generation expected_lease_version;
  lc_pouch_unix_seconds now_seconds = 0;
  int rc;

  ctx = (lc_pouch_acquire_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || state_view == NULL || options == NULL ||
      apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire metadata preparation requires context",
                        NULL, NULL, NULL);
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(options, 0, sizeof(*options));
  *apply = 1;
  if (!state_view->has_body || state_view->retention > 0) {
    rc = lc_pouch_retention_check_reservation(ctx->client->pouch, ctx->ns,
                                              ctx->req->key, error);
    if (rc != LC_OK)
      return rc;
  }
  if (ctx->req->if_not_exists && state_view->has_body) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire if_not_exists precondition failed", NULL,
                        NULL, NULL);
  }
  ctx->state_found = state_view->found;
  ctx->version = state_view->found ? state_view->version : 0UL;
  ctx->has_query_hidden = state_view->has_query_hidden;
  ctx->query_hidden = state_view->query_hidden;
  rc = lc_pouch_lease_record_parse(state_view->metadata,
                                   state_view->metadata_length,
                                   state_view->version, &lease_record, error);
  if (rc == LC_OK && lease_record.found) {
    lease_record.has_query_hidden = state_view->has_query_hidden;
    lease_record.query_hidden = state_view->query_hidden;
  }
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_now_unix(&now_seconds, error);
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(&lease_record);
    return rc;
  }
  if (lease_record.found && lease_record.expires_at_unix > now_seconds) {
    ctx->held_until_unix = lease_record.expires_at_unix;
    lc_pouch_lease_record_cleanup(&lease_record);
    *apply = 0;
    return LC_OK;
  }
  expected_lease_version = lease_record.found ? lease_record.version : 0UL;
  rc = lc_pouch_next_fencing_token(
      lease_record.found ? lease_record.fencing_token : 0L, &ctx->fencing_token,
      error);
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(&lease_record);
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(ctx->req->ttl_seconds,
                                    &ctx->lease_expires_at_unix, error);
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(&lease_record);
    return rc;
  }
  lc_pouch_generate_lease_id(ctx->lease_id, sizeof(ctx->lease_id));
  rc = lc_pouch_lease_record_build(
      &ctx->lease_metadata, ctx->ns, ctx->req->key, ctx->req->owner,
      ctx->lease_id, ctx->req->txn_id != NULL ? ctx->req->txn_id : "",
      ctx->fencing_token, ctx->lease_expires_at_unix, ctx->version,
      ctx->txn_explicit, error);
  lc_pouch_lease_record_cleanup(&lease_record);
  if (rc != LC_OK) {
    return rc;
  }
  options->content_type = LC_POUCH_LEASE_CONTENT_TYPE;
  options->suppress_query_index = 1;
  options->has_expected_version = expected_lease_version > 0UL;
  options->expected_version = expected_lease_version;
  options->has_metadata = 1;
  options->metadata = (const unsigned char *)ctx->lease_metadata.bytes;
  options->metadata_length = ctx->lease_metadata.length;
  if (lc_pouch_client_is_queue_state_key(ctx->req->key) &&
      !ctx->has_query_hidden) {
    options->has_query_hidden = 1;
    options->query_hidden = 1;
  }
  ctx->metadata_prepared = 1;
  return rc;
}

static int lc_pouch_acquire_locked(void *context, lc_error *error) {
  lc_pouch_acquire_context *ctx;
  int rc;

  ctx = (lc_pouch_acquire_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire lock requires context", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_state_update_metadata_prepared_locked(
      ctx->client->pouch, ctx->ns, ctx->req->key,
      lc_pouch_acquire_prepare_metadata, ctx, &ctx->lease_write_result, error);
  if (rc == LC_OK && ctx->held_until_unix == 0L) {
    ctx->has_query_hidden = ctx->lease_write_result.has_query_hidden;
    ctx->query_hidden = ctx->lease_write_result.query_hidden;
    ctx->acquired = 1;
  }
  return rc;
}

int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_acquire_context acquire_context;
  lc_acquire_req effective_request;
  const char *ns = NULL;
  lc_version acquired_version = 0L;
  lc_pouch_unix_seconds block_deadline_unix = 0;
  lc_pouch_unix_seconds now_seconds = 0;
  lc_pouch_txn_guard txn_guard;
  lc_lease *lease;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int txn_explicit;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  memset(&txn_guard, 0, sizeof(txn_guard));
  txn_guard.fd = -1;
  effective_request = *req;
  txn_explicit =
      effective_request.txn_id != NULL && effective_request.txn_id[0] != '\0';
  minted_txn_id[0] = '\0';
  if (effective_request.txn_id == NULL || effective_request.txn_id[0] == '\0') {
    rc = lc_xid_new(minted_txn_id, error);
    if (rc != LC_OK) {
      return rc;
    }
    effective_request.txn_id = minted_txn_id;
  } else if (!lc_xid_is_valid(effective_request.txn_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_id must be a valid xid", NULL, NULL, NULL);
  }
  rc = lc_pouch_client_validate_acquire_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_validate_owner(req->owner, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->ttl_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds must be positive", NULL, NULL, NULL);
  }
  if (req->block_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire block_seconds must be non-negative",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&acquire_context, 0, sizeof(acquire_context));
  acquire_context.client = client;
  acquire_context.req = &effective_request;
  acquire_context.ns = ns;
  acquire_context.txn_explicit = txn_explicit;
  rc = lc_pouch_now_unix(&now_seconds, error);
  if (rc != LC_OK) {
    return rc;
  }
  block_deadline_unix = now_seconds;
  if (req->block_seconds > 0L) {
    rc = lc_pouch_timestamp_add(now_seconds, req->block_seconds,
                                "block_seconds", &block_deadline_unix, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (;;) {
    lc_pouch_acquire_context_cleanup(&acquire_context);
    if (txn_explicit) {
      rc = lc_pouch_txn_guard_acquire(client->pouch, effective_request.txn_id,
                                      &txn_guard, error);
      if (rc != LC_OK) {
        break;
      }
    }
    rc = lc_pouch_state_with_key_lock(client->pouch, ns, req->key,
                                      lc_pouch_acquire_locked, &acquire_context,
                                      error);
    if (rc != LC_OK && acquire_context.metadata_prepared) {
      lc_error rollback_error;

      lc_error_init(&rollback_error);
      (void)lc_pouch_rollback_acquire_claim(&acquire_context, &rollback_error);
      lc_error_cleanup(&rollback_error);
    }
    if (rc != LC_OK || acquire_context.acquired) {
      break;
    }
    lc_pouch_txn_guard_release(&txn_guard);
    if (req->block_seconds <= 0L) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L, "pouch lease already held",
                        NULL, NULL, NULL);
      break;
    }
    rc = lc_pouch_now_unix(&now_seconds, error);
    if (rc != LC_OK) {
      break;
    }
    if (now_seconds >= block_deadline_unix) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L, "pouch lease already held",
                        NULL, NULL, NULL);
      break;
    }
    lc_pouch_acquire_poll_delay();
  }
  if (rc != LC_OK) {
    if (acquire_context.acquired) {
      lc_error rollback_error;

      lc_error_init(&rollback_error);
      (void)lc_pouch_rollback_acquire_claim(&acquire_context, &rollback_error);
      lc_error_cleanup(&rollback_error);
    }
    lc_pouch_acquire_context_cleanup(&acquire_context);
    lc_pouch_txn_guard_release(&txn_guard);
    return rc;
  }
  rc = lc_pouch_generation_to_version(acquire_context.version,
                                      &acquired_version, error);
  if (rc != LC_OK) {
    int rollback_rc;

    rollback_rc =
        lc_pouch_rollback_acquire_claim_after_failure(&acquire_context, error);
    lc_pouch_acquire_context_cleanup(&acquire_context);
    lc_pouch_txn_guard_release(&txn_guard);
    return rollback_rc != LC_OK ? rollback_rc : rc;
  }
  if (txn_explicit) {
    rc = lc_pouch_prepare_implicit_transaction(
        self, client, effective_request.txn_id, ns, effective_request.key,
        acquire_context.lease_expires_at_unix, error);
    if (rc != LC_OK) {
      int rollback_rc;

      rollback_rc = lc_pouch_rollback_acquire_claim_after_failure(
          &acquire_context, error);
      lc_pouch_acquire_context_cleanup(&acquire_context);
      lc_pouch_txn_guard_release(&txn_guard);
      return rollback_rc != LC_OK ? rollback_rc : rc;
    }
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_after_acquire_claim_hook != NULL) {
    lc_pouch_test_after_acquire_claim_hook(
        lc_pouch_test_after_acquire_claim_context);
  }
#endif
  lease = lc_lease_new(
      client, ns, effective_request.key, effective_request.owner,
      acquire_context.lease_id, effective_request.txn_id,
      acquire_context.fencing_token, acquired_version,
      acquire_context.state_found ? acquire_context.lease_write_result.etag
                                  : NULL,
      NULL);
  if (lease == NULL) {
    int rollback_rc;

    rollback_rc =
        lc_pouch_rollback_acquire_claim_after_failure(&acquire_context, error);
    lc_pouch_acquire_context_cleanup(&acquire_context);
    lc_pouch_txn_guard_release(&txn_guard);
    if (rollback_rc != LC_OK) {
      return rollback_rc;
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease", NULL, NULL, NULL);
  }
  lc_pouch_lease_refresh_expiration((lc_lease_handle *)lease,
                                    acquire_context.lease_expires_at_unix);
  lc_pouch_lease_refresh_query_metadata((lc_lease_handle *)lease,
                                        acquire_context.has_query_hidden,
                                        acquire_context.query_hidden);
  lc_pouch_lease_refresh_retention(
      (lc_lease_handle *)lease, acquire_context.lease_write_result.has_body,
      acquire_context.lease_write_result.retention);
  lc_pouch_acquire_context_cleanup(&acquire_context);
  lc_pouch_txn_guard_release(&txn_guard);
  lc_pouch_patch_lease_methods(lease);
  *out = lease;
  return LC_OK;
}

int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *context, lc_error *error) {
  lc_client_handle *client;
  lc_lease *lease;
  lc_lease_handle *lease_handle;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error handler_error;
  lc_error release_error;
  lc_pouch_acquire_for_update_file file;
  lc_sink sink;
  lc_acquire_for_update_context update;
  lc_lease_ref lease_ref;
  FILE *fp;
  int (*original_update)(lc_lease *, lc_source *, const lc_update_opts *,
                         lc_error *);
  const char *stage_txn_id;
  int discarded;
  int rc;
  int release_rc;

  if (self == NULL || req == NULL || handler == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update requires self, req, and "
                        "handler",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  lease = NULL;
  fp = NULL;
  original_update = NULL;
  stage_txn_id = NULL;
  discarded = 0;
  memset(&get_res, 0, sizeof(get_res));
  lc_get_opts_init(&get_opts);
  lc_release_req_init(&release_req);
  lc_error_init(&handler_error);
  lc_error_init(&release_error);
  memset(&file, 0, sizeof(file));
  memset(&sink, 0, sizeof(sink));
  memset(&update, 0, sizeof(update));
  lc_lease_ref_init(&lease_ref);

  rc = lc_pouch_client_acquire_method(self, req, &lease, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  lease_handle = (lc_lease_handle *)lease;
  stage_txn_id = lease_handle->txn_id != NULL && lease_handle->txn_id[0] != '\0'
                     ? lease_handle->txn_id
                     : lease_handle->lease_id;
  original_update = lease->update;

  fp = tmpfile();
  if (fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  file.fp = fp;
  sink.write = lc_pouch_acquire_for_update_sink_write;
  sink.close = lc_pouch_acquire_for_update_sink_close;
  sink.impl = &file;
  rc = lease->get(lease, &sink, &get_opts, &get_res, error);
  if (rc != LC_OK) {
    goto release_and_cleanup;
  }
  if (fflush(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  if (fseek(fp, 0L, SEEK_SET) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to rewind pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }

  update.lease = lease;
  update.state.has_state = !get_res.no_content;
  update.state.content_type = get_res.content_type;
  update.state.etag = get_res.etag;
  update.state.version = get_res.version;
  update.state.fencing_token = get_res.fencing_token;
  update.state.correlation_id = get_res.correlation_id;
  update.state.retention_support = get_res.retention_support;
  update.state.has_retention_expires_at_unix =
      get_res.has_retention_expires_at_unix;
  update.state.retention_expires_at_unix = get_res.retention_expires_at_unix;
  if (!get_res.no_content) {
    rc = lc_source_from_callbacks(lc_pouch_acquire_for_update_source_read,
                                  lc_pouch_acquire_for_update_source_reset,
                                  NULL, &file, &update.state.reader, error);
    if (rc != LC_OK) {
      goto release_and_cleanup;
    }
  }

  lease_handle->pouch_stage_active = 1;
  lease_handle->pouch_stage_dirty = 0;
  lc_client_free(client, lease_handle->pouch_stage_etag);
  lease_handle->pouch_stage_etag = NULL;
  lease_handle->pouch_stage_version = 0L;
  lease->update = lc_pouch_lease_staged_update_method;
  rc = handler(context, &update, &handler_error);
  lease->update = original_update;
  original_update = NULL;
  lease_handle->pouch_stage_active = 0;
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
    update.state.reader = NULL;
  }
  if (rc != LC_OK) {
    if (error != NULL) {
      *error = handler_error;
      lc_error_init(&handler_error);
    }
    release_req.rollback = 1;
    if (lease_handle->pouch_stage_dirty) {
      lc_error discard_error;

      lc_error_init(&discard_error);
      rc = lc_pouch_state_discard_staged(client->pouch, lease_handle->ns,
                                         lease_handle->key, stage_txn_id,
                                         &discarded, &discard_error);
      if (rc != LC_OK && error != NULL && error->code == LC_OK) {
        *error = discard_error;
        lc_error_init(&discard_error);
      }
      lc_error_cleanup(&discard_error);
    }
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  } else {
    /* A minted lease is decided by release.  Promoting it here would bypass
     * the release-time lease/fencing/expiry validation and could publish a
     * callback result after the lease had expired. */
    lease_ref.ns = lease_handle->ns;
    lease_ref.key = lease_handle->key;
    lease_ref.lease_id = lease_handle->lease_id;
    lease_ref.txn_id = lease_handle->txn_id;
    lease_ref.fencing_token = lease_handle->fencing_token;
    rc = lc_pouch_validate_lease_record(client, &lease_ref, lease_handle->ns,
                                        lease_handle->key, NULL, error);
    if (rc != LC_OK) {
      release_req.rollback = 1;
    }
  }

release_and_cleanup:
  if (lease != NULL && original_update != NULL) {
    lease->update = original_update;
  }
  if (lease != NULL) {
    ((lc_lease_handle *)lease)->pouch_stage_active = 0;
  }
  release_rc =
      lease != NULL
          ? lc_pouch_lease_release_method(lease, &release_req, &release_error)
          : LC_OK;
  if (release_rc != LC_OK && rc == LC_OK) {
    rc = release_rc;
    if (error != NULL) {
      *error = release_error;
      lc_error_init(&release_error);
    }
  }
  if (release_rc == LC_OK) {
    lease = NULL;
  } else if (lease != NULL) {
    lc_lease_close(lease);
    lease = NULL;
  }

cleanup:
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
  }
  if (fp != NULL) {
    fclose(fp);
  }
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&handler_error);
  lc_error_cleanup(&release_error);
  return rc;
}

int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_lease_record lease_record;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_read_result staged_result;
  const char *ns = NULL;
  lc_pouch_unix_seconds now_seconds = 0;
  char *staged_key;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch describe requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&staged_result, 0, sizeof(staged_result));
  staged_key = NULL;
  rc = lc_pouch_client_validate_public_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read_metadata(client->pouch, ns, req->key, &read_result,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch key metadata was not found", NULL, "not_found",
                        NULL);
  }
  memset(&lease_record, 0, sizeof(lease_record));
  rc = lc_pouch_lease_record_parse(read_result.metadata,
                                   read_result.metadata_length,
                                   read_result.version, &lease_record, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return rc;
  }
  if (lease_record.found) {
    lease_record.has_query_hidden = read_result.has_query_hidden;
    lease_record.query_hidden = read_result.query_hidden;
  }
  out->ns = lc_strdup_local(ns);
  out->key = lc_strdup_local(req->key);
  out->state_etag = lc_strdup_local(read_result.etag);
  rc = lc_pouch_now_unix(&now_seconds, error);
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(&lease_record);
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return rc;
  }
  if (lease_record.found && lease_record.expires_at_unix > now_seconds) {
    out->owner = lc_strdup_local(lease_record.owner);
    out->lease_id = lc_strdup_local(lease_record.lease_id);
    out->txn_id = lc_strdup_local(lease_record.txn_id);
    out->lease_expires_at_unix = lease_record.expires_at_unix;
    out->fencing_token = lease_record.fencing_token;
  }
  if (out->ns == NULL || out->key == NULL || out->state_etag == NULL ||
      (lease_record.found && lease_record.expires_at_unix > now_seconds &&
       (out->owner == NULL || out->lease_id == NULL || out->txn_id == NULL))) {
    lc_pouch_lease_record_cleanup(&lease_record);
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    lc_describe_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch describe response", NULL,
                        NULL, NULL);
  }
  rc =
      lc_pouch_generation_to_version(read_result.version, &out->version, error);
  if (rc != LC_OK) {
    lc_pouch_lease_record_cleanup(&lease_record);
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    lc_describe_res_cleanup(out);
    return rc;
  }
  out->has_query_hidden = read_result.has_query_hidden;
  out->query_hidden = read_result.query_hidden;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix =
      read_result.has_body && read_result.retention >= 0;
  out->retention_expires_at_unix =
      out->has_retention_expires_at_unix ? read_result.retention : 0;
  if (lease_record.found && lease_record.expires_at_unix > now_seconds &&
      lc_pouch_txn_id_present(lease_record.txn_id)) {
    staged_key =
        lc_pouch_staged_storage_key(req->key, lease_record.txn_id, error);
    if (staged_key == NULL) {
      lc_pouch_lease_record_cleanup(&lease_record);
      lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
      lc_describe_res_cleanup(out);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_state_read(client->pouch, ns, staged_key, &staged_result,
                             error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, staged_key);
      lc_pouch_lease_record_cleanup(&lease_record);
      lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
      lc_describe_res_cleanup(out);
      return rc;
    }
    if (staged_result.found &&
        !lc_pouch_state_result_is_delete_marker(&staged_result)) {
      char *staged_etag;

      staged_etag = lc_strdup_local(staged_result.etag);
      if (staged_etag == NULL) {
        lc_free_with_allocator(NULL, staged_key);
        lc_pouch_state_read_result_cleanup(&client->allocator, &staged_result);
        lc_pouch_lease_record_cleanup(&lease_record);
        lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
        lc_describe_res_cleanup(out);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch staged describe etag",
                            NULL, NULL, "pouch");
      }
      lc_free_local(out->state_etag);
      out->state_etag = staged_etag;
      rc = lc_pouch_generation_to_version(staged_result.version, &out->version,
                                          error);
      if (rc != LC_OK) {
        lc_free_with_allocator(NULL, staged_key);
        lc_pouch_state_read_result_cleanup(&client->allocator, &staged_result);
        lc_pouch_lease_record_cleanup(&lease_record);
        lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
        lc_describe_res_cleanup(out);
        return rc;
      }
      out->has_query_hidden = staged_result.has_query_hidden;
      out->query_hidden = staged_result.query_hidden;
      out->has_retention_expires_at_unix =
          staged_result.has_body && staged_result.retention >= 0;
      out->retention_expires_at_unix =
          out->has_retention_expires_at_unix ? staged_result.retention : 0;
    }
  }
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged_result);
  lc_pouch_lease_record_cleanup(&lease_record);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return LC_OK;
}

static int lc_pouch_client_get_method_impl(lc_client *self, const char *key,
                                           const lc_get_opts *opts,
                                           lc_sink *dst, lc_get_res *out,
                                           lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_get_namespace(client, NULL, key, opts, dst, out,
                                       error);
}

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_get_method_impl(self, key, opts, dst, out, error);
  /* A caller-owned sink can close its receiver while Pouch is streaming. Pin
   * the concrete client until the state read and response cleanup unwind. */
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_get_method_impl(self, key, opts, dst, out, error);
  lc_client_handle_release(client);
  return rc;
}

static int lc_pouch_client_get_in_namespace_impl(
    lc_client *self, const char *ns, const char *key, const lc_get_opts *opts,
    lc_sink *dst, lc_get_res *out, lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespaced get requires namespace, key, dst, "
                        "and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(key, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_client_get_namespace(client, ns, key, opts, dst, out, error);
}

int lc_pouch_client_get_in_namespace(lc_client *self, const char *ns,
                                     const char *key, const lc_get_opts *opts,
                                     lc_sink *dst, lc_get_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL) {
    return lc_pouch_client_get_in_namespace_impl(self, ns, key, opts, dst, out,
                                                 error);
  }
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_get_in_namespace_impl(self, ns, key, opts, dst, out,
                                             error);
  lc_client_handle_release(client);
  return rc;
}

int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch load requires self, key, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_load_namespace(client, NULL, key, map, dst, opts, out,
                                        error);
}

int lc_pouch_client_load_in_namespace(lc_client *self, const char *ns,
                                      const char *key, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespaced load requires namespace, key, map, "
                        "destination, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_load_namespace(client, ns, key, map, dst, opts, out,
                                        error);
}

static int lc_pouch_client_update_method_impl(lc_client *self,
                                              const lc_update_req *req,
                                              lc_source *src,
                                              lc_update_res *out,
                                              lc_error *error) {
  lc_client_handle *client;
  lc_pouch_counting_source counting_source;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_source *counted_source;
  const char *ns = NULL;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch update requires self, req with key, src, and "
                        "out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&counting_source, 0, sizeof(counting_source));
  memset(&lease_precondition, 0, sizeof(lease_precondition));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  counted_source = NULL;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  options.content_type =
      req->content_type != NULL ? req->content_type : "application/json";
  rc = lc_pouch_retention_options(&options, req->has_retention_seconds,
                                  req->retention_seconds, ns, req->lease.key,
                                  error);
  if (rc != LC_OK)
    return rc;
  options.query_index_operation_active = 1;
  options.query_index_operation_id = req->lease.lease_id;
  options.expected_etag = req->if_state_etag;
  lease_precondition.client = client;
  lease_precondition.lease = &req->lease;
  lease_precondition.ns = ns;
  lease_precondition.key = req->lease.key;
  options.query_index_operation_expires_at_unix =
      &lease_precondition.lease_expires_at_unix;
  if (!lc_pouch_txn_id_present(req->lease.txn_id)) {
    options.view_precondition = lc_pouch_lease_view_precondition_check;
    options.view_precondition_context = &lease_precondition;
  }
  if (req->has_if_version) {
    rc = lc_pouch_version_to_generation(req->if_version,
                                        &options.expected_version, error);
    if (rc != LC_OK) {
      return rc;
    }
    options.has_expected_version = 1;
  }
  /* The generic response reports a long byte count on every architecture. */
  counting_source.inner = src;
  counting_source.max_bytes = LONG_MAX;
  counting_source.has_max_bytes = 1;
  rc = lc_source_from_callbacks(lc_pouch_counting_source_read,
                                lc_pouch_counting_source_reset, NULL,
                                &counting_source, &counted_source, error);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
    return rc;
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_stage_transaction_write(
        client, &req->lease, ns, req->lease.key, req->lease.txn_id,
        counted_source, &options, &write_result, NULL, error);
  } else {
    rc = lc_pouch_state_write(client->pouch, ns, req->lease.key, counted_source,
                              &options, &write_result, error);
  }
  lc_source_close(counted_source);
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_update_metadata(&write_result, out, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_update_method_impl(self, req, src, out, error);
  /* Request leases are value credentials, not necessarily a live lease
   * handle. A source callback can therefore be the only re-entrant owner. */
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_update_method_impl(self, req, src, out, error);
  lc_client_handle_release(client);
  return rc;
}

int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_mutation_prepare_context direct_mutation;
  lc_pouch_mutation_prepare_context *mutation;
  lc_pouch_txn_mutation_context txn_mutation;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  const char *ns = NULL;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&lease_precondition, 0, sizeof(lease_precondition));
  memset(&direct_mutation, 0, sizeof(direct_mutation));
  memset(&txn_mutation, 0, sizeof(txn_mutation));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  mutation = NULL;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  mutation = lc_pouch_txn_id_present(req->lease.txn_id) ? &txn_mutation.mutation
                                                        : &direct_mutation;
  mutation->client = client;
  mutation->ns = ns;
  mutation->key = req->lease.key;
  rc = lc_pouch_retention_options(&options, req->has_retention_seconds,
                                  req->retention_seconds, ns, req->lease.key,
                                  error);
  if (rc != LC_OK)
    goto cleanup;
  rc = lc_mutation_plan_build(req->mutations, req->mutation_count, NULL,
                              &mutation->plan, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  options.content_type = "application/json";
  options.query_index_operation_active = 1;
  options.query_index_operation_id = req->lease.lease_id;
  options.expected_etag = req->if_state_etag;
  lease_precondition.client = client;
  lease_precondition.lease = &req->lease;
  lease_precondition.ns = ns;
  lease_precondition.key = req->lease.key;
  options.query_index_operation_expires_at_unix =
      &lease_precondition.lease_expires_at_unix;
  if (!lc_pouch_txn_id_present(req->lease.txn_id)) {
    options.view_precondition = lc_pouch_lease_view_precondition_check;
    options.view_precondition_context = &lease_precondition;
  }
  if (req->has_if_version) {
    rc = lc_pouch_version_to_generation(req->if_version,
                                        &options.expected_version, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    options.has_expected_version = 1;
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    txn_mutation.stage.client = client;
    txn_mutation.stage.lease = &req->lease;
    txn_mutation.stage.ns = ns;
    txn_mutation.stage.key = req->lease.key;
    txn_mutation.stage.txn_id = req->lease.txn_id;
    rc = lc_pouch_client_stage_transaction_mutation(&txn_mutation, &options,
                                                    &write_result, error);
  } else {
    rc = lc_pouch_state_write_prepared(client->pouch, ns, req->lease.key,
                                       &options,
                                       lc_pouch_client_prepare_mutation_source,
                                       mutation, &write_result, error);
  }
  if (mutation->source != NULL) {
    lc_source_close(mutation->source);
    mutation->source = NULL;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_mutate_metadata(&write_result, out, error);
  }

cleanup:
  if (mutation != NULL && mutation->source != NULL) {
    lc_source_close(mutation->source);
  }
  if (mutation != NULL) {
    lc_pouch_mutate_file_cleanup(&mutation->mutated);
    if (mutation->plan != NULL) {
      lc_mutation_plan_close(mutation->plan);
    }
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                                    lc_metadata_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *ns = NULL;
  char *namespace_copy;
  char *key_copy;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (!req->has_query_hidden && !req->has_retention_seconds) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch metadata requires query_hidden or retention_seconds", NULL, NULL,
        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&lease_precondition, 0, sizeof(lease_precondition));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.has_query_hidden = req->has_query_hidden;
  options.query_hidden = req->query_hidden;
  options.query_index_operation_active = 1;
  options.query_index_operation_id = req->lease.lease_id;
  if (req->has_if_version) {
    rc = lc_pouch_version_to_generation(req->if_version,
                                        &options.expected_version, error);
    if (rc != LC_OK) {
      return rc;
    }
    options.has_expected_version = 1;
  }
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  rc = lc_pouch_retention_options(&options, req->has_retention_seconds,
                                  req->retention_seconds, ns, req->lease.key,
                                  error);
  if (rc != LC_OK)
    return rc;
  lease_precondition.client = client;
  lease_precondition.lease = &req->lease;
  lease_precondition.ns = ns;
  lease_precondition.key = req->lease.key;
  options.query_index_operation_expires_at_unix =
      &lease_precondition.lease_expires_at_unix;
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_stage_transaction_metadata(
        client, &req->lease, ns, req->lease.key, req->lease.txn_id, &options,
        &result, error);
  } else {
    options.view_precondition = lc_pouch_lease_view_precondition_check;
    options.view_precondition_context = &lease_precondition;
    rc = lc_pouch_state_update_metadata(client->pouch, ns, req->lease.key,
                                        &options, &result, error);
  }
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  namespace_copy = lc_strdup_local(ns);
  key_copy = lc_strdup_local(req->lease.key);
  if (namespace_copy == NULL || key_copy == NULL) {
    lc_free_local(namespace_copy);
    lc_free_local(key_copy);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata response", NULL,
                        NULL, NULL);
  }
  out->ns = namespace_copy;
  out->key = key_copy;
  rc = lc_pouch_generation_to_version(result.version, &out->version, error);
  if (rc != LC_OK) {
    lc_metadata_res_cleanup(out);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  out->has_query_hidden = result.has_query_hidden;
  out->query_hidden = result.query_hidden;
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix = result.has_body && result.retention >= 0;
  out->retention_expires_at_unix =
      out->has_retention_expires_at_unix ? result.retention : 0;
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return LC_OK;
}

int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *ns = NULL;
  int staged_removed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch remove requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&lease_precondition, 0, sizeof(lease_precondition));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  staged_removed = 0;
  options.query_index_operation_active = 1;
  options.query_index_operation_id = req->lease.lease_id;
  options.expected_etag = req->if_state_etag;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->has_if_version) {
    rc = lc_pouch_version_to_generation(req->if_version,
                                        &options.expected_version, error);
    if (rc != LC_OK) {
      return rc;
    }
    options.has_expected_version = 1;
  }
  lease_precondition.client = client;
  lease_precondition.lease = &req->lease;
  lease_precondition.ns = ns;
  lease_precondition.key = req->lease.key;
  options.query_index_operation_expires_at_unix =
      &lease_precondition.lease_expires_at_unix;
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_stage_transaction_remove(
        client, &req->lease, ns, req->lease.key, req->lease.txn_id, &options,
        &result, &staged_removed, error);
    if (rc == LC_OK) {
      out->removed = staged_removed;
    }
  } else {
    options.view_precondition = lc_pouch_lease_view_precondition_check;
    options.view_precondition_context = &lease_precondition;
    rc = lc_pouch_state_delete(client->pouch, ns, req->lease.key, &options,
                               &result, error);
    if (rc == LC_OK) {
      out->removed = result.version > 0UL;
      /* Removing a non-transactional document consumes its lease metadata,
       * so remove itself is the final operation boundary for foreground
       * indexing. A later lease release cannot observe this lease. */
      lc_pouch_indexer_note_operation_complete(
          client->pouch, ns, req->lease.key, req->lease.lease_id);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_generation_to_version(result.version, &out->new_version,
                                        error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_keepalive_locked(void *context, lc_error *error);

int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_keepalive_context keepalive_context;
  lc_pouch_state_read_result staged_state;
  const char *ns = NULL;
  lc_pouch_unix_seconds lease_expires_at_unix = 0;
  char *state_etag = NULL;
  char *staged_key = NULL;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(req->ttl_seconds, &lease_expires_at_unix,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_lease_ref_has_credentials(&req->lease)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive requires lease_id", NULL, NULL, NULL);
  }
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&keepalive_context, 0, sizeof(keepalive_context));
  memset(&staged_state, 0, sizeof(staged_state));
  keepalive_context.client = client;
  keepalive_context.req = req;
  keepalive_context.ns = ns;
  keepalive_context.lease_expires_at_unix = lease_expires_at_unix;
  rc = lc_pouch_state_with_key_lock(client->pouch, ns, req->lease.key,
                                    lc_pouch_keepalive_locked,
                                    &keepalive_context, error);
  if (rc == LC_OK) {
    lc_pouch_query_index_operation_refresh(
        client->pouch, ns, req->lease.key,
        lc_pouch_txn_id_present(req->lease.txn_id) ? req->lease.txn_id
                                                   : req->lease.lease_id,
        keepalive_context.lease_expires_at_unix);
  }
  if (rc == LC_OK && keepalive_context.write_result.etag != NULL) {
    state_etag = lc_strdup_local(keepalive_context.write_result.etag);
  }
  if (rc == LC_OK && keepalive_context.write_result.etag != NULL &&
      state_etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch keepalive state etag", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    memset(out, 0, sizeof(*out));
    out->lease_expires_at_unix = keepalive_context.lease_expires_at_unix;
    /* Retain the same version floor used when preparing a transaction stage:
     * immediate writes can advance the body beyond the lease snapshot, while
     * migrated extended leases can carry a higher snapshot than their body.
     * A transaction-local stage below still supplies its own visible version.
     */
    rc = lc_pouch_generation_to_version(keepalive_context.state_version,
                                        &out->version, error);
  }
  if (rc == LC_OK && lc_pouch_txn_id_present(req->lease.txn_id)) {
    staged_key =
        lc_pouch_staged_storage_key(req->lease.key, req->lease.txn_id, error);
    if (staged_key == NULL) {
      rc = error != NULL ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && staged_key != NULL) {
    rc = lc_pouch_state_read(client->pouch, ns, staged_key, &staged_state,
                             error);
  }
  if (rc == LC_OK && staged_state.found) {
    if (lc_pouch_state_result_is_delete_marker(&staged_state)) {
      /* A staged delete is the transaction-local absent-state view. Do not
       * leak its private generation into the lease: the public lease helpers
       * would turn it into a conditional update against the committed body. */
      lc_free_local(state_etag);
      state_etag = NULL;
      out->version = 0L;
    } else {
      rc = lc_pouch_generation_to_version(staged_state.version, &out->version,
                                          error);
      if (rc == LC_OK && staged_state.etag != NULL) {
        char *staged_etag = lc_strdup_local(staged_state.etag);

        if (staged_etag == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch staged state etag", NULL,
                            NULL, NULL);
        } else {
          lc_free_local(state_etag);
          state_etag = staged_etag;
        }
      }
    }
  }
  if (rc == LC_OK) {
    out->state_etag = state_etag;
    state_etag = NULL;
  }
  lc_free_local(state_etag);
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged_state);
  lc_pouch_txn_buffer_cleanup(&keepalive_context.lease_metadata);
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &keepalive_context.write_result);
  return rc;
}

static int lc_pouch_keepalive_prepare_metadata(
    const lc_pouch_state_metadata_view *state_view, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_keepalive_context *ctx;
  lc_pouch_lease_record lease_record;
  int rc;

  ctx = (lc_pouch_keepalive_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || state_view == NULL || options == NULL ||
      apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive metadata preparation requires context",
                        NULL, NULL, "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(options, 0, sizeof(*options));
  *apply = 1;
  rc = lc_pouch_validate_lease_metadata_view(ctx->client, &ctx->req->lease,
                                             ctx->ns, ctx->req->lease.key,
                                             state_view, &lease_record, error);
  if (rc == LC_OK) {
    if (lease_record.expires_at_unix > ctx->lease_expires_at_unix) {
      ctx->lease_expires_at_unix = lease_record.expires_at_unix;
    }
    ctx->state_version = lease_record.state_version;
    if (state_view->version > ctx->state_version) {
      ctx->state_version = state_view->version;
    }
    rc = lc_pouch_lease_record_build(
        &ctx->lease_metadata, ctx->ns, ctx->req->lease.key, lease_record.owner,
        lease_record.lease_id, lease_record.txn_id, lease_record.fencing_token,
        ctx->lease_expires_at_unix, lease_record.state_version,
        lease_record.txn_explicit, error);
  }
  if (rc == LC_OK) {
    options->content_type = LC_POUCH_LEASE_CONTENT_TYPE;
    options->suppress_query_index = 1;
    options->has_expected_version = lease_record.version > 0UL;
    options->expected_version = lease_record.version;
    options->has_metadata = 1;
    options->metadata = (const unsigned char *)ctx->lease_metadata.bytes;
    options->metadata_length = ctx->lease_metadata.length;
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_keepalive_locked(void *context, lc_error *error) {
  lc_pouch_keepalive_context *ctx;

  ctx = (lc_pouch_keepalive_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive lock requires context", NULL, NULL,
                        NULL);
  }
  return lc_pouch_state_update_metadata_prepared_locked(
      ctx->client->pouch, ctx->ns, ctx->req->lease.key,
      lc_pouch_keepalive_prepare_metadata, ctx, &ctx->write_result, error);
}

static int lc_pouch_release_prepare_metadata(
    const lc_pouch_state_metadata_view *state_view, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error) {
  lc_pouch_release_context *ctx;
  lc_pouch_lease_record lease_record;
  int rc;

  ctx = (lc_pouch_release_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || state_view == NULL || options == NULL ||
      apply == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release metadata preparation requires context",
                        NULL, NULL, "pouch");
  }
  memset(&lease_record, 0, sizeof(lease_record));
  memset(options, 0, sizeof(*options));
  *apply = 1;
  rc = lc_pouch_validate_lease_metadata_view(ctx->client, &ctx->req->lease,
                                             ctx->ns, ctx->req->lease.key,
                                             state_view, &lease_record, error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_record_build(&ctx->lease_metadata, ctx->ns,
                                     ctx->req->lease.key, lease_record.owner,
                                     "", "", lease_record.fencing_token, 0L,
                                     lease_record.state_version, 0, error);
  }
  if (rc == LC_OK) {
    options->content_type = LC_POUCH_LEASE_CONTENT_TYPE;
    options->suppress_query_index = 1;
    options->has_expected_version = lease_record.version > 0UL;
    options->expected_version = lease_record.version;
    options->has_metadata = 1;
    options->metadata = (const unsigned char *)ctx->lease_metadata.bytes;
    options->metadata_length = ctx->lease_metadata.length;
  }
  lc_pouch_lease_record_cleanup(&lease_record);
  return rc;
}

static int lc_pouch_release_locked(void *context, lc_error *error) {
  lc_pouch_release_context *ctx;

  ctx = (lc_pouch_release_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release lock requires context", NULL, NULL,
                        NULL);
  }
  return lc_pouch_state_update_metadata_prepared_locked(
      ctx->client->pouch, ctx->ns, ctx->req->lease.key,
      lc_pouch_release_prepare_metadata, ctx, &ctx->write_result, error);
}

int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_release_context release_context;
  lc_pouch_lease_record lease_record;
  lc_pouch_txn_guard txn_guard;
  lc_txn_replay_req replay_request;
  lc_txn_replay_res replay_result;
  const char *ns = NULL;
  lc_pouch_unix_seconds now_seconds;
  int txn_expired;
  int txn_record_exists;
  int lease_matches;
  int decision_ready;
  int implicit;
  int native_completion;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_lease_ref_has_credentials(&req->lease)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires lease_id", NULL, NULL, NULL);
  }
  memset(&release_context, 0, sizeof(release_context));
  memset(&lease_record, 0, sizeof(lease_record));
  memset(&txn_guard, 0, sizeof(txn_guard));
  txn_guard.fd = -1;
  lc_txn_replay_req_init(&replay_request);
  memset(&replay_result, 0, sizeof(replay_result));
  now_seconds = 0L;
  txn_expired = 0;
  txn_record_exists = 0;
  lease_matches = 0;
  decision_ready = 0;
  implicit = native_completion = 0;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_txn_guard_acquire(client->pouch, req->lease.txn_id,
                                    &txn_guard, error);
    if (rc == LC_OK) {
      rc = lc_pouch_txn_record_exists(client, req->lease.txn_id,
                                      &txn_record_exists, error);
    }
    if (rc == LC_OK && txn_record_exists) {
      rc = lc_pouch_read_lease_record(client, ns, req->lease.key, &lease_record,
                                      error);
    }
    if (rc == LC_OK && txn_record_exists && lease_record.found &&
        strcmp(lease_record.lease_id, req->lease.lease_id) == 0 &&
        lease_record.fencing_token == req->lease.fencing_token &&
        strcmp(lease_record.txn_id, req->lease.txn_id) == 0) {
      lease_matches = 1;
      implicit = !lease_record.txn_explicit;
      rc = lc_pouch_now_unix(&now_seconds, error);
      if (rc == LC_OK && lease_record.expires_at_unix <= now_seconds) {
        txn_expired = 1;
      }
    }
    lc_pouch_lease_record_cleanup(&lease_record);
    if (rc == LC_OK && txn_record_exists && lease_matches) {
      rc = lc_pouch_txn_record_vote(
          client, req->lease.txn_id, ns, req->lease.key,
          req->rollback || txn_expired, 0, &decision_ready,
          implicit ? &native_completion : NULL, error);
    }
    if (rc == LC_OK && txn_record_exists && decision_ready) {
      replay_request.txn_id = req->lease.txn_id;
      rc = lc_pouch_client_txn_replay_impl(self, &replay_request,
                                           &replay_result, error);
      lc_txn_replay_res_cleanup(&replay_result);
    }
    if (rc == LC_OK && native_completion)
      rc = lc_pouch_retire_native_completion(client, req->lease.txn_id, ns,
                                             error);
    if (rc == LC_OK && !txn_record_exists) {
      rc = lc_pouch_txn_apply_state_participant(
          client, &req->lease, ns, req->lease.key, req->lease.txn_id,
          req->rollback ? "rollback" : "commit", 0, error);
    }
    lc_pouch_txn_guard_release(&txn_guard);
    if (rc != LC_OK) {
      return rc;
    }
    lc_pouch_indexer_note_operation_complete(client->pouch, ns, req->lease.key,
                                             req->lease.txn_id);
    memset(out, 0, sizeof(*out));
    out->released = 1;
    return LC_OK;
  }
  release_context.client = client;
  release_context.req = req;
  release_context.ns = ns;
  rc = lc_pouch_state_with_key_lock(client->pouch, ns, req->lease.key,
                                    lc_pouch_release_locked, &release_context,
                                    error);
  lc_pouch_txn_buffer_cleanup(&release_context.lease_metadata);
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &release_context.write_result);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_indexer_note_operation_complete(
      client->pouch, ns, req->lease.key,
      lc_pouch_txn_id_present(req->lease.txn_id) ? req->lease.txn_id
                                                 : req->lease.lease_id);
  memset(out, 0, sizeof(*out));
  out->released = 1;
  return LC_OK;
}

static int lc_pouch_client_attach_with_transaction_mode(
    lc_client *self, const lc_attach_op *req, lc_source *src,
    lc_attach_res *out, int stage_transaction, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attach_write_context attach_context;
  lc_pouch_counting_source counting_source;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *counted_source;
  const char *ns = NULL;
  const char *content_type;
  char *attachment_key;
  char *staged_key;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attach requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  if (req->name == NULL || req->name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attach requires attachment name", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&attach_context, 0, sizeof(attach_context));
  memset(&counting_source, 0, sizeof(counting_source));
  memset(&lease_precondition, 0, sizeof(lease_precondition));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  counted_source = NULL;
  attachment_key = NULL;
  staged_key = NULL;
  if (req->has_max_bytes && req->max_bytes < 0L) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch attachment max_bytes must be non-negative", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  /* The state-write precondition below validates this lease immediately before
   * publishing the attachment. Keeping validation there avoids a stale
   * preflight read and, for encrypted roots, a second decrypt of the parent
   * record on every attachment write. */
  content_type = req->content_type != NULL && req->content_type[0] != '\0'
                     ? req->content_type
                     : "application/octet-stream";

  attachment_key =
      lc_pouch_attachment_key(ns, req->lease.key, req->name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (stage_transaction && lc_pouch_txn_id_present(req->lease.txn_id)) {
    staged_key = lc_pouch_staged_attachment_key(ns, req->lease.key, req->name,
                                                req->lease.txn_id, error);
    if (staged_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      goto cleanup;
    }
  }
  counting_source.inner = src;
  counting_source.max_bytes = req->has_max_bytes ? req->max_bytes : LONG_MAX;
  counting_source.has_max_bytes = 1;
  rc = lc_source_from_callbacks(lc_pouch_counting_source_read,
                                lc_pouch_counting_source_reset, NULL,
                                &counting_source, &counted_source, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  options.content_type = content_type;
  options.create_if_absent = req->prevent_overwrite;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.object_record = 1;
  lease_precondition.client = client;
  lease_precondition.lease = &req->lease;
  lease_precondition.ns = ns;
  lease_precondition.key = req->lease.key;
  options.precondition = lc_pouch_lease_precondition_check;
  options.precondition_context = &lease_precondition;
  attach_context.client = client;
  attach_context.req = req;
  attach_context.ns = ns;
  attach_context.attachment_key = attachment_key;
  attach_context.staged_attachment_key = staged_key;
  attach_context.stage_transaction = stage_transaction;
  attach_context.source = counted_source;
  attach_context.options = &options;
  attach_context.result = &result;
  rc = lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_attach_write_locked, &attach_context, error);
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_info_fill(
        &out->attachment, req->name, counting_source.bytes, content_type,
        attach_context.attachment_created_at_unix, result.updated_at_unix,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_generation_to_version(result.version, &out->version, error);
  }
  if (rc == LC_OK) {
    out->correlation_id = lc_strdup_local("pouch-attachment-put");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment response", NULL,
                        NULL, NULL);
    }
  }

cleanup:
  if (counted_source != NULL) {
    lc_source_close(counted_source);
  }
  lc_free_with_allocator(NULL, attachment_key);
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  if (rc != LC_OK) {
    lc_attach_res_cleanup(out);
  }
  return rc;
}

static int lc_pouch_client_attach_method_impl(lc_client *self,
                                              const lc_attach_op *req,
                                              lc_source *src,
                                              lc_attach_res *out,
                                              lc_error *error) {
  return lc_pouch_client_attach_with_transaction_mode(self, req, src, out, 1,
                                                      error);
}

static int lc_pouch_client_list_attachments_with_transaction_mode(
    lc_client *self, const lc_attachment_list_req *req, lc_attachment_list *out,
    int stage_transaction, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attachment_list_builder builder;
  const char *ns = NULL;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch list_attachments requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&builder, 0, sizeof(builder));
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!req->public_read) {
    if (!lc_pouch_lease_ref_has_credentials(&req->lease)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch private attachment read requires lease_id",
                          NULL, NULL, NULL);
    }
    rc = lc_pouch_validate_lease_record(client, &req->lease, ns, req->lease.key,
                                        NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc =
      lc_pouch_collect_attachments(client, ns, req->lease.key, &builder, error);
  if (rc == LC_OK && stage_transaction && !req->public_read &&
      lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_overlay_staged_attachments(
        client, ns, req->lease.key, req->lease.txn_id, &builder, error);
  }
  if (rc == LC_OK) {
    out->items = builder.items;
    out->count = builder.count;
    out->correlation_id = lc_strdup_local("pouch-attachment-list");
    builder.items = NULL;
    builder.count = 0U;
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment list response",
                        NULL, NULL, NULL);
    }
  }
  lc_pouch_attachment_list_builder_cleanup(&builder);
  if (rc != LC_OK) {
    lc_attachment_list_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_attach_method_impl(self, req, src, out, error);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_attach_method_impl(self, req, src, out, error);
  lc_client_handle_release(client);
  return rc;
}

int lc_pouch_client_list_attachments_method(lc_client *self,
                                            const lc_attachment_list_req *req,
                                            lc_attachment_list *out,
                                            lc_error *error) {
  return lc_pouch_client_list_attachments_with_transaction_mode(self, req, out,
                                                                1, error);
}

static int lc_pouch_client_get_attachment_with_transaction_mode(
    lc_client *self, const lc_attachment_get_op *req, lc_sink *dst,
    lc_attachment_get_res *out, int stage_transaction, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *ns = NULL;
  char *name;
  char *attachment_key;
  char *staged_key;
  lc_pouch_unix_seconds created_at_unix;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get_attachment requires self, req, dst, and "
                        "out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  created_at_unix = 0L;
  name = NULL;
  attachment_key = NULL;
  staged_key = NULL;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!req->public_read) {
    if (!lc_pouch_lease_ref_has_credentials(&req->lease)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch private attachment read requires lease_id",
                          NULL, NULL, NULL);
    }
    rc = lc_pouch_validate_lease_record(client, &req->lease, ns, req->lease.key,
                                        NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
  }

  name = lc_pouch_attachment_name_from_selector(&req->selector, error);
  if (name == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  attachment_key = lc_pouch_attachment_key(ns, req->lease.key, name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (stage_transaction && !req->public_read &&
      lc_pouch_txn_id_present(req->lease.txn_id)) {
    staged_key = lc_pouch_staged_attachment_key(ns, req->lease.key, name,
                                                req->lease.txn_id, error);
    if (staged_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      goto cleanup;
    }
    rc =
        lc_pouch_state_read(client->pouch, ns, staged_key, &read_result, error);
    if (rc == LC_OK && read_result.found &&
        lc_pouch_attachment_is_delete_marker(read_result.content_type)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L, "pouch attachment not found",
                        NULL, NULL, NULL);
    }
    if (rc == LC_OK && !read_result.found) {
      lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
      memset(&read_result, 0, sizeof(read_result));
      rc = lc_pouch_state_read(client->pouch, ns, attachment_key, &read_result,
                               error);
    }
  } else {
    rc = lc_pouch_state_read(client->pouch, ns, attachment_key, &read_result,
                             error);
  }
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, "pouch attachment not found",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, dst, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_created_at_decode(read_result.metadata,
                                               read_result.metadata_length,
                                               &created_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_info_fill(
        &out->attachment, name, read_result.bytes, read_result.content_type,
        created_at_unix, read_result.updated_at_unix, error);
  }
  if (rc == LC_OK) {
    out->correlation_id = lc_strdup_local("pouch-attachment-get");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment get response",
                        NULL, NULL, NULL);
    }
  }

cleanup:
  lc_free_with_allocator(NULL, name);
  lc_free_with_allocator(NULL, attachment_key);
  lc_free_with_allocator(NULL, staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_attachment_get_res_cleanup(out);
  }
  return rc;
}

static int lc_pouch_client_get_attachment_method_impl(
    lc_client *self, const lc_attachment_get_op *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  return lc_pouch_client_get_attachment_with_transaction_mode(self, req, dst,
                                                              out, 1, error);
}

int lc_pouch_client_get_attachment_method(lc_client *self,
                                          const lc_attachment_get_op *req,
                                          lc_sink *dst,
                                          lc_attachment_get_res *out,
                                          lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL) {
    return lc_pouch_client_get_attachment_method_impl(self, req, dst, out,
                                                      error);
  }
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_get_attachment_method_impl(self, req, dst, out, error);
  lc_client_handle_release(client);
  return rc;
}

static int lc_pouch_attachment_delete_locked(void *context, lc_error *error) {
  lc_pouch_attachment_delete_context *ctx;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  int found;
  int rc;

  ctx = (lc_pouch_attachment_delete_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || ctx->attachment_key == NULL || ctx->deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment deletion requires context", NULL,
                        NULL, "pouch");
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  *ctx->deleted = 0;
  found = 0;
  rc = lc_pouch_validate_lease_record(ctx->client, &ctx->req->lease, ctx->ns,
                                      ctx->req->lease.key, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_read(ctx->client->pouch, ctx->ns, ctx->attachment_key,
                             &read_result, error);
  }
  if (rc == LC_OK && read_result.found) {
    found = 1;
  }
  if (rc == LC_OK && ctx->stage_transaction &&
      lc_pouch_txn_id_present(ctx->req->lease.txn_id)) {
    if (ctx->staged_key == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attachment deletion requires key", NULL,
                        NULL, "pouch");
    }
    if (rc == LC_OK) {
      lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
      memset(&read_result, 0, sizeof(read_result));
      rc = lc_pouch_state_read(ctx->client->pouch, ctx->ns, ctx->staged_key,
                               &read_result, error);
    }
    if (rc == LC_OK && read_result.found) {
      /* A staged tombstone is the transaction-local absence marker. It
       * overrides the committed attachment so repeated deletes are no-ops. */
      found = !lc_pouch_attachment_is_delete_marker(read_result.content_type);
    }
    if (rc == LC_OK && found) {
      rc = lc_pouch_attachment_stage_delete(ctx->client, ctx->ns,
                                            ctx->staged_key, error);
      if (rc == LC_OK) {
        *ctx->deleted = 1;
      }
    }
  } else if (rc == LC_OK && read_result.found) {
    options.expected_version = read_result.version;
    options.has_expected_version = 1;
    rc = lc_pouch_state_delete(ctx->client->pouch, ctx->ns, ctx->attachment_key,
                               &options, &write_result, error);
    if (rc == LC_OK) {
      *ctx->deleted = 1;
    }
  }
  lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
  lc_pouch_state_write_result_cleanup(&ctx->client->allocator, &write_result);
  return rc;
}

static int lc_pouch_attachment_delete_all_locked(void *context,
                                                 lc_error *error) {
  lc_pouch_attachment_delete_all_context *ctx;
  lc_pouch_attachment_list_builder builder;
  size_t i;
  int rc;

  ctx = (lc_pouch_attachment_delete_all_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->req == NULL ||
      ctx->ns == NULL || ctx->deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment clear requires context", NULL, NULL,
                        "pouch");
  }
  memset(&builder, 0, sizeof(builder));
  *ctx->deleted_count = 0;
  rc = lc_pouch_validate_lease_record(ctx->client, &ctx->req->lease, ctx->ns,
                                      ctx->req->lease.key, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_collect_attachments(ctx->client, ctx->ns, ctx->req->lease.key,
                                      &builder, error);
  }
  if (rc == LC_OK && ctx->stage_transaction &&
      lc_pouch_txn_id_present(ctx->req->lease.txn_id)) {
    rc = lc_pouch_overlay_staged_attachments(
        ctx->client, ctx->ns, ctx->req->lease.key, ctx->req->lease.txn_id,
        &builder, error);
  }
  if (rc == LC_OK && ctx->stage_transaction &&
      lc_pouch_txn_id_present(ctx->req->lease.txn_id)) {
    for (i = 0U; rc == LC_OK && i < builder.count; ++i) {
      char *attachment_key;
      char *staged_attachment_key;

      attachment_key = lc_pouch_attachment_key(ctx->ns, ctx->req->lease.key,
                                               builder.items[i].name, error);
      staged_attachment_key = NULL;
      if (attachment_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
      if (rc == LC_OK) {
        staged_attachment_key = lc_pouch_staged_attachment_key_from_committed(
            attachment_key, ctx->req->lease.txn_id, error);
      }
      if (rc == LC_OK && staged_attachment_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
      if (rc == LC_OK) {
        rc = lc_pouch_attachment_stage_delete(ctx->client, ctx->ns,
                                              staged_attachment_key, error);
      }
      lc_free_with_allocator(NULL, attachment_key);
      lc_free_with_allocator(NULL, staged_attachment_key);
      if (rc == LC_OK) {
        *ctx->deleted_count += 1;
      }
    }
  } else if (rc == LC_OK) {
    for (i = 0U; rc == LC_OK && i < builder.key_count; ++i) {
      lc_pouch_state_write_options options;
      lc_pouch_state_write_result result;

      memset(&options, 0, sizeof(options));
      memset(&result, 0, sizeof(result));
      options.expected_version = builder.keys[i].version;
      options.has_expected_version = 1;
      rc = lc_pouch_state_delete(ctx->client->pouch, ctx->ns,
                                 builder.keys[i].key, &options, &result, error);
      lc_pouch_state_write_result_cleanup(&ctx->client->allocator, &result);
      if (rc == LC_OK) {
        *ctx->deleted_count += 1;
      }
    }
  }
  lc_pouch_attachment_list_builder_cleanup(&builder);
  return rc;
}

/* The stateful outbox reclaimer has already verified the immutable parent is
 * terminal, so it deliberately bypasses a possibly stale crashed-worker lease.
 * Keep attachment deletion namespace-atomic with the same collector used by
 * the public leased operation. */
static int
lc_pouch_outbox_state_reclaim_delete_attachments_locked(void *context,
                                                        lc_error *error) {
  lc_pouch_outbox_state_reclaim_attachment_context *ctx;
  lc_pouch_attachment_list_builder builder;
  size_t index;
  int rc;

  ctx = (lc_pouch_outbox_state_reclaim_attachment_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->ns == NULL ||
      ctx->key == NULL || ctx->deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim attachments require "
                        "context",
                        NULL, NULL, "pouch");
  }
  memset(&builder, 0, sizeof(builder));
  *ctx->deleted_count = 0;
  rc = lc_pouch_collect_attachments(ctx->client, ctx->ns, ctx->key, &builder,
                                    error);
  for (index = 0U; rc == LC_OK && index < builder.key_count; ++index) {
    lc_pouch_state_write_options options;
    lc_pouch_state_write_result result;

    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.expected_version = builder.keys[index].version;
    options.has_expected_version = 1;
    rc = lc_pouch_state_delete(ctx->client->pouch, ctx->ns,
                               builder.keys[index].key, &options, &result,
                               error);
    lc_pouch_state_write_result_cleanup(&ctx->client->allocator, &result);
    if (rc == LC_OK) {
      *ctx->deleted_count += 1;
    }
  }
  lc_pouch_attachment_list_builder_cleanup(&builder);
  return rc;
}

int lc_pouch_outbox_state_reclaim_delete_attachments(lc_client_handle *client,
                                                     const char *ns,
                                                     const char *key,
                                                     int *deleted_count,
                                                     lc_error *error) {
  lc_pouch_outbox_state_reclaim_attachment_context context;

  if (client == NULL || !client->is_pouch || ns == NULL || ns[0] == '\0' ||
      key == NULL || key[0] == '\0' || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim requires client, "
                        "namespace, key, and deleted count",
                        NULL, NULL, "pouch");
  }
  memset(&context, 0, sizeof(context));
  context.client = client;
  context.ns = ns;
  context.key = key;
  context.deleted_count = deleted_count;
  return lc_pouch_state_with_namespace_lock(
      client->pouch, ns,
      lc_pouch_outbox_state_reclaim_delete_attachments_locked, &context, error);
}

static int lc_pouch_client_delete_attachment_with_transaction_mode(
    lc_client *self, const lc_attachment_delete_op *req, int *deleted,
    int stage_transaction, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attachment_delete_context delete_context;
  const char *ns = NULL;
  char *name;
  char *attachment_key;
  char *staged_key;
  int rc;

  if (self == NULL || req == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_attachment requires self, req, and "
                        "deleted",
                        NULL, NULL, NULL);
  }
  *deleted = 0;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&delete_context, 0, sizeof(delete_context));
  name = NULL;
  attachment_key = NULL;
  staged_key = NULL;
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }

  name = lc_pouch_attachment_name_from_selector(&req->selector, error);
  if (name == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  attachment_key = lc_pouch_attachment_key(ns, req->lease.key, name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (stage_transaction && lc_pouch_txn_id_present(req->lease.txn_id)) {
    staged_key = lc_pouch_staged_attachment_key(ns, req->lease.key, name,
                                                req->lease.txn_id, error);
    if (staged_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      goto cleanup;
    }
  }
  delete_context.client = client;
  delete_context.req = req;
  delete_context.ns = ns;
  delete_context.attachment_key = attachment_key;
  delete_context.staged_key = staged_key;
  delete_context.stage_transaction = stage_transaction;
  delete_context.deleted = deleted;
  rc = lc_pouch_state_with_namespace_lock(client->pouch, ns,
                                          lc_pouch_attachment_delete_locked,
                                          &delete_context, error);

cleanup:
  lc_free_with_allocator(NULL, name);
  lc_free_with_allocator(NULL, attachment_key);
  lc_free_with_allocator(NULL, staged_key);
  return rc;
}

int lc_pouch_client_delete_attachment_method(lc_client *self,
                                             const lc_attachment_delete_op *req,
                                             int *deleted, lc_error *error) {
  return lc_pouch_client_delete_attachment_with_transaction_mode(
      self, req, deleted, 1, error);
}

static int lc_pouch_client_delete_all_attachments_with_transaction_mode(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    int stage_transaction, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attachment_delete_all_context delete_context;
  const char *ns = NULL;
  int rc;

  if (self == NULL || req == NULL || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_all_attachments requires self, req, "
                        "and deleted_count",
                        NULL, NULL, NULL);
  }
  *deleted_count = 0;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_lease_key(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&delete_context, 0, sizeof(delete_context));
  rc = lc_pouch_client_public_namespace(client, req->lease.ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_client_require_lease_credentials(&req->lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  delete_context.client = client;
  delete_context.req = req;
  delete_context.ns = ns;
  delete_context.stage_transaction = stage_transaction;
  delete_context.deleted_count = deleted_count;
  return lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_attachment_delete_all_locked, &delete_context,
      error);
}

int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error) {
  return lc_pouch_client_delete_all_attachments_with_transaction_mode(
      self, req, deleted_count, 1, error);
}

int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_scan scan;
  const char *ns = NULL;
  lc_pouch_unix_seconds now;
  size_t i;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_stats requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_stats requires queue", NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&scan, 0, sizeof(scan));
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_scan_load(client, ns, req->queue, &scan, error);
  }
  if (rc == LC_OK) {
    out->ns = lc_strdup_local(ns);
    out->queue = lc_strdup_local(req->queue);
    out->correlation_id = lc_strdup_local("pouch-queue-stats");
    if (out->ns == NULL || out->queue == NULL || out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue stats response", NULL,
                        NULL, NULL);
    }
  }
  for (i = 0U; rc == LC_OK && i < scan.count; ++i) {
    lc_pouch_queue_record *record;

    record = &scan.records[i];
    if (!lc_pouch_queue_record_is_live_at(record, now)) {
      continue;
    }
    out->pending_candidates += 1;
    if (lc_pouch_queue_record_available(record, now)) {
      out->available += 1;
      if (out->head_message_id == NULL) {
        out->head_message_id = lc_strdup_local(record->message_id);
        if (out->head_message_id == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch queue head message id",
                            NULL, NULL, NULL);
          break;
        }
        out->head_enqueued_at_unix = record->enqueued_at_unix;
        out->head_not_visible_until_unix = record->not_visible_until_unix;
        out->head_age_seconds = now - record->enqueued_at_unix;
      }
    }
  }
  lc_pouch_queue_scan_cleanup(&scan);
  if (rc != LC_OK) {
    lc_queue_stats_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_ack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    rc = lc_pouch_queue_validate_state_lease(client, &req->message, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    lc_free_with_allocator(NULL, record.status);
    record.status = lc_strdup_with_allocator(NULL, "acked");
    if (record.status == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue ack status", NULL, NULL,
                        NULL);
    } else if (lc_pouch_txn_id_present(req->message.txn_id)) {
      rc = lc_pouch_queue_clear_lease(&record, error);
      if (rc == LC_OK) {
        rc = lc_pouch_queue_write_or_stage_record(client, &record,
                                                  req->message.txn_id, error);
      }
    } else {
      rc = lc_pouch_queue_delete_state_object_for_ack(client, &req->message,
                                                      error);
      if (rc == LC_OK) {
        rc = lc_pouch_queue_delete_message_objects(client, &record, error);
      }
    }
    if (rc == LC_OK &&
        (req->message.txn_id == NULL || req->message.txn_id[0] == '\0')) {
      rc = lc_pouch_queue_clear_message_lease(client, &req->message, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_queue_release_state_lease(client, &req->message, 0, error);
    }
    if (rc == LC_OK) {
      out->acked = 1;
    }
  }
  if (rc == LC_OK) {
    out->correlation_id = lc_strdup_local("pouch-queue-ack");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue ack response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_ack_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  lc_pouch_unix_seconds now;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->delay_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack delay_seconds must be non-negative",
                        NULL, NULL, NULL);
  }
  if (req->intent != LC_NACK_INTENT_UNSPECIFIED &&
      req->intent != LC_NACK_INTENT_FAILURE &&
      req->intent != LC_NACK_INTENT_DEFER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack intent is invalid", NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    rc = lc_pouch_queue_validate_state_lease(client, &req->message, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    lc_free_with_allocator(NULL, record.status);
    record.status = lc_strdup_with_allocator(NULL, "available");
    if (record.status == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue nack status", NULL,
                        NULL, NULL);
    } else {
      rc = lc_pouch_queue_clear_lease(&record, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_timestamp_add(now, req->delay_seconds, "delay_seconds",
                                  &record.not_visible_until_unix, error);
    }
    if (rc == LC_OK) {
      if (req->intent == LC_NACK_INTENT_UNSPECIFIED ||
          req->intent == LC_NACK_INTENT_FAILURE) {
        record.failure_attempts += 1;
      }
      if (!lc_pouch_queue_record_is_live_at(&record, now)) {
        lc_free_with_allocator(NULL, record.status);
        record.status =
            record.expires_at_unix > 0L && record.expires_at_unix <= now
                ? lc_strdup_with_allocator(NULL, "expired")
                : lc_strdup_with_allocator(NULL, "dead");
        if (record.status == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch queue terminal status",
                            NULL, NULL, NULL);
        }
      }
      if (rc == LC_OK) {
        if (strcmp(record.status, "dead") == 0 &&
            !lc_pouch_txn_id_present(req->message.txn_id)) {
          rc = lc_pouch_queue_move_to_dlq(client, &record, error);
        } else {
          rc = lc_pouch_queue_write_or_stage_record(client, &record,
                                                    req->message.txn_id, error);
        }
      }
      if (rc == LC_OK &&
          (req->message.txn_id == NULL || req->message.txn_id[0] == '\0')) {
        rc = lc_pouch_queue_clear_message_lease(client, &req->message, error);
      }
      if (rc == LC_OK) {
        rc =
            lc_pouch_queue_release_state_lease(client, &req->message, 1, error);
      }
    }
    if (rc == LC_OK) {
      out->requeued = lc_pouch_queue_record_is_live_at(&record, now);
      out->meta_etag = lc_strdup_local(record.meta_etag);
      out->correlation_id = lc_strdup_local("pouch-queue-nack");
      if (out->meta_etag == NULL || out->correlation_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue nack response", NULL,
                          NULL, NULL);
      }
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_nack_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  lc_pouch_unix_seconds now;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_extend requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->extend_by_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_extend extend_by_seconds must be "
                        "positive",
                        NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") == 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue message is already acked", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_timestamp_add(now, req->extend_by_seconds, "extend_by_seconds",
                               &record.not_visible_until_unix, error);
  }
  if (rc == LC_OK) {
    record.visibility_timeout_seconds = req->extend_by_seconds;
    rc = lc_pouch_queue_write_or_stage_record(client, &record,
                                              req->message.txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_extend_message_lease(
        client, &req->message, record.not_visible_until_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_extend_state_lease(
        client, &req->message, record.not_visible_until_unix,
        &out->state_lease_expires_at_unix, error);
  }
  if (rc == LC_OK) {
    out->lease_expires_at_unix = record.not_visible_until_unix;
    out->visibility_timeout_seconds = record.visibility_timeout_seconds;
    out->meta_etag = lc_strdup_local(record.meta_etag);
    out->correlation_id = lc_strdup_local("pouch-queue-extend");
    if (out->meta_etag == NULL || out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue extend response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_extend_res_cleanup(out);
  }
  return rc;
}

static int lc_pouch_client_enqueue_locked(lc_client_handle *client,
                                          const char *ns,
                                          const lc_enqueue_req *req,
                                          lc_source *src, lc_enqueue_res *out,
                                          lc_error *error) {
  lc_pouch_queue_record record;
  lc_source *record_source;
  lc_source *payload_source;
  lc_pouch_counting_source counting_source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_options payload_options;
  lc_pouch_state_write_result result;
  char *payload_key;
  unsigned int attempt;
  int payload_written;
  int rc;

  if (client == NULL || ns == NULL || req == NULL || src == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires client, namespace, req, src, "
                        "and out",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&options, 0, sizeof(options));
  memset(&payload_options, 0, sizeof(payload_options));
  memset(&result, 0, sizeof(result));
  record_source = NULL;
  payload_source = NULL;
  payload_key = NULL;
  if (rc == LC_OK) {
    record.ns = lc_strdup_with_allocator(NULL, ns);
    record.queue = lc_pouch_queue_normalize_name(req->queue, error);
    record.status = lc_strdup_with_allocator(NULL, "available");
    record.content_type = lc_strdup_with_allocator(
        NULL, req->content_type != NULL && req->content_type[0] != '\0'
                  ? req->content_type
                  : "application/octet-stream");
    record.lease_id = lc_strdup_with_allocator(NULL, "");
    record.lease_txn_id = lc_strdup_with_allocator(NULL, "");
    record.attempts = 0;
    record.max_attempts = req->max_attempts;
    record.failure_attempts = 0;
    record.expires_at_unix = 0L;
    record.visibility_timeout_seconds = req->visibility_timeout_seconds;
    if (rc == LC_OK &&
        (record.ns == NULL || record.queue == NULL || record.status == NULL ||
         record.content_type == NULL || record.lease_id == NULL ||
         record.lease_txn_id == NULL)) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue record", NULL, NULL,
                        NULL);
    }
  }
  options.create_if_absent = 1;
  options.content_type = "application/x-lockdc-pouch-queue";
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.object_record = 1;
  payload_options.create_if_absent = 1;
  payload_options.has_query_hidden = 1;
  payload_options.query_hidden = 1;
  payload_options.object_record = 1;
  for (attempt = 0U; rc == LC_OK && attempt < 16U; ++attempt) {
    payload_written = 0;
    lc_free_with_allocator(NULL, record.message_id);
    lc_free_with_allocator(NULL, record.storage_key);
    lc_free_with_allocator(NULL, payload_key);
    record.message_id = NULL;
    record.storage_key = NULL;
    payload_key = NULL;
    record.message_id = lc_pouch_queue_message_id(
        &record.enqueued_at_unix, &record.enqueued_at_nsec,
        &record.enqueue_sequence, error);
    record.storage_key =
        record.message_id != NULL
            ? lc_pouch_queue_key(ns, req->queue, record.message_id, error)
            : NULL;
    if (record.message_id == NULL || record.storage_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    payload_key =
        lc_pouch_queue_payload_key_from_meta(record.storage_key, error);
    if (payload_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    record.expires_at_unix = 0L;
    if (req->ttl_seconds > 0L) {
      rc =
          lc_pouch_timestamp_add(record.enqueued_at_unix, req->ttl_seconds,
                                 "ttl_seconds", &record.expires_at_unix, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_timestamp_add(record.enqueued_at_unix, req->delay_seconds,
                                  "delay_seconds",
                                  &record.not_visible_until_unix, error);
    }
    if (rc == LC_OK) {
      memset(&counting_source, 0, sizeof(counting_source));
      counting_source.inner = src;
      counting_source.max_bytes = LONG_MAX;
      counting_source.has_max_bytes = 1;
      payload_options.content_type = record.content_type;
      rc = lc_source_from_callbacks(lc_pouch_counting_source_read,
                                    lc_pouch_counting_source_reset, NULL,
                                    &counting_source, &payload_source, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_write(client->pouch, ns, payload_key, payload_source,
                                &payload_options, &result, error);
    }
    if (payload_source != NULL) {
      lc_source_close(payload_source);
      payload_source = NULL;
    }
    if (rc == LC_OK) {
      payload_written = 1;
      if (!lc_u64_to_size_checked((lc_u64)result.bytes,
                                  &record.payload_length)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch queue payload is too large", NULL, NULL,
                          "pouch");
      }
      lc_pouch_state_write_result_cleanup(&client->allocator, &result);
      if (rc == LC_OK) {
        rc = lc_pouch_queue_record_source(&record, &record_source, error);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_write(client->pouch, ns, record.storage_key,
                                record_source, &options, &result, error);
    }
    if (rc != LC_OK && payload_written && payload_key != NULL) {
      lc_error ignored_error;
      lc_pouch_state_write_result ignored_result;

      lc_error_init(&ignored_error);
      memset(&ignored_result, 0, sizeof(ignored_result));
      (void)lc_pouch_state_delete(client->pouch, ns, payload_key,
                                  &payload_options, &ignored_result,
                                  &ignored_error);
      lc_pouch_state_write_result_cleanup(&client->allocator, &ignored_result);
      lc_error_cleanup(&ignored_error);
    }
    if (rc == LC_OK) {
      break;
    }
    if (lc_pouch_queue_retryable_create_collision(error)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      if (record_source != NULL) {
        lc_source_close(record_source);
        record_source = NULL;
      }
      if (src->reset != NULL) {
        rc = src->reset(src, error);
        if (rc != LC_OK) {
          break;
        }
      }
      continue;
    }
    break;
  }
  if (rc == LC_OK && attempt == 16U) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "failed to allocate unique pouch queue message id", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    lc_pouch_queue_touch_notification(client, ns, req->queue);
  }
  if (rc == LC_OK) {
    out->ns = lc_strdup_local(ns);
    out->queue = lc_strdup_local(req->queue);
    out->message_id = lc_strdup_local(record.message_id);
    out->attempts = 0;
    out->max_attempts = req->max_attempts;
    out->failure_attempts = 0;
    out->not_visible_until_unix = record.not_visible_until_unix;
    out->visibility_timeout_seconds = req->visibility_timeout_seconds;
    if (out->ns == NULL || out->queue == NULL || out->message_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch enqueue response", NULL, NULL,
                        NULL);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_size_to_public_long((uint64_t)record.payload_length,
                                        &out->payload_bytes, error);
    }
    if (rc == LC_OK) {
      out->correlation_id = lc_strdup_local("pouch-queue-enqueue");
      if (out->correlation_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch enqueue response", NULL,
                          NULL, NULL);
      }
    }
  }
  if (record_source != NULL) {
    lc_source_close(record_source);
  }
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  lc_free_with_allocator(NULL, payload_key);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_enqueue_res_cleanup(out);
  }
  return rc;
}

static int lc_pouch_client_enqueue_locked_callback(void *context,
                                                   lc_error *error) {
  lc_pouch_enqueue_context *ctx;

  ctx = (lc_pouch_enqueue_context *)context;
  if (ctx == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue callback requires context", NULL, NULL,
                        NULL);
  }
  return lc_pouch_client_enqueue_locked(ctx->client, ctx->ns, ctx->req,
                                        ctx->src, ctx->out, error);
}

static int lc_pouch_client_enqueue_method_impl(lc_client *self,
                                               const lc_enqueue_req *req,
                                               lc_source *src,
                                               lc_enqueue_res *out,
                                               lc_error *error) {
  lc_client_handle *client;
  lc_pouch_enqueue_context context;
  const char *ns;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires queue", NULL, NULL, NULL);
  }
  if (req->delay_seconds < 0L || req->visibility_timeout_seconds < 0L ||
      req->ttl_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue timing values must be non-negative",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  ns = NULL;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&context, 0, sizeof(context));
  context.client = client;
  context.ns = ns;
  context.req = req;
  context.src = src;
  context.out = out;
  return lc_pouch_state_with_namespace_lock(
      client->pouch, ns, lc_pouch_client_enqueue_locked_callback, &context,
      error);
}

static int lc_pouch_queue_rollback_published_delivery(
    lc_client_handle *client, const lc_message_ref *message, lc_error *error);

static int lc_pouch_queue_rollback_delivery_after_failure(
    lc_client_handle *client, const lc_message_ref *message, int published,
    int failure_rc, lc_error *error) {
  lc_error rollback_error;
  int rollback_rc;

  lc_error_init(&rollback_error);
  rollback_rc = published ? lc_pouch_queue_rollback_published_delivery(
                                client, message, &rollback_error)
                          : lc_pouch_queue_clear_message_lease(client, message,
                                                               &rollback_error);
  if (rollback_rc != LC_OK && error != NULL) {
    lc_error_cleanup(error);
    *error = rollback_error;
    lc_error_init(&rollback_error);
  }
  lc_error_cleanup(&rollback_error);
  return rollback_rc != LC_OK ? rollback_rc : failure_rc;
}

static int
lc_pouch_queue_rollback_message_after_failure(lc_client_handle *client,
                                              lc_message *message,
                                              int failure_rc, lc_error *error) {
  lc_message_handle *handle;
  lc_message_ref rollback_message;

  if (message == NULL) {
    return failure_rc;
  }
  handle = (lc_message_handle *)message;
  memset(&rollback_message, 0, sizeof(rollback_message));
  rollback_message.ns = handle->ns;
  rollback_message.queue = handle->queue;
  rollback_message.message_id = handle->message_id;
  rollback_message.lease_id = handle->lease_id;
  rollback_message.txn_id = handle->txn_id;
  rollback_message.fencing_token = handle->fencing_token;
  rollback_message.state_lease_id = handle->state_lease_id;
  rollback_message.state_fencing_token = handle->state_fencing_token;
  return lc_pouch_queue_rollback_delivery_after_failure(
      client, &rollback_message, 1, failure_rc, error);
}

static int lc_pouch_queue_rollback_batch_messages_after_failure(
    lc_client_handle *client, lc_message **messages, size_t count,
    int failure_rc, lc_error *error) {
  size_t i;
  int rc;

  rc = failure_rc;
  for (i = 0U; i < count; ++i) {
    if (messages[i] != NULL) {
      rc = lc_pouch_queue_rollback_message_after_failure(client, messages[i],
                                                         rc, error);
      messages[i]->close(messages[i]);
      messages[i] = NULL;
    }
  }
  return rc;
}

int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_enqueue_method_impl(self, req, src, out, error);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_enqueue_method_impl(self, req, src, out, error);
  lc_client_handle_release(client);
  return rc;
}

static int lc_pouch_client_dequeue_once(lc_client_handle *client,
                                        const char *ns,
                                        const lc_dequeue_req *req,
                                        long visibility_timeout,
                                        lc_message **out, lc_error *error) {
  lc_pouch_queue_scan scan;
  const char *next_cursor;
  lc_pouch_unix_seconds now;
  size_t i;
  int after_cursor;
  int rc;

  now = 0L;
  memset(&scan, 0, sizeof(scan));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_scan_load(client, ns, req->queue, &scan, error);
  }
  after_cursor = req->start_after == NULL || req->start_after[0] == '\0';
  for (i = 0U; rc == LC_OK && i < scan.count; ++i) {
    lc_pouch_queue_record *record;
    char *message_lease_key;
    char lease_id[160];
    long message_fencing_token;
    lc_pouch_generation record_version_before;
    lc_message_ref rollback_message;
    int lease_acquired;

    message_lease_key = NULL;
    message_fencing_token = 0L;
    lease_acquired = 0;
    record_version_before = 0UL;
    memset(&rollback_message, 0, sizeof(rollback_message));
    record = &scan.records[i];
    record_version_before = record->version;
    if (!after_cursor) {
      if (strcmp(record->message_id, req->start_after) == 0) {
        after_cursor = 1;
      }
      continue;
    }
    if (!lc_pouch_queue_record_available(record, now)) {
      continue;
    }
    snprintf(lease_id, sizeof(lease_id), "pouch-qlease-%s-%d",
             record->message_id, record->attempts + 1);
    message_lease_key =
        lc_pouch_queue_message_lease_key_from_meta(record->storage_key, error);
    if (message_lease_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    rc = lc_pouch_timestamp_add(now, visibility_timeout,
                                "visibility_timeout_seconds",
                                &record->not_visible_until_unix, error);
    if (rc == LC_OK) {
      rc = lc_pouch_queue_acquire_message_lease(
          client, ns, req, message_lease_key, lease_id,
          record->not_visible_until_unix, now, &message_fencing_token,
          &lease_acquired, error);
    }
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, message_lease_key);
      break;
    }
    if (!lease_acquired) {
      lc_free_with_allocator(NULL, message_lease_key);
      continue;
    }
    rollback_message.ns = ns;
    rollback_message.queue = record->queue;
    rollback_message.message_id = record->message_id;
    rollback_message.lease_id = lease_id;
    rollback_message.txn_id = req->txn_id;
    rollback_message.fencing_token = message_fencing_token;
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_after_queue_lease_claim_hook != NULL) {
      rc = lc_pouch_test_after_queue_lease_claim_hook(
          lc_pouch_test_after_queue_lease_claim_context, error);
      if (rc != LC_OK) {
        rc = lc_pouch_queue_rollback_delivery_after_failure(
            client, &rollback_message, 0, rc, error);
        lc_free_with_allocator(NULL, message_lease_key);
        break;
      }
    }
#endif
    record->attempts += 1;
    lc_free_with_allocator(NULL, record->status);
    lc_free_with_allocator(NULL, record->lease_id);
    lc_free_with_allocator(NULL, record->lease_txn_id);
    record->status = lc_strdup_with_allocator(NULL, "inflight");
    record->lease_id = lc_strdup_with_allocator(NULL, lease_id);
    record->lease_txn_id =
        lc_strdup_with_allocator(NULL, req->txn_id != NULL ? req->txn_id : "");
    record->lease_fencing_token = message_fencing_token;
    record->visibility_timeout_seconds = visibility_timeout;
    if (record->status == NULL || record->lease_id == NULL ||
        record->lease_txn_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue delivery state", NULL,
                        NULL, NULL);
      rc = lc_pouch_queue_rollback_delivery_after_failure(
          client, &rollback_message, 0, rc, error);
      lc_free_with_allocator(NULL, message_lease_key);
      break;
    }
    rc = lc_pouch_queue_write_record(client, record, error);
    if (rc != LC_OK) {
      rc = lc_pouch_queue_rollback_delivery_after_failure(
          client, &rollback_message, record->version != record_version_before,
          rc, error);
    }
    lc_free_with_allocator(NULL, message_lease_key);
    if (rc != LC_OK && lc_pouch_queue_retryable_version_conflict(error)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      continue;
    }
    if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
      if (lc_pouch_test_before_queue_message_build_hook != NULL) {
        lc_pouch_test_before_queue_message_build_hook(
            lc_pouch_test_before_queue_message_build_context);
      }
#endif
      next_cursor = record->message_id;
      rc = lc_pouch_queue_make_message(client, record, next_cursor, NULL, out,
                                       error);
      if (rc != LC_OK) {
        rc = lc_pouch_queue_rollback_delivery_after_failure(
            client, &rollback_message, 1, rc, error);
      }
    }
    break;
  }
  lc_pouch_queue_scan_cleanup(&scan);
  return rc;
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  lc_client_handle *client;
  const char *ns = NULL;
  struct timespec deadline;
  long visibility_timeout;
  int has_deadline;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue requires self, req, and out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue requires queue", NULL, NULL, NULL);
  }
  rc = lc_pouch_client_validate_owner(req->owner, error);
  if (rc != LC_OK) {
    return rc;
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_queue_wait_deadline(req->wait_seconds, &deadline, &has_deadline,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  visibility_timeout = req->visibility_timeout_seconds > 0L
                           ? req->visibility_timeout_seconds
                           : 30L;
  do {
    rc = lc_pouch_client_dequeue_once(client, ns, req, visibility_timeout, out,
                                      error);
    if (rc != LC_OK || *out != NULL || !has_deadline ||
        lc_pouch_queue_wait_deadline_reached(&deadline)) {
      break;
    }
    lc_pouch_queue_dequeue_poll_delay();
  } while (1);
  return rc;
}

/* A local dequeue failure must not leave an inaccessible delivery leased. This
 * is intentionally a direct rollback, even for transaction-tagged deliveries:
 * no handle escaped to let a later transaction resolve a partial delivery. */
static int lc_pouch_queue_rollback_published_delivery(
    lc_client_handle *client, const lc_message_ref *message, lc_error *error) {
  lc_pouch_queue_record record;
  lc_pouch_generation record_version_before;
  int rollback_record_write_attempted;
  int rc;

  if (client == NULL || message == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue delivery rollback requires client and "
                        "message",
                        NULL, NULL, NULL);
  }
  memset(&record, 0, sizeof(record));
  record_version_before = 0UL;
  rollback_record_write_attempted = 0;
  rc = lc_pouch_queue_copy_message_ref(message, &record, client, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_validate_state_lease(client, message, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_replace_string(&record.status, "available", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_clear_lease(&record, error);
  }
  if (rc == LC_OK) {
    if (record.attempts > 0) {
      record.attempts -= 1;
    }
    record.not_visible_until_unix = 0L;
    record_version_before = record.version;
    rollback_record_write_attempted = 1;
    rc = lc_pouch_queue_write_record(client, &record, error);
  }
  if (rollback_record_write_attempted && rc != LC_OK &&
      record.version != record_version_before) {
    /* The durable rollback record is already installed. Its response ETag is
     * only a transient cache; continue clearing the paired lease. */
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_clear_message_lease(client, message, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_release_state_lease(client, message, 0, error);
  }
  lc_pouch_queue_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  lc_client_handle *client;
  lc_message *message;
  lc_message_handle *handle;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_result lease_write_result;
  lc_pouch_lease_record lease_record;
  char *state_key;
  char *state_object_key;
  char state_lease_id[192];
  lc_version state_version;
  long state_fencing_token = 0L;
  lc_pouch_unix_seconds now_seconds = 0;
  int state_lease_acquired;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_with_state requires self, req, and out",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  message = NULL;
  state_key = NULL;
  state_object_key = NULL;
  state_lease_id[0] = '\0';
  state_lease_acquired = 0;
  memset(&read_result, 0, sizeof(read_result));
  memset(&lease_write_result, 0, sizeof(lease_write_result));
  memset(&lease_record, 0, sizeof(lease_record));
  rc = lc_pouch_client_dequeue_method(self, req, &message, error);
  if (rc != LC_OK || message == NULL) {
    return rc;
  }
  handle = (lc_message_handle *)message;
  state_key =
      lc_pouch_queue_state_lease_key(handle->queue, handle->message_id, error);
  if (state_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  state_object_key =
      lc_pouch_queue_state_object_key_from_lease_key(state_key, error);
  if (state_object_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, handle->ns, state_object_key,
                           &read_result, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  state_version = 0;
  if (read_result.found) {
    rc = lc_pouch_generation_to_version(read_result.version, &state_version,
                                        error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  snprintf(state_lease_id, sizeof(state_lease_id), "pouch-qstate-%s-%d",
           handle->message_id, handle->attempts);
  rc = lc_pouch_read_lease_record(client, handle->ns, state_key, &lease_record,
                                  error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_now_unix(&now_seconds, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (lease_record.found && lease_record.expires_at_unix > now_seconds) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue state lease already held", NULL, NULL, NULL);
    goto cleanup;
  }
  rc = lc_pouch_next_fencing_token(
      lease_record.found ? lease_record.fencing_token : 0L,
      &state_fencing_token, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_write_lease_record_with_visibility(
      client, handle->ns, state_key, req->owner, state_lease_id, handle->txn_id,
      state_fencing_token, handle->not_visible_until_unix,
      read_result.found ? read_result.version : 0UL,
      lc_pouch_txn_id_present(handle->txn_id),
      lease_record.found ? lease_record.version : 0UL,
      !lease_record.has_query_hidden, &lease_write_result, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  state_lease_acquired = 1;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_after_dequeue_state_lease_hook != NULL) {
    lc_pouch_test_after_dequeue_state_lease_hook(
        lc_pouch_test_after_dequeue_state_lease_context);
  }
#endif
  handle->state_etag =
      lc_client_strdup(client, read_result.found ? read_result.etag : NULL);
  handle->state_lease_id = lc_client_strdup(client, state_lease_id);
  handle->state_txn_id = lc_client_strdup(client, handle->txn_id);
  if ((read_result.found && handle->state_etag == NULL) ||
      handle->state_lease_id == NULL ||
      (handle->txn_id != NULL && handle->state_txn_id == NULL)) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue state lease fields", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  handle->state_lease_expires_at_unix = handle->not_visible_until_unix;
  handle->state_fencing_token = state_fencing_token;
  handle->state_lease = lc_lease_new(
      client, handle->ns, state_key, req->owner, handle->state_lease_id,
      handle->state_txn_id, handle->state_fencing_token, state_version,
      handle->state_etag, handle->meta_etag);
  if (handle->state_lease == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue state lease", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  lc_pouch_lease_refresh_expiration((lc_lease_handle *)handle->state_lease,
                                    handle->state_lease_expires_at_unix);
  ((lc_lease_handle *)handle->state_lease)->pouch_state_key =
      lc_client_strdup(client, state_object_key);
  if (((lc_lease_handle *)handle->state_lease)->pouch_state_key == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue state object key", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  lc_pouch_patch_lease_methods(handle->state_lease);
  *out = message;
  message = NULL;

cleanup:
  if (rc != LC_OK && message != NULL) {
    lc_message_ref rollback_message;

    memset(&rollback_message, 0, sizeof(rollback_message));
    rollback_message.ns = handle->ns;
    rollback_message.queue = handle->queue;
    rollback_message.message_id = handle->message_id;
    rollback_message.lease_id = handle->lease_id;
    rollback_message.txn_id = handle->txn_id;
    rollback_message.fencing_token = handle->fencing_token;
    rollback_message.meta_etag = handle->meta_etag;
    if (state_lease_acquired) {
      rollback_message.state_lease_id = state_lease_id;
      rollback_message.state_fencing_token = state_fencing_token;
    }
    rc = lc_pouch_queue_rollback_delivery_after_failure(
        client, &rollback_message, 1, rc, error);
  }
  lc_free_with_allocator(NULL, state_object_key);
  lc_free_with_allocator(NULL, state_key);
  lc_pouch_lease_record_cleanup(&lease_record);
  lc_pouch_state_write_result_cleanup(&client->allocator, &lease_write_result);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (message != NULL) {
    message->close(message);
  }
  return rc;
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  lc_dequeue_req page_req;
  lc_client_handle *client;
  lc_pouch_queue_scan scan;
  lc_message **messages;
  const char *ns;
  lc_pouch_unix_seconds now;
  long visibility_timeout;
  size_t count;
  size_t capacity;
  size_t i;
  int limit;
  int after_cursor;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_batch requires self, req, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_batch requires queue", NULL, NULL, NULL);
  }
  rc = lc_pouch_client_validate_owner(req->owner, error);
  if (rc != LC_OK) {
    return rc;
  }
  client = (lc_client_handle *)self;
  page_req = *req;
  limit = req->page_size > 0 ? req->page_size : 1;
  messages = NULL;
  count = 0U;
  capacity = 0U;
  rc = LC_OK;
  now = 0L;
  if (req->wait_seconds == 0L) {
    ns = NULL;
    memset(&scan, 0, sizeof(scan));
    rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
    if (rc == LC_OK) {
      rc = lc_pouch_now_unix(&now, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_queue_scan_load(client, ns, req->queue, &scan, error);
    }
    visibility_timeout = req->visibility_timeout_seconds > 0L
                             ? req->visibility_timeout_seconds
                             : 30L;
    after_cursor = req->start_after == NULL || req->start_after[0] == '\0';
    for (i = 0U; rc == LC_OK && i < scan.count && (int)count < limit; ++i) {
      lc_pouch_queue_record *record;
      char *message_lease_key;
      lc_message *message;
      char lease_id[160];
      long message_fencing_token;
      lc_pouch_generation record_version_before;
      lc_message_ref rollback_message;
      int lease_acquired;

      message_lease_key = NULL;
      message_fencing_token = 0L;
      lease_acquired = 0;
      record_version_before = 0UL;
      memset(&rollback_message, 0, sizeof(rollback_message));
      record = &scan.records[i];
      record_version_before = record->version;
      if (!after_cursor) {
        if (strcmp(record->message_id, req->start_after) == 0) {
          after_cursor = 1;
        }
        continue;
      }
      if (!lc_pouch_queue_record_available(record, now)) {
        continue;
      }
      snprintf(lease_id, sizeof(lease_id), "pouch-qlease-%s-%d",
               record->message_id, record->attempts + 1);
      message_lease_key = lc_pouch_queue_message_lease_key_from_meta(
          record->storage_key, error);
      if (message_lease_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
        break;
      }
      rc = lc_pouch_timestamp_add(now, visibility_timeout,
                                  "visibility_timeout_seconds",
                                  &record->not_visible_until_unix, error);
      if (rc == LC_OK) {
        rc = lc_pouch_queue_acquire_message_lease(
            client, ns, req, message_lease_key, lease_id,
            record->not_visible_until_unix, now, &message_fencing_token,
            &lease_acquired, error);
      }
      if (rc != LC_OK) {
        lc_free_with_allocator(NULL, message_lease_key);
        break;
      }
      if (!lease_acquired) {
        lc_free_with_allocator(NULL, message_lease_key);
        continue;
      }
      rollback_message.ns = ns;
      rollback_message.queue = record->queue;
      rollback_message.message_id = record->message_id;
      rollback_message.lease_id = lease_id;
      rollback_message.txn_id = req->txn_id;
      rollback_message.fencing_token = message_fencing_token;
#ifdef LOCKDC_TEST_BUILD
      if (lc_pouch_test_after_queue_lease_claim_hook != NULL) {
        rc = lc_pouch_test_after_queue_lease_claim_hook(
            lc_pouch_test_after_queue_lease_claim_context, error);
        if (rc != LC_OK) {
          rc = lc_pouch_queue_rollback_delivery_after_failure(
              client, &rollback_message, 0, rc, error);
          lc_free_with_allocator(NULL, message_lease_key);
          break;
        }
      }
#endif
      record->attempts += 1;
      lc_free_with_allocator(NULL, record->status);
      lc_free_with_allocator(NULL, record->lease_id);
      lc_free_with_allocator(NULL, record->lease_txn_id);
      record->status = lc_strdup_with_allocator(NULL, "inflight");
      record->lease_id = lc_strdup_with_allocator(NULL, lease_id);
      record->lease_txn_id = lc_strdup_with_allocator(
          NULL, req->txn_id != NULL ? req->txn_id : "");
      record->lease_fencing_token = message_fencing_token;
      record->visibility_timeout_seconds = visibility_timeout;
      if (record->status == NULL || record->lease_id == NULL ||
          record->lease_txn_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue delivery state", NULL,
                          NULL, NULL);
        rc = lc_pouch_queue_rollback_delivery_after_failure(
            client, &rollback_message, 0, rc, error);
        lc_free_with_allocator(NULL, message_lease_key);
        break;
      }
      if (rc == LC_OK) {
        rc = lc_pouch_queue_write_record(client, record, error);
      }
      if (rc != LC_OK) {
        rc = lc_pouch_queue_rollback_delivery_after_failure(
            client, &rollback_message, record->version != record_version_before,
            rc, error);
      }
      lc_free_with_allocator(NULL, message_lease_key);
      if (rc != LC_OK && lc_pouch_queue_retryable_version_conflict(error)) {
        lc_error_cleanup(error);
        lc_error_init(error);
        rc = LC_OK;
        continue;
      }
      message = NULL;
      if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
        if (lc_pouch_test_before_queue_message_build_hook != NULL) {
          lc_pouch_test_before_queue_message_build_hook(
              lc_pouch_test_before_queue_message_build_context);
        }
#endif
        rc = lc_pouch_queue_make_message(client, record, record->message_id,
                                         NULL, &message, error);
      }
#ifdef LOCKDC_TEST_BUILD
      if (rc == LC_OK &&
          lc_pouch_test_after_queue_batch_message_build_hook != NULL) {
        rc = lc_pouch_test_after_queue_batch_message_build_hook(
            lc_pouch_test_after_queue_batch_message_build_context, error);
      }
#endif
      if (rc != LC_OK) {
        rc = lc_pouch_queue_rollback_delivery_after_failure(
            client, &rollback_message, 1, rc, error);
      }
      if (rc == LC_OK) {
        if (count == capacity) {
          lc_message **next;
          size_t next_capacity;

          next_capacity = capacity == 0U ? 4U : capacity * 2U;
          next = (lc_message **)lc_realloc_local(
              messages, next_capacity * sizeof(messages[0]));
          if (next == NULL) {
            rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch dequeue batch", NULL,
                              NULL, NULL);
            rc = lc_pouch_queue_rollback_delivery_after_failure(
                client, &rollback_message, 1, rc, error);
            message->close(message);
            message = NULL;
            break;
          }
          messages = next;
          capacity = next_capacity;
        }
        ((lc_message_handle *)message)->batch_owned = 1;
        messages[count++] = message;
      } else if (message != NULL) {
        message->close(message);
      }
    }
    lc_pouch_queue_scan_cleanup(&scan);
    if (rc == LC_OK) {
      out->messages = messages;
      out->count = count;
      messages = NULL;
    }
    if (messages != NULL) {
      if (rc != LC_OK) {
        rc = lc_pouch_queue_rollback_batch_messages_after_failure(
            client, messages, count, rc, error);
      } else {
        for (i = 0U; i < count; ++i) {
          messages[i]->close(messages[i]);
        }
      }
      lc_free_local(messages);
    }
    return rc;
  }
  while (rc == LC_OK && (int)count < limit) {
    lc_message *message;

    message = NULL;
    rc = lc_pouch_client_dequeue_method(self, &page_req, &message, error);
    if (rc != LC_OK || message == NULL) {
      break;
    }
    if (count == capacity) {
      lc_message **next;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4U : capacity * 2U;
      next = (lc_message **)lc_realloc_local(messages, next_capacity *
                                                           sizeof(messages[0]));
      if (next == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch dequeue batch", NULL, NULL,
                          NULL);
        rc = lc_pouch_queue_rollback_message_after_failure(client, message, rc,
                                                           error);
        message->close(message);
        break;
      }
      messages = next;
      capacity = next_capacity;
    }
    ((lc_message_handle *)message)->batch_owned = 1;
    messages[count++] = message;
    page_req.start_after = message->message_id;
    if (message->next_cursor == NULL || message->next_cursor[0] == '\0') {
      break;
    }
  }
  if (rc == LC_OK) {
    out->messages = messages;
    out->count = count;
    messages = NULL;
  }
  if (messages != NULL) {
    if (rc != LC_OK) {
      rc = lc_pouch_queue_rollback_batch_messages_after_failure(
          client, messages, count, rc, error);
    } else {
      size_t i;

      for (i = 0U; i < count; ++i) {
        messages[i]->close(messages[i]);
      }
    }
    lc_free_local(messages);
  }
  return rc;
}

static int lc_pouch_client_subscribe_common(lc_client *self,
                                            const lc_dequeue_req *req,
                                            const lc_consumer *consumer,
                                            lc_error *error, int with_state) {
  lc_dequeue_req page_req;
  int limit;
  int delivered;
  int rc;

  if (self == NULL || req == NULL || consumer == NULL ||
      consumer->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch subscribe requires self, req, and consumer",
                        NULL, NULL, NULL);
  }
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch subscribe requires queue", NULL, NULL, NULL);
  }
  rc = lc_pouch_client_validate_owner(req->owner, error);
  if (rc != LC_OK) {
    return rc;
  }
  page_req = *req;
  page_req.start_after = NULL;
  limit = req->page_size > 0 ? req->page_size : 1;
  delivered = 0;
  rc = LC_OK;
  while (rc == LC_OK && delivered < limit) {
    lc_message *message;
    lc_message_handle *handle;
    lc_nack_req nack_req;
    lc_error nack_error;
    int terminal;
    int handler_rc;

    message = NULL;
    terminal = 0;
    rc = with_state
             ? lc_pouch_client_dequeue_with_state_method(self, &page_req,
                                                         &message, error)
             : lc_pouch_client_dequeue_method(self, &page_req, &message, error);
    if (rc != LC_OK || message == NULL) {
      break;
    }
    handle = (lc_message_handle *)message;
    handle->terminal_flag = &terminal;
    handler_rc = consumer->handle(consumer->context, message, error);
    if (handler_rc == LC_OK && !terminal) {
      handler_rc = lc_error_set(
          error, LC_ERR_TRANSPORT, 0L,
          "consumer callback must ack() or nack() before returning LC_OK", NULL,
          NULL, NULL);
    } else if (handler_rc != LC_OK && error != NULL && error->code == LC_OK) {
      handler_rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                "consumer callback failed", NULL, NULL, NULL);
    }
    if (handler_rc != LC_OK && !terminal) {
      lc_nack_req_init(&nack_req);
      nack_req.intent = LC_NACK_INTENT_FAILURE;
      nack_req.delay_seconds = 0L;
      lc_error_init(&nack_error);
      if (message->nack(message, &nack_req, &nack_error) == LC_OK) {
        terminal = 1;
      } else {
        lc_error_set(error, nack_error.code, nack_error.http_status,
                     nack_error.message, nack_error.detail,
                     nack_error.server_code, nack_error.correlation_id);
        handler_rc = error != NULL ? error->code : nack_error.code;
      }
      lc_error_cleanup(&nack_error);
    }
    if (!terminal) {
      message->close(message);
    }
    if (handler_rc != LC_OK) {
      rc = handler_rc;
      break;
    }
    ++delivered;
  }
  return rc;
}

int lc_pouch_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_subscribe_common(self, req, consumer, error, 0);
  /* A delivery callback may close the client after terminalizing its message.
   * Keep the subscription's loop state valid until that callback unwinds. */
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_subscribe_common(self, req, consumer, error, 0);
  lc_client_handle_release(client);
  return rc;
}

int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_subscribe_common(self, req, consumer, error, 1);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_subscribe_common(self, req, consumer, error, 1);
  lc_client_handle_release(client);
  return rc;
}

static void lc_pouch_queue_watch_poll_delay(lc_client_handle *client,
                                            const lc_watch_queue_req *req) {
  struct timespec delay;

  if (client != NULL && client->pouch != NULL && req != NULL &&
      lc_pouch_queue_watch_wait(client->pouch, req->ns, req->queue, 100U) >=
          0) {
    return;
  }

  delay.tv_sec = 0;
  delay.tv_nsec = 100L * 1000L * 1000L;
  (void)nanosleep(&delay, NULL);
}

static int lc_pouch_queue_watch_emit(lc_watch_queue_req const *req,
                                     const lc_queue_stats_res *stats,
                                     const lc_watch_handler *handler,
                                     lc_error *error) {
  lc_watch_event event;
  int handler_rc;

  memset(&event, 0, sizeof(event));
  event.ns = lc_strdup_local(stats->ns);
  event.queue = lc_strdup_local(stats->queue);
  event.available = stats->available;
  event.head_message_id = lc_strdup_local(stats->head_message_id);
  event.changed_at_unix = 0L;
  event.correlation_id = lc_strdup_local("pouch-queue-watch");
  if (event.ns == NULL || event.queue == NULL || event.correlation_id == NULL ||
      (stats->head_message_id != NULL && event.head_message_id == NULL)) {
    lc_watch_event_cleanup(&event);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue watch event", NULL,
                        NULL, NULL);
  }
  if (lc_pouch_now_unix(&event.changed_at_unix, error) != LC_OK) {
    lc_watch_event_cleanup(&event);
    return error != NULL ? error->code : LC_ERR_TRANSPORT;
  }
  (void)req;
  handler_rc = handler->handle(handler->context, &event, error);
  lc_watch_event_cleanup(&event);
  if (handler_rc) {
    return LC_OK;
  }
  if (error != NULL && error->code != LC_OK) {
    return error->code;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "pouch queue watch handler stopped", NULL, NULL, NULL);
}

static int lc_pouch_client_watch_queue_method_impl(
    lc_client *self, const lc_watch_queue_req *req,
    const lc_watch_handler *handler, lc_error *error) {
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  char *last_head_message_id;
  int have_signature;
  int last_available;
  int rc;

  if (self == NULL || req == NULL || handler == NULL ||
      handler->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch watch_queue requires self, req, and handler",
                        NULL, NULL, NULL);
  }
  lc_queue_stats_req_init(&stats_req);
  stats_req.ns = req->ns;
  stats_req.queue = req->queue;
  last_head_message_id = NULL;
  have_signature = 0;
  last_available = 0;
  rc = LC_OK;
  while (rc == LC_OK) {
    int changed;

    memset(&stats, 0, sizeof(stats));
    rc = lc_pouch_client_queue_stats_method(self, &stats_req, &stats, error);
    if (rc != LC_OK) {
      break;
    }
    changed =
        !have_signature || last_available != stats.available ||
        ((last_head_message_id == NULL) != (stats.head_message_id == NULL)) ||
        (last_head_message_id != NULL && stats.head_message_id != NULL &&
         strcmp(last_head_message_id, stats.head_message_id) != 0);
    if (changed) {
      rc = lc_pouch_queue_watch_emit(req, &stats, handler, error);
      if (rc != LC_OK) {
        lc_queue_stats_res_cleanup(&stats);
        break;
      }
      lc_free_local(last_head_message_id);
      last_head_message_id = lc_strdup_local(stats.head_message_id);
      if (stats.head_message_id != NULL && last_head_message_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue watch signature",
                          NULL, NULL, NULL);
        lc_queue_stats_res_cleanup(&stats);
        break;
      }
      last_available = stats.available;
      have_signature = 1;
    }
    lc_queue_stats_res_cleanup(&stats);
    if (rc == LC_OK) {
      lc_pouch_queue_watch_poll_delay((lc_client_handle *)self, req);
    }
  }
  lc_free_local(last_head_message_id);
  return rc;
}

int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL) {
    return lc_pouch_client_watch_queue_method_impl(self, req, handler, error);
  }
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_watch_queue_method_impl(self, req, handler, error);
  lc_client_handle_release(client);
  return rc;
}

static int lc_pouch_client_query_method_impl(lc_client *self,
                                             const lc_query_req *req,
                                             lc_sink *dst, lc_query_res *out,
                                             lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_scan_context scan;
  lql *runtime;
  lql_selector *selector;
  lql_error lql_error_value;
  lql_status status;
  const char *ns = NULL;
  const char *effective_engine;
  char *owned_engine;
  int use_index_predicate;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_query_request_validate_selector(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query does not accept fields_json", NULL, NULL,
                        "pouch");
  }
  if (!lc_pouch_query_request_has_selector(req)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires selector_json or selector_lql",
                        NULL, NULL, "pouch");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "documents") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query currently supports only document return "
                        "mode",
                        NULL, NULL, "pouch");
  }
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0 && strcmp(req->engine, "index") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query engine must be scan or index", NULL, NULL,
                        "pouch");
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  owned_engine = NULL;
  rc = lc_pouch_client_query_engine(client, ns, req->engine, &effective_engine,
                                    &owned_engine, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      strcmp(effective_engine, "scan") == 0 &&
      lc_pouch_client_can_use_query_fallback(client, req->engine, "index")) {
    effective_engine = "index";
  }
  use_index_predicate = strcmp(effective_engine, "index") == 0;
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      (!use_index_predicate || strcmp(req->refresh, "wait_for") != 0)) {
    lc_free_with_allocator(NULL, owned_engine);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query refresh is only supported as wait_for on "
                        "indexed queries",
                        NULL, NULL, "pouch");
  }
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    lc_free_with_allocator(NULL, owned_engine);
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  rc = lc_pouch_query_parse_selector(runtime, req, &selector, error);
  if (rc != LC_OK) {
    runtime->destroy(runtime);
    lc_free_with_allocator(NULL, owned_engine);
    return rc;
  }

  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.ns = ns;
  scan.request = req;
  scan.limit = lc_pouch_query_effective_limit(req->limit);
  scan.sink = dst;
  scan.runtime = runtime;
  scan.selector = selector;
  scan.emit_documents = 1;
  scan.track_index_seq = use_index_predicate;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.start_after_key, error);
  if (rc == LC_OK) {
    if (use_index_predicate) {
      lc_pouch_generation flushed_seq;

      flushed_seq = 0UL;
      rc = lc_pouch_query_run_index_predicate(&scan, &flushed_seq, error);
    } else {
      rc = lc_pouch_query_run_scan_predicate(&scan, error);
    }
  }
  if (rc == LC_OK) {
    out->return_mode = lc_strdup_local("documents");
    out->metadata_json = use_index_predicate
                             ? lc_pouch_query_index_metadata_string(
                                   scan.seen, scan.matched, error)
                             : lc_pouch_query_scan_metadata_string(
                                   scan.seen, scan.matched, error);
    out->correlation_id = lc_strdup_local("pouch-query");
    out->index_seq = use_index_predicate ? scan.index_seq : 0UL;
    if (scan.page_full && scan.last_emitted_key != NULL) {
      out->cursor = lc_pouch_query_cursor_string(scan.last_emitted_key, error);
    }
    if (out->return_mode == NULL || out->metadata_json == NULL ||
        out->correlation_id == NULL ||
        (scan.page_full && scan.last_emitted_key != NULL &&
         out->cursor == NULL)) {
      lc_query_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch query response", NULL,
                              NULL, NULL);
    }
  }
  runtime->selector_destroy(runtime, selector);
  runtime->destroy(runtime);
  lc_free_with_allocator(NULL, owned_engine);
  lc_free_local(scan.last_emitted_key);
  lc_free_with_allocator(&client->allocator, scan.stream_buffer);
  return rc;
}

int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL)
    return lc_pouch_client_query_method_impl(self, req, dst, out, error);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_query_method_impl(self, req, dst, out, error);
  lc_client_handle_release(client);
  return rc;
}

static int
lc_pouch_client_query_keys_method_impl(lc_client *self, const lc_query_req *req,
                                       const lc_query_key_handler *handler,
                                       void *context, lc_query_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_scan_context scan;
  lql *runtime;
  lql_selector *selector;
  lql_error lql_error_value;
  lql_status status;
  const char *ns = NULL;
  const char *effective_engine;
  char *owned_engine;
  int has_selector;
  int use_index_summary;
  int use_index_predicate;
  int rc;

  if (self == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys requires self, req, handler, and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys does not accept fields_json", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_query_request_validate_selector(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  has_selector = lc_pouch_query_request_has_selector(req);
  use_index_summary = 0;
  use_index_predicate = 0;
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0 && strcmp(req->engine, "index") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys engine must be scan or index", NULL,
                        NULL, "pouch");
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  owned_engine = NULL;
  rc = lc_pouch_client_query_engine(client, ns, req->engine, &effective_engine,
                                    &owned_engine, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      strcmp(effective_engine, "scan") == 0 &&
      lc_pouch_client_can_use_query_fallback(client, req->engine, "index")) {
    effective_engine = "index";
  }
  if (!has_selector && strcmp(effective_engine, "index") == 0) {
    use_index_summary = 1;
  }
  if (has_selector && strcmp(effective_engine, "index") == 0) {
    use_index_predicate = 1;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      (!(use_index_summary || use_index_predicate) ||
       strcmp(req->refresh, "wait_for") != 0)) {
    lc_free_with_allocator(NULL, owned_engine);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys refresh is only supported as "
                        "wait_for on indexed queries",
                        NULL, NULL, "pouch");
  }
  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.ns = ns;
  scan.request = req;
  scan.limit = lc_pouch_query_effective_limit(req->limit);
  scan.handler = handler;
  scan.handler_context = context;
  scan.track_index_seq = use_index_summary || use_index_predicate;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.start_after_key, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, owned_engine);
    return rc;
  }
  if (use_index_summary) {
    lc_pouch_generation flushed_seq;
    int retried_repair;
    int validate_current;

    flushed_seq = 0UL;
    retried_repair = 0;
    validate_current =
        req->refresh != NULL && strcmp(req->refresh, "wait_for") == 0;
  query_summary_index:
    rc = lc_pouch_query_flush_summary_index(client, scan.ns, validate_current,
                                            &flushed_seq, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_visit(client->pouch, scan.ns,
                                      lc_pouch_query_index_summary_visit, &scan,
                                      &scan.index_seq, error);
    }
    if (rc == LC_ERR_INVALID && !validate_current && !retried_repair &&
        scan.seen == 0U && scan.matched == 0U && scan.emitted == 0U &&
        scan.last_emitted_key == NULL) {
      if (error != NULL) {
        lc_error_cleanup(error);
        lc_error_init(error);
      }
      lc_pouch_query_scan_reset_page_state(&scan);
      flushed_seq = 0UL;
      retried_repair = 1;
      validate_current = 1;
      rc = LC_OK;
      goto query_summary_index;
    }
    if (rc == LC_OK && scan.index_seq < flushed_seq) {
      scan.index_seq = flushed_seq;
    }
    if (rc == LC_OK) {
      out->return_mode = lc_strdup_local("keys");
      out->metadata_json = lc_pouch_query_index_summary_metadata_string(
          scan.seen, scan.matched, error);
      out->correlation_id = lc_strdup_local("pouch-query-keys");
      out->index_seq = scan.index_seq;
      if (scan.page_full && scan.last_emitted_key != NULL) {
        out->cursor =
            lc_pouch_query_cursor_string(scan.last_emitted_key, error);
      }
      if (out->return_mode == NULL || out->metadata_json == NULL ||
          out->correlation_id == NULL ||
          (scan.page_full && scan.last_emitted_key != NULL &&
           out->cursor == NULL)) {
        lc_query_res_cleanup(out);
        rc = error != NULL && error->code != LC_OK
                 ? error->code
                 : lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to allocate pouch query response", NULL,
                                NULL, NULL);
      }
    }
    lc_free_with_allocator(NULL, owned_engine);
    lc_free_local(scan.last_emitted_key);
    return rc;
  }
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    lc_free_with_allocator(NULL, owned_engine);
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  if (has_selector) {
    rc = lc_pouch_query_parse_selector(runtime, req, &selector, error);
    if (rc != LC_OK) {
      runtime->destroy(runtime);
      lc_free_with_allocator(NULL, owned_engine);
      return rc;
    }
  }
  scan.runtime = runtime;
  scan.selector = selector;
  if (use_index_predicate) {
    lc_pouch_generation flushed_seq;

    flushed_seq = 0UL;
    rc = lc_pouch_query_run_index_predicate(&scan, &flushed_seq, error);
  } else {
    rc = lc_pouch_query_run_scan_predicate(&scan, error);
  }
  if (rc == LC_OK) {
    out->return_mode = lc_strdup_local("keys");
    out->metadata_json = use_index_predicate
                             ? lc_pouch_query_index_metadata_string(
                                   scan.seen, scan.matched, error)
                             : lc_strdup_local("{\"engine\":\"scan\"}");
    out->correlation_id = lc_strdup_local("pouch-query-keys");
    out->index_seq = use_index_predicate ? scan.index_seq : 0UL;
    if (scan.page_full && scan.last_emitted_key != NULL) {
      out->cursor = lc_pouch_query_cursor_string(scan.last_emitted_key, error);
    }
    if (out->return_mode == NULL || out->metadata_json == NULL ||
        out->correlation_id == NULL ||
        (scan.page_full && scan.last_emitted_key != NULL &&
         out->cursor == NULL)) {
      lc_query_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch query response", NULL,
                              NULL, NULL);
    }
  }
  if (selector != NULL) {
    runtime->selector_destroy(runtime, selector);
  }
  runtime->destroy(runtime);
  lc_free_with_allocator(NULL, owned_engine);
  lc_free_local(scan.last_emitted_key);
  lc_free_with_allocator(&client->allocator, scan.stream_buffer);
  return rc;
}

int lc_pouch_client_query_keys_method(lc_client *self, const lc_query_req *req,
                                      const lc_query_key_handler *handler,
                                      void *context, lc_query_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_pouch_client_query_keys_method_impl(self, req, handler, context,
                                                  out, error);
  }
  /* Key handlers can close Pouch clients. Pin the concrete owner before any
   * scan can invoke one and release it after all scan state has unwound. */
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  rc = lc_pouch_client_query_keys_method_impl(self, req, handler, context, out,
                                              error);
  lc_client_handle_release(client);
  return rc;
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_namespace_config_record record;
  const char *ns = NULL;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch get_namespace_config requires self, req, and out", NULL, NULL,
        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_config_read(client, ns, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_config_response(out, ns, &record, error);
  lc_pouch_namespace_config_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_namespace_config_record record;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  const char *ns = NULL;
  const char *preferred_engine;
  const char *fallback_engine;
  lc_pouch_txn_buffer body;
  char *key;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch update_namespace_config requires self, req, and out", NULL, NULL,
        NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_config_read(client, ns, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->preferred_engine == NULL && req->fallback_engine == NULL) {
    rc = lc_pouch_namespace_config_response(out, ns, &record, error);
    lc_pouch_namespace_config_record_cleanup(&record);
    return rc;
  }
  preferred_engine =
      req->preferred_engine != NULL
          ? lc_pouch_namespace_config_normalize_preferred(req->preferred_engine)
          : record.preferred_engine;
  fallback_engine =
      req->fallback_engine != NULL
          ? lc_pouch_namespace_config_normalize_fallback(req->fallback_engine)
          : record.fallback_engine;
  rc = lc_pouch_namespace_config_set_record(&record, preferred_engine,
                                            fallback_engine, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_config_record_cleanup(&record);
    return rc;
  }
  memset(&body, 0, sizeof(body));
  rc = lc_pouch_namespace_config_build_record(&record, &body, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_config_record_cleanup(&record);
    return rc;
  }
  key = lc_pouch_namespace_config_key(ns, error);
  if (key == NULL) {
    lc_pouch_txn_buffer_cleanup(&body);
    lc_pouch_namespace_config_record_cleanup(&record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  rc = lc_source_from_memory(body.bytes, body.length, &source, error);
  options.content_type = LC_POUCH_NAMESPACE_CONFIG_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.object_record = 1;
  options.expected_etag =
      req->if_etag != NULL && req->if_etag[0] != '\0' ? req->if_etag : NULL;
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, ns, key, source, &options,
                              &write_result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    lc_free_with_allocator(NULL, record.etag);
    record.etag = lc_strdup_with_allocator(NULL, write_result.etag);
    if (record.etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace config etag", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_config_response(out, ns, &record, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  lc_pouch_txn_buffer_cleanup(&body);
  lc_free_with_allocator(NULL, key);
  lc_pouch_namespace_config_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_flush_index_method(lc_client *self,
                                       const lc_index_flush_req *req,
                                       lc_index_flush_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_index_flush_result index_result;
  const char *ns = NULL;
  const char *mode;
  lc_pouch_generation index_seq;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch flush_index requires self, req, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->mode != NULL && req->mode[0] != '\0' &&
      strcmp(req->mode, "wait") != 0 && strcmp(req->mode, "sync") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch flush_index mode must be wait or sync", NULL,
                        NULL, "pouch");
  }
  client = (lc_client_handle *)self;
  if (client->pouch == NULL || !client->pouch->query_indexing_enabled) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch query indexing is disabled; flush_index is unavailable", NULL,
        NULL, "pouch");
  }
  rc = lc_pouch_client_public_namespace(client, req->ns, &ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  mode = req->mode != NULL && req->mode[0] != '\0' ? req->mode : "wait";
  /* Go disk's wait flush publishes state already accepted by the indexer.
   * Replaying the state log or deserializing every newly published artifact
   * here is neither required for correctness nor part of that contract. */
  index_seq = 0UL;
  memset(&index_result, 0, sizeof(index_result));
  if (strcmp(mode, "sync") == 0) {
    rc = lc_pouch_state_query_index_seq(client->pouch, ns, &index_seq, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_query_index_flush_validated(client->pouch, ns, index_seq,
                                              &index_result, error);
  } else {
    rc = lc_pouch_state_query_index_seq(client->pouch, ns, &index_seq, error);
    if (rc != LC_OK) {
      return rc;
    }
    /* A shared-writer compaction can deliberately retire an index manifest:
     * canonical state remains valid, but a newly opened dispatcher must repair
     * that derived view before using it as a recovery boundary. The ordinary
     * current-manifest path remains a cheap trusted-sequence check; this does
     * not turn wait into synchronous artifact validation. */
    rc = lc_pouch_query_index_ensure_current(client->pouch, ns, index_seq, 0,
                                             &index_result, error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  out->ns = lc_strdup_local(ns);
  out->mode = lc_strdup_local(mode);
  out->flush_id =
      lc_strdup_local(index_result.repaired ? "pouch-query-index-repair"
                                            : "pouch-query-index-flush");
  out->accepted = 1;
  out->flushed = 1;
  out->pending = 0;
  out->index_seq = index_result.index_seq;
  out->correlation_id = lc_strdup_local("pouch-index-flush");
  if (out->ns == NULL || out->mode == NULL || out->flush_id == NULL ||
      out->correlation_id == NULL) {
    lc_index_flush_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index flush response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_txn_validate_target_backend(lc_pouch *pouch,
                                                const char *target_backend_hash,
                                                lc_error *error) {
  char local_backend_hash[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  const char *target;
  size_t target_length;
  size_t local_length;
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction target requires an open root", NULL,
                        NULL, NULL);
  }
  lc_pouch_txn_trim_bounds(target_backend_hash, &target, &target_length);
  if (target_length == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_backend_hash(pouch, local_backend_hash, error);
  if (rc != LC_OK) {
    return rc;
  }
  local_length = strlen(local_backend_hash);
  if (target_length != local_length ||
      strncmp(target, local_backend_hash, target_length) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction target backend hash does not "
                        "match root",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

/* Called under xid authority, before entering .txns mutation authority. Shared
 * roots have one projection mutex, so participant reads must finish first. */
static int
lc_pouch_txn_validate_retention(lc_client_handle *client, const char *txn_id,
                                const lc_txn_participant *participants,
                                size_t count, lc_pouch_unix_seconds commit_at,
                                lc_error *error) {
  size_t i;
  int rc = LC_OK;
  char local_hash[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];

  rc = lc_pouch_backend_hash(client->pouch, local_hash, error);
  for (i = 0; rc == LC_OK && i < count; ++i) {
    lc_pouch_state_read_result staged = {0};
    lc_pouch_unix_seconds deadline;
    const char *ns = NULL;
    const char *backend;
    size_t backend_length;
    char *key;

    lc_pouch_txn_trim_bounds(participants[i].backend_hash, &backend,
                             &backend_length);
    if ((backend_length != 0 &&
         (backend_length != strlen(local_hash) ||
          strncmp(backend, local_hash, backend_length) != 0)) ||
        lc_pouch_xa_participant(&participants[i]) ||
        lc_pouch_queue_is_message_lease_key(participants[i].key) ||
        lc_pouch_queue_is_state_lease_key(participants[i].key))
      continue;
    rc = lc_pouch_client_public_namespace(client, participants[i].ns, &ns,
                                          error);
    if (rc != LC_OK)
      break;
    key = lc_pouch_staged_storage_key(participants[i].key, txn_id, error);
    if (key == NULL)
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    rc = lc_pouch_state_read_metadata(client->pouch, ns, key, &staged, error);
    if (rc == LC_OK && staged.found && !staged.staged_delete_marker) {
      if (staged.retention < 0 && commit_at < 0)
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "retention-bearing commit is missing its timestamp",
                          NULL, NULL, "pouch");
      else
        rc = lc_pouch_retention_resolve(staged.retention, commit_at, &deadline,
                                        error);
    }
    lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
    lc_free_with_allocator(NULL, key);
  }
  return rc;
}

static int lc_pouch_txn_decision_record_locked(void *context, lc_error *error) {
  lc_pouch_txn_decision_context *ctx;
  lc_pouch_state_read_result read_result;
  lc_pouch_txn_buffer buffer;
  lc_pouch_state_write_options options;
  lc_txn_decision_req existing_request;
  lc_source *source;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *record_bytes;
  int write_record;
  int choosing_commit;
  int rc;

  ctx = (lc_pouch_txn_decision_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->request == NULL ||
      ctx->state == NULL || ctx->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction decision lock requires context",
                        NULL, NULL, NULL);
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&buffer, 0, sizeof(buffer));
  memset(&options, 0, sizeof(options));
  lc_txn_decision_req_init(&existing_request);
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  record_bytes = NULL;
  write_record = 0;
  rc = lc_pouch_state_read_locked(ctx->client->pouch, LC_POUCH_TXN_NAMESPACE,
                                  ctx->key, &read_result, error);
  if (rc == LC_OK) {
    ctx->response_index = read_result.index_seq;
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && read_result.found) {
    record_bytes = (char *)lc_alloc_with_allocator(NULL, length + 1U);
    if (record_bytes == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction decision record",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK && read_result.found) {
    memcpy(record_bytes, bytes, length);
    record_bytes[length] = '\0';
    rc = lc_pouch_txn_parse_record(record_bytes, length, &ctx->record, error);
  }
  if (rc == LC_OK && read_result.found) {
    existing_request.txn_id = ctx->request->txn_id;
    existing_request.participants = ctx->record.participants;
    existing_request.participant_count = ctx->record.participant_count;
    existing_request.expires_at_unix = ctx->record.expires_at_unix;
    existing_request.tc_term = ctx->record.tc_term;
    existing_request.target_backend_hash = ctx->record.target_backend_hash;
    rc = lc_pouch_txn_validate_participants(&existing_request, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_txn_validate_target_backend(
        ctx->client->pouch, ctx->record.target_backend_hash, error);
  }
  if (rc == LC_OK) {
    choosing_commit =
        ctx->record.state == NULL || strcmp(ctx->record.state, "commit") != 0;
    rc = lc_pouch_txn_record_merge_request(
        &ctx->record, ctx->request, ctx->state, ctx->owner_sealed,
        &write_record, &ctx->apply_decision, error);
  }
  if (rc == LC_OK && strcmp(ctx->state, "commit") == 0) {
    size_t i;
    for (i = 0U; i < ctx->record.participant_count; ++i)
      if (lc_pouch_xa_participant(&ctx->record.participants[i]) &&
          ctx->record.votes[i] != 1U) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "XA commit requires every external prepare vote",
                          NULL, NULL, "pouch-xa");
        break;
      }
  }
  if (rc == LC_OK && write_record && choosing_commit &&
      strcmp(ctx->record.state, "commit") == 0 &&
      ctx->record.commit_at_unix == 0) {
    if (ctx->commit_at_unix <= 0)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit requires a validated timestamp", NULL,
                        NULL, "pouch");
    else
      ctx->record.commit_at_unix = ctx->commit_at_unix;
  }
  if (rc == LC_OK && write_record) {
    rc = lc_pouch_txn_build_record(&ctx->record, &buffer, error);
  }
  if (rc == LC_OK && write_record) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  }
  if (rc == LC_OK && write_record) {
    options.content_type = "application/x-lockdc-pouch-txn";
    options.object_record = 1;
    if (read_result.found) {
      options.has_expected_version = 1;
      options.expected_version = read_result.version;
    }
    rc = lc_pouch_state_write_locked(ctx->client->pouch, LC_POUCH_TXN_NAMESPACE,
                                     ctx->key, source, &options,
                                     &ctx->write_result, error);
    if (rc == LC_OK) {
      ctx->response_index = ctx->write_result.index_seq;
    }
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_free_with_allocator(NULL, record_bytes);
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
  return rc;
}

static size_t
lc_pouch_txn_record_find_participant(const lc_pouch_txn_record *record,
                                     const char *ns, const char *key) {
  size_t i;

  if (record == NULL || ns == NULL || key == NULL) {
    return (size_t)-1;
  }
  for (i = 0U; i < record->participant_count; ++i) {
    if (strcmp(record->participants[i].ns, ns) == 0 &&
        strcmp(record->participants[i].key, key) == 0) {
      return i;
    }
  }
  return (size_t)-1;
}

static int
lc_pouch_txn_record_all_commit_votes(const lc_pouch_txn_record *record) {
  size_t i;

  if (record == NULL || record->participant_count == 0U ||
      record->votes == NULL) {
    return 0;
  }
  for (i = 0U; i < record->participant_count; ++i) {
    if (record->votes[i] != 1U) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_txn_vote_record_locked(void *context, lc_error *error) {
  lc_pouch_txn_vote_context *ctx;
  lc_pouch_state_read_result read_result;
  lc_pouch_txn_buffer buffer;
  lc_pouch_state_write_options options;
  lc_source *source;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t participant_index;
  unsigned char requested_vote;
  lc_pouch_unix_seconds now;
  char *record_bytes;
  int write_record;
  int rc;

  ctx = (lc_pouch_txn_vote_context *)context;
  if (ctx == NULL || ctx->client == NULL || ctx->txn_id == NULL ||
      ctx->ns == NULL || ctx->participant_key == NULL ||
      ctx->record_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction vote requires context", NULL, NULL,
                        NULL);
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&buffer, 0, sizeof(buffer));
  memset(&options, 0, sizeof(options));
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  record_bytes = NULL;
  write_record = 0;
  requested_vote = ctx->preparing ? 3U : ctx->rollback ? 2U : 1U;
  now = 0L;
  rc = lc_pouch_state_read_locked(ctx->client->pouch, LC_POUCH_TXN_NAMESPACE,
                                  ctx->record_key, &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch transaction record not found for lease release",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    ctx->response_index = read_result.index_seq;
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    record_bytes = (char *)lc_alloc_with_allocator(NULL, length + 1U);
    if (record_bytes == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction vote record",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(record_bytes, bytes, length);
    record_bytes[length] = '\0';
    rc = lc_pouch_txn_parse_record(record_bytes, length, &ctx->record, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_validate_target_backend(
        ctx->client->pouch, ctx->record.target_backend_hash, error);
  }
  if (rc == LC_OK && strcmp(ctx->record.state, "prepare") == 0 &&
      ctx->record.expires_at_unix > 0L) {
    rc = lc_pouch_now_unix(&now, error);
    if (rc == LC_OK && ctx->record.expires_at_unix <= now) {
      requested_vote = 2U;
    }
  }
  participant_index = (size_t)-1;
  if (rc == LC_OK) {
    participant_index = lc_pouch_txn_record_find_participant(
        &ctx->record, ctx->ns, ctx->participant_key);
    if (participant_index == (size_t)-1) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease is not a transaction participant", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK && strcmp(ctx->record.state, "prepare") == 0) {
    if (ctx->record.votes[participant_index] != 0U &&
        ctx->record.votes[participant_index] != 3U &&
        ctx->record.votes[participant_index] != requested_vote) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction participant already voted", NULL,
                        NULL, NULL);
    } else {
      if (ctx->record.votes[participant_index] == 0U ||
          (ctx->record.votes[participant_index] == 3U &&
           requested_vote != 3U)) {
        ctx->record.votes[participant_index] = requested_vote;
        write_record = 1;
      }
      if (rc == LC_OK && requested_vote == 2U) {
        rc = lc_pouch_txn_record_replace_string(&ctx->record.state, "rollback",
                                                error);
        if (rc == LC_OK) {
          write_record = 1;
          ctx->apply_decision = 1;
        }
      } else if (rc == LC_OK &&
                 lc_pouch_txn_record_all_commit_votes(&ctx->record)) {
        rc = lc_pouch_txn_record_replace_string(&ctx->record.state, "commit",
                                                error);
        if (rc == LC_OK) {
          write_record = 1;
          ctx->apply_decision = 1;
        }
      }
    }
  } else if (rc == LC_OK && (strcmp(ctx->record.state, "commit") == 0 ||
                             strcmp(ctx->record.state, "rollback") == 0)) {
    if (requested_vote == 3U)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA prepare cannot start after a durable decision",
                        NULL, NULL, "pouch-xa");
    else
      ctx->apply_decision = 1;
  } else if (rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction vote found unsupported state", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK && write_record && strcmp(ctx->record.state, "commit") == 0 &&
      ctx->record.commit_at_unix == 0) {
    if (ctx->commit_at_unix <= 0)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit vote requires a validated timestamp",
                        NULL, NULL, "pouch");
    else
      ctx->record.commit_at_unix = ctx->commit_at_unix;
  }
  if (rc == LC_OK && write_record) {
    rc = lc_pouch_txn_build_record(&ctx->record, &buffer, error);
  }
  if (rc == LC_OK && write_record) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  }
  if (rc == LC_OK && write_record) {
    options.content_type = "application/x-lockdc-pouch-txn";
    options.object_record = 1;
    options.has_expected_version = 1;
    options.expected_version = read_result.version;
    rc = lc_pouch_state_write_locked(ctx->client->pouch, LC_POUCH_TXN_NAMESPACE,
                                     ctx->record_key, source, &options,
                                     &ctx->write_result, error);
    if (rc == LC_OK) {
      ctx->response_index = ctx->write_result.index_seq;
    }
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_free_with_allocator(NULL, record_bytes);
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_read_result_cleanup(&ctx->client->allocator, &read_result);
  return rc;
}

static int lc_pouch_txn_record_vote(lc_client_handle *client,
                                    const char *txn_id, const char *ns,
                                    const char *participant_key, int rollback,
                                    int preparing, int *decision_ready,
                                    int *native_completion, lc_error *error) {
  lc_pouch_txn_vote_context context;
  lc_pouch_txn_record preview;
  char *record_key;
  int rc, retire = 0;

  if (decision_ready == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction vote requires decision output", NULL,
                        NULL, NULL);
  }
  *decision_ready = 0;
  if (native_completion != NULL)
    *native_completion = 0;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_txn_decision_hook != NULL) {
    rc = lc_pouch_test_before_txn_decision_hook(
        lc_pouch_test_before_txn_decision_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
#endif
  record_key = lc_pouch_txn_key(txn_id, error);
  if (record_key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  memset(&context, 0, sizeof(context));
  context.client = client;
  context.txn_id = txn_id;
  context.ns = ns;
  context.participant_key = participant_key;
  context.rollback = rollback;
  context.preparing = preparing;
  context.record_key = record_key;
  /* Every caller holds the xid guard. Shared roots have one non-recursive
   * projection mutex: validate/seal .xa authority before entering .txns. */
  rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &preview, error);
  if (rc == LC_OK && preview.state != NULL &&
      strcmp(preview.state, "prepare") == 0)
    rc = lc_pouch_xa_owner_check(client, txn_id, preview.tc_term, NULL, 0, NULL,
                                 error);
  if (rc == LC_OK && preview.state != NULL &&
      strcmp(preview.state, "prepare") == 0)
    rc = lc_pouch_xa_seal_vote(client, txn_id, preview.tc_term, error);
  if (rc == LC_OK && !rollback && !preparing && preview.state != NULL &&
      strcmp(preview.state, "prepare") == 0) {
    size_t i;
    int ready = 1;
    for (i = 0; i < preview.participant_count; ++i) {
      if (strcmp(preview.participants[i].ns, ns) == 0 &&
          strcmp(preview.participants[i].key, participant_key) == 0)
        continue;
      if (preview.votes[i] != 1U)
        ready = 0;
    }
    if (ready) {
      rc = lc_pouch_now_unix(&context.commit_at_unix, error);
      if (rc == LC_OK)
        rc = lc_pouch_txn_validate_retention(
            client, txn_id, preview.participants, preview.participant_count,
            context.commit_at_unix, error);
    }
  }
  if (rc == LC_OK && native_completion != NULL && !preparing &&
      preview.state != NULL && strcmp(preview.state, "prepare") == 0 &&
      preview.tc_term == 0 && preview.participant_count == 1 &&
      strcmp(preview.participants[0].ns, ns) == 0 &&
      strcmp(preview.participants[0].key, participant_key) == 0)
    retire = 1;
  lc_pouch_txn_record_cleanup(&preview);
  if (rc == LC_OK)
    rc = lc_pouch_state_with_key_lock(
        client->pouch, LC_POUCH_TXN_NAMESPACE, record_key,
        lc_pouch_txn_vote_record_locked, &context, error);
  if (rc == LC_OK) {
    *decision_ready = context.apply_decision;
    if (native_completion != NULL)
      *native_completion = retire && context.apply_decision;
  }
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &context.write_result);
  lc_pouch_txn_record_cleanup(&context.record);
  lc_free_with_allocator(NULL, record_key);
  return rc;
}

/* Read the same authoritative record used by native implicit-XA releases.
 * No external participant owns a separate commit/rollback journal.
 */
static int lc_pouch_xa_read_txn(lc_pouch *pouch, const char *txn_id,
                                lc_pouch_txn_record *record, lc_error *error) {
  lc_pouch_state_read_result result;
  lc_sink *sink = NULL;
  const void *bytes = NULL;
  size_t length = 0U;
  int rc;
  memset(record, 0, sizeof(*record));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_state_read(pouch, LC_POUCH_TXN_NAMESPACE, txn_id, &result,
                           error);
  if (rc == LC_OK && result.found)
    rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK && result.found)
    rc = lc_copy(result.body, sink, NULL, error);
  if (rc == LC_OK && result.found)
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  if (rc == LC_OK && result.found)
    rc = lc_pouch_txn_parse_record((const char *)bytes, length, record, error);
  if (sink != NULL)
    sink->close(sink);
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &result);
  return rc;
}

typedef struct lc_pouch_retention_cleanup {
  lc_pouch *pouch;
  const char *ns;
  lc_pouch_retention_work work;
  lc_pouch_retention_candidate candidate;
  lc_pouch_lease_record observed;
  lc_pouch_txn_record journal;
  lc_pouch_unix_seconds now;
  char new_xid[LC_XID_STRING_SIZE];
  int skip;
  int slot_claimed;
  int prepare_journal;
  int retire_journal;
} lc_pouch_retention_cleanup;

static void
lc_pouch_retention_cleanup_dispose(lc_pouch_retention_cleanup *ctx) {
  lc_pouch_retention_work_cleanup(ctx->pouch, &ctx->work);
  lc_free_with_allocator(&ctx->pouch->allocator, ctx->candidate.key);
  lc_pouch_lease_record_cleanup(&ctx->observed);
  lc_pouch_txn_record_cleanup(&ctx->journal);
}

static int lc_pouch_retention_load_locked(void *context, lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_state_metadata_view view;
  lc_pouch_state_read_result fallback = {0};
  int rc = lc_pouch_retention_work_read(ctx->pouch, ctx->ns, &ctx->work, error);
  if (rc != LC_OK || !ctx->work.present ||
      ctx->work.phase != LC_POUCH_RETENTION_IDLE)
    return rc;
  rc = lc_pouch_now_unix(&ctx->now, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_retention_select_locked(ctx->pouch, ctx->ns, ctx->now,
                                                &ctx->candidate, error);
  if (rc == LC_OK && ctx->candidate.key == NULL) {
    if (!ctx->candidate.has_expiring)
      rc = lc_pouch_state_retention_retire_hint_locked(ctx->pouch, ctx->ns,
                                                       error);
    return rc;
  }
  if (rc == LC_OK)
    rc = lc_pouch_state_read_metadata_view_locked(
        ctx->pouch, ctx->ns, ctx->candidate.key, &view, &fallback, error);
  if (rc == LC_OK)
    rc = lc_pouch_lease_record_parse(view.metadata, view.metadata_length,
                                     view.version, &ctx->observed, error);
  if (rc == LC_OK && ctx->observed.found && ctx->observed.lease_id[0] != '\0' &&
      (ctx->observed.expires_at_unix > ctx->now || ctx->observed.txn_explicit))
    ctx->skip = 1;
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &fallback);
  return rc;
}

static int lc_pouch_retention_write_work_locked(void *context,
                                                lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  int rc = lc_pouch_retention_work_write(
      ctx->pouch, ctx->ns, ctx->work.phase, ctx->work.txn_id, ctx->work.key,
      ctx->work.fencing_token, ctx->work.state_version, error);
  if (rc == LC_OK && ctx->work.phase == LC_POUCH_RETENTION_IDLE) {
    int expiring = 1;
    rc = lc_pouch_state_retention_has_expiring_locked(ctx->pouch, ctx->ns,
                                                      &expiring, error);
    if (rc == LC_OK && !expiring)
      rc = lc_pouch_state_retention_retire_hint_locked(ctx->pouch, ctx->ns,
                                                       error);
  }
  return rc;
}

static int lc_pouch_retention_claim_slot_locked(void *context,
                                                lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_retention_work current = {0};
  lc_pouch_state_read_result state = {0};
  lc_pouch_lease_record lease = {0};
  int rc = lc_pouch_retention_work_read(ctx->pouch, ctx->ns, &current, error);
  if (rc == LC_OK &&
      (!current.present || current.phase != LC_POUCH_RETENTION_IDLE))
    rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                      "state retention work slot changed", NULL, NULL, "pouch");
  if (rc == LC_OK)
    rc = lc_pouch_state_read_metadata_locked(ctx->pouch, ctx->ns,
                                             ctx->candidate.key, &state, error);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&ctx->now, error);
  if (rc == LC_OK &&
      (!state.has_body || state.version != ctx->candidate.version ||
       state.retention != ctx->candidate.deadline ||
       state.retention > ctx->now))
    ctx->skip = 1;
  if (rc == LC_OK && !ctx->skip)
    rc = lc_pouch_lease_record_parse(state.metadata, state.metadata_length,
                                     state.version, &lease, error);
  if (rc == LC_OK && !ctx->skip &&
      (lease.found != ctx->observed.found ||
       (lease.found && (lease.fencing_token != ctx->observed.fencing_token ||
                        strcmp(lease.lease_id, ctx->observed.lease_id) != 0 ||
                        strcmp(lease.txn_id, ctx->observed.txn_id) != 0))))
    ctx->skip = 1;
  if (rc == LC_OK && !ctx->skip) {
    ctx->work.txn_id =
        lc_strdup_with_allocator(&ctx->pouch->allocator, ctx->new_xid);
    ctx->work.key =
        lc_strdup_with_allocator(&ctx->pouch->allocator, ctx->candidate.key);
    if (ctx->work.txn_id == NULL || ctx->work.key == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate state cleanup reference", NULL,
                        NULL, "pouch");
    else {
      ctx->work.phase = LC_POUCH_RETENTION_ACTIVE;
      rc = lc_pouch_retention_write_work_locked(ctx, error);
      if (rc == LC_OK)
        ctx->slot_claimed = 1;
    }
  }
  lc_pouch_retention_work_cleanup(ctx->pouch, &current);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &state);
  lc_pouch_lease_record_cleanup(&lease);
  return rc;
}

static int lc_pouch_retention_journal_locked(void *context, lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_state_read_result current = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_txn_buffer buffer = {0};
  lc_source *source = NULL;
  int rc = lc_pouch_state_read_metadata_locked(
      ctx->pouch, LC_POUCH_TXN_NAMESPACE, ctx->work.txn_id, &current, error);
  if (rc == LC_OK && ctx->prepare_journal && current.found)
    rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                      "state cleanup transaction already exists", NULL, NULL,
                      "pouch");
  if (rc == LC_OK && !ctx->prepare_journal && !ctx->retire_journal &&
      !current.found)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "state cleanup transaction disappeared", NULL, NULL,
                      "pouch");
  options.object_record = 1;
  if (current.found) {
    options.has_expected_version = 1;
    options.expected_version = current.version;
  }
  if (rc == LC_OK && ctx->retire_journal && current.found)
    rc = lc_pouch_state_delete(ctx->pouch, LC_POUCH_TXN_NAMESPACE,
                               ctx->work.txn_id, &options, &written, error);
  if (rc == LC_OK && !ctx->retire_journal)
    rc = lc_pouch_txn_build_record(&ctx->journal, &buffer, error);
  if (rc == LC_OK && !ctx->retire_journal)
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  if (rc == LC_OK && !ctx->retire_journal) {
    options.content_type = "application/x-lockdc-pouch-txn";
    options.create_if_absent = ctx->prepare_journal;
    rc = lc_pouch_state_write_locked(ctx->pouch, LC_POUCH_TXN_NAMESPACE,
                                     ctx->work.txn_id, source, &options,
                                     &written, error);
  }
  lc_source_close(source);
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &written);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &current);
  return rc;
}

static int lc_pouch_retention_write_journal(lc_pouch_retention_cleanup *ctx,
                                            int prepare, int retire,
                                            lc_error *error) {
  ctx->prepare_journal = prepare;
  ctx->retire_journal = retire;
  return lc_pouch_state_with_namespace_lock(ctx->pouch, LC_POUCH_TXN_NAMESPACE,
                                            lc_pouch_retention_journal_locked,
                                            ctx, error);
}

static int
lc_pouch_retention_collect_attachment(const lc_pouch_state_visit_entry *entry,
                                      void *context, lc_error *error) {
  lc_pouch_txn_key_list *keys = context;
  if (!entry->has_payload ||
      lc_pouch_storage_key_has_staging_suffix(entry->key))
    return LC_OK;
  return lc_pouch_txn_key_list_append(keys, entry->key, error);
}

static int lc_pouch_retention_stage_attachments(lc_pouch_retention_cleanup *ctx,
                                                lc_error *error) {
  lc_pouch_txn_key_list keys = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_source *empty = NULL;
  char *prefix = lc_pouch_attachment_prefix(ctx->ns, ctx->work.key, error);
  size_t i;
  int rc;
  if (prefix == NULL)
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  rc = lc_pouch_state_visit_prefix(ctx->pouch, ctx->ns, prefix,
                                   lc_pouch_retention_collect_attachment, &keys,
                                   error);
  options.content_type = LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE;
  options.object_record = 1;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  for (i = 0; rc == LC_OK && i < keys.count; ++i) {
    char *key = lc_pouch_staged_attachment_key_from_committed(
        keys.keys[i], ctx->work.txn_id, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    rc = lc_source_from_memory("", 0, &empty, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_write(ctx->pouch, ctx->ns, key, empty, &options,
                                &written, error);
    lc_source_close(empty);
    empty = NULL;
    lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &written);
    lc_free_with_allocator(NULL, key);
  }
  lc_free_with_allocator(NULL, prefix);
  lc_pouch_txn_key_list_cleanup(&keys);
  return rc;
}

static int lc_pouch_retention_claim_and_stage_locked(void *context,
                                                     lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_state_read_result current = {0};
  lc_pouch_lease_record lease = {0};
  lc_pouch_txn_state_participant_context abandoned = {0};
  lc_pouch_txn_buffer metadata = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_source *empty = NULL;
  long fencing = 0;
  int rc = lc_pouch_state_read_metadata_locked(ctx->pouch, ctx->ns,
                                               ctx->work.key, &current, error);
  if (rc == LC_OK)
    rc = lc_pouch_lease_record_parse(current.metadata, current.metadata_length,
                                     current.version, &lease, error);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&ctx->now, error);
  if (rc == LC_OK && (!current.has_body || current.retention <= 0 ||
                      current.retention > ctx->now))
    ctx->skip = 1;
  if (rc == LC_OK && !ctx->skip && lease.found && lease.lease_id[0] != '\0') {
    if (lease.expires_at_unix > ctx->now || lease.txn_explicit)
      ctx->skip = 1;
    else if (lease.txn_id[0] == '\0')
      rc = lc_pouch_txn_clear_lease_locked(ctx->pouch, ctx->ns, ctx->work.key,
                                           &lease, error);
    else {
      abandoned.pouch = ctx->pouch;
      abandoned.ns = ctx->ns;
      abandoned.key = ctx->work.key;
      abandoned.txn_id = lease.txn_id;
      abandoned.state = "rollback";
      rc = lc_pouch_txn_apply_state_participant_locked(&abandoned, error);
      lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &current);
      if (rc == LC_OK)
        rc = lc_pouch_state_read_metadata_locked(
            ctx->pouch, ctx->ns, ctx->work.key, &current, error);
    }
  }
  if (rc == LC_OK && !ctx->skip &&
      (current.version == LC_U64_MAX || ctx->now > LC_I64_MAX - 60))
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "state cleanup claim exceeds local range", NULL, NULL,
                      "pouch");
  if (rc == LC_OK && !ctx->skip)
    rc = lc_pouch_next_fencing_token(lease.found ? lease.fencing_token : 0,
                                     &fencing, error);
  if (rc == LC_OK && !ctx->skip) {
    ctx->work.fencing_token = (lc_u64)fencing;
    ctx->work.state_version = current.version;
    rc = lc_pouch_retention_write_work_locked(ctx, error);
  }
  if (rc == LC_OK && !ctx->skip)
    rc = lc_pouch_lease_record_build(
        &metadata, ctx->ns, ctx->work.key, "liblockdc-state-retention",
        ctx->work.txn_id, ctx->work.txn_id, fencing, ctx->now + 60,
        current.version, 1, error);
  if (rc == LC_OK && !ctx->skip) {
    options.has_metadata = 1;
    options.metadata = (const unsigned char *)metadata.bytes;
    options.metadata_length = metadata.length;
    options.has_expected_version = 1;
    options.expected_version = current.version;
    rc = lc_pouch_state_update_metadata_locked(
        ctx->pouch, ctx->ns, ctx->work.key, &options, &written, error);
    lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &written);
  }
  if (rc == LC_OK && !ctx->skip)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_CLAIMED, error);
  if (rc == LC_OK && !ctx->skip)
    rc = lc_pouch_retention_stage_attachments(ctx, error);
  if (rc == LC_OK && !ctx->skip) {
    memset(&options, 0, sizeof(options));
    options.staged_delete_marker = 1;
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.has_logical_version = 1;
    options.logical_version = current.version + 1;
    rc = lc_source_from_memory("", 0, &empty, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_stage_write(ctx->pouch, ctx->ns, ctx->work.key,
                                      ctx->work.txn_id, empty, &options,
                                      &written, error);
  }
  lc_source_close(empty);
  lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &written);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &current);
  lc_pouch_lease_record_cleanup(&lease);
  lc_pouch_txn_buffer_cleanup(&metadata);
  return rc;
}

static int lc_pouch_retention_no_owned_work_locked(void *context,
                                                   lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_state_read_result parent = {0};
  lc_pouch_state_read_result staged = {0};
  lc_pouch_lease_record lease = {0};
  lc_pouch_txn_key_list attachments = {0};
  char *key =
      lc_pouch_staged_storage_key(ctx->work.key, ctx->work.txn_id, error);
  int owned = 0;
  int rc;
  if (key == NULL)
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  rc = lc_pouch_state_read_metadata_locked(ctx->pouch, ctx->ns, ctx->work.key,
                                           &parent, error);
  if (rc == LC_OK)
    rc = lc_pouch_lease_record_parse(parent.metadata, parent.metadata_length,
                                     parent.version, &lease, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_read_metadata_locked(ctx->pouch, ctx->ns, key, &staged,
                                             error);
  if (rc == LC_OK)
    rc = lc_pouch_collect_staged_attachment_bases(
        ctx->pouch, ctx->ns, ctx->work.key, ctx->work.txn_id, &attachments,
        error);
  if (rc == LC_OK) {
    owned = (ctx->journal.state == NULL && ctx->work.fencing_token != 0) ||
            staged.found || attachments.count != 0 ||
            (lease.found && strcmp(lease.txn_id, ctx->work.txn_id) == 0);
    if (owned)
      rc = lc_error_set(
          error, LC_ERR_PROTOCOL, 0L,
          "state cleanup has owned work but no transaction journal",
          ctx->work.txn_id, NULL, "pouch");
  }
  lc_pouch_txn_key_list_cleanup(&attachments);
  lc_free_with_allocator(NULL, key);
  lc_pouch_lease_record_cleanup(&lease);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &parent);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &staged);
  return rc;
}

static int lc_pouch_retention_apply_locked(void *context, lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  lc_pouch_txn_state_participant_context apply = {0};
  lc_pouch_state_read_result current = {0};
  lc_pouch_lease_record lease = {0};
  lc_pouch_txn_buffer metadata = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  int rc = lc_pouch_state_read_metadata_locked(ctx->pouch, ctx->ns,
                                               ctx->work.key, &current, error);
  if (rc == LC_OK)
    rc = lc_pouch_lease_record_parse(current.metadata, current.metadata_length,
                                     current.version, &lease, error);
  if (rc == LC_OK && lease.found && lease.lease_id[0] != '\0' &&
      strcmp(lease.txn_id, ctx->work.txn_id) != 0) {
    if (strcmp(ctx->journal.state, "rollback") == 0)
      rc = lc_pouch_retention_no_owned_work_locked(ctx, error);
    else
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "state cleanup reservation was replaced",
                        ctx->work.txn_id, NULL, "pouch");
    goto done;
  }
  if (rc == LC_OK && strcmp(ctx->journal.state, "commit") == 0 &&
      (!lease.found || lease.lease_id[0] == '\0')) {
    if (ctx->work.fencing_token == 0 ||
        ctx->work.fencing_token > (lc_u64)LONG_MAX)
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "state cleanup is missing its claim high-water", NULL,
                        NULL, "pouch");
    else {
      /* The parent tombstone may precede attachment replay at a crash. The
       * ACTIVE work reference fences recreation while this lease is restored.
       */
      rc = lc_pouch_lease_record_build(
          &metadata, ctx->ns, ctx->work.key, "liblockdc-state-retention",
          ctx->work.txn_id, ctx->work.txn_id, (long)ctx->work.fencing_token, 0,
          ctx->work.state_version, 1, error);
      if (rc == LC_OK) {
        options.has_metadata = 1;
        options.metadata = (const unsigned char *)metadata.bytes;
        options.metadata_length = metadata.length;
        rc = lc_pouch_state_update_metadata_locked(
            ctx->pouch, ctx->ns, ctx->work.key, &options, &written, error);
      }
    }
  }
  if (rc == LC_OK) {
    apply.pouch = ctx->pouch;
    apply.ns = ctx->ns;
    apply.key = ctx->work.key;
    apply.txn_id = ctx->work.txn_id;
    apply.state = ctx->journal.state;
    apply.commit_at_unix = ctx->journal.commit_at_unix;
    apply.retention_cleanup = 1;
    rc = lc_pouch_txn_apply_state_participant_locked(&apply, error);
  }
done:
  lc_pouch_state_write_result_cleanup(&ctx->pouch->allocator, &written);
  lc_pouch_state_read_result_cleanup(&ctx->pouch->allocator, &current);
  lc_pouch_lease_record_cleanup(&lease);
  lc_pouch_txn_buffer_cleanup(&metadata);
  return rc;
}

static int lc_pouch_retention_include_data_locked(void *context,
                                                  lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  return lc_pouch_state_durable_scope_include_namespace_locked(ctx->pouch,
                                                               ctx->ns, error);
}

static int lc_pouch_retention_include_journal_locked(void *context,
                                                     lc_error *error) {
  lc_pouch_retention_cleanup *ctx = context;
  return lc_pouch_state_durable_scope_include_namespace_locked(
      ctx->pouch, LC_POUCH_TXN_NAMESPACE, error);
}

static int lc_pouch_retention_finish(lc_pouch_retention_cleanup *ctx,
                                     lc_pouch_state_commit_group *scope,
                                     lc_error *error) {
  int rc = LC_OK;
  if (ctx->work.phase != LC_POUCH_RETENTION_FINISHED) {
    if (!ctx->slot_claimed && ctx->work.fencing_token != 0)
      rc = lc_pouch_state_with_namespace_lock(
          ctx->pouch, ctx->ns, lc_pouch_retention_include_data_locked, ctx,
          error);
    if (rc == LC_OK)
      rc = lc_pouch_state_durable_scope_barrier(scope, error);
    if (rc == LC_OK)
      rc =
          lc_pouch_retention_note_phase(LC_POUCH_RETENTION_DATA_DURABLE, error);
    if (rc == LC_OK) {
      lc_pouch_indexer_note_operation_complete(ctx->pouch, ctx->ns,
                                               ctx->work.key, ctx->work.txn_id);
      ctx->work.phase = LC_POUCH_RETENTION_FINISHED;
      rc = lc_pouch_state_with_namespace_lock(
          ctx->pouch, ctx->ns, lc_pouch_retention_write_work_locked, ctx,
          error);
    }
    if (rc == LC_OK)
      rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_WORK_FINISHED,
                                         error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_retention_write_journal(ctx, 0, 1, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_durable_scope_barrier(scope, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_JOURNAL_RETIRED,
                                       error);
  if (rc == LC_OK) {
    ctx->work.phase = LC_POUCH_RETENTION_IDLE;
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, ctx->ns, lc_pouch_retention_write_work_locked, ctx, error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_REF_CLEARED, error);
  if (rc == LC_OK) {
    lc_pouch_compaction_note_mutation(ctx->pouch, ctx->ns, 1);
    lc_pouch_compaction_note_mutation(ctx->pouch, LC_POUCH_TXN_NAMESPACE, 1);
  }
  return rc;
}

static int lc_pouch_retention_validate_journal(lc_pouch_retention_cleanup *ctx,
                                               lc_error *error) {
  lc_pouch_txn_record *record = &ctx->journal;
  if (record->state == NULL)
    return LC_OK;
  if (record->participant_count != 1 || record->tc_term != 0 ||
      strcmp(record->participants[0].ns, ctx->ns) != 0 ||
      strcmp(record->participants[0].key, ctx->work.key) != 0 ||
      (strcmp(record->state, "prepare") != 0 &&
       strcmp(record->state, "commit") != 0 &&
       strcmp(record->state, "rollback") != 0) ||
      (strcmp(record->state, "commit") == 0 &&
       (record->commit_at_unix <= 0 || record->votes[0] != 1)))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid state retention cleanup journal",
                        ctx->work.txn_id, NULL, "pouch");
  return lc_pouch_txn_validate_target_backend(
      ctx->pouch, record->target_backend_hash, error);
}

static int lc_pouch_retention_resume(lc_pouch_retention_cleanup *ctx,
                                     lc_pouch_state_commit_group *scope,
                                     lc_error *error) {
  int rc;
  if (ctx->work.phase == LC_POUCH_RETENTION_FINISHED)
    return lc_pouch_retention_finish(ctx, scope, error);
  lc_pouch_txn_record_cleanup(&ctx->journal);
  rc = lc_pouch_xa_read_txn(ctx->pouch, ctx->work.txn_id, &ctx->journal, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_validate_journal(ctx, error);
  if (rc == LC_OK && !ctx->slot_claimed && ctx->journal.state != NULL &&
      strcmp(ctx->journal.state, "commit") == 0) {
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, LC_POUCH_TXN_NAMESPACE,
        lc_pouch_retention_include_journal_locked, ctx, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_durable_scope_barrier(scope, error);
  }
  if (rc == LC_OK && ctx->journal.state == NULL)
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, ctx->ns, lc_pouch_retention_no_owned_work_locked, ctx,
        error);
  if (rc == LC_OK && ctx->journal.state != NULL &&
      strcmp(ctx->journal.state, "prepare") == 0) {
    rc = lc_pouch_txn_record_replace_string(&ctx->journal.state, "rollback",
                                            error);
    ctx->journal.votes[0] = 2;
    if (rc == LC_OK)
      rc = lc_pouch_retention_write_journal(ctx, 0, 0, error);
    if (rc == LC_OK)
      rc = lc_pouch_state_durable_scope_barrier(scope, error);
  }
  if (rc == LC_OK && ctx->journal.state != NULL)
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, ctx->ns, lc_pouch_retention_apply_locked, ctx, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_finish(ctx, scope, error);
  return rc;
}

static int lc_pouch_retention_start(lc_pouch_retention_cleanup *ctx,
                                    lc_pouch_state_commit_group *scope,
                                    lc_error *error) {
  lc_txn_participant participant = {0};
  lc_txn_decision_req prepare;
  lc_pouch_unix_seconds decided_at;
  int write_record;
  int apply_decision;
  int commit;
  int rc = lc_pouch_state_with_namespace_lock(
      ctx->pouch, ctx->ns, lc_pouch_retention_claim_slot_locked, ctx, error);
  if (rc != LC_OK || ctx->skip)
    return rc;
  rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_REF_CREATED, error);
  lc_txn_decision_req_init(&prepare);
  participant.ns = ctx->ns;
  participant.key = ctx->work.key;
  prepare.txn_id = ctx->work.txn_id;
  prepare.participants = &participant;
  prepare.participant_count = 1;
  if (rc == LC_OK)
    rc = lc_pouch_txn_record_merge_request(&ctx->journal, &prepare, "prepare",
                                           0, &write_record, &apply_decision,
                                           error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_write_journal(ctx, 1, 0, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_durable_scope_barrier(scope, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_PREPARED, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, ctx->ns, lc_pouch_retention_claim_and_stage_locked, ctx,
        error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_STAGED, error);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&decided_at, error);
  if (rc == LC_OK) {
    commit = !ctx->skip && decided_at >= ctx->candidate.deadline;
    rc = lc_pouch_txn_record_replace_string(
        &ctx->journal.state, commit ? "commit" : "rollback", error);
    ctx->journal.commit_at_unix = commit ? decided_at : 0;
    ctx->journal.votes[0] = commit ? 1 : 2;
  }
  if (rc == LC_OK)
    rc = lc_pouch_retention_write_journal(ctx, 0, 0, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_durable_scope_barrier(scope, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_note_phase(LC_POUCH_RETENTION_DECIDED, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_with_namespace_lock(
        ctx->pouch, ctx->ns, lc_pouch_retention_apply_locked, ctx, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_finish(ctx, scope, error);
  return rc;
}

static int lc_pouch_retention_attempt(lc_pouch_retention_cleanup *ctx,
                                      int fresh, lc_error *error) {
  lc_pouch_state_commit_group *scope = NULL;
  int rc = lc_pouch_state_durable_scope_begin(ctx->pouch, &scope, error);
  if (rc != LC_OK)
    return rc;
  rc = fresh ? lc_pouch_retention_start(ctx, scope, error)
             : lc_pouch_retention_resume(ctx, scope, error);
  if (rc != LC_OK && ctx->work.phase == LC_POUCH_RETENTION_ACTIVE &&
      (ctx->slot_claimed || !fresh) &&
      (ctx->journal.state == NULL ||
       strcmp(ctx->journal.state, "commit") != 0)) {
    lc_error rollback_error;
    int rollback_rc;
    lc_error_init(&rollback_error);
    rollback_rc = lc_pouch_retention_resume(ctx, scope, &rollback_error);
    if (rollback_rc != LC_OK) {
      pslog_field fields[3];
      fields[0] = lc_log_str_field("ns", ctx->ns);
      fields[1] = lc_log_str_field("key", ctx->work.key);
      fields[2] = lc_log_error_field("error", &rollback_error);
      lc_log_warn(ctx->pouch->logger, "retention.rollback.error", fields, 3);
    }
    lc_error_cleanup(&rollback_error);
  }
  return lc_pouch_state_durable_scope_end(scope, rc, error);
}

int lc_pouch_retention_run(lc_pouch *pouch, const char *ns, size_t max_attempts,
                           lc_pouch_maintenance_result *out, lc_error *error) {
  size_t attempts = 0;
  char *first_key = NULL;
  int wrapped = 0;
  int first_rc = LC_OK;
  lc_error first_error;
  if (pouch == NULL || ns == NULL || ns[0] == '\0')
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "state retention maintenance requires root and namespace", NULL, NULL,
        "pouch");
  if (out != NULL) {
    out->retention_scanned_count = 0;
    out->retention_expired_count = 0;
    out->retention_deleted_state_count = 0;
    out->retention_failed_count = 0;
  }
  lc_error_init(&first_error);
  while (attempts < max_attempts) {
    lc_pouch_retention_cleanup ctx = {0};
    lc_pouch_txn_guard guard = {0};
    lc_pouch_txn_guard foreign_guard = {0};
    lc_pouch_txn_record foreign = {0};
    lc_error cause;
    int fresh;
    int stop = 0;
    int rc;
    ctx.pouch = pouch;
    ctx.ns = ns;
    guard.fd = -1;
    foreign_guard.fd = -1;
    lc_error_init(&cause);
    rc = lc_pouch_state_with_namespace_lock(
        pouch, ns, lc_pouch_retention_load_locked, &ctx, &cause);
    fresh = ctx.work.phase == LC_POUCH_RETENTION_IDLE;
    if (out != NULL)
      out->retention_scanned_count += ctx.candidate.scanned;
    if (rc == LC_OK &&
        (!ctx.work.present || (fresh && ctx.candidate.key == NULL))) {
      stop = 1;
      goto attempt_done;
    }
    ++attempts;
    if (rc == LC_OK && fresh) {
      if (first_key != NULL) {
        if ((wrapped && ctx.candidate.wrapped) ||
            ((wrapped || ctx.candidate.wrapped) &&
             strcmp(ctx.candidate.key, first_key) >= 0)) {
          stop = 1;
          goto attempt_done;
        }
        wrapped = wrapped || ctx.candidate.wrapped;
      } else {
        first_key =
            lc_strdup_with_allocator(&pouch->allocator, ctx.candidate.key);
        if (first_key == NULL)
          rc = lc_error_set(&cause, LC_ERR_NOMEM, 0L,
                            "failed to retain state retention pass cursor",
                            NULL, NULL, "pouch");
      }
      if (out != NULL)
        ++out->retention_expired_count;
    }
    if (rc == LC_OK && ctx.skip)
      goto attempt_done;
    if (rc == LC_OK && fresh && ctx.observed.found &&
        ctx.observed.lease_id[0] != '\0' && ctx.observed.txn_id[0] != '\0') {
      rc = lc_pouch_txn_guard_lock(pouch, ctx.observed.txn_id, 0,
                                   &foreign_guard, &cause);
      if (rc == LC_OK)
        rc = lc_pouch_xa_read_txn(pouch, ctx.observed.txn_id, &foreign, &cause);
      if (rc == LC_OK && foreign.state != NULL)
        goto attempt_done;
    }
    if (rc == LC_OK && fresh)
      rc = lc_xid_new(ctx.new_xid, &cause);
    if (rc == LC_OK) {
      const char *txn_id = fresh ? ctx.new_xid : ctx.work.txn_id;
      /* An abandoned implicit xid can share this root's guard stripe with
       * the new cleanup xid. Its already-owned mutex and file lock protect
       * both; trying to reacquire that non-recursive stripe would skip itself.
       */
      if (!foreign_guard.mutex_locked ||
          foreign_guard.mutex !=
              &lc_pouch_txn_guard_mutexes[lc_pouch_txn_guard_stripe(txn_id)])
        rc = lc_pouch_txn_guard_lock(pouch, txn_id, 0, &guard, &cause);
    }
    if (rc == LC_OK)
      rc = lc_pouch_retention_attempt(&ctx, fresh, &cause);
    if (rc == LC_OK && ctx.journal.state != NULL &&
        strcmp(ctx.journal.state, "commit") == 0 && out != NULL)
      ++out->retention_deleted_state_count;
  attempt_done:
    lc_pouch_txn_guard_release(&guard);
    lc_pouch_txn_guard_release(&foreign_guard);
    lc_pouch_txn_record_cleanup(&foreign);
    if (rc == LC_ERR_PENDING) {
      stop = !fresh || ctx.work.phase != LC_POUCH_RETENTION_IDLE;
    } else if (rc != LC_OK) {
      pslog_field fields[4];
      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field(
          "key", ctx.work.key != NULL ? ctx.work.key : ctx.candidate.key);
      fields[2] = lc_log_error_field("error", &cause);
      fields[3] = lc_log_code_field(&cause);
      lc_log_warn(pouch->logger, "retention.cleanup.error", fields, 4);
      if (out != NULL)
        ++out->retention_failed_count;
      if (first_rc == LC_OK) {
        first_rc = rc;
        first_error = cause;
        lc_error_init(&cause);
      }
      if (ctx.candidate.key == NULL ||
          ctx.work.phase != LC_POUCH_RETENTION_IDLE)
        stop = 1;
    }
    lc_error_cleanup(&cause);
    lc_pouch_retention_cleanup_dispose(&ctx);
    if (stop)
      break;
  }
  lc_free_with_allocator(&pouch->allocator, first_key);
  if (first_rc != LC_OK && error != NULL) {
    lc_error_cleanup(error);
    *error = first_error;
    lc_error_init(&first_error);
  }
  lc_error_cleanup(&first_error);
  return first_rc;
}

int lc_pouch_client_xa_prepared(lc_client *self, const char *txn_id,
                                const char *key, unsigned char phase,
                                int *prepared, lc_error *error) {
  lc_pouch_txn_record record;
  char backend[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  size_t index;
  int rc = lc_pouch_xa_read_txn(((lc_client_handle *)self)->pouch, txn_id,
                                &record, error);
  *prepared = 0;
  index = rc == LC_OK ? lc_pouch_txn_record_find_participant(
                            &record, LC_POUCH_XA_NAMESPACE, key)
                      : (size_t)-1;
  if (rc == LC_OK && (record.state == NULL || index == (size_t)-1))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA branch has no authoritative participant history",
                      NULL, NULL, NULL);
  if (rc == LC_OK)
    *prepared = record.votes[index] == 1U;
  if (rc == LC_OK)
    rc = lc_pouch_backend_hash(((lc_client_handle *)self)->pouch, backend,
                               error);
  if (rc == LC_OK &&
      (strcmp(record.participants[index].backend_hash, backend) != 0 ||
       (phase >= LC_XA_COMMIT_SENT &&
        strcmp(record.state,
               phase == LC_XA_COMMIT_SENT || phase == LC_XA_COMMITTED
                   ? "commit"
                   : "rollback") != 0) ||
       ((phase == LC_XA_COMMIT_SENT || phase == LC_XA_COMMITTED) &&
        !*prepared)))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA branch history contradicts its coordinator/backend",
                      NULL, NULL, NULL);
  lc_pouch_txn_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_xa_enroll(lc_client *self,
                              const lc_xa_branch_record *branch,
                              const char *owner_id, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_txn_record record;
  lc_txn_participant participant;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  char key[LC_POUCH_XA_KEY_SIZE];
  char backend[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  size_t i;
  int rc;
  if (client == NULL || !client->is_pouch || branch == NULL ||
      branch->phase != LC_XA_ENROLLED)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA enrollment requires Pouch and a new branch", NULL,
                        NULL, "pouch-xa");
  rc = lc_pouch_xa_branch_key(branch, key, sizeof(key), error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_txn_guard_acquire(client->pouch, branch->txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  memset(&result, 0, sizeof(result));
  lc_txn_decision_req_init(&request);
  rc = lc_pouch_xa_read_txn(client->pouch, branch->txn_id, &record, error);
  if (rc == LC_OK) {
    int sealed = 0;
    rc = lc_pouch_xa_owner_check(client, branch->txn_id, record.tc_term,
                                 owner_id, 1, &sealed, error);
    if (rc == LC_OK && sealed)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA transaction enrollment is sealed", NULL, NULL,
                        "pouch-xa");
  }
  if (rc == LC_OK && record.state != NULL &&
      strcmp(record.state, "prepare") != 0)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "XA transaction is already decided", NULL, NULL,
                      "pouch-xa");
  for (i = 0U; rc == LC_OK && i < record.participant_count; ++i)
    if (record.votes[i] != 0U)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA transaction enrollment is sealed", NULL, NULL,
                        "pouch-xa");
  if (rc == LC_OK)
    rc = lc_pouch_backend_hash(client->pouch, backend, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_branch_write(client->pouch, branch, error);
  if (rc == LC_OK) {
    participant.ns = LC_POUCH_XA_NAMESPACE;
    participant.key = key;
    participant.backend_hash = backend;
    request.txn_id = branch->txn_id;
    request.participants = &participant;
    request.participant_count = 1U;
    request.tc_term = record.tc_term;
    request.expires_at_unix = record.expires_at_unix;
    rc = lc_pouch_client_txn_decision(self, &request, "prepare", 0, owner_id, 0,
                                      &result, error);
  }
  lc_txn_decision_res_cleanup(&result);
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}

int lc_pouch_client_xa_progress(lc_client *self,
                                const lc_xa_branch_record *branch,
                                const char *owner_id, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_txn_record record;
  char key[LC_POUCH_XA_KEY_SIZE];
  char backend[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  size_t index;
  int ready;
  int rc;
  if (client == NULL || !client->is_pouch || branch == NULL ||
      branch->phase == LC_XA_ENROLLED)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA progress requires Pouch and an enrolled branch",
                        NULL, NULL, "pouch-xa");
  rc = lc_pouch_xa_branch_key(branch, key, sizeof(key), error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_txn_guard_acquire(client->pouch, branch->txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_xa_read_txn(client->pouch, branch->txn_id, &record, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_owner_check(client, branch->txn_id, record.tc_term,
                                 owner_id, 1, NULL, error);
  index = rc == LC_OK ? lc_pouch_txn_record_find_participant(
                            &record, LC_POUCH_XA_NAMESPACE, key)
                      : (size_t)-1;
  if (rc == LC_OK && (record.state == NULL || index == (size_t)-1))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA branch is not in the durable participant set", NULL,
                      NULL, "pouch-xa");
  if (rc == LC_OK)
    rc = lc_pouch_backend_hash(client->pouch, backend, error);
  if (rc == LC_OK &&
      strcmp(record.participants[index].backend_hash, backend) != 0)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA branch targets another coordinator store", NULL, NULL,
                      "pouch-xa");
  if (rc == LC_OK) {
    const char *required = branch->phase <= LC_XA_PREPARED ? "prepare"
                           : (branch->phase == LC_XA_COMMIT_SENT ||
                              branch->phase == LC_XA_COMMITTED)
                               ? "commit"
                               : "rollback";
    if (strcmp(record.state, required) != 0)
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "XA branch operation contradicts the durable decision",
                        NULL, NULL, "pouch-xa");
  }
  /* Seal under the same .txns mutation lock used by native enrollment before
   * persisting/sending prepare. A crash leaves an undecided preparing marker,
   * never a fabricated YES vote; recovery retains the descriptor for cleanup.
   */
  if (rc == LC_OK && branch->phase == LC_XA_PREPARE_SENT) {
    rc = lc_pouch_txn_record_vote(client, branch->txn_id, LC_POUCH_XA_NAMESPACE,
                                  key, 0, 1, &ready, NULL, error);
    if (rc == LC_OK && ready)
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA transaction expired before prepare", NULL, NULL,
                        "pouch-xa");
  }
  if (rc == LC_OK)
    rc = lc_pouch_xa_branch_write(client->pouch, branch, error);
  if (rc == LC_OK && branch->phase == LC_XA_PREPARED)
    rc = lc_pouch_txn_record_vote(client, branch->txn_id, LC_POUCH_XA_NAMESPACE,
                                  key, 0, 0, &ready, NULL, error);
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}

static int lc_pouch_client_txn_replay_impl(lc_client *self,
                                           const lc_txn_replay_req *req,
                                           lc_txn_replay_res *out,
                                           lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  lc_pouch_txn_record txn_record;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *record;
  char *key;
  const char *response_state;
  lc_pouch_generation response_index;
  lc_pouch_unix_seconds now;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_replay requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (!lc_xid_is_valid(req->txn_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_id must be a valid xid", NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_txn_replay_hook != NULL) {
    rc = lc_pouch_test_before_txn_replay_hook(
        lc_pouch_test_before_txn_replay_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
#endif
  memset(&read_result, 0, sizeof(read_result));
  memset(&txn_record, 0, sizeof(txn_record));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  sink = NULL;
  record = NULL;
  response_state = NULL;
  response_index = 0UL;
  now = 0L;
  key = lc_pouch_txn_key(req->txn_id, error);
  if (key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, LC_POUCH_TXN_NAMESPACE, key,
                           &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch transaction replay record not found", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    record = (char *)lc_alloc_with_allocator(NULL, length + 1U);
    if (record == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction replay record",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(record, bytes, length);
    record[length] = '\0';
    rc = lc_pouch_txn_parse_record(record, length, &txn_record, error);
  }
  if (rc == LC_OK) {
    decision_req.txn_id = req->txn_id;
    decision_req.participants = txn_record.participants;
    decision_req.participant_count = txn_record.participant_count;
    decision_req.expires_at_unix = txn_record.expires_at_unix;
    decision_req.tc_term = txn_record.tc_term;
    decision_req.target_backend_hash = txn_record.target_backend_hash;
    response_state = txn_record.state;
    response_index = read_result.index_seq;
    rc = lc_pouch_txn_validate_participants(&decision_req, error);
    if (rc == LC_OK) {
      rc = lc_pouch_txn_validate_target_backend(
          client->pouch, decision_req.target_backend_hash, error);
    }
    if (rc == LC_OK && strcmp(txn_record.state, "prepare") == 0) {
      rc = lc_pouch_now_unix(&now, error);
      if (rc == LC_OK && txn_record.expires_at_unix > 0L &&
          txn_record.expires_at_unix <= now) {
        int managed = 0;
        rc = lc_pouch_xa_managed(client, req->txn_id, &managed, error);
        if (rc == LC_OK && managed)
          rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                            "managed XA requires fenced owner recovery", NULL,
                            NULL, "pouch-xa");
        if (rc == LC_OK)
          rc = lc_pouch_client_txn_decision(self, &decision_req, "rollback", 1,
                                            NULL, 0, &decision_res, error);
        if (!managed && (rc == LC_OK || rc == LC_ERR_PENDING)) {
          int operation_rc = rc;
          lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
          memset(&read_result, 0, sizeof(read_result));
          rc = lc_pouch_state_read_metadata(
              client->pouch, LC_POUCH_TXN_NAMESPACE, key, &read_result, error);
          if (rc == LC_OK && read_result.found) {
            response_state = "rollback";
            response_index = read_result.index_seq;
            rc = operation_rc;
          } else if (rc == LC_OK) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch transaction replay record disappeared",
                              NULL, NULL, NULL);
          }
        }
      }
    } else if (rc == LC_OK && (strcmp(txn_record.state, "commit") == 0 ||
                               strcmp(txn_record.state, "rollback") == 0)) {
      rc = lc_pouch_txn_apply_participants(client, &decision_req,
                                           txn_record.state, 1,
                                           txn_record.commit_at_unix, error);
    } else if (rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch transaction replay state is unsupported", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK || rc == LC_ERR_PENDING) {
    int operation_rc = rc;
    rc = lc_pouch_txn_replay_response(out, req->txn_id, response_state,
                                      response_index, error);
    if (rc == LC_OK)
      rc = operation_rc;
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_free_with_allocator(NULL, record);
  lc_free_with_allocator(NULL, key);
  lc_txn_decision_res_cleanup(&decision_res);
  lc_pouch_txn_record_cleanup(&txn_record);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_client_txn_decision(
    lc_client *self, const lc_txn_decision_req *req, const char *state,
    int tolerate_queue_lease_mismatch, const char *owner_id, int takeover,
    lc_txn_decision_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_decision_context context;
  lc_txn_decision_req decision_req;
  lc_txn_decision_req replay_req;
  char *key;
  int rc;

  if (self == NULL || req == NULL || state == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction decision requires self, req, "
                        "state, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  memset(&context, 0, sizeof(context));
  lc_txn_decision_req_init(&decision_req);
  if (!lc_xid_is_valid(req->txn_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_id must be a valid xid", NULL, NULL, NULL);
  }
  rc = lc_pouch_txn_validate_participants(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_validate_target_backend(client->pouch,
                                            req->target_backend_hash, error);
  if (rc != LC_OK) {
    return rc;
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_txn_decision_hook != NULL) {
    rc = lc_pouch_test_before_txn_decision_hook(
        lc_pouch_test_before_txn_decision_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
#endif
  key = lc_pouch_txn_key(req->txn_id, error);
  if (key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  context.client = client;
  context.request = req;
  context.state = state;
  context.key = key;
  context.owner_id = owner_id;
  context.takeover = takeover;
  rc = lc_pouch_xa_read_txn(client->pouch, req->txn_id, &context.record, error);
  if (rc == LC_OK && strcmp(state, "commit") == 0 &&
      (context.record.state == NULL ||
       strcmp(context.record.state, "prepare") == 0)) {
    rc = lc_pouch_now_unix(&context.commit_at_unix, error);
    if (rc == LC_OK)
      rc = lc_pouch_txn_validate_retention(
          client, req->txn_id, context.record.participants,
          context.record.participant_count, context.commit_at_unix, error);
    if (rc == LC_OK)
      rc = lc_pouch_txn_validate_retention(
          client, req->txn_id, req->participants, req->participant_count,
          context.commit_at_unix, error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_xa_decision_authority(&context, &context.owner_sealed, error);
  if (rc == LC_OK && context.owner_sealed && owner_id == NULL && !takeover &&
      context.record.state != NULL &&
      strcmp(context.record.state, "prepare") != 0) {
    /* An ownerless terminal retry observes/applies the immutable decision. It
     * cannot move its epoch independently of the durable managed owner. */
    replay_req = *req;
    replay_req.tc_term = context.record.tc_term;
    context.request = &replay_req;
  }
  if (rc == LC_OK) {
    lc_pouch_txn_record_cleanup(&context.record);
    rc = lc_pouch_state_with_key_lock(client->pouch, LC_POUCH_TXN_NAMESPACE,
                                      key, lc_pouch_txn_decision_record_locked,
                                      &context, error);
  }
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && context.apply_decision &&
      lc_pouch_test_after_txn_decision_hook != NULL)
    rc = lc_pouch_test_after_txn_decision_hook(
        lc_pouch_test_after_txn_decision_context, error);
#endif
  if (rc == LC_OK && context.apply_decision) {
    decision_req.txn_id = req->txn_id;
    decision_req.participants = context.record.participants;
    decision_req.participant_count = context.record.participant_count;
    decision_req.expires_at_unix = context.record.expires_at_unix;
    decision_req.tc_term = context.record.tc_term;
    decision_req.target_backend_hash = context.record.target_backend_hash;
    rc = lc_pouch_txn_apply_participants(
        client, &decision_req, context.record.state,
        tolerate_queue_lease_mismatch, context.record.commit_at_unix, error);
  }
  if ((rc == LC_OK || rc == LC_ERR_PENDING) && context.record.state != NULL) {
    int operation_rc = rc;
    rc = lc_pouch_txn_decision_response(out, req->txn_id, context.record.state,
                                        context.response_index, error);
    if (rc == LC_OK)
      rc = operation_rc;
  }
  lc_pouch_state_write_result_cleanup(&client->allocator,
                                      &context.write_result);
  lc_pouch_txn_record_cleanup(&context.record);
  lc_free_with_allocator(NULL, key);
  return rc;
}

static int lc_pouch_client_txn_guarded_decision(lc_client *self,
                                                const lc_txn_decision_req *req,
                                                const char *state,
                                                lc_txn_decision_res *out,
                                                lc_error *error) {
  lc_pouch_txn_guard guard;
  int rc;
  if (self == NULL || req == NULL || !lc_xid_is_valid(req->txn_id))
    return lc_pouch_client_txn_decision(self, req, state, 0, NULL, 0, out,
                                        error);
  rc = lc_pouch_txn_guard_acquire(((lc_client_handle *)self)->pouch,
                                  req->txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_client_txn_decision(self, req, state, 0, NULL, 0, out, error);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}
int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out, lc_error *error) {
  lc_pouch_txn_guard guard;
  int rc;
  if (self == NULL || req == NULL || !lc_xid_is_valid(req->txn_id))
    return lc_pouch_client_txn_replay_impl(self, req, out, error);
  rc = lc_pouch_txn_guard_acquire(((lc_client_handle *)self)->pouch,
                                  req->txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_client_txn_replay_impl(self, req, out, error);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}
int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error) {
  return lc_pouch_client_txn_guarded_decision(self, req, "prepare", out, error);
}

int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error) {
  return lc_pouch_client_txn_guarded_decision(self, req, "commit", out, error);
}

int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  return lc_pouch_client_txn_guarded_decision(self, req, "rollback", out,
                                              error);
}

int lc_pouch_client_recover_transactions(lc_client *self, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_key_list keys;
  lc_pouch_unix_seconds now;
  size_t i;
  int rc;

  now = 0L;
  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction recovery requires self", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&keys, 0, sizeof(keys));
  rc = lc_pouch_state_visit(client->pouch, LC_POUCH_TXN_NAMESPACE,
                            lc_pouch_txn_collect_key, &keys, error);
  if (rc != LC_OK) {
    lc_pouch_txn_key_list_cleanup(&keys);
    return rc;
  }
  rc = lc_pouch_now_unix(&now, error);
  for (i = 0U; rc == LC_OK && i < keys.count; ++i) {
    lc_pouch_state_read_result read_result;
    lc_pouch_txn_record record;
    lc_txn_decision_req req;
    lc_sink *sink;
    const void *bytes;
    size_t length;
    char *body;
    int cleanup_decision;
    int managed;
    int retention_owned;
    lc_pouch_txn_guard guard;

    memset(&read_result, 0, sizeof(read_result));
    memset(&record, 0, sizeof(record));
    lc_txn_decision_req_init(&req);
    sink = NULL;
    body = NULL;
    cleanup_decision = 0;
    managed = 0;
    retention_owned = 0;
    rc = lc_pouch_txn_guard_acquire(client->pouch, keys.keys[i], &guard, error);
    if (rc != LC_OK)
      break;
    rc = lc_pouch_xa_managed(client, keys.keys[i], &managed, error);
    if (rc == LC_OK && !managed) {
      lc_pouch_unix_seconds completed = 0;
      rc = lc_pouch_xa_completed_read(client, keys.keys[i], &completed, error);
      managed = completed != 0;
    }
    if (rc == LC_OK)
      rc = lc_pouch_state_read(client->pouch, LC_POUCH_TXN_NAMESPACE,
                               keys.keys[i], &read_result, error);
    if (rc == LC_OK && read_result.found) {
      rc = lc_sink_to_memory(&sink, error);
    }
    if (rc == LC_OK && read_result.found) {
      rc = lc_copy(read_result.body, sink, NULL, error);
    }
    if (rc == LC_OK && read_result.found) {
      rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    }
    if (rc == LC_OK && read_result.found) {
      body = (char *)lc_alloc_with_allocator(NULL, length + 1U);
      if (body == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction recovery "
                          "record",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK && read_result.found) {
      memcpy(body, bytes, length);
      body[length] = '\0';
      rc = lc_pouch_txn_parse_record(body, length, &record, error);
    }
    if (rc == LC_OK && read_result.found) {
      req.txn_id = keys.keys[i];
      req.participants = record.participants;
      req.participant_count = record.participant_count;
      req.expires_at_unix = record.expires_at_unix;
      req.tc_term = record.tc_term;
      req.target_backend_hash = record.target_backend_hash;
      rc = lc_pouch_txn_validate_participants(&req, error);
      if (rc == LC_OK) {
        rc = lc_pouch_txn_validate_target_backend(
            client->pouch, req.target_backend_hash, error);
      }
      if (rc == LC_OK && record.participant_count == 1)
        rc = lc_pouch_retention_owns_journal(
            client->pouch, record.participants[0].ns,
            record.participants[0].key, keys.keys[i], &retention_owned, error);
      if (rc == LC_OK && !retention_owned &&
          (strcmp(record.state, "commit") == 0 ||
           strcmp(record.state, "rollback") == 0)) {
        rc = lc_pouch_txn_apply_participants(client, &req, record.state, 1,
                                             record.commit_at_unix, error);
        if (rc == LC_OK) {
          cleanup_decision = 1;
        }
      } else if (rc == LC_OK && !retention_owned &&
                 strcmp(record.state, "prepare") == 0 &&
                 record.expires_at_unix > 0L && record.expires_at_unix <= now) {
        lc_txn_decision_res rollback_res;

        memset(&rollback_res, 0, sizeof(rollback_res));
        if (managed)
          rc = LC_ERR_PENDING;
        else
          rc = lc_pouch_client_txn_decision(self, &req, "rollback", 1, NULL, 0,
                                            &rollback_res, error);
        if (rc == LC_OK) {
          cleanup_decision = 1;
        }
        lc_txn_decision_res_cleanup(&rollback_res);
      }
    }
    /* An external resource may be unavailable when a client opens. Preserve its
     * immutable decision and staging; registration/recovery can complete later.
     * Completed XA decisions also remain for idempotent outcome lookup rather
     * than using native-only recovery's immediate deletion policy.
     */
    if (rc == LC_ERR_PENDING &&
        (managed || lc_pouch_xa_has_participants(&req))) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      cleanup_decision = 0;
    }
    if (managed || lc_pouch_xa_has_participants(&req))
      cleanup_decision = 0;
    if (rc == LC_OK && cleanup_decision) {
      rc = lc_pouch_txn_delete_recovered_record(client, keys.keys[i], error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
    lc_free_with_allocator(NULL, body);
    lc_pouch_txn_record_cleanup(&record);
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    lc_pouch_txn_guard_release(&guard);
  }
  lc_pouch_txn_key_list_cleanup(&keys);
  return rc;
}

static void lc_pouch_tc_lease_record_cleanup(lc_pouch_tc_lease_record *record) {
  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, record->leader_id);
  lc_free_with_allocator(NULL, record->leader_endpoint);
  memset(record, 0, sizeof(*record));
}

static int
lc_pouch_tc_expiration_from_ttl_ms(lc_pouch_unix_seconds now, long ttl_ms,
                                   lc_pouch_unix_seconds *expires_at_unix,
                                   lc_error *error) {
  long ttl_seconds;

  if (expires_at_unix == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC expiration requires output", NULL, NULL,
                        NULL);
  }
  *expires_at_unix = 0L;
  if (ttl_ms <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC ttl_ms must be positive", NULL, NULL, NULL);
  }
  ttl_seconds = ttl_ms / 1000L;
  if (ttl_ms % 1000L != 0L) {
    ++ttl_seconds;
  }
  if (ttl_seconds <= 0L || (uintmax_t)ttl_seconds > (uintmax_t)LC_I64_MAX ||
      now > LC_I64_MAX - (lc_pouch_unix_seconds)ttl_seconds) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC ttl_ms exceeds supported range", NULL, NULL,
                        NULL);
  }
  *expires_at_unix = now + ttl_seconds;
  return LC_OK;
}

static int
lc_pouch_tc_read_body_text(lc_client_handle *client,
                           const lc_pouch_state_read_result *read_result,
                           char **out, size_t *out_length, lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *copy;
  int rc;

  *out = NULL;
  *out_length = 0U;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result->body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    copy = (char *)lc_client_alloc(client, length + 1U);
    if (copy == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC record", NULL, NULL, NULL);
    } else {
      memcpy(copy, bytes, length);
      copy[length] = '\0';
      *out = copy;
      *out_length = length;
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  return rc;
}

static int lc_pouch_tc_read_lease_at(lc_client_handle *client, const char *ns,
                                     const char *key,
                                     lc_pouch_tc_lease_record *record,
                                     lc_error *error) {
  lc_pouch_state_read_result read_result;
  char *body;
  size_t body_length;
  int rc;

  memset(record, 0, sizeof(*record));
  memset(&read_result, 0, sizeof(read_result));
  body = NULL;
  body_length = 0U;
  rc = lc_pouch_state_read(client->pouch, ns, key, &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_tc_read_body_text(client, &read_result, &body, &body_length,
                                    error);
  }
  if (rc == LC_OK && read_result.found) {
    lc_pouch_binary_cursor cursor;
    uint64_t term;
    int64_t expires_at;

    memset(&cursor, 0, sizeof(cursor));
    term = 0U;
    expires_at = 0;
    cursor.bytes = (const unsigned char *)body;
    cursor.length = body_length;
    rc = lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_TC_RECORD_MAGIC, error);
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_string(&cursor, &record->leader_id, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_string(&cursor, &record->leader_endpoint,
                                         error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_u64(&cursor, &term, error);
      record->term = term;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_i64(&cursor, &expires_at, error);
      record->expires_at_unix = (lc_pouch_unix_seconds)expires_at;
    }
    if (rc == LC_OK && cursor.offset != cursor.length) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader record has trailing bytes", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK &&
        (record->leader_id == NULL || record->leader_endpoint == NULL)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader record is corrupt", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK) {
      record->found = 1;
      record->version = read_result.version;
    }
  }
  lc_client_free(client, body);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_pouch_tc_lease_record_cleanup(record);
  }
  return rc;
}

static int lc_pouch_tc_read_lease(lc_client_handle *client,
                                  lc_pouch_tc_lease_record *record,
                                  lc_error *error) {
  return lc_pouch_tc_read_lease_at(client, LC_POUCH_CONTROL_NAMESPACE,
                                   LC_POUCH_TC_LEADER_KEY, record, error);
}

static int lc_pouch_tc_write_lease_at(lc_client_handle *client, const char *ns,
                                      const char *key, const char *leader_id,
                                      const char *leader_endpoint,
                                      lc_tc_term term,
                                      lc_pouch_unix_seconds expires_at_unix,
                                      lc_pouch_generation expected_version,
                                      lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_txn_buffer buffer;
  lc_source *source;
  int rc;

  memset(&buffer, 0, sizeof(buffer));
  rc =
      lc_pouch_txn_buffer_append_bytes(&buffer, LC_POUCH_TC_RECORD_MAGIC,
                                       strlen(LC_POUCH_TC_RECORD_MAGIC), error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, leader_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, leader_endpoint, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(&buffer, term, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(&buffer, (int64_t)expires_at_unix,
                                        error);
  }
  if (rc != LC_OK) {
    lc_pouch_txn_buffer_cleanup(&buffer);
    return rc;
  }
  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  options.content_type = LC_POUCH_TC_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.has_expected_version = 1;
  options.expected_version = expected_version;
  options.object_record = 1;
  rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, ns, key, source, &options,
                              &write_result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

static int lc_pouch_tc_write_lease(lc_client_handle *client,
                                   const char *leader_id,
                                   const char *leader_endpoint, lc_tc_term term,
                                   lc_pouch_unix_seconds expires_at_unix,
                                   lc_pouch_generation expected_version,
                                   lc_error *error) {
  return lc_pouch_tc_write_lease_at(
      client, LC_POUCH_CONTROL_NAMESPACE, LC_POUCH_TC_LEADER_KEY, leader_id,
      leader_endpoint, term, expires_at_unix, expected_version, error);
}

static int lc_pouch_xa_owner_read(lc_client_handle *client, const char *txn_id,
                                  lc_pouch_tc_lease_record *owner,
                                  lc_error *error) {
  char key[sizeof("owner/") + LC_XID_STRING_SIZE];
  int rc;
  memset(owner, 0, sizeof(*owner));
  if (!lc_xid_is_valid(txn_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid XA transaction ID",
                        NULL, NULL, "pouch-xa");
  (void)snprintf(key, sizeof(key), "owner/%s", txn_id);
  rc = lc_pouch_tc_read_lease_at(client, LC_POUCH_XA_NAMESPACE, key, owner,
                                 error);
  if (rc == LC_OK && owner->found &&
      (!lc_xid_is_valid(owner->leader_id) || owner->term == 0U ||
       (strcmp(owner->leader_endpoint, "open") != 0 &&
        strcmp(owner->leader_endpoint, "sealed") != 0)))
    rc =
        lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                     "XA owner journal identity or enrollment phase is invalid",
                     NULL, NULL, "pouch-xa");
  return rc;
}
static int lc_pouch_xa_owner_write(lc_client_handle *client, const char *txn_id,
                                   const char *owner_id, lc_tc_term term,
                                   int sealed, lc_pouch_unix_seconds expires,
                                   lc_pouch_generation version,
                                   lc_error *error) {
  char key[sizeof("owner/") + LC_XID_STRING_SIZE];
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_before_xa_owner_write_hook != NULL) {
    int rc = lc_pouch_test_before_xa_owner_write_hook(
        lc_pouch_test_before_xa_owner_write_context, error);
    if (rc != LC_OK)
      return rc;
  }
#endif
  (void)snprintf(key, sizeof(key), "owner/%s", txn_id);
  return lc_pouch_tc_write_lease_at(client, LC_POUCH_XA_NAMESPACE, key,
                                    owner_id, sealed ? "sealed" : "open", term,
                                    expires, version, error);
}
static int lc_pouch_xa_managed(lc_client_handle *client, const char *txn_id,
                               int *managed, lc_error *error) {
  lc_pouch_tc_lease_record owner;
  int rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  *managed = rc == LC_OK && owner.found;
  lc_pouch_tc_lease_record_cleanup(&owner);
  return rc;
}
static int lc_pouch_xa_owner_check(lc_client_handle *client, const char *txn_id,
                                   lc_tc_term term, const char *owner_id,
                                   int require_owner, int *sealed,
                                   lc_error *error) {
  lc_pouch_tc_lease_record owner;
  lc_pouch_unix_seconds now = 0L;
  int rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (sealed != NULL)
    *sealed = 0;
  if (rc == LC_OK && owner.found) {
    if (sealed != NULL)
      *sealed = strcmp(owner.leader_endpoint, "sealed") == 0;
    rc = lc_pouch_now_unix(&now, error);
    if (rc == LC_OK &&
        (owner.term != term || owner.expires_at_unix <= now ||
         (require_owner &&
          (owner_id == NULL || strcmp(owner.leader_id, owner_id) != 0))))
      rc = lc_error_set(error, require_owner ? LC_ERR_INVALID : LC_ERR_PENDING,
                        0L, "XA owner is stale, expired or requires recovery",
                        NULL, NULL, "pouch-xa");
  } else if (rc == LC_OK && owner_id != NULL)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "managed XA requires a durable owner claim before work",
                      NULL, NULL, "pouch-xa");
  lc_pouch_tc_lease_record_cleanup(&owner);
  return rc;
}
static int lc_pouch_xa_managed_term(lc_client_handle *client,
                                    const char *txn_id, lc_tc_term *term,
                                    lc_error *error) {
  lc_pouch_tc_lease_record owner;
  int rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (rc == LC_OK && owner.found) {
    *term = owner.term;
    rc = lc_pouch_xa_owner_check(client, txn_id, *term, NULL, 0, NULL, error);
  }
  lc_pouch_tc_lease_record_cleanup(&owner);
  return rc;
}
static int lc_pouch_xa_seal_vote(lc_client_handle *client, const char *txn_id,
                                 lc_tc_term term, lc_error *error) {
  lc_pouch_tc_lease_record owner;
  int rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (rc == LC_OK && owner.found &&
      strcmp(owner.leader_endpoint, "sealed") != 0)
    rc = lc_pouch_xa_owner_write(client, txn_id, owner.leader_id, term, 1,
                                 owner.expires_at_unix, owner.version, error);
  lc_pouch_tc_lease_record_cleanup(&owner);
  return rc;
}
static int lc_pouch_xa_decision_authority(lc_pouch_txn_decision_context *ctx,
                                          int *sealed, lc_error *error) {
  lc_pouch_tc_lease_record owner;
  lc_pouch_unix_seconds now = 0L;
  int terminal =
      ctx->record.state != NULL && strcmp(ctx->record.state, "prepare") != 0;
  int rc =
      lc_pouch_xa_owner_read(ctx->client, ctx->request->txn_id, &owner, error);
  *sealed = 0;
  if (rc == LC_OK && owner.found)
    *sealed = terminal || strcmp(owner.leader_endpoint, "sealed") == 0;
  if (rc == LC_OK && ctx->takeover) {
    rc = lc_pouch_now_unix(&now, error);
    if (rc == LC_OK &&
        (ctx->owner_id == NULL || !lc_xid_is_valid(ctx->owner_id) ||
         ctx->request->tc_term == 0U ||
         (owner.found && (owner.expires_at_unix > now ||
                          ctx->request->tc_term <= owner.term))))
      rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                        "another XA owner is active or takeover epoch is stale",
                        NULL, NULL, "pouch-xa");
  } else if (rc == LC_OK && owner.found && !terminal) {
    rc = lc_pouch_xa_owner_check(ctx->client, ctx->request->txn_id,
                                 ctx->request->tc_term, ctx->owner_id,
                                 ctx->owner_id != NULL, sealed, error);
    if (rc == LC_OK && ctx->owner_id == NULL &&
        strcmp(ctx->state, "prepare") != 0)
      rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                        "managed XA decisions require their fenced owner", NULL,
                        NULL, "pouch-xa");
  } else if (rc == LC_OK && !owner.found && ctx->owner_id != NULL)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "managed XA decision has no durable owner claim", NULL,
                      NULL, "pouch-xa");
  if (rc == LC_OK && (owner.found || ctx->owner_id != NULL)) {
    size_t i;
    /* This coordinator owns one Pouch RM. Explicit foreign backend hashes
     * belong to raw multi-RM coordination, not this managed participant set. */
    for (i = 0U; rc == LC_OK && i < ctx->request->participant_count; ++i)
      rc = lc_pouch_txn_validate_target_backend(
          ctx->client->pouch, ctx->request->participants[i].backend_hash,
          error);
  }
  lc_pouch_tc_lease_record_cleanup(&owner);
  return rc;
}

static int lc_pouch_xa_snapshot_locked(lc_client_handle *client,
                                       const char *txn_id, lc_xa_snapshot *out,
                                       lc_error *error) {
  lc_pouch_txn_record record;
  lc_pouch_tc_lease_record owner;
  size_t i;
  int rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &record, error);
  memset(&owner, 0, sizeof(owner));
  memset(out, 0, sizeof(*out));
  if (rc == LC_OK && record.state == NULL)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, "XA transaction not found",
                      NULL, "not_found", "pouch-xa");
  if (rc == LC_OK && strcmp(record.state, "prepare") != 0 &&
      strcmp(record.state, "commit") != 0 &&
      strcmp(record.state, "rollback") != 0)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA snapshot has an invalid durable decision", NULL, NULL,
                      "pouch-xa");
  if (rc == LC_OK)
    rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_completed_read(client, txn_id, &out->completed_at_unix,
                                    error);
  if (rc == LC_OK && out->completed_at_unix != 0 && !owner.found)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "XA transaction history has expired", NULL, "not_found",
                      "pouch-xa");
  if (rc == LC_OK && out->completed_at_unix != 0 &&
      strcmp(record.state, "prepare") == 0)
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "XA completion has no terminal decision", NULL, NULL,
                      "pouch-xa");
  if (rc == LC_OK) {
    strcpy(out->txn_id, txn_id);
    out->decision = strcmp(record.state, "commit") == 0     ? 1
                    : strcmp(record.state, "rollback") == 0 ? 2
                                                            : 0;
    out->term = record.tc_term;
    out->sealed = out->decision != 0 ||
                  (owner.found && strcmp(owner.leader_endpoint, "sealed") == 0);
    for (i = 0U; i < record.participant_count; ++i) {
      if (record.votes[i] != 0U)
        out->sealed = 1;
      if (lc_pouch_xa_participant(&record.participants[i]))
        ++out->branch_count;
    }
    if (out->branch_count != 0U) {
      out->branches = (lc_xa_branch_record *)lc_calloc_with_allocator(
          NULL, out->branch_count, sizeof(*out->branches));
      if (out->branches == NULL)
        rc =
            lc_error_set(error, LC_ERR_NOMEM, 0L, "cannot allocate XA snapshot",
                         NULL, NULL, "pouch-xa");
    }
    out->branch_count = 0U;
    for (i = 0U; rc == LC_OK && i < record.participant_count; ++i)
      if (lc_pouch_xa_participant(&record.participants[i])) {
        int prepared;
        rc = lc_pouch_xa_branch_read(client->pouch, record.participants[i].key,
                                     &out->branches[out->branch_count], error);
        if (rc == LC_OK)
          rc = lc_pouch_client_xa_prepared(
              &client->pub, txn_id, record.participants[i].key,
              out->branches[out->branch_count].phase, &prepared, error);
        if (rc == LC_OK)
          ++out->branch_count;
      }
  }
  lc_pouch_tc_lease_record_cleanup(&owner);
  lc_pouch_txn_record_cleanup(&record);
  if (rc != LC_OK)
    lc_xa_snapshot_cleanup(out);
  return rc;
}
static int lc_pouch_xa_begin_guard(lc_client *self, const char *txn_id,
                                   lc_pouch_txn_guard *guard, lc_error *error) {
  if (self == NULL || !((lc_client_handle *)self)->is_pouch ||
      !lc_xid_is_valid(txn_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "managed XA requires Pouch and a valid transaction ID",
                        NULL, NULL, "pouch-xa");
  return lc_pouch_txn_guard_acquire(((lc_client_handle *)self)->pouch, txn_id,
                                    guard, error);
}
int lc_pouch_client_xa_snapshot(lc_client *self, const char *txn_id,
                                lc_xa_snapshot *out, lc_error *error) {
  lc_pouch_txn_guard guard;
  int rc;
  if (out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA snapshot output is missing", NULL, NULL,
                        "pouch-xa");
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_xa_begin_guard(self, txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc =
      lc_pouch_xa_snapshot_locked((lc_client_handle *)self, txn_id, out, error);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}
int lc_pouch_client_xa_claim(lc_client *self, const char *owner_id,
                             const char *txn_id, long ttl_seconds, int recover,
                             lc_xa_snapshot *out, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_txn_record record;
  lc_pouch_tc_lease_record owner;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  lc_pouch_unix_seconds now = 0L, expires = 0L;
  lc_tc_term term;
  int rc;
  int same;
  int created = 0;
  int acquired = 0;
  int adopting = recover == 2;
  int recovering = recover == 1;
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (!lc_xid_is_valid(owner_id) || out == NULL || ttl_seconds <= 0L ||
      (recover != 0 && recover != 1 && recover != 2))
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "XA claim requires owner, output, positive TTL and mode", NULL, NULL,
        "pouch-xa");
  rc = lc_pouch_xa_begin_guard(self, txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  memset(&record, 0, sizeof(record));
  memset(&owner, 0, sizeof(owner));
  memset(&result, 0, sizeof(result));
  lc_txn_decision_req_init(&request);
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_timestamp_add(now, ttl_seconds, "XA owner TTL", &expires,
                                error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &record, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (rc == LC_OK) {
    lc_pouch_unix_seconds completed = 0;
    rc = lc_pouch_xa_completed_read(client, txn_id, &completed, error);
    if (rc == LC_OK && completed != 0) {
      rc = recovering
               ? lc_pouch_xa_snapshot_locked(client, txn_id, out, error)
               : lc_error_set(
                     error, LC_ERR_INVALID, 0L,
                     "XA transaction ID has completed or expired history", NULL,
                     NULL, "pouch-xa");
      lc_pouch_tc_lease_record_cleanup(&owner);
      lc_pouch_txn_record_cleanup(&record);
      lc_pouch_txn_guard_release(&guard);
      return rc;
    }
  }
  same = rc == LC_OK && owner.found && strcmp(owner.leader_id, owner_id) == 0 &&
         owner.expires_at_unix > now;
  if (rc == LC_OK && adopting) {
    size_t i;
    if (owner.found ||
        (record.state != NULL &&
         (strcmp(record.state, "prepare") != 0 || record.tc_term != 0U ||
          lc_pouch_txn_record_has_xa(&record))))
      rc = lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "XA adoption requires an unowned, undecided local transaction", NULL,
          NULL, "pouch-xa");
    for (i = 0U; rc == LC_OK && i < record.participant_count; ++i)
      if (record.votes[i] != 0U)
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "XA adoption requires unvoted participants", NULL,
                          NULL, "pouch-xa");
  }
  if (rc == LC_OK && recovering && !owner.found)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "XA recovery has no durable managed owner history", NULL,
                      "not_found", "pouch-xa");
  if (rc == LC_OK && owner.found && owner.expires_at_unix > now &&
      (!same || (recovering && (record.state == NULL ||
                                strcmp(record.state, "prepare") == 0))))
    rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                      "XA transaction owner is active", NULL, NULL, "pouch-xa");
  if (rc == LC_OK && !recovering && !adopting &&
      (owner.found || record.state != NULL) && !same)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "XA transaction ID already exists; use recovery", NULL,
                      NULL, "pouch-xa");
  term = owner.term > record.tc_term ? owner.term : record.tc_term;
  if (rc == LC_OK && !same && term == ~(uint64_t)0U)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, "XA owner epoch is exhausted",
                      NULL, NULL, "pouch-xa");
  if (rc == LC_OK) {
    if (!same)
      ++term;
    request.txn_id = txn_id;
    request.tc_term = term;
    request.participants = record.participants;
    request.participant_count = record.participant_count;
    request.target_backend_hash = record.target_backend_hash;
    request.expires_at_unix = record.expires_at_unix;
    /* Fence first: persist ABORT for abandoned undecided work, or the same
     * immutable terminal direction, before publishing a replacement owner.
     * A crash between the two writes leaves mismatched epochs and blocks all
     * prepare/vote/progress. Recovery advances again; it never replays SQL. */
    if (!owner.found && !recovering) {
      /* First publish the owner identity. A crash before .txns exists is then
       * recoverable as an abandoned begin, and raw journals cannot adopt it. */
      rc = lc_pouch_xa_owner_write(client, txn_id, owner_id, term, 0, expires,
                                   0UL, error);
      if (rc == LC_OK) {
        acquired = 1;
        lc_pouch_tc_lease_record_cleanup(&owner);
        rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
        same = rc == LC_OK;
        created = same;
      }
    }
    if (rc == LC_OK && (!same || record.state == NULL || adopting))
      rc = lc_pouch_client_txn_decision(
          self, &request,
          recovering
              ? (record.state != NULL && strcmp(record.state, "commit") == 0
                     ? "commit"
                     : "rollback")
              : "prepare",
          1, owner_id, !same, &result, error);
    if (rc == LC_OK || (rc == LC_ERR_PENDING && result.state != NULL)) {
      if (!created) {
        rc = lc_pouch_xa_owner_write(
            client, txn_id, owner_id, term,
            recovering ||
                (owner.found && strcmp(owner.leader_endpoint, "sealed") == 0),
            expires, owner.found ? owner.version : 0UL, error);
        if (rc == LC_OK && !same)
          acquired = 1;
      }
      if (rc == LC_OK)
        rc = lc_pouch_xa_snapshot_locked(client, txn_id, out, error);
    }
  }
  lc_txn_decision_res_cleanup(&result);
  lc_pouch_tc_lease_record_cleanup(&owner);
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_txn_guard_release(&guard);
  out->claim_acquired = acquired;
  return rc;
}
int lc_pouch_client_xa_owner_update(lc_client *self, const char *owner_id,
                                    const char *txn_id, long ttl_seconds,
                                    int seal, int release, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_tc_lease_record owner;
  lc_pouch_unix_seconds now = 0L, expires = 0L;
  int rc;
  if (!lc_xid_is_valid(owner_id) || (!release && !seal && ttl_seconds <= 0L))
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid XA owner update",
                        NULL, NULL, "pouch-xa");
  rc = lc_pouch_xa_begin_guard(self, txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_xa_owner_read(client, txn_id, &owner, error);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK && (!owner.found || strcmp(owner.leader_id, owner_id) != 0 ||
                      (!release && owner.expires_at_unix <= now)))
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, "XA owner update is fenced",
                      NULL, NULL, "pouch-xa");
  if (rc == LC_OK) {
    expires = release ? 0 : owner.expires_at_unix;
    if (ttl_seconds > 0L && !release)
      rc = lc_pouch_timestamp_add(now, ttl_seconds, "XA owner TTL", &expires,
                                  error);
    if (rc == LC_OK)
      rc = lc_pouch_xa_owner_write(
          client, txn_id, owner_id, owner.term,
          seal || strcmp(owner.leader_endpoint, "sealed") == 0, expires,
          owner.version, error);
  }
  lc_pouch_tc_lease_record_cleanup(&owner);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}
int lc_pouch_client_xa_decide(lc_client *self, const char *owner_id,
                              const char *txn_id, int commit, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_txn_record record;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  size_t i;
  int sealed = 0;
  int rc;
  if (!lc_xid_is_valid(owner_id) || (commit != 0 && commit != 1))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid XA decision owner/mode", NULL, NULL,
                        "pouch-xa");
  rc = lc_pouch_xa_begin_guard(self, txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  memset(&result, 0, sizeof(result));
  lc_txn_decision_req_init(&request);
  rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &record, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_owner_check(client, txn_id, record.tc_term, owner_id, 1,
                                 &sealed, error);
  if (rc == LC_OK && record.state == NULL)
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, "XA transaction not found",
                      NULL, "not_found", "pouch-xa");
  if (rc == LC_OK && commit && strcmp(record.state, "prepare") == 0) {
    if (!sealed || record.participant_count == 0U)
      rc = LC_ERR_INVALID;
    for (i = 0U; rc == LC_OK && i < record.participant_count; ++i)
      if (record.votes[i] != 1U)
        rc = LC_ERR_INVALID;
    if (rc != LC_OK)
      rc = lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "XA COMMIT requires sealed enrollment and every YES vote", NULL, NULL,
          "pouch-xa");
  }
  if (rc == LC_OK) {
    request.txn_id = txn_id;
    request.tc_term = record.tc_term;
    request.expires_at_unix = record.expires_at_unix;
    rc = lc_pouch_client_txn_decision(self, &request,
                                      commit ? "commit" : "rollback", 1,
                                      owner_id, 0, &result, error);
  }
  lc_txn_decision_res_cleanup(&result);
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}
int lc_pouch_client_xa_prepare_pouch(lc_client *self, const char *owner_id,
                                     const char *txn_id, lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_guard guard;
  lc_pouch_txn_record record;
  size_t i;
  int ready = 0;
  int sealed = 0;
  int rc;
  if (!lc_xid_is_valid(owner_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid XA prepare owner",
                        NULL, NULL, "pouch-xa");
  rc = lc_pouch_xa_begin_guard(self, txn_id, &guard, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_xa_read_txn(client->pouch, txn_id, &record, error);
  if (rc == LC_OK)
    rc = lc_pouch_xa_owner_check(client, txn_id, record.tc_term, owner_id, 1,
                                 &sealed, error);
  if (rc == LC_OK && (!sealed || record.state == NULL))
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "seal XA enrollment before prepare", NULL, NULL,
                      "pouch-xa");
  for (i = 0U; rc == LC_OK && strcmp(record.state, "prepare") == 0 &&
               i < record.participant_count;
       ++i)
    if (!lc_pouch_xa_participant(&record.participants[i])) {
      lc_pouch_lease_record lease;
      const char *ns = record.participants[i].ns;
      const char *key = record.participants[i].key;
      memset(&lease, 0, sizeof(lease));
      rc = lc_pouch_read_lease_record(client, ns, key, &lease, error);
      if (rc == LC_OK && (!lease.found || strcmp(lease.txn_id, txn_id) != 0))
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "XA Pouch participant lost its transaction lease",
                          NULL, NULL, "pouch-xa");
      if (rc == LC_OK)
        rc = lc_pouch_txn_record_vote(client, txn_id, ns, key, 0, 0, &ready,
                                      NULL, error);
      lc_pouch_lease_record_cleanup(&lease);
    }
  lc_pouch_txn_record_cleanup(&record);
  lc_pouch_txn_guard_release(&guard);
  return rc;
}

typedef struct lc_pouch_xa_completed_entry {
  char txn_id[LC_XID_STRING_SIZE];
  lc_pouch_unix_seconds completed;
} lc_pouch_xa_completed_entry;

static int
lc_pouch_xa_collect_completed(const lc_pouch_state_visit_entry *entry,
                              void *context, lc_error *error) {
  if (entry->key == NULL || strncmp(entry->key, "completed/", 10U) != 0)
    return LC_OK;
  if (!lc_xid_is_valid(entry->key + 10U))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid XA completed-history key", NULL, NULL,
                        "pouch-xa");
  return lc_pouch_txn_key_list_append((lc_pouch_txn_key_list *)context,
                                      entry->key + 10U, error);
}
static int lc_pouch_xa_completed_compare(const void *a, const void *b) {
  const lc_pouch_xa_completed_entry *left = a, *right = b;
  if (left->completed != right->completed)
    return left->completed < right->completed ? -1 : 1;
  return strcmp(left->txn_id, right->txn_id);
}
int lc_pouch_client_xa_retire(lc_client *self, long retention_seconds,
                              size_t max_records, size_t *retired_count,
                              lc_error *error) {
  lc_client_handle *client = (lc_client_handle *)self;
  lc_pouch_txn_key_list keys;
  lc_pouch_xa_completed_entry *entries = NULL;
  lc_pouch_unix_seconds now = 0;
  size_t i, count = 0, remaining;
  int rc;
  if (retired_count != NULL)
    *retired_count = 0U;
  if (client == NULL || !client->is_pouch || retention_seconds < -1L ||
      retired_count == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid XA history-retirement policy/client/output",
                        NULL, NULL, "pouch-xa");
  memset(&keys, 0, sizeof(keys));
  rc = lc_pouch_state_visit(client->pouch, LC_POUCH_XA_NAMESPACE,
                            lc_pouch_xa_collect_completed, &keys, error);
  if (rc == LC_OK && keys.count != 0U) {
    entries = lc_calloc_with_allocator(NULL, keys.count, sizeof(*entries));
    if (entries == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "cannot allocate XA history-retirement candidates",
                        NULL, NULL, "pouch-xa");
    if (rc == LC_OK)
      rc = lc_pouch_now_unix(&now, error);
  }
  for (i = 0U; rc == LC_OK && i < keys.count; ++i) {
    lc_pouch_unix_seconds completed = 0;
    rc = lc_pouch_xa_completed_read(client, keys.keys[i], &completed, error);
    if (rc == LC_OK && completed != 0) {
      strcpy(entries[count].txn_id, keys.keys[i]);
      entries[count++].completed = completed;
    }
  }
  if (rc == LC_OK && count > 1U)
    qsort(entries, count, sizeof(*entries), lc_pouch_xa_completed_compare);
  remaining = count;
  for (i = 0U; rc == LC_OK && i < count; ++i) {
    lc_pouch_txn_guard guard;
    lc_pouch_tc_lease_record owner;
    lc_pouch_txn_record record;
    lc_pouch_unix_seconds completed = 0;
    char key[sizeof("completed/") + LC_XID_STRING_SIZE];
    size_t j;
    int eligible;
    rc = lc_pouch_txn_guard_acquire(client->pouch, entries[i].txn_id, &guard,
                                    error);
    if (rc != LC_OK)
      break;
    memset(&owner, 0, sizeof(owner));
    memset(&record, 0, sizeof(record));
    rc = lc_pouch_xa_completed_read(client, entries[i].txn_id, &completed,
                                    error);
    if (rc == LC_OK)
      rc = lc_pouch_xa_owner_read(client, entries[i].txn_id, &owner, error);
    eligible = rc == LC_OK && completed != 0 &&
               (!owner.found || owner.expires_at_unix <= now) &&
               (!owner.found ||
                (retention_seconds >= 0L && now >= completed &&
                 now - completed >= retention_seconds) ||
                (max_records != 0U && remaining > max_records));
    if (eligible) {
      rc = lc_pouch_xa_read_txn(client->pouch, entries[i].txn_id, &record,
                                error);
      if (rc == LC_OK && owner.found && record.state == NULL)
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "XA completion lost its authoritative decision", NULL,
                          NULL, "pouch-xa");
      if (rc == LC_OK && record.state != NULL &&
          strcmp(record.state, "commit") != 0 &&
          strcmp(record.state, "rollback") != 0)
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "unfinished XA history cannot retire", NULL, NULL,
                          "pouch-xa");
      if (rc == LC_OK && owner.found) {
        lc_txn_decision_req request;
        lc_txn_decision_req_init(&request);
        request.txn_id = entries[i].txn_id;
        request.participants = record.participants;
        request.participant_count = record.participant_count;
        rc = lc_pouch_xa_application_gate(client->pouch, &request, record.state,
                                          error);
      }
      /* Owner deletion is the durable recovery barrier. A crash after it must
       * report expired, never reconstruct ABORT from an absent .txns record.
       * Keep the completion marker until every dependent record is removed. */
      (void)snprintf(key, sizeof(key), "owner/%s", entries[i].txn_id);
      if (rc == LC_OK)
        rc = lc_pouch_tc_delete_key(client, LC_POUCH_XA_NAMESPACE, key, 0, 0U,
                                    error);
#ifdef LOCKDC_TEST_BUILD
      if (rc == LC_OK && owner.found &&
          lc_pouch_test_after_xa_retire_owner_hook != NULL)
        rc = lc_pouch_test_after_xa_retire_owner_hook(
            lc_pouch_test_after_xa_retire_owner_context, error);
#endif
      for (j = 0U; rc == LC_OK && j < record.participant_count; ++j)
        if (lc_pouch_xa_participant(&record.participants[j]))
          rc = lc_pouch_tc_delete_key(client, LC_POUCH_XA_NAMESPACE,
                                      record.participants[j].key, 0, 0U, error);
      if (rc == LC_OK)
        rc = lc_pouch_tc_delete_key(client, LC_POUCH_TXN_NAMESPACE,
                                    entries[i].txn_id, 0, 0U, error);
      (void)snprintf(key, sizeof(key), "completed/%s", entries[i].txn_id);
      if (rc == LC_OK)
        rc = lc_pouch_tc_delete_key(client, LC_POUCH_XA_NAMESPACE, key, 0, 0U,
                                    error);
      if (rc == LC_OK) {
        ++*retired_count;
        --remaining;
      }
    } else if (rc == LC_OK && completed == 0 && remaining != 0U)
      --remaining;
    lc_pouch_tc_lease_record_cleanup(&owner);
    lc_pouch_txn_record_cleanup(&record);
    lc_pouch_txn_guard_release(&guard);
  }
  lc_free_with_allocator(NULL, entries);
  lc_pouch_txn_key_list_cleanup(&keys);
  return rc;
}

static int lc_pouch_tc_delete_key(lc_client_handle *client, const char *ns,
                                  const char *key, int has_expected_version,
                                  lc_pouch_generation expected_version,
                                  lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.object_record = 1;
  if (has_expected_version) {
    options.has_expected_version = 1;
    options.expected_version = expected_version;
  }
  rc = lc_pouch_state_delete(client->pouch, ns, key, &options, &result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_tc_copy_lease_acquire_res(
    lc_tc_lease_acquire_res *out, int granted, const char *leader_id,
    const char *leader_endpoint, lc_tc_term term,
    lc_pouch_unix_seconds expires_at_unix, lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->granted = granted;
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-lease");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_lease_acquire_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_tc_copy_lease_renew_res(
    lc_tc_lease_renew_res *out, int renewed, const char *leader_id,
    const char *leader_endpoint, lc_tc_term term,
    lc_pouch_unix_seconds expires_at_unix, lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->renewed = renewed;
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-lease");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_lease_renew_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_tc_copy_leader_res(
    lc_tc_leader_res *out, const char *leader_id, const char *leader_endpoint,
    lc_tc_term term, lc_pouch_unix_seconds expires_at_unix, lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-leader");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_leader_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC leader response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static char *lc_pouch_tc_endpoint_normalize(const char *endpoint,
                                            lc_error *error) {
  const char *start;
  const char *end;
  char *copy;
  size_t length;

  if (endpoint == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch TC endpoint must be non-empty", NULL, NULL, NULL);
    return NULL;
  }
  start = endpoint;
  while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r') {
    ++start;
  }
  end = start + strlen(start);
  while (end > start && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' ||
                         end[-1] == '\r')) {
    --end;
  }
  while (end > start + 1 && end[-1] == '/') {
    --end;
  }
  length = (size_t)(end - start);
  if (length == 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch TC endpoint must be non-empty", NULL, NULL, NULL);
    return NULL;
  }
  if (length > LC_POUCH_CONTROL_STRING_MAX) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch TC endpoint exceeds supported length", NULL, NULL,
                 NULL);
    return NULL;
  }
  copy = (char *)lc_realloc_local(NULL, length + 1U);
  if (copy == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC endpoint", NULL, NULL, NULL);
    return NULL;
  }
  memcpy(copy, start, length);
  copy[length] = '\0';
  return copy;
}

static char *lc_pouch_tc_control_string_normalize(const char *value,
                                                  const char *field,
                                                  lc_error *error) {
  const char *start;
  const char *end;
  char *copy;
  size_t length;

  if (value == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L, field, NULL, NULL, NULL);
    return NULL;
  }
  start = value;
  while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r') {
    ++start;
  }
  end = start + strlen(start);
  while (end > start && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' ||
                         end[-1] == '\r')) {
    --end;
  }
  length = (size_t)(end - start);
  if (length == 0U || length > LC_POUCH_CONTROL_STRING_MAX) {
    lc_error_set(error, LC_ERR_INVALID, 0L, field, NULL, NULL, NULL);
    return NULL;
  }
  copy = (char *)lc_alloc_with_allocator(NULL, length + 1U);
  if (copy == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC control string", NULL, NULL,
                 NULL);
    return NULL;
  }
  memcpy(copy, start, length);
  copy[length] = '\0';
  return copy;
}

static int lc_pouch_tc_string_compare(const void *left, const void *right) {
  const char *const *a;
  const char *const *b;

  a = (const char *const *)left;
  b = (const char *const *)right;
  return strcmp(*a != NULL ? *a : "", *b != NULL ? *b : "");
}

static int lc_pouch_tc_endpoint_list_append(lc_pouch_tc_endpoint_list *list,
                                            const char *endpoint,
                                            lc_error *error) {
  char **items;
  char *normalized;
  size_t index;

  normalized = lc_pouch_tc_endpoint_normalize(endpoint, error);
  if (normalized == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  for (index = 0U; index < list->count; ++index) {
    if (strcmp(list->items[index], normalized) == 0) {
      lc_free_local(normalized);
      return LC_OK;
    }
  }
  if (list->count == list->capacity) {
    size_t next_capacity;

    next_capacity = list->capacity == 0U ? 4U : list->capacity * 2U;
    items =
        (char **)lc_realloc_local(list->items, next_capacity * sizeof(char *));
    if (items == NULL) {
      lc_free_local(normalized);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch TC endpoint list", NULL,
                          NULL, NULL);
    }
    list->items = items;
    list->capacity = next_capacity;
  }
  list->items[list->count] = normalized;
  ++list->count;
  return LC_OK;
}

static int lc_pouch_tc_string_list_append_unique(lc_string_list *list,
                                                 const char *value,
                                                 lc_error *error) {
  char **items;
  char *normalized;
  size_t index;

  normalized = lc_pouch_tc_endpoint_normalize(value, error);
  if (normalized == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  for (index = 0U; index < list->count; ++index) {
    if (strcmp(list->items[index], normalized) == 0) {
      lc_free_local(normalized);
      return LC_OK;
    }
  }
  items = (char **)lc_realloc_local(list->items,
                                    (list->count + 1U) * sizeof(char *));
  if (items == NULL) {
    lc_free_local(normalized);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC string list", NULL, NULL,
                        NULL);
  }
  list->items = items;
  list->items[list->count] = normalized;
  ++list->count;
  return LC_OK;
}

static void lc_pouch_tc_endpoint_list_cleanup(lc_pouch_tc_endpoint_list *list) {
  size_t i;

  if (list == NULL) {
    return;
  }
  for (i = 0U; i < list->count; ++i) {
    lc_free_local(list->items[i]);
  }
  lc_free_local(list->items);
  memset(list, 0, sizeof(*list));
}

static int lc_pouch_tc_copy_string_list(lc_string_list *out,
                                        lc_pouch_tc_endpoint_list *list,
                                        lc_error *error) {
  if (list->count > 1U) {
    qsort(list->items, list->count, sizeof(list->items[0]),
          lc_pouch_tc_string_compare);
  }
  memset(out, 0, sizeof(*out));
  out->items = list->items;
  out->count = list->count;
  list->items = NULL;
  list->count = 0U;
  list->capacity = 0U;
  (void)error;
  return LC_OK;
}

typedef struct lc_pouch_tc_cluster_scan {
  lc_client_handle *client;
  lc_pouch_tc_endpoint_list list;
  lc_pouch_unix_seconds now;
} lc_pouch_tc_cluster_scan;

static int lc_pouch_tc_cluster_read_endpoint(
    lc_client_handle *client, const char *key, char **endpoint,
    lc_pouch_unix_seconds *updated_at_unix,
    lc_pouch_unix_seconds *expires_at_unix, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_binary_cursor cursor;
  char *body;
  size_t body_length;
  int64_t signed_value;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  memset(&cursor, 0, sizeof(cursor));
  body = NULL;
  body_length = 0U;
  *endpoint = NULL;
  *updated_at_unix = 0L;
  *expires_at_unix = 0L;
  rc = lc_pouch_state_read(client->pouch, LC_POUCH_CONTROL_NAMESPACE, key,
                           &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_body_text(client, &read_result, &body, &body_length,
                                    error);
  }
  if (rc == LC_OK) {
    cursor.bytes = (const unsigned char *)body;
    cursor.length = body_length;
    rc = lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_TC_CLUSTER_RECORD_MAGIC,
                                      error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_string(&cursor, endpoint, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    *updated_at_unix = (lc_pouch_unix_seconds)signed_value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    *expires_at_unix = (lc_pouch_unix_seconds)signed_value;
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch TC cluster record has trailing bytes", NULL, NULL,
                      "pouch");
  }
  lc_client_free(client, body);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, *endpoint);
    *endpoint = NULL;
  }
  return rc;
}

static int lc_pouch_tc_cluster_visit(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  lc_pouch_tc_cluster_scan *scan;
  char *endpoint;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_unix_seconds expires_at_unix;
  int rc;

  scan = (lc_pouch_tc_cluster_scan *)context;
  if (entry->key == NULL || strncmp(entry->key, LC_POUCH_TC_CLUSTER_PREFIX,
                                    strlen(LC_POUCH_TC_CLUSTER_PREFIX)) != 0) {
    return LC_OK;
  }
  endpoint = NULL;
  updated_at_unix = 0L;
  expires_at_unix = 0L;
  rc = lc_pouch_tc_cluster_read_endpoint(scan->client, entry->key, &endpoint,
                                         &updated_at_unix, &expires_at_unix,
                                         error);
  if (rc == LC_OK && endpoint != NULL &&
      (expires_at_unix == 0L || expires_at_unix > scan->now)) {
    rc = lc_pouch_tc_endpoint_list_append(&scan->list, endpoint, error);
    if (rc == LC_OK && updated_at_unix > scan->list.updated_at_unix) {
      scan->list.updated_at_unix = updated_at_unix;
    }
    if (rc == LC_OK && expires_at_unix > scan->list.expires_at_unix) {
      scan->list.expires_at_unix = expires_at_unix;
    }
  }
  lc_free_with_allocator(NULL, endpoint);
  return rc;
}

static int lc_pouch_tc_cluster_response(lc_client_handle *client,
                                        lc_tc_cluster_res *out,
                                        lc_error *error) {
  lc_pouch_tc_cluster_scan scan;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  rc = lc_pouch_now_unix(&scan.now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_visit(client->pouch, LC_POUCH_CONTROL_NAMESPACE,
                              lc_pouch_tc_cluster_visit, &scan, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_string_list(&out->endpoints, &scan.list, error);
  }
  if (rc == LC_OK) {
    out->updated_at_unix = scan.list.updated_at_unix;
    out->expires_at_unix = scan.list.expires_at_unix;
    out->correlation_id = lc_strdup_local("pouch-tc-cluster");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC cluster response", NULL,
                        NULL, NULL);
    }
  }
  if (rc != LC_OK) {
    lc_tc_cluster_res_cleanup(out);
  }
  lc_pouch_tc_endpoint_list_cleanup(&scan.list);
  return rc;
}

static int lc_pouch_tc_write_cluster_self(lc_client_handle *client,
                                          const char *endpoint,
                                          lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_pouch_txn_buffer buffer;
  char *normalized;
  lc_source *source;
  lc_pouch_unix_seconds now = 0;
  int rc;

  normalized = lc_pouch_tc_endpoint_normalize(endpoint, error);
  if (normalized == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  memset(&buffer, 0, sizeof(buffer));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_bytes(
        &buffer, LC_POUCH_TC_CLUSTER_RECORD_MAGIC,
        strlen(LC_POUCH_TC_CLUSTER_RECORD_MAGIC), error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_string(&buffer, normalized, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(&buffer, (int64_t)now, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(&buffer, 0, error);
  }
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  options.content_type = LC_POUCH_TC_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.object_record = 1;
  if (rc == LC_OK) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, LC_POUCH_CONTROL_NAMESPACE,
                              LC_POUCH_TC_CLUSTER_PREFIX "self", source,
                              &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_free_local(normalized);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static lc_tc_rm_backend *
lc_pouch_tc_rm_registry_backend(lc_pouch_tc_rm_registry *registry,
                                const char *backend_hash, lc_error *error) {
  lc_tc_rm_backend *backends;
  size_t i;

  for (i = 0U; i < registry->backend_count; ++i) {
    if (strcmp(registry->backends[i].backend_hash, backend_hash) == 0) {
      return &registry->backends[i];
    }
  }
  if (registry->backend_count == registry->backend_capacity) {
    size_t next_capacity;

    next_capacity =
        registry->backend_capacity == 0U ? 4U : registry->backend_capacity * 2U;
    backends = (lc_tc_rm_backend *)lc_realloc_with_allocator(
        NULL, registry->backends, next_capacity * sizeof(lc_tc_rm_backend));
    if (backends == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch TC RM backend list", NULL, NULL,
                   NULL);
      return NULL;
    }
    registry->backends = backends;
    registry->backend_capacity = next_capacity;
  }
  memset(&registry->backends[registry->backend_count], 0,
         sizeof(lc_tc_rm_backend));
  registry->backends[registry->backend_count].backend_hash =
      lc_strdup_local(backend_hash);
  if (registry->backends[registry->backend_count].backend_hash == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC RM backend", NULL, NULL, NULL);
    return NULL;
  }
  ++registry->backend_count;
  return &registry->backends[registry->backend_count - 1U];
}

static int lc_pouch_tc_rm_backend_compare(const void *left, const void *right) {
  const lc_tc_rm_backend *a;
  const lc_tc_rm_backend *b;

  a = (const lc_tc_rm_backend *)left;
  b = (const lc_tc_rm_backend *)right;
  return strcmp(a->backend_hash != NULL ? a->backend_hash : "",
                b->backend_hash != NULL ? b->backend_hash : "");
}

static void lc_pouch_tc_rm_registry_sort(lc_pouch_tc_rm_registry *registry) {
  size_t i;

  if (registry->backend_count > 1U) {
    qsort(registry->backends, registry->backend_count,
          sizeof(registry->backends[0]), lc_pouch_tc_rm_backend_compare);
  }
  for (i = 0U; i < registry->backend_count; ++i) {
    if (registry->backends[i].endpoints.count > 1U) {
      qsort(registry->backends[i].endpoints.items,
            registry->backends[i].endpoints.count,
            sizeof(registry->backends[i].endpoints.items[0]),
            lc_pouch_tc_string_compare);
    }
  }
}

static void lc_pouch_tc_rm_registry_cleanup(lc_pouch_tc_rm_registry *registry) {
  size_t i;

  if (registry == NULL) {
    return;
  }
  for (i = 0U; i < registry->backend_count; ++i) {
    lc_free_local(registry->backends[i].backend_hash);
    lc_string_list_cleanup(&registry->backends[i].endpoints);
  }
  lc_free_with_allocator(NULL, registry->backends);
  memset(registry, 0, sizeof(*registry));
}

static int lc_pouch_tc_rm_registry_read(lc_client_handle *client,
                                        lc_pouch_tc_rm_registry *registry,
                                        lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_binary_cursor cursor;
  char *body;
  size_t body_length;
  uint64_t count;
  uint64_t endpoint_count;
  uint64_t index;
  uint64_t endpoint_index;
  int64_t signed_value = 0;
  char *backend_hash;
  char *endpoint;
  lc_tc_rm_backend *backend;
  int rc;

  memset(registry, 0, sizeof(*registry));
  memset(&read_result, 0, sizeof(read_result));
  body = NULL;
  body_length = 0U;
  rc = lc_pouch_state_read(client->pouch, LC_POUCH_CONTROL_NAMESPACE,
                           LC_POUCH_TC_RM_MEMBERS_KEY, &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  if (rc == LC_OK) {
    registry->found = 1;
    registry->version = read_result.version;
    rc = lc_pouch_tc_read_body_text(client, &read_result, &body, &body_length,
                                    error);
  }
  if (rc == LC_OK) {
    memset(&cursor, 0, sizeof(cursor));
    cursor.bytes = (const unsigned char *)body;
    cursor.length = body_length;
    rc = lc_pouch_binary_cursor_magic(&cursor, LC_POUCH_TC_RM_RECORD_MAGIC,
                                      error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
    registry->updated_at_unix = (lc_pouch_unix_seconds)signed_value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_binary_cursor_u64(&cursor, &count, error);
  }
  for (index = 0U; rc == LC_OK && index < count; ++index) {
    backend_hash = NULL;
    endpoint = NULL;
    endpoint_count = 0U;
    rc = lc_pouch_binary_cursor_string(&cursor, &backend_hash, error);
    if (rc == LC_OK) {
      backend = lc_pouch_tc_rm_registry_backend(registry, backend_hash, error);
      rc = backend != NULL
               ? LC_OK
               : (error != NULL && error->code != LC_OK ? error->code
                                                        : LC_ERR_NOMEM);
    } else {
      backend = NULL;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_i64(&cursor, &signed_value, error);
      backend->updated_at_unix = (lc_pouch_unix_seconds)signed_value;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_binary_cursor_u64(&cursor, &endpoint_count, error);
    }
    for (endpoint_index = 0U; rc == LC_OK && endpoint_index < endpoint_count;
         ++endpoint_index) {
      endpoint = NULL;
      rc = lc_pouch_binary_cursor_string(&cursor, &endpoint, error);
      if (rc == LC_OK) {
        rc = lc_pouch_tc_string_list_append_unique(&backend->endpoints,
                                                   endpoint, error);
      }
      lc_free_with_allocator(NULL, endpoint);
    }
    lc_free_with_allocator(NULL, backend_hash);
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch TC RM registry has trailing bytes", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    lc_pouch_tc_rm_registry_sort(registry);
  }
  lc_client_free(client, body);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_pouch_tc_rm_registry_cleanup(registry);
  }
  return rc;
}

static int
lc_pouch_tc_rm_registry_write(lc_client_handle *client,
                              const lc_pouch_tc_rm_registry *registry,
                              lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_pouch_txn_buffer buffer;
  lc_source *source;
  size_t index;
  size_t endpoint_index;
  int rc;

  if (registry->backend_count == 0U) {
    if (!registry->found) {
      return LC_OK;
    }
    return lc_pouch_tc_delete_key(client, LC_POUCH_CONTROL_NAMESPACE,
                                  LC_POUCH_TC_RM_MEMBERS_KEY, 1,
                                  registry->version, error);
  }
  memset(&buffer, 0, sizeof(buffer));
  rc = lc_pouch_txn_buffer_append_bytes(&buffer, LC_POUCH_TC_RM_RECORD_MAGIC,
                                        strlen(LC_POUCH_TC_RM_RECORD_MAGIC),
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_i64(
        &buffer, (int64_t)registry->updated_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_buffer_append_u64(
        &buffer, (uint64_t)registry->backend_count, error);
  }
  for (index = 0U; rc == LC_OK && index < registry->backend_count; ++index) {
    rc = lc_pouch_txn_buffer_append_string(
        &buffer, registry->backends[index].backend_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_txn_buffer_append_i64(
          &buffer, (int64_t)registry->backends[index].updated_at_unix, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_buffer_append_u64(
          &buffer, (uint64_t)registry->backends[index].endpoints.count, error);
    }
    for (endpoint_index = 0U;
         rc == LC_OK &&
         endpoint_index < registry->backends[index].endpoints.count;
         ++endpoint_index) {
      rc = lc_pouch_txn_buffer_append_string(
          &buffer, registry->backends[index].endpoints.items[endpoint_index],
          error);
    }
  }
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  options.content_type = LC_POUCH_TC_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  options.has_expected_version = 1;
  options.expected_version = registry->found ? registry->version : 0UL;
  options.object_record = 1;
  if (rc == LC_OK) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, &source, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, LC_POUCH_CONTROL_NAMESPACE,
                              LC_POUCH_TC_RM_MEMBERS_KEY, source, &options,
                              &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_txn_buffer_cleanup(&buffer);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int
lc_pouch_tc_rm_registry_copy_backend(const lc_pouch_tc_rm_registry *registry,
                                     const char *backend_hash,
                                     lc_tc_rm_res *out, lc_error *error) {
  size_t index;
  size_t endpoint_index;
  int rc;

  memset(out, 0, sizeof(*out));
  out->backend_hash = lc_strdup_local(backend_hash);
  out->correlation_id = lc_strdup_local("pouch-tc-rm");
  if (out->backend_hash == NULL || out->correlation_id == NULL) {
    lc_tc_rm_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM response", NULL, NULL,
                        NULL);
  }
  out->updated_at_unix = registry->updated_at_unix;
  for (index = 0U; index < registry->backend_count; ++index) {
    if (strcmp(registry->backends[index].backend_hash, backend_hash) != 0) {
      continue;
    }
    out->updated_at_unix = registry->backends[index].updated_at_unix;
    for (endpoint_index = 0U;
         endpoint_index < registry->backends[index].endpoints.count;
         ++endpoint_index) {
      rc = lc_pouch_tc_string_list_append_unique(
          &out->endpoints,
          registry->backends[index].endpoints.items[endpoint_index], error);
      if (rc != LC_OK) {
        lc_tc_rm_res_cleanup(out);
        return rc;
      }
    }
    if (out->endpoints.count > 1U) {
      qsort(out->endpoints.items, out->endpoints.count,
            sizeof(out->endpoints.items[0]), lc_pouch_tc_string_compare);
    }
    return LC_OK;
  }
  return LC_OK;
}

static int lc_pouch_tc_rm_registry_upsert(lc_pouch_tc_rm_registry *registry,
                                          const char *backend_hash,
                                          const char *endpoint,
                                          lc_pouch_unix_seconds now,
                                          int *changed, lc_error *error) {
  lc_tc_rm_backend *backend;
  size_t i;
  size_t before;
  int rc;

  *changed = 0;
  backend = lc_pouch_tc_rm_registry_backend(registry, backend_hash, error);
  if (backend == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  for (i = 0U; i < backend->endpoints.count; ++i) {
    if (strcmp(backend->endpoints.items[i], endpoint) == 0) {
      return LC_OK;
    }
  }
  before = backend->endpoints.count;
  rc = lc_pouch_tc_string_list_append_unique(&backend->endpoints, endpoint,
                                             error);
  if (rc == LC_OK && backend->endpoints.count != before) {
    backend->updated_at_unix = now;
    registry->updated_at_unix = now;
    *changed = 1;
    lc_pouch_tc_rm_registry_sort(registry);
  }
  return rc;
}

static int lc_pouch_tc_rm_registry_remove(lc_pouch_tc_rm_registry *registry,
                                          const char *backend_hash,
                                          const char *endpoint,
                                          lc_pouch_unix_seconds now,
                                          int *changed) {
  size_t backend_index;
  size_t endpoint_index;
  size_t write_index;

  *changed = 0;
  for (backend_index = 0U; backend_index < registry->backend_count;
       ++backend_index) {
    lc_tc_rm_backend *backend;

    backend = &registry->backends[backend_index];
    if (strcmp(backend->backend_hash, backend_hash) != 0) {
      continue;
    }
    write_index = 0U;
    for (endpoint_index = 0U; endpoint_index < backend->endpoints.count;
         ++endpoint_index) {
      if (strcmp(backend->endpoints.items[endpoint_index], endpoint) == 0) {
        lc_free_local(backend->endpoints.items[endpoint_index]);
        *changed = 1;
        continue;
      }
      backend->endpoints.items[write_index++] =
          backend->endpoints.items[endpoint_index];
    }
    backend->endpoints.count = write_index;
    if (!*changed) {
      return LC_OK;
    }
    registry->updated_at_unix = now;
    if (backend->endpoints.count != 0U) {
      backend->updated_at_unix = now;
      lc_pouch_tc_rm_registry_sort(registry);
      return LC_OK;
    }
    lc_free_local(backend->backend_hash);
    lc_free_local(backend->endpoints.items);
    for (; backend_index + 1U < registry->backend_count; ++backend_index) {
      registry->backends[backend_index] =
          registry->backends[backend_index + 1U];
    }
    --registry->backend_count;
    memset(&registry->backends[registry->backend_count], 0,
           sizeof(registry->backends[registry->backend_count]));
    return LC_OK;
  }
  return LC_OK;
}

static int
lc_pouch_tc_rm_registry_copy_list(const lc_pouch_tc_rm_registry *registry,
                                  lc_tc_rm_list_res *out, lc_error *error) {
  size_t index;
  size_t endpoint_index;
  int rc;

  memset(out, 0, sizeof(*out));
  out->updated_at_unix = registry->updated_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-rm");
  if (out->correlation_id == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM list response", NULL,
                        NULL, NULL);
  }
  if (registry->backend_count == 0U) {
    return LC_OK;
  }
  out->backends = (lc_tc_rm_backend *)lc_calloc_local(registry->backend_count,
                                                      sizeof(lc_tc_rm_backend));
  if (out->backends == NULL) {
    lc_tc_rm_list_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM backend list", NULL,
                        NULL, NULL);
  }
  out->backend_count = registry->backend_count;
  for (index = 0U; index < registry->backend_count; ++index) {
    out->backends[index].backend_hash =
        lc_strdup_local(registry->backends[index].backend_hash);
    out->backends[index].updated_at_unix =
        registry->backends[index].updated_at_unix;
    if (out->backends[index].backend_hash == NULL) {
      lc_tc_rm_list_res_cleanup(out);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch TC RM backend", NULL, NULL,
                          NULL);
    }
    for (endpoint_index = 0U;
         endpoint_index < registry->backends[index].endpoints.count;
         ++endpoint_index) {
      rc = lc_pouch_tc_string_list_append_unique(
          &out->backends[index].endpoints,
          registry->backends[index].endpoints.items[endpoint_index], error);
      if (rc != LC_OK) {
        lc_tc_rm_list_res_cleanup(out);
        return rc;
      }
    }
  }
  return LC_OK;
}

int lc_pouch_client_tc_lease_acquire_method(lc_client *self,
                                            const lc_tc_lease_acquire_req *req,
                                            lc_tc_lease_acquire_res *out,
                                            lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  lc_pouch_unix_seconds now;
  lc_pouch_unix_seconds expires_at_unix;
  int grant;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease acquire requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->candidate_id == NULL || req->candidate_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC candidate_id must be non-empty", NULL, NULL,
                        NULL);
  }
  if (req->candidate_endpoint == NULL || req->candidate_endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC candidate_endpoint must be non-empty", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  expires_at_unix = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_expiration_from_ttl_ms(now, req->ttl_ms, &expires_at_unix,
                                            error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  grant = 0;
  if (rc == LC_OK) {
    /* Expiry/release ends this epoch; it never makes an old term admissible.
     * A new owner advances the durable term. Only the live current owner may
     * reacquire its same term (an idempotent renewal). */
    grant = !record.found || req->term > record.term ||
            (req->term == record.term && record.expires_at_unix > now &&
             strcmp(record.leader_id, req->candidate_id) == 0);
    if (grant) {
      rc = lc_pouch_tc_write_lease(
          client, req->candidate_id, req->candidate_endpoint, req->term,
          expires_at_unix, record.found ? record.version : 0UL, error);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_lease_acquire_res(
        out, grant,
        grant || !record.found ? req->candidate_id : record.leader_id,
        grant || !record.found ? req->candidate_endpoint
                               : record.leader_endpoint,
        grant || !record.found ? req->term : record.term,
        grant || !record.found ? expires_at_unix : record.expires_at_unix,
        error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  lc_pouch_unix_seconds now;
  lc_pouch_unix_seconds expires_at_unix;
  int renewed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease renew requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->leader_id == NULL || req->leader_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader_id must be non-empty", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  expires_at_unix = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_expiration_from_ttl_ms(now, req->ttl_ms, &expires_at_unix,
                                            error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  renewed = 0;
  if (rc == LC_OK && record.found && record.expires_at_unix > now &&
      record.term == req->term &&
      strcmp(record.leader_id, req->leader_id) == 0) {
    renewed = 1;
    rc = lc_pouch_tc_write_lease(client, record.leader_id,
                                 record.leader_endpoint, record.term,
                                 expires_at_unix, record.version, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_lease_renew_res(
        out, renewed, record.found ? record.leader_id : req->leader_id,
        record.found ? record.leader_endpoint : "",
        record.found ? record.term : req->term,
        renewed ? expires_at_unix
                : (record.found ? record.expires_at_unix : 0L),
        error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_lease_release_method(lc_client *self,
                                            const lc_tc_lease_release_req *req,
                                            lc_tc_lease_release_res *out,
                                            lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  lc_pouch_unix_seconds now;
  int released;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease release requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->leader_id == NULL || req->leader_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader_id must be non-empty", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  released = 0;
  if (rc == LC_OK && record.found && record.expires_at_unix > now &&
      record.term == req->term &&
      strcmp(record.leader_id, req->leader_id) == 0) {
    released = 1;
    /* Keep the fencing high-water mark across orderly shutdown/reopen. A
     * deleted lease allowed a stale owner to re-enter an already used epoch. */
    rc = lc_pouch_tc_write_lease(client, "", "", record.term, 0L,
                                 record.version, error);
  }
  if (rc == LC_OK) {
    memset(out, 0, sizeof(*out));
    out->released = released;
    out->correlation_id = lc_strdup_local("pouch-tc-lease");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  lc_pouch_unix_seconds now;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader requires self and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  if (rc == LC_OK && (!record.found || record.expires_at_unix <= now)) {
    rc = lc_pouch_tc_copy_leader_res(
        out, "", "", record.found ? record.term : 0UL, 0L, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_leader_res(out, record.leader_id,
                                     record.leader_endpoint, record.term,
                                     record.expires_at_unix, error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC cluster announce requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_tc_write_cluster_self(client, req->self_endpoint, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_cluster_response(client, out, error);
  }
  return rc;
}

int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC cluster leave requires self and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_tc_delete_key(client, LC_POUCH_CONTROL_NAMESPACE,
                              LC_POUCH_TC_CLUSTER_PREFIX "self", 0, 0UL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_cluster_response(client, out, error);
  }
  return rc;
}

int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error) {
  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC cluster list requires self and out", NULL,
                        NULL, NULL);
  }
  return lc_pouch_tc_cluster_response((lc_client_handle *)self, out, error);
}

int lc_pouch_client_tc_rm_register_method(lc_client *self,
                                          const lc_tc_rm_register_req *req,
                                          lc_tc_rm_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_rm_registry registry;
  char *backend_hash;
  char *endpoint;
  lc_pouch_unix_seconds now = 0;
  int changed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM register requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->backend_hash == NULL || req->backend_hash[0] == '\0' ||
      req->endpoint == NULL || req->endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM register requires backend_hash and "
                        "endpoint",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&registry, 0, sizeof(registry));
  backend_hash = lc_pouch_tc_control_string_normalize(
      req->backend_hash, "pouch TC RM backend_hash must be non-empty", error);
  if (backend_hash == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  endpoint = lc_pouch_tc_endpoint_normalize(req->endpoint, error);
  if (endpoint == NULL) {
    lc_free_with_allocator(NULL, backend_hash);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_read(client, &registry, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_upsert(&registry, backend_hash, endpoint, now,
                                        &changed, error);
  }
  if (rc == LC_OK && changed) {
    rc = lc_pouch_tc_rm_registry_write(client, &registry, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_copy_backend(&registry, backend_hash, out,
                                              error);
  }
  lc_free_with_allocator(NULL, backend_hash);
  lc_free_local(endpoint);
  lc_pouch_tc_rm_registry_cleanup(&registry);
  return rc;
}

int lc_pouch_client_tc_rm_unregister_method(lc_client *self,
                                            const lc_tc_rm_unregister_req *req,
                                            lc_tc_rm_res *out,
                                            lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_rm_registry registry;
  char *backend_hash;
  char *endpoint;
  lc_pouch_unix_seconds now = 0;
  int changed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM unregister requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->backend_hash == NULL || req->backend_hash[0] == '\0' ||
      req->endpoint == NULL || req->endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM unregister requires backend_hash and "
                        "endpoint",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&registry, 0, sizeof(registry));
  backend_hash = lc_pouch_tc_control_string_normalize(
      req->backend_hash, "pouch TC RM backend_hash must be non-empty", error);
  if (backend_hash == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  endpoint = lc_pouch_tc_endpoint_normalize(req->endpoint, error);
  if (endpoint == NULL) {
    lc_free_with_allocator(NULL, backend_hash);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_read(client, &registry, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_remove(&registry, backend_hash, endpoint, now,
                                        &changed);
  }
  if (rc == LC_OK && changed) {
    rc = lc_pouch_tc_rm_registry_write(client, &registry, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_copy_backend(&registry, backend_hash, out,
                                              error);
  }
  lc_free_with_allocator(NULL, backend_hash);
  lc_free_local(endpoint);
  lc_pouch_tc_rm_registry_cleanup(&registry);
  return rc;
}

int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_rm_registry registry;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM list requires self and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&registry, 0, sizeof(registry));
  rc = lc_pouch_tc_rm_registry_read(client, &registry, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_registry_copy_list(&registry, out, error);
  }
  lc_pouch_tc_rm_registry_cleanup(&registry);
  return rc;
}

int lc_pouch_message_ack_method(lc_message *self, lc_error *error) {
  lc_message_handle *message;
  lc_ack_op op;
  lc_ack_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message ack requires self", NULL, NULL, NULL);
  }
  message = (lc_message_handle *)self;
  memset(&op, 0, sizeof(op));
  memset(&res, 0, sizeof(res));
  op.message.ns = message->ns;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_etag = message->state_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  rc =
      lc_pouch_client_queue_ack_method(&message->client->pub, &op, &res, error);
  lc_ack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    if (!message->batch_owned) {
      lc_message_close_method(self);
    }
  }
  return rc;
}

int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *req,
                                 lc_error *error) {
  lc_message_handle *message;
  lc_nack_op op;
  lc_nack_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message nack requires self and req", NULL, NULL,
                        NULL);
  }
  message = (lc_message_handle *)self;
  lc_nack_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.message.ns = message->ns;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_etag = message->state_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  op.delay_seconds = req->delay_seconds;
  op.intent = req->intent;
  op.last_error_json = req->last_error_json;
  rc = lc_pouch_client_queue_nack_method(&message->client->pub, &op, &res,
                                         error);
  if (rc == LC_OK && res.meta_etag != NULL) {
    char *copy;

    copy = lc_client_strdup(message->client, res.meta_etag);
    if (copy == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch message nack etag", NULL,
                        NULL, NULL);
    } else {
      lc_client_free(message->client, message->meta_etag);
      message->meta_etag = copy;
      message->pub.meta_etag = message->meta_etag;
    }
  }
  lc_nack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    if (!message->batch_owned) {
      lc_message_close_method(self);
    }
  }
  return rc;
}

int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *req,
                                   lc_error *error) {
  lc_message_handle *message;
  lc_extend_op op;
  lc_extend_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message extend requires self and req", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  lc_extend_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.message.ns = message->ns;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  op.extend_by_seconds = req->extend_by_seconds;
  rc = lc_pouch_client_queue_extend_method(&message->client->pub, &op, &res,
                                           error);
  if (rc == LC_OK) {
    char *copy;

    copy = lc_client_strdup(message->client, res.meta_etag);
    if (copy == NULL && res.meta_etag != NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch message extend etag", NULL,
                        NULL, NULL);
    } else {
      lc_client_free(message->client, message->meta_etag);
      message->meta_etag = copy;
      message->lease_expires_at_unix = res.lease_expires_at_unix;
      message->visibility_timeout_seconds = res.visibility_timeout_seconds;
      message->state_lease_expires_at_unix = res.state_lease_expires_at_unix;
      message->pub.meta_etag = message->meta_etag;
      message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
      message->pub.visibility_timeout_seconds =
          message->visibility_timeout_seconds;
      if (message->state_lease != NULL) {
        lc_pouch_lease_refresh_expiration(
            (lc_lease_handle *)message->state_lease,
            message->state_lease_expires_at_unix);
      }
    }
  }
  lc_extend_res_cleanup(&res);
  return rc;
}

static int lc_pouch_lease_validate_private_read(lc_lease_handle *lease,
                                                lc_error *error) {
  lc_lease_ref ref;

  if (lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch private lease read requires lease", NULL, NULL,
                        NULL);
  }
  memset(&ref, 0, sizeof(ref));
  ref.ns = lease->ns;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  return lc_pouch_validate_lease_record(lease->client, &ref, lease->ns,
                                        lease->key, NULL, error);
}

static const char *lc_pouch_lease_state_storage_key(lc_lease_handle *lease) {
  return lease != NULL && lease->pouch_state_key != NULL
             ? lease->pouch_state_key
         : lease != NULL ? lease->key
                         : NULL;
}

static int lc_pouch_lease_stages_transaction(const lc_lease_handle *lease) {
  return lease != NULL && lc_pouch_txn_id_present(lease->txn_id) &&
         lease->pouch_commit_immediately == LC_POUCH_COMMIT_STAGED;
}

static int lc_pouch_lease_read_private_state_metadata(
    lc_lease_handle *lease, lc_pouch_state_read_result *out, lc_error *error) {
  const char *state_key;
  char *staged_key;
  int rc;

  if (lease == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch private lease state requires context", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  state_key = lc_pouch_lease_state_storage_key(lease);
  staged_key = NULL;
  if (lc_pouch_lease_stages_transaction(lease)) {
    staged_key = lc_pouch_staged_storage_key(state_key, lease->txn_id, error);
    if (staged_key == NULL) {
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_state_read_metadata(lease->client->pouch, lease->ns,
                                      staged_key, out, error);
    lc_free_with_allocator(NULL, staged_key);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_state_result_is_delete_marker(out)) {
      lc_pouch_state_read_result_cleanup(&lease->client->allocator, out);
      memset(out, 0, sizeof(*out));
      return LC_OK;
    }
    if (out->found) {
      return LC_OK;
    }
    lc_pouch_state_read_result_cleanup(&lease->client->allocator, out);
    memset(out, 0, sizeof(*out));
  }
  return lc_pouch_state_read_metadata(lease->client->pouch, lease->ns,
                                      state_key, out, error);
}

static int lc_pouch_lease_read_private_state(lc_lease_handle *lease,
                                             lc_pouch_state_read_result *out,
                                             lc_error *error) {
  const char *state_key;
  char *staged_key;
  int rc;

  if (lease == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch private lease state requires context", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  state_key = lc_pouch_lease_state_storage_key(lease);
  staged_key = NULL;
  if (lc_pouch_lease_stages_transaction(lease)) {
    staged_key = lc_pouch_staged_storage_key(state_key, lease->txn_id, error);
    if (staged_key == NULL) {
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_state_read(lease->client->pouch, lease->ns, staged_key, out,
                             error);
    lc_free_with_allocator(NULL, staged_key);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_state_result_is_delete_marker(out)) {
      lc_pouch_state_read_result_cleanup(&lease->client->allocator, out);
      memset(out, 0, sizeof(*out));
      return LC_OK;
    }
    if (out->found) {
      return LC_OK;
    }
    lc_pouch_state_read_result_cleanup(&lease->client->allocator, out);
    memset(out, 0, sizeof(*out));
  }
  return lc_pouch_state_read(lease->client->pouch, lease->ns, state_key, out,
                             error);
}

int lc_pouch_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_read_result read_result;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease describe requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_lease_validate_private_read(lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_lease_read_private_state_metadata(lease, &read_result, error);
  if (rc == LC_OK) {
    lc_version version;

    version = 0;
    if (read_result.found) {
      rc = lc_pouch_generation_to_version(read_result.version, &version, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_lease_refresh_state(
          lease, read_result.found ? read_result.etag : NULL, version, error);
      if (rc == LC_OK)
        lc_pouch_lease_refresh_retention(lease, read_result.has_body,
                                         read_result.retention);
    }
  }
  lc_pouch_state_read_result_cleanup(&lease->client->allocator, &read_result);
  return rc;
}

int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_read_result read_result;
  int rc;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get requires self, dst, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&read_result, 0, sizeof(read_result));
  if (opts == NULL || !opts->public_read) {
    rc = lc_pouch_lease_validate_private_read(lease, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_txn_id_present(lease->txn_id)) {
      memset(out, 0, sizeof(*out));
      rc = lc_pouch_lease_read_private_state(lease, &read_result, error);
      if (rc == LC_OK && !read_result.found) {
        out->no_content = 1;
        out->retention_support = LC_STATE_RETENTION_SUPPORTED;
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_copy(read_result.body, dst, NULL, error);
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
      }
      lc_pouch_state_read_result_cleanup(&lease->client->allocator,
                                         &read_result);
      if (rc == LC_OK && !out->no_content) {
        rc =
            lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
      }
      if (rc == LC_OK && (opts == NULL || !opts->public_read))
        lc_pouch_lease_refresh_retention(lease,
                                         out->has_retention_expires_at_unix,
                                         out->retention_expires_at_unix);
      return rc;
    }
  }
  rc = lc_pouch_client_get_namespace(lease->client, lease->ns,
                                     lc_pouch_lease_state_storage_key(lease),
                                     opts, dst, out, error);
  if (rc == LC_OK && !out->no_content && (opts == NULL || !opts->public_read)) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  if (rc == LC_OK && (opts == NULL || !opts->public_read))
    lc_pouch_lease_refresh_retention(lease, out->has_retention_expires_at_unix,
                                     out->retention_expires_at_unix);
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_read_result read_result;
  lc_sink *memory_sink;
  const void *bytes;
  size_t length;
  char *json;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease load requires self, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&read_result, 0, sizeof(read_result));
  memory_sink = NULL;
  bytes = NULL;
  length = 0U;
  json = NULL;
  if (opts == NULL || !opts->public_read) {
    rc = lc_pouch_lease_validate_private_read(lease, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_txn_id_present(lease->txn_id)) {
      memset(out, 0, sizeof(*out));
      rc = lc_pouch_lease_read_private_state(lease, &read_result, error);
      if (rc == LC_OK && !read_result.found) {
        out->no_content = 1;
        out->retention_support = LC_STATE_RETENTION_SUPPORTED;
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_sink_to_memory(&memory_sink, error);
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_copy(read_result.body, memory_sink, NULL, error);
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_sink_memory_bytes(memory_sink, &bytes, &length, error);
      }
      if (rc == LC_OK && read_result.found) {
        json = (char *)lc_alloc_with_allocator(NULL, length + 1U);
        if (json == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch mapped lease load", NULL,
                            NULL, NULL);
        }
      }
      if (rc == LC_OK && read_result.found) {
        memcpy(json, bytes, length);
        json[length] = '\0';
        runtime = lc_thread_lonejson_runtime();
        rc = lc_lonejson_prepare_parse_destination(runtime, map, dst, error);
        memset(&lj_error, 0, sizeof(lj_error));
        status = rc == LC_OK ? lc_lonejson_parse_cstr_value(runtime, map, dst,
                                                            json, &lj_error)
                             : LONEJSON_STATUS_OK;
        if (status != LONEJSON_STATUS_OK) {
          rc = lc_lonejson_error_from_status(
              error, status, &lj_error, "failed to parse pouch lease state");
        }
      }
      if (rc == LC_OK && read_result.found) {
        rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
      }
      if (memory_sink != NULL) {
        memory_sink->close(memory_sink);
      }
      lc_free_with_allocator(NULL, json);
      lc_pouch_state_read_result_cleanup(&lease->client->allocator,
                                         &read_result);
      if (rc == LC_OK && !out->no_content) {
        rc =
            lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
      }
      if (rc == LC_OK && (opts == NULL || !opts->public_read))
        lc_pouch_lease_refresh_retention(lease,
                                         out->has_retention_expires_at_unix,
                                         out->retention_expires_at_unix);
      return rc;
    }
  }
  rc = lc_pouch_client_load_namespace(lease->client, lease->ns,
                                      lc_pouch_lease_state_storage_key(lease),
                                      map, dst, opts, out, error);
  if (rc == LC_OK && !out->no_content && (opts == NULL || !opts->public_read)) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  if (rc == LC_OK && (opts == NULL || !opts->public_read))
    lc_pouch_lease_refresh_retention(lease, out->has_retention_expires_at_unix,
                                     out->retention_expires_at_unix);
  return rc;
}

static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src,
                                      const lc_update_opts *opts,
                                      lc_error *error) {
  lc_source *source;
  lc_update_opts update;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease save requires self, map, and source", NULL,
                        NULL, NULL);
  }
  source = NULL;
  rc = lc_pouch_lonejson_source_open(map, src, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&update, 0, sizeof(update));
  if (opts != NULL)
    update = *opts;
  if (update.content_type == NULL)
    update.content_type = "application/json";
  rc = lc_pouch_lease_update_method(self, source, &update, error);
  lc_source_close(source);
  return rc;
}

static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_lease_ref lease_ref;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *stage_txn_id;
  char *etag_copy;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update requires self and src", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update used outside staging scope",
                        NULL, NULL, NULL);
  }
  stage_txn_id = lease->txn_id != NULL && lease->txn_id[0] != '\0'
                     ? lease->txn_id
                     : lease->lease_id;
  lc_lease_ref_init(&lease_ref);
  lease_ref.ns = lease->ns;
  lease_ref.key = lease->key;
  lease_ref.lease_id = lease->lease_id;
  lease_ref.txn_id = lease->txn_id;
  lease_ref.fencing_token = lease->fencing_token;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.content_type = "application/json";
  rc = lc_pouch_retention_options(
      &options, opts != NULL ? opts->has_retention_seconds : 0,
      opts != NULL ? opts->retention_seconds : 0, lease->ns,
      lc_pouch_lease_state_storage_key(lease), error);
  if (rc != LC_OK)
    return rc;
  options.has_query_hidden = lease->has_query_hidden;
  options.query_hidden = lease->query_hidden;
  /* Do not propagate automatic placeholder hiding into a staged first body. */
  if (lease->pouch_state_key == NULL && lease->version == 0L &&
      lease->has_query_hidden && lease->query_hidden) {
    options.has_query_hidden = 0;
    options.query_hidden = 0;
  }
  if (lease->pouch_state_key != NULL ||
      lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY) {
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
  }
  if (opts != NULL) {
    options.content_type =
        opts->content_type != NULL ? opts->content_type : options.content_type;
    options.expected_etag = opts->if_state_etag;
    if (opts->has_if_version) {
      rc = lc_pouch_version_to_generation(opts->if_version,
                                          &options.expected_version, error);
      if (rc != LC_OK) {
        return rc;
      }
      options.has_expected_version = 1;
    }
  } else if (lease->pouch_stage_dirty) {
    options.expected_etag = lease->pouch_stage_etag;
    rc = lc_pouch_version_to_generation(lease->pouch_stage_version,
                                        &options.expected_version, error);
    if (rc != LC_OK) {
      return rc;
    }
    options.has_expected_version = 1;
  }
  lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
  rc = lc_pouch_client_stage_transaction_write(
      lease->client, &lease_ref, lease->ns,
      lc_pouch_lease_state_storage_key(lease), stage_txn_id, src, &options,
      &result, NULL, error);
  if (rc == LC_OK) {
    etag_copy = lc_client_strdup(lease->client, result.etag);
    if (etag_copy == NULL) {
      lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch staged state etag", NULL,
                          NULL, NULL);
    }
    lc_client_free(lease->client, lease->pouch_stage_etag);
    lease->pouch_stage_etag = etag_copy;
    rc = lc_pouch_generation_to_version(result.version,
                                        &lease->pouch_stage_version, error);
    if (rc == LC_OK) {
      lease->pouch_stage_dirty = 1;
      rc = lc_pouch_lease_refresh_state(lease, result.etag,
                                        lease->pouch_stage_version, error);
      if (rc == LC_OK) {
        lc_pouch_lease_refresh_query_metadata(lease, result.has_query_hidden,
                                              result.query_hidden);
        lc_pouch_lease_refresh_retention(lease, result.has_body,
                                         result.retention);
      }
    }
  }
  lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
  return rc;
}

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts, lc_error *error) {
  lc_lease_handle *lease;
  lc_lease_ref lease_ref;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_update_req req;
  lc_update_res res;
  const char *state_key;
  const char *stage_txn_id;
  int first_body_from_hidden_placeholder;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease update requires self and src", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  /* Align the public lease view with a direct first-body write. */
  first_body_from_hidden_placeholder =
      lease->version == 0L && lease->has_query_hidden && lease->query_hidden;
  if (lease->pouch_state_key != NULL ||
      lease->pouch_commit_immediately != LC_POUCH_COMMIT_STAGED) {
    memset(&lease_precondition, 0, sizeof(lease_precondition));
    memset(&options, 0, sizeof(options));
    memset(&write_result, 0, sizeof(write_result));
    state_key = lc_pouch_lease_state_storage_key(lease);
    rc = lc_pouch_retention_options(
        &options, opts != NULL ? opts->has_retention_seconds : 0,
        opts != NULL ? opts->retention_seconds : 0, lease->ns, state_key,
        error);
    if (rc != LC_OK)
      return rc;
    /* The write precondition validates this lease from the resident record
     * while the exact key is owned. Do not add a stale pre-read before it. */
    options.content_type = opts != NULL && opts->content_type != NULL
                               ? opts->content_type
                               : "application/json";
    if (lease->pouch_state_key != NULL ||
        lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY) {
      options.has_query_hidden = 1;
      options.query_hidden = 1;
      options.object_record = 1;
    }
    if (opts != NULL) {
      options.expected_etag = opts->if_state_etag;
      if (opts->has_if_version) {
        rc = lc_pouch_version_to_generation(opts->if_version,
                                            &options.expected_version, error);
        if (rc != LC_OK) {
          return rc;
        }
        options.has_expected_version = 1;
      }
    }
    if (!options.has_expected_version && lease->version > 0L) {
      rc = lc_pouch_version_to_generation(lease->version,
                                          &options.expected_version, error);
      if (rc != LC_OK) {
        return rc;
      }
      options.has_expected_version = 1;
    }
    lc_lease_ref_init(&lease_ref);
    lease_ref.ns = lease->ns;
    lease_ref.key = lease->key;
    lease_ref.lease_id = lease->lease_id;
    lease_ref.txn_id = lease->txn_id;
    lease_ref.fencing_token = lease->fencing_token;
    if (lc_pouch_lease_stages_transaction(lease)) {
      stage_txn_id = lease->txn_id;
      lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
      rc = lc_pouch_client_stage_transaction_write(
          lease->client, &lease_ref, lease->ns, state_key, stage_txn_id, src,
          &options, &write_result, NULL, error);
    } else {
      lease_precondition.client = lease->client;
      lease_precondition.lease = &lease_ref;
      lease_precondition.ns = lease->ns;
      lease_precondition.key = lease->key;
      options.precondition = lc_pouch_lease_precondition_check;
      options.precondition_context = &lease_precondition;
      lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
      rc = lc_pouch_state_write(lease->client->pouch, lease->ns, state_key, src,
                                &options, &write_result, error);
    }
    if (rc == LC_OK) {
      lc_version version = 0L;

      rc =
          lc_pouch_generation_to_version(write_result.version, &version, error);
      if (rc == LC_OK) {
        rc = lc_pouch_lease_refresh_state(lease, write_result.etag, version,
                                          error);
        if (rc == LC_OK)
          lc_pouch_lease_refresh_retention(lease, write_result.has_body,
                                           write_result.retention);
      }
    }
    lc_pouch_state_write_result_cleanup(&lease->client->allocator,
                                        &write_result);
    return rc;
  }
  lc_update_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.lease.ns = lease->ns;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  if (opts != NULL) {
    req.if_state_etag = opts->if_state_etag;
    req.if_version = opts->if_version;
    req.has_if_version = opts->has_if_version;
    req.content_type = opts->content_type;
    req.has_retention_seconds = opts->has_retention_seconds;
    req.retention_seconds = opts->retention_seconds;
  }
  if (!req.has_if_version && lease->version > 0L) {
    req.if_version = lease->version;
    req.has_if_version = 1;
  }
  if (req.content_type == NULL) {
    req.content_type = "application/json";
  }
  lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
  rc = lc_pouch_client_update_method(&lease->client->pub, &req, src, &res,
                                     error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(lease, res.new_state_etag,
                                      res.new_version, error);
    if (rc == LC_OK)
      lc_pouch_lease_refresh_retention(lease, res.has_retention_expires_at_unix,
                                       res.retention_expires_at_unix);
    if (rc == LC_OK && first_body_from_hidden_placeholder) {
      lc_pouch_lease_refresh_query_metadata(lease, 0, 0);
    }
  }
  lc_update_res_cleanup(&res);
  return rc;
}

static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_mutate_file mutated;
  lc_update_opts opts;
  lc_source *source;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate requires self and req", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&mutated, 0, sizeof(mutated));
  lc_update_opts_init(&opts);
  source = NULL;

  if (lc_pouch_lease_stages_transaction(lease)) {
    rc = lc_pouch_client_prepare_txn_mutation_file(
        lease->client, lease->ns, lc_pouch_lease_state_storage_key(lease),
        lease->txn_id, req->mutations, req->mutation_count, NULL, &mutated,
        error);
  } else {
    rc = lc_pouch_prepare_mutation_file(
        lease->client, lease->ns, lc_pouch_lease_state_storage_key(lease),
        req->mutations, req->mutation_count, NULL, &mutated, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  source = lc_source_from_open_file(mutated.fp, 0);
  if (source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch lease mutate result source", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  opts.content_type = "application/json";
  opts.if_state_etag = req->if_state_etag;
  opts.if_version = req->if_version;
  opts.has_if_version = req->has_if_version;
  opts.has_retention_seconds = req->has_retention_seconds;
  opts.retention_seconds = req->retention_seconds;
  if (!opts.has_if_version && lease->version > 0L) {
    opts.if_version = lease->version;
    opts.has_if_version = 1;
  }
  rc = self->update(self, source, &opts, error);

cleanup:
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_mutate_file_cleanup(&mutated);
  return rc;
}

static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  lc_lease_handle *lease;
  lc_mutation_parse_options parse_options;
  lc_pouch_mutate_file mutated;
  lc_update_opts opts;
  lc_source *source;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate_local requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&parse_options, 0, sizeof(parse_options));
  memset(&mutated, 0, sizeof(mutated));
  lc_update_opts_init(&opts);
  source = NULL;

  parse_options.file_value_base_dir = req->file_value_base_dir;
  parse_options.file_value_resolver = req->file_value_resolver;
  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }
  if (lc_pouch_lease_stages_transaction(lease)) {
    rc = lc_pouch_client_prepare_txn_mutation_file(
        lease->client, lease->ns, lc_pouch_lease_state_storage_key(lease),
        lease->txn_id, req->mutations, req->mutation_count, &parse_options,
        &mutated, error);
  } else {
    rc = lc_pouch_prepare_mutation_file(
        lease->client, lease->ns, lc_pouch_lease_state_storage_key(lease),
        req->mutations, req->mutation_count, &parse_options, &mutated, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  source = lc_source_from_open_file(mutated.fp, 0);
  if (source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch lease local mutate result source",
                      NULL, NULL, NULL);
    goto cleanup;
  }
  opts = req->update;
  if (opts.content_type == NULL || opts.content_type[0] == '\0') {
    opts.content_type = "application/json";
  }
  if (!req->disable_fetched_cas) {
    if ((opts.if_state_etag == NULL || opts.if_state_etag[0] == '\0') &&
        mutated.etag != NULL && mutated.etag[0] != '\0') {
      opts.if_state_etag = mutated.etag;
    }
    if (!opts.has_if_version && mutated.found) {
      rc = lc_pouch_generation_to_version(mutated.version, &opts.if_version,
                                          error);
      if (rc != LC_OK) {
        goto cleanup;
      }
      opts.has_if_version = 1;
    }
  }
  rc = self->update(self, source, &opts, error);

cleanup:
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_mutate_file_cleanup(&mutated);
  return rc;
}

int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_metadata_op op;
  lc_metadata_res res;
  lc_lease_ref lease_ref;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease metadata requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_state_key != NULL ||
      lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY) {
    if (!req->has_query_hidden && !req->has_retention_seconds) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch lease metadata requires query_hidden or retention_seconds",
          NULL, NULL, NULL);
    }
    memset(&lease_precondition, 0, sizeof(lease_precondition));
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.has_query_hidden = req->has_query_hidden;
    options.query_hidden = req->query_hidden;
    options.object_record = 1;
    rc = lc_pouch_retention_options(
        &options, req->has_retention_seconds, req->retention_seconds, lease->ns,
        lc_pouch_lease_state_storage_key(lease), error);
    if (rc != LC_OK)
      return rc;
    if (req->has_if_version) {
      rc = lc_pouch_version_to_generation(req->if_version,
                                          &options.expected_version, error);
      if (rc != LC_OK) {
        return rc;
      }
      options.has_expected_version = 1;
    } else if (lease->version > 0L) {
      rc = lc_pouch_version_to_generation(lease->version,
                                          &options.expected_version, error);
      if (rc != LC_OK) {
        return rc;
      }
      options.has_expected_version = 1;
    }
    /* Metadata mutation performs the same lease validation under its exact
     * key authority; a pre-read would only duplicate resident lookup work. */
    lc_lease_ref_init(&lease_ref);
    lease_ref.ns = lease->ns;
    lease_ref.key = lease->key;
    lease_ref.lease_id = lease->lease_id;
    lease_ref.txn_id = lease->txn_id;
    lease_ref.fencing_token = lease->fencing_token;
    lease_precondition.client = lease->client;
    lease_precondition.lease = &lease_ref;
    lease_precondition.ns = lease->ns;
    lease_precondition.key = lease->key;
    options.precondition = lc_pouch_lease_precondition_check;
    options.precondition_context = &lease_precondition;
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_pouch_state_update_metadata(lease->client->pouch, lease->ns,
                                        lc_pouch_lease_state_storage_key(lease),
                                        &options, &result, error);
    if (rc == LC_OK) {
      rc = lc_pouch_generation_to_version(result.version, &lease->version,
                                          error);
      if (rc == LC_OK) {
        lease->pub.version = lease->version;
        lc_pouch_lease_refresh_query_metadata(lease, result.has_query_hidden,
                                              result.query_hidden);
        lc_pouch_lease_refresh_retention(lease, result.has_body,
                                         result.retention);
      }
    }
    lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
    return rc;
  }
  lc_metadata_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.has_query_hidden = req->has_query_hidden;
  op.query_hidden = req->query_hidden;
  op.if_version = req->if_version;
  op.has_if_version = req->has_if_version;
  op.has_retention_seconds = req->has_retention_seconds;
  op.retention_seconds = req->retention_seconds;
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
  rc = lc_pouch_client_metadata_method(&lease->client->pub, &op, &res, error);
  if (rc == LC_OK) {
    lease->version = res.version;
    lease->pub.version = lease->version;
    lc_pouch_lease_refresh_query_metadata(lease, res.has_query_hidden,
                                          res.query_hidden);
    lc_pouch_lease_refresh_retention(lease, res.has_retention_expires_at_unix,
                                     res.retention_expires_at_unix);
  }
  lc_metadata_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_lease_precondition lease_precondition;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_remove_op op;
  lc_remove_res res;
  lc_lease_ref lease_ref;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease remove requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_state_key != NULL ||
      lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY) {
    memset(&lease_precondition, 0, sizeof(lease_precondition));
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.has_query_hidden = 1;
    options.query_hidden = 1;
    options.object_record = 1;
    if (req != NULL) {
      options.expected_etag = req->if_state_etag;
      if (req->has_if_version) {
        rc = lc_pouch_version_to_generation(req->if_version,
                                            &options.expected_version, error);
        if (rc != LC_OK) {
          return rc;
        }
        options.has_expected_version = 1;
      }
    }
    if (!options.has_expected_version && lease->version > 0L) {
      rc = lc_pouch_version_to_generation(lease->version,
                                          &options.expected_version, error);
      if (rc != LC_OK) {
        return rc;
      }
      options.has_expected_version = 1;
    }
    /* Delete validates the lease through this in-lock precondition. */
    lc_lease_ref_init(&lease_ref);
    lease_ref.ns = lease->ns;
    lease_ref.key = lease->key;
    lease_ref.lease_id = lease->lease_id;
    lease_ref.txn_id = lease->txn_id;
    lease_ref.fencing_token = lease->fencing_token;
    lease_precondition.client = lease->client;
    lease_precondition.lease = &lease_ref;
    lease_precondition.ns = lease->ns;
    lease_precondition.key = lease->key;
    options.precondition = lc_pouch_lease_precondition_check;
    options.precondition_context = &lease_precondition;
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_pouch_state_delete(lease->client->pouch, lease->ns,
                               lc_pouch_lease_state_storage_key(lease),
                               &options, &result, error);
    if (rc == LC_OK && result.version > 0UL) {
      rc = lc_pouch_lease_refresh_state(lease, NULL, 0L, error);
      if (rc == LC_OK && lease->pouch_commit_immediately ==
                             LC_POUCH_COMMIT_STATE_IMMEDIATELY) {
        lease->pouch_state_removed = 1;
      }
    }
    if (rc == LC_OK)
      lc_pouch_lease_refresh_retention(lease, 0, 0);
    lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
    return rc;
  }
  lc_remove_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  if (req != NULL) {
    op.if_state_etag = req->if_state_etag;
    op.if_version = req->if_version;
    op.has_if_version = req->has_if_version;
  }
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
  rc = lc_pouch_client_remove_method(&lease->client->pub, &op, &res, error);
  if (rc == LC_OK && res.removed) {
    rc = lc_pouch_lease_refresh_state(lease, NULL, 0L, error);
  }
  if (rc == LC_OK)
    lc_pouch_lease_refresh_retention(lease, 0, 0);
  lc_remove_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                                    lc_error *error) {
  lc_lease_handle *lease;
  lc_keepalive_op op;
  lc_keepalive_res res;
  lc_pouch_state_read_result read_result;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease keepalive requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_keepalive_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.ttl_seconds = req->ttl_seconds;
  rc = lc_pouch_client_keepalive_method(&lease->client->pub, &op, &res, error);
  if (rc == LC_OK &&
      (lease->pouch_state_key != NULL ||
       lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY)) {
    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read_metadata(lease->client->pouch, lease->ns,
                                      lc_pouch_lease_state_storage_key(lease),
                                      &read_result, error);
    if (rc == LC_OK) {
      lc_free_local(res.state_etag);
      res.state_etag =
          read_result.etag != NULL ? lc_strdup_local(read_result.etag) : NULL;
      if (read_result.etag != NULL && res.state_etag == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch keepalive state etag", NULL,
                          NULL, NULL);
      } else if (read_result.found) {
        rc = lc_pouch_generation_to_version(read_result.version, &res.version,
                                            error);
      }
    }
    lc_pouch_state_read_result_cleanup(&lease->client->allocator, &read_result);
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_lease_refresh_state(lease, res.state_etag, res.version, error);
  }
  if (rc == LC_OK) {
    lc_pouch_lease_refresh_expiration(lease, res.lease_expires_at_unix);
  }
  lc_keepalive_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *req,
                                  lc_error *error) {
  lc_lease_handle *lease;
  lc_release_op op;
  lc_release_res res;
  lc_lease_ref ref;
  lc_pouch_lease_record lease_record;
  lc_pouch_state_write_result write_result;
  lc_error read_error;
  lc_error replace_error;
  int discarded;
  int read_rc;
  int rc;
  int use_read_error;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease release requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_commit_immediately != LC_POUCH_COMMIT_STAGED) {
    /* An immediate stateful checkpoint or parent terminal decision has an
     * acquire transaction id solely for lease ownership. Its mutation is
     * already durable, so applying the generic staged participant here could
     * discard it. Clear only the verified lease record. */
    if (lease->pouch_commit_immediately == LC_POUCH_COMMIT_STATE_IMMEDIATELY &&
        lease->pouch_state_removed) {
      lc_pouch_indexer_note_operation_complete(lease->client->pouch, lease->ns,
                                               lease->key, lease->lease_id);
      self->close(self);
      return LC_OK;
    }
    memset(&write_result, 0, sizeof(write_result));
    memset(&lease_record, 0, sizeof(lease_record));
    lc_error_init(&read_error);
    lc_error_init(&replace_error);
    use_read_error = 0;
    lc_lease_ref_init(&ref);
    ref.ns = lease->ns;
    ref.key = lease->key;
    ref.lease_id = lease->lease_id;
    ref.txn_id = lease->txn_id;
    ref.fencing_token = lease->fencing_token;
    rc = lc_pouch_replace_lease_record(lease->client, &ref, lease->ns,
                                       lease->key, 0L, 1, 0, 0, 0UL,
                                       &write_result, &replace_error);
    lc_pouch_state_write_result_cleanup(&lease->client->allocator,
                                        &write_result);
    if (rc == LC_ERR_INVALID) {
      read_rc = lc_pouch_read_lease_record(lease->client, lease->ns, lease->key,
                                           &lease_record, &read_error);
      if (read_rc == LC_OK && !lease_record.found) {
        rc = LC_OK;
      } else if (read_rc != LC_OK) {
        rc = read_rc;
        use_read_error = 1;
      }
    }
    lc_pouch_lease_record_cleanup(&lease_record);
    if (rc != LC_OK && error != NULL) {
      lc_error_cleanup(error);
      if (use_read_error) {
        *error = read_error;
        lc_error_init(&read_error);
      } else {
        *error = replace_error;
        lc_error_init(&replace_error);
      }
    }
    lc_error_cleanup(&replace_error);
    lc_error_cleanup(&read_error);
    if (rc == LC_OK) {
      lc_pouch_indexer_note_operation_complete(lease->client->pouch, lease->ns,
                                               lease->key, lease->lease_id);
      self->close(self);
    }
    return rc;
  }
  if (lease->pouch_state_key != NULL) {
    memset(&write_result, 0, sizeof(write_result));
    lc_lease_ref_init(&ref);
    ref.ns = lease->ns;
    ref.key = lease->key;
    ref.lease_id = lease->lease_id;
    ref.txn_id = lease->txn_id;
    ref.fencing_token = lease->fencing_token;
    if (req != NULL && req->rollback &&
        lc_pouch_lease_stages_transaction(lease)) {
      rc = lc_pouch_validate_lease_record(lease->client, &ref, lease->ns,
                                          lease->key, NULL, error);
      if (rc != LC_OK) {
        return rc;
      }
      discarded = 0;
      rc =
          lc_pouch_state_discard_staged(lease->client->pouch, lease->ns,
                                        lc_pouch_lease_state_storage_key(lease),
                                        lease->txn_id, &discarded, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    rc = lc_pouch_replace_lease_record(lease->client, &ref, lease->ns,
                                       lease->key, 0L, 1, 0, 0, 0UL,
                                       &write_result, error);
    lc_pouch_state_write_result_cleanup(&lease->client->allocator,
                                        &write_result);
    if (rc == LC_OK) {
      lc_pouch_indexer_note_operation_complete(
          lease->client->pouch, lease->ns, lease->key,
          lc_pouch_txn_id_present(lease->txn_id) ? lease->txn_id
                                                 : lease->lease_id);
      self->close(self);
    }
    return rc;
  }
  lc_release_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.rollback = req != NULL ? req->rollback : 0;
  rc = lc_pouch_client_release_method(&lease->client->pub, &op, &res, error);
  lc_release_res_cleanup(&res);
  if (rc != LC_OK) {
    return rc;
  }
  self->close(self);
  return LC_OK;
}

int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attach_op op;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease attach requires self, req, src, and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attach_op_init(&op);
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.name = req->name;
  op.content_type = req->content_type;
  op.max_bytes = req->max_bytes;
  op.has_max_bytes = req->has_max_bytes;
  op.prevent_overwrite = req->prevent_overwrite;
  return lc_pouch_client_attach_with_transaction_mode(
      &lease->client->pub, &op, src, out,
      lc_pouch_lease_stages_transaction(lease), error);
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_list_req req;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease list_attachments requires self and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_list_req_init(&req);
  req.lease.ns = lease->ns;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  return lc_pouch_client_list_attachments_with_transaction_mode(
      &lease->client->pub, &req, out, lc_pouch_lease_stages_transaction(lease),
      error);
}

int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_get_op op;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get_attachment requires self, req, dst, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_get_op_init(&op);
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.selector = req->selector;
  op.public_read = req->public_read;
  return lc_pouch_client_get_attachment_with_transaction_mode(
      &lease->client->pub, &op, dst, out,
      lc_pouch_lease_stages_transaction(lease), error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_op op;

  if (self == NULL || selector == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease delete_attachment requires self, "
                        "selector, and deleted",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_delete_op_init(&op);
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.selector = *selector;
  return lc_pouch_client_delete_attachment_with_transaction_mode(
      &lease->client->pub, &op, deleted,
      lc_pouch_lease_stages_transaction(lease), error);
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_all_op op;

  if (self == NULL || deleted_count == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch lease delete_all_attachments requires self and deleted_count",
        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_delete_all_op_init(&op);
  op.lease.ns = lease->ns;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  return lc_pouch_client_delete_all_attachments_with_transaction_mode(
      &lease->client->pub, &op, deleted_count,
      lc_pouch_lease_stages_transaction(lease), error);
}
