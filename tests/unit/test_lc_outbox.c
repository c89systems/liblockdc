#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>
#include <openssl/evp.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_outbox_store.h"

#include "../support/lc_test_barrier.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"

#define OUTBOX_TMP_PREFIX "/tmp/liblockdc-unit-outbox-"
#define OUTBOX_RECONCILIATION_RECORDS 256U
#define OUTBOX_PREFETCH_RECORDS 3U
#define OUTBOX_SHARED_PROCESS_RECORDS 256U
#define OUTBOX_SHARED_PROCESS_IDLE_LIMIT 40U
#define OUTBOX_CLEAN_REOPEN_FOREIGN_KEYS 32U
#define OUTBOX_CLEAN_REOPEN_FOREIGN_CHURN 16U
#define OUTBOX_CLEAN_REOPEN_PENDING_RECORDS 32U

typedef struct outbox_pouch_test_clock {
  struct timespec wall;
} outbox_pouch_test_clock;

static int outbox_pouch_test_gettime(clockid_t clock_id, struct timespec *out,
                                     void *context) {
  outbox_pouch_test_clock *clock = (outbox_pouch_test_clock *)context;

  if (clock_id == CLOCK_REALTIME) {
    *out = clock->wall;
    return 0;
  }
  return clock_gettime(clock_id, out);
}

static int outbox_pouch_test_clock_teardown(void **state) {
  (void)state;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_poll_delay = NULL;
  lc_pouch_test_queue_time_context = NULL;
  return 0;
}

/* The production API deliberately moved consumer operations to an explicit
 * dispatcher. Keep the pre-cutover behavioral cases meaningful while the
 * fixture is migrated: each adapter obtains that dispatcher, marks the old
 * fixture close path to stop it, and calls the new receiver. No compatibility
 * entry point is added to the installed SDK. */
typedef struct outbox_test_receipt_link {
  lc_outbox_transaction *transaction;
  lc_outbox_receipt *receipt;
  struct outbox_test_receipt_link *next;
} outbox_test_receipt_link;

static outbox_test_receipt_link *outbox_test_receipts;
static outbox_test_receipt_link *outbox_test_receipts_tail;

static void outbox_test_forget_receipts(lc_outbox_transaction *transaction) {
  outbox_test_receipt_link **link;

  link = &outbox_test_receipts;
  while (*link != NULL) {
    outbox_test_receipt_link *current = *link;

    if (current->transaction == transaction) {
      *link = current->next;
      free(current);
    } else {
      link = &current->next;
    }
  }
  outbox_test_receipts_tail = NULL;
  for (link = &outbox_test_receipts; *link != NULL; link = &(*link)->next)
    outbox_test_receipts_tail = *link;
}

static int outbox_test_track_receipt(lc_outbox_transaction *transaction,
                                     lc_outbox_receipt *receipt,
                                     lc_error *error) {
  outbox_test_receipt_link *link;

  link = (outbox_test_receipt_link *)malloc(sizeof(*link));
  if (link == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to track outbox test receipt", NULL, NULL,
                        NULL);
  }
  link->transaction = transaction;
  link->receipt = receipt;
  link->next = NULL;
  if (outbox_test_receipts_tail == NULL)
    outbox_test_receipts = link;
  else
    outbox_test_receipts_tail->next = link;
  outbox_test_receipts_tail = link;
  return LC_OK;
}

static int outbox_test_append(lc_outbox_transaction *transaction,
                              const lc_outbox_entry *entry, lc_source *payload,
                              lc_outbox_receipt *receipt, lc_error *error) {
  int rc;

  rc =
      lc_outbox_transaction_append(transaction, entry, payload, receipt, error);
  if (rc == LC_OK && !receipt->duplicate) {
    rc = outbox_test_track_receipt(transaction, receipt, error);
    if (rc != LC_OK)
      (void)lc_outbox_transaction_rollback(transaction, NULL);
  }
  return rc;
}

static int outbox_test_outbox_append(lc_outbox *outbox,
                                     const lc_outbox_entry *entry,
                                     lc_source *payload,
                                     lc_outbox_transaction **transaction,
                                     lc_outbox_receipt *receipt,
                                     lc_error *error) {
  int rc;

  rc = lc_outbox_append(outbox, entry, payload, transaction, receipt, error);
  if (rc == LC_OK && *transaction != NULL && !receipt->duplicate) {
    rc = outbox_test_track_receipt(*transaction, receipt, error);
    if (rc != LC_OK)
      (void)lc_outbox_transaction_rollback(*transaction, NULL);
  }
  return rc;
}

static lc_outbox_dispatcher *outbox_test_dispatcher(lc_outbox *outbox,
                                                    lc_error *error) {
  lc_outbox_dispatcher *dispatcher = NULL;

  if (lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, error) != LC_OK)
    return NULL;
  return dispatcher;
}

static int outbox_test_next(lc_outbox *outbox, long timeout_ms,
                            lc_outbox_job **out, lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_next(dispatcher, timeout_ms, out, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_next_with_state(lc_outbox *outbox, long timeout_ms,
                                       lc_outbox_job **out, lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_next_with_state(dispatcher, timeout_ms, out, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_stats(lc_outbox *outbox, lc_outbox_stats *out,
                             lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_get_stats(dispatcher, out, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_reconcile(lc_outbox *outbox, lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_reconcile(dispatcher, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_replay_dead_letter(lc_outbox *outbox, const char *key,
                                          lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_replay_dead_letter(dispatcher, key, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_delete_dead_letter(lc_outbox *outbox, const char *key,
                                          lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_delete_dead_letter(dispatcher, key, error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_export_dead_letters(
    lc_outbox *outbox, const lc_dead_letter_export_opts *options, lc_sink *dst,
    lc_dead_letter_export_res *out, lc_error *error) {
  lc_outbox_dispatcher *dispatcher = outbox_test_dispatcher(outbox, error);
  int rc;

  if (dispatcher == NULL)
    return error != NULL ? error->code : LC_ERR_INVALID;
  rc = lc_outbox_dispatcher_export_dead_letters(dispatcher, options, dst, out,
                                                error);
  lc_outbox_dispatcher_close(dispatcher);
  return rc;
}

static int outbox_test_commit(lc_outbox_transaction *transaction,
                              lc_error *error) {
  lc_outbox_commit_result result;
  outbox_test_receipt_link *link;
  size_t index;
  int rc;

  lc_outbox_commit_result_init(&result);
  rc = lc_outbox_transaction_commit(transaction, &result, error);
  if (rc == LC_OK) {
    index = 0U;
    for (link = outbox_test_receipts; link != NULL; link = link->next) {
      if (link->transaction == transaction &&
          index < result.outbox_receipt_count) {
        lc_outbox_receipt_cleanup(link->receipt);
        *link->receipt = result.outbox_receipts[index];
        memset(&result.outbox_receipts[index], 0,
               sizeof(result.outbox_receipts[index]));
        ++index;
      }
    }
    outbox_test_forget_receipts(transaction);
  }
  lc_outbox_commit_result_cleanup(&result);
  return rc;
}

static void outbox_test_transaction_close(lc_outbox_transaction *transaction) {
  outbox_test_forget_receipts(transaction);
  lc_outbox_transaction_close(transaction);
}

static int outbox_test_new(lc_client *client, const lc_outbox_config *config,
                           lc_outbox **out, lc_error *error) {
  lc_outbox_dispatcher *dispatcher;
  int rc;

  rc = lc_client_new_outbox(client, config, out, error);
  if (rc != LC_OK)
    return rc;
  dispatcher = NULL;
  rc = lc_outbox_dispatcher_get_or_start(*out, &dispatcher, error);
  if (rc == LC_OK) {
    lc_outbox_dispatcher_close(dispatcher);
  }
  if (rc != LC_OK) {
    lc_outbox_close(*out);
    *out = NULL;
  }
  return rc;
}

/* Keep direct public-wrapper coverage outside this test fixture's legacy
 * dispatcher adapter. */
static int outbox_public_new(lc_client *client, const lc_outbox_config *config,
                             lc_outbox **out, lc_error *error) {
  return lc_client_new_outbox(client, config, out, error);
}

static int outbox_public_new_with_dispatcher(lc_client *client,
                                             const lc_outbox_config *config,
                                             lc_outbox_dispatcher *dispatcher,
                                             lc_outbox **out, lc_error *error) {
  return lc_client_new_outbox_with_dispatcher(client, config, dispatcher, out,
                                              error);
}

static int outbox_public_get_or_start(lc_outbox *outbox,
                                      lc_outbox_dispatcher **out,
                                      lc_error *error) {
  return lc_outbox_dispatcher_get_or_start(outbox, out, error);
}

#define lc_client_new_outbox_test_new
#define lc_outbox_append outbox_test_outbox_append
#define lc_outbox_next outbox_test_next
#define lc_outbox_get_stats outbox_test_stats
#define lc_outbox_reconcile outbox_test_reconcile
#define lc_outbox_replay_dead_letter outbox_test_replay_dead_letter
#define lc_outbox_delete_dead_letter outbox_test_delete_dead_letter
#define lc_outbox_export_dead_letters outbox_test_export_dead_letters
#define lc_outbox_transaction_append outbox_test_append
#define lc_outbox_transaction_commit outbox_test_commit
#define lc_outbox_transaction_close outbox_test_transaction_close

static int outbox_fail_allocation(void *context, lc_error *error) {
  (void)context;
  return lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "forced outbox allocation failure", NULL, NULL, NULL);
}

typedef struct outbox_fail_once {
  unsigned int calls;
} outbox_fail_once;

typedef struct outbox_fail_on_call {
  unsigned int calls;
  unsigned int failure_call;
} outbox_fail_on_call;

typedef struct outbox_renewal_lease {
  lc_lease lease;
  long renewed_expiry;
  long requested_ttl;
  unsigned int calls;
} outbox_renewal_lease;

typedef struct outbox_metadata_append_failure {
  unsigned int calls;
  unsigned int failure_call;
} outbox_metadata_append_failure;

typedef struct outbox_inbox_runtime_failure {
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_inbox_message message;
  lc_outbox_transaction *transaction;
  lc_inbox_accept_result result;
  int rc;
  int error_code;
} outbox_inbox_runtime_failure;

typedef struct outbox_delete_outbox_hook {
  lc_client *client;
  const char *ns;
  const char *key;
  pthread_mutex_t mutex;
  unsigned int calls;
  int rc;
  int mutex_initialized;
} outbox_delete_outbox_hook;

typedef struct outbox_hold_dead_letter_hook {
  lc_client *client;
  const char *ns;
  const char *key;
  lc_lease *lease;
} outbox_hold_dead_letter_hook;

static int outbox_fail_first_call(void *context, lc_error *error) {
  outbox_fail_once *failure = (outbox_fail_once *)context;

  if (failure != NULL && failure->calls++ == 0U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "forced outbox transient claim failure", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int outbox_fail_on_configured_call(void *context, lc_error *error) {
  outbox_fail_on_call *failure = (outbox_fail_on_call *)context;

  assert_non_null(failure);
  ++failure->calls;
  if (failure->calls == failure->failure_call) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "forced outbox replay failure", NULL, NULL, "pouch");
  }
  return LC_OK;
}

static int outbox_renewal_keepalive(lc_lease *lease,
                                    const lc_keepalive_req *request,
                                    lc_error *error) {
  outbox_renewal_lease *renewal = (outbox_renewal_lease *)lease;

  (void)error;
  renewal->requested_ttl = request->ttl_seconds;
  ++renewal->calls;
  lease->lease_expires_at_unix = renewal->renewed_expiry;
  return LC_OK;
}

static int outbox_fail_metadata_append_at_call(void *context, lc_error *error) {
  outbox_metadata_append_failure *failure =
      (outbox_metadata_append_failure *)context;

  if (failure != NULL && failure->calls++ == failure->failure_call) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "forced outbox terminal metadata append failure", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static void *outbox_duplicate_inbox_without_json_runtime(void *context) {
  outbox_inbox_runtime_failure *attempt =
      (outbox_inbox_runtime_failure *)context;
  lc_error error;

  lc_error_init(&error);
  attempt->transaction = NULL;
  memset(&attempt->result, 0, sizeof(attempt->result));
  lc_lonejson_test_fail_thread_runtime_once();
  attempt->rc =
      lc_outbox_accept_inbox(attempt->outbox, &attempt->message,
                             &attempt->transaction, &attempt->result, &error);
  attempt->error_code = error.code;
  lc_error_cleanup(&error);
  return NULL;
}

static void outbox_delete_outbox_hook_init(outbox_delete_outbox_hook *hook,
                                           lc_client *client, const char *ns,
                                           const char *key) {
  memset(hook, 0, sizeof(*hook));
  hook->client = client;
  hook->ns = ns;
  hook->key = key;
  hook->rc = LC_ERR_INVALID;
  assert_int_equal(pthread_mutex_init(&hook->mutex, NULL), 0);
  hook->mutex_initialized = 1;
}

static void outbox_delete_outbox_hook_cleanup(outbox_delete_outbox_hook *hook) {
  if (hook != NULL && hook->mutex_initialized) {
    assert_int_equal(pthread_mutex_destroy(&hook->mutex), 0);
    hook->mutex_initialized = 0;
  }
}

static int outbox_delete_outbox_once(void *context, lc_error *error) {
  outbox_delete_outbox_hook *hook = (outbox_delete_outbox_hook *)context;
  lc_acquire_req acquire;
  lc_lease *lease;
  int rc;

  if (hook == NULL || hook->client == NULL || hook->ns == NULL ||
      hook->key == NULL || !hook->mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox delete hook requires an outbox target", NULL,
                        NULL, NULL);
  }
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  if (hook->calls != 0U) {
    assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
    return LC_OK;
  }
  ++hook->calls;
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);

  lc_acquire_req_init(&acquire);
  acquire.ns = hook->ns;
  acquire.key = hook->key;
  acquire.owner = "outbox-race-delete";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  rc = lc_acquire(hook->client, &acquire, &lease, error);
  if (rc == LC_OK) {
    rc = lc_lease_remove(lease, NULL, error);
  }
  lc_lease_close(lease);

  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  hook->rc = rc;
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
  return rc;
}

static void outbox_hold_dead_letter_for_export(void *context) {
  outbox_hold_dead_letter_hook *hook = (outbox_hold_dead_letter_hook *)context;
  lc_acquire_req acquire;
  lc_error error;

  assert_non_null(hook);
  assert_non_null(hook->client);
  assert_non_null(hook->ns);
  assert_non_null(hook->key);
  if (hook->lease != NULL)
    return;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire);
  acquire.ns = hook->ns;
  acquire.key = hook->key;
  acquire.owner = "outbox-dead-letter-export-race";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(hook->client, &acquire, &hook->lease, &error),
                   LC_OK);
  lc_error_cleanup(&error);
}

static void outbox_delete_outbox_hook_assert(outbox_delete_outbox_hook *hook) {
  unsigned int calls;
  int rc;

  assert_non_null(hook);
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  calls = hook->calls;
  rc = hook->rc;
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
  assert_int_equal(calls, 1U);
  assert_int_equal(rc, LC_OK);
}

static size_t outbox_failing_source_read(void *context, void *buffer,
                                         size_t count, lc_error *error) {
  (void)context;
  (void)buffer;
  (void)count;
  (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "forced outbox result attachment failure", NULL, NULL,
                     NULL);
  return 0U;
}

static int outbox_duration_overflows_unix_range(long duration) {
  time_t now = time(NULL);

  return duration > 0L && now != (time_t)-1 &&
         (uintmax_t)now <= (uintmax_t)LC_I64_MAX &&
         (uintmax_t)duration > (uintmax_t)LC_I64_MAX - (uintmax_t)now;
}

static int outbox_duration_overflows_wait_range(long duration) {
  time_t now = time(NULL);
  uintmax_t maximum;
  size_t bits;

  if (duration <= 0L || now == (time_t)-1 || now < (time_t)0)
    return 0;
  bits = sizeof(time_t) * CHAR_BIT;
  maximum = bits >= sizeof(uintmax_t) * CHAR_BIT ? (uintmax_t)-1
                                                 : ((uintmax_t)1U << bits) - 1U;
  if ((time_t)-1 < (time_t)0)
    maximum >>= 1U;
  return (uintmax_t)now <= maximum &&
         (uintmax_t)duration > maximum - (uintmax_t)now;
}

static void outbox_reset_allocation_failures(void) {
  lc_outbox_test_before_terminal_remove_hook = NULL;
  lc_outbox_test_before_terminal_remove_context = NULL;
  lc_outbox_test_after_close_requested_hook = NULL;
  lc_outbox_test_after_close_requested_context = NULL;
  lc_outbox_test_dead_letter_replay_client_hook = NULL;
  lc_outbox_test_dead_letter_replay_client_context = NULL;
  lc_outbox_test_after_recovery_overflow_hook = NULL;
  lc_outbox_test_after_recovery_overflow_context = NULL;
  lc_outbox_test_before_recovery_query_hook = NULL;
  lc_outbox_test_before_recovery_query_context = NULL;
  lc_outbox_test_after_recovery_capacity_pause_hook = NULL;
  lc_outbox_test_after_recovery_capacity_pause_context = NULL;
  lc_outbox_test_before_ready_job_detach_hook = NULL;
  lc_outbox_test_before_ready_job_detach_context = NULL;
  lc_outbox_test_before_ready_job_teardown_hook = NULL;
  lc_outbox_test_before_ready_job_teardown_context = NULL;
  lc_outbox_test_before_dispatcher_wait_hook = NULL;
  lc_outbox_test_before_dispatcher_wait_context = NULL;
  lc_outbox_test_before_next_wait_hook = NULL;
  lc_outbox_test_before_next_wait_context = NULL;
  lc_outbox_test_after_command_wait_pending_read_hook = NULL;
  lc_outbox_test_after_command_wait_pending_read_context = NULL;
  lc_outbox_test_before_next_release_hook = NULL;
  lc_outbox_test_before_next_release_context = NULL;
  lc_outbox_test_after_dispatcher_core_retain_hook = NULL;
  lc_outbox_test_after_dispatcher_core_retain_context = NULL;
  lc_outbox_test_before_worker_release_hook = NULL;
  lc_outbox_test_before_worker_release_context = NULL;
  lc_outbox_test_before_dead_letter_export_open_hook = NULL;
  lc_outbox_test_before_dead_letter_export_open_context = NULL;
  lc_outbox_test_before_ledger_append_hook = NULL;
  lc_outbox_test_before_ledger_append_context = NULL;
  lc_outbox_test_before_participant_allocation_hook = NULL;
  lc_outbox_test_before_participant_allocation_context = NULL;
  lc_outbox_test_before_command_receipt_copy_hook = NULL;
  lc_outbox_test_before_command_receipt_copy_context = NULL;
  lc_outbox_test_after_command_terminal_load_hook = NULL;
  lc_outbox_test_after_command_terminal_load_context = NULL;
  lc_outbox_test_before_outbox_receipt_copy_hook = NULL;
  lc_outbox_test_before_outbox_receipt_copy_context = NULL;
  lc_outbox_test_before_transaction_terminal_release_hook = NULL;
  lc_outbox_test_before_transaction_terminal_release_context = NULL;
  lc_outbox_test_after_transaction_terminal_decision_hook = NULL;
  lc_outbox_test_after_transaction_terminal_decision_context = NULL;
  lc_outbox_test_before_notification_copy_hook = NULL;
  lc_outbox_test_before_notification_copy_context = NULL;
  lc_outbox_test_before_claim_outbox_hook = NULL;
  lc_outbox_test_before_claim_outbox_context = NULL;
  lc_outbox_test_before_outbox_handoff_reacquire_hook = NULL;
  lc_outbox_test_before_outbox_handoff_reacquire_context = NULL;
  lc_outbox_test_before_stateful_handoff_hook = NULL;
  lc_outbox_test_before_stateful_handoff_context = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_hook = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_context = NULL;
  lc_outbox_test_before_periodic_recovery_schedule_hook = NULL;
  lc_outbox_test_before_periodic_recovery_schedule_context = NULL;
  lc_outbox_test_before_dead_letter_claim_cleanup_hook = NULL;
  lc_outbox_test_before_dead_letter_replay_release_hook = NULL;
  lc_outbox_test_before_dead_letter_replay_release_context = NULL;
  lc_outbox_test_before_dead_letter_claim_cleanup_context = NULL;
  lc_outbox_test_before_outbox_renew_keepalive_hook = NULL;
  lc_outbox_test_before_outbox_renew_keepalive_context = NULL;
  lc_outbox_test_before_outbox_renew_deadline_publish_hook = NULL;
  lc_outbox_test_before_outbox_renew_deadline_publish_context = NULL;
  lc_pouch_test_before_txn_replay_hook = NULL;
  lc_pouch_test_before_txn_replay_context = NULL;
}

static int outbox_bytes_contains(const void *bytes, size_t length,
                                 const char *needle) {
  size_t needle_length;
  size_t index;

  if (bytes == NULL || needle == NULL)
    return 0;
  needle_length = strlen(needle);
  if (needle_length == 0U || needle_length > length)
    return 0;
  for (index = 0U; index <= length - needle_length; ++index) {
    if (memcmp((const char *)bytes + index, needle, needle_length) == 0)
      return 1;
  }
  return 0;
}

typedef struct outbox_dead_letter_replay_client_capture {
  pthread_mutex_t mutex;
  unsigned int calls;
  int used_dispatcher_client;
} outbox_dead_letter_replay_client_capture;

static void outbox_capture_dead_letter_replay_client(lc_client *client,
                                                     lc_client *dispatcher,
                                                     void *context) {
  outbox_dead_letter_replay_client_capture *capture =
      (outbox_dead_letter_replay_client_capture *)context;

  assert_non_null(capture);
  assert_int_equal(pthread_mutex_lock(&capture->mutex), 0);
  ++capture->calls;
  capture->used_dispatcher_client = client == dispatcher;
  assert_int_equal(pthread_mutex_unlock(&capture->mutex), 0);
}

static int outbox_slow_test_runtime(void) {
  const char *value = getenv("LOCKDC_SLOW_TEST_RUNTIME");

  return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0;
}

typedef struct outbox_next_wait_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  lc_outbox *outbox;
  long timeout_ms;
  int dispatcher_wait_entered;
  int allow_dispatcher_wait;
  int next_wait_entered;
  int allow_next_wait;
  int next_finished;
  int next_rc;
  lc_outbox_job *job;
  struct timespec started;
  struct timespec finished;
} outbox_next_wait_race;

static int outbox_next_wait_race_wait(outbox_next_wait_race *race, int *flag) {
  struct timespec deadline;
  int reached;
  int rc;

  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0)
    return 0;
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  rc = 0;
  while (!*flag && rc == 0)
    rc = pthread_cond_timedwait(&race->condition, &race->mutex, &deadline);
  reached = *flag;
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return rc == 0 && reached;
}

static void outbox_dispatcher_wait_hook(void *context) {
  outbox_next_wait_race *race = (outbox_next_wait_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->dispatcher_wait_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_dispatcher_wait)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_next_wait_hook(void *context) {
  outbox_next_wait_race *race = (outbox_next_wait_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->next_wait_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_next_wait)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_next_wait_observed_hook(void *context) {
  outbox_next_wait_race *race = (outbox_next_wait_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->next_wait_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void *outbox_next_wait_thread(void *context) {
  outbox_next_wait_race *race = (outbox_next_wait_race *)context;
  lc_error error;
  lc_outbox_job *job;
  int rc;

  job = NULL;
  lc_error_init(&error);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &race->started), 0);
  rc = lc_outbox_next(race->outbox, race->timeout_ms, &job, &error);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &race->finished), 0);
  lc_error_cleanup(&error);
  (void)pthread_mutex_lock(&race->mutex);
  race->next_rc = rc;
  race->job = job;
  race->next_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

typedef struct outbox_dispatcher_core_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  lc_outbox_dispatcher *dispatcher;
  int core_retained;
  int allow_stats;
  int stats_finished;
  int stats_rc;
} outbox_dispatcher_core_race;

typedef struct outbox_dispatcher_acquire_race {
  lc_outbox *outbox;
  pthread_barrier_t *barrier;
  lc_outbox_dispatcher *dispatcher;
  int rc;
} outbox_dispatcher_acquire_race;

static void *outbox_dispatcher_acquire_thread(void *context) {
  outbox_dispatcher_acquire_race *race =
      (outbox_dispatcher_acquire_race *)context;
  lc_error error;
  int barrier_rc;

  lc_error_init(&error);
  barrier_rc = pthread_barrier_wait(race->barrier);
  assert_true(barrier_rc == 0 || barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD);
  race->rc = lc_outbox_dispatcher_get_or_start(race->outbox, &race->dispatcher,
                                               &error);
  lc_error_cleanup(&error);
  return NULL;
}

static int outbox_dispatcher_core_race_wait(outbox_dispatcher_core_race *race,
                                            int *flag) {
  struct timespec deadline;
  int reached;
  int rc;

  assert_int_equal(clock_gettime(CLOCK_REALTIME, &deadline), 0);
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  rc = 0;
  while (!*flag && rc == 0)
    rc = pthread_cond_timedwait(&race->condition, &race->mutex, &deadline);
  reached = *flag;
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return rc == 0 && reached;
}

static void outbox_dispatcher_core_retained_hook(void *context) {
  outbox_dispatcher_core_race *race = (outbox_dispatcher_core_race *)context;

  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  race->core_retained = 1;
  assert_int_equal(pthread_cond_broadcast(&race->condition), 0);
  while (!race->allow_stats)
    assert_int_equal(pthread_cond_wait(&race->condition, &race->mutex), 0);
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
}

static void *outbox_dispatcher_stats_thread(void *context) {
  outbox_dispatcher_core_race *race = (outbox_dispatcher_core_race *)context;
  lc_outbox_stats stats;
  lc_error error;
  int rc;

  lc_outbox_stats_init(&stats);
  lc_error_init(&error);
  rc = lc_outbox_dispatcher_get_stats(race->dispatcher, &stats, &error);
  lc_outbox_stats_cleanup(&stats);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  race->stats_rc = rc;
  race->stats_finished = 1;
  assert_int_equal(pthread_cond_broadcast(&race->condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return NULL;
}

static long outbox_claim_ttl_seconds(void) {
  return outbox_slow_test_runtime() ? 2L : 1L;
}

static long outbox_claim_next_timeout_ms(void) {
  return outbox_slow_test_runtime() ? 10000L : 5000L;
}

static unsigned int outbox_claim_expiry_wait_seconds(void) {
  return outbox_slow_test_runtime() ? 3U : 2U;
}

static long outbox_elapsed_milliseconds(const struct timespec *started,
                                        const struct timespec *finished);

typedef struct outbox_shutdown_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int periodic_schedule_entered;
  int close_requested;
  int allow_close;
  int ready_detach_entered;
  int allow_ready_detach;
  int teardown_entered;
  int allow_teardown;
  int close_finished;
  int next_finished;
  int next_rc;
  lc_outbox *outbox;
  lc_outbox_job *job;
} outbox_shutdown_race;

typedef struct outbox_blocked_next_close_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  lc_outbox_dispatcher *dispatcher;
  int next_wait_entered;
  int close_requested;
  int allow_close;
  int next_release_entered;
  int allow_next_release;
  int close_finished;
  int next_finished;
  int next_rc;
  lc_outbox_job *job;
} outbox_blocked_next_close_race;

static int outbox_shutdown_race_wait(outbox_shutdown_race *race, int *flag) {
  struct timespec deadline;
  int reached;
  int rc;

  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0)
    return 0;
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  rc = 0;
  while (!*flag && rc == 0)
    rc = pthread_cond_timedwait(&race->condition, &race->mutex, &deadline);
  reached = *flag;
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return rc == 0 && reached;
}

static void outbox_shutdown_race_after_close_requested(void *context) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->close_requested = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_close)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_shutdown_race_before_ready_detach(void *context) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->ready_detach_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_ready_detach)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_shutdown_race_before_teardown(void *context) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->teardown_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_teardown)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void *outbox_shutdown_race_close_thread(void *context) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;

  lc_outbox_close(race->outbox);
  (void)pthread_mutex_lock(&race->mutex);
  race->close_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

static int outbox_periodic_recovery_schedule_failure(void *context,
                                                     lc_error *error) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->periodic_schedule_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "forced periodic recovery scheduling failure", NULL, NULL,
                      NULL);
}

static void *outbox_shutdown_race_next_thread(void *context) {
  outbox_shutdown_race *race = (outbox_shutdown_race *)context;
  lc_error error;
  lc_outbox_job *job;
  int rc;

  lc_error_init(&error);
  job = NULL;
  rc = lc_outbox_next(race->outbox, 0L, &job, &error);
  lc_error_cleanup(&error);
  (void)pthread_mutex_lock(&race->mutex);
  race->next_rc = rc;
  race->job = job;
  race->next_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

static int
outbox_blocked_next_close_race_wait(outbox_blocked_next_close_race *race,
                                    int *flag) {
  struct timespec deadline;
  int reached;
  int rc;

  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0)
    return 0;
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  rc = 0;
  while (!*flag && rc == 0)
    rc = pthread_cond_timedwait(&race->condition, &race->mutex, &deadline);
  reached = *flag;
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return rc == 0 && reached;
}

static void outbox_blocked_next_close_before_wait(void *context) {
  outbox_blocked_next_close_race *race =
      (outbox_blocked_next_close_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->next_wait_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_blocked_next_close_after_close_requested(void *context) {
  outbox_blocked_next_close_race *race =
      (outbox_blocked_next_close_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->close_requested = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_close)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void outbox_blocked_next_close_before_release(void *context) {
  outbox_blocked_next_close_race *race =
      (outbox_blocked_next_close_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->next_release_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_next_release)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void *outbox_blocked_next_close_thread(void *context) {
  outbox_blocked_next_close_race *race =
      (outbox_blocked_next_close_race *)context;

  lc_error error;

  lc_error_init(&error);
  assert_int_equal(lc_outbox_dispatcher_stop(race->dispatcher, -1L, &error),
                   LC_OK);
  lc_error_cleanup(&error);
  (void)pthread_mutex_lock(&race->mutex);
  race->close_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

static void *outbox_blocked_next_next_thread(void *context) {
  outbox_blocked_next_close_race *race =
      (outbox_blocked_next_close_race *)context;
  lc_error error;
  lc_outbox_job *job;
  int rc;

  lc_error_init(&error);
  job = NULL;
  rc = lc_outbox_dispatcher_next(race->dispatcher, -1L, &job, &error);
  lc_error_cleanup(&error);
  (void)pthread_mutex_lock(&race->mutex);
  race->next_rc = rc;
  race->job = job;
  race->next_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

typedef struct outbox_query_count {
  unsigned long count;
} outbox_query_count;

typedef struct outbox_tracked_allocation {
  void *pointer;
  struct outbox_tracked_allocation *next;
} outbox_tracked_allocation;

typedef struct outbox_tracking_allocator {
  pthread_mutex_t mutex;
  outbox_tracked_allocation *allocations;
  size_t foreign_free_calls;
  size_t foreign_realloc_calls;
} outbox_tracking_allocator;

static void
outbox_tracking_allocator_init(outbox_tracking_allocator *allocator) {
  memset(allocator, 0, sizeof(*allocator));
  assert_int_equal(pthread_mutex_init(&allocator->mutex, NULL), 0);
}

static void
outbox_tracking_allocator_destroy(outbox_tracking_allocator *allocator) {
  outbox_tracked_allocation *allocation;

  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  allocation = allocator->allocations;
  allocator->allocations = NULL;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  assert_null(allocation);
  assert_int_equal(allocator->foreign_free_calls, 0U);
  assert_int_equal(allocator->foreign_realloc_calls, 0U);
  pthread_mutex_destroy(&allocator->mutex);
}

static void *outbox_tracking_malloc(void *context, size_t size) {
  outbox_tracking_allocator *allocator = (outbox_tracking_allocator *)context;
  outbox_tracked_allocation *allocation;
  void *pointer;

  pointer = malloc(size == 0U ? 1U : size);
  if (pointer == NULL)
    return NULL;
  allocation = (outbox_tracked_allocation *)malloc(sizeof(*allocation));
  if (allocation == NULL) {
    free(pointer);
    return NULL;
  }
  allocation->pointer = pointer;
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  allocation->next = allocator->allocations;
  allocator->allocations = allocation;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  return pointer;
}

static void *outbox_tracking_realloc(void *context, void *pointer,
                                     size_t size) {
  outbox_tracking_allocator *allocator = (outbox_tracking_allocator *)context;
  outbox_tracked_allocation **cursor;
  outbox_tracked_allocation *allocation;
  void *resized;

  if (pointer == NULL)
    return outbox_tracking_malloc(context, size);
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  cursor = &allocator->allocations;
  while (*cursor != NULL && (*cursor)->pointer != pointer)
    cursor = &(*cursor)->next;
  if (*cursor == NULL) {
    ++allocator->foreign_realloc_calls;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    return NULL;
  }
  allocation = *cursor;
  if (size == 0U) {
    *cursor = allocation->next;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    free(allocation);
    free(pointer);
    return NULL;
  }
  resized = realloc(pointer, size);
  if (resized != NULL)
    allocation->pointer = resized;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  return resized;
}

static void outbox_tracking_free(void *context, void *pointer) {
  outbox_tracking_allocator *allocator = (outbox_tracking_allocator *)context;
  outbox_tracked_allocation **cursor;
  outbox_tracked_allocation *allocation;

  if (pointer == NULL)
    return;
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  cursor = &allocator->allocations;
  while (*cursor != NULL && (*cursor)->pointer != pointer)
    cursor = &(*cursor)->next;
  if (*cursor == NULL) {
    ++allocator->foreign_free_calls;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    return;
  }
  allocation = *cursor;
  *cursor = allocation->next;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  free(allocation);
  free(pointer);
}

typedef struct outbox_reconcile_overflow_hook {
  lc_outbox *outbox;
  int calls;
  int rc;
} outbox_reconcile_overflow_hook;

typedef struct outbox_recovery_overflow_drain_hook {
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  const char *terminal_key;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int calls;
  int query_gate_calls;
  int fill_on_query;
  int query_started;
  int allow_query;
  int overflowed;
  int rc;
} outbox_recovery_overflow_drain_hook;

static int outbox_query_count_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int outbox_query_count_chunk(void *context, const char *bytes,
                                    size_t length, lc_error *error) {
  (void)context;
  (void)bytes;
  (void)length;
  (void)error;
  return 1;
}

static int outbox_query_count_end(void *context, lc_error *error) {
  outbox_query_count *count = (outbox_query_count *)context;

  (void)error;
  ++count->count;
  return 1;
}

static void
seed_outbox_with_counters(lc_client *client, const char *ns, const char *key,
                          const char *dispatch_state, const char *attempt_count,
                          const char *replay_count, lc_error *error) {
  char state[512];
  int state_length;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  lc_attach_req attach;
  lc_attach_res attach_result;

  assert_non_null(dispatch_state);
  assert_non_null(attempt_count);
  assert_non_null(replay_count);
  state_length = snprintf(
      state, sizeof(state),
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"recovery-op\","
      "\"effect_id\":\"recovery-effect\",\"effect_key\":\"recovery-key\","
      "\"payload_digest\":\"recovery-payload-digest\","
      "\"message_id\":\"msg_recovery\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"%s\","
      "\"attempt_count\":%s,\"not_before_unix\":0,\"replay_count\":%s}",
      dispatch_state, attempt_count, replay_count);
  assert_true(state_length > 0 && (size_t)state_length < sizeof(state));
  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-recovery-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, (size_t)state_length, &state_source, error),
      LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  payload_source = NULL;
  assert_int_equal(
      lc_source_from_memory("recovery-payload", 16U, &payload_source, error),
      LC_OK);
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = "text/plain";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  assert_int_equal(
      lc_lease_attach(lease, &attach, payload_source, &attach_result, error),
      LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_recovery_outbox_with_attempt_count(lc_client *client,
                                                    const char *ns,
                                                    const char *key,
                                                    const char *attempt_count,
                                                    lc_error *error) {
  seed_outbox_with_counters(client, ns, key, "pending", attempt_count, "0",
                            error);
}

static void seed_dead_letter_outbox_with_replay_count(lc_client *client,
                                                      const char *ns,
                                                      const char *key,
                                                      const char *replay_count,
                                                      lc_error *error) {
  seed_outbox_with_counters(client, ns, key, "dead_letter", "1", replay_count,
                            error);
}

static void seed_retained_dead_letter_outbox(lc_client *client, const char *ns,
                                             const char *key,
                                             const char *retention_id,
                                             lc_error *error) {
  char state[768];
  int state_length;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *source;

  seed_dead_letter_outbox_with_replay_count(client, ns, key, "0", error);
  state_length = snprintf(
      state, sizeof(state),
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"retention-op\","
      "\"effect_id\":\"retention-effect\",\"effect_key\":\"retention-key\","
      "\"payload_digest\":\"retention-payload-digest\","
      "\"message_id\":\"msg_retention\","
      "\"kind\":\"test\",\"destination\":\"retention://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"dead_letter\","
      "\"attempt_count\":1,\"not_before_unix\":0,\"replay_count\":0,"
      "\"dead_letter_retention_id\":\"%s\"}",
      retention_id);
  assert_true(state_length > 0 && (size_t)state_length < sizeof(state));
  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-retention-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  source = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  assert_int_equal(
      lc_source_from_memory(state, (size_t)state_length, &source, error),
      LC_OK);
  assert_int_equal(lc_lease_update(lease, source, NULL, error), LC_OK);
  lc_source_close(source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void outbox_assert_state_presence(lc_client *client, const char *ns,
                                         const char *key, int present,
                                         lc_error *error) {
  lc_acquire_req acquire;
  lc_get_res result;
  lc_lease *lease;
  lc_sink *sink;

  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-retention-inspect";
  acquire.ttl_seconds = 30L;
  memset(&result, 0, sizeof(result));
  lease = NULL;
  sink = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &result, error), LC_OK);
  assert_int_equal(result.no_content == 0, present != 0);
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_recovery_outbox(lc_client *client, const char *ns,
                                 const char *key, lc_error *error) {
  seed_recovery_outbox_with_attempt_count(client, ns, key, "0", error);
}

static void seed_malformed_recovery_outbox(lc_client *client, const char *ns,
                                           const char *key, lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\","
      "\"operation_id\":\"malformed-op\","
      "\"effect_id\":\"malformed-effect\","
      "\"effect_key\":\"malformed-key\","
      "\"payload_digest\":\"malformed-payload-digest\","
      "\"message_id\":\"msg_malformed\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"headers_json\":\"{\","
      "\"dispatch_state\":\"pending\",\"attempt_count\":0,"
      "\"not_before_unix\":0,\"replay_count\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;

  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-malformed-recovery-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, sizeof(state) - 1U, &state_source, error),
      LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_malformed_dead_letter_outbox(lc_client *client, const char *ns,
                                              const char *key,
                                              lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\","
      "\"operation_id\":\"malformed-op\","
      "\"effect_id\":\"malformed-effect\","
      "\"effect_key\":\"malformed-key\","
      "\"payload_digest\":\"malformed-payload-digest\","
      "\"message_id\":\"msg_malformed\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"headers_json\":\"{\","
      "\"dispatch_state\":\"dead_letter\",\"attempt_count\":1,"
      "\"not_before_unix\":0,\"replay_count\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;

  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-malformed-dead-letter-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, sizeof(state) - 1U, &state_source, error),
      LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_terminal_outbox(lc_client *client, const char *ns,
                                 const char *key, lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"terminal-op\","
      "\"effect_id\":\"terminal-effect\",\"effect_key\":\"terminal-key\","
      "\"payload_digest\":\"terminal-payload-digest\","
      "\"message_id\":\"msg_terminal\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"completed\","
      "\"attempt_count\":1,\"not_before_unix\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *source;

  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-terminal-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  source = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  assert_int_equal(
      lc_source_from_memory(state, sizeof(state) - 1U, &source, error), LC_OK);
  assert_int_equal(lc_lease_update(lease, source, NULL, error), LC_OK);
  lc_source_close(source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_foreign_outbox_state(lc_client *client, const char *ns,
                                      const char *key,
                                      const char *dispatch_state,
                                      lc_error *error) {
  char state[160];
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;

  assert_true(snprintf(state, sizeof(state),
                       "{\"record_type\":\"foreign.v1\","
                       "\"dispatch_state\":\"%s\"}",
                       dispatch_state) > 0);
  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-foreign-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, strlen(state), &state_source, error), LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

typedef struct outbox_process_result {
  int rc;
  int got_job;
  unsigned long delivered;
  char error_message[256];
  char error_detail[256];
} outbox_process_result;

static outbox_process_result
outbox_shared_process_claim(const char *root, const char *ns, int start_fd) {
  outbox_process_result result;
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  char start;

  memset(&result, 0, sizeof(result));
  result.rc = LC_ERR_TRANSPORT;
  if (read(start_fd, &start, 1U) != 1) {
    (void)close(start_fd);
    return result;
  }
  (void)close(start_fd);
  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s?single_writer=false",
               root) < 0)
    return result;
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  job = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  result.rc = lc_client_open(&client_config, &client, &error);
  if (result.rc == LC_OK) {
    lc_outbox_config_init(&outbox_config);
    outbox_config.ns = ns;
    outbox_config.owner = "outbox-shared-child";
    outbox_config.recovery_interval_seconds = 1L;
    result.rc = lc_client_new_outbox(client, &outbox_config, &outbox, &error);
  }
  if (result.rc == LC_OK)
    result.rc = lc_outbox_next(outbox, 5000L, &job, &error);
  if (result.rc == LC_OK && job != NULL) {
    result.got_job = 1;
    result.rc = lc_outbox_job_complete(job, NULL, &error);
    if (result.rc == LC_OK) {
      result.delivered = 1UL;
      job = NULL;
    }
  }
  if (job != NULL)
    lc_outbox_job_close(job);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  if (error.message != NULL)
    (void)snprintf(result.error_message, sizeof(result.error_message), "%s",
                   error.message);
  if (error.detail != NULL)
    (void)snprintf(result.error_detail, sizeof(result.error_detail), "%s",
                   error.detail);
  lc_error_cleanup(&error);
  return result;
}

static outbox_process_result outbox_shared_process_drain(const char *root,
                                                         const char *ns,
                                                         const char *owner,
                                                         int start_fd) {
  outbox_process_result result;
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  char start;
  unsigned long idle_count;

  memset(&result, 0, sizeof(result));
  result.rc = LC_ERR_TRANSPORT;
  if (read(start_fd, &start, 1U) != 1) {
    (void)close(start_fd);
    return result;
  }
  (void)close(start_fd);
  if (start != 's' ||
      snprintf(endpoint, sizeof(endpoint),
               "pouch://%s?single_writer=false&segment_target_bytes=65536",
               root) < 0) {
    result.rc = LC_ERR_INVALID;
    return result;
  }
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  idle_count = 0U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  result.rc = lc_client_open(&client_config, &client, &error);
  if (result.rc == LC_OK) {
    lc_outbox_config_init(&outbox_config);
    outbox_config.ns = ns;
    outbox_config.owner = owner;
    outbox_config.notification_capacity = 16U;
    outbox_config.recovery_interval_seconds = 1L;
    result.rc = lc_client_new_outbox(client, &outbox_config, &outbox, &error);
  }
  while (result.rc == LC_OK && idle_count < OUTBOX_SHARED_PROCESS_IDLE_LIMIT) {
    lc_outbox_job *job;

    job = NULL;
    result.rc = lc_outbox_next(outbox, 250L, &job, &error);
    if (result.rc != LC_OK)
      break;
    if (job == NULL) {
      ++idle_count;
      continue;
    }
    idle_count = 0U;
    result.got_job = 1;
    result.rc = lc_outbox_job_complete(job, NULL, &error);
    if (result.rc == LC_OK) {
      ++result.delivered;
      job = NULL;
    }
    if (job != NULL)
      lc_outbox_job_close(job);
  }
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  if (error.message != NULL)
    (void)snprintf(result.error_message, sizeof(result.error_message), "%s",
                   error.message);
  if (error.detail != NULL)
    (void)snprintf(result.error_detail, sizeof(result.error_detail), "%s",
                   error.detail);
  lc_error_cleanup(&error);
  return result;
}

static void outbox_assert_outbox_completed(lc_client *client, const char *key,
                                           lc_error *error) {
  lc_get_opts options;
  lc_get_res result;
  lc_sink *sink;
  const void *bytes;
  size_t length;

  lc_get_opts_init(&options);
  options.public_read = 1;
  memset(&result, 0, sizeof(result));
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_to_memory(&sink, error), LC_OK);
  assert_int_equal(lc_get(client, key, &options, sink, &result, error), LC_OK);
  assert_false(result.no_content);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, error), LC_OK);
  assert_true(
      outbox_bytes_contains(bytes, length, "\"dispatch_state\":\"completed\""));
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
}

static void outbox_assert_public_state_absent(lc_client *client,
                                              const char *key,
                                              lc_error *error) {
  lc_get_opts options;
  lc_get_res result;
  lc_sink *sink;

  lc_get_opts_init(&options);
  options.public_read = 1;
  memset(&result, 0, sizeof(result));
  sink = NULL;
  assert_int_equal(lc_sink_to_memory(&sink, error), LC_OK);
  assert_int_equal(lc_get(client, key, &options, sink, &result, error), LC_OK);
  assert_true(result.no_content);
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
}

static void outbox_claim_deadline_key(const char *outbox_key, char key[128]) {
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  char encoded[EVP_ENCODE_LENGTH(32U)];
  unsigned int digest_length;
  size_t index;

  assert_non_null(outbox_key);
  ctx = EVP_MD_CTX_new();
  assert_non_null(ctx);
  digest_length = 0U;
  assert_int_equal(EVP_DigestInit_ex(ctx, EVP_sha256(), NULL), 1);
  assert_int_equal(EVP_DigestUpdate(ctx, outbox_key, strlen(outbox_key)), 1);
  assert_int_equal(EVP_DigestFinal_ex(ctx, digest, &digest_length), 1);
  EVP_MD_CTX_free(ctx);
  assert_int_equal(digest_length, 32U);
  assert_int_equal(
      EVP_EncodeBlock((unsigned char *)encoded, digest, (int)digest_length),
      44);
  for (index = 0U; index < 43U; ++index) {
    if (encoded[index] == '+')
      encoded[index] = '-';
    else if (encoded[index] == '/')
      encoded[index] = '_';
  }
  encoded[43] = '\0';
  assert_true(snprintf(key, 128U, "__lockdc_io/v1/outbox-claim/%s", encoded) >
              0);
}

static int outbox_force_pouch_txn_decision_failure(void *context,
                                                   lc_error *error) {
  int *calls = (int *)context;

  ++*calls;
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "forced outbox terminal decision failure", NULL, NULL,
                      "pouch");
}

static void outbox_reconcile_overflow_commit_hook(void *context) {
  outbox_reconcile_overflow_hook *hook =
      (outbox_reconcile_overflow_hook *)context;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_source *payload;
  lc_error error;

  if (hook == NULL || hook->outbox == NULL || hook->calls != 0)
    return;
  ++hook->calls;
  hook->rc = LC_ERR_INVALID;
  lc_error_init(&error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "reconcile-overflow-operation";
  entry.effect_id = "reconcile-overflow-effect";
  entry.effect_key = "reconcile-overflow-effect-key";
  entry.payload_digest = "reconcile-overflow-payload-digest";
  entry.kind = "test";
  entry.destination = "reconcile://overflow";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  payload = NULL;
  hook->rc = lc_source_from_memory("overflow", 8U, &payload, &error);
  if (hook->rc == LC_OK) {
    hook->rc = lc_outbox_append(hook->outbox, &entry, payload, &transaction,
                                &receipt, &error);
  }
  if (hook->rc == LC_OK) {
    hook->rc = lc_outbox_transaction_commit(transaction, &error);
  }
  if (transaction != NULL)
    lc_outbox_transaction_close(transaction);
  if (payload != NULL)
    lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
}

static void outbox_drain_recovery_overflow_hook(void *context) {
  outbox_recovery_overflow_drain_hook *hook =
      (outbox_recovery_overflow_drain_hook *)context;
  lc_outbox_job *job;
  lc_error error;

  if (hook == NULL || hook->outbox == NULL || hook->calls != 0)
    return;
  ++hook->calls;
  hook->rc = LC_ERR_INVALID;
  job = NULL;
  lc_error_init(&error);
  hook->rc = lc_outbox_next(hook->outbox, 0L, &job, &error);
  if (hook->rc == LC_OK && job != NULL) {
    hook->rc = lc_outbox_job_complete(job, NULL, &error);
    job = NULL;
  }
  if (job != NULL)
    lc_outbox_job_close(job);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  hook->overflowed = 1;
  assert_int_equal(pthread_cond_broadcast(&hook->condition), 0);
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
}

static void outbox_gate_first_recovery_query(void *context) {
  outbox_recovery_overflow_drain_hook *hook =
      (outbox_recovery_overflow_drain_hook *)context;

  assert_non_null(hook);
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  if (hook->query_gate_calls++ == 0) {
    hook->query_started = 1;
    assert_int_equal(pthread_cond_broadcast(&hook->condition), 0);
    while (!hook->allow_query)
      assert_int_equal(pthread_cond_wait(&hook->condition, &hook->mutex), 0);
  }
  if (hook->query_gate_calls == hook->fill_on_query) {
    lc_error error;

    assert_non_null(hook->dispatcher);
    assert_non_null(hook->terminal_key);
    lc_error_init(&error);
    hook->rc = lc_outbox_dispatcher_notify_outbox_key(
        hook->dispatcher, hook->terminal_key, &error);
    lc_error_cleanup(&error);
  }
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
}

static void outbox_wait_for_recovery_overflow_hook(
    outbox_recovery_overflow_drain_hook *hook, int *flag) {
  struct timespec deadline;
  int wait_rc;

  assert_int_equal(clock_gettime(CLOCK_REALTIME, &deadline), 0);
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  wait_rc = 0;
  while (!*flag && wait_rc == 0)
    wait_rc = pthread_cond_timedwait(&hook->condition, &hook->mutex, &deadline);
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
  assert_int_equal(wait_rc, 0);
  assert_true(*flag);
}

static void outbox_note_recovery_capacity_pause(void *context) {
  outbox_recovery_overflow_drain_hook *hook =
      (outbox_recovery_overflow_drain_hook *)context;

  assert_non_null(hook);
  assert_int_equal(pthread_mutex_lock(&hook->mutex), 0);
  ++hook->calls;
  hook->overflowed = 1;
  assert_int_equal(pthread_cond_broadcast(&hook->condition), 0);
  assert_int_equal(pthread_mutex_unlock(&hook->mutex), 0);
}

/* Fill the candidate queue at the precise point where next() has removed the
 * current key but has not yet installed its claim-recovery wake. This is the
 * only interleaving that can otherwise exceed the combined bounded capacity. */
static int outbox_claim_refill_hook(void *context, lc_error *error) {
  outbox_reconcile_overflow_hook *hook =
      (outbox_reconcile_overflow_hook *)context;

  outbox_reconcile_overflow_commit_hook(context);
  if (hook != NULL && hook->rc == LC_OK)
    return LC_OK;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox claim refill hook failed", NULL, NULL, NULL);
}

static void test_pouch_claim_recovery_respects_combined_capacity(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_reconcile_overflow_hook hook;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_outbox_stats stats;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "claim-capacity-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  memset(&hook, 0, sizeof(hook));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-claim-capacity";
  outbox_config.owner = "outbox-claim-capacity-test";
  outbox_config.notification_capacity = 1U;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  seed_recovery_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/claim-capacity-a", &error);
  assert_int_equal(lc_outbox_dispatcher_reconcile(dispatcher, &error), LC_OK);
  assert_int_equal(lc_outbox_dispatcher_reconcile(dispatcher, &error), LC_OK);
  hook.outbox = outbox;
  lc_outbox_test_before_claim_outbox_hook = outbox_claim_refill_hook;
  lc_outbox_test_before_claim_outbox_context = &hook;
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  lc_outbox_test_before_claim_outbox_hook = NULL;
  lc_outbox_test_before_claim_outbox_context = NULL;
  assert_non_null(job);
  assert_int_equal(hook.calls, 1);
  assert_int_equal(hook.rc, LC_OK);
  memset(&stats, 0, sizeof(stats));
  assert_int_equal(lc_outbox_dispatcher_get_stats(dispatcher, &stats, &error),
                   LC_OK);
  assert_int_equal(stats.pending_candidates, 1U);
  assert_int_equal(stats.delayed_wakes, 0U);
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_duplicate_rejects_immutable_envelope_conflicts(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_outbox_receipt receipt;
  lc_source *payload;
  lc_source *conflicting_payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "immutable-envelope-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox";
  outbox_config.owner = "outbox-immutable-envelope";
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "immutable-envelope-operation";
  entry.effect_id = "immutable-envelope-effect";
  entry.effect_key = "immutable-envelope-effect-key";
  entry.payload_digest = "sha256:immutable-envelope-payload";
  entry.causation_id = "command-immutable-1";
  entry.kind = "http";
  entry.schema_version = "v1";
  entry.destination = "https://example.invalid/immutable-envelope";
  entry.content_type = "text/plain";
  entry.headers_json = "{\"x-request-id\":\"first\"}";
  entry.trace_context = "trace-first";
  payload = NULL;
  conflicting_payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  entry.content_type = "application/json";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.content_type = "text/plain";

  entry.headers_json = "{\"x-request-id\":\"second\"}";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.headers_json = "{\"x-request-id\":\"first\"}";

  entry.causation_id = "command-immutable-2";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_SERVER);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.causation_id = "command-immutable-1";

  entry.schema_version = "v2";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_SERVER);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.schema_version = "v1";

  entry.trace_context = "trace-second";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);

  entry.trace_context = "trace-first";
  entry.payload_digest = "sha256:immutable-envelope-changed-payload";
  assert_int_equal(
      lc_source_from_memory("changed", 7U, &conflicting_payload, &error),
      LC_OK);
  assert_int_equal(lc_outbox_append(outbox, &entry, conflicting_payload,
                                    &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);
  lc_source_close(conflicting_payload);
  conflicting_payload = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

/* A stateful claim is intentionally an opt-in paired handoff. Its checkpoint
 * survives a terminal dead-letter/replay cycle, but a completed effect has no
 * future delivery that could consume it and must reclaim it before the parent
 * job is released. */
static void outbox_assert_marker_presence(lc_client *client, const char *ns,
                                          int kind, const char *name,
                                          int present) {
  lc_pouch *pouch = ((lc_client_handle *)client)->pouch;
  lc_pouch_outbox_store store = {0};
  unsigned char *bytes = NULL;
  size_t length = 0U;
  lc_error error;
  int rc;
  lc_error_init(&error);
  assert_int_equal(
      lc_pouch_outbox_store_open(pouch, ns, kind, 0, &store, &error), LC_OK);
  rc = store.ops->read(store.context, name, &pouch->allocator, 1U,
                       LC_POUCH_OUTBOX_MARKER_MAX_BYTES, &bytes, &length,
                       &error);
  assert_int_equal(rc, present ? LC_OK : LC_POUCH_OUTBOX_MARKER_MISSING);
  assert_int_equal(bytes != NULL, present);
  lc_free_with_allocator(&pouch->allocator, bytes);
  assert_int_equal(lc_pouch_outbox_store_close(&store, LC_OK, &error), LC_OK);
  lc_error_cleanup(&error);
}
static void outbox_assert_stateful_remote_rejected(void) {
  const char *endpoint = "http://127.0.0.1:1";
  lc_client_config config;
  lc_outbox_config outbox_config;
  lc_client *client = NULL;
  lc_outbox *outbox = NULL;
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_job *job = (lc_outbox_job *)(uintptr_t)1;
  lc_error error;

  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = &endpoint;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  assert_int_equal(lc_client_open(&config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "stateful-rejection";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  assert_int_equal(
      lc_outbox_dispatcher_next_with_state(dispatcher, 0L, &job, &error),
      LC_ERR_INVALID);
  assert_null(job);
  assert_non_null(strstr(error.message, "requires Pouch"));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
}
static void test_pouch_outbox_stateful_delivery_lifecycle(void **state) {
  char root[256], template_path[256], endpoint[320], state_key[192],
      retried_state_key[192], deleted_state_key[192], crashed_state_key[192],
      reclaim_directory[384], deleted_state_marker_id[44],
      deleted_state_marker_path[512];
  const char *endpoints[1];
  const void *bytes;
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_outbox_job *job;
  lc_client *client;
  lc_outbox *outbox;
  lc_lease *state_lease;
  lc_source *payload;
  lc_source *checkpoint;
  lc_source *attachment;
  lc_sink *sink;
  lc_get_res get_result;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_outbox_retry retry;
  lc_remove_req state_remove;
  lc_attachment_list attachments;
  lc_attachment_list_req attachment_list;
  lc_pouch_state_read_result reclaim_result;
  lc_acquire_req inspect_acquire;
  lc_lease *inspect_lease;
  size_t length;
  outbox_fail_once terminal_cleanup_failure;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "stateful-lifecycle-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&terminal_cleanup_failure, 0, sizeof(terminal_cleanup_failure));
  checkpoint = NULL;
  attachment = NULL;
  sink = NULL;
  inspect_lease = NULL;
  memset(&get_result, 0, sizeof(get_result));
  memset(&reclaim_result, 0, sizeof(reclaim_result));
  memset(&attach_result, 0, sizeof(attach_result));
  memset(&attachments, 0, sizeof(attachments));
  lc_attachment_list_req_init(&attachment_list);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-stateful-lifecycle";
  outbox_config.owner = "outbox-stateful-lifecycle-owner";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  /* Stateful delivery is deliberately Pouch-only. It must reject before any
   * wait or claim, and must never silently produce a stateless job. */
  outbox_assert_stateful_remote_rejected();
  /* A normal non-query probe remains entirely stateless: it must not even
   * create the private crash-reclaim marker directory. */
  assert_int_equal(outbox_test_next(outbox, 0L, &job, &error), LC_OK);
  assert_null(job);
  assert_true(snprintf(reclaim_directory, sizeof(reclaim_directory),
                       "%s/.lockd/outbox-state-reclaim", root) > 0);
  if (!((lc_client_handle *)client)->pouch->storage.ops) {
    errno = 0;
    assert_int_equal(access(reclaim_directory, F_OK), -1);
    assert_int_equal(errno, ENOENT);
  }
  lc_outbox_entry_init(&entry);
  entry.operation_id = "stateful-lifecycle-operation";
  entry.effect_id = "stateful-lifecycle-effect";
  entry.effect_key = "stateful-lifecycle-idempotency";
  entry.payload_digest = "sha256:stateful-lifecycle";
  entry.kind = "test";
  entry.destination = "stateful://lifecycle";
  entry.content_type = "application/json";
  assert_int_equal(lc_source_from_memory("{}", 2U, &payload, &error), LC_OK);
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;
  assert_non_null(receipt.outbox_key);

  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  assert_ptr_equal(job->state(job), state_lease);
  assert_true(snprintf(state_key, sizeof(state_key), "%s", state_lease->key) >
              0);
  assert_true(strncmp(state_key, "__lockdc_io/v1/outbox-state/",
                      sizeof("__lockdc_io/v1/outbox-state/") - 1U) == 0);
  /* The checkpoint-presence bit is committed on the parent before the host
   * can mutate the independently durable sidecar. A close/expiry therefore
   * cannot erase cleanup knowledge for a later stateless claimant. */
  assert_int_equal(lc_pouch_state_read(((lc_client_handle *)client)->pouch,
                                       outbox_config.ns, receipt.outbox_key,
                                       &reclaim_result, &error),
                   LC_OK);
  assert_true(reclaim_result.found);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_copy(reclaim_result.body, sink, NULL, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length, "\"checkpoint_present\":1"));
  lc_sink_close(sink);
  sink = NULL;
  lc_pouch_state_read_result_cleanup(&((lc_client_handle *)client)->allocator,
                                     &reclaim_result);
  memset(&reclaim_result, 0, sizeof(reclaim_result));
  assert_int_equal(
      lc_source_from_memory("{\"checkpoint\":1}", 16U, &checkpoint, &error),
      LC_OK);
  assert_int_equal(lc_lease_update(state_lease, checkpoint, NULL, &error),
                   LC_OK);
  lc_source_close(checkpoint);
  checkpoint = NULL;
  lc_attach_req_init(&attach);
  attach.name = "checkpoint.txt";
  attach.content_type = "text/plain";
  assert_int_equal(
      lc_source_from_memory("checkpoint attachment", 21U, &attachment, &error),
      LC_OK);
  assert_int_equal(
      lc_lease_attach(state_lease, &attach, attachment, &attach_result, &error),
      LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(attachment);
  attachment = NULL;
  assert_int_equal(lc_outbox_job_dead_letter(job, "exercise replay", &error),
                   LC_OK);
  job = NULL;

  assert_int_equal(
      outbox_test_replay_dead_letter(outbox, receipt.outbox_key, &error),
      LC_OK);
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(state_lease, sink, NULL, &get_result, &error),
                   LC_OK);
  assert_false(get_result.no_content);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_int_equal(length, 16U);
  assert_memory_equal(bytes, "{\"checkpoint\":1}", length);
  assert_int_equal(lc_lease_list_attachments(state_lease, &attachments, &error),
                   LC_OK);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "checkpoint.txt");
  lc_attachment_list_cleanup(&attachments);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  sink = NULL;
  /* Removing a state body is a valid host operation. Its private attachment
   * still belongs to this terminal delivery, so complete() must use the
   * durable checkpoint path rather than revalidating the removed state lease.
   */
  lc_remove_req_init(&state_remove);
  assert_int_equal(lc_lease_remove(state_lease, &state_remove, &error), LC_OK);
  /* The terminal parent is committed before irreversible cleanup. A cleanup
   * failure therefore still completes the delivery and retains its durable
   * exact-key marker for a later bounded stateful reclaim. */
  lc_outbox_test_before_stateful_terminal_cleanup_hook = outbox_fail_first_call;
  lc_outbox_test_before_stateful_terminal_cleanup_context =
      &terminal_cleanup_failure;
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(terminal_cleanup_failure.calls, 1U);
  lc_outbox_test_before_stateful_terminal_cleanup_hook = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_context = NULL;
  /* A cleanup marker survives a completed parent if its immediate exact-key
   * reclaim is interrupted. The next stateful pull retires it before handing
   * out unrelated work, without a namespace scan. */
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "stateful-crash-cleanup-effect";
  entry.effect_key = "stateful-crash-cleanup-idempotency";
  entry.payload_digest = "sha256:stateful-crash-cleanup";
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  assert_true(snprintf(crashed_state_key, sizeof(crashed_state_key), "%s",
                       state_lease->key) > 0);
  assert_int_equal(
      lc_source_from_memory("{\"checkpoint\":3}", 16U, &checkpoint, &error),
      LC_OK);
  assert_int_equal(lc_lease_update(state_lease, checkpoint, NULL, &error),
                   LC_OK);
  lc_source_close(checkpoint);
  checkpoint = NULL;
  lc_attach_req_init(&attach);
  attach.name = "crash-checkpoint.txt";
  attach.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("crash checkpoint attachment", 27U,
                                         &attachment, &error),
                   LC_OK);
  assert_int_equal(
      lc_lease_attach(state_lease, &attach, attachment, &attach_result, &error),
      LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(attachment);
  attachment = NULL;
  memset(&terminal_cleanup_failure, 0, sizeof(terminal_cleanup_failure));
  lc_outbox_test_before_stateful_terminal_cleanup_hook = outbox_fail_first_call;
  lc_outbox_test_before_stateful_terminal_cleanup_context =
      &terminal_cleanup_failure;
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_test_before_stateful_terminal_cleanup_hook = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_context = NULL;
  job = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "stateful-crash-cleanup-trigger-effect";
  entry.effect_key = "stateful-crash-cleanup-trigger-idempotency";
  entry.payload_digest = "sha256:stateful-crash-cleanup-trigger";
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key,
                      "stateful-crash-cleanup-trigger-idempotency");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_pouch_state_read(((lc_client_handle *)client)->pouch,
                                       outbox_config.ns, crashed_state_key,
                                       &reclaim_result, &error),
                   LC_OK);
  assert_false(reclaim_result.found);
  lc_pouch_state_read_result_cleanup(&((lc_client_handle *)client)->allocator,
                                     &reclaim_result);
  memset(&reclaim_result, 0, sizeof(reclaim_result));
  attachment_list.lease.ns = outbox_config.ns;
  attachment_list.lease.key = crashed_state_key;
  attachment_list.public_read = 1;
  assert_int_equal(
      client->list_attachments(client, &attachment_list, &attachments, &error),
      LC_OK);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);
  lc_attachment_list_req_init(&attachment_list);

  /* A checkpoint survives a retry, but the next claimant is free to use the
   * ordinary API. That stateless completion must still retire the exact
   * stateful sidecar rather than leaving durable checkpoint data behind. */
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "stateful-retry-stateless-complete-effect";
  entry.effect_key = "stateful-retry-stateless-complete-idempotency";
  entry.payload_digest = "sha256:stateful-retry-stateless-complete";
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  assert_true(snprintf(retried_state_key, sizeof(retried_state_key), "%s",
                       state_lease->key) > 0);
  assert_int_equal(
      lc_source_from_memory("{\"checkpoint\":4}", 16U, &checkpoint, &error),
      LC_OK);
  assert_int_equal(lc_lease_update(state_lease, checkpoint, NULL, &error),
                   LC_OK);
  lc_source_close(checkpoint);
  checkpoint = NULL;
  lc_outbox_retry_init(&retry);
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  job = NULL;
  assert_int_equal(outbox_test_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_null(lc_outbox_job_state(job));
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_acquire_req_init(&inspect_acquire);
  inspect_acquire.ns = outbox_config.ns;
  inspect_acquire.key = retried_state_key;
  inspect_acquire.owner = "outbox-stateful-retry-stateless-inspect";
  inspect_acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &inspect_acquire, &inspect_lease, &error),
                   LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_lease_get(inspect_lease, sink, NULL, &get_result, &error),
                   LC_OK);
  assert_true(get_result.no_content);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  sink = NULL;
  assert_int_equal(lc_lease_release(inspect_lease, NULL, &error), LC_OK);
  inspect_lease = NULL;

  /* An ordinary pull stays stateless even after this dispatcher has delivered
   * a stateful job. It must not take the optional paired-claim branch. */
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "stateless-after-stateful-effect";
  entry.effect_key = "stateless-after-stateful-idempotency";
  entry.payload_digest = "sha256:stateless-after-stateful";
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(outbox_test_next(outbox, 0L, &job, &error), LC_OK);
  if (job == NULL) {
    assert_int_equal(outbox_test_next(outbox, 3000L, &job, &error), LC_OK);
  }
  assert_non_null(job);
  assert_null(lc_outbox_job_state(job));
  assert_null(job->state(job));
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  lc_acquire_req_init(&inspect_acquire);
  inspect_acquire.ns = outbox_config.ns;
  inspect_acquire.key = state_key;
  inspect_acquire.owner = "outbox-stateful-lifecycle-inspect";
  inspect_acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &inspect_acquire, &inspect_lease, &error),
                   LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_lease_get(inspect_lease, sink, NULL, &get_result, &error),
                   LC_OK);
  assert_true(get_result.no_content);
  assert_int_equal(
      lc_lease_list_attachments(inspect_lease, &attachments, &error), LC_OK);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  sink = NULL;
  assert_int_equal(lc_lease_release(inspect_lease, NULL, &error), LC_OK);
  inspect_lease = NULL;

  /* Deleting a dead letter is a permanent management action. Its checkpoint
   * must be removed before the envelope is consumed, otherwise a future
   * recreated effect could inherit a stale opaque host checkpoint. */
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "stateful-dead-letter-delete-effect";
  entry.effect_key = "stateful-dead-letter-delete-idempotency";
  entry.payload_digest = "sha256:stateful-dead-letter-delete";
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  assert_true(snprintf(deleted_state_key, sizeof(deleted_state_key), "%s",
                       state_lease->key) > 0);
  assert_int_equal(
      lc_source_from_memory("{\"checkpoint\":2}", 16U, &checkpoint, &error),
      LC_OK);
  assert_int_equal(lc_lease_update(state_lease, checkpoint, NULL, &error),
                   LC_OK);
  lc_source_close(checkpoint);
  checkpoint = NULL;
  lc_attach_req_init(&attach);
  attach.name = "delete-checkpoint.txt";
  attach.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("delete checkpoint attachment", 26U,
                                         &attachment, &error),
                   LC_OK);
  assert_int_equal(
      lc_lease_attach(state_lease, &attach, attachment, &attach_result, &error),
      LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(attachment);
  attachment = NULL;
  assert_int_equal(lc_outbox_job_dead_letter(job, "delete checkpoint", &error),
                   LC_OK);
  job = NULL;
  /* An interrupted stateful completion can leave its exact-key cleanup marker
   * while management subsequently removes the dead letter. Delete that marker
   * first, so later stateful pulls cannot be blocked by orphaned control data.
   */
  assert_int_equal(lc_outbox_test_state_reclaim_marker_id(
                       receipt.outbox_key, deleted_state_marker_id, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_outbox_state_reclaim_marker_write(
                       ((lc_client_handle *)client)->pouch, outbox_config.ns,
                       deleted_state_marker_id, receipt.outbox_key,
                       deleted_state_key, &error),
                   LC_OK);
  assert_true(snprintf(deleted_state_marker_path,
                       sizeof(deleted_state_marker_path),
                       "%s/.lockd/outbox-state-reclaim/%s/%s", root,
                       outbox_config.ns, deleted_state_marker_id) > 0);
  outbox_assert_marker_presence(client, outbox_config.ns, -1,
                                deleted_state_marker_id, 1);
  if (!((lc_client_handle *)client)->pouch->storage.ops)
    assert_int_equal(access(deleted_state_marker_path, F_OK), 0);
  assert_int_equal(
      outbox_test_delete_dead_letter(outbox, receipt.outbox_key, &error),
      LC_OK);
  outbox_assert_marker_presence(client, outbox_config.ns, -1,
                                deleted_state_marker_id, 0);
  if (!((lc_client_handle *)client)->pouch->storage.ops) {
    errno = 0;
    assert_int_equal(access(deleted_state_marker_path, F_OK), -1);
    assert_int_equal(errno, ENOENT);
  }
  lc_acquire_req_init(&inspect_acquire);
  inspect_acquire.ns = outbox_config.ns;
  inspect_acquire.key = deleted_state_key;
  inspect_acquire.owner = "outbox-stateful-delete-inspect";
  inspect_acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &inspect_acquire, &inspect_lease, &error),
                   LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_lease_get(inspect_lease, sink, NULL, &get_result, &error),
                   LC_OK);
  assert_true(get_result.no_content);
  assert_int_equal(
      lc_lease_list_attachments(inspect_lease, &attachments, &error), LC_OK);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  sink = NULL;
  assert_int_equal(lc_lease_release(inspect_lease, NULL, &error), LC_OK);
  inspect_lease = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

/* Renewals must preserve the current parent and checkpoint CAS snapshots,
 * including an immediate parent write newer than its lease metadata. */
static void outbox_assert_stored_body(lc_client *client, const char *key,
                                      const char *expected) {
  lc_pouch_state_read_result result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  lc_error error;

  memset(&result, 0, sizeof(result));
  lc_error_init(&error);
  assert_int_equal(lc_pouch_state_read(((lc_client_handle *)client)->pouch,
                                       "outbox-renewal", key, &result, &error),
                   LC_OK);
  assert_int_equal(result.found, expected != NULL);
  if (expected != NULL) {
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(lc_copy(result.body, sink, NULL, &error), LC_OK);
    assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error),
                     LC_OK);
    assert_true(outbox_bytes_contains(bytes, length, expected));
    lc_sink_close(sink);
  }
  lc_pouch_state_read_result_cleanup(&((lc_client_handle *)client)->allocator,
                                     &result);
  lc_error_cleanup(&error);
}

static void outbox_worker_release_gate(void *context);

static void outbox_renewal_terminal_case(int shared, int sequence, int terminal,
                                         int failure) {
  char root[256], template_path[256], endpoint[320], checkpoint_key[192];
  char marker_directory[384];
  const char *endpoints[1];
  lc_client_config config;
  lc_pouch_settings settings;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_lease *checkpoint;
  lc_source *source;
  lc_update_opts options;
  lc_outbox_retry retry;
  outbox_pouch_test_clock clock;
  pthread_barrier_t gates[2];
  FILE *blocker;
  lc_error error;
  int step, rc;

  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "renewal-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_SINGLE_WRITER |
                      LC_POUCH_SETTING_BACKGROUND_COMPACTION |
                      LC_POUCH_SETTING_QUERY_INDEXING;
  settings.single_writer = !shared;
  config.pouch_settings = &settings;
  assert_int_equal(lc_client_open(&config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-renewal";
  outbox_config.claim_ttl_seconds = 30L;
  assert_int_equal(outbox_public_new(client, &outbox_config, &outbox, &error),
                   LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "renewal";
  entry.effect_id = "renewal";
  entry.effect_key = "renewal";
  entry.kind = "test";
  entry.destination = "local";
  entry.payload_digest = "sha256:renewal";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("{}", 2U, &source, &error), LC_OK);
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, source,
                                             &transaction, &receipt, &error),
                   LC_OK);
  lc_source_close(source);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  assert_int_equal(outbox_public_get_or_start(outbox, &dispatcher, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_dispatcher_notify_outbox_key(
                       dispatcher, receipt.outbox_key, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_dispatcher_next_with_state(dispatcher, 0L, &job, &error),
      LC_OK);
  assert_non_null(job);
  if (failure != 0) {
    /* Stop private storage work before installing process-wide seams. The
     * handed-out job stays usable while the worker waits at its release gate.
     */
    assert_int_equal(pthread_barrier_init(&gates[0], NULL, 2U), 0);
    assert_int_equal(pthread_barrier_init(&gates[1], NULL, 2U), 0);
    lc_outbox_test_before_worker_release_context = gates;
    lc_outbox_test_before_worker_release_hook = outbox_worker_release_gate;
    assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, 0L, &error),
                     LC_ERR_TIMEOUT);
    (void)pthread_barrier_wait(&gates[0]);
    lc_error_cleanup(&error);
    lc_error_init(&error);
  }
  checkpoint = lc_outbox_job_state(job);
  assert_non_null(checkpoint);
  assert_true(snprintf(checkpoint_key, sizeof(checkpoint_key), "%s",
                       checkpoint->key) > 0);
  for (step = 0; step < 2; ++step) {
    if (sequence == 0 || sequence == 2)
      assert_int_equal(lc_outbox_job_renew(job, 30L, &error), LC_OK);
    assert_int_equal(
        lc_source_from_memory(step ? "{\"seen\":2}" : "{\"seen\":1}", 10U,
                              &source, &error),
        LC_OK);
    assert_int_equal(lc_lease_update(checkpoint, source, NULL, &error), LC_OK);
    lc_source_close(source);
    if (sequence == 1 || sequence == 2)
      assert_int_equal(lc_outbox_job_renew(job, 30L, &error), LC_OK);
    outbox_assert_stored_body(client, checkpoint_key,
                              step ? "{\"seen\":2}" : "{\"seen\":1}");
  }
  /* Renewal must not weaken an explicitly stale checkpoint precondition. */
  lc_update_opts_init(&options);
  options.has_if_version = 1;
  options.if_version = checkpoint->version - 1L;
  assert_int_equal(lc_source_from_memory("{}", 2U, &source, &error), LC_OK);
  assert_int_not_equal(lc_lease_update(checkpoint, source, &options, &error),
                       LC_OK);
  lc_source_close(source);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  if (failure == 1) {
    /* A failed cleanup-marker creation precedes the parent decision. Neither
     * completion nor checkpoint deletion may escape, and retry must work. */
    assert_true(snprintf(marker_directory, sizeof(marker_directory),
                         "%s/.lockd", root) > 0);
    rc = mkdir(marker_directory, 0700);
    assert_true(rc == 0 || errno == EEXIST);
    assert_true(snprintf(marker_directory, sizeof(marker_directory),
                         "%s/.lockd/outbox-state-reclaim", root) > 0);
    rc = mkdir(marker_directory, 0700);
    assert_true(rc == 0 || errno == EEXIST);
    assert_true(snprintf(marker_directory, sizeof(marker_directory),
                         "%s/.lockd/outbox-state-reclaim/outbox-renewal",
                         root) > 0);
    blocker = fopen(marker_directory, "wb");
    assert_non_null(blocker);
    assert_int_equal(fclose(blocker), 0);
    rc = lc_outbox_job_complete(job, NULL, &error);
    assert_int_equal(unlink(marker_directory), 0);
    assert_int_not_equal(rc, LC_OK);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    outbox_assert_stored_body(client, receipt.outbox_key,
                              "\"dispatch_state\":\"claimed\"");
    outbox_assert_stored_body(client, checkpoint_key, "{\"seen\":2}");
  } else if (failure == 2) {
    clock.wall.tv_sec = (time_t)job->lease_expires_at_unix + 1;
    clock.wall.tv_nsec = 0L;
    lc_pouch_test_queue_time_context = &clock;
    lc_pouch_test_queue_clock_gettime = outbox_pouch_test_gettime;
    rc = lc_outbox_job_renew(job, 30L, &error);
    assert_int_not_equal(rc, LC_OK);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    rc = lc_outbox_job_complete(job, NULL, &error);
    lc_pouch_test_queue_clock_gettime = NULL;
    lc_pouch_test_queue_time_context = NULL;
    assert_int_not_equal(rc, LC_OK);
    outbox_assert_stored_body(client, receipt.outbox_key,
                              "\"dispatch_state\":\"claimed\"");
    outbox_assert_stored_body(client, checkpoint_key, "{\"seen\":2}");
    lc_outbox_job_close(job);
    goto cleanup;
  }
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 30L;
  if (terminal == 0)
    rc = lc_outbox_job_complete(job, NULL, &error);
  else if (terminal == 1)
    rc = lc_outbox_job_retry(job, &retry, &error);
  else
    rc = lc_outbox_job_dead_letter(job, "renewed", &error);
  assert_int_equal(rc, LC_OK);
  outbox_assert_stored_body(client, receipt.outbox_key,
                            terminal == 0 ? "\"dispatch_state\":\"completed\""
                            : terminal == 1
                                ? "\"dispatch_state\":\"retry_wait\""
                                : "\"dispatch_state\":\"dead_letter\"");
  outbox_assert_stored_body(client, checkpoint_key,
                            terminal == 0 ? NULL : "{\"seen\":2}");
cleanup:
  if (failure != 0)
    (void)pthread_barrier_wait(&gates[1]);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  if (failure != 0) {
    lc_outbox_test_before_worker_release_hook = NULL;
    lc_outbox_test_before_worker_release_context = NULL;
    assert_int_equal(pthread_barrier_destroy(&gates[0]), 0);
    assert_int_equal(pthread_barrier_destroy(&gates[1]), 0);
  }
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_outbox_receipt_cleanup(&receipt);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_stateful_renewal_terminal_versions(void **state) {
  int shared, sequence, terminal;
  (void)state;
  for (shared = 0; shared < 2; ++shared)
    for (sequence = 0; sequence < 3; ++sequence)
      for (terminal = 0; terminal < 3; ++terminal)
        outbox_renewal_terminal_case(shared, sequence, terminal, 0);
}

static void test_pouch_outbox_stateful_renewal_terminal_failure(void **state) {
  int shared, failure;
  (void)state;
  for (shared = 0; shared < 2; ++shared)
    for (failure = 1; failure < 3; ++failure)
      outbox_renewal_terminal_case(shared, 2, 0, failure);
}

/* If the paired state handoff cannot be constructed, the failed internal
 * attempt returns the envelope to pending without spending an attempt. The
 * same blocking demand may then make one complete paired handoff, but it must
 * never expose the partial job. */
static void
test_pouch_outbox_stateful_handoff_failure_rolls_back(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_outbox_job *job;
  lc_client *client;
  lc_outbox *outbox;
  lc_source *payload;
  outbox_fail_once failure;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "stateful-handoff-failure-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&failure, 0, sizeof(failure));
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-stateful-handoff-failure";
  outbox_config.owner = "outbox-stateful-handoff-failure-owner";
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "stateful-handoff-failure-operation";
  entry.effect_id = "stateful-handoff-failure-effect";
  entry.effect_key = "stateful-handoff-failure-idempotency";
  entry.payload_digest = "sha256:stateful-handoff-failure";
  entry.kind = "test";
  entry.destination = "stateful://handoff-failure";
  assert_int_equal(lc_source_from_memory("{}", 2U, &payload, &error), LC_OK);
  assert_int_equal(outbox_test_outbox_append(outbox, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(outbox_test_commit(transaction, &error), LC_OK);
  outbox_test_transaction_close(transaction);
  transaction = NULL;

  lc_outbox_test_before_stateful_handoff_hook = outbox_fail_first_call;
  lc_outbox_test_before_stateful_handoff_context = &failure;
  /* The blocking demand performs the initial durable discovery. It compensates
   * the first failed handoff, then receives only a fully paired replacement. */
  assert_int_equal(outbox_test_next_with_state(outbox, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_int_equal(failure.calls, 2U);
  lc_outbox_test_before_stateful_handoff_hook = NULL;
  lc_outbox_test_before_stateful_handoff_context = NULL;
  assert_int_equal(job->attempt, 1);
  assert_non_null(lc_outbox_job_state(job));
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_transaction_and_duplicate(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_transaction *duplicate_transaction;
  lc_outbox_participant *participant;
  lc_outbox_receipt receipt;
  lc_outbox_receipt duplicate_receipt;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_transaction *inbox_transaction;
  lc_outbox_job *job;
  lc_acquire_req duplicate_barrier_acquire;
  lc_release_req duplicate_barrier_release;
  lc_lease *duplicate_barrier_lease;
  lc_acquire_req domain_read_acquire;
  lc_release_req domain_read_release;
  lc_lease *domain_read_lease;
  lc_get_opts domain_read_options;
  lc_get_res domain_read_result;
  lc_source *payload;
  lc_source *state_source;
  lc_sink *payload_sink;
  const void *payload_bytes;
  size_t payload_length;
  size_t payload_written;
  lc_error error;
  char long_consumer_id[256];

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "core-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  memset(long_consumer_id, 'c', sizeof(long_consumer_id) - 1U);
  long_consumer_id[sizeof(long_consumer_id) - 1U] = '\0';
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox";
  outbox_config.owner = "outbox-test";
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "operation-1";
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  entry.payload_digest = "sha256:outbox-payload";
  entry.kind = "http";
  entry.destination = "https://example.invalid/effect?target=\"primary\"";
  entry.content_type = "text/plain";
  entry.trace_context = "trace-\"quoted\"";
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  /* Fresh keys become observable only in the durable commit result. */
  assert_null(receipt.outbox_key);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "domain";
  participant_request.acquire.key = "order-1";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  participant = NULL;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  assert_non_null(participant->txn_id);
  state_source = NULL;
  assert_int_equal(lc_source_from_memory("{\"status\":\"paid\"}", 17U,
                                         &state_source, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_participant_update(participant, state_source, NULL, &error),
      LC_OK);
  lc_source_close(state_source);
  lc_outbox_participant_close(participant);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);

  /* A lazy, domain-first transaction must still establish a durable XA
   * decision. It used to stage its update under Pouch's single-lease implicit
   * xid and then report commit without publishing the state. */
  transaction = NULL;
  assert_int_equal(lc_outbox_begin(outbox, &transaction, &error), LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "domain";
  participant_request.acquire.key = "order-domain-first";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  participant = NULL;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant->txn_id);
  state_source = NULL;
  assert_int_equal(lc_source_from_memory("{\"status\":\"created\"}", 20U,
                                         &state_source, &error),
                   LC_OK);
  assert_int_equal(participant->update(participant, state_source, NULL, &error),
                   LC_OK);
  lc_source_close(state_source);
  lc_outbox_participant_close(participant);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_acquire_req_init(&domain_read_acquire);
  domain_read_acquire.ns = "domain";
  domain_read_acquire.key = "order-domain-first";
  domain_read_acquire.owner = "domain-reader";
  domain_read_acquire.ttl_seconds = 30L;
  domain_read_lease = NULL;
  assert_int_equal(
      lc_acquire(client, &domain_read_acquire, &domain_read_lease, &error),
      LC_OK);
  lc_get_opts_init(&domain_read_options);
  memset(&domain_read_result, 0, sizeof(domain_read_result));
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  assert_int_equal(lc_sink_to_memory(&payload_sink, &error), LC_OK);
  assert_int_equal(domain_read_lease->get(domain_read_lease, payload_sink,
                                          &domain_read_options,
                                          &domain_read_result, &error),
                   LC_OK);
  assert_false(domain_read_result.no_content);
  assert_int_equal(lc_sink_memory_bytes(payload_sink, &payload_bytes,
                                        &payload_length, &error),
                   LC_OK);
  assert_int_equal(payload_length, 20U);
  assert_memory_equal(payload_bytes, "{\"status\":\"created\"}", 20U);
  lc_get_res_cleanup(&domain_read_result);
  lc_sink_close(payload_sink);
  lc_release_req_init(&domain_read_release);
  domain_read_release.rollback = 1;
  assert_int_equal(
      lc_lease_release(domain_read_lease, &domain_read_release, &error), LC_OK);
  domain_read_lease = NULL;

  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_string_equal(job->destination, entry.destination);
  assert_int_equal(job->attempt, 1);
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  payload_written = 0U;
  assert_int_equal(lc_sink_to_memory(&payload_sink, &error), LC_OK);
  assert_int_equal(
      lc_outbox_job_write_payload(job, payload_sink, &payload_written, &error),
      LC_OK);
  assert_int_equal(lc_sink_memory_bytes(payload_sink, &payload_bytes,
                                        &payload_length, &error),
                   LC_OK);
  assert_int_equal(payload_written, 7U);
  assert_int_equal(payload_length, 7U);
  assert_memory_equal(payload_bytes, "payload", 7U);
  lc_sink_close(payload_sink);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&duplicate_receipt);
  lc_acquire_req_init(&duplicate_barrier_acquire);
  duplicate_barrier_acquire.ns = outbox_config.ns;
  duplicate_barrier_acquire.key = receipt.outbox_key;
  duplicate_barrier_acquire.owner = "duplicate-barrier-holder";
  duplicate_barrier_acquire.ttl_seconds = 30L;
  duplicate_barrier_lease = NULL;
  assert_int_equal(lc_acquire(client, &duplicate_barrier_acquire,
                              &duplicate_barrier_lease, &error),
                   LC_OK);
  duplicate_transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_append(outbox, &entry, payload,
                                    &duplicate_transaction, &duplicate_receipt,
                                    &error),
                   LC_ERR_INVALID);
  assert_null(duplicate_transaction);
  assert_false(duplicate_receipt.duplicate);
  assert_null(duplicate_receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_release_req_init(&duplicate_barrier_release);
  duplicate_barrier_release.rollback = 1;
  assert_int_equal(lc_lease_release(duplicate_barrier_lease,
                                    &duplicate_barrier_release, &error),
                   LC_OK);
  duplicate_barrier_lease = NULL;
  duplicate_transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_append(outbox, &entry, payload,
                                    &duplicate_transaction, &duplicate_receipt,
                                    &error),
                   LC_OK);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_string_equal(receipt.outbox_key, duplicate_receipt.outbox_key);
  lc_source_close(payload);
  payload = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "effect-retry";
  entry.effect_key = "foreign-idempotency-retry";
  entry.payload_digest = "sha256:outbox-retry-payload";
  assert_int_equal(
      lc_source_from_memory("retry-payload", 13U, &payload, &error), LC_OK);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 3601L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "foreign-idempotency-retry");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  payload = NULL;
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  entry.payload_digest = "sha256:outbox-payload";
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = long_consumer_id;
  inbox.source_kind = "http";
  inbox.source_id = "gateway";
  inbox.message_id = "message-1";
  inbox.payload_digest = "abc123";
  inbox.operation_id = "operation-inbox-1";
  inbox_transaction = NULL;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &inbox_transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_non_null(inbox_transaction);
  assert_int_equal(lc_outbox_transaction_commit(inbox_transaction, &error),
                   LC_OK);
  lc_outbox_transaction_close(inbox_transaction);
  inbox_transaction = (lc_outbox_transaction *)1;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &inbox_transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_null(inbox_transaction);
  assert_true(inbox_result.duplicate);
  inbox.operation_id = "operation-inbox-conflict";
  inbox_transaction = (lc_outbox_transaction *)1;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &inbox_transaction,
                                          &inbox_result, &error),
                   LC_ERR_SERVER);
  assert_null(inbox_transaction);
  assert_false(inbox_result.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_duplicate_inbox_handles_json_runtime_failure(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_inbox_runtime_failure attempt;
  lc_inbox_message message;
  lc_inbox_accept_result result;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_error error;
  pthread_t thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "json-runtime-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  memset(&attempt, 0, sizeof(attempt));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-json-runtime";
  outbox_config.owner = "outbox-json-runtime-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&message);
  message.consumer_id = "json-runtime-consumer";
  message.source_kind = "http";
  message.source_id = "json-runtime-gateway";
  message.message_id = "json-runtime-message";
  message.payload_digest = "json-runtime-digest";
  message.operation_id = "json-runtime-operation";
  memset(&result, 0, sizeof(result));
  assert_int_equal(
      lc_outbox_accept_inbox(outbox, &message, &transaction, &result, &error),
      LC_OK);
  assert_true(result.accepted);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  /* A duplicate on a fresh host thread must report the Pouch JSON-runtime
   * allocation failure safely. Previously the duplicate path could dereference
   * that missing runtime while decoding and cleaning its leased record. */
  attempt.outbox = outbox;
  attempt.message = message;
  assert_int_equal(pthread_create(&thread, NULL,
                                  outbox_duplicate_inbox_without_json_runtime,
                                  &attempt),
                   0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(attempt.rc, LC_ERR_NOMEM);
  assert_int_equal(attempt.error_code, LC_ERR_NOMEM);
  assert_null(attempt.transaction);
  assert_false(attempt.result.accepted);
  assert_false(attempt.result.duplicate);

  /* An acquire failure for an absent create-only key is not a duplicate. The
   * duplicate probe must preserve the retryable error rather than acquiring a
   * second lease and diagnosing an invented immutable record. */
  attempt.message.message_id = "json-runtime-absent-message";
  assert_int_equal(pthread_create(&thread, NULL,
                                  outbox_duplicate_inbox_without_json_runtime,
                                  &attempt),
                   0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(attempt.rc, LC_ERR_NOMEM);
  assert_int_equal(attempt.error_code, LC_ERR_NOMEM);
  assert_null(attempt.transaction);
  assert_false(attempt.result.accepted);
  assert_false(attempt.result.duplicate);

  memset(&result, 0, sizeof(result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &attempt.message,
                                          &transaction, &result, &error),
                   LC_OK);
  assert_true(result.accepted);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  /* The one-shot failure does not change the durable duplicate barrier. */
  memset(&result, 0, sizeof(result));
  assert_int_equal(
      lc_outbox_accept_inbox(outbox, &message, &transaction, &result, &error),
      LC_OK);
  assert_null(transaction);
  assert_true(result.duplicate);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_participant_cleanup_after_transaction_close(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "participant-lifetime-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "participant-lifetime";
  outbox_config.owner = "participant-lifetime-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "participant-lifetime-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "participant-lifetime-source";
  inbox.message_id = "participant-lifetime-message";
  inbox.payload_digest = "participant-lifetime-digest";
  inbox.operation_id = "participant-lifetime-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "participant-lifetime";
  participant_request.acquire.key = "participant-lifetime-domain";
  participant_request.acquire.owner = "participant-lifetime-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  assert_string_equal(participant->key, "participant-lifetime-domain");
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);

  /* Terminal leases are owned by the transaction. Views must fail closed
   * rather than retain a pointer to a released lease. */
  assert_int_equal(participant->describe(participant, &error), LC_ERR_INVALID);
  assert_null(participant->key);
  assert_null(participant->txn_id);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  /* This is the normal cleanup order when a host retains a participant view:
   * close the outbox and transaction first, then close the view. */
  lc_outbox_close(outbox);
  outbox = NULL;
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(participant->describe(participant, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_participant_close(participant);
  participant = NULL;

  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_participant_allocation_failure_rolls_back_enrollment(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "participant-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "participant-oom";
  outbox_config.owner = "participant-oom-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "participant-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "participant-oom-source";
  inbox.message_id = "participant-oom-message";
  inbox.payload_digest = "participant-oom-digest";
  inbox.operation_id = "participant-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "participant-oom";
  participant_request.acquire.key = "participant-oom-domain";
  participant_request.acquire.owner = "participant-oom-test";
  participant_request.acquire.ttl_seconds = 30L;

  outbox_reset_allocation_failures();
  lc_outbox_test_before_participant_allocation_hook = outbox_fail_allocation;
  rc = lc_outbox_transaction_acquire(transaction, &participant_request,
                                     &participant, &error);
  outbox_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(participant);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  /* The implicit-XA rollback is terminal. A caller retries in a new outbox
   * transaction, never by committing the failed xid. */
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "participant-oom-retry-message";
  inbox.payload_digest = "participant-oom-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  lc_outbox_participant_close(participant);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_command_receipt_allocation_failure_rolls_back_enrollment(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-oom";
  outbox_config.owner = "command-oom-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "command-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "command-oom-source";
  inbox.message_id = "command-oom-message";
  inbox.payload_digest = "command-oom-digest";
  inbox.operation_id = "command-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "command-oom-scope";
  command.identity.command_type = "command-oom-type";
  command.identity.idempotency_key = "command-oom-key";
  command.request_digest = "command-oom-digest";
  command.operation_id = "command-oom-operation";

  outbox_reset_allocation_failures();
  lc_outbox_test_before_command_receipt_copy_hook = outbox_fail_allocation;
  rc = lc_outbox_transaction_accept_command(transaction, &command, &receipt,
                                            &error);
  outbox_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.command_id);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "command-oom-retry-message";
  inbox.payload_digest = "command-oom-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_accept_command(transaction, &command,
                                                        &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                 &receipt, &error),
                   LC_OK);
  assert_false(receipt.duplicate);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_command_terminal_load_failure_cleans_record(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-terminal-load-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-terminal-load";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "command-terminal-load-scope";
  command.identity.command_type = "command-terminal-load-type";
  command.identity.idempotency_key = "command-terminal-load-key";
  command.request_digest = "command-terminal-load-digest";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.result_code = "created";
  lc_outbox_test_after_command_terminal_load_hook = outbox_fail_allocation;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_NOMEM);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_allocation_failures_roll_back_enrollment(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_participant_request participant_request;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_source *payload;
  lc_error error;
  size_t index;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "outbox-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-oom";
  outbox_config.owner = "outbox-oom-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "outbox-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "outbox-oom-source";
  inbox.message_id = "outbox-oom-message";
  inbox.payload_digest = "outbox-oom-digest";
  inbox.operation_id = "outbox-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-oom";
  participant_request.acquire.owner = "outbox-oom-test";
  participant_request.acquire.ttl_seconds = 30L;
  for (index = 0U; index < 3U; ++index) {
    char key[64];

    assert_true(snprintf(key, sizeof(key), "outbox-oom-domain-%lu",
                         (unsigned long)index) > 0);
    participant_request.acquire.key = key;
    assert_int_equal(lc_outbox_transaction_acquire(transaction,
                                                   &participant_request,
                                                   &participant, &error),
                     LC_OK);
    lc_outbox_participant_close(participant);
    participant = NULL;
  }
  lc_outbox_entry_init(&entry);
  entry.operation_id = "outbox-oom-operation";
  entry.effect_id = "outbox-oom-ledger";
  entry.effect_key = "outbox-oom-ledger-effect";
  entry.payload_digest = "sha256:outbox-oom-payload";
  entry.kind = "test";
  entry.destination = "outbox://oom-ledger";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);

  /* The ledger grows after staging the outbox attachment. Its allocation
   * failure must roll that staged lease back, so the exact retry succeeds. */
  outbox_reset_allocation_failures();
  lc_outbox_test_before_ledger_append_hook = outbox_fail_allocation;
  rc = lc_outbox_transaction_append(transaction, &entry, payload, &receipt,
                                    &error);
  outbox_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.outbox_key);
  lc_source_close(payload);
  payload = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "outbox-oom-ledger-retry-message";
  inbox.payload_digest = "outbox-oom-ledger-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_append(transaction, &entry, payload,
                                                &receipt, &error),
                   LC_OK);
  lc_source_close(payload);
  payload = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  /* The receipt copy happens after the outbox is enrolled. Failing it must
   * likewise remove the staged effect before the caller retries. */
  inbox.message_id = "outbox-oom-receipt-message";
  inbox.payload_digest = "outbox-oom-receipt-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  entry.effect_id = "outbox-oom-receipt";
  entry.effect_key = "outbox-oom-receipt-effect";
  entry.destination = "outbox://oom-receipt";
  assert_int_equal(lc_source_from_memory("receipt", 7U, &payload, &error),
                   LC_OK);
  outbox_reset_allocation_failures();
  lc_outbox_test_before_outbox_receipt_copy_hook = outbox_fail_allocation;
  rc = lc_outbox_transaction_append(transaction, &entry, payload, &receipt,
                                    &error);
  outbox_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.outbox_key);
  lc_source_close(payload);
  payload = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "outbox-oom-receipt-retry-message";
  inbox.payload_digest = "outbox-oom-receipt-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("receipt", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_append(transaction, &entry, payload,
                                                &receipt, &error),
                   LC_OK);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_transaction_close(transaction);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_notification_allocation_failure_reconciles_committed_outbox(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "notify-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "notify-oom";
  outbox_config.owner = "notify-oom-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "notify-oom-operation";
  entry.effect_id = "notify-oom-effect";
  entry.effect_key = "notify-oom-key";
  entry.payload_digest = "sha256:notify-oom-payload";
  entry.kind = "test";
  entry.destination = "test://notify-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);

  /* The commit is durable before its best-effort direct-key signal. Losing
   * that allocation must make the private dispatcher reconcile instead. */
  lc_outbox_test_before_notification_copy_hook = outbox_fail_allocation;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_test_before_notification_copy_hook = NULL;
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_retry_notification_allocation_failure_recovers_at_deadline(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_retry retry;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "retry-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "retry-oom";
  outbox_config.owner = "retry-oom-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "retry-oom-operation";
  entry.effect_id = "retry-oom-effect";
  entry.effect_key = "retry-oom-key";
  entry.payload_digest = "sha256:retry-oom-payload";
  entry.kind = "test";
  entry.destination = "test://retry-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 1L;

  /* `retry_wait` is already durable when its delayed-key allocation fails.
   * The dispatcher must retain a recovery deadline instead of stranding it. */
  lc_outbox_test_before_notification_copy_hook = outbox_fail_allocation;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  lc_outbox_test_before_notification_copy_hook = NULL;
  job = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_outbox_delayed_recovery_waits_for_deadline(void **state) {
  (void)state;

  /* A lost delayed-key signal is recovered from durable state, but must remain
   * dormant until its recorded retry/claim deadline. Otherwise a full
   * namespace reconciliation can busy-loop while nothing is eligible. */
  assert_false(lc_outbox_test_recovery_is_due(1, 0, 1001, 1000));
  assert_true(lc_outbox_test_recovery_is_due(1, 0, 1001, 1001));
  assert_true(lc_outbox_test_recovery_is_due(1, 0, 0, 1000));
  assert_true(lc_outbox_test_recovery_is_due(1, 1, 1001, 1000));
  assert_false(lc_outbox_test_recovery_is_due(0, 0, 1001, 1002));
}

static void test_outbox_startup_replay_defers_retention_wakeups(void **state) {
  (void)state;

  /* Startup replay has explicit operator precedence. A due retention marker
   * must remain dormant until reconciliation consumes that request, otherwise
   * the private dispatcher never reaches its condition wait. */
  assert_false(lc_outbox_test_dead_letter_reclaim_is_due(1, 1, 1000, 1000));
  assert_false(lc_outbox_test_dead_letter_reclaim_is_due(1, 1, 0, 1000));
  assert_true(lc_outbox_test_dead_letter_reclaim_is_due(0, 1, 1000, 1000));
  assert_true(lc_outbox_test_dead_letter_reclaim_is_due(0, 1, 0, 1000));
}

static void
test_outbox_dead_letter_capacity_burst_keeps_coalesced_wakeup(void **state) {
  lc_unix_seconds next_reclaim_unix;

  (void)state;
  next_reclaim_unix = 0;
  /* The first marker queued a capacity check at 1001 but found no overflow.
   * A later marker from the same second must wake that check rather than leave
   * capacity reclaim deferred until the 30-day age deadline. */
  assert_int_equal(lc_outbox_test_schedule_dead_letter_reclaim_deadline(
                       2593000, 1001, 2593000, 1000, 1, &next_reclaim_unix),
                   LC_OK);
  assert_int_equal(next_reclaim_unix, 1001);
}

static void test_outbox_handed_off_job_keeps_root_timeout(void **state) {
  (void)state;

  /* shutdown_timeout_ms is exclusively for the private dispatcher. A job
   * returned to host code must preserve normal root-client request semantics.
   */
  assert_int_equal(lc_outbox_test_host_job_timeout(30000L, 1000L), 30000L);
  assert_int_equal(lc_outbox_test_host_job_timeout(1234L, 1L), 1234L);
}

static void test_outbox_claim_deadline_uses_replay_generation(void **state) {
  (void)state;

  assert_true(lc_outbox_test_claim_deadline_matches(1, 0, 1, 0));
  /* A matching durable companion is the server-authoritative expiry. */
  assert_int_equal(lc_outbox_test_select_claim_deadline(900, 1, 0, 1, 0, 1000),
                   900);
  assert_int_equal(lc_outbox_test_select_claim_deadline(1100, 1, 0, 1, 0, 1000),
                   1100);
  {
    lc_i64 retry_deadline;

    assert_int_equal(lc_outbox_test_conflicted_recovery_deadline(
                         1000, 900, 1L, &retry_deadline),
                     LC_OK);
    assert_int_equal(retry_deadline, 1001);
    assert_int_equal(lc_outbox_test_conflicted_recovery_deadline(
                         1000, 1100, 1L, &retry_deadline),
                     LC_OK);
    assert_int_equal(retry_deadline, 1100);
  }
  /* Replaying a dead letter resets its delivery attempt count, so attempt
   * number alone must never revive the earlier claim's renewal deadline. */
  assert_false(lc_outbox_test_claim_deadline_matches(1, 0, 1, 1));
  assert_false(lc_outbox_test_claim_deadline_matches(1, 1, 2, 1));
  assert_int_equal(lc_outbox_test_select_claim_deadline(900, 1, 0, 1, 1, 1000),
                   1000);
}

static void test_pouch_transient_claim_failure_is_rescheduled(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_outbox_stats stats;
  lc_error error;
  outbox_fail_once failure;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "claim-retry-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&failure, 0, sizeof(failure));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "claim-retry";
  outbox_config.owner = "claim-retry-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-retry-operation";
  entry.effect_id = "claim-retry-effect";
  entry.effect_key = "claim-retry-key";
  entry.payload_digest = "sha256:claim-retry-payload";
  entry.kind = "test";
  entry.destination = "test://claim-retry";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  lc_outbox_test_before_claim_outbox_hook = outbox_fail_first_call;
  lc_outbox_test_before_claim_outbox_context = &failure;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_int_equal(failure.calls, 2U);
  lc_outbox_stats_init(&stats);
  assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_outbox_active_remote_claim_conflict_is_not_retryable(void **state) {
  lc_error error;

  (void)state;
  lc_error_init(&error);
  assert_int_equal(lc_error_set(&error, LC_ERR_SERVER, 409L,
                                "active remote outbox claim", NULL,
                                "lease_conflict", "remote"),
                   LC_ERR_SERVER);
  assert_false(lc_outbox_test_claim_failure_is_retryable(&error));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_error_set(&error, LC_ERR_SERVER, 503L,
                                "remote service unavailable", NULL, NULL,
                                "remote"),
                   LC_ERR_SERVER);
  assert_true(lc_outbox_test_claim_failure_is_retryable(&error));
  lc_error_cleanup(&error);
}

static void
test_pouch_reconciliation_preserves_earliest_claim_recovery(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_stats stats;
  lc_outbox_transaction *transaction;
  lc_outbox_job *first, *recovered;
  lc_source *payload;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  struct timespec delay;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "claim-recovery-deadline-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  first = NULL;
  recovered = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "claim-recovery-deadline";
  outbox_config.owner = "claim-recovery-deadline-test";
  outbox_config.claim_ttl_seconds = 5L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-recovery-deadline-operation";
  entry.effect_id = "claim-recovery-deadline-effect";
  entry.effect_key = "claim-recovery-deadline-key";
  entry.payload_digest = "sha256:claim-recovery-deadline-payload";
  entry.kind = "test";
  entry.destination = "test://claim-recovery-deadline";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &first, &error),
      LC_OK);
  assert_non_null(first);
  /* Simulate an abandoned worker. The durable claim remains and is scheduled
   * at its original expiry. */
  lc_outbox_job_close(first);
  first = NULL;
  delay.tv_sec = 2;
  delay.tv_nsec = 200L * 1000L * 1000L;
  assert_int_equal(nanosleep(&delay, NULL), 0);
  /* Reconciliation rediscovers the claim and races its expired lease. That
   * conflict must retain, not replace, the original recovery deadline. */
  assert_int_equal(lc_outbox_reconcile(outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 3300L, &recovered, &error), LC_OK);
  assert_non_null(recovered);
  lc_outbox_stats_init(&stats);
  assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(recovered, NULL, &error), LC_OK);
  recovered = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_overflowing_foreground_retry_reconciles(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_stats stats;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  outbox_fail_once failure;

  (void)state;
  if (!outbox_duration_overflows_unix_range(LONG_MAX) &&
      !outbox_duration_overflows_wait_range(LONG_MAX))
    return;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "claim-retry-overflow-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&failure, 0, sizeof(failure));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "claim-retry-overflow";
  outbox_config.owner = "claim-retry-overflow-test";
  outbox_config.retry_initial_delay_seconds = LONG_MAX;
  outbox_config.retry_max_delay_seconds = LONG_MAX;
  /* An unknown acquire result is retried at the claim-recovery boundary, not
   * in a foreground spin. Keep that boundary short for this regression. */
  outbox_config.claim_ttl_seconds = 1L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-retry-overflow-operation";
  entry.effect_id = "claim-retry-overflow-effect";
  entry.effect_key = "claim-retry-overflow-key";
  entry.payload_digest = "sha256:claim-retry-overflow-payload";
  entry.kind = "test";
  entry.destination = "test://claim-retry-overflow";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  lc_outbox_test_before_claim_outbox_hook = outbox_fail_first_call;
  lc_outbox_test_before_claim_outbox_context = &failure;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_int_equal(failure.calls, 2U);
  lc_outbox_test_before_claim_outbox_hook = NULL;
  lc_outbox_test_before_claim_outbox_context = NULL;
  lc_outbox_stats_init(&stats);
  assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  assert_non_null(stats.last_error);
  assert_non_null(
      strstr(stats.last_error, "retry initial delay exceeds supported"));
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_auto_retry_long_max_cap_is_safe(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_retry retry;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "retry-long-max-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "retry-long-max";
  outbox_config.owner = "retry-long-max-test";
  outbox_config.retry_initial_delay_seconds = LONG_MAX;
  outbox_config.retry_max_delay_seconds = LONG_MAX;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "retry-long-max-operation";
  entry.effect_id = "retry-long-max-effect";
  entry.effect_key = "retry-long-max-key";
  entry.payload_digest = "sha256:retry-long-max-payload";
  entry.kind = "test";
  entry.destination = "test://retry-long-max";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_claim_recovery_allocation_failure_recovers_at_expiry(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *first, *replacement;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "claim-recovery-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  first = NULL;
  replacement = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "claim-recovery-oom";
  outbox_config.owner = "claim-recovery-oom-test";
  outbox_config.claim_ttl_seconds = outbox_claim_ttl_seconds();
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-recovery-oom-operation";
  entry.effect_id = "claim-recovery-oom-effect";
  entry.effect_key = "claim-recovery-oom-key";
  entry.payload_digest = "sha256:claim-recovery-oom-payload";
  entry.kind = "test";
  entry.destination = "test://claim-recovery-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &first, &error),
      LC_OK);
  assert_non_null(first);
  lc_outbox_test_before_notification_copy_hook = outbox_fail_allocation;
  lc_outbox_job_close(first);
  first = NULL;
  lc_outbox_test_before_notification_copy_hook = NULL;
  assert_int_equal(lc_outbox_next(outbox, outbox_claim_next_timeout_ms(),
                                  &replacement, &error),
                   LC_OK);
  assert_non_null(replacement);
  assert_int_equal(replacement->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(replacement, NULL, &error), LC_OK);
  replacement = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_command_receipt_commits_with_outbox_and_result(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_request active_command;
  lc_command_receipt receipt, duplicate_receipt, active_receipt,
      active_duplicate_receipt;
  lc_outbox_transaction *transaction, *duplicate_transaction,
      *active_transaction, *active_duplicate_transaction;
  lc_outbox_participant_request participant_request;
  lc_outbox_participant *participant;
  lc_outbox_entry entry;
  lc_outbox_receipt outbox_receipt;
  lc_command_result command_result;
  lc_outbox_completion completion;
  lc_client *client;
  lc_outbox *outbox;
  lc_source *payload, *result_body, *domain_state;
  lc_sink *sink;
  const void *bytes;
  size_t length, written;
  lc_outbox_job *job;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  result_body = NULL;
  domain_state = NULL;
  job = NULL;
  lc_command_receipt_init(&receipt);
  lc_command_receipt_init(&duplicate_receipt);
  lc_command_receipt_init(&active_receipt);
  lc_command_receipt_init(&active_duplicate_receipt);
  lc_outbox_receipt_init(&outbox_receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-outbox";
  outbox_config.owner = "command-owner";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-a";
  command.identity.command_type = "orders.create.v1";
  command.identity.idempotency_key = "request-1";
  command.request_digest = "semantic-request-digest-1";
  command.operation_id = "order-operation-1";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  assert_false(receipt.duplicate);
  assert_non_null(receipt.command_id);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "orders";
  participant_request.acquire.key = "order-command-1";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("{\"status\":\"created\"}", 20U,
                                         &domain_state, &error),
                   LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_outbox_participant_close(participant);
  participant = NULL;
  lc_outbox_entry_init(&entry);
  entry.operation_id = "order-operation-1";
  entry.effect_id = "order-created";
  entry.effect_key = "foreign-order-created-1";
  entry.payload_digest = "sha256:order-created-payload";
  entry.kind = "http";
  entry.schema_version = "v1";
  entry.destination = "https://example.invalid/orders";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_append(transaction, &entry, payload,
                                                &outbox_receipt, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("created", 7U, &result_body, &error),
                   LC_OK);
  lc_command_result_init(&command_result);
  command_result.result_code = "created";
  command_result.result_reference = "order-command-1";
  command_result.content_type = "text/plain";
  command_result.body = result_body;
  assert_int_equal(lc_outbox_transaction_complete_command(
                       transaction, &command_result, &error),
                   LC_OK);
  lc_source_close(result_body);
  result_body = NULL;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                 &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  assert_string_equal(receipt.result_code, "created");
  assert_true(receipt.has_result_body);
  /* The input id may alias the receipt output being replaced. Both direct
   * receipt reads and command waits must preserve it before cleanup. */
  assert_int_equal(lc_outbox_get_command_receipt_by_id(
                       outbox, receipt.command_id, &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  assert_int_equal(
      lc_outbox_wait_command(outbox, receipt.command_id, 0L, &receipt, &error),
      LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  assert_int_equal(lc_outbox_get_command_receipt_by_id(
                       outbox, receipt.command_id, &duplicate_receipt, &error),
                   LC_OK);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  assert_int_equal(lc_outbox_wait_command(outbox, receipt.command_id, 0L,
                                          &duplicate_receipt, &error),
                   LC_OK);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  sink = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_outbox_write_command_result(outbox, &command.identity,
                                                  sink, &written, &error),
                   LC_OK);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_int_equal(written, 7U);
  assert_memory_equal(bytes, "created", 7U);
  lc_sink_close(sink);
  duplicate_transaction = (lc_outbox_transaction *)1;
  rc = lc_outbox_accept_command(outbox, &command, &duplicate_transaction,
                                &duplicate_receipt, &error);
  if (rc != LC_OK) {
    fail_msg("committed command duplicate failed: rc=%d message=%s detail=%s",
             rc, error.message != NULL ? error.message : "(none)",
             error.detail != NULL ? error.detail : "(none)");
  }
  assert_int_equal(rc, LC_OK);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  lc_command_request_init(&active_command);
  active_command.identity.scope = "tenant-a";
  active_command.identity.command_type = "orders.create.v1";
  active_command.identity.idempotency_key = "active-request";
  active_command.request_digest = "active-semantic-request-digest";
  active_command.operation_id = "active-order-operation";
  active_transaction = NULL;
  assert_int_equal(lc_outbox_accept_command(outbox, &active_command,
                                            &active_transaction,
                                            &active_receipt, &error),
                   LC_OK);
  assert_non_null(active_transaction);
  assert_int_equal(lc_outbox_transaction_commit(active_transaction, &error),
                   LC_OK);
  lc_outbox_transaction_close(active_transaction);
  active_transaction = NULL;
  assert_int_equal(lc_outbox_resume_command(outbox, &active_command.identity,
                                            &active_transaction,
                                            &active_receipt, &error),
                   LC_OK);
  assert_non_null(active_transaction);
  active_duplicate_transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_accept_command(outbox, &active_command,
                                            &active_duplicate_transaction,
                                            &active_duplicate_receipt, &error),
                   LC_ERR_INVALID);
  assert_null(active_duplicate_transaction);
  assert_false(active_duplicate_receipt.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_rollback(active_transaction, &error),
                   LC_OK);
  lc_outbox_transaction_close(active_transaction);
  active_transaction = NULL;
  active_duplicate_transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_accept_command(outbox, &active_command,
                                            &active_duplicate_transaction,
                                            &active_duplicate_receipt, &error),
                   LC_OK);
  assert_null(active_duplicate_transaction);
  assert_true(active_duplicate_receipt.duplicate);
  lc_command_receipt_cleanup(&active_duplicate_receipt);
  lc_command_receipt_cleanup(&active_receipt);
  command.request_digest = "conflicting-digest";
  assert_int_equal(lc_outbox_accept_command(outbox, &command,
                                            &duplicate_transaction,
                                            &duplicate_receipt, &error),
                   LC_ERR_SERVER);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  command.request_digest = "semantic-request-digest-1";
  assert_int_equal(lc_outbox_next(outbox, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_non_null(job->message_id);
  assert_string_equal(job->command_id, receipt.command_id);
  assert_string_equal(job->causation_id, receipt.command_id);
  assert_string_equal(job->schema_version, "v1");
  lc_outbox_completion_init(&completion);
  completion.delivery_reference = "provider-delivery-1";
  completion.response_digest = "provider-response-digest";
  assert_int_equal(lc_outbox_job_complete(job, &completion, &error), LC_OK);
  job = NULL;
  duplicate_transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_resume_command(outbox, &command.identity,
                                            &duplicate_transaction,
                                            &duplicate_receipt, &error),
                   LC_OK);
  assert_null(duplicate_transaction);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  lc_outbox_receipt_cleanup(&outbox_receipt);
  lc_command_receipt_cleanup(&duplicate_receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_generated_command_key_waits_by_id(void **state) {
  char root[256], template_path[256], endpoint[320], command_id[48],
      unknown_command_id[48];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_transaction *transaction;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-generated-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "generated-command-outbox";
  outbox_config.owner = "generated-command-owner";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-a";
  command.identity.command_type = "orders.create.v1";
  command.generate_idempotency_key = 1;
  command.request_digest = "generated-command-digest";
  command.identity.idempotency_key = "must-not-be-supplied";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  command.identity.idempotency_key = NULL;
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_true(lc_xid_is_valid(receipt.idempotency_key));
  assert_true(
      snprintf(command_id, sizeof(command_id), "%s", receipt.command_id) > 0);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_wait_command(outbox, command_id, 0L, &receipt, &error),
      LC_ERR_TIMEOUT);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_resume_command_by_id(
                       outbox, command_id, &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.result_code = "created";
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_wait_command(outbox, command_id, 0L, &receipt, &error), LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  assert_int_equal(lc_outbox_get_command_receipt_by_id(outbox, "cmd_invalid",
                                                       &receipt, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      lc_outbox_wait_command(outbox, "cmd_invalid", 0L, &receipt, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_resume_command_by_id(
                       outbox, "cmd_invalid", &transaction, &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  memcpy(unknown_command_id, "cmd_", 4U);
  memset(unknown_command_id + 4U, 'A', 43U);
  unknown_command_id[47] = '\0';
  assert_int_equal(lc_outbox_get_command_receipt_by_id(
                       outbox, unknown_command_id, &receipt, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      lc_outbox_wait_command(outbox, unknown_command_id, 0L, &receipt, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  transaction = (lc_outbox_transaction *)1;
  assert_int_equal(lc_outbox_resume_command_by_id(outbox, unknown_command_id,
                                                  &transaction, &receipt,
                                                  &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_command_attachment_failure_aborts_transaction(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt, retry_receipt;
  lc_command_result result;
  lc_outbox_transaction *transaction;
  lc_source *body;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "command-attachment-failure-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  body = NULL;
  lc_command_receipt_init(&receipt);
  lc_command_receipt_init(&retry_receipt);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-attachment";
  command.identity.command_type = "orders.attach-result.v1";
  command.identity.idempotency_key = "attachment-failure-request";
  command.request_digest = "attachment-failure-digest";
  command.operation_id = "attachment-failure-operation";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-attachment-failure";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_source_from_callbacks(outbox_failing_source_read, NULL,
                                            NULL, NULL, &body, &error),
                   LC_OK);
  lc_command_result_init(&result);
  result.result_code = "created";
  result.content_type = "text/plain";
  result.body = body;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "forced outbox result attachment failure");
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_source_close(body);
  body = NULL;
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  /* The failed terminal result must not create a duplicate or a receipt that
   * advertises the missing attachment. */
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &retry_receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_false(retry_receipt.duplicate);
  lc_command_result_init(&result);
  result.result_code = "created";
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_command_receipt_cleanup(&retry_receipt);
  assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                 &retry_receipt, &error),
                   LC_OK);
  assert_int_equal(retry_receipt.state, LC_COMMAND_COMPLETED);
  assert_false(retry_receipt.has_result_body);
  lc_command_receipt_cleanup(&retry_receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_rejects_overflowing_deadlines(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_retry retry;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;

  (void)state;
  if (!outbox_duration_overflows_unix_range(LONG_MAX) &&
      !outbox_duration_overflows_wait_range(LONG_MAX))
    return;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "deadline-overflow-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);

  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "deadline-overflow";
  outbox_config.claim_ttl_seconds = LONG_MAX;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error),
      LC_ERR_INVALID);
  assert_null(outbox);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "deadline-overflow";
  outbox_config.recovery_interval_seconds = LONG_MAX;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error),
      LC_ERR_INVALID);
  assert_null(outbox);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "deadline-overflow";
  outbox_config.host_retry_delay_max_seconds = LONG_MAX;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);

  lc_outbox_entry_init(&entry);
  entry.operation_id = "deadline-overflow-operation";
  entry.effect_id = "deadline-overflow-effect";
  entry.effect_key = "deadline-overflow-key";
  entry.payload_digest = "sha256:deadline-overflow-payload";
  entry.kind = "http";
  entry.destination = "https://example.invalid/deadline-overflow";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_outbox_next(outbox, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = LONG_MAX;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      lc_outbox_job_dead_letter(job, "overflow regression", &error), LC_OK);
  job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_retry_uses_wide_timestamp_comparison(void **state) {
  lc_unix_seconds now;

  (void)state;
  if ((uintmax_t)LONG_MAX >= (uintmax_t)LC_I64_MAX)
    return;
  now = (lc_unix_seconds)LONG_MAX;
  now += 1;
  assert_false(lc_outbox_test_retry_is_not_eligible(now, now));
  assert_true(lc_outbox_test_retry_is_not_eligible(now + 1, now));
}

static void
test_pouch_outbox_periodic_schedule_failure_does_not_deadlock(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_shutdown_race race;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  pthread_t close_thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "periodic-schedule-race-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  lc_outbox_test_before_periodic_recovery_schedule_hook =
      outbox_periodic_recovery_schedule_failure;
  lc_outbox_test_before_periodic_recovery_schedule_context = &race;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "periodic-schedule-race";
  outbox_config.recovery_interval_seconds = 1L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  race.outbox = outbox;
  assert_int_equal(lc_outbox_reconcile(outbox, &error), LC_OK);
  assert_true(
      outbox_shutdown_race_wait(&race, &race.periodic_schedule_entered));
  assert_int_equal(pthread_create(&close_thread, NULL,
                                  outbox_shutdown_race_close_thread, &race),
                   0);
  assert_true(outbox_shutdown_race_wait(&race, &race.close_finished));
  assert_int_equal(pthread_join(close_thread, NULL), 0);
  outbox = NULL;
  outbox_reset_allocation_failures();
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_shared_command_resume_is_durable(void **state) {
  char root[256], template_path[256], endpoint[320];
  char oversized[LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES + 2U];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_transaction *transaction;
  lc_client *first_client, *second_client;
  lc_outbox *first_outbox, *second_outbox;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-shared-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-shared";
  outbox_config.owner = "command-shared-first";
  first_client = NULL;
  first_outbox = NULL;
  second_client = NULL;
  second_outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-shared";
  command.identity.command_type = "orders.cancel.v1";
  command.identity.idempotency_key = "shared-request";
  command.request_digest = "shared-request-digest";
  assert_int_equal(lc_client_open(&client_config, &first_client, &error),
                   LC_OK);
  assert_int_equal(
      lc_client_new_outbox(first_client, &outbox_config, &first_outbox, &error),
      LC_OK);
  assert_int_equal(lc_outbox_accept_command(first_outbox, &command,
                                            &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_outbox_close(first_outbox);
  lc_client_close(first_client);
  outbox_config.owner = "command-shared-second";
  assert_int_equal(lc_client_open(&client_config, &second_client, &error),
                   LC_OK);
  assert_int_equal(lc_client_new_outbox(second_client, &outbox_config,
                                        &second_outbox, &error),
                   LC_OK);
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_resume_command(second_outbox, &command.identity,
                                            &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.failure_code = "cancelled";
  memset(oversized, 'x', sizeof(oversized) - 1U);
  oversized[sizeof(oversized) - 1U] = '\0';
  result.failure_message = oversized;
  assert_int_equal(
      lc_outbox_transaction_fail_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  result.failure_message = "order was already cancelled";
  assert_int_equal(
      lc_outbox_transaction_fail_command(transaction, &result, &error), LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_get_command_receipt(
                       second_outbox, &command.identity, &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_FAILED);
  assert_string_equal(receipt.failure_code, "cancelled");
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(second_outbox);
  lc_client_close(second_client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_multikey_prevote_failure_publishes_nothing(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_source *payload;
  lc_source *domain_state;
  lc_error error;
  outbox_fail_on_call release_failure;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "atomic-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  domain_state = NULL;
  memset(&release_failure, 0, sizeof(release_failure));
  release_failure.failure_call = 1U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-atomic";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-atomic";
  outbox_config.owner = "outbox-atomic-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "atomic-operation";
  entry.effect_id = "atomic-effect";
  entry.effect_key = "atomic-effect-key";
  entry.payload_digest = "sha256:atomic-payload";
  entry.kind = "test";
  entry.destination = "atomic://effect";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("atomic", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-atomic";
  participant_request.acquire.key = "domain-atomic";
  participant_request.acquire.owner = "outbox-atomic-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"committed\":true}", 18U, &domain_state, &error),
      LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_outbox_participant_close(participant);
  participant = NULL;

  /* A failure before the first release request has no indeterminate remote
   * outcome, so close may safely roll the whole transaction back. */
  lc_outbox_test_before_transaction_terminal_release_context = &release_failure;
  lc_outbox_test_before_transaction_terminal_release_hook =
      outbox_fail_on_configured_call;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(release_failure.calls, 1U);
  lc_outbox_test_before_transaction_terminal_release_hook = NULL;
  lc_outbox_test_before_transaction_terminal_release_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_null(receipt.outbox_key);
  outbox_assert_public_state_absent(client, "domain-atomic", &error);

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_multikey_commit_retry_retains_outbox_notification(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  int decision_calls;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "atomic-retry-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  decision_calls = 0;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-atomic-retry";
  outbox_config.owner = "outbox-atomic-retry-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "atomic-retry-operation";
  entry.effect_id = "atomic-retry-effect";
  entry.effect_key = "atomic-retry-effect-key";
  entry.payload_digest = "sha256:atomic-retry-payload";
  entry.kind = "test";
  entry.destination = "atomic://retry";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("atomic", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-atomic-retry";
  participant_request.acquire.key = "domain-atomic-retry";
  participant_request.acquire.owner = "outbox-atomic-retry-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  lc_outbox_participant_close(participant);
  participant = NULL;

  /* The first outbox release votes successfully. Force the final participant
   * release to fail, then retry the same transaction after the transport fault
   * clears. The committed outbox must still wake this local dispatcher. */
  lc_pouch_test_before_txn_decision_context = &decision_calls;
  lc_pouch_test_before_txn_decision_hook =
      outbox_force_pouch_txn_decision_failure;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(decision_calls, 1);
  lc_pouch_test_before_txn_decision_hook = NULL;
  lc_pouch_test_before_txn_decision_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 1000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_partial_command_commit_freezes_transaction(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_error error;
  outbox_fail_on_call release_failure;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "partial-command-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  memset(&release_failure, 0, sizeof(release_failure));
  release_failure.failure_call = 2U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "partial-command";
  outbox_config.owner = "partial-command-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "partial-command-scope";
  command.identity.command_type = "partial-command-type";
  command.identity.idempotency_key = "partial-command-key";
  command.request_digest = "partial-command-digest";
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "partial-command";
  participant_request.acquire.key = "partial-command-domain";
  participant_request.acquire.owner = "partial-command-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);

  /* The command lease votes first. A deterministic fault before the second
   * release must leave no exposed lease mutable or dereferenceable; only this
   * commit may be retried. */
  lc_outbox_test_before_transaction_terminal_release_context = &release_failure;
  lc_outbox_test_before_transaction_terminal_release_hook =
      outbox_fail_on_configured_call;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(release_failure.calls, 2U);
  lc_outbox_test_before_transaction_terminal_release_hook = NULL;
  lc_outbox_test_before_transaction_terminal_release_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_command_result_init(&result);
  result.result_code = "created";
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(participant->describe(participant, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_participant_close(participant);
  participant = NULL;

  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_indeterminate_first_vote_freezes_transaction(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_source *payload;
  lc_error error;
  outbox_fail_on_call response_loss;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "indeterminate-first-vote-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  payload = NULL;
  memset(&response_loss, 0, sizeof(response_loss));
  response_loss.failure_call = 1U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "indeterminate-first-vote";
  outbox_config.owner = "indeterminate-first-vote-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "indeterminate-first-vote-operation";
  entry.effect_id = "indeterminate-first-vote-effect";
  entry.effect_key = "indeterminate-first-vote-key";
  entry.payload_digest = "sha256:indeterminate-first-vote";
  entry.kind = "test";
  entry.destination = "test://indeterminate-first-vote";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);

  /* This injection is after the commit decision is frozen but before the
   * response-bearing release call. It models a request that may have reached
   * the resource manager while its response was lost. */
  lc_outbox_test_after_transaction_terminal_decision_context = &response_loss;
  lc_outbox_test_after_transaction_terminal_decision_hook =
      outbox_fail_on_configured_call;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(response_loss.calls, 1U);
  lc_outbox_test_after_transaction_terminal_decision_hook = NULL;
  lc_outbox_test_after_transaction_terminal_decision_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_transaction_rollback(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_append(transaction, &entry, payload,
                                                &receipt, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_indeterminate_commit_replay_retains_outbox_notification(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  outbox_fail_on_call replay_failure;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "indeterminate-replay-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  memset(&replay_failure, 0, sizeof(replay_failure));
  replay_failure.failure_call = 2U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-indeterminate-replay";
  outbox_config.owner = "outbox-indeterminate-replay-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "indeterminate-replay-operation";
  entry.effect_id = "indeterminate-replay-effect";
  entry.effect_key = "indeterminate-replay-effect-key";
  entry.payload_digest = "sha256:indeterminate-replay-payload";
  entry.kind = "test";
  entry.destination = "atomic://indeterminate-replay";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_source_from_memory("indeterminate", 13U, &payload, &error), LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-indeterminate-replay";
  participant_request.acquire.key = "domain-indeterminate-replay";
  participant_request.acquire.owner = "outbox-indeterminate-replay-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  lc_outbox_participant_close(participant);
  participant = NULL;

  /* Both releases have made their XA votes durable before this replay failure.
   * The caller receives an indeterminate terminal error, but the committed
   * outbox must still reach the local dispatcher without a restart or scan. */
  lc_pouch_test_before_txn_replay_context = &replay_failure;
  lc_pouch_test_before_txn_replay_hook = outbox_fail_on_configured_call;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(replay_failure.calls, 2U);
  lc_pouch_test_before_txn_replay_hook = NULL;
  lc_pouch_test_before_txn_replay_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 1000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_expired_commit_reports_rollback_without_signal(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *domain_state;
  lc_error error;
  outbox_pouch_test_clock clock;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "expired-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  job = NULL;
  payload = NULL;
  domain_state = NULL;
  memset(&clock, 0, sizeof(clock));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-expired-terminal";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-expired-terminal";
  outbox_config.owner = "outbox-expired-terminal-test";
  /* Leave enough setup headroom for the staged outbox and participant under
   * sanitizer or emulated execution; expiry is asserted explicitly below. */
  outbox_config.transaction_ttl_seconds = 3L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_null(((lc_client_handle *)client)->outbox_dispatchers);
  assert_int_equal(clock_gettime(CLOCK_REALTIME, &clock.wall), 0);
  lc_pouch_test_queue_time_context = &clock;
  lc_pouch_test_queue_clock_gettime = outbox_pouch_test_gettime;
  lc_outbox_entry_init(&entry);
  entry.operation_id = "expired-terminal-operation";
  entry.effect_id = "expired-terminal-effect";
  entry.effect_key = "expired-terminal-effect-key";
  entry.payload_digest = "sha256:expired-terminal-payload";
  entry.kind = "test";
  entry.destination = "atomic://expired-terminal";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("atomic", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-expired-terminal";
  participant_request.acquire.key = "domain-expired-terminal";
  participant_request.acquire.owner = "outbox-expired-terminal-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"committed\":true}", 18U, &domain_state, &error),
      LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_outbox_participant_close(participant);
  participant = NULL;

  /* The outbox lease expires while the domain participant remains live. Advance
   * Pouch's scoped test clock rather than sleeping: the shared XA record must
   * roll back and the outbox must not publish a false dispatch signal. */
  clock.wall.tv_sec += 4L;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  (void)outbox_pouch_test_clock_teardown(NULL);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_null(receipt.outbox_key);
  outbox_assert_public_state_absent(client, "domain-expired-terminal", &error);

  /* The outbox now always gives Pouch its own explicit xid, even when its
   * only participant is a domain record. Its terminal result must therefore
   * be replayed too: an expired one-participant vote cannot report commit. */
  assert_null(((lc_client_handle *)client)->outbox_dispatchers);
  assert_int_equal(clock_gettime(CLOCK_REALTIME, &clock.wall), 0);
  lc_pouch_test_queue_time_context = &clock;
  lc_pouch_test_queue_clock_gettime = outbox_pouch_test_gettime;
  assert_int_equal(lc_outbox_begin(outbox, &transaction, &error), LC_OK);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "outbox-expired-terminal";
  participant_request.acquire.key = "domain-expired-single";
  participant_request.acquire.owner = "outbox-expired-terminal-test";
  participant_request.acquire.ttl_seconds = 3L;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"committed\":true}", 18U, &domain_state, &error),
      LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_outbox_participant_close(participant);
  participant = NULL;
  clock.wall.tv_sec += 4L;
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  (void)outbox_pouch_test_clock_teardown(NULL);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  outbox_assert_public_state_absent(client, "domain-expired-single", &error);
  /* Starting a consumer also starts its real-clock maintenance thread. Finish
   * both controlled-clock transactions first, then check that neither one
   * published a dispatch signal. */
  assert_null(((lc_client_handle *)client)->outbox_dispatchers);
  assert_int_equal(lc_outbox_next(outbox, 250L, &job, &error), LC_OK);
  assert_null(job);

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void outbox_recovery_overflow_resumes_after_capacity_frees(int claimed) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_recovery_overflow_drain_hook hook;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_error error;

  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "recovery-overflow-resume-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s%s", root,
                       claimed ? "" : "?query_indexing=false") > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  memset(&hook, 0, sizeof(hook));
  assert_int_equal(pthread_mutex_init(&hook.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&hook.condition, NULL), 0);
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-recovery-overflow-resume";
  outbox_config.owner = "outbox-recovery-overflow-resume-owner";
  outbox_config.notification_capacity = 1U;
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  seed_outbox_with_counters(client, outbox_config.ns,
                            "__lockdc_io/v1/outbox/recovery-overflow-candidate",
                            claimed ? "claimed" : "pending", "0", "0", &error);
  seed_terminal_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/recovery-overflow-terminal",
                       &error);
  /* Inject a stale direct key after the scan has measured candidate space.
   * The overflow hook consumes that stale key before the scan completes. */
  hook.outbox = outbox;
  hook.dispatcher = dispatcher;
  hook.terminal_key = "__lockdc_io/v1/outbox/recovery-overflow-terminal";
  hook.fill_on_query = claimed ? 1 : 2;
  lc_outbox_test_before_recovery_query_context = &hook;
  lc_outbox_test_before_recovery_query_hook = outbox_gate_first_recovery_query;
  lc_outbox_test_after_recovery_overflow_context = &hook;
  lc_outbox_test_after_recovery_overflow_hook =
      outbox_drain_recovery_overflow_hook;
  assert_int_equal(lc_outbox_dispatcher_reconcile(dispatcher, &error), LC_OK);
  outbox_wait_for_recovery_overflow_hook(&hook, &hook.query_started);
  assert_int_equal(pthread_mutex_lock(&hook.mutex), 0);
  hook.allow_query = 1;
  assert_int_equal(pthread_cond_broadcast(&hook.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&hook.mutex), 0);
  outbox_wait_for_recovery_overflow_hook(&hook, &hook.overflowed);
  lc_outbox_test_before_recovery_query_hook = NULL;
  lc_outbox_test_before_recovery_query_context = NULL;
  lc_outbox_test_after_recovery_overflow_hook = NULL;
  lc_outbox_test_after_recovery_overflow_context = NULL;
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key,
                      "__lockdc_io/v1/outbox/recovery-overflow-candidate");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(hook.calls, 1);
  assert_int_equal(hook.rc, LC_OK);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  assert_int_equal(pthread_cond_destroy(&hook.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&hook.mutex), 0);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_recovery_overflow_resumes_after_capacity_frees(void **state) {
  (void)state;
  outbox_recovery_overflow_resumes_after_capacity_frees(0);
}

static void test_pouch_claim_recovery_overflow_resumes(void **state) {
  (void)state;
  outbox_recovery_overflow_resumes_after_capacity_frees(1);
}

static void
test_pouch_delayed_wake_does_not_block_ready_reconciliation(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_outbox_retry retry;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "delayed-wake-progress-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-delayed-wake-progress";
  outbox_config.owner = "outbox-delayed-wake-progress-owner";
  outbox_config.notification_capacity = 1U;
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  seed_recovery_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/delayed-wake-a", &error);
  assert_int_equal(
      lc_outbox_dispatcher_notify_outbox_key(
          dispatcher, "__lockdc_io/v1/outbox/delayed-wake-a", &error),
      LC_OK);
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 3600L;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  job = NULL;
  seed_recovery_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/delayed-wake-b", &error);
  /* B comes from another producer without a local notification. Explicit
   * reconciliation must evict A's future durable retry wake and discover the
   * ready envelope instead of treating the bounded delayed queue as a reason
   * to skip the scan. */
  assert_int_equal(lc_outbox_dispatcher_reconcile(dispatcher, &error), LC_OK);
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key, "__lockdc_io/v1/outbox/delayed-wake-b");
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_evicted_claim_wake_recovers_at_expiry(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "evicted-claim-wake-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-evicted-claim-wake";
  outbox_config.owner = "outbox-evicted-claim-wake-owner";
  outbox_config.claim_ttl_seconds = outbox_claim_ttl_seconds();
  outbox_config.notification_capacity = 1U;
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);

  seed_recovery_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/evicted-claim-a", &error);
  assert_int_equal(
      lc_outbox_dispatcher_notify_outbox_key(
          dispatcher, "__lockdc_io/v1/outbox/evicted-claim-a", &error),
      LC_OK);
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  /* Closing abandons a durable active claim and schedules its one-slot local
   * expiry wake. */
  lc_outbox_job_close(job);
  job = NULL;

  seed_recovery_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/evicted-claim-b", &error);
  assert_int_equal(
      lc_outbox_dispatcher_notify_outbox_key(
          dispatcher, "__lockdc_io/v1/outbox/evicted-claim-b", &error),
      LC_OK);
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key, "__lockdc_io/v1/outbox/evicted-claim-b");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  /* B displaced A's local claim wake. Once A expires, its durable claimed
   * envelope must be selected automatically; no explicit reconcile is needed.
   */
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 5000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key, "__lockdc_io/v1/outbox/evicted-claim-a");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_claim_recovery_survives_capacity_pause(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_recovery_overflow_drain_hook hook;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "claim-recovery-capacity-pause-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  memset(&hook, 0, sizeof(hook));
  assert_int_equal(pthread_mutex_init(&hook.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&hook.condition, NULL), 0);
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-claim-recovery-capacity-pause";
  outbox_config.owner = "outbox-claim-recovery-capacity-pause-owner";
  outbox_config.notification_capacity = 1U;
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  /* The released seed lease leaves a durable, already-expired claim. Only a
   * claim scan can turn it back into dispatchable pending work. */
  seed_outbox_with_counters(
      client, outbox_config.ns,
      "__lockdc_io/v1/outbox/claim-recovery-capacity-paused", "claimed", "0",
      "0", &error);
  seed_terminal_outbox(client, outbox_config.ns,
                       "__lockdc_io/v1/outbox/claim-recovery-capacity-terminal",
                       &error);
  assert_int_equal(lc_outbox_dispatcher_notify_outbox_key(
                       dispatcher,
                       "__lockdc_io/v1/outbox/claim-recovery-capacity-terminal",
                       &error),
                   LC_OK);
  lc_outbox_test_after_recovery_capacity_pause_context = &hook;
  lc_outbox_test_after_recovery_capacity_pause_hook =
      outbox_note_recovery_capacity_pause;
  assert_int_equal(lc_outbox_dispatcher_reconcile(dispatcher, &error), LC_OK);
  outbox_wait_for_recovery_overflow_hook(&hook, &hook.overflowed);
  lc_outbox_test_after_recovery_capacity_pause_hook = NULL;
  lc_outbox_test_after_recovery_capacity_pause_context = NULL;
  /* Removing the stale candidate must resume the deferred claim scan without
   * another explicit reconcile request or a periodic recovery timer. */
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 0L, &job, &error),
                   LC_OK);
  assert_null(job);
  assert_int_equal(lc_outbox_dispatcher_next(dispatcher, 3000L, &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(hook.calls, 1);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  assert_int_equal(pthread_cond_destroy(&hook.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&hook.mutex), 0);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_concurrent_dispatcher_acquisition_keeps_one_attachment_ref(
    void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  size_t iteration;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dispatcher-acquire-race-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);

  for (iteration = 0U; iteration < 20U; ++iteration) {
    pthread_barrier_t barrier;
    pthread_t first_thread;
    pthread_t second_thread;
    outbox_dispatcher_acquire_race first;
    outbox_dispatcher_acquire_race second;

    outbox = NULL;
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    lc_outbox_config_init(&outbox_config);
    outbox_config.ns = "outbox-dispatcher-acquire-race";
    outbox_config.owner = "outbox-dispatcher-acquire-race-owner";
    assert_int_equal(outbox_public_new(client, &outbox_config, &outbox, &error),
                     LC_OK);
    assert_int_equal(pthread_barrier_init(&barrier, NULL, 2U), 0);
    first.outbox = outbox;
    first.barrier = &barrier;
    second.outbox = outbox;
    second.barrier = &barrier;
    assert_int_equal(pthread_create(&first_thread, NULL,
                                    outbox_dispatcher_acquire_thread, &first),
                     0);
    assert_int_equal(pthread_create(&second_thread, NULL,
                                    outbox_dispatcher_acquire_thread, &second),
                     0);
    assert_int_equal(pthread_join(first_thread, NULL), 0);
    assert_int_equal(pthread_join(second_thread, NULL), 0);
    assert_int_equal(pthread_barrier_destroy(&barrier), 0);
    assert_int_equal(first.rc, LC_OK);
    assert_int_equal(second.rc, LC_OK);
    assert_non_null(first.dispatcher);
    assert_ptr_equal(first.dispatcher, second.dispatcher);
    /* Registry + outbox attachment + exactly one reference per caller. */
    assert_int_equal(lc_outbox_test_dispatcher_ref_count(first.dispatcher), 4U);
    assert_int_equal(lc_outbox_dispatcher_stop(first.dispatcher, -1L, &error),
                     LC_OK);
    lc_outbox_dispatcher_close(second.dispatcher);
    lc_outbox_dispatcher_close(first.dispatcher);
    lc_outbox_close(outbox);
  }

  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_reconciliation_retains_overflow_request(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_reconcile_overflow_hook hook;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  int delivered_recovered;
  int delivered_overflow;
  size_t attempt;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "reconcile-overflow-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  job = NULL;
  delivered_recovered = 0;
  delivered_overflow = 0;
  memset(&hook, 0, sizeof(hook));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-reconcile-overflow";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-reconcile-overflow";
  outbox_config.owner = "outbox-reconcile-overflow-test";
  outbox_config.notification_capacity = 1U;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);

  /* The explicit sweep queues the first durable key. Its after-query hook
   * commits the second key while the one-slot candidate queue is full. */
  seed_recovery_outbox(client, "outbox-reconcile-overflow",
                       "__lockdc_io/v1/outbox/recovered", &error);
  hook.outbox = outbox;
  lc_outbox_test_after_reconcile_query_context = &hook;
  lc_outbox_test_after_reconcile_query_hook =
      outbox_reconcile_overflow_commit_hook;
  assert_int_equal(lc_outbox_reconcile(outbox, &error), LC_OK);
  for (attempt = 0U; attempt < 2U; ++attempt) {
    job = NULL;
    assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
    assert_non_null(job);
    if (strcmp(job->effect_key, "reconcile-overflow-effect-key") == 0)
      delivered_overflow = 1;
    else if (strcmp(job->effect_key, "recovery-key") == 0)
      delivered_recovered = 1;
    else
      fail_msg("unexpected reconciled outbox job");
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    job = NULL;
  }
  lc_outbox_test_after_reconcile_query_hook = NULL;
  lc_outbox_test_after_reconcile_query_context = NULL;
  assert_int_equal(hook.calls, 1);
  assert_int_equal(hook.rc, LC_OK);
  assert_true(delivered_recovered);
  assert_true(delivered_overflow);

  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_reconciliation_skips_disappeared_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *ns = "outbox-reconcile-disappeared";
  const char *outbox_key = "__lockdc_io/v1/outbox/disappeared";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_delete_outbox_hook hook;
  lc_outbox_stats stats;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "reconcile-disappeared-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  job = NULL;
  memset(&hook, 0, sizeof(hook));
  memset(&stats, 0, sizeof(stats));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = ns;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = ns;
  outbox_config.owner = "outbox-reconcile-disappeared-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);

  seed_recovery_outbox(client, ns, outbox_key, &error);
  seed_recovery_outbox(client, ns, "__lockdc_io/v1/outbox/zz-sentinel", &error);
  outbox_delete_outbox_hook_init(&hook, client, ns, outbox_key);
  /* Delete the key after the query queues it and before the consumer acquires
   * its lease. A dispatcher callback after the query races the consumer. */
  lc_outbox_test_before_claim_outbox_hook = outbox_delete_outbox_once;
  lc_outbox_test_before_claim_outbox_context = &hook;
  assert_int_equal(lc_outbox_reconcile(outbox, &error), LC_OK);
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key, "__lockdc_io/v1/outbox/zz-sentinel");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_test_before_claim_outbox_hook = NULL;
  lc_outbox_test_before_claim_outbox_context = NULL;
  outbox_delete_outbox_hook_assert(&hook);

  assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  assert_non_null(stats.last_error);
  lc_outbox_stats_cleanup(&stats);

  outbox_reset_allocation_failures();
  outbox_delete_outbox_hook_cleanup(&hook);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_handoff_skips_disappeared_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *ns = "outbox-handoff-disappeared";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  outbox_delete_outbox_hook hook;
  lc_outbox_stats stats;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "handoff-disappeared-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&hook, 0, sizeof(hook));
  memset(&stats, 0, sizeof(stats));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = ns;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = ns;
  outbox_config.owner = "outbox-handoff-disappeared-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "handoff-disappeared-operation";
  entry.effect_id = "handoff-disappeared-effect";
  entry.effect_key = "handoff-disappeared-key";
  entry.payload_digest = "sha256:handoff-disappeared-payload";
  entry.kind = "test";
  entry.destination = "test://handoff-disappeared";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  outbox_delete_outbox_hook_init(&hook, client, ns, receipt.outbox_key);
  lc_outbox_test_before_outbox_handoff_reacquire_hook =
      outbox_delete_outbox_once;
  lc_outbox_test_before_outbox_handoff_reacquire_context = &hook;
  seed_recovery_outbox(client, ns, "__lockdc_io/v1/outbox/zz-sentinel", &error);
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_string_equal(job->outbox_key, "__lockdc_io/v1/outbox/zz-sentinel");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_test_before_outbox_handoff_reacquire_hook = NULL;
  lc_outbox_test_before_outbox_handoff_reacquire_context = NULL;
  outbox_delete_outbox_hook_assert(&hook);

  assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  assert_non_null(stats.last_error);
  lc_outbox_stats_cleanup(&stats);

  outbox_reset_allocation_failures();
  outbox_delete_outbox_hook_cleanup(&hook);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_dead_letter_operations(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_dead_letter_replay_client_capture replay_capture;
  outbox_hold_dead_letter_hook export_hook;
  lc_outbox_entry entry;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_outbox_job *job;
  lc_source *payload;
  lc_sink *sink;
  lc_dead_letter_export_opts export_options;
  lc_dead_letter_export_res export_result;
  lc_outbox_stats outbox_stats;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_attachment_list attachments;
  lc_pouch_outbox_retention_marker retained_marker;
  lc_pouch_unix_seconds next_due;
  char deadline_key[128];
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  memset(&replay_capture, 0, sizeof(replay_capture));
  assert_int_equal(pthread_mutex_init(&replay_capture.mutex, NULL), 0);
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dead-letter-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  /* Dead-letter recovery and export are outbox operations, not public
   * indexed queries. Exercise their supported scan-only root path too. */
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  sink = NULL;
  lease = NULL;
  memset(&export_hook, 0, sizeof(export_hook));
  memset(&attachments, 0, sizeof(attachments));
  memset(&retained_marker, 0, sizeof(retained_marker));
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-dead-letter";
  outbox_config.owner = "outbox-dead-letter-test";
  /* This fixture covers management transitions, not expiry. Keep its active
   * claims comfortably beyond Valgrind's execution cost. */
  outbox_config.claim_ttl_seconds = 60L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  if (SIZE_MAX > (size_t)LONG_MAX) {
    size_t too_large_limit = (size_t)LONG_MAX;

    ++too_large_limit;
    lc_dead_letter_export_opts_init(&export_options);
    export_options.limit = too_large_limit;
    lc_dead_letter_export_res_init(&export_result);
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(lc_outbox_export_dead_letters(
                         outbox, &export_options, sink, &export_result, &error),
                     LC_ERR_INVALID);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    lc_sink_close(sink);
    sink = NULL;
  }
  lc_outbox_entry_init(&entry);
  entry.operation_id = "dead-letter-operation";
  entry.effect_id = "dead-letter-effect";
  entry.effect_key = "foreign-dead-letter-idempotency-key";
  entry.payload_digest = "sha256:dead-letter-payload";
  entry.kind = "http";
  entry.destination = "https://example.invalid/dead-letter";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload-body", 12U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_renew(job, 120L, &error), LC_OK);
  assert_int_equal(lc_outbox_job_dead_letter(job, "permanent failure", &error),
                   LC_OK);
  job = NULL;
  seed_foreign_outbox_state(client, outbox_config.ns, "foreign-dead-letter",
                            "dead_letter", &error);

  /* Query selection and direct export acquisition are intentionally separate.
   * A concurrent management lease makes this selected row stale, not an export
   * failure. */
  export_hook.client = client;
  export_hook.ns = outbox_config.ns;
  export_hook.key = receipt.outbox_key;
  lc_outbox_test_before_dead_letter_export_open_hook =
      outbox_hold_dead_letter_for_export;
  lc_outbox_test_before_dead_letter_export_open_context = &export_hook;
  lc_dead_letter_export_res_init(&export_result);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(
      lc_outbox_export_dead_letters(outbox, NULL, sink, &export_result, &error),
      LC_OK);
  assert_int_equal(export_result.exported, 0U);
  lc_sink_close(sink);
  sink = NULL;
  lc_outbox_test_before_dead_letter_export_open_hook = NULL;
  lc_outbox_test_before_dead_letter_export_open_context = NULL;
  assert_non_null(export_hook.lease);
  assert_int_equal(lc_lease_release(export_hook.lease, NULL, &error), LC_OK);
  export_hook.lease = NULL;

  lc_dead_letter_export_res_init(&export_result);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(
      lc_outbox_export_dead_letters(outbox, NULL, sink, &export_result, &error),
      LC_OK);
  assert_int_equal(export_result.exported, 1U);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(length > 2U);
  assert_true(outbox_bytes_contains(bytes, length, "dead_letter"));
  assert_true(outbox_bytes_contains(bytes, length, entry.effect_key));
  assert_true(outbox_bytes_contains(bytes, length, "dead_letter_retention_id"));
  assert_false(outbox_bytes_contains(bytes, length, "payload-body"));
  lc_sink_close(sink);
  sink = NULL;

  lc_outbox_stats_init(&outbox_stats);
  assert_int_equal(lc_outbox_get_stats(outbox, &outbox_stats, &error), LC_OK);
  assert_true(outbox_stats.running);
  assert_true(outbox_stats.direct_notifications > 0U);
  lc_outbox_stats_cleanup(&outbox_stats);
  /* A failed release must leave the durable retention schedule intact, even
   * after the replay's pending body has been staged. */
  lc_outbox_test_before_dead_letter_replay_release_hook =
      outbox_fail_allocation;
  assert_int_equal(
      lc_outbox_replay_dead_letter(outbox, receipt.outbox_key, &error),
      LC_ERR_NOMEM);
  lc_outbox_test_before_dead_letter_replay_release_hook = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  next_due = 0;
  assert_int_equal(lc_pouch_outbox_retention_marker_take(
                       ((lc_client_handle *)client)->pouch, outbox_config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, NULL, 1, 0U, UINT64_MAX,
                       &retained_marker, &next_due, &error),
                   LC_OK);
  assert_non_null(retained_marker.marker_id);
  assert_string_equal(retained_marker.outbox_key, receipt.outbox_key);
  lc_pouch_outbox_retention_marker_cleanup(((lc_client_handle *)client)->pouch,
                                           &retained_marker);
  assert_int_equal(
      lc_outbox_replay_dead_letter(outbox, receipt.outbox_key, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(job->attempt, 1);
  assert_int_equal(lc_outbox_job_dead_letter(job, "second failure", &error),
                   LC_OK);
  job = NULL;

  lc_dead_letter_export_opts_init(&export_options);
  export_options.format = LC_DEAD_LETTER_EXPORT_JSONL;
  lc_dead_letter_export_res_init(&export_result);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_outbox_export_dead_letters(outbox, &export_options, sink,
                                                 &export_result, &error),
                   LC_OK);
  assert_int_equal(export_result.exported, 1U);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(length > 0U && ((const char *)bytes)[length - 1U] == '\n');
  assert_true(outbox_bytes_contains(bytes, length, "prior_dead_letter_error"));
  assert_true(outbox_bytes_contains(bytes, length, "second failure"));
  lc_sink_close(sink);
  sink = NULL;

  assert_int_equal(
      lc_outbox_delete_dead_letter(outbox, receipt.outbox_key, &error), LC_OK);
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox_config.ns;
  acquire.key = receipt.outbox_key;
  acquire.owner = "outbox-dead-letter-inspect";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  assert_int_equal(lc_lease_list_attachments(lease, &attachments, &error),
                   LC_OK);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lease = NULL;

  /* A recreated outbox identity starts a fresh dispatch generation.  The
   * renewal companion from its deleted predecessor must not defer recovery. */
  outbox_claim_deadline_key(receipt.outbox_key, deadline_key);
  outbox_assert_public_state_absent(client, deadline_key, &error);
  assert_int_equal(lc_source_from_memory("payload-body", 12U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  outbox_assert_public_state_absent(client, deadline_key, &error);
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;

  /* A companion-cleanup failure must leave the dead letter retryable. */
  entry.effect_id = "dead-letter-cleanup-failure-effect";
  entry.effect_key = "dead-letter-cleanup-failure-idempotency-key";
  assert_int_equal(lc_source_from_memory("payload-body", 12U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_renew(job, 120L, &error), LC_OK);
  assert_int_equal(lc_outbox_job_dead_letter(job, "cleanup failure", &error),
                   LC_OK);
  job = NULL;
  lc_outbox_test_before_dead_letter_claim_cleanup_hook = outbox_fail_allocation;
  assert_int_equal(
      lc_outbox_delete_dead_letter(outbox, receipt.outbox_key, &error),
      LC_ERR_NOMEM);
  lc_outbox_test_before_dead_letter_claim_cleanup_hook = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      lc_outbox_delete_dead_letter(outbox, receipt.outbox_key, &error), LC_OK);
  outbox_assert_public_state_absent(client, receipt.outbox_key, &error);
  outbox_claim_deadline_key(receipt.outbox_key, deadline_key);
  outbox_assert_public_state_absent(client, deadline_key, &error);

  entry.effect_id = "startup-replay-effect";
  entry.effect_key = "startup-replay-idempotency-key";
  assert_int_equal(
      lc_source_from_memory("startup-payload", 15U, &payload, &error), LC_OK);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_dead_letter(job, "startup replay", &error),
                   LC_OK);
  job = NULL;
  lc_outbox_close(outbox);
  outbox = NULL;
  outbox_config.replay_dead_letters_on_startup = 1;
  lc_outbox_test_dead_letter_replay_client_hook =
      outbox_capture_dead_letter_replay_client;
  lc_outbox_test_dead_letter_replay_client_context = &replay_capture;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(job->attempt, 1);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(pthread_mutex_lock(&replay_capture.mutex), 0);
  assert_int_equal(replay_capture.calls, 1U);
  assert_true(replay_capture.used_dispatcher_client);
  assert_int_equal(pthread_mutex_unlock(&replay_capture.mutex), 0);
  lc_outbox_test_dead_letter_replay_client_hook = NULL;
  lc_outbox_test_dead_letter_replay_client_context = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_mutex_destroy(&replay_capture.mutex), 0);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_startup_replay_precedes_due_retention(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *outbox_key =
      "__lockdc_io/v1/outbox/retention/startup-replay-precedence";
  char marker_id[LC_XID_STRING_SIZE];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "startup-retention-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  job = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-startup-retention";
  outbox_config.owner = "outbox-startup-retention-test";
  outbox_config.replay_dead_letters_on_startup = 1;
  /* This marker is already due before the dispatcher starts. The requested
   * replay must still restore and hand it out before retention can run. */
  assert_int_equal(lc_xid_new(marker_id, &error), LC_OK);
  seed_retained_dead_letter_outbox(client, outbox_config.ns, outbox_key,
                                   marker_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       ((lc_client_handle *)client)->pouch, outbox_config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, marker_id, outbox_key, 1,
                       32U, &error),
                   LC_OK);
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "retention-key");
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_dead_letter_management_rejects_invalid_candidates(void **state) {
  static const char unreserved_key[] = "ordinary-dead-letter";
  static const char malformed_key[] =
      "__lockdc_io/v1/outbox/malformed-dead-letter";
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_attachment_list attachments;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dead-letter-boundary-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  lease = NULL;
  sink = NULL;
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_dead_letter_outbox_with_replay_count(
      client, "outbox-dead-letter-boundary", unreserved_key, "0", &error);
  seed_malformed_dead_letter_outbox(client, "outbox-dead-letter-boundary",
                                    malformed_key, &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-dead-letter-boundary";
  outbox_config.owner = "outbox-dead-letter-boundary-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);

  assert_int_equal(lc_outbox_replay_dead_letter(outbox, unreserved_key, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_delete_dead_letter(outbox, unreserved_key, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_acquire_req_init(&acquire);
  acquire.ns = outbox_config.ns;
  acquire.key = unreserved_key;
  acquire.owner = "outbox-dead-letter-boundary-inspect";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  assert_int_equal(lc_lease_list_attachments(lease, &attachments, &error),
                   LC_OK);
  assert_int_equal(attachments.count, 1U);
  lc_attachment_list_cleanup(&attachments);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length,
                                    "\"dispatch_state\":\"dead_letter\""));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  sink = NULL;
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lease = NULL;

  assert_int_equal(lc_outbox_replay_dead_letter(outbox, malformed_key, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox_config.ns;
  acquire.key = malformed_key;
  acquire.owner = "outbox-malformed-dead-letter-inspect";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length,
                                    "\"dispatch_state\":\"dead_letter\""));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_startup_recovery_claims_seeded_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "recovery-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-recovery",
                       "__lockdc_io/v1/outbox/recovery", &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-recovery";
  outbox_config.owner = "outbox-recovery-test";
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 1);
  /* A handed-off job remains terminally usable after dispatcher shutdown. Its
   * retry is recovered durably by the replacement outbox, not lost with the
   * private in-memory scheduler. */
  lc_outbox_close(outbox);
  outbox = NULL;
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  job = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_reopen_reconciles_durable_index_mode(int shared) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req query_request;
  lc_query_key_handler query_handler;
  lc_query_res query_result;
  outbox_query_count query_count;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  struct timespec finished;
  size_t index;

  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "clean-reopen-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       shared ? "pouch://%s?single_writer=false" : "pouch://%s",
                       root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < OUTBOX_CLEAN_REOPEN_FOREIGN_CHURN; ++index) {
    size_t key_index;

    for (key_index = 0U; key_index < OUTBOX_CLEAN_REOPEN_FOREIGN_KEYS;
         ++key_index) {
      char key[128];

      assert_true(snprintf(key, sizeof(key), "foreign-%03lu",
                           (unsigned long)key_index) > 0);
      seed_foreign_outbox_state(client, "outbox-clean-reopen", key, "completed",
                                &error);
    }
  }
  for (index = 0U; index < OUTBOX_CLEAN_REOPEN_PENDING_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/clean-reopen-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "outbox-clean-reopen", key, &error);
  }
  lc_index_flush_req_init(&flush_request);
  memset(&flush_result, 0, sizeof(flush_result));
  flush_request.ns = "outbox-clean-reopen";
  flush_request.mode = "sync";
  assert_int_equal(
      lc_flush_index(client, &flush_request, &flush_result, &error), LC_OK);
  lc_index_flush_res_cleanup(&flush_result);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_query_req_init(&query_request);
  memset(&query_handler, 0, sizeof(query_handler));
  memset(&query_result, 0, sizeof(query_result));
  memset(&query_count, 0, sizeof(query_count));
  query_request.ns = "outbox-clean-reopen";
  query_request.selector_json =
      "{\"in\":{\"field\":\"/dispatch_state\",\"any\":[\"pending\","
      "\"retry_wait\"]}}";
  query_request.engine = "index";
  query_request.refresh = "wait_for";
  query_handler.begin = outbox_query_count_begin;
  query_handler.chunk = outbox_query_count_chunk;
  query_handler.end = outbox_query_count_end;
  assert_int_equal(lc_query_keys(client, &query_request, &query_handler,
                                 &query_count, &query_result, &error),
                   LC_OK);
  assert_int_equal(query_count.count, OUTBOX_CLEAN_REOPEN_PENDING_RECORDS);
  lc_query_res_cleanup(&query_result);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-clean-reopen";
  outbox_config.owner = "outbox-clean-reopen-test";
  outbox_config.notification_capacity = 1U;
  outbox = NULL;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &started), 0);
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &finished), 0);
  assert_true(outbox_elapsed_milliseconds(&started, &finished) < 5000L);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_clean_reopen_reconciles_durable_index(void **state) {
  (void)state;
  test_pouch_reopen_reconciles_durable_index_mode(0);
}

static void test_pouch_shared_reopen_reconciles_durable_index(void **state) {
  (void)state;
  test_pouch_reopen_reconciles_durable_index_mode(1);
}

/* Compaction is allowed to discard historical release records, but never the
 * final lease clear. Reconciliation must be able to claim every pending outbox
 * record immediately after a dense implicit-XA history is snapshotted. */
static void
test_pouch_compacted_reopen_reconciles_released_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_client_handle *client_handle;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  size_t index;
  size_t rewrite;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "compacted-reopen-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?segment_target_bytes=65536", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (rewrite = 0U; rewrite < 5U; ++rewrite) {
    for (index = 0U; index < 64U; ++index) {
      char key[128];

      assert_true(snprintf(key, sizeof(key),
                           "__lockdc_io/v1/outbox/compacted-terminal-%03lu",
                           (unsigned long)index) > 0);
      seed_terminal_outbox(client, "outbox-compacted-reopen", key, &error);
    }
  }
  for (index = 0U; index < 16U; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/compacted-pending-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "outbox-compacted-reopen", key, &error);
  }
  client_handle = (lc_client_handle *)client;
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  maintenance_options.ns = "outbox-compacted-reopen";
  maintenance_options.force = 1;
  assert_int_equal(lc_pouch_maintenance_run(client_handle->pouch,
                                            &maintenance_options,
                                            &maintenance_result, &error),
                   LC_OK);
  assert_true(maintenance_result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-compacted-reopen";
  outbox_config.owner = "outbox-compacted-reopen-test";
  outbox_config.notification_capacity = 16U;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  for (index = 0U; index < 16U; ++index) {
    job = NULL;
    assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    job = NULL;
  }

  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static long outbox_elapsed_milliseconds(const struct timespec *started,
                                        const struct timespec *finished) {
  long seconds = (long)(finished->tv_sec - started->tv_sec);
  long nanoseconds = (long)(finished->tv_nsec - started->tv_nsec);

  return seconds * 1000L + nanoseconds / 1000000L;
}

static void
test_pouch_dispatcher_wakeup_isolated_from_next_waiters(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_source *payload;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  outbox_next_wait_race race;
  pthread_t next_thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dispatcher-wakeup-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  transaction = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  lc_outbox_test_before_dispatcher_wait_hook = outbox_dispatcher_wait_hook;
  lc_outbox_test_before_dispatcher_wait_context = &race;
  lc_outbox_test_before_next_wait_hook = outbox_next_wait_hook;
  lc_outbox_test_before_next_wait_context = &race;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "dispatcher-wakeup";
  /* This race asserts only dispatcher/foreground wake isolation. Disable
   * unrelated Pouch retention maintenance so the private worker reaches its
   * untimed idle wait deterministically. */
  outbox_config.dead_letter_retention_seconds = -1L;
  outbox_config.completed_retention_seconds = -1L;
  outbox_config.inbox_retention_seconds = -1L;
  outbox_config.command_retention_seconds = -1L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  race.outbox = outbox;
  race.timeout_ms = outbox_slow_test_runtime() ? 10000L : 3000L;
  assert_true(outbox_next_wait_race_wait(&race, &race.dispatcher_wait_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_dispatcher_wait = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_int_equal(
      pthread_create(&next_thread, NULL, outbox_next_wait_thread, &race), 0);
  assert_true(outbox_next_wait_race_wait(&race, &race.next_wait_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_next_wait = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);

  lc_outbox_entry_init(&entry);
  entry.operation_id = "dispatcher-wakeup-operation";
  entry.effect_id = "dispatcher-wakeup-effect";
  entry.effect_key = "dispatcher-wakeup-key";
  entry.payload_digest = "sha256:dispatcher-wakeup-payload";
  entry.kind = "http";
  entry.destination = "https://example.invalid/dispatcher-wakeup";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  assert_true(outbox_next_wait_race_wait(&race, &race.next_finished));
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  assert_int_equal(race.next_rc, LC_OK);
  assert_non_null(race.job);
  assert_int_equal(lc_outbox_job_complete(race.job, NULL, &error), LC_OK);
  race.job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  outbox_reset_allocation_failures();
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_next_timeout_uses_one_deadline(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  outbox_next_wait_race race;
  pthread_t next_thread;
  struct timespec delay;
  long timeout_ms;
  long elapsed_ms;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "next-deadline-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  timeout_ms = outbox_slow_test_runtime() ? 1500L : 500L;
  delay.tv_sec = 0;
  delay.tv_nsec = outbox_slow_test_runtime() ? 200000000L : 100000000L;
  outbox_reset_allocation_failures();
  lc_outbox_test_before_next_wait_hook = outbox_next_wait_observed_hook;
  lc_outbox_test_before_next_wait_context = &race;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "next-deadline";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  race.outbox = outbox;
  race.timeout_ms = timeout_ms;
  assert_int_equal(
      pthread_create(&next_thread, NULL, outbox_next_wait_thread, &race), 0);
  assert_true(outbox_next_wait_race_wait(&race, &race.next_wait_entered));
  for (index = 0U; index < 3U; ++index) {
    (void)nanosleep(&delay, NULL);
    lc_outbox_test_wake_next_waiters(outbox);
  }
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  assert_int_equal(race.next_rc, LC_OK);
  assert_null(race.job);
  elapsed_ms = outbox_elapsed_milliseconds(&race.started, &race.finished);
  assert_true(elapsed_ms >= timeout_ms - timeout_ms / 4L);
  assert_true(elapsed_ms < timeout_ms + timeout_ms / 2L);
  outbox_reset_allocation_failures();
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_next_long_max_timeout_waits_until_dispatcher_stops(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  outbox_next_wait_race race;
  pthread_t next_thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "next-long-max-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  lc_outbox_test_before_next_wait_hook = outbox_next_wait_observed_hook;
  lc_outbox_test_before_next_wait_context = &race;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "next-long-max";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  dispatcher = outbox_test_dispatcher(outbox, &error);
  assert_non_null(dispatcher);
  race.outbox = outbox;
  race.timeout_ms = LONG_MAX;
  assert_int_equal(
      pthread_create(&next_thread, NULL, outbox_next_wait_thread, &race), 0);
  assert_true(outbox_next_wait_race_wait(&race, &race.next_wait_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  assert_false(race.next_finished);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  outbox_reset_allocation_failures();
  assert_int_equal(race.next_rc, LC_ERR_INVALID);
  assert_null(race.job);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_reconciliation_pages_large_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  struct timespec finished;
  long elapsed_ms;
  unsigned char seen[OUTBOX_RECONCILIATION_RECORDS];
  unsigned long delivered_index;
  size_t index;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "reconcile-large-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  memset(seen, 0, sizeof(seen));
  for (index = 0U; index < OUTBOX_RECONCILIATION_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/load-%03lu",
                         (unsigned long)index) > 0);
    /* Interleave abandoned claims with pending work across cursor pages. */
    seed_outbox_with_counters(client, "outbox-reconcile-large", key,
                              index % 16U == 0U ? "claimed" : "pending", "0",
                              "0", &error);
  }
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-reconcile-large";
  outbox_config.owner = "outbox-reconcile-large-test";
  outbox_config.notification_capacity = 16U;
  outbox = NULL;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &started), 0);
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  for (index = 0U; index < OUTBOX_RECONCILIATION_RECORDS; ++index) {
    job = NULL;
    assert_int_equal(lc_outbox_next(outbox, 30000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_non_null(job->outbox_key);
    assert_int_equal(sscanf(job->outbox_key, "__lockdc_io/v1/outbox/load-%lu",
                            &delivered_index),
                     1);
    assert_true(delivered_index < OUTBOX_RECONCILIATION_RECORDS);
    assert_false(seen[delivered_index]);
    seen[delivered_index] = 1;
    rc = lc_outbox_job_complete(job, NULL, &error);
    if (rc != LC_OK) {
      fail_msg("reconciled outbox completion failed: index=%lu key=%s rc=%d "
               "message=%s detail=%s",
               (unsigned long)index,
               job->outbox_key != NULL ? job->outbox_key : "(none)", rc,
               error.message != NULL ? error.message : "(none)",
               error.detail != NULL ? error.detail : "(none)");
    }
    assert_int_equal(rc, LC_OK);
    job = NULL;
  }
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &finished), 0);
  elapsed_ms = outbox_elapsed_milliseconds(&started, &finished);
  fprintf(stderr, "outbox reconciliation: %lu records in %ld ms\n",
          (unsigned long)OUTBOX_RECONCILIATION_RECORDS, elapsed_ms);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
  /* Release workers before a timing failure can longjmp into the next test. */
  assert_true(elapsed_ms < 30000L);
}

static void
test_pouch_reconciled_terminal_transport_failure_is_retryable(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *outbox_key = "__lockdc_io/v1/outbox/reconcile-terminal-retry";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_metadata_append_failure failure;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "reconcile-terminal-retry-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-reconcile-terminal-retry";
  client = NULL;
  outbox = NULL;
  job = NULL;
  memset(&failure, 0, sizeof(failure));
  failure.failure_call = 0U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-reconcile-terminal-retry", outbox_key,
                       &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-reconcile-terminal-retry";
  outbox_config.owner = "outbox-reconcile-terminal-retry-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);

  /* Model the uncertain I/O result that previously escaped from a reconciled
   * completion. Pouch rolls the unconfirmed append back, so a terminal error
   * must leave the existing claim valid for an explicit retry. */
  lc_pouch_test_after_metadata_batch_append_context = &failure;
  lc_pouch_test_after_metadata_batch_append_hook =
      outbox_fail_metadata_append_at_call;
  rc = lc_outbox_job_complete(job, NULL, &error);
  lc_pouch_test_after_metadata_batch_append_hook = NULL;
  lc_pouch_test_after_metadata_batch_append_context = NULL;
  assert_true(failure.calls >= 1U);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  outbox_assert_outbox_completed(client, outbox_key, &error);
  assert_int_equal(lc_outbox_next(outbox, 250L, &job, &error), LC_OK);
  assert_null(job);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_reconciled_terminal_release_failure_is_retryable(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *outbox_key =
      "__lockdc_io/v1/outbox/reconcile-terminal-release-retry";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_metadata_append_failure failure;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX
                       "reconcile-terminal-release-retry-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-reconcile-terminal-release-retry";
  client = NULL;
  outbox = NULL;
  job = NULL;
  memset(&failure, 0, sizeof(failure));
  /* The first append advances the lease's staged-version cursor; fail the
   * second append, which is the terminal release/commit batch. */
  failure.failure_call = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-reconcile-terminal-release-retry",
                       outbox_key, &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-reconcile-terminal-release-retry";
  outbox_config.owner = "outbox-reconcile-terminal-release-retry-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  assert_non_null(job);

  lc_pouch_test_after_metadata_batch_append_context = &failure;
  lc_pouch_test_after_metadata_batch_append_hook =
      outbox_fail_metadata_append_at_call;
  rc = lc_outbox_job_complete(job, NULL, &error);
  lc_pouch_test_after_metadata_batch_append_hook = NULL;
  lc_pouch_test_after_metadata_batch_append_context = NULL;
  assert_true(failure.calls >= 2U);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_job_dead_letter(job,
                                             "must not replace staged "
                                             "completion",
                                             &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  outbox_assert_outbox_completed(client, outbox_key, &error);
  assert_int_equal(lc_outbox_next(outbox, 250L, &job, &error), LC_OK);
  assert_null(job);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void outbox_worker_release_gate(void *context) {
  pthread_barrier_t *gates = (pthread_barrier_t *)context;

  (void)pthread_barrier_wait(&gates[0]);
  (void)pthread_barrier_wait(&gates[1]);
}

static void test_pouch_dispatcher_wait_includes_worker_release(void **state) {
  char root[256];
  char template_path[] = OUTBOX_TMP_PREFIX "worker-release-XXXXXX";
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config config;
  lc_outbox_config outbox_config;
  outbox_tracking_allocator allocator;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  pthread_barrier_t gates[2];
  int probe_rc;

  (void)state;
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  outbox_tracking_allocator_init(&allocator);
  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.allocator.malloc_fn = outbox_tracking_malloc;
  config.allocator.realloc_fn = outbox_tracking_realloc;
  config.allocator.free_fn = outbox_tracking_free;
  config.allocator.context = &allocator;
  assert_int_equal(lc_client_open(&config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "worker-release";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(
      lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error), LC_OK);
  assert_int_equal(pthread_barrier_init(&gates[0], NULL, 2U), 0);
  assert_int_equal(pthread_barrier_init(&gates[1], NULL, 2U), 0);
  lc_outbox_test_before_worker_release_context = gates;
  lc_outbox_test_before_worker_release_hook = outbox_worker_release_gate;
  (void)lc_outbox_dispatcher_stop(dispatcher, 0L, &error);
  (void)pthread_barrier_wait(&gates[0]);
  /* The worker is deliberately paused while it still owns a reference. Stop
   * must not report completion or let an embedding allocator go out of scope.
   */
  probe_rc = lc_outbox_dispatcher_wait(dispatcher, 0L, &error);
  (void)pthread_barrier_wait(&gates[1]);
  assert_int_equal(lc_outbox_dispatcher_wait(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_outbox_test_before_worker_release_hook = NULL;
  lc_outbox_test_before_worker_release_context = NULL;
  pthread_barrier_destroy(&gates[0]);
  pthread_barrier_destroy(&gates[1]);
  assert_int_equal(probe_rc, LC_ERR_TIMEOUT);
  outbox_tracking_allocator_destroy(&allocator);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_reconciliation_preserves_allocator_domains(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_tracking_allocator allocator;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_error error;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "reconcile-cursor-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < 3U; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/cursor-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "outbox-reconcile-cursor", key, &error);
  }
  lc_client_close(client);
  outbox_tracking_allocator_init(&allocator);
  client_config.allocator.malloc_fn = outbox_tracking_malloc;
  client_config.allocator.realloc_fn = outbox_tracking_realloc;
  client_config.allocator.free_fn = outbox_tracking_free;
  client_config.allocator.context = &allocator;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-reconcile-cursor";
  outbox_config.owner = "outbox-reconcile-cursor-test";
  outbox_config.notification_capacity = 1U;
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  for (index = 0U; index < 3U; ++index) {
    job = NULL;
    assert_int_equal(lc_outbox_next(outbox, 30000L, &job, &error), LC_OK);
    assert_non_null(job);
    /* A successful terminal operation consumes the job.  The tracking
     * allocator below proves that it also releases retained client/outbox
     * resources without an explicit close from the caller. */
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    job = NULL;
  }
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  outbox_tracking_allocator_destroy(&allocator);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_dispatcher_is_passive_until_consumer_demand(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_error error;
  lc_outbox_stats stats;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "prefetch-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < OUTBOX_PREFETCH_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/prefetch-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "outbox-prefetch", key, &error);
  }
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-prefetch";
  outbox_config.owner = "outbox-prefetch-test";
  outbox_config.notification_capacity = 2U;
  outbox_config.recovery_interval_seconds = 1L;
  /* Do not go through the legacy fixture adapter: this is the production
   * producer contract. Construction is threadless and has no recovery state. */
  assert_int_equal(client->new_outbox(client, &outbox_config, &outbox, &error),
                   LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  assert_non_null(dispatcher);
  memset(&stats, 0, sizeof(stats));
  assert_int_equal(dispatcher->get_stats(dispatcher, &stats, &error), LC_OK);
  assert_int_equal(stats.recovery_queries, 0U);
  assert_int_equal(stats.recovered_claims, 0U);
  lc_outbox_stats_cleanup(&stats);
  /* The interval is not an autonomous polling timer. It is armed only after
   * an actual blocking consumer request or explicit reconciliation. Keep this
   * deterministic instead of sleeping and hoping to observe no I/O. */
  assert_false(
      lc_outbox_test_dispatcher_periodic_recovery_is_armed(dispatcher));

  /* Starting private recovery may discover candidates, but it must not take a
   * lease until a real consumer asks for one.  Probe every seeded record after
   * the dispatcher exists; each lease remains freely acquirable. */
  for (index = 0U; index < OUTBOX_PREFETCH_RECORDS; ++index) {
    char key[128];
    lc_acquire_req acquire;
    lc_lease *lease;

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/prefetch-%03lu",
                         (unsigned long)index) > 0);
    lc_acquire_req_init(&acquire);
    acquire.ns = "outbox-prefetch";
    acquire.key = key;
    acquire.owner = "outbox-prefetch-probe";
    acquire.ttl_seconds = 30L;
    lease = NULL;
    assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
    assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  }

  /* A zero-deadline probe must not turn host polling into an indexed scan. */
  assert_int_equal(dispatcher->next(dispatcher, 0L, &job, &error), LC_OK);
  assert_null(job);
  memset(&stats, 0, sizeof(stats));
  assert_int_equal(dispatcher->get_stats(dispatcher, &stats, &error), LC_OK);
  assert_int_equal(stats.recovery_queries, 0U);
  assert_int_equal(stats.recovered_claims, 0U);
  lc_outbox_stats_cleanup(&stats);

  /* One demand claims exactly one job.  No polling or prefetch sleep is part
   * of this invariant. */
  assert_int_equal(dispatcher->next(dispatcher, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_dispatcher_scan_recovery_when_indexing_is_disabled(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[352];
  const char *endpoints[1];
  const char *outbox_key = "__lockdc_io/v1/outbox/scan-recovery";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "scan-recovery-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-scan-recovery", outbox_key, &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-scan-recovery";
  outbox_config.owner = "outbox-scan-recovery-test";
  assert_int_equal(client->new_outbox(client, &outbox_config, &outbox, &error),
                   LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  assert_int_equal(dispatcher->next(dispatcher, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

typedef struct outbox_client_close_callback_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  lc_client *client;
  lc_outbox_dispatcher *dispatcher;
  int recovery_query_entered;
  int allow_recovery_query;
  int client_close_returned;
  int source_reads;
} outbox_client_close_callback_race;

static void outbox_client_close_callback_before_recovery_query(void *context) {
  outbox_client_close_callback_race *race =
      (outbox_client_close_callback_race *)context;

  assert_non_null(race);
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  race->recovery_query_entered = 1;
  assert_int_equal(pthread_cond_broadcast(&race->condition), 0);
  while (!race->allow_recovery_query) {
    assert_int_equal(pthread_cond_wait(&race->condition, &race->mutex), 0);
  }
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
}

static size_t outbox_client_close_callback_source_read(void *context,
                                                       void *buffer,
                                                       size_t count,
                                                       lc_error *error) {
  outbox_client_close_callback_race *race =
      (outbox_client_close_callback_race *)context;
  lc_error reconcile_error;
  int rc;

  if (race->source_reads != 0) {
    return 0U;
  }
  ++race->source_reads;
  lc_error_init(&reconcile_error);
  rc = lc_outbox_dispatcher_reconcile(race->dispatcher, &reconcile_error);
  lc_error_cleanup(&reconcile_error);
  if (rc != LC_OK) {
    (void)lc_error_set(error, rc, 0L,
                       "failed to schedule recovery from source callback", NULL,
                       NULL, NULL);
    return 0U;
  }
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  while (!race->recovery_query_entered) {
    assert_int_equal(pthread_cond_wait(&race->condition, &race->mutex), 0);
  }
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  /* This is the formerly cyclic close: the source callback owns the namespace
   * lock while recovery has reached its attempt to acquire that same lock. */
  lc_client_close(race->client);
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  race->client_close_returned = 1;
  race->allow_recovery_query = 1;
  assert_int_equal(pthread_cond_broadcast(&race->condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  if (count < 2U) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "outbox close-callback source buffer is too small", NULL,
                       NULL, NULL);
    return 0U;
  }
  memcpy(buffer, "{}", 2U);
  return 2U;
}

static void test_pouch_client_close_from_source_callback(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_acquire_req acquire;
  outbox_client_close_callback_race race;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_lease *lease;
  lc_source *source;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "client-close-callback-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  lease = NULL;
  source = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-client-close-callback";
  outbox_config.owner = "outbox-client-close-callback-owner";
  outbox_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(
      lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error), LC_OK);
  race.client = client;
  race.dispatcher = dispatcher;
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox_config.ns;
  acquire.key = "callback-state";
  acquire.owner = "outbox-client-close-callback-lease";
  acquire.ttl_seconds = 30L;
  assert_int_equal(client->acquire(client, &acquire, &lease, &error), LC_OK);
  assert_int_equal(
      lc_source_from_callbacks(outbox_client_close_callback_source_read, NULL,
                               NULL, &race, &source, &error),
      LC_OK);
  lc_outbox_test_before_recovery_query_hook =
      outbox_client_close_callback_before_recovery_query;
  lc_outbox_test_before_recovery_query_context = &race;
  assert_int_equal(lease->update(lease, source, NULL, &error), LC_OK);
  lc_outbox_test_before_recovery_query_hook = NULL;
  lc_outbox_test_before_recovery_query_context = NULL;
  lc_source_close(source);
  source = NULL;
  assert_int_equal(race.client_close_returned, 1);
  lc_lease_close(lease);
  lease = NULL;
  /* The callback returns before recovery has acquired its lock. This explicit
   * wait proves the deferred worker cleanup reaches the normal terminal state
   * without leaking a registered dispatcher. */
  assert_int_equal(lc_outbox_dispatcher_wait(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  dispatcher = NULL;
  lc_outbox_close(outbox);
  outbox = NULL;
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

typedef struct outbox_callback_fixture {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
} outbox_callback_fixture;

typedef struct outbox_close_owner_callback {
  const char *bytes;
  size_t length;
  size_t offset;
  void *owner;
  void (*close_owner)(void *owner);
  unsigned int close_calls;
} outbox_close_owner_callback;

static void outbox_callback_fixture_open(outbox_callback_fixture *fixture,
                                         const char *ns) {
  lc_client_config client_config;
  lc_outbox_config outbox_config;

  memset(fixture, 0, sizeof(*fixture));
  assert_true(snprintf(fixture->template_path, sizeof(fixture->template_path),
                       OUTBOX_TMP_PREFIX "callback-owner-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(fixture->template_path, fixture->root,
                                  sizeof(fixture->root), OUTBOX_TMP_PREFIX));
  assert_true(snprintf(fixture->endpoint, sizeof(fixture->endpoint),
                       "pouch://%s", fixture->root) > 0);
  fixture->endpoints[0] = fixture->endpoint;
  lc_error_init(&fixture->error);
  lc_client_config_init(&client_config);
  client_config.endpoints = fixture->endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(
      lc_client_open(&client_config, &fixture->client, &fixture->error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = ns;
  outbox_config.owner = "outbox-callback-owner-test";
  assert_int_equal(lc_client_new_outbox(fixture->client, &outbox_config,
                                        &fixture->outbox, &fixture->error),
                   LC_OK);
}

static void outbox_callback_fixture_close(outbox_callback_fixture *fixture) {
  if (fixture->outbox != NULL) {
    lc_outbox_close(fixture->outbox);
    fixture->outbox = NULL;
  }
  if (fixture->client != NULL) {
    lc_client_close(fixture->client);
    fixture->client = NULL;
  }
  lc_error_cleanup(&fixture->error);
  lc_test_tmp_cleanup_path(fixture->root, OUTBOX_TMP_PREFIX);
}

static void outbox_callback_entry_init(lc_outbox_entry *entry) {
  lc_outbox_entry_init(entry);
  entry->operation_id = "callback-operation";
  entry->effect_id = "callback-effect";
  entry->effect_key = "callback-idempotency";
  entry->payload_digest = "sha256:callback-payload";
  entry->kind = "test";
  entry->destination = "callback://target";
  entry->content_type = "text/plain";
}

static size_t outbox_close_owner_source_read(void *context, void *buffer,
                                             size_t count, lc_error *error) {
  outbox_close_owner_callback *callback =
      (outbox_close_owner_callback *)context;
  size_t remaining;
  size_t written;

  (void)error;
  if (callback->close_calls == 0U) {
    ++callback->close_calls;
    callback->close_owner(callback->owner);
  }
  remaining = callback->length - callback->offset;
  written = remaining < count ? remaining : count;
  if (written > 0U) {
    memcpy(buffer, callback->bytes + callback->offset, written);
    callback->offset += written;
  }
  return written;
}

static int outbox_close_owner_sink_write(lc_sink *self, const void *bytes,
                                         size_t length, lc_error *error) {
  outbox_close_owner_callback *callback =
      (outbox_close_owner_callback *)self->impl;

  (void)bytes;
  (void)length;
  (void)error;
  if (callback->close_calls == 0U) {
    ++callback->close_calls;
    callback->close_owner(callback->owner);
  }
  return 1;
}

static void outbox_close_owner_sink_close(lc_sink *self) { (void)self; }

static void outbox_close_owner_outbox(void *owner) {
  lc_outbox_close((lc_outbox *)owner);
}

static void outbox_close_owner_transaction(void *owner) {
  lc_outbox_transaction_close((lc_outbox_transaction *)owner);
}

static void outbox_close_owner_participant(void *owner) {
  lc_outbox_participant_close((lc_outbox_participant *)owner);
}

static void outbox_close_owner_job(void *owner) {
  lc_outbox_job_close((lc_outbox_job *)owner);
}

static void test_pouch_outbox_append_pins_close_callback_owner(void **state) {
  outbox_callback_fixture fixture;
  outbox_close_owner_callback callback;
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_source *source;

  (void)state;
  outbox_callback_fixture_open(&fixture, "callback-close-outbox");
  memset(&callback, 0, sizeof(callback));
  callback.bytes = "payload";
  callback.length = strlen(callback.bytes);
  callback.owner = fixture.outbox;
  callback.close_owner = outbox_close_owner_outbox;
  source = NULL;
  assert_int_equal(lc_source_from_callbacks(outbox_close_owner_source_read,
                                            NULL, NULL, &callback, &source,
                                            &fixture.error),
                   LC_OK);
  outbox_callback_entry_init(&entry);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(lc_outbox_append(fixture.outbox, &entry, source,
                                    &transaction, &receipt, &fixture.error),
                   LC_ERR_INVALID);
  assert_int_equal(callback.close_calls, 1U);
  assert_null(transaction);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(source);
  fixture.outbox = NULL;
  outbox_callback_fixture_close(&fixture);
}

static void
test_pouch_outbox_transaction_append_pins_close_callback_owner(void **state) {
  outbox_callback_fixture fixture;
  outbox_close_owner_callback callback;
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_source *source;

  (void)state;
  outbox_callback_fixture_open(&fixture, "callback-close-transaction");
  transaction = NULL;
  assert_int_equal(
      lc_outbox_begin(fixture.outbox, &transaction, &fixture.error), LC_OK);
  assert_non_null(transaction);
  memset(&callback, 0, sizeof(callback));
  callback.bytes = "payload";
  callback.length = strlen(callback.bytes);
  callback.owner = transaction;
  callback.close_owner = outbox_close_owner_transaction;
  source = NULL;
  assert_int_equal(lc_source_from_callbacks(outbox_close_owner_source_read,
                                            NULL, NULL, &callback, &source,
                                            &fixture.error),
                   LC_OK);
  outbox_callback_entry_init(&entry);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_outbox_transaction_append(transaction, &entry, source,
                                                &receipt, &fixture.error),
                   LC_ERR_INVALID);
  assert_int_equal(callback.close_calls, 1U);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(source);
  outbox_callback_fixture_close(&fixture);
}

static void
test_pouch_outbox_participant_update_pins_close_callback_owner(void **state) {
  outbox_callback_fixture fixture;
  outbox_close_owner_callback callback;
  lc_outbox_participant_request request;
  lc_outbox_participant *participant;
  lc_outbox_transaction *transaction;
  lc_source *source;

  (void)state;
  outbox_callback_fixture_open(&fixture, "callback-close-participant");
  transaction = NULL;
  participant = NULL;
  assert_int_equal(
      lc_outbox_begin(fixture.outbox, &transaction, &fixture.error), LC_OK);
  lc_outbox_participant_request_init(&request);
  request.acquire.ns = "callback-close-participant";
  request.acquire.key = "callback-domain";
  request.acquire.owner = "callback-owner";
  request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_outbox_transaction_acquire(transaction, &request,
                                                 &participant, &fixture.error),
                   LC_OK);
  memset(&callback, 0, sizeof(callback));
  callback.bytes = "{}";
  callback.length = strlen(callback.bytes);
  callback.owner = participant;
  callback.close_owner = outbox_close_owner_participant;
  source = NULL;
  assert_int_equal(lc_source_from_callbacks(outbox_close_owner_source_read,
                                            NULL, NULL, &callback, &source,
                                            &fixture.error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_participant_update(participant, source, NULL, &fixture.error),
      LC_OK);
  assert_int_equal(callback.close_calls, 1U);
  participant = NULL;
  lc_source_close(source);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &fixture.error),
                   LC_OK);
  lc_outbox_transaction_close(transaction);
  outbox_callback_fixture_close(&fixture);
}

static void
test_pouch_outbox_job_payload_pins_close_callback_owner(void **state) {
  outbox_callback_fixture fixture;
  outbox_close_owner_callback callback;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_sink sink;
  lc_source *source;
  size_t written;

  (void)state;
  outbox_callback_fixture_open(&fixture, "callback-close-job");
  outbox_callback_entry_init(&entry);
  source = NULL;
  assert_int_equal(
      lc_source_from_memory("payload", 7U, &source, &fixture.error), LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(lc_outbox_append(fixture.outbox, &entry, source,
                                    &transaction, &receipt, &fixture.error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &fixture.error),
                   LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(source);
  job = NULL;
  assert_int_equal(
      outbox_test_next(fixture.outbox, 3000L, &job, &fixture.error), LC_OK);
  assert_non_null(job);
  memset(&callback, 0, sizeof(callback));
  callback.owner = job;
  callback.close_owner = outbox_close_owner_job;
  memset(&sink, 0, sizeof(sink));
  sink.impl = &callback;
  sink.write = outbox_close_owner_sink_write;
  sink.close = outbox_close_owner_sink_close;
  written = 0U;
  assert_int_equal(
      lc_outbox_job_write_payload(job, &sink, &written, &fixture.error), LC_OK);
  assert_int_equal(callback.close_calls, 1U);
  assert_int_equal(written, 7U);
  job = NULL;
  outbox_callback_fixture_close(&fixture);
}

static void
test_pouch_dispatcher_rejects_client_closed_replacement(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox *rejected_outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "client-closed-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  rejected_outbox = NULL;
  dispatcher = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-client-closed";
  assert_int_equal(client->new_outbox(client, &outbox_config, &outbox, &error),
                   LC_OK);
  lc_client_close(client);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_ERR_INVALID);
  assert_null(dispatcher);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      client->new_outbox(client, &outbox_config, &rejected_outbox, &error),
      LC_ERR_INVALID);
  assert_null(rejected_outbox);
  lc_error_cleanup(&error);
  lc_outbox_close(outbox);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_dispatcher_stop_timeout_preserves_handed_out_job(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  const char *outbox_key = "__lockdc_io/v1/outbox/stop-timeout";
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox *second_outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_dispatcher *second_dispatcher;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "stop-timeout-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  second_outbox = NULL;
  dispatcher = NULL;
  second_dispatcher = NULL;
  job = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-stop-timeout", outbox_key, &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-stop-timeout";
  outbox_config.owner = "outbox-stop-timeout-test";
  assert_int_equal(client->new_outbox(client, &outbox_config, &outbox, &error),
                   LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  assert_int_equal(dispatcher->next(dispatcher, 5000L, &job, &error), LC_OK);
  assert_non_null(job);

  /* Shutdown requests are immediate, but neither private cleanup nor a
   * handed-out durable claim is discarded to satisfy a zero deadline. */
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, 0L, &error),
                   LC_ERR_TIMEOUT);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_dispatcher_wait(dispatcher, 0L, &error),
                   LC_ERR_TIMEOUT);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(
      client->new_outbox(client, &outbox_config, &second_outbox, &error),
      LC_OK);
  assert_int_equal(second_outbox->get_or_start_dispatcher(
                       second_outbox, &second_dispatcher, &error),
                   LC_ERR_INVALID);
  assert_null(second_dispatcher);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_outbox_dispatcher_wait(dispatcher, -1L, &error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  assert_int_equal(second_outbox->get_or_start_dispatcher(
                       second_outbox, &second_dispatcher, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_dispatcher_stop(second_dispatcher, -1L, &error),
                   LC_OK);
  lc_outbox_dispatcher_close(second_dispatcher);
  lc_outbox_close(second_outbox);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_dispatcher_stop_retains_inflight_receiver(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_dispatcher_core_race race;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  pthread_t stats_thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dispatcher-core-race-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "dispatcher-core-race";
  outbox_config.owner = "dispatcher-core-race-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  race.dispatcher = dispatcher;
  lc_outbox_test_after_dispatcher_core_retain_hook =
      outbox_dispatcher_core_retained_hook;
  lc_outbox_test_after_dispatcher_core_retain_context = &race;
  assert_int_equal(pthread_create(&stats_thread, NULL,
                                  outbox_dispatcher_stats_thread, &race),
                   0);
  assert_true(outbox_dispatcher_core_race_wait(&race, &race.core_retained));

  /* stop() may detach its core while a receiver is already in flight. The
   * receiver's acquired core reference must keep its snapshot valid. */
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &error), LC_OK);
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_stats = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(outbox_dispatcher_core_race_wait(&race, &race.stats_finished));
  assert_int_equal(pthread_join(stats_thread, NULL), 0);
  assert_int_equal(race.stats_rc, LC_OK);
  outbox_reset_allocation_failures();
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_shared_process_dispatches_once(void **state) {
  char root[256], template_path[256], endpoint[320], start;
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  outbox_process_result child_result;
  lc_error error;
  int start_pipe[2], result_pipe[2], status, parent_got_job;
  pid_t child;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "shared-process-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-shared-process",
                       "__lockdc_io/v1/outbox/shared-process", &error);
  lc_client_close(client);
  assert_int_equal(pipe(start_pipe), 0);
  assert_int_equal(pipe(result_pipe), 0);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    outbox_process_result result;
    (void)close(start_pipe[1]);
    (void)close(result_pipe[0]);
    result = outbox_shared_process_claim(root, "outbox-shared-process",
                                         start_pipe[0]);
    if (write(result_pipe[1], &result, sizeof(result)) !=
        (ssize_t)sizeof(result))
      _exit(1);
    (void)close(result_pipe[1]);
    _exit(result.rc == LC_OK ? 0 : 1);
  }
  (void)close(start_pipe[0]);
  (void)close(result_pipe[1]);
  client = NULL;
  outbox = NULL;
  job = NULL;
  parent_got_job = 0;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-shared-process";
  outbox_config.owner = "outbox-shared-parent";
  outbox_config.recovery_interval_seconds = 1L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  start = 's';
  assert_int_equal(write(start_pipe[1], &start, 1U), 1);
  (void)close(start_pipe[1]);
  assert_int_equal(lc_outbox_next(outbox, 5000L, &job, &error), LC_OK);
  if (job != NULL) {
    parent_got_job = 1;
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    job = NULL;
  }
  assert_int_equal(read(result_pipe[0], &child_result, sizeof(child_result)),
                   (ssize_t)sizeof(child_result));
  (void)close(result_pipe[0]);
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status));
  assert_int_equal(WEXITSTATUS(status), 0);
  assert_int_equal(child_result.rc, LC_OK);
  assert_true((parent_got_job != 0) != (child_result.got_job != 0));
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_shared_process_reconciles_each_outbox_once(void **state) {
  char root[256], template_path[256], endpoint[384];
  const char *endpoints[1];
  lc_client_config client_config;
  outbox_process_result results[2];
  lc_client *client;
  lc_error error;
  int start_pipes[2][2];
  int result_pipes[2][2];
  int status;
  pid_t children[2];
  size_t index;
  unsigned long delivered;

  (void)state;
  memset(start_pipes, -1, sizeof(start_pipes));
  memset(result_pipes, -1, sizeof(result_pipes));
  memset(children, 0, sizeof(children));
  memset(results, 0, sizeof(results));
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "shared-reconcile-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(
      snprintf(endpoint, sizeof(endpoint),
               "pouch://%s?single_writer=false&segment_target_bytes=65536",
               root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-shared-reconcile";
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < OUTBOX_SHARED_PROCESS_RECORDS; ++index) {
    char key[160];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/shared-reconcile-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, client_config.default_namespace, key, &error);
  }
  lc_client_close(client);
  client = NULL;

  for (index = 0U; index < 2U; ++index) {
    char owner[64];

    assert_int_equal(pipe(start_pipes[index]), 0);
    assert_int_equal(pipe(result_pipes[index]), 0);
    children[index] = fork();
    assert_true(children[index] >= 0);
    if (children[index] == 0) {
      outbox_process_result child_result;

      (void)close(start_pipes[index][1]);
      (void)close(result_pipes[index][0]);
      assert_true(snprintf(owner, sizeof(owner), "outbox-shared-drain-%lu",
                           (unsigned long)index) > 0);
      child_result = outbox_shared_process_drain(
          root, client_config.default_namespace, owner, start_pipes[index][0]);
      if (write(result_pipes[index][1], &child_result, sizeof(child_result)) !=
          (ssize_t)sizeof(child_result))
        _exit(1);
      (void)close(result_pipes[index][1]);
      _exit(child_result.rc == LC_OK ? 0 : 1);
    }
    (void)close(start_pipes[index][0]);
    start_pipes[index][0] = -1;
    (void)close(result_pipes[index][1]);
    result_pipes[index][1] = -1;
  }
  for (index = 0U; index < 2U; ++index) {
    char start;

    start = 's';
    assert_int_equal(write(start_pipes[index][1], &start, 1U), 1);
    (void)close(start_pipes[index][1]);
    start_pipes[index][1] = -1;
  }
  delivered = 0UL;
  for (index = 0U; index < 2U; ++index) {
    assert_int_equal(
        read(result_pipes[index][0], &results[index], sizeof(results[index])),
        (ssize_t)sizeof(results[index]));
    (void)close(result_pipes[index][0]);
    result_pipes[index][0] = -1;
    assert_int_equal(waitpid(children[index], &status, 0), children[index]);
    assert_true(WIFEXITED(status));
    if (results[index].rc != LC_OK) {
      (void)fprintf(stderr,
                    "shared outbox drain %lu failed: rc=%d message=%s "
                    "detail=%s\n",
                    (unsigned long)index, results[index].rc,
                    results[index].error_message, results[index].error_detail);
    }
    assert_int_equal(results[index].rc, LC_OK);
    assert_int_equal(WEXITSTATUS(status), 0);
    delivered += results[index].delivered;
  }
  assert_int_equal(delivered, OUTBOX_SHARED_PROCESS_RECORDS);

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < OUTBOX_SHARED_PROCESS_RECORDS; ++index) {
    char key[160];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/shared-reconcile-%03lu",
                         (unsigned long)index) > 0);
    outbox_assert_outbox_completed(client, key, &error);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_shared_renewal_publishes_recovery_deadline(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config first_config, second_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *first, *second;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  lc_outbox_stats stats;
  struct timespec delay;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "renewed-claim-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  first = NULL;
  second = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&first_config);
  first_config.ns = "outbox-renewed-claim";
  first_config.owner = "outbox-renewed-claim-first";
  first_config.claim_ttl_seconds = outbox_claim_ttl_seconds();
  assert_int_equal(lc_client_new_outbox(client, &first_config, &first, &error),
                   LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "renewed-claim-operation";
  entry.effect_id = "renewed-claim-effect";
  entry.effect_key = "renewed-claim-key";
  entry.payload_digest = "sha256:renewed-claim";
  entry.kind = "test";
  entry.destination = "test://renewed-claim";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(first, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(
      lc_outbox_next(first, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_renew(job, 5L, &error), LC_OK);

  delay.tv_sec = outbox_claim_expiry_wait_seconds();
  delay.tv_nsec = 0L;
  (void)nanosleep(&delay, NULL);
  lc_outbox_config_init(&second_config);
  second_config.ns = first_config.ns;
  second_config.owner = "outbox-renewed-claim-second";
  second_config.recovery_interval_seconds = 0L;
  assert_int_equal(
      lc_client_new_outbox(client, &second_config, &second, &error), LC_OK);
  assert_int_equal(lc_outbox_reconcile(second, &error), LC_OK);
  {
    lc_outbox_job *observed = (lc_outbox_job *)1;

    assert_int_equal(lc_outbox_next(second, 250L, &observed, &error), LC_OK);
    assert_null(observed);
  }
  memset(&stats, 0, sizeof(stats));
  assert_int_equal(lc_outbox_get_stats(second, &stats, &error), LC_OK);
  /* Without the renewed durable deadline, each claimed-record pass schedules
   * another immediately-due reconciliation after its stale original expiry. */
  assert_true(stats.recovery_queries < 8U);
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_outbox_close(second);
  lc_outbox_close(first);
  lc_outbox_receipt_cleanup(&receipt);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_outbox_renewal_uses_server_expiry(void **state) {
  outbox_renewal_lease renewal;
  lc_error error;

  (void)state;
  memset(&renewal, 0, sizeof(renewal));
  renewal.lease.keepalive = outbox_renewal_keepalive;
  renewal.lease.lease_expires_at_unix = LONG_MAX - 100L;
  renewal.renewed_expiry = LONG_MAX - 50L;
  lc_error_init(&error);
  /* A remote server can legitimately be far ahead of this process's clock.
   * Its returned expiry, not a client-side now+ttl estimate, is authoritative.
   */
  assert_int_equal(lc_outbox_test_renew_claim_lease(&renewal.lease, 5L, &error),
                   LC_OK);
  assert_int_equal(renewal.calls, 1U);
  assert_int_equal(renewal.requested_ttl, 5L);
  assert_int_equal(renewal.lease.lease_expires_at_unix, LONG_MAX - 50L);
  lc_error_cleanup(&error);
}

static void test_pouch_failed_renew_does_not_publish_future_recovery_deadline(
    void **state) {
  char root[256], template_path[256], endpoint[320], deadline_key[128];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "failed-renew-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-failed-renew";
  outbox_config.owner = "outbox-failed-renew-test";
  outbox_config.claim_ttl_seconds = 30L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "failed-renew-operation";
  entry.effect_id = "failed-renew-effect";
  entry.effect_key = "failed-renew-key";
  entry.payload_digest = "sha256:failed-renew";
  entry.kind = "test";
  entry.destination = "test://failed-renew";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);

  outbox_claim_deadline_key(receipt.outbox_key, deadline_key);
  outbox_assert_public_state_absent(client, deadline_key, &error);
  /* A failed renewal must not persist the requested future deadline. This
   * fault is injected before the lease operation, so it is not clock-sensitive.
   */
  lc_outbox_test_before_outbox_renew_keepalive_hook = outbox_fail_allocation;
  assert_int_equal(lc_outbox_job_renew(job, 300L, &error), LC_ERR_NOMEM);
  lc_outbox_test_before_outbox_renew_keepalive_hook = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  outbox_assert_public_state_absent(client, deadline_key, &error);

  lc_outbox_job_close(job);
  job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_failed_renew_refreshes_recovery_from_server_expiry(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  lc_i64 original_deadline, scheduled_deadline;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "failed-renew-recovery-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-failed-renew-recovery";
  outbox_config.owner = "outbox-failed-renew-recovery-test";
  outbox_config.claim_ttl_seconds = 120L;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "failed-renew-recovery-operation";
  entry.effect_id = "failed-renew-recovery-effect";
  entry.effect_key = "failed-renew-recovery-key";
  entry.payload_digest = "sha256:failed-renew-recovery";
  entry.kind = "test";
  entry.destination = "test://failed-renew-recovery";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  original_deadline = job->lease_expires_at_unix;

  /* Keepalive mutates the real Pouch lease before this injected companion
   * publication failure. The local scheduler must retain that new deadline. */
  lc_outbox_test_before_outbox_renew_deadline_publish_hook =
      outbox_fail_allocation;
  assert_int_equal(lc_outbox_job_renew(job, 300L, &error), LC_ERR_NOMEM);
  lc_outbox_test_before_outbox_renew_deadline_publish_hook = NULL;
  assert_true(job->lease_expires_at_unix > original_deadline);
  assert_true(lc_outbox_test_dispatcher_delayed_recovery_deadline(
      dispatcher, receipt.outbox_key, &scheduled_deadline));
  assert_int_equal(scheduled_deadline, job->lease_expires_at_unix);

  lc_error_cleanup(&error);
  lc_outbox_job_close(job);
  job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  outbox_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_expired_claim_rejects_stale_terminal(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *first, *second;
  lc_outbox_job *stale, *replacement;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "stale-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-stale-terminal",
                       "__lockdc_io/v1/outbox/stale-terminal", &error);
  lc_outbox_config_init(&config);
  config.ns = "outbox-stale-terminal";
  config.owner = "outbox-stale-first";
  config.claim_ttl_seconds = outbox_claim_ttl_seconds();
  first = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &first, &error),
                   LC_OK);
  stale = NULL;
  assert_int_equal(
      lc_outbox_next(first, outbox_claim_next_timeout_ms(), &stale, &error),
      LC_OK);
  assert_non_null(stale);
  lc_outbox_close(first);
  sleep(outbox_claim_expiry_wait_seconds());
  config.owner = "outbox-stale-second";
  second = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &second, &error),
                   LC_OK);
  replacement = NULL;
  assert_int_equal(lc_outbox_next(second, outbox_claim_next_timeout_ms(),
                                  &replacement, &error),
                   LC_OK);
  assert_non_null(replacement);
  assert_string_equal(replacement->effect_key, stale->effect_key);
  assert_true(lc_outbox_job_complete(stale, NULL, &error) != LC_OK);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_complete(replacement, NULL, &error), LC_OK);
  replacement = NULL;
  lc_outbox_job_close(stale);
  lc_outbox_close(second);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_expired_claim_recovers_and_preserves_attempt_budget(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *first, *second, *unexpected;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "expired-budget-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-expired-budget",
                       "__lockdc_io/v1/outbox/expired-budget", &error);
  lc_outbox_config_init(&config);
  config.ns = "outbox-expired-budget";
  config.owner = "outbox-expired-budget";
  config.claim_ttl_seconds = outbox_claim_ttl_seconds();
  config.max_attempts = 2;
  outbox = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &outbox, &error),
                   LC_OK);

  first = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &first, &error),
      LC_OK);
  assert_non_null(first);
  assert_int_equal(first->attempt, 1);
  /* Closing an unfinished job must wake the local dispatcher at lease expiry;
   * it is not dependent on a restart or an opt-in recovery poll. */
  lc_outbox_job_close(first);

  second = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &second, &error),
      LC_OK);
  assert_non_null(second);
  assert_int_equal(second->attempt, 2);
  lc_outbox_job_close(second);

  /* A second abandoned claim consumes the final durable attempt. Expiry must
   * dead-letter it rather than reset the counter and hand out attempt three. */
  unexpected = (lc_outbox_job *)1;
  assert_int_equal(lc_outbox_next(outbox, outbox_claim_next_timeout_ms(),
                                  &unexpected, &error),
                   LC_OK);
  assert_null(unexpected);

  lc_acquire_req_init(&acquire);
  acquire.ns = config.ns;
  acquire.key = "__lockdc_io/v1/outbox/expired-budget";
  acquire.owner = "outbox-expired-budget-inspect";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  sink = NULL;
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length,
                                    "\"dispatch_state\":\"dead_letter\""));
  assert_true(outbox_bytes_contains(bytes, length, "\"attempt_count\":2"));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_dead_letters_persisted_exhausted_attempts(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "persisted-budget-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox_with_attempt_count(
      client, "outbox-persisted-budget",
      "__lockdc_io/v1/outbox/persisted-budget", "2", &error);
  lc_outbox_config_init(&config);
  config.ns = "outbox-persisted-budget";
  config.owner = "outbox-persisted-budget";
  config.max_attempts = 2;
  outbox = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &outbox, &error),
                   LC_OK);

  /* This simulates reopening a durable namespace with a lower attempt policy.
   * The record must become terminal without a third delivery. */
  job = (lc_outbox_job *)1;
  assert_int_equal(lc_outbox_next(outbox, 1000L, &job, &error), LC_OK);
  assert_null(job);

  lc_acquire_req_init(&acquire);
  acquire.ns = config.ns;
  acquire.key = "__lockdc_io/v1/outbox/persisted-budget";
  acquire.owner = "outbox-persisted-budget-inspect";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  sink = NULL;
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length,
                                    "\"dispatch_state\":\"dead_letter\""));
  assert_true(outbox_bytes_contains(bytes, length, "\"attempt_count\":2"));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_rejects_malformed_durable_outbox(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "malformed-outbox-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_malformed_recovery_outbox(client, "outbox-malformed-outbox",
                                 "__lockdc_io/v1/outbox/malformed-outbox",
                                 &error);
  lc_outbox_config_init(&config);
  config.ns = "outbox-malformed-outbox";
  config.owner = "outbox-malformed-outbox";
  outbox = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &outbox, &error),
                   LC_OK);

  job = (lc_outbox_job *)1;
  assert_int_equal(lc_outbox_next(outbox, 1000L, &job, &error), LC_OK);
  assert_null(job);

  lc_acquire_req_init(&acquire);
  acquire.ns = config.ns;
  acquire.key = "__lockdc_io/v1/outbox/malformed-outbox";
  acquire.owner = "outbox-malformed-outbox-inspect";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  sink = NULL;
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(outbox_bytes_contains(bytes, length, "\"attempt_count\":0"));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_outbox_rejects_oversized_outbox_envelope(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  char *destination;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "envelope-limit-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-envelope-limit";
  outbox_config.owner = "outbox-envelope-limit-test";
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  destination = (char *)malloc((size_t)LC_OUTBOX_MAX_ENVELOPE_BYTES + 1U);
  assert_non_null(destination);
  memset(destination, 'd', (size_t)LC_OUTBOX_MAX_ENVELOPE_BYTES);
  destination[LC_OUTBOX_MAX_ENVELOPE_BYTES] = '\0';
  lc_outbox_entry_init(&entry);
  entry.operation_id = "envelope-limit-operation";
  entry.effect_id = "envelope-limit-effect";
  entry.effect_key = "envelope-limit-effect-key";
  entry.payload_digest = "sha256:envelope-limit-payload";
  entry.kind = "test";
  entry.destination = destination;
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = (lc_outbox_transaction *)1;
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  job = (lc_outbox_job *)1;
  assert_int_equal(lc_outbox_next(outbox, 0L, &job, &error), LC_OK);
  assert_null(job);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  free(destination);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_outbox_rejects_oversized_receipt_metadata(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  char *oversized;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "receipt-limit-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-receipt-limit";
  outbox_config.owner = "outbox-receipt-limit-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  oversized = (char *)malloc((size_t)LC_OUTBOX_MAX_RECEIPT_BYTES + 1U);
  assert_non_null(oversized);
  memset(oversized, 'r', (size_t)LC_OUTBOX_MAX_RECEIPT_BYTES);
  oversized[LC_OUTBOX_MAX_RECEIPT_BYTES] = '\0';

  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "receipt-limit-consumer";
  inbox.source_kind = "test";
  inbox.source_id = "receipt-limit-source";
  inbox.message_id = oversized;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  assert_false(inbox_result.accepted);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_command_request_init(&command);
  command.identity.scope = "receipt-limit-scope";
  command.identity.command_type = "receipt-limit-command";
  command.identity.idempotency_key = "receipt-limit-key";
  command.request_digest = oversized;
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.command_id);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  inbox.message_id = "receipt-limit-message";
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_accept_command(transaction, &command,
                                                        &receipt, &error),
                   LC_ERR_INVALID);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  free(oversized);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_outbox_remote_default_receipt_limit(void **state) {
  lc_client_config config;
  size_t default_limit;

  (void)state;
  lc_client_config_init(&config);
  assert_int_equal(config.http_json_response_limit_bytes, 0U);
  default_limit = ((size_t)LC_OUTBOX_MAX_RECEIPT_BYTES - 1024U) / 6U;
  assert_int_equal(lc_outbox_test_receipt_input_limit(
                       0, config.http_json_response_limit_bytes),
                   default_limit);
  assert_int_equal(lc_outbox_test_receipt_input_limit(0, 8192U),
                   (8192U - 1024U) / 6U);
}

static void
test_outbox_rejects_oversized_terminal_receipt_metadata(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_transaction *transaction;
  lc_client *client;
  lc_outbox *outbox;
  char *oversized;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "terminal-receipt-limit-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-terminal-receipt-limit";
  outbox_config.owner = "outbox-terminal-receipt-limit-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  oversized = (char *)malloc((size_t)LC_OUTBOX_MAX_RECEIPT_BYTES + 1U);
  assert_non_null(oversized);
  memset(oversized, 'r', (size_t)LC_OUTBOX_MAX_RECEIPT_BYTES);
  oversized[LC_OUTBOX_MAX_RECEIPT_BYTES] = '\0';

  lc_command_request_init(&command);
  command.identity.scope = "terminal-receipt-limit-scope";
  command.identity.command_type = "terminal-receipt-limit-command";
  command.identity.idempotency_key = "terminal-receipt-limit-complete";
  command.request_digest = "terminal-receipt-limit-digest";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  lc_command_result_init(&result);
  result.result_code = oversized;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  result.result_code = "ok";
  result.result_reference = oversized;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  result.result_reference = NULL;
  result.content_type = oversized;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  result.content_type = NULL;
  assert_int_equal(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                 &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  lc_command_receipt_cleanup(&receipt);

  command.identity.idempotency_key = "terminal-receipt-limit-failed";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  lc_command_result_init(&result);
  result.failure_code = oversized;
  assert_int_equal(
      lc_outbox_transaction_fail_command(transaction, &result, &error),
      LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  result.failure_code = "rejected";
  result.failure_message = "request was rejected";
  assert_int_equal(
      lc_outbox_transaction_fail_command(transaction, &result, &error), LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                 &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_FAILED);
  assert_string_equal(receipt.failure_code, "rejected");
  lc_command_receipt_cleanup(&receipt);
  free(oversized);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_participant_attach_refreshes_version(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_participant_request participant_request;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_participant *participant;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_source *payload;
  lc_version version_before_attach;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "participant-attach-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "participant-attach";
  outbox_config.owner = "participant-attach-test";
  outbox = NULL;
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "participant-attach-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "participant-attach-source";
  inbox.message_id = "participant-attach-message";
  inbox.payload_digest = "participant-attach-digest";
  inbox.operation_id = "participant-attach-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  transaction = NULL;
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "participant-attach-domain";
  participant_request.acquire.key = "participant-attach-key";
  participant_request.acquire.owner = "participant-attach-test";
  participant_request.acquire.ttl_seconds = 30L;
  participant = NULL;
  assert_int_equal(lc_outbox_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  version_before_attach = participant->version;
  lc_attach_req_init(&attach);
  attach.name = "receipt.txt";
  attach.content_type = "text/plain";
  payload = NULL;
  assert_int_equal(lc_source_from_memory("receipt", 7U, &payload, &error),
                   LC_OK);
  memset(&attach_result, 0, sizeof(attach_result));
  assert_int_equal(participant->attach(participant, &attach, payload,
                                       &attach_result, &error),
                   LC_OK);
  assert_true(attach_result.version > version_before_attach);
  assert_true(participant->version == attach_result.version);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload);
  lc_outbox_participant_close(participant);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_rejects_out_of_range_durable_attempt_counts(void **state) {
  static const char *const keys[] = {
      "__lockdc_io/v1/outbox/attempt-int64-max",
      "__lockdc_io/v1/outbox/attempt-negative",
  };
  static const char *const attempt_counts[] = {
      "9223372036854775807",
      "-1",
  };
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_job *job;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  size_t index;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "attempt-range-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < sizeof(keys) / sizeof(keys[0]); ++index) {
    seed_recovery_outbox_with_attempt_count(client, "outbox-attempt-range",
                                            keys[index], attempt_counts[index],
                                            &error);
  }
  lc_outbox_config_init(&config);
  config.ns = "outbox-attempt-range";
  config.owner = "outbox-attempt-range";
  outbox = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &outbox, &error),
                   LC_OK);

  job = (lc_outbox_job *)1;
  assert_int_equal(lc_outbox_next(outbox, 1000L, &job, &error), LC_OK);
  assert_null(job);

  for (index = 0U; index < sizeof(keys) / sizeof(keys[0]); ++index) {
    lc_acquire_req_init(&acquire);
    acquire.ns = config.ns;
    acquire.key = keys[index];
    acquire.owner = "outbox-attempt-range-inspect";
    acquire.ttl_seconds = 30L;
    lease = NULL;
    assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
    sink = NULL;
    memset(&get_result, 0, sizeof(get_result));
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error),
                     LC_OK);
    bytes = NULL;
    length = 0U;
    assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error),
                     LC_OK);
    assert_true(
        outbox_bytes_contains(bytes, length, "\"dispatch_state\":\"pending\""));
    assert_true(outbox_bytes_contains(bytes, length, attempt_counts[index]));
    lc_get_res_cleanup(&get_result);
    lc_sink_close(sink);
    assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  }
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_outbox_rejects_out_of_range_durable_replay_counts(void **state) {
  static const char *const keys[] = {
      "__lockdc_io/v1/outbox/replay-int64-max",
      "__lockdc_io/v1/outbox/replay-negative",
  };
  static const char *const replay_counts[] = {
      "9223372036854775807",
      "-1",
  };
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  size_t index;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "replay-range-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < sizeof(keys) / sizeof(keys[0]); ++index) {
    seed_dead_letter_outbox_with_replay_count(client, "outbox-replay-range",
                                              keys[index], replay_counts[index],
                                              &error);
  }
  lc_outbox_config_init(&config);
  config.ns = "outbox-replay-range";
  config.owner = "outbox-replay-range";
  outbox = NULL;
  assert_int_equal(lc_client_new_outbox(client, &config, &outbox, &error),
                   LC_OK);

  for (index = 0U; index < sizeof(keys) / sizeof(keys[0]); ++index) {
    assert_int_equal(lc_outbox_replay_dead_letter(outbox, keys[index], &error),
                     LC_ERR_INVALID);
    lc_error_cleanup(&error);
    lc_error_init(&error);

    lc_acquire_req_init(&acquire);
    acquire.ns = config.ns;
    acquire.key = keys[index];
    acquire.owner = "outbox-replay-range-inspect";
    acquire.ttl_seconds = 30L;
    lease = NULL;
    assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
    sink = NULL;
    memset(&get_result, 0, sizeof(get_result));
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error),
                     LC_OK);
    bytes = NULL;
    length = 0U;
    assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error),
                     LC_OK);
    assert_true(outbox_bytes_contains(bytes, length,
                                      "\"dispatch_state\":\"dead_letter\""));
    assert_true(outbox_bytes_contains(bytes, length, replay_counts[index]));
    lc_get_res_cleanup(&get_result);
    lc_sink_close(sink);
    assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  }
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_validates_durable_input_contracts(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  char *oversized_diagnostic;
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_retry retry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *second_payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "input-contracts-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  second_payload = NULL;
  oversized_diagnostic = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-input-contracts";
  outbox_config.owner = "outbox-input-contracts-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "input-contract-operation";
  entry.effect_id = "input-contract-effect";
  entry.effect_key = "input-contract-effect-key";
  entry.kind = "test";
  entry.destination = "input-contract://destination";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);

  entry.headers_json = "{";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  entry.headers_json = "[]";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  entry.headers_json = "{\"x-request-id\":\"contract\"}";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.payload_digest = "sha256:input-contract-payload";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  /* Input validation precedes duplicate lookup: invalid metadata must not be
   * silently accepted just because a valid entry with the same identity exists.
   */
  entry.headers_json = "{";
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.headers_json = "{\"x-request-id\":\"contract\"}";

  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);

  oversized_diagnostic = (char *)malloc(LC_OUTBOX_MAX_DIAGNOSTIC_BYTES + 2U);
  assert_non_null(oversized_diagnostic);
  memset(oversized_diagnostic, 'x', LC_OUTBOX_MAX_DIAGNOSTIC_BYTES + 1U);
  oversized_diagnostic[LC_OUTBOX_MAX_DIAGNOSTIC_BYTES + 1U] = '\0';
  lc_outbox_retry_init(&retry);
  retry.diagnostic = oversized_diagnostic;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_dead_letter(job, oversized_diagnostic, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  free(oversized_diagnostic);
  oversized_diagnostic = NULL;

  /* A rejected append cannot roll back already-enrolled work. */
  entry.effect_id = "input-contract-preserve";
  entry.effect_key = "input-contract-preserve-key";
  entry.headers_json = "{\"x-request-id\":\"preserve\"}";
  assert_int_equal(
      lc_source_from_memory("preserve", 8U, &second_payload, &error), LC_OK);
  assert_int_equal(lc_outbox_append(outbox, &entry, second_payload,
                                    &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  entry.effect_id = "input-contract-rejected";
  entry.effect_key = "input-contract-rejected-key";
  entry.headers_json = "{";
  assert_int_equal(lc_outbox_transaction_append(
                       transaction, &entry, second_payload, &receipt, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_outbox_next(outbox, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  job = NULL;
  lc_source_close(second_payload);
  second_payload = NULL;

  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "a\nb";
  inbox.source_kind = "c";
  inbox.source_id = "d";
  inbox.message_id = "e";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  inbox.consumer_id = "a";
  inbox.source_kind = "b";
  inbox.source_id = "c\nd";
  inbox.message_id = "e";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_outbox_accept_inbox(outbox, &inbox, &transaction,
                                          &inbox_result, &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_false(inbox_result.duplicate);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_close_serializes_ready_job_detach(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_stats stats;
  outbox_shutdown_race race;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  pthread_t close_thread, next_thread;
  size_t attempt;
  int close_finished_before_next_detach;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "shutdown-race-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "outbox-shutdown-race",
                       "__lockdc_io/v1/outbox/shutdown-race", &error);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-shutdown-race";
  outbox_config.owner = "outbox-shutdown-race-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  memset(&stats, 0, sizeof(stats));
  for (attempt = 0U; attempt < 500U; ++attempt) {
    assert_int_equal(lc_outbox_get_stats(outbox, &stats, &error), LC_OK);
    if (stats.pending_candidates == 1U)
      break;
    lc_outbox_stats_cleanup(&stats);
    memset(&stats, 0, sizeof(stats));
    {
      struct timespec delay;
      delay.tv_sec = 0;
      delay.tv_nsec = 10000000L;
      (void)nanosleep(&delay, NULL);
    }
  }
  assert_int_equal(stats.pending_candidates, 1U);
  lc_outbox_stats_cleanup(&stats);

  memset(&race, 0, sizeof(race));
  race.outbox = outbox;
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  lc_outbox_test_after_close_requested_hook =
      outbox_shutdown_race_after_close_requested;
  lc_outbox_test_after_close_requested_context = &race;
  lc_outbox_test_before_ready_job_detach_hook =
      outbox_shutdown_race_before_ready_detach;
  lc_outbox_test_before_ready_job_detach_context = &race;
  lc_outbox_test_before_ready_job_teardown_hook =
      outbox_shutdown_race_before_teardown;
  lc_outbox_test_before_ready_job_teardown_context = &race;
  assert_int_equal(pthread_create(&close_thread, NULL,
                                  outbox_shutdown_race_close_thread, &race),
                   0);
  assert_true(outbox_shutdown_race_wait(&race, &race.close_requested));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_close = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(outbox_shutdown_race_wait(&race, &race.teardown_entered));

  /* close() has joined the dispatcher before it reaches this hook. Race the
   * remaining ready-job teardown against next() now, rather than blocking a
   * next() caller while the dispatcher still needs the same mutex to notice
   * close_requested. */
  assert_int_equal(pthread_create(&next_thread, NULL,
                                  outbox_shutdown_race_next_thread, &race),
                   0);
  assert_true(outbox_shutdown_race_wait(&race, &race.ready_detach_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_teardown = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  {
    struct timespec delay;
    delay.tv_sec = 0;
    delay.tv_nsec = 50000000L;
    (void)nanosleep(&delay, NULL);
  }
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  close_finished_before_next_detach = race.close_finished;
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);

  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_ready_detach = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(outbox_shutdown_race_wait(&race, &race.next_finished));
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  assert_int_equal(race.next_rc, LC_OK);
  assert_non_null(race.job);
  lc_outbox_job_close(race.job);
  assert_int_equal(pthread_join(close_thread, NULL), 0);
  assert_false(close_finished_before_next_detach);
  outbox_reset_allocation_failures();
  pthread_cond_destroy(&race.condition);
  pthread_mutex_destroy(&race.mutex);

  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_dispatcher_stop_retains_blocked_next(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  outbox_blocked_next_close_race race;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  pthread_t close_thread, next_thread;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "blocked-next-close-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  memset(&race, 0, sizeof(race));
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  outbox_reset_allocation_failures();
  lc_outbox_test_before_next_wait_hook = outbox_blocked_next_close_before_wait;
  lc_outbox_test_before_next_wait_context = &race;
  lc_outbox_test_after_close_requested_hook =
      outbox_blocked_next_close_after_close_requested;
  lc_outbox_test_after_close_requested_context = &race;
  lc_outbox_test_before_next_release_hook =
      outbox_blocked_next_close_before_release;
  lc_outbox_test_before_next_release_context = &race;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "blocked-next-close";
  assert_int_equal(client->new_outbox(client, &outbox_config, &outbox, &error),
                   LC_OK);
  assert_int_equal(outbox->get_or_start_dispatcher(outbox, &dispatcher, &error),
                   LC_OK);
  race.dispatcher = dispatcher;
  assert_int_equal(pthread_create(&next_thread, NULL,
                                  outbox_blocked_next_next_thread, &race),
                   0);
  assert_true(
      outbox_blocked_next_close_race_wait(&race, &race.next_wait_entered));
  assert_int_equal(pthread_create(&close_thread, NULL,
                                  outbox_blocked_next_close_thread, &race),
                   0);
  assert_true(
      outbox_blocked_next_close_race_wait(&race, &race.close_requested));
  assert_true(
      outbox_blocked_next_close_race_wait(&race, &race.next_release_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_close = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(outbox_blocked_next_close_race_wait(&race, &race.close_finished));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_next_release = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(outbox_blocked_next_close_race_wait(&race, &race.next_finished));
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  assert_int_equal(pthread_join(close_thread, NULL), 0);
  assert_int_equal(race.next_rc, LC_ERR_INVALID);
  assert_null(race.job);
  outbox_reset_allocation_failures();
  assert_int_equal(pthread_cond_destroy(&race.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&race.mutex), 0);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_command_receipt_rejects_malformed_terminal_records(void **state) {
  static const char *const malformed_records[] = {
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"completed\","
      "\"completed_at_unix\":1,\"has_result_body\":0}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"failed\","
      "\"failed_at_unix\":1,\"has_result_body\":0}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"completed\",\"result_code\":\"ok\","
      "\"completed_at_unix\":1,\"has_result_body\":2}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"completed\",\"result_code\":\"ok\","
      "\"failure_code\":\"contradiction\",\"failed_at_unix\":1,"
      "\"completed_at_unix\":1,\"has_result_body\":0}"};
  static const char *const identity_conflict_records[] = {
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"other-tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"pending\","
      "\"has_result_body\":0}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.other\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"pending\","
      "\"has_result_body\":0}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"%s\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"other-idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"pending\","
      "\"has_result_body\":0}",
      "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"cmd_other\","
      "\"scope\":\"tenant\",\"command_type\":\"orders.test\","
      "\"idempotency_key\":\"idempotency\",\"request_digest\":\"digest\","
      "\"accepted_at_unix\":1,\"state\":\"pending\","
      "\"has_result_body\":0}"};
  char root[256], template_path[256], endpoint[320], command_id[64], key[128],
      record[1024];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_client *client;
  lc_outbox *outbox;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *source;
  lc_error error;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "command-malformed-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  lease = NULL;
  source = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "command-malformed";
  outbox_config.owner = "command-malformed-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "tenant";
  command.identity.command_type = "orders.test";
  command.identity.idempotency_key = "idempotency";
  command.request_digest = "digest";
  assert_int_equal(lc_outbox_accept_command(outbox, &command, &transaction,
                                            &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_true(
      snprintf(command_id, sizeof(command_id), "%s", receipt.command_id) > 0);
  assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/command/%s",
                       command_id + 4U) > 0);

  for (index = 0U;
       index < sizeof(malformed_records) / sizeof(malformed_records[0]);
       ++index) {
    assert_true(snprintf(record, sizeof(record), malformed_records[index],
                         command_id) > 0);
    lc_acquire_req_init(&acquire);
    acquire.ns = outbox_config.ns;
    acquire.key = key;
    acquire.owner = "command-malformed-seed";
    acquire.ttl_seconds = 30L;
    assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
    assert_int_equal(
        lc_source_from_memory(record, strlen(record), &source, &error), LC_OK);
    assert_int_equal(lc_lease_update(lease, source, NULL, &error), LC_OK);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
    lease = NULL;

    lc_command_receipt_cleanup(&receipt);
    lc_command_receipt_init(&receipt);
    assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                   &receipt, &error),
                     LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    transaction = (lc_outbox_transaction *)1;
    assert_int_equal(lc_outbox_resume_command(outbox, &command.identity,
                                              &transaction, &receipt, &error),
                     LC_ERR_PROTOCOL);
    assert_null(transaction);
    lc_error_cleanup(&error);
    lc_error_init(&error);
  }

  for (index = 0U; index < sizeof(identity_conflict_records) /
                               sizeof(identity_conflict_records[0]);
       ++index) {
    assert_true(snprintf(record, sizeof(record),
                         identity_conflict_records[index], command_id) > 0);
    lc_acquire_req_init(&acquire);
    acquire.ns = outbox_config.ns;
    acquire.key = key;
    acquire.owner = "command-identity-conflict-seed";
    acquire.ttl_seconds = 30L;
    assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
    assert_int_equal(
        lc_source_from_memory(record, strlen(record), &source, &error), LC_OK);
    assert_int_equal(lc_lease_update(lease, source, NULL, &error), LC_OK);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
    lease = NULL;

    lc_command_receipt_cleanup(&receipt);
    lc_command_receipt_init(&receipt);
    assert_int_equal(lc_outbox_get_command_receipt(outbox, &command.identity,
                                                   &receipt, &error),
                     LC_ERR_SERVER);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    transaction = (lc_outbox_transaction *)1;
    assert_int_equal(lc_outbox_resume_command(outbox, &command.identity,
                                              &transaction, &receipt, &error),
                     LC_ERR_SERVER);
    assert_null(transaction);
    lc_error_cleanup(&error);
    lc_error_init(&error);
  }

  lc_command_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void test_pouch_outbox_completion_evidence_is_bounded(void **state) {
  char root[256], template_path[256], endpoint[320];
  char oversized[LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES + 2U];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_completion completion;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "completion-evidence-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  memset(oversized, 'x', sizeof(oversized) - 1U);
  oversized[sizeof(oversized) - 1U] = '\0';
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "completion-evidence";
  outbox_config.owner = "completion-evidence-test";
  assert_int_equal(
      lc_client_new_outbox(client, &outbox_config, &outbox, &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "completion-evidence-operation";
  entry.effect_id = "completion-evidence-effect";
  entry.effect_key = "completion-evidence-key";
  entry.payload_digest = "sha256:completion-evidence";
  entry.kind = "test";
  entry.destination = "test://completion-evidence";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, &error),
      LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &error), LC_OK);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(
      lc_outbox_next(outbox, outbox_claim_next_timeout_ms(), &job, &error),
      LC_OK);
  assert_non_null(job);
  lc_outbox_completion_init(&completion);
  completion.delivery_reference = oversized;
  assert_int_equal(lc_outbox_job_complete(job, &completion, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  completion.delivery_reference = NULL;
  completion.response_digest = oversized;
  assert_int_equal(lc_outbox_job_complete(job, &completion, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  completion.response_digest = NULL;
  assert_int_equal(lc_outbox_job_complete(job, &completion, &error), LC_OK);
  job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

typedef struct terminal_retention_fixture {
  char root[PATH_MAX];
  char endpoint[PATH_MAX];
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_error error;
} terminal_retention_fixture;

static void terminal_retention_open(terminal_retention_fixture *f) {
  char template_path[PATH_MAX];
  const char *endpoints[1];
  lc_client_config config;
  memset(f, 0, sizeof(*f));
  lc_error_init(&f->error);
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "terminal-retention-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(f->endpoint, sizeof(f->endpoint),
                       "pouch://%s?query_indexing=false&single_writer=false",
                       f->root) > 0);
  endpoints[0] = f->endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&config, &f->client, &f->error), LC_OK);
  lc_outbox_config_init(&f->config);
  f->config.ns = "terminal-retention";
  assert_int_equal(
      outbox_public_new(f->client, &f->config, &f->outbox, &f->error), LC_OK);
}

static void terminal_retention_reopen(terminal_retention_fixture *f) {
  assert_null(((lc_client_handle *)f->client)->outbox_dispatchers);
  f->outbox->close(f->outbox);
  f->outbox = NULL;
  assert_int_equal(
      outbox_public_new(f->client, &f->config, &f->outbox, &f->error), LC_OK);
}

static void terminal_retention_close(terminal_retention_fixture *f) {
  assert_null(((lc_client_handle *)f->client)->outbox_dispatchers);
  lc_outbox_test_before_terminal_remove_hook = NULL;
  lc_outbox_test_before_terminal_remove_context = NULL;
  lc_outbox_close(f->outbox);
  lc_client_close(f->client);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->root, OUTBOX_TMP_PREFIX);
}

static void terminal_retention_seed(terminal_retention_fixture *f, int kind,
                                    const char *key, const char *id,
                                    lc_i64 due) {
  char body[1024];
  int length;
  lc_acquire_req acquire;
  lc_attach_req attach;
  lc_attach_res attached;
  lc_lease *lease;
  lc_source *source;
  if (kind == LC_POUCH_RETENTION_COMPLETED) {
    length = snprintf(
        body, sizeof(body),
        "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"op\","
        "\"effect_id\":\"effect\",\"effect_key\":\"key\",\"payload_digest\":"
        "\"digest\","
        "\"message_id\":\"msg\",\"kind\":\"test\",\"destination\":\"test://"
        "target\","
        "\"content_type\":\"text/plain\",\"dispatch_state\":\"completed\","
        "\"completed_at_unix\":100,\"completed_retention_id\":\"%s\"}",
        id);
  } else if (kind == LC_POUCH_RETENTION_INBOX) {
    length = snprintf(
        body, sizeof(body),
        "{\"record_type\":\"lockdc.inbox.v1\",\"consumer_id\":\"consumer\","
        "\"source_kind\":\"queue\",\"source_id\":\"source\",\"message_id\":"
        "\"msg\","
        "\"processing_state\":\"accepted\",\"accepted_at_unix\":100,"
        "\"retention_id\":\"%s\"}",
        id);
  } else {
    length = snprintf(
        body, sizeof(body),
        "{\"record_type\":\"lockdc.command.v1\",\"command_id\":\"cmd_test\","
        "\"scope\":\"tenant\",\"command_type\":\"test\",\"idempotency_key\":"
        "\"key\","
        "\"request_digest\":\"digest\",\"accepted_at_unix\":50,\"state\":"
        "\"completed\","
        "\"result_code\":\"ok\",\"completed_at_unix\":100,\"retention_id\":\"%"
        "s\"}",
        id);
  }
  assert_true(length > 0 && (size_t)length < sizeof(body));
  lc_acquire_req_init(&acquire);
  acquire.ns = f->config.ns;
  acquire.owner = "terminal-retention-test";
  acquire.ttl_seconds = 30L;
  acquire.key = key;
  lease = NULL;
  assert_int_equal(lc_acquire(f->client, &acquire, &lease, &f->error), LC_OK);
  source = NULL;
  assert_int_equal(
      lc_source_from_memory(body, (size_t)length, &source, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(lease, source, NULL, &f->error), LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_attach_req_init(&attach);
  attach.name = "retained-data";
  memset(&attached, 0, sizeof(attached));
  assert_int_equal(lc_source_from_memory("data", 4U, &source, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_lease_attach(lease, &attach, source, &attached, &f->error), LC_OK);
  lc_source_close(source);
  lc_attach_res_cleanup(&attached);
  assert_int_equal(lc_lease_release(lease, NULL, &f->error), LC_OK);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       ((lc_client_handle *)f->client)->pouch, f->config.ns,
                       kind, id, key, due, 0U, &f->error),
                   LC_OK);
}

static void terminal_retention_assert_attachments(terminal_retention_fixture *f,
                                                  const char *key,
                                                  size_t count) {
  lc_acquire_req acquire;
  lc_attachment_list list;
  lc_lease *lease;
  lc_acquire_req_init(&acquire);
  acquire.ns = f->config.ns;
  acquire.owner = "terminal-retention-test";
  acquire.ttl_seconds = 30L;
  acquire.key = key;
  lease = NULL;
  memset(&list, 0, sizeof(list));
  assert_int_equal(lc_acquire(f->client, &acquire, &lease, &f->error), LC_OK);
  assert_int_equal(lc_lease_list_attachments(lease, &list, &f->error), LC_OK);
  assert_int_equal(list.count, count);
  lc_attachment_list_cleanup(&list);
  assert_int_equal(lc_lease_release(lease, NULL, &f->error), LC_OK);
}

static void test_pouch_terminal_retention_count_optout_and_time(void **state) {
  terminal_retention_fixture f;
  char ids[3][LC_XID_STRING_SIZE];
  char keys[3][128];
  size_t reclaimed;
  lc_i64 next_due;
  int kind;
  unsigned int index;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    for (index = 0U; index < 3U; ++index) {
      assert_int_equal(lc_xid_new(ids[index], &f.error), LC_OK);
      assert_true(snprintf(keys[index], sizeof(keys[index]),
                           "__lockdc_io/v1/%s/count-%d-%u",
                           kind == LC_POUCH_RETENTION_COMPLETED ? "outbox"
                           : kind == LC_POUCH_RETENTION_INBOX   ? "inbox"
                                                                : "command",
                           kind, index) > 0);
      terminal_retention_seed(&f, kind, keys[index], ids[index], 5000);
    }
    /* Zero count uses no capacity cap, including a backlog larger than one. */
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 0U);
    assert_int_equal(next_due, 5000);
    if (kind == LC_POUCH_RETENTION_COMPLETED)
      f.config.completed_max_count = 1U;
    else if (kind == LC_POUCH_RETENTION_INBOX)
      f.config.inbox_max_count = 1U;
    else
      f.config.command_max_count = 1U;
    terminal_retention_reopen(&f);
    for (index = 0U; index < 2U; ++index) {
      assert_int_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                       &reclaimed, &next_due,
                                                       &f.error),
                       LC_OK);
      assert_int_equal(reclaimed, 1U);
    }
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 0U);
    /* The final receipt remains protected until the time boundary. */
    if (kind == LC_POUCH_RETENTION_COMPLETED)
      f.config.completed_retention_seconds = -1L;
    else if (kind == LC_POUCH_RETENTION_INBOX)
      f.config.inbox_retention_seconds = -1L;
    else
      f.config.command_retention_seconds = -1L;
    terminal_retention_reopen(&f);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 5000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 0U);
    if (kind == LC_POUCH_RETENTION_COMPLETED)
      f.config.completed_retention_seconds = 0L;
    else if (kind == LC_POUCH_RETENTION_INBOX)
      f.config.inbox_retention_seconds = 0L;
    else
      f.config.command_retention_seconds = 0L;
    terminal_retention_reopen(&f);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 5000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 1U);
    for (index = 0U; index < 3U; ++index) {
      outbox_assert_state_presence(f.client, f.config.ns, keys[index], 0,
                                   &f.error);
      terminal_retention_assert_attachments(&f, keys[index], 0U);
    }
  }
  terminal_retention_close(&f);
}

static void
test_pouch_terminal_retention_generation_busy_and_failure(void **state) {
  terminal_retention_fixture f;
  char id[LC_XID_STRING_SIZE];
  char stale_id[LC_XID_STRING_SIZE];
  char key[128];
  lc_acquire_req acquire;
  lc_lease *held;
  size_t reclaimed;
  lc_i64 next_due;
  int kind;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    assert_int_equal(lc_xid_new(id, &f.error), LC_OK);
    assert_int_equal(lc_xid_new(stale_id, &f.error), LC_OK);
    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/%s/generation-%d",
                         kind == LC_POUCH_RETENTION_COMPLETED ? "outbox"
                         : kind == LC_POUCH_RETENTION_INBOX   ? "inbox"
                                                              : "command",
                         kind) > 0);
    terminal_retention_seed(&f, kind, key, id, 5000);
    assert_int_equal(lc_pouch_outbox_retention_marker_write(
                         ((lc_client_handle *)f.client)->pouch, f.config.ns,
                         kind, stale_id, key, 1, 0U, &f.error),
                     LC_OK);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 1U); /* Only stale control data was removed. */
    outbox_assert_state_presence(f.client, f.config.ns, key, 1, &f.error);
    terminal_retention_assert_attachments(&f, key, 1U);
    lc_acquire_req_init(&acquire);
    acquire.ns = f.config.ns;
    acquire.owner = "terminal-retention-held";
    acquire.ttl_seconds = 30L;
    acquire.key = key;
    held = NULL;
    assert_int_equal(lc_acquire(f.client, &acquire, &held, &f.error), LC_OK);
    assert_int_not_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 5000,
                                                         &reclaimed, &next_due,
                                                         &f.error),
                         LC_OK);
    assert_int_equal(reclaimed, 0U);
    lc_error_cleanup(&f.error);
    lc_error_init(&f.error);
    assert_int_equal(lc_lease_release(held, NULL, &f.error), LC_OK);
    lc_outbox_test_before_terminal_remove_hook = outbox_fail_allocation;
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 5000, &reclaimed, &next_due, &f.error),
                     LC_ERR_NOMEM);
    assert_int_equal(reclaimed, 0U);
    lc_outbox_test_before_terminal_remove_hook = NULL;
    lc_error_cleanup(&f.error);
    lc_error_init(&f.error);
    outbox_assert_state_presence(f.client, f.config.ns, key, 1, &f.error);
    terminal_retention_assert_attachments(&f, key, 1U);
    /* A fresh receiver resumes the durable marker after an interrupted cleanup.
     */
    terminal_retention_reopen(&f);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 5000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 1U);
    outbox_assert_state_presence(f.client, f.config.ns, key, 0, &f.error);
    terminal_retention_assert_attachments(&f, key, 0U);
  }
  terminal_retention_close(&f);
}

static void test_pouch_terminal_retention_public_transitions(void **state) {
  terminal_retention_fixture f;
  lc_command_request command;
  lc_command_result result;
  lc_command_receipt receipt;
  lc_inbox_message inbox;
  lc_inbox_accept_result accepted;
  lc_outbox_transaction *transaction;
  lc_outbox_entry entry;
  lc_outbox_receipt effect;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_source *source;
  lc_pouch_outbox_retention_marker marker;
  lc_i64 next_due;
  lc_i64 now;
  size_t reclaimed;
  char pending_key[128];
  int kind;
  int index;
  (void)state;
  terminal_retention_open(&f);
  lc_command_receipt_init(&receipt);
  lc_outbox_receipt_init(&effect);
  lc_command_request_init(&command);
  command.identity.scope = "tenant";
  command.identity.command_type = "test";
  command.request_digest = "digest";
  lc_command_result_init(&result);
  now = (lc_i64)time(NULL);
  for (index = 0; index < 3; ++index) {
    command.identity.idempotency_key = index == 0   ? "completed"
                                       : index == 1 ? "failed"
                                                    : "pending";
    transaction = NULL;
    assert_int_equal(lc_outbox_accept_command(f.outbox, &command, &transaction,
                                              &receipt, &f.error),
                     LC_OK);
    lc_command_result_init(&result);
    source = NULL;
    if (index == 0) {
      result.result_code = "ok";
      result.content_type = "text/plain";
      assert_int_equal(lc_source_from_memory("result", 6U, &source, &f.error),
                       LC_OK);
      result.body = source;
      assert_int_equal(
          transaction->complete_command(transaction, &result, &f.error), LC_OK);
    } else if (index == 1) {
      result.failure_code = "rejected";
      assert_int_equal(
          transaction->fail_command(transaction, &result, &f.error), LC_OK);
    } else {
      assert_true(snprintf(pending_key, sizeof(pending_key),
                           "__lockdc_io/v1/command/%s",
                           receipt.command_id + 4U) > 0);
    }
    assert_int_equal(lc_outbox_transaction_commit(transaction, &f.error),
                     LC_OK);
    lc_outbox_transaction_close(transaction);
    lc_source_close(source);
    lc_command_receipt_cleanup(&receipt);
  }
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "consumer";
  inbox.source_kind = "queue";
  inbox.source_id = "source";
  inbox.message_id = "message";
  transaction = NULL;
  assert_int_equal(lc_outbox_accept_inbox(f.outbox, &inbox, &transaction,
                                          &accepted, &f.error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &f.error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "op";
  entry.effect_id = "effect";
  entry.effect_key = "key";
  entry.payload_digest = "digest";
  entry.kind = "test";
  entry.destination = "test://target";
  source = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &source, &f.error),
                   LC_OK);
  assert_int_equal(lc_outbox_append(f.outbox, &entry, source, &transaction,
                                    &effect, &f.error),
                   LC_OK);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &f.error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_source_close(source);
  dispatcher = outbox_test_dispatcher(f.outbox, &f.error);
  assert_non_null(dispatcher);
  job = NULL;
  assert_int_equal(dispatcher->next(dispatcher, 3000L, &job, &f.error), LC_OK);
  assert_non_null(job);
  assert_int_equal(job->complete(job, NULL, &f.error), LC_OK);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &f.error), LC_OK);
  dispatcher->close(dispatcher);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    memset(&marker, 0, sizeof(marker));
    assert_int_equal(lc_pouch_outbox_retention_marker_take(
                         ((lc_client_handle *)f.client)->pouch, f.config.ns,
                         kind, NULL, now + 31L * 86400L, SIZE_MAX, UINT64_MAX,
                         &marker, &next_due, &f.error),
                     LC_OK);
    assert_non_null(marker.marker_id);
    assert_true(marker.due_at_unix >= now + 30L * 86400L);
    assert_true(marker.due_at_unix <= (lc_i64)time(NULL) + 30L * 86400L);
    assert_int_equal(
        lc_outbox_test_reclaim_terminal(f.outbox, kind, marker.due_at_unix - 1,
                                        &reclaimed, &next_due, &f.error),
        LC_OK);
    assert_int_equal(reclaimed, 0U);
    assert_int_equal(
        lc_outbox_test_reclaim_terminal(f.outbox, kind, marker.due_at_unix,
                                        &reclaimed, &next_due, &f.error),
        LC_OK);
    assert_int_equal(reclaimed, 1U);
    outbox_assert_state_presence(f.client, f.config.ns, marker.outbox_key, 0,
                                 &f.error);
    terminal_retention_assert_attachments(&f, marker.outbox_key, 0U);
    lc_pouch_outbox_retention_marker_cleanup(
        ((lc_client_handle *)f.client)->pouch, &marker);
    /* Both successful and failed commands expire; pending commands do not. */
    if (kind == LC_POUCH_RETENTION_COMMAND) {
      assert_int_equal(
          lc_outbox_test_reclaim_terminal(f.outbox, kind, now + 31L * 86400L,
                                          &reclaimed, &next_due, &f.error),
          LC_OK);
      assert_int_equal(reclaimed, 1U);
    }
  }
  outbox_assert_state_presence(f.client, f.config.ns, pending_key, 1, &f.error);
  /* Expiry deliberately reopens the identity's duplicate barrier. */
  transaction = NULL;
  assert_int_equal(lc_outbox_accept_inbox(f.outbox, &inbox, &transaction,
                                          &accepted, &f.error),
                   LC_OK);
  assert_true(accepted.accepted);
  assert_false(accepted.duplicate);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &f.error),
                   LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_outbox_receipt_cleanup(&effect);
  terminal_retention_close(&f);
}

static void
test_pouch_terminal_retention_live_dispatcher_restart(void **state) {
  terminal_retention_fixture f;
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_stats stats;
  char ids[3][LC_XID_STRING_SIZE];
  const char *keys[3] = {"__lockdc_io/v1/outbox/restart",
                         "__lockdc_io/v1/inbox/restart",
                         "__lockdc_io/v1/command/restart"};
  struct timespec delay;
  unsigned int attempt;
  int kind;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    assert_int_equal(lc_xid_new(ids[kind - 1], &f.error), LC_OK);
    terminal_retention_seed(&f, kind, keys[kind - 1], ids[kind - 1], 1);
  }
  /* A completely new Pouch session discovers another session's markers.
   * Merely starting the explicit dispatcher performs maintenance; there is
   * no consumer demand, query index, or receipt read to trigger it. */
  lc_outbox_close(f.outbox);
  lc_client_close(f.client);
  f.outbox = NULL;
  f.client = NULL;
  endpoints[0] = f.endpoint;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &f.client, &f.error), LC_OK);
  f.config.dead_letter_retention_seconds = -1L;
  assert_int_equal(outbox_public_new(f.client, &f.config, &f.outbox, &f.error),
                   LC_OK);
  dispatcher = outbox_test_dispatcher(f.outbox, &f.error);
  assert_non_null(dispatcher);
  delay.tv_sec = 0;
  delay.tv_nsec = 10000000L;
  memset(&stats, 0, sizeof(stats));
  for (attempt = 0U; attempt < 500U; ++attempt) {
    assert_int_equal(dispatcher->get_stats(dispatcher, &stats, &f.error),
                     LC_OK);
    if (stats.terminal_reclaims == 3U)
      break;
    lc_outbox_stats_cleanup(&stats);
    (void)nanosleep(&delay, NULL);
  }
  assert_int_equal(stats.terminal_reclaims, 3U);
  assert_int_equal(stats.terminal_reclaim_failures, 0U);
  assert_int_equal(stats.dead_letter_reclaims, 0U);
  assert_int_equal(stats.recovery_queries, 0U);
  lc_outbox_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &f.error), LC_OK);
  dispatcher->close(dispatcher);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    outbox_assert_state_presence(f.client, f.config.ns, keys[kind - 1], 0,
                                 &f.error);
    terminal_retention_assert_attachments(&f, keys[kind - 1], 0U);
  }
  terminal_retention_close(&f);
}

static void test_pouch_terminal_retention_provisional_inbox(void **state) {
  terminal_retention_fixture f;
  lc_inbox_message message;
  lc_inbox_accept_result accepted;
  lc_outbox_transaction *transaction;
  lc_pouch_outbox_retention_marker marker;
  lc_i64 next_due;
  size_t reclaimed;
  (void)state;
  terminal_retention_open(&f);
  f.config.inbox_retention_seconds = 123L;
  terminal_retention_reopen(&f);
  lc_inbox_message_init(&message);
  message.consumer_id = "consumer";
  message.source_kind = "queue";
  message.source_id = "source";
  message.message_id = "provisional";
  transaction = NULL;
  assert_int_equal(lc_outbox_accept_inbox(f.outbox, &message, &transaction,
                                          &accepted, &f.error),
                   LC_OK);
  memset(&marker, 0, sizeof(marker));
  assert_int_equal(lc_pouch_outbox_retention_marker_take(
                       ((lc_client_handle *)f.client)->pouch, f.config.ns,
                       LC_POUCH_RETENTION_INBOX, NULL, (lc_i64)time(NULL) + 200,
                       SIZE_MAX, UINT64_MAX, &marker, &next_due, &f.error),
                   LC_OK);
  assert_non_null(marker.marker_id);
  assert_true(marker.due_at_unix <= (lc_i64)time(NULL) + 123);
  assert_true(marker.due_at_unix >= (lc_i64)time(NULL) + 120);
  assert_int_not_equal(lc_outbox_test_reclaim_terminal(
                           f.outbox, LC_POUCH_RETENTION_INBOX,
                           marker.due_at_unix, &reclaimed, &next_due, &f.error),
                       LC_OK);
  assert_int_equal(reclaimed, 0U);
  lc_error_cleanup(&f.error);
  lc_error_init(&f.error);
  assert_int_equal(lc_outbox_transaction_rollback(transaction, &f.error),
                   LC_OK);
  lc_outbox_transaction_close(transaction);
  assert_int_equal(lc_outbox_test_reclaim_terminal(
                       f.outbox, LC_POUCH_RETENTION_INBOX, marker.due_at_unix,
                       &reclaimed, &next_due, &f.error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U); /* Rolled-back provisional marker only. */
  transaction = NULL;
  assert_int_equal(lc_outbox_accept_inbox(f.outbox, &message, &transaction,
                                          &accepted, &f.error),
                   LC_OK);
  assert_true(accepted.accepted);
  assert_false(accepted.duplicate);
  assert_int_equal(lc_outbox_transaction_commit(transaction, &f.error), LC_OK);
  lc_outbox_transaction_close(transaction);
  lc_pouch_outbox_retention_marker_cleanup(
      ((lc_client_handle *)f.client)->pouch, &marker);
  terminal_retention_close(&f);
}

static void
test_pouch_terminal_retention_selector_is_fair_and_lazy(void **state) {
  terminal_retention_fixture f;
  const char *directories[3] = {"outbox-completed-reclaim",
                                "outbox-inbox-reclaim",
                                "outbox-command-reclaim"};
  const char *families[3] = {"outbox", "inbox", "command"};
  char ids[3][LC_XID_STRING_SIZE];
  char keys[3][128];
  char path[PATH_MAX];
  lc_acquire_req acquire;
  lc_lease *held;
  FILE *file;
  lc_i64 next_due;
  size_t reclaimed;
  int kind;
  unsigned int index;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    for (index = 0U; index < 3U; ++index) {
      assert_int_equal(lc_xid_new(ids[index], &f.error), LC_OK);
      assert_true(snprintf(keys[index], sizeof(keys[index]),
                           "__lockdc_io/v1/%s/selector-%u", families[kind - 1],
                           index) > 0);
      terminal_retention_seed(&f, kind, keys[index], ids[index],
                              index == 2U ? 5000 : 1);
    }
    assert_true(strcmp(ids[0], ids[1]) < 0);
    assert_true(snprintf(path, sizeof(path),
                         "%s/.lockd/%s/%s/0000000000001388-%s", f.root,
                         directories[kind - 1], f.config.ns, ids[2]) > 0);
    file = fopen(path, "wb");
    assert_non_null(file);
    assert_int_equal(fwrite("corrupt", 1U, 7U, file), 7U);
    assert_int_equal(fclose(file), 0);
    lc_acquire_req_init(&acquire);
    acquire.ns = f.config.ns;
    acquire.key = keys[0];
    acquire.owner = "selector-held";
    acquire.ttl_seconds = 30L;
    held = NULL;
    assert_int_equal(lc_acquire(f.client, &acquire, &held, &f.error), LC_OK);
    assert_int_not_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                         &reclaimed, &next_due,
                                                         &f.error),
                         LC_OK);
    lc_error_cleanup(&f.error);
    lc_error_init(&f.error);
    /* Busy first marker advances the turn to the other eligible receipt.
     * The future corrupt body must not be opened during either attempt. */
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 1U);
    outbox_assert_state_presence(f.client, f.config.ns, keys[1], 0, &f.error);
    assert_int_equal(lc_lease_release(held, NULL, &f.error), LC_OK);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 1U);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 0U);
    assert_int_equal(next_due, 5000);
    assert_int_equal(lc_outbox_test_reclaim_terminal(
                         f.outbox, kind, 5000, &reclaimed, &next_due, &f.error),
                     LC_ERR_PROTOCOL);
    lc_error_cleanup(&f.error);
    lc_error_init(&f.error);
    outbox_assert_state_presence(f.client, f.config.ns, keys[2], 1, &f.error);
    terminal_retention_assert_attachments(&f, keys[2], 1U);
  }
  terminal_retention_close(&f);
}

static void
test_pouch_terminal_retention_corrupt_marker_does_not_starve(void **state) {
  terminal_retention_fixture f;
  const char *directories[3] = {"outbox-completed-reclaim",
                                "outbox-inbox-reclaim",
                                "outbox-command-reclaim"};
  const char *families[3] = {"outbox", "inbox", "command"};
  char ids[2][LC_XID_STRING_SIZE];
  char keys[2][128];
  char path[PATH_MAX];
  char renamed[PATH_MAX];
  FILE *file;
  lc_i64 next_due;
  size_t reclaimed;
  int kind;
  int mode;
  unsigned int index;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    for (mode = 0; mode < 2; ++mode) {
      for (index = 0U; index < 2U; ++index) {
        assert_int_equal(lc_xid_new(ids[index], &f.error), LC_OK);
        assert_true(snprintf(keys[index], sizeof(keys[index]),
                             "__lockdc_io/v1/%s/corrupt-%d-%u",
                             families[kind - 1], mode, index) > 0);
        terminal_retention_seed(&f, kind, keys[index], ids[index], 1);
      }
      assert_true(strcmp(ids[0], ids[1]) < 0);
      assert_true(snprintf(path, sizeof(path),
                           "%s/.lockd/%s/%s/0000000000000001-%s", f.root,
                           directories[kind - 1], f.config.ns, ids[0]) > 0);
      if (mode == 0) {
        file = fopen(path, "wb");
        assert_non_null(file);
        assert_int_equal(fwrite("corrupt", 1U, 7U, file), 7U);
        assert_int_equal(fclose(file), 0);
      } else {
        assert_true(snprintf(renamed, sizeof(renamed),
                             "%s/.lockd/%s/%s/0000000000000002-%s", f.root,
                             directories[kind - 1], f.config.ns, ids[0]) > 0);
        assert_int_equal(rename(path, renamed), 0);
      }
      assert_int_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                       &reclaimed, &next_due,
                                                       &f.error),
                       LC_ERR_PROTOCOL);
      assert_int_equal(reclaimed, 0U);
      lc_error_cleanup(&f.error);
      lc_error_init(&f.error);
      /* Keep the bad marker for diagnosis, but advance past its filename
       * identity so another eligible parent can still be reclaimed. */
      assert_int_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                       &reclaimed, &next_due,
                                                       &f.error),
                       LC_OK);
      assert_int_equal(reclaimed, 1U);
      outbox_assert_state_presence(f.client, f.config.ns, keys[1], 0, &f.error);
      terminal_retention_assert_attachments(&f, keys[1], 0U);
      outbox_assert_state_presence(f.client, f.config.ns, keys[0], 1, &f.error);
      terminal_retention_assert_attachments(&f, keys[0], 1U);
      /* Wrapping the turn retries the corruption; it is never silently lost. */
      assert_int_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                       &reclaimed, &next_due,
                                                       &f.error),
                       LC_ERR_PROTOCOL);
      lc_error_cleanup(&f.error);
      lc_error_init(&f.error);
      if (mode == 1)
        assert_int_equal(rename(renamed, path), 0);
      assert_int_equal(lc_pouch_outbox_retention_marker_write(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           kind, ids[0], keys[0], 1, 0U, &f.error),
                       LC_OK);
      assert_int_equal(lc_outbox_test_reclaim_terminal(f.outbox, kind, 1000,
                                                       &reclaimed, &next_due,
                                                       &f.error),
                       LC_OK);
      assert_int_equal(reclaimed, 1U);
      outbox_assert_state_presence(f.client, f.config.ns, keys[0], 0, &f.error);
      terminal_retention_assert_attachments(&f, keys[0], 0U);
    }
  }
  terminal_retention_close(&f);
}

static void test_pouch_terminal_retention_filename_deadlines(void **state) {
  terminal_retention_fixture f;
  lc_pouch_outbox_retention_marker marker;
  lc_i64 deadlines[3];
  lc_i64 next_due;
  char id[LC_XID_STRING_SIZE];
  int kind;
  unsigned int index;
  (void)state;
  deadlines[0] = 1000;
  deadlines[1] = ((lc_i64)0x01234567UL << 32U) | (lc_i64)0x89abcdefUL;
  deadlines[2] = LC_I64_MAX;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    for (index = 0U; index < 3U; ++index) {
      assert_int_equal(lc_xid_new(id, &f.error), LC_OK);
      assert_int_equal(lc_pouch_outbox_retention_marker_write(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           kind, id, "__lockdc_io/v1/inbox/deadline",
                           deadlines[index], 0U, &f.error),
                       LC_OK);
      memset(&marker, 0, sizeof(marker));
      assert_int_equal(lc_pouch_outbox_retention_marker_take(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           kind, NULL, deadlines[index] - 1, SIZE_MAX,
                           UINT64_MAX, &marker, &next_due, &f.error),
                       LC_OK);
      assert_null(marker.marker_id);
      assert_int_equal(next_due, deadlines[index]);
      assert_int_equal(lc_pouch_outbox_retention_marker_take(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           kind, NULL, deadlines[index], SIZE_MAX, UINT64_MAX,
                           &marker, &next_due, &f.error),
                       LC_OK);
      assert_string_equal(marker.marker_id, id);
      assert_int_equal(marker.due_at_unix, deadlines[index]);
      lc_pouch_outbox_retention_marker_cleanup(
          ((lc_client_handle *)f.client)->pouch, &marker);
      assert_int_equal(lc_pouch_outbox_retention_marker_remove(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           kind, id, deadlines[index], &f.error),
                       LC_OK);
    }
  }
  terminal_retention_close(&f);
}

static void test_pouch_terminal_retention_invalid_configuration(void **state) {
  terminal_retention_fixture f;
  lc_outbox_config config;
  lc_outbox *invalid;
  int kind;
  int pass;
  long seconds;
  (void)state;
  terminal_retention_open(&f);
  for (kind = LC_POUCH_RETENTION_COMPLETED;
       kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind) {
    for (pass = 0; pass < 2; ++pass) {
      if (pass == 1 && !outbox_duration_overflows_unix_range(LONG_MAX))
        continue;
      seconds = pass == 0 ? -2L : LONG_MAX;
      config = f.config;
      if (kind == LC_POUCH_RETENTION_COMPLETED)
        config.completed_retention_seconds = seconds;
      else if (kind == LC_POUCH_RETENTION_INBOX)
        config.inbox_retention_seconds = seconds;
      else
        config.command_retention_seconds = seconds;
      invalid = NULL;
      assert_int_equal(outbox_public_new(f.client, &config, &invalid, &f.error),
                       LC_ERR_INVALID);
      assert_null(invalid);
      lc_error_cleanup(&f.error);
      lc_error_init(&f.error);
    }
  }
  terminal_retention_close(&f);
}

static void
test_pouch_dead_letter_retention_corrupt_markers_do_not_starve(void **state) {
  terminal_retention_fixture f;
  char ids[4][LC_XID_STRING_SIZE];
  char keys[4][128];
  char path[PATH_MAX];
  FILE *file;
  lc_i64 due;
  lc_i64 next_due;
  size_t reclaimed;
  unsigned int mode;
  unsigned int index;
  unsigned int pass;
  int rc;
  (void)state;
  terminal_retention_open(&f);
  /* Exercise expiry and both overload paths, including name-only successor
   * discovery after the scanner has stopped reading capacity-prefix bodies. */
  for (mode = 0U; mode < 3U; ++mode) {
    f.config.dead_letter_max_count = mode == 1U ? 1U : SIZE_MAX;
    f.config.dead_letter_max_bytes = mode == 2U ? 1U : UINT64_MAX;
    terminal_retention_reopen(&f);
    due = mode == 0U ? 1 : 5000;
    for (index = 0U; index < 4U; ++index) {
      assert_int_equal(lc_xid_new(ids[index], &f.error), LC_OK);
      assert_true(snprintf(keys[index], sizeof(keys[index]),
                           "__lockdc_io/v1/outbox/corrupt-dlq-%u-%u", mode,
                           index) > 0);
      seed_retained_dead_letter_outbox(f.client, f.config.ns, keys[index],
                                       ids[index], &f.error);
      assert_int_equal(lc_pouch_outbox_retention_marker_write(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           LC_POUCH_RETENTION_DEAD_LETTER, ids[index],
                           keys[index], due, 32U, &f.error),
                       LC_OK);
      if (index % 2U == 0U) {
        assert_true(snprintf(path, sizeof(path),
                             "%s/.lockd/outbox-dead-letter-reclaim/%s/%s",
                             f.root, f.config.ns, ids[index]) > 0);
        file = fopen(path, "wb");
        assert_non_null(file);
        assert_int_equal(fwrite("corrupt", 1U, 7U, file), 7U);
        assert_int_equal(fclose(file), 0);
      }
    }
    for (pass = 0U; pass < 12U; ++pass) {
      rc = lc_outbox_test_reclaim_dead_letters(f.outbox, 1000, &reclaimed,
                                               &next_due, &f.error);
      assert_true(rc == LC_OK || rc == LC_ERR_PROTOCOL);
      lc_error_cleanup(&f.error);
      lc_error_init(&f.error);
    }
    for (index = 0U; index < 4U; ++index) {
      outbox_assert_state_presence(f.client, f.config.ns, keys[index],
                                   index % 2U == 0U, &f.error);
      terminal_retention_assert_attachments(&f, keys[index],
                                            index % 2U == 0U ? 1U : 0U);
    }
    /* Damaged work remains diagnosable and is reclaimed after repair. */
    for (index = 0U; index < 4U; index += 2U)
      assert_int_equal(lc_pouch_outbox_retention_marker_write(
                           ((lc_client_handle *)f.client)->pouch, f.config.ns,
                           LC_POUCH_RETENTION_DEAD_LETTER, ids[index],
                           keys[index], 1, 32U, &f.error),
                       LC_OK);
    assert_int_equal(lc_outbox_test_reclaim_dead_letters(
                         f.outbox, 1000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 2U);
    for (index = 0U; index < 4U; ++index)
      outbox_assert_state_presence(f.client, f.config.ns, keys[index], 0,
                                   &f.error);
  }
  terminal_retention_close(&f);
}

static void
test_pouch_dead_letter_retention_is_bounded_and_generation_safe(void **state) {
  char endpoint[PATH_MAX];
  const char *endpoints[1];
  char first_id[LC_XID_STRING_SIZE];
  char second_id[LC_XID_STRING_SIZE];
  char third_id[LC_XID_STRING_SIZE];
  char stale_id[LC_XID_STRING_SIZE];
  char root[PATH_MAX];
  char third_marker_path[PATH_MAX];
  char template_path[PATH_MAX];
  const char *first_key;
  const char *second_key;
  lc_client_config client_config;
  lc_client_handle *client_handle;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_outbox_retention_marker selected_marker;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_error error;
  lc_acquire_req hold_acquire;
  lc_lease *hold_lease;
  lc_lease *second_hold_lease;
  lc_i64 next_due;
  size_t reclaimed;
  unsigned int pass;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "dead-letter-retention-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false&single_writer=false",
                       root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  outbox = NULL;
  hold_lease = NULL;
  second_hold_lease = NULL;
  memset(&selected_marker, 0, sizeof(selected_marker));
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  /* A successful empty maintenance turn must schedule a bounded future
   * rediscovery. A shared writer can publish a marker after that first scan;
   * the dispatcher will observe it without ever adding retention work to
   * direct next()/next_with_state() calls. */
  next_due = 0;
  assert_int_equal(
      lc_outbox_test_dead_letter_discovery_deadline(1000, 0, &next_due, &error),
      LC_OK);
  assert_int_equal(next_due, 1060);
  assert_int_equal(lc_outbox_test_dead_letter_discovery_deadline(
                       1000, 1005, &next_due, &error),
                   LC_OK);
  assert_int_equal(next_due, 1005);
  assert_int_equal(lc_outbox_test_dead_letter_discovery_deadline(
                       1000, 2000, &next_due, &error),
                   LC_OK);
  assert_int_equal(next_due, 1060);
  lc_outbox_config_init(&config);
  config.ns = "outbox-dead-letter-retention";
  config.owner = "outbox-dead-letter-retention-test";
  config.dead_letter_retention_seconds = 3600L;
  config.dead_letter_max_count = 1U;
  config.dead_letter_max_bytes = UINT64_MAX;
  /* Keep this producer threadless: the test drives maintenance with an exact
   * synthetic clock and consequently needs neither sleeps nor a timing race. */
  assert_int_equal(outbox_public_new(client, &config, &outbox, &error), LC_OK);
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  assert_int_equal(lc_xid_new(second_id, &error), LC_OK);
  if (strcmp(first_id, second_id) < 0) {
    first_key = "__lockdc_io/v1/outbox/retention/first";
    second_key = "__lockdc_io/v1/outbox/retention/second";
  } else {
    char temporary_id[LC_XID_STRING_SIZE];

    memcpy(temporary_id, first_id, sizeof(temporary_id));
    memcpy(first_id, second_id, sizeof(first_id));
    memcpy(second_id, temporary_id, sizeof(second_id));
    first_key = "__lockdc_io/v1/outbox/retention/first";
    second_key = "__lockdc_io/v1/outbox/retention/second";
  }
  seed_retained_dead_letter_outbox(client, config.ns, first_key, first_id,
                                   &error);
  seed_retained_dead_letter_outbox(client, config.ns, second_key, second_id,
                                   &error);
  client_handle = (lc_client_handle *)client;
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, first_id, first_key,
                       5000, 32U, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, second_id, second_key,
                       6000, 32U, &error),
                   LC_OK);
  reclaimed = 0U;
  next_due = 0;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 1000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);
  assert_int_equal(next_due, 6000);
  outbox_assert_state_presence(client, config.ns, first_key, 0, &error);
  outbox_assert_state_presence(client, config.ns, second_key, 1, &error);
  reclaimed = 0U;
  next_due = 0;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 6000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);
  outbox_assert_state_presence(client, config.ns, second_key, 0, &error);

  /* An old marker must not delete a record that was replayed and then entered
   * a later dead-letter generation. It is stale control data, not a failure. */
  assert_int_equal(lc_xid_new(stale_id, &error), LC_OK);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/replayed",
                                   second_id, &error);
  assert_int_equal(
      lc_pouch_outbox_retention_marker_write(
          client_handle->pouch, config.ns, LC_POUCH_RETENTION_DEAD_LETTER,
          stale_id, "__lockdc_io/v1/outbox/retention/replayed", 1, 32U, &error),
      LC_OK);
  reclaimed = 0U;
  next_due = 0;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/replayed", 1, &error);
  reclaimed = 1U;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 0U);

  /* A maintenance collision leaves the parent and its marker retryable. Once
   * the active claim releases, the same exact marker completes cleanup. */
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/held",
                                   first_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, first_id,
                       "__lockdc_io/v1/outbox/retention/held", 1, 32U, &error),
                   LC_OK);
  assert_int_equal(lc_xid_new(second_id, &error), LC_OK);
  assert_true(strcmp(second_id, first_id) > 0);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/held-next",
                                   second_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, second_id,
                       "__lockdc_io/v1/outbox/retention/held-next", 1, 32U,
                       &error),
                   LC_OK);
  assert_int_equal(lc_xid_new(third_id, &error), LC_OK);
  assert_true(strcmp(third_id, second_id) > 0);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/held-third",
                                   third_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, third_id,
                       "__lockdc_io/v1/outbox/retention/held-third", 1, 32U,
                       &error),
                   LC_OK);
  lc_acquire_req_init(&hold_acquire);
  hold_acquire.ns = config.ns;
  hold_acquire.key = "__lockdc_io/v1/outbox/retention/held";
  hold_acquire.owner = "outbox-retention-held";
  hold_acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &hold_acquire, &hold_lease, &error),
                   LC_OK);
  hold_acquire.key = "__lockdc_io/v1/outbox/retention/held-next";
  assert_int_equal(
      lc_acquire(client, &hold_acquire, &second_hold_lease, &error), LC_OK);
  /* The cursor follows lexical marker order, not readdir order. Otherwise a
   * blocked late marker can repeatedly leap over another busy parent and
   * prevent a healthy one from ever being selected. */
  assert_int_equal(lc_pouch_outbox_retention_marker_take(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, first_id, 7000, 1U,
                       UINT64_MAX, &selected_marker, &next_due, &error),
                   LC_OK);
  assert_string_equal(selected_marker.marker_id, second_id);
  lc_pouch_outbox_retention_marker_cleanup(client_handle->pouch,
                                           &selected_marker);
  assert_int_equal(lc_pouch_outbox_retention_marker_take(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, second_id, 7000, 1U,
                       UINT64_MAX, &selected_marker, &next_due, &error),
                   LC_OK);
  assert_string_equal(selected_marker.marker_id, third_id);
  lc_pouch_outbox_retention_marker_cleanup(client_handle->pouch,
                                           &selected_marker);
  /* The real reclaimer may encounter the third marker on its first turn or
   * after either lease collision. No fixed enumeration order is assumed. */
  for (pass = 0U; pass < 8U; ++pass) {
    reclaimed = 0U;
    if (lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed, &next_due,
                                            &error) != LC_OK) {
      lc_error_cleanup(&error);
      lc_error_init(&error);
    }
  }
  assert_true(snprintf(third_marker_path, sizeof(third_marker_path),
                       "%s/.lockd/outbox-dead-letter-reclaim/%s/%s", root,
                       config.ns, third_id) > 0);
  outbox_assert_marker_presence(client, config.ns,
                                LC_POUCH_RETENTION_DEAD_LETTER, third_id, 0);
  if (!client_handle->pouch->storage.ops) {
    assert_int_equal(access(third_marker_path, F_OK), -1);
    assert_int_equal(errno, ENOENT);
  }
  assert_int_equal(lc_lease_release(hold_lease, NULL, &error), LC_OK);
  hold_lease = NULL;
  assert_int_equal(lc_lease_release(second_hold_lease, NULL, &error), LC_OK);
  second_hold_lease = NULL;
  outbox_assert_state_presence(client, config.ns,
                               "__lockdc_io/v1/outbox/retention/held-third", 0,
                               &error);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/held", 1, &error);
  outbox_assert_state_presence(client, config.ns,
                               "__lockdc_io/v1/outbox/retention/held-next", 1,
                               &error);
  reclaimed = 0U;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 2U);
  outbox_assert_state_presence(client, config.ns,
                               "__lockdc_io/v1/outbox/retention/held-next", 0,
                               &error);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/held", 0, &error);

  /* A failure after the exact parent claim but before sidecar cleanup leaves
   * the durable marker in place. It must be retryable rather than silently
   * dropping the only automatic-retention work item. */
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/cleanup",
                                   first_id, &error);
  assert_int_equal(
      lc_pouch_outbox_retention_marker_write(
          client_handle->pouch, config.ns, LC_POUCH_RETENTION_DEAD_LETTER,
          first_id, "__lockdc_io/v1/outbox/retention/cleanup", 1, 32U, &error),
      LC_OK);
  lc_outbox_test_before_dead_letter_claim_cleanup_hook = outbox_fail_allocation;
  reclaimed = 0U;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_ERR_NOMEM);
  assert_int_equal(reclaimed, 0U);
  lc_outbox_test_before_dead_letter_claim_cleanup_hook = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/cleanup", 1, &error);
  reclaimed = 0U;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/cleanup", 0, &error);

  /* A marker orphaned by a crash after the durable parent delete must be
   * consumed as stale rather than poisoning every later maintenance turn.
   * Keep this synthetic-clock case producer-only; public management starts a
   * real dispatcher and is covered separately below. */
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  assert_int_equal(
      lc_pouch_outbox_retention_marker_write(
          client_handle->pouch, config.ns, LC_POUCH_RETENTION_DEAD_LETTER,
          first_id, "__lockdc_io/v1/outbox/retention/absent", 1, 32U, &error),
      LC_OK);
  reclaimed = 0U;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);

  /* The byte bound uses the same exact-key path, independently of the count
   * bound. A measured payload-bearing envelope over the cap is reclaimed even
   * though its expiry is still far in the future. */
  assert_null(client_handle->outbox_dispatchers);
  lc_outbox_close(outbox);
  outbox = NULL;
  config.ns = "outbox-dead-letter-retention-bytes";
  config.dead_letter_max_count = SIZE_MAX;
  config.dead_letter_max_bytes = 1U;
  assert_int_equal(outbox_public_new(client, &config, &outbox, &error), LC_OK);
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  assert_int_equal(lc_xid_new(second_id, &error), LC_OK);
  assert_true(strcmp(second_id, first_id) > 0);
  seed_retained_dead_letter_outbox(
      client, config.ns, "__lockdc_io/v1/outbox/retention/bytes-first",
      first_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, first_id,
                       "__lockdc_io/v1/outbox/retention/bytes-first", 30000,
                       32U, &error),
                   LC_OK);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/bytes-next",
                                   second_id, &error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, second_id,
                       "__lockdc_io/v1/outbox/retention/bytes-next", 30000, 32U,
                       &error),
                   LC_OK);
  /* A single oversized marker can cross the byte limit immediately. A busy
   * first parent must not pin the turn cursor there forever: taking after it
   * advances to the next marker without turning capacity control into an
   * inventory scan. */
  memset(&selected_marker, 0, sizeof(selected_marker));
  assert_int_equal(lc_pouch_outbox_retention_marker_take(
                       client_handle->pouch, config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, first_id, 1000, SIZE_MAX,
                       1U, &selected_marker, &next_due, &error),
                   LC_OK);
  assert_string_equal(selected_marker.marker_id, second_id);
  lc_pouch_outbox_retention_marker_cleanup(client_handle->pouch,
                                           &selected_marker);
  reclaimed = 0U;
  next_due = 0;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 1000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 2U);
  outbox_assert_state_presence(client, config.ns,
                               "__lockdc_io/v1/outbox/retention/bytes-first", 0,
                               &error);
  outbox_assert_state_presence(client, config.ns,
                               "__lockdc_io/v1/outbox/retention/bytes-next", 0,
                               &error);

  /* Marker scheduling is outside the namespace log. A compaction and a fresh
   * Pouch session must still recover the exact retained parent without
   * replaying or scanning the outbox namespace. */
  lc_outbox_close(outbox);
  outbox = NULL;
  config.ns = "outbox-dead-letter-retention-reopen";
  config.dead_letter_max_count = 1U;
  config.dead_letter_max_bytes = UINT64_MAX;
  assert_int_equal(outbox_public_new(client, &config, &outbox, &error), LC_OK);
  assert_int_equal(lc_xid_new(first_id, &error), LC_OK);
  seed_retained_dead_letter_outbox(client, config.ns,
                                   "__lockdc_io/v1/outbox/retention/reopen",
                                   first_id, &error);
  assert_int_equal(
      lc_pouch_outbox_retention_marker_write(
          client_handle->pouch, config.ns, LC_POUCH_RETENTION_DEAD_LETTER,
          first_id, "__lockdc_io/v1/outbox/retention/reopen", 1, 32U, &error),
      LC_OK);
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  maintenance_options.ns = config.ns;
  maintenance_options.force = 1;
  assert_int_equal(lc_pouch_maintenance_run(client_handle->pouch,
                                            &maintenance_options,
                                            &maintenance_result, &error),
                   LC_OK);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_outbox_close(outbox);
  outbox = NULL;
  lc_client_close(client);
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  client_handle = (lc_client_handle *)client;
  assert_int_equal(outbox_public_new(client, &config, &outbox, &error), LC_OK);
  reclaimed = 0U;
  next_due = 0;
  assert_int_equal(lc_outbox_test_reclaim_dead_letters(outbox, 7000, &reclaimed,
                                                       &next_due, &error),
                   LC_OK);
  assert_int_equal(reclaimed, 1U);
  outbox_assert_state_presence(
      client, config.ns, "__lockdc_io/v1/outbox/retention/reopen", 0, &error);

  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

static void
test_pouch_dead_letter_manual_delete_before_retention(void **state) {
  terminal_retention_fixture f;
  lc_outbox_dispatcher *dispatcher;
  lc_pouch_outbox_retention_marker marker;
  lc_pouch *pouch;
  char id[LC_XID_STRING_SIZE];
  const char *key = "__lockdc_io/v1/outbox/retention/manual";
  lc_i64 due;
  lc_i64 next_due;
  size_t reclaimed;
  unsigned int scenario;

  (void)state;
  for (scenario = 0U; scenario < 2U; ++scenario) {
    terminal_retention_open(&f);
    /* An already-due marker is safe for management only when automatic
     * retention is disabled. With retention enabled, keep the record live. */
    f.config.dead_letter_retention_seconds = scenario == 0U ? -1L : 3600L;
    terminal_retention_reopen(&f);
    pouch = ((lc_client_handle *)f.client)->pouch;
    due = scenario == 0U ? 1 : (lc_i64)time(NULL) + 3600;
    assert_int_equal(lc_xid_new(id, &f.error), LC_OK);
    seed_retained_dead_letter_outbox(f.client, f.config.ns, key, id, &f.error);
    assert_int_equal(lc_pouch_outbox_retention_marker_write(
                         pouch, f.config.ns, LC_POUCH_RETENTION_DEAD_LETTER, id,
                         key, due, 32U, &f.error),
                     LC_OK);
    reclaimed = 0U;
    next_due = 0;
    assert_int_equal(lc_outbox_test_reclaim_dead_letters(
                         f.outbox, 7000, &reclaimed, &next_due, &f.error),
                     LC_OK);
    assert_int_equal(reclaimed, 0U);
    outbox_assert_state_presence(f.client, f.config.ns, key, 1, &f.error);

    dispatcher = NULL;
    assert_int_equal(
        outbox_public_get_or_start(f.outbox, &dispatcher, &f.error), LC_OK);
    assert_int_equal(
        lc_outbox_dispatcher_delete_dead_letter(dispatcher, key, &f.error),
        LC_OK);
    outbox_assert_state_presence(f.client, f.config.ns, key, 0, &f.error);
    memset(&marker, 0, sizeof(marker));
    next_due = 0;
    assert_int_equal(lc_pouch_outbox_retention_marker_take(
                         pouch, f.config.ns, LC_POUCH_RETENTION_DEAD_LETTER,
                         NULL, 7000, SIZE_MAX, UINT64_MAX, &marker, &next_due,
                         &f.error),
                     LC_OK);
    assert_null(marker.marker_id);
    assert_int_equal(next_due, 0);
    lc_pouch_outbox_retention_marker_cleanup(pouch, &marker);
    assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &f.error),
                     LC_OK);
    lc_outbox_dispatcher_close(dispatcher);
    terminal_retention_close(&f);
  }
}

static void test_pouch_dead_letter_management_after_retention(void **state) {
  terminal_retention_fixture f;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_stats stats;
  char id[LC_XID_STRING_SIZE];
  const char *key = "__lockdc_io/v1/outbox/retention/expired";
  struct timespec delay;
  unsigned int attempt;

  (void)state;
  terminal_retention_open(&f);
  assert_int_equal(lc_xid_new(id, &f.error), LC_OK);
  seed_retained_dead_letter_outbox(f.client, f.config.ns, key, id, &f.error);
  assert_int_equal(lc_pouch_outbox_retention_marker_write(
                       ((lc_client_handle *)f.client)->pouch, f.config.ns,
                       LC_POUCH_RETENTION_DEAD_LETTER, id, key, 1, 32U,
                       &f.error),
                   LC_OK);
  dispatcher = NULL;
  assert_int_equal(outbox_public_get_or_start(f.outbox, &dispatcher, &f.error),
                   LC_OK);
  /* Force retention to finish before management instead of relying on which
   * thread happens to win. This is the release failure's exact interleaving. */
  delay.tv_sec = 0;
  delay.tv_nsec = 10000000L;
  for (attempt = 0U; attempt < 500U; ++attempt) {
    lc_outbox_stats_init(&stats);
    assert_int_equal(
        lc_outbox_dispatcher_get_stats(dispatcher, &stats, &f.error), LC_OK);
    if (stats.dead_letter_reclaims == 1U)
      break;
    lc_outbox_stats_cleanup(&stats);
    (void)nanosleep(&delay, NULL);
  }
  assert_true(attempt < 500U);
  assert_int_equal(stats.dead_letter_reclaim_failures, 0U);
  lc_outbox_stats_cleanup(&stats);
  outbox_assert_state_presence(f.client, f.config.ns, key, 0, &f.error);
  assert_int_equal(
      lc_outbox_dispatcher_delete_dead_letter(dispatcher, key, &f.error),
      LC_ERR_INVALID);
  lc_error_cleanup(&f.error);
  lc_error_init(&f.error);
  assert_int_equal(
      lc_outbox_dispatcher_replay_dead_letter(dispatcher, key, &f.error),
      LC_ERR_INVALID);
  lc_error_cleanup(&f.error);
  lc_error_init(&f.error);
  assert_int_equal(lc_outbox_dispatcher_stop(dispatcher, -1L, &f.error), LC_OK);
  lc_outbox_dispatcher_close(dispatcher);
  terminal_retention_close(&f);
}

static void test_outbox_public_rejections_clear_handle_outputs(void **state) {
  char endpoint[PATH_MAX];
  const char *endpoints[1];
  char root[PATH_MAX];
  char template_path[PATH_MAX];
  lc_client_config client_config;
  lc_outbox_config config;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       OUTBOX_TMP_PREFIX "receiver-output-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  OUTBOX_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_outbox_config_init(&config);
  client = NULL;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  outbox = (lc_outbox *)(uintptr_t)1U;
  assert_int_equal(client->new_outbox_with_dispatcher(client, &config, NULL,
                                                      &outbox, &error),
                   LC_ERR_INVALID);
  assert_null(outbox);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  outbox = (lc_outbox *)(uintptr_t)1U;
  assert_int_equal(outbox_public_new(NULL, &config, &outbox, &error),
                   LC_ERR_INVALID);
  assert_null(outbox);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  outbox = (lc_outbox *)(uintptr_t)1U;
  assert_int_equal(
      outbox_public_new_with_dispatcher(NULL, &config, NULL, &outbox, &error),
      LC_ERR_INVALID);
  assert_null(outbox);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  dispatcher = (lc_outbox_dispatcher *)(uintptr_t)1U;
  assert_int_equal(outbox_public_get_or_start(NULL, &dispatcher, &error),
                   LC_ERR_INVALID);
  assert_null(dispatcher);
  lc_error_cleanup(&error);
  lc_client_close(client);
  lc_test_tmp_cleanup_path(root, OUTBOX_TMP_PREFIX);
}

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(
          test_pouch_outbox_duplicate_rejects_immutable_envelope_conflicts),
      cmocka_unit_test(test_pouch_outbox_stateful_delivery_lifecycle),
      cmocka_unit_test(test_pouch_outbox_stateful_renewal_terminal_versions),
      cmocka_unit_test(test_pouch_outbox_stateful_renewal_terminal_failure),
      cmocka_unit_test(test_pouch_outbox_stateful_handoff_failure_rolls_back),
      cmocka_unit_test(test_pouch_outbox_transaction_and_duplicate),
      cmocka_unit_test(test_pouch_duplicate_inbox_handles_json_runtime_failure),
      cmocka_unit_test(test_pouch_participant_cleanup_after_transaction_close),
      cmocka_unit_test(
          test_pouch_participant_allocation_failure_rolls_back_enrollment),
      cmocka_unit_test(
          test_pouch_command_receipt_allocation_failure_rolls_back_enrollment),
      cmocka_unit_test(test_pouch_command_terminal_load_failure_cleans_record),
      cmocka_unit_test(
          test_pouch_outbox_allocation_failures_roll_back_enrollment),
      cmocka_unit_test(
          test_pouch_notification_allocation_failure_reconciles_committed_outbox),
      cmocka_unit_test(test_pouch_claim_recovery_respects_combined_capacity),
      cmocka_unit_test(
          test_pouch_retry_notification_allocation_failure_recovers_at_deadline),
      cmocka_unit_test(test_outbox_delayed_recovery_waits_for_deadline),
      cmocka_unit_test(test_outbox_startup_replay_defers_retention_wakeups),
      cmocka_unit_test(
          test_outbox_dead_letter_capacity_burst_keeps_coalesced_wakeup),
      cmocka_unit_test(test_outbox_handed_off_job_keeps_root_timeout),
      cmocka_unit_test(test_outbox_claim_deadline_uses_replay_generation),
      cmocka_unit_test(test_pouch_transient_claim_failure_is_rescheduled),
      cmocka_unit_test(
          test_outbox_active_remote_claim_conflict_is_not_retryable),
      cmocka_unit_test(
          test_pouch_reconciliation_preserves_earliest_claim_recovery),
      cmocka_unit_test(test_pouch_overflowing_foreground_retry_reconciles),
      cmocka_unit_test(test_pouch_auto_retry_long_max_cap_is_safe),
      cmocka_unit_test(
          test_pouch_claim_recovery_allocation_failure_recovers_at_expiry),
      cmocka_unit_test(
          test_pouch_command_receipt_commits_with_outbox_and_result),
      cmocka_unit_test(test_pouch_generated_command_key_waits_by_id),
      cmocka_unit_test(
          test_pouch_command_receipt_rejects_malformed_terminal_records),
      cmocka_unit_test(
          test_pouch_command_attachment_failure_aborts_transaction),
      cmocka_unit_test(test_pouch_outbox_completion_evidence_is_bounded),
      cmocka_unit_test(test_pouch_terminal_retention_public_transitions),
      cmocka_unit_test(test_pouch_terminal_retention_count_optout_and_time),
      cmocka_unit_test(test_pouch_terminal_retention_live_dispatcher_restart),
      cmocka_unit_test(test_pouch_terminal_retention_provisional_inbox),
      cmocka_unit_test(test_pouch_terminal_retention_selector_is_fair_and_lazy),
      cmocka_unit_test(test_pouch_terminal_retention_invalid_configuration),
      cmocka_unit_test(
          test_pouch_terminal_retention_corrupt_marker_does_not_starve),
      cmocka_unit_test(test_pouch_terminal_retention_filename_deadlines),
      cmocka_unit_test(
          test_pouch_terminal_retention_generation_busy_and_failure),
      cmocka_unit_test(
          test_pouch_dead_letter_retention_is_bounded_and_generation_safe),
      cmocka_unit_test(test_pouch_dead_letter_manual_delete_before_retention),
      cmocka_unit_test(test_pouch_dead_letter_management_after_retention),
      cmocka_unit_test(
          test_pouch_dead_letter_retention_corrupt_markers_do_not_starve),
      cmocka_unit_test(test_outbox_public_rejections_clear_handle_outputs),
      cmocka_unit_test(test_pouch_outbox_rejects_overflowing_deadlines),
      cmocka_unit_test(test_pouch_outbox_retry_uses_wide_timestamp_comparison),
      cmocka_unit_test(
          test_pouch_outbox_periodic_schedule_failure_does_not_deadlock),
      cmocka_unit_test(test_pouch_shared_command_resume_is_durable),
      cmocka_unit_test(test_pouch_multikey_prevote_failure_publishes_nothing),
      cmocka_unit_test(
          test_pouch_multikey_commit_retry_retains_outbox_notification),
      cmocka_unit_test(test_pouch_partial_command_commit_freezes_transaction),
      cmocka_unit_test(test_pouch_indeterminate_first_vote_freezes_transaction),
      cmocka_unit_test(
          test_pouch_indeterminate_commit_replay_retains_outbox_notification),
      cmocka_unit_test_setup_teardown(
          test_pouch_expired_commit_reports_rollback_without_signal, NULL,
          outbox_pouch_test_clock_teardown),
      cmocka_unit_test(
          test_pouch_recovery_overflow_resumes_after_capacity_frees),
      cmocka_unit_test(test_pouch_claim_recovery_overflow_resumes),
      cmocka_unit_test(
          test_pouch_delayed_wake_does_not_block_ready_reconciliation),
      cmocka_unit_test(test_pouch_evicted_claim_wake_recovers_at_expiry),
      cmocka_unit_test(test_pouch_claim_recovery_survives_capacity_pause),
      cmocka_unit_test(
          test_pouch_concurrent_dispatcher_acquisition_keeps_one_attachment_ref),
      cmocka_unit_test(test_pouch_reconciliation_retains_overflow_request),
      cmocka_unit_test(test_pouch_reconciliation_skips_disappeared_outbox),
      cmocka_unit_test(test_pouch_handoff_skips_disappeared_outbox),
      cmocka_unit_test(test_pouch_dead_letter_operations),
      cmocka_unit_test(test_pouch_startup_replay_precedes_due_retention),
      cmocka_unit_test(
          test_pouch_dead_letter_management_rejects_invalid_candidates),
      cmocka_unit_test(test_pouch_startup_recovery_claims_seeded_outbox),
      cmocka_unit_test(test_pouch_clean_reopen_reconciles_durable_index),
      cmocka_unit_test(test_pouch_shared_reopen_reconciles_durable_index),
      cmocka_unit_test(test_pouch_compacted_reopen_reconciles_released_outbox),
      cmocka_unit_test(test_pouch_dispatcher_wakeup_isolated_from_next_waiters),
      cmocka_unit_test(test_pouch_next_timeout_uses_one_deadline),
      cmocka_unit_test(
          test_pouch_next_long_max_timeout_waits_until_dispatcher_stops),
      cmocka_unit_test(test_pouch_reconciliation_pages_large_outbox),
      cmocka_unit_test(
          test_pouch_reconciled_terminal_transport_failure_is_retryable),
      cmocka_unit_test(
          test_pouch_reconciled_terminal_release_failure_is_retryable),
      cmocka_unit_test(test_pouch_dispatcher_wait_includes_worker_release),
      cmocka_unit_test(test_pouch_reconciliation_preserves_allocator_domains),
      cmocka_unit_test(test_pouch_dispatcher_is_passive_until_consumer_demand),
      cmocka_unit_test(
          test_pouch_dispatcher_scan_recovery_when_indexing_is_disabled),
      cmocka_unit_test(test_pouch_client_close_from_source_callback),
      cmocka_unit_test(test_pouch_outbox_append_pins_close_callback_owner),
      cmocka_unit_test(
          test_pouch_outbox_transaction_append_pins_close_callback_owner),
      cmocka_unit_test(
          test_pouch_outbox_participant_update_pins_close_callback_owner),
      cmocka_unit_test(test_pouch_outbox_job_payload_pins_close_callback_owner),
      cmocka_unit_test(test_pouch_dispatcher_rejects_client_closed_replacement),
      cmocka_unit_test(
          test_pouch_dispatcher_stop_timeout_preserves_handed_out_job),
      cmocka_unit_test(test_pouch_dispatcher_stop_retains_inflight_receiver),
      cmocka_unit_test(test_pouch_shared_process_dispatches_once),
      cmocka_unit_test(test_pouch_shared_process_reconciles_each_outbox_once),
      cmocka_unit_test(test_outbox_renewal_uses_server_expiry),
      cmocka_unit_test(test_pouch_shared_renewal_publishes_recovery_deadline),
      cmocka_unit_test(
          test_pouch_failed_renew_does_not_publish_future_recovery_deadline),
      cmocka_unit_test(
          test_pouch_failed_renew_refreshes_recovery_from_server_expiry),
      cmocka_unit_test(test_pouch_expired_claim_rejects_stale_terminal),
      cmocka_unit_test(
          test_pouch_expired_claim_recovers_and_preserves_attempt_budget),
      cmocka_unit_test(
          test_pouch_outbox_dead_letters_persisted_exhausted_attempts),
      cmocka_unit_test(test_pouch_outbox_rejects_malformed_durable_outbox),
      cmocka_unit_test(test_outbox_rejects_oversized_outbox_envelope),
      cmocka_unit_test(test_outbox_rejects_oversized_receipt_metadata),
      cmocka_unit_test(test_outbox_remote_default_receipt_limit),
      cmocka_unit_test(test_outbox_rejects_oversized_terminal_receipt_metadata),
      cmocka_unit_test(test_pouch_participant_attach_refreshes_version),
      cmocka_unit_test(
          test_pouch_outbox_rejects_out_of_range_durable_attempt_counts),
      cmocka_unit_test(
          test_pouch_outbox_rejects_out_of_range_durable_replay_counts),
      cmocka_unit_test(test_pouch_outbox_validates_durable_input_contracts),
      cmocka_unit_test(test_pouch_dispatcher_stop_retains_blocked_next),
  };
  if (getenv("LOCKDC_TEST_ALLOCATOR_TRACKING") != NULL &&
      strcmp(getenv("LOCKDC_TEST_ALLOCATOR_TRACKING"), "1") == 0 &&
      lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(tests[0]), argc, argv,
                           NULL, NULL);
}
