#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#define FUZZ_OUTBOX_TMP_PREFIX "/tmp/liblockdc-outbox-fuzz-"
#define FUZZ_OUTBOX_CRYPTO_KEY                                                 \
  "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

static void outbox_fuzz_require(int rc, lc_error *error, const char *stage) {
  if (rc == LC_OK)
    return;
  (void)fprintf(stderr, "outbox dispatcher fuzz failed at %s: rc=%d %s\n",
                stage, rc, error->message == NULL ? "" : error->message);
  abort();
}

static void outbox_fuzz_run(const uint8_t *data, size_t size,
                            const char *crypto_key, const char *compression) {
  char template_path[] = "/tmp/liblockdc-outbox-fuzz-XXXXXX";
  char root[sizeof(template_path)];
  char endpoint[sizeof(root) + 32U];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_commit_result commit_result;
  lc_command_receipt command_receipt;
  lc_command_request command_request;
  lc_command_result command_result;
  lc_outbox_transaction *transaction;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_lease *state_lease;
  lc_source *payload;
  lc_source *checkpoint;
  lc_client *client;
  lc_error error;
  char command_id[48];
  unsigned int knobs;

  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                           FUZZ_OUTBOX_TMP_PREFIX))
    return;
  knobs = size == 0U ? 0U : (unsigned int)data[0];
  client = NULL;
  transaction = NULL;
  dispatcher = NULL;
  job = NULL;
  state_lease = NULL;
  payload = NULL;
  checkpoint = NULL;
  lc_error_init(&error);
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s/store", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "outbox-fuzz";
  client_config.pouch_crypto_key = crypto_key;
  client_config.pouch_compression = compression;
  outbox_fuzz_require(lc_client_open(&client_config, &client, &error), &error,
                      "open");
  lc_outbox_config_init(&outbox_config);
  outbox_config.ns = "outbox-fuzz";
  outbox_config.owner = "outbox-fuzz-owner";
  outbox_config.notification_capacity = 1U + (knobs & 3U);
  outbox_config.recovery_interval_seconds = 0L;
  /* Exercise the retention writer and the over-byte maintenance path without
   * adding a time-dependent wait to the mutation harness. */
  if ((knobs & 64U) != 0U)
    outbox_config.dead_letter_max_bytes = 1U;
  {
    lc_outbox *outbox = NULL;
    lc_outbox *attached_outbox = NULL;

    outbox_fuzz_require(
        lc_client_new_outbox(client, &outbox_config, &outbox, &error), &error,
        "new-outbox");
    outbox_fuzz_require(
        lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error), &error,
        "dispatcher");
    outbox_fuzz_require(
        lc_client_new_outbox_with_dispatcher(client, &outbox_config, dispatcher,
                                             &attached_outbox, &error),
        &error, "attach-producer");
    lc_outbox_entry_init(&entry);
    entry.operation_id = "outbox-fuzz-operation";
    entry.effect_id = "outbox-fuzz-effect";
    entry.effect_key = "outbox-fuzz-key";
    entry.payload_digest = "sha256:outbox-fuzz";
    entry.kind = "fuzz";
    entry.destination = "fuzz://outbox";
    entry.content_type = "text/plain";
    lc_outbox_receipt_init(&receipt);
    lc_command_receipt_init(&command_receipt);
    outbox_fuzz_require(lc_source_from_memory(data, size, &payload, &error),
                        &error, "payload");
    outbox_fuzz_require(lc_outbox_append(attached_outbox, &entry, payload,
                                         &transaction, &receipt, &error),
                        &error, "append");
    lc_source_close(payload);
    payload = NULL;
    if (receipt.outbox_key != NULL || receipt.duplicate)
      abort();
    lc_outbox_commit_result_init(&commit_result);
    outbox_fuzz_require(
        lc_outbox_transaction_commit(transaction, &commit_result, &error),
        &error, "commit");
    if (commit_result.outbox_receipt_count != 1U ||
        commit_result.outbox_receipts[0].outbox_key == NULL)
      abort();
    if ((knobs & 1U) != 0U) {
      outbox_fuzz_require(
          lc_outbox_dispatcher_notify_outbox_key(
              dispatcher, commit_result.outbox_receipts[0].outbox_key, &error),
          &error, "duplicate-notify");
    }
    if ((knobs & 2U) != 0U)
      outbox_fuzz_require(lc_outbox_dispatcher_reconcile(dispatcher, &error),
                          &error, "reconcile");
    lc_outbox_transaction_close(transaction);
    transaction = NULL;
    lc_outbox_commit_result_cleanup(&commit_result);
    lc_outbox_receipt_cleanup(&receipt);

    lc_command_request_init(&command_request);
    command_request.identity.scope = "outbox-fuzz";
    command_request.identity.command_type = "fuzz.command.v1";
    command_request.identity.idempotency_key = "outbox-fuzz-command";
    command_request.request_digest = "sha256:outbox-fuzz-command";
    outbox_fuzz_require(lc_outbox_accept_command(outbox, &command_request,
                                                 &transaction, &command_receipt,
                                                 &error),
                        &error, "accept-command");
    if (transaction == NULL || command_receipt.command_id == NULL)
      abort();
    if (snprintf(command_id, sizeof(command_id), "%s",
                 command_receipt.command_id) < 0 ||
        strlen(command_id) != strlen(command_receipt.command_id))
      abort();
    outbox_fuzz_require(
        lc_outbox_transaction_commit(transaction, &commit_result, &error),
        &error, "commit-command-pending");
    lc_outbox_commit_result_cleanup(&commit_result);
    lc_outbox_transaction_close(transaction);
    transaction = NULL;
    lc_command_receipt_cleanup(&command_receipt);
    lc_command_receipt_init(&command_receipt);
    if (lc_outbox_wait_command(outbox, command_id, 0L, &command_receipt,
                               &error) != LC_ERR_TIMEOUT ||
        command_receipt.state != LC_COMMAND_PENDING)
      abort();
    lc_error_cleanup(&error);
    lc_error_init(&error);
    lc_command_receipt_cleanup(&command_receipt);
    lc_command_receipt_init(&command_receipt);
    outbox_fuzz_require(
        lc_outbox_resume_command_by_id(outbox, command_id, &transaction,
                                       &command_receipt, &error),
        &error, "resume-command");
    if (transaction == NULL || command_receipt.state != LC_COMMAND_PENDING)
      abort();
    lc_command_result_init(&command_result);
    if ((knobs & 16U) != 0U) {
      command_result.failure_code = "fuzz-terminal";
      outbox_fuzz_require(lc_outbox_transaction_fail_command(
                              transaction, &command_result, &error),
                          &error, "fail-command");
    } else {
      command_result.result_code = "fuzz-terminal";
      outbox_fuzz_require(lc_outbox_transaction_complete_command(
                              transaction, &command_result, &error),
                          &error, "complete-command");
    }
    outbox_fuzz_require(
        lc_outbox_transaction_commit(transaction, &commit_result, &error),
        &error, "commit-command-terminal");
    lc_outbox_commit_result_cleanup(&commit_result);
    lc_outbox_transaction_close(transaction);
    transaction = NULL;
    lc_command_receipt_cleanup(&command_receipt);
    lc_command_receipt_init(&command_receipt);
    outbox_fuzz_require(lc_outbox_wait_command(outbox, command_id, 0L,
                                               &command_receipt, &error),
                        &error, "wait-command-terminal");
    if (command_receipt.state !=
        (((knobs & 16U) != 0U) ? LC_COMMAND_FAILED : LC_COMMAND_COMPLETED))
      abort();
    lc_command_receipt_cleanup(&command_receipt);
    if ((knobs & 32U) != 0U) {
      outbox_fuzz_require(
          lc_outbox_dispatcher_next_with_state(dispatcher, 5000L, &job, &error),
          &error, "next-with-state");
      if (job == NULL)
        abort();
      state_lease = lc_outbox_job_state(job);
      if (state_lease == NULL)
        abort();
      outbox_fuzz_require(
          lc_source_from_memory("checkpoint", 10U, &checkpoint, &error), &error,
          "state-checkpoint-source");
      outbox_fuzz_require(
          lc_lease_update(state_lease, checkpoint, NULL, &error), &error,
          "state-checkpoint-update");
      lc_source_close(checkpoint);
      checkpoint = NULL;
    } else {
      outbox_fuzz_require(
          lc_outbox_dispatcher_next(dispatcher, 5000L, &job, &error), &error,
          "next");
    }
    if (job == NULL)
      abort();
    if ((knobs & 12U) == 4U) {
      lc_outbox_retry retry;

      lc_outbox_retry_init(&retry);
      retry.diagnostic = "fuzz retry";
      outbox_fuzz_require(lc_outbox_job_retry(job, &retry, &error), &error,
                          "retry");
    } else if ((knobs & 12U) == 8U) {
      outbox_fuzz_require(
          lc_outbox_job_dead_letter(job, "fuzz dead letter", &error), &error,
          "dead-letter");
    } else {
      outbox_fuzz_require(lc_outbox_job_complete(job, NULL, &error), &error,
                          "complete");
    }
    job = NULL;
    outbox_fuzz_require(lc_outbox_dispatcher_stop(dispatcher, -1L, &error),
                        &error, "stop");
    lc_outbox_dispatcher_close(dispatcher);
    dispatcher = NULL;
    lc_outbox_close(attached_outbox);
    lc_outbox_close(outbox);
  }
  if (checkpoint != NULL)
    lc_source_close(checkpoint);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, FUZZ_OUTBOX_TMP_PREFIX);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  outbox_fuzz_run(data, size, NULL, NULL);
  outbox_fuzz_run(data, size, FUZZ_OUTBOX_CRYPTO_KEY, NULL);
  outbox_fuzz_run(data, size, NULL, "zlib");
  outbox_fuzz_run(data, size, FUZZ_OUTBOX_CRYPTO_KEY, "zlib");
  return 0;
}
