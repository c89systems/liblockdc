#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cmocka.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_retention.h"
#include "lc_pouch_storage.h"

static int check_visited_retention(const lc_pouch_state_visit_entry *entry,
                                   void *context, lc_error *error) {
  int *seen = context;
  (void)error;
  if (strcmp(entry->key, "order") == 0) {
    assert_int_equal(entry->retention, INT64_C(2000000100));
    ++*seen;
  }
  return LC_OK;
}

static void exercise_storage(int shared, const char *compression,
                             int encrypted) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  lc_pouch *pouch = NULL;
  lc_pouch_open_options open;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result written;
  lc_pouch_state_read_result read;
  lc_pouch_maintenance_options maintenance;
  lc_pouch_maintenance_result maintained;
  lc_source *body = NULL;
  char *crypto_key = NULL;
  lc_error error;
  int pass;
  int seen;

  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  memset(&open, 0, sizeof(open));
  memset(&options, 0, sizeof(options));
  memset(&written, 0, sizeof(written));
  memset(&read, 0, sizeof(read));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(&maintained, 0, sizeof(maintained));
  lc_error_init(&error);
  if (encrypted) {
    assert_int_equal(lc_pouch_crypto_generate_key_string(&crypto_key, &error),
                     LC_OK);
    open.crypto_key = crypto_key;
  }
  open.background_compaction_enabled_set = 1;
  open.background_compaction_enabled = 0;
  open.query_indexing_enabled_set = 1;
  open.query_indexing_enabled = 0;
  open.segment_target_bytes = 1U;
  open.compression = compression;
  open.single_writer_set = 1;
  open.single_writer = !shared;
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  options.has_metadata = 1;
  options.metadata = (const unsigned char *)"lease-data";
  options.metadata_length = 10;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders",
                                                  "placeholder", &options,
                                                  &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  memset(&options, 0, sizeof(options));
  options.has_retention = 1;
  options.retention = INT64_C(2000000100);
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders",
                                                  "placeholder", &options,
                                                  &written, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  options.has_retention = 1;
  options.retention = INT64_C(2000000100);
  assert_int_equal(lc_source_from_memory("{\"v\":1}", 7U, &body, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body,
                                        &options, &written, &error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  for (pass = 0; pass < 3; ++pass) {
    assert_int_equal(
        lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
        LC_OK);
    assert_int_equal(read.retention, INT64_C(2000000100));
    lc_pouch_state_read_result_cleanup(NULL, &read);
    seen = 0;
    assert_int_equal(lc_pouch_state_visit(pouch, "orders",
                                          check_visited_retention, &seen,
                                          &error),
                     LC_OK);
    assert_int_equal(seen, 1);
    /* Body replacement and unrelated lease metadata must both preserve the
     * policy without copying it out into a separate control record. */
    memset(&options, 0, sizeof(options));
    assert_int_equal(lc_source_from_memory("{\"v\":2}", 7U, &body, &error),
                     LC_OK);
    assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body,
                                          &options, &written, &error),
                     LC_OK);
    lc_source_close(body);
    assert_int_equal(written.retention, INT64_C(2000000100));
    lc_pouch_state_write_result_cleanup(NULL, &written);
    memset(&options, 0, sizeof(options));
    options.has_metadata = 1;
    options.metadata = (const unsigned char *)"lease-data";
    options.metadata_length = 10U;
    assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                    &options, &written, &error),
                     LC_OK);
    assert_int_equal(written.retention, INT64_C(2000000100));
    lc_pouch_state_write_result_cleanup(NULL, &written);
    options.has_expected_version = 1;
    options.expected_version = 0;
    options.has_retention = 1;
    options.retention = 0;
    assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                    &options, &written, &error),
                     LC_ERR_INVALID);
    lc_error_cleanup(&error);
    if (pass == 1) {
      maintenance.ns = "orders";
      maintenance.force = 1;
      assert_int_equal(
          lc_pouch_maintenance_run(pouch, &maintenance, &maintained, &error),
          LC_OK);
      assert_true(maintained.compacted);
      lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    }
    lc_pouch_close(pouch);
    pouch = NULL;
    assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  }
  memset(&options, 0, sizeof(options));
  options.has_retention = 1;
  options.retention = 0;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(
      lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
      LC_OK);
  assert_int_equal(read.retention, 0);
  assert_int_equal(read.metadata_length, 10U);
  assert_memory_equal(read.metadata, "lease-data", 10U);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  /* Removing the object ends its policy; recreating its key starts fresh. */
  memset(&options, 0, sizeof(options));
  options.has_retention = 1;
  options.retention = INT64_C(2000000200);
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(
      lc_pouch_state_delete(pouch, "orders", "order", NULL, &written, &error),
      LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(lc_source_from_memory("{}", 2U, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body, NULL,
                                        &written, &error),
                   LC_OK);
  lc_source_close(body);
  assert_int_equal(written.retention, 0);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  lc_pouch_close(pouch);
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

static void test_exclusive_retention_storage(void **state) {
  (void)state;
  exercise_storage(0, NULL, 0);
}

static void test_shared_retention_storage(void **state) {
  (void)state;
  exercise_storage(1, NULL, 0);
}

static void test_compressed_retention_storage(void **state) {
  (void)state;
  exercise_storage(0, "zlib", 0);
}

static void test_encrypted_shared_retention_storage(void **state) {
  (void)state;
  exercise_storage(1, "zlib", 1);
}

typedef struct retention_clock {
  lc_pouch_unix_seconds now;
  int failed;
  int streamed;
} retention_clock;

static int retention_clock_gettime(clockid_t clock_id, struct timespec *out,
                                   void *context) {
  retention_clock *clock = context;
  (void)clock_id;
  if (clock->failed) {
    errno = EIO;
    return -1;
  }
  out->tv_sec = (time_t)clock->now;
  out->tv_nsec = 0;
  return 0;
}

static size_t retention_stream_read(void *context, void *dst, size_t capacity,
                                    lc_error *error) {
  retention_clock *clock = context;
  (void)error;
  if (!clock->streamed && capacity >= 2) {
    memcpy(dst, "{}", 2);
    clock->streamed = 1;
    clock->now = 2000;
    return 2;
  }
  return 0;
}

static int reset_retention_clock(void **state) {
  (void)state;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_pouch_test_after_txn_decision_hook = NULL;
  lc_pouch_test_after_txn_decision_context = NULL;
  lc_pouch_test_before_txn_replay_hook = NULL;
  lc_pouch_test_before_txn_replay_context = NULL;
  lc_pouch_test_cleanup_fsync_hook = NULL;
  lc_pouch_test_cleanup_fsync_context = NULL;
  lc_pouch_test_retention_hook = NULL;
  lc_pouch_test_retention_context = NULL;
  return 0;
}

static int lose_decision_ack(void *context, lc_error *error) {
  (void)context;
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "injected lost commit acknowledgement", NULL, NULL,
                      "test");
}

static void exercise_coordinated_clock(int mode) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  char endpoint[512];
  char xid[LC_XID_STRING_SIZE];
  const char *endpoints[] = {endpoint};
  lc_client_config config;
  lc_client *client = NULL;
  lc_pouch *pouch;
  lc_lease *leases[2] = {NULL, NULL};
  lc_acquire_req acquire;
  lc_txn_participant participants[2] = {{"orders", "a", NULL},
                                        {"orders", "b", NULL}};
  lc_txn_decision_req decision;
  lc_txn_decision_res decided = {0};
  lc_txn_replay_req replay;
  lc_txn_replay_res replayed = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_read_result read = {0};
  lc_error error;
  lc_source *body = NULL;
  retention_clock clock = {1000, 0, 0};
  int i;

  lc_error_init(&error);
  lc_pouch_test_queue_clock_gettime = retention_clock_gettime;
  lc_pouch_test_queue_time_context = &clock;
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://"
                       "%s?background_compaction=false&query_indexing=false&"
                       "single_writer=%s",
                       root, mode == 5 ? "false" : "true") > 0);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  assert_int_equal(lc_pouch_legacy_client_open(&config, &client, &error),
                   LC_OK);
  pouch = ((lc_client_handle *)client)->pouch;
  assert_int_equal(lc_xid_new(xid, &error), LC_OK);
  lc_txn_decision_req_init(&decision);
  decision.txn_id = xid;
  decision.participants = participants;
  decision.participant_count = 2;
  assert_int_equal(client->txn_prepare(client, &decision, &decided, &error),
                   LC_OK);
  lc_txn_decision_res_cleanup(&decided);
  for (i = 0; i < 2; ++i) {
    lc_acquire_req_init(&acquire);
    acquire.ns = "orders";
    acquire.key = participants[i].key;
    acquire.owner = "retention-test";
    acquire.ttl_seconds = 100000;
    acquire.txn_id = xid;
    assert_int_equal(client->acquire(client, &acquire, &leases[i], &error),
                     LC_OK);
    options.has_retention = 1;
    options.retention = mode == 3 && i == 0 ? 10000 : -60 * (i + 1);
    /* Overflow only the second participant without requiring a clock value
     * outside a 32-bit time_t. Durable staged durations are always int64. */
    if (mode == 2 && i == 1)
      options.retention = -(LC_I64_MAX - 2999);
    assert_int_equal(lc_source_from_memory("{}", 2U, &body, &error), LC_OK);
    assert_int_equal(lc_pouch_state_stage_write(pouch, "orders",
                                                participants[i].key, xid, body,
                                                &options, &written, &error),
                     LC_OK);
    lc_source_close(body);
    lc_pouch_state_write_result_cleanup(NULL, &written);
  }
  clock.now = 3000;
  if (mode == 4) {
    clock.now = 2000;
    assert_int_equal(leases[0]->release(leases[0], NULL, &error), LC_OK);
    leases[0] = NULL;
    clock.now = 3000;
    assert_int_equal(leases[1]->release(leases[1], NULL, &error), LC_OK);
    leases[1] = NULL;
  } else {
    lc_pouch_test_after_txn_decision_hook = lose_decision_ack;
    assert_int_equal(client->txn_commit(client, &decision, &decided, &error),
                     mode == 2 ? LC_ERR_INVALID : LC_ERR_TRANSPORT);
  }
  lc_error_cleanup(&error);
  lc_txn_decision_res_cleanup(&decided);
  lc_pouch_test_after_txn_decision_hook = NULL;
  for (i = 0; i < 2; ++i) {
    assert_int_equal(lc_pouch_state_read_metadata(
                         pouch, "orders", participants[i].key, &read, &error),
                     LC_OK);
    assert_int_equal(read.has_body, mode == 4);
    lc_pouch_state_read_result_cleanup(NULL, &read);
    if (leases[i] != NULL)
      leases[i]->close(leases[i]);
  }
  lc_txn_replay_req_init(&replay);
  replay.txn_id = xid;
  if (mode == 3) {
    lc_sink *journal = NULL;
    const void *bytes;
    size_t length;
    lc_pouch_state_write_options journal_options = {0};
    journal_options.object_record = 1;
    journal_options.content_type = "application/x-lockdc-pouch-txn";
    assert_int_equal(lc_pouch_state_read(pouch, ".txns", xid, &read, &error),
                     LC_OK);
    assert_int_equal(lc_sink_to_memory(&journal, &error), LC_OK);
    assert_int_equal(lc_copy(read.body, journal, NULL, &error), LC_OK);
    assert_int_equal(lc_sink_memory_bytes(journal, &bytes, &length, &error),
                     LC_OK);
    lc_pouch_state_read_result_cleanup(NULL, &read);
    assert_true(length > 8);
    /* An old timestamp-free COMMIT with newly staged retention is inconsistent:
     * recovery must preserve it and fail, rather than grant fresh retention. */
    assert_int_equal(lc_source_from_memory(bytes, length - 8, &body, &error),
                     LC_OK);
    assert_int_equal(lc_pouch_state_write(pouch, ".txns", xid, body,
                                          &journal_options, &written, &error),
                     LC_OK);
    lc_source_close(body);
    lc_pouch_state_write_result_cleanup(NULL, &written);
    clock.now = 9000;
    assert_int_equal(client->txn_replay(client, &replay, &replayed, &error),
                     LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
    lc_txn_replay_res_cleanup(&replayed);
    for (i = 0; i < 2; ++i) {
      assert_int_equal(lc_pouch_state_read_metadata(
                           pouch, "orders", participants[i].key, &read, &error),
                       LC_OK);
      assert_false(read.has_body);
      lc_pouch_state_read_result_cleanup(NULL, &read);
    }
    assert_int_equal(lc_source_from_memory(bytes, length, &body, &error),
                     LC_OK);
    assert_int_equal(lc_pouch_state_write(pouch, ".txns", xid, body,
                                          &journal_options, &written, &error),
                     LC_OK);
    lc_source_close(body);
    lc_pouch_state_write_result_cleanup(NULL, &written);
    lc_sink_close(journal);
  }
  if (mode == 2) {
    /* The first duration fits, but the second overflows. No participant may
     * publish and the journal must still allow an ordinary rollback. */
    clock.now = 1000;
    assert_int_equal(client->txn_replay(client, &replay, &replayed, &error),
                     LC_OK);
    assert_string_equal(replayed.state, "prepare");
    lc_txn_replay_res_cleanup(&replayed);
    assert_int_equal(client->txn_rollback(client, &decision, &decided, &error),
                     LC_OK);
    lc_txn_decision_res_cleanup(&decided);
  } else {
    lc_client_close(client);
    clock.now = mode == 1 ? 2000 : mode == 6 ? 200000 : 9000;
    assert_int_equal(lc_pouch_legacy_client_open(&config, &client, &error),
                     LC_OK);
    pouch = ((lc_client_handle *)client)->pouch;
    for (i = 0; i < 2; ++i) {
      assert_int_equal(lc_pouch_state_read_metadata(
                           pouch, "orders", participants[i].key, &read, &error),
                       LC_OK);
      assert_true(read.has_body);
      assert_int_equal(read.retention,
                       mode == 3 && i == 0 ? 10000 : 3000 + 60 * (i + 1));
      lc_pouch_state_read_result_cleanup(NULL, &read);
    }
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

static void test_coordinated_retention_delayed_recovery(void **state) {
  (void)state;
  exercise_coordinated_clock(0);
}

static void test_coordinated_retention_clock_rollback(void **state) {
  (void)state;
  exercise_coordinated_clock(1);
}

static void test_coordinated_retention_checks_every_duration(void **state) {
  (void)state;
  exercise_coordinated_clock(2);
}

static void test_coordinated_retention_missing_commit_time(void **state) {
  (void)state;
  exercise_coordinated_clock(3);
}

static void test_coordinated_retention_commit_votes(void **state) {
  (void)state;
  exercise_coordinated_clock(4);
}

static void test_coordinated_retention_shared_recovery(void **state) {
  (void)state;
  exercise_coordinated_clock(5);
}

static void test_coordinated_retention_expired_lease_recovery(void **state) {
  (void)state;
  exercise_coordinated_clock(6);
}

static void test_retention_commit_clock_and_staging(void **state) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  lc_pouch *pouch = NULL;
  lc_pouch_open_options open = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_read_result read = {0};
  lc_source *body = NULL;
  lc_error error;
  retention_clock clock = {1000, 0, 0};
  lc_pouch_unix_seconds resolved;
  int discarded;
  (void)state;
  lc_error_init(&error);
  lc_pouch_test_queue_clock_gettime = retention_clock_gettime;
  lc_pouch_test_queue_time_context = &clock;
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  open.background_compaction_enabled_set = 1;
  open.query_indexing_enabled_set = 1;
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  options.has_retention = 1;
  options.retention = -60;
  assert_int_equal(lc_source_from_callbacks(retention_stream_read, NULL, NULL,
                                            &clock, &body, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body,
                                        &options, &written, &error),
                   LC_OK);
  lc_source_close(body);
  /* A slow streaming producer must not consume its own retention period. */
  assert_int_equal(written.retention, 2060);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  clock.now = 3000;
  options.retention = -120;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_OK);
  assert_int_equal(written.retention, 3120);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  options.retention = -LC_I64_MAX;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  clock.failed = 1;
  options.retention = -60;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  clock.failed = 0;
  assert_int_equal(
      lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
      LC_OK);
  assert_int_equal(read.retention, 3120);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  assert_int_equal(lc_source_from_memory("{}", 2U, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_stage_write(pouch, "orders", "order",
                                              "pending", body, &options,
                                              &written, &error),
                   LC_OK);
  lc_source_close(body);
  assert_int_equal(written.retention, -60);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  clock.now = 4000;
  assert_int_equal(lc_pouch_state_commit_staged(pouch, "orders", "order",
                                                "pending", &written, &error),
                   LC_OK);
  assert_int_equal(written.retention, 4060);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  clock.now = 9000;
  assert_int_equal(lc_pouch_state_commit_staged(pouch, "orders", "order",
                                                "pending", &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(
      lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
      LC_OK);
  assert_int_equal(read.retention, 4060);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  assert_int_equal(lc_source_from_memory("{}", 2U, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_stage_write(pouch, "orders", "order", "abort",
                                              body, &options, &written, &error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(lc_pouch_state_discard_staged(pouch, "orders", "order",
                                                 "abort", &discarded, &error),
                   LC_OK);
  assert_true(discarded);
  assert_int_equal(
      lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
      LC_OK);
  assert_int_equal(read.retention, 4060);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  assert_int_equal(
      lc_pouch_retention_resolve(-LC_I64_MAX, 0, &resolved, &error), LC_OK);
  assert_int_equal(resolved, LC_I64_MAX);
  assert_int_equal(lc_pouch_retention_resolve(LC_I64_MIN, 0, &resolved, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_pouch_close(pouch);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

static int observe_cleanup_fsync(void *context, lc_error *error) {
  int *calls = context;
  (void)error;
  ++*calls;
  return LC_OK;
}

typedef struct retention_crash_image {
  char path[512];
  off_t durable_length;
  int rc;
  int hint_present;
  int cut;
  unsigned int syncs;
} retention_crash_image;

typedef struct retention_sync_watch {
  retention_crash_image image;
  lc_pouch *pouch;
  lc_pouch *peer;
  dev_t device;
  ino_t inode;
  int delete_last;
  int fail_sync;
  int inject_failure;
  int output_fd;
} retention_sync_watch;

static retention_sync_watch *retention_watch;
int __real_fsync(int fd);
int __wrap_fsync(int fd) {
  struct stat st;
  int watched = retention_watch != NULL && fstat(fd, &st) == 0 &&
                st.st_dev == retention_watch->device &&
                st.st_ino == retention_watch->inode;
  int rc;
  if (watched && retention_watch->fail_sync) {
    errno = EIO;
    return -1;
  }
  rc = __real_fsync(fd);
  if (watched && rc == 0) {
    retention_watch->image.durable_length = st.st_size;
    ++retention_watch->image.syncs;
  }
  return rc;
}

static int retention_watch_active(retention_sync_watch *watch,
                                  lc_error *error) {
  lc_pouch_namespace_manifest manifest = {0};
  struct stat st;
  int length;
  int rc = lc_pouch_namespace_manifest_open(
      NULL, watch->pouch->root_path, "orders", &manifest, NULL, NULL, error);
  if (rc != LC_OK)
    return rc;
  length =
      snprintf(watch->image.path, sizeof(watch->image.path), "%s/segments/%s",
               manifest.namespace_path, manifest.active_segment);
  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  if (length < 0 || (size_t)length >= sizeof(watch->image.path) ||
      stat(watch->image.path, &st) != 0)
    return LC_ERR_TRANSPORT;
  if (st.st_dev != watch->device || st.st_ino != watch->inode)
    watch->image.durable_length = 0;
  watch->device = st.st_dev;
  watch->inode = st.st_ino;
  return LC_OK;
}

static int retention_seed(lc_pouch *pouch, const char *key,
                          lc_pouch_unix_seconds deadline, lc_error *error) {
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_source *body = NULL;
  int rc = lc_source_from_memory("{}", 2, &body, error);
  options.has_retention = 1;
  options.retention = deadline;
  if (rc == LC_OK)
    rc = lc_pouch_state_write(pouch, "orders", key, body, &options, &written,
                              error);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  return rc;
}

static int retention_clear_last(retention_sync_watch *watch, lc_error *error) {
  lc_pouch *pouch = watch->peer != NULL ? watch->peer : watch->pouch;
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  int rc;
  options.has_retention = 1;
  if (watch->delete_last)
    rc = lc_pouch_state_delete(pouch, "orders", "last", &options, &written,
                               error);
  else
    rc = lc_pouch_state_update_metadata(pouch, "orders", "last", &options,
                                        &written, error);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  return rc;
}

static void retention_report_crash(retention_sync_watch *watch, int rc,
                                   int cut) {
  lc_pouch_retention_work work = {0};
  lc_error error;
  lc_error_init(&error);
  watch->image.rc = rc;
  watch->image.cut = cut;
  if (lc_pouch_retention_work_read(watch->pouch, "orders", &work, &error) !=
      LC_OK)
    _exit(4);
  watch->image.hint_present = work.present;
  if (write(watch->output_fd, &watch->image, sizeof(watch->image)) !=
      (ssize_t)sizeof(watch->image))
    _exit(5);
  /* No close/checkpoint: the parent keeps only bytes covered by real fsync. */
  _exit(0);
}

static int retention_idle_crash(int phase, void *context, lc_error *error) {
  retention_sync_watch *watch = context;
  int rc = LC_OK;
  if (phase == LC_POUCH_RETENTION_JOURNAL_RETIRED) {
    /* This handle has an independent NoSync commit group. Its write is not
     * in the cleanup's deferred descriptors, despite sharing the namespace.
     * Same-thread interleaving respects the shared-root transaction guard. */
    rc = retention_clear_last(watch, error);
    watch->fail_sync = watch->inject_failure;
  }
  if (phase == LC_POUCH_RETENTION_REF_CLEARED)
    retention_report_crash(watch, LC_OK, 1);
  return rc;
}

static void retention_crash_child(const char *root, int delete_last, int rotate,
                                  int idle, int fail_sync, int output_fd) {
  retention_sync_watch watch = {0};
  lc_pouch_open_options options = {0};
  lc_pouch_state_commit_group *scope = NULL;
  lc_error error;
  struct stat st;
  int rc;
  lc_error_init(&error);
  options.single_writer_set = 1;
  options.single_writer = 0;
  options.background_compaction_enabled_set = 1;
  options.query_indexing_enabled_set = 1;
  options.segment_target_bytes = rotate ? 1 : 1048576;
  watch.delete_last = delete_last;
  watch.inject_failure = fail_sync;
  watch.output_fd = output_fd;
  rc = lc_pouch_open(root, NULL, &options, &watch.pouch, &error);
  if (rc == LC_OK)
    rc = lc_pouch_state_durable_scope_begin(watch.pouch, &scope, &error);
  if (rc == LC_OK)
    rc = retention_seed(watch.pouch, "last", INT64_C(2000000100), &error);
  if (rc == LC_OK && idle)
    rc = retention_seed(watch.pouch, "due", 1, &error);
  rc = lc_pouch_state_durable_scope_end(scope, rc, &error);
  if (rc == LC_OK && idle)
    rc = lc_pouch_open(root, NULL, &options, &watch.peer, &error);
  if (rc == LC_OK)
    rc = retention_watch_active(&watch, &error);
  if (rc != LC_OK || stat(watch.image.path, &st) != 0)
    _exit(2);
  watch.image.durable_length = st.st_size;
  retention_watch = &watch;
  if (idle) {
    lc_pouch_test_retention_hook = retention_idle_crash;
    lc_pouch_test_retention_context = &watch;
  } else {
    rc = retention_clear_last(&watch, &error);
    if (rc == LC_OK)
      rc = retention_watch_active(&watch, &error);
    if (rc != LC_OK || (!rotate && watch.image.syncs != 0))
      _exit(3);
    watch.image.syncs = 0;
    watch.fail_sync = fail_sync;
  }
  rc = lc_pouch_retention_run(watch.pouch, "orders", 16, NULL, &error);
  retention_report_crash(&watch, rc, 0);
}

static void exercise_retention_retirement_crash(int idle, int fail_sync) {
  int deleted, rotated;
  for (deleted = 0; deleted <= 1; ++deleted) {
    for (rotated = 0; rotated <= !idle; ++rotated) {
      char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
      char root[sizeof(template_path)];
      retention_crash_image image = {0};
      lc_pouch_open_options options = {0};
      lc_pouch_state_read_result record = {0};
      lc_pouch *pouch = NULL;
      retention_clock clock = {0};
      lc_error error;
      int pipes[2], status, fd, pass;
      pid_t child;
      lc_error_init(&error);
      clock.now = INT64_C(2000000000);
      lc_pouch_test_queue_clock_gettime = retention_clock_gettime;
      lc_pouch_test_queue_time_context = &clock;
      assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                      "/tmp/liblockdc-retention-"));
      assert_int_equal(pipe(pipes), 0);
      child = fork();
      assert_true(child >= 0);
      if (child == 0) {
        close(pipes[0]);
        retention_crash_child(root, deleted, rotated, idle, fail_sync,
                              pipes[1]);
        _exit(6);
      }
      close(pipes[1]);
      assert_int_equal(read(pipes[0], &image, sizeof(image)), sizeof(image));
      close(pipes[0]);
      assert_int_equal(waitpid(child, &status, 0), child);
      assert_true(WIFEXITED(status));
      assert_int_equal(WEXITSTATUS(status), 0);
      assert_int_equal(image.rc, fail_sync ? LC_ERR_TRANSPORT : LC_OK);
      assert_int_equal(image.hint_present, fail_sync);
      assert_int_equal(image.cut, idle && !fail_sync);
      fd = open(image.path, O_RDWR);
      assert_true(fd >= 0);
      assert_int_equal(ftruncate(fd, image.durable_length), 0);
      assert_int_equal(fsync(fd), 0);
      close(fd);
      options.single_writer_set = 1;
      options.background_compaction_enabled_set = 1;
      options.background_compaction_enabled = fail_sync;
      options.compaction_interval_seconds = 3600;
      options.query_indexing_enabled_set = 1;
      clock.now += 200;
      assert_int_equal(lc_pouch_open(root, NULL, &options, &pouch, &error),
                       LC_OK);
      assert_int_equal(lc_pouch_state_read_metadata(pouch, "orders", "last",
                                                    &record, &error),
                       LC_OK);
      assert_int_equal(record.has_body, fail_sync || !deleted);
      if (record.has_body)
        assert_int_equal(record.retention, fail_sync ? INT64_C(2000000100) : 0);
      lc_pouch_state_read_result_cleanup(NULL, &record);
      if (!fail_sync)
        assert_true(image.syncs > 0);
      if (fail_sync) {
        /* Recovery must find the expired body through the root's ordinary
         * discovery/scheduler, without another write or explicit namespace. */
        for (pass = 0; pass < 8; ++pass)
          lc_pouch_test_compaction_run_pass(pouch);
        assert_int_equal(lc_pouch_state_read_metadata(pouch, "orders", "last",
                                                      &record, &error),
                         LC_OK);
        assert_false(record.has_body);
        lc_pouch_state_read_result_cleanup(NULL, &record);
      }
      lc_pouch_close(pouch);
      lc_error_cleanup(&error);
      lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
    }
  }
}

static void
test_retention_hint_retirement_survives_nosync_power_loss(void **state) {
  (void)state;
  exercise_retention_retirement_crash(0, 0);
}
static void
test_retention_idle_retirement_survives_nosync_power_loss(void **state) {
  (void)state;
  exercise_retention_retirement_crash(1, 0);
}
static void test_retention_hint_sync_failure_preserves_discovery(void **state) {
  (void)state;
  exercise_retention_retirement_crash(0, 1);
}
static void test_retention_idle_sync_failure_preserves_discovery(void **state) {
  (void)state;
  exercise_retention_retirement_crash(1, 1);
}

static void test_cleanup_durability_on_nosync_root(void **state) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  lc_pouch *pouch = NULL;
  lc_pouch_open_options open = {0};
  lc_pouch_state_commit_group *scope = NULL;
  lc_pouch_state_commit_group *nested = NULL;
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_read_result read = {0};
  lc_source *body = NULL;
  lc_error error;
  int calls = 0;
  (void)state;
  lc_error_init(&error);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  open.background_compaction_enabled_set = 1;
  open.query_indexing_enabled_set = 1;
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  lc_pouch_test_cleanup_fsync_hook = observe_cleanup_fsync;
  lc_pouch_test_cleanup_fsync_context = &calls;
  assert_int_equal(lc_pouch_state_durable_scope_begin(pouch, &scope, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_state_durable_scope_begin(pouch, &nested, &error),
                   LC_ERR_INVALID);
  assert_null(nested);
  lc_error_cleanup(&error);
  assert_int_equal(lc_source_from_memory("{}", 2, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body, NULL,
                                        &written, &error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(lc_pouch_state_durable_scope_barrier(scope, &error), LC_OK);
  assert_true(calls > 0);
  calls = 0;
  assert_int_equal(lc_pouch_state_durable_scope_barrier(scope, &error), LC_OK);
  assert_int_equal(calls, 0);
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "order",
                                                  &options, &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(lc_pouch_state_durable_scope_barrier(scope, &error), LC_OK);
  assert_true(calls > 0);
  calls = 0;
  assert_int_equal(lc_source_from_memory("{\"v\":2}", 7, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_write(pouch, "orders", "order", body, NULL,
                                        &written, &error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  lc_pouch_test_cleanup_fsync_hook = lose_decision_ack;
  assert_int_equal(lc_pouch_state_durable_scope_barrier(scope, &error),
                   LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  lc_pouch_test_cleanup_fsync_hook = observe_cleanup_fsync;
  assert_int_equal(
      lc_pouch_state_durable_scope_end(scope, LC_ERR_TRANSPORT, &error),
      LC_ERR_TRANSPORT);
  assert_true(calls > 0);
  assert_int_equal(lc_pouch_state_durable_scope_begin(pouch, &scope, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_state_durable_scope_end(scope, LC_OK, &error),
                   LC_OK);
  lc_pouch_close(pouch);
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  assert_int_equal(
      lc_pouch_state_read_metadata(pouch, "orders", "order", &read, &error),
      LC_OK);
  assert_true(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_pouch_close(pouch);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

static void test_retention_hint_lifecycle_and_discovery(void **state) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  char xid[LC_XID_STRING_SIZE];
  char path[512];
  char *ns = NULL;
  char *cursor = NULL;
  char *next_ns = NULL;
  char *next_cursor = NULL;
  lc_pouch *pouch = NULL;
  lc_pouch_open_options open = {0};
  lc_pouch_retention_work work = {0};
  lc_error error;
  struct stat before;
  struct stat after;
  (void)state;
  lc_error_init(&error);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  open.background_compaction_enabled_set = 1;
  open.query_indexing_enabled_set = 1;
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  assert_int_equal(
      lc_pouch_retention_hint_next(pouch, NULL, &ns, &cursor, &error), LC_OK);
  assert_null(ns);
  assert_null(cursor);
  assert_int_equal(lc_pouch_retention_hint_ensure(pouch, "orders/eu", &error),
                   LC_OK);
  assert_int_equal(
      lc_pouch_retention_work_read(pouch, "orders/eu", &work, &error), LC_OK);
  assert_true(work.present);
  assert_int_equal(work.phase, LC_POUCH_RETENTION_IDLE);
  lc_pouch_retention_work_cleanup(pouch, &work);
  assert_int_equal(lc_xid_new(xid, &error), LC_OK);
  assert_int_equal(lc_pouch_retention_work_write(pouch, "orders/eu",
                                                 LC_POUCH_RETENTION_ACTIVE, xid,
                                                 "order/\n%", 321, 99, &error),
                   LC_OK);
  assert_int_equal(
      lc_pouch_retention_hint_next(pouch, NULL, &ns, &cursor, &error), LC_OK);
  assert_string_equal(ns, "orders/eu");
  assert_true(snprintf(path, sizeof(path), "%s/.lockd/state-retention/%s/work",
                       root, cursor) > 0);
  assert_int_equal(stat(path, &before), 0);
  assert_int_equal(lc_pouch_retention_hint_ensure(pouch, "orders/eu", &error),
                   LC_OK);
  assert_int_equal(stat(path, &after), 0);
  assert_int_equal(before.st_ino, after.st_ino);
  assert_int_equal(
      lc_pouch_retention_work_read(pouch, "orders/eu", &work, &error), LC_OK);
  assert_int_equal(work.phase, LC_POUCH_RETENTION_ACTIVE);
  assert_string_equal(work.txn_id, xid);
  assert_string_equal(work.key, "order/\n%");
  assert_int_equal(work.fencing_token, 321);
  assert_int_equal(work.state_version, 99);
  lc_pouch_retention_work_cleanup(pouch, &work);
  assert_int_equal(lc_pouch_retention_hint_ensure(pouch, "orders/us", &error),
                   LC_OK);
  assert_int_equal(
      lc_pouch_ensure_namespace(pouch, "unrelated/namespace", &error), LC_OK);
  assert_int_equal(lc_pouch_retention_hint_next(pouch, cursor, &next_ns,
                                                &next_cursor, &error),
                   LC_OK);
  assert_string_equal(next_ns, "orders/us");
  lc_free_with_allocator(NULL, ns);
  lc_free_with_allocator(NULL, cursor);
  ns = NULL;
  cursor = NULL;
  assert_int_equal(
      lc_pouch_retention_hint_next(pouch, next_cursor, &ns, &cursor, &error),
      LC_OK);
  assert_string_equal(ns, "orders/eu");
  lc_free_with_allocator(NULL, ns);
  lc_free_with_allocator(NULL, cursor);
  lc_free_with_allocator(NULL, next_ns);
  lc_free_with_allocator(NULL, next_cursor);
  assert_int_equal(lc_pouch_retention_work_write(
                       pouch, "orders/eu", LC_POUCH_RETENTION_FINISHED, xid,
                       "order/\n%", 321, 99, &error),
                   LC_OK);
  lc_pouch_close(pouch);
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  assert_int_equal(
      lc_pouch_retention_work_read(pouch, "orders/eu", &work, &error), LC_OK);
  assert_int_equal(work.phase, LC_POUCH_RETENTION_FINISHED);
  assert_string_equal(work.txn_id, xid);
  assert_int_equal(work.fencing_token, 321);
  assert_int_equal(work.state_version, 99);
  lc_pouch_retention_work_cleanup(pouch, &work);
  assert_int_equal(
      lc_pouch_path_write_text_file(path, "LPR1\n1\ncorrupt\nkey\n", &error),
      LC_OK);
  assert_int_equal(
      lc_pouch_retention_work_read(pouch, "orders/eu", &work, &error),
      LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  assert_false(work.present);
  assert_int_equal(lc_pouch_retention_work_write(pouch, "orders/eu",
                                                 LC_POUCH_RETENTION_IDLE, NULL,
                                                 NULL, 0, 0, &error),
                   LC_OK);
  assert_int_equal(lc_pouch_retention_hint_remove(pouch, "orders/eu", &error),
                   LC_OK);
  assert_int_equal(lc_pouch_retention_hint_remove(pouch, "orders/us", &error),
                   LC_OK);
  assert_int_equal(
      lc_pouch_retention_hint_next(pouch, NULL, &ns, &cursor, &error), LC_OK);
  assert_null(ns);
  assert_null(cursor);
  lc_pouch_close(pouch);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

typedef struct retention_selection {
  lc_pouch *pouch;
  lc_pouch_unix_seconds now;
  lc_pouch_retention_candidate candidate;
} retention_selection;

static int select_retention_parent(void *context, lc_error *error) {
  retention_selection *selection = context;
  return lc_pouch_state_retention_select_locked(
      selection->pouch, "orders", selection->now, &selection->candidate, error);
}

static void test_retention_selection_boundary_and_fairness(void **state) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  char root[sizeof(template_path)];
  const char *keys[] = {"a", "b", "future", "ordinary"};
  lc_pouch *pouch = NULL;
  lc_pouch_open_options open = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_source *body = NULL;
  lc_error error;
  retention_selection selection = {0};
  int i;
  (void)state;
  lc_error_init(&error);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  "/tmp/liblockdc-retention-"));
  open.background_compaction_enabled_set = 1;
  open.query_indexing_enabled_set = 1;
  assert_int_equal(lc_pouch_open(root, NULL, &open, &pouch, &error), LC_OK);
  selection.pouch = pouch;
  selection.now = 99;
  for (i = 0; i < 4; ++i) {
    options.has_retention = i != 3;
    options.retention = i == 2 ? 1000 : 100;
    assert_int_equal(lc_source_from_memory("{}", 2, &body, &error), LC_OK);
    assert_int_equal(lc_pouch_state_write(pouch, "orders", keys[i], body,
                                          &options, &written, &error),
                     LC_OK);
    lc_source_close(body);
    lc_pouch_state_write_result_cleanup(NULL, &written);
  }
  options.has_retention = 1;
  options.retention = 50;
  assert_int_equal(lc_source_from_memory("{}", 2, &body, &error), LC_OK);
  assert_int_equal(lc_pouch_state_stage_write(pouch, "orders", "a", "pending",
                                              body, &options, &written, &error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(select_retention_parent(&selection, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  assert_int_equal(lc_pouch_state_with_namespace_lock(pouch, "orders",
                                                      select_retention_parent,
                                                      &selection, &error),
                   LC_OK);
  assert_null(selection.candidate.key);
  assert_true(selection.candidate.has_expiring);
  selection.now = 100;
  for (i = 0; i < 3; ++i) {
    assert_int_equal(lc_pouch_state_with_namespace_lock(pouch, "orders",
                                                        select_retention_parent,
                                                        &selection, &error),
                     LC_OK);
    assert_string_equal(selection.candidate.key, i == 1 ? "b" : "a");
    assert_int_equal(selection.candidate.deadline, 100);
    assert_int_equal(selection.candidate.version, 1);
    lc_free_with_allocator(NULL, selection.candidate.key);
  }
  options.retention = 0;
  for (i = 0; i < 2; ++i) {
    assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", keys[i],
                                                    &options, &written, &error),
                     LC_OK);
    lc_pouch_state_write_result_cleanup(NULL, &written);
  }
  assert_int_equal(lc_pouch_state_with_namespace_lock(pouch, "orders",
                                                      select_retention_parent,
                                                      &selection, &error),
                   LC_OK);
  assert_null(selection.candidate.key);
  assert_true(selection.candidate.has_expiring);
  selection.now = 1000;
  assert_int_equal(lc_pouch_state_with_namespace_lock(pouch, "orders",
                                                      select_retention_parent,
                                                      &selection, &error),
                   LC_OK);
  assert_string_equal(selection.candidate.key, "future");
  lc_free_with_allocator(NULL, selection.candidate.key);
  assert_int_equal(lc_pouch_state_update_metadata(pouch, "orders", "future",
                                                  &options, &written, &error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(lc_pouch_state_with_namespace_lock(pouch, "orders",
                                                      select_retention_parent,
                                                      &selection, &error),
                   LC_OK);
  assert_null(selection.candidate.key);
  assert_false(selection.candidate.has_expiring);
  lc_pouch_close(pouch);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-retention-");
}

typedef struct cleanup_fixture {
  char root[128];
  char endpoint[512];
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_pouch *pouch;
  lc_lease *lease;
  lc_lease *other_lease;
  lc_source *source;
  lc_error error;
  retention_clock clock;
  long original_fencing;
} cleanup_fixture;

static int setup_cleanup_fixture(void **state) {
  char template_path[] = "/tmp/liblockdc-retention-XXXXXX";
  cleanup_fixture *fixture = calloc(1, sizeof(*fixture));
  lc_acquire_req acquire;
  lc_attach_req attach;
  lc_attach_res attached = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  assert_non_null(fixture);
  *state = fixture;
  lc_error_init(&fixture->error);
  fixture->clock.now = 1000;
  lc_pouch_test_queue_clock_gettime = retention_clock_gettime;
  lc_pouch_test_queue_time_context = &fixture->clock;
  assert_true(lc_test_tmp_mkdtemp(template_path, fixture->root,
                                  sizeof(fixture->root),
                                  "/tmp/liblockdc-retention-"));
  assert_true(
      snprintf(fixture->endpoint, sizeof(fixture->endpoint),
               "pouch://%s?background_compaction=false&query_indexing=false",
               fixture->root) > 0);
  fixture->endpoints[0] = fixture->endpoint;
  lc_client_config_init(&fixture->config);
  fixture->config.endpoints = fixture->endpoints;
  fixture->config.endpoint_count = 1;
  assert_int_equal(lc_pouch_legacy_client_open(
                       &fixture->config, &fixture->client, &fixture->error),
                   LC_OK);
  fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
  lc_acquire_req_init(&acquire);
  acquire.ns = "orders";
  acquire.key = "order";
  acquire.owner = "retention-test";
  acquire.ttl_seconds = 100;
  assert_int_equal(fixture->client->acquire(fixture->client, &acquire,
                                            &fixture->lease, &fixture->error),
                   LC_OK);
  fixture->original_fencing = fixture->lease->fencing_token;
  assert_int_equal(
      lc_source_from_memory("{}", 2, &fixture->source, &fixture->error), LC_OK);
  assert_int_equal(fixture->lease->update(fixture->lease, fixture->source, NULL,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  lc_attach_req_init(&attach);
  attach.name = "receipt.txt";
  attach.content_type = "text/plain";
  assert_int_equal(
      lc_source_from_memory("receipt", 7, &fixture->source, &fixture->error),
      LC_OK);
  assert_int_equal(fixture->lease->attach(fixture->lease, &attach,
                                          fixture->source, &attached,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  lc_attach_res_cleanup(&attached);
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  options.has_retention = 1;
  options.retention = 1050;
  assert_int_equal(lc_pouch_state_update_metadata(fixture->pouch, "orders",
                                                  "order", &options, &written,
                                                  &fixture->error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  return 0;
}

static int teardown_cleanup_fixture(void **state) {
  cleanup_fixture *fixture = *state;
  reset_retention_clock(NULL);
  if (fixture != NULL) {
    lc_source_close(fixture->source);
    if (fixture->lease != NULL)
      fixture->lease->close(fixture->lease);
    if (fixture->other_lease != NULL)
      fixture->other_lease->close(fixture->other_lease);
    if (fixture->client != NULL)
      lc_client_close(fixture->client);
    else
      lc_pouch_close(fixture->pouch);
    lc_error_cleanup(&fixture->error);
    lc_test_tmp_cleanup_path(fixture->root, "/tmp/liblockdc-retention-");
    free(fixture);
  }
  *state = NULL;
  return 0;
}

static void
test_retention_deletes_owned_attachments_and_preserves_fencing(void **state) {
  cleanup_fixture *fixture = *state;
  lc_pouch_maintenance_result maintained = {0};
  lc_pouch_state_read_result read = {0};
  lc_pouch_retention_work work = {0};
  lc_attachment_list_req list_request;
  lc_attachment_list list = {0};
  lc_acquire_req acquire;
  fixture->clock.now = 1049;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_int_equal(maintained.retention_deleted_state_count, 0);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_true(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  fixture->clock.now = 1050;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_int_equal(maintained.retention_deleted_state_count, 1);
  assert_int_equal(maintained.retention_failed_count, 0);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_false(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_attachment_list_req_init(&list_request);
  list_request.lease.ns = "orders";
  list_request.lease.key = "order";
  list_request.public_read = 1;
  assert_int_equal(fixture->client->list_attachments(
                       fixture->client, &list_request, &list, &fixture->error),
                   LC_OK);
  assert_int_equal(list.count, 0);
  lc_attachment_list_cleanup(&list);
  assert_int_equal(lc_pouch_retention_work_read(fixture->pouch, "orders", &work,
                                                &fixture->error),
                   LC_OK);
  assert_false(work.present);
  lc_pouch_retention_work_cleanup(fixture->pouch, &work);
  lc_acquire_req_init(&acquire);
  acquire.ns = "orders";
  acquire.key = "order";
  acquire.owner = "replacement";
  acquire.ttl_seconds = 100;
  assert_int_equal(fixture->client->acquire(fixture->client, &acquire,
                                            &fixture->lease, &fixture->error),
                   LC_OK);
  assert_true(fixture->lease->fencing_token > fixture->original_fencing);
  assert_int_equal(
      lc_source_from_memory("{}", 2, &fixture->source, &fixture->error), LC_OK);
  assert_int_equal(fixture->lease->update(fixture->lease, fixture->source, NULL,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_true(read.has_body);
  assert_int_equal(read.retention, 0);
  lc_pouch_state_read_result_cleanup(NULL, &read);
}

typedef struct cleanup_failure {
  int phase;
  int fired;
} cleanup_failure;

static int fail_cleanup_phase(int phase, void *context, lc_error *error) {
  cleanup_failure *failure = context;
  if (phase == failure->phase && !failure->fired) {
    failure->fired = 1;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "injected cleanup phase failure", NULL, NULL, "test");
  }
  return LC_OK;
}

static void assert_cleanup_state(cleanup_fixture *fixture, int has_body,
                                 size_t attachment_count) {
  lc_pouch_state_read_result read = {0};
  lc_attachment_list_req request;
  lc_attachment_list list = {0};
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_int_equal(read.has_body, has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_attachment_list_req_init(&request);
  request.lease.ns = "orders";
  request.lease.key = "order";
  request.public_read = 1;
  assert_int_equal(fixture->client->list_attachments(fixture->client, &request,
                                                     &list, &fixture->error),
                   LC_OK);
  assert_int_equal(list.count, attachment_count);
  lc_attachment_list_cleanup(&list);
}

static void test_retention_cleanup_rolls_back_before_commit(void **state) {
  cleanup_fixture *fixture = *state;
  cleanup_failure failure = {LC_POUCH_RETENTION_STAGED, 0};
  lc_pouch_maintenance_result maintained = {0};
  lc_pouch_retention_work work = {0};
  fixture->clock.now = 1050;
  lc_pouch_test_retention_hook = fail_cleanup_phase;
  lc_pouch_test_retention_context = &failure;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_ERR_TRANSPORT);
  assert_true(failure.fired);
  assert_int_equal(maintained.retention_failed_count, 1);
  assert_int_equal(maintained.retention_deleted_state_count, 0);
  lc_error_cleanup(&fixture->error);
  lc_pouch_test_retention_hook = NULL;
  assert_cleanup_state(fixture, 1, 1);
  assert_int_equal(lc_pouch_retention_work_read(fixture->pouch, "orders", &work,
                                                &fixture->error),
                   LC_OK);
  assert_true(work.present);
  assert_int_equal(work.phase, LC_POUCH_RETENTION_IDLE);
  lc_pouch_retention_work_cleanup(fixture->pouch, &work);
  fixture->clock.now = 1000;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_cleanup_state(fixture, 1, 1);
  fixture->clock.now = 1050;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_cleanup_state(fixture, 0, 0);
}

static void exercise_interrupted_cleanup(cleanup_fixture *fixture,
                                         lc_pouch_unix_seconds recover_time) {
  cleanup_failure failure = {LC_POUCH_RETENTION_PARENT_APPLIED, 0};
  lc_pouch_maintenance_result maintained = {0};
  lc_pouch_retention_work work = {0};
  lc_pouch_state_read_result journal = {0};
  lc_acquire_req acquire;
  lc_txn_replay_req replay;
  lc_txn_replay_res replayed = {0};
  char xid[LC_XID_STRING_SIZE];
  lc_u64 cleanup_fence;
  fixture->clock.now = 1050;
  lc_pouch_test_retention_hook = fail_cleanup_phase;
  lc_pouch_test_retention_context = &failure;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_ERR_TRANSPORT);
  lc_error_cleanup(&fixture->error);
  assert_true(failure.fired);
  lc_pouch_test_retention_hook = NULL;
  assert_int_equal(lc_pouch_retention_work_read(fixture->pouch, "orders", &work,
                                                &fixture->error),
                   LC_OK);
  assert_int_equal(work.phase, LC_POUCH_RETENTION_ACTIVE);
  strcpy(xid, work.txn_id);
  cleanup_fence = work.fencing_token;
  lc_pouch_retention_work_cleanup(fixture->pouch, &work);
  lc_acquire_req_init(&acquire);
  acquire.ns = "orders";
  acquire.key = "order";
  acquire.owner = "replacement";
  acquire.ttl_seconds = 100;
  assert_int_equal(fixture->client->acquire(fixture->client, &acquire,
                                            &fixture->lease, &fixture->error),
                   LC_ERR_PENDING);
  assert_null(fixture->lease);
  lc_error_cleanup(&fixture->error);
  lc_client_close(fixture->client);
  fixture->client = NULL;
  fixture->pouch = NULL;
  fixture->clock.now = recover_time;
  assert_int_equal(lc_pouch_legacy_client_open(
                       &fixture->config, &fixture->client, &fixture->error),
                   LC_OK);
  fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
  /* Client construction must preserve this root-owned journal for its worker.
   */
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, ".txns", xid,
                                                &journal, &fixture->error),
                   LC_OK);
  assert_true(journal.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &journal);
  lc_txn_replay_req_init(&replay);
  replay.txn_id = xid;
  assert_int_equal(fixture->client->txn_replay(fixture->client, &replay,
                                               &replayed, &fixture->error),
                   LC_ERR_PENDING);
  assert_string_equal(replayed.state, "commit");
  lc_txn_replay_res_cleanup(&replayed);
  lc_error_cleanup(&fixture->error);
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_cleanup_state(fixture, 0, 0);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, ".txns", xid,
                                                &journal, &fixture->error),
                   LC_OK);
  assert_false(journal.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &journal);
  assert_int_equal(fixture->client->acquire(fixture->client, &acquire,
                                            &fixture->lease, &fixture->error),
                   LC_OK);
  assert_true((lc_u64)fixture->lease->fencing_token > cleanup_fence);
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
}

static void test_retention_cleanup_resumes_after_lease_expiry(void **state) {
  exercise_interrupted_cleanup(*state, 2000);
}

static void test_retention_cleanup_resumes_after_clock_rollback(void **state) {
  exercise_interrupted_cleanup(*state, 900);
}

static int observe_retention_discovery(int phase, void *context,
                                       lc_error *error) {
  int *discoveries = context;
  (void)error;
  if (phase == LC_POUCH_RETENTION_DISCOVERED)
    ++*discoveries;
  return LC_OK;
}

static void exercise_retention_worker_discovery(void **state,
                                                int scan_failure) {
  cleanup_fixture *fixture = *state;
  lc_pouch_open_options options = {0};
  lc_pouch_state_read_result read = {0};
  int discoveries = 0;
  char directory[256];
  char saved[256];
  lc_client_close(fixture->client);
  fixture->client = NULL;
  fixture->pouch = NULL;
  if (scan_failure) {
    FILE *blocked;
    assert_true(snprintf(directory, sizeof(directory),
                         "%s/.lockd/state-retention", fixture->root) > 0);
    assert_true(snprintf(saved, sizeof(saved), "%s.saved", directory) > 0);
    assert_int_equal(rename(directory, saved), 0);
    blocked = fopen(directory, "w");
    assert_non_null(blocked);
    assert_int_equal(fclose(blocked), 0);
  }
  options.background_compaction_enabled_set = 1;
  options.background_compaction_enabled = 1;
  options.compaction_interval_seconds = 3600;
  options.query_indexing_enabled_set = 1;
  lc_pouch_test_retention_hook = observe_retention_discovery;
  lc_pouch_test_retention_context = &discoveries;
  assert_int_equal(lc_pouch_open(fixture->root, NULL, &options, &fixture->pouch,
                                 &fixture->error),
                   LC_OK);
  if (scan_failure) {
    assert_int_equal(unlink(directory), 0);
    assert_int_equal(rename(saved, directory), 0);
  }
  assert_int_equal(discoveries, 1);
  lc_pouch_test_retention_hook = NULL;
  fixture->clock.now = 1049;
  lc_pouch_test_compaction_run_pass(fixture->pouch);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_true(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  fixture->clock.now = 1050;
  lc_pouch_test_compaction_run_pass(fixture->pouch);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "order", &read,
                                                &fixture->error),
                   LC_OK);
  assert_false(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_pouch_close(fixture->pouch);
  fixture->pouch = NULL;
  assert_int_equal(lc_pouch_legacy_client_open(
                       &fixture->config, &fixture->client, &fixture->error),
                   LC_OK);
  fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
  assert_cleanup_state(fixture, 0, 0);
}

static void
test_retention_worker_rediscovers_idle_store_without_client(void **state) {
  exercise_retention_worker_discovery(state, 0);
}

static void
test_retention_worker_retries_failed_startup_discovery(void **state) {
  exercise_retention_worker_discovery(state, 1);
}

static void
test_retention_manual_maintenance_below_compaction_threshold(void **state) {
  cleanup_fixture *fixture = *state;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  fixture->clock.now = 1050;
  /* Background maintenance is disabled on this fixture. Neither an explicit
   * test turn nor physical obsolete-file cleanup may perform logical expiry. */
  lc_pouch_test_compaction_run_pass(fixture->pouch);
  assert_cleanup_state(fixture, 1, 1);
  options.ns = "orders";
  options.cleanup_only = 1;
  assert_int_equal(lc_pouch_maintenance_run(fixture->pouch, &options, &result,
                                            &fixture->error),
                   LC_OK);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  assert_cleanup_state(fixture, 1, 1);
  options.cleanup_only = 0;
  assert_int_equal(lc_pouch_maintenance_run(fixture->pouch, &options, &result,
                                            &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_deleted_state_count, 1);
  assert_false(result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  assert_cleanup_state(fixture, 0, 0);
}

static int crash_cleanup_phase(int phase, void *context, lc_error *error) {
  int *cut = context;
  (void)error;
  if (phase == *cut)
    _exit(50 + phase);
  return LC_OK;
}

static void test_retention_cleanup_crash_cuts(void **state) {
  int phase;
  for (phase = LC_POUCH_RETENTION_REF_CREATED;
       phase <= LC_POUCH_RETENTION_REF_CLEARED; ++phase) {
    cleanup_fixture *fixture;
    lc_pouch_maintenance_result maintained = {0};
    lc_acquire_req acquire;
    pid_t child;
    int status;
    int reopened;
    if (phase != LC_POUCH_RETENTION_REF_CREATED) {
      teardown_cleanup_fixture(state);
      setup_cleanup_fixture(state);
    }
    fixture = *state;
    lc_client_close(fixture->client);
    fixture->client = NULL;
    fixture->pouch = NULL;
    fixture->clock.now = 1050;
    /* A crashed exclusive writer deliberately keeps its HA fence until that
     * lease expires. Exercise process-crash recovery with shared writers so
     * these cuts require no wall-clock wait or weakened writer fencing. */
    assert_true(snprintf(fixture->endpoint, sizeof(fixture->endpoint),
                         "pouch://"
                         "%s?background_compaction=false&query_indexing=false&"
                         "single_writer=false",
                         fixture->root) > 0);
    child = fork();
    assert_true(child >= 0);
    if (child == 0) {
      int rc = lc_pouch_legacy_client_open(&fixture->config, &fixture->client,
                                           &fixture->error);
      if (rc != LC_OK)
        _exit(2);
      fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
      lc_pouch_test_retention_hook = crash_cleanup_phase;
      lc_pouch_test_retention_context = &phase;
      (void)lc_pouch_retention_run(fixture->pouch, "orders", 16, &maintained,
                                   &fixture->error);
      _exit(3);
    }
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 50 + phase);
    fixture->clock.now = phase < LC_POUCH_RETENTION_DECIDED ? 1000
                         : phase % 2                        ? 900
                                                            : 2000;
    reopened = lc_pouch_legacy_client_open(&fixture->config, &fixture->client,
                                           &fixture->error);
    if (reopened != LC_OK)
      fail_msg("cleanup crash cut %d reopen failed: %s", phase,
               fixture->error.message != NULL ? fixture->error.message
                                              : "unknown error");
    fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
    assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16,
                                            &maintained, &fixture->error),
                     LC_OK);
    assert_cleanup_state(fixture, phase < LC_POUCH_RETENTION_DECIDED,
                         phase < LC_POUCH_RETENTION_DECIDED ? 1 : 0);
    lc_acquire_req_init(&acquire);
    acquire.ns = "orders";
    acquire.key = "order";
    acquire.owner = "after-crash";
    acquire.ttl_seconds = 100;
    assert_int_equal(fixture->client->acquire(fixture->client, &acquire,
                                              &fixture->lease, &fixture->error),
                     LC_OK);
    assert_true(fixture->lease->fencing_token >
                fixture->original_fencing +
                    (phase >= LC_POUCH_RETENTION_CLAIMED ? 1 : 0));
    assert_int_equal(
        fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
    fixture->lease = NULL;
  }
}

static void acquire_cleanup_fixture_lease(cleanup_fixture *fixture,
                                          const char *key, const char *txn_id,
                                          long ttl, lc_lease **out) {
  lc_acquire_req request;
  lc_acquire_req_init(&request);
  request.ns = "orders";
  request.key = key;
  request.owner = "retention-test";
  request.txn_id = txn_id;
  request.ttl_seconds = ttl;
  assert_int_equal(
      fixture->client->acquire(fixture->client, &request, out, &fixture->error),
      LC_OK);
}

static void stage_cleanup_fixture_body(cleanup_fixture *fixture) {
  assert_int_equal(
      lc_source_from_memory("{\"v\":2}", 7, &fixture->source, &fixture->error),
      LC_OK);
  assert_int_equal(fixture->lease->update(fixture->lease, fixture->source, NULL,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
}

static void
test_retention_busy_parent_consumes_budget_without_starving_next(void **state) {
  cleanup_fixture *fixture = *state;
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_maintenance_result result = {0};
  lc_pouch_state_read_result read = {0};
  fixture->clock.now = 1040;
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  options.has_retention = 1;
  options.retention = 1050;
  assert_int_equal(
      lc_source_from_memory("{}", 2, &fixture->source, &fixture->error), LC_OK);
  assert_int_equal(lc_pouch_state_write(fixture->pouch, "orders", "other",
                                        fixture->source, &options, &written,
                                        &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &written);
  fixture->clock.now = 1050;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 1, &result,
                                          &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_expired_count, 1);
  assert_int_equal(result.retention_deleted_state_count, 0);
  assert_cleanup_state(fixture, 1, 1);
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 1, &result,
                                          &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_deleted_state_count, 1);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, "orders",
                                                "other", &read,
                                                &fixture->error),
                   LC_OK);
  assert_false(read.has_body);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  assert_cleanup_state(fixture, 1, 1);
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 1, &result,
                                          &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_deleted_state_count, 1);
  assert_cleanup_state(fixture, 0, 0);
}

static void exercise_prepared_retention_protection(cleanup_fixture *fixture,
                                                   int implicit_leader) {
  char xid[LC_XID_STRING_SIZE];
  lc_pouch_maintenance_result result = {0};
  lc_txn_decision_req decision;
  lc_txn_decision_res decided = {0};
  fixture->clock.now = 1040;
  assert_int_equal(lc_xid_new(xid, &fixture->error), LC_OK);
  acquire_cleanup_fixture_lease(fixture, "order", implicit_leader ? NULL : xid,
                                10, &fixture->lease);
  strcpy(xid, fixture->lease->txn_id);
  acquire_cleanup_fixture_lease(fixture, "other", xid, 10,
                                &fixture->other_lease);
  stage_cleanup_fixture_body(fixture);
  fixture->clock.now = 1060;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16, &result,
                                          &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_expired_count, 1);
  assert_int_equal(result.retention_deleted_state_count, 0);
  assert_int_equal(result.retention_failed_count, 0);
  assert_cleanup_state(fixture, 1, 1);
  lc_txn_decision_req_init(&decision);
  decision.txn_id = xid;
  assert_int_equal(fixture->client->txn_rollback(fixture->client, &decision,
                                                 &decided, &fixture->error),
                   LC_OK);
  lc_txn_decision_res_cleanup(&decided);
  fixture->lease->close(fixture->lease);
  fixture->lease = NULL;
  fixture->other_lease->close(fixture->other_lease);
  fixture->other_lease = NULL;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16, &result,
                                          &fixture->error),
                   LC_OK);
  assert_cleanup_state(fixture, 0, 0);
}

static void test_retention_protects_expired_prepared_reservation(void **state) {
  exercise_prepared_retention_protection(*state, 0);
}

static void test_retention_protects_implicit_leader_after_join(void **state) {
  exercise_prepared_retention_protection(*state, 1);
}

static void
test_retention_reclaims_expired_unmanaged_implicit_lease(void **state) {
  cleanup_fixture *fixture = *state;
  lc_attach_req attach;
  lc_attach_res attached = {0};
  lc_pouch_maintenance_result result = {0};
  fixture->clock.now = 1040;
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 10, &fixture->lease);
  stage_cleanup_fixture_body(fixture);
  lc_attach_req_init(&attach);
  attach.name = "unpublished.txt";
  attach.content_type = "text/plain";
  assert_int_equal(
      lc_source_from_memory("xx", 2, &fixture->source, &fixture->error), LC_OK);
  assert_int_equal(fixture->lease->attach(fixture->lease, &attach,
                                          fixture->source, &attached,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  lc_attach_res_cleanup(&attached);
  fixture->clock.now = 1060;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 16, &result,
                                          &fixture->error),
                   LC_OK);
  assert_int_equal(result.retention_deleted_state_count, 1);
  assert_cleanup_state(fixture, 0, 0);
  fixture->lease->close(fixture->lease);
  fixture->lease = NULL;
}

static void
test_retention_supports_existing_namespace_name_limit(void **state) {
  cleanup_fixture *fixture = *state;
  char ns[256];
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_read_result read = {0};
  int rc;
  memset(ns, 'n', sizeof(ns) - 1);
  ns[sizeof(ns) - 1] = '\0';
  assert_int_equal(
      lc_pouch_ensure_namespace(fixture->pouch, ns, &fixture->error), LC_OK);
  options.has_retention = 1;
  options.retention = 1200;
  assert_int_equal(
      lc_source_from_memory("{}", 2, &fixture->source, &fixture->error), LC_OK);
  rc = lc_pouch_state_write(fixture->pouch, ns, "long-namespace",
                            fixture->source, &options, &written,
                            &fixture->error);
  lc_source_close(fixture->source);
  fixture->source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &written);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, ns,
                                                "long-namespace", &read,
                                                &fixture->error),
                   LC_OK);
  assert_int_equal(read.retention, 1200);
  lc_pouch_state_read_result_cleanup(NULL, &read);
}

static void public_retention_write(cleanup_fixture *fixture,
                                   const lc_update_opts *opts) {
  assert_int_equal(
      lc_source_from_memory("{\"v\":2}", 7, &fixture->source, &fixture->error),
      LC_OK);
  assert_int_equal(fixture->lease->update(fixture->lease, fixture->source, opts,
                                          &fixture->error),
                   LC_OK);
  lc_source_close(fixture->source);
  fixture->source = NULL;
}

static void assert_public_retention(cleanup_fixture *fixture,
                                    lc_unix_seconds deadline) {
  lc_get_res result = {0};
  lc_sink *sink = NULL;
  assert_int_equal(lc_sink_to_memory(&sink, &fixture->error), LC_OK);
  assert_int_equal(lc_get_in_namespace(fixture->client, "orders", "order", NULL,
                                       sink, &result, &fixture->error),
                   LC_OK);
  lc_sink_close(sink);
  assert_int_equal(result.retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_true(result.has_retention_expires_at_unix);
  assert_int_equal(result.retention_expires_at_unix, deadline);
  lc_get_res_cleanup(&result);
}

static int count_native_decision(const lc_pouch_state_visit_entry *entry,
                                 void *context, lc_error *error) {
  size_t *count = context;
  (void)entry;
  (void)error;
  ++*count;
  return LC_OK;
}

static void
test_native_retention_release_leaves_no_startup_backlog(void **state) {
  cleanup_fixture *fixture = *state;
  lc_pouch_settings settings;
  lc_update_opts update = {0};
  lc_metadata_req metadata = {0};
  lc_release_req release = {0};
  lc_unix_seconds deadline = 1050;
  long fencing = fixture->original_fencing;
  size_t decisions;
  for (int durable = 0; durable < 2; ++durable) {
    lc_client_close(fixture->client);
    fixture->client = NULL;
    lc_pouch_settings_init(&settings);
    settings.set_mask =
        LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_SINGLE_WRITER;
    settings.durable_sync = durable;
    settings.single_writer = 0;
    fixture->config.pouch_settings = &settings;
    assert_int_equal(lc_pouch_legacy_client_open(
                         &fixture->config, &fixture->client, &fixture->error),
                     LC_OK);
    fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
    for (unsigned int i = 0; i < 128; ++i) {
      fixture->clock.now = 1100 + durable * 128 + i;
      acquire_cleanup_fixture_lease(fixture, "order", NULL, 100,
                                    &fixture->lease);
      assert_true(fixture->lease->fencing_token > fencing);
      fencing = fixture->lease->fencing_token;
      update.has_retention_seconds = metadata.has_retention_seconds = 1;
      update.retention_seconds = metadata.retention_seconds = 60;
      release.rollback = i % 3 == 2;
      if (i % 3 == 0)
        public_retention_write(fixture, &update);
      else
        assert_int_equal(fixture->lease->metadata(fixture->lease, &metadata,
                                                  &fixture->error),
                         LC_OK);
      if (i % 4 == 0) {
        lc_release_op operation;
        lc_release_res released = {0};
        lc_release_op_init(&operation);
        operation.lease.ns = fixture->lease->ns;
        operation.lease.key = fixture->lease->key;
        operation.lease.lease_id = fixture->lease->lease_id;
        operation.lease.txn_id = fixture->lease->txn_id;
        operation.lease.fencing_token = fixture->lease->fencing_token;
        operation.rollback = release.rollback;
        assert_int_equal(fixture->client->release(fixture->client, &operation,
                                                  &released, &fixture->error),
                         LC_OK);
        lc_release_res_cleanup(&released);
        fixture->lease->close(fixture->lease);
      } else
        assert_int_equal(
            fixture->lease->release(fixture->lease, &release, &fixture->error),
            LC_OK);
      fixture->lease = NULL;
      if (!release.rollback)
        deadline = fixture->clock.now + 60;
    }
    decisions = 0;
    assert_int_equal(lc_pouch_state_visit(fixture->pouch, ".txns",
                                          count_native_decision, &decisions,
                                          &fixture->error),
                     LC_OK);
    assert_int_equal(decisions,
                     0); /* No replay/deletion work on the next open. */
    lc_client_close(fixture->client);
    fixture->client = NULL;
    assert_int_equal(lc_pouch_legacy_client_open(
                         &fixture->config, &fixture->client, &fixture->error),
                     LC_OK);
    fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
    assert_public_retention(fixture, deadline);
    assert_cleanup_state(fixture, 1, 1);
  }
  fixture->config.pouch_settings = NULL;
}

static void test_native_retirement_failure_preserves_recovery(void **state) {
  cleanup_fixture *fixture = *state;
  lc_metadata_req metadata = {0};
  lc_pouch_state_read_result decision = {0};
  char id[LC_XID_STRING_SIZE];
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 60;
  for (int phase = 0; phase < 2; ++phase) {
    fixture->clock.now = 1010 + phase * 200;
    acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
    strcpy(id, fixture->lease->txn_id);
    assert_int_equal(
        fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
        LC_OK);
    if (phase == 0)
      lc_pouch_test_before_txn_replay_hook = lose_decision_ack;
    else
      lc_pouch_test_cleanup_fsync_hook = lose_decision_ack;
    assert_int_equal(
        fixture->lease->release(fixture->lease, NULL, &fixture->error),
        LC_ERR_TRANSPORT);
    lc_pouch_test_before_txn_replay_hook = NULL;
    lc_pouch_test_cleanup_fsync_hook = NULL;
    lc_error_cleanup(&fixture->error);
    assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, ".txns", id,
                                                  &decision, &fixture->error),
                     LC_OK);
    assert_true(decision.found);
    lc_pouch_state_read_result_cleanup(NULL, &decision);
    fixture->lease->close(fixture->lease);
    fixture->lease = NULL;
    fixture->clock.now += 40;
    lc_client_close(fixture->client);
    fixture->client = NULL;
    assert_int_equal(lc_pouch_legacy_client_open(
                         &fixture->config, &fixture->client, &fixture->error),
                     LC_OK);
    fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
    assert_public_retention(fixture, 1070 + phase * 200);
    assert_cleanup_state(fixture, 1, 1);
    assert_int_equal(lc_pouch_state_read_metadata(fixture->pouch, ".txns", id,
                                                  &decision, &fixture->error),
                     LC_OK);
    assert_false(decision.found);
    lc_pouch_state_read_result_cleanup(NULL, &decision);
  }
}

static void
test_native_retirement_preserves_explicit_and_joined_outcomes(void **state) {
  cleanup_fixture *fixture = *state;
  lc_metadata_req metadata = {0};
  lc_release_req release = {0};
  lc_txn_replay_req request;
  lc_txn_replay_res result = {0};
  char id[LC_XID_STRING_SIZE];
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 60;
  for (int mode = 0; mode < 4; ++mode) {
    assert_int_equal(lc_xid_new(id, &fixture->error), LC_OK);
    acquire_cleanup_fixture_lease(fixture, "order", mode >= 2 ? NULL : id, 100,
                                  &fixture->lease);
    if (mode == 2) {
      strcpy(id, fixture->lease->txn_id);
      acquire_cleanup_fixture_lease(fixture, "other", id, 100,
                                    &fixture->other_lease);
    }
    assert_int_equal(
        fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
        LC_OK);
    release.rollback = mode == 1;
    if (mode == 3) {
      lc_txn_decision_req commit;
      lc_txn_decision_res committed = {0};
      strcpy(id, fixture->lease->txn_id);
      lc_txn_decision_req_init(&commit);
      commit.txn_id = id;
      assert_int_equal(
          lc_txn_commit(fixture->client, &commit, &committed, &fixture->error),
          LC_OK);
      lc_txn_decision_res_cleanup(&committed);
    }
    assert_int_equal(
        fixture->lease->release(fixture->lease, &release, &fixture->error),
        LC_OK);
    fixture->lease = NULL;
    if (mode == 2) {
      assert_int_equal(fixture->other_lease->release(fixture->other_lease, NULL,
                                                     &fixture->error),
                       LC_OK);
      fixture->other_lease = NULL;
    }
    lc_txn_replay_req_init(&request);
    request.txn_id = id;
    assert_int_equal(
        lc_txn_replay(fixture->client, &request, &result, &fixture->error),
        LC_OK);
    assert_string_equal(result.state, mode == 1 ? "rollback" : "commit");
    lc_txn_replay_res_cleanup(&result);
  }
}

static void
test_public_retention_commit_clock_and_omitted_updates(void **state) {
  cleanup_fixture *fixture = *state;
  lc_update_opts opts = {0};
  lc_describe_req describe = {"orders", "order"};
  lc_describe_res described = {0};
  lc_get_opts public_read = {1};
  lc_get_res read = {0};
  lc_sink *sink = NULL;
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  opts.has_retention_seconds = 1;
  opts.retention_seconds = 60;
  public_retention_write(fixture, &opts);
  assert_int_equal(fixture->lease->retention_support,
                   LC_STATE_RETENTION_SUPPORTED);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  assert_public_retention(fixture, 1050);
  assert_int_equal(lc_sink_to_memory(&sink, &fixture->error), LC_OK);
  assert_int_equal(fixture->lease->get(fixture->lease, sink, &public_read,
                                       &read, &fixture->error),
                   LC_OK);
  lc_sink_close(sink);
  assert_int_equal(read.retention_expires_at_unix, 1050);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  lc_get_res_cleanup(&read);
  assert_int_equal(
      lc_describe(fixture->client, &describe, &described, &fixture->error),
      LC_OK);
  assert_int_equal(described.retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_false(described.has_retention_expires_at_unix);
  lc_describe_res_cleanup(&described);
  fixture->clock.now = 1010;
  public_retention_write(fixture, NULL);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  fixture->clock.now = 1030;
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_public_retention(fixture, 1090);
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  opts.has_retention_seconds = 0;
  opts.retention_seconds = -42; /* An unselected value is ignored. */
  public_retention_write(fixture, &opts);
  assert_true(fixture->lease->has_retention_expires_at_unix);
  assert_int_equal(fixture->lease->retention_expires_at_unix, 1090);
  fixture->clock.now = 1040;
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_public_retention(fixture, 1090);
}

static void test_public_retention_metadata_clear_and_recreation(void **state) {
  cleanup_fixture *fixture = *state;
  lc_metadata_req metadata = {0};
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 90;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_OK);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  public_retention_write(fixture, NULL);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  metadata.retention_seconds = 0;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_OK);
  assert_true(fixture->lease->has_retention_expires_at_unix);
  assert_int_equal(fixture->lease->retention_expires_at_unix, 0);
  public_retention_write(fixture, NULL);
  assert_true(fixture->lease->has_retention_expires_at_unix);
  assert_int_equal(fixture->lease->retention_expires_at_unix, 0);
  fixture->clock.now = 1020;
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_public_retention(fixture, 0);
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  metadata.retention_seconds = 90;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_OK);
  assert_int_equal(
      fixture->lease->remove(fixture->lease, NULL, &fixture->error), LC_OK);
  public_retention_write(fixture, NULL);
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_public_retention(fixture, 0);
}

static void test_public_retention_replays_immutable_commit_time(void **state) {
  cleanup_fixture *fixture = *state;
  lc_update_opts opts = {0};
  lc_txn_replay_req replay;
  lc_txn_decision_req decision;
  lc_txn_decision_res decided = {0};
  lc_txn_replay_res replayed = {0};
  char txn_id[LC_XID_STRING_SIZE];
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  opts.has_retention_seconds = 1;
  opts.retention_seconds = 60;
  public_retention_write(fixture, &opts);
  assert_true(strlen(fixture->lease->txn_id) < sizeof(txn_id));
  strcpy(txn_id, fixture->lease->txn_id);
  fixture->clock.now = 1030;
  lc_pouch_test_after_txn_decision_hook = lose_decision_ack;
  lc_txn_decision_req_init(&decision);
  decision.txn_id = txn_id;
  assert_int_equal(
      lc_txn_commit(fixture->client, &decision, &decided, &fixture->error),
      LC_ERR_TRANSPORT);
  lc_txn_decision_res_cleanup(&decided);
  lc_pouch_test_after_txn_decision_hook = NULL;
  lc_error_cleanup(&fixture->error);
  fixture->clock.now = 1080;
  lc_txn_replay_req_init(&replay);
  replay.txn_id = txn_id;
  assert_int_equal(
      lc_txn_replay(fixture->client, &replay, &replayed, &fixture->error),
      LC_OK);
  lc_txn_replay_res_cleanup(&replayed);
  assert_public_retention(fixture, 1090);
}

typedef struct retained_document {
  lonejson_int64 value;
} retained_document;
static const lonejson_field retained_fields[] = {
    LONEJSON_FIELD_I64(retained_document, value, "value")};
LONEJSON_MAP_DEFINE(retained_map, retained_document, retained_fields);

static void
test_public_retention_mapped_save_and_input_rejection(void **state) {
  cleanup_fixture *fixture = *state;
  lc_update_opts options = {0};
  lc_metadata_req metadata = {0};
  retained_document document = {1};
  acquire_cleanup_fixture_lease(fixture, "empty", NULL, 100, &fixture->lease);
  metadata.has_retention_seconds = 1;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_ERR_INVALID);
  lc_error_cleanup(&fixture->error);
  options.has_retention_seconds = 1;
  options.retention_seconds = -1;
  assert_int_equal(lc_lease_save(fixture->lease, &retained_map, &document,
                                 &options, &fixture->error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&fixture->error);
  options.retention_seconds = 60;
  assert_int_equal(lc_lease_save(fixture->lease, &retained_map, &document,
                                 &options, &fixture->error),
                   LC_OK);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  assert_int_equal(lc_lease_save(fixture->lease, &retained_map, &document, NULL,
                                 &fixture->error),
                   LC_OK);
  assert_false(fixture->lease->has_retention_expires_at_unix);
  metadata.retention_seconds = 0;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_OK);
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  acquire_cleanup_fixture_lease(fixture, "__lockdc_io/v1/outbox/reserved", NULL,
                                100, &fixture->lease);
  assert_int_equal(lc_lease_save(fixture->lease, &retained_map, &document,
                                 &options, &fixture->error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&fixture->error);
}

static void test_public_retention_cannot_change_after_vote(void **state) {
  cleanup_fixture *fixture = *state;
  lc_update_opts options = {0};
  lc_metadata_req metadata = {0};
  lc_release_op release;
  lc_release_res released = {0};
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  acquire_cleanup_fixture_lease(fixture, "other", fixture->lease->txn_id, 100,
                                &fixture->other_lease);
  options.has_retention_seconds = 1;
  options.retention_seconds = 60;
  public_retention_write(fixture, &options);
  lc_release_op_init(&release);
  release.lease.ns = fixture->lease->ns;
  release.lease.key = fixture->lease->key;
  release.lease.lease_id = fixture->lease->lease_id;
  release.lease.txn_id = fixture->lease->txn_id;
  release.lease.fencing_token = fixture->lease->fencing_token;
  assert_int_equal(
      lc_release(fixture->client, &release, &released, &fixture->error), LC_OK);
  lc_release_res_cleanup(&released);
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 30;
  assert_int_equal(
      fixture->lease->metadata(fixture->lease, &metadata, &fixture->error),
      LC_ERR_INVALID);
  lc_error_cleanup(&fixture->error);
  assert_public_retention(fixture, 1050);
  fixture->clock.now = 1020;
  assert_int_equal(fixture->other_lease->release(fixture->other_lease, NULL,
                                                 &fixture->error),
                   LC_OK);
  fixture->other_lease = NULL;
  assert_public_retention(fixture, 1080);
}

static int count_retention_query_key(void *context, lc_error *error) {
  (void)error;
  ++*(int *)context;
  return 1;
}

static void assert_indexed_retention_count(cleanup_fixture *fixture,
                                           int expected) {
  lc_index_flush_req flush;
  lc_index_flush_res flushed = {0};
  lc_query_req request;
  lc_query_res result = {0};
  lc_query_key_handler handler = {0};
  int count = 0;
  lc_index_flush_req_init(&flush);
  flush.ns = "orders";
  flush.mode = "sync";
  assert_int_equal(
      lc_flush_index(fixture->client, &flush, &flushed, &fixture->error),
      LC_OK);
  lc_index_flush_res_cleanup(&flushed);
  lc_query_req_init(&request);
  request.ns = "orders";
  request.engine = "index";
  request.limit = 10;
  handler.end = count_retention_query_key;
  assert_int_equal(lc_query_keys(fixture->client, &request, &handler, &count,
                                 &result, &fixture->error),
                   LC_OK);
  lc_query_res_cleanup(&result);
  assert_int_equal(count, expected);
}

static void
test_public_retention_expired_state_can_be_extended_and_queried(void **state) {
  cleanup_fixture *fixture = *state;
  lc_update_opts options = {0};
  lc_pouch_maintenance_result maintained = {0};
  lc_client_close(fixture->client);
  fixture->client = NULL;
  fixture->pouch = NULL;
  assert_true(snprintf(fixture->endpoint, sizeof(fixture->endpoint),
                       "pouch://%s?background_compaction=false",
                       fixture->root) > 0);
  assert_int_equal(lc_pouch_legacy_client_open(
                       &fixture->config, &fixture->client, &fixture->error),
                   LC_OK);
  fixture->pouch = ((lc_client_handle *)fixture->client)->pouch;
  fixture->clock.now = 1050;
  assert_public_retention(fixture, 1050);
  assert_indexed_retention_count(fixture, 1);
  acquire_cleanup_fixture_lease(fixture, "order", NULL, 100, &fixture->lease);
  options.has_retention_seconds = 1;
  options.retention_seconds = 60;
  public_retention_write(fixture, &options);
  fixture->clock.now = 1051;
  assert_int_equal(
      fixture->lease->release(fixture->lease, NULL, &fixture->error), LC_OK);
  fixture->lease = NULL;
  assert_public_retention(fixture, 1111);
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 64,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_int_equal(maintained.retention_deleted_state_count, 0);
  assert_indexed_retention_count(fixture, 1);
  fixture->clock.now = 1111;
  assert_int_equal(lc_pouch_retention_run(fixture->pouch, "orders", 64,
                                          &maintained, &fixture->error),
                   LC_OK);
  assert_int_equal(maintained.retention_deleted_state_count, 1);
  assert_indexed_retention_count(fixture, 0);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_teardown(
          test_retention_hint_retirement_survives_nosync_power_loss,
          reset_retention_clock),
      cmocka_unit_test_teardown(
          test_retention_idle_retirement_survives_nosync_power_loss,
          reset_retention_clock),
      cmocka_unit_test_teardown(
          test_retention_hint_sync_failure_preserves_discovery,
          reset_retention_clock),
      cmocka_unit_test_teardown(
          test_retention_idle_sync_failure_preserves_discovery,
          reset_retention_clock),
      cmocka_unit_test_setup_teardown(
          test_native_retention_release_leaves_no_startup_backlog,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_native_retirement_failure_preserves_recovery,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_native_retirement_preserves_explicit_and_joined_outcomes,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_expired_state_can_be_extended_and_queried,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_worker_retries_failed_startup_discovery,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_mapped_save_and_input_rejection,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_cannot_change_after_vote, setup_cleanup_fixture,
          teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_commit_clock_and_omitted_updates,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_metadata_clear_and_recreation,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_public_retention_replays_immutable_commit_time,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_supports_existing_namespace_name_limit,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_busy_parent_consumes_budget_without_starving_next,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_protects_expired_prepared_reservation,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_protects_implicit_leader_after_join,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_reclaims_expired_unmanaged_implicit_lease,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_worker_rediscovers_idle_store_without_client,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_manual_maintenance_below_compaction_threshold,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(test_retention_cleanup_crash_cuts,
                                      setup_cleanup_fixture,
                                      teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_deletes_owned_attachments_and_preserves_fencing,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_cleanup_rolls_back_before_commit,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_cleanup_resumes_after_lease_expiry,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test_setup_teardown(
          test_retention_cleanup_resumes_after_clock_rollback,
          setup_cleanup_fixture, teardown_cleanup_fixture),
      cmocka_unit_test(test_retention_selection_boundary_and_fairness),
      cmocka_unit_test(test_retention_hint_lifecycle_and_discovery),
      cmocka_unit_test_teardown(test_cleanup_durability_on_nosync_root,
                                reset_retention_clock),
      cmocka_unit_test(test_exclusive_retention_storage),
      cmocka_unit_test(test_shared_retention_storage),
      cmocka_unit_test(test_compressed_retention_storage),
      cmocka_unit_test(test_encrypted_shared_retention_storage),
      cmocka_unit_test_teardown(test_retention_commit_clock_and_staging,
                                reset_retention_clock),
      cmocka_unit_test_teardown(test_coordinated_retention_delayed_recovery,
                                reset_retention_clock),
      cmocka_unit_test_teardown(test_coordinated_retention_clock_rollback,
                                reset_retention_clock),
      cmocka_unit_test_teardown(
          test_coordinated_retention_checks_every_duration,
          reset_retention_clock),
      cmocka_unit_test_teardown(test_coordinated_retention_missing_commit_time,
                                reset_retention_clock),
      cmocka_unit_test_teardown(test_coordinated_retention_commit_votes,
                                reset_retention_clock),
      cmocka_unit_test_teardown(test_coordinated_retention_shared_recovery,
                                reset_retention_clock),
      cmocka_unit_test_teardown(
          test_coordinated_retention_expired_lease_recovery,
          reset_retention_clock),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
