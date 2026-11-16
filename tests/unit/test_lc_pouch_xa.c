#include <cmocka.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include "../support/lc_test_cases.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_xa.h"
#include "lc_xa_internal.h"

#include <dirent.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

typedef struct fixture {
  lc_client *client;
  lc_client *peer;
  lc_lease *lease;
  lc_error error;
  char root[1024];
  char endpoint[1100];
  lc_xa_branch_record branches[2];
  lc_xa_store *journals[3];
  lc_xa_coordinator *coordinators[2];
  lc_xa_transaction *transactions[2];
  lc_xa_coordinator_operation *operation;
  lc_outbox *outbox;
  lc_outbox_transaction *box;
  lc_outbox_participant *participant;
  lc_outbox_commit_result receipts;
} fixture;

#ifdef LC_XA_NATIVE_SYNC_TEST
static pthread_mutex_t sync_watch_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct {
  int active, fail;
  dev_t device;
  ino_t inode;
  size_t calls;
} sync_watch;
int __real_fsync(int fd);
int __wrap_fsync(int fd) {
  struct stat st;
  int fail = 0;
  pthread_mutex_lock(&sync_watch_mutex);
  if (sync_watch.active && !fstat(fd, &st) && st.st_dev == sync_watch.device &&
      st.st_ino == sync_watch.inode) {
    ++sync_watch.calls;
    fail = sync_watch.fail;
  }
  pthread_mutex_unlock(&sync_watch_mutex);
  if (fail) {
    errno = EIO;
    return -1;
  }
  return __real_fsync(fd);
}
#endif

static void remove_tree(const char *path) {
  DIR *directory = opendir(path);
  struct dirent *entry;
  char child[2048];
  struct stat metadata;
  if (directory == NULL) {
    assert_int_equal(unlink(path), 0);
    return;
  }
  while ((entry = readdir(directory)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    assert_true(snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) >
                0);
    assert_int_equal(lstat(child, &metadata), 0);
    if (S_ISDIR(metadata.st_mode))
      remove_tree(child);
    else
      assert_int_equal(unlink(child), 0);
  }
  assert_int_equal(closedir(directory), 0);
  assert_int_equal(rmdir(path), 0);
}

static void clear_error(fixture *f) {
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}

static void open_client(fixture *f) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;
  endpoints[0] = f->endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  rc = lc_client_open(&config, &f->client, &f->error);
  if (rc != LC_OK)
    print_error("XA fixture client open failed (%d): %s (%s)\n", rc,
                f->error.message != NULL ? f->error.message : "no summary",
                f->error.detail != NULL ? f->error.detail : "no detail");
  assert_int_equal(rc, LC_OK);
}

static int setup(void **state) {
  fixture *f = (fixture *)calloc(1U, sizeof(*f));
  char cwd[900];
  size_t i;
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  assert_non_null(getcwd(cwd, sizeof(cwd)));
  assert_true(snprintf(f->root, sizeof(f->root), "%s/xa-pouch-XXXXXX", cwd) >
              0);
  assert_non_null(mkdtemp(f->root));
  assert_true(snprintf(f->endpoint, sizeof(f->endpoint),
                       "pouch://%s?single_writer=true", f->root) > 0);
  open_client(f);
  assert_int_equal(lc_xid_new(f->branches[0].txn_id, &f->error), LC_OK);
  for (i = 0U; i < 2U; ++i) {
    lc_xa_branch_record *branch = &f->branches[i];
    if (i != 0U)
      strcpy(branch->txn_id, f->branches[0].txn_id);
    assert_int_equal(lc_xid_new(branch->resource_id, &f->error), LC_OK);
    assert_int_equal(lc_xid_new(branch->branch_id, &f->error), LC_OK);
    assert_true(snprintf(branch->gid, sizeof(branch->gid), "lc-%s-%s",
                         branch->txn_id, branch->branch_id) > 0);
    strcpy(branch->full_xid, "18446744073709551615");
    strcpy(branch->system_id, "7695692707854696465");
    strcpy(branch->database_oid, "16384");
    strcpy(branch->role_oid, "16385");
  }
  return 0;
}

static int teardown(void **state) {
  fixture *f = (fixture *)*state;
#ifdef LC_XA_NATIVE_SYNC_TEST
  pthread_mutex_lock(&sync_watch_mutex);
  sync_watch.active = 0;
  pthread_mutex_unlock(&sync_watch_mutex);
#endif
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_pouch_test_before_txn_decision_hook = NULL;
  lc_pouch_test_before_txn_decision_context = NULL;
  lc_pouch_test_before_xa_owner_write_hook = NULL;
  lc_pouch_test_before_xa_owner_write_context = NULL;
  lc_pouch_test_after_xa_retire_owner_hook = NULL;
  lc_pouch_test_after_xa_retire_owner_context = NULL;
  (void)lc_xa_coordinator_operation_close_blocking(f->operation, NULL);
  (void)lc_xa_transaction_close_blocking(f->transactions[0], NULL);
  (void)lc_xa_transaction_close_blocking(f->transactions[1], NULL);
  (void)lc_xa_coordinator_close_blocking(f->coordinators[0], NULL);
  (void)lc_xa_coordinator_close_blocking(f->coordinators[1], NULL);
  lc_outbox_transaction_close(f->box);
  lc_outbox_participant_close(f->participant);
  lc_outbox_commit_result_cleanup(&f->receipts);
  lc_outbox_close(f->outbox);
  {
    size_t i;
    for (i = 0U; i < 3U; ++i)
      lc_xa_store_close(f->journals[i]);
  }
  if (f->lease != NULL)
    f->lease->close(f->lease);
  if (f->client != NULL)
    lc_client_close(f->client);
  if (f->peer != NULL)
    lc_client_close(f->peer);
  lc_error_cleanup(&f->error);
  remove_tree(f->root);
  free(f);
  return 0;
}

static lc_pouch *store(fixture *f) {
  return ((lc_client_handle *)f->client)->pouch;
}

static void enroll(fixture *f, size_t index) {
  assert_int_equal(lc_pouch_client_xa_enroll(f->client, &f->branches[index],
                                             NULL, &f->error),
                   LC_OK);
}

static void progress(fixture *f, size_t index, unsigned char phase) {
  f->branches[index].phase = phase;
  if (phase == LC_XA_PREPARED)
    f->branches[index].prepared_observed = 1;
  if (phase >= LC_XA_COMMITTED) {
    f->branches[index].evidence = LC_XA_EVIDENCE_NATIVE;
    f->branches[index].completed_at_unix = (lc_unix_seconds)time(NULL);
  }
  assert_int_equal(lc_pouch_client_xa_progress(f->client, &f->branches[index],
                                               NULL, &f->error),
                   LC_OK);
}

static void prepare_branch(fixture *f, size_t index) {
  progress(f, index, LC_XA_PREPARE_SENT);
  progress(f, index, LC_XA_PREPARED);
}

static void stage_native(fixture *f) {
  lc_txn_participant participant;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result written;
  lc_source *source = NULL;
  const char value[] = "{\"value\":42}";
  memset(&result, 0, sizeof(result));
  memset(&written, 0, sizeof(written));
  memset(&options, 0, sizeof(options));
  lc_txn_decision_req_init(&request);
  participant.ns = "default";
  participant.key = "business";
  participant.backend_hash = NULL;
  request.txn_id = f->branches[0].txn_id;
  request.participants = &participant;
  request.participant_count = 1U;
  assert_int_equal(lc_txn_prepare(f->client, &request, &result, &f->error),
                   LC_OK);
  lc_txn_decision_res_cleanup(&result);
  assert_int_equal(
      lc_source_from_memory(value, sizeof(value) - 1U, &source, &f->error),
      LC_OK);
  options.content_type = "application/json";
  assert_int_equal(lc_pouch_state_stage_write(store(f), "default", "business",
                                              request.txn_id, source, &options,
                                              &written, &f->error),
                   LC_OK);
  source->close(source);
  lc_pouch_state_write_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &written);
}

static void decision(fixture *f, int rollback, int expected) {
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  int rc;
  lc_txn_decision_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.txn_id = f->branches[0].txn_id;
  rc = rollback ? lc_txn_rollback(f->client, &request, &result, &f->error)
                : lc_txn_commit(f->client, &request, &result, &f->error);
  assert_int_equal(rc, expected);
  if (expected == LC_OK || expected == LC_ERR_PENDING)
    assert_string_equal(result.state, rollback ? "rollback" : "commit");
  lc_txn_decision_res_cleanup(&result);
  clear_error(f);
}

static void replay(fixture *f, const char *state, int expected) {
  lc_txn_replay_req request;
  lc_txn_replay_res result;
  lc_txn_replay_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.txn_id = f->branches[0].txn_id;
  assert_int_equal(lc_txn_replay(f->client, &request, &result, &f->error),
                   expected);
  if (expected == LC_OK || expected == LC_ERR_PENDING)
    assert_string_equal(result.state, state);
  lc_txn_replay_res_cleanup(&result);
  clear_error(f);
}

static void visible(fixture *f, int expected) {
  lc_pouch_state_read_result result;
  memset(&result, 0, sizeof(result));
  assert_int_equal(
      lc_pouch_state_read(store(f), "default", "business", &result, &f->error),
      LC_OK);
  assert_int_equal(result.found, expected);
  if (expected) {
    char value[32];
    size_t count = 0U;
    memset(value, 0, sizeof(value));
    count =
        result.body->read(result.body, value, sizeof(value) - 1U, &f->error);
    assert_int_equal(f->error.code, LC_OK);
    assert_true(count > 0U);
    assert_string_equal(value, "{\"value\":42}");
  }
  lc_pouch_state_read_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &result);
}

static void reopen(fixture *f) {
  lc_client_close(f->client);
  f->client = NULL;
  open_client(f);
}

static void test_pending_commit_survives_reopen(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_branch_record loaded;
  char key[LC_POUCH_XA_KEY_SIZE];
  enroll(f, 0U);
  stage_native(f);
  prepare_branch(f, 0U);
  decision(f, 0, LC_ERR_PENDING);
  visible(f, 0);
  reopen(f);
  visible(f, 0);
  replay(f, "commit", LC_ERR_PENDING);
  progress(f, 0U, LC_XA_COMMIT_SENT);
  progress(f, 0U, LC_XA_COMMITTED);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  assert_int_equal(lc_pouch_xa_branch_read(store(f), key, &loaded, &f->error),
                   LC_OK);
  assert_string_equal(loaded.full_xid, "18446744073709551615");
  assert_int_equal(loaded.evidence, LC_XA_EVIDENCE_NATIVE);
  replay(f, "commit", LC_OK);
  visible(f, 1);
  reopen(f);
  replay(f, "commit", LC_OK);
  visible(f, 1);
  decision(f, 1, LC_ERR_INVALID);
}

static void
test_all_external_branches_complete_before_application(void **state) {
  fixture *f = (fixture *)*state;
  enroll(f, 0U);
  enroll(f, 1U);
  stage_native(f);
  prepare_branch(f, 0U);
  decision(f, 0, LC_ERR_INVALID);
  replay(f, "prepare", LC_OK);
  prepare_branch(f, 1U);
  decision(f, 0, LC_ERR_PENDING);
  progress(f, 0U, LC_XA_COMMIT_SENT);
  progress(f, 0U, LC_XA_COMMITTED);
  replay(f, "commit", LC_ERR_PENDING);
  visible(f, 0);
  progress(f, 1U, LC_XA_COMMIT_SENT);
  progress(f, 1U, LC_XA_COMMITTED);
  replay(f, "commit", LC_OK);
  visible(f, 1);
}

static void test_rollback_waits_for_uncertain_prepare_cleanup(void **state) {
  fixture *f = (fixture *)*state;
  enroll(f, 0U);
  stage_native(f);
  progress(f, 0U, LC_XA_PREPARE_SENT);
  decision(f, 1, LC_ERR_PENDING);
  reopen(f);
  replay(f, "rollback", LC_ERR_PENDING);
  progress(f, 0U, LC_XA_ROLLBACK_SENT);
  progress(f, 0U, LC_XA_ABORTED);
  replay(f, "rollback", LC_OK);
  visible(f, 0);
  decision(f, 0, LC_ERR_INVALID);
}

static void
test_enrollment_seals_and_opposite_resolution_is_rejected(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_branch_record wrong;
  enroll(f, 0U);
  progress(f, 0U, LC_XA_PREPARE_SENT);
  assert_int_equal(
      lc_pouch_client_xa_enroll(f->client, &f->branches[1], NULL, &f->error),
      LC_ERR_INVALID);
  clear_error(f);
  replay(f, "prepare", LC_OK);
  progress(f, 0U, LC_XA_PREPARED);
  replay(f, "commit", LC_ERR_PENDING);
  wrong = f->branches[0];
  wrong.phase = LC_XA_ROLLBACK_SENT;
  assert_int_equal(
      lc_pouch_client_xa_progress(f->client, &wrong, NULL, &f->error),
      LC_ERR_PROTOCOL);
  clear_error(f);
  progress(f, 0U, LC_XA_COMMIT_SENT);
  progress(f, 0U, LC_XA_COMMITTED);
  replay(f, "commit", LC_OK);
}

static void
test_branch_identity_is_immutable_and_namespace_private(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_branch_record wrong;
  lc_sink *sink = NULL;
  lc_get_res result;
  lc_xa_branch_record loaded;
  char key[LC_POUCH_XA_KEY_SIZE];
  memset(&result, 0, sizeof(result));
  enroll(f, 0U);
  enroll(f, 0U);
  wrong = f->branches[0];
  strcpy(wrong.full_xid, "42");
  assert_int_equal(
      lc_pouch_client_xa_enroll(f->client, &wrong, NULL, &f->error),
      LC_ERR_PROTOCOL);
  clear_error(f);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &f->error), LC_OK);
  assert_int_equal(lc_get_in_namespace(f->client, ".xa", key, NULL, sink,
                                       &result, &f->error),
                   LC_ERR_INVALID);
  sink->close(sink);
  lc_get_res_cleanup(&result);
  clear_error(f);
  assert_int_equal(lc_pouch_xa_branch_read(store(f), key, &loaded, &f->error),
                   LC_OK);
  assert_string_equal(loaded.full_xid, f->branches[0].full_xid);
}

static void test_missing_descriptor_blocks_entire_application(void **state) {
  fixture *f = (fixture *)*state;
  lc_pouch_state_write_result result;
  char key[LC_POUCH_XA_KEY_SIZE];
  memset(&result, 0, sizeof(result));
  enroll(f, 0U);
  stage_native(f);
  prepare_branch(f, 0U);
  decision(f, 0, LC_ERR_PENDING);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  assert_int_equal(
      lc_pouch_state_delete(store(f), ".xa", key, NULL, &result, &f->error),
      LC_OK);
  lc_pouch_state_write_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &result);
  replay(f, NULL, LC_ERR_PROTOCOL);
  visible(f, 0);
}

static void test_invalid_native_identity_and_phase_are_rejected(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_branch_record wrong = f->branches[0];
  char key[LC_POUCH_XA_KEY_SIZE];
  strcpy(wrong.full_xid, "18446744073709551616");
  assert_int_equal(lc_pouch_xa_branch_key(&wrong, key, sizeof(key), &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
  wrong = f->branches[0];
  memset(wrong.gid, 'x', sizeof(wrong.gid));
  assert_int_equal(lc_pouch_xa_branch_key(&wrong, key, sizeof(key), &f->error),
                   LC_ERR_PROTOCOL);
  clear_error(f);
  enroll(f, 0U);
  wrong = f->branches[0];
  wrong.phase = LC_XA_COMMITTED;
  wrong.evidence = LC_XA_EVIDENCE_NATIVE;
  wrong.completed_at_unix = (lc_unix_seconds)time(NULL);
  assert_int_equal(
      lc_pouch_client_xa_progress(f->client, &wrong, NULL, &f->error),
      LC_ERR_PROTOCOL);
  clear_error(f);
  replay(f, "prepare", LC_OK);
}

static void test_prepare_intent_is_not_a_yes_vote_after_reopen(void **state) {
  fixture *f = (fixture *)*state;
  lc_txn_participant participant;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  enroll(f, 0U);
  progress(f, 0U, LC_XA_PREPARE_SENT);
  reopen(f);
  replay(f, "prepare", LC_OK);
  decision(f, 0, LC_ERR_INVALID);
  lc_txn_decision_req_init(&request);
  memset(&result, 0, sizeof(result));
  participant.ns = "default";
  participant.key = "late-native";
  participant.backend_hash = NULL;
  request.txn_id = f->branches[0].txn_id;
  request.participants = &participant;
  request.participant_count = 1U;
  assert_int_equal(lc_txn_prepare(f->client, &request, &result, &f->error),
                   LC_ERR_INVALID);
  lc_txn_decision_res_cleanup(&result);
  clear_error(f);
  progress(f, 0U, LC_XA_PREPARED);
  replay(f, "commit", LC_ERR_PENDING);
}

static void
test_expired_prepare_preserves_abort_until_external_cleanup(void **state) {
  fixture *f = (fixture *)*state;
  lc_txn_decision_req request;
  lc_txn_decision_res result;
  enroll(f, 0U);
  stage_native(f);
  lc_txn_decision_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.txn_id = f->branches[0].txn_id;
  request.expires_at_unix = 1;
  assert_int_equal(lc_txn_prepare(f->client, &request, &result, &f->error),
                   LC_OK);
  lc_txn_decision_res_cleanup(&result);
  f->branches[0].phase = LC_XA_PREPARE_SENT;
  assert_int_equal(
      lc_pouch_client_xa_progress(f->client, &f->branches[0], NULL, &f->error),
      LC_ERR_INVALID);
  clear_error(f);
  replay(f, "rollback", LC_ERR_PENDING);
  reopen(f);
  replay(f, "rollback", LC_ERR_PENDING);
  progress(f, 0U, LC_XA_ROLLBACK_SENT);
  progress(f, 0U, LC_XA_ABORTED);
  replay(f, "rollback", LC_OK);
  visible(f, 0);
}

static void
test_shared_client_observes_durable_external_completion(void **state) {
  fixture *f = (fixture *)*state;
  lc_client *original;
  lc_client_close(f->client);
  f->client = NULL;
  assert_true(snprintf(f->endpoint, sizeof(f->endpoint),
                       "pouch://%s?single_writer=false", f->root) > 0);
  open_client(f);
  enroll(f, 0U);
  stage_native(f);
  prepare_branch(f, 0U);
  decision(f, 0, LC_ERR_PENDING);
  original = f->client;
  f->client = NULL;
  open_client(f);
  f->peer = original;
  replay(f, "commit", LC_ERR_PENDING);
  visible(f, 0);
  progress(f, 0U, LC_XA_COMMIT_SENT);
  progress(f, 0U, LC_XA_COMMITTED);
  original = f->client;
  f->client = f->peer;
  f->peer = original;
  replay(f, "commit", LC_OK);
  visible(f, 1);
}

static void test_corrupt_descriptor_never_opens_application_gate(void **state) {
  fixture *f = (fixture *)*state;
  lc_pouch_state_read_result current;
  lc_pouch_state_write_result written;
  lc_source *source = NULL;
  lc_sink *sink = NULL;
  const void *bytes = NULL;
  unsigned char damaged[512];
  size_t length = 0U;
  size_t variant;
  char key[LC_POUCH_XA_KEY_SIZE];
  memset(&current, 0, sizeof(current));
  enroll(f, 0U);
  stage_native(f);
  prepare_branch(f, 0U);
  decision(f, 0, LC_ERR_PENDING);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  assert_int_equal(
      lc_pouch_state_read(store(f), ".xa", key, &current, &f->error), LC_OK);
  assert_int_equal(lc_sink_to_memory(&sink, &f->error), LC_OK);
  assert_int_equal(lc_copy(current.body, sink, NULL, &f->error), LC_OK);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &f->error),
                   LC_OK);
  assert_true(length > 4U && length + 1U < sizeof(damaged));
  lc_pouch_state_read_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &current);
  for (variant = 0U; variant < 4U; ++variant) {
    size_t damaged_length = length;
    memcpy(damaged, bytes, length);
    if (variant == 0U)
      damaged_length -= 1U;
    else if (variant == 1U)
      damaged[damaged_length++] = 0U;
    else if (variant == 2U)
      damaged[4U + LC_XID_STRING_LENGTH] = 'x';
    else
      damaged[length - 1U] = 0x80U;
    memset(&written, 0, sizeof(written));
    assert_int_equal(
        lc_source_from_memory(damaged, damaged_length, &source, &f->error),
        LC_OK);
    assert_int_equal(lc_pouch_state_write(store(f), ".xa", key, source, NULL,
                                          &written, &f->error),
                     LC_OK);
    source->close(source);
    lc_pouch_state_write_result_cleanup(
        &((lc_client_handle *)f->client)->allocator, &written);
    replay(f, NULL, LC_ERR_PROTOCOL);
    visible(f, 0);
  }
  sink->close(sink);
}

static void test_native_lease_vote_waits_for_external_completion(void **state) {
  fixture *f = (fixture *)*state;
  lc_acquire_req acquire;
  lc_release_req release;
  lc_source *source = NULL;
  const char value[] = "{\"value\":42}";
  enroll(f, 0U);
  lc_acquire_req_init(&acquire);
  acquire.key = "business";
  acquire.owner = "xa-unit";
  acquire.ttl_seconds = 30L;
  acquire.txn_id = f->branches[0].txn_id;
  assert_int_equal(lc_acquire(f->client, &acquire, &f->lease, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory(value, sizeof(value) - 1U, &source, &f->error),
      LC_OK);
  assert_int_equal(lc_lease_update(f->lease, source, NULL, &f->error), LC_OK);
  source->close(source);
  prepare_branch(f, 0U);
  lc_release_req_init(&release);
  assert_int_equal(lc_lease_release(f->lease, &release, &f->error),
                   LC_ERR_PENDING);
  clear_error(f);
  replay(f, "commit", LC_ERR_PENDING);
  visible(f, 0);
  progress(f, 0U, LC_XA_COMMIT_SENT);
  progress(f, 0U, LC_XA_COMMITTED);
  assert_int_equal(lc_lease_release(f->lease, &release, &f->error), LC_OK);
  f->lease = NULL;
  visible(f, 1);
}

static void
test_public_store_pins_client_and_preserves_resource_identity(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_store *journal = NULL;
  lc_xa_branch_record loaded;
  lc_xa_branch_record wrong;
  int prepared = -1;
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  assert_int_equal(
      lc_xa_store_register_blocking(journal, &f->branches[0], &f->error),
      LC_OK);
  assert_int_equal(
      lc_xa_store_register_blocking(journal, &f->branches[0], &f->error),
      LC_OK);
  wrong = f->branches[0];
  strcpy(wrong.role_oid, "42");
  assert_int_equal(lc_xa_store_register_blocking(journal, &wrong, NULL),
                   LC_ERR_PROTOCOL);
  assert_int_equal(
      lc_xa_store_enroll_blocking(journal, &f->branches[0], &f->error), LC_OK);
  assert_int_equal(lc_xa_store_read_blocking(journal, f->branches[0].txn_id,
                                             f->branches[0].branch_id, &loaded,
                                             &prepared, &f->error),
                   LC_OK);
  assert_int_equal(prepared, 0);
  f->branches[0].phase = LC_XA_PREPARE_SENT;
  assert_int_equal(
      lc_xa_store_progress_blocking(journal, &f->branches[0], &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_read_blocking(journal, loaded.txn_id,
                                             loaded.branch_id, &loaded,
                                             &prepared, &f->error),
                   LC_OK);
  assert_int_equal(prepared, 0);
  f->branches[0].phase = LC_XA_PREPARED;
  f->branches[0].prepared_observed = 1;
  assert_int_equal(
      lc_xa_store_progress_blocking(journal, &f->branches[0], &f->error),
      LC_OK);
  lc_client_close(f->client);
  f->client = NULL;
  assert_int_equal(lc_xa_store_read_blocking(journal, f->branches[0].txn_id,
                                             f->branches[0].branch_id, &loaded,
                                             &prepared, &f->error),
                   LC_OK);
  assert_int_equal(prepared, 1);
  assert_string_equal(loaded.full_xid, f->branches[0].full_xid);
  lc_xa_store_close(journal);
  open_client(f);
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  assert_int_equal(
      lc_xa_store_register_blocking(journal, &f->branches[0], &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_register_blocking(journal, &wrong, NULL),
                   LC_ERR_PROTOCOL);
  lc_xa_store_close(journal);
}

static void
test_public_store_rejects_completion_contrary_to_decision(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_store *journal = NULL;
  lc_xa_branch_record loaded;
  enroll(f, 0U);
  prepare_branch(f, 0U);
  f->branches[0].phase = LC_XA_ROLLBACK_SENT;
  assert_int_equal(
      lc_pouch_xa_branch_write(store(f), &f->branches[0], &f->error), LC_OK);
  f->branches[0].phase = LC_XA_ABORTED;
  f->branches[0].evidence = LC_XA_EVIDENCE_NATIVE;
  f->branches[0].completed_at_unix = (lc_unix_seconds)time(NULL);
  assert_int_equal(
      lc_pouch_xa_branch_write(store(f), &f->branches[0], &f->error), LC_OK);
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  assert_int_equal(lc_xa_store_read_blocking(journal, f->branches[0].txn_id,
                                             f->branches[0].branch_id, &loaded,
                                             NULL, &f->error),
                   LC_ERR_PROTOCOL);
  lc_xa_store_close(journal);
}

static void
test_native_prepare_observation_is_durable_without_yes(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_store *journal = NULL;
  lc_xa_branch_record loaded;
  int yes = -1;
  enroll(f, 0U);
  progress(f, 0U, LC_XA_PREPARE_SENT);
  decision(f, 1, LC_ERR_PENDING);
  progress(f, 0U, LC_XA_ROLLBACK_SENT);
  f->branches[0].prepared_observed = 1;
  assert_int_equal(
      lc_pouch_client_xa_progress(f->client, &f->branches[0], NULL, &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  assert_int_equal(lc_xa_store_read_blocking(journal, f->branches[0].txn_id,
                                             f->branches[0].branch_id, &loaded,
                                             &yes, &f->error),
                   LC_OK);
  assert_int_equal(yes, 0);
  assert_int_equal(loaded.prepared_observed, 1U);
  f->branches[0].prepared_observed = 0;
  assert_int_equal(
      lc_pouch_client_xa_progress(f->client, &f->branches[0], NULL, &f->error),
      LC_ERR_PROTOCOL);
  clear_error(f);
  f->branches[0].prepared_observed = 1;
  progress(f, 0U, LC_XA_ABORTED);
  replay(f, "rollback", LC_OK);
  lc_xa_store_close(journal);
}

typedef struct driver_fixture {
  lc_xa_store *journal;
  lc_xa_branch_record identity;
  int opens, closes, branch_closes, op_closes;
  int advances, completed, creates_branch, cancelled, reject_start;
} driver_fixture;
static int fixture_driver_open(lc_xa_store *journal, const void *config,
                               void **out, lc_error *error) {
  driver_fixture *d = (driver_fixture *)config;
  (void)error;
  d->journal = journal;
  ++d->opens;
  *out = d;
  return LC_OK;
}
static void fixture_driver_close(void *context) {
  ++((driver_fixture *)context)->closes;
}
static const lc_xa_branch_record *fixture_driver_identity(void *context) {
  return &((driver_fixture *)context)->identity;
}
static int fixture_driver_start(void *context, const char *txn,
                                int64_t deadline, void **out, lc_error *error) {
  driver_fixture *d = (driver_fixture *)context;
  (void)txn;
  (void)deadline;
  (void)error;
  *out = NULL;
  if (d->reject_start)
    return LC_ERR_NOMEM;
  d->advances = d->completed = d->cancelled = 0;
  d->creates_branch = 1;
  *out = d;
  return LC_OK;
}
static int fixture_driver_register(void *context, int64_t deadline, void **out,
                                   lc_error *error) {
  driver_fixture *d = (driver_fixture *)context;
  int rc = lc_xa_store_register_blocking(d->journal, &d->identity, error);
  if (rc != LC_OK)
    return rc;
  rc = fixture_driver_start(context, NULL, deadline, out, error);
  d->creates_branch = 0;
  return rc;
}
static int fixture_driver_prepare(void *context, int64_t deadline, void **out,
                                  lc_error *error) {
  int rc = fixture_driver_start(context, NULL, deadline, out, error);
  ((driver_fixture *)context)->creates_branch = 0;
  return rc;
}
static int fixture_driver_resolve(void *context, int commit, int64_t deadline,
                                  void **out, lc_error *error) {
  (void)commit;
  return fixture_driver_prepare(context, deadline, out, error);
}
static int fixture_driver_recover(void *context, const char *txn,
                                  const char *branch, int commit,
                                  int64_t deadline, void **out,
                                  lc_error *error) {
  (void)branch;
  (void)commit;
  return fixture_driver_start(context, txn, deadline, out, error);
}
static int fixture_driver_status(void *context, const char *txn,
                                 const char *branch, int64_t deadline,
                                 void **out, lc_error *error) {
  (void)txn;
  (void)branch;
  return fixture_driver_prepare(context, deadline, out, error);
}
static int fixture_driver_inventory(void *context, const char *after,
                                    size_t limit, int64_t deadline, void **out,
                                    lc_error *error) {
  (void)after;
  (void)limit;
  return fixture_driver_prepare(context, deadline, out, error);
}
static int fixture_driver_advance(void *context, lc_error *error) {
  driver_fixture *d = (driver_fixture *)context;
  (void)error;
  if (++d->advances == 1 && !d->completed)
    return LC_ERR_PENDING;
  d->completed = 1;
  return d->cancelled ? LC_ERR_TRANSPORT : LC_OK;
}
static int fixture_driver_wait(void *context, lc_xa_wait *out,
                               lc_error *error) {
  (void)context;
  (void)error;
  out->fd = 7;
  out->events = 1U;
  out->deadline_ms = 1000;
  return LC_OK;
}
static int fixture_driver_result(void *context, lc_xa_result *out,
                                 void **branch) {
  driver_fixture *d = (driver_fixture *)context;
  if (!d->completed)
    return 0;
  memset(out, 0, sizeof(*out));
  out->record = d->identity;
  *branch = d->creates_branch && !d->cancelled ? context : NULL;
  return 1;
}
static int fixture_driver_cancel(void *context, lc_error *error) {
  driver_fixture *d = (driver_fixture *)context;
  (void)error;
  if (!d->completed) {
    d->cancelled = d->completed = 1;
  }
  return LC_OK;
}
static void fixture_driver_op_close(void *context) {
  ++((driver_fixture *)context)->op_closes;
}
static int fixture_driver_borrow(void *context, lc_xa_borrow **out,
                                 lc_error *error) {
  (void)context;
  (void)out;
  (void)error;
  return LC_ERR_INVALID;
}
static void fixture_driver_branch_close(void *context) {
  ++((driver_fixture *)context)->branch_closes;
}
static const lc_xa_driver fixture_driver = {
    sizeof(lc_xa_driver),       LC_XA_DRIVER_ABI,
    LC_XA_RESOURCE_POSTGRES,    fixture_driver_open,
    fixture_driver_close,       fixture_driver_identity,
    fixture_driver_register,    fixture_driver_start,
    fixture_driver_prepare,     fixture_driver_resolve,
    fixture_driver_recover,     fixture_driver_status,
    fixture_driver_inventory,   fixture_driver_advance,
    fixture_driver_wait,        fixture_driver_result,
    fixture_driver_cancel,      fixture_driver_op_close,
    fixture_driver_borrow,      fixture_driver_identity,
    fixture_driver_branch_close};

typedef struct driver_foreign {
  lc_xa_resource *resource;
  int passed;
} driver_foreign;
static void *fixture_driver_foreign_thread(void *context) {
  driver_foreign *foreign = (driver_foreign *)context;
  lc_xa_operation *op = (lc_xa_operation *)1;
  foreign->passed =
      lc_xa_resource_branch_start(foreign->resource, "invalid", 0, &op, NULL) ==
          LC_ERR_INVALID &&
      op == NULL &&
      lc_xa_resource_close_blocking(foreign->resource, NULL) == LC_ERR_INVALID;
  return NULL;
}

static void
test_driver_contract_pins_closed_owners_and_preallocates_branch(void **state) {
  fixture *f = (fixture *)*state;
  driver_fixture d;
  lc_xa_driver table = fixture_driver;
  lc_xa_resource *resource = NULL;
  lc_xa_operation *op = NULL;
  lc_xa_branch *branch = NULL;
  lc_xa_branch *duplicate = NULL;
  lc_xa_store *journal = NULL;
  lc_xa_wait wait;
  driver_foreign foreign;
  pthread_t thread;
  pid_t child;
  int status;
  memset(&d, 0, sizeof(d));
  d.identity = f->branches[0];
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  table.abi_version++;
  assert_int_equal(
      lc_xa_resource_open(journal, &table, &d, &resource, &f->error),
      LC_ERR_INVALID);
  assert_null(resource);
  assert_int_equal(d.opens, 0);
  table = fixture_driver;
  table.prepare_start = NULL;
  assert_int_equal(
      lc_xa_resource_open(journal, &table, &d, &resource, &f->error),
      LC_ERR_INVALID);
  table = fixture_driver;
  assert_int_equal(
      lc_xa_resource_open(journal, &table, &d, &resource, &f->error), LC_OK);
  memset(&table, 0, sizeof(table)); /* The resource copied its contract. */
  foreign.resource = resource;
  foreign.passed = 0;
  assert_int_equal(
      pthread_create(&thread, NULL, fixture_driver_foreign_thread, &foreign),
      0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(foreign.passed, 1);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    lc_xa_operation *inherited = (lc_xa_operation *)1;
    _exit(lc_xa_resource_register_start(resource, 0, &inherited, NULL) ==
                      LC_ERR_INVALID &&
                  inherited == NULL &&
                  lc_xa_resource_identity(resource) == NULL &&
                  lc_xa_resource_close_blocking(resource, NULL) ==
                      LC_ERR_INVALID
              ? 0
              : 1);
  }
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  lc_xa_store_close(journal);
  lc_client_close(f->client);
  f->client = NULL;
  assert_int_equal(lc_xa_resource_register_start(resource, 0, &op, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_PENDING);
  assert_null(lc_xa_operation_result(op));
  assert_int_equal(lc_xa_operation_wait(op, &wait, &f->error), LC_OK);
  assert_int_equal(wait.fd, 7);
  assert_int_equal(wait.events, 1U);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
  d.reject_start = 1;
  assert_int_equal(lc_xa_resource_branch_start(resource, d.identity.txn_id, 0,
                                               &op, &f->error),
                   LC_ERR_NOMEM);
  assert_null(op);
  d.reject_start = 0;
  assert_int_equal(lc_xa_resource_branch_start(resource, d.identity.txn_id, 0,
                                               &op, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_operation_take_branch(op, &branch, &f->error),
                   LC_ERR_INVALID);
  assert_null(branch);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_PENDING);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_cancel(op, &f->error),
                   LC_OK); /* Completed no-op. */
  assert_int_equal(lc_xa_operation_take_branch(op, &branch, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_take_branch(op, &duplicate, &f->error),
                   LC_ERR_INVALID);
  assert_null(duplicate);
  assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
  assert_string_equal(lc_xa_branch_record_get(branch)->txn_id,
                      d.identity.txn_id);
  assert_int_equal(lc_xa_branch_prepare_start(branch, 0, &op, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_branch_close(branch, &f->error), LC_OK);
  assert_int_equal(lc_xa_resource_close_blocking(resource, &f->error), LC_OK);
  assert_int_equal(d.branch_closes, 0);
  assert_int_equal(d.closes, 0);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_PENDING);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
  assert_int_equal(d.branch_closes, 1);
  assert_int_equal(d.closes, 1);
}

static void test_driver_contract_pins_transferred_branch_until_operation_close(
    void **state) {
  fixture *f = (fixture *)*state;
  driver_fixture d;
  lc_xa_resource *resource;
  lc_xa_operation *op;
  lc_xa_branch *branch, *duplicate;
  lc_xa_store *journal;
  int recover;
  for (recover = 0; recover != 2; ++recover) {
    memset(&d, 0, sizeof(d));
    d.identity = f->branches[0];
    assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
    assert_int_equal(
        lc_xa_resource_open(journal, &fixture_driver, &d, &resource, &f->error),
        LC_OK);
    lc_xa_store_close(journal);
    assert_int_equal(
        recover ? lc_xa_resource_recover_start(resource, d.identity.txn_id,
                                               d.identity.branch_id, 0, 0, &op,
                                               &f->error)
                : lc_xa_resource_branch_start(resource, d.identity.txn_id, 0,
                                              &op, &f->error),
        LC_OK);
    assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_PENDING);
    assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_OK);
    assert_int_equal(lc_xa_operation_take_branch(op, &branch, &f->error),
                     LC_OK);
    assert_int_equal(lc_xa_operation_take_branch(op, &duplicate, &f->error),
                     LC_ERR_INVALID);
    assert_null(duplicate);
    assert_int_equal(lc_xa_branch_close(branch, &f->error), LC_OK);
    assert_int_equal(lc_xa_resource_close_blocking(resource, &f->error), LC_OK);
    assert_int_equal(d.branch_closes, 0);
    assert_int_equal(d.op_closes, 0);
    assert_int_equal(d.closes, 0);
    assert_non_null(lc_xa_operation_result(op));
    assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
    assert_int_equal(d.op_closes, 1);
    assert_int_equal(d.branch_closes, 1);
    assert_int_equal(d.closes, 1);
  }
}

static void
test_driver_contract_reclaims_unclaimed_and_cancelled_branches(void **state) {
  fixture *f = (fixture *)*state;
  driver_fixture d;
  lc_xa_resource *resource;
  lc_xa_operation *op;
  lc_xa_branch *branch;
  lc_xa_store *journal;
  memset(&d, 0, sizeof(d));
  d.identity = f->branches[0];
  assert_int_equal(lc_xa_store_open(f->client, &journal, &f->error), LC_OK);
  assert_int_equal(
      lc_xa_resource_open(journal, &fixture_driver, &d, &resource, &f->error),
      LC_OK);
  lc_xa_store_close(journal);
  assert_int_equal(lc_xa_resource_branch_start(resource, d.identity.txn_id, 0,
                                               &op, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_PENDING);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
  assert_int_equal(d.branch_closes,
                   1); /* Never taken, but no native owner leak. */
  assert_int_equal(lc_xa_resource_branch_start(resource, d.identity.txn_id, 0,
                                               &op, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_operation_cancel(op, &f->error), LC_OK);
  assert_int_equal(lc_xa_operation_take_branch(op, &branch, &f->error),
                   LC_ERR_INVALID);
  assert_null(branch);
  assert_int_equal(lc_xa_operation_advance(op, &f->error), LC_ERR_TRANSPORT);
  assert_int_equal(lc_xa_operation_close_blocking(op, &f->error), LC_OK);
  assert_int_equal(d.branch_closes, 1);
  assert_int_equal(lc_xa_resource_close_blocking(resource, &f->error), LC_OK);
  assert_int_equal(d.closes, 1);
}

static int tc_clock(clockid_t clock_id, struct timespec *out, void *context) {
  if (clock_id != CLOCK_REALTIME)
    return clock_gettime(clock_id, out);
  out->tv_sec = *(time_t *)context;
  out->tv_nsec = 0L;
  return 0;
}
static void tc_acquire(fixture *f, const char *owner_id, lc_tc_term term,
                       int expected) {
  lc_tc_lease_acquire_req request;
  lc_tc_lease_acquire_res result;
  lc_tc_lease_acquire_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.candidate_id = owner_id;
  request.candidate_endpoint = "pouch://xa-coordinator";
  request.term = term;
  request.ttl_ms = 60000L;
  assert_int_equal(lc_tc_lease_acquire(f->client, &request, &result, &f->error),
                   LC_OK);
  assert_int_equal(result.granted, expected);
  lc_tc_lease_acquire_res_cleanup(&result);
}
static void tc_release(fixture *f, const char *owner_id, lc_tc_term term,
                       int expected) {
  lc_tc_lease_release_req request;
  lc_tc_lease_release_res result;
  lc_tc_lease_release_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.leader_id = owner_id;
  request.term = term;
  assert_int_equal(lc_tc_lease_release(f->client, &request, &result, &f->error),
                   LC_OK);
  assert_int_equal(result.released, expected);
  lc_tc_lease_release_res_cleanup(&result);
}
static void tc_renew(fixture *f, const char *owner_id, lc_tc_term term,
                     int expected) {
  lc_tc_lease_renew_req request;
  lc_tc_lease_renew_res result;
  lc_tc_lease_renew_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.leader_id = owner_id;
  request.term = term;
  request.ttl_ms = 60000L;
  assert_int_equal(lc_tc_lease_renew(f->client, &request, &result, &f->error),
                   LC_OK);
  assert_int_equal(result.renewed, expected);
  lc_tc_lease_renew_res_cleanup(&result);
}
static void test_tc_expired_owner_cannot_reuse_an_old_epoch(void **state) {
  fixture *f = (fixture *)*state;
  time_t now = time(NULL);
  lc_tc_leader_res leader;
  lc_pouch_test_queue_time_context = &now;
  lc_pouch_test_queue_clock_gettime = tc_clock;
  tc_acquire(f, "original", 5U, 1);
  tc_acquire(f, "original", 5U, 1);
  tc_renew(f, "original", 5U, 1);
  now += 61;
  tc_renew(f, "original", 5U, 0);
  tc_acquire(f, "stale", 4U, 0);
  memset(&leader, 0, sizeof(leader));
  assert_int_equal(lc_tc_leader(f->client, &leader, &f->error), LC_OK);
  assert_string_equal(leader.leader_id, "");
  assert_int_equal(leader.term, 5U);
  lc_tc_leader_res_cleanup(&leader);
  tc_acquire(f, "original", 5U, 0);
  tc_acquire(f, "replacement", 5U, 0);
  tc_acquire(f, "replacement", 6U, 1);
  tc_renew(f, "original", 5U, 0);
  tc_renew(f, "replacement", 6U, 1);
  tc_release(f, "original", 5U, 0);
  tc_release(f, "replacement", 6U, 1);
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
}
static void test_tc_release_preserves_epoch_across_reopen(void **state) {
  fixture *f = (fixture *)*state;
  lc_tc_leader_res leader;
  tc_acquire(f, "original", 7U, 1);
  tc_release(f, "original", 7U, 1);
  lc_client_close(f->client);
  f->client = NULL;
  open_client(f);
  tc_acquire(f, "original", 7U, 0);
  memset(&leader, 0, sizeof(leader));
  assert_int_equal(lc_tc_leader(f->client, &leader, &f->error), LC_OK);
  assert_string_equal(leader.leader_id, "");
  assert_int_equal(leader.expires_at_unix, 0);
  assert_int_equal(leader.term, 7U);
  lc_tc_leader_res_cleanup(&leader);
  tc_acquire(f, "stale", 6U, 0);
  tc_acquire(f, "replacement", 8U, 1);
  tc_release(f, "replacement", 8U, 1);
}

static void owned_journals(fixture *f) {
  char owner[LC_XID_STRING_SIZE];
  size_t i;
  for (i = 0U; i < 2U; ++i) {
    assert_int_equal(lc_xid_new(owner, &f->error), LC_OK);
    assert_int_equal(
        lc_xa_store_open_owned(f->client, owner, &f->journals[i], &f->error),
        LC_OK);
  }
  assert_int_equal(lc_xa_store_open(f->client, &f->journals[2], &f->error),
                   LC_OK);
}
static void owned_claim(fixture *f, size_t actor, int recover_mode,
                        int decision_value, lc_tc_term term) {
  lc_xa_snapshot result;
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[actor],
                                              f->branches[0].txn_id, 30L,
                                              recover_mode, &result, &f->error),
                   LC_OK);
  assert_int_equal(result.decision, decision_value);
  assert_int_equal(result.term, term);
  assert_string_equal(result.txn_id, f->branches[0].txn_id);
  lc_xa_snapshot_cleanup(&result);
}
static void owned_progress(fixture *f, size_t actor, unsigned char phase) {
  lc_xa_branch_record *branch = &f->branches[0];
  branch->phase = phase;
  if (phase == LC_XA_PREPARED)
    branch->prepared_observed = 1;
  if (phase >= LC_XA_COMMITTED) {
    branch->evidence = LC_XA_EVIDENCE_NATIVE;
    branch->completed_at_unix = (lc_unix_seconds)time(NULL);
  }
  assert_int_equal(
      lc_xa_store_progress_blocking(f->journals[actor], branch, &f->error),
      LC_OK);
}
static int fail_owner_journal(void *context, lc_error *error) {
  (void)context;
  return lc_error_set(error, LC_ERR_NOMEM, 0L, "injected owner journal failure",
                      NULL, NULL, NULL);
}
static void test_managed_owner_exclusion_and_commit_takeover(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_snapshot result;
  lc_xa_branch_record stale;
  size_t i;
  owned_journals(f);
  owned_claim(f, 0U, 0, 0, 1U);
  {
    lc_txn_decision_req request;
    lc_txn_decision_res response;
    lc_txn_participant participant;
    lc_txn_decision_req_init(&request);
    memset(&response, 0, sizeof(response));
    participant.ns = "default";
    participant.key = "foreign-pouch";
    participant.backend_hash =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    request.txn_id = f->branches[0].txn_id;
    request.tc_term = 1U;
    request.participants = &participant;
    request.participant_count = 1U;
    assert_int_equal(lc_txn_prepare(f->client, &request, &response, &f->error),
                     LC_ERR_INVALID);
    lc_txn_decision_res_cleanup(&response);
  }
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[1],
                                              f->branches[0].txn_id, 30L, 1,
                                              &result, &f->error),
                   LC_ERR_PENDING);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[2], &f->branches[0], &f->error),
      LC_ERR_INVALID);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[0], f->branches[0].txn_id, 1, &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(lc_xa_store_seal_blocking(f->journals[0],
                                             f->branches[0].txn_id, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[1], &f->error),
      LC_ERR_INVALID);
  owned_progress(f, 0U, LC_XA_PREPARE_SENT);
  owned_progress(f, 0U, LC_XA_PREPARED);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[0], f->branches[0].txn_id, &f->error),
                   LC_OK);
  owned_claim(f, 1U, 1, 1, 2U);
  stale = f->branches[0];
  stale.phase = LC_XA_COMMIT_SENT;
  for (i = 0U; i < 3U; i += 2U)
    assert_int_equal(
        lc_xa_store_progress_blocking(f->journals[i], &stale, &f->error),
        LC_ERR_INVALID);
  assert_int_equal(lc_xa_store_renew_blocking(
                       f->journals[0], f->branches[0].txn_id, 30L, &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[0], f->branches[0].txn_id, &f->error),
                   LC_ERR_INVALID);
  owned_progress(f, 1U, LC_XA_COMMIT_SENT);
  owned_progress(f, 1U, LC_XA_COMMITTED);
  owned_claim(f, 1U, 1, 1,
              2U); /* Matching repeat neither restarts nor advances epoch. */
  replay(f, "commit", LC_OK);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[1], f->branches[0].txn_id, 0, &f->error),
                   LC_ERR_INVALID);
}
static void
test_managed_owner_expiry_preserves_shared_staging_until_abort(void **state) {
  fixture *f = (fixture *)*state;
  lc_acquire_req acquire;
  lc_source *body;
  lc_xa_snapshot result;
  time_t now = time(NULL);
  lc_client *original;
  lc_client_close(f->client);
  f->client = NULL;
  (void)snprintf(f->endpoint, sizeof(f->endpoint),
                 "pouch://%s?single_writer=false", f->root);
  open_client(f);
  owned_journals(f);
  lc_pouch_test_queue_time_context = &now;
  lc_pouch_test_queue_clock_gettime = tc_clock;
  owned_claim(f, 0U, 0, 0, 1U);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
      LC_OK);
  lc_acquire_req_init(&acquire);
  acquire.owner = "owned-pouch";
  acquire.key = "business";
  acquire.txn_id = f->branches[0].txn_id;
  acquire.ttl_seconds = 5L;
  assert_int_equal(lc_acquire(f->client, &acquire, &f->lease, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
  body->close(body);
  owned_progress(f, 0U, LC_XA_PREPARE_SENT);
  now += 6;
  original = f->client;
  f->client = NULL;
  open_client(f);
  f->peer = original;
  replay(f, "prepare", LC_ERR_PENDING);
  visible(f, 0);
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[1],
                                              f->branches[0].txn_id, 30L, 1,
                                              &result, &f->error),
                   LC_ERR_PENDING);
  now += 25;
  assert_int_equal(lc_xa_store_renew_blocking(
                       f->journals[0], f->branches[0].txn_id, 30L, &f->error),
                   LC_ERR_INVALID);
  owned_claim(f, 1U, 1, 2, 2U);
  replay(f, "rollback", LC_ERR_PENDING);
  owned_progress(f, 1U, LC_XA_ROLLBACK_SENT);
  owned_progress(f, 1U, LC_XA_ABORTED);
  replay(f, "rollback", LC_OK);
  visible(f, 0);
}
static void
test_managed_owner_begin_failure_retains_recoverable_identity(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_snapshot result;
  owned_journals(f);
  lc_pouch_test_before_txn_decision_hook = fail_owner_journal;
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[0],
                                              f->branches[0].txn_id, 30L, 0,
                                              &result, &f->error),
                   LC_ERR_NOMEM);
  lc_pouch_test_before_txn_decision_hook = NULL;
  assert_int_equal(lc_xa_store_snapshot_blocking(f->journals[0],
                                                 f->branches[0].txn_id, &result,
                                                 &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[2], &f->branches[0], &f->error),
      LC_ERR_INVALID);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[0], f->branches[0].txn_id, &f->error),
                   LC_OK);
  owned_claim(f, 1U, 1, 2, 2U);
  replay(f, "rollback", LC_OK);
}
static void
test_managed_owner_publication_failure_keeps_terminal_decision(void **state) {
  fixture *f = (fixture *)*state;
  lc_xa_snapshot result;
  int commit;
  owned_journals(f);
  for (commit = 0; commit <= 1; ++commit) {
    assert_int_equal(lc_xid_new(f->branches[0].txn_id, &f->error), LC_OK);
    (void)snprintf(f->branches[0].gid, sizeof(f->branches[0].gid), "lc-%s-%s",
                   f->branches[0].txn_id, f->branches[0].branch_id);
    f->branches[0].phase = LC_XA_ENROLLED;
    f->branches[0].prepared_observed = f->branches[0].evidence = 0;
    f->branches[0].completed_at_unix = 0;
    owned_claim(f, 0U, 0, 0, 1U);
    assert_int_equal(
        lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
        LC_OK);
    owned_progress(f, 0U, LC_XA_PREPARE_SENT);
    if (commit)
      owned_progress(f, 0U, LC_XA_PREPARED);
    assert_int_equal(lc_xa_store_release_blocking(
                         f->journals[0], f->branches[0].txn_id, &f->error),
                     LC_OK);
    lc_pouch_test_before_xa_owner_write_hook = fail_owner_journal;
    assert_int_equal(lc_xa_store_claim_blocking(f->journals[1],
                                                f->branches[0].txn_id, 30L, 1,
                                                &result, &f->error),
                     LC_ERR_NOMEM);
    lc_pouch_test_before_xa_owner_write_hook = NULL;
    assert_int_equal(lc_xa_store_snapshot_blocking(f->journals[1],
                                                   f->branches[0].txn_id,
                                                   &result, &f->error),
                     LC_OK);
    assert_int_equal(result.decision, commit ? 1 : 2);
    assert_int_equal(result.term, 2U);
    lc_xa_snapshot_cleanup(&result);
    assert_int_equal(lc_xa_store_progress_blocking(f->journals[0],
                                                   &f->branches[0], &f->error),
                     LC_ERR_INVALID);
    owned_claim(f, 1U, 1, commit ? 1 : 2, 3U);
    owned_progress(f, 1U, commit ? LC_XA_COMMIT_SENT : LC_XA_ROLLBACK_SENT);
    owned_progress(f, 1U, commit ? LC_XA_COMMITTED : LC_XA_ABORTED);
    replay(f, commit ? "commit" : "rollback", LC_OK);
  }
}

static void test_managed_pouch_vote_and_independent_transactions(void **state) {
  fixture *f = (fixture *)*state;
  lc_acquire_req request;
  lc_source *body;
  lc_xa_snapshot result;
  char independent[LC_XID_STRING_SIZE];
  owned_journals(f);
  assert_int_equal(lc_xid_new(independent, &f->error), LC_OK);
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[1], independent, 30L,
                                              1, &result, &f->error),
                   LC_ERR_INVALID);
  owned_claim(f, 0U, 0, 0, 1U);
  assert_int_equal(lc_xa_store_claim_blocking(f->journals[1], independent, 30L,
                                              0, &result, &f->error),
                   LC_OK);
  assert_int_equal(result.term, 1U);
  assert_int_equal(result.decision, 0);
  lc_xa_snapshot_cleanup(&result);
  assert_int_equal(
      lc_xa_store_decide_blocking(f->journals[1], independent, 0, &f->error),
      LC_OK);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
      LC_OK);
  lc_acquire_req_init(&request);
  request.key = "business";
  request.owner = "owned-pouch";
  request.ttl_seconds = 30L;
  request.txn_id = f->branches[0].txn_id;
  assert_int_equal(lc_acquire(f->client, &request, &f->lease, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
  body->close(body);
  assert_int_equal(lc_xa_store_prepare_pouch_blocking(
                       f->journals[0], request.txn_id, &f->error),
                   LC_ERR_INVALID);
  assert_int_equal(
      lc_xa_store_seal_blocking(f->journals[0], request.txn_id, &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_prepare_pouch_blocking(
                       f->journals[0], request.txn_id, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_xa_store_decide_blocking(f->journals[0], request.txn_id, 1, &f->error),
      LC_ERR_INVALID);
  visible(f, 0);
  owned_progress(f, 0U, LC_XA_PREPARE_SENT);
  owned_progress(f, 0U, LC_XA_PREPARED);
  assert_int_equal(
      lc_xa_store_decide_blocking(f->journals[0], request.txn_id, 1, &f->error),
      LC_ERR_PENDING);
  visible(f, 0);
  owned_progress(f, 0U, LC_XA_COMMIT_SENT);
  owned_progress(f, 0U, LC_XA_COMMITTED);
  assert_int_equal(
      lc_xa_store_decide_blocking(f->journals[0], request.txn_id, 1, &f->error),
      LC_OK);
  assert_int_equal(lc_xa_store_prepare_pouch_blocking(
                       f->journals[0], request.txn_id, &f->error),
                   LC_OK);
  visible(f, 1);
  replay(f, "commit", LC_OK);
  assert_int_equal(lc_xa_store_snapshot_blocking(f->journals[0], request.txn_id,
                                                 &result, &f->error),
                   LC_OK);
  assert_int_equal(result.branch_count, 1U);
  assert_int_equal(result.branches[0].phase, LC_XA_COMMITTED);
  lc_xa_snapshot_cleanup(&result);
}

static int64_t coordinator_now(void) {
  struct timespec now;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &now), 0);
  return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static int coordinator_await(fixture *f) {
  int64_t deadline = coordinator_now() + 10000;
  for (;;) {
    lc_xa_wait wait;
    struct pollfd fd;
    int rc;
    clear_error(f);
    rc = lc_xa_coordinator_operation_advance(f->operation, &f->error);
    if (rc != LC_ERR_PENDING ||
        lc_xa_coordinator_operation_result(f->operation)->done)
      return rc;
    assert_true(coordinator_now() < deadline);
    assert_int_equal(
        lc_xa_coordinator_operation_wait(f->operation, &wait, &f->error),
        LC_OK);
    fd.fd = wait.fd;
    fd.events =
        (wait.events & 1U ? POLLIN : 0) | (wait.events & 2U ? POLLOUT : 0);
    fd.revents = 0;
    {
      int timeout = 100;
      if (wait.deadline_ms != -1 &&
          wait.deadline_ms - coordinator_now() < timeout)
        timeout = wait.deadline_ms <= coordinator_now()
                      ? 0
                      : (int)(wait.deadline_ms - coordinator_now());
      assert_true(poll(&fd, 1U, timeout) >= 0);
    }
  }
}
static void coordinator_close_operation(fixture *f) {
  assert_int_equal(
      lc_xa_coordinator_operation_close_blocking(f->operation, &f->error),
      LC_OK);
  f->operation = NULL;
}
static void coordinator_begin(fixture *f) {
  assert_int_equal(lc_xa_coordinator_begin_start(f->coordinators[0], 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  assert_true(lc_xid_is_valid(
      lc_xa_coordinator_operation_result(f->operation)->txn_id));
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_transaction(
                       f->operation, &f->transactions[0], &f->error),
                   LC_OK);
  coordinator_close_operation(f);
}
static void
test_coordinator_state_only_commit_and_retained_receivers(void **state) {
  fixture *f = (fixture *)*state;
  lc_acquire_req acquire;
  lc_source *body;
  const lc_xa_coordinator_result *result;
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  coordinator_begin(f);
  lc_acquire_req_init(&acquire);
  acquire.key = "business";
  acquire.owner = "managed-state";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_xa_transaction_acquire_start(f->transactions[0], &acquire,
                                                   0, &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_lease(f->operation,
                                                          &f->lease, &f->error),
                   LC_OK);
  coordinator_close_operation(f);
  assert_string_equal(f->lease->txn_id,
                      lc_xa_transaction_id(f->transactions[0]));
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
  body->close(body);
  visible(f, 0);
  assert_int_equal(lc_xa_transaction_commit_start(f->transactions[0], 0,
                                                  &f->operation, &f->error),
                   LC_OK);
  /* Operation/transaction retain their coordinator after its caller closes. */
  assert_int_equal(
      lc_xa_coordinator_close_blocking(f->coordinators[0], &f->error), LC_OK);
  f->coordinators[0] = NULL;
  assert_int_equal(coordinator_await(f), LC_OK);
  result = lc_xa_coordinator_operation_result(f->operation);
  assert_int_equal(result->decision, LC_XA_DECISION_COMMIT);
  assert_int_equal(result->applied, 1);
  assert_string_equal(result->txn_id, lc_xa_transaction_id(f->transactions[0]));
  coordinator_close_operation(f);
  visible(f, 1);
  assert_int_equal(lc_xa_transaction_rollback_start(f->transactions[0], 0,
                                                    &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_ERR_INVALID);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_COMMIT);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
}
static void
test_coordinator_raw_terminal_replay_preserves_owner_epoch(void **state) {
  fixture *f = *state;
  int commit;
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  for (commit = 0; commit < 2; ++commit) {
    lc_acquire_req acquire;
    lc_txn_decision_req raw;
    lc_txn_decision_res response;
    lc_source *body;
    lc_xa_snapshot before, after;
    const char *id;
    coordinator_begin(f);
    id = lc_xa_transaction_id(f->transactions[0]);
    lc_acquire_req_init(&acquire);
    acquire.key = "business";
    acquire.owner = "terminal-replay";
    acquire.ttl_seconds = 30L;
    assert_int_equal(lc_xa_transaction_acquire_start(f->transactions[0],
                                                     &acquire, 0, &f->operation,
                                                     &f->error),
                     LC_OK);
    assert_int_equal(coordinator_await(f), LC_OK);
    assert_int_equal(lc_xa_coordinator_operation_take_lease(
                         f->operation, &f->lease, &f->error),
                     LC_OK);
    coordinator_close_operation(f);
    assert_int_equal(
        lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
    assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
    lc_source_close(body);
    assert_int_equal(
        commit ? lc_xa_transaction_commit_start(f->transactions[0], 0,
                                                &f->operation, &f->error)
               : lc_xa_transaction_rollback_start(f->transactions[0], 0,
                                                  &f->operation, &f->error),
        LC_OK);
    assert_int_equal(coordinator_await(f), LC_OK);
    coordinator_close_operation(f);
    assert_int_equal(
        lc_pouch_client_xa_snapshot(f->client, id, &before, &f->error), LC_OK);
    lc_txn_decision_req_init(&raw);
    raw.txn_id = id;
    raw.tc_term = before.term + 1U;
    memset(&response, 0, sizeof(response));
    assert_int_equal(
        commit ? lc_txn_commit(f->client, &raw, &response, &f->error)
               : lc_txn_rollback(f->client, &raw, &response, &f->error),
        LC_OK);
    lc_txn_decision_res_cleanup(&response);
    assert_int_equal(
        lc_pouch_client_xa_snapshot(f->client, id, &after, &f->error), LC_OK);
    assert_int_equal(after.term, before.term);
    assert_int_equal(after.decision, before.decision);
    lc_xa_snapshot_cleanup(&before);
    lc_xa_snapshot_cleanup(&after);
    assert_int_equal(lc_xa_transaction_renew_start(f->transactions[0], 0,
                                                   &f->operation, &f->error),
                     LC_OK);
    assert_int_equal(coordinator_await(f), LC_OK);
    coordinator_close_operation(f);
    assert_int_equal(
        commit ? lc_xa_transaction_commit_start(f->transactions[0], 0,
                                                &f->operation, &f->error)
               : lc_xa_transaction_rollback_start(f->transactions[0], 0,
                                                  &f->operation, &f->error),
        LC_OK);
    assert_int_equal(coordinator_await(f), LC_OK);
    assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                     1);
    coordinator_close_operation(f);
    lc_lease_close(f->lease);
    f->lease = NULL;
    assert_int_equal(
        lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
    f->transactions[0] = NULL;
  }
}
static void
test_coordinator_cancelled_begin_and_active_owner_recovery(void **state) {
  fixture *f = (fixture *)*state;
  char id[LC_XID_STRING_SIZE];
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[1], &f->error),
      LC_OK);
  coordinator_begin(f);
  strcpy(id, lc_xa_transaction_id(f->transactions[0]));
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[1], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_ERR_PENDING);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   0);
  coordinator_close_operation(f);
  assert_int_equal(
      lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
  f->transactions[0] = NULL;
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[1], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_ABORT);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
  assert_int_equal(lc_xa_coordinator_begin_start(f->coordinators[0], 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  strcpy(id, lc_xa_coordinator_operation_result(f->operation)->txn_id);
  assert_int_equal(lc_xa_coordinator_operation_cancel(f->operation, &f->error),
                   LC_ERR_PENDING);
  coordinator_close_operation(
      f); /* Observes claim then releases it without a sleep. */
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[1], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_ABORT);
  coordinator_close_operation(f);
}

static void test_coordinator_rejected_same_owner_recovery_preserves_live_claim(
    void **state) {
  fixture *f = *state;
  lc_acquire_req acquire;
  lc_source *body;
  char id[LC_XID_STRING_SIZE];
  int drive;
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  coordinator_begin(f);
  strcpy(id, lc_xa_transaction_id(f->transactions[0]));
  for (drive = 0; drive < 2; ++drive) {
    assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[0], id, 0,
                                                     &f->operation, &f->error),
                     LC_OK);
    if (drive)
      assert_int_equal(coordinator_await(f), LC_ERR_PENDING);
    /* Both observed rejection and close/cancel before advancement preserve
     * the original transaction. Neither attempt installed an owner claim. */
    coordinator_close_operation(f);
    clear_error(f);
    assert_int_equal(lc_xa_transaction_renew_start(f->transactions[0], 0,
                                                   &f->operation, &f->error),
                     LC_OK);
    assert_int_equal(coordinator_await(f), LC_OK);
    coordinator_close_operation(f);
  }
  lc_acquire_req_init(&acquire);
  acquire.key = "business";
  acquire.owner = "same-owner-recovery";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_xa_transaction_acquire_start(f->transactions[0], &acquire,
                                                   0, &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_lease(f->operation,
                                                          &f->lease, &f->error),
                   LC_OK);
  coordinator_close_operation(f);
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
  lc_source_close(body);
  assert_int_equal(lc_xa_transaction_commit_start(f->transactions[0], 0,
                                                  &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
  visible(f, 1);
}
static void test_coordinator_begin_journal_failure_preserves_id(void **state) {
  fixture *f = (fixture *)*state;
  char id[LC_XID_STRING_SIZE];
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[1], &f->error),
      LC_OK);
  lc_pouch_test_before_txn_decision_hook = fail_owner_journal;
  assert_int_equal(lc_xa_coordinator_begin_start(f->coordinators[0], 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  strcpy(id, lc_xa_coordinator_operation_result(f->operation)->txn_id);
  assert_int_equal(coordinator_await(f), LC_ERR_NOMEM);
  assert_string_equal(lc_xa_coordinator_operation_result(f->operation)->txn_id,
                      id);
  lc_pouch_test_before_txn_decision_hook = NULL;
  coordinator_close_operation(f);
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[1], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_ABORT);
  coordinator_close_operation(f);
}

typedef struct coordinator_foreign {
  lc_xa_coordinator *coordinator;
  int passed;
} coordinator_foreign;
static void *coordinator_wrong_thread(void *context) {
  coordinator_foreign *foreign = (coordinator_foreign *)context;
  lc_xa_coordinator_operation *op = (lc_xa_coordinator_operation *)1;
  foreign->passed = lc_xa_coordinator_begin_start(foreign->coordinator, 0, &op,
                                                  NULL) == LC_ERR_INVALID &&
                    op == NULL &&
                    lc_xa_coordinator_close_blocking(foreign->coordinator,
                                                     NULL) == LC_ERR_INVALID;
  return NULL;
}
static void
test_coordinator_owner_checks_and_deferred_transaction_close(void **state) {
  fixture *f = (fixture *)*state;
  coordinator_foreign foreign;
  pthread_t thread;
  pid_t child;
  int status;
  char id[LC_XID_STRING_SIZE];
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  /* Complete startup maintenance before forking. A child has no worker thread
   * stack to retain an in-flight startup allocation under Memcheck. */
  coordinator_begin(f);
  foreign.coordinator = f->coordinators[0];
  foreign.passed = 0;
  assert_int_equal(
      pthread_create(&thread, NULL, coordinator_wrong_thread, &foreign), 0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(foreign.passed, 1);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    lc_xa_coordinator_operation *op = NULL;
    _exit(lc_xa_coordinator_begin_start(f->coordinators[0], 0, &op, NULL) ==
                      LC_ERR_INVALID &&
                  op == NULL &&
                  lc_xa_coordinator_close_blocking(f->coordinators[0], NULL) ==
                      LC_ERR_INVALID
              ? 0
              : 1);
  }
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  assert_int_equal(lc_xa_coordinator_begin_start(f->coordinators[0], 1,
                                                 &f->operation, &f->error),
                   LC_ERR_INVALID);
  assert_null(f->operation);
  strcpy(id, lc_xa_transaction_id(f->transactions[0]));
  assert_int_equal(lc_xa_coordinator_begin_start(f->coordinators[0], 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
  f->transactions[0] = NULL;
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_transaction(
                       f->operation, &f->transactions[1], &f->error),
                   LC_OK);
  coordinator_close_operation(f);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[1], &f->error),
      LC_OK);
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[1], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_ABORT);
  coordinator_close_operation(f);
}

static void xa_outbox_open(fixture *f) {
  lc_outbox_config config;
  lc_outbox_config_init(&config);
  config.ns = "default";
  assert_int_equal(
      lc_client_new_outbox(f->client, &config, &f->outbox, &f->error), LC_OK);
  assert_int_equal(lc_outbox_begin(f->outbox, &f->box, &f->error), LC_OK);
}
static void xa_outbox_stage(fixture *f) {
  lc_outbox_participant_request request;
  lc_outbox_entry entry;
  lc_source *source;
  lc_outbox_receipt receipt;
  lc_outbox_participant_request_init(&request);
  request.acquire.key = "business";
  assert_int_equal(lc_outbox_transaction_acquire(f->box, &request,
                                                 &f->participant, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &source, &f->error), LC_OK);
  assert_int_equal(
      lc_outbox_participant_update(f->participant, source, NULL, &f->error),
      LC_OK);
  source->close(source);
  lc_outbox_entry_init(&entry);
  lc_outbox_receipt_init(&receipt);
  entry.operation_id = "xa-operation";
  entry.effect_id = "xa-effect";
  entry.effect_key = "xa-key";
  entry.payload_digest = "sha256:xa-payload";
  entry.kind = "test";
  entry.destination = "xa://target";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &source, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_outbox_transaction_append(f->box, &entry, source, &receipt, &f->error),
      LC_OK);
  source->close(source);
  assert_null(receipt.outbox_key);
  lc_outbox_receipt_cleanup(&receipt);
}
static void test_xa_outbox_sql_first_binding_and_receipts(void **state) {
  fixture *f = (fixture *)*state;
  char initial[LC_XID_STRING_SIZE];
  xa_outbox_open(f);
  strcpy(initial, lc_outbox_transaction_id(f->box));
  assert_true(lc_xid_is_valid(initial));
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  coordinator_begin(f);
  assert_int_equal(
      lc_outbox_transaction_bind_xa(f->box, f->transactions[0], &f->error),
      LC_OK);
  assert_string_equal(lc_outbox_transaction_id(f->box),
                      lc_xa_transaction_id(f->transactions[0]));
  assert_string_not_equal(initial, lc_outbox_transaction_id(f->box));
  xa_outbox_stage(f);
  assert_string_equal(f->participant->txn_id, lc_outbox_transaction_id(f->box));
  visible(f, 0);
  assert_int_equal(
      lc_outbox_transaction_commit(f->box, &f->receipts, &f->error),
      LC_ERR_PENDING);
  assert_int_equal(
      lc_outbox_transaction_commit_start(f->box, 0, &f->operation, &f->error),
      LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_COMMIT);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
  visible(f, 1);
  assert_int_equal(
      lc_outbox_transaction_commit(f->box, &f->receipts, &f->error), LC_OK);
  assert_int_equal(f->receipts.outbox_receipt_count, 1U);
  assert_string_equal(f->receipts.outbox_receipts[0].effect_key, "xa-key");
  assert_int_equal(lc_outbox_transaction_rollback(f->box, &f->error),
                   LC_ERR_INVALID);
}
static void test_xa_outbox_first_adoption_and_rollback(void **state) {
  fixture *f = (fixture *)*state;
  char id[LC_XID_STRING_SIZE];
  xa_outbox_open(f);
  strcpy(id, lc_outbox_transaction_id(f->box));
  xa_outbox_stage(f);
  assert_string_equal(id, lc_outbox_transaction_id(f->box));
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  assert_int_equal(lc_xa_coordinator_adopt_start(f->coordinators[0], id, 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_transaction(
                       f->operation, &f->transactions[0], &f->error),
                   LC_OK);
  coordinator_close_operation(f);
  assert_int_equal(
      lc_outbox_transaction_bind_xa(f->box, f->transactions[0], &f->error),
      LC_OK);
  assert_int_equal(
      lc_outbox_transaction_rollback_start(f->box, 0, &f->operation, &f->error),
      LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                   LC_XA_DECISION_ABORT);
  coordinator_close_operation(f);
  visible(f, 0);
  assert_int_equal(lc_outbox_transaction_rollback(f->box, &f->error), LC_OK);
  assert_int_equal(
      lc_outbox_transaction_commit(f->box, &f->receipts, &f->error),
      LC_ERR_INVALID);
}
static void
test_xa_outbox_close_during_pending_commit_keeps_owner(void **state) {
  fixture *f = (fixture *)*state;
  xa_outbox_open(f);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  coordinator_begin(f);
  assert_int_equal(
      lc_outbox_transaction_bind_xa(f->box, f->transactions[0], &f->error),
      LC_OK);
  xa_outbox_stage(f);
  assert_int_equal(
      lc_outbox_transaction_commit_start(f->box, 0, &f->operation, &f->error),
      LC_OK);
  lc_outbox_transaction_close(f->box);
  f->box = NULL;
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
  visible(f, 1);
}

static void
test_xa_outbox_detached_owner_closes_application_views(void **state) {
  fixture *f = (fixture *)*state;
  lc_source *source;
  lc_sink *sink;
  lc_get_res result;
  xa_outbox_open(f);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  coordinator_begin(f);
  assert_int_equal(
      lc_outbox_transaction_bind_xa(f->box, f->transactions[0], &f->error),
      LC_OK);
  xa_outbox_stage(f);
  assert_int_equal(
      lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
  f->transactions[0] = NULL;
  assert_int_equal(lc_source_from_memory("{}", 2U, &source, &f->error), LC_OK);
  assert_int_equal(
      lc_outbox_participant_update(f->participant, source, NULL, &f->error),
      LC_ERR_INVALID);
  source->close(source);
  assert_int_equal(lc_sink_to_memory(&sink, &f->error), LC_OK);
  memset(&result, 0, sizeof(result));
  assert_int_equal(
      lc_outbox_participant_get(f->participant, sink, NULL, &result, &f->error),
      LC_ERR_INVALID);
  lc_get_res_cleanup(&result);
  sink->close(sink);
  assert_int_equal(
      lc_outbox_transaction_commit_start(f->box, 0, &f->operation, &f->error),
      LC_ERR_INVALID);
  assert_null(f->operation);
}

static void history_complete(fixture *f, char *id, const char *key, int commit,
                             int keep_transaction) {
  lc_acquire_req request;
  lc_source *body;
  coordinator_begin(f);
  strcpy(id, lc_xa_transaction_id(f->transactions[0]));
  lc_acquire_req_init(&request);
  request.key = key;
  request.owner = "history-test";
  request.ttl_seconds = 30L;
  assert_int_equal(lc_xa_transaction_acquire_start(f->transactions[0], &request,
                                                   0, &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_take_lease(f->operation,
                                                          &f->lease, &f->error),
                   LC_OK);
  coordinator_close_operation(f);
  assert_int_equal(
      lc_source_from_memory("{\"value\":42}", 12U, &body, &f->error), LC_OK);
  assert_int_equal(lc_lease_update(f->lease, body, NULL, &f->error), LC_OK);
  lc_source_close(body);
  assert_int_equal(
      commit ? lc_xa_transaction_commit_start(f->transactions[0], 0,
                                              &f->operation, &f->error)
             : lc_xa_transaction_rollback_start(f->transactions[0], 0,
                                                &f->operation, &f->error),
      LC_OK);
  assert_int_equal(coordinator_await(f), LC_OK);
  assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                   1);
  coordinator_close_operation(f);
  lc_lease_close(f->lease);
  f->lease = NULL;
  if (!keep_transaction) {
    assert_int_equal(
        lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
    f->transactions[0] = NULL;
  }
}
static size_t history_retire(fixture *f, int expected) {
  size_t retired;
  assert_int_equal(lc_xa_coordinator_retire_start(f->coordinators[0], 0,
                                                  &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), expected);
  retired = lc_xa_coordinator_operation_result(f->operation)->retired_count;
  coordinator_close_operation(f);
  return retired;
}
static void history_recover(fixture *f, const char *id, int decision) {
  assert_int_equal(lc_xa_coordinator_recover_start(f->coordinators[0], id, 0,
                                                   &f->operation, &f->error),
                   LC_OK);
  if (decision != 0) {
    assert_int_equal(coordinator_await(f), LC_OK);
    assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->decision,
                     decision);
    assert_int_equal(lc_xa_coordinator_operation_result(f->operation)->applied,
                     1);
  } else {
    assert_int_equal(coordinator_await(f), LC_ERR_INVALID);
    assert_string_equal(f->error.server_code, "not_found");
  }
  coordinator_close_operation(f);
  clear_error(f);
}
static void history_clock(time_t *now) {
  lc_pouch_test_queue_time_context = now;
  lc_pouch_test_queue_clock_gettime = tc_clock;
}
static void test_history_default_age_and_idempotent_completion(void **state) {
  fixture *f = *state;
  time_t now = time(NULL);
  char id[LC_XID_STRING_SIZE];
  history_clock(&now);
  assert_int_equal(
      lc_xa_coordinator_open(f->client, NULL, &f->coordinators[0], &f->error),
      LC_OK);
  history_complete(f, id, "business", 1, 0);
  now += 30L * 24L * 60L * 60L - 1L;
  assert_int_equal(history_retire(f, LC_OK), 0U);
  history_recover(f, id, LC_XA_DECISION_COMMIT);
  visible(f, 1);
  ++now;
  assert_int_equal(history_retire(f, LC_OK), 1U);
  history_recover(f, id, 0);
  visible(f, 1);
}
static void
test_history_live_owner_and_unfinished_work_are_protected(void **state) {
  fixture *f = *state;
  lc_xa_coordinator_config config;
  time_t now = time(NULL);
  char complete_id[LC_XID_STRING_SIZE], unfinished_id[LC_XID_STRING_SIZE];
  memset(&config, 0, sizeof(config));
  config.completed_retention_seconds = 10L;
  history_clock(&now);
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  history_complete(f, complete_id, "business", 1, 1);
  f->transactions[1] = f->transactions[0];
  f->transactions[0] = NULL;
  coordinator_begin(f);
  strcpy(unfinished_id, lc_xa_transaction_id(f->transactions[0]));
  now += 11;
  assert_int_equal(history_retire(f, LC_OK), 0U);
  history_recover(f, complete_id, LC_XA_DECISION_COMMIT);
  assert_int_equal(
      lc_xa_transaction_close_blocking(f->transactions[1], &f->error), LC_OK);
  f->transactions[1] = NULL;
  assert_int_equal(history_retire(f, LC_OK), 1U);
  history_recover(f, complete_id, 0);
  assert_int_equal(
      lc_xa_transaction_close_blocking(f->transactions[0], &f->error), LC_OK);
  f->transactions[0] = NULL;
  now += 100000;
  assert_int_equal(history_retire(f, LC_OK), 0U);
  history_recover(f, unfinished_id, LC_XA_DECISION_ABORT);
}
static void test_history_count_cap_retires_oldest_on_release(void **state) {
  fixture *f = *state;
  lc_xa_coordinator_config config;
  time_t now = time(NULL);
  char ids[3][LC_XID_STRING_SIZE];
  memset(&config, 0, sizeof(config));
  config.completed_retention_seconds = -1L;
  config.completed_max_records = 2U;
  history_clock(&now);
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  history_complete(f, ids[0], "first", 1, 0);
  ++now;
  history_complete(f, ids[1], "second", 0, 0);
  ++now;
  history_complete(f, ids[2], "third", 1, 0);
  /* No explicit sweep: the enabled count policy runs at final release. */
  history_recover(f, ids[0], 0);
  history_recover(f, ids[1], LC_XA_DECISION_ABORT);
  history_recover(f, ids[2], LC_XA_DECISION_COMMIT);
  now += 100L * 24L * 60L * 60L;
  assert_int_equal(history_retire(f, LC_OK), 0U);
  history_recover(f, ids[1], LC_XA_DECISION_ABORT);
  history_recover(f, ids[2], LC_XA_DECISION_COMMIT);
}
static void
test_history_invalid_policy_and_corrupt_completion_fail_closed(void **state) {
  fixture *f = *state;
  lc_xa_coordinator_config config;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result written;
  lc_pouch_state_read_result record;
  lc_source *body;
  const unsigned char invalid[] = {'L', 'P', 'X', 'C', 0, 0, 0, 0, 0, 0, 0, 0};
  time_t now = time(NULL);
  char id[LC_XID_STRING_SIZE];
  char key[sizeof("completed/") + LC_XID_STRING_SIZE];
  memset(&config, 0, sizeof(config));
  config.completed_retention_seconds = -2L;
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_ERR_INVALID);
  assert_null(f->coordinators[0]);
  assert_non_null(strstr(f->error.message, "completed_retention_seconds"));
  clear_error(f);
  config.completed_retention_seconds = 1L;
  history_clock(&now);
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  history_complete(f, id, "business", 1, 0);
  (void)snprintf(key, sizeof(key), "completed/%s", id);
  memset(&options, 0, sizeof(options));
  memset(&written, 0, sizeof(written));
  options.object_record = 1;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  assert_int_equal(
      lc_source_from_memory(invalid, sizeof(invalid), &body, &f->error), LC_OK);
  assert_int_equal(lc_pouch_state_write(store(f), ".xa", key, body, &options,
                                        &written, &f->error),
                   LC_OK);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &written);
  now += 2;
  assert_int_equal(history_retire(f, LC_ERR_PROTOCOL), 0U);
  clear_error(f);
  memset(&record, 0, sizeof(record));
  assert_int_equal(
      lc_pouch_state_read(store(f), ".txns", id, &record, &f->error), LC_OK);
  assert_true(record.found);
  lc_pouch_state_read_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &record);
  (void)snprintf(key, sizeof(key), "owner/%s", id);
  memset(&record, 0, sizeof(record));
  assert_int_equal(
      lc_pouch_state_read(store(f), ".xa", key, &record, &f->error), LC_OK);
  assert_true(record.found);
  lc_pouch_state_read_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &record);
  visible(f, 1);
}
static void test_history_unfinished_commit_preserves_branch_until_application(
    void **state) {
  fixture *f = *state;
  lc_xa_coordinator_config config;
  lc_xa_snapshot snapshot;
  lc_pouch_state_read_result record;
  time_t now = time(NULL);
  char key[LC_POUCH_XA_KEY_SIZE];
  memset(&config, 0, sizeof(config));
  config.completed_retention_seconds = 1L;
  config.completed_max_records = 1U;
  history_clock(&now);
  owned_journals(f);
  owned_claim(f, 0U, 0, 0, 1U);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
      LC_OK);
  owned_progress(f, 0U, LC_XA_PREPARE_SENT);
  owned_progress(f, 0U, LC_XA_PREPARED);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[0], f->branches[0].txn_id, 1, &f->error),
                   LC_ERR_PENDING);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[0], f->branches[0].txn_id, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  assert_int_equal(history_retire(f, LC_OK), 0U);
  now += 40L * 24L * 60L * 60L;
  assert_int_equal(history_retire(f, LC_OK), 0U);
  assert_int_equal(lc_xa_store_snapshot_blocking(f->journals[0],
                                                 f->branches[0].txn_id,
                                                 &snapshot, &f->error),
                   LC_OK);
  assert_int_equal(snapshot.decision, LC_XA_DECISION_COMMIT);
  assert_int_equal(snapshot.completed_at_unix, 0);
  assert_int_equal(snapshot.branch_count, 1U);
  assert_int_equal(snapshot.branches[0].phase, LC_XA_PREPARED);
  lc_xa_snapshot_cleanup(&snapshot);
  owned_claim(f, 1U, 1, LC_XA_DECISION_COMMIT, 2U);
  owned_progress(f, 1U, LC_XA_COMMIT_SENT);
  owned_progress(f, 1U, LC_XA_COMMITTED);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[1], f->branches[0].txn_id, 1, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[1], f->branches[0].txn_id, &f->error),
                   LC_OK);
  ++now;
  assert_int_equal(history_retire(f, LC_OK), 1U);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  memset(&record, 0, sizeof(record));
  assert_int_equal(
      lc_pouch_state_read(store(f), ".xa", key, &record, &f->error), LC_OK);
  assert_false(record.found);
  lc_pouch_state_read_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &record);
  history_recover(f, f->branches[0].txn_id, 0);
}
static int interrupt_retirement(void *context, lc_error *error) {
  (void)context;
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "injected interruption after retirement barrier", NULL,
                      NULL, NULL);
}
static void
test_history_interrupted_retirement_resumes_after_reopen(void **state) {
  fixture *f = *state;
  lc_pouch_state_write_result removed;
  lc_xa_snapshot snapshot;
  lc_xa_coordinator_config config;
  time_t now = time(NULL);
  char branch_key[LC_POUCH_XA_KEY_SIZE];
  size_t i;
  memset(&config, 0, sizeof(config));
  config.completed_retention_seconds = 1L;
  history_clock(&now);
  owned_journals(f);
  owned_claim(f, 0U, 0, 0, 1U);
  assert_int_equal(
      lc_xa_store_enroll_blocking(f->journals[0], &f->branches[0], &f->error),
      LC_OK);
  owned_progress(f, 0U, LC_XA_PREPARE_SENT);
  owned_progress(f, 0U, LC_XA_PREPARED);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[0], f->branches[0].txn_id, 1, &f->error),
                   LC_ERR_PENDING);
  owned_progress(f, 0U, LC_XA_COMMIT_SENT);
  owned_progress(f, 0U, LC_XA_COMMITTED);
  assert_int_equal(lc_xa_store_decide_blocking(
                       f->journals[0], f->branches[0].txn_id, 1, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_store_release_blocking(
                       f->journals[0], f->branches[0].txn_id, &f->error),
                   LC_OK);
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  /* Finish a sweep before advancing the injected wall clock. */
  assert_int_equal(history_retire(f, LC_OK), 0U);
  now += 2;
  lc_pouch_test_after_xa_retire_owner_hook = interrupt_retirement;
  assert_int_equal(history_retire(f, LC_ERR_TRANSPORT), 0U);
  lc_pouch_test_after_xa_retire_owner_hook = NULL;
  history_recover(f, f->branches[0].txn_id, 0);
  /* Simulate the next deletion having reached disk before another crash. */
  assert_int_equal(lc_pouch_xa_branch_key(&f->branches[0], branch_key,
                                          sizeof(branch_key), &f->error),
                   LC_OK);
  memset(&removed, 0, sizeof(removed));
  assert_int_equal(lc_pouch_state_delete(store(f), ".xa", branch_key, NULL,
                                         &removed, &f->error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &removed);
  memset(&removed, 0, sizeof(removed));
  assert_int_equal(lc_pouch_state_delete(store(f), ".txns",
                                         f->branches[0].txn_id, NULL, &removed,
                                         &f->error),
                   LC_OK);
  lc_pouch_state_write_result_cleanup(
      &((lc_client_handle *)f->client)->allocator, &removed);
  /* The final completion marker still owns this ID. Adoption cannot create
   * replacement work that interrupted retirement could then delete. */
  assert_int_equal(lc_xa_coordinator_adopt_start(f->coordinators[0],
                                                 f->branches[0].txn_id, 0,
                                                 &f->operation, &f->error),
                   LC_OK);
  assert_int_equal(coordinator_await(f), LC_ERR_INVALID);
  assert_non_null(strstr(f->error.message, "completed or expired"));
  coordinator_close_operation(f);
  clear_error(f);
  assert_int_equal(
      lc_xa_coordinator_close_blocking(f->coordinators[0], &f->error), LC_OK);
  f->coordinators[0] = NULL;
  for (i = 0U; i < 3U; ++i) {
    lc_xa_store_close(f->journals[i]);
    f->journals[i] = NULL;
  }
  lc_client_close(f->client);
  f->client = NULL;
  open_client(f); /* Must not reapply the completed decision/missing branch. */
  assert_int_equal(lc_pouch_client_xa_snapshot(f->client, f->branches[0].txn_id,
                                               &snapshot, &f->error),
                   LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "not_found");
  assert_int_equal(lc_xa_coordinator_open(f->client, &config,
                                          &f->coordinators[0], &f->error),
                   LC_OK);
  /* Open's automatic sweep or the explicit sweep finishes the same deletion. */
  assert_true(history_retire(f, LC_OK) <= 1U);
  history_recover(f, f->branches[0].txn_id, 0);
  {
    lc_pouch_state_read_result record;
    char key[sizeof("completed/") + LC_XID_STRING_SIZE];
    memset(&record, 0, sizeof(record));
    (void)snprintf(key, sizeof(key), "completed/%s", f->branches[0].txn_id);
    assert_int_equal(
        lc_pouch_state_read(store(f), ".xa", key, &record, &f->error), LC_OK);
    assert_false(record.found);
    lc_pouch_state_read_result_cleanup(
        &((lc_client_handle *)f->client)->allocator, &record);
  }
}

#ifdef LC_XA_NATIVE_SYNC_TEST
static void retry_syncs_decision(fixture *f, int rollback) {
  lc_pouch_namespace_manifest manifest = {0};
  struct stat st;
  char path[1500], key[LC_POUCH_XA_KEY_SIZE];
  int prepared;
  size_t calls;
  lc_client_close(f->client);
  f->client = NULL;
  assert_true(snprintf(f->endpoint, sizeof(f->endpoint),
                       "pouch://%s?single_writer=true&durable_sync=false&"
                       "background_compaction=false&query_indexing=false&"
                       "queue_watch=false&query_engine=scan",
                       f->root) > 0);
  open_client(f);
  enroll(f, 0U);
  stage_native(f);
  prepare_branch(f, 0U);
  assert_int_equal(lc_pouch_namespace_manifest_open(NULL, f->root, ".txns",
                                                    &manifest, NULL, NULL,
                                                    &f->error),
                   LC_OK);
  assert_true(snprintf(path, sizeof(path), "%s/segments/%s",
                       manifest.namespace_path, manifest.active_segment) > 0);
  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  assert_int_equal(stat(path, &st), 0);
  pthread_mutex_lock(&sync_watch_mutex);
  sync_watch.device = st.st_dev;
  sync_watch.inode = st.st_ino;
  sync_watch.active = sync_watch.fail = 1;
  sync_watch.calls = 0U;
  pthread_mutex_unlock(&sync_watch_mutex);
  decision(f, rollback, LC_ERR_TRANSPORT);
  assert_int_equal(
      lc_pouch_xa_branch_key(&f->branches[0], key, sizeof(key), &f->error),
      LC_OK);
  assert_int_equal(lc_pouch_client_xa_prepared(
                       f->client, f->branches[0].txn_id, key,
                       rollback ? LC_XA_ROLLBACK_SENT : LC_XA_COMMIT_SENT,
                       &prepared, &f->error),
                   LC_OK);
  pthread_mutex_lock(&sync_watch_mutex);
  calls = sync_watch.calls;
  sync_watch.fail = 0;
  sync_watch.calls = 0U;
  pthread_mutex_unlock(&sync_watch_mutex);
  assert_true(calls > 0U);
  progress(f, 0U, rollback ? LC_XA_ROLLBACK_SENT : LC_XA_COMMIT_SENT);
  pthread_mutex_lock(&sync_watch_mutex);
  calls = sync_watch.calls;
  sync_watch.active = 0;
  pthread_mutex_unlock(&sync_watch_mutex);
  assert_true(calls > 0U);
  progress(f, 0U, rollback ? LC_XA_ABORTED : LC_XA_COMMITTED);
  replay(f, rollback ? "rollback" : "commit", LC_OK);
  visible(f, !rollback);
}
static void test_commit_retry_syncs_authoritative_decision(void **state) {
  retry_syncs_decision((fixture *)*state, 0);
}
static void test_rollback_retry_syncs_authoritative_decision(void **state) {
  retry_syncs_decision((fixture *)*state, 1);
}
#endif

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
#ifdef LC_XA_NATIVE_SYNC_TEST
      cmocka_unit_test_setup_teardown(
          test_commit_retry_syncs_authoritative_decision, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_rollback_retry_syncs_authoritative_decision, setup, teardown),
#endif
      cmocka_unit_test_setup_teardown(
          test_history_default_age_and_idempotent_completion, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_history_live_owner_and_unfinished_work_are_protected, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_history_count_cap_retires_oldest_on_release, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_history_invalid_policy_and_corrupt_completion_fail_closed, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_history_unfinished_commit_preserves_branch_until_application,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_history_interrupted_retirement_resumes_after_reopen, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_xa_outbox_detached_owner_closes_application_views, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_xa_outbox_sql_first_binding_and_receipts, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_xa_outbox_first_adoption_and_rollback, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_xa_outbox_close_during_pending_commit_keeps_owner, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_owner_checks_and_deferred_transaction_close, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_state_only_commit_and_retained_receivers, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_cancelled_begin_and_active_owner_recovery, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_raw_terminal_replay_preserves_owner_epoch, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_rejected_same_owner_recovery_preserves_live_claim,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_coordinator_begin_journal_failure_preserves_id, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_managed_pouch_vote_and_independent_transactions, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_managed_owner_exclusion_and_commit_takeover, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_managed_owner_expiry_preserves_shared_staging_until_abort, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_managed_owner_begin_failure_retains_recoverable_identity, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_managed_owner_publication_failure_keeps_terminal_decision, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_tc_expired_owner_cannot_reuse_an_old_epoch, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_tc_release_preserves_epoch_across_reopen, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_driver_contract_pins_closed_owners_and_preallocates_branch,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_driver_contract_pins_transferred_branch_until_operation_close,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_driver_contract_reclaims_unclaimed_and_cancelled_branches, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_native_prepare_observation_is_durable_without_yes, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_public_store_rejects_completion_contrary_to_decision, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_public_store_pins_client_and_preserves_resource_identity, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_pending_commit_survives_reopen,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_all_external_branches_complete_before_application, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_rollback_waits_for_uncertain_prepare_cleanup, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_enrollment_seals_and_opposite_resolution_is_rejected, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_branch_identity_is_immutable_and_namespace_private, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_missing_descriptor_blocks_entire_application, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_invalid_native_identity_and_phase_are_rejected, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_prepare_intent_is_not_a_yes_vote_after_reopen, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_expired_prepare_preserves_abort_until_external_cleanup, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_shared_client_observes_durable_external_completion, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_corrupt_descriptor_never_opens_application_gate, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_native_lease_vote_waits_for_external_completion, setup,
          teardown)};
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(tests[0]), argc, argv,
                           NULL, NULL);
}
