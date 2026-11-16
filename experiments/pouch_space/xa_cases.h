#ifndef POUCH_SPACE_XA_CASES_H
#define POUCH_SPACE_XA_CASES_H
#include "lc_pouch_xa.h"
#include "lc_xa_internal.h"

static char *xa_test_key;
static void xa_free_key(void) {
  lc_pouch_crypto_key_string_free(xa_test_key);
  xa_test_key = NULL;
}

typedef struct xa_admission {
  int create;
  const ps_io *io;
  void (*ready)(void *);
  void *context;
} xa_admission;
static int xa_admit(void *context, const char *path, const lc_allocator *a,
                    const lc_pouch_open_options *logical, lc_pouch **out,
                    lc_error *e) {
  xa_admission *admission = context;
  ps_pouch_state_options options = {0};
  options.logical = *logical;
  options.io = admission->io;
  int rc = ps_pouch_state_open(path, admission->create, a, &options, out, e);
  if (rc == LC_OK && admission->ready)
    admission->ready(admission->context);
  return rc;
}
static lc_client *xa_client_with_admission(const char *path, int sync,
                                           int create, const ps_io *io,
                                           void (*ready)(void *), void *context,
                                           lc_error *e) {
  char endpoint[1600];
  const char *endpoints[] = {endpoint};
  lc_client_config config;
  lc_pouch_settings settings;
  xa_admission admission = {create, io, ready, context};
  lc_client *client = NULL;
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?single_writer=true&background_compaction=false&"
                   "query_indexing=false&queue_watch=false&query_engine=scan",
                   path) < (int)sizeof(endpoint));
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  const char *codec_text = getenv("POUCH_XA_CODEC");
  int codec = codec_text ? atoi(codec_text) : 0;
  REQUIRE(codec >= 0 && codec <= 3);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION;
  settings.durable_sync = sync;
  settings.compression = codec & 2 ? "zlib" : "none";
  if ((codec & 1) && !xa_test_key) {
    const char *inherited = getenv("POUCH_XA_KEY");
    if (inherited) {
      xa_test_key = lc_strdup_with_allocator(NULL, inherited);
      REQUIRE(xa_test_key);
    } else
      ok(lc_pouch_crypto_generate_key_string(&xa_test_key, e), e);
    REQUIRE(!atexit(xa_free_key));
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.allocator = allocator;
  config.pouch_settings = &settings;
  config.pouch_crypto_key = codec & 1 ? xa_test_key : NULL;
  ok(lc_client_open_with_pouch_factory(&config, xa_admit, &admission, &client,
                                       e),
     e);
  return client;
}
static lc_client *xa_client(const char *path, int sync, int create,
                            const ps_io *io, lc_error *e) {
  return xa_client_with_admission(path, sync, create, io, NULL, NULL, e);
}
static void xa_branch(lc_xa_branch_record *branch) {
  memset(branch, 0, sizeof(*branch));
  strcpy(branch->txn_id, "00000000000000000000");
  strcpy(branch->resource_id, "00000000000000000010");
  strcpy(branch->branch_id, "00000000000000000020");
  strcpy(branch->gid, "lc-physical-xa");
  strcpy(branch->full_xid, "12345");
  strcpy(branch->system_id, "7695692707854696465");
  strcpy(branch->database_oid, "16384");
  strcpy(branch->role_oid, "16385");
}
static void xa_stage(lc_client *client, const char *json, lc_error *e) {
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_txn_decision_req request;
  lc_txn_decision_res response = {0};
  lc_txn_participant participant = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result written = {0};
  lc_source *source = NULL;
  lc_txn_decision_req_init(&request);
  participant.ns = "business";
  participant.key = "value";
  request.txn_id = retention_xid;
  request.participants = &participant;
  request.participant_count = 1U;
  ok(lc_txn_prepare(client, &request, &response, e), e);
  lc_txn_decision_res_cleanup(&response);
  ok(lc_source_from_memory(json, strlen(json), &source, e), e);
  options.content_type = "application/json";
  ok(lc_pouch_state_stage_write(p, "business", "value", retention_xid, source,
                                &options, &written, e),
     e);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(&allocator, &written);
}
static int xa_decision(lc_client *client, int rollback, int replay,
                       lc_error *e) {
  int rc;
  if (replay) {
    lc_txn_replay_req request;
    lc_txn_replay_res response = {0};
    lc_txn_replay_req_init(&request);
    request.txn_id = retention_xid;
    rc = lc_txn_replay(client, &request, &response, e);
    lc_txn_replay_res_cleanup(&response);
  } else {
    lc_txn_decision_req request;
    lc_txn_decision_res response = {0};
    lc_txn_decision_req_init(&request);
    request.txn_id = retention_xid;
    rc = rollback ? lc_txn_rollback(client, &request, &response, e)
                  : lc_txn_commit(client, &request, &response, e);
    lc_txn_decision_res_cleanup(&response);
  }
  return rc;
}
static int xa_action(lc_client *client, int action, lc_error *e) {
  lc_xa_branch_record branch;
  xa_branch(&branch);
  if (action == 10) {
    lc_xa_store *journal = NULL;
    int rc = lc_xa_store_open(client, &journal, e);
    if (rc == LC_OK)
      rc = lc_xa_store_register_blocking(journal, &branch, e);
    lc_xa_store_close(journal);
    return rc;
  }
  if (!action)
    return lc_pouch_client_xa_enroll(client, &branch, NULL, e);
  if (action == 3)
    return xa_decision(client, 0, 0, e);
  if (action == 6 || action == 9)
    return xa_decision(client, action == 9, 1, e);
  branch.phase = action == 1   ? LC_XA_PREPARE_SENT
                 : action == 2 ? LC_XA_PREPARED
                 : action == 4 ? LC_XA_COMMIT_SENT
                 : action == 5 ? LC_XA_COMMITTED
                 : action == 7 ? LC_XA_ROLLBACK_SENT
                               : LC_XA_ABORTED;
  branch.prepared_observed = action >= 2 && action <= 5;
  if (branch.phase >= LC_XA_COMMITTED) {
    branch.evidence = LC_XA_EVIDENCE_NATIVE;
    branch.completed_at_unix = 1000;
  }
  return lc_pouch_client_xa_progress(client, &branch, NULL, e);
}
static void xa_ok(int action, int rc, lc_error *e) {
  if (action == 3) {
    REQUIRE(rc == LC_ERR_PENDING);
    clear(e);
  } else
    ok(rc, e);
}
static void xa_seed(lc_client *client, int action, lc_error *e) {
  if (!action || action == 10)
    return;
  xa_ok(0, xa_action(client, 0, e), e);
  xa_stage(client, "{\"value\":42}", e);
  if (action > 1)
    xa_ok(1, xa_action(client, 1, e), e);
  if (action >= 7) {
    REQUIRE(xa_decision(client, 1, 0, e) == LC_ERR_PENDING);
    clear(e);
    if (action > 7)
      xa_ok(7, xa_action(client, 7, e), e);
    if (action > 8)
      xa_ok(8, xa_action(client, 8, e), e);
    return;
  }
  for (int step = 2; step < action; ++step)
    xa_ok(step, xa_action(client, step, e), e);
}
static int xa_phase(int action) {
  return action == 0   ? LC_XA_ENROLLED
         : action == 1 ? LC_XA_PREPARE_SENT
         : action <= 3 ? LC_XA_PREPARED
         : action == 4 ? LC_XA_COMMIT_SENT
         : action <= 6 ? LC_XA_COMMITTED
         : action == 7 ? LC_XA_ROLLBACK_SENT
                       : LC_XA_ABORTED;
}
static void xa_registration(lc_client *client, int required, lc_error *e) {
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_xa_branch_record branch;
  lc_pouch_state_read_result result = {0};
  char key[LC_POUCH_XA_KEY_SIZE];
  unsigned char actual[48] = {0}, expected[47] = {0};
  xa_branch(&branch);
  REQUIRE(snprintf(key, sizeof(key), "resource/%s", branch.resource_id) > 0);
  ok(lc_pouch_state_read(p, ".xa", key, &result, e), e);
  REQUIRE(!required || result.found);
  if (result.found) {
    size_t total = 0U, count;
    REQUIRE(result.body);
    do {
      count = result.body->read(result.body, actual + total,
                                sizeof(actual) - total, e);
      ok(e->code, e);
      REQUIRE(count <= sizeof(actual) - total);
      total += count;
    } while (count && total < sizeof(actual));
    memcpy(expected, "LXAR", 4U);
    memcpy(expected + 4U, branch.system_id, strlen(branch.system_id));
    memcpy(expected + 25U, branch.database_oid, strlen(branch.database_oid));
    memcpy(expected + 36U, branch.role_oid, strlen(branch.role_oid));
    REQUIRE(total == sizeof(expected) && !memcmp(actual, expected, total));
  }
  lc_pouch_state_read_result_cleanup(&allocator, &result);
}
static void xa_visible(lc_client *client, int action, lc_error *e) {
  if (action == 10) {
    xa_registration(client, 1, e);
    return;
  }
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_pouch_state_read_result result = {0};
  ok(lc_pouch_state_read(p, "business", "value", &result, e), e);
  if (action < 7) {
    char bytes[16] = {0};
    REQUIRE(result.found && result.body);
    REQUIRE(result.body->read(result.body, bytes, sizeof(bytes) - 1U, e) ==
            12U);
    REQUIRE(!strcmp(bytes, action == 3 ? "{\"value\":84}" : "{\"value\":42}"));
  } else
    REQUIRE(!result.found);
  lc_pouch_state_read_result_cleanup(&allocator, &result);
}
static void xa_verify(lc_client *client, int action, int completed,
                      lc_error *e) {
  if (action == 10) {
    lc_xa_store *journal = NULL;
    lc_xa_branch_record wrong;
    xa_registration(client, completed, e);
    ok(xa_action(client, action, e), e);
    xa_registration(client, 1, e);
    xa_branch(&wrong);
    strcpy(wrong.role_oid, "42");
    ok(lc_xa_store_open(client, &journal, e), e);
    REQUIRE(lc_xa_store_register_blocking(journal, &wrong, e) ==
            LC_ERR_PROTOCOL);
    clear(e);
    lc_xa_store_close(journal);
    return;
  }
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_xa_branch_record branch, actual;
  char key[LC_POUCH_XA_KEY_SIZE];
  lc_xa_store *journal = NULL;
  lc_xa_snapshot snapshot = {0};
  xa_branch(&branch);
  ok(lc_pouch_xa_branch_key(&branch, key, sizeof(key), e), e);
  ok(lc_xa_store_open(client, &journal, e), e);
  int rc = lc_xa_store_snapshot_blocking(journal, retention_xid, &snapshot, e);
  if (!action && !completed && rc == LC_ERR_INVALID)
    clear(e); /* Descriptor publication may precede participant enrollment. */
  else {
    ok(rc, e);
    REQUIRE(snapshot.branch_count == 1U);
    actual = snapshot.branches[0];
    if (completed) {
      REQUIRE(actual.phase == xa_phase(action));
      REQUIRE(snapshot.decision == (action < 3 ? 0 : action < 7 ? 1 : 2));
      int prepared = 0;
      ok(lc_pouch_client_xa_prepared(client, retention_xid, key, actual.phase,
                                     &prepared, e),
         e);
      REQUIRE(prepared == (action >= 2 && action < 7));
    } else {
      int prior = action == 1 || action == 7 ? LC_XA_ENROLLED
                  : action == 8              ? LC_XA_ROLLBACK_SENT
                  : action > 0               ? xa_phase(action - 1)
                                             : LC_XA_ENROLLED;
      if (action == 7)
        prior = LC_XA_PREPARE_SENT;
      REQUIRE(actual.phase == prior || actual.phase == xa_phase(action));
      REQUIRE(snapshot.decision == (action < 3   ? 0
                                    : action < 7 ? 1
                                                 : 2) ||
              (action == 3 && snapshot.decision == 0));
    }
  }
  /* A NoSync stage may be lost before a failed commit's prerequisite barrier.
   * It may be recreated only while no terminal decision exists. Once COMMIT
   * exists, recovery must prove that the replacement stage preceded it. */
  if (action == 3 && snapshot.decision == 0)
    xa_stage(client, "{\"value\":84}", e);
  lc_xa_snapshot_cleanup(&snapshot);
  lc_xa_store_close(journal);
  xa_ok(action, xa_action(client, action, e), e);
  if (!action)
    xa_stage(client, "{\"value\":42}", e);
  for (int step = action + 1; step <= (action < 7 ? 6 : 9); ++step)
    xa_ok(step, xa_action(client, step, e), e);
  xa_visible(client, action, e);
  /* Opposite decisions can never supersede the completed direction. */
  REQUIRE(xa_decision(client, action < 7, 0, e) == LC_ERR_INVALID);
  clear(e);
  (void)p;
}
/* Allocation cuts test ownership/retry. Successful sync is immediate in this
 * profile; separate stable-image and exec-death profiles prove persistence. */
static int xa_allocation_sync(void *context, int fd) {
  (void)context;
  (void)fd;
  return 0;
}
static int xa_selected(const char *name, int value, int maximum) {
  const char *text = getenv(name);
  char *end;
  if (!text)
    return 1;
  errno = 0;
  long selected = strtol(text, &end, 10);
  REQUIRE(text[0] && !errno && !*end && selected >= 0 && selected <= maximum);
  return selected == value;
}
static size_t xa_trial(const char *directory, int action, int sync, int kind,
                       int persist, size_t cut) {
  lc_error e;
  retention_disk disk = {0};
  char path[1400];
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-fault", directory) <
          (int)sizeof(path));
  if (kind != 2) {
    disk.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
    disk.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
    disk.scratch = malloc(PS_PAGE);
    disk.persist = persist != 0;
    disk.complete_writes = persist == 2;
    REQUIRE(disk.stable && disk.dirty && disk.scratch);
  }
  ps_io io = {&disk,
              retention_disk_read,
              retention_disk_write,
              retention_disk_sync,
              retention_disk_reserve,
              NULL};
  ps_io allocation_io = {&disk.fault,        disk_read,    disk_write,
                         xa_allocation_sync, disk_reserve, NULL};
  lc_client *client =
      xa_client(path, sync, 1, kind == 2 ? &allocation_io : &io, &e);
  xa_seed(client, action, &e);
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  ok(ps_checkpoint(ps_pouch_state_root(p)->space, &e), &e);
  if (action == 3)
    xa_stage(client, "{\"value\":84}", &e);
  allocation = 0U;
  fail_at = cut;
  armed = kind == 2;
  disk.fault.calls = 0U;
  disk.fault.fail = cut;
  disk.fault.armed = kind != 2;
  int rc = xa_action(client, action, &e);
  armed = disk.fault.armed = 0;
  size_t sites = kind == 2 ? allocation : disk.fault.calls;
  int completed =
      rc == LC_OK || (action == 3 && rc == LC_ERR_PENDING && e.server_code &&
                      !strcmp(e.server_code, "xa_pending"));
  if (!cut)
    xa_ok(action, rc, &e);
  else
    REQUIRE(sites >= cut);
  clear(&e);
  disk.frozen = 1;
  lc_client_close(client);
  REQUIRE(!live);
  if (kind != 2)
    retention_disk_restore(&disk, path);
  client = xa_client(path, sync, 0, kind == 2 ? &allocation_io : NULL, &e);
  xa_verify(client, action, completed, &e);
  lc_client_close(client);
  REQUIRE(!live);
  client = xa_client(path, sync, 0, kind == 2 ? &allocation_io : NULL, &e);
  xa_visible(client, action, &e);
  lc_client_close(client);
  REQUIRE(!live);
  one_file(path);
  REQUIRE(!unlink(path));
  free(disk.stable);
  free(disk.dirty);
  free(disk.scratch);
  lc_error_cleanup(&e);
  return sites;
}
static void xa_faults(const char *directory, int kind) {
  size_t cuts = 0U;
  size_t cases = 0U;
  for (int sync = 0; sync < 2; ++sync)
    for (int persist = 0; persist < (kind == 1 ? 3 : 1); ++persist)
      for (int action = 0; action < 11; ++action) {
        if (!xa_selected("POUCH_XA_ACTION", action, 10) ||
            !xa_selected("POUCH_XA_SYNC", sync, 1))
          continue;
        ++cases;
        size_t sites = xa_trial(directory, action, sync, kind, persist, 0U);
        fprintf(stderr, "XA kind=%d sync=%d persist=%d action=%d sites=%zu\n",
                kind, sync, persist, action, sites);
        if (kind)
          for (size_t cut = 1U; cut <= sites; ++cut) {
            (void)xa_trial(directory, action, sync, kind, persist, cut);
            ++cuts;
          }
      }
  REQUIRE(cases);
  printf("XA %s cuts=%zu passed\n",
         kind == 0   ? "durability"
         : kind == 1 ? "I/O"
                     : "allocation",
         cuts);
}
#endif
