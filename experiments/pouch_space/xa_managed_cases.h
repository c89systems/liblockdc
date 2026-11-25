#ifndef POUCH_SPACE_XA_MANAGED_CASES_H
#define POUCH_SPACE_XA_MANAGED_CASES_H
#include "xa_cases.h"

static const char xa_owner_ids[2][LC_XID_STRING_SIZE] = {
    "00000000000000000030", "00000000000000000040"};
typedef struct xa_image {
  unsigned char *bytes;
  size_t length;
  int found;
} xa_image;
static void xa_images(lc_client *client, xa_image images[4], lc_error *e) {
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_xa_branch_record branch;
  char key[LC_POUCH_XA_KEY_SIZE];
  const char *names[] = {retention_xid, "owner/00000000000000000000",
                         "completed/00000000000000000000", key};
  xa_branch(&branch);
  ok(lc_pouch_xa_branch_key(&branch, key, sizeof(key), e), e);
  for (size_t i = 0; i < 4U; ++i) {
    lc_pouch_state_read_result value = {0};
    memset(&images[i], 0, sizeof(images[i]));
    ok(lc_pouch_state_read(p, i ? ".xa" : ".txns", names[i], &value, e), e);
    images[i].found = value.found;
    if (value.found) {
      REQUIRE(value.body && value.bytes <= 8192U);
      images[i].bytes =
          lc_alloc_with_allocator(&allocator, (size_t)value.bytes + 1U);
      REQUIRE(images[i].bytes);
      while (images[i].length < (size_t)value.bytes) {
        size_t got =
            value.body->read(value.body, images[i].bytes + images[i].length,
                             (size_t)value.bytes - images[i].length, e);
        REQUIRE(got && e->code == LC_OK);
        images[i].length += got;
      }
    }
    lc_pouch_state_read_result_cleanup(&allocator, &value);
  }
}
static void xa_images_clear(xa_image images[4]) {
  for (size_t i = 0; i < 4U; ++i) {
    lc_free_with_allocator(&allocator, images[i].bytes);
    memset(&images[i], 0, sizeof(images[i]));
  }
}
static void xa_owned_claim(lc_xa_store *journal, const char *txn_id,
                           lc_error *e) {
  lc_xa_snapshot snapshot = {0};
  ok(lc_xa_store_claim_blocking(journal, txn_id, 3600L, 0, &snapshot, e), e);
  lc_xa_snapshot_cleanup(&snapshot);
}
static void xa_owned_progress(lc_xa_store *journal, const char *txn_id,
                              int phase, lc_error *e) {
  lc_xa_branch_record branch;
  xa_branch(&branch);
  strcpy(branch.txn_id, txn_id);
  branch.phase = (unsigned char)phase;
  branch.prepared_observed = phase == LC_XA_PREPARED ||
                             phase == LC_XA_COMMIT_SENT ||
                             phase == LC_XA_COMMITTED;
  if (phase >= LC_XA_COMMITTED) {
    branch.evidence = LC_XA_EVIDENCE_NATIVE;
    branch.completed_at_unix = 1000;
  }
  ok(lc_xa_store_progress_blocking(journal, &branch, e), e);
}
static void xa_owned_stage(lc_client *client, const char *txn_id,
                           const char *key, lc_error *e) {
  lc_acquire_req request;
  lc_lease *lease = NULL;
  lc_source *source = NULL;
  lc_acquire_req_init(&request);
  request.ns = "business";
  request.key = key;
  request.owner = "XA managed fixture";
  request.txn_id = txn_id;
  request.ttl_seconds = 3600L;
  ok(lc_acquire(client, &request, &lease, e), e);
  ok(lc_source_from_memory("{\"value\":42}", 12U, &source, e), e);
  ok(lc_lease_update(lease, source, NULL, e), e);
  lc_source_close(source);
  lc_lease_close(lease);
}
static void xa_managed_seed_id(lc_client *client, lc_xa_store *journal,
                               int action, const char *txn_id, const char *key,
                               lc_error *e) {
  lc_xa_branch_record branch;
  if (!action)
    return;
  xa_owned_claim(journal, txn_id, e);
  if (action < 5)
    return;
  xa_branch(&branch);
  strcpy(branch.txn_id, txn_id);
  ok(lc_xa_store_enroll_blocking(journal, &branch, e), e);
  xa_owned_stage(client, txn_id, key, e);
  ok(lc_xa_store_seal_blocking(journal, txn_id, e), e);
  xa_owned_progress(journal, txn_id, LC_XA_PREPARE_SENT, e);
  if (action != 6) {
    ok(lc_xa_store_prepare_pouch_blocking(journal, txn_id, e), e);
    xa_owned_progress(journal, txn_id, LC_XA_PREPARED, e);
    REQUIRE(lc_xa_store_decide_blocking(journal, txn_id, 1, e) ==
            LC_ERR_PENDING);
    clear(e);
  }
  if (action == 7) {
    xa_owned_progress(journal, txn_id, LC_XA_COMMIT_SENT, e);
    xa_owned_progress(journal, txn_id, LC_XA_COMMITTED, e);
    ok(lc_xa_store_decide_blocking(journal, txn_id, 1, e), e);
  }
  ok(lc_xa_store_release_blocking(journal, txn_id, e), e);
}
static void xa_managed_seed(lc_client *client, lc_xa_store *journal, int action,
                            lc_error *e) {
  xa_managed_seed_id(client, journal, action, retention_xid, "value", e);
}
static int xa_managed_action(lc_client *client, lc_xa_store *journal,
                             lc_xa_store *other, int action, lc_error *e) {
  lc_xa_snapshot snapshot = {0};
  lc_xa_branch_record branch;
  size_t retired = 0U;
  int rc;
  xa_branch(&branch);
  if (action == 0 || action == 5 || action == 6) {
    rc = lc_xa_store_claim_blocking(action ? other : journal, retention_xid,
                                    3600L, action != 0, &snapshot, e);
    lc_xa_snapshot_cleanup(&snapshot);
    return rc;
  }
  if (action == 1)
    return lc_xa_store_enroll_blocking(journal, &branch, e);
  if (action == 2)
    return lc_xa_store_renew_blocking(journal, retention_xid, 7200L, e);
  if (action == 3)
    return lc_xa_store_seal_blocking(journal, retention_xid, e);
  if (action == 4)
    return lc_xa_store_release_blocking(journal, retention_xid, e);
  return lc_pouch_client_xa_retire(client, 0L, 0U, &retired, e);
}
static void xa_managed_verify(lc_client *client, lc_xa_store *journal,
                              lc_xa_store *other, int action, lc_error *e) {
  ok(xa_managed_action(client, journal, other, action, e), e);
  if (action == 5 || action == 6) {
    lc_xa_snapshot snapshot = {0};
    ok(lc_xa_store_snapshot_blocking(other, retention_xid, &snapshot, e), e);
    REQUIRE(snapshot.decision == (action == 5 ? 1 : 2) && snapshot.term >= 2U);
    lc_xa_snapshot_cleanup(&snapshot);
    REQUIRE(lc_xa_store_renew_blocking(journal, retention_xid, 3600L, e) ==
            LC_ERR_INVALID);
    clear(e);
    xa_owned_progress(other, retention_xid,
                      action == 5 ? LC_XA_COMMIT_SENT : LC_XA_ROLLBACK_SENT, e);
    xa_owned_progress(other, retention_xid,
                      action == 5 ? LC_XA_COMMITTED : LC_XA_ABORTED, e);
    ok(lc_xa_store_decide_blocking(other, retention_xid, action == 5, e), e);
    xa_visible(client, action == 5 ? 0 : 7, e);
  }
  if (action == 7) {
    lc_xa_snapshot snapshot = {0};
    REQUIRE(lc_xa_store_snapshot_blocking(journal, retention_xid, &snapshot,
                                          e) == LC_ERR_INVALID);
    clear(e);
    REQUIRE(lc_xa_store_claim_blocking(other, retention_xid, 3600L, 1,
                                       &snapshot, e) == LC_ERR_INVALID);
    clear(e);
    xa_visible(client, 0, e);
  }
}
static size_t xa_managed_trial(const char *directory, int action, int sync,
                               int kind, int persist, size_t cut) {
  retention_disk disk = {0};
  lc_error e;
  char path[1400];
  xa_image expected[4] = {{0}}, actual[4] = {{0}};
  lc_xa_store *journal = NULL, *other = NULL;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-managed", directory) <
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
  ps_io allocation_io = {&disk.fault,           disk_read,    disk_write,
                         fixture_complete_sync, disk_reserve, NULL};
  ps_io recovery_io = ps_native_io;
  recovery_io.sync = fixture_complete_sync;
  const ps_io *reopen_io = kind == 2 ? &allocation_io
                           : kind    ? &recovery_io
                                     : NULL;
  lc_client *client =
      xa_client(path, sync, 1, kind == 2 ? &allocation_io : &io, &e);
  ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e), &e);
  ok(lc_xa_store_open_owned(client, xa_owner_ids[1], &other, &e), &e);
  xa_managed_seed(client, journal, action, &e);
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  ok(ps_checkpoint(ps_pouch_state_root(p)->space, &e), &e);
  allocation = 0U;
  fail_at = cut;
  armed = kind == 2;
  disk.fault.calls = 0U;
  disk.fault.fail = cut;
  disk.fault.armed = kind != 2;
  int rc = xa_managed_action(client, journal, other, action, &e);
  armed = disk.fault.armed = 0;
  size_t sites = kind == 2 ? allocation : disk.fault.calls;
  if (!cut)
    ok(rc, &e);
  else
    REQUIRE(sites >= cut);
  clear(&e);
  if (rc == LC_OK)
    xa_images(client, expected, &e);
  disk.frozen = 1;
  lc_xa_store_close(other);
  lc_xa_store_close(journal);
  lc_client_close(client);
  REQUIRE(live == 0U ||
          rc == LC_OK); /* Captured expected image owns tracked bytes. */
  if (kind != 2)
    retention_disk_restore(&disk, path);
  client = xa_client(path, sync, 0, reopen_io, &e);
  ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e), &e);
  ok(lc_xa_store_open_owned(client, xa_owner_ids[1], &other, &e), &e);
  if (rc == LC_OK) {
    xa_images(client, actual, &e);
    for (size_t i = 0; i < 4U; ++i)
      REQUIRE(
          expected[i].found == actual[i].found &&
          expected[i].length == actual[i].length &&
          (!expected[i].length ||
           !memcmp(expected[i].bytes, actual[i].bytes, expected[i].length)));
  }
  xa_images_clear(expected);
  xa_images_clear(actual);
  xa_managed_verify(client, journal, other, action, &e);
  lc_xa_store_close(other);
  lc_xa_store_close(journal);
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
static void xa_managed_faults(const char *directory, int kind) {
  size_t cuts = 0U;
  size_t cases = 0U;
  fixture_directory_sync_model(kind != 0);
  for (int sync = 0; sync < 2; ++sync) {
    if (!fixture_selected("POUCH_XA_SYNC", sync, 1))
      continue;
    for (int persist = 0; persist < (kind == 1 ? 3 : 1); ++persist)
      for (int action = 0; action < 8; ++action) {
        ++cases;
        size_t sites =
            xa_managed_trial(directory, action, sync, kind, persist, 0U);
        fprintf(stderr,
                "XA managed kind=%d sync=%d persist=%d action=%d sites=%zu\n",
                kind, sync, persist, action, sites);
        if (kind)
          for (size_t cut = 1U; cut <= sites; ++cut) {
            (void)xa_managed_trial(directory, action, sync, kind, persist, cut);
            ++cuts;
          }
      }
  }
  fixture_directory_sync_model(0);
  REQUIRE(cases);
  printf("XA managed %s cuts=%zu passed\n",
         kind == 0   ? "durability"
         : kind == 1 ? "I/O"
                     : "allocation",
         cuts);
}

static const char xa_batch_xids[2][LC_XID_STRING_SIZE] = {
    "00000000000000000000", "00000000000000000050"};

typedef struct xa_batch_disk {
  retention_disk disk;
  unsigned int calls, cut;
  int armed;
} xa_batch_disk;

static int xa_batch_sync(void *context, int fd) {
  xa_batch_disk *test = context;
  retention_disk *disk = &test->disk;
  int rc;
  disk->fault.armed = test->armed && ++test->calls == test->cut;
  disk->fault.calls = 0;
  disk->fault.fail = 1;
  rc = retention_disk_sync(disk, fd);
  disk->fault.armed = 0;
  return rc;
}

static void xa_batch_expect(lc_client *client, unsigned int completed,
                            lc_error *e) {
  lc_pouch *pouch = ((lc_client_handle *)client)->pouch;
  for (size_t i = 0; i < 2; ++i) {
    char owner[sizeof("owner/") + LC_XID_STRING_SIZE];
    char marker[sizeof("completed/") + LC_XID_STRING_SIZE];
    char branch_key[LC_POUCH_XA_KEY_SIZE];
    lc_xa_branch_record branch;
    xa_branch(&branch);
    strcpy(branch.txn_id, xa_batch_xids[i]);
    (void)snprintf(owner, sizeof(owner), "owner/%s", branch.txn_id);
    (void)snprintf(marker, sizeof(marker), "completed/%s", branch.txn_id);
    ok(lc_pouch_xa_branch_key(&branch, branch_key, sizeof(branch_key), e), e);
    const char *keys[] = {owner, branch_key, branch.txn_id, marker};
    for (size_t j = 0; j < 4; ++j) {
      lc_pouch_state_read_result value = {0};
      ok(lc_pouch_state_read(pouch, j == 2 ? ".txns" : ".xa", keys[j], &value,
                             e),
         e);
      REQUIRE(value.found == (j >= completed));
      lc_pouch_state_read_result_cleanup(&allocator, &value);
    }
    read_value(client, "business", branch.txn_id, "{\"value\":42}", e);
  }
}

static void xa_batch_crash(const char *directory, int sync, unsigned int cut,
                           int persisted) {
  xa_batch_disk test = {0};
  retention_disk *disk = &test.disk;
  lc_error e = {0};
  char path[1400];
  lc_xa_store *journal = NULL;
  size_t retired = 0;
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-batch", directory) <
          (int)sizeof(path));
  disk->stable = calloc(1, (size_t)PS_DEFAULT_CAPACITY);
  disk->dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1);
  disk->scratch = malloc(PS_PAGE);
  REQUIRE(disk->stable && disk->dirty && disk->scratch);
  disk->persist = persisted;
  ps_io io = {&test,         retention_disk_read,    retention_disk_write,
              xa_batch_sync, retention_disk_reserve, NULL};
  lc_client *client = xa_client(path, sync, 1, &io, &e);
  ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e), &e);
  for (size_t i = 0; i < 2; ++i)
    xa_managed_seed_id(client, journal, 7, xa_batch_xids[i], xa_batch_xids[i],
                       &e);
  lc_pouch *pouch = ((lc_client_handle *)client)->pouch;
  ok(ps_checkpoint(ps_pouch_state_root(pouch)->space, &e), &e);
  xa_batch_expect(client, 0, &e);
  test.cut = cut;
  test.armed = 1;
  int rc = lc_pouch_client_xa_retire(client, 0, 0, &retired, &e);
  REQUIRE(test.calls == (cut ? cut : 4));
  REQUIRE(cut ? rc == LC_ERR_TRANSPORT && !retired
              : rc == LC_OK && retired == 2);
  clear(&e);
  test.armed = 0;
  disk->frozen = 1;
  lc_xa_store_close(journal);
  lc_client_close(client);
  retention_disk_restore(disk, path);
  client = xa_client(path, sync, 0, NULL, &e);
  unsigned int completed = cut ? cut - 1 + (unsigned int)persisted : 4;
  xa_batch_expect(client, completed, &e);
  ok(lc_pouch_client_xa_retire(client, 0, 0, &retired, &e), &e);
  REQUIRE(retired == (completed == 4 ? 0 : 2));
  xa_batch_expect(client, 4, &e);
  lc_client_close(client);
  REQUIRE(!live && !unlink(path));
  free(disk->stable);
  free(disk->dirty);
  free(disk->scratch);
}

typedef struct xa_batch_wait {
  io_fault fault;
  lc_client *client;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  size_t retired, syncs;
  int armed, result;
} xa_batch_wait;

static int xa_batch_wait_sync(void *context, int fd) {
  xa_batch_wait *wait = context;
  int rc = ps_native_io.sync(NULL, fd);
  REQUIRE(!pthread_mutex_lock(&wait->mutex));
  if (wait->armed) {
    ++wait->syncs;
    REQUIRE(!pthread_cond_signal(&wait->changed));
  }
  REQUIRE(!pthread_mutex_unlock(&wait->mutex));
  return rc;
}
static void *xa_batch_sweep(void *context) {
  xa_batch_wait *wait = context;
  lc_error e = {0};
  wait->result =
      lc_pouch_client_xa_retire(wait->client, 0, 0, &wait->retired, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void xa_batch_contended(const char *directory) {
  char path[1400];
  xa_batch_wait wait = {0};
  ps_io io = {&wait,        disk_read, disk_write, xa_batch_wait_sync,
              disk_reserve, NULL};
  lc_error e = {0};
  lc_xa_store *journal = NULL;
  lc_pouch_txn_guard guard;
  pthread_t thread;
  struct timespec deadline;
  int timed = 0;
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-contended-batch", directory) <
          (int)sizeof(path));
  REQUIRE(!pthread_mutex_init(&wait.mutex, NULL));
  REQUIRE(!pthread_cond_init(&wait.changed, NULL));
  wait.client = xa_client(path, 1, 1, &io, &e);
  ok(lc_xa_store_open_owned(wait.client, xa_owner_ids[0], &journal, &e), &e);
  for (size_t i = 0; i < 2; ++i)
    xa_managed_seed_id(wait.client, journal, 7, xa_batch_xids[i],
                       xa_batch_xids[i], &e);
  lc_pouch *pouch = ((lc_client_handle *)wait.client)->pouch;
  ok(ps_checkpoint(ps_pouch_state_root(pouch)->space, &e), &e);
  ok(lc_pouch_txn_guard_acquire(pouch, xa_batch_xids[1], &guard, &e), &e);
  REQUIRE(!lc_pouch_txn_guard_covers(&guard, xa_batch_xids[0]));
  wait.armed = 1;
  REQUIRE(!pthread_create(&thread, NULL, xa_batch_sweep, &wait));
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
  deadline.tv_sec += 10;
  REQUIRE(!pthread_mutex_lock(&wait.mutex));
  while (wait.syncs < 4 && !timed)
    timed = pthread_cond_timedwait(&wait.changed, &wait.mutex, &deadline);
  REQUIRE(!pthread_mutex_unlock(&wait.mutex));
  lc_pouch_txn_guard_release(&guard);
  REQUIRE(!pthread_join(thread, NULL));
  REQUIRE(!timed && wait.result == LC_OK && wait.retired == 2 &&
          wait.syncs == 8);
  xa_batch_expect(wait.client, 4, &e);
  lc_xa_store_close(journal);
  lc_client_close(wait.client);
  REQUIRE(!live && !unlink(path));
  REQUIRE(!pthread_cond_destroy(&wait.changed));
  REQUIRE(!pthread_mutex_destroy(&wait.mutex));
}

static void xa_retirement_batch(const char *directory) {
  for (int sync = 0; sync < 2; ++sync) {
    xa_batch_crash(directory, sync, 0, 0);
    for (unsigned int cut = 1; cut <= 4; ++cut)
      for (int persisted = 0; persisted <= 1; ++persisted)
        xa_batch_crash(directory, sync, cut, persisted);
  }
  xa_batch_contended(directory);
}
#endif
