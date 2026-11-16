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
static void xa_owned_claim(lc_xa_store *journal, int recover, lc_error *e) {
  lc_xa_snapshot snapshot = {0};
  ok(lc_xa_store_claim_blocking(journal, retention_xid, 3600L, recover,
                                &snapshot, e),
     e);
  lc_xa_snapshot_cleanup(&snapshot);
}
static void xa_owned_progress(lc_xa_store *journal, int phase, lc_error *e) {
  lc_xa_branch_record branch;
  xa_branch(&branch);
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
static void xa_owned_stage(lc_client *client, lc_error *e) {
  lc_acquire_req request;
  lc_lease *lease = NULL;
  lc_source *source = NULL;
  lc_acquire_req_init(&request);
  request.ns = "business";
  request.key = "value";
  request.owner = "XA managed fixture";
  request.txn_id = retention_xid;
  request.ttl_seconds = 3600L;
  ok(lc_acquire(client, &request, &lease, e), e);
  ok(lc_source_from_memory("{\"value\":42}", 12U, &source, e), e);
  ok(lc_lease_update(lease, source, NULL, e), e);
  lc_source_close(source);
  lc_lease_close(lease);
}
static void xa_managed_seed(lc_client *client, lc_xa_store *journal, int action,
                            lc_error *e) {
  lc_xa_branch_record branch;
  if (!action)
    return;
  xa_owned_claim(journal, 0, e);
  if (action < 5)
    return;
  xa_branch(&branch);
  ok(lc_xa_store_enroll_blocking(journal, &branch, e), e);
  xa_owned_stage(client, e);
  ok(lc_xa_store_seal_blocking(journal, retention_xid, e), e);
  xa_owned_progress(journal, LC_XA_PREPARE_SENT, e);
  if (action != 6) {
    ok(lc_xa_store_prepare_pouch_blocking(journal, retention_xid, e), e);
    xa_owned_progress(journal, LC_XA_PREPARED, e);
    REQUIRE(lc_xa_store_decide_blocking(journal, retention_xid, 1, e) ==
            LC_ERR_PENDING);
    clear(e);
  }
  if (action == 7) {
    xa_owned_progress(journal, LC_XA_COMMIT_SENT, e);
    xa_owned_progress(journal, LC_XA_COMMITTED, e);
    ok(lc_xa_store_decide_blocking(journal, retention_xid, 1, e), e);
  }
  ok(lc_xa_store_release_blocking(journal, retention_xid, e), e);
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
    xa_owned_progress(other,
                      action == 5 ? LC_XA_COMMIT_SENT : LC_XA_ROLLBACK_SENT, e);
    xa_owned_progress(other, action == 5 ? LC_XA_COMMITTED : LC_XA_ABORTED, e);
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
  ps_io allocation_io = {&disk.fault,        disk_read,    disk_write,
                         xa_allocation_sync, disk_reserve, NULL};
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
  client = xa_client(path, sync, 0, kind == 2 ? &allocation_io : NULL, &e);
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
  for (int sync = 0; sync < 2; ++sync)
    for (int persist = 0; persist < (kind == 1 ? 3 : 1); ++persist)
      for (int action = 0; action < 8; ++action) {
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
  printf("XA managed %s cuts=%zu passed\n",
         kind == 0   ? "durability"
         : kind == 1 ? "I/O"
                     : "allocation",
         cuts);
}
#endif
