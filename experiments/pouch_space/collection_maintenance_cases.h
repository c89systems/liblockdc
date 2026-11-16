/* Included by the SDK harness: shared fixtures exercise public operations. */
#include "collection_reclaim.h"

static uint64_t collection_owner_id(fixture *f) {
  ps_namespace *ns = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_root *root = client_root(f);
  ok(ps_root_namespace_acquire(root, "journal", 0, &ns, &f->error), &f->error);
  ok(ps_catalog_get(root->space, ns->identity.owner, PS_CATALOG_COLLECTION,
                    "events/\303\245", strlen("events/\303\245"), &view,
                    &f->error),
     &f->error);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, &info, &f->error), &f->error);
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  return info.stream.owner;
}
static void maintain(fixture *f, int cleanup,
                     lc_pouch_maintenance_result *out) {
  ok(ps_pouch_collection_maintain(f->collection, 1, cleanup, out, &f->error),
     &f->error);
}
static void maintenance_lifecycle(const char *directory, int codec, int sync) {
  fixture f;
  lc_collection_head_res before, after;
  lc_collection_edit_req edit;
  lc_collection_edit_res edited;
  lc_collection_edit_status_res receipt;
  lc_collection_append_req replacement;
  lc_collection_edit_range range = {0U, 1U};
  lc_collection_lock old_pin = {0}, new_pin = {0};
  lc_pouch_maintenance_result result;
  lc_collection_metadata_req metadata_req;
  lc_collection_info info;
  ps_catalog_view *raw = NULL;
  producer p;
  unsigned char name[8] = {1U};
  setup(&f, directory, codec, sync, 1);
  lc_collection_handle *c = f.collection->impl;
  c->pouch->compaction_delete_grace_seconds = 0U;
  (void)append(f.collection, "removed", 73728U, &f.error);
  (void)append(f.collection, "small", 80U, &f.error);
  ok(lc_collection_head(f.collection, &before, &f.error), &f.error);
  lc_collection_edit_req_init(&edit);
  lc_collection_append_req_init(&replacement);
  edit.operation_id = "maintenance-edit";
  edit.expected_head = before.etag;
  edit.ranges = &range;
  edit.range_count = 1U;
  edit.replacement = &replacement;
  replacement.append_id = "replacement";
  producer_init(&p, 512U);
  ok(lc_collection_edit(f.collection, &edit, &p.source, &edited, &f.error),
     &f.error);
  (void)append(f.collection, "large", 262144U, &f.error);
  ok(lc_collection_head(f.collection, &before, &f.error), &f.error);
  ok(lc_collection_read_pin(c, c->head.snapshot, &old_pin, &f.error), &f.error);
  ok(ps_catalog_get(client_root(&f)->space, collection_owner_id(&f),
                    PS_CATALOG_SEGMENT, name, sizeof(name), &raw, &f.error),
     &f.error);
  REQUIRE(raw);
  maintain(&f, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U &&
          result.cleanup_pending_count && !result.cleanup_deleted_count);
  REQUIRE(c->head.snapshot && c->head.snapshot_extra_parts);
  ok(lc_collection_head(f.collection, &after, &f.error), &f.error);
  REQUIRE(!strcmp(before.etag, after.etag) && after.live_count == 3U);
  read_entry(f.peer, 2U, 80U, &f.error);
  read_entry(f.peer, 3U, 512U, &f.error);
  read_entry(f.peer, 4U, 262144U, &f.error);
  ok(lc_collection_read_pin(c, c->head.snapshot, &new_pin, &f.error), &f.error);
  lc_collection_lock_leave(&old_pin);
  maintain(&f, 1, &result);
  REQUIRE(result.cleanup_deleted_count && result.cleanup_pending_count);
  ps_catalog_view_close(raw);
  maintain(&f, 1, &result);
  REQUIRE(result.cleanup_deleted_count == 1U && !result.cleanup_pending_count);
  (void)append(f.peer, "tail", 8192U, &f.error);
  maintain(&f, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U &&
          result.cleanup_pending_count);
  lc_collection_lock_leave(&new_pin);
  maintain(&f, 1, &result);
  REQUIRE(result.cleanup_deleted_count);
  reopen(&f);
  read_entry(f.collection, 2U, 80U, &f.error);
  read_entry(f.collection, 3U, 512U, &f.error);
  read_entry(f.collection, 4U, 262144U, &f.error);
  read_entry(f.collection, 5U, 8192U, &f.error);
  ok(lc_collection_edit_status(f.collection, "maintenance-edit", &receipt,
                               &f.error),
     &f.error);
  REQUIRE(receipt.found && receipt.result.replacement.position == 3U);
  memset(&metadata_req, 0, sizeof(metadata_req));
  metadata_req.has_max_entries = 1;
  metadata_req.max_entries = 1U;
  ok(lc_collection_metadata(f.collection, &metadata_req, &info, &f.error),
     &f.error);
  maintain(&f, 0, &result);
  ok(lc_collection_head(f.collection, &after, &f.error), &f.error);
  REQUIRE(after.live_count == 1U);
  read_entry(f.collection, 5U, 8192U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  printf("collection maintenance lifecycle codec=%d durable=%d passed\n", codec,
         sync);
}

typedef struct maintenance_phase {
  fixture *f;
  int phase, hits, die, append_tail, change_metadata;
  int open_upload;
  lc_collection_writer *writer;
} maintenance_phase;
static void *maintenance_peer_append(void *context) {
  fixture *f = context;
  lc_error e;
  lc_error_init(&e);
  (void)append(f->peer, "concurrent-tail", 8192U, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static int maintenance_hook(void *context, lc_collection *c, int phase,
                            lc_error *e) {
  maintenance_phase *work = context;
  (void)c;
  if (phase != work->phase || work->hits++)
    return LC_OK;
  if (work->die)
    _exit(73);
  if (work->append_tail) {
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, maintenance_peer_append, work->f));
    REQUIRE(!pthread_join(thread, NULL));
    return LC_OK;
  }
  if (work->open_upload) {
    lc_collection_append_req req;
    lc_pouch_maintenance_result nested;
    lc_collection_append_req_init(&req);
    req.append_id = "started-during-copy";
    ok(lc_collection_append_begin(work->f->peer, &req, &work->writer, e), e);
    feed_writer(work->writer, 0U, 131072U, e);
    ok(ps_pouch_collection_maintain(work->f->peer, 1, 1, &nested, e), e);
    REQUIRE(nested.cleanup_pending_count == 1U &&
            !nested.compacted_collection_count);
    return LC_OK;
  }
  if (work->change_metadata) {
    lc_collection_metadata_req req = {0};
    lc_collection_append_res entry;
    req.has_retention_seconds = 1;
    req.retention_seconds = 100;
    return lc_collection_entry_metadata(work->f->peer, 1U, &req, &entry, e);
  }
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                      "injected collection maintenance phase failure", NULL,
                      NULL, "test");
}
static void maintenance_phases(const char *directory, int codec, int sync,
                               int death) {
  for (int phase = LC_COLLECTION_COMPACT_CAPTURED;
       phase <= LC_COLLECTION_COMPACT_IDS_PUBLISHED; ++phase) {
    fixture f;
    maintenance_phase work = {0};
    setup(&f, directory, codec, sync, 1);
    (void)append(f.collection, "seed", 73728U, &f.error);
    ok(ps_flush(client_root(&f)->space, &f.error), &f.error);
    work.f = &f;
    work.phase = phase;
    work.die = death;
    lc_collection_compact_test_context = &work;
    lc_collection_compact_test_hook = maintenance_hook;
    if (death) {
      close_store(&f);
      pid_t child = fork();
      REQUIRE(child >= 0);
      if (!child) {
        open_client(&f, 0);
        open_collection(&f, 0, &f.collection);
        maintain(&f, 0, NULL);
        _exit(74);
      }
      int status;
      REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
              WEXITSTATUS(status) == 73);
      open_client(&f, 0);
      open_collection(&f, 0, &f.collection);
      open_collection(&f, 0, &f.peer);
    } else {
      REQUIRE(ps_pouch_collection_maintain(f.collection, 1, 0, NULL,
                                           &f.error) == LC_ERR_TRANSPORT);
      REQUIRE(work.hits == 1);
      clear(&f.error);
      reopen(&f);
    }
    lc_collection_compact_test_hook = NULL;
    lc_collection_compact_test_context = NULL;
    read_entry(f.collection, 1U, 73728U, &f.error);
    maintain(&f, 0, NULL);
    read_entry(f.peer, 1U, 73728U, &f.error);
    close_store(&f);
    lc_error_cleanup(&f.error);
  }
}
static void maintenance_concurrency(const char *directory, int codec,
                                    int sync) {
  fixture f;
  maintenance_phase work = {0};
  lc_pouch_maintenance_result result;
  setup(&f, directory, codec, sync, 1);
  (void)append(f.collection, "seed", 73728U, &f.error);
  work.f = &f;
  work.phase = LC_COLLECTION_COMPACT_COPIED;
  work.append_tail = 1;
  lc_collection_compact_test_context = &work;
  lc_collection_compact_test_hook = maintenance_hook;
  maintain(&f, 0, &result);
  REQUIRE(work.hits == 1 && result.compacted_collection_count == 1U);
  read_entry(f.collection, 2U, 8192U, &f.error);
  work.hits = work.append_tail = 0;
  work.open_upload = 1;
  maintain(&f, 0, &result);
  REQUIRE(work.hits == 1 && result.compacted_collection_count == 1U);
  feed_writer(work.writer, 131072U, 262144U, &f.error);
  lc_collection_append_res uploaded;
  ok(lc_collection_writer_commit(work.writer, &uploaded, &f.error), &f.error);
  lc_collection_writer_close(work.writer);
  work.writer = NULL;
  work.open_upload = 0;
  REQUIRE(uploaded.position == 3U);
  read_entry(f.collection, uploaded.position, 262144U, &f.error);
  (void)append(f.peer, "after-compact", 80U, &f.error);
  work.hits = work.append_tail = 0;
  work.change_metadata = 1;
  maintain(&f, 0, &result);
  REQUIRE(work.hits == 1 && !result.compacted_collection_count &&
          result.cleanup_pending_count);
  lc_collection_compact_test_hook = NULL;
  lc_collection_compact_test_context = NULL;
  maintain(&f, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U);
  /* An admitted upload excludes capture until its private range is settled. */
  lc_collection_append_req req;
  lc_collection_writer *writer = NULL;
  lc_collection_append_req_init(&req);
  req.append_id = "unfinished";
  ok(lc_collection_append_begin(f.peer, &req, &writer, &f.error), &f.error);
  feed_writer(writer, 0U, 131072U, &f.error);
  (void)append(f.collection, "ready", 80U, &f.error);
  maintain(&f, 0, &result);
  REQUIRE(!result.compacted_collection_count);
  lc_collection_writer_close(writer);
  maintain(&f, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U);
  reopen(&f);
  read_entry(f.collection, 1U, 73728U, &f.error);
  read_entry(f.collection, 2U, 8192U, &f.error);
  read_entry(f.collection, 3U, 262144U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
}

static int maintenance_clock(clockid_t id, struct timespec *out,
                             void *context) {
  (void)id;
  out->tv_sec = (time_t)*(lc_u64 *)context;
  out->tv_nsec = 0;
  return 0;
}
static void maintenance_grace(const char *directory, int codec, int sync) {
  fixture f;
  lc_u64 now = 2000000000U;
  lc_pouch_maintenance_result result;
  ps_collection_resources list;
  uint64_t owner;
  setup(&f, directory, codec, sync, 1);
  lc_collection_handle *c = f.collection->impl;
  c->pouch->compaction_delete_grace_seconds = 100U;
  lc_pouch_test_queue_clock_gettime = maintenance_clock;
  lc_pouch_test_queue_time_context = &now;
  (void)append(f.collection, "seed", 73728U, &f.error);
  owner = collection_owner_id(&f);
  maintain(&f, 0, &result);
  REQUIRE(result.cleanup_pending_count && !result.cleanup_deleted_count);
  now -= 10U;
  maintain(&f, 1, &result);
  REQUIRE(result.cleanup_pending_count && !result.cleanup_deleted_count);
  now += 60U;
  (void)append(f.collection, "next", 8192U, &f.error);
  maintain(&f, 0, &result);
  ok(ps_collection_resources_capture(client_root(&f)->space, owner, &list,
                                     &f.error),
     &f.error);
  size_t markers = 0U;
  for (size_t i = 0U; i < list.count; ++i)
    if (list.items[i].kind == PS_CATALOG_CONTROL) {
      ++markers;
      if (list.items[i].id == 1U)
        REQUIRE(list.items[i].marked == 2000000000U);
    }
  REQUIRE(markers > 1U);
  /* A death between canonical-name and marker removal leaves a retry alias. */
  for (size_t i = 0U; i < list.count; ++i)
    if (list.items[i].id == 1U && list.items[i].kind == PS_CATALOG_SEGMENT)
      ok(ps_catalog_remove(client_root(&f)->space, owner, list.items[i].kind,
                           list.items[i].name, list.items[i].length,
                           &list.items[i].binding.revision, &f.error),
         &f.error);
  ps_collection_resources_clear(&list);
  reopen(&f);
  c = f.collection->impl;
  c->pouch->compaction_delete_grace_seconds = 100U;
  now = 2000000101U;
  maintain(&f, 1, &result);
  REQUIRE(result.cleanup_deleted_count && result.cleanup_pending_count);
  read_entry(f.collection, 1U, 73728U, &f.error);
  read_entry(f.collection, 2U, 8192U, &f.error);
  ok(lc_collection_drop(f.collection, &f.error), &f.error);
  lc_collection_close(f.peer);
  lc_collection_close(f.collection);
  f.peer = f.collection = NULL;
  ok(ps_collection_resources_capture(client_root(&f)->space, owner, &list,
                                     &f.error),
     &f.error);
  REQUIRE(!list.count);
  ps_collection_resources_clear(&list);
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static void maintenance_reuse(const char *directory, int codec, int sync) {
  fixture f;
  ps_stats stats;
  lc_pouch_maintenance_result result;
  lc_collection_delete_res deleted;
  lc_collection_lease_req lease;
  lc_collection *owned = NULL;
  setup(&f, directory, codec, sync, 1);
  lc_collection_close(f.peer);
  f.peer = NULL;
  lc_collection_handle *c = f.collection->impl;
  c->pouch->compaction_delete_grace_seconds = 0U;
  for (unsigned int turn = 0U; turn < 4U; ++turn) {
    char id[32];
    snprintf(id, sizeof(id), "reuse-%u", turn);
    (void)append(f.collection, id, 524288U, &f.error);
    if (turn)
      ok(lc_collection_delete_entry(f.collection, turn, &deleted, &f.error),
         &f.error);
    maintain(&f, 0, &result);
    REQUIRE(result.compacted_collection_count == 1U &&
            result.cleanup_deleted_count);
    ok(ps_status(client_root(&f)->space, &stats, &f.error), &f.error);
    REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY && !stats.retired_bytes &&
            stats.allocated_bytes < PS_DEFAULT_CAPACITY / 2U);
    read_entry(f.collection, turn + 1U, 524288U, &f.error);
  }
  /* Maintenance may relocate a private leased journal without granting a
   * public body read or changing the lease's fencing identity. */
  lc_collection_lease_req_init(&lease);
  lease.owner = "maintenance";
  lease.private_read = 1;
  ok(lc_collection_acquire(f.collection, &lease, &owned, &f.error), &f.error);
  ok(lc_collection_delete_entry(owned, 4U, &deleted, &f.error), &f.error);
  maintain(&f, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U);
  ok(lc_collection_release(owned, &f.error), &f.error);
  lc_collection_head_res head;
  ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
  REQUIRE(!head.live_count && head.upper_position == 4U);
  reopen(&f);
  ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
  REQUIRE(!head.live_count && head.upper_position == 4U);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static void maintenance_suite(const char *directory, int codec, int sync,
                              int death) {
  maintenance_lifecycle(directory, codec, sync);
  maintenance_concurrency(directory, codec, sync);
  maintenance_grace(directory, codec, sync);
  maintenance_reuse(directory, codec, sync);
  maintenance_phases(directory, codec, sync, 0);
  if (death)
    maintenance_phases(directory, codec, sync, 1);
}
