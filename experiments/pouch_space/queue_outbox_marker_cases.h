#ifndef POUCH_SPACE_QUEUE_OUTBOX_MARKER_CASES_H
#define POUCH_SPACE_QUEUE_OUTBOX_MARKER_CASES_H
/* Shared typed marker codecs/policies with both physical owners. */
#include "lc_pouch_outbox_store.h"
#include "outbox_markers.h"
#include "owned.h"
#include "root_internal.h"

static void outbox_marker_store_put(lc_pouch *p, int kind, const char *name,
                                    const void *bytes, size_t length,
                                    lc_error *e) {
  lc_pouch_outbox_store store = {0};
  ok(lc_pouch_outbox_store_open(p, "work/a", kind, 1, &store, e), e);
  ok(store.ops->write(store.context, name, bytes, length, e), e);
  ok(lc_pouch_outbox_store_close(&store, LC_OK, e), e);
}
static void outbox_marker_ids(char ids[3][LC_XID_STRING_SIZE], lc_error *e) {
  size_t i, j;
  char swap[LC_XID_STRING_SIZE];
  for (i = 0U; i < 3U; ++i)
    ok(lc_xid_new(ids[i], e), e);
  for (i = 0U; i < 3U; ++i)
    for (j = i + 1U; j < 3U; ++j)
      if (strcmp(ids[i], ids[j]) > 0) {
        memcpy(swap, ids[i], sizeof(swap));
        memcpy(ids[i], ids[j], sizeof(swap));
        memcpy(ids[j], swap, sizeof(swap));
      }
}
static void outbox_marker_reclaim(lc_pouch *p, lc_error *e) {
  static const char *const ids[] = {"aaa", "mmm", "zzz"};
  char *id = NULL, *parent = NULL, *state = NULL;
  size_t turn;
  ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "missing", "aaa", e), e);
  REQUIRE(lc_pouch_outbox_state_reclaim_marker_remove(p, "missing", "aaa",
                                                      NULL) == LC_OK);
  for (turn = 0U; turn < 3U; ++turn)
    ok(lc_pouch_outbox_state_reclaim_marker_write(
           p, "work/a", ids[turn], "parent\n/key", "checkpoint", e),
       e);
  for (turn = 0U; turn < 4U; ++turn) {
    ok(lc_pouch_outbox_state_reclaim_marker_take(
           p, "work/a", turn ? ids[(turn - 1U) % 3U] : NULL, &id, &parent,
           &state, e),
       e);
    REQUIRE(id && !strcmp(id, ids[turn % 3U]) && parent &&
            !strcmp(parent, "parent\n/key") && state &&
            !strcmp(state, "checkpoint"));
    lc_free_with_allocator(&p->allocator, id);
    lc_free_with_allocator(&p->allocator, parent);
    lc_free_with_allocator(&p->allocator, state);
    id = parent = state = NULL;
  }
  outbox_marker_store_put(p, -1, "aaa", "corrupt", 7U, e);
  REQUIRE(lc_pouch_outbox_state_reclaim_marker_take(
              p, "work/a", NULL, &id, &parent, &state, e) == LC_ERR_PROTOCOL);
  REQUIRE(!id && !parent && !state);
  clear(e);
  for (turn = 0U; turn < 3U; ++turn) {
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", ids[turn], e),
       e);
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", ids[turn], e),
       e);
  }
  ok(lc_pouch_outbox_state_reclaim_marker_take(p, "work/a", NULL, &id, &parent,
                                               &state, e),
     e);
  REQUIRE(!id && !parent && !state);
}
static void outbox_marker_retention(lc_pouch *p, int kind, lc_error *e) {
  char ids[3][LC_XID_STRING_SIZE];
  char name[17U + LC_XID_STRING_SIZE];
  lc_pouch_outbox_retention_marker marker = {0};
  lc_pouch_unix_seconds due = 0;
  size_t i;
  outbox_marker_ids(ids, e);
  for (i = 0U; i < 3U; ++i)
    ok(lc_pouch_outbox_retention_marker_write(p, "work/a", kind, ids[i],
                                              "parent/key",
                                              i == 2U ? 2000 : 1000, 32U, e),
       e);
  ok(lc_pouch_outbox_retention_marker_take(
         p, "work/a", kind, NULL, 999, SIZE_MAX, UINT64_MAX, &marker, &due, e),
     e);
  REQUIRE(!marker.marker_id && due == 1000 &&
          marker.queued_count ==
              (kind == LC_POUCH_RETENTION_DEAD_LETTER ? 3U : 0U));
  lc_pouch_outbox_retention_marker_cleanup(p, &marker);
  /* A count breach permits one bounded turn before deadline, preserving the
   * common selector's cursor and minimum next-deadline information. */
  ok(lc_pouch_outbox_retention_marker_take(p, "work/a", kind, NULL, 999, 0U,
                                           UINT64_MAX, &marker, &due, e),
     e);
  REQUIRE(marker.marker_id && marker.outbox_key &&
          (kind == LC_POUCH_RETENTION_DEAD_LETTER ? due == marker.due_at_unix
                                                  : due == 1000));
  lc_pouch_outbox_retention_marker_cleanup(p, &marker);
  if (kind == LC_POUCH_RETENTION_DEAD_LETTER)
    strcpy(name, ids[0]);
  else
    REQUIRE(snprintf(name, sizeof(name), "%016lx-%s", 1000UL, ids[0]) > 0);
  outbox_marker_store_put(p, kind, name, "corrupt", 7U, e);
  REQUIRE(lc_pouch_outbox_retention_marker_take(p, "work/a", kind, NULL, 1000,
                                                SIZE_MAX, UINT64_MAX, &marker,
                                                &due, e) == LC_ERR_PROTOCOL);
  REQUIRE(!marker.marker_id && !marker.outbox_key &&
          !strcmp(marker.selected_id, ids[0]));
  lc_pouch_outbox_retention_marker_cleanup(p, &marker);
  clear(e);
  ok(lc_pouch_outbox_retention_marker_take(p, "work/a", kind, ids[0], 1000,
                                           SIZE_MAX, UINT64_MAX, &marker, &due,
                                           e),
     e);
  REQUIRE(marker.marker_id && !strcmp(marker.marker_id, ids[1]) &&
          !strcmp(marker.outbox_key, "parent/key"));
  lc_pouch_outbox_retention_marker_cleanup(p, &marker);
  for (i = 0U; i < 3U; ++i) {
    ok(lc_pouch_outbox_retention_marker_remove(p, "work/a", kind, ids[i],
                                               i == 2U ? 2000 : 1000, e),
       e);
    ok(lc_pouch_outbox_retention_marker_remove(p, "work/a", kind, ids[i],
                                               i == 2U ? 2000 : 1000, e),
       e);
  }
}
static void outbox_marker_pinned(lc_pouch *p, lc_error *e) {
  ps_root *root = ps_pouch_state_root(p);
  ps_namespace *ns = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_reader *reader = NULL;
  unsigned char name[] = {0U, 'p', 'i', 'n'};
  uint64_t reclaimed, skipped;
  int pending;
  ok(lc_pouch_outbox_state_reclaim_marker_write(p, "work/a", "pin", "parent",
                                                "state", e),
     e);
  ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, e), e);
  REQUIRE(!ns->data && !ns->collections);
  ok(ps_catalog_get(root->space, ns->identity.owner, PS_CATALOG_CONTROL, name,
                    sizeof(name), &view, e),
     e);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, &info, e), e);
  ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "pin", e), e);
  ps_key key = {info.stream.owner, 1U, PS_CONTROL};
  ok(ps_read_open(root->space, key, NULL, &reader, e), e);
  ps_reader_close(reader);
  ok(ps_catalog_orphans_pending(root->space, &pending, e), e);
  REQUIRE(pending);
  ps_catalog_view_close(view);
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, e), e);
  REQUIRE(reclaimed);
  REQUIRE(ps_read_open(root->space, key, NULL, &reader, e) == LC_ERR_INVALID);
  clear(e);
  ps_namespace_release(ns);
  ok(ps_checkpoint(root->space, e), e);
}
static void outbox_marker_owner_identity(lc_pouch *p, lc_error *e) {
  ps_root *root = ps_pouch_state_root(p);
  ps_namespace *ns = NULL;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  ps_writer *writer = NULL;
  ps_reader *reader = NULL;
  uint64_t reclaimed, skipped;
  ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, e), e);
  ps_key stream = {ns->identity.owner, UINT64_MAX, PS_STREAM};
  ps_key control = {ns->identity.owner, 1U, PS_CONTROL};
  /* A canonical stream can use the same numeric key. Its owner's unrelated
   * control must survive reclamation when no marker-owner tag is present. */
  ok(ps_owned_open(root->space, NULL, stream, 1, NULL, &head, &status, e), e);
  ok(ps_owned_seal(head, e), e);
  ps_owned_close(head);
  ok(ps_begin(root->space, NULL, control, &writer, e), e);
  ok(ps_write(writer, "keep", 4U, e), e);
  ok(ps_publish(writer, NULL, e), e);
  ps_writer_close(writer);
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, e), e);
  REQUIRE(reclaimed);
  ok(ps_read_open(root->space, control, NULL, &reader, e), e);
  REQUIRE(ps_read_size(reader) == 4U);
  ps_reader_close(reader);
  ok(ps_delete(root->space, control, e), e);
  ps_namespace_release(ns);
}
static void outbox_markers(const char *directory) {
  lc_error e;
  char path[1400];
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      REQUIRE(snprintf(path, sizeof(path), "%s/outbox-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      lc_pouch *p = visit_open(path, native, sync, 1, NULL, &allocator, &e);
      outbox_marker_reclaim(p, &e);
      for (int kind = 0; kind < LC_POUCH_RETENTION_KIND_COUNT; ++kind)
        outbox_marker_retention(p, kind, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
      if (!native) {
        outbox_marker_pinned(p, &e);
        outbox_marker_owner_identity(p, &e);
      }
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "outbox-");
  lc_error_cleanup(&e);
}
typedef struct outbox_notification {
  lc_pouch *pouch;
  const char *queue;
  int done, result;
} outbox_notification;
static void *outbox_notification_wait(void *context) {
  outbox_notification *wait = context;
  wait->result =
      lc_pouch_queue_watch_wait(wait->pouch, "work/a", wait->queue, 5000U);
  __atomic_store_n(&wait->done, 1, __ATOMIC_RELEASE);
  return NULL;
}
static void outbox_notifications(const char *directory) {
  lc_error e;
  char path[1400];
  ps_pouch_state_options options = {0};
  lc_pouch *p = NULL;
  pthread_t threads[3];
  outbox_notification waits[3];
  lc_error_init(&e);
  options.logical.single_writer = options.logical.single_writer_set = 1;
  options.logical.query_indexing_enabled_set = 1;
  options.logical.query_engine = "scan";
  options.logical.background_compaction_enabled_set = 1;
  options.logical.queue_watch = 1;
  REQUIRE(snprintf(path, sizeof(path), "%s/notifications", directory) <
          (int)sizeof(path));
  ok(ps_pouch_state_open(path, 1, &allocator, &options, &p, &e), &e);
  REQUIRE(!strcmp(p->queue_watch_mode, "inprocess"));
  REQUIRE(lc_pouch_queue_watch_wait(p, "work/a", "queue", 0U) == 0);
  REQUIRE(lc_pouch_queue_watch_wait(p, "", "queue", 0U) == -1);
  for (size_t i = 0U; i < 3U; ++i) {
    memset(&waits[i], 0, sizeof(waits[i]));
    waits[i].pouch = p;
    waits[i].queue = i == 2U ? "other" : "queue";
    REQUIRE(!pthread_create(&threads[i], NULL, outbox_notification_wait,
                            &waits[i]));
  }
  size_t attempts;
  for (attempts = 0U; attempts < 1000U; ++attempts) {
    if (ps_pouch_queue_waiter_count(p, "work/a", "queue") == 2U &&
        ps_pouch_queue_waiter_count(p, "work/a", "other") == 1U)
      break;
    usleep(1000);
  }
  REQUIRE(attempts < 1000U);
  p->storage.ops->queue_notify(p->storage.context, "other-ns", "queue");
  p->storage.ops->queue_notify(p->storage.context, "work/a", "queue");
  for (size_t i = 0U; i < 2U; ++i) {
    REQUIRE(!pthread_join(threads[i], NULL));
    REQUIRE(waits[i].result == 1);
  }
  REQUIRE(!__atomic_load_n(&waits[2].done, __ATOMIC_ACQUIRE));
  p->storage.ops->queue_notify(p->storage.context, "work/a", "other");
  REQUIRE(!pthread_join(threads[2], NULL) && waits[2].result == 1);
  REQUIRE(!ps_pouch_queue_waiter_count(p, "work/a", "queue"));
  lc_pouch_close(p);
  REQUIRE(!live);
  one_file(path);
  lc_error_cleanup(&e);
}
typedef struct outbox_marker_result {
  char *id, *parent, *state;
  lc_pouch_outbox_retention_marker marker;
  lc_pouch_unix_seconds due;
} outbox_marker_result;
static int outbox_marker_kind(int operation) {
  return operation <= 3 || operation == 11 ? -1 : operation <= 7 ? 0 : 1;
}
static void outbox_marker_seed(lc_pouch *p, int operation, lc_error *e) {
  int kind = outbox_marker_kind(operation);
  if (operation == 0 || operation == 4 || operation == 8)
    return;
  if (kind == -1)
    ok(lc_pouch_outbox_state_reclaim_marker_write(
           p, "work/a", "marker", "old-parent", "checkpoint", e),
       e);
  else
    ok(lc_pouch_outbox_retention_marker_write(p, "work/a", kind, retention_xid,
                                              "old-parent", 1000, 32U, e),
       e);
  if (operation == 11) {
    ps_root *root = ps_pouch_state_root(p);
    ps_namespace *ns = NULL;
    ps_catalog_view *view = NULL;
    const unsigned char name[] = {0U, 'm', 'a', 'r', 'k', 'e', 'r'};
    ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, e), e);
    ok(ps_catalog_get(root->space, ns->identity.owner, PS_CATALOG_CONTROL, name,
                      sizeof(name), &view, e),
       e);
    REQUIRE(view);
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "marker", e),
       e);
    ps_catalog_view_close(view);
    ps_namespace_release(ns);
  }
}
static int outbox_marker_action(lc_pouch *p, int operation,
                                outbox_marker_result *result, lc_error *e) {
  int kind = outbox_marker_kind(operation);
  memset(result, 0, sizeof(*result));
  if (operation == 0 || operation == 1)
    return lc_pouch_outbox_state_reclaim_marker_write(
        p, "work/a", "marker", "new-parent", "checkpoint", e);
  if (operation == 2)
    return lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "marker",
                                                       e);
  if (operation == 3)
    return lc_pouch_outbox_state_reclaim_marker_take(
        p, "work/a", NULL, &result->id, &result->parent, &result->state, e);
  if (operation == 4 || operation == 5 || operation == 8)
    return lc_pouch_outbox_retention_marker_write(
        p, "work/a", kind, retention_xid, "new-parent", 1000, 64U, e);
  if (operation == 6 || operation == 9)
    return lc_pouch_outbox_retention_marker_remove(p, "work/a", kind,
                                                   retention_xid, 1000, e);
  if (operation == 7 || operation == 10)
    return lc_pouch_outbox_retention_marker_take(
        p, "work/a", kind, NULL, 1000, SIZE_MAX, UINT64_MAX, &result->marker,
        &result->due, e);
  uint64_t reclaimed, skipped;
  ps_root *root = ps_pouch_state_root(p);
  int rc = ps_root_reclaim_orphans(root, &reclaimed, &skipped, e);
  if (rc == LC_OK)
    rc = ps_checkpoint(root->space, e);
  return rc;
}
static void outbox_marker_result_clear(lc_pouch *p,
                                       outbox_marker_result *result) {
  lc_free_with_allocator(&p->allocator, result->id);
  lc_free_with_allocator(&p->allocator, result->parent);
  lc_free_with_allocator(&p->allocator, result->state);
  lc_pouch_outbox_retention_marker_cleanup(p, &result->marker);
  memset(result, 0, sizeof(*result));
}
static void outbox_marker_verify(lc_pouch *p, int operation, int completed,
                                 lc_error *e) {
  int kind = outbox_marker_kind(operation);
  outbox_marker_result result = {0};
  const char *parent;
  if (kind == -1) {
    ok(lc_pouch_outbox_state_reclaim_marker_take(
           p, "work/a", NULL, &result.id, &result.parent, &result.state, e),
       e);
    parent = result.parent;
    if (parent)
      REQUIRE(result.id && !strcmp(result.id, "marker") && result.state &&
              !strcmp(result.state, "checkpoint"));
  } else {
    ok(lc_pouch_outbox_retention_marker_take(p, "work/a", kind, NULL, 1000,
                                             SIZE_MAX, UINT64_MAX,
                                             &result.marker, &result.due, e),
       e);
    parent = result.marker.outbox_key;
    if (parent)
      REQUIRE(result.marker.marker_id &&
              !strcmp(result.marker.marker_id, retention_xid));
  }
  int first = operation == 0 || operation == 4 || operation == 8;
  int replace = operation == 1 || operation == 5;
  int remove =
      operation == 2 || operation == 6 || operation == 9 || operation == 11;
  if (parent)
    REQUIRE(!strcmp(parent, "old-parent") || !strcmp(parent, "new-parent"));
  if (completed) {
    if (first || replace)
      REQUIRE(parent && !strcmp(parent, "new-parent"));
    else if (remove)
      REQUIRE(!parent);
    else
      REQUIRE(parent && !strcmp(parent, "old-parent"));
  } else if (first) {
    REQUIRE(!parent || !strcmp(parent, "new-parent"));
  } else if (replace) {
    REQUIRE(parent);
  } else if (!remove) {
    REQUIRE(parent && !strcmp(parent, "old-parent"));
  }
  if (operation == 11)
    REQUIRE(!parent);
  outbox_marker_result_clear(p, &result);
  /* Retry on the recovered owner, then remove every public marker. Deferred
   * control cleanup must converge with no unbound identity left behind. */
  ok(outbox_marker_action(p, operation, &result, e), e);
  outbox_marker_result_clear(p, &result);
  if (kind == -1)
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "marker", e),
       e);
  else
    ok(lc_pouch_outbox_retention_marker_remove(p, "work/a", kind, retention_xid,
                                               1000, e),
       e);
  uint64_t reclaimed, skipped;
  int pending;
  ps_root *root = ps_pouch_state_root(p);
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, e), e);
  ok(ps_catalog_orphans_pending(root->space, &pending, e), e);
  REQUIRE(!pending);
  ok(ps_checkpoint(root->space, e), e);
}
static size_t outbox_marker_trial(const char *directory, int operation,
                                  int sync, int persist, size_t cut,
                                  int allocation_mode) {
  lc_error e;
  outbox_marker_result result;
  retention_disk disk = {0};
  char path[1400];
  int rc;
  size_t sites;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/marker-fault", directory) <
          (int)sizeof(path));
  ps_io io = {&disk,
              retention_disk_read,
              retention_disk_write,
              retention_disk_sync,
              retention_disk_reserve,
              NULL};
  if (!allocation_mode) {
    disk.persist = persist;
    disk.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
    disk.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
    disk.scratch = malloc(PS_PAGE);
    REQUIRE(disk.stable && disk.dirty && disk.scratch);
  }
  lc_pouch *p = visit_open(path, 0, sync, 1, allocation_mode ? NULL : &io,
                           &allocator, &e);
  outbox_marker_seed(p, operation, &e);
  if (allocation_mode) {
    allocation = 0U;
    fail_at = cut;
    armed = 1;
  } else {
    disk.fault.calls = 0U;
    disk.fault.fail = cut;
    disk.fault.armed = 1;
  }
  rc = outbox_marker_action(p, operation, &result, &e);
  armed = disk.fault.armed = 0;
  sites = allocation_mode ? allocation : disk.fault.calls;
  if (cut)
    REQUIRE(sites >= cut);
  else
    ok(rc, &e);
  if (rc != LC_OK)
    REQUIRE(!result.id && !result.parent && !result.state &&
            !result.marker.marker_id && !result.marker.outbox_key);
  outbox_marker_result_clear(p, &result);
  clear(&e);
  disk.frozen = 1;
  lc_pouch_close(p);
  REQUIRE(!live);
  if (!allocation_mode)
    retention_disk_restore(&disk, path);
  p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
  outbox_marker_verify(p, operation, rc == LC_OK, &e);
  lc_pouch_close(p);
  REQUIRE(!live);
  one_file(path);
  REQUIRE(!unlink(path));
  free(disk.stable);
  free(disk.dirty);
  free(disk.scratch);
  lc_error_cleanup(&e);
  return sites;
}
static void outbox_marker_faults(const char *directory, int allocation_mode) {
  size_t cuts = 0U;
  for (int sync = 0; sync < 2; ++sync)
    for (int persist = 0; persist < (allocation_mode ? 1 : 2); ++persist)
      for (int operation = 0; operation < 12; ++operation) {
        size_t sites = outbox_marker_trial(directory, operation, sync, persist,
                                           0U, allocation_mode);
        REQUIRE(sites);
        for (size_t cut = 1U; cut <= sites; ++cut) {
          (void)outbox_marker_trial(directory, operation, sync, persist, cut,
                                    allocation_mode);
          ++cuts;
        }
      }
  printf("outbox marker %s cuts=%zu passed\n",
         allocation_mode ? "allocation" : "I/O", cuts);
}
#endif
