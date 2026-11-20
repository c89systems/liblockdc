#ifndef POUCH_SPACE_TAIL_CASES_H
#define POUCH_SPACE_TAIL_CASES_H
#include "reclaim_cases.h"

static void active_retirement_delete(ps_namespace *ns) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry deletion = {0};
  ps_namespace_append *append = NULL;
  uint64_t bytes;
  deletion.key = "lease/0";
  deletion.etag = "deleted";
  deletion.record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
  deletion.seen = 1;
  deletion.index_seq = deletion.version = 2;
  deletion.lifetime = 1;
  OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &deletion, 0,
                                            &bytes, &error));
  OK(ps_namespace_append_begin(ns, 0, bytes, PS_APPEND_MAINTENANCE, NULL,
                               &append, &error));
  OK(lc_pouch_state_record_write_snapshot(
      &allocator, ps_namespace_append_output(append),
      ps_namespace_append_leaf(append), &deletion, NULL, NULL, 0, &error));
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
  OK(ps_namespace_checkpoint_flush(ns, &error));
}
static int active_retirement_seen(void *context, uint64_t segment,
                                  const lc_pouch_state_entry *entry,
                                  lc_error *e) {
  int *seen = context;
  (void)segment;
  (void)e;
  if (!entry->control) {
    CHECK(!strcmp(entry->key, "lease/0") && !entry->found &&
          entry->version == 2 && entry->index_seq == 2);
    ++*seen;
  }
  return LC_OK;
}
static void active_retirement_verify(ps_namespace *ns) {
  int seen = 0;
  ps_namespace_view *view = NULL;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, active_retirement_seen, &seen, &error));
  CHECK(seen == 1);
  ps_namespace_view_close(view);
}
static void namespace_active_retirement(int durable, int shared) {
  ps_root_options options = {0};
  ps_root *root = NULL, *peer = NULL;
  ps_namespace *ns = NULL, *other_ns = NULL;
  ps_namespace_view *view = NULL;
  namespace_repack_pin pin = {0};
  ps_namespace_status status;
  ps_stats before, after;
  ps_namespace_retirement *retirement = NULL, *stale = NULL;
  uint64_t closed, pending;
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.maximum_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.shared = shared;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 1, &ns, &error));
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_unlink_active(ns, 1, &retirement, &error));
  CHECK(!retirement);
  if (shared) {
    OK(ps_root_open(path, 0, &options, &peer, &error));
    OK(ps_root_namespace_acquire(peer, "tail", 0, &other_ns, &error));
  } else
    other_ns = ns;
  pin.ns = other_ns;
  OK(ps_namespace_view_open(other_ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 1 && pin.body);
  active_retirement_delete(ns);
  BAD(ps_namespace_unlink_active(ns, 1, &retirement, &error));
  CHECK(!retirement);
  ps_namespace_view_close(view);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  OK(ps_namespace_unlink_active(ns, 1, &retirement, &error));
  CHECK(retirement);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.syncs - before.syncs == 3);
  OK(ps_namespace_status_get(other_ns, &status, &error));
  CHECK(status.active_segment == 2 && !status.tail);
  OK(ps_namespace_unlink_active(ns, 1, &stale, &error));
  CHECK(!stale);
  OK(ps_namespace_reap(ns, &closed, &pending, &error));
  CHECK(!closed && pending == 1);
  active_retirement_verify(other_ns);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  checkpoint_put(shared ? peer : root, other_ns, "next", 3, 1, 3, 1, 0);
  OK(ps_namespace_checkpoint_flush(other_ns, &error));
  OK(ps_namespace_status_get(other_ns, &status, &error));
  CHECK(status.active_segment == 2 && status.covered_segment == 2);
  OK(ps_status(ps_root_space(root), &before, &error));
  OK(ps_namespace_retire_unlinked(ns, retirement, &error));
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.syncs - before.syncs == 1);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  ps_namespace_read_close(pin.body);
  OK(ps_namespace_reap(ns, &closed, &pending, &error));
  CHECK(closed == 1 && !pending);
  if (shared) {
    ps_namespace_release(other_ns);
    OK(ps_root_close(peer, &error));
  }
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
static void namespace_active_retirement_faults(int durable, int shared) {
  model m;
  ps_io io = model_init_capacity(&m, (size_t)PS_DEFAULT_CAPACITY);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_status status;
  size_t sites = 0;
  unsigned char *baseline = malloc((size_t)PS_DEFAULT_CAPACITY);
  ps_namespace_retirement *retirement = NULL;
  uint64_t orphans, skipped;
  CHECK(baseline);
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.maximum_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.shared = shared;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 1, &ns, &error));
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  active_retirement_delete(ns);
  OK(ps_checkpoint(ps_root_space(root), &error));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  memcpy(baseline, m.stable, (size_t)PS_DEFAULT_CAPACITY);
  for (size_t cut = 0; cut <= sites; ++cut) {
    memcpy(m.stable, baseline, (size_t)PS_DEFAULT_CAPACITY);
    crash(&m);
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "tail", 0, &ns, &error));
    m.operations = 0;
    m.fail_at = cut;
    m.tear = m.persist_torn = 1;
    int rc = ps_namespace_unlink_active(ns, 1, &retirement, &error);
    if (rc == LC_OK && retirement)
      rc = ps_namespace_retire_unlinked(ns, retirement, &error);
    m.fail_at = 0;
    if (!cut) {
      OK(rc);
      sites = m.operations;
      CHECK(sites && retirement);
    } else
      BAD(rc);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    crash(&m);
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "tail", 0, &ns, &error));
    active_retirement_verify(ns);
    OK(ps_namespace_status_get(ns, &status, &error));
    CHECK(status.active_segment == 1 || status.active_segment == 2);
    if (status.active_segment == 1) {
      OK(ps_namespace_unlink_active(ns, 1, &retirement, &error));
      CHECK(retirement);
      OK(ps_namespace_retire_unlinked(ns, retirement, &error));
    } else
      OK(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &error));
    OK(ps_root_reclaim_orphans(root, &orphans, &skipped, &error));
    CHECK(!skipped);
    checkpoint_put(root, ns, "next", 3, 1, 3, 1, 0);
    OK(ps_namespace_checkpoint_flush(ns, &error));
    OK(ps_namespace_status_get(ns, &status, &error));
    CHECK(status.active_segment == 2 && status.covered_segment == 2);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
  }
  printf("active retirement sync%d shared%d: %zu write/sync cuts\n", durable,
         shared, sites);
  free(baseline);
  free(m.stable);
  clean();
}

typedef struct tail_storage {
  uint64_t bytes, runs, sources, capacity;
} tail_storage;
static int tail_storage_visit(void *context, const ps_namespace_source *source,
                              lc_error *e) {
  tail_storage *s = context;
  (void)e;
  CHECK((!source->runs) == (!source->stored_bytes));
  s->bytes += source->stored_bytes;
  s->runs += source->runs;
  s->capacity += source->capacity;
  ++s->sources;
  return LC_OK;
}
static void namespace_tail(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_repack_result result;
  ps_namespace_status before, after;
  namespace_repack_pin pin = {0};
  tail_storage storage = {0};
  ps_stats held, released;
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 1, &ns, &error));
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  checkpoint_put(root, ns, "lease/1", 2, 1, 2, 1, 0);
  checkpoint_put(root, ns, "lease/1", 3, 2, 2, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_status_get(ns, &before, &error));
  CHECK(before.active_segment == 1 && before.tail < PS_PAGE - PS_RUN_HEADER &&
        before.covered_offset == before.tail);
  OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
  CHECK(storage.sources == 1 && storage.runs == 1 && storage.bytes == PS_PAGE);
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 2 && pin.body);
  CHECK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL, &result,
                            &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  ps_namespace_view_close(view);
  OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL, &result,
                         &error));
  CHECK(result.replaced == 1 && result.finished_tail && !result.pending);
  memset(&storage, 0, sizeof(storage));
  OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
  CHECK(storage.sources == 2 && storage.runs == 1 && storage.bytes < PS_PAGE);
  OK(ps_namespace_status_get(ns, &after, &error));
  CHECK(after.active_segment == 2 && !after.tail &&
        after.covered_segment == 1 && after.covered_offset == before.tail);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &held, &error));
  CHECK(held.retired_bytes >= PS_PAGE);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  OK(ps_status(ps_root_space(root), &released, &error));
  CHECK(released.allocated_bytes + PS_PAGE <= held.allocated_bytes);
  for (unsigned int i = 0; i < 4; ++i) {
    OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 2, 1, NULL, NULL, &result,
                           &error));
    CHECK(!result.visited && !result.replaced && !result.pending);
  }
  checkpoint_put(root, ns, "lease/2", 4, 1, 3, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_status_get(ns, &after, &error));
  CHECK(after.active_segment == 2 && after.tail && after.covered_segment == 2);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 0, &ns, &error));
  pin.ns = ns;
  pin.count = 0;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 3);
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
static void tail_seed(ps_namespace *ns, ps_root *root) {
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  checkpoint_put(root, ns, "lease/1", 2, 1, 2, 1, 0);
  checkpoint_put(root, ns, "lease/1", 3, 2, 2, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
}
static void namespace_tail_dense(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_repack_result result;
  ps_namespace_status status;
  tail_storage storage = {0};
  churn_pin pin = {0};
  uint64_t body = PS_CHUNK, prefix = 0, target = 0, written;
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  for (int trial = 0; trial < 2; ++trial) {
    clean();
    OK(ps_root_open(path, 1, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "dense", 1, &ns, &error));
    written = namespace_churn_put(root, ns, "stable", 1, 1, 1, (size_t)body);
    OK(ps_namespace_checkpoint_flush(ns, &error));
    if (!trial) {
      OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
      prefix = written - body;
      target = storage.capacity - 1;
      CHECK(target > prefix + PS_CHUNK);
      body = target - prefix;
    } else {
      CHECK(written == target);
      for (unsigned int pass = 0; pass < 3; ++pass) {
        OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 16, NULL, NULL,
                               &result, &error));
        CHECK(!result.finished_tail && !result.pending &&
              (!pass || !result.replaced));
        OK(ps_namespace_status_get(ns, &status, &error));
        CHECK(status.active_segment == 1 && status.tail == target);
      }
      pin.ns = ns;
      OK(ps_namespace_view_open(ns, &view, &error));
      OK(ps_namespace_view_visit(view, 1, namespace_churn_visit, &pin, &error));
      CHECK(pin.count == 1);
      ps_namespace_view_close(view);
      ps_namespace_read_close(pin.body);
    }
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
  }
  clean();
}
static void namespace_tail_faults(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_repack_result result;
  ps_namespace_status status;
  namespace_repack_pin pin = {0};
  unsigned char *baseline = malloc((size_t)PS_MIB);
  size_t sites;
  CHECK(baseline);
  clean();
  options.storage.initial_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 1, &ns, &error));
  tail_seed(ns, root);
  OK(ps_checkpoint(ps_root_space(root), &error));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 3; ++kind) {
    sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      OK(ps_root_open(path, 0, &options, &root, &error));
      OK(ps_root_namespace_acquire(root, "tail", 0, &ns, &error));
      m.operations = m.reads = allocation_call = 0;
      m.tear = m.persist_torn = 1;
      m.fail_at = kind == 1 ? cut : 0;
      m.fail_read = kind == 2 ? cut : 0;
      allocation_fail = kind == 0 ? (cut ? cut : (size_t)-1) : 0;
      int rc = ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                                   &result, &error);
      allocation_fail = m.fail_at = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0   ? allocation_call
                : kind == 1 ? m.operations
                            : m.reads;
        CHECK(sites && result.finished_tail && result.replaced == 1);
      } else {
        CHECK(rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        lc_error_cleanup(&error);
        if (kind == 0) {
          OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL,
                                 &result, &error));
          CHECK(!result.pending);
        }
      }
      ps_namespace_release(ns);
      OK(ps_root_close(root, &error));
      crash(&m);
      OK(ps_root_open(path, 0, &options, &root, &error));
      OK(ps_root_namespace_acquire(root, "tail", 0, &ns, &error));
      pin.ns = ns;
      pin.body = NULL;
      pin.count = 0;
      OK(ps_namespace_view_open(ns, &view, &error));
      OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin,
                                 &error));
      CHECK(pin.count == 2 && pin.body);
      ps_namespace_view_close(view);
      ps_namespace_read_close(pin.body);
      OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL, &result,
                             &error));
      CHECK(!result.pending);
      OK(ps_namespace_status_get(ns, &status, &error));
      CHECK(status.active_segment == 2 && !status.tail);
      ps_namespace_release(ns);
      OK(ps_root_close(root, &error));
    }
    printf("namespace tail sync%d %s: %zu sites\n", durable,
           kind == 0   ? "allocation"
           : kind == 1 ? "write/sync"
                       : "read",
           sites);
  }
  free(baseline);
  free(m.stable);
  clean();
}
static ssize_t tail_held_write(void *context, int fd, const void *bytes,
                               size_t count, off_t offset) {
  commit_gate *g = context;
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && !g->entered && count >= 8 &&
      !memcmp(bytes, "LCPSMAPR", 8)) {
    g->entered = 1;
    pthread_cond_broadcast(&g->cond);
    while (!g->released)
      pthread_cond_wait(&g->cond, &g->mutex);
  }
  pthread_mutex_unlock(&g->mutex);
  return pwrite(fd, bytes, count, offset);
}
static void namespace_tail_concurrency(int durable) {
  commit_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate,       gate_read,    tail_held_write,
              repack_sync, gate_reserve, NULL};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  namespace_repack_pin pin = {0};
  ps_namespace_status status;
  repack_actor actor = {0};
  pthread_t thread;
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "tail", 1, &ns, &error));
  tail_seed(ns, root);
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  ps_namespace_view_close(view);
  actor.ns = ns;
  gate.enabled = 1;
  CHECK(!pthread_create(&thread, NULL, repack_worker, &actor));
  await_gate(&gate, 0);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(status.active_segment == 2 && !status.tail);
  /* The old source is sealed, but copying holds no publication authority.
   * Same-namespace mutation, sibling publication and an admitted old reader
   * must all finish before its candidate write is released. */
  checkpoint_put(root, ns, "lease/0", 4, 2, 1, 1, 0);
  put(ps_root_space(root), state(999), "peer", 4, 4);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(!pthread_join(thread, NULL) && actor.rc == LC_OK);
  CHECK(actor.result.finished_tail && actor.result.pending &&
        !actor.result.replaced);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL,
                         &actor.result, &error));
  CHECK(actor.result.replaced == 1 && !actor.result.pending);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(status.active_segment == 2);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  CHECK(!pthread_cond_destroy(&gate.cond) &&
        !pthread_mutex_destroy(&gate.mutex));
  clean();
}
#endif
