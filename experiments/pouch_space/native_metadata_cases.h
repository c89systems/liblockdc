#ifndef POUCH_SPACE_NATIVE_METADATA_CASES_H
#define POUCH_SPACE_NATIVE_METADATA_CASES_H
#include "namespace_metadata.h"

/* Metadata fixtures contain no producer or application payload buffer. */
typedef struct native_metadata_batch {
  ps_checkpoint_record records[4];
  lc_pouch_record_header headers[4];
  lc_pouch_storage_batch batch;
  uint64_t bytes;
  allocation_pause *pause;
} native_metadata_batch;
static int native_metadata_prefix(void *context, size_t at,
                                  lc_pouch_record_header *header,
                                  const void **key_bytes, const void **meta,
                                  lc_error *e) {
  native_metadata_batch *b = context;
  (void)e;
  if (b->pause && !at) {
    pthread_mutex_lock(&b->pause->mutex);
    b->pause->entered = 1;
    pthread_cond_broadcast(&b->pause->cond);
    while (!b->pause->released)
      pthread_cond_wait(&b->pause->cond, &b->pause->mutex);
    pthread_mutex_unlock(&b->pause->mutex);
  }
  *header = b->headers[at];
  *key_bytes = header->key_len ? b->records[at].bytes + 32 : NULL;
  *meta = b->records[at].bytes + 32 + header->key_len;
  return LC_OK;
}
static void native_metadata_free(native_metadata_batch *b) {
  for (size_t i = 0; i < 4; ++i)
    ps_checkpoint_record_close(&b->records[i]);
}
static void native_metadata_fixture_full(native_metadata_batch *b,
                                         int replacement, const char *first,
                                         const char *etag) {
  memset(b, 0, sizeof(*b));
  for (size_t i = 0; i < 4; ++i) {
    lc_pouch_state_entry entry = {0};
    entry.seen = entry.found = 1;
    entry.index_seq = replacement * 4 + i + 1;
    entry.has_query_index_seq = 1;
    entry.version = replacement + 1;
    if (i < 2) {
      entry.key = (char *)(i ? "bravo" : first);
      entry.content_type = "application/test";
      entry.etag = (char *)etag;
      entry.record_type = replacement ? LC_POUCH_STATE_RECORD_STATE_DELETE
                                      : LC_POUCH_STATE_RECORD_STATE_META;
      entry.found = !replacement;
    } else {
      entry.control = 1;
      entry.record_type = i == 2 ? LC_POUCH_STATE_RECORD_DECISION
                                 : LC_POUCH_STATE_RECORD_HIGH_WATER;
      if (i == 2) {
        entry.key = "alpha/.staging/txn";
        entry.etag = "empty";
        entry.decision = "committed";
      }
    }
    OK(ps_checkpoint_record_encode(NULL, &entry, &b->records[i], &error));
    OK(lc_pouch_record_header_decode(b->records[i].bytes, &b->headers[i],
                                     &error));
    b->bytes += b->records[i].length;
  }
  b->batch.count = 4;
  b->batch.prefix = native_metadata_prefix;
  b->batch.context = b;
}
static void native_metadata_fixture(native_metadata_batch *b, int replacement,
                                    const char *first) {
  native_metadata_fixture_full(b, replacement, first, "empty");
}
typedef struct native_metadata_seen {
  size_t keys, decisions;
  uint64_t index, version, decision_version, boundary;
  int found;
} native_metadata_seen;
static int native_metadata_visit(void *context, uint64_t segment,
                                 const lc_pouch_state_entry *entry,
                                 lc_error *e) {
  native_metadata_seen *seen = context;
  (void)e;
  CHECK(!segment && !entry->payload_span.present && !entry->lifetime &&
        !entry->query_index_seq);
  if (entry->control && !entry->decision) {
    seen->boundary = entry->index_seq;
    return LC_OK;
  }
  if (!entry->control) {
    CHECK(!strcmp(entry->key, "alpha") || !strcmp(entry->key, "bravo"));
    if (!seen->keys) {
      seen->found = entry->found;
      seen->version = entry->version;
    }
    CHECK(entry->version == seen->version && entry->found == seen->found);
    ++seen->keys;
  } else {
    CHECK(!strcmp(entry->decision, "committed"));
    seen->decision_version = entry->version;
    ++seen->decisions;
  }
  if (entry->index_seq > seen->index)
    seen->index = entry->index_seq;
  return LC_OK;
}
static int native_metadata_capture(ps_namespace *ns, native_metadata_seen *seen,
                                   lc_error *e) {
  ps_namespace_view *view = NULL;
  int rc = ps_namespace_view_open(ns, &view, e);
  if (rc == LC_OK)
    rc = ps_namespace_view_visit(view, 1, native_metadata_visit, seen, e);
  ps_namespace_view_close(view);
  return rc;
}
static int native_metadata_observe(ps_namespace *ns) {
  native_metadata_seen seen = {0};
  OK(native_metadata_capture(ns, &seen, &error));
  CHECK(seen.keys == 2 && seen.decisions == 1 &&
        seen.decision_version == seen.version &&
        seen.boundary == (seen.found ? 4 : 8) &&
        seen.index == (seen.found ? 3 : 7));
  return !seen.found;
}
static int native_metadata_write(ps_namespace *ns, native_metadata_batch *b,
                                 enum ps_commit_mode mode, lc_error *e) {
  ps_namespace_metadata *plan = NULL;
  int rc = ps_namespace_metadata_begin(ns, 0, &b->batch, b->bytes, &plan, e);
  if (rc == LC_OK)
    rc = ps_namespace_metadata_commit(plan, &b->batch, mode, e);
  ps_namespace_metadata_close(plan);
  return rc;
}
static void native_metadata_lifecycle(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_metadata *plan = NULL;
  ps_namespace_view *held = NULL;
  ps_namespace_status status;
  ps_stats before, after;
  native_metadata_batch original, changed, wrong;
  native_metadata_seen seen = {0};
  clean();
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "metadata", 1, &ns, &error));
  native_metadata_fixture(&original, 0, "alpha");
  native_metadata_fixture(&changed, 1, "alpha");
  native_metadata_fixture(&wrong, 1, "other");
  /* Admission rejects producer bytes without selecting any metadata. */
  original.headers[0].payload_len = 1;
  BAD(ps_namespace_metadata_begin(ns, 0, &original.batch, original.bytes, &plan,
                                  &error));
  CHECK(!plan);
  original.headers[0].payload_len = 0;
  OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
  CHECK(!native_metadata_observe(ns));
  OK(ps_flush(ps_root_space(root), &error));
  OK(ps_namespace_view_open(ns, &held, &error));
  /* A finalized batch cannot add a key that begin did not admit. */
  OK(ps_namespace_metadata_begin(ns, 0, &changed.batch, changed.bytes, &plan,
                                 &error));
  BAD(ps_namespace_metadata_commit(plan, &wrong.batch, PS_COMMIT_DEFAULT,
                                   &error));
  ps_namespace_metadata_close(plan);
  plan = NULL;
  CHECK(!native_metadata_observe(ns));
  OK(ps_status(ps_root_space(root), &before, &error));
  OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFER_SYNC, &error));
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.syncs == before.syncs);
  CHECK(native_metadata_observe(ns) == !durable);
  OK(ps_flush(ps_root_space(root), &error));
  CHECK(native_metadata_observe(ns));
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_namespace_view_visit(held, 1, native_metadata_visit, &seen, &error));
  CHECK(seen.keys == 2 && seen.decisions == 1 && seen.found &&
        seen.version == 1 && seen.index == 3);
  ps_namespace_view_close(held);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(!status.active_segment && !status.tail && !status.covered_segment &&
        !status.covered_offset && !status.pending_keys &&
        status.checkpoint_keys == 3);
  OK(ps_check(ps_root_space(root), &error));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "metadata", 0, &ns, &error));
  CHECK(native_metadata_observe(ns));
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(!status.active_segment && !status.pending_keys &&
        status.checkpoint_keys == 3);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  native_metadata_free(&original);
  native_metadata_free(&changed);
  native_metadata_free(&wrong);
  clean();
}
static void native_metadata_faults(int durable, int kind) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  native_metadata_batch original, changed;
  size_t sites = 0;
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  native_metadata_fixture(&original, 0, "alpha");
  native_metadata_fixture(&changed, 1, "alpha");
  for (size_t cut = 0; cut <= sites; ++cut) {
    clean();
    OK(ps_root_open(path, 1, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "metadata", 1, &ns, &error));
    OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
    OK(ps_flush(ps_root_space(root), &error));
    m.operations = m.reads = allocation_call = 0;
    m.tear = m.persist_torn = 1;
    m.fail_at = kind == 0 ? cut : 0;
    allocation_fail = kind == 1 ? (cut ? cut : (size_t)-1) : 0;
    m.fail_read = kind == 2 ? cut : 0;
    int rc = native_metadata_write(ns, &changed, PS_COMMIT_DEFAULT, &error);
    if (rc == LC_OK && kind == 2) {
      native_metadata_seen seen = {0};
      rc = native_metadata_capture(ns, &seen, &error);
    }
    if (!cut)
      sites = kind == 0 ? m.operations : kind == 1 ? allocation_call : m.reads;
    m.fail_at = m.fail_read = allocation_fail = 0;
    CHECK(cut || rc == LC_OK);
    CHECK(rc == LC_OK || rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
    lc_error_cleanup(&error);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    root = NULL;
    crash(&m);
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "metadata", 0, &ns, &error));
    int selected = native_metadata_observe(ns);
    CHECK(!durable || rc != LC_OK || selected);
    /* Both generations are valid as a whole; retry must finish the new one. */
    if (!selected)
      OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFAULT, &error));
    CHECK(native_metadata_observe(ns));
    OK(ps_check(ps_root_space(root), &error));
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    root = NULL;
  }
  printf("native metadata cuts: durable=%d kind=%d sites=%zu\n", durable, kind,
         sites);
  CHECK(sites);
  native_metadata_free(&original);
  native_metadata_free(&changed);
  free(m.stable);
  clean();
}
static void native_metadata_retirement(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_metadata *plan = NULL;
  native_metadata_batch original, changed;
  ps_stats before, pending, completed;
  char *etag = malloc(16385);
  CHECK(etag);
  memset(etag, 'e', 16384);
  etag[16384] = 0;
  clean();
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "metadata", 1, &ns, &error));
  native_metadata_fixture_full(&original, 0, "alpha", etag);
  native_metadata_fixture(&changed, 1, "alpha");
  OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
  OK(ps_flush(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFER_SYNC, &error));
  OK(ps_status(ps_root_space(root), &pending, &error));
  CHECK(pending.reserve_payload_bytes == before.reserve_payload_bytes);
  OK(ps_flush(ps_root_space(root), &error));
  /* The next admission observes the completed prefix and retires the debt. */
  OK(ps_namespace_metadata_begin(ns, 0, &changed.batch, changed.bytes, &plan,
                                 &error));
  ps_namespace_metadata_close(plan);
  OK(ps_status(ps_root_space(root), &completed, &error));
  CHECK(completed.reserve_payload_bytes < before.reserve_payload_bytes);
  CHECK(native_metadata_observe(ns));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  native_metadata_free(&original);
  native_metadata_free(&changed);
  free(etag);
  clean();
}
typedef struct native_metadata_thread {
  ps_namespace_metadata *plan;
  native_metadata_batch *batch;
  int rc;
} native_metadata_thread;
static void *native_metadata_commit_thread(void *context) {
  native_metadata_thread *t = context;
  lc_error e;
  lc_error_init(&e);
  t->rc = ps_namespace_metadata_commit(t->plan, &t->batch->batch,
                                       PS_COMMIT_DEFAULT, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void native_metadata_concurrency(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL, *peer = NULL;
  native_metadata_batch original, changed;
  native_metadata_thread t = {0};
  allocation_pause pause = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
                            0, 0};
  pthread_t thread;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "metadata", 1, &ns, &error));
  OK(ps_root_namespace_acquire(root, "peer", 1, &peer, &error));
  native_metadata_fixture(&original, 0, "alpha");
  native_metadata_fixture(&changed, 1, "alpha");
  OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
  t.batch = &changed;
  OK(ps_namespace_metadata_begin(ns, 0, &changed.batch, changed.bytes, &t.plan,
                                 &error));
  changed.pause = &pause;
  CHECK(!pthread_create(&thread, NULL, native_metadata_commit_thread, &t));
  pthread_mutex_lock(&pause.mutex);
  while (!pause.entered)
    pthread_cond_wait(&pause.cond, &pause.mutex);
  pthread_mutex_unlock(&pause.mutex);
  CHECK(!native_metadata_observe(ns));
  checkpoint_put(root, peer, "lease/0", 1, 1, 1, 1, 0);
  pthread_mutex_lock(&pause.mutex);
  pause.released = 1;
  pthread_cond_broadcast(&pause.cond);
  pthread_mutex_unlock(&pause.mutex);
  CHECK(!pthread_join(thread, NULL));
  OK(t.rc);
  ps_namespace_metadata_close(t.plan);
  CHECK(native_metadata_observe(ns));
  ps_namespace_release(peer);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  native_metadata_free(&original);
  native_metadata_free(&changed);
  pthread_cond_destroy(&pause.cond);
  pthread_mutex_destroy(&pause.mutex);
  clean();
}
#endif
