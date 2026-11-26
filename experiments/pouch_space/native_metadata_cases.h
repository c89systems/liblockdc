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
  size_t missing_metadata;
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
  *meta = b->missing_metadata == at + 1
              ? NULL
              : b->records[at].bytes + 32 + header->key_len;
  return LC_OK;
}
static void native_metadata_free(native_metadata_batch *b) {
  for (size_t i = 0; i < 4; ++i)
    ps_checkpoint_record_close(&b->records[i]);
}
static void native_metadata_add(native_metadata_batch *b,
                                const lc_pouch_state_entry *entry) {
  size_t i = b->batch.count++;
  CHECK(i < sizeof(b->records) / sizeof(b->records[0]));
  OK(ps_checkpoint_record_encode(NULL, entry, &b->records[i], &error));
  OK(lc_pouch_record_header_decode(b->records[i].bytes, &b->headers[i],
                                   &error));
  b->bytes += b->records[i].length;
  b->batch.prefix = native_metadata_prefix;
  b->batch.context = b;
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
    native_metadata_add(b, &entry);
  }
}
static void native_metadata_fixture(native_metadata_batch *b, int replacement,
                                    const char *first) {
  native_metadata_fixture_full(b, replacement, first, "empty");
}
typedef struct native_metadata_seen {
  const char *first_key, *second_key, *decision_key;
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
    CHECK(!strcmp(entry->key, seen->first_key ? seen->first_key : "alpha") ||
          !strcmp(entry->key, seen->second_key ? seen->second_key : "bravo"));
    if (!seen->keys) {
      seen->found = entry->found;
      seen->version = entry->version;
    }
    CHECK(entry->version == seen->version && entry->found == seen->found);
    ++seen->keys;
  } else {
    CHECK(!strcmp(entry->decision, "committed"));
    if (seen->decision_key)
      CHECK(!strcmp(entry->key, seen->decision_key));
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
static const char native_stage[] = "alpha/.staging/txn";
static const char native_other_stage[] = "bravo/.staging/txn";

static void native_metadata_stage(native_metadata_batch *b, int complete,
                                  const char *etag) {
  lc_pouch_state_entry entry = {0};
  memset(b, 0, sizeof(*b));
  entry.key = "alpha";
  entry.etag = (char *)etag;
  entry.content_type = "application/test";
  entry.seen = entry.has_query_index_seq = 1;
  entry.found = !complete;
  entry.version = complete ? 3 : 1;
  entry.index_seq = complete ? 5 : 1;
  entry.record_type = complete ? LC_POUCH_STATE_RECORD_STATE_DELETE
                               : LC_POUCH_STATE_RECORD_STATE_META;
  native_metadata_add(b, &entry);
  if (!complete) {
    entry.key = (char *)native_stage;
    entry.index_seq = 2;
    native_metadata_add(b, &entry);
  }
  entry.key = (char *)(complete ? native_stage : native_other_stage);
  entry.decision = "committed";
  entry.control = 1;
  entry.found = 0;
  entry.version = complete ? 2 : 1;
  entry.index_seq = complete ? 6 : 3;
  entry.record_type = LC_POUCH_STATE_RECORD_DECISION;
  native_metadata_add(b, &entry);
  if (complete) {
    entry.decision = NULL;
    entry.control = 0;
    entry.version = 3;
    entry.index_seq = 7;
    entry.record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    native_metadata_add(b, &entry);
  }
  entry.key = entry.etag = entry.decision = NULL;
  entry.control = 1;
  entry.index_seq = complete ? 8 : 4;
  entry.record_type = LC_POUCH_STATE_RECORD_HIGH_WATER;
  native_metadata_add(b, &entry);
}

static int native_metadata_stage_capture(ps_namespace *ns, int *complete,
                                         lc_error *e) {
  native_metadata_seen seen = {0};
  int rc;
  seen.second_key = native_stage;
  seen.decision_key = native_other_stage;
  rc = native_metadata_capture(ns, &seen, e);
  if (rc == LC_OK) {
    CHECK(seen.keys == (seen.found ? 2 : 1) && seen.decisions == 1 &&
          seen.decision_version == 1 && seen.version == (seen.found ? 1 : 3) &&
          seen.boundary == (seen.found ? 4 : 8) &&
          seen.index == (seen.found ? 3 : 5));
    *complete = !seen.found;
  }
  return rc;
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
typedef struct native_metadata_wide {
  ps_checkpoint_record record;
  lc_pouch_record_header header;
  char key[16];
} native_metadata_wide;
static int native_metadata_wide_prefix(void *context, size_t at,
                                       lc_pouch_record_header *header,
                                       const void **key_bytes,
                                       const void **meta, lc_error *e) {
  native_metadata_wide *fixture = context;
  (void)e;
  CHECK(snprintf(fixture->key, sizeof(fixture->key), "slot-%06zu", at) ==
        (int)fixture->header.key_len);
  *header = fixture->header;
  *key_bytes = fixture->key;
  *meta =
      fixture->record.bytes + LC_POUCH_RECORD_HEADER_BYTES + header->key_len;
  return LC_OK;
}
static void native_metadata_wide_revision(void) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_metadata *plan = NULL;
  ps_namespace_info identity;
  ps_values *values = NULL;
  native_metadata_wide fixture = {0};
  lc_pouch_storage_batch batch = {0};
  lc_pouch_state_entry entry = {0};
  char etag[129];
  clean();
  memset(etag, 'e', sizeof(etag) - 1);
  etag[sizeof(etag) - 1] = 0;
  entry.key = "slot-000000";
  entry.etag = etag;
  entry.content_type = "application/test";
  entry.seen = entry.found = 1;
  entry.index_seq = entry.version = 1;
  entry.record_type = LC_POUCH_STATE_RECORD_STATE_META;
  OK(ps_checkpoint_record_encode(NULL, &entry, &fixture.record, &error));
  OK(lc_pouch_record_header_decode(fixture.record.bytes, &fixture.header,
                                   &error));
  CHECK(fixture.record.length < PS_KEY_RECORD_INLINE_MAX);
  batch.count = 192;
  batch.prefix = native_metadata_wide_prefix;
  batch.context = &fixture;
  uint64_t bytes = batch.count * fixture.record.length;
  CHECK(bytes + batch.count * (512 + 64) > PS_JOURNAL);
  options.storage.initial_capacity = options.storage.maximum_capacity =
      4 * PS_MIB;
  options.storage.durable_sync = 1;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "wide", 1, &ns, &error));
  OK(ps_namespace_metadata_begin(ns, 0, &batch, bytes, &plan, &error));
  OK(ps_namespace_metadata_commit(plan, &batch, PS_COMMIT_DEFAULT, &error));
  ps_namespace_metadata_close(plan);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "wide", 0, &ns, &error));
  OK(ps_namespace_identity(ns, &identity, &error));
  OK(ps_values_capture(ps_root_space(root), identity.owner, PS_KEY_METADATA,
                       PS_KEY_METADATA, &values, &error));
  CHECK(ps_values_count(values) == batch.count);
  ps_values_close(values);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  ps_checkpoint_record_close(&fixture.record);
  clean();
}
static void native_metadata_stage_peer(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL, *peer = NULL;
  ps_namespace *ns = NULL, *other_ns = NULL;
  ps_namespace_view *held = NULL;
  native_metadata_batch original, changed;
  native_metadata_seen seen = {0};
  int complete;
  clean();
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.shared = 1;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "metadata", 1, &ns, &error));
  native_metadata_stage(&original, 0, "empty");
  native_metadata_stage(&changed, 1, "empty");
  OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
  OK(ps_flush(ps_root_space(root), &error));
  OK(ps_root_open(path, 0, &options, &peer, &error));
  OK(ps_root_namespace_acquire(peer, "metadata", 0, &other_ns, &error));
  OK(ps_namespace_view_open(other_ns, &held, &error));
  OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFER_SYNC, &error));
  OK(native_metadata_stage_capture(other_ns, &complete, &error));
  CHECK(complete == !durable);
  OK(ps_flush(ps_root_space(root), &error));
  OK(native_metadata_stage_capture(other_ns, &complete, &error));
  CHECK(complete);
  OK(ps_refresh(ps_root_space(root), &error));
  OK(ps_checkpoint(ps_root_space(root), &error));
  seen.second_key = native_stage;
  seen.decision_key = native_other_stage;
  OK(ps_namespace_view_visit(held, 1, native_metadata_visit, &seen, &error));
  CHECK(seen.found && seen.keys == 2 && seen.decisions == 1 &&
        seen.boundary == 4);
  ps_namespace_view_close(held);
  ps_namespace_release(other_ns);
  ps_namespace_release(ns);
  OK(ps_root_close(peer, &error));
  OK(ps_root_close(root, &error));
  native_metadata_free(&original);
  native_metadata_free(&changed);
  clean();
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
  /* A missing prefix after one valid record cannot select a partial batch. */
  OK(ps_namespace_metadata_begin(ns, 0, &changed.batch, changed.bytes, &plan,
                                 &error));
  changed.missing_metadata = 2;
  CHECK(ps_namespace_metadata_commit(plan, &changed.batch, PS_COMMIT_DEFAULT,
                                     &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  changed.missing_metadata = 0;
  ps_namespace_metadata_close(plan);
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
  /* Crash replay uses the model's durable bytes for every fault site. */
  m.snapshot_only = 1;
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  char etag[PS_KEY_RECORD_INLINE_MAX + 1];
  memset(etag, 'e', sizeof(etag) - 1);
  etag[sizeof(etag) - 1] = 0;
  native_metadata_stage(&original, 0, kind == 2 ? etag : "empty");
  native_metadata_stage(&changed, 1, kind == 2 ? etag : "empty");
  for (size_t cut = 0; cut <= sites; ++cut) {
    int selected = 0;
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
    if (rc == LC_OK && kind == 2)
      rc = native_metadata_stage_capture(ns, &selected, &error);
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
    OK(native_metadata_stage_capture(ns, &selected, &error));
    CHECK(!durable || rc != LC_OK || selected);
    /* Both generations are valid as a whole; retry must finish the new one. */
    if (!selected)
      OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFAULT, &error));
    OK(native_metadata_stage_capture(ns, &selected, &error));
    CHECK(selected);
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
static void native_metadata_shared_reserve(int durable) {
  ps_root_options options = {0};
  ps_root *owner = NULL, *peer = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_metadata *plan = NULL;
  ps_budget *pressure = NULL;
  ps_budget_plan claim = {0};
  ps_space *other;
  ps_stats base, live, observed;
  native_metadata_batch original, changed;
  native_metadata_seen seen = {0};
  char *large = malloc(60 * 1024 + 1);
  CHECK(large);
  memset(large, 'k', 60 * 1024);
  large[60 * 1024] = 0;
  clean();
  options.storage.maximum_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.shared = 1;
  OK(ps_root_open(path, 1, &options, &owner, &error));
  OK(ps_root_open(path, 0, &options, &peer, &error));
  other = ps_root_space(peer);
  OK(ps_status(other, &base, &error));
  OK(ps_root_namespace_acquire(owner, "cold", 1, &ns, &error));
  native_metadata_fixture(&original, 0, large);
  native_metadata_fixture(&changed, 1, large);
  OK(ps_namespace_metadata_begin(ns, 0, &original.batch, original.bytes, &plan,
                                 &error));
  /* Another allocator can use ordinary capacity after private admission.
   * Select the new standing requirement only if it still fits at commit. */
  OK(ps_budget_open(other, &claim, &pressure, &error));
  for (uint64_t step = PS_DEFAULT_CAPACITY / (2 * PS_PAGE); step; step /= 2) {
    claim.payload_bytes += step * PS_PAGE;
    int rc = ps_budget_resize(pressure, &claim, &error);
    if (rc != LC_OK) {
      CHECK(rc == LC_ERR_INVALID);
      lc_error_cleanup(&error);
      claim.payload_bytes -= step * PS_PAGE;
    }
  }
  CHECK(claim.payload_bytes > PS_DEFAULT_CAPACITY / 2);
  CHECK(ps_namespace_metadata_commit(plan, &original.batch, PS_COMMIT_DEFAULT,
                                     &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  ps_namespace_metadata_close(plan);
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_owners == base.reserve_owners &&
        observed.reserve_payload_bytes == base.reserve_payload_bytes);
  OK(ps_budget_close(pressure, &error));
  OK(native_metadata_write(ns, &original, PS_COMMIT_DEFAULT, &error));
  OK(ps_flush(ps_root_space(owner), &error));
  OK(ps_status(ps_root_space(owner), &live, &error));
  CHECK(live.reserve_owners == 2 &&
        live.reserve_payload_bytes > base.reserve_payload_bytes + 120 * 1024);
  /* This root opened before the namespace existed. Following the physical
   * journal and then a physical root checkpoint must establish the same
   * reserve without loading the canonical keys. */
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes &&
        observed.reserve_metadata_bytes == live.reserve_metadata_bytes &&
        observed.reserve_entries == live.reserve_entries &&
        observed.reserve_owners == live.reserve_owners);
  OK(ps_checkpoint(ps_root_space(owner), &error));
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes &&
        observed.reserve_owners == live.reserve_owners);
  ps_namespace_release(ns);
  OK(ps_root_close(owner, &error));
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes &&
        observed.reserve_owners == live.reserve_owners);
  OK(ps_root_open(path, 0, &options, &owner, &error));
  OK(ps_root_namespace_acquire(owner, "cold", 0, &ns, &error));
  OK(native_metadata_write(ns, &changed, PS_COMMIT_DEFER_SYNC, &error));
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes);
  /* A new private plan cannot borrow a shrinking descriptor's reserve.
   * Durability may remove that descriptor while the new plan is open. */
  OK(ps_namespace_metadata_begin(ns, 0, &original.batch, original.bytes, &plan,
                                 &error));
  OK(ps_flush(ps_root_space(owner), &error));
  OK(ps_status(ps_root_space(owner), &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes);
  ps_namespace_metadata_close(plan);
  ps_namespace_release(ns);
  OK(ps_root_close(owner, &error));
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.capacity == PS_DEFAULT_CAPACITY &&
        observed.reserve_payload_bytes == base.reserve_payload_bytes &&
        observed.reserve_entries == base.reserve_entries &&
        observed.reserve_owners == base.reserve_owners);
  /* A different publisher can exit without closing. The already-open peer
   * still obtains the selected reserve from the child process's revision. */
  pid_t child = fork();
  int child_status;
  CHECK(child >= 0);
  if (!child) {
    ps_root *writer = NULL;
    ps_namespace *created = NULL;
    OK(ps_root_open(path, 0, &options, &writer, &error));
    OK(ps_root_namespace_acquire(writer, "abandoned", 1, &created, &error));
    OK(native_metadata_write(created, &original, PS_COMMIT_DEFAULT, &error));
    OK(ps_flush(ps_root_space(writer), &error));
    _exit(0);
  }
  CHECK(waitpid(child, &child_status, 0) == child && WIFEXITED(child_status) &&
        WEXITSTATUS(child_status) == 0);
  OK(ps_refresh(other, &error));
  OK(ps_status(other, &observed, &error));
  CHECK(observed.reserve_payload_bytes == live.reserve_payload_bytes &&
        observed.reserve_owners == live.reserve_owners);
  OK(ps_root_close(peer, &error));
  OK(ps_root_open(path, 0, &options, &peer, &error));
  OK(ps_root_namespace_acquire(peer, "cold", 0, &ns, &error));
  seen.first_key = large;
  OK(native_metadata_capture(ns, &seen, &error));
  CHECK(seen.keys == 2 && !seen.found && seen.version == 2 &&
        seen.decisions == 1 && seen.decision_version == 2 &&
        seen.boundary == 8 && seen.index == 7);
  ps_namespace_release(ns);
  OK(ps_root_close(peer, &error));
  native_metadata_free(&original);
  native_metadata_free(&changed);
  free(large);
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
