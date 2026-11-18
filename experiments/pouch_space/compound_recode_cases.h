#ifndef POUCH_SPACE_COMPOUND_RECODE_CASES_H
#define POUCH_SPACE_COMPOUND_RECODE_CASES_H
#include "body_select_cases.h"

static int compound_memory_read(void *context, uint64_t at,
                                const struct iovec *parts, int count,
                                lc_error *e) {
  const ps_checkpoint_record *record = context;
  for (int i = 0; i < count; ++i) {
    if (at > record->length || parts[i].iov_len > record->length - at)
      return lc_error_set(e, LC_ERR_INVALID, 0,
                          "fixture metadata read exceeds span", NULL, NULL,
                          "test");
    memcpy(parts[i].iov_base, record->bytes + at, parts[i].iov_len);
    at += parts[i].iov_len;
  }
  return LC_OK;
}
static void compound_clone(const lc_pouch_state_entry *entry,
                           lc_pouch_state_entry *out, lc_error *e) {
  static const lc_pouch_record_io_ops ops = {compound_memory_read, NULL, NULL,
                                             NULL, NULL};
  ps_checkpoint_record encoded = {0};
  ok(ps_checkpoint_record_encode(&allocator, entry, &encoded, e), e);
  lc_pouch_record_io io = {&ops, &encoded};
  uint64_t next = 0;
  int found = 0, truncated = 0;
  ok(lc_pouch_state_record_read(
         io, &allocator, "snapshot-00000000000000000001.log", 0, encoded.length,
         0, 0, NULL, out, &next, &found, &truncated, e),
     e);
  REQUIRE(found && !truncated && next == encoded.length);
  ps_checkpoint_record_close(&encoded);
}
typedef struct compound_capture {
  const lc_pouch_state_entry *expected;
  lc_pouch_state_entry *selected;
  size_t keys;
  int changed;
} compound_capture;
static int compound_visit(void *context, uint64_t segment,
                          const lc_pouch_state_entry *entry, lc_error *e) {
  compound_capture *capture = context;
  REQUIRE(!segment);
  if (entry->control) {
    REQUIRE(entry->index_seq == 23 && entry->query_index_seq == 7);
    return LC_OK;
  }
  if (strcmp(entry->key, capture->expected->key))
    return LC_OK;
  lc_pouch_state_entry same = *entry;
  same.payload_span = capture->expected->payload_span;
  same.cipher_bytes = capture->expected->cipher_bytes;
  same.descriptor = capture->expected->descriptor;
  same.record_type = capture->expected->record_type;
  same.object_link = capture->expected->object_link;
  ps_checkpoint_record a = {0}, b = {0};
  ok(ps_checkpoint_record_encode(&allocator, &same, &a, e), e);
  ok(ps_checkpoint_record_encode(&allocator, capture->expected, &b, e), e);
  REQUIRE(a.length == b.length && !memcmp(a.bytes, b.bytes, a.length));
  ps_checkpoint_record_close(&a);
  ps_checkpoint_record_close(&b);
  capture->changed =
      strcmp(entry->payload_span.container_leaf,
             capture->expected->payload_span.container_leaf) != 0;
  compound_clone(entry, capture->selected, e);
  ++capture->keys;
  return LC_OK;
}
static int compound_selected(ps_namespace *ns,
                             const lc_pouch_state_entry *expected,
                             lc_pouch_state_entry *selected, lc_error *e) {
  ps_namespace_view *view = NULL;
  compound_capture capture = {expected, selected, 0, 0};
  ok(ps_namespace_view_open(ns, &view, e), e);
  ok(ps_namespace_view_visit(view, 1, compound_visit, &capture, e), e);
  ps_namespace_view_close(view);
  REQUIRE(capture.keys == 1);
  return capture.changed;
}
static void compound_physical(ps_namespace *ns,
                              const lc_pouch_state_entry *selected,
                              lc_pouch_state_entry *out, lc_error *e) {
  unsigned int kind;
  uint64_t id, next = 0;
  int found = 0, truncated = 0;
  const lc_pouch_state_payload_span *span = &selected->payload_span;
  REQUIRE(ps_namespace_parse_source_leaf(span->container_leaf, &kind, &id));
  REQUIRE(kind == PS_CATALOG_BODY);
  uint64_t end = span->payload_offset + span->payload_length;
  ps_namespace_window *window = NULL;
  ok(ps_namespace_read_open(ns, kind, id, span->record_offset,
                            end - span->record_offset, &window, e),
     e);
  unsigned char *scratch = NULL;
  ok(lc_pouch_state_record_read(ps_namespace_read_input(window), &allocator,
                                span->container_leaf, span->record_offset, end,
                                0, 1, &scratch, out, &next, &found, &truncated,
                                e),
     e);
  REQUIRE(found && !truncated && next == end);
  lc_free_with_allocator(&allocator, scratch);
  ps_namespace_read_close(window);
}
static void compound_claims(ps_space *space, const ps_stats *expected,
                            lc_error *e) {
  ps_stats actual;
  ok(ps_status(space, &actual, e), e);
  REQUIRE(actual.budgets == expected->budgets &&
          actual.reserved_payload_bytes == expected->reserved_payload_bytes &&
          actual.reserved_metadata_bytes == expected->reserved_metadata_bytes &&
          actual.reserved_entries == expected->reserved_entries);
}
static size_t compound_trial(const char *path, const char *key, int codec,
                             int sync, int alias, int kind, size_t cut) {
  lc_error e;
  lc_error_init(&e);
  ps_root_options options = {0};
  options.crypto_allocator = &allocator;
  options.crypto.key_string = codec & 1 ? key : NULL;
  options.crypto.compression_enabled = !!(codec & 2);
  options.storage.durable_sync = sync;
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  if (kind >= 2) {
    m.live = calloc(1, PS_MIB);
    m.stable = calloc(1, PS_MIB);
    REQUIRE(m.live && m.stable);
    m.torn = kind - 2;
    options.storage.io = &io;
    options.storage.initial_capacity = options.storage.maximum_capacity =
        PS_MIB;
  }
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "compound", 1, &e);
  lc_pouch_state_entry entry = {0}, expected = {0}, result = {0},
                       selected = {0};
  select_seed(root, ns, alias, &entry, &e);
  /* Faults target conversion with an already durable source. NoSync seed
   * metadata may otherwise disappear at an earlier read failure. */
  ok(ps_flush(ps_root_space(root), &e), &e);
  compound_clone(&entry, &expected, &e);
  const char *binding = alias ? "conversion-alias" : "namespace-test";
  lc_pouch_crypto_open_options target_options = options.crypto;
  target_options.compression_enabled = !options.crypto.compression_enabled;
  lc_pouch_crypto *target = NULL;
  ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e), &e);
  ps_space *space = ps_root_space(root);
  ps_stats initial;
  ok(ps_status(space, &initial, &e), &e);
  ps_namespace_recode *work = NULL;
  if (!kind) {
    ps_budget *pressure = capacity_peer(space, 1, &e);
    ps_stats claimed;
    ok(ps_status(space, &claimed, &e), &e);
    REQUIRE(ps_namespace_recode_begin(ns, target, &entry, binding, &work, &e) ==
                LC_ERR_INVALID &&
            !work);
    clear(&e);
    compound_claims(space, &claimed, &e);
    ok(ps_budget_close(pressure, &e), &e);
    ok(ps_namespace_recode_begin(ns, target, &entry, binding, &work, &e), &e);
    ps_namespace_recode_close(work);
    work = NULL;
    compound_claims(space, &initial, &e);
  }
  result.key = lc_strdup_with_allocator(&allocator, "sentinel");
  result.version = 999;
  REQUIRE(result.key);
  ps_namespace_status before_status, after_status;
  ok(ps_namespace_status_get(ns, &before_status, &e), &e);
  allocation = m.calls = 0;
  if (kind == 1)
    fail_allocation = cut ? cut : SIZE_MAX;
  if (kind >= 2)
    m.fail = cut;
  int rc = ps_namespace_recode_begin(ns, target, &entry, binding, &work,
                                     kind == 1 && cut % 2 ? NULL : &e);
  ps_budget *peer = NULL;
  if (rc == LC_OK && !kind) {
    /* Begin owns its source snapshot; caller-owned fields may now disappear. */
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    peer = capacity_peer(space, 1, &e);
    REQUIRE(ps_namespace_recode_run(work, NULL, &e) == LC_ERR_INVALID);
    clear(&e);
  }
  if (rc == LC_OK)
    rc = ps_namespace_recode_run(work, &result,
                                 kind == 1 && cut % 2 ? NULL : &e);
  if (!kind && rc == LC_OK) {
    REQUIRE(ps_namespace_recode_run(work, &result, &e) == LC_ERR_INVALID);
    clear(&e);
  }
  ps_namespace_recode_close(work);
  work = NULL;
  size_t calls = kind == 1 ? allocation : m.calls;
  fail_allocation = m.fail = 0;
  if (peer)
    ok(ps_budget_close(peer, &e), &e);
  if (!cut)
    ok(rc, &e);
  else {
    REQUIRE(calls >= cut);
    REQUIRE(rc == LC_OK || rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
  }
  if (rc != LC_OK)
    REQUIRE(result.version == 999 && !strcmp(result.key, "sentinel"));
  clear(&e);
  if (kind >= 2) {
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    crash(&m, path);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "compound", 0, &e);
  }
  int changed = compound_selected(ns, &expected, &selected, &e);
  ok(ps_namespace_status_get(ns, &after_status, &e), &e);
  REQUIRE(after_status.checkpoint_epoch >= before_status.checkpoint_epoch);
  REQUIRE(changed ==
          (after_status.checkpoint_epoch > before_status.checkpoint_epoch));
  REQUIRE(after_status.covered_segment == before_status.covered_segment &&
          after_status.covered_offset == before_status.covered_offset);
  if (rc == LC_OK)
    REQUIRE(changed);
  if (!changed) {
    ok(ps_namespace_recode_begin(ns, target, &selected, binding, &work, &e),
       &e);
    ok(ps_namespace_recode_run(work, &result, &e), &e);
    ps_namespace_recode_close(work);
    REQUIRE(compound_selected(ns, &expected, &selected, &e));
  } else if (rc != LC_OK)
    compound_physical(ns, &selected, &result, &e);
  recode_check(root, ns, &result, binding, &e);
  compound_claims(ps_root_space(root), &initial, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "compound", 0, &e);
  REQUIRE(compound_selected(ns, &expected, &selected, &e));
  recode_check(root, ns, &result, binding, &e);
  REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(root)) ==
          options.crypto.compression_enabled);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_pouch_crypto_close(target);
  lc_pouch_state_entry_cleanup(&allocator, &entry);
  lc_pouch_state_entry_cleanup(&allocator, &expected);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  lc_pouch_state_entry_cleanup(&allocator, &selected);
  REQUIRE(!unlink(path));
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
  return calls;
}
typedef struct compound_job {
  ps_namespace_recode *work;
  lc_pouch_state_entry result;
  int rc;
} compound_job;
static void *compound_thread(void *context) {
  compound_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc = ps_namespace_recode_run(job->work, &job->result, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void compound_concurrency(const char *path, const char *key) {
  for (int sync = 0; sync < 2; ++sync) {
    select_pause pause = {0};
    pause.m.live = calloc(1, PS_MIB);
    pause.m.stable = calloc(1, PS_MIB);
    REQUIRE(pause.m.live && pause.m.stable);
    REQUIRE(!pthread_mutex_init(&pause.gate.mutex, NULL));
    REQUIRE(!pthread_cond_init(&pause.gate.changed, NULL));
    ps_io io = {&pause,
                select_paused_read,
                select_paused_write,
                select_paused_sync,
                select_paused_reserve,
                NULL};
    ps_root_options options = {0};
    options.storage.initial_capacity = options.storage.maximum_capacity =
        PS_MIB;
    options.storage.durable_sync = sync;
    options.storage.io = &io;
    options.crypto_allocator = &allocator;
    options.crypto.key_string = key;
    lc_error e;
    lc_error_init(&e);
    ps_root *root = NULL;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_namespace *ns = acquire(root, "compound", 1, &e);
    ps_namespace *peer = acquire(root, "peer", 1, &e);
    lc_pouch_state_entry entry = {0};
    select_seed(root, ns, 0, &entry, &e);
    lc_pouch_crypto_open_options target_options = options.crypto;
    target_options.compression_enabled = 1;
    lc_pouch_crypto *target = NULL;
    ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
       &e);
    compound_job job = {0};
    ok(ps_namespace_recode_begin(ns, target, &entry, "namespace-test",
                                 &job.work, &e),
       &e);
    pause.armed = 1;
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, compound_thread, &job));
    pthread_mutex_lock(&pause.gate.mutex);
    while (!pause.gate.entered)
      pthread_cond_wait(&pause.gate.changed, &pause.gate.mutex);
    pthread_mutex_unlock(&pause.gate.mutex);
    (void)select_check(ns, NULL, &entry, NULL, &e);
    ok(append(root, peer, 1, 0, 31, &e), &e);
    count(peer, 1, &e);
    ps_namespace_append *contended = NULL;
    REQUIRE(ps_namespace_append_begin(ns, 0, 1, PS_APPEND_PREFIX, NULL,
                                      &contended, &e) == LC_ERR_INVALID &&
            !contended);
    clear(&e);
    pthread_mutex_lock(&pause.gate.mutex);
    pause.gate.released = 1;
    pthread_cond_broadcast(&pause.gate.changed);
    pthread_mutex_unlock(&pause.gate.mutex);
    REQUIRE(!pthread_join(thread, NULL));
    ok(job.rc, &e);
    ps_namespace_recode_close(job.work);
    lc_pouch_state_entry selected = select_projection(&entry, &job.result);
    (void)select_check(ns, NULL, &selected, NULL, &e);
    recode_check(root, ns, &job.result, "namespace-test", &e);
    ps_namespace_release(peer);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    lc_pouch_crypto_close(target);
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    lc_pouch_state_entry_cleanup(&allocator, &job.result);
    REQUIRE(!unlink(path));
    REQUIRE(!pthread_mutex_destroy(&pause.gate.mutex));
    REQUIRE(!pthread_cond_destroy(&pause.gate.changed));
    free(pause.m.live);
    free(pause.m.stable);
    lc_error_cleanup(&e);
  }
}
static void compound_recode(const char *path, const char *key, int faults) {
  size_t sites = 0;
  if (!faults) {
    for (int codec = 0; codec < 4; ++codec)
      for (int sync = 0; sync < 2; ++sync)
        for (int alias = 0; alias < 2; ++alias)
          (void)compound_trial(path, key, codec, sync, alias, 0, 0);
  } else {
    for (int codec = 0; codec <= 3; codec += 3)
      for (int sync = 0; sync < 2; ++sync)
        for (int kind = 1; kind <= 5; ++kind) {
          size_t calls = compound_trial(path, key, codec, sync, 1, kind, 0);
          REQUIRE(calls);
          for (size_t cut = 1; cut <= calls; ++cut) {
            (void)compound_trial(path, key, codec, sync, 1, kind, cut);
            ++sites;
          }
        }
    printf("compound conversion verified allocation/I/O/torn sites=%zu\n",
           sites);
  }
}
#endif
