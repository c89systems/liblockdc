#ifndef POUCH_SPACE_BODY_SELECT_CASES_H
#define POUCH_SPACE_BODY_SELECT_CASES_H
#include "namespace_metadata.h"
#include "recode_cases.h"

static void select_metadata(ps_namespace *ns, const lc_pouch_state_entry *entry,
                            int metadata_only, lc_error *e) {
  ps_checkpoint_record encoded = {0};
  ok(ps_checkpoint_record_encode(&allocator, entry, &encoded, e), e);
  if (metadata_only) {
    lc_pouch_record_header header;
    ok(lc_pouch_record_header_decode(encoded.bytes, &header, e), e);
    header.type = LC_POUCH_STATE_RECORD_STATE_META;
    lc_pouch_record_header_encode(&header, encoded.bytes);
  }
  batch_view input = {0};
  input.frames[0] = encoded.bytes;
  input.bytes[0] = encoded.length;
  input.count = 1;
  lc_pouch_storage_batch batch = {1, batch_prefix, &input};
  ps_namespace_metadata *plan = NULL;
  ok(ps_namespace_metadata_begin(ns, 0, &batch, encoded.length, &plan, e), e);
  ok(ps_namespace_metadata_commit(plan, &batch, PS_COMMIT_DEFAULT, e), e);
  ps_namespace_metadata_close(plan);
  ps_checkpoint_record_close(&encoded);
  ok(ps_namespace_checkpoint_flush(ns, e), e);
}
static void select_seed(ps_root *root, ps_namespace *ns, int alias,
                        lc_pouch_state_entry *entry, lc_error *e) {
  ok(put(root, ns, 7, 0, 4097, NULL, NULL, entry, e), e);
  recode_projection(root, entry, alias, e);
  if (alias)
    entry->query_index_seq = 0;
  select_metadata(ns, entry, 0, e);
}
typedef struct select_view {
  const lc_pouch_state_entry *expected, *alternative;
  size_t keys, controls;
  int selected;
} select_view;
static int select_view_record(void *context, uint64_t segment,
                              const lc_pouch_state_entry *record, lc_error *e) {
  select_view *v = context;
  REQUIRE(!segment);
  if (record->control) {
    REQUIRE(record->record_type == LC_POUCH_STATE_RECORD_HIGH_WATER &&
            record->index_seq == v->expected->index_seq &&
            record->query_index_seq == 7);
    ++v->controls;
    return LC_OK;
  }
  if (strcmp(record->key, v->expected->key))
    return LC_OK;
  ++v->keys;
  ps_checkpoint_record actual = {0}, expected = {0};
  ok(ps_checkpoint_record_encode(&allocator, record, &actual, e), e);
  ok(ps_checkpoint_record_encode(&allocator, v->expected, &expected, e), e);
  int match = actual.length == expected.length &&
              !memcmp(actual.bytes, expected.bytes, actual.length);
  ps_checkpoint_record_close(&expected);
  if (!match && v->alternative) {
    ok(ps_checkpoint_record_encode(&allocator, v->alternative, &expected, e),
       e);
    match = actual.length == expected.length &&
            !memcmp(actual.bytes, expected.bytes, actual.length);
    v->selected = match;
    ps_checkpoint_record_close(&expected);
  }
  REQUIRE(match);
  ps_checkpoint_record_close(&actual);
  return LC_OK;
}
static int select_check(ps_namespace *ns, ps_namespace_view *held,
                        const lc_pouch_state_entry *expected,
                        const lc_pouch_state_entry *alternative, lc_error *e) {
  ps_namespace_view *view = held;
  select_view v = {expected, alternative, 0, 0, 0};
  if (!held)
    ok(ps_namespace_view_open(ns, &view, e), e);
  ok(ps_namespace_view_visit(view, 1, select_view_record, &v, e), e);
  REQUIRE(v.keys == 1 && v.controls == 1);
  if (!held)
    ps_namespace_view_close(view);
  return v.selected;
}
static lc_pouch_state_entry
select_projection(const lc_pouch_state_entry *logical,
                  const lc_pouch_state_entry *body_entry) {
  lc_pouch_state_entry entry = *logical;
  entry.payload_span = body_entry->payload_span;
  entry.cipher_bytes = body_entry->cipher_bytes;
  entry.descriptor = body_entry->descriptor;
  return entry;
}
static int select_observe(void *context, const char *name,
                          const ps_checkpoint_ref *refs, size_t count,
                          void **out, lc_error *e) {
  (void)context;
  (void)name;
  (void)refs;
  (void)count;
  (void)e;
  *out = NULL;
  return LC_OK;
}
static void select_observed(void *context, void *plan, int published) {
  (void)context;
  (void)plan;
  (void)published;
}
static void select_negatives(ps_namespace *ns, lc_pouch_crypto *target,
                             const lc_pouch_state_entry *entry,
                             const lc_pouch_state_entry *converted,
                             const char *binding, lc_error *e) {
  lc_pouch_state_entry bad = *converted;
  ++bad.version;
  REQUIRE(ps_namespace_body_select(ns, entry, &bad, e) == LC_ERR_INVALID);
  clear(e);
  for (int mutation = 0; mutation < 11; ++mutation) {
    lc_pouch_state_entry changed = *entry, alien = {0};
    switch (mutation) {
    case 0:
      ++changed.version;
      break;
    case 1:
      ++changed.index_seq;
      break;
    case 2:
      ++changed.lifetime;
      break;
    case 3:
      ++changed.query_index_seq;
      break;
    case 4:
      ++changed.updated_at_unix;
      break;
    case 5:
      --changed.retention;
      break;
    case 6:
      changed.query_hidden = !changed.query_hidden;
      break;
    case 7:
      changed.metadata = (unsigned char *)"fence=42";
      break;
    case 8:
      changed.content_type = "application/other";
      break;
    case 9:
      changed.key = "other/.staging/txn";
      break;
    default:
      changed.staged_delete_marker = !changed.staged_delete_marker;
      break;
    }
    ok(ps_namespace_body_recode(ns, target, &changed, binding, &alien, e), e);
    REQUIRE(ps_namespace_body_select(ns, entry, &alien, e) == LC_ERR_INVALID);
    clear(e);
    REQUIRE(ps_namespace_body_select(ns, &changed, &alien, e) ==
            LC_ERR_INVALID);
    clear(e);
    (void)select_check(ns, NULL, entry, NULL, e);
    lc_pouch_state_entry_cleanup(&allocator, &alien);
  }
}
static size_t select_trial(const char *path, const char *key, int codec,
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
  ps_namespace *ns = acquire(root, "select", 1, &e);
  lc_pouch_state_entry entry = {0}, converted = {0}, roundtrip = {0};
  select_seed(root, ns, alias, &entry, &e);
  const char *binding = alias ? "conversion-alias" : "namespace-test";
  lc_pouch_crypto_open_options target_options = options.crypto;
  target_options.compression_enabled = !options.crypto.compression_enabled;
  lc_pouch_crypto *target = NULL;
  ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e), &e);
  ok(ps_namespace_body_recode(ns, target, &entry, binding, &converted, &e), &e);
  lc_pouch_state_entry selected = select_projection(&entry, &converted);
  (void)select_check(ns, NULL, &entry, NULL, &e);
  if (!kind && codec == 3 && sync && alias) {
    select_negatives(ns, target, &entry, &converted, binding, &e);
    ps_namespace *foreign = acquire(root, "foreign", 1, &e);
    REQUIRE(ps_namespace_body_select(foreign, &entry, &converted, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ps_namespace_release(foreign);
    ps_checkpoint_observer observer = {select_observe, select_observed, NULL};
    ok(ps_namespace_checkpoint_observe(ns, &observer, &e), &e);
    REQUIRE(ps_namespace_body_select(ns, &entry, &converted, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "select", 0, &e);
  }
  ps_namespace_view *held = NULL;
  if (!kind)
    ok(ps_namespace_view_open(ns, &held, &e), &e);
  ps_namespace_status before_status, after_status;
  ok(ps_namespace_status_get(ns, &before_status, &e), &e);
  allocation = m.calls = 0;
  if (kind == 1)
    fail_allocation = cut ? cut : SIZE_MAX;
  if (kind >= 2)
    m.fail = cut;
  int rc = ps_namespace_body_select(ns, &entry, &converted,
                                    kind == 1 && cut % 2 ? NULL : &e);
  size_t calls = kind == 1 ? allocation : m.calls;
  fail_allocation = m.fail = 0;
  if (!cut)
    ok(rc, &e);
  else {
    REQUIRE(calls >= cut);
    REQUIRE(rc == LC_OK || rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
  }
  clear(&e);
  if (held) {
    (void)select_check(ns, held, &entry, NULL, &e);
    ps_namespace_view_close(held);
  }
  if (kind >= 2) {
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    crash(&m, path);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "select", 0, &e);
  }
  int chose_new = select_check(ns, NULL, &entry, &selected, &e);
  ok(ps_namespace_status_get(ns, &after_status, &e), &e);
  REQUIRE(after_status.checkpoint_epoch >= before_status.checkpoint_epoch);
  REQUIRE(after_status.checkpoint_epoch > before_status.checkpoint_epoch
              ? chose_new
              : !chose_new);
  REQUIRE(after_status.covered_segment == before_status.covered_segment &&
          after_status.covered_offset == before_status.covered_offset &&
          after_status.checkpoint_keys == before_status.checkpoint_keys);
  if (rc == LC_OK)
    REQUIRE(chose_new);
  else if (!chose_new) {
    ok(ps_namespace_body_select(ns, &entry, &converted, &e), &e);
    (void)select_check(ns, NULL, &selected, NULL, &e);
  }
  recode_check(root, ns, &converted, binding, &e);
  unsigned int source_kind;
  uint64_t source_id;
  REQUIRE(ps_namespace_parse_source_leaf(converted.payload_span.container_leaf,
                                         &source_kind, &source_id));
  REQUIRE(ps_namespace_source_refs(ns, source_kind, source_id) == 1);
  ps_namespace_window *pin = NULL;
  unsigned char before[31], after[31];
  struct iovec part = {before, sizeof(before)};
  if (!kind) {
    ok(ps_namespace_view_open(ns, &held, &e), &e);
    ok(ps_namespace_read_open(ns, source_kind, source_id,
                              converted.payload_span.payload_offset,
                              converted.cipher_bytes, &pin, &e),
       &e);
    ok(lc_pouch_record_io_read(ps_namespace_read_input(pin),
                               converted.payload_span.payload_offset, &part, 1,
                               &e),
       &e);
  }
  REQUIRE(ps_namespace_body_select(ns, &entry, &converted, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  ok(ps_namespace_body_recode(ns, ps_root_crypto(root), &converted, binding,
                              &roundtrip, &e),
     &e);
  ok(ps_namespace_body_select(ns, &selected, &roundtrip, &e), &e);
  lc_pouch_state_entry final = select_projection(&entry, &roundtrip);
  (void)select_check(ns, NULL, &final, NULL, &e);
  REQUIRE(!ps_namespace_source_refs(ns, source_kind, source_id));
  if (!kind) {
    (void)select_check(ns, held, &selected, NULL, &e);
    REQUIRE(ps_namespace_reclaim(ns, source_kind, source_id, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ps_namespace_view_close(held);
  }
  ok(ps_namespace_reclaim(ns, source_kind, source_id, &e), &e);
  if (!kind) {
    uint64_t closed, pending;
    ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
    REQUIRE(!closed && pending == 1);
    part.iov_base = after;
    ok(lc_pouch_record_io_read(ps_namespace_read_input(pin),
                               converted.payload_span.payload_offset, &part, 1,
                               &e),
       &e);
    REQUIRE(!memcmp(before, after, sizeof(before)));
    ps_namespace_read_close(pin);
    ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
    REQUIRE(closed == 1 && !pending);
    /* A lease update must still recognize the object's in-memory class. */
    lc_pouch_state_entry follow = final;
    follow.record_type = LC_POUCH_STATE_RECORD_STATE_META;
    follow.object_link = 0;
    ++follow.index_seq;
    ++follow.updated_at_unix;
    select_metadata(ns, &follow, 1, &e);
    final.index_seq = follow.index_seq;
    final.updated_at_unix = follow.updated_at_unix;
    (void)select_check(ns, NULL, &final, NULL, &e);
  }
  REQUIRE(ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 1) ==
          (unsigned)alias);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  if (kind >= 2)
    crash(&m, path);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "select", 0, &e);
  (void)select_check(ns, NULL, &final, NULL, &e);
  recode_check(root, ns, &roundtrip, binding, &e);
  REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(root)) ==
          options.crypto.compression_enabled);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_pouch_crypto_close(target);
  lc_pouch_state_entry_cleanup(&allocator, &entry);
  lc_pouch_state_entry_cleanup(&allocator, &converted);
  lc_pouch_state_entry_cleanup(&allocator, &roundtrip);
  REQUIRE(!unlink(path));
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
  return calls;
}
typedef struct select_pause {
  model m;
  gate gate;
  int armed;
} select_pause;
static ssize_t select_paused_read(void *context, int fd, void *bytes, size_t n,
                                  off_t offset) {
  select_pause *p = context;
  pthread_mutex_lock(&p->gate.mutex);
  if (p->armed) {
    p->armed = 0;
    p->gate.entered = 1;
    pthread_cond_broadcast(&p->gate.changed);
    while (!p->gate.released)
      pthread_cond_wait(&p->gate.changed, &p->gate.mutex);
  }
  pthread_mutex_unlock(&p->gate.mutex);
  return rd(&p->m, fd, bytes, n, offset);
}
static ssize_t select_paused_write(void *context, int fd, const void *bytes,
                                   size_t n, off_t offset) {
  select_pause *p = context;
  return wr(&p->m, fd, bytes, n, offset);
}
static int select_paused_sync(void *context, int fd) {
  select_pause *p = context;
  return sync_file(&p->m, fd);
}
static int select_paused_reserve(void *context, int fd, off_t at, off_t bytes) {
  select_pause *p = context;
  return reserve(&p->m, fd, at, bytes);
}
typedef struct select_job {
  ps_namespace *ns;
  const lc_pouch_state_entry *expected, *converted;
  int rc;
} select_job;
static void *select_thread(void *context) {
  select_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc =
      ps_namespace_body_select(job->ns, job->expected, job->converted, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void body_select_concurrency(const char *path, const char *key) {
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
    ps_root *root = NULL;
    lc_error e;
    lc_error_init(&e);
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_namespace *ns = acquire(root, "select", 1, &e);
    ps_namespace *peer = acquire(root, "peer", 1, &e);
    lc_pouch_state_entry entry = {0}, converted = {0};
    select_seed(root, ns, 0, &entry, &e);
    lc_pouch_crypto_open_options target_options = options.crypto;
    target_options.compression_enabled = 1;
    lc_pouch_crypto *target = NULL;
    ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
       &e);
    ok(ps_namespace_body_recode(ns, target, &entry, "namespace-test",
                                &converted, &e),
       &e);
    select_job job = {ns, &entry, &converted, LC_ERR_INVALID};
    pause.armed = 1;
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, select_thread, &job));
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
    lc_pouch_state_entry selected = select_projection(&entry, &converted);
    (void)select_check(ns, NULL, &selected, NULL, &e);
    recode_check(root, ns, &converted, "namespace-test", &e);
    ps_namespace_release(peer);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    lc_pouch_crypto_close(target);
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    lc_pouch_state_entry_cleanup(&allocator, &converted);
    REQUIRE(!unlink(path));
    REQUIRE(!pthread_mutex_destroy(&pause.gate.mutex));
    REQUIRE(!pthread_cond_destroy(&pause.gate.changed));
    free(pause.m.live);
    free(pause.m.stable);
    lc_error_cleanup(&e);
  }
}
static void body_select_large(const char *path, const char *key) {
  for (int encrypted = 0; encrypted < 2; ++encrypted)
    for (int sync = 0; sync < 2; ++sync) {
      lc_error e;
      lc_error_init(&e);
      ps_root_options options = {0};
      options.crypto_allocator = &allocator;
      options.crypto.key_string = encrypted ? key : NULL;
      options.storage.durable_sync = sync;
      options.storage.initial_capacity = options.storage.maximum_capacity =
          PS_DEFAULT_CAPACITY;
      ps_root *root = NULL;
      ok(ps_root_open(path, 1, &options, &root, &e), &e);
      ps_namespace *ns = acquire(root, "select", 1, &e);
      lc_pouch_state_entry request = {0}, entry = {0}, converted = {0};
      request.key = "key";
      request.content_type = "application/incompressible";
      request.payload_context = "namespace-test";
      request.version = request.index_seq = 7;
      request.lifetime = 1;
      request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
      recode_generated g = {0};
      g.source.impl = &g;
      g.source.read = recode_generate;
      g.length = 2 * PS_MIB;
      g.random = 0xdeadbeef;
      ps_namespace_append *append = NULL;
      ok(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                                   &e),
         &e);
      ok(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                      ps_namespace_append_output(append),
                                      ps_namespace_append_leaf(append),
                                      &request, &g.source, 0, &entry, &e),
         &e);
      ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
      ps_namespace_append_close(append);
      recode_projection(root, &entry, 0, &e);
      lc_free_with_allocator(&allocator, entry.metadata);
      entry.metadata_length = 32769;
      entry.metadata =
          lc_alloc_with_allocator(&allocator, entry.metadata_length);
      REQUIRE(entry.metadata);
      memset(entry.metadata, 'm', entry.metadata_length);
      select_metadata(ns, &entry, 0, &e);
      lc_pouch_crypto_open_options target_options = options.crypto;
      target_options.compression_enabled = 1;
      lc_pouch_crypto *target = NULL;
      ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
         &e);
      ok(ps_namespace_body_recode(ns, target, &entry, "namespace-test",
                                  &converted, &e),
         &e);
      largest = 0;
      measuring = 1;
      int rc = ps_namespace_body_select(ns, &entry, &converted, &e);
      measuring = 0;
      ok(rc, &e);
      REQUIRE(largest < 128 * 1024);
      printf("body selection 2 MiB encrypted=%d sync=%d metadata=%zu "
             "largest=%zu\n",
             encrypted, sync, entry.metadata_length, largest);
      lc_pouch_state_entry selected = select_projection(&entry, &converted);
      (void)select_check(ns, NULL, &selected, NULL, &e);
      recode_check(root, ns, &converted, "namespace-test", &e);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      ok(ps_root_open(path, 0, &options, &root, &e), &e);
      ns = acquire(root, "select", 0, &e);
      (void)select_check(ns, NULL, &selected, NULL, &e);
      recode_check(root, ns, &converted, "namespace-test", &e);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      lc_pouch_crypto_close(target);
      lc_pouch_state_entry_cleanup(&allocator, &entry);
      lc_pouch_state_entry_cleanup(&allocator, &converted);
      REQUIRE(!unlink(path));
      lc_error_cleanup(&e);
    }
}
static void body_select(const char *path, const char *key, int faults) {
  size_t sites = 0;
  if (!faults) {
    for (int codec = 0; codec < 4; ++codec)
      for (int sync = 0; sync < 2; ++sync)
        for (int alias = 0; alias < 2; ++alias)
          (void)select_trial(path, key, codec, sync, alias, 0, 0);
    body_select_large(path, key);
  } else {
    for (int codec = 0; codec <= 3; codec += 3)
      for (int sync = 0; sync < 2; ++sync)
        for (int kind = 1; kind <= 5; ++kind) {
          size_t count = select_trial(path, key, codec, sync, 1, kind, 0);
          REQUIRE(count);
          for (size_t cut = 1; cut <= count; ++cut) {
            (void)select_trial(path, key, codec, sync, 1, kind, cut);
            ++sites;
          }
        }
    printf("body selection verified allocation/I/O/torn sites=%zu\n", sites);
  }
}
#endif
