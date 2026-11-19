#ifndef POUCH_SPACE_NAMESPACE_CONVERSION_CASES_H
#define POUCH_SPACE_NAMESPACE_CONVERSION_CASES_H
#include "compound_recode_cases.h"
#include "namespace_internal.h"

static const char *sweep_keys[] = {"one", "two", "three"};
static void sweep_put(ps_root *root, ps_namespace *ns, size_t index,
                      size_t bytes, lc_error *e) {
  lc_pouch_state_entry request = {0}, result = {0};
  ps_namespace_append *append = NULL;
  recode_generated g = {0};
  g.source.impl = &g;
  g.source.read = recode_generate;
  g.length = bytes;
  g.random = 0xdeadbeef ^ (uint32_t)index;
  char *context = lc_pouch_state_crypto_context(&allocator, "sweep",
                                                sweep_keys[index - 1], 1, 1);
  REQUIRE(context);
  request.key = (char *)sweep_keys[index - 1];
  request.content_type = "application/incompressible";
  request.payload_context = context;
  request.version = 1;
  request.index_seq = request.lifetime = index;
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  request.metadata = (unsigned char *)request.key;
  request.metadata_length = strlen(request.key);
  ok(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append, e),
     e);
  ok(lc_pouch_state_record_append(
         &allocator, ps_root_crypto(root), ps_namespace_append_output(append),
         ps_namespace_append_leaf(append), &request, &g.source, 0, &result, e),
     e);
  ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, e), e);
  ps_namespace_append_close(append);
  lc_free_with_allocator(&allocator, context);
  lc_pouch_state_entry_cleanup(&allocator, &result);
}
typedef struct sweep_seen {
  ps_root *root;
  ps_namespace *ns;
  lc_pouch_crypto *target;
  size_t count, bytes, keys;
} sweep_seen;
static int sweep_visit(void *context, uint64_t segment,
                       const lc_pouch_state_entry *entry, lc_error *e) {
  sweep_seen *v = context;
  REQUIRE(!segment);
  if (entry->control) {
    REQUIRE(entry->index_seq == v->count && entry->query_index_seq == v->count);
    return LC_OK;
  }
  size_t index;
  for (index = 1; index <= v->count; ++index)
    if (!strcmp(entry->key, sweep_keys[index - 1]))
      break;
  REQUIRE(index <= v->count && entry->found && entry->payload_span.present &&
          entry->version == 1 && entry->index_seq == index &&
          entry->query_index_seq == index && entry->lifetime == index &&
          entry->bytes == v->bytes &&
          entry->metadata_length == strlen(entry->key) &&
          !memcmp(entry->metadata, entry->key, entry->metadata_length));
  int matches = 0;
  ok(lc_pouch_crypto_descriptor_matches(v->target, entry->descriptor, &matches,
                                        e),
     e);
  REQUIRE(matches);
  unsigned int kind;
  uint64_t id;
  REQUIRE(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                         &kind, &id));
  ps_namespace_window *window = NULL;
  ok(ps_namespace_read_open(v->ns, kind, id, entry->payload_span.payload_offset,
                            entry->cipher_bytes, &window, e),
     e);
  lc_pouch_record_span span;
  ok(lc_pouch_record_span_bind(&span, ps_namespace_read_input(window),
                               entry->payload_span.payload_offset,
                               entry->cipher_bytes, NULL, 0, e),
     e);
  char *binding =
      lc_pouch_state_crypto_context(&allocator, "sweep", entry->key, 1, 1);
  REQUIRE(binding);
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *source = NULL;
  ok(lc_pouch_crypto_reader_open(ps_root_crypto(v->root), &reader, e), e);
  ok(lc_pouch_crypto_reader_bind_source(reader, entry->payload_context, binding,
                                        &span.source, entry->cipher_bytes,
                                        entry->descriptor, &source, e),
     e);
  unsigned char bytes[1024];
  size_t at = 0, got;
  uint32_t random = 0xdeadbeef ^ (uint32_t)index;
  while ((got = source->read(source, bytes, sizeof(bytes), e))) {
    int same = 1;
    REQUIRE(got <= v->bytes - at);
    for (size_t i = 0; i < got; ++i)
      if (bytes[i] != recode_random(&random))
        same = 0;
    REQUIRE(same);
    at += got;
  }
  ok(e->code, e);
  REQUIRE(at == v->bytes);
  lc_pouch_crypto_reader_close(reader);
  lc_free_with_allocator(&allocator, binding);
  ps_namespace_read_close(window);
  ++v->keys;
  return LC_OK;
}
static void sweep_check(ps_root *root, ps_namespace *ns,
                        lc_pouch_crypto *target, size_t keys, size_t bytes,
                        lc_error *e) {
  ps_namespace_view *view = NULL;
  sweep_seen seen = {root, ns, target, keys, bytes, 0};
  ok(ps_namespace_view_open(ns, &view, e), e);
  ok(ps_namespace_view_visit(view, 1, sweep_visit, &seen, e), e);
  ps_namespace_view_close(view);
  REQUIRE(seen.keys == keys);
}
static int sweep_run(ps_namespace *ns, lc_pouch_crypto *target, size_t records,
                     size_t maintenance, ps_namespace_conversion_result *total,
                     lc_error *e) {
  ps_namespace_conversion *work = NULL;
  int rc = ps_namespace_conversion_open(ns, target, &work, e);
  size_t calls = 0;
  ps_namespace_conversion_result part = {0};
  memset(total, 0, sizeof(*total));
  while (rc == LC_OK && !part.finished) {
    rc = ps_namespace_conversion_step(work, records, maintenance, &part, e);
    if (rc == LC_OK) {
      REQUIRE(part.visited <= records && part.maintenance <= maintenance &&
              !part.waiting);
      total->visited += part.visited;
      total->converted += part.converted;
      total->matching += part.matching;
      total->maintenance += part.maintenance;
      total->reclaimed += part.reclaimed;
      total->finished = part.finished;
    }
    REQUIRE(++calls < 8192);
  }
  ps_namespace_conversion_close(work);
  return rc;
}
static void sweep_pressure(const char *path, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync) {
      lc_error e;
      lc_error_init(&e);
      ps_root_options options = {0};
      options.crypto_allocator = &allocator;
      options.crypto.key_string = codec & 1 ? key : NULL;
      options.crypto.compression_enabled = !!(codec & 2);
      options.storage.durable_sync = sync;
      options.storage.initial_capacity = options.storage.maximum_capacity =
          PS_DEFAULT_CAPACITY;
      ps_root *root = NULL;
      ok(ps_root_open(path, 1, &options, &root, &e), &e);
      ps_namespace *ns = acquire(root, "sweep", 1, &e);
      for (size_t index = 1; index <= 3; ++index)
        sweep_put(root, ns, index, 2 * PS_MIB, &e);
      ok(ps_namespace_checkpoint_flush(ns, &e), &e);
      ok(ps_flush(ps_root_space(root), &e), &e);
      lc_pouch_crypto_open_options target_options = options.crypto;
      target_options.compression_enabled = !options.crypto.compression_enabled;
      lc_pouch_crypto *target = NULL;
      ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
         &e);
      ps_namespace_conversion *work = NULL;
      ok(ps_namespace_conversion_open(ns, target, &work, &e), &e);
      size_t calls = 0, converted = 0;
      int restarted = 0;
      largest = 0;
      measuring = 1;
      ps_namespace_conversion_result part = {0};
      while (!part.finished) {
        int rc = ps_namespace_conversion_step(work, 1, 3, &part, &e);
        if (rc != LC_OK)
          fprintf(stderr, "sweep codec=%d sync=%d call=%zu converted=%zu\n",
                  codec, sync, calls, converted);
        ok(rc, &e);
        REQUIRE(part.visited <= 1 && part.maintenance <= 3 && !part.waiting);
        converted += part.converted;
        REQUIRE(++calls < 8192);
        if (converted == 1 && !restarted) {
          measuring = 0;
          ps_namespace_conversion_close(work);
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          ok(ps_root_open(path, 0, &options, &root, &e), &e);
          ns = acquire(root, "sweep", 0, &e);
          ok(ps_namespace_conversion_open(ns, target, &work, &e), &e);
          restarted = 1;
          memset(&part, 0, sizeof(part));
          measuring = 1;
        }
      }
      measuring = 0;
      REQUIRE(converted == 3 && restarted && largest < 256 * 1024);
      ps_namespace_conversion_close(work);
      sweep_check(root, ns, target, 3, 2 * PS_MIB, &e);
      ok(ps_checkpoint(ps_root_space(root), &e), &e);
      ps_stats before, after;
      ok(ps_status(ps_root_space(root), &before, &e), &e);
      REQUIRE(before.capacity == PS_DEFAULT_CAPACITY &&
              before.allocated_bytes < 8 * PS_MIB);
      ps_namespace_conversion_result total;
      ok(sweep_run(ns, target, 2, 4, &total, &e), &e);
      REQUIRE(total.visited == 3 && !total.converted && total.matching == 3 &&
              total.finished);
      ok(ps_status(ps_root_space(root), &after, &e), &e);
      REQUIRE(after.syncs == before.syncs &&
              after.allocated_bytes == before.allocated_bytes);
      printf("namespace conversion codec=%d sync=%d live=6291456 capacity=%llu "
             "allocated=%llu steps=%zu largest=%zu\n",
             codec, sync, (unsigned long long)before.capacity,
             (unsigned long long)before.allocated_bytes, calls, largest);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      lc_pouch_crypto_close(target);
      REQUIRE(!unlink(path));
      lc_error_cleanup(&e);
    }
}
static size_t sweep_fault_trial(const char *path, const char *key, int codec,
                                int sync, int kind, size_t cut) {
  lc_error e;
  lc_error_init(&e);
  model m = {0};
  m.live = calloc(1, PS_MIB);
  m.stable = calloc(1, PS_MIB);
  REQUIRE(m.live && m.stable);
  m.torn = kind >= 2 ? kind - 2 : 0;
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.crypto_allocator = &allocator;
  options.crypto.key_string = codec & 1 ? key : NULL;
  options.crypto.compression_enabled = !!(codec & 2);
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = sync;
  options.storage.io = &io;
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "sweep", 1, &e);
  for (size_t index = 1; index <= 2; ++index)
    sweep_put(root, ns, index, 257, &e);
  ok(ps_namespace_checkpoint_flush(ns, &e), &e);
  ok(ps_flush(ps_root_space(root), &e), &e);
  lc_pouch_crypto_open_options target_options = options.crypto;
  target_options.compression_enabled = !options.crypto.compression_enabled;
  lc_pouch_crypto *target = NULL;
  ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e), &e);
  allocation = m.calls = 0;
  if (kind == 1)
    fail_allocation = cut ? cut : SIZE_MAX;
  else
    m.fail = cut;
  ps_namespace_conversion_result total;
  int rc =
      sweep_run(ns, target, 1, 2, &total, kind == 1 && cut % 2 ? NULL : &e);
  size_t calls = kind == 1 ? allocation : m.calls;
  fail_allocation = m.fail = 0;
  if (!cut)
    ok(rc, &e);
  else {
    REQUIRE(calls >= cut);
    REQUIRE(rc == LC_OK || rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
  }
  clear(&e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  crash(&m, path);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  uint64_t reclaimed, skipped;
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, &e), &e);
  ns = acquire(root, "sweep", 0, &e);
  if (rc == LC_OK)
    sweep_check(root, ns, target, 2, 257, &e);
  ok(sweep_run(ns, target, 1, 2, &total, &e), &e);
  REQUIRE(total.finished && total.visited == 2 &&
          total.converted + total.matching == 2);
  sweep_check(root, ns, target, 2, 257, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_pouch_crypto_close(target);
  free(m.live);
  free(m.stable);
  REQUIRE(!unlink(path));
  lc_error_cleanup(&e);
  return calls;
}
static void sweep_faults(const char *path, const char *key) {
  size_t sites = 0;
  for (int codec = 0; codec <= 3; codec += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int kind = 1; kind <= 5; ++kind) {
        size_t count = sweep_fault_trial(path, key, codec, sync, kind, 0);
        REQUIRE(count);
        for (size_t cut = 1; cut <= count; ++cut) {
          (void)sweep_fault_trial(path, key, codec, sync, kind, cut);
          ++sites;
        }
      }
  printf("namespace conversion verified allocation/I/O/torn sites=%zu\n",
         sites);
}
typedef struct sweep_capture {
  const char *key;
  lc_pouch_state_entry *out;
  size_t count;
} sweep_capture;
static int sweep_capture_entry(void *context, uint64_t segment,
                               const lc_pouch_state_entry *entry, lc_error *e) {
  sweep_capture *capture = context;
  REQUIRE(!segment);
  if (!entry->control && !strcmp(entry->key, capture->key)) {
    compound_clone(entry, capture->out, e);
    ++capture->count;
  }
  return LC_OK;
}
static void sweep_capture_key(ps_namespace *ns, const char *key,
                              lc_pouch_state_entry *out, lc_error *e) {
  ps_namespace_view *view = NULL;
  sweep_capture capture = {key, out, 0};
  ok(ps_namespace_view_open(ns, &view, e), e);
  ok(ps_namespace_view_visit(view, 1, sweep_capture_entry, &capture, e), e);
  ps_namespace_view_close(view);
  REQUIRE(capture.count == 1);
}
typedef struct sweep_cursor_job {
  ps_key_cursor *cursor;
  lc_pouch_state_entry entry;
  int rc, found;
} sweep_cursor_job;
static void *sweep_cursor_thread(void *context) {
  sweep_cursor_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc = ps_key_cursor_next(job->cursor, &job->entry, &job->found, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void sweep_guards(const char *path, const char *key) {
  for (int sync = 0; sync < 2; ++sync) {
    lc_error e;
    lc_error_init(&e);
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
    options.crypto_allocator = &allocator;
    options.crypto.key_string = key;
    options.storage.initial_capacity = options.storage.maximum_capacity =
        PS_MIB;
    options.storage.durable_sync = sync;
    options.storage.io = &io;
    ps_root *root = NULL;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_namespace *ns = acquire(root, "sweep", 1, &e);
    ps_namespace *peer = acquire(root, "peer", 1, &e);
    sweep_put(root, ns, 1, 257, &e);
    sweep_put(root, ns, 2, 257, &e);
    ok(ps_namespace_checkpoint_flush(ns, &e), &e);
    lc_pouch_crypto_open_options target_options = options.crypto;
    target_options.compression_enabled = 1;
    lc_pouch_crypto *target = NULL;
    ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
       &e);
    lc_pouch_state_entry entry = {0};
    sweep_capture_key(ns, "one", &entry, &e);
    ps_namespace_conversion *work = NULL, *other = NULL;
    ok(ps_namespace_conversion_open(ns, target, &work, &e), &e);
    REQUIRE(ps_namespace_conversion_open(ns, target, &other, &e) ==
                LC_ERR_INVALID &&
            !other);
    clear(&e);
    ps_namespace_recode *raw = NULL;
    char *binding =
        lc_pouch_state_crypto_context(&allocator, "sweep", "one", 1, 1);
    REQUIRE(binding);
    REQUIRE(ps_namespace_recode_begin(ns, target, &entry, binding, &raw, &e) ==
                LC_ERR_INVALID &&
            !raw);
    clear(&e);
    ps_namespace_conversion_result result = {.visited = 999};
    REQUIRE(ps_namespace_conversion_step(work, 0, 2, &result, &e) ==
                LC_ERR_INVALID &&
            result.visited == 999);
    clear(&e);
    ps_namespace_view *view = NULL;
    ok(ps_namespace_view_open(ns, &view, &e), &e);
    ok(ps_namespace_conversion_step(work, 1, 2, &result, &e), &e);
    REQUIRE(result.waiting && !result.finished && !result.converted);
    ok(append(root, peer, 1, 0, 31, &e), &e);
    count(peer, 1, &e);
    ps_namespace_view_close(view);
    /* A logical update between steps invalidates the cursor even after the
     * new key checkpoint is fully flushed. Reopening retains that update. */
    entry.index_seq = 3;
    select_metadata(ns, &entry, 1, &e);
    result.visited = 999;
    REQUIRE(ps_namespace_conversion_step(work, 1, 2, &result, &e) ==
                LC_ERR_INVALID &&
            result.visited == 999);
    clear(&e);
    ps_namespace_conversion_close(work);
    ok(sweep_run(ns, target, 1, 2, &result, &e), &e);
    REQUIRE(result.finished && result.visited == 2 && result.converted == 2);
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    sweep_capture_key(ns, "one", &entry, &e);
    int matches = 0;
    ok(lc_pouch_crypto_descriptor_matches(target, entry.descriptor, &matches,
                                          &e),
       &e);
    REQUIRE(matches && entry.version == 1 && entry.index_seq == 3 &&
            entry.lifetime == 1 && entry.query_index_seq == 1);
    /* Failed next leaves both caller-owned metadata and traversal position
     * untouched; the next successful call reads the first slot. */
    struct ps_namespace_data *data = NULL;
    ok(ps_ns_data_get(ns, &data, &e), &e);
    ps_key_cursor *cursor = NULL;
    ok(ps_key_cursor_open(data->checkpoint, &cursor, &e), &e);
    lc_pouch_state_entry sentinel = {.version = 999};
    int found = 7;
    allocation = 0;
    fail_allocation = 1;
    int rc = ps_key_cursor_next(cursor, &sentinel, &found, &e);
    fail_allocation = 0;
    REQUIRE(rc == LC_ERR_NOMEM && !found && sentinel.version == 999);
    clear(&e);
    ok(ps_key_cursor_next(cursor, &sentinel, &found, &e), &e);
    REQUIRE(found && !strcmp(sentinel.key, "two"));
    lc_pouch_state_entry_cleanup(&allocator, &sentinel);
    ps_key_cursor_close(cursor);
    /* Pause an actual metadata read. A same-namespace publication must
     * complete without waiting for that I/O, then invalidate its result. */
    ok(ps_key_cursor_open(data->checkpoint, &cursor, &e), &e);
    sweep_cursor_job job = {.cursor = cursor, .entry = {.version = 999}};
    pause.armed = 1;
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, sweep_cursor_thread, &job));
    pthread_mutex_lock(&pause.gate.mutex);
    while (!pause.gate.entered)
      pthread_cond_wait(&pause.gate.changed, &pause.gate.mutex);
    pthread_mutex_unlock(&pause.gate.mutex);
    entry.index_seq = 4;
    select_metadata(ns, &entry, 1, &e);
    pthread_mutex_lock(&pause.gate.mutex);
    pause.gate.released = 1;
    pthread_cond_broadcast(&pause.gate.changed);
    pthread_mutex_unlock(&pause.gate.mutex);
    REQUIRE(!pthread_join(thread, NULL));
    REQUIRE(job.rc == LC_ERR_INVALID && !job.found && job.entry.version == 999);
    REQUIRE(ps_key_cursor_check(cursor, &e) == LC_ERR_INVALID);
    clear(&e);
    ps_key_cursor_close(cursor);
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    sweep_capture_key(ns, "one", &entry, &e);
    REQUIRE(entry.index_seq == 4 && entry.version == 1);
    /* A matching codec still requires authentic binding to the selected key. */
    char *wrong = NULL;
    ok(lc_pouch_crypto_rebind_descriptor(target, entry.descriptor, binding,
                                         "wrong-key-binding", &wrong, &e),
       &e);
    lc_free_with_allocator(&allocator, entry.descriptor);
    entry.descriptor = wrong;
    entry.index_seq = 5;
    select_metadata(ns, &entry, 0, &e);
    rc = sweep_run(ns, target, 1, 2, &result, &e);
    if (rc != LC_ERR_PROTOCOL)
      fprintf(stderr, "matching binding rejected rc=%d: %s\n", rc,
              e.message ? e.message : "?");
    REQUIRE(rc == LC_ERR_PROTOCOL);
    clear(&e);
    lc_free_with_allocator(&allocator, binding);
    lc_pouch_state_entry_cleanup(&allocator, &entry);
    lc_pouch_crypto_close(target);
    ps_namespace_release(peer);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(!unlink(path));
    REQUIRE(!pthread_mutex_destroy(&pause.gate.mutex));
    REQUIRE(!pthread_cond_destroy(&pause.gate.changed));
    free(pause.m.live);
    free(pause.m.stable);
    lc_error_cleanup(&e);
  }
}
#endif
