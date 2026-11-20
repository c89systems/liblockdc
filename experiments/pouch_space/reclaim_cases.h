#ifndef POUCH_SPACE_RECLAIM_CASES_H
#define POUCH_SPACE_RECLAIM_CASES_H

static int count_selection(void *context, lc_error *e) {
  (void)e;
  ++*(unsigned int *)context;
  return LC_OK;
}
static void checkpoint_ranges(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_key_checkpoint *c = NULL;
  ps_checkpoint_ranges captured = {0};
  lc_pouch_state_entry entry = {0};
  struct interval {
    uint64_t start, bytes, source;
  } model[96] = {{0}};
  unsigned char expected[3000], actual[3000];
  char name[32];
  unsigned int selected = 0;
  uint64_t epoch;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "ranges", 1, &ns, &error));
  OK(ps_key_checkpoint_open(ns, &c, &error));
  for (unsigned int i = 0; i < 700; ++i) {
    unsigned int slot = i * 37 % 96;
    CHECK(snprintf(name, sizeof(name), "range/%u", slot) > 0);
    memset(&entry, 0, sizeof(entry));
    entry.key = name;
    entry.content_type = "application/test";
    entry.etag = "range-etag";
    entry.seen = 1;
    entry.found = i % 7 != 0;
    entry.record_type = entry.found ? LC_POUCH_STATE_RECORD_STATE_PUT
                                    : LC_POUCH_STATE_RECORD_STATE_DELETE;
    entry.index_seq = entry.version = entry.lifetime = i + 1;
    if (entry.found) {
      entry.payload_span.present = 1;
      entry.payload_span.container_leaf = i % 3
                                              ? "body-00000000000000000001.log"
                                              : "body-00000000000000000002.log";
      entry.payload_span.payload_offset = i * 31 % 2048;
      entry.payload_span.payload_length = i * 13 % 113 + 1;
      entry.bytes = entry.cipher_bytes = entry.payload_span.payload_length;
    }
    model[slot].start = entry.payload_span.payload_offset;
    model[slot].bytes = entry.payload_span.payload_length;
    model[slot].source = entry.found ? (i % 3 ? 1 : 2) : 0;
    checkpoint_replay_one(c, &entry, 0, 0);
    if (i % 17 != 0 && i != 699)
      continue;
    for (unsigned int source = 1; source <= 2; ++source) {
      memset(expected, 0, sizeof(expected));
      memset(actual, 0, sizeof(actual));
      for (size_t j = 0; j < 96; ++j)
        if (model[j].source == source)
          for (uint64_t at = model[j].start;
               at < model[j].start + model[j].bytes; ++at)
            if (at >= 128 && at < 3128)
              expected[at - 128] = 1;
      OK(ps_key_checkpoint_ranges(c, PS_CATALOG_BODY, source, 128, 3000,
                                  &captured, &error));
      uint64_t total = 0, end = 0;
      for (size_t j = 0; j < captured.count; ++j) {
        CHECK(captured.ranges[j].bytes &&
              (!j || captured.ranges[j].offset > end));
        end = captured.ranges[j].offset + captured.ranges[j].bytes;
        CHECK(end <= sizeof(actual));
        memset(actual + captured.ranges[j].offset, 1,
               (size_t)captured.ranges[j].bytes);
        total += captured.ranges[j].bytes;
      }
      CHECK(!memcmp(expected, actual, sizeof(actual)) &&
            total == captured.bytes);
      OK(ps_key_checkpoint_ranges_select(c, PS_CATALOG_BODY, source,
                                         captured.epoch, count_selection,
                                         &selected, &error));
      lc_free_with_allocator(NULL, captured.ranges);
    }
  }
  /* Same body, new lease metadata: the captured body generation survives. */
  OK(ps_key_checkpoint_ranges(c, PS_CATALOG_BODY, 2, 0, PS_CHUNK, &captured,
                              &error));
  epoch = captured.epoch;
  lc_free_with_allocator(NULL, captured.ranges);
  entry.index_seq = 701;
  entry.record_type = LC_POUCH_STATE_RECORD_STATE_META;
  checkpoint_replay_one(c, &entry, 0, 0);
  OK(ps_key_checkpoint_ranges_select(c, PS_CATALOG_BODY, 2, epoch,
                                     count_selection, &selected, &error));
  entry.index_seq = 702;
  entry.payload_span.payload_offset += 1;
  checkpoint_replay_one(c, &entry, 0, 0);
  CHECK(ps_key_checkpoint_ranges_select(c, PS_CATALOG_BODY, 2, epoch,
                                        count_selection, &selected,
                                        &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  checkpoint_replay_one(c, NULL, 1, 9999);
  OK(ps_key_checkpoint_flush(c, &error));
  ps_key_checkpoint_close(c);
  OK(ps_key_checkpoint_open(ns, &c, &error));
  OK(ps_key_checkpoint_ranges(c, PS_CATALOG_BODY, 2, 0, PS_CHUNK, &captured,
                              &error));
  CHECK(captured.count && captured.bytes);
  lc_free_with_allocator(NULL, captured.ranges);
  ps_key_checkpoint_close(c);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}

typedef struct namespace_repack_pin {
  ps_namespace *ns;
  ps_namespace_window *body;
  uint64_t offset, count;
} namespace_repack_pin;
static int namespace_repack_visit(void *context, uint64_t segment,
                                  const lc_pouch_state_entry *entry,
                                  lc_error *e) {
  namespace_repack_pin *pin = context;
  ps_namespace_window *window = NULL;
  unsigned int kind;
  uint64_t id;
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  (void)segment;
  if (entry->control)
    return LC_OK;
  CHECK(entry->found && entry->bytes == 4 &&
        entry->payload_span.payload_length == sizeof(bytes));
  CHECK(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                       &kind, &id));
  OK(ps_namespace_read_open(pin->ns, kind, id,
                            entry->payload_span.payload_offset,
                            entry->payload_span.payload_length, &window, e));
  OK(lc_pouch_record_io_read(ps_namespace_read_input(window),
                             entry->payload_span.payload_offset, &part, 1, e));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  if (!strcmp(entry->key, "lease/0") && !pin->body) {
    pin->body = window;
    pin->offset = entry->payload_span.payload_offset;
  } else
    ps_namespace_read_close(window);
  ++pin->count;
  return LC_OK;
}
static void namespace_repack(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_repack_result result;
  ps_namespace_status info;
  ps_stats before, pinned, after;
  namespace_repack_pin pin = {0};
  uint64_t replaced = 0;
  char name[32];
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "repack", 1, &ns, &error));
  for (unsigned int i = 0; i < 120; ++i) {
    CHECK(snprintf(name, sizeof(name), "lease/%u", i) > 0);
    checkpoint_put(root, ns, name, i + 1, 1, i + 1, 1, 0);
  }
  OK(ps_namespace_checkpoint_flush(ns, &error));
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 120 && pin.body);
  for (unsigned int i = 1; i < 120; ++i) {
    CHECK(snprintf(name, sizeof(name), "lease/%u", i) > 0);
    checkpoint_put(root, ns, name, 120 + i, 2, i + 1, 1, 0);
  }
  OK(ps_namespace_checkpoint_flush(ns, &error));
  CHECK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL, &result,
                            &error) == LC_ERR_INVALID);
  CHECK(error.server_code &&
        !strcmp(error.server_code, "pouch_namespace_busy"));
  lc_error_cleanup(&error);
  ps_namespace_view_close(view);
  view = NULL;
  OK(ps_namespace_status_get(ns, &info, &error));
  CHECK(info.active_segment == 1 && info.covered_segment == 1 &&
        info.covered_offset == info.tail);
  OK(ps_status(ps_root_space(root), &before, &error));
  for (unsigned int step = 0; step < 200; ++step) {
    OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL, &result,
                           &error));
    CHECK(result.visited <= 2);
    replaced += result.replaced;
    if (result.visited < 2)
      break;
    CHECK(step < 199);
  }
  CHECK(replaced > 2);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &pinned, &error));
  CHECK(pinned.allocated_bytes < before.allocated_bytes &&
        pinned.retired_bytes);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(pinned.allocated_bytes >= after.allocated_bytes + PS_PAGE);
  OK(ps_namespace_view_open(ns, &view, &error));
  pin.count = 0;
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 120);
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  /* The completed tail selected an empty successor. Body offsets in the
   * predecessor remain stable while new appends use the successor. */
  checkpoint_put(root, ns, "lease/0", 240, 2, 1, 1, 0);
  OK(ps_namespace_status_get(ns, &info, &error));
  CHECK(info.tail && info.active_segment == 2);
  OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL, &result,
                         &error));
  CHECK(!result.replaced &&
        result.pending); /* selected old key still owns it */
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "repack", 0, &ns, &error));
  pin.ns = ns;
  pin.count = 0;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 121); /* 120 selected keys followed by uncovered update */
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  OK(ps_namespace_checkpoint_flush(ns, &error));
  replaced = 0;
  for (unsigned int step = 0; step < 200; ++step) {
    OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL, &result,
                           &error));
    replaced += result.replaced;
    if (result.visited < 2)
      break;
    CHECK(step < 199);
  }
  CHECK(replaced); /* a later sweep revisits the newly dead early body */
  OK(ps_namespace_view_open(ns, &view, &error));
  pin.count = 0;
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 120);
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
typedef struct repack_actor {
  ps_namespace *ns;
  ps_namespace_repack_result result;
  int silent, rc;
} repack_actor;
static uint64_t namespace_churn_put(ps_root *root, ps_namespace *ns,
                                    const char *key_name, uint64_t index,
                                    uint64_t version, uint64_t lifetime,
                                    size_t bytes) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry request = {0}, result = {0};
  ps_namespace_append *append = NULL;
  checkpoint_history_input input = {0};
  uint64_t start, end;
  input.source.impl = &input;
  input.source.read = checkpoint_history_read;
  input.remaining = bytes;
  request.key = (char *)key_name;
  request.content_type = "application/test";
  request.payload_context = "repack-churn";
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  request.index_seq = index;
  request.version = version;
  request.lifetime = lifetime;
  OK(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  OK(lc_pouch_record_io_tail(ps_namespace_append_output(append), &start,
                             &error));
  OK(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                  ps_namespace_append_output(append),
                                  ps_namespace_append_leaf(append), &request,
                                  &input.source, 0, &result, &error));
  CHECK(!input.remaining && input.calls > 1 && result.bytes == bytes);
  OK(lc_pouch_record_io_tail(ps_namespace_append_output(append), &end, &error));
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  return end - start;
}
typedef struct churn_pin {
  ps_namespace *ns;
  ps_namespace_window *body;
  uint64_t offset, bytes, count;
  unsigned long crc;
} churn_pin;
static void churn_read(ps_namespace_window *window, uint64_t offset,
                       uint64_t bytes, unsigned long expected, lc_error *e) {
  unsigned char scratch[PS_CHUNK];
  unsigned long crc = 0;
  uint64_t at = 0;
  while (at < bytes) {
    size_t take =
        bytes - at > sizeof(scratch) ? sizeof(scratch) : (size_t)(bytes - at);
    struct iovec part = {scratch, take};
    OK(lc_pouch_record_io_read(ps_namespace_read_input(window), offset + at,
                               &part, 1, e));
    for (size_t i = 0; i < take; ++i)
      CHECK(scratch[i] == 'h');
    crc = crc32(crc, scratch, (uInt)take);
    at += take;
  }
  CHECK(crc == expected);
}
static int namespace_churn_visit(void *context, uint64_t segment,
                                 const lc_pouch_state_entry *entry,
                                 lc_error *e) {
  churn_pin *pin = context;
  ps_namespace_window *window = NULL;
  unsigned int kind;
  uint64_t id;
  (void)segment;
  if (entry->control)
    return LC_OK;
  CHECK(entry->found && entry->payload_span.present);
  CHECK(!strcmp(entry->key, "stable") || !strcmp(entry->key, "hot"));
  CHECK(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                       &kind, &id));
  OK(ps_namespace_read_open(pin->ns, kind, id,
                            entry->payload_span.payload_offset,
                            entry->payload_span.payload_length, &window, e));
  churn_read(window, entry->payload_span.payload_offset,
             entry->payload_span.payload_length,
             entry->payload_span.payload_crc, e);
  if (!strcmp(entry->key, "stable") && !pin->body) {
    pin->body = window;
    pin->offset = entry->payload_span.payload_offset;
    pin->bytes = entry->payload_span.payload_length;
    pin->crc = entry->payload_span.payload_crc;
  } else
    ps_namespace_read_close(window);
  ++pin->count;
  return LC_OK;
}
typedef struct churn_sweep {
  ps_namespace *ns;
  uint64_t copied, mappings, stored;
} churn_sweep;
static int churn_sources(void *context, const ps_namespace_source *source,
                         lc_error *e) {
  churn_sweep *s = context;
  ps_namespace_repack_result result = {0};
  if (source->kind != PS_CATALOG_SEGMENT)
    return LC_OK;
  if (source->sealed &&
      !ps_namespace_source_refs(s->ns, source->kind, source->id))
    return ps_namespace_reclaim(s->ns, source->kind, source->id, e);
  for (unsigned int step = 0; step < 16; ++step) {
    OK(ps_namespace_repack(s->ns, source->kind, source->id, 16, NULL, NULL,
                           &result, e));
    CHECK(result.visited <= 16 && result.body_bytes <= result.mapping_bytes &&
          result.mapping_bytes <= 16 * PS_CHUNK);
    s->copied += result.mapping_bytes;
    if (!result.pending)
      break;
    CHECK(step < 15);
  }
  if ((source->sealed || result.finished_tail) &&
      !ps_namespace_source_refs(s->ns, source->kind, source->id))
    OK(ps_namespace_reclaim(s->ns, source->kind, source->id, e));
  return LC_OK;
}
static int churn_storage(void *context, const ps_namespace_source *source,
                         lc_error *e) {
  churn_sweep *s = context;
  (void)e;
  s->mappings += source->runs;
  s->stored += source->stored_bytes;
  return LC_OK;
}
static void namespace_repack_churn(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_status info;
  ps_stats stats;
  uint64_t history;
  churn_sweep sweep = {0};
  churn_pin pin = {0};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "churn", 1, &ns, &error));
  history = namespace_churn_put(root, ns, "stable", 1, 1, 1, (size_t)PS_MIB);
  sweep.ns = ns;
  OK(ps_namespace_checkpoint_flush(ns, &error));
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_churn_visit, &pin, &error));
  CHECK(pin.count == 1 && pin.body);
  ps_namespace_view_close(view);
  /* Hold a body larger than the maintenance workspace while repeatedly
   * replacing its neighbor. Logical history passes physical capacity, but
   * actual live mappings and physical ownership stay bounded. */
  for (unsigned int cycle = 0; cycle < 52; ++cycle) {
    history += namespace_churn_put(root, ns, "hot", cycle + 2, cycle + 1, 2,
                                   12 * PS_CHUNK);
    OK(ps_namespace_checkpoint_flush(ns, &error));
    OK(ps_namespace_sources_visit(ns, churn_sources, &sweep, &error));
    OK(ps_checkpoint(ps_root_space(root), &error));
    OK(ps_status(ps_root_space(root), &stats, &error));
    CHECK(stats.capacity == PS_DEFAULT_CAPACITY &&
          stats.allocated_bytes < 2 * PS_MIB);
  }
  OK(ps_namespace_status_get(ns, &info, &error));
  OK(ps_namespace_sources_visit(ns, churn_storage, &sweep, &error));
  CHECK(info.active_segment > 1 && !info.tail &&
        history > PS_DEFAULT_CAPACITY && sweep.mappings < 100 &&
        sweep.stored <= stats.allocated_bytes);
  CHECK(sweep.copied && sweep.copied < 52 * 12 * PS_CHUNK);
  churn_read(pin.body, pin.offset, pin.bytes, pin.crc, &error);
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  printf("namespace churn sync%d: logical=%llu physical_capacity=%llu "
         "allocated=%llu present_mappings=%llu selected_map_bytes=%llu\n",
         durable, (unsigned long long)history,
         (unsigned long long)stats.capacity,
         (unsigned long long)stats.allocated_bytes,
         (unsigned long long)sweep.mappings, (unsigned long long)sweep.copied);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "churn", 0, &ns, &error));
  pin.ns = ns;
  pin.count = 0;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_churn_visit, &pin, &error));
  CHECK(pin.count == 2);
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
static int repack_sync(void *context, int fd) {
  (void)context;
  return fdatasync(fd);
}
static void *repack_worker(void *context) {
  repack_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  a->rc = ps_namespace_repack(a->ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                              &a->result, a->silent ? NULL : &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void namespace_repack_concurrency(int durable, int shared) {
  commit_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
  ps_io io = {&gate, gate_read, gate_write, repack_sync, gate_reserve, NULL};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_root *peer = NULL;
  ps_namespace *peer_ns = NULL;
  ps_namespace_body *published_body = NULL;
  ps_namespace_view *view = NULL;
  namespace_repack_pin pin = {0};
  repack_actor actor = {0};
  pthread_t thread;
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  char name[32], body_leaf[32];
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.shared = shared;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "repack", 1, &ns, &error));
  if (shared) {
    options.storage.io = NULL;
    OK(ps_root_open(path, 0, &options, &peer, &error));
    OK(ps_root_namespace_acquire(peer, "repack", 0, &peer_ns, &error));
  }
  for (unsigned int i = 0; i < 120; ++i) {
    CHECK(snprintf(name, sizeof(name), "lease/%u", i) > 0);
    checkpoint_put(root, ns, name, i + 1, 1, i + 1, 1, 0);
  }
  if (shared) {
    struct iovec body_part = {(void *)"data", 4};
    OK(ps_namespace_body_begin(ns, 4, &published_body, &error));
    OK(lc_pouch_record_io_append(ps_namespace_body_output(published_body),
                                 &body_part, 1, &error));
    OK(ps_namespace_body_commit(published_body, &error));
    CHECK(snprintf(body_leaf, sizeof(body_leaf), "%s",
                   ps_namespace_body_leaf(published_body)) > 0);
  }
  OK(ps_namespace_checkpoint_flush(ns, &error));
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  ps_namespace_view_close(view);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_namespace_checkpoint_recover(ns, &error));
  actor.ns = ns;
  actor.silent = shared;
  gate.enabled = gate.hold_payload = 1;
  CHECK(pthread_create(&thread, NULL, repack_worker, &actor) == 0);
  await_gate(&gate, 0);
  /* The bounded candidate copy owns no namespace/root/allocator authority.
   * Canonical replacement, a peer publication, checkpoint and admitted body
   * consumption must finish before its I/O is released. Its stale liveness
   * must then prevent selection of the completed candidate. */
  if (shared) {
    ps_namespace_repack_result competing;
    int rc;
    checkpoint_put(peer, peer_ns, "lease/0", 121, 2, 1, 1, 0);
    OK(ps_namespace_checkpoint_recover(ns, &error));
    CHECK(!strcmp(ps_namespace_body_leaf(published_body), body_leaf));
    ps_namespace_body_close(published_body);
    rc = ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                             &competing, &error);
    CHECK(rc == LC_ERR_INVALID && error.server_code &&
          !strcmp(error.server_code, "pouch_namespace_busy"));
    lc_error_cleanup(&error);
    checkpoint_put(root, ns, "lease/1", 122, 2, 2, 1, 0);
  } else {
    checkpoint_put(root, ns, "lease/0", 121, 2, 1, 1, 0);
    put(ps_root_space(root), state(999), "peer", 4, 4);
  }
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  pthread_mutex_lock(&gate.mutex);
  gate.released = 1;
  pthread_cond_broadcast(&gate.cond);
  pthread_mutex_unlock(&gate.mutex);
  CHECK(pthread_join(thread, NULL) == 0 && actor.rc == LC_OK);
  CHECK(actor.result.visited == 1 && !actor.result.replaced &&
        actor.result.pending);
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  ps_namespace_read_close(pin.body);
  pin.body = NULL;
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                         &actor.result, &error));
  CHECK(actor.result.replaced == 1);
  OK(ps_namespace_view_open(ns, &view, &error));
  pin.count = 0;
  OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
  CHECK(pin.count == 120);
  ps_namespace_view_close(view);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  if (shared) {
    ps_namespace_release(peer_ns);
    OK(ps_root_close(peer, &error));
  }
  OK(ps_root_close(root, &error));
  if (shared) {
    checkpoint_seen seen = {0};
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "repack", 0, &ns, &error));
    pin = (namespace_repack_pin){0};
    pin.ns = ns;
    OK(ps_namespace_view_open(ns, &view, &error));
    OK(ps_namespace_view_visit(view, 1, namespace_repack_visit, &pin, &error));
    OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
    CHECK(pin.count == 120 && seen.first_version == 2 && seen.maximum == 122);
    ps_namespace_view_close(view);
    ps_namespace_read_close(pin.body);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
  }
  CHECK(pthread_cond_destroy(&gate.cond) == 0 &&
        pthread_mutex_destroy(&gate.mutex) == 0);
  clean();
}

static int reject_piece(void *context, ps_piece_publish_fn publish,
                        void *publication, lc_error *e) {
  ps_stats stats;
  (void)publish;
  (void)publication;
  OK(ps_status(context, &stats, e));
  CHECK(stats.private_bytes); /* copying finished before liveness selection */
  return lc_error_set(e, LC_ERR_INVALID, 0, "captured liveness changed", NULL,
                      NULL, "test");
}
static void owned_repack_faults(int durable, int bitmap) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_owned_run *raw = NULL, *mapped = NULL, **runs = NULL;
  size_t keep_count = bitmap ? (PS_CHUNK - PS_RUN_HEADER) / 2 : 1;
  ps_owned_range *keep = calloc(keep_count, sizeof(*keep));
  ps_piece_checksum crc = {0};
  unsigned char *baseline = malloc((size_t)PS_MIB);
  unsigned char bytes[PS_CHUNK], scratch[PS_CHUNK];
  size_t sites, count;
  int found;
  CHECK(baseline && keep);
  for (size_t i = 0; i < keep_count; ++i) {
    keep[i].offset = bitmap ? i * 2 : 73;
    keep[i].bytes = bitmap ? 1 : 91;
  }
  memset(bytes, 0x35, sizeof(bytes));
  clean();
  s = create(PS_MIB, durable, &io);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &info, &error));
  OK(ps_owned_grant(head, NULL, 0, PS_CHUNK, &raw, &found, &error));
  CHECK(found);
  OK(ps_owned_write(raw, 0, bytes, (size_t)ps_owned_run_capacity(raw), &error));
  OK(ps_owned_piece_extend(raw, 0, NULL, ps_owned_run_capacity(raw), scratch,
                           &crc, &error));
  OK(ps_owned_commit(head, ps_owned_run_capacity(raw), PS_COMMIT_DEFAULT, NULL,
                     NULL, &error));
  OK(ps_checkpoint(s, &error));
  ps_owned_run_close(raw);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 3; ++kind) {
    sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      s = piece_reopen(durable, &io);
      OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
      OK(ps_owned_grant(head, NULL, 0, 0, &raw, &found, &error));
      CHECK(found);
      m.operations = m.reads = allocation_call = 0;
      m.tear = m.persist_torn = 1;
      m.fail_at = kind == 0 ? cut : 0;
      m.fail_read = kind == 2 ? cut : 0;
      allocation_fail = kind == 1 ? (cut ? cut : (size_t)-1) : 0;
      mapped = NULL;
      int rc = ps_owned_run_repack(head, raw, NULL, keep, keep_count, NULL,
                                   NULL, &mapped, &error);
      allocation_fail = m.fail_at = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0   ? m.operations
                : kind == 1 ? allocation_call
                            : m.reads;
        CHECK(sites);
      } else {
        CHECK(rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        lc_error_cleanup(&error);
        CHECK(!mapped);
      }
      ps_owned_run_close(mapped);
      ps_owned_run_close(raw);
      ps_owned_close(head);
      OK(ps_close(s, &error));
      crash(&m);
      s = piece_reopen(durable, &io);
      OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
      OK(ps_owned_runs_capture(head, &runs, &count, &error));
      CHECK(count == 1 && ps_owned_run_mapped(runs[0]) == !cut);
      OK(ps_owned_read(runs[0], keep[0].offset, scratch, (size_t)keep[0].bytes,
                       &error));
      CHECK(!memcmp(bytes + keep[0].offset, scratch, (size_t)keep[0].bytes));
      if (cut) {
        OK(ps_owned_read(runs[0], 0, scratch, sizeof(scratch) - PS_RUN_HEADER,
                         &error));
        CHECK(!memcmp(bytes, scratch, sizeof(scratch) - PS_RUN_HEADER));
      }
      ps_owned_run_close(runs[0]);
      lc_free_with_allocator(NULL, runs);
      ps_owned_close(head);
      OK(ps_close(s, &error));
    }
    printf("piece repack sync%d bitmap%d %s: %zu failure sites\n", durable,
           bitmap,
           kind == 0   ? "write/sync"
           : kind == 1 ? "allocation"
                       : "read",
           sites);
  }
  /* A complete descriptor without its candidate payload must preserve the
   * selected raw predecessor, rather than accepting a broken replacement. */
  memcpy(m.stable, baseline, (size_t)PS_MIB);
  crash(&m);
  s = piece_reopen(durable, &io);
  OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
  OK(ps_owned_grant(head, NULL, 0, 0, &raw, &found, &error));
  OK(ps_owned_run_repack(head, raw, NULL, keep, keep_count, NULL, NULL, &mapped,
                         &error));
  ps_owned_run_close(mapped);
  ps_owned_run_close(raw);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  size_t payload = 0;
  for (size_t i = PS_DATA; i + PS_CHUNK < PS_MIB; i += PS_CELL)
    if (!memcmp(m.stable + i, "LCPSMAPR", 8)) {
      CHECK(!payload);
      payload = i;
    }
  CHECK(payload);
  m.stable[payload + 64 + (bitmap ? (PS_CHUNK - PS_RUN_HEADER + 7) / 8 : 8)] ^=
      1;
  crash(&m);
  s = piece_reopen(durable, &io);
  OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
  OK(ps_owned_runs_capture(head, &runs, &count, &error));
  CHECK(count == 1 && !ps_owned_run_mapped(runs[0]));
  OK(ps_owned_read(runs[0], 0, scratch, sizeof(scratch) - PS_RUN_HEADER,
                   &error));
  CHECK(!memcmp(bytes, scratch, sizeof(scratch) - PS_RUN_HEADER));
  ps_owned_run_close(runs[0]);
  lc_free_with_allocator(NULL, runs);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  free(baseline);
  free(keep);
  free(m.stable);
  clean();
}
static void owned_repack(int durable) {
  ps_space *s;
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_owned_run *raw = NULL, *mapped = NULL, *next = NULL, **capture = NULL;
  ps_owned_range keep[2] = {{2, 17}, {9000, 26}};
  ps_piece_checksum crc = {0};
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_stats before, after;
  unsigned char *bytes = malloc(PS_CHUNK), *readback = malloc(PS_CHUNK);
  uint64_t span, base;
  size_t count;
  int found;
  CHECK(bytes && readback);
  for (size_t i = 0; i < PS_CHUNK; ++i)
    bytes[i] = (unsigned char)(i % 251);
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &info, &error));
  OK(ps_owned_grant(head, NULL, 0, PS_CHUNK, &raw, &found, &error));
  CHECK(found);
  span = ps_owned_run_capacity(raw);
  OK(ps_owned_write(raw, 0, bytes, (size_t)span, &error));
  OK(ps_owned_piece_extend(raw, 0, NULL, span, readback, &crc, &error));
  OK(ps_owned_commit(head, span, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  CHECK(ps_owned_run_repack(head, raw, NULL, keep, 2, reject_piece, s, &mapped,
                            &error) == LC_ERR_INVALID &&
        !mapped);
  lc_error_cleanup(&error);
  OK(ps_budget_plan_piece(&plan, span, 2, 43, &error));
  plan.maintenance = 1;
  OK(ps_budget_open(s, &plan, &budget, &error));
  OK(ps_owned_run_repack(head, raw, budget, keep, 2, NULL, NULL, &mapped,
                         &error));
  OK(ps_budget_close(budget, &error));
  budget = NULL;
  CHECK(ps_owned_run_mapped(mapped) && ps_owned_run_capacity(mapped) == span);
  OK(ps_owned_read(raw, 0, readback, (size_t)span, &error));
  CHECK(!memcmp(bytes, readback, (size_t)span));
  OK(ps_owned_read(mapped, 2, readback, 17, &error));
  CHECK(!memcmp(bytes + 2, readback, 17));
  OK(ps_owned_read(mapped, 9000, readback, 26, &error));
  CHECK(!memcmp(bytes + 9000, readback, 26));
  CHECK(ps_owned_read(mapped, 19, readback, 1, &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  BAD(ps_owned_write(mapped, 2, bytes, 1, &error));
  OK(ps_owned_runs_capture(head, &capture, &count, &error));
  CHECK(count == 1 && ps_owned_run_mapped(capture[0]) &&
        ps_owned_run_base(capture[0]) == 0);
  ps_owned_run_close(capture[0]);
  lc_free_with_allocator(NULL, capture);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &before, &error));
  CHECK(before.retired_bytes >= PS_CHUNK);
  ps_owned_run_close(raw);
  raw = NULL;
  OK(ps_status(s, &after, &error));
  CHECK(before.allocated_bytes >= after.allocated_bytes + PS_CHUNK);
  keep[0].offset = 9005;
  keep[0].bytes = 5;
  OK(ps_owned_run_repack(head, mapped, NULL, keep, 1, NULL, NULL, &next,
                         &error));
  OK(ps_owned_read(mapped, 2, readback, 17, &error));
  CHECK(!memcmp(bytes + 2, readback, 17));
  CHECK(ps_owned_run_repack(head, mapped, NULL, keep, 1, NULL, NULL, &raw,
                            &error) == LC_ERR_INVALID &&
        !raw);
  lc_error_cleanup(&error);
  ps_owned_run_close(mapped);
  mapped = next;
  next = NULL;
  OK(ps_owned_read(mapped, 9005, readback, 5, &error));
  CHECK(!memcmp(bytes + 9005, readback, 5));
  OK(ps_owned_run_repack(head, mapped, NULL, NULL, 0, NULL, NULL, &next,
                         &error));
  CHECK(!next);
  ps_owned_run_close(mapped);
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &after, &error));
  CHECK(!after.allocated_bytes && after.capacity == PS_DEFAULT_CAPACITY);
  OK(ps_owned_status_get(head, &info, &error));
  CHECK(!info.runs && info.capacity == span && info.tail == span);
  /* Repeated reclaim advances logical offsets while physical capacity stays
   * fixed. No work or allocation is proportional to the historical holes. */
  for (size_t cycle = 0; cycle < 660; ++cycle) {
    base = info.capacity;
    OK(ps_owned_grant(head, NULL, base, PS_CHUNK, &raw, &found, &error));
    CHECK(found);
    OK(ps_owned_write(raw, 0, bytes, 1, &error));
    OK(ps_owned_commit(head, base + ps_owned_run_capacity(raw),
                       PS_COMMIT_DEFAULT, NULL, NULL, &error));
    OK(ps_owned_run_repack(head, raw, NULL, NULL, 0, NULL, NULL, &next,
                           &error));
    ps_owned_run_close(raw);
    if (cycle % 8 == 7)
      OK(ps_checkpoint(s, &error));
    OK(ps_owned_status_get(head, &info, &error));
    CHECK(!info.runs);
  }
  CHECK(info.tail > PS_DEFAULT_CAPACITY && info.capacity == info.tail);
  base = info.capacity;
  OK(ps_owned_grant(head, NULL, base, PS_PAGE, &raw, &found, &error));
  CHECK(found);
  OK(ps_owned_write(raw, 0, bytes, 64, &error));
  OK(ps_owned_commit(head, base + 64, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  OK(ps_checkpoint(s, &error)); /* raw-owner tail hint, not canonical replay */
  ps_owned_run_close(raw);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  s = piece_reopen(durable, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
  CHECK(info.runs == 1 && info.tail == base + 64);
  OK(ps_owned_runs_capture(head, &capture, &count, &error));
  CHECK(count == 1 && ps_owned_run_base(capture[0]) == base);
  OK(ps_owned_read(capture[0], 0, readback, 64, &error));
  CHECK(!memcmp(bytes, readback, 64));
  OK(ps_owned_retire(head, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_owned_read(capture[0], 0, readback, 64, &error));
  CHECK(!memcmp(bytes, readback, 64));
  ps_owned_run_close(capture[0]);
  lc_free_with_allocator(NULL, capture);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  free(bytes);
  free(readback);
  clean();
}
static void owned_bitmap(int durable) {
  ps_space *s;
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_owned_run *raw = NULL, *mapped = NULL;
  ps_piece_checksum crc = {0};
  ps_owned_range *keep;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_stats before, after;
  unsigned char *body = malloc(PS_CHUNK), *scratch = malloc(PS_CHUNK);
  size_t count;
  uint64_t span;
  int found;
  CHECK(body && scratch);
  for (size_t i = 0; i < PS_CHUNK; ++i)
    body[i] = (unsigned char)(i % 251);
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &info, &error));
  OK(ps_owned_grant(head, NULL, 0, PS_CHUNK, &raw, &found, &error));
  CHECK(found);
  span = ps_owned_run_capacity(raw);
  count = (size_t)(span + 1) / 2;
  keep = calloc(count, sizeof(*keep));
  CHECK(keep);
  for (size_t i = 0; i < count; ++i) {
    keep[i].offset = i * 2;
    keep[i].bytes = 1;
  }
  OK(ps_owned_write(raw, 0, body, (size_t)span, &error));
  OK(ps_owned_piece_extend(raw, 0, NULL, span, scratch, &crc, &error));
  OK(ps_owned_commit(head, span, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  OK(ps_status(s, &before, &error));
  OK(ps_budget_plan_piece(&plan, span, count, count, &error));
  CHECK(plan.payload_bytes <= 3 * PS_PAGE);
  plan.maintenance = 1;
  OK(ps_budget_open(s, &plan, &budget, &error));
  allocation_max = 0;
  measure_allocations = 1;
  OK(ps_owned_run_repack(head, raw, budget, keep, count, NULL, NULL, &mapped,
                         &error));
  measure_allocations = 0;
  CHECK(allocation_max <= PS_CHUNK + 256);
  CHECK(ps_owned_run_matches(mapped, keep, count));
  ++keep[0].offset;
  CHECK(!ps_owned_run_matches(mapped, keep, count));
  --keep[0].offset;
  OK(ps_budget_close(budget, &error));
  OK(ps_owned_piece_verify(mapped, span, scratch, &error));
  for (size_t i = 0; i < count; i += 251) {
    OK(ps_owned_read(mapped, i * 2, scratch, 1, &error));
    CHECK(scratch[0] == body[i * 2]);
  }
  CHECK(ps_owned_read(mapped, 1, scratch, 1, &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  OK(ps_checkpoint(s, &error));
  ps_owned_run_close(raw);
  OK(ps_status(s, &after, &error));
  CHECK(after.allocated_bytes <= 3 * PS_PAGE &&
        before.allocated_bytes >= after.allocated_bytes + PS_PAGE);
  ps_owned_run_close(mapped);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  s = piece_reopen(durable, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 0, NULL, &head, &info, &error));
  OK(ps_owned_grant(head, NULL, 0, 0, &mapped, &found, &error));
  CHECK(found && ps_owned_run_mapped(mapped));
  OK(ps_owned_piece_verify(mapped, span, scratch, &error));
  OK(ps_owned_read(mapped, span - 2, scratch, 1, &error));
  CHECK(scratch[0] == body[span - 2]);
  ps_owned_run_close(mapped);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  free(body);
  free(scratch);
  free(keep);
  clean();
}
static void owned_page_packing(int durable) {
  ps_space *s;
  ps_owned_head *head = NULL;
  ps_owned_status info;
  ps_owned_run *raw[3] = {NULL}, *mapped = NULL;
  ps_piece_checksum crc = {0};
  ps_owned_range keep = {0, 2000};
  ps_budget_plan plan;
  ps_budget *budget = NULL;
  ps_stats before, after;
  unsigned char bytes[PS_CHUNK], scratch[PS_CHUNK];
  uint64_t base = 0;
  int found;
  memset(bytes, 0x4c, sizeof(bytes));
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  OK(ps_owned_open(s, NULL, stream_key(1), 1, NULL, &head, &info, &error));
  for (int i = 0; i < 3; ++i) {
    OK(ps_owned_grant(head, NULL, base, PS_PAGE, &raw[i], &found, &error));
    CHECK(found);
    OK(ps_owned_write(raw[i], 0, bytes, (size_t)ps_owned_run_capacity(raw[i]),
                      &error));
    OK(ps_owned_piece_extend(raw[i], 0, NULL, ps_owned_run_capacity(raw[i]),
                             scratch, &crc, &error));
    base += ps_owned_run_capacity(raw[i]);
  }
  OK(ps_owned_commit(head, base, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  OK(ps_status(s, &before, &error));
  CHECK(before.allocated_bytes == 3 * PS_PAGE);
  for (int i = 0; i < 3; ++i) {
    memset(&plan, 0, sizeof(plan));
    plan.maintenance = 1;
    OK(ps_budget_plan_piece(&plan, ps_owned_run_capacity(raw[i]), 1, keep.bytes,
                            &error));
    OK(ps_budget_open(s, &plan, &budget, &error));
    OK(ps_owned_run_repack(head, raw[i], budget, &keep, 1, NULL, NULL, &mapped,
                           &error));
    OK(ps_budget_close(budget, &error));
    OK(ps_owned_read(mapped, 0, scratch, 2000, &error));
    CHECK(!memcmp(bytes, scratch, 2000));
    OK(ps_checkpoint(s, &error));
    ps_owned_run_close(raw[i]);
    ps_owned_run_close(mapped);
  }
  OK(ps_status(s, &after, &error));
  CHECK(after.allocated_bytes <= 2 * PS_PAGE);
  ps_owned_close(head);
  OK(ps_close(s, &error));
  clean();
}
static void stream_repack(int durable) {
  ps_space *s;
  ps_stream *stream;
  ps_stream_reader *old = NULL;
  ps_stream_piece_info piece;
  ps_stream_info info;
  ps_owned_range keep;
  ps_stats before, pinned, after;
  unsigned char *a = malloc(22000), *b = malloc(22000), *got = malloc(22000);
  uint64_t first, second, cursor = 0, start, end;
  size_t n;
  int found;
  CHECK(a && b && got);
  memset(a, 'a', 22000);
  memset(b, 'b', 22000);
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  stream = stream_open(s, 1, 1, NULL, NULL);
  OK(canonical_append(stream, a, 22000, 317, 1, &first, &error));
  OK(canonical_append(stream, b, 22000, 317, 2, &second, &error));
  OK(ps_stream_seal(stream, &error));
  OK(ps_stream_read_open(stream, first + 43, 22000, &old, &error));
  OK(ps_status(s, &before, &error));
  for (;;) {
    OK(ps_stream_piece_next(stream, cursor, &piece, &found, &error));
    if (!found)
      break;
    cursor = piece.base + piece.bytes;
    start = second + 43 > piece.base ? second + 43 : piece.base;
    end = second + 43 + 22000 < piece.base + piece.completed_bytes
              ? second + 43 + 22000
              : piece.base + piece.completed_bytes;
    keep.offset = start - piece.base;
    keep.bytes = start < end ? end - start : 0;
    OK(ps_stream_piece_repack(stream, piece.base, NULL,
                              keep.bytes ? &keep : NULL, keep.bytes ? 1 : 0,
                              NULL, NULL, &error));
  }
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &pinned, &error));
  CHECK(pinned.retired_bytes);
  OK(ps_stream_read(old, got, 22000, &n, &error));
  CHECK(n == 22000 && !memcmp(got, a, n));
  ps_stream_reader_close(old);
  OK(ps_status(s, &after, &error));
  CHECK(!after.retired_bytes && after.allocated_bytes < before.allocated_bytes);
  OK(ps_stream_read_open(stream, second + 43, 22000, &old, &error));
  OK(ps_stream_read(old, got, 22000, &n, &error));
  CHECK(n == 22000 && !memcmp(got, b, n));
  ps_stream_reader_close(old);
  old = NULL;
  CHECK(ps_stream_read_open(stream, first + 43, 1, &old, &error) ==
        LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  CHECK(!old);
  OK(ps_stream_status(stream, &info, &error));
  CHECK(info.runs == 2 && info.sealed);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  s = piece_reopen(durable, NULL);
  stream = stream_open(s, 1, 0, &info.ref, NULL);
  OK(ps_stream_read_open(stream, second + 43, 22000, &old, &error));
  OK(ps_stream_read(old, got, 22000, &n, &error));
  CHECK(n == 22000 && !memcmp(got, b, n));
  ps_stream_reader_close(old);
  OK(ps_stream_close(stream, &error));
  OK(ps_close(s, &error));
  free(a);
  free(b);
  free(got);
  clean();
}
#endif
