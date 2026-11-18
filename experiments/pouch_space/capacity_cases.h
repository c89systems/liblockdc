#ifndef POUCH_SPACE_CAPACITY_CASES_H
#define POUCH_SPACE_CAPACITY_CASES_H
#include "checkpoint_cases.h"
#include "namespace_work.h"

static void standing_bound(void) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry entry;
  lc_pouch_record_header header;
  ps_checkpoint_record encoded = {0};
  uint64_t bound, expected;
  char name[32];
  unsigned char side[256] = {0};
  checkpoint_fixture(&entry, name, 0);
  entry.metadata = side;
  entry.metadata_length = sizeof(side);
  OK(ps_checkpoint_record_encode(&allocator, &entry, &encoded, &error));
  OK(lc_pouch_record_header_decode(encoded.bytes, &header, &error));
  allocation_fail = 1;
  allocation_call = 0;
  OK(lc_pouch_state_record_delete_bound(
      header.type, header.key_len,
      encoded.bytes + LC_POUCH_RECORD_HEADER_BYTES + header.key_len,
      header.meta_len, &bound, &error));
  CHECK(!allocation_call);
  allocation_fail = 0;
  entry.found = 0;
  entry.record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
  OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &entry, 0,
                                            &expected, &error));
  CHECK(bound == expected && bound < encoded.length);
  entry.found = 1;
  CHECK(lc_pouch_state_entry_delete_bytes(&entry) == expected);
  CHECK(lc_pouch_state_record_delete_bound(header.type, header.key_len,
                                           encoded.bytes, 73, &bound,
                                           &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  CHECK(lc_pouch_state_record_delete_bound(9, 1, encoded.bytes, encoded.length,
                                           &bound, &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  CHECK(lc_pouch_state_record_delete_bound(
            header.type, LC_POUCH_STATE_RECORD_KEY_MAX_BYTES + 1, encoded.bytes,
            encoded.length, &bound, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  OK(lc_pouch_state_record_delete_bound(LC_POUCH_STATE_RECORD_STATE_DELETE, 1,
                                        NULL, 0, &bound, &error));
  CHECK(!bound);
  ps_checkpoint_record_close(&encoded);
}
typedef struct standing_input {
  ps_space *space;
  size_t calls, key_bytes;
} standing_input;
static size_t standing_input_read(lc_source *source, void *bytes, size_t count,
                                  lc_error *e) {
  standing_input *input = source->impl;
  ps_stats stats;
  (void)bytes;
  (void)count;
  (void)e;
  ++input->calls;
  OK(ps_status(input->space, &stats, &error));
  CHECK(stats.reserve_payload_bytes > 2 * input->key_bytes);
  return 0;
}
static void standing_prefix(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_append *append = NULL;
  lc_pouch_state_entry request = {0}, result = {0};
  lc_allocator allocator = {0};
  ps_stats before, after;
  ps_namespace_status prior, current;
  standing_input input = {0};
  lc_source source = {0};
  unsigned char filler[PS_PAGE] = {0};
  uint64_t next = 1;
  char *large = malloc(60 * 1024 + 1);
  CHECK(large);
  memset(large, 'k', 60 * 1024);
  large[60 * 1024] = 0;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_status_get(ns, &prior, &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  input.space = ps_root_space(root);
  input.key_bytes = 60 * 1024;
  source.impl = &input;
  source.read = standing_input_read;
  request.key = large;
  request.content_type = "application/test";
  request.payload_context = "checkpoint";
  request.index_seq = request.lifetime = 2;
  request.version = 1;
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  for (int full = 0; full < 2; ++full) {
    if (full)
      budget_fill(ps_root_space(root), &next, filler);
    input.calls = 0;
    OK(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                                 &error));
    int rc = lc_pouch_state_record_append(
        &allocator, NULL, ps_namespace_append_output(append),
        ps_namespace_append_leaf(append), &request, &source, 0, &result,
        &error);
    if (full) {
      CHECK(rc == LC_ERR_INVALID && !input.calls);
      lc_error_cleanup(&error);
    } else {
      OK(rc);
      CHECK(input.calls == 1);
    }
    /* Discard even the successfully encoded body: its grown requirement is
     * released only after the private stream suffix has been cleared. */
    ps_namespace_append_close(append);
    lc_pouch_state_entry_cleanup(&allocator, &result);
    OK(ps_status(ps_root_space(root), &after, &error));
    CHECK(after.reserve_owners == before.reserve_owners &&
          after.reserve_payload_bytes == before.reserve_payload_bytes &&
          after.reserve_metadata_bytes == before.reserve_metadata_bytes &&
          after.reserve_entries == before.reserve_entries && !after.budgets);
    OK(ps_namespace_status_get(ns, &current, &error));
    CHECK(current.active_segment == prior.active_segment &&
          current.tail == prior.tail && !current.pending_keys);
  }
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  free(large);
  clean();
}

static void reserve_observe(ps_space *s, const ps_budget_plan *base,
                            const ps_budget_plan *plans, ps_reserve **owners,
                            size_t count) {
  ps_budget_plan expected = *base;
  ps_stats stats;
  size_t live = 0;
  for (size_t i = 0; i < count; ++i) {
    if (!owners[i])
      continue;
    ++live;
    if (plans[i].payload_bytes > expected.payload_bytes)
      expected.payload_bytes = plans[i].payload_bytes;
    if (plans[i].metadata_bytes > expected.metadata_bytes)
      expected.metadata_bytes = plans[i].metadata_bytes;
    if (plans[i].entries > expected.entries)
      expected.entries = plans[i].entries;
  }
  OK(ps_status(s, &stats, &error));
  CHECK(stats.reserve_owners == live &&
        stats.reserve_payload_bytes == expected.payload_bytes &&
        stats.reserve_metadata_bytes == expected.metadata_bytes &&
        stats.reserve_entries == expected.entries);
}
static void standing_value_bound(void) {
  ps_space *s;
  ps_budget_plan plan = {0}, before;
  ps_budget *budget = NULL;
  ps_writer *writer = NULL;
  ps_reader *reader = NULL;
  ps_stats stats;
  unsigned char bytes[PS_PAGE], output[PS_PAGE];
  size_t got;
  clean();
  s = create(PS_DEFAULT_CAPACITY, 0, NULL);
  memset(bytes, 0xa9, sizeof(bytes));
  (void)put(s, state(1), bytes, PS_PAGE - PS_CELL, PS_PAGE);
  OK(ps_budget_plan_value_bound(&plan, PS_PAGE, &error));
  before = plan;
  CHECK(ps_budget_plan_value_bound(&plan,
                                   (PS_JOURNAL - 64) / 24 * (uint64_t)PS_PAGE,
                                   &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  CHECK(plan.payload_bytes == before.payload_bytes &&
        plan.metadata_bytes == before.metadata_bytes &&
        plan.entries == before.entries &&
        plan.maintenance == before.maintenance);
  OK(ps_budget_open(s, &plan, &budget, &error));
  OK(ps_begin(s, budget, state(2), &writer, &error));
  /* This shorter value first fills the old 64-byte tail, then allocates its
   * remaining bytes in one new page: two extents under an aligned limit. */
  OK(ps_write(writer, bytes, PS_PAGE - 1, &error));
  OK(ps_publish(writer, NULL, &error));
  ps_writer_close(writer);
  OK(ps_budget_close(budget, &error));
  OK(ps_read_open(s, state(2), NULL, &reader, &error));
  OK(ps_read(reader, output, sizeof(output), &got, &error));
  CHECK(got == PS_PAGE - 1 && !memcmp(output, bytes, got));
  ps_reader_close(reader);
  OK(ps_status(s, &stats, &error));
  CHECK(stats.allocated_bytes == 2 * PS_PAGE);
  OK(ps_close(s, &error));
  clean();
}
static void standing_owners(void) {
  enum { OWNERS = 127 };
  ps_reserve *owners[OWNERS] = {0}, *failed = NULL;
  ps_budget_plan plans[OWNERS] = {{0}}, base = {0}, changed;
  ps_space *s;
  clean();
  s = create(PS_DEFAULT_CAPACITY, 0, NULL);
  OK(ps_budget_plan_value(&base, PS_PAGE, &error));
  OK(ps_reserve_base(s, &base, 0, &error));
  allocation_fail = 1;
  allocation_call = 0;
  CHECK(ps_reserve_open(s, &base, 0, &failed, &error) == LC_ERR_NOMEM &&
        !failed);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  for (size_t i = 0; i < OWNERS; ++i) {
    plans[i].payload_bytes = (i * 17 % 11) * PS_PAGE;
    plans[i].metadata_bytes = i * 37 % 2000;
    plans[i].entries = i % 17;
    OK(ps_reserve_open(s, &plans[i], 0, &owners[i], &error));
    reserve_observe(s, &base, plans, owners, OWNERS);
  }
  CHECK(ps_close(s, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  changed = base;
  changed.payload_bytes = PS_DEFAULT_CAPACITY;
  CHECK(ps_reserve_resize(owners[3], &changed, 0, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  CHECK(ps_reserve_base(s, &changed, 0, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  CHECK(ps_reserve_open(s, &changed, 0, &failed, &error) == LC_ERR_INVALID &&
        !failed);
  lc_error_cleanup(&error);
  changed.payload_bytes = 1;
  CHECK(ps_reserve_resize(owners[3], &changed, 0, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  changed = base;
  changed.maintenance = 1;
  CHECK(ps_reserve_open(s, &changed, 0, &failed, &error) == LC_ERR_INVALID &&
        !failed);
  lc_error_cleanup(&error);
  reserve_observe(s, &base, plans, owners, OWNERS);
  for (size_t i = 0; i < OWNERS; ++i) {
    size_t at = i * 53 % OWNERS;
    changed = plans[at];
    changed.payload_bytes = (i % 7) * PS_PAGE;
    changed.metadata_bytes = i * 13;
    changed.entries = i % 5;
    OK(ps_reserve_resize(owners[at], &changed, 0, &error));
    plans[at] = changed;
    reserve_observe(s, &base, plans, owners, OWNERS);
  }
  /* Shrink/removal cannot fail for lack of heap or capacity. */
  allocation_fail = 1;
  allocation_call = 0;
  memset(&changed, 0, sizeof(changed));
  for (size_t i = 0; i < OWNERS; ++i) {
    size_t at = i * 53 % OWNERS;
    OK(ps_reserve_resize(owners[at], &changed, 0, &error));
    ps_reserve_close(owners[at]);
    owners[at] = NULL;
    reserve_observe(s, &base, plans, owners, OWNERS);
  }
  CHECK(!allocation_call);
  allocation_fail = 0;
  OK(ps_close(s, &error));
  clean();
}
static void standing_capacity(int durable) {
  enum { EMPTY = 1024, BATCH = 32 };
  ps_budget_plan plan = {0}, ordinary, mandatory, small = {0};
  ps_reserve *owner = NULL;
  ps_budget *budget = NULL;
  ps_space *s;
  ps_writer *writers[BATCH] = {0};
  ps_reader *reader = NULL;
  ps_stats stats;
  unsigned char payload[PS_PAGE], readback[PS_PAGE];
  uint64_t next = 1;
  size_t got;
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  memset(payload, 0xb3, sizeof(payload));
  for (size_t i = 0; i < EMPTY; ++i)
    OK(ps_budget_plan_value(&plan, 0, &error));
  OK(ps_budget_plan_value(&plan, 3 * PS_PAGE, &error));
  OK(ps_reserve_open(s, &plan, 0, &owner, &error));
  budget_fill(s, &next, payload);
  OK(ps_checkpoint(s, &error));
  budget_fill(s, &next, payload);
  ordinary = plan;
  CHECK(ps_budget_open(s, &ordinary, &budget, &error) == LC_ERR_INVALID &&
        !budget);
  lc_error_cleanup(&error);
  mandatory = plan;
  mandatory.maintenance = 1;
  OK(ps_budget_open(s, &mandatory, &budget, &error));
  CHECK(ps_budget_foreground_check(s, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  /* Spend actual payload and descriptor/entry credits while peer checkpoints
   * repeatedly rewrite the growing metadata. Payload-only floors miss this
   * requirement even though all empty values consume zero body pages. */
  for (size_t first = 0; first < EMPTY; first += BATCH) {
    for (size_t i = 0; i < BATCH; ++i)
      OK(ps_begin(s, budget, key(PS_STATE, 77, first + i), &writers[i],
                  &error));
    OK(ps_revision_publish(writers, BATCH, 1, PS_COMMIT_DEFAULT, NULL, NULL,
                           &error));
    for (size_t i = 0; i < BATCH; ++i) {
      ps_writer_close(writers[i]);
      writers[i] = NULL;
    }
    OK(ps_checkpoint(s, &error));
  }
  OK(ps_begin(s, budget, key(PS_STATE, 77, EMPTY), &writers[0], &error));
  for (size_t i = 0; i < 3; ++i)
    OK(ps_write(writers[0], payload, sizeof(payload), &error));
  OK(ps_publish(writers[0], NULL, &error));
  ps_writer_close(writers[0]);
  OK(ps_budget_close(budget, &error));
  budget = NULL;
  OK(ps_checkpoint(s, &error));
  OK(ps_status(s, &stats, &error));
  CHECK(stats.capacity == PS_DEFAULT_CAPACITY && !stats.budgets &&
        !stats.reserved_payload_bytes && !stats.reserved_metadata_bytes &&
        !stats.reserved_entries);
  ps_reserve_close(owner);
  owner = NULL;
  OK(ps_close(s, &error));
  s = reopen(NULL);
  /* An interrupted owner can recover its requirement while that future floor
   * is still occupied. Ordinary admission stops, while funded mandatory work
   * and root checkpoints retain their existing completion guarantee. */
  CHECK(ps_reserve_open(s, &plan, 0, &owner, &error) == LC_ERR_INVALID &&
        !owner);
  lc_error_cleanup(&error);
  OK(ps_reserve_open(s, &plan, 1, &owner, &error));
  CHECK(ps_budget_foreground_check(s, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  small.maintenance = 1;
  OK(ps_budget_plan_value(&small, PS_PAGE, &error));
  OK(ps_budget_open(s, &small, &budget, &error));
  OK(ps_begin(s, budget, key(PS_STATE, 78, 1), &writers[0], &error));
  OK(ps_write(writers[0], payload, sizeof(payload), &error));
  OK(ps_publish(writers[0], NULL, &error));
  ps_writer_close(writers[0]);
  OK(ps_budget_close(budget, &error));
  OK(ps_checkpoint(s, &error));
  OK(ps_read_open(s, key(PS_STATE, 77, EMPTY), NULL, &reader, &error));
  for (size_t i = 0; i < 3; ++i) {
    OK(ps_read(reader, readback, sizeof(readback), &got, &error));
    CHECK(got == sizeof(readback) && !memcmp(payload, readback, got));
  }
  ps_reader_close(reader);
  for (size_t i = 0; i < EMPTY; ++i) {
    OK(ps_read_open(s, key(PS_STATE, 77, i), NULL, &reader, &error));
    CHECK(ps_read_size(reader) == 0);
    ps_reader_close(reader);
  }
  ps_reserve_close(owner);
  OK(ps_close(s, &error));
  clean();
}
typedef struct reserve_worker {
  ps_space *space;
  ps_reserve *owner;
  uint64_t id;
  int rc;
} reserve_worker;
static void *standing_worker(void *context) {
  reserve_worker *w = context;
  lc_error e = {0};
  ps_budget_plan plan = {0};
  ps_writer *writer = NULL;
  unsigned char bytes[64];
  memset(bytes, (int)w->id, sizeof(bytes));
  for (size_t i = 0; !w->rc && i < 64; ++i) {
    plan.payload_bytes = (1 + (w->id + i) % 16) * PS_PAGE;
    plan.metadata_bytes = i * 100;
    plan.entries = i % 20;
    w->rc = ps_reserve_resize(w->owner, &plan, 0, &e);
    if (!w->rc)
      w->rc = ps_begin(w->space, NULL, key(PS_STATE, w->id, i), &writer, &e);
    if (!w->rc)
      w->rc = ps_write(writer, bytes, sizeof(bytes), &e);
    if (!w->rc)
      w->rc = ps_publish(writer, NULL, &e);
    ps_writer_close(writer);
    writer = NULL;
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void standing_concurrency(int durable) {
  ps_space *s;
  reserve_worker workers[4] = {{0}};
  pthread_t threads[4];
  ps_budget_plan empty = {0};
  ps_reader *reader = NULL;
  unsigned char bytes[64];
  size_t got;
  clean();
  s = create(PS_DEFAULT_CAPACITY, durable, NULL);
  for (size_t i = 0; i < 4; ++i) {
    workers[i].space = s;
    workers[i].id = 81 + i;
    OK(ps_reserve_open(s, &empty, 0, &workers[i].owner, &error));
  }
  for (size_t i = 0; i < 4; ++i)
    CHECK(!pthread_create(&threads[i], NULL, standing_worker, &workers[i]));
  for (size_t i = 0; i < 4; ++i) {
    CHECK(!pthread_join(threads[i], NULL) && !workers[i].rc);
    for (size_t j = 0; j < 64; ++j) {
      OK(ps_read_open(s, key(PS_STATE, workers[i].id, j), NULL, &reader,
                      &error));
      OK(ps_read(reader, bytes, sizeof(bytes), &got, &error));
      CHECK(got == sizeof(bytes));
      for (size_t at = 0; at < got; ++at)
        CHECK(bytes[at] == workers[i].id);
      ps_reader_close(reader);
    }
    ps_reserve_close(workers[i].owner);
  }
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  clean();
}

static void obligation_entry(lc_pouch_state_entry *entry, char *name,
                             char *large, unsigned int i) {
  checkpoint_fixture(entry, name, i);
  if (!i)
    entry->key = large;
}
static void obligation_group(ps_namespace *ns, char *large, size_t keys) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry entry;
  ps_namespace_append *append = NULL;
  uint64_t bytes, total = 0;
  char name[32];
  for (size_t i = 0; i < keys; ++i) {
    obligation_entry(&entry, name, large, (unsigned int)i);
    OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &entry, 0,
                                              &bytes, &error));
    total += bytes;
  }
  OK(ps_namespace_append_begin(ns, 0, total, PS_APPEND_COMPLETE, NULL, &append,
                               &error));
  for (size_t i = 0; i < keys; ++i) {
    obligation_entry(&entry, name, large, (unsigned int)i);
    OK(lc_pouch_state_record_write_snapshot(
        &allocator, ps_namespace_append_output(append),
        ps_namespace_append_leaf(append), &entry, NULL, NULL, 0, &error));
  }
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
}
typedef struct standing_seen {
  const char *key;
  int found;
  size_t count;
} standing_seen;
static int standing_visit(void *context, uint64_t segment,
                          const lc_pouch_state_entry *entry, lc_error *e) {
  standing_seen *seen = context;
  (void)segment;
  (void)e;
  if (!entry->control) {
    CHECK(!strcmp(entry->key, seen->key) && entry->found == seen->found &&
          entry->version == (uint64_t)!seen->found);
    ++seen->count;
  }
  return LC_OK;
}
static void standing_deletion(int durable, size_t key_bytes) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *pin = NULL;
  ps_namespace_append *append = NULL;
  ps_namespace_status status;
  ps_namespace_repack_result result;
  lc_pouch_state_entry deletion = {0};
  lc_allocator allocator = {0};
  ps_stats before, after, retired;
  unsigned char filler[PS_PAGE];
  char *large = malloc(key_bytes + 1);
  uint64_t next = 1, bytes, reclaimed = 0;
  standing_seen seen;
  CHECK(large);
  memset(large, 'k', key_bytes);
  large[key_bytes] = 0;
  memset(filler, 0x7d, sizeof(filler));
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  obligation_group(ns, large, 1);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  CHECK(before.reserve_owners == 1 &&
        before.reserve_payload_bytes > 2 * key_bytes);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.reserve_owners == 1 &&
        after.reserve_payload_bytes == before.reserve_payload_bytes &&
        after.reserve_metadata_bytes == before.reserve_metadata_bytes &&
        after.reserve_entries == before.reserve_entries);
  budget_fill(ps_root_space(root), &next, filler);
  OK(ps_checkpoint(ps_root_space(root), &error));
  budget_fill(ps_root_space(root), &next, filler);
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
  OK(ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY, 1, &error));
  OK(ps_namespace_view_open(ns, &pin, &error));
  deletion.key = large;
  deletion.etag = "placeholder";
  deletion.record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
  deletion.seen = 1;
  deletion.index_seq = 2;
  deletion.version = 1;
  OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &deletion, 0,
                                            &bytes, &error));
  OK(ps_namespace_append_begin(ns, 0, bytes, PS_APPEND_MAINTENANCE, NULL,
                               &append, &error));
  OK(lc_pouch_state_record_write_snapshot(
      &allocator, ps_namespace_append_output(append),
      ps_namespace_append_leaf(append), &deletion, NULL, NULL, 0, &error));
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.reserve_payload_bytes == before.reserve_payload_bytes);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(!after.reserve_owners &&
        after.reserve_payload_bytes < before.reserve_payload_bytes);
  seen.key = large;
  seen.found = 1;
  seen.count = 0;
  OK(ps_namespace_view_visit(pin, 1, standing_visit, &seen, &error));
  CHECK(seen.count == 1);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, status.active_segment, 16,
                            NULL, NULL, &result, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  ps_namespace_view_close(pin);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  for (size_t pass = 0; pass < 64; ++pass) {
    OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, status.active_segment, 16,
                           NULL, NULL, &result, &error));
    reclaimed += result.replaced;
    if (!result.visited)
      break;
  }
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &retired, &error));
  CHECK(reclaimed && retired.allocated_bytes < before.allocated_bytes &&
        retired.capacity == PS_DEFAULT_CAPACITY);
  printf(
      "standing deletion sync%d key%zu: %llu mappings, %llu freed bytes\n",
      durable, key_bytes, (unsigned long long)reclaimed,
      (unsigned long long)(before.allocated_bytes - retired.allocated_bytes));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
  OK(ps_namespace_view_open(ns, &pin, &error));
  seen.found = 0;
  seen.count = 0;
  OK(ps_namespace_view_visit(pin, 1, standing_visit, &seen, &error));
  CHECK(seen.count == 1);
  ps_namespace_view_close(pin);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  free(large);
  clean();
}
static void root_obligations(int durable, int partial, int full) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *cold = NULL, *peer = NULL;
  ps_namespace_status info;
  ps_stats before, recovered, after;
  checkpoint_observation observation = {0};
  ps_checkpoint_observer observer = {checkpoint_prepare, checkpoint_finish,
                                     &observation};
  ps_values *values = NULL;
  ps_key k;
  ps_ref ref;
  uint64_t bytes, next = 100;
  char *large = malloc((size_t)PS_MIB + 1);
  unsigned char filler[PS_PAGE];
  size_t keys = partial ? 65 : 1;
  int selected_large = 0;
  CHECK(large);
  printf("root obligations sync%d partial%d full%d\n", durable, partial, full);
  memset(large, 'k', (size_t)PS_MIB);
  large[PS_MIB] = 0;
  memset(filler, 0x5b, sizeof(filler));
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &cold, &error));
  OK(ps_root_namespace_acquire(root, "peer", 1, &peer, &error));
  checkpoint_put(root, peer, "lease/0", 1, 1, 1, 1, 0);
  OK(ps_namespace_checkpoint_flush(peer, &error));
  obligation_group(cold, large, keys);
  if (partial) {
    OK(ps_namespace_checkpoint_observe(cold, &observer, &error));
    observation.reject = 2;
    CHECK(ps_namespace_checkpoint_flush(cold, &error) == LC_ERR_NOMEM);
    lc_error_cleanup(&error);
    CHECK(observation.published == 1);
    ps_namespace_info identity;
    OK(ps_namespace_identity(cold, &identity, &error));
    OK(ps_values_capture(ps_root_space(root), identity.owner, PS_KEY_METADATA,
                         PS_KEY_METADATA, &values, &error));
    CHECK(ps_values_count(values) == 64);
    for (size_t i = 0; i < ps_values_count(values); ++i) {
      OK(ps_values_info(values, i, &k, &ref, &bytes, &error));
      selected_large += bytes > PS_MIB;
    }
    CHECK(selected_large == 1);
    ps_values_close(values);
  }
  if (full)
    budget_fill(ps_root_space(root), &next, filler);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  CHECK(before.budgets == 1 && before.reserved_payload_bytes >= 2 * PS_PAGE);
  if (!partial)
    CHECK(before.reserved_payload_bytes > PS_MIB);
  ps_namespace_release(cold);
  ps_namespace_release(peer);
  cold = peer = NULL;
  OK(ps_root_close(root, &error));
  root = NULL;
  /* Root return is the admission barrier. No caller has acquired the cold
   * namespace after reopen, yet a peer must already be unable to spend its
   * completion capacity. This also covers forward key slots from an
   * interrupted checkpoint whose coverage control still names the old prefix.
   */
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_status(ps_root_space(root), &recovered, &error));
  CHECK(recovered.budgets == 1 &&
        recovered.reserved_payload_bytes == before.reserved_payload_bytes &&
        recovered.capacity == PS_DEFAULT_CAPACITY);
  budget_fill(ps_root_space(root), &next, filler);
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &cold, &error));
  OK(ps_namespace_status_get(cold, &info, &error));
  CHECK(info.pending_keys == (partial ? 1 : keys) &&
        info.checkpoint_keys == keys);
  OK(ps_namespace_checkpoint_flush(cold, &error));
  OK(ps_namespace_status_get(cold, &info, &error));
  CHECK(info.checkpoint_epoch && !info.pending_keys &&
        info.covered_segment == info.active_segment &&
        info.covered_offset == info.tail);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(!after.budgets && !after.reserved_payload_bytes &&
        !after.reserved_metadata_bytes && !after.reserved_entries &&
        after.capacity == PS_DEFAULT_CAPACITY);
  ps_namespace_release(cold);
  OK(ps_root_close(root, &error));
  free(large);
  clean();
}

static void root_obligation_faults(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_stats stats;
  unsigned char *baseline = malloc((size_t)PS_MIB);
  const char *names[] = {"first", "last"};
  size_t sites;
  CHECK(baseline);
  clean();
  options.storage.initial_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  for (size_t i = 0; i < 2; ++i) {
    OK(ps_root_namespace_acquire(root, names[i], 1, &ns, &error));
    checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
    ps_namespace_release(ns);
  }
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_root_close(root, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 3; ++kind) {
    sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      m.operations = m.reads = allocation_call = 0;
      m.fail_at = kind == 1 ? cut : 0;
      m.fail_read = kind == 2 ? cut : 0;
      m.tear = m.persist_torn = 1;
      allocation_fail = kind == 0 ? (cut ? cut : (size_t)-1) : 0;
      root = NULL;
      int rc = ps_root_open(path, 0, &options, &root, &error);
      allocation_fail = m.fail_at = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0   ? allocation_call
                : kind == 1 ? m.operations
                            : m.reads;
        CHECK(sites);
      } else {
        CHECK(!root && rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        lc_error_cleanup(&error);
        /* Failed bootstrap releases every partial owner and capacity claim,
         * including the file lock. A same-path retry needs no extra capacity
         * and retains both namespaces' canonical state. */
        OK(ps_root_open(path, 0, &options, &root, &error));
      }
      OK(ps_status(ps_root_space(root), &stats, &error));
      CHECK(stats.budgets == 2 && stats.reserved_payload_bytes == 4 * PS_PAGE);
      for (size_t i = 0; i < 2; ++i) {
        checkpoint_seen seen = {0};
        OK(ps_root_namespace_acquire(root, names[i], 0, &ns, &error));
        OK(ps_namespace_view_open(ns, &view, &error));
        OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
        CHECK(seen.count == 1 && seen.maximum == 1 && seen.first_version == 1);
        ps_namespace_view_close(view);
        ps_namespace_release(ns);
      }
      OK(ps_root_close(root, &error));
    }
    printf("root obligation bootstrap sync%d %s: %zu failure sites\n", durable,
           kind == 0   ? "allocation"
           : kind == 1 ? "write/sync"
                       : "read",
           sites);
  }
  free(baseline);
  free(m.stable);
  clean();
}

#endif
