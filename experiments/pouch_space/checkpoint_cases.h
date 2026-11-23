#ifndef POUCH_SPACE_CHECKPOINT_CASES_H
#define POUCH_SPACE_CHECKPOINT_CASES_H
#include "checkpoint.h"
#include "checkpoint_record.h"
#include "lc_pouch_storage.h"
#include "revision.h"

typedef struct checkpoint_probe {
  size_t calls;
  int reject;
} checkpoint_probe;
static int checkpoint_probe_call(checkpoint_probe *probe) {
  ++probe->calls;
  if (probe->reject)
    errno = EIO;
  return probe->reject;
}
static ssize_t checkpoint_probe_read(void *context, int fd, void *bytes,
                                     size_t length, off_t offset) {
  return checkpoint_probe_call(context) ? -1
                                        : pread(fd, bytes, length, offset);
}
static ssize_t checkpoint_probe_write(void *context, int fd, const void *bytes,
                                      size_t length, off_t offset) {
  return checkpoint_probe_call(context) ? -1
                                        : pwrite(fd, bytes, length, offset);
}
static int checkpoint_probe_sync(void *context, int fd) {
  return checkpoint_probe_call(context) ? -1 : fdatasync(fd);
}
static int checkpoint_probe_reserve(void *context, int fd, off_t offset,
                                     off_t length) {
  return checkpoint_probe_call(context) ? -1 : fallocate(fd, 0, offset, length);
}
static void checkpoint_unchanged(ps_space *space, checkpoint_probe *probe) {
  ps_stats before, after;
  uint64_t selected, durable, selected_after, durable_after;
  OK(ps_status(space, &before, &error));
  OK(ps_publication_prefix(space, &selected, &durable, &error));
  CHECK(selected == durable && !before.journal_bytes);
  size_t calls = probe->calls;
  for (size_t i = 0; i < 4; ++i) {
    /* An unchanged pass must succeed even when every allocation and I/O is
     * unavailable. In particular it must not poison an otherwise healthy root.
     */
    probe->reject = 1;
    allocation_call = 0;
    allocation_fail = 1;
    int rc = ps_checkpoint(space, &error);
    allocation_fail = 0;
    probe->reject = 0;
    OK(rc);
    CHECK(!allocation_call && probe->calls == calls);
    OK(ps_check(space, &error));
  }
  OK(ps_status(space, &after, &error));
  OK(ps_publication_prefix(space, &selected_after, &durable_after, &error));
  CHECK(after.checkpoints == before.checkpoints &&
        after.syncs == before.syncs && after.capacity == before.capacity &&
        after.metadata_pages == before.metadata_pages &&
        after.directory_page_writes == before.directory_page_writes &&
        after.descriptor_page_writes == before.descriptor_page_writes &&
        after.private_bytes == before.private_bytes &&
        after.reserved_payload_bytes == before.reserved_payload_bytes &&
        selected_after == selected && durable_after == durable);
}
static void checkpoint_idempotence(int durable) {
  checkpoint_probe probe = {0};
  ps_io io = {&probe, checkpoint_probe_read, checkpoint_probe_write,
              checkpoint_probe_sync, checkpoint_probe_reserve, NULL};
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *private = NULL;
  ps_reader *held = NULL;
  ps_stats before, after;
  unsigned char chunk[2048];
  uint64_t selected, completed;
  clean();
  ps_space *space = create(PS_DEFAULT_CAPACITY, durable, &io);
  checkpoint_unchanged(space, &probe);
  ps_ref prior = put(space, state(1), "prior", 5, 1);
  OK(ps_checkpoint(space, &error));
  checkpoint_unchanged(space, &probe);
  OK(ps_read_open(space, state(1), &prior, &held, &error));
  ps_ref current = put(space, state(1), "current", 7, 1);
  OK(ps_publication_prefix(space, &selected, &completed, &error));
  CHECK(durable ? selected == completed : selected > completed);

  /* A bounded private producer and its future capacity claim survive both a
   * real checkpoint and repeated unchanged passes without becoming selected. */
  OK(ps_budget_plan_value(&plan, 3 * PS_CHUNK, &error));
  OK(ps_budget_open(space, &plan, &budget, &error));
  OK(ps_begin(space, budget, state(2), &private, &error));
  memset(chunk, 0x6d, sizeof(chunk));
  for (size_t i = 0; i < 2 * PS_CHUNK / sizeof(chunk); ++i)
    OK(ps_write(private, chunk, sizeof(chunk), &error));
  OK(ps_status(space, &before, &error));
  CHECK(before.private_bytes && before.reserved_payload_bytes);
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.checkpoints == before.checkpoints + 1 &&
        after.syncs >= before.syncs + 3 && !after.journal_bytes &&
        after.private_bytes == before.private_bytes && after.retired_bytes);
  checkpoint_unchanged(space, &probe);
  read_value(held, "prior", 5);
  ps_reader_close(held);
  expect(space, state(1), &current, "current", 7);
  absent(space, state(2));
  ps_writer_close(private);
  OK(ps_budget_close(budget, &error));
  checkpoint_unchanged(space, &probe);

  OK(ps_status(space, &before, &error));
  OK(ps_grow(space, PS_MIB, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.capacity == PS_DEFAULT_CAPACITY + PS_MIB &&
        after.checkpoints == before.checkpoints + 1);
  checkpoint_unchanged(space, &probe);
  before = after;
  OK(ps_metadata_repack(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.checkpoints == before.checkpoints + 1 &&
        after.descriptor_page_writes > before.descriptor_page_writes);
  checkpoint_unchanged(space, &probe);
  OK(ps_close(space, &error));
  ps_options options = {0};
  options.io = &io;
  options.durable_sync = durable;
  OK(ps_open(path, 0, &options, &space, &error));
  OK(ps_status(space, &before, &error));
  CHECK(before.capacity == PS_DEFAULT_CAPACITY + PS_MIB &&
        !before.journal_bytes);
  checkpoint_unchanged(space, &probe);

  /* Recover a real journal suffix. Opening a clean root assigns incarnations
   * in the fixed grant records and leaves no journal work for a checkpoint. */
  ps_ref replayed = put(space, state(4), "replayed", 8, 1);
  OK(ps_flush(space, &error));
  OK(ps_close(space, &error));
  OK(ps_open(path, 0, &options, &space, &error));
  OK(ps_status(space, &before, &error));
  CHECK(before.capacity == PS_DEFAULT_CAPACITY + PS_MIB &&
        before.journal_bytes);
  expect(space, state(1), &current, "current", 7);
  expect(space, state(4), &replayed, "replayed", 8);
  absent(space, state(2));
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &after, &error));
  CHECK(after.checkpoints == before.checkpoints + 1);
  checkpoint_unchanged(space, &probe);

  /* New metadata still requires actual durable publication. Failure poisons
   * the root; a subsequent checkpoint must not hide that uncertain outcome. */
  put(space, state(3), "changed", 7, 1);
  probe.reject = 1;
  CHECK(ps_checkpoint(space, &error) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  size_t calls = probe.calls;
  CHECK(ps_checkpoint(space, &error) == LC_ERR_TRANSPORT);
  CHECK(probe.calls == calls);
  lc_error_cleanup(&error);
  probe.reject = 0;
  OK(ps_close(space, &error));
  OK(ps_open(path, 0, &options, &space, &error));
  expect(space, state(1), &current, "current", 7);
  expect(space, state(3), NULL, "changed", 7);
  absent(space, state(2));
  OK(ps_close(space, &error));
  clean();
}

typedef struct checkpoint_walk {
  const char *leaf;
  lc_pouch_state_entry metadata, decision;
} checkpoint_walk;
static int checkpoint_walk_decode(void *context, ps_key key, ps_reader *reader,
                                  lc_error *e) {
  checkpoint_walk *walk = context;
  return ps_checkpoint_record_read(
      reader, NULL, walk->leaf,
      key.kind == PS_KEY_METADATA ? &walk->metadata : &walk->decision, e);
}
static void checkpoint_terminal_fields(ps_checkpoint_record *record,
                                       const char *leaf) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry decoded = {0};
  lc_pouch_state_terminal view;
  OK(lc_pouch_state_record_decode_span(record->bytes, record->length,
                                       &allocator, leaf, &decoded, &error));
  allocation_call = 0;
  allocation_fail = (size_t)-1;
  OK(lc_pouch_state_record_terminal_span(record->bytes, record->length, &view,
                                         &error));
  allocation_fail = 0;
  CHECK(!allocation_call && view.type == decoded.record_type &&
        view.key_bytes == strlen(decoded.key) &&
        !memcmp(view.key, decoded.key, view.key_bytes) &&
        view.etag_bytes == strlen(decoded.etag) &&
        !memcmp(view.etag, decoded.etag, view.etag_bytes) &&
        view.version == decoded.version &&
        view.index_seq == decoded.index_seq &&
        view.lifetime == decoded.lifetime &&
        view.has_query_index_seq == decoded.has_query_index_seq &&
        view.query_index_seq == decoded.query_index_seq &&
        view.updated_at_unix == decoded.updated_at_unix &&
        (view.decision ? !strcmp(view.decision, decoded.decision)
                       : !decoded.decision));
  CHECK(view.key_bytes && view.etag_bytes);
  size_t text[] = {(size_t)(view.key - record->bytes),
                   (size_t)(view.etag - record->bytes)};
  for (size_t i = 0; i < sizeof(text) / sizeof(text[0]); ++i) {
    unsigned char original = record->bytes[text[i]];
    record->bytes[text[i]] = 0;
    CHECK(lc_pouch_state_record_terminal_span(
              record->bytes, record->length, &view, &error) == LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
    CHECK(lc_pouch_state_record_decode_span(record->bytes, record->length,
                                            &allocator, leaf, &decoded,
                                            &error) == LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
    record->bytes[text[i]] = original;
  }
  CHECK(lc_pouch_state_record_terminal_span(record->bytes, record->length - 1,
                                            &view, &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  lc_pouch_state_entry_cleanup(&allocator, &decoded);
}
static void checkpoint_records(int durable) {
  lc_allocator allocator = {0};
  ps_options options = {.durable_sync = durable, .shared = 1};
  lc_pouch_state_entry prior = {0}, changed = {0}, projected = {0},
                       decoded = {0};
  ps_checkpoint_record metadata = {0}, decision = {0};
  ps_writer *writers[2] = {NULL, NULL};
  ps_budget_plan plan = {0}, grown, observed;
  ps_budget *budget = NULL;
  ps_values *captured = NULL;
  ps_reader *reader = NULL;
  ps_key slot = key(PS_KEY_METADATA, 9, 1);
  ps_key control = key(PS_KEY_DECISION, 9, 2);
  ps_space *s;
  ps_ref ref;
  uint64_t bytes;
  size_t at, metadata_at = (size_t)-1;
  const char *leaf = "snapshot-00000000000000000001.log";
  checkpoint_walk walk = {0};
  int accepted;
  clean();
  OK(ps_open(path, 1, &options, &s, &error));
  prior.key = "lease/value";
  prior.record_type = LC_POUCH_STATE_RECORD_STATE_META;
  prior.seen = prior.found = 1;
  prior.index_seq = 1;
  CHECK(lc_pouch_state_entry_project(NULL, &prior, &projected));
  CHECK(!projected.query_index_seq && !projected.lifetime);
  prior = projected;
  changed = prior;
  changed.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  changed.has_query_index_seq = 0;
  changed.index_seq = changed.lifetime = 3;
  changed.version = 1;
  changed.content_type = "application/octet-stream";
  changed.bytes = changed.cipher_bytes = UINT64_C(1) << 40;
  changed.etag = "body-etag";
  changed.payload_span.present = 1;
  changed.payload_span.container_leaf = "body-00000000000000000001.log";
  changed.payload_span.payload_offset = 128;
  changed.payload_span.payload_length = changed.cipher_bytes;
  changed.payload_span.payload_crc = 123;
  CHECK(lc_pouch_state_entry_project(&prior, &changed, &projected));
  CHECK(projected.query_index_seq == 3 && projected.lifetime == 3);
  prior = projected;
  changed = prior;
  changed.record_type = LC_POUCH_STATE_RECORD_STATE_META;
  changed.has_query_index_seq = 0;
  changed.index_seq = 4;
  CHECK(lc_pouch_state_entry_project(&prior, &changed, &projected));
  CHECK(projected.query_index_seq == 3 && projected.version == 1);
  changed.index_seq = 2;
  CHECK(!lc_pouch_state_entry_project(&prior, &changed, &decoded));
  CHECK(!decoded.seen);
  OK(ps_checkpoint_record_encode(&allocator, &projected, &metadata, &error));
  CHECK(metadata.length < PS_PAGE); /* No application-body materialization. */
  lc_pouch_state_terminal prefix = {0};
  OK(lc_pouch_state_record_terminal_span(metadata.bytes, metadata.length,
                                         &prefix, &error));
  CHECK(!prefix.type && prefix.key_bytes == strlen(prior.key) &&
        !memcmp(prefix.key, prior.key, prefix.key_bytes));
  ps_checkpoint_record high_water = {0};
  lc_pouch_state_entry high = {0};
  high.control = high.seen = 1;
  high.record_type = LC_POUCH_STATE_RECORD_HIGH_WATER;
  high.index_seq = 12;
  high.query_index_seq = 3;
  OK(ps_checkpoint_record_encode(&allocator, &high, &high_water, &error));
  ref =
      put(s, state(99), high_water.bytes, high_water.length, high_water.length);
  OK(ps_read_open(s, state(99), &ref, &reader, &error));
  OK(ps_checkpoint_record_read(reader, &allocator, leaf, &decoded, &error));
  CHECK(decoded.control && !decoded.decision && decoded.index_seq == 12 &&
        decoded.query_index_seq == 3);
  lc_pouch_state_entry_cleanup(&allocator, &decoded);
  ps_reader_close(reader);
  reader = NULL;
  ps_checkpoint_record_close(&high_water);
  lc_pouch_state_entry note = {0};
  note.key = ".staging/transaction/key";
  note.etag = "decision-etag";
  note.decision = "committed";
  note.control = note.seen = 1;
  note.record_type = LC_POUCH_STATE_RECORD_DECISION;
  note.version = 2;
  note.index_seq = 9;
  OK(ps_checkpoint_record_encode(&allocator, &note, &decision, &error));
  checkpoint_terminal_fields(&decision, leaf);
  ps_checkpoint_record tombstone = {0};
  lc_pouch_state_entry removed = {0};
  removed.key = note.key;
  removed.etag = note.etag;
  removed.seen = 1;
  removed.version = 3;
  removed.index_seq = 10;
  removed.lifetime = 2;
  removed.updated_at_unix = 509;
  removed.record_type = LC_POUCH_STATE_RECORD_OBJECT_DELETE;
  removed.has_query_index_seq = 1;
  removed.query_index_seq = 7;
  OK(ps_checkpoint_record_encode(&allocator, &removed, &tombstone, &error));
  checkpoint_terminal_fields(&tombstone, leaf);
  ps_checkpoint_record_close(&tombstone);
  OK(ps_budget_plan_key_slot(&plan, slot.kind, metadata.length, 0, &error));
  OK(ps_budget_plan_value(&plan, decision.length, &error));
  OK(ps_budget_open(s, &plan, &budget, &error));
  OK(ps_begin(s, budget, slot, &writers[0], &error));
  grown = plan;
  OK(ps_budget_plan_value(&grown, PS_PAGE, &error));
  OK(ps_budget_resize(budget, &grown, &error));
  CHECK(ps_budget_resize(budget, &plan, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  ps_writer_close(writers[0]);
  writers[0] = NULL;
  OK(ps_budget_resize(budget, &plan, &error));
  allocation_call = 0;
  allocation_fail = 1;
  CHECK(ps_budget_resize(budget, &grown, &error) == LC_ERR_NOMEM);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  OK(ps_budget_status(budget, &observed, &error));
  CHECK(observed.payload_bytes == plan.payload_bytes &&
        observed.metadata_bytes == plan.metadata_bytes &&
        observed.entries == plan.entries);
  OK(ps_checkpoint_record_write(s, budget, slot, &metadata, &writers[0],
                                &error));
  OK(ps_checkpoint_record_write(s, budget, control, &decision, &writers[1],
                                &error));
  OK(ps_revision_publish(writers, 2, 1, PS_COMMIT_DEFAULT, NULL, NULL, &error));
  for (at = 0; at < 2; ++at) {
    ps_writer_close(writers[at]);
    writers[at] = NULL;
  }
  OK(ps_budget_close(budget, &error));
  budget = NULL;
  OK(ps_checkpoint(s, &error));
  OK(ps_values_capture(s, 9, PS_KEY_METADATA, PS_KEY_DECISION, &captured,
                       &error));
  CHECK(ps_values_count(captured) == 2);
  walk.leaf = leaf;
  OK(ps_internal_values_startup_walk(s, 9, checkpoint_walk_decode, &walk,
                                     &accepted, &error));
  CHECK(accepted);
  /* The walk has already discarded its page cache. Decoded fields are owned. */
  CHECK(!strcmp(walk.metadata.key, prior.key) &&
        walk.metadata.payload_span.payload_offset == 128 &&
        !strcmp(walk.decision.key, note.key) &&
        !strcmp(walk.decision.decision, note.decision));
  CHECK(lc_pouch_state_record_decode_span(metadata.bytes, metadata.length - 1,
                                          &allocator, leaf, &walk.metadata,
                                          &error) == LC_ERR_PROTOCOL);
  lc_error_cleanup(&error);
  CHECK(!strcmp(walk.metadata.key, prior.key));
  const ps_entry *packed = ps_internal_lookup(s, slot);
  unsigned char first, changed_byte;
  CHECK(packed && packed->count == 1 &&
        packed->extents[0].flags == EXTENT_PACKED);
  off_t offset = (off_t)packed->extents[0].offset;
  int checked = packed->value_checked;
  CHECK(pread(s->fd, &first, 1, offset) == 1);
  changed_byte = (unsigned char)(first ^ 1U);
  CHECK(pwrite(s->fd, &changed_byte, 1, offset) == 1);
  CHECK(ps_internal_values_startup_walk(s, 9, checkpoint_walk_decode, &walk,
                                        &accepted, &error) == LC_ERR_PROTOCOL);
  CHECK(accepted && packed->value_checked == checked && error.message &&
        !strcmp(error.message, "captured value payload checksum mismatch") &&
        !strcmp(walk.metadata.key, prior.key));
  CHECK(pwrite(s->fd, &first, 1, offset) == 1);
  lc_error_cleanup(&error);
  lc_pouch_state_entry_cleanup(&allocator, &walk.metadata);
  lc_pouch_state_entry_cleanup(&allocator, &walk.decision);
  CHECK(ps_close(s, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  for (at = 0; at < 2; ++at) {
    ps_key observed_key;
    OK(ps_values_info(captured, at, &observed_key, &ref, &bytes, &error));
    OK(ps_values_read_open(captured, at, &reader, &error));
    OK(ps_checkpoint_record_read(reader, &allocator, leaf, &decoded, &error));
    if (observed_key.kind == PS_KEY_METADATA) {
      metadata_at = at;
      CHECK(!strcmp(decoded.key, prior.key) && decoded.index_seq == 4 &&
            decoded.query_index_seq == 3 && decoded.lifetime == 3 &&
            decoded.cipher_bytes == (UINT64_C(1) << 40) &&
            decoded.payload_span.payload_offset == 128 &&
            decoded.payload_span.payload_crc == 123);
    } else {
      CHECK(decoded.control && decoded.decision &&
            !strcmp(decoded.decision, "committed") && decoded.index_seq == 9 &&
            !strcmp(decoded.etag, note.etag));
    }
    lc_pouch_state_entry_cleanup(&allocator, &decoded);
    ps_reader_close(reader);
    reader = NULL;
  }
  CHECK(metadata_at != (size_t)-1);
  put(s, slot, "replacement", 11, 3);
  OK(ps_checkpoint(s, &error));
  OK(ps_values_read_open(captured, metadata_at, &reader, &error));
  ps_values_close(captured);
  captured = NULL;
  OK(ps_checkpoint_record_read(reader, &allocator, leaf, &decoded, &error));
  CHECK(decoded.index_seq == 4 && decoded.query_index_seq == 3);
  lc_pouch_state_entry_cleanup(&allocator, &decoded);
  CHECK(ps_close(s, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  ps_reader_close(reader);
  ps_checkpoint_record_close(&metadata);
  ps_checkpoint_record_close(&decision);
  OK(ps_close(s, &error));
  clean();
}
typedef struct checkpoint_observation {
  size_t prepared, finished, published, reject;
} checkpoint_observation;
static int checkpoint_prepare(void *context, const char *name,
                              const ps_checkpoint_ref *refs, size_t count,
                              void **plan, lc_error *e) {
  checkpoint_observation *observation = context;
  CHECK(!strcmp(name, "checkpoint") && count && refs[0].key && refs[0].leaf);
  ++observation->prepared;
  if (observation->prepared == observation->reject)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "checkpoint observer interrupted",
                        NULL, NULL, "test");
  *plan = observation;
  return LC_OK;
}
static void checkpoint_finish(void *context, void *plan, int published) {
  checkpoint_observation *observation = context;
  CHECK(plan == context);
  ++observation->finished;
  observation->published += published != 0;
}
static void checkpoint_fixture(lc_pouch_state_entry *entry, char *name,
                               unsigned int i) {
  memset(entry, 0, sizeof(*entry));
  CHECK(snprintf(name, 32, "lease/%u", i) > 0);
  entry->key = name;
  entry->content_type = "application/octet-stream";
  entry->etag = "placeholder";
  entry->record_type = LC_POUCH_STATE_RECORD_STATE_META;
  entry->seen = entry->found = 1;
  entry->index_seq = i + 1;
}
static ps_checkpoint_delta *checkpoint_stage_fixture(ps_key_checkpoint *c) {
  ps_checkpoint_delta *replay = NULL;
  lc_pouch_state_entry entry;
  char name[32];
  OK(ps_checkpoint_delta_open(c, 0, &replay, &error));
  for (unsigned int i = 0; i < 130; ++i) {
    checkpoint_fixture(&entry, name, i);
    OK(ps_checkpoint_delta_replay(replay, &entry, &error));
  }
  checkpoint_fixture(&entry, name, 129);
  entry.index_seq = 1;
  OK(ps_checkpoint_delta_replay(replay, &entry, &error));
  return replay;
}
static void checkpoint_replay_one(ps_key_checkpoint *c,
                                  const lc_pouch_state_entry *entry,
                                  uint64_t segment, uint64_t end) {
  ps_checkpoint_delta *replay = NULL;
  OK(ps_checkpoint_delta_open(c, 0, &replay, &error));
  if (entry)
    OK(ps_checkpoint_delta_replay(replay, entry, &error));
  OK(ps_checkpoint_delta_replay_commit(replay, segment, end, end, &error));
  ps_checkpoint_delta_close(replay);
}
static void checkpoint_owner(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_key_checkpoint *checkpoint = NULL, *seed = NULL;
  ps_checkpoint_delta *replay;
  ps_stats before, after;
  ps_checkpoint_info info;
  checkpoint_observation observation = {0};
  ps_checkpoint_observer observer = {checkpoint_prepare, checkpoint_finish,
                                     &observation};
  lc_pouch_state_entry entry = {0}, decoded = {0};
  lc_allocator allocator = {0};
  ps_values *values = NULL;
  ps_reader *reader = NULL;
  ps_namespace_info identity;
  char name[32];
  size_t i;
  uint64_t offset;
  int resumed;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  OK(ps_namespace_identity(ns, &identity, &error));
  OK(ps_key_checkpoint_open(ns, &checkpoint, &error));
  OK(ps_key_checkpoint_open(ns, &seed, &error));
  OK(ps_key_checkpoint_observe(checkpoint, &observer, &error));
  /* More than one revision of independent logical keys. Metadata-only lease
   * placeholders must remain query-neutral, including after partial restart. */
  OK(ps_status(ps_root_space(root), &before, &error));
  replay = checkpoint_stage_fixture(checkpoint);
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(!info.slots && !info.pending && !info.epoch);
  allocation_call = 0;
  allocation_fail = 1;
  CHECK(ps_checkpoint_delta_replay_commit(replay, 1, 9000, 9000, &error) ==
        LC_ERR_NOMEM);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  ps_checkpoint_delta_close(replay);
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(!info.slots && !info.pending && !info.epoch);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.reserved_payload_bytes == before.reserved_payload_bytes &&
        after.reserved_metadata_bytes == before.reserved_metadata_bytes &&
        after.reserved_entries == before.reserved_entries);
  checkpoint_fixture(&entry, name, 129);
  checkpoint_replay_one(seed, &entry, 1, 100);
  replay = checkpoint_stage_fixture(checkpoint);
  OK(ps_checkpoint_delta_replay_commit(replay, 1, 9000, 9000, &error));
  ps_checkpoint_delta_close(replay);
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(info.slots == 130 && info.pending == 130 && !info.epoch);
  CHECK(ps_key_checkpoint_due(checkpoint));
  observation.reject = 2;
  CHECK(ps_key_checkpoint_flush(checkpoint, &error) == LC_ERR_NOMEM);
  lc_error_cleanup(&error);
  CHECK(observation.published == 1);
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(!info.epoch && info.pending && info.pending < 130);
  OK(ps_key_checkpoint_capture(seed, PS_CHECKPOINT_CAPTURE_SEED, &values, &info,
                               &error));
  CHECK(!info.control && ps_values_count(values));
  CHECK(ps_key_checkpoint_confirm_seed(seed, values, &info, &error) ==
        LC_ERR_PENDING);
  lc_error_cleanup(&error);
  ps_values_close(values);
  values = NULL;
  OK(ps_key_checkpoint_capture(seed, PS_CHECKPOINT_CAPTURE_CHANGED, &values,
                               &info, &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  allocation_call = 0;
  allocation_fail = 23;
  CHECK(ps_key_checkpoint_adopt(seed, values, &info, 1, 9000, &resumed, &offset,
                                &error) == LC_ERR_NOMEM);
  allocation_fail = 0;
  lc_error_cleanup(&error);
  ps_key_checkpoint_info(seed, &info);
  CHECK(!info.control && info.slots == 1 && info.pending == 1 && !resumed);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(after.reserved_payload_bytes == before.reserved_payload_bytes &&
        after.reserved_metadata_bytes == before.reserved_metadata_bytes &&
        after.reserved_entries == before.reserved_entries);
  OK(ps_key_checkpoint_adopt(seed, values, &info, 1, 9000, &resumed, &offset,
                             &error));
  CHECK(!resumed);
  OK(ps_key_checkpoint_confirm_seed(seed, values, &info, &error));
  ps_key_checkpoint_info(seed, &info);
  CHECK(info.slots == ps_values_count(values) && !info.control &&
        !info.pending);
  ps_values_close(values);
  values = NULL;
  ps_key_checkpoint_close(seed);
  ps_key_checkpoint_close(checkpoint);
  checkpoint = NULL;
  ps_namespace_release(ns);
  ns = NULL;
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
  OK(ps_key_checkpoint_open(ns, &checkpoint, &error));
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(!info.epoch && info.slots && info.slots < 130);
  /* The unchanged coverage requires all original canonical records to replay.
   * Already selected forward metadata must not change supersession outcomes. */
  replay = checkpoint_stage_fixture(checkpoint);
  OK(ps_checkpoint_delta_replay_commit(replay, 1, 9000, 9000, &error));
  ps_checkpoint_delta_close(replay);
  OK(ps_key_checkpoint_flush(checkpoint, &error));
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(info.epoch && info.segment == 1 && info.offset == 9000 &&
        info.state_index == 130 && !info.query_index && !info.pending &&
        info.slots == 130);
  OK(ps_values_capture(ps_root_space(root), identity.owner, PS_KEY_METADATA,
                       PS_KEY_DECISION, &values, &error));
  CHECK(ps_values_count(values) == 130);
  for (i = 0; i < ps_values_count(values); ++i) {
    OK(ps_values_read_open(values, i, &reader, &error));
    OK(ps_checkpoint_record_read(reader, &allocator,
                                 "snapshot-00000000000000000001.log", &decoded,
                                 &error));
    CHECK(decoded.seen && decoded.found && !decoded.payload_span.present &&
          !decoded.lifetime && !decoded.query_index_seq &&
          decoded.index_seq >= 1 && decoded.index_seq <= 130);
    if (!strcmp(decoded.key, "lease/129"))
      CHECK(decoded.index_seq == 130);
    ps_reader_close(reader);
    reader = NULL;
    lc_pouch_state_entry_cleanup(&allocator, &decoded);
  }
  ps_values_close(values);
  values = NULL;
  /* A rejected/stale replay must not resurrect an old payload generation. */
  OK(ps_key_checkpoint_open(ns, &seed, &error));
  checkpoint_fixture(&entry, name, 129);
  entry.index_seq = 1;
  checkpoint_replay_one(checkpoint, &entry, 2, 100);
  OK(ps_key_checkpoint_flush(checkpoint, &error));
  ps_key_checkpoint_info(checkpoint, &info);
  CHECK(info.state_index == 130 && info.segment == 2 && info.offset == 100 &&
        info.slots == 130 && !info.pending);
  /* Coverage advances without changing any slot. Their existing ledger and
   * reservations already suffice, even if no allocations are available. */
  OK(ps_key_checkpoint_capture(seed, PS_CHECKPOINT_CAPTURE_CHANGED, &values,
                               &info, &error));
  allocation_call = 0;
  allocation_fail = 1;
  OK(ps_key_checkpoint_adopt(seed, values, &info, 0, 0, &resumed, &offset,
                             &error));
  allocation_fail = 0;
  CHECK(!allocation_call && !resumed);
  OK(ps_key_checkpoint_confirm_seed(seed, values, &info, &error));
  ps_key_checkpoint_info(seed, &info);
  CHECK(info.slots == 130 && info.segment == 2 && info.offset == 100 &&
        info.state_index == 130 && !info.pending);
  ps_values_close(values);
  ps_key_checkpoint_close(seed);
  ps_key_checkpoint_close(checkpoint);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
typedef struct checkpoint_seen {
  size_t count, first_metadata;
  uint64_t maximum, first_version;
} checkpoint_seen;
static int checkpoint_visit(void *context, uint64_t segment,
                            const lc_pouch_state_entry *entry, lc_error *e) {
  checkpoint_seen *seen = context;
  (void)segment;
  (void)e;
  if (entry->control)
    return LC_OK;
  CHECK(entry->found && entry->payload_span.present && entry->lifetime &&
        entry->bytes == 4);
  ++seen->count;
  if (entry->index_seq > seen->maximum)
    seen->maximum = entry->index_seq;
  if (!strcmp(entry->key, "lease/0")) {
    seen->first_version = entry->version;
    seen->first_metadata = entry->metadata_length;
    for (size_t i = 0; i < entry->metadata_length; ++i)
      CHECK(entry->metadata[i] == 0x7c);
  }
  return LC_OK;
}
static void checkpoint_put_with_metadata(ps_root *root, ps_namespace *ns,
                                         const char *key_name, uint64_t index,
                                         uint64_t version, uint64_t lifetime,
                                         int commit, uint64_t target,
                                         unsigned char *metadata,
                                         size_t metadata_bytes) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry request = {0}, result = {0};
  ps_namespace_append *append = NULL;
  lc_source *source = NULL;
  request.key = (char *)key_name;
  request.content_type = "application/test";
  request.payload_context = "checkpoint";
  request.metadata = metadata;
  request.metadata_length = metadata_bytes;
  request.index_seq = index;
  request.version = version;
  request.lifetime = lifetime;
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  OK(lc_source_from_memory("data", 4, &source, &error));
  OK(ps_namespace_append_begin(ns, target, 0, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  OK(lc_pouch_state_record_append(
      &allocator, ps_root_crypto(root), ps_namespace_append_output(append),
      ps_namespace_append_leaf(append), &request, source, 0, &result, &error));
  if (commit)
    OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
  source->close(source);
  lc_pouch_state_entry_cleanup(&allocator, &result);
}
static void checkpoint_put(ps_root *root, ps_namespace *ns,
                           const char *key_name, uint64_t index,
                           uint64_t version, uint64_t lifetime, int commit,
                           uint64_t target) {
  checkpoint_put_with_metadata(root, ns, key_name, index, version, lifetime,
                               commit, target, NULL, 0);
}
static void checkpoint_namespace(int durable, int shared) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_status status;
  checkpoint_seen seen = {0};
  char name[32];
  unsigned char metadata[PS_PAGE - 512];
  size_t i;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.shared = shared;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  for (i = 0; i < 70; ++i) {
    CHECK(snprintf(name, sizeof(name), "lease/%u", (unsigned int)i) > 0);
    checkpoint_put(root, ns, name, i + 1, 1, i + 1, 1, 0);
  }
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(status.checkpoint_epoch && status.checkpoint_keys == 70 &&
        status.pending_keys == 6 && status.covered_offset < status.tail);
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
  CHECK(seen.count == 70 && seen.maximum == 70 && seen.first_version == 1);
  checkpoint_put(root, ns, "aborted", 71, 1, 71, 0, 0);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(status.checkpoint_keys == 70 && status.pending_keys == 6);
  memset(metadata, 0x7c, sizeof(metadata));
  checkpoint_put_with_metadata(root, ns, "lease/0", 71, 2, 1, 1, 1,
                               shared ? metadata : NULL,
                               shared ? sizeof(metadata) : 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  if (shared) {
    const ps_entry *p;
    int packed = 0, large = 0;
    for (p = ps_root_space(root)->entries; p; p = p->all_next) {
      if (p->key.kind != PS_KEY_METADATA || p->state != 1 || p->deleted)
        continue;
      packed |= p->count == 1 && p->extents[0].flags == EXTENT_PACKED;
      large |= p->bytes > sizeof(metadata) && p->bytes < PS_PAGE;
    }
    CHECK(packed && large);
  }
  CHECK(ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 1) == 69);
  CHECK(ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 2) == 1);
  memset(&seen, 0, sizeof(seen));
  OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
  CHECK(seen.count == 70 && seen.maximum == 70 && seen.first_version == 1);
  ps_namespace_view_close(view);
  view = NULL;
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
  OK(ps_namespace_view_open(ns, &view, &error));
  memset(&seen, 0, sizeof(seen));
  OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
  CHECK(seen.count == 70 && seen.maximum == 71 && seen.first_version == 2);
  CHECK(seen.first_metadata == (shared ? sizeof(metadata) : 0));
  ps_namespace_view_close(view);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  CHECK(ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 1) == 69);
  CHECK(ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 2) == 1);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
#define CHECKPOINT_FAULT_KEYS 65U
static void checkpoint_group_entry(lc_pouch_state_entry *entry, char *name,
                                   size_t ordinal, size_t keys) {
  checkpoint_fixture(entry, name, (unsigned int)(ordinal % keys));
  entry->index_seq = ordinal + 1;
  entry->has_query_index_seq = 1;
  if (ordinal >= keys) {
    entry->found = 0;
    entry->version = 1;
    entry->record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
  }
}
static void checkpoint_group(ps_namespace *ns, size_t keys) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry entry;
  ps_namespace_append *append = NULL;
  uint64_t bytes, total = 0;
  size_t i;
  char name[32];
  for (i = 0; i < 2 * keys; ++i) {
    checkpoint_group_entry(&entry, name, i, keys);
    OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &entry, 0,
                                              &bytes, &error));
    total += bytes;
  }
  OK(ps_namespace_append_begin(ns, 0, total, PS_APPEND_COMPLETE, NULL, &append,
                               &error));
  for (i = 0; i < 2 * keys; ++i) {
    checkpoint_group_entry(&entry, name, i, keys);
    OK(lc_pouch_state_record_write_snapshot(
        &allocator, ps_namespace_append_output(append),
        ps_namespace_append_leaf(append), &entry, NULL, NULL, 0, &error));
  }
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
}
typedef struct checkpoint_group_state {
  uint64_t index[CHECKPOINT_FAULT_KEYS];
  int found[CHECKPOINT_FAULT_KEYS];
} checkpoint_group_state;
static int checkpoint_group_visit(void *context, uint64_t segment,
                                  const lc_pouch_state_entry *entry,
                                  lc_error *e) {
  checkpoint_group_state *state = context;
  lc_pouch_state_entry prior = {0}, projected;
  unsigned int i;
  char trailing;
  (void)segment;
  (void)e;
  if (entry->control)
    return LC_OK;
  CHECK(sscanf(entry->key, "lease/%u%c", &i, &trailing) == 1 &&
        i < CHECKPOINT_FAULT_KEYS);
  prior.key = entry->key;
  prior.seen = state->index[i] != 0;
  prior.index_seq = state->index[i];
  prior.version = prior.seen && !state->found[i];
  prior.record_type = state->found[i] ? LC_POUCH_STATE_RECORD_STATE_META
                                      : LC_POUCH_STATE_RECORD_STATE_DELETE;
  prior.found = state->found[i];
  if (lc_pouch_state_entry_project(&prior, entry, &projected)) {
    state->index[i] = projected.index_seq;
    state->found[i] = projected.found;
    CHECK(!projected.query_index_seq);
  }
  return LC_OK;
}
/* The physical test model normally caps fixtures at 4 MiB. This surface uses
 * the mandated 10 MiB root; its larger stable image is still a test fixture,
 * never application buffering or a substitute for streamed product I/O. */
static int checkpoint_model_sync(void *context, int fd) {
  model *m = context;
  struct stat st;
  ++m->syncs;
  if (fail(m)) {
    errno = EIO;
    return -1;
  }
  CHECK(fdatasync(fd) == 0);
  CHECK(fstat(fd, &st) == 0 && st.st_size <= (off_t)PS_DEFAULT_CAPACITY);
  m->stable_bytes = (size_t)st.st_size;
  CHECK(pread(fd, m->stable, m->stable_bytes, 0) == (ssize_t)m->stable_bytes);
  return 0;
}
static void checkpoint_faults(int durable, int heap) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_status status;
  checkpoint_group_state state;
  size_t cut, operations = 0, i;
  int rc;
  free(m.stable);
  m.stable = malloc((size_t)PS_DEFAULT_CAPACITY);
  CHECK(m.stable);
  io.sync = checkpoint_model_sync;
  options.storage.initial_capacity = options.storage.maximum_capacity =
      PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  for (cut = 0; cut == 0 || cut <= operations; ++cut) {
    clean();
    m.fail_at = m.operations = 0;
    OK(ps_root_open(path, 1, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
    checkpoint_group(ns, CHECKPOINT_FAULT_KEYS);
    OK(ps_flush(ps_root_space(root), &error));
    m.operations = allocation_call = 0;
    m.tear = m.persist_torn = 1;
    if (heap)
      allocation_fail = cut ? cut : (size_t)-1;
    else
      m.fail_at = cut;
    rc = ps_namespace_checkpoint_flush(ns, &error);
    if (!cut)
      operations = heap ? allocation_call : m.operations;
    m.fail_at = allocation_fail = 0;
    CHECK(!cut || heap || rc != LC_OK);
    CHECK(cut || rc == LC_OK);
    if (heap && rc != LC_OK)
      CHECK(rc == LC_ERR_NOMEM);
    lc_error_cleanup(&error);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    root = NULL;
    crash(&m);
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
    OK(ps_namespace_status_get(ns, &status, &error));
    CHECK(!status.checkpoint_epoch || (status.covered_segment == 1 &&
                                       status.covered_offset == status.tail));
    memset(&state, 0, sizeof(state));
    OK(ps_namespace_view_open(ns, &view, &error));
    OK(ps_namespace_view_visit(view, 1, checkpoint_group_visit, &state,
                               &error));
    ps_namespace_view_close(view);
    view = NULL;
    for (i = 0; i < CHECKPOINT_FAULT_KEYS; ++i)
      CHECK(state.index[i] == CHECKPOINT_FAULT_KEYS + i + 1 && !state.found[i]);
    OK(ps_namespace_checkpoint_flush(ns, &error));
    OK(ps_namespace_status_get(ns, &status, &error));
    CHECK(status.checkpoint_epoch &&
          status.checkpoint_keys == CHECKPOINT_FAULT_KEYS &&
          !status.pending_keys && status.covered_offset == status.tail);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    root = NULL;
  }
  CHECK(operations > 10);
  printf("checkpoint atomic-group cuts (durable=%d %s): %lu\n", durable,
         heap ? "allocation" : "write/sync", (unsigned long)operations);
  free(m.stable);
  clean();
}
typedef struct checkpoint_large_state {
  size_t keys, records;
  uint64_t *index;
} checkpoint_large_state;
static int checkpoint_large_visit(void *context, uint64_t segment,
                                  const lc_pouch_state_entry *entry,
                                  lc_error *e) {
  checkpoint_large_state *state = context;
  unsigned int i;
  char trailing;
  (void)segment;
  (void)e;
  if (entry->control)
    return LC_OK;
  CHECK(sscanf(entry->key, "lease/%u%c", &i, &trailing) == 1 &&
        i < state->keys);
  CHECK(!entry->payload_span.present && entry->has_query_index_seq &&
        !entry->query_index_seq && !entry->lifetime);
  if (entry->index_seq > state->keys)
    CHECK(!entry->found && entry->version == 1);
  if (entry->index_seq > state->index[i])
    state->index[i] = entry->index_seq;
  ++state->records;
  return LC_OK;
}
static void checkpoint_large_group(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_status status;
  checkpoint_observation observation = {0};
  ps_checkpoint_observer observer = {checkpoint_prepare, checkpoint_finish,
                                     &observation};
  checkpoint_large_state state = {1500, 0, NULL};
  ps_budget_plan descriptors = {0};
  size_t i;
  clean();
  state.index = calloc(state.keys, sizeof(*state.index));
  CHECK(state.index);
  for (i = 0; i < state.keys; ++i)
    OK(ps_budget_plan_value(&descriptors, 512, &error));
  CHECK(descriptors.metadata_bytes > PS_JOURNAL - 64);
  options.storage.initial_capacity = options.storage.maximum_capacity =
      PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  OK(ps_namespace_checkpoint_observe(ns, &observer, &error));
  checkpoint_group(ns, state.keys);
  OK(ps_flush(ps_root_space(root), &error));
  observation.reject = 2;
  CHECK(ps_namespace_checkpoint_flush(ns, &error) == LC_ERR_NOMEM);
  lc_error_cleanup(&error);
  CHECK(observation.published == 1);
  OK(ps_namespace_status_get(ns, &status, &error));
  CHECK(!status.checkpoint_epoch && status.checkpoint_keys == state.keys &&
        status.pending_keys && status.pending_keys < state.keys);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  root = NULL;
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &ns, &error));
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, checkpoint_large_visit, &state, &error));
  ps_namespace_view_close(view);
  view = NULL;
  for (i = 0; i < state.keys; ++i)
    CHECK(state.index[i] == state.keys + i + 1);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  memset(state.index, 0, state.keys * sizeof(*state.index));
  state.records = 0;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, checkpoint_large_visit, &state, &error));
  CHECK(state.records == state.keys);
  for (i = 0; i < state.keys; ++i)
    CHECK(state.index[i] == state.keys + i + 1);
  ps_namespace_view_close(view);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  free(state.index);
  clean();
}
typedef struct checkpoint_pin {
  ps_namespace *ns;
  ps_namespace_window *body, *metadata;
  uint64_t body_offset, metadata_bytes;
  char leaf[34];
} checkpoint_pin;
static int checkpoint_pin_visit(void *context, uint64_t segment,
                                const lc_pouch_state_entry *entry,
                                lc_error *e) {
  checkpoint_pin *pin = context;
  lc_pouch_record_header header;
  unsigned char bytes[LC_POUCH_RECORD_HEADER_BYTES];
  struct iovec part = {bytes, sizeof(bytes)};
  unsigned int kind;
  uint64_t id;
  (void)segment;
  if (entry->control)
    return LC_OK;
  CHECK(!pin->body && entry->index_seq == 1 && entry->has_record_ref &&
        strlen(entry->record_container_leaf) < sizeof(pin->leaf));
  strcpy(pin->leaf, entry->record_container_leaf);
  CHECK(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                       &kind, &id));
  OK(ps_namespace_read_open(pin->ns, kind, id,
                            entry->payload_span.payload_offset,
                            entry->payload_span.payload_length, &pin->body, e));
  pin->body_offset = entry->payload_span.payload_offset;
  CHECK(ps_namespace_parse_source_leaf(pin->leaf, &kind, &id) &&
        kind == PS_CATALOG_SNAPSHOT);
  OK(ps_namespace_read_open(pin->ns, kind, id, 0, sizeof(bytes), &pin->metadata,
                            e));
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin->metadata), 0, &part,
                             1, e));
  OK(lc_pouch_record_header_decode(bytes, &header, e));
  pin->metadata_bytes = sizeof(bytes) + header.key_len + header.meta_len;
  CHECK(!header.payload_len);
  ps_namespace_read_close(pin->metadata);
  pin->metadata = NULL;
  OK(ps_namespace_read_open(pin->ns, kind, id, 0, pin->metadata_bytes,
                            &pin->metadata, e));
  return LC_OK;
}
static void checkpoint_retirement(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  checkpoint_pin pin = {0};
  lc_allocator allocator = {0};
  lc_pouch_state_entry entry = {0};
  unsigned char bytes[4];
  struct iovec part = {bytes, sizeof(bytes)};
  uint64_t next, closed, pending;
  int found, truncated;
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  pin.ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, checkpoint_pin_visit, &pin, &error));
  checkpoint_put(root, ns, "lease/0", 2, 2, 1, 1, 1);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  CHECK(!ps_namespace_source_refs(ns, PS_CATALOG_SEGMENT, 1));
  CHECK(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &error) ==
        LC_ERR_INVALID);
  CHECK(error.server_code &&
        !strcmp(error.server_code, "pouch_namespace_busy"));
  lc_error_cleanup(&error);
  OK(lc_pouch_state_record_read(ps_namespace_read_input(pin.metadata),
                                &allocator, pin.leaf, 0, pin.metadata_bytes, 0,
                                0, NULL, &entry, &next, &found, &truncated,
                                &error));
  CHECK(found && !truncated && entry.index_seq == 1 && entry.version == 1);
  lc_pouch_state_entry_cleanup(&allocator, &entry);
  ps_namespace_view_close(view);
  view = NULL;
  ps_namespace_read_close(pin.metadata);
  pin.metadata = NULL;
  OK(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &error));
  OK(ps_namespace_reap(ns, &closed, &pending, &error));
  CHECK(pending == 1);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(lc_pouch_record_io_read(ps_namespace_read_input(pin.body), pin.body_offset,
                             &part, 1, &error));
  CHECK(!memcmp(bytes, "data", sizeof(bytes)));
  ps_namespace_read_close(pin.body);
  OK(ps_namespace_reap(ns, &closed, &pending, &error));
  CHECK(closed == 1 && !pending);
  OK(ps_checkpoint(ps_root_space(root), &error));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
typedef struct checkpoint_history_input {
  lc_source source;
  size_t remaining, calls;
} checkpoint_history_input;
static size_t checkpoint_history_read(lc_source *source, void *bytes,
                                      size_t count, lc_error *e) {
  checkpoint_history_input *input = source->impl;
  (void)e;
  if (count > input->remaining)
    count = input->remaining;
  memset(bytes, 'h', count);
  input->remaining -= count;
  ++input->calls;
  return count;
}
static void checkpoint_history(int durable) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_append *append = NULL;
  ps_namespace_status before, after;
  lc_allocator allocator = {0};
  lc_pouch_state_entry request = {0}, result = {0};
  checkpoint_history_input input = {0};
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &ns, &error));
  input.remaining = (size_t)PS_MIB;
  input.source.impl = &input;
  input.source.read = checkpoint_history_read;
  request.key = "lease/0";
  request.content_type = "application/test";
  request.payload_context = "checkpoint-history";
  request.version = request.index_seq = request.lifetime = 1;
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  OK(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  OK(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                  ps_namespace_append_output(append),
                                  ps_namespace_append_leaf(append), &request,
                                  &input.source, 0, &result, &error));
  CHECK(!input.remaining && input.calls > 2 && result.bytes == PS_MIB);
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
  append = NULL;
  lc_pouch_state_entry_cleanup(&allocator, &result);
  OK(ps_namespace_status_get(ns, &before, &error));
  CHECK(!before.checkpoint_epoch && before.pending_keys == 1 &&
        before.pending_metadata_bytes < PS_PAGE && before.tail > PS_MIB);
  /* Repeated writes to one key never meet the distinct-key trigger. The
   * history trigger must checkpoint before admitting the next body producer. */
  OK(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  OK(ps_namespace_status_get(ns, &after, &error));
  CHECK(after.checkpoint_epoch && !after.pending_keys &&
        after.checkpoint_keys == 1 && after.covered_offset == before.tail);
  ps_namespace_append_close(append);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  clean();
}
#endif
