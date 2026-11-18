/* Shared SDK fixtures, common scheduler, and public namespace maintenance. */
#include "lc_pouch_retention.h"
#include "namespace.h"
#include "namespace_work.h"
#include <zlib.h>

static lc_pouch *fixture_pouch(fixture *f) {
  return ((lc_client_handle *)f->client)->pouch;
}
static void namespace_pass(fixture *f, const char *ns, int force, int cleanup,
                           lc_pouch_maintenance_result *out) {
  lc_pouch_maintenance_options options = {0};
  options.ns = ns;
  options.force = force;
  options.cleanup_only = cleanup;
  ok(lc_pouch_maintenance_run(fixture_pouch(f), &options, out, &f->error),
     &f->error);
}
static void background_peer_prepare(fixture *f, lc_collection_lock *pin) {
  lc_collection_handle *peer = f->peer->impl;
  namespace_pass(f, "journal", 1, 0, NULL);
  ok(lc_collection_lock_enter(peer->authority, LC_COLLECTION_AUTH_APPEND, 0U, 1,
                              1, pin, &f->error),
     &f->error);
}
static void state_seed(fixture *f, const char *ns, const char *key_name,
                       uint64_t size, lc_pouch_unix_seconds deadline) {
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result result = {0};
  producer p;
  producer_init(&p, size);
  options.has_retention = 1;
  options.retention = deadline;
  ok(lc_pouch_state_write(fixture_pouch(f), ns, key_name, &p.source, &options,
                          &result, &f->error),
     &f->error);
  REQUIRE(p.at == size);
  lc_pouch_state_write_result_cleanup(NULL, &result);
}
static void state_body(fixture *f, const char *ns, const char *key_name,
                       uint64_t size) {
  consumer output = {0};
  lc_pouch_state_read_result result = {0};
  output.size = size;
  output.sink.impl = &output;
  output.sink.write = consume;
  ok(lc_pouch_state_copy(fixture_pouch(f), ns, key_name, &output.sink, &result,
                         &f->error),
     &f->error);
  REQUIRE(result.found && output.at == size);
  lc_pouch_state_read_result_cleanup(NULL, &result);
}
static void background_named(fixture *f, const char *ns, const char *name,
                             int create, lc_collection **out) {
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.ns = ns;
  config.name = name;
  ok(create ? lc_client_create_collection(f->client, &config, out, &f->error)
            : lc_client_open_collection(f->client, &config, out, &f->error),
     &f->error);
}
static void background_policy(const char *directory, int codec, int sync) {
  fixture f;
  lc_pouch_maintenance_result result = {0};
  lc_collection_metadata_req limit = {0};
  lc_collection_info info;
  lc_collection_head_res head;
  setup(&f, directory, codec, sync, 1);
  (void)append(f.collection, "first", 73728U, &f.error);
  (void)append(f.collection, "second", 512U, &f.error);
  (void)append(f.collection, "third", 8192U, &f.error);
  limit.has_max_entries = 1;
  limit.max_entries = 1U;
  ok(lc_collection_metadata(f.collection, &limit, &info, &f.error), &f.error);
  state_seed(&f, "journal", "state", 73728U, 0);
  state_seed(&f, "journal", "state", 8192U, 0);
  namespace_pass(&f, "journal", 1, 0, &result);
  REQUIRE(result.ns && !strcmp(result.ns, "journal") && result.compacted &&
          result.compacted_collection_count == 1U);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  ok(lc_collection_head(f.peer, &head, &f.error), &f.error);
  REQUIRE(head.live_count == 1U && head.upper_position == 3U);
  read_entry(f.peer, 3U, 8192U, &f.error);
  state_body(&f, "journal", "state", 8192U);
  lc_collection_handle *c = f.collection->impl;
  lc_collection_disk_head disk;
  ok(c->control.load(c->control.context, &disk, &f.error), &f.error);
  lc_u64 snapshot = disk.snapshot;
  namespace_pass(&f, "journal", 0, 1, &result);
  ok(c->control.load(c->control.context, &disk, &f.error), &f.error);
  REQUIRE(!result.compacted_collection_count && disk.snapshot == snapshot);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  /* Explicit age sweeping reuses the same logical version checks. */
  lc_pouch_maintenance_options options = {0};
  options.ns = "journal";
  options.retention_updated_before_unix = 2000000000;
  ok(lc_pouch_maintenance_run(fixture_pouch(&f), &options, &result, NULL),
     &f.error);
  REQUIRE(result.retention_deleted_state_count >= 1U);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  lc_pouch_state_read_result state = {0};
  ok(lc_pouch_state_read_metadata(fixture_pouch(&f), "journal", "state", &state,
                                  &f.error),
     &f.error);
  REQUIRE(!state.found);
  lc_pouch_state_read_result_cleanup(NULL, &state);
  reopen(&f);
  read_entry(f.peer, 3U, 8192U, &f.error);
  namespace_pass(&f, "journal", 0, 1, &result);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
typedef struct background_peer_source {
  lc_source source;
  producer input;
  fixture *fixture;
  lc_u64 previous_snapshot;
  int observed;
} background_peer_source;
static size_t background_peer_read(lc_source *pub, void *bytes, size_t count,
                                   lc_error *e) {
  background_peer_source *source = pub->impl;
  fixture *f = source->fixture;
  lc_collection_handle *peer = f->peer->impl;
  if (!source->observed) {
    lc_collection_head_res head;
    lc_collection_append_status_res status;
    source->observed = 1;
    ok(lc_collection_head(f->peer, &head, e), e);
    REQUIRE(head.upper_position == 3U && head.live_count == 1U &&
            peer->order.snapshot != source->previous_snapshot &&
            peer->ids.snapshot == peer->order.snapshot);
    read_entry(f->peer, 1U, 73728U, e);
    ok(lc_collection_append_status(f->peer, "live", &status, e), e);
    REQUIRE(status.state == LC_COLLECTION_APPEND_COMMITTED &&
            status.entry.position == 1U);
  }
  return source->input.source.read(&source->input.source, bytes, count, e);
}
static void background_peer_view(const char *directory, int codec, int sync) {
  fixture f;
  lc_collection_head_res head;
  lc_collection_delete_res deleted;
  lc_pouch_maintenance_result result = {0};
  lc_collection_lock append_pin;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, 2);
  (void)append(f.collection, "live", 73728U, &f.error);
  (void)append(f.collection, "deleted", 512U, &f.error);
  ok(lc_collection_delete_entry(f.collection, 2U, &deleted, &f.error),
     &f.error);
  ok(lc_collection_head(f.peer, &head, &f.error), &f.error);
  lc_collection_handle *peer = f.peer->impl;
  lc_u64 before = peer->order.snapshot;
  namespace_pass(&f, "journal", 1, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  REQUIRE(peer->order.snapshot == before);
  /* Deterministically enter the read/prepare window while append authority is
   * busy. A peer read must refresh its captured canonical topology rather than
   * waiting for an upload or treating its old disposable projection as damage.
   */
  ok(lc_collection_lock_enter(peer->authority, LC_COLLECTION_AUTH_APPEND, 0U, 1,
                              1, &append_pin, &f.error),
     &f.error);
  ok(lc_collection_head(f.peer, &head, &f.error), &f.error);
  REQUIRE(head.upper_position == 2U && head.live_count == 1U &&
          peer->order.snapshot != before &&
          peer->ids.snapshot == peer->order.snapshot);
  read_entry(f.peer, 1U, 73728U, &f.error);
  lc_collection_append_status_res status;
  ok(lc_collection_append_status(f.peer, "live", &status, &f.error), &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_COMMITTED &&
          status.entry.position == 1U);
  lc_collection_lock_leave(&append_pin);
  /* Repeat inside a real producer with a private canonical suffix. Refreshing
   * the peer's projection must neither wait for nor truncate that suffix. */
  before = peer->order.snapshot;
  (void)append(f.collection, "next-deleted", 512U, &f.error);
  ok(lc_collection_delete_entry(f.collection, 3U, &deleted, &f.error),
     &f.error);
  namespace_pass(&f, "journal", 1, 0, NULL);
  background_peer_source source = {0};
  source.fixture = &f;
  source.previous_snapshot = before;
  source.source.impl = &source;
  source.source.read = background_peer_read;
  producer_init(&source.input, 73728U);
  lc_collection_append_req request;
  lc_collection_append_res appended;
  lc_collection_append_req_init(&request);
  request.append_id = "after-snapshot";
  ok(lc_collection_append(f.collection, &request, &source.source, &appended,
                          &f.error),
     &f.error);
  REQUIRE(source.observed && source.input.at == source.input.size &&
          appended.position == 4U);
  reopen(&f);
  read_entry(f.peer, 1U, 73728U, &f.error);
  read_entry(f.peer, 4U, 73728U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static void background_discovery(const char *directory, int codec, int sync) {
  fixture f;
  lc_collection *cold = NULL, *hot = NULL;
  lc_collection_delete_res deleted;
  ps_namespace *ns = NULL;
  lc_collection_head_res head;
  char *name = NULL, *cursor = NULL;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, 2);
  background_named(&f, "cold/\303\245", "events", 1, &cold);
  background_named(&f, "hot", "events", 1, &hot);
  (void)append(cold, "old", 80U, &f.error);
  (void)append(cold, "live", 512U, &f.error);
  ok(lc_collection_delete_entry(cold, 1U, &deleted, &f.error), &f.error);
  state_seed(&f, "ordinary", "state", 131072U, 0);
  state_seed(&f, "ordinary", "state", 8192U, 0);
  lc_collection_close(cold);
  lc_collection_close(hot);
  close_store(&f);
  open_client(&f, 0);
  ps_root *root = client_root(&f);
  ok(ps_root_namespace_acquire(root, "cold/\303\245", 0, &ns, &f.error),
     &f.error);
  REQUIRE(!ns->data && !ns->collections);
  ok(lc_collection_maintenance_next(fixture_pouch(&f), NULL, &name, &cursor,
                                    &f.error),
     &f.error);
  REQUIRE(name && !strcmp(name, "cold/\303\245") && cursor && !ns->data &&
          !ns->collections);
  lc_free_with_allocator(NULL, name);
  lc_free_with_allocator(NULL, cursor);
  name = cursor = NULL;
  ok(ps_namespace_work_next(root, NULL, LC_POUCH_STORAGE_WORK_ORDINARY, NULL,
                            &name, &cursor, &f.error),
     &f.error);
  REQUIRE(name && !strcmp(name, "ordinary") && !ns->data && !ns->collections);
  lc_free_with_allocator(NULL, name);
  lc_free_with_allocator(NULL, cursor);
  ps_namespace_release(ns);
  background_named(&f, "hot", "events", 0, &hot);
  /* Keep the volatile queue continuously hot. Durable ordinary discovery has
   * its own fair source, so it cannot be starved by that queue. */
  for (unsigned int turn = 0U; turn < 12U; ++turn) {
    char id[32];
    snprintf(id, sizeof(id), "hot-%u", turn);
    (void)append(hot, id, 80U, &f.error);
    lc_pouch_test_compaction_run_pass(fixture_pouch(&f));
  }
  background_named(&f, "cold/\303\245", "events", 0, &cold);
  ok(lc_collection_head(cold, &head, &f.error), &f.error);
  REQUIRE(head.live_count == 1U &&
          ((lc_collection_handle *)cold->impl)->head.snapshot);
  state_body(&f, "ordinary", "state", 8192U);
  ok(ps_root_namespace_acquire(root, "ordinary", 0, &ns, &f.error), &f.error);
  ps_namespace_status status;
  ok(ps_namespace_status_get(ns, &status, &f.error), &f.error);
  REQUIRE(status.snapshot);
  ps_namespace_release(ns);
  lc_collection_close(cold);
  lc_collection_close(hot);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static ps_catalog_view *background_catalog(fixture *f, ps_catalog_info *info,
                                           ps_namespace **ns) {
  ps_catalog_view *view = NULL;
  ok(ps_root_namespace_acquire(client_root(f), "journal", 0, ns, &f->error),
     &f->error);
  ok(ps_catalog_get(client_root(f)->space, (*ns)->identity.owner,
                    PS_CATALOG_COLLECTION, "events/\303\245",
                    strlen("events/\303\245"), &view, &f->error),
     &f->error);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, info, &f->error), &f->error);
  return view;
}
static void background_drop_pending(fixture *f) {
  ps_namespace *ns = NULL;
  ps_catalog_info info;
  ps_catalog_view *view = background_catalog(f, &info, &ns);
  ok(ps_catalog_remove(client_root(f)->space, ns->identity.owner,
                       PS_CATALOG_COLLECTION, "events/\303\245",
                       strlen("events/\303\245"), &info.revision, &f->error),
     &f->error);
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  close_store(f);
  open_client(f, 0);
}
static void background_verify_empty(fixture *f) {
  size_t count = 0U;
  char *name = NULL, *cursor = NULL;
  ps_stats stats;
  ok(lc_client_list_collections(f->client, "journal", listed, &count,
                                &f->error),
     &f->error);
  REQUIRE(!count);
  ok(background_operation(f, 1, &f->error), &f->error);
  ok(lc_collection_maintenance_next(fixture_pouch(f), NULL, &name, &cursor,
                                    &f->error),
     &f->error);
  REQUIRE(!name && !cursor);
  ok(ps_status(client_root(f)->space, &stats, &f->error), &f->error);
  REQUIRE(!stats.retired_bytes);
}
static void background_hints(const char *directory, int codec, int sync) {
  fixture f;
  ps_namespace *ns = NULL;
  lc_collection *good = NULL;
  lc_collection_delete_res deleted;
  lc_pouch_maintenance_result result = {0};
  unsigned char original[32], damaged[32];
  ps_reader *reader = NULL;
  ps_writer *writer = NULL;
  size_t got;
  int present;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, 2);
  background_named(&f, "z-good", "events", 1, &good);
  (void)append(good, "old", 80U, &f.error);
  (void)append(good, "live", 512U, &f.error);
  ok(lc_collection_delete_entry(good, 1U, &deleted, &f.error), &f.error);
  lc_collection_close(good);
  ok(ps_root_namespace_acquire(client_root(&f), "bad", 1, &ns, &f.error),
     &f.error);
  for (int kind = LC_POUCH_STORAGE_WORK_ORDINARY;
       kind <= LC_POUCH_STORAGE_WORK_COLLECTION; ++kind) {
    ok(ps_namespace_work_ensure(ns, (enum lc_pouch_storage_work_kind)kind, 0,
                                &f.error),
       &f.error);
    ok(ps_namespace_work_get(ns, (enum lc_pouch_storage_work_kind)kind,
                             &present, &f.error),
       &f.error);
    REQUIRE(present && !ns->data && !ns->collections);
  }
  ps_key key = {ns->identity.owner, 6U, PS_CONTROL};
  ok(ps_read_open(client_root(&f)->space, key, NULL, &reader, &f.error),
     &f.error);
  ok(ps_read(reader, original, sizeof(original), &got, &f.error), &f.error);
  REQUIRE(got == sizeof(original));
  ps_reader_close(reader);
  /* Independently construct malformed lengths and identities with valid outer
   * container checksums. Decode must fail atomically, not accept a neighbour's
   * owner/kind or a partially decoded presence bit. */
  for (int bad = 0; bad < 5; ++bad) {
    unsigned char bytes[33] = {0};
    size_t length = bad == 0 ? 31U : bad == 1 ? 33U : 32U;
    memcpy(bytes, original, sizeof(original));
    if (bad == 2)
      bytes[0] ^= 1U;
    if (bad == 3)
      bytes[8] ^= 1U;
    if (bad == 4)
      bytes[16] = 0U;
    uint64_t crc = (uint64_t)crc32(0L, bytes, 24U);
    for (size_t i = 0U; i < 8U; ++i)
      bytes[24U + i] = (unsigned char)(crc >> (8U * i));
    writer = NULL;
    ok(ps_begin(client_root(&f)->space, NULL, key, &writer, &f.error),
       &f.error);
    ok(ps_write(writer, bytes, length, &f.error), &f.error);
    ok(ps_publish(writer, NULL, &f.error), &f.error);
    ps_writer_close(writer);
    present = 1;
    REQUIRE(ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_COLLECTION,
                                  &present, &f.error) == LC_ERR_PROTOCOL);
    REQUIRE(!present);
    clear(&f.error);
  }
  memcpy(damaged, original, sizeof(damaged));
  damaged[31] ^= 1U;
  ok(ps_begin(client_root(&f)->space, NULL, key, &writer, &f.error), &f.error);
  ok(ps_write(writer, damaged, sizeof(damaged), &f.error), &f.error);
  ok(ps_publish(writer, NULL, &f.error), &f.error);
  ps_writer_close(writer);
  ps_namespace_release(ns);
  close_store(&f);
  /* Discovery admits metadata identities, so malformed advisory contents do
   * not prevent open or hide healthy sibling namespaces. */
  open_client(&f, 0);
  char *name = NULL, *cursor = NULL;
  ok(lc_collection_maintenance_next(fixture_pouch(&f), NULL, &name, &cursor,
                                    &f.error),
     &f.error);
  REQUIRE(name && !strcmp(name, "bad"));
  lc_free_with_allocator(NULL, name);
  lc_free_with_allocator(NULL, cursor);
  lc_pouch_maintenance_options options = {0};
  options.ns = "bad";
  options.force = 1;
  REQUIRE(lc_pouch_maintenance_run(fixture_pouch(&f), &options, &result,
                                   &f.error) == LC_ERR_PROTOCOL);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  clear(&f.error);
  for (int turn = 0; turn < 15; ++turn)
    lc_pouch_test_compaction_run_pass(fixture_pouch(&f));
  background_named(&f, "z-good", "events", 0, &good);
  lc_collection_handle *c = good->impl;
  REQUIRE(c->head.snapshot);
  read_entry(good, 2U, 512U, &f.error);
  lc_collection_close(good);
  writer = NULL;
  ok(ps_begin(client_root(&f)->space, NULL, key, &writer, &f.error), &f.error);
  ok(ps_write(writer, original, sizeof(original), &f.error), &f.error);
  ok(ps_publish(writer, NULL, &f.error), &f.error);
  ps_writer_close(writer);
  namespace_pass(&f, "bad", 0, 1, &result);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static void background_protected(const char *directory, int codec, int sync) {
  fixture f;
  producer p;
  lc_pouch_state_write_result staged = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_maintenance_result result = {0};
  lc_collection_delete_res deleted;
  ps_namespace *ns = NULL;
  ps_namespace_append *upload = NULL;
  ps_namespace_status status;
  setup(&f, directory, codec, sync, 1);
  state_seed(&f, "journal", "state", 80U, 0);
  producer_init(&p, 512U);
  ok(lc_pouch_state_stage_write(fixture_pouch(&f), "journal", "state",
                                "live-transaction", &p.source, &options,
                                &staged, &f.error),
     &f.error);
  lc_pouch_state_write_result_cleanup(NULL, &staged);
  (void)append(f.collection, "first", 80U, &f.error);
  (void)append(f.collection, "deleted", 80U, &f.error);
  ok(lc_collection_delete_entry(f.collection, 2U, &deleted, &f.error),
     &f.error);
  namespace_pass(&f, "journal", 1, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U &&
          result.cleanup_pending_count);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  state_body(&f, "journal", "state", 80U);
  ok(ps_root_namespace_acquire(client_root(&f), "journal", 0, &ns, &f.error),
     &f.error);
  ok(ps_namespace_status_get(ns, &status, &f.error), &f.error);
  REQUIRE(!status.snapshot);
  int discarded = 0;
  ok(lc_pouch_state_discard_staged(fixture_pouch(&f), "journal", "state",
                                   "live-transaction", &discarded, &f.error),
     &f.error);
  REQUIRE(discarded);
  ok(ps_namespace_append_begin(ns, 0U, 4096U, PS_APPEND_PREFIX, &upload,
                               &f.error),
     &f.error);
  (void)append(f.collection, "during-upload", 80U, &f.error);
  namespace_pass(&f, "journal", 1, 0, &result);
  REQUIRE(result.compacted_collection_count == 1U &&
          result.cleanup_pending_count);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  ps_namespace_append_close(upload);
  ps_namespace_release(ns);
  namespace_pass(&f, "journal", 1, 0, &result);
  REQUIRE(result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  state_body(&f, "journal", "state", 80U);
  read_entry(f.collection, 1U, 80U, &f.error);
  read_entry(f.collection, 3U, 80U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static void background_orphans(const char *directory, int codec, int sync,
                               int process) {
  fixture f;
  ps_namespace *ns = NULL;
  ps_catalog_info info;
  ps_catalog_view *view;
  lc_pouch_maintenance_result result = {0};
  int present;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, process ? 2 : 0);
  (void)append(f.collection, "seed", 73728U, &f.error);
  view = background_catalog(&f, &info, &ns);
  if (process) {
    ok(ps_catalog_remove(client_root(&f)->space, ns->identity.owner,
                         PS_CATALOG_COLLECTION, "events/\303\245",
                         strlen("events/\303\245"), &info.revision, &f.error),
       &f.error);
    ps_catalog_view_close(view);
    ps_namespace_release(ns);
    close_store(&f);
    pid_t child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      f.worker_mode = 1;
      open_client(&f, 0);
      ps_namespace *child_ns = NULL;
      ok(ps_root_namespace_acquire(client_root(&f), "journal", 0, &child_ns,
                                   &f.error),
         &f.error);
      for (unsigned int attempt = 0U; attempt < 750U; ++attempt) {
        ok(ps_namespace_work_get(child_ns, LC_POUCH_STORAGE_WORK_COLLECTION,
                                 &present, &f.error),
           &f.error);
        if (!present)
          break;
        usleep(20000);
      }
      REQUIRE(!present);
      _exit(73);
    }
    int status;
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 73);
    open_client(&f, 0);
    ok(ps_root_namespace_acquire(client_root(&f), "journal", 0, &ns, &f.error),
       &f.error);
  } else {
    ok(lc_collection_drop(f.collection, &f.error), &f.error);
    lc_collection_close(f.peer);
    lc_collection_close(f.collection);
    f.peer = f.collection = NULL;
    namespace_pass(&f, "journal", 0, 1, &result);
    REQUIRE(result.cleanup_pending_count);
    lc_pouch_maintenance_result_cleanup(NULL, &result);
    ok(ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_COLLECTION, &present,
                             &f.error),
       &f.error);
    REQUIRE(present);
    ps_catalog_view_close(view);
    namespace_pass(&f, "journal", 0, 1, NULL);
  }
  ok(ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_COLLECTION, &present,
                           &f.error),
     &f.error);
  REQUIRE(!present);
  ps_namespace_release(ns);
  size_t count = 0U;
  ok(lc_client_list_collections(f.client, "journal", listed, &count, &f.error),
     &f.error);
  REQUIRE(!count);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static int background_clock(clockid_t id, struct timespec *out, void *context) {
  (void)id;
  out->tv_sec = (time_t)__atomic_load_n((lc_u64 *)context, __ATOMIC_ACQUIRE);
  out->tv_nsec = 0;
  return 0;
}
static double background_now(void) {
  struct timespec time;
  REQUIRE(!clock_gettime(CLOCK_MONOTONIC, &time));
  return (double)time.tv_sec + (double)time.tv_nsec / 1e9;
}
static void background_live(const char *directory, int codec, int sync) {
  fixture f;
  lc_u64 now = 2000000000U;
  lc_collection *hot = NULL;
  lc_collection_metadata_req expiry = {0};
  lc_collection_append_res entry;
  lc_collection_head_res head;
  lc_pouch_test_queue_clock_gettime = background_clock;
  lc_pouch_test_queue_time_context = &now;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, 1);
  (void)append(f.collection, "expire", 80U, &f.error);
  expiry.has_retention_seconds = 1;
  expiry.retention_seconds = 1;
  ok(lc_collection_entry_metadata(f.collection, 1U, &expiry, &entry, &f.error),
     &f.error);
  state_seed(&f, "journal", "expire", 80U, (lc_i64)now + 1);
  background_named(&f, "hot", "events", 1, &hot);
  __atomic_store_n(&now, 2000000002U, __ATOMIC_RELEASE);
  double deadline = background_now() + 15.0;
  int expired = 0;
  for (unsigned int n = 0U; background_now() < deadline; ++n) {
    char id[32];
    snprintf(id, sizeof(id), "tick-%u", n);
    (void)append(hot, id, 80U, &f.error);
    ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
    lc_pouch_state_read_result result = {0};
    ok(lc_pouch_state_read_metadata(fixture_pouch(&f), "journal", "expire",
                                    &result, &f.error),
       &f.error);
    lc_pouch_retention_work work = {0};
    ok(lc_pouch_retention_work_read(fixture_pouch(&f), "journal", &work,
                                    &f.error),
       &f.error);
    expired = !head.live_count && !result.has_body &&
              (!work.present || work.phase == LC_POUCH_RETENTION_IDLE);
    lc_pouch_retention_work_cleanup(fixture_pouch(&f), &work);
    lc_pouch_state_read_result_cleanup(NULL, &result);
    if (expired)
      break;
    usleep(20000);
  }
  if (!expired) {
    lc_pouch_maintenance_options options = {0};
    lc_pouch_maintenance_result result = {0};
    options.ns = "journal";
    options.force = 1;
    int rc = lc_pouch_maintenance_run(fixture_pouch(&f), &options, &result,
                                      &f.error);
    fprintf(stderr,
            "background timeout live=%llu rc=%d error=%s [%s] "
            "scanned=%lu expired=%lu deleted=%lu failed=%lu\n",
            (unsigned long long)head.live_count, rc,
            f.error.message ? f.error.message : "",
            f.error.server_code ? f.error.server_code : "",
            result.retention_scanned_count, result.retention_expired_count,
            result.retention_deleted_state_count,
            result.retention_failed_count);
    lc_pouch_maintenance_result_cleanup(NULL, &result);
  }
  REQUIRE(expired);
  lc_collection_close(hot);
  close_store(&f);
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_error_cleanup(&f.error);
}
static int background_pause(void *context, lc_collection *c, int phase,
                            lc_error *e) {
  gate *g = context;
  (void)c;
  (void)e;
  if (phase == LC_COLLECTION_COMPACT_COPIED && !g->entered) {
    gate_signal(g, 0);
    gate_wait(g, 1);
  }
  return LC_OK;
}
typedef struct background_close {
  lc_client *client;
  gate started;
  int done;
} background_close;
static void *background_close_client(void *context) {
  background_close *work = context;
  gate_signal(&work->started, 0);
  lc_client_close(work->client);
  __atomic_store_n(&work->done, 1, __ATOMIC_RELEASE);
  return NULL;
}
static void background_shutdown(const char *directory, int codec, int sync) {
  fixture f;
  gate g = {0};
  background_close work = {0};
  pthread_t thread;
  lc_collection_delete_res deleted;
  REQUIRE(!pthread_mutex_init(&g.mutex, NULL));
  REQUIRE(!pthread_cond_init(&g.condition, NULL));
  REQUIRE(!pthread_mutex_init(&work.started.mutex, NULL));
  REQUIRE(!pthread_cond_init(&work.started.condition, NULL));
  lc_collection_compact_test_context = &g;
  lc_collection_compact_test_hook = background_pause;
  setup_io_mode(&f, directory, codec, sync, 1, NULL, 1);
  (void)append(f.collection, "body", 73728U, &f.error);
  (void)append(f.collection, "deleted", 80U, &f.error);
  ok(lc_collection_delete_entry(f.collection, 2U, &deleted, &f.error),
     &f.error);
  gate_wait(&g, 0);
  lc_collection_close(f.peer);
  lc_collection_close(f.collection);
  f.peer = f.collection = NULL;
  work.client = f.client;
  REQUIRE(!pthread_create(&thread, NULL, background_close_client, &work));
  gate_wait(&work.started, 0);
  usleep(20000);
  REQUIRE(!__atomic_load_n(&work.done, __ATOMIC_ACQUIRE));
  gate_signal(&g, 1);
  REQUIRE(!pthread_join(thread, NULL));
  f.client = NULL;
  lc_collection_compact_test_hook = NULL;
  lc_collection_compact_test_context = NULL;
  open_client(&f, 0);
  open_collection(&f, 0, &f.collection);
  read_entry(f.collection, 1U, 73728U, &f.error);
  close_store(&f);
  REQUIRE(!pthread_mutex_destroy(&g.mutex));
  REQUIRE(!pthread_cond_destroy(&g.condition));
  REQUIRE(!pthread_mutex_destroy(&work.started.mutex));
  REQUIRE(!pthread_cond_destroy(&work.started.condition));
  lc_error_cleanup(&f.error);
}
static void background_suite(const char *directory, int codec, int sync,
                             int process) {
  snprintf(scenario, sizeof(scenario), "background policy %d/%d", codec, sync);
  background_policy(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background peer view %d/%d", codec,
           sync);
  background_peer_view(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background discovery %d/%d", codec,
           sync);
  background_discovery(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background hints %d/%d", codec, sync);
  background_hints(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background protected %d/%d", codec,
           sync);
  background_protected(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background orphans %d/%d", codec, sync);
  background_orphans(directory, codec, sync, 0);
  if (process)
    background_orphans(directory, codec, sync, 1);
  snprintf(scenario, sizeof(scenario), "background live %d/%d", codec, sync);
  background_live(directory, codec, sync);
  snprintf(scenario, sizeof(scenario), "background shutdown %d/%d", codec,
           sync);
  background_shutdown(directory, codec, sync);
  printf("background maintenance codec=%d durable=%d passed\n", codec, sync);
}
