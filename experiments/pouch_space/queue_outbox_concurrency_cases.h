#include "queue_outbox_marker_cases.h"
typedef struct outbox_marker_writer {
  lc_pouch *pouch;
  const char *parent;
  int result;
} outbox_marker_writer;
static void *outbox_marker_write_thread(void *context) {
  outbox_marker_writer *writer = context;
  lc_error e;
  lc_error_init(&e);
  writer->result = lc_pouch_outbox_state_reclaim_marker_write(
      writer->pouch, "work/a", "held", writer->parent, "checkpoint", &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void outbox_marker_concurrency(const char *directory) {
  lc_error e;
  char path[1400];
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    REQUIRE(snprintf(path, sizeof(path), "%s/concurrent-%d", directory, sync) <
            (int)sizeof(path));
    lc_pouch *p = visit_open(path, 0, sync, 1, NULL, &allocator, &e);
    ps_root *root = ps_pouch_state_root(p);
    ps_namespace *ns = NULL;
    void *pin = NULL;
    pthread_t threads[2];
    outbox_marker_writer writers[2] = {{p, "first-parent", 0},
                                       {p, "second-parent", 0}};
    ok(ps_root_namespace_acquire(root, "work/a", 1, &ns, &e), &e);
    ok(ps_outbox_marker_enter(ns, -1, "held", &pin, &e), &e);
    for (size_t i = 0U; i < 2U; ++i)
      REQUIRE(!pthread_create(&threads[i], NULL, outbox_marker_write_thread,
                              &writers[i]));
    size_t attempt;
    for (attempt = 0U; attempt < 1000U; ++attempt) {
      if (ps_outbox_marker_waiter_count(ns, -1, "held") == 2U)
        break;
      usleep(1000);
    }
    REQUIRE(attempt < 1000U);
    /* Both same-name writers are confirmed blocked. Unrelated identities and
     * namespaces must complete ordinary durable publication in that window. */
    ok(lc_pouch_outbox_state_reclaim_marker_write(
           p, "work/a", "other", "other-parent", "checkpoint", &e),
       &e);
    ok(lc_pouch_outbox_state_reclaim_marker_write(
           p, "work/b", "held", "independent-parent", "checkpoint", &e),
       &e);
    char *id = NULL, *parent = NULL, *state = NULL;
    ok(lc_pouch_outbox_state_reclaim_marker_take(p, "work/a", NULL, &id,
                                                 &parent, &state, &e),
       &e);
    REQUIRE(id && !strcmp(id, "other") && parent &&
            !strcmp(parent, "other-parent"));
    lc_free_with_allocator(&p->allocator, id);
    lc_free_with_allocator(&p->allocator, parent);
    lc_free_with_allocator(&p->allocator, state);
    id = parent = state = NULL;
    ps_outbox_marker_leave(ns, pin);
    for (size_t i = 0U; i < 2U; ++i) {
      REQUIRE(!pthread_join(threads[i], NULL));
      ok(writers[i].result, &e);
    }
    REQUIRE(!ps_outbox_marker_waiter_count(ns, -1, "held"));
    ok(lc_pouch_outbox_state_reclaim_marker_take(p, "work/a", NULL, &id,
                                                 &parent, &state, &e),
       &e);
    REQUIRE(
        id && !strcmp(id, "held") && parent &&
        (!strcmp(parent, "first-parent") || !strcmp(parent, "second-parent")));
    lc_free_with_allocator(&p->allocator, id);
    lc_free_with_allocator(&p->allocator, parent);
    lc_free_with_allocator(&p->allocator, state);
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "held", &e),
       &e);
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/a", "other", &e),
       &e);
    ok(lc_pouch_outbox_state_reclaim_marker_remove(p, "work/b", "held", &e),
       &e);
    uint64_t reclaimed, skipped;
    int pending;
    ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, &e), &e);
    ok(ps_catalog_orphans_pending(root->space, &pending, &e), &e);
    REQUIRE(!pending);
    ok(ps_checkpoint(root->space, &e), &e);
    ps_namespace_release(ns);
    lc_pouch_close(p);
    REQUIRE(!live);
    one_file(path);
  }
  lc_error_cleanup(&e);
}
