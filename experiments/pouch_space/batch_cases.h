#ifndef POUCH_SPACE_BATCH_CASES_H
#define POUCH_SPACE_BATCH_CASES_H
#include "lc_pouch_storage.h"
typedef struct batch_view {
  unsigned char *frames[2];
  size_t bytes[2], count, calls, fail;
  gate *gate;
} batch_view;
static int batch_prefix(void *context, size_t index, lc_pouch_record_header *h,
                        const void **key, const void **metadata, lc_error *e) {
  batch_view *v = context;
  REQUIRE(index < v->count);
  ++v->calls;
  if (v->gate && index == 1) {
    pthread_mutex_lock(&v->gate->mutex);
    v->gate->entered = 1;
    pthread_cond_broadcast(&v->gate->changed);
    while (!v->gate->released)
      pthread_cond_wait(&v->gate->changed, &v->gate->mutex);
    pthread_mutex_unlock(&v->gate->mutex);
  }
  if (v->fail && v->calls == v->fail)
    return lc_error_set(e, LC_ERR_INVALID, 0, "prefix observer stopped", NULL,
                        NULL, "test");
  int rc = lc_pouch_record_header_decode(v->frames[index], h, e);
  if (rc == LC_OK) {
    REQUIRE(LC_POUCH_RECORD_HEADER_BYTES + h->key_len + h->meta_len +
                h->payload_len ==
            v->bytes[index]);
    *key = v->frames[index] + LC_POUCH_RECORD_HEADER_BYTES;
    *metadata = (const unsigned char *)*key + h->key_len;
  }
  return rc;
}
static ps_budget *capacity_peer(ps_space *space, int maintenance, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *b = NULL;
  uint64_t low = 0, high = PS_DEFAULT_CAPACITY / PS_PAGE;
  plan.maintenance = maintenance;
  while (low + 1 < high) {
    uint64_t mid = (low + high) / 2;
    plan.payload_bytes = mid * PS_PAGE;
    int rc = ps_budget_open(space, &plan, &b, e);
    if (rc == LC_OK) {
      low = mid;
      ok(ps_budget_close(b, e), e);
    } else {
      REQUIRE(rc == LC_ERR_INVALID);
      clear(e);
      high = mid;
    }
  }
  plan.payload_bytes = low * PS_PAGE;
  ok(ps_budget_open(space, &plan, &b, e), e);
  return b;
}
typedef struct batch_observer {
  ps_namespace *ns;
  unsigned int mask;
} batch_observer;
static int batch_visit(void *context, uint64_t segment,
                       const lc_pouch_state_entry *r, lc_error *e) {
  batch_observer *observer = context;
  unsigned int *mask = &observer->mask;
  if (r->control) {
    REQUIRE(r->record_type == LC_POUCH_STATE_RECORD_HIGH_WATER);
    return LC_OK;
  }
  REQUIRE(segment <= 1 && r->found && r->payload_span.present &&
          r->bytes == 31 && r->version == r->index_seq);
  if (!strcmp(r->key, "key")) {
    REQUIRE(r->version == 1 && !(*mask & 1));
    *mask |= 1;
  } else {
    REQUIRE(!strcmp(r->key, "alt") && r->version == 2 && !(*mask & 2));
    *mask |= 2;
  }
  ps_namespace_window *reader = NULL;
  unsigned char bytes[31];
  struct iovec part = {bytes, sizeof(bytes)};
  ok(ps_namespace_read_open(observer->ns, PS_CATALOG_SEGMENT, 1,
                            r->payload_span.payload_offset,
                            r->payload_span.payload_length, &reader, e),
     e);
  ok(lc_pouch_record_io_read(ps_namespace_read_input(reader),
                             r->payload_span.payload_offset, &part, 1, e),
     e);
  REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
  ps_namespace_read_close(reader);
  return LC_OK;
}
static void batch_readback(ps_namespace *ns, lc_error *e) {
  batch_observer observer = {ns, 0};
  ps_namespace_view *view = NULL;
  ok(ps_namespace_view_open(ns, &view, e), e);
  ok(ps_namespace_view_visit(view, 1, batch_visit, &observer, e), e);
  ps_namespace_view_close(view);
  REQUIRE(observer.mask == 3);
}
static void batch_rebase(unsigned char *frame, uint64_t offset, lc_error *e) {
  lc_pouch_record_header h;
  ok(lc_pouch_record_header_decode(frame, &h, e), e);
  unsigned char *meta = frame + LC_POUCH_RECORD_HEADER_BYTES + h.key_len;
  put64(meta + 32, offset);
  put64(meta + 40,
        offset + LC_POUCH_RECORD_HEADER_BYTES + h.key_len + h.meta_len);
}
typedef struct batch_job {
  ps_namespace *ns;
  const lc_pouch_storage_batch *batch;
  uint64_t minimum;
  ps_namespace_append *append;
  int rc;
} batch_job;
static void *batch_begin(void *context) {
  batch_job *job = context;
  lc_error e;
  lc_error_init(&e);
  job->rc =
      ps_namespace_append_begin(job->ns, 0, job->minimum, PS_APPEND_COMPLETE,
                                job->batch, &job->append, &e);
  if (job->rc != LC_OK)
    fprintf(stderr, "batch admission: %s\n", e.message ? e.message : "?");
  lc_error_cleanup(&e);
  return NULL;
}
static void batch_admission(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options options = {0};
    options.storage.durable_sync = sync;
    ps_root *root = NULL;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_space *space = ps_root_space(root);
    ps_namespace *fixture = acquire(root, "fixture", 1, &e);
    batch_view prefixes = {0};
    prefixes.count = 2;
    prefixes.frames[0] = append_frame(root, fixture, 1, &prefixes.bytes[0], &e);
    prefixes.frames[1] = append_frame(root, fixture, 2, &prefixes.bytes[1], &e);
    memcpy(prefixes.frames[1] + LC_POUCH_RECORD_HEADER_BYTES, "alt", 3);
    batch_rebase(prefixes.frames[0], PS_FRAME_BYTES, &e);
    batch_rebase(prefixes.frames[1], PS_FRAME_BYTES + prefixes.bytes[0], &e);
    ok(ps_namespace_checkpoint_flush(fixture, &e), &e);
    uint64_t minimum = prefixes.bytes[0] + prefixes.bytes[1];
    lc_pouch_storage_batch batch = {2, batch_prefix, &prefixes};
    ps_namespace *ns = acquire(root, "planned", 1, &e);
    ps_namespace *reject = acquire(root, "rejected", 1, &e);
    ps_namespace_append *a = NULL;
    for (int fault = 0; fault < 2; ++fault) {
      prefixes.calls = 0;
      prefixes.fail = fault ? 2 : 0;
      REQUIRE(ps_namespace_append_begin(reject, 0, minimum - (fault ? 0 : 1),
                                        PS_APPEND_COMPLETE, &batch, &a,
                                        &e) == LC_ERR_INVALID &&
              !a);
      clear(&e);
      ps_namespace_status status;
      ok(ps_namespace_status_get(reject, &status, &e), &e);
      REQUIRE(!status.active_segment && !status.tail &&
              !status.bound_segments && !status.pending_metadata_bytes);
    }
    prefixes.calls = prefixes.fail = 0;
    gate held = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
    prefixes.gate = &held;
    batch_job job = {ns, &batch, minimum, NULL, 0};
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, batch_begin, &job));
    pthread_mutex_lock(&held.mutex);
    while (!held.entered)
      pthread_cond_wait(&held.changed, &held.mutex);
    pthread_mutex_unlock(&held.mutex);
    /* Preflight borrows the producer's prefixes outside root/metadata locks.
     * An unrelated writer and a selected source reader must both progress. */
    ok(append(root, reject, 1, 0, 31, &e), &e);
    count(reject, 1, &e);
    ps_namespace_window *reader = NULL;
    uint64_t offset = prefixes.bytes[0] - 31;
    unsigned char bytes[31];
    struct iovec part = {bytes, sizeof(bytes)};
    ok(ps_namespace_read_open(fixture, PS_CATALOG_SEGMENT, 1, offset, 31,
                              &reader, &e),
       &e);
    ok(lc_pouch_record_io_read(ps_namespace_read_input(reader), offset, &part,
                               1, &e),
       &e);
    REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
    ps_namespace_read_close(reader);
    pthread_mutex_lock(&held.mutex);
    held.released = 1;
    pthread_cond_broadcast(&held.changed);
    pthread_mutex_unlock(&held.mutex);
    REQUIRE(!pthread_join(thread, NULL) && job.rc == LC_OK);
    a = job.append;
    prefixes.gate = NULL;
    REQUIRE(!pthread_cond_destroy(&held.changed));
    REQUIRE(!pthread_mutex_destroy(&held.mutex));
    ps_namespace_release(fixture);
    REQUIRE(prefixes.calls == 2);
    /* Callback lifetime ends at admission. Actual prefixes must use only the
     * funded key/metadata ceilings even after the ordinary floor is occupied.
     */
    prefixes.fail = 1;
    ps_budget *peer = capacity_peer(space, 1, &e);
    ok(ps_checkpoint(space, &e), &e);
    ps_namespace_append *denied = NULL;
    REQUIRE(ps_namespace_append_begin(reject, 0, minimum, PS_APPEND_COMPLETE,
                                      &batch, &denied, &e) == LC_ERR_INVALID &&
            !denied);
    clear(&e);
    prefixes.calls = 0;
    lc_pouch_record_io output = ps_namespace_append_output(a);
    for (size_t i = 0; i < prefixes.count; ++i) {
      size_t at = append_fixture_prefix(output, prefixes.frames[i],
                                        prefixes.bytes[i], &e);
      append_fixture_body(output, prefixes.frames[i], at, prefixes.bytes[i],
                          &e);
    }
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    REQUIRE(!prefixes.calls);
    ps_namespace_append_close(a);
    /* Existing grant bytes and key debt can make the compound delta empty.
     * A new ordinary frame must still honor the foreground floor. */
    prefixes.fail = 0;
    lc_pouch_storage_batch repeat = {1, batch_prefix, &prefixes};
    REQUIRE(ps_namespace_append_begin(ns, 0, prefixes.bytes[0],
                                      PS_APPEND_COMPLETE, &repeat, &denied,
                                      &e) == LC_ERR_INVALID &&
            !denied);
    clear(&e);
    batch_readback(ns, &e);
    ok(ps_namespace_checkpoint_flush(ns, &e), &e);
    batch_readback(ns, &e);
    ok(ps_budget_close(peer, &e), &e);
    ps_namespace *drift = acquire(root, "drift", 1, &e);
    ok(ps_namespace_append_begin(drift, 0, prefixes.bytes[0],
                                 PS_APPEND_COMPLETE, &repeat, &a, &e),
       &e);
    output = ps_namespace_append_output(a);
    lc_pouch_record_header h;
    ok(lc_pouch_record_header_decode(prefixes.frames[0], &h, &e), &e);
    struct iovec changed[3] = {
        {prefixes.frames[0], LC_POUCH_RECORD_HEADER_BYTES},
        {(void *)"bad", 3},
        {prefixes.frames[0] + LC_POUCH_RECORD_HEADER_BYTES + h.key_len,
         h.meta_len}};
    uint64_t tail, unchanged;
    ok(lc_pouch_record_io_tail(output, &tail, &e), &e);
    REQUIRE(lc_pouch_record_io_append(output, changed, 3, &e) ==
            LC_ERR_PROTOCOL);
    clear(&e);
    ok(lc_pouch_record_io_tail(output, &unchanged, &e), &e);
    REQUIRE(tail == unchanged);
    ps_namespace_append_close(a);
    ps_namespace_status status;
    ok(ps_namespace_status_get(drift, &status, &e), &e);
    REQUIRE(!status.active_segment && !status.tail &&
            !status.pending_metadata_bytes);
    /* The failed private frame releases its claims and remains retryable. */
    ok(ps_namespace_append_begin(drift, 0, prefixes.bytes[0],
                                 PS_APPEND_COMPLETE, &repeat, &a, &e),
       &e);
    output = ps_namespace_append_output(a);
    size_t at = append_fixture_prefix(output, prefixes.frames[0],
                                      prefixes.bytes[0], &e);
    append_fixture_body(output, prefixes.frames[0], at, prefixes.bytes[0], &e);
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(a);
    count(drift, 1, &e);
    ps_namespace_release(drift);
    ps_namespace_release(reject);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "planned", 0, &e);
    batch_readback(ns, &e);
    ps_namespace_release(ns);
    drift = acquire(root, "drift", 0, &e);
    count(drift, 1, &e);
    ps_namespace_release(drift);
    ok(ps_root_close(root, &e), &e);
    free(prefixes.frames[0]);
    free(prefixes.frames[1]);
    REQUIRE(!unlink(path));
  }
  lc_error_cleanup(&e);
}
static void budget_transfer(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options options = {0};
    options.storage.durable_sync = sync;
    ps_root *root = NULL;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_space *space = ps_root_space(root);
    ps_budget_plan unit = {0}, plan = {0}, empty = {0}, remaining;
    ps_budget *source = NULL, *destination = NULL;
    ok(ps_budget_plan_value(&unit, 64, &e), &e);
    ok(ps_budget_plan_add(&plan, &unit, &e), &e);
    ok(ps_budget_plan_add(&plan, &unit, &e), &e);
    ok(ps_budget_open(space, &plan, &source, &e), &e);
    empty.maintenance = 1;
    ok(ps_budget_open(space, &empty, &destination, &e), &e);
    ps_budget *peer = capacity_peer(space, 1, &e);
    ps_stats before, after;
    ok(ps_status(space, &before, &e), &e);
    allocation = 0;
    fail_allocation = 1;
    int rc = ps_budget_transfer(source, destination, &unit, &e);
    fail_allocation = 0;
    ok(rc, &e);
    REQUIRE(!allocation);
    ok(ps_status(space, &after, &e), &e);
    REQUIRE(before.reserved_payload_bytes == after.reserved_payload_bytes &&
            before.reserved_metadata_bytes == after.reserved_metadata_bytes &&
            before.reserved_entries == after.reserved_entries &&
            before.metadata_workspace_bytes == after.metadata_workspace_bytes &&
            before.journal_bytes == after.journal_bytes &&
            before.checkpoints == after.checkpoints &&
            before.syncs == after.syncs);
    REQUIRE(ps_budget_transfer(destination, source, &unit, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_budget_transfer(source, source, &unit, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_budget_transfer(source, destination, &plan, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ps_writer *writer = NULL;
    ps_key first = {99, 1, PS_STATE}, second = {99, 2, PS_STATE};
    ok(ps_begin(space, source, first, &writer, &e), &e);
    REQUIRE(ps_budget_transfer(source, destination, &unit, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ps_writer_close(writer);
    writer = NULL;
    ok(ps_begin(space, destination, first, &writer, &e), &e);
    ok(ps_budget_transfer(source, destination, &unit, &e), &e);
    ok(ps_budget_status(source, &remaining, &e), &e);
    REQUIRE(!remaining.payload_bytes && !remaining.metadata_bytes &&
            !remaining.entries);
    ok(ps_write(writer, body, 64, &e), &e);
    ok(ps_publish(writer, NULL, &e), &e);
    ps_writer_close(writer);
    writer = NULL;
    ok(ps_begin(space, destination, second, &writer, &e), &e);
    ok(ps_write(writer, body, 64, &e), &e);
    ok(ps_publish(writer, NULL, &e), &e);
    ps_writer_close(writer);
    ok(ps_budget_close(source, &e), &e);
    ok(ps_budget_close(destination, &e), &e);
    ok(ps_budget_close(peer, &e), &e);
    ok(ps_flush(space, &e), &e);
    for (size_t key = 1; key <= 2; ++key) {
      ps_reader *r = NULL;
      ps_key k = {99, key, PS_STATE};
      unsigned char bytes[64];
      size_t got;
      ok(ps_read_open(space, k, NULL, &r, &e), &e);
      ok(ps_read(r, bytes, sizeof(bytes), &got, &e), &e);
      REQUIRE(got == sizeof(bytes) && !memcmp(bytes, body, sizeof(bytes)));
      ps_reader_close(r);
    }
    ok(ps_root_close(root, &e), &e);
    REQUIRE(!unlink(path));
  }
  lc_error_cleanup(&e);
}
#endif
