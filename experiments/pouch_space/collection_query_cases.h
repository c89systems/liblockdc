/* Included by the collection SDK harness. Query pages contain bounded
 * derived metadata; the canonical bodies still flow through public sources. */
#include "budget.h"
#include "lc_collection_query.h"
#include "query_index.h"
#include <zlib.h>

static int cq_build(fixture *f, lc_u64 ordinal) {
  lc_collection_call call;
  int rc = lc_collection_call_begin(f->collection, 0, &call, &f->error);
  if (rc == LC_OK) {
    rc = lc_collection_query_build(&call, ordinal, call.error);
    lc_collection_call_end(&call);
  }
  return rc;
}
static void cq_seed(fixture *f, unsigned int count) {
  for (unsigned int i = 1U; i <= count; ++i) {
    char id[32], json[128];
    lc_source *source = NULL;
    lc_collection_append_res result;
    lc_collection_append_req request = {0};
    REQUIRE(snprintf(id, sizeof(id), "query-%u", i) > 0);
    REQUIRE(snprintf(json, sizeof(json), "{\"kind\":\"%s\",\"n\":%u}",
                     i % 2U ? "other" : "match", i) > 0);
    request.append_id = id;
    ok(lc_source_from_memory(json, strlen(json), &source, &f->error),
       &f->error);
    ok(lc_collection_append(f->collection, &request, source, &result,
                            &f->error),
       &f->error);
    lc_source_close(source);
    REQUIRE(result.position == i && !result.duplicate);
  }
}
static int cq_load_proof(fixture *f, lc_collection_index *index, int *found,
                         const unsigned char *alternate) {
  lc_collection_handle *c = f->collection->impl;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  unsigned char proof[LC_COLLECTION_PAGE_PROOF_BYTES];
  size_t count = 0U;
  int rc = lc_collection_prepare(c, &f->error);
  if (rc == LC_OK)
    rc = lc_collection_order_rows(&c->order, 0U, rows,
                                  LC_COLLECTION_ORDER_PAGE_ROWS, &count, proof,
                                  &f->error);
  if (rc == LC_OK)
    rc = lc_collection_query_load(c, lc_collection_query_epoch(c, &c->order),
                                  0U, lc_collection_query_row_hash(rows, count),
                                  alternate ? alternate : proof, index, found,
                                  &f->error);
  return rc;
}
static int cq_load(fixture *f, lc_collection_index *index, int *found) {
  return cq_load_proof(f, index, found, NULL);
}

static int cq_dummy(void *context, lc_sink *sink, lc_error *e) {
  (void)context;
  if (sink->write(sink, "{}", 2, e))
    return LC_OK;
  return e->code;
}
static void cq_obsolete(fixture *f) {
  lc_collection_handle *c = f->collection->impl;
  char epoch[LC_XID_STRING_SIZE];
  ok(lc_xid_new(epoch, &f->error), &f->error);
  ok(c->queries.replace(c->queries.context, epoch, 0U, 2U, cq_dummy, NULL, NULL,
                        NULL, &f->error),
     &f->error);
}
typedef struct cq_output {
  uint64_t positions;
} cq_output;
static int cq_metadata(void *context, const lc_collection_read_metadata *entry,
                       lc_error *e) {
  cq_output *out = context;
  (void)e;
  if (entry->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    REQUIRE(entry->position && entry->position <= 64U);
    out->positions |= UINT64_C(1) << (entry->position - 1U);
  }
  return LC_OK;
}
static int cq_discard(lc_sink *sink, const void *bytes, size_t size,
                      lc_error *e) {
  (void)sink;
  (void)bytes;
  (void)size;
  (void)e;
  return 1;
}
static void cq_verify(fixture *f, uint64_t expected) {
  lc_collection_read_opts options = {0};
  lc_collection_read_res result = {0};
  cq_output out = {0};
  lc_sink sink = {0};
  sink.write = cq_discard;
  options.selector_lql = "eq{field=/kind,value=match}";
  options.on_metadata = cq_metadata;
  options.metadata_context = &out;
  ok(lc_collection_read(f->collection, &options, &sink, &result, &f->error),
     &f->error);
  if (!result.complete || out.positions != expected)
    fprintf(stderr,
            "query positions=%llx expected=%llx complete=%d records=%llu\n",
            (unsigned long long)out.positions, (unsigned long long)expected,
            result.complete, (unsigned long long)result.completed_records);
  REQUIRE(result.complete && out.positions == expected);
  lc_collection_read_res_cleanup(&result);
}
static uint64_t cq_expected(unsigned int count, int deleted) {
  uint64_t expected = 0U;
  for (unsigned int i = 2U; i <= count; i += 2U)
    expected |= UINT64_C(1) << (i - 1U);
  return deleted ? expected & ~UINT64_C(2) : expected;
}
static int cq_count(void *context, const void *name, size_t length,
                    const ps_catalog_info *info, lc_error *e) {
  size_t *count = context;
  (void)name;
  (void)info;
  (void)e;
  REQUIRE(length == LC_XID_STRING_SIZE - 1U + 8U);
  ++*count;
  return LC_OK;
}
static size_t cq_artifacts(fixture *f, uint64_t owner) {
  size_t count = 0U;
  ok(ps_catalog_visit(client_root(f)->space, owner, PS_CATALOG_PROJECTION,
                      cq_count, &count, &f->error),
     &f->error);
  return count;
}
static unsigned long cq_input_crc(lc_pouch_index_input *input, lc_error *e) {
  unsigned char bytes[2048];
  unsigned long crc = crc32(0L, Z_NULL, 0);
  for (uint64_t at = 0U; at < input->bytes;) {
    size_t count = input->bytes - at < sizeof(bytes)
                       ? (size_t)(input->bytes - at)
                       : sizeof(bytes);
    struct iovec part = {bytes, count};
    ok(lc_pouch_record_io_read(input->io, at, &part, 1, e), e);
    crc = crc32(crc, bytes, (uInt)count);
    at += count;
  }
  return crc;
}
static ps_budget *cq_peer(ps_space *space, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  uint64_t low = 0U, high = PS_DEFAULT_CAPACITY / PS_PAGE;
  plan.maintenance = 1;
  while (low + 1U < high) {
    uint64_t mid = (low + high) / 2U;
    plan.payload_bytes = mid * PS_PAGE;
    int rc = ps_budget_open(space, &plan, &budget, e);
    if (rc == LC_OK) {
      low = mid;
      ok(ps_budget_close(budget, e), e);
    } else {
      REQUIRE(rc == LC_ERR_INVALID);
      clear(e);
      high = mid;
    }
  }
  plan.payload_bytes = low * PS_PAGE;
  ok(ps_budget_open(space, &plan, &budget, e), e);
  return budget;
}
typedef struct cq_parallel {
  gate paused;
  int sibling_done;
} cq_parallel;
typedef struct cq_task {
  lc_collection *collection;
  cq_parallel *state;
  int append, result;
} cq_task;
static int cq_pause(void *context, lc_collection_handle *collection,
                    const char *temporary, lc_error *e) {
  cq_parallel *state = context;
  (void)temporary;
  (void)e;
  if (collection == state->paused.collection->impl) {
    gate_signal(&state->paused, 0);
    gate_wait(&state->paused, 1);
  }
  return LC_OK;
}
static void *cq_run(void *context) {
  cq_task *task = context;
  lc_error e;
  lc_error_init(&e);
  if (task->append) {
    lc_source *source = NULL;
    lc_collection_append_req request = {"complete-page"};
    lc_collection_append_res result;
    task->result = lc_source_from_memory("{}", 2U, &source, &e);
    if (task->result == LC_OK)
      task->result =
          lc_collection_append(task->collection, &request, source, &result, &e);
    lc_source_close(source);
    pthread_mutex_lock(&task->state->paused.mutex);
    task->state->sibling_done = 1;
    pthread_cond_broadcast(&task->state->paused.condition);
    pthread_mutex_unlock(&task->state->paused.mutex);
  } else {
    lc_collection_call call;
    task->result = lc_collection_call_begin(task->collection, 0, &call, &e);
    if (task->result == LC_OK) {
      task->result = lc_collection_query_build(&call, 0U, &e);
      lc_collection_call_end(&call);
    }
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void cq_concurrency(const char *directory, int codec, int sync) {
  fixture f;
  cq_parallel state = {0};
  cq_task tasks[2] = {{0}, {0}};
  pthread_t threads[2];
  struct timespec until;
  lc_collection_index index = {0};
  int found, rc = 0;
  setup(&f, directory, codec, sync, 0);
  cq_seed(&f, LC_COLLECTION_ORDER_PAGE_ROWS - 1U);
  REQUIRE(!pthread_mutex_init(&state.paused.mutex, NULL));
  REQUIRE(!pthread_cond_init(&state.paused.condition, NULL));
  state.paused.collection = f.collection;
  lc_collection_query_test_context = &state;
  lc_collection_query_test_hook = cq_pause;
  for (int i = 0; i < 2; ++i) {
    tasks[i].collection = i ? f.peer : f.collection;
    tasks[i].state = &state;
    tasks[i].append = i;
  }
  REQUIRE(!pthread_create(&threads[0], NULL, cq_run, &tasks[0]));
  gate_wait(&state.paused, 0);
  REQUIRE(!pthread_create(&threads[1], NULL, cq_run, &tasks[1]));
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &until));
  until.tv_sec += 5;
  pthread_mutex_lock(&state.paused.mutex);
  while (!state.sibling_done && !rc)
    rc = pthread_cond_timedwait(&state.paused.condition, &state.paused.mutex,
                                &until);
  int independent = state.sibling_done;
  state.paused.released = 1;
  pthread_cond_broadcast(&state.paused.condition);
  pthread_mutex_unlock(&state.paused.mutex);
  REQUIRE(!pthread_join(threads[0], NULL));
  REQUIRE(!pthread_join(threads[1], NULL));
  lc_collection_query_test_hook = NULL;
  lc_collection_query_test_context = NULL;
  REQUIRE(independent && tasks[0].result == LC_OK && tasks[1].result == LC_OK);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found && index.rows == LC_COLLECTION_ORDER_PAGE_ROWS);
  lc_collection_index_cleanup(&index);
  REQUIRE(!pthread_cond_destroy(&state.paused.condition));
  REQUIRE(!pthread_mutex_destroy(&state.paused.mutex));
  close_store(&f);
  REQUIRE(!unlink(f.path));
  lc_error_cleanup(&f.error);
}
static int cq_decline(void *context, lc_collection_handle *collection,
                      const char *temporary, lc_error *e) {
  int *called = context;
  (void)collection;
  (void)temporary;
  ++*called;
  return lc_error_set(e, LC_ERR_NOMEM, 0L,
                      "injected collection index producer failure", NULL, NULL,
                      "test");
}
static void cq_optional_failure(const char *directory, int codec, int sync) {
  fixture f;
  lc_source *source = NULL;
  lc_collection_append_req request = {"committed-without-index"};
  lc_collection_append_res appended;
  lc_collection_append_status_res status;
  lc_collection_index index = {0};
  int called = 0, found;
  setup(&f, directory, codec, sync, 0);
  cq_seed(&f, LC_COLLECTION_ORDER_PAGE_ROWS - 1U);
  lc_collection_query_test_context = &called;
  lc_collection_query_test_hook = cq_decline;
  ok(lc_source_from_memory("{}", 2U, &source, &f.error), &f.error);
  ok(lc_collection_append(f.peer, &request, source, &appended, &f.error),
     &f.error);
  lc_source_close(source);
  lc_collection_query_test_hook = NULL;
  lc_collection_query_test_context = NULL;
  REQUIRE(called == 1 && appended.position == LC_COLLECTION_ORDER_PAGE_ROWS &&
          f.error.code == LC_OK);
  ok(lc_collection_append_status(f.collection, request.append_id, &status,
                                 &f.error),
     &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_COMMITTED);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(!found);
  lc_collection_index_cleanup(&index);
  cq_verify(&f, cq_expected(LC_COLLECTION_ORDER_PAGE_ROWS - 1U, 0));
  ok(ps_check(client_root(&f)->space, &f.error), &f.error);
  ok(ps_pouch_collection_maintain(f.collection, 0, 0, NULL, &f.error),
     &f.error);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found && index.rows == LC_COLLECTION_ORDER_PAGE_ROWS);
  lc_collection_index_cleanup(&index);
  close_store(&f);
  REQUIRE(!unlink(f.path));
  lc_error_cleanup(&f.error);
}
static void collection_queries(const char *directory, int codec, int sync) {
  fixture f;
  lc_collection_index index = {0}, stale = {0};
  lc_collection_delete_res deleted;
  lc_pouch_index_input pinned = {0};
  char recovery_id[LC_XID_STRING_SIZE];
  int found = 0;
  setup(&f, directory, codec, sync, 0);
  cq_seed(&f, LC_COLLECTION_ORDER_PAGE_ROWS);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found && index.rows == LC_COLLECTION_ORDER_PAGE_ROWS);
  lc_collection_index_cleanup(&index);
  cq_verify(&f, cq_expected(LC_COLLECTION_ORDER_PAGE_ROWS, 0));
  lc_collection_handle *c = f.collection->impl;
  uint64_t owner = collection_owner_id(&f);
  REQUIRE(cq_artifacts(&f, owner) == 1U);
  ok(c->queries.open(c->queries.context,
                     lc_collection_query_epoch(c, &c->order), 0U, &pinned,
                     &f.error),
     &f.error);
  REQUIRE(pinned.signature.present);
  unsigned long crc = cq_input_crc(&pinned, &f.error);
  ok(lc_collection_delete_entry(f.collection, 2U, &deleted, &f.error),
     &f.error);
  lc_pouch_maintenance_options maintenance = {0};
  maintenance.ns = "journal";
  c->pouch->compaction_min_reclaimable_bytes = LC_U64_MAX;
  ok(lc_pouch_maintenance_run(c->pouch, &maintenance, NULL, &f.error),
     &f.error);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found && !(index.covered & UINT64_C(2)));
  lc_collection_index_cleanup(&index);
  cq_verify(&f, cq_expected(LC_COLLECTION_ORDER_PAGE_ROWS, 1));
  ok(ps_checkpoint(client_root(&f)->space, &f.error), &f.error);
  REQUIRE(cq_input_crc(&pinned, &f.error) == crc);
  c->queries.close(c->queries.context, &pinned);
  ps_budget *peer = cq_peer(client_root(&f)->space, &f.error);
  REQUIRE(cq_build(&f, 0U) != LC_OK);
  clear(&f.error);
  ok(ps_check(client_root(&f)->space, &f.error), &f.error);
  ok(ps_budget_close(peer, &f.error), &f.error);
  ok(cq_build(&f, 0U), &f.error);
  ok(cq_load(&f, &stale, &found), &f.error);
  REQUIRE(found);
  memcpy(recovery_id, c->order.query_id, sizeof(recovery_id));
  cq_obsolete(&f);
  reopen(&f);
  c = f.collection->impl;
  REQUIRE(strcmp(recovery_id, c->order.query_id));
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found && !memcmp(index.page_proof, stale.page_proof,
                           sizeof(index.page_proof)));
  lc_collection_index_cleanup(&index);
  cq_verify(&f, cq_expected(LC_COLLECTION_ORDER_PAGE_ROWS, 1));
  /* The same ordinals, row count and CRC cannot reuse a page from different
   * payloads, even after a recovery has reused its positions. */
  stale.page_proof[0] ^= 1U;
  ok(cq_load_proof(&f, &index, &found, stale.page_proof), &f.error);
  REQUIRE(!found);
  ok(lc_collection_query_store(c, &stale, &f.error), &f.error);
  ok(cq_load(&f, &index, &found), &f.error);
  REQUIRE(found);
  lc_collection_index_cleanup(&index);
  lc_collection_index_cleanup(&stale);
  REQUIRE(cq_artifacts(&f, owner) == 2U);
  ok(lc_collection_query_cleanup(f.collection, NULL, &f.error), &f.error);
  REQUIRE(cq_artifacts(&f, owner) == 1U);
  cq_verify(&f, cq_expected(LC_COLLECTION_ORDER_PAGE_ROWS, 1));
  ok(lc_collection_drop(f.collection, &f.error), &f.error);
  lc_collection_close(f.collection);
  lc_collection_close(f.peer);
  f.collection = f.peer = NULL;
  uint64_t reclaimed = 0U, skipped = 0U;
  ok(ps_root_reclaim_orphans(client_root(&f), &reclaimed, &skipped, &f.error),
     &f.error);
  REQUIRE(!cq_artifacts(&f, owner));
  ok(ps_checkpoint(client_root(&f)->space, &f.error), &f.error);
  close_store(&f);
  REQUIRE(!unlink(f.path));
  lc_error_cleanup(&f.error);
  cq_concurrency(directory, codec, sync);
  cq_optional_failure(directory, codec, sync);
}
static int cq_action(fixture *f, int action) {
  if (action == 1)
    return lc_collection_query_maintain(f->collection, &f->error);
  if (action == 2)
    return lc_collection_query_cleanup(f->collection, NULL, &f->error);
  return cq_build(f, 0U);
}
static size_t cq_trial_run(const char *directory, int codec, int sync,
                           int action, int allocating, int mode, size_t cut) {
  fixture f;
  disk_model m = {0};
  lc_collection_delete_res deleted;
  m.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
  m.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
  m.scratch = malloc(PS_PAGE);
  REQUIRE(m.stable && m.dirty && m.scratch);
  ps_io io = {&m, disk_read, disk_write, disk_sync, disk_reserve, NULL};
  fixture_directory_sync_model(1);
  setup_io(&f, directory, codec, sync, 0, &io);
  cq_seed(&f, 7U);
  if (action) {
    ok(cq_build(&f, 0U), &f.error);
    if (action == 1)
      ok(lc_collection_delete_entry(f.collection, 2U, &deleted, &f.error),
         &f.error);
    else {
      cq_obsolete(&f);
      reopen(&f);
    }
  }
  ok(ps_flush(client_root(&f)->space, &f.error), &f.error);
  m.mode = mode;
  m.cut = cut;
  m.calls = 0U;
  allocation_calls = 0U;
  allocation_fail = cut;
  allocation_fired = 0;
  allocation_thread = pthread_self();
  if (allocating)
    __atomic_store_n(&allocation_armed, 1, __ATOMIC_RELEASE);
  else
    m.armed = 1;
  int rc = cq_action(&f, action);
  m.armed = 0;
  __atomic_store_n(&allocation_armed, 0, __ATOMIC_RELEASE);
  size_t calls = allocating ? allocation_calls : m.calls;
  if (!cut ? rc != LC_OK : allocating ? !allocation_fired : !m.fired)
    fprintf(stderr,
            "collection query action=%d allocation=%d mode=%d cut=%zu "
            "calls=%zu rc=%d\n",
            action, allocating, mode, cut, calls, rc);
  REQUIRE(!cut ? rc == LC_OK : allocating ? allocation_fired : m.fired);
  clear(&f.error);
  m.freeze = 1;
  close_store(&f);
  if (!allocating)
    restore_disk(&m, f.path);
  io = ps_native_io;
  io.sync = fixture_complete_sync;
  f.io = &io;
  open_client(&f, 0);
  open_collection(&f, 0, &f.collection);
  open_collection(&f, 0, &f.peer);
  cq_verify(&f, cq_expected(7U, action == 1));
  ok(cq_build(&f, 0U), &f.error);
  cq_verify(&f, cq_expected(7U, action == 1));
  ok(lc_collection_query_cleanup(f.collection, NULL, &f.error), &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  REQUIRE(!unlink(f.path));
  free(m.stable);
  free(m.dirty);
  free(m.scratch);
  fixture_directory_sync_model(0);
  return calls;
}
typedef struct cq_trial_context {
  const char *directory;
  int codec, sync, action, allocating, mode;
  size_t cut, calls;
} cq_trial_context;
static void *cq_trial_worker(void *context) {
  cq_trial_context *trial = context;
  trial->calls =
      cq_trial_run(trial->directory, trial->codec, trial->sync, trial->action,
                   trial->allocating, trial->mode, trial->cut);
  return NULL;
}
static size_t cq_trial(const char *directory, int codec, int sync, int action,
                       int allocating, int mode, size_t cut) {
  cq_trial_context trial = {directory,  codec, sync, action,
                            allocating, mode,  cut,  0U};
  pthread_t worker;
  /* The JSON extractor owns a per-thread first-use cache. Every trial starts
   * with the same cache state, including its initialization allocation. */
  REQUIRE(!pthread_create(&worker, NULL, cq_trial_worker, &trial));
  REQUIRE(!pthread_join(worker, NULL));
  return trial.calls;
}
static void collection_query_faults(const char *directory, int codec, int sync,
                                    int allocating) {
  size_t cuts = 0U;
  const char *only = getenv("POUCH_COLLECTION_QUERY_CUT");
  for (int action = 0; action < 3; ++action) {
    size_t calls = cq_trial(directory, codec, sync, action, allocating, 0, 0U);
    REQUIRE(calls);
    for (int mode = 0; mode < (allocating ? 1 : 4); ++mode)
      for (size_t cut = 1U; cut <= calls; ++cut) {
        if (only && cut != (size_t)strtoul(only, NULL, 10))
          continue;
        (void)cq_trial(directory, codec, sync, action, allocating, mode, cut);
        ++cuts;
      }
  }
  printf("collection query codec=%d durable=%d allocation=%d cuts=%zu passed\n",
         codec, sync, allocating, cuts);
}
