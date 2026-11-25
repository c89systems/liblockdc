#ifndef POUCH_SPACE_INDEX_CONCURRENCY_CASES_H
#define POUCH_SPACE_INDEX_CONCURRENCY_CASES_H
#include "index_cases.h"
#include "owned.h"

static int index_capture_binding(void *context, const void *name, size_t length,
                                 const ps_catalog_info *info, lc_error *e) {
  ps_catalog_info *captured = context;
  (void)name;
  (void)e;
  REQUIRE(length == 10U && !captured->stream.owner);
  *captured = *info;
  return LC_OK;
}

typedef struct index_gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int entered, release, sibling_done, armed, peer_payload;
} index_gate;
static void index_gate_init(index_gate *gate) {
  memset(gate, 0, sizeof(*gate));
  REQUIRE(!pthread_mutex_init(&gate->mutex, NULL));
  REQUIRE(!pthread_cond_init(&gate->changed, NULL));
}
static void index_gate_clear(index_gate *gate) {
  REQUIRE(!pthread_cond_destroy(&gate->changed));
  REQUIRE(!pthread_mutex_destroy(&gate->mutex));
}
static void index_gate_wait(index_gate *gate) {
  struct timespec deadline;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
  deadline.tv_sec += 5;
  pthread_mutex_lock(&gate->mutex);
  int rc = 0;
  while (!gate->entered && !rc)
    rc = pthread_cond_timedwait(&gate->changed, &gate->mutex, &deadline);
  int entered = gate->entered;
  pthread_mutex_unlock(&gate->mutex);
  REQUIRE(entered);
}
static int index_gate_release(index_gate *gate) {
  struct timespec deadline;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
  deadline.tv_sec += 5;
  pthread_mutex_lock(&gate->mutex);
  int rc = 0;
  while (!gate->sibling_done && !rc)
    rc = pthread_cond_timedwait(&gate->changed, &gate->mutex, &deadline);
  int independent = gate->sibling_done;
  gate->release = 1;
  pthread_cond_broadcast(&gate->changed);
  pthread_mutex_unlock(&gate->mutex);
  return independent;
}
static int index_paused_producer(void *context, lc_sink *sink, lc_error *e) {
  index_gate *gate = context;
  REQUIRE(sink->write(sink, "new ", 4U, e));
  pthread_mutex_lock(&gate->mutex);
  gate->entered = 1;
  pthread_cond_broadcast(&gate->changed);
  while (!gate->release)
    pthread_cond_wait(&gate->changed, &gate->mutex);
  pthread_mutex_unlock(&gate->mutex);
  REQUIRE(sink->write(sink, "tail", 4U, e));
  return LC_OK;
}
static int index_small_producer(void *context, lc_sink *sink, lc_error *e) {
  const char *bytes = context;
  return sink->write(sink, bytes, strlen(bytes), e) ? LC_OK : e->code;
}
typedef struct index_actor {
  lc_client *client;
  ps_root *root;
  index_gate *gate;
  lc_pouch_index_artifact_id id;
  int sibling, result, flush;
} index_actor;
static void *index_actor_run(void *context) {
  index_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  if (actor->flush) {
    if (actor->sibling) {
      index_put(actor->client, "two",
                "{\"kind\":\"active\",\"rank\":2,\"title\":\"audit sibling\"}",
                &e);
      actor->result = LC_OK;
    } else {
      lc_pouch_generation sequence = 0U;
      lc_pouch_query_index_flush_result result = {0};
      actor->result = lc_pouch_state_query_index_seq(
          index_pouch(actor->client), index_namespace, &sequence, &e);
      if (actor->result == LC_OK)
        actor->result = lc_pouch_query_index_flush(
            index_pouch(actor->client), index_namespace, sequence, &result, &e);
    }
  } else
    actor->result = ps_query_index_store_ops.replace(
        actor->root, "query/concurrent", &actor->id, 8U,
        actor->sibling ? index_small_producer : index_paused_producer,
        actor->sibling ? (void *)"sibling" : (void *)actor->gate, 0, 0, &e);
  if (actor->sibling) {
    pthread_mutex_lock(&actor->gate->mutex);
    actor->gate->sibling_done = 1;
    pthread_cond_broadcast(&actor->gate->changed);
    pthread_mutex_unlock(&actor->gate->mutex);
  }
  lc_error_cleanup(&e);
  return NULL;
}
static int index_gated_sync(void *context, int fd) {
  index_gate *gate = context;
  pthread_mutex_lock(&gate->mutex);
  if (gate->armed && !gate->entered) {
    gate->entered = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->release)
      pthread_cond_wait(&gate->changed, &gate->mutex);
  }
  pthread_mutex_unlock(&gate->mutex);
  return ps_native_io.sync(NULL, fd);
}
static ssize_t index_gate_read(void *context, int fd, void *bytes, size_t size,
                               off_t offset) {
  (void)context;
  return pread(fd, bytes, size, offset);
}
static ssize_t index_gate_write(void *context, int fd, const void *bytes,
                                size_t size, off_t offset) {
  index_gate *gate = context;
  ssize_t result = pwrite(fd, bytes, size, offset);
  pthread_mutex_lock(&gate->mutex);
  if (gate->armed && gate->entered && !gate->release && offset >= PS_DATA &&
      result == (ssize_t)size) {
    gate->peer_payload = 1;
    pthread_cond_broadcast(&gate->changed);
  }
  pthread_mutex_unlock(&gate->mutex);
  return result;
}
static int index_gate_reserve(void *context, int fd, off_t offset, off_t size) {
  (void)context;
  return ps_native_io.reserve(NULL, fd, offset, size);
}
static void index_raw_expect(ps_root *root,
                             const lc_pouch_index_artifact_id *id,
                             const char *expected, lc_error *e) {
  lc_pouch_index_input input = {0};
  unsigned char bytes[16] = {0};
  ok(ps_query_index_store_ops.open(root, "query/concurrent", id, &input, e), e);
  REQUIRE(input.signature.present && input.bytes == strlen(expected));
  struct iovec part = {bytes, (size_t)input.bytes};
  ok(lc_pouch_record_io_read(input.io, 0U, &part, 1, e), e);
  REQUIRE(!memcmp(bytes, expected, (size_t)input.bytes));
  ps_query_index_store_ops.close(root, &input);
}
typedef struct index_visibility_actor {
  ps_namespace *ns;
  uint64_t owner;
  index_gate gate;
  int result;
} index_visibility_actor;
static int index_selected_pause(void *context, lc_pouch_index_commit_fn commit,
                                void *publication, lc_error *e) {
  index_gate *gate = context;
  int rc = commit(publication, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&gate->mutex);
  gate->entered = 1;
  pthread_cond_broadcast(&gate->changed);
  while (!gate->release)
    pthread_cond_wait(&gate->changed, &gate->mutex);
  pthread_mutex_unlock(&gate->mutex);
  return LC_OK;
}
static void *index_visibility_publish(void *context) {
  index_visibility_actor *actor = context;
  const unsigned char name[10] = {1U, LC_POUCH_INDEX_PACKED, 1U};
  lc_error e;
  lc_error_init(&e);
  actor->result = ps_query_artifact_replace(
      actor->ns, actor->owner, name, sizeof(name), 7U, index_small_producer,
      (void *)"visible", index_selected_pause, &actor->gate, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void index_visibility(const char *directory) {
  char path[1400];
  ps_root *root = NULL;
  ps_root_options options = {0};
  ps_namespace_info identity;
  index_visibility_actor actor = {0};
  const unsigned char name[10] = {1U, LC_POUCH_INDEX_PACKED, 1U};
  lc_pouch_index_input input = {0};
  pthread_t thread;
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-visible", directory) <
          (int)sizeof(path));
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ok(ps_root_namespace_acquire(root, "query/visible", 1, &actor.ns, &e), &e);
  ok(ps_namespace_identity(actor.ns, &identity, &e), &e);
  actor.owner = identity.owner;
  index_gate_init(&actor.gate);
  REQUIRE(!pthread_create(&thread, NULL, index_visibility_publish, &actor));
  index_gate_wait(&actor.gate);
  int opened = ps_query_artifact_open(actor.ns, actor.owner, name, sizeof(name),
                                      &input, &e);
  pthread_mutex_lock(&actor.gate.mutex);
  actor.gate.release = 1;
  pthread_cond_broadcast(&actor.gate.changed);
  pthread_mutex_unlock(&actor.gate.mutex);
  REQUIRE(!pthread_join(thread, NULL));
  REQUIRE(actor.result == LC_OK);
  ok(opened, &e);
  REQUIRE(input.signature.present && input.bytes == 7U);
  unsigned long crc =
      crc32(crc32(0L, Z_NULL, 0), (const unsigned char *)"visible", 7U);
  REQUIRE(index_input_crc(&input, &e) == crc);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  REQUIRE(index_input_crc(&input, &e) == crc);
  ps_query_artifact_close(root, &input);
  ps_namespace_release(actor.ns);
  ok(ps_root_close(root, &e), &e);
  index_gate_clear(&actor.gate);
  REQUIRE(!unlink(path));
  lc_error_cleanup(&e);
}
typedef struct index_input_gate {
  index_gate gate;
  pthread_t reader;
  int armed;
} index_input_gate;
static void *index_gate_allocate(void *context, size_t bytes) {
  index_input_gate *state = context;
  void *memory = allocate(NULL, bytes);
  if (memory && __atomic_load_n(&state->armed, __ATOMIC_ACQUIRE) &&
      pthread_equal(state->reader, pthread_self()) &&
      __atomic_exchange_n(&state->armed, 0, __ATOMIC_ACQ_REL)) {
    pthread_mutex_lock(&state->gate.mutex);
    state->gate.entered = 1;
    pthread_cond_broadcast(&state->gate.changed);
    while (!state->gate.release)
      pthread_cond_wait(&state->gate.changed, &state->gate.mutex);
    pthread_mutex_unlock(&state->gate.mutex);
  }
  return memory;
}
typedef struct index_input_actor {
  ps_root *root;
  ps_namespace *ns;
  uint64_t owner;
  index_input_gate *gate;
  lc_pouch_index_artifact_id id;
  lc_pouch_index_input input;
  int result;
} index_input_actor;
static void *index_captured_open(void *context) {
  index_input_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  actor->gate->reader = pthread_self();
  __atomic_store_n(&actor->gate->armed, 1, __ATOMIC_RELEASE);
  const unsigned char name[10] = {1U, LC_POUCH_INDEX_PACKED, 7U};
  actor->result = ps_query_artifact_open(actor->ns, actor->owner, name,
                                         sizeof(name), &actor->input, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void index_retirement_admission(const char *directory) {
  char path[1400];
  index_input_gate gate = {0};
  lc_allocator domain = {index_gate_allocate, resize, deallocate, &gate};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_info identity;
  ps_catalog_info binding = {0};
  ps_owned_head *retiring = NULL;
  ps_owned_status status;
  index_input_actor actor = {0};
  pthread_t thread;
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-retiring", directory) <
          (int)sizeof(path));
  options.crypto_allocator = &domain;
  index_gate_init(&gate.gate);
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  actor.root = root;
  actor.gate = &gate;
  actor.id.kind = LC_POUCH_INDEX_PACKED;
  actor.id.segmented = 1;
  actor.id.generation = 7U;
  const unsigned char name[10] = {1U, LC_POUCH_INDEX_PACKED, 7U};
  ok(ps_root_namespace_acquire(root, "query/retirement", 1, &ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  actor.ns = ns;
  actor.owner = identity.owner;
  ok(ps_query_artifact_replace(ns, identity.owner, name, sizeof(name), 3U,
                               index_small_producer, (void *)"old", NULL, NULL,
                               &e),
     &e);
  ok(ps_catalog_visit(ps_root_space(root), identity.owner,
                      PS_CATALOG_PROJECTION, index_capture_binding, &binding,
                      &e),
     &e);
  ok(ps_owned_open(ps_root_space(root), NULL, binding.stream, 0,
                   &binding.target, &retiring, &status, &e),
     &e);
  lc_pouch_index_input unavailable = {0};
  REQUIRE(ps_query_artifact_open(ns, identity.owner, name, sizeof(name),
                                 &unavailable, &e) == LC_ERR_INVALID);
  REQUIRE(!unavailable.signature.present);
  clear(&e);
  ps_owned_close(retiring);
  retiring = NULL;
  REQUIRE(!pthread_create(&thread, NULL, index_captured_open, &actor));
  index_gate_wait(&gate.gate);
  ok(ps_query_artifact_remove(ps_root_space(root), identity.owner, name,
                              sizeof(name), NULL, &e),
     &e);
  /* Hold the now-unbound physical head exactly as a concurrent retirement
   * admission may do. The earlier catalog view still protects its bytes. */
  ok(ps_owned_open(ps_root_space(root), NULL, binding.stream, 0,
                   &binding.target, &retiring, &status, &e),
     &e);
  pthread_mutex_lock(&gate.gate.mutex);
  gate.gate.release = 1;
  pthread_cond_broadcast(&gate.gate.changed);
  pthread_mutex_unlock(&gate.gate.mutex);
  REQUIRE(!pthread_join(thread, NULL));
  ps_owned_close(retiring);
  REQUIRE(actor.result == LC_OK && !actor.input.signature.present);
  ps_query_artifact_close(root, &actor.input);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(!live && !unlink(path));
  index_gate_clear(&gate.gate);
  lc_error_cleanup(&e);
}
typedef struct index_revision_actor {
  ps_root *root;
  index_gate *gate;
  lc_pouch_index_artifact_id generation, manifest;
  lc_pouch_index_signature signature;
  void *build;
  int publishing, result;
} index_revision_actor;
static void *index_revision_run(void *context) {
  index_revision_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  if (!actor->publishing)
    actor->result = ps_query_index_store_ops.prepare(
        actor->root, "query/concurrent", &actor->generation, 8U,
        index_paused_producer, actor->gate, 0, 16U, &actor->build,
        &actor->signature, &e);
  else
    actor->result = ps_query_index_store_ops.publish(
        actor->root, actor->build, "query/concurrent", &actor->manifest, 8U,
        index_paused_producer, actor->gate, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void index_revision_selection(const char *directory) {
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-revision", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync) {
    ps_root *root = NULL;
    ps_root_options options = {0};
    options.storage.durable_sync = sync;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    index_gate gate;
    index_gate_init(&gate);
    index_revision_actor actor = {0};
    actor.root = root;
    actor.gate = &gate;
    actor.generation.kind = LC_POUCH_INDEX_PACKED;
    actor.generation.segmented = 1;
    actor.generation.generation = 7U;
    actor.manifest.kind = LC_POUCH_INDEX_MANIFEST;
    ps_namespace *ns = NULL;
    ps_stats before = {0}, after = {0};
    int hint;
    ok(ps_root_namespace_acquire(root, "query/concurrent", 1, &ns, &e), &e);
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    ok(ps_query_index_store_ops.prepare(root, "query/concurrent",
                                        &actor.generation, 3U,
                                        index_small_producer, (void *)"old", 0,
                                        5U, &actor.build, &actor.signature, &e),
       &e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.syncs == before.syncs);
    ok(ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_ORDINARY, &hint, &e),
       &e);
    REQUIRE(!hint);
    ok(ps_query_index_store_ops.publish(
           root, actor.build, "query/concurrent", &actor.manifest, 5U,
           index_small_producer, (void *)"prior", &e),
       &e);
    ps_query_index_store_ops.dispose(root, actor.build);
    actor.build = NULL;
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.syncs == before.syncs + 1U);
    ok(ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_ORDINARY, &hint, &e),
       &e);
    REQUIRE(hint);
    ps_namespace_release(ns);
    lc_pouch_index_input old_generation = {0}, old_manifest = {0};
    ok(ps_query_index_store_ops.open(root, "query/concurrent",
                                     &actor.generation, &old_generation, &e),
       &e);
    ok(ps_query_index_store_ops.open(root, "query/concurrent", &actor.manifest,
                                     &old_manifest, &e),
       &e);
    unsigned long generation_crc = index_input_crc(&old_generation, &e);
    unsigned long manifest_crc = index_input_crc(&old_manifest, &e);
    pthread_t builder, peer;
    REQUIRE(!pthread_create(&builder, NULL, index_revision_run, &actor));
    index_gate_wait(&gate);
    index_raw_expect(root, &actor.generation, "old", &e);
    index_raw_expect(root, &actor.manifest, "prior", &e);
    index_actor sibling = {0};
    sibling.root = root;
    sibling.gate = &gate;
    sibling.sibling = 1;
    sibling.id = actor.generation;
    ++sibling.id.generation;
    REQUIRE(!pthread_create(&peer, NULL, index_actor_run, &sibling));
    int independent = index_gate_release(&gate);
    REQUIRE(!pthread_join(builder, NULL) && !pthread_join(peer, NULL));
    REQUIRE(independent && actor.result == LC_OK && sibling.result == LC_OK);
    REQUIRE(actor.signature.present && actor.signature.size == 8U &&
            actor.signature.mtime != old_generation.signature.mtime);
    /* A prepared identity can bind the manifest without selecting or reading
     * the new generation. Even a peer checkpoint retains both old selections.
     */
    index_raw_expect(root, &actor.generation, "old", &e);
    index_raw_expect(root, &actor.manifest, "prior", &e);
    pthread_mutex_lock(&gate.mutex);
    gate.entered = gate.release = gate.sibling_done = 0;
    pthread_mutex_unlock(&gate.mutex);
    actor.publishing = 1;
    REQUIRE(!pthread_create(&builder, NULL, index_revision_run, &actor));
    index_gate_wait(&gate);
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    index_raw_expect(root, &actor.generation, "old", &e);
    index_raw_expect(root, &actor.manifest, "prior", &e);
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    pthread_mutex_lock(&gate.mutex);
    gate.release = 1;
    pthread_cond_broadcast(&gate.changed);
    pthread_mutex_unlock(&gate.mutex);
    REQUIRE(!pthread_join(builder, NULL) && actor.result == LC_OK);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.syncs - before.syncs == 1U);
    index_raw_expect(root, &actor.generation, "new tail", &e);
    index_raw_expect(root, &actor.manifest, "new tail", &e);
    ps_query_index_store_ops.dispose(root, actor.build);
    actor.build = NULL;
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    REQUIRE(index_input_crc(&old_generation, &e) == generation_crc &&
            index_input_crc(&old_manifest, &e) == manifest_crc);
    ps_query_index_store_ops.close(root, &old_generation);
    ps_query_index_store_ops.close(root, &old_manifest);
    ok(ps_query_index_store_ops.prepare(
           root, "query/concurrent", &actor.generation, 7U,
           index_small_producer, (void *)"aborted", 0, 4U, &actor.build,
           &actor.signature, &e),
       &e);
    REQUIRE(ps_query_index_store_ops.publish(
                root, actor.build, "query/concurrent", &actor.manifest, 8U,
                index_small_producer, (void *)"too long",
                &e) == LC_ERR_INVALID);
    clear(&e);
    ps_query_index_store_ops.dispose(root, actor.build);
    index_raw_expect(root, &actor.generation, "new tail", &e);
    index_raw_expect(root, &actor.manifest, "new tail", &e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(!after.budgets && !after.private_bytes &&
            !after.reserved_payload_bytes && !after.reserved_metadata_bytes &&
            !after.reserved_entries);
    ok(ps_root_close(root, &e), &e);
    index_gate_clear(&gate);
    REQUIRE(!live && !unlink(path));
  }
  lc_error_cleanup(&e);
}
typedef struct index_parallel_actor {
  lc_client *client;
  const char *ns;
  int result;
} index_parallel_actor;
static void *index_parallel_flush(void *context) {
  index_parallel_actor *actor = context;
  lc_error e;
  lc_pouch_generation sequence = 0;
  lc_pouch_query_index_flush_result flushed = {0};
  lc_error_init(&e);
  actor->result = lc_pouch_state_query_index_seq(index_pouch(actor->client),
                                                 actor->ns, &sequence, &e);
  if (actor->result == LC_OK)
    actor->result = lc_pouch_query_index_flush(
        index_pouch(actor->client), actor->ns, sequence, &flushed, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void index_parallel_namespaces(const char *directory) {
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-parallel", directory) <
          (int)sizeof(path));
  index_gate gate;
  index_gate_init(&gate);
  ps_io io = {&gate,
              index_gate_read,
              index_gate_write,
              index_gated_sync,
              index_gate_reserve,
              NULL};
  lc_client *client = index_client(path, 0, 1, 1, &io, &e);
  const char *names[] = {"query/parallel-a", "query/parallel-b"};
  index_parallel_actor actors[2] = {{client, names[0], -1},
                                    {client, names[1], -1}};
  for (size_t i = 0; i < 2; ++i)
    index_put_in(client, names[i], "one", "{\"v\":1}", &e);
  pthread_mutex_lock(&gate.mutex);
  gate.armed = 1;
  pthread_mutex_unlock(&gate.mutex);
  pthread_t threads[2];
  REQUIRE(!pthread_create(&threads[0], NULL, index_parallel_flush, &actors[0]));
  index_gate_wait(&gate);
  REQUIRE(!pthread_create(&threads[1], NULL, index_parallel_flush, &actors[1]));
  struct timespec deadline;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
  deadline.tv_sec += 5;
  pthread_mutex_lock(&gate.mutex);
  int waited = 0;
  while (!gate.peer_payload && !waited)
    waited = pthread_cond_timedwait(&gate.changed, &gate.mutex, &deadline);
  int progressed = gate.peer_payload;
  gate.release = 1;
  pthread_cond_broadcast(&gate.changed);
  pthread_mutex_unlock(&gate.mutex);
  REQUIRE(!pthread_join(threads[0], NULL) && !pthread_join(threads[1], NULL));
  REQUIRE(progressed && actors[0].result == LC_OK && actors[1].result == LC_OK);
  for (size_t i = 0; i < 2; ++i)
    REQUIRE(index_query_in(client, names[i], "index", "{\"exists\":\"/v\"}",
                           &e) == 1U);
  lc_client_close(client);
  REQUIRE(!live);
  client = index_client(path, 0, 0, 1, NULL, &e);
  for (size_t i = 0; i < 2; ++i) {
    lc_pouch_generation state = 0, indexed = 0;
    ok(lc_pouch_state_query_index_seq(index_pouch(client), names[i], &state,
                                      &e),
       &e);
    ok(lc_pouch_query_index_manifest_seq(index_pouch(client), names[i],
                                         &indexed, &e),
       &e);
    REQUIRE(state && state == indexed);
    REQUIRE(index_query_in(client, names[i], "index", "{\"exists\":\"/v\"}",
                           &e) == 1U);
  }
  lc_client_close(client);
  REQUIRE(!live && !unlink(path));
  index_gate_clear(&gate);
  lc_error_cleanup(&e);
}
static void index_concurrency(const char *directory) {
  index_parallel_namespaces(directory);
  index_revision_selection(directory);
  index_visibility(directory);
  index_retirement_admission(directory);
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-concurrent", directory) <
          (int)sizeof(path));
  index_gate gate;
  index_gate_init(&gate);
  ps_root *root = NULL;
  ps_root_options options = {0};
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  index_actor actors[2] = {{0}, {0}};
  for (int i = 0; i < 2; ++i) {
    actors[i].root = root;
    actors[i].gate = &gate;
    actors[i].id.kind = LC_POUCH_INDEX_PACKED;
    actors[i].id.segmented = 1;
    actors[i].id.generation = (uint64_t)i + 1U;
    actors[i].sibling = i;
  }
  ok(ps_query_index_store_ops.replace(root, "query/concurrent", &actors[0].id,
                                      3U, index_small_producer, (void *)"old",
                                      1, 0, &e),
     &e);
  lc_pouch_index_input pinned = {0};
  ok(ps_query_index_store_ops.open(root, "query/concurrent", &actors[0].id,
                                   &pinned, &e),
     &e);
  unsigned long crc = index_input_crc(&pinned, &e);
  ps_namespace *ns = NULL;
  ps_namespace_info identity;
  ps_ref target;
  uint64_t observed_bytes;
  int present;
  ok(ps_root_namespace_acquire(root, "query/concurrent", 0, &ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ps_key physical = {identity.owner, 1U,
                     PS_QUERY_FIRST + 2U * LC_POUCH_INDEX_PACKED + 1U};
  ok(ps_value_info(ps_root_space(root), physical, &target, &observed_bytes,
                   &present, &e),
     &e);
  ps_namespace_release(ns);
  REQUIRE(present && observed_bytes == 3U);
  ps_reader *rejected = NULL;
  ps_ref stale = target;
  ++stale.incarnation;
  REQUIRE(ps_read_open(ps_root_space(root), physical, &stale, &rejected, &e) ==
              LC_ERR_INVALID &&
          !rejected);
  clear(&e);
  stale = target;
  stale.container[0] ^= 1U;
  REQUIRE(ps_read_open(ps_root_space(root), physical, &stale, &rejected, &e) ==
              LC_ERR_INVALID &&
          !rejected);
  clear(&e);
  pthread_t threads[2];
  REQUIRE(!pthread_create(&threads[0], NULL, index_actor_run, &actors[0]));
  index_gate_wait(&gate);
  index_raw_expect(root, &actors[0].id, "old", &e);
  REQUIRE(!pthread_create(&threads[1], NULL, index_actor_run, &actors[1]));
  int independent = index_gate_release(&gate);
  REQUIRE(!pthread_join(threads[0], NULL));
  REQUIRE(!pthread_join(threads[1], NULL));
  if (!independent || actors[0].result != LC_OK || actors[1].result != LC_OK)
    fprintf(stderr, "index sync gate independent=%d publisher=%d sibling=%d\n",
            independent, actors[0].result, actors[1].result);
  REQUIRE(independent && actors[0].result == LC_OK &&
          actors[1].result == LC_OK);
  index_raw_expect(root, &actors[0].id, "new tail", &e);
  index_raw_expect(root, &actors[1].id, "sibling", &e);
  ok(ps_query_index_store_ops.prune(root, "query/concurrent", NULL, 0U, &e),
     &e);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  REQUIRE(crc == index_input_crc(&pinned, &e));
  ps_stats held = {0}, released = {0};
  ok(ps_status(ps_root_space(root), &held, &e), &e);
  REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(crc == index_input_crc(&pinned, &e));
  ps_query_index_store_ops.close(root, &pinned);
  ok(ps_status(ps_root_space(root), &released, &e), &e);
  REQUIRE(released.retired_bytes < held.retired_bytes);
  ok(ps_value_info(ps_root_space(root), physical, &target, &observed_bytes,
                   &present, &e),
     &e);
  REQUIRE(!present);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(!unlink(path));
  index_gate_clear(&gate);

  index_gate_init(&gate);
  ps_io io = {&gate,
              index_gate_read,
              index_gate_write,
              index_gated_sync,
              index_gate_reserve,
              NULL};
  lc_client *client = index_client(path, 0, 1, 1, &io, &e);
  index_seed(client, 1, &e);
  index_change(client, &e);
  memset(actors, 0, sizeof(actors));
  for (int i = 0; i < 2; ++i) {
    actors[i].client = client;
    actors[i].gate = &gate;
    actors[i].sibling = i;
    actors[i].flush = 1;
  }
  pthread_mutex_lock(&gate.mutex);
  gate.armed = 1;
  pthread_mutex_unlock(&gate.mutex);
  REQUIRE(!pthread_create(&threads[0], NULL, index_actor_run, &actors[0]));
  index_gate_wait(&gate);
  REQUIRE(!pthread_create(&threads[1], NULL, index_actor_run, &actors[1]));
  independent = index_gate_release(&gate);
  REQUIRE(!pthread_join(threads[0], NULL));
  REQUIRE(!pthread_join(threads[1], NULL));
  REQUIRE(independent && actors[0].result == LC_OK &&
          actors[1].result == LC_OK);
  index_verify(client, 1, 0, &e);
  REQUIRE(index_query(client, "index", index_selectors[0], &e) == 6U);
  lc_client_close(client);
  REQUIRE(!live);
  client = index_client(path, 0, 0, 1, NULL, &e);
  index_verify(client, 1, 0, &e);
  REQUIRE(index_query(client, "index", index_selectors[0], &e) == 6U);
  lc_client_close(client);
  REQUIRE(!live);
  one_file(path);
  REQUIRE(!unlink(path));
  index_gate_clear(&gate);
  lc_error_cleanup(&e);
}
#endif
