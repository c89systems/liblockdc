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
  int entered, release, sibling_done, armed;
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
  return fdatasync(fd);
}
static ssize_t index_gate_read(void *context, int fd, void *bytes, size_t size,
                               off_t offset) {
  (void)context;
  return pread(fd, bytes, size, offset);
}
static ssize_t index_gate_write(void *context, int fd, const void *bytes,
                                size_t size, off_t offset) {
  (void)context;
  return pwrite(fd, bytes, size, offset);
}
static int index_gate_reserve(void *context, int fd, off_t offset, off_t size) {
  (void)context;
  return posix_fallocate(fd, offset, size);
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
  lc_pouch_index_artifact_id id = {1U, LC_POUCH_INDEX_PACKED, 1};
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
  int opened =
      ps_query_index_store_ops.open(root, "query/visible", &id, &input, &e);
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
  ps_query_index_store_ops.close(root, &input);
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
  actor->result = ps_query_index_store_ops.open(actor->root, "query/retirement",
                                                &actor->id, &actor->input, &e);
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
  ok(ps_query_index_store_ops.replace(root, "query/retirement", &actor.id, 3U,
                                      index_small_producer, (void *)"old", 0, 0,
                                      &e),
     &e);
  ok(ps_root_namespace_acquire(root, "query/retirement", 0, &ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(ps_catalog_visit(ps_root_space(root), identity.owner,
                      PS_CATALOG_PROJECTION, index_capture_binding, &binding,
                      &e),
     &e);
  ps_namespace_release(ns);
  ok(ps_owned_open(ps_root_space(root), NULL, binding.stream, 0,
                   &binding.target, &retiring, &status, &e),
     &e);
  lc_pouch_index_input unavailable = {0};
  REQUIRE(ps_query_index_store_ops.open(root, "query/retirement", &actor.id,
                                        &unavailable, &e) == LC_ERR_INVALID);
  REQUIRE(!unavailable.signature.present);
  clear(&e);
  ps_owned_close(retiring);
  retiring = NULL;
  REQUIRE(!pthread_create(&thread, NULL, index_captured_open, &actor));
  index_gate_wait(&gate.gate);
  ok(ps_query_index_store_ops.remove(root, "query/retirement", &actor.id, &e),
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
  ps_query_index_store_ops.close(root, &actor.input);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(!live && !unlink(path));
  index_gate_clear(&gate.gate);
  lc_error_cleanup(&e);
}
static void index_concurrency(const char *directory) {
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
  ps_catalog_info binding = {0};
  ps_owned_status inspected;
  ok(ps_root_namespace_acquire(root, "query/concurrent", 0, &ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(ps_catalog_visit(ps_root_space(root), identity.owner,
                      PS_CATALOG_PROJECTION, index_capture_binding, &binding,
                      &e),
     &e);
  ps_namespace_release(ns);
  ok(ps_owned_inspect(ps_root_space(root), binding.stream, &binding.target,
                      &inspected, &e),
     &e);
  REQUIRE(inspected.sealed && inspected.tail == 3U);
  ps_ref stale = binding.target;
  ++stale.incarnation;
  REQUIRE(ps_owned_inspect(ps_root_space(root), binding.stream, &stale,
                           &inspected, &e) == LC_ERR_INVALID);
  clear(&e);
  stale = binding.target;
  stale.container[0] ^= 1U;
  REQUIRE(ps_owned_inspect(ps_root_space(root), binding.stream, &stale,
                           &inspected, &e) == LC_ERR_INVALID);
  clear(&e);
  pthread_t threads[2];
  REQUIRE(!pthread_create(&threads[0], NULL, index_actor_run, &actors[0]));
  index_gate_wait(&gate);
  index_raw_expect(root, &actors[0].id, "old", &e);
  REQUIRE(!pthread_create(&threads[1], NULL, index_actor_run, &actors[1]));
  int independent = index_gate_release(&gate);
  REQUIRE(!pthread_join(threads[0], NULL));
  REQUIRE(!pthread_join(threads[1], NULL));
  REQUIRE(independent && actors[0].result == LC_OK &&
          actors[1].result == LC_OK);
  index_raw_expect(root, &actors[0].id, "new tail", &e);
  index_raw_expect(root, &actors[1].id, "sibling", &e);
  ok(ps_query_index_store_ops.prune(root, "query/concurrent", NULL, 0U, &e),
     &e);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  REQUIRE(crc == index_input_crc(&pinned, &e));
  REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(crc == index_input_crc(&pinned, &e));
  ps_query_index_store_ops.close(root, &pinned);
  uint64_t reclaimed = 0U, skipped = 0U;
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, &e), &e);
  REQUIRE(reclaimed > 0U);
  REQUIRE(ps_owned_inspect(ps_root_space(root), binding.stream, &binding.target,
                           &inspected, &e) == LC_ERR_INVALID);
  clear(&e);
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
