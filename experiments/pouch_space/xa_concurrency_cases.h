#ifndef POUCH_SPACE_XA_CONCURRENCY_CASES_H
#define POUCH_SPACE_XA_CONCURRENCY_CASES_H
#include "xa_cases.h"
typedef struct xa_sync_gate {
  io_fault fault;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int armed, entered, release, sibling_done;
} xa_sync_gate;
typedef struct xa_actor {
  lc_client *client;
  xa_sync_gate *gate;
  int sibling, result;
} xa_actor;
static int xa_gated_sync(void *context, int fd) {
  xa_sync_gate *gate = context;
  pthread_mutex_lock(&gate->mutex);
  if (gate->armed) {
    gate->armed = 0;
    gate->entered = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->release)
      pthread_cond_wait(&gate->changed, &gate->mutex);
  }
  pthread_mutex_unlock(&gate->mutex);
  return ps_native_io.sync(NULL, fd);
}
static int xa_sibling_write(lc_client *client, const char *value, lc_error *e) {
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result result = {0};
  lc_source *source = NULL;
  int rc = lc_source_from_memory(value, strlen(value), &source, e);
  options.object_record = 1;
  if (rc == LC_OK)
    rc = lc_pouch_state_write(p, ".xa", "independent", source, &options,
                              &result, e);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(&allocator, &result);
  return rc;
}
static void *xa_actor_run(void *context) {
  xa_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  actor->result = actor->sibling ? xa_sibling_write(actor->client, "new", &e)
                                 : xa_action(actor->client, 1, &e);
  if (actor->sibling) {
    pthread_mutex_lock(&actor->gate->mutex);
    actor->gate->sibling_done = 1;
    pthread_cond_broadcast(&actor->gate->changed);
    pthread_mutex_unlock(&actor->gate->mutex);
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void xa_concurrency(const char *directory) {
  char path[1400];
  lc_error e;
  xa_sync_gate gate = {0};
  pthread_t threads[2];
  lc_error_init(&e);
  REQUIRE(!pthread_mutex_init(&gate.mutex, NULL));
  REQUIRE(!pthread_cond_init(&gate.changed, NULL));
  ps_io io = {&gate, disk_read, disk_write, xa_gated_sync, disk_reserve, NULL};
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-concurrent", directory) <
          (int)sizeof(path));
  lc_client *client = xa_client(path, 0, 1, &io, &e);
  xa_seed(client, 1, &e);
  ok(xa_sibling_write(client, "old", &e), &e);
  lc_pouch *p = ((lc_client_handle *)client)->pouch;
  ok(ps_checkpoint(ps_pouch_state_root(p)->space, &e), &e);
  xa_actor actors[2] = {{client, &gate, 0, -1}, {client, &gate, 1, -1}};
  pthread_mutex_lock(&gate.mutex);
  gate.armed = 1;
  pthread_mutex_unlock(&gate.mutex);
  REQUIRE(!pthread_create(&threads[0], NULL, xa_actor_run, &actors[0]));
  pthread_mutex_lock(&gate.mutex);
  while (!gate.entered)
    pthread_cond_wait(&gate.changed, &gate.mutex);
  pthread_mutex_unlock(&gate.mutex);
  lc_pouch_txn_guard denied, other;
  REQUIRE(lc_pouch_txn_guard_lock(p, retention_xid, 0, &denied, &e) ==
          LC_ERR_PENDING);
  lc_pouch_txn_guard_release(&denied);
  clear(&e);
  int acquired = 0;
  for (unsigned i = 1; i < 256U && !acquired; ++i) {
    char id[LC_XID_STRING_SIZE];
    REQUIRE(snprintf(id, sizeof(id), "0000000000000000%03x0", i) == 20);
    int rc = lc_pouch_txn_guard_lock(p, id, 0, &other, &e);
    REQUIRE(rc == LC_OK || rc == LC_ERR_PENDING);
    acquired = rc == LC_OK;
    if (acquired)
      lc_pouch_txn_guard_release(&other);
    clear(&e);
  }
  REQUIRE(acquired);
  REQUIRE(!pthread_create(&threads[1], NULL, xa_actor_run, &actors[1]));
  struct timespec deadline;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
  deadline.tv_sec += 10;
  int rc = 0;
  pthread_mutex_lock(&gate.mutex);
  while (!gate.sibling_done && !rc)
    rc = pthread_cond_timedwait(&gate.changed, &gate.mutex, &deadline);
  int independent = gate.sibling_done;
  gate.release = 1;
  pthread_cond_broadcast(&gate.changed);
  pthread_mutex_unlock(&gate.mutex);
  REQUIRE(!pthread_join(threads[0], NULL));
  REQUIRE(!pthread_join(threads[1], NULL));
  REQUIRE(independent && actors[0].result == LC_OK &&
          actors[1].result == LC_OK);
  xa_verify(client, 1, 1, &e);
  lc_pouch_state_read_result value = {0};
  ok(lc_pouch_state_read(p, ".xa", "independent", &value, &e), &e);
  char bytes[4] = {0};
  REQUIRE(value.found && value.body &&
          value.body->read(value.body, bytes, sizeof(bytes), &e) == 3U &&
          !strcmp(bytes, "new"));
  lc_pouch_state_read_result_cleanup(&allocator, &value);
  lc_client_close(client);
  REQUIRE(!live);
  one_file(path);
  REQUIRE(!pthread_cond_destroy(&gate.changed));
  REQUIRE(!pthread_mutex_destroy(&gate.mutex));
  lc_error_cleanup(&e);
}
typedef struct xa_scope_context {
  lc_pouch *pouch;
} xa_scope_context;
static int xa_scope_put(lc_pouch *p, const char *text, int locked,
                        lc_error *e) {
  lc_source *source = NULL;
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result result = {0};
  int rc = lc_source_from_memory(text, strlen(text), &source, e);
  options.object_record = 1;
  if (rc == LC_OK)
    rc = locked ? lc_pouch_state_write_locked(p, ".xa", "scope", source,
                                              &options, &result, e)
                : lc_pouch_state_write(p, ".xa", "scope", source, &options,
                                       &result, e);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(&allocator, &result);
  return rc;
}
static int xa_borrow_scope(void *context, lc_error *e) {
  xa_scope_context *scope = context;
  int rc = xa_scope_put(scope->pouch, "first", 1, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_sync_namespace(scope->pouch, ".xa", e);
  if (rc == LC_OK)
    rc = xa_scope_put(scope->pouch, "later", 1, e);
  return rc;
}
static void xa_scope(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    retention_disk disk = {0};
    disk.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
    disk.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
    disk.scratch = malloc(PS_PAGE);
    REQUIRE(disk.stable && disk.dirty && disk.scratch);
    ps_io io = {&disk,
                retention_disk_read,
                retention_disk_write,
                retention_disk_sync,
                retention_disk_reserve,
                NULL};
    char path[1400];
    REQUIRE(snprintf(path, sizeof(path), "%s/xa-scope", directory) <
            (int)sizeof(path));
    lc_client *client = xa_client(path, sync, 1, &io, &e);
    lc_pouch *p = ((lc_client_handle *)client)->pouch;
    ok(xa_scope_put(p, "seed", 0, &e), &e);
    ok(ps_checkpoint(ps_pouch_state_root(p)->space, &e), &e);
    xa_scope_context context = {p};
    ok(lc_pouch_state_with_namespace_lock(p, ".xa", xa_borrow_scope, &context,
                                          &e),
       &e);
    disk.frozen = 1;
    lc_client_close(client);
    REQUIRE(!live);
    retention_disk_restore(&disk, path);
    client = xa_client(path, sync, 0, NULL, &e);
    p = ((lc_client_handle *)client)->pouch;
    lc_pouch_state_read_result result = {0};
    ok(lc_pouch_state_read(p, ".xa", "scope", &result, &e), &e);
    char value[8] = {0};
    REQUIRE(result.found && result.body &&
            result.body->read(result.body, value, sizeof(value), &e) == 5U &&
            !strcmp(value, sync ? "later" : "first"));
    lc_pouch_state_read_result_cleanup(&allocator, &result);
    lc_client_close(client);
    REQUIRE(!live);
    REQUIRE(!unlink(path));
    free(disk.stable);
    free(disk.dirty);
    free(disk.scratch);
  }
  lc_error_cleanup(&e);
}
#endif
