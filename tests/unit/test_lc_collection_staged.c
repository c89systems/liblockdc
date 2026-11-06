#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include "../support/lc_test_cases.h"
#include "../support/lc_test_tmp.h"
#include "lc_allocation_tracking.h"
#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_pouch_recode.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-staged-"
#define BODY_BYTES (256U * 1024U)

typedef struct allocations {
  size_t live;
  size_t maximum, denied, deny_size;
  int fail;
} allocations;

typedef struct fixture {
  char root[256], endpoint[512];
  char *body, *key;
  lc_client *client;
  lc_collection *collection, *peer, *owner;
  lc_allocator allocator;
  void *(*saved_builtin_malloc)(size_t);
  void *(*saved_builtin_realloc)(void *, size_t);
  allocations allocation_stats;
  lc_collection_writer *pending;
  unsigned char *stream_chunk;
  lc_error error;
  lc_pouch_unix_seconds now;
  lc_u64 segment_target;
} fixture;

typedef struct input input;
struct input {
  lc_source pub;
  fixture *f;
  const char *body;
  size_t length, offset, chunk, calls, resets, closes, fail_at, action_at;
  int fail, oversized, acted;
  int (*action)(input *, lc_error *);
  void *context;
};

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("staged append failure %d: %s\n", rc,
                f->error.message != NULL ? f->error.message : "");
  assert_int_equal(rc, LC_OK);
}

static void close_owners(fixture *f) {
  lc_collection_close(f->owner);
  f->owner = NULL;
  lc_collection_close(f->peer);
  f->peer = NULL;
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_client_close(f->client);
  f->client = NULL;
}

static void open_owners(fixture *f, int mode, int create) {
  lc_client_config config;
  lc_collection_config collection = {0};
  lc_pouch_settings settings;
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_DURABLE_SYNC;
  settings.single_writer = 0;
  settings.background_compaction_enabled = 0;
  settings.query_indexing_enabled = 0;
  settings.compression = mode & 2 ? "zlib" : "none";
  settings.durable_sync = (mode & 4) != 0;
  if (f->segment_target) {
    settings.set_mask |= LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
    settings.segment_target_bytes = f->segment_target;
  }
  if (mode & 1) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = f->key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "journal";
  config.allocator = f->allocator;
  config.pouch_settings = &settings;
  check(f, lc_client_open(&config, &f->client, &f->error));
  collection.name = "events";
  check(f, create ? lc_client_create_collection(f->client, &collection,
                                                &f->collection, &f->error)
                  : lc_client_open_collection(f->client, &collection,
                                              &f->collection, &f->error));
  check(f,
        lc_client_open_collection(f->client, &collection, &f->peer, &f->error));
}

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char temporary[] = PREFIX "XXXXXX";
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  assert_true(lc_test_tmp_mkdtemp(temporary, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  f->body = malloc(BODY_BYTES + 1U);
  assert_non_null(f->body);
  memcpy(f->body, "{\"v\":\"", 6);
  memset(f->body + 6, 'x', BODY_BYTES - 8U);
  memcpy(f->body + BODY_BYTES - 2U, "\"}", 3);
  open_owners(f, 0, 1);
  return 0;
}

static size_t denied_builtin_attempts;
static void *deny_builtin_malloc(size_t size) {
  ++denied_builtin_attempts;
  (void)size;
  return NULL;
}
static void *deny_builtin_realloc(void *pointer, size_t size) {
  ++denied_builtin_attempts;
  (void)pointer;
  (void)size;
  return NULL;
}
static void guard_builtin_allocations(fixture *f, int enabled) {
  if (enabled) {
    if (!f->saved_builtin_malloc) {
      f->saved_builtin_malloc = lc_builtin_malloc;
      f->saved_builtin_realloc = lc_builtin_realloc;
    }
    lc_builtin_malloc = deny_builtin_malloc;
    lc_builtin_realloc = deny_builtin_realloc;
  } else if (f->saved_builtin_malloc) {
    lc_builtin_malloc = f->saved_builtin_malloc;
    lc_builtin_realloc = f->saved_builtin_realloc;
  }
}

static int teardown(void **state) {
  fixture *f = *state;
  guard_builtin_allocations(f, 0);
  f->allocation_stats.fail = 0;
  lc_collection_append_test_hook = NULL;
  lc_collection_append_test_context = NULL;
  lc_collection_chunks_test_pwrite = NULL;
  lc_collection_chunks_test_sync_fd = NULL;
  lc_collection_test_rotate_sync_fd = NULL;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_collection_writer_close(f->pending);
  f->pending = NULL;
  close_owners(f);
  free(f->stream_chunk);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  free(f->body);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static size_t input_read(lc_source *self, void *bytes, size_t capacity,
                         lc_error *error) {
  input *in = self->impl;
  ++in->calls;
  if (in->action != NULL && !in->acted && in->offset >= in->action_at) {
    in->acted = 1;
    if (in->action(in, error) != LC_OK)
      return 0;
  }
  if (in->fail && in->offset >= in->fail_at) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "injected source failure", NULL,
                 NULL, NULL);
    return 0;
  }
  if (in->oversized)
    return capacity + 1U;
  size_t count = in->length - in->offset;
  if (count > capacity)
    count = capacity;
  if (count > in->chunk)
    count = in->chunk;
  if (count != 0U)
    memcpy(bytes, in->body + in->offset, count);
  in->offset += count;
  return count;
}
static int input_reset(lc_source *self, lc_error *error) {
  input *in = self->impl;
  (void)error;
  ++in->resets;
  return LC_ERR_INVALID;
}
static void input_close(lc_source *self) {
  input *in = self->impl;
  ++in->closes;
}
static void input_init(input *in, fixture *f, const char *body, size_t length) {
  memset(in, 0, sizeof(*in));
  in->pub.read = input_read;
  in->pub.reset = input_reset;
  in->pub.close = input_close;
  in->pub.impl = in;
  in->f = f;
  in->body = body;
  in->length = length;
  in->chunk = 16384;
}

static void status_is(fixture *f, const char *id,
                      lc_collection_append_state expected) {
  lc_collection_append_status_res status;
  check(f, lc_collection_append_status(f->collection, id, &status, &f->error));
  assert_int_equal(status.state, expected);
}

static int push(lc_collection *collection, const char *id, const char *body,
                size_t length, lc_collection_append_res *out, lc_error *error) {
  lc_collection_append_req req = {0};
  lc_collection_writer *writer = NULL;
  req.append_id = id;
  int rc = lc_collection_append_begin(collection, &req, &writer, error);
  for (size_t at = 0; rc == LC_OK && at < length;) {
    size_t count = length - at < 16384 ? length - at : 16384;
    rc = lc_collection_writer_write(writer, body + at, count, error);
    at += count;
  }
  if (rc == LC_OK)
    rc = lc_collection_writer_commit(writer, out, error);
  lc_collection_writer_close(writer);
  return rc;
}

static void read_one(fixture *f, const char *expected, size_t length) {
  lc_sink *sink = NULL;
  lc_collection_read_res result;
  const void *bytes;
  size_t count;
  lc_collection_read_res_init(&result);
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(f->collection, NULL, sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &count, &f->error));
  assert_int_equal(count, length + 1U);
  assert_memory_equal(bytes, expected, length);
  assert_int_equal(((const char *)bytes)[length], '\n');
  assert_int_equal(result.completed_records, 1);
  lc_sink_close(sink);
  lc_collection_read_res_cleanup(&result);
}

static void test_source_errors_are_unpublished_and_borrowed(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {0};
  lc_collection_append_res out;
  lc_collection_info info;
  input in;
  req.has_collection_retention_seconds = 1;
  req.collection_retention_seconds = 900;
  for (unsigned int failure = 0; failure < 6; ++failure) {
    char id[32];
    snprintf(id, sizeof(id), "failed-%u", failure);
    req.append_id = id;
    input_init(&in, f, f->body, BODY_BYTES);
    if (failure < 2) {
      in.fail = 1;
      in.fail_at = failure ? 196608 : 0;
    }
    if (failure == 2)
      in.oversized = 1;
    if (failure >= 3) {
      const char *bad[] = {"[]", "{", "{}false"};
      in.body = bad[failure - 3];
      in.length = strlen(in.body);
    }
    memset(&out, 0xff, sizeof(out));
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    assert_int_equal(rc, failure < 2 ? LC_ERR_TIMEOUT : LC_ERR_INVALID);
    if (failure < 2)
      assert_string_equal(f->error.message, "injected source failure");
    assert_true(out.position == 0 && in.resets == 0 && in.closes == 0);
    status_is(f, id, LC_COLLECTION_APPEND_ABSENT);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_int_equal(info.retention_expires_at_unix, 0);
    check(f, push(f->collection, id, "{}", 2, &out, &f->error));
    assert_int_equal(out.position, failure + 1U);
  }
  input_init(&in, f, "{}", 2);
  assert_int_equal(
      lc_collection_append_staged(f->collection, &req, NULL, &out, NULL),
      LC_ERR_INVALID);
  assert_int_equal(
      lc_collection_append_staged(f->collection, &req, &in.pub, NULL, NULL),
      LC_ERR_INVALID);
  assert_int_equal(
      lc_collection_append_staged(f->collection, NULL, &in.pub, &out, NULL),
      LC_ERR_INVALID);
  assert_int_equal(lc_collection_append_staged(NULL, &req, &in.pub, &out, NULL),
                   LC_ERR_INVALID);
  assert_int_equal(in.calls, 0);
}

static int clock_now(clockid_t id, struct timespec *out, void *context) {
  fixture *f = context;
  (void)id;
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}
static void test_codecs_retry_retention_and_restart(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {"entry", 1, 60, 1, 100};
  lc_collection_append_res out;
  lc_collection_delete_res deleted;
  lc_collection_capabilities caps;
  lc_collection_info info;
  input in;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  lc_pouch_test_queue_clock_gettime = clock_now;
  lc_pouch_test_queue_time_context = f;
  for (int mode = 0; mode < 8; ++mode) {
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/codec-%d", f->root,
             mode);
    f->now = 1000;
    open_owners(f, mode, 1);
    check(f, lc_client_collection_capabilities(f->client, &caps, &f->error));
    assert_true(caps.append_staged && caps.append_writer);
    input_init(&in, f, f->body, BODY_BYTES);
    in.chunk = 1007;
    check(f, f->collection->append_staged(f->collection, &req, &in.pub, &out,
                                          &f->error));
    assert_true(out.position == 1 && out.bytes == BODY_BYTES && !out.duplicate);
    assert_true(out.retention_expires_at_unix == 1060 &&
                out.collection_retention_expires_at_unix == 1100);
    assert_true(in.offset == BODY_BYTES &&
                in.calls == (BODY_BYTES + 1006) / 1007 + 1);
    assert_true(in.resets == 0 && in.closes == 0);
    f->now = 1010;
    input_init(&in, f, f->body, BODY_BYTES);
    check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error));
    assert_true(out.duplicate && out.position == 1 &&
                out.retention_expires_at_unix == 1060);
    check(f, lc_collection_delete_entry(f->collection, 1, &deleted, &f->error));
    input_init(&in, f, f->body, BODY_BYTES);
    check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error));
    assert_true(out.duplicate && out.deleted && out.position == 1);
    check(f, push(f->collection, "live", f->body, BODY_BYTES, &out, &f->error));
    assert_int_equal(out.position, 2);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
    status_is(f, "entry", LC_COLLECTION_APPEND_DELETED);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_true(info.live_count == 1 && info.retention_expires_at_unix == 1100);
  }
}

static int owner_close(input *in, lc_error *error) {
  fixture *f = in->f;
  lc_error nested;
  lc_collection_info info;
  lc_collection_append_res result;
  lc_collection_append_req req = {"nested"};
  lc_source *source = NULL;
  lc_error_init(&nested);
  assert_int_equal(lc_collection_describe(f->collection, &info, &nested),
                   LC_ERR_INVALID);
  assert_int_equal(lc_collection_describe(f->peer, &info, &nested), LC_OK);
  assert_int_equal(lc_source_from_memory("{}", 2, &source, &nested), LC_OK);
  assert_int_equal(
      lc_collection_append_staged(f->peer, &req, source, &result, &nested),
      LC_ERR_INVALID);
  lc_source_close(source);
  lc_error_cleanup(&nested);
  close_owners(f);
  (void)error;
  return LC_OK;
}
static void test_callbacks_reentry_and_owner_close(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {"survivor"};
  lc_collection_append_res out;
  input in;
  for (unsigned int failure = 0; failure < 2; ++failure) {
    input_init(&in, f, f->body, BODY_BYTES);
    in.action = owner_close;
    in.action_at = 196608;
    in.fail = failure;
    in.fail_at = 196608;
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    assert_int_equal(rc, failure ? LC_ERR_TIMEOUT : LC_OK);
    assert_true(in.acted && in.resets == 0 && in.closes == 0);
    open_owners(f, 0, 0);
    status_is(f, req.append_id,
              failure ? LC_COLLECTION_APPEND_ABSENT
                      : LC_COLLECTION_APPEND_COMMITTED);
    if (!failure)
      read_one(f, f->body, BODY_BYTES);
    req.append_id = "failed-after-close";
  }
}

static void *acquire_peer(void *context) {
  input *in = context;
  lc_collection_lease_req req = {"peer", 30, 0};
  lc_error error;
  lc_error_init(&error);
  int rc = lc_collection_acquire(in->f->peer, &req, &in->f->owner, &error);
  lc_error_cleanup(&error);
  return (void *)(intptr_t)rc;
}
static int acquire_during_source(input *in, lc_error *error) {
  pthread_t thread;
  void *result;
  (void)error;
  assert_int_equal(pthread_create(&thread, NULL, acquire_peer, in), 0);
  assert_int_equal(pthread_join(thread, &result), 0);
  assert_int_equal((intptr_t)result, LC_OK);
  return LC_OK;
}
static void test_source_ownership_change_is_fenced(void **state) {
  fixture *f = *state;
  input in;
  lc_collection_append_req req = {"fenced"};
  lc_collection_append_res out;
  input_init(&in, f, f->body, BODY_BYTES);
  in.action = acquire_during_source;
  in.action_at = 196608;
  assert_int_equal(lc_collection_append_staged(f->collection, &req, &in.pub,
                                               &out, &f->error),
                   LC_ERR_INVALID);
  assert_true(in.acted && out.position == 0);
  status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
  check(f, lc_collection_release(f->owner, &f->error));
  f->owner = NULL;
  input_init(&in, f, "{}", 2);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_int_equal(out.position, 1);
}

typedef struct slow_group slow_group;
typedef struct slow_worker {
  slow_group *group;
  unsigned int number;
  int rc;
  lc_collection_append_res result;
} slow_worker;
struct slow_group {
  fixture *f;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  unsigned int ready;
  int release, peer_done, peer_rc;
};

static int wait_for(slow_group *group, int peer) {
  struct timespec deadline;
  int rc = 0;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&group->mutex);
  while ((peer ? !group->peer_done : group->ready < 4) && rc == 0)
    rc = pthread_cond_timedwait(&group->condition, &group->mutex, &deadline);
  pthread_mutex_unlock(&group->mutex);
  return rc;
}
static int slow_input(input *in, lc_error *error) {
  slow_group *group = in->context;
  struct timespec deadline;
  int rc = 0;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 10;
  pthread_mutex_lock(&group->mutex);
  ++group->ready;
  pthread_cond_broadcast(&group->condition);
  while (!group->release && rc == 0)
    rc = pthread_cond_timedwait(&group->condition, &group->mutex, &deadline);
  pthread_mutex_unlock(&group->mutex);
  return rc == 0 ? LC_OK
                 : lc_error_set(error, LC_ERR_TIMEOUT, 0,
                                "slow producer rendezvous timed out", NULL,
                                NULL, NULL);
}
static void *slow_producer(void *context) {
  slow_worker *work = context;
  fixture *f = work->group->f;
  char id[32];
  lc_error error;
  lc_collection_append_req req = {0};
  lc_collection_writer *writer = NULL;
  input in;
  lc_error_init(&error);
  snprintf(id, sizeof(id), "worker-%u", work->number);
  req.append_id = id;
  input_init(&in, f, f->body, BODY_BYTES);
  in.action = slow_input;
  in.action_at = 196608;
  in.context = work->group;
  in.fail = work->number == 2;
  in.fail_at = 196608;
  if (work->number % 2 == 0) {
    work->rc = lc_collection_append_staged(f->collection, &req, &in.pub,
                                           &work->result, &error);
  } else {
    work->rc = lc_collection_append_begin(f->collection, &req, &writer, &error);
    for (size_t at = 0; work->rc == LC_OK && at < 196608; at += 16384)
      work->rc =
          lc_collection_writer_write(writer, f->body + at, 16384, &error);
    if (work->rc == LC_OK)
      work->rc = slow_input(&in, &error);
    if (work->rc == LC_OK && work->number == 3)
      work->rc = lc_collection_writer_commit(writer, &work->result,
                                             &error); /* Truncated JSON. */
    else if (work->rc == LC_OK) {
      work->rc = lc_collection_writer_write(writer, f->body + 196608,
                                            BODY_BYTES - 196608, &error);
      if (work->rc == LC_OK)
        work->rc = lc_collection_writer_commit(writer, &work->result, &error);
    }
    lc_collection_writer_close(writer);
  }
  lc_error_cleanup(&error);
  return NULL;
}
static void *fast_peer(void *context) {
  slow_group *group = context;
  fixture *f = group->f;
  lc_collection_append_res out;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  lc_error error;
  lc_error_init(&error);
  int rc = push(f->peer, "fast-peer", "{}", 2, &out, &error);
  options.ns = "journal";
  options.force = 1;
  if (rc == LC_OK)
    rc = lc_pouch_maintenance_run(((lc_client_handle *)f->client)->pouch,
                                  &options, &result, &error);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  lc_error_cleanup(&error);
  pthread_mutex_lock(&group->mutex);
  group->peer_rc = rc;
  group->peer_done = 1;
  pthread_cond_broadcast(&group->condition);
  pthread_mutex_unlock(&group->mutex);
  return NULL;
}
static void
test_mixed_slow_producers_allow_peer_commit_and_compaction(void **state) {
  fixture *f = *state;
  for (int durable = 0; durable < 2; ++durable) {
    if (durable) {
      close_owners(f);
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/durable", f->root);
      open_owners(f, 4, 1);
    }
    slow_group group = {0};
    slow_worker workers[4] = {{0}};
    pthread_t threads[4], peer;
    group.f = f;
    assert_int_equal(pthread_mutex_init(&group.mutex, NULL), 0);
    assert_int_equal(pthread_cond_init(&group.condition, NULL), 0);
    for (unsigned int i = 0; i < 4; ++i) {
      workers[i].group = &group;
      workers[i].number = i;
      assert_int_equal(
          pthread_create(&threads[i], NULL, slow_producer, &workers[i]), 0);
    }
    int ready_rc = wait_for(&group, 0), peer_rc = -1;
    if (ready_rc == 0) {
      assert_int_equal(pthread_create(&peer, NULL, fast_peer, &group), 0);
      peer_rc = wait_for(&group, 1);
    }
    pthread_mutex_lock(&group.mutex);
    group.release = 1;
    pthread_cond_broadcast(&group.condition);
    pthread_mutex_unlock(&group.mutex);
    for (unsigned int i = 0; i < 4; ++i)
      assert_int_equal(pthread_join(threads[i], NULL), 0);
    if (ready_rc == 0)
      assert_int_equal(pthread_join(peer, NULL), 0);
    pthread_cond_destroy(&group.condition);
    pthread_mutex_destroy(&group.mutex);
    assert_int_equal(ready_rc, 0);
    assert_int_equal(peer_rc, 0);
    assert_int_equal(group.peer_rc, LC_OK);
    assert_int_equal(workers[0].rc, LC_OK);
    assert_int_equal(workers[1].rc, LC_OK);
    assert_int_equal(workers[2].rc, LC_ERR_TIMEOUT);
    assert_int_equal(workers[3].rc, LC_ERR_INVALID);
    assert_true(workers[0].result.position != workers[1].result.position);
    close_owners(f);
    open_owners(f, durable ? 4 : 0, 0);
    lc_collection_info info;
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_true(info.live_count == 3 && info.upper_position == 3);
    status_is(f, "worker-0", LC_COLLECTION_APPEND_COMMITTED);
    status_is(f, "worker-1", LC_COLLECTION_APPEND_COMMITTED);
    status_is(f, "worker-2", LC_COLLECTION_APPEND_ABSENT);
    status_is(f, "worker-3", LC_COLLECTION_APPEND_ABSENT);
  }
}

static int fail_sync(int fd) {
  (void)fd;
  errno = EIO;
  return -1;
}
static void test_uncertain_sync_retains_cross_api_retry_identity(void **state) {
  fixture *f = *state;
  close_owners(f);
  open_owners(f, 4, 0);
  lc_collection_append_req req = {"uncertain"};
  lc_collection_append_res out;
  input in;
  input_init(&in, f, "{}", 2);
  lc_pouch_test_sync_fd = fail_sync;
  int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error);
  lc_pouch_test_sync_fd = NULL;
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(out.position, 1);
  status_is(f, req.append_id, LC_COLLECTION_APPEND_COMMITTED);
  check(f, push(f->collection, req.append_id, "{}", 2, &out, &f->error));
  assert_true(out.position == 1 && out.duplicate);
  input_init(&in, f, "{}", 2);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_true(out.position == 1 && out.duplicate && in.resets == 0 &&
              in.closes == 0);
}

static void *allocate(void *context, size_t size) {
  allocations *stats = context;
  int denied = stats->fail || (stats->deny_size && size == stats->deny_size);
  void *bytes = denied ? NULL : malloc(size);
  if (denied)
    ++stats->denied;
  if (bytes != NULL && size > stats->maximum)
    stats->maximum = size;
  if (bytes != NULL)
    ++stats->live;
  return bytes;
}
static void release(void *context, void *bytes) {
  allocations *stats = context;
  if (bytes != NULL) {
    --stats->live;
    free(bytes);
  }
}
static void *resize(void *context, void *bytes, size_t size) {
  allocations *stats = context;
  if (bytes == NULL)
    return allocate(context, size);
  if (size == 0) {
    release(context, bytes);
    return NULL;
  }
  int denied = stats->fail || (stats->deny_size && size == stats->deny_size);
  void *result = denied ? NULL : realloc(bytes, size);
  if (denied)
    ++stats->denied;
  if (result != NULL && size > stats->maximum)
    stats->maximum = size;
  return result;
}
static int fail_allocation(input *in, lc_error *error) {
  allocations *stats = in->context;
  (void)error;
  stats->fail = 1;
  return LC_OK;
}
static void test_allocation_failure_closes_internal_writer(void **state) {
  fixture *f = *state;
  allocations *stats = &f->allocation_stats;
  close_owners(f);
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = stats;
  open_owners(f, 0, 0);
  lc_collection_append_req req = {"allocation-failed"};
  lc_collection_append_res out;
  input in;
  stats->maximum = 0;
  input_init(&in, f, f->body, BODY_BYTES);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_true(stats->maximum <= 65536U && in.closes == 0 && in.resets == 0);
  req.append_id = "failed-allocation";
  for (unsigned int during = 0; during < 2; ++during) {
    input_init(&in, f, f->body, BODY_BYTES);
    if (during) {
      in.action = fail_allocation;
      in.context = stats;
    } else
      stats->fail = 1;
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    stats->fail = 0;
    assert_int_equal(rc, LC_ERR_NOMEM);
    assert_true(in.resets == 0 && in.closes == 0 && out.position == 0);
    status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
  }
  close_owners(f);
  assert_int_equal(stats->live, 0);
  memset(&f->allocator, 0, sizeof(f->allocator));
}

static int fail_binding_allocation(void *context, lc_collection *collection,
                                   int phase, lc_error *error) {
  (void)collection;
  (void)error;
  if (phase == LC_COLLECTION_APPEND_PUBLISHED)
    ((allocations *)context)->fail = 1;
  return LC_OK;
}

static void test_chunk_checkpoint_preserves_missing_binding(void **state) {
  fixture *f = *state;
  allocations *stats = &f->allocation_stats;
  lc_collection_append_res out;
  lc_collection_append_req req = {"peer-stream"};
  lc_collection_writer *peer = NULL;
  lc_collection_read_opts opts = {0};
  lc_collection_read_res result;
  lc_collection_append_status_res status;
  lc_sink *sink = NULL;
  const void *bytes;
  size_t count;
  close_owners(f);
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = stats;
  open_owners(f, 0, 0);
  for (unsigned int i = 0; i < 32; ++i) {
    char id[32];
    snprintf(id, sizeof(id), "prefix-%u", i);
    check(f, push(f->collection, id, "{}", 2, &out, &f->error));
  }
  lc_collection_handle *handle = f->collection->impl;
  assert_int_equal(handle->ids.count, handle->ids.capacity / 2);
  check(f, lc_collection_append_begin(f->peer, &req, &peer, &f->error));
  check(f, lc_collection_writer_write(peer, f->body, 16384, &f->error));
  lc_collection_append_test_hook = fail_binding_allocation;
  lc_collection_append_test_context = stats;
  int rc = push(f->collection, "missing-binding", "{}", 2, &out, &f->error);
  stats->fail = 0;
  lc_collection_append_test_hook = NULL;
  lc_collection_append_test_context = NULL;
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_int_equal(out.position, 33);
  assert_int_equal(handle->order.count, 33);
  assert_int_equal(handle->ids.count, 32);
  /* A different, already-admitted handle checkpoints raw progress beyond
   * this incomplete projection. Reconciliation must still find the binding. */
  for (size_t at = 16384; at < BODY_BYTES; at += 4096)
    check(f, lc_collection_writer_write(peer, f->body + at, 4096, &f->error));
  check(f, lc_collection_append_status(f->peer, "missing-binding", &status,
                                       &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
  assert_int_equal(status.entry.position, 33);
  check(f, lc_collection_writer_commit(peer, &out, &f->error));
  assert_int_equal(out.position, 34);
  lc_collection_writer_close(peer);
  opts.has_after = 1;
  opts.after = 33;
  lc_collection_read_res_init(&result);
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(f->collection, &opts, sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &count, &f->error));
  assert_int_equal(result.completed_records, 1);
  assert_int_equal(count, BODY_BYTES + 1);
  assert_memory_equal(bytes, f->body, BODY_BYTES);
  lc_sink_close(sink);
  lc_collection_read_res_cleanup(&result);
  close_owners(f);
  open_owners(f, 0, 0);
  status_is(f, "missing-binding", LC_COLLECTION_APPEND_COMMITTED);
  status_is(f, "peer-stream", LC_COLLECTION_APPEND_COMMITTED);
  close_owners(f);
  assert_int_equal(stats->live, 0);
  memset(&f->allocator, 0, sizeof(f->allocator));
}

static struct {
  unsigned char type;
  int mode, calls, fd;
  off_t offset;
} header_fault;
static ssize_t fail_extent_header(int fd, const void *bytes, size_t count,
                                  off_t offset) {
  const unsigned char *p = bytes;
  if (!header_fault.calls && count == LC_COLLECTION_CHUNK_HEADER &&
      p[5] == header_fault.type) {
    header_fault.calls = 1;
    header_fault.fd = fd;
    header_fault.offset = offset;
    if (header_fault.mode == 0 || header_fault.mode == 4) {
      errno = header_fault.mode == 4 ? EINTR : EIO;
      return -1;
    }
    if (header_fault.mode == 1)
      return 0;
    /* Clear PENDING but leave its old checksum: a following I/O failure
     * must remove this partial header before append authority is released. */
    return pwrite(fd, bytes, 8, offset);
  }
  if (header_fault.calls == 1 && fd == header_fault.fd &&
      offset == header_fault.offset + (header_fault.mode == 4 ? 0 : 8)) {
    ++header_fault.calls;
    if (header_fault.mode == 2) {
      errno = EIO;
      return -1;
    }
    if (header_fault.mode == 3)
      return 0;
  }
  return pwrite(fd, bytes, count, offset);
}
static void test_failed_extent_headers_do_not_poison_collection(void **state) {
  fixture *f = *state;
  lc_collection_append_res prior, out;
  lc_collection_append_status_res status;
  lc_collection_read_opts opts = {0};
  lc_collection_read_res result;
  lc_sink *sink = NULL;
  const void *bytes;
  size_t count;
  for (int durable = 0; durable < 2; ++durable) {
    close_owners(f);
    open_owners(f, durable ? 4 : 0, 0);
    for (int kind = 0; kind < 2; ++kind) {
      for (int mode = 0; mode < 6; ++mode) {
        char id[64], prior_id[64];
        snprintf(id, sizeof(id), "header-%d-%d-%d", durable, kind, mode);
        snprintf(prior_id, sizeof(prior_id), "prior-header-%d-%d-%d", durable,
                 kind, mode);
        check(f, push(f->collection, prior_id, "{}", 2, &prior, &f->error));
        memset(&header_fault, 0, sizeof(header_fault));
        header_fault.type =
            kind ? LC_POUCH_COLLECTION_NODE : LC_POUCH_COLLECTION_CHUNK;
        header_fault.mode = mode;
        ((lc_collection_handle *)f->collection->impl)
            ->pouch->segment_target_bytes = 131072U;
        lc_collection_chunks_test_pwrite = fail_extent_header;
        memset(&out, 0, sizeof(out));
        int rc = push(f->collection, id, f->body, BODY_BYTES, &out, &f->error);
        lc_collection_chunks_test_pwrite = NULL;
        assert_int_equal(header_fault.calls, mode < 2 ? 1 : 2);
        assert_int_equal(rc, mode < 4 ? LC_ERR_TRANSPORT : LC_OK);
        if (mode < 4)
          assert_int_equal(out.position, 0);
        close_owners(f);
        open_owners(f, durable ? 4 : 0, 0);
        status_is(f, prior_id, LC_COLLECTION_APPEND_COMMITTED);
        check(f, lc_collection_append_status(f->collection, id, &status,
                                             &f->error));
        assert_int_equal(status.state, mode < 4
                                           ? LC_COLLECTION_APPEND_ABSENT
                                           : LC_COLLECTION_APPEND_COMMITTED);
        check(f, push(f->collection, id, f->body, BODY_BYTES, &out, &f->error));
        assert_int_equal(out.position, prior.position + 1);
        assert_int_equal(out.duplicate, mode >= 4);
        opts.has_after = 1;
        opts.after = prior.position;
        lc_collection_read_res_init(&result);
        check(f, lc_sink_to_memory(&sink, &f->error));
        check(f, lc_collection_read(f->collection, &opts, sink, &result,
                                    &f->error));
        check(f, lc_sink_memory_bytes(sink, &bytes, &count, &f->error));
        assert_int_equal(result.completed_records, 1);
        assert_int_equal(count, BODY_BYTES + 1);
        assert_memory_equal(bytes, f->body, BODY_BYTES);
        lc_sink_close(sink);
        sink = NULL;
        lc_collection_read_res_cleanup(&result);
      }
    }
    /* Failed private final headers are intentionally unreadable. Conversion
     * must discover published roots first and ignore those orphan bytes. */
    close_owners(f);
    open_owners(f, durable ? 6 : 2, 0);
    lc_collection_info info;
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_int_equal(info.live_count, out.position);
    assert_int_equal(info.upper_position, out.position);
    close_owners(f);
    open_owners(f, durable ? 4 : 0, 0);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_int_equal(info.live_count, out.position);
  }
}

/* Generated input and a checking sink keep this test independent of an
 * entry-sized source/sink. Denying every SDK allocation while feeding the
 * string catches both body growth and allocation on each fragment. */
#define STREAM_BYTES (1024U * 1024U)
typedef struct stream_check {
  lc_sink sink;
  size_t offset, records;
  unsigned long random;
} stream_check;
static unsigned char stream_next(unsigned long *random) {
  static const char alphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
  *random = (*random * 1664525UL + 1013904223UL) & 0xffffffffUL;
  return (unsigned char)alphabet[(*random >> 24) & 63];
}
static int stream_output(lc_sink *sink, const void *bytes, size_t length,
                         lc_error *error) {
  stream_check *check = sink->impl;
  const unsigned char *p = bytes;
  (void)error;
  assert_true(length <= 65536);
  for (size_t i = 0; i < length; ++i, ++check->offset) {
    unsigned char expected;
    if (check->offset == STREAM_BYTES + 9) {
      check->offset = 0;
      check->random = 17;
    }
    if (check->offset < 6)
      expected = (unsigned char)"{\"v\":\""[check->offset];
    else if (check->offset < STREAM_BYTES + 6)
      expected = stream_next(&check->random);
    else {
      assert_true(check->offset < STREAM_BYTES + 9);
      expected = (unsigned char)"\"}\n"[check->offset - STREAM_BYTES - 6];
    }
    assert_int_equal(p[i], expected);
    if (check->offset == STREAM_BYTES + 8)
      ++check->records;
  }
  return 1;
}
static void stream_verify_records(fixture *f, size_t records) {
  stream_check output = {0};
  lc_collection_read_res result;
  output.random = 17;
  output.sink.write = stream_output;
  output.sink.impl = &output;
  lc_collection_read_res_init(&result);
  check(f, lc_collection_read(f->collection, NULL, &output.sink, &result,
                              &f->error));
  assert_int_equal(output.offset, STREAM_BYTES + 9);
  assert_int_equal(result.completed_records, records);
  assert_int_equal(output.records, records);
  lc_collection_read_res_cleanup(&result);
}
static void stream_verify(fixture *f) { stream_verify_records(f, 1); }
#ifdef __linux__
static unsigned long long stream_io(const char *wanted) {
  FILE *fp = fopen("/proc/self/io", "r");
  char key[64];
  unsigned long long value = 0, answer = 0;
  assert_non_null(fp);
  while (fscanf(fp, "%63s %llu", key, &value) == 2)
    if (!strcmp(key, wanted))
      answer = value;
  assert_int_equal(fclose(fp), 0);
  return answer;
}
#endif
static void run_stream_allocation_guard(fixture *f, int conversion) {
  allocations *stats = &f->allocation_stats;
  unsigned char *chunk = f->stream_chunk = malloc(4096);
  lc_collection_append_req req = {"streaming"};
  lc_collection_append_res result;
  struct stat before, after;
  assert_non_null(chunk);
  close_owners(f);
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = stats;
  for (int mode = 0; mode < 8; ++mode) {
    unsigned long random = 17;
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/stream-%d", f->root,
             mode);
    open_owners(f, mode, 1);
    lc_collection_handle *handle = f->collection->impl;
    handle->pouch->segment_target_bytes =
        conversion ? 2U * STREAM_BYTES : 65536U;
    stats->maximum = 0;
    check(f, lc_collection_append_begin(f->collection, &req, &f->pending,
                                        &f->error));
    /* One 64 KiB chunk plus GCM frame and terminal prefixes/tags (48 bytes).
     * The admission bound must stay independent of the 1 MiB entry size. */
    assert_uint_in_range(stats->maximum, 1U, 65536U + 48U);
    check(f, lc_collection_writer_write(f->pending, "{\"v\":\"", 6, &f->error));
    stats->denied = denied_builtin_attempts = 0;
    stats->fail = 1;
    guard_builtin_allocations(f, 1);
    for (size_t at = 0; at < STREAM_BYTES; at += 4096) {
      for (size_t i = 0; i < 4096; ++i)
        chunk[i] = stream_next(&random);
      int rc = lc_collection_writer_write(f->pending, chunk, 4096, &f->error);
      guard_builtin_allocations(f, 0);
      stats->fail = 0; /* Keep diagnostics and teardown outside the guard. */
      check(f, rc);
      stats->fail = 1;
      guard_builtin_allocations(f, 1);
    }
    int rc = lc_collection_writer_write(f->pending, "\"}", 2, &f->error);
    guard_builtin_allocations(f, 0);
    stats->fail = 0;
    check(f, rc);
    assert_int_equal(stats->denied, 0);
    assert_int_equal(denied_builtin_attempts, 0);
    assert_int_equal(fstat(handle->active_fd, &before), 0);
    lc_u64 allocated = 0;
    int directory = open(handle->directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    assert_true(directory >= 0);
    for (lc_u64 segment = 1; segment <= handle->head.active; ++segment) {
      struct stat st;
      char leaf[64];
      check(f, lc_collection_record_leaf(segment, leaf, &f->error));
      assert_int_equal(fstatat(directory, leaf, &st, 0), 0);
      allocated += (lc_u64)st.st_blocks * 512U;
    }
    assert_int_equal(close(directory), 0);
    assert_true(allocated >= STREAM_BYTES / 2);
    if (!conversion)
      assert_true(handle->head.active > 1);
    status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
#ifdef __linux__
    unsigned long long reads = stream_io("rchar:"),
                       writes = stream_io("wchar:");
#endif
    check(f, lc_collection_writer_commit(f->pending, &result, &f->error));
#ifdef __linux__
    assert_true(stream_io("rchar:") - reads < 32768);
    assert_true(stream_io("wchar:") - writes < 131072);
#endif
    assert_int_equal(fstat(handle->active_fd, &after), 0);
    /* A final bounded codec fragment may reserve a sparse canonical range.
     * Its address space is not a payload copy: guard allocated blocks as well
     * as the actual process reads/writes above. */
    assert_true((after.st_blocks - before.st_blocks) * 512 < 131072);
    assert_int_equal(result.bytes, STREAM_BYTES + 8);
    lc_collection_writer_close(f->pending);
    f->pending = NULL;
    stream_verify(f);
    close_owners(f);
    open_owners(f, mode, 0);
    stream_verify(f);
    close_owners(f);
    if (conversion) {
      /* Separate conversion coverage from the rollover/allocation regression
       * so each invariant has an independently selectable failure. */
      f->segment_target = 65536U;
      open_owners(f, mode ^ 2, 0);
      stream_verify(f);
      close_owners(f);
      open_owners(f, mode, 0);
      stream_verify(f);
      close_owners(f);
      f->segment_target = 0;
    }
    assert_int_equal(stats->live, 0);
  }
  memset(&f->allocator, 0, sizeof(f->allocator));
  free(f->stream_chunk);
  f->stream_chunk = NULL;
}

static void
test_push_is_canonical_before_commit_without_payload_allocations(void **state) {
  run_stream_allocation_guard(*state, 0);
}
static void test_stream_units_survive_compression_conversion(void **state) {
  run_stream_allocation_guard(*state, 1);
}

typedef struct direct_check {
  fixture *f;
  size_t offset, calls, largest, fail_at;
  int initial_empty;
} direct_check;

static int direct_output(lc_sink *sink, const void *bytes, size_t length,
                         lc_error *error) {
  direct_check *output = sink->impl;
  const unsigned char *p = bytes;
  assert_true(length <= 65536);
  ++output->calls;
  if (length > output->largest)
    output->largest = length;
  if (output->calls == output->fail_at) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0, "injected read sink rejection",
                 NULL, NULL, NULL);
    return 0;
  }
  for (size_t i = 0; i < length; ++i, ++output->offset) {
    size_t at = output->offset;
    if (output->initial_empty) {
      if (at < 3U) {
        assert_int_equal(p[i], "{}\n"[at]);
        continue;
      }
      at -= 3U;
    }
    assert_true(at <= BODY_BYTES);
    assert_int_equal(p[i], at == BODY_BYTES ? '\n' : output->f->body[at]);
  }
  return 1;
}

static void direct_verify(fixture *f, int filter, int expected) {
  direct_check output = {0};
  lc_sink sink = {0};
  lc_collection_read_opts options = {0};
  lc_collection_read_res result;
  output.f = f;
  options.selector_lql = filter ? "exists{/v}" : NULL;
  sink.write = direct_output;
  sink.impl = &output;
  lc_collection_read_res_init(&result);
  int rc =
      lc_collection_read(f->collection, &options, &sink, &result, &f->error);
  assert_int_equal(rc, expected);
  assert_int_equal(result.completed_records, expected == LC_OK ? 1 : 0);
  assert_int_equal(output.offset, expected == LC_OK ? BODY_BYTES + 1U : 0U);
  lc_collection_read_res_cleanup(&result);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}

static lc_collection_order_row direct_row(fixture *f) {
  lc_collection_order_row row;
  lc_collection_info info;
  int found;
  lc_collection_handle *c = f->collection->impl;
  check(f, lc_collection_describe(f->collection, &info, &f->error));
  check(f, lc_collection_order_find(&c->order, 1, &row, &found, &f->error));
  assert_true(found);
  return row;
}

static void test_direct_reader_bounded_chunks_failure_and_resume(void **state) {
  fixture *f = *state;
  allocations *stats = &f->allocation_stats;
  lc_collection_append_res appended;
  unsigned long random = 17;
  for (size_t i = 6; i < BODY_BYTES - 2U; ++i)
    f->body[i] = (char)stream_next(&random);
  close_owners(f);
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = stats;
  f->segment_target = 2U * 1024U * 1024U;
  for (int mode = 0; mode < 4; ++mode) {
    char cursor[LC_COLLECTION_CURSOR_BYTES];
    direct_check output = {0};
    lc_sink sink = {0};
    lc_collection_read_opts options = {0};
    lc_collection_read_res result;
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/read-chunks-%d",
             f->root, mode);
    open_owners(f, mode, 1);
    check(f, push(f->collection, "small-read", "{}", 2, &appended, &f->error));
    check(f, push(f->collection, "bounded-read", f->body, BODY_BYTES, &appended,
                  &f->error));
    assert_int_equal(appended.position, 2);
    output.f = f;
    output.initial_empty = 1;
    sink.impl = &output;
    sink.write = direct_output;
    size_t live = stats->live;
    stats->deny_size = 65536;
    stats->denied = 0;
    lc_collection_read_res_init(&result);
    int rc = lc_collection_read(f->collection, NULL, &sink, &result, &f->error);
    stats->deny_size = 0;
    assert_int_equal(rc, LC_ERR_NOMEM);
    assert_int_equal(stats->denied, 1);
    assert_int_equal(output.offset, 3);
    assert_int_equal(result.completed_records, 1);
    assert_int_equal(result.last_examined_position, 1);
    assert_non_null(result.resume_cursor);
    assert_true(strlen(result.resume_cursor) < sizeof(cursor));
    strcpy(cursor, result.resume_cursor);
    lc_collection_read_res_cleanup(&result);
    lc_error_cleanup(&f->error);
    lc_error_init(&f->error);
    assert_int_equal(stats->live, live);

    memset(&output, 0, sizeof(output));
    output.f = f;
    output.fail_at = 2;
    options.cursor = cursor;
    stats->maximum = 0;
    lc_collection_read_res_init(&result);
    rc = lc_collection_read(f->collection, &options, &sink, &result, &f->error);
    assert_int_equal(rc, LC_ERR_TRANSPORT);
    assert_uint_in_range(output.offset, 1U, 65536U);
    assert_false(result.complete);
    assert_int_equal(result.completed_records, 0);
    assert_int_equal(result.last_examined_position, 1);
    assert_null(result.checkpoint_cursor);
    assert_non_null(result.resume_cursor);
    assert_true(strlen(result.resume_cursor) < sizeof(cursor));
    strcpy(cursor, result.resume_cursor);
    lc_collection_read_res_cleanup(&result);
    lc_error_cleanup(&f->error);
    lc_error_init(&f->error);

    memset(&output, 0, sizeof(output));
    output.f = f;
    options.cursor = cursor;
    lc_collection_read_res_init(&result);
    check(f, lc_collection_read(f->collection, &options, &sink, &result,
                                &f->error));
    assert_true(result.complete);
    assert_int_equal(result.completed_records, 1);
    assert_int_equal(output.offset, BODY_BYTES + 1U);
    assert_int_equal(output.largest, 65536);
    /* AES can deliver its short first frame separately to decrypt subsequent
     * complete frames directly into output. Still forbid many small cycles. */
    assert_true(output.calls <= BODY_BYTES / 65536U + (mode == 1 ? 2U : 1U));
    assert_true(stats->maximum < BODY_BYTES);
    lc_collection_read_res_cleanup(&result);
    direct_verify(f, 1, LC_OK);
    close_owners(f);
    assert_int_equal(stats->live, 0);
  }
  memset(&f->allocator, 0, sizeof(f->allocator));
}

static void
test_direct_units_filter_metadata_corruption_and_rebuild(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  lc_collection_metadata_req metadata = {0};
  lc_pouch_maintenance_options maintenance = {0};
  lc_pouch_maintenance_result maintained = {0};
  unsigned long random = 17;
  for (size_t i = 6; i < BODY_BYTES - 2U; ++i)
    f->body[i] = (char)stream_next(&random);
  close_owners(f);
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  f->segment_target = 2U * 1024U * 1024U;
  maintenance.ns = "journal";
  maintenance.force = 1;
  for (int mode = 0; mode < 4; ++mode) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/direct-%d", f->root,
             mode);
    open_owners(f, mode, 1);
    check(f, push(f->collection, "direct", f->body, BODY_BYTES, &result,
                  &f->error));
    lc_collection_handle *c = f->collection->impl;
    lc_collection_order_row row = direct_row(f);
    assert_true(row.unit_segment != 0);
    direct_verify(f, 0, LC_OK);
    /* LQL consumes the borrowed prefix first; presentation must then reopen
     * from the fd rather than replay plaintext-overwritten input scratch. */
    direct_verify(f, 1, LC_OK);
    char *path = NULL, leaf[64];
    check(f, lc_collection_record_path(c, row.unit_segment, &path, leaf,
                                       &f->error));
    int fd = open(path, O_RDWR);
    lc_free_with_allocator(&c->allocator, path);
    assert_true(fd >= 0);
    lc_collection_unit original, altered;
    check(f, lc_collection_unit_read(fd, row.unit_offset, row.stored_bytes,
                                     &original, &f->error));
    for (int corrupt = 0; corrupt < 5; ++corrupt) {
      altered = original;
      if (corrupt == 0)
        ++altered.ordinal;
      if (corrupt == 1)
        ++altered.plain;
      if (corrupt == 2)
        altered.etag[0] = altered.etag[0] == 'a' ? 'b' : 'a';
      if (corrupt == 3)
        altered.context[0] = 'x';
      if (corrupt == 4) {
        /* Keep a well-formed upload identity. Its changed manifest must still
         * fail the order proof or encrypted descriptor binding. */
        char *id = strstr(altered.context, ":direct:");
        assert_non_null(id);
        id[1] = 'e';
      }
      check(f, lc_collection_unit_write_meta(fd, row.unit_offset, &altered,
                                             &f->error));
      direct_verify(f, corrupt & 1, LC_ERR_PROTOCOL);
    }
    check(f, lc_collection_unit_write_meta(fd, row.unit_offset, &original,
                                           &f->error));
    assert_int_equal(close(fd), 0);
    metadata.has_retention_seconds = 1;
    metadata.retention_seconds = 3600;
    check(f, lc_collection_entry_metadata(f->collection, 1, &metadata, &result,
                                          &f->error));
    row = direct_row(f);
    assert_int_equal(row.unit_segment, 0);
    direct_verify(f, 1, LC_OK);
    metadata.retention_seconds = 0;
    check(f, lc_collection_entry_metadata(f->collection, 1, &metadata, &result,
                                          &f->error));
    row = direct_row(f);
    assert_int_equal(row.unit_segment, 0);
    direct_verify(f, 1, LC_OK);
    check(f, lc_pouch_maintenance_run(c->pouch, &maintenance, &maintained,
                                      &f->error));
    assert_int_equal(maintained.compacted_collection_count, 1);
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    row = direct_row(f);
    assert_true(row.unit_segment & LC_COLLECTION_SNAPSHOT_BIT);
    assert_int_equal(row.unit_offset, 0);
    direct_verify(f, 1, LC_OK);
    char order_path[1024];
    assert_true(strlen(c->order_path) < sizeof(order_path));
    strcpy(order_path, c->order_path);
    close_owners(f);
    /* A CRC-valid former projection layout must rebuild canonically. */
    unsigned char root[LC_COLLECTION_ORDER_ROOT_BYTES];
    fd = open(order_path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, root, sizeof(root), 0), sizeof(root));
    root[152] = 0;
    unsigned long crc = crc32(0L, root, sizeof(root) - 4U);
    for (size_t i = 0; i < 4; ++i)
      root[sizeof(root) - 4U + i] = (unsigned char)(crc >> (i * 8U));
    assert_int_equal(pwrite(fd, root, sizeof(root), 0), sizeof(root));
    assert_int_equal(close(fd), 0);
    open_owners(f, mode, 0);
    row = direct_row(f);
    assert_true(row.unit_segment & LC_COLLECTION_SNAPSHOT_BIT);
    direct_verify(f, 1, LC_OK);
    close_owners(f);
  }
}

static void
test_stream_units_survive_rolling_snapshots_and_rebuild(void **state) {
  fixture *f = *state;
  unsigned char *chunk = f->stream_chunk = malloc(4096);
  lc_collection_append_res result;
  lc_collection_metadata_req metadata = {0};
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result maintained = {0};
  assert_non_null(chunk);
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  options.ns = "journal";
  options.force = 1;
  metadata.has_retention_seconds = 1;
  close_owners(f);
  for (int mode = 0; mode < 4; ++mode) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/snapshot-units-%d",
             f->root, mode);
    open_owners(f, mode | 4, 1);
    lc_collection_handle *c = f->collection->impl;
    c->pouch->segment_target_bytes = 512U * 1024U;
    c->pouch->compaction_delete_grace_seconds = 0;
    for (int entry = 0; entry < 2; ++entry) {
      lc_collection_append_req req = {0};
      char id[32];
      unsigned long random = 17;
      snprintf(id, sizeof(id), "entry-%d", entry);
      req.append_id = id;
      check(f, lc_collection_append_begin(f->collection, &req, &f->pending,
                                          &f->error));
      check(f,
            lc_collection_writer_write(f->pending, "{\"v\":\"", 6, &f->error));
      for (size_t at = 0; at < STREAM_BYTES; at += 4096) {
        for (size_t i = 0; i < 4096; ++i)
          chunk[i] = stream_next(&random);
        check(f,
              lc_collection_writer_write(f->pending, chunk, 4096, &f->error));
      }
      check(f, lc_collection_writer_write(f->pending, "\"}", 2, &f->error));
      check(f, lc_collection_writer_commit(f->pending, &result, &f->error));
      lc_collection_writer_close(f->pending);
      f->pending = NULL;
    }
    for (int pass = 0; pass < 2; ++pass) {
      /* Exercise aliases as well as direct roots, and compact an already
       * partitioned snapshot after every old segment has become reclaimable. */
      metadata.retention_seconds = 3600 + pass;
      check(f, lc_collection_entry_metadata(f->collection, 1, &metadata,
                                            &result, &f->error));
      check(f, lc_pouch_maintenance_run(c->pouch, &options, &maintained,
                                        &f->error));
      assert_int_equal(maintained.compacted_collection_count, 1);
      lc_pouch_maintenance_result_cleanup(NULL, &maintained);
      stream_verify_records(f, 2);
      assert_true(c->head.snapshot_extra_parts > 0);
      for (lc_u64 part = 0; part <= c->head.snapshot_extra_parts; ++part) {
        char *path = NULL, leaf[64];
        struct stat st;
        check(f, lc_collection_record_path(
                     c, (c->head.snapshot + part) | LC_COLLECTION_SNAPSHOT_BIT,
                     &path, leaf, &f->error));
        assert_int_equal(stat(path, &st), 0);
        assert_true(st.st_size > 0 &&
                    (lc_u64)st.st_size <=
                        c->pouch->segment_target_bytes + 65536U);
        if ((lc_u64)st.st_size > c->pouch->segment_target_bytes) {
          lc_pouch_collection_record record;
          int fd = open(path, O_RDONLY | O_CLOEXEC);
          assert_true(fd >= 0);
          check(f, lc_pouch_collection_record_read(fd, 0, (lc_u64)st.st_size,
                                                   leaf, &record, &f->error));
          assert_int_equal(record.kind, LC_POUCH_COLLECTION_CHUNK);
          assert_int_equal(record.next_offset, (lc_u64)st.st_size);
          assert_int_equal(close(fd), 0);
        }
        lc_free_with_allocator(&c->allocator, path);
      }
      char *order = strdup(c->order_path), *ids = strdup(c->ids_path);
      assert_non_null(order);
      assert_non_null(ids);
      close_owners(f);
      assert_int_equal(unlink(order), 0);
      assert_int_equal(unlink(ids), 0);
      free(order);
      free(ids);
      open_owners(f, mode | 4, 0);
      c = f->collection->impl;
      c->pouch->segment_target_bytes = 512U * 1024U;
      c->pouch->compaction_delete_grace_seconds = 0;
      stream_verify_records(f, 2);
    }
    metadata.retention_seconds = 7200;
    check(f, lc_collection_entry_metadata(f->collection, 1, &metadata, &result,
                                          &f->error));
    lc_unix_seconds deadline = result.retention_expires_at_unix;
    close_owners(f);
    for (int direction = 0; direction < 2; ++direction) {
      lc_collection_append_status_res status;
      f->segment_target = 512U * 1024U;
      open_owners(f, (direction ? mode : mode ^ 2) | 4, 0);
      stream_verify_records(f, 2);
      check(f, lc_collection_append_status(f->collection, "entry-0", &status,
                                           &f->error));
      assert_int_equal(status.entry.retention_expires_at_unix, deadline);
      close_owners(f);
    }
    f->segment_target = 0;
  }
  free(f->stream_chunk);
  f->stream_chunk = NULL;
}

typedef struct split_chunks {
  lc_sink sink;
  lc_collection_handle *collection;
  lc_collection_chunks *chunks;
  char context[256];
} split_chunks;
static int split_encoded(lc_sink *sink, const void *bytes, size_t length,
                         lc_error *error) {
  split_chunks *split = sink->impl;
  const unsigned char *p = bytes;
  /* One-byte independent plaintext units cross two complete carry levels
   * with only 4 KiB of JSON; metadata remains bounded independently of input.
   */
  for (size_t i = 0; i < length; ++i) {
    lc_collection_unit unit = {0};
    lc_u64 available;
    unit.plain = unit.stored = 1;
    unit.ordinal = split->chunks->extents;
    unit.crc = crc32(0L, p + i, 1);
    if (lc_collection_unit_context(split->context, unit.ordinal, unit.context,
                                   error) != LC_OK)
      return 0;
    if (lc_collection_chunks_prepare(split->collection, split->chunks, 1,
                                     &available, error) != LC_OK ||
        lc_collection_chunks_append(split->collection, split->chunks, p + i, 1,
                                    error) != LC_OK ||
        lc_collection_chunks_end_unit(split->collection, split->chunks, &unit,
                                      error) != LC_OK)
      return 0;
  }
  return 1;
}
static void build_deep_index(fixture *f, const char *expected, size_t size) {
  lc_collection_chunks *chunks = calloc(1, sizeof(*chunks));
  lc_collection_call call;
  lc_collection_guard guard;
  lc_pouch_collection_writer *encoder = NULL;
  lc_pouch_collection_record body;
  lc_collection_append_res result;
  split_chunks split = {0};
  assert_non_null(chunks);
  check(f, lc_collection_call_begin(f->collection, 1, &call, &f->error));
  check(f, lc_collection_append_enter(&call, &guard, &f->error));
  split.collection = call.collection;
  split.chunks = chunks;
  split.sink.write = split_encoded;
  split.sink.impl = &split;
  call.collection->pouch->segment_target_bytes = 4096;
  snprintf(split.context, sizeof(split.context), "collection:%s:deep:upload",
           call.collection->id);
  check(f, lc_pouch_collection_writer_open_sink(
               call.pouch, &split.sink, call.collection->id, "deep",
               split.context, 0, 0, 0, &encoder, &f->error));
  for (size_t at = 0; at < size;) {
    size_t n = size - at < 127 ? size - at : 127;
    check(f, lc_pouch_collection_writer_write(encoder, expected + at, n,
                                              &f->error));
    at += n;
  }
  check(f, lc_pouch_collection_writer_finish(encoder, &body, &f->error));
  check(f, lc_collection_chunks_finish(call.collection, chunks, &f->error));
  assert_true(chunks->height >= 3);
  body.stream_segment = chunks->root.segment;
  body.stream_offset = chunks->root.offset;
  body.stream_root_bytes = chunks->root.bytes;
  body.stream_extents = chunks->extents;
  body.stream_height = chunks->height;
  body.stored_bytes = chunks->bytes;
  check(f,
        lc_collection_publish_stream(&call, &guard, encoder,
                                     call.collection->head.lease.fencing_token,
                                     &body, chunks, &result, &f->error));
  assert_int_equal(result.position, 1);
  /* Visible unit/index bytes are credited; bounded unused range tails and
   * their allocation headers remain reclaimable. */
  assert_true(call.collection->order.reclaimable_bytes > 0);
  assert_true(call.collection->order.reclaimable_bytes < 2U * 131072U);
  lc_collection_call_end(&call);
  lc_collection_chunks_close(chunks);
  free(chunks);
}
static int conversion_crash(void *context, int phase, lc_error *error) {
  int *seen = context;
  (void)error;
  if (phase == LC_POUCH_RECODE_SEGMENT_INSTALLED && ++*seen == 3)
    _exit(62);
  return LC_OK;
}
static void
test_deep_index_integrity_compaction_and_conversion_restart(void **state) {
  fixture *f = *state;
  char *expected = malloc(4096 + 9);
  assert_non_null(expected);
  memcpy(expected, "{\"v\":\"", 6);
  memset(expected + 6, 'x', 4096);
  memcpy(expected + 4102, "\"}", 3);
  close_owners(f);
  for (int scenario = 0; scenario < 2; ++scenario) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/deep-%d", f->root,
             scenario);
    open_owners(f, 0, 1);
    build_deep_index(f, expected, 4104);
    read_one(f, expected, 4104);
    lc_collection_handle *c = f->collection->impl;
    lc_pouch_collection_record body;
    lc_u64 segment;
    int found;
    check(f, lc_collection_lookup_id(c, "deep", &body, &segment, &found,
                                     &f->error));
    assert_true(found && body.stream_height >= 3);
    char *path = NULL, leaf[64];
    check(f, lc_collection_record_path(c, body.stream_segment, &path, leaf,
                                       &f->error));
    int fd = open(path, O_RDWR | O_CLOEXEC);
    lc_free_with_allocator(&c->allocator, path);
    assert_true(fd >= 0);
    unsigned char original, changed;
    assert_int_equal(pread(fd, &original, 1, (off_t)(body.stream_offset + 40)),
                     1);
    changed = original ^ 1;
    assert_int_equal(pwrite(fd, &changed, 1, (off_t)(body.stream_offset + 40)),
                     1);
    lc_sink *sink = NULL;
    lc_collection_read_res read;
    lc_collection_read_res_init(&read);
    check(f, lc_sink_to_memory(&sink, &f->error));
    assert_int_equal(
        lc_collection_read(f->collection, NULL, sink, &read, &f->error),
        LC_ERR_PROTOCOL);
    lc_sink_close(sink);
    lc_collection_read_res_cleanup(&read);
    assert_int_equal(pwrite(fd, &original, 1, (off_t)(body.stream_offset + 40)),
                     1);
    assert_int_equal(close(fd), 0);
    read_one(f, expected, 4104);
    if (!scenario) {
      c->pouch->segment_target_bytes = 1024U * 1024U;
      lc_pouch_maintenance_options options = {0};
      lc_pouch_maintenance_result maintained = {0};
      options.ns = "journal";
      options.force = 1;
      check(f, lc_pouch_maintenance_run(c->pouch, &options, &maintained,
                                        &f->error));
      assert_int_equal(maintained.compacted_collection_count, 1);
      lc_pouch_maintenance_result_cleanup(NULL, &maintained);
      read_one(f, expected, 4104);
      close_owners(f);
      open_owners(f, 0, 0);
      read_one(f, expected, 4104);
    } else {
      close_owners(f);
      pid_t child = fork();
      assert_true(child >= 0);
      if (!child) {
        int seen = 0;
        lc_pouch_recode_test_hook = conversion_crash;
        lc_pouch_recode_test_context = &seen;
        open_owners(f, 2, 0);
        _exit(63);
      }
      int exited;
      assert_int_equal(waitpid(child, &exited, 0), child);
      assert_true(WIFEXITED(exited) && WEXITSTATUS(exited) == 62);
      open_owners(f, 2, 0);
      read_one(f, expected, 4104);
      close_owners(f);
      open_owners(f, 0, 0);
      read_one(f, expected, 4104);
    }
    close_owners(f);
  }
  free(expected);
}
static int producer_crash(void *context, lc_collection *collection, int phase,
                          lc_error *error) {
  (void)context;
  (void)error;
  if (phase == LC_COLLECTION_APPEND_CHUNK_PENDING) {
    lc_collection_handle *c = collection->impl;
    off_t end = lseek(c->active_fd, 0, SEEK_END);
    if (end < 3 || ftruncate(c->active_fd, end - 3))
      _exit(67);
    _exit(66);
  }
  return LC_OK;
}
static void pipe_event(int fd) {
  struct pollfd event = {fd, POLLIN, 0};
  char byte;
  assert_int_equal(poll(&event, 1, 5000), 1);
  assert_int_equal(read(fd, &byte, 1), 1);
}
typedef struct private_io_pause {
  int ready, proceed;
} private_io_pause;

static int pause_private_io(void *context, lc_collection *collection, int phase,
                            lc_error *error) {
  private_io_pause *pause = context;
  struct pollfd event = {pause->proceed, POLLIN, 0};
  char byte;
  (void)collection;
  (void)error;
  if (phase != LC_COLLECTION_APPEND_CHUNK_PENDING)
    return LC_OK;
  lc_collection_append_test_hook = NULL;
  if (write(pause->ready, "r", 1) != 1 || poll(&event, 1, 5000) != 1 ||
      read(pause->proceed, &byte, 1) != 1)
    _exit(71);
  return LC_OK;
}

static void test_private_io_does_not_hold_append_authority(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  lc_collection_delete_res deleted;
  for (int mode = 0; mode <= 4; mode += 4) {
    int ready[2], proceed[2], exited;
    char slow_id[32], peer_id[32];
    close_owners(f);
    snprintf(slow_id, sizeof(slow_id), "paused-io-%d", mode);
    snprintf(peer_id, sizeof(peer_id), "peer-io-%d", mode);
    assert_int_equal(pipe(ready), 0);
    assert_int_equal(pipe(proceed), 0);
    pid_t child = fork();
    assert_true(child >= 0);
    if (!child) {
      private_io_pause pause = {ready[1], proceed[0]};
      close(ready[0]);
      close(proceed[1]);
      open_owners(f, mode, 0);
      lc_collection_append_test_context = &pause;
      lc_collection_append_test_hook = pause_private_io;
      check(f, push(f->collection, slow_id, f->body, BODY_BYTES, &result,
                    &f->error));
      close_owners(f);
      close(ready[1]);
      close(proceed[0]);
      _exit(0);
    }
    close(ready[1]);
    close(proceed[0]);
    pipe_event(ready[0]);
    open_owners(f, mode, 0);
    check(f, push(f->collection, peer_id, "{}", 2, &result, &f->error));
    assert_int_equal(result.position, (unsigned int)mode / 2U + 1U);
    status_is(f, slow_id, LC_COLLECTION_APPEND_ABSENT);
    struct pollfd still_paused = {ready[0], POLLIN, 0};
    assert_int_equal(poll(&still_paused, 1, 0), 0);
    assert_int_equal(write(proceed[1], "g", 1), 1);
    close(ready[0]);
    close(proceed[1]);
    assert_int_equal(waitpid(child, &exited, 0), child);
    assert_true(WIFEXITED(exited) && WEXITSTATUS(exited) == 0);
    check(f, lc_collection_delete_entry(f->collection, result.position,
                                        &deleted, &f->error));
    read_one(f, f->body, BODY_BYTES);
    check(f, lc_collection_delete_entry(f->collection, result.position + 1U,
                                        &deleted, &f->error));
    status_is(f, slow_id, LC_COLLECTION_APPEND_DELETED);
  }
}
static struct {
  dev_t device;
  ino_t inode;
  int calls, fail;
} old_range_sync;

static int observe_old_range_sync(int fd) {
  struct stat st;
  if (fstat(fd, &st) == 0 && st.st_dev == old_range_sync.device &&
      st.st_ino == old_range_sync.inode) {
    ++old_range_sync.calls;
    if (old_range_sync.fail) {
      errno = EIO;
      return -1;
    }
  }
  return fsync(fd);
}

static void
test_nosync_seals_late_private_writes_before_publication(void **state) {
  fixture *f = *state;
  lc_collection_handle *c = f->collection->impl;
  lc_collection_append_req req = {"late-range"};
  lc_collection_append_res result, peer_result;
  lc_collection_read_opts options = {0};
  lc_collection_read_res read_result;
  lc_sink *sink = NULL;
  const void *bytes;
  size_t count;
  struct stat st;
  uint64_t original = c->head.active;
  int peers = 0, rc;
  char id[48];
  assert_false(c->pouch->durable_sync);
  c->pouch->segment_target_bytes = 4U * BODY_BYTES;
  check(f, lc_collection_append_begin(f->collection, &req, &f->pending,
                                      &f->error));
  check(f, lc_collection_writer_write(f->pending, f->body, BODY_BYTES / 2U,
                                      &f->error));
  assert_int_equal(fstat(c->active_fd, &st), 0);
  memset(&old_range_sync, 0, sizeof(old_range_sync));
  old_range_sync.device = st.st_dev;
  old_range_sync.inode = st.st_ino;
  while (c->head.active == original && peers < 8) {
    snprintf(id, sizeof(id), "peer-before-seal-%d", peers);
    check(f, push(f->peer, id, f->body, BODY_BYTES, &peer_result, &f->error));
    ++peers;
    check(f, lc_collection_head_read(c, &f->error));
  }
  assert_true(c->head.active != original);
  old_range_sync.fail = 1;
  lc_collection_chunks_test_sync_fd = observe_old_range_sync;
  memset(&result, 0, sizeof(result));
  rc = lc_collection_writer_write(f->pending, f->body + BODY_BYTES / 2U,
                                  BODY_BYTES / 2U, &f->error);
  if (rc == LC_OK)
    rc = lc_collection_writer_commit(f->pending, &result, &f->error);
  lc_collection_chunks_test_sync_fd = NULL;
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(old_range_sync.calls, 1);
  assert_int_equal(result.position, 0);
  lc_collection_writer_close(f->pending);
  f->pending = NULL;
  status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
  check(f, push(f->collection, req.append_id, f->body, BODY_BYTES, &result,
                &f->error));
  assert_int_equal(result.position, (uint64_t)peers + 1U);
  options.has_after = 1;
  options.after = peers;
  lc_collection_read_res_init(&read_result);
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(f->collection, &options, sink, &read_result,
                              &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &count, &f->error));
  assert_int_equal(read_result.completed_records, 1);
  assert_int_equal(count, BODY_BYTES + 1U);
  assert_memory_equal(bytes, f->body, BODY_BYTES);
  lc_sink_close(sink);
  lc_collection_read_res_cleanup(&read_result);
  close_owners(f);
  open_owners(f, 0, 0);
  status_is(f, req.append_id, LC_COLLECTION_APPEND_COMMITTED);
}

static int seal_ready_fd, seal_proceed_fd;

static int pause_first_inode_seal(int fd) {
  struct stat st;
  char byte;
  struct pollfd proceed = {seal_proceed_fd, POLLIN, 0};
  if (fstat(fd, &st) == 0 && st.st_dev == old_range_sync.device &&
      st.st_ino == old_range_sync.inode) {
    ++old_range_sync.calls;
    if (write(seal_ready_fd, "s", 1) != 1 || poll(&proceed, 1, 5000) != 1 ||
        read(seal_proceed_fd, &byte, 1) != 1)
      _exit(72);
    if (old_range_sync.fail) {
      errno = EIO;
      return -1;
    }
  }
  return fsync(fd);
}

static void test_private_seal_allows_peer_publication_and_retry(void **state) {
  fixture *f = *state;
  lc_collection_append_res peer, result;
  lc_collection_delete_res deleted;
  f->segment_target = BODY_BYTES;
  for (int scenario = 0; scenario < 4; ++scenario) {
    int failure = scenario & 1;
    int mode = scenario & 2 ? 4 : 0;
    int ready[2], proceed[2], status, peers = 0;
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/seal-progress-%d",
             f->root, scenario);
    assert_int_equal(pipe(ready), 0);
    assert_int_equal(pipe(proceed), 0);
    pid_t child = fork();
    assert_true(child >= 0);
    if (!child) {
      lc_collection_append_req req = {"paused-seal"};
      struct stat st;
      char byte;
      struct pollfd go = {proceed[0], POLLIN, 0};
      close(ready[0]);
      close(proceed[1]);
      open_owners(f, mode, 1);
      lc_collection_handle *c = f->collection->impl;
      int rc = lc_collection_append_begin(f->collection, &req, &f->pending,
                                          &f->error);
      if (rc == LC_OK)
        rc = lc_collection_writer_write(f->pending, f->body, BODY_BYTES,
                                        &f->error);
      if (rc != LC_OK || fstat(c->active_fd, &st) != 0)
        _exit(73);
      memset(&old_range_sync, 0, sizeof(old_range_sync));
      old_range_sync.device = st.st_dev;
      old_range_sync.inode = st.st_ino;
      old_range_sync.fail = failure;
      if (write(ready[1], "r", 1) != 1 || poll(&go, 1, 5000) != 1 ||
          read(proceed[0], &byte, 1) != 1)
        _exit(74);
      seal_ready_fd = ready[1];
      seal_proceed_fd = proceed[0];
      lc_collection_chunks_test_sync_fd = pause_first_inode_seal;
      rc = lc_collection_writer_commit(f->pending, &result, &f->error);
      lc_collection_chunks_test_sync_fd = NULL;
      lc_collection_writer_close(f->pending);
      f->pending = NULL;
      close_owners(f);
      close(ready[1]);
      close(proceed[0]);
      _exit(rc == (failure ? LC_ERR_TRANSPORT : LC_OK) &&
                    old_range_sync.calls == 1
                ? 0
                : 75);
    }
    close(ready[1]);
    close(proceed[0]);
    pipe_event(ready[0]);
    open_owners(f, mode, 0);
    lc_collection_handle *c = f->collection->impl;
    lc_u64 original = c->head.active;
    while (c->head.active == original && peers < 8) {
      char id[32];
      snprintf(id, sizeof(id), "before-seal-%d", peers++);
      check(f, push(f->peer, id, f->body, BODY_BYTES, &peer, &f->error));
      check(f, lc_collection_head_read(c, &f->error));
    }
    assert_true(c->head.active != original);
    assert_int_equal(write(proceed[1], "g", 1), 1);
    pipe_event(ready[0]);
    /* The producer is paused in its prerequisite data sync. This commit must
     * finish while it remains paused, before a publication position is taken.
     */
    check(f, push(f->peer, "during-seal", "{}", 2, &peer, &f->error));
    struct pollfd alive = {ready[0], POLLIN, 0};
    assert_int_equal(poll(&alive, 1, 0), 0);
    assert_int_equal(write(proceed[1], "g", 1), 1);
    close(ready[0]);
    close(proceed[1]);
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    status_is(f, "paused-seal",
              failure ? LC_COLLECTION_APPEND_ABSENT
                      : LC_COLLECTION_APPEND_COMMITTED);
    if (failure)
      check(f, push(f->collection, "paused-seal", f->body, BODY_BYTES, &result,
                    &f->error));
    for (lc_u64 position = 1; position <= peer.position; ++position)
      check(f, lc_collection_delete_entry(f->collection, position, &deleted,
                                          &f->error));
    read_one(f, f->body, BODY_BYTES);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
  }
}

static void test_exact_final_frames_keep_contiguous_records(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  lc_collection_delete_res deleted;
  lc_pouch_collection_record record;
  lc_u64 segment;
  int found;
  const int modes[] = {0, 1, 4, 5};
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  f->stream_chunk = malloc(65538);
  assert_non_null(f->stream_chunk);
  for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); ++i) {
    int mode = modes[i];
    size_t boundary = mode & 1 ? 4096 : 65536;
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/exact-frame-%d",
             f->root, mode);
    open_owners(f, mode, 1);
    lc_collection_handle *c = f->collection->impl;
    for (size_t bytes = boundary - 1; bytes <= boundary + 1; ++bytes) {
      char id[32];
      snprintf(id, sizeof(id), "frame-%lu", (unsigned long)bytes);
      memcpy(f->stream_chunk, "{\"v\":\"", 6);
      memset(f->stream_chunk + 6, 'x', bytes - 8);
      memcpy(f->stream_chunk + bytes - 2, "\"}", 2);
      check(f, push(f->collection, id, (char *)f->stream_chunk, bytes, &result,
                    &f->error));
      check(f, lc_collection_lookup_id(c, id, &record, &segment, &found,
                                       &f->error));
      assert_true(found);
      if (bytes <= boundary)
        assert_int_equal(record.stream_extents, 0);
      else
        assert_true(record.stream_extents > 0);
      read_one(f, (char *)f->stream_chunk, bytes);
      check(f, lc_collection_delete_entry(f->collection, result.position,
                                          &deleted, &f->error));
    }
  }
}

static void test_rotation_seal_covers_finished_private_units(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  struct stat st;
  for (int mode = 0; mode <= 4; mode += 4) {
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/covered-seal-%d",
             f->root, mode);
    f->segment_target = 65536;
    open_owners(f, mode, 1);
    lc_collection_handle *c = f->collection->impl;
    lc_u64 original = c->head.active;
    assert_int_equal(fstat(c->active_fd, &st), 0);
    memset(&old_range_sync, 0, sizeof(old_range_sync));
    old_range_sync.device = st.st_dev;
    old_range_sync.inode = st.st_ino;
    old_range_sync.fail = 1;
    lc_collection_chunks_test_sync_fd = observe_old_range_sync;
    check(f, push(f->collection, "covered", f->body, BODY_BYTES, &result,
                  &f->error));
    lc_collection_chunks_test_sync_fd = NULL;
    assert_int_equal(old_range_sync.calls, 0);
    assert_true(c->head.active > original);
    read_one(f, f->body, BODY_BYTES);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
  }
}

static void test_target_sized_values_do_not_split_for_framing(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  lc_pouch_collection_record record;
  lc_u64 segment;
  int found;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  for (int mode = 0; mode < 8; ++mode) {
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/framing-%d", f->root,
             mode);
    f->segment_target = BODY_BYTES;
    open_owners(f, mode, 1);
    lc_collection_handle *c = f->collection->impl;
    lc_u64 first = c->head.active;
    for (int entry = 0; entry < 3; ++entry) {
      char id[32];
      snprintf(id, sizeof(id), "target-%d", entry);
      check(f,
            push(f->collection, id, f->body, BODY_BYTES, &result, &f->error));
      assert_int_equal(result.position, (unsigned int)entry + 1U);
      assert_int_equal(result.bytes, BODY_BYTES);
      check(f, lc_collection_lookup_id(c, id, &record, &segment, &found,
                                       &f->error));
      assert_true(found);
      /* Subsequent compressed entries can use a shorter reusable range. The
       * initial fresh reservation must hold the target-sized value whole. */
      if (!entry || !(mode & 2)) {
        if (mode & 2) {
          /* A compressible final chunk can use the ordinary inline record.
           * It must still fit one bounded codec delivery, never split a
           * target-sized value solely to accommodate framing. */
          assert_true(record.stream_extents <= 1);
          if (!record.stream_extents)
            assert_true(record.stored_bytes <= 65536U + 48U);
        } else
          assert_int_equal(record.stream_extents, 1);
        assert_int_equal(record.stream_height, 0);
      }
      /* Without compression the first two reservations share an inode. The
       * third, rather than the second publication, owns its next boundary. */
      if (!(mode & 2))
        assert_int_equal(c->head.active, first + (entry == 2));
    }
    close_owners(f);
    open_owners(f, mode, 0);
    for (int entry = 0; entry < 3; ++entry) {
      char id[32];
      snprintf(id, sizeof(id), "target-%d", entry);
      status_is(f, id, LC_COLLECTION_APPEND_COMMITTED);
    }
  }
}

static void test_first_segment_seals_final_metadata_once(void **state) {
  fixture *f = *state;
  for (int mode = 0; mode <= 4; mode += 4) {
    lc_collection_append_req req = {"late-first"};
    lc_collection_append_res result, peer;
    lc_collection_delete_res deleted;
    struct stat st;
    char id[32];
    int peers = 0;
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/first-seal-%d",
             f->root, mode);
    f->segment_target = 1024U * 1024U;
    open_owners(f, mode, 1);
    lc_collection_handle *c = f->collection->impl;
    uint64_t original = c->head.active;
    check(f, lc_collection_append_begin(f->collection, &req, &f->pending,
                                        &f->error));
    check(f, lc_collection_writer_write(f->pending, f->body, BODY_BYTES / 2U,
                                        &f->error));
    assert_int_equal(fstat(c->active_fd, &st), 0);
    memset(&old_range_sync, 0, sizeof(old_range_sync));
    old_range_sync.device = st.st_dev;
    old_range_sync.inode = st.st_ino;
    while (c->head.active == original && peers < 8) {
      snprintf(id, sizeof(id), "rotate-%d", peers);
      check(f, push(f->peer, id, f->body, BODY_BYTES, &peer, &f->error));
      ++peers;
      check(f, lc_collection_head_read(c, &f->error));
    }
    assert_true(c->head.active != original);
    lc_collection_chunks_test_sync_fd = observe_old_range_sync;
    check(f, lc_collection_writer_write(f->pending, f->body + BODY_BYTES / 2U,
                                        BODY_BYTES / 2U, &f->error));
    check(f, lc_collection_writer_commit(f->pending, &result, &f->error));
    lc_collection_chunks_test_sync_fd = NULL;
    assert_int_equal(old_range_sync.calls, 1);
    lc_collection_writer_close(f->pending);
    f->pending = NULL;
    assert_int_equal(result.position, (unsigned int)peers + 1U);
    for (int position = 1; position <= peers; ++position)
      check(f, lc_collection_delete_entry(f->collection, position, &deleted,
                                          &f->error));
    read_one(f, f->body, BODY_BYTES);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
  }
}

static off_t rotation_durable_bytes;
static int rotation_sync_failure;
static int persist_rotation_boundary(int fd) {
  struct stat st;
  if (rotation_sync_failure) {
    errno = EIO;
    return -1;
  }
  assert_int_equal(fstat(fd, &st), 0);
  int rc = fsync(fd);
  if (!rc)
    rotation_durable_bytes = st.st_size;
  return rc;
}

static void
test_abandoned_reservation_rotation_survives_power_loss(void **state) {
  fixture *f = *state;
  lc_collection_append_req abandoned = {"abandoned"}, survivor = {"survivor"};
  lc_collection_append_res result;
  unsigned long random = 17;
  static const char alphabet[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
  for (size_t at = 6; at < BODY_BYTES - 2U; ++at) {
    random = random * 1664525UL + 1013904223UL;
    f->body[at] = alphabet[(random >> 19) & 63U];
  }
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  f->segment_target = BODY_BYTES;
  for (int mode = 0; mode < 8; ++mode) {
    char *old_path = NULL, leaf[64];
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/rotation-loss-%d",
             f->root, mode);
    open_owners(f, mode, 1);
    lc_collection_handle *c = f->collection->impl;
    lc_u64 segment = c->head.active;
    check(f, lc_collection_append_begin(f->collection, &abandoned, &f->pending,
                                        &f->error));
    for (size_t at = 0; at < BODY_BYTES - 4096; at += 4096)
      check(f, lc_collection_writer_write(f->pending, f->body + at, 4096,
                                          &f->error));
    check(f, lc_collection_writer_abort(f->pending, &f->error));
    lc_collection_writer_close(f->pending);
    f->pending = NULL;
    assert_int_equal(c->head.active, segment);
    check(f, lc_collection_record_path(c, segment, &old_path, leaf, &f->error));
    char *order = strdup(c->order_path), *ids = strdup(c->ids_path);
    assert_non_null(order);
    assert_non_null(ids);
    /* Simulate a persisted REGION header whose unsynced extent is lost.
     * Only a successful boundary sync preserves the complete old inode. */
    rotation_durable_bytes = 32;
    rotation_sync_failure = 1;
    lc_collection_test_rotate_sync_fd = persist_rotation_boundary;
    input in;
    input_init(&in, f, "{}", 2);
    assert_int_equal(
        lc_collection_append(f->peer, &survivor, &in.pub, &result, &f->error),
        LC_ERR_TRANSPORT);
    lc_error_cleanup(&f->error);
    lc_error_init(&f->error);
    check(f, lc_collection_head_read(c, &f->error));
    assert_int_equal(c->head.active, segment);
    status_is(f, "survivor", LC_COLLECTION_APPEND_ABSENT);
    rotation_sync_failure = 0;
    input_init(&in, f, "{}", 2);
    check(f, lc_collection_append(f->peer, &survivor, &in.pub, &result,
                                  &f->error));
    lc_collection_test_rotate_sync_fd = NULL;
    assert_int_equal(result.position, 1);
    close_owners(f);
    int fd = open(old_path, O_RDWR | O_CLOEXEC);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, rotation_durable_bytes), 0);
    assert_int_equal(fsync(fd), 0);
    assert_int_equal(close(fd), 0);
    lc_free_with_allocator(&f->allocator, old_path);
    assert_int_equal(unlink(order), 0);
    assert_int_equal(unlink(ids), 0);
    free(order);
    free(ids);
    open_owners(f, mode, 0);
    status_is(f, "abandoned", LC_COLLECTION_APPEND_ABSENT);
    status_is(f, "survivor", LC_COLLECTION_APPEND_COMMITTED);
    read_one(f, "{}", 2);
  }
}

static void test_existing_writer_repairs_peer_pending_tail(void **state) {
  fixture *f = *state;
  int ready[2], proceed[2], exited;
  lc_collection_writer *writer = NULL;
  lc_collection_append_req req = {"survivor"};
  lc_collection_append_res result;
  close_owners(f);
  assert_int_equal(pipe(ready), 0);
  assert_int_equal(pipe(proceed), 0);
  pid_t child = fork();
  assert_true(child >= 0);
  if (!child) {
    close(ready[0]);
    close(proceed[1]);
    req.append_id = "crashed";
    open_owners(f, 0, 0);
    check(f,
          lc_collection_append_begin(f->collection, &req, &writer, &f->error));
    check(f, lc_collection_writer_write(writer, f->body, 16384, &f->error));
    assert_int_equal(write(ready[1], "r", 1), 1);
    pipe_event(proceed[0]);
    lc_collection_append_test_hook = producer_crash;
    (void)lc_collection_writer_write(writer, f->body + 16384,
                                     BODY_BYTES - 16384, &f->error);
    _exit(68);
  }
  close(ready[1]);
  close(proceed[0]);
  pipe_event(ready[0]);
  close(ready[0]);
  open_owners(f, 0, 0);
  check(f, lc_collection_append_begin(f->collection, &req, &writer, &f->error));
  check(f, lc_collection_writer_write(writer, f->body, 16384, &f->error));
  assert_int_equal(write(proceed[1], "g", 1), 1);
  close(proceed[1]);
  assert_int_equal(waitpid(child, &exited, 0), child);
  assert_true(WIFEXITED(exited) && WEXITSTATUS(exited) == 66);
  for (size_t at = 16384; at < BODY_BYTES; at += 4096)
    check(f, lc_collection_writer_write(writer, f->body + at, 4096, &f->error));
  check(f, lc_collection_writer_commit(writer, &result, &f->error));
  lc_collection_writer_close(writer);
  assert_int_equal(result.position, 1);
  status_is(f, "crashed", LC_COLLECTION_APPEND_ABSENT);
  read_one(f, f->body, BODY_BYTES);
  close_owners(f);
  open_owners(f, 0, 0);
  read_one(f, f->body, BODY_BYTES);
}

static void
test_reserved_tail_recovery_preserves_published_history(void **state) {
  fixture *f = *state;
  const int modes[] = {0, 5, 2, 7};
  lc_collection_append_req req = {"seed", 1, 60, 1, 100};
  lc_collection_append_res result;
  lc_collection_delete_res deleted;
  lc_collection_info info;
  input in;
  unsigned long random = 17;
  static const char alphabet[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
  for (size_t at = 6; at < BODY_BYTES - 2U; ++at) {
    random = random * 1664525UL + 1013904223UL;
    f->body[at] = alphabet[(random >> 19) & 63U];
  }
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  lc_pouch_test_queue_clock_gettime = clock_now;
  lc_pouch_test_queue_time_context = f;
  f->now = 1000;
  f->segment_target = 65536;
  for (size_t scenario = 0; scenario < sizeof(modes) / sizeof(modes[0]);
       ++scenario) {
    int mode = modes[scenario], ready[2], proceed[2], exited;
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/replay-%d", f->root,
             mode);
    open_owners(f, mode, 1);
    input_init(&in, f, f->body, BODY_BYTES);
    check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &result,
                                         &f->error));
    assert_int_equal(result.position, 1);
    assert_int_equal(result.retention_expires_at_unix, 1060);
    assert_int_equal(result.collection_retention_expires_at_unix, 1100);
    if (scenario == 1 || scenario == 2) {
      lc_pouch_maintenance_options options = {0};
      lc_pouch_maintenance_result maintained = {0};
      options.ns = "journal";
      options.force = 1;
      check(f, lc_pouch_maintenance_run(((lc_client_handle *)f->client)->pouch,
                                        &options, &maintained, &f->error));
      assert_int_equal(maintained.compacted_collection_count, 1);
      lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    }
    close_owners(f);
    assert_int_equal(pipe(ready), 0);
    assert_int_equal(pipe(proceed), 0);
    pid_t child = fork();
    assert_true(child >= 0);
    if (!child) {
      lc_collection_append_req failed = {"crashed"};
      lc_collection_writer *writer = NULL;
      close(ready[0]);
      close(proceed[1]);
      open_owners(f, mode, 0);
      check(f, lc_collection_append_begin(f->collection, &failed, &writer,
                                          &f->error));
      check(f, lc_collection_writer_write(writer, f->body, 16384, &f->error));
      assert_int_equal(write(ready[1], "r", 1), 1);
      pipe_event(proceed[0]);
      lc_collection_append_test_hook = producer_crash;
      (void)lc_collection_writer_write(writer, f->body + 16384,
                                       BODY_BYTES - 16384, &f->error);
      _exit(68);
    }
    close(ready[1]);
    close(proceed[0]);
    pipe_event(ready[0]);
    close(ready[0]);
    open_owners(f, mode, 0);
    assert_int_equal(write(proceed[1], "g", 1), 1);
    close(proceed[1]);
    assert_int_equal(waitpid(child, &exited, 0), child);
    assert_true(WIFEXITED(exited) && WEXITSTATUS(exited) == 66);
    check(f, push(f->collection, "survivor", "{}", 2, &result, &f->error));
    assert_int_equal(result.position, 2);
    status_is(f, "crashed", LC_COLLECTION_APPEND_ABSENT);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_int_equal(info.live_count, 2);
    assert_int_equal(info.retention_expires_at_unix, 1100);
    check(f, lc_collection_delete_entry(f->collection, 2, &deleted, &f->error));
    read_one(f, f->body, BODY_BYTES);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
  }
}

static void test_failed_tail_recovery_preserves_corrupt_source(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  lc_collection_info info;
  struct stat source, order, ids, after;
  close_owners(f);
  open_owners(f, 4, 0);
  lc_collection_handle *c = f->collection->impl;
  check(f, push(f->collection, "published", "{}", 2, &result, &f->error));
  assert_int_equal(fstat(c->active_fd, &source), 0);
  assert_true(source.st_size > 1 && source.st_size < 4096);
  f->stream_chunk = malloc((size_t)source.st_size);
  assert_non_null(f->stream_chunk);
  assert_int_equal(
      pread(c->active_fd, f->stream_chunk, (size_t)source.st_size, 0),
      source.st_size);
  for (int whole_record = 0; whole_record < 2; ++whole_record) {
    off_t damaged_size = whole_record ? 0 : source.st_size - 1;
    assert_int_equal(stat(c->order_path, &order), 0);
    assert_int_equal(stat(c->ids_path, &ids), 0);
    assert_int_equal(ftruncate(c->active_fd, damaged_size), 0);
    for (int attempt = 0; attempt < 2; ++attempt) {
      assert_int_equal(lc_collection_describe(f->collection, &info, &f->error),
                       LC_ERR_PROTOCOL);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      assert_int_equal(fstat(c->active_fd, &after), 0);
      assert_true(after.st_ino == source.st_ino &&
                  after.st_size == damaged_size);
      assert_int_equal(stat(c->order_path, &after), 0);
      assert_true(after.st_ino == order.st_ino &&
                  after.st_size == order.st_size);
      assert_int_equal(stat(c->ids_path, &after), 0);
      assert_true(after.st_ino == ids.st_ino && after.st_size == ids.st_size);
    }
    assert_int_equal(
        pwrite(c->active_fd, f->stream_chunk, (size_t)source.st_size, 0),
        source.st_size);
    read_one(f, "{}", 2);
    status_is(f, "published", LC_COLLECTION_APPEND_COMMITTED);
  }
}

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_exact_final_frames_keep_contiguous_records, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_rotation_seal_covers_finished_private_units, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_abandoned_reservation_rotation_survives_power_loss, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_private_seal_allows_peer_publication_and_retry, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_direct_reader_bounded_chunks_failure_and_resume, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_direct_units_filter_metadata_corruption_and_rebuild, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_target_sized_values_do_not_split_for_framing, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_first_segment_seals_final_metadata_once, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_tail_recovery_preserves_corrupt_source, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_reserved_tail_recovery_preserves_published_history, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_private_io_does_not_hold_append_authority, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_stream_units_survive_compression_conversion, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_stream_units_survive_rolling_snapshots_and_rebuild, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_nosync_seals_late_private_writes_before_publication, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_extent_headers_do_not_poison_collection, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_chunk_checkpoint_preserves_missing_binding, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_existing_writer_repairs_peer_pending_tail, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_deep_index_integrity_compaction_and_conversion_restart, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_push_is_canonical_before_commit_without_payload_allocations,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_source_errors_are_unpublished_and_borrowed, setup, teardown),
      cmocka_unit_test_setup_teardown(test_codecs_retry_retention_and_restart,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_callbacks_reentry_and_owner_close,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_source_ownership_change_is_fenced,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_mixed_slow_producers_allow_peer_commit_and_compaction, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_uncertain_sync_retains_cross_api_retry_identity, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_allocation_failure_closes_internal_writer, setup, teardown)};
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(tests[0]), argc, argv,
                           NULL, NULL);
}
