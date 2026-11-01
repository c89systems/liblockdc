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
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_collection_internal.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-sync-"
#define WRITERS 4

typedef struct fixture {
  char root[512], endpoint[640];
  lc_client *clients[WRITERS];
  lc_collection *handles[WRITERS];
  lc_error error;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  unsigned int staged;
  unsigned int sync_calls;
  unsigned int enqueued;
  unsigned int expected_requests;
  uint64_t batch_limit;
  int fail_sync;
  int release;
  const char *body;
} fixture;

static fixture *sync_fixture;
typedef enum append_kind {
  DIRECT_APPEND,
  PUSH_APPEND,
  STAGED_APPEND
} append_kind;

typedef struct writer {
  lc_collection *collection;
  const char *id;
  const char *body;
  lc_collection_append_res result;
  int rc;
  append_kind kind;
} writer;

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("collection sync failure %d: %s\n", rc,
                f->error.message == NULL ? "" : f->error.message);
  assert_int_equal(rc, LC_OK);
}

static void close_all(fixture *f) {
  for (size_t i = 0; i < WRITERS; ++i) {
    lc_collection_close(f->handles[i]);
    f->handles[i] = NULL;
  }
  for (size_t i = 0; i < WRITERS; ++i) {
    lc_client_close(f->clients[i]);
    f->clients[i] = NULL;
  }
}

static void open_client(fixture *f, size_t index, int exclusive, long segment) {
  lc_client_config config;
  lc_pouch_settings settings;
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_DURABLE_SYNC |
      LC_POUCH_SETTING_FSYNC_BATCH_MAX_OPS |
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_QUERY_INDEXING |
      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = exclusive;
  settings.durable_sync = 1;
  settings.fsync_batch_max_ops = f->batch_limit;
  settings.segment_target_bytes = (uint64_t)segment;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "sync";
  config.pouch_settings = &settings;
  check(f, lc_client_open(&config, &f->clients[index], &f->error));
}

static void open_handles(fixture *f, int exclusive, long segment) {
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.name = "events";
  open_client(f, 0, exclusive, segment);
  check(f, lc_client_create_collection(f->clients[0], &config, &f->handles[0],
                                       &f->error));
  for (size_t i = 1; i < WRITERS; ++i) {
    if (!exclusive)
      open_client(f, i, 0, segment);
    check(f,
          lc_client_open_collection(exclusive ? f->clients[0] : f->clients[i],
                                    &config, &f->handles[i], &f->error));
  }
}

static int setup(void **state) {
  char template_path[] = PREFIX "XXXXXX";
  fixture *f = calloc(1, sizeof(*f));
  assert_non_null(f);
  f->batch_limit = WRITERS;
  f->expected_requests = WRITERS;
  lc_error_init(&f->error);
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  assert_int_equal(pthread_mutex_init(&f->mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&f->condition, NULL), 0);
  *state = f;
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_commit_wait_test_hook = NULL;
  lc_collection_commit_wait_test_context = NULL;
  lc_pouch_test_fsync_batch_delay_ns = 0;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_fsync_enqueued = NULL;
  sync_fixture = NULL;
  close_all(f);
  lc_error_cleanup(&f->error);
  pthread_cond_destroy(&f->condition);
  pthread_mutex_destroy(&f->mutex);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

/* All callers must reach the wait boundary before any can queue a sync. The
 * old append/root lock scope cannot satisfy this rendezvous. No timing-based
 * throughput assertion or scheduling sleep is needed. */
static int staged(void *context, lc_collection *collection, lc_error *error) {
  fixture *f = context;
  struct timespec deadline;
  int rc = 0;
  (void)collection;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&f->mutex);
  ++f->staged;
  pthread_cond_broadcast(&f->condition);
  while (f->staged < WRITERS && rc == 0)
    rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  pthread_mutex_unlock(&f->mutex);
  return rc == 0 ? LC_OK
                 : lc_error_set(error, LC_ERR_TIMEOUT, 0,
                                "append authority was retained while waiting "
                                "for commit peers",
                                NULL, NULL, NULL);
}

static void *append_thread(void *context) {
  writer *w = context;
  lc_source *source = NULL;
  lc_error error;
  lc_collection_append_req request;
  lc_error_init(&error);
  lc_collection_append_req_init(&request);
  request.append_id = w->id;
  if (w->kind == PUSH_APPEND) {
    lc_collection_writer *push = NULL;
    w->rc = lc_collection_append_begin(w->collection, &request, &push, &error);
    if (w->rc == LC_OK)
      w->rc =
          lc_collection_writer_write(push, w->body, strlen(w->body), &error);
    if (w->rc == LC_OK)
      w->rc = lc_collection_writer_commit(push, &w->result, &error);
    lc_collection_writer_close(push);
  } else {
    w->rc = lc_source_from_memory(w->body, strlen(w->body), &source, &error);
    if (w->rc == LC_OK)
      w->rc = w->kind == STAGED_APPEND
                  ? lc_collection_append_staged(w->collection, &request, source,
                                                &w->result, &error)
                  : lc_collection_append(w->collection, &request, source,
                                         &w->result, &error);
  }
  lc_source_close(source);
  lc_error_cleanup(&error);
  return NULL;
}

static void enqueued(void) {
  fixture *f = sync_fixture;
  pthread_mutex_lock(&f->mutex);
  ++f->enqueued;
  pthread_cond_broadcast(&f->condition);
  pthread_mutex_unlock(&f->mutex);
}

static int tracked_sync(int fd) {
  fixture *f = sync_fixture;
  struct stat st;
  struct timespec deadline;
  int rc = 0;
  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
    return -1;
  ++f->sync_calls; /* Only the root batcher's worker calls this hook. */
  /* Hold the first I/O until every caller is queued. Arrivals during I/O
   * must form a group without a timer or a scheduling-dependent sleep. */
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&f->mutex);
  while (f->enqueued < f->expected_requests && rc == 0)
    rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  pthread_mutex_unlock(&f->mutex);
  if (rc != 0) {
    errno = rc;
    return -1;
  }
  if (f->fail_sync) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}

static void parallel(fixture *f, int exclusive, int same_receiver,
                     int duplicate, int failure, long segment,
                     append_kind kind) {
  const char *ids[] = {"one", "two", "three", "four"};
  pthread_t threads[WRITERS];
  writer workers[WRITERS] = {{0}};
  lc_pouch_fsync_stats stats;
  unsigned int positions = 0, duplicates = 0;
  open_handles(f, exclusive, segment);
  lc_collection_commit_wait_test_hook = staged;
  lc_collection_commit_wait_test_context = f;
  f->fail_sync = failure;
  sync_fixture = f;
  lc_pouch_test_sync_fd = tracked_sync;
  lc_pouch_test_fsync_enqueued = enqueued;
  for (size_t i = 0; i < WRITERS; ++i) {
    workers[i].collection = f->handles[same_receiver ? 0 : i];
    workers[i].id = ids[duplicate ? 0 : i];
    workers[i].body = f->body != NULL ? f->body : "{\"event\":true}";
    workers[i].kind = kind;
    assert_int_equal(
        pthread_create(&threads[i], NULL, append_thread, &workers[i]), 0);
  }
  for (size_t i = 0; i < WRITERS; ++i)
    assert_int_equal(pthread_join(threads[i], NULL), 0);
  lc_collection_commit_wait_test_hook = NULL;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_fsync_enqueued = NULL;
  assert_int_equal(f->staged, WRITERS);
  for (size_t i = 0; i < WRITERS; ++i) {
    assert_int_equal(workers[i].rc, failure ? LC_ERR_TRANSPORT : LC_OK);
    assert_true(workers[i].result.position >= 1 &&
                workers[i].result.position <= WRITERS);
    positions |= 1U << (unsigned int)(workers[i].result.position - 1U);
    duplicates += workers[i].result.duplicate != 0;
  }
  assert_int_equal(positions, duplicate ? 1U : 15U);
  assert_int_equal(duplicates, duplicate ? WRITERS - 1 : 0);
  check(f, lc_pouch_fsync_stats_read(((lc_client_handle *)f->clients[0])->pouch,
                                     &stats, &f->error));
  assert_int_equal(stats.total_requests, WRITERS);
  assert_int_equal(f->enqueued, WRITERS);
  if (f->batch_limit == 1) {
    assert_int_equal(stats.max_batch_size, 1);
    assert_int_equal(stats.total_batches, WRITERS);
  } else {
    assert_true(stats.max_batch_size > 1 && stats.total_batches < WRITERS);
    if (f->batch_limit != 0)
      assert_true(stats.max_batch_size <= f->batch_limit);
  }
  assert_int_equal(f->sync_calls,
                   segment == 256 ? WRITERS : stats.total_batches);
  lc_collection_info info;
  check(f, lc_collection_describe(f->handles[0], &info, &f->error));
  assert_int_equal(info.live_count, duplicate ? 1 : WRITERS);
  for (size_t i = 0; i < WRITERS; ++i) {
    lc_collection_append_status_res status;
    check(f, lc_collection_append_status(f->handles[0], workers[i].id, &status,
                                         &f->error));
    assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
    assert_int_equal(status.entry.position, workers[i].result.position);
    if (failure) {
      uint64_t original = workers[i].result.position;
      append_thread(&workers[i]);
      assert_int_equal(workers[i].rc, LC_OK);
      assert_true(workers[i].result.duplicate);
      assert_int_equal(workers[i].result.position, original);
    }
  }
}

static void test_shared_handles(void **state) {
  parallel(*state, 0, 0, 0, 0, 65536, 0);
}
static void single_request(fixture *f, append_kind kind) {
  writer work = {0};
  lc_pouch_fsync_stats stats;
  open_handles(f, 0, 65536);
  f->expected_requests = 1;
  sync_fixture = f;
  lc_pouch_test_sync_fd = tracked_sync;
  lc_pouch_test_fsync_enqueued = enqueued;
  work.collection = f->handles[0];
  work.id = "single";
  work.body = "{\"event\":true}";
  work.kind = kind;
  append_thread(&work);
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_fsync_enqueued = NULL;
  assert_int_equal(work.rc, LC_OK);
  assert_int_equal(work.result.position, 1);
  assert_int_equal(f->sync_calls, 1);
  assert_int_equal(f->enqueued, 1);
  check(f, lc_pouch_fsync_stats_read(((lc_client_handle *)f->clients[0])->pouch,
                                     &stats, &f->error));
  assert_int_equal(stats.total_requests, 1);
  assert_int_equal(stats.total_batches, 1);
}
static void test_single_direct_needs_no_peer(void **state) {
  single_request(*state, DIRECT_APPEND);
}
static void test_single_push_needs_no_peer(void **state) {
  single_request(*state, PUSH_APPEND);
}
static void test_single_staged_needs_no_peer(void **state) {
  single_request(*state, STAGED_APPEND);
}
static void test_unbounded_direct_group(void **state) {
  fixture *f = *state;
  f->batch_limit = 0;
  parallel(f, 0, 0, 0, 0, 65536, DIRECT_APPEND);
}
static void test_unbounded_push_group(void **state) {
  fixture *f = *state;
  f->batch_limit = 0;
  parallel(f, 0, 1, 0, 0, 65536, PUSH_APPEND);
}
static void test_unbounded_staged_group(void **state) {
  fixture *f = *state;
  f->batch_limit = 0;
  parallel(f, 0, 1, 0, 0, 65536, STAGED_APPEND);
}
static void test_group_limit_two(void **state) {
  fixture *f = *state;
  f->batch_limit = 2;
  parallel(f, 0, 0, 0, 0, 65536, DIRECT_APPEND);
}
static void test_group_limit_one(void **state) {
  fixture *f = *state;
  f->batch_limit = 1;
  parallel(f, 0, 0, 0, 0, 65536, DIRECT_APPEND);
}
static void test_exclusive_handles(void **state) {
  parallel(*state, 1, 0, 0, 0, 65536, 0);
}
static void test_same_receiver(void **state) {
  parallel(*state, 0, 1, 0, 0, 65536, 0);
}
static void test_duplicate_pending_identity(void **state) {
  parallel(*state, 0, 0, 1, 0, 65536, 0);
}
static void test_sync_failure_reaches_every_waiter(void **state) {
  parallel(*state, 0, 0, 0, 1, 65536, 0);
}
static void test_rotation_preserves_commit_descriptors(void **state) {
  parallel(*state, 0, 1, 0, 0, 256, 0);
}

static void test_push_shared_group(void **state) {
  parallel(*state, 0, 0, 0, 0, 65536, 1);
}
static void test_push_exclusive_same_receiver_group(void **state) {
  parallel(*state, 1, 1, 0, 0, 65536, 1);
}
static void test_push_duplicate_group(void **state) {
  parallel(*state, 0, 0, 1, 0, 65536, 1);
}
static void test_push_sync_failure_group(void **state) {
  parallel(*state, 0, 1, 0, 1, 65536, 1);
}
static void test_push_rotation_group(void **state) {
  parallel(*state, 0, 1, 0, 0, 256, 1);
}

static void spilled_group(void **state, append_kind kind) {
  fixture *f = *state;
  char *body = malloc(131073);
  assert_non_null(body);
  memcpy(body, "{\"v\":\"", 6);
  memset(body + 6, 'x', 131064);
  memcpy(body + 131070, "\"}", 3);
  f->body = body;
  parallel(f, 0, 1, 0, 0, 1024 * 1024, kind);
  f->body = NULL;
  free(body);
}

static void test_push_spilled_group_on_default_thread_stacks(void **state) {
  spilled_group(state, PUSH_APPEND);
}
static void test_staged_spilled_group_on_default_thread_stacks(void **state) {
  spilled_group(state, STAGED_APPEND);
}
static void test_staged_shared_group(void **state) {
  parallel(*state, 0, 0, 0, 0, 65536, STAGED_APPEND);
}
static void test_staged_exclusive_same_receiver_group(void **state) {
  parallel(*state, 1, 1, 0, 0, 65536, STAGED_APPEND);
}
static void test_staged_duplicate_group(void **state) {
  parallel(*state, 0, 0, 1, 0, 65536, STAGED_APPEND);
}
static void test_staged_sync_failure_group(void **state) {
  parallel(*state, 0, 1, 0, 1, 65536, STAGED_APPEND);
}
static void test_staged_rotation_group(void **state) {
  parallel(*state, 0, 1, 0, 0, 256, STAGED_APPEND);
}

static int hold_commit(void *context, lc_collection *collection,
                       lc_error *error) {
  fixture *f = context;
  struct timespec deadline;
  int rc = 0;
  (void)collection;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&f->mutex);
  ++f->staged;
  pthread_cond_broadcast(&f->condition);
  while (!f->release && rc == 0)
    rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  pthread_mutex_unlock(&f->mutex);
  return rc == 0 ? LC_OK
                 : lc_error_set(error, LC_ERR_TIMEOUT, 0,
                                "commit hold timed out", NULL, NULL, NULL);
}

typedef struct observer {
  fixture *f;
  int mutate, started, finished, rc;
  lc_collection_info info;
} observer;

static int observed_bytes(lc_sink *sink, const void *bytes, size_t count,
                          lc_error *error) {
  observer *o = sink->impl;
  int released;
  (void)bytes;
  (void)count;
  pthread_mutex_lock(&o->f->mutex);
  released = o->f->release;
  pthread_mutex_unlock(&o->f->mutex);
  if (!released) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0,
                 "read exposed an unsynchronized entry", NULL, NULL, NULL);
    return 0;
  }
  return 1;
}

static void *observe_thread(void *context) {
  observer *o = context;
  lc_error error;
  lc_error_init(&error);
  pthread_mutex_lock(&o->f->mutex);
  o->started = 1;
  pthread_cond_broadcast(&o->f->condition);
  pthread_mutex_unlock(&o->f->mutex);
  if (o->mutate == 3) {
    lc_collection_read_res result = {0};
    lc_sink sink = {0};
    sink.write = observed_bytes;
    sink.impl = o;
    o->rc = lc_collection_read(o->f->handles[1], NULL, &sink, &result, &error);
    o->info.live_count = result.completed_records;
    if (o->rc == LC_OK && !result.complete)
      o->rc = LC_ERR_PROTOCOL;
    lc_collection_read_res_cleanup(&result);
  } else if (o->mutate == 2) {
    lc_pouch_maintenance_result result = {0};
    o->rc = lc_collection_compact(o->f->handles[1], 1, 0, &result, &error);
    lc_pouch_maintenance_result_cleanup(NULL, &result);
    if (o->rc == LC_OK)
      o->rc = lc_collection_describe(o->f->handles[1], &o->info, &error);
  } else if (o->mutate) {
    lc_collection_metadata_req req = {1, 3600};
    o->rc = lc_collection_metadata(o->f->handles[1], &req, &o->info, &error);
  } else
    o->rc = lc_collection_describe(o->f->handles[1], &o->info, &error);
  lc_error_cleanup(&error);
  pthread_mutex_lock(&o->f->mutex);
  o->finished = 1;
  pthread_cond_broadcast(&o->f->condition);
  pthread_mutex_unlock(&o->f->mutex);
  return NULL;
}

static void visibility(fixture *f, int mutate) {
  pthread_t appender, observer_thread;
  struct timespec deadline;
  writer work = {0};
  observer view = {0};
  int wait_rc = 0, pin, probe_rc, probe_errno, premature;
  char path[768];
  open_handles(f, 0, 65536);
  lc_collection_handle *handle = f->handles[0]->impl;
  snprintf(path, sizeof(path), "%s/.commit.lock", handle->directory);
  lc_collection_commit_wait_test_hook = hold_commit;
  lc_collection_commit_wait_test_context = f;
  work.collection = f->handles[0];
  work.id = "inflight";
  work.body = "{}";
  assert_int_equal(pthread_create(&appender, NULL, append_thread, &work), 0);
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&f->mutex);
  while (!f->staged && wait_rc == 0)
    wait_rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  pthread_mutex_unlock(&f->mutex);
  /* An independent open description must be fenced while a sync is pending. */
  pin = open(path, O_RDWR | O_CLOEXEC);
  probe_rc = pin < 0 ? -1 : flock(pin, LOCK_EX | LOCK_NB);
  probe_errno = errno;
  if (pin >= 0)
    close(pin);
  view.f = f;
  view.mutate = mutate;
  assert_int_equal(
      pthread_create(&observer_thread, NULL, observe_thread, &view), 0);
  pthread_mutex_lock(&f->mutex);
  while (!view.started && wait_rc == 0)
    wait_rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  /* The lock probe supplies a deterministic fence; this bounded condition wait
   * additionally checks that the real public operation does not complete. */
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_nsec += 50000000L;
  if (deadline.tv_nsec >= 1000000000L) {
    ++deadline.tv_sec;
    deadline.tv_nsec -= 1000000000L;
  }
  while (!view.finished && wait_rc == 0)
    wait_rc = pthread_cond_timedwait(&f->condition, &f->mutex, &deadline);
  premature = view.finished;
  f->release = 1;
  pthread_cond_broadcast(&f->condition);
  pthread_mutex_unlock(&f->mutex);
  assert_int_equal(pthread_join(appender, NULL), 0);
  assert_int_equal(pthread_join(observer_thread, NULL), 0);
  lc_collection_commit_wait_test_hook = NULL;
  assert_true(pin >= 0 && probe_rc == -1 &&
              (probe_errno == EWOULDBLOCK || probe_errno == EAGAIN));
  assert_false(premature);
  assert_int_equal(work.rc, LC_OK);
  assert_int_equal(view.rc, LC_OK);
  assert_int_equal(view.info.live_count, 1);
}

static void test_capture_waits_for_sync(void **state) { visibility(*state, 0); }
static void test_compaction_waits_for_sync(void **state) {
  visibility(*state, 2);
}
static void test_streaming_read_waits_for_sync(void **state) {
  visibility(*state, 3);
}
static void
test_metadata_waits_for_sync_without_blocking_completion(void **state) {
  visibility(*state, 1);
}

typedef struct callback_source {
  lc_source source;
  lc_collection *peer;
  size_t offset;
  int called;
} callback_source;

static size_t callback_read(lc_source *source, void *bytes, size_t count,
                            lc_error *error) {
  callback_source *s = source->impl;
  lc_collection_info info;
  if (lc_collection_describe(s->peer, &info, error) != LC_OK)
    return 0;
  if (info.live_count != 0) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0, "source saw its unpublished entry",
                 NULL, NULL, NULL);
    return 0;
  }
  ++s->called;
  if (s->offset == 2 || count == 0)
    return 0;
  count = count > 2 - s->offset ? 2 - s->offset : count;
  memcpy(bytes, "{}" + s->offset, count);
  s->offset += count;
  return count;
}

static void test_source_callback_reads_previous_view(void **state) {
  fixture *f = *state;
  callback_source source = {0};
  lc_collection_append_req request = {0};
  lc_collection_append_res result;
  open_handles(f, 0, 65536);
  source.source.read = callback_read;
  source.source.impl = &source;
  source.peer = f->handles[1];
  request.append_id = "callback";
  check(f, lc_collection_append(f->handles[0], &request, &source.source,
                                &result, &f->error));
  assert_true(source.called > 0);
}

static int crash_before_sync(void *context, lc_collection *collection,
                             lc_error *error) {
  (void)context;
  (void)collection;
  (void)error;
  _exit(42);
}

static void test_crash_pending_commit_releases_pin(void **state) {
  fixture *f = *state;
  int status;
  lc_collection_config config = {0};
  writer work = {0};
  config.name = "events";
  open_handles(f, 0, 65536);
  close_all(f);
  pid_t child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    open_client(f, 0, 0, 65536);
    check(f, lc_client_open_collection(f->clients[0], &config, &f->handles[0],
                                       &f->error));
    lc_collection_commit_wait_test_hook = crash_before_sync;
    work.collection = f->handles[0];
    work.id = "crashed";
    work.body = "{}";
    append_thread(&work);
    _exit(1);
  }
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 42);
  open_client(f, 0, 0, 65536);
  check(f, lc_client_open_collection(f->clients[0], &config, &f->handles[0],
                                     &f->error));
  work.collection = f->handles[0];
  work.id = "crashed";
  work.body = "{}";
  append_thread(&work);
  assert_int_equal(work.rc, LC_OK);
  assert_true(work.result.duplicate && work.result.position == 1);
  work.id = "after-crash";
  append_thread(&work);
  assert_int_equal(work.rc, LC_OK);
  assert_true(!work.result.duplicate && work.result.position == 2);
}

static void
test_duplicate_after_compaction_syncs_snapshot_binding(void **state) {
  fixture *f = *state;
  writer work = {0};
  lc_pouch_maintenance_result result = {0};
  open_handles(f, 0, 65536);
  work.collection = f->handles[0];
  work.id = "snapshot";
  work.body = "{}";
  append_thread(&work);
  assert_int_equal(work.rc, LC_OK);
  check(f, lc_collection_compact(work.collection, 1, 0, &result, &f->error));
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  append_thread(&work);
  assert_int_equal(work.rc, LC_OK);
  assert_true(work.result.duplicate && work.result.position == 1);
}

typedef struct process_pin {
  int ready, release;
} process_pin;
static int process_staged(void *context, lc_collection *collection,
                          lc_error *error) {
  process_pin *pin = context;
  char byte = 'x';
  (void)collection;
  if (write(pin->ready, &byte, 1) != 1 || read(pin->release, &byte, 1) != 1)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                        "process commit rendezvous failed", NULL, NULL, NULL);
  return LC_OK;
}

static void test_process_writers_release_shared_root_before_sync(void **state) {
  fixture *f = *state;
  int ready[2][2], release[2][2], statuses[2], reached = 0;
  pid_t children[2];
  open_handles(f, 0, 65536);
  close_all(f);
  for (size_t i = 0; i < 2; ++i) {
    assert_int_equal(pipe(ready[i]), 0);
    assert_int_equal(pipe(release[i]), 0);
  }
  for (size_t i = 0; i < 2; ++i) {
    children[i] = fork();
    assert_true(children[i] >= 0);
    if (children[i] == 0) {
      process_pin pin = {ready[i][1], release[i][0]};
      lc_collection_config config = {0};
      writer work = {0};
      config.name = "events";
      open_client(f, 0, 0, 65536);
      check(f, lc_client_open_collection(f->clients[0], &config, &f->handles[0],
                                         &f->error));
      lc_collection_commit_wait_test_hook = process_staged;
      lc_collection_commit_wait_test_context = &pin;
      work.collection = f->handles[0];
      work.id = i == 0 ? "process-one" : "process-two";
      work.body = "{}";
      append_thread(&work);
      lc_collection_commit_wait_test_hook = NULL;
      close_all(f);
      lc_error_cleanup(&f->error);
      exit(work.rc == LC_OK ? 0 : 1);
    }
  }
  for (size_t i = 0; i < 2; ++i) {
    struct pollfd pollfd = {ready[i][0], POLLIN, 0};
    char byte;
    if (poll(&pollfd, 1, 5000) == 1 && read(ready[i][0], &byte, 1) == 1)
      ++reached;
  }
  /* Always release/join both children before asserting, including failures. */
  for (size_t i = 0; i < 2; ++i)
    assert_int_equal(write(release[i][1], "x", 1), 1);
  for (size_t i = 0; i < 2; ++i) {
    assert_int_equal(waitpid(children[i], &statuses[i], 0), children[i]);
    close(ready[i][0]);
    close(ready[i][1]);
    close(release[i][0]);
    close(release[i][1]);
  }
  assert_int_equal(reached, 2);
  for (size_t i = 0; i < 2; ++i)
    assert_true(WIFEXITED(statuses[i]) && WEXITSTATUS(statuses[i]) == 0);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(test_single_direct_needs_no_peer, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_single_push_needs_no_peer, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_single_staged_needs_no_peer, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_unbounded_direct_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_unbounded_push_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_unbounded_staged_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_group_limit_two, setup, teardown),
      cmocka_unit_test_setup_teardown(test_group_limit_one, setup, teardown),
      cmocka_unit_test_setup_teardown(test_staged_shared_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_staged_exclusive_same_receiver_group,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_staged_duplicate_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_staged_sync_failure_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_staged_rotation_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_staged_spilled_group_on_default_thread_stacks, setup, teardown),
      cmocka_unit_test_setup_teardown(test_push_shared_group, setup, teardown),
      cmocka_unit_test_setup_teardown(test_push_exclusive_same_receiver_group,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_push_duplicate_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_push_sync_failure_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_push_rotation_group, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_push_spilled_group_on_default_thread_stacks, setup, teardown),
      cmocka_unit_test_setup_teardown(test_shared_handles, setup, teardown),
      cmocka_unit_test_setup_teardown(test_exclusive_handles, setup, teardown),
      cmocka_unit_test_setup_teardown(test_same_receiver, setup, teardown),
      cmocka_unit_test_setup_teardown(test_duplicate_pending_identity, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_sync_failure_reaches_every_waiter,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_rotation_preserves_commit_descriptors, setup, teardown),
      cmocka_unit_test_setup_teardown(test_capture_waits_for_sync, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_compaction_waits_for_sync, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_streaming_read_waits_for_sync, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_metadata_waits_for_sync_without_blocking_completion, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_source_callback_reads_previous_view,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_process_writers_release_shared_root_before_sync, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_crash_pending_commit_releases_pin,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_duplicate_after_compaction_syncs_snapshot_binding, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
