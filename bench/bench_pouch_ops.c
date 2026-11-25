#include "bench_pouch.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct bench_phase {
  const char *name;
  double *samples;
  size_t count, capacity;
  double wall, cpu;
} bench_phase;

typedef struct bench_store {
  char root[512], url[640];
  const char *endpoint;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *owner, *peer;
  lc_error error;
} bench_store;

static double now(clockid_t clock) {
  struct timespec ts;
  if (clock_gettime(clock, &ts) != 0) {
    perror("clock_gettime");
    exit(1);
  }
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void require(int ok, const char *message) {
  if (!ok) {
    fprintf(stderr, "Pouch benchmark verification failed: %s\n", message);
    exit(1);
  }
}

static void check(int rc, const lc_error *e, const char *operation) {
  if (rc != LC_OK) {
    fprintf(stderr, "Pouch benchmark %s failed (%d): %s [%s]\n", operation, rc,
            e->message ? e->message : "", e->server_code ? e->server_code : "");
    exit(1);
  }
}

static void record(bench_phase *phase, double wall, double cpu) {
  double elapsed = now(CLOCK_MONOTONIC) - wall;
  phase->cpu += now(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  if (phase->capacity == phase->count) {
    size_t capacity = phase->capacity ? phase->capacity * 2U : 32U;
    double *samples = realloc(phase->samples, capacity * sizeof(*samples));
    require(samples != NULL, "allocate latency samples");
    phase->samples = samples;
    phase->capacity = capacity;
  }
  phase->wall += elapsed;
  phase->samples[phase->count++] = elapsed;
}

#define MEASURE(phase, operation)                                              \
  do {                                                                         \
    double wall_start = now(CLOCK_MONOTONIC);                                  \
    double cpu_start = now(CLOCK_PROCESS_CPUTIME_ID);                          \
    int operation_rc = (operation);                                            \
    record(&(phase), wall_start, cpu_start);                                   \
    check(operation_rc, e, (phase).name);                                      \
  } while (0)

static int compare_elapsed(const void *left, const void *right) {
  double a = *(const double *)left;
  double b = *(const double *)right;
  return (a > b) - (a < b);
}

static void report(bench_phase *phases, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    bench_phase *p = &phases[i];
    require(p->count != 0U, p->name);
    qsort(p->samples, p->count, sizeof(*p->samples), compare_elapsed);
    printf("metric=%s operations=%zu seconds=%.9f cpu_seconds=%.9f "
           "per_op_us=%.3f p95_us=%.3f\n",
           p->name, p->count, p->wall, p->cpu, p->wall * 1e6 / (double)p->count,
           p->samples[p->count - p->count / 20U - 1U] * 1e6);
    free(p->samples);
  }
}

#define REPORT(phases) report(phases, sizeof(phases) / sizeof((phases)[0]))

static void open_store(bench_store *store, const char *name, int shared,
                       int watch) {
  memset(store, 0, sizeof(*store));
  lc_error_init(&store->error);
  require(bench_pouch_root_path(store->root, sizeof(store->root), name) == 0,
          "create temporary root");
  snprintf(store->url, sizeof(store->url), "pouch://%s", store->root);
  store->endpoint = store->url;
  lc_pouch_settings_init(&store->settings);
  store->settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_QUEUE_WATCH;
  store->settings.single_writer = !shared;
  store->settings.queue_watch = watch;
  lc_client_config_init(&store->config);
  store->config.endpoints = &store->endpoint;
  store->config.endpoint_count = 1U;
  store->config.default_namespace = "bench";
  store->config.pouch_settings = &store->settings;
  check(bench_open_pouch(&store->config, 1, 1, &store->owner, &store->error),
        &store->error, "create owner");
  if (shared)
    check(bench_open_pouch(&store->config, 0, 1, &store->peer, &store->error),
          &store->error, "join owner");
}

static void close_store(bench_store *store) {
  lc_client_close(store->peer);
  lc_client_close(store->owner);
  bench_pouch_cleanup_root(store->root);
  lc_error_cleanup(&store->error);
}

int bench_pouch_lease(long iterations) {
  bench_store store;
  lc_error *e = &store.error;
  lc_acquire_req req = {.key = "held", .owner = "first", .ttl_seconds = 120L};
  lc_keepalive_req renew = {.ttl_seconds = 180L};
  enum { ACQUIRE, CONTENDED, RENEW, RELEASE, HANDOFF };
  bench_phase phases[] = {{.name = "lease-acquire"},
                          {.name = "lease-contended"},
                          {.name = "lease-keepalive"},
                          {.name = "lease-release"},
                          {.name = "lease-peer-handoff"}};
  open_store(&store, "lease-ops", 1, 0);
  for (long i = 0; i < iterations; ++i) {
    lc_lease *lease = NULL, *peer = NULL;
    long fence;
    lc_unix_seconds expiry;
    double wall, cpu;
    int rc;
    req.owner = "first";
    MEASURE(phases[ACQUIRE], lc_acquire(store.owner, &req, &lease, e));
    fence = lease->fencing_token;
    expiry = lease->lease_expires_at_unix;
    req.owner = "second";
    wall = now(CLOCK_MONOTONIC);
    cpu = now(CLOCK_PROCESS_CPUTIME_ID);
    rc = lc_acquire(store.peer, &req, &peer, e);
    record(&phases[CONTENDED], wall, cpu);
    require(rc == LC_ERR_INVALID && !peer,
            "competing owner is refused while the lease is held");
    lc_error_cleanup(e);
    lc_error_init(e);
    MEASURE(phases[RENEW], lc_lease_keepalive(lease, &renew, e));
    require(lease->fencing_token == fence &&
                lease->lease_expires_at_unix > expiry,
            "keepalive extends the lease at the same fence");
    MEASURE(phases[RELEASE], lc_lease_release(lease, NULL, e));
    MEASURE(phases[HANDOFF], lc_acquire(store.peer, &req, &peer, e));
    require(peer->fencing_token > fence, "handoff advances the fencing token");
    check(lc_lease_release(peer, NULL, e), e, "release peer");
  }
  close_store(&store);
  REPORT(phases);
  return 0;
}

typedef struct bench_copy {
  const char *body;
  size_t length, offset;
} bench_copy;

static int verify_chunk(lc_sink *sink, const void *bytes, size_t count,
                        lc_error *error) {
  bench_copy *copy = sink->impl;
  if (count > copy->length - copy->offset ||
      memcmp(copy->body + copy->offset, bytes, count) != 0) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0L, "stream returned different bytes",
                 NULL, NULL, NULL);
    return 0;
  }
  copy->offset += count;
  return 1;
}

int bench_pouch_state_api(long iterations) {
  enum { PUT, MUTATE, LOCAL_MUTATE, META, DESCRIBE, READ, REMOVE };
  bench_phase phases[] = {
      {.name = "state-lease-update"},       {.name = "state-lease-mutate"},
      {.name = "state-lease-mutate-local"}, {.name = "state-lease-metadata"},
      {.name = "state-peer-describe"},      {.name = "state-lease-read"},
      {.name = "state-lease-remove"}};
  bench_store store;
  lc_error *e = &store.error;
  lc_acquire_req acquire = {
      .key = "document", .owner = "benchmark", .ttl_seconds = 3600L};
  const char *expressions[] = {"/counter++"};
  lc_mutate_req mutate = {.mutations = expressions, .mutation_count = 1U};
  lc_mutate_local_req local = {.mutations = expressions, .mutation_count = 1U};
  lc_metadata_req meta = {.has_query_hidden = 1, .query_hidden = 1};
  lc_describe_req describe = {.key = acquire.key};
  const char seed[] = "{\"counter\":0}", expected[] = "{\"counter\":2}";
  lc_lease *lease = NULL;
  open_store(&store, "state-operations", 1, 0);
  check(lc_acquire(store.owner, &acquire, &lease, e), e, "acquire state lease");
  for (long i = 0; i < iterations; ++i) {
    lc_source *source = NULL;
    lc_describe_res described = {0};
    lc_get_res got = {0};
    bench_copy copy = {expected, sizeof(expected) - 1U, 0U};
    lc_sink sink = {.write = verify_chunk, .impl = &copy};
    check(lc_source_from_memory(seed, sizeof(seed) - 1U, &source, e), e,
          "create source");
    MEASURE(phases[PUT], lc_lease_update(lease, source, NULL, e));
    lc_source_close(source);
    MEASURE(phases[MUTATE], lc_lease_mutate(lease, &mutate, e));
    MEASURE(phases[LOCAL_MUTATE], lc_lease_mutate_local(lease, &local, e));
    MEASURE(phases[META], lc_lease_metadata(lease, &meta, e));
    MEASURE(phases[DESCRIBE],
            lc_describe(store.peer, &describe, &described, e));
    require(described.owner && strcmp(described.owner, acquire.owner) == 0 &&
                described.version == lease->version &&
                described.has_query_hidden && described.query_hidden,
            "peer saw the updated state and query visibility");
    lc_describe_res_cleanup(&described);
    MEASURE(phases[READ], lc_lease_get(lease, &sink, NULL, &got, e));
    require(!got.no_content && copy.offset == copy.length,
            "leased read saw both mutations");
    lc_get_res_cleanup(&got);
    MEASURE(phases[REMOVE], lc_lease_remove(lease, NULL, e));
    copy.offset = 0U;
    check(lc_lease_get(lease, &sink, NULL, &got, e), e, "verify removed state");
    require(got.no_content && copy.offset == 0U, "read observed removed state");
    lc_get_res_cleanup(&got);
  }
  check(lc_lease_release(lease, NULL, e), e, "release state lease");
  close_store(&store);
  REPORT(phases);
  return 0;
}

int bench_pouch_attachment(long iterations) {
  enum { ATTACH, LIST, GET, DELETE };
  bench_phase phases[] = {{.name = "attachment-put"},
                          {.name = "attachment-list"},
                          {.name = "attachment-get"},
                          {.name = "attachment-delete"}};
  bench_store store;
  lc_error *e = &store.error;
  lc_acquire_req lease_req = {
      .key = "object", .owner = "benchmark", .ttl_seconds = 3600L};
  lc_attach_req attach = {.name = "payload", .content_type = "text/plain"};
  lc_attachment_get_req get = {.selector = {.name = attach.name}};
  const size_t size = 64U * 1024U;
  char *body = malloc(size);
  lc_lease *lease = NULL;
  require(body != NULL, "attachment input allocation");
  for (size_t i = 0; i < size; ++i)
    body[i] = (char)('a' + i % 26U);
  open_store(&store, "attachment-ops", 0, 0);
  check(lc_acquire(store.owner, &lease_req, &lease, e), e,
        "acquire attachment lease");
  for (long i = 0; i < iterations; ++i) {
    lc_source *source = NULL;
    lc_attach_res attached = {0};
    lc_attachment_list list = {0};
    lc_attachment_get_res fetched = {0};
    bench_copy copy = {body, size, 0U};
    lc_sink sink = {.write = verify_chunk, .impl = &copy};
    int deleted = 0;
    check(lc_source_from_memory(body, size, &source, e), e, "create source");
    MEASURE(phases[ATTACH],
            lc_lease_attach(lease, &attach, source, &attached, e));
    lc_source_close(source);
    require(!attached.noop && attached.attachment.name &&
                strcmp(attached.attachment.name, attach.name) == 0,
            "attachment publication");
    MEASURE(phases[LIST], lc_lease_list_attachments(lease, &list, e));
    require(list.count == 1U && strcmp(list.items[0].name, attach.name) == 0,
            "listed the new attachment");
    MEASURE(phases[GET],
            lc_lease_get_attachment(lease, &get, &sink, &fetched, e));
    require(copy.offset == size, "attachment stream length");
    MEASURE(phases[DELETE],
            lc_lease_delete_attachment(lease, &get.selector, &deleted, e));
    require(deleted == 1, "attachment was deleted");
    lc_attachment_get_res_cleanup(&fetched);
    lc_attachment_list_cleanup(&list);
    lc_attach_res_cleanup(&attached);
  }
  lc_attachment_list empty = {0};
  check(lc_lease_list_attachments(lease, &empty, e), e,
        "verify empty attachments");
  require(empty.count == 0U, "attachment deletion is visible");
  lc_attachment_list_cleanup(&empty);
  check(lc_lease_release(lease, NULL, e), e, "release attachment lease");
  close_store(&store);
  free(body);
  printf("attachment_bytes=%zu\n", size);
  REPORT(phases);
  return 0;
}

static int enqueue(lc_client *client, const char *queue, const char *body,
                   size_t size, lc_enqueue_res *result, lc_error *e) {
  lc_enqueue_req req;
  lc_source *source = NULL;
  lc_enqueue_req_init(&req);
  req.queue = queue;
  req.visibility_timeout_seconds = 120L;
  req.content_type = "text/plain";
  int rc = lc_source_from_memory(body, size, &source, e);
  if (rc == LC_OK)
    rc = lc_enqueue(client, &req, source, result, e);
  lc_source_close(source);
  return rc;
}

int bench_pouch_queue_delivery(long iterations) {
  enum { ENQUEUE, STATS, BATCH, PAYLOAD, EXTEND, DEFER, ACK, REDELIVER };
  bench_phase phases[] = {
      {.name = "queue-enqueue"},       {.name = "queue-stats"},
      {.name = "queue-dequeue-batch"}, {.name = "queue-read-payload"},
      {.name = "queue-extend"},        {.name = "queue-defer"},
      {.name = "queue-ack"},           {.name = "queue-redeliver"}};
  enum { PAGE = 4 };
  bench_store store;
  lc_error *e = &store.error;
  lc_dequeue_req req;
  lc_queue_stats_req inspect = {.queue = "delivery"};
  lc_extend_req extend = {.extend_by_seconds = 180L};
  lc_nack_req nack = {.intent = LC_NACK_INTENT_DEFER};
  char body[1024];
  memset(body, 'q', sizeof(body));
  lc_dequeue_req_init(&req);
  req.queue = inspect.queue;
  req.owner = "consumer";
  req.visibility_timeout_seconds = 120L;
  open_store(&store, "queue-delivery", 1, 0);
  for (long i = 0; i < iterations; ++i) {
    lc_enqueue_res enqueued[PAGE] = {{0}};
    lc_dequeue_batch_res batch = {0};
    lc_queue_stats_res stats = {0};
    lc_message *retry = NULL;
    for (unsigned j = 0; j < PAGE; ++j) {
      body[0] = (char)('0' + j);
      MEASURE(phases[ENQUEUE], enqueue(store.owner, req.queue, body,
                                       sizeof(body), &enqueued[j], e));
      require(enqueued[j].message_id != NULL, "enqueue receipt");
    }
    MEASURE(phases[STATS], lc_queue_stats(store.peer, &inspect, &stats, e));
    require(stats.available && stats.pending_candidates == PAGE,
            "queue stats see all unclaimed jobs");
    lc_queue_stats_res_cleanup(&stats);
    req.page_size = PAGE;
    MEASURE(phases[BATCH], lc_dequeue_batch(store.peer, &req, &batch, e));
    require(batch.count == PAGE, "complete dequeue batch");
    for (unsigned j = 0; j < PAGE; ++j) {
      unsigned k = 0;
      while (k < PAGE &&
             strcmp(batch.messages[j]->message_id, enqueued[k].message_id) != 0)
        ++k;
      require(k < PAGE, "dequeued an enqueued message");
      body[0] = (char)('0' + k);
      bench_copy copy = {body, sizeof(body), 0U};
      lc_sink sink = {.write = verify_chunk, .impl = &copy};
      size_t bytes = 0;
      MEASURE(phases[PAYLOAD],
              lc_message_write_payload(batch.messages[j], &sink, &bytes, e));
      require(bytes == sizeof(body) && copy.offset == sizeof(body),
              "queue body");
    }
    lc_unix_seconds expiry = batch.messages[0]->lease_expires_at_unix;
    MEASURE(phases[EXTEND], lc_message_extend(batch.messages[0], &extend, e));
    require(batch.messages[0]->lease_expires_at_unix > expiry,
            "delivery visibility was extended");
    MEASURE(phases[DEFER], lc_message_nack(batch.messages[0], &nack, e));
    for (unsigned j = 1; j < PAGE; ++j)
      MEASURE(phases[ACK], lc_message_ack(batch.messages[j], e));
    req.page_size = 0;
    MEASURE(phases[REDELIVER],
            lc_dequeue_with_state(store.peer, &req, &retry, e));
    require(retry &&
                strcmp(retry->message_id, batch.messages[0]->message_id) == 0 &&
                retry->attempts == 2 && retry->failure_attempts == 0,
            "only the deferred message was redelivered without a failure");
    require(lc_message_state(retry) != NULL,
            "redelivery acquired the state lease");
    MEASURE(phases[ACK], lc_message_ack(retry, e));
    lc_dequeue_batch_cleanup(&batch);
    for (unsigned j = 0; j < PAGE; ++j)
      lc_enqueue_res_cleanup(&enqueued[j]);
  }
  lc_queue_stats_res empty = {0};
  check(lc_queue_stats(store.peer, &inspect, &empty, e), e,
        "verify queue drained");
  require(!empty.available && empty.pending_candidates == 0, "queue drained");
  lc_queue_stats_res_cleanup(&empty);
  close_store(&store);
  REPORT(phases);
  return 0;
}

typedef struct bench_watch {
  lc_client *producer;
  const char *queue;
  lc_enqueue_res enqueued;
  bench_phase *phase;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  char *observed;
  double started, cpu;
  int ready;
} bench_watch;

static void *publish_watch_event(void *context) {
  bench_watch *watch = context;
  lc_error error;
  int ready;
  struct timespec delay = {0, 10L * 1000L * 1000L};
  pthread_mutex_lock(&watch->mutex);
  while (!watch->ready)
    pthread_cond_wait(&watch->condition, &watch->mutex);
  ready = watch->ready;
  pthread_mutex_unlock(&watch->mutex);
  if (ready < 0)
    return NULL;
  /* Position publication inside a wait slice without making the assertion
   * depend on the scheduler hitting that interval. */
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
  pthread_mutex_lock(&watch->mutex);
  watch->started = now(CLOCK_MONOTONIC);
  watch->cpu = now(CLOCK_PROCESS_CPUTIME_ID);
  pthread_mutex_unlock(&watch->mutex);
  lc_error_init(&error);
  check(enqueue(watch->producer, watch->queue, "wakeup", 6U, &watch->enqueued,
                &error),
        &error, "publish watch event");
  lc_error_cleanup(&error);
  return NULL;
}

static int watch_event(void *context, const lc_watch_event *event,
                       lc_error *e) {
  bench_watch *watch = context;
  pthread_mutex_lock(&watch->mutex);
  if (!watch->ready) {
    require(!event->available, "watch begins on an empty queue");
    watch->ready = 1;
    pthread_cond_signal(&watch->condition);
    pthread_mutex_unlock(&watch->mutex);
    return 1;
  }
  require(watch->started > 0.0, "producer started before the observed event");
  record(watch->phase, watch->started, watch->cpu);
  pthread_mutex_unlock(&watch->mutex);
  require(event->available && event->head_message_id,
          "watch observed a head message");
  watch->observed = strdup(event->head_message_id);
  require(watch->observed != NULL, "retain observed message ID");
  lc_error_set(e, LC_ERR_PENDING, 0L, "watch benchmark observed a message",
               NULL, NULL, NULL);
  return 0;
}

int bench_pouch_queue_watch(long iterations) {
  bench_phase phases[] = {{.name = "queue-watch-local-publish"},
                          {.name = "queue-watch-peer-publish"}};
  bench_store store;
  lc_error *e = &store.error;
  lc_watch_queue_req req = {.ns = "bench", .queue = "observed"};
  lc_dequeue_req dequeue;
  lc_enqueue_res warm = {0};
  lc_message *message = NULL;
  lc_dequeue_req_init(&dequeue);
  dequeue.queue = req.queue;
  dequeue.owner = "watcher";
  open_store(&store, "queue-watch", 1, 1);
  check(enqueue(store.owner, req.queue, "warmup", 6U, &warm, e), e,
        "initialize notification state");
  check(lc_dequeue(store.owner, &dequeue, &message, e), e, "clear warmup");
  require(message != NULL, "warmup message was delivered");
  check(lc_message_ack(message, e), e, "ack warmup");
  lc_enqueue_res_cleanup(&warm);
  for (unsigned peer = 0; peer < 2U; ++peer) {
    for (long i = 0; i < iterations; ++i) {
      bench_watch watch = {.producer = peer ? store.peer : store.owner,
                           .queue = req.queue,
                           .phase = &phases[peer],
                           .mutex = PTHREAD_MUTEX_INITIALIZER,
                           .condition = PTHREAD_COND_INITIALIZER};
      lc_watch_handler handler = {.handle = watch_event, .context = &watch};
      pthread_t producer;
      int rc;
      require(pthread_create(&producer, NULL, publish_watch_event, &watch) == 0,
              "start watch producer");
      rc = lc_watch_queue(store.owner, &req, &handler, e);
      pthread_mutex_lock(&watch.mutex);
      if (!watch.ready) {
        watch.ready = -1;
        pthread_cond_signal(&watch.condition);
      }
      pthread_mutex_unlock(&watch.mutex);
      require(pthread_join(producer, NULL) == 0, "join watch producer");
      pthread_cond_destroy(&watch.condition);
      pthread_mutex_destroy(&watch.mutex);
      require(rc == LC_ERR_PENDING && watch.observed &&
                  watch.enqueued.message_id &&
                  strcmp(watch.observed, watch.enqueued.message_id) == 0,
              "watch stopped after observing the produced message");
      lc_error_cleanup(e);
      lc_error_init(e);
      message = NULL;
      check(lc_dequeue(store.owner, &dequeue, &message, e), e, "dequeue event");
      require(message && strcmp(message->message_id, watch.observed) == 0,
              "watch and dequeue agree on the message");
      check(lc_message_ack(message, e), e, "ack watched message");
      lc_enqueue_res_cleanup(&watch.enqueued);
      free(watch.observed);
    }
  }
  close_store(&store);
  REPORT(phases);
  return 0;
}

int bench_pouch_transaction(long iterations) {
  enum { STAGE, STAGE_ACK, PREPARE, COMMIT, ROLLBACK, REPLAY };
  bench_phase phases[] = {
      {.name = "transaction-stage"},    {.name = "transaction-stage-queue-ack"},
      {.name = "transaction-prepare"},  {.name = "transaction-commit"},
      {.name = "transaction-rollback"}, {.name = "transaction-replay"}};
  char committed[48] = "";
  bench_store store;
  lc_error *e = &store.error;
  lc_txn_participant participants[] = {{.ns = "bench", .key = "transactional"},
                                       {.ns = "bench"}};
  lc_acquire_req acquire = {
      .key = participants[0].key, .owner = "transaction", .ttl_seconds = 120L};
  lc_txn_decision_req decision = {.participants = participants,
                                  .participant_count = 2U};
  lc_dequeue_req dequeue = {.queue = "transactional-queue",
                            .owner = "consumer",
                            .visibility_timeout_seconds = 120L};
  lc_queue_stats_req inspect = {.queue = dequeue.queue};
  require(iterations >= 2, "transaction workload needs both decisions");
  open_store(&store, "transaction", 1, 0);
  for (long i = 0; i < iterations; ++i) {
    char id[LC_XID_STRING_SIZE];
    char body[sizeof(committed)];
    char queue_key[128];
    const int rollback = (i % 2L) != 0L;
    const char *expected = rollback ? "rollback" : "commit";
    lc_lease *lease = NULL;
    lc_source *source = NULL;
    lc_txn_decision_res decided = {0};
    lc_txn_replay_req replay = {.txn_id = id};
    lc_txn_replay_res observed = {0};
    bench_copy copy = {.body = committed};
    lc_sink sink = {.write = verify_chunk, .impl = &copy};
    lc_get_res got = {0};
    lc_enqueue_res enqueued = {0};
    lc_message *message = NULL;
    lc_ack_op ack = {0};
    lc_ack_res acked = {0};
    lc_queue_stats_res stats = {0};
    int n;
    check(lc_xid_new(id, e), e, "allocate transaction ID");
    snprintf(body, sizeof(body), "{\"revision\":%ld}", i);
    acquire.txn_id = decision.txn_id = id;
    decision.expires_at_unix = (lc_unix_seconds)time(NULL) + 120;
    check(lc_acquire(store.owner, &acquire, &lease, e), e,
          "acquire participant");
    check(lc_source_from_memory(body, strlen(body), &source, e), e,
          "create source");
    MEASURE(phases[STAGE], lc_lease_update(lease, source, NULL, e));
    lc_source_close(source);
    check(enqueue(store.owner, dequeue.queue, body, strlen(body), &enqueued, e),
          e, "enqueue transaction input");
    dequeue.txn_id = id;
    check(lc_dequeue(store.owner, &dequeue, &message, e), e,
          "acquire transaction delivery");
    require(message && message->txn_id && strcmp(message->txn_id, id) == 0 &&
                strcmp(message->message_id, enqueued.message_id) == 0,
            "delivery has the same transaction identity as state");
    ack.message.ns = message->ns;
    ack.message.queue = message->queue;
    ack.message.message_id = message->message_id;
    ack.message.lease_id = message->lease_id;
    ack.message.txn_id = message->txn_id;
    ack.message.fencing_token = message->fencing_token;
    ack.message.meta_etag = message->meta_etag;
    MEASURE(phases[STAGE_ACK], lc_queue_ack(store.owner, &ack, &acked, e));
    require(acked.acked, "queue acknowledgement was staged");
    lc_ack_res_cleanup(&acked);
    n = snprintf(queue_key, sizeof(queue_key), "q/%s/msg/%s", message->queue,
                 message->message_id);
    require(n > 0 && (size_t)n < sizeof(queue_key), "queue participant key");
    participants[1].key = queue_key;
    lc_message_close(message);
    MEASURE(phases[PREPARE],
            lc_txn_prepare(store.peer, &decision, &decided, e));
    require(decided.state && strcmp(decided.state, "prepare") == 0,
            "transaction prepared");
    lc_txn_decision_res_cleanup(&decided);
    if (rollback)
      MEASURE(phases[ROLLBACK],
              lc_txn_rollback(store.peer, &decision, &decided, e));
    else
      MEASURE(phases[COMMIT],
              lc_txn_commit(store.peer, &decision, &decided, e));
    require(decided.state && strcmp(decided.state, expected) == 0,
            "transaction reached the selected terminal state");
    lc_txn_decision_res_cleanup(&decided);
    lc_lease_close(lease);
    MEASURE(phases[REPLAY], lc_txn_replay(store.owner, &replay, &observed, e));
    require(observed.state && strcmp(observed.state, expected) == 0,
            "peer replay returned the same decision");
    lc_txn_replay_res_cleanup(&observed);
    if (!rollback)
      memcpy(committed, body, strlen(body) + 1U);
    copy.length = strlen(committed);
    check(lc_get(store.owner, acquire.key, NULL, &sink, &got, e), e,
          "verify the committed state");
    require(copy.offset == copy.length && !got.no_content,
            "only the committed body is visible");
    lc_get_res_cleanup(&got);
    check(lc_queue_stats(store.owner, &inspect, &stats, e), e,
          "verify committed queue decision");
    require(stats.available == rollback && stats.pending_candidates == rollback,
            "queue decision and state decision agree");
    lc_queue_stats_res_cleanup(&stats);
    if (rollback) {
      dequeue.txn_id = NULL;
      message = NULL;
      check(lc_dequeue(store.peer, &dequeue, &message, e), e,
            "redeliver after rollback");
      require(message && strcmp(message->message_id, enqueued.message_id) == 0,
              "rollback restored the original delivery");
      check(lc_message_ack(message, e), e, "ack rolled-back delivery");
    }
    lc_enqueue_res_cleanup(&enqueued);
  }
  close_store(&store);
  REPORT(phases);
  return 0;
}

static int count_collection(void *context, const lc_collection_info *info,
                            lc_error *e) {
  (void)e;
  require(strcmp(info->name, "events") == 0 && info->upper_position == 1U &&
              info->live_count == 1U,
          "listed collection identity and counts");
  ++*(unsigned *)context;
  return LC_OK;
}

int bench_collection_control(long iterations) {
  enum {
    CREATE,
    OPEN,
    LIST,
    OWN,
    RENEW,
    META,
    ENTRY_META,
    STATUS,
    DELETE,
    RELEASE,
    DROP
  };
  bench_phase phases[] = {{.name = "collection-create"},
                          {.name = "collection-open"},
                          {.name = "collection-list"},
                          {.name = "collection-acquire"},
                          {.name = "collection-keepalive"},
                          {.name = "collection-metadata"},
                          {.name = "collection-entry-metadata"},
                          {.name = "collection-append-status"},
                          {.name = "collection-delete-entry"},
                          {.name = "collection-release"},
                          {.name = "collection-drop"}};
  bench_store store;
  lc_error *e = &store.error;
  lc_collection_config config = {.name = "events"};
  lc_collection_append_req req = {.append_id = "event"};
  lc_collection_lease_req lease = {.owner = "benchmark", .ttl_seconds = 120L};
  lc_collection_metadata_req metadata = {.has_max_entries = 1,
                                         .max_entries = 50U};
  lc_collection_metadata_req entry_metadata = {.has_retention_seconds = 1,
                                               .retention_seconds = 3600L};
  open_store(&store, "collection-controls", 1, 0);
  for (long i = 0; i < iterations; ++i) {
    lc_collection *creator = NULL, *peer = NULL, *owned = NULL;
    lc_source *source = NULL;
    lc_collection_append_res append;
    lc_collection_append_status_res status;
    lc_collection_delete_res deleted;
    lc_collection_info info;
    lc_collection_lease_info renewed;
    unsigned listed = 0;
    MEASURE(phases[CREATE],
            lc_client_create_collection(store.owner, &config, &creator, e));
    check(lc_source_from_memory("{}", 2U, &source, e), e, "create source");
    check(lc_collection_append(creator, &req, source, &append, e), e,
          "seed collection");
    lc_source_close(source);
    require(append.position == 1U,
            "fresh collection has a fresh position space");
    MEASURE(phases[OPEN],
            lc_client_open_collection(store.peer, &config, &peer, e));
    MEASURE(phases[LIST], lc_client_list_collections(
                              store.peer, NULL, count_collection, &listed, e));
    require(listed == 1U, "peer listed the new collection");
    MEASURE(phases[OWN], lc_collection_acquire(peer, &lease, &owned, e));
    MEASURE(phases[RENEW], lc_collection_keepalive(owned, 180L, &renewed, e));
    require(renewed.active && renewed.fencing_token != 0U,
            "collection lease remained active");
    MEASURE(phases[META], lc_collection_metadata(owned, &metadata, &info, e));
    require(info.max_entries == 50U, "collection limit was updated");
    MEASURE(phases[ENTRY_META], lc_collection_entry_metadata(
                                    owned, 1U, &entry_metadata, &append, e));
    require(append.retention_expires_at_unix > 0 && append.position == 1U,
            "entry expiry changed without changing position");
    MEASURE(phases[STATUS],
            lc_collection_append_status(owned, req.append_id, &status, e));
    require(status.state == LC_COLLECTION_APPEND_COMMITTED &&
                status.entry.position == 1U,
            "append receipt is committed");
    MEASURE(phases[DELETE], lc_collection_delete_entry(owned, 1U, &deleted, e));
    require(deleted.deleted && deleted.position == 1U,
            "deleted the selected entry");
    check(lc_collection_append_status(owned, req.append_id, &status, e), e,
          "observe deleted append receipt");
    require(status.state == LC_COLLECTION_APPEND_DELETED,
            "append receipt retains deletion");
    MEASURE(phases[RELEASE], lc_collection_release(owned, e));
    MEASURE(phases[DROP], lc_collection_drop(creator, e));
    lc_collection_close(peer);
    lc_collection_close(creator);
    listed = 0;
    check(lc_client_list_collections(store.peer, NULL, count_collection,
                                     &listed, e),
          e, "list after drop");
    require(listed == 0U, "both owners observe the drop");
  }
  close_store(&store);
  REPORT(phases);
  return 0;
}
