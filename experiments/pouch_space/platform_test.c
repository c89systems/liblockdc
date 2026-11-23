#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE 1
#endif
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "pouch.h"
#include "readiness.h"
#include "space_internal.h"
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static lc_error error;
static const ps_key key = {1, 1, PS_STATE};

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s (%s)\n", __FILE__, __LINE__, #condition,      \
              error.message ? error.message : "no error");                     \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
#define OK(operation) REQUIRE((operation) == LC_OK)

static ps_space *open_shared(const char *path, int create) {
  ps_options options = {0};
  ps_space *space = NULL;
  options.shared = 1;
  options.durable_sync = 1;
  OK(ps_open(path, create, &options, &space, &error));
  return space;
}

static void produce(ps_space *space, const char *body) {
  ps_writer *writer = NULL;
  OK(ps_begin(space, NULL, key, &writer, &error));
  OK(ps_write(writer, body, strlen(body), &error));
  OK(ps_publish(writer, NULL, &error));
  ps_writer_close(writer);
}

static void prepare_unpublished(ps_space *space) {
  static const unsigned char chunk[2048] = {1};
  ps_writer *writer = NULL;
  OK(ps_begin(space, NULL, key, &writer, &error));
  for (size_t at = 0; at < 3U * PS_CHUNK; at += sizeof(chunk))
    OK(ps_write(writer, chunk, sizeof(chunk), &error));
}

static void kill_publisher(const char *self, const char *path,
                           const char *mode) {
  char ack_fd[32], ack;
  int pipefd[2], status;
  REQUIRE(!pipe(pipefd));
  REQUIRE(snprintf(ack_fd, sizeof(ack_fd), "%d", pipefd[1]) <
          (int)sizeof(ack_fd));
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    (void)close(pipefd[0]);
    execl(self, self, path, mode, ack_fd, (char *)NULL);
    _exit(127);
  }
  REQUIRE(!close(pipefd[1]) && read(pipefd[0], &ack, 1) == 1 && ack == '1');
  REQUIRE(!close(pipefd[0]) && !kill(child, SIGKILL) &&
          waitpid(child, &status, 0) == child && WIFSIGNALED(status) &&
          WTERMSIG(status) == SIGKILL);
}

static void expect(ps_reader *reader, const char *expected) {
  char bytes[64];
  size_t count = 0;
  OK(ps_read(reader, bytes, sizeof(bytes), &count, &error));
  REQUIRE(count == strlen(expected) && !memcmp(bytes, expected, count));
  OK(ps_read(reader, bytes, sizeof(bytes), &count, &error));
  REQUIRE(count == 0);
}

static void expect_current(ps_space *space, const char *expected) {
  ps_reader *reader = NULL;
  OK(ps_read_open(space, key, NULL, &reader, &error));
  expect(reader, expected);
  ps_reader_close(reader);
}

static void check_capacity(const char *path, uint64_t expected) {
  struct stat status;
  REQUIRE(!stat(path, &status) && (uint64_t)status.st_size == expected &&
          (uint64_t)status.st_blocks >= expected / 512U);
}

typedef struct growth_failure {
  ps_coordination *peer;
  unsigned int reserve_calls;
} growth_failure;

static int reject_growth(void *context, int fd, off_t offset, off_t length) {
  growth_failure *failure = context;
  ps_coordination_guard *guard = NULL;
  (void)fd;
  REQUIRE(offset == (off_t)PS_DEFAULT_CAPACITY && length == (off_t)PS_MIB);
  REQUIRE(ps_coordination_lock(failure->peer, PS_COORDINATION_ALLOCATION, 0, 0,
                               &guard, NULL) == LC_ERR_PENDING &&
          !guard);
  ++failure->reserve_calls;
  errno = ENOSPC;
  return -1;
}

static void check_failed_growth(const char *path) {
  ps_space *peer = open_shared(path, 0);
  growth_failure failure = {ps_space_coordination(peer), 0};
  ps_io io = ps_native_io;
  ps_options options = {0};
  ps_space *space = NULL;
  options.shared = 1;
  options.durable_sync = 1;
  options.io = &io;
  io.context = &failure;
  io.reserve = reject_growth;
  OK(ps_open(path, 0, &options, &space, &error));
  REQUIRE(ps_grow(space, PS_MIB, &error) == LC_ERR_TRANSPORT &&
          failure.reserve_calls == 1);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  OK(ps_close(space, &error));
  expect_current(peer, "durable successor");
  OK(ps_close(peer, &error));
  check_capacity(path, PS_DEFAULT_CAPACITY);
}

static uint64_t monotonic_ns(void) {
  struct timespec now;
  REQUIRE(!clock_gettime(CLOCK_MONOTONIC, &now));
  return (uint64_t)now.tv_sec * 1000000000U + (uint64_t)now.tv_nsec;
}

static lc_client *open_pouch(const char *path, int create) {
  char endpoint[1250];
  const char *endpoints[] = {endpoint};
  lc_client *client = NULL;
  lc_client_config config;
  lc_pouch_settings settings;
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?single_writer=false&background_compaction=false&"
                   "query_indexing=false&queue_watch=true",
                   path) < (int)sizeof(endpoint));
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_DURABLE_SYNC;
  settings.durable_sync = 1;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "journal";
  config.pouch_settings = &settings;
  OK(ps_pouch_client_open(&config, create, NULL, &client, &error));
  return client;
}

static void collection_round_trip(const char *path, int create) {
  static const char body[] = "{\"reading\":1}";
  lc_client *client = open_pouch(path, create);
  lc_collection *collection = NULL;
  lc_source *source = NULL;
  lc_sink *sink = NULL;
  lc_collection_config collection_config;
  lc_collection_append_req request;
  lc_collection_append_res appended;
  lc_collection_read_res result;
  const void *read;
  size_t count;
  lc_collection_config_init(&collection_config);
  collection_config.name = "events";
  OK(create ? lc_client_create_collection(client, &collection_config,
                                          &collection, &error)
            : lc_client_open_collection(client, &collection_config, &collection,
                                        &error));
  lc_collection_append_req_init(&request);
  request.append_id = "reading-1";
  OK(lc_source_from_memory(body, sizeof(body) - 1, &source, &error));
  OK(lc_collection_append(collection, &request, source, &appended, &error));
  REQUIRE(appended.position == 1 && appended.duplicate == !create);
  lc_source_close(source);
  OK(lc_sink_to_memory(&sink, &error));
  lc_collection_read_res_init(&result);
  OK(lc_collection_read(collection, NULL, sink, &result, &error));
  OK(lc_sink_memory_bytes(sink, &read, &count, &error));
  REQUIRE(count == sizeof(body) && !memcmp(read, body, sizeof(body) - 1) &&
          ((const char *)read)[count - 1] == '\n');
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
  lc_collection_close(collection);
  lc_client_close(client);
  check_capacity(path, PS_DEFAULT_CAPACITY);
}

static void lease_round_trip(lc_client *producer, lc_client *consumer) {
  lc_acquire_req acquire;
  lc_lease *lease = NULL;
  lc_keepalive_op stale = {0};
  lc_keepalive_res result = {0};
  lc_acquire_req_init(&acquire);
  acquire.key = "claim";
  acquire.owner = "producer";
  acquire.ttl_seconds = 60;
  OK(lc_acquire(producer, &acquire, &lease, &error));
  stale.lease.fencing_token = lease->fencing_token;
  OK(lc_lease_release(lease, NULL, &error));
  acquire.owner = "consumer";
  OK(lc_acquire(consumer, &acquire, &lease, &error));
  REQUIRE(lease->fencing_token > stale.lease.fencing_token);
  stale.lease.ns = lease->ns;
  stale.lease.key = lease->key;
  stale.lease.lease_id = lease->lease_id;
  stale.lease.txn_id = lease->txn_id;
  stale.ttl_seconds = 60;
  REQUIRE(lc_keepalive(producer, &stale, &result, &error) == LC_ERR_INVALID &&
          error.message &&
          !strcmp(error.message, "pouch lease validation failed"));
  lc_keepalive_res_cleanup(&result);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  OK(lc_lease_release(lease, NULL, &error));
}

static void queue_round_trip(const char *path) {
  static const char body[] = "transactional message";
  lc_client *producer = open_pouch(path, 1);
  lc_client *consumer = open_pouch(path, 0);
  lc_enqueue_req enqueue;
  lc_dequeue_req dequeue;
  lc_enqueue_res published = {0};
  lc_source *source = NULL;
  lc_message *message = NULL;
  lc_txn_decision_req decision = {0};
  lc_txn_decision_res committed = {0};
  lc_txn_replay_req replay = {0};
  lc_txn_replay_res recovered = {0};
  char xid[LC_XID_STRING_SIZE], key[128], chunk[5];
  lc_txn_participant participant = {"journal", key, NULL};
  size_t count, offset = 0;
  lease_round_trip(producer, consumer);
  lc_enqueue_req_init(&enqueue);
  enqueue.queue = "jobs";
  OK(lc_source_from_memory(body, sizeof(body) - 1U, &source, &error));
  OK(lc_enqueue(producer, &enqueue, source, &published, &error));
  lc_source_close(source);
  REQUIRE(published.payload_bytes == (long)sizeof(body) - 1L);
  OK(lc_xid_new(xid, &error));
  lc_dequeue_req_init(&dequeue);
  dequeue.queue = enqueue.queue;
  dequeue.owner = "worker";
  dequeue.txn_id = xid;
  dequeue.visibility_timeout_seconds = 60;
  OK(lc_dequeue(consumer, &dequeue, &message, &error));
  REQUIRE(message && message->txn_id && !strcmp(message->txn_id, xid) &&
          !strcmp(message->message_id, published.message_id));
  source = message->payload_reader(message);
  REQUIRE(source);
  while ((count = source->read(source, chunk, sizeof(chunk), &error)) != 0U) {
    REQUIRE(count <= sizeof(body) - 1U - offset &&
            !memcmp(chunk, body + offset, count));
    offset += count;
  }
  REQUIRE(error.code == LC_OK && offset == sizeof(body) - 1U);
  REQUIRE(snprintf(key, sizeof(key), "q/jobs/msg/%s", message->message_id) <
          (int)sizeof(key));
  OK(lc_message_ack(message, &error));
  decision.txn_id = replay.txn_id = xid;
  decision.participants = &participant;
  decision.participant_count = 1;
  OK(lc_txn_commit(producer, &decision, &committed, &error));
  REQUIRE(committed.state && !strcmp(committed.state, "commit"));
  lc_txn_decision_res_cleanup(&committed);
  OK(lc_txn_replay(consumer, &replay, &recovered, &error));
  REQUIRE(recovered.state && !strcmp(recovered.state, "commit"));
  lc_txn_replay_res_cleanup(&recovered);
  lc_enqueue_res_cleanup(&published);
  lc_client_close(consumer);
  lc_client_close(producer);

  consumer = open_pouch(path, 0);
  lc_queue_stats_req stats = {0};
  lc_queue_stats_res depth = {0};
  stats.queue = enqueue.queue;
  OK(lc_queue_stats(consumer, &stats, &depth, &error));
  REQUIRE(!depth.pending_candidates && !depth.available);
  lc_queue_stats_res_cleanup(&depth);
  dequeue.txn_id = NULL;
  OK(lc_dequeue(consumer, &dequeue, &message, &error));
  REQUIRE(!message);
  lc_pouch *pouch = ((lc_client_handle *)consumer)->pouch;
  uint64_t before = monotonic_ns();
  REQUIRE(lc_pouch_queue_watch_wait(pouch, "journal", "jobs", 20U) == 0 &&
          monotonic_ns() - before >= 10000000U);
  lc_client_close(consumer);
  check_capacity(path, PS_DEFAULT_CAPACITY);
}

int main(int argc, char **argv) {
  lc_error_init(&error);
  if (argc == 4 &&
      (!strcmp(argv[2], "--prepare") || !strcmp(argv[2], "--publish"))) {
    ps_space *space = open_shared(argv[1], 0);
    if (!strcmp(argv[2], "--prepare"))
      prepare_unpublished(space);
    else
      produce(space, "durable successor");
    REQUIRE(write(atoi(argv[3]), "1", 1) == 1);
    for (;;)
      pause();
  }
  if (argc == 3 && (!strcmp(argv[2], "--write-collection") ||
                    !strcmp(argv[2], "--verify-collection"))) {
    collection_round_trip(argv[1], !strcmp(argv[2], "--write-collection"));
    lc_error_cleanup(&error);
    return 0;
  }
  if (argc != 2)
    return 2;
  char directory[1024], path[1100];
  REQUIRE(snprintf(directory, sizeof(directory), "%s/pouch-platform-XXXXXX",
                   argv[1]) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  ps_space *space = open_shared(path, 1);
  check_capacity(path, PS_DEFAULT_CAPACITY);
  uint32_t observed = ps_readiness_sync_observe(space);
  uint64_t before = monotonic_ns();
  REQUIRE(ps_readiness_sync_wait(space, observed) &&
          monotonic_ns() - before >= 2000000U);
  ps_readiness_sync_wake(space);
  REQUIRE(ps_readiness_sync_observe(space) != observed &&
          ps_readiness_sync_wait(space, observed));
  produce(space, "checkpointed predecessor");
  OK(ps_checkpoint(space, &error));
  ps_reader *held = NULL;
  OK(ps_read_open(space, key, NULL, &held, &error));

  kill_publisher(argv[0], path, "--publish");
  OK(ps_refresh(space, &error));
  expect_current(space, "durable successor");
  expect(held, "checkpointed predecessor");
  ps_reader_close(held);
  OK(ps_close(space, &error));

  /* The child is the last owner. Reopen must discard its private payload. */
  kill_publisher(argv[0], path, "--prepare");
  check_failed_growth(path);
  space = open_shared(path, 0);
  expect_current(space, "durable successor");
  ps_space *stale = open_shared(path, 0);
  OK(ps_grow(space, PS_MIB, &error));
  check_capacity(path, PS_DEFAULT_CAPACITY + PS_MIB);
  OK(ps_grow(stale, PS_MIB, &error));
  check_capacity(path, PS_DEFAULT_CAPACITY + 2U * PS_MIB);
  OK(ps_close(stale, &error));
  OK(ps_close(space, &error));

  space = open_shared(path, 0);
  expect_current(space, "durable successor");
  check_capacity(path, PS_DEFAULT_CAPACITY + 2U * PS_MIB);
  OK(ps_close(space, &error));
  REQUIRE(!unlink(path));
  collection_round_trip(path, 1);
  collection_round_trip(path, 0);
  REQUIRE(!unlink(path));
  queue_round_trip(path);
  REQUIRE(!unlink(path) && !rmdir(directory));
  lc_error_cleanup(&error);
  puts("platform recovery, reader pin, growth, collection retry, lease "
       "fencing, and queue commit verified");
  return 0;
}
