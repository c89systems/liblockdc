#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE 1
#endif
#include "pouch.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
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

static void publish(ps_space *space, const char *bytes) {
  ps_writer *writer = NULL;
  OK(ps_begin(space, NULL, key, &writer, &error));
  OK(ps_write(writer, bytes, strlen(bytes), &error));
  OK(ps_publish(writer, NULL, &error));
  ps_writer_close(writer);
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

static void collection_round_trip(const char *path, int create) {
  static const char body[] = "{\"reading\":1}";
  char endpoint[1250];
  const char *endpoints[] = {endpoint};
  lc_client *client = NULL;
  lc_collection *collection = NULL;
  lc_source *source = NULL;
  lc_sink *sink = NULL;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config collection_config;
  lc_collection_append_req request;
  lc_collection_append_res appended;
  lc_collection_read_res result;
  const void *read;
  size_t count;
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?background_compaction=false&"
                   "query_indexing=false&queue_watch=false",
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

int main(int argc, char **argv) {
  lc_error_init(&error);
  if (argc == 4 && !strcmp(argv[2], "--publish")) {
    ps_space *space = open_shared(argv[1], 0);
    publish(space, "durable successor");
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
  char directory[1024], path[1100], ack_fd[32], ack;
  REQUIRE(snprintf(directory, sizeof(directory), "%s/pouch-platform-XXXXXX",
                   argv[1]) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  ps_space *space = open_shared(path, 1);
  check_capacity(path, PS_DEFAULT_CAPACITY);
  publish(space, "checkpointed predecessor");
  OK(ps_checkpoint(space, &error));
  ps_reader *held = NULL;
  OK(ps_read_open(space, key, NULL, &held, &error));

  int pipefd[2], status;
  REQUIRE(pipe(pipefd) == 0);
  REQUIRE(snprintf(ack_fd, sizeof(ack_fd), "%d", pipefd[1]) <
          (int)sizeof(ack_fd));
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    (void)close(pipefd[0]);
    execl(argv[0], argv[0], path, "--publish", ack_fd, (char *)NULL);
    _exit(127);
  }
  REQUIRE(!close(pipefd[1]) && read(pipefd[0], &ack, 1) == 1 && ack == '1');
  REQUIRE(!close(pipefd[0]) && !kill(child, SIGKILL) &&
          waitpid(child, &status, 0) == child && WIFSIGNALED(status) &&
          WTERMSIG(status) == SIGKILL);

  OK(ps_refresh(space, &error));
  expect_current(space, "durable successor");
  expect(held, "checkpointed predecessor");
  ps_reader_close(held);
  OK(ps_close(space, &error));
  space = open_shared(path, 0);
  expect_current(space, "durable successor");
  OK(ps_grow(space, PS_MIB, &error));
  check_capacity(path, PS_DEFAULT_CAPACITY + PS_MIB);
  OK(ps_close(space, &error));

  space = open_shared(path, 0);
  expect_current(space, "durable successor");
  check_capacity(path, PS_DEFAULT_CAPACITY + PS_MIB);
  OK(ps_close(space, &error));
  REQUIRE(!unlink(path));
  collection_round_trip(path, 1);
  collection_round_trip(path, 0);
  REQUIRE(!unlink(path) && !rmdir(directory));
  lc_error_cleanup(&error);
  puts("platform recovery, reader pin, growth, and collection retry verified");
  return 0;
}
