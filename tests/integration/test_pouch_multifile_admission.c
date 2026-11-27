#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(test)                                                            \
  do {                                                                         \
    if (!(test)) {                                                             \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #test);               \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

static lc_error error;

static void ok(int rc) {
  if (rc != LC_OK)
    fprintf(stderr, "pouch error %d: %s (%s)\n", rc,
            error.message ? error.message : "",
            error.detail ? error.detail : "");
  CHECK(rc == LC_OK);
}

static lc_client *open_receiver(const char *scheme, const char *root,
                                const char *compression) {
  char endpoint[1024];
  const char *endpoints[1];
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *client = NULL;
  int n = snprintf(endpoint, sizeof(endpoint), "%s://%s", scheme, root);
  CHECK(n > 0 && (size_t)n < sizeof(endpoint));
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_BACKGROUND_COMPACTION |
                      LC_POUCH_SETTING_QUERY_INDEXING |
                      LC_POUCH_SETTING_CRYPTO_KEY;
  settings.crypto_key =
      "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  if (compression) {
    settings.set_mask |= LC_POUCH_SETTING_COMPRESSION;
    settings.compression = compression;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.pouch_settings = &settings;
#ifdef LC_POUCH_MULTIFILE_FIXTURE
  {
    extern int lc_pouch_legacy_client_open(const lc_client_config *,
                                           lc_client **, lc_error *);
    ok(lc_pouch_legacy_client_open(&config, &client, &error));
  }
#else
  if (lc_client_open(&config, &client, &error) != LC_OK)
    CHECK(client == NULL);
#endif
  return client;
}

static void write_state(lc_client *client, const char *value) {
  lc_acquire_req request;
  lc_lease *lease = NULL;
  lc_source *body = NULL;
  lc_acquire_req_init(&request);
  request.key = "value";
  request.owner = "multifile-admission";
  request.ttl_seconds = 30;
  ok(lc_acquire(client, &request, &lease, &error));
  ok(lc_source_from_memory(value, strlen(value), &body, &error));
  ok(lc_lease_update(lease, body, NULL, &error));
  lc_source_close(body);
  ok(lc_lease_release(lease, NULL, &error));
}

static void seed(lc_client *client) {
  lc_collection_config config;
  lc_collection *journal = NULL;
  lc_collection_append_req request = {0};
  lc_collection_append_res res = {0};
  lc_history_consumer_config history_config;
  lc_history_consumer *consumer = NULL;
  lc_source *body = NULL;
  write_state(client, "\"legacy value\"");
  lc_history_consumer_config_init(&history_config);
  history_config.ns = "default";
  history_config.consumer_id = "receiver-probe";
  history_config.initial_acknowledged_index_seq = 1;
  ok(lc_client_new_history_consumer(client, &history_config, &consumer,
                                    &error));
  lc_history_consumer_close(consumer);
  lc_collection_config_init(&config);
  config.name = "journal";
  ok(lc_client_create_collection(client, &config, &journal, &error));
  request.append_id = "first";
  ok(lc_source_from_memory("{}", 2, &body, &error));
  ok(lc_collection_append(journal, &request, body, &res, &error));
  CHECK(res.position == 1 && !res.duplicate);
  lc_source_close(body);
  lc_collection_close(journal);
}

#ifndef LC_POUCH_MULTIFILE_FIXTURE
static void check_state(lc_client *client, const char *expected) {
  lc_get_opts opts;
  lc_get_res res = {0};
  lc_sink *sink = NULL;
  const void *value;
  size_t length;
  lc_get_opts_init(&opts);
  opts.public_read = 1;
  ok(lc_sink_to_memory(&sink, &error));
  ok(lc_get(client, "value", &opts, sink, &res, &error));
  ok(lc_sink_memory_bytes(sink, &value, &length, &error));
  CHECK(!res.no_content && length == strlen(expected) &&
        !memcmp(value, expected, length));
  lc_get_res_cleanup(&res);
  lc_sink_close(sink);
}

static void check_collection(lc_client *client) {
  lc_collection_config config;
  lc_collection *journal = NULL;
  lc_collection_head_res head = {0};
  lc_collection_config_init(&config);
  config.name = "journal";
  ok(lc_client_open_collection(client, &config, &journal, &error));
  ok(lc_collection_head(journal, &head, &error));
  CHECK(head.live_count == 1 && head.upper_position == 1);
  lc_collection_close(journal);
}

static void check_history(lc_client *client) {
  lc_history_consumer_config config;
  lc_history_consumer_position position;
  lc_history_consumer *consumer = NULL;
  lc_history_consumer_config_init(&config);
  config.ns = "default";
  config.consumer_id = "receiver-probe";
  ok(lc_client_new_history_consumer(client, &config, &consumer, &error));
  ok(lc_history_consumer_get_position(consumer, &position, &error));
  CHECK(position.acknowledged_index_seq == 1 && position.current_index_seq > 1);
  ok(lc_history_consumer_advance(consumer, position.current_index_seq,
                                 &position, &error));
  CHECK(position.acknowledged_index_seq == position.current_index_seq);
  ok(lc_history_consumer_unregister(consumer, &error));
  lc_history_consumer_close(consumer);
}

static void seed_with_fixture(const char *fixture, const char *root) {
  int status;
  pid_t child = fork();
  CHECK(child >= 0);
  if (!child) {
    execl(fixture, fixture, root, (char *)NULL);
    _exit(127);
  }
  CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
        WEXITSTATUS(status) == 0);
}
#endif

int main(int argc, char **argv) {
  lc_client *client;
  lc_error_init(&error);
#ifdef LC_POUCH_MULTIFILE_FIXTURE
  CHECK(argc == 2);
  client = open_receiver("pouch", argv[1], NULL);
  CHECK(client != NULL);
  seed(client);
  lc_client_close(client);
#else
  {
    char prefix[768], template_path[800], dir[800], root[832];
    struct stat info;
    CHECK(argc == 4);
    CHECK(snprintf(prefix, sizeof(prefix), "%s/multifile-admission-", argv[1]) <
          (int)sizeof(prefix));
    CHECK(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) <
          (int)sizeof(template_path));
    CHECK(lc_test_tmp_mkdtemp(template_path, dir, sizeof(dir), prefix));
    CHECK(snprintf(root, sizeof(root), "%s/store", dir) < (int)sizeof(root));
    client = open_receiver("pouch+multifile", root, NULL);
    if (!strcmp(argv[3], "1")) {
      ok(error.code);
      CHECK(client && lstat(root, &info) == 0 && S_ISDIR(info.st_mode));
      seed(client);
      lc_client_close(client);
      client = open_receiver("pouch+multifile", root, NULL);
      ok(error.code);
      CHECK(client && lstat(root, &info) == 0 && S_ISDIR(info.st_mode));
      check_state(client, "\"legacy value\"");
      lc_client_close(client);
    } else {
      CHECK(!client && error.code == LC_ERR_UNSUPPORTED && error.server_code &&
            !strcmp(error.server_code, "pouch_multifile_disabled"));
      CHECK(lstat(root, &info) != 0 && errno == ENOENT);
      lc_error_cleanup(&error);
      seed_with_fixture(argv[2], root);
      CHECK(lstat(root, &info) == 0 && S_ISDIR(info.st_mode));
      CHECK(!open_receiver("pouch+multifile", root, NULL) &&
            error.code == LC_ERR_UNSUPPORTED);
      CHECK(lstat(root, &info) == 0 && S_ISDIR(info.st_mode));
      lc_error_cleanup(&error);
    }
    client = open_receiver("pouch", root, "zlib");
    ok(error.code);
    CHECK(client && lstat(root, &info) == 0 && S_ISREG(info.st_mode));
    check_state(client, "\"legacy value\"");
    check_collection(client);
    check_history(client);
    write_state(client, "\"single-file value\"");
    lc_client_close(client);
    client = open_receiver("pouch", root, NULL);
    ok(error.code);
    CHECK(client != NULL);
    check_state(client, "\"single-file value\"");
    lc_client_close(client);
    lc_test_tmp_cleanup_path(dir, prefix);
  }
#endif
  lc_error_cleanup(&error);
  return 0;
}
