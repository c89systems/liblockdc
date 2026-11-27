#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>

#define CHECK(test)                                                            \
  do {                                                                         \
    if (!(test)) {                                                             \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #test);               \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

static const char key_a[] =
    "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
static const char key_b[] =
    "lc-pouch-key-v1:BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
static const char value[] = "\"admitted\"";
static pthread_barrier_t creation_gate;
static const char *race_directory;

int __real_lc_pouch_path_ensure_directory(const char *, const char *,
                                          lc_error *);
int __wrap_lc_pouch_path_ensure_directory(const char *path, const char *message,
                                          lc_error *error) {
  /* Every caller has already observed a missing root when it reaches this
   * call. Release the contenders together before any can publish. */
  if (race_directory && !strcmp(path, race_directory)) {
    int rc = pthread_barrier_wait(&creation_gate);
    CHECK(!rc || rc == PTHREAD_BARRIER_SERIAL_THREAD);
  }
  return __real_lc_pouch_path_ensure_directory(path, message, error);
}

static void ok(int rc, const lc_error *error) {
  if (rc != LC_OK)
    fprintf(stderr, "pouch error %d: %s (%s)\n", rc,
            error->message ? error->message : "",
            error->detail ? error->detail : "");
  CHECK(rc == LC_OK);
}

static int open_pouch(const char *root, const char *key, lc_client **client,
                      lc_error *error) {
  char endpoint[1024];
  const char *endpoints[] = {endpoint};
  lc_client_config config;
  lc_pouch_settings settings;
  int n = snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  CHECK(n > 0 && (size_t)n < sizeof(endpoint));
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_QUERY_INDEXING |
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_CRYPTO_KEY;
  settings.single_writer = 0;
  settings.crypto_key = key;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.pouch_settings = &settings;
  return lc_client_open(&config, client, error);
}

static void write_state(lc_client *client, lc_error *error) {
  lc_acquire_req req;
  lc_lease *lease = NULL;
  lc_source *body = NULL;
  lc_acquire_req_init(&req);
  req.key = "value";
  req.owner = "root-admission-test";
  req.ttl_seconds = 30;
  ok(lc_acquire(client, &req, &lease, error), error);
  ok(lc_source_from_memory(value, sizeof(value) - 1, &body, error), error);
  ok(lc_lease_update(lease, body, NULL, error), error);
  lc_source_close(body);
  ok(lc_lease_release(lease, NULL, error), error);
}

static void read_state(lc_client *client, lc_error *error) {
  lc_get_opts opts;
  lc_get_res res = {0};
  lc_sink *sink = NULL;
  const void *bytes;
  size_t size;
  lc_get_opts_init(&opts);
  opts.public_read = 1;
  ok(lc_sink_to_memory(&sink, error), error);
  ok(lc_get(client, "value", &opts, sink, &res, error), error);
  ok(lc_sink_memory_bytes(sink, &bytes, &size, error), error);
  CHECK(!res.no_content && size == sizeof(value) - 1 &&
        !memcmp(value, bytes, size));
  lc_get_res_cleanup(&res);
  lc_sink_close(sink);
}

static void check_directory(const char *path, int expected) {
  DIR *dir = opendir(path);
  struct dirent *entry;
  int stores = 0;
  CHECK(dir);
  while ((entry = readdir(dir)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    CHECK(!strcmp(entry->d_name, "store"));
    ++stores;
  }
  CHECK(closedir(dir) == 0 && stores == expected);
}

static void retry_failed_creation(const char *root, const char *directory) {
  struct rlimit saved, limit;
  struct sigaction ignore = {0}, old_action;
  lc_client *client = NULL;
  lc_error error = {0};
  int rc;

  CHECK(getrlimit(RLIMIT_FSIZE, &saved) == 0);
  limit = saved;
  limit.rlim_cur = 0;
  ignore.sa_handler = SIG_IGN;
  CHECK(sigemptyset(&ignore.sa_mask) == 0);
  CHECK(sigaction(SIGXFSZ, &ignore, &old_action) == 0);
  CHECK(setrlimit(RLIMIT_FSIZE, &limit) == 0);
  rc = open_pouch(root, key_a, &client, &error);
  CHECK(setrlimit(RLIMIT_FSIZE, &saved) == 0);
  CHECK(sigaction(SIGXFSZ, &old_action, NULL) == 0);
  CHECK(rc == LC_ERR_TRANSPORT && !client);
  lc_error_cleanup(&error);
  check_directory(directory, 0);

  ok(open_pouch(root, key_a, &client, &error), &error);
  write_state(client, &error);
  lc_client_close(client);
  ok(open_pouch(root, key_a, &client, &error), &error);
  read_state(client, &error);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

typedef struct contender {
  const char *root, *key;
  lc_client *client;
  lc_error error;
  int rc;
} contender;

static void *open_contender(void *arg) {
  contender *peer = arg;
  peer->rc = open_pouch(peer->root, peer->key, &peer->client, &peer->error);
  return NULL;
}

static void race_creation(const char *root, const char *directory, int mixed) {
  enum { PEERS = 8 };
  pthread_t threads[PEERS];
  contender peers[PEERS] = {{0}};
  const char *winning_key = NULL;
  size_t i;
  CHECK(pthread_barrier_init(&creation_gate, NULL, PEERS) == 0);
  race_directory = directory;
  for (i = 0; i < PEERS; ++i) {
    peers[i].root = root;
    peers[i].key = mixed && i % 2 ? key_b : key_a;
    CHECK(pthread_create(&threads[i], NULL, open_contender, &peers[i]) == 0);
  }
  for (i = 0; i < PEERS; ++i)
    CHECK(pthread_join(threads[i], NULL) == 0);
  race_directory = NULL;
  CHECK(pthread_barrier_destroy(&creation_gate) == 0);

  for (i = 0; i < PEERS; ++i) {
    if (peers[i].rc == LC_OK) {
      winning_key = peers[i].key;
      write_state(peers[i].client, &peers[i].error);
      break;
    }
  }
  CHECK(winning_key);
  for (i = 0; i < PEERS; ++i) {
    contender *peer = &peers[i];
    if (peer->key == winning_key) {
      ok(peer->rc, &peer->error);
      read_state(peer->client, &peer->error);
    } else {
      CHECK(peer->rc == LC_ERR_INVALID && !peer->client);
      CHECK(peer->error.server_code &&
            !strcmp(peer->error.server_code, "pouch_root_crypto_mismatch"));
    }
    lc_client_close(peer->client);
    lc_error_cleanup(&peer->error);
  }
}

int main(int argc, char **argv) {
  char prefix[768], template[800], directory[800], root[832];
  struct stat info;
  CHECK(argc == 3);
  CHECK(snprintf(prefix, sizeof(prefix), "%s/root-admission-", argv[1]) <
        (int)sizeof(prefix));
  CHECK(snprintf(template, sizeof(template), "%sXXXXXX", prefix) <
        (int)sizeof(template));
  CHECK(lc_test_tmp_mkdtemp(template, directory, sizeof(directory), prefix));
  CHECK(snprintf(root, sizeof(root), "%s/store", directory) <
        (int)sizeof(root));
  if (!strcmp(argv[2], "retry"))
    retry_failed_creation(root, directory);
  else
    race_creation(root, directory, !strcmp(argv[2], "shared-keys"));
  CHECK(lstat(root, &info) == 0 && S_ISREG(info.st_mode));
  check_directory(directory, 1);
  lc_test_tmp_cleanup_path(directory, prefix);
  return 0;
}
