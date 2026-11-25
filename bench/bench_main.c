#include "../tests/support/lc_test_tmp.h"
#include "bench_collections.h"
#include "bench_pouch.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch.h"

#include <errno.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define BENCH_POUCH_TMP_PREFIX "/tmp/liblockdc-pouch-bench-"
#define BENCH_OUTBOX_MAX_DISPATCHERS 16U
#define BENCH_OUTBOX_DISPATCHER_IDLE_LIMIT 24
#define BENCH_OUTBOX_COMPACT_SEGMENT_TARGET_BYTES (64L * 1024L)

typedef struct bench_case {
  const char *name;
  long default_iterations;
  int (*run)(long iterations);
} bench_case;

typedef struct bench_query_key_count {
  size_t rows;
} bench_query_key_count;

typedef struct bench_pouch_query_case {
  const char *selector_json;
  const char *selector_lql;
  const char *engine;
  int documents;
} bench_pouch_query_case;

typedef struct bench_pouch_perf_fixture {
  lc_client *client;
  char *crypto_key;
  char root[512];
} bench_pouch_perf_fixture;

typedef struct bench_outbox_fixture {
  lc_client *client;
  char root[512];
  char ns[128];
  char key_prefix[96];
  long pouch_segment_target_bytes;
  int is_pouch;
} bench_outbox_fixture;

typedef struct bench_outbox_maintenance_stats {
  double seconds;
  unsigned long candidate_segments;
  uint64_t candidate_bytes;
  int compacted;
} bench_outbox_maintenance_stats;

typedef struct bench_outbox_dispatcher_result {
  unsigned long delivered;
  double first_delivery_seconds;
  double last_delivery_seconds;
  int ready;
  int rc;
  int failure_stage;
  char job_key[160];
  char error_message[160];
} bench_outbox_dispatcher_result;

static double bench_now_seconds(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0.0;
  }
  return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

static int bench_payload_digest(const char *payload, size_t payload_length,
                                char output[72]) {
  static const char hex[] = "0123456789abcdef";
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_length;
  size_t index;

  if ((payload == NULL && payload_length != 0U) ||
      EVP_Digest(payload == NULL ? "" : payload, payload_length, digest,
                 &digest_length, EVP_sha256(), NULL) != 1 ||
      digest_length != 32U) {
    return 1;
  }
  memcpy(output, "sha256:", sizeof("sha256:") - 1U);
  for (index = 0U; index < digest_length; ++index) {
    output[sizeof("sha256:") - 1U + index * 2U] = hex[digest[index] >> 4U];
    output[sizeof("sha256:") + index * 2U] = hex[digest[index] & 0x0fU];
  }
  output[71] = '\0';
  return 0;
}

static int bench_stream_copy(long iterations) {
  static const char payload[] =
      "{\"key\":\"orders/42\",\"state\":{\"items\":[1,2,3]}}";
  lc_error error;
  long i;

  lc_error_init(&error);
  for (i = 0; i < iterations; ++i) {
    lc_source *source;
    lc_sink *sink;
    size_t written;

    source = NULL;
    sink = NULL;
    written = 0U;
    if (lc_source_from_memory(payload, sizeof(payload) - 1U, &source, &error) !=
        LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    if (lc_sink_to_memory(&sink, &error) != LC_OK) {
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    if (lc_copy(source, sink, &written, &error) != LC_OK) {
      lc_sink_close(sink);
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    lc_sink_close(sink);
    lc_source_close(source);
  }
  lc_error_cleanup(&error);
  return 0;
}

static int bench_pouch_tmp_prefix(char *prefix, size_t size) {
  const char *directory = getenv("LOCKDC_BENCH_DIRECTORY");
  int written;
  if (!directory)
    directory = "/tmp";
  if (directory[0] != '/')
    return 0;
  written = snprintf(prefix, size, "%s/liblockdc-pouch-bench-", directory);
  return written >= 0 && (size_t)written < size;
}

int bench_pouch_root_path(char *buffer, size_t buffer_size,
                          const char *suffix) {
  char template_path[512];
  char prefix[512];
  int written;

  if (!bench_pouch_tmp_prefix(prefix, sizeof(prefix)))
    return 1;
  written = snprintf(template_path, sizeof(template_path), "%s%s-XXXXXX",
                     prefix, suffix);
  if (written < 0 || (size_t)written >= sizeof(template_path)) {
    return 1;
  }
  return lc_test_tmp_mkdtemp(template_path, buffer, buffer_size, prefix) ? 0
                                                                         : 1;
}

void bench_pouch_cleanup_root(const char *root) {
  char prefix[512];
  if (bench_pouch_tmp_prefix(prefix, sizeof(prefix)))
    lc_test_tmp_cleanup_path(root, prefix);
}

static int bench_query_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int bench_query_key_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int bench_query_key_end(void *context, lc_error *error) {
  bench_query_key_count *count;

  (void)error;
  count = (bench_query_key_count *)context;
  count->rows++;
  return 1;
}

static int bench_pouch_client_open(const char *root, lc_client **out,
                                   lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[640];

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  return bench_open_pouch(&config, 1, 1, out, error);
}

static int bench_env_enabled(const char *name) {
  const char *value;

  value = getenv(name);
  return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0 &&
         strcmp(value, "false") != 0 && strcmp(value, "FALSE") != 0;
}

static long bench_env_long(const char *name, long fallback, long minimum) {
  const char *value;
  char *end;
  long parsed;

  value = getenv(name);
  if (value == NULL || value[0] == '\0') {
    return fallback;
  }
  end = NULL;
  parsed = strtol(value, &end, 10);
  if (end == value || *end != '\0' || parsed < minimum) {
    return fallback;
  }
  return parsed;
}

static int bench_pouch_client_open_crypto(const char *root,
                                          const char *crypto_key, int create,
                                          lc_client **out, lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[640];

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_crypto_key = crypto_key;
  return bench_open_pouch(&config, create, 1, out, error);
}

static int bench_pouch_perf_fixture_open(bench_pouch_perf_fixture *fixture,
                                         lc_error *error) {
  int rc;

  if (fixture == NULL) {
    return 1;
  }
  memset(fixture, 0, sizeof(*fixture));
  if (bench_pouch_root_path(fixture->root, sizeof(fixture->root), "perf") !=
      0) {
    return 1;
  }
  if (bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO")) {
    rc = lc_pouch_crypto_generate_key_string(&fixture->crypto_key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return bench_pouch_client_open_crypto(fixture->root, fixture->crypto_key, 1,
                                        &fixture->client, error);
}

static void bench_pouch_perf_fixture_close(bench_pouch_perf_fixture *fixture) {
  if (fixture == NULL) {
    return;
  }
  if (fixture->client != NULL) {
    fixture->client->close(fixture->client);
  }
  lc_pouch_crypto_key_string_free(fixture->crypto_key);
  bench_pouch_cleanup_root(fixture->root);
  memset(fixture, 0, sizeof(*fixture));
}

static int bench_outbox_remote_client_open(const char *endpoint,
                                           const char *failover_endpoint,
                                           const char *bundle_path,
                                           const char *ns, lc_client **out,
                                           lc_error *error) {
  const char *endpoints[2];
  lc_client_config config;
  lc_source *bundle_source;
  int rc;

  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  if (rc != LC_OK) {
    return rc;
  }
  endpoints[0] = endpoint;
  endpoints[1] = failover_endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = failover_endpoint == NULL ? 1U : 2U;
  config.default_namespace = ns;
  config.timeout_ms = 30000L;
  config.client_bundle_source = bundle_source;
  config.insecure_skip_verify = 1;
  rc = lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
  return rc;
}

static int bench_outbox_flush_index(lc_client *client, const char *ns,
                                    const char *mode, lc_error *error) {
  lc_index_flush_req request;
  lc_index_flush_res result;
  int rc;

  lc_index_flush_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.ns = ns;
  request.mode = mode;
  rc = lc_flush_index(client, &request, &result, error);
  lc_index_flush_res_cleanup(&result);
  return rc;
}

static int bench_outbox_remote_probe(lc_client *client, const char *ns,
                                     lc_error *error) {
  return bench_outbox_flush_index(client, ns, "wait", error);
}

static int bench_outbox_fixture_client_open(const bench_outbox_fixture *fixture,
                                            int pouch_shared_writer, int create,
                                            lc_client **out, lc_error *error) {
  const char *endpoint;
  const char *failover_endpoint;
  const char *bundle_path;
  const char *endpoints[1];
  char pouch_endpoint[sizeof(fixture->root) + 96U];
  lc_client_config config;

  if (fixture == NULL || out == NULL) {
    return 1;
  }
  if (fixture->is_pouch) {
    long segment_target = fixture->pouch_segment_target_bytes;
    if (pouch_shared_writer || segment_target > 0L) {
      int written;

      if (pouch_shared_writer && segment_target > 0L) {
        written = snprintf(pouch_endpoint, sizeof(pouch_endpoint),
                           "%s?single_writer=false&segment_target_bytes=%ld",
                           fixture->root, segment_target);
      } else if (pouch_shared_writer) {
        written = snprintf(pouch_endpoint, sizeof(pouch_endpoint),
                           "%s?single_writer=false", fixture->root);
      } else {
        written = snprintf(pouch_endpoint, sizeof(pouch_endpoint),
                           "%s?segment_target_bytes=%ld", fixture->root,
                           segment_target);
      }
      if (written < 0 || (size_t)written >= sizeof(pouch_endpoint)) {
        return 1;
      }
      endpoints[0] = pouch_endpoint;
    } else {
      endpoints[0] = fixture->root;
    }
    lc_client_config_init(&config);
    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.default_namespace = fixture->ns;
    config.timeout_ms = 30000L;
    return bench_open_pouch(&config, create, 1, out, error);
  }
  endpoint = getenv("LOCKDC_OUTBOX_BENCH_ENDPOINT");
  if (endpoint == NULL || endpoint[0] == '\0') {
    return 1;
  }
  failover_endpoint = getenv("LOCKDC_OUTBOX_BENCH_FAILOVER_ENDPOINT");
  if (failover_endpoint != NULL && failover_endpoint[0] == '\0') {
    failover_endpoint = NULL;
  }
  bundle_path = getenv("LOCKDC_OUTBOX_BENCH_CLIENT_BUNDLE");
  if (bundle_path == NULL || bundle_path[0] == '\0') {
    bundle_path = "./build/devenv/credentials/disk/client.pem";
  }
  return bench_outbox_remote_client_open(endpoint, failover_endpoint,
                                         bundle_path, fixture->ns, out, error);
}

static int bench_outbox_fixture_compact(bench_outbox_fixture *fixture,
                                        bench_outbox_maintenance_stats *stats,
                                        lc_error *error) {
  lc_client_handle *client;
  lc_pouch_maintenance_options options;
  lc_pouch_maintenance_result result;
  double started;
  int rc;

  if (fixture == NULL || fixture->client == NULL || stats == NULL) {
    return LC_ERR_INVALID;
  }
  client = (lc_client_handle *)fixture->client;
  if (!fixture->is_pouch || !client->is_pouch || client->pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox compaction benchmark requires Pouch", NULL,
                        NULL, NULL);
  }
  memset(stats, 0, sizeof(*stats));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.ns = fixture->ns;
  options.force = 1;
  started = bench_now_seconds();
  rc = lc_pouch_maintenance_run(client->pouch, &options, &result, error);
  stats->seconds = bench_now_seconds() - started;
  stats->candidate_segments = result.candidate_segment_count;
  stats->candidate_bytes = result.candidate_bytes;
  stats->compacted = result.compacted;
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  if (rc == LC_OK && !stats->compacted) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox compaction benchmark produced no snapshot", NULL,
                      NULL, NULL);
  }
  return rc;
}

static int bench_outbox_fixture_open(bench_outbox_fixture *fixture,
                                     int pouch_shared_writer,
                                     long pouch_segment_target_bytes,
                                     lc_error *error) {
  const char *endpoint;
  const char *failover_endpoint;
  const char *bundle_path;
  const char *ns;
  const char *endpoints[1];
  char root_path[sizeof(fixture->root)];
  lc_client_config config;
  struct timespec now;
  const char *selected_endpoint;
  const char *selected_failover_endpoint;
  int attempt;
  int rc;

  if (fixture == NULL) {
    return 1;
  }
  memset(fixture, 0, sizeof(*fixture));
  fixture->pouch_segment_target_bytes = pouch_segment_target_bytes;
  endpoints[0] = NULL;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    return 1;
  }
  if (snprintf(fixture->key_prefix, sizeof(fixture->key_prefix),
               "__lockdc_io/v1/outbox/bench-%ld-%ld", (long)getpid(),
               (long)now.tv_nsec) < 0) {
    return 1;
  }
  endpoint = getenv("LOCKDC_OUTBOX_BENCH_ENDPOINT");
  failover_endpoint = getenv("LOCKDC_OUTBOX_BENCH_FAILOVER_ENDPOINT");
  ns = getenv("LOCKDC_OUTBOX_BENCH_NAMESPACE");
  lc_client_config_init(&config);
  config.timeout_ms = 30000L;
  if (endpoint == NULL || endpoint[0] == '\0') {
    if (snprintf(fixture->ns, sizeof(fixture->ns), "outbox-bench-%ld-%ld",
                 (long)getpid(), (long)now.tv_nsec) < 0) {
      return 1;
    }
    if (bench_pouch_root_path(fixture->root, sizeof(fixture->root), "outbox") !=
        0) {
      return 1;
    }
    memcpy(root_path, fixture->root, sizeof(root_path));
    if (snprintf(fixture->root, sizeof(fixture->root), "pouch://%s",
                 root_path) < 0) {
      return 1;
    }
    endpoints[0] = fixture->root;
    fixture->is_pouch = 1;
  } else {
    if (ns == NULL || ns[0] == '\0') {
      ns = "default";
    }
    if (snprintf(fixture->ns, sizeof(fixture->ns), "%s", ns) < 0) {
      return 1;
    }
    bundle_path = getenv("LOCKDC_OUTBOX_BENCH_CLIENT_BUNDLE");
    if (bundle_path == NULL || bundle_path[0] == '\0') {
      bundle_path = "./build/devenv/credentials/disk/client.pem";
    }
    selected_endpoint = NULL;
    selected_failover_endpoint = NULL;
    for (attempt = 0; attempt < 30; ++attempt) {
      rc = bench_outbox_remote_client_open(
          endpoint, NULL, bundle_path, fixture->ns, &fixture->client, error);
      if (rc == LC_OK) {
        rc = bench_outbox_remote_probe(fixture->client, fixture->ns, error);
        if (rc == LC_OK) {
          selected_endpoint = endpoint;
          selected_failover_endpoint = failover_endpoint;
          fixture->client->close(fixture->client);
          fixture->client = NULL;
          break;
        }
        fixture->client->close(fixture->client);
        fixture->client = NULL;
      }
      if (failover_endpoint != NULL && failover_endpoint[0] != '\0') {
        lc_error_cleanup(error);
        lc_error_init(error);
        rc = bench_outbox_remote_client_open(failover_endpoint, NULL,
                                             bundle_path, fixture->ns,
                                             &fixture->client, error);
        if (rc == LC_OK) {
          rc = bench_outbox_remote_probe(fixture->client, fixture->ns, error);
          if (rc == LC_OK) {
            selected_endpoint = failover_endpoint;
            selected_failover_endpoint = endpoint;
            fixture->client->close(fixture->client);
            fixture->client = NULL;
            break;
          }
          fixture->client->close(fixture->client);
          fixture->client = NULL;
        }
      }
      if (attempt + 1 < 30) {
        lc_error_cleanup(error);
        lc_error_init(error);
        sleep(1U);
      }
    }
    if (selected_endpoint == NULL) {
      return rc;
    }
    return bench_outbox_remote_client_open(
        selected_endpoint, selected_failover_endpoint, bundle_path, fixture->ns,
        &fixture->client, error);
  }
  if (fixture->is_pouch) {
    return bench_outbox_fixture_client_open(fixture, pouch_shared_writer, 1,
                                            &fixture->client, error);
  }
  config.default_namespace = fixture->ns;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  return lc_client_open(&config, &fixture->client, error);
}

static void bench_outbox_fixture_close(bench_outbox_fixture *fixture) {
  if (fixture == NULL) {
    return;
  }
  if (fixture->client != NULL) {
    fixture->client->close(fixture->client);
  }
  if (fixture->is_pouch) {
    const char *root = fixture->root + strlen("pouch://");

    bench_pouch_cleanup_root(root);
  }
  memset(fixture, 0, sizeof(*fixture));
}

static int bench_outbox_fixture_reopen(bench_outbox_fixture *fixture,
                                       lc_error *error) {
  if (fixture == NULL) {
    return 1;
  }
  if (fixture->client != NULL) {
    fixture->client->close(fixture->client);
    fixture->client = NULL;
  }
  return bench_outbox_fixture_client_open(fixture, 0, 0, &fixture->client,
                                          error);
}

static int bench_outbox_seed_record(lc_client *client, const char *ns,
                                    const char *key, const char *dispatch_state,
                                    long generation, const char *payload,
                                    size_t payload_length, lc_error *error) {
  char state[512];
  char payload_digest[72];
  lc_acquire_req acquire;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  int rc;

  if (bench_payload_digest(payload, payload_length, payload_digest) != 0 ||
      snprintf(state, sizeof(state),
               "{\"record_type\":\"lockdc.outbox.v1\","
               "\"operation_id\":\"outbox-bench-%s-%ld\","
               "\"effect_id\":\"outbox-bench-effect-%s-%ld\","
               "\"effect_key\":\"outbox-bench-key-%s-%ld\","
               "\"payload_digest\":\"%s\","
               "\"message_id\":\"outbox-bench-message-%s-%ld\","
               "\"kind\":\"benchmark\","
               "\"destination\":\"benchmark://reconcile\","
               "\"content_type\":\"application/octet-stream\","
               "\"dispatch_state\":\"%s\",\"attempt_count\":0,"
               "\"not_before_unix\":0,\"benchmark_generation\":%ld}",
               dispatch_state, generation, dispatch_state, generation,
               dispatch_state, generation, payload_digest, dispatch_state,
               generation, dispatch_state, generation) < 0) {
    return 1;
  }
  lease = NULL;
  state_source = NULL;
  payload_source = NULL;
  memset(&attach_result, 0, sizeof(attach_result));
  lc_acquire_req_init(&acquire);
  acquire.ns = ns;
  acquire.key = key;
  acquire.owner = "outbox-benchmark-seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, error);
  if (rc == LC_OK) {
    rc = lc_source_from_memory(state, strlen(state), &state_source, error);
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, state_source, NULL, error);
  }
  if (rc == LC_OK && payload != NULL && payload_length > 0U) {
    rc = lc_source_from_memory(payload, payload_length, &payload_source, error);
  }
  if (rc == LC_OK && payload_source != NULL) {
    lc_attach_req_init(&attach);
    attach.name = "payload";
    attach.content_type = "application/octet-stream";
    attach.prevent_overwrite = 1;
    rc = lease->attach(lease, &attach, payload_source, &attach_result, error);
  }
  if (rc == LC_OK) {
    rc = lease->release(lease, NULL, error);
    if (rc == LC_OK) {
      lease = NULL;
    }
  }
  lc_attach_res_cleanup(&attach_result);
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  if (state_source != NULL) {
    lc_source_close(state_source);
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int bench_outbox_reconcile_case(long iterations, int index_mode,
                                       int with_state) {
  bench_outbox_fixture fixture;
  lc_outbox_config config;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_stats stats;
  lc_outbox_job *job;
  lc_lease *state_lease;
  lc_source *checkpoint;
  char *checkpoint_payload;
  lc_error error;
  struct timespec started;
  char maintenance_bytes[32];
  double first_delivery_seconds;
  double drain_seconds;
  double next_seconds;
  double checkpoint_seconds;
  double completion_seconds;
  long rows;
  long terminal_rows;
  long churn_updates;
  long payload_bytes;
  long page_capacity;
  long pouch_segment_target_bytes;
  long dead_letter_every;
  long dead_letter_max_count;
  long dead_letter_max_bytes;
  long checkpoint_bytes;
  unsigned long effective_dead_letter_max_count;
  unsigned long dead_lettered;
  unsigned long expected_dead_letter_reclaims;
  bench_outbox_maintenance_stats maintenance;
  uint64_t expected_recovery_queries;
  char *payload;
  size_t index;
  int rc;

  rows = iterations > 0L ? iterations : 256L;
  terminal_rows =
      bench_env_long("LOCKDC_OUTBOX_BENCH_TERMINAL_ROWS", rows * 4L, 0L);
  churn_updates = bench_env_long("LOCKDC_OUTBOX_BENCH_CHURN_UPDATES", 4L, 0L);
  payload_bytes =
      bench_env_long("LOCKDC_OUTBOX_BENCH_PAYLOAD_BYTES", 4096L, 0L);
  page_capacity = bench_env_long("LOCKDC_OUTBOX_BENCH_PAGE_CAPACITY", 16L, 1L);
  dead_letter_every =
      bench_env_long("LOCKDC_OUTBOX_BENCH_DEAD_LETTER_EVERY", 0L, 0L);
  dead_letter_max_count =
      bench_env_long("LOCKDC_OUTBOX_BENCH_DEAD_LETTER_MAX_COUNT", 0L, 0L);
  dead_letter_max_bytes =
      bench_env_long("LOCKDC_OUTBOX_BENCH_DEAD_LETTER_MAX_BYTES", 0L, 0L);
  checkpoint_bytes =
      bench_env_long("LOCKDC_OUTBOX_BENCH_CHECKPOINT_BYTES", 0L, 0L);
  effective_dead_letter_max_count = dead_letter_max_count == 0L
                                        ? 1024UL
                                        : (unsigned long)dead_letter_max_count;
  pouch_segment_target_bytes = bench_env_long(
      "LOCKDC_OUTBOX_BENCH_SEGMENT_TARGET_BYTES",
      index_mode == 3 ? BENCH_OUTBOX_COMPACT_SEGMENT_TARGET_BYTES : 0L, 0L);
  expected_recovery_queries =
      ((uint64_t)rows + (uint64_t)page_capacity - 1U) / (uint64_t)page_capacity;
  payload = NULL;
  checkpoint_payload = NULL;
  outbox = NULL;
  dispatcher = NULL;
  memset(&fixture, 0, sizeof(fixture));
  memset(&stats, 0, sizeof(stats));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(maintenance_bytes, 0, sizeof(maintenance_bytes));
  lc_error_init(&error);
  rc = bench_outbox_fixture_open(&fixture, 0, pouch_segment_target_bytes,
                                 &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (payload_bytes > 0L) {
    payload = (char *)malloc((size_t)payload_bytes);
    if (payload == NULL) {
      rc = 1;
      goto done;
    }
    memset(payload, 'p', (size_t)payload_bytes);
  }
  if (with_state && checkpoint_bytes > 0L) {
    checkpoint_payload = (char *)malloc((size_t)checkpoint_bytes);
    if (checkpoint_payload == NULL) {
      rc = 1;
      goto done;
    }
    memset(checkpoint_payload, 'c', (size_t)checkpoint_bytes);
  }
  for (index = 0U; rc == LC_OK && index < (size_t)terminal_rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = 1;
      break;
    }
    rc = bench_outbox_seed_record(fixture.client, fixture.ns, key, "completed",
                                  0L, NULL, 0U, &error);
  }
  for (index = 0U; rc == LC_OK && index < (size_t)churn_updates; ++index) {
    size_t terminal_index;

    for (terminal_index = 0U; terminal_index < (size_t)terminal_rows;
         ++terminal_index) {
      char key[128];

      if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                   (unsigned long)terminal_index) < 0) {
        rc = 1;
        break;
      }
      rc =
          bench_outbox_seed_record(fixture.client, fixture.ns, key, "completed",
                                   (long)index + 1L, NULL, 0U, &error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  for (index = 0U; rc == LC_OK && index < (size_t)rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/pending-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = 1;
      break;
    }
    rc = bench_outbox_seed_record(fixture.client, fixture.ns, key, "pending",
                                  0L, payload, (size_t)payload_bytes, &error);
  }
  if (rc != LC_OK) {
    goto done;
  }
  if (index_mode != 0) {
    rc = bench_outbox_flush_index(fixture.client, fixture.ns, "sync", &error);
    if (rc != LC_OK) {
      goto done;
    }
    if (index_mode == 3) {
      rc = bench_outbox_fixture_compact(&fixture, &maintenance, &error);
      if (rc != LC_OK) {
        goto done;
      }
    }
    if (index_mode >= 2) {
      rc = bench_outbox_fixture_reopen(&fixture, &error);
      if (rc != LC_OK) {
        goto done;
      }
    }
  }
  lc_outbox_config_init(&config);
  config.ns = fixture.ns;
  config.owner = "outbox-benchmark-dispatcher";
  config.notification_capacity = (size_t)page_capacity;
  config.recovery_interval_seconds = 0L;
  config.dead_letter_max_count = (size_t)dead_letter_max_count;
  config.dead_letter_max_bytes = (uint64_t)dead_letter_max_bytes;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    rc = 1;
    goto done;
  }
  rc = lc_client_new_outbox(fixture.client, &config, &outbox, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
  }
  first_delivery_seconds = 0.0;
  next_seconds = 0.0;
  checkpoint_seconds = 0.0;
  completion_seconds = 0.0;
  dead_lettered = 0UL;
  for (index = 0U; rc == LC_OK && index < (size_t)rows; ++index) {
    job = NULL;
    state_lease = NULL;
    checkpoint = NULL;
    {
      double operation_started = bench_now_seconds();

      rc = with_state
               ? dispatcher->next_with_state(dispatcher, 30000L, &job, &error)
               : dispatcher->next(dispatcher, 30000L, &job, &error);
      next_seconds += bench_now_seconds() - operation_started;
    }
    if (rc != LC_OK || job == NULL) {
      if (rc == LC_OK) {
        rc = 1;
      }
      break;
    }
    if (job->outbox_key == NULL || strncmp(job->outbox_key, fixture.key_prefix,
                                           strlen(fixture.key_prefix)) != 0) {
      job->close(job);
      rc = 1;
      break;
    }
    if (index == 0U) {
      first_delivery_seconds =
          bench_now_seconds() -
          ((double)started.tv_sec + (double)started.tv_nsec / 1000000000.0);
    }
    if (with_state) {
      lc_attach_req checkpoint_attachment_request;
      lc_attach_res checkpoint_attachment_result;
      lc_source *checkpoint_attachment;

      state_lease = job->state(job);
      if (state_lease == NULL) {
        job->close(job);
        rc = 1;
        break;
      }
      {
        double operation_started = bench_now_seconds();

        rc = lc_source_from_memory("{\"checkpoint\":1}", 16U, &checkpoint,
                                   &error);
        if (rc == LC_OK) {
          rc = lc_lease_update(state_lease, checkpoint, NULL, &error);
        }
        checkpoint_seconds += bench_now_seconds() - operation_started;
      }
      if (checkpoint != NULL) {
        lc_source_close(checkpoint);
      }
      if (rc != LC_OK) {
        job->close(job);
        break;
      }
      checkpoint_attachment = NULL;
      memset(&checkpoint_attachment_result, 0,
             sizeof(checkpoint_attachment_result));
      if (checkpoint_payload != NULL) {
        double operation_started = bench_now_seconds();

        rc = lc_source_from_memory(checkpoint_payload, (size_t)checkpoint_bytes,
                                   &checkpoint_attachment, &error);
        if (rc == LC_OK) {
          lc_attach_req_init(&checkpoint_attachment_request);
          checkpoint_attachment_request.name = "checkpoint.bin";
          checkpoint_attachment_request.content_type =
              "application/octet-stream";
          checkpoint_attachment_request.prevent_overwrite = 1;
          rc = lc_lease_attach(state_lease, &checkpoint_attachment_request,
                               checkpoint_attachment,
                               &checkpoint_attachment_result, &error);
        }
        checkpoint_seconds += bench_now_seconds() - operation_started;
      }
      lc_attach_res_cleanup(&checkpoint_attachment_result);
      if (checkpoint_attachment != NULL)
        lc_source_close(checkpoint_attachment);
      if (rc != LC_OK) {
        job->close(job);
        break;
      }
    }
    {
      double operation_started = bench_now_seconds();

      if (dead_letter_every > 0L &&
          ((long)index + 1L) % dead_letter_every == 0L) {
        rc = job->dead_letter(job, "worst-case benchmark terminal failure",
                              &error);
        if (rc == LC_OK)
          ++dead_lettered;
      } else {
        rc = job->complete(job, NULL, &error);
      }
      completion_seconds += bench_now_seconds() - operation_started;
    }
    if (rc != LC_OK) {
      (void)fprintf(stderr,
                    "outbox-reconcile completion failed key=%s code=%d "
                    "message=%s detail=%s\n",
                    job->outbox_key == NULL ? "" : job->outbox_key, rc,
                    error.message == NULL ? "" : error.message,
                    error.detail == NULL ? "" : error.detail);
    } else {
      job = NULL;
    }
    if (job != NULL) {
      job->close(job);
    }
  }
  drain_seconds =
      bench_now_seconds() -
      ((double)started.tv_sec + (double)started.tv_nsec / 1000000000.0);
  if (rc == LC_OK) {
    rc = dispatcher->get_stats(dispatcher, &stats, &error);
  }
  if (rc == LC_OK && (stats.recovered_claims < (uint64_t)rows ||
                      stats.recovery_queries < expected_recovery_queries)) {
    rc = 1;
  }
  /* Capacity retention is asynchronous maintenance.  This bounded benchmark
   * probe waits for the observable reclaim counter instead of treating a
   * transient scheduler turn as proof that stateful sidecars were reclaimed. */
  expected_dead_letter_reclaims =
      dead_lettered > effective_dead_letter_max_count
          ? dead_lettered - effective_dead_letter_max_count
          : 0UL;
  if (rc == LC_OK && expected_dead_letter_reclaims > 0UL) {
    double reclaim_deadline = bench_now_seconds() + 30.0;

    while (stats.dead_letter_reclaims < expected_dead_letter_reclaims &&
           bench_now_seconds() < reclaim_deadline) {
      struct timespec pause;

      lc_outbox_stats_cleanup(&stats);
      memset(&stats, 0, sizeof(stats));
      pause.tv_sec = 0;
      pause.tv_nsec = 10000000L;
      (void)nanosleep(&pause, NULL);
      rc = dispatcher->get_stats(dispatcher, &stats, &error);
      if (rc != LC_OK)
        break;
    }
    if (rc == LC_OK &&
        stats.dead_letter_reclaims < expected_dead_letter_reclaims)
      rc = LC_ERR_TIMEOUT;
  }
  if (lc_u64_format_base10((lc_u64)maintenance.candidate_bytes,
                           maintenance_bytes, sizeof(maintenance_bytes)) < 0) {
    rc = 1;
  }
  printf("metric=outbox-reconcile backend=%s delivery_mode=%s index_mode=%s "
         "pending_rows=%ld "
         "terminal_rows=%ld churn_updates=%ld payload_bytes=%ld "
         "dead_lettered=%lu dead_letter_reclaims=%lu "
         "page_capacity=%ld first_delivery_ms=%.3f drain_ms=%.3f "
         "next_ms=%.3f checkpoint_ms=%.3f completion_ms=%.3f "
         "throughput_rows_per_second=%.3f recovery_queries=%.0f "
         "recovered_claims=%.0f maintenance_ms=%.3f "
         "maintenance_segments=%lu maintenance_bytes=%s "
         "maintenance_compacted=%d rc=%d\n",
         fixture.is_pouch ? "pouch" : "remote",
         with_state ? "stateful" : "stateless",
         index_mode == 3
             ? "compacted"
             : (index_mode == 2 ? "persisted"
                                : (index_mode == 1 ? "preflushed" : "catchup")),
         rows, terminal_rows, churn_updates, payload_bytes, dead_lettered,
         (unsigned long)stats.dead_letter_reclaims, page_capacity,
         first_delivery_seconds * 1000.0, drain_seconds * 1000.0,
         next_seconds * 1000.0, checkpoint_seconds * 1000.0,
         completion_seconds * 1000.0,
         drain_seconds > 0.0 ? (double)rows / drain_seconds : 0.0,
         (double)stats.recovery_queries, (double)stats.recovered_claims,
         maintenance.seconds * 1000.0, maintenance.candidate_segments,
         maintenance_bytes, maintenance.compacted, rc);

done:
  if (dispatcher != NULL) {
    (void)dispatcher->stop(dispatcher, 30000L, NULL);
    dispatcher->close(dispatcher);
  }
  if (outbox != NULL) {
    outbox->close(outbox);
  }
  lc_outbox_stats_cleanup(&stats);
  free(checkpoint_payload);
  free(payload);
  bench_outbox_fixture_close(&fixture);
  if (rc != LC_OK && error.message != NULL) {
    fprintf(stderr,
            "outbox-reconcile failed: code=%d http=%ld message=%s"
            " detail=%s server_code=%s\n",
            error.code, error.http_status, error.message,
            error.detail == NULL ? "" : error.detail,
            error.server_code == NULL ? "" : error.server_code);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_outbox_reconcile(long iterations) {
  return bench_outbox_reconcile_case(iterations, 0, 0);
}

static int bench_outbox_reconcile_warm(long iterations) {
  return bench_outbox_reconcile_case(iterations, 2, 0);
}

static int bench_outbox_reconcile_preflushed(long iterations) {
  return bench_outbox_reconcile_case(iterations, 1, 0);
}

static int bench_outbox_reconcile_compacted(long iterations) {
  return bench_outbox_reconcile_case(iterations, 3, 0);
}

static int bench_outbox_stateful_delivery(long iterations) {
  return bench_outbox_reconcile_case(iterations, 0, 1);
}

static int bench_outbox_stateful_delivery_preflushed(long iterations) {
  return bench_outbox_reconcile_case(iterations, 1, 1);
}

static int bench_outbox_stateful_delivery_warm(long iterations) {
  return bench_outbox_reconcile_case(iterations, 2, 1);
}

static int bench_outbox_stateful_delivery_compacted(long iterations) {
  return bench_outbox_reconcile_case(iterations, 3, 1);
}

static int bench_outbox_command_wait(long iterations) {
  bench_outbox_fixture fixture;
  lc_outbox_config config;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
  lc_command_request request;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_commit_result commit_result;
  lc_error error;
  char idempotency_key[96];
  char command_id[48];
  double wait_seconds;
  double started;
  long completed_rows;
  long rows;
  long index;
  int rc;

  rows = iterations > 0L ? iterations : 64L;
  memset(&fixture, 0, sizeof(fixture));
  outbox = NULL;
  transaction = NULL;
  wait_seconds = 0.0;
  completed_rows = 0L;
  lc_error_init(&error);
  lc_command_receipt_init(&receipt);
  lc_outbox_commit_result_init(&commit_result);
  rc = bench_outbox_fixture_open(&fixture, 0, 0L, &error);
  if (rc != LC_OK || !fixture.is_pouch) {
    if (rc == LC_OK) {
      rc = lc_error_set(&error, LC_ERR_INVALID, 0L,
                        "command wait benchmark requires Pouch", NULL, NULL,
                        NULL);
    }
    goto done;
  }
  lc_outbox_config_init(&config);
  config.ns = fixture.ns;
  config.owner = "outbox-command-wait-benchmark";
  rc = lc_client_new_outbox(fixture.client, &config, &outbox, &error);
  for (index = 0L; rc == LC_OK && index < rows; ++index) {
    if (snprintf(idempotency_key, sizeof(idempotency_key), "wait-%08ld",
                 index) < 0) {
      rc = LC_ERR_INVALID;
      break;
    }
    transaction = NULL;
    lc_command_receipt_cleanup(&receipt);
    lc_command_receipt_init(&receipt);
    lc_command_request_init(&request);
    request.identity.scope = "outbox-command-wait-benchmark";
    request.identity.command_type = "receipt.read.v1";
    request.identity.idempotency_key = idempotency_key;
    request.request_digest = "sha256:outbox-command-wait-benchmark";
    rc = lc_outbox_accept_command(outbox, &request, &transaction, &receipt,
                                  &error);
    if (rc != LC_OK || transaction == NULL) {
      if (rc == LC_OK)
        rc = LC_ERR_INVALID;
      break;
    }
    {
      int written;

      written =
          snprintf(command_id, sizeof(command_id), "%s", receipt.command_id);
      if (written < 0 || (size_t)written >= sizeof(command_id)) {
        rc = LC_ERR_INVALID;
        break;
      }
    }
    rc = lc_outbox_transaction_commit(transaction, &commit_result, &error);
    lc_outbox_commit_result_cleanup(&commit_result);
    lc_outbox_transaction_close(transaction);
    transaction = NULL;
    if (rc != LC_OK)
      break;
    lc_command_receipt_cleanup(&receipt);
    lc_command_receipt_init(&receipt);
    rc = lc_outbox_resume_command_by_id(outbox, command_id, &transaction,
                                        &receipt, &error);
    if (rc != LC_OK || transaction == NULL) {
      if (rc == LC_OK)
        rc = LC_ERR_INVALID;
      break;
    }
    lc_command_result_init(&result);
    result.result_code = "completed";
    rc = lc_outbox_transaction_complete_command(transaction, &result, &error);
    if (rc == LC_OK)
      rc = lc_outbox_transaction_commit(transaction, &commit_result, &error);
    lc_outbox_commit_result_cleanup(&commit_result);
    lc_outbox_transaction_close(transaction);
    transaction = NULL;
    if (rc != LC_OK)
      break;
    lc_command_receipt_cleanup(&receipt);
    lc_command_receipt_init(&receipt);
    started = bench_now_seconds();
    rc = lc_outbox_wait_command(outbox, command_id, 0L, &receipt, &error);
    wait_seconds += bench_now_seconds() - started;
    if (rc != LC_OK || receipt.state != LC_COMMAND_COMPLETED) {
      if (rc == LC_OK)
        rc = LC_ERR_INVALID;
      break;
    }
    completed_rows += 1L;
  }
  printf("metric=outbox-command-wait backend=pouch terminal_receipts=%ld "
         "terminal_read_ms=%.3f terminal_reads_per_second=%.3f rc=%d\n",
         completed_rows, wait_seconds * 1000.0,
         wait_seconds > 0.0 ? (double)completed_rows / wait_seconds : 0.0, rc);

done:
  if (transaction != NULL)
    lc_outbox_transaction_close(transaction);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_command_receipt_cleanup(&receipt);
  bench_outbox_fixture_close(&fixture);
  if (rc != LC_OK && error.message != NULL) {
    fprintf(
        stderr, "outbox-command-wait failed: code=%d message=%s detail=%s\n",
        error.code, error.message, error.detail == NULL ? "" : error.detail);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_outbox_dispatcher_child(const bench_outbox_fixture *fixture,
                                         unsigned long dispatcher_index,
                                         long page_capacity, int start_fd,
                                         int result_fd) {
  bench_outbox_dispatcher_result result;
  lc_outbox_config config;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_client *client;
  lc_error error;
  char owner[128];
  char start;
  int idle_count;
  int rc;

  memset(&result, 0, sizeof(result));
  outbox = NULL;
  dispatcher = NULL;
  client = NULL;
  idle_count = 0;
  lc_error_init(&error);
  result.ready = 1;
  result.rc = LC_OK;
  if (write(result_fd, &result, sizeof(result)) != (ssize_t)sizeof(result)) {
    rc = LC_ERR_TRANSPORT;
  } else {
    rc = LC_OK;
  }
  if (rc == LC_OK && read(start_fd, &start, 1U) != 1) {
    rc = LC_ERR_TRANSPORT;
  }
  if (rc == LC_OK && start != 's') {
    rc = LC_ERR_INVALID;
  }
  if (rc == LC_OK) {
    result.failure_stage = 3;
    rc = bench_outbox_fixture_client_open(fixture, fixture->is_pouch, 0,
                                          &client, &error);
  }
  if (rc == LC_OK &&
      snprintf(owner, sizeof(owner), "outbox-benchmark-dispatcher-%lu",
               dispatcher_index) < 0) {
    rc = LC_ERR_INVALID;
  }
  if (rc == LC_OK) {
    result.failure_stage = 4;
    lc_outbox_config_init(&config);
    config.ns = fixture->ns;
    config.owner = owner;
    config.notification_capacity = (size_t)page_capacity;
    config.recovery_interval_seconds = 1L;
    rc = lc_client_new_outbox(client, &config, &outbox, &error);
    if (rc == LC_OK) {
      rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
    }
  }
  while (rc == LC_OK && idle_count < BENCH_OUTBOX_DISPATCHER_IDLE_LIMIT) {
    lc_outbox_job *job;

    job = NULL;
    result.failure_stage = 5;
    rc = dispatcher->next(dispatcher, 250L, &job, &error);
    if (rc != LC_OK) {
      break;
    }
    if (job == NULL) {
      ++idle_count;
      continue;
    }
    idle_count = 0;
    if (job->outbox_key == NULL || strncmp(job->outbox_key, fixture->key_prefix,
                                           strlen(fixture->key_prefix)) != 0) {
      result.failure_stage = 1;
      if (job->outbox_key != NULL) {
        (void)snprintf(result.job_key, sizeof(result.job_key), "%s",
                       job->outbox_key);
      }
      job->close(job);
      rc = LC_ERR_INVALID;
      break;
    }
    if (result.delivered == 0UL) {
      result.first_delivery_seconds = bench_now_seconds();
    }
    if (bench_env_enabled("LOCKDC_OUTBOX_BENCH_TRACE")) {
      (void)fprintf(stderr, "outbox dispatcher=%s claimed=%s\n", owner,
                    job->outbox_key == NULL ? "" : job->outbox_key);
    }
    rc = job->complete(job, NULL, &error);
    if (rc == LC_OK) {
      job = NULL;
    }
    if (job != NULL) {
      job->close(job);
    }
    if (rc == LC_OK) {
      ++result.delivered;
      result.last_delivery_seconds = bench_now_seconds();
      result.failure_stage = 0;
    } else {
      result.failure_stage = 2;
      (void)fprintf(stderr,
                    "outbox dispatcher completion failed: code=%d message=%s "
                    "detail=%s\n",
                    rc, error.message == NULL ? "" : error.message,
                    error.detail == NULL ? "" : error.detail);
    }
  }
  result.ready = 0;
  result.rc = rc;
  if (error.message != NULL) {
    (void)snprintf(result.error_message, sizeof(result.error_message), "%s",
                   error.message);
  }
  if (write(result_fd, &result, sizeof(result)) != (ssize_t)sizeof(result)) {
    rc = LC_ERR_TRANSPORT;
  }
  if (outbox != NULL) {
    outbox->close(outbox);
  }
  if (dispatcher != NULL) {
    (void)dispatcher->stop(dispatcher, 30000L, NULL);
    dispatcher->close(dispatcher);
  }
  if (client != NULL) {
    client->close(client);
  }
  lc_error_cleanup(&error);
  return rc;
}

static int bench_outbox_reconcile_multi_case(long iterations,
                                             int compact_before_fork) {
  bench_outbox_fixture fixture;
  bench_outbox_maintenance_stats maintenance;
  bench_outbox_dispatcher_result result;
  bench_outbox_dispatcher_result results[BENCH_OUTBOX_MAX_DISPATCHERS];
  pid_t children[BENCH_OUTBOX_MAX_DISPATCHERS];
  int start_pipes[BENCH_OUTBOX_MAX_DISPATCHERS][2];
  int result_pipes[BENCH_OUTBOX_MAX_DISPATCHERS][2];
  lc_error error;
  double started;
  double first_delivery_seconds;
  double last_delivery_seconds;
  long rows;
  long terminal_rows;
  long churn_updates;
  long payload_bytes;
  long page_capacity;
  long dispatcher_count;
  long pouch_segment_target_bytes;
  unsigned long delivered;
  char maintenance_bytes[32];
  char *payload;
  size_t index;
  int rc;

  rows = iterations > 0L ? iterations : 256L;
  terminal_rows =
      bench_env_long("LOCKDC_OUTBOX_BENCH_TERMINAL_ROWS", rows * 4L, 0L);
  churn_updates = bench_env_long("LOCKDC_OUTBOX_BENCH_CHURN_UPDATES", 4L, 0L);
  payload_bytes =
      bench_env_long("LOCKDC_OUTBOX_BENCH_PAYLOAD_BYTES", 4096L, 0L);
  page_capacity = bench_env_long("LOCKDC_OUTBOX_BENCH_PAGE_CAPACITY", 16L, 1L);
  dispatcher_count = bench_env_long("LOCKDC_OUTBOX_BENCH_DISPATCHERS", 2L, 2L);
  if ((unsigned long)dispatcher_count > BENCH_OUTBOX_MAX_DISPATCHERS) {
    dispatcher_count = (long)BENCH_OUTBOX_MAX_DISPATCHERS;
  }
  pouch_segment_target_bytes =
      bench_env_long("LOCKDC_OUTBOX_BENCH_SEGMENT_TARGET_BYTES",
                     BENCH_OUTBOX_COMPACT_SEGMENT_TARGET_BYTES, 0L);
  memset(&fixture, 0, sizeof(fixture));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(maintenance_bytes, 0, sizeof(maintenance_bytes));
  memset(results, 0, sizeof(results));
  payload = NULL;
  for (index = 0U; index < BENCH_OUTBOX_MAX_DISPATCHERS; ++index) {
    children[index] = -1;
    start_pipes[index][0] = -1;
    start_pipes[index][1] = -1;
    result_pipes[index][0] = -1;
    result_pipes[index][1] = -1;
  }
  lc_error_init(&error);
  rc = bench_outbox_fixture_open(&fixture, 1, pouch_segment_target_bytes,
                                 &error);
  if (rc == LC_OK && payload_bytes > 0L) {
    payload = (char *)malloc((size_t)payload_bytes);
    if (payload == NULL) {
      rc = LC_ERR_NOMEM;
    } else {
      memset(payload, 'p', (size_t)payload_bytes);
    }
  }
  for (index = 0U; rc == LC_OK && index < (size_t)terminal_rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = LC_ERR_INVALID;
      break;
    }
    rc = bench_outbox_seed_record(fixture.client, fixture.ns, key, "completed",
                                  0L, NULL, 0U, &error);
  }
  for (index = 0U; rc == LC_OK && index < (size_t)churn_updates; ++index) {
    size_t terminal_index;

    for (terminal_index = 0U; terminal_index < (size_t)terminal_rows;
         ++terminal_index) {
      char key[128];

      if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                   (unsigned long)terminal_index) < 0) {
        rc = LC_ERR_INVALID;
        break;
      }
      rc =
          bench_outbox_seed_record(fixture.client, fixture.ns, key, "completed",
                                   (long)index + 1L, NULL, 0U, &error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  for (index = 0U; rc == LC_OK && index < (size_t)rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/pending-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = LC_ERR_INVALID;
      break;
    }
    rc = bench_outbox_seed_record(fixture.client, fixture.ns, key, "pending",
                                  0L, payload, (size_t)payload_bytes, &error);
  }
  if (rc == LC_OK) {
    rc = bench_outbox_flush_index(fixture.client, fixture.ns, "sync", &error);
  }
  if (rc == LC_OK && compact_before_fork) {
    rc = bench_outbox_fixture_compact(&fixture, &maintenance, &error);
  }
  if (fixture.client != NULL) {
    fixture.client->close(fixture.client);
    fixture.client = NULL;
  }
  for (index = 0U; rc == LC_OK && index < (size_t)dispatcher_count; ++index) {
    if (pipe(start_pipes[index]) != 0 || pipe(result_pipes[index]) != 0) {
      rc = LC_ERR_TRANSPORT;
      break;
    }
    children[index] = fork();
    if (children[index] < 0) {
      rc = LC_ERR_TRANSPORT;
      break;
    }
    if (children[index] == 0) {
      size_t close_index;
      int child_rc;

      for (close_index = 0U; close_index <= index; ++close_index) {
        if (close_index != index) {
          if (start_pipes[close_index][0] >= 0) {
            (void)close(start_pipes[close_index][0]);
          }
          if (result_pipes[close_index][1] >= 0) {
            (void)close(result_pipes[close_index][1]);
          }
        }
        if (start_pipes[close_index][1] >= 0) {
          (void)close(start_pipes[close_index][1]);
        }
        if (result_pipes[close_index][0] >= 0) {
          (void)close(result_pipes[close_index][0]);
        }
      }
      child_rc = bench_outbox_dispatcher_child(
          &fixture, (unsigned long)index, page_capacity, start_pipes[index][0],
          result_pipes[index][1]);
      (void)close(start_pipes[index][0]);
      (void)close(result_pipes[index][1]);
      _exit(child_rc == LC_OK ? 0 : 1);
    }
    (void)close(start_pipes[index][0]);
    start_pipes[index][0] = -1;
    (void)close(result_pipes[index][1]);
    result_pipes[index][1] = -1;
  }
  for (index = 0U; rc == LC_OK && index < (size_t)dispatcher_count; ++index) {
    ssize_t read_count;

    read_count = read(result_pipes[index][0], &result, sizeof(result));
    if (read_count != (ssize_t)sizeof(result)) {
      rc = lc_error_set(&error, LC_ERR_TRANSPORT, errno,
                        "outbox dispatcher did not report readiness", NULL,
                        NULL, NULL);
      break;
    }
    if (!result.ready || result.rc != LC_OK) {
      rc = lc_error_set(
          &error, result.rc == LC_OK ? LC_ERR_PROTOCOL : result.rc, 0L,
          "outbox dispatcher failed to initialize",
          result.error_message[0] == '\0' ? NULL : result.error_message, NULL,
          NULL);
      break;
    }
  }
  started = bench_now_seconds();
  for (index = 0U; rc == LC_OK && index < (size_t)dispatcher_count; ++index) {
    char start;

    start = 's';
    if (write(start_pipes[index][1], &start, 1U) != 1) {
      rc = LC_ERR_TRANSPORT;
      break;
    }
    (void)close(start_pipes[index][1]);
    start_pipes[index][1] = -1;
  }
  first_delivery_seconds = 0.0;
  last_delivery_seconds = 0.0;
  for (index = 0U; index < (size_t)dispatcher_count; ++index) {
    if (result_pipes[index][0] >= 0) {
      ssize_t read_count;

      read_count = read(result_pipes[index][0], &result, sizeof(result));
      if (read_count != (ssize_t)sizeof(result)) {
        if (rc == LC_OK) {
          rc = lc_error_set(&error, LC_ERR_TRANSPORT, errno,
                            "outbox dispatcher did not report completion", NULL,
                            NULL, NULL);
        }
      } else if (result.ready || result.rc != LC_OK) {
        if (rc == LC_OK) {
          rc = lc_error_set(
              &error, result.rc == LC_OK ? LC_ERR_PROTOCOL : result.rc, 0L,
              "outbox dispatcher failed while reconciling",
              result.error_message[0] == '\0' ? NULL : result.error_message,
              NULL, NULL);
          (void)fprintf(stderr,
                        "outbox dispatcher failure_stage=%d job_key=%s\n",
                        result.failure_stage,
                        result.job_key[0] == '\0' ? "" : result.job_key);
        }
      } else {
        results[index] = result;
        if (result.first_delivery_seconds > 0.0 &&
            (first_delivery_seconds == 0.0 ||
             result.first_delivery_seconds < first_delivery_seconds)) {
          first_delivery_seconds = result.first_delivery_seconds;
        }
        if (result.last_delivery_seconds > last_delivery_seconds) {
          last_delivery_seconds = result.last_delivery_seconds;
        }
      }
      (void)close(result_pipes[index][0]);
      result_pipes[index][0] = -1;
    }
  }
  for (index = 0U; index < (size_t)dispatcher_count; ++index) {
    int status;

    if (children[index] > 0 &&
        waitpid(children[index], &status, 0) != children[index]) {
      if (rc == LC_OK) {
        rc = LC_ERR_TRANSPORT;
      }
    } else if (children[index] > 0 &&
               (!WIFEXITED(status) || WEXITSTATUS(status) != 0)) {
      if (rc == LC_OK) {
        rc = lc_error_set(&error, LC_ERR_TRANSPORT, 0L,
                          "outbox dispatcher exited unsuccessfully", NULL, NULL,
                          NULL);
      }
      (void)fprintf(stderr, "outbox dispatcher exit_status=%d\n", status);
    }
  }
  delivered = 0UL;
  for (index = 0U; index < (size_t)dispatcher_count; ++index) {
    delivered += results[index].delivered;
  }
  if (rc == LC_OK) {
    if (delivered != (unsigned long)rows || first_delivery_seconds == 0.0 ||
        last_delivery_seconds < first_delivery_seconds) {
      rc = LC_ERR_INVALID;
    }
  }
  if (lc_u64_format_base10((lc_u64)maintenance.candidate_bytes,
                           maintenance_bytes, sizeof(maintenance_bytes)) < 0) {
    rc = LC_ERR_INVALID;
  }
  printf(
      "metric=outbox-reconcile-multi backend=%s index_mode=%s "
      "dispatchers=%ld "
      "pending_rows=%ld terminal_rows=%ld churn_updates=%ld delivered_rows=%lu "
      "first_delivery_ms=%.3f drain_ms=%.3f throughput_rows_per_second=%.3f "
      "maintenance_ms=%.3f maintenance_segments=%lu maintenance_bytes=%s "
      "maintenance_compacted=%d "
      "rc=%d\n",
      fixture.is_pouch ? "pouch" : "remote",
      compact_before_fork ? "compacted" : "persisted", dispatcher_count, rows,
      terminal_rows, churn_updates, delivered,
      first_delivery_seconds > 0.0 ? (first_delivery_seconds - started) * 1000.0
                                   : 0.0,
      last_delivery_seconds > 0.0 ? (last_delivery_seconds - started) * 1000.0
                                  : 0.0,
      last_delivery_seconds > started
          ? (double)rows / (last_delivery_seconds - started)
          : 0.0,
      maintenance.seconds * 1000.0, maintenance.candidate_segments,
      maintenance_bytes, maintenance.compacted, rc);

  for (index = 0U; index < BENCH_OUTBOX_MAX_DISPATCHERS; ++index) {
    if (start_pipes[index][0] >= 0) {
      (void)close(start_pipes[index][0]);
    }
    if (start_pipes[index][1] >= 0) {
      (void)close(start_pipes[index][1]);
    }
    if (result_pipes[index][0] >= 0) {
      (void)close(result_pipes[index][0]);
    }
    if (result_pipes[index][1] >= 0) {
      (void)close(result_pipes[index][1]);
    }
  }
  bench_outbox_fixture_close(&fixture);
  free(payload);
  if (rc != LC_OK && error.message != NULL) {
    fprintf(stderr, "outbox-reconcile-multi failed: code=%d message=%s\n",
            error.code, error.message);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_outbox_reconcile_multi(long iterations) {
  return bench_outbox_reconcile_multi_case(iterations, 0);
}

static int bench_outbox_reconcile_multi_compacted(long iterations) {
  return bench_outbox_reconcile_multi_case(iterations, 1);
}

static char *bench_pouch_perf_document(long row, long generation,
                                       long payload_bytes) {
  const char *summary;
  const char *description;
  const char *stage;
  const char *tier;
  const char *risk;
  size_t capacity;
  size_t used;
  char *json;
  int written;

  if (payload_bytes < 1024L) {
    payload_bytes = 1024L;
  }
  capacity = (size_t)payload_bytes + 2048U;
  json = (char *)malloc(capacity);
  if (json == NULL) {
    return NULL;
  }
  summary = row % 2L == 0L ? "remediation audit outbox timeout risk narrative"
                           : "ordinary audit outbox context narrative";
  description = row % 3L == 0L
                    ? "audit evidence remediation plan with timeout handling"
                    : "audit evidence review plan with normal handling";
  stage = row % 5L == 0L ? "escalated" : "review";
  tier = row % 4L == 0L ? "enterprise" : "standard";
  risk = row % 7L == 0L ? "timeout pressure" : "routine pressure";
  written =
      snprintf(json, capacity,
               "{\"tenant\":{\"id\":\"tenant-%03ld\",\"tier\":\"%s\"},"
               "\"outbox\":{\"id\":\"wf-%03ld\",\"stage\":\"%s\","
               "\"owner\":{\"team\":\"ops\",\"region\":\"%s\"}},"
               "\"metrics\":{\"amount_usd\":%ld,\"risk_score\":%ld},"
               "\"risk\":{\"summary\":\"%s\"},"
               "\"narrative\":{\"summary\":\"%s row %ld generation %ld\","
               "\"description\":\"%s row %ld generation %ld\"},"
               "\"tags\":[\"audit\",\"finance\",\"planning\"],"
               "\"created_at\":\"2026-01-01T00:00:00Z\",\"payload\":\"",
               row % 17L, tier, row % 23L, stage, row % 3L == 0L ? "us" : "eu",
               1000L + (row * 37L), row % 100L, risk, summary, row, generation,
               description, row, generation);
  if (written <= 0 || (size_t)written >= capacity) {
    free(json);
    return NULL;
  }
  used = (size_t)written;
  while (used + 96U < capacity && (long)used < payload_bytes) {
    written = snprintf(json + used, capacity - used,
                       " audit remediation evidence outbox row %ld gen %ld;",
                       row, generation);
    if (written <= 0 || (size_t)written >= capacity - used) {
      free(json);
      return NULL;
    }
    used += (size_t)written;
  }
  if (used + 3U >= capacity) {
    free(json);
    return NULL;
  }
  json[used++] = '"';
  json[used++] = '}';
  json[used] = '\0';
  return json;
}

static int bench_pouch_seed_update(lc_client *client, const char *key,
                                   lc_source *source, lc_error *error) {
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_release_req release_req;
  lc_update_opts update_opts;
  int rc;

  if (client == NULL || key == NULL || source == NULL) {
    return 1;
  }
  lease = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_update_opts_init(&update_opts);
  acquire_req.ns = "bench";
  acquire_req.key = key;
  acquire_req.owner = "lockdc-bench";
  acquire_req.ttl_seconds = 30L;
  update_opts.content_type = "application/json";
  rc = client->acquire(client, &acquire_req, &lease, error);
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (lease != NULL) {
    if (rc == LC_OK) {
      rc = lease->release(lease, &release_req, error);
      if (rc == LC_OK) {
        lease = NULL;
      }
    }
    if (lease != NULL) {
      lease->close(lease);
    }
  }
  return rc;
}

static int bench_pouch_seed_perf_docs(lc_client *client, long rows,
                                      long generation, long payload_bytes,
                                      lc_error *error) {
  long row;

  for (row = 0L; row < rows; ++row) {
    lc_source *source;
    char key[64];
    char *json;
    int rc;

    json = bench_pouch_perf_document(row, generation, payload_bytes);
    if (json == NULL) {
      return 1;
    }
    source = NULL;
    rc = lc_source_from_memory(json, strlen(json), &source, error);
    if (rc == LC_OK) {
      snprintf(key, sizeof(key), "doc/%08ld", row);
      rc = bench_pouch_seed_update(client, key, source, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    free(json);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int bench_pouch_perf_flush(lc_client *client, lc_error *error) {
  lc_index_flush_req req;
  lc_index_flush_res res;
  int rc;

  lc_index_flush_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.ns = "bench";
  req.mode = "wait";
  rc = client->flush_index(client, &req, &res, error);
  lc_index_flush_res_cleanup(&res);
  return rc;
}

static int bench_pouch_perf_query(lc_client *client, const char *selector_lql,
                                  const char *engine, int documents, long limit,
                                  lc_error *error) {
  lc_query_req req;
  lc_query_res res;
  int rc;

  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.ns = "bench";
  req.selector_lql = selector_lql;
  req.engine = engine;
  req.limit = limit > 0L ? limit : 1L;
  if (documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_discard(&sink, error);
    if (rc == LC_OK) {
      rc = client->query(client, &req, sink, &res, error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
  } else {
    lc_query_key_handler handler;
    bench_query_key_count key_count;

    memset(&handler, 0, sizeof(handler));
    memset(&key_count, 0, sizeof(key_count));
    handler.begin = bench_query_key_begin;
    handler.chunk = bench_query_key_chunk;
    handler.end = bench_query_key_end;
    rc = client->query_keys(client, &req, &handler, &key_count, &res, error);
  }
  lc_query_res_cleanup(&res);
  return rc;
}

static long bench_pouch_perf_rows(long iterations) {
  return iterations > 0L ? iterations : 128L;
}

static long bench_pouch_perf_payload_bytes(void) {
  const char *value;
  long parsed;

  value = getenv("LOCKDC_POUCH_PERF_PAYLOAD_BYTES");
  if (value == NULL || value[0] == '\0') {
    return 4096L;
  }
  parsed = strtol(value, NULL, 10);
  return parsed > 0L ? parsed : 4096L;
}

static int bench_pouch_perf_prepare(bench_pouch_perf_fixture *fixture,
                                    long rows, long payload_bytes,
                                    lc_error *error) {
  int rc;

  rc = bench_pouch_perf_fixture_open(fixture, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = bench_pouch_seed_perf_docs(fixture->client, rows, 0L, payload_bytes,
                                  error);
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture->client, error);
  }
  return rc;
}

static int bench_pouch_perf_query_case(long iterations, const char *name,
                                       const char *selector_lql,
                                       const char *engine, int documents) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client, selector_lql, engine, documents,
                                rows, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client, selector_lql, engine, documents,
                                rows, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=%s rows=%ld crypto=%d seconds=%.6f per_row_us=%.3f rc=%d\n",
         name, rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_index_docs(long iterations) {
  return bench_pouch_perf_query_case(
      iterations, "pouch-perf-index-docs",
      "icontains{field=/narrative/summary,value=remediation}", "index", 1);
}

static int bench_pouch_perf_get_body(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  lc_get_opts options;
  lc_sink *sink;
  double start;
  double elapsed;
  long calls;
  long index;
  int rc;

  lc_error_init(&error);
  memset(&fixture, 0, sizeof(fixture));
  lc_get_opts_init(&options);
  options.public_read = 1;
  sink = NULL;
  calls = bench_pouch_perf_rows(iterations);
  rc = bench_pouch_perf_prepare(&fixture, 1L, bench_pouch_perf_payload_bytes(),
                                &error);
  if (rc == LC_OK) {
    rc = lc_sink_to_discard(&sink, &error);
  }
  if (rc == LC_OK) {
    lc_get_res result;

    memset(&result, 0, sizeof(result));
    rc = fixture.client->get(fixture.client, "doc/00000000", &options, sink,
                             &result, &error);
    lc_get_res_cleanup(&result);
  }
  start = bench_now_seconds();
  for (index = 0L; rc == LC_OK && index < calls; ++index) {
    lc_get_res result;

    memset(&result, 0, sizeof(result));
    rc = fixture.client->get(fixture.client, "doc/00000000", &options, sink,
                             &result, &error);
    lc_get_res_cleanup(&result);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-get-body calls=%ld bytes=%ld crypto=%d "
         "seconds=%.6f per_call_us=%.3f rc=%d\n",
         calls, bench_pouch_perf_payload_bytes(),
         bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         calls > 0L ? (elapsed * 1000000.0) / (double)calls : 0.0, rc);
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_full_text_keys(long iterations) {
  return bench_pouch_perf_query_case(iterations, "pouch-perf-full-text-keys",
                                     "icontains{field=/...,value=audit}",
                                     "index", 0);
}

static int bench_pouch_perf_full_text_reopen_keys(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (fixture.client != NULL) {
    fixture.client->close(fixture.client);
    fixture.client = NULL;
  }
  if (rc == LC_OK) {
    rc = bench_pouch_client_open_crypto(fixture.root, fixture.crypto_key, 0,
                                        &fixture.client, &error);
  }
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client,
                                "icontains{field=/...,value=audit}", "index", 0,
                                rows, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-full-text-reopen-keys rows=%ld crypto=%d "
         "seconds=%.6f per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_scan_keys(long iterations) {
  return bench_pouch_perf_query_case(
      iterations, "pouch-perf-scan-keys",
      "in{field=/outbox/stage,any=review|escalated}", "scan", 0);
}

static int bench_pouch_perf_flush_intermediate(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (rc == LC_OK) {
    rc = bench_pouch_seed_perf_docs(fixture.client, rows, 1L,
                                    bench_pouch_perf_payload_bytes(), &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-flush-intermediate rows=%ld crypto=%d "
         "seconds=%.6f per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_flush_reopen(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (fixture.client != NULL) {
    fixture.client->close(fixture.client);
    fixture.client = NULL;
  }
  if (rc == LC_OK) {
    rc = bench_pouch_client_open_crypto(fixture.root, fixture.crypto_key, 0,
                                        &fixture.client, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-flush-reopen rows=%ld crypto=%d seconds=%.6f "
         "per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_seed_query_docs(lc_client *client, long count,
                                       lc_error *error) {
  long i;

  for (i = 0; i < count; ++i) {
    lc_source *source;
    char key[64];
    const char *bucket;
    const char *group;
    const char *region;
    const char *tag0;
    const char *tag1;
    const char *created_at;
    const char *message;
    const char *flag;
    char json[512];
    int match;
    int written;
    int rc;

    match = (i % 2L) == 0L;
    bucket = i % 64L == 0L ? "needle" : "haystack";
    group = match ? "even" : "odd";
    if (i % 3L == 0L) {
      region = "us";
    } else if (i % 3L == 1L) {
      region = "eu";
    } else {
      region = "apac";
    }
    tag0 = match ? "planning" : "runtime";
    tag1 = i % 4L == 0L ? "finance" : "ops";
    if (i % 5L == 0L) {
      created_at = "2026-01-01T00:00:00Z";
    } else if (i % 5L == 1L) {
      created_at = "not-a-date";
    } else {
      created_at = "2024-01-01T00:00:00Z";
    }
    message = i % 8L == 0L ? "timeout" : "ordinary";
    flag = i % 7L == 0L ? "true" : "false";
    snprintf(key, sizeof(key), "doc/%08ld", i);
    written = snprintf(
        json, sizeof(json),
        "{\"n\":%ld,\"owner\":\"%s\",\"bucket\":\"%s\",\"group\":\"%s\","
        "\"region\":\"%s\",\"value\":%ld,\"tags\":[\"%s\",\"%s\"],"
        "\"created_at\":\"%s\",\"details\":{\"message\":\"%s benchmark "
        "document %ld\"},\"flag\":%s}",
        i, match ? "alpha" : "beta", bucket, group, region, i, tag0, tag1,
        created_at, message, i, flag);
    if (written <= 0 || (size_t)written >= sizeof(json)) {
      return 1;
    }
    source = NULL;
    rc = lc_source_from_memory(json, strlen(json), &source, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = bench_pouch_seed_update(client, key, source, error);
    lc_source_close(source);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int bench_pouch_query_once(lc_client *client,
                                  const bench_pouch_query_case *query_case,
                                  long count, lc_error *error) {
  lc_query_req req;
  lc_query_res res;
  int rc;

  memset(&res, 0, sizeof(res));
  lc_query_req_init(&req);
  req.ns = "bench";
  req.selector_json = query_case->selector_json;
  req.selector_lql = query_case->selector_lql;
  req.engine = query_case->engine;
  req.limit = count > 0L ? count : 1L;
  if (query_case->documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_memory(&sink, error);
    if (rc == LC_OK) {
      rc = client->query(client, &req, sink, &res, error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
  } else {
    lc_query_key_handler handler;
    bench_query_key_count key_count;

    memset(&handler, 0, sizeof(handler));
    memset(&key_count, 0, sizeof(key_count));
    handler.begin = bench_query_key_begin;
    handler.chunk = bench_query_key_chunk;
    handler.end = bench_query_key_end;
    rc = client->query_keys(client, &req, &handler, &key_count, &res, error);
  }
  lc_query_res_cleanup(&res);
  return rc;
}

static int bench_pouch_query_text(long iterations,
                                  const bench_pouch_query_case *query_case) {
  lc_client *client;
  lc_error error;
  char root[512];
  int rc;

  lc_error_init(&error);
  client = NULL;
  root[0] = '\0';
  rc = 0;
  if (iterations <= 0L) {
    iterations = 1L;
  }
  if (bench_pouch_root_path(root, sizeof(root), "query-text") != 0) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_client_open(root, &client, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_seed_query_docs(client, iterations, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_query_once(client, query_case, iterations, &error) != LC_OK) {
    rc = 1;
    goto done;
  }

done:
  if (client != NULL) {
    client->close(client);
  }
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}

#ifndef LOCKDC_POUCH_SPACE_BENCH
static int bench_pouch_open(long iterations) {
  lc_error error;
  char root[512];
  long i;
  int rc;

  lc_error_init(&error);
  root[0] = '\0';
  rc = 0;
  if (bench_pouch_root_path(root, sizeof(root), "open") != 0) {
    rc = 1;
    goto done;
  }
  for (i = 0; i < iterations; ++i) {
    lc_pouch *pouch;

    pouch = NULL;
    if (lc_pouch_open(root, NULL, NULL, &pouch, &error) != LC_OK) {
      rc = 1;
      goto done;
    }
    lc_pouch_close(pouch);
  }

done:
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}

static int bench_pouch_namespace(long iterations) {
  lc_error error;
  lc_pouch *pouch;
  char root[512];
  long i;
  int rc;

  lc_error_init(&error);
  pouch = NULL;
  root[0] = '\0';
  rc = 0;
  if (bench_pouch_root_path(root, sizeof(root), "namespace") != 0) {
    rc = 1;
    goto done;
  }
  if (lc_pouch_open(root, NULL, NULL, &pouch, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  for (i = 0; i < iterations; ++i) {
    char ns[64];

    snprintf(ns, sizeof(ns), "bench/%ld", i);
    if (lc_pouch_ensure_namespace(pouch, ns, &error) != LC_OK) {
      rc = 1;
      goto done;
    }
  }

done:
  if (pouch != NULL) {
    lc_pouch_close(pouch);
  }
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}
#endif

static int bench_pouch_query_iprefix_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "index",
      0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "scan",
      0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "index",
      1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "scan",
      1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL,
      "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL, "scan",
      0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL,
      "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL, "scan",
      1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "scan", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "scan", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

#define BENCH_POUCH_LQL_FUNC(function_name, selector_text, engine_text,        \
                             documents_value)                                  \
  static int function_name(long iterations) {                                  \
    static const bench_pouch_query_case query_case = {                         \
        NULL, selector_text, engine_text, documents_value};                    \
                                                                               \
    return bench_pouch_query_text(iterations, &query_case);                    \
  }

BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_index_keys,
                     "eq{field=/bucket,value=needle}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_scan_keys,
                     "eq{field=/bucket,value=needle}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_index_documents,
                     "eq{field=/bucket,value=needle}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_scan_documents,
                     "eq{field=/bucket,value=needle}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_index_keys,
                     "eq{field=/group,value=even}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_scan_keys,
                     "eq{field=/group,value=even}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_index_documents,
                     "eq{field=/group,value=even}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_scan_documents,
                     "eq{field=/group,value=even}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_index_keys,
                     "range{field=/value,gte=0}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_scan_keys,
                     "range{field=/value,gte=0}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_index_documents,
                     "range{field=/value,gte=0}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_scan_documents,
                     "range{field=/value,gte=0}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_index_keys,
                     "in{field=/region,any=us}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_scan_keys,
                     "in{field=/region,any=us}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_index_documents,
                     "in{field=/region,any=us}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_scan_documents,
                     "in{field=/region,any=us}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_index_keys,
                     "in{field=/tags[],any=planning|finance}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_scan_keys,
                     "in{field=/tags[],any=planning|finance}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_index_documents,
                     "in{field=/tags[],any=planning|finance}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_scan_documents,
                     "in{field=/tags[],any=planning|finance}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_index_keys,
                     "contains{field=/details/message,value=timeout}", "index",
                     0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_scan_keys,
                     "contains{field=/details/message,value=timeout}", "scan",
                     0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_index_documents,
                     "contains{field=/details/message,value=timeout}", "index",
                     1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_scan_documents,
                     "contains{field=/details/message,value=timeout}", "scan",
                     1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_index_keys,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_scan_keys,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_index_documents,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_scan_documents,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "scan", 1)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_index_keys,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "index",
    0)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_keys,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    0)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_index_documents,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "index",
    1)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_documents,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    1)

static const bench_case *bench_cases(void) {
  static const bench_case cases[] = {
      {"stream-copy", 1000L, bench_stream_copy},
      {"collections", 1024L, bench_collections},
#ifdef LOCKDC_POUCH_SPACE_BENCH
      {"pouch-lease", 64L, bench_pouch_lease},
      {"pouch-state-api", 32L, bench_pouch_state_api},
      {"pouch-attachment", 32L, bench_pouch_attachment},
      {"pouch-queue-delivery", 16L, bench_pouch_queue_delivery},
      {"pouch-queue-watch", 16L, bench_pouch_queue_watch},
      {"pouch-transaction", 16L, bench_pouch_transaction},
      {"collection-control", 16L, bench_collection_control},
#else
      {"pouch-open", 1000L, bench_pouch_open},
      {"pouch-namespace", 1000L, bench_pouch_namespace},
#endif
      {"pouch-perf-index-docs", 128L, bench_pouch_perf_index_docs},
      {"pouch-perf-get-body", 1024L, bench_pouch_perf_get_body},
      {"pouch-perf-full-text-keys", 128L, bench_pouch_perf_full_text_keys},
      {"pouch-perf-full-text-reopen-keys", 128L,
       bench_pouch_perf_full_text_reopen_keys},
      {"pouch-perf-scan-keys", 128L, bench_pouch_perf_scan_keys},
      {"pouch-perf-flush-intermediate", 128L,
       bench_pouch_perf_flush_intermediate},
      {"pouch-perf-flush-reopen", 128L, bench_pouch_perf_flush_reopen},
      {"outbox-reconcile", 256L, bench_outbox_reconcile},
      {"outbox-stateful-delivery", 256L, bench_outbox_stateful_delivery},
      {"outbox-stateful-delivery-preflushed", 256L,
       bench_outbox_stateful_delivery_preflushed},
      {"outbox-stateful-delivery-warm", 256L,
       bench_outbox_stateful_delivery_warm},
      {"outbox-stateful-delivery-compacted", 256L,
       bench_outbox_stateful_delivery_compacted},
      {"outbox-reconcile-preflushed", 256L, bench_outbox_reconcile_preflushed},
      {"outbox-reconcile-warm", 256L, bench_outbox_reconcile_warm},
      {"outbox-reconcile-compacted", 256L, bench_outbox_reconcile_compacted},
      {"outbox-reconcile-multi", 256L, bench_outbox_reconcile_multi},
      {"outbox-reconcile-multi-compacted", 256L,
       bench_outbox_reconcile_multi_compacted},
      {"outbox-command-wait", 64L, bench_outbox_command_wait},
      {"pouch-query-eq-sparse-index-keys", 1024L,
       bench_pouch_query_eq_sparse_index_keys},
      {"pouch-query-eq-sparse-scan-keys", 1024L,
       bench_pouch_query_eq_sparse_scan_keys},
      {"pouch-query-eq-sparse-index-documents", 1024L,
       bench_pouch_query_eq_sparse_index_documents},
      {"pouch-query-eq-sparse-scan-documents", 1024L,
       bench_pouch_query_eq_sparse_scan_documents},
      {"pouch-query-eq-dense-index-keys", 1024L,
       bench_pouch_query_eq_dense_index_keys},
      {"pouch-query-eq-dense-scan-keys", 1024L,
       bench_pouch_query_eq_dense_scan_keys},
      {"pouch-query-eq-dense-index-documents", 1024L,
       bench_pouch_query_eq_dense_index_documents},
      {"pouch-query-eq-dense-scan-documents", 1024L,
       bench_pouch_query_eq_dense_scan_documents},
      {"pouch-query-range-half-index-keys", 1024L,
       bench_pouch_query_range_half_index_keys},
      {"pouch-query-range-half-scan-keys", 1024L,
       bench_pouch_query_range_half_scan_keys},
      {"pouch-query-range-half-index-documents", 1024L,
       bench_pouch_query_range_half_index_documents},
      {"pouch-query-range-half-scan-documents", 1024L,
       bench_pouch_query_range_half_scan_documents},
      {"pouch-query-in-region-single-index-keys", 1024L,
       bench_pouch_query_in_region_single_index_keys},
      {"pouch-query-in-region-single-scan-keys", 1024L,
       bench_pouch_query_in_region_single_scan_keys},
      {"pouch-query-in-region-single-index-documents", 1024L,
       bench_pouch_query_in_region_single_index_documents},
      {"pouch-query-in-region-single-scan-documents", 1024L,
       bench_pouch_query_in_region_single_scan_documents},
      {"pouch-query-in-tags-index-keys", 1024L,
       bench_pouch_query_in_tags_index_keys},
      {"pouch-query-in-tags-scan-keys", 1024L,
       bench_pouch_query_in_tags_scan_keys},
      {"pouch-query-in-tags-index-documents", 1024L,
       bench_pouch_query_in_tags_index_documents},
      {"pouch-query-in-tags-scan-documents", 1024L,
       bench_pouch_query_in_tags_scan_documents},
      {"pouch-query-contains-message-index-keys", 1024L,
       bench_pouch_query_contains_message_index_keys},
      {"pouch-query-contains-message-scan-keys", 1024L,
       bench_pouch_query_contains_message_scan_keys},
      {"pouch-query-contains-message-index-documents", 1024L,
       bench_pouch_query_contains_message_index_documents},
      {"pouch-query-contains-message-scan-documents", 1024L,
       bench_pouch_query_contains_message_scan_documents},
      {"pouch-query-date-after-index-keys", 1024L,
       bench_pouch_query_date_after_index_keys},
      {"pouch-query-date-after-scan-keys", 1024L,
       bench_pouch_query_date_after_scan_keys},
      {"pouch-query-date-after-index-documents", 1024L,
       bench_pouch_query_date_after_index_documents},
      {"pouch-query-date-after-scan-documents", 1024L,
       bench_pouch_query_date_after_scan_documents},
      {"pouch-query-or-sparse-or-flag-index-keys", 1024L,
       bench_pouch_query_or_sparse_or_flag_index_keys},
      {"pouch-query-or-sparse-or-flag-scan-keys", 1024L,
       bench_pouch_query_or_sparse_or_flag_scan_keys},
      {"pouch-query-or-sparse-or-flag-index-documents", 1024L,
       bench_pouch_query_or_sparse_or_flag_index_documents},
      {"pouch-query-or-sparse-or-flag-scan-documents", 1024L,
       bench_pouch_query_or_sparse_or_flag_scan_documents},
      {"pouch-query-iprefix-index-keys", 1024L,
       bench_pouch_query_iprefix_index_keys},
      {"pouch-query-iprefix-scan-keys", 1024L,
       bench_pouch_query_iprefix_scan_keys},
      {"pouch-query-iprefix-index-documents", 1024L,
       bench_pouch_query_iprefix_index_documents},
      {"pouch-query-iprefix-scan-documents", 1024L,
       bench_pouch_query_iprefix_scan_documents},
      {"pouch-query-icontains-index-keys", 1024L,
       bench_pouch_query_icontains_index_keys},
      {"pouch-query-icontains-scan-keys", 1024L,
       bench_pouch_query_icontains_scan_keys},
      {"pouch-query-icontains-index-documents", 1024L,
       bench_pouch_query_icontains_index_documents},
      {"pouch-query-icontains-scan-documents", 1024L,
       bench_pouch_query_icontains_scan_documents},
      {"pouch-query-recursive-exists-index-keys", 1024L,
       bench_pouch_query_recursive_exists_index_keys},
      {"pouch-query-recursive-exists-scan-keys", 1024L,
       bench_pouch_query_recursive_exists_scan_keys},
      {"pouch-query-recursive-exists-index-documents", 1024L,
       bench_pouch_query_recursive_exists_index_documents},
      {"pouch-query-recursive-exists-scan-documents", 1024L,
       bench_pouch_query_recursive_exists_scan_documents},
      {NULL, 0L, NULL}};

  return cases;
}

static void bench_usage(const char *argv0) {
  const bench_case *bench;

  fprintf(stderr, "usage: %s [iterations] [all", argv0);
  for (bench = bench_cases(); bench->name != NULL; ++bench) {
    fprintf(stderr, "|%s", bench->name);
  }
  fprintf(stderr, "]\n");
}

static const bench_case *bench_find(const char *name) {
  const bench_case *bench;

  for (bench = bench_cases(); bench->name != NULL; ++bench) {
    if (strcmp(bench->name, name) == 0) {
      return bench;
    }
  }
  return NULL;
}

static int bench_run_one(const bench_case *bench, long iterations) {
  double start;
  double elapsed;
  int rc;

  start = bench_now_seconds();
  rc = bench->run(iterations);
  elapsed = bench_now_seconds() - start;
  printf("%s iterations=%ld seconds=%.6f per_op_us=%.3f rc=%d\n", bench->name,
         iterations, elapsed,
         iterations > 0L ? (elapsed * 1000000.0) / (double)iterations : 0.0,
         rc);
  return rc;
}

int main(int argc, char **argv) {
  const bench_case *bench;
  long iterations;
  const char *name;
  int failed;

  if (argc > 1 &&
      (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)) {
    bench_usage(argv[0]);
    return 0;
  }
  iterations = argc > 1 ? strtol(argv[1], NULL, 10) : 0L;
  name = argc > 2 ? argv[2] : "all";
  failed = 0;
  if (!getenv("LOCKDC_BENCH_DIRECTORY"))
    lc_test_tmp_cleanup_stale("/tmp", "liblockdc-pouch-bench-",
                              BENCH_POUCH_TMP_PREFIX);

  if (strcmp(name, "all") == 0) {
    for (bench = bench_cases(); bench->name != NULL; ++bench) {
      long selected_iterations;

      selected_iterations =
          iterations > 0L ? iterations : bench->default_iterations;
      if (bench_run_one(bench, selected_iterations) != 0) {
        failed = 1;
      }
    }
    return failed ? 1 : 0;
  }

  bench = bench_find(name);
  if (bench == NULL) {
    bench_usage(argv[0]);
    return 1;
  }
  return bench_run_one(bench, iterations > 0L ? iterations
                                              : bench->default_iterations);
}
