#include "../support/lc_test_tmp.h"
#include "lc_allocation_tracking.h"
#include "lc_api_internal.h"
#include "lc_mutate_stream.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHUNK_BYTES 65536U
#define SCRATCH_BYTES 16384U
#define TMP_PREFIX "/tmp/liblockdc-stream-scratch-"
#define CHECK(expr)                                                            \
  do {                                                                         \
    if (!(expr)) {                                                             \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr);               \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

typedef struct scratch_probe {
  void *live[8];
  size_t calls, releases, failures, fail_at, chunk_bytes;
} scratch_probe;

static void *(*real_malloc)(size_t);
static void *(*real_realloc)(void *, size_t);
static void (*real_free)(void *);
static scratch_probe builtin;

/* Observe only scratch allocations, leaving the normal tracker in control of
 * ownership. Failure injection is local to this serial test process. */
static void *probe_malloc(void *context, size_t size) {
  scratch_probe *probe = context;
  void *pointer;
  size_t i;
  size_t chunk = probe->chunk_bytes ? probe->chunk_bytes : SCRATCH_BYTES;
  if (size == chunk) {
    ++probe->calls;
    if (probe->calls == probe->fail_at) {
      ++probe->failures;
      return NULL;
    }
  }
  pointer = real_malloc(size);
  if (size == chunk && pointer != NULL) {
    for (i = 0; i < 8U && probe->live[i] != NULL; ++i) {
    }
    CHECK(i < 8U);
    probe->live[i] = pointer;
  }
  return pointer;
}

static void probe_free(void *context, void *pointer) {
  scratch_probe *probe = context;
  size_t i;
  if (pointer != NULL) {
    for (i = 0; i < 8U; ++i) {
      if (probe->live[i] == pointer) {
        probe->live[i] = NULL;
        ++probe->releases;
        break;
      }
    }
  }
  real_free(pointer);
}

static void *builtin_malloc(size_t size) {
  return probe_malloc(&builtin, size);
}
static void builtin_free(void *pointer) { probe_free(&builtin, pointer); }
static void *probe_realloc(void *context, void *pointer, size_t size) {
  (void)context;
  return real_realloc(pointer, size);
}
static void check_released(scratch_probe *probe) {
  size_t i;
  CHECK(probe->calls == probe->releases + probe->failures);
  for (i = 0; i < 8U; ++i)
    CHECK(probe->live[i] == NULL);
}
static void reset_probe(scratch_probe *probe, size_t fail_at) {
  size_t chunk = probe->chunk_bytes;
  check_released(probe);
  memset(probe, 0, sizeof(*probe));
  probe->fail_at = fail_at;
  probe->chunk_bytes = chunk;
}

typedef struct copy_job {
  lc_source *inner;
  lc_sink *sink;
  size_t emitted;
  size_t copies;
  int fail_sink;
} copy_job;

/* A modest callback workspace must coexist with two active SDK copies on a
 * default musl worker. The old two 64 KiB local buffers exhaust that stack. */
static int nested_copy(lc_sink *self, const void *bytes, size_t count,
                       lc_error *error) {
  volatile unsigned char workspace[4096];
  copy_job *job = self->impl;
  size_t written = 0;
  int rc;
  workspace[0] = 17;
  workspace[4095] = 29;
  (void)bytes;
  if (job->fail_sink && job->emitted != 0U)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "injected sink error",
                        NULL, NULL, NULL) == LC_OK;
  CHECK(job->inner->reset(job->inner, error) == LC_OK);
  rc = lc_copy(job->inner, job->sink, &written, error);
  if (rc != LC_OK)
    return 0;
  CHECK(written == 8192U);
  CHECK(workspace[0] == 17 && workspace[4095] == 29);
  job->emitted += count;
  ++job->copies;
  return 1;
}

static size_t failing_read(void *context, void *buffer, size_t count,
                           lc_error *error) {
  (void)context;
  (void)buffer;
  (void)count;
  lc_error_set(error, LC_ERR_TRANSPORT, 0L, "injected source error", NULL, NULL,
               NULL);
  return 0;
}

static size_t failing_after_bytes(void *context, void *buffer, size_t count,
                                  lc_error *error) {
  size_t *remaining = context;
  size_t got = count < *remaining ? count : *remaining;
  if (got == 0U)
    return failing_read(NULL, buffer, count, error);
  memset(buffer, 'x', got);
  *remaining -= got;
  return got;
}

static void test_copies(void) {
  unsigned char *payload = malloc(CHUNK_BYTES * 3U + 37U);
  lc_source *source;
  lc_sink output;
  copy_job job;
  lc_error error;
  size_t written, failure, remaining;
  CHECK(payload != NULL);
  memset(payload, 'x', CHUNK_BYTES * 3U + 37U);
  memset(&job, 0, sizeof(job));
  memset(&output, 0, sizeof(output));
  lc_error_init(&error);
  CHECK(lc_source_from_memory(payload, CHUNK_BYTES * 3U + 37U, &source,
                              &error) == LC_OK);
  CHECK(lc_source_from_memory(payload, 8192U, &job.inner, &error) == LC_OK);
  CHECK(lc_sink_to_discard(&job.sink, &error) == LC_OK);
  output.write = nested_copy;
  output.impl = &job;
  for (failure = 0; failure < 4U; ++failure) {
    CHECK(source->reset(source, &error) == LC_OK);
    reset_probe(&builtin, failure);
    written = 0;
    CHECK(lc_copy(source, &output, &written, &error) ==
          (failure == 0U ? LC_OK : LC_ERR_NOMEM));
    if (failure == 0U) {
      CHECK(written == CHUNK_BYTES * 3U + 37U);
      CHECK(job.emitted == written);
      CHECK(builtin.calls == job.copies + 1U);
    }
    check_released(&builtin);
    lc_error_cleanup(&error);
  }
  reset_probe(&builtin, 0);
  job.fail_sink = 1;
  job.emitted = 0;
  CHECK(source->reset(source, &error) == LC_OK);
  CHECK(lc_copy(source, &output, NULL, &error) == LC_ERR_TRANSPORT);
  check_released(&builtin);
  lc_source_close(source);
  remaining = CHUNK_BYTES + 4096U;
  CHECK(lc_source_from_callbacks(failing_after_bytes, NULL, NULL, &remaining,
                                 &source, &error) == LC_OK);
  CHECK(lc_copy(source, job.sink, NULL, &error) == LC_ERR_TRANSPORT);
  check_released(&builtin);
  lc_source_close(source);
  lc_source_close(job.inner);
  lc_sink_close(job.sink);
  lc_error_cleanup(&error);
  free(payload);
}

typedef struct file_input {
  const char *text;
  int fail_read;
  size_t closes;
} file_input;
static void file_close(void *context) { ++((file_input *)context)->closes; }
static int file_open(void *context, const char *path, lc_source **out,
                     lc_error *error) {
  file_input *input = context;
  (void)path;
  if (input->fail_read)
    return lc_source_from_callbacks(failing_read, NULL, file_close, input, out,
                                    error);
  return lc_source_from_memory(input->text, strlen(input->text), out, error);
}

static void test_mutations(void) {
  const char *expr;
  const char *expected;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_file_value_resolver resolver;
  file_input value;
  lc_error error;
  FILE *input, *output;
  char bytes[128];
  size_t mode, failure, got;
  int rc;
  memset(&value, 0, sizeof(value));
  resolver.open = file_open;
  resolver.context = &value;
  memset(&options, 0, sizeof(options));
  options.file_value_resolver = &resolver;
  options.file_value_base_dir = "/virtual";
  lc_error_init(&error);
  /* NUL/incomplete UTF-8 handling is additionally covered by the existing
   * mutation suite; these paths exercise cleanup after allocation. */
  for (mode = 0; mode < 4U; ++mode) {
    expr = mode == 0U ? "rm:/absent" : "textfile:/value=input";
    expected = mode == 0U ? "{\"keep\":1}" : "{\"keep\":1,\"value\":\"text\"}";
    value.text = mode == 2U ? "\xc3" : "text";
    value.fail_read = mode == 3U;
    CHECK(lc_mutation_plan_build(&expr, 1U, &options, &plan, &error) == LC_OK);
    for (failure = 0; failure < 2U; ++failure) {
      input = tmpfile();
      CHECK(input != NULL);
      CHECK(fwrite("{\"keep\":1}", 1U, 10U, input) == 10U);
      CHECK(fflush(input) == 0);
      output = NULL;
      reset_probe(&builtin, failure);
      rc = lc_mutation_plan_apply(plan, input, &output, &error);
      CHECK(rc == (failure      ? LC_ERR_NOMEM
                   : mode == 2U ? LC_ERR_INVALID
                   : mode == 3U ? LC_ERR_TRANSPORT
                                : LC_OK));
      CHECK(builtin.calls == 1U);
      check_released(&builtin);
      if (rc == LC_OK) {
        rewind(output);
        got = fread(bytes, 1U, sizeof(bytes) - 1U, output);
        bytes[got] = '\0';
        CHECK(strcmp(bytes, expected) == 0);
        CHECK(fclose(output) == 0);
      } else
        CHECK(output == NULL);
      CHECK(fclose(input) == 0);
      lc_error_cleanup(&error);
    }
    lc_mutation_plan_close(plan);
  }
  CHECK(value.closes == 2U);
}

static int count_key(void *context, const char *bytes, size_t count,
                     lc_error *error) {
  (void)bytes;
  (void)count;
  (void)error;
  ++*(size_t *)context;
  return 1;
}

static int reject_write(lc_sink *self, const void *bytes, size_t count,
                        lc_error *error) {
  (void)self;
  (void)bytes;
  (void)count;
  lc_error_set(error, LC_ERR_TRANSPORT, 0L, "injected query sink error", NULL,
               NULL, NULL);
  return 0;
}

static void test_queries(void) {
  scratch_probe custom;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *client;
  lc_source *source;
  lc_query_req request;
  lc_query_res result;
  lc_query_key_handler handler;
  lc_pouch_state_write_result written;
  lc_error error;
  lc_sink rejected;
  char root[512], endpoint[640], key[16];
  char template_path[] = TMP_PREFIX "XXXXXX";
  const char *endpoints[1];
  size_t i, failure, keys;
  memset(&custom, 0, sizeof(custom));
  custom.chunk_bytes = SCRATCH_BYTES;
  lc_error_init(&error);
  CHECK(lc_test_tmp_mkdtemp(template_path, root, sizeof(root), TMP_PREFIX));
  snprintf(endpoint, sizeof(endpoint), "pouch://%s/store", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_QUERY_INDEXING;
  settings.background_compaction_enabled = 0;
  settings.query_indexing_enabled = 0;
  config.pouch_settings = &settings;
  config.allocator.context = &custom;
  config.allocator.malloc_fn = probe_malloc;
  config.allocator.free_fn = probe_free;
  /* A full hook set leaves scratch sizes visible to this test. */
  config.allocator.realloc_fn = probe_realloc;
  CHECK(lc_client_open(&config, &client, &error) == LC_OK);
  for (i = 0; i < 3U; ++i) {
    snprintf(key, sizeof(key), "doc/%u", (unsigned)i);
    memset(&written, 0, sizeof(written));
    CHECK(lc_source_from_memory("{\"n\":1,\"flag\":true}", 19U, &source,
                                &error) == LC_OK);
    CHECK(lc_pouch_state_write(((lc_client_handle *)client)->pouch, "docs", key,
                               source, NULL, &written, &error) == LC_OK);
    lc_source_close(source);
    lc_pouch_state_write_result_cleanup(&config.allocator, &written);
  }
  memset(&request, 0, sizeof(request));
  request.ns = "docs";
  request.engine = "scan";
  /* A conjunction uses the generic LQL streaming path, not scalar fast paths.
   */
  request.selector_json =
      "{\"and\":[{\"eq\":{\"field\":\"/n\",\"value\":1}},{\"eq\":{\"field\":\"/"
      "flag\",\"value\":true}}]}";
  memset(&handler, 0, sizeof(handler));
  handler.chunk = count_key;
  for (failure = 0; failure < 2U; ++failure) {
    reset_probe(&custom, failure);
    memset(&result, 0, sizeof(result));
    keys = 0;
    CHECK(client->query_keys(client, &request, &handler, &keys, &result,
                             &error) == (failure ? LC_ERR_NOMEM : LC_OK));
    CHECK(custom.calls == 1U);
    CHECK(keys == (failure ? 0U : 3U));
    check_released(&custom);
    lc_query_res_cleanup(&result);
    lc_error_cleanup(&error);
  }
  reset_probe(&custom, 0);
  memset(&rejected, 0, sizeof(rejected));
  rejected.write = reject_write;
  memset(&result, 0, sizeof(result));
  CHECK(client->query(client, &request, &rejected, &result, &error) ==
        LC_ERR_TRANSPORT);
  CHECK(custom.calls == 1U);
  check_released(&custom);
  lc_query_res_cleanup(&result);
  lc_client_close(client);
  check_released(&custom);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, TMP_PREFIX);
}

static void *run_worker(void *context) {
  const char *mode = context;
  if (strcmp(mode, "all") == 0 || strcmp(mode, "copy") == 0)
    test_copies();
  if (strcmp(mode, "all") == 0 || strcmp(mode, "mutation") == 0) {
    reset_probe(&builtin, 0);
    test_mutations();
  }
  if (strcmp(mode, "all") == 0 || strcmp(mode, "query") == 0) {
    reset_probe(&builtin, 0);
    test_queries();
  }
  check_released(&builtin);
  return NULL;
}

int main(int argc, char **argv) {
  pthread_t worker;
  lc_error error;
  const char *mode = argc == 1 ? "all" : argv[1];
  CHECK(argc <= 2 &&
        (strcmp(mode, "all") == 0 || strcmp(mode, "copy") == 0 ||
         strcmp(mode, "mutation") == 0 || strcmp(mode, "query") == 0));
  lc_error_init(&error);
  CHECK(lc_allocator_tracking_configure(1, &error) == LC_OK);
  real_malloc = lc_builtin_malloc;
  real_realloc = lc_builtin_realloc;
  real_free = lc_builtin_free;
  lc_builtin_malloc = builtin_malloc;
  lc_builtin_free = builtin_free;
  /* NULL attributes deliberately retain musl's default worker stack. */
  CHECK(pthread_create(&worker, NULL, run_worker, (void *)mode) == 0);
  CHECK(pthread_join(worker, NULL) == 0);
  lc_builtin_malloc = real_malloc;
  lc_builtin_free = real_free;
  check_released(&builtin);
  lc_error_cleanup(&error);
  puts(
      "default-stack streaming scratch, allocator failures and cleanup passed");
  return 0;
}
