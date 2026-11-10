#include <errno.h>
#include <pthread.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "../support/lc_test_outbox_hooks.h"
#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_pouch.h"

#define POUCH_E2E_TMP_PREFIX LC_E2E_TMP_ROOT "/liblockdc-e2e-pouch-"
#define LOCAL_MUTATE_TMP_PREFIX LC_E2E_TMP_ROOT "/liblockdc-local-mutate-"
#define S3_LOCAL_MUTATE_TMP_PREFIX LC_E2E_TMP_ROOT "/liblockdc-s3-local-mutate-"

typedef struct e2e_status_doc {
  char *status;
} e2e_status_doc;

static const lonejson_field e2e_status_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(e2e_status_doc, status, "status")};

LONEJSON_MAP_DEFINE(e2e_status_map, e2e_status_doc, e2e_status_fields);

static void assert_lc_ok(int rc, lc_error *error);
static void assert_lc_server_error(int rc, lc_error *error, long http_status);
static int buffer_contains(const void *bytes, size_t length,
                           const char *needle);
static lonejson *e2e_lonejson_runtime(void);
static void e2e_lonejson_cleanup(const lonejson_map *map, void *value);

static lonejson *e2e_runtime_instance;
static pthread_once_t e2e_runtime_once = PTHREAD_ONCE_INIT;

static void e2e_lonejson_runtime_init(void) {
  lonejson_error error;

  lonejson_error_init(&error);
  e2e_runtime_instance = lonejson_new(NULL, &error);
}

static lonejson *e2e_lonejson_runtime(void) {
  (void)pthread_once(&e2e_runtime_once, e2e_lonejson_runtime_init);
  return e2e_runtime_instance;
}

static void e2e_lonejson_cleanup(const lonejson_map *map, void *value) {
  lonejson *runtime;

  runtime = e2e_lonejson_runtime();
  assert_non_null(runtime);
  assert_int_equal(runtime->cleanup(runtime, map, value, NULL),
                   LONEJSON_STATUS_OK);
}

static void save_json_text_or_die(lc_lease *lease, const char *json_text,
                                  lc_error *error) {
  lc_source *src;
  int rc;

  src = NULL;
  rc = lc_source_from_memory(json_text, strlen(json_text), &src, error);
  assert_lc_ok(rc, error);
  rc = lease->update(lease, src, NULL, error);
  lc_source_close(src);
  assert_lc_ok(rc, error);
}

typedef struct acquire_for_update_e2e_state {
  const char *expected;
  int saw_snapshot;
} acquire_for_update_e2e_state;

typedef struct queue_watch_e2e_context {
  const char *endpoint;
  const char *bundle_path;
  const char *queue_name;
  char queue_name_storage[128];
  int event_count;
  int unavailable_count;
  int available_count;
  int saw_available;
  int saw_unavailable;
  int stop_after_initial_unavailable;
  int stop_after_available;
  int stop_after_unavailable_after_available;
  char event_queue[128];
  char head_message_id[128];
  char unavailable_head_message_id[128];
  char correlation_id[128];
  int enqueue_rc;
  int dequeue_rc;
  lc_error enqueue_error;
  lc_error dequeue_error;
} queue_watch_e2e_context;

typedef struct query_keys_e2e_capture {
  const char *key_prefix;
  size_t key_prefix_len;
  size_t expected_count;
  size_t begin_calls;
  size_t chunk_calls;
  size_t end_calls;
  size_t total_bytes;
  char current[160];
  size_t current_length;
  unsigned char seen[2048];
} query_keys_e2e_capture;

typedef struct pouch_e2e_key_count {
  size_t rows;
} pouch_e2e_key_count;

typedef struct pouch_e2e_state_entry_count {
  const char *key_prefix;
  size_t count;
} pouch_e2e_state_entry_count;

enum {
  POUCH_E2E_HARDENING_CHURN_ROWS = 1024,
  POUCH_E2E_HARDENING_MULTI_SEGMENT_ROWS = 128,
  POUCH_E2E_HARDENING_OUTBOX_EFFECTS = 32,
  POUCH_E2E_HARDENING_RETENTION_ROWS = 16,
  /* Keep this below one machine word: child results encode exactly-once
   * terminal delivery as a compact bit set sent through a pipe. */
  POUCH_E2E_SHARED_OUTBOX_ROWS = 24
};

#define POUCH_E2E_HARDENING_SEGMENT_BYTES (64UL * 1024UL * 1024UL)
#define POUCH_E2E_HARDENING_MULTI_SEGMENT_BYTES 4096UL

typedef struct pouch_e2e_churn_child_result {
  int rc;
  int stage;
  int compacted;
  unsigned long candidate_segment_count;
  int multi_segment_compacted;
  unsigned long multi_segment_candidate_count;
  char error_message[256];
} pouch_e2e_churn_child_result;

typedef struct pouch_e2e_dispatcher_hold {
  lc_outbox_dispatcher *dispatcher;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int claimed;
  int release;
  int completed;
  int rc;
  char error_message[256];
} pouch_e2e_dispatcher_hold;

typedef struct pouch_e2e_outbox_child_result {
  unsigned long delivered_mask;
  size_t delivered;
  int rc;
  int stage;
  char error_message[256];
} pouch_e2e_outbox_child_result;

typedef struct pouch_e2e_outbox_child {
  pid_t pid;
  int ready_read;
  int start_write;
  int result_read;
  int claimed_read;
  int release_write;
} pouch_e2e_outbox_child;

typedef enum pouch_e2e_command_child_mode {
  POUCH_E2E_COMMAND_CHILD_COMPLETE = 0,
  POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_FOREIGN_EFFECT = 1,
  POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_COMMAND_COMMIT = 2
} pouch_e2e_command_child_mode;

typedef struct pouch_e2e_command_child_result {
  int rc;
  int stage;
  int performed_foreign_effect;
  int saw_terminal_command;
  char command_id[48];
  char error_message[256];
} pouch_e2e_command_child_result;

typedef struct pouch_e2e_command_child {
  pid_t pid;
  int ready_read;
  int start_write;
  int result_read;
  int phase_read;
  int release_write;
} pouch_e2e_command_child;

typedef struct pouch_e2e_command_waiter_result {
  int rc;
  int state;
  char error_message[256];
} pouch_e2e_command_waiter_result;

typedef struct pouch_e2e_command_waiter {
  pid_t pid;
  int ready_read;
  int pending_read;
  int result_read;
} pouch_e2e_command_waiter;

typedef struct pouch_e2e_command_wait_hook {
  int pending_fd;
  int signaled;
} pouch_e2e_command_wait_hook;

static int query_keys_e2e_begin(void *context, lc_error *error) {
  query_keys_e2e_capture *capture;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  capture->begin_calls += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int query_keys_e2e_chunk(void *context, const char *bytes, size_t len,
                                lc_error *error) {
  query_keys_e2e_capture *capture;
  size_t available;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  capture->chunk_calls += 1U;
  capture->total_bytes += len;
  available = sizeof(capture->current) - capture->current_length - 1U;
  assert_true(len <= available);
  memcpy(capture->current + capture->current_length, bytes, len);
  capture->current_length += len;
  capture->current[capture->current_length] = '\0';
  return 1;
}

static int query_keys_e2e_end(void *context, lc_error *error) {
  query_keys_e2e_capture *capture;
  const char *suffix;
  char *end;
  unsigned long index;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  assert_true(capture->current_length > capture->key_prefix_len);
  assert_int_equal(
      strncmp(capture->current, capture->key_prefix, capture->key_prefix_len),
      0);
  suffix = capture->current + capture->key_prefix_len;
  index = strtoul(suffix, &end, 10);
  assert_non_null(end);
  assert_int_equal(*end, '\0');
  assert_true(index < capture->expected_count);
  assert_int_equal(capture->seen[index], 0U);
  capture->seen[index] = 1U;
  capture->end_calls += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

/* A synchronous Pouch index boundary may bootstrap an absent derived view or
 * repair a manifest retired by shared-root compaction.  Both outcomes make
 * the exact same current-index guarantee; callers must not infer a stronger
 * lifecycle distinction from this diagnostic. */
static void
pouch_e2e_assert_current_index_flush(lc_index_flush_res *flush_res) {
  int current;

  current = flush_res != NULL && flush_res->accepted && flush_res->flushed &&
            !flush_res->pending && flush_res->flush_id != NULL &&
            (strcmp(flush_res->flush_id, "pouch-query-index-flush") == 0 ||
             strcmp(flush_res->flush_id, "pouch-query-index-repair") == 0);
  lc_index_flush_res_cleanup(flush_res);
  assert_true(current);
}

static int pouch_e2e_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int pouch_e2e_key_chunk(void *context, const char *bytes, size_t len,
                               lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int pouch_e2e_key_end(void *context, lc_error *error) {
  pouch_e2e_key_count *count;

  (void)error;
  count = (pouch_e2e_key_count *)context;
  count->rows++;
  return 1;
}

static int pouch_e2e_count_state_entry(const lc_pouch_state_visit_entry *entry,
                                       void *context, lc_error *error) {
  pouch_e2e_state_entry_count *count;

  (void)error;
  count = (pouch_e2e_state_entry_count *)context;
  if (entry->has_payload &&
      strncmp(entry->key, count->key_prefix, strlen(count->key_prefix)) == 0) {
    count->count++;
  }
  return LC_OK;
}

static int acquire_for_update_e2e_handler(void *context,
                                          lc_acquire_for_update_context *update,
                                          lc_error *error) {
  acquire_for_update_e2e_state *state;
  lc_sink *sink;
  lc_source *src;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  state = (acquire_for_update_e2e_state *)context;
  sink = NULL;
  src = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  assert_true(update->state.has_state);
  assert_non_null(update->state.reader);
  assert_true(update->state.version >= 1L);
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lc_copy(update->state.reader, sink, &written, error);
  assert_lc_ok(rc, error);
  assert_true(written > 0U);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, state->expected));
  state->saw_snapshot = 1;
  lc_sink_close(sink);

  rc = lc_source_from_memory(
      "{\"value\":2,\"via\":\"acquire-for-update\"}",
      strlen("{\"value\":2,\"via\":\"acquire-for-update\"}"), &src, error);
  assert_lc_ok(rc, error);
  rc = update->lease->update(update->lease, src, NULL, error);
  lc_source_close(src);
  return rc;
}

static int acquire_for_update_failing_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  lc_source *src;
  int rc;

  (void)context;
  src = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  rc = lc_source_from_memory(
      "{\"value\":3,\"via\":\"failed-acquire-for-update\"}",
      strlen("{\"value\":3,\"via\":\"failed-acquire-for-update\"}"), &src,
      error);
  assert_lc_ok(rc, error);
  rc = update->lease->update(update->lease, src, NULL, error);
  lc_source_close(src);
  assert_lc_ok(rc, error);
  if (error != NULL) {
    error->code = LC_ERR_INVALID;
    error->message = strdup("intentional acquire_for_update handler failure");
    assert_non_null(error->message);
  }
  return LC_ERR_INVALID;
}

static void write_temp_file_or_die(char *template_path,
                                   const char *allowed_prefix,
                                   const unsigned char *bytes, size_t length) {
  int fd;
  FILE *fp;

  fd = lc_test_tmp_mkstemp(template_path, allowed_prefix);
  assert_true(fd >= 0);
  fp = fdopen(fd, "wb");
  assert_non_null(fp);
  assert_int_equal(fwrite(bytes, 1U, length, fp), (int)length);
  assert_int_equal(fclose(fp), 0);
}

static const char *env_or_default(const char *name, const char *fallback) {
  const char *value;

  value = getenv(name);
  if (value == NULL || value[0] == '\0') {
    return fallback;
  }
  return value;
}

static int file_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int socket_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISSOCK(st.st_mode);
}

static void require_file_or_skip(const char *path) {
  if (!file_exists(path)) {
    skip();
  }
}

static void require_socket_or_skip(const char *path) {
  if (!socket_exists(path)) {
    skip();
  }
}

static void assert_lc_ok(int rc, lc_error *error) {
  if (rc != LC_OK) {
    print_message(
        "lc error: code=%d http=%ld message=%s detail=%s server_code=%s "
        "correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_equal(rc, LC_OK);
}

static void assert_lc_server_error(int rc, lc_error *error, long http_status) {
  if (rc == LC_OK) {
    print_message("expected server error, got success\n");
  } else {
    print_message(
        "expected server error: code=%d http=%ld message=%s detail=%s "
        "server_code=%s correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_not_equal(rc, LC_OK);
  assert_non_null(error);
  assert_int_equal(error->code, LC_ERR_SERVER);
  if (http_status > 0L) {
    assert_int_equal(error->http_status, http_status);
  } else {
    assert_true(error->http_status >= 400L);
  }
}

static int buffer_contains(const void *bytes, size_t length,
                           const char *needle) {
  const unsigned char *cursor;
  size_t needle_length;
  size_t i;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_length = strlen(needle);
  if (needle_length == 0U || length < needle_length) {
    return 0;
  }
  cursor = (const unsigned char *)bytes;
  for (i = 0U; i + needle_length <= length; ++i) {
    if (memcmp(cursor + i, needle, needle_length) == 0) {
      return 1;
    }
  }
  return 0;
}

static void assert_local_mutate_file_variants(lc_lease *lease,
                                              const char *text_path,
                                              const char *binary_path,
                                              lc_error *error) {
  const char *mutations[9];
  const char *failure_mutations[1];
  lc_mutate_local_req mutate_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char text_mutation[512];
  char base64_mutation[512];
  char auto_text_mutation[512];
  char auto_bin_mutation[512];
  char missing_file_mutation[512];
  char missing_path[] =
      LC_E2E_TMP_ROOT "/liblockdc-local-mutate-missing-XXXXXX";
  int missing_fd;
  int rc;

  lc_mutate_local_req_init(&mutate_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  mutate_req.file_value_base_dir = NULL;

  mutations[0] = "/filename=\"blob.txt\"";
  mutations[1] = "/counter=2";
  mutations[2] = "rm:/remove_me";
  mutations[3] = "time:/ts=NOW";
  snprintf(text_mutation, sizeof(text_mutation), "textfile:/text_blob=%s",
           text_path);
  mutations[4] = text_mutation;
  snprintf(base64_mutation, sizeof(base64_mutation), "base64file:/bin_blob=%s",
           binary_path);
  mutations[5] = base64_mutation;
  snprintf(auto_text_mutation, sizeof(auto_text_mutation), "file:/auto_text=%s",
           text_path);
  mutations[6] = auto_text_mutation;
  snprintf(auto_bin_mutation, sizeof(auto_bin_mutation), "file:/auto_bin=%s",
           binary_path);
  mutations[7] = auto_bin_mutation;
  mutations[8] = "rm:/optional/missing";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 9U;
  rc = lease->mutate_local(lease, &mutate_req, error);
  assert_lc_ok(rc, error);
  assert_non_null(lease->state_etag);

  sink = NULL;
  bytes = NULL;
  length = 0U;
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lease->get(lease, sink, &get_opts, &get_res, error);
  assert_lc_ok(rc, error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"counter\":3"));
  assert_true(buffer_contains(bytes, length,
                              "\"text_blob\":\"hello\\n\\\"quoted\\\"\""));
  assert_true(buffer_contains(bytes, length, "\"bin_blob\":\"AAECYQ==\""));
  assert_true(buffer_contains(bytes, length,
                              "\"auto_text\":\"hello\\n\\\"quoted\\\"\""));
  assert_true(buffer_contains(bytes, length, "\"auto_bin\":\"AAECYQ==\""));
  assert_true(buffer_contains(bytes, length, "\"ts\":\""));
  assert_false(buffer_contains(bytes, length, "\"remove_me\""));
  assert_false(buffer_contains(bytes, length, "\"optional\""));
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  missing_fd = lc_test_tmp_mkstemp(missing_path, LOCAL_MUTATE_TMP_PREFIX);
  assert_true(missing_fd >= 0);
  assert_int_equal(close(missing_fd), 0);
  assert_int_equal(unlink(missing_path), 0);
  lc_test_tmp_untrack_path(missing_path);
  snprintf(missing_file_mutation, sizeof(missing_file_mutation),
           "base64file:/missing=%s", missing_path);
  failure_mutations[0] = missing_file_mutation;
  lc_mutate_local_req_init(&mutate_req);
  mutate_req.mutations = failure_mutations;
  mutate_req.mutation_count = 1U;
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lease->mutate_local(lease, &mutate_req, error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(error->code, LC_ERR_TRANSPORT);
  assert_non_null(error->message);

  sink = NULL;
  bytes = NULL;
  length = 0U;
  memset(&get_res, 0, sizeof(get_res));
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lease->get(lease, sink, &get_opts, &get_res, error);
  assert_lc_ok(rc, error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"counter\":3"));
  assert_false(buffer_contains(bytes, length, "\"missing\""));
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
}

static void make_unique_name(const char *prefix, char *buffer,
                             size_t capacity) {
  long now;
  int pid;

  now = (long)time(NULL);
  pid = (int)getpid();
  snprintf(buffer, capacity, "%s-%ld-%d", prefix, now, pid);
}

static void make_pouch_root(const char *suffix, char *root,
                            size_t root_capacity, char *endpoint,
                            size_t endpoint_capacity) {
  char template_path[512];
  int written;

  written = snprintf(template_path, sizeof(template_path),
                     POUCH_E2E_TMP_PREFIX "%s-XXXXXX", suffix);
  assert_true(written > 0 && (size_t)written < sizeof(template_path));
  assert_true(lc_test_tmp_mkdtemp(template_path, root, root_capacity,
                                  POUCH_E2E_TMP_PREFIX));
  written = snprintf(endpoint, endpoint_capacity, "pouch://%s", root);
  assert_true(written > 0 && (size_t)written < endpoint_capacity);
}

static void cleanup_pouch_root(const char *root) {
  lc_test_tmp_cleanup_path(root, POUCH_E2E_TMP_PREFIX);
}

static void cleanup_all_pouch_roots(void) {
  lc_test_tmp_cleanup_stale(LC_E2E_TMP_ROOT, "liblockdc-e2e-pouch-",
                            POUCH_E2E_TMP_PREFIX);
}

static void pouch_e2e_legacy_append_u16(unsigned char *bytes, size_t *offset,
                                        size_t capacity, size_t value) {
  assert_true(value <= 65535U);
  assert_true(*offset <= capacity - 2U);
  bytes[(*offset)++] = (unsigned char)(value & 0xffU);
  bytes[(*offset)++] = (unsigned char)((value >> 8U) & 0xffU);
}

static void pouch_e2e_legacy_append_i64(unsigned char *bytes, size_t *offset,
                                        size_t capacity, int64_t value) {
  uint64_t raw;
  size_t i;

  assert_true(*offset <= capacity - 8U);
  raw = (uint64_t)value;
  for (i = 0U; i < 8U; ++i) {
    bytes[(*offset)++] = (unsigned char)((raw >> (i * 8U)) & 0xffU);
  }
}

static void pouch_e2e_legacy_append_string(unsigned char *bytes, size_t *offset,
                                           size_t capacity, const char *value) {
  size_t length;

  length = strlen(value);
  pouch_e2e_legacy_append_u16(bytes, offset, capacity, length);
  assert_true(*offset <= capacity - length);
  memcpy(bytes + *offset, value, length);
  *offset += length;
}

static void pouch_e2e_write_legacy_lease(lc_pouch *pouch, const char *key,
                                         int extended, lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  unsigned char metadata[512];
  lc_source *source;
  size_t offset;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  memset(metadata, 0, sizeof(metadata));
  source = NULL;
  offset = 0U;
  memcpy(metadata + offset, "LPL1", 4U);
  offset += 4U;
  pouch_e2e_legacy_append_string(metadata, &offset, sizeof(metadata),
                                 "default");
  pouch_e2e_legacy_append_string(metadata, &offset, sizeof(metadata), key);
  pouch_e2e_legacy_append_string(metadata, &offset, sizeof(metadata),
                                 "legacy-owner");
  pouch_e2e_legacy_append_string(metadata, &offset, sizeof(metadata),
                                 "legacy-lease");
  pouch_e2e_legacy_append_string(metadata, &offset, sizeof(metadata), "");
  pouch_e2e_legacy_append_i64(metadata, &offset, sizeof(metadata), 4L);
  pouch_e2e_legacy_append_i64(metadata, &offset, sizeof(metadata),
                              extended ? (lc_i64)time(NULL) + 300L : 0L);
  if (extended) {
    pouch_e2e_legacy_append_i64(metadata, &offset, sizeof(metadata), 37L);
    metadata[offset++] = 1U;
  }
  options.content_type = "application/x-lockdc-pouch-lease";
  options.has_metadata = 1;
  options.metadata = metadata;
  options.metadata_length = offset;
  rc = lc_source_from_memory("{}", 2U, &source, error);
  assert_lc_ok(rc, error);
  rc = lc_pouch_state_write(pouch, "default", key, source, &options, &result,
                            error);
  lc_source_close(source);
  assert_lc_ok(rc, error);
  lc_pouch_state_write_result_cleanup(NULL, &result);
}

static void pouch_e2e_remove_control_migration_marker(const char *root) {
  char path[1024];
  int written;

  written =
      snprintf(path, sizeof(path), "%s/.lockdc-control-migration-v1", root);
  assert_true(written > 0 && (size_t)written < sizeof(path));
  assert_int_equal(unlink(path), 0);
}

static void pouch_e2e_write_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "wb");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static int setup_pouch_e2e_group(void **state) {
  (void)state;
  cleanup_all_pouch_roots();
  return 0;
}

static int teardown_pouch_e2e_group(void **state) {
  (void)state;
  cleanup_all_pouch_roots();
  return 0;
}

static void open_tcp_client_with_json_response_limit(const char *endpoint,
                                                     const char *bundle_path,
                                                     size_t json_response_limit,
                                                     lc_client **out,
                                                     lc_error *error) {
  lc_client_config config;
  lc_source *bundle_source;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  assert_lc_ok(rc, error);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_source = bundle_source;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  config.insecure_skip_verify = 1;
  config.http_json_response_limit_bytes = json_response_limit;
  rc = lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
  assert_lc_ok(rc, error);
}

static void open_tcp_client(const char *endpoint, const char *bundle_path,
                            lc_client **out, lc_error *error) {
  open_tcp_client_with_json_response_limit(endpoint, bundle_path, 0U, out,
                                           error);
}

static void open_tcp_client_allow_error(const char *endpoint,
                                        const char *bundle_path,
                                        lc_client **out, lc_error *error) {
  lc_client_config config;
  lc_source *bundle_source;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  assert_lc_ok(rc, error);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_source = bundle_source;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  config.insecure_skip_verify = 1;
  *out = NULL;
  lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
}

static void open_pouch_client(const char *endpoint, lc_client **out,
                              lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void open_uds_client(const char *socket_path, lc_client **out,
                            lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = "http://localhost";
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.unix_socket_path = socket_path;
  config.default_namespace = "default";
  config.disable_mtls = 1;
  config.timeout_ms = 5000L;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void test_disk_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"disk\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->ns, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = lc_get_in_namespace(client, "default", key, &get_opts, sink, &get_res,
                           &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"disk\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_server_minted_multikey_xa_transaction(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *commit_a;
  lc_lease *commit_b;
  lc_lease *rollback_a;
  lc_lease *rollback_b;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char commit_key_a[96];
  char commit_key_b[96];
  char rollback_key_a[96];
  char rollback_key_b[96];
  int rc;
  lc_error error;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  commit_a = NULL;
  commit_b = NULL;
  rollback_a = NULL;
  rollback_b = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-xa-server-commit-a", commit_key_a,
                   sizeof(commit_key_a));
  make_unique_name("disk-xa-server-commit-b", commit_key_b,
                   sizeof(commit_key_b));

  acquire_req.key = commit_key_a;
  acquire_req.owner = "lc-e2e-xa";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &commit_a, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(commit_a);
  assert_non_null(commit_a->txn_id);
  assert_true(commit_a->txn_id[0] != '\0');

  acquire_req.key = commit_key_b;
  acquire_req.txn_id = commit_a->txn_id;
  rc = client->acquire(client, &acquire_req, &commit_b, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(commit_b);
  assert_string_equal(commit_b->txn_id, commit_a->txn_id);

  save_json_text_or_die(commit_a, "{\"transaction\":\"commit-a\"}", &error);
  save_json_text_or_die(commit_b, "{\"transaction\":\"commit-b\"}", &error);
  rc = commit_a->release(commit_a, NULL, &error);
  assert_lc_ok(rc, &error);
  commit_a = NULL;
  rc = commit_b->release(commit_b, NULL, &error);
  assert_lc_ok(rc, &error);
  commit_b = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, commit_key_a, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"transaction\":\"commit-a\""));
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, commit_key_b, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"transaction\":\"commit-b\""));
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_acquire_req_init(&acquire_req);
  make_unique_name("disk-xa-server-rollback-a", rollback_key_a,
                   sizeof(rollback_key_a));
  make_unique_name("disk-xa-server-rollback-b", rollback_key_b,
                   sizeof(rollback_key_b));
  acquire_req.key = rollback_key_a;
  acquire_req.owner = "lc-e2e-xa";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &rollback_a, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(rollback_a);
  assert_non_null(rollback_a->txn_id);

  acquire_req.key = rollback_key_b;
  acquire_req.txn_id = rollback_a->txn_id;
  rc = client->acquire(client, &acquire_req, &rollback_b, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(rollback_b);
  assert_string_equal(rollback_b->txn_id, rollback_a->txn_id);

  save_json_text_or_die(rollback_a, "{\"transaction\":\"rollback-a\"}", &error);
  save_json_text_or_die(rollback_b, "{\"transaction\":\"rollback-b\"}", &error);
  release_req.rollback = 1;
  rc = rollback_a->release(rollback_a, &release_req, &error);
  assert_lc_ok(rc, &error);
  rollback_a = NULL;
  rc = rollback_b->release(rollback_b, &release_req, &error);
  assert_lc_ok(rc, &error);
  rollback_b = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, rollback_key_a, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, rollback_key_b, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(get_res.no_content);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_server_explicit_xa_enlists_on_acquire(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *first;
  lc_lease *second;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_error error;
  char txn_id[LC_XID_STRING_SIZE];
  char first_key[96];
  char second_key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  first = NULL;
  second = NULL;
  sink = NULL;
  txn_id[0] = '\0';
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  rc = lc_xid_new(txn_id, &error);
  assert_lc_ok(rc, &error);
  make_unique_name("disk-xa-terminal-enlist-a", first_key, sizeof(first_key));
  make_unique_name("disk-xa-terminal-enlist-b", second_key, sizeof(second_key));

  acquire_req.key = first_key;
  acquire_req.owner = "lc-e2e-xa-terminal-enlist";
  acquire_req.ttl_seconds = 30L;
  acquire_req.txn_id = txn_id;
  rc = client->acquire(client, &acquire_req, &first, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first);
  acquire_req.key = second_key;
  rc = client->acquire(client, &acquire_req, &second, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(second);

  save_json_text_or_die(first, "{\"transaction\":\"first\"}", &error);
  save_json_text_or_die(second, "{\"transaction\":\"second\"}", &error);
  rc = first->release(first, NULL, &error);
  assert_lc_ok(rc, &error);
  first = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, first_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, second_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = second->release(second, NULL, &error);
  assert_lc_ok(rc, &error);
  second = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, second_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_server_metadata_finalization_preserves_staged_version(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_metadata_req metadata_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_error error;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_metadata_req_init(&metadata_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-metadata-staged-version", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-metadata-staged-version";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);
  save_json_text_or_die(lease, "{\"value\":31}", &error);
  assert_int_equal(lease->version, 1L);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 1L);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, 1L);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_acquire_for_update_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  acquire_for_update_e2e_state handler_state;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  memset(&handler_state, 0, sizeof(handler_state));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-acquire-for-update", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-seed";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"value\":1}", &error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.owner = "lc-e2e-acquire-for-update";
  handler_state.expected = "\"value\":1";
  rc = lc_acquire_for_update(client, &acquire_req,
                             acquire_for_update_e2e_handler, &handler_state,
                             &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"value\":2"));
  assert_true(buffer_contains(bytes, length, "\"via\":\"acquire-for-update\""));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_acquire_for_update_handler_error_rolls_back(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-acquire-for-update-error", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-seed";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"value\":1}", &error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.owner = "lc-e2e-acquire-for-update-error";
  rc = lc_acquire_for_update(client, &acquire_req,
                             acquire_for_update_failing_handler, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_string_equal(error.message,
                      "intentional acquire_for_update handler failure");
  lc_error_cleanup(&error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"value\":1"));
  assert_false(
      buffer_contains(bytes, length, "\"via\":\"failed-acquire-for-update\""));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"s3\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./build/devenv/credentials/s3/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->ns, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"s3\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_attachment_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  const void *bytes;
  size_t length;
  char key[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l',
                                          'o', '-', 's', '3'};
  int deleted;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./build/devenv/credentials/s3/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted = 0;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-attachment", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, "{\"kind\":\"attachment\"}", &error);

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.bin";
  attach_req.content_type = "application/octet-stream";
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "blob.bin");
  assert_true(attach_res.attachment.size >= (long)sizeof(payload));

  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_true(attachment_list.count >= 1U);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  memset(&get_req, 0, sizeof(get_req));
  get_req.selector.name = "blob.bin";
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = lease->delete_attachment(lease, &get_req.selector, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 1);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_sink_close(sink);
  lc_source_close(src);
  lc_attachment_get_res_cleanup(&get_res);
  lc_attachment_list_cleanup(&attachment_list);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_queue_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_sink *sink;
  lc_message *message;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  const void *bytes;
  size_t length;
  size_t written;
  char queue_name[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l', 'o',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./build/devenv/state/mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  message = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  lc_error_init(&error);
  memset(&enqueue_req, 0, sizeof(enqueue_req));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&dequeue_req, 0, sizeof(dequeue_req));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(enqueue_res.queue, queue_name);
  assert_true(enqueue_res.payload_bytes >= (long)sizeof(payload));

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->queue, queue_name);
  assert_string_equal(message->payload_content_type, "text/plain");

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, sizeof(payload));
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = message->rewind_payload(message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_sink_close(sink);
  lc_source_close(src);
  if (message != NULL) {
    lc_message_close(message);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_dequeue_batch_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char queue_name[96];
  static const unsigned char payload_a[] = {'b', 'a', 't', 'c', 'h', '-', '1'};
  static const unsigned char payload_b[] = {'b', 'a', 't', 'c', 'h', '-', '2'};
  int rc;
  size_t i;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./build/devenv/state/mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&batch, 0, sizeof(batch));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue-batch", queue_name, sizeof(queue_name));
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;

  rc = lc_source_from_memory(payload_a, sizeof(payload_a), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  rc = lc_source_from_memory(payload_b, sizeof(payload_b), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-batch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(batch.count, 2U);

  for (i = 0U; i < batch.count; ++i) {
    rc = lc_sink_to_memory(&sink, &error);
    assert_lc_ok(rc, &error);
    rc =
        batch.messages[i]->write_payload(batch.messages[i], sink, NULL, &error);
    assert_lc_ok(rc, &error);
    rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
    assert_lc_ok(rc, &error);
    assert_true(length == sizeof(payload_a) || length == sizeof(payload_b));
    rc = batch.messages[i]->ack(batch.messages[i], &error);
    assert_lc_ok(rc, &error);
    lc_sink_close(sink);
    sink = NULL;
  }

  lc_dequeue_batch_cleanup(&batch);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static int queue_watch_e2e_handler(void *context, const lc_watch_event *event,
                                   lc_error *error) {
  queue_watch_e2e_context *watch;

  watch = (queue_watch_e2e_context *)context;
  watch->event_count += 1;
  if (event->queue != NULL) {
    snprintf(watch->event_queue, sizeof(watch->event_queue), "%s",
             event->queue);
  }
  if (event->correlation_id != NULL) {
    snprintf(watch->correlation_id, sizeof(watch->correlation_id), "%s",
             event->correlation_id);
  }
  if (event->available) {
    watch->available_count += 1;
    watch->saw_available = 1;
    if (event->head_message_id != NULL) {
      snprintf(watch->head_message_id, sizeof(watch->head_message_id), "%s",
               event->head_message_id);
    }
    if (!watch->stop_after_available) {
      return 1;
    }
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("queue watch e2e observed available event");
    assert_non_null(error->message);
    return 0;
  } else {
    watch->unavailable_count += 1;
    watch->saw_unavailable = 1;
    if (event->head_message_id != NULL) {
      snprintf(watch->unavailable_head_message_id,
               sizeof(watch->unavailable_head_message_id), "%s",
               event->head_message_id);
    }
    if (!watch->stop_after_initial_unavailable &&
        (!watch->stop_after_unavailable_after_available ||
         !watch->saw_available)) {
      return 1;
    }
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("queue watch e2e observed unavailable event");
    assert_non_null(error->message);
    return 0;
  }
}

static void *queue_watch_e2e_enqueue_thread(void *arg) {
  queue_watch_e2e_context *watch;
  lc_client *client;
  lc_source *src;
  lc_enqueue_req req;
  lc_enqueue_res res;
  static const unsigned char payload[] = {'w', 'a', 't', 'c', 'h'};

  watch = (queue_watch_e2e_context *)arg;
  client = NULL;
  src = NULL;
  lc_error_init(&watch->enqueue_error);
  lc_enqueue_req_init(&req);
  memset(&res, 0, sizeof(res));
  usleep(200000U);

  open_tcp_client(watch->endpoint, watch->bundle_path, &client,
                  &watch->enqueue_error);
  watch->enqueue_rc = lc_source_from_memory(payload, sizeof(payload), &src,
                                            &watch->enqueue_error);
  if (watch->enqueue_rc == LC_OK) {
    req.queue = watch->queue_name;
    req.content_type = "text/plain";
    req.visibility_timeout_seconds = 30L;
    req.ttl_seconds = 300L;
    watch->enqueue_rc =
        client->enqueue(client, &req, src, &res, &watch->enqueue_error);
  }
  if (src != NULL) {
    lc_source_close(src);
  }
  lc_enqueue_res_cleanup(&res);
  lc_client_close(client);
  return NULL;
}

static void *queue_watch_e2e_enqueue_dequeue_thread(void *arg) {
  queue_watch_e2e_context *watch;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  static const unsigned char payload[] = {'w', 'a', 't', 'c', 'h',
                                          '-', 'a', 'c', 'k'};

  watch = (queue_watch_e2e_context *)arg;
  client = NULL;
  src = NULL;
  message = NULL;
  lc_error_init(&watch->enqueue_error);
  lc_error_init(&watch->dequeue_error);
  lc_enqueue_req_init(&enqueue_req);
  lc_dequeue_req_init(&dequeue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  usleep(200000U);

  open_tcp_client(watch->endpoint, watch->bundle_path, &client,
                  &watch->enqueue_error);
  watch->enqueue_rc = lc_source_from_memory(payload, sizeof(payload), &src,
                                            &watch->enqueue_error);
  if (watch->enqueue_rc == LC_OK) {
    enqueue_req.queue = watch->queue_name;
    enqueue_req.content_type = "text/plain";
    enqueue_req.visibility_timeout_seconds = 30L;
    enqueue_req.ttl_seconds = 300L;
    watch->enqueue_rc = client->enqueue(client, &enqueue_req, src, &enqueue_res,
                                        &watch->enqueue_error);
  }
  if (src != NULL) {
    lc_source_close(src);
  }
  lc_enqueue_res_cleanup(&enqueue_res);

  if (watch->enqueue_rc == LC_OK) {
    usleep(200000U);
    dequeue_req.queue = watch->queue_name;
    dequeue_req.owner = "lc-e2e-watch-worker";
    dequeue_req.visibility_timeout_seconds = 30L;
    dequeue_req.wait_seconds = 2L;
    watch->dequeue_rc =
        client->dequeue(client, &dequeue_req, &message, &watch->dequeue_error);
    if (watch->dequeue_rc == LC_OK) {
      watch->dequeue_rc = message->ack(message, &watch->dequeue_error);
      message = NULL;
    }
  } else {
    watch->dequeue_rc = watch->enqueue_rc;
  }

  if (message != NULL) {
    lc_message_close(message);
  }
  lc_client_close(client);
  return NULL;
}

static void
test_disk_queue_watch_stream_observes_initial_unavailable(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  make_unique_name("disk-watch-empty", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_initial_unavailable = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed unavailable event");
  assert_true(watch.event_count >= 1);
  assert_true(watch.unavailable_count >= 1);
  assert_int_equal(watch.available_count, 0);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_int_equal(watch.saw_available, 0);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] == '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_queue_watch_stream_observes_enqueue(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  pthread_t enqueue_thread;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  watch.endpoint = endpoint;
  watch.bundle_path = bundle_path;
  make_unique_name("disk-watch-queue", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_available = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  assert_int_equal(pthread_create(&enqueue_thread, NULL,
                                  queue_watch_e2e_enqueue_thread, &watch),
                   0);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(pthread_join(enqueue_thread, NULL), 0);
  assert_lc_ok(watch.enqueue_rc, &watch.enqueue_error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed available event");
  assert_true(watch.event_count >= 1);
  assert_true(watch.unavailable_count >= 1);
  assert_true(watch.available_count >= 1);
  assert_int_equal(watch.saw_available, 1);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] != '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&watch.enqueue_error);
  lc_error_cleanup(&error);
}

static void
test_disk_queue_watch_stream_observes_available_then_unavailable(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  pthread_t worker_thread;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  watch.endpoint = endpoint;
  watch.bundle_path = bundle_path;
  make_unique_name("disk-watch-transition", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_unavailable_after_available = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  assert_int_equal(pthread_create(&worker_thread, NULL,
                                  queue_watch_e2e_enqueue_dequeue_thread,
                                  &watch),
                   0);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(pthread_join(worker_thread, NULL), 0);
  assert_lc_ok(watch.enqueue_rc, &watch.enqueue_error);
  assert_lc_ok(watch.dequeue_rc, &watch.dequeue_error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed unavailable event");
  assert_true(watch.event_count >= 3);
  assert_true(watch.unavailable_count >= 2);
  assert_true(watch.available_count >= 1);
  assert_int_equal(watch.saw_available, 1);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] != '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&watch.enqueue_error);
  lc_error_cleanup(&watch.dequeue_error);
  lc_error_cleanup(&error);
}

static void test_disk_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  char text_template[] = LC_E2E_TMP_ROOT "/liblockdc-local-mutate-text-XXXXXX";
  char binary_template[] = LC_E2E_TMP_ROOT "/liblockdc-local-mutate-bin-XXXXXX";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_test_tmp_cleanup_stale(LC_E2E_TMP_ROOT, "liblockdc-local-mutate-",
                            LOCAL_MUTATE_TMP_PREFIX);
  write_temp_file_or_die(text_template, LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char *)"hello\n\"quoted\"",
                         sizeof("hello\n\"quoted\"") - 1U);
  write_temp_file_or_die(binary_template, LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char[]){0x00, 0x01, 0x02, 'a'}, 4U);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"e2e-local\",\"counter\":1,\"remove_me\":true}",
      &error);
  assert_local_mutate_file_variants(lease, text_template, binary_template,
                                    &error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_test_tmp_cleanup_path(text_template, LOCAL_MUTATE_TMP_PREFIX);
  lc_test_tmp_cleanup_path(binary_template, LOCAL_MUTATE_TMP_PREFIX);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  char text_template[] =
      LC_E2E_TMP_ROOT "/liblockdc-s3-local-mutate-text-XXXXXX";
  char binary_template[] =
      LC_E2E_TMP_ROOT "/liblockdc-s3-local-mutate-bin-XXXXXX";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./build/devenv/credentials/s3/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_test_tmp_cleanup_stale(LC_E2E_TMP_ROOT, "liblockdc-s3-local-mutate-",
                            S3_LOCAL_MUTATE_TMP_PREFIX);
  write_temp_file_or_die(text_template, S3_LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char *)"hello\n\"quoted\"",
                         sizeof("hello\n\"quoted\"") - 1U);
  write_temp_file_or_die(binary_template, S3_LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char[]){0x00, 0x01, 0x02, 'a'}, 4U);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"e2e-local\",\"counter\":1,\"remove_me\":true}",
      &error);
  assert_local_mutate_file_variants(lease, text_template, binary_template,
                                    &error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_test_tmp_cleanup_path(text_template, S3_LOCAL_MUTATE_TMP_PREFIX);
  lc_test_tmp_cleanup_path(binary_template, S3_LOCAL_MUTATE_TMP_PREFIX);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_acquire_if_not_exists_conflict(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_lease *conflicting_lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  conflicting_lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-conflict", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  acquire_req.if_not_exists = 1;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  rc = client->acquire(client, &acquire_req, &conflicting_lease, &error);
  assert_lc_server_error(rc, &error, 409L);
  assert_null(conflicting_lease);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_state_cas_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_update_opts update_opts;
  lc_source *src;
  lc_mutate_req mutate_req;
  lc_metadata_req metadata_req;
  lc_remove_req remove_req;
  const char *mutations[1];
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_update_opts_init(&update_opts);
  lc_mutate_req_init(&mutate_req);
  lc_metadata_req_init(&metadata_req);
  lc_remove_req_init(&remove_req);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-cas", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"kind\":\"cas\"}", &error);

  rc = lc_source_from_memory("{\"kind\":\"cas-update\"}",
                             strlen("{\"kind\":\"cas-update\"}"), &src, &error);
  assert_lc_ok(rc, &error);
  update_opts.if_version = lease->version + 100L;
  update_opts.has_if_version = 1;
  rc = lease->update(lease, src, &update_opts, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_source_close(src);
  src = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  mutations[0] = "/kind=\"cas-mutate\"";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 1U;
  mutate_req.if_version = lease->version + 100L;
  mutate_req.has_if_version = 1;
  rc = lease->mutate(lease, &mutate_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.if_version = lease->version + 100L;
  metadata_req.has_if_version = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  remove_req.if_state_etag = "stale-etag";
  rc = lease->remove(lease, &remove_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_rejects_invalid_inputs(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_error error;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_sink *sink;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  sink = NULL;
  lc_error_init(&error);
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  query_req.selector_json = NULL;
  query_req.limit = 1L;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_query_res_cleanup(&query_res);
  memset(&query_res, 0, sizeof(query_res));

  query_req.selector_json = "";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_documents_captures_lockd_trailers(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key_a[96];
  char key_b[96];
  char kind[96];
  char json_text[256];
  char selector_json[192];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-query-trailer-kind", kind, sizeof(kind));
  make_unique_name("disk-query-trailer-a", key_a, sizeof(key_a));
  make_unique_name("disk-query-trailer-b", key_b, sizeof(key_b));

  acquire_req.owner = "lc-e2e-query-trailers";
  acquire_req.ttl_seconds = 30L;

  acquire_req.key = key_a;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  snprintf(json_text, sizeof(json_text),
           "{\"kind\":\"%s\",\"ordinal\":1,\"status\":\"ready\"}", kind);
  save_json_text_or_die(lease, json_text, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.key = key_b;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  snprintf(json_text, sizeof(json_text),
           "{\"kind\":\"%s\",\"ordinal\":2,\"status\":\"ready\"}", kind);
  save_json_text_or_die(lease, json_text, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);

  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  query_req.selector_json = selector_json;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "index";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(length > 0U);
  assert_true(buffer_contains(bytes, length, kind));
  assert_true(buffer_contains(bytes, length, "\"status\":\"ready\""));
  assert_non_null(query_res.metadata_json);
  assert_true(buffer_contains(query_res.metadata_json,
                              strlen(query_res.metadata_json),
                              "query_candidates"));
  assert_string_equal(query_res.return_mode, "documents");

  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_keys_streams_many_indexed_keys(void **state) {
  enum { key_count = 512 };
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  query_keys_e2e_capture capture;
  char kind[96];
  char key_prefix[128];
  char key[160];
  char json_text[256];
  char selector_json[192];
  size_t index;
  size_t seen_count;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-query-keys-kind", kind, sizeof(kind));
  snprintf(key_prefix, sizeof(key_prefix), "e2e/query-keys/%s/", kind);
  acquire_req.owner = "lc-e2e-query-keys";
  acquire_req.ttl_seconds = 120L;

  for (index = 0U; index < (size_t)key_count; ++index) {
    snprintf(key, sizeof(key), "%s%04zu", key_prefix, index);
    acquire_req.key = key;
    rc = client->acquire(client, &acquire_req, &lease, &error);
    assert_lc_ok(rc, &error);
    snprintf(json_text, sizeof(json_text),
             "{\"kind\":\"%s\",\"ordinal\":%zu,\"status\":\"ready\"}", kind,
             index);
    save_json_text_or_die(lease, json_text, &error);
    rc = lease->release(lease, &release_req, &error);
    assert_lc_ok(rc, &error);
    lease = NULL;
  }

  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);

  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  query_req.selector_json = selector_json;
  query_req.limit = key_count;
  query_req.engine = "index";
  handler.begin = query_keys_e2e_begin;
  handler.chunk = query_keys_e2e_chunk;
  handler.end = query_keys_e2e_end;
  capture.key_prefix = key_prefix;
  capture.key_prefix_len = strlen(key_prefix);
  capture.expected_count = key_count;
  rc =
      lc_query_keys(client, &query_req, &handler, &capture, &query_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.begin_calls, (size_t)key_count);
  assert_int_equal(capture.end_calls, (size_t)key_count);
  assert_true(capture.chunk_calls >= (size_t)key_count);
  assert_true(capture.total_bytes >=
              (strlen(key_prefix) + 4U) * (size_t)key_count);
  seen_count = 0U;
  for (index = 0U; index < (size_t)key_count; ++index) {
    seen_count += capture.seen[index] != 0U ? 1U : 0U;
  }
  assert_int_equal(seen_count, (size_t)key_count);

  lc_query_res_cleanup(&query_res);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_attachment_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  char key[96];
  static const unsigned char payload[] = {'a', 't', 't', 'a', 'c', 'h'};
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./build/devenv/credentials/s3/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_get_req_init(&get_req);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-attach-fail", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"kind\":\"attachment-failure\"}", &error);

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  attach_req.has_max_bytes = 1;
  attach_req.max_bytes = 4L;
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  assert_lc_server_error(rc, &error, 413L);
  lc_source_close(src);
  src = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_req.selector.name = "missing.txt";
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_lc_server_error(rc, &error, 404L);

  lc_sink_close(sink);
  lc_attachment_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_queue_failure_modes(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_message *invalid_message;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_req invalid_dequeue_req;
  lc_nack_req nack_req;
  char queue_name[96];
  static const unsigned char payload[] = {'q', 'u', 'e', 'u', 'e',
                                          '-', 'n', 'e', 'g'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./build/devenv/state/mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  message = NULL;
  invalid_message = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_dequeue_req_init(&invalid_dequeue_req);
  lc_nack_req_init(&nack_req);

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue-fail", queue_name, sizeof(queue_name));

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.owner = "lc-e2e-worker";
  dequeue_req.queue = queue_name;
  dequeue_req.wait_seconds = 2L;
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  nack_req.intent = (lc_nack_intent)99;
  rc = message->nack(message, &nack_req, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  invalid_dequeue_req.queue = queue_name;
  invalid_dequeue_req.wait_seconds = 1L;
  invalid_dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &invalid_dequeue_req, &invalid_message, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_null(invalid_message);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_management_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_error error;
  lc_tc_lease_acquire_req acquire_req;
  lc_tc_lease_acquire_res acquire_res;
  lc_tc_cluster_res cluster_res;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                               "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_tc_lease_acquire_req_init(&acquire_req);
  memset(&acquire_res, 0, sizeof(acquire_res));
  memset(&cluster_res, 0, sizeof(cluster_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);

  acquire_req.candidate_id = "rw-client";
  acquire_req.candidate_endpoint = "https://example.invalid:9443";
  acquire_req.term = 1UL;
  acquire_req.ttl_ms = 1000L;
  rc = client->tc_lease_acquire(client, &acquire_req, &acquire_res, &error);
  assert_lc_server_error(rc, &error, 403L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = client->tc_cluster_list(client, &cluster_res, &error);
  assert_lc_server_error(rc, &error, 403L);

  lc_tc_lease_acquire_res_cleanup(&acquire_res);
  lc_tc_cluster_res_cleanup(&cluster_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_auth_permission_failure_modes(void **state) {
  const char *endpoint;
  const char *rw_bundle_path;
  const char *tc_bundle_path;
  lc_client *rw_client;
  lc_client *tc_client;
  lc_error error;
  lc_tc_lease_acquire_req tc_req;
  lc_tc_lease_acquire_res tc_res;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  rw_bundle_path = env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                                  "./build/devenv/credentials/disk/client.pem");
  tc_bundle_path = env_or_default(
      "LOCKDC_E2E_TC_BUNDLE", "./build/devenv/credentials/disk/tc-client.pem");
  require_file_or_skip(rw_bundle_path);
  require_file_or_skip(tc_bundle_path);

  rw_client = NULL;
  tc_client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_tc_lease_acquire_req_init(&tc_req);
  memset(&tc_res, 0, sizeof(tc_res));
  lc_acquire_req_init(&acquire_req);

  open_tcp_client(endpoint, rw_bundle_path, &rw_client, &error);
  tc_req.candidate_id = "lc-e2e-tc";
  tc_req.candidate_endpoint = "https://example.invalid:9443";
  tc_req.term = 1UL;
  tc_req.ttl_ms = 1000L;
  rc = rw_client->tc_lease_acquire(rw_client, &tc_req, &tc_res, &error);
  assert_lc_server_error(rc, &error, 403L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  open_tcp_client(endpoint, tc_bundle_path, &tc_client, &error);
  make_unique_name("tc-no-data", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = tc_client->acquire(tc_client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_tc_lease_acquire_res_cleanup(&tc_res);
  lc_client_close(tc_client);
  lc_client_close(rw_client);
  lc_error_cleanup(&error);
}

typedef struct e2e_consumer_backend {
  const char *label;
  const char *endpoint_env;
  const char *endpoint_default;
  const char *bundle_env;
  const char *bundle_default;
  const char *socket_env;
  const char *socket_default;
  int use_uds;
} e2e_consumer_backend;

typedef struct e2e_consumer_context {
  pthread_mutex_t mutex;
  int handled;
  int error_events;
  int start_events;
  int stop_events;
  int first_delivery_mode;
  int expect_state;
  int read_payload;
  int saw_state;
  long last_visibility_timeout_seconds;
  int last_error_code;
  long last_error_http_status;
  char last_error_server_code[96];
  char last_error_message[192];
  const char *backend_label;
  const char *queue_name;
  int enqueue_count;
  size_t worker_count;
  int max_failures;
  int expected_minimum_count;
  int queue_stats_available;
  long queue_stats_waiting_consumers;
  long queue_stats_pending_candidates;
  long queue_stats_total_consumers;
  int queue_stats_has_active_watcher;
  long queue_stats_head_enqueued_at_unix;
  long queue_stats_head_not_visible_until_unix;
  long queue_stats_head_age_seconds;
  char queue_stats_head_message_id[160];
  char queue_stats_correlation_id[96];
  char payload[64];
  char state_json[128];
  char state_key[160];
} e2e_consumer_context;

enum {
  E2E_CONSUMER_FIRST_DELIVERY_NONE = 0,
  E2E_CONSUMER_FIRST_DELIVERY_FAIL = 1,
  E2E_CONSUMER_FIRST_DELIVERY_DEFER = 2,
  E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE = 3,
  E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR = 4,
  E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE = 5,
  E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR = 6,
  E2E_CONSUMER_FIRST_DELIVERY_ACK = 7
};

static const e2e_consumer_backend e2e_backend_disk = {
    "disk",
    "LOCKDC_E2E_DISK_A_ENDPOINT",
    "https://127.0.0.1:19441",
    "LOCKDC_E2E_DISK_A_BUNDLE",
    "./build/devenv/credentials/disk/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_s3 = {
    "s3",
    "LOCKDC_E2E_S3_ENDPOINT",
    "https://127.0.0.1:19443",
    "LOCKDC_E2E_S3_BUNDLE",
    "./build/devenv/credentials/s3/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_mem = {
    "mem",
    NULL,
    NULL,
    NULL,
    NULL,
    "LOCKDC_E2E_MEM_SOCKET",
    "./build/devenv/state/mem-run/lockd.sock",
    1};

static const char *e2e_consumer_delivery_mode_name(int mode) {
  switch (mode) {
  case E2E_CONSUMER_FIRST_DELIVERY_NONE:
    return "none";
  case E2E_CONSUMER_FIRST_DELIVERY_FAIL:
    return "fail";
  case E2E_CONSUMER_FIRST_DELIVERY_DEFER:
    return "defer";
  case E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE:
    return "nack_failure";
  case E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR:
    return "nack_failure_error";
  case E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE:
    return "fail_twice";
  case E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR:
    return "ack_error";
  case E2E_CONSUMER_FIRST_DELIVERY_ACK:
    return "ack";
  default:
    return "unknown";
  }
}

static void e2e_consumer_capture_queue_stats(lc_client *client,
                                             const char *queue_name,
                                             e2e_consumer_context *context) {
  lc_queue_stats_req req;
  lc_queue_stats_res res;
  lc_error error;
  int rc;

  if (client == NULL || queue_name == NULL || context == NULL) {
    return;
  }

  lc_queue_stats_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  req.queue = queue_name;
  rc = client->queue_stats(client, &req, &res, &error);

  pthread_mutex_lock(&context->mutex);
  if (rc == LC_OK) {
    context->queue_stats_available = res.available;
    context->queue_stats_waiting_consumers = res.waiting_consumers;
    context->queue_stats_pending_candidates = res.pending_candidates;
    context->queue_stats_total_consumers = res.total_consumers;
    context->queue_stats_has_active_watcher = res.has_active_watcher;
    context->queue_stats_head_enqueued_at_unix = res.head_enqueued_at_unix;
    context->queue_stats_head_not_visible_until_unix =
        res.head_not_visible_until_unix;
    context->queue_stats_head_age_seconds = res.head_age_seconds;
    memset(context->queue_stats_head_message_id, 0,
           sizeof(context->queue_stats_head_message_id));
    if (res.head_message_id != NULL) {
      strncpy(context->queue_stats_head_message_id, res.head_message_id,
              sizeof(context->queue_stats_head_message_id) - 1U);
    }
    memset(context->queue_stats_correlation_id, 0,
           sizeof(context->queue_stats_correlation_id));
    if (res.correlation_id != NULL) {
      strncpy(context->queue_stats_correlation_id, res.correlation_id,
              sizeof(context->queue_stats_correlation_id) - 1U);
    }
  } else {
    context->queue_stats_available = -1;
    context->queue_stats_waiting_consumers = -1L;
    context->queue_stats_pending_candidates = -1L;
    context->queue_stats_total_consumers = -1L;
    context->queue_stats_has_active_watcher = 0;
    context->queue_stats_head_enqueued_at_unix = -1L;
    context->queue_stats_head_not_visible_until_unix = -1L;
    context->queue_stats_head_age_seconds = -1L;
    memset(context->queue_stats_head_message_id, 0,
           sizeof(context->queue_stats_head_message_id));
    memset(context->queue_stats_correlation_id, 0,
           sizeof(context->queue_stats_correlation_id));
    if (error.correlation_id != NULL) {
      strncpy(context->queue_stats_correlation_id, error.correlation_id,
              sizeof(context->queue_stats_correlation_id) - 1U);
    }
  }
  pthread_mutex_unlock(&context->mutex);

  lc_queue_stats_res_cleanup(&res);
  lc_error_cleanup(&error);
}

static void open_consumer_backend_client(const e2e_consumer_backend *backend,
                                         lc_client **out, lc_error *error) {
  const char *endpoint;
  const char *bundle_path;
  const char *socket_path;

  if (backend->use_uds) {
    socket_path = env_or_default(backend->socket_env, backend->socket_default);
    require_socket_or_skip(socket_path);
    open_uds_client(socket_path, out, error);
    return;
  }

  endpoint = env_or_default(backend->endpoint_env, backend->endpoint_default);
  bundle_path = env_or_default(backend->bundle_env, backend->bundle_default);
  require_file_or_skip(bundle_path);
  open_tcp_client(endpoint, bundle_path, out, error);
}

static int e2e_consumer_record_payload(e2e_consumer_context *consumer_context,
                                       const void *bytes, size_t length) {
  pthread_mutex_lock(&consumer_context->mutex);
  memset(consumer_context->payload, 0, sizeof(consumer_context->payload));
  if (length >= sizeof(consumer_context->payload)) {
    length = sizeof(consumer_context->payload) - 1U;
  }
  memcpy(consumer_context->payload, bytes, length);
  consumer_context->handled += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
  return LC_OK;
}

static void e2e_sleep_ms(long delay_ms) {
  struct timespec ts;

  if (delay_ms <= 0L) {
    return;
  }
  ts.tv_sec = delay_ms / 1000L;
  ts.tv_nsec = (delay_ms % 1000L) * 1000000L;
  while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
  }
}

static int e2e_consumer_persist_state(e2e_consumer_context *consumer_context,
                                      lc_lease *state, lc_error *error) {
  e2e_status_doc doc;
  lc_get_res get_res;
  int rc;

  if (state == NULL) {
    return LC_OK;
  }

  memset(&doc, 0, sizeof(doc));
  memset(&get_res, 0, sizeof(get_res));
  doc.status = "from-consumer-service";
  rc = state->save(state, &e2e_status_map, &doc, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&doc, 0, sizeof(doc));
  rc = state->load(state, &e2e_status_map, &doc, NULL, &get_res, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(&get_res);
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->saw_state = 1;
  memset(consumer_context->state_json, 0, sizeof(consumer_context->state_json));
  if (doc.status != NULL) {
    strncpy(consumer_context->state_json, doc.status,
            sizeof(consumer_context->state_json) - 1U);
  }
  memset(consumer_context->state_key, 0, sizeof(consumer_context->state_key));
  if (state->key != NULL) {
    strncpy(consumer_context->state_key, state->key,
            sizeof(consumer_context->state_key) - 1U);
  }
  pthread_mutex_unlock(&consumer_context->mutex);

  e2e_lonejson_cleanup(&e2e_status_map, &doc);
  lc_get_res_cleanup(&get_res);
  return LC_OK;
}

static int e2e_consumer_handle(void *context, lc_consumer_message *delivery,
                               lc_error *error) {
  e2e_consumer_context *consumer_context;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int handled_count;
  lc_lease *state_lease;
  int rc;
  lc_nack_req nack_req;

  consumer_context = (e2e_consumer_context *)context;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  handled_count = 0;
  if (consumer_context->read_payload) {
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = delivery->message->write_payload(delivery->message, sink, NULL, error);
    if (rc != LC_OK) {
      lc_sink_close(sink);
      return rc;
    }
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc == LC_OK) {
      rc = e2e_consumer_record_payload(consumer_context, bytes, length);
    }
    lc_sink_close(sink);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    pthread_mutex_lock(&consumer_context->mutex);
    consumer_context->handled += 1;
    pthread_mutex_unlock(&consumer_context->mutex);
  }

  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->last_visibility_timeout_seconds =
      delivery != NULL && delivery->message != NULL
          ? delivery->message->visibility_timeout_seconds
          : 0L;
  pthread_mutex_unlock(&consumer_context->mutex);

  state_lease = delivery->state;
  if (state_lease == NULL && delivery->message != NULL &&
      delivery->message->state != NULL) {
    state_lease = delivery->message->state(delivery->message);
  }
  if (consumer_context->expect_state && state_lease == NULL) {
    return LC_ERR_PROTOCOL;
  }
  rc = e2e_consumer_persist_state(consumer_context, state_lease, error);
  if (rc != LC_OK) {
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  handled_count = consumer_context->handled;
  pthread_mutex_unlock(&consumer_context->mutex);
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_FAIL &&
      handled_count == 1) {
    e2e_sleep_ms(1500L);
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE &&
      handled_count <= 2) {
    e2e_sleep_ms(1500L);
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_DEFER &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_DEFER;
    nack_req.delay_seconds = 0L;
    return delivery->message->nack(delivery->message, &nack_req, error);
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    return delivery->message->nack(delivery->message, &nack_req, error);
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    rc = delivery->message->nack(delivery->message, &nack_req, error);
    if (rc != LC_OK) {
      return rc;
    }
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR &&
      handled_count == 1) {
    rc = delivery->message->ack(delivery->message, error);
    if (rc != LC_OK) {
      return rc;
    }
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_ACK &&
      handled_count == 1) {
    return delivery->message->ack(delivery->message, error);
  }

  return LC_OK;
}

static int e2e_consumer_on_error(void *context, const lc_consumer_error *event,
                                 lc_error *error) {
  e2e_consumer_context *consumer_context;

  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->error_events += 1;
  consumer_context->last_error_code =
      event != NULL && event->cause != NULL ? event->cause->code : LC_OK;
  consumer_context->last_error_http_status =
      event != NULL && event->cause != NULL ? event->cause->http_status : 0L;
  memset(consumer_context->last_error_server_code, 0,
         sizeof(consumer_context->last_error_server_code));
  if (event != NULL && event->cause != NULL &&
      event->cause->server_code != NULL) {
    strncpy(consumer_context->last_error_server_code, event->cause->server_code,
            sizeof(consumer_context->last_error_server_code) - 1U);
  }
  memset(consumer_context->last_error_message, 0,
         sizeof(consumer_context->last_error_message));
  if (event != NULL && event->cause != NULL && event->cause->message != NULL) {
    strncpy(consumer_context->last_error_message, event->cause->message,
            sizeof(consumer_context->last_error_message) - 1U);
  }
  pthread_mutex_unlock(&consumer_context->mutex);
  (void)error;
  return LC_OK;
}

static void e2e_consumer_on_start(void *context,
                                  const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->start_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void e2e_consumer_on_stop(void *context,
                                 const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->stop_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void wait_for_consumer_handled(e2e_consumer_context *consumer_context,
                                      int minimum_count) {
  int maximum_retries;
  int retries;

  maximum_retries = 120;
  if (minimum_count > 1) {
    maximum_retries = 600;
  }
  retries = 0;
  for (;;) {
    pthread_mutex_lock(&consumer_context->mutex);
    if (consumer_context->handled >= minimum_count &&
        (!consumer_context->expect_state || consumer_context->saw_state)) {
      pthread_mutex_unlock(&consumer_context->mutex);
      return;
    }
    pthread_mutex_unlock(&consumer_context->mutex);
    if (retries++ > maximum_retries) {
      fail_msg(
          "consumer retry timed out: backend=%s queue=%s mode=%s "
          "enqueue_count=%d worker_count=%lu max_failures=%d "
          "expected_minimum_count=%d handled=%d start_events=%d "
          "stop_events=%d error_events=%d last_error_code=%d "
          "last_error_http_status=%ld last_error_server_code=%s "
          "last_error_message=%s last_visibility_timeout_seconds=%ld "
          "payload=%s state_json=%s state_key=%s "
          "queue_available=%d waiting_consumers=%ld "
          "pending_candidates=%ld total_consumers=%ld "
          "has_active_watcher=%d head_message_id=%s "
          "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
          "head_age_seconds=%ld stats_cid=%s",
          consumer_context->backend_label, consumer_context->queue_name,
          e2e_consumer_delivery_mode_name(
              consumer_context->first_delivery_mode),
          consumer_context->enqueue_count,
          (unsigned long)consumer_context->worker_count,
          consumer_context->max_failures,
          consumer_context->expected_minimum_count, consumer_context->handled,
          consumer_context->start_events, consumer_context->stop_events,
          consumer_context->error_events, consumer_context->last_error_code,
          consumer_context->last_error_http_status,
          consumer_context->last_error_server_code,
          consumer_context->last_error_message,
          consumer_context->last_visibility_timeout_seconds,
          consumer_context->payload, consumer_context->state_json,
          consumer_context->state_key, consumer_context->queue_stats_available,
          consumer_context->queue_stats_waiting_consumers,
          consumer_context->queue_stats_pending_candidates,
          consumer_context->queue_stats_total_consumers,
          consumer_context->queue_stats_has_active_watcher,
          consumer_context->queue_stats_head_message_id,
          consumer_context->queue_stats_head_enqueued_at_unix,
          consumer_context->queue_stats_head_not_visible_until_unix,
          consumer_context->queue_stats_head_age_seconds,
          consumer_context->queue_stats_correlation_id);
    }
    usleep(100000);
  }
}

static void run_consumer_service_variant_with_max_failures(
    const e2e_consumer_backend *backend, const char *name_prefix,
    int enqueue_count, int first_delivery_mode, int expect_state,
    size_t worker_count, int max_failures, int read_payload) {
  lc_client *client;
  lc_consumer_service *service;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  e2e_consumer_context consumer_context;
  char queue_name[96];
  static const unsigned char payload[] = {'c', 'o', 'n', 's', 'u', 'm',
                                          'e', 'r', '-', 'o', 'k'};
  int rc;
  int i;

  client = NULL;
  service = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  memset(&consumer_context, 0, sizeof(consumer_context));
  consumer_context.backend_label = backend->label;
  consumer_context.first_delivery_mode = first_delivery_mode;
  consumer_context.expect_state = expect_state;
  consumer_context.read_payload = read_payload;
  consumer_context.enqueue_count = enqueue_count;
  consumer_context.worker_count = worker_count;
  consumer_context.max_failures = max_failures;
  consumer_context.queue_stats_available = -1;
  consumer_context.queue_stats_waiting_consumers = -1L;
  consumer_context.queue_stats_pending_candidates = -1L;
  consumer_context.queue_stats_total_consumers = -1L;
  consumer_context.queue_stats_head_enqueued_at_unix = -1L;
  consumer_context.queue_stats_head_not_visible_until_unix = -1L;
  consumer_context.queue_stats_head_age_seconds = -1L;
  assert_int_equal(pthread_mutex_init(&consumer_context.mutex, NULL), 0);

  open_consumer_backend_client(backend, &client, &error);
  make_unique_name(name_prefix, queue_name, sizeof(queue_name));
  consumer_context.queue_name = queue_name;

  consumer.name = backend->label;
  consumer.request.queue = queue_name;
  consumer.request.visibility_timeout_seconds =
      first_delivery_mode != E2E_CONSUMER_FIRST_DELIVERY_NONE ? 2L : 30L;
  consumer.request.wait_seconds = 1L;
  consumer.worker_count = worker_count;
  consumer.with_state = expect_state;
  consumer.handle = e2e_consumer_handle;
  consumer.on_error = e2e_consumer_on_error;
  consumer.on_start = e2e_consumer_on_start;
  consumer.on_stop = e2e_consumer_on_stop;
  consumer.context = &consumer_context;
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 250L;
  consumer.restart_policy.max_failures = max_failures;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  if (rc != LC_OK) {
    fail_msg("consumer service create failed: backend=%s queue=%s mode=%s "
             "enqueue_count=%d worker_count=%lu max_failures=%d rc=%d "
             "error_code=%d http_status=%ld message=%s detail=%s",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             enqueue_count, (unsigned long)worker_count, max_failures, rc,
             error.code, error.http_status,
             error.message != NULL ? error.message : "",
             error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);
  assert_non_null(service);

  rc = service->start(service, &error);
  if (rc != LC_OK) {
    fail_msg("consumer service start failed: backend=%s queue=%s mode=%s "
             "enqueue_count=%d worker_count=%lu max_failures=%d rc=%d "
             "error_code=%d http_status=%ld message=%s detail=%s",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             enqueue_count, (unsigned long)worker_count, max_failures, rc,
             error.code, error.http_status,
             error.message != NULL ? error.message : "",
             error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  for (i = 0; i < enqueue_count; ++i) {
    lc_source *src;

    src = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
    assert_lc_ok(rc, &error);
    rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
    assert_lc_ok(rc, &error);
    lc_source_close(src);
    lc_enqueue_res_cleanup(&enqueue_res);
    memset(&enqueue_res, 0, sizeof(enqueue_res));
  }

  {
    int minimum_count;

    minimum_count = enqueue_count;
    if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_DEFER ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR) {
      minimum_count += 1;
    } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE) {
      minimum_count += 2;
    }
    consumer_context.expected_minimum_count = minimum_count;
    wait_for_consumer_handled(&consumer_context, minimum_count);
  }

  e2e_consumer_capture_queue_stats(client, queue_name, &consumer_context);
  rc = (service->stop)(service);
  if (rc != LC_OK) {
    fail_msg("consumer service stop failed: backend=%s queue=%s mode=%s "
             "handled=%d error_events=%d start_events=%d stop_events=%d rc=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.handled, consumer_context.error_events,
             consumer_context.start_events, consumer_context.stop_events, rc);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  e2e_consumer_capture_queue_stats(client, queue_name, &consumer_context);
  if (rc != LC_OK) {
    fail_msg(
        "consumer service wait failed: backend=%s queue=%s mode=%s "
        "enqueue_count=%d worker_count=%lu max_failures=%d handled=%d "
        "start_events=%d stop_events=%d error_events=%d "
        "last_error_code=%d last_error_http_status=%ld "
        "last_error_server_code=%s last_error_message=%s "
        "queue_available=%d waiting_consumers=%ld pending_candidates=%ld "
        "total_consumers=%ld has_active_watcher=%d head_message_id=%s "
        "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
        "head_age_seconds=%ld "
        "stats_cid=%s rc=%d error_code=%d http_status=%ld message=%s detail=%s",
        backend->label, queue_name,
        e2e_consumer_delivery_mode_name(first_delivery_mode), enqueue_count,
        (unsigned long)worker_count, max_failures, consumer_context.handled,
        consumer_context.start_events, consumer_context.stop_events,
        consumer_context.error_events, consumer_context.last_error_code,
        consumer_context.last_error_http_status,
        consumer_context.last_error_server_code,
        consumer_context.last_error_message,
        consumer_context.queue_stats_available,
        consumer_context.queue_stats_waiting_consumers,
        consumer_context.queue_stats_pending_candidates,
        consumer_context.queue_stats_total_consumers,
        consumer_context.queue_stats_has_active_watcher,
        consumer_context.queue_stats_head_message_id,
        consumer_context.queue_stats_head_enqueued_at_unix,
        consumer_context.queue_stats_head_not_visible_until_unix,
        consumer_context.queue_stats_head_age_seconds,
        consumer_context.queue_stats_correlation_id, rc, error.code,
        error.http_status, error.message != NULL ? error.message : "",
        error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);

  pthread_mutex_lock(&consumer_context.mutex);
  if (!(consumer_context.start_events >= (int)worker_count)) {
    fail_msg("consumer start_events too low: backend=%s queue=%s mode=%s "
             "start_events=%d worker_count=%lu handled=%d error_events=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.start_events, (unsigned long)worker_count,
             consumer_context.handled, consumer_context.error_events);
  }
  if (!(consumer_context.stop_events >= (int)worker_count)) {
    fail_msg("consumer stop_events too low: backend=%s queue=%s mode=%s "
             "stop_events=%d worker_count=%lu handled=%d error_events=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.stop_events, (unsigned long)worker_count,
             consumer_context.handled, consumer_context.error_events);
  }
  assert_true(consumer_context.start_events >= (int)worker_count);
  assert_true(consumer_context.stop_events >= (int)worker_count);
  if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL ||
      first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE ||
      first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR) {
    if (consumer_context.handled !=
        enqueue_count +
            (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE
                 ? 2
                 : 1)) {
      fail_msg("unexpected handled count after retrying failure path: "
               "backend=%s queue=%s mode=%s handled=%d enqueue_count=%d "
               "error_events=%d start_events=%d stop_events=%d "
               "last_error_code=%d last_error_http_status=%ld "
               "last_error_server_code=%s last_error_message=%s",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.handled, enqueue_count,
               consumer_context.error_events, consumer_context.start_events,
               consumer_context.stop_events, consumer_context.last_error_code,
               consumer_context.last_error_http_status,
               consumer_context.last_error_server_code,
               consumer_context.last_error_message);
    }
    assert_int_equal(
        consumer_context.handled,
        enqueue_count +
            (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE
                 ? 2
                 : 1));
    assert_true(consumer_context.error_events >= 1);
    assert_true(consumer_context.start_events >= (int)worker_count + 1);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR) {
    if (consumer_context.handled != enqueue_count ||
        consumer_context.error_events < 1 ||
        consumer_context.start_events < (int)worker_count + 1) {
      fail_msg(
          "unexpected ack-error consumer state: backend=%s queue=%s "
          "mode=%s handled=%d enqueue_count=%d error_events=%d "
          "start_events=%d stop_events=%d last_error_code=%d "
          "last_error_http_status=%ld last_error_server_code=%s "
          "last_error_message=%s payload=%s "
          "queue_available=%d waiting_consumers=%ld pending_candidates=%ld "
          "total_consumers=%ld head_message_id=%s "
          "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
          "head_age_seconds=%ld stats_cid=%s",
          backend->label, queue_name,
          e2e_consumer_delivery_mode_name(first_delivery_mode),
          consumer_context.handled, enqueue_count,
          consumer_context.error_events, consumer_context.start_events,
          consumer_context.stop_events, consumer_context.last_error_code,
          consumer_context.last_error_http_status,
          consumer_context.last_error_server_code,
          consumer_context.last_error_message, consumer_context.payload,
          consumer_context.queue_stats_available,
          consumer_context.queue_stats_waiting_consumers,
          consumer_context.queue_stats_pending_candidates,
          consumer_context.queue_stats_total_consumers,
          consumer_context.queue_stats_head_message_id,
          consumer_context.queue_stats_head_enqueued_at_unix,
          consumer_context.queue_stats_head_not_visible_until_unix,
          consumer_context.queue_stats_head_age_seconds,
          consumer_context.queue_stats_correlation_id);
    }
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_true(consumer_context.error_events >= 1);
    assert_true(consumer_context.start_events >= (int)worker_count + 1);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_ACK) {
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_DEFER ||
             first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE) {
    assert_int_equal(consumer_context.handled, enqueue_count + 1);
    if (consumer_context.error_events != 0) {
      fail_msg(
          "unexpected consumer error event: error_events=%d "
          "last_error_code=%d last_error_http_status=%ld "
          "last_error_server_code=%s last_error_message=%s "
          "start_events=%d stop_events=%d last_visibility_timeout_seconds=%ld",
          consumer_context.error_events, consumer_context.last_error_code,
          consumer_context.last_error_http_status,
          consumer_context.last_error_server_code,
          consumer_context.last_error_message, consumer_context.start_events,
          consumer_context.stop_events,
          consumer_context.last_visibility_timeout_seconds);
    }
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  } else {
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  }
  if (read_payload) {
    if (strcmp(consumer_context.payload, "consumer-ok") != 0) {
      fail_msg("unexpected consumer payload: backend=%s queue=%s mode=%s "
               "payload=%s handled=%d error_events=%d",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.payload, consumer_context.handled,
               consumer_context.error_events);
    }
    assert_string_equal(consumer_context.payload, "consumer-ok");
  } else {
    assert_string_equal(consumer_context.payload, "");
  }
  if (expect_state) {
    if (consumer_context.saw_state != 1 ||
        strstr(consumer_context.state_json, "from-consumer-service") == NULL ||
        strstr(consumer_context.state_key, "/state/") == NULL ||
        strstr(consumer_context.state_key, queue_name) == NULL) {
      fail_msg("unexpected consumer state persistence: backend=%s queue=%s "
               "mode=%s saw_state=%d state_json=%s state_key=%s",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.saw_state, consumer_context.state_json,
               consumer_context.state_key);
    }
    assert_int_equal(consumer_context.saw_state, 1);
    assert_non_null(
        strstr(consumer_context.state_json, "from-consumer-service"));
    assert_non_null(strstr(consumer_context.state_key, "/state/"));
    assert_non_null(strstr(consumer_context.state_key, queue_name));
  } else {
    assert_int_equal(consumer_context.saw_state, 0);
  }
  pthread_mutex_unlock(&consumer_context.mutex);

  pthread_mutex_destroy(&consumer_context.mutex);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_consumer_service_close(service);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void run_consumer_service_variant(const e2e_consumer_backend *backend,
                                         const char *name_prefix,
                                         int enqueue_count,
                                         int first_delivery_mode,
                                         int expect_state,
                                         size_t worker_count) {
  run_consumer_service_variant_with_max_failures(
      backend, name_prefix, enqueue_count, first_delivery_mode, expect_state,
      worker_count, 5, 1);
}

static void run_consumer_service_variant_without_payload_read(
    const e2e_consumer_backend *backend, const char *name_prefix,
    int enqueue_count, int first_delivery_mode, int expect_state,
    size_t worker_count) {
  run_consumer_service_variant_with_max_failures(
      backend, name_prefix, enqueue_count, first_delivery_mode, expect_state,
      worker_count, 5, 0);
}

static void test_disk_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_disk_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_disk_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void test_disk_consumer_service_delivery_failures_ignore_failure_budget(
    void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_disk, "disk-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_disk_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_disk_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void test_disk_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_disk, "disk-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_disk_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_disk_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void
test_disk_consumer_service_failure_nacks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_disk_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_s3_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_s3_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_s3_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void
test_s3_consumer_service_delivery_failures_ignore_failure_budget(void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_s3, "s3-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_s3_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_s3_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void test_s3_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_s3, "s3-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_s3_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_s3_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void
test_s3_consumer_service_failure_nacks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_s3_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_mem_uds_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_mem_uds_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void
test_mem_uds_consumer_service_delivery_failures_ignore_failure_budget(
    void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_mem, "mem-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_mem_uds_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_mem_uds_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_mem, "mem-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_mem_uds_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_mem_uds_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_failure_nacks_without_reading_payload(
    void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_mem_uds_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_worker(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-workers", 2, 0,
                               0, 2U);
}

static void test_mem_uds_dequeue_with_state_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't', 'e',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./build/devenv/state/mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, NULL, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, &get_res,
                           &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-direct-state");
    e2e_lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_dequeue_with_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't',
                                          'e', '-', 's', '3'};
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./build/devenv/credentials/s3/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-s3-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, NULL, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, &get_res,
                           &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-s3-direct-state");
    e2e_lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_pouch_direct_migrates_legacy_lease_before_client_open(void **state) {
  lc_pouch *pouch;
  lc_client *client;
  lc_lease *lease;
  lc_pouch_open_options options;
  lc_acquire_req request;
  lc_keepalive_op keepalive;
  lc_keepalive_res kept;
  lc_error error;
  char root[512];
  char endpoint[1024];
  static const char crypto_key[] =
      "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  int rc;

  (void)state;
  pouch = NULL;
  client = NULL;
  lease = NULL;
  memset(&options, 0, sizeof(options));
  lc_acquire_req_init(&request);
  lc_keepalive_op_init(&keepalive);
  memset(&kept, 0, sizeof(kept));
  lc_error_init(&error);
  make_pouch_root("legacy-control", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  options.crypto_key = crypto_key;
  rc = lc_pouch_open(root, NULL, &options, &pouch, &error);
  assert_lc_ok(rc, &error);
  pouch_e2e_write_legacy_lease(pouch, "state/legacy-control", 0, &error);
  pouch_e2e_write_legacy_lease(pouch, "state/extended-legacy-control", 1,
                               &error);
  lc_pouch_close(pouch);
  pouch = NULL;
  pouch_e2e_remove_control_migration_marker(root);
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s?crypto_key=%s",
                       root, crypto_key) > 0);

  open_pouch_client(endpoint, &client, &error);
  request.key = "state/legacy-control";
  request.owner = "pouch-e2e-migration";
  request.ttl_seconds = 30L;
  rc = client->acquire(client, &request, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  keepalive.lease.ns = "default";
  keepalive.lease.key = "state/extended-legacy-control";
  keepalive.lease.lease_id = "legacy-lease";
  keepalive.lease.fencing_token = 4L;
  keepalive.ttl_seconds = 30L;
  rc = client->keepalive(client, &keepalive, &kept, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(kept.version, 37L);
  lc_keepalive_res_cleanup(&kept);

  lc_lease_close(lease);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_direct_transformed_metadata_replay_defers_body_materialization(
    void **state) {
  static const char crypto_key[] =
      "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  static const char payload[] =
      "{\"kind\":\"pouch-transformed-replay\",\"value\":1}";
  lc_pouch *writer;
  lc_pouch *reader;
  lc_source *body;
  lc_pouch_open_options options;
  lc_pouch_state_write_options write_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result metadata_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[256];
  char endpoint[512];
  char segment_path[640];
  int written;
  int rc;

  (void)state;
  writer = NULL;
  reader = NULL;
  body = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_options, 0, sizeof(write_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&metadata_result, 0, sizeof(metadata_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_pouch_root("transformed-lazy-replay", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  options.crypto_key = crypto_key;
  options.compression = "zlib";
  rc = lc_pouch_open(root, NULL, &options, &writer, &error);
  assert_lc_ok(rc, &error);
  rc = lc_source_from_memory(payload, strlen(payload), &body, &error);
  assert_lc_ok(rc, &error);
  write_options.content_type = "application/json";
  rc = lc_pouch_state_write(writer, "default", "state/transformed", body,
                            &write_options, &write_result, &error);
  lc_source_close(body);
  body = NULL;
  assert_lc_ok(rc, &error);
  lc_pouch_close(writer);
  writer = NULL;

  rc = lc_pouch_open(root, NULL, &options, &reader, &error);
  assert_lc_ok(rc, &error);
  rc = lc_pouch_state_read_metadata(reader, "default", "state/transformed",
                                    &metadata_result, &error);
  assert_lc_ok(rc, &error);
  assert_true(metadata_result.found);
  assert_null(metadata_result.body);
  lc_pouch_state_read_result_cleanup(NULL, &metadata_result);

  written = snprintf(
      segment_path, sizeof(segment_path),
      "%s/namespaces/default/segments/seg-00000000000000000001.log", root);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  assert_int_equal(unlink(segment_path), 0);
  rc = lc_pouch_state_read(reader, "default", "state/transformed", &read_result,
                           &error);
  assert_int_not_equal(rc, LC_OK);
  assert_null(read_result.body);

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_read_result_cleanup(NULL, &metadata_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(reader);
  cleanup_pouch_root(root);
  lc_error_cleanup(&error);
}

static void test_pouch_direct_query_indexing_disabled_roundtrip(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/kind\",\"value\":\"pouch-non-query\"}}";
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  pouch_e2e_key_count key_count;
  char root[256];
  char endpoint[512];
  int rc;

  (void)state;
  make_pouch_root("query-indexing-disabled", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?query_indexing=false&query_engine=index&"
                       "query_fallback_engine=index&indexer_flush_docs=1",
                       root) > 0);
  client = NULL;
  reader = NULL;
  lease = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&flush_res, 0, sizeof(flush_res));
  memset(&key_count, 0, sizeof(key_count));
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_query_req_init(&query_req);
  lc_index_flush_req_init(&flush_req);
  handler.begin = pouch_e2e_key_begin;
  handler.chunk = pouch_e2e_key_chunk;
  handler.end = pouch_e2e_key_end;

  open_pouch_client(endpoint, &client, &error);
  acquire_req.key = "docs/non-query";
  acquire_req.owner = "lc-e2e-pouch-non-query";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"kind\":\"pouch-non-query\"}", &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &key_count, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(key_count.rows, 1U);
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  lc_query_res_cleanup(&query_res);

  lc_query_req_init(&query_req);
  query_req.selector_json = selector;
  query_req.engine = "index";
  rc = client->query_keys(client, &query_req, &handler, &key_count, &query_res,
                          &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch query indexing is disabled; use engine=scan");
  lc_error_cleanup(&error);
  lc_error_init(&error);

  flush_req.ns = "default";
  flush_req.mode = "sync";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "pouch query indexing is disabled; flush_index is unavailable");
  lc_index_flush_res_cleanup(&flush_res);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_client_close(client);
  client = NULL;
  open_pouch_client(endpoint, &reader, &error);
  lc_query_req_init(&query_req);
  memset(&key_count, 0, sizeof(key_count));
  query_req.selector_json = selector;
  rc = reader->query_keys(reader, &query_req, &handler, &key_count, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(key_count.rows, 1U);
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  lc_query_res_cleanup(&query_res);

  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_direct_route_command_wait_roundtrip(void **state) {
  char root[256], endpoint[512], command_id[48], failed_command_id[48];
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_outbox_entry entry;
  lc_outbox_receipt outbox_receipt;
  lc_outbox_commit_result commit_result;
  lc_source *payload;
  lc_error error;

  (void)state;
  make_pouch_root("route-command-wait", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_command_receipt_init(&receipt);
  lc_outbox_receipt_init(&outbox_receipt);
  lc_outbox_commit_result_init(&commit_result);
  open_pouch_client(endpoint, &client, &error);
  {
    lc_outbox_config config;

    lc_outbox_config_init(&config);
    config.ns = "route-command";
    config.owner = "route-command-owner";
    assert_lc_ok(lc_client_new_outbox(client, &config, &outbox, &error),
                 &error);
  }
  lc_command_request_init(&command);
  command.identity.scope = "tenant-route";
  command.identity.command_type = "orders.create.v1";
  command.generate_idempotency_key = 1;
  command.request_digest = "route-command-digest";
  assert_lc_ok(lc_outbox_accept_command(outbox, &command, &transaction,
                                        &receipt, &error),
               &error);
  assert_non_null(transaction);
  assert_non_null(receipt.idempotency_key);
  assert_int_equal(strlen(receipt.idempotency_key), LC_XID_STRING_LENGTH);
  assert_true(
      snprintf(command_id, sizeof(command_id), "%s", receipt.command_id) > 0);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "route-operation";
  entry.effect_id = "route-final-effect";
  entry.effect_key = "route-final-effect-key";
  entry.payload_digest = "sha256:route-final-effect";
  entry.kind = "route-test";
  entry.destination = "https://example.invalid/route-test";
  assert_lc_ok(lc_source_from_memory("route", 5U, &payload, &error), &error);
  assert_lc_ok(lc_outbox_transaction_append(transaction, &entry, payload,
                                            &outbox_receipt, &error),
               &error);
  assert_lc_ok(
      lc_outbox_transaction_commit(transaction, &commit_result, &error),
      &error);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_wait_command(outbox, command_id, 0L, &receipt, &error),
      LC_ERR_TIMEOUT);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_lc_ok(lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error),
               &error);
  assert_lc_ok(lc_outbox_dispatcher_next(dispatcher, 5000L, &job, &error),
               &error);
  assert_non_null(job);
  assert_string_equal(job->command_id, command_id);
  assert_lc_ok(lc_outbox_resume_command_by_id(outbox, job->command_id,
                                              &transaction, &receipt, &error),
               &error);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.result_code = "created";
  result.result_reference = "route-resource";
  assert_lc_ok(
      lc_outbox_transaction_complete_command(transaction, &result, &error),
      &error);
  assert_lc_ok(
      lc_outbox_transaction_commit(transaction, &commit_result, &error),
      &error);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_lc_ok(lc_outbox_job_complete(job, NULL, &error), &error);
  job = NULL;
  assert_lc_ok(
      lc_outbox_wait_command(outbox, command_id, 1000L, &receipt, &error),
      &error);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  lc_command_receipt_cleanup(&receipt);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-route";
  command.identity.command_type = "orders.create.v1";
  command.identity.idempotency_key = "route-command-failure";
  command.request_digest = "route-command-failure-digest";
  assert_lc_ok(lc_outbox_accept_command(outbox, &command, &transaction,
                                        &receipt, &error),
               &error);
  assert_non_null(transaction);
  assert_true(snprintf(failed_command_id, sizeof(failed_command_id), "%s",
                       receipt.command_id) > 0);
  assert_lc_ok(
      lc_outbox_transaction_commit(transaction, &commit_result, &error),
      &error);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(
      lc_outbox_wait_command(outbox, failed_command_id, 0L, &receipt, &error),
      LC_ERR_TIMEOUT);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_lc_ok(lc_outbox_resume_command_by_id(outbox, failed_command_id,
                                              &transaction, &receipt, &error),
               &error);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.failure_code = "declined";
  result.failure_message = "route request was declined";
  assert_lc_ok(lc_outbox_transaction_fail_command(transaction, &result, &error),
               &error);
  assert_lc_ok(
      lc_outbox_transaction_commit(transaction, &commit_result, &error),
      &error);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  assert_lc_ok(
      lc_outbox_wait_command(outbox, failed_command_id, 0L, &receipt, &error),
      &error);
  assert_int_equal(receipt.state, LC_COMMAND_FAILED);
  assert_string_equal(receipt.failure_code, "declined");
  lc_outbox_receipt_cleanup(&outbox_receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_direct_state_attachment_reopen_roundtrip(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  const void *bytes;
  size_t length;
  char root[256];
  char endpoint[320];
  char key[96];
  static const unsigned char attachment_payload[] = {'p', 'o', 'u', 'c', 'h'};
  int rc;

  (void)state;
  make_pouch_root("state-attachment", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);

  client = NULL;
  reader = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_list_req_init(&list_req);
  memset(&attachment_list, 0, sizeof(attachment_list));
  lc_attachment_get_op_init(&get_attachment_req);
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));

  open_pouch_client(endpoint, &client, &error);
  make_unique_name("pouch-direct-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"pouch-e2e\",\"value\":1,\"tags\":[\"pouch\"]}",
      &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lc_source_from_memory(attachment_payload, sizeof(attachment_payload),
                             &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.bin";
  attach_req.content_type = "application/octet-stream";
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "blob.bin");

  reader = NULL;
  open_pouch_client(endpoint, &reader, &error);
  list_req.lease.ns = lease->ns;
  list_req.lease.key = lease->key;
  list_req.lease.lease_id = lease->lease_id;
  list_req.lease.txn_id = lease->txn_id;
  list_req.lease.fencing_token = lease->fencing_token;
  rc = reader->list_attachments(reader, &list_req, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "blob.bin");

  get_attachment_req.lease = list_req.lease;
  get_attachment_req.selector.name = "blob.bin";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = reader->get_attachment(reader, &get_attachment_req, sink,
                              &get_attachment_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(attachment_payload));
  assert_memory_equal(bytes, attachment_payload, sizeof(attachment_payload));
  lc_sink_close(sink);
  sink = NULL;

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  lc_client_close(client);
  client = NULL;
  lc_client_close(reader);
  reader = NULL;

  open_pouch_client(endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = reader->get(reader, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_true(get_res.version >= 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"pouch-e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"pouch\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_attachment_get_res_cleanup(&get_attachment_res);
  lc_attachment_list_cleanup(&attachment_list);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void pouch_e2e_run_maintenance(const char *root, const char *ns,
                                      int force, int cleanup_only,
                                      long retention_cutoff, lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.ns = ns;
  maintenance_options.force = force;
  maintenance_options.cleanup_only = cleanup_only;
  maintenance_options.retention_updated_before_unix = retention_cutoff;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  if (force) {
    assert_true(maintenance_result.compacted || maintenance_result.skipped);
  }
  if (retention_cutoff > 0L) {
    assert_string_equal(maintenance_result.diagnostic, "retention-complete");
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static size_t pouch_e2e_query_key_count(lc_client *client, const char *ns,
                                        const char *selector_json,
                                        lc_error *error) {
  lc_query_key_handler handler;
  lc_query_req query_req;
  lc_query_res query_res;
  pouch_e2e_key_count count;
  int rc;

  memset(&handler, 0, sizeof(handler));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&count, 0, sizeof(count));
  handler.begin = pouch_e2e_key_begin;
  handler.chunk = pouch_e2e_key_chunk;
  handler.end = pouch_e2e_key_end;
  query_req.ns = ns;
  query_req.selector_json = selector_json;
  query_req.engine = "index";
  query_req.limit = 512L;
  rc = lc_query_keys(client, &query_req, &handler, &count, &query_res, error);
  assert_lc_ok(rc, error);
  lc_query_res_cleanup(&query_res);
  return count.rows;
}

static void pouch_e2e_write_segmented_docs_direct(const char *root,
                                                  const char *ns,
                                                  const char *kind,
                                                  size_t doc_count,
                                                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_options write_options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  char body[1024];
  char key[192];
  char pad[640];
  size_t index;
  int written;
  int rc;

  pouch = NULL;
  source = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_options, 0, sizeof(write_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(pad, 'x', sizeof(pad) - 1U);
  pad[sizeof(pad) - 1U] = '\0';
  open_options.segment_target_bytes = 4096UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 0;
  open_options.background_compaction_enabled_set = 1;
  open_options.query_engine = "index";
  write_options.content_type = "application/json";

  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  for (index = 0U; index < doc_count; ++index) {
    written = snprintf(key, sizeof(key), "pouch/large/%s/%04zu", kind, index);
    assert_true(written > 0 && (size_t)written < sizeof(key));
    written = snprintf(body, sizeof(body),
                       "{\"kind\":\"%s\",\"ordinal\":%zu,\"group\":\"%s\","
                       "\"tags\":[\"pouch\",\"large\",\"%s\"],\"pad\":\"%s\"}",
                       kind, index, index % 2U == 0U ? "even" : "odd",
                       index % 3U == 0U ? "planning" : "finance", pad);
    assert_true(written > 0 && (size_t)written < sizeof(body));
    rc = lc_source_from_memory(body, strlen(body), &source, error);
    assert_lc_ok(rc, error);
    rc = lc_pouch_state_write(pouch, ns, key, source, &write_options,
                              &write_result, error);
    lc_source_close(source);
    source = NULL;
    assert_lc_ok(rc, error);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
  }
  lc_pouch_close(pouch);
}

static void pouch_e2e_force_maintenance_expect_segments(const char *root,
                                                        const char *ns,
                                                        lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.ns = ns;
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  assert_true(maintenance_result.compacted);
  assert_true(maintenance_result.candidate_segment_count > 1UL);
  assert_true(maintenance_result.candidate_bytes > 4096UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static void pouch_e2e_reclaim_one_active_churn_segment(const char *root,
                                                       const char *ns,
                                                       const char *kind,
                                                       lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  char key[128];
  char body[192];
  size_t index;
  int rc;

  pouch = NULL;
  source = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  open_options.segment_target_bytes = 1024U * 1024U;
  open_options.terminal_reclaim_min_bytes = 1U;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  for (index = 0U; index < 256U; ++index) {
    assert_true(snprintf(key, sizeof(key), "pouch/churn/%03zu", index) > 0);
    assert_true(snprintf(body, sizeof(body),
                         "{\"kind\":\"%s\",\"ordinal\":%zu}", kind, index) > 0);
    rc = lc_source_from_memory(body, strlen(body), &source, error);
    assert_lc_ok(rc, error);
    rc = lc_pouch_state_write(pouch, ns, key, source, NULL, &write_result,
                              error);
    lc_source_close(source);
    source = NULL;
    assert_lc_ok(rc, error);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
  }
  for (index = 0U; index < 255U; ++index) {
    assert_true(snprintf(key, sizeof(key), "pouch/churn/%03zu", index) > 0);
    rc = lc_pouch_state_delete(pouch, ns, key, NULL, &write_result, error);
    assert_lc_ok(rc, error);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
  }
  maintenance_options.ns = ns;
  maintenance_options.terminal_reclaim = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  assert_true(maintenance_result.compacted);
  assert_int_equal(maintenance_result.candidate_segment_count, 1UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static int pouch_e2e_write_exact(int fd, const void *bytes, size_t length) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)bytes;
  while (length != 0U) {
    ssize_t written;

    written = write(fd, cursor, length);
    if (written > 0) {
      cursor += (size_t)written;
      length -= (size_t)written;
      continue;
    }
    if (written < 0 && errno == EINTR)
      continue;
    return 0;
  }
  return 1;
}

static int pouch_e2e_read_exact(int fd, void *bytes, size_t length) {
  unsigned char *cursor;

  cursor = (unsigned char *)bytes;
  while (length != 0U) {
    ssize_t received;

    received = read(fd, cursor, length);
    if (received > 0) {
      cursor += (size_t)received;
      length -= (size_t)received;
      continue;
    }
    if (received < 0 && errno == EINTR)
      continue;
    return 0;
  }
  return 1;
}

static int pouch_e2e_shared_churn_child(const char *root,
                                        pouch_e2e_churn_child_result *result) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  lc_error error;
  char body[320];
  char key[96];
  char pad[128];
  size_t index;
  int rc;

  pouch = NULL;
  source = NULL;
  memset(result, 0, sizeof(*result));
  result->rc = LC_ERR_INVALID;
  result->stage = 1;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  memset(pad, 'x', sizeof(pad) - 1U);
  pad[sizeof(pad) - 1U] = '\0';
  lc_error_init(&error);

  open_options.segment_target_bytes = POUCH_E2E_HARDENING_SEGMENT_BYTES;
  open_options.terminal_reclaim_min_bytes = 1U;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 1;
  open_options.compaction_interval_seconds = 3600U;
  open_options.single_writer_set = 1;
  open_options.single_writer = 0;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  if (rc != LC_OK)
    goto done;
  result->stage = 2;
  for (index = 0U; index < POUCH_E2E_HARDENING_CHURN_ROWS; ++index) {
    if (snprintf(key, sizeof(key), "state/churn/%04zu", index) < 0 ||
        snprintf(body, sizeof(body),
                 "{\"kind\":\"pouch-hardening-survivor\",\"ordinal\":%zu,"
                 "\"pad\":\"%s\"}",
                 index, pad) < 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    rc = lc_source_from_memory(body, strlen(body), &source, &error);
    if (rc != LC_OK)
      goto done;
    rc = lc_pouch_state_write(pouch, "hardening-churn", key, source, NULL,
                              &write_result, &error);
    lc_source_close(source);
    source = NULL;
    if (rc != LC_OK)
      goto done;
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }
  for (index = 0U; index + 1U < POUCH_E2E_HARDENING_CHURN_ROWS; ++index) {
    if (snprintf(key, sizeof(key), "state/churn/%04zu", index) < 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    rc = lc_pouch_state_delete(pouch, "hardening-churn", key, NULL,
                               &write_result, &error);
    if (rc != LC_OK)
      goto done;
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }
  result->stage = 3;
  maintenance_options.ns = "hardening-churn";
  maintenance_options.terminal_reclaim = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  if (rc == LC_OK) {
    result->compacted = maintenance_result.compacted;
    result->candidate_segment_count =
        maintenance_result.candidate_segment_count;
    if (!result->compacted || result->candidate_segment_count != 1UL)
      rc = LC_ERR_INVALID;
  }
  if (rc != LC_OK)
    goto done;
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  lc_pouch_close(pouch);
  pouch = NULL;

  result->stage = 4;
  memset(&open_options, 0, sizeof(open_options));
  open_options.segment_target_bytes = 4096U;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1U;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 0;
  open_options.single_writer_set = 1;
  open_options.single_writer = 0;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  if (rc != LC_OK)
    goto done;
  for (index = 0U; index < POUCH_E2E_HARDENING_MULTI_SEGMENT_ROWS; ++index) {
    if (snprintf(key, sizeof(key), "state/multi/%04zu", index) < 0 ||
        snprintf(body, sizeof(body),
                 "{\"kind\":\"pouch-hardening-multi-survivor\","
                 "\"ordinal\":%zu,\"pad\":\"%s\"}",
                 index, pad) < 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    rc = lc_source_from_memory(body, strlen(body), &source, &error);
    if (rc != LC_OK)
      goto done;
    rc = lc_pouch_state_write(pouch, "hardening-multi", key, source, NULL,
                              &write_result, &error);
    lc_source_close(source);
    source = NULL;
    if (rc != LC_OK)
      goto done;
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }
  for (index = 0U; index + 1U < POUCH_E2E_HARDENING_MULTI_SEGMENT_ROWS;
       ++index) {
    if (snprintf(key, sizeof(key), "state/multi/%04zu", index) < 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    rc = lc_pouch_state_delete(pouch, "hardening-multi", key, NULL,
                               &write_result, &error);
    if (rc != LC_OK)
      goto done;
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }
  result->stage = 5;
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  maintenance_options.ns = "hardening-multi";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  if (rc == LC_OK) {
    result->multi_segment_compacted = maintenance_result.compacted;
    result->multi_segment_candidate_count =
        maintenance_result.candidate_segment_count;
    if (!result->multi_segment_compacted ||
        result->multi_segment_candidate_count <= 1UL) {
      rc = LC_ERR_INVALID;
    }
  }

done:
  if (source != NULL)
    lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  if (pouch != NULL)
    lc_pouch_close(pouch);
  result->rc = rc;
  if (rc != LC_OK && error.message != NULL) {
    (void)snprintf(result->error_message, sizeof(result->error_message), "%s",
                   error.message);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK;
}

static void *pouch_e2e_dispatcher_hold_worker(void *context) {
  pouch_e2e_dispatcher_hold *hold;
  lc_outbox_job *job;
  lc_error error;
  int rc;

  hold = (pouch_e2e_dispatcher_hold *)context;
  job = NULL;
  lc_error_init(&error);
  rc = lc_outbox_dispatcher_next(hold->dispatcher, 0L, &job, &error);
  assert_int_equal(pthread_mutex_lock(&hold->mutex), 0);
  hold->rc = rc;
  if (error.message != NULL) {
    (void)snprintf(hold->error_message, sizeof(hold->error_message), "%s",
                   error.message);
  }
  hold->claimed = 1;
  assert_int_equal(pthread_cond_broadcast(&hold->cond), 0);
  while (!hold->release) {
    assert_int_equal(pthread_cond_wait(&hold->cond, &hold->mutex), 0);
  }
  assert_int_equal(pthread_mutex_unlock(&hold->mutex), 0);

  if (rc == LC_OK && job != NULL) {
    rc = lc_outbox_job_complete(job, NULL, &error);
    if (rc == LC_OK)
      job = NULL;
  }
  if (job != NULL)
    lc_outbox_job_close(job);

  assert_int_equal(pthread_mutex_lock(&hold->mutex), 0);
  hold->rc = rc;
  if (rc != LC_OK && error.message != NULL) {
    (void)snprintf(hold->error_message, sizeof(hold->error_message), "%s",
                   error.message);
  }
  hold->completed = 1;
  assert_int_equal(pthread_cond_broadcast(&hold->cond), 0);
  assert_int_equal(pthread_mutex_unlock(&hold->mutex), 0);
  lc_error_cleanup(&error);
  return NULL;
}

static void pouch_e2e_append_hardening_effect(lc_outbox *outbox, size_t index,
                                              lc_error *error) {
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_commit_result commit_result;
  lc_outbox_transaction *transaction;
  lc_source *payload;
  char operation_id[96];
  char effect_id[96];
  char effect_key[96];
  char payload_digest[112];
  char payload_bytes[96];
  int rc;

  transaction = NULL;
  payload = NULL;
  lc_outbox_entry_init(&entry);
  lc_outbox_receipt_init(&receipt);
  lc_outbox_commit_result_init(&commit_result);
  assert_true(snprintf(operation_id, sizeof(operation_id),
                       "pouch-hardening-operation-%04zu", index) > 0);
  assert_true(snprintf(effect_id, sizeof(effect_id), "effect-%04zu", index) >
              0);
  assert_true(snprintf(effect_key, sizeof(effect_key),
                       "pouch-hardening-key-%04zu", index) > 0);
  assert_true(snprintf(payload_digest, sizeof(payload_digest),
                       "sha256:pouch-hardening-%04zu", index) > 0);
  assert_true(snprintf(payload_bytes, sizeof(payload_bytes),
                       "{\"effect\":%zu,\"kind\":\"pouch-hardening\"}",
                       index) > 0);
  entry.operation_id = operation_id;
  entry.effect_id = effect_id;
  entry.effect_key = effect_key;
  entry.payload_digest = payload_digest;
  entry.kind = "pouch-hardening";
  entry.destination = "test://pouch-hardening";
  entry.content_type = "application/json";
  rc = lc_source_from_memory(payload_bytes, strlen(payload_bytes), &payload,
                             error);
  assert_lc_ok(rc, error);
  rc = lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, error);
  lc_source_close(payload);
  payload = NULL;
  assert_lc_ok(rc, error);
  assert_non_null(transaction);
  rc = lc_outbox_transaction_commit(transaction, &commit_result, error);
  assert_lc_ok(rc, error);
  assert_int_equal(commit_result.outbox_receipt_count, 1U);
  lc_outbox_transaction_close(transaction);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_receipt_cleanup(&receipt);
}

static int pouch_e2e_outbox_effect_key(char *buffer, size_t capacity,
                                       const char *label, size_t index) {
  int written;

  written = snprintf(buffer, capacity, "pouch-e2e-%s-key-%04zu", label, index);
  return written > 0 && (size_t)written < capacity;
}

static int pouch_e2e_open_shared_client(const char *root, lc_client **out,
                                        lc_error *error) {
  lc_client_config config;
  lc_pouch_settings settings;
  const char *endpoints[1];
  char endpoint[320];
  int rc;

  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) < 0 ||
      strlen(endpoint) >= sizeof(endpoint))
    return LC_ERR_INVALID;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  /* A small, explicitly shared layout makes every scenario span multiple
   * segments. It keeps the process topology production-shaped while ensuring
   * that cross-segment replay and writer coordination are exercised too. */
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = 0;
  settings.segment_target_bytes = 4096U;
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.pouch_settings = &settings;
  rc = lc_client_open(&config, out, error);
  return rc;
}

static int pouch_e2e_append_shared_outbox_effect(lc_outbox *outbox,
                                                 const char *label,
                                                 size_t index,
                                                 lc_error *error) {
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_commit_result commit_result;
  lc_outbox_transaction *transaction;
  lc_source *payload;
  char operation_id[128];
  char effect_id[128];
  char effect_key[128];
  char payload_digest[160];
  char payload_bytes[160];
  int rc;

  transaction = NULL;
  payload = NULL;
  lc_outbox_entry_init(&entry);
  lc_outbox_receipt_init(&receipt);
  lc_outbox_commit_result_init(&commit_result);
  if (snprintf(operation_id, sizeof(operation_id), "pouch-e2e-%s-op-%04zu",
               label, index) < 0 ||
      snprintf(effect_id, sizeof(effect_id), "pouch-e2e-%s-effect-%04zu", label,
               index) < 0 ||
      !pouch_e2e_outbox_effect_key(effect_key, sizeof(effect_key), label,
                                   index) ||
      snprintf(payload_digest, sizeof(payload_digest),
               "sha256:pouch-e2e-%s-%04zu", label, index) < 0 ||
      snprintf(payload_bytes, sizeof(payload_bytes),
               "{\"effect\":%zu,\"scenario\":\"%s\"}", index, label) < 0) {
    rc = LC_ERR_INVALID;
    goto done;
  }
  entry.operation_id = operation_id;
  entry.effect_id = effect_id;
  entry.effect_key = effect_key;
  entry.payload_digest = payload_digest;
  entry.kind = "pouch-e2e";
  entry.destination = "test://pouch-e2e";
  entry.content_type = "application/json";
  rc = lc_source_from_memory(payload_bytes, strlen(payload_bytes), &payload,
                             error);
  if (rc != LC_OK)
    goto done;
  rc = lc_outbox_append(outbox, &entry, payload, &transaction, &receipt, error);
  if (rc != LC_OK)
    goto done;
  rc = lc_outbox_transaction_commit(transaction, &commit_result, error);
  if (rc == LC_OK && commit_result.outbox_receipt_count != 1U)
    rc = LC_ERR_INVALID;

done:
  if (transaction != NULL)
    lc_outbox_transaction_close(transaction);
  if (payload != NULL)
    lc_source_close(payload);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_receipt_cleanup(&receipt);
  return rc;
}

static void pouch_e2e_seed_shared_outbox(const char *root, const char *ns,
                                         const char *label, size_t count,
                                         lc_error *error) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  size_t index;
  int rc;

  client = NULL;
  outbox = NULL;
  lc_outbox_config_init(&config);
  rc = pouch_e2e_open_shared_client(root, &client, error);
  assert_lc_ok(rc, error);
  config.ns = ns;
  config.owner = "pouch-e2e-shared-producer";
  config.notification_capacity = count;
  config.recovery_interval_seconds = 1L;
  rc = lc_client_new_outbox(client, &config, &outbox, error);
  assert_lc_ok(rc, error);
  for (index = 0U; index < count; ++index) {
    rc = pouch_e2e_append_shared_outbox_effect(outbox, label, index, error);
    assert_lc_ok(rc, error);
  }
  lc_outbox_close(outbox);
  lc_client_close(client);
}

static int pouch_e2e_outbox_effect_index(const char *effect_key,
                                         const char *label, size_t count,
                                         size_t *out) {
  char expected[128];
  size_t index;

  if (effect_key == NULL || out == NULL)
    return 0;
  for (index = 0U; index < count; ++index) {
    if (!pouch_e2e_outbox_effect_key(expected, sizeof(expected), label, index))
      return 0;
    if (strcmp(effect_key, expected) == 0) {
      *out = index;
      return 1;
    }
  }
  return 0;
}

static int pouch_e2e_update_shared_outbox_state(lc_lease *lease,
                                                int expect_checkpoint,
                                                lc_error *error) {
  static const char checkpoint[] = "{\"checkpoint\":\"shared-outbox\"}";
  static const char attachment_name[] = "checkpoint.txt";
  static const char attachment_body[] = "shared-outbox-checkpoint";
  lc_attach_req attach;
  lc_attach_res attach_res;
  lc_attachment_get_req attachment_get;
  lc_attachment_get_res attachment_res;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_source *source;
  const void *bytes;
  size_t length;
  int rc;

  sink = NULL;
  source = NULL;
  bytes = NULL;
  length = 0U;
  lc_attach_req_init(&attach);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_get_req_init(&attachment_get);
  memset(&attachment_res, 0, sizeof(attachment_res));
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  if (expect_checkpoint) {
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK)
      goto done;
    rc = lease->get(lease, sink, &get_opts, &get_res, error);
    if (rc != LC_OK)
      goto done;
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc != LC_OK)
      goto done;
    if (get_res.no_content || length != sizeof(checkpoint) - 1U ||
        memcmp(bytes, checkpoint, sizeof(checkpoint) - 1U) != 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    lc_sink_close(sink);
    sink = NULL;
    lc_get_res_cleanup(&get_res);
    memset(&get_res, 0, sizeof(get_res));
    attachment_get.selector.name = attachment_name;
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK)
      goto done;
    rc = lc_lease_get_attachment(lease, &attachment_get, sink, &attachment_res,
                                 error);
    if (rc != LC_OK)
      goto done;
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc != LC_OK)
      goto done;
    if (length != sizeof(attachment_body) - 1U ||
        memcmp(bytes, attachment_body, sizeof(attachment_body) - 1U) != 0) {
      rc = LC_ERR_INVALID;
      goto done;
    }
    lc_sink_close(sink);
    sink = NULL;
    lc_attachment_get_res_cleanup(&attachment_res);
    memset(&attachment_res, 0, sizeof(attachment_res));
  }
  rc = lc_source_from_memory(checkpoint, sizeof(checkpoint) - 1U, &source,
                             error);
  if (rc != LC_OK)
    goto done;
  rc = lc_lease_update(lease, source, NULL, error);
  if (rc != LC_OK)
    goto done;
  lc_source_close(source);
  source = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc != LC_OK)
    goto done;
  memset(&get_res, 0, sizeof(get_res));
  rc = lease->get(lease, sink, &get_opts, &get_res, error);
  if (rc != LC_OK)
    goto done;
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  if (rc != LC_OK)
    goto done;
  if (get_res.no_content || length != sizeof(checkpoint) - 1U ||
      memcmp(bytes, checkpoint, sizeof(checkpoint) - 1U) != 0)
    rc = LC_ERR_INVALID;
  if (rc != LC_OK)
    goto done;
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_source_from_memory(attachment_body, sizeof(attachment_body) - 1U,
                             &source, error);
  if (rc != LC_OK)
    goto done;
  attach.name = attachment_name;
  attach.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach, source, &attach_res, error);

done:
  if (source != NULL)
    lc_source_close(source);
  if (sink != NULL)
    lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_attach_res_cleanup(&attach_res);
  lc_attachment_get_res_cleanup(&attachment_res);
  return rc;
}

static int pouch_e2e_shared_outbox_child(
    const char *root, const char *ns, const char *owner, const char *label,
    size_t total_count, size_t delivery_count, long claim_ttl_seconds,
    int with_state, int expect_state_checkpoint, int ready_fd, int start_fd,
    int claimed_fd, int release_fd, pouch_e2e_outbox_child_result *result) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_dispatcher *dispatcher;
  lc_error error;
  size_t delivered;
  char start;
  int held;
  int rc;

  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  delivered = 0U;
  held = 0;
  rc = LC_ERR_INVALID;
  memset(result, 0, sizeof(*result));
  result->rc = LC_ERR_INVALID;
  result->stage = 1;
  lc_error_init(&error);
  lc_outbox_config_init(&config);
  rc = pouch_e2e_open_shared_client(root, &client, &error);
  if (rc != LC_OK)
    goto done;
  config.ns = ns;
  config.owner = owner;
  config.claim_ttl_seconds = claim_ttl_seconds;
  config.notification_capacity = total_count;
  /* A shared root intentionally has a positive periodic recovery cadence;
   * the first blocking demand below still requests immediate startup repair. */
  config.recovery_interval_seconds = 1L;
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  if (rc != LC_OK)
    goto done;
  rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
  if (rc != LC_OK)
    goto done;
  result->stage = 2;
  start = '\0';
  if (!pouch_e2e_write_exact(ready_fd, "r", 1U) ||
      !pouch_e2e_read_exact(start_fd, &start, 1U) || start != 's') {
    rc = LC_ERR_INVALID;
    goto done;
  }
  held = 0;
  result->stage = 3;
  while (delivered < delivery_count) {
    lc_outbox_job *job;
    size_t index;

    job = NULL;
    rc = with_state ? lc_outbox_dispatcher_next_with_state(dispatcher, -1L,
                                                           &job, &error)
                    : lc_outbox_dispatcher_next(dispatcher, -1L, &job, &error);
    if (rc != LC_OK || job == NULL)
      goto done;
    if (with_state) {
      lc_lease *state_lease = lc_outbox_job_state(job);

      if (state_lease == NULL) {
        lc_outbox_job_close(job);
        rc = LC_ERR_INVALID;
        goto done;
      }
      result->stage = expect_state_checkpoint ? 5 : 4;
      rc = pouch_e2e_update_shared_outbox_state(
          state_lease, expect_state_checkpoint, &error);
      if (rc != LC_OK) {
        lc_outbox_job_close(job);
        goto done;
      }
      expect_state_checkpoint = 0;
      result->stage = 3;
    }
    if (!pouch_e2e_outbox_effect_index(job->effect_key, label, total_count,
                                       &index) ||
        (result->delivered_mask & (1UL << index)) != 0UL) {
      lc_outbox_job_close(job);
      rc = LC_ERR_INVALID;
      goto done;
    }
    if (claimed_fd >= 0 && !held) {
      char release;

      if (!pouch_e2e_write_exact(claimed_fd, "c", 1U) ||
          !pouch_e2e_read_exact(release_fd, &release, 1U) || release != 'q') {
        lc_outbox_job_close(job);
        rc = LC_ERR_INVALID;
        goto done;
      }
      held = 1;
    }
    rc = lc_outbox_job_complete(job, NULL, &error);
    if (rc != LC_OK) {
      lc_outbox_job_close(job);
      goto done;
    }
    result->delivered_mask |= 1UL << index;
    ++delivered;
    result->stage = 3;
  }
  result->delivered = delivered;
  result->stage = 4;

done:
  if (dispatcher != NULL)
    lc_outbox_dispatcher_close(dispatcher);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  result->rc = rc;
  if (rc != LC_OK && error.message != NULL) {
    (void)snprintf(result->error_message, sizeof(result->error_message), "%s",
                   error.message);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK;
}

static void pouch_e2e_outbox_child_init(pouch_e2e_outbox_child *child) {
  memset(child, 0, sizeof(*child));
  child->pid = -1;
  child->ready_read = -1;
  child->start_write = -1;
  child->result_read = -1;
  child->claimed_read = -1;
  child->release_write = -1;
}

static int pouch_e2e_launch_shared_outbox_child(
    const char *root, const char *ns, const char *owner, const char *label,
    size_t total_count, size_t delivery_count, long claim_ttl_seconds,
    int with_state, int expect_state_checkpoint, int hold_first,
    pouch_e2e_outbox_child *child) {
  int ready_pipe[2];
  int start_pipe[2];
  int result_pipe[2];
  int claimed_pipe[2];
  int release_pipe[2];
  pid_t pid;

  ready_pipe[0] = ready_pipe[1] = -1;
  start_pipe[0] = start_pipe[1] = -1;
  result_pipe[0] = result_pipe[1] = -1;
  claimed_pipe[0] = claimed_pipe[1] = -1;
  release_pipe[0] = release_pipe[1] = -1;
  pouch_e2e_outbox_child_init(child);
  if (pipe(ready_pipe) != 0 || pipe(start_pipe) != 0 ||
      pipe(result_pipe) != 0 ||
      (hold_first && (pipe(claimed_pipe) != 0 || pipe(release_pipe) != 0)))
    goto failed;
  pid = fork();
  if (pid < 0)
    goto failed;
  if (pid == 0) {
    pouch_e2e_outbox_child_result result;
    int ok;

    (void)close(ready_pipe[0]);
    (void)close(start_pipe[1]);
    (void)close(result_pipe[0]);
    if (hold_first) {
      (void)close(claimed_pipe[0]);
      (void)close(release_pipe[1]);
    }
    ok = pouch_e2e_shared_outbox_child(
        root, ns, owner, label, total_count, delivery_count, claim_ttl_seconds,
        with_state, expect_state_checkpoint, ready_pipe[1], start_pipe[0],
        hold_first ? claimed_pipe[1] : -1, hold_first ? release_pipe[0] : -1,
        &result);
    (void)pouch_e2e_write_exact(result_pipe[1], &result, sizeof(result));
    (void)close(ready_pipe[1]);
    (void)close(start_pipe[0]);
    (void)close(result_pipe[1]);
    if (hold_first) {
      (void)close(claimed_pipe[1]);
      (void)close(release_pipe[0]);
    }
    _exit(ok ? 0 : 1);
  }
  (void)close(ready_pipe[1]);
  (void)close(start_pipe[0]);
  (void)close(result_pipe[1]);
  child->pid = pid;
  child->ready_read = ready_pipe[0];
  child->start_write = start_pipe[1];
  child->result_read = result_pipe[0];
  if (hold_first) {
    (void)close(claimed_pipe[1]);
    (void)close(release_pipe[0]);
    child->claimed_read = claimed_pipe[0];
    child->release_write = release_pipe[1];
  }
  return LC_OK;

failed:
  if (ready_pipe[0] >= 0)
    (void)close(ready_pipe[0]);
  if (ready_pipe[1] >= 0)
    (void)close(ready_pipe[1]);
  if (start_pipe[0] >= 0)
    (void)close(start_pipe[0]);
  if (start_pipe[1] >= 0)
    (void)close(start_pipe[1]);
  if (result_pipe[0] >= 0)
    (void)close(result_pipe[0]);
  if (result_pipe[1] >= 0)
    (void)close(result_pipe[1]);
  if (claimed_pipe[0] >= 0)
    (void)close(claimed_pipe[0]);
  if (claimed_pipe[1] >= 0)
    (void)close(claimed_pipe[1]);
  if (release_pipe[0] >= 0)
    (void)close(release_pipe[0]);
  if (release_pipe[1] >= 0)
    (void)close(release_pipe[1]);
  return LC_ERR_INVALID;
}

static void
pouch_e2e_wait_shared_outbox_child_ready(pouch_e2e_outbox_child *child) {
  char ready;
  pouch_e2e_outbox_child_result result;
  int status;

  if (!pouch_e2e_read_exact(child->ready_read, &ready, 1U)) {
    memset(&result, 0, sizeof(result));
    (void)pouch_e2e_read_exact(child->result_read, &result, sizeof(result));
    (void)close(child->ready_read);
    child->ready_read = -1;
    (void)close(child->result_read);
    child->result_read = -1;
    (void)waitpid(child->pid, &status, 0);
    child->pid = -1;
    print_message("shared outbox child failed before ready at stage %d: %s\n",
                  result.stage, result.error_message);
    assert_int_equal(result.rc, LC_OK);
  }
  assert_int_equal(ready, 'r');
  assert_int_equal(close(child->ready_read), 0);
  child->ready_read = -1;
}

static void pouch_e2e_start_shared_outbox_child(pouch_e2e_outbox_child *child) {
  assert_true(pouch_e2e_write_exact(child->start_write, "s", 1U));
  assert_int_equal(close(child->start_write), 0);
  child->start_write = -1;
}

static void
pouch_e2e_release_shared_outbox_child(pouch_e2e_outbox_child *child) {
  assert_true(pouch_e2e_write_exact(child->release_write, "q", 1U));
  assert_int_equal(close(child->release_write), 0);
  child->release_write = -1;
}

static pouch_e2e_outbox_child_result
pouch_e2e_collect_shared_outbox_child(pouch_e2e_outbox_child *child) {
  pouch_e2e_outbox_child_result result;
  int status;

  memset(&result, 0, sizeof(result));
  assert_true(
      pouch_e2e_read_exact(child->result_read, &result, sizeof(result)));
  assert_int_equal(close(child->result_read), 0);
  child->result_read = -1;
  assert_int_equal(waitpid(child->pid, &status, 0), child->pid);
  child->pid = -1;
  assert_true(WIFEXITED(status));
  if (result.rc != LC_OK) {
    print_message("shared outbox child failed at stage %d: %s\n", result.stage,
                  result.error_message);
  }
  assert_int_equal(result.rc, LC_OK);
  assert_int_equal(WEXITSTATUS(status), 0);
  return result;
}

static int pouch_e2e_command_effect_once(const char *root,
                                         const char *command_id,
                                         int *performed) {
  char effects_dir[384];
  char marker[448];

  if (root == NULL || command_id == NULL || performed == NULL ||
      snprintf(effects_dir, sizeof(effects_dir), "%s/command-effects", root) <
          0 ||
      snprintf(marker, sizeof(marker), "%s/%s", effects_dir, command_id) < 0) {
    return LC_ERR_INVALID;
  }
  if (mkdir(effects_dir, 0700) != 0 && errno != EEXIST) {
    return LC_ERR_TRANSPORT;
  }
  if (mkdir(marker, 0700) == 0) {
    *performed = 1;
    return LC_OK;
  }
  if (errno == EEXIST) {
    *performed = 0;
    return LC_OK;
  }
  return LC_ERR_TRANSPORT;
}

static int pouch_e2e_seed_shared_command(const char *root, const char *ns,
                                         const char *idempotency_key,
                                         char command_id[48], lc_error *error) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_command_request command;
  lc_command_receipt command_receipt;
  lc_outbox_transaction *transaction;
  lc_outbox_entry entry;
  lc_outbox_receipt outbox_receipt;
  lc_outbox_commit_result commit_result;
  lc_source *payload;
  char operation_id[128];
  char effect_key[128];
  int rc;

  client = NULL;
  outbox = NULL;
  transaction = NULL;
  payload = NULL;
  lc_command_receipt_init(&command_receipt);
  lc_outbox_receipt_init(&outbox_receipt);
  lc_outbox_commit_result_init(&commit_result);
  rc = pouch_e2e_open_shared_client(root, &client, error);
  if (rc != LC_OK)
    goto done;
  lc_outbox_config_init(&config);
  config.ns = ns;
  config.owner = "pouch-e2e-command-producer";
  config.notification_capacity = 8U;
  config.claim_ttl_seconds = 1L;
  config.recovery_interval_seconds = 1L;
  rc = lc_client_new_outbox(client, &config, &outbox, error);
  if (rc != LC_OK)
    goto done;
  lc_command_request_init(&command);
  command.identity.scope = "pouch-e2e-command";
  command.identity.command_type = "effects.create.v1";
  command.identity.idempotency_key = idempotency_key;
  command.request_digest = "sha256:pouch-e2e-command";
  rc = lc_outbox_accept_command(outbox, &command, &transaction,
                                &command_receipt, error);
  if (rc != LC_OK || transaction == NULL)
    goto done;
  if (snprintf(command_id, 48U, "%s", command_receipt.command_id) <= 0) {
    rc = LC_ERR_INVALID;
    goto done;
  }
  lc_outbox_entry_init(&entry);
  if (snprintf(operation_id, sizeof(operation_id), "operation-%s",
               idempotency_key) < 0 ||
      snprintf(effect_key, sizeof(effect_key), "effect-%s", idempotency_key) <
          0) {
    rc = LC_ERR_INVALID;
    goto done;
  }
  entry.operation_id = operation_id;
  entry.effect_id = "final-effect";
  entry.effect_key = effect_key;
  entry.payload_digest = "sha256:pouch-e2e-command-effect";
  entry.kind = "pouch-e2e-command";
  entry.destination = "test://pouch-e2e-command";
  entry.content_type = "application/json";
  rc = lc_source_from_memory("{}", 2U, &payload, error);
  if (rc != LC_OK)
    goto done;
  rc = lc_outbox_transaction_append(transaction, &entry, payload,
                                    &outbox_receipt, error);
  if (rc != LC_OK)
    goto done;
  rc = lc_outbox_transaction_commit(transaction, &commit_result, error);

done:
  if (transaction != NULL)
    lc_outbox_transaction_close(transaction);
  if (payload != NULL)
    lc_source_close(payload);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_receipt_cleanup(&outbox_receipt);
  lc_command_receipt_cleanup(&command_receipt);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  return rc;
}

static int pouch_e2e_shared_command_child(
    const char *root, const char *ns, const char *owner,
    pouch_e2e_command_child_mode mode, int ready_fd, int start_fd, int phase_fd,
    int release_fd, pouch_e2e_command_child_result *result) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_command_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_command_result command_result;
  lc_outbox_commit_result commit_result;
  char start;
  int rc;

  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  transaction = NULL;
  start = '\0';
  rc = LC_ERR_INVALID;
  memset(result, 0, sizeof(*result));
  result->rc = LC_ERR_INVALID;
  result->stage = 1;
  lc_command_receipt_init(&receipt);
  lc_outbox_commit_result_init(&commit_result);
  {
    lc_error error;

    lc_error_init(&error);
    rc = pouch_e2e_open_shared_client(root, &client, &error);
    if (rc == LC_OK) {
      lc_outbox_config_init(&config);
      config.ns = ns;
      config.owner = owner;
      /* Normal completion must not race a one-second claim while peer
       * processes commit. Only deliberate crash/recovery needs fast expiry. */
      config.claim_ttl_seconds =
          mode == POUCH_E2E_COMMAND_CHILD_COMPLETE ? 30L : 1L;
      config.notification_capacity = 8U;
      config.recovery_interval_seconds = 1L;
      rc = lc_client_new_outbox(client, &config, &outbox, &error);
    }
    if (rc == LC_OK)
      rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
    result->stage = 2;
    if (rc == LC_OK &&
        (!pouch_e2e_write_exact(ready_fd, "r", 1U) ||
         !pouch_e2e_read_exact(start_fd, &start, 1U) || start != 's')) {
      rc = LC_ERR_INVALID;
    }
    if (rc == LC_OK) {
      result->stage = 3;
      rc = lc_outbox_dispatcher_next(dispatcher, -1L, &job, &error);
    }
    if (rc == LC_OK && (job == NULL || job->command_id == NULL ||
                        snprintf(result->command_id, sizeof(result->command_id),
                                 "%s", job->command_id) <= 0)) {
      rc = LC_ERR_INVALID;
    }
    if (rc == LC_OK)
      rc = lc_outbox_get_command_receipt_by_id(outbox, job->command_id,
                                               &receipt, &error);
    if (rc == LC_OK && receipt.state == LC_COMMAND_PENDING) {
      rc = pouch_e2e_command_effect_once(root, job->command_id,
                                         &result->performed_foreign_effect);
      if (rc == LC_OK &&
          mode == POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_FOREIGN_EFFECT) {
        char release;

        result->stage = 4;
        if (!pouch_e2e_write_exact(phase_fd, "f", 1U) ||
            !pouch_e2e_read_exact(release_fd, &release, 1U)) {
          rc = LC_ERR_INVALID;
        }
      }
      if (rc == LC_OK) {
        lc_command_receipt_cleanup(&receipt);
        rc = lc_outbox_resume_command_by_id(outbox, job->command_id,
                                            &transaction, &receipt, &error);
      }
      if (rc == LC_OK && transaction == NULL) {
        result->saw_terminal_command = 1;
      } else if (rc == LC_OK) {
        lc_command_result_init(&command_result);
        command_result.result_code = "created";
        rc = lc_outbox_transaction_complete_command(transaction,
                                                    &command_result, &error);
        if (rc == LC_OK)
          rc =
              lc_outbox_transaction_commit(transaction, &commit_result, &error);
        lc_outbox_commit_result_cleanup(&commit_result);
        lc_outbox_transaction_close(transaction);
        transaction = NULL;
      }
      if (rc == LC_OK &&
          mode == POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_COMMAND_COMMIT) {
        char release;

        result->stage = 5;
        if (!pouch_e2e_write_exact(phase_fd, "t", 1U) ||
            !pouch_e2e_read_exact(release_fd, &release, 1U)) {
          rc = LC_ERR_INVALID;
        }
      }
    } else if (rc == LC_OK) {
      result->saw_terminal_command = 1;
    }
    if (rc == LC_OK) {
      result->stage = 6;
      rc = lc_outbox_job_complete(job, NULL, &error);
      if (rc == LC_OK)
        job = NULL;
    }
    if (rc != LC_OK && error.message != NULL) {
      (void)snprintf(result->error_message, sizeof(result->error_message), "%s",
                     error.message);
    }
    lc_error_cleanup(&error);
  }
  if (transaction != NULL)
    lc_outbox_transaction_close(transaction);
  if (job != NULL)
    lc_outbox_job_close(job);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_command_receipt_cleanup(&receipt);
  if (dispatcher != NULL)
    lc_outbox_dispatcher_close(dispatcher);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  result->rc = rc;
  return rc == LC_OK;
}

static void pouch_e2e_command_child_init(pouch_e2e_command_child *child) {
  memset(child, 0, sizeof(*child));
  child->pid = -1;
  child->ready_read = -1;
  child->start_write = -1;
  child->result_read = -1;
  child->phase_read = -1;
  child->release_write = -1;
}

static int pouch_e2e_launch_shared_command_child(
    const char *root, const char *ns, const char *owner,
    pouch_e2e_command_child_mode mode, pouch_e2e_command_child *child) {
  int ready_pipe[2];
  int start_pipe[2];
  int result_pipe[2];
  int phase_pipe[2];
  int release_pipe[2];
  pid_t pid;

  ready_pipe[0] = ready_pipe[1] = -1;
  start_pipe[0] = start_pipe[1] = -1;
  result_pipe[0] = result_pipe[1] = -1;
  phase_pipe[0] = phase_pipe[1] = -1;
  release_pipe[0] = release_pipe[1] = -1;
  pouch_e2e_command_child_init(child);
  if (pipe(ready_pipe) != 0 || pipe(start_pipe) != 0 ||
      pipe(result_pipe) != 0 || pipe(phase_pipe) != 0 ||
      pipe(release_pipe) != 0) {
    goto failed;
  }
  pid = fork();
  if (pid < 0)
    goto failed;
  if (pid == 0) {
    pouch_e2e_command_child_result result;
    int ok;

    (void)close(ready_pipe[0]);
    (void)close(start_pipe[1]);
    (void)close(result_pipe[0]);
    (void)close(phase_pipe[0]);
    (void)close(release_pipe[1]);
    ok = pouch_e2e_shared_command_child(root, ns, owner, mode, ready_pipe[1],
                                        start_pipe[0], phase_pipe[1],
                                        release_pipe[0], &result);
    (void)pouch_e2e_write_exact(result_pipe[1], &result, sizeof(result));
    (void)close(ready_pipe[1]);
    (void)close(start_pipe[0]);
    (void)close(result_pipe[1]);
    (void)close(phase_pipe[1]);
    (void)close(release_pipe[0]);
    _exit(ok ? 0 : 1);
  }
  (void)close(ready_pipe[1]);
  (void)close(start_pipe[0]);
  (void)close(result_pipe[1]);
  (void)close(phase_pipe[1]);
  (void)close(release_pipe[0]);
  child->pid = pid;
  child->ready_read = ready_pipe[0];
  child->start_write = start_pipe[1];
  child->result_read = result_pipe[0];
  child->phase_read = phase_pipe[0];
  child->release_write = release_pipe[1];
  return LC_OK;

failed:
  if (ready_pipe[0] >= 0)
    (void)close(ready_pipe[0]);
  if (ready_pipe[1] >= 0)
    (void)close(ready_pipe[1]);
  if (start_pipe[0] >= 0)
    (void)close(start_pipe[0]);
  if (start_pipe[1] >= 0)
    (void)close(start_pipe[1]);
  if (result_pipe[0] >= 0)
    (void)close(result_pipe[0]);
  if (result_pipe[1] >= 0)
    (void)close(result_pipe[1]);
  if (phase_pipe[0] >= 0)
    (void)close(phase_pipe[0]);
  if (phase_pipe[1] >= 0)
    (void)close(phase_pipe[1]);
  if (release_pipe[0] >= 0)
    (void)close(release_pipe[0]);
  if (release_pipe[1] >= 0)
    (void)close(release_pipe[1]);
  return LC_ERR_INVALID;
}

static void
pouch_e2e_wait_shared_command_child_ready(pouch_e2e_command_child *child) {
  char ready;

  assert_true(pouch_e2e_read_exact(child->ready_read, &ready, 1U));
  assert_int_equal(ready, 'r');
  assert_int_equal(close(child->ready_read), 0);
  child->ready_read = -1;
}

static void
pouch_e2e_start_shared_command_child(pouch_e2e_command_child *child) {
  assert_true(pouch_e2e_write_exact(child->start_write, "s", 1U));
  assert_int_equal(close(child->start_write), 0);
  child->start_write = -1;
}

static pouch_e2e_command_child_result
pouch_e2e_collect_shared_command_child(pouch_e2e_command_child *child) {
  pouch_e2e_command_child_result result;
  int status;

  memset(&result, 0, sizeof(result));
  assert_true(
      pouch_e2e_read_exact(child->result_read, &result, sizeof(result)));
  assert_int_equal(close(child->result_read), 0);
  child->result_read = -1;
  assert_int_equal(close(child->phase_read), 0);
  child->phase_read = -1;
  assert_int_equal(close(child->release_write), 0);
  child->release_write = -1;
  assert_int_equal(waitpid(child->pid, &status, 0), child->pid);
  child->pid = -1;
  assert_true(WIFEXITED(status));
  if (result.rc != LC_OK) {
    print_message("shared command child failed at stage %d: %s\n", result.stage,
                  result.error_message);
  }
  assert_int_equal(result.rc, LC_OK);
  assert_int_equal(WEXITSTATUS(status), 0);
  return result;
}

static void
pouch_e2e_kill_shared_command_child(pouch_e2e_command_child *child) {
  int status;

  assert_int_equal(kill(child->pid, SIGKILL), 0);
  assert_int_equal(waitpid(child->pid, &status, 0), child->pid);
  child->pid = -1;
  assert_true(WIFSIGNALED(status));
  assert_int_equal(WTERMSIG(status), SIGKILL);
  assert_int_equal(close(child->result_read), 0);
  child->result_read = -1;
  assert_int_equal(close(child->phase_read), 0);
  child->phase_read = -1;
  assert_int_equal(close(child->release_write), 0);
  child->release_write = -1;
}

static void pouch_e2e_command_wait_pending_hook(void *context) {
  pouch_e2e_command_wait_hook *hook = (pouch_e2e_command_wait_hook *)context;

  if (hook != NULL && !hook->signaled) {
    hook->signaled = 1;
    (void)pouch_e2e_write_exact(hook->pending_fd, "p", 1U);
  }
}

static int pouch_e2e_command_waiter_child(
    const char *root, const char *ns, const char *command_id, long timeout_ms,
    int ready_fd, int pending_fd, pouch_e2e_command_waiter_result *result) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_command_receipt receipt;
  lc_error error;
  pouch_e2e_command_wait_hook hook;
  int rc;

  client = NULL;
  outbox = NULL;
  memset(result, 0, sizeof(*result));
  result->rc = LC_ERR_INVALID;
  lc_error_init(&error);
  lc_command_receipt_init(&receipt);
  memset(&hook, 0, sizeof(hook));
  hook.pending_fd = pending_fd;
  rc = pouch_e2e_open_shared_client(root, &client, &error);
  if (rc == LC_OK) {
    lc_outbox_config_init(&config);
    config.ns = ns;
    config.owner = "pouch-e2e-command-waiter";
    rc = lc_client_new_outbox(client, &config, &outbox, &error);
  }
  if (rc == LC_OK && !pouch_e2e_write_exact(ready_fd, "r", 1U))
    rc = LC_ERR_INVALID;
  if (rc == LC_OK) {
    lc_outbox_test_after_command_wait_pending_read_hook =
        pouch_e2e_command_wait_pending_hook;
    lc_outbox_test_after_command_wait_pending_read_context = &hook;
    rc = lc_outbox_wait_command(outbox, command_id, timeout_ms, &receipt,
                                &error);
    lc_outbox_test_after_command_wait_pending_read_hook = NULL;
    lc_outbox_test_after_command_wait_pending_read_context = NULL;
  }
  result->rc = rc;
  result->state = receipt.state;
  if (rc != LC_OK && error.message != NULL) {
    (void)snprintf(result->error_message, sizeof(result->error_message), "%s",
                   error.message);
  }
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  if (outbox != NULL)
    lc_outbox_close(outbox);
  if (client != NULL)
    lc_client_close(client);
  return rc == LC_OK;
}

static void pouch_e2e_command_waiter_init(pouch_e2e_command_waiter *waiter) {
  memset(waiter, 0, sizeof(*waiter));
  waiter->pid = -1;
  waiter->ready_read = -1;
  waiter->pending_read = -1;
  waiter->result_read = -1;
}

static int pouch_e2e_launch_command_waiter(const char *root, const char *ns,
                                           const char *command_id,
                                           long timeout_ms,
                                           pouch_e2e_command_waiter *waiter) {
  int ready_pipe[2];
  int pending_pipe[2];
  int result_pipe[2];
  pid_t pid;

  ready_pipe[0] = ready_pipe[1] = -1;
  pending_pipe[0] = pending_pipe[1] = -1;
  result_pipe[0] = result_pipe[1] = -1;
  pouch_e2e_command_waiter_init(waiter);
  if (pipe(ready_pipe) != 0 || pipe(pending_pipe) != 0 ||
      pipe(result_pipe) != 0) {
    goto failed;
  }
  pid = fork();
  if (pid < 0)
    goto failed;
  if (pid == 0) {
    pouch_e2e_command_waiter_result result;
    int ok;

    (void)close(ready_pipe[0]);
    (void)close(pending_pipe[0]);
    (void)close(result_pipe[0]);
    ok =
        pouch_e2e_command_waiter_child(root, ns, command_id, timeout_ms,
                                       ready_pipe[1], pending_pipe[1], &result);
    (void)pouch_e2e_write_exact(result_pipe[1], &result, sizeof(result));
    (void)close(ready_pipe[1]);
    (void)close(pending_pipe[1]);
    (void)close(result_pipe[1]);
    _exit(ok ? 0 : 1);
  }
  (void)close(ready_pipe[1]);
  (void)close(pending_pipe[1]);
  (void)close(result_pipe[1]);
  waiter->pid = pid;
  waiter->ready_read = ready_pipe[0];
  waiter->pending_read = pending_pipe[0];
  waiter->result_read = result_pipe[0];
  return LC_OK;

failed:
  if (ready_pipe[0] >= 0)
    (void)close(ready_pipe[0]);
  if (ready_pipe[1] >= 0)
    (void)close(ready_pipe[1]);
  if (pending_pipe[0] >= 0)
    (void)close(pending_pipe[0]);
  if (pending_pipe[1] >= 0)
    (void)close(pending_pipe[1]);
  if (result_pipe[0] >= 0)
    (void)close(result_pipe[0]);
  if (result_pipe[1] >= 0)
    (void)close(result_pipe[1]);
  return LC_ERR_INVALID;
}

static void
pouch_e2e_wait_command_waiter_ready(pouch_e2e_command_waiter *waiter) {
  char ready;

  assert_true(pouch_e2e_read_exact(waiter->ready_read, &ready, 1U));
  assert_int_equal(ready, 'r');
  assert_int_equal(close(waiter->ready_read), 0);
  waiter->ready_read = -1;
}

static void
pouch_e2e_wait_command_waiter_pending(pouch_e2e_command_waiter *waiter) {
  char pending;

  assert_true(pouch_e2e_read_exact(waiter->pending_read, &pending, 1U));
  assert_int_equal(pending, 'p');
  assert_int_equal(close(waiter->pending_read), 0);
  waiter->pending_read = -1;
}

static pouch_e2e_command_waiter_result
pouch_e2e_collect_command_waiter(pouch_e2e_command_waiter *waiter) {
  pouch_e2e_command_waiter_result result;
  int status;

  memset(&result, 0, sizeof(result));
  assert_true(
      pouch_e2e_read_exact(waiter->result_read, &result, sizeof(result)));
  assert_int_equal(close(waiter->result_read), 0);
  waiter->result_read = -1;
  assert_int_equal(waitpid(waiter->pid, &status, 0), waiter->pid);
  waiter->pid = -1;
  assert_true(WIFEXITED(status));
  assert_int_equal(result.rc, LC_OK);
  assert_int_equal(WEXITSTATUS(status), 0);
  return result;
}

static unsigned long pouch_e2e_shared_outbox_full_mask(size_t count) {
  assert_true(count < sizeof(unsigned long) * 8U);
  return (1UL << count) - 1UL;
}

static void test_pouch_shared_outbox_competing_instances(void **state) {
  lc_error error;
  pouch_e2e_outbox_child first;
  pouch_e2e_outbox_child second;
  pouch_e2e_outbox_child_result first_result;
  pouch_e2e_outbox_child_result second_result;
  char root[256];
  char endpoint[320];
  unsigned long full_mask;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-competing", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  /* The producer is a normal transaction producer, not a test-only record
   * writer. It closes before either independently started worker opens its
   * own Pouch session, which also proves startup reconciliation. */
  pouch_e2e_seed_shared_outbox(root, "outbox-shared", "competing",
                               POUCH_E2E_SHARED_OUTBOX_ROWS, &error);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-shared", "pouch-e2e-instance-a", "competing",
      POUCH_E2E_SHARED_OUTBOX_ROWS, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0, 0, 0,
      0, &first);
  assert_int_equal(rc, LC_OK);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-shared", "pouch-e2e-instance-b", "competing",
      POUCH_E2E_SHARED_OUTBOX_ROWS, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0, 0, 0,
      0, &second);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&first);
  pouch_e2e_wait_shared_outbox_child_ready(&second);
  pouch_e2e_start_shared_outbox_child(&first);
  pouch_e2e_start_shared_outbox_child(&second);
  first_result = pouch_e2e_collect_shared_outbox_child(&first);
  second_result = pouch_e2e_collect_shared_outbox_child(&second);
  full_mask = pouch_e2e_shared_outbox_full_mask(POUCH_E2E_SHARED_OUTBOX_ROWS);
  assert_int_equal(first_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(second_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(first_result.delivered_mask & second_result.delivered_mask,
                   0UL);
  assert_int_equal(first_result.delivered_mask | second_result.delivered_mask,
                   full_mask);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

/* Two independently opened shared-root consumers must never split one
 * outbox claim from its checkpoint. This is the production rolling-update
 * topology: either process may win a job, but every winner gets both leases
 * and every effect is terminalized exactly once. */
static void
test_pouch_shared_outbox_competing_stateful_instances(void **state) {
  lc_error error;
  pouch_e2e_outbox_child first;
  pouch_e2e_outbox_child second;
  pouch_e2e_outbox_child_result first_result;
  pouch_e2e_outbox_child_result second_result;
  char root[256];
  char endpoint[320];
  unsigned long full_mask;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-competing-stateful", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  pouch_e2e_seed_shared_outbox(root, "outbox-shared-stateful", "stateful",
                               POUCH_E2E_SHARED_OUTBOX_ROWS, &error);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-shared-stateful", "pouch-e2e-stateful-instance-a",
      "stateful", POUCH_E2E_SHARED_OUTBOX_ROWS,
      POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0L, 1, 0, 0, &first);
  assert_int_equal(rc, LC_OK);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-shared-stateful", "pouch-e2e-stateful-instance-b",
      "stateful", POUCH_E2E_SHARED_OUTBOX_ROWS,
      POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0L, 1, 0, 0, &second);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&first);
  pouch_e2e_wait_shared_outbox_child_ready(&second);
  pouch_e2e_start_shared_outbox_child(&first);
  pouch_e2e_start_shared_outbox_child(&second);
  first_result = pouch_e2e_collect_shared_outbox_child(&first);
  second_result = pouch_e2e_collect_shared_outbox_child(&second);
  full_mask = pouch_e2e_shared_outbox_full_mask(POUCH_E2E_SHARED_OUTBOX_ROWS);
  assert_int_equal(first_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(second_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(first_result.delivered_mask & second_result.delivered_mask,
                   0UL);
  assert_int_equal(first_result.delivered_mask | second_result.delivered_mask,
                   full_mask);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_shared_outbox_independent_namespaces(void **state) {
  const size_t rows_per_namespace = POUCH_E2E_SHARED_OUTBOX_ROWS / 2U;
  lc_error error;
  pouch_e2e_outbox_child alpha;
  pouch_e2e_outbox_child beta;
  pouch_e2e_outbox_child_result alpha_result;
  pouch_e2e_outbox_child_result beta_result;
  char root[256];
  char endpoint[320];
  unsigned long full_mask;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-namespaces", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  pouch_e2e_seed_shared_outbox(root, "outbox-alpha", "alpha",
                               rows_per_namespace, &error);
  pouch_e2e_seed_shared_outbox(root, "outbox-beta", "beta", rows_per_namespace,
                               &error);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-alpha", "pouch-e2e-alpha", "alpha", rows_per_namespace,
      rows_per_namespace, 0L, 0, 0, 0, &alpha);
  assert_int_equal(rc, LC_OK);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-beta", "pouch-e2e-beta", "beta", rows_per_namespace,
      rows_per_namespace, 0L, 0, 0, 0, &beta);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&alpha);
  pouch_e2e_wait_shared_outbox_child_ready(&beta);
  pouch_e2e_start_shared_outbox_child(&alpha);
  pouch_e2e_start_shared_outbox_child(&beta);
  alpha_result = pouch_e2e_collect_shared_outbox_child(&alpha);
  beta_result = pouch_e2e_collect_shared_outbox_child(&beta);
  full_mask = pouch_e2e_shared_outbox_full_mask(rows_per_namespace);
  assert_int_equal(alpha_result.delivered, rows_per_namespace);
  assert_int_equal(beta_result.delivered, rows_per_namespace);
  assert_int_equal(alpha_result.delivered_mask, full_mask);
  assert_int_equal(beta_result.delivered_mask, full_mask);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_shared_outbox_rolling_handoff(void **state) {
  lc_error error;
  pouch_e2e_outbox_child old_instance;
  pouch_e2e_outbox_child new_instance;
  pouch_e2e_outbox_child_result old_result;
  pouch_e2e_outbox_child_result new_result;
  char claimed;
  char root[256];
  char endpoint[320];
  unsigned long full_mask;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-rolling", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  pouch_e2e_seed_shared_outbox(root, "outbox-rolling", "rolling",
                               POUCH_E2E_SHARED_OUTBOX_ROWS, &error);

  /* The old instance owns a real durable claim while the new instance opens
   * and drains its half. No sleep or race is used to manufacture overlap. */
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-rolling", "pouch-e2e-old", "rolling",
      POUCH_E2E_SHARED_OUTBOX_ROWS, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0L, 0, 0,
      1, &old_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&old_instance);
  pouch_e2e_start_shared_outbox_child(&old_instance);
  assert_true(pouch_e2e_read_exact(old_instance.claimed_read, &claimed, 1U));
  assert_int_equal(claimed, 'c');
  assert_int_equal(close(old_instance.claimed_read), 0);
  old_instance.claimed_read = -1;

  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-rolling", "pouch-e2e-new", "rolling",
      POUCH_E2E_SHARED_OUTBOX_ROWS, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U, 0L, 0, 0,
      0, &new_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&new_instance);
  pouch_e2e_start_shared_outbox_child(&new_instance);
  pouch_e2e_release_shared_outbox_child(&old_instance);
  old_result = pouch_e2e_collect_shared_outbox_child(&old_instance);
  new_result = pouch_e2e_collect_shared_outbox_child(&new_instance);
  full_mask = pouch_e2e_shared_outbox_full_mask(POUCH_E2E_SHARED_OUTBOX_ROWS);
  assert_int_equal(old_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(new_result.delivered, POUCH_E2E_SHARED_OUTBOX_ROWS / 2U);
  assert_int_equal(old_result.delivered_mask & new_result.delivered_mask, 0UL);
  assert_int_equal(old_result.delivered_mask | new_result.delivered_mask,
                   full_mask);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_shared_outbox_abandoned_claim_recovers(void **state) {
  lc_error error;
  pouch_e2e_outbox_child failed_instance;
  pouch_e2e_outbox_child recovering_instance;
  pouch_e2e_outbox_child_result recovered;
  char claimed;
  char root[256];
  char endpoint[320];
  int status;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-abandoned", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  pouch_e2e_seed_shared_outbox(root, "outbox-abandoned", "abandoned", 1U,
                               &error);
  rc = pouch_e2e_launch_shared_outbox_child(root, "outbox-abandoned",
                                            "pouch-e2e-failed", "abandoned", 1U,
                                            1U, 1L, 0, 0, 1, &failed_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&failed_instance);
  pouch_e2e_start_shared_outbox_child(&failed_instance);
  assert_true(pouch_e2e_read_exact(failed_instance.claimed_read, &claimed, 1U));
  assert_int_equal(claimed, 'c');
  assert_int_equal(close(failed_instance.claimed_read), 0);
  failed_instance.claimed_read = -1;

  /* Simulate an ungraceful instance replacement after the durable claim has
   * been handed to its foreign-effect owner. The next instance must recover
   * that record at its documented one-second claim deadline. */
  assert_int_equal(kill(failed_instance.pid, SIGKILL), 0);
  assert_int_equal(waitpid(failed_instance.pid, &status, 0),
                   failed_instance.pid);
  failed_instance.pid = -1;
  assert_true(WIFSIGNALED(status));
  assert_int_equal(WTERMSIG(status), SIGKILL);
  assert_int_equal(close(failed_instance.result_read), 0);
  failed_instance.result_read = -1;
  assert_int_equal(close(failed_instance.release_write), 0);
  failed_instance.release_write = -1;

  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-abandoned", "pouch-e2e-recovering", "abandoned", 1U, 1U, 1L,
      0, 0, 0, &recovering_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&recovering_instance);
  pouch_e2e_start_shared_outbox_child(&recovering_instance);
  recovered = pouch_e2e_collect_shared_outbox_child(&recovering_instance);
  assert_int_equal(recovered.delivered, 1U);
  assert_int_equal(recovered.delivered_mask, 1UL);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

/* A stateful worker may persist its foreign-effect checkpoint and then die.
 * Its replacement must reclaim both leases after expiry and receive that same
 * checkpoint before it decides whether the foreign action needs repeating. */
static void
test_pouch_shared_outbox_abandoned_stateful_claim_recovers(void **state) {
  lc_error error;
  pouch_e2e_outbox_child failed_instance;
  pouch_e2e_outbox_child recovering_instance;
  pouch_e2e_outbox_child_result recovered;
  char claimed;
  char root[256];
  char endpoint[320];
  int status;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("outbox-abandoned-stateful", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  pouch_e2e_seed_shared_outbox(root, "outbox-abandoned-stateful", "abandoned",
                               1U, &error);
  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-abandoned-stateful", "pouch-e2e-stateful-failed",
      "abandoned", 1U, 1U, 1L, 1, 0, 1, &failed_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&failed_instance);
  pouch_e2e_start_shared_outbox_child(&failed_instance);
  assert_true(pouch_e2e_read_exact(failed_instance.claimed_read, &claimed, 1U));
  assert_int_equal(claimed, 'c');
  assert_int_equal(close(failed_instance.claimed_read), 0);
  failed_instance.claimed_read = -1;

  assert_int_equal(kill(failed_instance.pid, SIGKILL), 0);
  assert_int_equal(waitpid(failed_instance.pid, &status, 0),
                   failed_instance.pid);
  failed_instance.pid = -1;
  assert_true(WIFSIGNALED(status));
  assert_int_equal(WTERMSIG(status), SIGKILL);
  assert_int_equal(close(failed_instance.result_read), 0);
  failed_instance.result_read = -1;
  assert_int_equal(close(failed_instance.release_write), 0);
  failed_instance.release_write = -1;

  rc = pouch_e2e_launch_shared_outbox_child(
      root, "outbox-abandoned-stateful", "pouch-e2e-stateful-recovering",
      "abandoned", 1U, 1U, 1L, 1, 1, 0, &recovering_instance);
  assert_int_equal(rc, LC_OK);
  pouch_e2e_wait_shared_outbox_child_ready(&recovering_instance);
  pouch_e2e_start_shared_outbox_child(&recovering_instance);
  recovered = pouch_e2e_collect_shared_outbox_child(&recovering_instance);
  assert_int_equal(recovered.delivered, 1U);
  assert_int_equal(recovered.delivered_mask, 1UL);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

/* Checkpoint durability is independent of the terminal decision for a single
 * attempt. Exercise the public C path through retry and dead-letter replay;
 * each claimant must receive the same body and attachment before completion. */
static void
test_pouch_stateful_outbox_retry_and_replay_preserve_checkpoint(void **state) {
  lc_client *client;
  lc_error error;
  lc_lease *state_lease;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_outbox_retry retry;
  char outbox_key[256];
  char root[256];
  char endpoint[320];
  int rc;

  (void)state;
  client = NULL;
  outbox = NULL;
  dispatcher = NULL;
  job = NULL;
  lc_error_init(&error);
  make_pouch_root("outbox-stateful-lifecycle", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  rc = pouch_e2e_open_shared_client(root, &client, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_config_init(&config);
  config.ns = "outbox-stateful-lifecycle";
  config.owner = "pouch-e2e-stateful-lifecycle";
  config.claim_ttl_seconds = 1L;
  config.recovery_interval_seconds = 1L;
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  assert_lc_ok(rc, &error);
  rc = pouch_e2e_append_shared_outbox_effect(outbox, "stateful-lifecycle", 0U,
                                             &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_dispatcher_next_with_state(dispatcher, 3000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_non_null(job->outbox_key);
  assert_true(snprintf(outbox_key, sizeof(outbox_key), "%s", job->outbox_key) >
              0);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  rc = pouch_e2e_update_shared_outbox_state(state_lease, 0, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 1L;
  retry.diagnostic = "retry checkpoint";
  rc = lc_outbox_job_retry(job, &retry, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  rc = lc_outbox_dispatcher_next_with_state(dispatcher, 3000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  rc = pouch_e2e_update_shared_outbox_state(state_lease, 1, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_job_dead_letter(job, "replay checkpoint", &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  rc = lc_outbox_dispatcher_replay_dead_letter(dispatcher, outbox_key, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_dispatcher_next_with_state(dispatcher, 3000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  state_lease = lc_outbox_job_state(job);
  assert_non_null(state_lease);
  rc = pouch_e2e_update_shared_outbox_state(state_lease, 1, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void pouch_e2e_assert_shared_command_state(const char *root,
                                                  const char *ns,
                                                  const char *command_id,
                                                  int expected_rc,
                                                  int expected_state) {
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_command_receipt receipt;
  lc_error error;
  int rc;

  client = NULL;
  outbox = NULL;
  lc_error_init(&error);
  lc_command_receipt_init(&receipt);
  assert_lc_ok(pouch_e2e_open_shared_client(root, &client, &error), &error);
  lc_outbox_config_init(&config);
  config.ns = ns;
  config.owner = "pouch-e2e-command-observer";
  assert_lc_ok(lc_client_new_outbox(client, &config, &outbox, &error), &error);
  rc = lc_outbox_wait_command(outbox, command_id, 0L, &receipt, &error);
  assert_int_equal(rc, expected_rc);
  assert_int_equal(receipt.state, expected_state);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  lc_outbox_close(outbox);
  lc_client_close(client);
}

static void pouch_e2e_assert_command_effect_marker(const char *root,
                                                   const char *command_id) {
  char marker[448];
  struct stat st;

  assert_true(snprintf(marker, sizeof(marker), "%s/command-effects/%s", root,
                       command_id) > 0);
  assert_int_equal(stat(marker, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
}

static void pouch_e2e_assert_no_command_effect_marker(const char *root,
                                                      const char *command_id) {
  char marker[448];
  struct stat st;

  assert_true(snprintf(marker, sizeof(marker), "%s/command-effects/%s", root,
                       command_id) > 0);
  assert_int_equal(stat(marker, &st), -1);
  assert_int_equal(errno, ENOENT);
}

static void test_pouch_shared_command_competing_instances(void **state) {
  const char *ns = "command-competing";
  char root[256];
  char endpoint[320];
  char first_id[48];
  char second_id[48];
  pouch_e2e_command_child first;
  pouch_e2e_command_child second;
  pouch_e2e_command_child_result first_result;
  pouch_e2e_command_child_result second_result;
  lc_error error;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("command-competing", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_lc_ok(
      pouch_e2e_seed_shared_command(root, ns, "first", first_id, &error),
      &error);
  assert_lc_ok(
      pouch_e2e_seed_shared_command(root, ns, "second", second_id, &error),
      &error);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-worker-a",
                   POUCH_E2E_COMMAND_CHILD_COMPLETE, &first),
               &error);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-worker-b",
                   POUCH_E2E_COMMAND_CHILD_COMPLETE, &second),
               &error);
  pouch_e2e_wait_shared_command_child_ready(&first);
  pouch_e2e_wait_shared_command_child_ready(&second);
  pouch_e2e_start_shared_command_child(&first);
  pouch_e2e_start_shared_command_child(&second);
  first_result = pouch_e2e_collect_shared_command_child(&first);
  second_result = pouch_e2e_collect_shared_command_child(&second);
  assert_true(first_result.performed_foreign_effect);
  assert_true(second_result.performed_foreign_effect);
  assert_int_not_equal(
      strcmp(first_result.command_id, second_result.command_id), 0);
  assert_true((strcmp(first_result.command_id, first_id) == 0 &&
               strcmp(second_result.command_id, second_id) == 0) ||
              (strcmp(first_result.command_id, second_id) == 0 &&
               strcmp(second_result.command_id, first_id) == 0));
  pouch_e2e_assert_shared_command_state(root, ns, first_id, LC_OK,
                                        LC_COMMAND_COMPLETED);
  pouch_e2e_assert_shared_command_state(root, ns, second_id, LC_OK,
                                        LC_COMMAND_COMPLETED);
  pouch_e2e_assert_command_effect_marker(root, first_id);
  pouch_e2e_assert_command_effect_marker(root, second_id);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_shared_command_recovers_after_foreign_effect_crash(void **state) {
  const char *ns = "command-foreign-crash";
  char root[256];
  char endpoint[320];
  char command_id[48];
  char phase;
  pouch_e2e_command_child failed;
  pouch_e2e_command_child recovered;
  pouch_e2e_command_child_result recovered_result;
  lc_error error;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("command-foreign-crash", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_lc_ok(pouch_e2e_seed_shared_command(root, ns, "foreign-crash",
                                             command_id, &error),
               &error);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-foreign-failed",
                   POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_FOREIGN_EFFECT, &failed),
               &error);
  pouch_e2e_wait_shared_command_child_ready(&failed);
  pouch_e2e_start_shared_command_child(&failed);
  assert_true(pouch_e2e_read_exact(failed.phase_read, &phase, 1U));
  assert_int_equal(phase, 'f');
  pouch_e2e_kill_shared_command_child(&failed);
  pouch_e2e_assert_shared_command_state(root, ns, command_id, LC_ERR_TIMEOUT,
                                        LC_COMMAND_PENDING);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-foreign-recovered",
                   POUCH_E2E_COMMAND_CHILD_COMPLETE, &recovered),
               &error);
  pouch_e2e_wait_shared_command_child_ready(&recovered);
  pouch_e2e_start_shared_command_child(&recovered);
  recovered_result = pouch_e2e_collect_shared_command_child(&recovered);
  assert_false(recovered_result.performed_foreign_effect);
  assert_false(recovered_result.saw_terminal_command);
  pouch_e2e_assert_shared_command_state(root, ns, command_id, LC_OK,
                                        LC_COMMAND_COMPLETED);
  pouch_e2e_assert_command_effect_marker(root, command_id);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_shared_command_terminal_commit_survives_job_crash(void **state) {
  const char *ns = "command-terminal-crash";
  char root[256];
  char endpoint[320];
  char command_id[48];
  char phase;
  pouch_e2e_command_child failed;
  pouch_e2e_command_child recovered;
  pouch_e2e_command_child_result recovered_result;
  lc_error error;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("command-terminal-crash", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_lc_ok(pouch_e2e_seed_shared_command(root, ns, "terminal-crash",
                                             command_id, &error),
               &error);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-terminal-failed",
                   POUCH_E2E_COMMAND_CHILD_CRASH_AFTER_COMMAND_COMMIT, &failed),
               &error);
  pouch_e2e_wait_shared_command_child_ready(&failed);
  pouch_e2e_start_shared_command_child(&failed);
  assert_true(pouch_e2e_read_exact(failed.phase_read, &phase, 1U));
  assert_int_equal(phase, 't');
  pouch_e2e_assert_shared_command_state(root, ns, command_id, LC_OK,
                                        LC_COMMAND_COMPLETED);
  pouch_e2e_kill_shared_command_child(&failed);
  assert_lc_ok(pouch_e2e_launch_shared_command_child(
                   root, ns, "command-terminal-recovered",
                   POUCH_E2E_COMMAND_CHILD_COMPLETE, &recovered),
               &error);
  pouch_e2e_wait_shared_command_child_ready(&recovered);
  pouch_e2e_start_shared_command_child(&recovered);
  recovered_result = pouch_e2e_collect_shared_command_child(&recovered);
  assert_false(recovered_result.performed_foreign_effect);
  assert_true(recovered_result.saw_terminal_command);
  pouch_e2e_assert_shared_command_state(root, ns, command_id, LC_OK,
                                        LC_COMMAND_COMPLETED);
  pouch_e2e_assert_command_effect_marker(root, command_id);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_shared_command_waiter_blocks_until_terminal(void **state) {
  const char *ns = "command-waiter";
  const char *keys[2] = {"wait-infinite", "wait-deadline"};
  const long timeouts[2] = {-1L, 5000L};
  char root[256];
  char endpoint[320];
  char command_ids[2][48];
  pouch_e2e_command_waiter waiters[2];
  pouch_e2e_command_waiter_result waiter_results[2];
  pouch_e2e_command_child workers[2];
  pouch_e2e_command_child_result worker_results[2];
  lc_error error;
  size_t index;

  (void)state;
  lc_error_init(&error);
  make_pouch_root("command-waiter", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  for (index = 0U; index < 2U; ++index) {
    assert_lc_ok(pouch_e2e_seed_shared_command(root, ns, keys[index],
                                               command_ids[index], &error),
                 &error);
    assert_lc_ok(pouch_e2e_launch_command_waiter(root, ns, command_ids[index],
                                                 timeouts[index],
                                                 &waiters[index]),
                 &error);
    pouch_e2e_wait_command_waiter_ready(&waiters[index]);
    /* The hook runs after the waiter has reread a pending receipt.  Only then
     * may this separate supervisor claim and terminalize the command effect. */
    pouch_e2e_wait_command_waiter_pending(&waiters[index]);
    pouch_e2e_assert_no_command_effect_marker(root, command_ids[index]);
  }
  for (index = 0U; index < 2U; ++index) {
    assert_lc_ok(pouch_e2e_launch_shared_command_child(
                     root, ns, "command-waiter-supervisor",
                     POUCH_E2E_COMMAND_CHILD_COMPLETE, &workers[index]),
                 &error);
    pouch_e2e_wait_shared_command_child_ready(&workers[index]);
  }
  for (index = 0U; index < 2U; ++index)
    pouch_e2e_start_shared_command_child(&workers[index]);
  for (index = 0U; index < 2U; ++index) {
    worker_results[index] =
        pouch_e2e_collect_shared_command_child(&workers[index]);
    assert_true(worker_results[index].performed_foreign_effect);
    waiter_results[index] = pouch_e2e_collect_command_waiter(&waiters[index]);
    assert_int_equal(waiter_results[index].state, LC_COMMAND_COMPLETED);
    pouch_e2e_assert_command_effect_marker(root, command_ids[index]);
  }
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void pouch_e2e_write_released_hardening_state(lc_client *client,
                                                     size_t index,
                                                     lc_error *error) {
  lc_acquire_req acquire;
  lc_release_req release;
  lc_lease *lease;
  char body[128];
  char key[96];
  int rc;

  lease = NULL;
  lc_acquire_req_init(&acquire);
  lc_release_req_init(&release);
  assert_true(snprintf(key, sizeof(key), "state/retention/%04zu", index) > 0);
  assert_true(snprintf(body, sizeof(body),
                       "{\"kind\":\"pouch-hardening-expired\",\"ordinal\":%zu}",
                       index) > 0);
  acquire.ns = "hardening-retention";
  acquire.key = key;
  acquire.owner = "pouch-hardening-retention";
  acquire.ttl_seconds = 60L;
  rc = lc_acquire(client, &acquire, &lease, error);
  assert_lc_ok(rc, error);
  save_json_text_or_die(lease, body, error);
  rc = lc_lease_release(lease, &release, error);
  assert_lc_ok(rc, error);
}

static void pouch_e2e_run_shared_maintenance(const char *root, const char *ns,
                                             int cleanup_only,
                                             long retention_cutoff,
                                             lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  open_options.segment_target_bytes = POUCH_E2E_HARDENING_SEGMENT_BYTES;
  open_options.terminal_reclaim_min_bytes = 1U;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 0;
  open_options.single_writer_set = 1;
  open_options.single_writer = 0;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.ns = ns;
  maintenance_options.cleanup_only = cleanup_only;
  maintenance_options.retention_updated_before_unix = retention_cutoff;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  if (retention_cutoff > 0L) {
    assert_string_equal(maintenance_result.diagnostic, "retention-complete");
    assert_int_equal(maintenance_result.retention_failed_count, 0UL);
    assert_true(maintenance_result.retention_deleted_state_count > 0UL);
  } else {
    assert_true(maintenance_result.skipped);
    assert_string_equal(maintenance_result.diagnostic, "cleanup-complete");
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static void pouch_e2e_force_shared_multi_segment_compaction(const char *root,
                                                            const char *ns,
                                                            lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  open_options.segment_target_bytes = POUCH_E2E_HARDENING_MULTI_SEGMENT_BYTES;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 0;
  open_options.single_writer_set = 1;
  open_options.single_writer = 0;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.ns = ns;
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  assert_true(maintenance_result.compacted);
  assert_true(maintenance_result.candidate_segment_count > 1UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static size_t pouch_e2e_count_shared_payloads(const char *root, const char *ns,
                                              const char *key_prefix,
                                              lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  pouch_e2e_state_entry_count count;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&count, 0, sizeof(count));
  open_options.single_writer_set = 1;
  open_options.single_writer = 0;
  open_options.background_compaction_enabled_set = 1;
  open_options.background_compaction_enabled = 0;
  count.key_prefix = key_prefix;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  rc = lc_pouch_state_visit(pouch, ns, pouch_e2e_count_state_entry, &count,
                            error);
  assert_lc_ok(rc, error);
  lc_pouch_close(pouch);
  return count.count;
}

static void
test_pouch_direct_lifecycle_maintenance_reopen_roundtrip(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_message *message;
  lc_source *src;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  char root[256];
  char endpoint[320];
  char kind[96];
  char keep_key[128];
  char expired_kind[96];
  char expired_key[128];
  char selector_json[192];
  char expired_selector_json[192];
  char queue_name[160];
  size_t rows;
  int rc;

  (void)state;
  make_pouch_root("lifecycle-maintenance", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  reader = NULL;
  lease = NULL;
  message = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);

  make_unique_name("pouch-life", kind, sizeof(kind));
  make_unique_name("pouch-expired", expired_kind, sizeof(expired_kind));
  snprintf(keep_key, sizeof(keep_key), "pouch/lifecycle/%s/keep", kind);
  snprintf(expired_key, sizeof(expired_key), "pouch/lifecycle/%s/dead",
           expired_kind);
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  snprintf(expired_selector_json, sizeof(expired_selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", expired_kind);
  snprintf(queue_name, sizeof(queue_name), "pouch-life-%s", kind);

  open_pouch_client(endpoint, &client, &error);
  acquire_req.ns = "life";
  acquire_req.key = keep_key;
  acquire_req.owner = "lc-e2e-pouch-life";
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  {
    char body[256];

    snprintf(body, sizeof(body),
             "{\"kind\":\"%s\",\"status\":\"keep\",\"value\":1}", kind);
    save_json_text_or_die(lease, body, &error);
  }
  attach_req.name = "life.txt";
  attach_req.content_type = "text/plain";
  rc =
      lc_source_from_memory("life-object", strlen("life-object"), &src, &error);
  assert_lc_ok(rc, &error);
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.ns = "life-retention";
  acquire_req.key = expired_key;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  {
    char body[256];

    snprintf(body, sizeof(body),
             "{\"kind\":\"%s\",\"status\":\"expired\",\"value\":2}",
             expired_kind);
    save_json_text_or_die(lease, body, &error);
  }
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  enqueue_req.ns = "life";
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 3;
  rc = lc_source_from_memory("life-queue", strlen("life-queue"), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);

  rows = pouch_e2e_query_key_count(client, "life", selector_json, &error);
  assert_int_equal(rows, 1U);
  rows = pouch_e2e_query_key_count(client, "life-retention",
                                   expired_selector_json, &error);
  assert_int_equal(rows, 1U);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_run_maintenance(root, "life", 1, 0, 0L, &error);
  pouch_e2e_run_maintenance(root, "life", 0, 1, 0L, &error);
  pouch_e2e_run_maintenance(root, "life-retention", 0, 0, 2147483647L, &error);

  open_pouch_client(endpoint, &reader, &error);
  rows = pouch_e2e_query_key_count(reader, "life", selector_json, &error);
  assert_int_equal(rows, 1U);
  rows = pouch_e2e_query_key_count(reader, "life-retention",
                                   expired_selector_json, &error);
  assert_int_equal(rows, 0U);

  acquire_req.ns = "life";
  acquire_req.key = keep_key;
  acquire_req.owner = "lc-e2e-pouch-life-reader";
  rc = reader->acquire(reader, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "life.txt");
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  dequeue_req.ns = "life";
  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-pouch-life-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = reader->dequeue(reader, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_attachment_list_cleanup(&attachment_list);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_direct_large_namespace_segmented_index_reopen(void **state) {
  lc_client *client;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char root[256];
  char endpoint[320];
  char kind[96];
  char selector_json[192];
  size_t rows;
  const size_t doc_count = 72U;
  int rc;

  (void)state;
  make_pouch_root("large-namespace", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  memset(&flush_res, 0, sizeof(flush_res));
  lc_index_flush_req_init(&flush_req);
  lc_error_init(&error);

  make_unique_name("pouch-large", kind, sizeof(kind));
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);

  pouch_e2e_write_segmented_docs_direct(root, "large", kind, doc_count, &error);

  open_pouch_client(endpoint, &client, &error);
  flush_req.ns = "large";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "large", selector_json, &error);
  assert_int_equal(rows, doc_count);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_force_maintenance_expect_segments(root, "large", &error);
  pouch_e2e_run_maintenance(root, "large", 0, 1, 0L, &error);

  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "large", selector_json, &error);
  assert_int_equal(rows, doc_count);

  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_terminal_reclaim_one_segment_churn_reopen(void **state) {
  lc_client *client;
  lc_error error;
  char root[256];
  char endpoint[320];
  char kind[96];
  char selector_json[192];
  size_t rows;

  (void)state;
  make_pouch_root("terminal-churn", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  client = NULL;
  lc_error_init(&error);
  make_unique_name("pouch-terminal-churn", kind, sizeof(kind));
  assert_true(snprintf(selector_json, sizeof(selector_json),
                       "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}",
                       kind) > 0);

  /* This mirrors the c89 shape: many terminal keys in one otherwise active
   * segment. The deterministic maintenance boundary replaces a timing gate. */
  pouch_e2e_reclaim_one_active_churn_segment(root, "churn", kind, &error);

  open_pouch_client(endpoint, &client, &error);
  rows = pouch_e2e_query_key_count(client, "churn", selector_json, &error);
  assert_int_equal(rows, 1U);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_shared_hardening_churn_dispatch_and_maintenance(void **state) {
  lc_client_config config;
  lc_pouch_settings settings;
  lc_outbox_config outbox_config;
  lc_outbox_dispatcher *dispatcher;
  lc_outbox_stats dispatcher_stats;
  lc_outbox *outbox;
  lc_client *client;
  lc_client *reader;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_query_key_handler query_handler;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_error error;
  pouch_e2e_churn_child_result child_result;
  pouch_e2e_dispatcher_hold hold;
  pthread_t worker;
  int start_pipe[2];
  int result_pipe[2];
  pid_t child;
  int child_status;
  char start;
  char root[256];
  char endpoint[320];
  const char *endpoints[1];
  static const char survivor_selector[] =
      "{\"eq\":{\"field\":\"/kind\","
      "\"value\":\"pouch-hardening-survivor\"}}";
  static const char expired_selector[] =
      "{\"eq\":{\"field\":\"/kind\","
      "\"value\":\"pouch-hardening-expired\"}}";
  static const char multi_survivor_selector[] =
      "{\"eq\":{\"field\":\"/kind\","
      "\"value\":\"pouch-hardening-multi-survivor\"}}";
  size_t index;
  size_t delivered;
  size_t rows;
  query_keys_e2e_capture query_capture;
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  outbox = NULL;
  dispatcher = NULL;
  child = -1;
  memset(&dispatcher_stats, 0, sizeof(dispatcher_stats));
  memset(&flush_res, 0, sizeof(flush_res));
  memset(&query_handler, 0, sizeof(query_handler));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&query_capture, 0, sizeof(query_capture));
  memset(&hold, 0, sizeof(hold));
  memset(&child_result, 0, sizeof(child_result));
  lc_error_init(&error);
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  lc_outbox_config_init(&outbox_config);
  lc_index_flush_req_init(&flush_req);
  make_pouch_root("shared-hardening", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);

  /* Use the typed surface to make shared-root and bounded reclaim policy
   * explicit. The child independently opens the same root with equivalent
   * shared-writer and reclaim settings, so this is a cross-process
   * shared-writer test. */
  settings.set_mask = LC_POUCH_SETTING_SINGLE_WRITER |
                      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES |
                      LC_POUCH_SETTING_BACKGROUND_COMPACTION |
                      LC_POUCH_SETTING_TERMINAL_RECLAIM_MIN_BYTES;
  settings.single_writer = 0;
  settings.segment_target_bytes = POUCH_E2E_HARDENING_MULTI_SEGMENT_BYTES;
  settings.background_compaction_enabled = 0;
  settings.terminal_reclaim_min_bytes = 1U;
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.pouch_settings = &settings;

  assert_int_equal(pipe(start_pipe), 0);
  assert_int_equal(pipe(result_pipe), 0);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    char child_start;
    int child_ok;

    (void)close(start_pipe[1]);
    (void)close(result_pipe[0]);
    child_ok = pouch_e2e_read_exact(start_pipe[0], &child_start, 1U) &&
               child_start == 's';
    if (child_ok)
      child_ok = pouch_e2e_shared_churn_child(root, &child_result);
    if (!child_ok && child_result.rc == LC_OK)
      child_result.rc = LC_ERR_INVALID;
    child_ok = pouch_e2e_write_exact(result_pipe[1], &child_result,
                                     sizeof(child_result));
    (void)close(start_pipe[0]);
    (void)close(result_pipe[1]);
    _exit(child_ok ? 0 : 1);
  }
  assert_int_equal(close(start_pipe[0]), 0);
  assert_int_equal(close(result_pipe[1]), 0);

  rc = lc_client_open(&config, &client, &error);
  assert_lc_ok(rc, &error);
  outbox_config.ns = "hardening-outbox";
  outbox_config.owner = "pouch-hardening-dispatcher";
  outbox_config.notification_capacity = POUCH_E2E_HARDENING_OUTBOX_EFFECTS;
  rc = lc_client_new_outbox(client, &outbox_config, &outbox, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
  assert_lc_ok(rc, &error);

  /* Queue one known-ready effect before the worker calls next(0). This proves
   * the dispatcher is actively holding a real claim without a timeout race. */
  pouch_e2e_append_hardening_effect(outbox, 0U, &error);
  hold.dispatcher = dispatcher;
  assert_int_equal(pthread_mutex_init(&hold.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&hold.cond, NULL), 0);
  assert_int_equal(
      pthread_create(&worker, NULL, pouch_e2e_dispatcher_hold_worker, &hold),
      0);
  assert_int_equal(pthread_mutex_lock(&hold.mutex), 0);
  while (!hold.claimed) {
    assert_int_equal(pthread_cond_wait(&hold.cond, &hold.mutex), 0);
  }
  assert_int_equal(hold.rc, LC_OK);
  assert_int_equal(pthread_mutex_unlock(&hold.mutex), 0);

  /* The child now churns one active segment while this dispatcher claim stays
   * live. Meanwhile the parent continues to write terminal domain state and
   * outbox work through the independent shared-root client. */
  start = 's';
  assert_true(pouch_e2e_write_exact(start_pipe[1], &start, 1U));
  assert_int_equal(close(start_pipe[1]), 0);
  for (index = 0U; index < POUCH_E2E_HARDENING_RETENTION_ROWS; ++index) {
    pouch_e2e_write_released_hardening_state(client, index, &error);
  }
  for (index = 1U; index < POUCH_E2E_HARDENING_OUTBOX_EFFECTS; ++index) {
    pouch_e2e_append_hardening_effect(outbox, index, &error);
  }

  assert_true(pouch_e2e_read_exact(result_pipe[0], &child_result,
                                   sizeof(child_result)));
  assert_int_equal(close(result_pipe[0]), 0);
  assert_int_equal(waitpid(child, &child_status, 0), child);
  child = -1;
  assert_true(WIFEXITED(child_status));
  assert_int_equal(WEXITSTATUS(child_status), 0);
  if (child_result.rc != LC_OK) {
    print_message("shared churn child failed at stage %d: %s "
                  "(single=%d/%lu multi=%d/%lu)\n",
                  child_result.stage, child_result.error_message,
                  child_result.compacted, child_result.candidate_segment_count,
                  child_result.multi_segment_compacted,
                  child_result.multi_segment_candidate_count);
  }
  assert_int_equal(child_result.rc, LC_OK);
  assert_true(child_result.compacted);
  assert_int_equal(child_result.candidate_segment_count, 1UL);
  assert_true(child_result.multi_segment_compacted);
  assert_true(child_result.multi_segment_candidate_count > 1UL);
  assert_int_equal(pouch_e2e_count_shared_payloads(root, "hardening-churn",
                                                   "state/churn/", &error),
                   1U);
  assert_int_equal(pouch_e2e_count_shared_payloads(root, "hardening-multi",
                                                   "state/multi/", &error),
                   1U);

  /* The explicit janitor pass and whole-document retention sweep run while
   * the dispatcher still owns its claimed job and the remaining work is
   * pending. Neither maintenance pass may disturb dispatch state. */
  pouch_e2e_run_shared_maintenance(root, "hardening-retention", 1, 0L, &error);
  pouch_e2e_run_shared_maintenance(root, "hardening-retention", 0, 2147483647L,
                                   &error);

  assert_int_equal(pthread_mutex_lock(&hold.mutex), 0);
  hold.release = 1;
  assert_int_equal(pthread_cond_broadcast(&hold.cond), 0);
  assert_int_equal(pthread_mutex_unlock(&hold.mutex), 0);
  assert_int_equal(pthread_join(worker, NULL), 0);
  assert_int_equal(hold.rc, LC_OK);
  assert_true(hold.completed);
  assert_int_equal(pthread_cond_destroy(&hold.cond), 0);
  assert_int_equal(pthread_mutex_destroy(&hold.mutex), 0);

  delivered = 1U;
  while (delivered < POUCH_E2E_HARDENING_OUTBOX_EFFECTS) {
    lc_outbox_job *job;

    job = NULL;
    rc = lc_outbox_dispatcher_next(dispatcher, 0L, &job, &error);
    assert_lc_ok(rc, &error);
    assert_non_null(job);
    rc = lc_outbox_job_complete(job, NULL, &error);
    assert_lc_ok(rc, &error);
    ++delivered;
  }
  rc = lc_outbox_dispatcher_get_stats(dispatcher, &dispatcher_stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(dispatcher_stats.running);
  assert_true(dispatcher_stats.direct_notifications >=
              POUCH_E2E_HARDENING_OUTBOX_EFFECTS);
  lc_outbox_stats_cleanup(&dispatcher_stats);

  /* Leave one real effect durable, then force a multi-segment shared-root
   * compaction. A new dispatcher has no local index state to lean on: it must
   * rebuild the retired derived index from canonical state before it can
   * recover and claim this effect. */
  pouch_e2e_append_hardening_effect(outbox, POUCH_E2E_HARDENING_OUTBOX_EFFECTS,
                                    &error);

  lc_outbox_dispatcher_close(dispatcher);
  dispatcher = NULL;
  lc_outbox_close(outbox);
  outbox = NULL;
  lc_client_close(client);
  client = NULL;

  pouch_e2e_force_shared_multi_segment_compaction(root, "hardening-outbox",
                                                  &error);

  rc = lc_client_open(&config, &reader, &error);
  assert_lc_ok(rc, &error);
  rc = lc_client_new_outbox(reader, &outbox_config, &outbox, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &error);
  assert_lc_ok(rc, &error);
  {
    lc_outbox_job *job;

    job = NULL;
    /* An explicit reconciliation is the documented producer-side request;
     * zero-timeout next() intentionally remains an in-memory probe. The
     * bounded wait observes completion of that requested cold recovery rather
     * than racing a timer or sleeping for an arbitrary interval. */
    rc = lc_outbox_dispatcher_reconcile(dispatcher, &error);
    assert_lc_ok(rc, &error);
    rc = lc_outbox_dispatcher_next(dispatcher, 5000L, &job, &error);
    assert_lc_ok(rc, &error);
    assert_non_null(job);
    assert_string_equal(job->effect_key, "pouch-hardening-key-0032");
    rc = lc_outbox_job_complete(job, NULL, &error);
    assert_lc_ok(rc, &error);
  }
  lc_outbox_dispatcher_close(dispatcher);
  dispatcher = NULL;
  lc_outbox_close(outbox);
  outbox = NULL;
  flush_req.ns = "hardening-churn";
  flush_req.mode = "sync";
  rc = reader->flush_index(reader, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  pouch_e2e_assert_current_index_flush(&flush_res);
  query_capture.key_prefix = "state/churn/";
  query_capture.key_prefix_len = strlen(query_capture.key_prefix);
  query_capture.expected_count = POUCH_E2E_HARDENING_CHURN_ROWS;
  query_handler.begin = query_keys_e2e_begin;
  query_handler.chunk = query_keys_e2e_chunk;
  query_handler.end = query_keys_e2e_end;
  query_req.ns = "hardening-churn";
  query_req.selector_json = survivor_selector;
  query_req.engine = "index";
  query_req.limit = 512L;
  rc = reader->query_keys(reader, &query_req, &query_handler, &query_capture,
                          &query_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(query_capture.end_calls, 1U);
  lc_query_res_cleanup(&query_res);
  flush_req.ns = "hardening-multi";
  rc = reader->flush_index(reader, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  pouch_e2e_assert_current_index_flush(&flush_res);
  rows = pouch_e2e_query_key_count(reader, "hardening-multi",
                                   multi_survivor_selector, &error);
  assert_int_equal(rows, 1U);
  rows = pouch_e2e_query_key_count(reader, "hardening-retention",
                                   expired_selector, &error);
  assert_int_equal(rows, 0U);

  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_direct_marker_damage_and_index_rebuild_after_snapshot(void **state) {
  lc_client *client;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char root[256];
  char endpoint[320];
  char kind[96];
  char selector_json[192];
  char namespace_path[512];
  char manifest_path[640];
  char marker_path[640];
  size_t rows;
  const size_t doc_count = 36U;
  int written;
  int rc;

  (void)state;
  make_pouch_root("marker-index-repair", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_error_init(&error);

  make_unique_name("pouch-repair", kind, sizeof(kind));
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  written = snprintf(namespace_path, sizeof(namespace_path),
                     "%s/namespaces/repair", root);
  assert_true(written > 0 && (size_t)written < sizeof(namespace_path));
  written = snprintf(manifest_path, sizeof(manifest_path),
                     "%s/index/query.manifest", namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(manifest_path));
  written = snprintf(marker_path, sizeof(marker_path),
                     "%s/markers/writer-damaged.marker", namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(marker_path));

  pouch_e2e_write_segmented_docs_direct(root, "repair", kind, doc_count,
                                        &error);

  open_pouch_client(endpoint, &client, &error);
  flush_req.ns = "repair";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "repair", selector_json, &error);
  assert_int_equal(rows, doc_count);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_force_maintenance_expect_segments(root, "repair", &error);
  pouch_e2e_run_maintenance(root, "repair", 0, 1, 0L, &error);

  /* Recreate the post-snapshot derived index, then remove its manifest. */
  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(unlink(manifest_path), 0);
  pouch_e2e_write_text_file(marker_path, "damaged-marker\n");

  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "repair", selector_json, &error);
  assert_int_equal(rows, doc_count);

  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_direct_consumer_service_with_state(void **state) {
  lc_client *client;
  lc_consumer_service *service;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  e2e_consumer_context consumer_context;
  lc_source *src;
  char root[256];
  char endpoint[320];
  char queue_name[96];
  static const unsigned char payload[] = {'p', 'o', 'u', 'c', 'h',
                                          '-', 'w', 'o', 'r', 'k'};
  int rc;

  (void)state;
  make_pouch_root("consumer", root, sizeof(root), endpoint, sizeof(endpoint));

  client = NULL;
  service = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  memset(&consumer_context, 0, sizeof(consumer_context));
  assert_int_equal(pthread_mutex_init(&consumer_context.mutex, NULL), 0);
  consumer_context.backend_label = "pouch";
  consumer_context.first_delivery_mode = E2E_CONSUMER_FIRST_DELIVERY_NONE;
  consumer_context.expect_state = 1;
  consumer_context.read_payload = 1;
  consumer_context.enqueue_count = 1;
  consumer_context.worker_count = 1U;
  consumer_context.max_failures = 5;

  open_pouch_client(endpoint, &client, &error);
  make_unique_name("pouch-consumer", queue_name, sizeof(queue_name));
  consumer_context.queue_name = queue_name;

  consumer.name = "pouch";
  consumer.request.queue = queue_name;
  consumer.request.owner = "lc-e2e-pouch-consumer";
  consumer.request.visibility_timeout_seconds = 30L;
  consumer.request.wait_seconds = 1L;
  consumer.with_state = 1;
  consumer.handle = e2e_consumer_handle;
  consumer.on_error = e2e_consumer_on_error;
  consumer.on_start = e2e_consumer_on_start;
  consumer.on_stop = e2e_consumer_on_stop;
  consumer.context = &consumer_context;
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 250L;
  consumer.restart_policy.max_failures = 5;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);

  /* The service retains Pouch's exclusive local session after its caller
   * closes the source client. */
  lc_client_close(client);
  client = NULL;

  rc = service->start(service, &error);
  assert_lc_ok(rc, &error);

  consumer_context.expected_minimum_count = 1;
  wait_for_consumer_handled(&consumer_context, 1);

  rc = (service->stop)(service);
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  assert_lc_ok(rc, &error);

  pthread_mutex_lock(&consumer_context.mutex);
  assert_int_equal(consumer_context.handled, 1);
  assert_int_equal(consumer_context.error_events, 0);
  assert_true(consumer_context.start_events >= 1);
  assert_true(consumer_context.stop_events >= 1);
  assert_string_equal(consumer_context.payload, "pouch-work");
  assert_int_equal(consumer_context.saw_state, 1);
  assert_non_null(strstr(consumer_context.state_json, "from-consumer-service"));
  assert_non_null(strstr(consumer_context.state_key, "/state/"));
  assert_non_null(strstr(consumer_context.state_key, queue_name));
  pthread_mutex_unlock(&consumer_context.mutex);

  pthread_mutex_destroy(&consumer_context.mutex);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_consumer_service_close(service);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

#if defined(LC_E2E_GROUP_DISK_DIRECT)
/*
 * lockd v0.8.1 does not correctly preserve implicit-XA atomicity once a
 * outbox enlists multiple participants.  Keep these remote outbox
 * scenarios ready for the upstream fix, but do not run them as product e2e
 * coverage meanwhile: Pouch is the supported and fully-tested outbox
 * backend.
 */
#if 0
static void test_disk_outbox_implicit_xa_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_receipt duplicate_receipt;
  lc_outbox_transaction *transaction;
  lc_outbox_transaction *duplicate_transaction;
  lc_outbox_participant_request participant_request;
  lc_outbox_participant *participant;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *duplicate_payload;
  lc_source *domain_state;
  lc_sink *payload_sink;
  lc_get_opts get_options;
  lc_get_res get_result;
  const void *payload_bytes;
  size_t payload_length;
  size_t payload_written;
  lc_error error;
  char domain_key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("outbox-domain", domain_key, sizeof(domain_key));
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  participant = NULL;
  job = NULL;
  payload = NULL;
  duplicate_payload = NULL;
  domain_state = NULL;
  duplicate_transaction = NULL;
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  payload_written = 0U;
  lc_error_init(&error);
  lc_get_opts_init(&get_options);
  memset(&get_result, 0, sizeof(get_result));
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_outbox_config_init(&config);
  config.ns = "default";
  config.owner = "outbox-e2e";
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = domain_key;
  entry.effect_id = "notify";
  entry.effect_key = domain_key;
  entry.payload_digest = "sha256:outbox-e2e-payload";
  entry.kind = "test";
  entry.destination = "https://example.invalid/outbox";
  entry.content_type = "text/plain";
  rc = lc_source_from_memory("outbox-payload", 16U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_outbox_append(outbox, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "default";
  participant_request.acquire.key = domain_key;
  participant_request.acquire.owner = "outbox-e2e";
  participant_request.acquire.ttl_seconds = 30L;
  rc = lc_outbox_transaction_acquire(transaction, &participant_request,
                                       &participant, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(participant->txn_id);
  rc = lc_source_from_memory("{\"outbox\":true}", 17U, &domain_state, &error);
  assert_lc_ok(rc, &error);
  rc = participant->update(participant, domain_state, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_outbox_participant_close(participant);
  participant = NULL;
  rc = lc_outbox_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_transaction_close(transaction);
  get_options.public_read = 1;
  rc = lc_sink_to_memory(&payload_sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, domain_key, &get_options, payload_sink, &get_result,
                   &error);
  assert_lc_ok(rc, &error);
  assert_false(get_result.no_content);
  rc = lc_sink_memory_bytes(payload_sink, &payload_bytes, &payload_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_memory_equal(payload_bytes, "{\"outbox\":true}", 17U);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(payload_sink);
  payload_sink = NULL;
  rc = lc_outbox_next(outbox, 2000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, domain_key);
  rc = lc_sink_to_memory(&payload_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_job_write_payload(job, payload_sink, &payload_written, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(payload_sink, &payload_bytes, &payload_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(payload_written, 16U);
  assert_int_equal(payload_length, 16U);
  assert_memory_equal(payload_bytes, "outbox-payload", 16U);
  lc_sink_close(payload_sink);
  payload_sink = NULL;
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  rc = lc_source_from_memory("outbox-payload", 16U, &duplicate_payload,
                             &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&duplicate_receipt);
  duplicate_transaction = (lc_outbox_transaction *)1;
  rc = lc_outbox_append(outbox, &entry, duplicate_payload,
                                 &duplicate_transaction, &duplicate_receipt,
                                 &error);
  assert_lc_ok(rc, &error);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_string_equal(duplicate_receipt.outbox_key, receipt.outbox_key);
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_source_close(duplicate_payload);
  duplicate_payload = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  if (duplicate_payload != NULL)
    lc_source_close(duplicate_payload);
  if (payload_sink != NULL)
    lc_sink_close(payload_sink);
  if (job != NULL)
    lc_outbox_job_close(job);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_outbox_dispatcher_uses_internal_json_limit(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  char effect_key[128];
  char header_value[1537];
  char headers_json[1600];
  int header_length;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("outbox-response-limit", effect_key, sizeof(effect_key));
  memset(header_value, 'a', sizeof(header_value) - 1U);
  header_value[sizeof(header_value) - 1U] = '\0';
  header_length = snprintf(headers_json, sizeof(headers_json),
                           "{\"x-outbox-proof\":\"%s\"}", header_value);
  assert_true(header_length > 1024);
  assert_true((size_t)header_length < sizeof(headers_json));
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  open_tcp_client_with_json_response_limit(endpoint, bundle_path, 1024U,
                                           &client, &error);
  lc_outbox_config_init(&config);
  config.ns = "default";
  config.owner = "outbox-response-limit-e2e";
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = effect_key;
  entry.effect_id = "notify";
  entry.effect_key = effect_key;
  entry.payload_digest = "sha256:outbox-response-limit-payload";
  entry.kind = "test";
  entry.destination = "https://example.invalid/outbox-response-limit";
  entry.content_type = "text/plain";
  entry.headers_json = headers_json;
  rc = lc_source_from_memory("outbox-payload", 16U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_outbox_append(outbox, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  rc = lc_outbox_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  rc = lc_outbox_next(outbox, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_outbox_retry_redelivery(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction;
  lc_outbox_job *job;
  lc_outbox_retry retry;
  lc_source *payload;
  lc_sink *export_sink;
  lc_dead_letter_export_opts export_options;
  lc_dead_letter_export_res export_result;
  lc_outbox_stats outbox_stats;
  const void *exported_bytes;
  size_t exported_length;
  lc_error error;
  char effect_key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("outbox-retry", effect_key, sizeof(effect_key));
  client = NULL;
  outbox = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  export_sink = NULL;
  lc_error_init(&error);
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_outbox_config_init(&config);
  config.ns = "default";
  config.owner = "outbox-retry-e2e";
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = effect_key;
  entry.effect_id = "retry";
  entry.effect_key = effect_key;
  entry.payload_digest = "sha256:outbox-retry-payload";
  entry.kind = "test";
  entry.destination = "retry://target";
  rc = lc_source_from_memory("retry-payload", 13U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_outbox_append(outbox, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  rc = lc_outbox_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  rc = lc_outbox_next(outbox, 3000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 1L;
  retry.diagnostic = "temporary";
  rc = lc_outbox_job_retry(job, &retry, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  rc = lc_outbox_next(outbox, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  assert_int_equal(job->attempt, 2);
  rc = lc_outbox_job_dead_letter(job, "permanent", &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  lc_outbox_stats_init(&outbox_stats);
  rc = lc_outbox_get_stats(outbox, &outbox_stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(outbox_stats.running);
  lc_outbox_stats_cleanup(&outbox_stats);
  lc_dead_letter_export_opts_init(&export_options);
  export_options.format = LC_DEAD_LETTER_EXPORT_JSONL;
  lc_dead_letter_export_res_init(&export_result);
  rc = lc_sink_to_memory(&export_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_export_dead_letters(outbox, &export_options, export_sink,
                                       &export_result, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(export_result.exported, 1U);
  exported_bytes = NULL;
  exported_length = 0U;
  rc = lc_sink_memory_bytes(export_sink, &exported_bytes, &exported_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_true(exported_length > 0U);
  lc_sink_close(export_sink);
  export_sink = NULL;
  rc = lc_outbox_replay_dead_letter(outbox, receipt.outbox_key, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_next(outbox, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  assert_int_equal(job->attempt, 1);
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  lc_dead_letter_export_res_init(&export_result);
  rc = lc_sink_to_memory(&export_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_export_dead_letters(outbox, &export_options, export_sink,
                                       &export_result, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(export_result.exported, 0U);
  lc_sink_close(export_sink);
  export_sink = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_outbox_startup_recovery(void **state) {
  static const char state_json[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"recovery-op\","
      "\"effect_id\":\"recovery-effect\",\"effect_key\":\"recovery-key\","
      "\"payload_digest\":\"recovery-payload-digest\","
      "\"message_id\":\"msg_recovery\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"pending\","
      "\"attempt_count\":0,\"not_before_unix\":0}";
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_config config;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_outbox_job *job;
  lc_error error;
  char suffix[64];
  char key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./build/devenv/credentials/disk/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("outbox-recovery", suffix, sizeof(suffix));
  /* This fixture sorts before digest-shaped production keys so a bounded
   * recovery page proves discovery without claiming unrelated durable work in
   * the shared local-test namespace. */
  assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/-%s", suffix) >
              0);
  client = NULL;
  outbox = NULL;
  lease = NULL;
  state_source = NULL;
  payload_source = NULL;
  job = NULL;
  lc_error_init(&error);
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.ns = "default";
  acquire.key = key;
  acquire.owner = "outbox-recovery-e2e";
  acquire.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lc_source_from_memory(state_json, sizeof(state_json) - 1U, &state_source,
                             &error);
  assert_lc_ok(rc, &error);
  rc = lc_lease_update(lease, state_source, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(state_source);
  state_source = NULL;
  rc = lc_source_from_memory("recovery-payload", 16U, &payload_source, &error);
  assert_lc_ok(rc, &error);
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = "text/plain";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  rc = lc_lease_attach(lease, &attach, payload_source, &attach_result, &error);
  assert_lc_ok(rc, &error);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload_source);
  payload_source = NULL;
  rc = lc_lease_release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  lc_outbox_config_init(&config);
  config.ns = "default";
  config.owner = "outbox-recovery-e2e";
  config.notification_capacity = 1U;
  rc = lc_client_new_outbox(client, &config, &outbox, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_next(outbox, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  job = NULL;
  lc_outbox_close(outbox);
  lc_client_close(client);
  lc_error_cleanup(&error);
}
#endif

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_disk_lease_state_roundtrip),
      cmocka_unit_test(test_disk_server_minted_multikey_xa_transaction),
      cmocka_unit_test(test_disk_server_explicit_xa_enlists_on_acquire),
      cmocka_unit_test(
          test_disk_server_metadata_finalization_preserves_staged_version),
      cmocka_unit_test(test_disk_acquire_for_update_roundtrip),
      cmocka_unit_test(test_disk_acquire_for_update_handler_error_rolls_back),
      cmocka_unit_test(test_disk_acquire_if_not_exists_conflict),
      cmocka_unit_test(test_disk_state_cas_failure_modes),
      cmocka_unit_test(test_disk_query_rejects_invalid_inputs),
      cmocka_unit_test(test_disk_query_documents_captures_lockd_trailers),
      cmocka_unit_test(test_disk_query_keys_streams_many_indexed_keys),
      cmocka_unit_test(test_disk_management_failure_modes),
      cmocka_unit_test(test_disk_auth_permission_failure_modes),
      cmocka_unit_test(
          test_disk_queue_watch_stream_observes_initial_unavailable),
      cmocka_unit_test(test_disk_queue_watch_stream_observes_enqueue),
      cmocka_unit_test(
          test_disk_queue_watch_stream_observes_available_then_unavailable),
      cmocka_unit_test(test_disk_local_mutate_stream_roundtrip)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_POUCH_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(
          test_pouch_direct_migrates_legacy_lease_before_client_open),
      cmocka_unit_test(
          test_pouch_direct_transformed_metadata_replay_defers_body_materialization),
      cmocka_unit_test(test_pouch_direct_query_indexing_disabled_roundtrip),
      cmocka_unit_test(test_pouch_direct_route_command_wait_roundtrip),
      cmocka_unit_test(test_pouch_direct_state_attachment_reopen_roundtrip),
      cmocka_unit_test(
          test_pouch_direct_lifecycle_maintenance_reopen_roundtrip),
      cmocka_unit_test(
          test_pouch_direct_large_namespace_segmented_index_reopen),
      cmocka_unit_test(test_pouch_terminal_reclaim_one_segment_churn_reopen),
      cmocka_unit_test(
          test_pouch_shared_hardening_churn_dispatch_and_maintenance),
      cmocka_unit_test(
          test_pouch_direct_marker_damage_and_index_rebuild_after_snapshot),
      cmocka_unit_test(test_pouch_direct_consumer_service_with_state)};
  return cmocka_run_group_tests(tests, setup_pouch_e2e_group,
                                teardown_pouch_e2e_group);
}
#elif defined(LC_E2E_GROUP_POUCH_SHARED_OUTBOX)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_shared_outbox_competing_instances),
      cmocka_unit_test(test_pouch_shared_outbox_competing_stateful_instances),
      cmocka_unit_test(test_pouch_shared_outbox_independent_namespaces),
      cmocka_unit_test(test_pouch_shared_outbox_rolling_handoff),
      cmocka_unit_test(test_pouch_shared_outbox_abandoned_claim_recovers),
      cmocka_unit_test(
          test_pouch_shared_outbox_abandoned_stateful_claim_recovers),
      cmocka_unit_test(
          test_pouch_stateful_outbox_retry_and_replay_preserve_checkpoint),
      cmocka_unit_test(test_pouch_shared_command_competing_instances),
      cmocka_unit_test(
          test_pouch_shared_command_recovers_after_foreign_effect_crash),
      cmocka_unit_test(
          test_pouch_shared_command_terminal_commit_survives_job_crash),
      cmocka_unit_test(test_pouch_shared_command_waiter_blocks_until_terminal)};
  return cmocka_run_group_tests(tests, setup_pouch_e2e_group,
                                teardown_pouch_e2e_group);
}
#elif defined(LC_E2E_GROUP_S3_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_s3_lease_state_roundtrip),
      cmocka_unit_test(test_s3_attachment_failure_modes),
      cmocka_unit_test(test_s3_local_mutate_stream_roundtrip),
      cmocka_unit_test(test_s3_attachment_roundtrip),
      cmocka_unit_test(test_s3_dequeue_with_state_roundtrip)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_MEM_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mem_uds_dequeue_with_state_roundtrip),
      cmocka_unit_test(test_mem_uds_dequeue_batch_roundtrip),
      cmocka_unit_test(test_mem_uds_queue_roundtrip),
      cmocka_unit_test(test_mem_uds_queue_failure_modes)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_DISK_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_disk_consumer_service_happy),
      cmocka_unit_test(test_disk_consumer_service_explicit_ack),
      cmocka_unit_test(test_disk_consumer_service_multi_delivery),
      cmocka_unit_test(test_disk_consumer_service_with_state),
      cmocka_unit_test(
          test_disk_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_disk_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_disk_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_disk_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_disk_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(test_disk_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_S3_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_s3_consumer_service_happy),
      cmocka_unit_test(test_s3_consumer_service_explicit_ack),
      cmocka_unit_test(test_s3_consumer_service_multi_delivery),
      cmocka_unit_test(test_s3_consumer_service_with_state),
      cmocka_unit_test(test_s3_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_s3_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_s3_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_s3_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_s3_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(test_s3_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(test_s3_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_s3_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_s3_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_MEM_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mem_uds_consumer_service_happy),
      cmocka_unit_test(test_mem_uds_consumer_service_explicit_ack),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_delivery),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_worker),
      cmocka_unit_test(test_mem_uds_consumer_service_with_state),
      cmocka_unit_test(
          test_mem_uds_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_mem_uds_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_mem_uds_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_mem_uds_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_mem_uds_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(
          test_mem_uds_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#else
#error "An LC_E2E_GROUP_* compile definition is required"
#endif
