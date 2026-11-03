#include <cmocka.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pslog.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"
#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_collection_internal.h"
#include "lc_pouch.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_recode.h"
#include "lc_pouch_record.h"

#define PREFIX "/tmp/liblockdc-compression-"
typedef struct fixture {
  char root[256], endpoint[320];
  lc_client *client;
  lc_collection *collection;
  lc_error error;
  char *key;
  FILE *log;
  pslog_logger *logger;
  int query_indexing;
  int custom_allocator;
  size_t fail_allocation_size;
  mode_t saved_umask;
  int restore_umask;
} fixture;

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("compression error %d: %s [%s]\n", rc,
                f->error.message ? f->error.message : "",
                f->error.detail ? f->error.detail : "");
  assert_int_equal(rc, LC_OK);
}
static void *test_allocate(void *context, size_t size) {
  fixture *f = context;
  if (size == f->fail_allocation_size) {
    f->fail_allocation_size = 0;
    return NULL;
  }
  return malloc(size);
}
static void *test_reallocate(void *context, void *pointer, size_t size) {
  fixture *f = context;
  if (size == f->fail_allocation_size) {
    f->fail_allocation_size = 0;
    return NULL;
  }
  return realloc(pointer, size);
}
static void fixture_free(void *context, void *pointer) {
  (void)context;
  free(pointer);
}
static int open_client(fixture *f, const char *mode, lc_client **out) {
  lc_client_config config;
  lc_pouch_settings settings;
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_BACKGROUND_COMPACTION |
                      LC_POUCH_SETTING_SINGLE_WRITER |
                      LC_POUCH_SETTING_QUERY_INDEXING;
  settings.query_indexing_enabled = f->query_indexing;
  if (mode) {
    settings.set_mask |= LC_POUCH_SETTING_COMPRESSION;
    settings.compression = mode;
  }
  if (f->key) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = f->key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "test";
  config.pouch_settings = &settings;
  config.logger = f->logger;
  if (f->custom_allocator) {
    config.allocator.malloc_fn = test_allocate;
    config.allocator.realloc_fn = test_reallocate;
    config.allocator.free_fn = fixture_free;
    config.allocator.context = f;
  }
  return lc_client_open(&config, out, &f->error);
}
static void expect_mode(fixture *f, lc_client *client, const char *mode) {
  lc_pouch_status status = {0};
  check(f, lc_pouch_status_read(((lc_client_handle *)client)->pouch, &status,
                                &f->error));
  assert_string_equal(status.compression, mode);
  assert_string_equal(((lc_client_handle *)client)->pouch_compression, mode);
  lc_pouch_status_cleanup(&((lc_client_handle *)client)->allocator, &status);
}
static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char pattern[] = PREFIX "XXXXXX";
  pslog_config log;
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  assert_true(lc_test_tmp_mkdtemp(pattern, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  f->log = tmpfile();
  assert_non_null(f->log);
  pslog_default_config(&log);
  log.mode = PSLOG_MODE_JSON;
  log.min_level = PSLOG_LEVEL_TRACE;
  log.timestamps = 0;
  log.verbose_fields = 1;
  log.output = pslog_output_from_fp(f->log, 0);
  f->logger = pslog_new(&log);
  assert_non_null(f->logger);
  return 0;
}
static int teardown(void **state) {
  fixture *f = *state;
  lc_pouch_recode_test_hook = NULL;
  lc_pouch_recode_test_context = NULL;
  if (f) {
    if (f->restore_umask)
      umask(f->saved_umask);
    lc_collection_close(f->collection);
    lc_client_close(f->client);
    lc_pouch_crypto_key_string_free(f->key);
    lc_error_cleanup(&f->error);
    f->logger->destroy(f->logger);
    fclose(f->log);
    lc_test_tmp_cleanup_path(f->root, PREFIX);
    free(f);
  }
  return 0;
}
static void close_client(fixture *f) {
  lc_client_close(f->client);
  f->client = NULL;
}
static void write_state(fixture *f) {
  lc_acquire_req acquire;
  lc_update_opts update = {0};
  lc_metadata_req metadata = {0};
  lc_attach_req attach;
  lc_attach_res attached = {0};
  lc_lease *lease = NULL;
  lc_source *source = NULL;
  char *bytes = malloc(65536);
  assert_non_null(bytes);
  memset(bytes, 'x', 65536);
  lc_acquire_req_init(&acquire);
  acquire.key = "state";
  acquire.owner = "compression-test";
  acquire.ttl_seconds = 3600;
  update.has_retention_seconds = 1;
  update.retention_seconds = 7776000;
  check(f, f->client->acquire(f->client, &acquire, &lease, &f->error));
  check(f, lc_source_from_memory(bytes, 65536, &source, &f->error));
  check(f, lease->update(lease, source, &update, &f->error));
  lc_source_close(source);
  lc_attach_req_init(&attach);
  attach.name = "receipt";
  check(f, lc_source_from_memory("attachment", 10, &source, &f->error));
  check(f, lease->attach(lease, &attach, source, &attached, &f->error));
  lc_source_close(source);
  lc_attach_res_cleanup(&attached);
  check(f, lease->release(lease, NULL, &f->error));
  check(f, f->client->acquire(f->client, &acquire, &lease, &f->error));
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 7776000;
  check(f, lease->metadata(lease, &metadata, &f->error));
  check(f, lease->release(lease, NULL, &f->error));
  free(bytes);
}
static void read_state(fixture *f, char etag[65], lc_version *version) {
  lc_sink *sink = NULL;
  lc_get_res result = {0};
  const void *bytes;
  size_t size, i;
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, f->client->get(f->client, "state", NULL, sink, &result, &f->error));
  assert_false(result.no_content);
  check(f, lc_sink_memory_bytes(sink, &bytes, &size, &f->error));
  assert_int_equal(size, 65536);
  for (i = 0; i < size; ++i)
    assert_int_equal(((const char *)bytes)[i], 'x');
  if (etag[0]) {
    assert_string_equal(etag, result.etag);
    assert_int_equal(*version, result.version);
  } else {
    strcpy(etag, result.etag);
    *version = result.version;
  }
  assert_true(result.retention_expires_at_unix > 0);
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
  {
    lc_attachment_get_op request = {0};
    lc_attachment_get_res attachment = {0};
    request.public_read = 1;
    request.lease.key = "state";
    request.selector.name = "receipt";
    check(f, lc_sink_to_memory(&sink, &f->error));
    check(f,
          lc_get_attachment(f->client, &request, sink, &attachment, &f->error));
    check(f, lc_sink_memory_bytes(sink, &bytes, &size, &f->error));
    assert_int_equal(size, 10);
    assert_memory_equal(bytes, "attachment", size);
    lc_attachment_get_res_cleanup(&attachment);
    lc_sink_close(sink);
  }
}
static void
test_omission_inherits_and_explicit_changes_empty_root(void **state) {
  fixture *f = *state;
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "none");
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  expect_mode(f, f->client, "zlib");
  close_client(f);
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "zlib");
  close_client(f);
  check(f, open_client(f, "none", &f->client));
  expect_mode(f, f->client, "none");
}
static void test_both_directions_preserve_shared_payloads(void **state) {
  fixture *f = *state;
  char etag[65] = {0};
  lc_version version = 0;
  check(f, open_client(f, "none", &f->client));
  write_state(f);
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  expect_mode(f, f->client, "zlib");
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, NULL, &f->client));
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, "none", &f->client));
  expect_mode(f, f->client, "none");
  read_state(f, etag, &version);
}
static void test_encrypted_both_directions(void **state) {
  fixture *f = *state;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  test_both_directions_preserve_shared_payloads(state);
}
static void
test_active_writer_refuses_conversion_and_adopts_root(void **state) {
  fixture *f = *state;
  lc_client *peer = NULL;
  check(f, open_client(f, "none", &f->client));
  check(f, open_client(f, "zlib", &peer));
  expect_mode(f, peer, "none");
  lc_client_close(peer);
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  check(f, open_client(f, "none", &peer));
  expect_mode(f, peer, "zlib");
  lc_client_close(peer);
}
static int crash_at(void *context, int phase, lc_error *error) {
  (void)error;
  if (phase == *(int *)context)
    _exit(41);
  return LC_OK;
}
static void
test_publication_crashes_resume_without_explicit_option(void **state) {
  fixture *f = *state;
  char base[256], etag[65];
  lc_version version;
  pid_t child;
  int phase, crash_phase, status;
  strcpy(base, f->root);
  for (phase = 0; phase < 2 * LC_POUCH_RECODE_RECLAIMED; ++phase) {
    const char *from = phase < LC_POUCH_RECODE_RECLAIMED ? "none" : "zlib";
    const char *to = phase < LC_POUCH_RECODE_RECLAIMED ? "zlib" : "none";
    crash_phase = 1 + phase % LC_POUCH_RECODE_RECLAIMED;
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/phase-%d", base,
             phase);
    etag[0] = '\0';
    version = 0;
    check(f, open_client(f, from, &f->client));
    write_state(f);
    read_state(f, etag, &version);
    close_client(f);
    child = fork();
    assert_true(child >= 0);
    if (child == 0) {
      lc_pouch_recode_test_hook = crash_at;
      lc_pouch_recode_test_context = &crash_phase;
      (void)open_client(f, to, &f->client);
      _exit(42);
    }
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 41);
    check(f, open_client(f, NULL, &f->client));
    expect_mode(f, f->client, to);
    read_state(f, etag, &version);
    close_client(f);
  }
}

static void expect_json(fixture *f, lc_collection *collection,
                        lc_collection_read_opts *options, const char *json,
                        char *cursor) {
  lc_sink *sink = NULL;
  lc_collection_read_res result = {0};
  const void *bytes;
  size_t length;
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(collection, options, sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
  assert_int_equal(length, strlen(json));
  if (length)
    assert_memory_equal(bytes, json, length);
  assert_true(result.complete);
  if (cursor) {
    assert_non_null(result.checkpoint_cursor);
    strcpy(cursor, result.checkpoint_cursor);
  }
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
}

static void
test_collections_cursors_compaction_and_retry_bindings(void **state) {
  fixture *f = *state;
  lc_collection_config config;
  lc_collection *collection = NULL;
  lc_collection_append_req request;
  lc_collection_append_res appended = {0}, changed = {0};
  lc_collection_append_status_res status = {0};
  lc_collection_metadata_req metadata = {0};
  lc_collection_delete_res deleted = {0};
  lc_collection_info info;
  lc_collection_read_opts options = {0};
  lc_pouch_maintenance_result compacted = {0};
  lc_source *source = NULL;
  lc_collection_writer *writer = NULL;
  char cursor[LC_COLLECTION_CURSOR_BYTES], id[LC_XID_STRING_SIZE],
      append_id[16];
  char json[32];
  const char *modes[] = {"zlib", "none"};
  unsigned int i;
  f->query_indexing = 1;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  check(f, open_client(f, "none", &f->client));
  lc_collection_config_init(&config);
  config.name = "empty";
  check(f, lc_client_create_collection(f->client, &config, &collection,
                                       &f->error));
  lc_collection_close(collection);
  config.name = "events";
  config.has_max_entries = 1;
  config.max_entries = 100;
  check(f, lc_client_create_collection(f->client, &config, &collection,
                                       &f->error));
  for (i = 1; i <= 3; ++i) {
    snprintf(append_id, sizeof(append_id), "event-%u", i);
    snprintf(json, sizeof(json), "{ \"n\" : %u }", i);
    lc_collection_append_req_init(&request);
    request.append_id = append_id;
    request.has_collection_retention_seconds = 1;
    request.collection_retention_seconds = 7776000;
    if (i == 1) {
      check(f, lc_collection_append_begin(collection, &request, &writer,
                                          &f->error));
      check(f,
            lc_collection_writer_write(writer, json, strlen(json), &f->error));
      check(f, lc_collection_writer_commit(writer, &appended, &f->error));
      lc_collection_writer_close(writer);
    } else {
      check(f, lc_source_from_memory(json, strlen(json), &source, &f->error));
      check(f, lc_collection_append(collection, &request, source, &appended,
                                    &f->error));
      lc_source_close(source);
    }
    assert_int_equal(appended.position, i);
  }
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 7776000;
  check(f, lc_collection_entry_metadata(collection, 1, &metadata, &changed,
                                        &f->error));
  check(f, lc_collection_delete_entry(collection, 2, &deleted, &f->error));
  check(f, lc_collection_describe(collection, &info, &f->error));
  strcpy(id, info.collection_id);
  check(f, lc_collection_compact(collection, 1, 0, &compacted, &f->error));
  lc_pouch_maintenance_result_cleanup(NULL, &compacted);
  options.has_first_n = 1;
  options.first_n = 1;
  expect_json(f, collection, &options, "{\"n\":1}\n", cursor);
  lc_collection_close(collection);
  close_client(f);
  config.has_max_entries = 0;
  for (i = 0; i < 2; ++i) {
    check(f, open_client(f, modes[i], &f->client));
    check(f, lc_client_open_collection(f->client, &config, &collection,
                                       &f->error));
    check(f, lc_collection_describe(collection, &info, &f->error));
    assert_string_equal(info.collection_id, id);
    assert_int_equal(info.upper_position, 3);
    assert_int_equal(info.live_count, 2);
    assert_int_equal(info.max_entries, 100);
    assert_true(info.has_retention_expires_at_unix);
    options.cursor = cursor;
    expect_json(f, collection, &options, "{\"n\":3}\n", NULL);
    {
      lc_collection_read_opts filtered = {0};
      filtered.selector_lql = "eq{field=/n,value=3}";
      expect_json(f, collection, &filtered, "{\"n\":3}\n", NULL);
    }
    check(f, lc_collection_append_status(collection, "event-1", &status,
                                         &f->error));
    assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
    assert_string_equal(status.entry.etag, changed.etag);
    assert_int_equal(status.entry.retention_expires_at_unix,
                     changed.retention_expires_at_unix);
    check(f, lc_collection_append_status(collection, "event-2", &status,
                                         &f->error));
    assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
    lc_collection_close(collection);
    close_client(f);
  }
}

static int fail_before_publication(void *context, int phase, lc_error *error) {
  (void)context;
  if (phase == LC_POUCH_RECODE_BUILT)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                        "injected conversion I/O failure", NULL, NULL, "pouch");
  return LC_OK;
}
static void test_failed_build_keeps_source_and_retry_succeeds(void **state) {
  fixture *f = *state;
  char etag[65] = {0};
  lc_version version = 0;
  check(f, open_client(f, "none", &f->client));
  write_state(f);
  read_state(f, etag, &version);
  close_client(f);
  lc_pouch_recode_test_hook = fail_before_publication;
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_TRANSPORT);
  assert_null(f->client);
  lc_pouch_recode_test_hook = NULL;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "zlib");
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  read_state(f, etag, &version);
}

static void
test_conversion_log_levels_and_no_work_on_inherited_open(void **state) {
  fixture *f = *state;
  char line[4096];
  int starts = 0, completes = 0, refused = 0;
  test_active_writer_refuses_conversion_and_adopts_root(state);
  close_client(f);
  check(f, open_client(f, NULL, &f->client));
  fflush(f->log);
  rewind(f->log);
  while (fgets(line, sizeof(line), f->log)) {
    if (strstr(line, "compression.convert.start")) {
      ++starts;
      assert_non_null(strstr(line, "\"level\":\"warn\""));
    }
    if (strstr(line, "compression.convert.complete")) {
      ++completes;
      assert_non_null(strstr(line, "\"level\":\"info\""));
    }
    if (strstr(line, "compression.convert.refused")) {
      ++refused;
      assert_non_null(strstr(line, "\"level\":\"warn\""));
    }
  }
  assert_int_equal(starts, 1);
  assert_int_equal(completes, 1);
  assert_int_equal(refused, 2);
}

static void
test_staged_payload_survives_both_directions_and_promotes(void **state) {
  fixture *f = *state;
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_read_result read = {0};
  lc_source *source = NULL;
  char value[32] = {0};
  const char *modes[] = {"none", "zlib", "none"};
  size_t i;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  check(f, open_client(f, modes[0], &f->client));
  check(f, lc_source_from_memory("staged-body", 11, &source, &f->error));
  check(f,
        lc_pouch_state_stage_write(((lc_client_handle *)f->client)->pouch,
                                   "staged-only", "business", "compression-tx",
                                   source, &options, &written, &f->error));
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &written);
  close_client(f);
  for (i = 1; i < 3; ++i) {
    check(f, open_client(f, modes[i], &f->client));
    check(f, lc_pouch_state_read(((lc_client_handle *)f->client)->pouch,
                                 "staged-only", "business", &read, &f->error));
    assert_false(read.found);
    lc_pouch_state_read_result_cleanup(NULL, &read);
    if (i == 1)
      close_client(f);
  }
  check(f, lc_pouch_state_commit_staged(((lc_client_handle *)f->client)->pouch,
                                        "staged-only", "business",
                                        "compression-tx", &written, &f->error));
  lc_pouch_state_write_result_cleanup(NULL, &written);
  check(f, lc_pouch_state_read(((lc_client_handle *)f->client)->pouch,
                               "staged-only", "business", &read, &f->error));
  assert_true(read.found);
  assert_int_equal(read.body->read(read.body, value, sizeof(value), &f->error),
                   11);
  assert_string_equal(value, "staged-body");
  lc_pouch_state_read_result_cleanup(NULL, &read);
}

static void test_process_writer_refuses_conversion(void **state) {
  fixture *f = *state;
  int ready[2], finish[2], status;
  char signal;
  pid_t child;
  /* Fork before either process owns Pouch's pthread state. */
  assert_int_equal(pipe(ready), 0);
  assert_int_equal(pipe(finish), 0);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    close(ready[0]);
    close(finish[1]);
    if (open_client(f, "none", &f->client) != LC_OK)
      _exit(42);
    if (write(ready[1], "r", 1) != 1 || read(finish[0], &signal, 1) != 1)
      _exit(43);
    close_client(f);
    _exit(0);
  }
  close(ready[1]);
  close(finish[0]);
  assert_int_equal(read(ready[0], &signal, 1), 1);
  check(f, open_client(f, "zlib", &f->client));
  expect_mode(f, f->client, "none");
  close_client(f);
  assert_int_equal(write(finish[1], "f", 1), 1);
  close(ready[0]);
  close(finish[1]);
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  check(f, open_client(f, "zlib", &f->client));
  expect_mode(f, f->client, "zlib");
}

typedef struct generated_body {
  lc_source source;
  lc_sink sink;
  size_t remaining;
} generated_body;
static size_t generate_read(lc_source *source, void *buffer, size_t capacity,
                            lc_error *error) {
  generated_body *body = source->impl;
  size_t count = body->remaining < capacity ? body->remaining : capacity;
  (void)error;
  assert_true(capacity <= 65536);
  memset(buffer, 'z', count);
  body->remaining -= count;
  return count;
}
static int generated_write(lc_sink *sink, const void *bytes, size_t length,
                           lc_error *error) {
  generated_body *body = sink->impl;
  size_t i;
  (void)error;
  assert_true(length <= 65536 && length <= body->remaining);
  for (i = 0; i < length; ++i)
    assert_int_equal(((const char *)bytes)[i], 'z');
  body->remaining -= length;
  return 1;
}
static void
test_streamed_large_retained_history_preserves_sequences(void **state) {
  fixture *f = *state;
  generated_body body = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_read_result read = {0};
  lc_pouch_state_write_options options = {0};
  lc_history_consumer_config config;
  lc_history_consumer *consumer = NULL;
  lc_history_consumer_position before, after;
  lc_pouch_maintenance_options maintenance = {0};
  lc_pouch_maintenance_result result = {0};
  const char *modes[] = {"zlib", "none"};
  lc_pouch *pouch;
  size_t i;
  body.source.read = generate_read;
  body.source.impl = &body;
  body.sink.write = generated_write;
  body.sink.impl = &body;
  check(f, open_client(f, "none", &f->client));
  pouch = ((lc_client_handle *)f->client)->pouch;
  pouch->segment_target_bytes = 4096;
  for (i = 0; i < 2; ++i) {
    body.remaining = 3 * 1024 * 1024;
    check(f, lc_pouch_state_write(pouch, "history", "large", &body.source,
                                  &options, &written, &f->error));
    assert_int_equal(body.remaining, 0);
    lc_pouch_state_write_result_cleanup(NULL, &written);
  }
  lc_history_consumer_config_init(&config);
  config.ns = "history";
  config.consumer_id = "compression-history";
  check(f, lc_client_new_history_consumer(f->client, &config, &consumer,
                                          &f->error));
  check(f, lc_history_consumer_get_position(consumer, &before, &f->error));
  lc_history_consumer_close(consumer);
  maintenance.ns = "history";
  maintenance.force = 1;
  check(f, lc_pouch_maintenance_run(pouch, &maintenance, &result, &f->error));
  assert_true(result.skipped);
  assert_string_equal(result.diagnostic, "history-consumer-behind");
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  close_client(f);
  for (i = 0; i < 2; ++i) {
    check(f, open_client(f, modes[i], &f->client));
    pouch = ((lc_client_handle *)f->client)->pouch;
    check(f, lc_client_new_history_consumer(f->client, &config, &consumer,
                                            &f->error));
    check(f, lc_history_consumer_get_position(consumer, &after, &f->error));
    assert_int_equal(after.acknowledged_index_seq,
                     before.acknowledged_index_seq);
    assert_int_equal(after.current_index_seq, before.current_index_seq);
    lc_history_consumer_close(consumer);
    body.remaining = 3 * 1024 * 1024;
    check(f, lc_pouch_state_copy(pouch, "history", "large", &body.sink, &read,
                                 &f->error));
    assert_int_equal(body.remaining, 0);
    assert_int_equal(read.version, 2);
    if (i == 0)
      assert_true(read.cipher_bytes < read.bytes / 10);
    else
      assert_int_equal(read.cipher_bytes, read.bytes);
    lc_pouch_state_read_result_cleanup(NULL, &read);
    close_client(f);
  }
}

static void
test_corruption_fails_before_publication_and_preserves_mode(void **state) {
  fixture *f = *state;
  lc_pouch_namespace_manifest manifest = {0};
  char path[1024], etag[65] = {0};
  lc_version version = 0;
  unsigned char original, corrupted;
  int fd;
  check(f, open_client(f, "none", &f->client));
  write_state(f);
  read_state(f, etag, &version);
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "test", &manifest,
                                            NULL, NULL, &f->error));
  snprintf(path, sizeof(path), "%s/namespaces/test/segments/%s", f->root,
           manifest.segment_leaves[0]);
  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  close_client(f);
  fd = open(path, O_RDWR);
  assert_true(fd >= 0);
  /* A complete canonical header must never be treated as a torn tail. */
  assert_int_equal(pread(fd, &original, 1, 0), 1);
  corrupted = original ^ 0x40;
  assert_int_equal(pwrite(fd, &corrupted, 1, 0), 1);
  assert_int_equal(fsync(fd), 0);
  assert_int_not_equal(open_client(f, "zlib", &f->client), LC_OK);
  assert_null(f->client);
  assert_int_equal(pwrite(fd, &original, 1, 0), 1);
  assert_int_equal(fsync(fd), 0);
  close(fd);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "zlib");
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  read_state(f, etag, &version);
}

static void test_compacted_snapshot_preserves_payload_links(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  char etag[65] = {0};
  lc_version version = 0;
  const char *modes[] = {"zlib", "none"};
  unsigned int i;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  check(f, open_client(f, "none", &f->client));
  ((lc_client_handle *)f->client)->pouch->segment_target_bytes = 4096;
  write_state(f);
  options.ns = "test";
  options.force = 1;
  check(f, lc_pouch_maintenance_run(((lc_client_handle *)f->client)->pouch,
                                    &options, &result, &f->error));
  assert_true(result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  read_state(f, etag, &version);
  close_client(f);
  for (i = 0; i < 2; ++i) {
    check(f, open_client(f, modes[i], &f->client));
    read_state(f, etag, &version);
    close_client(f);
  }
}

static const char compacted_body[] =
    "retained payload retained payload retained payload retained payload";

static void compacted_write(fixture *f, const char *key) {
  lc_source *source = NULL;
  lc_pouch_state_write_result written = {0};
  lc_pouch_state_write_options options = {0};
  options.content_type = "application/octet-stream";
  check(f, lc_source_from_memory(compacted_body, sizeof(compacted_body) - 1,
                                 &source, &f->error));
  check(f, lc_pouch_state_write(((lc_client_handle *)f->client)->pouch,
                                "compacted", key, source, &options, &written,
                                &f->error));
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &written);
}

static void
expect_compacted_payload(fixture *f,
                         const lc_pouch_state_read_result *expected) {
  lc_pouch_state_read_result read = {0};
  lc_sink *sink = NULL;
  const void *bytes;
  size_t size;
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_pouch_state_copy(((lc_client_handle *)f->client)->pouch,
                               "compacted", "payload", sink, &read, &f->error));
  assert_true(read.found);
  check(f, lc_sink_memory_bytes(sink, &bytes, &size, &f->error));
  assert_int_equal(size, sizeof(compacted_body) - 1);
  assert_memory_equal(bytes, compacted_body, size);
  assert_string_equal(read.etag, expected->etag);
  assert_string_equal(read.content_type, expected->content_type);
  assert_int_equal(read.version, expected->version);
  assert_int_equal(read.index_seq, expected->index_seq);
  assert_int_equal(read.updated_at_unix, expected->updated_at_unix);
  assert_int_equal(read.retention, expected->retention);
  assert_int_equal(read.has_query_hidden, expected->has_query_hidden);
  assert_int_equal(read.query_hidden, expected->query_hidden);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_sink_close(sink);
}

static void obsolete_segment_conversion(fixture *f) {
  const char *modes[] = {"none", "zlib"};
  lc_pouch *pouch;
  lc_pouch_namespace_manifest manifest = {0};
  lc_pouch_state_read_result expected = {0};
  lc_pouch_state_write_options metadata = {0};
  lc_pouch_state_write_result written = {0};
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  struct stat st;
  char roll[32], *directory, *path;
  unsigned int direction, round;
  unsigned long i;
  options.ns = "compacted";
  options.force = 1;
  metadata.has_retention = 1;
  metadata.retention = (lc_pouch_unix_seconds)4102444800UL;
  metadata.has_query_hidden = 1;
  metadata.query_hidden = 1;
  for (direction = 0; direction < 2; ++direction) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/grace-%u", f->root,
             direction);
    check(f, open_client(f, modes[direction], &f->client));
    pouch = ((lc_client_handle *)f->client)->pouch;
    pouch->segment_target_bytes = 1;
    pouch->compaction_delete_grace_seconds = 86400;
    compacted_write(f, "payload");
    for (round = 0; round < 3; ++round) {
      if (round != 0) {
        metadata.retention += 1;
        check(f,
              lc_pouch_state_update_metadata(pouch, "compacted", "payload",
                                             &metadata, &written, &f->error));
        lc_pouch_state_write_result_cleanup(NULL, &written);
      }
      snprintf(roll, sizeof(roll), "roll-%u", round);
      compacted_write(f, roll);
      check(f, lc_pouch_maintenance_run(pouch, &options, &result, &f->error));
      assert_true(result.compacted);
      lc_pouch_maintenance_result_cleanup(NULL, &result);
    }
    /* Keep a live sealed reference to the latest snapshot as well. Filtering
     * must drop only obsolete segments, preserving subsequent metadata. */
    metadata.retention += 1;
    check(f, lc_pouch_state_update_metadata(pouch, "compacted", "payload",
                                            &metadata, &written, &f->error));
    lc_pouch_state_write_result_cleanup(NULL, &written);
    compacted_write(f, "live-tail");
    check(f, lc_pouch_state_read_metadata(pouch, "compacted", "payload",
                                          &expected, &f->error));
    check(f,
          lc_pouch_namespace_manifest_open(NULL, pouch->root_path, "compacted",
                                           &manifest, NULL, NULL, &f->error));
    assert_true(manifest.obsolete_segment_count > 1);
    assert_true(manifest.obsolete_snapshot_count > 1);
    for (i = 0; i < manifest.obsolete_segment_count; ++i) {
      directory = lc_pouch_path_join(NULL, manifest.namespace_path, "segments");
      assert_non_null(directory);
      path = lc_pouch_path_join(NULL, directory, manifest.obsolete_segments[i]);
      assert_non_null(path);
      assert_int_equal(stat(path, &st), 0);
      lc_free_with_allocator(NULL, path);
      lc_free_with_allocator(NULL, directory);
    }
    close_client(f);
    check(f, open_client(f, NULL, &f->client));
    expect_compacted_payload(f, &expected);
    close_client(f);
    check(f, open_client(f, modes[1 - direction], &f->client));
    expect_mode(f, f->client, modes[1 - direction]);
    expect_compacted_payload(f, &expected);
    for (i = 0; i < manifest.obsolete_segment_count; ++i) {
      directory = lc_pouch_path_join(NULL, manifest.namespace_path, "segments");
      assert_non_null(directory);
      path = lc_pouch_path_join(NULL, directory, manifest.obsolete_segments[i]);
      assert_non_null(path);
      assert_int_equal(stat(path, &st), -1);
      assert_int_equal(errno, ENOENT);
      lc_free_with_allocator(NULL, path);
      lc_free_with_allocator(NULL, directory);
    }
    close_client(f);
    check(f, open_client(f, NULL, &f->client));
    expect_compacted_payload(f, &expected);
    close_client(f);
    lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
    lc_pouch_state_read_result_cleanup(NULL, &expected);
  }
}
static void test_conversion_omits_obsolete_segment_references(void **state) {
  obsolete_segment_conversion(*state);
}
static void
test_encrypted_conversion_omits_obsolete_segment_references(void **state) {
  fixture *f = *state;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  obsolete_segment_conversion(f);
}
static void test_allocation_failure_keeps_source_and_retries(void **state) {
  fixture *f = *state;
  char etag[65] = {0};
  lc_version version = 0;
  f->custom_allocator = 1;
  check(f, open_client(f, "none", &f->client));
  write_state(f);
  read_state(f, etag, &version);
  close_client(f);
  f->fail_allocation_size = 65536;
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_NOMEM);
  assert_null(f->client);
  assert_int_equal(f->fail_allocation_size, 0);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "zlib");
  read_state(f, etag, &version);
  close_client(f);
  check(f, open_client(f, "zlib", &f->client));
  read_state(f, etag, &version);
}
static int open_other_root(void *context, int phase, lc_error *error) {
  fixture *f = context;
  char endpoint[sizeof(f->endpoint)];
  lc_client *other = NULL;
  int rc;
  (void)error;
  if (phase != LC_POUCH_RECODE_READY)
    return LC_OK;
  strcpy(endpoint, f->endpoint);
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/unrelated", f->root);
  rc = open_client(f, NULL, &other);
  lc_client_close(other);
  strcpy(f->endpoint, endpoint);
  return rc;
}
static void test_conversion_does_not_block_unrelated_root(void **state) {
  fixture *f = *state;
  check(f, open_client(f, "none", &f->client));
  close_client(f);
  lc_pouch_recode_test_hook = open_other_root;
  lc_pouch_recode_test_context = f;
  check(f, open_client(f, "zlib", &f->client));
}
static void test_wrong_key_cannot_start_conversion(void **state) {
  fixture *f = *state;
  char *correct, etag[65] = {0};
  lc_version version = 0;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  check(f, open_client(f, "none", &f->client));
  write_state(f);
  close_client(f);
  correct = f->key;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_INVALID);
  assert_null(f->client);
  lc_pouch_crypto_key_string_free(f->key);
  f->key = correct;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, open_client(f, NULL, &f->client));
  expect_mode(f, f->client, "none");
  read_state(f, etag, &version);
}
static void test_empty_collection_torn_tail_and_storage_modes(void **state) {
  fixture *f = *state;
  lc_collection_config config;
  lc_collection *collection = NULL;
  lc_collection_handle *handle;
  lc_collection_read_opts options = {0};
  char path[1024], leaf[64], directory[1024];
  struct stat st;
  int fd;
  check(f, open_client(f, "none", &f->client));
  lc_collection_config_init(&config);
  config.name = "empty-torn";
  check(f, lc_client_create_collection(f->client, &config, &collection,
                                       &f->error));
  handle = collection->impl;
  check(f, lc_collection_record_leaf(handle->head.active, leaf, &f->error));
  snprintf(directory, sizeof(directory), "%s", handle->directory);
  assert_true(strlen(directory) + strlen(leaf) + 2 < sizeof(path));
  strcpy(path, directory);
  strcat(path, "/");
  strcat(path, leaf);
  lc_collection_close(collection);
  close_client(f);
  fd = open(path, O_WRONLY | O_APPEND);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, "cut", 3), 3);
  assert_int_equal(fchmod(fd, 0640), 0);
  assert_int_equal(fsync(fd), 0);
  close(fd);
  assert_int_equal(chmod(directory, 0750), 0);
  check(f, open_client(f, "zlib", &f->client));
  assert_int_equal(stat(directory, &st), 0);
  assert_int_equal(st.st_mode & 0777, 0750);
  assert_int_equal(stat(path, &st), 0);
  assert_int_equal(st.st_mode & 0777, 0640);
  assert_int_equal(st.st_size, 0);
  check(f,
        lc_client_open_collection(f->client, &config, &collection, &f->error));
  expect_json(f, collection, &options, "", NULL);
  lc_collection_close(collection);
}

static lc_collection *create_event(fixture *f,
                                   const lc_collection_config *config) {
  lc_collection *collection = NULL;
  lc_collection_append_req request;
  lc_collection_append_res appended;
  lc_source *source = NULL;
  check(f,
        lc_client_create_collection(f->client, config, &collection, &f->error));
  lc_collection_append_req_init(&request);
  request.append_id = "event";
  check(f, lc_source_from_memory("{}", 2, &source, &f->error));
  check(f, lc_collection_append(collection, &request, source, &appended,
                                &f->error));
  lc_source_close(source);
  return collection;
}

static void unbound_partition_conversion(fixture *f, int dropped) {
  const char *modes[] = {"none", "zlib"};
  lc_collection_config config;
  lc_collection *collection;
  lc_collection_handle *handle;
  lc_collection_read_opts options = {0};
  char id[LC_XID_STRING_SIZE], live_id[LC_XID_STRING_SIZE];
  char *partition, *data, *head;
  struct stat st;
  unsigned int direction;
  lc_collection_config_init(&config);
  config.name = "events";
  for (direction = 0; direction < 2; ++direction) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/direction-%u",
             f->root, direction);
    check(f, open_client(f, modes[direction], &f->client));
    collection = create_event(f, &config);
    handle = collection->impl;
    if (dropped) {
      partition = lc_strdup_with_allocator(NULL, handle->directory);
      assert_non_null(partition);
      check(f, lc_collection_drop(collection, &f->error));
      lc_collection_close(collection);
      head = lc_pouch_path_join(NULL, partition, "head");
      assert_non_null(head);
      /* Reclamation removed the head but stopped before the partition. The
       * surviving identity now names a different, live incarnation. */
      assert_int_equal(unlink(head), 0);
      lc_free_with_allocator(NULL, head);
      collection = create_event(f, &config);
    } else {
      check(f, lc_xid_new(id, &f->error));
      data = lc_pouch_path_join(NULL, handle->catalog_base, "data");
      assert_non_null(data);
      partition = lc_pouch_path_join(NULL, data, id);
      assert_non_null(partition);
      lc_free_with_allocator(NULL, data);
      /* Creation stopped before publishing either identity or head. */
      assert_int_equal(mkdir(partition, 0700), 0);
    }
    strcpy(live_id, collection->collection_id);
    lc_collection_close(collection);
    close_client(f);
    /* This state is already supported by ordinary open, without maintenance. */
    check(f, open_client(f, NULL, &f->client));
    check(f, lc_client_open_collection(f->client, &config, &collection,
                                       &f->error));
    expect_json(f, collection, &options, "{}\n", NULL);
    if (!dropped) {
      /* The live partition is retained by catalog membership even if its
       * derived identity is missing too. */
      head = lc_pouch_path_join(
          NULL, ((lc_collection_handle *)collection->impl)->directory,
          "identity");
      assert_non_null(head);
      assert_int_equal(unlink(head), 0);
      lc_free_with_allocator(NULL, head);
    }
    lc_collection_close(collection);
    close_client(f);
    check(f, open_client(f, modes[1 - direction], &f->client));
    expect_mode(f, f->client, modes[1 - direction]);
    /* Unbound partitions remain available to ordinary orphan maintenance. */
    assert_int_equal(stat(partition, &st), 0);
    check(f, lc_client_open_collection(f->client, &config, &collection,
                                       &f->error));
    assert_string_equal(collection->collection_id, live_id);
    expect_json(f, collection, &options, "{}\n", NULL);
    lc_collection_close(collection);
    close_client(f);
    lc_free_with_allocator(NULL, partition);
  }
}
static void
test_conversion_skips_unpublished_collection_partition(void **state) {
  unbound_partition_conversion(*state, 0);
}
static void
test_conversion_skips_partly_reclaimed_replaced_partition(void **state) {
  unbound_partition_conversion(*state, 1);
}

static void bound_head_conversion(fixture *f, int corrupt) {
  const char *modes[] = {"none", "zlib"};
  lc_collection_config config;
  lc_collection *collection;
  lc_collection_read_opts options = {0};
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES],
      damaged[LC_COLLECTION_HEAD_BYTES];
  char *head;
  FILE *fp;
  unsigned int direction;
  lc_collection_config_init(&config);
  config.name = "bound";
  for (direction = 0; direction < 2; ++direction) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/direction-%u",
             f->root, direction);
    check(f, open_client(f, modes[direction], &f->client));
    collection = create_event(f, &config);
    head = lc_pouch_path_join(
        NULL, ((lc_collection_handle *)collection->impl)->directory, "head");
    assert_non_null(head);
    lc_collection_close(collection);
    close_client(f);
    fp = fopen(head, "rb");
    assert_non_null(fp);
    assert_int_equal(fread(bytes, 1, sizeof(bytes), fp), sizeof(bytes));
    assert_int_equal(fgetc(fp), EOF);
    assert_int_equal(fclose(fp), 0);
    if (corrupt) {
      memcpy(damaged, bytes, sizeof(bytes));
      damaged[0] ^= 0x40;
      check(f, lc_pouch_path_write_bytes_file(head, damaged, sizeof(damaged),
                                              &f->error));
    } else
      assert_int_equal(unlink(head), 0);
    assert_int_not_equal(open_client(f, modes[1 - direction], &f->client),
                         LC_OK);
    assert_null(f->client);
    assert_non_null(f->error.message);
    assert_non_null(strstr(f->error.message, "head"));
    lc_error_cleanup(&f->error);
    lc_error_init(&f->error);
    assert_int_not_equal(open_client(f, NULL, &f->client), LC_OK);
    assert_null(f->client);
    lc_error_cleanup(&f->error);
    lc_error_init(&f->error);
    check(f, lc_pouch_path_write_bytes_file(head, bytes, sizeof(bytes),
                                            &f->error));
    check(f, open_client(f, modes[1 - direction], &f->client));
    check(f, lc_client_open_collection(f->client, &config, &collection,
                                       &f->error));
    expect_json(f, collection, &options, "{}\n", NULL);
    lc_collection_close(collection);
    close_client(f);
    lc_free_with_allocator(NULL, head);
  }
}
static void
test_conversion_rejects_bound_collection_without_head(void **state) {
  bound_head_conversion(*state, 0);
}
static void
test_conversion_rejects_bound_collection_corrupt_head(void **state) {
  bound_head_conversion(*state, 1);
}
static void bound_directory_symlink(fixture *f, int parent) {
  lc_collection_config config;
  lc_collection *collection;
  lc_collection_read_opts options = {0};
  char *partition, *saved;
  check(f, open_client(f, "none", &f->client));
  lc_collection_config_init(&config);
  config.name = "bound";
  collection = create_event(f, &config);
  partition =
      parent
          ? lc_pouch_path_join(
                NULL, ((lc_collection_handle *)collection->impl)->catalog_base,
                "data")
          : lc_strdup_with_allocator(
                NULL, ((lc_collection_handle *)collection->impl)->directory);
  saved = lc_pouch_path_join(NULL, f->root, "saved-partition");
  assert_non_null(partition);
  assert_non_null(saved);
  lc_collection_close(collection);
  close_client(f);
  assert_int_equal(rename(partition, saved), 0);
  assert_int_equal(symlink(saved, partition), 0);
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_INVALID);
  assert_null(f->client);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(unlink(partition), 0);
  assert_int_equal(rename(saved, partition), 0);
  check(f, open_client(f, "zlib", &f->client));
  check(f,
        lc_client_open_collection(f->client, &config, &collection, &f->error));
  expect_json(f, collection, &options, "{}\n", NULL);
  lc_collection_close(collection);
  lc_free_with_allocator(NULL, partition);
  lc_free_with_allocator(NULL, saved);
}
static void test_conversion_rejects_bound_partition_symlink(void **state) {
  bound_directory_symlink(*state, 0);
}
static void test_conversion_rejects_collection_data_symlink(void **state) {
  bound_directory_symlink(*state, 1);
}
static void encrypted_collection_metadata_conversion(fixture *f,
                                                     int compact_first) {
  const char *modes[] = {"none", "zlib"};
  const char *json = "{}\n{\"n\":2}\n";
  lc_collection_config config;
  lc_collection_append_req request;
  lc_collection_append_res second, changed;
  lc_collection_append_status_res status;
  lc_collection_metadata_req metadata = {0};
  lc_collection_read_opts options = {0};
  lc_pouch_maintenance_result result = {0};
  lc_collection_writer *writer = NULL;
  char id[LC_XID_STRING_SIZE];
  unsigned int direction, snapshot, update;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  lc_collection_config_init(&config);
  config.name = "metadata";
  metadata.has_retention_seconds = 1;
  for (direction = 0; direction < 2; ++direction) {
    for (snapshot = 0; snapshot < 2; ++snapshot) {
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/metadata-%u-%u",
               f->root, direction, snapshot);
      check(f, open_client(f, modes[direction], &f->client));
      f->collection = create_event(f, &config);
      strcpy(id, f->collection->collection_id);
      lc_collection_append_req_init(&request);
      request.append_id = "second";
      check(f, lc_collection_append_begin(f->collection, &request, &writer,
                                          &f->error));
      check(f, lc_collection_writer_write(writer, "{\"n\":2}", 7, &f->error));
      check(f, lc_collection_writer_commit(writer, &second, &f->error));
      lc_collection_writer_close(writer);
      if (snapshot) {
        check(f,
              lc_collection_compact(f->collection, 1, 0, &result, &f->error));
        assert_true(result.compacted);
        lc_pouch_maintenance_result_cleanup(NULL, &result);
      }
      /* Leave metadata-only references in the canonical log, including a
       * repeated update and a cleared expiry. Pre-conversion compaction after
       * these changes would replace the references with PUTs and mask the bug.
       */
      for (update = 0; update < 3; ++update) {
        metadata.retention_seconds = update == 1 ? 0 : 7776000;
        check(f, lc_collection_entry_metadata(f->collection, 1, &metadata,
                                              &changed, &f->error));
      }
      expect_json(f, f->collection, &options, json, NULL);
      lc_collection_close(f->collection);
      f->collection = NULL;
      close_client(f);
      check(f, open_client(f, modes[1 - direction], &f->client));
      check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                         &f->error));
      assert_string_equal(f->collection->collection_id, id);
      if (compact_first) {
        check(f,
              lc_collection_compact(f->collection, 1, 0, &result, &f->error));
        assert_true(result.compacted);
        lc_pouch_maintenance_result_cleanup(NULL, &result);
      }
      expect_json(f, f->collection, &options, json, NULL);
      check(f, lc_collection_append_status(f->collection, "event", &status,
                                           &f->error));
      assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
      assert_int_equal(status.entry.position, changed.position);
      assert_string_equal(status.entry.etag, changed.etag);
      assert_int_equal(status.entry.retention_expires_at_unix,
                       changed.retention_expires_at_unix);
      metadata.retention_seconds = 0;
      check(f, lc_collection_entry_metadata(f->collection, 1, &metadata,
                                            &changed, &f->error));
      expect_json(f, f->collection, &options, json, NULL);
      check(f, lc_collection_compact(f->collection, 1, 0, &result, &f->error));
      assert_true(result.compacted);
      lc_pouch_maintenance_result_cleanup(NULL, &result);
      lc_collection_close(f->collection);
      f->collection = NULL;
      close_client(f);
      check(f, open_client(f, NULL, &f->client));
      check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                         &f->error));
      expect_json(f, f->collection, &options, json, NULL);
      check(f, lc_collection_append_status(f->collection, "event", &status,
                                           &f->error));
      assert_int_equal(status.entry.retention_expires_at_unix, 0);
      assert_string_equal(status.entry.etag, changed.etag);
      lc_collection_close(f->collection);
      f->collection = NULL;
      close_client(f);
    }
  }
}
static void
test_encrypted_collection_metadata_reads_after_conversion(void **state) {
  encrypted_collection_metadata_conversion(*state, 0);
}
static void
test_encrypted_collection_metadata_compacts_after_conversion(void **state) {
  encrypted_collection_metadata_conversion(*state, 1);
}
typedef struct saved_collection_record {
  char path[1024];
  unsigned char bytes[4096];
  size_t length;
  lc_pouch_record_header header;
  struct stat identity;
} saved_collection_record;

static void save_collection_record(fixture *f, saved_collection_record *saved) {
  lc_collection_handle *handle = f->collection->impl;
  char leaf[64];
  int fd;
  check(f, lc_collection_record_leaf(handle->head.active, leaf, &f->error));
  assert_true(strlen(handle->directory) + strlen(leaf) + 2 <
              sizeof(saved->path));
  strcpy(saved->path, handle->directory);
  strcat(saved->path, "/");
  strcat(saved->path, leaf);
  fd = open(saved->path, O_RDONLY);
  assert_true(fd >= 0);
  assert_int_equal(fstat(fd, &saved->identity), 0);
  assert_true(saved->identity.st_size > LC_POUCH_RECORD_HEADER_BYTES);
  assert_true((uint64_t)saved->identity.st_size <= sizeof(saved->bytes));
  saved->length = (size_t)saved->identity.st_size;
  assert_int_equal(pread(fd, saved->bytes, saved->length, 0), saved->length);
  close(fd);
  check(f,
        lc_pouch_record_header_decode(saved->bytes, &saved->header, &f->error));
  assert_int_equal(saved->header.flags, 0);
  assert_true(saved->header.payload_len > 0);
  assert_int_equal(saved->length,
                   LC_POUCH_RECORD_HEADER_BYTES + saved->header.key_len +
                       saved->header.meta_len + saved->header.payload_len);
}

static void published_collection_truncation(fixture *f, int part) {
  const char *modes[] = {"none", "zlib"};
  lc_collection_config config;
  lc_collection_read_opts options = {0};
  saved_collection_record saved;
  unsigned char actual[sizeof(saved.bytes)];
  struct stat st;
  size_t cut;
  unsigned int direction, encrypted;
  int fd, rc;
  lc_collection_config_init(&config);
  config.name = "published";
  for (encrypted = 0; encrypted < 2; ++encrypted) {
    if (encrypted)
      check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    for (direction = 0; direction < 2; ++direction) {
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/cut-%u-%u",
               f->root, encrypted, direction);
      check(f, open_client(f, modes[direction], &f->client));
      f->collection = create_event(f, &config);
      save_collection_record(f, &saved);
      lc_collection_close(f->collection);
      f->collection = NULL;
      close_client(f);
      cut = part == 0 ? saved.length - 1
                      : LC_POUCH_RECORD_HEADER_BYTES + saved.header.key_len +
                            (part == 1 ? saved.header.meta_len : 0) - 1;
      fd = open(saved.path, O_RDWR);
      assert_true(fd >= 0);
      assert_int_equal(ftruncate(fd, (off_t)cut), 0);
      assert_int_equal(fsync(fd), 0);
      close(fd);
      check(f, open_client(f, NULL, &f->client));
      assert_int_equal(lc_client_open_collection(f->client, &config,
                                                 &f->collection, &f->error),
                       LC_ERR_PROTOCOL);
      assert_null(f->collection);
      close_client(f);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      rc = open_client(f, modes[1 - direction], &f->client);
      if (rc == LC_OK)
        close_client(f);
      assert_int_equal(rc, LC_ERR_PROTOCOL);
      assert_null(f->client);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      /* Pending conversion must keep failing until the source is repaired. */
      assert_int_equal(open_client(f, NULL, &f->client), LC_ERR_PROTOCOL);
      assert_null(f->client);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      fd = open(saved.path, O_RDWR);
      assert_true(fd >= 0);
      assert_int_equal(fstat(fd, &st), 0);
      assert_int_equal(st.st_ino, saved.identity.st_ino);
      assert_int_equal(st.st_size, cut);
      assert_int_equal(pread(fd, actual, cut, 0), cut);
      assert_memory_equal(actual, saved.bytes, cut);
      assert_int_equal(pwrite(fd, saved.bytes, saved.length, 0), saved.length);
      assert_int_equal(ftruncate(fd, (off_t)saved.length), 0);
      assert_int_equal(fsync(fd), 0);
      close(fd);
      check(f, open_client(f, modes[1 - direction], &f->client));
      check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                         &f->error));
      expect_json(f, f->collection, &options, "{}\n", NULL);
      lc_collection_close(f->collection);
      f->collection = NULL;
      close_client(f);
    }
  }
}
static void
test_conversion_rejects_truncated_published_collection_payload(void **state) {
  published_collection_truncation(*state, 0);
}
static void
test_conversion_rejects_truncated_published_collection_metadata(void **state) {
  published_collection_truncation(*state, 1);
}
static void
test_conversion_rejects_truncated_published_collection_key(void **state) {
  published_collection_truncation(*state, 2);
}

static void
test_conversion_recovers_unpublished_collection_tails(void **state) {
  fixture *f = *state;
  const char *modes[] = {"none", "zlib"};
  lc_collection_config config;
  lc_collection_read_opts options = {0};
  saved_collection_record saved;
  unsigned char tail[sizeof(saved.bytes)];
  lc_pouch_record_header pending;
  struct stat st;
  size_t length;
  unsigned int direction, encrypted, kind;
  int fd, attempt;
  lc_collection_config_init(&config);
  config.name = "unpublished";
  for (encrypted = 0; encrypted < 2; ++encrypted) {
    if (encrypted)
      check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    for (direction = 0; direction < 2; ++direction) {
      for (kind = 0; kind < 3; ++kind) {
        snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/tail-%u-%u-%u",
                 f->root, encrypted, direction, kind);
        check(f, open_client(f, modes[direction], &f->client));
        f->collection = create_event(f, &config);
        save_collection_record(f, &saved);
        lc_collection_close(f->collection);
        f->collection = NULL;
        close_client(f);
        memcpy(tail, saved.bytes, saved.length);
        pending = saved.header;
        pending.flags = LC_POUCH_RECORD_FLAG_PENDING;
        lc_pouch_record_header_encode(&pending, tail);
        length = kind == 0   ? 3
                 : kind == 1 ? LC_POUCH_RECORD_HEADER_BYTES
                             : saved.length - 1;
        /* First prove ordinary replay repairs this tail. Reinstall it before
         * conversion to verify the same recovery without losing the prefix. */
        for (attempt = 0; attempt < 2; ++attempt) {
          fd = open(saved.path, O_WRONLY | O_APPEND);
          assert_true(fd >= 0);
          assert_int_equal(write(fd, tail, length), length);
          assert_int_equal(fsync(fd), 0);
          close(fd);
          check(f, open_client(f, attempt == 0 ? NULL : modes[1 - direction],
                               &f->client));
          check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                             &f->error));
          expect_json(f, f->collection, &options, "{}\n", NULL);
          lc_collection_close(f->collection);
          f->collection = NULL;
          close_client(f);
          if (attempt == 0) {
            assert_int_equal(stat(saved.path, &st), 0);
            assert_int_equal(st.st_size, saved.length);
          }
        }
      }
    }
  }
}
typedef struct conversion_probe {
  fixture *fixture;
  char root[640], stage[768], catalog[1024], ns[768];
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  char query_id[LC_XID_STRING_SIZE];
  int fail_phase, fail_after, observed, installed, refs;
  lc_u64 max_payload;
} conversion_probe;

static void privacy_path(char *out, size_t capacity, const char *parent,
                         const char *leaf) {
  int n = snprintf(out, capacity, "%s/%s", parent, leaf);
  assert_true(n > 0 && (size_t)n < capacity);
}
static void expect_permissions(const char *path, mode_t mode) {
  struct stat st;
  assert_int_equal(stat(path, &st), 0);
  assert_int_equal(st.st_mode & 0777, mode);
}

static int observe_conversion(void *context, int phase, lc_error *error) {
  conversion_probe *p = context;
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  char path[1024];
  int payloads = 0;
  if (phase == LC_POUCH_RECODE_SEGMENT_INSTALLED)
    ++p->installed;
  if (phase == LC_POUCH_RECODE_REFS_INSTALLED)
    ++p->refs;
  privacy_path(path, sizeof(path), p->root, ".compression-new");
  assert_int_equal(lstat(path, &st), -1);
  assert_int_equal(errno, ENOENT);
  privacy_path(path, sizeof(path), p->root, ".compression-old");
  assert_int_equal(lstat(path, &st), -1);
  assert_int_equal(errno, ENOENT);
  if (phase != LC_POUCH_RECODE_READY && phase != LC_POUCH_RECODE_RECLAIMED) {
    expect_permissions(p->stage, 0700);
    expect_permissions(p->ns, 0700);
    expect_permissions(p->catalog, 0644);
    dir = opendir(p->stage);
    assert_non_null(dir);
    while ((entry = readdir(dir)) != NULL) {
      if (entry->d_name[0] == '.')
        continue;
      privacy_path(path, sizeof(path), p->stage, entry->d_name);
      assert_int_equal(lstat(path, &st), 0);
      /* There is no namespace copy and no duplicated directory hierarchy. */
      assert_true(S_ISREG(st.st_mode));
      if (!strcmp(entry->d_name, "payload")) {
        ++payloads;
        if ((lc_u64)st.st_size > p->max_payload)
          p->max_payload = (lc_u64)st.st_size;
      }
    }
    closedir(dir);
    assert_true(payloads <= 1);
  }
  if (phase == p->fail_phase && ++p->observed == p->fail_after)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                        "injected per-segment conversion failure", NULL, NULL,
                        "pouch");
  return LC_OK;
}

static void conversion_noise(unsigned char *bytes, size_t length) {
  uint32_t random = 123456789U;
  size_t i;
  for (i = 0; i < length; ++i) {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    bytes[i] = (unsigned char)random;
  }
}

static void write_conversion_noise(fixture *f) {
  unsigned char *bytes = malloc(65536);
  lc_source *source = NULL;
  lc_pouch_state_write_options options = {0};
  lc_pouch_state_write_result result = {0};
  assert_non_null(bytes);
  conversion_noise(bytes, 65536);
  check(f, lc_source_from_memory(bytes, 65536, &source, &f->error));
  check(f,
        lc_pouch_state_write(((lc_client_handle *)f->client)->pouch, "noise",
                             "random", source, &options, &result, &f->error));
  lc_pouch_state_write_result_cleanup(NULL, &result);
  lc_source_close(source);
  free(bytes);
}

static void read_conversion_noise(fixture *f) {
  unsigned char *expected = malloc(65536);
  lc_sink *sink = NULL;
  lc_pouch_state_read_result result = {0};
  const void *bytes;
  size_t length;
  assert_non_null(expected);
  conversion_noise(expected, 65536);
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_pouch_state_copy(((lc_client_handle *)f->client)->pouch, "noise",
                               "random", sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
  assert_true(result.found);
  assert_int_equal(length, 65536);
  assert_memory_equal(bytes, expected, length);
  lc_pouch_state_read_result_cleanup(NULL, &result);
  lc_sink_close(sink);
  free(expected);
}

static void seed_conversion(fixture *f, conversion_probe *p, const char *mode) {
  lc_collection_config config;
  lc_collection_metadata_req metadata = {0};
  lc_collection_append_res changed = {0};
  lc_collection_handle *handle;
  lc_collection_read_opts read = {0};
  memset(p, 0, sizeof(*p));
  p->fixture = f;
  p->fail_phase = -1;
  strcpy(p->root, f->endpoint + 8);
  privacy_path(p->stage, sizeof(p->stage), p->root, ".compression-work");
  privacy_path(p->ns, sizeof(p->ns), p->root, "namespaces/test");
  check(f, open_client(f, mode, &f->client));
  ((lc_client_handle *)f->client)->pouch->segment_target_bytes = 4096;
  write_state(f);
  compacted_write(f, "payload");
  write_conversion_noise(f);
  lc_collection_config_init(&config);
  config.name = "private-metadata";
  f->collection = create_event(f, &config);
  handle = f->collection->impl;
  strcpy(p->catalog, handle->catalog_path);
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 7776000;
  check(f, lc_collection_entry_metadata(f->collection, 1, &metadata, &changed,
                                        &f->error));
  expect_json(f, f->collection, &read, "{}\n", p->cursor);
  strcpy(p->query_id, handle->order.query_id);
  assert_int_equal(chmod(p->catalog, 0644), 0);
  assert_int_equal(chmod(p->ns, 0700), 0);
  lc_collection_close(f->collection);
  f->collection = NULL;
  close_client(f);
}

static void verify_conversion(fixture *f, conversion_probe *p,
                              const char *mode) {
  lc_collection_config config;
  lc_collection_read_opts options = {0};
  lc_pouch_maintenance_options maintenance = {0};
  lc_pouch_maintenance_result maintained = {0};
  lc_collection_append_status_res status = {0};
  char etag[65] = {0}, path[1024];
  lc_version version = 0;
  struct stat st;
  expect_mode(f, f->client, mode);
  read_state(f, etag, &version);
  expect_permissions(p->ns, 0700);
  expect_permissions(p->catalog, 0644);
  read_conversion_noise(f);
  lc_collection_config_init(&config);
  config.name = "private-metadata";
  check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                     &f->error));
  assert_string_equal(
      ((lc_collection_handle *)f->collection->impl)->order.query_id,
      p->query_id);
  expect_json(f, f->collection, &options, "{}\n", NULL);
  options.cursor = p->cursor;
  expect_json(f, f->collection, &options, "", NULL);
  check(f, lc_collection_append_status(f->collection, "event", &status,
                                       &f->error));
  lc_collection_close(f->collection);
  f->collection = NULL;
  maintenance.ns = "test";
  maintenance.force = 1;
  check(f, lc_pouch_maintenance_run(((lc_client_handle *)f->client)->pouch,
                                    &maintenance, &maintained, &f->error));
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
  read_state(f, etag, &version);
  privacy_path(path, sizeof(path), p->root, ".compression-ready");
  assert_int_equal(lstat(path, &st), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(lstat(p->stage, &st), -1);
  assert_int_equal(errno, ENOENT);
}

static void private_segment_conversion(fixture *f, int fault) {
  const char *modes[] = {"none", "zlib"};
  const int phases[] = {LC_POUCH_RECODE_COPIED, LC_POUCH_RECODE_SEGMENT_READY,
                        LC_POUCH_RECODE_SEGMENT_INSTALLED,
                        LC_POUCH_RECODE_REFS_READY, LC_POUCH_RECODE_HEAD};
  const mode_t masks[] = {0022, 0000};
  conversion_probe p;
  unsigned int mask, direction;
  f->saved_umask = umask(masks[0]);
  f->restore_umask = 1;
  for (mask = 0; mask < 2; ++mask) {
    umask(masks[mask]);
    for (direction = 0; direction < 2; ++direction) {
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/private-%u-%u",
               f->root, mask, direction);
      seed_conversion(f, &p, modes[direction]);
      p.fail_phase = fault ? phases[fault - 1] : -1;
      p.fail_after = 1;
      lc_pouch_recode_test_hook = observe_conversion;
      lc_pouch_recode_test_context = &p;
      if (fault) {
        assert_int_equal(open_client(f, modes[1 - direction], &f->client),
                         LC_ERR_TRANSPORT);
        assert_null(f->client);
        expect_permissions(p.stage, 0700);
        expect_permissions(p.ns, 0700);
        expect_permissions(p.catalog, 0644);
        lc_error_cleanup(&f->error);
        lc_error_init(&f->error);
        p.fail_phase = -1;
        check(f, open_client(f, NULL, &f->client));
      } else
        check(f, open_client(f, modes[1 - direction], &f->client));
      lc_pouch_recode_test_hook = NULL;
      lc_pouch_recode_test_context = NULL;
      /* The test has several segments but only one can be duplicated. The
       * state body is 64 KiB; include bounded codec and metadata overhead. */
      assert_true(p.max_payload < 131072);
      assert_true(p.installed > 2);
      assert_true(p.refs > 0);
      verify_conversion(f, &p, modes[1 - direction]);
      close_client(f);
    }
  }
}
static void test_conversion_keeps_private_segment_staging(void **state) {
  private_segment_conversion(*state, 0);
}
static void test_inventory_failure_resumes_with_private_journal(void **state) {
  private_segment_conversion(*state, 1);
}
static void test_prepared_segment_failure_resumes(void **state) {
  private_segment_conversion(*state, 2);
}
static void test_installed_segment_failure_resumes(void **state) {
  private_segment_conversion(*state, 3);
}
static void test_reference_failure_resumes(void **state) {
  private_segment_conversion(*state, 4);
}
static void test_head_publication_failure_resumes(void **state) {
  private_segment_conversion(*state, 5);
}

static void
test_segment_crashes_resume_encrypted_both_directions(void **state) {
  fixture *f = *state;
  const char *modes[] = {"none", "zlib"};
  const int phases[] = {LC_POUCH_RECODE_SEGMENT_READY,
                        LC_POUCH_RECODE_SEGMENT_RENAMED,
                        LC_POUCH_RECODE_SEGMENT_INSTALLED,
                        LC_POUCH_RECODE_REFS_READY,
                        LC_POUCH_RECODE_REFS_RENAMED,
                        LC_POUCH_RECODE_REFS_INSTALLED,
                        LC_POUCH_RECODE_HEAD_WRITTEN,
                        LC_POUCH_RECODE_ORDER_REBUILT,
                        LC_POUCH_RECODE_HEAD};
  conversion_probe p;
  unsigned int encrypted, direction, i;
  int status, phase;
  pid_t child;
  for (encrypted = 0; encrypted < 2; ++encrypted) {
    if (encrypted)
      check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    for (direction = 0; direction < 2; ++direction) {
      for (i = 0; i < sizeof(phases) / sizeof(phases[0]); ++i) {
        snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/crash-%u-%u-%u",
                 f->root, encrypted, direction, i);
        seed_conversion(f, &p, modes[direction]);
        phase = phases[i];
        child = fork();
        assert_true(child >= 0);
        if (child == 0) {
          lc_pouch_recode_test_hook = crash_at;
          lc_pouch_recode_test_context = &phase;
          (void)open_client(f, modes[1 - direction], &f->client);
          _exit(42);
        }
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 41);
        check(f, open_client(f, NULL, &f->client));
        verify_conversion(f, &p, modes[1 - direction]);
        close_client(f);
      }
    }
  }
}

static void test_repeated_failures_preserve_completed_segments(void **state) {
  fixture *f = *state;
  conversion_probe p;
  unsigned char bytes[8];
  lc_u64 total = 0;
  unsigned int i;
  char path[1024];
  FILE *fp;
  seed_conversion(f, &p, "none");
  p.fail_phase = LC_POUCH_RECODE_SEGMENT_INSTALLED;
  p.fail_after = 1;
  lc_pouch_recode_test_hook = observe_conversion;
  lc_pouch_recode_test_context = &p;
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_TRANSPORT);
  assert_null(f->client);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  p.observed = 0;
  assert_int_equal(open_client(f, NULL, &f->client), LC_ERR_TRANSPORT);
  assert_null(f->client);
  assert_int_equal(p.installed, 2);
  privacy_path(path, sizeof(path), p.stage, "plan");
  fp = fopen(path, "rb");
  assert_non_null(fp);
  assert_int_equal(fread(bytes, 1, sizeof(bytes), fp), sizeof(bytes));
  fclose(fp);
  for (i = 0; i < 8; ++i)
    total = (total << 8) | bytes[i];
  assert_true(total > 2);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  p.fail_phase = -1;
  check(f, open_client(f, NULL, &f->client));
  assert_int_equal(p.installed, total);
  lc_pouch_recode_test_hook = NULL;
  lc_pouch_recode_test_context = NULL;
  verify_conversion(f, &p, "zlib");
}

static void test_partial_output_write_failure_resumes(void **state) {
  fixture *f = *state;
  const char *modes[] = {"none", "zlib"};
  conversion_probe p;
  struct rlimit limit;
  struct stat st;
  char path[1024];
  unsigned int direction;
  int status, rc;
  pid_t child;
  for (direction = 0; direction < 2; ++direction) {
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/short-%u", f->root,
             direction);
    seed_conversion(f, &p, modes[direction]);
    child = fork();
    assert_true(child >= 0);
    if (child == 0) {
      if (getrlimit(RLIMIT_FSIZE, &limit) != 0)
        _exit(43);
      limit.rlim_cur = 16384;
      if (setrlimit(RLIMIT_FSIZE, &limit) != 0 ||
          signal(SIGXFSZ, SIG_IGN) == SIG_ERR)
        _exit(44);
      rc = open_client(f, modes[1 - direction], &f->client);
      _exit(rc == LC_ERR_TRANSPORT && f->client == NULL ? 41 : 42);
    }
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 41);
    privacy_path(path, sizeof(path), p.stage, "payload");
    assert_int_equal(stat(path, &st), 0);
    assert_true(st.st_size > 0 && st.st_size <= 16384);
    check(f, open_client(f, NULL, &f->client));
    verify_conversion(f, &p, modes[1 - direction]);
    close_client(f);
  }
}

static void test_opposite_request_finishes_pending_cutover_first(void **state) {
  fixture *f = *state;
  conversion_probe p;
  seed_conversion(f, &p, "none");
  p.fail_phase = LC_POUCH_RECODE_SEGMENT_INSTALLED;
  p.fail_after = 1;
  lc_pouch_recode_test_hook = observe_conversion;
  lc_pouch_recode_test_context = &p;
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_TRANSPORT);
  assert_null(f->client);
  lc_pouch_recode_test_hook = NULL;
  lc_pouch_recode_test_context = NULL;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, open_client(f, "none", &f->client));
  verify_conversion(f, &p, "none");
}

static void test_inventory_resume_reports_allocation_failure(void **state) {
  fixture *f = *state;
  conversion_probe p;
  unsigned char bytes[8];
  lc_u64 length = 0;
  char path[1024];
  FILE *fp;
  unsigned int i;
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/allocation-resume",
           f->root);
  seed_conversion(f, &p, "none");
  p.fail_phase = LC_POUCH_RECODE_COPIED;
  p.fail_after = 1;
  lc_pouch_recode_test_hook = observe_conversion;
  lc_pouch_recode_test_context = &p;
  assert_int_equal(open_client(f, "zlib", &f->client), LC_ERR_TRANSPORT);
  lc_pouch_recode_test_hook = NULL;
  lc_pouch_recode_test_context = NULL;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  privacy_path(path, sizeof(path), p.stage, "plan");
  fp = fopen(path, "rb");
  assert_non_null(fp);
  assert_int_equal(fseek(fp, 8, SEEK_SET), 0);
  assert_int_equal(fread(bytes, 1, sizeof(bytes), fp), sizeof(bytes));
  fclose(fp);
  for (i = 0; i < 8; ++i)
    length = (length << 8) | bytes[i];
  f->custom_allocator = 1;
  f->fail_allocation_size = (size_t)length + 1;
  assert_int_equal(open_client(f, NULL, &f->client), LC_ERR_NOMEM);
  assert_null(f->client);
  assert_int_equal(f->fail_allocation_size, 0);
  assert_string_equal(f->error.message,
                      "failed to resume Pouch compression conversion");
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  f->custom_allocator = 0;
  check(f, open_client(f, NULL, &f->client));
  verify_conversion(f, &p, "zlib");
}

static void test_conversion_reclaims_interrupted_index_builds(void **state) {
  fixture *f = *state;
  conversion_probe p;
  lc_collection_config config;
  char paths[2][1024];
  const char *leaves[] = {"order.rebuild-interrupted",
                          "ids.rebuild-interrupted"};
  struct stat st;
  unsigned int i;
  int fd;
  seed_conversion(f, &p, "none");
  check(f, open_client(f, NULL, &f->client));
  lc_collection_config_init(&config);
  config.name = "private-metadata";
  check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                     &f->error));
  for (i = 0; i < 2; ++i)
    privacy_path(paths[i], sizeof(paths[i]),
                 ((lc_collection_handle *)f->collection->impl)->directory,
                 leaves[i]);
  lc_collection_close(f->collection);
  f->collection = NULL;
  close_client(f);
  for (i = 0; i < 2; ++i) {
    fd = open(paths[i], O_WRONLY | O_CREAT | O_EXCL, 0600);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 1048576), 0);
    assert_int_equal(fsync(fd), 0);
    close(fd);
  }
  check(f, open_client(f, "zlib", &f->client));
  for (i = 0; i < 2; ++i) {
    assert_int_equal(stat(paths[i], &st), -1);
    assert_int_equal(errno, ENOENT);
  }
  verify_conversion(f, &p, "zlib");
}

static void test_conversion_preserves_readonly_sealed_segments(void **state) {
  fixture *f = *state;
  conversion_probe p;
  lc_pouch_namespace_manifest manifest = {0};
  char path[1024];
  unsigned long i;
  seed_conversion(f, &p, "none");
  check(f, open_client(f, NULL, &f->client));
  write_state(f);
  check(f, lc_pouch_namespace_manifest_open(NULL, p.root, "test", &manifest,
                                            NULL, NULL, &f->error));
  close_client(f);
  assert_true(manifest.segment_count > 2);
  for (i = 0; i < manifest.segment_count; ++i) {
    if (!strcmp(manifest.segment_leaves[i], manifest.active_segment))
      continue;
    snprintf(path, sizeof(path), "%s/segments/%s", manifest.namespace_path,
             manifest.segment_leaves[i]);
    assert_int_equal(chmod(path, 0440), 0);
  }
  check(f, open_client(f, "zlib", &f->client));
  for (i = 0; i < manifest.segment_count; ++i) {
    if (!strcmp(manifest.segment_leaves[i], manifest.active_segment))
      continue;
    snprintf(path, sizeof(path), "%s/segments/%s", manifest.namespace_path,
             manifest.segment_leaves[i]);
    expect_permissions(path, 0440);
  }
  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  verify_conversion(f, &p, "zlib");
}

/* A small crash-image fixture, not a filesystem emulator. Restore only the
 * targeted bytes/dentries that were not covered by a successful real fsync.
 * Unrelated durable files and journal entries deliberately remain intact. */
enum {
  POWER_BODY,
  POWER_REFS,
  POWER_RENAME_UNDONE,
  POWER_RENAME_ALIAS,
  POWER_SYNC_ERROR
};
typedef struct power_watch {
  char directory[1024], payload[1024], image[1024], original[1024];
  char source[1024], source_directory[1024];
  int mode, output, syncs, refs_started, renamed, destination_synced;
  unsigned long files, installed;
  dev_t device;
  ino_t inode;
} power_watch;
static power_watch *power_sync_watch;
typedef struct receipt_watch {
  char directory[1024], path[1200];
  const char *suffix;
  int retry, fail_retry, durable, renamed, output;
} receipt_watch;
static receipt_watch *receipt_sync_watch;
int __real_fsync(int fd);

static void power_cut(power_watch *watch, int rc) {
  int report[3];
  report[0] = watch->mode;
  report[1] = watch->syncs;
  report[2] = rc;
  if (write(watch->output, report, sizeof(report)) != sizeof(report))
    _exit(43);
  if (write(watch->output, watch->source, sizeof(watch->source)) !=
      sizeof(watch->source))
    _exit(43);
  _exit(41);
}
static void power_copy_fd(int in, int out) {
  unsigned char bytes[2048];
  off_t offset = 0;
  ssize_t got, written;
  size_t done;
  for (;;) {
    got = pread(in, bytes, sizeof(bytes), offset);
    assert_true(got >= 0);
    if (got == 0)
      break;
    done = 0;
    while (done < (size_t)got) {
      written = write(out, bytes + done, (size_t)got - done);
      assert_true(written > 0);
      done += (size_t)written;
    }
    offset += got;
  }
  assert_int_equal(ftruncate(out, offset), 0);
  assert_int_equal(__real_fsync(out), 0);
}
static int power_same_file(int fd, const char *path) {
  struct stat a, b;
  return fstat(fd, &a) == 0 && stat(path, &b) == 0 && a.st_dev == b.st_dev &&
         a.st_ino == b.st_ino;
}
static int receipt_sync(int fd, receipt_watch *w) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  const char *suffix;
  int rc;
  if (power_same_file(fd, w->directory)) {
    if (!w->retry && !w->path[0]) {
      dir = opendir(w->directory);
      assert_non_null(dir);
      while ((entry = readdir(dir)) != NULL) {
        suffix = strrchr(entry->d_name, '.');
        if (suffix && !strcmp(suffix, w->suffix)) {
          privacy_path(w->path, sizeof(w->path), w->directory, entry->d_name);
          break;
        }
      }
      closedir(dir);
    }
    /* The receipt's contents were synced, but its published name was not.
     * Keep it visible for a warm retry after the reported I/O error. */
    if ((!w->retry && w->path[0]) || (w->retry && w->fail_retry)) {
      errno = EIO;
      return -1;
    }
    rc = __real_fsync(fd);
    if (rc == 0 && w->retry)
      w->durable = 1;
    return rc;
  }
  rc = __real_fsync(fd);
  if (rc == 0 && w->retry && !w->fail_retry && w->renamed &&
      fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
    /* Cut power after the canonical directory is durable, before the next
     * journal sync. The parent drops only a still-unsynced receipt name. */
    if (write(w->output, &w->durable, sizeof(w->durable)) != sizeof(w->durable))
      _exit(43);
    _exit(41);
  }
  return rc;
}
int __wrap_fsync(int fd) {
  power_watch *w = power_sync_watch;
  struct stat st;
  int rc, image, input;
  if (receipt_sync_watch)
    return receipt_sync(fd, receipt_sync_watch);
  if (!w)
    return __real_fsync(fd);
  if (w->renamed && w->mode == POWER_RENAME_ALIAS) {
    if (power_same_file(fd, w->directory)) {
      assert_true(w->destination_synced);
      power_cut(w, LC_OK);
    }
    rc = __real_fsync(fd);
    if (rc == 0 && power_same_file(fd, w->source_directory))
      w->destination_synced = 1;
    return rc;
  }
  if (w->mode > POWER_REFS && w->mode != POWER_SYNC_ERROR)
    return __real_fsync(fd);
  if (!power_same_file(fd, w->payload) || fstat(fd, &st) != 0)
    return __real_fsync(fd);
  if (w->mode == POWER_BODY || w->mode == POWER_SYNC_ERROR) {
    if (st.st_size < 16384)
      return __real_fsync(fd);
    if (w->mode == POWER_BODY)
      power_cut(w, LC_OK); /* This payload has no successful sync yet. */
    errno = EIO;
    return -1;
  }
  if (!w->refs_started)
    return __real_fsync(fd);
  if (w->device != st.st_dev || w->inode != st.st_ino) {
    w->device = st.st_dev;
    w->inode = st.st_ino;
    w->syncs = 0;
  }
  if (w->syncs != 0)
    power_cut(w, LC_OK); /* Lose the reference patches, keep the synced copy. */
  rc = __real_fsync(fd);
  if (rc == 0) {
    ++w->syncs;
    image = open(w->image, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    input = open(w->payload, O_RDONLY);
    assert_true(image >= 0 && input >= 0);
    power_copy_fd(input, image);
    close(input);
    close(image);
  }
  return rc;
}
static int power_phase(void *context, int phase, lc_error *error) {
  power_watch *w = context;
  unsigned char bytes[8];
  FILE *fp;
  char path[1200];
  char relative[1024];
  lc_u64 length = 0;
  unsigned int i;
  (void)error;
  if (phase == LC_POUCH_RECODE_COPIED) {
    snprintf(path, sizeof(path), "%s/plan", w->directory);
    fp = fopen(path, "rb");
    assert_non_null(fp);
    assert_int_equal(fread(bytes, 1, sizeof(bytes), fp), sizeof(bytes));
    for (i = 0; i < 8; ++i)
      w->files = (w->files << 8) | bytes[i];
    assert_int_equal(fread(bytes, 1, sizeof(bytes), fp), sizeof(bytes));
    for (i = 0; i < 8; ++i)
      length = (length << 8) | bytes[i];
    assert_true(length < sizeof(relative));
    assert_int_equal(fread(relative, 1, (size_t)length, fp), length);
    relative[length] = 0;
    fclose(fp);
    privacy_path(w->source, sizeof(w->source), w->source_directory, relative);
    strcpy(w->source_directory, w->source);
    *strrchr(w->source_directory, '/') = 0;
  }
  if (phase == LC_POUCH_RECODE_SEGMENT_INSTALLED && ++w->installed == w->files)
    w->refs_started = 1;
  if (w->mode != POWER_RENAME_UNDONE && w->mode != POWER_RENAME_ALIAS)
    return LC_OK;
  if (phase == LC_POUCH_RECODE_SEGMENT_READY && w->installed == 0)
    assert_int_equal(link(w->source, w->original), 0);
  if (phase == LC_POUCH_RECODE_SEGMENT_RENAMED && w->installed == 0) {
    w->renamed = 1;
    if (w->mode == POWER_RENAME_UNDONE)
      power_cut(w, LC_OK);
  }
  return LC_OK;
}
static void exercise_conversion_power_loss(fixture *f, int mode) {
  const char *modes[] = {"none", "zlib"};
  conversion_probe *p = calloc(1, sizeof(*p));
  power_watch *w = calloc(1, sizeof(*w));
  unsigned int direction, encrypted, torn;
  int pipefd[2], report[3], status, in, out, rc;
  pid_t child;
  assert_non_null(w);
  assert_non_null(p);
  for (encrypted = 0; encrypted < 2; ++encrypted) {
    if (encrypted)
      check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    for (direction = 0; direction < 2; ++direction) {
      for (torn = 0; torn < (mode == POWER_BODY ? 2U : 1U); ++torn) {
        memset(w, 0, sizeof(*w));
        snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/power-%u-%u-%u",
                 f->root, encrypted, direction, torn);
        seed_conversion(f, p, modes[direction]);
        strcpy(w->directory, p->stage);
        privacy_path(w->payload, sizeof(w->payload), p->stage, "payload");
        privacy_path(w->image, sizeof(w->image), p->root,
                     ".test-durable-image");
        privacy_path(w->original, sizeof(w->original), p->root,
                     ".test-original");
        privacy_path(w->source_directory, sizeof(w->source_directory), p->root,
                     "namespaces");
        assert_int_equal(pipe(pipefd), 0);
        w->mode = mode;
        w->output = pipefd[1];
        child = fork();
        assert_true(child >= 0);
        if (child == 0) {
          close(pipefd[0]);
          power_sync_watch = w;
          lc_pouch_recode_test_hook = power_phase;
          lc_pouch_recode_test_context = w;
          rc = open_client(f, modes[1 - direction], &f->client);
          if (mode == POWER_SYNC_ERROR)
            power_cut(w, rc);
          _exit(42);
        }
        close(pipefd[1]);
        assert_int_equal(read(pipefd[0], report, sizeof(report)),
                         sizeof(report));
        assert_int_equal(read(pipefd[0], w->source, sizeof(w->source)),
                         sizeof(w->source));
        close(pipefd[0]);
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 41);
        assert_int_equal(report[0], mode);
        assert_int_equal(report[2],
                         mode == POWER_SYNC_ERROR ? LC_ERR_TRANSPORT : LC_OK);
        if (mode == POWER_BODY || mode == POWER_SYNC_ERROR) {
          out = open(w->payload, O_WRONLY);
          assert_true(out >= 0);
          assert_int_equal(ftruncate(out, torn ? 7 : 0), 0);
          assert_int_equal(__real_fsync(out), 0);
          close(out);
        } else if (mode == POWER_REFS) {
          assert_int_equal(report[1], 1);
          in = open(w->image, O_RDONLY);
          out = open(w->payload, O_WRONLY | O_TRUNC);
          assert_true(in >= 0 && out >= 0);
          power_copy_fd(in, out);
          close(in);
          close(out);
          assert_int_equal(unlink(w->image), 0);
        } else if (mode == POWER_RENAME_UNDONE) {
          /* Neither directory sync completed: restore the durable old name
           * and staged replacement, retaining their recorded inode identities.
           */
          assert_int_equal(rename(w->source, w->payload), 0);
          assert_int_equal(rename(w->original, w->source), 0);
        } else {
          /* Destination synced, staging directory did not: its old payload
           * name can survive as a second link to the installed replacement. */
          assert_int_equal(link(w->source, w->payload), 0);
          assert_int_equal(unlink(w->original), 0);
        }
        check(f, open_client(f, NULL, &f->client));
        verify_conversion(f, p, modes[1 - direction]);
        close_client(f);
      }
    }
    if (encrypted) {
      lc_pouch_crypto_key_string_free(f->key);
      f->key = NULL;
    }
  }
  free(w);
  free(p);
}
static void test_conversion_recovers_lost_unsynced_payload_bytes(void **state) {
  exercise_conversion_power_loss(*state, POWER_BODY);
}
static void
test_conversion_recovers_lost_unsynced_reference_patches(void **state) {
  exercise_conversion_power_loss(*state, POWER_REFS);
}
static void test_conversion_recovers_unsynced_rename_rollback(void **state) {
  exercise_conversion_power_loss(*state, POWER_RENAME_UNDONE);
}
static void
test_conversion_recovers_unsynced_staging_directory_alias(void **state) {
  exercise_conversion_power_loss(*state, POWER_RENAME_ALIAS);
}
static void test_conversion_failed_payload_sync_never_publishes(void **state) {
  exercise_conversion_power_loss(*state, POWER_SYNC_ERROR);
}

static int receipt_phase(void *context, int phase, lc_error *error) {
  receipt_watch *w = context;
  int expected = !strcmp(w->suffix, ".map") ? LC_POUCH_RECODE_SEGMENT_RENAMED
                                            : LC_POUCH_RECODE_REFS_RENAMED;
  (void)error;
  if (phase == expected)
    w->renamed = 1;
  return LC_OK;
}

static int64_t receipt_entries(fixture *f, int verify) {
  static const char first[] =
      "{\"padding\":"
      "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}";
  lc_collection_config config;
  lc_collection_append_req request;
  lc_collection_append_res result = {0};
  lc_collection_metadata_req metadata = {0};
  lc_collection_append_status_res status = {0};
  lc_collection_read_opts options = {0};
  lc_source *source = NULL;
  char expected[256];
  lc_collection_config_init(&config);
  config.name = "private-metadata";
  if (verify) {
    check(f, lc_client_open_collection(f->client, &config, &f->collection,
                                       &f->error));
    snprintf(expected, sizeof(expected), "%s\n{\"n\":2}\n", first);
    expect_json(f, f->collection, &options, expected, NULL);
    check(f, lc_collection_append_status(f->collection, "second-event", &status,
                                         &f->error));
    assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
    assert_int_equal(status.entry.position, 2);
    assert_true(status.entry.retention_expires_at_unix > 0);
  } else {
    check(f, lc_client_create_collection(f->client, &config, &f->collection,
                                         &f->error));
    lc_collection_append_req_init(&request);
    request.append_id = "first-event";
    check(f,
          lc_source_from_memory(first, sizeof(first) - 1, &source, &f->error));
    check(f, lc_collection_append(f->collection, &request, source, &result,
                                  &f->error));
    lc_source_close(source);
    lc_collection_append_req_init(&request);
    request.append_id = "second-event";
    check(f, lc_source_from_memory("{\"n\":2}", 7, &source, &f->error));
    check(f, lc_collection_append(f->collection, &request, source, &result,
                                  &f->error));
    lc_source_close(source);
    assert_int_equal(result.position, 2);
    metadata.has_retention_seconds = 1;
    metadata.retention_seconds = 7776000;
    check(f, lc_collection_entry_metadata(f->collection, 2, &metadata, &result,
                                          &f->error));
  }
  lc_collection_close(f->collection);
  f->collection = NULL;
  return verify ? status.entry.retention_expires_at_unix
                : result.retention_expires_at_unix;
}

static void exercise_recovered_receipt(fixture *f, const char *suffix,
                                       int fail_retry) {
  const char *modes[] = {"none", "zlib"};
  receipt_watch *w = calloc(1, sizeof(*w));
  char payload[1024];
  struct stat before, after;
  int64_t retention;
  unsigned int encrypted, direction;
  int rc, pipefd[2], durable, status;
  pid_t child;
  assert_non_null(w);
  for (encrypted = 0; encrypted < 2; ++encrypted) {
    if (encrypted)
      check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    for (direction = 0; direction < 2; ++direction) {
      memset(w, 0, sizeof(*w));
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/receipt-%u-%u",
               f->root, encrypted, direction);
      /* Keep the referenced body in the first converted file, after a body
       * whose compression changes its offset. Earlier installed maps would
       * incidentally sync the journal and hide this recovery ordering gap. */
      check(f, open_client(f, modes[direction], &f->client));
      retention = receipt_entries(f, 0);
      close_client(f);
      privacy_path(w->directory, sizeof(w->directory), f->endpoint + 8,
                   ".compression-work");
      w->suffix = suffix;
      receipt_sync_watch = w;
      rc = open_client(f, modes[1 - direction], &f->client);
      receipt_sync_watch = NULL;
      assert_int_equal(rc, LC_ERR_TRANSPORT);
      assert_null(f->client);
      assert_true(w->path[0] != 0);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      privacy_path(payload, sizeof(payload), w->directory, "payload");
      assert_int_equal(stat(payload, &before), 0);
      w->retry = 1;
      w->fail_retry = fail_retry;
      if (fail_retry) {
        receipt_sync_watch = w;
        lc_pouch_recode_test_hook = receipt_phase;
        lc_pouch_recode_test_context = w;
        rc = open_client(f, NULL, &f->client);
        receipt_sync_watch = NULL;
        lc_pouch_recode_test_hook = NULL;
        lc_pouch_recode_test_context = NULL;
        assert_int_equal(rc, LC_ERR_TRANSPORT);
        assert_null(f->client);
        /* Failure to establish receipt durability must leave the prepared
         * replacement untouched and available for the next recovery. */
        assert_int_equal(stat(payload, &after), 0);
        assert_int_equal(after.st_ino, before.st_ino);
        assert_int_equal(w->renamed, 0);
        lc_error_cleanup(&f->error);
        lc_error_init(&f->error);
      } else {
        assert_int_equal(pipe(pipefd), 0);
        w->output = pipefd[1];
        child = fork();
        assert_true(child >= 0);
        if (child == 0) {
          close(pipefd[0]);
          receipt_sync_watch = w;
          lc_pouch_recode_test_hook = receipt_phase;
          lc_pouch_recode_test_context = w;
          (void)open_client(f, NULL, &f->client);
          _exit(42);
        }
        close(pipefd[1]);
        assert_int_equal(read(pipefd[0], &durable, sizeof(durable)),
                         sizeof(durable));
        close(pipefd[0]);
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 41);
        if (!durable)
          assert_int_equal(unlink(w->path), 0);
      }
      check(f, open_client(f, NULL, &f->client));
      expect_mode(f, f->client, modes[1 - direction]);
      assert_int_equal(receipt_entries(f, 1), retention);
      if (!fail_retry)
        assert_int_equal(durable, 1);
      close_client(f);
    }
    if (encrypted) {
      lc_pouch_crypto_key_string_free(f->key);
      f->key = NULL;
    }
  }
  free(w);
}

static void test_recovered_offset_map_survives_second_power_loss(void **state) {
  exercise_recovered_receipt(*state, ".map", 0);
}
static void
test_recovered_reference_receipt_survives_second_power_loss(void **state) {
  exercise_recovered_receipt(*state, ".refs", 0);
}
static void
test_recovered_offset_map_sync_failure_preserves_source(void **state) {
  exercise_recovered_receipt(*state, ".map", 1);
}
static void
test_recovered_reference_sync_failure_preserves_source(void **state) {
  exercise_recovered_receipt(*state, ".refs", 1);
}

int main(int argc, char **argv) {
  const struct CMUnitTest cases[] = {
      cmocka_unit_test_setup_teardown(
          test_omission_inherits_and_explicit_changes_empty_root, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_both_directions_preserve_shared_payloads, setup, teardown),
      cmocka_unit_test_setup_teardown(test_encrypted_both_directions, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_active_writer_refuses_conversion_and_adopts_root, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_publication_crashes_resume_without_explicit_option, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_collections_cursors_compaction_and_retry_bindings, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_build_keeps_source_and_retry_succeeds, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_log_levels_and_no_work_on_inherited_open, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_staged_payload_survives_both_directions_and_promotes, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_process_writer_refuses_conversion,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_streamed_large_retained_history_preserves_sequences, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_corruption_fails_before_publication_and_preserves_mode, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compacted_snapshot_preserves_payload_links, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_allocation_failure_keeps_source_and_retries, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_does_not_block_unrelated_root, setup, teardown),
      cmocka_unit_test_setup_teardown(test_wrong_key_cannot_start_conversion,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_empty_collection_torn_tail_and_storage_modes, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_skips_unpublished_collection_partition, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_skips_partly_reclaimed_replaced_partition, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_bound_collection_without_head, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_bound_collection_corrupt_head, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_bound_partition_symlink, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_collection_data_symlink, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_omits_obsolete_segment_references, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_encrypted_conversion_omits_obsolete_segment_references, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_encrypted_collection_metadata_reads_after_conversion, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_encrypted_collection_metadata_compacts_after_conversion, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_truncated_published_collection_payload, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_truncated_published_collection_metadata,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_rejects_truncated_published_collection_key, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_recovers_unpublished_collection_tails, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_keeps_private_segment_staging, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_inventory_failure_resumes_with_private_journal, setup, teardown),
      cmocka_unit_test_setup_teardown(test_prepared_segment_failure_resumes,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_installed_segment_failure_resumes,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_reference_failure_resumes, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_head_publication_failure_resumes,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_segment_crashes_resume_encrypted_both_directions, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_repeated_failures_preserve_completed_segments, setup, teardown),
      cmocka_unit_test_setup_teardown(test_partial_output_write_failure_resumes,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_opposite_request_finishes_pending_cutover_first, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_inventory_resume_reports_allocation_failure, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_reclaims_interrupted_index_builds, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_preserves_readonly_sealed_segments, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_recovers_lost_unsynced_payload_bytes, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_recovers_lost_unsynced_reference_patches, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_recovers_unsynced_rename_rollback, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_recovers_unsynced_staging_directory_alias, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_conversion_failed_payload_sync_never_publishes, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_recovered_offset_map_survives_second_power_loss, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_recovered_reference_receipt_survives_second_power_loss, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_recovered_offset_map_sync_failure_preserves_source, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_recovered_reference_sync_failure_preserves_source, setup,
          teardown)};
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 2;
  return lc_test_run_cases(cases, sizeof(cases) / sizeof(cases[0]), argc, argv,
                           NULL, NULL);
}
