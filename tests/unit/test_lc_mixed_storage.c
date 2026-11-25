#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"
#include "../support/lc_test_tmp.h"
#include "lc_collection_internal.h"
#include "lc_collection_query.h"
#include "lc_pouch_namespace.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/mixed-storage-"
#define WRITERS 3
#define ROUNDS 3
#define PER_ROUND 24

typedef struct fixture {
  char root[512], endpoint[640];
  lc_client *client;
  lc_collection *collection;
  lc_error error;
  lc_pouch_unix_seconds now;
  unsigned int sync_calls;
  int state_failure;
  int background;
} fixture;

static fixture *sync_fixture;

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("mixed storage: %d: %s [%s]\n", rc,
                f->error.message ? f->error.message : "",
                f->error.server_code ? f->error.server_code : "");
  assert_int_equal(rc, LC_OK);
}

static lc_pouch *pouch(fixture *f) {
  return ((lc_client_handle *)f->client)->pouch;
}

static void close_client(fixture *f) {
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_client_close(f->client);
  f->client = NULL;
}

static void open_client(fixture *f, int create, int durable, uint64_t segment,
                        int indexed) {
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config collection = {0};
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_DURABLE_SYNC |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = 0;
  settings.durable_sync = durable;
  settings.segment_target_bytes = segment;
  settings.query_indexing_enabled = indexed;
  settings.background_compaction_enabled = f->background;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "mixed";
  config.pouch_settings = &settings;
  check(f, lc_client_open(&config, &f->client, &f->error));
  collection.name = "events";
  check(f, create ? lc_client_create_collection(f->client, &collection,
                                                &f->collection, &f->error)
                  : lc_client_open_collection(f->client, &collection,
                                              &f->collection, &f->error));
  pouch(f)->compaction_delete_grace_seconds = 0;
}

static int setup(void **state) {
  char template_path[] = PREFIX "XXXXXX";
  fixture *f = calloc(1, sizeof(*f));
  assert_non_null(f);
  lc_error_init(&f->error);
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  *state = f;
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_compact_test_hook = NULL;
  lc_collection_compact_test_context = NULL;
  lc_collection_commit_wait_test_hook = NULL;
  lc_collection_commit_wait_test_context = NULL;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  sync_fixture = NULL;
  close_client(f);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static int write_state_in(fixture *f, const char *ns, const char *key,
                          const char *body,
                          const lc_pouch_state_write_options *options) {
  lc_pouch_state_write_result result = {0};
  lc_source *source = NULL;
  int rc = lc_source_from_memory(body, strlen(body), &source, &f->error);
  if (rc == LC_OK)
    rc = lc_pouch_state_write(pouch(f), ns, key, source, options, &result,
                              &f->error);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &result);
  return rc;
}

static int write_state(fixture *f, const char *key, const char *body,
                       const lc_pouch_state_write_options *options) {
  return write_state_in(f, "mixed", key, body, options);
}

static lc_pouch_generation assert_state(fixture *f, const char *key,
                                        const char *expected) {
  lc_pouch_state_read_result result = {0};
  char body[512];
  size_t used = 0, got;
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, lc_pouch_state_read(pouch(f), "mixed", key, &result, &f->error));
  assert_int_equal(result.found, expected != NULL);
  lc_pouch_generation version = result.version;
  if (expected != NULL) {
    assert_non_null(result.body);
    while ((got = result.body->read(result.body, body + used,
                                    sizeof(body) - used, &f->error)) != 0) {
      used += got;
      assert_true(used < sizeof(body));
    }
    check(f, f->error.code);
    assert_int_equal(used, strlen(expected));
    assert_memory_equal(body, expected, used);
  }
  lc_pouch_state_read_result_cleanup(NULL, &result);
  return version;
}

static int append(fixture *f, const char *id, const char *body, long retention,
                  lc_collection_append_res *result) {
  lc_source *source = NULL;
  lc_collection_append_req request = {0};
  request.append_id = id;
  request.has_retention_seconds = retention >= 0;
  request.retention_seconds = retention;
  int rc = lc_source_from_memory(body, strlen(body), &source, &f->error);
  if (rc == LC_OK)
    rc = lc_collection_append(f->collection, &request, source, result,
                              &f->error);
  lc_source_close(source);
  return rc;
}

static int ordered(void *context, const lc_collection_read_metadata *entry,
                   lc_error *error) {
  uint64_t *last = context;
  if (entry->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    if (entry->position <= *last)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                          "collection positions are not increasing", NULL, NULL,
                          NULL);
    *last = entry->position;
  }
  return LC_OK;
}

static void assert_collection(fixture *f, const char *selector, uint64_t count,
                              uint64_t upper) {
  lc_collection_read_opts options = {0};
  lc_collection_read_res result = {0};
  lc_sink *sink = NULL;
  uint64_t last = 0;
  options.selector_lql = selector;
  options.on_metadata = ordered;
  options.metadata_context = &last;
  check(f, lc_sink_to_discard(&sink, &f->error));
  check(f,
        lc_collection_read(f->collection, &options, sink, &result, &f->error));
  assert_true(result.complete);
  assert_int_equal(result.completed_records, count);
  assert_int_equal(result.upper_position, upper);
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
}

static void assert_read_bytes(fixture *f, lc_collection *collection,
                              const lc_collection_read_opts *options,
                              const char *expected, uint64_t upper,
                              uint64_t boundary, char *checkpoint) {
  lc_collection_read_res result = {0};
  lc_sink *sink = NULL;
  const void *bytes;
  size_t length;
  uint64_t records = 0;
  for (const char *p = expected; *p; ++p)
    records += *p == '\n';
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(collection, options, sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
  assert_int_equal(length, strlen(expected));
  if (length)
    assert_memory_equal(bytes, expected, length);
  assert_true(result.complete);
  assert_int_equal(result.completed_records, records);
  assert_int_equal(result.upper_position, upper);
  assert_int_equal(result.last_examined_position, boundary);
  assert_null(result.resume_cursor);
  assert_non_null(result.checkpoint_cursor);
  if (checkpoint)
    strcpy(checkpoint, result.checkpoint_cursor);
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
}

typedef struct selection_window {
  uint64_t limit, inputs, boundary, upper;
  int exact;
  char bytes[(LC_COLLECTION_ORDER_PAGE_ROWS + 3) * 128];
  size_t length;
} selection_window;

/* Independent oracle: read all live metadata within the bounds, then count
 * inputs and filter the known fixture IDs ourselves, without LQL or first-N. */
static int capture_input(void *context,
                         const lc_collection_read_metadata *entry,
                         lc_error *error) {
  selection_window *window = context;
  int writer, n;
  (void)error;
  if (entry->kind != LC_COLLECTION_READ_METADATA_ENTRY ||
      window->inputs == window->limit)
    return LC_OK;
  ++window->inputs;
  window->boundary = entry->position;
  if (sscanf(entry->append_id, "writer-%d-%d", &writer, &n) == 2 &&
      (window->exact ? n == 20 : n >= 12)) {
    int length = snprintf(
        window->bytes + window->length, sizeof(window->bytes) - window->length,
        "{\"kind\":\"event\",\"writer\":%d,\"n\":%d}\n", writer, n);
    assert_true(length > 0 &&
                (size_t)length < sizeof(window->bytes) - window->length);
    window->length += (size_t)length;
  }
  return LC_OK;
}

static void capture_window(fixture *f, lc_collection *collection,
                           uint64_t after, uint64_t through, uint64_t limit,
                           int exact, selection_window *window) {
  lc_collection_read_opts options = {0};
  lc_collection_read_res result = {0};
  lc_sink *sink = NULL;
  memset(window, 0, sizeof(*window));
  window->limit = limit;
  window->exact = exact;
  options.has_after = options.has_through = 1;
  options.after = after;
  options.through = through;
  options.on_metadata = capture_input;
  options.metadata_context = window;
  check(f, lc_sink_to_discard(&sink, &f->error));
  check(f, lc_collection_read(collection, &options, sink, &result, &f->error));
  assert_true(result.complete);
  window->upper = result.upper_position;
  if (window->inputs < limit)
    window->boundary = result.last_examined_position;
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
}

typedef struct interrupted_selection {
  lc_collection *reader;
  lc_collection_read_opts options;
  selection_window window;
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  size_t accepted_bytes;
  int cancel_on_entry, cancelled;
} interrupted_selection;

static int interrupt_selection(void *context,
                               const lc_collection_read_metadata *entry,
                               lc_error *error) {
  interrupted_selection *selection = context;
  if (selection == NULL || !selection->cancel_on_entry)
    return lc_error_set(error, LC_ERR_TIMEOUT, 0,
                        "injected selection progress rejection", NULL, NULL,
                        NULL);
  if (entry->kind == LC_COLLECTION_READ_METADATA_ENTRY)
    selection->cancelled = 1;
  return LC_OK;
}

static int selection_cancelled(void *context) {
  return ((interrupted_selection *)context)->cancelled;
}

static void prepare_selection(fixture *f, int round,
                              interrupted_selection *selection) {
  lc_collection_config config = {0};
  lc_collection_read_res result = {0};
  lc_sink *sink = NULL;
  const void *bytes;
  size_t length;
  memset(selection, 0, sizeof(*selection));
  config.name = "events";
  check(f, lc_client_open_collection(f->client, &config, &selection->reader,
                                     &f->error));
  lc_collection_read_opts *options = &selection->options;
  options->has_after = options->has_through = options->has_first_n = 1;
  options->after = round ? LC_COLLECTION_ORDER_PAGE_ROWS - 3 : 0;
  options->through = 1 + round * (WRITERS * PER_ROUND + 2) + 10;
  options->first_n = round == 2 ? LC_COLLECTION_ORDER_PAGE_ROWS + 3 : 8;
  options->selector_lql =
      round == 2 ? "range{field=/n,gte=12}" : "eq{field=/n,value=20}";
  if (round == 1) {
    lc_collection_call call;
    check(f, lc_collection_call_begin(selection->reader, 0, &call, &f->error));
    int rc = lc_collection_query_build(&call, 0, &f->error);
    lc_collection_call_end(&call);
    check(f, rc);
  }
  capture_window(f, selection->reader, options->after, options->through,
                 options->first_n, round != 2, &selection->window);
  selection->cancel_on_entry = round == 2;
  options->on_metadata = interrupt_selection;
  options->metadata_context = selection;
  options->cancelled = selection_cancelled;
  options->cancel_context = selection;
  check(f, lc_sink_to_memory(&sink, &f->error));
  int rc =
      lc_collection_read(selection->reader, options, sink, &result, &f->error);
  assert_int_equal(rc, selection->cancel_on_entry ? LC_OK : LC_ERR_TIMEOUT);
  assert_false(result.complete);
  assert_int_equal(result.upper_position, selection->window.upper);
  assert_int_equal(result.completed_records,
                   selection->cancel_on_entry ? 1 : 0);
  assert_non_null(result.resume_cursor);
  strcpy(selection->cursor, result.resume_cursor);
  if (selection->cancel_on_entry) {
    assert_int_equal(result.stop_reason, LC_COLLECTION_READ_CANCELLED);
    check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
    assert_true(length > 0 && length <= selection->window.length);
    assert_memory_equal(bytes, selection->window.bytes, length);
    selection->accepted_bytes = length;
  }
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
  options->on_metadata = NULL;
  options->cancelled = NULL;
  options->cursor = selection->cursor;
}

static void resume_selection(fixture *f, interrupted_selection *selection,
                             char *checkpoint) {
  assert_read_bytes(f, selection->reader, &selection->options,
                    selection->window.bytes + selection->accepted_bytes,
                    selection->window.upper, selection->window.boundary,
                    checkpoint);
}

static void rejected_cursor(fixture *f, const lc_collection_read_opts *options,
                            char *cursor) {
  lc_collection_read_opts rejected = *options;
  lc_collection_read_res result = {0};
  lc_sink *sink = NULL;
  rejected.on_metadata = interrupt_selection;
  rejected.metadata_context = NULL;
  check(f, lc_sink_to_discard(&sink, &f->error));
  assert_int_equal(
      lc_collection_read(f->collection, &rejected, sink, &result, &f->error),
      LC_ERR_TIMEOUT);
  assert_false(result.complete);
  assert_int_equal(result.completed_records, 0);
  assert_non_null(result.resume_cursor);
  strcpy(cursor, result.resume_cursor);
  lc_collection_read_res_cleanup(&result);
  lc_sink_close(sink);
}

/* Bounded handshakes force interleavings; no sleep or throughput threshold
 * decides whether a race was exercised. Pipe messages fit within PIPE_BUF. */
static int send_bytes(int fd, const void *bytes, size_t count) {
  return write(fd, bytes, count) == (ssize_t)count;
}

static int receive_bytes(int fd, void *bytes, size_t count) {
  struct pollfd event = {fd, POLLIN, 0};
  int timeout = getenv("LOCKDC_SLOW_TEST_RUNTIME") ? 15000 : 5000;
  return poll(&event, 1, timeout) == 1 &&
         read(fd, bytes, count) == (ssize_t)count;
}

typedef struct churn {
  int ready[WRITERS][2], go[WRITERS][2];
  int fired;
  fixture *f;
  interrupted_selection *selection;
} churn;

typedef struct outcome {
  int state_wins, id_wins, duplicates;
} outcome;

static void process_writer(fixture *f, churn *work, int writer) {
  close_client(f);
  for (int i = 0; i < WRITERS; ++i) {
    close(work->ready[i][0]);
    close(work->go[i][1]);
    if (i != writer) {
      close(work->ready[i][1]);
      close(work->go[i][0]);
    }
  }
  for (int round = 0; round < ROUNDS; ++round) {
    char token, id[64], body[256];
    lc_collection_append_res result;
    lc_pouch_state_read_result counter = {0};
    lc_pouch_state_write_options cas = {0};
    outcome out = {0};
    open_client(f, 0, 0, 8192, 1);
    check(f, lc_pouch_state_read_metadata(pouch(f), "mixed", "counter",
                                          &counter, &f->error));
    assert_true(counter.found);
    cas.has_expected_version = 1;
    cas.expected_version = counter.version;
    lc_pouch_state_read_result_cleanup(NULL, &counter);
    assert_true(send_bytes(work->ready[writer][1], "r", 1));
    assert_true(receive_bytes(work->go[writer][0], &token, 1));
    /* Every worker read the same counter version before this barrier. */
    snprintf(body, sizeof(body), "{\"writer\":%d}", writer);
    int rc = write_state(f, "counter", body, &cas);
    assert_true(rc == LC_OK || rc == LC_ERR_INVALID);
    out.state_wins = rc == LC_OK;
    snprintf(id, sizeof(id), "conflict-%d", round);
    rc = append(f, id, body, -1, &result);
    if (rc != LC_OK && rc != LC_ERR_INVALID)
      print_error("writer %d round %d append: %d: %s [%s]\n", writer, round, rc,
                  f->error.message ? f->error.message : "",
                  f->error.server_code ? f->error.server_code : "");
    assert_true(rc == LC_OK || rc == LC_ERR_INVALID);
    if (rc != LC_OK)
      assert_string_equal(f->error.server_code, "append_id_conflict");
    out.id_wins = rc == LC_OK;
    snprintf(id, sizeof(id), "retry-%d", round);
    check(f, append(f, id, "{}", -1, &result));
    out.duplicates = result.duplicate;
    for (int i = 0; i < PER_ROUND; ++i) {
      int n = round * PER_ROUND + i;
      snprintf(id, sizeof(id), "writer-%d-%d", writer, n);
      snprintf(body, sizeof(body),
               "{\"kind\":\"state\",\"writer\":%d,\"n\":%d}", writer, n);
      check(f, write_state(f, id, body, NULL));
      snprintf(body, sizeof(body),
               "{\"kind\":\"event\",\"writer\":%d,\"n\":%d}", writer, n);
      check(f, append(f, id, body, -1, &result));
      assert_false(result.duplicate);
    }
    close_client(f); /* Reopen between rounds while peers and compactor live. */
    assert_true(send_bytes(work->ready[writer][1], &out, sizeof(out)));
  }
  close(work->ready[writer][1]);
  close(work->go[writer][0]);
  lc_error_cleanup(&f->error);
  free(f);
}

typedef struct child_writer {
  fixture *f;
  churn *work;
  int writer;
} child_writer;

static void run_writer(void **state) {
  child_writer *child = *state;
  process_writer(child->f, child->work, child->writer);
}

static int churn_during_copy(void *context, lc_collection *collection,
                             int phase, lc_error *error) {
  churn *work = context;
  outcome total = {0};
  (void)collection;
  if (phase != LC_COLLECTION_COMPACT_CAPTURED || work->fired)
    return LC_OK;
  ++work->fired;
  for (int i = 0; i < WRITERS; ++i) {
    if (!send_bytes(work->go[i][1], "g", 1))
      return lc_error_set(error, LC_ERR_TRANSPORT, 0, "writer release failed",
                          NULL, NULL, NULL);
  }
  for (int i = 0; i < WRITERS; ++i) {
    outcome out;
    if (!receive_bytes(work->ready[i][0], &out, sizeof(out)))
      return lc_error_set(error, LC_ERR_TIMEOUT, 0, "writer completion failed",
                          NULL, NULL, NULL);
    total.state_wins += out.state_wins;
    total.id_wins += out.id_wins;
    total.duplicates += out.duplicates;
  }
  if (total.state_wins != 1 || total.id_wins != 1 ||
      total.duplicates != WRITERS - 1)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                        "mixed conflict counts differ", NULL, NULL, NULL);
  /* Writers have rotated the logs, but the compactor has not copied/published
   * its replacement yet. The old cursor must still select its original view. */
  resume_selection(work->f, work->selection, NULL);
  return LC_OK;
}

static void
test_mixed_process_churn_crosses_rotation_and_index_pages(void **state) {
  fixture *f = *state;
  churn work = {0};
  interrupted_selection selection = {0};
  pid_t children[WRITERS];
  lc_collection_append_res result;
  lc_pouch_namespace_manifest manifest = {0};
  int rc = LC_OK, child_failed = 0;
  open_client(f, 1, 0, 8192, 1);
  check(f, write_state(f, "counter", "{}", NULL));
  check(f, append(f, "anchor", "{}", -1, &result));
  uint64_t initial_capacity =
      ((lc_collection_handle *)f->collection->impl)->ids.capacity;
  close_client(f); /* Never fork a root with running SDK worker threads. */
  for (int i = 0; i < WRITERS; ++i) {
    assert_int_equal(pipe(work.ready[i]), 0);
    assert_int_equal(pipe(work.go[i]), 0);
  }
  for (int i = 0; i < WRITERS; ++i) {
    children[i] = fork();
    assert_true(children[i] >= 0);
    if (children[i] == 0) {
      /* The parent owns cleanup. Contain worker assertions in a child case so
       * a failure cannot jump into the inherited parent test runner. */
      lc_test_tmp_untrack_path(f->root);
      child_writer child = {f, &work, i};
      const struct CMUnitTest tests[] = {
          cmocka_unit_test_prestate(run_writer, &child)};
      unsetenv("CMOCKA_TEST_FILTER");
      unsetenv("CMOCKA_SKIP_FILTER");
      cmocka_set_test_filter("*");
      cmocka_set_skip_filter(NULL);
      exit(cmocka_run_group_tests(tests, NULL, NULL) != 0);
    }
  }
  for (int i = 0; i < WRITERS; ++i) {
    close(work.ready[i][1]);
    close(work.go[i][0]);
  }
  open_client(f, 0, 0, 8192, 1);
  work.f = f;
  work.selection = &selection;
  for (int round = 0; round < ROUNDS && rc == LC_OK; ++round) {
    for (int i = 0; i < WRITERS; ++i) {
      char token;
      if (!receive_bytes(work.ready[i][0], &token, 1) || token != 'r') {
        rc = LC_ERR_TIMEOUT;
        break;
      }
    }
    if (rc != LC_OK)
      break;
    prepare_selection(f, round, &selection);
    lc_pouch_maintenance_result maintained = {0};
    work.fired = 0;
    lc_collection_compact_test_hook = churn_during_copy;
    lc_collection_compact_test_context = &work;
    rc = lc_collection_compact(f->collection, 1, 0, &maintained, &f->error);
    lc_collection_compact_test_hook = NULL;
    if (rc == LC_OK &&
        (work.fired != 1 || maintained.compacted_collection_count != 1))
      rc = LC_ERR_PROTOCOL;
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    if (rc == LC_OK) {
      char checkpoint[LC_COLLECTION_CURSOR_BYTES];
      /* Reuse the same interrupted cursor after publication, both with the
       * index and with a scan. No new appends may extend its captured upper. */
      for (int indexed = 1; indexed >= 0; --indexed) {
        pouch(f)->query_indexing_enabled = indexed;
        resume_selection(f, &selection, checkpoint);
      }
      pouch(f)->query_indexing_enabled = 1;
      capture_window(f, selection.reader, selection.window.boundary,
                     selection.options.through, selection.options.first_n,
                     selection.window.exact, &selection.window);
      selection.options.cursor = checkpoint;
      selection.accepted_bytes = 0;
      resume_selection(f, &selection, NULL);
    }
    lc_collection_close(selection.reader);
    selection.reader = NULL;
    uint64_t count = 1 + (round + 1) * (WRITERS * PER_ROUND + 2);
    if (rc == LC_OK)
      assert_collection(f, NULL, count, count);
  }
  /* Release blocked workers and join them before a parent failure can clean
   * their shared root. A worker failure must not cascade into later tests. */
  for (int i = 0; i < WRITERS; ++i)
    close(work.go[i][1]);
  for (int i = 0; i < WRITERS; ++i) {
    int status;
    assert_int_equal(waitpid(children[i], &status, 0), children[i]);
    close(work.ready[i][0]);
    child_failed |= !WIFEXITED(status) || WEXITSTATUS(status) != 0;
  }
  check(f, rc);
  assert_false(child_failed);
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "mixed", &manifest,
                                            NULL, NULL, &f->error));
  assert_true(manifest.segment_count > 1);
  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  lc_collection_handle *handle = f->collection->impl;
  assert_true(handle->head.active > 1);
  assert_true(handle->order.count > 3 * LC_COLLECTION_ORDER_PAGE_ROWS);
  assert_true(handle->ids.capacity > initial_capacity);
  /* Build/load a complete derived page as evidence that the page boundary,
   * rather than merely a small collection scan, was exercised. */
  lc_collection_call call;
  check(f, lc_collection_call_begin(f->collection, 0, &call, &f->error));
  rc = lc_collection_query_build(&call, 0, &f->error);
  lc_collection_call_end(&call);
  check(f, rc);
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  unsigned char proof[LC_COLLECTION_PAGE_PROOF_BYTES];
  lc_collection_index index = {0};
  size_t row_count;
  int found;
  check(f, lc_collection_order_rows(&handle->order, 0, rows,
                                    LC_COLLECTION_ORDER_PAGE_ROWS, &row_count,
                                    proof, &f->error));
  check(f,
        lc_collection_query_load(handle, handle->order.query_id, 0,
                                 lc_collection_query_row_hash(rows, row_count),
                                 proof, &index, &found, &f->error));
  assert_true(found && index.rows == LC_COLLECTION_ORDER_PAGE_ROWS);
  lc_collection_index_cleanup(&index);
  close_client(f);
  open_client(f, 0, 0, 8192, 1);
  uint64_t count = 1 + ROUNDS * (WRITERS * PER_ROUND + 2);
  assert_collection(f, "eq{field=/kind,value=event}",
                    WRITERS * ROUNDS * PER_ROUND, count);
  assert_collection(f, "eq{field=/kind,value=state}", 0, count);
  for (int w = 0; w < WRITERS; ++w) {
    uint64_t last = 0;
    for (int n = 0; n < ROUNDS * PER_ROUND; ++n) {
      char id[64], body[128];
      lc_collection_append_status_res status;
      snprintf(id, sizeof(id), "writer-%d-%d", w, n);
      snprintf(body, sizeof(body),
               "{\"kind\":\"state\",\"writer\":%d,\"n\":%d}", w, n);
      assert_state(f, id, body);
      check(f,
            lc_collection_append_status(f->collection, id, &status, &f->error));
      assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
      assert_true(status.entry.position > last);
      last = status.entry.position;
    }
  }
}

static void
test_head_identity_stays_pinned_across_peer_compaction(void **state) {
  fixture *f = *state;
  lc_collection *peer = NULL;
  lc_collection_config config = {0};
  lc_collection_info info;
  lc_collection_append_res result;
  lc_pouch_maintenance_result maintained = {0};
  struct stat old, current, pinned;
  open_client(f, 1, 0, 1024, 0);
  config.name = "events";
  check(f, append(f, "before", "{}", -1, &result));
  check(f, lc_client_open_collection(f->client, &config, &peer, &f->error));
  lc_collection_handle *handle = peer->impl;
  check(f, lc_collection_describe(peer, &info, &f->error));
  assert_int_equal(fstat(handle->head_fd, &old), 0);
  for (int i = 0; i < 8; ++i) {
    char id[32];
    snprintf(id, sizeof(id), "replacement-%d", i);
    check(f, write_state(f, id, "{\"ordinary\":true}", NULL));
    check(f, append(f, id, "{}", -1, &result));
  }
  check(f, lc_collection_compact(f->collection, 1, 0, &maintained, &f->error));
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
  /* The dormant receiver keeps its old immutable inode alive, so a new head
   * cannot alias its cached identity even under immediate inode reuse. */
  assert_int_equal(fstat(handle->head_fd, &pinned), 0);
  assert_true(old.st_dev == pinned.st_dev && old.st_ino == pinned.st_ino);
  assert_int_equal(pinned.st_nlink, 0);
  assert_int_equal(stat(handle->head_path, &current), 0);
  assert_true(old.st_dev != current.st_dev || old.st_ino != current.st_ino);
  check(f, lc_collection_describe(peer, &info, &f->error));
  assert_int_equal(info.live_count, 9);
  assert_int_equal(fstat(handle->head_fd, &pinned), 0);
  assert_true(current.st_dev == pinned.st_dev &&
              current.st_ino == pinned.st_ino);
  assert_true(fcntl(handle->head_fd, F_GETFD) & FD_CLOEXEC);
  lc_collection_append_req request = {0};
  lc_source *source = NULL;
  request.append_id = "after";
  check(f, lc_source_from_memory("{}", 2, &source, &f->error));
  check(f, lc_collection_append(peer, &request, source, &result, &f->error));
  lc_source_close(source);
  assert_int_equal(result.position, 10);
  lc_collection_close(peer);
  assert_collection(f, NULL, 10, 10);
  assert_state(f, "replacement-7", "{\"ordinary\":true}");
}

/* Restore only canonical suffixes that were not protected by a successful
 * sync. Newer order/ID projections deliberately survive. Process exit alone
 * would retain dirty OS pages and would not model this power-loss condition. */
static void test_crash_mixed_unsynced_tail_loss_and_tearing(void **state) {
  fixture *f = *state;
  char original[sizeof(f->root)];
  strcpy(original, f->root);
  for (int mask = 1; mask <= 3; ++mask) {
    for (int torn = 0; torn <= 1; ++torn) {
      char state_path[1024], collection_path[1024];
      char stable[LC_COLLECTION_CURSOR_BYTES];
      char future[LC_COLLECTION_CURSOR_BYTES] = {0};
      char checkpoint[LC_COLLECTION_CURSOR_BYTES];
      lc_collection_read_opts selected = {0};
      int observed_cursor[2];
      lc_pouch_namespace_manifest manifest = {0};
      lc_collection_append_res result;
      struct stat state_st, collection_st;
      int length = snprintf(f->root, sizeof(f->root), "%s/loss-%d-%d", original,
                            mask, torn);
      assert_true(length > 0 && (size_t)length < sizeof(f->root));
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
      open_client(f, 1, 1, 1048576, 0);
      check(f, write_state(f, "same", "{\"state\":1}", NULL));
      check(f, append(f, "same", "{\"event\":1}", -1, &result));
      selected.has_after = selected.has_through = selected.has_first_n = 1;
      selected.through = 3;
      selected.first_n = 1;
      selected.selector_lql = "range{field=/event,gte=2}";
      rejected_cursor(f, &selected, stable);
      selected.cursor = stable;
      assert_read_bytes(f, f->collection, &selected, "", 1, 1, checkpoint);
      selected.cursor = NULL;
      lc_pouch_fsync_stats durable;
      check(f, lc_pouch_fsync_stats_read(pouch(f), &durable, &f->error));
      assert_true(durable.total_requests >= 2);
      check(f, lc_pouch_namespace_manifest_open(
                   NULL, f->root, "mixed", &manifest, NULL, NULL, &f->error));
      snprintf(state_path, sizeof(state_path), "%s/segments/%s",
               manifest.namespace_path, manifest.active_segment);
      lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
      lc_collection_handle *handle = f->collection->impl;
      char *path = NULL, leaf[64];
      check(f, lc_collection_record_path(handle, handle->head.active, &path,
                                         leaf, &f->error));
      assert_true(strlen(path) < sizeof(collection_path));
      strcpy(collection_path, path);
      lc_free_with_allocator(NULL, path);
      close_client(f);
      assert_int_equal(stat(state_path, &state_st), 0);
      assert_int_equal(stat(collection_path, &collection_st), 0);
      assert_int_equal(pipe(observed_cursor), 0);
      pid_t child = fork();
      assert_true(child >= 0);
      if (child == 0) {
        close(observed_cursor[0]);
        open_client(f, 0, 0, 1048576, 0);
        check(f, write_state(f, "same", "{\"state\":2}", NULL));
        check(f, append(f, "lost", "{\"event\":2}", -1, &result));
        rejected_cursor(f, &selected, future);
        assert_true(send_bytes(observed_cursor[1], future, sizeof(future)));
        close(observed_cursor[1]);
        lc_pouch_fsync_stats stats;
        check(f, lc_pouch_fsync_stats_read(pouch(f), &stats, &f->error));
        assert_int_equal(stats.total_requests, 0);
        /* Explicitly retain projections while canonical writes may be lost. */
        handle = f->collection->impl;
        assert_int_equal(fsync(handle->order.fd), 0);
        assert_int_equal(fsync(handle->ids.fd), 0);
        _exit(42);
      }
      close(observed_cursor[1]);
      int status;
      assert_int_equal(waitpid(child, &status, 0), child);
      assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 42);
      assert_true(receive_bytes(observed_cursor[0], future, sizeof(future)));
      close(observed_cursor[0]);
      const char *paths[] = {state_path, collection_path};
      off_t ends[] = {state_st.st_size, collection_st.st_size};
      for (int i = 0; i < 2; ++i) {
        if (!(mask & (1 << i)))
          continue;
        int fd = open(paths[i], O_RDWR);
        assert_true(fd >= 0);
        assert_int_equal(ftruncate(fd, ends[i] + (torn ? 7 : 0)), 0);
        assert_int_equal(fsync(fd), 0);
        close(fd);
      }
      open_client(f, 0, 0, 1048576, 0);
      assert_state(f, "same", mask & 1 ? "{\"state\":1}" : "{\"state\":2}");
      lc_collection_append_status_res observed;
      check(f, lc_collection_append_status(f->collection, "lost", &observed,
                                           &f->error));
      assert_int_equal(observed.state, mask & 2
                                           ? LC_COLLECTION_APPEND_ABSENT
                                           : LC_COLLECTION_APPEND_COMMITTED);
      assert_collection(f, NULL, mask & 2 ? 1 : 2, mask & 2 ? 1 : 2);
      /* A durable-prefix cursor stays bounded even if the newer entry
       * survives. A cursor that saw lost history must fail, never clamp. */
      selected.cursor = stable;
      assert_read_bytes(f, f->collection, &selected, "", 1, 1, NULL);
      selected.cursor = future;
      if (mask & 2) {
        lc_collection_read_res unavailable = {0};
        lc_sink *sink = NULL;
        check(f, lc_sink_to_memory(&sink, &f->error));
        assert_int_equal(lc_collection_read(f->collection, &selected, sink,
                                            &unavailable, &f->error),
                         LC_ERR_INVALID);
        assert_string_equal(f->error.server_code, "cursor_unavailable");
        assert_int_equal(unavailable.completed_records, 0);
        assert_null(unavailable.resume_cursor);
        const void *bytes;
        size_t length;
        check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
        assert_int_equal(length, 0);
        lc_collection_read_res_cleanup(&unavailable);
        lc_sink_close(sink);
      } else {
        assert_read_bytes(f, f->collection, &selected, "", 2, 1, NULL);
      }
      selected.cursor = checkpoint;
      uint64_t recovered = mask & 2 ? 1 : 2;
      assert_read_bytes(f, f->collection, &selected,
                        mask & 2 ? "" : "{\"event\":2}\n", recovered, recovered,
                        checkpoint);
      check(f, write_state(f, "same", "{\"state\":3}", NULL));
      check(f, append(f, "after", "{\"event\":3}", -1, &result));
      uint64_t expected = mask & 2 ? 2 : 3;
      assert_int_equal(result.position, expected);
      close_client(f);
      open_client(f, 0, 0, 1048576, 0);
      assert_state(f, "same", "{\"state\":3}");
      assert_collection(f, NULL, expected, expected);
      selected.cursor = checkpoint;
      assert_read_bytes(f, f->collection, &selected, "{\"event\":3}\n",
                        expected, expected, NULL);
      close_client(f);
    }
  }
  strcpy(f->root, original);
}

static int fail_sync(int fd) {
  (void)fd;
  ++sync_fixture->sync_calls;
  errno = EIO;
  return -1;
}

static int write_during_pending_commit(void *context, lc_collection *collection,
                                       lc_error *error) {
  fixture *f = context;
  (void)collection;
  f->state_failure = write_state(f, "pending", "{}", NULL);
  if (f->state_failure != LC_ERR_TRANSPORT)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                        "state write did not report the injected sync error",
                        NULL, NULL, NULL);
  return LC_OK;
}

static void test_mixed_sync_errors_and_retries(void **state) {
  fixture *f = *state;
  lc_collection_append_res result;
  open_client(f, 1, 1, 1048576, 0);
  check(f, write_state(f, "before", "{}", NULL));
  sync_fixture = f;
  lc_pouch_test_sync_fd = fail_sync;
  lc_collection_commit_wait_test_hook = write_during_pending_commit;
  lc_collection_commit_wait_test_context = f;
  assert_int_equal(append(f, "pending", "{}", -1, &result), LC_ERR_TRANSPORT);
  assert_int_equal(f->state_failure, LC_ERR_TRANSPORT);
  assert_true(f->sync_calls >= 2);
  lc_pouch_test_sync_fd = NULL;
  lc_collection_commit_wait_test_hook = NULL;
  check(f, write_state(f, "pending", "{\"retried\":true}", NULL));
  check(f, append(f, "pending", "{}", -1, &result));
  assert_true(result.duplicate && result.position == 1);
  close_client(f);
  open_client(f, 0, 1, 1048576, 0);
  assert_state(f, "before", "{}");
  assert_state(f, "pending", "{\"retried\":true}");
  assert_collection(f, NULL, 1, 1);
}

static int clock_now(clockid_t id, struct timespec *out, void *context) {
  fixture *f = context;
  (void)id;
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}

static void scheduler_backlog(fixture *f) {
  /* Keep both mutation and durable-marker work available. Their namespaces
   * deliberately do not contain the objects that require expiry discovery. */
  check(f, write_state_in(f, "busy", "current", "{}", NULL));
  check(f, lc_pouch_compaction_note_history_advanced(pouch(f), "terminal",
                                                     &f->error));
  assert_true(lc_pouch_test_compaction_queue_count(pouch(f)) > 0);
}

static void assert_namespace_states(fixture *f, const char *key, int present) {
  const char *names[] = {"retention-a", "retention-b"};
  for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
    lc_pouch_state_read_result result = {0};
    check(f, lc_pouch_state_read(pouch(f), names[i], key, &result, &f->error));
    int found = result.found;
    lc_pouch_state_read_result_cleanup(NULL, &result);
    assert_int_equal(found, present);
  }
}

static void prepare_scheduler(fixture *f) {
  lc_pouch_state_write_options expiry = {0};
  lc_collection_append_res entry;
  f->now = 2000;
  lc_pouch_test_queue_clock_gettime = clock_now;
  lc_pouch_test_queue_time_context = f;
  open_client(f, 1, 0, 1, 0);
  expiry.has_retention = 1;
  expiry.retention = 2010;
  check(f, write_state_in(f, "retention-a", "expire", "{}", &expiry));
  check(f, write_state_in(f, "retention-b", "expire", "{}", &expiry));
  expiry.retention = 2020;
  check(f, write_state_in(f, "retention-a", "expire-later", "{}", &expiry));
  check(f, write_state_in(f, "retention-b", "expire-later", "{}", &expiry));
  check(f, write_state_in(f, "terminal", "old", "{}", NULL));
  check(f, write_state_in(f, "terminal", "old", "{}", NULL));
  check(f, lc_pouch_compaction_note_history_advanced(pouch(f), "terminal",
                                                     &f->error));
  check(f, append(f, "expire", "{}", 10, &entry));
  check(f, append(f, "expire-later", "{}", 20, &entry));
  check(f, append(f, "keep", "{}", -1, &entry));
  close_client(f);
  /* Reopening must rediscover expiry; none of its namespaces is in the
   * in-memory mutation queue. The worker's normal long interval prevents a
   * timed pass; invoke its real pass directly, without sleeps. */
  f->background = 1;
  open_client(f, 0, 0, 1, 0);
  pouch(f)->compaction_min_segment_count = 1;
  pouch(f)->compaction_min_reclaimable_bytes = 1;
  pouch(f)->terminal_reclaim_min_bytes = 1;
}

static void test_background_maintenance_sources_do_not_starve(void **state) {
  fixture *f = *state;
  prepare_scheduler(f);
  for (int epoch = 0; epoch < 2; ++epoch) {
    f->now = 2010 + epoch * 10;
    for (int pass = 0; pass < 8; ++pass) {
      scheduler_backlog(f);
      lc_pouch_test_compaction_run_pass(pouch(f));
    }
    assert_namespace_states(f, epoch ? "expire-later" : "expire", 0);
    assert_collection(f, NULL, epoch ? 1 : 2, 3);
    if (epoch == 0)
      assert_namespace_states(f, "expire-later", 1);
  }
  const char *names[] = {"busy", "terminal"};
  for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
    lc_pouch_namespace_manifest manifest = {0};
    check(f, lc_pouch_namespace_manifest_open(
                 NULL, f->root, names[i], &manifest, NULL, NULL, &f->error));
    assert_non_null(manifest.latest_snapshot);
    lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  }
}

static void
test_background_maintenance_discovery_failure_does_not_pin_turn(void **state) {
  fixture *f = *state;
  char path[1024], saved[1040];
  prepare_scheduler(f);
  snprintf(path, sizeof(path), "%s/collection-work", f->root);
  snprintf(saved, sizeof(saved), "%s.saved", path);
  assert_int_equal(rename(path, saved), 0);
  int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
  assert_true(fd >= 0);
  assert_int_equal(close(fd), 0);
  f->now = 2010;
  for (int pass = 0; pass < 8; ++pass) {
    scheduler_backlog(f);
    lc_pouch_test_compaction_run_pass(pouch(f));
  }
  /* Failed collection discovery is reported by the real pass, but cannot
   * prevent other namespaces' state-retention turns. */
  assert_namespace_states(f, "expire", 0);
  assert_int_equal(unlink(path), 0);
  assert_int_equal(rename(saved, path), 0);
  f->now = 2020;
  for (int pass = 0; pass < 8; ++pass) {
    scheduler_backlog(f);
    lc_pouch_test_compaction_run_pass(pouch(f));
  }
  assert_namespace_states(f, "expire-later", 0);
  assert_collection(f, NULL, 1, 3);
}

static void maintain(fixture *f) {
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  options.ns = "mixed";
  options.force = 1;
  int rc = lc_pouch_maintenance_run(pouch(f), &options, &result, &f->error);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  check(f, rc);
}

static void
test_mixed_retention_renewal_compaction_and_parent_expiry(void **state) {
  fixture *f = *state;
  lc_pouch_state_write_options expiry = {0};
  lc_pouch_state_write_result written = {0};
  lc_collection_append_res entry;
  lc_collection_metadata_req metadata = {1, 30};
  lc_collection_info info;
  f->now = 2000;
  lc_pouch_test_queue_clock_gettime = clock_now;
  lc_pouch_test_queue_time_context = f;
  open_client(f, 1, 0, 1024, 1);
  check(f, lc_collection_metadata(f->collection, &metadata, &info, &f->error));
  char identity[LC_XID_STRING_SIZE];
  strcpy(identity, info.collection_id);
  expiry.has_retention = 1;
  expiry.retention = 2010;
  check(f, write_state(f, "expire", "{\"state\":1}", &expiry));
  check(f, write_state(f, "renew", "{\"state\":2}", &expiry));
  check(f, append(f, "expire", "{\"event\":1}", 10, &entry));
  check(f, append(f, "renew", "{\"event\":2}", 10, &entry));
  f->now = 2009;
  maintain(f);
  assert_state(f, "expire", "{\"state\":1}");
  assert_collection(f, NULL, 2, 2);
  /* Deadlines reached, but renewal is still legal before the janitor deletes.
   */
  f->now = 2010;
  expiry.retention = 2040;
  check(f, lc_pouch_state_update_metadata(pouch(f), "mixed", "renew", &expiry,
                                          &written, &f->error));
  lc_pouch_state_write_result_cleanup(NULL, &written);
  check(f, lc_collection_entry_metadata(f->collection, 2, &metadata, &entry,
                                        &f->error));
  assert_int_equal(entry.retention_expires_at_unix, 2040);
  maintain(f);
  assert_state(f, "expire", NULL);
  assert_state(f, "renew", "{\"state\":2}");
  lc_collection_append_status_res status;
  check(f, lc_collection_append_status(f->collection, "expire", &status,
                                       &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  assert_collection(f, NULL, 1, 2);
  close_client(f);
  open_client(f, 0, 0, 1024, 1);
  assert_state(f, "renew", "{\"state\":2}");
  assert_collection(f, NULL, 1, 2);
  f->now = 2030;
  maintain(f);
  assert_int_equal(lc_collection_describe(f->collection, &info, &f->error),
                   LC_ERR_INVALID);
  assert_state(f, "renew", "{\"state\":2}");
  close_client(f);
  open_client(f, 1, 0, 1024, 1);
  check(f, lc_collection_describe(f->collection, &info, &f->error));
  assert_string_not_equal(identity, info.collection_id);
  check(f, append(f, "renew", "{}", -1, &entry));
  assert_int_equal(entry.position, 1);
  assert_state(f, "renew", "{\"state\":2}");
  f->now = 2040;
  maintain(f);
  assert_state(f, "renew", NULL);
  assert_collection(f, NULL, 1, 1);
}

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_background_maintenance_sources_do_not_starve, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_background_maintenance_discovery_failure_does_not_pin_turn,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_head_identity_stays_pinned_across_peer_compaction, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_mixed_process_churn_crosses_rotation_and_index_pages, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_crash_mixed_unsynced_tail_loss_and_tearing, setup, teardown),
      cmocka_unit_test_setup_teardown(test_mixed_sync_errors_and_retries, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_mixed_retention_renewal_compaction_and_parent_expiry, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(*tests), argc, argv,
                           NULL, NULL);
}
