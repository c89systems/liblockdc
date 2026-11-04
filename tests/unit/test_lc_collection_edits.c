#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"
#include "../support/lc_test_tmp.h"
#include "lc_collection_internal.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-edit-"
typedef struct fixture {
  char root[512], endpoint[600];
  lc_client *client;
  lc_collection *collection;
  lc_collection *peer, *next_owner;
  lc_error error;
  lc_source *source;
  lc_sink *sink;
  lc_collection_read_res read;
  lc_collection_head_res head;
  lc_collection_edit_req req;
  lc_collection_edit_res result;
  lc_collection_edit_range ranges[LC_COLLECTION_EDIT_MAX_RANGES + 1];
  lc_collection_append_req replacement;
  char token[65];
  char *key;
  int compressed, durable, phase, crash, fail_once;
  lc_allocator allocator;
  pthread_mutex_t allocation_mutex;
  long fail_after;
  size_t live_allocations, max_allocation;
  uint64_t positions[512];
  size_t seen;
  lc_pouch_unix_seconds now;
  int clock_failed;
} fixture;
static void *allocate(void *context, size_t size) {
  fixture *f = context;
  void *p = NULL;
  pthread_mutex_lock(&f->allocation_mutex);
  if (f->fail_after != 0) {
    if (f->fail_after > 0)
      --f->fail_after;
    if (size > f->max_allocation)
      f->max_allocation = size;
    p = malloc(size);
    if (p)
      ++f->live_allocations;
  }
  pthread_mutex_unlock(&f->allocation_mutex);
  return p;
}
static void deallocate(void *context, void *p) {
  fixture *f = context;
  if (p) {
    pthread_mutex_lock(&f->allocation_mutex);
    assert_true(f->live_allocations > 0);
    --f->live_allocations;
    pthread_mutex_unlock(&f->allocation_mutex);
    free(p);
  }
}
static void *resize(void *context, void *p, size_t size) {
  fixture *f = context;
  void *resized = NULL;
  if (!p)
    return allocate(context, size);
  if (!size) {
    deallocate(context, p);
    return NULL;
  }
  pthread_mutex_lock(&f->allocation_mutex);
  if (f->fail_after != 0) {
    if (f->fail_after > 0)
      --f->fail_after;
    if (size > f->max_allocation)
      f->max_allocation = size;
    resized = realloc(p, size);
  }
  pthread_mutex_unlock(&f->allocation_mutex);
  return resized;
}
static void check(fixture *f, int rc) {
  if (rc)
    print_error("edit error %d: %s [%s]\n", rc,
                f->error.message ? f->error.message : "",
                f->error.server_code ? f->error.server_code : "");
  assert_int_equal(rc, LC_OK);
}
static void clear(fixture *f) {
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
}
static void close_all(fixture *f) {
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_client_close(f->client);
  f->client = NULL;
}
static int open_client(fixture *f) {
  lc_client_config config;
  lc_pouch_settings settings;
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_SINGLE_WRITER |
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = 0;
  settings.durable_sync = f->durable;
  settings.segment_target_bytes = 65536;
  settings.compression = f->compressed ? "zlib" : "none";
  if (f->key) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = f->key;
  }
  config.allocator = f->allocator;
  config.pouch_settings = &settings;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "journal";
  return lc_client_open(&config, &f->client, &f->error);
}
static void open_all(fixture *f, int create) {
  lc_collection_config collection;
  check(f, open_client(f));
  lc_collection_config_init(&collection);
  collection.name = "events";
  check(f, create ? lc_client_create_collection(f->client, &collection,
                                                &f->collection, &f->error)
                  : lc_client_open_collection(f->client, &collection,
                                              &f->collection, &f->error));
}
static int setup_mode(void **state, int encrypted) {
  fixture *f = calloc(1, sizeof(*f));
  char path[] = PREFIX "XXXXXX";
  assert_non_null(f);
  lc_error_init(&f->error);
  assert_int_equal(pthread_mutex_init(&f->allocation_mutex, NULL), 0);
  f->fail_after = -1;
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = deallocate;
  f->allocator.context = f;
  assert_true(lc_test_tmp_mkdtemp(path, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  if (encrypted) {
    check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
    f->compressed = 1;
  }
  *state = f;
  open_all(f, 1);
  return 0;
}
static int setup(void **state) { return setup_mode(state, 0); }
static int setup_encrypted(void **state) { return setup_mode(state, 1); }
static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_edit_test_hook = NULL;
  lc_collection_edit_test_context = NULL;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  lc_source_close(f->source);
  lc_sink_close(f->sink);
  lc_collection_read_res_cleanup(&f->read);
  lc_collection_close(f->next_owner);
  lc_collection_close(f->peer);
  close_all(f);
  assert_int_equal(f->live_allocations, 0);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  assert_int_equal(pthread_mutex_destroy(&f->allocation_mutex), 0);
  free(f);
  return 0;
}
static void prepare(fixture *f, const char *id) {
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  strcpy(f->token, f->head.etag);
  lc_collection_edit_req_init(&f->req);
  f->req.operation_id = id;
  f->req.expected_head = f->token;
}
static int edit(fixture *f, const char *json) {
  int rc;
  if (json)
    check(f, lc_source_from_memory(json, strlen(json), &f->source, &f->error));
  rc = lc_collection_edit(f->collection, &f->req, f->source, &f->result,
                          &f->error);
  lc_source_close(f->source);
  f->source = NULL;
  return rc;
}
static void append(fixture *f, const char *id, const char *json) {
  lc_collection_append_req req;
  lc_collection_append_res result;
  lc_collection_append_req_init(&req);
  req.append_id = id;
  check(f, lc_source_from_memory(json, strlen(json), &f->source, &f->error));
  check(f, lc_collection_append(f->collection, &req, f->source, &result,
                                &f->error));
  lc_source_close(f->source);
  f->source = NULL;
}
static void seed(fixture *f, unsigned int count) {
  char id[32], json[64];
  unsigned int i;
  for (i = 1; i <= count; ++i) {
    snprintf(id, sizeof(id), "entry-%u", i);
    snprintf(json, sizeof(json), "{\"n\":%u}", i);
    append(f, id, json);
  }
}
static int observe(void *context, const lc_collection_read_metadata *metadata,
                   lc_error *error) {
  fixture *f = context;
  (void)error;
  if (metadata->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    assert_true(f->seen < sizeof(f->positions) / sizeof(*f->positions));
    f->positions[f->seen++] = metadata->position;
  }
  return LC_OK;
}
static void scan(fixture *f, const char *expected) {
  const void *bytes;
  size_t count;
  lc_collection_read_opts opts;
  lc_sink_close(f->sink);
  f->sink = NULL;
  lc_collection_read_res_cleanup(&f->read);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  lc_collection_read_opts_init(&opts);
  f->seen = 0;
  opts.on_metadata = observe;
  opts.metadata_context = f;
  check(f,
        lc_collection_read(f->collection, &opts, f->sink, &f->read, &f->error));
  check(f, lc_sink_memory_bytes(f->sink, &bytes, &count, &f->error));
  if (expected) {
    assert_int_equal(count, strlen(expected));
    assert_memory_equal(bytes, expected, count);
  }
}
static void replacement(fixture *f, const char *id) {
  lc_collection_append_req_init(&f->replacement);
  f->replacement.append_id = id;
  f->req.replacement = &f->replacement;
}
static void compact(fixture *f) {
  lc_pouch_maintenance_result result;
  memset(&result, 0, sizeof(result));
  check(f, lc_collection_compact(f->collection, 1, 0, &result, &f->error));
  assert_true(result.compacted);
}
static void test_ranges_and_receipts(void **state) {
  fixture *f = *state;
  lc_collection_append_status_res status;
  lc_collection_edit_status_res receipt;
  lc_collection_delete_res deleted;
  char original[65];
  lc_collection_read_opts filtered;
  const void *filtered_bytes;
  size_t filtered_length;
  seed(f, 8);
  prepare(f, "trim");
  replacement(f, "context");
  f->ranges[0].after = 1;
  f->ranges[0].through = 2;
  f->ranges[1].after = 4;
  f->ranges[1].through = 8;
  f->req.ranges = f->ranges;
  f->req.range_count = 2;
  f->req.has_recovery = 1;
  f->req.recovery = "{\"baseline\":true}";
  f->req.recovery_anchor = LC_U64_MAX;
  check(f, edit(f, " { \"context\" : 1 } "));
  assert_int_equal(f->result.deleted_count, 5);
  assert_int_equal(f->result.live_count, 4);
  assert_int_equal(f->result.replacement.position, 9);
  strcpy(original, f->result.head_etag);
  scan(f, "{\"n\":1}\n{\"n\":3}\n{\"n\":4}\n{\"context\":1}\n");
  lc_collection_read_opts_init(&filtered);
  filtered.has_after = 1;
  filtered.after = 1;
  filtered.has_first_n = 1;
  filtered.first_n = 2;
  filtered.selector_lql = "range{field=/n,gte=4}";
  lc_sink_close(f->sink);
  f->sink = NULL;
  lc_collection_read_res_cleanup(&f->read);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  check(f, lc_collection_read(f->collection, &filtered, f->sink, &f->read,
                              &f->error));
  check(f, lc_sink_memory_bytes(f->sink, &filtered_bytes, &filtered_length,
                                &f->error));
  assert_int_equal(filtered_length, 8);
  assert_memory_equal(filtered_bytes, "{\"n\":4}\n", 8);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.etag, original);
  assert_int_equal(f->head.recovery_anchor, 9);
  assert_true(f->head.recovery_anchor_available);
  check(f, lc_collection_append_status(f->collection, "entry-2", &status,
                                       &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  append(f, "later", "{}");
  check(f, lc_collection_delete_entry(f->collection, 9, &deleted, &f->error));
  check(f, edit(f, "{\"context\":1}"));
  assert_true(f->result.duplicate);
  assert_string_equal(f->result.head_etag, original);
  assert_int_equal(edit(f, "{\"context\":2}"), LC_ERR_INVALID);
  clear(f);
  f->ranges[1].through = 7;
  assert_int_equal(edit(f, "{\"context\":1}"), LC_ERR_INVALID);
  clear(f);
  f->ranges[1].through = 8;
  f->replacement.has_retention_seconds = 1;
  f->replacement.retention_seconds = 0;
  assert_int_equal(edit(f, "{\"context\":1}"), LC_ERR_INVALID);
  clear(f);
  f->replacement.has_retention_seconds = 0;
  f->req.has_recovery = 0;
  assert_int_equal(edit(f, "{\"context\":1}"), LC_ERR_INVALID);
  clear(f);
  f->req.has_recovery = 1;
  f->req.recovery = "changed marker";
  assert_int_equal(edit(f, "{\"context\":1}"), LC_ERR_INVALID);
  clear(f);
  f->req.recovery = "{\"baseline\":true}";

  /* Force ordinary rows and the receipt into distinct snapshot parts. */
  lc_collection_handle *collection = f->collection->impl;
  collection->pouch->segment_target_bytes = 512U;
  compact(f);
  assert_true(collection->head.snapshot_extra_parts > 0);
  check(f,
        lc_collection_edit_status(f->collection, "trim", &receipt, &f->error));
  assert_true(receipt.found);
  assert_string_equal(receipt.result.head_etag, original);
  check(f, edit(f, "{\"context\":1}"));
  assert_true(f->result.duplicate);
  close_all(f);
  open_all(f, 0);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_false(f->head.recovery_anchor_available);
  check(f, edit(f, "{\"context\":1}"));
  assert_true(f->result.duplicate);
  scan(f, "{\"n\":1}\n{\"n\":3}\n{\"n\":4}\n{}\n");
}
static void test_validation_and_heads(void **state) {
  fixture *f = *state;
  lc_collection_metadata_req meta;
  lc_collection_info info;
  lc_collection_append_res entry;
  lc_collection_delete_res deleted;
  lc_collection_read_opts opts;
  lc_collection_capabilities cap;
  check(f, lc_client_collection_capabilities(f->client, &cap, &f->error));
  assert_true(cap.atomic_edit);
  seed(f, 4);
  prepare(f, "collision");
  replacement(f, "entry-1");
  assert_int_equal(edit(f, "{}"), LC_ERR_INVALID);
  clear(f);
  scan(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n");
  prepare(f, "stale-policy");
  lc_collection_metadata_req_init(&meta);
  meta.has_max_entries = 1;
  meta.max_entries = 200;
  check(f, lc_collection_metadata(f->collection, &meta, &info, &f->error));
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  prepare(f, "stale-entry");
  meta.has_max_entries = 0;
  meta.has_retention_seconds = 1;
  meta.retention_seconds = 0;
  check(f, lc_collection_entry_metadata(f->collection, 1, &meta, &entry,
                                        &f->error));
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  prepare(f, "stale-delete");
  check(f, lc_collection_delete_entry(f->collection, 4, &deleted, &f->error));
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  prepare(f, "invalid");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 2;
  f->ranges[0].through = 2;
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  f->ranges[0].through = 99;
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  f->req.range_count = LC_COLLECTION_EDIT_MAX_RANGES + 1;
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  f->req.range_count = 2;
  f->ranges[0].after = 0;
  f->ranges[0].through = 2;
  f->ranges[1].after = 1;
  f->ranges[1].through = 3;
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  prepare(f, "nothing");
  check(f, edit(f, NULL));
  lc_collection_read_opts_init(&opts);
  opts.expected_head = f->token;
  lc_sink_close(f->sink);
  f->sink = NULL;
  lc_collection_read_res_cleanup(&f->read);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  assert_int_equal(
      lc_collection_read(f->collection, &opts, f->sink, &f->read, &f->error),
      LC_ERR_INVALID);
  clear(f);
  assert_int_equal(f->read.completed_records, 0);
  prepare(f, "marker");
  f->req.has_recovery = 1;
  f->req.recovery = "start";
  f->req.recovery_anchor = 1;
  check(f, edit(f, NULL));
  prepare(f, "clear");
  f->req.has_recovery = 1;
  f->req.recovery = "";
  check(f, edit(f, NULL));
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, "");
  compact(f);
}
static int fail_phase(void *context, lc_collection *c, int phase, lc_error *e) {
  fixture *f = context;
  (void)c;
  if (phase != f->phase)
    return LC_OK;
  if (f->crash)
    _exit(37);
  if (f->fail_once)
    f->phase = 0;
  return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected edit boundary", NULL,
                      NULL, "test");
}
static void test_failure_recovery(void **state) {
  fixture *f = *state;
  int phase;
  for (phase = LC_COLLECTION_EDIT_PREPARED; phase <= LC_COLLECTION_EDIT_BODY;
       ++phase) {
    char op[32], id[32];
    snprintf(op, sizeof(op), "fault-%d", phase);
    snprintf(id, sizeof(id), "body-%d", phase);
    for (unsigned int i = 0; i < 130; ++i) {
      char entry_id[48];
      snprintf(entry_id, sizeof(entry_id), "seed-%d-%u", phase, i);
      append(f, entry_id, "{}");
    }
    prepare(f, op);
    replacement(f, id);
    f->req.ranges = f->ranges;
    f->req.range_count = 1;
    f->ranges[0].after = 0;
    f->ranges[0].through = f->head.upper_position;
    lc_collection_handle *before = (lc_collection_handle *)f->collection;
    uint64_t before_segment = before->head.active;
    off_t before_end = lseek(before->active_fd, 0, SEEK_END);
    f->phase = phase;
    lc_collection_edit_test_hook = fail_phase;
    lc_collection_edit_test_context = f;
    assert_int_equal(edit(f, "{}"), LC_ERR_TRANSPORT);
    assert_int_equal(f->result.published,
                     phase != LC_COLLECTION_EDIT_PREPARED &&
                         phase != LC_COLLECTION_EDIT_BODY);
    clear(f);
    lc_collection_edit_test_hook = NULL;
    check(f, lc_collection_head(f->collection, &f->head, &f->error));
    assert_int_equal(f->head.live_count,
                     phase == LC_COLLECTION_EDIT_PREPARED ? 130
                     : phase == LC_COLLECTION_EDIT_BODY   ? 131
                                                          : 1);
    if (phase == LC_COLLECTION_EDIT_BODY) {
      lc_collection_handle *after = (lc_collection_handle *)f->collection;
      assert_int_equal(lseek(after->active_fd, 0, SEEK_END),
                       after->head.active == before_segment ? before_end : 0);
    }
    check(f, edit(f, "{}"));
    assert_int_equal(f->result.duplicate,
                     phase != LC_COLLECTION_EDIT_PREPARED &&
                         phase != LC_COLLECTION_EDIT_BODY);
    close_all(f);
    open_all(f, 0);
  }
}
static void test_crash_recovery(void **state) {
  fixture *f = *state;
  int phase, status;
  pid_t pid;
  f->durable = 1;
  close_all(f);
  open_all(f, 0);
  seed(f, 130);
  for (phase = LC_COLLECTION_EDIT_PREPARED; phase <= LC_COLLECTION_EDIT_BODY;
       ++phase) {
    lc_collection_handle *before = (lc_collection_handle *)f->collection;
    uint64_t before_segment = before->head.active;
    off_t before_end = lseek(before->active_fd, 0, SEEK_END);
    close_all(f);
    pid = fork();
    assert_true(pid >= 0);
    if (!pid) {
      char op[32];
      open_all(f, 0);
      snprintf(op, sizeof(op), "crash-%d", phase);
      prepare(f, op);
      replacement(f, op);
      f->req.ranges = f->ranges;
      f->req.range_count = 1;
      f->ranges[0].after = 0;
      f->ranges[0].through = f->head.upper_position;
      f->phase = phase;
      f->crash = 1;
      lc_collection_edit_test_hook = fail_phase;
      lc_collection_edit_test_context = f;
      edit(f, "{}");
      _exit(99);
    }
    assert_int_equal(waitpid(pid, &status, 0), pid);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 37);
    open_all(f, 0);
    scan(f, NULL);
    assert_int_equal(f->seen, phase == LC_COLLECTION_EDIT_PREPARED ? 130 : 1);
    if (phase == LC_COLLECTION_EDIT_BODY) {
      lc_collection_handle *after = (lc_collection_handle *)f->collection;
      assert_int_equal(lseek(after->active_fd, 0, SEEK_END),
                       after->head.active == before_segment ? before_end : 0);
    }
  }
}
static void test_codec_conversion(void **state) {
  fixture *f = *state;
  lc_collection_metadata_req metadata;
  lc_collection_append_res updated;
  seed(f, 150);
  prepare(f, "encrypted-cut");
  replacement(f, "encrypted-replacement");
  f->req.range_count = 1;
  f->req.ranges = f->ranges;
  f->ranges[0].after = 10;
  f->ranges[0].through = 140;
  f->req.has_recovery = 1;
  f->req.recovery = "secret marker";
  f->req.recovery_anchor = LC_U64_MAX;
  check(f, edit(f, "{\"yes\":true}"));
  lc_collection_metadata_req_init(&metadata);
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 0;
  check(f, lc_collection_entry_metadata(f->collection,
                                        f->result.replacement.position,
                                        &metadata, &updated, &f->error));
  scan(f, NULL);
  assert_int_equal(f->seen, 21);
  close_all(f);
  f->compressed = 0;
  open_all(f, 0);
  scan(f, NULL);
  assert_int_equal(f->seen, 21);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, "secret marker");
  compact(f);
  close_all(f);
  f->compressed = 1;
  open_all(f, 0);
  scan(f, NULL);
  assert_int_equal(f->seen, 21);
  check(f, edit(f, "{\"yes\":true}"));
  assert_true(f->result.duplicate);
}
static void *worker(void *context) {
  fixture *f = context;
  seed(f, 75);
  prepare(f, "worker-cut");
  replacement(f, "worker-body");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 2;
  f->ranges[0].through = 74;
  check(f, edit(f, "{\"utf8\":\"å\"}"));
  scan(f, NULL);
  assert_int_equal(f->seen, 4);
  compact(f);
  return NULL;
}
static void test_default_worker_stack(void **state) {
  fixture *f = *state;
  pthread_t thread;
  assert_int_equal(pthread_create(&thread, NULL, worker, f), 0);
  assert_int_equal(pthread_join(thread, NULL), 0);
}

typedef struct input {
  lc_source pub;
  size_t offset, length, chunk;
  int fail, resets, closes;
} input;
static size_t input_read(lc_source *source, void *bytes, size_t capacity,
                         lc_error *e) {
  input *in = (input *)source;
  unsigned char *out = bytes;
  static const char prefix[] = "{\"s\":\"";
  size_t n = in->length + 8 - in->offset, i, at;
  if (n > capacity)
    n = capacity;
  if (n > in->chunk)
    n = in->chunk;
  for (i = 0; i < n; ++i) {
    at = in->offset + i;
    out[i] = at < 6                 ? (unsigned char)prefix[at]
             : at < in->length + 6  ? ((at - 6) % 2 ? 0xa5 : 0xc3)
             : at == in->length + 6 ? '"'
                                    : '}';
  }
  in->offset += n;
  if (in->fail)
    lc_error_set(e, LC_ERR_TIMEOUT, 0, "injected partial input failure", NULL,
                 NULL, "test");
  return n;
}
static int input_reset(lc_source *source, lc_error *e) {
  input *in = (input *)source;
  (void)e;
  ++in->resets;
  return LC_ERR_INVALID;
}
static void input_close(lc_source *source) {
  input *in = (input *)source;
  ++in->closes;
}
static void test_limits_streaming_and_retention(void **state) {
  fixture *f = *state;
  input in = {0};
  lc_collection_delete_res deleted;
  lc_collection_edit_status_res status;
  char descriptor[LC_COLLECTION_RECOVERY_MAX_BYTES + 2];
  seed(f, 128);
  prepare(f, "maximum");
  replacement(f, "big-body");
  for (size_t i = 0; i < LC_COLLECTION_EDIT_MAX_RANGES; ++i) {
    f->ranges[i].after = i * 2;
    f->ranges[i].through = i * 2 + 1;
  }
  f->req.ranges = f->ranges;
  f->req.range_count = LC_COLLECTION_EDIT_MAX_RANGES;
  memset(descriptor, 'x', sizeof(descriptor) - 1);
  descriptor[sizeof(descriptor) - 1] = 0;
  f->req.has_recovery = 1;
  f->req.recovery = descriptor;
  f->req.recovery_anchor = LC_U64_MAX;
  assert_int_equal(edit(f, "{}"), LC_ERR_INVALID);
  clear(f);
  descriptor[LC_COLLECTION_RECOVERY_MAX_BYTES] = 0;
  f->replacement.has_retention_seconds = 1;
  f->replacement.retention_seconds = 60;
  in.pub.read = input_read;
  in.pub.reset = input_reset;
  in.pub.close = input_close;
  in.chunk = 4093;
  in.length = 1024 * 1024;
  in.fail = 1;
  assert_int_equal(lc_collection_edit(f->collection, &f->req, &in.pub,
                                      &f->result, &f->error),
                   LC_ERR_TIMEOUT);
  clear(f);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_int_equal(f->head.live_count, 128);
  in.fail = 0;
  in.offset = 0;
  f->max_allocation = 0;
  check(f, lc_collection_edit(f->collection, &f->req, &in.pub, &f->result,
                              &f->error));
  assert_int_equal(f->result.deleted_count, 64);
  assert_int_equal(f->result.live_count, 65);
  assert_true(f->max_allocation < 256 * 1024);
  assert_int_equal(in.resets, 0);
  assert_int_equal(in.closes, 0);
  /* Publication uses its original bounded control, while observation/reopen
   * decodes the stored receipt. Both paths must expose the same outcome. */
  close_all(f);
  open_all(f, 0);
  check(f, lc_collection_edit_status(f->collection, "maximum", &status,
                                     &f->error));
  assert_true(status.found && status.result.published);
  assert_string_equal(status.result.head_etag, f->result.head_etag);
  assert_int_equal(status.result.deleted_count, f->result.deleted_count);
  assert_int_equal(status.result.live_count, f->result.live_count);
  assert_int_equal(status.result.upper_position, f->result.upper_position);
  assert_int_equal(status.result.replacement.position,
                   f->result.replacement.position);
  assert_int_equal(status.result.replacement.bytes,
                   f->result.replacement.bytes);
  assert_string_equal(status.result.replacement.etag,
                      f->result.replacement.etag);
  assert_true(status.result.replacement.retention_expires_at_unix ==
              f->result.replacement.retention_expires_at_unix);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, descriptor);
  check(f, lc_collection_expire_entry(
               f->collection, f->result.replacement.position,
               f->result.replacement.retention_expires_at_unix + 1, &deleted,
               &f->error));
  assert_true(deleted.deleted);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_false(f->head.recovery_anchor_available);
  check(f, lc_collection_edit_status(f->collection, "maximum", &status,
                                     &f->error));
  assert_true(status.found);
  in.offset = 0;
  check(f, lc_collection_edit(f->collection, &f->req, &in.pub, &f->result,
                              &f->error));
  assert_true(f->result.duplicate);
  assert_false(f->result.replacement
                   .deleted); /* Original result, not refreshed state. */
}
static void test_allocation_failures(void **state) {
  fixture *f = *state;
  lc_collection_edit_status_res status;
  int successes = 0;
  seed(f, 2);
  for (long budget = 0; budget < 96 && successes < 3; ++budget) {
    char op[40];
    snprintf(op, sizeof(op), "allocation-%ld", budget);
    prepare(f, op);
    replacement(f, op);
    f->req.ranges = f->ranges;
    f->req.range_count = 1;
    f->ranges[0].after = 0;
    f->ranges[0].through = f->head.upper_position;
    f->fail_after = budget;
    int rc = edit(f, "{}");
    f->fail_after = -1;
    clear(f);
    if (rc == LC_OK)
      ++successes;
    else
      successes = 0;
    check(f, lc_collection_edit_status(f->collection, op, &status, &f->error));
    check(f, lc_collection_head(f->collection, &f->head, &f->error));
    if (status.found)
      assert_int_equal(f->head.live_count, 1);
    assert_true(f->head.live_count <= 2);
  }
  assert_int_equal(successes, 3);
}
static int waiting_metadata(void *context, const lc_collection_read_metadata *m,
                            lc_error *e) {
  int *pipes = context;
  (void)e;
  if (m->kind == LC_COLLECTION_READ_METADATA_ENTRY && m->position == 1) {
    char byte = 'r';
    assert_int_equal(write(pipes[0], &byte, 1), 1);
    assert_int_equal(read(pipes[1], &byte, 1), 1);
  }
  return LC_OK;
}
static void test_cross_process_pinned_view(void **state) {
  fixture *f = *state;
  int ready[2], go[2], status;
  pid_t pid;
  seed(f, 200);
  close_all(f);
  assert_int_equal(pipe(ready), 0);
  assert_int_equal(pipe(go), 0);
  pid = fork();
  assert_true(pid >= 0);
  if (!pid) {
    int channels[] = {ready[1], go[0]};
    lc_collection_read_opts opts;
    open_all(f, 0);
    lc_collection_read_opts_init(&opts);
    opts.on_metadata = waiting_metadata;
    opts.metadata_context = channels;
    check(f, lc_sink_to_memory(&f->sink, &f->error));
    check(f, lc_collection_read(f->collection, &opts, f->sink, &f->read,
                                &f->error));
    assert_int_equal(f->read.completed_records, 200);
    lc_sink_close(f->sink);
    lc_collection_read_res_cleanup(&f->read);
    close_all(f);
    _exit(0);
  }
  close(ready[1]);
  close(go[0]);
  char byte;
  assert_int_equal(read(ready[0], &byte, 1), 1);
  open_all(f, 0);
  prepare(f, "concurrent-cut");
  replacement(f, "concurrent-body");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 2;
  f->ranges[0].through = 199;
  check(f, edit(f, "{}"));
  scan(f, NULL);
  assert_int_equal(f->seen, 4);
  compact(f);
  assert_int_equal(write(go[1], "g", 1), 1);
  assert_int_equal(waitpid(pid, &status, 0), pid);
  assert_true(WIFEXITED(status));
  assert_int_equal(WEXITSTATUS(status), 0);
  close(ready[0]);
  close(go[1]);
  close_all(f);
  open_all(f, 0);
  scan(f, NULL);
  assert_int_equal(f->seen, 4);
}
static void test_empty_marker_and_recreation(void **state) {
  fixture *f = *state;
  lc_collection_config config;
  prepare(f, "marker-only");
  f->req.has_recovery = 1;
  f->req.recovery = "empty baseline";
  check(f, edit(f, NULL));
  compact(f);
  close_all(f);
  open_all(f, 0);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, "empty baseline");
  strcpy(f->token, f->head.etag);
  check(f, lc_collection_drop(f->collection, &f->error));
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_collection_config_init(&config);
  config.name = "events";
  check(f, lc_client_create_collection(f->client, &config, &f->collection,
                                       &f->error));
  assert_int_equal(edit(f, NULL), LC_ERR_INVALID);
  clear(f);
  prepare(f, "marker-only");
  check(f, edit(f, NULL));
  assert_false(f->result.duplicate);
}

static int failing_sync(int fd) {
  (void)fd;
  errno = EIO;
  return -1;
}
static void test_uncertain_sync(void **state) {
  fixture *f = *state;
  lc_collection_edit_status_res status;
  close_all(f);
  f->durable = 1;
  open_all(f, 0);
  seed(f, 3);
  prepare(f, "sync-loss");
  replacement(f, "sync-body");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 0;
  f->ranges[0].through = 3;
  lc_pouch_test_sync_fd = failing_sync;
  assert_int_equal(edit(f, "{}"), LC_ERR_TRANSPORT);
  lc_pouch_test_sync_fd = NULL;
  clear(f);
  check(f, lc_collection_edit_status(f->collection, "sync-loss", &status,
                                     &f->error));
  assert_true(status.found);
  check(f, edit(f, "{}"));
  assert_true(f->result.duplicate);
  close_all(f);
  open_all(f, 0);
  scan(f, "{}\n");
}
typedef struct takeover_input {
  lc_source pub;
  lc_collection *public, *replacement;
  size_t offset;
  int rc;
} takeover_input;
static void *takeover_worker(void *context) {
  takeover_input *in = context;
  lc_collection_info info;
  lc_collection_lease_req req;
  lc_error e;
  lc_error_init(&e);
  lc_collection_lease_req_init(&req);
  req.owner = "recovery";
  in->rc = lc_collection_describe(in->public, &info, &e);
  if (in->rc == LC_OK)
    in->rc = lc_collection_takeover(in->public, &req, info.lease.lease_id,
                                    &in->replacement, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static size_t takeover_read(lc_source *source, void *bytes, size_t capacity,
                            lc_error *e) {
  takeover_input *in = (takeover_input *)source;
  pthread_t worker_thread;
  (void)e;
  if (in->offset)
    return 0;
  assert_true(capacity >= 2);
  memcpy(bytes, "{}", 2);
  in->offset = 2;
  assert_int_equal(pthread_create(&worker_thread, NULL, takeover_worker, in),
                   0);
  assert_int_equal(pthread_join(worker_thread, NULL), 0);
  assert_int_equal(in->rc, LC_OK);
  return 2;
}
static void test_lease_fencing_and_private_head(void **state) {
  fixture *f = *state;
  takeover_input in = {0};
  lc_collection_lease_req lease;
  lc_collection_lease_info ownership;
  lc_collection_head_res *head = calloc(1, sizeof(*head));
  lc_collection *public = f->collection;
  assert_non_null(head);
  lc_collection_lease_req_init(&lease);
  lease.owner = "writer";
  lease.private_read = 1;
  check(f, lc_collection_acquire(public, &lease, &f->collection, &f->error));
  prepare(f, "fenced");
  replacement(f, "fenced-body");
  assert_int_equal(lc_collection_head(public, head, &f->error), LC_ERR_INVALID);
  clear(f);
  check(f, lc_collection_keepalive(f->collection, 60, &ownership, &f->error));
  check(f, lc_collection_head(f->collection, head, &f->error));
  assert_string_equal(head->etag, f->token);
  in.pub.read = takeover_read;
  in.public = public;
  assert_int_equal(lc_collection_edit(f->collection, &f->req, &in.pub,
                                      &f->result, &f->error),
                   LC_ERR_INVALID);
  clear(f);
  lc_collection_close(f->collection);
  f->collection = in.replacement;
  scan(f, "");
  prepare(f, "fenced");
  replacement(f, "fenced-body");
  check(f, edit(f, "{}"));
  check(f, lc_collection_release(f->collection, &f->error));
  f->collection = public;
  scan(f, "{}\n");
  free(head);
}
enum retry_action {
  RETRY_TAKEOVER,
  RETRY_EXPIRE,
  RETRY_RELEASE,
  RETRY_ACQUIRE,
  RETRY_ACQUIRE_RELEASE,
  RETRY_CLOCK_FAILURE,
  RETRY_RENEW
};
typedef struct retry_input {
  lc_source pub;
  fixture *f;
  lc_collection *public;
  enum retry_action action;
  unsigned int calls;
  int rc;
} retry_input;
static int retry_clock(clockid_t id, struct timespec *out, void *context) {
  fixture *f = context;
  (void)id;
  if (f->clock_failed) {
    errno = EIO;
    return -1;
  }
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}
static void *retry_control(void *context) {
  retry_input *in = context;
  lc_collection_lease_req req = {"next-owner", 60, 1};
  lc_collection_lease_info renewed;
  lc_collection_info info;
  lc_error e;
  lc_error_init(&e);
  if (in->action == RETRY_RELEASE)
    in->rc = lc_collection_release(in->f->collection, &e);
  else if (in->action == RETRY_RENEW)
    in->rc = lc_collection_keepalive(in->f->collection, 60, &renewed, &e);
  else if (in->action == RETRY_TAKEOVER) {
    in->rc = lc_collection_describe(in->public, &info, &e);
    if (in->rc == LC_OK)
      in->rc = lc_collection_takeover(in->public, &req, info.lease.lease_id,
                                      &in->f->next_owner, &e);
  } else {
    in->rc = lc_collection_acquire(in->public, &req, &in->f->next_owner, &e);
    if (in->rc == LC_OK && in->action == RETRY_ACQUIRE_RELEASE) {
      in->rc = lc_collection_release(in->f->next_owner, &e);
      if (in->rc == LC_OK)
        in->f->next_owner = NULL;
    }
  }
  lc_error_cleanup(&e);
  return NULL;
}
static size_t retry_read(lc_source *source, void *bytes, size_t capacity,
                         lc_error *error) {
  retry_input *in = (retry_input *)source;
  pthread_t thread;
  (void)error;
  if (in->calls++ == 0) {
    assert_true(capacity >= 2);
    memcpy(bytes, "{}", 2);
    return 2;
  }
  assert_int_equal(in->calls, 2);
  if (in->action == RETRY_EXPIRE)
    in->f->now = 1030;
  else if (in->action == RETRY_CLOCK_FAILURE)
    in->f->clock_failed = 1;
  else {
    in->f->now = 1010;
    assert_int_equal(pthread_create(&thread, NULL, retry_control, in), 0);
    assert_int_equal(pthread_join(thread, NULL), 0);
    assert_int_equal(in->rc, LC_OK);
  }
  return 0;
}
static void test_committed_retry_ownership(void **state) {
  fixture *f = *state;
  lc_collection_config config;
  lc_collection_lease_req lease = {"initial-owner", 30, 1};
  lc_collection_edit_res original;
  lc_collection_edit_status_res status;
  lc_collection_append_status_res entry;
  lc_collection *public_receiver, *observer;
  char name[48], current_head[65];
  retry_input in;
  int rc, unowned;
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_pouch_test_queue_clock_gettime = retry_clock;
  lc_pouch_test_queue_time_context = f;
  for (int action = RETRY_TAKEOVER; action <= RETRY_RENEW; ++action) {
    f->now = 1000;
    f->clock_failed = 0;
    snprintf(name, sizeof(name), "retry-%d", action);
    lc_collection_config_init(&config);
    config.name = name;
    check(f, lc_client_create_collection(f->client, &config, &f->collection,
                                         &f->error));
    public_receiver = f->collection;
    unowned = action == RETRY_ACQUIRE || action == RETRY_ACQUIRE_RELEASE;
    if (!unowned) {
      f->peer = public_receiver;
      f->collection = NULL;
      check(f, lc_collection_acquire(public_receiver, &lease, &f->collection,
                                     &f->error));
    }
    prepare(f, "original");
    replacement(f, "original-body");
    f->req.has_recovery = 1;
    f->req.recovery = "original-marker";
    check(f, edit(f, "{}"));
    original = f->result;
    append(f, "later", "{\"n\":2}");
    check(f, lc_collection_head(f->collection, &f->head, &f->error));
    strcpy(current_head, f->head.etag);
    memset(&in, 0, sizeof(in));
    in.pub.read = retry_read;
    in.f = f;
    in.public = public_receiver;
    in.action = (enum retry_action)action;
    rc = lc_collection_edit(f->collection, &f->req, &in.pub, &f->result,
                            &f->error);
    if (action == RETRY_RELEASE)
      f->collection =
          NULL; /* release consumes the receiver; the call pinned it. */
    f->clock_failed = 0;
    assert_int_equal(in.calls, 2);
    assert_int_equal(rc, action == RETRY_RENEW           ? LC_OK
                         : action == RETRY_CLOCK_FAILURE ? LC_ERR_TRANSPORT
                                                         : LC_ERR_INVALID);
    if (rc != LC_OK) {
      if (action != RETRY_CLOCK_FAILURE)
        assert_string_equal(f->error.server_code,
                            action == RETRY_ACQUIRE_RELEASE ? "lease_changed"
                            : action == RETRY_ACQUIRE ? "collection_leased"
                                                      : "lease_lost");
      assert_false(f->result.published);
      assert_false(f->result.duplicate);
      assert_string_equal(f->result.head_etag, "");
    }
    clear(f);
    if (action == RETRY_EXPIRE)
      check(f, lc_collection_acquire(public_receiver, &lease, &f->next_owner,
                                     &f->error));
    observer = f->next_owner   ? f->next_owner
               : f->collection ? f->collection
                               : public_receiver;
    check(f,
          lc_collection_edit_status(observer, "original", &status, &f->error));
    assert_true(status.found);
    assert_string_equal(status.result.head_etag, original.head_etag);
    check(f, lc_source_from_memory("{}", 2, &f->source, &f->error));
    check(f, lc_collection_edit(observer, &f->req, f->source, &f->result,
                                &f->error));
    lc_source_close(f->source);
    f->source = NULL;
    assert_true(f->result.duplicate && f->result.published);
    assert_string_equal(f->result.head_etag, original.head_etag);
    assert_int_equal(f->result.live_count, original.live_count);
    assert_int_equal(f->result.upper_position, original.upper_position);
    check(f, lc_collection_head(observer, &f->head, &f->error));
    assert_string_equal(f->head.etag, current_head);
    assert_string_equal(f->head.recovery, "original-marker");
    assert_int_equal(f->head.live_count, 2);
    check(f, lc_collection_append_status(observer, "original-body", &entry,
                                         &f->error));
    assert_int_equal(entry.state, LC_COLLECTION_APPEND_COMMITTED);
    assert_string_equal(entry.entry.etag, original.replacement.etag);
    lc_collection_close(f->next_owner);
    f->next_owner = NULL;
    lc_collection_close(f->peer);
    f->peer = NULL;
    lc_collection_close(f->collection);
    f->collection = NULL;
  }
}
static void test_committed_retry_ownership_encrypted(void **state) {
  fixture *f = *state;
  close_all(f);
  f->durable = 1;
  open_all(f, 0);
  test_committed_retry_ownership(state);
}

static void test_conversion_rejects_truncated_edit(void **state) {
  fixture *f = *state;
  lc_collection_handle *h;
  char *path, leaf[64];
  unsigned char last;
  off_t end;
  int fd;
  seed(f, 4);
  prepare(f, "published");
  replacement(f, "published-body");
  check(f, edit(f, "{}"));
  h = (lc_collection_handle *)f->collection;
  check(f,
        lc_collection_record_path(h, h->head.active, &path, leaf, &f->error));
  end = lseek(h->active_fd, 0, SEEK_END);
  assert_true(end > 0);
  assert_int_equal(pread(h->active_fd, &last, 1, end - 1), 1);
  assert_int_equal(ftruncate(h->active_fd, end - 1), 0);
  close_all(f);
  f->compressed = 0;
  assert_int_equal(open_client(f), LC_ERR_PROTOCOL);
  clear(f);
  assert_null(f->client);
  fd = open(path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(pwrite(fd, &last, 1, end - 1), 1);
  close(fd);
  lc_free_with_allocator(&f->allocator, path);
  open_all(f, 0);
  scan(f, NULL);
  assert_int_equal(f->seen, 5);
}

static void test_nosync_lost_edit(void **state) {
  fixture *f = *state;
  lc_collection_handle *h;
  lc_collection_edit_status_res status;
  off_t durable_end;
  seed(f, 4);
  close_all(f);
  open_all(f, 0); /* Explicit durable baseline. */
  h = (lc_collection_handle *)f->collection;
  durable_end = lseek(h->active_fd, 0, SEEK_END);
  assert_true(durable_end > 0);
  prepare(f, "lost");
  replacement(f, "lost-body");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 0;
  f->ranges[0].through = 4;
  f->req.has_recovery = 1;
  f->req.recovery = "not durable";
  f->req.recovery_anchor = LC_U64_MAX;
  check(f, edit(f, "{}"));
  /* Simulate loss of precisely the bytes not covered by a successful sync.
   * Leave the newer derived index intact: recovery must reject its watermark.
   */
  assert_int_equal(ftruncate(h->active_fd, durable_end), 0);
  close_all(f);
  open_all(f, 0);
  scan(f, NULL);
  assert_int_equal(f->seen, 4);
  check(f,
        lc_collection_edit_status(f->collection, "lost", &status, &f->error));
  assert_false(status.found);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, "");
}
typedef struct contender {
  lc_collection *handle;
  const char *token, *id;
  pthread_barrier_t *gate;
  lc_collection_edit_res result;
  lc_error error;
  int rc;
} contender;
static void *contend(void *context) {
  contender *c = context;
  lc_collection_edit_req req;
  lc_collection_edit_range range = {10, 130};
  lc_collection_edit_req_init(&req);
  req.operation_id = c->id;
  req.expected_head = c->token;
  req.ranges = &range;
  req.range_count = 1;
  lc_error_init(&c->error);
  pthread_barrier_wait(c->gate);
  c->rc = lc_collection_edit(c->handle, &req, NULL, &c->result, &c->error);
  return NULL;
}
static void test_competing_edits(void **state) {
  fixture *f = *state;
  contender contenders[2] = {{0}};
  pthread_t threads[2];
  pthread_barrier_t gate;
  lc_collection_config config;
  lc_collection_edit_status_res status;
  size_t winner;
  seed(f, 130);
  prepare(f, "observation");
  lc_collection_config_init(&config);
  config.name = "events";
  assert_int_equal(pthread_barrier_init(&gate, NULL, 2), 0);
  for (size_t i = 0; i < 2; ++i) {
    check(f, lc_client_open_collection(f->client, &config,
                                       &contenders[i].handle, &f->error));
    contenders[i].token = f->token;
    contenders[i].id = i ? "second" : "first";
    contenders[i].gate = &gate;
  }
  for (size_t i = 0; i < 2; ++i)
    assert_int_equal(pthread_create(&threads[i], NULL, contend, &contenders[i]),
                     0);
  for (size_t i = 0; i < 2; ++i)
    assert_int_equal(pthread_join(threads[i], NULL), 0);
  winner = contenders[0].rc == LC_OK ? 0 : 1;
  assert_int_equal(contenders[winner].rc, LC_OK);
  assert_int_equal(contenders[1 - winner].rc, LC_ERR_INVALID);
  assert_string_equal(contenders[1 - winner].error.server_code,
                      "collection_changed");
  assert_string_equal(contenders[1 - winner].result.head_etag,
                      contenders[winner].result.head_etag);
  check(f, lc_collection_edit_status(f->collection, contenders[1 - winner].id,
                                     &status, &f->error));
  assert_false(status.found);
  scan(f, NULL);
  assert_int_equal(f->seen, 10);
  for (size_t i = 0; i < 2; ++i) {
    lc_collection_close(contenders[i].handle);
    lc_error_cleanup(&contenders[i].error);
  }
  assert_int_equal(pthread_barrier_destroy(&gate), 0);
}
static void test_disjoint_first_page_failure(void **state) {
  fixture *f = *state;
  seed(f, 130);
  prepare(f, "disjoint-failure");
  f->req.ranges = f->ranges;
  f->req.range_count = 2;
  f->ranges[0].after = 0;
  f->ranges[0].through = 1;
  f->ranges[1].after = 100;
  f->ranges[1].through = 130;
  f->phase = LC_COLLECTION_EDIT_PAGE;
  f->fail_once = 1;
  lc_collection_edit_test_hook = fail_phase;
  lc_collection_edit_test_context = f;
  assert_int_equal(edit(f, NULL), LC_ERR_TRANSPORT);
  clear(f);
  lc_collection_edit_test_hook = NULL;
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_int_equal(f->head.live_count, 99);
}
typedef struct changing_input {
  lc_source pub;
  fixture *fixture;
  char *recovery;
  int done;
} changing_input;
static size_t changing_read(lc_source *source, void *bytes, size_t capacity,
                            lc_error *error) {
  changing_input *in = (changing_input *)source;
  (void)error;
  if (in->done)
    return 0;
  assert_true(capacity >= 2);
  memcpy(bytes, "{}", 2);
  in->done = 1;
  in->fixture->req.operation_id = "next-operation";
  in->fixture->req.range_count = 0;
  in->fixture->ranges[0].through = 5;
  memset(in->fixture->token, '0', 64);
  in->fixture->replacement.append_id = "next-body";
  if (in->recovery)
    strcpy(in->recovery, "next marker");
  in->fixture->req.has_recovery = 0;
  in->fixture->replacement.has_retention_seconds = 0;
  return 2;
}
static void test_request_snapshot_across_source_callbacks(void **state) {
  fixture *f = *state;
  changing_input in = {0};
  char recovery[32] = "committed marker";
  lc_collection_edit_status_res status;
  seed(f, 5);
  prepare(f, "stable-operation");
  replacement(f, "stable-body");
  f->req.ranges = f->ranges;
  f->req.range_count = 1;
  f->ranges[0].after = 0;
  f->ranges[0].through = 2;
  f->req.has_recovery = 1;
  f->req.recovery = recovery;
  in.recovery = recovery;
  f->req.recovery_anchor = LC_U64_MAX;
  f->replacement.has_retention_seconds = 1;
  f->replacement.retention_seconds = 60;
  in.pub.read = changing_read;
  in.fixture = f;
  check(f, lc_collection_edit(f->collection, &f->req, &in.pub, &f->result,
                              &f->error));
  assert_int_equal(f->result.deleted_count, 2);
  assert_int_equal(f->result.live_count, 4);
  assert_true(f->result.replacement.retention_expires_at_unix > 0);
  check(f, lc_collection_edit_status(f->collection, "stable-operation", &status,
                                     &f->error));
  assert_true(status.found);
  check(f, lc_collection_edit_status(f->collection, "next-operation", &status,
                                     &f->error));
  assert_false(status.found);
  f->req.operation_id = "stable-operation";
  f->req.expected_head = f->head.etag;
  f->req.range_count = 1;
  f->ranges[0].through = 2;
  f->req.has_recovery = 1;
  f->req.recovery = "committed marker";
  f->replacement.append_id = "stable-body";
  f->replacement.has_retention_seconds = 1;
  check(f, edit(f, "{}"));
  assert_true(f->result.duplicate);
  check(f, lc_collection_head(f->collection, &f->head, &f->error));
  assert_string_equal(f->head.recovery, "committed marker");
}
int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(test_committed_retry_ownership, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_committed_retry_ownership_encrypted,
                                      setup_encrypted, teardown),
      cmocka_unit_test_setup_teardown(
          test_request_snapshot_across_source_callbacks, setup, teardown),
      cmocka_unit_test_setup_teardown(test_disjoint_first_page_failure, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_nosync_lost_edit, setup, teardown),
      cmocka_unit_test_setup_teardown(test_competing_edits, setup, teardown),
      cmocka_unit_test_setup_teardown(test_uncertain_sync, setup, teardown),
      cmocka_unit_test_setup_teardown(test_lease_fencing_and_private_head,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_conversion_rejects_truncated_edit,
                                      setup_encrypted, teardown),
      cmocka_unit_test_setup_teardown(test_limits_streaming_and_retention,
                                      setup_encrypted, teardown),
      cmocka_unit_test_setup_teardown(test_allocation_failures, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_cross_process_pinned_view,
                                      setup_encrypted, teardown),
      cmocka_unit_test_setup_teardown(test_empty_marker_and_recreation, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_ranges_and_receipts, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_validation_and_heads, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_failure_recovery, setup, teardown),
      cmocka_unit_test_setup_teardown(test_crash_recovery, setup, teardown),
      cmocka_unit_test_setup_teardown(test_codec_conversion, setup_encrypted,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_default_worker_stack, setup,
                                      teardown)};
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return EXIT_FAILURE;
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(*tests), argc, argv,
                           NULL, NULL);
}
