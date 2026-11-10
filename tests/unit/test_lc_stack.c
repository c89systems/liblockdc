#include "../support/lc_test_tmp.h"
/* Establish the same JSON receiver feature layout as production before any
 * public header includes lonejson.h. */
#include "lc_api_internal.h"
#include "lc_collection_internal.h"
#include "lc_mutate_stream.h"
#include "lc_pouch.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_recode.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
#define DEPTH 1024U
#define STACK_PREFIX LC_STACK_TMP_PARENT "/liblockdc-stack-"
static int fail_after = -1;
static size_t fail_size;
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *allocator,
                                     size_t size) {
  if (fail_size != 0 && size == fail_size) {
    fail_size = 0;
    return NULL;
  }
  if (fail_after == 0) {
    fail_after = -1;
    return NULL;
  }
  if (fail_after > 0)
    --fail_after;
  return __real_lc_alloc_with_allocator(allocator, size);
}
static void mutation(void) {
  char *expr = malloc(DEPTH * 4U + 64U), *at = expr;
  const char *expressions[2];
  lc_mutation_plan *plan = NULL;
  lc_error error;
  size_t i;
  CHECK(expr != NULL);
  for (i = 0; i < DEPTH; ++i) {
    memcpy(at, "/a{", 3);
    at += 3;
  }
  memcpy(at, "/v=1,/w=2", 9);
  at += 9;
  for (i = 0; i < DEPTH; ++i)
    *at++ = '}';
  *at = 0;
  expressions[0] = expr;
  expressions[1] = "/after=3";
  lc_error_init(&error);
  CHECK(lc_mutation_plan_build(expressions, 2, NULL, &plan, &error) == LC_OK);
  CHECK(plan->count == 3 && plan->items[0].path_segment_count == DEPTH + 1);
  CHECK(strcmp(plan->items[0].path_segments[DEPTH], "v") == 0);
  CHECK(strcmp(plan->items[1].path_segments[DEPTH], "w") == 0);
  CHECK(strcmp(plan->items[2].path_segments[0], "after") == 0);
  lc_mutation_plan_close(plan);
  plan = NULL;
  for (i = 0; i < 3; ++i) {
    fail_after = (int)i;
    CHECK(lc_mutation_plan_build(expressions, 2, NULL, &plan, &error) != LC_OK);
    fail_after = -1;
    CHECK(error.code == LC_ERR_NOMEM && plan == NULL);
    lc_error_cleanup(&error);
  }
  /* An error after a successful expansion must discard the partial plan. */
  expressions[1] = "/invalid";
  CHECK(lc_mutation_plan_build(expressions, 2, NULL, &plan, &error) ==
        LC_ERR_INVALID);
  CHECK(plan == NULL);
  lc_error_cleanup(&error);
  free(expr);
}
typedef struct byte_source {
  lc_source source;
  const char *data;
  size_t length, offset, fail_at, chunk;
} byte_source;
static size_t source_read(lc_source *source, void *buffer, size_t count,
                          lc_error *error) {
  byte_source *s = source->impl;
  size_t n;
  if (s->offset >= s->fail_at) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0, "fixture read failure", NULL, NULL,
                 NULL);
    return 0;
  }
  n = s->length - s->offset;
  if (n > count)
    n = count;
  if (n > s->chunk)
    n = s->chunk;
  if (n > s->fail_at - s->offset)
    n = s->fail_at - s->offset;
  memcpy(buffer, s->data + s->offset, n);
  s->offset += n;
  return n;
}
static int query(const char *body, const char *field, size_t chunk,
                 size_t fail_at, lc_error *error) {
  byte_source source = {0};
  int decided = 0, matched = 0, rc;
  source.source.read = source_read;
  source.source.impl = &source;
  source.data = body;
  source.length = strlen(body);
  source.chunk = chunk;
  source.fail_at = fail_at;
  rc = lc_pouch_test_scalar_scan(&source.source, field, "1", 'n', &decided,
                                 &matched, error);
  if (rc == LC_OK)
    CHECK(decided && matched);
  return rc;
}
static void queries(void) {
  char *body = malloc(DEPTH * 8U + 80U), *field = malloc(DEPTH * 3U + 20U),
       *at = body;
  lc_error error;
  size_t i;
  int rc;
  CHECK(body && field);
  lc_error_init(&error);
  memcpy(at, "{\"skip\":", 8);
  at += 8;
  for (i = 0; i < DEPTH; ++i) {
    if (i % 2 == 0)
      *at++ = '[';
    else {
      memcpy(at, "{\"k\":", 5);
      at += 5;
    }
  }
  *at++ = '0';
  for (i = DEPTH; i > 0; --i)
    *at++ = (i - 1) % 2 == 0 ? ']' : '}';
  memcpy(at, ",\"wanted\":1}", 13);
  CHECK(query(body, "/wanted", 1, (size_t)-1, &error) == LC_OK);
  CHECK(query(body, "/wanted", 97, (size_t)-1, &error) == LC_OK);
  CHECK(query(body, "/wanted", 1, 70, &error) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&error);
  for (i = 0; i < 4; ++i) {
    fail_after = (int)i;
    rc = query(body, "/wanted", 1, (size_t)-1, &error);
    fail_after = -1;
    CHECK(rc == LC_ERR_NOMEM);
    lc_error_cleanup(&error);
  }
  /* A mismatched deep closing delimiter is still a normal parse failure. */
  body[8U + (DEPTH / 2U) * 6U + 1U] = '[';
  CHECK(query(body, "/wanted", 1, (size_t)-1, &error) == LC_ERR_INVALID);
  lc_error_cleanup(&error);
  at = body;
  for (i = 0; i < DEPTH; ++i) {
    memcpy(at, "{\"x\":", 5);
    at += 5;
    field[i * 2] = '/';
    field[i * 2 + 1] = 'x';
  }
  memcpy(at, "{\"wanted\":1}", 12);
  at += 12;
  for (i = 0; i < DEPTH; ++i)
    *at++ = '}';
  *at = 0;
  strcpy(field + DEPTH * 2, "/wanted");
  CHECK(query(body, field, 1, (size_t)-1, &error) == LC_OK);
  free(body);
  free(field);
  lc_error_cleanup(&error);
}
static unsigned init_calls, reset_calls;
static lonejson_status fake_status;
static lonejson_status fake_init(lonejson *runtime, const lonejson_map *map,
                                 void *value, lonejson_error *error) {
  (void)runtime;
  (void)map;
  (void)value;
  ++init_calls;
  if (error != NULL)
    error->code = fake_status;
  return fake_status;
}
static lonejson_status fake_reset(lonejson *runtime, const lonejson_map *map,
                                  void *value, lonejson_error *error) {
  (void)runtime;
  (void)map;
  (void)value;
  ++reset_calls;
  if (error != NULL)
    error->code = fake_status;
  return fake_status;
}
static size_t native_live;
static void *native_malloc(void *context, size_t size) {
  void *bytes = *(int *)context ? NULL : malloc(size);
  if (bytes != NULL)
    ++native_live;
  return bytes;
}
static void *native_realloc(void *context, void *bytes, size_t size) {
  if (*(int *)context)
    return NULL;
  if (bytes == NULL)
    return native_malloc(context, size);
  if (size == 0) {
    free(bytes);
    --native_live;
    return NULL;
  }
  return realloc(bytes, size);
}
static void native_free(void *context, void *bytes) {
  (void)context;
  if (bytes != NULL)
    --native_live;
  free(bytes);
}
static void maps(void) {
  lonejson_map *maps = calloc(DEPTH + 1, sizeof(*maps));
  lonejson_field *fields = calloc(DEPTH + 1, sizeof(*fields));
  lonejson *runtime = calloc(1, sizeof(*runtime));
  lonejson *native;
  lonejson_config *config = malloc(sizeof(*config));
  lonejson_allocator native_allocator = {0};
  lonejson_error json_error;
  int native_fail = 0;
  char *value = NULL;
  lc_error error;
  size_t i;
  CHECK(maps && fields && runtime && config);
  runtime->init = fake_init;
  runtime->reset = fake_reset;
  for (i = 0; i <= DEPTH; ++i) {
    maps[i].fields = &fields[i];
    maps[i].field_count = 1;
    maps[i].struct_size = sizeof(value);
    fields[i].kind = LONEJSON_FIELD_KIND_OBJECT;
    fields[i].json_key = "x";
    fields[i].json_key_len = 1;
    fields[i].json_key_first = fields[i].json_key_last = 'x';
    fields[i].submap = i < DEPTH ? &maps[i + 1] : NULL;
  }
  fields[DEPTH].kind = LONEJSON_FIELD_KIND_STRING;
  fields[DEPTH].storage = LONEJSON_STORAGE_DYNAMIC;
  fields[DEPTH].json_key = "label";
  fields[DEPTH].json_key_len = 5;
  fields[DEPTH].json_key_first = fields[DEPTH].json_key_last = 'l';
  lc_error_init(&error);
  CHECK(lc_lonejson_prepare_parse_destination(runtime, maps, &value, &error) ==
        LC_OK);
  CHECK(init_calls == 1 && reset_calls == 0);
  value = "owned by fixture";
  CHECK(lc_lonejson_prepare_parse_destination(runtime, maps, &value, &error) ==
        LC_OK);
  CHECK(reset_calls == 1);
  fail_after = 0;
  CHECK(lc_lonejson_prepare_parse_destination(runtime, maps, &value, &error) ==
        LC_ERR_NOMEM);
  fail_after = -1;
  CHECK(reset_calls == 1 && strcmp(value, "owned by fixture") == 0);
  lc_error_cleanup(&error);
  fake_status = LONEJSON_STATUS_ALLOCATION_FAILED;
  CHECK(lc_lonejson_prepare_parse_destination(runtime, maps, &value, &error) ==
        LC_ERR_NOMEM);
  CHECK(strcmp(value, "owned by fixture") == 0);
  value = NULL;
  CHECK(lc_lonejson_prepare_parse_destination(runtime, maps, &value, &error) ==
        LC_ERR_NOMEM);
  lonejson_error_init(&json_error);
  /* The fake has no parse method: a failed init must return before parsing. */
  CHECK(
      lc_lonejson_parse_cstr_value(runtime, maps, &value, "{}", &json_error) ==
      LONEJSON_STATUS_ALLOCATION_FAILED);
  fake_status = LONEJSON_STATUS_OK;
  lc_error_cleanup(&error);

  /* Exercise the released native lifecycle through its public API on this
   * default worker, without substituting the dependency's map traversal. */
  *config = lonejson_default_config();
  config->clear_destination_by_default = 0;
  native_allocator.malloc_fn = native_malloc;
  native_allocator.realloc_fn = native_realloc;
  native_allocator.free_fn = native_free;
  native_allocator.ctx = &native_fail;
  config->allocator = &native_allocator;
  native = lonejson_new(config, &json_error);
  CHECK(native);
  fprintf(stderr, "stack: native init failure/retry\n");
  native_fail = 1;
  CHECK(lc_lonejson_prepare_parse_destination(native, maps, &value, &error) ==
        LC_ERR_NOMEM);
  CHECK(value == NULL);
  native_fail = 0;
  CHECK(lc_lonejson_prepare_parse_destination(native, maps, &value, &error) ==
        LC_OK);
  CHECK(native->parse_cstr(native, &maps[DEPTH], &value,
                           "{\"label\":\"retained\"}",
                           &json_error) == LONEJSON_STATUS_OK);
  fprintf(stderr, "stack: native reset failure/retry\n");
  native_fail = 1;
  CHECK(lc_lonejson_prepare_parse_destination(native, maps, &value, &error) ==
        LC_ERR_NOMEM);
  CHECK(strcmp(value, "retained") == 0);
  native_fail = 0;
  CHECK(lc_lonejson_prepare_parse_destination(native, maps, &value, &error) ==
        LC_OK);
  CHECK(value == NULL);
  CHECK(native->parse_cstr(native, &maps[DEPTH], &value,
                           "{\"label\":\"retained\"}",
                           &json_error) == LONEJSON_STATUS_OK);
  fprintf(stderr, "stack: native cleanup failure/retry\n");
  native_fail = 1;
  CHECK(native->cleanup(native, maps, &value, &json_error) ==
        LONEJSON_STATUS_ALLOCATION_FAILED);
  CHECK(strcmp(value, "retained") == 0);
  native_fail = 0;
  CHECK(native->cleanup(native, maps, &value, &json_error) ==
        LONEJSON_STATUS_OK);
  CHECK(value == NULL);
  native->free(native);
  CHECK(native_live == 0);
  fprintf(stderr, "stack: native lifecycle complete\n");
  free(config);
  lc_error_cleanup(&error);
  free(maps);
  free(fields);
  free(runtime);
}
static lonejson_status discard_json(void *user, const void *data, size_t count,
                                    lonejson_error *error) {
  (void)user;
  (void)data;
  (void)count;
  (void)error;
  return LONEJSON_STATUS_OK;
}
static lonejson_status clear_capture_modes(lonejson *runtime,
                                           const lonejson_map *map, void *value,
                                           lonejson_error *error) {
  lonejson_json_value *values = value;
  size_t i;
  (void)runtime;
  (void)error;
  for (i = 0; i < map->field_count; ++i) {
    values[i].parse_mode = LONEJSON_JSON_VALUE_PARSE_NONE;
    values[i].parse_sink = NULL;
    values[i].parse_sink_user = NULL;
    values[i].parse_visitor = NULL;
    values[i].parse_visitor_user = NULL;
  }
  ++reset_calls;
  return LONEJSON_STATUS_OK;
}
static void captures(void) {
  lonejson_json_value *values = calloc(6, sizeof(*values));
  lonejson_field fields[6] = {{0}};
  lonejson_map map = {0};
  lonejson *runtime = calloc(1, sizeof(*runtime));
  lonejson *real_runtime = lc_thread_lonejson_runtime();
  lonejson_value_visitor visitor = {0};
  lonejson_error json_error;
  lc_error error;
  size_t i;
  unsigned before = reset_calls;
  CHECK(values && runtime && real_runtime);
  runtime->reset = clear_capture_modes;
  map.fields = fields;
  map.field_count = 6;
  map.struct_size = 6 * sizeof(*values);
  lonejson_error_init(&json_error);
  lc_error_init(&error);
  for (i = 0; i < 6; ++i) {
    lonejson_json_value_init(real_runtime, &values[i]);
    fields[i].kind = LONEJSON_FIELD_KIND_JSON_VALUE;
    fields[i].struct_offset = i * sizeof(*values);
    if (i % 3 == 0)
      CHECK(values[i].methods->set_parse_sink(&values[i], discard_json,
                                              &fields[i], &json_error) ==
            LONEJSON_STATUS_OK);
    else if (i % 3 == 1)
      CHECK(values[i].methods->set_parse_visitor(&values[i], &visitor,
                                                 &fields[i], &json_error) ==
            LONEJSON_STATUS_OK);
    else
      CHECK(values[i].methods->enable_parse_capture(&values[i], &json_error) ==
            LONEJSON_STATUS_OK);
  }
  /* Spill failure must leave the destination and its callbacks untouched. */
  fail_after = 0;
  CHECK(lc_lonejson_prepare_parse_destination(runtime, &map, values, &error) ==
        LC_ERR_NOMEM);
  fail_after = -1;
  CHECK(reset_calls == before);
  CHECK(lc_lonejson_prepare_parse_destination(runtime, &map, values, &error) ==
        LC_OK);
  CHECK(reset_calls == before + 1);
  for (i = 0; i < 6; ++i) {
    if (i % 3 == 0) {
      CHECK(values[i].parse_mode == LONEJSON_JSON_VALUE_PARSE_SINK);
      CHECK(values[i].parse_sink == discard_json);
      CHECK(values[i].parse_sink_user == &fields[i]);
    } else if (i % 3 == 1) {
      CHECK(values[i].parse_mode == LONEJSON_JSON_VALUE_PARSE_VISITOR);
      CHECK(values[i].parse_visitor == &visitor);
      CHECK(values[i].parse_visitor_user == &fields[i]);
    } else
      CHECK(values[i].parse_mode == LONEJSON_JSON_VALUE_PARSE_CAPTURE);
    lonejson_json_value_cleanup(&values[i]);
  }
  lc_error_cleanup(&error);
  free(values);
  free(runtime);
}
static void directories(void) {
  char template_path[] = STACK_PREFIX "XXXXXX";
  char root[512], *namespace_path, *path, *leaf;
  lc_pouch_open_options options = {0};
  lc_pouch *pouch = NULL;
  lc_error error;
  lc_source *source = NULL;
  lc_pouch_state_write_result result = {0};
  FILE *file;
  size_t i, n;
  CHECK(lc_test_tmp_mkdtemp(template_path, root, sizeof(root), STACK_PREFIX));
  lc_error_init(&error);
  options.compression = "none";
  options.background_compaction_enabled_set = 1;
  CHECK(lc_pouch_open(root, NULL, &options, &pouch, &error) == LC_OK);
  CHECK(lc_source_from_memory("{}", 2, &source, &error) == LC_OK);
  CHECK(lc_pouch_state_write(pouch, "default", "one", source, NULL, &result,
                             &error) == LC_OK);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &result);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  CHECK(namespace_path);
  lc_pouch_close(pouch);
  pouch = NULL;
  path = malloc(strlen(namespace_path) + 300);
  CHECK(path);
  strcpy(path, namespace_path);
  n = strlen(path);
  for (i = 0; i < 128; ++i) {
    memcpy(path + n, "/d", 3);
    n += 2;
    CHECK(mkdir(path, 0700) == 0);
  }
  leaf = malloc(n + 16);
  CHECK(leaf);
  sprintf(leaf, "%s/marker", path);
  file = fopen(leaf, "wb");
  CHECK(file);
  CHECK(fwrite("hello", 1, 5, file) == 5);
  CHECK(fclose(file) == 0);
  options.compression = "zlib";
  CHECK(lc_pouch_open(root, NULL, &options, &pouch, &error) == LC_OK);
  lc_pouch_close(pouch);
  file = fopen(leaf, "rb");
  CHECK(file);
  CHECK(fgetc(file) == 'h');
  CHECK(fclose(file) == 0);
  options.compression = "none";
  CHECK(lc_pouch_open(root, NULL, &options, &pouch, &error) == LC_OK);
  lc_pouch_close(pouch);
  for (i = 0; i < 2; ++i) {
    fail_after = (int)i;
    CHECK(lc_pouch_recode_remove_tree(root, &error) == LC_ERR_NOMEM);
    fail_after = -1;
    lc_error_cleanup(&error);
  }
  CHECK(lc_pouch_recode_remove_tree(root, &error) == LC_OK);
  free(leaf);
  free(path);
  lc_free_with_allocator(NULL, namespace_path);
  lc_error_cleanup(&error);
}
static int count_bytes(lc_sink *sink, const void *data, size_t count,
                       lc_error *error) {
  size_t *written = sink->impl;
  (void)data;
  (void)error;
  *written += count;
  return 1;
}
static void collection_workspaces(void) {
  char path[] = STACK_PREFIX "XXXXXX", root[512], endpoint[640];
  const char *endpoints[] = {endpoint};
  lc_client *client = NULL;
  lc_collection *collection = NULL;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config collection_config = {0};
  lc_collection_append_req request = {"one"};
  lc_collection_append_res appended = {0};
  lc_collection_read_res result = {0};
  lc_source *source = NULL;
  lc_sink sink = {0};
  lc_error error;
  size_t written = 0;
  CHECK(lc_test_tmp_mkdtemp(path, root, sizeof(root), STACK_PREFIX));
  CHECK(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  lc_error_init(&error);
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_BACKGROUND_COMPACTION;
  config.pouch_settings = &settings;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  CHECK(lc_client_open(&config, &client, &error) == LC_OK);
  collection_config.name = "stack";
  CHECK(lc_client_create_collection(client, &collection_config, &collection,
                                    &error) == LC_OK);
  CHECK(lc_source_from_memory("{}", 2, &source, &error) == LC_OK);
  CHECK(lc_collection_append(collection, &request, source, &appended, &error) ==
        LC_OK);
  lc_source_close(source);
  sink.write = count_bytes;
  sink.impl = &written;
  fail_after = 0;
  CHECK(lc_collection_read(collection, NULL, &sink, &result, &error) ==
        LC_ERR_NOMEM);
  fail_after = -1;
  CHECK(error.code == LC_ERR_NOMEM && written == 0);
  CHECK(result.stop_reason == LC_COLLECTION_READ_ERROR && !result.complete);
  CHECK(lc_collection_read(collection, NULL, &sink, &result, &error) == LC_OK);
  CHECK(result.complete && written == 3);
  lc_collection_read_res_cleanup(&result);
  fail_size = LC_COLLECTION_ORDER_PAGE_ROWS * sizeof(lc_collection_order_row);
  CHECK(lc_collection_compact(collection, 1, 0, NULL, &error) == LC_ERR_NOMEM);
  CHECK(fail_size == 0 && strstr(error.message, "compaction row page") != NULL);
  CHECK(lc_collection_compact(collection, 1, 0, NULL, &error) == LC_OK);
  written = 0;
  CHECK(lc_collection_read(collection, NULL, &sink, &result, &error) == LC_OK);
  CHECK(result.complete && written == 3);
  lc_collection_read_res_cleanup(&result);
  lc_collection_close(collection);
  lc_client_close(client);
  CHECK(lc_pouch_recode_remove_tree(root, &error) == LC_OK);
  lc_error_cleanup(&error);
}
static void *worker(void *unused) {
  (void)unused;
  fprintf(stderr, "stack: mutation/query traversal\n");
  mutation();
  queries();
  fprintf(stderr, "stack: mapped lifecycle\n");
  maps();
  fprintf(stderr, "stack: capture preservation\n");
  captures();
  fprintf(stderr, "stack: directory conversion\n");
  directories();
  fprintf(stderr, "stack: collection workspaces\n");
  collection_workspaces();
  return NULL;
}
int main(void) {
  pthread_t thread;
  lc_error error;
  lc_error_init(&error);
  CHECK(lc_allocator_tracking_configure(1, &error) == LC_OK);
  CHECK(pthread_create(&thread, NULL, worker, NULL) == 0);
  CHECK(pthread_join(thread, NULL) == 0);
  lc_error_cleanup(&error);
  puts("default-worker nesting, error cleanup, and conversion checks passed");
  return 0;
}
