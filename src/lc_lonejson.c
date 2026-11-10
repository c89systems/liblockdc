#include "lc_api_internal.h"
#include "lc_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_LONEJSON_DEFAULT_LIMIT_BYTES LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT

static pthread_once_t lc_lonejson_thread_runtime_once = PTHREAD_ONCE_INIT;
static pthread_key_t lc_lonejson_thread_runtime_key;
static int lc_lonejson_thread_runtime_key_status = -1;
typedef struct lc_lonejson_thread_runtime {
  lonejson *runtime;
  struct lc_lonejson_thread_runtime *next;
} lc_lonejson_thread_runtime;
static lc_lonejson_thread_runtime *lc_lonejson_thread_runtimes;
static pthread_mutex_t lc_lonejson_runtime_mutex = PTHREAD_MUTEX_INITIALIZER;
static void lc_lonejson_runtime_lock(void) {
  (void)pthread_mutex_lock(&lc_lonejson_runtime_mutex);
}
static void lc_lonejson_runtime_unlock(void) {
  (void)pthread_mutex_unlock(&lc_lonejson_runtime_mutex);
}
#ifdef LOCKDC_TEST_BUILD
static pthread_key_t lc_lonejson_test_fail_thread_runtime_key;
static int lc_lonejson_test_fail_thread_runtime_key_status = -1;
static const char lc_lonejson_test_fail_thread_runtime_marker;
#endif

static const char *lc_lonejson_detail_message(const lonejson_error *error,
                                              const char *fallback) {
  if (error != NULL && error->message[0] != '\0') {
    return error->message;
  }
  return fallback;
}

static int lc_lonejson_status_to_error_code(lonejson_status status) {
  switch (status) {
  case LONEJSON_STATUS_OK:
    return LC_OK;
  case LONEJSON_STATUS_INVALID_ARGUMENT:
    return LC_ERR_INVALID;
  case LONEJSON_STATUS_ALLOCATION_FAILED:
    return LC_ERR_NOMEM;
  case LONEJSON_STATUS_CALLBACK_FAILED:
  case LONEJSON_STATUS_IO_ERROR:
  case LONEJSON_STATUS_INTERNAL_ERROR:
    return LC_ERR_TRANSPORT;
  case LONEJSON_STATUS_INVALID_JSON:
  case LONEJSON_STATUS_TYPE_MISMATCH:
  case LONEJSON_STATUS_MISSING_REQUIRED_FIELD:
  case LONEJSON_STATUS_DUPLICATE_FIELD:
  case LONEJSON_STATUS_OVERFLOW:
  case LONEJSON_STATUS_TRUNCATED:
  default:
    return LC_ERR_PROTOCOL;
  }
}

void lc_lonejson_config_init(lonejson_config *config, size_t limit) {
  if (config == NULL) {
    return;
  }
  *config = lonejson_default_config();
  config->clear_destination_by_default = 0;
  config->max_alloc_bytes = limit;
  config->max_dynamic_string_bytes = limit;
  config->json_value_max_total_bytes = limit;
  config->json_value_max_string_bytes = limit;
  config->json_value_max_key_bytes = limit;
  config->write_max_output_bytes = limit;
  config->spool_default.max_bytes = limit;
  config->spool_blob.max_bytes = limit;
  config->spool_large_text.max_bytes = limit;
}

static void lc_lonejson_thread_runtime_destroy(void *ptr) {
  lc_lonejson_thread_runtime *entry = ptr;
  lc_lonejson_thread_runtime **link;
  if (entry == NULL)
    return;
  lc_lonejson_runtime_lock();
  for (link = &lc_lonejson_thread_runtimes; *link != NULL;
       link = &(*link)->next) {
    if (*link == entry) {
      *link = entry->next;
      break;
    }
  }
  lc_lonejson_runtime_unlock();
  entry->runtime->free(entry->runtime);
  lc_free_with_allocator(NULL, entry);
}

static void lc_lonejson_thread_runtime_key_init(void) {
  lc_lonejson_thread_runtime_key_status = pthread_key_create(
      &lc_lonejson_thread_runtime_key, lc_lonejson_thread_runtime_destroy);
  if (lc_lonejson_thread_runtime_key_status == 0 &&
      pthread_atfork(lc_lonejson_runtime_lock, lc_lonejson_runtime_unlock,
                     lc_lonejson_runtime_unlock) != 0) {
    (void)pthread_key_delete(lc_lonejson_thread_runtime_key);
    lc_lonejson_thread_runtime_key_status = -1;
  }
#ifdef LOCKDC_TEST_BUILD
  lc_lonejson_test_fail_thread_runtime_key_status =
      pthread_key_create(&lc_lonejson_test_fail_thread_runtime_key, NULL);
#endif
}

/* pthread key destructors do not run for the main thread at ordinary exit,
 * nor when a DSO unloads while otherwise idle application threads survive.
 * No thread may execute SDK code during unload. Delete the callback key and
 * release every remaining runtime before its code/dependencies disappear. */
static void lc_lonejson_cleanup_thread_runtimes(void)
    __attribute__((destructor));
static void lc_lonejson_cleanup_thread_runtimes(void) {
  if (lc_lonejson_thread_runtime_key_status == 0) {
    (void)pthread_key_delete(lc_lonejson_thread_runtime_key);
    while (lc_lonejson_thread_runtimes != NULL)
      lc_lonejson_thread_runtime_destroy(lc_lonejson_thread_runtimes);
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_lonejson_test_fail_thread_runtime_key_status == 0)
    (void)pthread_key_delete(lc_lonejson_test_fail_thread_runtime_key);
#endif
}

size_t lc_lonejson_runtime_limit(size_t configured_limit) {
  return configured_limit > 0U ? configured_limit
                               : (size_t)LC_LONEJSON_DEFAULT_LIMIT_BYTES;
}

lonejson *lc_engine_lonejson_runtime(lc_engine_client *client) {
  return client != NULL ? client->lonejson : NULL;
}

lonejson *lc_thread_lonejson_runtime(void) {
  lc_lonejson_thread_runtime *entry;
  lonejson *runtime;
  lonejson_config config;
  lonejson_error error;
  size_t limit;

  (void)pthread_once(&lc_lonejson_thread_runtime_once,
                     lc_lonejson_thread_runtime_key_init);
  if (lc_lonejson_thread_runtime_key_status != 0)
    return NULL;
  entry = pthread_getspecific(lc_lonejson_thread_runtime_key);
  if (entry != NULL)
    return entry->runtime;
#ifdef LOCKDC_TEST_BUILD
  if (lc_lonejson_test_fail_thread_runtime_key_status == 0 &&
      pthread_getspecific(lc_lonejson_test_fail_thread_runtime_key) != NULL) {
    (void)pthread_setspecific(lc_lonejson_test_fail_thread_runtime_key, NULL);
    return NULL;
  }
#endif
  limit = lc_lonejson_runtime_limit(0U);
  lc_lonejson_config_init(&config, limit);
  lonejson_error_init(&error);
  runtime = lonejson_new(&config, &error);
  if (runtime == NULL) {
    return NULL;
  }
  entry = lc_alloc_with_allocator(NULL, sizeof(*entry));
  if (entry == NULL) {
    runtime->free(runtime);
    return NULL;
  }
  entry->runtime = runtime;
  if (pthread_setspecific(lc_lonejson_thread_runtime_key, entry) != 0) {
    runtime->free(runtime);
    lc_free_with_allocator(NULL, entry);
    return NULL;
  }
  lc_lonejson_runtime_lock();
  entry->next = lc_lonejson_thread_runtimes;
  lc_lonejson_thread_runtimes = entry;
  lc_lonejson_runtime_unlock();
  return runtime;
}

#ifdef LOCKDC_TEST_BUILD
void lc_lonejson_test_fail_thread_runtime_once(void) {
  (void)pthread_once(&lc_lonejson_thread_runtime_once,
                     lc_lonejson_thread_runtime_key_init);
  if (lc_lonejson_test_fail_thread_runtime_key_status == 0)
    (void)pthread_setspecific(lc_lonejson_test_fail_thread_runtime_key,
                              &lc_lonejson_test_fail_thread_runtime_marker);
}
#endif

void lc_lonejson_cleanup_value(lonejson *runtime, const lonejson_map *map,
                               void *value) {
  if (runtime == NULL || map == NULL || value == NULL) {
    return;
  }
  runtime->cleanup(runtime, map, value, NULL);
}

void lc_engine_lonejson_cleanup(lc_engine_client *client,
                                const lonejson_map *map, void *value) {
  lc_lonejson_cleanup_value(lc_engine_lonejson_runtime(client), map, value);
}

typedef struct lc_lonejson_map_frame {
  const lonejson_map *map;
  unsigned char *value;
  size_t index;
} lc_lonejson_map_frame;

typedef struct lc_lonejson_parse_config {
  lonejson_json_value *value;
  lonejson_json_value_parse_mode mode;
  lonejson_sink_fn sink;
  void *sink_user;
  const lonejson_value_visitor *visitor;
  void *visitor_user;
} lc_lonejson_parse_config;

static int lc_lonejson_field_initialized(const lonejson_field *field,
                                         const void *value) {
  switch (field->kind) {
  case LONEJSON_FIELD_KIND_STRING:
    return field->storage == LONEJSON_STORAGE_DYNAMIC &&
           *(char *const *)value != NULL;
  case LONEJSON_FIELD_KIND_STRING_STREAM:
  case LONEJSON_FIELD_KIND_BASE64_STREAM:
    return ((const lonejson_spooled *)value)->cleanup != NULL;
  case LONEJSON_FIELD_KIND_STRING_SOURCE:
  case LONEJSON_FIELD_KIND_BASE64_SOURCE:
    return ((const lonejson_source *)value)->cleanup != NULL;
  case LONEJSON_FIELD_KIND_JSON_VALUE:
    return ((const lonejson_json_value *)value)->methods != NULL;
  case LONEJSON_FIELD_KIND_STRING_ARRAY:
    return ((const lonejson_string_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_I64_ARRAY:
    return ((const lonejson_i64_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_U64_ARRAY:
    return ((const lonejson_u64_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_F64_ARRAY:
    return ((const lonejson_f64_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_BOOL_ARRAY:
    return ((const lonejson_bool_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_OBJECT_ARRAY:
    return ((const lonejson_object_array *)value)->items != NULL;
  case LONEJSON_FIELD_KIND_STRING_ARRAY_STREAM:
    return ((const lonejson_string_array_stream *)value)->set_handler != NULL;
  case LONEJSON_FIELD_KIND_MAPPED_ARRAY_STREAM:
    return ((const lonejson_mapped_array_stream *)value)->set_handler != NULL;
  default:
    return 0;
  }
}

int lc_lonejson_prepare_parse_destination(lonejson *runtime,
                                          const lonejson_map *map, void *value,
                                          lc_error *error) {
  lc_lonejson_map_frame local_frames[8], *frames = local_frames;
  lc_lonejson_parse_config local_configs[4], *configs = local_configs;
  size_t depth = 1U, frame_capacity = 8U, count = 0U, config_capacity = 4U;
  size_t i;
  int initialized = 0, rc = LC_OK;
  lonejson_error parse_error;
  lonejson_status lifecycle_status;
  if (runtime == NULL || map == NULL || value == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "mapped destination requires runtime, map, and value",
                        NULL, NULL, NULL);
  frames[0].map = map;
  frames[0].value = value;
  frames[0].index = 0U;
  /* One metadata walk preserves capture settings without native recursion.
   * Ordinary shallow maps use only the small inline work arrays. */
  while (depth != 0U) {
    lc_lonejson_map_frame *frame = &frames[depth - 1U];
    const lonejson_field *field;
    unsigned char *field_value;
    if (frame->index == frame->map->field_count) {
      --depth;
      continue;
    }
    field = &frame->map->fields[frame->index++];
    field_value = frame->value + field->struct_offset;
    if (field->kind == LONEJSON_FIELD_KIND_OBJECT && field->submap != NULL) {
      if (depth == frame_capacity) {
        lc_lonejson_map_frame *next;
        if (frame_capacity > (size_t)-1 / 2U / sizeof(*next)) {
          rc = LC_ERR_NOMEM;
          break;
        }
        next =
            lc_alloc_with_allocator(NULL, frame_capacity * 2U * sizeof(*next));
        if (next == NULL) {
          rc = LC_ERR_NOMEM;
          break;
        }
        memcpy(next, frames, depth * sizeof(*next));
        if (frames != local_frames)
          lc_free_with_allocator(NULL, frames);
        frames = next;
        frame_capacity *= 2U;
      }
      frames[depth].map = field->submap;
      frames[depth].value = field_value;
      frames[depth++].index = 0U;
      continue;
    }
    initialized |= lc_lonejson_field_initialized(field, field_value);
    if (field->kind == LONEJSON_FIELD_KIND_JSON_VALUE) {
      lonejson_json_value *json = (lonejson_json_value *)field_value;
      if (json->methods == NULL ||
          json->parse_mode == LONEJSON_JSON_VALUE_PARSE_NONE)
        continue;
      if (count == config_capacity) {
        lc_lonejson_parse_config *next;
        if (config_capacity > (size_t)-1 / 2U / sizeof(*next)) {
          rc = LC_ERR_NOMEM;
          break;
        }
        next =
            lc_alloc_with_allocator(NULL, config_capacity * 2U * sizeof(*next));
        if (next == NULL) {
          rc = LC_ERR_NOMEM;
          break;
        }
        memcpy(next, configs, count * sizeof(*next));
        if (configs != local_configs)
          lc_free_with_allocator(NULL, configs);
        configs = next;
        config_capacity *= 2U;
      }
      configs[count].value = json;
      configs[count].mode = json->parse_mode;
      configs[count].sink = json->parse_sink;
      configs[count].sink_user = json->parse_sink_user;
      configs[count].visitor = json->parse_visitor;
      configs[count++].visitor_user = json->parse_visitor_user;
    }
  }
  if (rc == LC_OK) {
    lonejson_error_init(&parse_error);
    if (initialized)
      lifecycle_status = runtime->reset(runtime, map, value, &parse_error);
    else
      lifecycle_status = runtime->init(runtime, map, value, &parse_error);
    rc = lc_lonejson_error_from_status(error, lifecycle_status, &parse_error,
                                       "failed to prepare mapped destination");
    for (i = 0U; i < count && rc == LC_OK; ++i) {
      lc_lonejson_parse_config *config = &configs[i];
      lonejson_json_value *json = config->value;
      lonejson_status status = LONEJSON_STATUS_OK;
      if (json->methods == NULL)
        continue;
      lonejson_error_init(&parse_error);
      if (config->mode == LONEJSON_JSON_VALUE_PARSE_SINK &&
          config->sink != NULL)
        status = json->methods->set_parse_sink(json, config->sink,
                                               config->sink_user, &parse_error);
      else if (config->mode == LONEJSON_JSON_VALUE_PARSE_VISITOR &&
               config->visitor != NULL)
        status = json->methods->set_parse_visitor(
            json, config->visitor, config->visitor_user, &parse_error);
      else if (config->mode == LONEJSON_JSON_VALUE_PARSE_CAPTURE)
        status = json->methods->enable_parse_capture(json, &parse_error);
      if (status != LONEJSON_STATUS_OK)
        rc = lc_lonejson_error_from_status(
            error, status, &parse_error,
            "failed to restore mapped capture settings");
    }
  } else {
    rc = lc_error_set(error, rc, 0L,
                      "failed to allocate mapped destination traversal", NULL,
                      NULL, NULL);
  }
  if (frames != local_frames)
    lc_free_with_allocator(NULL, frames);
  if (configs != local_configs)
    lc_free_with_allocator(NULL, configs);
  return rc;
}

#ifdef LONEJSON_WITH_CURL
void lc_lonejson_curl_parse_cleanup(struct lonejson_curl_parse *parse) {
  if (parse != NULL && parse->cleanup != NULL) {
    parse->cleanup(parse);
  }
}

void lc_lonejson_curl_upload_cleanup(struct lonejson_curl_upload *upload) {
  if (upload != NULL && upload->cleanup != NULL) {
    upload->cleanup(upload);
  }
}
#endif

lonejson_status lc_lonejson_parse_cstr_value(lonejson *runtime,
                                             const lonejson_map *map, void *dst,
                                             const char *json,
                                             lonejson_error *error) {
  lonejson_status status;
  if (runtime == NULL || map == NULL || dst == NULL || json == NULL) {
    if (error != NULL) {
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  status = runtime->init(runtime, map, dst, error);
  if (status != LONEJSON_STATUS_OK)
    return status;
  return runtime->parse_cstr(runtime, map, dst, json, error);
}

int lc_lonejson_error_from_status(lc_error *error, lonejson_status status,
                                  const lonejson_error *lj_error,
                                  const char *message) {
  int code;

  code = lc_lonejson_status_to_error_code(status);
  if (code == LC_OK) {
    return LC_OK;
  }
  return lc_error_set(error, code, 0L, message,
                      lc_lonejson_detail_message(lj_error, NULL), NULL, NULL);
}

int lc_engine_lonejson_error_from_status(lc_engine_error *error,
                                         lonejson_status status,
                                         const lonejson_error *lj_error,
                                         const char *message) {
  switch (status) {
  case LONEJSON_STATUS_OK:
    return LC_ENGINE_OK;
  case LONEJSON_STATUS_INVALID_ARGUMENT:
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        message != NULL ? message
                        : lc_lonejson_detail_message(
                              lj_error, "invalid lonejson argument"));
  case LONEJSON_STATUS_ALLOCATION_FAILED:
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        message != NULL ? message
                        : lc_lonejson_detail_message(
                              lj_error, "lonejson allocation failed"));
  case LONEJSON_STATUS_CALLBACK_FAILED:
  case LONEJSON_STATUS_IO_ERROR:
  case LONEJSON_STATUS_INTERNAL_ERROR:
    return lc_engine_set_transport_error(
        error, message != NULL ? message
                               : lc_lonejson_detail_message(
                                     lj_error, "local JSON mapping failed"));
  case LONEJSON_STATUS_INVALID_JSON:
  case LONEJSON_STATUS_TYPE_MISMATCH:
  case LONEJSON_STATUS_MISSING_REQUIRED_FIELD:
  case LONEJSON_STATUS_DUPLICATE_FIELD:
  case LONEJSON_STATUS_OVERFLOW:
  case LONEJSON_STATUS_TRUNCATED:
  default:
    return lc_engine_set_protocol_error(
        error, message != NULL ? message
                               : lc_lonejson_detail_message(
                                     lj_error, "failed to parse JSON"));
  }
}

int lc_lonejson_parse_file(lonejson *runtime, FILE *fp, const lonejson_map *map,
                           void *dst, lc_error *error, const char *message) {
  int rc;
  if (runtime == NULL || fp == NULL || map == NULL || dst == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL
            ? message
            : "lonejson parse requires runtime, file, map, and destination",
        NULL, NULL, NULL);
  }
  rc = lc_lonejson_prepare_parse_destination(runtime, map, dst, error);
  if (rc != LC_OK)
    return rc;
  return lc_lonejson_parse_prepared_file(runtime, fp, map, dst, error, message);
}

int lc_lonejson_parse_prepared_file(lonejson *runtime, FILE *fp,
                                    const lonejson_map *map, void *dst,
                                    lc_error *error, const char *message) {
  lonejson_error lj_error;
  lonejson_status status;

  memset(&lj_error, 0, sizeof(lj_error));
  if (runtime == NULL || fp == NULL || map == NULL || dst == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL
            ? message
            : "lonejson parse requires runtime, file, map, and destination",
        NULL, NULL, NULL);
  }
  status = runtime->parse_filep(runtime, map, dst, fp, &lj_error);
  return lc_lonejson_error_from_status(error, status, &lj_error, message);
}

int lc_lonejson_serialize_file(lonejson *runtime, FILE *fp,
                               const lonejson_map *map, const void *src,
                               lc_error *error, const char *message) {
  lonejson_error lj_error;
  lonejson_status status;

  memset(&lj_error, 0, sizeof(lj_error));
  if (runtime == NULL || fp == NULL || map == NULL || src == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL
            ? message
            : "lonejson serialize requires runtime, file, map, and source",
        NULL, NULL, NULL);
  }
  status = runtime->serialize_filep(runtime, map, src, fp, &lj_error);
  return lc_lonejson_error_from_status(error, status, &lj_error, message);
}
