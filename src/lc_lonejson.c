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
  runtime->cleanup(runtime, map, value);
}

void lc_engine_lonejson_cleanup(lc_engine_client *client,
                                const lonejson_map *map, void *value) {
  lc_lonejson_cleanup_value(lc_engine_lonejson_runtime(client), map, value);
}

static int lc_lonejson_has_configured_json_value(const lonejson_map *map,
                                                 const void *value) {
  const unsigned char *base;
  size_t index;

  if (map == NULL || value == NULL) {
    return 0;
  }
  base = (const unsigned char *)value;
  for (index = 0U; index < map->field_count; ++index) {
    const lonejson_field *field;
    const void *field_value;

    field = &map->fields[index];
    field_value = base + field->struct_offset;
    if (field->kind == LONEJSON_FIELD_KIND_JSON_VALUE) {
      const lonejson_json_value *json_value;

      json_value = (const lonejson_json_value *)field_value;
      if (json_value->methods != NULL &&
          json_value->parse_mode != LONEJSON_JSON_VALUE_PARSE_NONE) {
        return 1;
      }
    } else if (field->kind == LONEJSON_FIELD_KIND_OBJECT &&
               field->submap != NULL &&
               lc_lonejson_has_configured_json_value(field->submap,
                                                     field_value)) {
      return 1;
    }
  }
  return 0;
}

static int lc_lonejson_has_initialized_destination(const lonejson_map *map,
                                                   const void *value) {
  const unsigned char *base;
  size_t index;

  if (map == NULL || value == NULL) {
    return 0;
  }
  base = (const unsigned char *)value;
  for (index = 0U; index < map->field_count; ++index) {
    const lonejson_field *field;
    const void *field_value;

    field = &map->fields[index];
    field_value = base + field->struct_offset;
    switch (field->kind) {
    case LONEJSON_FIELD_KIND_STRING:
      if (field->storage == LONEJSON_STORAGE_DYNAMIC &&
          *(char *const *)field_value != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_STRING_STREAM:
    case LONEJSON_FIELD_KIND_BASE64_STREAM:
      if (((const lonejson_spooled *)field_value)->cleanup != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_STRING_SOURCE:
    case LONEJSON_FIELD_KIND_BASE64_SOURCE:
      if (((const lonejson_source *)field_value)->cleanup != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_JSON_VALUE:
      if (((const lonejson_json_value *)field_value)->methods != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_OBJECT:
      if (field->submap != NULL &&
          lc_lonejson_has_initialized_destination(field->submap, field_value)) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_STRING_ARRAY:
      if (((const lonejson_string_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_I64_ARRAY:
      if (((const lonejson_i64_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_U64_ARRAY:
      if (((const lonejson_u64_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_F64_ARRAY:
      if (((const lonejson_f64_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_BOOL_ARRAY:
      if (((const lonejson_bool_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_OBJECT_ARRAY:
      if (((const lonejson_object_array *)field_value)->items != NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_STRING_ARRAY_STREAM:
      if (((const lonejson_string_array_stream *)field_value)->set_handler !=
          NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_MAPPED_ARRAY_STREAM:
      if (((const lonejson_mapped_array_stream *)field_value)->set_handler !=
          NULL) {
        return 1;
      }
      break;
    case LONEJSON_FIELD_KIND_I64:
    case LONEJSON_FIELD_KIND_U64:
    case LONEJSON_FIELD_KIND_F64:
    case LONEJSON_FIELD_KIND_BOOL:
    default:
      break;
    }
  }
  return 0;
}

static void lc_lonejson_prepare_parse_storage(lonejson *runtime,
                                              const lonejson_map *map,
                                              void *value) {
  if (lc_lonejson_has_initialized_destination(map, value)) {
    runtime->reset(runtime, map, value);
  } else {
    runtime->init(runtime, map, value);
  }
}

typedef struct lc_lonejson_json_value_parse_config {
  lonejson_json_value_parse_mode parse_mode;
  lonejson_sink_fn parse_sink;
  void *parse_sink_user;
  const lonejson_value_visitor *parse_visitor;
  void *parse_visitor_user;
} lc_lonejson_json_value_parse_config;

static size_t lc_lonejson_count_json_value_fields(const lonejson_map *map) {
  size_t count;
  size_t index;

  if (map == NULL) {
    return 0U;
  }
  count = 0U;
  for (index = 0U; index < map->field_count; ++index) {
    const lonejson_field *field;

    field = &map->fields[index];
    if (field->kind == LONEJSON_FIELD_KIND_JSON_VALUE) {
      ++count;
    } else if (field->kind == LONEJSON_FIELD_KIND_OBJECT &&
               field->submap != NULL) {
      count += lc_lonejson_count_json_value_fields(field->submap);
    }
  }
  return count;
}

static void lc_lonejson_collect_json_value_parse_configs(
    const lonejson_map *map, const void *value,
    lc_lonejson_json_value_parse_config *configs, size_t *config_index) {
  const unsigned char *base;
  size_t index;

  if (map == NULL || value == NULL || configs == NULL || config_index == NULL) {
    return;
  }
  base = (const unsigned char *)value;
  for (index = 0U; index < map->field_count; ++index) {
    const lonejson_field *field;
    const void *field_value;

    field = &map->fields[index];
    field_value = base + field->struct_offset;
    if (field->kind == LONEJSON_FIELD_KIND_JSON_VALUE) {
      const lonejson_json_value *json_value;
      lc_lonejson_json_value_parse_config *config;

      json_value = (const lonejson_json_value *)field_value;
      config = &configs[*config_index];
      config->parse_mode = json_value->parse_mode;
      config->parse_sink = json_value->parse_sink;
      config->parse_sink_user = json_value->parse_sink_user;
      config->parse_visitor = json_value->parse_visitor;
      config->parse_visitor_user = json_value->parse_visitor_user;
      ++(*config_index);
    } else if (field->kind == LONEJSON_FIELD_KIND_OBJECT &&
               field->submap != NULL) {
      lc_lonejson_collect_json_value_parse_configs(field->submap, field_value,
                                                   configs, config_index);
    }
  }
}

static void lc_lonejson_restore_json_value_parse_configs(
    const lonejson_map *map, void *value,
    const lc_lonejson_json_value_parse_config *configs, size_t *config_index) {
  unsigned char *base;
  lonejson_error error;
  size_t index;

  if (map == NULL || value == NULL || configs == NULL || config_index == NULL) {
    return;
  }
  memset(&error, 0, sizeof(error));
  base = (unsigned char *)value;
  for (index = 0U; index < map->field_count; ++index) {
    const lonejson_field *field;
    void *field_value;

    field = &map->fields[index];
    field_value = base + field->struct_offset;
    if (field->kind == LONEJSON_FIELD_KIND_JSON_VALUE) {
      lonejson_json_value *json_value;
      const lc_lonejson_json_value_parse_config *config;

      json_value = (lonejson_json_value *)field_value;
      config = &configs[*config_index];
      ++(*config_index);
      if (json_value->methods == NULL) {
        continue;
      }
      switch (config->parse_mode) {
      case LONEJSON_JSON_VALUE_PARSE_SINK:
        if (config->parse_sink != NULL) {
          (void)json_value->methods->set_parse_sink(
              json_value, config->parse_sink, config->parse_sink_user, &error);
        }
        break;
      case LONEJSON_JSON_VALUE_PARSE_VISITOR:
        if (config->parse_visitor != NULL) {
          (void)json_value->methods->set_parse_visitor(
              json_value, config->parse_visitor, config->parse_visitor_user,
              &error);
        }
        break;
      case LONEJSON_JSON_VALUE_PARSE_CAPTURE:
        (void)json_value->methods->enable_parse_capture(json_value, &error);
        break;
      case LONEJSON_JSON_VALUE_PARSE_NONE:
      default:
        break;
      }
    } else if (field->kind == LONEJSON_FIELD_KIND_OBJECT &&
               field->submap != NULL) {
      lc_lonejson_restore_json_value_parse_configs(field->submap, field_value,
                                                   configs, config_index);
    }
  }
}

void lc_lonejson_prepare_parse_destination(lonejson *runtime,
                                           const lonejson_map *map,
                                           void *value) {
  lc_lonejson_json_value_parse_config *configs;
  size_t config_count;
  size_t config_index;

  if (runtime == NULL || map == NULL || value == NULL) {
    return;
  }
  if (!lc_lonejson_has_configured_json_value(map, value)) {
    lc_lonejson_prepare_parse_storage(runtime, map, value);
    return;
  }
  config_count = lc_lonejson_count_json_value_fields(map);
  configs = NULL;
  if (config_count > 0U) {
    configs = calloc(config_count, sizeof(*configs));
  }
  if (configs != NULL) {
    config_index = 0U;
    lc_lonejson_collect_json_value_parse_configs(map, value, configs,
                                                 &config_index);
  }
  lc_lonejson_prepare_parse_storage(runtime, map, value);
  if (configs != NULL) {
    config_index = 0U;
    lc_lonejson_restore_json_value_parse_configs(map, value, configs,
                                                 &config_index);
    free(configs);
  }
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
  if (runtime == NULL || map == NULL || dst == NULL || json == NULL) {
    if (error != NULL) {
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  runtime->init(runtime, map, dst);
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
  if (runtime == NULL || fp == NULL || map == NULL || dst == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL
            ? message
            : "lonejson parse requires runtime, file, map, and destination",
        NULL, NULL, NULL);
  }
  lc_lonejson_prepare_parse_destination(runtime, map, dst);
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
