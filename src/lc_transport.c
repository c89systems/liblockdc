#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_log.h"

#include <ctype.h>
#include <lc/version.h>
#include <limits.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *LC_ENGINE_VERSION_STRING = LC_VERSION_STRING;
#define LC_ENGINE_HTTP_ERROR_BODY_LIMIT_DEFAULT (8U * 1024U)

#ifdef LOCKDC_TEST_BUILD
int (*lc_transport_test_clock_gettime)(clockid_t clock_id, struct timespec *out,
                                       void *context) = NULL;
void *lc_transport_test_clock_context = NULL;

static int lc_transport_clock_gettime(clockid_t clock_id,
                                      struct timespec *out) {
  if (lc_transport_test_clock_gettime != NULL) {
    return lc_transport_test_clock_gettime(clock_id, out,
                                           lc_transport_test_clock_context);
  }
  return clock_gettime(clock_id, out);
}
#else
#define lc_transport_clock_gettime clock_gettime
#endif

static size_t lc_engine_lonejson_curl_upload_read_callback(char *ptr,
                                                           size_t size,
                                                           size_t nmemb,
                                                           void *userdata) {
  lonejson_curl_upload *upload;

  upload = (lonejson_curl_upload *)userdata;
  if (upload == NULL) {
    return 0U;
  }
  return upload->read_callback(upload, ptr, size, nmemb);
}

static lonejson *lc_engine_runtime(lc_engine_client *client) {
  return lc_engine_lonejson_runtime(client);
}

lonejson_read_result lc_engine_json_memory_reader(void *user,
                                                  unsigned char *buffer,
                                                  size_t capacity);
static size_t lc_engine_header_callback(char *buffer, size_t size,
                                        size_t nitems, void *userdata);
static CURLcode lc_engine_ssl_ctx_callback(CURL *curl, void *ssl_ctx,
                                           void *userdata);
static int lc_engine_case_equal_n(const char *left, const char *right,
                                  size_t count);
static int lc_engine_store_x509(X509 ***items, size_t *item_count, X509 *value);
static int lc_engine_match_private_key(X509 *certificate, EVP_PKEY *candidate);
static void lc_engine_assign_header_string(char **target, const char *value,
                                           const char *end);
static const char *lc_engine_trim_http_ows_end(const char *value,
                                               const char *end);
static size_t
lc_engine_header_numeric_parse_failed(lc_engine_http_result *result,
                                      const char *message);
static char *lc_engine_join_endpoints(const lc_engine_client *client);

typedef struct lc_engine_http_error_json {
  char *server_error_code;
  char *detail;
  char *leader_endpoint;
  char *current_etag;
  lonejson_int64 current_version;
  lonejson_int64 retry_after_seconds;
} lc_engine_http_error_json;

static const lonejson_field lc_engine_http_error_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_http_error_json, server_error_code,
                                "error"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_http_error_json, detail, "detail"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_http_error_json, leader_endpoint,
                                "leader_endpoint"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_http_error_json, current_etag,
                                "current_etag"),
    LONEJSON_FIELD_I64(lc_engine_http_error_json, current_version,
                       "current_version"),
    LONEJSON_FIELD_I64(lc_engine_http_error_json, retry_after_seconds,
                       "retry_after_seconds")};

LONEJSON_MAP_DEFINE(lc_engine_http_error_map, lc_engine_http_error_json,
                    lc_engine_http_error_fields);

typedef struct lc_engine_json_http_state {
  lc_engine_client *client;
  lc_engine_http_result *result;
  lc_engine_error *error;
  const lonejson_map *response_map;
  void *response_dst;
  lc_engine_http_error_json error_body;
  lonejson_curl_parse parse;
  size_t bytes_received;
  size_t byte_limit;
  size_t error_byte_limit;
  int parser_initialized;
  int parser_is_error;
  int limit_exceeded;
} lc_engine_json_http_state;

static size_t lc_engine_json_http_header_callback(char *buffer, size_t size,
                                                  size_t nitems,
                                                  void *userdata);
static size_t lc_engine_json_http_write_callback(char *contents, size_t size,
                                                 size_t nmemb, void *userdata);
static int lc_engine_json_http_init_parser(lc_engine_json_http_state *state);
static void lc_engine_json_http_state_cleanup(lc_engine_json_http_state *state);

/* A clone used by a bounded higher-level operation may have more than one
 * endpoint. Recalculate before every attempt so a failover cannot restart the
 * operation's deadline. */
int lc_engine_client_attempt_timeout_ms(const lc_engine_client *client,
                                        long *out) {
  struct timespec now;
  time_t seconds;
  long nanoseconds;
  long remaining;
  long configured;

  if (client == NULL || out == NULL)
    return 0;
  configured = client->timeout_ms > 0L ? client->timeout_ms : 30000L;
  if (!client->request_deadline_set) {
    *out = configured;
    return 1;
  }
  if (lc_transport_clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return 0;
  if (now.tv_sec > client->request_deadline.tv_sec ||
      (now.tv_sec == client->request_deadline.tv_sec &&
       now.tv_nsec >= client->request_deadline.tv_nsec))
    return 0;
  seconds = client->request_deadline.tv_sec - now.tv_sec;
  nanoseconds = client->request_deadline.tv_nsec - now.tv_nsec;
  if (nanoseconds < 0L) {
    --seconds;
    nanoseconds += 1000000000L;
  }
  if (seconds < 0)
    return 0;
  if (seconds > (time_t)(LONG_MAX / 1000L)) {
    remaining = LONG_MAX;
  } else {
    remaining = (long)seconds * 1000L;
    if (nanoseconds > 0L) {
      long milliseconds = (nanoseconds + 999999L) / 1000000L;

      remaining = remaining > LONG_MAX - milliseconds
                      ? LONG_MAX
                      : remaining + milliseconds;
    }
  }
  if (remaining < 1L)
    return 0;
  *out = remaining < configured ? remaining : configured;
  return 1;
}

static int lc_engine_transport_cancel_requested(lc_engine_client *client) {
  return client != NULL && client->cancel_check != NULL &&
         client->cancel_check(client->cancel_context);
}

static int lc_engine_transport_progress(void *client_ptr, curl_off_t dltotal,
                                        curl_off_t dlnow, curl_off_t ultotal,
                                        curl_off_t ulnow) {
  (void)dltotal;
  (void)dlnow;
  (void)ultotal;
  (void)ulnow;
  return lc_engine_transport_cancel_requested((lc_engine_client *)client_ptr);
}

const char *lc_engine_version_string(void) { return LC_ENGINE_VERSION_STRING; }

void lc_engine_client_config_init(lc_engine_client_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->timeout_ms = 30000L;
  config->prefer_http_2 = 1;
  config->http_json_response_limit_bytes = 0U;
}

void lc_engine_client_set_request_deadline(lc_engine_client *client,
                                           const struct timespec *deadline) {
  if (client == NULL)
    return;
  if (deadline == NULL) {
    memset(&client->request_deadline, 0, sizeof(client->request_deadline));
    client->request_deadline_set = 0;
    return;
  }
  client->request_deadline = *deadline;
  client->request_deadline_set = 1;
}

void lc_engine_error_init(lc_engine_error *error) {
  if (error == NULL) {
    return;
  }
  memset(error, 0, sizeof(*error));
}

void lc_engine_error_reset(lc_engine_error *error) {
  if (error == NULL) {
    return;
  }
  lc_engine_free_string(&error->message);
  lc_engine_free_string(&error->server_error_code);
  lc_engine_free_string(&error->detail);
  lc_engine_free_string(&error->leader_endpoint);
  lc_engine_free_string(&error->current_etag);
  lc_engine_free_string(&error->correlation_id);
  memset(error, 0, sizeof(*error));
}

void lc_engine_error_cleanup(lc_engine_error *error) {
  lc_engine_error_reset(error);
}

void lc_engine_acquire_response_cleanup(lc_engine_acquire_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->owner);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_get_response_cleanup(lc_engine_get_response *response) {
  if (response == NULL) {
    return;
  }
  free(response->body);
  lc_engine_free_string(&response->content_type);
  lc_engine_free_string(&response->etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_keepalive_response_cleanup(
    lc_engine_keepalive_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_release_response_cleanup(lc_engine_release_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_enqueue_response_cleanup(lc_engine_enqueue_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->queue);
  lc_engine_free_string(&response->message_id);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_dequeue_response_cleanup(lc_engine_dequeue_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->queue);
  lc_engine_free_string(&response->message_id);
  lc_engine_free_string(&response->payload_content_type);
  if (response->payload != NULL) {
    response->payload->close(response->payload);
  }
  lc_engine_free_string(&response->correlation_id);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->meta_etag);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->state_lease_id);
  lc_engine_free_string(&response->state_txn_id);
  lc_engine_free_string(&response->next_cursor);
  memset(response, 0, sizeof(*response));
}

char *lc_engine_strdup_local(const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_engine_strdup_range(const char *begin, const char *end) {
  size_t length;
  char *copy;

  if (begin == NULL || end == NULL || end < begin) {
    return NULL;
  }
  length = (size_t)(end - begin);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, begin, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_engine_free_string(char **value) {
  if (value == NULL || *value == NULL) {
    return;
  }
  free(*value);
  *value = NULL;
}

void lc_engine_trim_trailing_slash(char *value) {
  size_t length;

  if (value == NULL) {
    return;
  }
  length = strlen(value);
  while (length > 0U && value[length - 1U] == '/') {
    value[length - 1U] = '\0';
    --length;
  }
}

int lc_engine_set_client_error(lc_engine_error *error, int code,
                               const char *message) {
  if (error != NULL) {
    lc_engine_error_reset(error);
    error->code = code;
    error->message = lc_engine_strdup_local(message);
    if (message != NULL && error->message == NULL) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  return code;
}

int lc_engine_set_transport_error(lc_engine_error *error, const char *message) {
  return lc_engine_set_client_error(error, LC_ENGINE_ERROR_TRANSPORT, message);
}

int lc_engine_set_protocol_error(lc_engine_error *error, const char *message) {
  return lc_engine_set_client_error(error, LC_ENGINE_ERROR_PROTOCOL, message);
}

void lc_engine_buffer_init(lc_engine_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  memset(buffer, 0, sizeof(*buffer));
}

void lc_engine_buffer_cleanup(lc_engine_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  free(buffer->data);
  memset(buffer, 0, sizeof(*buffer));
}

int lc_engine_buffer_append(lc_engine_buffer *buffer, const char *bytes,
                            size_t count) {
  size_t required;
  size_t capacity;
  char *next_data;

  if (buffer == NULL || (count > 0U && bytes == NULL)) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  required = buffer->length + count + 1U;
  if (required > buffer->capacity) {
    capacity = buffer->capacity == 0U ? 256U : buffer->capacity;
    while (capacity < required) {
      capacity *= 2U;
    }
    next_data = (char *)realloc(buffer->data, capacity);
    if (next_data == NULL) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
    buffer->data = next_data;
    buffer->capacity = capacity;
  }
  if (count > 0U) {
    memcpy(buffer->data + buffer->length, bytes, count);
    buffer->length += count;
  }
  buffer->data[buffer->length] = '\0';
  return LC_ENGINE_OK;
}

int lc_engine_buffer_append_cstr(lc_engine_buffer *buffer, const char *value) {
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  return lc_engine_buffer_append(buffer, value, strlen(value));
}

lonejson_read_result lc_engine_json_memory_reader(void *user,
                                                  unsigned char *buffer,
                                                  size_t capacity) {
  lc_engine_json_reader_source *source;
  lonejson_read_result result;
  size_t count;

  source = (lc_engine_json_reader_source *)user;
  result = lonejson_default_read_result();
  if (source == NULL || buffer == NULL || capacity == 0U) {
    result.eof = 1;
    return result;
  }
  if (source->remaining == 0U) {
    result.eof = 1;
    return result;
  }
  count = source->remaining < capacity ? source->remaining : capacity;
  memcpy(buffer, source->cursor, count);
  source->cursor += count;
  source->remaining -= count;
  result.bytes_read = count;
  result.eof = source->remaining == 0U;
  return result;
}

int lc_engine_buffer_append_limited(lc_engine_buffer *buffer, const char *bytes,
                                    size_t count, size_t limit) {
  size_t required;

  if (buffer == NULL || (count > 0U && bytes == NULL)) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  if (limit > 0U) {
    required = buffer->length + count + 1U;
    if (required > limit) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  return lc_engine_buffer_append(buffer, bytes, count);
}

static int lc_engine_case_equal_n(const char *left, const char *right,
                                  size_t count) {
  size_t index;

  for (index = 0U; index < count; ++index) {
    if (tolower((unsigned char)left[index]) !=
        tolower((unsigned char)right[index])) {
      return 0;
    }
  }
  return 1;
}

static void lc_engine_assign_header_string(char **target, const char *value,
                                           const char *end) {
  lc_engine_free_string(target);
  *target = lc_engine_strdup_range(value, end);
}

static const char *lc_engine_trim_http_ows_end(const char *value,
                                               const char *end) {
  while (end > value && (end[-1] == ' ' || end[-1] == '\t')) {
    --end;
  }
  return end;
}

int lc_engine_retention_validate(lc_engine_client *client, int selected,
                                 long seconds, const char *ns, const char *key,
                                 lc_engine_error *error) {
  lc_error validation;
  int rc;
  lc_error_init(&validation);
  rc = lc_state_retention_validate(selected, seconds,
                                   lc_engine_effective_namespace(client, ns),
                                   key, &validation);
  if (rc != LC_OK)
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                    validation.message);
  lc_error_cleanup(&validation);
  return rc;
}

int lc_engine_retention_header(lc_engine_retention_headers *headers,
                               const char *name, size_t name_length,
                               const char *value, size_t value_length) {
  int support_field =
      name_length == sizeof("X-State-Retention-Supported") - 1 &&
      lc_engine_case_equal_n(name, "X-State-Retention-Supported", name_length);
  int expiry_field =
      name_length == sizeof("X-State-Retention-Expires-At-Unix") - 1 &&
      lc_engine_case_equal_n(name, "X-State-Retention-Expires-At-Unix",
                             name_length);
  lc_unix_seconds expiry;
  size_t i;
  if (!support_field && !expiry_field)
    return 0;
  if (support_field) {
    lc_state_retention_support support;
    if (value_length != 1 || (value[0] != '0' && value[0] != '1'))
      return -1;
    support = value[0] == '1' ? LC_STATE_RETENTION_SUPPORTED
                              : LC_STATE_RETENTION_UNSUPPORTED;
    if ((headers->has_support && headers->support != support) ||
        (support == LC_STATE_RETENTION_UNSUPPORTED && headers->has_expiry))
      return -1;
    headers->has_support = 1;
    headers->support = support;
    return 1;
  }
  if (value_length == 0 || headers->support == LC_STATE_RETENTION_UNSUPPORTED)
    return -1;
  for (i = 0; i < value_length; ++i)
    if (value[i] < '0' || value[i] > '9')
      return -1;
  if (!lc_parse_i64_base10_range_checked(value, value_length, &expiry) ||
      (headers->has_expiry && headers->expiry != expiry))
    return -1;
  headers->support = LC_STATE_RETENTION_SUPPORTED;
  headers->has_expiry = 1;
  headers->expiry = expiry;
  return 1;
}

static size_t
lc_engine_header_numeric_parse_failed(lc_engine_http_result *result,
                                      const char *message) {
  if (result != NULL) {
    result->header_parse_failed = 1;
    lc_engine_free_string(&result->header_parse_error_message);
    result->header_parse_error_message = lc_engine_strdup_local(message);
  }
  return 0U;
}

static int lc_engine_json_http_init_parser(lc_engine_json_http_state *state) {
  const lonejson_map *map;
  void *dst;
  lonejson *runtime;
  lonejson_status status;

  if (state == NULL || state->result == NULL) {
    return 0;
  }
  map =
      state->parser_is_error ? &lc_engine_http_error_map : state->response_map;
  dst =
      state->parser_is_error ? (void *)&state->error_body : state->response_dst;
  if (map == NULL || dst == NULL) {
    state->parser_initialized = 1;
    return 1;
  }

  runtime = lc_engine_runtime(state->client);
  runtime->init(runtime, map, dst);
  status = runtime->curl_parse_init(runtime, &state->parse, map, dst);
  if (status != LONEJSON_STATUS_OK) {
    lc_engine_lonejson_error_from_status(
        state->error, status, &state->parse.error,
        "failed to initialize JSON response parser");
    return 0;
  }
  state->parser_initialized = 1;
  return 1;
}

static size_t lc_engine_json_http_header_callback(char *buffer, size_t size,
                                                  size_t nitems,
                                                  void *userdata) {
  lc_engine_json_http_state *state;
  size_t total;
  long parsed;

  state = (lc_engine_json_http_state *)userdata;
  total = size * nitems;
  if (state == NULL || state->result == NULL) {
    return 0U;
  }

  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    char status_line[64];
    size_t status_length;

    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    parsed = 0L;
    sscanf(status_line, "%*s %ld", &parsed);
    state->result->http_status = parsed;
    memset(&state->result->retention, 0, sizeof(state->result->retention));
    state->parser_is_error = !(parsed >= 200L && parsed < 300L);
    return total;
  }

  total = lc_engine_header_callback(buffer, size, nitems, state->result);
  if (state->result->http_status == 0L) {
    return total;
  }
  return total;
}

static size_t lc_engine_json_http_write_callback(char *contents, size_t size,
                                                 size_t nmemb, void *userdata) {
  lc_engine_json_http_state *state;
  size_t total;
  size_t available;
  size_t written;

  state = (lc_engine_json_http_state *)userdata;
  total = size * nmemb;
  if (state == NULL || state->result == NULL || total == 0U) {
    return total;
  }

  available =
      state->parser_is_error ? state->error_byte_limit : state->byte_limit;
  available = available > state->bytes_received
                  ? available - state->bytes_received
                  : 0U;
  if (total > available) {
    state->limit_exceeded = 1;
    lc_engine_set_protocol_error(
        state->error, "typed JSON response exceeds configured byte limit");
    return 0U;
  }
  if (!state->parser_initialized) {
    if (state->result->http_status >= 200L &&
        state->result->http_status < 300L) {
      if (state->result->http_status == 204L ||
          state->result->http_status == 205L ||
          state->result->http_status == 304L) {
        state->parser_initialized = 1;
        return total;
      }
      state->parser_is_error = 0;
    } else if (state->result->http_status >= 400L) {
      state->parser_is_error = 1;
    } else {
      state->limit_exceeded = 1;
      lc_engine_set_protocol_error(state->error,
                                   "typed JSON response has no parser");
      return 0U;
    }
    if (!lc_engine_json_http_init_parser(state)) {
      return 0U;
    }
  }
  written = state->parse.write_callback(&state->parse, contents, 1U, total);
  if (written != total) {
    lc_engine_lonejson_error_from_status(state->error, state->parse.error.code,
                                         &state->parse.error,
                                         "failed to parse typed JSON response");
    return 0U;
  }
  state->bytes_received += total;
  return total;
}

static void
lc_engine_json_http_state_cleanup(lc_engine_json_http_state *state) {
  if (state == NULL) {
    return;
  }
  lc_lonejson_curl_parse_cleanup(&state->parse);
  if (state->parser_is_error) {
    lc_engine_runtime(state->client)
        ->cleanup(lc_engine_runtime(state->client), &lc_engine_http_error_map,
                  &state->error_body);
  }
  state->parser_initialized = 0;
  state->parser_is_error = 0;
}

static char *lc_engine_join_endpoints(const lc_engine_client *client) {
  size_t total_length;
  size_t index;
  char *joined;
  char *cursor;

  if (client == NULL || client->endpoint_count == 0U) {
    return NULL;
  }
  total_length = 1U;
  for (index = 0U; index < client->endpoint_count; ++index) {
    if (client->endpoints[index] != NULL) {
      total_length += strlen(client->endpoints[index]);
    }
    if (index + 1U < client->endpoint_count) {
      total_length += 1U;
    }
  }
  joined = (char *)malloc(total_length);
  if (joined == NULL) {
    return NULL;
  }
  cursor = joined;
  for (index = 0U; index < client->endpoint_count; ++index) {
    size_t item_length;

    item_length = client->endpoints[index] != NULL
                      ? strlen(client->endpoints[index])
                      : 0U;
    if (item_length > 0U) {
      memcpy(cursor, client->endpoints[index], item_length);
      cursor += item_length;
    }
    if (index + 1U < client->endpoint_count) {
      *cursor++ = ',';
    }
  }
  *cursor = '\0';
  return joined;
}

static size_t lc_engine_header_callback(char *buffer, size_t size,
                                        size_t nitems, void *userdata) {
  lc_engine_http_result *result;
  size_t total;
  const char *value;
  const char *end;
  size_t name_length;
  const char *colon;
  int retention_rc;

  result = (lc_engine_http_result *)userdata;
  total = size * nitems;

  colon = memchr(buffer, ':', total);
  if (colon != NULL && result->http_status >= 200 &&
      result->http_status < 300) {
    value = colon + 1;
    end = buffer + total;
    while (value < end && (*value == ' ' || *value == '\t'))
      ++value;
    while (end > value && (end[-1] == '\r' || end[-1] == '\n' ||
                           end[-1] == ' ' || end[-1] == '\t'))
      --end;
    retention_rc = lc_engine_retention_header(&result->retention, buffer,
                                              (size_t)(colon - buffer), value,
                                              (size_t)(end - value));
    if (retention_rc < 0)
      return lc_engine_header_numeric_parse_failed(
          result,
          "response state retention headers are invalid or contradictory");
    if (retention_rc > 0)
      return total;
  }

  if (total > 0U) {
    name_length = strlen("X-Correlation-Id:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Correlation-Id:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      lc_engine_assign_header_string(&result->correlation_id, value, end);
      return total;
    }

    name_length = strlen("ETag:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "ETag:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      if (end > value && *value == '"' && end[-1] == '"') {
        ++value;
        --end;
      }
      lc_engine_assign_header_string(&result->etag, value, end);
      return total;
    }

    name_length = strlen("X-Key-Version:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Key-Version:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      end = lc_engine_trim_http_ows_end(value, end);
      if (!lc_parse_i64_base10_range_checked(value, (size_t)(end - value),
                                             &result->key_version)) {
        return lc_engine_header_numeric_parse_failed(
            result, "response X-Key-Version header is invalid");
      }
      return total;
    }

    name_length = strlen("X-Fencing-Token:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Fencing-Token:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      end = lc_engine_trim_http_ows_end(value, end);
      if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                              &result->fencing_token)) {
        return lc_engine_header_numeric_parse_failed(
            result, "response X-Fencing-Token header is invalid");
      }
      return total;
    }

    name_length = strlen("Content-Type:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "Content-Type:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      lc_engine_assign_header_string(&result->content_type, value, end);
      return total;
    }
  }

  return total;
}

void lc_engine_http_result_cleanup(lc_engine_http_result *result) {
  if (result == NULL) {
    return;
  }
  lc_engine_free_string(&result->correlation_id);
  lc_engine_free_string(&result->etag);
  lc_engine_free_string(&result->content_type);
  lc_engine_free_string(&result->header_parse_error_message);
  lc_engine_free_string(&result->server_error_code);
  lc_engine_free_string(&result->detail);
  lc_engine_free_string(&result->leader_endpoint);
  lc_engine_free_string(&result->current_etag);
  memset(result, 0, sizeof(*result));
}

static void lc_engine_build_url(lc_engine_client *client, const char *endpoint,
                                const char *path, lc_engine_buffer *url) {
  const char *scheme;

  scheme = strstr(endpoint, "://");
  if (scheme == NULL) {
    if (client->disable_mtls) {
      lc_engine_buffer_append_cstr(url, "http://");
    } else {
      lc_engine_buffer_append_cstr(url, "https://");
    }
  }
  lc_engine_buffer_append_cstr(url, endpoint);
  lc_engine_buffer_append_cstr(url, path);
}

static CURLcode lc_engine_ssl_ctx_callback(CURL *curl, void *ssl_ctx,
                                           void *userdata) {
  lc_engine_client *client;
  SSL_CTX *ctx;
  X509_STORE *store;
  size_t index;

  (void)curl;
  client = (lc_engine_client *)userdata;
  if (client == NULL || client->disable_mtls) {
    return CURLE_OK;
  }

  ctx = (SSL_CTX *)ssl_ctx;
  if (client->tls_bundle.client_cert == NULL ||
      client->tls_bundle.client_key == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }

  for (index = 0U; index < client->tls_bundle.chain_cert_count; ++index) {
    X509 *copy;

    copy = X509_dup(client->tls_bundle.chain_certs[index]);
    if (copy == NULL) {
      return CURLE_OUT_OF_MEMORY;
    }
    if (SSL_CTX_add_extra_chain_cert(ctx, copy) != 1) {
      X509_free(copy);
      return CURLE_SSL_CERTPROBLEM;
    }
  }

  store = SSL_CTX_get_cert_store(ctx);
  for (index = 0U; index < client->tls_bundle.ca_cert_count; ++index) {
    if (X509_STORE_add_cert(store, client->tls_bundle.ca_certs[index]) != 1) {
      ERR_clear_error();
    }
  }

  return CURLE_OK;
}

int lc_engine_http_json_request(
    lc_engine_client *client, const char *method, const char *path,
    const void *body, size_t body_length, const lc_engine_header_pair *headers,
    size_t header_count, const lonejson_map *response_map, void *response,
    lc_engine_http_result *result, lc_engine_error *error) {
  size_t endpoint_index;
  char *endpoint_list;

  if (client == NULL || method == NULL || path == NULL || result == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, method, path, and result are required");
  }

  memset(result, 0, sizeof(*result));
  endpoint_list = lc_engine_join_endpoints(client);
  if (endpoint_list != NULL) {
    pslog_field order_fields[3];

    order_fields[0] = lc_log_str_field("method", method);
    order_fields[1] = lc_log_str_field("path", path);
    order_fields[2] = lc_log_str_field("endpoints", endpoint_list);
    lc_log_trace(client->logger, "http.order", order_fields, 3U);
    free(endpoint_list);
  }

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    CURL *easy;
    struct curl_slist *curl_headers;
    lc_engine_buffer url;
    size_t header_index;
    char error_buffer[CURL_ERROR_SIZE];
    CURLcode curl_code;
    int should_retry;
    long request_timeout_ms;
    lc_engine_json_http_state state;

    if (!lc_engine_client_attempt_timeout_ms(client, &request_timeout_ms)) {
      lc_engine_http_result_cleanup(result);
      return lc_engine_set_transport_error(error, "request deadline elapsed");
    }
    lc_engine_http_result_cleanup(result);
    lc_engine_buffer_init(&url);
    lc_engine_build_url(client, client->endpoints[endpoint_index], path, &url);

    easy = curl_easy_init();
    curl_headers = NULL;
    memset(error_buffer, 0, sizeof(error_buffer));
    memset(&state, 0, sizeof(state));
    state.client = client;
    state.result = result;
    state.error = error;
    state.response_map = response_map;
    state.response_dst = response;
    state.byte_limit = client->http_json_response_limit_bytes > 0U
                           ? client->http_json_response_limit_bytes
                           : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    state.error_byte_limit = (size_t)LC_ENGINE_HTTP_ERROR_BODY_LIMIT_DEFAULT;

    if (easy == NULL) {
      lc_engine_buffer_cleanup(&url);
      return lc_engine_set_transport_error(error, "curl_easy_init failed");
    }

    curl_easy_setopt(easy, CURLOPT_URL, url.data);
    {
      pslog_field attempt_fields[5];

      attempt_fields[0] = lc_log_str_field("method", method);
      attempt_fields[1] = lc_log_str_field("path", path);
      attempt_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      attempt_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      attempt_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      lc_log_trace(client->logger, "http.attempt", attempt_fields, 5U);
    }
    if (strcmp(method, "POST") != 0) {
      curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method);
    }
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,
                     lc_engine_json_http_write_callback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &state);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,
                     lc_engine_json_http_header_callback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, &state);
    curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, error_buffer);
    curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                     client->prefer_http_2 ? CURL_HTTP_VERSION_2TLS
                                           : CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION,
                     lc_engine_transport_progress);
    curl_easy_setopt(easy, CURLOPT_XFERINFODATA, client);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, request_timeout_ms);
    if (client->unix_socket_path != NULL &&
        client->unix_socket_path[0] != '\0') {
      curl_easy_setopt(easy, CURLOPT_UNIX_SOCKET_PATH,
                       client->unix_socket_path);
    }

    if (!client->disable_mtls) {
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER,
                       client->insecure_skip_verify ? 0L : 1L);
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST,
                       client->insecure_skip_verify ? 0L : 2L);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_FUNCTION,
                       lc_engine_ssl_ctx_callback);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_DATA, client);
    }

    for (header_index = 0U; header_index < header_count; ++header_index) {
      lc_engine_buffer line;

      lc_engine_buffer_init(&line);
      lc_engine_buffer_append_cstr(&line, headers[header_index].name);
      lc_engine_buffer_append_cstr(&line, ": ");
      lc_engine_buffer_append_cstr(&line, headers[header_index].value);
      curl_headers = curl_slist_append(curl_headers, line.data);
      lc_engine_buffer_cleanup(&line);
      if (curl_headers == NULL) {
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate curl header list");
      }
    }

    if (strcmp(method, "POST") == 0) {
      curl_easy_setopt(easy, CURLOPT_POST, 1L);
      if (body != NULL) {
        curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, (long)body_length);
      } else {
        curl_easy_setopt(easy, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, 0L);
      }
    } else if (body != NULL) {
      curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body);
      curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, (long)body_length);
    }

    if (curl_headers != NULL) {
      curl_easy_setopt(easy, CURLOPT_HTTPHEADER, curl_headers);
    }

    curl_code = curl_easy_perform(easy);
    if (curl_code != CURLE_OK) {
      pslog_field error_fields[6];
      const char *error_text;
      int return_code;

      error_text = error_buffer[0] != '\0' ? error_buffer
                                           : curl_easy_strerror(curl_code);
      error_fields[0] = lc_log_str_field("method", method);
      error_fields[1] = lc_log_str_field("path", path);
      error_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      error_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      error_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      error_fields[5] = lc_log_str_field("error", error_text);
      lc_log_trace(client->logger, "http.error", error_fields, 6U);
      if (curl_code == CURLE_ABORTED_BY_CALLBACK &&
          lc_engine_transport_cancel_requested(client)) {
        lc_engine_set_transport_error(error, "request cancelled");
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
        should_retry = 0;
      } else if (state.limit_exceeded) {
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (state.parser_initialized != 0 &&
                 (state.parse.error.code != LONEJSON_STATUS_OK ||
                  (error != NULL && error->code != LC_ENGINE_OK))) {
        return_code = error != NULL && error->code != LC_ENGINE_OK
                          ? error->code
                          : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (result->header_parse_failed) {
        lc_engine_set_protocol_error(error,
                                     result->header_parse_error_message != NULL
                                         ? result->header_parse_error_message
                                         : "response header is invalid");
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (endpoint_index + 1U < client->endpoint_count) {
        should_retry = 1;
        return_code = LC_ENGINE_OK;
      } else {
        if (error_buffer[0] != '\0') {
          lc_engine_set_transport_error(error, error_buffer);
        } else {
          lc_engine_set_transport_error(error, curl_easy_strerror(curl_code));
        }
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
        should_retry = 0;
      }
      curl_slist_free_all(curl_headers);
      curl_easy_cleanup(easy);
      lc_engine_buffer_cleanup(&url);
      if (state.parser_initialized != 0 && state.parser_is_error == 0 &&
          state.response_map != NULL && state.response_dst != NULL) {
        lc_engine_runtime(client)->cleanup(
            lc_engine_runtime(client), state.response_map, state.response_dst);
      }
      lc_engine_json_http_state_cleanup(&state);
      lc_engine_http_result_cleanup(result);
      if (should_retry) {
        continue;
      }
      return return_code;
    }

    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &result->http_status);
    if (state.parser_initialized && state.parser_is_error) {
      lonejson_status parse_status;

      parse_status = state.parse.finish(&state.parse);
      if (parse_status != LONEJSON_STATUS_OK) {
        lc_engine_lonejson_error_from_status(
            error, parse_status, &state.parse.error,
            "failed to finish typed JSON error response");
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
      }
      result->server_error_code =
          lc_engine_strdup_local(state.error_body.server_error_code);
      result->detail = lc_engine_strdup_local(state.error_body.detail);
      result->leader_endpoint =
          lc_engine_strdup_local(state.error_body.leader_endpoint);
      result->current_etag =
          lc_engine_strdup_local(state.error_body.current_etag);
      result->current_version = (long)state.error_body.current_version;
      result->retry_after_seconds = (long)state.error_body.retry_after_seconds;
      if ((state.error_body.server_error_code != NULL &&
           result->server_error_code == NULL) ||
          (state.error_body.detail != NULL && result->detail == NULL) ||
          (state.error_body.leader_endpoint != NULL &&
           result->leader_endpoint == NULL) ||
          (state.error_body.current_etag != NULL &&
           result->current_etag == NULL)) {
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to copy error response");
      }
    }
    if (state.parser_initialized) {
      lonejson_status parse_status;

      parse_status = state.parse.finish(&state.parse);
      if (parse_status != LONEJSON_STATUS_OK) {
        lc_engine_lonejson_error_from_status(
            error, parse_status, &state.parse.error,
            "failed to finish typed JSON response");
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
      }
    }
    {
      pslog_field success_fields[7];

      success_fields[0] = lc_log_str_field("method", method);
      success_fields[1] = lc_log_str_field("path", path);
      success_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      success_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      success_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      success_fields[5] = pslog_i64("status", (pslog_int64)result->http_status);
      success_fields[6] = lc_log_str_field("cid", result->correlation_id);
      lc_log_trace(client->logger, "http.success", success_fields, 7U);
    }
    curl_slist_free_all(curl_headers);
    curl_easy_cleanup(easy);
    lc_engine_buffer_cleanup(&url);
    lc_engine_json_http_state_cleanup(&state);

    should_retry = 0;
    if (result->http_status == 503L &&
        endpoint_index + 1U < client->endpoint_count) {
      if (result->server_error_code != NULL &&
          strcmp(result->server_error_code, "node_passive") == 0) {
        should_retry = 1;
      }
    }
    if (should_retry) {
      lc_engine_http_result_cleanup(result);
      continue;
    }
    return LC_ENGINE_OK;
  }

  {
    pslog_field failure_fields[3];

    failure_fields[0] = lc_log_str_field("method", method);
    failure_fields[1] = lc_log_str_field("path", path);
    failure_fields[2] =
        lc_log_str_field("error", error != NULL ? error->message : NULL);
    lc_log_debug(client->logger, "http.unreachable", failure_fields, 3U);
  }
  return lc_engine_set_transport_error(error,
                                       "all configured endpoints failed");
}

int lc_engine_http_json_request_stream(
    lc_engine_client *client, const char *method, const char *path,
    const lonejson_map *body_map, const void *body_src,
    const lc_engine_header_pair *headers, size_t header_count,
    const lonejson_map *response_map, void *response,
    lc_engine_http_result *result, lc_engine_error *error) {
  size_t endpoint_index;
  char *endpoint_list;

  if (client == NULL || method == NULL || path == NULL || result == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, method, path, and result are required");
  }
  if ((body_map == NULL) != (body_src == NULL)) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "body map and source must either both be provided or both be null");
  }

  memset(result, 0, sizeof(*result));
  endpoint_list = lc_engine_join_endpoints(client);
  if (endpoint_list != NULL) {
    pslog_field order_fields[3];

    order_fields[0] = lc_log_str_field("method", method);
    order_fields[1] = lc_log_str_field("path", path);
    order_fields[2] = lc_log_str_field("endpoints", endpoint_list);
    lc_log_trace(client->logger, "http.order", order_fields, 3U);
    free(endpoint_list);
  }

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    CURL *easy;
    struct curl_slist *curl_headers;
    lc_engine_buffer url;
    size_t header_index;
    char error_buffer[CURL_ERROR_SIZE];
    CURLcode curl_code;
    int should_retry;
    lc_engine_json_http_state state;
    lonejson_curl_upload body_upload;
    size_t measured_body_length;
    int has_measured_body_length;
    long request_timeout_ms;

    if (!lc_engine_client_attempt_timeout_ms(client, &request_timeout_ms)) {
      lc_engine_http_result_cleanup(result);
      return lc_engine_set_transport_error(error, "request deadline elapsed");
    }

    lc_engine_http_result_cleanup(result);
    lc_engine_buffer_init(&url);
    lc_engine_build_url(client, client->endpoints[endpoint_index], path, &url);

    easy = curl_easy_init();
    curl_headers = NULL;
    memset(&body_upload, 0, sizeof(body_upload));
    memset(error_buffer, 0, sizeof(error_buffer));
    memset(&state, 0, sizeof(state));
    measured_body_length = 0U;
    has_measured_body_length = 0;
    state.client = client;
    state.result = result;
    state.error = error;
    state.response_map = response_map;
    state.response_dst = response;
    state.byte_limit = client->http_json_response_limit_bytes > 0U
                           ? client->http_json_response_limit_bytes
                           : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    state.error_byte_limit = (size_t)LC_ENGINE_HTTP_ERROR_BODY_LIMIT_DEFAULT;

    if (easy == NULL) {
      lc_engine_buffer_cleanup(&url);
      return lc_engine_set_transport_error(error, "curl_easy_init failed");
    }

    curl_easy_setopt(easy, CURLOPT_URL, url.data);
    {
      pslog_field attempt_fields[5];

      attempt_fields[0] = lc_log_str_field("method", method);
      attempt_fields[1] = lc_log_str_field("path", path);
      attempt_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      attempt_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      attempt_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      lc_log_trace(client->logger, "http.attempt", attempt_fields, 5U);
    }
    if (strcmp(method, "POST") != 0) {
      curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method);
    }
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,
                     lc_engine_json_http_write_callback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &state);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,
                     lc_engine_json_http_header_callback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, &state);
    curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, error_buffer);
    curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                     client->prefer_http_2 ? CURL_HTTP_VERSION_2TLS
                                           : CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION,
                     lc_engine_transport_progress);
    curl_easy_setopt(easy, CURLOPT_XFERINFODATA, client);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, request_timeout_ms);
    if (client->unix_socket_path != NULL &&
        client->unix_socket_path[0] != '\0') {
      curl_easy_setopt(easy, CURLOPT_UNIX_SOCKET_PATH,
                       client->unix_socket_path);
    }

    if (!client->disable_mtls) {
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER,
                       client->insecure_skip_verify ? 0L : 1L);
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST,
                       client->insecure_skip_verify ? 0L : 2L);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_FUNCTION,
                       lc_engine_ssl_ctx_callback);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_DATA, client);
    }

    for (header_index = 0U; header_index < header_count; ++header_index) {
      lc_engine_buffer line;

      lc_engine_buffer_init(&line);
      lc_engine_buffer_append_cstr(&line, headers[header_index].name);
      lc_engine_buffer_append_cstr(&line, ": ");
      lc_engine_buffer_append_cstr(&line, headers[header_index].value);
      curl_headers = curl_slist_append(curl_headers, line.data);
      lc_engine_buffer_cleanup(&line);
      if (curl_headers == NULL) {
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate curl header list");
      }
    }

    if (body_map != NULL) {
      curl_off_t upload_size;
      lonejson_status upload_status;

      upload_status = lc_engine_runtime(client)->curl_upload_init(
          lc_engine_runtime(client), &body_upload, body_map, body_src);
      if (upload_status != LONEJSON_STATUS_OK) {
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        return lc_engine_lonejson_error_from_status(
            error, upload_status, NULL, "failed to initialize JSON upload");
      }
      upload_size = body_upload.size_fn(&body_upload);
      if (upload_size >= 0 && upload_size == (curl_off_t)(size_t)upload_size) {
        measured_body_length = (size_t)upload_size;
        has_measured_body_length = 1;
      }
      if (strcmp(method, "POST") == 0) {
        curl_easy_setopt(easy, CURLOPT_POST, 1L);
        if (has_measured_body_length) {
          curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE,
                           (curl_off_t)measured_body_length);
        }
      } else {
        curl_easy_setopt(easy, CURLOPT_UPLOAD, 1L);
        if (has_measured_body_length) {
          curl_easy_setopt(easy, CURLOPT_INFILESIZE_LARGE,
                           (curl_off_t)measured_body_length);
        }
      }
      curl_easy_setopt(easy, CURLOPT_READFUNCTION,
                       lc_engine_lonejson_curl_upload_read_callback);
      curl_easy_setopt(easy, CURLOPT_READDATA, &body_upload);
      curl_easy_setopt(easy, CURLOPT_SEEKFUNCTION, lonejson_curl_seek_callback);
      curl_easy_setopt(easy, CURLOPT_SEEKDATA, &body_upload);
    } else if (strcmp(method, "POST") == 0) {
      curl_easy_setopt(easy, CURLOPT_POST, 1L);
    }

    if (curl_headers != NULL) {
      curl_easy_setopt(easy, CURLOPT_HTTPHEADER, curl_headers);
    }

    curl_code = curl_easy_perform(easy);
    if (body_map != NULL) {
      lc_lonejson_curl_upload_cleanup(&body_upload);
    }
    if (curl_code != CURLE_OK) {
      pslog_field error_fields[6];
      const char *error_text;
      int return_code;

      error_text = error_buffer[0] != '\0' ? error_buffer
                                           : curl_easy_strerror(curl_code);
      error_fields[0] = lc_log_str_field("method", method);
      error_fields[1] = lc_log_str_field("path", path);
      error_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      error_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      error_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      error_fields[5] = lc_log_str_field("error", error_text);
      lc_log_trace(client->logger, "http.error", error_fields, 6U);
      if (curl_code == CURLE_ABORTED_BY_CALLBACK &&
          lc_engine_transport_cancel_requested(client)) {
        lc_engine_set_transport_error(error, "request cancelled");
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
        should_retry = 0;
      } else if (state.limit_exceeded) {
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (state.parser_initialized != 0 &&
                 (state.parse.error.code != LONEJSON_STATUS_OK ||
                  (error != NULL && error->code != LC_ENGINE_OK))) {
        return_code = error != NULL && error->code != LC_ENGINE_OK
                          ? error->code
                          : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (result->header_parse_failed) {
        lc_engine_set_protocol_error(error,
                                     result->header_parse_error_message != NULL
                                         ? result->header_parse_error_message
                                         : "response header is invalid");
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
        should_retry = 0;
      } else if (endpoint_index + 1U < client->endpoint_count) {
        should_retry = 1;
        return_code = LC_ENGINE_OK;
      } else {
        if (error_buffer[0] != '\0') {
          lc_engine_set_transport_error(error, error_buffer);
        } else {
          lc_engine_set_transport_error(error, curl_easy_strerror(curl_code));
        }
        return_code = error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
        should_retry = 0;
      }
      curl_slist_free_all(curl_headers);
      curl_easy_cleanup(easy);
      lc_engine_buffer_cleanup(&url);
      if (state.parser_initialized != 0 && state.parser_is_error == 0 &&
          state.response_map != NULL && state.response_dst != NULL) {
        lc_engine_runtime(client)->cleanup(
            lc_engine_runtime(client), state.response_map, state.response_dst);
      }
      lc_engine_json_http_state_cleanup(&state);
      lc_engine_http_result_cleanup(result);
      if (should_retry) {
        continue;
      }
      return return_code;
    }

    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &result->http_status);
    if (state.parser_initialized && state.parser_is_error) {
      lonejson_status parse_status;

      parse_status = state.parse.finish(&state.parse);
      if (parse_status != LONEJSON_STATUS_OK) {
        lc_engine_lonejson_error_from_status(
            error, parse_status, &state.parse.error,
            "failed to finish typed JSON error response");
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
      }
      result->server_error_code =
          lc_engine_strdup_local(state.error_body.server_error_code);
      result->detail = lc_engine_strdup_local(state.error_body.detail);
      result->leader_endpoint =
          lc_engine_strdup_local(state.error_body.leader_endpoint);
      result->current_etag =
          lc_engine_strdup_local(state.error_body.current_etag);
      result->current_version = (long)state.error_body.current_version;
      result->retry_after_seconds = (long)state.error_body.retry_after_seconds;
      if ((state.error_body.server_error_code != NULL &&
           result->server_error_code == NULL) ||
          (state.error_body.detail != NULL && result->detail == NULL) ||
          (state.error_body.leader_endpoint != NULL &&
           result->leader_endpoint == NULL) ||
          (state.error_body.current_etag != NULL &&
           result->current_etag == NULL)) {
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to copy error response");
      }
    }
    if (state.parser_initialized) {
      lonejson_status parse_status;

      parse_status = state.parse.finish(&state.parse);
      if (parse_status != LONEJSON_STATUS_OK) {
        lc_engine_lonejson_error_from_status(
            error, parse_status, &state.parse.error,
            "failed to finish typed JSON response");
        curl_slist_free_all(curl_headers);
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        lc_engine_json_http_state_cleanup(&state);
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
      }
    }
    {
      pslog_field success_fields[7];

      success_fields[0] = lc_log_str_field("method", method);
      success_fields[1] = lc_log_str_field("path", path);
      success_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      success_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      success_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      success_fields[5] = pslog_i64("status", (pslog_int64)result->http_status);
      success_fields[6] = lc_log_str_field("cid", result->correlation_id);
      lc_log_trace(client->logger, "http.success", success_fields, 7U);
    }
    curl_slist_free_all(curl_headers);
    curl_easy_cleanup(easy);
    lc_engine_buffer_cleanup(&url);
    lc_engine_json_http_state_cleanup(&state);

    should_retry = 0;
    if (result->http_status == 503L &&
        endpoint_index + 1U < client->endpoint_count) {
      if (result->server_error_code != NULL &&
          strcmp(result->server_error_code, "node_passive") == 0) {
        should_retry = 1;
      }
    }
    if (should_retry) {
      lc_engine_http_result_cleanup(result);
      continue;
    }
    return LC_ENGINE_OK;
  }

  {
    pslog_field failure_fields[3];

    failure_fields[0] = lc_log_str_field("method", method);
    failure_fields[1] = lc_log_str_field("path", path);
    failure_fields[2] =
        lc_log_str_field("error", error != NULL ? error->message : NULL);
    lc_log_debug(client->logger, "http.unreachable", failure_fields, 3U);
  }
  return lc_engine_set_transport_error(error,
                                       "all configured endpoints failed");
}

static int lc_engine_store_x509(X509 ***items, size_t *item_count,
                                X509 *value) {
  X509 **next_items;

  next_items = (X509 **)realloc(*items, (*item_count + 1U) * sizeof(X509 *));
  if (next_items == NULL) {
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  X509_up_ref(value);
  next_items[*item_count] = value;
  *items = next_items;
  *item_count += 1U;
  return LC_ENGINE_OK;
}

static int lc_engine_match_private_key(X509 *certificate, EVP_PKEY *candidate) {
  EVP_PKEY *public_key;
  int match;

  public_key = X509_get_pubkey(certificate);
  if (public_key == NULL) {
    return 0;
  }
  match = EVP_PKEY_eq(public_key, candidate) == 1;
  EVP_PKEY_free(public_key);
  return match;
}

typedef struct lc_engine_source_bio_state {
  lc_source *source;
  lc_error error;
  int failed;
} lc_engine_source_bio_state;

static int lc_engine_source_bio_create(BIO *bio) {
  BIO_set_init(bio, 1);
  BIO_set_data(bio, NULL);
  return 1;
}

static int lc_engine_source_bio_destroy(BIO *bio) {
  if (bio == NULL) {
    return 0;
  }
  BIO_set_data(bio, NULL);
  BIO_set_init(bio, 0);
  return 1;
}

static int lc_engine_source_bio_read(BIO *bio, char *buffer, int count) {
  lc_engine_source_bio_state *state;
  size_t nread;

  if (bio == NULL || buffer == NULL || count <= 0) {
    return 0;
  }
  state = (lc_engine_source_bio_state *)BIO_get_data(bio);
  if (state == NULL || state->source == NULL) {
    return 0;
  }
  BIO_clear_retry_flags(bio);
  nread =
      state->source->read(state->source, buffer, (size_t)count, &state->error);
  if (nread == 0U && state->error.code != LC_OK) {
    state->failed = 1;
    return -1;
  }
  return (int)nread;
}

static BIO_METHOD *lc_engine_new_source_bio_method(void) {
  BIO_METHOD *method;

  method = BIO_meth_new(BIO_TYPE_SOURCE_SINK, "liblockdc source");
  if (method == NULL) {
    return NULL;
  }
  if (BIO_meth_set_create(method, lc_engine_source_bio_create) != 1 ||
      BIO_meth_set_destroy(method, lc_engine_source_bio_destroy) != 1 ||
      BIO_meth_set_read(method, lc_engine_source_bio_read) != 1) {
    BIO_meth_free(method);
    return NULL;
  }
  return method;
}

static int lc_engine_load_bundle_from_bio(lc_engine_client *client, BIO *bio,
                                          lc_engine_error *error) {
  STACK_OF(X509_INFO) * info_stack;
  size_t index;
  int rc;

  info_stack = PEM_X509_INFO_read_bio(bio, NULL, NULL, NULL);
  if (info_stack == NULL) {
    return lc_engine_set_protocol_error(error,
                                        "failed to parse client bundle PEM");
  }

  rc = LC_ENGINE_OK;
  for (index = 0U; index < (size_t)sk_X509_INFO_num(info_stack); ++index) {
    X509_INFO *info;

    info = sk_X509_INFO_value(info_stack, (int)index);
    if (info->x509 != NULL) {
      if (X509_check_ca(info->x509)) {
        rc =
            lc_engine_store_x509(&client->tls_bundle.ca_certs,
                                 &client->tls_bundle.ca_cert_count, info->x509);
      } else if (client->tls_bundle.client_cert == NULL) {
        X509_up_ref(info->x509);
        client->tls_bundle.client_cert = info->x509;
      } else {
        rc = lc_engine_store_x509(&client->tls_bundle.chain_certs,
                                  &client->tls_bundle.chain_cert_count,
                                  info->x509);
      }
      if (rc != LC_ENGINE_OK) {
        break;
      }
    }
  }

  if (rc == LC_ENGINE_OK && client->tls_bundle.client_cert != NULL) {
    for (index = 0U; index < (size_t)sk_X509_INFO_num(info_stack); ++index) {
      X509_INFO *info;

      info = sk_X509_INFO_value(info_stack, (int)index);
      if (info->x_pkey != NULL && info->x_pkey->dec_pkey != NULL &&
          lc_engine_match_private_key(client->tls_bundle.client_cert,
                                      info->x_pkey->dec_pkey)) {
        EVP_PKEY_up_ref(info->x_pkey->dec_pkey);
        client->tls_bundle.client_key = info->x_pkey->dec_pkey;
        break;
      }
    }
  }

  sk_X509_INFO_pop_free(info_stack, X509_INFO_free);

  if (rc != LC_ENGINE_OK) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_client_error(error, rc,
                                      "failed to retain parsed client bundle");
  }
  if (client->tls_bundle.client_cert == NULL) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a client certificate");
  }
  if (client->tls_bundle.client_key == NULL) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a matching private key");
  }
  if (client->tls_bundle.ca_cert_count == 0U) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a CA certificate");
  }
  return LC_ENGINE_OK;
}

int lc_engine_load_bundle(lc_engine_client *client, lc_source *bundle_source,
                          const char *bundle_path, lc_engine_error *error) {
  BIO *bio;
  int rc;

  if (client == NULL || (bundle_source == NULL && bundle_path == NULL)) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client and bundle source or path are required");
  }

  if (bundle_source != NULL) {
    BIO_METHOD *method;
    lc_engine_source_bio_state state;

    memset(&state, 0, sizeof(state));
    state.source = bundle_source;
    method = lc_engine_new_source_bio_method();
    if (method == NULL) {
      return lc_engine_set_transport_error(
          error, "failed to allocate client bundle source BIO method");
    }
    bio = BIO_new(method);
    if (bio == NULL) {
      BIO_meth_free(method);
      return lc_engine_set_transport_error(
          error, "failed to allocate client bundle BIO");
    }
    BIO_set_data(bio, &state);
    BIO_set_init(bio, 1);
    {
      BIO *buffered_bio;

      buffered_bio = BIO_new(BIO_f_buffer());
      if (buffered_bio == NULL) {
        BIO_free(bio);
        BIO_meth_free(method);
        return lc_engine_set_transport_error(
            error, "failed to allocate buffered client bundle BIO");
      }
      BIO_push(buffered_bio, bio);
      rc = lc_engine_load_bundle_from_bio(client, buffered_bio, error);
      BIO_free_all(buffered_bio);
    }
    BIO_meth_free(method);
    if (state.failed) {
      const char *message;

      message = state.error.message != NULL
                    ? state.error.message
                    : "failed to read client bundle source";
      lc_engine_set_transport_error(error, message);
      lc_error_cleanup(&state.error);
      return LC_ENGINE_ERROR_TRANSPORT;
    }
    lc_error_cleanup(&state.error);
    return rc;
  }

  bio = BIO_new_file(bundle_path, "rb");
  if (bio == NULL) {
    return lc_engine_set_transport_error(error, "failed to open client bundle");
  }
  {
    int rc;

    rc = lc_engine_load_bundle_from_bio(client, bio, error);
    BIO_free(bio);
    return rc;
  }
}

void lc_engine_free_bundle(lc_engine_tls_bundle *bundle) {
  size_t index;

  if (bundle == NULL) {
    return;
  }
  if (bundle->client_cert != NULL) {
    X509_free(bundle->client_cert);
  }
  if (bundle->client_key != NULL) {
    EVP_PKEY_free(bundle->client_key);
  }
  for (index = 0U; index < bundle->ca_cert_count; ++index) {
    X509_free(bundle->ca_certs[index]);
  }
  for (index = 0U; index < bundle->chain_cert_count; ++index) {
    X509_free(bundle->chain_certs[index]);
  }
  free(bundle->ca_certs);
  free(bundle->chain_certs);
  memset(bundle, 0, sizeof(*bundle));
}

const char *lc_engine_effective_namespace(lc_engine_client *client,
                                          const char *ns) {
  if (ns != NULL && ns[0] != '\0') {
    return ns;
  }
  if (client != NULL && client->default_namespace != NULL &&
      client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

char *lc_engine_url_encode(const char *value) {
  CURL *easy;
  char *escaped;
  char *copy;

  if (value == NULL) {
    return lc_engine_strdup_local("");
  }
  easy = curl_easy_init();
  if (easy == NULL) {
    return NULL;
  }
  escaped = curl_easy_escape(easy, value, 0);
  curl_easy_cleanup(easy);
  if (escaped == NULL) {
    return NULL;
  }
  copy = lc_engine_strdup_local(escaped);
  curl_free(escaped);
  return copy;
}

int lc_engine_set_server_error_from_result(
    lc_engine_error *error, const lc_engine_http_result *result) {
  int rc;

  rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_SERVER,
                                  "lockd returned a non-success status");
  if (rc == LC_ENGINE_ERROR_NO_MEMORY) {
    return rc;
  }

  if (error != NULL) {
    error->http_status = result->http_status;
    if (result->correlation_id != NULL) {
      error->correlation_id = lc_engine_strdup_local(result->correlation_id);
      if (error->correlation_id == NULL) {
        return LC_ENGINE_ERROR_NO_MEMORY;
      }
    }
    if (result->server_error_code != NULL) {
      error->server_error_code =
          lc_engine_strdup_local(result->server_error_code);
      if (error->server_error_code == NULL) {
        return LC_ENGINE_ERROR_NO_MEMORY;
      }
      if (result->detail != NULL) {
        error->detail = lc_engine_strdup_local(result->detail);
        if (error->detail == NULL) {
          return LC_ENGINE_ERROR_NO_MEMORY;
        }
      }
      if (result->leader_endpoint != NULL) {
        error->leader_endpoint =
            lc_engine_strdup_local(result->leader_endpoint);
        if (error->leader_endpoint == NULL) {
          return LC_ENGINE_ERROR_NO_MEMORY;
        }
      }
      if (result->current_etag != NULL) {
        error->current_etag = lc_engine_strdup_local(result->current_etag);
        if (error->current_etag == NULL) {
          return LC_ENGINE_ERROR_NO_MEMORY;
        }
      }
      error->current_version = result->current_version;
      error->retry_after_seconds = result->retry_after_seconds;
    }
  }

  return LC_ENGINE_ERROR_SERVER;
}

static int lc_engine_open_lonejson_runtime(lc_engine_client *client,
                                           lc_engine_error *error) {
  lonejson_config config;
  lonejson_error lj_error;
  size_t limit;

  if (client == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "client is required");
  }
  limit = lc_lonejson_runtime_limit(client->http_json_response_limit_bytes);
  lc_lonejson_config_init(&config, limit);
  lonejson_error_init(&lj_error);
  client->lonejson = lonejson_new(&config, &lj_error);
  if (client->lonejson == NULL) {
    return lc_engine_lonejson_error_from_status(
        error, lj_error.code, &lj_error,
        "failed to initialize lonejson runtime");
  }
  return LC_ENGINE_OK;
}

int lc_engine_set_server_error_from_json(lc_engine_client *client,
                                         lc_engine_error *error,
                                         long http_status,
                                         const char *correlation_id,
                                         const char *json) {
  lc_engine_http_error_json parsed;
  lc_engine_http_result synthetic;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  if (json == NULL || json[0] == '\0') {
    memset(&synthetic, 0, sizeof(synthetic));
    synthetic.http_status = http_status;
    synthetic.correlation_id = (char *)correlation_id;
    return lc_engine_set_server_error_from_result(error, &synthetic);
  }

  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  lc_engine_runtime(client)->init(lc_engine_runtime(client),
                                  &lc_engine_http_error_map, &parsed);
  status = lc_engine_runtime(client)->parse_cstr(lc_engine_runtime(client),
                                                 &lc_engine_http_error_map,
                                                 &parsed, json, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to decode error response body");
  if (rc != LC_ENGINE_OK) {
    lc_engine_runtime(client)->cleanup(lc_engine_runtime(client),
                                       &lc_engine_http_error_map, &parsed);
    return rc;
  }

  memset(&synthetic, 0, sizeof(synthetic));
  synthetic.http_status = http_status;
  synthetic.correlation_id = (char *)correlation_id;
  synthetic.server_error_code = parsed.server_error_code;
  synthetic.detail = parsed.detail;
  synthetic.leader_endpoint = parsed.leader_endpoint;
  synthetic.current_etag = parsed.current_etag;
  synthetic.current_version = (long)parsed.current_version;
  synthetic.retry_after_seconds = (long)parsed.retry_after_seconds;
  rc = lc_engine_set_server_error_from_result(error, &synthetic);
  lc_engine_runtime(client)->cleanup(lc_engine_runtime(client),
                                     &lc_engine_http_error_map, &parsed);
  return rc;
}

int lc_engine_client_open(const lc_engine_client_config *config,
                          lc_engine_client **out_client,
                          lc_engine_error *error) {
  lc_engine_client *client;
  size_t index;

  if (config == NULL || out_client == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "config and out_client are required");
  }
  if (config->endpoints == NULL || config->endpoint_count == 0U) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "at least one endpoint is required");
  }
  if ((config->allocator.malloc_fn == NULL) !=
          (config->allocator.free_fn == NULL) ||
      (config->allocator.realloc_fn != NULL &&
       config->allocator.malloc_fn == NULL))
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "custom allocators require matching malloc/free hooks");
  if (!config->disable_mtls && config->client_bundle_source == NULL &&
      (config->client_bundle_path == NULL ||
       config->client_bundle_path[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client_bundle_source or client_bundle_path is required unless "
        "disable_mtls is enabled");
  }

  curl_global_init(CURL_GLOBAL_DEFAULT);
  OPENSSL_init_ssl(0U, NULL);

  client = (lc_engine_client *)calloc(1U, sizeof(*client));
  if (client == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate client");
  }

  client->endpoint_count = config->endpoint_count;
  client->endpoints = (char **)calloc(client->endpoint_count, sizeof(char *));
  if (client->endpoints == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate endpoint array");
  }
  for (index = 0U; index < client->endpoint_count; ++index) {
    client->endpoints[index] = lc_engine_strdup_local(config->endpoints[index]);
    if (client->endpoints[index] == NULL) {
      lc_engine_client_close(client);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to copy endpoint");
    }
    lc_engine_trim_trailing_slash(client->endpoints[index]);
  }

  client->default_namespace = lc_engine_strdup_local(
      config->default_namespace != NULL ? config->default_namespace
                                        : "default");
  if (client->default_namespace == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy default namespace");
  }
  client->unix_socket_path = lc_engine_strdup_local(config->unix_socket_path);
  if (config->unix_socket_path != NULL && client->unix_socket_path == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy unix_socket_path");
  }

  client->timeout_ms = config->timeout_ms > 0L ? config->timeout_ms : 30000L;
  client->disable_mtls = config->disable_mtls ? 1 : 0;
  client->insecure_skip_verify = config->insecure_skip_verify ? 1 : 0;
  client->prefer_http_2 = config->prefer_http_2 ? 1 : 0;
  client->http_json_response_limit_bytes =
      config->http_json_response_limit_bytes > 0U
          ? config->http_json_response_limit_bytes
          : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
  client->disable_logger_sys_field = config->disable_logger_sys_field ? 1 : 0;
  client->base_logger =
      config->logger != NULL ? config->logger : lc_log_noop_logger();
  client->logger = lc_log_client_logger(client->base_logger,
                                        client->disable_logger_sys_field);
  if (client->logger == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to initialize client logger");
  }
  client->owns_logger = client->logger != client->base_logger;
  if (lc_engine_open_lonejson_runtime(client, error) != LC_ENGINE_OK) {
    lc_engine_client_close(client);
    return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
  }

  if (!client->disable_mtls) {
    if (lc_engine_load_bundle(client, config->client_bundle_source,
                              config->client_bundle_path,
                              error) != LC_ENGINE_OK) {
      lc_engine_client_close(client);
      return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
    }
  }

  *out_client = client;
  {
    pslog_field fields[2];

    fields[0] = lc_log_u64_field("endpoint_count", client->endpoint_count);
    fields[1] =
        lc_log_str_field("default_namespace", client->default_namespace);
    lc_log_info(client->logger, "init", fields, 2U);
  }
  return LC_ENGINE_OK;
}

pslog_logger *lc_engine_client_logger(lc_engine_client *client) {
  if (client == NULL || client->logger == NULL) {
    return lc_log_noop_logger();
  }
  return client->logger;
}

void lc_engine_client_set_cancel_check(lc_engine_client *client,
                                       int (*cancel_check)(void *context),
                                       void *cancel_context) {
  if (client == NULL) {
    return;
  }
  client->cancel_check = cancel_check;
  client->cancel_context = cancel_context;
}

void lc_engine_client_close(lc_engine_client *client) {
  size_t index;

  if (client == NULL) {
    return;
  }
  lc_engine_free_bundle(&client->tls_bundle);
  if (client->endpoints != NULL) {
    for (index = 0U; index < client->endpoint_count; ++index) {
      lc_engine_free_string(&client->endpoints[index]);
    }
  }
  free(client->endpoints);
  lc_engine_free_string(&client->unix_socket_path);
  lc_engine_free_string(&client->default_namespace);
  if (client->lonejson != NULL) {
    client->lonejson->free(client->lonejson);
  }
  if (client->owns_logger && client->logger != NULL) {
    client->logger->destroy(client->logger);
  }
  free(client);
}
