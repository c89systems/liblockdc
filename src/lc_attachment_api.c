#include "lc_api_internal.h"
#include "lc_internal.h"

#include <curl/curl.h>
#include <limits.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

typedef struct lc_engine_http_error_json {
  char *server_error_code;
  char *detail;
  char *leader_endpoint;
  char *current_etag;
  lonejson_int64 current_version;
  lonejson_int64 retry_after_seconds;
} lc_engine_http_error_json;

typedef struct lc_engine_stream_request_state {
  lc_engine_retention_headers retention;
  lc_engine_client *client;
  lc_engine_write_callback writer;
  void *writer_context;
  lc_engine_error *error;
  long http_status;
  char *correlation_id;
  char *content_type;
  char *etag;
  lc_version key_version;
  long fencing_token;
  char *attachment_id;
  char *attachment_name;
  char *attachment_sha256;
  char *attachment_content_type;
  long attachment_size;
  lc_unix_seconds attachment_created_at_unix;
  lc_unix_seconds attachment_updated_at_unix;
  const lonejson_map *response_map;
  void *response_dst;
  lonejson_curl_parse parse;
  lc_engine_http_error_json error_body;
  size_t bytes_received;
  size_t byte_limit;
  int parser_initialized;
  int parser_is_error;
  int limit_exceeded;
  int stream_error;
} lc_engine_stream_request_state;

typedef struct lc_engine_reader_state {
  lc_engine_read_callback reader;
  void *reader_context;
  lc_engine_error *error;
} lc_engine_reader_state;

typedef struct lc_engine_multipart_upload_state {
  const char *prefix;
  size_t prefix_length;
  size_t prefix_offset;
  const char *suffix;
  size_t suffix_length;
  size_t suffix_offset;
  lc_engine_read_callback payload_reader;
  void *payload_context;
  lc_engine_reset_callback payload_reset;
  void *payload_reset_context;
  int payload_done;
} lc_engine_multipart_upload_state;

typedef struct lc_engine_attachment_info_json {
  char *id;
  char *name;
  lonejson_int64 size;
  char *plaintext_sha256;
  char *content_type;
  lonejson_int64 created_at_unix;
  lonejson_int64 updated_at_unix;
} lc_engine_attachment_info_json;

typedef struct lc_engine_attach_response_json {
  lc_engine_attachment_info_json attachment;
  bool noop;
  lonejson_int64 version;
} lc_engine_attach_response_json;

typedef struct lc_engine_list_attachments_response_json {
  char *ns;
  char *key;
  lonejson_object_array attachments;
} lc_engine_list_attachments_response_json;

typedef struct lc_engine_delete_attachment_response_json {
  bool deleted;
  lonejson_int64 version;
} lc_engine_delete_attachment_response_json;

typedef struct lc_engine_delete_all_attachments_response_json {
  lonejson_int64 deleted;
  lonejson_int64 version;
} lc_engine_delete_all_attachments_response_json;

typedef struct lc_engine_enqueue_meta_json {
  char *ns;
  char *queue;
  lonejson_int64 delay_seconds;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 ttl_seconds;
  lonejson_int64 max_attempts;
  char *payload_content_type;
} lc_engine_enqueue_meta_json;

static int lc_engine_validate_update_retry_source(
    const lc_engine_client *client, lc_engine_read_callback reader,
    void *reader_context, lc_engine_error *error) {
  lc_read_bridge *bridge;

  if (client == NULL || client->endpoint_count <= 1U || reader == NULL) {
    return LC_ENGINE_OK;
  }
  if (reader != lc_engine_read_bridge) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "multi-endpoint updates require lc_source-backed rewindable payloads");
  }
  bridge = (lc_read_bridge *)reader_context;
  if (bridge == NULL || bridge->source == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "payload reset requires source");
  }
  if (bridge->source->reset == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "payload source is not rewindable");
  }
  return LC_ENGINE_OK;
}

static const lonejson_field lc_engine_enqueue_meta_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json, ns, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json, queue, "queue"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, delay_seconds,
                       "delay_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, max_attempts,
                       "max_attempts"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json,
                                payload_content_type, "payload_content_type")};

LONEJSON_MAP_DEFINE(lc_engine_enqueue_meta_map, lc_engine_enqueue_meta_json,
                    lc_engine_enqueue_meta_fields);

static const lonejson_field lc_engine_attachment_info_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_attachment_info_json, id, "id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_attachment_info_json, name, "name"),
    LONEJSON_FIELD_I64(lc_engine_attachment_info_json, size, "size"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_attachment_info_json,
                                plaintext_sha256, "plaintext_sha256"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_attachment_info_json, content_type,
                                "content_type"),
    LONEJSON_FIELD_I64(lc_engine_attachment_info_json, created_at_unix,
                       "created_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_attachment_info_json, updated_at_unix,
                       "updated_at_unix")};

LONEJSON_MAP_DEFINE(lc_engine_attachment_info_map,
                    lc_engine_attachment_info_json,
                    lc_engine_attachment_info_fields);

static const lonejson_field lc_engine_attach_response_fields[] = {
    LONEJSON_FIELD_OBJECT(lc_engine_attach_response_json, attachment,
                          "attachment", &lc_engine_attachment_info_map),
    LONEJSON_FIELD_BOOL(lc_engine_attach_response_json, noop, "noop"),
    LONEJSON_FIELD_I64(lc_engine_attach_response_json, version, "version")};

LONEJSON_MAP_DEFINE(lc_engine_attach_response_map,
                    lc_engine_attach_response_json,
                    lc_engine_attach_response_fields);

static const lonejson_field lc_engine_list_attachments_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_list_attachments_response_json, ns,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_list_attachments_response_json, key,
                                "key"),
    LONEJSON_FIELD_OBJECT_ARRAY(
        lc_engine_list_attachments_response_json, attachments, "attachments",
        lc_engine_attachment_info_json, &lc_engine_attachment_info_map,
        LONEJSON_OVERFLOW_FAIL)};

LONEJSON_MAP_DEFINE(lc_engine_list_attachments_response_map,
                    lc_engine_list_attachments_response_json,
                    lc_engine_list_attachments_response_fields);

static const lonejson_field lc_engine_delete_attachment_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_delete_attachment_response_json, deleted,
                        "deleted"),
    LONEJSON_FIELD_I64(lc_engine_delete_attachment_response_json, version,
                       "version")};

LONEJSON_MAP_DEFINE(lc_engine_delete_attachment_response_map,
                    lc_engine_delete_attachment_response_json,
                    lc_engine_delete_attachment_response_fields);

static const lonejson_field lc_engine_delete_all_attachments_response_fields[] =
    {LONEJSON_FIELD_I64(lc_engine_delete_all_attachments_response_json, deleted,
                        "deleted"),
     LONEJSON_FIELD_I64(lc_engine_delete_all_attachments_response_json, version,
                        "version")};

LONEJSON_MAP_DEFINE(lc_engine_delete_all_attachments_response_map,
                    lc_engine_delete_all_attachments_response_json,
                    lc_engine_delete_all_attachments_response_fields);

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

typedef struct lc_engine_update_response_json {
  lonejson_int64 new_version;
  char *new_state_etag;
  lonejson_int64 bytes;
} lc_engine_update_response_json;

static const lonejson_field lc_engine_update_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_update_response_json, new_version,
                       "new_version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_update_response_json, new_state_etag,
                                "new_state_etag"),
    LONEJSON_FIELD_I64(lc_engine_update_response_json, bytes, "bytes")};

LONEJSON_MAP_DEFINE(lc_engine_update_response_map,
                    lc_engine_update_response_json,
                    lc_engine_update_response_fields);

typedef struct lc_engine_enqueue_response_json {
  char *ns;
  char *queue;
  char *message_id;
  lonejson_int64 attempts;
  lonejson_int64 max_attempts;
  lonejson_int64 failure_attempts;
  lonejson_int64 not_visible_until_unix;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 payload_bytes;
} lc_engine_enqueue_response_json;

static const lonejson_field lc_engine_enqueue_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, ns,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, queue,
                                "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, message_id,
                                "message_id"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, attempts, "attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, max_attempts,
                       "max_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, failure_attempts,
                       "failure_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, not_visible_until_unix,
                       "not_visible_until_unix"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json,
                       visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, payload_bytes,
                       "payload_bytes")};

LONEJSON_MAP_DEFINE(lc_engine_enqueue_response_map,
                    lc_engine_enqueue_response_json,
                    lc_engine_enqueue_response_fields);

static size_t lc_engine_multipart_upload_read(void *context, void *buffer,
                                              size_t count,
                                              lc_engine_error *error) {
  lc_engine_multipart_upload_state *state;
  size_t written;
  size_t remaining;
  size_t chunk;

  state = (lc_engine_multipart_upload_state *)context;
  written = 0U;
  while (written < count) {
    if (state->prefix_offset < state->prefix_length) {
      remaining = state->prefix_length - state->prefix_offset;
      chunk = count - written;
      if (chunk > remaining) {
        chunk = remaining;
      }
      memcpy((char *)buffer + written, state->prefix + state->prefix_offset,
             chunk);
      state->prefix_offset += chunk;
      written += chunk;
      continue;
    }
    if (!state->payload_done) {
      chunk = state->payload_reader(state->payload_context,
                                    (char *)buffer + written, count - written,
                                    error);
      if (chunk > 0U) {
        written += chunk;
        continue;
      }
      state->payload_done = 1;
      continue;
    }
    if (state->suffix_offset < state->suffix_length) {
      remaining = state->suffix_length - state->suffix_offset;
      chunk = count - written;
      if (chunk > remaining) {
        chunk = remaining;
      }
      memcpy((char *)buffer + written, state->suffix + state->suffix_offset,
             chunk);
      state->suffix_offset += chunk;
      written += chunk;
      continue;
    }
    break;
  }
  return written;
}

static int lc_engine_multipart_upload_reset(void *context,
                                            lc_engine_error *error) {
  lc_engine_multipart_upload_state *state;
  int rc;

  state = (lc_engine_multipart_upload_state *)context;
  if (state == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "multipart upload reset requires state");
  }
  state->prefix_offset = 0U;
  state->suffix_offset = 0U;
  state->payload_done = 0;
  if (state->payload_reset != NULL) {
    rc = state->payload_reset(state->payload_reset_context, error);
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
  }
  return LC_ENGINE_OK;
}

static CURLcode lc_engine_attachment_ssl_ctx(CURL *curl, void *ssl_ctx,
                                             void *userptr) {
  lc_engine_client *client;
  SSL_CTX *ctx;
  X509_STORE *store;
  size_t index;

  (void)curl;
  client = (lc_engine_client *)userptr;
  ctx = (SSL_CTX *)ssl_ctx;
  if (client == NULL || ctx == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (client->tls_bundle.client_cert != NULL &&
      SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (client->tls_bundle.client_key != NULL &&
      SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  store = SSL_CTX_get_cert_store(ctx);
  if (store == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  for (index = 0U; index < client->tls_bundle.ca_cert_count; ++index) {
    if (client->tls_bundle.ca_certs[index] != NULL) {
      X509_STORE_add_cert(store, client->tls_bundle.ca_certs[index]);
    }
  }
  for (index = 0U; index < client->tls_bundle.chain_cert_count; ++index) {
    if (client->tls_bundle.chain_certs[index] != NULL) {
      X509_up_ref(client->tls_bundle.chain_certs[index]);
      SSL_CTX_add_extra_chain_cert(ctx, client->tls_bundle.chain_certs[index]);
    }
  }
  return CURLE_OK;
}

static int lc_engine_header_name_equals(const char *begin, size_t length,
                                        const char *expected) {
  size_t index;
  size_t expected_length;

  expected_length = strlen(expected);
  if (length != expected_length) {
    return 0;
  }
  for (index = 0U; index < length; ++index) {
    char a;
    char b;

    a = begin[index];
    b = expected[index];
    if (a >= 'A' && a <= 'Z') {
      a = (char)(a - 'A' + 'a');
    }
    if (b >= 'A' && b <= 'Z') {
      b = (char)(b - 'A' + 'a');
    }
    if (a != b) {
      return 0;
    }
  }
  return 1;
}

static int lc_engine_set_header_value(char **slot, const char *begin,
                                      const char *end) {
  char *copy;

  copy = lc_engine_strdup_range(begin, end);
  if (copy == NULL) {
    return 0;
  }
  lc_engine_free_string(slot);
  *slot = copy;
  return 1;
}

static size_t lc_engine_stream_header_callback(char *buffer, size_t size,
                                               size_t nitems, void *userdata) {
  lc_engine_stream_request_state *state;
  size_t total;
  char *colon;
  char *value;
  char *end;
  char status_line[64];
  size_t status_length;
  long parsed;
  int retention_rc;

  state = (lc_engine_stream_request_state *)userdata;
  total = size * nitems;
  if (total == 0U) {
    return 0U;
  }
  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    parsed = 0L;
    sscanf(status_line, "%*s %ld", &parsed);
    state->http_status = parsed;
    memset(&state->retention, 0, sizeof(state->retention));
    return total;
  }
  colon = (char *)memchr(buffer, ':', total);
  if (colon == NULL) {
    return total;
  }
  value = colon + 1;
  end = buffer + total;
  while (value < end && (*value == ' ' || *value == '\t')) {
    ++value;
  }
  while (end > value && (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' ' ||
                         end[-1] == '\t')) {
    --end;
  }

  retention_rc = state->http_status >= 200 && state->http_status < 300
                     ? lc_engine_retention_header(&state->retention, buffer,
                                                  (size_t)(colon - buffer),
                                                  value, (size_t)(end - value))
                     : 0;
  if (retention_rc < 0) {
    lc_engine_set_protocol_error(
        state->error,
        "response state retention headers are invalid or contradictory");
    state->stream_error = 1;
    return 0;
  }
  if (retention_rc > 0)
    return total;

  if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                   "X-Correlation-Id")) {
    if (!lc_engine_set_header_value(&state->correlation_id, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "Content-Type")) {
    if (!lc_engine_set_header_value(&state->content_type, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "ETag")) {
    if (!lc_engine_set_header_value(&state->etag, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Key-Version")) {
    if (!lc_parse_i64_base10_range_checked(value, (size_t)(end - value),
                                           &state->key_version)) {
      lc_engine_set_protocol_error(state->error,
                                   "attachment key version is out of range");
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Fencing-Token")) {
    if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                            &state->fencing_token)) {
      lc_engine_set_protocol_error(state->error,
                                   "attachment fencing token is out of range");
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-ID")) {
    if (!lc_engine_set_header_value(&state->attachment_id, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-Name")) {
    if (!lc_engine_set_header_value(&state->attachment_name, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-SHA256")) {
    if (!lc_engine_set_header_value(&state->attachment_sha256, value, end)) {
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-Size")) {
    if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                            &state->attachment_size)) {
      lc_engine_set_protocol_error(state->error,
                                   "attachment size is out of range");
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-Created-At")) {
    if (!lc_parse_i64_base10_range_checked(
            value, (size_t)(end - value), &state->attachment_created_at_unix)) {
      lc_engine_set_protocol_error(state->error,
                                   "attachment created_at is out of range");
      state->stream_error = 1;
      return 0U;
    }
  } else if (lc_engine_header_name_equals(buffer, (size_t)(colon - buffer),
                                          "X-Attachment-Updated-At")) {
    if (!lc_parse_i64_base10_range_checked(
            value, (size_t)(end - value), &state->attachment_updated_at_unix)) {
      lc_engine_set_protocol_error(state->error,
                                   "attachment updated_at is out of range");
      state->stream_error = 1;
      return 0U;
    }
  }
  return total;
}

static size_t lc_engine_stream_write_callback(char *ptr, size_t size,
                                              size_t nmemb, void *userdata) {
  lc_engine_stream_request_state *state;
  size_t total;

  state = (lc_engine_stream_request_state *)userdata;
  total = size * nmemb;
  if (state->writer != NULL) {
    if (!state->writer(state->writer_context, ptr, total, state->error)) {
      if (state->error->code == LC_ENGINE_OK) {
        lc_engine_set_transport_error(state->error,
                                      "stream writer callback failed");
      }
      state->stream_error = 1;
      return 0U;
    }
    return total;
  }
  (void)ptr;
  return total;
}

static size_t lc_engine_stream_read_callback(char *ptr, size_t size,
                                             size_t nmemb, void *userdata) {
  lc_engine_reader_state *state;
  size_t capacity;

  state = (lc_engine_reader_state *)userdata;
  capacity = size * nmemb;
  return state->reader(state->reader_context, ptr, capacity, state->error);
}

static void
lc_engine_stream_state_cleanup(lc_engine_stream_request_state *state) {
  if (state == NULL) {
    return;
  }
  lc_lonejson_curl_parse_cleanup(&state->parse);
  if (state->parser_is_error) {
    lc_engine_lonejson_cleanup(state->client, &lc_engine_http_error_map,
                               &state->error_body);
  }
  state->parser_initialized = 0;
  state->parser_is_error = 0;
  lc_engine_free_string(&state->correlation_id);
  lc_engine_free_string(&state->content_type);
  lc_engine_free_string(&state->etag);
  lc_engine_free_string(&state->attachment_id);
  lc_engine_free_string(&state->attachment_name);
  lc_engine_free_string(&state->attachment_sha256);
  lc_engine_free_string(&state->attachment_content_type);
}

static int
lc_engine_stream_json_init_parser(lc_engine_stream_request_state *state) {
  const lonejson_map *map;
  lonejson *runtime;
  void *dst;
  lonejson_status status;

  if (state == NULL) {
    return 0;
  }
  map =
      state->parser_is_error ? &lc_engine_http_error_map : state->response_map;
  dst =
      state->parser_is_error ? (void *)&state->error_body : state->response_dst;
  if (map == NULL || dst == NULL) {
    return 0;
  }
  runtime = lc_engine_lonejson_runtime(state->client);
  status = runtime->init(runtime, map, dst, &state->parse.error);
  if (status != LONEJSON_STATUS_OK) {
    lc_engine_lonejson_error_from_status(
        state->error, status, &state->parse.error,
        "failed to initialize JSON response destination");
    return 0;
  }
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

static size_t lc_engine_stream_json_write_callback(char *contents, size_t size,
                                                   size_t nmemb,
                                                   void *userdata) {
  lc_engine_stream_request_state *state;
  size_t total;
  size_t available;
  size_t written;

  state = (lc_engine_stream_request_state *)userdata;
  total = size * nmemb;
  if (state == NULL || total == 0U) {
    return total;
  }
  if (state->response_map == NULL && state->http_status < 400L) {
    return lc_engine_stream_write_callback(contents, size, nmemb, userdata);
  }
  available = state->byte_limit > state->bytes_received
                  ? state->byte_limit - state->bytes_received
                  : 0U;
  if (total > available) {
    state->limit_exceeded = 1;
    lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_PROTOCOL,
                               "typed JSON response exceeds configured byte "
                               "limit");
    return 0U;
  }
  if (!state->parser_initialized) {
    if (state->http_status >= 200L && state->http_status < 300L) {
      if (state->http_status == 204L || state->http_status == 205L ||
          state->http_status == 304L) {
        return total;
      }
      state->parser_is_error = 0;
    } else if (state->http_status >= 400L) {
      state->parser_is_error = 1;
    } else {
      lc_engine_set_protocol_error(state->error,
                                   "typed JSON response has no parser");
      state->stream_error = 1;
      return 0U;
    }
    if (!lc_engine_stream_json_init_parser(state)) {
      state->stream_error = 1;
      return 0U;
    }
  }
  written = state->parse.write_callback(&state->parse, contents, 1U, total);
  if (written != total) {
    lc_engine_lonejson_error_from_status(state->error, state->parse.error.code,
                                         &state->parse.error,
                                         "failed to parse typed JSON response");
    state->stream_error = 1;
    return 0U;
  }
  state->bytes_received += total;
  return total;
}

static int lc_engine_append_header(struct curl_slist **headers,
                                   const char *name, const char *value) {
  lc_engine_buffer line;

  lc_engine_buffer_init(&line);
  if (lc_engine_buffer_append_cstr(&line, name) != LC_ENGINE_OK ||
      lc_engine_buffer_append_cstr(&line, ": ") != LC_ENGINE_OK ||
      lc_engine_buffer_append_cstr(&line, value) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&line);
    return 0;
  }
  *headers = curl_slist_append(*headers, line.data);
  lc_engine_buffer_cleanup(&line);
  return *headers != NULL;
}

static int lc_engine_append_long_header(struct curl_slist **headers,
                                        const char *name,
                                        lonejson_int64 value) {
  char scratch[64];

  if (lc_i64_format_base10((lc_i64)value, scratch, sizeof(scratch)) < 0) {
    return 0;
  }
  return lc_engine_append_header(headers, name, scratch);
}

static struct curl_slist *
lc_engine_build_stream_headers(const char *content_type, const char *accept,
                               const char *lease_id, const char *txn_id,
                               lonejson_int64 fencing_token) {
  struct curl_slist *headers;

  headers = NULL;
  if (accept != NULL && accept[0] != '\0') {
    if (!lc_engine_append_header(&headers, "Accept", accept)) {
      curl_slist_free_all(headers);
      return NULL;
    }
  }
  if (content_type != NULL && content_type[0] != '\0') {
    if (!lc_engine_append_header(&headers, "Content-Type", content_type)) {
      curl_slist_free_all(headers);
      return NULL;
    }
  }
  if (lease_id != NULL && lease_id[0] != '\0') {
    if (!lc_engine_append_header(&headers, "X-Lease-ID", lease_id)) {
      curl_slist_free_all(headers);
      return NULL;
    }
  }
  if (txn_id != NULL && txn_id[0] != '\0') {
    if (!lc_engine_append_header(&headers, "X-Txn-ID", txn_id)) {
      curl_slist_free_all(headers);
      return NULL;
    }
  }
  if (fencing_token > 0L) {
    if (!lc_engine_append_long_header(&headers, "X-Fencing-Token",
                                      fencing_token)) {
      curl_slist_free_all(headers);
      return NULL;
    }
  }
  return headers;
}

static int lc_engine_perform_streaming(
    lc_engine_client *client, const char *method, const char *path,
    struct curl_slist *headers, lc_engine_read_callback reader,
    void *reader_context, lc_engine_reset_callback reader_reset,
    void *reader_reset_context, const lonejson_map *response_map,
    void *response_dst, lc_engine_stream_request_state *state) {
  CURL *curl;
  CURLcode curl_rc;
  lc_engine_reader_state read_state;
  lc_engine_http_result synthetic;
  char *url;
  size_t url_length;
  size_t endpoint_index;
  int rc;

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    long request_timeout_ms;

    if (!lc_engine_client_attempt_timeout_ms(client, &request_timeout_ms)) {
      curl_slist_free_all(headers);
      lc_engine_stream_state_cleanup(state);
      return lc_engine_set_transport_error(state->error,
                                           "request deadline elapsed");
    }
    memset(&read_state, 0, sizeof(read_state));
    read_state.reader = reader;
    read_state.reader_context = reader_context;
    read_state.error = state->error;

    state->client = client;
    state->http_status = 0L;
    state->stream_error = 0;
    state->key_version = 0;
    memset(&state->retention, 0, sizeof(state->retention));
    state->fencing_token = 0L;
    state->attachment_size = 0L;
    state->attachment_created_at_unix = 0L;
    state->attachment_updated_at_unix = 0L;
    state->response_map = response_map;
    state->response_dst = response_dst;
    state->byte_limit = client->http_json_response_limit_bytes > 0U
                            ? client->http_json_response_limit_bytes
                            : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    state->bytes_received = 0U;
    state->parser_initialized = 0;
    state->parser_is_error = 0;
    state->limit_exceeded = 0;
    state->stream_error = 0;
    memset(&state->error_body, 0, sizeof(state->error_body));
    memset(&state->parse, 0, sizeof(state->parse));
    lc_engine_free_string(&state->correlation_id);
    lc_engine_free_string(&state->content_type);
    lc_engine_free_string(&state->etag);
    lc_engine_free_string(&state->attachment_id);
    lc_engine_free_string(&state->attachment_name);
    lc_engine_free_string(&state->attachment_sha256);

    url_length = strlen(client->endpoints[endpoint_index]) + strlen(path) + 1U;
    url = (char *)malloc(url_length);
    if (url == NULL) {
      curl_slist_free_all(headers);
      lc_engine_stream_state_cleanup(state);
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate request URL");
    }
    snprintf(url, url_length, "%s%s", client->endpoints[endpoint_index], path);

    curl = curl_easy_init();
    if (curl == NULL) {
      free(url);
      curl_slist_free_all(headers);
      lc_engine_stream_state_cleanup(state);
      return lc_engine_set_transport_error(state->error,
                                           "failed to initialize curl");
    }
    if (endpoint_index > 0U && reader_reset != NULL) {
      rc = reader_reset(reader_reset_context, state->error);
      if (rc != LC_ENGINE_OK) {
        free(url);
        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
        lc_engine_stream_state_cleanup(state);
        return rc;
      }
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                     lc_engine_stream_json_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, state);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                     lc_engine_stream_header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, state);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, request_timeout_ms);
    if (client->unix_socket_path != NULL &&
        client->unix_socket_path[0] != '\0') {
      curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH,
                       client->unix_socket_path);
    }
    if (client->prefer_http_2) {
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
                       (long)CURL_HTTP_VERSION_2TLS);
    } else {
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
    }
    if (!client->disable_mtls) {
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                       client->insecure_skip_verify ? 0L : 1L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                       client->insecure_skip_verify ? 0L : 2L);
      curl_easy_setopt(curl, CURLOPT_SSL_CTX_FUNCTION,
                       lc_engine_attachment_ssl_ctx);
      curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, client);
    }
    if (reader != NULL) {
      curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
      curl_easy_setopt(curl, CURLOPT_READFUNCTION,
                       lc_engine_stream_read_callback);
      curl_easy_setopt(curl, CURLOPT_READDATA, &read_state);
    }

    curl_rc = curl_easy_perform(curl);
    free(url);
    if (curl_rc == CURLE_WRITE_ERROR && state->stream_error) {
      curl_slist_free_all(headers);
      curl_easy_cleanup(curl);
      lc_engine_stream_state_cleanup(state);
      return state->error->code;
    }
    if (curl_rc != CURLE_OK) {
      curl_slist_free_all(headers);
      curl_easy_cleanup(curl);
      lc_engine_stream_state_cleanup(state);
      return lc_engine_set_transport_error(state->error,
                                           curl_easy_strerror(curl_rc));
    }
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &state->http_status);
    curl_easy_cleanup(curl);

    if (state->http_status >= 200L && state->http_status < 300L) {
      if (state->parser_initialized && !state->parser_is_error) {
        lonejson_status parse_status;

        parse_status = state->parse.finish(&state->parse);
        if (parse_status != LONEJSON_STATUS_OK) {
          lc_engine_lonejson_error_from_status(
              state->error, parse_status, &state->parse.error,
              "failed to finish typed JSON response");
          rc = state->error->code;
          curl_slist_free_all(headers);
          lc_engine_stream_state_cleanup(state);
          return rc;
        }
      }
      curl_slist_free_all(headers);
      return LC_ENGINE_OK;
    }

    if (state->parser_initialized && state->parser_is_error) {
      lonejson_status parse_status;

      parse_status = state->parse.finish(&state->parse);
      if (parse_status != LONEJSON_STATUS_OK) {
        lc_engine_lonejson_error_from_status(
            state->error, parse_status, &state->parse.error,
            "failed to finish typed JSON error response");
        rc = state->error->code;
        curl_slist_free_all(headers);
        lc_engine_stream_state_cleanup(state);
        return rc;
      }
      memset(&synthetic, 0, sizeof(synthetic));
      synthetic.http_status = state->http_status;
      synthetic.server_error_code = state->error_body.server_error_code;
      synthetic.detail = state->error_body.detail;
      synthetic.leader_endpoint = state->error_body.leader_endpoint;
      synthetic.current_etag = state->error_body.current_etag;
      synthetic.current_version = (long)state->error_body.current_version;
      synthetic.retry_after_seconds =
          (long)state->error_body.retry_after_seconds;
      synthetic.correlation_id = state->correlation_id;
      rc = lc_engine_set_server_error_from_result(state->error, &synthetic);
      if (state->error->server_error_code == NULL ||
          strcmp(state->error->server_error_code, "node_passive") != 0) {
        curl_slist_free_all(headers);
        lc_engine_stream_state_cleanup(state);
        return rc;
      }
      lc_engine_error_reset(state->error);
      lc_engine_stream_state_cleanup(state);
      continue;
    }

    memset(&synthetic, 0, sizeof(synthetic));
    synthetic.http_status = state->http_status;
    synthetic.correlation_id = state->correlation_id;
    rc = lc_engine_set_server_error_from_result(state->error, &synthetic);
    if (state->error->server_error_code == NULL ||
        strcmp(state->error->server_error_code, "node_passive") != 0) {
      curl_slist_free_all(headers);
      lc_engine_stream_state_cleanup(state);
      return rc;
    }
    lc_engine_error_reset(state->error);
    lc_engine_stream_state_cleanup(state);
  }

  curl_slist_free_all(headers);
  lc_engine_stream_state_cleanup(state);
  return lc_engine_set_transport_error(state->error,
                                       "all endpoints rejected the request");
}

static int lc_engine_attachment_build_query(
    lc_engine_buffer *path, const char *base_path, const char *ns,
    const char *key, const lc_engine_attachment_selector *selector,
    int public_read, const char *name_override, const char *content_type,
    int prevent_overwrite, long max_bytes, int has_max_bytes,
    lc_engine_error *error) {
  char *encoded;
  char scratch[32];

  lc_engine_buffer_init(path);
  if (lc_engine_buffer_append_cstr(path, base_path) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate request path");
  }
  encoded = lc_engine_url_encode(key);
  if (encoded == NULL) {
    lc_engine_buffer_cleanup(path);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to encode key");
  }
  lc_engine_buffer_append_cstr(path, "?key=");
  lc_engine_buffer_append_cstr(path, encoded);
  free(encoded);
  if (ns != NULL && ns[0] != '\0') {
    encoded = lc_engine_url_encode(ns);
    if (encoded == NULL) {
      lc_engine_buffer_cleanup(path);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to encode namespace");
    }
    lc_engine_buffer_append_cstr(path, "&namespace=");
    lc_engine_buffer_append_cstr(path, encoded);
    free(encoded);
  }
  if (name_override != NULL && name_override[0] != '\0') {
    encoded = lc_engine_url_encode(name_override);
    if (encoded == NULL) {
      lc_engine_buffer_cleanup(path);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to encode name");
    }
    lc_engine_buffer_append_cstr(path, "&name=");
    lc_engine_buffer_append_cstr(path, encoded);
    free(encoded);
  }
  if (selector != NULL) {
    if (selector->name != NULL && selector->name[0] != '\0') {
      encoded = lc_engine_url_encode(selector->name);
      if (encoded == NULL) {
        lc_engine_buffer_cleanup(path);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to encode attachment selector name");
      }
      lc_engine_buffer_append_cstr(path, "&name=");
      lc_engine_buffer_append_cstr(path, encoded);
      free(encoded);
    }
    if (selector->id != NULL && selector->id[0] != '\0') {
      encoded = lc_engine_url_encode(selector->id);
      if (encoded == NULL) {
        lc_engine_buffer_cleanup(path);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to encode attachment selector id");
      }
      lc_engine_buffer_append_cstr(path, "&id=");
      lc_engine_buffer_append_cstr(path, encoded);
      free(encoded);
    }
  }
  if (content_type != NULL && content_type[0] != '\0') {
    encoded = lc_engine_url_encode(content_type);
    if (encoded == NULL) {
      lc_engine_buffer_cleanup(path);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to encode content_type");
    }
    lc_engine_buffer_append_cstr(path, "&content_type=");
    lc_engine_buffer_append_cstr(path, encoded);
    free(encoded);
  }
  if (prevent_overwrite) {
    lc_engine_buffer_append_cstr(path, "&prevent_overwrite=1");
  }
  if (has_max_bytes) {
    if (lc_i64_format_base10((lc_i64)max_bytes, scratch, sizeof(scratch)) < 0) {
      lc_engine_buffer_cleanup(path);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to format max_bytes");
    }
    lc_engine_buffer_append_cstr(path, "&max_bytes=");
    lc_engine_buffer_append_cstr(path, scratch);
  }
  if (public_read) {
    lc_engine_buffer_append_cstr(path, "&public=1");
  }
  return LC_ENGINE_OK;
}

static int
lc_engine_capture_get_metadata(lc_engine_get_stream_response *response,
                               const lc_engine_stream_request_state *state) {
  response->retention_support = state->retention.support;
  response->has_retention_expires_at_unix =
      !response->no_content && state->retention.has_expiry;
  response->retention_expires_at_unix = state->retention.expiry;
  if (state->content_type != NULL) {
    response->content_type = lc_engine_strdup_local(state->content_type);
    if (response->content_type == NULL) {
      return 0;
    }
  }
  if (state->etag != NULL) {
    response->etag = lc_engine_strdup_local(state->etag);
    if (response->etag == NULL) {
      return 0;
    }
  }
  if (state->correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(state->correlation_id);
    if (response->correlation_id == NULL) {
      return 0;
    }
  }
  response->version = state->key_version;
  response->fencing_token = state->fencing_token;
  return 1;
}

static int lc_engine_attachment_info_from_headers(
    lc_engine_attachment_info *info,
    const lc_engine_stream_request_state *state) {
  if (state->attachment_id != NULL) {
    info->id = lc_engine_strdup_local(state->attachment_id);
    if (info->id == NULL) {
      return 0;
    }
  }
  if (state->attachment_name != NULL) {
    info->name = lc_engine_strdup_local(state->attachment_name);
    if (info->name == NULL) {
      return 0;
    }
  }
  if (state->attachment_sha256 != NULL) {
    info->plaintext_sha256 = lc_engine_strdup_local(state->attachment_sha256);
    if (info->plaintext_sha256 == NULL) {
      return 0;
    }
  }
  if (state->content_type != NULL) {
    info->content_type = lc_engine_strdup_local(state->content_type);
    if (info->content_type == NULL) {
      return 0;
    }
  }
  info->size = state->attachment_size;
  info->created_at_unix = state->attachment_created_at_unix;
  info->updated_at_unix = state->attachment_updated_at_unix;
  return 1;
}

static int lc_engine_i64_assign(lonejson_int64 value, lonejson_int64 *out_value,
                                lc_engine_error *error) {
  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing i64 destination");
  }
  *out_value = value;
  return LC_ENGINE_OK;
}

static int lc_engine_i64_to_long_checked(lonejson_int64 value,
                                         const char *label, long *out_value,
                                         lc_engine_error *error) {
  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing long destination");
  }
  if (value < (lonejson_int64)LONG_MIN || value > (lonejson_int64)LONG_MAX) {
    return lc_engine_set_protocol_error(error, label);
  }
  *out_value = (long)value;
  return LC_ENGINE_OK;
}

static int lc_engine_i64_to_int_checked(lonejson_int64 value, const char *label,
                                        int *out_value,
                                        lc_engine_error *error) {
  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing int destination");
  }
  if (value < (lonejson_int64)INT_MIN || value > (lonejson_int64)INT_MAX) {
    return lc_engine_set_protocol_error(error, label);
  }
  *out_value = (int)value;
  return LC_ENGINE_OK;
}

static int lc_engine_attachment_info_from_json(
    lc_engine_attachment_info *info,
    const lc_engine_attachment_info_json *parsed, lc_engine_error *error) {
  int rc;

  rc = lc_engine_i64_to_long_checked(
      parsed->size, "attachment size is out of range", &info->size, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  info->id = lc_engine_strdup_local(parsed->id);
  info->name = lc_engine_strdup_local(parsed->name);
  info->plaintext_sha256 = lc_engine_strdup_local(parsed->plaintext_sha256);
  info->content_type = lc_engine_strdup_local(parsed->content_type);
  if ((parsed->id != NULL && info->id == NULL) ||
      (parsed->name != NULL && info->name == NULL) ||
      (parsed->plaintext_sha256 != NULL && info->plaintext_sha256 == NULL) ||
      (parsed->content_type != NULL && info->content_type == NULL)) {
    lc_engine_attachment_info_cleanup(info);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy attachment strings");
  }
  rc = lc_engine_i64_assign(parsed->created_at_unix, &info->created_at_unix,
                            error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  return lc_engine_i64_assign(parsed->updated_at_unix, &info->updated_at_unix,
                              error);
}

int lc_engine_parse_attach_response_json(const char *json,
                                         const char *correlation_id,
                                         lc_engine_attach_response *response,
                                         lc_engine_error *error) {
  lonejson *runtime;
  lc_engine_attach_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  if (json == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "parse_attach_response_json requires json, response, and error");
  }

  lc_engine_attach_response_cleanup(response);
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate thread-local JSON runtime");
  }
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = runtime->init(runtime, &lc_engine_attach_response_map, &parsed,
                         &lj_error);
  if (status != LONEJSON_STATUS_OK)
    return lc_engine_lonejson_error_from_status(
        error, status, &lj_error, "failed to initialize attach response");
  status = runtime->parse_cstr(runtime, &lc_engine_attach_response_map, &parsed,
                               json, &lj_error);
  rc = lc_engine_lonejson_error_from_status(error, status, &lj_error,
                                            "failed to parse attach response");
  if (rc != LC_ENGINE_OK) {
    runtime->cleanup(runtime, &lc_engine_attach_response_map, &parsed, NULL);
    return rc;
  }
  rc = lc_engine_attachment_info_from_json(&response->attachment,
                                           &parsed.attachment, error);
  if (rc != LC_ENGINE_OK) {
    runtime->cleanup(runtime, &lc_engine_attach_response_map, &parsed, NULL);
    lc_engine_attach_response_cleanup(response);
    return rc;
  }
  response->noop = parsed.noop ? 1 : 0;
  rc = lc_engine_i64_assign(parsed.version, &response->version, error);
  if (rc != LC_ENGINE_OK) {
    runtime->cleanup(runtime, &lc_engine_attach_response_map, &parsed, NULL);
    lc_engine_attach_response_cleanup(response);
    return rc;
  }
  if (correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(correlation_id);
    if (response->correlation_id == NULL) {
      runtime->cleanup(runtime, &lc_engine_attach_response_map, &parsed, NULL);
      lc_engine_attach_response_cleanup(response);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate attachment correlation id");
    }
  }
  runtime->cleanup(runtime, &lc_engine_attach_response_map, &parsed, NULL);
  return LC_ENGINE_OK;
}

int lc_engine_parse_list_attachments_response_json(
    const char *json, const char *correlation_id,
    lc_engine_list_attachments_response *response, lc_engine_error *error) {
  lonejson *runtime;
  lc_engine_list_attachments_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  if (json == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "parse_list_attachments_response_json "
                                      "requires json, response, and error");
  }

  lc_engine_list_attachments_response_cleanup(response);
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate thread-local JSON runtime");
  }
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = runtime->init(runtime, &lc_engine_list_attachments_response_map,
                         &parsed, &lj_error);
  if (status != LONEJSON_STATUS_OK)
    return lc_engine_lonejson_error_from_status(
        error, status, &lj_error, "failed to initialize attachment list");
  status =
      runtime->parse_cstr(runtime, &lc_engine_list_attachments_response_map,
                          &parsed, json, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse list_attachments response");
  if (rc != LC_ENGINE_OK) {
    runtime->cleanup(runtime, &lc_engine_list_attachments_response_map, &parsed,
                     NULL);
    return rc;
  }
  response->ns = lc_engine_strdup_local(parsed.ns);
  response->key = lc_engine_strdup_local(parsed.key);
  if ((parsed.ns != NULL && response->ns == NULL) ||
      (parsed.key != NULL && response->key == NULL)) {
    runtime->cleanup(runtime, &lc_engine_list_attachments_response_map, &parsed,
                     NULL);
    lc_engine_list_attachments_response_cleanup(response);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to copy list_attachments response");
  }
  if (parsed.attachments.count > 0U) {
    size_t index;
    lc_engine_attachment_info_json *items;

    response->attachments = (lc_engine_attachment_info *)calloc(
        parsed.attachments.count, sizeof(lc_engine_attachment_info));
    if (response->attachments == NULL) {
      runtime->cleanup(runtime, &lc_engine_list_attachments_response_map,
                       &parsed, NULL);
      lc_engine_list_attachments_response_cleanup(response);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate attachments array");
    }
    response->attachment_count = parsed.attachments.count;
    items = (lc_engine_attachment_info_json *)parsed.attachments.items;
    for (index = 0U; index < parsed.attachments.count; ++index) {
      rc = lc_engine_attachment_info_from_json(&response->attachments[index],
                                               &items[index], error);
      if (rc != LC_ENGINE_OK) {
        runtime->cleanup(runtime, &lc_engine_list_attachments_response_map,
                         &parsed, NULL);
        lc_engine_list_attachments_response_cleanup(response);
        return rc;
      }
    }
  }
  if (correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(correlation_id);
    if (response->correlation_id == NULL) {
      runtime->cleanup(runtime, &lc_engine_list_attachments_response_map,
                       &parsed, NULL);
      lc_engine_list_attachments_response_cleanup(response);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate correlation id");
    }
  }
  runtime->cleanup(runtime, &lc_engine_list_attachments_response_map, &parsed,
                   NULL);
  return LC_ENGINE_OK;
}

void lc_engine_get_stream_response_cleanup(
    lc_engine_get_stream_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->content_type);
  lc_engine_free_string(&response->etag);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_attachment_info_cleanup(lc_engine_attachment_info *info) {
  if (info == NULL) {
    return;
  }
  lc_engine_free_string(&info->id);
  lc_engine_free_string(&info->name);
  lc_engine_free_string(&info->plaintext_sha256);
  lc_engine_free_string(&info->content_type);
}

void lc_engine_attach_response_cleanup(lc_engine_attach_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_attachment_info_cleanup(&response->attachment);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_list_attachments_response_cleanup(
    lc_engine_list_attachments_response *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->key);
  for (index = 0U; index < response->attachment_count; ++index) {
    lc_engine_attachment_info_cleanup(&response->attachments[index]);
  }
  free(response->attachments);
  response->attachments = NULL;
  response->attachment_count = 0U;
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_get_attachment_response_cleanup(
    lc_engine_get_attachment_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_attachment_info_cleanup(&response->attachment);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_delete_attachment_response_cleanup(
    lc_engine_delete_attachment_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_delete_all_attachments_response_cleanup(
    lc_engine_delete_all_attachments_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
}

static size_t lc_engine_empty_reader(void *context, void *buffer, size_t count,
                                     lc_engine_error *error) {
  (void)context;
  (void)buffer;
  (void)count;
  (void)error;
  return 0U;
}

int lc_engine_client_get_into(lc_engine_client *client,
                              const lc_engine_get_request *request,
                              lc_engine_write_callback writer,
                              void *writer_context,
                              lc_engine_get_stream_response *response,
                              lc_engine_error *error) {
  lc_engine_stream_request_state state;
  lc_engine_buffer path;
  struct curl_slist *headers;
  const char *ns;
  int rc;

  if (client == NULL || request == NULL || writer == NULL || response == NULL ||
      error == NULL || request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_into requires client, request, writer, response, error, and key");
  }
  if (!request->public_read &&
      (request->lease_id == NULL || request->lease_id[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_into requires lease_id unless public_read is enabled");
  }

  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(&path, "/v1/get", ns, request->key,
                                        NULL, request->public_read, NULL, NULL,
                                        0, 0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers = lc_engine_build_stream_headers(NULL, "application/json",
                                           request->lease_id, NULL,
                                           request->fencing_token);
  memset(&state, 0, sizeof(state));
  state.writer = writer;
  state.writer_context = writer_context;
  state.error = error;
  rc = lc_engine_perform_streaming(client, "GET", path.data, headers, NULL,
                                   NULL, NULL, NULL, NULL, NULL, &state);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  if (state.http_status == 204L) {
    response->no_content = 1;
  }
  if (!lc_engine_capture_get_metadata(response, &state)) {
    lc_engine_stream_state_cleanup(&state);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to capture get response metadata");
  }
  lc_engine_stream_state_cleanup(&state);
  return LC_ENGINE_OK;
}

int lc_engine_client_update_from(lc_engine_client *client,
                                 const lc_engine_update_request *request,
                                 lc_engine_read_callback reader,
                                 void *reader_context,
                                 lc_engine_update_response *response,
                                 lc_engine_error *error) {
  lc_engine_stream_request_state state;
  lc_engine_buffer path;
  struct curl_slist *headers;
  const char *ns;
  char if_version_buffer[64];
  char retention_value[32];
  lc_engine_update_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->key == NULL || request->key[0] == '\0' ||
      request->lease_id == NULL || request->lease_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "update_from requires client, request, response, error, key, lease_id, "
        "and fencing_token");
  }

  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_retention_validate(client, request->has_retention_seconds,
                                    request->retention_seconds, ns,
                                    request->key, error);
  if (rc != LC_ENGINE_OK)
    return rc;
  rc = lc_engine_attachment_build_query(&path, "/v1/update", ns, request->key,
                                        NULL, 0, NULL, NULL, 0, 0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers = lc_engine_build_stream_headers(
      request->content_type != NULL ? request->content_type
                                    : "application/json",
      "application/json", request->lease_id, request->txn_id,
      request->fencing_token);
  if (headers == NULL) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build update_from headers");
  }
  if (request->if_state_etag != NULL && request->if_state_etag[0] != '\0') {
    if (!lc_engine_append_header(&headers, "X-If-State-ETag",
                                 request->if_state_etag)) {
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&path);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to append update_from if_state_etag header");
    }
  }
  if (request->has_if_version) {
    if (lc_i64_format_base10((lc_i64)request->if_version, if_version_buffer,
                             sizeof(if_version_buffer)) < 0 ||
        !lc_engine_append_header(&headers, "X-If-Version", if_version_buffer)) {
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&path);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to append update_from if_version header");
    }
  }

  if (request->has_retention_seconds) {
    snprintf(retention_value, sizeof(retention_value), "%ld",
             request->retention_seconds);
    if (!lc_engine_append_header(&headers, "X-State-Retention-Seconds",
                                 retention_value)) {
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&path);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to append state retention header");
    }
  }
  lc_engine_update_response_cleanup(response);
  memset(&parsed, 0, sizeof(parsed));
  memset(&state, 0, sizeof(state));
  rc = lc_engine_validate_update_retry_source(client, reader, reader_context,
                                              error);
  if (rc != LC_ENGINE_OK) {
    curl_slist_free_all(headers);
    lc_engine_buffer_cleanup(&path);
    return rc;
  }
  state.error = error;
  rc = lc_engine_perform_streaming(
      client, "POST", path.data, headers,
      reader != NULL ? reader : lc_engine_empty_reader, reader_context,
      reader != NULL ? lc_engine_reset_bridge : NULL, reader_context,
      &lc_engine_update_response_map, &parsed, &state);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  response->new_version = parsed.new_version;
  response->retention_support = state.retention.support;
  response->has_retention_expires_at_unix = state.retention.has_expiry;
  response->retention_expires_at_unix = state.retention.expiry;
  response->new_state_etag = lc_engine_strdup_local(parsed.new_state_etag);
  if (parsed.new_state_etag != NULL && response->new_state_etag == NULL) {
    lc_engine_lonejson_cleanup(client, &lc_engine_update_response_map, &parsed);
    lc_engine_stream_state_cleanup(&state);
    lc_engine_update_response_cleanup(response);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy update response");
  }
  response->bytes = parsed.bytes;
  if (state.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(state.correlation_id);
    if (response->correlation_id == NULL) {
      lc_engine_lonejson_cleanup(client, &lc_engine_update_response_map,
                                 &parsed);
      lc_engine_stream_state_cleanup(&state);
      lc_engine_update_response_cleanup(response);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate update_from correlation id");
    }
  }
  lc_engine_lonejson_cleanup(client, &lc_engine_update_response_map, &parsed);
  lc_engine_stream_state_cleanup(&state);
  return LC_ENGINE_OK;
}

int lc_engine_client_enqueue_from(lc_engine_client *client,
                                  const lc_engine_enqueue_request *request,
                                  lc_engine_read_callback reader,
                                  void *reader_context,
                                  lc_engine_enqueue_response *response,
                                  lc_engine_error *error) {
  lc_engine_stream_request_state state;
  lc_engine_multipart_upload_state upload_state;
  lc_engine_buffer meta;
  lc_engine_buffer prefix;
  lc_engine_buffer suffix;
  lc_engine_buffer content_type;
  lc_engine_enqueue_meta_json meta_src;
  lonejson_owned_buffer meta_owned;
  lonejson_error lj_error;
  struct curl_slist *headers;
  lc_engine_enqueue_response_json parsed;
  const char *boundary;
  const char *payload_content_type;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->queue == NULL || request->queue[0] == '\0') {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "enqueue_from requires client, request, response, error, and queue");
  }

  boundary = "lockdc-stream-boundary-7e4dbe2f";
  payload_content_type = request->payload_content_type != NULL &&
                                 request->payload_content_type[0] != '\0'
                             ? request->payload_content_type
                             : "application/octet-stream";

  lc_engine_enqueue_response_cleanup(response);
  memset(&meta_src, 0, sizeof(meta_src));
  meta_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  meta_src.queue = (char *)request->queue;
  meta_src.delay_seconds = request->delay_seconds;
  meta_src.visibility_timeout_seconds = request->visibility_timeout_seconds;
  meta_src.ttl_seconds = request->ttl_seconds;
  meta_src.max_attempts = request->max_attempts;
  meta_src.payload_content_type = (char *)payload_content_type;
  meta_owned = lonejson_default_owned_buffer();
  lonejson_error_init(&lj_error);
  rc = lc_engine_lonejson_runtime(client)->serialize_owned(
      lc_engine_lonejson_runtime(client), &lc_engine_enqueue_meta_map,
      &meta_src, &meta_owned, &lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    lonejson_owned_buffer_free(&meta_owned);
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to build enqueue_from metadata");
  }
  lc_engine_buffer_init(&meta);
  if (lc_engine_buffer_append(&meta, meta_owned.data, meta_owned.len) !=
      LC_ENGINE_OK) {
    lonejson_owned_buffer_free(&meta_owned);
    lc_engine_buffer_cleanup(&meta);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy enqueue_from metadata");
  }
  lonejson_owned_buffer_free(&meta_owned);

  lc_engine_buffer_init(&prefix);
  rc = lc_engine_buffer_append_cstr(&prefix, "--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(
        &prefix, "\r\nContent-Type: application/json\r\nContent-Disposition: "
                 "form-data; name=\"meta\"\r\n\r\n");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append(&prefix, meta.data, meta.length);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix, "\r\n--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix,
                                      "\r\nContent-Disposition: form-data; "
                                      "name=\"payload\"\r\nContent-Type: ");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix, payload_content_type);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&prefix, "\r\n\r\n");
  lc_engine_buffer_cleanup(&meta);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&prefix);
    return lc_engine_set_client_error(
        error, rc, "failed to build enqueue_from multipart prefix");
  }

  lc_engine_buffer_init(&suffix);
  rc = lc_engine_buffer_append_cstr(&suffix, "\r\n--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&suffix, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&suffix, "--\r\n");
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&prefix);
    lc_engine_buffer_cleanup(&suffix);
    return lc_engine_set_client_error(
        error, rc, "failed to build enqueue_from multipart suffix");
  }

  lc_engine_buffer_init(&content_type);
  rc = lc_engine_buffer_append_cstr(&content_type,
                                    "multipart/related; boundary=");
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_buffer_append_cstr(&content_type, boundary);
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&prefix);
    lc_engine_buffer_cleanup(&suffix);
    lc_engine_buffer_cleanup(&content_type);
    return lc_engine_set_client_error(
        error, rc, "failed to build enqueue_from content type");
  }

  headers = lc_engine_build_stream_headers(content_type.data,
                                           "application/json", NULL, NULL, 0L);
  if (headers == NULL) {
    lc_engine_buffer_cleanup(&prefix);
    lc_engine_buffer_cleanup(&suffix);
    lc_engine_buffer_cleanup(&content_type);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build enqueue_from headers");
  }

  memset(&upload_state, 0, sizeof(upload_state));
  upload_state.prefix = prefix.data;
  upload_state.prefix_length = prefix.length;
  upload_state.suffix = suffix.data;
  upload_state.suffix_length = suffix.length;
  upload_state.payload_reader =
      reader != NULL ? reader : lc_engine_empty_reader;
  upload_state.payload_context = reader_context;
  upload_state.payload_reset = reader != NULL ? lc_engine_reset_bridge : NULL;
  upload_state.payload_reset_context = reader_context;

  memset(&state, 0, sizeof(state));
  state.error = error;
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_perform_streaming(
      client, "POST", "/v1/queue/enqueue", headers,
      lc_engine_multipart_upload_read, &upload_state,
      lc_engine_multipart_upload_reset, &upload_state,
      &lc_engine_enqueue_response_map, &parsed, &state);
  lc_engine_buffer_cleanup(&prefix);
  lc_engine_buffer_cleanup(&suffix);
  lc_engine_buffer_cleanup(&content_type);
  if (rc != LC_ENGINE_OK) {
    lc_engine_stream_state_cleanup(&state);
    lc_engine_lonejson_cleanup(client, &lc_engine_enqueue_response_map,
                               &parsed);
    return rc;
  }
  response->ns = lc_engine_strdup_local(parsed.ns);
  response->queue = lc_engine_strdup_local(parsed.queue);
  response->message_id = lc_engine_strdup_local(parsed.message_id);
  if ((parsed.ns != NULL && response->ns == NULL) ||
      (parsed.queue != NULL && response->queue == NULL) ||
      (parsed.message_id != NULL && response->message_id == NULL)) {
    lc_engine_lonejson_cleanup(client, &lc_engine_enqueue_response_map,
                               &parsed);
    lc_engine_stream_state_cleanup(&state);
    lc_engine_enqueue_response_cleanup(response);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy enqueue response");
  }
  rc = lc_engine_i64_to_int_checked(parsed.attempts,
                                    "enqueue attempts is out of range",
                                    &response->attempts, error);
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_i64_to_int_checked(parsed.max_attempts,
                                      "enqueue max_attempts is out of range",
                                      &response->max_attempts, error);
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_i64_to_int_checked(
        parsed.failure_attempts, "enqueue failure_attempts is out of range",
        &response->failure_attempts, error);
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_i64_assign(parsed.not_visible_until_unix,
                              &response->not_visible_until_unix, error);
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_i64_to_long_checked(
        parsed.visibility_timeout_seconds,
        "enqueue visibility_timeout_seconds is out of range",
        &response->visibility_timeout_seconds, error);
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_i64_to_long_checked(parsed.payload_bytes,
                                       "enqueue payload_bytes is out of range",
                                       &response->payload_bytes, error);
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_lonejson_cleanup(client, &lc_engine_enqueue_response_map,
                               &parsed);
    lc_engine_enqueue_response_cleanup(response);
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  if (state.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(state.correlation_id);
    if (response->correlation_id == NULL) {
      lc_engine_lonejson_cleanup(client, &lc_engine_enqueue_response_map,
                                 &parsed);
      lc_engine_stream_state_cleanup(&state);
      lc_engine_enqueue_response_cleanup(response);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate enqueue_from correlation id");
    }
  }
  lc_engine_lonejson_cleanup(client, &lc_engine_enqueue_response_map, &parsed);
  lc_engine_stream_state_cleanup(&state);
  return LC_ENGINE_OK;
}

int lc_engine_client_attach_from(lc_engine_client *client,
                                 const lc_engine_attach_request *request,
                                 lc_engine_read_callback reader,
                                 void *reader_context,
                                 lc_engine_attach_response *response,
                                 lc_engine_error *error) {
  lc_engine_stream_request_state state;
  lc_engine_buffer path;
  struct curl_slist *headers;
  const char *ns;
  lc_engine_attach_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || reader == NULL || response == NULL ||
      error == NULL || request->key == NULL || request->name == NULL ||
      request->lease_id == NULL || request->txn_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "attach_from requires client, request, reader, response, error, key, "
        "name, lease_id, and txn_id");
  }
  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(
      &path, "/v1/attachments", ns, request->key, NULL, 0, request->name,
      request->content_type, request->prevent_overwrite, request->max_bytes,
      request->has_max_bytes, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers = lc_engine_build_stream_headers(
      request->content_type != NULL ? request->content_type
                                    : "application/octet-stream",
      "application/json", request->lease_id, request->txn_id,
      request->fencing_token);
  if (headers == NULL) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to build attachment upload headers");
  }
  memset(&state, 0, sizeof(state));
  state.error = error;
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_perform_streaming(
      client, "POST", path.data, headers, reader, reader_context,
      reader != NULL ? lc_engine_reset_bridge : NULL, reader_context,
      &lc_engine_attach_response_map, &parsed, &state);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_stream_state_cleanup(&state);
    lc_engine_lonejson_cleanup(client, &lc_engine_attach_response_map, &parsed);
    return rc;
  }
  rc = lc_engine_attachment_info_from_json(&response->attachment,
                                           &parsed.attachment, error);
  if (rc == LC_ENGINE_OK) {
    response->noop = parsed.noop ? 1 : 0;
    rc = lc_engine_i64_assign(parsed.version, &response->version, error);
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_lonejson_cleanup(client, &lc_engine_attach_response_map, &parsed);
    lc_engine_attach_response_cleanup(response);
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  if (state.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(state.correlation_id);
    if (response->correlation_id == NULL) {
      rc = lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate attachment correlation id");
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_lonejson_cleanup(client, &lc_engine_attach_response_map, &parsed);
    lc_engine_attach_response_cleanup(response);
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  lc_engine_lonejson_cleanup(client, &lc_engine_attach_response_map, &parsed);
  lc_engine_stream_state_cleanup(&state);
  return rc;
}

int lc_engine_client_list_attachments(
    lc_engine_client *client, const lc_engine_list_attachments_request *request,
    lc_engine_list_attachments_response *response, lc_engine_error *error) {
  lc_engine_buffer path;
  const char *ns;
  lc_engine_header_pair headers[3];
  size_t header_count;
  lc_engine_http_result result;
  lc_engine_list_attachments_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "list_attachments requires client, request, response, error, and key");
  }
  if (!request->public_read &&
      (request->lease_id == NULL || request->lease_id[0] == '\0' ||
       request->txn_id == NULL || request->txn_id[0] == '\0')) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "list_attachments requires lease_id and "
                                      "txn_id unless public_read is enabled");
  }
  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(
      &path, "/v1/attachments", ns, request->key, NULL, request->public_read,
      NULL, NULL, 0, 0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  header_count = 0U;
  if (!request->public_read) {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
    headers[header_count].name = "X-Txn-ID";
    headers[header_count].value = request->txn_id;
    ++header_count;
    if (request->fencing_token > 0L) {
      static char token_buf[64];
      if (lc_i64_format_base10((lc_i64)request->fencing_token, token_buf,
                               sizeof(token_buf)) < 0) {
        lc_engine_buffer_cleanup(&path);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to format fencing token");
      }
      headers[header_count].name = "X-Fencing-Token";
      headers[header_count].value = token_buf;
      ++header_count;
    }
  }
  memset(&result, 0, sizeof(result));
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_http_json_request(
      client, "GET", path.data, NULL, 0U, headers, header_count,
      &lc_engine_list_attachments_response_map, &parsed, &result, error);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200L || result.http_status >= 300L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_lonejson_cleanup(client, &lc_engine_list_attachments_response_map,
                               &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  response->ns = lc_engine_strdup_local(parsed.ns);
  response->key = lc_engine_strdup_local(parsed.key);
  if ((parsed.ns != NULL && response->ns == NULL) ||
      (parsed.key != NULL && response->key == NULL)) {
    lc_engine_lonejson_cleanup(client, &lc_engine_list_attachments_response_map,
                               &parsed);
    lc_engine_list_attachments_response_cleanup(response);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to copy list_attachments response");
  }
  if (parsed.attachments.count > 0U) {
    size_t index;
    lc_engine_attachment_info_json *items;

    response->attachments = (lc_engine_attachment_info *)calloc(
        parsed.attachments.count, sizeof(lc_engine_attachment_info));
    if (response->attachments == NULL) {
      lc_engine_lonejson_cleanup(
          client, &lc_engine_list_attachments_response_map, &parsed);
      lc_engine_list_attachments_response_cleanup(response);
      lc_engine_http_result_cleanup(&result);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate attachments array");
    }
    response->attachment_count = parsed.attachments.count;
    items = (lc_engine_attachment_info_json *)parsed.attachments.items;
    for (index = 0U; index < parsed.attachments.count; ++index) {
      rc = lc_engine_attachment_info_from_json(&response->attachments[index],
                                               &items[index], error);
      if (rc != LC_ENGINE_OK) {
        lc_engine_lonejson_cleanup(
            client, &lc_engine_list_attachments_response_map, &parsed);
        lc_engine_list_attachments_response_cleanup(response);
        lc_engine_http_result_cleanup(&result);
        return rc;
      }
    }
  }
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      lc_engine_lonejson_cleanup(
          client, &lc_engine_list_attachments_response_map, &parsed);
      lc_engine_list_attachments_response_cleanup(response);
      lc_engine_http_result_cleanup(&result);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate correlation id");
    }
  }
  lc_engine_lonejson_cleanup(client, &lc_engine_list_attachments_response_map,
                             &parsed);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_get_attachment_into(
    lc_engine_client *client, const lc_engine_get_attachment_request *request,
    lc_engine_write_callback writer, void *writer_context,
    lc_engine_get_attachment_response *response, lc_engine_error *error) {
  lc_engine_stream_request_state state;
  lc_engine_buffer path;
  struct curl_slist *headers;
  const char *ns;
  int rc;

  if (client == NULL || request == NULL || writer == NULL || response == NULL ||
      error == NULL || request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_attachment_into requires client, request, writer, response, "
        "error, and key");
  }
  if (request->selector.id == NULL && request->selector.name == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_attachment_into requires selector id or name");
  }
  if (!request->public_read &&
      (request->lease_id == NULL || request->lease_id[0] == '\0' ||
       request->txn_id == NULL || request->txn_id[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_attachment_into requires lease_id and txn_id unless public_read "
        "is enabled");
  }
  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(
      &path, "/v1/attachment", ns, request->key, &request->selector,
      request->public_read, NULL, NULL, 0, 0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers = lc_engine_build_stream_headers(NULL, "application/octet-stream",
                                           request->lease_id, request->txn_id,
                                           request->fencing_token);
  memset(&state, 0, sizeof(state));
  state.writer = writer;
  state.writer_context = writer_context;
  state.error = error;
  rc = lc_engine_perform_streaming(client, "GET", path.data, headers, NULL,
                                   NULL, NULL, NULL, NULL, NULL, &state);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_stream_state_cleanup(&state);
    return rc;
  }
  if (!lc_engine_attachment_info_from_headers(&response->attachment, &state)) {
    lc_engine_stream_state_cleanup(&state);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate attachment headers");
  }
  if (state.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(state.correlation_id);
    if (response->correlation_id == NULL) {
      lc_engine_stream_state_cleanup(&state);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate correlation id");
    }
  }
  lc_engine_stream_state_cleanup(&state);
  return LC_ENGINE_OK;
}

int lc_engine_client_delete_attachment(
    lc_engine_client *client,
    const lc_engine_delete_attachment_request *request,
    lc_engine_delete_attachment_response *response, lc_engine_error *error) {
  lc_engine_buffer path;
  const char *ns;
  lc_engine_header_pair headers[3];
  char token_buf[64];
  lc_engine_http_result result;
  lc_engine_delete_attachment_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->key == NULL || request->lease_id == NULL ||
      request->txn_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "delete_attachment requires client, request, response, error, key, "
        "lease_id, and txn_id");
  }
  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(&path, "/v1/attachment", ns,
                                        request->key, &request->selector, 0,
                                        NULL, NULL, 0, 0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers[0].name = "X-Lease-ID";
  headers[0].value = request->lease_id;
  headers[1].name = "X-Txn-ID";
  headers[1].value = request->txn_id;
  headers[2].name = "X-Fencing-Token";
  if (lc_i64_format_base10((lc_i64)request->fencing_token, token_buf,
                           sizeof(token_buf)) < 0) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to format fencing token");
  }
  headers[2].value = token_buf;
  memset(&result, 0, sizeof(result));
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_http_json_request(
      client, "DELETE", path.data, NULL, 0U, headers,
      request->fencing_token > 0L ? 3U : 2U,
      &lc_engine_delete_attachment_response_map, &parsed, &result, error);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200L || result.http_status >= 300L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_lonejson_cleanup(
        client, &lc_engine_delete_attachment_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  response->deleted = parsed.deleted ? 1 : 0;
  response->version = parsed.version;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
  }
  lc_engine_lonejson_cleanup(client, &lc_engine_delete_attachment_response_map,
                             &parsed);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_delete_all_attachments(
    lc_engine_client *client,
    const lc_engine_delete_all_attachments_request *request,
    lc_engine_delete_all_attachments_response *response,
    lc_engine_error *error) {
  lc_engine_buffer path;
  const char *ns;
  lc_engine_header_pair headers[3];
  char token_buf[64];
  lc_engine_http_result result;
  lc_engine_delete_all_attachments_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->key == NULL || request->lease_id == NULL ||
      request->txn_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "delete_all_attachments requires client, request, response, error, "
        "key, lease_id, and txn_id");
  }
  ns = lc_engine_effective_namespace(client, request->ns);
  rc = lc_engine_attachment_build_query(&path, "/v1/attachments", ns,
                                        request->key, NULL, 0, NULL, NULL, 0,
                                        0L, 0, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  headers[0].name = "X-Lease-ID";
  headers[0].value = request->lease_id;
  headers[1].name = "X-Txn-ID";
  headers[1].value = request->txn_id;
  headers[2].name = "X-Fencing-Token";
  if (lc_i64_format_base10((lc_i64)request->fencing_token, token_buf,
                           sizeof(token_buf)) < 0) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to format fencing token");
  }
  headers[2].value = token_buf;
  memset(&result, 0, sizeof(result));
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_http_json_request(
      client, "DELETE", path.data, NULL, 0U, headers,
      request->fencing_token > 0L ? 3U : 2U,
      &lc_engine_delete_all_attachments_response_map, &parsed, &result, error);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200L || result.http_status >= 300L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_lonejson_cleanup(
        client, &lc_engine_delete_all_attachments_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  response->version = parsed.version;
  rc = lc_engine_i64_to_int_checked(
      parsed.deleted, "delete attachments deleted count is out of range",
      &response->deleted, error);
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
  }
  lc_engine_lonejson_cleanup(
      client, &lc_engine_delete_all_attachments_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}
