#include "lc_internal.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <curl/curl.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#define LC_ENGINE_STREAM_ERROR_BODY_LIMIT (8U * 1024U)

typedef struct lc_engine_stream_query_state {
  lc_engine_client *client;
  lc_engine_write_callback writer;
  void *writer_context;
  lonejson_curl_parse *key_parse;
  lc_engine_query_stream_response *response;
  lc_engine_error *error;
  lc_engine_buffer error_body;
  long http_status;
  int saw_status;
  int write_failed;
  int header_failed;
} lc_engine_stream_query_state;

typedef struct lc_engine_query_key_stream_bridge {
  const lc_engine_query_key_handler *handler;
  void *handler_context;
  lc_engine_error *error;
} lc_engine_query_key_stream_bridge;

typedef struct lc_engine_query_body_json {
  char *ns;
  lonejson_json_value selector;
  lonejson_int64 limit;
  char *cursor;
  lonejson_json_value fields;
  char *return_mode;
} lc_engine_query_body_json;

typedef struct lc_engine_query_keys_response_json {
  lonejson_string_array_stream keys;
  char *cursor;
  lonejson_uint64 index_seq;
  int has_index_seq;
  lonejson_json_value metadata;
} lc_engine_query_keys_response_json;

static const lonejson_field lc_engine_query_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, ns, "namespace"),
    LONEJSON_FIELD_JSON_VALUE_REQ(lc_engine_query_body_json, selector,
                                  "selector"),
    LONEJSON_FIELD_I64(lc_engine_query_body_json, limit, "limit"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, cursor, "cursor"),
    LONEJSON_FIELD_JSON_VALUE(lc_engine_query_body_json, fields, "fields"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, return_mode,
                                "return")};
static const lonejson_field lc_engine_query_keys_response_fields[] = {
    /* lockd omits keys when the result set is empty. */
    LONEJSON_FIELD_STRING_ARRAY_STREAM(lc_engine_query_keys_response_json, keys,
                                       "keys"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_keys_response_json, cursor,
                                "cursor"),
    LONEJSON_FIELD_U64_PRESENT(lc_engine_query_keys_response_json, index_seq,
                               has_index_seq, "index_seq"),
    LONEJSON_FIELD_JSON_VALUE(lc_engine_query_keys_response_json, metadata,
                              "metadata")};
LONEJSON_MAP_DEFINE(lc_engine_query_keys_response_map,
                    lc_engine_query_keys_response_json,
                    lc_engine_query_keys_response_fields);

static int lc_engine_stream_cancel_requested(lc_engine_client *client) {
  return client != NULL && client->cancel_check != NULL &&
         client->cancel_check(client->cancel_context);
}

static int lc_engine_stream_progress(void *client_ptr, curl_off_t dltotal,
                                     curl_off_t dlnow, curl_off_t ultotal,
                                     curl_off_t ulnow) {
  (void)dltotal;
  (void)dlnow;
  (void)ultotal;
  (void)ulnow;
  return lc_engine_stream_cancel_requested((lc_engine_client *)client_ptr);
}

static CURLcode lc_engine_stream_ssl_ctx(CURL *curl, void *ssl_ctx,
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
  if (client->tls_bundle.client_cert != NULL) {
    if (SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
      return CURLE_SSL_CERTPROBLEM;
    }
  }
  if (client->tls_bundle.client_key != NULL) {
    if (SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
      return CURLE_SSL_CERTPROBLEM;
    }
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

static int lc_engine_stream_capture_range(lc_engine_client *client, char **slot,
                                          const char *begin, const char *end) {
  char *copy;

  if (begin == NULL || end == NULL || end < begin) {
    return 1;
  }
  copy = lc_engine_client_strdup_range(client, begin, end);
  if (copy == NULL) {
    return 0;
  }
  lc_engine_client_free_alloc(client, *slot);
  *slot = copy;
  return 1;
}

static int lc_engine_stream_capture_string(lc_engine_client *client,
                                           char **slot, const char *value) {
  if (value == NULL) {
    return 1;
  }
  return lc_engine_stream_capture_range(client, slot, value,
                                        value + strlen(value));
}

static size_t lc_engine_stream_header_callback(char *buffer, size_t size,
                                               size_t nitems, void *userdata) {
  lc_engine_stream_query_state *state;
  size_t total;
  char *colon;
  char *value;
  char *end;
  long parsed;
  char status_line[64];
  size_t status_length;

  state = (lc_engine_stream_query_state *)userdata;
  total = size * nitems;
  if (total == 0U) {
    return 0U;
  }
  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    parsed = 0L;
    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    sscanf(status_line, "%*s %ld", &parsed);
    state->http_status = parsed;
    state->saw_status = 1;
    if (state->response != NULL) {
      state->response->http_status = parsed;
    }
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

  if ((size_t)(colon - buffer) == 20U &&
      strncasecmp(buffer, "X-Lockd-Query-Cursor", 20U) == 0) {
    if (!lc_engine_stream_capture_range(state->client, &state->response->cursor,
                                        value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query cursor");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 23U &&
             strncasecmp(buffer, "X-Lockd-Query-Index-Seq", 23U) == 0) {
    lc_u64 index_seq;

    if (!lc_parse_u64_base10_range_checked(value, (size_t)(end - value),
                                           &index_seq)) {
      lc_engine_set_protocol_error(state->error,
                                   "failed to parse query index sequence");
      state->header_failed = 1;
      return 0U;
    }
    state->response->index_seq = (lc_index_seq)index_seq;
    state->response->index_seq_present = 1;
  } else if ((size_t)(colon - buffer) == 22U &&
             strncasecmp(buffer, "X-Lockd-Query-Metadata", 22U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->metadata_json, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query metadata");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 20U &&
             strncasecmp(buffer, "X-Lockd-Query-Return", 20U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->return_mode, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query return mode");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 16U &&
             strncasecmp(buffer, "X-Correlation-Id", 16U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->correlation_id, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture correlation id");
      state->header_failed = 1;
      return 0U;
    }
  }

  return total;
}

static size_t lc_engine_stream_write_callback(char *ptr, size_t size,
                                              size_t nmemb, void *userdata) {
  lc_engine_stream_query_state *state;
  size_t total;
  size_t written;
  int ok;

  state = (lc_engine_stream_query_state *)userdata;
  total = size * nmemb;
  if (total == 0U) {
    return 0U;
  }
  if (state->saw_status && state->http_status >= 400L) {
    if (lc_engine_buffer_append_limited(&state->error_body, ptr, total,
                                        LC_ENGINE_STREAM_ERROR_BODY_LIMIT) !=
        LC_ENGINE_OK) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to buffer error response");
      state->write_failed = 1;
      return 0U;
    }
    return total;
  }
  if (state->key_parse != NULL) {
    written =
        state->key_parse->write_callback(state->key_parse, ptr, size, nmemb);
    if (written != total) {
      if (state->error->code == LC_ENGINE_OK) {
        lc_engine_lonejson_error_from_status(
            state->error, state->key_parse->error.code,
            &state->key_parse->error, "failed to parse query keys response");
      }
      state->write_failed = 1;
      return 0U;
    }
    return total;
  }
  ok = state->writer(state->writer_context, ptr, total, state->error);
  if (!ok) {
    if (state->error->code == LC_ENGINE_OK) {
      lc_engine_set_transport_error(state->error,
                                    "query writer callback failed");
    }
    state->write_failed = 1;
    return 0U;
  }
  return total;
}

static int lc_engine_query_keys_apply_body_metadata(
    lc_engine_client *client, lc_engine_query_stream_response *response,
    const lc_engine_query_keys_response_json *body, lc_engine_error *error) {
  if (response->cursor == NULL && body->cursor != NULL) {
    if (!lc_engine_stream_capture_string(client, &response->cursor,
                                         body->cursor)) {
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to capture query cursor");
    }
  }
  if (!response->index_seq_present && body->has_index_seq) {
    response->index_seq = (lc_index_seq)body->index_seq;
    response->index_seq_present = 1;
  }
  if (response->metadata_json == NULL &&
      body->metadata.kind != LONEJSON_JSON_VALUE_NULL &&
      body->metadata.json != NULL) {
    if (!lc_engine_stream_capture_range(
            client, &response->metadata_json, body->metadata.json,
            body->metadata.json + body->metadata.len)) {
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to capture query metadata");
    }
  }
  return LC_ENGINE_OK;
}

static lonejson_status lc_engine_query_key_stream_begin(void *user,
                                                        lonejson_error *error) {
  lc_engine_query_key_stream_bridge *bridge;

  (void)error;
  bridge = (lc_engine_query_key_stream_bridge *)user;
  if (bridge == NULL || bridge->handler == NULL ||
      bridge->handler->begin == NULL) {
    return LONEJSON_STATUS_OK;
  }
  if (!bridge->handler->begin(bridge->handler_context, bridge->error)) {
    if (bridge->error != NULL && bridge->error->code == LC_ENGINE_OK) {
      lc_engine_set_transport_error(bridge->error,
                                    "query key begin callback failed");
    }
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_engine_query_key_stream_chunk(void *user,
                                                        const char *data,
                                                        size_t len,
                                                        lonejson_error *error) {
  lc_engine_query_key_stream_bridge *bridge;

  (void)error;
  bridge = (lc_engine_query_key_stream_bridge *)user;
  if (bridge == NULL || bridge->handler == NULL ||
      bridge->handler->chunk == NULL) {
    return LONEJSON_STATUS_OK;
  }
  if (!bridge->handler->chunk(bridge->handler_context, data, len,
                              bridge->error)) {
    if (bridge->error != NULL && bridge->error->code == LC_ENGINE_OK) {
      lc_engine_set_transport_error(bridge->error,
                                    "query key chunk callback failed");
    }
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_engine_query_key_stream_end(void *user,
                                                      lonejson_error *error) {
  lc_engine_query_key_stream_bridge *bridge;

  (void)error;
  bridge = (lc_engine_query_key_stream_bridge *)user;
  if (bridge == NULL || bridge->handler == NULL ||
      bridge->handler->end == NULL) {
    return LONEJSON_STATUS_OK;
  }
  if (!bridge->handler->end(bridge->handler_context, bridge->error)) {
    if (bridge->error != NULL && bridge->error->code == LC_ENGINE_OK) {
      lc_engine_set_transport_error(bridge->error,
                                    "query key end callback failed");
    }
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  return LONEJSON_STATUS_OK;
}

static int lc_engine_query_param_append(char *url, size_t url_length,
                                        size_t *offset, int *has_query,
                                        const char *name, const char *value) {
  int written;

  if (value == NULL || value[0] == '\0') {
    return 1;
  }
  written = snprintf(url + *offset, url_length - *offset, "%c%s=%s",
                     *has_query ? '&' : '?', name, value);
  if (written < 0 || (size_t)written >= url_length - *offset) {
    return 0;
  }
  *offset += (size_t)written;
  *has_query = 1;
  return 1;
}

static char *lc_engine_query_url(const char *endpoint,
                                 const lc_engine_query_request *request) {
  char *engine;
  char *refresh;
  char *url;
  size_t url_length;
  size_t offset;
  int has_query;
  int written;

  engine = NULL;
  refresh = NULL;
  if (request->engine != NULL && request->engine[0] != '\0') {
    engine = lc_engine_url_encode(request->engine);
    if (engine == NULL) {
      return NULL;
    }
  }
  if (request->refresh != NULL && request->refresh[0] != '\0') {
    refresh = lc_engine_url_encode(request->refresh);
    if (refresh == NULL) {
      free(engine);
      return NULL;
    }
  }

  url_length = strlen(endpoint) + strlen("/v1/query") + 1U;
  if (engine != NULL && engine[0] != '\0') {
    url_length += strlen("?engine=") + strlen(engine);
  }
  if (refresh != NULL && refresh[0] != '\0') {
    url_length += strlen("&refresh=") + strlen(refresh);
  }
  url = (char *)malloc(url_length);
  if (url == NULL) {
    free(refresh);
    free(engine);
    return NULL;
  }
  written = snprintf(url, url_length, "%s/v1/query", endpoint);
  if (written < 0 || (size_t)written >= url_length) {
    free(url);
    free(refresh);
    free(engine);
    return NULL;
  }
  offset = (size_t)written;
  has_query = 0;
  if (!lc_engine_query_param_append(url, url_length, &offset, &has_query,
                                    "engine", engine) ||
      !lc_engine_query_param_append(url, url_length, &offset, &has_query,
                                    "refresh", refresh)) {
    free(url);
    free(refresh);
    free(engine);
    return NULL;
  }
  free(refresh);
  free(engine);
  return url;
}

static int
lc_engine_stream_perform_query(lc_engine_client *client, const char *url,
                               const lonejson_curl_upload *body_upload,
                               lc_engine_stream_query_state *state) {
  CURL *curl;
  CURLcode curl_rc;
  struct curl_slist *headers;

  curl = curl_easy_init();
  if (curl == NULL) {
    return lc_engine_set_transport_error(state->error,
                                         "failed to initialize curl");
  }

  headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers,
                              "Accept: application/x-ndjson, application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  if (body_upload != NULL) {
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, lonejson_curl_read_callback);
    curl_easy_setopt(curl, CURLOPT_READDATA, (void *)body_upload);
  }
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                   lc_engine_stream_write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, state);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                   lc_engine_stream_header_callback);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, state);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
  curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, lc_engine_stream_progress);
  curl_easy_setopt(curl, CURLOPT_XFERINFODATA, client);
  if (client->timeout_ms > 0L) {
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, client->timeout_ms);
  }
  if (client->unix_socket_path != NULL && client->unix_socket_path[0] != '\0') {
    curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, client->unix_socket_path);
  }
  if (client->prefer_http_2) {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
  } else {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
  }
  if (!client->disable_mtls) {
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                     client->insecure_skip_verify ? 0L : 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                     client->insecure_skip_verify ? 0L : 2L);
    curl_easy_setopt(curl, CURLOPT_SSL_CTX_FUNCTION, lc_engine_stream_ssl_ctx);
    curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, client);
  }

  curl_rc = curl_easy_perform(curl);
  if (state->header_failed && state->error->code != LC_ENGINE_OK) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return state->error->code;
  }
  if (curl_rc == CURLE_WRITE_ERROR &&
      (state->write_failed || state->header_failed)) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return state->error->code;
  }
  if (curl_rc != CURLE_OK) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (curl_rc == CURLE_ABORTED_BY_CALLBACK &&
        lc_engine_stream_cancel_requested(client)) {
      return lc_engine_set_transport_error(state->error, "request cancelled");
    }
    return lc_engine_set_transport_error(state->error,
                                         curl_easy_strerror(curl_rc));
  }
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &state->http_status);
  state->response->http_status = state->http_status;
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  if (state->header_failed && state->error->code != LC_ENGINE_OK) {
    return state->error->code;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_query_into(lc_engine_client *client,
                                const lc_engine_query_request *request,
                                lc_engine_write_callback writer,
                                void *writer_context,
                                lc_engine_query_stream_response *response,
                                lc_engine_error *error) {
  lonejson *runtime;
  lc_engine_query_body_json body_src;
  lc_engine_stream_query_state state;
  lc_engine_json_reader_source selector_source;
  lc_engine_json_reader_source fields_source;
  lonejson_curl_upload body_upload;
  lonejson_field body_fields[6];
  lonejson_map body_map = {0};
  lonejson_error lj_error;
  char *url;
  size_t endpoint_index;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || writer == NULL || response == NULL ||
      error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "query_into requires client, request, writer, response, and error");
  }
  if (request->selector_json == NULL || request->selector_json[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "query_into requires selector_json");
  }

  memset(response, 0, sizeof(*response));
  memset(&state, 0, sizeof(state));
  memset(&body_src, 0, sizeof(body_src));
  runtime = lc_engine_lonejson_runtime(client);
  state.client = client;
  state.writer = writer;
  state.writer_context = writer_context;
  state.response = response;
  state.error = error;
  lc_engine_buffer_init(&state.error_body);
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_src.limit = request->limit;
  body_src.cursor = (char *)request->cursor;
  body_src.return_mode = (char *)request->return_mode;
  runtime->json_value_init(runtime, &body_src.selector);
  runtime->json_value_init(runtime, &body_src.fields);
  lonejson_error_init(&lj_error);
  selector_source.cursor = (const unsigned char *)request->selector_json;
  selector_source.remaining = strlen(request->selector_json);
  rc = body_src.selector.methods->set_reader(&body_src.selector,
                                             lc_engine_json_memory_reader,
                                             &selector_source, &lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    lc_engine_buffer_cleanup(&state.error_body);
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to configure query selector");
  }
  fields_source.cursor = (const unsigned char *)"";
  fields_source.remaining = 0U;
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    fields_source.cursor = (const unsigned char *)request->fields_json;
    fields_source.remaining = strlen(request->fields_json);
    rc = body_src.fields.methods->set_reader(&body_src.fields,
                                             lc_engine_json_memory_reader,
                                             &fields_source, &lj_error);
    if (rc != LONEJSON_STATUS_OK) {
      runtime->json_value_cleanup(runtime, &body_src.selector);
      lc_engine_buffer_cleanup(&state.error_body);
      return lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to configure query fields");
    }
  }
  body_field_count = 0U;
  if (body_src.ns != NULL && body_src.ns[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_query_body_fields[1];
  if (request->limit > 0L) {
    body_fields[body_field_count++] = lc_engine_query_body_fields[2];
  }
  if (body_src.cursor != NULL && body_src.cursor[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[3];
  }
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[4];
  }
  if (body_src.return_mode != NULL && body_src.return_mode[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[5];
  }
  body_map.name = "lc_engine_query_body_json";
  body_map.struct_size = sizeof(body_src);
  body_map.fields = body_fields;
  body_map.field_count = body_field_count;

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    state.http_status = 0L;
    state.saw_status = 0;
    state.write_failed = 0;
    state.header_failed = 0;
    state.error_body.length = 0U;
    if (state.error_body.data != NULL) {
      state.error_body.data[0] = '\0';
    }
    url = lc_engine_query_url(client->endpoints[endpoint_index], request);
    if (url == NULL) {
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate query URL");
    }
    memset(&body_upload, 0, sizeof(body_upload));
    selector_source.cursor = (const unsigned char *)request->selector_json;
    selector_source.remaining = strlen(request->selector_json);
    fields_source.cursor = (const unsigned char *)"";
    fields_source.remaining = 0U;
    if (request->fields_json != NULL && request->fields_json[0] != '\0') {
      fields_source.cursor = (const unsigned char *)request->fields_json;
      fields_source.remaining = strlen(request->fields_json);
    }
    rc = runtime->curl_upload_init(runtime, &body_upload, &body_map, &body_src);
    if (rc != LONEJSON_STATUS_OK) {
      free(url);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_lonejson_error_from_status(
          error, rc, NULL, "failed to prepare query request body");
    }
    rc = lc_engine_stream_perform_query(client, url, &body_upload, &state);
    lc_lonejson_curl_upload_cleanup(&body_upload);
    free(url);
    if (rc != LC_ENGINE_OK) {
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    if (state.http_status >= 200L && state.http_status < 300L) {
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      return LC_ENGINE_OK;
    }

    rc = lc_engine_set_server_error_from_json(client, error, state.http_status,
                                              response->correlation_id,
                                              state.error_body.data);
    if (error->server_error_code == NULL ||
        strcmp(error->server_error_code, "node_passive") != 0) {
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    lc_engine_query_stream_response_cleanup(client, response);
    lc_engine_error_reset(error);
  }

  runtime->json_value_cleanup(runtime, &body_src.selector);
  runtime->json_value_cleanup(runtime, &body_src.fields);
  lc_engine_buffer_cleanup(&state.error_body);
  lc_engine_query_stream_response_cleanup(client, response);
  return lc_engine_set_transport_error(
      error, "all endpoints rejected the streaming query request");
}

int lc_engine_client_query_keys(lc_engine_client *client,
                                const lc_engine_query_request *request,
                                const lc_engine_query_key_handler *handler,
                                void *handler_context,
                                lc_engine_query_stream_response *response,
                                lc_engine_error *error) {
  lonejson *runtime;
  lc_engine_query_body_json body_src;
  lc_engine_query_keys_response_json key_response;
  lc_engine_query_request url_request;
  lc_engine_stream_query_state state;
  lc_engine_query_key_stream_bridge bridge;
  lc_engine_json_reader_source selector_source;
  lc_engine_json_reader_source fields_source;
  lonejson_curl_upload body_upload;
  lonejson_curl_parse key_parse;
  lonejson_array_stream_string_handler string_handler;
  lonejson_field body_fields[6];
  lonejson_map body_map = {0};
  lonejson_error lj_error;
  char *url;
  size_t endpoint_index;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || handler == NULL ||
      response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "query_keys requires client, request, handler, response, and error");
  }
  if (request->selector_json == NULL || request->selector_json[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "query_keys requires selector_json");
  }

  memset(response, 0, sizeof(*response));
  memset(&state, 0, sizeof(state));
  memset(&bridge, 0, sizeof(bridge));
  memset(&body_src, 0, sizeof(body_src));
  memset(&key_response, 0, sizeof(key_response));
  runtime = lc_engine_lonejson_runtime(client);
  url_request = *request;
  url_request.return_mode = "keys";
  memset(&string_handler, 0, sizeof(string_handler));
  lonejson_error_init(&lj_error);
  state.client = client;
  state.response = response;
  state.error = error;
  bridge.handler = handler;
  bridge.handler_context = handler_context;
  bridge.error = error;
  string_handler.begin = lc_engine_query_key_stream_begin;
  string_handler.chunk = lc_engine_query_key_stream_chunk;
  string_handler.end = lc_engine_query_key_stream_end;
  rc = runtime->init(runtime, &lc_engine_query_keys_response_map, &key_response,
                     &lj_error);
  if (rc != LONEJSON_STATUS_OK)
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to initialize query keys response");
  rc = key_response.keys.set_handler(&key_response.keys, &string_handler,
                                     &bridge, &lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    runtime->cleanup(runtime, &lc_engine_query_keys_response_map, &key_response,
                     NULL);
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to configure query keys stream");
  }
  rc = key_response.metadata.methods->enable_parse_capture(
      &key_response.metadata, &lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    runtime->cleanup(runtime, &lc_engine_query_keys_response_map, &key_response,
                     NULL);
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to configure query metadata capture");
  }
  lc_engine_buffer_init(&state.error_body);
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_src.limit = request->limit;
  body_src.cursor = (char *)request->cursor;
  body_src.return_mode = "keys";
  runtime->json_value_init(runtime, &body_src.selector);
  runtime->json_value_init(runtime, &body_src.fields);
  selector_source.cursor = (const unsigned char *)request->selector_json;
  selector_source.remaining = strlen(request->selector_json);
  rc = body_src.selector.methods->set_reader(&body_src.selector,
                                             lc_engine_json_memory_reader,
                                             &selector_source, &lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    runtime->cleanup(runtime, &lc_engine_query_keys_response_map, &key_response,
                     NULL);
    lc_engine_buffer_cleanup(&state.error_body);
    return lc_engine_lonejson_error_from_status(
        error, rc, &lj_error, "failed to configure query selector");
  }
  fields_source.cursor = (const unsigned char *)"";
  fields_source.remaining = 0U;
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    fields_source.cursor = (const unsigned char *)request->fields_json;
    fields_source.remaining = strlen(request->fields_json);
    rc = body_src.fields.methods->set_reader(&body_src.fields,
                                             lc_engine_json_memory_reader,
                                             &fields_source, &lj_error);
    if (rc != LONEJSON_STATUS_OK) {
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      lc_engine_buffer_cleanup(&state.error_body);
      return lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to configure query fields");
    }
  }
  body_field_count = 0U;
  if (body_src.ns != NULL && body_src.ns[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_query_body_fields[1];
  if (request->limit > 0L) {
    body_fields[body_field_count++] = lc_engine_query_body_fields[2];
  }
  if (body_src.cursor != NULL && body_src.cursor[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[3];
  }
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[4];
  }
  body_fields[body_field_count++] = lc_engine_query_body_fields[5];
  body_map.name = "lc_engine_query_body_json";
  body_map.struct_size = sizeof(body_src);
  body_map.fields = body_fields;
  body_map.field_count = body_field_count;

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    state.http_status = 0L;
    state.saw_status = 0;
    state.write_failed = 0;
    state.header_failed = 0;
    state.error_body.length = 0U;
    if (state.error_body.data != NULL) {
      state.error_body.data[0] = '\0';
    }
    url = lc_engine_query_url(client->endpoints[endpoint_index], &url_request);
    if (url == NULL) {
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate query URL");
    }
    memset(&body_upload, 0, sizeof(body_upload));
    memset(&key_parse, 0, sizeof(key_parse));
    selector_source.cursor = (const unsigned char *)request->selector_json;
    selector_source.remaining = strlen(request->selector_json);
    fields_source.cursor = (const unsigned char *)"";
    fields_source.remaining = 0U;
    if (request->fields_json != NULL && request->fields_json[0] != '\0') {
      fields_source.cursor = (const unsigned char *)request->fields_json;
      fields_source.remaining = strlen(request->fields_json);
    }
    rc = runtime->curl_upload_init(runtime, &body_upload, &body_map, &body_src);
    if (rc != LONEJSON_STATUS_OK) {
      free(url);
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_lonejson_error_from_status(
          error, rc, NULL, "failed to prepare query request body");
    }
    rc = runtime->reset(runtime, &lc_engine_query_keys_response_map,
                        &key_response, &lj_error);
    if (rc == LONEJSON_STATUS_OK)
      rc = key_response.keys.set_handler(&key_response.keys, &string_handler,
                                         &bridge, &lj_error);
    if (rc == LONEJSON_STATUS_OK) {
      rc = key_response.metadata.methods->enable_parse_capture(
          &key_response.metadata, &lj_error);
    }
    if (rc != LONEJSON_STATUS_OK) {
      lc_lonejson_curl_upload_cleanup(&body_upload);
      free(url);
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to configure query keys parser");
    }
    rc = runtime->curl_parse_init(
        runtime, &key_parse, &lc_engine_query_keys_response_map, &key_response);
    if (rc != LONEJSON_STATUS_OK) {
      lc_lonejson_curl_upload_cleanup(&body_upload);
      free(url);
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_lonejson_error_from_status(
          error, rc, &key_parse.error, "failed to prepare query keys parser");
    }
    state.key_parse = &key_parse;
    rc = lc_engine_stream_perform_query(client, url, &body_upload, &state);
    state.key_parse = NULL;
    lc_lonejson_curl_upload_cleanup(&body_upload);
    free(url);
    if (rc != LC_ENGINE_OK) {
      lc_lonejson_curl_parse_cleanup(&key_parse);
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    if (state.http_status >= 200L && state.http_status < 300L) {
      rc = key_parse.finish(&key_parse);
      if (rc != LONEJSON_STATUS_OK) {
        lc_lonejson_curl_parse_cleanup(&key_parse);
        runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                         &key_response, NULL);
        runtime->json_value_cleanup(runtime, &body_src.selector);
        runtime->json_value_cleanup(runtime, &body_src.fields);
        lc_engine_buffer_cleanup(&state.error_body);
        lc_engine_query_stream_response_cleanup(client, response);
        return lc_engine_lonejson_error_from_status(
            error, rc, &key_parse.error, "failed to parse query keys response");
      }
      lc_lonejson_curl_parse_cleanup(&key_parse);
      rc = lc_engine_query_keys_apply_body_metadata(client, response,
                                                    &key_response, error);
      if (rc != LC_ENGINE_OK) {
        runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                         &key_response, NULL);
        runtime->json_value_cleanup(runtime, &body_src.selector);
        runtime->json_value_cleanup(runtime, &body_src.fields);
        lc_engine_buffer_cleanup(&state.error_body);
        lc_engine_query_stream_response_cleanup(client, response);
        return rc;
      }
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      return LC_ENGINE_OK;
    }

    lc_lonejson_curl_parse_cleanup(&key_parse);
    rc = lc_engine_set_server_error_from_json(client, error, state.http_status,
                                              response->correlation_id,
                                              state.error_body.data);
    if (error->server_error_code == NULL ||
        strcmp(error->server_error_code, "node_passive") != 0) {
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    lc_engine_query_stream_response_cleanup(client, response);
    rc = runtime->reset(runtime, &lc_engine_query_keys_response_map,
                        &key_response, &lj_error);
    if (rc == LONEJSON_STATUS_OK)
      rc = key_response.keys.set_handler(&key_response.keys, &string_handler,
                                         &bridge, &lj_error);
    if (rc == LONEJSON_STATUS_OK) {
      rc = key_response.metadata.methods->enable_parse_capture(
          &key_response.metadata, &lj_error);
    }
    if (rc != LONEJSON_STATUS_OK) {
      runtime->cleanup(runtime, &lc_engine_query_keys_response_map,
                       &key_response, NULL);
      runtime->json_value_cleanup(runtime, &body_src.selector);
      runtime->json_value_cleanup(runtime, &body_src.fields);
      lc_engine_buffer_cleanup(&state.error_body);
      return lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to reset query keys parser");
    }
    lc_engine_error_reset(error);
  }

  runtime->cleanup(runtime, &lc_engine_query_keys_response_map, &key_response,
                   NULL);
  runtime->json_value_cleanup(runtime, &body_src.selector);
  runtime->json_value_cleanup(runtime, &body_src.fields);
  lc_engine_buffer_cleanup(&state.error_body);
  lc_engine_query_stream_response_cleanup(client, response);
  return lc_engine_set_transport_error(
      error, "all endpoints rejected the streaming query request");
}
