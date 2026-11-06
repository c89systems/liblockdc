#include "lc_api_internal.h"
#include "lc_internal.h"

#include <stdlib.h>
#include <string.h>

static int lc_engine_build_get_path(lc_engine_client *client,
                                    const lc_engine_get_request *request,
                                    lc_engine_buffer *path);
static int lc_engine_build_update_path(lc_engine_client *client,
                                       const lc_engine_update_request *request,
                                       lc_engine_buffer *path);
static int lc_engine_build_mutate_path(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *path);
static int
lc_engine_build_metadata_path(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *path);
static int lc_engine_build_remove_path(lc_engine_client *client,
                                       const lc_engine_remove_request *request,
                                       lc_engine_buffer *path);
static int
lc_engine_build_describe_path(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_buffer *path);
typedef struct lc_engine_body_capture_state {
  lc_engine_buffer buffer;
} lc_engine_body_capture_state;
typedef struct lc_engine_mutate_body_json {
  char *ns;
  lonejson_string_array mutations;
} lc_engine_mutate_body_json;
static const lonejson_field lc_engine_acquire_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, ns, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, key, "key"),
    LONEJSON_FIELD_I64(lc_engine_acquire_request, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, owner, "owner"),
    LONEJSON_FIELD_I64(lc_engine_acquire_request, block_seconds,
                       "block_seconds"),
    LONEJSON_FIELD_BOOL(lc_engine_acquire_request, if_not_exists,
                        "if_not_exists"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, txn_id, "txn_id")};
static const lonejson_field lc_engine_keepalive_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, ns, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_request, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_request, fencing_token,
                       "fencing_token")};
static const lonejson_field lc_engine_release_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, ns, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_release_request, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_BOOL(lc_engine_release_request, rollback, "rollback")};
static const lonejson_field lc_engine_metadata_body_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_metadata_request, query_hidden,
                        "query_hidden")};
static const lonejson_field lc_engine_mutate_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_mutate_body_json, ns, "namespace"),
    LONEJSON_FIELD_STRING_ARRAY(lc_engine_mutate_body_json, mutations,
                                "mutations", LONEJSON_OVERFLOW_FAIL)};
typedef struct lc_engine_query_hidden_json {
  int query_hidden;
} lc_engine_query_hidden_json;
typedef struct lc_engine_acquire_response_json {
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 fencing_token;
} lc_engine_acquire_response_json;
typedef struct lc_engine_keepalive_response_json {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
} lc_engine_keepalive_response_json;
typedef struct lc_engine_release_response_json {
  int released;
} lc_engine_release_response_json;
typedef struct lc_engine_update_response_json {
  lonejson_int64 new_version;
  char *new_state_etag;
  lonejson_int64 bytes;
} lc_engine_update_response_json;
typedef struct lc_engine_metadata_response_json {
  char *ns;
  char *key;
  lonejson_int64 version;
  lc_engine_query_hidden_json metadata;
} lc_engine_metadata_response_json;
typedef struct lc_engine_remove_response_json {
  int removed;
  lonejson_int64 new_version;
} lc_engine_remove_response_json;
typedef struct lc_engine_describe_response_json {
  char *ns;
  char *key;
  char *owner;
  char *lease_id;
  lonejson_int64 expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 updated_at_unix;
  lc_engine_query_hidden_json metadata;
} lc_engine_describe_response_json;
typedef struct lc_engine_queue_stats_response_json {
  char *ns;
  char *queue;
  lonejson_int64 waiting_consumers;
  lonejson_int64 pending_candidates;
  lonejson_int64 total_consumers;
  int has_active_watcher;
  int available;
  char *head_message_id;
  lonejson_int64 head_enqueued_at_unix;
  lonejson_int64 head_not_visible_until_unix;
  lonejson_int64 head_age_seconds;
} lc_engine_queue_stats_response_json;
typedef struct lc_engine_queue_ack_response_json {
  int acked;
} lc_engine_queue_ack_response_json;
typedef struct lc_engine_queue_nack_response_json {
  int requeued;
  char *meta_etag;
} lc_engine_queue_nack_response_json;
typedef struct lc_engine_queue_extend_response_json {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 visibility_timeout_seconds;
  char *meta_etag;
  lonejson_int64 state_lease_expires_at_unix;
} lc_engine_queue_extend_response_json;
static const lonejson_map lc_engine_query_hidden_map;
static const lonejson_field lc_engine_acquire_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, ns,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, owner,
                                "owner"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, txn_id,
                                "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, lease_expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, fencing_token,
                       "fencing_token")};
static const lonejson_field lc_engine_keepalive_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json, lease_expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_response_json, state_etag,
                                "state_etag")};
static const lonejson_field lc_engine_release_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_release_response_json, released, "released")};
static const lonejson_field lc_engine_update_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_update_response_json, new_version,
                       "new_version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_update_response_json, new_state_etag,
                                "new_state_etag"),
    LONEJSON_FIELD_I64(lc_engine_update_response_json, bytes, "bytes")};
static const lonejson_field lc_engine_metadata_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_response_json, ns,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_response_json, key, "key"),
    LONEJSON_FIELD_I64(lc_engine_metadata_response_json, version, "version"),
    LONEJSON_FIELD_OBJECT(lc_engine_metadata_response_json, metadata,
                          "metadata", &lc_engine_query_hidden_map)};
static const lonejson_field lc_engine_remove_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_remove_response_json, removed, "removed"),
    LONEJSON_FIELD_I64(lc_engine_remove_response_json, new_version,
                       "new_version")};
static const lonejson_field lc_engine_describe_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, ns,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, owner,
                                "owner"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, updated_at_unix,
                       "updated_at_unix"),
    LONEJSON_FIELD_OBJECT(lc_engine_describe_response_json, metadata,
                          "metadata", &lc_engine_query_hidden_map)};
static const lonejson_field lc_engine_query_hidden_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_query_hidden_json, query_hidden,
                        "query_hidden")};
LONEJSON_MAP_DEFINE(lc_engine_query_hidden_map, lc_engine_query_hidden_json,
                    lc_engine_query_hidden_fields);
LONEJSON_MAP_DEFINE(lc_engine_acquire_response_map,
                    lc_engine_acquire_response_json,
                    lc_engine_acquire_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_keepalive_response_map,
                    lc_engine_keepalive_response_json,
                    lc_engine_keepalive_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_release_response_map,
                    lc_engine_release_response_json,
                    lc_engine_release_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_update_response_map,
                    lc_engine_update_response_json,
                    lc_engine_update_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_metadata_response_map,
                    lc_engine_metadata_response_json,
                    lc_engine_metadata_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_remove_response_map,
                    lc_engine_remove_response_json,
                    lc_engine_remove_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_describe_response_map,
                    lc_engine_describe_response_json,
                    lc_engine_describe_response_fields);
static int lc_engine_copy_bytes(unsigned char **out_bytes, size_t *out_length,
                                const char *bytes, size_t count);
static int lc_engine_capture_body_writer(void *context, const void *bytes,
                                         size_t count, lc_engine_error *error);
static int lc_engine_set_fencing_header(lonejson_int64 fencing_token,
                                        lc_engine_buffer *buffer,
                                        lc_engine_header_pair *header);
static int lc_engine_apply_common_mutation_headers(
    const char *content_type, const char *lease_id, const char *txn_id,
    const char *if_state_etag, lc_engine_buffer *fence_value,
    lc_engine_header_pair *headers, size_t *header_count,
    lonejson_int64 fencing_token);
static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                lonejson_int64 value);

static void lc_engine_lonejson_map_init(lonejson_map *map, const char *name,
                                        const lonejson_field *fields,
                                        size_t field_count,
                                        size_t struct_size) {
  if (map == NULL) {
    return;
  }
  map->name = name;
  map->struct_size = struct_size;
  map->fields = fields;
  map->field_count = field_count;
}

static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                lonejson_int64 value) {
  char scratch[32];
  int length;

  length = lc_i64_format_base10((lc_i64)value, scratch, sizeof(scratch));
  if (length < 0) {
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  return lc_engine_buffer_append(buffer, scratch, (size_t)length);
}

static int lc_engine_build_get_path(lc_engine_client *client,
                                    const lc_engine_get_request *request,
                                    lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/get?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);
  if (rc == LC_ENGINE_OK && request->public_read)
    rc = lc_engine_buffer_append_cstr(path, "&public=1");

  free(ns);
  free(key);
  return rc;
}

static int lc_engine_build_update_path(lc_engine_client *client,
                                       const lc_engine_update_request *request,
                                       lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/update?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);

  free(ns);
  free(key);
  return rc;
}

static int lc_engine_build_mutate_path(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/mutate?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);

  free(ns);
  free(key);
  return rc;
}

static int
lc_engine_build_metadata_path(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/metadata?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);

  free(ns);
  free(key);
  return rc;
}

static int lc_engine_build_remove_path(lc_engine_client *client,
                                       const lc_engine_remove_request *request,
                                       lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/remove?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);

  free(ns);
  free(key);
  return rc;
}

static int
lc_engine_build_describe_path(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_buffer *path) {
  char *ns;
  char *key;
  int rc;

  ns = lc_engine_url_encode(lc_engine_effective_namespace(client, request->ns));
  key = lc_engine_url_encode(request->key);
  if (ns == NULL || key == NULL) {
    free(ns);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/describe?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, ns);

  free(ns);
  free(key);
  return rc;
}

static int lc_engine_copy_bytes(unsigned char **out_bytes, size_t *out_length,
                                const char *bytes, size_t count) {
  unsigned char *copy;

  *out_bytes = NULL;
  *out_length = 0U;
  if (count == 0U) {
    return LC_ENGINE_OK;
  }
  copy = (unsigned char *)malloc(count);
  if (copy == NULL) {
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  memcpy(copy, bytes, count);
  *out_bytes = copy;
  *out_length = count;
  return LC_ENGINE_OK;
}

static int lc_engine_capture_body_writer(void *context, const void *bytes,
                                         size_t count, lc_engine_error *error) {
  lc_engine_body_capture_state *state;

  state = (lc_engine_body_capture_state *)context;
  if (state == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing body capture state") ==
           LC_ENGINE_OK;
  }
  if (count == 0U) {
    return 1;
  }
  if (lc_engine_buffer_append(&state->buffer, (const char *)bytes, count) !=
      LC_ENGINE_OK) {
    lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                               "failed to capture response body");
    return 0;
  }
  return 1;
}

static int lc_engine_set_fencing_header(lonejson_int64 fencing_token,
                                        lc_engine_buffer *buffer,
                                        lc_engine_header_pair *header) {
  lc_engine_buffer_init(buffer);
  if (lc_engine_buffer_append_long_decimal(buffer, fencing_token) !=
      LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(buffer);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  header->name = "X-Fencing-Token";
  header->value = buffer->data;
  return LC_ENGINE_OK;
}

static int lc_engine_apply_common_mutation_headers(
    const char *content_type, const char *lease_id, const char *txn_id,
    const char *if_state_etag, lc_engine_buffer *fence_value,
    lc_engine_header_pair *headers, size_t *header_count,
    lonejson_int64 fencing_token) {
  int rc;

  *header_count = 0U;
  headers[*header_count].name = "Content-Type";
  headers[*header_count].value = content_type;
  *header_count += 1U;
  rc = lc_engine_set_fencing_header(fencing_token, fence_value,
                                    &headers[*header_count]);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  *header_count += 1U;
  if (lease_id != NULL && lease_id[0] != '\0') {
    headers[*header_count].name = "X-Lease-ID";
    headers[*header_count].value = lease_id;
    *header_count += 1U;
  }
  headers[*header_count].name = "X-Txn-ID";
  headers[*header_count].value = txn_id;
  *header_count += 1U;
  if (if_state_etag != NULL && if_state_etag[0] != '\0') {
    headers[*header_count].name = "X-If-State-ETag";
    headers[*header_count].value = if_state_etag;
    *header_count += 1U;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_acquire(lc_engine_client *client,
                             const lc_engine_acquire_request *request,
                             lc_engine_acquire_response *response,
                             lc_engine_error *error) {
  lc_engine_http_result result;
  lc_engine_acquire_request body_src;
  lonejson_field body_fields[7];
  lonejson_map body_map = {0};
  lc_engine_header_pair headers[1];
  lc_engine_acquire_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_acquire_response_cleanup(response);
  body_src = *request;
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[0];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[1];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[2];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[3];
  if (request->block_seconds > 0L) {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[4];
  }
  if (request->if_not_exists) {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[5];
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[6];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_acquire_request",
                              body_fields, body_field_count, sizeof(body_src));
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/acquire", &body_map, &body_src, headers, 1U,
      &lc_engine_acquire_response_map, &response_json, &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->ns = lc_engine_strdup_local(response_json.ns);
  response->key = lc_engine_strdup_local(response_json.key);
  response->owner = lc_engine_strdup_local(response_json.owner);
  response->lease_id = lc_engine_strdup_local(response_json.lease_id);
  response->txn_id = lc_engine_strdup_local(response_json.txn_id);
  response->lease_expires_at_unix = response_json.lease_expires_at_unix;
  response->version = response_json.version;
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  response->fencing_token = response_json.fencing_token;
  if ((response_json.ns != NULL && response->ns == NULL) ||
      (response_json.key != NULL && response->key == NULL) ||
      (response_json.owner != NULL && response->owner == NULL) ||
      (response_json.lease_id != NULL && response->lease_id == NULL) ||
      (response_json.txn_id != NULL && response->txn_id == NULL) ||
      (response_json.state_etag != NULL && response->state_etag == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_acquire_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode acquire response");
  }

  lc_engine_lonejson_runtime(client)->cleanup(
      lc_engine_lonejson_runtime(client), &lc_engine_acquire_response_map,
      &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_get(lc_engine_client *client,
                         const lc_engine_get_request *request,
                         lc_engine_get_response *response,
                         lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_body_capture_state body_state;
  lc_engine_get_stream_response stream_response;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "get.key is required");
  }
  if (!request->public_read &&
      (request->lease_id == NULL || request->lease_id[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get requires lease_id unless public_read is enabled");
  }

  lc_engine_get_response_cleanup(response);
  rc = lc_engine_build_get_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build get path");
  }

  lc_engine_buffer_init(&fence_value);
  if (!request->public_read) {
    lc_engine_header_pair empty_header = {0};

    rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                      &empty_header);
    if (rc != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&path);
      return rc;
    }
  }

  lc_engine_buffer_init(&body_state.buffer);
  memset(&stream_response, 0, sizeof(stream_response));
  rc = lc_engine_client_get_into(client, request, lc_engine_capture_body_writer,
                                 &body_state, &stream_response, error);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body_state.buffer);
    return rc;
  }

  if (!stream_response.no_content) {
    rc = lc_engine_copy_bytes(&response->body, &response->body_length,
                              body_state.buffer.data, body_state.buffer.length);
    if (rc != LC_ENGINE_OK) {
      lc_engine_get_response_cleanup(response);
      lc_engine_buffer_cleanup(&body_state.buffer);
      lc_engine_get_stream_response_cleanup(&stream_response);
      return lc_engine_set_client_error(error, rc, "failed to copy get body");
    }
  }
  response->content_type = lc_engine_strdup_local(
      stream_response.content_type != NULL ? stream_response.content_type
                                           : "application/octet-stream");
  if (response->content_type == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (rc == LC_ENGINE_OK && stream_response.etag != NULL) {
    response->etag = lc_engine_strdup_local(stream_response.etag);
    if (response->etag == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc == LC_ENGINE_OK) {
    response->version = stream_response.version;
    response->fencing_token = stream_response.fencing_token;
    response->retention_support = stream_response.retention_support;
    response->has_retention_expires_at_unix =
        stream_response.has_retention_expires_at_unix;
    response->retention_expires_at_unix =
        stream_response.retention_expires_at_unix;
  }
  if (rc == LC_ENGINE_OK && stream_response.correlation_id != NULL) {
    response->correlation_id =
        lc_engine_strdup_local(stream_response.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_get_response_cleanup(response);
    lc_engine_buffer_cleanup(&body_state.buffer);
    lc_engine_get_stream_response_cleanup(&stream_response);
    return lc_engine_set_client_error(error, rc, "failed to copy get response");
  }

  lc_engine_buffer_cleanup(&body_state.buffer);
  lc_engine_get_stream_response_cleanup(&stream_response);
  return LC_ENGINE_OK;
}

int lc_engine_client_keepalive(lc_engine_client *client,
                               const lc_engine_keepalive_request *request,
                               lc_engine_keepalive_response *response,
                               lc_engine_error *error) {
  lc_engine_buffer fence_value;
  lc_engine_keepalive_request body_src;
  lonejson_field body_fields[6];
  lonejson_map body_map = {0};
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_keepalive_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_keepalive_response_cleanup(response);
  body_src = *request;
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[0];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[1];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[2];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[4];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[5];
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_keepalive_body_fields[3];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_keepalive_request",
                              body_fields, body_field_count, sizeof(body_src));
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare keepalive headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/keepalive", &body_map, &body_src, headers, 2U,
      &lc_engine_keepalive_response_map, &response_json, &result, error);
  lc_engine_buffer_cleanup(&fence_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->lease_expires_at_unix = response_json.lease_expires_at_unix;
  response->version = response_json.version;
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  if (response_json.state_etag != NULL && response->state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_keepalive_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode keepalive response");
  }

  lc_engine_lonejson_runtime(client)->cleanup(
      lc_engine_lonejson_runtime(client), &lc_engine_keepalive_response_map,
      &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_release(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_release_response *response,
                             lc_engine_error *error) {
  lc_engine_buffer fence_value;
  lc_engine_release_request body_src;
  lonejson_field body_fields[6];
  lonejson_map body_map = {0};
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_release_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_release_response_cleanup(response);
  body_src = *request;
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_release_body_fields[0];
  body_fields[body_field_count++] = lc_engine_release_body_fields[1];
  body_fields[body_field_count++] = lc_engine_release_body_fields[2];
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_release_body_fields[3];
  }
  if (request->rollback) {
    body_fields[body_field_count++] = lc_engine_release_body_fields[5];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_release_request",
                              body_fields, body_field_count, sizeof(body_src));
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare release headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/release", &body_map, &body_src, headers, 2U,
      &lc_engine_release_response_map, &response_json, &result, error);
  lc_engine_buffer_cleanup(&fence_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->released = response_json.released;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_release_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode release response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_update_response_cleanup(lc_engine_update_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->new_state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_update_stream(lc_engine_client *client,
                                   const lc_engine_update_request *request,
                                   const lonejson_map *body_map,
                                   const void *body_src,
                                   lc_engine_update_response *response,
                                   lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[7];
  char retention_value[32];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (body_map == NULL || body_src == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "update_stream requires body map and source");
  }
  if (request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "update requires key, txn_id, and fencing_token");
  }

  rc = lc_engine_retention_validate(client, request->has_retention_seconds,
                                    request->retention_seconds, request->ns,
                                    request->key, error);
  if (rc != LC_ENGINE_OK)
    return rc;
  lc_engine_update_response_cleanup(response);
  rc = lc_engine_build_update_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build update path");
  }

  rc = lc_engine_apply_common_mutation_headers(
      request->content_type != NULL ? request->content_type
                                    : "application/octet-stream",
      request->lease_id, request->txn_id, request->if_state_etag, &fence_value,
      headers, &header_count, request->fencing_token);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare update headers");
  }

  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare update version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  if (request->has_retention_seconds) {
    snprintf(retention_value, sizeof(retention_value), "%ld",
             request->retention_seconds);
    headers[header_count].name = "X-State-Retention-Seconds";
    headers[header_count++].value = retention_value;
  }
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, body_map, body_src, headers, header_count,
      &lc_engine_update_response_map, &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->new_version = response_json.new_version;
  response->retention_support = result.retention.support;
  response->has_retention_expires_at_unix = result.retention.has_expiry;
  response->retention_expires_at_unix = result.retention.expiry;
  response->new_state_etag =
      lc_engine_strdup_local(response_json.new_state_etag);
  if (response_json.new_state_etag != NULL &&
      response->new_state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_update_response_cleanup(response);
    lc_engine_free_string(&response_json.new_state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode update response");
  }

  lc_engine_lonejson_runtime(client)->cleanup(
      lc_engine_lonejson_runtime(client), &lc_engine_update_response_map,
      &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_mutate_response_cleanup(lc_engine_mutate_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->new_state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_mutate(lc_engine_client *client,
                            const lc_engine_mutate_request *request,
                            lc_engine_mutate_response *response,
                            lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_mutate_body_json body_src;
  lonejson_field body_fields[2];
  lonejson_map body_map = {0};
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[7];
  char retention_value[32];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t body_field_count;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  rc = lc_engine_retention_validate(client, request->has_retention_seconds,
                                    request->retention_seconds, request->ns,
                                    request->key, error);
  if (rc != LC_ENGINE_OK)
    return rc;
  lc_engine_mutate_response_cleanup(response);
  rc = lc_engine_build_mutate_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build mutate path");
  }
  body_src.ns = (char *)lc_engine_effective_namespace(client, request->ns);
  body_src.mutations.items = (char **)request->mutations;
  body_src.mutations.count = request->mutation_count;
  body_src.mutations.capacity = request->mutation_count;
  body_src.mutations.flags = LONEJSON_ARRAY_FIXED_CAPACITY;
  body_field_count = 0U;
  if (body_src.ns != NULL && body_src.ns[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_mutate_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_mutate_body_fields[1];
  lc_engine_lonejson_map_init(&body_map, "lc_engine_mutate_body_json",
                              body_fields, body_field_count, sizeof(body_src));
  rc = lc_engine_apply_common_mutation_headers(
      "application/json", request->lease_id, request->txn_id,
      request->if_state_etag, &fence_value, headers, &header_count,
      request->fencing_token);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare mutate headers");
  }
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare mutate version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  if (request->has_retention_seconds) {
    snprintf(retention_value, sizeof(retention_value), "%ld",
             request->retention_seconds);
    headers[header_count].name = "X-State-Retention-Seconds";
    headers[header_count++].value = retention_value;
  }
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, &body_map, &body_src, headers, header_count,
      &lc_engine_update_response_map, &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->new_version = response_json.new_version;
  response->retention_support = result.retention.support;
  response->has_retention_expires_at_unix = result.retention.has_expiry;
  response->retention_expires_at_unix = result.retention.expiry;
  response->new_state_etag =
      lc_engine_strdup_local(response_json.new_state_etag);
  if (response_json.new_state_etag != NULL &&
      response->new_state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_mutate_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode mutate response");
  }

  lc_engine_lonejson_cleanup(client, &lc_engine_update_response_map,
                             &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_metadata_response_cleanup(
    lc_engine_metadata_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_update_metadata(lc_engine_client *client,
                                     const lc_engine_metadata_request *request,
                                     lc_engine_metadata_response *response,
                                     lc_engine_error *error) {
  lc_engine_buffer path;
  lonejson_field body_fields[8];
  lonejson_map body_map = {0};
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[6];
  char retention_value[32];
  lc_engine_http_result result;
  lc_engine_metadata_response_json response_json;
  size_t body_field_count;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  rc = lc_engine_retention_validate(client, request->has_retention_seconds,
                                    request->retention_seconds, request->ns,
                                    request->key, error);
  if (rc != LC_ENGINE_OK)
    return rc;
  lc_engine_metadata_response_cleanup(response);
  rc = lc_engine_build_metadata_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build metadata path");
  }
  header_count = 0U;
  headers[header_count].name = "Content-Type";
  headers[header_count].value = "application/json";
  ++header_count;
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[header_count]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare metadata headers");
  }
  ++header_count;
  if (request->lease_id != NULL && request->lease_id[0] != '\0') {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
  }
  headers[header_count].name = "X-Txn-ID";
  headers[header_count].value = request->txn_id;
  ++header_count;
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare metadata version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  body_field_count = 0U;
  if (request->has_query_hidden) {
    body_fields[body_field_count++] = lc_engine_metadata_body_fields[0];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_metadata_request",
                              body_fields, body_field_count, sizeof(*request));
  if (request->has_retention_seconds) {
    snprintf(retention_value, sizeof(retention_value), "%ld",
             request->retention_seconds);
    headers[header_count].name = "X-State-Retention-Seconds";
    headers[header_count++].value = retention_value;
  }
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, &body_map, request, headers, header_count,
      &lc_engine_metadata_response_map, &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->ns = lc_engine_strdup_local(response_json.ns);
  response->key = lc_engine_strdup_local(response_json.key);
  if ((response_json.ns != NULL && response->ns == NULL) ||
      (response_json.key != NULL && response->key == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->version = response_json.version;
  response->retention_support = result.retention.support;
  response->has_retention_expires_at_unix = result.retention.has_expiry;
  response->retention_expires_at_unix = result.retention.expiry;
  response->has_query_hidden = 1;
  response->query_hidden = response_json.metadata.query_hidden;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_metadata_response_cleanup(response);
    lc_engine_free_string(&response_json.ns);
    lc_engine_free_string(&response_json.key);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode metadata response");
  }

  lc_engine_lonejson_cleanup(client, &lc_engine_metadata_response_map,
                             &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_remove_response_cleanup(lc_engine_remove_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_remove(lc_engine_client *client,
                            const lc_engine_remove_request *request,
                            lc_engine_remove_response *response,
                            lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[6];
  lc_engine_http_result result;
  lc_engine_remove_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "remove requires key, txn_id, and fencing_token");
  }

  lc_engine_remove_response_cleanup(response);
  rc = lc_engine_build_remove_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build remove path");
  }
  header_count = 0U;
  headers[header_count].name = "Content-Type";
  headers[header_count].value = "application/json";
  ++header_count;
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[header_count]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare remove headers");
  }
  ++header_count;
  if (request->lease_id != NULL && request->lease_id[0] != '\0') {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
  }
  headers[header_count].name = "X-Txn-ID";
  headers[header_count].value = request->txn_id;
  ++header_count;
  if (request->if_state_etag != NULL && request->if_state_etag[0] != '\0') {
    headers[header_count].name = "X-If-State-ETag";
    headers[header_count].value = request->if_state_etag;
    ++header_count;
  }
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare remove version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", path.data, NULL, 0U, headers,
                                   header_count, &lc_engine_remove_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->removed = response_json.removed;
  response->new_version = response_json.new_version;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_remove_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode remove response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_describe_response_cleanup(
    lc_engine_describe_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->ns);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->owner);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_describe(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_describe_response *response,
                              lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_http_result result;
  lc_engine_describe_response_json response_json;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "describe.key is required");
  }

  lc_engine_describe_response_cleanup(response);
  rc = lc_engine_build_describe_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build describe path");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "GET", path.data, NULL, 0U, NULL, 0U,
                                   &lc_engine_describe_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->ns = lc_engine_strdup_local(response_json.ns);
  response->key = lc_engine_strdup_local(response_json.key);
  response->owner = lc_engine_strdup_local(response_json.owner);
  response->lease_id = lc_engine_strdup_local(response_json.lease_id);
  if ((response_json.ns != NULL && response->ns == NULL) ||
      (response_json.key != NULL && response->key == NULL) ||
      (response_json.owner != NULL && response->owner == NULL) ||
      (response_json.lease_id != NULL && response->lease_id == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->expires_at_unix = response_json.expires_at_unix;
  response->version = response_json.version;
  response->retention_support = result.retention.support;
  response->has_retention_expires_at_unix = result.retention.has_expiry;
  response->retention_expires_at_unix = result.retention.expiry;
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  if (response_json.state_etag != NULL && response->state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->updated_at_unix = response_json.updated_at_unix;
  response->has_query_hidden = 1;
  response->query_hidden = response_json.metadata.query_hidden;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_describe_response_cleanup(response);
    lc_engine_free_string(&response_json.ns);
    lc_engine_free_string(&response_json.key);
    lc_engine_free_string(&response_json.owner);
    lc_engine_free_string(&response_json.lease_id);
    lc_engine_free_string(&response_json.state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode describe response");
  }

  lc_engine_lonejson_cleanup(client, &lc_engine_describe_response_map,
                             &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}
