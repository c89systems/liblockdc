#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_log.h"

static void lc_lease_log_error(lc_lease_handle *lease, pslog_level level,
                               const char *message, const pslog_field *fields,
                               size_t count, const lc_error *error) {
  pslog_field combined[16];
  size_t index;

  if (count + 3U > sizeof(combined) / sizeof(combined[0])) {
    count = sizeof(combined) / sizeof(combined[0]) - 3U;
  }
  for (index = 0U; index < count; ++index) {
    combined[index] = fields[index];
  }
  combined[count + 0U] = lc_log_code_field(error);
  combined[count + 1U] = lc_log_http_status_field(error);
  combined[count + 2U] = lc_log_error_field("error", error);
  switch (level) {
  case PSLOG_LEVEL_DEBUG:
    lc_log_debug(lease->client->logger, message, combined, count + 3U);
    break;
  case PSLOG_LEVEL_WARN:
    lc_log_warn(lease->client->logger, message, combined, count + 3U);
    break;
  default:
    lc_log_error(lease->client->logger, message, combined, count + 3U);
    break;
  }
}

typedef struct lc_lease_lonejson_load_state {
  lonejson_curl_parse parse;
  size_t byte_limit;
  size_t bytes_received;
} lc_lease_lonejson_load_state;

static int lc_lease_lonejson_load_write_callback(void *context,
                                                 const void *bytes,
                                                 size_t count,
                                                 lc_engine_error *error) {
  lc_lease_lonejson_load_state *state;
  size_t written;

  state = (lc_lease_lonejson_load_state *)context;
  if (state == NULL) {
    lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                               "mapped lease load state is required");
    return 0U;
  }
  if (count > 0U && state->byte_limit > 0U &&
      count > state->byte_limit - state->bytes_received) {
    lc_engine_set_protocol_error(
        error, "mapped state response exceeds configured byte limit");
    return 0U;
  }
  if (count == 0U) {
    return 1;
  }
  written =
      state->parse.write_callback(&state->parse, (char *)bytes, 1U, count);
  if (written != count) {
    lc_engine_lonejson_error_from_status(
        error, state->parse.error.code, &state->parse.error,
        "failed to parse mapped lease state response");
    return 0U;
  }
  state->bytes_received += count;
  return 1;
}

static int
lc_lease_duplicate_get_metadata(const char *content_type, const char *etag,
                                const char *correlation_id,
                                char **out_content_type, char **out_etag,
                                char **out_correlation_id, lc_error *error) {
  char *content_type_copy;
  char *etag_copy;
  char *correlation_id_copy;

  content_type_copy = lc_strdup_local(content_type);
  etag_copy = lc_strdup_local(etag);
  correlation_id_copy = lc_strdup_local(correlation_id);
  if ((content_type != NULL && content_type_copy == NULL) ||
      (etag != NULL && etag_copy == NULL) ||
      (correlation_id != NULL && correlation_id_copy == NULL)) {
    free(content_type_copy);
    free(etag_copy);
    free(correlation_id_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mapped load metadata", NULL, NULL,
                        NULL);
  }
  *out_content_type = content_type_copy;
  *out_etag = etag_copy;
  *out_correlation_id = correlation_id_copy;
  return LC_OK;
}

static int lc_lease_refresh_state_view(lc_lease_handle *lease,
                                       const char *state_etag,
                                       lc_version version, long fencing_token,
                                       lc_unix_seconds lease_expires_at_unix,
                                       lc_error *error) {
  char *state_etag_copy;

  if (lease == NULL) {
    return LC_OK;
  }
  state_etag_copy = lc_client_strdup(lease->client, state_etag);
  if (state_etag != NULL && state_etag_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate refreshed lease state etag", NULL,
                        NULL, NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = state_etag_copy;
  lease->version = version;
  if (fencing_token > 0L) {
    lease->fencing_token = fencing_token;
  }
  if (lease_expires_at_unix > 0L) {
    lease->lease_expires_at_unix = lease_expires_at_unix;
  }
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  lease->pub.fencing_token = lease->fencing_token;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  return LC_OK;
}

static int lc_lease_refresh_new_state_etag(lc_lease_handle *lease,
                                           const char *state_etag,
                                           lc_version version,
                                           lc_error *error) {
  char *state_etag_copy;

  state_etag_copy = lc_client_strdup(lease->client, state_etag);
  if (state_etag != NULL && state_etag_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate refreshed lease state etag", NULL,
                        NULL, NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = state_etag_copy;
  lease->version = version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  return LC_OK;
}

int lc_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_describe_request engine_req;
  lc_engine_describe_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease describe requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "describe.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  rc = lc_engine_client_describe(lease->client->engine, &engine_req,
                                 &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "describe.transport_error"
                             : "describe.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_client_free(lease->client, lease->owner);
  lease->owner = lc_client_strdup(lease->client, engine_res.owner);
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = lc_client_strdup(lease->client, engine_res.state_etag);
  lease->lease_expires_at_unix = engine_res.expires_at_unix;
  lease->version = engine_res.version;
  lease->has_query_hidden = engine_res.has_query_hidden;
  lease->query_hidden = engine_res.query_hidden;
  lease->pub.owner = lease->owner;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.version = lease->version;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  lc_lease_retention_observe(self, engine_res.retention_support,
                             engine_res.has_retention_expires_at_unix,
                             engine_res.retention_expires_at_unix);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("version", lease->version);
    fields[3] = lc_log_str_field("state_etag", lease->state_etag);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "describe.success", fields, 5U);
  }
  lc_engine_describe_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_get_method(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
                        lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_request engine_req;
  lc_engine_get_stream_response engine_res;
  lc_engine_error engine_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease get requires self, dst, and out", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(lease->client->logger, "get.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.sink = dst;
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(lease->client->engine, &engine_req,
                                 lc_engine_write_bridge, &bridge, &engine_res,
                                 &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "get.transport_error"
                             : "get.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->content_type = NULL;
  out->etag = NULL;
  out->correlation_id = NULL;
  rc = lc_lease_duplicate_get_metadata(
      engine_res.content_type, engine_res.etag, engine_res.correlation_id,
      &out->content_type, &out->etag, &out->correlation_id, error);
  if (rc != LC_OK) {
    lc_engine_get_stream_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->no_content = engine_res.no_content;
  out->version = engine_res.version;
  out->fencing_token = engine_res.fencing_token;
  out->retention_support = engine_res.retention_support;
  out->has_retention_expires_at_unix = engine_res.has_retention_expires_at_unix;
  out->retention_expires_at_unix = engine_res.retention_expires_at_unix;
  if (opts == NULL || !opts->public_read)
    lc_lease_retention_observe(self, engine_res.retention_support,
                               engine_res.has_retention_expires_at_unix,
                               engine_res.retention_expires_at_unix);
  if (!engine_res.no_content) {
    rc = lc_lease_refresh_state_view(lease, engine_res.etag, engine_res.version,
                                     engine_res.fencing_token, 0L, error);
    if (rc != LC_OK) {
      lc_get_res_cleanup(out);
      lc_engine_error_cleanup(&engine_error);
      return rc;
    }
  }
  if (engine_res.no_content) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_debug(lease->client->logger, "get.empty", fields, 4U);
  } else {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[4] = lc_log_str_field("etag", engine_res.etag);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "get.success", fields, 6U);
  }
  lc_engine_get_stream_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_load_method(lc_lease *self, const lonejson_map *map, void *dst,
                         const lc_get_opts *opts, lc_get_res *out,
                         lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_request engine_req;
  lc_engine_get_stream_response engine_res;
  lc_engine_error engine_error;
  lc_lease_lonejson_load_state load_state;
  lonejson *runtime;
  int no_content;
  char *content_type;
  char *etag;
  char *correlation_id;
  lc_version version;
  long fencing_token;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease load requires self, map, destination, and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(lease->client->logger, "get.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  memset(&load_state, 0, sizeof(load_state));
  runtime = lc_engine_lonejson_runtime(lease->client->engine);
  load_state.byte_limit = lease->client->http_json_response_limit_bytes > 0U
                              ? lease->client->http_json_response_limit_bytes
                              : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
  rc = lc_lonejson_prepare_parse_destination(runtime, map, dst, error);
  if (rc != LC_OK)
    return rc;
  rc = runtime->curl_parse_init(runtime, &load_state.parse, map, dst);
  if (rc != LONEJSON_STATUS_OK) {
    return lc_lonejson_error_from_status(
        error, rc, &load_state.parse.error,
        "failed to initialize mapped lease load parser");
  }
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(lease->client->engine, &engine_req,
                                 lc_lease_lonejson_load_write_callback,
                                 &load_state, &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[7];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "get.transport_error"
                             : "get.error",
                         fields, 4U, error);
    }
    lc_engine_get_stream_response_cleanup(&engine_res);
    lc_lonejson_curl_parse_cleanup(&load_state.parse);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  if (engine_res.no_content) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_debug(lease->client->logger, "get.empty", fields, 4U);
  }
  no_content = engine_res.no_content;
  version = engine_res.version;
  fencing_token = engine_res.fencing_token;
  content_type = NULL;
  etag = NULL;
  correlation_id = NULL;
  if (!engine_res.no_content) {
    rc = load_state.parse.finish(&load_state.parse);
    if (rc != LONEJSON_STATUS_OK) {
      lc_engine_get_stream_response_cleanup(&engine_res);
      lc_engine_error_cleanup(&engine_error);
      lc_lonejson_curl_parse_cleanup(&load_state.parse);
      return lc_lonejson_error_from_status(
          error, rc, &load_state.parse.error,
          "failed to parse mapped lease state");
    }
  }
  rc = lc_lease_duplicate_get_metadata(engine_res.content_type, engine_res.etag,
                                       engine_res.correlation_id, &content_type,
                                       &etag, &correlation_id, error);
  if (rc != LC_OK) {
    lc_engine_get_stream_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    lc_lonejson_curl_parse_cleanup(&load_state.parse);
    return rc;
  }
  if (!engine_res.no_content) {
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      fields[3] = lc_log_str_field("etag", engine_res.etag);
      fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
      lc_log_trace(lease->client->logger, "get.success", fields, 5U);
    }
  }
  lc_lonejson_curl_parse_cleanup(&load_state.parse);
  out->no_content = no_content;
  out->content_type = content_type;
  out->etag = etag;
  out->version = version;
  out->fencing_token = fencing_token;
  out->retention_support = engine_res.retention_support;
  out->has_retention_expires_at_unix = engine_res.has_retention_expires_at_unix;
  out->retention_expires_at_unix = engine_res.retention_expires_at_unix;
  if (opts == NULL || !opts->public_read)
    lc_lease_retention_observe(self, engine_res.retention_support,
                               engine_res.has_retention_expires_at_unix,
                               engine_res.retention_expires_at_unix);
  out->correlation_id = correlation_id;
  if (!no_content) {
    rc = lc_lease_refresh_state_view(lease, engine_res.etag, engine_res.version,
                                     engine_res.fencing_token, 0L, error);
    if (rc != LC_OK) {
      lc_get_res_cleanup(out);
      lc_engine_get_stream_response_cleanup(&engine_res);
      lc_engine_error_cleanup(&engine_error);
      return rc;
    }
  }
  lc_engine_get_stream_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_save_method(lc_lease *self, const lonejson_map *map,
                         const void *src, const lc_update_opts *opts,
                         lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_update_request engine_req;
  lc_engine_update_response engine_res;
  lc_engine_error engine_error;
  lc_update_opts update_opts;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease save requires self, map, and source", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  memset(&update_opts, 0, sizeof(update_opts));
  if (opts != NULL)
    update_opts = *opts;
  if (update_opts.content_type == NULL)
    update_opts.content_type = "application/json";
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.if_state_etag = update_opts.if_state_etag;
  engine_req.if_version = update_opts.if_version;
  engine_req.has_if_version = update_opts.has_if_version;
  if (!engine_req.has_if_version && lease->version > 0L) {
    engine_req.if_version = lease->version;
    engine_req.has_if_version = 1;
  }
  engine_req.content_type = update_opts.content_type;
  engine_req.has_retention_seconds = update_opts.has_retention_seconds;
  engine_req.retention_seconds = update_opts.retention_seconds;
  rc = lc_engine_client_update_stream(lease->client->engine, &engine_req, map,
                                      src, &engine_res, &engine_error);
  if (rc != LC_OK) {
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("content_type", update_opts.content_type);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "update.transport_error"
                             : "update.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  rc = lc_lease_refresh_new_state_etag(lease, engine_res.new_state_etag,
                                       engine_res.new_version, error);
  if (rc != LC_OK) {

    lc_lease_retention_observe(
        self,
        rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
        rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
        engine_res.retention_expires_at_unix);
    lc_engine_update_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("version", lease->version);
    fields[3] = lc_log_str_field("state_etag", lease->state_etag);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    fields[5] = lc_log_str_field("content_type", update_opts.content_type);
    lc_log_trace(lease->client->logger, "update.success", fields, 6U);
  }

  lc_lease_retention_observe(
      self,
      rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
      rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
      engine_res.retention_expires_at_unix);
  lc_engine_update_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_update_method(lc_lease *self, lc_source *src,
                           const lc_update_opts *opts, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_update_request engine_req;
  lc_engine_update_response engine_res;
  lc_engine_error engine_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease update requires self and src", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("content_type",
                                 opts != NULL ? opts->content_type : NULL);
    lc_log_trace(lease->client->logger, "update.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.source = src;
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.if_state_etag = opts != NULL ? opts->if_state_etag : NULL;
  engine_req.if_version = opts != NULL ? opts->if_version : 0L;
  engine_req.has_if_version = opts != NULL ? opts->has_if_version : 0;
  engine_req.has_retention_seconds =
      opts != NULL ? opts->has_retention_seconds : 0;
  engine_req.retention_seconds = opts != NULL ? opts->retention_seconds : 0;
  if (!engine_req.has_if_version && lease->version > 0L) {
    engine_req.if_version = lease->version;
    engine_req.has_if_version = 1;
  }
  engine_req.content_type =
      opts != NULL ? opts->content_type : "application/json";
  rc = lc_engine_client_update_from(lease->client->engine, &engine_req,
                                    lc_engine_read_bridge, &bridge, &engine_res,
                                    &engine_error);
  if (rc != LC_ENGINE_OK) {
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[7];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("content_type",
                                   opts != NULL ? opts->content_type : NULL);
      fields[4] = lc_log_code_field(error);
      fields[5] = lc_log_http_status_field(error);
      fields[6] = lc_log_error_field("error", error);
      lc_log_warn(lease->client->logger, "update.error", fields, 7U);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  rc = lc_lease_refresh_new_state_etag(lease, engine_res.new_state_etag,
                                       engine_res.new_version, error);
  if (rc != LC_OK) {

    lc_lease_retention_observe(
        self,
        rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
        rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
        engine_res.retention_expires_at_unix);
    lc_engine_update_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("new_etag", engine_res.new_state_etag);
    fields[4] = lc_log_i64_field("version", engine_res.new_version);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "update.success", fields, 6U);
  }

  lc_lease_retention_observe(
      self,
      rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
      rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
      engine_res.retention_expires_at_unix);
  lc_engine_update_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_mutate_method(lc_lease *self, const lc_mutate_req *req,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_mutate_request engine_req;
  lc_engine_mutate_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease mutate requires self and req", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    lc_log_trace(lease->client->logger, "mutate.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.if_state_etag = req->if_state_etag;
  engine_req.if_version = req->if_version;
  engine_req.has_if_version = req->has_if_version;
  engine_req.has_retention_seconds = req->has_retention_seconds;
  engine_req.retention_seconds = req->retention_seconds;
  if (!engine_req.has_if_version && lease->version > 0L) {
    engine_req.if_version = lease->version;
    engine_req.has_if_version = 1;
  }
  engine_req.mutations = req->mutations;
  engine_req.mutation_count = req->mutation_count;
  rc = lc_engine_client_mutate(lease->client->engine, &engine_req, &engine_res,
                               &engine_error);
  if (rc != LC_ENGINE_OK) {
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "mutate.transport_error"
                             : "mutate.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  rc = lc_lease_refresh_new_state_etag(lease, engine_res.new_state_etag,
                                       engine_res.new_version, error);
  if (rc != LC_OK) {

    lc_lease_retention_observe(
        self,
        rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
        rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
        engine_res.retention_expires_at_unix);
    lc_engine_mutate_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("new_version", engine_res.new_version);
    fields[4] = lc_log_str_field("new_etag", engine_res.new_state_etag);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "mutate.success", fields, 6U);
  }

  lc_lease_retention_observe(
      self,
      rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
      rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
      engine_res.retention_expires_at_unix);
  lc_engine_mutate_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                             lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_metadata_request engine_req;
  lc_engine_metadata_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease metadata requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "metadata.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  if (req != NULL) {
    engine_req.if_version = req->if_version;
    engine_req.has_if_version = req->has_if_version;
    engine_req.has_retention_seconds = req->has_retention_seconds;
    engine_req.retention_seconds = req->retention_seconds;
    engine_req.has_query_hidden = req->has_query_hidden;
    engine_req.query_hidden = req->query_hidden;
  }
  if (!engine_req.has_if_version && lease->version > 0L) {
    engine_req.if_version = lease->version;
    engine_req.has_if_version = 1;
  }
  rc = lc_engine_client_update_metadata(lease->client->engine, &engine_req,
                                        &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "metadata.transport_error"
                             : "metadata.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lease->version = engine_res.version;
  lease->has_query_hidden = engine_res.has_query_hidden;
  lease->query_hidden = engine_res.query_hidden;
  lease->pub.version = lease->version;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("version", engine_res.version);
    fields[3] = lc_log_bool_field("query_hidden", engine_res.query_hidden);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "metadata.success", fields, 5U);
  }

  lc_lease_retention_observe(
      self,
      rc == LC_OK ? engine_res.retention_support : LC_STATE_RETENTION_UNKNOWN,
      rc == LC_OK ? engine_res.has_retention_expires_at_unix : 0,
      engine_res.retention_expires_at_unix);
  lc_engine_metadata_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_remove_request engine_req;
  lc_engine_remove_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "lease remove requires self",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "remove.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.if_state_etag = req != NULL ? req->if_state_etag : NULL;
  engine_req.if_version = req != NULL ? req->if_version : 0L;
  engine_req.has_if_version = req != NULL ? req->has_if_version : 0;
  if (!engine_req.has_if_version && lease->version > 0L) {
    engine_req.if_version = lease->version;
    engine_req.has_if_version = 1;
  }
  rc = lc_engine_client_remove(lease->client->engine, &engine_req, &engine_res,
                               &engine_error);
  if (rc != LC_ENGINE_OK) {
    lc_lease_retention_observe(self, LC_STATE_RETENTION_UNKNOWN, 0, 0);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "remove.transport_error"
                             : "remove.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  if (engine_res.removed) {
    lc_client_free(lease->client, lease->state_etag);
    lease->state_etag = NULL;
    lease->version = engine_res.new_version;
    lease->pub.state_etag = lease->state_etag;
    lease->pub.version = lease->version;
  }
  lc_lease_retention_observe(self, self->retention_support, 0, 0);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_bool_field("removed", engine_res.removed);
    fields[3] = lc_log_i64_field("new_version", engine_res.new_version);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "remove.success", fields, 5U);
  }
  lc_engine_remove_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                              lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_keepalive_request engine_req;
  lc_engine_keepalive_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease keepalive requires self and req", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("ttl_s", req->ttl_seconds);
    lc_log_trace(lease->client->logger, "keepalive.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.ttl_seconds = req->ttl_seconds;
  engine_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_keepalive(lease->client->engine, &engine_req,
                                  &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("ttl_s", req->ttl_seconds);
      lc_lease_log_error(lease, PSLOG_LEVEL_ERROR, "keepalive.error", fields,
                         4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  rc = lc_lease_refresh_state_view(lease, engine_res.state_etag,
                                   engine_res.version, 0L,
                                   engine_res.lease_expires_at_unix, error);
  if (rc != LC_OK) {
    lc_engine_keepalive_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("version", engine_res.version);
    fields[4] =
        lc_log_i64_field("expires_at", engine_res.lease_expires_at_unix);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "keepalive.success", fields, 6U);
  }
  lc_engine_keepalive_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_release_method(lc_lease *self, const lc_release_req *req,
                            lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_release_request engine_req;
  lc_engine_release_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease release requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_bool_field("rollback", req != NULL ? req->rollback : 0);
    lc_log_trace(lease->client->logger, "release.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.rollback = req != NULL ? req->rollback : 0;
  rc = lc_engine_client_release(lease->client->engine, &engine_req, &engine_res,
                                &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("rollback", req != NULL ? req->rollback : 0);
      lc_lease_log_error(lease, PSLOG_LEVEL_ERROR, "release.error", fields, 4U,
                         error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_bool_field("released", engine_res.released);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "release.success", fields, 5U);
  }
  lc_engine_release_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  lc_lease_close_method(self);
  return LC_OK;
}

int lc_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                           lc_source *src, lc_attach_res *out,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_attach_request engine_req;
  lc_engine_attach_response engine_res;
  lc_engine_error engine_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease attach requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("name", req->name);
    lc_log_trace(lease->client->logger, "attachment.put.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.source = src;
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.name = req->name;
  engine_req.content_type = req->content_type;
  engine_req.max_bytes = req->max_bytes;
  engine_req.has_max_bytes = req->has_max_bytes;
  engine_req.prevent_overwrite = req->prevent_overwrite;
  rc = lc_engine_client_attach_from(lease->client->engine, &engine_req,
                                    lc_engine_read_bridge, &bridge, &engine_res,
                                    &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("name", req->name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "attachment.put.transport_error"
                             : "attachment.put.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &engine_res.attachment);
  out->noop = engine_res.noop;
  out->version = engine_res.version;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("name", engine_res.attachment.name);
    fields[3] = lc_log_str_field("attachment_id", engine_res.attachment.id);
    fields[4] = lc_log_bool_field("noop", engine_res.noop);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "attachment.put.success", fields, 6U);
  }
  lc_engine_attach_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_list_attachments_method(lc_lease *self, lc_attachment_list *out,
                                     lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_list_attachments_request engine_req;
  lc_engine_list_attachments_response engine_res;
  lc_engine_error engine_error;
  size_t index;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "list_attachments requires self and out", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    lc_log_trace(lease->client->logger, "attachment.list.start", fields, 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_list_attachments(lease->client->engine, &engine_req,
                                         &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "attachment.list.transport_error"
                             : "attachment.list.error",
                         fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->count = engine_res.attachment_count;
  if (out->count > 0U) {
    out->items =
        (lc_attachment_info *)calloc(out->count, sizeof(lc_attachment_info));
    if (out->items == NULL) {
      lc_engine_list_attachments_response_cleanup(&engine_res);
      lc_engine_error_cleanup(&engine_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate attachment list", NULL, NULL,
                          NULL);
    }
    for (index = 0U; index < out->count; ++index) {
      lc_attachment_info_copy(&out->items[index],
                              &engine_res.attachments[index]);
    }
  }
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_u64_field("count", out->count);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "attachment.list.success", fields, 4U);
  }
  lc_engine_list_attachments_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_get_attachment_method(lc_lease *self,
                                   const lc_attachment_get_req *req,
                                   lc_sink *dst, lc_attachment_get_res *out,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_attachment_request engine_req;
  lc_engine_get_attachment_response engine_res;
  lc_engine_error engine_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get_attachment requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", req->selector.id);
    fields[3] = lc_log_str_field("name", req->selector.name);
    lc_log_trace(lease->client->logger, "attachment.get.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.sink = dst;
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.public_read = req->public_read;
  engine_req.selector.id = req->selector.id;
  engine_req.selector.name = req->selector.name;
  rc = lc_engine_client_get_attachment_into(lease->client->engine, &engine_req,
                                            lc_engine_write_bridge, &bridge,
                                            &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("attachment_id", req->selector.id);
      fields[3] = lc_log_str_field("name", req->selector.name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "attachment.get.transport_error"
                             : "attachment.get.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &engine_res.attachment);
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", engine_res.attachment.id);
    fields[3] = lc_log_str_field("name", engine_res.attachment.name);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "attachment.get.success", fields, 5U);
  }
  lc_engine_get_attachment_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_delete_attachment_method(lc_lease *self,
                                      const lc_attachment_selector *selector,
                                      int *deleted, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_delete_attachment_request engine_req;
  lc_engine_delete_attachment_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || selector == NULL || deleted == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "delete_attachment requires self, selector, and deleted", NULL, NULL,
        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", selector->id);
    fields[3] = lc_log_str_field("name", selector->name);
    lc_log_trace(lease->client->logger, "attachment.delete.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  engine_req.selector.id = selector->id;
  engine_req.selector.name = selector->name;
  rc = lc_engine_client_delete_attachment(lease->client->engine, &engine_req,
                                          &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("attachment_id", selector->id);
      fields[3] = lc_log_str_field("name", selector->name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "attachment.delete.transport_error"
                             : "attachment.delete.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  *deleted = engine_res.deleted;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", selector->id);
    fields[3] = lc_log_bool_field("deleted", engine_res.deleted);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "attachment.delete.success", fields,
                 5U);
  }
  lc_engine_delete_attachment_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_lease_delete_all_attachments_method(lc_lease *self, int *deleted_count,
                                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_delete_all_attachments_request engine_req;
  lc_engine_delete_all_attachments_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || deleted_count == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "delete_all_attachments requires self and deleted_count", NULL, NULL,
        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    lc_log_trace(lease->client->logger, "attachment.delete_all.start", fields,
                 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.ns = lease->ns;
  engine_req.key = lease->key;
  engine_req.lease_id = lease->lease_id;
  engine_req.txn_id = lease->txn_id;
  engine_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_delete_all_attachments(
      lease->client->engine, &engine_req, &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "attachment.delete_all.transport_error"
                             : "attachment.delete_all.error",
                         fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  *deleted_count = engine_res.deleted;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("deleted", engine_res.deleted);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(lease->client->logger, "attachment.delete_all.success", fields,
                 4U);
  }
  lc_engine_delete_all_attachments_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

void lc_lease_close_method(lc_lease *self) {
  lc_lease_handle *lease;
  lc_client_handle *client;

  if (self == NULL) {
    return;
  }
  lease = (lc_lease_handle *)self;
  client = lease->client;
  lc_client_free(client, lease->ns);
  lc_client_free(client, lease->key);
  lc_client_free(client, lease->owner);
  lc_client_free(client, lease->lease_id);
  lc_client_free(client, lease->txn_id);
  lc_client_free(client, lease->state_etag);
  lc_client_free(client, lease->queue_state_etag);
  lc_client_free(client, lease->pouch_state_key);
  lc_client_free(client, lease->pouch_stage_etag);
  lc_client_free(client, lease);
  lc_client_handle_release(client);
}
