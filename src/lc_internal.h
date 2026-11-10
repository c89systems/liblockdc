#ifndef LC_ENGINE_INTERNAL_H
#define LC_ENGINE_INTERNAL_H

#include <curl/curl.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <pslog.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>

#ifndef LONEJSON_WITH_CURL
#define LONEJSON_WITH_CURL
#endif
#include <lonejson.h>

#include "lc_engine_api.h"
#include "lc_intcompat.h"

typedef struct lc_engine_buffer {
  char *data;
  size_t length;
  size_t capacity;
} lc_engine_buffer;

typedef struct lc_engine_header_pair {
  const char *name;
  const char *value;
} lc_engine_header_pair;

typedef struct lc_engine_json_reader_source {
  const unsigned char *cursor;
  size_t remaining;
} lc_engine_json_reader_source;

/* Retention is response-local. An expiry alone advertises support; an absent
 * header never proves the backend accepted a requested policy. */
typedef struct lc_engine_retention_headers {
  lc_state_retention_support support;
  int has_support;
  int has_expiry;
  lc_unix_seconds expiry;
} lc_engine_retention_headers;

/* Returns one for a parsed field, zero for another header, minus one if
 * invalid. */
int lc_engine_retention_header(lc_engine_retention_headers *headers,
                               const char *name, size_t name_length,
                               const char *value, size_t value_length);
int lc_engine_retention_validate(lc_engine_client *client, int selected,
                                 long seconds, const char *ns, const char *key,
                                 lc_engine_error *error);

typedef struct lc_engine_http_result {
  lc_engine_retention_headers retention;
  long http_status;
  char *correlation_id;
  char *etag;
  lc_version key_version;
  long fencing_token;
  char *content_type;
  char *header_parse_error_message;
  char *server_error_code;
  char *detail;
  char *leader_endpoint;
  char *current_etag;
  long current_version;
  long retry_after_seconds;
  int header_parse_failed;
} lc_engine_http_result;

typedef struct lc_engine_tls_bundle {
  X509 *client_cert;
  EVP_PKEY *client_key;
  X509 **ca_certs;
  size_t ca_cert_count;
  X509 **chain_certs;
  size_t chain_cert_count;
} lc_engine_tls_bundle;

struct lc_engine_client {
  char **endpoints;
  size_t endpoint_count;
  char *unix_socket_path;
  char *default_namespace;
  long timeout_ms;
  struct timespec request_deadline;
  int request_deadline_set;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  size_t http_json_response_limit_bytes;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
  int owns_logger;
  int (*cancel_check)(void *context);
  void *cancel_context;
  lc_engine_allocator allocator;
  lc_engine_tls_bundle tls_bundle;
  lonejson *lonejson;
};

void lc_engine_error_reset(lc_engine_error *error);
int lc_engine_client_attempt_timeout_ms(const lc_engine_client *client,
                                        long *out);
int lc_engine_set_client_error(lc_engine_error *error, int code,
                               const char *message);
int lc_engine_set_transport_error(lc_engine_error *error, const char *message);
int lc_engine_set_protocol_error(lc_engine_error *error, const char *message);
int lc_engine_set_server_error_from_result(lc_engine_error *error,
                                           const lc_engine_http_result *result);

char *lc_engine_strdup_local(const char *value);
char *lc_engine_strdup_range(const char *begin, const char *end);
void lc_engine_free_string(char **value);
void lc_engine_trim_trailing_slash(char *value);
void *lc_engine_client_alloc(lc_engine_client *client, size_t size);
void lc_engine_client_free_alloc(lc_engine_client *client, void *ptr);
char *lc_engine_client_strdup(lc_engine_client *client, const char *value);
char *lc_engine_client_strdup_range(lc_engine_client *client, const char *begin,
                                    const char *end);

void lc_engine_buffer_init(lc_engine_buffer *buffer);
void lc_engine_buffer_cleanup(lc_engine_buffer *buffer);
int lc_engine_buffer_append(lc_engine_buffer *buffer, const char *bytes,
                            size_t count);
int lc_engine_buffer_append_cstr(lc_engine_buffer *buffer, const char *value);
int lc_engine_lonejson_error_from_status(lc_engine_error *error,
                                         lonejson_status status,
                                         const lonejson_error *lj_error,
                                         const char *message);
size_t lc_lonejson_runtime_limit(size_t configured_limit);
void lc_lonejson_config_init(lonejson_config *config, size_t limit);
lonejson *lc_engine_lonejson_runtime(lc_engine_client *client);
lonejson *lc_thread_lonejson_runtime(void);
void lc_lonejson_cleanup_value(lonejson *runtime, const lonejson_map *map,
                               void *value);
void lc_engine_lonejson_cleanup(lc_engine_client *client,
                                const lonejson_map *map, void *value);
/* Iterative walk; traversal allocation failure leaves the destination intact.
 */
int lc_lonejson_prepare_parse_destination(lonejson *runtime,
                                          const lonejson_map *map, void *value,
                                          lc_error *error);
int lc_lonejson_parse_file(lonejson *runtime, FILE *fp, const lonejson_map *map,
                           void *dst, lc_error *error, const char *message);
int lc_lonejson_parse_prepared_file(lonejson *runtime, FILE *fp,
                                    const lonejson_map *map, void *dst,
                                    lc_error *error, const char *message);
int lc_lonejson_serialize_file(lonejson *runtime, FILE *fp,
                               const lonejson_map *map, const void *src,
                               lc_error *error, const char *message);
#ifdef LONEJSON_WITH_CURL
struct lonejson_curl_parse;
struct lonejson_curl_upload;
void lc_lonejson_curl_parse_cleanup(struct lonejson_curl_parse *parse);
void lc_lonejson_curl_upload_cleanup(struct lonejson_curl_upload *upload);
#endif
lonejson_status lc_lonejson_parse_cstr_value(lonejson *runtime,
                                             const lonejson_map *map, void *dst,
                                             const char *json,
                                             lonejson_error *error);
lonejson_read_result lc_engine_json_memory_reader(void *user,
                                                  unsigned char *buffer,
                                                  size_t capacity);
int lc_engine_buffer_append_limited(lc_engine_buffer *buffer, const char *bytes,
                                    size_t count, size_t limit);

void lc_engine_http_result_cleanup(lc_engine_http_result *result);
int lc_engine_http_json_request(
    lc_engine_client *client, const char *method, const char *path,
    const void *body, size_t body_length, const lc_engine_header_pair *headers,
    size_t header_count, const lonejson_map *response_map, void *response,
    lc_engine_http_result *result, lc_engine_error *error);
int lc_engine_client_update_stream(lc_engine_client *client,
                                   const lc_engine_update_request *request,
                                   const lonejson_map *body_map,
                                   const void *body_src,
                                   lc_engine_update_response *response,
                                   lc_engine_error *error);
int lc_engine_http_json_request_stream(
    lc_engine_client *client, const char *method, const char *path,
    const lonejson_map *body_map, const void *body_src,
    const lc_engine_header_pair *headers, size_t header_count,
    const lonejson_map *response_map, void *response,
    lc_engine_http_result *result, lc_engine_error *error);
int lc_engine_set_server_error_from_json(lc_engine_client *client,
                                         lc_engine_error *error,
                                         long http_status,
                                         const char *correlation_id,
                                         const char *json);

int lc_engine_load_bundle(lc_engine_client *client, lc_source *bundle_source,
                          const char *bundle_path, lc_engine_error *error);
void lc_engine_free_bundle(lc_engine_tls_bundle *bundle);

const char *lc_engine_effective_namespace(lc_engine_client *client,
                                          const char *ns);
char *lc_engine_url_encode(const char *value);

#endif
