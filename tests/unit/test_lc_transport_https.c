#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cmocka.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_engine_api.h"

#define HTTPS_TRANSPORT_TMP_PREFIX "/tmp/liblockdc-transport-"

typedef struct test_value_doc {
  lonejson_int64 value;
} test_value_doc;

typedef struct client_closing_sink {
  lc_client *client;
  lc_sink *delegate;
  size_t write_calls;
} client_closing_sink;

static const lonejson_field test_value_fields[] = {
    LONEJSON_FIELD_I64(test_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(test_value_map, test_value_doc, test_value_fields);

typedef struct test_json_value_doc {
  lonejson_json_value payload;
} test_json_value_doc;

static const lonejson_field test_json_value_fields[] = {
    LONEJSON_FIELD_JSON_VALUE_REQ(test_json_value_doc, payload, "payload")};

LONEJSON_MAP_DEFINE(test_json_value_map, test_json_value_doc,
                    test_json_value_fields);

typedef struct test_json_value_with_label_doc {
  lonejson_json_value payload;
  char label[64];
} test_json_value_with_label_doc;

static const lonejson_field test_json_value_with_label_fields[] = {
    LONEJSON_FIELD_JSON_VALUE_REQ(test_json_value_with_label_doc, payload,
                                  "payload"),
    LONEJSON_FIELD_STRING_FIXED(test_json_value_with_label_doc, label, "label",
                                LONEJSON_OVERFLOW_FAIL)};

LONEJSON_MAP_DEFINE(test_json_value_with_label_map,
                    test_json_value_with_label_doc,
                    test_json_value_with_label_fields);

typedef struct test_large_payload_doc {
  char payload[1024];
} test_large_payload_doc;

static const lonejson_field test_large_payload_fields[] = {
    LONEJSON_FIELD_STRING_FIXED(test_large_payload_doc, payload, "payload",
                                LONEJSON_OVERFLOW_FAIL)};

LONEJSON_MAP_DEFINE(test_large_payload_map, test_large_payload_doc,
                    test_large_payload_fields);

typedef struct test_spooled_payload_doc {
  lonejson_spooled payload;
} test_spooled_payload_doc;

static const lonejson_field test_spooled_payload_fields[] = {
    LONEJSON_FIELD_STRING_STREAM_REQ(test_spooled_payload_doc, payload,
                                     "payload")};

LONEJSON_MAP_DEFINE(test_spooled_payload_map, test_spooled_payload_doc,
                    test_spooled_payload_fields);

typedef struct test_optional_label_doc {
  lonejson_int64 value;
  char label[64];
} test_optional_label_doc;

static const lonejson_field test_optional_label_fields[] = {
    LONEJSON_FIELD_I64(test_optional_label_doc, value, "value"),
    LONEJSON_FIELD_STRING_FIXED(test_optional_label_doc, label, "label",
                                LONEJSON_OVERFLOW_FAIL)};

LONEJSON_MAP_DEFINE(test_optional_label_map, test_optional_label_doc,
                    test_optional_label_fields);

typedef struct test_request_capture {
  char *data;
  size_t length;
  size_t capacity;
} test_request_capture;

static void test_lonejson_cleanup_value(const lonejson_map *map, void *value) {
  lonejson *runtime;

  runtime = lc_thread_lonejson_runtime();
  assert_int_equal(runtime->cleanup(runtime, map, value, NULL),
                   LONEJSON_STATUS_OK);
}

typedef struct https_expectation {
  const char *method;
  const char *path;
  const char *const *required_headers;
  size_t required_header_count;
  const char *const *required_body_substrings;
  size_t required_body_substring_count;
  int expect_empty_body;
  int response_status;
  const char *const *response_headers;
  size_t response_header_count;
  const char *response_body;
  const char *expected_client_cn;
  size_t response_body_chunk_size;
  const char *const *response_trailers;
  size_t response_trailer_count;
  const char *forbidden_header;
} https_expectation;

typedef struct https_tls_material {
  char temp_dir[PATH_MAX];
  char client_bundle_path[PATH_MAX];
  char client_bundle_without_ca_path[PATH_MAX];
  X509 *ca_cert;
  EVP_PKEY *ca_key;
  X509 *server_cert;
  EVP_PKEY *server_key;
  int uses_shared_state;
} https_tls_material;

typedef struct https_testserver {
  int listener_fd;
  unsigned short port;
  SSL_CTX *ssl_ctx;
  pthread_t thread;
  const https_expectation *expectations;
  size_t expectation_count;
  size_t handled_count;
  long response_delay_ms;
  int allow_response_write_failure;
  volatile sig_atomic_t stop_requested;
  char failure_message[1024];
} https_testserver;

typedef struct delayed_cancel_check {
  pthread_mutex_t mutex;
  int requested;
} delayed_cancel_check;

typedef struct subscribe_capture {
  int begin_calls;
  int end_calls;
  char message_id[64];
  char payload[256];
  size_t payload_length;
} subscribe_capture;

typedef struct canceling_subscribe_capture {
  subscribe_capture capture;
  int cancel_requested;
} canceling_subscribe_capture;

typedef struct watch_capture {
  int event_count;
  char ns[64];
  char queue[64];
  char head_message_id[64];
  char correlation_id[64];
  long long changed_at_unix;
  int available;
} watch_capture;

typedef struct query_key_capture {
  int begin_calls;
  int chunk_calls;
  int end_calls;
  int fail_on_chunk;
  int fail_on_end;
  char bytes[256];
  size_t length;
} query_key_capture;

typedef struct query_key_large_capture {
  size_t begin_calls;
  size_t chunk_calls;
  size_t end_calls;
  size_t total_bytes;
  size_t current_length;
  size_t current_index;
  char current[128];
  char first[128];
  char last[128];
} query_key_large_capture;

typedef struct tracking_allocator_state {
  size_t malloc_calls;
  size_t realloc_calls;
  size_t free_calls;
  size_t bytes_requested;
} tracking_allocator_state;

typedef struct test_enqueue_source {
  lc_source pub;
  lc_client *client_to_close;
  const unsigned char *bytes;
  size_t length;
  size_t offset;
  size_t read_count;
  size_t reset_count;
  size_t max_chunk;
} test_enqueue_source;

static https_tls_material shared_tls_material;
static int shared_tls_material_initialized;

static size_t test_enqueue_source_read(lc_source *self, void *buffer,
                                       size_t count, lc_error *error) {
  test_enqueue_source *source;
  size_t remaining;
  size_t chunk;

  (void)error;
  source = (test_enqueue_source *)self;
  if (source == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  if (source->client_to_close != NULL) {
    lc_client_close(source->client_to_close);
    source->client_to_close = NULL;
  }
  if (source->offset >= source->length) {
    return 0U;
  }
  ++source->read_count;
  remaining = source->length - source->offset;
  chunk = count < remaining ? count : remaining;
  if (source->max_chunk > 0U && chunk > source->max_chunk) {
    chunk = source->max_chunk;
  }
  memcpy(buffer, source->bytes + source->offset, chunk);
  source->offset += chunk;
  return chunk;
}

static int test_enqueue_source_reset(lc_source *self, lc_error *error) {
  test_enqueue_source *source;

  (void)error;
  source = (test_enqueue_source *)self;
  if (source == NULL) {
    return 0;
  }
  ++source->reset_count;
  source->offset = 0U;
  return 1;
}

static void test_enqueue_source_close(lc_source *self) { free(self); }

static const char test_ca_cert_pem[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBnTCCAUOgAwIBAgIUOWr99fMFJ1QzGFbUpwdb+top+jAwCgYIKoZIzj0EAwIw\n"
    "HDEaMBgGA1UEAwwRbGlibG9ja2RjLXRlc3QtY2EwHhcNMjYwNTEzMDkyNDUyWhcN\n"
    "MzYwNTEwMDkyNDUyWjAcMRowGAYDVQQDDBFsaWJsb2NrZGMtdGVzdC1jYTBZMBMG\n"
    "ByqGSM49AgEGCCqGSM49AwEHA0IABJVbOSJmV0Mpdf+efxL7TkvWqhdFXpVPAOZa\n"
    "UU/HUGXV3E0uZ4TmdNh3mB1ZL/LGFwabH986Xz373Eu1EONhJDOjYzBhMB0GA1Ud\n"
    "DgQWBBRhJsVVpWFQwPbEZbzHchpB6gADUzAfBgNVHSMEGDAWgBRhJsVVpWFQwPbE\n"
    "ZbzHchpB6gADUzAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjAKBggq\n"
    "hkjOPQQDAgNIADBFAiBqJim3EWrI6T7djPWbigkHAioxzQsNSxn235I/zTjGCAIh\n"
    "AKxXEPuExIVGeRAj3u+ufdNeoCQ1BJ0gs1q9kx7hATCh\n"
    "-----END CERTIFICATE-----\n";

static const char test_server_cert_pem[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBujCCAV+gAwIBAgITcXftkPHe43ReOThojIto8afVuzAKBggqhkjOPQQDAjAc\n"
    "MRowGAYDVQQDDBFsaWJsb2NrZGMtdGVzdC1jYTAeFw0yNjA1MTMwOTI1MjBaFw0z\n"
    "NjA1MTAwOTI1MjBaMBQxEjAQBgNVBAMMCTEyNy4wLjAuMTBZMBMGByqGSM49AgEG\n"
    "CCqGSM49AwEHA0IABFwVMIW0hbixJw8kVMl4Q/91CQou6jOgYHbzGCbZjS8cj2zD\n"
    "fp2W2/9FLJLgXCheiQtgs5FVM3UTafmhr6u1blGjgYcwgYQwDAYDVR0TAQH/BAIw\n"
    "ADAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0RBAgw\n"
    "BocEfwAAATAdBgNVHQ4EFgQUHLf7oMF8LK0B4M7Fs1L1KimczI8wHwYDVR0jBBgw\n"
    "FoAUYSbFVaVhUMD2xGW8x3IaQeoAA1MwCgYIKoZIzj0EAwIDSQAwRgIhAL6EQPan\n"
    "NsEcJS32fVUAqOvOIwLat1kNvigoLWDfMgmAAiEAmKZ9gN1EQkCiUihfRt8p15oi\n"
    "yXb+97r7yY5Edwcqaj8=\n"
    "-----END CERTIFICATE-----\n";

static const char test_server_key_pem[] =
    "-----BEGIN EC PRIVATE KEY-----\n"
    "MHcCAQEEIB82xUKiuFyk14MPXt1Wn6oh/0GZPrGQTK3EllZNvQEgoAoGCCqGSM49\n"
    "AwEHoUQDQgAEXBUwhbSFuLEnDyRUyXhD/3UJCi7qM6BgdvMYJtmNLxyPbMN+nZbb\n"
    "/0UskuBcKF6JC2CzkVUzdRNp+aGvq7VuUQ==\n"
    "-----END EC PRIVATE KEY-----\n";

static const char test_client_cert_pem[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBsjCCAVigAwIBAgITcXftkPHe43ReOThojIto8afVvTAKBggqhkjOPQQDAjAc\n"
    "MRowGAYDVQQDDBFsaWJsb2NrZGMtdGVzdC1jYTAeFw0yNjA1MTMwOTMwMjFaFw0z\n"
    "NjA1MTAwOTMwMjFaMCAxHjAcBgNVBAMMFWxpYmxvY2tkYyB0ZXN0IGNsaWVudDBZ\n"
    "MBMGByqGSM49AgEGCCqGSM49AwEHA0IABDsuNvQzPLbcyLgHlP/LMUhN4QlwVq0+\n"
    "4mTM3oX0clJhPI4PVTEocaW5KN2UkOPEpT0udpGF4NpqSktW3RuMO3ajdTBzMAwG\n"
    "A1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMC\n"
    "MB0GA1UdDgQWBBQR8wr4fCTmUD2wgOgErzgCP0p5lzAfBgNVHSMEGDAWgBRhJsVV\n"
    "pWFQwPbEZbzHchpB6gADUzAKBggqhkjOPQQDAgNIADBFAiEAkuu6OJ7zfr662Zag\n"
    "mgAZsa5G826L2lj8vpg5NTwtcrICIHH7U/bQzd/duxhMhvMC5hiPVP20mNzHkIAS\n"
    "v/vIcWo/\n"
    "-----END CERTIFICATE-----\n";

static const char test_client_key_pem[] =
    "-----BEGIN EC PRIVATE KEY-----\n"
    "MHcCAQEEIG0AZ89DCmoEjxTzzkBlFVWQDSpL87NfazNArOz1zcGXoAoGCCqGSM49\n"
    "AwEHoUQDQgAEOy429DM8ttzIuAeU/8sxSE3hCXBWrT7iZMzehfRyUmE8jg9VMShx\n"
    "pbko3ZSQ48SlPS52kYXg2mpKS1bdG4w7dg==\n"
    "-----END EC PRIVATE KEY-----\n";

static int test_enqueue_source_new(const void *bytes, size_t length,
                                   size_t max_chunk, lc_source **out,
                                   test_enqueue_source **out_state,
                                   lc_error *error) {
  test_enqueue_source *source;

  if (out == NULL || out_state == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "enqueue source output is required", NULL, NULL, NULL);
  }
  source = (test_enqueue_source *)calloc(1, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate enqueue source", NULL, NULL, NULL);
  }
  source->pub.read = test_enqueue_source_read;
  source->pub.reset = test_enqueue_source_reset;
  source->pub.close = test_enqueue_source_close;
  source->bytes = (const unsigned char *)bytes;
  source->length = length;
  source->max_chunk = max_chunk;
  *out = &source->pub;
  *out_state = source;
  return LC_OK;
}

static int test_enqueue_source_new_non_rewindable(
    const void *bytes, size_t length, size_t max_chunk, lc_source **out,
    test_enqueue_source **out_state, lc_error *error) {
  int rc;

  rc = test_enqueue_source_new(bytes, length, max_chunk, out, out_state, error);
  if (rc != LC_OK) {
    return rc;
  }
  (*out)->reset = NULL;
  return LC_OK;
}

static int watch_event_sink(void *context,
                            const lc_engine_queue_watch_event *event,
                            lc_engine_error *error) {
  (void)context;
  (void)event;
  (void)error;
  return 1;
}

static int watch_capture_sink(void *context,
                              const lc_engine_queue_watch_event *event,
                              lc_engine_error *error) {
  watch_capture *capture;

  (void)error;
  capture = (watch_capture *)context;
  capture->event_count += 1;
  if (event->ns != NULL) {
    snprintf(capture->ns, sizeof(capture->ns), "%s", event->ns);
  }
  if (event->queue != NULL) {
    snprintf(capture->queue, sizeof(capture->queue), "%s", event->queue);
  }
  if (event->head_message_id != NULL) {
    snprintf(capture->head_message_id, sizeof(capture->head_message_id), "%s",
             event->head_message_id);
  }
  if (event->correlation_id != NULL) {
    snprintf(capture->correlation_id, sizeof(capture->correlation_id), "%s",
             event->correlation_id);
  }
  capture->changed_at_unix = event->changed_at_unix;
  capture->available = event->available;
  return 1;
}

static int capture_query_key_begin(void *context, lc_error *error) {
  query_key_capture *capture;

  (void)error;
  capture = (query_key_capture *)context;
  capture->begin_calls += 1;
  if (capture->length + 1U < sizeof(capture->bytes)) {
    capture->bytes[capture->length++] = '[';
    capture->bytes[capture->length] = '\0';
  }
  return 1;
}

static int capture_query_key_chunk(void *context, const char *bytes, size_t len,
                                   lc_error *error) {
  query_key_capture *capture;
  size_t available;

  capture = (query_key_capture *)context;
  capture->chunk_calls += 1;
  if (capture->fail_on_chunk) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L, "capture rejected query key",
                 NULL, NULL, NULL);
    return 0;
  }
  available = sizeof(capture->bytes) - capture->length - 1U;
  if (len > available) {
    len = available;
  }
  if (len > 0U) {
    memcpy(capture->bytes + capture->length, bytes, len);
    capture->length += len;
    capture->bytes[capture->length] = '\0';
  }
  return 1;
}

static int capture_query_key_end(void *context, lc_error *error) {
  query_key_capture *capture;

  (void)error;
  capture = (query_key_capture *)context;
  capture->end_calls += 1;
  if (capture->fail_on_end) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L, "capture rejected query key end",
                 NULL, NULL, NULL);
    return 0;
  }
  if (capture->length + 1U < sizeof(capture->bytes)) {
    capture->bytes[capture->length++] = ']';
    capture->bytes[capture->length] = '\0';
  }
  return 1;
}

static int capture_large_query_key_begin(void *context, lc_error *error) {
  query_key_large_capture *capture;

  (void)error;
  capture = (query_key_large_capture *)context;
  capture->begin_calls += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int capture_large_query_key_chunk(void *context, const char *bytes,
                                         size_t len, lc_error *error) {
  query_key_large_capture *capture;
  size_t available;

  (void)error;
  capture = (query_key_large_capture *)context;
  capture->chunk_calls += 1U;
  capture->total_bytes += len;
  available = sizeof(capture->current) - capture->current_length - 1U;
  if (len > available) {
    len = available;
  }
  if (len > 0U) {
    memcpy(capture->current + capture->current_length, bytes, len);
    capture->current_length += len;
    capture->current[capture->current_length] = '\0';
  }
  return 1;
}

static int capture_large_query_key_end(void *context, lc_error *error) {
  query_key_large_capture *capture;

  (void)error;
  capture = (query_key_large_capture *)context;
  if (capture->end_calls == 0U) {
    snprintf(capture->first, sizeof(capture->first), "%s", capture->current);
  }
  snprintf(capture->last, sizeof(capture->last), "%s", capture->current);
  capture->end_calls += 1U;
  capture->current_index += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

static void set_failure(https_testserver *server, const char *format, ...) {
  va_list args;

  if (server->failure_message[0] != '\0') {
    return;
  }
  va_start(args, format);
  vsnprintf(server->failure_message, sizeof(server->failure_message), format,
            args);
  va_end(args);
}

static int buffer_append(test_request_capture *capture, const void *bytes,
                         size_t count) {
  char *next;
  size_t required;
  size_t capacity;

  required = capture->length + count + 1U;
  capacity = capture->capacity == 0U ? 1024U : capture->capacity;
  while (capacity < required) {
    capacity *= 2U;
  }
  if (capacity != capture->capacity) {
    next = (char *)realloc(capture->data, capacity);
    if (next == NULL) {
      return 0;
    }
    capture->data = next;
    capture->capacity = capacity;
  }
  memcpy(capture->data + capture->length, bytes, count);
  capture->length += count;
  capture->data[capture->length] = '\0';
  return 1;
}

static void buffer_cleanup(test_request_capture *capture) {
  free(capture->data);
  memset(capture, 0, sizeof(*capture));
}

static int g_fail_lc_strdup_local_on_call;
static int g_fail_lc_client_strdup_on_call;
static int g_lc_strdup_local_call_count;
static int g_lc_client_strdup_call_count;

static void reset_strdup_failure_state(void) {
  g_fail_lc_strdup_local_on_call = 0;
  g_fail_lc_client_strdup_on_call = 0;
  g_lc_strdup_local_call_count = 0;
  g_lc_client_strdup_call_count = 0;
}

static void *tracking_malloc(void *context, size_t size) {
  tracking_allocator_state *state;

  state = (tracking_allocator_state *)context;
  if (state != NULL) {
    state->malloc_calls += 1U;
    state->bytes_requested += size;
  }
  return malloc(size);
}

static void *tracking_realloc(void *context, void *ptr, size_t size) {
  tracking_allocator_state *state;

  state = (tracking_allocator_state *)context;
  if (state != NULL) {
    state->realloc_calls += 1U;
    state->bytes_requested += size;
  }
  return realloc(ptr, size);
}

static void tracking_free(void *context, void *ptr) {
  tracking_allocator_state *state;

  state = (tracking_allocator_state *)context;
  if (state != NULL && ptr != NULL) {
    state->free_calls += 1U;
  }
  free(ptr);
}

static void tracking_allocator_configure(lc_allocator *allocator,
                                         tracking_allocator_state *state) {
  lc_allocator_init(allocator);
  allocator->malloc_fn = tracking_malloc;
  allocator->realloc_fn = tracking_realloc;
  allocator->free_fn = tracking_free;
  allocator->context = state;
}

char *__real_lc_strdup_local(const char *value);
char *__real_lc_client_strdup(lc_client_handle *client, const char *value);

char *__wrap_lc_strdup_local(const char *value) {
  if (value != NULL) {
    g_lc_strdup_local_call_count += 1;
    if (g_fail_lc_strdup_local_on_call > 0 &&
        g_lc_strdup_local_call_count == g_fail_lc_strdup_local_on_call) {
      return NULL;
    }
  }
  return __real_lc_strdup_local(value);
}

char *__wrap_lc_client_strdup(lc_client_handle *client, const char *value) {
  if (value != NULL) {
    g_lc_client_strdup_call_count += 1;
    if (g_fail_lc_client_strdup_on_call > 0 &&
        g_lc_client_strdup_call_count == g_fail_lc_client_strdup_on_call) {
      return NULL;
    }
  }
  return __real_lc_client_strdup(client, value);
}

static lonejson_status test_lonejson_capture_sink(void *user, const void *data,
                                                  size_t len,
                                                  lonejson_error *error) {
  test_request_capture *capture;

  (void)error;
  capture = (test_request_capture *)user;
  return buffer_append(capture, data, len) ? LONEJSON_STATUS_OK
                                           : LONEJSON_STATUS_ALLOCATION_FAILED;
}

static int client_closing_sink_write(lc_sink *self, const void *bytes,
                                     size_t count, lc_error *error) {
  client_closing_sink *sink;

  sink = (client_closing_sink *)self->impl;
  if (sink == NULL || sink->delegate == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "client-closing sink is unavailable", NULL, NULL, NULL);
  }
  sink->write_calls += 1U;
  if (sink->client != NULL) {
    lc_client_close(sink->client);
    sink->client = NULL;
  }
  return sink->delegate->write(sink->delegate, bytes, count, error);
}

static void client_closing_sink_close(lc_sink *self) {
  client_closing_sink *sink;

  sink = self != NULL ? (client_closing_sink *)self->impl : NULL;
  if (sink != NULL && sink->delegate != NULL) {
    lc_sink_close(sink->delegate);
    sink->delegate = NULL;
  }
}

static char *make_repeat_json_body(const char *prefix, const char *suffix,
                                   size_t payload_len, char fill) {
  size_t prefix_len;
  size_t suffix_len;
  char *body;

  prefix_len = strlen(prefix);
  suffix_len = strlen(suffix);
  body = (char *)malloc(prefix_len + payload_len + suffix_len + 1U);
  if (body == NULL) {
    return NULL;
  }
  memcpy(body, prefix, prefix_len);
  memset(body + prefix_len, fill, payload_len);
  memcpy(body + prefix_len + payload_len, suffix, suffix_len);
  body[prefix_len + payload_len + suffix_len] = '\0';
  return body;
}

static void format_large_query_key(size_t index, char *buffer,
                                   size_t capacity) {
  snprintf(buffer, capacity, "resource/large-key-%04zu", index);
}

static char *make_large_query_keys_body(size_t key_count,
                                        size_t *out_total_key_bytes) {
  test_request_capture body;
  char key[64];
  size_t index;

  memset(&body, 0, sizeof(body));
  if (!buffer_append(&body, "{\"keys\":[", strlen("{\"keys\":["))) {
    return NULL;
  }
  if (out_total_key_bytes != NULL) {
    *out_total_key_bytes = 0U;
  }
  for (index = 0U; index < key_count; ++index) {
    format_large_query_key(index, key, sizeof(key));
    if (index > 0U && !buffer_append(&body, ",", 1U)) {
      buffer_cleanup(&body);
      return NULL;
    }
    if (!buffer_append(&body, "\"", 1U) ||
        !buffer_append(&body, key, strlen(key)) ||
        !buffer_append(&body, "\"", 1U)) {
      buffer_cleanup(&body);
      return NULL;
    }
    if (out_total_key_bytes != NULL) {
      *out_total_key_bytes += strlen(key);
    }
  }
  if (!buffer_append(&body, "]}", strlen("]}"))) {
    buffer_cleanup(&body);
    return NULL;
  }
  return body.data;
}

static char *make_watch_body_with_oversized_event_data_after_valid_event(void) {
  static const char valid_event[] =
      "event: queue_watch\r\n"
      "data: {\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"available\":true,\"head_message_id\":\"msg-1\","
      "\"changed_at_unix\":12345}\r\n"
      "\r\n";
  static const char data_prefix[] = "data: ";
  static const char crlf[] = "\r\n";
  test_request_capture body;
  char *line_payload;
  size_t index;

  memset(&body, 0, sizeof(body));
  line_payload = (char *)malloc(16384U);
  if (line_payload == NULL) {
    return NULL;
  }
  memset(line_payload, 'e', 16384U);
  if (!buffer_append(&body, valid_event, strlen(valid_event)) ||
      !buffer_append(&body, "event: queue_watch\r\n",
                     strlen("event: queue_watch\r\n"))) {
    buffer_cleanup(&body);
    free(line_payload);
    return NULL;
  }
  for (index = 0U; index < 80U; ++index) {
    if (!buffer_append(&body, data_prefix, strlen(data_prefix)) ||
        !buffer_append(&body, line_payload, 16384U) ||
        !buffer_append(&body, crlf, strlen(crlf))) {
      buffer_cleanup(&body);
      free(line_payload);
      return NULL;
    }
  }
  if (!buffer_append(&body, crlf, strlen(crlf))) {
    buffer_cleanup(&body);
    free(line_payload);
    return NULL;
  }
  free(line_payload);
  return body.data;
}

static char *make_subscribe_meta_with_extra_headers(size_t extra_header_count) {
  static const char prefix[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n";
  static const char meta[] =
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}";
  static const char suffix[] = "\r\n--queue-boundary--\r\n";
  test_request_capture body;
  char header[64];
  size_t index;
  int written;

  memset(&body, 0, sizeof(body));
  if (!buffer_append(&body, prefix, strlen(prefix))) {
    buffer_cleanup(&body);
    return NULL;
  }
  for (index = 0U; index < extra_header_count; ++index) {
    written = snprintf(header, sizeof(header), "X-Test-%zu: value\r\n", index);
    if (written < 0 || (size_t)written >= sizeof(header) ||
        !buffer_append(&body, header, (size_t)written)) {
      buffer_cleanup(&body);
      return NULL;
    }
  }
  written = snprintf(header, sizeof(header), "Content-Length: %zu\r\n\r\n",
                     strlen(meta));
  if (written < 0 || (size_t)written >= sizeof(header) ||
      !buffer_append(&body, header, (size_t)written) ||
      !buffer_append(&body, meta, strlen(meta)) ||
      !buffer_append(&body, suffix, strlen(suffix))) {
    buffer_cleanup(&body);
    return NULL;
  }
  return body.data;
}

static char *make_subscribe_body_with_oversized_header_after_delivery(void) {
  static const char delivered[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n";

  static const char bad_header_prefix[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"; x=\"";
  static const char suffix[] = "\"\r\n"
                               "Content-Type: application/json\r\n"
                               "Content-Length: 2\r\n"
                               "\r\n"
                               "{}\r\n"
                               "--queue-boundary--\r\n";
  test_request_capture body;
  char *header_value;

  memset(&body, 0, sizeof(body));
  header_value = (char *)malloc(70000U);
  if (header_value == NULL) {
    return NULL;
  }
  memset(header_value, 'h', 70000U);
  if (!buffer_append(&body, delivered, strlen(delivered)) ||
      !buffer_append(&body, bad_header_prefix, strlen(bad_header_prefix)) ||
      !buffer_append(&body, header_value, 70000U) ||
      !buffer_append(&body, suffix, strlen(suffix))) {
    buffer_cleanup(&body);
    free(header_value);
    return NULL;
  }
  free(header_value);
  return body.data;
}

static const char *find_header_end(const char *data, size_t length) {
  size_t index;

  if (length < 4U) {
    return NULL;
  }
  for (index = 0U; index + 4U <= length; ++index) {
    if (memcmp(data + index, "\r\n\r\n", 4U) == 0) {
      return data + index;
    }
  }
  return NULL;
}

static long parse_content_length(const char *headers) {
  const char *cursor;

  cursor = headers;
  while (cursor != NULL && *cursor != '\0') {
    const char *line_end;

    if (strncasecmp(cursor, "Content-Length:", 15U) == 0) {
      cursor += 15U;
      while (*cursor == ' ' || *cursor == '\t') {
        ++cursor;
      }
      return strtol(cursor, NULL, 10);
    }
    line_end = strstr(cursor, "\r\n");
    if (line_end == NULL) {
      break;
    }
    cursor = line_end + 2;
  }
  return 0L;
}

static int request_uses_chunked_transfer_encoding(const char *headers) {
  return strstr(headers, "Transfer-Encoding: chunked") != NULL;
}

static int chunked_request_complete(const char *body, size_t length) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    const char *line_end;
    char *end_ptr;
    unsigned long chunk_size;
    size_t chunk_begin;

    line_end = strstr(body + offset, "\r\n");
    if (line_end == NULL) {
      return 0;
    }
    chunk_size = strtoul(body + offset, &end_ptr, 16);
    if (end_ptr != line_end) {
      return 0;
    }
    offset = (size_t)(line_end - body) + 2U;
    if (chunk_size == 0UL) {
      return offset + 2U <= length && memcmp(body + offset, "\r\n", 2U) == 0;
    }
    chunk_begin = offset;
    if ((size_t)chunk_size > length - chunk_begin) {
      return 0;
    }
    offset = chunk_begin + (size_t)chunk_size;
    if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
      return 0;
    }
    offset += 2U;
  }
  return 0;
}

static int decode_chunked_request_body(const char *body, size_t length,
                                       char **out_body) {
  test_request_capture decoded;
  size_t offset;

  if (out_body == NULL) {
    return 0;
  }
  *out_body = NULL;
  memset(&decoded, 0, sizeof(decoded));
  offset = 0U;
  while (offset < length) {
    const char *line_end;
    char *end_ptr;
    unsigned long chunk_size;
    size_t chunk_begin;

    line_end = strstr(body + offset, "\r\n");
    if (line_end == NULL) {
      buffer_cleanup(&decoded);
      return 0;
    }
    chunk_size = strtoul(body + offset, &end_ptr, 16);
    if (end_ptr != line_end) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset = (size_t)(line_end - body) + 2U;
    if (chunk_size == 0UL) {
      if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
        buffer_cleanup(&decoded);
        return 0;
      }
      *out_body = decoded.data;
      decoded.data = NULL;
      return 1;
    }
    chunk_begin = offset;
    if ((size_t)chunk_size > length - chunk_begin) {
      buffer_cleanup(&decoded);
      return 0;
    }
    if (!buffer_append(&decoded, body + chunk_begin, (size_t)chunk_size)) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset = chunk_begin + (size_t)chunk_size;
    if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset += 2U;
  }
  buffer_cleanup(&decoded);
  return 0;
}

static int read_http_request(SSL *ssl, test_request_capture *capture) {
  char chunk[2048];
  const char *header_end;
  long content_length;
  size_t target_length;
  int chunked;

  memset(capture, 0, sizeof(*capture));
  header_end = NULL;
  content_length = 0L;
  target_length = 0U;
  chunked = 0;
  while (1) {
    int read_count;

    read_count = SSL_read(ssl, chunk, (int)sizeof(chunk));
    if (read_count <= 0) {
      return 0;
    }
    if (!buffer_append(capture, chunk, (size_t)read_count)) {
      return 0;
    }
    if (header_end == NULL) {
      header_end = find_header_end(capture->data, capture->length);
      if (header_end != NULL) {
        /* libcurl waits a full second before sending an upload body unless
         * the server acknowledges Expect. Model the real HTTP handshake
         * instead of making every streaming case pay that fallback delay. */
        const char *expect = strstr(capture->data, "Expect: 100-continue\r\n");
        if (expect != NULL && expect < header_end &&
            SSL_write(ssl, "HTTP/1.1 100 Continue\r\n\r\n", 25) != 25)
          return 0;
        chunked = request_uses_chunked_transfer_encoding(capture->data);
        content_length = parse_content_length(capture->data);
        target_length = (size_t)((header_end + 4) - capture->data);
        if (chunked) {
          target_length = 0U;
        } else if (content_length > 0L) {
          target_length += (size_t)content_length;
        }
      }
    }
    if (header_end != NULL) {
      if (chunked) {
        if (chunked_request_complete(
                header_end + 4,
                capture->length - (size_t)((header_end + 4) - capture->data))) {
          return 1;
        }
      } else if (capture->length >= target_length) {
        return 1;
      }
    }
  }
}

static const char *status_reason(int status) {
  switch (status) {
  case 200:
    return "OK";
  case 201:
    return "Created";
  case 204:
    return "No Content";
  case 400:
    return "Bad Request";
  case 500:
    return "Internal Server Error";
  default:
    return "OK";
  }
}

static int write_http_response(SSL *ssl, const https_expectation *expectation) {
  char response[16384];
  char chunk_header[64];
  int written;
  size_t body_length;
  size_t offset;
  size_t index;
  int chunked;

  body_length = expectation->response_body != NULL
                    ? strlen(expectation->response_body)
                    : 0U;
  chunked = expectation->response_trailer_count > 0U;
  written = snprintf(response, sizeof(response),
                     "HTTP/1.1 %d %s\r\n"
                     "Connection: close\r\n",
                     expectation->response_status,
                     status_reason(expectation->response_status));
  if (written < 0 || (size_t)written >= sizeof(response)) {
    return 0;
  }
  offset = (size_t)written;
  if (chunked) {
    written = snprintf(response + offset, sizeof(response) - offset,
                       "Transfer-Encoding: chunked\r\n");
  } else {
    written = snprintf(response + offset, sizeof(response) - offset,
                       "Content-Length: %zu\r\n", body_length);
  }
  if (written < 0 || (size_t)written >= sizeof(response) - offset) {
    return 0;
  }
  offset += (size_t)written;
  for (index = 0U; index < expectation->response_header_count; ++index) {
    written = snprintf(response + offset, sizeof(response) - offset, "%s\r\n",
                       expectation->response_headers[index]);
    if (written < 0 || (size_t)written >= sizeof(response) - offset) {
      return 0;
    }
    offset += (size_t)written;
  }
  if (offset + 2U > sizeof(response)) {
    return 0;
  }
  memcpy(response + offset, "\r\n", 2U);
  offset += 2U;
  if (SSL_write(ssl, response, (int)offset) != (int)offset) {
    return 0;
  }
  if (chunked) {
    size_t body_offset;
    size_t chunk_size;

    body_offset = 0U;
    while (body_offset < body_length) {
      chunk_size = body_length - body_offset;
      if (expectation->response_body_chunk_size > 0U &&
          chunk_size > expectation->response_body_chunk_size) {
        chunk_size = expectation->response_body_chunk_size;
      }
      written =
          snprintf(chunk_header, sizeof(chunk_header), "%zx\r\n", chunk_size);
      if (written < 0 || (size_t)written >= sizeof(chunk_header)) {
        return 0;
      }
      if (SSL_write(ssl, chunk_header, written) != written ||
          SSL_write(ssl, expectation->response_body + body_offset,
                    (int)chunk_size) != (int)chunk_size ||
          SSL_write(ssl, "\r\n", 2) != 2) {
        return 0;
      }
      body_offset += chunk_size;
    }
    if (SSL_write(ssl, "0\r\n", 3) != 3) {
      return 0;
    }
    for (index = 0U; index < expectation->response_trailer_count; ++index) {
      written = snprintf(response, sizeof(response), "%s\r\n",
                         expectation->response_trailers[index]);
      if (written < 0 || (size_t)written >= sizeof(response)) {
        return 0;
      }
      if (SSL_write(ssl, response, written) != written) {
        return 0;
      }
    }
    return SSL_write(ssl, "\r\n", 2) == 2;
  }
  if (body_length > 0U) {
    size_t body_offset;
    size_t chunk_size;

    if (expectation->response_body_chunk_size == 0U) {
      return SSL_write(ssl, expectation->response_body, (int)body_length) ==
             (int)body_length;
    }
    body_offset = 0U;
    while (body_offset < body_length) {
      chunk_size = body_length - body_offset;
      if (chunk_size > expectation->response_body_chunk_size) {
        chunk_size = expectation->response_body_chunk_size;
      }
      if (SSL_write(ssl, expectation->response_body + body_offset,
                    (int)chunk_size) != (int)chunk_size) {
        return 0;
      }
      body_offset += chunk_size;
    }
  }
  return 1;
}

static int extract_common_name(X509 *cert, char *buffer, size_t buffer_size) {
  X509_NAME *subject;
  int len;

  subject = X509_get_subject_name(cert);
  if (subject == NULL) {
    return 0;
  }
  len = X509_NAME_get_text_by_NID(subject, NID_commonName, buffer,
                                  (int)buffer_size);
  return len >= 0;
}

static void verify_expectation(https_testserver *server, size_t index,
                               const https_expectation *expectation,
                               const test_request_capture *capture, SSL *ssl) {
  const char *request_line_end;
  const char *space;
  const char *path_end;
  const char *headers;
  const char *body;
  const char *body_view;
  char *decoded_body;
  size_t body_length;
  char method[32];
  char path[1024];
  size_t method_length;
  size_t path_length;
  size_t header_index;

  request_line_end = strstr(capture->data, "\r\n");
  if (request_line_end == NULL) {
    set_failure(server, "request missing request line");
    return;
  }
  space =
      memchr(capture->data, ' ', (size_t)(request_line_end - capture->data));
  if (space == NULL) {
    set_failure(server, "request line missing first space");
    return;
  }
  path_end = memchr(space + 1, ' ', (size_t)(request_line_end - (space + 1)));
  if (path_end == NULL) {
    set_failure(server, "request line missing second space");
    return;
  }

  method_length = (size_t)(space - capture->data);
  if (method_length >= sizeof(method)) {
    method_length = sizeof(method) - 1U;
  }
  memcpy(method, capture->data, method_length);
  method[method_length] = '\0';

  path_length = (size_t)(path_end - (space + 1));
  if (path_length >= sizeof(path)) {
    path_length = sizeof(path) - 1U;
  }
  memcpy(path, space + 1, path_length);
  path[path_length] = '\0';

  if (strcmp(method, expectation->method) != 0) {
    set_failure(server, "request[%zu] expected method %s, got %s", index,
                expectation->method, method);
    return;
  }
  if (strcmp(path, expectation->path) != 0) {
    set_failure(server, "request[%zu] expected path %s, got %s", index,
                expectation->path, path);
    return;
  }

  headers = request_line_end + 2;
  body = strstr(headers, "\r\n\r\n");
  if (body == NULL) {
    set_failure(server, "request missing header terminator");
    return;
  }
  body += 4;
  body_view = body;
  decoded_body = NULL;
  body_length = capture->length - (size_t)(body - capture->data);
  if (request_uses_chunked_transfer_encoding(headers)) {
    if (!decode_chunked_request_body(body, body_length, &decoded_body)) {
      set_failure(server, "failed to decode chunked request body");
      return;
    }
    body_view = decoded_body;
  }

  for (header_index = 0U; header_index < expectation->required_header_count;
       ++header_index) {
    if (strstr(headers, expectation->required_headers[header_index]) == NULL) {
      set_failure(server, "missing required header substring: %s",
                  expectation->required_headers[header_index]);
      free(decoded_body);
      return;
    }
  }
  if (expectation->forbidden_header != NULL &&
      strstr(headers, expectation->forbidden_header) != NULL) {
    set_failure(server, "unexpected header substring: %s",
                expectation->forbidden_header);
    free(decoded_body);
    return;
  }
  if (expectation->expect_empty_body &&
      (body_view != NULL && body_view[0] != '\0')) {
    set_failure(server, "expected empty body, got %s",
                body_view != NULL ? body_view : "(null)");
    free(decoded_body);
    return;
  }
  for (header_index = 0U;
       header_index < expectation->required_body_substring_count;
       ++header_index) {
    if (strstr(body_view,
               expectation->required_body_substrings[header_index]) == NULL) {
      set_failure(server, "missing required body substring: %s body=%s",
                  expectation->required_body_substrings[header_index],
                  body_view);
      free(decoded_body);
      return;
    }
  }
  free(decoded_body);
  if (expectation->expected_client_cn != NULL) {
    X509 *peer_cert;
    char common_name[256];

    peer_cert = SSL_get_peer_certificate(ssl);
    if (peer_cert == NULL) {
      set_failure(server, "server did not receive a client certificate");
      return;
    }
    memset(common_name, 0, sizeof(common_name));
    if (!extract_common_name(peer_cert, common_name, sizeof(common_name)) ||
        strcmp(common_name, expectation->expected_client_cn) != 0) {
      X509_free(peer_cert);
      set_failure(server, "expected client common name %s, got %s",
                  expectation->expected_client_cn, common_name);
      return;
    }
    X509_free(peer_cert);
  }
}

static void *https_testserver_main(void *context) {
  https_testserver *server;
  size_t index;

  server = (https_testserver *)context;
  for (index = 0U; index < server->expectation_count; ++index) {
    const https_expectation *expectation;
    struct sockaddr_in addr;
    socklen_t addr_len;
    int client_fd;
    SSL *ssl;
    test_request_capture capture;

    expectation = &server->expectations[index];
    addr_len = (socklen_t)sizeof(addr);
    client_fd =
        accept(server->listener_fd, (struct sockaddr *)&addr, &addr_len);
    if (client_fd < 0) {
      if (server->stop_requested)
        break;
      set_failure(server, "accept failed: %s", strerror(errno));
      break;
    }

    ssl = SSL_new(server->ssl_ctx);
    if (ssl == NULL) {
      close(client_fd);
      set_failure(server, "SSL_new failed");
      break;
    }
    SSL_set_fd(ssl, client_fd);
    if (SSL_accept(ssl) <= 0) {
      SSL_free(ssl);
      close(client_fd);
      set_failure(server, "SSL_accept failed");
      break;
    }

    if (!read_http_request(ssl, &capture)) {
      buffer_cleanup(&capture);
      SSL_free(ssl);
      close(client_fd);
      set_failure(server, "failed to read HTTP request");
      break;
    }
    verify_expectation(server, index, expectation, &capture, ssl);
    if (server->response_delay_ms > 0L) {
      struct timespec delay;

      delay.tv_sec = server->response_delay_ms / 1000L;
      delay.tv_nsec = (server->response_delay_ms % 1000L) * 1000000L;
      (void)nanosleep(&delay, NULL);
    }
    if (!write_http_response(ssl, expectation) &&
        !server->allow_response_write_failure) {
      set_failure(server, "failed to write HTTP response");
    }
    buffer_cleanup(&capture);
    SSL_free(ssl);
    close(client_fd);
    server->handled_count = index + 1U;
    if (server->failure_message[0] != '\0') {
      break;
    }
  }
  close(server->listener_fd);
  server->listener_fd = -1;
  return NULL;
}

static X509 *read_cert_pem(const char *pem) {
  BIO *bio;
  X509 *cert;

  bio = BIO_new_mem_buf(pem, -1);
  if (bio == NULL) {
    return NULL;
  }
  cert = PEM_read_bio_X509(bio, NULL, NULL, NULL);
  BIO_free(bio);
  return cert;
}

static EVP_PKEY *read_key_pem(const char *pem) {
  BIO *bio;
  EVP_PKEY *key;

  bio = BIO_new_mem_buf(pem, -1);
  if (bio == NULL) {
    return NULL;
  }
  key = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
  BIO_free(bio);
  return key;
}

static int write_client_bundle_file(const char *path, int include_ca) {
  FILE *file;
  const char *parts[3];
  size_t i;

  file = fopen(path, "wb");
  if (file == NULL) {
    return 0;
  }
  parts[0] = include_ca ? test_ca_cert_pem : "";
  parts[1] = test_client_cert_pem;
  parts[2] = test_client_key_pem;
  for (i = 0U; i < sizeof(parts) / sizeof(parts[0]); ++i) {
    size_t length;

    length = strlen(parts[i]);
    if (length > 0U && fwrite(parts[i], 1U, length, file) != length) {
      fclose(file);
      return 0;
    }
  }
  fclose(file);
  return 1;
}

static int read_file_bytes(const char *path, unsigned char **out,
                           size_t *out_length) {
  FILE *file;
  long length;
  unsigned char *bytes;

  file = fopen(path, "rb");
  if (file == NULL) {
    return 0;
  }
  if (fseek(file, 0L, SEEK_END) != 0) {
    fclose(file);
    return 0;
  }
  length = ftell(file);
  if (length < 0L || fseek(file, 0L, SEEK_SET) != 0) {
    fclose(file);
    return 0;
  }
  bytes = (unsigned char *)malloc((size_t)length);
  if (bytes == NULL && length > 0L) {
    fclose(file);
    return 0;
  }
  if (length > 0L && fread(bytes, 1U, (size_t)length, file) != (size_t)length) {
    free(bytes);
    fclose(file);
    return 0;
  }
  fclose(file);
  *out = bytes;
  *out_length = (size_t)length;
  return 1;
}

static void https_cleanup_stale_transport_roots(void) {
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-transport-",
                            HTTPS_TRANSPORT_TMP_PREFIX);
}

static int https_tls_material_init_shared(void) {
  char template_path[] = "/tmp/liblockdc-transport-XXXXXX";
  https_tls_material *material;

  if (shared_tls_material_initialized) {
    return 1;
  }
  https_cleanup_stale_transport_roots();
  material = &shared_tls_material;
  memset(material, 0, sizeof(*material));
  if (!lc_test_tmp_mkdtemp(template_path, material->temp_dir,
                           sizeof(material->temp_dir),
                           HTTPS_TRANSPORT_TMP_PREFIX)) {
    return 0;
  }
  if (snprintf(material->client_bundle_path,
               sizeof(material->client_bundle_path), "%s/client-bundle.pem",
               template_path) >= (int)sizeof(material->client_bundle_path) ||
      snprintf(material->client_bundle_without_ca_path,
               sizeof(material->client_bundle_without_ca_path),
               "%s/client-bundle-no-ca.pem", template_path) >=
          (int)sizeof(material->client_bundle_without_ca_path)) {
    return 0;
  }

  material->ca_cert = read_cert_pem(test_ca_cert_pem);
  material->server_cert = read_cert_pem(test_server_cert_pem);
  material->server_key = read_key_pem(test_server_key_pem);
  if (material->ca_cert == NULL || material->server_cert == NULL ||
      material->server_key == NULL) {
    return 0;
  }

  if (!write_client_bundle_file(material->client_bundle_path, 1) ||
      !write_client_bundle_file(material->client_bundle_without_ca_path, 0)) {
    return 0;
  }

  shared_tls_material_initialized = 1;
  return 1;
}

static int https_tls_material_init(https_tls_material *material,
                                   int include_ca_in_bundle) {
  if (material == NULL) {
    return 0;
  }
  if (!https_tls_material_init_shared()) {
    return 0;
  }
  memset(material, 0, sizeof(*material));
  if (snprintf(material->temp_dir, sizeof(material->temp_dir), "%s",
               shared_tls_material.temp_dir) >=
          (int)sizeof(material->temp_dir) ||
      snprintf(material->client_bundle_path,
               sizeof(material->client_bundle_path), "%s",
               include_ca_in_bundle
                   ? shared_tls_material.client_bundle_path
                   : shared_tls_material.client_bundle_without_ca_path) >=
          (int)sizeof(material->client_bundle_path) ||
      snprintf(material->client_bundle_without_ca_path,
               sizeof(material->client_bundle_without_ca_path), "%s",
               shared_tls_material.client_bundle_without_ca_path) >=
          (int)sizeof(material->client_bundle_without_ca_path)) {
    return 0;
  }
  material->ca_cert = shared_tls_material.ca_cert;
  material->ca_key = shared_tls_material.ca_key;
  material->server_cert = shared_tls_material.server_cert;
  material->server_key = shared_tls_material.server_key;
  material->uses_shared_state = 1;
  return 1;
}

static void unlink_if_exists(const char *path) {
  if (path[0] != '\0') {
    unlink(path);
  }
}

static void https_tls_material_cleanup(https_tls_material *material) {
  if (material->uses_shared_state) {
    memset(material, 0, sizeof(*material));
    return;
  }
  unlink_if_exists(material->client_bundle_path);
  unlink_if_exists(material->client_bundle_without_ca_path);
  if (material->temp_dir[0] != '\0') {
    lc_test_tmp_cleanup_path(material->temp_dir, HTTPS_TRANSPORT_TMP_PREFIX);
  }
  X509_free(material->ca_cert);
  EVP_PKEY_free(material->ca_key);
  X509_free(material->server_cert);
  EVP_PKEY_free(material->server_key);
  memset(material, 0, sizeof(*material));
}

static int https_tls_material_teardown_shared(void **state) {
  (void)state;
  if (!shared_tls_material_initialized) {
    return 0;
  }
  https_tls_material_cleanup(&shared_tls_material);
  shared_tls_material_initialized = 0;
  return 0;
}

static int https_tls_material_setup_shared(void **state) {
  (void)state;
  return https_tls_material_init_shared() ? 0 : -1;
}

static int https_testserver_start(https_testserver *server,
                                  const https_tls_material *material,
                                  const https_expectation *expectations,
                                  size_t expectation_count) {
  struct sockaddr_in addr;
  socklen_t addr_len;
  int fd;

  memset(server, 0, sizeof(*server));
  server->listener_fd = -1;
  server->expectations = expectations;
  server->expectation_count = expectation_count;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  {
    int yes;

    yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
      listen(fd, (int)expectation_count) != 0) {
    close(fd);
    return 0;
  }
  addr_len = (socklen_t)sizeof(addr);
  if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0) {
    close(fd);
    return 0;
  }

  server->ssl_ctx = SSL_CTX_new(TLS_server_method());
  if (server->ssl_ctx == NULL) {
    close(fd);
    return 0;
  }
  if (SSL_CTX_use_certificate(server->ssl_ctx, material->server_cert) != 1 ||
      SSL_CTX_use_PrivateKey(server->ssl_ctx, material->server_key) != 1 ||
      SSL_CTX_check_private_key(server->ssl_ctx) != 1) {
    SSL_CTX_free(server->ssl_ctx);
    close(fd);
    return 0;
  }
  if (X509_STORE_add_cert(SSL_CTX_get_cert_store(server->ssl_ctx),
                          material->ca_cert) != 1) {
    ERR_clear_error();
  }
  SSL_CTX_set_verify(server->ssl_ctx,
                     SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);

  server->listener_fd = fd;
  server->port = ntohs(addr.sin_port);
  if (pthread_create(&server->thread, NULL, https_testserver_main, server) !=
      0) {
    SSL_CTX_free(server->ssl_ctx);
    close(fd);
    server->listener_fd = -1;
    return 0;
  }
  return 1;
}

static void https_testserver_stop(https_testserver *server) {
  if (server->listener_fd >= 0 || server->expectation_count > 0U) {
    pthread_join(server->thread, NULL);
  }
  if (server->ssl_ctx != NULL) {
    SSL_CTX_free(server->ssl_ctx);
  }
}

/* A deadline test can intentionally stop before the fixture's next expected
 * connection. Wake its blocking accept without classifying that designed early
 * stop as a server failure. */
static void https_testserver_stop_early(https_testserver *server) {
  if (server->listener_fd >= 0) {
    server->stop_requested = 1;
    (void)shutdown(server->listener_fd, SHUT_RDWR);
  }
  https_testserver_stop(server);
}

static void init_client_config(lc_engine_client_config *config,
                               unsigned short port, const char *bundle_path) {
  static char endpoint[128];
  static const char *endpoints[1];

  lc_engine_client_config_init(config);
  snprintf(endpoint, sizeof(endpoint), "https://127.0.0.1:%u", (unsigned)port);
  endpoints[0] = endpoint;
  config->endpoints = endpoints;
  config->endpoint_count = 1U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
}

static void init_client_config_two_endpoints(lc_engine_client_config *config,
                                             unsigned short port,
                                             const char *bundle_path) {
  static char endpoint_a[128];
  static char endpoint_b[128];
  static const char *endpoints[2];

  lc_engine_client_config_init(config);
  snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
           (unsigned)port);
  snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
           (unsigned)port);
  endpoints[0] = endpoint_a;
  endpoints[1] = endpoint_b;
  config->endpoints = endpoints;
  config->endpoint_count = 2U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
}

static void init_public_client_config(lc_client_config *config,
                                      unsigned short port,
                                      const char *bundle_path,
                                      pslog_logger *logger) {
  static char endpoint[128];
  static const char *endpoints[1];

  lc_client_config_init(config);
  snprintf(endpoint, sizeof(endpoint), "https://127.0.0.1:%u", (unsigned)port);
  endpoints[0] = endpoint;
  config->endpoints = endpoints;
  config->endpoint_count = 1U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
  config->logger = logger;
}

static pslog_logger *open_test_logger(FILE **out_fp) {
  FILE *fp;
  pslog_config config;

  fp = tmpfile();
  if (fp == NULL) {
    return NULL;
  }
  pslog_default_config(&config);
  config.mode = PSLOG_MODE_JSON;
  config.min_level = PSLOG_LEVEL_TRACE;
  config.timestamps = 0;
  config.verbose_fields = 1;
  config.output = pslog_output_from_fp(fp, 0);
  *out_fp = fp;
  return pslog_new(&config);
}

static char *read_stream_text(FILE *fp) {
  long length;
  char *buffer;

  if (fp == NULL || fflush(fp) != 0 || fseek(fp, 0L, SEEK_END) != 0) {
    return NULL;
  }
  length = ftell(fp);
  if (length < 0L || fseek(fp, 0L, SEEK_SET) != 0) {
    return NULL;
  }
  buffer = (char *)calloc((size_t)length + 1U, 1U);
  if (buffer == NULL) {
    return NULL;
  }
  if (length > 0L && fread(buffer, 1U, (size_t)length, fp) != (size_t)length) {
    free(buffer);
    return NULL;
  }
  buffer[length] = '\0';
  return buffer;
}

static void assert_server_ok(https_testserver *server) {
  assert_int_equal(server->handled_count, server->expectation_count);
  if (server->failure_message[0] != '\0') {
    fail_msg("%s", server->failure_message);
  }
  assert_true(server->failure_message[0] == '\0');
}

static int capture_delivery_begin(void *context,
                                  const lc_engine_dequeue_response *delivery,
                                  lc_engine_error *error) {
  subscribe_capture *capture;

  (void)error;
  capture = (subscribe_capture *)context;
  capture->begin_calls += 1;
  if (delivery->message_id != NULL) {
    snprintf(capture->message_id, sizeof(capture->message_id), "%s",
             delivery->message_id);
  }
  return 1;
}

static int capture_delivery_chunk(void *context, const void *bytes,
                                  size_t count, lc_engine_error *error) {
  subscribe_capture *capture;
  size_t copy_count;

  (void)error;
  capture = (subscribe_capture *)context;
  copy_count = count;
  if (copy_count > sizeof(capture->payload) - capture->payload_length - 1U) {
    copy_count = sizeof(capture->payload) - capture->payload_length - 1U;
  }
  memcpy(capture->payload + capture->payload_length, bytes, copy_count);
  capture->payload_length += copy_count;
  capture->payload[capture->payload_length] = '\0';
  return 1;
}

static int fail_delivery_chunk(void *context, const void *bytes, size_t count,
                               lc_engine_error *error) {
  (void)context;
  (void)bytes;
  (void)count;
  error->code = LC_ENGINE_ERROR_TRANSPORT;
  error->message = strdup("test payload chunk failure");
  return 0;
}

static int test_cancel_check(void *context) {
  canceling_subscribe_capture *capture;

  capture = (canceling_subscribe_capture *)context;
  return capture != NULL && capture->cancel_requested;
}

static int test_delayed_cancel_check(void *context) {
  delayed_cancel_check *cancel = (delayed_cancel_check *)context;
  int requested;

  pthread_mutex_lock(&cancel->mutex);
  requested = cancel->requested;
  pthread_mutex_unlock(&cancel->mutex);
  return requested;
}

static void *test_request_cancel_after_delay(void *context) {
  delayed_cancel_check *cancel = (delayed_cancel_check *)context;
  struct timespec delay;

  delay.tv_sec = 0;
  delay.tv_nsec = 100000000L;
  (void)nanosleep(&delay, NULL);
  pthread_mutex_lock(&cancel->mutex);
  cancel->requested = 1;
  pthread_mutex_unlock(&cancel->mutex);
  return NULL;
}

static void test_state_transport_honors_client_cancel(void **state) {
  static const char *const headers[] = {"Content-Type: application/json"};
  static const char *const body[] = {"\"namespace\":\"cancel-test\"",
                                     "\"key\":\"key\"", "\"owner\":\"owner\""};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", headers, 1U, body, 3U, 0, 200, NULL, 0U, "{}",
       "liblockdc test client"}};
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_acquire_request request;
  lc_engine_acquire_response response;
  lc_engine_error error;
  https_tls_material material;
  https_testserver server;
  delayed_cancel_check cancel;
  pthread_t cancel_thread;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(https_testserver_start(&server, &material, expectations, 1U));
  server.response_delay_ms = 2000L;
  server.allow_response_write_failure = 1;
  init_client_config(&config, server.port, material.client_bundle_path);
  client = NULL;
  lc_engine_error_init(&error);
  memset(&request, 0, sizeof(request));
  memset(&response, 0, sizeof(response));
  memset(&cancel, 0, sizeof(cancel));
  assert_int_equal(pthread_mutex_init(&cancel.mutex, NULL), 0);
  assert_int_equal(lc_engine_client_open(&config, &client, &error),
                   LC_ENGINE_OK);
  lc_engine_client_set_cancel_check(client, test_delayed_cancel_check, &cancel);
  request.ns = "cancel-test";
  request.key = "key";
  request.owner = "owner";
  request.ttl_seconds = 30L;
  assert_int_equal(pthread_create(&cancel_thread, NULL,
                                  test_request_cancel_after_delay, &cancel),
                   0);
  rc = lc_engine_client_acquire(client, &request, &response, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_TRANSPORT);
  assert_string_equal(error.message, "request cancelled");
  assert_int_equal(pthread_join(cancel_thread, NULL), 0);
  lc_engine_acquire_response_cleanup(&response);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  pthread_mutex_destroy(&cancel.mutex);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static int cancel_after_delivery_chunk(void *context, const void *bytes,
                                       size_t count, lc_engine_error *error) {
  canceling_subscribe_capture *capture;

  capture = (canceling_subscribe_capture *)context;
  assert_non_null(capture);
  assert_true(capture_delivery_chunk(&capture->capture, bytes, count, error));
  if (capture->capture.payload_length >= 11U) {
    capture->cancel_requested = 1;
  }
  return 1;
}

static int fail_and_cancel_delivery_chunk(void *context, const void *bytes,
                                          size_t count,
                                          lc_engine_error *error) {
  canceling_subscribe_capture *capture;

  capture = (canceling_subscribe_capture *)context;
  assert_non_null(capture);
  capture->cancel_requested = 1;
  return fail_delivery_chunk(&capture->capture, bytes, count, error);
}

static int capture_delivery_end(void *context,
                                const lc_engine_dequeue_response *delivery,
                                lc_engine_error *error) {
  subscribe_capture *capture;

  (void)delivery;
  (void)error;
  capture = (subscribe_capture *)context;
  capture->end_calls += 1;
  return 1;
}

typedef struct chunked_bundle_source {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
  size_t chunk_size;
} chunked_bundle_source;

static size_t chunked_bundle_read(void *context, void *buffer, size_t count,
                                  lc_error *error) {
  chunked_bundle_source *source;
  size_t chunk;

  (void)error;
  source = (chunked_bundle_source *)context;
  if (source->offset >= source->length) {
    return 0U;
  }
  chunk = source->length - source->offset;
  if (chunk > source->chunk_size) {
    chunk = source->chunk_size;
  }
  if (chunk > count) {
    chunk = count;
  }
  memcpy(buffer, source->bytes + source->offset, chunk);
  source->offset += chunk;
  return chunk;
}

static size_t failing_bundle_read(void *context, void *buffer, size_t count,
                                  lc_error *error) {
  (void)context;
  (void)buffer;
  (void)count;
  lc_error_set(error, LC_ERR_TRANSPORT, 0L, "synthetic bundle source failure",
               NULL, NULL, NULL);
  return 0U;
}

static void test_client_open_rejects_bundle_without_ca(void **state) {
  https_tls_material material;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 0));
  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lc_engine_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_path = material.client_bundle_path;
  }
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_null(client);
  assert_non_null(error.message);
  assert_non_null(
      strstr(error.message, "client bundle does not contain a CA certificate"));
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_client_open_accepts_memory_bundle_source(void **state) {
  https_tls_material material;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_error source_error;
  lc_source *source;
  unsigned char *bytes;
  size_t length;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  memset(&error, 0, sizeof(error));
  memset(&source_error, 0, sizeof(source_error));
  source = NULL;
  client = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(read_file_bytes(material.client_bundle_path, &bytes, &length));
  rc = lc_source_from_memory(bytes, length, &source, &source_error);
  assert_int_equal(rc, LC_OK);
  memset(&config, 0, sizeof(config));
  lc_engine_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_source = source;
    config.client_bundle_path = "/definitely/missing/liblockdc-client.pem";
  }
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);
  lc_engine_client_close(client);
  lc_source_close(source);
  lc_error_cleanup(&source_error);
  lc_engine_error_cleanup(&error);
  free(bytes);
  https_tls_material_cleanup(&material);
}

static void
test_client_open_propagates_callback_bundle_source_failure(void **state) {
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_error source_error;
  lc_source *source;
  int rc;

  (void)state;
  memset(&error, 0, sizeof(error));
  memset(&source_error, 0, sizeof(source_error));
  source = NULL;
  client = NULL;
  rc = lc_source_from_callbacks(failing_bundle_read, NULL, NULL, NULL, &source,
                                &source_error);
  assert_int_equal(rc, LC_OK);
  memset(&config, 0, sizeof(config));
  lc_engine_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_source = source;
  }
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_TRANSPORT);
  assert_null(client);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "synthetic bundle source failure"));
  lc_source_close(source);
  lc_error_cleanup(&source_error);
  lc_engine_error_cleanup(&error);
}

static void test_client_open_accepts_fd_bundle_source(void **state) {
  https_tls_material material;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_error source_error;
  lc_source *source;
  FILE *file;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  memset(&error, 0, sizeof(error));
  memset(&source_error, 0, sizeof(source_error));
  source = NULL;
  client = NULL;
  file = fopen(material.client_bundle_path, "rb");
  assert_non_null(file);
  rc = lc_source_from_fd(fileno(file), &source, &source_error);
  assert_int_equal(rc, LC_OK);
  memset(&config, 0, sizeof(config));
  lc_engine_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_source = source;
  }
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);
  lc_engine_client_close(client);
  lc_source_close(source);
  fclose(file);
  lc_error_cleanup(&source_error);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_client_open_accepts_chunked_callback_bundle_source(void **state) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  lc_source *source;
  chunked_bundle_source chunked;
  unsigned char *bytes;
  size_t length;
  int rc;

  (void)state;
  assert_true(https_tls_material_init_shared());
  memset(&error, 0, sizeof(error));
  source = NULL;
  client = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(
      read_file_bytes(shared_tls_material.client_bundle_path, &bytes, &length));
  memset(&chunked, 0, sizeof(chunked));
  chunked.bytes = bytes;
  chunked.length = length;
  chunked.chunk_size = 7U;
  rc = lc_source_from_callbacks(chunked_bundle_read, NULL, NULL, &chunked,
                                &source, &error);
  assert_int_equal(rc, LC_OK);
  lc_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_source = source;
  }
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  lc_client_close(client);
  lc_source_close(source);
  lc_error_cleanup(&error);
  free(bytes);
}

static void test_state_transport_paths_use_mtls(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\"",
      "\"txn_id\":\"txn-acquire\""};
  static const char *corr_acquire[] = {"X-Correlation-Id: corr-acquire",
                                       "Content-Type: application/json",
                                       "X-Key-Version: 2147483648"};
  static const char *corr_get[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200, corr_acquire,
       sizeof(corr_acquire) / sizeof(corr_acquire[0]),
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, corr_get, sizeof(corr_get) / sizeof(corr_get[0]),
       "{\"value\":1}", "liblockdc test client"},
  };
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_acquire_request acquire_req;
  lc_engine_acquire_response acquire_res;
  lc_engine_get_request get_req;
  lc_engine_get_response get_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);

  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&acquire_res, 0, sizeof(acquire_res));
  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  acquire_req.block_seconds = 5L;
  acquire_req.if_not_exists = 1;
  acquire_req.txn_id = "txn-acquire";
  rc = lc_engine_client_acquire(client, &acquire_req, &acquire_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(acquire_res.correlation_id, "corr-acquire");
  assert_int_equal(acquire_res.fencing_token, 11L);

  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));
  get_req.ns = "transport-ns";
  get_req.key = "resource/1";
  get_req.public_read = 1;
  rc = lc_engine_client_get(client, &get_req, &get_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_memory_equal(get_res.body, "{\"value\":1}", strlen("{\"value\":1}"));
  assert_string_equal(get_res.etag, "etag-1");
  assert_int_equal(get_res.version, 4L);
  assert_int_equal(get_res.fencing_token, 11L);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_acquire_response_cleanup(&acquire_res);
  lc_engine_get_response_cleanup(&get_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_client_namespaced_load_rejects_empty_namespace(void **state) {
  static const char *endpoints[] = {"https://127.0.0.1:1"};
  lc_client_config config;
  lc_client *client;
  lc_get_res get_res;
  lc_error error;
  test_value_doc value_doc;
  int rc;

  (void)state;
  client = NULL;
  memset(&get_res, 0, sizeof(get_res));
  memset(&value_doc, 0, sizeof(value_doc));
  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  config.prefer_http_2 = 0;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  rc = lc_get_in_namespace(client, NULL, "resource/1", NULL, NULL, &get_res,
                           &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "namespaced get requires client and namespace");
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = client->get_in_namespace(client, "", "resource/1", NULL, NULL, &get_res,
                                &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "namespaced get requires a non-empty namespace");
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = client->load_in_namespace(client, NULL, "resource/1", &test_value_map,
                                 &value_doc, NULL, &get_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "namespaced load requires a non-empty namespace");
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = client->load_in_namespace(client, "", "resource/1", &test_value_map,
                                 &value_doc, NULL, &get_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "namespaced load requires a non-empty namespace");

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_state_transport_parses_buffered_typed_json_response(void **state) {
  static const char *response_headers[] = {"X-Correlation-Id: corr-describe",
                                           "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/describe?key=resource%2F1&namespace=transport-ns", NULL, 0U,
       NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"expires_at_unix\":1000,\"version\":4,"
       "\"state_etag\":\"etag-1\",\"updated_at_unix\":1001,"
       "\"metadata\":{\"query_hidden\":true}}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_describe_request req;
  lc_engine_describe_response res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.key = "resource/1";
  rc = lc_engine_client_describe(client, &req, &res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(res.ns, "transport-ns");
  assert_string_equal(res.key, "resource/1");
  assert_string_equal(res.owner, "owner-a");
  assert_string_equal(res.lease_id, "lease-1");
  assert_int_equal(res.expires_at_unix, 1000L);
  assert_int_equal(res.version, 4L);
  assert_string_equal(res.state_etag, "etag-1");
  assert_int_equal(res.updated_at_unix, 1001L);
  assert_true(res.has_query_hidden);
  assert_true(res.query_hidden);
  assert_string_equal(res.correlation_id, "corr-describe");

  lc_engine_describe_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_management_transport_paths_use_mtls(void **state) {
  static const char *json_headers[] = {"Content-Type: application/json",
                                       "If-Match: \"config-etag\""};
  https_expectation expectations[] = {
      {"POST", "/v1/namespace", json_headers,
       sizeof(json_headers) / sizeof(json_headers[0]),
       (const char *const[]){"\"namespace\":\"team-a\"",
                             "\"preferred_engine\":\"index\"",
                             "\"fallback_engine\":\"scan\""},
       3U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-ns-put",
                             "ETag: \"config-next\"",
                             "Content-Type: application/json"},
       3U,
       "{\"namespace\":\"team-a\",\"query\":{\"preferred_engine\":\"index\","
       "\"fallback_engine\":\"scan\"}}",
       "liblockdc test client"},
  };
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_namespace_config_request ns_req;
  lc_engine_namespace_config_response ns_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&ns_req, 0, sizeof(ns_req));
  memset(&ns_res, 0, sizeof(ns_res));
  ns_req.ns = "team-a";
  ns_req.preferred_engine = "index";
  ns_req.fallback_engine = "scan";
  ns_req.if_etag = "\"config-etag\"";
  rc = lc_engine_client_update_namespace_config(client, &ns_req, &ns_res,
                                                &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(ns_res.preferred_engine, "index");
  assert_string_equal(ns_res.fallback_engine, "scan");
  assert_string_equal(ns_res.etag, "config-next");

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_namespace_config_response_cleanup(&ns_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_paths_use_mtls(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":1,\"pending_candidates\":2,"
       "\"total_consumers\":3,\"has_active_watcher\":true,"
       "\"available\":true,\"head_message_id\":\"msg-1\","
       "\"head_enqueued_at_unix\":100,\"head_not_visible_until_unix\":101,"
       "\"head_age_seconds\":2}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(stats_res.available);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_rejects_oversized_error_body(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  char *error_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 503, (const char *const[]){"Content-Type: application/json"}, 1U,
       NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  error_body = make_repeat_json_body("{\"error\":\"", "\"}", 9000U, 'x');
  assert_non_null(error_body);
  expectations[0].response_body = error_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "byte limit"));

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(error_body);
}

static void test_watch_transport_rejects_oversized_error_body(void **state) {
  static const char *watch_headers[] = {"Content-Type: application/json",
                                        "Accept: text/event-stream"};
  char *error_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch", watch_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 503, (const char *const[]){"Content-Type: application/json"}, 1U,
       NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_watch_queue_request watch_req;
  int rc;

  (void)state;
  error_body = make_repeat_json_body("{\"error\":\"", "\"}", 9000U, 'w');
  assert_non_null(error_body);
  expectations[0].response_body = error_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&watch_req, 0, sizeof(watch_req));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_event_sink, NULL,
                                    &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "watch_queue error body"));

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(error_body);
}

static void
test_queue_transport_rejects_overflowing_numeric_fields(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats-overflow",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":2147483648}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_watch_stream_filters_events_and_finishes_trailing_event(void **state) {
  static const char response_body[] =
      "event: queue_noise\r\n"
      "data: {not-json-but-skipped}\r\n"
      "\r\n"
      "event: queue_watch\r\n"
      "data: {\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"available\":true,\"head_message_id\":\"msg-1\","
      "\"changed_at_unix\":12345}";
  static const char *watch_headers[] = {"X-Correlation-Id: corr-watch-header",
                                        "Content-Type: text/event-stream"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch",
       (const char *const[]){"Accept: text/event-stream",
                             "Content-Type: application/json"},
       2U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, watch_headers,
       sizeof(watch_headers) / sizeof(watch_headers[0]), response_body,
       "liblockdc test client", 7U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_watch_queue_request watch_req;
  watch_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&watch_req, 0, sizeof(watch_req));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.ns = "transport-ns";
  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_capture_sink,
                                    &capture, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(capture.event_count, 1);
  assert_string_equal(capture.ns, "transport-ns");
  assert_string_equal(capture.queue, "jobs");
  assert_string_equal(capture.head_message_id, "msg-1");
  assert_string_equal(capture.correlation_id, "corr-watch-header");
  assert_int_equal(capture.available, 1);
  assert_int_equal(capture.changed_at_unix, 12345);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_watch_stream_rejects_malformed_selected_event(void **state) {
  static const char response_body[] =
      "event: queue_watch\r\n"
      "data: {\"namespace\":\"transport-ns\",\"queue\":\"jobs\",";
  static const char *watch_headers[] = {"X-Correlation-Id: corr-watch-bad",
                                        "Content-Type: text/event-stream"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch",
       (const char *const[]){"Accept: text/event-stream",
                             "Content-Type: application/json"},
       2U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, watch_headers,
       sizeof(watch_headers) / sizeof(watch_headers[0]), response_body,
       "liblockdc test client", 5U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_watch_queue_request watch_req;
  watch_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&watch_req, 0, sizeof(watch_req));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.ns = "transport-ns";
  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_capture_sink,
                                    &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(capture.event_count, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_watch_stream_rejects_oversized_line(void **state) {
  static const char *watch_headers[] = {
      "X-Correlation-Id: corr-watch-line-limit",
      "Content-Type: text/event-stream"};
  char *response_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch",
       (const char *const[]){"Accept: text/event-stream",
                             "Content-Type: application/json"},
       2U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, watch_headers,
       sizeof(watch_headers) / sizeof(watch_headers[0]), NULL,
       "liblockdc test client", 4096U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_watch_queue_request watch_req;
  watch_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  response_body = make_repeat_json_body(
      "event: queue_watch\r\n"
      "data: {\"namespace\":\"transport-ns\",\"queue\":\"",
      "\"}\r\n\r\n", 70000U, 'w');
  assert_non_null(response_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&watch_req, 0, sizeof(watch_req));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.ns = "transport-ns";
  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_capture_sink,
                                    &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.event_count, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_int_equal(server.handled_count, 1U);
  if (server.failure_message[0] != '\0') {
    assert_string_equal(server.failure_message,
                        "failed to write HTTP response");
  }
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void
test_watch_stream_rejects_oversized_event_data_after_prior_event(void **state) {
  static const char *watch_headers[] = {
      "X-Correlation-Id: corr-watch-event-data-limit",
      "Content-Type: text/event-stream"};
  char *response_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch",
       (const char *const[]){"Accept: text/event-stream",
                             "Content-Type: application/json"},
       2U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, watch_headers,
       sizeof(watch_headers) / sizeof(watch_headers[0]), NULL,
       "liblockdc test client", 0U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_watch_queue_request watch_req;
  watch_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  response_body = make_watch_body_with_oversized_event_data_after_valid_event();
  assert_non_null(response_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&watch_req, 0, sizeof(watch_req));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.ns = "transport-ns";
  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_capture_sink,
                                    &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.event_count, 1);
  assert_string_equal(capture.head_message_id, "msg-1");

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_int_equal(server.handled_count, 1U);
  if (server.failure_message[0] != '\0') {
    assert_string_equal(server.failure_message,
                        "failed to write HTTP response");
  }
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void
test_queue_transport_preserves_typed_json_parse_errors(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char malformed_body[] = "{";
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats-bad-json",
                             "Content-Type: application/json"},
       2U, malformed_body, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_respects_client_meta_limit(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *meta_body;
  char *response_body;
  size_t response_body_len;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  meta_body = make_repeat_json_body(
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"",
      "\"}", 128U, 'm');
  assert_non_null(meta_body);
  response_body_len = strlen(meta_body) + 256U;
  response_body = (char *)malloc(response_body_len);
  assert_non_null(response_body);
  snprintf(response_body, response_body_len,
           "--queue-boundary\r\n"
           "Content-Disposition: form-data; name=\"meta\"\r\n"
           "Content-Type: application/json\r\n"
           "Content-Length: %zu \r\n"
           "\r\n"
           "%s\r\n"
           "--queue-boundary--\r\n",
           strlen(meta_body), meta_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  config.http_json_response_limit_bytes = 64U;
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_non_null(error.message);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(meta_body);
  free(response_body);
}

static void test_subscribe_rejects_default_meta_overflow(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *meta_body;
  char *response_body;
  size_t response_body_len;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  meta_body = make_repeat_json_body(
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"",
      "\"}", 6000U, 'm');
  assert_non_null(meta_body);
  response_body_len = strlen(meta_body) + 256U;
  response_body = (char *)malloc(response_body_len);
  assert_non_null(response_body);
  snprintf(response_body, response_body_len,
           "--queue-boundary\r\n"
           "Content-Disposition: form-data; name=\"meta\"\r\n"
           "Content-Type: application/json\r\n"
           "Content-Length: %zu \r\n"
           "\r\n"
           "%s\r\n"
           "--queue-boundary--\r\n",
           strlen(meta_body), meta_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_int_equal(error.code, LC_ENGINE_ERROR_NO_MEMORY);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(meta_body);
  free(response_body);
}

static void
test_state_transport_accepts_numeric_headers_with_trailing_ows(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-get-ows", "Content-Type: application/json",
      "ETag: etag-2", "X-Key-Version: 2147483648 \t", "X-Fencing-Token: 11 "};
  https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_get_request req;
  lc_engine_get_response res;
  lc_engine_error error;
  lc_version expected_version;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  client = NULL;
  expected_version = (lc_version)2147483647L + 1;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.key = "resource/1";
  req.public_read = 1;
  rc = lc_engine_client_get(client, &req, &res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(res.version == expected_version);
  assert_int_equal(res.fencing_token, 11L);

  lc_engine_get_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_state_transport_rejects_invalid_numeric_headers_as_protocol(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-get-bad-header", "Content-Type: application/json",
      "X-Key-Version: not-a-number"};
  https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_get_request req;
  lc_engine_get_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.key = "resource/1";
  req.public_read = 1;
  rc = lc_engine_client_get(client, &req, &res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);

  lc_engine_get_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

typedef struct test_deadline_clock {
  unsigned int calls;
} test_deadline_clock;

static int test_deadline_clock_gettime(clockid_t clock_id, struct timespec *out,
                                       void *context) {
  test_deadline_clock *clock;

  if (clock_id != CLOCK_MONOTONIC || out == NULL || context == NULL) {
    return -1;
  }
  clock = (test_deadline_clock *)context;
  out->tv_sec = clock->calls++ == 0U ? 1000 : 1001;
  out->tv_nsec = 0L;
  return 0;
}

/* A bounded higher-level operation can use a one-shot transport clone. Its
 * absolute deadline must cover node-passive failover rather than resetting for
 * the second endpoint. The old per-attempt limit returned the 200 response. */
static void
test_engine_request_deadline_bounds_node_passive_failover(void **state) {
  static const char *json_headers[] = {"Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 503, json_headers, 1U, "{\"error\":\"node_passive\"}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, json_headers, 1U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_get_request request;
  lc_engine_get_response response;
  lc_engine_error error;
  struct timespec deadline;
  test_deadline_clock clock;
  int rc;

  (void)state;
  client = NULL;
  memset(&request, 0, sizeof(request));
  memset(&response, 0, sizeof(response));
  memset(&clock, 0, sizeof(clock));
  lc_engine_error_init(&error);
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  assert_int_equal(lc_engine_client_open(&config, &client, &error),
                   LC_ENGINE_OK);
  /* The first endpoint deterministically sees one second of budget and
   * returns node_passive. The second attempt observes the same absolute
   * deadline as elapsed, without relying on scheduler or TLS timing. */
  deadline.tv_sec = 1001;
  deadline.tv_nsec = 0L;
  lc_engine_client_set_request_deadline(client, &deadline);
  lc_transport_test_clock_gettime = test_deadline_clock_gettime;
  lc_transport_test_clock_context = &clock;
  request.key = "resource/1";
  request.public_read = 1;
  rc = lc_engine_client_get(client, &request, &response, &error);
  lc_transport_test_clock_gettime = NULL;
  lc_transport_test_clock_context = NULL;
  assert_int_equal(rc, LC_ENGINE_ERROR_TRANSPORT);
  assert_string_equal(error.message, "request deadline elapsed");
  assert_int_equal(clock.calls, 2U);

  lc_engine_get_response_cleanup(&response);
  lc_engine_client_close(client);
  https_testserver_stop_early(&server);
  assert_int_equal(server.handled_count, 1U);
  assert_true(server.failure_message[0] == '\0');
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_subscribe_accepts_content_length_with_trailing_ows(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192 \t\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11 \t\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 3U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(capture.begin_calls, 1);
  assert_int_equal(capture.end_calls, 1);
  assert_string_equal(capture.message_id, "msg-1");
  assert_string_equal(capture.payload, "{\"ok\":true}");

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_rejects_oversized_header_line(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe-header-limit",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *response_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client", 4096U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  response_body = make_repeat_json_body(
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"; x=\"",
      "\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 2\r\n"
      "\r\n"
      "{}\r\n"
      "--queue-boundary--\r\n",
      70000U, 'h');
  assert_non_null(response_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void test_subscribe_rejects_oversized_boundary(void **state) {
  char *content_type;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, NULL, 0U, "--queue-boundary--\r\n", "liblockdc test client",
       0U}};
  const char *response_headers[2];
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  content_type = make_repeat_json_body(
      "Content-Type: multipart/mixed; boundary=", "", 256U, 'b');
  assert_non_null(content_type);
  response_headers[0] = "X-Correlation-Id: corr-subscribe-boundary-limit";
  response_headers[1] = content_type;
  expectations[0].response_headers = response_headers;
  expectations[0].response_header_count = 2U;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(content_type);
}

static void test_subscribe_rejects_too_many_headers(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe-header-count-limit",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *response_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client", 0U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  response_body = make_subscribe_meta_with_extra_headers(64U);
  assert_non_null(response_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void
test_subscribe_rejects_oversized_header_after_completed_delivery(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe-combined-header-limit",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *response_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client", 0U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  response_body = make_subscribe_body_with_oversized_header_after_delivery();
  assert_non_null(response_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_int_equal(capture.begin_calls, 1);
  assert_int_equal(capture.end_calls, 1);
  assert_string_equal(capture.message_id, "msg-1");
  assert_string_equal(capture.payload, "{\"ok\":true}");

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void
test_subscribe_rejects_payload_without_content_length(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 2U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_rejects_missing_closing_boundary(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 4U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_propagates_payload_callback_failure(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 3U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = fail_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_TRANSPORT);
  assert_int_equal(error.code, LC_ENGINE_ERROR_TRANSPORT);
  assert_string_equal(error.message, "test payload chunk failure");
  assert_int_equal(capture.begin_calls, 1);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_subscribe_treats_cancelled_callback_failure_as_ok(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 3U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  canceling_subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  lc_engine_client_set_cancel_check(client, test_cancel_check, &capture);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = fail_and_cancel_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(error.code, LC_ENGINE_OK);
  assert_int_equal(capture.capture.begin_calls, 1);
  assert_int_equal(capture.capture.end_calls, 0);
  assert_int_equal(capture.cancel_requested, 1);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_treats_cancelled_finish_failure_as_ok(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 4U}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  canceling_subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  lc_engine_client_set_cancel_check(client, test_cancel_check, &capture);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = cancel_after_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(error.code, LC_ENGINE_OK);
  assert_int_equal(capture.capture.begin_calls, 1);
  assert_string_equal(capture.capture.payload, "{\"ok\":true}");
  assert_int_equal(capture.cancel_requested, 1);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_emits_pslog_messages(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-acquire",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req req;
  lc_lease *lease;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lease = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.key = "resource/1";
  req.owner = "owner-a";
  req.ttl_seconds = 30L;
  rc = client->acquire(client, &req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lease->close(lease);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"init\""));
  assert_non_null(strstr(logs, "\"message\":\"http.attempt\""));
  assert_non_null(strstr(logs, "\"message\":\"http.success\""));
  assert_non_null(strstr(logs, "\"message\":\"acquire.success\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_null(strstr(logs, "\"message\":\"client."));
  assert_null(strstr(logs, "\"component\":"));
  assert_null(strstr(logs, "\"subsystem\":"));
  assert_non_null(strstr(logs, "\"key\":\"resource/1\""));
  assert_non_null(strstr(logs, "\"lease_id\":\"lease-1\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-acquire\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_can_disable_sdk_sys_field(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-acquire",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req req;
  lc_lease *lease;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lease = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  config.disable_logger_sys_field = 1;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.key = "resource/1";
  req.owner = "owner-a";
  req.ttl_seconds = 30L;
  rc = client->acquire(client, &req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lease->close(lease);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"init\""));
  assert_non_null(strstr(logs, "\"message\":\"acquire.success\""));
  assert_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_null(strstr(logs, "\"sys\":\"client.sdk\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_bound_lease_methods_emit_logs(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *keepalive_request_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11"};
  static const char *keepalive_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"ttl_seconds\":45",
      "\"txn_id\":\"txn-acquire\""};
  static const char *keepalive_response_headers[] = {
      "X-Correlation-Id: corr-keepalive", "Content-Type: application/json"};
  static const char *release_request_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11"};
  static const char *release_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"txn_id\":\"txn-acquire\"",
      "\"rollback\":true"};
  static const char *release_response_headers[] = {
      "X-Correlation-Id: corr-release", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/keepalive", keepalive_request_headers, 2U, keepalive_body,
       5U, 0, 200, keepalive_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"lease_id\":\"lease-1\",\"txn_id\":\"txn-acquire\","
       "\"expires_at_unix\":2000,\"version\":5,\"state_etag\":\"etag-2\"}",
       "liblockdc test client"},
      {"POST", "/v1/release", release_request_headers, 2U, release_body, 5U, 0,
       200, release_response_headers, 2U, "{\"released\":true}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_keepalive_req keepalive_req;
  lc_release_req release_req;
  lc_error error;
  lc_lease *lease;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  lease = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&keepalive_req, 0, sizeof(keepalive_req));
  memset(&release_req, 0, sizeof(release_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  keepalive_req.ttl_seconds = 45L;
  rc = lc_lease_keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);

  release_req.rollback = 1;
  rc = lc_lease_release(lease, &release_req, &error);
  if (rc != LC_OK) {
    fail_msg("lease release rc=%d code=%d http_status=%ld message=%s "
             "detail=%s server=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  lease = NULL;

  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"keepalive.start\""));
  assert_non_null(strstr(logs, "\"message\":\"keepalive.success\""));
  assert_non_null(strstr(logs, "\"message\":\"release.start\""));
  assert_non_null(strstr(logs, "\"message\":\"release.success\""));
  assert_non_null(strstr(logs, "\"lease_id\":\"lease-1\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-keepalive\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-release\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_attach_rejects_malformed_json_response(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-bad-json",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]), NULL,
       0U, 0, 200, attach_response_headers, 2U, "{", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_source_from_memory("hello world", 11U, &src, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_non_null(error.message);

  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_attach_retries_node_passive_and_cleans_parser_state(
    void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *first_attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-passive-1",
      "Content-Type: application/json"};
  static const char *second_attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-passive-2",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]), NULL,
       0U, 0, 503, first_attach_response_headers, 2U,
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]), NULL,
       0U, 0, 200, second_attach_response_headers, 2U,
       "{\"attachment\":{\"id\":\"att-1\",\"name\":\"blob.txt\","
       "\"size\":11,\"plaintext_sha256\":\"sha-1\","
       "\"content_type\":\"text/plain\",\"created_at_unix\":2147483648,"
       "\"updated_at_unix\":2147483649},\"noop\":false,\"version\":2147483650}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  static char endpoint_a[128];
  static char endpoint_b[128];
  static const char *endpoints[2];
  snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
           (unsigned)server.port);
  snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
           (unsigned)server.port);
  endpoints[0] = endpoint_a;
  endpoints[1] = endpoint_b;
  config.endpoints = endpoints;
  config.endpoint_count = 2U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_source_from_memory("hello world", 11U, &src, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(attach_res.attachment.id, "att-1");
  assert_string_equal(attach_res.attachment.name, "blob.txt");
  assert_int_equal(attach_res.attachment.created_at_unix,
                   (lc_unix_seconds)2147483647L + 1L);
  assert_int_equal(attach_res.attachment.updated_at_unix,
                   (lc_unix_seconds)2147483647L + 2L);
  assert_false(attach_res.noop);
  assert_int_equal(attach_res.version, (lc_version)2147483647L + 3L);

  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_attachment_get_preserves_i64_timestamp_headers(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-attachment-get",
      "Content-Type: text/plain",
      "X-Attachment-ID: att-1",
      "X-Attachment-Name: blob.txt",
      "X-Attachment-SHA256: sha-1",
      "X-Attachment-Size: 5",
      "X-Attachment-Created-At: 2147483648",
      "X-Attachment-Updated-At: 2147483649"};
  static const https_expectation expectations[] = {
      {"GET",
       "/v1/attachment?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "public=1",
       NULL, 0U, NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), "hello",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_attachment_get_op req;
  lc_attachment_get_res res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  req.lease.ns = "transport-ns";
  req.lease.key = "resource/1";
  req.selector.name = "blob.txt";
  req.public_read = 1;
  rc = lc_get_attachment(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.attachment.id, "att-1");
  assert_string_equal(res.attachment.name, "blob.txt");
  assert_int_equal(res.attachment.size, 5L);
  assert_string_equal(res.attachment.plaintext_sha256, "sha-1");
  assert_string_equal(res.attachment.content_type, "text/plain");
  assert_int_equal(res.attachment.created_at_unix,
                   (lc_unix_seconds)2147483647L + 1L);
  assert_int_equal(res.attachment.updated_at_unix,
                   (lc_unix_seconds)2147483647L + 2L);
  assert_string_equal(res.correlation_id, "corr-attachment-get");
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, 5U);
  assert_memory_equal(bytes, "hello", length);

  lc_attachment_get_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_attach_rejects_non_rewindable_retry_source(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *first_attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-passive-1",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]), NULL,
       0U, 0, 503, first_attach_response_headers, 2U,
       "{\"error\":\"node_passive\"}", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  test_enqueue_source *src_state;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  src_state = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  {
    static char endpoint_a[128];
    static char endpoint_b[128];
    static const char *endpoints[2];

    snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
             (unsigned)server.port);
    snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
             (unsigned)server.port);
    endpoints[0] = endpoint_a;
    endpoints[1] = endpoint_b;
    config.endpoints = endpoints;
    config.endpoint_count = 2U;
  }
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  rc = test_enqueue_source_new_non_rewindable("hello world", 11U, 0U, &src,
                                              &src_state, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "not rewindable"));
  assert_int_equal(src_state->reset_count, 0U);

  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_client_update_rejects_non_rewindable_retry_source(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_source *src;
  test_enqueue_source *src_state;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  src_state = NULL;
  memset(&update_req, 0, sizeof(update_req));
  memset(&update_res, 0, sizeof(update_res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  {
    static char endpoint_a[128];
    static char endpoint_b[128];
    static const char *endpoints[2];

    snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
             (unsigned)server.port);
    snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
             (unsigned)server.port);
    endpoints[0] = endpoint_a;
    endpoints[1] = endpoint_b;
    config.endpoints = endpoints;
    config.endpoint_count = 2U;
  }
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  rc = test_enqueue_source_new_non_rewindable("{\"value\":2}", 11U, 0U, &src,
                                              &src_state, &error);
  assert_int_equal(rc, LC_OK);
  update_req.lease.ns = "transport-ns";
  update_req.lease.key = "resource/1";
  update_req.lease.lease_id = "lease-1";
  update_req.lease.txn_id = "txn-acquire";
  update_req.lease.fencing_token = 11L;
  update_req.content_type = "application/json";
  rc = lc_update(client, &update_req, src, &update_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "not rewindable"));
  assert_int_equal(src_state->reset_count, 0U);

  lc_update_res_cleanup(&update_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_update_rejects_non_rewindable_retry_source(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_update_opts update_opts;
  lc_source *src;
  test_enqueue_source *src_state;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  src_state = NULL;
  memset(&update_opts, 0, sizeof(update_opts));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  {
    static char endpoint_a[128];
    static char endpoint_b[128];
    static const char *endpoints[2];

    snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
             (unsigned)server.port);
    snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
             (unsigned)server.port);
    endpoints[0] = endpoint_a;
    endpoints[1] = endpoint_b;
    config.endpoints = endpoints;
    config.endpoint_count = 2U;
  }
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  rc = test_enqueue_source_new_non_rewindable("{\"value\":2}", 11U, 0U, &src,
                                              &src_state, &error);
  assert_int_equal(rc, LC_OK);
  update_opts.content_type = "application/json";
  rc = lc_lease_update(lease, src, &update_opts, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "not rewindable"));
  assert_int_equal(src_state->reset_count, 0U);

  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_retries_node_passive_and_cleans_parser_state(
    void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *const queue_stats_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\""};
  static const char *first_response_headers[] = {
      "X-Correlation-Id: corr-queue-stats-passive-1",
      "Content-Type: application/json"};
  static const char *second_response_headers[] = {
      "X-Correlation-Id: corr-queue-stats-passive-2",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U, queue_stats_body,
       sizeof(queue_stats_body) / sizeof(queue_stats_body[0]), 0, 503,
       first_response_headers,
       sizeof(first_response_headers) / sizeof(first_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST", "/v1/queue/stats", queue_headers, 1U, queue_stats_body,
       sizeof(queue_stats_body) / sizeof(queue_stats_body[0]), 0, 200,
       second_response_headers,
       sizeof(second_response_headers) / sizeof(second_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":1,\"pending_candidates\":2,"
       "\"total_consumers\":3,\"has_active_watcher\":false,"
       "\"available\":true,\"head_message_id\":\"msg-1\","
       "\"head_enqueued_at_unix\":100,\"head_not_visible_until_unix\":101,"
       "\"head_age_seconds\":2}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_queue_stats_request req;
  lc_engine_queue_stats_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &req, &res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg(
        "queue_stats retry rc=%d code=%d http_status=%ld message=%s "
        "server=%s detail=%s handled=%zu",
        rc, error.code, error.http_status,
        error.message != NULL ? error.message : "(null)",
        error.server_error_code != NULL ? error.server_error_code : "(null)",
        error.detail != NULL ? error.detail : "(null)", server.handled_count);
  }
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(res.waiting_consumers, 1);
  assert_int_equal(res.pending_candidates, 2);
  assert_int_equal(res.total_consumers, 3);
  assert_true(res.available);
  assert_false(res.has_active_watcher);
  assert_string_equal(res.head_message_id, "msg-1");

  lc_engine_queue_stats_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_enqueue_from_retries_node_passive_and_cleans_parser_state(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue-passive-2",
      "Content-Type: application/json"};
  static const char *const request_body_substrings[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"", "name=\"meta\"",
      "name=\"payload\""};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", queue_headers, 1U, request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]), 0,
       503, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST", "/v1/queue/enqueue", queue_headers, 1U, request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]), 0,
       200, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-enqueue-passive-2\",\"attempts\":0,"
       "\"max_attempts\":5,\"failure_attempts\":0,"
       "\"not_visible_until_unix\":123,\"visibility_timeout_seconds\":30,"
       "\"payload_bytes\":0}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_enqueue_request req;
  lc_engine_enqueue_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.ns = "transport-ns";
  req.queue = "jobs";
  req.payload_content_type = "application/json";
  rc = lc_engine_client_enqueue_from(client, &req, NULL, NULL, &res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("enqueue_from retry rc=%d code=%d http_status=%ld message=%s "
             "server=%s handled=%zu",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(null)",
             server.handled_count);
  }
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(res.ns, "transport-ns");
  assert_string_equal(res.queue, "jobs");
  assert_string_equal(res.message_id, "msg-enqueue-passive-2");
  assert_string_equal(res.correlation_id, "corr-enqueue-passive-2");
  assert_int_equal(res.payload_bytes, 0L);

  lc_engine_enqueue_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_enqueue_from_checks_public_metadata_range(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *enqueue_response_headers[] = {
      "Content-Type: application/json"};
  static const char *const request_body_substrings[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"", "name=\"meta\"",
      "name=\"payload\""};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", queue_headers, 1U, request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]), 0,
       200, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-timeout\",\"attempts\":0,"
       "\"max_attempts\":5,\"failure_attempts\":0,"
       "\"not_visible_until_unix\":123,"
       "\"visibility_timeout_seconds\":2147483648,\"payload_bytes\":0}",
       "liblockdc test client"},
      {"POST", "/v1/queue/enqueue", queue_headers, 1U, request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]), 0,
       200, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-payload\",\"attempts\":0,"
       "\"max_attempts\":5,\"failure_attempts\":0,"
       "\"not_visible_until_unix\":123,"
       "\"visibility_timeout_seconds\":30,\"payload_bytes\":2147483648}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_enqueue_request req;
  lc_engine_enqueue_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.ns = "transport-ns";
  req.queue = "jobs";
  req.payload_content_type = "application/json";
  rc = lc_engine_client_enqueue_from(client, &req, NULL, NULL, &res, &error);
  if (sizeof(long) < sizeof(lonejson_int64)) {
    assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
    assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
    assert_string_equal(error.message,
                        "enqueue visibility_timeout_seconds is out of range");
    assert_null(res.message_id);
  } else {
    assert_int_equal(rc, LC_ENGINE_OK);
    assert_string_equal(res.message_id, "msg-timeout");
    assert_true(res.visibility_timeout_seconds > 0L);
  }
  lc_engine_enqueue_response_cleanup(&res);
  lc_engine_error_cleanup(&error);
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));

  rc = lc_engine_client_enqueue_from(client, &req, NULL, NULL, &res, &error);
  if (sizeof(long) < sizeof(lonejson_int64)) {
    assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
    assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
    assert_string_equal(error.message, "enqueue payload_bytes is out of range");
    assert_null(res.message_id);
  } else {
    assert_int_equal(rc, LC_ENGINE_OK);
    assert_string_equal(res.message_id, "msg-payload");
    assert_true(res.payload_bytes > 0L);
  }

  lc_engine_enqueue_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_enqueue_from_rejects_non_rewindable_retry_source(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue-passive-1",
      "Content-Type: application/json"};
  static const char *const request_body_substrings[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"", "name=\"meta\"",
      "name=\"payload\""};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", queue_headers, 1U, request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]), 0,
       503, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_enqueue_request req;
  lc_engine_enqueue_response res;
  lc_engine_error error;
  lc_source *src;
  lc_read_bridge bridge;
  test_enqueue_source *src_state;
  int rc;

  (void)state;
  client = NULL;
  src = NULL;
  src_state = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  rc = test_enqueue_source_new_non_rewindable("hello world", 11U, 0U, &src,
                                              &src_state, NULL);
  assert_int_equal(rc, LC_OK);
  req.ns = "transport-ns";
  req.queue = "jobs";
  req.payload_content_type = "application/json";
  bridge.source = src;
  rc = lc_engine_client_enqueue_from(client, &req, lc_engine_read_bridge,
                                     &bridge, &res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_INVALID_ARGUMENT);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "not rewindable"));
  assert_int_equal(src_state->reset_count, 0U);

  lc_source_close(src);
  lc_engine_enqueue_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_mutate_local_covers_no_content_path(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/2\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-b\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire-local", "Content-Type: application/json"};
  static const char *get_headers[] = {"X-Correlation-Id: corr-local-get",
                                      "Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 22",
      "X-Lease-ID: lease-2", "X-Txn-ID: txn-local"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-local-update", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/2\","
       "\"owner\":\"owner-b\",\"lease_id\":\"lease-2\","
       "\"txn_id\":\"txn-local\",\"expires_at_unix\":1000,"
       "\"version\":0,\"state_etag\":\"\",\"fencing_token\":22}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F2&namespace=transport-ns", NULL, 0U, NULL,
       0U, 1, 204, get_headers, 2U, NULL, "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F2&namespace=transport-ns",
       update_headers, 2U, NULL, 0U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":1,\"new_state_etag\":\"etag-local\",\"bytes\":17}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_mutate_local_req mutate_local_req;
  const char *mutations[1];
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&mutate_local_req, 0, sizeof(mutate_local_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/2";
  acquire_req.owner = "owner-b";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  mutations[0] = "/extra=\"value\"";
  mutate_local_req.mutations = mutations;
  mutate_local_req.mutation_count = 1U;
  rc = lc_lease_mutate_local(lease, &mutate_local_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(lease->state_etag, "etag-local");
  assert_int_equal(lease->version, 1L);

  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_management_methods_emit_logs(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *ns_response_headers[] = {"X-Correlation-Id: corr-ns-get",
                                              "Content-Type: application/json"};
  static const char *tc_body[] = {"\"candidate_id\":\"node-a\"",
                                  "\"candidate_endpoint\":"
                                  "\"https://node-a:9443\"",
                                  "\"term\":3", "\"ttl_ms\":2000"};
  static const char *tc_response_headers[] = {
      "X-Correlation-Id: corr-tc-acquire", "Content-Type: application/json"};
  static const char *rm_response_headers[] = {"X-Correlation-Id: corr-rm-list",
                                              "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/namespace?namespace=team-a", NULL, 0U, NULL, 0U, 1, 200,
       ns_response_headers, 2U,
       "{\"namespace\":\"team-a\",\"query\":{\"preferred_engine\":\"index\","
       "\"fallback_engine\":\"scan\"}}",
       "liblockdc test client"},
      {"POST", "/v1/tc/lease/acquire", json_header, 1U, tc_body, 4U, 0, 200,
       tc_response_headers, 2U,
       "{\"granted\":true,\"leader_id\":\"node-a\","
       "\"leader_endpoint\":\"https://node-a:9443\",\"term\":3,"
       "\"expires_at\":9000}",
       "liblockdc test client"},
      {"GET", "/v1/tc/rm/list", NULL, 0U, NULL, 0U, 1, 200, rm_response_headers,
       2U,
       "{\"backends\":[{\"backend_hash\":\"bh-1\","
       "\"endpoints\":[\"https://rm-b:9443\"],\"updated_at_unix\":302}],"
       "\"updated_at_unix\":303}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_namespace_config_req ns_req;
  lc_namespace_config_res ns_res;
  lc_tc_lease_acquire_req tc_req;
  lc_tc_lease_acquire_res tc_res;
  lc_tc_rm_list_res rm_res;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&ns_req, 0, sizeof(ns_req));
  memset(&ns_res, 0, sizeof(ns_res));
  memset(&tc_req, 0, sizeof(tc_req));
  memset(&tc_res, 0, sizeof(tc_res));
  memset(&rm_res, 0, sizeof(rm_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  ns_req.ns = "team-a";
  rc = lc_get_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);

  tc_req.candidate_id = "node-a";
  tc_req.candidate_endpoint = "https://node-a:9443";
  tc_req.term = 3UL;
  tc_req.ttl_ms = 2000L;
  rc = lc_tc_lease_acquire(client, &tc_req, &tc_res, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_tc_rm_list(client, &rm_res, &error);
  assert_int_equal(rc, LC_OK);

  lc_namespace_config_res_cleanup(&ns_res);
  lc_tc_lease_acquire_res_cleanup(&tc_res);
  lc_tc_rm_list_res_cleanup(&rm_res);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"namespace.get.start\""));
  assert_non_null(strstr(logs, "\"message\":\"namespace.get.success\""));
  assert_non_null(strstr(logs, "\"message\":\"tc.lease.acquire.start\""));
  assert_non_null(strstr(logs, "\"message\":\"tc.lease.acquire.success\""));
  assert_non_null(strstr(logs, "\"message\":\"rm.list.start\""));
  assert_non_null(strstr(logs, "\"message\":\"rm.list.success\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-rm-list\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_queue_nack_maps_enum_intents(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *const nack_failure_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-failure\"", "\"lease_id\":\"lease-failure\"",
      "\"meta_etag\":\"meta-failure\"", "\"delay_seconds\":2",
      "\"intent\":\"failure\""};
  static const char *const nack_failure_headers[] = {
      "X-Correlation-Id: corr-public-nack-1", "Content-Type: application/json"};
  static const char *const nack_defer_body[] = {
      "\"namespace\":\"transport-ns\"",
      "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-defer\"",
      "\"lease_id\":\"lease-defer\"",
      "\"meta_etag\":\"meta-defer\"",
      "\"delay_seconds\":3",
      "\"intent\":\"defer\""};
  static const char *const nack_defer_headers[] = {
      "X-Correlation-Id: corr-public-nack-2", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/nack", queue_headers, 1U, nack_failure_body, 7U, 0,
       200, nack_failure_headers, 2U,
       "{\"requeued\":true,\"meta_etag\":\"meta-after-failure\","
       "\"correlation_id\":\"corr-public-nack-1\"}",
       "liblockdc test client"},
      {"POST", "/v1/queue/nack", queue_headers, 1U, nack_defer_body, 7U, 0, 200,
       nack_defer_headers, 2U,
       "{\"requeued\":true,\"meta_etag\":\"meta-after-defer\","
       "\"correlation_id\":\"corr-public-nack-2\"}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_nack_op req;
  lc_nack_res out;
  lc_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-failure";
  req.message.lease_id = "lease-failure";
  req.message.meta_etag = "meta-failure";
  req.delay_seconds = 2L;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(out.requeued);
  lc_nack_res_cleanup(&out);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-defer";
  req.message.lease_id = "lease-defer";
  req.message.meta_etag = "meta-defer";
  req.delay_seconds = 3L;
  req.intent = LC_NACK_INTENT_DEFER;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(out.requeued);
  lc_nack_res_cleanup(&out);

  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_enqueue_emits_logs(void **state) {
  static const char payload[] = "{\"ok\":true}";
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", NULL, 0U, NULL, 0U, 0, 200,
       enqueue_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-enqueue\",\"attempts\":0,\"max_attempts\":5,"
       "\"failure_attempts\":0,\"not_visible_until_unix\":123,"
       "\"visibility_timeout_seconds\":30,\"payload_bytes\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res out;
  lc_source *src;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  src = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory(payload, sizeof(payload) - 1U, &src, &error);
  assert_int_equal(rc, LC_OK);

  req.queue = "jobs";
  req.content_type = "application/json";
  req.max_attempts = 5L;
  req.visibility_timeout_seconds = 30L;
  rc = lc_enqueue(client, &req, src, &out, &error);
  assert_int_equal(rc, LC_OK);

  lc_enqueue_res_cleanup(&out);
  lc_source_close(src);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"queue.enqueue.begin\""));
  assert_non_null(strstr(logs, "\"message\":\"queue.enqueue.success\""));
  assert_non_null(strstr(logs, "\"msg_id\":\"msg-enqueue\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-enqueue\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_enqueue_streams_payload_from_source(void **state) {
  static const unsigned char payload[] =
      "{\"kind\":\"enqueue\",\"streamed\":true,\"payload\":1}";
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue", "Content-Type: application/json"};
  static const char *const required_headers[] = {
      "Accept: application/json",
      "Content-Type: multipart/related; "
      "boundary=lockdc-stream-boundary-7e4dbe2f",
      "Transfer-Encoding: chunked"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", required_headers, 3U, NULL, 0U, 0, 200,
       enqueue_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-enqueue\",\"attempts\":0,\"max_attempts\":5,"
       "\"failure_attempts\":0,\"not_visible_until_unix\":123,"
       "\"visibility_timeout_seconds\":30,\"payload_bytes\":46}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res out;
  lc_source *src;
  test_enqueue_source *src_state;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  src = NULL;
  src_state = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = test_enqueue_source_new(payload, sizeof(payload) - 1U, 4U, &src,
                               &src_state, &error);
  assert_int_equal(rc, LC_OK);
  src_state->client_to_close = client;

  req.queue = "jobs";
  req.content_type = "application/json";
  req.max_attempts = 5L;
  req.visibility_timeout_seconds = 30L;
  rc = lc_enqueue(client, &req, src, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(src_state->client_to_close);
  assert_int_equal(out.payload_bytes, (long)(sizeof(payload) - 1U));
  assert_true(src_state->read_count > 1U);
  assert_int_equal(src_state->offset, sizeof(payload) - 1U);

  lc_enqueue_res_cleanup(&out);
  lc_source_close(src);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_queue_nack_rejects_invalid_intent(void **state) {
  https_tls_material material;
  lc_client_config config;
  lc_client *client;
  lc_nack_op req;
  lc_nack_res out;
  lc_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  client = NULL;

  init_public_client_config(&config, 1U, material.client_bundle_path, NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-invalid";
  req.message.lease_id = "lease-invalid";
  req.message.meta_etag = "meta-invalid";
  req.intent = (lc_nack_intent)99;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_client_close(client);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_dequeue_emits_stream_transport_logs(void **state) {
  static const char dequeue_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 213\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\","
      "\"meta_etag\":\"meta-1\"},\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *dequeue_headers[] = {
      "X-Correlation-Id: corr-dequeue",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  static const char *const dequeue_required_headers[] = {
      "Content-Type: application/json"};
  static const char *const dequeue_required_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"owner\":\"worker-1\""};
  static const char *const ack_required_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-1\"", "\"lease_id\":\"lease-1\"",
      "\"meta_etag\":\"meta-1\""};
  static const char *ack_headers[] = {"X-Correlation-Id: corr-ack",
                                      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/dequeue", dequeue_required_headers, 1U,
       dequeue_required_body, 3U, 0, 200, dequeue_headers,
       sizeof(dequeue_headers) / sizeof(dequeue_headers[0]), dequeue_body,
       "liblockdc test client"},
      {"POST", "/v1/queue/ack", dequeue_required_headers, 1U, ack_required_body,
       5U, 0, 200, ack_headers, sizeof(ack_headers) / sizeof(ack_headers[0]),
       "{\"acked\":true,\"correlation_id\":\"corr-ack\"}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_dequeue_req req;
  lc_message *message;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  message = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.queue = "jobs";
  req.owner = "worker-1";
  rc = lc_dequeue(client, &req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  rc = lc_message_ack(message, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"queue.dequeue.begin\""));
  assert_non_null(strstr(logs, "\"message\":\"http.attempt\""));
  assert_non_null(strstr(logs, "\"path\":\"/v1/queue/dequeue\""));
  assert_non_null(strstr(logs, "\"message\":\"http.success\""));
  assert_non_null(strstr(logs, "\"message\":\"queue.dequeue.success\""));
  assert_non_null(strstr(logs, "\"message\":\"queue.ack.success\""));
  assert_non_null(strstr(logs, "\"msg_id\":\"msg-1\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_captures_headers_and_body(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"namespace\":\"transport-ns\"",
                                     "\"selector\":{\"owner\":\"owner-a\"}",
                                     "\"limit\":2",
                                     "\"cursor\":\"cursor-0\"",
                                     "\"fields\":[\"key\"]",
                                     "\"return\":\"compact\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-stream",
      "X-Lockd-Query-Cursor: cursor-1",
      "X-Lockd-Query-Index-Seq: 12",
      "X-Lockd-Query-Metadata: {\"partial\":false}",
      "X-Lockd-Query-Return: compact",
      "Content-Type: application/x-ndjson"};
  static const https_expectation expectations[] = {
      {"POST",
       "/v1/query?engine=scan%20engine%2F1&refresh=wait%26refresh%2Bnow",
       query_headers, sizeof(query_headers) / sizeof(query_headers[0]),
       query_body, sizeof(query_body) / sizeof(query_body[0]), 0, 200,
       response_headers, sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/1\"}\n", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  lc_sink closing_sink;
  client_closing_sink closing_context;
  const void *bytes;
  size_t length;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&closing_sink, 0, sizeof(closing_sink));
  memset(&closing_context, 0, sizeof(closing_context));
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  closing_context.client = client;
  closing_context.delegate = sink;
  closing_sink.write = client_closing_sink_write;
  closing_sink.close = client_closing_sink_close;
  closing_sink.impl = &closing_context;

  req.ns = "transport-ns";
  req.selector_json = "{\"owner\":\"owner-a\"}";
  req.limit = 2L;
  req.cursor = "cursor-0";
  req.fields_json = "[\"key\"]";
  req.return_mode = "compact";
  req.engine = "scan engine/1";
  req.refresh = "wait&refresh+now";
  rc = lc_query(client, &req, &closing_sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(closing_context.write_calls, 1U);
  assert_null(closing_context.client);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"key\":\"resource/1\"}\n"));
  assert_memory_equal(bytes, "{\"key\":\"resource/1\"}\n", length);
  assert_string_equal(res.cursor, "cursor-1");
  assert_string_equal(res.return_mode, "compact");
  assert_int_equal(res.index_seq, 12UL);
  assert_string_equal(res.correlation_id, "corr-query-stream");

  lc_query_res_cleanup(&res);
  lc_sink_close(&closing_sink);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_keys_streams_chunks_and_headers(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {
      "\"namespace\":\"transport-ns\"", "\"selector\":{\"eq\"",
      "\"field\":\"/owner\"", "\"value\":\"owner-a\"", "\"return\":\"keys\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-keys",
      "X-Lockd-Query-Cursor: cursor-keys-1",
      "X-Lockd-Query-Index-Seq: 45",
      "X-Lockd-Query-Metadata: {\"source\":\"header\"}",
      "X-Lockd-Query-Return: keys",
      "Content-Type: application/json"};
  static const char response_body[] =
      "{\"namespace\":\"transport-ns\",\"keys\":[\"resource/one\","
      "\"resource/two\"],\"cursor\":\"body-cursor\",\"index_seq\":1,"
      "\"metadata\":{\"source\":\"body\"}}";
  https_expectation expectations[] = {
      {"POST",
       "/v1/"
       "query?engine=index%26scan%2Ffast%2Bsafe&refresh=wait%20for%2Fseq%2B1",
       query_headers, sizeof(query_headers) / sizeof(query_headers[0]),
       query_body, sizeof(query_body) / sizeof(query_body[0]), 0, 200,
       response_headers, sizeof(response_headers) / sizeof(response_headers[0]),
       response_body, "liblockdc test client", 3U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  req.ns = "transport-ns";
  req.selector_lql = "eq{field=/owner,value=owner-a}";
  req.limit = 2L;
  req.engine = "index&scan/fast+safe";
  req.refresh = "wait for/seq+1";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.begin_calls, 2);
  assert_true(capture.chunk_calls >= 2);
  assert_int_equal(capture.end_calls, 2);
  assert_string_equal(capture.bytes, "[resource/one][resource/two]");
  assert_string_equal(res.cursor, "cursor-keys-1");
  assert_string_equal(res.return_mode, "keys");
  assert_int_equal(res.index_seq, 45UL);
  assert_string_equal(res.metadata_json, "{\"source\":\"header\"}");
  assert_string_equal(res.correlation_id, "corr-query-keys");

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_keys_captures_body_metadata(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"return\":\"keys\""};
  static const char *response_headers[] = {"Content-Type: application/json"};
  static const char response_body[] =
      "{\"keys\":[\"resource/body-one\",\"resource/body-two\"],"
      "\"cursor\":\"body-cursor\",\"index_seq\":77,"
      "\"metadata\":{\"source\":\"body\"}}";
  https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 5U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.begin_calls, 2);
  assert_true(capture.chunk_calls >= 2);
  assert_int_equal(capture.end_calls, 2);
  assert_string_equal(capture.bytes, "[resource/body-one][resource/body-two]");
  assert_string_equal(res.cursor, "body-cursor");
  assert_int_equal(res.index_seq, 77UL);
  assert_string_equal(res.metadata_json, "{\"source\":\"body\"}");
  assert_null(res.correlation_id);

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_keys_accepts_empty_results(void **state) {
  static const char *response_headers[] = {"Content-Type: application/json"};
  static const char *response_bodies[] = {
      "{\"namespace\":\"transport-ns\",\"index_seq\":77,"
      "\"metadata\":{\"index_pending\":\"false\"}}",
      "{\"keys\":[],\"index_seq\":77,"
      "\"metadata\":{\"index_pending\":\"false\"}}",
      "{\"keys\":null,\"index_seq\":77,"
      "\"metadata\":{\"index_pending\":\"false\"}}",
      "{\"keys\":1}",
      "{\"keys\":{}}",
      "{\"keys\":\"invalid\"}"};
  https_expectation expectations[6];
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  size_t index;
  int rc;

  (void)state;
  memset(expectations, 0, sizeof(expectations));
  for (index = 0U; index < 6U; ++index) {
    expectations[index].method = "POST";
    expectations[index].path = "/v1/query?engine=index&refresh=wait_for";
    expectations[index].response_status = 200;
    expectations[index].response_headers = response_headers;
    expectations[index].response_header_count = 1U;
    expectations[index].response_body = response_bodies[index];
    expectations[index].expected_client_cn = "liblockdc test client";
    expectations[index].response_body_chunk_size = 3U;
  }
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(https_testserver_start(&server, &material, expectations, 6U));
  /* Invalid key types can abort reception before all fragments are sent. */
  server.allow_response_write_failure = 1;
  memset(&config, 0, sizeof(config));
  memset(&handler, 0, sizeof(handler));
  lc_error_init(&error);
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  lc_query_req_init(&req);
  req.ns = "transport-ns";
  req.selector_json =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"claimed\"}}";
  req.limit = 16L;
  req.engine = "index";
  req.refresh = "wait_for";
  for (index = 0U; index < 6U; ++index) {
    memset(&res, 0, sizeof(res));
    memset(&capture, 0, sizeof(capture));
    rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
    assert_int_equal(rc, index < 2U ? LC_OK : LC_ERR_PROTOCOL);
    assert_int_equal(capture.begin_calls, 0);
    assert_int_equal(capture.chunk_calls, 0);
    assert_int_equal(capture.end_calls, 0);
    if (index < 2U) {
      assert_null(res.cursor);
      assert_int_equal(res.index_seq, 77UL);
      assert_string_equal(res.metadata_json, "{\"index_pending\":\"false\"}");
    }
    lc_query_res_cleanup(&res);
    lc_error_cleanup(&error);
    lc_error_init(&error);
  }
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_keys_streams_large_response_without_client_alloc(
    void **state) {
  enum { key_count = 4096 };
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"return\":\"keys\""};
  static const char *response_headers[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client", 11U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_large_capture capture;
  tracking_allocator_state alloc_state;
  char expected_first[64];
  char expected_last[64];
  char *response_body;
  size_t total_key_bytes;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  response_body = make_large_query_keys_body(key_count, &total_key_bytes);
  assert_non_null(response_body);
  expectations[0].response_body = response_body;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&alloc_state, 0, sizeof(alloc_state));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  tracking_allocator_configure(&config.allocator, &alloc_state);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  memset(&alloc_state, 0, sizeof(alloc_state));

  handler.begin = capture_large_query_key_begin;
  handler.chunk = capture_large_query_key_chunk;
  handler.end = capture_large_query_key_end;
  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.begin_calls, (size_t)key_count);
  assert_int_equal(capture.end_calls, (size_t)key_count);
  assert_true(capture.chunk_calls >= (size_t)key_count);
  assert_int_equal(capture.total_bytes, total_key_bytes);
  format_large_query_key(0U, expected_first, sizeof(expected_first));
  format_large_query_key((size_t)key_count - 1U, expected_last,
                         sizeof(expected_last));
  assert_string_equal(capture.first, expected_first);
  assert_string_equal(capture.last, expected_last);
  assert_null(res.cursor);
  assert_int_equal(res.index_seq, 0UL);
  assert_null(res.metadata_json);
  assert_int_equal(alloc_state.malloc_calls, 0U);
  assert_int_equal(alloc_state.realloc_calls, 0U);
  assert_int_equal(alloc_state.bytes_requested, 0U);

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void
test_public_query_keys_propagates_chunk_callback_failure(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"return\":\"keys\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-keys-fail", "X-Lockd-Query-Return: keys",
      "Content-Type: application/json"};
  static const char response_body[] = "{\"keys\":[\"resource/fail\"]}";
  https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 0U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  capture.fail_on_chunk = 1;
  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "capture rejected query key");
  assert_int_equal(capture.begin_calls, 1);
  assert_int_equal(capture.end_calls, 0);

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_query_keys_propagates_end_callback_failure(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"return\":\"keys\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-keys-end-fail",
      "X-Lockd-Query-Return: keys", "Content-Type: application/json"};
  static const char response_body[] = "{\"keys\":[\"resource/end-fail\"]}";
  https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 0U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  capture.fail_on_end = 1;
  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "capture rejected query key end");
  assert_int_equal(capture.begin_calls, 1);
  assert_true(capture.chunk_calls >= 1);
  assert_int_equal(capture.end_calls, 1);

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_keys_rejects_malformed_keys_json(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"return\":\"keys\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-keys-bad-json",
      "X-Lockd-Query-Return: keys", "Content-Type: application/json"};
  static const char response_body[] = "{\"keys\":[\"unterminated\"";
  https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client", 3U, NULL, 0U}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  handler.begin = capture_query_key_begin;
  handler.chunk = capture_query_key_chunk;
  handler.end = capture_query_key_end;
  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_non_null(error.message);

  lc_query_res_cleanup(&res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_query_stream_captures_trailers_after_body(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {
      "\"selector\":{\"eq\":{\"field\":\"/kind\"",
      "\"value\":\"trailer-unit\"}}", "\"return\":\"documents\""};
  static const char *response_headers[] = {
      "x-correlation-id: corr-query-stream-trailer",
      "x-lockd-query-return: documents",
      "Trailer: x-lockd-query-cursor, x-lockd-query-index-seq, "
      "x-lockd-query-metadata",
      "Content-Type: application/x-ndjson"};
  static const char *response_trailers[] = {
      "x-lockd-query-cursor: cursor-from-trailer",
      "x-lockd-query-index-seq: 34",
      "x-lockd-query-metadata: {\"source\":\"trailer\"}"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query?engine=index", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/trailer\"}\n", "liblockdc test client", 5U,
       response_trailers,
       sizeof(response_trailers) / sizeof(response_trailers[0])}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json =
      "{\"eq\":{\"field\":\"/kind\",\"value\":\"trailer-unit\"}}";
  req.return_mode = "documents";
  req.engine = "index";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"key\":\"resource/trailer\"}\n"));
  assert_memory_equal(bytes, "{\"key\":\"resource/trailer\"}\n", length);
  assert_string_equal(res.cursor, "cursor-from-trailer");
  assert_string_equal(res.return_mode, "documents");
  assert_int_equal(res.index_seq, 34UL);
  assert_string_equal(res.metadata_json, "{\"source\":\"trailer\"}");
  assert_string_equal(res.correlation_id, "corr-query-stream-trailer");

  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_trailers_override_headers(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {
      "\"selector\":{\"eq\":{\"field\":\"/kind\"",
      "\"value\":\"trailer-override\"}}", "\"return\":\"documents\""};
  static const char *response_headers[] = {
      "x-correlation-id: corr-query-stream-override",
      "x-lockd-query-return: headers-return",
      "x-lockd-query-cursor: stale-header-cursor",
      "x-lockd-query-index-seq: 1",
      "x-lockd-query-metadata: {\"source\":\"header\"}",
      "Trailer: x-lockd-query-cursor, x-lockd-query-index-seq, "
      "x-lockd-query-metadata, x-lockd-query-return",
      "Content-Type: application/x-ndjson"};
  static const char *response_trailers[] = {
      "x-lockd-query-cursor: final-trailer-cursor",
      "x-lockd-query-index-seq: 99",
      "x-lockd-query-metadata: {\"source\":\"trailer\"}",
      "x-lockd-query-return: documents"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/override\"}\n", "liblockdc test client", 7U,
       response_trailers,
       sizeof(response_trailers) / sizeof(response_trailers[0])}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json =
      "{\"eq\":{\"field\":\"/kind\",\"value\":\"trailer-override\"}}";
  req.return_mode = "documents";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"key\":\"resource/override\"}\n"));
  assert_memory_equal(bytes, "{\"key\":\"resource/override\"}\n", length);
  assert_string_equal(res.cursor, "final-trailer-cursor");
  assert_string_equal(res.return_mode, "documents");
  assert_int_equal(res.index_seq, 99UL);
  assert_string_equal(res.metadata_json, "{\"source\":\"trailer\"}");
  assert_string_equal(res.correlation_id, "corr-query-stream-override");

  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_retries_node_passive_and_cleans_trailers(
    void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"selector\":{\"owner\":\"owner-a\"}",
                                     "\"return\":\"documents\""};
  static const char *first_response_headers[] = {
      "X-Correlation-Id: corr-query-passive", "X-Lockd-Query-Return: documents",
      "Trailer: x-lockd-query-cursor, x-lockd-query-index-seq, "
      "x-lockd-query-metadata",
      "Content-Type: application/json"};
  static const char *first_response_trailers[] = {
      "x-lockd-query-cursor: stale-passive-cursor",
      "x-lockd-query-index-seq: 88",
      "x-lockd-query-metadata: {\"source\":\"passive\"}"};
  static const char *second_response_headers[] = {
      "X-Correlation-Id: corr-query-active", "X-Lockd-Query-Return: documents",
      "Content-Type: application/x-ndjson"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 503,
       first_response_headers,
       sizeof(first_response_headers) / sizeof(first_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client", 6U,
       first_response_trailers,
       sizeof(first_response_trailers) / sizeof(first_response_trailers[0])},
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200,
       second_response_headers,
       sizeof(second_response_headers) / sizeof(second_response_headers[0]),
       "{\"key\":\"resource/active\"}\n", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  static char endpoint_a[128];
  static char endpoint_b[128];
  static const char *endpoints[2];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
           (unsigned)server.port);
  snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
           (unsigned)server.port);
  endpoints[0] = endpoint_a;
  endpoints[1] = endpoint_b;
  config.endpoints = endpoints;
  config.endpoint_count = 2U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json = "{\"owner\":\"owner-a\"}";
  req.return_mode = "documents";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"key\":\"resource/active\"}\n"));
  assert_memory_equal(bytes, "{\"key\":\"resource/active\"}\n", length);
  assert_null(res.cursor);
  assert_string_equal(res.return_mode, "documents");
  assert_int_equal(res.index_seq, 0UL);
  assert_null(res.metadata_json);
  assert_string_equal(res.correlation_id, "corr-query-active");

  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_query_stream_rejects_invalid_trailer_index_seq(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {
      "\"selector\":{\"eq\":{\"field\":\"/kind\"",
      "\"value\":\"bad-trailer-unit\"}}", "\"return\":\"documents\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-stream-trailer",
      "X-Lockd-Query-Return: documents", "Trailer: x-lockd-query-index-seq",
      "Content-Type: application/x-ndjson"};
  static const char *response_trailers[] = {
      "x-lockd-query-index-seq: not-a-number"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/bad-trailer\"}\n", "liblockdc test client", 0U,
       response_trailers,
       sizeof(response_trailers) / sizeof(response_trailers[0])}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json =
      "{\"eq\":{\"field\":\"/kind\",\"value\":\"bad-trailer-unit\"}}";
  req.return_mode = "documents";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_non_null(error.message);
  assert_string_equal(error.message, "failed to parse query index sequence");

  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_lease_save_uses_mapped_lonejson_upload(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-If-Version: 4"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-update", "Content-Type: application/json"};
  static const char *update_body[] = {"{\"value\":2}"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]),
       update_body, 1U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\","
       "\"bytes\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  value_doc.value = 2;
  rc = lc_lease_save(lease, &test_value_map, &value_doc, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 5L);
  assert_string_equal(lease->state_etag, "etag-2");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_save_ignores_response_limit_for_request_body(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-If-Version: 4"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-update", "Content-Type: application/json"};
  const char *update_body[1];
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]),
       update_body, 1U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\","
       "\"bytes\":715}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  test_large_payload_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(value_doc.payload, 'x', 700U);
  value_doc.payload[700] = '\0';
  update_body[0] = value_doc.payload;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  config.http_json_response_limit_bytes = 512U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_lease_save(lease, &test_large_payload_map, &value_doc, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 5L);
  assert_string_equal(lease->state_etag, "etag-2");

  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_lease_get_refreshes_state_view(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-next", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  get_opts.public_read = 1;
  rc = lc_lease_get(lease, sink, &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":1}"));
  assert_memory_equal(bytes, "{\"value\":1}", length);
  assert_string_equal(get_res.etag, "etag-next");
  assert_int_equal(get_res.version, 5L);
  assert_int_equal(get_res.fencing_token, 12L);
  assert_int_equal(((lc_lease_handle *)lease)->version, 5L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 12L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-next");

  lc_get_res_cleanup(&get_res);
  lc_sink_close(sink);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_load_respects_configured_json_response_limit(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  config.http_json_response_limit_bytes = 0U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  ((lc_client_handle *)client)->http_json_response_limit_bytes = 1U;
  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, &get_opts, &get_res,
                     &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_string_equal(error.message,
                      "mapped state response exceeds configured byte limit");
  assert_int_equal(value_doc.value, 0);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_load_respects_configured_spooled_response_limit(
    void **state) {
  const size_t payload_len = (size_t)LONEJSON_SPOOL_MAX_BYTES + 1024U;
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_spooled_payload_doc value_doc;
  char *response_body;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  response_body =
      make_repeat_json_body("{\"payload\":\"", "\"}", payload_len, 'x');
  assert_non_null(response_body);
  expectations[0].response_body = response_body;
  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  config.http_json_response_limit_bytes = payload_len + 4096U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  get_opts.public_read = 1;
  rc = lc_load(client, "resource/1", &test_spooled_payload_map, &value_doc,
               &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lonejson_spooled_size(&value_doc.payload), payload_len);
  assert_true(lonejson_spooled_spilled(&value_doc.payload));

  test_lonejson_cleanup_value(&test_spooled_payload_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(response_body);
}

static void test_public_client_load_preserves_preinitialized_json_value_capture(
    void **state) {
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"payload\":{\"nested\":true}}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lonejson *runtime;
  test_json_value_doc value_doc;
  test_request_capture capture;
  lonejson_error lj_error;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  runtime = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&capture, 0, sizeof(capture));
  memset(&lj_error, 0, sizeof(lj_error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  runtime = lc_thread_lonejson_runtime();
  assert_int_equal(
      runtime->init(runtime, &test_json_value_map, &value_doc, NULL),
      LONEJSON_STATUS_OK);
  rc = value_doc.payload.methods->enable_parse_capture(&value_doc.payload,
                                                       &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);

  get_opts.public_read = 1;
  rc = lc_load(client, "resource/1", &test_json_value_map, &value_doc,
               &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = value_doc.payload.methods->write_to_sink(
      &value_doc.payload, test_lonejson_capture_sink, &capture, &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);
  assert_non_null(capture.data);
  assert_string_equal(capture.data, "{\"nested\":true}");

  buffer_cleanup(&capture);
  test_lonejson_cleanup_value(&test_json_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_load_clears_mixed_json_value_mapped_destination(
    void **state) {
  static const char *first_headers[] = {
      "X-Correlation-Id: corr-get-first", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const char *second_headers[] = {
      "X-Correlation-Id: corr-get-second", "Content-Type: application/json",
      "ETag: etag-2", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, first_headers, 5U,
       "{\"payload\":{\"nested\":true},\"label\":\"first\"}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, second_headers, 5U,
       "{\"payload\":{\"nested\":false}}", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lonejson *runtime;
  test_json_value_with_label_doc value_doc;
  test_request_capture capture;
  lonejson_error lj_error;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  runtime = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&capture, 0, sizeof(capture));
  memset(&lj_error, 0, sizeof(lj_error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  runtime = lc_thread_lonejson_runtime();
  assert_int_equal(
      runtime->init(runtime, &test_json_value_with_label_map, &value_doc, NULL),
      LONEJSON_STATUS_OK);
  rc = value_doc.payload.methods->enable_parse_capture(&value_doc.payload,
                                                       &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);

  get_opts.public_read = 1;
  rc = lc_load(client, "resource/1", &test_json_value_with_label_map,
               &value_doc, &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(value_doc.label, "first");
  rc = value_doc.payload.methods->write_to_sink(
      &value_doc.payload, test_lonejson_capture_sink, &capture, &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);
  assert_non_null(capture.data);
  assert_string_equal(capture.data, "{\"nested\":true}");
  buffer_cleanup(&capture);
  lc_get_res_cleanup(&get_res);

  rc = lc_load(client, "resource/1", &test_json_value_with_label_map,
               &value_doc, &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(value_doc.label, "");
  rc = value_doc.payload.methods->write_to_sink(
      &value_doc.payload, test_lonejson_capture_sink, &capture, &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);
  assert_non_null(capture.data);
  assert_string_equal(capture.data, "{\"nested\":false}");

  buffer_cleanup(&capture);
  test_lonejson_cleanup_value(&test_json_value_with_label_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_client_load_clears_reused_mapped_destination(void **state) {
  static const char *first_headers[] = {
      "X-Correlation-Id: corr-get-first", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const char *second_headers[] = {
      "X-Correlation-Id: corr-get-second", "Content-Type: application/json",
      "ETag: etag-2", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, first_headers, 5U,
       "{\"value\":1,\"label\":\"first\"}", "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, second_headers, 5U, "{\"value\":2}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_optional_label_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  get_opts.public_read = 1;
  rc = lc_load(client, "resource/1", &test_optional_label_map, &value_doc,
               &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(value_doc.value, 1L);
  assert_string_equal(value_doc.label, "first");
  lc_get_res_cleanup(&get_res);

  rc = lc_load(client, "resource/1", &test_optional_label_map, &value_doc,
               &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(value_doc.value, 2L);
  assert_string_equal(value_doc.label, "");

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_load_fails_on_metadata_allocation(void **state) {
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  reset_strdup_failure_state();
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  get_opts.public_read = 1;
  g_fail_lc_strdup_local_on_call = 1;
  rc = lc_load(client, "resource/1", &test_value_map, &value_doc, &get_opts,
               &get_res, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_int_equal(error.code, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to allocate mapped load metadata");
  assert_null(get_res.content_type);
  assert_null(get_res.etag);
  assert_null(get_res.correlation_id);
  assert_int_equal(get_res.version, 0L);
  assert_int_equal(get_res.fencing_token, 0L);

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  reset_strdup_failure_state();
}

static void
test_public_lease_load_parse_failure_does_not_refresh_state_view(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-next", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, &get_opts, &get_res,
                     &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "failed to parse mapped lease state"));
  assert_int_equal(value_doc.value, 0);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"get.start\""));
  assert_null(strstr(logs, "\"message\":\"get.success\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_lease_load_fails_on_metadata_allocation(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-next", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  reset_strdup_failure_state();
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  get_opts.public_read = 1;
  g_fail_lc_strdup_local_on_call = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, &get_opts, &get_res,
                     &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_int_equal(error.code, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to allocate mapped load metadata");
  assert_null(get_res.content_type);
  assert_null(get_res.etag);
  assert_null(get_res.correlation_id);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  reset_strdup_failure_state();
}

static void
test_public_lease_save_fails_on_state_etag_allocation(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-If-Version: 4"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-update", "Content-Type: application/json"};
  static const char *update_body[] = {"{\"value\":2}"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]),
       update_body, 1U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\","
       "\"bytes\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&error, 0, sizeof(error));
  reset_strdup_failure_state();
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  value_doc.value = 2;
  g_fail_lc_client_strdup_on_call = 1;
  rc = lc_lease_save(lease, &test_value_map, &value_doc, NULL, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_int_equal(error.code, LC_ERR_NOMEM);
  assert_string_equal(error.message,
                      "failed to allocate refreshed lease state etag");
  assert_int_equal(lease->version, 4L);
  assert_string_equal(lease->state_etag, "etag-1");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  reset_strdup_failure_state();
}

static void
test_public_lease_load_empty_does_not_refresh_state_view(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {"X-Correlation-Id: corr-get"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 204, get_headers, 1U, NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, &get_opts, &get_res,
                     &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  assert_null(get_res.etag);
  assert_int_equal(get_res.version, 0L);
  assert_int_equal(get_res.fencing_token, 0L);
  assert_int_equal(value_doc.value, 0);
  assert_int_equal(((lc_lease_handle *)lease)->version, 3L);
  assert_int_equal(((lc_lease_handle *)lease)->fencing_token, 7L);
  assert_string_equal(((lc_lease_handle *)lease)->state_etag, "etag-prev");

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_lease_load_preserves_preinitialized_json_value_capture(
    void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":3,\"state_etag\":\"etag-prev\",\"fencing_token\":7}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"payload\":{\"nested\":true}}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lonejson *runtime;
  test_json_value_doc value_doc;
  test_request_capture capture;
  lonejson_error lj_error;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  runtime = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  memset(&capture, 0, sizeof(capture));
  memset(&lj_error, 0, sizeof(lj_error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  runtime = lc_thread_lonejson_runtime();
  assert_int_equal(
      runtime->init(runtime, &test_json_value_map, &value_doc, NULL),
      LONEJSON_STATUS_OK);
  rc = value_doc.payload.methods->enable_parse_capture(&value_doc.payload,
                                                       &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);

  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_json_value_map, &value_doc, &get_opts,
                     &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = value_doc.payload.methods->write_to_sink(
      &value_doc.payload, test_lonejson_capture_sink, &capture, &lj_error);
  assert_int_equal(rc, LONEJSON_STATUS_OK);
  assert_non_null(capture.data);
  assert_string_equal(capture.data, "{\"nested\":true}");

  buffer_cleanup(&capture);
  test_lonejson_cleanup_value(&test_json_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_client_load_parse_failure_does_not_log_success(void **state) {
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-next", "X-Key-Version: 5", "X-Fencing-Token: 12"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  client = NULL;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  get_opts.public_read = 1;
  rc = lc_load(client, "resource/1", &test_value_map, &value_doc, &get_opts,
               &get_res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "failed to parse mapped state"));

  test_lonejson_cleanup_value(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"get.start\""));
  assert_null(strstr(logs, "\"message\":\"get.success\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_rejects_invalid_index_seq(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"selector\":{\"owner\":\"owner-a\"}"};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-stream",
      "X-Lockd-Query-Index-Seq: not-a-number",
      "Content-Type: application/x-ndjson"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/1\"}\n", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_string_equal(error.message, "failed to parse query index sequence");
  assert_null(res.cursor);
  assert_null(res.return_mode);
  assert_null(res.correlation_id);
  assert_int_equal(res.index_seq, 0UL);

  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_retention_remote_options_and_observations(void **state) {
  const char *json[] = {"Content-Type: application/json"};
  const char *ttl[] = {"X-State-Retention-Seconds: 60"};
  const char *clear[] = {"X-State-Retention-Seconds: 0"};
  const char *pending[] = {"Content-Type: application/json",
                           "x-state-retention-supported: \t1\t "};
  const char *no_expiry[] = {"Content-Type: application/json",
                             "X-State-Retention-Expires-At-Unix: 0"};
  const char *unsupported[] = {"Content-Type: application/json",
                               "X-State-Retention-Supported: 0"};
  const char *uncertain[] = {"Content-Type: application/json",
                             "X-State-Retention-Expires-At-Unix: -1"};
  const char *expiry[] = {"Content-Type: application/json", "X-Key-Version: 8",
                          "X-State-Retention-Expires-At-Unix: 2147483648",
                          "X-State-Retention-Expires-At-Unix: 2147483648",
                          "X-State-Retention-Supported: 1"};
  https_expectation expectations[] = {
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns", ttl, 1,
       NULL, 0, 0, 200, pending, 2,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\",\"bytes\":11}"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns", clear, 1,
       NULL, 0, 0, 200, no_expiry, 2,
       "{\"new_version\":6,\"new_state_etag\":\"etag-3\",\"bytes\":11}"},
      {"POST", "/v1/mutate?key=resource%2F1&namespace=transport-ns", ttl, 1,
       NULL, 0, 0, 200, unsupported, 2,
       "{\"new_version\":7,\"new_state_etag\":\"etag-4\",\"bytes\":11}"},
      {"POST", "/v1/metadata?key=resource%2F1&namespace=transport-ns", clear, 1,
       NULL, 0, 0, 200, json, 1,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/"
       "1\",\"version\":8,\"metadata\":{}}"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns", NULL, 0, NULL,
       0, 1, 200, expiry, 5, "{\"value\":1}"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0, NULL, 0, 1, 200, unsupported, 2, "{\"value\":1}"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns", ttl, 1,
       NULL, 0, 0, 200, uncertain, 2,
       "{\"new_version\":9,\"new_state_etag\":\"etag-uncertain\",\"bytes\":"
       "11}"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns", NULL, 0,
       NULL, 0, 0, 200, json, 1,
       "{\"new_version\":9,\"new_state_etag\":\"etag-5\",\"bytes\":11}"},
  };
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client = NULL;
  lc_lease *lease;
  lc_update_opts options = {0};
  lc_mutate_req mutate = {0};
  lc_metadata_req metadata = {0};
  const char *mutations[] = {"value=2"};
  test_value_doc document = {2};
  lc_get_opts public_read = {1};
  lc_get_res result = {0};
  lc_source *source = NULL;
  lc_sink *sink = NULL;
  lc_error error;
  (void)state;
  lc_error_init(&error);
  expectations[7].forbidden_header = "X-State-Retention-Seconds:";
  assert_true(https_tls_material_init(&material, 1));
  assert_true(https_testserver_start(&server, &material, expectations, 8));
  server.allow_response_write_failure = 1;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  assert_int_equal(lc_client_open(&config, &client, &error), LC_OK);
  lease = lc_lease_new((lc_client_handle *)client, "transport-ns", "resource/1",
                       "owner", "lease-1", "txn-1", 11, 4, "etag-1", NULL);
  assert_non_null(lease);
  options.has_retention_seconds = 1;
  options.retention_seconds = 60;
  assert_int_equal(
      lc_lease_save(lease, &test_value_map, &document, &options, &error),
      LC_OK);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_false(lease->has_retention_expires_at_unix);
  options.retention_seconds = 0;
  assert_int_equal(lc_source_from_memory("{\"value\":2}", 11, &source, &error),
                   LC_OK);
  assert_int_equal(lc_lease_update(lease, source, &options, &error), LC_OK);
  lc_source_close(source);
  assert_true(lease->has_retention_expires_at_unix);
  assert_int_equal(lease->retention_expires_at_unix, 0);
  mutate.has_retention_seconds = 1;
  mutate.retention_seconds = 60;
  mutate.mutations = mutations;
  mutate.mutation_count = 1;
  assert_int_equal(lc_lease_mutate(lease, &mutate, &error), LC_OK);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_UNSUPPORTED);
  assert_false(lease->has_retention_expires_at_unix);
  metadata.has_retention_seconds = 1;
  assert_int_equal(lc_lease_metadata(lease, &metadata, &error), LC_OK);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_UNKNOWN);
  assert_false(lease->has_retention_expires_at_unix);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &result, &error), LC_OK);
  assert_int_equal(result.retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_true(result.has_retention_expires_at_unix);
  assert_int_equal(result.retention_expires_at_unix, INT64_C(2147483648));
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, &public_read, &result, &error),
                   LC_OK);
  assert_int_equal(result.retention_support, LC_STATE_RETENTION_UNSUPPORTED);
  assert_false(result.has_retention_expires_at_unix);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_int_equal(lease->retention_expires_at_unix, INT64_C(2147483648));
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
  options.retention_seconds = 60;
  assert_int_equal(
      lc_lease_save(lease, &test_value_map, &document, &options, &error),
      LC_ERR_PROTOCOL);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_UNKNOWN);
  assert_false(lease->has_retention_expires_at_unix);
  lc_error_cleanup(&error);
  options.has_retention_seconds = 0;
  options.retention_seconds = -1;
  assert_int_equal(
      lc_lease_save(lease, &test_value_map, &document, &options, &error),
      LC_OK);
  assert_int_equal(lease->retention_support, LC_STATE_RETENTION_UNKNOWN);
  assert_false(lease->has_retention_expires_at_unix);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_retention_remote_rejects_invalid_headers(void **state) {
  const char *bad[] = {
      "X-State-Retention-Supported: 2",
      "X-State-Retention-Supported: true",
      "X-State-Retention-Supported:",
      "X-State-Retention-Expires-At-Unix: -1",
      "X-State-Retention-Expires-At-Unix: +1",
      "X-State-Retention-Expires-At-Unix: 1.5",
      "X-State-Retention-Expires-At-Unix: 9223372036854775808",
      "X-State-Retention-Supported: 1\r\nX-State-Retention-Supported: 0",
      "X-State-Retention-Expires-At-Unix: "
      "1\r\nX-State-Retention-Expires-At-Unix: 2",
      "X-State-Retention-Supported: 0\r\nX-State-Retention-Expires-At-Unix: 0",
      "X-State-Retention-Expires-At-Unix: 0\r\nX-State-Retention-Supported: 0",
  };
  https_tls_material material;
  size_t i;
  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  for (i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
    const char *headers[] = {"Content-Type: application/json", bad[i]};
    https_expectation expectations[] = {
        {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1",
         NULL, 0, NULL, 0, 1, 200, headers, 2, "{}"},
        {"GET", "/v1/describe?key=resource%2F1&namespace=transport-ns", NULL, 0,
         NULL, 0, 1, 200, headers, 2, "{}"},
    };
    https_testserver server;
    lc_client_config config;
    lc_client *client = NULL;
    lc_get_opts options = {1};
    lc_get_res result = {0};
    lc_describe_req describe = {"transport-ns", "resource/1"};
    lc_describe_res described = {0};
    lc_sink *sink = NULL;
    lc_error error;
    lc_error_init(&error);
    assert_true(https_testserver_start(&server, &material, expectations, 2));
    server.allow_response_write_failure = 1;
    init_public_client_config(&config, server.port, material.client_bundle_path,
                              NULL);
    assert_int_equal(lc_client_open(&config, &client, &error), LC_OK);
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(
        lc_get(client, "resource/1", &options, sink, &result, &error),
        LC_ERR_PROTOCOL);
    lc_sink_close(sink);
    lc_get_res_cleanup(&result);
    lc_error_cleanup(&error);
    assert_int_equal(lc_describe(client, &describe, &described, &error),
                     LC_ERR_PROTOCOL);
    lc_describe_res_cleanup(&described);
    lc_error_cleanup(&error);
    lc_client_close(client);
    https_testserver_stop(&server);
    assert_server_ok(&server);
  }
  https_tls_material_cleanup(&material);
}

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_public_retention_remote_options_and_observations),
      cmocka_unit_test(test_retention_remote_rejects_invalid_headers),
      cmocka_unit_test(test_client_open_rejects_bundle_without_ca),
      cmocka_unit_test(test_client_open_accepts_memory_bundle_source),
      cmocka_unit_test(
          test_client_open_propagates_callback_bundle_source_failure),
      cmocka_unit_test(test_client_open_accepts_fd_bundle_source),
      cmocka_unit_test(
          test_public_client_open_accepts_chunked_callback_bundle_source),
      cmocka_unit_test(test_state_transport_paths_use_mtls),
      cmocka_unit_test(
          test_public_client_namespaced_load_rejects_empty_namespace),
      cmocka_unit_test(test_state_transport_honors_client_cancel),
      cmocka_unit_test(
          test_state_transport_parses_buffered_typed_json_response),
      cmocka_unit_test(
          test_state_transport_accepts_numeric_headers_with_trailing_ows),
      cmocka_unit_test(
          test_state_transport_rejects_invalid_numeric_headers_as_protocol),
      cmocka_unit_test(test_management_transport_paths_use_mtls),
      cmocka_unit_test(test_queue_transport_paths_use_mtls),
      cmocka_unit_test(test_queue_transport_rejects_oversized_error_body),
      cmocka_unit_test(test_watch_transport_rejects_oversized_error_body),
      cmocka_unit_test(
          test_watch_stream_filters_events_and_finishes_trailing_event),
      cmocka_unit_test(test_watch_stream_rejects_malformed_selected_event),
      cmocka_unit_test(test_watch_stream_rejects_oversized_line),
      cmocka_unit_test(
          test_watch_stream_rejects_oversized_event_data_after_prior_event),
      cmocka_unit_test(test_queue_transport_rejects_overflowing_numeric_fields),
      cmocka_unit_test(test_queue_transport_preserves_typed_json_parse_errors),
      cmocka_unit_test(test_subscribe_accepts_content_length_with_trailing_ows),
      cmocka_unit_test(test_subscribe_rejects_oversized_header_line),
      cmocka_unit_test(test_subscribe_rejects_oversized_boundary),
      cmocka_unit_test(test_subscribe_rejects_too_many_headers),
      cmocka_unit_test(
          test_subscribe_rejects_oversized_header_after_completed_delivery),
      cmocka_unit_test(test_subscribe_respects_client_meta_limit),
      cmocka_unit_test(test_subscribe_rejects_default_meta_overflow),
      cmocka_unit_test(test_subscribe_rejects_payload_without_content_length),
      cmocka_unit_test(test_subscribe_rejects_missing_closing_boundary),
      cmocka_unit_test(test_subscribe_propagates_payload_callback_failure),
      cmocka_unit_test(test_subscribe_treats_cancelled_callback_failure_as_ok),
      cmocka_unit_test(test_subscribe_treats_cancelled_finish_failure_as_ok),
      cmocka_unit_test(test_public_client_emits_pslog_messages),
      cmocka_unit_test(test_public_client_can_disable_sdk_sys_field),
      cmocka_unit_test(test_public_bound_lease_methods_emit_logs),
      cmocka_unit_test(
          test_public_lease_attach_rejects_malformed_json_response),
      cmocka_unit_test(
          test_public_lease_attach_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(
          test_engine_request_deadline_bounds_node_passive_failover),
      cmocka_unit_test(
          test_public_attachment_get_preserves_i64_timestamp_headers),
      cmocka_unit_test(
          test_public_lease_attach_rejects_non_rewindable_retry_source),
      cmocka_unit_test(
          test_public_client_update_rejects_non_rewindable_retry_source),
      cmocka_unit_test(
          test_public_lease_update_rejects_non_rewindable_retry_source),
      cmocka_unit_test(
          test_queue_transport_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(
          test_enqueue_from_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(test_enqueue_from_checks_public_metadata_range),
      cmocka_unit_test(test_enqueue_from_rejects_non_rewindable_retry_source),
      cmocka_unit_test(test_public_lease_save_uses_mapped_lonejson_upload),
      cmocka_unit_test(
          test_public_lease_save_ignores_response_limit_for_request_body),
      cmocka_unit_test(test_public_lease_get_refreshes_state_view),
      cmocka_unit_test(
          test_public_client_load_preserves_preinitialized_json_value_capture),
      cmocka_unit_test(
          test_public_client_load_clears_mixed_json_value_mapped_destination),
      cmocka_unit_test(
          test_public_client_load_clears_reused_mapped_destination),
      cmocka_unit_test(test_public_client_load_fails_on_metadata_allocation),
      cmocka_unit_test(
          test_public_lease_load_respects_configured_json_response_limit),
      cmocka_unit_test(
          test_public_client_load_respects_configured_spooled_response_limit),
      cmocka_unit_test(
          test_public_lease_load_parse_failure_does_not_refresh_state_view),
      cmocka_unit_test(
          test_public_lease_load_empty_does_not_refresh_state_view),
      cmocka_unit_test(
          test_public_lease_load_preserves_preinitialized_json_value_capture),
      cmocka_unit_test(test_public_lease_load_fails_on_metadata_allocation),
      cmocka_unit_test(
          test_public_client_load_parse_failure_does_not_log_success),
      cmocka_unit_test(test_public_lease_save_fails_on_state_etag_allocation),
      cmocka_unit_test(test_public_lease_mutate_local_covers_no_content_path),
      cmocka_unit_test(test_public_management_methods_emit_logs),
      cmocka_unit_test(test_public_enqueue_emits_logs),
      cmocka_unit_test(test_public_enqueue_streams_payload_from_source),
      cmocka_unit_test(test_public_dequeue_emits_stream_transport_logs),
      cmocka_unit_test(test_public_query_stream_captures_headers_and_body),
      cmocka_unit_test(test_public_query_keys_streams_chunks_and_headers),
      cmocka_unit_test(test_public_query_keys_captures_body_metadata),
      cmocka_unit_test(test_public_query_keys_accepts_empty_results),
      cmocka_unit_test(
          test_public_query_keys_streams_large_response_without_client_alloc),
      cmocka_unit_test(
          test_public_query_keys_propagates_chunk_callback_failure),
      cmocka_unit_test(test_public_query_keys_propagates_end_callback_failure),
      cmocka_unit_test(test_public_query_keys_rejects_malformed_keys_json),
      cmocka_unit_test(test_public_query_stream_captures_trailers_after_body),
      cmocka_unit_test(test_public_query_stream_trailers_override_headers),
      cmocka_unit_test(
          test_public_query_stream_retries_node_passive_and_cleans_trailers),
      cmocka_unit_test(test_public_query_stream_rejects_invalid_index_seq),
      cmocka_unit_test(
          test_public_query_stream_rejects_invalid_trailer_index_seq),
      cmocka_unit_test(test_public_queue_nack_maps_enum_intents),
      cmocka_unit_test(test_public_queue_nack_rejects_invalid_intent)};
  struct CMUnitTest selected[sizeof(tests) / sizeof(tests[0])];
  size_t count = 0U;
  size_t index;
  int argument;

  for (index = 0U; index < sizeof(tests) / sizeof(tests[0]); ++index) {
    if (argc == 1) {
      selected[count++] = tests[index];
    } else {
      for (argument = 1; argument < argc; ++argument) {
        if (strcmp(argv[argument], tests[index].name) == 0) {
          selected[count++] = tests[index];
          break;
        }
      }
    }
  }
  if (count == 0U || (argc > 1 && count != (size_t)(argc - 1))) {
    fprintf(stderr, "Unknown or duplicate HTTPS test case\n");
    return EXIT_FAILURE;
  }

  /* Several negative streaming cases deliberately close the client after it
   * has detected a malformed response. The in-process TLS server may then
   * observe EPIPE while finishing its fixture response; model that as a write
   * failure rather than terminating the test process with SIGPIPE. */
  if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
    return EXIT_FAILURE;
  return _cmocka_run_group_tests("HTTPS transport", selected, count,
                                 https_tls_material_setup_shared,
                                 https_tls_material_teardown_shared);
}
