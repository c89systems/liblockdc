#include <errno.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_log.h"

/* Preserve malloc's full alignment contract after the tracking prefix. */
typedef union tracked_alloc_header {
  long double alignment;
  size_t size;
} tracked_alloc_header;

typedef struct tracked_allocator_state {
  pthread_mutex_t mutex;
  size_t live_allocations;
  size_t total_allocations;
} tracked_allocator_state;

typedef struct consumer_test_state {
  lc_consumer_service *service;
  pthread_mutex_t mutex;
  size_t expected_messages;
  size_t handled_messages;
  size_t subscribe_calls;
  size_t deliveries_per_subscribe;
  size_t end_callback_rejections;
  size_t end_callback_rejections_without_engine_error;
  size_t ack_calls;
  size_t nack_calls;
  size_t extend_calls;
  size_t close_calls;
  size_t error_events;
  int saw_logger;
  int fail_subscribe;
  int stop_requested;
  int stop_before_end;
  int last_nack_intent;
  int last_error_attempt;
  long last_error_restart_in_ms;
  int last_error_code;
  int handler_mode;
  int extend_should_fail;
  int ack_should_fail;
  int fail_message_factory;
  int subscribe_mode;
  long nack_delay_ms;
  int fail_thread_create_call;
  int thread_create_calls;
  size_t last_client_open_json_limit;
  char last_client_open_pouch_compression[16];
  int track_pthread_primitives;
  int wrapped_mutex_init_calls;
  int wrapped_mutex_destroy_calls;
  int wrapped_cond_init_calls;
  int wrapped_cond_destroy_calls;
  int wrapped_unknown_mutex_destroy_calls;
  int wrapped_unknown_cond_destroy_calls;
  pthread_mutex_t *tracked_mutexes[16];
  size_t tracked_mutex_count;
  pthread_cond_t *tracked_conds[16];
  size_t tracked_cond_count;
  int have_extend_thread;
  int extend_joined;
  int close_saw_extend_joined;
  pthread_t extend_thread;
  long handler_delay_ms;
  FILE *log_fp;
  int last_followup_rc;
  int last_followup_error_code;
  char last_followup_error_message[160];
  size_t pouch_dequeue_calls;
  size_t pouch_dequeue_with_state_calls;
  long last_pouch_wait_seconds;
  size_t stop_after_pouch_dequeue_calls;
} consumer_test_state;

enum {
  CONSUMER_HANDLER_MODE_AUTO_ACK = 0,
  CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_OK = 1,
  CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_ERROR = 2,
  CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_ERROR = 3,
  CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_OK = 4,
  CONSUMER_HANDLER_MODE_AUTO_ACK_AFTER_DELAY = 5,
  CONSUMER_HANDLER_MODE_AUTO_ACK_NO_STOP = 6,
  CONSUMER_HANDLER_MODE_FAIL_ONCE_THEN_SUCCESS = 7,
  CONSUMER_HANDLER_MODE_EXPLICIT_ACK_ERROR_ONCE_THEN_SUCCESS = 8,
  CONSUMER_HANDLER_MODE_FAIL_TWICE_THEN_SUCCESS = 9,
  CONSUMER_HANDLER_MODE_EXPLICIT_ACK_OK = 10,
  CONSUMER_HANDLER_MODE_EXPLICIT_CLOSE_OK = 11,
  CONSUMER_HANDLER_MODE_EXPLICIT_ACK_FAILS_THEN_FOLLOWUP_NACK = 12
};

enum {
  CONSUMER_SUBSCRIBE_MODE_DELIVERY = 0,
  CONSUMER_SUBSCRIBE_MODE_UNTIL_STOP = 1
};

typedef struct fake_delivery_message {
  lc_message pub;
  consumer_test_state *state;
  lc_source *payload;
  const lc_allocator *allocator;
  int *terminal_flag;
  int closed;
} fake_delivery_message;

typedef struct fake_service {
  lc_consumer_service pub;
  int run_calls;
  int start_calls;
  int stop_calls;
  int wait_calls;
  int close_calls;
} fake_service;

typedef struct fake_client {
  lc_client pub;
  int new_consumer_service_calls;
  lc_consumer_service *service_to_return;
} fake_client;

typedef struct direct_subscribe_close_context {
  lc_client *client;
  size_t callback_calls;
} direct_subscribe_close_context;

static consumer_test_state *g_consumer_test_state = NULL;
static pslog_logger *g_test_client_logger = NULL;

static void stop_test_service(consumer_test_state *state);

int __real_lc_client_open(const lc_client_config *config, lc_client **out,
                          lc_error *error);
int __real_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *arg);
int __real_pthread_join(pthread_t thread, void **retval);
int __real_pthread_mutex_init(pthread_mutex_t *mutex,
                              const pthread_mutexattr_t *attr);
int __real_pthread_mutex_destroy(pthread_mutex_t *mutex);
int __real_pthread_cond_init(pthread_cond_t *cond,
                             const pthread_condattr_t *attr);
int __real_pthread_cond_destroy(pthread_cond_t *cond);
lc_message *__real_lc_message_new(lc_client_handle *client,
                                  const lc_engine_dequeue_response *engine,
                                  lc_source *payload, int *terminal_flag);
int __real_lc_engine_client_subscribe(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error);
int __real_lc_engine_client_subscribe_with_state(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error);

struct lc_consumer_service_handle {
  lc_consumer_service pub;
  lc_client_handle *pouch_client;
  char **endpoints;
  size_t endpoint_count;
  char *unix_socket_path;
  unsigned char *client_bundle_bytes;
  size_t client_bundle_length;
  char *client_bundle_path;
  char *default_namespace;
  long timeout_ms;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  size_t http_json_response_limit_bytes;
  char *pouch_crypto_key;
  char *pouch_crypto_key_file;
  char *pouch_compression;
  int pouch_crypto_generate_key_file;
  int pouch_crypto_generate_key_file_set;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
};

static int fake_service_run(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->run_calls += 1;
  return LC_OK;
}

static int fake_service_start(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->start_calls += 1;
  return LC_OK;
}

static int fake_service_stop(lc_consumer_service *self) {
  fake_service *service;
  service = (fake_service *)self;
  service->stop_calls += 1;
  return LC_OK;
}

static int fake_service_wait(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->wait_calls += 1;
  return LC_OK;
}

static void fake_service_close(lc_consumer_service *self) {
  fake_service *service;
  service = (fake_service *)self;
  service->close_calls += 1;
}

static int
fake_client_new_consumer_service(lc_client *self,
                                 const lc_consumer_service_config *config,
                                 lc_consumer_service **out, lc_error *error) {
  fake_client *client;

  (void)config;
  (void)error;
  client = (fake_client *)self;
  client->new_consumer_service_calls += 1;
  *out = client->service_to_return;
  return LC_OK;
}

static void *tracked_malloc(void *context, size_t size) {
  tracked_alloc_header *header;
  tracked_allocator_state *state;

  state = (tracked_allocator_state *)context;
  header = (tracked_alloc_header *)malloc(sizeof(*header) + size);
  if (header == NULL) {
    return NULL;
  }
  header->size = size;
  pthread_mutex_lock(&state->mutex);
  state->live_allocations += 1U;
  state->total_allocations += 1U;
  pthread_mutex_unlock(&state->mutex);
  return (void *)(header + 1);
}

static void tracked_free(void *context, void *ptr) {
  tracked_alloc_header *header;
  tracked_allocator_state *state;

  if (ptr == NULL) {
    return;
  }
  state = (tracked_allocator_state *)context;
  header = ((tracked_alloc_header *)ptr) - 1;
  pthread_mutex_lock(&state->mutex);
  assert_true(state->live_allocations > 0U);
  state->live_allocations -= 1U;
  pthread_mutex_unlock(&state->mutex);
  free(header);
}

static void *tracked_realloc(void *context, void *ptr, size_t size) {
  tracked_alloc_header *header;
  tracked_alloc_header *next;

  if (ptr == NULL) {
    return tracked_malloc(context, size);
  }
  if (size == 0U) {
    tracked_free(context, ptr);
    return NULL;
  }
  header = ((tracked_alloc_header *)ptr) - 1;
  next = (tracked_alloc_header *)realloc(header, sizeof(*header) + size);
  if (next == NULL) {
    return NULL;
  }
  next->size = size;
  return (void *)(next + 1);
}

static void tracked_allocator_state_init(tracked_allocator_state *state) {
  memset(state, 0, sizeof(*state));
  pthread_mutex_init(&state->mutex, NULL);
}

static void tracked_allocator_state_cleanup(tracked_allocator_state *state) {
  pthread_mutex_destroy(&state->mutex);
}

static void tracked_allocator_init(lc_allocator *allocator,
                                   tracked_allocator_state *state) {
  lc_allocator_init(allocator);
  allocator->malloc_fn = tracked_malloc;
  allocator->realloc_fn = tracked_realloc;
  allocator->free_fn = tracked_free;
  allocator->context = state;
}

static void fake_test_client_close(lc_client *self) {
  lc_client_handle *client;

  client = (lc_client_handle *)self;
  lc_engine_client_close(client->engine);
  lc_free_with_allocator(&client->allocator, client);
}

static int fake_pouch_dequeue_common(lc_client *self,
                                     const lc_dequeue_req *request,
                                     lc_message **out, lc_error *error,
                                     int with_state) {
  consumer_test_state *state;
  size_t calls;
  size_t stop_after;

  (void)self;
  (void)error;
  state = g_consumer_test_state;
  assert_non_null(state);
  assert_non_null(request);
  assert_non_null(out);
  *out = NULL;
  pthread_mutex_lock(&state->mutex);
  state->pouch_dequeue_calls += 1U;
  if (with_state) {
    state->pouch_dequeue_with_state_calls += 1U;
  }
  state->last_pouch_wait_seconds = request->wait_seconds;
  calls = state->pouch_dequeue_calls;
  stop_after = state->stop_after_pouch_dequeue_calls;
  pthread_mutex_unlock(&state->mutex);
  if (stop_after != 0U && calls >= stop_after) {
    stop_test_service(state);
  }
  return LC_OK;
}

static int fake_pouch_dequeue(lc_client *self, const lc_dequeue_req *request,
                              lc_message **out, lc_error *error) {
  return fake_pouch_dequeue_common(self, request, out, error, 0);
}

static int fake_pouch_dequeue_with_state(lc_client *self,
                                         const lc_dequeue_req *request,
                                         lc_message **out, lc_error *error) {
  return fake_pouch_dequeue_common(self, request, out, error, 1);
}

static void stop_test_service(consumer_test_state *state) {
  int (*stop_fn)(lc_consumer_service *);

  if (state == NULL || state->service == NULL) {
    return;
  }
  stop_fn = state->service->stop;
  stop_fn(state->service);
}

static void consumer_test_sleep_ms(long delay_ms) {
  if (delay_ms <= 0L) {
    return;
  }
  usleep((useconds_t)(delay_ms * 1000L));
}

static void
fake_delivery_message_close_internal(fake_delivery_message *message) {
  if (message == NULL || message->closed) {
    return;
  }
  message->closed = 1;
  pthread_mutex_lock(&message->state->mutex);
  message->state->close_calls += 1U;
  message->state->close_saw_extend_joined = message->state->extend_joined;
  pthread_mutex_unlock(&message->state->mutex);
  if (message->payload != NULL) {
    message->payload->close(message->payload);
    message->payload = NULL;
  }
  lc_free_with_allocator(message->allocator, message);
}

static int fake_delivery_message_ack(lc_message *self, lc_error *error) {
  fake_delivery_message *message;

  message = (fake_delivery_message *)self;
  pthread_mutex_lock(&message->state->mutex);
  message->state->ack_calls += 1U;
  pthread_mutex_unlock(&message->state->mutex);
  if (message->state->ack_should_fail) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "synthetic ack failure",
                        NULL, NULL, NULL);
  }
  if (message->terminal_flag != NULL) {
    *message->terminal_flag = 1;
  }
  fake_delivery_message_close_internal(message);
  return LC_OK;
}

static int fake_delivery_message_nack(lc_message *self, const lc_nack_req *req,
                                      lc_error *error) {
  fake_delivery_message *message;

  (void)error;
  message = (fake_delivery_message *)self;
  consumer_test_sleep_ms(message->state->nack_delay_ms);
  pthread_mutex_lock(&message->state->mutex);
  message->state->nack_calls += 1U;
  message->state->last_nack_intent = req != NULL ? (int)req->intent : -1;
  pthread_mutex_unlock(&message->state->mutex);
  if (message->terminal_flag != NULL) {
    *message->terminal_flag = 1;
  }
  fake_delivery_message_close_internal(message);
  return LC_OK;
}

static int fake_consumer_on_error_stop_after_first(
    void *context, const lc_consumer_error *event, lc_error *error) {
  consumer_test_state *state;

  (void)error;
  state = (consumer_test_state *)context;
  pthread_mutex_lock(&state->mutex);
  state->error_events += 1U;
  state->last_error_attempt = event != NULL ? event->attempt : -1;
  state->last_error_restart_in_ms = event != NULL ? event->restart_in_ms : -1L;
  state->last_error_code =
      event != NULL && event->cause != NULL ? event->cause->code : LC_OK;
  pthread_mutex_unlock(&state->mutex);
  stop_test_service(state);
  return LC_OK;
}

static int fake_consumer_on_error_record_only(void *context,
                                              const lc_consumer_error *event,
                                              lc_error *error) {
  consumer_test_state *state;

  (void)error;
  state = (consumer_test_state *)context;
  pthread_mutex_lock(&state->mutex);
  state->error_events += 1U;
  state->last_error_attempt = event != NULL ? event->attempt : -1;
  state->last_error_restart_in_ms = event != NULL ? event->restart_in_ms : -1L;
  state->last_error_code =
      event != NULL && event->cause != NULL ? event->cause->code : LC_OK;
  pthread_mutex_unlock(&state->mutex);
  return LC_OK;
}

static int fake_delivery_message_extend(lc_message *self,
                                        const lc_extend_req *req,
                                        lc_error *error) {
  fake_delivery_message *message;

  (void)req;
  message = (fake_delivery_message *)self;
  pthread_mutex_lock(&message->state->mutex);
  message->state->extend_calls += 1U;
  pthread_mutex_unlock(&message->state->mutex);
  if (message->state->extend_should_fail) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "synthetic extend failure",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static lc_lease *fake_delivery_message_state(lc_message *self) {
  (void)self;
  return NULL;
}

static lc_source *fake_delivery_message_payload(lc_message *self) {
  fake_delivery_message *message;

  message = (fake_delivery_message *)self;
  return message->payload;
}

static int fake_delivery_message_rewind(lc_message *self, lc_error *error) {
  fake_delivery_message *message;

  message = (fake_delivery_message *)self;
  if (message->payload == NULL || message->payload->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "fake delivery payload is not rewindable", NULL, NULL,
                        NULL);
  }
  return message->payload->reset(message->payload, error);
}

static int fake_delivery_message_write_payload(lc_message *self, lc_sink *dst,
                                               size_t *written,
                                               lc_error *error) {
  fake_delivery_message *message;
  unsigned char buffer[256];
  size_t total_written;
  size_t chunk_size;
  int rc;

  message = (fake_delivery_message *)self;
  if (message->payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "fake delivery payload is not available", NULL, NULL,
                        NULL);
  }
  total_written = 0U;
  for (;;) {
    chunk_size =
        message->payload->read(message->payload, buffer, sizeof(buffer), error);
    if (error != NULL && error->code != LC_OK) {
      return error->code;
    }
    if (chunk_size == 0U) {
      break;
    }
    rc = dst->write(dst, buffer, chunk_size, error);
    if (rc != LC_OK) {
      return rc;
    }
    total_written += chunk_size;
  }
  if (written != NULL) {
    *written = total_written;
  }
  return LC_OK;
}

static void fake_delivery_message_close(lc_message *self) {
  fake_delivery_message_close_internal((fake_delivery_message *)self);
}

lc_message *__wrap_lc_message_new(lc_client_handle *client,
                                  const lc_engine_dequeue_response *delivery,
                                  lc_source *payload, int *terminal_flag) {
  fake_delivery_message *message;

  if (g_consumer_test_state == NULL) {
    return __real_lc_message_new(client, delivery, payload, terminal_flag);
  }
  if (g_consumer_test_state->fail_message_factory) {
    return NULL;
  }
  message = (fake_delivery_message *)lc_calloc_with_allocator(
      &client->allocator, 1U, sizeof(*message));
  if (message == NULL) {
    return NULL;
  }
  message->state = g_consumer_test_state;
  message->payload = payload;
  message->allocator = &client->allocator;
  message->terminal_flag = terminal_flag;
  message->pub.ack = fake_delivery_message_ack;
  message->pub.nack = fake_delivery_message_nack;
  message->pub.extend = fake_delivery_message_extend;
  message->pub.state = fake_delivery_message_state;
  message->pub.payload_reader = fake_delivery_message_payload;
  message->pub.rewind_payload = fake_delivery_message_rewind;
  message->pub.write_payload = fake_delivery_message_write_payload;
  message->pub.close = fake_delivery_message_close;
  message->pub.ns = delivery->ns;
  message->pub.queue = delivery->queue;
  message->pub.message_id = delivery->message_id;
  message->pub.attempts = delivery->attempts;
  message->pub.max_attempts = delivery->max_attempts;
  message->pub.failure_attempts = delivery->failure_attempts;
  message->pub.not_visible_until_unix = delivery->not_visible_until_unix;
  message->pub.visibility_timeout_seconds =
      delivery->visibility_timeout_seconds;
  message->pub.lease_expires_at_unix = delivery->lease_expires_at_unix;
  message->pub.fencing_token = delivery->fencing_token;
  message->pub.payload = payload;
  return &message->pub;
}

static int handle_consumer_message(void *context, lc_consumer_message *message,
                                   lc_error *error) {
  consumer_test_state *state;
  int (*stop_fn)(lc_consumer_service *);
  int rc;

  state = (consumer_test_state *)context;
  assert_non_null(message->logger);
  pthread_mutex_lock(&state->mutex);
  state->saw_logger = 1;
  pthread_mutex_unlock(&state->mutex);
  rc = message->message->ack(message->message, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_mutex_lock(&state->mutex);
  state->handled_messages += 1U;
  if (state->handled_messages >= state->expected_messages &&
      state->service != NULL) {
    stop_fn = state->service->stop;
    stop_fn(state->service);
  }
  pthread_mutex_unlock(&state->mutex);
  return LC_OK;
}

static int handle_terminal_scenario(void *context, lc_consumer_message *message,
                                    lc_error *error) {
  consumer_test_state *state;
  lc_nack_req nack_req;
  int rc;

  state = (consumer_test_state *)context;
  pthread_mutex_lock(&state->mutex);
  state->handled_messages += 1U;
  pthread_mutex_unlock(&state->mutex);

  switch (state->handler_mode) {
  case CONSUMER_HANDLER_MODE_AUTO_ACK:
    stop_test_service(state);
    return LC_OK;
  case CONSUMER_HANDLER_MODE_AUTO_ACK_NO_STOP:
    return LC_OK;
  case CONSUMER_HANDLER_MODE_AUTO_ACK_AFTER_DELAY:
    consumer_test_sleep_ms(state->handler_delay_ms);
    stop_test_service(state);
    return LC_OK;
  case CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_OK:
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    rc = message->message->nack(message->message, &nack_req, error);
    if (rc == LC_OK) {
      stop_test_service(state);
    }
    return rc;
  case CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_ERROR:
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    rc = message->message->nack(message->message, &nack_req, error);
    if (rc != LC_OK) {
      return rc;
    }
    stop_test_service(state);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "synthetic handler failure", NULL, NULL, NULL);
  case CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_ERROR:
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_DEFER;
    nack_req.delay_seconds = 0L;
    rc = message->message->nack(message->message, &nack_req, error);
    if (rc != LC_OK) {
      return rc;
    }
    stop_test_service(state);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "synthetic handler failure", NULL, NULL, NULL);
  case CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_OK:
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_DEFER;
    nack_req.delay_seconds = 0L;
    rc = message->message->nack(message->message, &nack_req, error);
    if (rc == LC_OK) {
      stop_test_service(state);
    }
    return rc;
  case CONSUMER_HANDLER_MODE_FAIL_ONCE_THEN_SUCCESS:
    if (state->handled_messages == 1U) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "synthetic handler failure", NULL, NULL, NULL);
    }
    stop_test_service(state);
    return LC_OK;
  case CONSUMER_HANDLER_MODE_FAIL_TWICE_THEN_SUCCESS:
    if (state->handled_messages <= 2U) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "synthetic handler failure", NULL, NULL, NULL);
    }
    stop_test_service(state);
    return LC_OK;
  case CONSUMER_HANDLER_MODE_EXPLICIT_ACK_ERROR_ONCE_THEN_SUCCESS:
    if (state->handled_messages == 1U) {
      rc = message->message->ack(message->message, error);
      if (rc != LC_OK) {
        return rc;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "synthetic handler failure after ack", NULL, NULL,
                          NULL);
    }
    stop_test_service(state);
    return LC_OK;
  case CONSUMER_HANDLER_MODE_EXPLICIT_ACK_OK:
    rc = message->message->ack(message->message, error);
    if (rc == LC_OK) {
      stop_test_service(state);
    }
    return rc;
  case CONSUMER_HANDLER_MODE_EXPLICIT_ACK_FAILS_THEN_FOLLOWUP_NACK:
    rc = message->message->ack(message->message, error);
    if (rc != LC_OK) {
      lc_error followup_error;

      lc_error_init(&followup_error);
      lc_nack_req_init(&nack_req);
      nack_req.intent = LC_NACK_INTENT_FAILURE;
      nack_req.delay_seconds = 0L;
      state->last_followup_rc =
          message->message->nack(message->message, &nack_req, &followup_error);
      state->last_followup_error_code = followup_error.code;
      if (followup_error.message != NULL) {
        snprintf(state->last_followup_error_message,
                 sizeof(state->last_followup_error_message), "%s",
                 followup_error.message);
      }
      lc_error_cleanup(&followup_error);
      return rc;
    }
    stop_test_service(state);
    return rc;
  case CONSUMER_HANDLER_MODE_EXPLICIT_CLOSE_OK:
    message->message->close(message->message);
    stop_test_service(state);
    return LC_OK;
  default:
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "unknown consumer handler mode", NULL, NULL, NULL);
  }
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

static void wait_for_subscribe_calls(consumer_test_state *state,
                                     size_t expected_calls) {
  size_t attempt;
  size_t subscribe_calls;

  for (attempt = 0U; attempt < 1000U; ++attempt) {
    pthread_mutex_lock(&state->mutex);
    subscribe_calls = state->subscribe_calls;
    pthread_mutex_unlock(&state->mutex);
    if (subscribe_calls >= expected_calls) {
      return;
    }
    usleep(1000);
  }

  fail_msg("timed out waiting for subscribe calls: expected=%lu actual=%lu",
           (unsigned long)expected_calls, (unsigned long)subscribe_calls);
}

static int fake_subscribe(lc_consumer_service_handle *service,
                          const lc_dequeue_req *request, int with_state,
                          lc_client_handle *client,
                          const lc_engine_queue_stream_handler *handler,
                          void *handler_context,
                          lc_engine_error *engine_error) {
  consumer_test_state *state;
  lc_engine_dequeue_response delivery;
  char message_id[64];
  size_t deliveries;
  size_t delivery_index;
  size_t subscribe_call;
  int accepted;

  (void)service;
  (void)client;
  state = g_consumer_test_state;
  pthread_mutex_lock(&state->mutex);
  state->subscribe_calls += 1U;
  subscribe_call = state->subscribe_calls;
  deliveries = state->deliveries_per_subscribe;
  pthread_mutex_unlock(&state->mutex);
  if (deliveries == 0U) {
    deliveries = 1U;
  }

  if (state->fail_subscribe) {
    lc_engine_error_init(engine_error);
    engine_error->code = LC_ENGINE_ERROR_TRANSPORT;
    engine_error->message = lc_strdup_local("synthetic subscribe failure");
    return LC_ENGINE_ERROR_TRANSPORT;
  }

  for (delivery_index = 0U; delivery_index < deliveries; ++delivery_index) {
    snprintf(message_id, sizeof(message_id), "msg-%lu-%lu",
             (unsigned long)subscribe_call,
             (unsigned long)(delivery_index + 1U));
    memset(&delivery, 0, sizeof(delivery));
    delivery.ns = (char *)request->ns;
    delivery.queue = (char *)request->queue;
    delivery.message_id = message_id;
    delivery.payload_content_type = "application/json";
    delivery.lease_id = "lease-1";
    delivery.meta_etag = "meta-1";
    delivery.fencing_token = 11L;
    delivery.visibility_timeout_seconds = request->visibility_timeout_seconds;
    if (with_state) {
      delivery.state_lease_id = "state-lease-1";
      delivery.state_etag = "state-etag-1";
      delivery.state_fencing_token = 17L;
    }

    accepted = handler->begin(handler_context, &delivery, engine_error);
    if (!accepted) {
      return LC_ENGINE_ERROR_TRANSPORT;
    }
    /* These consumer-service unit tests exercise delivery lifecycle and
     * terminal-action semantics, not payload streaming. Keep the harness
     * payload-less so an immediate ack/defer/nack cannot race a synthetic
     * producer write and turn the test into a stream timing flake. */
    if (state->stop_before_end && state->service != NULL) {
      stop_test_service(state);
    }
    if (!handler->end(handler_context, &delivery, engine_error)) {
      pthread_mutex_lock(&state->mutex);
      state->end_callback_rejections += 1U;
      if (engine_error == NULL || engine_error->code == LC_ENGINE_OK) {
        state->end_callback_rejections_without_engine_error += 1U;
      }
      pthread_mutex_unlock(&state->mutex);
      return LC_ENGINE_ERROR_TRANSPORT;
    }
  }
  return LC_ENGINE_OK;
}

static int fake_subscribe_until_stop(
    lc_consumer_service_handle *service, const lc_dequeue_req *request,
    int with_state, lc_client_handle *client,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *engine_error) {
  consumer_test_state *state;

  (void)service;
  (void)request;
  (void)with_state;
  (void)client;
  (void)handler;
  (void)handler_context;
  (void)engine_error;
  state = g_consumer_test_state;
  pthread_mutex_lock(&state->mutex);
  state->subscribe_calls += 1U;
  pthread_mutex_unlock(&state->mutex);
  for (;;) {
    pthread_mutex_lock(&state->mutex);
    if (state->stop_requested) {
      pthread_mutex_unlock(&state->mutex);
      break;
    }
    pthread_mutex_unlock(&state->mutex);
    usleep(1000);
  }
  return LC_ENGINE_OK;
}

int __wrap_lc_client_open(const lc_client_config *config, lc_client **out,
                          lc_error *error) {
  lc_client_handle *client;
  lc_engine_client_config engine_config;
  lc_engine_error engine_error;
  const char *dummy_endpoint;
  int rc;

  if (g_consumer_test_state == NULL) {
    return __real_lc_client_open(config, out, error);
  }
  assert_true(
      config->pouch_compression == NULL ||
      strlen(config->pouch_compression) <
          sizeof(g_consumer_test_state->last_client_open_pouch_compression));
  snprintf(g_consumer_test_state->last_client_open_pouch_compression,
           sizeof(g_consumer_test_state->last_client_open_pouch_compression),
           "%s",
           config->pouch_compression != NULL ? config->pouch_compression : "");
  client = (lc_client_handle *)lc_calloc_with_allocator(&config->allocator, 1U,
                                                        sizeof(*client));
  if (client == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate fake worker client", NULL, NULL,
                        NULL);
  }
  if (config->endpoint_count == 1U && config->endpoints != NULL &&
      config->endpoints[0] != NULL &&
      strncmp(config->endpoints[0], "pouch:", 6U) == 0) {
    client->allocator = config->allocator;
    client->is_pouch = 1;
    client->base_logger = g_test_client_logger != NULL ? g_test_client_logger
                                                       : lc_log_noop_logger();
    client->logger = g_test_client_logger != NULL ? g_test_client_logger
                                                  : lc_log_noop_logger();
    client->pub.close = fake_test_client_close;
    client->pub.dequeue = fake_pouch_dequeue;
    client->pub.dequeue_with_state = fake_pouch_dequeue_with_state;
    *out = &client->pub;
    return LC_OK;
  }
  lc_engine_client_config_init(&engine_config);
  lc_engine_error_init(&engine_error);
  dummy_endpoint = "https://127.0.0.1:1";
  if (config->endpoint_count == 0U && (config->unix_socket_path == NULL ||
                                       config->unix_socket_path[0] == '\0')) {
    engine_config.endpoints = &dummy_endpoint;
    engine_config.endpoint_count = 1U;
    engine_config.unix_socket_path = NULL;
    engine_config.disable_mtls = 1;
    engine_config.insecure_skip_verify = 1;
  } else {
    engine_config.endpoints = config->endpoints;
    engine_config.endpoint_count = config->endpoint_count;
    engine_config.unix_socket_path = config->unix_socket_path;
    engine_config.disable_mtls = config->disable_mtls;
    engine_config.insecure_skip_verify = config->insecure_skip_verify;
  }
  engine_config.client_bundle_source = config->client_bundle_source;
  engine_config.client_bundle_path = config->client_bundle_path;
  engine_config.default_namespace = config->default_namespace;
  engine_config.timeout_ms = config->timeout_ms;
  engine_config.prefer_http_2 = config->prefer_http_2;
  engine_config.http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  engine_config.logger = g_test_client_logger != NULL ? g_test_client_logger
                                                      : lc_log_noop_logger();
  engine_config.disable_logger_sys_field = config->disable_logger_sys_field;
  engine_config.allocator.malloc_fn = config->allocator.malloc_fn;
  engine_config.allocator.realloc_fn = config->allocator.realloc_fn;
  engine_config.allocator.free_fn = config->allocator.free_fn;
  engine_config.allocator.context = config->allocator.context;
  g_consumer_test_state->last_client_open_json_limit =
      config->http_json_response_limit_bytes;
  rc = lc_engine_client_open(&engine_config, &client->engine, &engine_error);
  lc_engine_error_cleanup(&engine_error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&config->allocator, client);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open fake worker engine", NULL, NULL, NULL);
  }
  client->allocator = config->allocator;
  client->base_logger = g_test_client_logger != NULL ? g_test_client_logger
                                                     : lc_log_noop_logger();
  client->logger = g_test_client_logger != NULL ? g_test_client_logger
                                                : lc_log_noop_logger();
  client->http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  client->pub.close = fake_test_client_close;
  *out = &client->pub;
  return LC_OK;
}

static int
wrap_consumer_engine_subscribe(const lc_engine_dequeue_request *request,
                               const lc_engine_queue_stream_handler *handler,
                               void *handler_context,
                               lc_engine_error *engine_error, int with_state) {
  lc_dequeue_req public_request;

  if (g_consumer_test_state == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  memset(&public_request, 0, sizeof(public_request));
  public_request.ns = request->ns;
  public_request.queue = request->queue;
  public_request.owner = request->owner;
  public_request.txn_id = request->txn_id;
  public_request.visibility_timeout_seconds =
      request->visibility_timeout_seconds;
  public_request.wait_seconds = request->wait_seconds;
  public_request.page_size = request->page_size;
  public_request.start_after = request->start_after;
  if (g_consumer_test_state->subscribe_mode ==
      CONSUMER_SUBSCRIBE_MODE_UNTIL_STOP) {
    return fake_subscribe_until_stop(NULL, &public_request, with_state, NULL,
                                     handler, handler_context, engine_error);
  }
  return fake_subscribe(NULL, &public_request, with_state, NULL, handler,
                        handler_context, engine_error);
}

int __wrap_lc_engine_client_subscribe(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error) {
  if (g_consumer_test_state == NULL) {
    return __real_lc_engine_client_subscribe(client, request, handler,
                                             handler_context, error);
  }
  return wrap_consumer_engine_subscribe(request, handler, handler_context,
                                        error, 0);
}

int __wrap_lc_engine_client_subscribe_with_state(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error) {
  if (g_consumer_test_state == NULL) {
    return __real_lc_engine_client_subscribe_with_state(
        client, request, handler, handler_context, error);
  }
  return wrap_consumer_engine_subscribe(request, handler, handler_context,
                                        error, 1);
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *arg) {
  consumer_test_state *state;
  int rc;

  state = g_consumer_test_state;
  if (state == NULL) {
    return __real_pthread_create(thread, attr, start_routine, arg);
  }
  pthread_mutex_lock(&state->mutex);
  state->thread_create_calls += 1;
  if (state->fail_thread_create_call > 0 &&
      state->thread_create_calls == state->fail_thread_create_call) {
    pthread_mutex_unlock(&state->mutex);
    return EAGAIN;
  }
  pthread_mutex_unlock(&state->mutex);
  rc = __real_pthread_create(thread, attr, start_routine, arg);
  if (rc == 0) {
    pthread_mutex_lock(&state->mutex);
    if (state->thread_create_calls == 3) {
      state->extend_thread = *thread;
      state->have_extend_thread = 1;
    }
    pthread_mutex_unlock(&state->mutex);
  }
  return rc;
}

int __wrap_pthread_join(pthread_t thread, void **retval) {
  consumer_test_state *state;

  state = g_consumer_test_state;
  if (state != NULL) {
    pthread_mutex_lock(&state->mutex);
    if (state->have_extend_thread &&
        pthread_equal(state->extend_thread, thread)) {
      state->extend_joined = 1;
    }
    pthread_mutex_unlock(&state->mutex);
  }
  return __real_pthread_join(thread, retval);
}

int __wrap_pthread_mutex_init(pthread_mutex_t *mutex,
                              const pthread_mutexattr_t *attr) {
  consumer_test_state *state;
  size_t index;

  state = g_consumer_test_state;
  if (state != NULL && state->track_pthread_primitives) {
    pthread_mutex_lock(&state->mutex);
    state->wrapped_mutex_init_calls += 1;
    for (index = 0U; index < state->tracked_mutex_count; ++index) {
      if (state->tracked_mutexes[index] == mutex) {
        pthread_mutex_unlock(&state->mutex);
        return __real_pthread_mutex_init(mutex, attr);
      }
    }
    assert_true(state->tracked_mutex_count <
                sizeof(state->tracked_mutexes) /
                    sizeof(state->tracked_mutexes[0]));
    state->tracked_mutexes[state->tracked_mutex_count++] = mutex;
    pthread_mutex_unlock(&state->mutex);
  }
  return __real_pthread_mutex_init(mutex, attr);
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *mutex) {
  consumer_test_state *state;
  size_t index;
  int found;

  state = g_consumer_test_state;
  if (state != NULL && state->track_pthread_primitives) {
    pthread_mutex_lock(&state->mutex);
    state->wrapped_mutex_destroy_calls += 1;
    found = 0;
    for (index = 0U; index < state->tracked_mutex_count; ++index) {
      if (state->tracked_mutexes[index] == mutex) {
        state->tracked_mutex_count -= 1U;
        state->tracked_mutexes[index] =
            state->tracked_mutexes[state->tracked_mutex_count];
        state->tracked_mutexes[state->tracked_mutex_count] = NULL;
        found = 1;
        break;
      }
    }
    if (!found) {
      state->wrapped_unknown_mutex_destroy_calls += 1;
    }
    pthread_mutex_unlock(&state->mutex);
  }
  return __real_pthread_mutex_destroy(mutex);
}

int __wrap_pthread_cond_init(pthread_cond_t *cond,
                             const pthread_condattr_t *attr) {
  consumer_test_state *state;
  size_t index;

  state = g_consumer_test_state;
  if (state != NULL && state->track_pthread_primitives) {
    pthread_mutex_lock(&state->mutex);
    state->wrapped_cond_init_calls += 1;
    for (index = 0U; index < state->tracked_cond_count; ++index) {
      if (state->tracked_conds[index] == cond) {
        pthread_mutex_unlock(&state->mutex);
        return __real_pthread_cond_init(cond, attr);
      }
    }
    assert_true(state->tracked_cond_count <
                sizeof(state->tracked_conds) / sizeof(state->tracked_conds[0]));
    state->tracked_conds[state->tracked_cond_count++] = cond;
    pthread_mutex_unlock(&state->mutex);
  }
  return __real_pthread_cond_init(cond, attr);
}

int __wrap_pthread_cond_destroy(pthread_cond_t *cond) {
  consumer_test_state *state;
  size_t index;
  int found;

  state = g_consumer_test_state;
  if (state != NULL && state->track_pthread_primitives) {
    pthread_mutex_lock(&state->mutex);
    state->wrapped_cond_destroy_calls += 1;
    found = 0;
    for (index = 0U; index < state->tracked_cond_count; ++index) {
      if (state->tracked_conds[index] == cond) {
        state->tracked_cond_count -= 1U;
        state->tracked_conds[index] =
            state->tracked_conds[state->tracked_cond_count];
        state->tracked_conds[state->tracked_cond_count] = NULL;
        found = 1;
        break;
      }
    }
    if (!found) {
      state->wrapped_unknown_cond_destroy_calls += 1;
    }
    pthread_mutex_unlock(&state->mutex);
  }
  return __real_pthread_cond_destroy(cond);
}

static void init_fake_root_client(lc_client_handle *client,
                                  const lc_allocator *allocator) {
  memset(client, 0, sizeof(*client));
  client->allocator = *allocator;
  client->default_namespace = "default";
  client->base_logger = lc_log_noop_logger();
  client->logger = lc_log_noop_logger();
}

static void test_consumer_restart_policy_init_sets_defaults(void **state) {
  lc_consumer_restart_policy policy;

  (void)state;
  memset(&policy, 0xff, sizeof(policy));
  lc_consumer_restart_policy_init(&policy);
  assert_int_equal(policy.immediate_retries, 3);
  assert_int_equal(policy.base_delay_ms, 250L);
  assert_int_equal(policy.max_delay_ms, 300000L);
  assert_true(policy.multiplier > 1.0);
  assert_int_equal(policy.jitter_ms, 0L);
  assert_int_equal(policy.max_failures, 0);
}

static void test_consumer_config_inits_are_zero_safe(void **state) {
  lc_consumer_config config;
  lc_consumer_service_config service_config;

  (void)state;
  memset(&config, 0xff, sizeof(config));
  memset(&service_config, 0xff, sizeof(service_config));
  lc_consumer_config_init(&config);
  lc_consumer_service_config_init(&service_config);
  assert_null(config.name);
  assert_null(config.request.queue);
  assert_int_equal(config.worker_count, 1U);
  assert_int_equal(config.with_state, 0);
  assert_null(config.handle);
  assert_int_equal(config.restart_policy.immediate_retries, 3);
  assert_null(service_config.consumers);
  assert_int_equal(service_config.consumer_count, 0U);
}

static void test_consumer_service_wrappers_delegate(void **state) {
  fake_service service;
  fake_client client;
  lc_consumer_service_config config;
  lc_consumer_service *out;
  lc_error error;
  int rc;

  (void)state;
  memset(&service, 0, sizeof(service));
  memset(&client, 0, sizeof(client));
  lc_consumer_service_config_init(&config);
  out = NULL;
  lc_error_init(&error);

  service.pub.run = fake_service_run;
  service.pub.start = fake_service_start;
  service.pub.stop = fake_service_stop;
  service.pub.wait = fake_service_wait;
  service.pub.close = fake_service_close;
  client.pub.new_consumer_service = fake_client_new_consumer_service;
  client.service_to_return = &service.pub;

  rc = lc_client_new_consumer_service(&client.pub, &config, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(client.new_consumer_service_calls, 1);
  assert_ptr_equal(out, &service.pub);

  rc = lc_consumer_service_run(out, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_start(out, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_stop(out);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_wait(out, &error);
  assert_int_equal(rc, LC_OK);
  lc_consumer_service_close(out);

  assert_int_equal(service.run_calls, 1);
  assert_int_equal(service.start_calls, 1);
  assert_int_equal(service.stop_calls, 1);
  assert_int_equal(service.wait_calls, 1);
  assert_int_equal(service.close_calls, 1);
  lc_error_cleanup(&error);
}

static void
test_consumer_service_multi_worker_releases_tracked_allocations(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 3U;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service->close(service);
  assert_int_equal(error.code, LC_OK);
  assert_true(alloc_state.total_allocations > 0U);
  assert_int_equal(alloc_state.live_allocations, 0U);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_rejects_worker_count_overflow(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumers[2];
  lc_consumer_service_config config;
  lc_consumer_service *service;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  memset(consumers, 0, sizeof(consumers));
  lc_consumer_config_init(&consumers[0]);
  lc_consumer_config_init(&consumers[1]);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);

  consumers[0].name = "worker-a";
  consumers[0].request.queue = "jobs-a";
  consumers[0].handle = handle_consumer_message;
  consumers[0].worker_count = SIZE_MAX;
  consumers[1].name = "worker-b";
  consumers[1].request.queue = "jobs-b";
  consumers[1].handle = handle_consumer_message;
  consumers[1].worker_count = 1U;
  config.consumers = consumers;
  config.consumer_count = 2U;
  service = NULL;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(service);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_int_equal(alloc_state.live_allocations, 0U);

  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_pouch_dequeue_wait_is_shutdown_bounded(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  char *endpoints[1];
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  endpoints[0] = "pouch:///tmp/liblockdc-consumer-service-pouch-test";
  client.endpoints = endpoints;
  client.endpoint_count = 1U;
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.stop_after_pouch_dequeue_calls = 1U;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.wait_seconds = LONG_MAX;
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.pouch_dequeue_calls, 1U);
  assert_int_equal(runtime_state.pouch_dequeue_with_state_calls, 0U);
  assert_int_equal(runtime_state.last_pouch_wait_seconds, 1L);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_multi_worker_runs_without_transport(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 3U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  runtime_state.subscribe_mode = CONSUMER_SUBSCRIBE_MODE_UNTIL_STOP;
  g_consumer_test_state = &runtime_state;

  rc = service->start(service, &error);
  if (rc != LC_OK) {
    fail_msg("service start failed rc=%d err=%d msg=%s", rc, error.code,
             error.message != NULL ? error.message : "(null)");
  }
  wait_for_subscribe_calls(&runtime_state, 3U);
  pthread_mutex_lock(&runtime_state.mutex);
  runtime_state.stop_requested = 1;
  pthread_mutex_unlock(&runtime_state.mutex);
  {
    int (*stop_fn)(lc_consumer_service *);
    stop_fn = service->stop;
    rc = stop_fn(service);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  if (rc != LC_OK) {
    fail_msg("service wait failed rc=%d err=%d msg=%s", rc, error.code,
             error.message != NULL ? error.message : "(null)");
  }
  assert_true(runtime_state.subscribe_calls >= 3U);

  service->close(service);
  g_consumer_test_state = NULL;
  assert_true(alloc_state.total_allocations > 0U);
  assert_int_equal(alloc_state.live_allocations, 0U);

  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_releases_each_delivery_in_single_subscribe_stream(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.expected_messages = 2U;
  runtime_state.deliveries_per_subscribe = 2U;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_OK);
  assert_int_equal(runtime_state.subscribe_calls, 1U);
  assert_int_equal(runtime_state.handled_messages, 2U);
  assert_int_equal(runtime_state.ack_calls, 2U);
  assert_int_equal(runtime_state.close_calls, 2U);

  service->close(service);
  g_consumer_test_state = NULL;
  assert_true(alloc_state.total_allocations > 0U);
  assert_int_equal(alloc_state.live_allocations, 0U);

  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_logs_restart_with_configured_logger(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  pslog_logger *logger;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  logs = NULL;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  logger = open_test_logger(&runtime_state.log_fp);
  assert_non_null(logger);
  client.base_logger = logger;
  client.logger = lc_log_client_logger(logger, 0);
  assert_non_null(client.logger);
  g_test_client_logger = client.logger;
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 2;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  g_test_client_logger = NULL;
  client.logger->destroy(client.logger);
  logger->destroy(logger);
  logs = read_stream_text(runtime_state.log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"consumer.restart\""));
  assert_non_null(strstr(logs, "\"consumer\":\"worker-test\""));
  assert_non_null(strstr(logs, "\"queue\":\"jobs\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_null(strstr(logs, "\"component\""));
  assert_null(strstr(logs, "\"subsystem\""));

  free(logs);
  fclose(runtime_state.log_fp);
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_logs_subscribe_lifecycle(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  pslog_logger *logger;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  logs = NULL;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  logger = open_test_logger(&runtime_state.log_fp);
  assert_non_null(logger);
  client.base_logger = logger;
  client.logger = lc_log_client_logger(logger, 0);
  assert_non_null(client.logger);
  g_test_client_logger = client.logger;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.owner = "worker-owner";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  runtime_state.subscribe_mode = CONSUMER_SUBSCRIBE_MODE_UNTIL_STOP;
  g_consumer_test_state = &runtime_state;

  rc = service->start(service, &error);
  assert_int_equal(rc, LC_OK);
  usleep(10000);
  pthread_mutex_lock(&runtime_state.mutex);
  runtime_state.stop_requested = 1;
  pthread_mutex_unlock(&runtime_state.mutex);
  {
    int (*stop_fn)(lc_consumer_service *);

    stop_fn = service->stop;
    rc = stop_fn(service);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  assert_int_equal(rc, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  g_test_client_logger = NULL;
  client.logger->destroy(client.logger);
  logger->destroy(logger);
  logs = read_stream_text(runtime_state.log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"queue.subscribe.begin\""));
  assert_non_null(strstr(logs, "\"consumer\":\"worker-test\""));
  assert_non_null(strstr(logs, "\"queue\":\"jobs\""));
  assert_non_null(strstr(logs, "\"owner\":\"worker-owner\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_null(strstr(logs, "\"component\""));
  assert_null(strstr(logs, "\"subsystem\""));

  free(logs);
  fclose(runtime_state.log_fp);
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_retains_owned_logger_after_root_client_close(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_service_config config;
  lc_consumer_config consumer;
  lc_consumer_service *service;
  lc_consumer_service_handle *service_handle;
  pslog_logger *base_logger;
  FILE *log_fp;
  char *logs;
  lc_error error;
  int rc;

  (void)state;
  service = NULL;
  base_logger = NULL;
  log_fp = NULL;
  logs = NULL;
  memset(&client, 0, sizeof(client));
  memset(&config, 0, sizeof(config));
  memset(&consumer, 0, sizeof(consumer));
  memset(&error, 0, sizeof(error));
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  init_fake_root_client(&client, &allocator);

  base_logger = open_test_logger(&log_fp);
  assert_non_null(base_logger);
  client.base_logger = base_logger;
  client.logger = lc_log_client_logger(base_logger, 0);
  assert_non_null(client.logger);

  lc_consumer_service_config_init(&config);
  lc_consumer_config_init(&consumer);
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service_handle = (lc_consumer_service_handle *)service;
  assert_ptr_equal(service_handle->base_logger, base_logger);
  assert_non_null(service_handle->logger);
  assert_ptr_not_equal(service_handle->logger, client.logger);

  client.logger->destroy(client.logger);
  client.logger = NULL;
  lc_log_info(service_handle->logger, "consumer.service.logger.still.valid",
              NULL, 0U);

  lc_consumer_service_close(service);
  base_logger->destroy(base_logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(
      strstr(logs, "\"message\":\"consumer.service.logger.still.valid\""));

  free(logs);
  fclose(log_fp);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_multi_worker_failure_cleans_up(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 2U;
  consumer.context = &runtime_state;
  consumer.restart_policy.max_failures = 2;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_true(error.code != LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  assert_int_equal(alloc_state.live_allocations, 0U);

  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void run_consumer_terminal_scenario_unit_test(
    int handler_mode, int expected_run_rc, size_t expected_ack_calls,
    size_t expected_nack_calls, size_t expected_extend_calls,
    int expected_nack_intent, int extend_should_fail, long handler_delay_ms) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = handler_mode;
  runtime_state.last_nack_intent = -1;
  runtime_state.extend_should_fail = extend_should_fail;
  runtime_state.handler_delay_ms = handler_delay_ms;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  if (expected_run_rc == LC_OK) {
    assert_int_equal(rc, LC_OK);
  } else {
    assert_int_not_equal(rc, LC_OK);
  }
  assert_int_equal(runtime_state.ack_calls, expected_ack_calls);
  assert_int_equal(runtime_state.nack_calls, expected_nack_calls);
  assert_int_equal(runtime_state.extend_calls, expected_extend_calls);
  assert_int_equal(
      runtime_state.close_calls,
      expected_ack_calls + expected_nack_calls +
          (handler_mode == CONSUMER_HANDLER_MODE_EXPLICIT_CLOSE_OK ? 1U : 0U));
  assert_int_equal(runtime_state.handled_messages, 1U);
  assert_int_equal(runtime_state.last_nack_intent, expected_nack_intent);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_auto_acks_open_delivery_on_success(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(CONSUMER_HANDLER_MODE_AUTO_ACK,
                                           LC_OK, 1U, 0U, 0U, -1, 0, 0L);
}

static void
test_consumer_service_preserves_explicit_failure_nack_on_success(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_OK, LC_OK, 0U, 1U, 0U,
      LC_NACK_INTENT_FAILURE, 0, 0L);
}

static void
test_consumer_service_preserves_explicit_ack_on_success(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_ACK_OK, LC_OK, 1U, 0U, 0U, -1, 0, 0L);
}

static void
test_consumer_service_preserves_explicit_close_on_success(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_CLOSE_OK, LC_OK, 0U, 0U, 0U, -1, 0, 0L);
}

static void
test_consumer_service_explicit_failure_nack_blocks_extender_race(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_OK;
  runtime_state.last_nack_intent = -1;
  runtime_state.extend_should_fail = 1;
  runtime_state.nack_delay_ms = 600L;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.nack_calls, 1U);
  assert_int_equal(runtime_state.extend_calls, 0U);
  assert_int_equal(runtime_state.error_events, 0U);
  assert_int_equal(runtime_state.close_calls, 1U);
  assert_int_equal(runtime_state.last_nack_intent, LC_NACK_INTENT_FAILURE);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_does_not_double_nack_explicit_failure_handler_error(
    void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_FAILURE_NACK_ERROR, LC_OK, 0U, 1U, 0U,
      LC_NACK_INTENT_FAILURE, 0, 0L);
}

static void
test_consumer_service_does_not_double_nack_explicit_defer_handler_error(
    void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_ERROR, LC_OK, 0U, 1U, 0U,
      LC_NACK_INTENT_DEFER, 0, 0L);
}

static void
test_consumer_service_preserves_explicit_defer_on_success(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_EXPLICIT_DEFER_OK, LC_OK, 0U, 1U, 0U,
      LC_NACK_INTENT_DEFER, 0, 0L);
}

static void
test_consumer_service_skips_auto_ack_after_extend_failure(void **state) {
  (void)state;
  run_consumer_terminal_scenario_unit_test(
      CONSUMER_HANDLER_MODE_AUTO_ACK_AFTER_DELAY, LC_OK, 0U, 1U, 1U,
      LC_NACK_INTENT_FAILURE, 1, 1500L);
}

static void test_consumer_service_shutdown_stops_extender_before_message_close(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = CONSUMER_HANDLER_MODE_AUTO_ACK_AFTER_DELAY;
  runtime_state.handler_delay_ms = 1500L;
  runtime_state.last_nack_intent = -1;
  runtime_state.ack_should_fail = 1;
  runtime_state.stop_before_end = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.close_calls, 1U);
  assert_int_equal(runtime_state.close_saw_extend_joined, 1);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_preserves_auto_ack_failure_details(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = CONSUMER_HANDLER_MODE_AUTO_ACK_NO_STOP;
  runtime_state.last_nack_intent = -1;
  runtime_state.ack_should_fail = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_stop_after_first;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.ack_calls, 1U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.close_calls, 1U);
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.end_callback_rejections, 1U);
  assert_int_equal(runtime_state.end_callback_rejections_without_engine_error,
                   0U);
  assert_int_equal(runtime_state.last_error_attempt, 0);
  assert_int_equal(runtime_state.last_error_restart_in_ms, 0L);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_handler_failure_does_not_consume_failure_budget(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = CONSUMER_HANDLER_MODE_FAIL_ONCE_THEN_SUCCESS;
  runtime_state.last_nack_intent = -1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  if (runtime_state.handled_messages != 2U) {
    fail_msg("handled=%lu subscribe=%lu errors=%lu ack=%lu nack=%lu code=%d",
             (unsigned long)runtime_state.handled_messages,
             (unsigned long)runtime_state.subscribe_calls,
             (unsigned long)runtime_state.error_events,
             (unsigned long)runtime_state.ack_calls,
             (unsigned long)runtime_state.nack_calls, error.code);
  }
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.last_error_attempt, 0);
  assert_int_equal(runtime_state.last_error_restart_in_ms, 0L);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(runtime_state.nack_calls, 1U);
  assert_int_equal(runtime_state.ack_calls, 1U);
  assert_int_equal(error.code, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_repeated_handler_failures_do_not_consume_failure_budget(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode = CONSUMER_HANDLER_MODE_FAIL_TWICE_THEN_SUCCESS;
  runtime_state.last_nack_intent = -1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.handled_messages, 3U);
  assert_int_equal(runtime_state.error_events, 2U);
  assert_int_equal(runtime_state.last_error_attempt, 0);
  assert_int_equal(runtime_state.last_error_restart_in_ms, 0L);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(runtime_state.nack_calls, 2U);
  assert_int_equal(runtime_state.ack_calls, 1U);
  assert_int_equal(error.code, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_explicit_ack_then_handler_error_surfaces_without_nack(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode =
      CONSUMER_HANDLER_MODE_EXPLICIT_ACK_ERROR_ONCE_THEN_SUCCESS;
  runtime_state.last_nack_intent = -1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.handled_messages, 2U);
  assert_int_equal(runtime_state.ack_calls, 2U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.end_callback_rejections, 1U);
  assert_int_equal(runtime_state.end_callback_rejections_without_engine_error,
                   0U);
  assert_int_equal(runtime_state.last_error_attempt, 0);
  assert_int_equal(runtime_state.last_error_restart_in_ms, 0L);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_explicit_ack_failure_marks_terminal_outcome_indeterminate(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.handler_mode =
      CONSUMER_HANDLER_MODE_EXPLICIT_ACK_FAILS_THEN_FOLLOWUP_NACK;
  runtime_state.last_nack_intent = -1;
  runtime_state.ack_should_fail = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.visibility_timeout_seconds = 1L;
  consumer.handle = handle_terminal_scenario;
  consumer.on_error = fake_consumer_on_error_stop_after_first;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(runtime_state.ack_calls, 1U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.last_followup_rc, LC_ERR_INVALID);
  assert_int_equal(runtime_state.last_followup_error_code, LC_ERR_INVALID);
  assert_non_null(strstr(runtime_state.last_followup_error_message,
                         "terminal outcome is indeterminate"));
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_subscribe_failures_consume_service_failure_budget(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 2;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.subscribe_calls, 3U);
  assert_int_equal(runtime_state.error_events, 2U);
  assert_int_equal(runtime_state.last_error_attempt, 2);
  assert_true(runtime_state.last_error_restart_in_ms >= 0L);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(runtime_state.ack_calls, 0U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_handler_thread_start_failure_is_fatal(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_thread_create_call = 2;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.thread_create_calls, 2);
  assert_int_equal(runtime_state.ack_calls, 0U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.close_calls, 1U);
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.last_error_attempt, 1);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_worker_thread_start_failure_is_fatal(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_thread_create_call = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.thread_create_calls, 1);
  assert_int_equal(runtime_state.ack_calls, 0U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.close_calls, 0U);
  assert_int_equal(runtime_state.error_events, 0U);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_extender_thread_start_failure_is_fatal(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_thread_create_call = 3;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.thread_create_calls, 3);
  assert_int_equal(runtime_state.ack_calls, 1U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.close_calls, 1U);
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.last_error_attempt, 1);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_message_factory_failure_is_fatal(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_message_factory = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.ack_calls, 0U);
  assert_int_equal(runtime_state.nack_calls, 0U);
  assert_int_equal(runtime_state.close_calls, 0U);
  assert_int_equal(runtime_state.error_events, 1U);
  assert_int_equal(runtime_state.last_error_attempt, 1);
  assert_int_equal(runtime_state.last_error_code, LC_ERR_NOMEM);
  assert_int_equal(error.code, LC_ERR_NOMEM);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_worker_clone_preserves_json_response_limit(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  client.http_json_response_limit_bytes = 123U;
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 2;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.last_client_open_json_limit, 123U);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_outbox_dispatcher_clone_uses_standard_json_response_limit(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_client *dispatcher_client;
  consumer_test_state runtime_state;
  lc_error error;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  client.http_json_response_limit_bytes = 1U;
  client.timeout_ms = 123L;
  memset(&runtime_state, 0, sizeof(runtime_state));
  assert_int_equal(pthread_mutex_init(&runtime_state.mutex, NULL), 0);
  lc_error_init(&error);
  dispatcher_client = NULL;
  g_consumer_test_state = &runtime_state;

  assert_int_equal(lc_client_clone_remote_for_outbox(
                       &client, 456L, NULL, &dispatcher_client, &error),
                   LC_OK);
  assert_non_null(dispatcher_client);
  assert_int_equal(runtime_state.last_client_open_json_limit,
                   LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT);
  assert_int_equal(
      ((lc_client_handle *)dispatcher_client)->http_json_response_limit_bytes,
      LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT);

  dispatcher_client->close(dispatcher_client);
  g_consumer_test_state = NULL;
  lc_error_cleanup(&error);
  assert_int_equal(pthread_mutex_destroy(&runtime_state.mutex), 0);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_worker_clone_preserves_pouch_compression(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  char *endpoints[1];
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  endpoints[0] = "pouch:///tmp/liblockdc-consumer-service-compression-test";
  client.endpoints = endpoints;
  client.endpoint_count = 1U;
  client.pouch_compression = "zlib";
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.stop_after_pouch_dequeue_calls = 1U;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(runtime_state.last_client_open_pouch_compression, "zlib");

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_no_delivery_does_not_destroy_uninitialized_primitives(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_subscribe = 1;
  runtime_state.track_pthread_primitives = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.on_error = fake_consumer_on_error_record_only;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 2;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(runtime_state.wrapped_unknown_mutex_destroy_calls, 0);
  assert_int_equal(runtime_state.wrapped_unknown_cond_destroy_calls, 0);

  service->close(service);
  g_consumer_test_state = NULL;
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static int direct_subscribe_close_handler(void *context, lc_message *message,
                                          lc_error *error) {
  direct_subscribe_close_context *close_context;

  close_context = (direct_subscribe_close_context *)context;
  close_context->callback_calls += 1U;
  /* The root close is deliberately reentrant with the native subscriber. The
   * subscription frame must retain the concrete client until its handler
   * thread has joined and every final log/error path has unwound. */
  lc_client_close(close_context->client);
  return lc_message_ack(message, error);
}

static void
test_direct_subscribe_retains_client_across_callback_close(void **state) {
  consumer_test_state runtime_state;
  direct_subscribe_close_context close_context;
  lc_allocator allocator;
  lc_client_handle *client;
  lc_consumer consumer;
  lc_dequeue_req request;
  lc_error error;
  int rc;

  (void)state;
  memset(&runtime_state, 0, sizeof(runtime_state));
  memset(&close_context, 0, sizeof(close_context));
  lc_allocator_init(&allocator);
  client = (lc_client_handle *)lc_calloc_with_allocator(&allocator, 1U,
                                                        sizeof(*client));
  assert_non_null(client);
  client->allocator = allocator;
  client->base_logger = lc_log_noop_logger();
  client->logger = lc_log_noop_logger();
  client->refcount = 1U;
  assert_int_equal(pthread_mutex_init(&client->lifecycle_mutex, NULL), 0);
  client->lifecycle_mutex_initialized = 1;
  client->pub.subscribe = lc_client_subscribe_method;
  client->pub.close = lc_client_close_method;
  close_context.client = &client->pub;
  lc_consumer_init(&consumer);
  consumer.handle = direct_subscribe_close_handler;
  consumer.context = &close_context;
  lc_dequeue_req_init(&request);
  request.ns = "callback-close";
  request.queue = "jobs";
  request.owner = "worker";
  request.visibility_timeout_seconds = 30L;
  request.wait_seconds = 1L;
  request.page_size = 1;
  lc_error_init(&error);
  assert_int_equal(pthread_mutex_init(&runtime_state.mutex, NULL), 0);
  g_consumer_test_state = &runtime_state;

  rc = lc_subscribe(&client->pub, &request, &consumer, &error);

  g_consumer_test_state = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(close_context.callback_calls, 1U);
  assert_int_equal(runtime_state.ack_calls, 1U);
  lc_error_cleanup(&error);
  pthread_mutex_destroy(&runtime_state.mutex);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_consumer_restart_policy_init_sets_defaults),
      cmocka_unit_test(test_consumer_config_inits_are_zero_safe),
      cmocka_unit_test(test_consumer_service_wrappers_delegate),
      cmocka_unit_test(
          test_consumer_service_multi_worker_releases_tracked_allocations),
      cmocka_unit_test(test_consumer_service_rejects_worker_count_overflow),
      cmocka_unit_test(
          test_consumer_service_pouch_dequeue_wait_is_shutdown_bounded),
      cmocka_unit_test(
          test_consumer_service_multi_worker_runs_without_transport),
      cmocka_unit_test(
          test_consumer_service_releases_each_delivery_in_single_subscribe_stream),
      cmocka_unit_test(test_consumer_service_multi_worker_failure_cleans_up),
      cmocka_unit_test(
          test_consumer_service_auto_acks_open_delivery_on_success),
      cmocka_unit_test(test_consumer_service_preserves_explicit_ack_on_success),
      cmocka_unit_test(
          test_consumer_service_preserves_explicit_close_on_success),
      cmocka_unit_test(
          test_consumer_service_preserves_explicit_failure_nack_on_success),
      cmocka_unit_test(
          test_consumer_service_explicit_failure_nack_blocks_extender_race),
      cmocka_unit_test(
          test_consumer_service_does_not_double_nack_explicit_failure_handler_error),
      cmocka_unit_test(
          test_consumer_service_does_not_double_nack_explicit_defer_handler_error),
      cmocka_unit_test(
          test_consumer_service_preserves_explicit_defer_on_success),
      cmocka_unit_test(
          test_consumer_service_skips_auto_ack_after_extend_failure),
      cmocka_unit_test(
          test_consumer_service_shutdown_stops_extender_before_message_close),
      cmocka_unit_test(
          test_consumer_service_preserves_auto_ack_failure_details),
      cmocka_unit_test(
          test_consumer_service_handler_failure_does_not_consume_failure_budget),
      cmocka_unit_test(
          test_consumer_service_repeated_handler_failures_do_not_consume_failure_budget),
      cmocka_unit_test(
          test_consumer_service_explicit_ack_then_handler_error_surfaces_without_nack),
      cmocka_unit_test(
          test_consumer_service_explicit_ack_failure_marks_terminal_outcome_indeterminate),
      cmocka_unit_test(
          test_consumer_service_subscribe_failures_consume_service_failure_budget),
      cmocka_unit_test(
          test_consumer_service_worker_thread_start_failure_is_fatal),
      cmocka_unit_test(
          test_consumer_service_handler_thread_start_failure_is_fatal),
      cmocka_unit_test(
          test_consumer_service_extender_thread_start_failure_is_fatal),
      cmocka_unit_test(test_consumer_service_message_factory_failure_is_fatal),
      cmocka_unit_test(
          test_consumer_service_worker_clone_preserves_json_response_limit),
      cmocka_unit_test(
          test_outbox_dispatcher_clone_uses_standard_json_response_limit),
      cmocka_unit_test(
          test_consumer_service_worker_clone_preserves_pouch_compression),
      cmocka_unit_test(
          test_consumer_service_no_delivery_does_not_destroy_uninitialized_primitives),
      cmocka_unit_test(
          test_consumer_service_retains_owned_logger_after_root_client_close),
      cmocka_unit_test(
          test_consumer_service_logs_restart_with_configured_logger),
      cmocka_unit_test(test_consumer_service_logs_subscribe_lifecycle),
      cmocka_unit_test(
          test_direct_subscribe_retains_client_across_callback_close),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
