#include "lc/lc.h"
#include "pslog.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_QUEUE_PREFIX "example-startconsumer"
#define EXAMPLE_CONSUMER_NAME "ackOnThree"
#define EXAMPLE_OWNER "example-startconsumer"
#define EXAMPLE_PAYLOAD "{\"hello\":\"world\"}"

typedef struct startconsumer_context {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pslog_logger *example_logger;
  lc_consumer_service *service;
  int acked;
  int failed;
  char final_state_json[64];
  int counter_history[8];
  size_t counter_count;
} startconsumer_context;

typedef struct example_counter_doc {
  lonejson_int64 counter;
} example_counter_doc;

typedef struct example_hello_doc {
  char *hello;
} example_hello_doc;

static const lonejson_field example_counter_fields[] = {
    LONEJSON_FIELD_I64(example_counter_doc, counter, "counter")};

LONEJSON_MAP_DEFINE(example_counter_map, example_counter_doc,
                    example_counter_fields);

static const lonejson_field example_hello_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(example_hello_doc, hello, "hello")};

LONEJSON_MAP_DEFINE(example_hello_map, example_hello_doc, example_hello_fields);

static lonejson *example_lonejson_runtime(void);

static lonejson *example_runtime_instance;
static pthread_once_t example_runtime_once = PTHREAD_ONCE_INIT;

static void example_lonejson_runtime_init(void) {
  lonejson_error error;

  lonejson_error_init(&error);
  example_runtime_instance = lonejson_new(NULL, &error);
}

static lonejson *example_lonejson_runtime(void) {
  (void)pthread_once(&example_runtime_once, example_lonejson_runtime_init);
  return example_runtime_instance;
}

static int fail_with_error(pslog_logger *logger, const char *step,
                           lc_error *error) {
  logger->errorf(logger, step, "code=%d http_status=%ld message=%s detail=%s",
                 error != NULL ? error->code : 0,
                 error != NULL ? error->http_status : 0L,
                 error != NULL && error->message != NULL ? error->message : "",
                 error != NULL && error->detail != NULL ? error->detail : "");
  return 1;
}

static int parse_hello_json(const char *json_text, char *hello_out,
                            size_t hello_out_size) {
  example_hello_doc parsed;
  lonejson *runtime;
  lonejson_error error;
  lonejson_status status;
  size_t hello_length;

  if (hello_out == NULL || hello_out_size == 0U) {
    return 1;
  }
  hello_out[0] = '\0';
  if (json_text == NULL || json_text[0] == '\0') {
    return 1;
  }
  runtime = example_lonejson_runtime();
  if (runtime == NULL) {
    return 1;
  }
  memset(&parsed, 0, sizeof(parsed));
  memset(&error, 0, sizeof(error));
  status = runtime->parse_cstr(runtime, &example_hello_map, &parsed, json_text,
                               &error);
  if (status != LONEJSON_STATUS_OK || parsed.hello == NULL) {
    runtime->cleanup(runtime, &example_hello_map, &parsed, NULL);
    return 1;
  }
  hello_length = strlen(parsed.hello);
  if (hello_length >= hello_out_size) {
    hello_length = hello_out_size - 1U;
  }
  memcpy(hello_out, parsed.hello, hello_length);
  hello_out[hello_length] = '\0';
  runtime->cleanup(runtime, &example_hello_map, &parsed, NULL);
  return 0;
}

static int load_state_counter(lc_lease *state, long *counter_out,
                              lc_error *error) {
  example_counter_doc doc;
  lonejson *runtime;
  lc_get_res get_res;
  int rc;

  if (counter_out == NULL) {
    (void)error;
    return LC_ERR_INVALID;
  }
  *counter_out = 0L;
  if (state == NULL) {
    return LC_ERR_INVALID;
  }
  runtime = example_lonejson_runtime();
  if (runtime == NULL) {
    return LC_ERR_INVALID;
  }

  memset(&doc, 0, sizeof(doc));
  memset(&get_res, 0, sizeof(get_res));
  rc = state->load(state, &example_counter_map, &doc, NULL, &get_res, error);
  if (rc != LC_OK) {
    if (error != NULL &&
        (error->http_status == 204L || error->http_status == 404L)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      lc_get_res_cleanup(&get_res);
      return LC_OK;
    }
    lc_get_res_cleanup(&get_res);
    return rc;
  }

  if (doc.counter < (lonejson_int64)LONG_MIN ||
      doc.counter > (lonejson_int64)LONG_MAX) {
    runtime->cleanup(runtime, &example_counter_map, &doc, NULL);
    lc_get_res_cleanup(&get_res);
    return LC_ERR_PROTOCOL;
  }
  *counter_out = (long)doc.counter;
  runtime->cleanup(runtime, &example_counter_map, &doc, NULL);
  lc_get_res_cleanup(&get_res);
  return LC_OK;
}

static void record_counter(startconsumer_context *context, long counter) {
  pthread_mutex_lock(&context->mutex);
  if (context->counter_count <
      sizeof(context->counter_history) / sizeof(context->counter_history[0])) {
    context->counter_history[context->counter_count++] = (int)counter;
  }
  pthread_mutex_unlock(&context->mutex);
}

static void set_final_state_json(startconsumer_context *context,
                                 const char *json_text) {
  size_t copy_length;

  if (context == NULL || json_text == NULL) {
    return;
  }
  pthread_mutex_lock(&context->mutex);
  copy_length = strlen(json_text);
  if (copy_length >= sizeof(context->final_state_json)) {
    copy_length = sizeof(context->final_state_json) - 1U;
  }
  memcpy(context->final_state_json, json_text, copy_length);
  context->final_state_json[copy_length] = '\0';
  pthread_mutex_unlock(&context->mutex);
}

static int handle_message(void *context, lc_consumer_message *delivery,
                          lc_error *error) {
  startconsumer_context *example;
  lc_sink *sink;
  const void *payload_bytes;
  size_t payload_length;
  long counter;
  char state_json[64];
  char payload_text[128];
  char hello_value[64];
  example_counter_doc counter_doc;
  lc_nack_req nack;
  int rc;

  example = (startconsumer_context *)context;
  sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  counter = 0L;
  memset(payload_text, 0, sizeof(payload_text));
  memset(hello_value, 0, sizeof(hello_value));

  example->example_logger->infof(
      example->example_logger, "example.startconsumer.consumer",
      "name=%s with_state=%d message_id=%s attempts=%d failure_attempts=%d",
      delivery->name != NULL ? delivery->name : "", delivery->with_state,
      delivery->message != NULL && delivery->message->message_id != NULL
          ? delivery->message->message_id
          : "",
      delivery->message != NULL ? delivery->message->attempts : 0,
      delivery->message != NULL ? delivery->message->failure_attempts : 0);

  rc = lc_sink_to_memory(&sink, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = delivery->message->write_payload(delivery->message, sink, NULL, error);
  if (rc != LC_OK) {
    lc_sink_close(sink);
    return rc;
  }
  rc = lc_sink_memory_bytes(sink, &payload_bytes, &payload_length, error);
  if (rc != LC_OK) {
    lc_sink_close(sink);
    return rc;
  }
  if (payload_length >= sizeof(payload_text)) {
    payload_length = sizeof(payload_text) - 1U;
  }
  memcpy(payload_text, payload_bytes, payload_length);
  payload_text[payload_length] = '\0';
  if (parse_hello_json(payload_text, hello_value, sizeof(hello_value)) == 0) {
    example->example_logger->infof(example->example_logger,
                                   "example.startconsumer.payload",
                                   "hello=%s payload_size=%lu", hello_value,
                                   (unsigned long)payload_length);
  } else {
    example->example_logger->infof(
        example->example_logger, "example.startconsumer.payload",
        "payload_size=%lu", (unsigned long)payload_length);
  }
  lc_sink_close(sink);

  if (!delivery->with_state || delivery->state == NULL) {
    return LC_ERR_PROTOCOL;
  }

  rc = load_state_counter(delivery->state, &counter, error);
  if (rc != LC_OK) {
    if (delivery->state->version <= 0L && error != NULL &&
        error->code == LC_ERR_SERVER) {
      lc_error_cleanup(error);
      lc_error_init(error);
      counter = 0L;
    } else {
      return rc;
    }
  }
  counter += 1L;
  record_counter(example, counter);
  snprintf(state_json, sizeof(state_json), "{\"counter\":%ld}", counter);
  counter_doc.counter = (lonejson_int64)counter;
  rc = delivery->state->save(delivery->state, &example_counter_map,
                             &counter_doc, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  set_final_state_json(example, state_json);
  example->example_logger->infof(example->example_logger,
                                 "example.startconsumer.counter", "counter=%ld",
                                 counter);

  if (counter >= 3L) {
    rc = delivery->message->ack(delivery->message, error);
    if (rc != LC_OK) {
      return rc;
    }
    pthread_mutex_lock(&example->mutex);
    example->acked = 1;
    pthread_cond_broadcast(&example->cond);
    pthread_mutex_unlock(&example->mutex);
    return LC_OK;
  }

  lc_nack_req_init(&nack);
  nack.delay_seconds = 1L;
  nack.intent = LC_NACK_INTENT_DEFER;
  rc = delivery->message->nack(delivery->message, &nack, error);
  if (rc != LC_OK) {
    return rc;
  }
  return LC_OK;
}

static void *wait_for_completion(void *context) {
  startconsumer_context *example;

  example = (startconsumer_context *)context;
  pthread_mutex_lock(&example->mutex);
  while (!example->acked && !example->failed) {
    pthread_cond_wait(&example->cond, &example->mutex);
  }
  pthread_mutex_unlock(&example->mutex);

  if (example->acked) {
    sleep(2);
  }
  if (example->service != NULL) {
    (void)example->service->stop(example->service);
  }
  return NULL;
}

int main(void) {
  const char *endpoint;
  const char *client_pem;
  const char *ns;
  const char *endpoints[1];
  char queue_name[96];
  lc_client_config client_config;
  lc_client *client;
  lc_source *client_bundle;
  lc_source *src;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_error error;
  startconsumer_context context;
  pthread_t stopper_thread;
  pslog_config sdk_log_config;
  pslog_config example_log_config;
  pslog_logger *sdk_logger;
  pslog_logger *example_logger;
  int have_thread;
  int rc;

  pslog_default_config(&sdk_log_config);
  sdk_log_config.mode = PSLOG_MODE_CONSOLE;
  sdk_log_config.min_level = PSLOG_LEVEL_TRACE;
  sdk_log_config.timestamps = 1;
  sdk_logger = pslog_new(&sdk_log_config);
  if (sdk_logger == NULL) {
    fprintf(stderr, "failed to allocate logger\n");
    return 1;
  }

  pslog_default_config(&example_log_config);
  example_log_config.mode = PSLOG_MODE_CONSOLE;
  example_log_config.min_level = PSLOG_LEVEL_TRACE;
  example_log_config.timestamps = 1;
  example_log_config.palette = &pslog_builtin_palette_horizon;
  example_logger = pslog_new(&example_log_config);
  if (example_logger == NULL) {
    sdk_logger->destroy(sdk_logger);
    fprintf(stderr, "failed to allocate example logger\n");
    return 1;
  }

  endpoint = getenv("LOCKDC_URL");
  client_pem = getenv("LOCKDC_CLIENT_PEM");
  ns = getenv("LOCKDC_NAMESPACE");
  if (endpoint == NULL || endpoint[0] == '\0') {
    endpoint = EXAMPLE_ENDPOINT;
  }
  if (client_pem == NULL || client_pem[0] == '\0') {
    client_pem = EXAMPLE_CLIENT_PEM;
  }
  if (ns == NULL || ns[0] == '\0') {
    ns = EXAMPLE_NAMESPACE;
  }

  snprintf(queue_name, sizeof(queue_name), "%s-%ld", EXAMPLE_QUEUE_PREFIX,
           (long)getpid());

  endpoints[0] = endpoint;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = ns;
  client_config.logger = sdk_logger;

  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  memset(&context, 0, sizeof(context));
  client = NULL;
  client_bundle = NULL;
  src = NULL;
  service = NULL;
  have_thread = 0;

  if (pthread_mutex_init(&context.mutex, NULL) != 0 ||
      pthread_cond_init(&context.cond, NULL) != 0) {
    fprintf(stderr, "failed to initialize pthread primitives\n");
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return 1;
  }
  context.example_logger = example_logger;
  strcpy(context.final_state_json, "{}");

  example_logger->infof(example_logger, "example.startconsumer.start",
                        "endpoint=%s client_pem=%s namespace=%s queue=%s",
                        endpoint, client_pem, ns, queue_name);

  rc = lc_source_from_file(client_pem, &client_bundle, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_source_from_file", &error);
    goto cleanup;
  }
  client_config.client_bundle_source = client_bundle;
  rc = lc_client_open(&client_config, &client, &error);
  lc_source_close(client_bundle);
  client_bundle = NULL;
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_client_open", &error);
    goto cleanup;
  }

  rc = lc_source_from_memory(EXAMPLE_PAYLOAD, strlen(EXAMPLE_PAYLOAD), &src,
                             &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_source_from_memory", &error);
    goto cleanup;
  }

  enqueue_req.queue = queue_name;
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  enqueue_req.delay_seconds = 1L;
  enqueue_req.content_type = "application/json";
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->enqueue", &error);
    goto cleanup;
  }
  example_logger->infof(example_logger, "example.startconsumer.enqueued",
                        "queue=%s message_id=%s", queue_name,
                        enqueue_res.message_id != NULL ? enqueue_res.message_id
                                                       : "");

  consumer.name = EXAMPLE_CONSUMER_NAME;
  consumer.request.queue = queue_name;
  consumer.request.owner = EXAMPLE_OWNER;
  consumer.request.visibility_timeout_seconds = 30L;
  consumer.request.wait_seconds = 1L;
  consumer.with_state = 1;
  consumer.handle = handle_message;
  consumer.context = &context;
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 500L;
  consumer.restart_policy.max_failures = 5;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  if (rc != LC_OK) {
    rc =
        fail_with_error(example_logger, "client->new_consumer_service", &error);
    goto cleanup;
  }
  context.service = service;

  rc = pthread_create(&stopper_thread, NULL, wait_for_completion, &context);
  if (rc != 0) {
    fprintf(stderr, "failed to create consumer stopper thread\n");
    rc = 1;
    goto cleanup;
  }
  have_thread = 1;

  rc = service->run(service, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "service->run", &error);
    goto cleanup;
  }

  pthread_mutex_lock(&context.mutex);
  if (!context.acked) {
    context.failed = 1;
    pthread_cond_broadcast(&context.cond);
  }
  pthread_mutex_unlock(&context.mutex);

  if (have_thread) {
    pthread_join(stopper_thread, NULL);
    have_thread = 0;
  }

  puts(context.final_state_json);

  rc = 0;

cleanup:
  if (have_thread) {
    pthread_mutex_lock(&context.mutex);
    context.failed = 1;
    pthread_cond_broadcast(&context.cond);
    pthread_mutex_unlock(&context.mutex);
    pthread_join(stopper_thread, NULL);
  }
  if (src != NULL) {
    src->close(src);
  }
  if (client_bundle != NULL) {
    client_bundle->close(client_bundle);
  }
  if (service != NULL) {
    service->close(service);
  }
  if (client != NULL) {
    client->close(client);
  }
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_error_cleanup(&error);
  pthread_cond_destroy(&context.cond);
  pthread_mutex_destroy(&context.mutex);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return rc;
}
