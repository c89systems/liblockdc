#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <pslog.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef LC_TEST_INTERNAL_RUNTIME
extern lonejson *lc_thread_lonejson_runtime(void);
int lc_thread_unload_probe(void) {
  return lc_thread_lonejson_runtime() != NULL ? 0 : 1;
}
#endif

int lc_log_unload_probe(const char *mode) {
  lc_client_config config;
  lc_client *client = NULL;
  lc_lease *lease = NULL;
  lc_source *source = NULL;
  lc_acquire_req acquire;
  lc_error error;
  pslog_config log_config;
  pslog_logger *logger = NULL;
  char template_path[] = "/tmp/liblockdc-unload-XXXXXX";
  char root[256] = "";
  char endpoint[512];
  char key_path[512];
  const char *endpoints[1];
  unsigned int i;
  int rc = 1;

  lc_error_init(&error);
  if (lc_allocator_tracking_configure(1, &error) != LC_OK)
    goto done;
#ifdef LC_TEST_INTERNAL_RUNTIME
  if (strcmp(mode, "key-failure") == 0) {
    pthread_key_t keys[2048];
    size_t count = 0;
    size_t j;
    while (count < sizeof(keys) / sizeof(keys[0]) &&
           pthread_key_create(&keys[count], NULL) == 0)
      ++count;
    rc = lc_thread_lonejson_runtime() == NULL ? 0 : 1;
    for (j = 0; j < count; ++j)
      (void)pthread_key_delete(keys[j]);
    goto done;
  }
  if (lc_thread_lonejson_runtime() == NULL)
    goto done;
  if (strcmp(mode, "runtime-only") == 0) {
    rc = 0;
    goto done;
  }
#endif
  if (strstr(mode, "supplied") != NULL) {
    pslog_default_config(&log_config);
    log_config.min_level = PSLOG_LEVEL_DISABLED;
    logger = pslog_new(&log_config);
    if (logger == NULL)
      goto done;
  }
  lc_client_config_init(&config);
  config.logger = logger;
  config.disable_logger_sys_field = strstr(mode, "borrowed") != NULL;
  config.disable_mtls = 1;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  if (strstr(mode, "remote") != NULL) {
    endpoints[0] = "http://127.0.0.1:1";
  } else {
    if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                             "/tmp/liblockdc-unload-"))
      goto done;
    if (snprintf(endpoint, sizeof(endpoint),
                 "pouch://%s/store?single_writer=false", root) < 0)
      goto done;
    endpoints[0] = endpoint;
    if (strstr(mode, "encrypted") != NULL) {
      if (snprintf(key_path, sizeof(key_path), "%s/key", root) < 0)
        goto done;
      config.pouch_crypto_key_file = key_path;
      config.pouch_crypto_generate_key_file = 1;
      config.pouch_crypto_generate_key_file_set = 1;
    }
    if (strstr(mode, "failure") != NULL) {
      FILE *file;
      if (snprintf(key_path, sizeof(key_path), "%s/file", root) < 0)
        goto done;
      file = fopen(key_path, "w");
      if (file == NULL || fclose(file) != 0)
        goto done;
      if (snprintf(endpoint, sizeof(endpoint), "pouch://%s/file/store", root) <
          0)
        goto done;
    }
  }
  for (i = 0; i < 2U; ++i) {
    int status = lc_client_open(&config, &client, &error);
    if (strstr(mode, "failure") != NULL) {
      if (status == LC_OK || client != NULL)
        goto done;
      lc_error_cleanup(&error);
      continue;
    }
    if (status != LC_OK)
      goto done;
    if (strstr(mode, "remote") == NULL) {
      lc_acquire_req_init(&acquire);
      acquire.ns = "default";
      acquire.key = "unload";
      acquire.owner = "unload-test";
      acquire.ttl_seconds = 30L;
      if (client->acquire(client, &acquire, &lease, &error) != LC_OK ||
          lc_source_from_memory("{\"value\":1}", 11U, &source, &error) !=
              LC_OK ||
          lc_lease_update(lease, source, NULL, &error) != LC_OK)
        goto done;
      lc_source_close(source);
      source = NULL;
      if (lc_lease_release(lease, NULL, &error) != LC_OK)
        goto done;
      lease = NULL;
    }
    lc_client_close(client);
    client = NULL;
  }
  /* The SDK must not destroy the borrowed base logger. */
  if (logger != NULL)
    logger->info(logger, "still owned by the caller", NULL, 0U);
  rc = 0;
done:
  if (rc != 0)
    fprintf(stderr, "client lifetime: %s\n",
            error.message != NULL ? error.message : "setup failed");
  lc_source_close(source);
  lc_lease_close(lease);
  lc_client_close(client);
  lc_error_cleanup(&error);
  if (logger != NULL)
    logger->destroy(logger);
  if (root[0] != '\0')
    lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-unload-");
  return rc;
}

#ifdef LC_TEST_STATIC_EXIT
int main(void) { return lc_log_unload_probe("pouch-encrypted-supplied"); }
#endif
