#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <openssl/rand.h>
#include <pthread.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TMP_PREFIX "/tmp/liblockdc-pouch-key-"

extern lonejson *lc_thread_lonejson_runtime(void);
extern int __real_pthread_key_create(pthread_key_t *, void (*)(void *));
extern int __real_pthread_key_delete(pthread_key_t);

static pthread_key_t host_key;
static pthread_t injection_thread;
static unsigned int calls;
static unsigned int fail_at;
static int armed;
static int injected;

int __wrap_pthread_key_create(pthread_key_t *key, void (*cleanup)(void *)) {
  if (armed && pthread_equal(pthread_self(), injection_thread)) {
    ++calls;
    if (fail_at != 0 && calls == fail_at) {
      injected = 1;
      return EAGAIN;
    }
  }
  return __real_pthread_key_create(key, cleanup);
}

int __wrap_pthread_key_delete(pthread_key_t key) {
  if (armed && key == host_key) {
    fprintf(stderr, "SDK cleanup deleted the application's TLS key\n");
    abort();
  }
  return __real_pthread_key_delete(key);
}

int main(int argc, char **argv) {
  lc_client_config config;
  lc_client *client = NULL;
  lc_lease *lease = NULL;
  lc_source *source = NULL;
  lc_acquire_req acquire;
  lc_error error;
  unsigned char bytes[16];
  char template_path[] = TMP_PREFIX "XXXXXX";
  char root[256] = "";
  char endpoint[512];
  const char *endpoints[1];
  unsigned int round;
  unsigned int required_keys;
  int retained_failed_lease = 0;
  int rc = LC_OK;
  int failed = 1;

  if (argc != 3 || pthread_key_create(&host_key, NULL) != 0 ||
      pthread_setspecific(host_key, (void *)1) != 0)
    return 2;
  lc_error_init(&error);
  /* Isolate Pouch key creation from the separately tested crypto/JSON keys. */
  if (lc_allocator_tracking_configure(1, &error) != LC_OK ||
      lc_thread_lonejson_runtime() == NULL ||
      RAND_bytes(bytes, sizeof(bytes)) != 1 ||
      !lc_test_tmp_mkdtemp(template_path, root, sizeof(root), TMP_PREFIX))
    goto done;
  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s/store?single_writer=%s",
               root, strcmp(argv[2], "single") == 0 ? "true" : "false") < 0)
    goto done;
  endpoints[0] = endpoint;
  required_keys = strcmp(argv[2], "single") == 0 ? 3U : 4U;
#ifdef LOCKDC_POUCH_CONTAINER
  /* Shared container owners use their own physical mutation authority. */
  required_keys = 3U;
#endif
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  injection_thread = pthread_self();
  fail_at = (unsigned int)strtoul(argv[1], NULL, 10);
  armed = 1;
  /* Repeat after close/reopen: failed once-initialization must stay safe, and
   * acquired guards must have unwound rather than deadlocking the next call. */
  for (round = 0; round < 2U; ++round) {
    rc = lc_client_open(&config, &client, &error);
    if (rc == LC_OK) {
      lc_acquire_req_init(&acquire);
      acquire.ns = "default";
      acquire.key =
          retained_failed_lease ? "key-failure-reopen" : "key-failure";
      acquire.owner = "key-failure-test";
      acquire.ttl_seconds = 30L;
      rc = lc_acquire(client, &acquire, &lease, &error);
    }
    if (rc == LC_OK) {
      if (lease == NULL)
        goto done;
      rc = lc_source_from_memory("{\"value\":1}", 11U, &source, &error);
      if (rc == LC_OK)
        rc = lc_lease_update(lease, source, NULL, &error);
      lc_source_close(source);
      source = NULL;
    }
    if (injected && fail_at <= required_keys) {
      if (rc != LC_ERR_TRANSPORT || error.code != rc)
        goto done;
      /* An exclusive container can acquire before the update first needs a
       * namespace guard. Leave that acquired lease untouched on reopen. */
      retained_failed_lease = lease != NULL;
      lc_lease_close(lease);
      lease = NULL;
    } else {
      if (rc != LC_OK)
        goto done;
      if (lc_lease_release(lease, NULL, &error) != LC_OK)
        goto done;
      lease = NULL;
    }
    lc_client_close(client);
    client = NULL;
    lc_error_cleanup(&error);
    if (pthread_getspecific(host_key) != (void *)1)
      goto done;
  }
  if (fail_at > 0 && fail_at <= required_keys && !injected)
    goto done;
  failed = 0;
done:
  if (failed)
    fprintf(stderr,
            "Pouch key failure: ordinal=%u mode=%s calls=%u rc=%d: %s\n",
            fail_at, argc == 3 ? argv[2] : "?", calls, rc,
            error.message != NULL ? error.message : "no API error");
  lc_source_close(source);
  lc_lease_close(lease);
  lc_client_close(client);
  lc_error_cleanup(&error);
  if (root[0] != '\0')
    lc_test_tmp_cleanup_path(root, TMP_PREFIX);
  if (pthread_getspecific(host_key) != (void *)1)
    failed = 1;
  printf("Pouch fail_at=%u mode=%s calls=%u injected=%d failed=%d\n", fail_at,
         argv[2], calls, injected, failed);
  return failed;
}
