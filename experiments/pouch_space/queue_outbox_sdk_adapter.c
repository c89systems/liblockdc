#define _GNU_SOURCE
#include "lc_api_internal.h"
#include "lc_pouch_storage.h"
#include "pouch.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Reuse the production SDK behavior cases with an exclusive container owner.
 * Directory fixtures keep one container.pouch inside their existing cleanup
 * envelope. Scan fallback is explicit until indexed-query ownership is added.
 * This adapter establishes no shared-root or indexed-query proof. */
static pthread_mutex_t key_mutex = PTHREAD_MUTEX_INITIALIZER;
static char *test_key;
static void free_key(void) {
  lc_pouch_crypto_key_string_free(test_key);
  test_key = NULL;
}
static int setting(const char *name, int maximum, int *out, lc_error *e) {
  const char *text = getenv(name);
  if (!text || text[0] < '0' || text[0] > '0' + maximum || text[1])
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "container SDK fixture requires codec and durability",
                        name, NULL, "pouch");
  *out = text[0] - '0';
  return LC_OK;
}
static int factory(void *context, const char *path, const lc_allocator *a,
                   const lc_pouch_open_options *logical, lc_pouch **out,
                   lc_error *e) {
  ps_pouch_state_options options = {0};
  struct stat status;
  char *physical;
  size_t length = strlen(path);
  int codec = 0, sync = 0, rc, create;
  (void)context;
  rc = setting("POUCH_SPACE_TEST_CODEC", 3, &codec, e);
  if (rc == LC_OK)
    rc = setting("POUCH_SPACE_TEST_SYNC", 1, &sync, e);
  if (rc != LC_OK)
    return rc;
  physical = lc_alloc_with_allocator(a, length + sizeof("/container.pouch"));
  if (!physical)
    return lc_error_set(e, LC_ERR_NOMEM, 0L, "cannot allocate SDK fixture path",
                        NULL, NULL, "pouch");
  if (stat(path, &status) == 0 && S_ISDIR(status.st_mode))
    snprintf(physical, length + sizeof("/container.pouch"),
             "%s/container.pouch", path);
  else
    strcpy(physical, path);
  create = stat(physical, &status) != 0 && errno == ENOENT;
  options.logical = *logical;
  options.logical.single_writer = options.logical.single_writer_set = 1;
  options.logical.query_indexing_enabled = 0;
  options.logical.query_indexing_enabled_set = 1;
  options.logical.query_engine = "scan";
  options.logical.durable_sync = sync;
  if (getenv("POUCH_SPACE_TEST_WATCH")) {
    int watch = 0;
    rc = setting("POUCH_SPACE_TEST_WATCH", 1, &watch, e);
    options.logical.queue_watch = watch;
  }
  options.logical.compression = codec & 2 ? "zlib" : "none";
  if ((codec & 1) && !logical->crypto_key && !logical->crypto_key_file) {
    pthread_mutex_lock(&key_mutex);
    if (!test_key) {
      rc = lc_pouch_crypto_generate_key_string(&test_key, e);
      if (rc == LC_OK && atexit(free_key) != 0) {
        free_key();
        rc = lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                          "cannot retain SDK fixture key", NULL, NULL, "pouch");
      }
    }
    options.logical.crypto_key = test_key;
    pthread_mutex_unlock(&key_mutex);
  }
  if (rc == LC_OK)
    rc = ps_pouch_state_open(physical, create, a, &options, out, e);
  lc_free_with_allocator(a, physical);
  return rc;
}
int __real_lc_client_open(const lc_client_config *, lc_client **, lc_error *);
int __real_lc_pouch_open(const char *, const lc_allocator *,
                         const lc_pouch_open_options *, lc_pouch **,
                         lc_error *);
int __wrap_lc_pouch_open(const char *path, const lc_allocator *a,
                         const lc_pouch_open_options *logical, lc_pouch **out,
                         lc_error *e) {
  lc_pouch_open_options defaults = {0};
  if (!path || !out)
    return __real_lc_pouch_open(path, a, logical, out, e);
  return factory(NULL, path, a, logical ? logical : &defaults, out, e);
}
int __wrap_lc_client_open(const lc_client_config *config, lc_client **out,
                          lc_error *e) {
  if (!config || !config->endpoints || !config->endpoint_count ||
      !config->endpoints[0] || strncmp(config->endpoints[0], "pouch://", 8U))
    return __real_lc_client_open(config, out, e);
  return lc_client_open_with_pouch_factory(config, factory, NULL, out, e);
}
