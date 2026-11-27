#include "bench_pouch.h"

#ifdef LOCKDC_POUCH_SPACE_BENCH
#include "lc_pouch_storage.h"
#include "pouch.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#endif

int bench_open_pouch(const lc_client_config *config, int create,
                     int owned_directory, lc_client **out, lc_error *error) {
#ifdef LOCKDC_POUCH_SPACE_BENCH
  const char *backend = getenv("LOCKDC_BENCH_BACKEND");
  const char *sync = getenv("LOCKDC_BENCH_SYNC");
  const ps_pouch_client_options options = {10U * PS_MIB, 0U, NULL, NULL};
  lc_client_config selected = *config;
  lc_pouch_settings settings;
  char endpoint[PATH_MAX];
  const char *url = config->endpoints[0];
  const char *query = strchr(url, '?');
  size_t path_length = query ? (size_t)(query - url) : strlen(url);
  int n;
  *out = NULL;
  if (owned_directory) {
    n = snprintf(endpoint, sizeof(endpoint), "%.*s/store%s", (int)path_length,
                 url, query ? query : "");
    if (n < 0 || (size_t)n >= sizeof(endpoint))
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "Pouch benchmark endpoint is too long", NULL, NULL,
                          NULL);
    url = endpoint;
  }
  selected.endpoints = &url;
  if (sync) {
    if (strcmp(sync, "0") != 0 && strcmp(sync, "1") != 0)
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "LOCKDC_BENCH_SYNC must be 0 or 1", NULL, NULL, NULL);
    lc_pouch_settings_init(&settings);
    if (config->pouch_settings)
      settings = *config->pouch_settings;
    settings.set_mask |= LC_POUCH_SETTING_DURABLE_SYNC;
    settings.durable_sync = sync[0] == '1';
    selected.pouch_settings = &settings;
  }
  if (backend && strcmp(backend, "single-file") == 0)
    return ps_pouch_client_open(&selected, create, &options, out, error);
  if (!backend || strcmp(backend, "multifile") == 0)
    return lc_pouch_legacy_client_open(&selected, out, error);
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "LOCKDC_BENCH_BACKEND must be single-file or multifile",
                      NULL, NULL, NULL);
#else
  (void)create;
  (void)owned_directory;
  return lc_client_open(config, out, error);
#endif
}
