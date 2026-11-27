#include "bench_pouch.h"

#ifdef LOCKDC_POUCH_SPACE_BENCH
#include "lc_pouch_storage.h"
#include "pouch.h"
#include <stdlib.h>
#endif

#include <limits.h>
#include <stdio.h>
#include <string.h>

int bench_open_pouch(const lc_client_config *config, int create,
                     int owned_directory, lc_client **out, lc_error *error) {
  lc_client_config selected = *config;
  char endpoint[PATH_MAX];
  const char *owned_endpoint = endpoint;
#ifdef LOCKDC_POUCH_SPACE_BENCH
  const char *backend = getenv("LOCKDC_BENCH_BACKEND");
  const char *sync = getenv("LOCKDC_BENCH_SYNC");
  const ps_pouch_client_options options = {10U * PS_MIB, 0U, NULL, NULL};
  lc_pouch_settings settings;
#endif

  *out = NULL;
  if (owned_directory) {
    const char *url = config->endpoints[0];
    const char *query = strchr(url, '?');
    size_t path_length = query ? (size_t)(query - url) : strlen(url);
    int n = -1;
    if (path_length < sizeof(endpoint))
      n = snprintf(endpoint, sizeof(endpoint), "%.*s/store%s", (int)path_length,
                   url, query ? query : "");
    if (n < 0 || (size_t)n >= sizeof(endpoint))
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "Pouch benchmark endpoint is too long", NULL, NULL,
                          NULL);
    selected.endpoints = &owned_endpoint;
  }
#ifdef LOCKDC_POUCH_SPACE_BENCH
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
  return lc_client_open(&selected, out, error);
#endif
}
