#include "lc_api_internal.h"
#include "lc_log.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"

#include <dirent.h>
#include <errno.h>
#include <string.h>

/*
 * Exclusive Pouch pays this bounded, read-only cache construction at open so
 * its first query does not decode immutable segment readers. Plain shared
 * roots remain lazy to keep their optional multi-writer resident footprint
 * bounded. Transformed roots retain their existing eager warming in either
 * writer mode.
 */
static int lc_pouch_warm_open_namespaces(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  unsigned long namespace_count;
  int saved_errno;
  int rc;

  if (pouch == NULL || (!lc_pouch_single_writer_enabled(pouch) &&
                        !lc_pouch_crypto_enabled(pouch->crypto) &&
                        !lc_pouch_crypto_compression_enabled(pouch->crypto))) {
    return LC_OK;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("reason", "namespaces-missing");
      lc_log_debug(pouch->logger, "cache.load.skip", fields, 1U);
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  rc = LC_OK;
  saved_errno = 0;
  namespace_count = 0UL;
  {
    pslog_field fields[2];

    fields[0] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[1] = lc_log_bool_field(
        "compression", lc_pouch_crypto_compression_enabled(pouch->crypto));
    lc_log_debug(pouch->logger, "cache.load.start", fields, 2U);
  }
  while (rc == LC_OK) {
    char *ns;
    lc_error warm_error;
    int index_rc;
    int state_rc;

    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      saved_errno = errno;
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    ns = lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    if (ns == NULL) {
      continue;
    }
    lc_error_init(&warm_error);
    state_rc = lc_pouch_state_warm_namespace(pouch, ns, &warm_error);
    if (state_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.state.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    lc_error_init(&warm_error);
    index_rc = LC_OK;
    if (pouch->query_indexing_enabled) {
      index_rc = lc_pouch_query_index_warm_namespace(pouch, ns, &warm_error);
    }
    if (index_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.index.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_bool_field("state_ok", state_rc == LC_OK);
      fields[2] = lc_log_bool_field("index_ok", index_rc == LC_OK);
      lc_log_debug(pouch->logger, "cache.load.namespace", fields, 3U);
    }
    namespace_count++;
    lc_free_with_allocator(&pouch->allocator, ns);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces",
                      strerror(errno != 0 ? errno : EIO), NULL, "pouch");
  } else if (rc == LC_OK && saved_errno != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces", strerror(saved_errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc == LC_OK) {
    pslog_field fields[1];

    fields[0] = lc_log_u64_field("count", namespace_count);
    lc_log_debug(pouch->logger, "cache.load.complete", fields, 1U);
  }
  return rc;
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch = NULL;
  int rc;
  if (!out)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_open requires output", NULL, NULL, NULL);
  *out = NULL;
  rc = lc_pouch_prepare_multifile(root_path, allocator, options, 1, &pouch,
                                  error);
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_warm_open_namespaces(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_runtime_start_maintenance(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_runtime_start_indexing(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  {
    pslog_field fields[12];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    fields[1] = lc_log_str_field("query_engine", pouch->query_engine);
    fields[2] =
        lc_log_str_field("query_fallback_engine", pouch->query_fallback_engine);
    fields[3] = lc_log_str_field("compression", pouch->compression);
    fields[4] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[5] =
        lc_log_u64_field("segment_target_bytes", pouch->segment_target_bytes);
    fields[6] = lc_log_bool_field("single_writer",
                                  lc_pouch_single_writer_enabled(pouch));
    fields[7] = lc_log_bool_field("background_compaction",
                                  pouch->background_compaction_enabled);
    fields[8] = lc_log_bool_field("durable_sync", pouch->durable_sync);
    fields[9] =
        lc_log_u64_field("indexer_flush_docs", pouch->indexer_flush_docs);
    fields[10] = lc_log_u64_field("indexer_flush_interval_seconds",
                                  pouch->indexer_flush_interval_seconds);
    fields[11] =
        lc_log_bool_field("query_indexing", pouch->query_indexing_enabled);
    lc_log_info(pouch->logger, "open", fields, 12U);
  }
  return LC_OK;
}

int lc_pouch_legacy_runtime_factory(void *context, const char *path,
                                    const lc_allocator *allocator,
                                    const lc_pouch_open_options *options,
                                    lc_pouch **out, lc_error *error) {
  (void)context;
  return lc_pouch_open(path, allocator, options, out, error);
}
