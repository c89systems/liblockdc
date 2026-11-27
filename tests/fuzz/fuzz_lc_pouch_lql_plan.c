#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch.h"

#define FUZZ_POUCH_LQL_TMP_PREFIX "/tmp/liblockdc-pouch-lql-fuzz-"
#define FUZZ_POUCH_LQL_CRYPTO_KEY                                              \
  "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

typedef struct fuzz_key_count {
  size_t rows;
} fuzz_key_count;

typedef struct fuzz_storage_mode {
  const char *name;
  const char *crypto_key;
  const char *compression;
} fuzz_storage_mode;

static int fuzz_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int fuzz_key_chunk(void *context, const char *bytes, size_t len,
                          lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int fuzz_key_end(void *context, lc_error *error) {
  fuzz_key_count *count;

  (void)error;
  count = (fuzz_key_count *)context;
  count->rows++;
  return 1;
}

static void fuzz_cleanup_root(const char *root) {
  lc_test_tmp_cleanup_path(root, FUZZ_POUCH_LQL_TMP_PREFIX);
}

static lc_source *fuzz_source_from_text(const char *text, lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(text, strlen(text), &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static int fuzz_open_client(const char *root, int scan_mode,
                            const fuzz_storage_mode *mode, lc_client **out,
                            lc_error *error) {
  const char *scheme =
      LC_POUCH_INTEGRATION_MULTIFILE ? "pouch+multifile" : "pouch";
  char endpoint[512];
  const char *endpoints[1];
  lc_client_config config;

  (void)snprintf(endpoint, sizeof(endpoint), "%s://%s%s", scheme, root,
                 scan_mode ? "?query_engine=scan" : "");
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "fuzz";
  if (mode != NULL) {
    config.pouch_crypto_key = mode->crypto_key;
    config.pouch_compression = mode->compression;
  }
  return lc_client_open(&config, out, error);
}

static int fuzz_put_doc(lc_client *client, const char *key, const char *json,
                        lc_error *error) {
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_update_opts update_opts;
  lc_lease *lease;
  lc_source *source;
  int rc;

  lc_acquire_req_init(&acquire);
  lc_release_req_init(&release_req);
  lc_update_opts_init(&update_opts);
  acquire.key = key;
  acquire.owner = "fuzz-writer";
  acquire.ttl_seconds = 60L;
  update_opts.content_type = "application/json";
  lease = NULL;
  source = NULL;
  rc = client->acquire(client, &acquire, &lease, error);
  if (rc == LC_OK) {
    source = fuzz_source_from_text(json, error);
    if (source == NULL) {
      rc = LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    rc = lease->release(lease, &release_req, error);
    lease = NULL;
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int fuzz_seed_store(lc_client *client, lc_error *error) {
  static const char *const docs[][2] = {
      {"fuzz/doc/0001",
       "{\"bucket\":\"needle\",\"group\":\"even\",\"region\":\"us\","
       "\"value\":1,\"tags\":[\"planning\",\"ops\"],"
       "\"created_at\":\"2026-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"timeout alpha\"},\"flag\":true}"},
      {"fuzz/doc/0002",
       "{\"bucket\":\"haystack\",\"group\":\"odd\",\"region\":\"eu\","
       "\"value\":2,\"tags\":[\"runtime\",\"finance\"],"
       "\"created_at\":\"not-a-date\","
       "\"details\":{\"message\":\"normal beta\"},\"flag\":false}"},
      {"fuzz/doc/0003",
       "{\"bucket\":\"haystack\",\"group\":\"even\",\"region\":\"apac\","
       "\"value\":3,\"tags\":[\"ops\",\"runtime\"],"
       "\"created_at\":\"2027-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"timeout gamma\"}}"},
      {"fuzz/doc/0004",
       "{\"bucket\":\"needle\",\"group\":\"odd\",\"region\":\"us\","
       "\"value\":4,\"tags\":[\"planning\",\"finance\"],"
       "\"created_at\":\"2024-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"normal delta\"},\"flag\":true}"}};
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  size_t index;
  int rc;

  for (index = 0U; index < sizeof(docs) / sizeof(docs[0]); ++index) {
    rc = fuzz_put_doc(client, docs[index][0], docs[index][1], error);
    if (rc != LC_OK) {
      return rc;
    }
  }

  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  flush_req.ns = "fuzz";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, error);
  lc_index_flush_res_cleanup(&flush_res);
  return rc;
}

static int fuzz_install_snapshot(const char *root,
                                 const fuzz_storage_mode *mode,
                                 lc_error *error) {
  lc_client *client = NULL;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = fuzz_open_client(root, 0, mode, &client, error);
  if (rc == LC_OK) {
    maintenance_options.ns = "fuzz";
    maintenance_options.force = 1;
    rc = lc_pouch_maintenance_run(((lc_client_handle *)client)->pouch,
                                  &maintenance_options, &maintenance_result,
                                  error);
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_client_close(client);
  return rc;
}

static int fuzz_namespace_path(char *path, size_t path_size, const char *root,
                               const char *leaf) {
  int written;

  if (root == NULL || leaf == NULL) {
    return 0;
  }
  written = snprintf(path, path_size, "%s/namespaces/fuzz/%s", root, leaf);
  return written > 0 && (size_t)written < path_size;
}

static void fuzz_write_text_file(const char *path, const char *text) {
  FILE *fp;

  if (path == NULL || text == NULL) {
    return;
  }
  fp = fopen(path, "wb");
  if (fp == NULL) {
    return;
  }
  (void)fwrite(text, 1U, strlen(text), fp);
  (void)fclose(fp);
}

static void fuzz_write_bytes_file(const char *path, const unsigned char *bytes,
                                  size_t length) {
  FILE *fp;

  if (path == NULL || (bytes == NULL && length > 0U)) {
    return;
  }
  fp = fopen(path, "wb");
  if (fp == NULL) {
    return;
  }
  if (length > 0U) {
    (void)fwrite(bytes, 1U, length, fp);
  }
  (void)fclose(fp);
}

static int fuzz_newest_query_segment_artifact_path(const char *root,
                                                   const char *artifact_leaf,
                                                   char *path,
                                                   size_t path_size) {
  char manifest_path[768];
  char line[256];
  char segment_id[128];
  FILE *fp;
  int written;

  if (path == NULL || path_size == 0U || artifact_leaf == NULL ||
      !fuzz_namespace_path(manifest_path, sizeof(manifest_path), root,
                           "index/query.manifest")) {
    return 0;
  }
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    return 0;
  }
  segment_id[0] = '\0';
  while (fgets(line, sizeof(line), fp) != NULL) {
    if (sscanf(line, "segment %127s", segment_id) == 1 &&
        segment_id[0] != '\0') {
      break;
    }
    segment_id[0] = '\0';
  }
  (void)fclose(fp);
  if (segment_id[0] == '\0') {
    return 0;
  }
  written = snprintf(path, path_size, "%s/namespaces/fuzz/index/query.%s.%s",
                     root, segment_id, artifact_leaf);
  if (written < 0 || (size_t)written >= path_size) {
    return 0;
  }
  return 1;
}

static void fuzz_damage_namespace_manifest(const char *root,
                                           unsigned int mode) {
  char path[768];

  if (!fuzz_namespace_path(path, sizeof(path), root, "manifest") ||
      mode == 0U) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
  } else if (mode == 2U) {
    fuzz_write_text_file(path, "not-a-manifest\nactive_segment=broken\n");
  } else {
    fuzz_write_text_file(path, "active_segment=seg-00000000000000000000.log\n"
                               "snapshot=snapshot-00000000000000000000.log\n");
  }
}

static void fuzz_damage_marker(const char *root, unsigned int mode) {
  char path[768];

  if (mode == 0U || !fuzz_namespace_path(path, sizeof(path), root,
                                         "markers/writer-fuzz-peer.marker")) {
    return;
  }
  if (mode == 1U) {
    fuzz_write_text_file(path, "sequence=not-a-number\n");
  } else if (mode == 2U) {
    fuzz_write_text_file(path, "");
  } else {
    fuzz_write_text_file(path, "sequence=184467440737095516150\n");
  }
}

static void fuzz_damage_query_index(const char *root, unsigned int mode) {
  char path[768];
  FILE *fp;
  const char *artifact_leaf;
  static const unsigned char corrupt_generation[] = {
      'L', 'P', 'I', 'T', 'G', 'E', 'N', '1', 0xffU, 0x00U, 0x7fU, 0x42U};
  static const unsigned char corrupt_doc_table[] = {
      'L', 'P', 'D', 'T', 'G', 'E', 'N', '1', 0x01U, 0x00U, 0x00U, 0x00U};

  if (root == NULL || mode == 0U) {
    return;
  }
  if (mode >= 4U) {
    artifact_leaf = "query.index.lcpttg";
    if (mode == 5U) {
      artifact_leaf = "query.index.lcptxg";
    } else if (mode == 6U) {
      artifact_leaf = "query.index.lcpt3g";
    } else if (mode == 7U) {
      artifact_leaf = "query.index.lcpdtg";
    }
    if (!fuzz_newest_query_segment_artifact_path(root, artifact_leaf, path,
                                                 sizeof(path))) {
      return;
    }
    if (mode == 7U) {
      fuzz_write_bytes_file(path, corrupt_doc_table, sizeof(corrupt_doc_table));
    } else {
      fuzz_write_bytes_file(path, corrupt_generation,
                            sizeof(corrupt_generation));
    }
    return;
  }
  if (!fuzz_namespace_path(path, sizeof(path), root, "index/query.manifest")) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
    return;
  }
  if (mode == 3U && !fuzz_newest_query_segment_artifact_path(
                        root, "query.index", path, sizeof(path))) {
    return;
  }
  fp = fopen(path, "wb");
  if (fp == NULL) {
    return;
  }
  if (mode == 2U) {
    (void)fwrite("not-a-pouch-query-index-manifest\nsegment broken\n", 1U,
                 strlen("not-a-pouch-query-index-manifest\nsegment broken\n"),
                 fp);
  } else {
    static const char future_index[] = "format=pouch-query-index\n"
                                       "version=999999\n"
                                       "state_index_seq=999999\n"
                                       "row_count=1\n"
                                       "row_hash=1\n"
                                       "term_index_complete=1\n"
                                       "term_count=1\n"
                                       "term_hash=1\n"
                                       "presence_index_complete=1\n"
                                       "presence_count=1\n"
                                       "presence_hash=1\n";
    (void)fwrite(future_index, 1U, sizeof(future_index) - 1U, fp);
  }
  (void)fclose(fp);
}

static void fuzz_damage_snapshot(const char *root, unsigned int mode) {
  char path[768];
  FILE *fp;

  if (mode == 0U ||
      !fuzz_namespace_path(path, sizeof(path), root,
                           "snapshots/snapshot-00000000000000000001.log")) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
    return;
  }
  fp = fopen(path, mode == 2U ? "ab" : "wb");
  if (fp == NULL) {
    return;
  }
  if (mode == 2U) {
    static const unsigned char garbage_tail[] = {'L', 'H', 'C', 'P', 1U, 255U,
                                                 0U,  0U,  0U,  0U,  0U, 0U};
    (void)fwrite(garbage_tail, 1U, sizeof(garbage_tail), fp);
  } else {
    static const unsigned char corrupt_snapshot[] = {
        'L', 'H', 'C', 'P', 1U, 1U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U,
        0U,  0U,  0U,  0U,  0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U, 0U};
    (void)fwrite(corrupt_snapshot, 1U, sizeof(corrupt_snapshot), fp);
  }
  (void)fclose(fp);
}

static int fuzz_query_keys(lc_client *client, const char *selector,
                           int selector_is_lql, size_t *rows_out,
                           lc_error *error) {
  lc_query_key_handler handler;
  lc_query_req req;
  lc_query_res res;
  fuzz_key_count count;
  int rc;

  memset(&handler, 0, sizeof(handler));
  memset(&count, 0, sizeof(count));
  memset(&res, 0, sizeof(res));
  lc_query_req_init(&req);
  handler.begin = fuzz_key_begin;
  handler.chunk = fuzz_key_chunk;
  handler.end = fuzz_key_end;
  req.ns = "fuzz";
  if (selector_is_lql) {
    req.selector_lql = selector;
  } else {
    req.selector_json = selector;
  }
  req.limit = 16L;
  rc = client->query_keys(client, &req, &handler, &count, &res, error);
  lc_query_res_cleanup(&res);
  if (rc == LC_OK) {
    *rows_out = count.rows;
  }
  return rc;
}

static char *fuzz_selector_from_input(const uint8_t *data, size_t size,
                                      int *selector_is_lql) {
  static const char fallback[] = "{}";
  static const char lql_prefix[] = "lql:";
  const uint8_t *selector_data;
  char *selector;
  size_t selector_size;
  size_t index;

  *selector_is_lql = 0;
  selector_data = data;
  selector_size = size;
  if (selector_size >= sizeof(lql_prefix) - 1U &&
      memcmp(selector_data, lql_prefix, sizeof(lql_prefix) - 1U) == 0) {
    *selector_is_lql = 1;
    selector_data += sizeof(lql_prefix) - 1U;
    selector_size -= sizeof(lql_prefix) - 1U;
  }
  while (selector_size > 0U && (selector_data[selector_size - 1U] == '\n' ||
                                selector_data[selector_size - 1U] == '\r')) {
    --selector_size;
  }
  if (selector_size == 0U && !*selector_is_lql) {
    selector_size = sizeof(fallback) - 1U;
    selector_data = (const uint8_t *)fallback;
  }
  if (selector_size > 512U) {
    selector_size = 512U;
  }
  selector = (char *)malloc(selector_size + 1U);
  if (selector == NULL) {
    return NULL;
  }
  for (index = 0U; index < selector_size; ++index) {
    selector[index] =
        selector_data[index] == 0U ? ' ' : (char)selector_data[index];
  }
  selector[selector_size] = '\0';
  return selector;
}

static void fuzz_require_open(int rc, const fuzz_storage_mode *mode,
                              const lc_error *error) {
  if (rc != LC_OK) {
    fprintf(stderr, "pouch LQL fixture failed in %s mode: %d %s\n", mode->name,
            rc, error->message ? error->message : "");
    abort();
  }
}

static void fuzz_run_mode(size_t size, const fuzz_storage_mode *mode,
                          const char *selector, int selector_is_lql) {
  char root_template[] = "/tmp/liblockdc-pouch-lql-fuzz-XXXXXX";
  char root_path[sizeof(root_template)];
  char root[sizeof(root_path) + sizeof("/store")];
  lc_client *index_client;
  lc_client *scan_client;
  lc_error index_error;
  lc_error scan_error;
  size_t index_rows;
  size_t scan_rows;
  int index_rc;
  int scan_rc;

  if (!lc_test_tmp_mkdtemp(root_template, root_path, sizeof(root_path),
                           FUZZ_POUCH_LQL_TMP_PREFIX)) {
    return;
  }
  (void)snprintf(root, sizeof(root), "%s/store", root_path);

  index_client = NULL;
  scan_client = NULL;
  index_rows = 0U;
  scan_rows = 0U;
  lc_error_init(&index_error);
  lc_error_init(&scan_error);

  fuzz_require_open(
      fuzz_open_client(root, 0, mode, &index_client, &index_error), mode,
      &index_error);
  fuzz_require_open(fuzz_seed_store(index_client, &index_error), mode,
                    &index_error);
  lc_client_close(index_client);
  index_client = NULL;
  fuzz_require_open(fuzz_install_snapshot(root, mode, &index_error), mode,
                    &index_error);
  if (LC_POUCH_INTEGRATION_MULTIFILE) {
    unsigned int damage = (unsigned int)size;
    fuzz_damage_snapshot(root, (damage / 64U) % 4U);
    fuzz_damage_namespace_manifest(root, (damage / 4U) % 4U);
    fuzz_damage_marker(root, (damage / 16U) % 4U);
    fuzz_damage_query_index(root, damage % 8U);
  }
  index_rc = fuzz_open_client(root, 0, mode, &index_client, &index_error);
  if (!LC_POUCH_INTEGRATION_MULTIFILE)
    fuzz_require_open(index_rc, mode, &index_error);
  if (index_rc == LC_OK)
    index_rc = fuzz_query_keys(index_client, selector, selector_is_lql,
                               &index_rows, &index_error);
  lc_client_close(index_client);

  scan_rc = fuzz_open_client(root, 1, mode, &scan_client, &scan_error);
  if (!LC_POUCH_INTEGRATION_MULTIFILE)
    fuzz_require_open(scan_rc, mode, &scan_error);
  if (scan_rc == LC_OK)
    scan_rc = fuzz_query_keys(scan_client, selector, selector_is_lql,
                              &scan_rows, &scan_error);
  if (index_rc == LC_OK && scan_rc == LC_OK && index_rows != scan_rows) {
    abort();
  }

  lc_client_close(scan_client);
  lc_error_cleanup(&scan_error);
  lc_error_cleanup(&index_error);
  fuzz_cleanup_root(root_path);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  fuzz_storage_mode plaintext_mode;
  fuzz_storage_mode crypto_mode;
  fuzz_storage_mode compression_mode;
  fuzz_storage_mode crypto_compression_mode;
  char *selector;
  int selector_is_lql;

  selector = fuzz_selector_from_input(data, size, &selector_is_lql);
  if (selector == NULL) {
    return 0;
  }

  plaintext_mode.name = "plaintext";
  plaintext_mode.crypto_key = NULL;
  plaintext_mode.compression = NULL;
  crypto_mode.name = "crypto";
  crypto_mode.crypto_key = FUZZ_POUCH_LQL_CRYPTO_KEY;
  crypto_mode.compression = NULL;
  compression_mode.name = "compression";
  compression_mode.crypto_key = NULL;
  compression_mode.compression = "zlib";
  crypto_compression_mode.name = "crypto+compression";
  crypto_compression_mode.crypto_key = FUZZ_POUCH_LQL_CRYPTO_KEY;
  crypto_compression_mode.compression = "zlib";

  fuzz_run_mode(size, &plaintext_mode, selector, selector_is_lql);
  fuzz_run_mode(size, &crypto_mode, selector, selector_is_lql);
  fuzz_run_mode(size, &compression_mode, selector, selector_is_lql);
  fuzz_run_mode(size, &crypto_compression_mode, selector, selector_is_lql);

  free(selector);
  return 0;
}
