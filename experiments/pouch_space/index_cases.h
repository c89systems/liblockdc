#ifndef POUCH_SPACE_INDEX_CASES_H
#define POUCH_SPACE_INDEX_CASES_H
#include "lc_pouch_index_store.h"
#include "lc_pouch_query_index.h"
#include "query_index.h"
#include <zlib.h>
#ifdef __SANITIZE_ADDRESS__
extern int __lsan_do_recoverable_leak_check(void);
#endif

static char *index_key;
static void index_key_free(void) {
  lc_pouch_crypto_key_string_free(index_key);
  index_key = NULL;
}
static int index_codec(void) {
  const char *text = getenv("POUCH_INDEX_CODEC");
  int codec = text ? atoi(text) : 0;
  REQUIRE(codec >= 0 && codec <= 3);
  return codec;
}
typedef struct index_admission {
  const ps_io *io;
  int create;
  void (*ready)(void *);
  void *context;
} index_admission;
static int index_admit(void *context, const char *path, const lc_allocator *a,
                       const lc_pouch_open_options *logical, lc_pouch **out,
                       lc_error *e) {
  index_admission *admission = context;
  ps_pouch_state_options options = {0};
  options.logical = *logical;
  options.io = admission->io;
  int rc = ps_pouch_state_open(path, admission->create, a, &options, out, e);
  if (rc == LC_OK && admission->ready)
    admission->ready(admission->context);
  return rc;
}
static lc_client *index_client_admit(const char *path, int sync, int create,
                                     int indexing, int shared, const ps_io *io,
                                     void (*ready)(void *), void *context,
                                     lc_error *e) {
  char endpoint[1600];
  const char *endpoints[] = {endpoint};
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *client = NULL;
  index_admission admission = {io, create, ready, context};
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?single_writer=%s&background_compaction=false&"
                   "queue_watch=false&query_engine=index",
                   path, shared ? "false" : "true") < (int)sizeof(endpoint));
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_INDEXER_FLUSH_DOCS |
      LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS;
  settings.durable_sync = sync;
  settings.compression = index_codec() & 2 ? "zlib" : "none";
  settings.query_indexing_enabled = indexing;
  settings.indexer_flush_docs = UINT64_MAX;
  settings.indexer_flush_interval_seconds = 3600U;
  if ((index_codec() & 1) && !index_key) {
    const char *inherited = getenv("POUCH_INDEX_KEY");
    if (inherited) {
      index_key = lc_strdup_with_allocator(NULL, inherited);
      REQUIRE(index_key);
    } else
      ok(lc_pouch_crypto_generate_key_string(&index_key, e), e);
    REQUIRE(!atexit(index_key_free));
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.allocator = allocator;
  config.pouch_settings = &settings;
  config.pouch_crypto_key = index_codec() & 1 ? index_key : NULL;
  ok(ready ? lc_client_open_with_pouch_factory(&config, index_admit, &admission,
                                               &client, e)
           : ps_pouch_client_open(&config, create,
                                  &(ps_pouch_client_options){.io = io}, &client,
                                  e),
     e);
  return client;
}
static lc_client *index_client(const char *path, int sync, int create,
                               int indexing, const ps_io *io, lc_error *e) {
  return index_client_admit(path, sync, create, indexing, 0, io, NULL, NULL, e);
}
static lc_pouch *index_pouch(lc_client *client) {
  return ((lc_client_handle *)client)->pouch;
}
static lc_pouch_index_artifact *
index_artifact(lc_client *client, const char *ns, unsigned int kind,
               uint64_t generation, lc_error *e) {
  lc_pouch_index_artifact_id id = {0};
  id.kind = kind;
  id.generation = generation;
  id.segmented = kind != LC_POUCH_INDEX_MANIFEST;
  lc_pouch_index_artifact *artifact =
      lc_pouch_index_artifact_new(index_pouch(client), ns, &id, e);
  REQUIRE(artifact);
  return artifact;
}
static void index_artifact_expect(lc_pouch_index_artifact *artifact,
                                  const void *expected, size_t size,
                                  lc_error *e) {
  char *bytes = NULL;
  size_t length = 0U;
  int present = 0, valid = 0;
  ok(lc_pouch_index_artifact_load(artifact, 1, &bytes, &length, &present,
                                  &valid, e),
     e);
  REQUIRE(present && valid && length == size && !memcmp(bytes, expected, size));
  lc_free_with_allocator(&allocator, bytes);
}
static unsigned long index_input_crc(lc_pouch_index_input *input, lc_error *e) {
  unsigned char bytes[2048];
  unsigned long crc = crc32(0L, Z_NULL, 0);
  for (uint64_t offset = 0U; offset < input->bytes;) {
    size_t size = input->bytes - offset < sizeof(bytes)
                      ? (size_t)(input->bytes - offset)
                      : sizeof(bytes);
    struct iovec part = {bytes, size};
    ok(lc_pouch_record_io_read(input->io, offset, &part, 1, e), e);
    crc = crc32(crc, bytes, (uInt)size);
    offset += size;
  }
  return crc;
}
static int index_failed_producer(void *context, lc_sink *sink, lc_error *e) {
  int oversized = *(int *)context;
  if (oversized) {
    REQUIRE(!sink->write(sink, "over-admission", 14U, e));
    REQUIRE(e->code == LC_ERR_INVALID);
    return e->code;
  }
  REQUIRE(sink->write(sink, "partial", 7U, e));
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                      "injected index metadata producer failure", NULL, NULL,
                      "test");
}
static void index_artifacts(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  char path[1400];
  REQUIRE(snprintf(path, sizeof(path), "%s/index-artifacts", directory) <
          (int)sizeof(path));
  const size_t size = PS_MIB + 77U;
  unsigned char *bytes = malloc(size);
  REQUIRE(bytes);
  for (size_t i = 0; i < size; ++i)
    bytes[i] = (unsigned char)(i * 13U + 7U);
  for (int sync = 0; sync < 2; ++sync) {
    lc_client *client = index_client(path, sync, 1, 1, NULL, &e);
    lc_pouch *pouch = index_pouch(client);
    lc_pouch_index_artifact *artifact = index_artifact(
        client, "team/index:../exact", LC_POUCH_INDEX_PACKED, 7U, &e);
    lc_pouch_index_artifact *other = index_artifact(
        client, "team/index:../other", LC_POUCH_INDEX_PACKED, 7U, &e);
    lc_pouch_index_signature missing = {0}, before = {0}, after = {0};
    ok(lc_pouch_index_artifact_signature(artifact, &missing, &e), &e);
    REQUIRE(!missing.present && strcmp(artifact->key, other->key));
    ok(lc_pouch_index_artifact_write(artifact, (const char *)bytes, size, 1, 0,
                                     &e),
       &e);
    index_artifact_expect(artifact, bytes, size, &e);
    ok(lc_pouch_index_artifact_signature(artifact, &before, &e), &e);
    REQUIRE(before.present &&
            (index_codec() & 1 ? before.size > size : before.size == size));
    void *context = NULL;
    const lc_pouch_index_store_ops *ops =
        pouch->storage.ops->query_indexes(pouch->storage.context, &context);
    lc_pouch_index_input first = {0}, second = {0};
    ok(ops->open(context, artifact->ns, &artifact->id, &first, &e), &e);
    ok(ops->open(context, artifact->ns, &artifact->id, &second, &e), &e);
    unsigned long original = index_input_crc(&first, &e);
    REQUIRE(original == index_input_crc(&second, &e));
    for (int oversized = 0; oversized < 2; ++oversized) {
      REQUIRE(ops->replace(context, artifact->ns, &artifact->id,
                           oversized ? 4U : 1024U, index_failed_producer,
                           &oversized, 0, 0, &e) ==
              (oversized ? LC_ERR_INVALID : LC_ERR_TRANSPORT));
      clear(&e);
      ok(ps_check(ps_root_space(ps_pouch_state_root(pouch)), &e), &e);
      index_artifact_expect(artifact, bytes, size, &e);
    }
    bytes[0] ^= 1U;
    ok(lc_pouch_index_artifact_write(artifact, (const char *)bytes, size, 0, 0,
                                     &e),
       &e);
    ok(lc_pouch_index_artifact_signature(artifact, &after, &e), &e);
    REQUIRE(before.mtime != after.mtime);
    index_artifact_expect(artifact, bytes, size, &e);
    REQUIRE(original == index_input_crc(&first, &e));
    ok(lc_pouch_index_artifact_write(other, "sibling", 7U, 1, 0, &e), &e);
    ok(lc_pouch_index_artifacts_prune(pouch, artifact->ns, NULL, 0U, &e), &e);
    ok(lc_pouch_index_artifact_signature(artifact, &after, &e), &e);
    REQUIRE(!after.present);
    index_artifact_expect(other, "sibling", 7U, &e);
    ps_root *root = ps_pouch_state_root(pouch);
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    REQUIRE(original == index_input_crc(&first, &e) &&
            original == index_input_crc(&second, &e));
    ps_stats held = {0}, released = {0};
    ok(ps_status(ps_root_space(root), &held, &e), &e);
    REQUIRE(held.retired_bytes);
    ops->close(context, &first);
    ops->close(context, &second);
    ok(ps_status(ps_root_space(root), &released, &e), &e);
    REQUIRE(released.retired_bytes < held.retired_bytes &&
            released.allocated_bytes < held.allocated_bytes);
    lc_free_with_allocator(&allocator, artifact);
    lc_free_with_allocator(&allocator, other);
    lc_client_close(client);
    REQUIRE(!live);
    client = index_client(path, sync, 0, 1, NULL, &e);
    other = index_artifact(client, "team/index:../other", LC_POUCH_INDEX_PACKED,
                           7U, &e);
    index_artifact_expect(other, "sibling", 7U, &e);
    lc_free_with_allocator(&allocator, other);
    lc_client_close(client);
    REQUIRE(!live);
    one_file(path);
    REQUIRE(!unlink(path));
  }
  free(bytes);
  lc_error_cleanup(&e);
}

static const char index_namespace[] = "query/model";
static void index_put_in(lc_client *client, const char *ns, const char *key,
                         const char *json, lc_error *e) {
  lc_source *source = NULL;
  lc_pouch_state_write_result result = {0};
  ok(lc_source_from_memory(json, strlen(json), &source, e), e);
  ok(lc_pouch_state_write(index_pouch(client), ns, key, source, NULL, &result,
                          e),
     e);
  source->close(source);
  lc_pouch_state_write_result_cleanup(&allocator, &result);
}
static void index_put(lc_client *client, const char *key, const char *json,
                      lc_error *e) {
  index_put_in(client, index_namespace, key, json, e);
}
static int index_flush(lc_client *client, lc_error *e) {
  lc_index_flush_req request;
  lc_index_flush_res result = {0};
  lc_index_flush_req_init(&request);
  request.ns = index_namespace;
  request.mode = "sync";
  int rc = lc_flush_index(client, &request, &result, e);
  if (rc == LC_OK)
    REQUIRE(result.flushed && !result.pending);
  lc_index_flush_res_cleanup(&result);
  return rc;
}
static void index_seed(lc_client *client, int action, lc_error *e) {
  index_put(client, "one",
            "{\"kind\":\"active\",\"rank\":1,\"title\":\"audit old\","
            "\"tags\":[\"base\"],\"when\":\"2026-11-16T00:00:00Z\"}",
            e);
  index_put(client, "two",
            "{\"kind\":\"inactive\",\"rank\":2,\"title\":\"other\"}", e);
  if (!action)
    return;
  ok(index_flush(client, e), e);
  if (action == 2)
    for (unsigned int i = 0U; i < 7U; ++i) {
      char value[160];
      REQUIRE(
          snprintf(value, sizeof(value),
                   "{\"kind\":\"active\",\"rank\":%u,\"title\":\"audit old\"}",
                   i + 2U) > 0);
      index_put(client, "one", value, e);
      ok(index_flush(client, e), e);
    }
}
static void index_change(lc_client *client, lc_error *e) {
  lc_pouch_state_write_result result = {0};
  index_put(client, "one",
            "{\"kind\":\"inactive\",\"rank\":5,\"title\":\"audit new\"}", e);
  ok(lc_pouch_state_delete(index_pouch(client), index_namespace, "two", NULL,
                           &result, e),
     e);
  lc_pouch_state_write_result_cleanup(&allocator, &result);
  index_put(client, "three",
            "{\"kind\":\"active\",\"rank\":3,\"title\":\"audit fresh\","
            "\"tags\":[\"base\"],\"when\":\"2026-11-17T00:00:00Z\"}",
            e);
}
static int index_action(lc_client *client, int action, lc_error *e) {
  if (action != 3)
    return index_flush(client, e);
  lc_pouch_maintenance_options options;
  lc_pouch_maintenance_result result = {0};
  memset(&options, 0, sizeof(options));
  options.ns = index_namespace;
  options.force = 1;
  int rc = lc_pouch_maintenance_run(index_pouch(client), &options, &result, e);
  if (rc == LC_OK)
    REQUIRE(result.compacted);
  lc_pouch_maintenance_result_cleanup(&allocator, &result);
  return rc;
}
typedef struct index_keys {
  char key[32];
  size_t length, count;
  unsigned int mask;
} index_keys;
static int index_key_begin(void *context, lc_error *e) {
  index_keys *keys = context;
  (void)e;
  keys->length = 0U;
  return 1;
}
static int index_key_chunk(void *context, const char *bytes, size_t size,
                           lc_error *e) {
  index_keys *keys = context;
  (void)e;
  REQUIRE(size < sizeof(keys->key) - keys->length);
  memcpy(keys->key + keys->length, bytes, size);
  keys->length += size;
  keys->key[keys->length] = '\0';
  return 1;
}
static int index_key_end(void *context, lc_error *e) {
  index_keys *keys = context;
  (void)e;
  unsigned int bit = !strcmp(keys->key, "one")     ? 1U
                     : !strcmp(keys->key, "two")   ? 2U
                     : !strcmp(keys->key, "three") ? 4U
                                                   : 0U;
  REQUIRE(bit && !(keys->mask & bit));
  keys->mask |= bit;
  ++keys->count;
  return 1;
}
static const char *const index_selectors[] = {
    "eq{field=/kind,value=active}",
    "range{field=/rank,gte=2}",
    "prefix{field=/title,value=audit}",
    "icontains{field=/...,value=AUDIT}",
    "{\"in\":{\"field\":\"/tags[]\",\"any\":[\"base\"]}}",
    "{\"exists\":\"/when\"}",
    "date{field=/when,after=2026-11-16}"};
static unsigned int index_query_in(lc_client *client, const char *ns,
                                   const char *engine, const char *selector,
                                   lc_error *e) {
  lc_query_req request;
  lc_query_res result = {0};
  lc_query_key_handler handler = {index_key_begin, index_key_chunk,
                                  index_key_end};
  index_keys keys = {0};
  lc_query_req_init(&request);
  request.ns = ns;
  request.engine = engine;
  if (selector[0] == '{')
    request.selector_json = selector;
  else
    request.selector_lql = selector;
  int rc = lc_query_keys(client, &request, &handler, &keys, &result, e);
  if (rc != LC_OK)
    fprintf(stderr, "index query engine=%s selector=%s\n", engine, selector);
  ok(rc, e);
  if (!strcmp(engine, "index"))
    REQUIRE(result.metadata_json &&
            strstr(result.metadata_json, "\"engine\":\"index\""));
  lc_query_res_cleanup(&result);
  return keys.mask;
}
static unsigned int index_query(lc_client *client, const char *engine,
                                const char *selector, lc_error *e) {
  return index_query_in(client, index_namespace, engine, selector, e);
}
static void index_verify(lc_client *client, int action, int completed,
                         lc_error *e) {
  static const unsigned int expected[2][7] = {{1U, 2U, 1U, 1U, 1U, 1U, 0U},
                                              {4U, 5U, 5U, 5U, 4U, 4U, 4U}};
  ok(index_flush(client, e), e);
  for (size_t i = 0U; i < sizeof(index_selectors) / sizeof(index_selectors[0]);
       ++i) {
    unsigned int scan = index_query(client, "scan", index_selectors[i], e);
    unsigned int indexed = index_query(client, "index", index_selectors[i], e);
    REQUIRE(scan == indexed);
    if (completed && indexed != expected[action != 0][i])
      fprintf(stderr,
              "index recovery action=%d selector=%zu actual=%u expected=%u\n",
              action, i, indexed, expected[action != 0][i]);
    if (completed)
      REQUIRE(indexed == expected[action != 0][i]);
  }
}
static void index_shared_lease(lc_client *writer, lc_client *peer,
                               lc_error *e) {
  lc_acquire_req request;
  lc_lease *lease = NULL;
  lc_pouch_generation base = 0, seq = 0;
  const char *const staged[] = {"{\"kind\":\"intermediate\"}",
                                "{\"kind\":\"ready\"}"};
  ok(lc_pouch_query_index_manifest_seq(index_pouch(peer), index_namespace,
                                       &base, e),
     e);
  REQUIRE(base > 0);
  lc_acquire_req_init(&request);
  request.ns = index_namespace;
  request.key = "one";
  request.owner = "index-shared";
  request.ttl_seconds = 300L;
  ok(lc_acquire(writer, &request, &lease, e), e);
  for (size_t i = 0; i < sizeof(staged) / sizeof(*staged); ++i) {
    lc_source *source = NULL;
    ok(lc_source_from_memory(staged[i], strlen(staged[i]), &source, e), e);
    ok(lc_lease_update(lease, source, NULL, e), e);
    lc_source_close(source);
    ok(index_flush(peer, e), e);
    ok(lc_pouch_query_index_manifest_seq(index_pouch(peer), index_namespace,
                                         &seq, e),
       e);
    REQUIRE(seq == base &&
            index_query(peer, "index", index_selectors[0], e) == 1U);
  }
  ok(lc_lease_release(lease, NULL, e), e);
  ok(index_flush(peer, e), e);
  REQUIRE(index_query(writer, "index", "eq{field=/kind,value=ready}", e) == 1U);
  REQUIRE(index_query(writer, "index", "eq{field=/kind,value=intermediate}",
                      e) == 0U);
}
static void index_shared(const char *directory) {
  char path[1400];
  lc_error e = {0};
  REQUIRE(snprintf(path, sizeof(path), "%s/index-shared", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync <= 1; ++sync) {
    lc_client *a =
        index_client_admit(path, sync, 1, 1, 1, NULL, NULL, NULL, &e);
    lc_client *b =
        index_client_admit(path, sync, 0, 1, 1, NULL, NULL, NULL, &e);
    lc_pouch_generation state = 0, indexed = 0;
    index_seed(a, 1, &e);
    REQUIRE(index_query(b, "index", index_selectors[0], &e) == 1U);
    index_shared_lease(b, a, &e);
    index_change(b, &e);
    ok(lc_pouch_query_index_manifest_seq(index_pouch(a), index_namespace,
                                         &indexed, &e),
       &e);
    ok(lc_pouch_state_query_index_seq(index_pouch(a), index_namespace, &state,
                                      &e),
       &e);
    REQUIRE(indexed > 0 && indexed < state);
    ok(index_flush(b, &e), &e);
    REQUIRE(index_query(a, "index", index_selectors[0], &e) == 4U);
    index_verify(a, 1, 1, &e);
    lc_client_close(b);
    lc_client_close(a);
    REQUIRE(!live);
    a = index_client_admit(path, sync, 0, 1, 1, NULL, NULL, NULL, &e);
    ok(lc_pouch_query_index_manifest_seq(index_pouch(a), index_namespace,
                                         &indexed, &e),
       &e);
    REQUIRE(indexed == state);
    REQUIRE(index_query(a, "index", index_selectors[0], &e) == 4U);
    lc_client_close(a);
    REQUIRE(!live);
    one_file(path);
    REQUIRE(!unlink(path));
  }
  lc_error_cleanup(&e);
}
static int index_immediate_sync(void *context, int fd) {
  (void)context;
  (void)fd;
  return 0;
}
static size_t index_trial(const char *directory, int action, int sync, int kind,
                          int persist, size_t cut) {
  lc_error e;
  lc_error_init(&e);
  char path[1400];
  REQUIRE(snprintf(path, sizeof(path), "%s/index-fault", directory) <
          (int)sizeof(path));
  retention_disk disk = {0};
  if (kind != 2) {
    disk.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
    disk.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
    disk.scratch = malloc(PS_PAGE);
    REQUIRE(disk.stable && disk.dirty && disk.scratch);
    disk.persist = persist != 0;
    disk.complete_writes = persist == 2;
  }
  ps_io io = {&disk,
              retention_disk_read,
              retention_disk_write,
              retention_disk_sync,
              retention_disk_reserve,
              NULL};
  ps_io allocations = {&disk.fault,          disk_read,    disk_write,
                       index_immediate_sync, disk_reserve, NULL};
  const ps_io *selected = kind == 2 ? &allocations : &io;
  lc_client *client = index_client(path, sync, 1, 1, selected, &e);
  index_seed(client, action, &e);
  if (action == 3) {
    lc_client_close(client);
    client = index_client(path, sync, 0, 0, selected, &e);
  }
  ok(ps_checkpoint(ps_root_space(ps_pouch_state_root(index_pouch(client))), &e),
     &e);
  if (action)
    index_change(client, &e);
  allocation = 0U;
  fail_at = cut;
  armed = kind == 2;
  disk.fault.calls = 0U;
  disk.fault.fail = cut;
  disk.fault.armed = kind != 2;
  int rc = index_action(client, action, &e);
  armed = disk.fault.armed = 0;
  size_t sites = kind == 2 ? allocation : disk.fault.calls;
  if (!cut)
    ok(rc, &e);
  else
    REQUIRE(sites >= cut);
  clear(&e);
  if (kind == 2 && action != 3)
    index_verify(client, action, 1, &e);
  disk.frozen = 1;
  lc_client_close(client);
  if (live) {
    fprintf(
        stderr,
        "index allocation leak action=%d sync=%d kind=%d cut=%zu live=%zu\n",
        action, sync, kind, cut, live);
#ifdef __SANITIZE_ADDRESS__
    (void)__lsan_do_recoverable_leak_check();
#endif
  }
  REQUIRE(!live);
  if (kind != 2)
    retention_disk_restore(&disk, path);
  client = index_client(path, sync, 0, 1, kind == 2 ? &allocations : NULL, &e);
  index_verify(client, action, rc == LC_OK, &e);
  lc_client_close(client);
  REQUIRE(!live);
  client = index_client(path, sync, 0, 1, kind == 2 ? &allocations : NULL, &e);
  index_verify(client, action, rc == LC_OK, &e);
  lc_client_close(client);
  REQUIRE(!live);
  one_file(path);
  REQUIRE(!unlink(path));
  free(disk.stable);
  free(disk.dirty);
  free(disk.scratch);
  lc_error_cleanup(&e);
  return sites;
}
static void index_faults(const char *directory, int kind) {
  size_t cuts = 0U;
  for (int sync = 0; sync < 2; ++sync)
    for (int persist = 0; persist < (kind == 1 ? 3 : 1); ++persist)
      for (int action = 0; action < 4; ++action) {
        size_t sites = index_trial(directory, action, sync, kind, persist, 0U);
        fprintf(stderr,
                "index kind=%d sync=%d persist=%d action=%d sites=%zu\n", kind,
                sync, persist, action, sites);
        if (kind)
          for (size_t cut = 1U; cut <= sites; ++cut) {
            (void)index_trial(directory, action, sync, kind, persist, cut);
            ++cuts;
          }
      }
  printf("index %s cuts=%zu passed\n",
         kind == 2 ? "allocation"
         : kind    ? "I/O"
                   : "durability",
         cuts);
}
static uint64_t index_fixture_u64(const unsigned char *p) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (8 * i);
  return value;
}
static void index_corrupt_generation(const char *directory) {
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-corrupt-generation",
                   directory) < (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync) {
    retention_disk disk = {0};
    disk.stable = calloc(1, (size_t)PS_DEFAULT_CAPACITY);
    disk.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1);
    disk.scratch = malloc(PS_PAGE);
    REQUIRE(disk.stable && disk.dirty && disk.scratch);
    ps_io io = {&disk,
                retention_disk_read,
                retention_disk_write,
                retention_disk_sync,
                retention_disk_reserve,
                NULL};
    lc_client *client = index_client(path, sync, 1, 1, &io, &e);
    index_seed(client, 0, &e);
    ok(index_flush(client, &e), &e);
    ps_root *root = ps_pouch_state_root(index_pouch(client));
    ps_namespace *ns = NULL;
    ps_namespace_info identity;
    lc_pouch_generation sequence = 0;
    ok(ps_root_namespace_acquire(root, index_namespace, 0, &ns, &e), &e);
    ok(ps_namespace_identity(ns, &identity, &e), &e);
    ps_namespace_release(ns);
    ok(lc_pouch_state_query_index_seq(index_pouch(client), index_namespace,
                                      &sequence, &e),
       &e);
    lc_pouch_index_artifact *packed = index_artifact(
        client, index_namespace, LC_POUCH_INDEX_PACKED, sequence, &e);
    lc_pouch_index_signature original = {0};
    ok(lc_pouch_index_artifact_signature(packed, &original, &e), &e);
    REQUIRE(original.present);
    lc_free_with_allocator(&allocator, packed);
    /* A later acknowledged canonical owner must survive derived corruption.
     * Its different namespace leaves the indexed watermark unchanged. */
    index_put_in(client, "query/neighbor", "one", "{\"title\":\"neighbor\"}",
                 &e);
    ok(ps_flush(ps_root_space(root), &e), &e);
    ps_stats stats = {0};
    ok(ps_status(ps_root_space(root), &stats, &e), &e);
    uint64_t payload = 0;
    size_t at = 0;
    while (at < stats.journal_bytes) {
      const unsigned char *frame = disk.stable + PS_JOURNAL_OFFSET + at;
      size_t bytes = (uint32_t)index_fixture_u64(frame + 4);
      REQUIRE(bytes >= 64 && bytes <= stats.journal_bytes - at);
      if (!memcmp(frame, "PSR1", 4)) {
        size_t member = 64;
        while (member < bytes) {
          const unsigned char *record = frame + member;
          size_t span = (uint32_t)index_fixture_u64(record + 4);
          REQUIRE(span >= 64 && span <= bytes - member);
          if ((uint32_t)index_fixture_u64(record + 16) ==
                  PS_QUERY_FIRST + 2 * LC_POUCH_INDEX_PACKED + 1 &&
              index_fixture_u64(record + 24) == identity.owner &&
              index_fixture_u64(record + 32) == sequence) {
            REQUIRE(span >= 88);
            payload = index_fixture_u64(record + 64);
          }
          member += span;
        }
      }
      at += bytes;
    }
    REQUIRE(payload >= PS_DATA && payload < PS_DEFAULT_CAPACITY);
    disk.frozen = 1;
    lc_client_close(client);
    REQUIRE(!live);
    disk.stable[payload] ^= 1U;
    retention_disk_restore(&disk, path);
    client = index_client(path, sync, 0, 1, NULL, &e);
    /* Query before explicit repair: a valid manifest cannot make the damaged
     * generation authoritative or let it discard the later canonical owner. */
    REQUIRE(index_query(client, "index", index_selectors[0], &e) == 1U);
    REQUIRE(index_query_in(client, "query/neighbor", "scan",
                           "prefix{field=/title,value=neighbor}", &e) == 1U);
    index_verify(client, 0, 1, &e);
    packed = index_artifact(client, index_namespace, LC_POUCH_INDEX_PACKED,
                            sequence, &e);
    lc_pouch_index_signature repaired = {0};
    ok(lc_pouch_index_artifact_signature(packed, &repaired, &e), &e);
    REQUIRE(repaired.present && repaired.mtime != original.mtime);
    lc_free_with_allocator(&allocator, packed);
    lc_client_close(client);
    REQUIRE(!live);
    one_file(path);
    REQUIRE(!unlink(path));
    free(disk.stable);
    free(disk.dirty);
    free(disk.scratch);
  }
  lc_error_cleanup(&e);
}
static void index_lost_revision(const char *directory) {
  /* Persist complete descriptor writes even when their acknowledgement is
   * lost, while earlier query payload writes may disappear. Canonical state
   * remains authoritative and the first validating flush must rebuild. */
  size_t sites = index_trial(directory, 0, 0, 1, 2, 0);
  for (size_t cut = 1; cut <= sites; ++cut) {
    fprintf(stderr, "lost query revision cut=%zu/%zu\n", cut, sites);
    (void)index_trial(directory, 0, 0, 1, 2, cut);
  }
  index_corrupt_generation(directory);
}
#endif
