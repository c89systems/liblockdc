#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "pouch.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

static size_t checks, allocation, fail_at, live;
static int armed;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static int fails(void) {
  return __atomic_load_n(&armed, __ATOMIC_ACQUIRE) &&
         __atomic_add_fetch(&allocation, 1, __ATOMIC_RELAXED) == fail_at;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t s) {
  return fails() ? NULL : __real_lc_calloc_with_allocator(a, n, s);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
typedef union header {
  long double alignment;
  struct {
    uintptr_t cookie;
    size_t size;
  } value;
} header;
static void *allocate(void *context, size_t n) {
  header *h = malloc(sizeof(*h) + n);
  (void)context;
  if (!h)
    return NULL;
  h->value.cookie = (uintptr_t)h ^ UINT64_C(0x15263748abcd9876);
  h->value.size = n;
  __atomic_add_fetch(&live, 1, __ATOMIC_RELAXED);
  return h + 1;
}
static void deallocate(void *context, void *p) {
  (void)context;
  if (!p)
    return;
  header *h = (header *)p - 1;
  REQUIRE(h->value.cookie == ((uintptr_t)h ^ UINT64_C(0x15263748abcd9876)));
  h->value.cookie = 0;
  __atomic_sub_fetch(&live, 1, __ATOMIC_RELAXED);
  free(h);
}
static void *resize(void *context, void *p, size_t n) {
  void *next = allocate(context, n);
  if (next && p) {
    header *h = (header *)p - 1;
    memcpy(next, p, h->value.size < n ? h->value.size : n);
    deallocate(context, p);
  }
  return next;
}
static lc_allocator allocator = {allocate, resize, deallocate, NULL};
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "SDK %d: %s\n", rc, e->message ? e->message : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
typedef struct value {
  lonejson_int64 value;
} value;
static const lonejson_field value_fields[] = {
    LONEJSON_FIELD_I64(value, value, "value")};
LONEJSON_MAP_DEFINE(value_map, value, value_fields);
typedef struct input {
  lc_source pub;
  const char *bytes;
  size_t at;
} input;
static size_t pull(lc_source *pub, void *bytes, size_t n, lc_error *e) {
  input *s = pub->impl;
  (void)e;
  if (n > 7)
    n = 7;
  size_t left = strlen(s->bytes) - s->at;
  if (n > left)
    n = left;
  memcpy(bytes, s->bytes + s->at, n);
  s->at += n;
  return n;
}
static void seed(lc_client *client, const char *ns, const char *key,
                 const char *body, int hidden, int object, lc_error *e) {
  lc_pouch_state_write_options o = {0};
  lc_pouch_state_write_result w = {0};
  input s = {0};
  s.pub.read = pull;
  s.pub.impl = &s;
  s.bytes = body;
  o.content_type = "application/json";
  o.has_query_hidden = o.query_hidden = hidden;
  o.object_record = object;
  ok(lc_pouch_state_write(((lc_client_handle *)client)->pouch, ns, key, &s.pub,
                          &o, &w, e),
     e);
  REQUIRE(w.version == 1 && s.at == strlen(body));
  lc_pouch_state_write_result_cleanup(&((lc_client_handle *)client)->allocator,
                                      &w);
}
static void read_value(lc_client *client, const char *ns, const char *key,
                       const char *body, lc_error *e) {
  lc_sink *sink = NULL;
  lc_get_res r = {0};
  const void *bytes;
  size_t n;
  ok(lc_sink_to_memory(&sink, e), e);
  ok(ns ? client->get_in_namespace(client, ns, key, NULL, sink, &r, e)
        : client->get(client, key, NULL, sink, &r, e),
     e);
  REQUIRE(!r.no_content && r.version == 1 && r.etag &&
          !strcmp(r.content_type, "application/json"));
  ok(lc_sink_memory_bytes(sink, &bytes, &n, e), e);
  REQUIRE(n == strlen(body) && !memcmp(bytes, body, n));
  lc_get_res_cleanup(&r);
  sink->close(sink);
}
typedef struct keys {
  size_t at, count;
  unsigned int mask;
  int fail;
  char key[64];
} keys;
static int key_begin(void *context, lc_error *e) {
  keys *k = context;
  (void)e;
  k->at = 0;
  return 1;
}
static int key_chunk(void *context, const char *bytes, size_t n, lc_error *e) {
  keys *k = context;
  (void)e;
  REQUIRE(n < sizeof(k->key) - k->at);
  memcpy(k->key + k->at, bytes, n);
  k->at += n;
  k->key[k->at] = 0;
  return 1;
}
static int key_end(void *context, lc_error *e) {
  keys *k = context;
  (void)e;
  unsigned int bit = !strcmp(k->key, "one")      ? 1U
                     : !strcmp(k->key, "object") ? 2U
                                                 : 0;
  REQUIRE(bit && !(k->mask & bit));
  k->mask |= bit;
  ++k->count;
  if (k->fail) {
    lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected key callback error", NULL,
                 NULL, "test");
    return 0;
  }
  return 1;
}
static void query(lc_client *client, lc_error *e) {
  const lc_query_key_handler handler = {key_begin, key_chunk, key_end};
  lc_query_req q = {0};
  lc_query_res r = {0};
  keys k = {0};
  q.engine = "scan";
  q.limit = 10;
  q.selector_json = "{\"eq\":{\"field\":\"/value\",\"value\":1}}";
  ok(client->query_keys(client, &q, &handler, &k, &r, e), e);
  /* Pure scan queries report no index freshness sequence in the SDK. */
  REQUIRE(k.count == 2 && k.mask == 3 && !r.cursor && r.index_seq == 0);
  lc_query_res_cleanup(&r);
  memset(&k, 0, sizeof(k));
  k.fail = 1;
  REQUIRE(client->query_keys(client, &q, &handler, &k, &r, e) ==
          LC_ERR_TRANSPORT);
  REQUIRE(k.count == 1);
  clear(e);
  lc_query_res_cleanup(&r);
  memset(&k, 0, sizeof(k));
  q.limit = 1;
  char *cursor = NULL;
  size_t pages = 0;
  do {
    q.cursor = cursor;
    ok(client->query_keys(client, &q, &handler, &k, &r, e), e);
    free(cursor);
    cursor = r.cursor;
    r.cursor = NULL;
    lc_query_res_cleanup(&r);
    REQUIRE(++pages <= 3);
  } while (cursor && cursor[0]);
  free(cursor);
  REQUIRE(k.count == 2 && k.mask == 3 && pages >= 2);
}
static void one_file(const char *path) {
  struct stat s;
  REQUIRE(stat(path, &s) == 0 && S_ISREG(s.st_mode) &&
          s.st_size == PS_DEFAULT_CAPACITY);
}
static void namespace_config(lc_client *client, int cold, char **etag,
                             lc_error *e) {
  lc_namespace_config_req req = {0};
  lc_namespace_config_res r = {0};
  ok(client->get_namespace_config(client, &req, &r, e), e);
  REQUIRE(!strcmp(r.ns, "team/alpha") && !strcmp(r.preferred_engine, "scan"));
  if (cold) {
    REQUIRE(r.etag && !strcmp(r.etag, *etag) &&
            !strcmp(r.fallback_engine, "none"));
  } else {
    REQUIRE(r.etag && !r.etag[0]);
    lc_namespace_config_res_cleanup(&r);
    req.preferred_engine = "scan";
    req.fallback_engine = "none";
    ok(client->update_namespace_config(client, &req, &r, e), e);
    REQUIRE(r.etag && !strcmp(r.fallback_engine, "none"));
    *etag = strdup(r.etag);
    REQUIRE(*etag);
  }
  lc_namespace_config_res_cleanup(&r);
  req.preferred_engine = "index";
  req.if_etag = "wrong-etag";
  REQUIRE(client->update_namespace_config(client, &req, &r, e) ==
          LC_ERR_INVALID);
  clear(e);
  lc_namespace_config_res_cleanup(&r);
  req.preferred_engine = req.if_etag = NULL;
  ok(client->get_namespace_config(client, &req, &r, e), e);
  REQUIRE(!strcmp(r.etag, *etag) && !strcmp(r.preferred_engine, "scan"));
  lc_namespace_config_res_cleanup(&r);
}
typedef struct closing_sink {
  lc_sink pub;
  lc_client **client;
  size_t bytes, calls;
} closing_sink;
static int close_during_copy(lc_sink *pub, const void *bytes, size_t n,
                             lc_error *e) {
  closing_sink *s = pub->impl;
  (void)e;
  REQUIRE(s->bytes + n <= strlen("{\"value\":1}"));
  REQUIRE(!memcmp(bytes, "{\"value\":1}" + s->bytes, n));
  if (!s->calls++) {
    lc_client_close(*s->client);
    *s->client = NULL;
  }
  s->bytes += n;
  return 1;
}
static void closing_get(lc_client **client, lc_error *e) {
  closing_sink sink = {0};
  sink.pub.write = close_during_copy;
  sink.pub.impl = &sink;
  sink.client = client;
  lc_get_res r = {0};
  lc_client *borrowed = *client;
  ok(borrowed->get(borrowed, "one", NULL, &sink.pub, &r, e), e);
  REQUIRE(!*client && sink.calls && sink.bytes == strlen("{\"value\":1}") &&
          r.version == 1);
  lc_get_res_cleanup(&r);
  REQUIRE(!live);
}
static void lifecycle(const char *directory) {
  lc_error e;
  char *key = NULL;
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  for (int native = 1; native >= 0; --native)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int sync = 0; sync < 2; ++sync) {
        char path[1400], endpoint[1600];
        REQUIRE(snprintf(path, sizeof(path), "%s/store-%d-%d-%d", directory,
                         native, crypto, sync) < (int)sizeof(path));
        REQUIRE(
            snprintf(endpoint, sizeof(endpoint),
                     "pouch://"
                     "%s?single_writer=true&background_compaction=false&query_"
                     "indexing=false&queue_watch=false&query_engine=scan",
                     path) < (int)sizeof(endpoint));
        const char *endpoints[] = {endpoint};
        lc_pouch_settings settings;
        lc_pouch_settings_init(&settings);
        settings.set_mask =
            LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION;
        settings.durable_sync = sync;
        settings.compression = crypto & 2 ? "zlib" : "none";
        lc_client_config cfg;
        lc_client_config_init(&cfg);
        cfg.endpoints = endpoints;
        cfg.allocator = allocator;
        cfg.endpoint_count = 1;
        cfg.default_namespace = "team/alpha";
        cfg.pouch_settings = &settings;
        cfg.pouch_crypto_key = crypto & 1 ? key : NULL;
        lc_client *client = NULL;
        char *config_etag = NULL;
        for (int cold = 0; cold < 2; ++cold) {
          ok(native ? lc_client_open(&cfg, &client, &e)
                    : ps_pouch_client_open(&cfg, !cold, NULL, &client, &e),
             &e);
          lc_client_handle *handle = (lc_client_handle *)client;
          REQUIRE(handle->is_pouch && !handle->engine &&
                  handle->pouch->durable_sync == sync &&
                  !strcmp(handle->pouch_compression, settings.compression) &&
                  !strcmp(client->default_namespace, cfg.default_namespace));
          if (!cold) {
            seed(client, "team/alpha", "one", "{\"value\":1}", 0, 0, &e);
            seed(client, "team/alpha", "two", "{\"value\":2}", 0, 0, &e);
            seed(client, "team/alpha", "hidden", "{\"value\":1}", 1, 0, &e);
            seed(client, "team/alpha", "object", "{\"value\":1}", 0, 1, &e);
            seed(client, "team/alpha", "state/one/attachments/data",
                 "{\"value\":1}", 1, 1, &e);
            seed(client, "other/ns", "one", "{\"value\":3}", 0, 0, &e);
          }
          read_value(client, NULL, "one", "{\"value\":1}", &e);
          read_value(client, "other/ns", "one", "{\"value\":3}", &e);
          value doc = {0};
          lc_get_res r = {0};
          ok(client->load(client, "one", &value_map, &doc, NULL, &r, &e), &e);
          REQUIRE(doc.value == 1 && r.version == 1);
          lc_get_res_cleanup(&r);
          namespace_config(client, cold, &config_etag, &e);
          query(client, &e);
          if (cold)
            closing_get(&client, &e);
          else {
            lc_client_close(client);
            client = NULL;
          }
          REQUIRE(!live);
          if (!native)
            one_file(path);
        }
        free(config_etag);
      }
  DIR *entries = opendir(directory);
  REQUIRE(entries);
  struct dirent *entry;
  size_t roots = 0;
  while ((entry = readdir(entries)) != NULL) {
    /* The fixture owner marker belongs to the test runner, not a store. */
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    REQUIRE(strlen(entry->d_name) == 11 &&
            !strncmp(entry->d_name, "store-", 6) && entry->d_name[6] >= '0' &&
            entry->d_name[6] <= '1' && entry->d_name[7] == '-' &&
            entry->d_name[8] >= '0' && entry->d_name[8] <= '3' &&
            entry->d_name[9] == '-' && entry->d_name[10] >= '0' &&
            entry->d_name[10] <= '1');
    ++roots;
  }
  REQUIRE(closedir(entries) == 0 && roots == 16);
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
}
typedef struct run {
  const char *directory, *mode;
} run;
typedef struct admission {
  int mode, calls;
} admission;
static int admit(void *context, const char *path, const lc_allocator *a,
                 const lc_pouch_open_options *logical, lc_pouch **out,
                 lc_error *e) {
  admission *c = context;
  ++c->calls;
  *out = NULL;
  REQUIRE(a->malloc_fn == allocate && a->free_fn == deallocate &&
          a->context == allocator.context && logical->single_writer &&
          logical->single_writer_set && !logical->query_indexing_enabled &&
          logical->query_indexing_enabled_set &&
          !logical->background_compaction_enabled);
  if (c->mode == 0)
    return LC_OK;
  if (c->mode == 2) {
    ps_pouch_state_options o = {0};
    o.logical = *logical;
    int rc = ps_pouch_state_open(path, 1, a, &o, out, e);
    if (rc != LC_OK)
      return rc;
  }
  return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected factory rejection",
                      NULL, NULL, "test");
}
static void validation(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  char path[1400], endpoint[1600];
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://"
                   "%s?single_writer=true&background_compaction=false&query_"
                   "indexing=false&queue_watch=false&query_engine=scan",
                   path) < (int)sizeof(endpoint));
  const char *endpoints[2] = {endpoint, endpoint};
  lc_client_config cfg;
  lc_client_config_init(&cfg);
  cfg.allocator = allocator;
  cfg.endpoints = endpoints;
  cfg.endpoint_count = 1;
  cfg.default_namespace = "team/alpha";
  lc_client *client = NULL;
  admission a = {0};
  for (int no_error = 0; no_error < 2; ++no_error) {
    lc_error *error = no_error ? NULL : &e;
    a.calls = 0;
    REQUIRE(lc_client_open_with_pouch_factory(NULL, admit, &a, &client,
                                              error) == LC_ERR_INVALID);
    REQUIRE(!a.calls && !live);
    const char *saved = endpoints[0];
    endpoints[0] = "http://127.0.0.1:1";
    REQUIRE(lc_client_open_with_pouch_factory(&cfg, admit, &a, &client,
                                              error) == LC_ERR_INVALID);
    endpoints[0] = saved;
    cfg.endpoint_count = 2;
    REQUIRE(lc_client_open_with_pouch_factory(&cfg, admit, &a, &client,
                                              error) == LC_ERR_INVALID);
    cfg.endpoint_count = 1;
    REQUIRE(lc_client_open_with_pouch_factory(&cfg, admit, &a, NULL, error) ==
            LC_ERR_INVALID);
    REQUIRE(!a.calls && !live && access(path, F_OK) != 0);
    clear(&e);
    for (int mode = 0; mode < 3; ++mode) {
      a.mode = mode;
      a.calls = 0;
      int rc =
          lc_client_open_with_pouch_factory(&cfg, admit, &a, &client, error);
      REQUIRE(rc == (mode ? LC_ERR_TRANSPORT : LC_ERR_PROTOCOL));
      REQUIRE(a.calls == 1 && !client && !live);
      clear(&e);
      if (mode == 2) {
        ok(ps_pouch_client_open(&cfg, 0, NULL, &client, &e), &e);
        lc_client_close(client);
        client = NULL;
        REQUIRE(!live && unlink(path) == 0);
      } else
        REQUIRE(access(path, F_OK) != 0);
    }
  }
  ps_pouch_client_options physical = {2 * PS_MIB, 2 * PS_MIB, NULL};
  ok(ps_pouch_client_open(&cfg, 1, &physical, &client, &e), &e);
  struct stat s;
  REQUIRE(stat(path, &s) == 0 && s.st_size == 2 * PS_MIB);
  seed(client, "team/alpha", "one", "{\"value\":1}", 0, 0, &e);
  lc_client_close(client);
  client = NULL;
  REQUIRE(ps_pouch_client_open(&cfg, 1, &physical, &client, &e) ==
          LC_ERR_TRANSPORT);
  REQUIRE(!client && !live);
  clear(&e);
  ok(ps_pouch_client_open(&cfg, 0, &physical, &client, &e), &e);
  read_value(client, NULL, "one", "{\"value\":1}", &e);
  lc_client_close(client);
  client = NULL;
  REQUIRE(!live && unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct io_fault {
  size_t calls, fail;
  int armed, partial;
} io_fault;
static int io_fails(io_fault *f) {
  if (f->armed && ++f->calls == f->fail) {
    errno = EIO;
    return 1;
  }
  return 0;
}
static ssize_t disk_read(void *context, int fd, void *bytes, size_t n,
                         off_t at) {
  io_fault *f = context;
  if (io_fails(f))
    return f->partial && n > 1 ? pread(fd, bytes, n / 2, at) : -1;
  return pread(fd, bytes, n, at);
}
static ssize_t disk_write(void *context, int fd, const void *bytes, size_t n,
                          off_t at) {
  io_fault *f = context;
  if (io_fails(f)) {
    if (f->partial && n > 1)
      (void)pwrite(fd, bytes, n / 2, at);
    errno = EIO;
    return -1;
  }
  return pwrite(fd, bytes, n, at);
}
static int disk_sync(void *context, int fd) {
  return io_fails(context) ? -1 : fdatasync(fd);
}
static int disk_reserve(void *context, int fd, off_t at, off_t n) {
  return io_fails(context) ? -1 : fallocate(fd, 0, at, n);
}
static void unlocked(const char *path) {
  int fd = open(path, O_RDWR);
  if (fd < 0) {
    REQUIRE(errno == ENOENT);
    return;
  }
  REQUIRE(flock(fd, LOCK_EX | LOCK_NB) == 0);
  REQUIRE(close(fd) == 0);
}
static size_t failure(const char *directory, size_t nth, int kind, int existing,
                      int crypto, int sync, const char *key, int no_error) {
  char path[1400], endpoint[1600];
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://"
                   "%s?single_writer=true&background_compaction=false&query_"
                   "indexing=false&queue_watch=false&query_engine=scan",
                   path) < (int)sizeof(endpoint));
  const char *endpoints[] = {endpoint};
  lc_pouch_settings settings;
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION;
  settings.durable_sync = sync;
  settings.compression = crypto ? "zlib" : "none";
  lc_client_config cfg;
  lc_client_config_init(&cfg);
  cfg.endpoints = endpoints;
  cfg.endpoint_count = 1;
  cfg.allocator = allocator;
  cfg.default_namespace = "team/alpha";
  cfg.pouch_settings = &settings;
  cfg.pouch_crypto_key = crypto ? key : NULL;
  io_fault f = {0};
  ps_io io = {&f, disk_read, disk_write, disk_sync, disk_reserve, NULL};
  ps_pouch_client_options physical = {0, 0, &io};
  lc_client *client = NULL;
  lc_error e;
  lc_error_init(&e);
  if (existing) {
    ok(ps_pouch_client_open(&cfg, 1, &physical, &client, &e), &e);
    seed(client, "team/alpha", "one", "{\"value\":1}", 0, 0, &e);
    lc_client_close(client);
    client = NULL;
    REQUIRE(!live);
  }
  allocation = f.calls = 0;
  fail_at = f.fail = nth;
  f.armed = kind != 0;
  f.partial = kind == 2;
  __atomic_store_n(&armed, kind == 0, __ATOMIC_RELEASE);
  int rc = ps_pouch_client_open(&cfg, !existing, &physical, &client,
                                no_error ? NULL : &e);
  __atomic_store_n(&armed, 0, __ATOMIC_RELEASE);
  f.armed = 0;
  size_t count = kind ? f.calls : allocation;
  if (rc != LC_OK && rc != (kind ? LC_ERR_TRANSPORT : LC_ERR_NOMEM))
    fprintf(stderr,
            "SDK open fault nth=%lu kind=%d existing=%d crypto=%d sync=%d "
            "null=%d rc=%d: %s\n",
            (unsigned long)nth, kind, existing, crypto, sync, no_error, rc,
            e.message ? e.message : "?");
  REQUIRE(rc == LC_OK || rc == (kind ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
  if (nth == SIZE_MAX)
    ok(rc, &e);
  REQUIRE(rc == LC_OK ? client != NULL : client == NULL);
  clear(&e);
  if (client) {
    if (existing)
      read_value(client, NULL, "one", "{\"value\":1}", &e);
    lc_client_close(client);
    client = NULL;
  }
  REQUIRE(!live);
  unlocked(path);
  if (existing) {
    ok(ps_pouch_client_open(&cfg, 0, &physical, &client, &e), &e);
    read_value(client, NULL, "one", "{\"value\":1}", &e);
    lc_client_close(client);
    client = NULL;
    REQUIRE(!live);
  }
  if (access(path, F_OK) == 0)
    REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
  return count;
}
static void faults(const char *directory, int io) {
  lc_error e;
  lc_error_init(&e);
  char *key = NULL;
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  size_t total = 0;
  for (int crypto = 0; crypto <= 3; crypto += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int existing = 0; existing < 2; ++existing)
        for (int kind = io ? 1 : 0; kind < (io ? 3 : 1); ++kind) {
          size_t n = failure(directory, SIZE_MAX, kind, existing, crypto, sync,
                             key, 0);
          REQUIRE(n > 0);
          total += n;
          for (size_t nth = 1; nth <= n; ++nth)
            for (int no_error = 0; no_error < 2; ++no_error)
              (void)failure(directory, nth, kind, existing, crypto, sync, key,
                            no_error);
        }
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  printf("SDK %s failure sites: %lu\n", io ? "I/O" : "allocation",
         (unsigned long)total);
}
static void *worker(void *context) {
  run *r = context;
  if (!strcmp(r->mode, "lifecycle"))
    lifecycle(r->directory);
  else if (!strcmp(r->mode, "validation"))
    validation(r->directory);
  else if (!strcmp(r->mode, "faults"))
    faults(r->directory, 0);
  else if (!strcmp(r->mode, "io-faults"))
    faults(r->directory, 1);
  else
    REQUIRE(0);
  return NULL;
}
int main(int argc, char **argv) {
  char prefix[1100], pattern[1200], directory[1200];
  pthread_t thread;
  REQUIRE(argc == 3);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-sdk-", argv[1]) <
          (int)sizeof(prefix));
  REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) <
          (int)sizeof(pattern));
  REQUIRE(lc_test_tmp_mkdtemp(pattern, directory, sizeof(directory), prefix));
  run r = {directory, argv[2]};
  REQUIRE(!pthread_create(&thread, NULL, worker, &r));
  REQUIRE(!pthread_join(thread, NULL));
  lc_test_tmp_cleanup_path(directory, prefix);
  printf("container SDK: %lu checks\n", (unsigned long)checks);
  return 0;
}
