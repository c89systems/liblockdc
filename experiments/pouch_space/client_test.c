#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "budget.h"
#include "fixture_barriers.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_retention_record.h"
#include "lc_pouch_txn_guard.h"
#include "namespace.h"
#include "pouch.h"
#include "pouch_internal.h"
#include "space_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static size_t checks, allocation, live;
static size_t fail_at __attribute__((unused));
static int armed;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
#ifdef __linux__
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
static size_t mutex_init_calls, mutex_init_fail;
static int mutex_init_armed;
int __real_pthread_mutex_init(pthread_mutex_t *, const pthread_mutexattr_t *);
int __wrap_pthread_mutex_init(pthread_mutex_t *mutex,
                              const pthread_mutexattr_t *attributes) {
  if (__atomic_load_n(&mutex_init_armed, __ATOMIC_ACQUIRE) &&
      __atomic_add_fetch(&mutex_init_calls, 1, __ATOMIC_RELAXED) ==
          mutex_init_fail)
    return EAGAIN;
  return __real_pthread_mutex_init(mutex, attributes);
}
#endif
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
  h->value.cookie = (uintptr_t)h ^ (uintptr_t)UINT64_C(0x15263748abcd9876);
  h->value.size = n;
  __atomic_add_fetch(&live, 1, __ATOMIC_RELAXED);
  return h + 1;
}
static void deallocate(void *context, void *p) {
  (void)context;
  if (!p)
    return;
  header *h = (header *)p - 1;
  REQUIRE(h->value.cookie ==
          ((uintptr_t)h ^ (uintptr_t)UINT64_C(0x15263748abcd9876)));
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
  static const char expected[] = "{\"value\":1}";
  closing_sink *s = pub->impl;
  (void)e;
  REQUIRE(s->bytes + n < sizeof(expected));
  REQUIRE(!memcmp(bytes, expected + s->bytes, n));
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
  return io_fails(context) ? -1 : ps_native_io.sync(NULL, fd);
}
static int disk_reserve(void *context, int fd, off_t at, off_t n) {
  return io_fails(context) ? -1 : ps_native_io.reserve(NULL, fd, at, n);
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
  fixture_barriers_model(1);
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
  fixture_barriers_model(0);
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  printf("SDK %s failure sites: %lu\n", io ? "I/O" : "allocation",
         (unsigned long)total);
}
static lc_pouch *visit_open(const char *path, int native, int sync, int create,
                            const ps_io *io, const lc_allocator *domain,
                            lc_error *e) {
  ps_pouch_state_options o = {0};
  lc_pouch *pouch = NULL;
  o.logical.single_writer = o.logical.single_writer_set = 1;
  o.logical.background_compaction_enabled_set = 1;
  o.logical.query_indexing_enabled_set = 1;
  o.logical.query_engine = "scan";
  o.logical.durable_sync = sync;
  o.io = io;
  ok(native ? lc_pouch_open(path, domain, &o.logical, &pouch, e)
            : ps_pouch_state_open(path, create, domain, &o, &pouch, e),
     e);
  return pouch;
}
static void namespace_artifacts(const char *directory, const char *prefix) {
  DIR *dir = opendir(directory);
  struct dirent *entry;
  unsigned int mask = 0;
  char expected[64];
  REQUIRE(dir);
  while ((entry = readdir(dir)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    unsigned int bit = 0;
    for (int native = 0; native < 2; ++native)
      for (int sync = 0; sync < 2; ++sync) {
        REQUIRE(snprintf(expected, sizeof(expected), "%s%d-%d", prefix, native,
                         sync) < (int)sizeof(expected));
        if (!strcmp(entry->d_name, expected))
          bit = 1U << (native * 2 + sync);
      }
    REQUIRE(bit && !(mask & bit));
    mask |= bit;
  }
  REQUIRE(closedir(dir) == 0 && mask == 15);
}
static const char *visit_names[] = {
    "team/alpha", "%2f", ".", "space name", ".txns", "created/during/visit"};
typedef struct namespace_walk {
  lc_pouch *pouch;
  size_t count;
  unsigned int mask;
  int mutate, fail;
} namespace_walk;
static int namespace_name(void *context, const char *name, lc_error *e) {
  namespace_walk *v = context;
  size_t i;
  for (i = 0; i < sizeof(visit_names) / sizeof(*visit_names); ++i)
    if (!strcmp(name, visit_names[i]))
      break;
  REQUIRE(i < sizeof(visit_names) / sizeof(*visit_names));
  REQUIRE(!(v->mask & (1U << i)));
  v->mask |= 1U << i;
  ++v->count;
  if (v->fail)
    return lc_error_set(e, LC_ERR_PENDING, 0, "namespace callback stopped",
                        NULL, NULL, "test");
  if (v->mutate && v->count == 1) {
    namespace_walk nested = {0};
    nested.pouch = v->pouch;
    ok(lc_pouch_ensure_namespace(v->pouch, visit_names[5], e), e);
    ok(lc_pouch_storage_namespaces_visit(v->pouch, namespace_name, &nested, e),
       e);
    REQUIRE(nested.mask == 63 && nested.count == 6);
  }
  return LC_OK;
}
static void namespace_expect(lc_pouch *pouch, unsigned int mask, lc_error *e) {
  namespace_walk v = {0};
  v.pouch = pouch;
  ok(lc_pouch_storage_namespaces_visit(pouch, namespace_name, &v, e), e);
  REQUIRE(v.mask == mask);
}
static size_t namespace_cold_failures(const char *path, int native, int sync,
                                      const ps_io *io, lc_pouch **pouch,
                                      lc_error *e) {
  lc_pouch_close(*pouch);
  REQUIRE(!live);
  *pouch = visit_open(path, native, sync, 0, io, &allocator, e);
  allocation = 0;
  fail_at = SIZE_MAX;
  armed = 1;
  namespace_expect(*pouch, 31, e);
  armed = 0;
  size_t sites = allocation;
  REQUIRE(sites);
  for (size_t nth = 1; nth <= sites; ++nth)
    for (int no_error = 0; no_error < 2; ++no_error) {
      lc_pouch_close(*pouch);
      REQUIRE(!live);
      *pouch = visit_open(path, native, sync, 0, io, &allocator, e);
      namespace_walk v = {0};
      allocation = 0;
      fail_at = nth;
      armed = 1;
      int rc = lc_pouch_storage_namespaces_visit(*pouch, namespace_name, &v,
                                                 no_error ? NULL : e);
      armed = 0;
      REQUIRE(rc == LC_ERR_NOMEM);
      clear(e);
      if (no_error) {
        lc_pouch_close(*pouch);
        REQUIRE(!live);
        *pouch = visit_open(path, native, sync, 0, io, &allocator, e);
      }
      namespace_expect(*pouch, 31, e);
    }
  return sites;
}
static void namespace_visits(const char *directory) {
  lc_error e;
  size_t sites = 0, cold_sites = 0;
  lc_error_init(&e);
  REQUIRE(lc_pouch_storage_namespaces_visit(NULL, namespace_name, NULL, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400], invalid[1500], saved[1500];
      io_fault f = {0};
      ps_io io = {&f, disk_read, disk_write, disk_sync, disk_reserve, NULL};
      REQUIRE(snprintf(path, sizeof(path), "%s/visit-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      lc_pouch *pouch = visit_open(path, native, sync, 1, &io, &allocator, &e);
      namespace_expect(pouch, 0, &e);
      REQUIRE(lc_pouch_storage_namespaces_visit(pouch, NULL, NULL, NULL) ==
              LC_ERR_INVALID);
      for (size_t i = 0; i < 5; ++i)
        ok(lc_pouch_ensure_namespace(pouch, visit_names[i], &e), &e);
      if (native) {
        const char *bad[] = {"%", "%0", "%zz", "%00", "%00x"};
        for (size_t i = 0; i < sizeof(bad) / sizeof(*bad); ++i) {
          REQUIRE(snprintf(invalid, sizeof(invalid), "%s/namespaces/%s", path,
                           bad[i]) < (int)sizeof(invalid));
          REQUIRE(mkdir(invalid, 0700) == 0);
        }
      }
      for (int cold = 0; cold < 2; ++cold) {
        if (cold) {
          lc_pouch_close(pouch);
          REQUIRE(!live);
          pouch = visit_open(path, native, sync, 0, &io, &allocator, &e);
        }
        f.calls = 0;
        f.armed = 1;
        namespace_expect(pouch, 31, &e);
        f.armed = 0;
        if (!native)
          REQUIRE(!f.calls); /* The resident catalog needs no physical I/O. */
        namespace_walk stopped = {0};
        stopped.fail = 1;
        REQUIRE(lc_pouch_storage_namespaces_visit(
                    pouch, namespace_name, &stopped, &e) == LC_ERR_PENDING);
        REQUIRE(stopped.count == 1 && e.message &&
                !strcmp(e.message, "namespace callback stopped"));
        clear(&e);
        allocation = 0;
        fail_at = SIZE_MAX;
        armed = 1;
        namespace_expect(pouch, 31, &e);
        armed = 0;
        size_t n = allocation, baseline = live;
        REQUIRE(n);
        sites += n;
        for (size_t nth = 1; nth <= n; ++nth)
          for (int no_error = 0; no_error < 2; ++no_error) {
            namespace_walk v = {0};
            allocation = 0;
            fail_at = nth;
            armed = 1;
            int rc = lc_pouch_storage_namespaces_visit(
                pouch, namespace_name, &v, no_error ? NULL : &e);
            armed = 0;
            REQUIRE(rc == LC_ERR_NOMEM);
            clear(&e);
            REQUIRE(live == baseline);
            namespace_expect(pouch, 31, &e);
          }
      }
      cold_sites +=
          namespace_cold_failures(path, native, sync, &io, &pouch, &e);
      namespace_walk v = {0};
      v.pouch = pouch;
      v.mutate = 1;
      ok(lc_pouch_storage_namespaces_visit(pouch, namespace_name, &v, &e), &e);
      REQUIRE(native ? (v.mask == 31 || v.mask == 63) : v.mask == 31);
      namespace_expect(pouch, 63, &e);
      if (!native) {
        f.calls = 0;
        f.fail = f.armed = 1;
        REQUIRE(ps_checkpoint(ps_root_space(ps_pouch_state_root(pouch)), &e) ==
                LC_ERR_TRANSPORT);
        f.armed = 0;
        clear(&e);
        namespace_walk denied = {0};
        REQUIRE(lc_pouch_storage_namespaces_visit(
                    pouch, namespace_name, &denied, &e) == LC_ERR_TRANSPORT);
        REQUIRE(!denied.count);
        clear(&e);
        lc_pouch_close(pouch);
        REQUIRE(!live);
        pouch = visit_open(path, 0, sync, 0, &io, &allocator, &e);
        namespace_expect(pouch, 63, &e);
      } else {
        namespace_walk denied = {0};
        REQUIRE(snprintf(invalid, sizeof(invalid), "%s/namespaces", path) <
                (int)sizeof(invalid));
        REQUIRE(snprintf(saved, sizeof(saved), "%s/namespaces-saved", path) <
                (int)sizeof(saved));
        REQUIRE(rename(invalid, saved) == 0);
        int fd = open(invalid, O_CREAT | O_EXCL | O_WRONLY, 0600);
        REQUIRE(fd >= 0 && close(fd) == 0);
        REQUIRE(lc_pouch_storage_namespaces_visit(
                    pouch, namespace_name, &denied, &e) == LC_ERR_TRANSPORT);
        REQUIRE(!denied.count);
        clear(&e);
        REQUIRE(unlink(invalid) == 0 && rename(saved, invalid) == 0);
        namespace_expect(pouch, 63, &e);
      }
      lc_pouch_close(pouch);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  printf("namespace visits: %zu warm and %zu cold allocation sites, "
         "both owners/policies, "
         "cold/warm, callback retry, nested mutation and poisoned admission\n",
         sites, cold_sites);
  namespace_artifacts(directory, "visit-");
  lc_error_cleanup(&e);
}
static double namespace_clock(clockid_t clock) {
  struct timespec t;
  REQUIRE(clock_gettime(clock, &t) == 0);
  return (double)t.tv_sec + (double)t.tv_nsec / 1000000000.0;
}
typedef struct visit_measurement {
  uint64_t mask;
  size_t count;
} visit_measurement;
static int measure_namespace(void *context, const char *name, lc_error *e) {
  visit_measurement *v = context;
  size_t id;
  if (strlen(name) != 7 || strncmp(name, "team/", 5) || name[5] < '0' ||
      name[5] > '6' || name[6] < '0' || name[6] > '9')
    return lc_error_set(e, LC_ERR_PROTOCOL, 0, "unexpected benchmark namespace",
                        NULL, NULL, "test");
  id = (size_t)(name[5] - '0') * 10 + (size_t)(name[6] - '0');
  if (id >= 64 || (v->mask & (UINT64_C(1) << id)))
    return lc_error_set(e, LC_ERR_PROTOCOL, 0, "duplicate benchmark namespace",
                        NULL, NULL, "test");
  v->mask |= UINT64_C(1) << id;
  ++v->count;
  return LC_OK;
}
static void measure_namespace_walk(lc_pouch *pouch, lc_error *e) {
  visit_measurement v = {0};
  ok(lc_pouch_storage_namespaces_visit(pouch, measure_namespace, &v, e), e);
  REQUIRE(v.mask == UINT64_MAX && v.count == 64);
}
typedef struct concurrent_visit {
  lc_pouch *pouch;
  size_t id;
} concurrent_visit;
static void *create_and_visit(void *context) {
  concurrent_visit *w = context;
  lc_error e;
  char name[24];
  lc_error_init(&e);
  for (size_t i = 0; i < 16; ++i) {
    REQUIRE(snprintf(name, sizeof(name), "team/%02zu", w->id * 16 + i) <
            (int)sizeof(name));
    ok(lc_pouch_ensure_namespace(w->pouch, name, &e), &e);
    visit_measurement v = {0};
    ok(lc_pouch_storage_namespaces_visit(w->pouch, measure_namespace, &v, &e),
       &e);
    REQUIRE(v.count > 0 && v.count <= 64);
  }
  lc_error_cleanup(&e);
  return NULL;
}
static void namespace_concurrency(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      pthread_t threads[4];
      concurrent_visit workers[4];
      char path[1400];
      REQUIRE(snprintf(path, sizeof(path), "%s/concurrent-%d-%d", directory,
                       native, sync) < (int)sizeof(path));
      lc_pouch *pouch = visit_open(path, native, sync, 1, NULL, &allocator, &e);
      for (size_t i = 0; i < 4; ++i) {
        workers[i].pouch = pouch;
        workers[i].id = i;
        REQUIRE(
            !pthread_create(threads + i, NULL, create_and_visit, workers + i));
      }
      for (size_t i = 0; i < 4; ++i)
        REQUIRE(!pthread_join(threads[i], NULL));
      measure_namespace_walk(pouch, &e);
      lc_pouch_close(pouch);
      REQUIRE(!live);
      pouch = visit_open(path, native, sync, 0, NULL, &allocator, &e);
      measure_namespace_walk(pouch, &e);
      lc_pouch_close(pouch);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "concurrent-");
  lc_error_cleanup(&e);
}
static void namespace_compare(const char *directory, int reverse) {
  lc_error e;
  lc_error_init(&e);
  for (int engine = 0; engine < 2; ++engine)
    for (int policy = 0; policy < 2; ++policy) {
      int native = reverse ? 1 - engine : engine;
      int sync = reverse ? 1 - policy : policy;
      char path[1400], name[64];
      REQUIRE(snprintf(path, sizeof(path), "%s/compare-%d-%d", directory,
                       native, sync) < (int)sizeof(path));
      lc_pouch *pouch = visit_open(path, native, sync, 1, NULL, NULL, &e);
      for (size_t i = 0; i < 64; ++i) {
        REQUIRE(snprintf(name, sizeof(name), "team/%02zu", i) <
                (int)sizeof(name));
        ok(lc_pouch_ensure_namespace(pouch, name, &e), &e);
      }
      for (int cold = 0; cold < 2; ++cold) {
        double open_ms = 0;
        if (cold) {
          lc_pouch_close(pouch);
          REQUIRE(!live);
          double start = namespace_clock(CLOCK_MONOTONIC);
          pouch = visit_open(path, native, sync, 0, NULL, NULL, &e);
          open_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        }
        double start = namespace_clock(CLOCK_MONOTONIC);
        measure_namespace_walk(pouch, &e);
        double first_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        double cpu_start = namespace_clock(CLOCK_PROCESS_CPUTIME_ID);
        start = namespace_clock(CLOCK_MONOTONIC);
        for (size_t i = 0; i < 128; ++i)
          measure_namespace_walk(pouch, &e);
        double elapsed_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        double cpu_ms =
            1000 * (namespace_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu_start);
        printf("namespace-compare native=%d sync=%d cold=%d names=64 walks=128 "
               "open_ms=%.6f first_ms=%.6f elapsed_ms=%.6f cpu_ms=%.6f "
               "verified=1\n",
               native, sync, cold, open_ms, first_ms, elapsed_ms, cpu_ms);
      }
      lc_pouch_close(pouch);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "compare-");
  lc_error_cleanup(&e);
}
static const char retention_xid[] = "00000000000000000000";
static void retention_only_artifact(const char *directory, const char *leaf) {
  DIR *dir = opendir(directory);
  struct dirent *entry;
  size_t count = 0;
  REQUIRE(dir);
  while ((entry = readdir(dir))) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    REQUIRE(!strcmp(entry->d_name, leaf));
    ++count;
  }
  REQUIRE(!closedir(dir) && count == 1);
}
static void retention_expect(lc_pouch *p, const char *ns, int present,
                             int phase, const char *key, lc_u64 fence,
                             lc_pouch_generation version, lc_error *e) {
  lc_pouch_retention_work w = {0};
  ok(lc_pouch_retention_work_read(p, ns, &w, e), e);
  REQUIRE(w.present == present && w.phase == phase);
  if (present && phase) {
    REQUIRE(w.txn_id && !strcmp(w.txn_id, retention_xid) && w.key &&
            !strcmp(w.key, key) && w.fencing_token == fence &&
            w.state_version == version);
  } else
    REQUIRE(!w.txn_id && !w.key && !w.fencing_token && !w.state_version);
  lc_pouch_retention_work_cleanup(p, &w);
}
static void retention_store(lc_pouch *p, const char *ns, int phase,
                            const char *key, lc_error *e) {
  ok(lc_pouch_retention_work_write(p, ns, phase, retention_xid, key, UINT64_MAX,
                                   UINT64_MAX, e),
     e);
}
static void retention_codec(void) {
  const char *invalid[] = {
      "",
      "LPR1\n0",
      "LPR1\n0\nextra",
      "LPR1\n3\n",
      "LPR1\n1\nx\n0\n0\nx\n",
      "LPR1\n1\n00000000000000000000\n-1\n0\nx\n",
      "LPR1\n1\n00000000000000000000\n18446744073709551616\n0\nx\n",
      "LPR1\n1\n00000000000000000000\n0\n0\n\n",
      "LPR1\n1\n00000000000000000000\n0\n0\n%\n",
      "LPR1\n1\n00000000000000000000\n0\n0\n%0\n",
      "LPR1\n1\n00000000000000000000\n0\n0\n%gg\n",
      "LPR1\n1\n00000000000000000000\n0\n0\n%00\n",
      "LPR1\n1\n00000000000000000000\n0\n0\na%00b\n",
      "LPR1\n1\n00000000000000000000\n0\n0\nx\nextra\n"};
  lc_error e;
  lc_pouch_retention_work w = {0};
  char *text = NULL;
  size_t length;
  lc_error_init(&e);
  for (size_t i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
    char *copy = strdup(invalid[i]);
    REQUIRE(copy);
    REQUIRE(lc_pouch_retention_record_decode(&allocator, copy, strlen(copy),
                                             "fixture", &w,
                                             &e) == LC_ERR_PROTOCOL);
    REQUIRE(!w.present && !w.txn_id && !w.key && !w.fencing_token &&
            !w.state_version && !live);
    free(copy);
    clear(&e);
  }
  char nul[] = "LPR1\n0\n\0tail";
  REQUIRE(lc_pouch_retention_record_decode(&allocator, nul, sizeof(nul) - 1,
                                           NULL, &w, NULL) == LC_ERR_PROTOCOL);
  REQUIRE(lc_pouch_retention_record_decode(&allocator, nul,
                                           LC_POUCH_RETENTION_CONTROL_MAX + 1,
                                           NULL, &w, NULL) == LC_ERR_PROTOCOL);
  REQUIRE(lc_pouch_retention_record_decode(&allocator, NULL, 7, NULL, &w,
                                           NULL) == LC_ERR_PROTOCOL);
  REQUIRE(lc_pouch_retention_record_decode(&allocator, nul, 7, NULL, NULL,
                                           NULL) == LC_ERR_INVALID);
  for (int phase = 0; phase < 3; ++phase) {
    ok(lc_pouch_retention_record_encode(&allocator, phase, retention_xid,
                                        "order/\n%", UINT64_MAX, UINT64_MAX,
                                        &text, &length, &e),
       &e);
    if (!phase)
      REQUIRE(length == 7 && !strcmp(text, "LPR1\n0\n"));
    else {
      char expected[160];
      REQUIRE(snprintf(expected, sizeof(expected),
                       "LPR1\n%d\n%s\n18446744073709551615\n"
                       "18446744073709551615\norder%%2f%%0a%%25\n",
                       phase, retention_xid) == (int)length);
      REQUIRE(!strcmp(text, expected));
    }
    ok(lc_pouch_retention_record_decode(&allocator, text, length, NULL, &w, &e),
       &e);
    REQUIRE(w.present && w.phase == phase &&
            (!phase ||
             (!strcmp(w.key, "order/\n%") && w.fencing_token == UINT64_MAX &&
              w.state_version == UINT64_MAX)));
    lc_free_with_allocator(&allocator, w.txn_id);
    lc_free_with_allocator(&allocator, w.key);
    lc_free_with_allocator(&allocator, text);
    REQUIRE(!live);
  }
  for (int no_error = 0; no_error < 2; ++no_error)
    for (size_t cut = 1; cut <= 2; ++cut) {
      char copy[] = "LPR1\n1\n00000000000000000000\n1\n2\nkey\n";
      allocation = 0;
      fail_at = cut;
      armed = 1;
      int rc = lc_pouch_retention_record_decode(
          &allocator, copy, sizeof(copy) - 1, NULL, &w, no_error ? NULL : &e);
      armed = 0;
      REQUIRE(rc == LC_ERR_NOMEM && !w.present && !w.txn_id && !w.key && !live);
      clear(&e);
      allocation = 0;
      armed = 1;
      rc = lc_pouch_retention_record_encode(&allocator, 1, retention_xid, "key",
                                            1, 2, &text, &length,
                                            no_error ? NULL : &e);
      armed = 0;
      REQUIRE(rc == LC_ERR_NOMEM && !text && !length && !live);
      clear(&e);
    }
  for (int phase = -1; phase < 5; ++phase) {
    if (phase >= 0 && phase <= 2)
      continue;
    REQUIRE(lc_pouch_retention_record_encode(&allocator, phase, retention_xid,
                                             "key", 0, 0, &text, &length,
                                             NULL) == LC_ERR_INVALID);
  }
  REQUIRE(lc_pouch_retention_record_encode(&allocator, 1, "bad", "key", 0, 0,
                                           &text, &length,
                                           NULL) == LC_ERR_INVALID);
  REQUIRE(lc_pouch_retention_record_encode(&allocator, 1, retention_xid, "", 0,
                                           0, &text, &length,
                                           NULL) == LC_ERR_INVALID);
  REQUIRE(lc_pouch_retention_record_encode(&allocator, 0, NULL, NULL, 0, 0,
                                           NULL, &length,
                                           NULL) == LC_ERR_INVALID);
  size_t key_length = (size_t)PS_MIB + 56;
  char *key = malloc(key_length + 2);
  REQUIRE(key);
  memset(key, '%', (size_t)PS_MIB);
  memset(key + PS_MIB, 'k', 57);
  key[key_length] = '\0';
  ok(lc_pouch_retention_record_encode(&allocator, 1, retention_xid, key,
                                      UINT64_MAX, UINT64_MAX, &text, &length,
                                      &e),
     &e);
  REQUIRE(length == LC_POUCH_RETENTION_CONTROL_MAX - 1);
  ok(lc_pouch_retention_record_decode(&allocator, text, length, NULL, &w, &e),
     &e);
  REQUIRE(!strcmp(w.key, key));
  lc_free_with_allocator(&allocator, w.key);
  lc_free_with_allocator(&allocator, w.txn_id);
  lc_free_with_allocator(&allocator, text);
  key[key_length] = 'k';
  key[key_length + 1] = '\0';
  REQUIRE(lc_pouch_retention_record_encode(&allocator, 1, retention_xid, key,
                                           UINT64_MAX, UINT64_MAX, &text,
                                           &length, &e) == LC_ERR_INVALID);
  REQUIRE(!text && !length && !live);
  free(key);
  lc_error_cleanup(&e);
}
static void retention_lifecycle(const char *directory) {
  const char *names[] = {"orders/eu", "orders/us", ".", "%", "space\nname"};
  lc_error e;
  lc_error_init(&e);
  char *long_key = malloc((size_t)PS_MIB + 1);
  REQUIRE(long_key);
  memset(long_key, '%', (size_t)PS_MIB);
  long_key[PS_MIB] = '\0';
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400], *name = NULL, *cursor = NULL;
      REQUIRE(snprintf(path, sizeof(path), "%s/retention-%d-%d", directory,
                       native, sync) < (int)sizeof(path));
      lc_pouch *p = visit_open(path, native, sync, 1, NULL, &allocator, &e);
      retention_expect(p, "missing", 0, 0, NULL, 0, 0, &e);
      REQUIRE(lc_pouch_retention_work_write(p, "missing", 1, retention_xid,
                                            "key", 1, 2,
                                            &e) == LC_ERR_TRANSPORT);
      clear(&e);
      ok(lc_pouch_retention_hint_remove(p, "missing", &e), &e);
      namespace_expect(p, 0, &e);
      for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i) {
        ok(lc_pouch_retention_hint_ensure(p, names[i], &e), &e);
        retention_expect(p, names[i], 1, 0, NULL, 0, 0, &e);
        retention_store(p, names[i], 1, "key/\n%", &e);
        ok(lc_pouch_retention_hint_ensure(p, names[i], &e), &e);
        retention_expect(p, names[i], 1, 1, "key/\n%", UINT64_MAX, UINT64_MAX,
                         &e);
      }
      unsigned int seen = 0;
      for (size_t i = 0; i < sizeof(names) / sizeof(*names) + 1; ++i) {
        char *next = NULL;
        ok(lc_pouch_retention_hint_next(p, cursor, &name, &next, &e), &e);
        REQUIRE(name && next && name != next);
        size_t index = 0;
        while (index < sizeof(names) / sizeof(*names) &&
               strcmp(name, names[index]))
          ++index;
        REQUIRE(index < sizeof(names) / sizeof(*names));
        if (i < sizeof(names) / sizeof(*names)) {
          REQUIRE(!(seen & (1U << index)));
          seen |= 1U << index;
        } else
          REQUIRE(seen == 31);
        lc_free_with_allocator(&allocator, name);
        lc_free_with_allocator(&allocator, cursor);
        cursor = next;
      }
      lc_free_with_allocator(&allocator, cursor);
      if (!native) {
        ps_namespace *ns = NULL;
        ok(ps_root_namespace_acquire(ps_pouch_state_root(p), names[0], 0, &ns,
                                     &e),
           &e);
        ps_namespace_info identity;
        ps_writer *corrupt = NULL;
        ok(ps_namespace_identity(ns, &identity, &e), &e);
        ps_key bad_key = {identity.owner, 3, PS_CONTROL};
        ok(ps_begin(ps_root_space(ps_pouch_state_root(p)), NULL, bad_key,
                    &corrupt, &e),
           &e);
        ok(ps_write(corrupt, "bad", 3, &e), &e);
        ok(ps_publish(corrupt, NULL, &e), &e);
        ps_writer_close(corrupt);
        lc_pouch_retention_work bad = {0};
        REQUIRE(lc_pouch_retention_work_read(p, names[0], &bad, &e) ==
                LC_ERR_PROTOCOL);
        REQUIRE(!bad.present && !bad.txn_id && !bad.key);
        clear(&e);
        /* Discovery must remain independent of malformed work bytes. */
        char *found = NULL, *advisory = NULL;
        ok(lc_pouch_retention_hint_next(p, NULL, &found, &advisory, &e), &e);
        REQUIRE(found && advisory);
        lc_free_with_allocator(&allocator, found);
        lc_free_with_allocator(&allocator, advisory);
        ps_namespace_release(ns);
        lc_pouch_close(p);
        REQUIRE(!live);
        p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
        REQUIRE(lc_pouch_retention_work_read(p, names[0], &bad, &e) ==
                LC_ERR_PROTOCOL);
        REQUIRE(!bad.present && !bad.txn_id && !bad.key);
        clear(&e);
        ok(ps_root_namespace_acquire(ps_pouch_state_root(p), names[0], 0, &ns,
                                     &e),
           &e);
        lc_pouch_retention_work idle = {0};
        ok(ps_namespace_retention_work_store(ns, &idle, &e), &e);
        ps_namespace_release(ns);
      }
      retention_store(p, names[0], 2, long_key, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
      retention_expect(p, names[0], 1, 2, long_key, UINT64_MAX, UINT64_MAX, &e);
      for (int i = 0; i < 5; ++i) {
        retention_store(p, names[0], 1 + i % 2, long_key, &e);
        retention_expect(p, names[0], 1, 1 + i % 2, long_key, UINT64_MAX,
                         UINT64_MAX, &e);
      }
      for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i) {
        retention_store(p, names[i], 0, NULL, &e);
        retention_expect(p, names[i], 1, 0, NULL, 0, 0, &e);
        ok(lc_pouch_retention_hint_remove(p, names[i], &e), &e);
        ok(lc_pouch_retention_hint_remove(p, names[i], &e), &e);
        retention_expect(p, names[i], 0, 0, NULL, 0, 0, &e);
      }
      name = cursor = NULL;
      ok(lc_pouch_retention_hint_next(p, NULL, &name, &cursor, &e), &e);
      REQUIRE(!name && !cursor);
      /* Removing a cached marker must not make a later ensure disappear. */
      ok(lc_pouch_retention_hint_ensure(p, names[0], &e), &e);
      retention_store(p, names[0], 1, "again", &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
      retention_expect(p, names[0], 1, 1, "again", UINT64_MAX, UINT64_MAX, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  free(long_key);
  namespace_artifacts(directory, "retention-");
  lc_error_cleanup(&e);
}
typedef struct retention_result {
  lc_pouch_retention_work work;
  char *name, *cursor;
} retention_result;
static int retention_action(lc_pouch *p, int action, retention_result *r,
                            lc_error *e) {
  memset(r, 0, sizeof(*r));
  switch (action) {
  case 0:
    return lc_pouch_retention_hint_ensure(p, "work/a", e);
  case 1:
    return lc_pouch_retention_work_read(p, "work/a", &r->work, e);
  case 2:
    return lc_pouch_retention_work_write(p, "work/a", 2, retention_xid,
                                         "new/\n%", 3, 4, e);
  case 3:
    return lc_pouch_retention_work_write(p, "work/a", 0, NULL, NULL, 0, 0, e);
  case 4:
    return lc_pouch_retention_hint_remove(p, "work/a", e);
  case 7:
    return lc_pouch_retention_work_write(p, "work/a", 2, retention_xid, "old",
                                         1, 2, e);
  case 8:
    return lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, "old",
                                         3, 4, e);
  default:
    return lc_pouch_retention_hint_next(p, NULL, &r->name, &r->cursor, e);
  }
}
static void retention_result_cleanup(lc_pouch *p, retention_result *r) {
  lc_pouch_retention_work_cleanup(p, &r->work);
  lc_free_with_allocator(&p->allocator, r->name);
  lc_free_with_allocator(&p->allocator, r->cursor);
}
static void retention_result_check(int action, int rc, retention_result *r) {
  if (rc != LC_OK)
    REQUIRE(!r->work.present && !r->work.txn_id && !r->work.key &&
            !r->work.fencing_token && !r->work.state_version && !r->name &&
            !r->cursor);
  else if (action == 1)
    REQUIRE(r->work.present && r->work.phase == 1 &&
            !strcmp(r->work.key, "old") &&
            !strcmp(r->work.txn_id, retention_xid) &&
            r->work.fencing_token == 1 && r->work.state_version == 2);
  else if (action == 5)
    REQUIRE(r->name && r->cursor &&
            (!strcmp(r->name, "work/a") || !strcmp(r->name, "work/b")));
}
static void retention_action_expect(lc_pouch *p, int action, lc_error *e) {
  int phase = action == 2 || action == 7                  ? 2
              : action == 1 || action == 5 || action == 8 ? 1
                                                          : 0;
  uint64_t fencing = action == 2 || action == 8 ? 3 : phase ? 1 : 0;
  uint64_t version = action == 2 || action == 8 ? 4 : phase ? 2 : 0;
  retention_expect(p, "work/a", action != 4, phase,
                   action == 2 ? "new/\n%" : "old", fencing, version, e);
}
static void retention_allocation_faults(const char *directory) {
  char prefix[1400], pattern[1500], trial[1500], path[1600];
  size_t total = 0;
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/retention-alloc-", directory) <
          (int)sizeof(prefix));
  fixture_barriers_model(1);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int cold = 0; cold < 2; ++cold)
        for (int action = 0; action < 9; ++action)
          for (int no_error = 0; no_error < 2; ++no_error) {
            if (action == 6)
              continue; /* larger replacement has its separate capacity sweep */
            size_t sites = 0;
            for (size_t cut = 0; cut <= sites; ++cut) {
              retention_result r;
              REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) <
                      (int)sizeof(pattern));
              REQUIRE(
                  lc_test_tmp_mkdtemp(pattern, trial, sizeof(trial), prefix));
              REQUIRE(snprintf(path, sizeof(path), "%s/store", trial) <
                      (int)sizeof(path));
              lc_pouch *p =
                  visit_open(path, native, sync, 1, NULL, &allocator, &e);
              ok(lc_pouch_ensure_namespace(p, "work/a", &e), &e);
              if (action) {
                ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
                ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid,
                                                 "old", action == 8 ? 0 : 1,
                                                 action == 8 ? 0 : 2, &e),
                   &e);
              }
              if (action == 5)
                ok(lc_pouch_retention_hint_ensure(p, "work/b", &e), &e);
              if (cold) {
                lc_pouch_close(p);
                REQUIRE(!live);
                p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
              }
              allocation = 0;
              fail_at = cut ? cut : SIZE_MAX;
              armed = 1;
              int rc = retention_action(p, action, &r, no_error ? NULL : &e);
              armed = 0;
              if (!cut) {
                sites = allocation;
                REQUIRE(rc == LC_OK && sites);
              } else {
                REQUIRE(allocation >= cut &&
                        (rc == LC_ERR_NOMEM || rc == LC_OK));
                ++total;
              }
              retention_result_check(action, rc, &r);
              retention_result_cleanup(p, &r);
              clear(&e);
              if (no_error && cut) {
                lc_pouch_close(p);
                REQUIRE(!live);
                p = visit_open(path, native, sync, 0, NULL, &allocator, &e);
              }
              ok(retention_action(p, action, &r, &e), &e);
              retention_result_check(action, LC_OK, &r);
              retention_result_cleanup(p, &r);
              retention_action_expect(p, action, &e);
              lc_pouch_close(p);
              REQUIRE(!live);
              if (!native)
                one_file(path);
              retention_only_artifact(trial, "store");
              lc_test_tmp_cleanup_path(trial, prefix);
            }
          }
  fixture_barriers_model(0);
  printf("retention controls: %lu warm/cold allocation cuts, both "
         "owners/policies, "
         "same-owner and close/reopen retry\n",
         (unsigned long)total);
  lc_error_cleanup(&e);
}
typedef struct retention_disk {
  io_fault fault;
  unsigned char *stable, *dirty, *scratch;
  uint64_t read_bytes;
  int persist, frozen, complete_writes;
} retention_disk;
static ssize_t retention_disk_read(void *context, int fd, void *p, size_t n,
                                   off_t at) {
  retention_disk *m = context;
  if (io_fails(&m->fault))
    return -1;
  ssize_t got = pread(fd, p, n, at);
  if (got > 0)
    m->read_bytes += (uint64_t)got;
  return got;
}
static ssize_t retention_disk_write(void *context, int fd, const void *p,
                                    size_t n, off_t at) {
  retention_disk *m = context;
  REQUIRE(at >= 0 && (uint64_t)at + n <= PS_DEFAULT_CAPACITY);
  int fail = io_fails(&m->fault);
  size_t want = fail && !m->complete_writes ? n / 2 : n;
  REQUIRE(pwrite(fd, p, want, at) == (ssize_t)want);
  if (want) {
    for (size_t page = (size_t)at / PS_PAGE;
         page <= ((size_t)at + want - 1) / PS_PAGE; ++page)
      m->dirty[page] = 1;
    if (fail && m->persist && !m->frozen)
      memcpy(m->stable + at, p, want);
  }
  if (fail) {
    errno = EIO;
    return -1;
  }
  return (ssize_t)want;
}
static int retention_disk_sync(void *context, int fd) {
  retention_disk *m = context;
  int fail = io_fails(&m->fault);
  if (!fail || m->persist) {
    /* The stable image models which writes a completed barrier preserves. */
    for (size_t page = 0; !m->frozen && page < PS_DEFAULT_CAPACITY / PS_PAGE;
         ++page)
      if (m->dirty[page]) {
        REQUIRE(pread(fd, m->scratch, PS_PAGE, (off_t)(page * PS_PAGE)) ==
                PS_PAGE);
        memcpy(m->stable + page * PS_PAGE, m->scratch, PS_PAGE);
        m->dirty[page] = 0;
      }
  }
  if (fail) {
    errno = EIO;
    return -1;
  }
  return 0;
}
static int retention_disk_reserve(void *context, int fd, off_t at, off_t n) {
  retention_disk *m = context;
  return io_fails(&m->fault) ? -1 : ps_native_io.reserve(NULL, fd, at, n);
}
static void retention_disk_restore(retention_disk *m, const char *path) {
  int fd = open(path, O_RDWR);
  REQUIRE(fd >= 0);
  for (size_t page = 0; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
    if (m->dirty[page]) {
      size_t at = page * PS_PAGE;
      REQUIRE(pwrite(fd, m->stable + at, PS_PAGE, (off_t)at) == PS_PAGE);
    }
  REQUIRE(!close(fd));
  memset(m->dirty, 0, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
}
/* Leave a peer claim holding the largest currently admissible payload.
 * Keep an intended source of later reclamation pinned during this search:
 * budget admission can itself checkpoint. */
static ps_budget *retention_peer(ps_space *space, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  uint64_t low = 0, high = PS_DEFAULT_CAPACITY / PS_PAGE;
  plan.maintenance = 1;
  while (low + 1 < high) {
    uint64_t mid = (low + high) / 2;
    plan.payload_bytes = mid * PS_PAGE;
    int rc = ps_budget_open(space, &plan, &budget, e);
    if (rc == LC_OK) {
      low = mid;
      ok(ps_budget_close(budget, e), e);
    } else {
      REQUIRE(rc == LC_ERR_INVALID);
      clear(e);
      high = mid;
    }
  }
  plan.payload_bytes = low * PS_PAGE;
  ok(ps_budget_open(space, &plan, &budget, e), e);
  return budget;
}
static ps_reader *retention_pin_identity(lc_pouch *p, lc_error *e) {
  ps_root *root = ps_pouch_state_root(p);
  ps_namespace *ns = NULL;
  ps_namespace_info identity;
  ps_reader *reader = NULL;
  ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, e), e);
  ok(ps_namespace_identity(ns, &identity, e), e);
  ps_key key = {identity.owner, 3, PS_CONTROL};
  ok(ps_read_open(ps_root_space(root), key, NULL, &reader, e), e);
  ps_namespace_release(ns);
  return reader;
}
static void retention_recovered(lc_pouch *p, int action, int completed,
                                const char *large, retention_disk *m,
                                lc_error *e) {
  lc_pouch_retention_work w = {0};
  char *name = NULL, *cursor = NULL;
  ok(lc_pouch_retention_work_read(p, "work/a", &w, e), e);
  if (completed) {
    int phase = action == 0 || action == 3 || action == 4   ? 0
                : action == 2 || action == 6 || action == 7 ? 2
                                                            : 1;
    REQUIRE(w.present == (action != 4) && w.phase == phase);
  }
  if (w.present && w.phase) {
    REQUIRE(!strcmp(w.txn_id, retention_xid));
    if (w.phase == 1 && action == 8)
      REQUIRE(!strcmp(w.key, "old") &&
              ((!completed && !w.fencing_token && !w.state_version) ||
               (w.fencing_token == 3 && w.state_version == 4)));
    else if (w.phase == 1)
      REQUIRE(action != 3 && action != 4 && strcmp(w.key, "old") == 0 &&
              w.fencing_token == 1 && w.state_version == 2 &&
              (!completed || (action != 2 && action != 6)));
    else
      REQUIRE(w.phase == 2 &&
              (action == 2 || action == 6 || action == 7 ||
               (!completed && (action == 3 || action == 4))) &&
              !strcmp(w.key, action == 6 ? large
                             : action == 7 || action == 3 || action == 4
                                 ? "old"
                                 : "new/\n%") &&
              w.fencing_token ==
                  (action == 7 || action == 3 || action == 4 ? 1 : 3) &&
              w.state_version ==
                  (action == 7 || action == 3 || action == 4 ? 2 : 4));
  } else {
    REQUIRE(action == 0 || action == 3 || action == 4);
    if (action == 3)
      REQUIRE(w.present);
  }
  int present = w.present;
  lc_pouch_retention_work_cleanup(p, &w);
  m->read_bytes = 0;
  ok(lc_pouch_retention_hint_next(p, NULL, &name, &cursor, e), e);
  REQUIRE((name != NULL) == present && (cursor != NULL) == present &&
          (!present || !strcmp(name, "work/a")) && m->read_bytes == 0);
  lc_free_with_allocator(&p->allocator, name);
  lc_free_with_allocator(&p->allocator, cursor);
}
static void retention_io_faults(const char *directory) {
  lc_error e;
  size_t total = 0;
  char path[1400];
  char *large = malloc(8193), *retired = malloc(32769);
  REQUIRE(large && retired);
  memset(large, '%', 8192);
  large[8192] = '\0';
  memset(retired, '%', 32768);
  retired[32768] = '\0';
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/retention-io", directory) <
          (int)sizeof(path));
  fixture_directory_sync_model(1);
  for (int sync = 0; sync < 2; ++sync)
    for (int persist = 0; persist < 2; ++persist)
      for (int action = 0; action < 9; ++action) {
        size_t sites = 0;
        for (size_t cut = 0; cut <= sites; ++cut) {
          retention_disk m = {0};
          m.persist = persist;
          m.stable = calloc(1, (size_t)PS_DEFAULT_CAPACITY);
          m.dirty = calloc(1, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
          m.scratch = malloc(PS_PAGE);
          REQUIRE(m.stable && m.dirty && m.scratch);
          ps_io io = {&m,
                      retention_disk_read,
                      retention_disk_write,
                      retention_disk_sync,
                      retention_disk_reserve,
                      NULL};
          lc_pouch *p = visit_open(path, 0, sync, 1, &io, &allocator, &e);
          ps_budget *peer = NULL;
          ps_reader *reclaim = NULL;
          ok(lc_pouch_ensure_namespace(p, "work/a", &e), &e);
          if (action) {
            ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
            if (action == 6) {
              ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid,
                                               retired, 1, 2, &e),
                 &e);
              reclaim = retention_pin_identity(p, &e);
            }
            ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid,
                                             "old", action == 8 ? 0 : 1,
                                             action == 8 ? 0 : 2, &e),
               &e);
          }
          ps_space *space = ps_root_space(ps_pouch_state_root(p));
          if (action == 3 || action == 4)
            ok(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid,
                                             "old", 1, 2, &e),
               &e);
          if (action == 6) {
            peer = retention_peer(space, &e);
            ps_reader_close(reclaim);
          }
          ps_stats before, after;
          ok(ps_status(space, &before, &e), &e);
          if (action == 6)
            REQUIRE(before.journal_bytes && before.retired_bytes >= PS_CHUNK);
          m.fault.calls = 0;
          m.fault.fail = cut;
          m.fault.armed = 1;
          retention_result r = {0};
          int rc = action == 6 ? lc_pouch_retention_work_write(p, "work/a", 2,
                                                               retention_xid,
                                                               large, 3, 4, &e)
                               : retention_action(p, action, &r, &e);
          m.fault.armed = 0;
          if (!cut) {
            ok(rc, &e);
            sites = m.fault.calls;
            /* Discovery uses resident catalog metadata without reading the
             * hint value. The consuming work operations still perform I/O. */
            REQUIRE(action == 5 ? sites == 0U : sites > 0U);
            if (action == 6) {
              ok(ps_status(space, &after, &e), &e);
              REQUIRE(after.checkpoints > before.checkpoints &&
                      after.capacity == PS_DEFAULT_CAPACITY);
            }
          } else {
            REQUIRE(rc == LC_ERR_TRANSPORT);
            ++total;
          }
          retention_result_check(action, rc, &r);
          retention_result_cleanup(p, &r);
          clear(&e);
          if (ps_check(space, NULL) == LC_ERR_TRANSPORT) {
            m.fault.calls = 0;
            m.fault.fail = SIZE_MAX;
            m.fault.armed = 1;
            for (int blocked = 0; blocked < 6; ++blocked) {
              REQUIRE(retention_action(p, blocked, &r, &e) == LC_ERR_TRANSPORT);
              retention_result_check(blocked, LC_ERR_TRANSPORT, &r);
              retention_result_cleanup(p, &r);
              clear(&e);
            }
            m.fault.armed = 0;
            REQUIRE(!m.fault.calls);
          }
          ok(ps_budget_close(peer, &e), &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          retention_disk_restore(&m, path);
          p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
          retention_recovered(p, action, rc == LC_OK, large, &m, &e);
          ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
          retention_store(p, "work/a", 1, "retry", &e);
          retention_expect(p, "work/a", 1, 1, "retry", UINT64_MAX, UINT64_MAX,
                           &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          one_file(path);
          retention_only_artifact(directory, "retention-io");
          REQUIRE(!unlink(path));
          free(m.stable);
          free(m.dirty);
          free(m.scratch);
        }
        printf("retention I/O sync%d persist%d action%d: %lu cuts\n", sync,
               persist, action, (unsigned long)sites);
      }
  fixture_directory_sync_model(0);
  printf("retention: %lu lost/torn write/barrier/read cuts\n",
         (unsigned long)total);
  free(large);
  free(retired);
  lc_error_cleanup(&e);
}
static void retention_capacity(const char *directory) {
  lc_error e;
  char path[1400];
  char *old_key = malloc(32769), *next_key = malloc(8193);
  REQUIRE(old_key && next_key);
  memset(old_key, '%', 32768);
  old_key[32768] = '\0';
  memset(next_key, '%', 8192);
  next_key[8192] = '\0';
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    REQUIRE(snprintf(path, sizeof(path), "%s/retention-capacity-%d", directory,
                     sync) < (int)sizeof(path));
    lc_pouch *p = visit_open(path, 0, sync, 1, NULL, &allocator, &e);
    ps_root *root = ps_pouch_state_root(p);
    ps_namespace *ns = NULL;
    ps_namespace_info identity;
    ps_reader *pin = NULL;
    ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
    ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, old_key, 1,
                                     2, &e),
       &e);
    ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, &e), &e);
    ok(ps_namespace_identity(ns, &identity, &e), &e);
    ps_key key = {identity.owner, 3, PS_CONTROL};
    ok(ps_read_open(ps_root_space(root), key, NULL, &pin, &e), &e);
    ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, "old", 1, 2,
                                     &e),
       &e);
    ps_budget *peer = retention_peer(ps_root_space(root), &e);
    REQUIRE(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid,
                                          next_key, 3, 4,
                                          &e) == LC_ERR_INVALID);
    clear(&e);
    retention_expect(p, "work/a", 1, 1, "old", 1, 2, &e);
    size_t length = 32768, at = 0, got;
    unsigned char header[80];
    unsigned char buffer[317];
    REQUIRE(ps_read_size(pin) == sizeof(header) + length);
    ok(ps_read(pin, header, sizeof(header), &got, &e), &e);
    REQUIRE(got == sizeof(header) && !memcmp(header, "LCPSRTWK", 8));
    do {
      ok(ps_read(pin, buffer, sizeof(buffer), &got, &e), &e);
      REQUIRE(got <= length - at && !memcmp(buffer, old_key + at, got));
      at += got;
    } while (got);
    REQUIRE(at == length);
    ps_reader_close(pin);
    /* The peer still owns all of its claim. Only releasing this historical
     * reader makes the superseded control's payload available. */
    ok(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid, next_key, 3,
                                     4, &e),
       &e);
    ok(ps_budget_close(peer, &e), &e);
    retention_expect(p, "work/a", 1, 2, next_key, 3, 4, &e);
    ps_stats stats;
    ok(ps_status(ps_root_space(root), &stats, &e), &e);
    REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY && !stats.budgets &&
            !stats.reserved_payload_bytes && !stats.reserved_metadata_bytes &&
            !stats.reserved_entries);
    ps_namespace_release(ns);
    lc_pouch_close(p);
    REQUIRE(!live);
    p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
    retention_expect(p, "work/a", 1, 2, next_key, 3, 4, &e);
    lc_pouch_close(p);
    REQUIRE(!live);
    one_file(path);
  }
  free(old_key);
  free(next_key);
  lc_error_cleanup(&e);
}
static void retention_capacity_faults(const char *directory) {
  char path[1400];
  char *old_key = malloc(32769), *next_key = malloc(8193);
  size_t total = 0;
  lc_error e;
  REQUIRE(old_key && next_key);
  memset(old_key, '%', 32768);
  old_key[32768] = '\0';
  memset(next_key, '%', 8192);
  next_key[8192] = '\0';
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/retention-capacity-fault",
                   directory) < (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync)
    for (int no_error = 0; no_error < 2; ++no_error) {
      size_t sites = 0;
      for (size_t cut = 0; cut <= sites; ++cut) {
        lc_pouch *p = visit_open(path, 0, sync, 1, NULL, &allocator, &e);
        ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
        ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, old_key,
                                         1, 2, &e),
           &e);
        ps_reader *reclaim = retention_pin_identity(p, &e);
        ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, "old",
                                         1, 2, &e),
           &e);
        ps_budget *peer =
            retention_peer(ps_root_space(ps_pouch_state_root(p)), &e);
        ps_reader_close(reclaim);
        allocation = 0;
        fail_at = cut ? cut : SIZE_MAX;
        armed = 1;
        int rc =
            lc_pouch_retention_work_write(p, "work/a", 2, retention_xid,
                                          next_key, 3, 4, no_error ? NULL : &e);
        armed = 0;
        if (!cut) {
          sites = allocation;
          REQUIRE(rc == LC_OK && sites > 10);
        } else {
          if (allocation < cut ||
              (rc != LC_ERR_NOMEM && rc != LC_OK && rc != LC_ERR_INVALID))
            fprintf(
                stderr,
                "capacity sync%d null%d cut%lu/%lu allocations%lu rc%d: %s\n",
                sync, no_error, (unsigned long)cut, (unsigned long)sites,
                (unsigned long)allocation, rc, e.message ? e.message : "?");
          /* A completed checkpoint may defer collect if allocating a free-run
           * node fails. Its following admission then correctly remains full;
           * the selected journal must stay intact and retry must reclaim it. */
          REQUIRE(allocation >= cut &&
                  (rc == LC_ERR_NOMEM || rc == LC_OK || rc == LC_ERR_INVALID));
          if (rc == LC_ERR_INVALID && !no_error)
            REQUIRE(e.message && strstr(e.message, "capacity"));
          ++total;
        }
        clear(&e);
        retention_expect(p, "work/a", 1, rc == LC_OK ? 2 : 1,
                         rc == LC_OK ? next_key : "old", rc == LC_OK ? 3 : 1,
                         rc == LC_OK ? 4 : 2, &e);
        if (no_error) {
          ok(ps_budget_close(peer, &e), &e);
          peer = NULL;
          lc_pouch_close(p);
          REQUIRE(!live);
          p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
          retention_expect(p, "work/a", 1, rc == LC_OK ? 2 : 1,
                           rc == LC_OK ? next_key : "old", rc == LC_OK ? 3 : 1,
                           rc == LC_OK ? 4 : 2, &e);
        }
        ok(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid,
                                         next_key, 3, 4, &e),
           &e);
        ok(ps_budget_close(peer, &e), &e);
        retention_expect(p, "work/a", 1, 2, next_key, 3, 4, &e);
        lc_pouch_close(p);
        REQUIRE(!live);
        one_file(path);
        retention_only_artifact(directory, "retention-capacity-fault");
        REQUIRE(!unlink(path));
      }
    }
  printf("retention capacity retry: %lu allocation cuts\n",
         (unsigned long)total);
  free(old_key);
  free(next_key);
  lc_error_cleanup(&e);
}
typedef struct retention_gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int enabled, payload, entered, released, captured_read;
  uint64_t written_bytes;
  uint64_t sync_calls;
} retention_gate;
static void retention_gate_wait(retention_gate *g) {
  g->entered = 1;
  REQUIRE(!pthread_cond_broadcast(&g->changed));
  while (!g->released)
    REQUIRE(!pthread_cond_wait(&g->changed, &g->mutex));
}
static ssize_t retention_gate_read(void *context, int fd, void *p, size_t n,
                                   off_t at) {
  retention_gate *g = context;
  ssize_t got = pread(fd, p, n, at);
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && g->captured_read && !g->entered && got >= 8 &&
      !memcmp(p, "LCPSRTWK", 8))
    retention_gate_wait(g);
  pthread_mutex_unlock(&g->mutex);
  return got;
}
static ssize_t retention_gate_write(void *context, int fd, const void *p,
                                    size_t n, off_t at) {
  retention_gate *g = context;
  pthread_mutex_lock(&g->mutex);
  if (g->enabled && g->payload && !g->entered && at >= PS_DATA)
    retention_gate_wait(g);
  g->written_bytes += n;
  pthread_mutex_unlock(&g->mutex);
  return pwrite(fd, p, n, at);
}
static int retention_gate_sync(void *context, int fd) {
  retention_gate *g = context;
  pthread_mutex_lock(&g->mutex);
  ++g->sync_calls;
  if (g->enabled && !g->payload && !g->entered)
    retention_gate_wait(g);
  pthread_mutex_unlock(&g->mutex);
  return ps_native_io.sync(NULL, fd);
}
static int retention_gate_reserve(void *context, int fd, off_t at, off_t n) {
  (void)context;
  return ps_native_io.reserve(NULL, fd, at, n);
}
static void retention_bootstrap_cuts(const char *path, int sync,
                                     const ps_io *io, lc_error *e) {
  ps_pouch_state_options o = {0};
  size_t sites = 0;
  o.logical.single_writer = o.logical.single_writer_set = 1;
  o.logical.background_compaction_enabled_set = 1;
  o.logical.query_indexing_enabled_set = 1;
  o.logical.query_engine = "scan";
  o.logical.durable_sync = sync;
  o.io = io;
  for (size_t cut = 0; cut <= sites; ++cut) {
    lc_pouch *p = NULL;
    allocation = 0;
    fail_at = cut ? cut : SIZE_MAX;
    armed = 1;
    int rc = ps_pouch_state_open(path, 0, &allocator, &o, &p, e);
    armed = 0;
    if (!cut) {
      sites = allocation;
      REQUIRE(sites && rc == LC_OK);
    } else
      REQUIRE(rc == LC_ERR_NOMEM || rc == LC_OK);
    if (rc == LC_OK) {
      ps_stats stats;
      ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &stats, e), e);
      REQUIRE(stats.budgets == 1 &&
              stats.reserved_payload_bytes == 2 * PS_PAGE);
    } else
      REQUIRE(!p);
    lc_pouch_close(p);
    clear(e);
    REQUIRE(!live);
  }
  printf("retention completion bootstrap sync%d: %zu allocation cuts\n", sync,
         sites);
}
static uint64_t retention_fill_ordinary(ps_space *space, uint64_t start,
                                        lc_error *e) {
  ps_key key = {UINT64_MAX - 2, start, PS_STATE};
  unsigned char *bytes = calloc(1, PS_CHUNK);
  REQUIRE(bytes);
  for (size_t length = PS_CHUNK; length >= PS_PAGE; length /= 4) {
    for (; key.key < 4096; ++key.key) {
      ps_writer *writer = NULL;
      int rc = ps_begin(space, NULL, key, &writer, e);
      if (rc == LC_OK)
        rc = ps_write(writer, bytes, length, e);
      if (rc == LC_OK)
        rc = ps_publish(writer, NULL, e);
      ps_writer_close(writer);
      if (rc == LC_ERR_INVALID) {
        clear(e);
        break;
      }
      ok(rc, e);
    }
    REQUIRE(key.key < 4096);
  }
  free(bytes);
  return key.key;
}
static void retention_many_progress(const char *directory) {
  char path[1400], name[32], key[257];
  lc_error e;
  memset(key, '%', sizeof(key) - 1);
  key[sizeof(key) - 1] = 0;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    REQUIRE(snprintf(path, sizeof(path), "%s/retention-many-%d", directory,
                     sync) < (int)sizeof(path));
    lc_pouch *p = visit_open(path, 0, sync, 1, NULL, &allocator, &e);
    for (int owner = 0; owner < 24; ++owner) {
      snprintf(name, sizeof(name), "work/%d", owner);
      ok(lc_pouch_retention_hint_ensure(p, name, &e), &e);
      ok(lc_pouch_retention_work_write(p, name, 1, retention_xid, key, 0, 0,
                                       &e),
         &e);
    }
    uint64_t fill =
        retention_fill_ordinary(ps_root_space(ps_pouch_state_root(p)), 1, &e);
    lc_pouch_close(p);
    REQUIRE(!live);
    p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
    ps_space *space = ps_root_space(ps_pouch_state_root(p));
    ps_stats stats;
    ps_budget_plan peer_plan;
    ok(ps_status(space, &stats, &e), &e);
    REQUIRE(stats.budgets == 24 &&
            stats.reserved_payload_bytes == 48 * PS_PAGE);
    ps_budget *peer = retention_peer(space, &e);
    for (int owner = 0; owner < 24; ++owner) {
      snprintf(name, sizeof(name), "work/%d", owner);
      ok(lc_pouch_retention_work_write(p, name, 1, retention_xid, key, 1, 2,
                                       &e),
         &e);
    }
    ok(ps_status(space, &stats, &e), &e);
    ok(ps_budget_status(peer, &peer_plan, &e), &e);
    REQUIRE(stats.budgets == 25 && stats.reserved_payload_bytes ==
                                       peer_plan.payload_bytes + 24 * PS_PAGE);
    /* Producers may use released claim slack, but cannot spend any owner's
     * admitted terminal publication. */
    fill = retention_fill_ordinary(space, fill, &e);
    for (int owner = 0; owner < 24; ++owner) {
      snprintf(name, sizeof(name), "work/%d", owner);
      ok(lc_pouch_retention_work_write(p, name, 2, retention_xid, key, 1, 2,
                                       &e),
         &e);
    }
    ok(ps_status(space, &stats, &e), &e);
    REQUIRE(stats.budgets == 1);
    ok(ps_budget_close(peer, &e), &e);
    (void)retention_fill_ordinary(space, fill, &e);
    lc_pouch_close(p);
    REQUIRE(!live);
    p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
    ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &stats, &e), &e);
    REQUIRE(!stats.budgets && !stats.reserved_payload_bytes &&
            stats.capacity == PS_DEFAULT_CAPACITY);
    for (int owner = 0; owner < 24; ++owner) {
      snprintf(name, sizeof(name), "work/%d", owner);
      retention_expect(p, name, 1, 2, key, 1, 2, &e);
    }
    lc_pouch_close(p);
    REQUIRE(!live);
    one_file(path);
  }
  lc_error_cleanup(&e);
}
static void retention_progress_capacity(const char *directory) {
  char path[1400];
  char *key = malloc((size_t)PS_MIB + 1);
  lc_error e;
  REQUIRE(key);
  memset(key, '%', (size_t)PS_MIB);
  key[PS_MIB] = 0;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync)
    for (int cold = 0; cold < 2; ++cold) {
      retention_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
      ps_io io = {&g,
                  retention_gate_read,
                  retention_gate_write,
                  retention_gate_sync,
                  retention_gate_reserve,
                  NULL};
      REQUIRE(snprintf(path, sizeof(path), "%s/retention-progress-%d-%d",
                       directory, sync, cold) < (int)sizeof(path));
      lc_pouch *p = visit_open(path, 0, sync, 1, &io, &allocator, &e);
      ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
      ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key, 0, 0,
                                       &e),
         &e);
      if (cold) {
        lc_pouch_close(p);
        REQUIRE(!live);
        retention_bootstrap_cuts(path, sync, &io, &e);
        p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
      }
      ps_root *root = ps_pouch_state_root(p);
      ps_space *space = ps_root_space(root);
      ps_namespace *ns = NULL;
      ps_namespace_info owner;
      ps_reader *pin = NULL, *selected = NULL;
      ps_ref before_ref, after_ref;
      ps_stats before, after;
      ok(ps_root_namespace_acquire(root, "work/a", 0, &ns, &e), &e);
      ok(ps_namespace_identity(ns, &owner, &e), &e);
      ps_key identity = {owner.owner, 3, PS_CONTROL};
      ok(ps_read_open(space, identity, NULL, &pin, &e), &e);
      REQUIRE(ps_read_size(pin) == PS_MIB + 80);
      ps_read_reference(pin, &before_ref);
      ok(ps_status(space, &before, &e), &e);
      REQUIRE(before.budgets == 1 &&
              before.reserved_payload_bytes == 2 * PS_PAGE);
      ps_budget *peer = retention_peer(space, &e);
      g.written_bytes = 0;
      ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key, 1, 2,
                                       &e),
         &e);
      uint64_t claim_bytes = g.written_bytes;
      REQUIRE(claim_bytes && claim_bytes < PS_CHUNK);
      retention_expect(p, "work/a", 1, 1, key, 1, 2, &e);
      ps_budget *second_peer = retention_peer(space, &e);
      REQUIRE(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key,
                                            3, 4, &e) == LC_ERR_INVALID);
      clear(&e);
      retention_expect(p, "work/a", 1, 1, key, 1, 2, &e);
      g.written_bytes = 0;
      ok(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid, key, 1, 2,
                                       &e),
         &e);
      uint64_t finish_bytes = g.written_bytes;
      REQUIRE(finish_bytes && finish_bytes < PS_CHUNK);
      ok(ps_read_open(space, identity, NULL, &selected, &e), &e);
      ps_read_reference(selected, &after_ref);
      REQUIRE(before_ref.incarnation == after_ref.incarnation &&
              !memcmp(before_ref.container, after_ref.container, 16));
      ps_reader_close(selected);
      selected = NULL;
      retention_expect(p, "work/a", 1, 2, key, 1, 2, &e);
      ok(ps_budget_close(second_peer, &e), &e);
      unsigned char original[80], retained[4096];
      size_t got, at = 0;
      ok(ps_read(pin, original, sizeof(original), &got, &e), &e);
      REQUIRE(got == sizeof(original) && !memcmp(original, "LCPSRTWK", 8));
      for (size_t byte = 16; byte < 32; ++byte)
        REQUIRE(!original[byte]);
      while (at < PS_MIB) {
        size_t take = (size_t)PS_MIB - at;
        if (take > sizeof(retained))
          take = sizeof(retained);
        ok(ps_read(pin, retained, take, &got, &e), &e);
        REQUIRE(got == take && !memcmp(retained, key + at, got));
        at += got;
      }
      ok(ps_status(space, &after, &e), &e);
      REQUIRE(after.allocated_bytes <= before.allocated_bytes + 2 * PS_PAGE &&
              after.capacity == PS_DEFAULT_CAPACITY && after.budgets == 1);
      ok(ps_budget_close(peer, &e), &e);
      /* Reusing every logical fact still requires a new work incarnation.
       * The old selected progress cannot complete this new active work. */
      ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key, 1, 2,
                                       &e),
         &e);
      retention_expect(p, "work/a", 1, 1, key, 1, 2, &e);
      ok(ps_read_open(space, identity, NULL, &selected, &e), &e);
      ps_read_reference(selected, &after_ref);
      REQUIRE(after_ref.incarnation != before_ref.incarnation);
      ps_reader_close(selected);
      ps_reader_close(pin);
      ps_namespace_release(ns);
      lc_pouch_close(p);
      REQUIRE(!live);
      p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
      retention_expect(p, "work/a", 1, 1, key, 1, 2, &e);
      ok(lc_pouch_retention_work_write(p, "work/a", 2, retention_xid, key, 1, 2,
                                       &e),
         &e);
      g.sync_calls = 0;
      retention_store(p, "work/a", 0, NULL, &e);
      REQUIRE(g.sync_calls == 1);
      ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &after, &e), &e);
      REQUIRE(!after.budgets && !after.reserved_payload_bytes);
      lc_pouch_close(p);
      REQUIRE(!live);
      one_file(path);
      printf("retention progress sync%d cold%d key_bytes=%llu "
             "claim_written=%llu finish_written=%llu\n",
             sync, cold, (unsigned long long)PS_MIB,
             (unsigned long long)claim_bytes, (unsigned long long)finish_bytes);
      REQUIRE(!pthread_cond_destroy(&g.changed) &&
              !pthread_mutex_destroy(&g.mutex));
    }
  free(key);
  lc_error_cleanup(&e);
  retention_many_progress(directory);
}
typedef struct retention_actor {
  lc_pouch *p;
  const char *key;
  int remove, rc;
  int read;
  lc_pouch_retention_work work;
} retention_actor;
static void *retention_actor_run(void *context) {
  retention_actor *a = context;
  lc_error e;
  lc_error_init(&e);
  a->rc = a->read ? lc_pouch_retention_work_read(a->p, "work/a", &a->work, &e)
          : a->remove ? lc_pouch_retention_hint_remove(a->p, "work/a", &e)
                      : lc_pouch_retention_work_write(
                            a->p, "work/a", 2, retention_xid, a->key, 3, 4, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void retention_captured_completion(const char *directory) {
  lc_error e;
  char path[1400];
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync) {
    retention_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
    ps_io io = {&g,
                retention_gate_read,
                retention_gate_write,
                retention_gate_sync,
                retention_gate_reserve,
                NULL};
    REQUIRE(snprintf(path, sizeof(path), "%s/retention-captured-%d", directory,
                     sync) < (int)sizeof(path));
    lc_pouch *p = visit_open(path, 0, sync, 1, &io, &allocator, &e);
    ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
    retention_store(p, "work/a", 1, "old", &e);
    retention_store(p, "work/a", 2, "old", &e);
    retention_actor actor = {0};
    actor.p = p;
    actor.read = 1;
    pthread_t thread;
    g.enabled = g.captured_read = 1;
    REQUIRE(!pthread_create(&thread, NULL, retention_actor_run, &actor));
    struct timespec deadline;
    REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
    deadline.tv_sec += 10;
    pthread_mutex_lock(&g.mutex);
    int rc = 0;
    while (!g.entered && !rc)
      rc = pthread_cond_timedwait(&g.changed, &g.mutex, &deadline);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(!rc && g.entered);
    /* The captured A identity and A completion stay coherent even after both
     * selected controls advance to B. Payload reads hold no writer authority.
     */
    retention_store(p, "work/a", 1, "new", &e);
    retention_store(p, "work/a", 2, "new", &e);
    pthread_mutex_lock(&g.mutex);
    g.released = 1;
    pthread_cond_broadcast(&g.changed);
    pthread_mutex_unlock(&g.mutex);
    REQUIRE(!pthread_join(thread, NULL) && actor.rc == LC_OK);
    REQUIRE(actor.work.phase == 2 && !strcmp(actor.work.key, "old"));
    lc_pouch_retention_work_cleanup(p, &actor.work);
    retention_expect(p, "work/a", 1, 2, "new", UINT64_MAX, UINT64_MAX, &e);
    lc_pouch_close(p);
    REQUIRE(!live);
    one_file(path);
    REQUIRE(!pthread_cond_destroy(&g.changed) &&
            !pthread_mutex_destroy(&g.mutex));
  }
  lc_error_cleanup(&e);
}
static void retention_concurrency(const char *directory) {
  lc_error e;
  char path[1400];
  char *key = malloc(8193);
  REQUIRE(key);
  memset(key, '%', 8192);
  key[8192] = '\0';
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync)
    for (int phase = 0; phase < 3; ++phase) {
      REQUIRE(snprintf(path, sizeof(path), "%s/retention-concurrent-%d-%d",
                       directory, sync, phase) < (int)sizeof(path));
      retention_gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};
      ps_io io = {&g,
                  retention_gate_read,
                  retention_gate_write,
                  retention_gate_sync,
                  retention_gate_reserve,
                  NULL};
      lc_pouch *p = visit_open(path, 0, sync, 1, &io, &allocator, &e);
      ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
      ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, "old", 1,
                                       2, &e),
         &e);
      retention_actor a = {p, key, phase == 2, -1};
      pthread_t thread;
      g.enabled = 1;
      g.payload = phase == 0;
      REQUIRE(!pthread_create(&thread, NULL, retention_actor_run, &a));
      struct timespec deadline;
      REQUIRE(!clock_gettime(CLOCK_REALTIME, &deadline));
      deadline.tv_sec += 10;
      pthread_mutex_lock(&g.mutex);
      int rc = 0;
      while (!g.entered && !rc)
        rc = pthread_cond_timedwait(&g.changed, &g.mutex, &deadline);
      pthread_mutex_unlock(&g.mutex);
      REQUIRE(!rc && g.entered);
      retention_expect(p, "work/a", 1, 1, "old", 1, 2, &e);
      char *name = NULL, *cursor = NULL;
      ok(lc_pouch_retention_hint_next(p, NULL, &name, &cursor, &e), &e);
      REQUIRE(name && cursor && !strcmp(name, "work/a"));
      lc_free_with_allocator(&allocator, name);
      lc_free_with_allocator(&allocator, cursor);
      if (!phase) {
        lc_source *source = NULL;
        lc_pouch_state_write_options o = {0};
        lc_pouch_state_write_result written = {0};
        ok(lc_source_from_memory("peer", 4, &source, &e), &e);
        ok(lc_pouch_state_write(p, "peer", "body", source, &o, &written, &e),
           &e);
        REQUIRE(written.version == 1);
        lc_source_close(source);
        lc_pouch_state_write_result_cleanup(&allocator, &written);
      }
      pthread_mutex_lock(&g.mutex);
      g.released = 1;
      pthread_cond_broadcast(&g.changed);
      pthread_mutex_unlock(&g.mutex);
      REQUIRE(!pthread_join(thread, NULL) && a.rc == LC_OK);
      retention_expect(p, "work/a", phase != 2, phase == 2 ? 0 : 2, key,
                       phase == 2 ? 0 : 3, phase == 2 ? 0 : 4, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
      retention_expect(p, "work/a", phase != 2, phase == 2 ? 0 : 2, key,
                       phase == 2 ? 0 : 3, phase == 2 ? 0 : 4, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      one_file(path);
      REQUIRE(!pthread_cond_destroy(&g.changed) &&
              !pthread_mutex_destroy(&g.mutex));
    }
  free(key);
  lc_error_cleanup(&e);
  retention_captured_completion(directory);
}
static void retention_compare(const char *directory, int reverse) {
  lc_error e;
  lc_error_init(&e);
  const size_t sizes[] = {32, 8192, (size_t)PS_MIB};
  for (int engine = 0; engine < 2; ++engine)
    for (int policy = 0; policy < 2; ++policy)
      for (size_t fixture = 0; fixture < sizeof(sizes) / sizeof(*sizes);
           ++fixture) {
        int native = reverse ? 1 - engine : engine;
        int sync = reverse ? 1 - policy : policy;
        size_t size = sizes[fixture];
        char path[1400];
        char *key = malloc(size + 1);
        REQUIRE(key);
        memset(key, '%', size);
        key[size] = '\0';
        REQUIRE(snprintf(path, sizeof(path), "%s/retention-compare-%d-%d-%lu",
                         directory, native, sync,
                         (unsigned long)size) < (int)sizeof(path));
        lc_pouch *p = visit_open(path, native, sync, 1, NULL, NULL, &e);
        ok(lc_pouch_retention_hint_ensure(p, "work/a", &e), &e);
        for (int cold = 0; cold < 2; ++cold) {
          ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key,
                                           0, 0, &e),
             &e);
          double close_ms = 0, reopen_ms = 0;
          if (cold) {
            double boundary = namespace_clock(CLOCK_MONOTONIC);
            lc_pouch_close(p);
            close_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - boundary);
            boundary = namespace_clock(CLOCK_MONOTONIC);
            p = visit_open(path, native, sync, 0, NULL, NULL, &e);
            reopen_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - boundary);
          }
          retention_expect(p, "work/a", 1, 1, key, 0, 0, &e);
          retention_store(p, "work/a", 0, NULL, &e);
          double cpu_start = namespace_clock(CLOCK_PROCESS_CPUTIME_ID);
          double start = namespace_clock(CLOCK_MONOTONIC);
          for (int cycle = 0; cycle < 8; ++cycle) {
            ok(lc_pouch_retention_work_write(p, "work/a", 1, retention_xid, key,
                                             0, 0, &e),
               &e);
            retention_expect(p, "work/a", 1, 1, key, 0, 0, &e);
            retention_store(p, "work/a", 1, key, &e);
            retention_expect(p, "work/a", 1, 1, key, UINT64_MAX, UINT64_MAX,
                             &e);
            retention_store(p, "work/a", 2, key, &e);
            retention_expect(p, "work/a", 1, 2, key, UINT64_MAX, UINT64_MAX,
                             &e);
            retention_store(p, "work/a", 0, NULL, &e);
            retention_expect(p, "work/a", 1, 0, NULL, 0, 0, &e);
          }
          double elapsed_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
          double cpu_ms =
              1000 * (namespace_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu_start);
          printf("retention-compare native=%d sync=%d cold=%d key_bytes=%lu "
                 "cycles=8 steps=4 elapsed_ms=%.6f cpu_ms=%.6f close_ms=%.6f "
                 "reopen_ms=%.6f verified=1\n",
                 native, sync, cold, (unsigned long)size, elapsed_ms, cpu_ms,
                 close_ms, reopen_ms);
        }
        lc_pouch_close(p);
        REQUIRE(!live);
        if (!native)
          one_file(path);
        free(key);
      }
  DIR *dir = opendir(directory);
  struct dirent *entry;
  unsigned int mask = 0;
  REQUIRE(dir);
  while ((entry = readdir(dir))) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    unsigned int bit = 0;
    for (int native = 0; native < 2; ++native)
      for (int sync = 0; sync < 2; ++sync)
        for (size_t size = 0; size < sizeof(sizes) / sizeof(*sizes); ++size) {
          char expected[80];
          REQUIRE(snprintf(expected, sizeof(expected),
                           "retention-compare-%d-%d-%lu", native, sync,
                           (unsigned long)sizes[size]) < (int)sizeof(expected));
          if (!strcmp(entry->d_name, expected))
            bit = 1U << (native * 6 + sync * 3 + size);
        }
    REQUIRE(bit && !(mask & bit));
    mask |= bit;
  }
  REQUIRE(!closedir(dir) && mask == 4095);
  lc_error_cleanup(&e);
}
static void retention_discovery_step(lc_pouch *p, char **cursor,
                                     unsigned int index, lc_error *e) {
  char expected[32], *name = NULL, *next = NULL;
  REQUIRE(snprintf(expected, sizeof(expected), "hint/%02u", index) <
          (int)sizeof(expected));
  ok(lc_pouch_retention_hint_next(p, *cursor, &name, &next, e), e);
  REQUIRE(name && next && !strcmp(name, expected));
  lc_free_with_allocator(&p->allocator, name);
  lc_free_with_allocator(&p->allocator, *cursor);
  *cursor = next;
}
static void retention_discovery_compare(const char *directory, int reverse) {
  lc_error e;
  char *key = malloc((size_t)PS_MIB + 1);
  REQUIRE(key);
  memset(key, '%', (size_t)PS_MIB);
  key[PS_MIB] = '\0';
  lc_error_init(&e);
  for (int engine = 0; engine < 2; ++engine)
    for (int policy = 0; policy < 2; ++policy) {
      int native = reverse ? 1 - engine : engine;
      int sync = reverse ? 1 - policy : policy;
      char path[1400], ns[32];
      REQUIRE(snprintf(path, sizeof(path), "%s/retention-discovery-%d-%d",
                       directory, native, sync) < (int)sizeof(path));
      lc_pouch *p = visit_open(path, native, sync, 1, NULL, NULL, &e);
      for (unsigned int i = 0; i < 64; ++i) {
        REQUIRE(snprintf(ns, sizeof(ns), "hint/%02u", i) < (int)sizeof(ns));
        ok(lc_pouch_retention_hint_ensure(p, ns, &e), &e);
      }
      /* Discovery must not depend on the work-record size. */
      retention_store(p, "hint/00", 1, key, &e);
      for (int cold = 0; cold < 2; ++cold) {
        double open_ms = 0;
        if (cold) {
          lc_pouch_close(p);
          double start = namespace_clock(CLOCK_MONOTONIC);
          p = visit_open(path, native, sync, 0, NULL, NULL, &e);
          open_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        }
        char *cursor = NULL;
        double start = namespace_clock(CLOCK_MONOTONIC);
        retention_discovery_step(p, &cursor, 0, &e);
        double first_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        lc_free_with_allocator(&p->allocator, cursor);
        cursor = NULL;
        double cpu_start = namespace_clock(CLOCK_PROCESS_CPUTIME_ID);
        start = namespace_clock(CLOCK_MONOTONIC);
        for (unsigned int i = 0; i < 128; ++i)
          retention_discovery_step(p, &cursor, i % 64, &e);
        double elapsed_ms = 1000 * (namespace_clock(CLOCK_MONOTONIC) - start);
        double cpu_ms =
            1000 * (namespace_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu_start);
        lc_free_with_allocator(&p->allocator, cursor);
        printf(
            "retention-discovery native=%d sync=%d cold=%d hints=64 walks=128 "
            "open_ms=%.6f first_ms=%.6f elapsed_ms=%.6f cpu_ms=%.6f "
            "verified=1\n",
            native, sync, cold, open_ms, first_ms, elapsed_ms, cpu_ms);
      }
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "retention-discovery-");
  free(key);
  lc_error_cleanup(&e);
}

/* Exercise the authority used by the public engine. This pair shares
 * multifile's stripe 15 but has distinct container lock addresses. */
static const char guard_xid[] = "xid-0";
static const char guard_neighbor[] = "xid-347";
static void guard_unused(const lc_pouch_txn_guard *g) {
  REQUIRE(g->fd == -1 && !g->mutex_locked && !g->typed_handle &&
          !g->shared_mutation);
}
static void guard_lifecycle(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400];
      REQUIRE(snprintf(path, sizeof(path), "%s/guard-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      for (int cold = 0; cold < 2; ++cold) {
        lc_pouch *p =
            visit_open(path, native, sync, !cold, NULL, &allocator, &e);
        lc_pouch_txn_guard g, second;
        for (int no_error = 0; no_error < 2; ++no_error) {
          lc_error *error = no_error ? NULL : &e;
          REQUIRE(lc_pouch_txn_guard_lock(NULL, "xid", 0, &g, error) ==
                  LC_ERR_INVALID);
          guard_unused(&g);
          lc_pouch_txn_guard_release(&g);
          clear(&e);
          REQUIRE(lc_pouch_txn_guard_lock(p, NULL, 0, &g, error) ==
                  LC_ERR_INVALID);
          guard_unused(&g);
          clear(&e);
          REQUIRE(lc_pouch_txn_guard_lock(p, "", 0, &g, error) ==
                  LC_ERR_INVALID);
          guard_unused(&g);
          clear(&e);
          REQUIRE(lc_pouch_txn_guard_lock(p, "xid", 0, NULL, error) ==
                  LC_ERR_INVALID);
          clear(&e);
        }
        ok(lc_pouch_txn_guard_acquire(p, guard_xid, &g, &e), &e);
        REQUIRE(lc_pouch_txn_guard_covers(&g, guard_xid) &&
                lc_pouch_txn_guard_covers(&g, guard_neighbor) == native &&
                !lc_pouch_txn_guard_covers(&g, "xid-1"));
        if (!native) {
          ok(lc_pouch_txn_guard_lock(p, guard_neighbor, 0, &second, &e), &e);
          lc_pouch_txn_guard_release(&second);
        }
        for (int no_error = 0; no_error < 2; ++no_error) {
          REQUIRE(lc_pouch_txn_guard_lock(p, guard_xid, 0, &second,
                                          no_error ? NULL : &e) ==
                  LC_ERR_PENDING);
          guard_unused(&second);
          lc_pouch_txn_guard_release(&second);
          clear(&e);
        }
        ok(lc_pouch_txn_guard_lock(p, "xid-1", 0, &second, &e), &e);
        lc_pouch_txn_guard_release(&second);
        lc_pouch_txn_guard_release(&g);
        lc_pouch_txn_guard_release(&g);
        REQUIRE(!lc_pouch_txn_guard_covers(&g, guard_xid));
        ok(lc_pouch_txn_guard_acquire(p, guard_xid, &g, &e), &e);
        if (!native) {
          char peer_path[1450];
          REQUIRE(snprintf(peer_path, sizeof(peer_path), "%s/guard-peer",
                           directory) < (int)sizeof(peer_path));
          lc_pouch *peer =
              visit_open(peer_path, 0, sync, 1, NULL, &allocator, &e);
          ok(lc_pouch_txn_guard_lock(peer, guard_xid, 0, &second, &e), &e);
          lc_pouch_txn_guard_release(&second);
          lc_pouch_close(peer);
          one_file(peer_path);
          REQUIRE(!unlink(peer_path));
        }
        if (!native) {
          /* The handle, rather than the logical runtime, retains this owner. */
          lc_pouch_close(p);
          p = NULL;
          ps_pouch_state_options o = {0};
          o.logical.single_writer = o.logical.single_writer_set = 1;
          o.logical.background_compaction_enabled_set = 1;
          o.logical.query_indexing_enabled_set = 1;
          lc_pouch *blocked = NULL;
          REQUIRE(ps_pouch_state_open(path, 0, &allocator, &o, &blocked, &e) ==
                  LC_ERR_INVALID);
          REQUIRE(!blocked && e.message &&
                  strstr(e.message, "container already open"));
          clear(&e);
        }
        lc_pouch_txn_guard_release(&g);
        lc_pouch_close(p);
        REQUIRE(!live);
        if (!native)
          one_file(path);
      }
    }
  namespace_artifacts(directory, "guard-");
  lc_error_cleanup(&e);
}
typedef struct guard_actor {
  lc_pouch *p;
  const char *xid;
  int wait, started, complete, rc;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
} guard_actor;
static void *guard_act(void *context) {
  guard_actor *a = context;
  lc_pouch_txn_guard g;
  lc_error e;
  lc_error_init(&e);
  pthread_mutex_lock(&a->mutex);
  a->started = 1;
  pthread_cond_signal(&a->changed);
  pthread_mutex_unlock(&a->mutex);
  a->rc = lc_pouch_txn_guard_lock(a->p, a->xid, a->wait, &g, &e);
  if (a->rc == LC_OK)
    lc_pouch_txn_guard_release(&g);
  else
    guard_unused(&g);
  pthread_mutex_lock(&a->mutex);
  a->complete = 1;
  pthread_cond_signal(&a->changed);
  pthread_mutex_unlock(&a->mutex);
  lc_error_cleanup(&e);
  return NULL;
}
static void guard_concurrency(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400];
      REQUIRE(snprintf(path, sizeof(path), "%s/guard-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      lc_pouch *p = visit_open(path, native, sync, 1, NULL, &allocator, &e);
      lc_pouch_txn_guard held;
      ok(lc_pouch_txn_guard_acquire(p, guard_xid, &held, &e), &e);
      /* Independent container authority succeeds while this XID is held. */
      for (int i = 0; i < 3; ++i) {
        guard_actor a = {0};
        a.p = p;
        a.xid = i == 0 ? guard_xid : i == 1 ? guard_neighbor : "xid-1";
        REQUIRE(!pthread_mutex_init(&a.mutex, NULL));
        REQUIRE(!pthread_cond_init(&a.changed, NULL));
        pthread_t thread;
        REQUIRE(!pthread_create(&thread, NULL, guard_act, &a));
        REQUIRE(!pthread_join(thread, NULL));
        REQUIRE(a.complete &&
                a.rc == (!i || (i == 1 && native) ? LC_ERR_PENDING : LC_OK));
        REQUIRE(!pthread_cond_destroy(&a.changed));
        REQUIRE(!pthread_mutex_destroy(&a.mutex));
      }
      guard_actor a = {0};
      a.p = p;
      a.xid = guard_xid;
      a.wait = 1;
      REQUIRE(!pthread_mutex_init(&a.mutex, NULL));
      REQUIRE(!pthread_cond_init(&a.changed, NULL));
      pthread_t thread;
      REQUIRE(!pthread_create(&thread, NULL, guard_act, &a));
      pthread_mutex_lock(&a.mutex);
      while (!a.started)
        pthread_cond_wait(&a.changed, &a.mutex);
      REQUIRE(!a.complete);
      pthread_mutex_unlock(&a.mutex);
      lc_pouch_txn_guard_release(&held);
      REQUIRE(!pthread_join(thread, NULL));
      REQUIRE(a.complete && a.rc == LC_OK);
      REQUIRE(!pthread_cond_destroy(&a.changed));
      REQUIRE(!pthread_mutex_destroy(&a.mutex));
      if (native) {
        /* Fork before acquiring any guard. Child uses async-signal-safe file
         * operations only; parent must observe the process's native lock. */
        char leaf[1600];
        REQUIRE(snprintf(leaf, sizeof(leaf), "%s/pouch.implicit-txn-%02lx.lock",
                         path, (unsigned long)held.stripe) < (int)sizeof(leaf));
        int ready[2], resume[2];
        REQUIRE(!pipe(ready) && !pipe(resume));
        pid_t child = fork();
        REQUIRE(child >= 0);
        if (!child) {
          int fd = open(leaf, O_RDWR);
          struct flock lock;
          memset(&lock, 0, sizeof(lock));
          lock.l_type = F_WRLCK;
          lock.l_whence = SEEK_SET;
          char byte = 'x';
          if (fd < 0 || fcntl(fd, F_SETLK, &lock) ||
              write(ready[1], &byte, 1) != 1 || read(resume[0], &byte, 1) != 1)
            _exit(2);
          close(fd);
          _exit(0);
        }
        REQUIRE(!close(ready[1]) && !close(resume[0]));
        char byte;
        REQUIRE(read(ready[0], &byte, 1) == 1);
        REQUIRE(lc_pouch_txn_guard_lock(p, "xid-0", 0, &held, &e) ==
                LC_ERR_PENDING);
        guard_unused(&held);
        clear(&e);
        REQUIRE(write(resume[1], "x", 1) == 1);
        int status;
        REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
                WEXITSTATUS(status) == 0);
        REQUIRE(!close(ready[0]) && !close(resume[1]));
        ok(lc_pouch_txn_guard_acquire(p, "xid-0", &held, &e), &e);
        lc_pouch_txn_guard_release(&held);
      }
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "guard-");
  lc_error_cleanup(&e);
}
#ifdef __linux__
static void guard_faults(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400], moved[1450];
      REQUIRE(snprintf(path, sizeof(path), "%s/guard-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      io_fault f = {0};
      ps_io io = {&f, disk_read, disk_write, disk_sync, disk_reserve, NULL};
      lc_pouch *p = visit_open(path, native, sync, 1, &io, &allocator, &e);
      if (!native) {
        lc_pouch_close(p);
        REQUIRE(!live);
        mutex_init_calls = mutex_init_fail = 0;
        mutex_init_armed = 1;
        p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
        size_t count = mutex_init_calls;
        mutex_init_armed = 0;
        REQUIRE(count > 0);
        lc_pouch_close(p);
        REQUIRE(!live);
        /* Discover the actual initialization sites. File coordination now
         * precedes recovery; mutex ordering is not an interface contract.
         * Every required initializer must reject admission without leaking
         * lifetime authority, including with no error-output object. */
        for (size_t cut = 1; cut <= count; ++cut)
          for (int no_error = 0; no_error < 2; ++no_error) {
            ps_pouch_state_options o = {0};
            o.logical.single_writer = o.logical.single_writer_set = 1;
            o.logical.background_compaction_enabled_set = 1;
            o.logical.query_indexing_enabled_set = 1;
            o.logical.query_engine = "scan";
            o.logical.durable_sync = sync;
            o.io = &io;
            p = NULL;
            mutex_init_calls = 0;
            mutex_init_fail = cut;
            mutex_init_armed = 1;
            int rc = ps_pouch_state_open(path, 0, &allocator, &o, &p,
                                         no_error ? NULL : &e);
            mutex_init_armed = 0;
            REQUIRE(mutex_init_calls >= cut);
            REQUIRE(rc == LC_ERR_TRANSPORT && !p && !live);
            REQUIRE(no_error || (e.code == rc && e.message && *e.message));
            clear(&e);
            unlocked(path);
          }
        p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
      }
      lc_pouch_txn_guard g;
      allocation = fail_at = 0;
      armed = f.armed = 1;
      ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
      size_t total = allocation;
      armed = f.armed = 0;
      REQUIRE(!f.calls);
      lc_pouch_txn_guard_release(&g);
      REQUIRE(total == 1);
      for (size_t nth = 1; nth <= total; ++nth)
        for (int no_error = 0; no_error < 2; ++no_error) {
          allocation = 0;
          fail_at = nth;
          armed = 1;
          int rc =
              lc_pouch_txn_guard_acquire(p, "xid-0", &g, no_error ? NULL : &e);
          armed = 0;
          REQUIRE(rc == LC_ERR_NOMEM);
          guard_unused(&g);
          lc_pouch_txn_guard_release(&g);
          clear(&e);
          ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
          lc_pouch_txn_guard_release(&g);
        }
      if (native) {
        REQUIRE(snprintf(moved, sizeof(moved), "%s-moved", path) <
                (int)sizeof(moved));
        REQUIRE(!rename(path, moved));
        REQUIRE(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e) ==
                LC_ERR_TRANSPORT);
        guard_unused(&g);
        lc_pouch_txn_guard_release(&g);
        clear(&e);
        REQUIRE(!rename(moved, path));
        ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
        lc_pouch_txn_guard_release(&g);
      }
      if (!native) {
        ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
        void *handle = NULL;
        REQUIRE(p->storage.ops->txn_guard_lock(p->storage.context, "", 0,
                                               &handle, &e) == LC_ERR_INVALID);
        REQUIRE(!handle);
        clear(&e);
        f.calls = 0;
        f.fail = f.armed = 1;
        REQUIRE(ps_checkpoint(ps_root_space(ps_pouch_state_root(p)), &e) ==
                LC_ERR_TRANSPORT);
        f.armed = 0;
        clear(&e);
        lc_pouch_txn_guard denied;
        for (int no_error = 0; no_error < 2; ++no_error) {
          f.calls = 0;
          f.armed = 1;
          REQUIRE(lc_pouch_txn_guard_lock(p, "xid-1", 0, &denied,
                                          no_error ? NULL : &e) ==
                  LC_ERR_TRANSPORT);
          f.armed = 0;
          REQUIRE(!f.calls);
          guard_unused(&denied);
          lc_pouch_txn_guard_release(&denied);
          clear(&e);
        }
        lc_pouch_txn_guard_release(&g);
        lc_pouch_close(p);
        REQUIRE(!live);
        p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
        ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
        lc_pouch_txn_guard_release(&g);
      }
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!native)
        one_file(path);
    }
  namespace_artifacts(directory, "guard-");
  puts("transaction guards: open initializer and guard allocation faults, "
       "both policies and nullable errors, uncertain storage admission, "
       "retry, and no-I/O container acquire");
  lc_error_cleanup(&e);
}
#endif
static void guard_public(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync) {
      char path[1400], endpoint[1600];
      REQUIRE(snprintf(path, sizeof(path), "%s/guard-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      REQUIRE(
          snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?single_writer=true&background_compaction=false&"
                   "query_indexing=false&queue_watch=false&query_engine=scan",
                   path) < (int)sizeof(endpoint));
      const char *endpoints[] = {endpoint};
      lc_pouch_settings settings;
      lc_pouch_settings_init(&settings);
      settings.set_mask = LC_POUCH_SETTING_DURABLE_SYNC;
      settings.durable_sync = sync;
      lc_client_config cfg;
      lc_client_config_init(&cfg);
      cfg.endpoints = endpoints;
      cfg.endpoint_count = 1;
      cfg.allocator = allocator;
      cfg.default_namespace = "orders";
      cfg.pouch_settings = &settings;
      for (int cold = 0; cold < 2; ++cold) {
        lc_client *client = NULL;
        ok(native ? lc_client_open(&cfg, &client, &e)
                  : ps_pouch_client_open(&cfg, !cold, NULL, &client, &e),
           &e);
        if (cold) {
          read_value(client, NULL, "kept", "{\"value\":7}", &e);
        } else {
          lc_lease *lease = NULL;
          lc_acquire_req acquire;
          lc_acquire_req_init(&acquire);
          acquire.key = "kept";
          acquire.owner = "guard-test";
          acquire.ttl_seconds = 300;
          ok(client->acquire(client, &acquire, &lease, &e), &e);
          value v = {7};
          lc_update_opts options = {0};
          options.has_retention_seconds = 1;
          options.retention_seconds = 3600;
          ok(lc_lease_save(lease, &value_map, &v, &options, &e), &e);
          ok(lc_lease_release(lease, NULL, &e), &e);
          read_value(client, NULL, "kept", "{\"value\":7}", &e);
          acquire.key = "rolled";
          ok(client->acquire(client, &acquire, &lease, &e), &e);
          ok(lc_lease_save(lease, &value_map, &v, &options, &e), &e);
          lc_release_req release = {1};
          ok(lc_lease_release(lease, &release, &e), &e);
        }
        lc_pouch_state_read_result r = {0};
        lc_pouch *p = ((lc_client_handle *)client)->pouch;
        ok(lc_pouch_state_read_metadata(p, "orders", "rolled", &r, &e), &e);
        REQUIRE(!r.has_body && !r.retention);
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        ok(lc_pouch_state_read_metadata(p, "orders", "kept", &r, &e), &e);
        lc_pouch_unix_seconds now = 0;
        ok(lc_pouch_now_unix(&now, &e), &e);
        REQUIRE(r.has_body && r.retention > now && r.retention <= now + 3600);
        lc_pouch_state_read_result_cleanup(&allocator, &r);
        lc_client_close(client);
        REQUIRE(!live);
        if (!native)
          one_file(path);
      }
    }
  namespace_artifacts(directory, "guard-");
  lc_error_cleanup(&e);
}
static void guard_compare(const char *directory, int reverse) {
  lc_error e;
  lc_error_init(&e);
  for (int engine = 0; engine < 2; ++engine)
    for (int policy = 0; policy < 2; ++policy) {
      int native = reverse ? 1 - engine : engine;
      int sync = reverse ? 1 - policy : policy;
      char path[1400];
      REQUIRE(snprintf(path, sizeof(path), "%s/guard-%d-%d", directory, native,
                       sync) < (int)sizeof(path));
      for (int cold = 0; cold < 2; ++cold) {
        lc_pouch *p = visit_open(path, native, sync, !cold, NULL, NULL, &e);
        lc_pouch_txn_guard g;
        double first = namespace_clock(CLOCK_MONOTONIC);
        ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
        REQUIRE(lc_pouch_txn_guard_covers(&g, "xid-0"));
        lc_pouch_txn_guard_release(&g);
        first = 1e6 * (namespace_clock(CLOCK_MONOTONIC) - first);
        double cpu = namespace_clock(CLOCK_PROCESS_CPUTIME_ID);
        double start = namespace_clock(CLOCK_MONOTONIC);
        for (unsigned int i = 0; i < 1024; ++i) {
          ok(lc_pouch_txn_guard_acquire(p, "xid-0", &g, &e), &e);
          REQUIRE(lc_pouch_txn_guard_covers(&g, "xid-0"));
          lc_pouch_txn_guard_release(&g);
        }
        double elapsed = 1e6 * (namespace_clock(CLOCK_MONOTONIC) - start);
        cpu = 1e6 * (namespace_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu);
        printf(
            "guard-compare native=%d sync=%d cold=%d locks=1024 first_us=%.6f "
            "elapsed_us=%.6f cpu_us=%.6f verified=1\n",
            native, sync, cold, first, elapsed, cpu);
        lc_pouch_close(p);
        if (!native)
          one_file(path);
      }
    }
  namespace_artifacts(directory, "guard-");
  lc_error_cleanup(&e);
}
#include "index_capacity_cases.h"
#include "index_cases.h"
#include "index_concurrency_cases.h"
#include "index_crash_cases.h"
#ifdef __linux__
#include "index_open_cases.h"
#endif
#include "queue_outbox_concurrency_cases.h"
#include "queue_outbox_crash_cases.h"
#include "xa_capacity_cases.h"
#include "xa_cases.h"
#include "xa_concurrency_cases.h"
#include "xa_crash_cases.h"
#include "xa_managed_cases.h"
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
  else if (!strcmp(r->mode, "namespaces"))
    namespace_visits(r->directory);
  else if (!strcmp(r->mode, "namespace-concurrency"))
    namespace_concurrency(r->directory);
  else if (!strcmp(r->mode, "namespace-compare") ||
           !strcmp(r->mode, "namespace-compare-reverse"))
    namespace_compare(r->directory,
                      !strcmp(r->mode, "namespace-compare-reverse"));
  else if (!strcmp(r->mode, "retention-codec"))
    retention_codec();
  else if (!strcmp(r->mode, "outbox-markers"))
    outbox_markers(r->directory);
  else if (!strcmp(r->mode, "xa-durability"))
    xa_faults(r->directory, 0);
  else if (!strcmp(r->mode, "index-artifacts"))
    index_artifacts(r->directory);
  else if (!strcmp(r->mode, "index-shared"))
    index_shared(r->directory);
#ifdef __linux__
  else if (!strcmp(r->mode, "index-open-faults"))
    index_open_faults(r->directory);
#endif
  else if (!strcmp(r->mode, "index-durability"))
    index_faults(r->directory, 0);
  else if (!strcmp(r->mode, "index-io-faults"))
    index_faults(r->directory, 1);
  else if (!strcmp(r->mode, "index-recovery"))
    index_lost_revision(r->directory);
  else if (!strcmp(r->mode, "index-allocation-faults"))
    index_faults(r->directory, 2);
  else if (!strcmp(r->mode, "index-capacity"))
    index_capacity(r->directory);
  else if (!strcmp(r->mode, "index-concurrency"))
    index_concurrency(r->directory);
  else if (!strcmp(r->mode, "index-crash"))
    index_crashes(r->directory);
  else if (!strcmp(r->mode, "xa-io-faults"))
    xa_faults(r->directory, 1);
  else if (!strcmp(r->mode, "xa-allocation-faults"))
    xa_faults(r->directory, 2);
  else if (!strcmp(r->mode, "xa-managed-durability"))
    xa_managed_faults(r->directory, 0);
  else if (!strcmp(r->mode, "xa-retirement-batch"))
    xa_retirement_batch(r->directory);
  else if (!strcmp(r->mode, "xa-concurrency"))
    xa_concurrency(r->directory);
  else if (!strcmp(r->mode, "xa-scope"))
    xa_scope(r->directory);
  else if (!strcmp(r->mode, "xa-capacity"))
    xa_capacity(r->directory);
  else if (!strcmp(r->mode, "xa-crash"))
    xa_crashes(r->directory);
  else if (!strcmp(r->mode, "xa-managed-io-faults"))
    xa_managed_faults(r->directory, 1);
  else if (!strcmp(r->mode, "xa-managed-allocation-faults"))
    xa_managed_faults(r->directory, 2);
  else if (!strcmp(r->mode, "outbox-notifications"))
    outbox_notifications(r->directory);
  else if (!strcmp(r->mode, "outbox-marker-io-faults"))
    outbox_marker_faults(r->directory, 0);
  else if (!strcmp(r->mode, "outbox-marker-allocation-faults"))
    outbox_marker_faults(r->directory, 1);
  else if (!strcmp(r->mode, "outbox-marker-crash"))
    outbox_marker_crashes(r->directory);
  else if (!strcmp(r->mode, "outbox-marker-concurrency"))
    outbox_marker_concurrency(r->directory);
  else if (!strcmp(r->mode, "retention"))
    retention_lifecycle(r->directory);
  else if (!strcmp(r->mode, "retention-faults"))
    retention_allocation_faults(r->directory);
  else if (!strcmp(r->mode, "retention-io-faults"))
    retention_io_faults(r->directory);
  else if (!strcmp(r->mode, "retention-capacity"))
    retention_capacity(r->directory);
  else if (!strcmp(r->mode, "retention-progress"))
    retention_progress_capacity(r->directory);
  else if (!strcmp(r->mode, "retention-capacity-faults"))
    retention_capacity_faults(r->directory);
  else if (!strcmp(r->mode, "retention-concurrency"))
    retention_concurrency(r->directory);
  else if (!strcmp(r->mode, "retention-compare") ||
           !strcmp(r->mode, "retention-compare-reverse"))
    retention_compare(r->directory,
                      !strcmp(r->mode, "retention-compare-reverse"));
  else if (!strcmp(r->mode, "retention-discovery-compare") ||
           !strcmp(r->mode, "retention-discovery-compare-reverse"))
    retention_discovery_compare(
        r->directory, !strcmp(r->mode, "retention-discovery-compare-reverse"));
  else if (!strcmp(r->mode, "guards"))
    guard_lifecycle(r->directory);
  else if (!strcmp(r->mode, "guard-concurrency"))
    guard_concurrency(r->directory);
#ifdef __linux__
  else if (!strcmp(r->mode, "guard-faults"))
    guard_faults(r->directory);
#endif
  else if (!strcmp(r->mode, "guard-public"))
    guard_public(r->directory);
  else if (!strcmp(r->mode, "guard-compare") ||
           !strcmp(r->mode, "guard-compare-reverse"))
    guard_compare(r->directory, !strcmp(r->mode, "guard-compare-reverse"));
  else
    REQUIRE(0);
  return NULL;
}
int main(int argc, char **argv) {
  char prefix[1100], pattern[1200], directory[1200];
  pthread_t thread;
  REQUIRE(argc == 3 || argc == 4);
  if (argc == 4)
    REQUIRE(!setenv("POUCH_INDEX_CODEC", argv[3], 1));
  if (!strcmp(argv[2], "xa-crash-child"))
    xa_crash_child();
  if (!strcmp(argv[2], "index-crash-child"))
    index_crash_child();
  if (!strcmp(argv[2], "outbox-marker-crash-child"))
    outbox_marker_crash_child();
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
