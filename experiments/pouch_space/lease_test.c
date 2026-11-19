#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "budget.h"
#include "coordination.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_retention.h"
#include "lc_pouch_txn_guard.h"
#include "pouch.h"
#include "pouch_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static size_t checks, live, roots;
static size_t cases, allocation_calls, allocation_fail;
static size_t allocation_self, allocation_other;
static pthread_t allocation_thread;
static int allocation_armed, allocation_fired;
static char *crypto_key;
static const char *executable_path;
static char scenario[192];
static int benchmark_normal, measure_enabled;
static size_t measured_syncs;
static double phase_started, phase_elapsed;
static int shared_lease_fixture;
int __real_ps_coordination_open(int, const ps_coordination_options *,
                                ps_coordination **, lc_error *);
int __wrap_ps_coordination_open(int fd, const ps_coordination_options *options,
                                ps_coordination **out, lc_error *e) {
  ps_coordination_options selected = *options;
  if (shared_lease_fixture)
    selected.shared = 1;
  return __real_ps_coordination_open(fd, &selected, out, e);
}
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s [%s]\n", __FILE__, __LINE__, #x, scenario);   \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "lease %d: %s (%s)\n", rc,
            e && e->message ? e->message : "?",
            e && e->detail ? e->detail : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static double real_clock(clockid_t id) {
  struct timespec t;
  REQUIRE(!clock_gettime(id, &t));
  return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}
int __real_fdatasync(int);
int __real_fsync(int);
int __wrap_fdatasync(int fd) {
  if (__atomic_load_n(&measure_enabled, __ATOMIC_ACQUIRE))
    __atomic_add_fetch(&measured_syncs, 1, __ATOMIC_RELAXED);
  return __real_fdatasync(fd);
}
int __wrap_fsync(int fd) {
  if (__atomic_load_n(&measure_enabled, __ATOMIC_ACQUIRE))
    __atomic_add_fetch(&measured_syncs, 1, __ATOMIC_RELAXED);
  return __real_fsync(fd);
}
typedef union allocation_header {
  long double alignment;
  struct {
    size_t bytes;
    uintptr_t cookie;
  } value;
} allocation_header;
static void *allocate(void *context, size_t n) {
  (void)context;
  allocation_header *h = malloc(sizeof(*h) + n);
  if (!h)
    return NULL;
  h->value.bytes = n;
  h->value.cookie = (uintptr_t)h ^ UINT64_C(0x2367bd95134579ac);
  __atomic_add_fetch(&live, 1, __ATOMIC_RELAXED);
  return h + 1;
}
static void deallocate(void *context, void *p) {
  (void)context;
  if (!p)
    return;
  allocation_header *h = (allocation_header *)p - 1;
  REQUIRE(h->value.cookie == ((uintptr_t)h ^ UINT64_C(0x2367bd95134579ac)));
  h->value.cookie = 0;
  __atomic_sub_fetch(&live, 1, __ATOMIC_RELAXED);
  free(h);
}
static void *resize(void *context, void *p, size_t n) {
  void *next = allocate(context, n);
  if (p && next) {
    allocation_header *old = (allocation_header *)p - 1;
    memcpy(next, p, old->value.bytes < n ? old->value.bytes : n);
    deallocate(context, p);
  }
  return next;
}
static lc_allocator allocator = {allocate, resize, deallocate, NULL};
static int allocation_cut(void) {
  if (!__atomic_load_n(&allocation_armed, __ATOMIC_ACQUIRE))
    return 0;
  if (!pthread_equal(pthread_self(), allocation_thread)) {
    __atomic_add_fetch(&allocation_other, 1, __ATOMIC_RELAXED);
    return 0; /* Independent HA heartbeats are outside this operation's cut. */
  }
  __atomic_add_fetch(&allocation_self, 1, __ATOMIC_RELAXED);
  if (__atomic_add_fetch(&allocation_calls, 1, __ATOMIC_RELAXED) ==
      allocation_fail) {
    __atomic_store_n(&allocation_fired, 1, __ATOMIC_RELAXED);
    return 1;
  }
  return 0;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return allocation_cut() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t s) {
  return allocation_cut() ? NULL : __real_lc_calloc_with_allocator(a, n, s);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return allocation_cut() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return allocation_cut() ? NULL : __real_lc_strdup_with_allocator(a, p);
}

/* The established logical clock hook leaves native heartbeat timers and all
 * operational clocks real. time()-based metadata timestamps are independent. */
static int clock_enabled;
static lc_pouch_unix_seconds clock_now;
static int logical_clock(clockid_t id, struct timespec *out, void *context) {
  (void)context;
  if (id == CLOCK_REALTIME &&
      __atomic_load_n(&clock_enabled, __ATOMIC_ACQUIRE)) {
    out->tv_sec = (time_t)__atomic_load_n(&clock_now, __ATOMIC_RELAXED);
    out->tv_nsec = 0;
    return 0;
  }
  return clock_gettime(id, out);
}
static void now(lc_pouch_unix_seconds n) {
  __atomic_store_n(&clock_now, n, __ATOMIC_RELAXED);
  __atomic_store_n(&clock_enabled, 1, __ATOMIC_RELEASE);
}
static int phase_fail;
static int phase_crash;
static unsigned long phase_mask;
static size_t phase_hits;
static size_t attachment_cut_skip;
typedef struct rendezvous {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int entered, released;
} rendezvous;
static rendezvous *phase_gate;
static int gated_phase;
static void gate_init(rendezvous *g) {
  memset(g, 0, sizeof(*g));
  REQUIRE(!pthread_mutex_init(&g->mutex, NULL));
  REQUIRE(!pthread_cond_init(&g->condition, NULL));
}
static void gate_wait(rendezvous *g, int released) {
  struct timespec until;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &until));
  until.tv_sec += 30;
  REQUIRE(!pthread_mutex_lock(&g->mutex));
  while (!(released ? g->released : g->entered))
    REQUIRE(!pthread_cond_timedwait(&g->condition, &g->mutex, &until));
  REQUIRE(!pthread_mutex_unlock(&g->mutex));
}
static void gate_signal(rendezvous *g, int released) {
  REQUIRE(!pthread_mutex_lock(&g->mutex));
  if (released)
    g->released = 1;
  else
    g->entered = 1;
  REQUIRE(!pthread_cond_broadcast(&g->condition));
  REQUIRE(!pthread_mutex_unlock(&g->mutex));
}
static void gate_destroy(rendezvous *g) {
  REQUIRE(!pthread_cond_destroy(&g->condition));
  REQUIRE(!pthread_mutex_destroy(&g->mutex));
}
static int note_phase(int phase, void *context, lc_error *e) {
  (void)context;
  REQUIRE(phase >= LC_POUCH_RETENTION_REF_CREATED &&
          phase < LC_POUCH_RETENTION_PHASE_COUNT);
  __atomic_fetch_or(&phase_mask, 1UL << phase, __ATOMIC_RELAXED);
  if (__atomic_load_n(&measure_enabled, __ATOMIC_ACQUIRE)) {
    if (phase == LC_POUCH_RETENTION_STAGED)
      phase_started = real_clock(CLOCK_MONOTONIC);
    else if (phase == LC_POUCH_RETENTION_DECIDED)
      phase_elapsed += real_clock(CLOCK_MONOTONIC) - phase_started;
  }
  if (phase_gate && phase == gated_phase) {
    gate_signal(phase_gate, 0);
    gate_wait(phase_gate, 1);
  }
  if (phase == __atomic_load_n(&phase_crash, __ATOMIC_ACQUIRE))
    _exit(70 + phase);
  if (phase == LC_POUCH_RETENTION_ATTACHMENT_APPLIED &&
      phase == __atomic_load_n(&phase_fail, __ATOMIC_ACQUIRE) &&
      attachment_cut_skip) {
    --attachment_cut_skip;
    return LC_OK;
  }
  if (phase == __atomic_load_n(&phase_fail, __ATOMIC_ACQUIRE) &&
      __atomic_exchange_n(&phase_fail, 0, __ATOMIC_ACQ_REL)) {
    __atomic_add_fetch(&phase_hits, 1, __ATOMIC_RELAXED);
    return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected expiry phase failure",
                        NULL, NULL, "test");
  }
  return LC_OK;
}

typedef struct fixture {
  char path[1400], endpoint[1700];
  const char *endpoints[1];
  lc_pouch_settings settings;
  lc_client_config config;
  lc_client *client;
  const ps_io *io;
  int native, sync, crypto;
  long fencing;
} fixture;
static lc_pouch *store(fixture *f) {
  return ((lc_client_handle *)f->client)->pouch;
}
static void open_store(fixture *f, int create, lc_error *e) {
  REQUIRE(!f->client);
  ps_pouch_client_options physical = {0};
  physical.io = f->io;
  ok(f->native ? lc_client_open(&f->config, &f->client, e)
               : ps_pouch_client_open(&f->config, create,
                                      f->io ? &physical : NULL, &f->client, e),
     e);
}
static void close_store(fixture *f) {
  lc_client_close(f->client);
  f->client = NULL;
  REQUIRE(!live);
  if (!f->native) {
    struct stat info;
    REQUIRE(!stat(f->path, &info) && S_ISREG(info.st_mode) &&
            info.st_size == PS_DEFAULT_CAPACITY);
  }
}
static void configure(fixture *f, int native, int sync, int crypto) {
  f->native = native;
  f->sync = sync;
  f->crypto = crypto;
  REQUIRE(snprintf(f->endpoint, sizeof(f->endpoint),
                   "pouch://%s?single_writer=true&background_compaction=false&"
                   "query_indexing=false&queue_watch=false&query_engine=scan",
                   f->path) < (int)sizeof(f->endpoint));
  f->endpoints[0] = f->endpoint;
  lc_pouch_settings_init(&f->settings);
  f->settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION;
  f->settings.durable_sync = sync;
  f->settings.compression = crypto & 2 ? "zlib" : "none";
  lc_client_config_init(&f->config);
  f->config.endpoints = f->endpoints;
  f->config.endpoint_count = 1;
  f->config.pouch_settings = &f->settings;
  f->config.pouch_crypto_key = crypto & 1 ? crypto_key : NULL;
  f->config.default_namespace = "orders";
  if (!benchmark_normal)
    f->config.allocator = allocator;
}
static void setup_io(fixture *f, const char *directory, int native, int sync,
                     int crypto, const ps_io *io, lc_error *e) {
  memset(f, 0, sizeof(*f));
  f->io = io;
  ++cases;
  REQUIRE(snprintf(f->path, sizeof(f->path), "%s/root-%lu", directory,
                   (unsigned long)roots++) < (int)sizeof(f->path));
  configure(f, native, sync, crypto);
  open_store(f, 1, e);
}
static void setup(fixture *f, const char *directory, int native, int sync,
                  int crypto, lc_error *e) {
  setup_io(f, directory, native, sync, crypto, NULL, e);
}
/* Only called on a closed fixture created beneath this test's owned directory.
 * Removing each failure trial bounds disk use independently of cut count. */
static void remove_tree(const char *path) {
  DIR *dir = opendir(path);
  if (!dir) {
    REQUIRE(!unlink(path));
    return;
  }
  struct dirent *entry;
  while ((entry = readdir(dir))) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    char child[2000];
    struct stat info;
    REQUIRE(snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) <
            (int)sizeof(child));
    REQUIRE(!lstat(child, &info));
    if (S_ISDIR(info.st_mode))
      remove_tree(child);
    else
      REQUIRE(!unlink(child));
  }
  REQUIRE(!closedir(dir) && !rmdir(path));
}
static void retire(fixture *f) {
  REQUIRE(!f->client && roots == 1);
  remove_tree(f->path);
  --roots;
}
typedef struct producer {
  lc_source pub;
  const char *bytes;
  size_t at, length, calls;
} producer;
static size_t pull(lc_source *pub, void *bytes, size_t n, lc_error *e) {
  producer *p = pub->impl;
  (void)e;
  ++p->calls;
  if (n > 17)
    n = 17;
  if (n > p->length - p->at)
    n = p->length - p->at;
  memcpy(bytes, p->bytes + p->at, n);
  p->at += n;
  return n;
}
static producer source(const char *bytes) {
  producer p = {0};
  p.pub.read = pull;
  p.bytes = bytes;
  p.length = strlen(bytes);
  return p;
}
static lc_lease *acquire(fixture *f, const char *key, const char *xid,
                         lc_error *e) {
  lc_acquire_req req;
  lc_lease *lease = NULL;
  lc_acquire_req_init(&req);
  req.key = key;
  req.owner = "expiry-test";
  req.ttl_seconds = 300;
  req.txn_id = xid;
  ok(f->client->acquire(f->client, &req, &lease, e), e);
  REQUIRE(lease && lease->fencing_token > 0);
  return lease;
}
static void payload(lc_lease *lease, const lc_update_opts *options,
                    lc_error *e) {
  producer p =
      source("{\"value\":42,\"description\":\"expiry streaming fixture\"}");
  p.pub.impl = &p;
  ok(lease->update(lease, &p.pub, options, e), e);
  REQUIRE(p.at == p.length && p.calls >= 3);
}
static void write_parent(fixture *f, const char *key, long seconds,
                         int attachment, lc_error *e) {
  lc_lease *lease = acquire(f, key, NULL, e);
  lc_update_opts options = {0};
  options.has_retention_seconds = 1;
  options.retention_seconds = seconds;
  payload(lease, &options, e);
  if (attachment) {
    lc_attach_req req;
    lc_attach_res result = {0};
    lc_attach_req_init(&req);
    req.name = "receipt.txt";
    req.content_type = "text/plain";
    producer a = source("receipt data supplied through the public lease API");
    a.pub.impl = &a;
    ok(lease->attach(lease, &req, &a.pub, &result, e), e);
    REQUIRE(a.at == a.length && a.calls >= 3);
    lc_attach_res_cleanup(&result);
  }
  f->fencing = lease->fencing_token;
  ok(lease->release(lease, NULL, e), e);
}
static void parent(fixture *f, const char *key, int present,
                   lc_pouch_unix_seconds deadline, lc_error *e) {
  lc_pouch_state_read_result r = {0};
  ok(lc_pouch_state_read_metadata(store(f), "orders", key, &r, e), e);
  REQUIRE(r.has_body == present && r.retention == deadline);
  lc_pouch_state_read_result_cleanup(&store(f)->allocator, &r);
  lc_sink *sink = NULL;
  lc_get_res got = {0};
  ok(lc_sink_to_memory(&sink, e), e);
  int rc = f->client->get(f->client, key, NULL, sink, &got, e);
  if (present) {
    ok(rc, e);
    const void *bytes;
    size_t n;
    ok(lc_sink_memory_bytes(sink, &bytes, &n, e), e);
    const char *expected =
        "{\"value\":42,\"description\":\"expiry streaming fixture\"}";
    REQUIRE(!got.no_content && n == strlen(expected) &&
            !memcmp(bytes, expected, n));
  } else {
    ok(rc, e);
    REQUIRE(got.no_content);
  }
  lc_get_res_cleanup(&got);
  lc_sink_close(sink);
}
static void attachments(fixture *f, const char *key, size_t count,
                        lc_error *e) {
  lc_attachment_list_req req;
  lc_attachment_list list = {0};
  lc_attachment_list_req_init(&req);
  req.lease.ns = "orders";
  req.lease.key = key;
  req.public_read = 1;
  ok(f->client->list_attachments(f->client, &req, &list, e), e);
  REQUIRE(list.count == count);
  lc_attachment_list_cleanup(&list);
}
static lc_pouch_maintenance_result clean(fixture *f, size_t attempts,
                                         lc_error *e) {
  lc_pouch_maintenance_result result = {0};
  ok(lc_pouch_retention_run(store(f), "orders", attempts, &result, e), e);
  REQUIRE(!result.retention_failed_count);
  return result;
}
static void settled(fixture *f, lc_error *e) {
  lc_pouch_retention_work work = {0};
  ok(lc_pouch_retention_work_read(store(f), "orders", &work, e), e);
  if (work.present)
    fprintf(stderr, "unsettled phase=%d key=%s xid=%s [%s]\n", work.phase,
            work.key ? work.key : "", work.txn_id ? work.txn_id : "", scenario);
  REQUIRE(!work.present);
  lc_pouch_retention_work_cleanup(store(f), &work);
  parent(f, "order", 0, 0, e);
  attachments(f, "order", 0, e);
}
static void lifecycle(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto) {
        now(1000);
        setup(f, directory, native, sync, crypto, &e);
        write_parent(f, "order", 50, 1, &e);
        long old_fencing = f->fencing;
        parent(f, "order", 1, 1050, &e);
        attachments(f, "order", 1, &e);
        close_store(f);
        open_store(f, 0, &e);
        now(1049);
        REQUIRE(!clean(f, 16, &e).retention_deleted_state_count);
        parent(f, "order", 1, 1050, &e);
        now(1050);
        REQUIRE(clean(f, 16, &e).retention_deleted_state_count == 1);
        settled(f, &e);
        close_store(f);
        open_store(f, 0, &e);
        settled(f, &e);
        write_parent(f, "order", 0, 0, &e);
        REQUIRE(f->fencing > old_fencing);
        parent(f, "order", 1, 0, &e);
        close_store(f);
      }
  free(f);
  lc_error_cleanup(&e);
}
static void phase_failures(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto)
        for (int step = LC_POUCH_RETENTION_REF_CREATED;
             step <= LC_POUCH_RETENTION_REF_CLEARED + 1; ++step) {
          int phase = step <= LC_POUCH_RETENTION_REF_CLEARED
                          ? step
                          : LC_POUCH_RETENTION_ATTACHMENT_APPLIED;
          now(1000);
          setup(f, directory, native, sync, crypto, &e);
          write_parent(f, "order", 50, 1, &e);
          now(1050);
          phase_hits = 0;
          phase_fail = phase;
          lc_pouch_maintenance_result result = {0};
          int rc = lc_pouch_retention_run(store(f), "orders", 1, &result, &e);
          if (phase_hits != 1 || phase_fail || rc != LC_ERR_TRANSPORT)
            fprintf(stderr,
                    "phase native=%d sync=%d crypto=%d cut=%d hits=%lu "
                    "pending=%d rc=%d\n",
                    native, sync, crypto, phase, (unsigned long)phase_hits,
                    phase_fail, rc);
          REQUIRE(phase_hits == 1 && !phase_fail && rc == LC_ERR_TRANSPORT);
          REQUIRE(result.retention_failed_count == 1);
          clear(&e);
          lc_pouch_retention_work work = {0};
          ok(lc_pouch_retention_work_read(store(f), "orders", &work, &e), &e);
          if (work.present && work.phase != LC_POUCH_RETENTION_IDLE) {
            lc_acquire_req req;
            lc_lease *blocked = NULL;
            lc_acquire_req_init(&req);
            req.key = "order";
            req.owner = "replacement";
            req.ttl_seconds = 300;
            REQUIRE(f->client->acquire(f->client, &req, &blocked, &e) ==
                    LC_ERR_PENDING);
            REQUIRE(!blocked);
            clear(&e);
          }
          lc_pouch_retention_work_cleanup(store(f), &work);
          close_store(f);
          open_store(f, 0, &e);
          now(phase < LC_POUCH_RETENTION_DECIDED ? 1000 : 900);
          (void)clean(f, 16, &e);
          if (phase < LC_POUCH_RETENTION_DECIDED) {
            parent(f, "order", 1, 1050, &e);
            attachments(f, "order", 1, &e);
          } else
            settled(f, &e);
          now(1050);
          (void)clean(f, 16, &e);
          settled(f, &e);
          close_store(f);
        }
  free(f);
  lc_error_cleanup(&e);
}
static void lease_policy(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto) {
        now(1000);
        setup(f, directory, native, sync, crypto, &e);
        write_parent(f, "order", 50, 1, &e);
        long original = f->fencing;
        lc_lease *lease = acquire(f, "order", NULL, &e);
        REQUIRE(lease->fencing_token > original &&
                lease->retention_support == LC_STATE_RETENTION_SUPPORTED &&
                lease->has_retention_expires_at_unix &&
                lease->retention_expires_at_unix == 1050);
        lc_update_opts options = {0};
        options.has_retention_seconds = 1;
        options.retention_seconds = 60;
        payload(lease, &options, &e);
        REQUIRE(!lease->has_retention_expires_at_unix);
        parent(f, "order", 1, 1050, &e);
        now(1010);
        payload(lease, NULL, &e);
        lc_keepalive_req keep = {500};
        ok(lease->keepalive(lease, &keep, &e), &e);
        REQUIRE(lease->lease_expires_at_unix == 1510);
        now(1030);
        ok(lease->release(lease, NULL, &e), &e);
        parent(f, "order", 1, 1090, &e);
        lease = acquire(f, "order", NULL, &e);
        options.has_retention_seconds = 0;
        options.retention_seconds = -42;
        payload(lease, &options, &e);
        REQUIRE(lease->has_retention_expires_at_unix &&
                lease->retention_expires_at_unix == 1090);
        now(1040);
        ok(lease->release(lease, NULL, &e), &e);
        parent(f, "order", 1, 1090, &e);
        lease = acquire(f, "order", NULL, &e);
        lc_metadata_req metadata = {0};
        metadata.has_retention_seconds = 1;
        metadata.retention_seconds = -1;
        REQUIRE(lease->metadata(lease, &metadata, &e) == LC_ERR_INVALID);
        clear(&e);
        metadata.retention_seconds = 90;
        ok(lease->metadata(lease, &metadata, &e), &e);
        REQUIRE(!lease->has_retention_expires_at_unix);
        metadata.retention_seconds = 0;
        ok(lease->metadata(lease, &metadata, &e), &e);
        REQUIRE(lease->has_retention_expires_at_unix &&
                !lease->retention_expires_at_unix);
        payload(lease, NULL, &e);
        ok(lease->release(lease, NULL, &e), &e);
        parent(f, "order", 1, 0, &e);
        now(2000);
        REQUIRE(!clean(f, 16, &e).retention_deleted_state_count);
        parent(f, "order", 1, 0, &e);
        attachments(f, "order", 1, &e);
        close_store(f);
        open_store(f, 0, &e);
        parent(f, "order", 1, 0, &e);
        close_store(f);
      }
  free(f);
  lc_error_cleanup(&e);
}
static void shared_lease(const char *directory) {
  fixture *a = calloc(1, sizeof(*a)), *b = calloc(1, sizeof(*b));
  REQUIRE(a && b);
  shared_lease_fixture = 1;
  for (int sync = 0; sync <= 1; ++sync) {
    lc_error e = {0};
    lc_acquire_req req;
    lc_lease *owner = NULL, *successor = NULL;
    now(1000);
    setup(a, directory, 0, sync, sync ? 3 : 0, &e);
    strcpy(b->path, a->path);
    configure(b, 0, a->sync, a->crypto);
    open_store(b, 0, &e);
    /* Warm the peer before the first owner publishes a lock record. */
    parent(b, "order", 0, 0, &e);
    owner = acquire(a, "order", NULL, &e);
    payload(owner, NULL, &e);
    const long fence = owner->fencing_token;
    lc_acquire_req_init(&req);
    req.key = "order";
    req.owner = "peer";
    req.ttl_seconds = 300;
    REQUIRE(b->client->acquire(b->client, &req, &successor, &e) ==
                LC_ERR_INVALID &&
            !successor && e.message &&
            !strcmp(e.message, "pouch lease already held"));
    clear(&e);
    now(1310);
    ok(b->client->acquire(b->client, &req, &successor, &e), &e);
    REQUIRE(successor && successor->fencing_token > fence);
    producer stale = source("stale lease cannot publish");
    stale.pub.impl = &stale;
    REQUIRE(owner->update(owner, &stale.pub, NULL, &e) == LC_ERR_INVALID);
    REQUIRE(stale.calls == 0);
    clear(&e);
    owner->close(owner);
    payload(successor, NULL, &e);
    ok(successor->release(successor, NULL, &e), &e);
    parent(a, "order", 1, 0, &e);
    lc_client_close(b->client);
    b->client = NULL;
    close_store(a);
    open_store(a, 0, &e);
    parent(a, "order", 1, 0, &e);
    close_store(a);
    clear(&e);
  }
  shared_lease_fixture = 0;
  free(b);
  free(a);
}
static void protection(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto)
        for (int kind = 0; kind < 3; ++kind) {
          now(1000);
          setup(f, directory, native, sync, crypto, &e);
          write_parent(f, "order", 50, 1, &e);
          now(1040);
          char xid[LC_XID_STRING_SIZE];
          ok(lc_xid_new(xid, &e), &e);
          lc_lease *first = acquire(f, "order", kind == 1 ? xid : NULL, &e);
          strcpy(xid, first->txn_id);
          lc_lease *second = kind ? acquire(f, "other", xid, &e) : NULL;
          payload(first, NULL, &e);
          now(1400);
          lc_pouch_maintenance_result result = clean(f, 16, &e);
          REQUIRE(result.retention_expired_count == 1 &&
                  result.retention_deleted_state_count == (kind ? 0 : 1));
          if (kind) {
            parent(f, "order", 1, 1050, &e);
            attachments(f, "order", 1, &e);
            lc_txn_decision_req request;
            lc_txn_decision_res decided = {0};
            lc_txn_decision_req_init(&request);
            request.txn_id = xid;
            ok(f->client->txn_rollback(f->client, &request, &decided, &e), &e);
            lc_txn_decision_res_cleanup(&decided);
            second->close(second);
          }
          first->close(first);
          (void)clean(f, 16, &e);
          settled(f, &e);
          close_store(f);
        }
  free(f);
  lc_error_cleanup(&e);
}
static void fairness(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto) {
        now(1000);
        setup(f, directory, native, sync, crypto, &e);
        write_parent(f, "order", 50, 1, &e);
        write_parent(f, "other", 50, 0, &e);
        write_parent(f, "future", 1000, 0, &e);
        write_parent(f, "ordinary", 0, 0, &e);
        now(1040);
        lc_lease *held = acquire(f, "order", NULL, &e);
        now(1050);
        lc_pouch_maintenance_result result = clean(f, 1, &e);
        REQUIRE(result.retention_expired_count == 1 &&
                !result.retention_deleted_state_count);
        parent(f, "order", 1, 1050, &e);
        REQUIRE(clean(f, 1, &e).retention_deleted_state_count == 1);
        parent(f, "other", 0, 0, &e);
        parent(f, "order", 1, 1050, &e);
        ok(held->release(held, NULL, &e), &e);
        REQUIRE(clean(f, 1, &e).retention_deleted_state_count == 1);
        parent(f, "order", 0, 0, &e);
        attachments(f, "order", 0, &e);
        parent(f, "future", 1, 2000, &e);
        parent(f, "ordinary", 1, 0, &e);
        close_store(f);
        open_store(f, 0, &e);
        REQUIRE(!clean(f, 16, &e).retention_deleted_state_count);
        now(2000);
        REQUIRE(clean(f, 16, &e).retention_deleted_state_count == 1);
        settled(f, &e);
        parent(f, "ordinary", 1, 0, &e);
        close_store(f);
      }
  free(f);
  lc_error_cleanup(&e);
}
/* A producer and checking sink need only positions. Neither retains the value.
 * Hexadecimal hash blocks keep compression from collapsing the progress probe.
 */
#define LARGE_BYTES (512U * 1024U)
#define PRODUCER_PAUSE (256U * 1024U)
static unsigned char large_byte(size_t at) {
  static const char prefix[] = "{\"data\":\"";
  if (at < sizeof(prefix) - 1)
    return (unsigned char)prefix[at];
  if (at >= LARGE_BYTES - 2)
    return at == LARGE_BYTES - 2 ? '"' : '}';
  size_t position = at - (sizeof(prefix) - 1);
  uint32_t hash = (uint32_t)(position / 8) + UINT32_C(0x9e3779b9);
  hash ^= hash >> 16;
  hash *= UINT32_C(0x7feb352d);
  hash ^= hash >> 15;
  hash *= UINT32_C(0x846ca68b);
  hash ^= hash >> 16;
  return (unsigned char)"0123456789abcdef"[(hash >> (4 * (position % 8))) & 15];
}
typedef struct progress_io {
  int armed;
  size_t bytes;
} progress_io;
static ssize_t progress_read(void *context, int fd, void *bytes, size_t n,
                             off_t at) {
  (void)context;
  return pread(fd, bytes, n, at);
}
static int progress_sync(void *context, int fd) {
  (void)context;
  return fdatasync(fd);
}
static int progress_reserve(void *context, int fd, off_t at, off_t n) {
  (void)context;
  return fallocate(fd, 0, at, n);
}
static ssize_t progress_write(void *context, int fd, const void *bytes,
                              size_t n, off_t at) {
  progress_io *p = context;
  ssize_t result = pwrite(fd, bytes, n, at);
  if (result > 0 && n >= PS_PAGE &&
      __atomic_load_n(&p->armed, __ATOMIC_ACQUIRE)) {
    const unsigned char *data = bytes;
    size_t nonzero = 0;
    for (ssize_t i = 0; i < result; ++i)
      nonzero += data[i] != 0;
    if (nonzero > (size_t)result / 2)
      __atomic_add_fetch(&p->bytes, (size_t)result, __ATOMIC_RELAXED);
  }
  return result;
}
typedef struct large_producer {
  lc_source pub;
  size_t at;
  rendezvous gate;
  progress_io *progress;
  int paused;
} large_producer;
static size_t large_pull(lc_source *pub, void *bytes, size_t n, lc_error *e) {
  large_producer *p = pub->impl;
  (void)e;
  if (!p->at && p->progress) {
    __atomic_store_n(&p->progress->bytes, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&p->progress->armed, 1, __ATOMIC_RELEASE);
  }
  if (p->at == PRODUCER_PAUSE && !p->paused) {
    p->paused = 1;
    gate_signal(&p->gate, 0);
    gate_wait(&p->gate, 1);
  }
  if (n > 2048)
    n = 2048;
  if (n > LARGE_BYTES - p->at)
    n = LARGE_BYTES - p->at;
  if (p->at < PRODUCER_PAUSE && n > PRODUCER_PAUSE - p->at)
    n = PRODUCER_PAUSE - p->at;
  for (size_t i = 0; i < n; ++i)
    ((unsigned char *)bytes)[i] = large_byte(p->at++);
  return n;
}
static int large_check(lc_sink *sink, const void *bytes, size_t n,
                       lc_error *e) {
  size_t *at = sink->impl;
  (void)e;
  REQUIRE(n <= LARGE_BYTES - *at);
  for (size_t i = 0; i < n; ++i)
    REQUIRE(((const unsigned char *)bytes)[i] == large_byte((*at)++));
  return 1;
}
typedef struct concurrent_task {
  fixture *f;
  lc_lease *lease;
  large_producer *producer;
  rendezvous done;
} concurrent_task;
static void *concurrent_worker(void *context) {
  concurrent_task *task = context;
  lc_error e;
  lc_error_init(&e);
  if (!task->lease) {
    REQUIRE(clean(task->f, 1, &e).retention_deleted_state_count == 1);
  } else if (task->producer) {
    ok(task->lease->update(task->lease, &task->producer->pub, NULL, &e), &e);
  } else {
    payload(task->lease, NULL, &e);
    ok(task->lease->release(task->lease, NULL, &e), &e);
    task->lease = NULL;
  }
  lc_error_cleanup(&e);
  gate_signal(&task->done, 0);
  return NULL;
}
static lc_lease *independent_lease(fixture *f, const char *key, lc_error *e) {
  char xid[LC_XID_STRING_SIZE];
  int admitted = 0;
  /* Explicitly exclude documented stripe collisions with the held cleanup.
   * The test asserts independence, not parallelism within a striped lock. */
  for (int i = 0; i < 1024 && !admitted; ++i) {
    lc_pouch_txn_guard guard = {0};
    guard.fd = -1;
    ok(lc_xid_new(xid, e), e);
    int rc = lc_pouch_txn_guard_lock(store(f), xid, 0, &guard, e);
    REQUIRE(rc == LC_OK || rc == LC_ERR_PENDING);
    if (rc == LC_OK) {
      lc_pouch_txn_guard_release(&guard);
      admitted = 1;
    } else
      clear(e);
  }
  REQUIRE(admitted);
  lc_acquire_req req;
  lc_acquire_req_init(&req);
  req.ns = "independent";
  req.key = key;
  req.owner = "independent-writer";
  req.ttl_seconds = 300;
  req.txn_id = xid;
  lc_lease *lease = NULL;
  ok(f->client->acquire(f->client, &req, &lease, e), e);
  REQUIRE(lease);
  return lease;
}
static void concurrency(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto)
        for (int phase = LC_POUCH_RETENTION_STAGED;
             phase <= LC_POUCH_RETENTION_DECIDED; ++phase) {
          REQUIRE(snprintf(scenario, sizeof(scenario),
                           "concurrency native=%d sync=%d crypto=%d phase=%d",
                           native, sync, crypto,
                           phase) < (int)sizeof(scenario));
          progress_io progress = {0};
          ps_io io = {0};
          io.context = &progress;
          io.read = progress_read;
          io.write = progress_write;
          io.sync = progress_sync;
          io.reserve = progress_reserve;
          now(1000);
          setup_io(f, directory, native, sync, crypto, native ? NULL : &io, &e);
          write_parent(f, "order", 50, 1, &e);
          now(1050);
          rendezvous held;
          gate_init(&held);
          phase_gate = &held;
          gated_phase = phase;
          concurrent_task cleanup = {0}, peer = {0};
          cleanup.f = peer.f = f;
          gate_init(&cleanup.done);
          gate_init(&peer.done);
          pthread_t cleaner, writer;
          REQUIRE(!pthread_create(&cleaner, NULL, concurrent_worker, &cleanup));
          gate_wait(&held, 0);
          peer.lease = independent_lease(f, "small", &e);
          REQUIRE(!pthread_create(&writer, NULL, concurrent_worker, &peer));
          gate_wait(&peer.done, 0);
          REQUIRE(!pthread_join(writer, NULL));
          REQUIRE(!peer.lease);
          lc_pouch_state_read_result metadata = {0};
          ok(lc_pouch_state_read_metadata(store(f), "independent", "small",
                                          &metadata, &e),
             &e);
          REQUIRE(metadata.has_body);
          lc_pouch_state_read_result_cleanup(&store(f)->allocator, &metadata);
          gate_destroy(&peer.done);
          memset(&peer, 0, sizeof(peer));
          peer.f = f;
          peer.lease = independent_lease(f, "large", &e);
          large_producer producer = {0};
          producer.pub.read = large_pull;
          producer.pub.impl = &producer;
          producer.progress = native ? NULL : &progress;
          gate_init(&producer.gate);
          peer.producer = &producer;
          gate_init(&peer.done);
          REQUIRE(!pthread_create(&writer, NULL, concurrent_worker, &peer));
          gate_wait(&producer.gate, 0);
          REQUIRE(producer.at == PRODUCER_PAUSE);
          if (!native)
            REQUIRE(__atomic_load_n(&progress.bytes, __ATOMIC_RELAXED) >=
                    2 * PS_CHUNK);
          gate_signal(&held, 1);
          gate_wait(&cleanup.done, 0);
          REQUIRE(!pthread_join(cleaner, NULL));
          phase_gate = NULL;
          /* Cleanup, including the stage barrier, finished before producer EOF.
           */
          REQUIRE(producer.at == PRODUCER_PAUSE);
          settled(f, &e);
          gate_signal(&producer.gate, 1);
          gate_wait(&peer.done, 0);
          REQUIRE(!pthread_join(writer, NULL));
          REQUIRE(producer.at == LARGE_BYTES);
          ok(peer.lease->release(peer.lease, NULL, &e), &e);
          size_t at = 0;
          lc_sink sink = {large_check, NULL, &at};
          lc_get_opts opts;
          lc_get_opts_init(&opts);
          lc_get_res got = {0};
          ok(f->client->get_in_namespace(f->client, "independent", "large",
                                         &opts, &sink, &got, &e),
             &e);
          REQUIRE(!got.no_content && at == LARGE_BYTES);
          lc_get_res_cleanup(&got);
          close_store(f);
          gate_destroy(&producer.gate);
          gate_destroy(&cleanup.done);
          gate_destroy(&peer.done);
          gate_destroy(&held);
        }
  free(f);
  lc_error_cleanup(&e);
}
static int lose_decision_ack(void *context, lc_error *e) {
  (void)context;
  return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                      "injected lost decision acknowledgement", NULL, NULL,
                      "test");
}
static void coordinated(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int native = 0; native < 2; ++native)
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto < 4; ++crypto)
        for (int mode = 0; mode < 4; ++mode) {
          REQUIRE(snprintf(scenario, sizeof(scenario),
                           "coordinated native=%d sync=%d crypto=%d mode=%d",
                           native, sync, crypto, mode) < (int)sizeof(scenario));
          now(1000);
          setup(f, directory, native, sync, crypto, &e);
          write_parent(f, "order", 50, 1, &e);
          write_parent(f, "other", 60, 1, &e);
          char xid[LC_XID_STRING_SIZE];
          ok(lc_xid_new(xid, &e), &e);
          lc_txn_participant participants[] = {{"orders", "order", NULL},
                                               {"orders", "other", NULL}};
          lc_txn_decision_req decision;
          lc_txn_decision_req_init(&decision);
          decision.txn_id = xid;
          decision.participants = participants;
          decision.participant_count = 2;
          lc_txn_decision_res result = {0};
          ok(f->client->txn_prepare(f->client, &decision, &result, &e), &e);
          REQUIRE(result.state && !strcmp(result.state, "prepare"));
          lc_txn_decision_res_cleanup(&result);
          lc_lease *leases[2];
          for (int i = 0; i < 2; ++i) {
            leases[i] = acquire(f, participants[i].key, xid, &e);
            lc_update_opts options = {0};
            options.has_retention_seconds = 1;
            options.retention_seconds = 100 * (i + 1);
            payload(leases[i], &options, &e);
            lc_attach_req attach;
            lc_attach_req_init(&attach);
            attach.name = "staged-receipt";
            attach.content_type = "text/plain";
            producer p = source("transactional attachment");
            p.pub.impl = &p;
            lc_attach_res attached = {0};
            ok(leases[i]->attach(leases[i], &attach, &p.pub, &attached, &e),
               &e);
            lc_attach_res_cleanup(&attached);
          }
          parent(f, "order", 1, 1050, &e);
          parent(f, "other", 1, 1060, &e);
          attachments(f, "order", 1, &e);
          if (mode == 0 || mode == 3) {
            now(1010);
            ok(leases[0]->release(leases[0], NULL, &e), &e);
            leases[0] = NULL;
            parent(f, "order", 1, 1050, &e);
            parent(f, "other", 1, 1060, &e);
            now(1040);
            lc_release_req release = {0};
            release.rollback = mode == 3;
            ok(leases[1]->release(leases[1], &release, &e), &e);
            leases[1] = NULL;
          } else {
            now(1040);
            lc_pouch_test_after_txn_decision_hook = lose_decision_ack;
            REQUIRE(f->client->txn_commit(f->client, &decision, &result, &e) ==
                    LC_ERR_TRANSPORT);
            lc_pouch_test_after_txn_decision_hook = NULL;
            lc_txn_decision_res_cleanup(&result);
            clear(&e);
            parent(f, "order", 1, 1050, &e);
            parent(f, "other", 1, 1060, &e);
            leases[0]->close(leases[0]);
            leases[1]->close(leases[1]);
          }
          lc_txn_replay_req replay;
          lc_txn_replay_req_init(&replay);
          replay.txn_id = xid;
          lc_txn_replay_res replayed = {0};
          if (mode == 1 || mode == 2) {
            close_store(f);
            now(mode == 2 ? 900 : 1100);
            open_store(f, 0, &e);
            /* Existing native-only startup policy applies and retires terminal
             * journals. Retention below must use the original decision time. */
            lc_pouch_state_read_result journal = {0};
            ok(lc_pouch_state_read_metadata(store(f), ".txns", xid, &journal,
                                            &e),
               &e);
            REQUIRE(!journal.found);
            lc_pouch_state_read_result_cleanup(&store(f)->allocator, &journal);
          } else {
            ok(f->client->txn_replay(f->client, &replay, &replayed, &e), &e);
            REQUIRE(replayed.state &&
                    !strcmp(replayed.state, mode == 3 ? "rollback" : "commit"));
            lc_txn_replay_res_cleanup(&replayed);
          }
          parent(f, "order", 1, mode == 3 ? 1050 : 1140, &e);
          parent(f, "other", 1, mode == 3 ? 1060 : 1240, &e);
          attachments(f, "order", mode == 3 ? 1 : 2, &e);
          attachments(f, "other", mode == 3 ? 1 : 2, &e);
          now(2000);
          REQUIRE(clean(f, 16, &e).retention_deleted_state_count == 2);
          settled(f, &e);
          parent(f, "other", 0, 0, &e);
          attachments(f, "other", 0, &e);
          /* Expiry owns only its cleanup journal. Startup recovery has its
           * separate existing authority to retire native-only outcomes. */
          if (mode == 0 || mode == 3) {
            ok(f->client->txn_replay(f->client, &replay, &replayed, &e), &e);
            REQUIRE(replayed.state &&
                    !strcmp(replayed.state, mode == 3 ? "rollback" : "commit"));
            lc_txn_replay_res_cleanup(&replayed);
          }
          close_store(f);
          open_store(f, 0, &e);
          lc_pouch_state_read_result journal = {0};
          ok(lc_pouch_state_read_metadata(store(f), ".txns", xid, &journal, &e),
             &e);
          REQUIRE(!journal.found);
          lc_pouch_state_read_result_cleanup(&store(f)->allocator, &journal);
          settled(f, &e);
          close_store(f);
        }
  free(f);
  lc_error_cleanup(&e);
}
typedef struct measurement {
  double wall, cpu, staged_decision;
  size_t syncs;
} measurement;
static measurement measure_begin(void) {
  measurement m = {0};
  phase_elapsed = 0;
  __atomic_store_n(&measured_syncs, 0, __ATOMIC_RELAXED);
  m.cpu = real_clock(CLOCK_PROCESS_CPUTIME_ID);
  m.wall = real_clock(CLOCK_MONOTONIC);
  __atomic_store_n(&measure_enabled, 1, __ATOMIC_RELEASE);
  return m;
}
static void measure_end(measurement *m) {
  __atomic_store_n(&measure_enabled, 0, __ATOMIC_RELEASE);
  m->wall = 1000 * (real_clock(CLOCK_MONOTONIC) - m->wall);
  m->cpu = 1000 * (real_clock(CLOCK_PROCESS_CPUTIME_ID) - m->cpu);
  m->syncs = __atomic_load_n(&measured_syncs, __ATOMIC_RELAXED);
  m->staged_decision = 1000 * phase_elapsed;
}
static void compare(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  benchmark_normal = 1;
  for (int sample = 0; sample < 5; ++sample)
    for (int profile = 0; profile < 16; ++profile)
      for (int engine = 0; engine < 2; ++engine) {
        int selected = sample & 1 ? 15 - profile : profile;
        int crypto = selected % 4, sync = (selected / 4) % 2;
        int cold = selected / 8;
        int native = sample & 1 ? 1 - engine : engine;
        now(1000);
        setup(f, directory, native, sync, crypto, &e);
        measurement save = measure_begin();
        for (int i = 0; i < 8; ++i) {
          char key[32];
          REQUIRE(snprintf(key, sizeof(key), "order-%d", i) < (int)sizeof(key));
          write_parent(f, key, 50, 1, &e);
        }
        measure_end(&save);
        measurement opened = {0};
        if (cold) {
          close_store(f);
          opened = measure_begin();
          open_store(f, 0, &e);
          measure_end(&opened);
        }
        now(1050);
        measurement expired = measure_begin();
        REQUIRE(clean(f, 16, &e).retention_deleted_state_count == 8);
        measure_end(&expired);
        for (int i = 0; i < 8; ++i) {
          char key[32];
          REQUIRE(snprintf(key, sizeof(key), "order-%d", i) < (int)sizeof(key));
          parent(f, key, 0, 0, &e);
          attachments(f, key, 0, &e);
        }
        lc_pouch_retention_work work = {0};
        ok(lc_pouch_retention_work_read(store(f), "orders", &work, &e), &e);
        REQUIRE(!work.present);
        lc_pouch_retention_work_cleanup(store(f), &work);
        printf("lease-compare sample=%d native=%d sync=%d crypto=%d cold=%d "
               "records=8 save_ms=%.6f save_cpu_ms=%.6f save_syncs=%lu "
               "open_ms=%.6f open_cpu_ms=%.6f open_syncs=%lu "
               "expiry_ms=%.6f expiry_cpu_ms=%.6f expiry_syncs=%lu "
               "staged_decision_ms=%.6f verified=1\n",
               sample, native, sync, crypto, cold, save.wall, save.cpu,
               (unsigned long)save.syncs, opened.wall, opened.cpu,
               (unsigned long)opened.syncs, expired.wall, expired.cpu,
               (unsigned long)expired.syncs, expired.staged_decision);
        fflush(stdout);
        close_store(f);
        retire(f);
      }
  benchmark_normal = 0;
  free(f);
  lc_error_cleanup(&e);
}
static size_t allocation_trial(const char *directory, int native, int sync,
                               int crypto, int cold, int no_error, size_t cut) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  now(1000);
  setup(f, directory, native, sync, crypto, &e);
  write_parent(f, "order", 50, 1, &e);
  if (cold) {
    close_store(f);
    open_store(f, 0, &e);
  }
  now(1050);
  allocation_calls = 0;
  allocation_self = allocation_other = 0;
  allocation_thread = pthread_self();
  allocation_fail = cut;
  allocation_fired = 0;
  __atomic_store_n(&allocation_armed, 1, __ATOMIC_RELEASE);
  lc_pouch_maintenance_result result = {0};
  int rc = lc_pouch_retention_run(store(f), "orders", 1, &result,
                                  no_error ? NULL : &e);
  __atomic_store_n(&allocation_armed, 0, __ATOMIC_RELEASE);
  size_t count = allocation_calls;
  if (!cut)
    printf("expiry allocation measure total=%lu calling=%lu other=%lu\n",
           (unsigned long)count, (unsigned long)allocation_self,
           (unsigned long)allocation_other);
  if (rc != LC_OK && rc != LC_ERR_NOMEM)
    fprintf(stderr,
            "allocation native=%d sync=%d crypto=%d cold=%d nullable=%d "
            "cut=%lu calls=%lu rc=%d %s\n",
            native, sync, crypto, cold, no_error, (unsigned long)cut,
            (unsigned long)count, rc, e.message ? e.message : "?");
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM);
  if (cut && !allocation_fired)
    fprintf(stderr,
            "allocation site missing: native=%d sync=%d crypto=%d cold=%d "
            "nullable=%d cut=%lu calls=%lu\n",
            native, sync, crypto, cold, no_error, (unsigned long)cut,
            (unsigned long)count);
  REQUIRE(cut ? allocation_fired : rc == LC_OK);
  clear(&e);
  (void)clean(f, 16, &e);
  settled(f, &e);
  close_store(f);
  open_store(f, 0, &e);
  (void)clean(f, 16, &e);
  settled(f, &e);
  close_store(f);
  retire(f);
  free(f);
  lc_error_cleanup(&e);
  return count;
}
static void allocation_failures(const char *directory, int crypto, int native,
                                int sync, int cold, int no_error,
                                int diagnostic, size_t selected) {
  size_t count = allocation_trial(directory, native, sync, crypto, cold, 0, 0);
  REQUIRE(count);
  printf("expiry allocation native=%d sync=%d crypto=%d cold=%d nullable=%d "
         "sites=%lu\n",
         native, sync, crypto, cold, no_error, (unsigned long)count);
  fflush(stdout);
  if (diagnostic) {
    size_t observed = allocation_trial(directory, native, sync, crypto, cold,
                                       no_error, selected);
    printf(
        "expiry allocation diagnostic selected=%lu observed=%lu baseline=%lu\n",
        (unsigned long)selected, (unsigned long)observed, (unsigned long)count);
    return;
  }
  for (size_t cut = 1; cut <= count; ++cut) {
    (void)allocation_trial(directory, native, sync, crypto, cold, no_error,
                           cut);
    if (!(cut % 256)) {
      printf("expiry allocation completed=%lu/%lu\n", (unsigned long)cut,
             (unsigned long)count);
      fflush(stdout);
    }
  }
  printf("expiry allocation verified cuts=%lu nullable=%d, both retry paths\n",
         (unsigned long)count, no_error);
}
static void process_cuts(const char *directory, int crypto) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  /* Exclusive multifile HA fencing after _exit intentionally waits for its
   * writer lease. Its shared-writer crash suite remains the native reference;
   * this probe establishes the admitted exclusive container's process recovery.
   */
  for (int sync = 0; sync < 2; ++sync)
    for (int step = LC_POUCH_RETENTION_REF_CREATED;
         step <= LC_POUCH_RETENTION_REF_CLEARED + 1; ++step) {
      int phase = step <= LC_POUCH_RETENTION_REF_CLEARED
                      ? step
                      : LC_POUCH_RETENTION_ATTACHMENT_APPLIED;
      now(1000);
      setup(f, directory, 0, sync, crypto, &e);
      write_parent(f, "order", 50, 1, &e);
      long original = f->fencing;
      close_store(f);
      now(1050);
      char sync_arg[2] = {(char)('0' + sync), 0};
      char crypto_arg[2] = {(char)('0' + crypto), 0};
      char phase_arg[16];
      REQUIRE(snprintf(phase_arg, sizeof(phase_arg), "%d", phase) <
              (int)sizeof(phase_arg));
      char *args[] = {(char *)executable_path,
                      "--phase-child",
                      f->path,
                      sync_arg,
                      crypto_arg,
                      phase_arg,
                      crypto_key,
                      NULL};
      pid_t child = fork();
      REQUIRE(child >= 0);
      if (!child) {
        execv(executable_path, args);
        _exit(3);
      }
      int status;
      REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
              WEXITSTATUS(status) == 70 + phase);
      now(phase < LC_POUCH_RETENTION_DECIDED ? 1000 : 900);
      open_store(f, 0, &e);
      (void)clean(f, 16, &e);
      if (phase < LC_POUCH_RETENTION_DECIDED) {
        parent(f, "order", 1, 1050, &e);
        attachments(f, "order", 1, &e);
      } else
        settled(f, &e);
      now(1050);
      (void)clean(f, 16, &e);
      settled(f, &e);
      lc_lease *replacement = acquire(f, "order", NULL, &e);
      REQUIRE(replacement->fencing_token > original);
      ok(replacement->release(replacement, NULL, &e), &e);
      close_store(f);
      retire(f);
    }
  free(f);
  lc_error_cleanup(&e);
}
/* This image is a durability oracle for the syscall seam, not an application
 * spool. Only successful barriers and explicitly selected failed writes enter
 * stable. Restore after closing every owner, discarding all other dirty bytes.
 */
typedef struct disk_model {
  unsigned char *stable, *dirty, *scratch;
  size_t calls, cut;
  int armed, fired, mode, freeze;
  unsigned long cut_phases;
} disk_model;
static int disk_cut(disk_model *m) {
  if (m->armed && ++m->calls == m->cut) {
    m->fired = 1;
    m->cut_phases = __atomic_load_n(&phase_mask, __ATOMIC_RELAXED);
    return 1;
  }
  return 0;
}
static ssize_t disk_read(void *context, int fd, void *out, size_t n, off_t at) {
  disk_model *m = context;
  if (disk_cut(m)) {
    errno = EIO;
    return -1;
  }
  return pread(fd, out, n, at);
}
static ssize_t disk_write(void *context, int fd, const void *bytes, size_t n,
                          off_t at) {
  disk_model *m = context;
  REQUIRE(at >= 0 && (uint64_t)at + n <= PS_DEFAULT_CAPACITY);
  int fail = disk_cut(m);
  size_t want = !fail || m->mode == 3 ? n : m->mode ? n / 2 : 0;
  if (want) {
    REQUIRE(pwrite(fd, bytes, want, at) == (ssize_t)want);
    for (size_t page = (size_t)at / PS_PAGE;
         page <= ((size_t)at + want - 1) / PS_PAGE; ++page)
      m->dirty[page] = 1;
    if (fail && m->mode >= 2 && !m->freeze)
      memcpy(m->stable + at, bytes, want);
  }
  if (fail) {
    errno = EIO;
    return -1;
  }
  return (ssize_t)want;
}
static int disk_sync(void *context, int fd) {
  disk_model *m = context;
  int fail = disk_cut(m);
  if (!fail || m->mode >= 2) {
    REQUIRE(!fdatasync(fd));
    if (!m->freeze)
      for (size_t page = 0; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
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
static int disk_reserve(void *context, int fd, off_t at, off_t n) {
  disk_model *m = context;
  if (disk_cut(m)) {
    errno = EIO;
    return -1;
  }
  return fallocate(fd, 0, at, n);
}
static void restore_disk(disk_model *m, const char *path) {
  int fd = open(path, O_RDWR);
  REQUIRE(fd >= 0);
  for (size_t page = 0; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
    if (m->dirty[page])
      REQUIRE(pwrite(fd, m->stable + page * PS_PAGE, PS_PAGE,
                     (off_t)(page * PS_PAGE)) == PS_PAGE);
  REQUIRE(!fdatasync(fd) && !close(fd));
  memset(m->dirty, 0, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
  m->freeze = 0;
}
static int selected_commit(fixture *f, lc_error *e) {
  lc_pouch_retention_work work = {0};
  lc_pouch_state_read_result parent_state = {0}, journal = {0};
  ok(lc_pouch_retention_work_read(store(f), "orders", &work, e), e);
  ok(lc_pouch_state_read_metadata(store(f), "orders", "order", &parent_state,
                                  e),
     e);
  int committed = !parent_state.has_body;
  lc_pouch_state_read_result_cleanup(&store(f)->allocator, &parent_state);
  if (work.present && work.phase == LC_POUCH_RETENTION_FINISHED)
    committed = 1;
  else if (work.present && work.phase == LC_POUCH_RETENTION_ACTIVE) {
    ok(lc_pouch_state_read(store(f), ".txns", work.txn_id, &journal, e), e);
    committed = 0;
    if (journal.has_body) {
      REQUIRE(journal.bytes >= 6 && journal.bytes <= 4096);
      unsigned char *bytes = malloc((size_t)journal.bytes);
      REQUIRE(bytes && journal.body);
      size_t at = 0;
      while (at < journal.bytes) {
        size_t n = journal.body->read(journal.body, bytes + at,
                                      (size_t)journal.bytes - at, e);
        REQUIRE(n && e->code == LC_OK);
        at += n;
      }
      unsigned char end;
      REQUIRE(!journal.body->read(journal.body, &end, 1, e) &&
              e->code == LC_OK);
      REQUIRE(!memcmp(bytes, "LPT3", 4));
      size_t length = bytes[4] | ((size_t)bytes[5] << 8);
      REQUIRE(length <= journal.bytes - 6);
      committed = length == 6 && !memcmp(bytes + 6, "commit", 6);
      REQUIRE(committed || (length == 7 && !memcmp(bytes + 6, "prepare", 7)) ||
              (length == 8 && !memcmp(bytes + 6, "rollback", 8)));
      if (committed) {
        uint64_t stamp = 0;
        for (size_t i = 0; i < 8; ++i)
          stamp |= (uint64_t)bytes[journal.bytes - 8 + i] << (8 * i);
        REQUIRE(stamp == 1050);
      }
      free(bytes);
    } else
      REQUIRE(!work.fencing_token);
    lc_pouch_state_read_result_cleanup(&store(f)->allocator, &journal);
  }
  lc_pouch_retention_work_cleanup(store(f), &work);
  return committed;
}
static size_t io_trial(const char *directory, int sync, int crypto, int mode,
                       int cold, size_t cut) {
  disk_model m = {0};
  m.mode = mode;
  m.stable = calloc(1, (size_t)PS_DEFAULT_CAPACITY);
  m.dirty = calloc(1, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
  m.scratch = malloc(PS_PAGE);
  REQUIRE(m.stable && m.dirty && m.scratch);
  ps_io io = {&m, disk_read, disk_write, disk_sync, disk_reserve, NULL};
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  now(1000);
  setup_io(f, directory, 0, sync, crypto, &io, &e);
  write_parent(f, "order", 50, 1, &e);
  long original = f->fencing;
  ps_space *space = ps_root_space(ps_pouch_state_root(store(f)));
  /* The pre-existing input is durable even for a NoSync root. */
  ok(ps_flush(space, &e), &e);
  if (cold) {
    close_store(f);
    open_store(f, 0, &e);
    space = ps_root_space(ps_pouch_state_root(store(f)));
  }
  int baseline_fd = open(f->path, O_RDONLY);
  REQUIRE(baseline_fd >= 0);
  for (size_t at = 0; at < PS_DEFAULT_CAPACITY; at += PS_PAGE) {
    REQUIRE(pread(baseline_fd, m.scratch, PS_PAGE, (off_t)at) == PS_PAGE);
    REQUIRE(!memcmp(m.stable + at, m.scratch, PS_PAGE));
  }
  REQUIRE(!close(baseline_fd));
  now(1050);
  phase_mask = 0;
  m.calls = 0;
  m.cut = cut;
  m.armed = 1;
  lc_pouch_maintenance_result result = {0};
  int rc = lc_pouch_retention_run(store(f), "orders", 1, &result, &e);
  m.armed = 0;
  size_t count = m.calls;
  if (cut && !m.fired)
    fprintf(stderr,
            "missing I/O site sync=%d crypto=%d mode=%d cut=%lu calls=%lu\n",
            sync, crypto, mode, (unsigned long)cut, (unsigned long)count);
  REQUIRE(cut ? m.fired : rc == LC_OK);
  REQUIRE(rc == LC_OK || rc == LC_ERR_TRANSPORT);
  clear(&e);
  if (ps_check(space, NULL) == LC_ERR_TRANSPORT) {
    m.calls = 0;
    m.cut = SIZE_MAX;
    m.armed = 1;
    REQUIRE(lc_pouch_retention_run(store(f), "orders", 1, NULL, &e) ==
            LC_ERR_TRANSPORT);
    m.armed = 0;
    REQUIRE(!m.calls);
    clear(&e);
  }
  /* Close may drain resources, but cannot improve the captured crash image. */
  m.freeze = 1;
  close_store(f);
  restore_disk(&m, f->path);
  now(1000);
  open_store(f, 0, &e);
  int committed = selected_commit(f, &e);
  REQUIRE(snprintf(scenario, sizeof(scenario),
                   "I/O sync=%d crypto=%d mode=%d cold=%d cut=%lu rc=%d "
                   "committed=%d phases=%lx",
                   sync, crypto, mode, cold, (unsigned long)cut, rc, committed,
                   m.cut_phases) < (int)sizeof(scenario));
  if (rc == LC_OK)
    REQUIRE(committed);
  now(1000);
  (void)clean(f, 16, &e);
  if (committed)
    settled(f, &e);
  else {
    parent(f, "order", 1, 1050, &e);
    attachments(f, "order", 1, &e);
  }
  now(1050);
  (void)clean(f, 16, &e);
  settled(f, &e);
  lc_lease *replacement = acquire(f, "order", NULL, &e);
  REQUIRE(replacement->fencing_token > original);
  ok(replacement->release(replacement, NULL, &e), &e);
  close_store(f);
  retire(f);
  free(f);
  free(m.stable);
  free(m.dirty);
  free(m.scratch);
  lc_error_cleanup(&e);
  return count;
}
static void io_failures(const char *directory, int crypto, int sync,
                        int diagnostic, size_t selected, int selected_mode,
                        int selected_cold) {
  if (diagnostic) {
    (void)io_trial(directory, sync, crypto, selected_mode, selected_cold,
                   selected);
    return;
  }
  size_t total = 0;
  for (int cold = 0; cold < 2; ++cold)
    for (int mode = 0; mode < 4; ++mode) {
      size_t count = io_trial(directory, sync, crypto, mode, cold, 0);
      REQUIRE(count);
      for (size_t cut = 1; cut <= count; ++cut) {
        (void)io_trial(directory, sync, crypto, mode, cold, cut);
        ++total;
      }
    }
  printf("expiry I/O crypto=%d sync=%d verified cuts=%lu (failure, lost torn, "
         "persisted torn, lost acknowledgement)\n",
         crypto, sync, (unsigned long)total);
}
static void artifacts(const char *directory) {
  DIR *dir = opendir(directory);
  struct dirent *entry;
  unsigned char *seen = calloc(roots ? roots : 1, 1);
  size_t count = 0;
  REQUIRE(dir && seen);
  while ((entry = readdir(dir))) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    char *end;
    REQUIRE(!strncmp(entry->d_name, "root-", 5));
    unsigned long n = strtoul(entry->d_name + 5, &end, 10);
    REQUIRE(!*end && n < roots && !seen[n]);
    seen[n] = 1;
    ++count;
  }
  REQUIRE(count == roots && !closedir(dir));
  free(seen);
}
#include "lease_capacity_cases.h"
#include "shared_queue_cases.h"
typedef struct run {
  const char *directory, *mode;
  int native, sync, cold, no_error;
  int diagnostic;
  size_t selected;
  int io_mode;
} run;
static void *worker(void *context) {
  run *r = context;
  if (!strcmp(r->mode, "lifecycle"))
    lifecycle(r->directory);
  else if (!strcmp(r->mode, "phases"))
    phase_failures(r->directory);
  else if (!strcmp(r->mode, "policy"))
    lease_policy(r->directory);
  else if (!strcmp(r->mode, "shared-authority"))
    shared_lease(r->directory);
  else if (!strcmp(r->mode, "shared-queue-delivery"))
    shared_queue_delivery(r->directory);
  else if (!strcmp(r->mode, "shared-queue-transaction"))
    shared_queue_transaction(r->directory);
  else if (!strcmp(r->mode, "protection"))
    protection(r->directory);
  else if (!strcmp(r->mode, "fairness"))
    fairness(r->directory);
  else if (!strcmp(r->mode, "concurrency"))
    concurrency(r->directory);
  else if (!strcmp(r->mode, "coordinated"))
    coordinated(r->directory);
  else if (!strcmp(r->mode, "compare"))
    compare(r->directory);
  else if (!strcmp(r->mode, "capacity-probe"))
    lease_capacity_case(r->directory, 0, 0, 0, 0, 0, 0);
  else if (!strcmp(r->mode, "capacity-tail-probe"))
    lease_capacity_case(r->directory, 0, 0, 1, 16, LC_POUCH_RETENTION_STAGED,
                        1);
  else if (!strcmp(r->mode, "capacity-completion"))
    lease_capacity_completion(r->directory);
  else if (!strcmp(r->mode, "attachment-windows"))
    lease_attachment_windows(r->directory);
  else if (!strcmp(r->mode, "capacity-uncontended")) {
    for (int sync = 0; sync < 2; ++sync)
      for (int crypto = 0; crypto <= 3; crypto += 3)
        lease_capacity_case(r->directory, sync, crypto, 0, 1, -1, 1);
  } else if (!strcmp(r->mode, "capacity-profile") ||
             !strcmp(r->mode, "capacity-required"))
    lease_capacity_profile(r->directory, !strcmp(r->mode, "capacity-required"));
  else if (!strncmp(r->mode, "allocation-", 11) && strlen(r->mode) == 12 &&
           r->mode[11] >= '0' && r->mode[11] <= '3')
    allocation_failures(r->directory, r->mode[11] - '0', r->native, r->sync,
                        r->cold, r->no_error, r->diagnostic, r->selected);
  else if (!strncmp(r->mode, "process-", 8) && strlen(r->mode) == 9 &&
           r->mode[8] >= '0' && r->mode[8] <= '3')
    process_cuts(r->directory, r->mode[8] - '0');
  else if (!strncmp(r->mode, "io-", 3) && strlen(r->mode) == 6 &&
           r->mode[3] >= '0' && r->mode[3] <= '3' && r->mode[4] == '-' &&
           r->mode[5] >= '0' && r->mode[5] <= '1')
    io_failures(r->directory, r->mode[3] - '0', r->mode[5] - '0', r->diagnostic,
                r->selected, r->io_mode, r->cold);
  else
    REQUIRE(0);
  REQUIRE(!live);
  artifacts(r->directory);
  return NULL;
}
typedef struct child_run {
  fixture *f;
  int phase;
} child_run;
static void *phase_child(void *context) {
  child_run *r = context;
  lc_error e;
  lc_error_init(&e);
  now(1050);
  open_store(r->f, 0, &e);
  phase_crash = r->phase;
  lc_pouch_maintenance_result result = {0};
  (void)lc_pouch_retention_run(store(r->f), "orders", 16, &result, &e);
  return NULL;
}
int main(int argc, char **argv) {
  executable_path = argv[0];
  lc_pouch_test_queue_clock_gettime = logical_clock;
  lc_pouch_test_retention_hook = note_phase;
  if (argc == 7 && !strcmp(argv[1], "--phase-child")) {
    fixture *f = calloc(1, sizeof(*f));
    REQUIRE(f);
    REQUIRE(snprintf(f->path, sizeof(f->path), "%s", argv[2]) <
            (int)sizeof(f->path));
    crypto_key = argv[6];
    configure(f, 0, atoi(argv[3]), atoi(argv[4]));
    child_run r = {f, atoi(argv[5])};
    pthread_t thread;
    REQUIRE(!pthread_create(&thread, NULL, phase_child, &r));
    REQUIRE(!pthread_join(thread, NULL));
    return 3;
  }
  REQUIRE(argc == 3 || argc == 5 || argc == 6 || argc == 7 || argc == 8);
  char prefix[1100], pattern[1200], directory[1200];
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-lease-", argv[1]) <
          (int)sizeof(prefix));
  REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) <
          (int)sizeof(pattern));
  REQUIRE(lc_test_tmp_mkdtemp(pattern, directory, sizeof(directory), prefix));
  lc_error e;
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&crypto_key, &e), &e);
  run r = {directory, argv[2]};
  if (!strncmp(r.mode, "allocation-", 11)) {
    REQUIRE(argc == 7 || argc == 8);
    for (int i = 3; i < 7; ++i)
      REQUIRE(strlen(argv[i]) == 1 && argv[i][0] >= '0' && argv[i][0] <= '1');
    r.native = argv[3][0] - '0';
    r.sync = argv[4][0] - '0';
    r.cold = argv[5][0] - '0';
    r.no_error = argv[6][0] - '0';
    if (argc == 8) {
      char *end;
      r.selected = strtoul(argv[7], &end, 10);
      REQUIRE(end != argv[7] && !*end);
      r.diagnostic = 1;
    }
  } else if (!strncmp(r.mode, "io-", 3) && (argc == 5 || argc == 6)) {
    char *end;
    r.selected = strtoul(argv[3], &end, 10);
    REQUIRE(end != argv[3] && !*end && strlen(argv[4]) == 1 &&
            argv[4][0] >= '0' && argv[4][0] <= '3');
    r.io_mode = argv[4][0] - '0';
    if (argc == 6) {
      REQUIRE(strlen(argv[5]) == 1 && argv[5][0] >= '0' && argv[5][0] <= '1');
      r.cold = argv[5][0] - '0';
    }
    r.diagnostic = 1;
  } else
    REQUIRE(argc == 3);
  pthread_t thread;
  REQUIRE(!pthread_create(&thread, NULL, worker, &r));
  REQUIRE(!pthread_join(thread, NULL));
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, prefix);
  printf("Pouch leased expiry: %lu checks, %lu cases, %lu retained roots\n",
         (unsigned long)checks, (unsigned long)cases, (unsigned long)roots);
  return 0;
}
