#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "collection_segment.h"
#include "lc_collection_internal.h"
#include "owned.h"
#include "pouch.h"
#include "pouch_internal.h"
#include "root_internal.h"
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

static size_t checks, roots;
static char *key;
static char scenario[192];
static int diagnostic;
static int allocation_armed, allocation_fired;
static size_t allocation_calls, allocation_fail;
static pthread_t allocation_thread;
static int allocation_cut(void) {
  if (!__atomic_load_n(&allocation_armed, __ATOMIC_ACQUIRE) ||
      !pthread_equal(pthread_self(), allocation_thread))
    return 0;
  if (++allocation_calls == allocation_fail) {
    allocation_fired = 1;
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
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *s) {
  return allocation_cut() ? NULL : __real_lc_strdup_with_allocator(a, s);
}
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1U, __ATOMIC_RELAXED);                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static void ok(int rc, lc_error *e) {
  if (rc)
    fprintf(stderr, "collection SDK %d: %s [%s] %s\n", rc,
            e->message ? e->message : "?", e->server_code ? e->server_code : "",
            scenario);
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
typedef struct fixture {
  char path[1400], endpoint[1700];
  const char *endpoints[1];
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *client;
  lc_collection *collection, *peer;
  lc_error error;
  const ps_io *io;
} fixture;
static void open_client(fixture *f, int create) {
  ps_pouch_client_options physical = {0};
  physical.io = f->io;
  ok(ps_pouch_client_open(&f->config, create, f->io ? &physical : NULL,
                          &f->client, &f->error),
     &f->error);
}
static void open_collection(fixture *f, int create, lc_collection **out) {
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.name = "events/\303\245";
  ok(create ? lc_client_create_collection(f->client, &config, out, &f->error)
            : lc_client_open_collection(f->client, &config, out, &f->error),
     &f->error);
}
static int unexpected_collection(void *context, const lc_collection_info *info,
                                 lc_error *e) {
  (void)context;
  (void)info;
  return lc_error_set(e, LC_ERR_PROTOCOL, 0L,
                      "a missing namespace must have an empty collection list",
                      NULL, NULL, "test");
}
static void setup_io(fixture *f, const char *directory, int codec, int sync,
                     int rotate, const ps_io *io) {
  memset(f, 0, sizeof(*f));
  lc_error_init(&f->error);
  f->io = io;
  REQUIRE(snprintf(f->path, sizeof(f->path), "%s/root-%zu", directory,
                   roots++) > 0);
  REQUIRE(snprintf(f->endpoint, sizeof(f->endpoint),
                   "pouch://%s?single_writer=true&background_compaction=false&"
                   "query_indexing=false&queue_watch=false&query_engine=scan",
                   f->path) > 0);
  f->endpoints[0] = f->endpoint;
  lc_client_config_init(&f->config);
  lc_pouch_settings_init(&f->settings);
  f->settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_COMPRESSION;
  f->settings.durable_sync = sync;
  f->settings.compression = codec & 2 ? "zlib" : "none";
  if (rotate) {
    f->settings.set_mask |= LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
    f->settings.segment_target_bytes = 65536U;
  }
  f->config.endpoints = f->endpoints;
  f->config.endpoint_count = 1U;
  f->config.default_namespace = "journal";
  f->config.pouch_settings = &f->settings;
  f->config.pouch_crypto_key = codec & 1 ? key : NULL;
  open_client(f, 1);
  lc_collection_config missing;
  lc_collection *absent = NULL;
  lc_collection_config_init(&missing);
  missing.ns = "missing-namespace";
  missing.name = "absent";
  REQUIRE(lc_client_open_collection(f->client, &missing, &absent, &f->error) ==
          LC_ERR_INVALID);
  REQUIRE(!absent && f->error.server_code &&
          !strcmp(f->error.server_code, "collection_not_found"));
  clear(&f->error);
  ok(lc_client_list_collections(f->client, missing.ns, unexpected_collection,
                                NULL, &f->error),
     &f->error);
  open_collection(f, 1, &f->collection);
  open_collection(f, 0, &f->peer);
}
static void setup(fixture *f, const char *directory, int codec, int sync,
                  int rotate) {
  setup_io(f, directory, codec, sync, rotate, NULL);
}
static void close_store(fixture *f) {
  struct stat info;
  lc_collection_close(f->peer);
  lc_collection_close(f->collection);
  f->peer = f->collection = NULL;
  lc_client_close(f->client);
  f->client = NULL;
  REQUIRE(!stat(f->path, &info));
  REQUIRE(S_ISREG(info.st_mode) && info.st_size == PS_DEFAULT_CAPACITY);
}
static void reopen(fixture *f) {
  close_store(f);
  open_client(f, 0);
  open_collection(f, 0, &f->collection);
  open_collection(f, 0, &f->peer);
}
static unsigned char byte_at(uint64_t at, uint64_t size) {
  static const char prefix[] = "{\"blob\":\"";
  static const char alphabet[] =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
  uint64_t n;
  if (at < sizeof(prefix) - 1U)
    return (unsigned char)prefix[at];
  if (at >= size - 2U)
    return at == size - 2U ? '"' : '}';
  n = at + UINT64_C(0x9e3779b97f4a7c15);
  n = (n ^ (n >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
  n = (n ^ (n >> 27U)) * UINT64_C(0x94d049bb133111eb);
  return (unsigned char)alphabet[(n ^ (n >> 31U)) & 63U];
}
typedef struct producer {
  lc_source source;
  uint64_t at, size;
  size_t reads;
} producer;
static size_t produce(lc_source *self, void *bytes, size_t count, lc_error *e) {
  producer *p = self->impl;
  (void)e;
  ++p->reads;
  if (count > 1024U)
    count = 1024U;
  if ((uint64_t)count > p->size - p->at)
    count = (size_t)(p->size - p->at);
  for (size_t i = 0U; i < count; ++i)
    ((unsigned char *)bytes)[i] = byte_at(p->at + i, p->size);
  p->at += count;
  return count;
}
static void producer_init(producer *p, uint64_t size) {
  memset(p, 0, sizeof(*p));
  p->size = size;
  p->source.read = produce;
  p->source.impl = p;
}
static lc_collection_append_res append(lc_collection *c, const char *id,
                                       uint64_t size, lc_error *e) {
  producer p;
  lc_collection_append_req req;
  lc_collection_append_res out;
  producer_init(&p, size);
  lc_collection_append_req_init(&req);
  req.append_id = id;
  ok(lc_collection_append_staged(c, &req, &p.source, &out, e), e);
  REQUIRE(p.at == size && p.reads > 1U);
  REQUIRE(out.bytes == size);
  return out;
}
typedef struct consumer {
  lc_sink sink;
  uint64_t size, at, position;
  size_t delivered;
} consumer;
static int consume(lc_sink *self, const void *bytes, size_t count,
                   lc_error *e) {
  consumer *c = self->impl;
  const unsigned char *p = bytes;
  (void)e;
  REQUIRE(c->at + count <= c->size + 1U);
  for (size_t i = 0U; i < count; ++i)
    REQUIRE(p[i] ==
            (c->at + i == c->size ? '\n' : byte_at(c->at + i, c->size)));
  c->at += count;
  return 1;
}
static int metadata(void *context, const lc_collection_read_metadata *m,
                    lc_error *e) {
  consumer *c = context;
  (void)e;
  if (m->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    REQUIRE(m->position == c->position && m->bytes == c->size);
    REQUIRE(c->at == c->size + 1U);
    ++c->delivered;
  }
  return LC_OK;
}
static void read_entry(lc_collection *collection, uint64_t position,
                       uint64_t size, lc_error *e) {
  consumer c;
  lc_collection_read_opts opts;
  lc_collection_read_res result;
  memset(&c, 0, sizeof(c));
  c.sink.write = consume;
  c.sink.impl = &c;
  c.size = size;
  c.position = position;
  lc_collection_read_opts_init(&opts);
  opts.has_after = opts.has_through = 1;
  opts.after = position - 1U;
  opts.through = position;
  opts.on_metadata = metadata;
  opts.metadata_context = &c;
  lc_collection_read_res_init(&result);
  ok(lc_collection_read(collection, &opts, &c.sink, &result, e), e);
  REQUIRE(c.delivered == 1U && c.at == size + 1U);
  lc_collection_read_res_cleanup(&result);
}
static char *read_window(lc_collection *collection, uint64_t after,
                         uint64_t position, uint64_t size, const char *cursor,
                         lc_error *e) {
  consumer c = {0};
  lc_collection_read_opts opts;
  lc_collection_read_res result;
  c.sink.write = consume;
  c.sink.impl = &c;
  c.size = size;
  c.position = position;
  lc_collection_read_opts_init(&opts);
  opts.has_after = opts.has_first_n = 1;
  opts.after = after;
  opts.first_n = 1U;
  opts.selector_lql = "exists{/blob}";
  opts.cursor = cursor;
  opts.on_metadata = metadata;
  opts.metadata_context = &c;
  lc_collection_read_res_init(&result);
  ok(lc_collection_read(collection, &opts, &c.sink, &result, e), e);
  REQUIRE(c.delivered == 1U && result.complete && result.checkpoint_cursor);
  char *next = lc_strdup_with_allocator(NULL, result.checkpoint_cursor);
  REQUIRE(next);
  lc_collection_read_res_cleanup(&result);
  return next;
}
typedef struct stream_proof {
  producer *producer;
  size_t before_eof;
} stream_proof;
static int note_chunk(void *context, lc_collection *collection, int phase,
                      lc_error *e) {
  stream_proof *proof = context;
  (void)collection;
  (void)e;
  if (phase == LC_COLLECTION_APPEND_CHUNK_PENDING && proof->producer->at > 0U &&
      proof->producer->at < proof->producer->size)
    ++proof->before_eof;
  return LC_OK;
}
static int listed(void *context, const lc_collection_info *info, lc_error *e) {
  size_t *count = context;
  (void)e;
  REQUIRE(!strcmp(info->name, "events/\303\245"));
  REQUIRE(info->upper_position > 0U);
  ++*count;
  return LC_OK;
}
static void lifecycle(const char *directory, int codec, int sync) {
  fixture f;
  producer input;
  stream_proof proof;
  lc_collection_append_req req, replacement;
  lc_collection_append_res large, small, duplicate;
  lc_collection_append_status_res status;
  lc_collection_delete_res deleted;
  lc_collection_edit_req edit;
  lc_collection_edit_res edited;
  lc_collection_edit_status_res receipt;
  lc_collection_edit_range range;
  lc_collection_head_res head, peer_head;
  lc_collection_lease_req lease;
  lc_collection_lease_info ownership;
  lc_collection *owned = NULL;
  lc_collection *successor = NULL;
  char *checkpoint = NULL, *next_checkpoint;
  lc_collection_writer *push = NULL;
  lc_collection_metadata_req policy;
  lc_collection_info info;
  char old_id[LC_XID_STRING_SIZE];
  size_t count = 0U;
  const uint64_t large_size = PS_MIB + PS_MIB / 4U;
  setup(&f, directory, codec, sync, 0);
  producer_init(&input, large_size);
  memset(&proof, 0, sizeof(proof));
  proof.producer = &input;
  lc_collection_append_req_init(&req);
  req.append_id = "large";
  lc_collection_append_test_context = &proof;
  lc_collection_append_test_hook = note_chunk;
  ok(lc_collection_append_staged(f.collection, &req, &input.source, &large,
                                 &f.error),
     &f.error);
  lc_collection_append_test_hook = NULL;
  lc_collection_append_test_context = NULL;
  REQUIRE(proof.before_eof && large.bytes == large_size && !large.duplicate);
  small = append(f.peer, "small", 80U, &f.error);
  REQUIRE(small.position > large.position);
  read_entry(f.peer, large.position, large_size, &f.error);
  duplicate = append(f.peer, "large", large_size, &f.error);
  REQUIRE(duplicate.duplicate && duplicate.position == large.position);
  REQUIRE(!strcmp(duplicate.etag, large.etag));
  ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
  ok(lc_collection_head(f.peer, &peer_head, &f.error), &f.error);
  REQUIRE(!strcmp(head.etag, peer_head.etag));
  checkpoint =
      read_window(f.peer, large.position, small.position, 80U, NULL, &f.error);
  memcpy(old_id, head.collection_id, sizeof(old_id));
  lc_collection_edit_req_init(&edit);
  lc_collection_append_req_init(&replacement);
  replacement.append_id = "replacement";
  range.after = large.position;
  range.through = small.position;
  edit.operation_id = "edit-1";
  edit.expected_head = head.etag;
  edit.ranges = &range;
  edit.range_count = 1U;
  edit.replacement = &replacement;
  edit.has_recovery = 1;
  edit.recovery = "checkpoint-description";
  edit.recovery_anchor = UINT64_MAX;
  producer_init(&input, 96U);
  ok(lc_collection_edit(f.peer, &edit, &input.source, &edited, &f.error),
     &f.error);
  REQUIRE(edited.published && edited.deleted_count == 1U &&
          edited.has_replacement &&
          edited.replacement.position > small.position);
  read_entry(f.collection, edited.replacement.position, 96U, &f.error);
  next_checkpoint =
      read_window(f.collection, large.position, edited.replacement.position,
                  96U, checkpoint, &f.error);
  lc_free_with_allocator(NULL, checkpoint);
  lc_free_with_allocator(NULL, next_checkpoint);
  checkpoint = NULL;
  producer_init(&input, 96U);
  ok(lc_collection_edit(f.collection, &edit, &input.source, &edited, &f.error),
     &f.error);
  REQUIRE(edited.duplicate);
  ok(lc_collection_delete_entry(f.collection, large.position, &deleted,
                                &f.error),
     &f.error);
  REQUIRE(deleted.deleted && !deleted.already_deleted);
  duplicate = append(f.peer, "large", large_size, &f.error);
  REQUIRE(duplicate.duplicate && duplicate.deleted &&
          duplicate.position == large.position);
  ok(lc_client_list_collections(f.client, NULL, listed, &count, &f.error),
     &f.error);
  REQUIRE(count == 1U);
  reopen(&f);
  ok(lc_collection_append_status(f.peer, "large", &status, &f.error), &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_DELETED &&
          status.entry.position == large.position);
  ok(lc_collection_edit_status(f.collection, "edit-1", &receipt, &f.error),
     &f.error);
  REQUIRE(receipt.found &&
          receipt.result.replacement.position == edited.replacement.position);
  ok(lc_collection_head(f.peer, &head, &f.error), &f.error);
  REQUIRE(head.recovery_anchor == edited.replacement.position &&
          !strcmp(head.recovery, "checkpoint-description"));
  read_entry(f.collection, head.recovery_anchor, 96U, &f.error);
  lc_collection_metadata_req_init(&policy);
  policy.has_retention_seconds = 1;
  policy.retention_seconds = 300L;
  ok(lc_collection_entry_metadata(f.collection, head.recovery_anchor, &policy,
                                  &duplicate, &f.error),
     &f.error);
  REQUIRE(duplicate.has_retention_expires_at_unix &&
          duplicate.retention_expires_at_unix > 0);
  policy.has_max_entries = 1;
  policy.max_entries = 8U;
  ok(lc_collection_metadata(f.peer, &policy, &info, &f.error), &f.error);
  REQUIRE(info.max_entries == 8U && info.retention_expires_at_unix > 0);
  lc_collection_append_req_init(&req);
  req.append_id = "direct";
  producer_init(&input, 512U);
  ok(lc_collection_append(f.peer, &req, &input.source, &duplicate, &f.error),
     &f.error);
  read_entry(f.collection, duplicate.position, 512U, &f.error);
  req.append_id = "push";
  ok(lc_collection_append_begin(f.collection, &req, &push, &f.error), &f.error);
  ok(lc_collection_writer_write(push, "{\"blob\":\"", 9U, &f.error), &f.error);
  ok(lc_collection_append_status(f.peer, "push", &status, &f.error), &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_ABSENT);
  ok(lc_collection_writer_write(push, "done\"}", 6U, &f.error), &f.error);
  ok(lc_collection_writer_commit(push, &duplicate, &f.error), &f.error);
  lc_collection_writer_close(push);
  push = NULL;
  req.append_id = "aborted";
  ok(lc_collection_append_begin(f.collection, &req, &push, &f.error), &f.error);
  ok(lc_collection_writer_write(push, "{\"blob\":\"", 9U, &f.error), &f.error);
  ok(lc_collection_writer_abort(push, &f.error), &f.error);
  lc_collection_writer_close(push);
  push = NULL;
  ok(lc_collection_append_status(f.peer, "aborted", &status, &f.error),
     &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_ABSENT);
  lc_collection_lease_req_init(&lease);
  lease.owner = "sdk-test";
  lease.private_read = 1;
  ok(lc_collection_acquire(f.collection, &lease, &owned, &f.error), &f.error);
  ok(lc_collection_keepalive(owned, 30L, &ownership, &f.error), &f.error);
  REQUIRE(ownership.fencing_token > 0U);
  consumer denied = {0};
  lc_collection_read_res denied_result;
  denied.sink.write = consume;
  denied.sink.impl = &denied;
  lc_collection_read_res_init(&denied_result);
  REQUIRE(lc_collection_read(f.peer, NULL, &denied.sink, &denied_result,
                             &f.error) == LC_ERR_INVALID);
  REQUIRE(f.error.server_code &&
          !strcmp(f.error.server_code, "collection_private"));
  clear(&f.error);
  lc_collection_read_res_cleanup(&denied_result);
  read_entry(owned, edited.replacement.position, 96U, &f.error);
  ok(lc_collection_takeover(f.peer, &lease, ownership.lease_id, &successor,
                            &f.error),
     &f.error);
  REQUIRE(lc_collection_keepalive(owned, 30L, &ownership, &f.error) ==
          LC_ERR_INVALID);
  REQUIRE(f.error.server_code && !strcmp(f.error.server_code, "lease_lost"));
  clear(&f.error);
  lc_collection_close(owned);
  owned = NULL;
  ok(lc_collection_release(successor, &f.error), &f.error);
  successor = NULL;
  ok(lc_collection_drop(f.collection, &f.error), &f.error);
  open_collection(&f, 1, &owned);
  ok(lc_collection_head(owned, &head, &f.error), &f.error);
  REQUIRE(strcmp(head.collection_id, old_id) && !head.upper_position);
  REQUIRE(lc_collection_describe(f.peer, &(lc_collection_info){0}, &f.error) ==
          LC_ERR_INVALID);
  REQUIRE(f.error.server_code &&
          !strcmp(f.error.server_code, "collection_stale"));
  clear(&f.error);
  lc_collection_close(owned);
  close_store(&f);
  lc_error_cleanup(&f.error);
  printf("SDK lifecycle codec=%d durable=%d passed\n", codec, sync);
  fflush(stdout);
}
typedef struct gate {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  lc_collection *collection;
  int entered, released;
} gate;
static void gate_wait(gate *g, int released) {
  struct timespec until;
  REQUIRE(!clock_gettime(CLOCK_REALTIME, &until));
  until.tv_sec += 30;
  REQUIRE(!pthread_mutex_lock(&g->mutex));
  while (!(released ? g->released : g->entered))
    REQUIRE(!pthread_cond_timedwait(&g->condition, &g->mutex, &until));
  REQUIRE(!pthread_mutex_unlock(&g->mutex));
}
static void gate_signal(gate *g, int released) {
  REQUIRE(!pthread_mutex_lock(&g->mutex));
  if (released)
    g->released = 1;
  else
    g->entered = 1;
  REQUIRE(!pthread_cond_broadcast(&g->condition));
  REQUIRE(!pthread_mutex_unlock(&g->mutex));
}
static int pause_chunk(void *context, lc_collection *c, int phase,
                       lc_error *e) {
  gate *g = context;
  (void)e;
  if (c == g->collection && phase == LC_COLLECTION_APPEND_CHUNK_PENDING &&
      !g->entered) {
    gate_signal(g, 0);
    gate_wait(g, 1);
  }
  return LC_OK;
}
typedef struct worker {
  lc_collection *collection;
  lc_collection_append_res result;
} worker;
static void *upload(void *context) {
  worker *w = context;
  lc_error e;
  lc_error_init(&e);
  w->result = append(w->collection, "held", 262144U, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(const char *directory, int sync) {
  fixture f;
  gate g;
  worker w;
  pthread_t thread;
  lc_collection_append_res peer;
  lc_collection_append_status_res status;
  setup(&f, directory, 0, sync, 1);
  memset(&g, 0, sizeof(g));
  REQUIRE(!pthread_mutex_init(&g.mutex, NULL));
  REQUIRE(!pthread_cond_init(&g.condition, NULL));
  g.collection = f.collection;
  w.collection = f.collection;
  lc_collection_append_test_context = &g;
  lc_collection_append_test_hook = pause_chunk;
  REQUIRE(!pthread_create(&thread, NULL, upload, &w));
  gate_wait(&g, 0);
  ok(lc_collection_append_status(f.peer, "held", &status, &f.error), &f.error);
  REQUIRE(status.state == LC_COLLECTION_APPEND_ABSENT);
  peer = append(f.peer, "peer", 131072U, &f.error);
  read_entry(f.peer, peer.position, 131072U, &f.error);
  gate_signal(&g, 1);
  REQUIRE(!pthread_join(thread, NULL));
  lc_collection_append_test_hook = NULL;
  lc_collection_append_test_context = NULL;
  REQUIRE(w.result.position > peer.position);
  read_entry(f.peer, w.result.position, 262144U, &f.error);
  reopen(&f);
  read_entry(f.collection, peer.position, 131072U, &f.error);
  read_entry(f.peer, w.result.position, 262144U, &f.error);
  close_store(&f);
  REQUIRE(!pthread_cond_destroy(&g.condition));
  REQUIRE(!pthread_mutex_destroy(&g.mutex));
  lc_error_cleanup(&f.error);
  printf("SDK independent producers durable=%d passed\n", sync);
}
typedef struct handoff_task {
  lc_collection_writer *writer;
  lc_collection_append_res result;
  lc_collection *peer;
  gate *done;
} handoff_task;
static void feed_writer(lc_collection_writer *writer, uint64_t start,
                        uint64_t end, lc_error *e) {
  unsigned char bytes[1024];
  while (start < end) {
    size_t count =
        end - start < sizeof(bytes) ? (size_t)(end - start) : sizeof(bytes);
    for (size_t i = 0U; i < count; ++i)
      bytes[i] = byte_at(start + i, 262144U);
    ok(lc_collection_writer_write(writer, bytes, count, e), e);
    start += count;
  }
}
static void *finish_handoff(void *context) {
  handoff_task *task = context;
  lc_error e;
  lc_error_init(&e);
  feed_writer(task->writer, 131072U, 262144U, &e);
  ok(lc_collection_writer_commit(task->writer, &task->result, &e), &e);
  lc_collection_writer_close(task->writer);
  lc_error_cleanup(&e);
  return NULL;
}
static void *wait_upload_pins(void *context) {
  handoff_task *task = context;
  lc_collection_handle *c = task->peer->impl;
  lc_collection_lock pin;
  lc_error e;
  lc_error_init(&e);
  gate_signal(task->done, 0);
  ok(lc_collection_lock_enter(c->authority, LC_COLLECTION_AUTH_STREAMS, 0U, 1,
                              1, &pin, &e),
     &e);
  lc_collection_lock_leave(&pin);
  gate_signal(task->done, 1);
  lc_error_cleanup(&e);
  return NULL;
}
static void writer_handoff(const char *directory, int codec, int sync) {
  fixture f;
  handoff_task task = {0};
  gate done = {0};
  lc_collection_writer *held = NULL;
  lc_collection_append_req req;
  lc_collection_lock pin;
  lc_collection_handle *peer;
  pthread_t producer, waiter;
  setup(&f, directory, codec, sync, 1);
  REQUIRE(!pthread_mutex_init(&done.mutex, NULL));
  REQUIRE(!pthread_cond_init(&done.condition, NULL));
  lc_collection_append_req_init(&req);
  req.append_id = "handoff";
  ok(lc_collection_append_begin(f.collection, &req, &task.writer, &f.error),
     &f.error);
  feed_writer(task.writer, 0U, 131072U, &f.error);
  req.append_id = "still-held";
  ok(lc_collection_append_begin(f.collection, &req, &held, &f.error), &f.error);
  feed_writer(held, 0U, 131072U, &f.error);
  task.peer = f.peer;
  task.done = &done;
  peer = f.peer->impl;
  REQUIRE(!pthread_create(&waiter, NULL, wait_upload_pins, &task));
  gate_wait(&done, 0);
  REQUIRE(!pthread_create(&producer, NULL, finish_handoff, &task));
  REQUIRE(!pthread_join(producer, NULL));
  REQUIRE(lc_collection_lock_enter(peer->authority, LC_COLLECTION_AUTH_STREAMS,
                                   0U, 1, 0, &pin, &f.error) == LC_ERR_PENDING);
  clear(&f.error);
  ok(lc_collection_writer_abort(held, &f.error), &f.error);
  lc_collection_writer_close(held);
  gate_wait(&done, 1);
  REQUIRE(!pthread_join(waiter, NULL));
  ok(lc_collection_lock_enter(peer->authority, LC_COLLECTION_AUTH_STREAMS, 0U,
                              1, 0, &pin, &f.error),
     &f.error);
  lc_collection_lock_leave(&pin);
  read_entry(f.peer, task.result.position, 262144U, &f.error);
  close_store(&f);
  REQUIRE(!pthread_cond_destroy(&done.condition));
  REQUIRE(!pthread_mutex_destroy(&done.mutex));
  lc_error_cleanup(&f.error);
  printf("SDK upload pin thread handoff codec=%d durable=%d passed\n", codec,
         sync);
}
static void process_recovery(const char *directory, int codec, int sync) {
  fixture f;
  pid_t child;
  int status;
  lc_collection_append_status_res result;
  setup(&f, directory, codec, sync, 1);
  close_store(&f);
  child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    open_client(&f, 0);
    open_collection(&f, 0, &f.collection);
    (void)append(f.collection, "child", 65536U, &f.error);
    _exit(83);
  }
  REQUIRE(waitpid(child, &status, 0) == child);
  REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 83);
  open_client(&f, 0);
  open_collection(&f, 0, &f.collection);
  ok(lc_collection_append_status(f.collection, "child", &result, &f.error),
     &f.error);
  REQUIRE(result.state == LC_COLLECTION_APPEND_COMMITTED);
  read_entry(f.collection, result.entry.position, 65536U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
}
static int count_child(void *context, const void *name, size_t length,
                       const ps_catalog_info *info, lc_error *e) {
  size_t *count = context;
  (void)name;
  (void)info;
  (void)e;
  REQUIRE(length == 8U);
  ++*count;
  return LC_OK;
}
static ps_root *client_root(fixture *f) {
  return ps_pouch_state_root(((lc_client_handle *)f->client)->pouch);
}
static void reclamation(const char *directory) {
  fixture f;
  ps_root *root;
  ps_namespace *ns = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info, restored;
  ps_owned_head *identity = NULL;
  ps_owned_status status;
  size_t count = 0U;
  uint64_t reclaimed, skipped, namespace_id;
  setup(&f, directory, 0, 1, 1);
  for (int i = 0; i < 4; ++i) {
    char id[32];
    snprintf(id, sizeof(id), "item-%d", i);
    (void)append(f.collection, id, 50000U, &f.error);
  }
  root = client_root(&f);
  ok(ps_root_namespace_acquire(root, "journal", 0, &ns, &f.error), &f.error);
  namespace_id = ns->identity.owner;
  ok(ps_catalog_get(root->space, namespace_id, PS_CATALOG_COLLECTION,
                    "events/\303\245", strlen("events/\303\245"), &view,
                    &f.error),
     &f.error);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, &info, &f.error), &f.error);
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  ok(ps_catalog_visit(root->space, info.stream.owner, PS_CATALOG_SEGMENT,
                      count_child, &count, &f.error),
     &f.error);
  REQUIRE(count > 1U);
  /* Simulate a name removal whose process dies before the cache observes it. */
  ok(ps_catalog_remove(root->space, namespace_id, PS_CATALOG_COLLECTION,
                       "events/\303\245", strlen("events/\303\245"),
                       &info.revision, &f.error),
     &f.error);
  close_store(&f);
  open_client(&f, 0);
  root = client_root(&f);
  ok(ps_owned_open(root->space, NULL, info.stream, 0, &info.target, &identity,
                   &status, &f.error),
     &f.error);
  ok(ps_owned_claim_orphan(identity, &f.error), &f.error);
  REQUIRE(ps_catalog_put(root->space, NULL, namespace_id, PS_CATALOG_COLLECTION,
                         "events/\303\245", strlen("events/\303\245"), NULL,
                         info.stream, &info.target, &restored,
                         &f.error) == LC_ERR_INVALID);
  clear(&f.error);
  ps_owned_close(identity);
  identity = NULL;
  ok(ps_catalog_put(root->space, NULL, namespace_id, PS_CATALOG_COLLECTION,
                    "events/\303\245", strlen("events/\303\245"), NULL,
                    info.stream, &info.target, &restored, &f.error),
     &f.error);
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, &f.error), &f.error);
  REQUIRE(!reclaimed);
  open_collection(&f, 0, &f.collection);
  read_entry(f.collection, 4U, 50000U, &f.error);
  ok(ps_catalog_remove(root->space, namespace_id, PS_CATALOG_COLLECTION,
                       "events/\303\245", strlen("events/\303\245"),
                       &restored.revision, &f.error),
     &f.error);
  close_store(&f);
  open_client(&f, 0);
  root = client_root(&f);
  ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, &f.error), &f.error);
  REQUIRE(reclaimed > 0U);
  count = 0U;
  ok(ps_catalog_visit(root->space, info.stream.owner, PS_CATALOG_SEGMENT,
                      count_child, &count, &f.error),
     &f.error);
  REQUIRE(!count);
  REQUIRE(ps_owned_open(root->space, NULL, info.stream, 0, &info.target,
                        &identity, &status, &f.error) == LC_ERR_INVALID);
  clear(&f.error);
  open_collection(&f, 1, &f.collection);
  (void)append(f.collection, "new-incarnation", 80U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  puts("SDK owner-tree reclamation passed");
}
static void checkpoint_corruption(const char *directory) {
  fixture f;
  lc_collection_handle *c;
  lc_collection_order_row row;
  ps_collection_segment *segment = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_namespace *ns;
  ps_root *root;
  unsigned char original[32], damaged[32];
  struct iovec part = {original, sizeof(original)};
  int found;
  setup(&f, directory, 0, 1, 0);
  (void)append(f.collection, "seed", 80U, &f.error);
  c = f.collection->impl;
  ok(lc_collection_order_find(&c->order, 1U, &row, &found, &f.error), &f.error);
  REQUIRE(found);
  root = client_root(&f);
  ok(ps_checkpoint(root->space, &f.error), &f.error);
  ok(lc_pouch_record_io_read(c->active.io, row.record_offset, &part, 1,
                             &f.error),
     &f.error);
  memcpy(damaged, original, sizeof(damaged));
  damaged[31] ^= 0x80U;
  part.iov_base = damaged;
  ok(lc_pouch_record_io_patch(c->active.io, row.record_offset, &part, 1,
                              &f.error),
     &f.error);
  ok(lc_collection_segment_force(&c->active, &f.error), &f.error);
  close_store(&f);
  open_client(&f, 0);
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.name = "events/\303\245";
  REQUIRE(lc_client_open_collection(f.client, &config, &f.collection,
                                    &f.error) == LC_ERR_PROTOCOL);
  clear(&f.error);
  root = client_root(&f);
  ok(ps_root_namespace_acquire(root, "journal", 0, &ns, &f.error), &f.error);
  ok(ps_catalog_get(root->space, ns->identity.owner, PS_CATALOG_COLLECTION,
                    config.name, strlen(config.name), &view, &f.error),
     &f.error);
  REQUIRE(view);
  ok(ps_catalog_view_info(view, &info, &f.error), &f.error);
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  info.stream.key = row.segment;
  ok(ps_collection_segment_open(root->space, info.stream, 0, NULL, NULL, NULL,
                                &segment, &f.error),
     &f.error);
  /* Failed admission must leave the damaged prefix intact. Repair this test
   * mutation through the same physical owner, then prove ordinary admission. */
  part.iov_base = damaged;
  ok(lc_pouch_record_io_read(ps_collection_segment_io(segment),
                             row.record_offset, &part, 1, &f.error),
     &f.error);
  REQUIRE(damaged[31] == (unsigned char)(original[31] ^ 0x80U));
  part.iov_base = original;
  ok(lc_pouch_record_io_patch(ps_collection_segment_io(segment),
                              row.record_offset, &part, 1, &f.error),
     &f.error);
  ok(ps_collection_segment_sync(segment, &f.error), &f.error);
  ps_collection_segment_close(segment);
  open_collection(&f, 0, &f.collection);
  read_entry(f.collection, 1U, 80U, &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  puts("SDK checkpointed-prefix corruption remains explicit");
}
typedef struct phase_failure {
  int phase, hits;
} phase_failure;
static int fail_edit(void *context, lc_collection *collection, int phase,
                     lc_error *e) {
  phase_failure *f = context;
  (void)collection;
  if (phase == f->phase && !f->hits++)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L, "injected edit phase failure",
                        NULL, NULL, "test");
  return LC_OK;
}
static void edit_recovery(const char *directory, int codec, int sync) {
  static const int phases[] = {
      LC_COLLECTION_EDIT_PREPARED,  LC_COLLECTION_EDIT_BODY,
      LC_COLLECTION_EDIT_PUBLISHED, LC_COLLECTION_EDIT_DIRTY,
      LC_COLLECTION_EDIT_PAGE,      LC_COLLECTION_EDIT_PROJECTED};
  for (size_t i = 0U; i < sizeof(phases) / sizeof(phases[0]); ++i) {
    fixture f;
    producer p;
    lc_collection_head_res head;
    lc_collection_edit_req edit;
    lc_collection_edit_res result;
    lc_collection_edit_status_res receipt;
    lc_collection_append_req replacement;
    char original_head[65];
    lc_collection_edit_range cut = {0U, 1U};
    phase_failure failure = {phases[i], 0};
    setup(&f, directory, codec, sync, 0);
    (void)append(f.collection, "seed", 80U, &f.error);
    ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
    memcpy(original_head, head.etag, sizeof(original_head));
    lc_collection_edit_req_init(&edit);
    lc_collection_append_req_init(&replacement);
    replacement.append_id = "new";
    edit.operation_id = "uncertain";
    edit.expected_head = original_head;
    edit.ranges = &cut;
    edit.range_count = 1U;
    edit.replacement = &replacement;
    producer_init(&p, 512U);
    lc_collection_edit_test_context = &failure;
    lc_collection_edit_test_hook = fail_edit;
    REQUIRE(lc_collection_edit(f.collection, &edit, &p.source, &result,
                               &f.error) == LC_ERR_TRANSPORT);
    lc_collection_edit_test_hook = NULL;
    lc_collection_edit_test_context = NULL;
    REQUIRE(failure.hits);
    clear(&f.error);
    /* A fresh receiver must repair canonical history, not trust this failed
     * receiver's disposable projection or its partially applied page. */
    reopen(&f);
    ok(lc_collection_edit_status(f.peer, "uncertain", &receipt, &f.error),
       &f.error);
    int published = phases[i] != LC_COLLECTION_EDIT_PREPARED &&
                    phases[i] != LC_COLLECTION_EDIT_BODY;
    REQUIRE(!!receipt.found == published);
    read_entry(f.collection, published ? 2U : 1U, published ? 512U : 80U,
               &f.error);
    ok(lc_collection_head(f.peer, &head, &f.error), &f.error);
    edit.expected_head = published ? original_head : head.etag;
    producer_init(&p, 512U);
    ok(lc_collection_edit(f.peer, &edit, &p.source, &result, &f.error),
       &f.error);
    REQUIRE(!!result.duplicate == published && result.deleted_count == 1U);
    read_entry(f.collection, result.replacement.position, 512U, &f.error);
    close_store(&f);
    lc_error_cleanup(&f.error);
  }
}
typedef struct disk_model {
  unsigned char *stable, *dirty, *scratch;
  size_t calls, cut;
  int armed, fired, mode, freeze;
} disk_model;
static int disk_cut(disk_model *m) {
  if (m->armed && ++m->calls == m->cut) {
    m->fired = m->freeze = 1;
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
  if (fail && diagnostic)
    fprintf(stderr,
            "write cut at=%llu bytes=%zu first=%02x,%02x,%02x,%02x "
            "type=%u flags=%u\n",
            (unsigned long long)at, n,
            n > 0U ? ((const unsigned char *)bytes)[0] : 0U,
            n > 1U ? ((const unsigned char *)bytes)[1] : 0U,
            n > 2U ? ((const unsigned char *)bytes)[2] : 0U,
            n > 3U ? ((const unsigned char *)bytes)[3] : 0U,
            n > 5U ? ((const unsigned char *)bytes)[5] : 0U,
            n > 6U ? ((const unsigned char *)bytes)[6] : 0U);
  size_t count = !fail || m->mode == 3 ? n : m->mode ? n / 2U : 0U;
  if (count) {
    REQUIRE(pwrite(fd, bytes, count, at) == (ssize_t)count);
    for (size_t page = (size_t)at / PS_PAGE;
         page <= ((size_t)at + count - 1U) / PS_PAGE; ++page)
      m->dirty[page] = 1;
    if (fail && m->mode >= 2)
      memcpy(m->stable + at, bytes, count);
  }
  if (fail) {
    errno = EIO;
    return -1;
  }
  return (ssize_t)count;
}
static int disk_sync(void *context, int fd) {
  disk_model *m = context;
  int fail = disk_cut(m);
  if (!fail || m->mode >= 2) {
    REQUIRE(!fdatasync(fd));
    if (!m->freeze || (fail && m->mode >= 2))
      for (size_t page = 0U; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
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
  if (disk_cut(context)) {
    errno = EIO;
    return -1;
  }
  return fallocate(fd, 0, at, n);
}
static void restore_disk(disk_model *m, const char *path) {
  int fd = open(path, O_RDWR | O_CLOEXEC);
  REQUIRE(fd >= 0);
  for (size_t page = 0U; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
    if (m->dirty[page])
      REQUIRE(pwrite(fd, m->stable + page * PS_PAGE, PS_PAGE,
                     (off_t)(page * PS_PAGE)) == PS_PAGE);
  REQUIRE(!fdatasync(fd) && !close(fd));
  memset(m->dirty, 0, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
  m->freeze = 0;
}
static size_t io_trial(const char *directory, int codec, int sync,
                       int operation, int mode, size_t cut) {
  fixture f;
  disk_model m = {0};
  ps_io io = {0};
  lc_collection_config config;
  lc_collection *created = NULL;
  lc_collection_append_req req;
  lc_collection_append_res appended;
  lc_collection_append_status_res status;
  lc_collection_edit_req edit;
  lc_collection_edit_res edited;
  lc_collection_edit_status_res receipt;
  lc_collection_edit_range range = {0U, 1U};
  lc_collection_head_res head;
  producer p;
  int rc;
  snprintf(scenario, sizeof(scenario),
           "codec=%d sync=%d operation=%d mode=%d "
           "cut=%zu",
           codec, sync, operation, mode, cut);
  m.mode = mode;
  m.cut = cut;
  m.stable = calloc(1U, (size_t)PS_DEFAULT_CAPACITY);
  m.dirty = calloc((size_t)(PS_DEFAULT_CAPACITY / PS_PAGE), 1U);
  m.scratch = malloc(PS_PAGE);
  REQUIRE(m.stable && m.dirty && m.scratch);
  io.context = &m;
  io.read = disk_read;
  io.write = disk_write;
  io.sync = disk_sync;
  io.reserve = disk_reserve;
  setup_io(&f, directory, codec, sync, 1, &io);
  (void)append(f.collection, "seed", 80U, &f.error);
  ok(ps_flush(ps_root_space(client_root(&f)), &f.error), &f.error);
  ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
  lc_collection_config_init(&config);
  config.name = "created";
  lc_collection_append_req_init(&req);
  req.append_id = operation == 1 ? "new-append" : "replacement";
  lc_collection_edit_req_init(&edit);
  edit.operation_id = "edit-cut";
  edit.expected_head = head.etag;
  edit.ranges = &range;
  edit.range_count = 1U;
  edit.replacement = &req;
  producer_init(&p, operation == 1 ? 73728U : 512U);
  m.armed = 1;
  if (operation == 0)
    rc = lc_client_create_collection(f.client, &config, &created, &f.error);
  else if (operation == 1)
    rc = lc_collection_append_staged(f.collection, &req, &p.source, &appended,
                                     &f.error);
  else if (operation == 2)
    rc = lc_collection_edit(f.collection, &edit, &p.source, &edited, &f.error);
  else
    rc = lc_collection_drop(f.collection, &f.error);
  m.armed = 0;
  size_t calls = m.calls;
  if (cut) {
    if (!m.fired || rc == LC_OK)
      fprintf(stderr,
              "I/O cut codec=%d sync=%d operation=%d mode=%d cut=%zu "
              "calls=%zu fired=%d rc=%d\n",
              codec, sync, operation, mode, cut, calls, m.fired, rc);
    REQUIRE(m.fired && rc != LC_OK);
  } else
    ok(rc, &f.error);
  m.freeze = 1;
  lc_collection_close(created);
  clear(&f.error);
  close_store(&f);
  restore_disk(&m, f.path);
  /* Reopen through ordinary I/O: only modeled stable bytes survived. */
  f.io = NULL;
  open_client(&f, 0);
  if (operation == 0) {
    rc = lc_client_open_collection(f.client, &config, &created, &f.error);
    REQUIRE(rc == LC_OK ||
            (rc == LC_ERR_INVALID && f.error.server_code &&
             !strcmp(f.error.server_code, "collection_not_found")));
    if (rc == LC_OK) {
      ok(lc_collection_head(created, &head, &f.error), &f.error);
      REQUIRE(!head.upper_position && !head.live_count);
    }
    lc_collection_close(created);
    created = NULL;
    clear(&f.error);
    open_collection(&f, 0, &f.collection);
    read_entry(f.collection, 1U, 80U, &f.error);
  } else if (operation == 3) {
    lc_collection_config_init(&config);
    config.name = "events/\303\245";
    rc = lc_client_open_collection(f.client, &config, &f.collection, &f.error);
    REQUIRE(rc == LC_OK ||
            (rc == LC_ERR_INVALID && f.error.server_code &&
             !strcmp(f.error.server_code, "collection_not_found")));
    if (rc == LC_OK)
      read_entry(f.collection, 1U, 80U, &f.error);
    else
      clear(&f.error);
    if (!cut)
      REQUIRE(rc != LC_OK);
  } else {
    open_collection(&f, 0, &f.collection);
    if (operation == 1) {
      ok(lc_collection_append_status(f.collection, "new-append", &status,
                                     &f.error),
         &f.error);
      REQUIRE(status.state == LC_COLLECTION_APPEND_ABSENT ||
              status.state == LC_COLLECTION_APPEND_COMMITTED);
      if (!cut && sync)
        REQUIRE(status.state == LC_COLLECTION_APPEND_COMMITTED);
      if (status.state == LC_COLLECTION_APPEND_COMMITTED)
        read_entry(f.collection, status.entry.position, 73728U, &f.error);
      read_entry(f.collection, 1U, 80U, &f.error);
    } else {
      ok(lc_collection_edit_status(f.collection, "edit-cut", &receipt,
                                   &f.error),
         &f.error);
      if (!cut && sync)
        REQUIRE(receipt.found);
      read_entry(f.collection, receipt.found ? 2U : 1U,
                 receipt.found ? 512U : 80U, &f.error);
      ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
      REQUIRE(head.live_count == 1U &&
              head.upper_position == (receipt.found ? 2U : 1U));
    }
  }
  uint64_t reclaimed, skipped;
  ok(ps_root_reclaim_orphans(client_root(&f), &reclaimed, &skipped, &f.error),
     &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  REQUIRE(!unlink(f.path));
  free(m.stable);
  free(m.dirty);
  free(m.scratch);
  return calls;
}
static void io_failures(const char *directory, int codec, int sync) {
  size_t cuts = 0U;
  for (int operation = 0; operation < 4; ++operation) {
    size_t calls = io_trial(directory, codec, sync, operation, 0, 0U);
    REQUIRE(calls > 0U);
    for (int mode = 0; mode < 4; ++mode)
      for (size_t cut = 1U; cut <= calls; ++cut) {
        (void)io_trial(directory, codec, sync, operation, mode, cut);
        ++cuts;
      }
  }
  printf("SDK I/O codec=%d durable=%d cuts=%zu passed\n", codec, sync, cuts);
}
static size_t allocation_trial(const char *directory, int codec, int sync,
                               int operation, size_t cut) {
  fixture f;
  producer p;
  lc_collection_config config;
  lc_collection *extra = NULL;
  lc_collection_append_req req;
  lc_collection_append_res appended;
  lc_collection_append_status_res status;
  lc_collection_edit_req edit;
  lc_collection_edit_res edited;
  lc_collection_edit_status_res receipt;
  lc_collection_edit_range range = {0U, 1U};
  lc_collection_head_res head;
  lc_collection_lease_req lease;
  lc_collection_read_res read;
  consumer output;
  size_t listed_count = 0U;
  int rc;
  snprintf(scenario, sizeof(scenario),
           "allocation codec=%d sync=%d op=%d "
           "cut=%zu",
           codec, sync, operation, cut);
  setup(&f, directory, codec, sync, 1);
  (void)append(f.collection, "seed", 80U, &f.error);
  ok(lc_collection_head(f.collection, &head, &f.error), &f.error);
  lc_collection_config_init(&config);
  config.name = operation == 0 ? "created" : "events/\303\245";
  lc_collection_append_req_init(&req);
  req.append_id = operation == 1 ? "new-append" : "replacement";
  lc_collection_edit_req_init(&edit);
  edit.operation_id = "allocation-edit";
  edit.expected_head = head.etag;
  edit.ranges = &range;
  edit.range_count = 1U;
  edit.replacement = &req;
  lc_collection_lease_req_init(&lease);
  lease.owner = "allocation-test";
  producer_init(&p, operation == 1 ? 73728U : 512U);
  lc_collection_read_res_init(&read);
  memset(&output, 0, sizeof(output));
  output.size = 80U;
  output.sink.write = consume;
  output.sink.impl = &output;
  if (operation == 5) {
    close_store(&f);
    open_client(&f, 0);
  }
  allocation_calls = allocation_fired = 0;
  allocation_fail = cut;
  allocation_thread = pthread_self();
  __atomic_store_n(&allocation_armed, 1, __ATOMIC_RELEASE);
  if (operation == 0)
    rc = lc_client_create_collection(f.client, &config, &extra, &f.error);
  else if (operation == 1)
    rc = lc_collection_append_staged(f.collection, &req, &p.source, &appended,
                                     &f.error);
  else if (operation == 2)
    rc = lc_collection_edit(f.collection, &edit, &p.source, &edited, &f.error);
  else if (operation == 3)
    rc = lc_collection_acquire(f.collection, &lease, &extra, &f.error);
  else if (operation == 4)
    rc = lc_collection_read(f.collection, NULL, &output.sink, &read, &f.error);
  else if (operation == 5)
    rc = lc_client_open_collection(f.client, &config, &extra, &f.error);
  else
    rc = lc_client_list_collections(f.client, NULL, listed, &listed_count,
                                    &f.error);
  __atomic_store_n(&allocation_armed, 0, __ATOMIC_RELEASE);
  size_t calls = allocation_calls;
  if (cut) {
    if (!allocation_fired || rc == LC_OK)
      fprintf(stderr, "%s fired=%d rc=%d calls=%zu\n", scenario,
              allocation_fired, rc, calls);
    REQUIRE(allocation_fired && rc != LC_OK);
  } else
    ok(rc, &f.error);
  clear(&f.error);
  lc_collection_read_res_cleanup(&read);
  if (operation == 3 && extra) {
    ok(lc_collection_release(extra, &f.error), &f.error);
    extra = NULL;
  }
  lc_collection_close(extra);
  if (operation == 5)
    open_collection(&f, 0, &f.collection);
  if (operation == 1) {
    ok(lc_collection_append_status(f.collection, "new-append", &status,
                                   &f.error),
       &f.error);
    if (status.state == LC_COLLECTION_APPEND_COMMITTED)
      read_entry(f.collection, status.entry.position, 73728U, &f.error);
    else
      (void)append(f.collection, "new-append", 73728U, &f.error);
  }
  if (operation == 2) {
    ok(lc_collection_edit_status(f.collection, "allocation-edit", &receipt,
                                 &f.error),
       &f.error);
    read_entry(f.collection, receipt.found ? 2U : 1U,
               receipt.found ? 512U : 80U, &f.error);
  } else
    read_entry(f.collection, 1U, 80U, &f.error);
  reopen(&f);
  if (operation == 2) {
    ok(lc_collection_edit_status(f.collection, "allocation-edit", &receipt,
                                 &f.error),
       &f.error);
    read_entry(f.collection, receipt.found ? 2U : 1U,
               receipt.found ? 512U : 80U, &f.error);
  } else
    read_entry(f.collection, 1U, 80U, &f.error);
  uint64_t reclaimed, skipped;
  ok(ps_root_reclaim_orphans(client_root(&f), &reclaimed, &skipped, &f.error),
     &f.error);
  close_store(&f);
  lc_error_cleanup(&f.error);
  REQUIRE(!unlink(f.path));
  return calls;
}
static void allocation_failures(const char *directory, int codec, int sync) {
  size_t cuts = 0U;
  for (int operation = 0; operation < 7; ++operation) {
    size_t calls = allocation_trial(directory, codec, sync, operation, 0U);
    REQUIRE(calls > 0U);
    for (size_t cut = 1U; cut <= calls; ++cut) {
      (void)allocation_trial(directory, codec, sync, operation, cut);
      ++cuts;
    }
  }
  printf("SDK allocation codec=%d durable=%d cuts=%zu passed\n", codec, sync,
         cuts);
}
int main(int argc, char **argv) {
  char path[1024], directory[1024];
  lc_error e;
  REQUIRE(argc == 2 || (argc == 3 && !strcmp(argv[2], "live")) ||
          (argc == 5 &&
           (!strcmp(argv[2], "io") || !strcmp(argv[2], "allocation"))) ||
          (argc == 8 && !strcmp(argv[2], "io-one")));
  REQUIRE(snprintf(path, sizeof(path), "%s/pouch-collection-sdk-XXXXXX",
                   argv[1]) > 0);
  REQUIRE(lc_test_tmp_mkdtemp(path, directory, sizeof(directory), argv[1]));
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  if (argc == 8) {
    diagnostic = 1;
    (void)io_trial(directory, atoi(argv[3]), atoi(argv[4]), atoi(argv[5]),
                   atoi(argv[6]), (size_t)strtoul(argv[7], NULL, 10));
  } else if (argc == 5) {
    int codec = atoi(argv[3]), sync = atoi(argv[4]);
    REQUIRE(codec >= 0 && codec < 4 && sync >= 0 && sync < 2);
    if (!strcmp(argv[2], "allocation"))
      allocation_failures(directory, codec, sync);
    else
      io_failures(directory, codec, sync);
  } else {
    for (int codec = 0; codec < 4; ++codec)
      for (int sync = 0; sync < 2; ++sync) {
        lifecycle(directory, codec, sync);
        if (argc != 3)
          process_recovery(directory, codec, sync);
        edit_recovery(directory, codec, sync);
      }
    for (int sync = 0; sync < 2; ++sync) {
      concurrency(directory, sync);
      for (int codec = 0; codec < 4; ++codec)
        writer_handoff(directory, codec, sync);
    }
    reclamation(directory);
    checkpoint_corruption(directory);
  }
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(directory, argv[1]);
  printf("collection SDK checks=%zu\n", checks);
  return 0;
}
