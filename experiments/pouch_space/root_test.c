#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "root.h"
#include "space_internal.h"
#include "stream.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocation, allocation_fail;
typedef struct allocation_gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int entered, released;
} allocation_gate;
static pthread_key_t thread_gate;
static int thread_gate_ready;
static int allocation_fails(void) {
  return allocation_fail && ++allocation == allocation_fail;
}
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return allocation_fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t bytes) {
  /* Pause/fail the unpublished catalog entry allocation. This syscall-free
   * fault seam runs after the root registry lock is released, so same-name
   * waiters can arrive before the initializer fails. Assertions below concern
   * namespace admission and durable identities, not allocator layout. */
  allocation_gate *gate =
      thread_gate_ready && n == 1 && bytes == sizeof(ps_entry)
          ? pthread_getspecific(thread_gate)
          : NULL;
  if (gate) {
    REQUIRE(pthread_setspecific(thread_gate, NULL) == 0);
    pthread_mutex_lock(&gate->mutex);
    gate->entered = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->released)
      pthread_cond_wait(&gate->changed, &gate->mutex);
    pthread_mutex_unlock(&gate->mutex);
    return NULL;
  }
  return allocation_fails() ? NULL
                            : __real_lc_calloc_with_allocator(a, n, bytes);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return allocation_fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  return allocation_fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "root error %d: %s\n", rc, e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static ps_namespace *acquire(ps_root *r, const char *name, int create,
                             lc_error *e) {
  ps_namespace *ns = NULL;
  ok(ps_root_namespace_acquire(r, name, create, &ns, e), e);
  REQUIRE(ns != NULL);
  return ns;
}
static void same(const ps_namespace_info *a, const ps_namespace_info *b) {
  REQUIRE(a->owner == b->owner &&
          a->incarnation.incarnation == b->incarnation.incarnation &&
          !memcmp(a->incarnation.container, b->incarnation.container, 16));
}
static int count_ns(const char *name, const ps_namespace_info *info, void *v,
                    lc_error *e) {
  (void)e;
  REQUIRE(*name && info->owner && info->incarnation.incarnation);
  ++*(size_t *)v;
  return LC_OK;
}
static void count(ps_root *r, size_t expected, lc_error *e) {
  size_t n = 0;
  ok(ps_root_namespace_visit(r, count_ns, &n, e), e);
  REQUIRE(n == expected);
}
typedef struct sink {
  lc_sink pub;
  ps_stream_writer *writer;
  size_t bytes;
} sink;
static int output(lc_sink *pub, const void *p, size_t n, lc_error *e) {
  sink *s = pub->impl;
  int rc = ps_stream_write(s->writer, p, n, e);
  if (rc == LC_OK)
    s->bytes += n;
  return rc == LC_OK;
}
typedef struct encoded {
  lc_source pub;
  ps_stream_reader *reader;
  uint64_t at, bytes;
} encoded;
static size_t input(lc_source *pub, void *p, size_t n, lc_error *e) {
  encoded *s = pub->impl;
  if (n > s->bytes - s->at)
    n = (size_t)(s->bytes - s->at);
  if (ps_stream_read_at(s->reader, s->at, p, n, e) != LC_OK)
    return 0;
  s->at += n;
  return n;
}
static int reset(lc_source *pub, lc_error *e) {
  (void)e;
  ((encoded *)pub->impl)->at = 0;
  return LC_OK;
}
static void read_body(ps_root *root, ps_stream *stream, const char *descriptor,
                      const unsigned char *body, size_t bytes, uint64_t stored,
                      lc_error *e) {
  lc_pouch_crypto_reader *codec = NULL;
  lc_source *decoded = NULL;
  encoded source = {0};
  unsigned char buffer[2048];
  size_t at = 0, n;
  source.pub.impl = &source;
  source.pub.read = input;
  source.pub.reset = reset;
  source.bytes = stored;
  ok(ps_stream_read_open(stream, 0, stored, &source.reader, e), e);
  ok(lc_pouch_crypto_reader_open(ps_root_crypto(root), &codec, e), e);
  ok(lc_pouch_crypto_reader_bind_source(codec, "root-test", "root-test",
                                        &source.pub, stored, descriptor,
                                        &decoded, e),
     e);
  do {
    n = decoded->read(decoded, buffer, sizeof(buffer), e);
    REQUIRE(e->code == LC_OK);
    REQUIRE(n <= bytes - at && !memcmp(buffer, body + at, n));
    at += n;
  } while (n);
  REQUIRE(at == bytes);
  lc_pouch_crypto_reader_close(codec);
  ps_stream_reader_close(source.reader);
}
static void lifecycle(const char *path, const char *key_string,
                      const char *wrong_key) {
  ps_root *r = NULL, *other = NULL;
  ps_root_options options = {0}, mismatch;
  ps_namespace *a, *b, *again;
  ps_namespace_info first, latest;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  ps_stream_reader *pin = NULL;
  ps_stream_info stream_info;
  lc_pouch_crypto_writer *codec = NULL;
  lc_error e;
  ps_key stream_key = {0, 1, PS_STREAM};
  unsigned char *body = malloc(1048601);
  uint32_t random = 17;
  REQUIRE(body != NULL);
  for (size_t i = 0; i < 1048601; ++i) {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    body[i] = (unsigned char)random;
  }
  lc_error_init(&e);
  REQUIRE(ps_root_open(NULL, 1, NULL, &r, &e) == LC_ERR_INVALID && r == NULL);
  clear(&e);
  REQUIRE(ps_root_open(path, 1, NULL, NULL, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(ps_root_space(NULL) == NULL && ps_root_crypto(NULL) == NULL);
  REQUIRE(ps_check(NULL, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_root_close(NULL, &e), &e);
  ps_namespace_release(NULL);
  for (int transform = 0; transform < 4; ++transform) {
    char *descriptor = NULL;
    uint64_t plain, stored;
    unsigned long crc;
    options.crypto.key_string = transform & 2 ? key_string : NULL;
    options.crypto.compression_enabled = transform & 1;
    options.compression_explicit = 1;
    ok(ps_root_open(path, 1, &options, &r, &e), &e);
    REQUIRE(ps_root_namespace_acquire(r, "", 1, &again, &e) == LC_ERR_INVALID &&
            again == NULL);
    clear(&e);
    REQUIRE(ps_root_namespace_acquire(r, "alpha", 1, NULL, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_namespace_identity(NULL, &first, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_root_namespace_visit(r, NULL, NULL, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(lc_pouch_crypto_enabled(ps_root_crypto(r)) == !!(transform & 2));
    REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(r)) ==
            !!(transform & 1));
    ps_stats stats;
    ok(ps_status(ps_root_space(r), &stats, &e), &e);
    REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY);
    REQUIRE(ps_root_open(path, 1, &options, &other, &e) != LC_OK &&
            other == NULL);
    clear(&e);
    REQUIRE(ps_root_open(path, 0, &options, &other, &e) != LC_OK &&
            other == NULL);
    clear(&e);
    REQUIRE(ps_root_namespace_acquire(r, "missing", 0, &again, &e) ==
                LC_ERR_INVALID &&
            again == NULL);
    clear(&e);
    a = acquire(r, "alpha", 1, &e);
    again = acquire(r, "alpha", 1, &e);
    REQUIRE(a == again);
    ok(ps_namespace_identity(a, &first, &e), &e);
    ps_namespace_release(again);
    b = acquire(r, "../a/b\377", 1, &e);
    ok(ps_namespace_identity(b, &latest, &e), &e);
    REQUIRE(first.owner != latest.owner);
    count(r, 2, &e);
    ps_namespace_release(b);
    REQUIRE(ps_root_close(r, &e) == LC_ERR_INVALID);
    clear(&e);
    /* Cached admission must require no allocation or physical I/O. */
    allocation = 0;
    allocation_fail = 1;
    int rc = ps_root_namespace_acquire(r, "alpha", 0, &again, &e);
    allocation_fail = 0;
    ok(rc, &e);
    REQUIRE(allocation == 0 && again == a);
    ps_namespace_release(again);
    stream_key.owner = first.owner;
    ok(ps_stream_open(ps_root_space(r), NULL, stream_key, 1, NULL, NULL, NULL,
                      &stream, &e),
       &e);
    ok(ps_stream_begin(stream, &writer, &e), &e);
    sink destination = {0};
    destination.writer = writer;
    destination.pub.impl = &destination;
    destination.pub.write = output;
    ok(lc_pouch_crypto_writer_open_sink(ps_root_crypto(r), "root-test",
                                        &destination.pub, &codec, &e),
       &e);
    lc_sink *encoded_output = lc_pouch_crypto_writer_sink(codec);
    for (size_t at = 0; at < 1048601;) {
      size_t n = 1048601 - at;
      if (n > 4096)
        n = 4096;
      REQUIRE(encoded_output->write(encoded_output, body + at, n, &e));
      at += n;
    }
    REQUIRE(destination.bytes > 0);
    ok(lc_pouch_crypto_writer_finish(codec, &plain, &stored, &crc, &descriptor,
                                     &e),
       &e);
    REQUIRE(plain == 1048601 && stored == destination.bytes);
    lc_pouch_crypto_writer_close(codec);
    codec = NULL;
    ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
    ps_stream_writer_close(writer);
    writer = NULL;
    ok(ps_stream_seal(stream, &e), &e);
    ok(ps_stream_status(stream, &stream_info, &e), &e);
    ps_namespace_release(a);
    REQUIRE(ps_root_close(r, &e) == LC_ERR_INVALID);
    clear(&e); /* raw child still pins root */
    read_body(r, stream, descriptor, body, 1048601, stored, &e);
    ok(ps_stream_read_open(stream, 0, 1, &pin, &e), &e);
    REQUIRE(ps_root_close(r, &e) == LC_ERR_INVALID);
    clear(&e);
    ps_stream_reader_close(pin);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_root_close(r, &e), &e);
    mismatch = options;
    mismatch.crypto.key_string = transform & 2 ? wrong_key : key_string;
    REQUIRE(ps_root_open(path, 0, &mismatch, &r, &e) == LC_ERR_INVALID &&
            r == NULL);
    clear(&e);
    mismatch = options;
    mismatch.crypto.compression_enabled = !(transform & 1);
    REQUIRE(ps_root_open(path, 0, &mismatch, &r, &e) == LC_ERR_UNSUPPORTED &&
            r == NULL);
    clear(&e);
    options.compression_explicit = 0;
    options.crypto.compression_enabled = !(transform & 1);
    ok(ps_root_open(path, 0, &options, &r, &e), &e);
    REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(r)) ==
            !!(transform & 1));
    a = acquire(r, "alpha", 0, &e);
    ok(ps_namespace_identity(a, &latest, &e), &e);
    same(&first, &latest);
    ok(ps_stream_open(ps_root_space(r), NULL, stream_key, 0, &stream_info.ref,
                      NULL, NULL, &stream, &e),
       &e);
    read_body(r, stream, descriptor, body, 1048601, stored, &e);
    ok(ps_stream_close(stream, &e), &e);
    ps_namespace_release(a);
    count(r, 2, &e);
    ok(ps_root_close(r, &e), &e);
    REQUIRE(unlink(path) == 0);
    lc_free_with_allocator(NULL, descriptor);
  }
  free(body);
  lc_error_cleanup(&e);
}
static void raw_config(ps_space *space, const unsigned char *bytes,
                       size_t count, lc_error *e) {
  ps_key k = {0, 1, PS_CONTROL};
  ps_writer *w = NULL;
  ok(ps_begin(space, NULL, k, &w, e), e);
  ok(ps_write(w, bytes, count, e), e);
  ok(ps_publish(w, NULL, e), e);
  ps_writer_close(w);
}
static void namespace_records(const char *path) {
  lc_error e;
  lc_error_init(&e);
  ps_root *root;
  ps_catalog_info first, next, held, temporary;
  ps_catalog_view *view = NULL;
  ps_stats before, after;
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ps_space *space = ps_root_space(root);
  ok(ps_status(space, &before, &e), &e);
  ok(ps_catalog_create_namespace(space, "slot", 4, &first, &e), &e);
  ok(ps_status(space, &after, &e), &e);
  REQUIRE(after.syncs == before.syncs + 1 &&
          after.entries == before.entries + 1);
  REQUIRE(first.id == first.revision.incarnation && !first.stream.kind &&
          !first.stream.owner && !first.stream.key &&
          !first.target.incarnation);
  for (unsigned i = 0; i < sizeof(first.target.container); ++i)
    REQUIRE(first.target.container[i] == 0);
  REQUIRE(ps_catalog_create_namespace(space, "slot", 4, &temporary, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  ok(ps_catalog_get(space, 0, PS_CATALOG_NAMESPACE, "slot", 4, &view, &e), &e);
  REQUIRE(view != NULL);
  ok(ps_catalog_remove(space, 0, PS_CATALOG_NAMESPACE, "slot", 4,
                       &first.revision, &e),
     &e);
  ok(ps_catalog_create_namespace(space, "slot", 4, &next, &e), &e);
  REQUIRE(next.id > first.id);
  ok(ps_checkpoint(space, &e), &e);
  ok(ps_catalog_view_info(view, &held, &e), &e);
  REQUIRE(held.id == first.id &&
          held.revision.incarnation == first.revision.incarnation);
  REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
  clear(&e);
  ps_catalog_view_close(view);
  REQUIRE(ps_catalog_remove(space, 0, PS_CATALOG_NAMESPACE, "slot", 4,
                            &first.revision, &e) == LC_ERR_INVALID);
  clear(&e);
  ps_stream *stream = NULL;
  ps_stream_info info;
  ps_key key = {900, 1, PS_STREAM};
  ok(ps_stream_open(space, NULL, key, 1, NULL, NULL, NULL, &stream, &e), &e);
  ok(ps_stream_status(stream, &info, &e), &e);
  REQUIRE(ps_catalog_put(space, NULL, 0, PS_CATALOG_NAMESPACE, "slot", 4,
                         &next.revision, key, &info.ref, &temporary,
                         &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_stream_retire(stream, &e), &e);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_checkpoint(space, &e), &e);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  ps_namespace *ns = acquire(root, "slot", 0, &e);
  ps_namespace_info identity;
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  REQUIRE(identity.owner == next.id);
  ps_namespace_release(ns);
  count(root, 1, &e);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void malformed(const char *path) {
  ps_root *root = NULL;
  ps_space *space = NULL;
  ps_reader *reader;
  ps_key config = {0, 1, PS_CONTROL};
  unsigned char original[128], bad[129];
  size_t got;
  lc_error e;
  lc_error_init(&e);
  ok(ps_open(path, 1, NULL, &space, &e), &e);
  ok(ps_close(space, &e), &e);
  REQUIRE(ps_root_open(path, 0, NULL, &root, &e) == LC_ERR_PROTOCOL &&
          root == NULL);
  clear(&e);
  REQUIRE(unlink(path) == 0);
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ok(ps_read_open(ps_root_space(root), config, NULL, &reader, &e), &e);
  ok(ps_read(reader, original, sizeof(original), &got, &e), &e);
  REQUIRE(got == sizeof(original));
  ps_reader_close(reader);
  ok(ps_root_close(root, &e), &e);
  for (size_t mutation = 0; mutation < 142; ++mutation) {
    memcpy(bad, original, sizeof(original));
    bad[128] = 0;
    size_t bytes = 128;
    if (mutation < 128)
      bad[mutation] ^= 1;
    else {
      static const unsigned offsets[] = {0,  8,  9,   10,  15,  16,
                                         95, 96, 100, 104, 123, 16};
      if (mutation < 140) {
        size_t field = mutation - 128;
        bad[offsets[field]] = field == 1 || field == 2 ? 2 : 1;
        if (field == 11) {
          bad[8] = 1;
          memset(bad + 16, 'a', 80);
        }
        uint32_t crc = (uint32_t)crc32(0L, bad, 124);
        for (unsigned i = 0; i < 4; ++i)
          bad[124 + i] = (unsigned char)(crc >> (8 * i));
      } else
        bytes = mutation == 140 ? 127 : 129;
    }
    ok(ps_open(path, 0, NULL, &space, &e), &e);
    raw_config(space, bad, bytes, &e);
    ok(ps_close(space, &e), &e);
    REQUIRE(ps_root_open(path, 0, NULL, &root, &e) == LC_ERR_PROTOCOL &&
            root == NULL);
    clear(&e);
  }
  REQUIRE(unlink(path) == 0);
  for (int variant = 0; variant < 6; ++variant) {
    ok(ps_root_open(path, 1, NULL, &root, &e), &e);
    ps_stream *anchor = NULL;
    ps_stream_writer *writer = NULL;
    ps_stream_info info;
    ps_key k = {variant == 3 ? 0 : 77, variant == 2 ? 1 : 0, PS_STREAM};
    ps_catalog_info binding;
    ok(ps_stream_open(ps_root_space(root), NULL, k, 1, NULL, NULL, NULL,
                      &anchor, &e),
       &e);
    if (variant == 1) {
      ok(ps_stream_begin(anchor, &writer, &e), &e);
      ok(ps_stream_write(writer, "data", 4, &e), &e);
      ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
      ps_stream_writer_close(writer);
    }
    if (variant != 0)
      ok(ps_stream_seal(anchor, &e), &e);
    ok(ps_stream_status(anchor, &info, &e), &e);
    const char *name = variant == 5 ? "" : "bad\0suffix";
    size_t length = variant == 5 ? 0 : variant == 4 ? 10 : 3;
    ok(ps_catalog_put(ps_root_space(root), NULL, 0, PS_CATALOG_NAMESPACE, name,
                      length, NULL, k, &info.ref, &binding, &e),
       &e);
    ok(ps_stream_close(anchor, &e), &e);
    ps_namespace *ns = NULL;
    if (variant < 4)
      REQUIRE(ps_root_namespace_acquire(root, "bad", 0, &ns, &e) ==
                  LC_ERR_PROTOCOL &&
              ns == NULL);
    else {
      size_t visited = 0;
      REQUIRE(ps_root_namespace_visit(root, count_ns, &visited, &e) ==
              LC_ERR_PROTOCOL);
      REQUIRE(visited == 0);
    }
    clear(&e);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
typedef struct visit_state {
  ps_root *root;
  size_t count;
  int fail, create;
} visit_state;
static int reentrant(const char *name, const ps_namespace_info *identity,
                     void *v, lc_error *e) {
  visit_state *state = v;
  ps_namespace_info now;
  ps_namespace *ns = acquire(state->root, name, 0, e);
  ok(ps_namespace_identity(ns, &now, e), e);
  same(identity, &now);
  ps_namespace_release(ns);
  REQUIRE(ps_root_close(state->root, e) == LC_ERR_INVALID);
  clear(e);
  if (state->create && !state->count) {
    ns = acquire(state->root, "added", 1, e);
    ps_namespace_release(ns);
  }
  ++state->count;
  return state->fail ? lc_error_set(e, LC_ERR_INVALID, 0, "stop visit", NULL,
                                    NULL, NULL)
                     : LC_OK;
}
static void catalog(const char *path) {
  ps_root *root;
  ps_namespace *ns;
  lc_error e;
  char name[64];
  lc_error_init(&e);
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  for (unsigned i = 0; i < 500; ++i) {
    snprintf(name, sizeof(name), "ns-%u", i);
    ns = acquire(root, name, 1, &e);
    ps_namespace_release(ns);
  }
  count(root, 500, &e);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  visit_state state = {root, 0, 0, 1};
  ok(ps_root_namespace_visit(root, reentrant, &state, &e), &e);
  REQUIRE(state.count == 500);
  count(root, 501, &e);
  state.count = 0;
  state.fail = 1;
  REQUIRE(ps_root_namespace_visit(root, reentrant, &state, &e) ==
          LC_ERR_INVALID);
  REQUIRE(state.count == 1);
  clear(&e);
  count(root, 501, &e);
  char *large = malloc(PS_CATALOG_NAME_MAX + 2);
  REQUIRE(large != NULL);
  memset(large, 'n', PS_CATALOG_NAME_MAX);
  large[PS_CATALOG_NAME_MAX] = 0;
  ns = acquire(root, large, 1, &e);
  ps_namespace_info before, after;
  ok(ps_namespace_identity(ns, &before, &e), &e);
  ps_namespace_release(ns);
  large[PS_CATALOG_NAME_MAX] = 'x';
  large[PS_CATALOG_NAME_MAX + 1] = 0;
  REQUIRE(ps_root_namespace_acquire(root, large, 1, &ns, &e) ==
              LC_ERR_INVALID &&
          ns == NULL);
  clear(&e);
  large[PS_CATALOG_NAME_MAX] = 0;
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  ns = acquire(root, large, 0, &e);
  ok(ps_namespace_identity(ns, &after, &e), &e);
  same(&before, &after);
  ps_namespace_release(ns);
  count(root, 502, &e);
  free(large);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}

typedef struct model {
  unsigned char *stable;
  size_t stable_bytes, calls, fail_at, reads, fail_read;
  int tear, persist_torn, hold, entered, released, publications;
  int fail_held_sync;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} model;
static int fail(model *m) {
  return __atomic_add_fetch(&m->calls, 1, __ATOMIC_RELAXED) == m->fail_at;
}
static ssize_t rd(void *v, int fd, void *p, size_t n, off_t at) {
  model *m = v;
  if (__atomic_add_fetch(&m->reads, 1, __ATOMIC_RELAXED) == m->fail_read) {
    errno = EIO;
    return -1;
  }
  return pread(fd, p, n, at);
}
static ssize_t wr(void *v, int fd, const void *p, size_t n, off_t at) {
  model *m = v;
  if (fail(m)) {
    if (m->tear && n > 1) {
      if (pwrite(fd, p, n / 2, at) != (ssize_t)(n / 2))
        abort();
      if (m->persist_torn && (uint64_t)at + n / 2 <= PS_MIB)
        memcpy(m->stable + at, p, n / 2);
    }
    errno = EIO;
    return -1;
  }
  ssize_t written = pwrite(fd, p, n, at);
  if (written == (ssize_t)n && at >= PS_JOURNAL_OFFSET && at < PS_DATA) {
    pthread_mutex_lock(&m->mutex);
    ++m->publications;
    pthread_cond_broadcast(&m->cond);
    pthread_mutex_unlock(&m->mutex);
  }
  return written;
}
static int sync_file(void *v, int fd) {
  model *m = v;
  int failed = fail(m);
  struct stat st;
  pthread_mutex_lock(&m->mutex);
  if (m->hold && !m->entered) {
    m->entered = 1;
    pthread_cond_broadcast(&m->cond);
    while (!m->released)
      pthread_cond_wait(&m->cond, &m->mutex);
    if (m->fail_held_sync)
      failed = 1;
  }
  pthread_mutex_unlock(&m->mutex);
  if (failed) {
    errno = EIO;
    return -1;
  }
  if (fdatasync(fd) != 0)
    return -1;
  if (m->stable) {
    if (fstat(fd, &st) != 0 || st.st_size > (off_t)PS_MIB)
      abort();
    m->stable_bytes = (size_t)st.st_size;
    if (pread(fd, m->stable, m->stable_bytes, 0) != (ssize_t)m->stable_bytes)
      abort();
  }
  return 0;
}
static int reserve(void *v, int fd, off_t at, off_t n) {
  model *m = v;
  if (fail(m)) {
    errno = ENOSPC;
    return -1;
  }
  return fallocate(fd, 0, at, n);
}
static void model_init(model *m) {
  memset(m, 0, sizeof(*m));
  m->stable = calloc(1, PS_MIB);
  REQUIRE(m->stable != NULL);
  REQUIRE(pthread_mutex_init(&m->mutex, NULL) == 0);
  REQUIRE(pthread_cond_init(&m->cond, NULL) == 0);
}
static void model_close(model *m) {
  free(m->stable);
  REQUIRE(pthread_cond_destroy(&m->cond) == 0);
  REQUIRE(pthread_mutex_destroy(&m->mutex) == 0);
}
static void crash(model *m, const char *path) {
  int fd = open(path, O_RDWR);
  REQUIRE(fd >= 0);
  REQUIRE(ftruncate(fd, (off_t)m->stable_bytes) == 0);
  if (m->stable_bytes) {
    REQUIRE(pwrite(fd, m->stable, m->stable_bytes, 0) ==
            (ssize_t)m->stable_bytes);
    REQUIRE(fallocate(fd, 0, 0, (off_t)m->stable_bytes) == 0);
  }
  REQUIRE(fdatasync(fd) == 0);
  REQUIRE(close(fd) == 0);
  m->calls = m->fail_at = 0;
}
static void io_faults(const char *path) {
  model m;
  model_init(&m);
  lc_error e;
  lc_error_init(&e);
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.io = &io;
  for (int phase = 0; phase < 3; ++phase) {
    size_t operations = 0;
    for (int mode = 0; mode < 3; ++mode)
      for (size_t at = 0; at <= operations; ++at) {
        if (mode && !at)
          continue;
        ps_root *root = NULL;
        ps_namespace *ns = NULL;
        int rc;
        m.calls = m.fail_at = m.stable_bytes = 0;
        m.tear = m.persist_torn = 0;
        memset(m.stable, 0, PS_MIB);
        if (phase) {
          ok(ps_root_open(path, 1, &options, &root, &e), &e);
          ns = acquire(root, "kept", 1, &e);
          ps_namespace_release(ns);
          ok(ps_checkpoint(ps_root_space(root), &e), &e);
          if (phase == 2) {
            ok(ps_root_close(root, &e), &e);
            root = NULL;
          }
        }
        m.calls = 0;
        m.fail_at = at;
        m.tear = mode > 0;
        m.persist_torn = mode == 2;
        if (phase == 1)
          rc = ps_root_namespace_acquire(root, "new", 1, &ns, &e);
        else
          rc = ps_root_open(path, !phase, &options, &root, &e);
        if (!at) {
          ok(rc, &e);
          operations = m.calls;
          REQUIRE(operations > 0);
        } else
          REQUIRE(rc != LC_OK);
        m.fail_at = 0;
        clear(&e);
        if (phase == 1 && rc != LC_OK) {
          ps_namespace *cached = NULL;
          REQUIRE(ps_root_namespace_acquire(root, "kept", 0, &cached, &e) ==
                      LC_ERR_TRANSPORT &&
                  cached == NULL);
          clear(&e);
        }
        if (phase == 1 && rc == LC_OK)
          ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        if (access(path, F_OK) == 0) {
          crash(&m, path);
          rc = ps_root_open(path, 0, &options, &root, &e);
          if (phase || !at) {
            ok(rc, &e);
            count(root, phase ? (phase == 1 && !at ? 2 : 1) : 0, &e);
            ok(ps_root_close(root, &e), &e);
          } else {
            REQUIRE(rc != LC_OK && root == NULL);
            clear(&e);
          }
          REQUIRE(unlink(path) == 0);
        } else
          REQUIRE(at != 0 && phase == 0);
      }
  }
  model_close(&m);
  lc_error_cleanup(&e);
}
static int no_callback(const char *name, const ps_namespace_info *id, void *v,
                       lc_error *e) {
  (void)name;
  (void)id;
  (void)v;
  (void)e;
  return LC_OK;
}
static void read_faults(const char *path) {
  model m;
  model_init(&m);
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.io = &io;
  lc_error e;
  lc_error_init(&e);
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "kept", 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  unsigned char *fixture = malloc(PS_MIB);
  REQUIRE(fixture != NULL && m.stable_bytes == PS_MIB);
  memcpy(fixture, m.stable, PS_MIB);
  size_t reads = 0;
  for (size_t at = 0; at <= reads; ++at) {
    memcpy(m.stable, fixture, PS_MIB);
    m.stable_bytes = PS_MIB;
    crash(&m, path);
    m.reads = 0;
    m.fail_read = at;
    int rc = ps_root_open(path, 0, &options, &root, &e);
    if (!at) {
      ok(rc, &e);
      reads = m.reads;
      REQUIRE(reads > 0);
      ok(ps_root_close(root, &e), &e);
    } else {
      REQUIRE(rc == LC_ERR_TRANSPORT && root == NULL);
      clear(&e);
    }
    m.fail_read = 0;
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    count(root, 1, &e);
    ok(ps_root_close(root, &e), &e);
  }
  free(fixture);
  REQUIRE(unlink(path) == 0);
  model_close(&m);
  lc_error_cleanup(&e);
}
static void allocations(const char *path, const char *key_string) {
  lc_error e;
  lc_error_init(&e);
  for (int phase = 0; phase < 8; ++phase) {
    size_t allocations = 0;
    for (size_t at = 0; at <= allocations; ++at) {
      ps_root *root = NULL;
      ps_namespace *ns = NULL;
      ps_root_options options = {0};
      int creating = phase == 0 || phase == 6;
      int reopening = phase == 1 || phase == 5 || phase == 7;
      int rc;
      options.crypto.key_string = phase >= 6 ? key_string : NULL;
      if (!creating) {
        options.crypto.compression_enabled = phase == 5;
        ok(ps_root_open(path, 1, &options, &root, &e), &e);
        unsigned n = phase == 3 ? 24 : 1;
        for (unsigned i = 0; i < n; ++i) {
          char name[32];
          snprintf(name, sizeof(name), "kept-%u", i);
          ns = acquire(root, name, 1, &e);
          ps_namespace_release(ns);
        }
        if (reopening) {
          ok(ps_root_close(root, &e), &e);
          root = NULL;
        }
      }
      allocation = 0;
      allocation_fail = at ? at : SIZE_MAX;
      options.crypto.compression_enabled = 0;
      if (creating || reopening)
        rc = ps_root_open(path, creating, &options, &root, &e);
      else if (phase == 4)
        rc = ps_root_namespace_visit(root, no_callback, NULL, &e);
      else
        rc = ps_root_namespace_acquire(root, "new", 1, &ns, &e);
      size_t used = allocation;
      allocation_fail = 0;
      if (!at) {
        ok(rc, &e);
        allocations = used;
        REQUIRE(used > 0);
      } else
        REQUIRE(rc == LC_ERR_NOMEM);
      clear(&e);
      if (root) {
        if ((phase == 2 || phase == 3) && rc == LC_OK)
          ps_namespace_release(ns);
        if (!creating && !reopening) {
          ok(ps_checkpoint(ps_root_space(root), &e), &e);
          count(root, (phase == 3 ? 24 : 1) + (rc == LC_OK && phase != 4), &e);
        }
        ok(ps_root_close(root, &e), &e);
      }
      if (reopening) {
        ok(ps_root_open(path, 0, &options, &root, &e), &e);
        REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(root)) ==
                (phase == 5));
        count(root, 1, &e);
        ok(ps_root_close(root, &e), &e);
      }
      if (access(path, F_OK) == 0)
        REQUIRE(unlink(path) == 0);
    }
  }
  lc_error_cleanup(&e);
}
typedef struct actor {
  ps_root *root;
  const char *name;
  ps_namespace *ns;
  int rc;
  allocation_gate *fail_gate;
} actor;
static void *create_ns(void *v) {
  actor *a = v;
  lc_error e;
  lc_error_init(&e);
  if (a->fail_gate)
    REQUIRE(pthread_setspecific(thread_gate, a->fail_gate) == 0);
  a->rc = ps_root_namespace_acquire(a->root, a->name, 1, &a->ns, &e);
  REQUIRE(pthread_setspecific(thread_gate, NULL) == 0);
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(const char *path, int same_name, int durable,
                        int fault) {
  model m;
  model_init(&m);
  free(m.stable);
  m.stable = NULL;
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.storage.io = &io;
  options.storage.durable_sync = durable;
  ps_root *root;
  lc_error e;
  lc_error_init(&e);
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  actor actors[4] = {{root, "first", NULL, 0, NULL},
                     {root, "second", NULL, 0, NULL},
                     {root, "third", NULL, 0, NULL},
                     {root, "fourth", NULL, 0, NULL}};
  pthread_t threads[4];
  struct timespec deadline;
  REQUIRE(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
  deadline.tv_sec += 10;
  allocation_gate gate = {0};
  if (fault == 2) {
    REQUIRE(pthread_mutex_init(&gate.mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&gate.changed, NULL) == 0);
    actors[0].fail_gate = &gate;
  }
  m.hold = fault != 2;
  m.fail_held_sync = fault == 1;
  m.publications = 0;
  REQUIRE(pthread_create(&threads[0], NULL, create_ns, &actors[0]) == 0);
  if (fault == 2) {
    pthread_mutex_lock(&gate.mutex);
    while (!gate.entered)
      REQUIRE(pthread_cond_timedwait(&gate.changed, &gate.mutex, &deadline) ==
              0);
    pthread_mutex_unlock(&gate.mutex);
  } else {
    pthread_mutex_lock(&m.mutex);
    while (!m.entered)
      REQUIRE(pthread_cond_timedwait(&m.cond, &m.mutex, &deadline) == 0);
    pthread_mutex_unlock(&m.mutex);
  }
  for (unsigned i = 1; i < 4; ++i) {
    if (same_name)
      actors[i].name = "first";
    REQUIRE(pthread_create(&threads[i], NULL, create_ns, &actors[i]) == 0);
  }
  if (!same_name) {
    pthread_mutex_lock(&m.mutex);
    while (m.publications < 4)
      REQUIRE(pthread_cond_timedwait(&m.cond, &m.mutex, &deadline) == 0);
    pthread_mutex_unlock(&m.mutex);
  }
  REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
  clear(&e);
  count(root, 0, &e); /* Catalog binding waits for its durable prerequisite. */
  if (fault == 2) {
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.changed);
    pthread_mutex_unlock(&gate.mutex);
  } else {
    pthread_mutex_lock(&m.mutex);
    m.released = 1;
    pthread_cond_broadcast(&m.cond);
    pthread_mutex_unlock(&m.mutex);
  }
  ps_namespace *shared = NULL;
  for (unsigned i = 0; i < 4; ++i) {
    REQUIRE(pthread_join(threads[i], NULL) == 0);
    if (fault == 1 || (fault == 2 && !i)) {
      REQUIRE(actors[i].rc == (fault == 1 ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
      REQUIRE(actors[i].ns == NULL);
      continue;
    }
    ok(actors[i].rc, &e);
    if (same_name) {
      if (shared)
        REQUIRE(actors[i].ns == shared);
      shared = actors[i].ns;
    }
    ps_namespace_release(actors[i].ns);
  }
  if (fault == 1) {
    ok(ps_root_close(root, &e), &e);
    m.fail_held_sync = 0;
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    size_t visible = 0;
    ok(ps_root_namespace_visit(root, count_ns, &visible, &e), &e);
    REQUIRE(visible <= 1); /* Failed sync permits a complete uncertain owner. */
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    model_close(&m);
    lc_error_cleanup(&e);
    return;
  }
  count(root, same_name ? 1 : 4, &e);
  if (fault == 2) {
    ps_stats stats;
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    ok(ps_status(ps_root_space(root), &stats, &e), &e);
    REQUIRE(stats.entries == 2);
    REQUIRE(pthread_cond_destroy(&gate.changed) == 0);
    REQUIRE(pthread_mutex_destroy(&gate.mutex) == 0);
  }
  size_t calls = m.calls, reads = m.reads;
  m.fail_at = calls + 1;
  m.fail_read = reads + 1;
  ps_namespace *cached = acquire(root, "first", 0, &e);
  REQUIRE(m.calls == calls && m.reads == reads);
  ps_namespace_release(cached);
  m.fail_at = m.fail_read = 0;
  ok(ps_root_close(root, &e), &e);
  REQUIRE(m.calls == calls && m.reads == reads);
  REQUIRE(unlink(path) == 0);
  model_close(&m);
  lc_error_cleanup(&e);
}
static void death(const char *path) {
  ps_root *root;
  lc_error e;
  int channel[2], status;
  unsigned char byte;
  lc_error_init(&e);
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(pipe(channel) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    close(channel[0]);
    if (ps_root_open(path, 0, NULL, &root, &e) != LC_OK)
      _exit(2);
    ps_namespace *ns = NULL;
    if (ps_root_namespace_acquire(root, "child", 1, &ns, &e) != LC_OK ||
        write(channel[1], "x", 1) != 1)
      _exit(3);
    for (;;)
      pause();
  }
  REQUIRE(close(channel[1]) == 0);
  REQUIRE(read(channel[0], &byte, 1) == 1);
  REQUIRE(close(channel[0]) == 0);
  REQUIRE(kill(child, SIGKILL) == 0);
  REQUIRE(waitpid(child, &status, 0) == child && WIFSIGNALED(status) &&
          WTERMSIG(status) == SIGKILL);
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  count(root, 1, &e);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct run {
  const char *path, *mode, *key, *wrong_key;
} run;
static void *run_tests(void *v) {
  run *r = v;
  if (!strcmp(r->mode, "lifecycle")) {
    lifecycle(r->path, r->key, r->wrong_key);
    namespace_records(r->path);
    malformed(r->path);
    catalog(r->path);
  } else if (!strcmp(r->mode, "faults")) {
    io_faults(r->path);
    read_faults(r->path);
    allocations(r->path, r->key);
  } else {
    for (int durable = 0; durable < 2; ++durable) {
      concurrency(r->path, 0, durable, 0);
      for (int fault = 0; fault < 3; ++fault)
        concurrency(r->path, 1, durable, fault);
    }
    death(r->path);
  }
  return NULL;
}
int main(int argc, char **argv) {
  char root[1024], prefix[1024], temporary[1100], path[1100];
  lc_error e;
  char *key = NULL, *wrong = NULL;
  pthread_t thread;
  REQUIRE(argc == 3);
  REQUIRE(pthread_key_create(&thread_gate, NULL) == 0);
  thread_gate_ready = 1;
  REQUIRE(!strcmp(argv[2], "lifecycle") || !strcmp(argv[2], "faults") ||
          !strcmp(argv[2], "concurrency"));
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-root-", argv[1]) > 0);
  REQUIRE(snprintf(temporary, sizeof(temporary), "%sXXXXXX", prefix) > 0);
  REQUIRE(lc_test_tmp_mkdtemp(temporary, root, sizeof(root), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/container", root) > 0);
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  ok(lc_pouch_crypto_generate_key_string(&wrong, &e), &e);
  run r = {path, argv[2], key, wrong};
  REQUIRE(pthread_create(&thread, NULL, run_tests, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  thread_gate_ready = 0;
  REQUIRE(pthread_key_delete(thread_gate) == 0);
  lc_pouch_crypto_key_string_free(key);
  lc_pouch_crypto_key_string_free(wrong);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(root, prefix);
  printf("pouch-space root %s: %lu observable checks passed\n", argv[2],
         (unsigned long)checks);
  return 0;
}
