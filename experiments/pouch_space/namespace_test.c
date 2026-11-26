#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "budget.h"
#include "fixture_barriers.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "namespace.h"
#include "namespace_internal.h"
#include "record_io.h"
#include "space_internal.h"
#include "stream.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocation, fail_allocation;
static size_t largest;
static int measuring;
static int shared_namespace, fail_namespace_load;
static const char *program;
int __real_ps_coordination_open(int, const ps_coordination_options *,
                                ps_coordination **, lc_error *);
int __wrap_ps_coordination_open(int fd, const ps_coordination_options *options,
                                ps_coordination **out, lc_error *e) {
  ps_coordination_options selected = *options;
  if (shared_namespace)
    selected.shared = 1;
  return __real_ps_coordination_open(fd, &selected, out, e);
}
static void track(size_t n) {
  if (measuring && n > largest)
    largest = n;
}
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static int fails(void) {
  return fail_allocation && ++allocation == fail_allocation;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  track(n);
  return fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t b) {
  track(n * b);
  if (fail_namespace_load && n == 1 && b == sizeof(struct ps_namespace_data)) {
    ++fail_namespace_load;
    return NULL;
  }
  return fails() ? NULL : __real_lc_calloc_with_allocator(a, n, b);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  track(n);
  return fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *p) {
  track(p ? strlen(p) + 1 : 0);
  return fails() ? NULL : __real_lc_strdup_with_allocator(a, p);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "%d: %s\n", rc, e->message ? e->message : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static lc_allocator allocator;
static ps_budget *capacity_peer(ps_space *, int, lc_error *);
static unsigned char body[65537];
typedef struct gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  int entered, released;
} gate;
static __thread pthread_cond_t *watched_scope;
static __thread gate *scope_wait;
int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
int __wrap_pthread_cond_wait(pthread_cond_t *condition,
                             pthread_mutex_t *mutex) {
  if (scope_wait && condition == watched_scope) {
    pthread_mutex_lock(&scope_wait->mutex);
    scope_wait->entered = 1;
    pthread_cond_broadcast(&scope_wait->changed);
    pthread_mutex_unlock(&scope_wait->mutex);
  }
  return __real_pthread_cond_wait(condition, mutex);
}
typedef struct input {
  lc_source pub;
  size_t at, bytes;
  gate *gate;
} input;
static size_t pull(lc_source *pub, void *p, size_t n, lc_error *e) {
  input *s = pub->impl;
  (void)e;
  if (s->gate && !s->at) {
    pthread_mutex_lock(&s->gate->mutex);
    s->gate->entered = 1;
    pthread_cond_broadcast(&s->gate->changed);
    while (!s->gate->released)
      pthread_cond_wait(&s->gate->changed, &s->gate->mutex);
    pthread_mutex_unlock(&s->gate->mutex);
  }
  if (n > s->bytes - s->at)
    n = s->bytes - s->at;
  memcpy(p, body + s->at, n);
  s->at += n;
  return n;
}
static int put_with_content_type(ps_root *root, ps_namespace *ns,
                                 size_t version, uint64_t target, size_t bytes,
                                 gate *g, uint64_t *segment,
                                 lc_pouch_state_entry *result,
                                 const char *content_type, lc_error *e) {
  lc_pouch_state_entry request = {0};
  ps_namespace_append *append = NULL;
  input source = {0};
  source.pub.impl = &source;
  source.pub.read = pull;
  source.bytes = bytes;
  source.gate = g;
  request.key = "key";
  request.content_type = (char *)content_type;
  request.payload_context = "namespace-test";
  request.version = version;
  request.index_seq = version;
  request.lifetime = 1;
  request.record_type = 1;
  int rc = ps_namespace_append_begin(ns, target, 0, PS_APPEND_PREFIX, NULL,
                                     &append, e);
  if (rc == LC_OK) {
    if (segment)
      *segment = ps_namespace_append_segment(append);
    rc = lc_pouch_state_record_append(
        &allocator, ps_root_crypto(root), ps_namespace_append_output(append),
        ps_namespace_append_leaf(append), &request, &source.pub, 0, result, e);
  }
  if (rc == LC_OK)
    rc = ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, e);
  ps_namespace_append_close(append);
  return rc;
}
static int put(ps_root *root, ps_namespace *ns, size_t version, uint64_t target,
               size_t bytes, gate *g, uint64_t *segment,
               lc_pouch_state_entry *result, lc_error *e) {
  return put_with_content_type(root, ns, version, target, bytes, g, segment,
                               result, "application/test", e);
}
static int append(ps_root *root, ps_namespace *ns, size_t version,
                  uint64_t target, size_t bytes, lc_error *e) {
  lc_pouch_state_entry result = {0};
  int rc = put(root, ns, version, target, bytes, NULL, NULL, &result, e);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  return rc;
}
typedef struct records {
  size_t count;
  uint64_t last_segment, version;
} records;
static int visit(void *p, uint64_t segment, const lc_pouch_state_entry *r,
                 lc_error *e) {
  records *v = p;
  (void)e;
  REQUIRE(segment >= v->last_segment && r->version > v->version);
  REQUIRE(!strcmp(r->key, "key") && r->found && r->payload_span.present);
  REQUIRE(r->index_seq == r->version && r->bytes <= sizeof(body));
  v->last_segment = segment;
  v->version = r->version;
  ++v->count;
  return LC_OK;
}
static int capture(ps_namespace *ns, records *records, lc_error *e) {
  ps_namespace_view *view = NULL;
  int rc = ps_namespace_view_open(ns, &view, e);
  if (rc == LC_OK)
    rc = ps_namespace_view_visit(view, 1, visit, records, e);
  ps_namespace_view_close(view);
  return rc;
}
static int cancel_visit(void *p, uint64_t segment,
                        const lc_pouch_state_entry *r, lc_error *e) {
  (void)segment;
  (void)r;
  ++*(size_t *)p;
  return lc_error_set(e, LC_ERR_INVALID, 0, "observer stopped", NULL, NULL,
                      "test");
}
static void count(ps_namespace *ns, size_t expected, lc_error *e) {
  records r = {0};
  ok(capture(ns, &r, e), e);
  REQUIRE(r.count == expected);
}
static ps_namespace *acquire(ps_root *root, const char *name, int create,
                             lc_error *e) {
  ps_namespace *ns = NULL;
  ok(ps_root_namespace_acquire(root, name, create, &ns, e), e);
  return ns;
}
static void lifecycle(const char *path, const char *key) {
  lc_error e;
  lc_error_init(&e);
  for (int mode = 0; mode < 4; ++mode) {
    ps_root_options options = {0};
    ps_root *root = NULL;
    options.crypto.compression_enabled = mode & 1;
    options.crypto.key_string = mode & 2 ? key : NULL;
    options.storage.durable_sync = mode % 2;
    ok(ps_root_open(path, 1, &options, &root, &e), &e);
    ps_namespace *ns = acquire(root, "a/../namespace\377", 1, &e);
    ps_namespace_status status;
    ps_stats physical;
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(!status.active_segment && !status.segments && !status.tail);
    ok(ps_status(ps_root_space(root), &physical, &e), &e);
    REQUIRE(physical.entries == 2);
    count(ns, 0, &e);
    lc_pouch_state_entry result = {0};
    uint64_t segment;
    ok(put(root, ns, 1, 0, sizeof(body), NULL, &segment, &result, &e), &e);
    REQUIRE(segment == 1);
    ps_namespace_window *window = NULL;
    ok(ps_namespace_read_open(ns, PS_CATALOG_SEGMENT, segment,
                              result.payload_span.payload_offset,
                              result.cipher_bytes, &window, &e),
       &e);
    ps_namespace_view *view = NULL;
    ok(ps_namespace_view_open(ns, &view, &e), &e);
    ok(ps_namespace_rotate(ns, 1, &e), &e);
    ok(ps_namespace_rotate(ns, 1, &e), &e);
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.active_segment == 2);
    REQUIRE(ps_namespace_rotate(ns, 0, &e) == LC_ERR_INVALID);
    clear(&e);
    ok(append(root, ns, 2, 1, 1, &e), &e);
    ok(append(root, ns, 3, 1, 0, &e), &e); /* target crossed -> segment 3 */
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.active_segment == 3);
    size_t stopped = 0;
    REQUIRE(ps_namespace_view_visit(view, 1, cancel_visit, &stopped, &e) ==
                LC_ERR_INVALID &&
            stopped == 1);
    clear(&e);
    records old = {0};
    ok(ps_namespace_view_visit(view, 1, visit, &old, &e), &e);
    REQUIRE(old.count == 1);
    count(ns, 3, &e);
    /* A window pins the root even after the caller releases its namespace. */
    ps_namespace_release(ns);
    REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
    clear(&e);
    ps_namespace_view_close(view);
    unsigned char *stored = malloc((size_t)result.cipher_bytes);
    REQUIRE(stored != NULL);
    struct iovec part = {stored, (size_t)result.cipher_bytes};
    ok(lc_pouch_record_io_read(ps_namespace_read_input(window),
                               result.payload_span.payload_offset, &part, 1,
                               &e),
       &e);
    REQUIRE(crc32(0L, stored, (uInt)part.iov_len) ==
            result.payload_span.payload_crc);
    free(stored);
    ps_namespace_read_close(window);
    lc_pouch_state_entry_cleanup(&allocator, &result);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "a/../namespace\377", 0, &e);
    count(ns, 3, &e);
    ok(ps_namespace_rotate(ns, 2, &e), &e); /* persisted retry stays at 3 */
    for (size_t i = 4; i < 90; ++i) {
      ok(ps_namespace_rotate(ns, i - 1, &e), &e);
      ok(append(root, ns, i, 0, i % 3, &e), &e);
    }
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    count(ns, 89, &e);
    largest = 0;
    measuring = 1;
    ps_namespace_view *metadata_view = NULL;
    int capture_rc = ps_namespace_view_open(ns, &metadata_view, &e);
    measuring = 0;
    ok(capture_rc, &e);
    REQUIRE(largest < PS_CHUNK);
    ps_namespace_view_close(metadata_view);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "a/../namespace\377", 0, &e);
    count(ns, 89, &e);
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.segments == 89);
    /* A failed raw-child close leaves cached namespace storage reconstructible.
     */
    ps_reader *raw = NULL;
    ps_key config = {0, 1, PS_CONTROL};
    ok(ps_read_open(ps_root_space(root), config, NULL, &raw, &e), &e);
    ps_namespace_release(ns);
    REQUIRE(ps_root_close(root, &e) == LC_ERR_INVALID);
    clear(&e);
    ns = acquire(root, "a/../namespace\377", 0, &e);
    count(ns, 89, &e);
    ps_reader_close(raw);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
typedef struct model {
  unsigned char *live, *stable;
  size_t calls, fail, reads;
  int torn;
  gate *head_gate;
  int head_fail;
} model;
static int io_fail(model *m) { return ++m->calls == m->fail; }
static ssize_t rd(void *p, int fd, void *bytes, size_t n, off_t at) {
  model *m = p;
  ++m->reads;
  if (io_fail(m)) {
    errno = EIO;
    return -1;
  }
  return pread(fd, bytes, n, at);
}
static ssize_t wr(void *p, int fd, const void *bytes, size_t n, off_t at) {
  model *m = p;
  REQUIRE(at >= 0 && (uint64_t)at + n <= PS_MIB);
  if (io_fail(m)) {
    if (m->torn && n > 1) {
      REQUIRE(pwrite(fd, bytes, n / 2, at) == (ssize_t)(n / 2));
      memcpy(m->live + at, bytes, n / 2);
      memcpy(m->stable + at, bytes, n / 2);
    }
    errno = EIO;
    return -1;
  }
  if (m->head_gate && n == 128 && !memcmp(bytes, "LCPSNSHD", 8) &&
      ((const unsigned char *)bytes)[16] == 2) {
    pthread_mutex_lock(&m->head_gate->mutex);
    m->head_gate->entered = 1;
    pthread_cond_broadcast(&m->head_gate->changed);
    while (!m->head_gate->released)
      pthread_cond_wait(&m->head_gate->changed, &m->head_gate->mutex);
    pthread_mutex_unlock(&m->head_gate->mutex);
    if (m->head_fail) {
      errno = EIO;
      return -1;
    }
  }
  ssize_t result = pwrite(fd, bytes, n, at);
  if (result > 0)
    memcpy(m->live + at, bytes, (size_t)result);
  return result;
}
static int sync_file(void *p, int fd) {
  model *m = p;
  (void)fd;
  if (io_fail(m)) {
    if (m->torn == 2) {
      for (size_t at = 0; at < PS_MIB; at += 64)
        memcpy(m->stable + at, m->live + at, 32);
    } else if (m->torn == 3) {
      memcpy(m->stable, m->live, PS_MIB / 2);
    }
    errno = EIO;
    return -1;
  }
  memcpy(m->stable, m->live, PS_MIB);
  return 0;
}
static int reserve(void *p, int fd, off_t at, off_t n) {
  model *m = p;
  if (io_fail(m)) {
    errno = ENOSPC;
    return -1;
  }
  int rc = posix_fallocate(fd, at, n);
  if (rc) {
    errno = rc;
    return -1;
  }
  return 0;
}
static void crash(model *m, const char *path) {
  int fd = open(path, O_RDWR);
  REQUIRE(fd >= 0);
  REQUIRE(pwrite(fd, m->stable, PS_MIB, 0) == PS_MIB);
  REQUIRE(close(fd) == 0);
  memcpy(m->live, m->stable, PS_MIB);
}
static void faults(const char *path) {
  model m = {0};
  m.live = malloc(PS_MIB);
  m.stable = malloc(PS_MIB);
  REQUIRE(m.live && m.stable);
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.io = &io;
  lc_error e;
  lc_error_init(&e);
  for (int durable = 0; durable < 2; ++durable)
    for (int kind = 0; kind < 4; ++kind)
      for (int phase = 0; phase < 6; ++phase) {
        size_t operations = 0;
        for (size_t at = 0; at <= operations; ++at) {
          memset(m.live, 0, PS_MIB);
          memset(m.stable, 0, PS_MIB);
          m.calls = m.fail = 0;
          m.torn = kind >= 2 ? kind : 0;
          ps_root *root = NULL;
          options.storage.durable_sync = durable;
          ok(ps_root_open(path, 1, &options, &root, &e), &e);
          ps_namespace *ns = acquire(root, "test", 1, &e);
          if (phase) {
            ok(append(root, ns, 1, 0, 31, &e), &e);
            if (phase == 2) {
              ok(ps_namespace_rotate(ns, 1, &e), &e);
              ok(append(root, ns, 2, 0, 17, &e), &e);
            } else if (phase == 5)
              ok(append(root, ns, 2, 1, 17, &e), &e);
            ok(ps_checkpoint(ps_root_space(root), &e), &e);
          }
          if (phase == 2) {
            ps_namespace_release(ns);
            ok(ps_root_close(root, &e), &e);
            ok(ps_root_open(path, 0, &options, &root, &e), &e);
            ns = acquire(root, "test", 0, &e);
          }
          m.calls = allocation = 0;
          if (kind == 1)
            fail_allocation = at ? at : SIZE_MAX;
          else
            m.fail = at;
          int rc;
          records scanned = {0};
          if (!phase)
            rc = append(root, ns, 1, 0, 31, &e);
          else if (phase == 1)
            rc = ps_namespace_rotate(ns, 1, &e);
          else if (phase == 2)
            rc = capture(ns, &scanned, &e);
          else if (phase == 3) {
            ps_namespace_append *reserved = NULL;
            rc = ps_namespace_append_begin(ns, 1, sizeof(body) + 2048,
                                           PS_APPEND_PREFIX, NULL, &reserved,
                                           &e);
            ps_namespace_append_close(reserved);
          } else if (phase == 5)
            rc = append(root, ns, 3, 1, sizeof(body), &e);
          else
            rc = append(root, ns, 2, 1, sizeof(body), &e);
          if (!at)
            operations = kind == 1 ? allocation : m.calls;
          m.fail = fail_allocation = 0;
          REQUIRE(!at || kind == 1 || rc != LC_OK);
          REQUIRE(at || rc == LC_OK);
          clear(&e);
          int health = ps_check(ps_root_space(root), &e);
          if (health != LC_OK) {
            REQUIRE(health == LC_ERR_TRANSPORT);
            clear(&e);
            ps_namespace_status status;
            ps_namespace_view *rejected = NULL;
            REQUIRE(ps_namespace_status_get(ns, &status, &e) ==
                    LC_ERR_TRANSPORT);
            clear(&e);
            REQUIRE(ps_namespace_view_open(ns, &rejected, &e) ==
                        LC_ERR_TRANSPORT &&
                    !rejected);
            clear(&e);
          } else if (phase == 2 && kind == 1 && rc != LC_OK) {
            records retried = {0};
            ok(capture(ns, &retried, &e), &e);
            REQUIRE(retried.count == 2);
          }
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          crash(&m, path);
          ok(ps_root_open(path, 0, &options, &root, &e), &e);
          ns = acquire(root, "test", 0, &e);
          records recovered = {0};
          ok(capture(ns, &recovered, &e), &e);
          if (!phase) {
            REQUIRE(recovered.count <= 1 &&
                    (!durable || at || recovered.count == 1));
            ok(append(root, ns, recovered.count + 1, 0, 7, &e), &e);
            count(ns, recovered.count + 1, &e);
          } else if (phase >= 4) {
            size_t prior = phase == 5 ? 2U : 1U;
            REQUIRE(recovered.count >= prior && recovered.count <= prior + 1 &&
                    (at || !durable || recovered.count == prior + 1));
            ok(append(root, ns, recovered.count + 1, 0, 17, &e), &e);
            count(ns, recovered.count + 1, &e);
          } else
            REQUIRE(recovered.count == (phase == 2 ? 2U : 1U));
          if (phase == 1) {
            ok(ps_namespace_rotate(ns, 1, &e), &e);
            ps_namespace_status status;
            ok(ps_namespace_status_get(ns, &status, &e), &e);
            REQUIRE(status.active_segment == 2);
            ok(append(root, ns, 2, 0, 13, &e), &e);
            count(ns, 2, &e);
          }
          if (phase == 3) {
            ok(append(root, ns, 2, 0, sizeof(body), &e), &e);
            count(ns, 2, &e);
          }
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          REQUIRE(unlink(path) == 0);
        }
      }
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
}
static void raw_head(const char *path, ps_key key, const unsigned char *bytes,
                     size_t length, lc_error *e) {
  ps_space *space = NULL;
  ps_writer *writer = NULL;
  ok(ps_open(path, 0, NULL, &space, e), e);
  ok(ps_begin(space, NULL, key, &writer, e), e);
  ok(ps_write(writer, bytes, length, e), e);
  ok(ps_publish(writer, NULL, e), e);
  ps_writer_close(writer);
  ok(ps_close(space, e), e);
}
static void invalid_namespace(const char *path, lc_error *e) {
  ps_root *root = NULL;
  /* Root admission now recovers namespace checkpoints eagerly. */
  REQUIRE(ps_root_open(path, 0, NULL, &root, e) == LC_ERR_PROTOCOL && !root);
  clear(e);
}
static void put64(unsigned char *p, uint64_t v) {
  for (size_t i = 0; i < 8; ++i) {
    p[i] = (unsigned char)v;
    v >>= 8;
  }
}
static void windows(const char *path) {
  lc_error e;
  lc_error_init(&e);
  ps_space *space = NULL;
  ps_stream *stream = NULL;
  ps_key key = {1, 1, PS_STREAM};
  ps_stream_writer *writer = NULL;
  ok(ps_open(path, 1, NULL, &space, &e), &e);
  ok(ps_stream_open(space, NULL, key, 1, NULL, NULL, NULL, &stream, &e), &e);
  ok(ps_stream_begin(stream, &writer, &e), &e);
  ok(ps_stream_write(writer, "abc", 3, &e), &e);
  ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(writer);
  ps_stream_window *pin = NULL, *rejected = NULL;
  allocation = 0;
  fail_allocation = 1;
  int rc = ps_stream_window_open(stream, 0, 3, &pin, &e);
  fail_allocation = 0;
  REQUIRE(rc == LC_ERR_NOMEM && !pin);
  clear(&e);
  ok(ps_stream_window_open(stream, 1, 2, &pin, &e), &e);
  REQUIRE(ps_stream_window_open(stream, 0, 4, &rejected, &e) ==
              LC_ERR_INVALID &&
          !rejected);
  clear(&e);
  REQUIRE(ps_stream_window_open(stream, UINT64_MAX, 1, &rejected, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  ok(ps_stream_begin(stream, &writer, &e), &e);
  ok(ps_stream_write(writer, "def", 3, &e), &e);
  ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(writer);
  char bytes[4] = {0};
  REQUIRE(ps_stream_window_read(pin, 0, bytes, 1, &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(ps_stream_window_read(pin, 2, bytes, 2, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_stream_window_read(pin, 3, NULL, 0, &e), &e);
  ok(ps_stream_window_read(pin, 1, bytes, 2, &e), &e);
  REQUIRE(!memcmp(bytes, "bc", 2));
  ok(ps_stream_retire(stream, &e), &e);
  REQUIRE(ps_stream_window_open(stream, 0, 1, &rejected, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_checkpoint(space, &e), &e);
  REQUIRE(ps_stream_close(stream, &e) == LC_ERR_INVALID);
  clear(&e);
  ok(ps_stream_window_read(pin, 1, bytes, 2, &e), &e);
  REQUIRE(!memcmp(bytes, "bc", 2));
  ps_stream_window_close(pin);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_checkpoint(space, &e), &e);
  ok(ps_close(space, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void validation(const char *path) {
  windows(path);
  lc_error e;
  lc_error_init(&e);
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ps_namespace *ns = acquire(root, "test", 1, &e);
  ps_namespace_info identity;
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(append(root, ns, 1, 0, 31, &e), &e);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  ps_reader *reader = NULL;
  ps_key key = {identity.owner, 1, PS_ROUTING};
  unsigned char valid[128], bytes[128];
  size_t got;
  ok(ps_read_open(ps_root_space(root), key, NULL, &reader, &e), &e);
  ok(ps_read(reader, valid, sizeof(valid), &got, &e), &e);
  REQUIRE(got == sizeof(valid));
  ps_reader_close(reader);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  for (size_t i = 0; i < sizeof(valid); ++i) {
    memcpy(bytes, valid, sizeof(bytes));
    bytes[i] ^= 1;
    raw_head(path, key, bytes, sizeof(bytes), &e);
    invalid_namespace(path, &e);
  }
  for (size_t field = 0; field < 5; ++field) {
    memcpy(bytes, valid, sizeof(bytes));
    if (field == 0)
      put64(bytes + 8, identity.owner + 1);
    if (field == 1)
      put64(bytes + 16, 0);
    if (field == 2)
      put64(bytes + 24, 0);
    if (field == 3)
      put64(bytes + 32, 31);
    if (field == 4)
      bytes[40] = 1;
    put64(bytes + 120, crc32(0L, bytes, 120));
    raw_head(path, key, bytes, sizeof(bytes), &e);
    invalid_namespace(path, &e);
  }
  raw_head(path, key, valid, 127, &e);
  invalid_namespace(path, &e);
  raw_head(path, key, valid, sizeof(valid), &e);
  /* Missing/interpreted-as-path/stale-owner bindings never become segments. */
  for (int bad = 0; bad < 5; ++bad) {
    ps_space *space = NULL;
    ps_catalog_info original, inserted;
    ps_catalog_view *view = NULL;
    const char *first = "seg-00000000000000000001.log";
    const char *name = bad == 0   ? ""
                       : bad == 1 ? "active"
                       : bad == 2 ? "seg-00000000000000000002.log"
                       : bad == 3 ? "seg-1.log"
                                  : "seg-000000000000000000001.log";
    ok(ps_open(path, 0, NULL, &space, &e), &e);
    ok(ps_catalog_get(space, identity.owner, PS_CATALOG_SEGMENT, first,
                      strlen(first), &view, &e),
       &e);
    REQUIRE(view != NULL);
    ok(ps_catalog_view_info(view, &original, &e), &e);
    ps_catalog_view_close(view);
    if (bad >= 3)
      ok(ps_catalog_remove(space, identity.owner, PS_CATALOG_SEGMENT, first,
                           strlen(first), &original.revision, &e),
         &e);
    ok(ps_catalog_put(space, NULL, identity.owner, PS_CATALOG_SEGMENT, name,
                      strlen(name), NULL, original.stream, &original.target,
                      &inserted, &e),
       &e);
    ok(ps_close(space, &e), &e);
    invalid_namespace(path, &e);
    ok(ps_open(path, 0, NULL, &space, &e), &e);
    ok(ps_catalog_remove(space, identity.owner, PS_CATALOG_SEGMENT, name,
                         strlen(name), &inserted.revision, &e),
       &e);
    if (bad >= 3)
      ok(ps_catalog_put(space, NULL, identity.owner, PS_CATALOG_SEGMENT, first,
                        strlen(first), NULL, original.stream, &original.target,
                        &inserted, &e),
         &e);
    ok(ps_close(space, &e), &e);
  }
  ok(ps_root_open(path, 0, NULL, &root, &e), &e);
  ns = acquire(root, "test", 0, &e);
  count(ns, 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct job {
  ps_root *root;
  ps_namespace *ns;
  gate *gate;
  int rc;
} job;
static void *produce(void *p) {
  job *j = p;
  lc_error e;
  lc_error_init(&e);
  lc_pouch_state_entry result = {0};
  j->rc = put(j->root, j->ns, 2, 0, sizeof(body), j->gate, NULL, &result, &e);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  lc_error_cleanup(&e);
  return NULL;
}
typedef struct reentrant {
  ps_root *root;
  ps_namespace *ns;
  records records;
} reentrant;
static int mutate(void *p, uint64_t segment, const lc_pouch_state_entry *r,
                  lc_error *e) {
  reentrant *v = p;
  int rc = visit(&v->records, segment, r, e);
  if (rc == LC_OK && v->records.count == 1) {
    rc = ps_namespace_rotate(v->ns, 1, e);
    if (rc == LC_OK)
      rc = append(v->root, v->ns, 3, 0, 3, e);
  }
  return rc;
}
typedef struct read_hold {
  gate gate;
  int enabled;
} read_hold;
static ssize_t held_read(void *p, int fd, void *bytes, size_t n, off_t at) {
  read_hold *h = p;
  pthread_mutex_lock(&h->gate.mutex);
  if (h->enabled && !h->gate.entered) {
    h->gate.entered = 1;
    pthread_cond_broadcast(&h->gate.changed);
    while (!h->gate.released)
      pthread_cond_wait(&h->gate.changed, &h->gate.mutex);
  }
  pthread_mutex_unlock(&h->gate.mutex);
  return pread(fd, bytes, n, at);
}
static ssize_t direct_write(void *p, int fd, const void *bytes, size_t n,
                            off_t at) {
  (void)p;
  return pwrite(fd, bytes, n, at);
}
static int direct_sync(void *p, int fd) {
  (void)p;
  return fdatasync(fd);
}
static int direct_reserve(void *p, int fd, off_t at, off_t n) {
  (void)p;
  int rc = posix_fallocate(fd, at, n);
  if (rc)
    errno = rc;
  return rc ? -1 : 0;
}
typedef struct read_job {
  ps_namespace *ns;
  int started, done, rc;
} read_job;
static void *read_namespace(void *p) {
  read_job *j = p;
  lc_error e;
  lc_error_init(&e);
  records found = {0};
  __atomic_store_n(&j->started, 1, __ATOMIC_RELEASE);
  j->rc = capture(j->ns, &found, &e);
  if (j->rc == LC_OK)
    REQUIRE(found.count == 3);
  __atomic_store_n(&j->done, 1, __ATOMIC_RELEASE);
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(const char *path) {
  ps_root *root = NULL;
  lc_error e;
  lc_error_init(&e);
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ps_namespace *a = acquire(root, "a", 1, &e), *b = acquire(root, "b", 1, &e);
  ok(append(root, a, 1, 0, 31, &e), &e);
  gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
  job j = {root, a, &g, 0};
  pthread_t thread;
  REQUIRE(pthread_create(&thread, NULL, produce, &j) == 0);
  pthread_mutex_lock(&g.mutex);
  while (!g.entered)
    pthread_cond_wait(&g.changed, &g.mutex);
  pthread_mutex_unlock(&g.mutex);
  ps_namespace_append *contended = NULL;
  REQUIRE(ps_namespace_append_begin(a, 0, 0, PS_APPEND_PREFIX, NULL, &contended,
                                    &e) == LC_ERR_INVALID &&
          !contended);
  clear(&e);
  REQUIRE(ps_namespace_rotate(a, 1, &e) == LC_ERR_INVALID);
  clear(&e);
  ps_namespace_view *captured = NULL;
  ok(ps_namespace_view_open(a, &captured, &e), &e);
  count(a, 1, &e);
  ok(append(root, b, 1, 0, 31, &e), &e);
  count(b, 1, &e);
  pthread_mutex_lock(&g.mutex);
  g.released = 1;
  pthread_cond_broadcast(&g.changed);
  pthread_mutex_unlock(&g.mutex);
  REQUIRE(pthread_join(thread, NULL) == 0 && j.rc == LC_OK);
  reentrant v = {root, a, {0}};
  ok(ps_namespace_view_visit(captured, 1, mutate, &v, &e), &e);
  REQUIRE(v.records.count == 1);
  ps_namespace_view_close(captured);
  count(a, 3, &e);
  pthread_cond_destroy(&g.changed);
  pthread_mutex_destroy(&g.mutex);
  ps_namespace_release(a);
  ps_namespace_release(b);
  ok(ps_root_close(root, &e), &e);
  read_hold held = {{PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0},
                    0};
  ps_io io = {&held,       held_read,      direct_write,
              direct_sync, direct_reserve, NULL};
  ps_root_options options = {0};
  options.storage.io = &io;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  a = acquire(root, "a", 0, &e);
  b = acquire(root, "b", 0, &e);
  read_job jobs[2] = {{a, 0, 0, 0}, {a, 0, 0, 0}};
  pthread_t threads[2];
  pthread_mutex_lock(&held.gate.mutex);
  held.enabled = 1;
  pthread_mutex_unlock(&held.gate.mutex);
  REQUIRE(pthread_create(&threads[0], NULL, read_namespace, &jobs[0]) == 0);
  pthread_mutex_lock(&held.gate.mutex);
  while (!held.gate.entered)
    pthread_cond_wait(&held.gate.changed, &held.gate.mutex);
  pthread_mutex_unlock(&held.gate.mutex);
  /* Root admission already recovered both namespaces. A blocked source read
   * must leave status, another captured reader and unrelated mutation free. */
  ps_namespace_status status;
  ok(ps_namespace_status_get(a, &status, &e), &e);
  REQUIRE(pthread_create(&threads[1], NULL, read_namespace, &jobs[1]) == 0);
  REQUIRE(pthread_join(threads[1], NULL) == 0 && jobs[1].rc == LC_OK);
  ok(append(root, b, 2, 0, 17, &e), &e);
  count(b, 2, &e);
  REQUIRE(!__atomic_load_n(&jobs[0].done, __ATOMIC_ACQUIRE) &&
          __atomic_load_n(&jobs[1].done, __ATOMIC_ACQUIRE));
  pthread_mutex_lock(&held.gate.mutex);
  held.gate.released = 1;
  pthread_cond_broadcast(&held.gate.changed);
  pthread_mutex_unlock(&held.gate.mutex);
  REQUIRE(pthread_join(threads[0], NULL) == 0 && jobs[0].rc == LC_OK);
  count(a, 3, &e);
  ps_namespace_release(a);
  ps_namespace_release(b);
  ok(ps_root_close(root, &e), &e);
  pthread_cond_destroy(&held.gate.changed);
  pthread_mutex_destroy(&held.gate.mutex);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void stream_reservation(const char *path) {
  for (int sync = 0; sync < 2; ++sync) {
    ps_options o = {0};
    ps_space *space = NULL;
    ps_stream *stream = NULL;
    ps_stream_writer *writer = NULL;
    ps_stream_reader *reader = NULL;
    ps_stream_info info;
    ps_stats before, after;
    ps_key key = {1, 1, PS_STREAM};
    lc_error e;
    unsigned char bytes[1024];
    size_t at = 0, got;
    o.durable_sync = sync;
    lc_error_init(&e);
    ok(ps_open(path, 1, &o, &space, &e), &e);
    ok(ps_stream_open(space, NULL, key, 1, NULL, NULL, NULL, &stream, &e), &e);
    ok(ps_stream_begin(stream, &writer, &e), &e);
    ok(ps_status(space, &before, &e), &e);
    ok(ps_stream_reserve(writer, NULL, 0, &e), &e);
    ok(ps_status(space, &after, &e), &e);
    REQUIRE(before.entries == after.entries &&
            before.journal_bytes == after.journal_bytes);
    ok(ps_stream_reserve(writer, NULL, PS_CHUNK + 1, &e), &e);
    ok(ps_stream_status(stream, &info, &e), &e);
    REQUIRE(!info.tail && info.capacity >= PS_CHUNK + 1 && info.runs > 1);
    REQUIRE(ps_stream_read_open(stream, 0, 1, &reader, &e) == LC_ERR_INVALID &&
            !reader);
    clear(&e);
    ok(ps_stream_write(writer, body, 17, &e), &e);
    ok(ps_stream_reserve(writer, NULL, sizeof(body), &e), &e);
    ok(ps_stream_status(stream, &info, &e), &e);
    REQUIRE(!info.tail && info.capacity >= sizeof(body));
    ok(ps_status(space, &before, &e), &e);
    ok(ps_stream_reserve(writer, NULL, sizeof(body), &e), &e);
    ok(ps_stream_write(writer, body + 17, sizeof(body) - 17, &e), &e);
    ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
    REQUIRE(ps_stream_reserve(writer, NULL, 1, &e) == LC_ERR_INVALID);
    clear(&e);
    ps_stream_writer_close(writer);
    writer = NULL;
    ok(ps_status(space, &after, &e), &e);
    REQUIRE(before.entries == after.entries &&
            before.journal_bytes == after.journal_bytes &&
            after.syncs == before.syncs + (unsigned)sync);
    ok(ps_stream_read_open(stream, 0, sizeof(body), &reader, &e), &e);
    while ((got = 0, ps_stream_read(reader, bytes, sizeof(bytes), &got, &e)) ==
               LC_OK &&
           got) {
      REQUIRE(got <= sizeof(body) - at && !memcmp(bytes, body + at, got));
      at += got;
    }
    REQUIRE(e.code == LC_OK && at == sizeof(body));
    ok(ps_stream_begin(stream, &writer, &e), &e);
    REQUIRE(ps_stream_reserve(writer, NULL, (uint64_t)INT64_MAX, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    ok(ps_stream_reserve(writer, NULL, 0, &e), &e);
    ps_stream_writer_close(writer);
    ps_stream_reader_close(reader);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(space, &e), &e);
    REQUIRE(ps_stream_reserve(NULL, NULL, 1, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_record_reserve(NULL, NULL, 1, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(unlink(path) == 0);
    lc_error_cleanup(&e);
  }
}
static void rotation_capacity(const char *path) {
  for (int durable = 0; durable < 2; ++durable) {
    ps_root_options o = {0};
    ps_root *root = NULL;
    ps_namespace *ns;
    ps_namespace_status before, after;
    ps_stats physical, rejected;
    ps_budget *held[64] = {0};
    ps_budget_plan page = {PS_PAGE, 0, 0, 1};
    ps_writer *writer = NULL;
    size_t claims = 0, checkpoint_claims;
    lc_error e;
    lc_error_init(&e);
    o.storage.initial_capacity = o.storage.maximum_capacity = PS_MIB;
    o.storage.durable_sync = durable;
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ns = acquire(root, "rotation-capacity", 1, &e);
    ok(append(root, ns, 1, 0, 31, &e), &e);
    for (uint64_t i = 0;; ++i) {
      ps_key key = {99, i, PS_STATE};
      ok(ps_begin(ps_root_space(root), NULL, key, &writer, &e), &e);
      ok(ps_write(writer, body, PS_PAGE, &e), &e);
      int rc = ps_publish(writer, NULL, &e);
      ps_writer_close(writer);
      if (rc != LC_OK) {
        REQUIRE(rc == LC_ERR_INVALID);
        clear(&e);
        break;
      }
      REQUIRE(i < 256);
    }
    /* Foreground exhaustion leaves enough reserved capacity for a complete
     * explicit rotation: successor, binding, routing and predecessor seal. */
    ok(ps_namespace_rotate(ns, 1, &e), &e);
    ok(ps_namespace_status_get(ns, &before, &e), &e);
    REQUIRE(before.active_segment == 2 && before.tail == 0);
    count(ns, 1, &e);
    ok(ps_status(ps_root_space(root), &physical, &e), &e);
    checkpoint_claims = physical.budgets;
    for (;;) {
      REQUIRE(claims < sizeof(held) / sizeof(*held));
      int rc = ps_budget_open(ps_root_space(root), &page, held + claims, &e);
      if (rc != LC_OK) {
        REQUIRE(rc == LC_ERR_INVALID && !held[claims]);
        clear(&e);
        break;
      }
      ++claims;
    }
    ok(ps_status(ps_root_space(root), &physical, &e), &e);
    REQUIRE(physical.budgets == claims + checkpoint_claims && claims > 0);
    REQUIRE(ps_namespace_rotate(ns, 2, &e) == LC_ERR_INVALID);
    clear(&e);
    ok(ps_status(ps_root_space(root), &rejected, &e), &e);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(rejected.entries == physical.entries &&
            rejected.journal_bytes == physical.journal_bytes &&
            rejected.budgets == physical.budgets &&
            rejected.reserved_payload_bytes ==
                physical.reserved_payload_bytes &&
            after.active_segment == before.active_segment &&
            after.tail == before.tail);
    for (size_t i = 0; i < claims; ++i)
      ok(ps_budget_close(held[i], &e), &e);
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    ok(ps_namespace_rotate(ns, 2, &e), &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "rotation-capacity", 0, &e);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(after.active_segment == 3 && after.tail == 0);
    count(ns, 1, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    lc_error_cleanup(&e);
  }
}
static void reservation(const char *path) {
  rotation_capacity(path);
  stream_reservation(path);
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options o = {0};
    ps_root *root = NULL;
    ps_namespace *ns;
    ps_namespace_append *reserved = NULL;
    ps_namespace_window *rejected = NULL;
    ps_namespace_view *old = NULL;
    ps_namespace_status before, after;
    ps_stats allocated, committed;
    lc_pouch_state_entry request = {0}, result = {0};
    input source = {0};
    records seen = {0};
    const char *stable = NULL;
    lc_error e;
    lc_error_init(&e);
    o.storage.initial_capacity = PS_MIB;
    o.storage.maximum_capacity = 2 * PS_MIB;
    o.storage.durable_sync = sync;
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ns = acquire(root, "reserve", 1, &e);
    ok(append(root, ns, 1, 0, 31, &e), &e);
    ok(ps_namespace_status_get(ns, &before, &e), &e);
    REQUIRE(ps_namespace_append_begin(ns, 1, PS_MIB, PS_APPEND_PREFIX, NULL,
                                      &reserved, &e) == LC_ERR_INVALID &&
            !reserved && e.message && strstr(e.message, "capacity"));
    clear(&e);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(before.active_segment == 1 && after.active_segment == 1 &&
            after.tail == before.tail);
    /* Failed successor admission must leave the predecessor appendable. */
    ok(append(root, ns, 2, 0, 17, &e), &e);
    count(ns, 2, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "reserve", 0, &e);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(after.active_segment == 1);
    count(ns, 2, &e);
    ok(ps_namespace_view_open(ns, &old, &e), &e);
    ok(ps_grow(ps_root_space(root), PS_MIB, &e), &e);
    REQUIRE(ps_namespace_append_begin(ns, 1, UINT64_MAX, PS_APPEND_PREFIX, NULL,
                                      &reserved, &e) == LC_ERR_INVALID &&
            !reserved);
    clear(&e);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(after.active_segment == 1);
    ok(ps_namespace_append_begin(ns, 1, PS_MIB, PS_APPEND_PREFIX, NULL,
                                 &reserved, &e),
       &e);
    REQUIRE(ps_namespace_append_segment(reserved) == 2);
    stable = ps_namespace_append_leaf(reserved);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(after.active_segment == 1 && after.tail > 0);
    REQUIRE(ps_namespace_read_open(ns, PS_CATALOG_SEGMENT, 2, 0, 1, &rejected,
                                   &e) == LC_ERR_INVALID &&
            !rejected);
    clear(&e);
    count(ns, 2, &e);
    ok(ps_status(ps_root_space(root), &allocated, &e), &e);
    request.key = "key";
    request.content_type = "application/test";
    request.payload_context = "namespace-test";
    request.version = request.index_seq = 3;
    request.lifetime = 1;
    request.record_type = 1;
    source.pub.impl = &source;
    source.pub.read = pull;
    source.bytes = sizeof(body);
    ok(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                    ps_namespace_append_output(reserved),
                                    ps_namespace_append_leaf(reserved),
                                    &request, &source.pub, 0, &result, &e),
       &e);
    ok(ps_namespace_append_commit(reserved, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(reserved);
    lc_pouch_state_entry_cleanup(&allocator, &result);
    ok(ps_status(ps_root_space(root), &committed, &e), &e);
    REQUIRE(allocated.entries == committed.entries &&
            committed.syncs == allocated.syncs + (unsigned)sync);
    ok(ps_namespace_status_get(ns, &after, &e), &e);
    REQUIRE(after.active_segment == 2 && after.tail > 0);
    for (size_t version = 4; version <= 12; ++version)
      ok(append(root, ns, version, 1, 17, &e), &e);
    REQUIRE(!strcmp(stable, "seg-00000000000000000002.log"));
    ok(ps_namespace_view_visit(old, 1, visit, &seen, &e), &e);
    REQUIRE(seen.count == 2);
    ps_namespace_view_close(old);
    count(ns, 12, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "reserve", 0, &e);
    count(ns, 12, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    lc_error_cleanup(&e);
  }
}
typedef struct publisher {
  ps_space *space;
  ps_stream *stream;
  int mode, calls;
} publisher;
static int publish_control(void *context, uint64_t tail, lc_error *e) {
  publisher *p = context;
  ps_stream_info info;
  ps_stream_reader *hidden = NULL;
  ps_writer *control = NULL;
  ps_key key = {7, 1, PS_CONTROL};
  unsigned char bytes[8];
  int rc;
  ++p->calls;
  REQUIRE(tail == 3);
  ok(ps_stream_status(p->stream, &info, e), e);
  REQUIRE(info.tail == 0);
  REQUIRE(ps_stream_read_open(p->stream, 0, 1, &hidden, e) == LC_ERR_INVALID &&
          !hidden);
  clear(e);
  if (!p->mode)
    return LC_OK; /* No barrier: valid only on a NoSync root. */
  if (p->mode == 1)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "publication fixture rejected",
                        NULL, NULL, "test");
  put64(bytes, tail);
  rc = ps_begin(p->space, NULL, key, &control, e);
  if (rc == LC_OK)
    rc = ps_write(control, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(control, NULL, e);
  ps_writer_close(control);
  return rc;
}
static void covered_publication(const char *path) {
  for (int commit = PS_COMMIT_DEFAULT; commit <= PS_COMMIT_DEFER_SYNC; ++commit)
    for (int sync = 0; sync < 2; ++sync)
      for (int mode = 0; mode < 3; ++mode) {
        ps_options o = {0};
        ps_space *space = NULL;
        ps_stream *stream = NULL;
        ps_stream_writer *writer = NULL;
        ps_stream_reader *reader = NULL;
        ps_stream_info info;
        ps_stats before, after;
        ps_key key = {1, 1, PS_STREAM};
        publisher p = {0};
        lc_error e;
        unsigned char bytes[3];
        size_t got;
        int rc;
        lc_error_init(&e);
        o.durable_sync = sync;
        ok(ps_open(path, 1, &o, &space, &e), &e);
        ok(ps_stream_open(space, NULL, key, 1, NULL, NULL, NULL, &stream, &e),
           &e);
        ok(ps_stream_begin(stream, &writer, &e), &e);
        ok(ps_stream_reserve(writer, NULL, 3, &e), &e);
        ok(ps_stream_write(writer, "abc", 3, &e), &e);
        p.space = space;
        p.stream = stream;
        p.mode = mode;
        ok(ps_status(space, &before, &e), &e);
        rc = ps_stream_commit(writer, (enum ps_commit_mode)2, publish_control,
                              &p, &e);
        REQUIRE(rc == LC_ERR_INVALID && p.calls == 0);
        clear(&e);
        ok(ps_status(space, &after, &e), &e);
        REQUIRE(after.syncs == before.syncs &&
                after.journal_bytes == before.journal_bytes);
        rc = ps_stream_commit(writer, (enum ps_commit_mode)commit,
                              publish_control, &p, &e);
        REQUIRE(p.calls == 1);
        if (!mode && sync) {
          REQUIRE(rc == LC_ERR_PROTOCOL);
          clear(&e);
          REQUIRE(ps_check(space, &e) == LC_ERR_TRANSPORT);
          clear(&e);
        } else if (mode == 1) {
          REQUIRE(rc == LC_ERR_NOMEM);
          clear(&e);
          ok(ps_stream_status(stream, &info, &e), &e);
          REQUIRE(info.tail == 0);
        } else {
          ok(rc, &e);
          ok(ps_status(space, &after, &e), &e);
          REQUIRE(after.syncs == before.syncs + (mode ? 1 : 0));
        }
        ps_stream_writer_close(writer);
        writer = NULL;
        if (mode == 1) {
          ok(ps_check(space, &e), &e);
          ok(ps_stream_begin(stream, &writer, &e), &e);
          ok(ps_stream_write(writer, "xyz", 3, &e), &e);
          ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
          ps_stream_writer_close(writer);
        }
        if (mode || !sync) {
          ok(ps_stream_read_open(stream, 0, 3, &reader, &e), &e);
          ok(ps_stream_read(reader, bytes, sizeof(bytes), &got, &e), &e);
          REQUIRE(got == 3 && !memcmp(bytes, mode == 1 ? "xyz" : "abc", 3));
          ps_stream_reader_close(reader);
        }
        ok(ps_stream_close(stream, &e), &e);
        ok(ps_close(space, &e), &e);
        REQUIRE(unlink(path) == 0);
        lc_error_cleanup(&e);
      }
}
static int recover_empty(ps_stream *stream, uint64_t checkpoint,
                         uint64_t capacity, uint64_t *accepted, void *context,
                         lc_error *e) {
  (void)stream;
  (void)capacity;
  (void)context;
  (void)e;
  REQUIRE(checkpoint == 0);
  *accepted = 0;
  return LC_OK;
}
static void reject_published_successor(const char *path) {
  ps_root *root = NULL;
  ps_namespace *ns;
  ps_namespace_info identity;
  ps_namespace_append *staged = NULL;
  ps_space *space = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info binding;
  ps_stream *stream = NULL;
  ps_stream_writer *writer = NULL;
  const char *leaf = "seg-00000000000000000002.log";
  lc_error e;
  lc_error_init(&e);
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ns = acquire(root, "test", 1, &e);
  ok(append(root, ns, 1, 0, 31, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(ps_namespace_append_begin(ns, 1, 1, PS_APPEND_PREFIX, NULL, &staged, &e),
     &e);
  ps_namespace_append_close(staged);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_open(path, 0, NULL, &space, &e), &e);
  ok(ps_catalog_get(space, identity.owner, PS_CATALOG_SEGMENT, leaf,
                    strlen(leaf), &view, &e),
     &e);
  REQUIRE(view != NULL);
  ok(ps_catalog_view_info(view, &binding, &e), &e);
  ps_catalog_view_close(view);
  ok(ps_stream_open(space, NULL, binding.stream, 0, &binding.target,
                    recover_empty, NULL, &stream, &e),
     &e);
  ok(ps_stream_begin(stream, &writer, &e), &e);
  ok(ps_stream_write(writer, "bad", 3, &e), &e);
  ok(ps_stream_commit(writer, PS_COMMIT_DEFAULT, NULL, NULL, &e), &e);
  ps_stream_writer_close(writer);
  /* This forbidden raw publication is now an authoritative checkpoint tail,
   * unlike the private zero-tail bytes in the held-head recovery test. */
  ok(ps_checkpoint(space, &e), &e);
  ok(ps_stream_close(stream, &e), &e);
  ok(ps_close(space, &e), &e);
  invalid_namespace(path, &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
static void routing_value(ps_space *space, ps_key key, unsigned char expected,
                          lc_error *e) {
  ps_reader *reader = NULL;
  unsigned char got = 0;
  size_t count;
  ok(ps_read_open(space, key, NULL, &reader, e), e);
  REQUIRE(ps_read_size(reader) == 1);
  ok(ps_read(reader, &got, 1, &count, e), e);
  REQUIRE(count == 1 && got == expected);
  ps_reader_close(reader);
}
static void routing(const char *path) {
  for (int sync = 0; sync < 2; ++sync)
    for (int phase = 0; phase < 4; ++phase) {
      model m = {0};
      ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
      ps_options o = {PS_MIB, PS_MIB, sync, &io};
      ps_space *space = NULL;
      ps_writer *writer = NULL;
      ps_reader *pin = NULL, *missing = NULL;
      ps_key key = {7, 1, PS_ROUTING};
      ps_stats before, after;
      lc_error e;
      unsigned char value = 1, got;
      size_t count;
      lc_error_init(&e);
      m.live = calloc(1, PS_MIB);
      m.stable = calloc(1, PS_MIB);
      REQUIRE(m.live && m.stable);
      ok(ps_open(path, 1, &o, &space, &e), &e);
      REQUIRE(ps_flush(NULL, &e) == LC_ERR_INVALID);
      clear(&e);
      ok(ps_begin(space, NULL, key, &writer, &e), &e);
      ok(ps_write(writer, &value, 1, &e), &e);
      ok(ps_publish(writer, NULL, &e), &e);
      ps_writer_close(writer);
      ok(ps_flush(space, &e), &e);
      ok(ps_read_open(space, key, NULL, &pin, &e), &e);
      ok(ps_status(space, &before, &e), &e);
      if (phase < 2) {
        value = 2;
        ok(ps_begin(space, NULL, key, &writer, &e), &e);
        ok(ps_write(writer, &value, 1, &e), &e);
        ok(ps_publish(writer, NULL, &e), &e);
        ps_writer_close(writer);
        routing_value(space, key, 2, &e);
      } else {
        ok(ps_delete(space, key, &e), &e);
        REQUIRE(ps_read_open(space, key, NULL, &missing, &e) ==
                    LC_ERR_INVALID &&
                !missing);
        clear(&e);
      }
      ok(ps_status(space, &after, &e), &e);
      REQUIRE(after.syncs == before.syncs + (unsigned)sync);
      if (phase % 2)
        ok(ps_flush(space, &e), &e);
      ok(ps_read(pin, &got, 1, &count, &e), &e);
      REQUIRE(count == 1 && got == 1);
      ps_reader_close(pin);
      ok(ps_status(space, &before, &e), &e);
      if (sync || phase % 2) {
        ok(ps_flush(space, &e), &e);
        ok(ps_status(space, &after, &e), &e);
        REQUIRE(after.syncs == before.syncs);
      }
      ok(ps_close(space, &e), &e);
      crash(&m, path);
      ok(ps_open(path, 0, &o, &space, &e), &e);
      if (phase >= 2 && (sync || phase % 2)) {
        REQUIRE(ps_read_open(space, key, NULL, &missing, &e) ==
                    LC_ERR_INVALID &&
                !missing);
        clear(&e);
      } else
        routing_value(space, key, sync || phase % 2 ? 2 : 1, &e);
      ok(ps_checkpoint(space, &e), &e);
      ok(ps_close(space, &e), &e);
      REQUIRE(unlink(path) == 0);
      free(m.live);
      free(m.stable);
      lc_error_cleanup(&e);
    }
}
typedef struct head_job {
  ps_root *root;
  ps_namespace *ns;
  int rc;
} head_job;
static void *commit_head(void *context) {
  head_job *j = context;
  lc_error e;
  lc_error_init(&e);
  j->rc = append(j->root, j->ns, 2, 1, sizeof(body), &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void publication(const char *path) {
  covered_publication(path);
  reject_published_successor(path);
  routing(path);
  for (int sync = 0; sync < 2; ++sync)
    for (int fail = 0; fail < 2; ++fail) {
      model m = {0};
      gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
      ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
      ps_root_options o = {0};
      ps_root *root = NULL;
      ps_namespace *ns, *peer;
      ps_namespace_view *old = NULL;
      ps_namespace_status before, after;
      records captured = {0};
      pthread_t thread;
      lc_error e;
      lc_error_init(&e);
      m.live = calloc(1, PS_MIB);
      m.stable = calloc(1, PS_MIB);
      REQUIRE(m.live && m.stable);
      o.storage.initial_capacity = o.storage.maximum_capacity = PS_MIB;
      o.storage.durable_sync = sync;
      o.storage.io = &io;
      ok(ps_root_open(path, 1, &o, &root, &e), &e);
      ns = acquire(root, "head", 1, &e);
      peer = acquire(root, "peer", 1, &e);
      ok(append(root, ns, 1, 0, 31, &e), &e);
      ok(ps_checkpoint(ps_root_space(root), &e), &e);
      ok(ps_namespace_status_get(ns, &before, &e), &e);
      ok(ps_namespace_view_open(ns, &old, &e), &e);
      m.head_gate = &g;
      m.head_fail = fail;
      head_job j = {root, ns, 0};
      REQUIRE(pthread_create(&thread, NULL, commit_head, &j) == 0);
      pthread_mutex_lock(&g.mutex);
      while (!g.entered)
        pthread_cond_wait(&g.changed, &g.mutex);
      pthread_mutex_unlock(&g.mutex);
      ok(ps_namespace_status_get(ns, &after, &e), &e);
      REQUIRE(after.active_segment == before.active_segment &&
              after.tail == before.tail);
      count(ns, 1, &e);
      ok(append(root, peer, 1, 0, 17, &e), &e);
      /* Complete private canonical bytes now survive a checkpoint, but their
       * stream checkpoint tail must remain zero until head publication. */
      ok(ps_checkpoint(ps_root_space(root), &e), &e);
      pthread_mutex_lock(&g.mutex);
      g.released = 1;
      pthread_cond_broadcast(&g.changed);
      pthread_mutex_unlock(&g.mutex);
      REQUIRE(pthread_join(thread, NULL) == 0);
      m.head_gate = NULL;
      REQUIRE(j.rc == (fail ? LC_ERR_TRANSPORT : LC_OK));
      if (fail) {
        REQUIRE(ps_namespace_view_visit(old, 1, visit, &captured, &e) ==
                LC_ERR_TRANSPORT);
        clear(&e);
      } else {
        ok(ps_namespace_view_visit(old, 1, visit, &captured, &e), &e);
        REQUIRE(captured.count == 1);
        count(ns, 2, &e);
      }
      ps_namespace_view_close(old);
      ps_namespace_release(ns);
      ps_namespace_release(peer);
      ok(ps_root_close(root, &e), &e);
      crash(&m, path);
      ok(ps_root_open(path, 0, &o, &root, &e), &e);
      ns = acquire(root, "head", 0, &e);
      peer = acquire(root, "peer", 0, &e);
      count(ns, fail || !sync ? 1 : 2, &e);
      count(peer, 1, &e);
      if (fail || !sync) {
        ok(append(root, ns, 2, 1, sizeof(body), &e), &e);
        count(ns, 2, &e);
      }
      ok(ps_flush(ps_root_space(root), &e), &e);
      ps_namespace_release(ns);
      ps_namespace_release(peer);
      ok(ps_root_close(root, &e), &e);
      crash(&m, path);
      ok(ps_root_open(path, 0, &o, &root, &e), &e);
      ns = acquire(root, "head", 0, &e);
      count(ns, 2, &e);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      REQUIRE(unlink(path) == 0);
      pthread_cond_destroy(&g.changed);
      pthread_mutex_destroy(&g.mutex);
      free(m.live);
      free(m.stable);
      lc_error_cleanup(&e);
    }
}
typedef struct body_sources {
  ps_root *root;
  ps_namespace *ns;
  size_t count;
  int reenter;
} body_sources;
static int body_source(void *context, const ps_namespace_source *s,
                       lc_error *e) {
  body_sources *c = context;
  ++c->count;
  REQUIRE(s->kind == PS_CATALOG_BODY && s->closed && s->tail == 3 &&
          s->capacity >= s->tail && s->capacity - s->tail < PS_PAGE);
  if (c->reenter)
    return append(c->root, c->ns, 1, 0, 31, e);
  return LC_OK;
}
static void body_budget_read(ps_namespace *ns, uint64_t id,
                             const void *expected, size_t bytes, lc_error *e) {
  ps_namespace_window *pin = NULL;
  unsigned char buffer[317];
  ok(ps_namespace_read_open(ns, PS_CATALOG_BODY, id, 0, bytes, &pin, e), e);
  for (size_t at = 0; at < bytes;) {
    size_t n = bytes - at;
    if (n > sizeof(buffer))
      n = sizeof(buffer);
    struct iovec part = {buffer, n};
    ok(lc_pouch_record_io_read(ps_namespace_read_input(pin), at, &part, 1, e),
       e);
    REQUIRE(!memcmp(buffer, (const unsigned char *)expected + at, n));
    at += n;
  }
  ps_namespace_read_close(pin);
}
static void body_budget_capacity(const char *path) {
  for (int durable = 0; durable < 2; ++durable) {
    ps_root_options o = {0};
    ps_root *root = NULL;
    ps_namespace *ns;
    ps_namespace_body *b = NULL, *rejected = NULL;
    ps_budget *peer = NULL;
    ps_budget_plan floor = {PS_MAINTENANCE_RESERVE, 0, 0, 1};
    ps_stats before, after;
    ps_namespace_status status;
    unsigned int kind;
    uint64_t id;
    lc_error e;
    lc_error_init(&e);
    o.storage.initial_capacity = o.storage.maximum_capacity = PS_MIB;
    o.storage.durable_sync = durable;
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ns = acquire(root, "body-budget", 1, &e);
    ok(append(root, ns, 1, 0, 31, &e), &e);
    ps_stats checkpoint_claim;
    ok(ps_status(ps_root_space(root), &checkpoint_claim, &e), &e);
    ok(ps_namespace_body_begin(ns, sizeof(body), &b, &e), &e);
    REQUIRE(
        ps_namespace_parse_source_leaf(ps_namespace_body_leaf(b), &kind, &id));
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    REQUIRE(before.budgets == checkpoint_claim.budgets + 1 &&
            before.reserved_entries > checkpoint_claim.reserved_entries);
    REQUIRE(ps_namespace_body_begin(ns, PS_MIB, &rejected, &e) ==
                LC_ERR_INVALID &&
            !rejected);
    clear(&e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.entries == before.entries &&
            after.journal_bytes == before.journal_bytes &&
            after.budgets == before.budgets);
    for (uint64_t i = 0;; ++i) {
      ps_key key = {99, i, PS_STATE};
      ps_writer *writer = NULL;
      ok(ps_begin(ps_root_space(root), NULL, key, &writer, &e), &e);
      ok(ps_write(writer, body, PS_PAGE, &e), &e);
      int rc = ps_publish(writer, NULL, &e);
      ps_writer_close(writer);
      if (rc != LC_OK) {
        REQUIRE(rc == LC_ERR_INVALID);
        clear(&e);
        break;
      }
      REQUIRE(i < 256);
    }
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    ok(ps_budget_open(ps_root_space(root), &floor, &peer, &e), &e);
    struct iovec part = {body, sizeof(body)};
    ok(lc_pouch_record_io_append(ps_namespace_body_output(b), &part, 1, &e),
       &e);
    ok(ps_namespace_body_commit(b, &e), &e);
    ps_namespace_body_close(b);
    ok(ps_budget_close(peer, &e), &e);
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.bound_bodies == 1 && status.active_segment == 1);
    body_budget_read(ns, id, body, sizeof(body), &e);
    count(ns, 1, &e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.budgets == checkpoint_claim.budgets &&
            after.reserved_payload_bytes ==
                checkpoint_claim.reserved_payload_bytes &&
            after.reserved_metadata_bytes ==
                checkpoint_claim.reserved_metadata_bytes &&
            after.reserved_entries == checkpoint_claim.reserved_entries);
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "body-budget", 0, &e);
    body_budget_read(ns, id, body, sizeof(body), &e);
    count(ns, 1, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    lc_error_cleanup(&e);
  }
}
static void body_budget_faults(const char *path) {
  model m = {0};
  m.live = malloc(PS_MIB);
  m.stable = malloc(PS_MIB);
  REQUIRE(m.live && m.stable);
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options options = {0};
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.io = &io;
  lc_error e;
  lc_error_init(&e);
  for (int durable = 0; durable < 2; ++durable)
    for (int kind = 0; kind < 2; ++kind) {
      size_t operations = 0;
      for (size_t cut = 0; cut <= operations; ++cut) {
        memset(m.live, 0, PS_MIB);
        memset(m.stable, 0, PS_MIB);
        m.calls = m.fail = 0;
        m.torn = kind ? 0 : 2;
        ps_root *root = NULL;
        options.storage.durable_sync = durable;
        ok(ps_root_open(path, 1, &options, &root, &e), &e);
        ps_namespace *ns = acquire(root, "body-budget", 1, &e);
        ok(append(root, ns, 1, 0, 31, &e), &e);
        ok(ps_checkpoint(ps_root_space(root), &e), &e);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        ok(ps_root_open(path, 0, &options, &root, &e), &e);
        ns = acquire(root, "body-budget", 0, &e);
        ps_stats initial;
        ok(ps_status(ps_root_space(root), &initial, &e), &e);
        ps_namespace_body *b = NULL;
        unsigned int source_kind;
        uint64_t id = 0;
        m.calls = allocation = 0;
        if (kind)
          fail_allocation = cut ? cut : SIZE_MAX;
        else
          m.fail = cut;
        int rc = ps_namespace_body_begin(ns, PS_CHUNK + 333, &b, &e);
        if (rc == LC_OK) {
          REQUIRE(ps_namespace_parse_source_leaf(ps_namespace_body_leaf(b),
                                                 &source_kind, &id));
          struct iovec part = {body, PS_CHUNK + 333};
          rc = lc_pouch_record_io_append(ps_namespace_body_output(b), &part, 1,
                                         &e);
        }
        if (rc == LC_OK)
          rc = ps_namespace_body_commit(b, &e);
        if (!cut)
          operations = kind ? allocation : m.calls;
        m.fail = fail_allocation = 0;
        REQUIRE(cut ? rc != LC_OK : rc == LC_OK);
        if (kind && rc != LC_OK)
          REQUIRE(rc == LC_ERR_NOMEM);
        clear(&e);
        ps_namespace_body_close(b);
        if (kind && rc != LC_OK) {
          ok(ps_check(ps_root_space(root), &e), &e);
          ps_namespace_body *retried = NULL;
          ok(ps_namespace_body_begin(ns, 3, &retried, &e), &e);
          ps_namespace_body_close(retried);
        }
        ps_stats physical;
        ok(ps_status(ps_root_space(root), &physical, &e), &e);
        REQUIRE(physical.budgets == initial.budgets &&
                physical.reserved_payload_bytes ==
                    initial.reserved_payload_bytes &&
                physical.reserved_metadata_bytes ==
                    initial.reserved_metadata_bytes &&
                physical.reserved_entries == initial.reserved_entries);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        crash(&m, path);
        ok(ps_root_open(path, 0, &options, &root, &e), &e);
        ns = acquire(root, "body-budget", 0, &e);
        count(ns, 1, &e);
        ps_namespace_status status;
        ok(ps_namespace_status_get(ns, &status, &e), &e);
        REQUIRE(status.bound_bodies <= 1);
        if (rc == LC_OK)
          REQUIRE(status.bound_bodies == 1);
        if (kind && rc != LC_OK)
          REQUIRE(!status.bound_bodies);
        if (status.bound_bodies)
          body_budget_read(ns, id, body, PS_CHUNK + 333, &e);
        ok(ps_namespace_body_begin(ns, 3, &b, &e), &e);
        struct iovec retry = {(void *)"abc", 3};
        ok(lc_pouch_record_io_append(ps_namespace_body_output(b), &retry, 1,
                                     &e),
           &e);
        ok(ps_namespace_body_commit(b, &e), &e);
        ps_namespace_body_close(b);
        ok(ps_checkpoint(ps_root_space(root), &e), &e);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        REQUIRE(unlink(path) == 0);
      }
      REQUIRE(operations > 10);
      printf("body budget failure cuts (durable=%d %s): %lu\n", durable,
             kind ? "allocation" : "write/sync", (unsigned long)operations);
    }
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
}
static unsigned char *append_frame(ps_root *root, ps_namespace *fixture,
                                   uint64_t version, size_t *bytes,
                                   lc_error *e) {
  ps_namespace_status before, after;
  ps_namespace_window *window = NULL;
  ok(ps_namespace_status_get(fixture, &before, e), e);
  ok(append(root, fixture, version, 0, 31, e), e);
  ok(ps_namespace_status_get(fixture, &after, e), e);
  *bytes = (size_t)(after.tail - before.tail);
  unsigned char *frame = malloc(*bytes);
  REQUIRE(frame && *bytes > 32 && *bytes < PS_PAGE);
  ok(ps_namespace_read_open(fixture, PS_CATALOG_SEGMENT, after.active_segment,
                            before.tail, *bytes, &window, e),
     e);
  struct iovec part = {frame, *bytes};
  ok(lc_pouch_record_io_read(ps_namespace_read_input(window), before.tail,
                             &part, 1, e),
     e);
  ps_namespace_read_close(window);
  /* Detached PUT bytes are copied into the first/second framed batches in
   * limits below. Rebase their inline self-reference for those frame headers;
   * the production codec independently checks the resulting physical span. */
  lc_pouch_record_header header;
  ok(lc_pouch_record_header_decode(frame, &header, e), e);
  uint64_t record_offset = before.tail + version * PS_FRAME_BYTES;
  unsigned char *meta = frame + LC_POUCH_RECORD_HEADER_BYTES + header.key_len;
  put64(meta + 32, record_offset);
  put64(meta + 40, record_offset + LC_POUCH_RECORD_HEADER_BYTES +
                       header.key_len + header.meta_len);
  return frame;
}
/* Fixtures retain structured canonical prefixes, including the metadata
 * obligation, before a body can be fragmented or peers consume spare space. */
static size_t append_fixture_prefix(lc_pouch_record_io output,
                                    unsigned char *frame, size_t bytes,
                                    lc_error *e) {
  lc_pouch_record_header header;
  ok(lc_pouch_record_header_decode(frame, &header, e), e);
  size_t offset =
      LC_POUCH_RECORD_HEADER_BYTES + header.key_len + header.meta_len;
  REQUIRE(offset <= bytes && header.payload_len == bytes - offset);
  struct iovec parts[3] = {
      {frame, LC_POUCH_RECORD_HEADER_BYTES},
      {frame + LC_POUCH_RECORD_HEADER_BYTES, header.key_len},
      {frame + LC_POUCH_RECORD_HEADER_BYTES + header.key_len, header.meta_len}};
  ok(lc_pouch_record_io_append(output, parts, 3, e), e);
  return offset;
}
static void append_fixture_body(lc_pouch_record_io output, unsigned char *frame,
                                size_t offset, size_t bytes, lc_error *e) {
  if (offset < bytes) {
    struct iovec part = {frame + offset, bytes - offset};
    ok(lc_pouch_record_io_append(output, &part, 1, e), e);
  }
}
static void append_budget_limits(const char *path) {
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options o = {0};
    o.storage.durable_sync = sync;
    ps_root *root = NULL;
    lc_error e;
    lc_error_init(&e);
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ps_namespace *fixture = acquire(root, "fixture", 1, &e);
    size_t n, n2;
    unsigned char *frame = append_frame(root, fixture, 1, &n, &e);
    unsigned char *frame2 = append_frame(root, fixture, 2, &n2, &e);
    ps_namespace_release(fixture);
    ps_namespace *ns = acquire(root, "limits", 1, &e);
    ps_namespace_append *a = NULL;
    ps_stats before, after;
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    REQUIRE(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_COMPLETE, NULL, &a,
                                      &e) == LC_ERR_INVALID &&
            !a);
    clear(&e);
    REQUIRE(ps_namespace_append_begin(ns, 0, 1,
                                      (enum ps_namespace_append_mode)3, NULL,
                                      &a, &e) == LC_ERR_INVALID &&
            !a);
    clear(&e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.entries == before.entries &&
            after.journal_bytes == before.journal_bytes);
    for (int filled = 0; filled < 2; ++filled) {
      ok(ps_namespace_append_begin(ns, 0, n, PS_APPEND_COMPLETE, NULL, &a, &e),
         &e);
      lc_pouch_record_io io = ps_namespace_append_output(a);
      uint64_t start, tail;
      ok(lc_pouch_record_io_tail(io, &start, &e), &e);
      if (filled) {
        size_t payload = append_fixture_prefix(io, frame, n, &e);
        append_fixture_body(io, frame, payload, n, &e);
      }
      struct iovec too_much[2] = {{frame, n}, {(void *)"x", 1}};
      REQUIRE(lc_pouch_record_io_append(io, too_much, filled ? 1 : 2, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      ok(lc_pouch_record_io_tail(io, &tail, &e), &e);
      REQUIRE(tail == start + (filled ? n : 0));
      REQUIRE(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      ps_namespace_append_close(a);
      ps_namespace_status status;
      ok(ps_namespace_status_get(ns, &status, &e), &e);
      REQUIRE(!status.active_segment && !status.tail);
    }
    ok(ps_namespace_append_begin(ns, 0, n, PS_APPEND_COMPLETE, NULL, &a, &e),
       &e);
    lc_pouch_record_io io = ps_namespace_append_output(a);
    size_t payload = append_fixture_prefix(io, frame, n, &e);
    size_t middle = payload + (n - payload) / 2;
    append_fixture_body(io, frame, payload, middle, &e);
    REQUIRE(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e) ==
            LC_ERR_INVALID);
    clear(&e);
    append_fixture_body(io, frame, middle, n, &e);
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(a);
    count(ns, 1, &e);
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    ok(ps_namespace_append_begin(ns, 0, n2, PS_APPEND_COMPLETE, NULL, &a, &e),
       &e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.budgets == before.budgets); /* Existing metadata claims. */
    payload =
        append_fixture_prefix(ps_namespace_append_output(a), frame2, n2, &e);
    append_fixture_body(ps_namespace_append_output(a), frame2, payload, n2, &e);
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(a);
    count(ns, 2, &e);
    ok(ps_flush(ps_root_space(root), &e), &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "limits", 0, &e);
    count(ns, 2, &e);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    free(frame);
    free(frame2);
    lc_error_cleanup(&e);
  }
}
static int append_known_record(ps_root *root, ps_namespace_append *a,
                               uint64_t version, lc_error *e) {
  lc_pouch_state_entry request = {0}, result = {0};
  input source = {0};
  request.key = "key";
  request.content_type = "application/test";
  request.payload_context = "namespace-test";
  request.version = request.index_seq = version;
  request.lifetime = 1;
  request.record_type = 1;
  source.pub.impl = &source;
  source.pub.read = pull;
  source.bytes = 31;
  int rc = lc_pouch_state_record_append(
      &allocator, ps_root_crypto(root), ps_namespace_append_output(a),
      ps_namespace_append_leaf(a), &request, &source.pub, 0, &result, e);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  return rc;
}
static void append_budget_capacity(const char *path) {
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options o = {0};
    o.storage.initial_capacity = o.storage.maximum_capacity = PS_MIB;
    o.storage.durable_sync = sync;
    ps_root *root = NULL;
    lc_error e;
    lc_error_init(&e);
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ps_namespace *fixture = acquire(root, "fixture", 1, &e);
    size_t n, n2;
    unsigned char *frame = append_frame(root, fixture, 1, &n, &e);
    unsigned char *frame2 = append_frame(root, fixture, 2, &n2, &e);
    ps_namespace_release(fixture);
    ps_namespace *ns = acquire(root, "budget", 1, &e);
    ps_namespace *other = acquire(root, "reject", 1, &e);
    ps_namespace_append *a = NULL, *rejected = NULL;
    ok(ps_namespace_append_begin(ns, 1, n, PS_APPEND_COMPLETE, NULL, &a, &e),
       &e);
    size_t payload =
        append_fixture_prefix(ps_namespace_append_output(a), frame, n, &e);
    for (uint64_t i = 0;; ++i) {
      ps_key k = {99, i, PS_STATE};
      ps_writer *w = NULL;
      ok(ps_begin(ps_root_space(root), NULL, k, &w, &e), &e);
      ok(ps_write(w, body, PS_PAGE, &e), &e);
      int rc = ps_publish(w, NULL, &e);
      ps_writer_close(w);
      if (rc != LC_OK) {
        REQUIRE(rc == LC_ERR_INVALID);
        clear(&e);
        break;
      }
      REQUIRE(i < 256);
    }
    ps_budget_plan floor = {PS_MAINTENANCE_RESERVE, 0, 0, 1};
    ps_budget *peer = NULL;
    ok(ps_budget_open(ps_root_space(root), &floor, &peer, &e), &e);
    ps_stats before, after;
    ok(ps_status(ps_root_space(root), &before, &e), &e);
    REQUIRE(ps_namespace_append_begin(other, 1, n, PS_APPEND_COMPLETE, NULL,
                                      &rejected, &e) == LC_ERR_INVALID &&
            !rejected);
    clear(&e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.entries == before.entries &&
            after.journal_bytes == before.journal_bytes);
    append_fixture_body(ps_namespace_append_output(a), frame, payload, n, &e);
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(a);
    ok(ps_budget_close(peer, &e), &e);
    count(ns, 1, &e);
    ok(ps_checkpoint(ps_root_space(root), &e), &e);
    /* A checkpoint can return pages. Hold the remaining physical capacity;
     * exhausting a payload-only ordinary plan is insufficient when another
     * plan can use already protected metadata workspace. */
    peer = capacity_peer(ps_root_space(root), 1, &e);
    REQUIRE(ps_namespace_append_begin(ns, 1, n2, PS_APPEND_COMPLETE, NULL,
                                      &rejected, &e) == LC_ERR_INVALID &&
            !rejected);
    clear(&e);
    ok(ps_budget_close(peer, &e), &e);
    ok(ps_namespace_append_begin(ns, 1, n2, PS_APPEND_MAINTENANCE, NULL, &a,
                                 &e),
       &e);
    ok(append_known_record(root, a, 2, &e), &e);
    ok(ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e), &e);
    ps_namespace_append_close(a);
    count(ns, 2, &e);
    ok(ps_status(ps_root_space(root), &after, &e), &e);
    REQUIRE(after.budgets == 2 && after.reserved_payload_bytes &&
            after.reserved_metadata_bytes && after.reserved_entries);
    ok(ps_flush(ps_root_space(root), &e), &e);
    ps_namespace_release(other);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "budget", 0, &e);
    count(ns, 2, &e);
    other = acquire(root, "reject", 0, &e);
    ps_namespace_status status;
    ok(ps_namespace_status_get(other, &status, &e), &e);
    REQUIRE(!status.active_segment && !status.bound_segments);
    ps_namespace_release(other);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    free(frame);
    free(frame2);
    lc_error_cleanup(&e);
  }
}
#include "batch_cases.h"
static void append_budget_faults(const char *path, int planned) {
  model m = {0};
  m.live = malloc(PS_MIB);
  m.stable = malloc(PS_MIB);
  REQUIRE(m.live && m.stable);
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  ps_root_options o = {0};
  o.storage.initial_capacity = o.storage.maximum_capacity = PS_MIB;
  o.storage.io = &io;
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync)
    for (int rotating = 0; rotating < 2; ++rotating)
      for (int kind = 0; kind < 2; ++kind) {
        size_t operations = 0;
        for (size_t cut = 0; cut <= operations; ++cut) {
          memset(m.live, 0, PS_MIB);
          memset(m.stable, 0, PS_MIB);
          m.calls = m.fail = 0;
          m.torn = kind ? 0 : 2;
          o.storage.durable_sync = sync;
          ps_root *root = NULL;
          ok(ps_root_open(path, 1, &o, &root, &e), &e);
          ps_namespace *fixture = acquire(root, "fixture", 1, &e);
          size_t n;
          unsigned char *frame = append_frame(root, fixture, 1, &n, &e);
          ok(ps_namespace_checkpoint_flush(fixture, &e), &e);
          ps_namespace_release(fixture);
          ps_namespace *ns = acquire(root, "faults", 1, &e);
          if (rotating)
            ok(append(root, ns, 1, 0, 31, &e), &e);
          ok(ps_checkpoint(ps_root_space(root), &e), &e);
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          ok(ps_root_open(path, 0, &o, &root, &e), &e);
          ns = acquire(root, "faults", 0, &e);
          ps_namespace_append *a = NULL;
          batch_view prefixes = {{frame, NULL}, {n, 0}, 1, 0, 0};
          lc_pouch_storage_batch batch = {1, batch_prefix, &prefixes};
          enum ps_namespace_append_mode mode =
              rotating ? PS_APPEND_MAINTENANCE : PS_APPEND_COMPLETE;
          m.calls = allocation = 0;
          if (kind)
            fail_allocation = cut ? cut : SIZE_MAX;
          else
            m.fail = cut;
          int rc = ps_namespace_append_begin(ns, 1, n, mode,
                                             planned ? &batch : NULL, &a, &e);
          free(frame);
          if (rc == LC_OK)
            rc = append_known_record(root, a, rotating + 1, &e);
          if (rc == LC_OK)
            rc = ps_namespace_append_commit(a, PS_COMMIT_DEFAULT, &e);
          if (!cut)
            operations = kind ? allocation : m.calls;
          m.fail = fail_allocation = 0;
          REQUIRE(!cut || kind || rc != LC_OK);
          REQUIRE(cut || rc == LC_OK);
          if (kind && rc != LC_OK)
            REQUIRE(rc == LC_ERR_NOMEM);
          clear(&e);
          ps_namespace_append_close(a);
          if (kind && rc != LC_OK) {
            ok(ps_check(ps_root_space(root), &e), &e);
            count(ns, rotating, &e);
            ok(ps_namespace_append_begin(ns, 1, n, mode, NULL, &a, &e), &e);
            ps_namespace_append_close(a);
          }
          ps_stats physical;
          ok(ps_status(ps_root_space(root), &physical, &e), &e);
          REQUIRE(physical.budgets <= 1);
          if (physical.budgets) {
            REQUIRE(rotating || rc == LC_OK);
            REQUIRE(physical.budgets == 1 && physical.reserved_payload_bytes &&
                    physical.reserved_metadata_bytes &&
                    physical.reserved_entries);
          } else {
            /* An I/O failure can precede reconstruction of an old namespace's
             * debt. Such an owner must remain poisoned, or have no prior work.
             */
            REQUIRE((!rotating && rc != LC_OK) ||
                    ps_check(ps_root_space(root), NULL) != LC_OK);
            REQUIRE(!physical.budgets && !physical.reserved_payload_bytes &&
                    !physical.reserved_metadata_bytes &&
                    !physical.reserved_entries);
          }
          if (rc == LC_OK)
            count(ns, rotating + 1, &e);
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          crash(&m, path);
          ok(ps_root_open(path, 0, &o, &root, &e), &e);
          ns = acquire(root, "faults", 0, &e);
          ps_namespace_status status;
          ok(ps_namespace_status_get(ns, &status, &e), &e);
          REQUIRE(status.active_segment == (uint64_t)rotating ||
                  status.active_segment == (uint64_t)rotating + 1);
          if (rc == LC_OK && sync)
            REQUIRE(status.active_segment == (uint64_t)rotating + 1);
          if (kind && rc != LC_OK)
            REQUIRE(status.active_segment == (uint64_t)rotating);
          size_t expected =
              rotating + (status.active_segment > (uint64_t)rotating);
          count(ns, expected, &e);
          ok(append(root, ns, expected + 1, 0, 17, &e), &e);
          count(ns, expected + 1, &e);
          ok(ps_checkpoint(ps_root_space(root), &e), &e);
          ps_namespace_release(ns);
          ok(ps_root_close(root, &e), &e);
          REQUIRE(unlink(path) == 0);
        }
        REQUIRE(operations > 10);
        printf("complete append planned%d cuts (sync=%d rotating=%d %s): %lu\n",
               planned, sync, rotating, kind ? "allocation" : "write/sync",
               (unsigned long)operations);
      }
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
}
static void bodies(const char *path) {
  lc_error e;
  lc_error_init(&e);
  unsigned int kind;
  uint64_t id;
  REQUIRE(ps_namespace_parse_source_leaf("body-18446744073709551615.log", &kind,
                                         &id) &&
          kind == PS_CATALOG_BODY && id == UINT64_MAX);
  const char *invalid[] = {
      "body-00000000000000000000.log", "body-18446744073709551616.log",
      "body-0000000000000000000x.log", "body-1.log",
      "body-00000000000000000001.bad", "body-000000000000000000001.log"};
  for (size_t i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i)
    REQUIRE(!ps_namespace_parse_source_leaf(invalid[i], &kind, &id));
  for (int sync = 0; sync < 2; ++sync) {
    ps_root_options o = {0};
    o.storage.durable_sync = sync;
    ps_root *root = NULL;
    ok(ps_root_open(path, 1, &o, &root, &e), &e);
    ps_namespace *ns = acquire(root, "test", 1, &e);
    ps_namespace_body *b = NULL;
    REQUIRE(ps_namespace_body_begin(ns, 3, NULL, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_namespace_body_begin(NULL, 3, &b, &e) == LC_ERR_INVALID && !b);
    clear(&e);
    REQUIRE(ps_namespace_body_commit(NULL, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_namespace_checkpoint_flush(NULL, &e) == LC_ERR_INVALID);
    clear(&e);
    REQUIRE(ps_namespace_sources_visit(ns, NULL, NULL, &e) == LC_ERR_INVALID);
    clear(&e);
    ok(ps_namespace_body_begin(ns, 3, &b, &e), &e);
    char leaf[40];
    strcpy(leaf, ps_namespace_body_leaf(b));
    REQUIRE(ps_namespace_parse_source_leaf(leaf, &kind, &id) &&
            kind == PS_CATALOG_BODY);
    ps_namespace_window *pin = NULL, *rejected = NULL;
    REQUIRE(ps_namespace_read_open(ns, kind, id, 0, 3, &pin, &e) ==
                LC_ERR_INVALID &&
            !pin);
    clear(&e);
    body_sources c = {root, ns, 0, 0};
    ok(ps_namespace_sources_visit(ns, body_source, &c, &e), &e);
    REQUIRE(!c.count);
    REQUIRE(ps_namespace_body_commit(b, &e) == LC_ERR_INVALID);
    clear(&e);
    /* The physical pack owner treats bytes as opaque. The logical maintenance
     * producer supplies complete canonical frames and measured sizes. */
    struct iovec part = {(void *)"abc", 3};
    ok(lc_pouch_record_io_append(ps_namespace_body_output(b), &part, 1, &e),
       &e);
    ok(ps_namespace_body_commit(b, &e), &e);
    REQUIRE(ps_namespace_body_commit(b, &e) == LC_ERR_INVALID);
    clear(&e);
    ps_namespace_body_close(b);
    ps_namespace_body_close(NULL);
    count(ns, 0, &e); /* Body packs never become a canonical replay prefix. */
    c.reenter = 1;
    ok(ps_namespace_sources_visit(ns, body_source, &c, &e), &e);
    REQUIRE(c.count == 1); /* The callback's new segment was not captured. */
    count(ns, 1, &e);
    ps_namespace_status status;
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.bound_bodies == 1 && status.bound_segments == 1 &&
            !status.checkpoint_epoch && status.checkpoint_keys == 1);
    /* A valid pack's stream key also binds its owning namespace. */
    ps_namespace *foreign = acquire(root, "foreign", 1, &e);
    ps_namespace_info own, other;
    ok(ps_namespace_identity(ns, &own, &e), &e);
    ok(ps_namespace_identity(foreign, &other, &e), &e);
    ps_catalog_view *catalog = NULL;
    ps_catalog_info binding, inserted;
    ok(ps_catalog_get(ps_root_space(root), own.owner, kind, leaf, strlen(leaf),
                      &catalog, &e),
       &e);
    REQUIRE(catalog != NULL);
    ok(ps_catalog_view_info(catalog, &binding, &e), &e);
    ps_catalog_view_close(catalog);
    ok(ps_catalog_put(ps_root_space(root), NULL, other.owner, kind, leaf,
                      strlen(leaf), NULL, binding.stream, &binding.target,
                      &inserted, &e),
       &e);
    REQUIRE(ps_namespace_status_get(foreign, &status, &e) == LC_ERR_PROTOCOL);
    clear(&e);
    ok(ps_catalog_remove(ps_root_space(root), other.owner, kind, leaf,
                         strlen(leaf), &inserted.revision, &e),
       &e);
    ps_namespace_release(foreign);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    ok(ps_root_open(path, 0, &o, &root, &e), &e);
    ns = acquire(root, "test", 0, &e);
    count(ns, 1, &e);
    ok(ps_namespace_read_open(ns, kind, id, 0, 3, &pin, &e), &e);
    ok(ps_namespace_reclaim(ns, kind, id, &e), &e);
    REQUIRE(ps_namespace_read_open(ns, kind, id, 0, 3, &rejected, &e) ==
                LC_ERR_INVALID &&
            !rejected);
    clear(&e);
    uint64_t closed, pending;
    ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
    REQUIRE(!closed && pending == 1);
    char bytes[3];
    part.iov_base = bytes;
    ok(lc_pouch_record_io_read(ps_namespace_read_input(pin), 0, &part, 1, &e),
       &e);
    REQUIRE(!memcmp(bytes, "abc", 3));
    ps_namespace_read_close(pin);
    ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
    REQUIRE(closed == 1 && !pending);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
  /* A body catalog binding never authorizes recovery of an unsealed pack. */
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, NULL, &root, &e), &e);
  ps_namespace *ns = acquire(root, "test", 1, &e);
  ps_namespace_info identity;
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(ps_catalog_allocate_id(ps_root_space(root), &id, &e), &e);
  char leaf[40];
  REQUIRE(snprintf(leaf, sizeof(leaf), "body-%020lu.log", (unsigned long)id) ==
          29);
  ps_key key = {id, identity.owner, PS_STREAM};
  ps_stream *stream = NULL;
  ps_stream_info info;
  ps_catalog_info binding;
  ok(ps_stream_open(ps_root_space(root), NULL, key, 1, NULL, NULL, NULL,
                    &stream, &e),
     &e);
  ok(ps_stream_status(stream, &info, &e), &e);
  ok(ps_catalog_put(ps_root_space(root), NULL, identity.owner, PS_CATALOG_BODY,
                    leaf, strlen(leaf), NULL, key, &info.ref, &binding, &e),
     &e);
  ok(ps_stream_close(stream, &e), &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  invalid_namespace(path, &e);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}
#include "body_select_cases.h"
#include "compound_recode_cases.h"
#include "namespace_conversion_cases.h"
#include "recode_cases.h"
typedef struct shared_records {
  uint64_t index, version;
  size_t keys;
} shared_records;
static int visit_shared(void *context, uint64_t segment,
                        const lc_pouch_state_entry *record, lc_error *e) {
  shared_records *r = context;
  (void)segment;
  (void)e;
  if (record->control)
    r->index = record->index_seq;
  else {
    REQUIRE(!strcmp(record->key, "key") && record->found);
    r->version = record->version;
    ++r->keys;
  }
  return LC_OK;
}
typedef struct checkpoint_visibility {
  ps_space *publisher;
  int synced;
} checkpoint_visibility;
static int advance_checkpoint_visibility(void *context, int fd, int cmd,
                                         struct flock *lock) {
  checkpoint_visibility *change = context;
  if (change->publisher && cmd == F_OFD_SETLK && lock->l_type == F_RDLCK &&
      lock->l_start >= (off_t)(PS_METADATA_START - PS_PAGE / 2) &&
      lock->l_start < (off_t)PS_METADATA_START) {
    ps_space *publisher = change->publisher;
    lc_error e = {0};
    change->publisher = NULL;
    ok(ps_flush(publisher, &e), &e);
    lc_error_cleanup(&e);
    ++change->synced;
  }
  return fcntl(fd, cmd, lock);
}
typedef struct scope_acquisition {
  gate acquiring;
  off_t offset;
  int record, armed, reject;
} scope_acquisition;
static int hold_scope_acquisition(void *context, int fd, int cmd,
                                  struct flock *lock) {
  scope_acquisition *scope = context;
  gate *g = &scope->acquiring;
  int reject = 0;
  if (cmd == F_OFD_SETLK && lock->l_type == F_WRLCK && lock->l_len == 1) {
    pthread_mutex_lock(&g->mutex);
    if (scope->record)
      scope->offset = lock->l_start;
    if (scope->armed && lock->l_start == scope->offset) {
      scope->armed = 0;
      g->entered = 1;
      pthread_cond_broadcast(&g->changed);
      while (!g->released)
        pthread_cond_wait(&g->changed, &g->mutex);
      reject = scope->reject;
    }
    pthread_mutex_unlock(&g->mutex);
  }
  if (reject) {
    errno = EIO;
    return -1;
  }
  return fcntl(fd, cmd, lock);
}
static void await_scope_gate(gate *g) {
  pthread_mutex_lock(&g->mutex);
  while (!g->entered && !g->released)
    pthread_cond_wait(&g->changed, &g->mutex);
  REQUIRE(g->entered);
  pthread_mutex_unlock(&g->mutex);
}
static void release_scope_gate(gate *g) {
  pthread_mutex_lock(&g->mutex);
  g->released = 1;
  pthread_cond_broadcast(&g->changed);
  pthread_mutex_unlock(&g->mutex);
}
static void destroy_scope_gate(gate *g) {
  REQUIRE(!pthread_cond_destroy(&g->changed));
  REQUIRE(!pthread_mutex_destroy(&g->mutex));
}
typedef struct scope_job {
  ps_root *root;
  ps_namespace *ns;
  pthread_cond_t *changed;
  gate *waiting, *producing;
  lc_pouch_storage_namespace_info point;
  int rc;
} scope_job;
static void *run_scope_job(void *context) {
  scope_job *job = context;
  lc_pouch_state_entry written = {0};
  lc_error e = {0};
  watched_scope = job->changed;
  scope_wait = job->waiting;
  job->rc = job->producing ? put(job->root, job->ns, 2, 0, 31, job->producing,
                                 NULL, &written, &e)
                           : ps_namespace_projection(job->ns, &job->point, &e);
  if (job->waiting)
    release_scope_gate(job->waiting);
  lc_pouch_state_entry_cleanup(&allocator, &written);
  lc_error_cleanup(&e);
  return NULL;
}
static void shared_scope_handoff(const char *path, int durable, int edit_first,
                                 int reject_read) {
  scope_acquisition scope = {
      {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0}, -1, 0, 0};
  gate waiting = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
  gate producing = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
  ps_coordination_io io = {&scope, hold_scope_acquisition};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns, *other;
  struct ps_namespace_data *data = NULL;
  ps_coordination_guard *discover = NULL;
  ps_namespace_append *overlap = NULL;
  ps_namespace_view *captured = NULL;
  lc_pouch_storage_namespace_info before;
  records old = {0};
  lc_error e = {0};
  pthread_t reader, writer;
  options.storage.shared = 1;
  options.storage.durable_sync = durable;
  options.storage.coordination_io = &io;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ns = acquire(root, "shared", 1, &e);
  other = acquire(root, "independent", 1, &e);
  ok(append(root, ns, 1, 0, 31, &e), &e);
  ok(ps_namespace_projection(ns, &before, &e), &e);
  ok(ps_ns_data_lookup(ns, &data, &e), &e);
  scope.record = 1;
  ok(ps_namespace_guard(ns, &discover, &e), &e);
  scope.record = 0;
  ok(ps_namespace_unguard(ns, discover, LC_OK, &e), &e);
  REQUIRE(scope.offset >= 0);
  scope_job read = {root, ns, &data->scope_changed,
                    edit_first ? &waiting : NULL};
  scope_job edit = {root, ns, &data->scope_changed,
                    edit_first ? NULL : &waiting, &producing};
  scope.armed = 1;
  scope.reject = reject_read;
  REQUIRE(!pthread_create(edit_first ? &writer : &reader, NULL, run_scope_job,
                          edit_first ? &edit : &read));
  await_scope_gate(&scope.acquiring);
  REQUIRE(!pthread_create(edit_first ? &reader : &writer, NULL, run_scope_job,
                          edit_first ? &read : &edit));
  await_scope_gate(&waiting);
  ok(append(root, other, 1, 0, 31, &e), &e);
  release_scope_gate(&scope.acquiring);
  await_scope_gate(&producing);
  REQUIRE(!pthread_join(reader, NULL));
  REQUIRE(read.rc == (reject_read ? LC_ERR_TRANSPORT : LC_OK));
  if (!reject_read)
    REQUIRE(read.point.tail == before.tail &&
            read.point.active_incarnation == before.active_incarnation);
  REQUIRE(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &overlap,
                                    &e) == LC_ERR_INVALID &&
          !overlap);
  REQUIRE(e.server_code && !strcmp(e.server_code, "pouch_namespace_busy"));
  clear(&e);
  ok(ps_namespace_view_open(ns, &captured, &e), &e);
  release_scope_gate(&producing);
  REQUIRE(!pthread_join(writer, NULL) && edit.rc == LC_OK);
  ok(ps_namespace_view_visit(captured, 1, visit, &old, &e), &e);
  REQUIRE(old.count == 1 && old.version == 1);
  ps_namespace_view_close(captured);
  count(ns, 2, &e);
  ps_namespace_release(other);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  destroy_scope_gate(&scope.acquiring);
  destroy_scope_gate(&waiting);
  destroy_scope_gate(&producing);
  REQUIRE(!unlink(path));
  lc_error_cleanup(&e);
}
static void shared_handoff(const char *path) {
  for (int durable = 0; durable <= 1; ++durable) {
    lc_error e = {0};
    ps_root_options options = {0};
    checkpoint_visibility visibility = {0};
    ps_coordination_io io = {&visibility, advance_checkpoint_visibility};
    ps_root *a = NULL, *b = NULL;
    ps_namespace *x, *y, *other;
    ps_namespace_view *old = NULL, *view = NULL;
    ps_namespace_window *window = NULL, *peer_window = NULL;
    ps_namespace_append *held = NULL;
    ps_namespace_status status;
    lc_pouch_storage_namespace_info point;
    ps_checkpoint_info deferred;
    struct ps_namespace_data *owner;
    lc_pouch_state_entry written = {0};
    uint64_t segment = 0, old_tail;
    unsigned char bytes[64];
    struct iovec part = {bytes, sizeof(bytes)};
    records original = {0}, fresh = {0}, suffix = {0};
    shared_records checkpoint = {0};
    shared_namespace = 1;
    options.storage.durable_sync = durable;
    options.storage.coordination_io = &io;
    ok(ps_root_open(path, 1, &options, &a, &e), &e);
    x = acquire(a, "shared", 1, &e);
    other = acquire(a, "independent", 1, &e);
    ok(put(a, x, 1, 0, sizeof(bytes), NULL, &segment, &written, &e), &e);
    REQUIRE(segment == 1 && written.cipher_bytes == sizeof(bytes));
    ok(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                              written.payload_span.payload_offset,
                              sizeof(bytes), &window, &e),
       &e);
    ok(ps_namespace_view_open(x, &old, &e), &e);
    ps_namespace_view_projection(old, &point);
    old_tail = point.tail;
    ok(ps_root_open(path, 0, &options, &b, &e), &e);
    y = acquire(b, "shared", 0, &e);
    ok(ps_namespace_append_begin(y, 0, 0, PS_APPEND_PREFIX, NULL, &held, &e),
       &e);
    REQUIRE(append(a, x, 2, 0, 65, &e) == LC_ERR_PENDING);
    clear(&e);
    REQUIRE(ps_namespace_view_open(x, &view, &e) == LC_ERR_PENDING && !view);
    clear(&e);
    REQUIRE(ps_namespace_projection(x, &point, &e) == LC_ERR_PENDING);
    clear(&e);
    ok(append(a, other, 1, 0, 65, &e), &e);
    ps_namespace_append_close(held);
    ok(append(b, y, 2, 0, 65, &e), &e);
    fail_namespace_load = 1;
    ok(ps_namespace_projection(x, &point, &e), &e);
    REQUIRE(fail_namespace_load == 1);
    ok(ps_namespace_status_get(y, &status, &e), &e);
    REQUIRE(fail_namespace_load == 1 && point.active_segment == 1 &&
            point.active_incarnation && point.tail == status.tail &&
            point.tail > written.payload_span.payload_offset &&
            point.checkpoint_epoch == status.checkpoint_epoch);
    ok(ps_namespace_view_suffix_open(x, &point, old_tail, &view, &e), &e);
    ok(ps_namespace_view_visit(view, 1, visit, &suffix, &e), &e);
    ps_namespace_view_close(view);
    view = NULL;
    REQUIRE(suffix.count == 1 && suffix.version == 2);
    ok(capture(x, &fresh, &e), &e);
    REQUIRE(fail_namespace_load == 1 && fresh.count == 2 &&
            fresh.version == 2 && fresh.last_segment == 1);
    ok(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                              written.payload_span.payload_offset,
                              sizeof(bytes), &peer_window, &e),
       &e);
    REQUIRE(fail_namespace_load == 1);
    REQUIRE(ps_namespace_status_get(x, &status, &e) == LC_ERR_NOMEM);
    REQUIRE(fail_namespace_load == 2);
    fail_namespace_load = 0;
    clear(&e);
    ok(ps_ns_data_get(x, &owner, &e), &e);
    REQUIRE(owner->replay_segment == 1 && owner->replay_offset == old_tail);
    ok(ps_namespace_checkpoint_recover(x, &e), &e);
    REQUIRE(owner->checkpoint_ready && !owner->replay_segment &&
            ps_namespace_source_refs(x, PS_CATALOG_SEGMENT, 1) == 1);
    ok(ps_namespace_view_visit(old, 1, visit, &original, &e), &e);
    REQUIRE(original.count == 1 && original.version == 1);
    ps_namespace_view_close(old);
    old = NULL;
    ok(ps_ns_enter_checkpoint(y, &owner, &e), &e);
    pthread_mutex_lock(&owner->mutex);
    ok(ps_key_checkpoint_flush_deferred(owner->checkpoint, &e), &e);
    ps_key_checkpoint_info(owner->checkpoint, &deferred);
    pthread_mutex_unlock(&owner->mutex);
    ps_ns_leave(owner);
    REQUIRE(deferred.epoch && deferred.segment == 1);
    ok(ps_namespace_projection(y, &point, &e), &e);
    REQUIRE(point.checkpoint_epoch == deferred.epoch);
    ok(ps_namespace_projection(x, &point, &e), &e);
    REQUIRE(point.checkpoint_epoch == (durable ? 0 : deferred.epoch));
    if (durable) {
      ok(ps_flush(ps_root_space(b), &e), &e);
    } else {
      /* The observer's seed predates the sync. Make the prepared checkpoint
       * visible at acquisition of the replay view, while the name is held. */
      visibility.publisher = ps_root_space(b);
      ok(ps_namespace_checkpoint_recover(x, &e), &e);
      REQUIRE(visibility.synced == 1 && !visibility.publisher);
    }
    ok(ps_namespace_projection(x, &point, &e), &e);
    REQUIRE(point.checkpoint_epoch == deferred.epoch);
    ok(append(b, y, 3, 1, 65, &e), &e);
    ok(ps_namespace_checkpoint_flush(y, &e), &e);
    ok(ps_checkpoint(ps_root_space(b), &e), &e);
    fail_namespace_load = 1;
    REQUIRE(ps_namespace_projection(x, &point, &e) == LC_ERR_NOMEM);
    REQUIRE(fail_namespace_load == 2);
    fail_namespace_load = 0;
    clear(&e);
    ok(ps_namespace_projection(x, &point, &e), &e);
    ok(ps_namespace_status_get(x, &status, &e), &e);
    REQUIRE(status.active_segment == 2 && status.checkpoint_keys == 1 &&
            status.covered_segment == 2 && point.active_segment == 2 &&
            point.tail == status.tail && point.checkpoint_epoch &&
            point.checkpoint_epoch == status.checkpoint_epoch);
    ok(ps_namespace_view_open(x, &view, &e), &e);
    ok(ps_namespace_view_visit(view, 1, visit_shared, &checkpoint, &e), &e);
    REQUIRE(checkpoint.index == 3 && checkpoint.version == 3 &&
            checkpoint.keys == 1);
    ps_namespace_view_close(view);
    ok(lc_pouch_record_io_read(ps_namespace_read_input(window),
                               written.payload_span.payload_offset, &part, 1,
                               &e),
       &e);
    REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
    memset(bytes, 0, sizeof(bytes));
    ok(lc_pouch_record_io_read(ps_namespace_read_input(peer_window),
                               written.payload_span.payload_offset, &part, 1,
                               &e),
       &e);
    REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
    ps_namespace_read_close(peer_window);
    lc_pouch_state_entry_cleanup(&allocator, &written);
    ps_namespace_read_close(window);
    ps_namespace_view_close(old);
    ps_namespace_release(y);
    ps_namespace_release(x);
    ps_namespace_release(other);
    ok(ps_root_close(b, &e), &e);
    ok(ps_root_close(a, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
  shared_namespace = 0;
}
typedef struct startup_checkpoint_probe {
  const char *path;
  uint64_t page;
  int durable, reads;
} startup_checkpoint_probe;
static ssize_t advance_startup_checkpoint(void *context, int fd, void *bytes,
                                          size_t n, off_t offset) {
  startup_checkpoint_probe *probe = context;
  if ((uint64_t)offset == probe->page && n == PS_PAGE && ++probe->reads == 1) {
    lc_error e = {0};
    ps_space *peer = NULL;
    ps_options options = {.durable_sync = probe->durable, .shared = 1};
    ok(ps_open(probe->path, 0, &options, &peer, &e), &e);
    ok(ps_metadata_repack(peer, &e), &e);
    ok(ps_close(peer, &e), &e);
    clear(&e);
    errno = EIO;
    return -1;
  }
  return pread(fd, bytes, n, offset);
}
static void shared_startup_checkpoint(const char *path, int durable) {
  lc_error e = {0};
  ps_root *root = NULL;
  ps_namespace_view *view = NULL;
  ps_root_options options = {0};
  ps_namespace_info identity;
  shared_records recovered = {0};
  startup_checkpoint_probe probe = {.path = path, .durable = durable};
  ps_io io = {&probe,         advance_startup_checkpoint,
              direct_write,   direct_sync,
              direct_reserve, NULL};
  options.storage.shared = 1;
  options.storage.durable_sync = durable;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "startup", 1, &e);
  lc_pouch_state_entry result = {0};
  char content_type[PS_KEY_RECORD_INLINE_MAX + 1];
  memset(content_type, 't', sizeof(content_type) - 1);
  content_type[sizeof(content_type) - 1] = 0;
  ok(put_with_content_type(root, ns, 1, 0, 64, NULL, NULL, &result,
                           content_type, &e),
     &e);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  ok(ps_namespace_checkpoint_flush(ns, &e), &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ps_space *space = ps_root_space(root);
  ok(ps_checkpoint(space, &e), &e);
  for (const ps_entry *p = space->entries; p; p = p->all_next)
    if (p->key.kind == PS_KEY_METADATA && p->key.owner == identity.owner &&
        !p->deleted && p->count && (p->extents[0].flags & EXTENT_PACKED)) {
      probe.page = p->extents[0].offset / PS_PAGE * PS_PAGE;
      break;
    }
  REQUIRE(probe.page >= PS_DATA);
  uint64_t generation = space->generation;
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);

  options.storage.io = &io;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  REQUIRE(probe.reads >= 2 && ps_root_space(root)->generation > generation);
  ns = acquire(root, "startup", 0, &e);
  ok(ps_namespace_view_open(ns, &view, &e), &e);
  ok(ps_namespace_view_visit(view, 1, visit_shared, &recovered, &e), &e);
  REQUIRE(recovered.keys == 1 && recovered.version == 1 &&
          recovered.index == 1);
  ps_namespace_view_close(view);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(!unlink(path));
}
enum join_admission_fault {
  JOIN_NO_FAULT,
  JOIN_READ_ERROR,
  JOIN_JOURNAL_RACE,
  JOIN_CHECKPOINT_RACE
};
typedef struct join_admission_probe {
  ps_root *peer;
  ps_namespace *writer;
  uint64_t offsets[2];
  size_t sizes[2];
  enum join_admission_fault fault;
  unsigned int seen;
  int allocator, captured_reads, injected;
} join_admission_probe;
static int join_admission_lock(void *context, int fd, int command,
                               struct flock *lock) {
  join_admission_probe *probe = context;
  int rc = fcntl(fd, command, lock);
  if (!rc && lock->l_start == 4) {
    if (command == F_OFD_SETLKW && lock->l_type == F_WRLCK)
      probe->allocator = 1;
    else if (command == F_OFD_SETLK && lock->l_type == F_UNLCK)
      probe->allocator = 0;
  }
  return rc;
}
static ssize_t join_admission_read(void *context, int fd, void *bytes, size_t n,
                                   off_t offset) {
  join_admission_probe *probe = context;
  for (size_t i = 0; i < 2; ++i) {
    if ((uint64_t)offset != probe->offsets[i] || n != probe->sizes[i])
      continue;
    if (probe->allocator) {
      ++probe->captured_reads;
      break;
    }
    probe->seen |= 1U << i;
    if (probe->fault && !probe->injected) {
      lc_error e = {0};
      probe->injected = 1;
      if (probe->fault == JOIN_JOURNAL_RACE)
        ok(append(probe->peer, probe->writer, 2, 0, 65, &e), &e);
      else if (probe->fault == JOIN_CHECKPOINT_RACE)
        ok(ps_metadata_repack(ps_root_space(probe->peer), &e), &e);
      clear(&e);
      errno = EIO;
      return -1;
    }
    break;
  }
  return pread(fd, bytes, n, offset);
}
static void shared_join_admission(const char *path, int durable,
                                  enum join_admission_fault fault) {
  const char *names[] = {"first", "second"};
  lc_error e = {0};
  ps_root *joiner = NULL;
  ps_namespace *writers[2];
  ps_root_options options = {0};
  join_admission_probe probe = {.fault = fault};
  ps_coordination_io locks = {&probe, join_admission_lock};
  ps_io io = {&probe,      join_admission_read, direct_write,
              direct_sync, direct_reserve,      NULL};
  options.storage.shared = 1;
  options.storage.durable_sync = durable;
  ok(ps_root_open(path, 1, &options, &probe.peer, &e), &e);
  ps_space *space = ps_root_space(probe.peer);
  for (size_t i = 0; i < 2; ++i) {
    ps_namespace_info identity;
    ps_key key = {0, 1, PS_ROUTING};
    writers[i] = acquire(probe.peer, names[i], 1, &e);
    ok(append(probe.peer, writers[i], 1, 0, 64, &e), &e);
    ok(ps_namespace_checkpoint_flush(writers[i], &e), &e);
    ok(ps_namespace_identity(writers[i], &identity, &e), &e);
    key.owner = identity.owner;
    const ps_entry *p = ps_internal_lookup(space, key);
    REQUIRE(p && p->count == 1);
    probe.offsets[i] = p->extents[0].offset;
    probe.sizes[i] = (size_t)p->bytes;
  }
  probe.writer = writers[0];
  uint64_t generation = space->generation;
  options.storage.io = &io;
  options.storage.coordination_io = &locks;
  int rc = ps_root_open(path, 0, &options, &joiner, &e);
  if (fault == JOIN_READ_ERROR) {
    REQUIRE(rc == LC_ERR_TRANSPORT && !joiner && probe.injected);
    clear(&e);
  } else {
    ok(rc, &e);
    if (fault == JOIN_NO_FAULT)
      REQUIRE(probe.seen == 3U && !probe.captured_reads);
    else {
      REQUIRE(probe.injected && probe.seen);
      if (fault == JOIN_JOURNAL_RACE)
        REQUIRE(space->generation == generation);
      else
        REQUIRE(ps_root_space(joiner)->generation > generation);
    }
    for (size_t i = 0; i < 2; ++i) {
      ps_namespace_view *view = NULL;
      shared_records recovered = {0};
      size_t expected = i == 0 && fault == JOIN_JOURNAL_RACE ? 2 : 1;
      ps_namespace *ns = acquire(joiner, names[i], 0, &e);
      ok(ps_namespace_view_open(ns, &view, &e), &e);
      ok(ps_namespace_view_visit(view, 1, visit_shared, &recovered, &e), &e);
      REQUIRE(recovered.keys == expected && recovered.version == expected);
      ps_namespace_view_close(view);
      ps_namespace_release(ns);
    }
    ok(ps_root_close(joiner, &e), &e);
  }
  ps_namespace_release(writers[1]);
  ps_namespace_release(writers[0]);
  ok(ps_root_close(probe.peer, &e), &e);
  REQUIRE(!unlink(path));
}
typedef struct projection_probe {
  ps_root *peer_root;
  ps_namespace *peer;
  size_t acquisitions, headers;
  off_t header;
  int armed, held, reject, corrupt, discover, checkpoint_peer, expect_allocator;
} projection_probe;
static int projection_lock(void *context, int fd, int command,
                           struct flock *lock) {
  projection_probe *p = context;
  int take = p->armed && lock->l_start == 4 && command == F_OFD_SETLKW &&
             lock->l_type == F_WRLCK;
  if (take && p->reject) {
    errno = EAGAIN;
    return -1;
  }
  int rc = fcntl(fd, command, lock);
  if (!rc && p->armed && (p->peer || p->checkpoint_peer) &&
      command == F_OFD_SETLK && lock->l_type == F_RDLCK &&
      lock->l_start >= PS_DATA && lock->l_start < (off_t)PS_DEFAULT_CAPACITY) {
    lc_error e = {0};
    REQUIRE(!p->held && !p->acquisitions);
    if (p->peer) {
      ps_namespace *ns = p->peer;
      p->peer = NULL;
      ok(append(p->peer_root, ns, 1, 0, 65, &e), &e);
    } else {
      p->checkpoint_peer = 0;
      ok(ps_checkpoint(ps_root_space(p->peer_root), &e), &e);
    }
    clear(&e);
  }
  if (rc || !p->armed || lock->l_start != 4)
    return rc;
  if (take) {
    REQUIRE(!p->held);
    p->held = 1;
    ++p->acquisitions;
  } else if (command == F_OFD_SETLK && lock->l_type == F_UNLCK) {
    REQUIRE(p->held);
    p->held = 0;
  }
  return rc;
}
static ssize_t projection_read(void *context, int fd, void *bytes, size_t n,
                               off_t at) {
  projection_probe *p = context;
  ssize_t rc = pread(fd, bytes, n, at);
  if (p->discover && rc == PS_RUN_IDENTITY_BYTES && at >= PS_DATA &&
      !(at % PS_PAGE))
    p->header = at;
  if (p->armed && rc == PS_RUN_IDENTITY_BYTES && at == p->header) {
    if (p->held || (p->acquisitions != 0) != p->expect_allocator)
      fprintf(stderr,
              "stream header read with allocator held=%d, acquisitions=%zu, "
              "expect allocator=%d\n",
              p->held, p->acquisitions, p->expect_allocator);
    REQUIRE(!p->held && (p->acquisitions != 0) == p->expect_allocator);
    ++p->headers;
    if (p->corrupt)
      *(unsigned char *)bytes ^= 1;
  }
  return rc;
}
static void shared_projection_capture(const char *path, int durable) {
  projection_probe probe = {0};
  ps_coordination_io locks = {&probe, projection_lock};
  ps_io disk = {&probe,      projection_read, direct_write,
                direct_sync, direct_reserve,  NULL};
  ps_root_options options = {0};
  ps_root *a = NULL, *b = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_window *window = NULL;
  lc_pouch_storage_namespace_info before, after;
  lc_pouch_state_entry written = {0};
  ps_coordination_guard *allocator_guard = NULL;
  ps_stats st;
  lc_error e = {0};
  records selected = {0};
  uint64_t segment;
  unsigned char bytes[65];
  struct iovec part = {bytes, sizeof(bytes)};
  options.storage.shared = 1;
  options.storage.durable_sync = durable;
  options.storage.io = &disk;
  options.storage.coordination_io = &locks;
  ok(ps_root_open(path, 1, &options, &a, &e), &e);
  ps_namespace *x = acquire(a, "projection", 1, &e);
  ok(put(a, x, 1, 0, sizeof(bytes), NULL, &segment, &written, &e), &e);
  options.storage.io = NULL;
  options.storage.coordination_io = NULL;
  ok(ps_root_open(path, 0, &options, &b, &e), &e);
  ps_namespace *peer = acquire(b, "independent", 1, &e);
  ok(ps_namespace_projection(x, &before, &e), &e);
  REQUIRE(before.active_segment == segment && before.tail);
  probe.discover = 1;
  ok(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                            written.payload_span.payload_offset, sizeof(bytes),
                            &window, &e),
     &e);
  probe.discover = 0;
  REQUIRE(probe.header >= PS_DATA);
  ps_namespace_read_close(window);
  window = NULL;
  ok(ps_coordination_lock(ps_space_coordination(ps_root_space(b)),
                          PS_COORDINATION_ALLOCATION, 0, 1, &allocator_guard,
                          &e),
     &e);
  probe.armed = probe.reject = 1;
  ok(ps_namespace_projection(x, &after, &e), &e);
  REQUIRE(!probe.acquisitions && !probe.headers && after.tail == before.tail &&
          after.active_incarnation == before.active_incarnation &&
          after.checkpoint_epoch == before.checkpoint_epoch);
  ok(ps_namespace_view_open(x, &view, &e), &e);
  REQUIRE(!probe.acquisitions && probe.headers == 1);
  ps_namespace_view_close(view);
  view = NULL;
  ok(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                            written.payload_span.payload_offset, sizeof(bytes),
                            &window, &e),
     &e);
  REQUIRE(!probe.acquisitions && probe.headers == 2);
  ps_namespace_read_close(window);
  window = NULL;
  ps_coordination_unlock(allocator_guard);
  probe.acquisitions = probe.headers = 0;
  probe.reject = 0;
  probe.peer_root = b;
  probe.peer = peer;
  ok(ps_namespace_view_open(x, &view, &e), &e);
  REQUIRE(!probe.acquisitions && probe.headers && !probe.peer);
  probe.armed = 0;
  ok(ps_namespace_view_visit(view, 1, visit, &selected, &e), &e);
  REQUIRE(selected.count == 1 && selected.version == 1);
  ps_namespace_view_close(view);
  count(peer, 1, &e);
  ok(ps_namespace_projection(x, &after, &e), &e);

  probe.acquisitions = probe.headers = 0;
  probe.expect_allocator = 0;
  probe.armed = probe.corrupt = 1;
  REQUIRE(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                                 written.payload_span.payload_offset,
                                 sizeof(bytes), &window,
                                 &e) == LC_ERR_PROTOCOL &&
          !window);
  REQUIRE(!probe.acquisitions && probe.headers == 1 && !probe.held);
  clear(&e);
  probe.armed = probe.corrupt = 0;
  ok(ps_status(ps_root_space(a), &st, &e), &e);
  REQUIRE(!st.retained_payload_bytes);
  probe.acquisitions = probe.headers = 0;
  probe.armed = 1;
  ok(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, segment,
                            written.payload_span.payload_offset, sizeof(bytes),
                            &window, &e),
     &e);
  REQUIRE(!probe.acquisitions && probe.headers == 1 && !probe.held);
  probe.armed = 0;
  ok(lc_pouch_record_io_read(ps_namespace_read_input(window),
                             written.payload_span.payload_offset, &part, 1, &e),
     &e);
  REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
  ps_namespace_read_close(window);

  memset(&selected, 0, sizeof(selected));
  probe.acquisitions = probe.headers = 0;
  probe.armed = probe.checkpoint_peer = probe.expect_allocator = 1;
  ok(ps_namespace_view_open(x, &view, &e), &e);
  REQUIRE(probe.acquisitions && probe.headers && !probe.checkpoint_peer);
  probe.armed = 0;
  ok(ps_namespace_view_visit(view, 1, visit, &selected, &e), &e);
  REQUIRE(selected.count == 1 && selected.version == 1);
  ps_namespace_view_close(view);
  lc_pouch_state_entry_cleanup(&allocator, &written);
  ps_namespace_release(peer);
  ps_namespace_release(x);
  ok(ps_root_close(b, &e), &e);
  ok(ps_root_close(a, &e), &e);
  REQUIRE(!unlink(path));
}
typedef struct shared_body_pin {
  ps_namespace *ns;
  ps_namespace_window *window;
  uint64_t offset;
} shared_body_pin;
static int open_old_body(void *context, uint64_t segment,
                         const lc_pouch_state_entry *entry, lc_error *e) {
  shared_body_pin *pin = context;
  unsigned int kind;
  uint64_t id;
  (void)segment;
  if (entry->control)
    return LC_OK;
  REQUIRE(!pin->window && entry->found && entry->version == 1);
  REQUIRE(entry->payload_span.payload_length == sizeof(body));
  REQUIRE(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                         &kind, &id));
  REQUIRE(kind == PS_CATALOG_SEGMENT && id == 1);
  pin->offset = entry->payload_span.payload_offset;
  return ps_namespace_read_open(pin->ns, kind, id, pin->offset, 64,
                                &pin->window, e);
}
static void source_busy(int rc, lc_error *e) {
  REQUIRE(rc == LC_ERR_INVALID && e->server_code &&
          !strcmp(e->server_code, "pouch_namespace_busy"));
  clear(e);
}
typedef struct late_view {
  ps_namespace *ns;
  ps_coordination_guard *witness;
} late_view;
static int install_late_view(void *context, ps_piece_publish_fn publish,
                             void *publication, lc_error *e) {
  late_view *late = context;
  ps_coordination_guard *guard = NULL;
  ps_coordination *coordination =
      ps_space_coordination(ps_root_space(late->ns->root));
  REQUIRE(!late->witness);
  ok(ps_namespace_guard(late->ns, &guard, e), e);
  ok(ps_coordination_lock(coordination, PS_COORDINATION_NAMESPACE_VIEW,
                          late->ns->hash, 0, &late->witness, e),
     e);
  ok(ps_namespace_unguard(late->ns, guard, LC_OK, e), e);
  return publish(publication, e);
}
static void shared_source_pins(const char *path) {
  shared_namespace = 1;
  for (int durable = 0; durable <= 1; ++durable) {
    lc_error e = {0};
    ps_root_options options = {0};
    ps_root *a = NULL, *b = NULL;
    ps_namespace_view *old = NULL, *second = NULL, *fresh = NULL;
    ps_namespace_window *removed = NULL;
    shared_body_pin pin = {0};
    ps_namespace_repack_result result;
    ps_namespace_status selected;
    ps_stats physical;
    unsigned char bytes[64];
    struct iovec part = {bytes, sizeof(bytes)};
    options.storage.durable_sync = durable;
    ok(ps_root_open(path, 1, &options, &a, &e), &e);
    ps_namespace *x = acquire(a, "shared", 1, &e);
    ok(append(a, x, 1, 0, sizeof(body), &e), &e);
    ok(ps_namespace_status_get(x, &selected, &e), &e);
    REQUIRE(selected.runs > 2);
    ok(ps_namespace_checkpoint_flush(x, &e), &e);
    ok(ps_namespace_view_open(x, &old, &e), &e);
    ok(ps_namespace_view_open(x, &second, &e), &e);

    ok(ps_root_open(path, 0, &options, &b, &e), &e);
    ps_namespace *y = acquire(b, "shared", 0, &e);
    ok(append(b, y, 2, 1, sizeof(bytes), &e), &e);
    ok(ps_namespace_checkpoint_flush(y, &e), &e);
    REQUIRE(!ps_namespace_source_refs(y, PS_CATALOG_SEGMENT, 1));
    ok(ps_namespace_view_open(y, &fresh, &e), &e);
    shared_records latest = {0};
    ok(ps_namespace_view_visit(fresh, 1, visit_shared, &latest, &e), &e);
    REQUIRE(latest.version == 2 && latest.keys == 1);
    ps_namespace_view_close(fresh);
    source_busy(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
    source_busy(ps_namespace_repack(y, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                                    &result, &e),
                &e);

    ps_namespace *other = acquire(b, "independent", 1, &e);
    ok(append(b, other, 1, 0, sizeof(bytes), &e), &e);
    ok(append(b, other, 2, 1, sizeof(bytes), &e), &e);
    ok(ps_namespace_reclaim(other, PS_CATALOG_SEGMENT, 1, &e), &e);
    ps_namespace_release(other);
    ps_namespace_view_close(old);
    source_busy(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
    pin.ns = x;
    ok(ps_namespace_view_visit(second, 1, open_old_body, &pin, &e), &e);
    REQUIRE(pin.window);
    ps_namespace_view_close(second);
    ok(ps_status(ps_root_space(a), &physical, &e), &e);
    REQUIRE(physical.retained_payload_bytes &&
            physical.retained_payload_bytes <= 2 * PS_CHUNK);
    ok(ps_namespace_checkpoint_flush(y, &e), &e);

    /* Inject a foreign witness after preparation, under the same name
     * guard used by view capture. Publication must recheck liveness. */
    late_view late = {x, NULL};
    ok(ps_namespace_repack(y, PS_CATALOG_SEGMENT, 1, 1, install_late_view,
                           &late, &result, &e),
       &e);
    REQUIRE(late.witness && result.visited == 1 && result.pending &&
            !result.replaced);
    source_busy(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
    ps_coordination_unlock(late.witness);
    ok(ps_namespace_repack(y, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL, &result,
                           &e),
       &e);
    REQUIRE(result.replaced == 1);
    ok(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
    REQUIRE(ps_namespace_read_open(x, PS_CATALOG_SEGMENT, 1, pin.offset,
                                   sizeof(bytes), &removed,
                                   &e) == LC_ERR_INVALID &&
            !removed);
    clear(&e);
    ok(lc_pouch_record_io_read(ps_namespace_read_input(pin.window), pin.offset,
                               &part, 1, &e),
       &e);
    REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
    ps_namespace_read_close(pin.window);
    ok(ps_status(ps_root_space(a), &physical, &e), &e);
    REQUIRE(!physical.retained_payload_bytes);
    ps_namespace_release(y);
    ps_namespace_release(x);
    ok(ps_root_close(b, &e), &e);
    ok(ps_root_close(a, &e), &e);
    REQUIRE(unlink(path) == 0);
  }
  shared_namespace = 0;
}
typedef struct view_fault {
  int cmd, type, err, fired;
} view_fault;
static int view_fault_lock(void *context, int fd, int cmd, struct flock *lock) {
  view_fault *fault = context;
  if (cmd == fault->cmd && lock->l_type == fault->type &&
      lock->l_start >= (off_t)(PS_METADATA_START - PS_PAGE / 2) &&
      lock->l_start < (off_t)PS_METADATA_START) {
    fault->cmd = 0;
    ++fault->fired;
    errno = fault->err;
    return -1;
  }
  return fcntl(fd, cmd, lock);
}
static void shared_source_faults(const char *path) {
  lc_error e = {0};
  ps_root_options options = {0};
  ps_root *a = NULL, *b = NULL;
  ps_namespace_view *view = NULL;
  view_fault fault = {0};
  ps_coordination_io io = {&fault, view_fault_lock};
  shared_namespace = 1;
  options.storage.durable_sync = 1;
  options.storage.coordination_io = &io;
  ok(ps_root_open(path, 1, &options, &a, &e), &e);
  ps_namespace *x = acquire(a, "shared", 1, &e);
  ok(append(a, x, 1, 0, 64, &e), &e);
  ok(ps_namespace_checkpoint_flush(x, &e), &e);
  fault = (view_fault){F_OFD_SETLK, F_RDLCK, EAGAIN, 0};
  REQUIRE(ps_namespace_view_open(x, &view, &e) == LC_ERR_PENDING && !view);
  REQUIRE(fault.fired == 1);
  clear(&e);
  fault = (view_fault){F_OFD_SETLK, F_RDLCK, EIO, 0};
  REQUIRE(ps_namespace_view_open(x, &view, &e) == LC_ERR_TRANSPORT && !view);
  REQUIRE(fault.fired == 1);
  clear(&e);
  ok(ps_namespace_view_open(x, &view, &e), &e);
  ok(ps_root_open(path, 0, &options, &b, &e), &e);
  ps_namespace *y = acquire(b, "shared", 0, &e);
  ok(append(b, y, 2, 1, 64, &e), &e);
  ok(ps_namespace_checkpoint_flush(y, &e), &e);
  fault = (view_fault){F_OFD_GETLK, F_WRLCK, EIO, 0};
  REQUIRE(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e) ==
          LC_ERR_TRANSPORT);
  REQUIRE(fault.fired == 1);
  clear(&e);
  REQUIRE(ps_check(ps_root_space(b), &e) == LC_ERR_TRANSPORT);
  clear(&e);
  ps_namespace_release(y);
  ok(ps_root_close(b, &e), &e);
  b = NULL;
  ok(ps_root_open(path, 0, &options, &b, &e), &e);
  y = acquire(b, "shared", 0, &e);
  fault = (view_fault){F_OFD_SETLK, F_UNLCK, EIO, 0};
  ps_namespace_view_close(view);
  REQUIRE(fault.fired == 1);
  REQUIRE(ps_check(ps_root_space(a), &e) == LC_ERR_TRANSPORT);
  clear(&e);
  source_busy(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
  ps_namespace_release(x);
  ok(ps_root_close(a, &e), &e);
  ok(ps_namespace_reclaim(y, PS_CATALOG_SEGMENT, 1, &e), &e);
  ps_namespace_release(y);
  ok(ps_root_close(b, &e), &e);
  REQUIRE(unlink(path) == 0);
  shared_namespace = 0;
}
static void namespace_view_child(const char *path, int notify) {
  lc_error e = {0};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace_view *view = NULL;
  shared_namespace = 1;
  options.storage.durable_sync = 1;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "shared", 0, &e);
  ok(ps_namespace_view_open(ns, &view, &e), &e);
  REQUIRE(write(notify, "R", 1) == 1);
  for (;;)
    pause();
}
static void shared_source_exit(const char *path) {
  lc_error e = {0};
  ps_root_options options = {0};
  ps_root *root = NULL;
  int notify[2], exited;
  char byte;
  shared_namespace = 1;
  options.storage.durable_sync = 1;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "shared", 1, &e);
  ok(append(root, ns, 1, 0, 64, &e), &e);
  ok(ps_namespace_checkpoint_flush(ns, &e), &e);
  REQUIRE(pipe(notify) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char fd[24];
    REQUIRE(close(notify[0]) == 0);
    snprintf(fd, sizeof(fd), "%d", notify[1]);
    execlp(program, program, "--view-child", path, fd, (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(notify[1]) == 0);
  REQUIRE(read(notify[0], &byte, 1) == 1 && byte == 'R');
  ok(append(root, ns, 2, 1, 64, &e), &e);
  source_busy(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &e), &e);
  REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(close(notify[0]) == 0);
  ok(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, 1, &e), &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  shared_namespace = 0;
}
static void namespace_abandoned_child(const char *path, int notify) {
  lc_error e = {0};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace_append *append = NULL;
  lc_pouch_state_entry request = {0}, discarded = {0};
  input source = {0};
  memset(body, '!', sizeof(body));
  source.pub.impl = &source;
  source.pub.read = pull;
  source.bytes = sizeof(body);
  request.key = "key";
  request.content_type = "application/test";
  request.payload_context = "namespace-test";
  request.version = request.index_seq = 2;
  request.lifetime = 1;
  request.record_type = 1;
  shared_namespace = 1;
  options.storage.durable_sync = 1;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "shared", 0, &e);
  ok(ps_namespace_append_begin(ns, 1, 0, PS_APPEND_PREFIX, NULL, &append, &e),
     &e);
  REQUIRE(ps_namespace_append_segment(append) == 2);
  ok(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                  ps_namespace_append_output(append),
                                  ps_namespace_append_leaf(append), &request,
                                  &source.pub, 0, &discarded, &e),
     &e);
  ok(ps_flush(ps_root_space(root), &e), &e);
  REQUIRE(write(notify, "W", 1) == 1);
  for (;;)
    pause();
}
static void shared_abandoned(const char *path) {
  lc_error e = {0};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace_view *old = NULL;
  ps_namespace_status current;
  records before = {0}, after = {0};
  int notify[2], exited;
  char byte;
  shared_namespace = 1;
  options.storage.durable_sync = 1;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "shared", 1, &e);
  ok(append(root, ns, 1, 0, 128, &e), &e);
  ok(ps_namespace_view_open(ns, &old, &e), &e);
  REQUIRE(pipe(notify) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char fd[24];
    REQUIRE(close(notify[0]) == 0);
    snprintf(fd, sizeof(fd), "%d", notify[1]);
    execlp(program, program, "--shared-child", path, fd, (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(notify[1]) == 0);
  REQUIRE(read(notify[0], &byte, 1) == 1 && byte == 'W');
  REQUIRE(ps_namespace_status_get(ns, &current, &e) == LC_ERR_PENDING);
  clear(&e);
  REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(close(notify[0]) == 0);
  ok(ps_namespace_view_visit(old, 1, visit, &before, &e), &e);
  REQUIRE(before.count == 1 && before.version == 1);
  ok(ps_namespace_status_get(ns, &current, &e), &e);
  REQUIRE(current.active_segment == 1 && current.bound_segments == 2);
  ok(append(root, ns, 2, 1, 128, &e), &e);
  ok(capture(ns, &after, &e), &e);
  REQUIRE(after.count == 2 && after.version == 2 && after.last_segment == 2);
  ps_namespace_view_close(old);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "shared", 0, &e);
  count(ns, 2, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  shared_namespace = 0;
}
typedef struct successor_barrier {
  int notify, flush_first, armed;
} successor_barrier;
static ssize_t successor_read(void *context, int fd, void *bytes, size_t n,
                              off_t at) {
  (void)context;
  return pread(fd, bytes, n, at);
}
static int successor_sync(void *context, int fd) {
  successor_barrier *barrier = context;
  if (barrier->armed) {
    /* Leave the framed binding at either edge of the sync callback,
     * before the process certifies the result to its surviving peer. */
    if (barrier->flush_first)
      REQUIRE(fdatasync(fd) == 0);
    REQUIRE(write(barrier->notify, "P", 1) == 1);
    for (;;)
      pause();
  }
  return fdatasync(fd);
}
static void successor_child(const char *path, int notify, int mode) {
  successor_barrier barrier = {notify, mode & 1, 0};
  ps_io io = {&barrier,       successor_read, direct_write,
              successor_sync, direct_reserve, NULL};
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace_append *appender = NULL;
  lc_error e = {0};
  shared_namespace = 1;
  options.storage.durable_sync = mode >> 1;
  options.storage.io = &io;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "successor", 0, &e);
  barrier.armed = 1;
  ok(ps_namespace_append_begin(ns, 0, 1, PS_APPEND_PREFIX, NULL, &appender, &e),
     &e);
  REQUIRE(0);
}
static void shared_successor(const char *path, int mode) {
  const char *leaf = "seg-00000000000000000001.log";
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_catalog_view *binding = NULL;
  ps_catalog_info repaired;
  ps_namespace_info identity;
  ps_namespace_status admitted;
  ps_owned_status staged;
  int notify[2], exited;
  char event;
  lc_error e = {0};
  shared_namespace = 1;
  options.storage.durable_sync = mode >> 1;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "successor", 1, &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  REQUIRE(pipe(notify) == 0);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    char notify_fd[24], flags[8];
    REQUIRE(close(notify[0]) == 0);
    snprintf(notify_fd, sizeof(notify_fd), "%d", notify[1]);
    snprintf(flags, sizeof(flags), "%d", mode);
    execlp(program, program, "--successor-child", path, notify_fd, flags,
           (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(notify[1]) == 0);
  REQUIRE(read(notify[0], &event, 1) == 1 && event == 'P');
  ok(ps_catalog_get(ps_root_space(root), identity.owner, PS_CATALOG_SEGMENT,
                    leaf, strlen(leaf), &binding, &e),
     &e);
  REQUIRE(!binding);
  ps_key successor = {identity.owner, 1, PS_STREAM};
  ok(ps_owned_inspect(ps_root_space(root), successor, NULL, &staged, &e), &e);
  REQUIRE(!staged.tail && !staged.capacity);
  REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &exited, 0) == child &&
          WIFSIGNALED(exited) && WTERMSIG(exited) == SIGKILL);
  REQUIRE(close(notify[0]) == 0);
  ok(ps_catalog_get(ps_root_space(root), identity.owner, PS_CATALOG_SEGMENT,
                    leaf, strlen(leaf), &binding, &e),
     &e);
  REQUIRE(!binding);
  ok(append(root, ns, 1, 0, 64, &e), &e);
  ok(ps_flush(ps_root_space(root), &e), &e);
  ok(ps_catalog_get(ps_root_space(root), identity.owner, PS_CATALOG_SEGMENT,
                    leaf, strlen(leaf), &binding, &e),
     &e);
  ok(ps_catalog_view_info(binding, &repaired, &e), &e);
  REQUIRE(repaired.target.incarnation == staged.ref.incarnation &&
          !memcmp(repaired.target.container, staged.ref.container,
                  sizeof(staged.ref.container)));
  ps_catalog_view_close(binding);
  ok(ps_namespace_status_get(ns, &admitted, &e), &e);
  REQUIRE(admitted.active_segment == 1 && admitted.bound_segments == 1);
  count(ns, 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "successor", 0, &e);
  count(ns, 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  shared_namespace = 0;
  lc_error_cleanup(&e);
}
enum retire_fault { RETIRE_OK, RETIRE_BUSY, RETIRE_SECOND_WRITE, RETIRE_SYNC };
typedef struct retire_io {
  enum retire_fault fault;
  size_t syncs, removes;
} retire_io;
static ssize_t retirement_write(void *context, int fd, const void *bytes,
                                size_t count, off_t at) {
  static const unsigned char removal[] = {PS_STREAM, 0, 0, 0,
                                          PS_REMOVE, 0, 0, 0};
  retire_io *io = context;
  const unsigned char *wire = bytes;
  if (io->fault == RETIRE_SECOND_WRITE && count >= 64 && at >= JOURNAL_OFFSET &&
      at < PS_DATA && !memcmp(wire + 16, removal, sizeof(removal)) &&
      ++io->removes == 2) {
    errno = EIO;
    return -1;
  }
  return pwrite(fd, bytes, count, at);
}
static int retirement_sync(void *context, int fd) {
  retire_io *io = context;
  ++io->syncs;
  if (io->fault == RETIRE_SYNC) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
static void rolled_sources(const char *path, int reopen_durable,
                           enum retire_fault fault) {
  ps_root_options options = {0};
  retire_io probe = {0};
  ps_io io = {&probe,          successor_read, retirement_write,
              retirement_sync, direct_reserve, NULL};
  ps_root *root = NULL, *peer = NULL;
  ps_namespace *ns, *joined;
  ps_namespace_info identity;
  ps_namespace_status status;
  ps_namespace_window *window = NULL, *missing = NULL;
  ps_namespace_view *view = NULL;
  ps_namespace_retirement *retired[PS_NAMESPACE_RETIRE_LIMIT];
  lc_pouch_state_entry first = {0};
  ps_owned_status physical;
  shared_records observed = {0};
  ps_key key;
  uint64_t offset, closed, pending;
  unsigned int confirmed;
  size_t completed;
  unsigned char bytes[64];
  struct iovec part = {bytes, sizeof(bytes)};
  lc_error e = {0};
  shared_namespace = 1;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ns = acquire(root, "roll", 1, &e);
  ok(put(root, ns, 1, 0, sizeof(bytes), NULL, NULL, &first, &e), &e);
  offset = first.payload_span.payload_offset;
  lc_pouch_state_entry_cleanup(&allocator, &first);
  ok(append(root, ns, 2, 1, sizeof(bytes), &e), &e);
  ok(append(root, ns, 3, 1, sizeof(bytes), &e), &e);
  ok(ps_root_open(path, 0, &options, &peer, &e), &e);
  joined = acquire(peer, "roll", 0, &e);
  ok(append(peer, joined, 4, 0, sizeof(bytes), &e), &e);
  ok(ps_flush(ps_root_space(peer), &e), &e);
  ps_namespace_release(joined);
  ps_namespace_release(ns);
  ok(ps_root_close(peer, &e), &e);
  ok(ps_root_close(root, &e), &e);

  options.storage.durable_sync = reopen_durable;
  options.storage.io = &io;
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "roll", 0, &e);
  ok(ps_namespace_identity(ns, &identity, &e), &e);
  ok(ps_namespace_status_get(ns, &status, &e), &e);
  REQUIRE(status.active_segment == 3 && status.bound_segments == 3);
  ok(ps_namespace_view_open(ns, &view, &e), &e);
  ok(ps_namespace_view_visit(view, 1, visit_shared, &observed, &e), &e);
  REQUIRE(observed.keys == 4 && observed.version == 4);
  ps_namespace_view_close(view);
  key.owner = identity.owner;
  key.kind = PS_STREAM;
  for (key.key = 1; key.key < 3; ++key.key) {
    ok(ps_owned_inspect(ps_root_space(root), key, NULL, &physical, &e), &e);
    REQUIRE(physical.tail && !physical.sealed);
  }
  ok(ps_namespace_read_open(ns, PS_CATALOG_SEGMENT, 1, offset, sizeof(bytes),
                            &window, &e),
     &e);
  ok(ps_namespace_checkpoint_flush(ns, &e), &e);
  ok(ps_namespace_unlink_covered(ns, 0, retired, &completed, &e), &e);
  REQUIRE(completed == 2);
  ps_coordination_guard *edit = NULL;
  size_t busy = reopen_durable ? 0 : 1;
  if (fault == RETIRE_BUSY) {
    ok(ps_root_open(path, 0, &options, &peer, &e), &e);
    key.key = busy + 1;
    ok(ps_owned_inspect(ps_root_space(peer), key, NULL, &physical, &e), &e);
    ok(ps_coordination_lock(ps_space_coordination(ps_root_space(peer)),
                            PS_COORDINATION_STREAM, physical.ref.incarnation, 0,
                            &edit, &e),
       &e);
  }
  probe.syncs = 0;
  probe.fault = fault;
  int rc = ps_namespace_retire_unlinked_batch(ns, retired, completed,
                                              &confirmed, &e);
  if (fault == RETIRE_SECOND_WRITE || fault == RETIRE_SYNC) {
    REQUIRE(rc == LC_ERR_TRANSPORT && !confirmed &&
            probe.syncs == (fault == RETIRE_SYNC ? 1U : 0U));
    clear(&e);
    ps_namespace_read_close(window);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    probe.fault = RETIRE_OK;
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "roll", 0, &e);
    ok(ps_namespace_status_get(ns, &status, &e), &e);
    REQUIRE(status.active_segment == 3 && status.bound_segments == 1);
    memset(&observed, 0, sizeof(observed));
    ok(ps_namespace_view_open(ns, &view, &e), &e);
    ok(ps_namespace_view_visit(view, 1, visit_shared, &observed, &e), &e);
    REQUIRE(observed.keys == 1 && observed.version == 4);
    ps_namespace_view_close(view);
    ok(ps_root_reclaim_orphans(root, &closed, &pending, &e), &e);
    REQUIRE(!pending);
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(unlink(path) == 0);
    shared_namespace = 0;
    return;
  }
  ok(rc, &e);
  REQUIRE(probe.syncs == 1);
  if (fault == RETIRE_BUSY) {
    REQUIRE(confirmed == (3U ^ (1U << busy)));
    ps_coordination_unlock(edit);
    ok(ps_root_close(peer, &e), &e);
    ok(ps_namespace_reap(ns, &closed, &pending, &e), &e);
    REQUIRE(closed + pending == 2);
  } else
    REQUIRE(confirmed == 3U);
  key.key = 3;
  REQUIRE(ps_namespace_read_open(ns, PS_CATALOG_SEGMENT, 1, offset,
                                 sizeof(bytes), &missing,
                                 &e) == LC_ERR_INVALID &&
          !missing);
  clear(&e);
  ok(ps_checkpoint(ps_root_space(root), &e), &e);
  ok(lc_pouch_record_io_read(ps_namespace_read_input(window), offset, &part, 1,
                             &e),
     &e);
  REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
  ps_namespace_read_close(window);
  ok(ps_namespace_rotate(ns, 3, &e), &e);
  ok(ps_owned_inspect(ps_root_space(root), key, NULL, &physical, &e), &e);
  REQUIRE(physical.sealed);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "roll", 0, &e);
  ok(ps_namespace_status_get(ns, &status, &e), &e);
  REQUIRE(status.active_segment == 4 && status.bound_segments == 2);
  memset(&observed, 0, sizeof(observed));
  ok(ps_namespace_view_open(ns, &view, &e), &e);
  ok(ps_namespace_view_visit(view, 1, visit_shared, &observed, &e), &e);
  REQUIRE(observed.keys == 1 && observed.version == 4);
  ps_namespace_view_close(view);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(unlink(path) == 0);
  shared_namespace = 0;
  lc_error_cleanup(&e);
}
typedef struct run {
  const char *path, *key, *mode;
} run;
static void *worker(void *p) {
  run *r = p;
  /* These sweeps restore their modeled durable bytes or reopen on this host. */
  fixture_barriers_model(!strcmp(r->mode, "recode-faults") ||
                         !strcmp(r->mode, "body-select-faults") ||
                         !strcmp(r->mode, "compound-recode-faults") ||
                         !strcmp(r->mode, "conversion-faults") ||
                         !strcmp(r->mode, "append-budget-faults") ||
                         !strcmp(r->mode, "batch-faults"));
  if (!strcmp(r->mode, "lifecycle"))
    lifecycle(r->path, r->key);
  else if (!strcmp(r->mode, "faults"))
    faults(r->path);
  else if (!strcmp(r->mode, "validation"))
    validation(r->path);
  else if (!strcmp(r->mode, "reservation"))
    reservation(r->path);
  else if (!strcmp(r->mode, "publication"))
    publication(r->path);
  else if (!strcmp(r->mode, "bodies"))
    bodies(r->path);
  else if (!strcmp(r->mode, "recode") || !strcmp(r->mode, "recode-faults"))
    record_recode(r->path, r->key, !strcmp(r->mode, "recode-faults"));
  else if (!strcmp(r->mode, "recode-capacity"))
    recode_capacity(r->path, r->key);
  else if (!strcmp(r->mode, "body-select") ||
           !strcmp(r->mode, "body-select-faults"))
    body_select(r->path, r->key, !strcmp(r->mode, "body-select-faults"));
  else if (!strcmp(r->mode, "body-select-concurrency"))
    body_select_concurrency(r->path, r->key);
  else if (!strcmp(r->mode, "compound-recode") ||
           !strcmp(r->mode, "compound-recode-faults"))
    compound_recode(r->path, r->key,
                    !strcmp(r->mode, "compound-recode-faults"));
  else if (!strcmp(r->mode, "compound-recode-concurrency"))
    compound_concurrency(r->path, r->key);
  else if (!strcmp(r->mode, "conversion"))
    sweep_pressure(r->path, r->key);
  else if (!strcmp(r->mode, "conversion-faults"))
    sweep_faults(r->path, r->key);
  else if (!strcmp(r->mode, "conversion-guards"))
    sweep_guards(r->path, r->key);
  else if (!strcmp(r->mode, "body-budget"))
    body_budget_capacity(r->path);
  else if (!strcmp(r->mode, "body-budget-faults"))
    body_budget_faults(r->path);
  else if (!strcmp(r->mode, "append-budget")) {
    append_budget_limits(r->path);
    append_budget_capacity(r->path);
  } else if (!strcmp(r->mode, "append-budget-faults"))
    append_budget_faults(r->path, 0);
  else if (!strcmp(r->mode, "batch-admission"))
    batch_admission(r->path);
  else if (!strcmp(r->mode, "batch-faults"))
    append_budget_faults(r->path, 1);
  else if (!strcmp(r->mode, "budget-transfer"))
    budget_transfer(r->path);
  else if (!strcmp(r->mode, "shared-handoff")) {
    shared_handoff(r->path);
    shared_projection_capture(r->path, 0);
    shared_projection_capture(r->path, 1);
    shared_startup_checkpoint(r->path, 0);
    shared_startup_checkpoint(r->path, 1);
    for (int durable = 0; durable <= 1; ++durable)
      for (int fault = JOIN_NO_FAULT; fault <= JOIN_CHECKPOINT_RACE; ++fault)
        shared_join_admission(r->path, durable, fault);
  } else if (!strcmp(r->mode, "shared-scope-handoff")) {
    for (int durable = 0; durable <= 1; ++durable) {
      for (int edit_first = 0; edit_first <= 1; ++edit_first)
        shared_scope_handoff(r->path, durable, edit_first, 0);
      shared_scope_handoff(r->path, durable, 0, 1);
    }
  } else if (!strcmp(r->mode, "shared-source-pins"))
    shared_source_pins(r->path);
  else if (!strcmp(r->mode, "shared-source-faults"))
    shared_source_faults(r->path);
  else if (!strcmp(r->mode, "shared-source-exit"))
    shared_source_exit(r->path);
  else if (!strcmp(r->mode, "shared-abandoned"))
    shared_abandoned(r->path);
  else if (!strcmp(r->mode, "shared-successor")) {
    for (int mode = 0; mode < 4; ++mode)
      shared_successor(r->path, mode);
  } else if (!strcmp(r->mode, "rolled-sources")) {
    for (int durable = 0; durable <= 1; ++durable) {
      rolled_sources(r->path, durable, RETIRE_OK);
      rolled_sources(r->path, durable, RETIRE_BUSY);
      rolled_sources(r->path, durable, RETIRE_SECOND_WRITE);
      rolled_sources(r->path, durable, RETIRE_SYNC);
    }
  } else
    concurrency(r->path);
  fixture_barriers_model(0);
  return NULL;
}
int main(int argc, char **argv) {
  if (argc == 5 && !strcmp(argv[1], "--successor-child")) {
    successor_child(argv[2], atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  if (argc == 4 && !strcmp(argv[1], "--shared-child")) {
    namespace_abandoned_child(argv[2], atoi(argv[3]));
    return 0;
  }
  if (argc == 4 && !strcmp(argv[1], "--view-child")) {
    namespace_view_child(argv[2], atoi(argv[3]));
    return 0;
  }
  REQUIRE(argc == 3);
  program = argv[0];
  char prefix[1024], pattern[1100], root[1024], path[1100];
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/namespace-", argv[1]) > 0);
  REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) > 0);
  REQUIRE(lc_test_tmp_mkdtemp(pattern, root, sizeof(root), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/container", root) > 0);
  for (size_t i = 0; i < sizeof(body); ++i)
    body[i] = (unsigned char)(i * 37 + i / 19);
  char *key = NULL;
  lc_error e;
  lc_error_init(&e);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  run r = {path, key, argv[2]};
  pthread_t thread;
  REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(root, prefix);
  printf("pouch-space namespaces: %lu observable checks passed\n",
         (unsigned long)checks);
  return 0;
}
