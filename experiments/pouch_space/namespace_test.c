#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "budget.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "namespace.h"
#include "namespace_internal.h"
#include "record_io.h"
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
static int put(ps_root *root, ps_namespace *ns, size_t version, uint64_t target,
               size_t bytes, gate *g, uint64_t *segment,
               lc_pouch_state_entry *result, lc_error *e) {
  lc_pouch_state_entry request = {0};
  ps_namespace_append *append = NULL;
  input source = {0};
  source.pub.impl = &source;
  source.pub.read = pull;
  source.bytes = bytes;
  source.gate = g;
  request.key = "key";
  request.content_type = "application/test";
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
      for (int phase = 0; phase < 5; ++phase) {
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
            }
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
          } else
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
          } else if (phase == 4) {
            REQUIRE(recovered.count >= 1 && recovered.count <= 2 &&
                    (at || !durable || recovered.count == 2));
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
  REQUIRE(s->kind == PS_CATALOG_BODY && s->sealed && s->tail == 3 &&
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
static void shared_handoff(const char *path) {
  for (int durable = 0; durable <= 1; ++durable) {
    lc_error e = {0};
    ps_root_options options = {0};
    ps_root *a = NULL, *b = NULL;
    ps_namespace *x, *y, *other;
    ps_namespace_view *old = NULL, *view = NULL;
    ps_namespace_window *window = NULL;
    ps_namespace_append *held = NULL;
    ps_namespace_status status;
    lc_pouch_state_entry written = {0};
    uint64_t segment = 0;
    unsigned char bytes[64];
    struct iovec part = {bytes, sizeof(bytes)};
    records original = {0}, fresh = {0};
    shared_records checkpoint = {0};
    shared_namespace = 1;
    options.storage.durable_sync = durable;
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
    ok(ps_root_open(path, 0, &options, &b, &e), &e);
    y = acquire(b, "shared", 0, &e);
    ok(ps_namespace_append_begin(y, 0, 0, PS_APPEND_PREFIX, NULL, &held, &e),
       &e);
    REQUIRE(append(a, x, 2, 0, 65, &e) == LC_ERR_PENDING);
    clear(&e);
    REQUIRE(ps_namespace_view_open(x, &view, &e) == LC_ERR_PENDING && !view);
    clear(&e);
    ok(append(a, other, 1, 0, 65, &e), &e);
    ps_namespace_append_close(held);
    ok(append(b, y, 2, 0, 65, &e), &e);
    fail_namespace_load = 1;
    REQUIRE(ps_namespace_view_open(x, &view, &e) == LC_ERR_NOMEM && !view);
    REQUIRE(fail_namespace_load == 2);
    fail_namespace_load = 0;
    clear(&e);
    ok(ps_namespace_view_visit(old, 1, visit, &original, &e), &e);
    REQUIRE(original.count == 1 && original.version == 1);
    ok(capture(x, &fresh, &e), &e);
    REQUIRE(fresh.count == 2 && fresh.version == 2 && fresh.last_segment == 1);
    ok(append(b, y, 3, 1, 65, &e), &e);
    ok(ps_namespace_checkpoint_flush(y, &e), &e);
    ok(ps_checkpoint(ps_root_space(b), &e), &e);
    ok(ps_namespace_status_get(x, &status, &e), &e);
    REQUIRE(status.active_segment == 2 && status.checkpoint_keys == 1 &&
            status.covered_segment == 2);
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
typedef struct run {
  const char *path, *key, *mode;
} run;
static void *worker(void *p) {
  run *r = p;
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
  else if (!strcmp(r->mode, "shared-handoff"))
    shared_handoff(r->path);
  else if (!strcmp(r->mode, "shared-abandoned"))
    shared_abandoned(r->path);
  else
    concurrency(r->path);
  return NULL;
}
int main(int argc, char **argv) {
  if (argc == 4 && !strcmp(argv[1], "--shared-child")) {
    namespace_abandoned_child(argv[2], atoi(argv[3]));
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
