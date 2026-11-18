#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "namespace.h"
#include "owned.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocation, fail_allocation;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return fail_allocation && ++allocation == fail_allocation
             ? NULL
             : __real_lc_alloc_with_allocator(a, n);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "frame %d: %s\n", rc, e->message ? e->message : "?");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
typedef struct memory_input {
  unsigned char *bytes;
  size_t size, calls, fail, largest;
} memory_input;
static int read_memory(void *context, uint64_t at, const struct iovec *v,
                       int count, lc_error *e) {
  memory_input *m = context;
  if (++m->calls == m->fail)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected frame read failure",
                        NULL, NULL, "test");
  for (int i = 0; i < count; ++i) {
    REQUIRE(at <= m->size && v[i].iov_len <= m->size - at);
    if (v[i].iov_len > m->largest)
      m->largest = v[i].iov_len;
    memcpy(v[i].iov_base, m->bytes + at, v[i].iov_len);
    at += v[i].iov_len;
  }
  return LC_OK;
}
static unsigned char byte_at(size_t at) {
  return (unsigned char)((at * 97 + at / 251 + 19) & 255);
}
static void codec(void) {
  static const lc_pouch_record_io_ops ops = {read_memory, NULL, NULL, NULL,
                                             NULL};
  lc_error e;
  lc_error_init(&e);
  for (size_t size = 97; size <= 65569; size = size == 97 ? 65569 : 65570) {
    memory_input m = {0};
    const size_t start = 37, end = start + PS_FRAME_BYTES + size;
    m.bytes = malloc(end);
    m.size = end;
    REQUIRE(m.bytes);
    memset(m.bytes, 0, start);
    for (size_t i = 0; i < size; ++i)
      m.bytes[start + PS_FRAME_BYTES + i] = byte_at(i);
    lc_pouch_record_header h = {0};
    h.type = PS_FRAME_TYPE;
    h.payload_len = size;
    h.payload_crc = crc32(0L, m.bytes + start + PS_FRAME_BYTES, (uInt)size);
    lc_pouch_record_header_encode(&h, m.bytes + start);
    lc_pouch_record_io io = {&ops, &m};
    unsigned char *scratch = NULL;
    ps_frame f;
    ok(ps_frame_read(io, start, end, &scratch, &f, &e), &e);
    REQUIRE(f.framed && f.start == start + PS_FRAME_BYTES && f.end == end);
    REQUIRE(m.largest <= PS_CHUNK);
    size_t calls = m.calls;
    lc_free_with_allocator(NULL, scratch);
    for (int no_error = 0; no_error < 2; ++no_error) {
      lc_error *error = no_error ? NULL : &e;
      for (size_t cut = start; cut < end; ++cut) {
        if (size > 97 && cut > start + 2 * PS_FRAME_BYTES &&
            cut + PS_CHUNK < end && cut % PS_CHUNK)
          continue;
        scratch = NULL;
        REQUIRE(ps_frame_read(io, start, cut, &scratch, &f, error) ==
                LC_ERR_PROTOCOL);
        REQUIRE(!f.framed);
        lc_free_with_allocator(NULL, scratch);
        clear(&e);
      }
      for (int field = 0; field < 6; ++field) {
        lc_pouch_record_header bad = h;
        if (field == 0)
          bad.flags = LC_POUCH_RECORD_FLAG_PENDING;
        if (field == 1)
          bad.key_len = 1;
        if (field == 2)
          bad.meta_len = 1;
        if (field == 3)
          bad.payload_len = 0;
        if (field == 4)
          bad.payload_len = UINT64_MAX;
        if (field == 5)
          bad.payload_crc ^= 1;
        lc_pouch_record_header_encode(&bad, m.bytes + start);
        scratch = NULL;
        REQUIRE(ps_frame_read(io, start, end, &scratch, &f, error) ==
                LC_ERR_PROTOCOL);
        REQUIRE(!f.framed);
        lc_free_with_allocator(NULL, scratch);
        clear(&e);
      }
      lc_pouch_record_header_encode(&h, m.bytes + start);
      for (size_t at = start; at < end; ++at) {
        if (size > 97 && at > start + PS_FRAME_BYTES && at + 1 < end &&
            at % PS_CHUNK)
          continue;
        m.bytes[at] ^= 1;
        scratch = NULL;
        REQUIRE(ps_frame_read(io, start, end, &scratch, &f, error) ==
                LC_ERR_PROTOCOL);
        REQUIRE(!f.framed);
        lc_free_with_allocator(NULL, scratch);
        m.bytes[at] ^= 1;
        clear(&e);
      }
      for (size_t nth = 1; nth <= calls; ++nth) {
        m.calls = 0;
        m.fail = nth;
        scratch = NULL;
        REQUIRE(ps_frame_read(io, start, end, &scratch, &f, error) ==
                LC_ERR_TRANSPORT);
        REQUIRE(!f.framed);
        m.fail = 0;
        lc_free_with_allocator(NULL, scratch);
        clear(&e);
      }
      scratch = NULL;
      allocation = 0;
      fail_allocation = 1;
      int rc = ps_frame_read(io, start, end, &scratch, &f, error);
      fail_allocation = 0;
      REQUIRE(rc == LC_ERR_NOMEM && !scratch && !f.framed);
      clear(&e);
    }
    h.type = 1;
    h.payload_len = 0;
    h.payload_crc = 0;
    lc_pouch_record_header_encode(&h, m.bytes + start);
    scratch = NULL;
    ok(ps_frame_read(io, start, end, &scratch, &f, &e), &e);
    REQUIRE(!f.framed && f.start == start && f.end == end && !scratch);
    free(m.bytes);
  }
  lc_error_cleanup(&e);
}
typedef struct input {
  lc_source pub;
  size_t size, at;
} input;
static size_t produce(lc_source *pub, void *bytes, size_t n, lc_error *e) {
  input *s = pub->impl;
  (void)e;
  if (n > 4096)
    n = 4096;
  if (n > s->size - s->at)
    n = s->size - s->at;
  for (size_t i = 0; i < n; ++i)
    ((unsigned char *)bytes)[i] = byte_at(s->at + i);
  s->at += n;
  return n;
}
typedef struct fault {
  int armed;
} fault;
static ssize_t disk_read(void *context, int fd, void *p, size_t n, off_t at) {
  fault *f = context;
  if (f->armed) {
    f->armed = 0;
    errno = EIO;
    return -1;
  }
  return pread(fd, p, n, at);
}
static ssize_t disk_write(void *c, int fd, const void *p, size_t n, off_t at) {
  (void)c;
  return pwrite(fd, p, n, at);
}
static int disk_sync(void *c, int fd) {
  (void)c;
  return fdatasync(fd);
}
static int disk_reserve(void *c, int fd, off_t at, off_t n) {
  (void)c;
  return fallocate(fd, 0, at, n);
}
static int append_body(ps_root *root, ps_namespace_append *append, size_t size,
                       uint64_t version, lc_error *e) {
  lc_allocator a = {0};
  lc_pouch_state_entry q = {0}, r = {0};
  input s = {0};
  s.pub.read = produce;
  s.pub.impl = &s;
  s.size = size;
  q.key = "key";
  q.content_type = "application/test";
  q.payload_context = "frame-test";
  q.record_type = 1;
  q.version = q.index_seq = version;
  q.lifetime = 1;
  int rc = lc_pouch_state_record_append(
      &a, ps_root_crypto(root), ps_namespace_append_output(append),
      ps_namespace_append_leaf(append), &q, &s.pub, 0, &r, e);
  if (rc == LC_OK)
    REQUIRE(s.at == size);
  lc_pouch_state_entry_cleanup(&a, &r);
  return rc;
}
typedef struct observer {
  ps_namespace *ns;
  size_t size, count;
  uint64_t second;
} observer;
static int inspect(void *context, uint64_t segment,
                   const lc_pouch_state_entry *r, lc_error *e) {
  observer *v = context;
  ps_namespace_window *window = NULL;
  unsigned char bytes[2048];
  REQUIRE(r->found && r->bytes == v->size && r->version == v->count + 1);
  REQUIRE(r->record_offset ==
          (v->count ? v->second + (v->count - 1) * (v->second - PS_FRAME_BYTES)
                    : 0));
  ++v->count;
  int rc = ps_namespace_read_open(v->ns, PS_CATALOG_SEGMENT, segment,
                                  r->payload_span.payload_offset,
                                  r->payload_span.payload_length, &window, e);
  for (size_t at = 0; rc == LC_OK && at < v->size;) {
    size_t n = v->size - at < sizeof(bytes) ? v->size - at : sizeof(bytes);
    struct iovec p = {bytes, n};
    rc = lc_pouch_record_io_read(ps_namespace_read_input(window),
                                 r->payload_span.payload_offset + at, &p, 1, e);
    if (rc == LC_OK)
      for (size_t i = 0; i < n; ++i)
        REQUIRE(bytes[i] == byte_at(at + i));
    at += n;
  }
  ps_namespace_read_close(window);
  return rc;
}
static void writer(const char *path) {
  static const size_t sizes[] = {0, 31, 65569, 131077};
  lc_error e;
  lc_error_init(&e);
  for (size_t which = 0; which < sizeof(sizes) / sizeof(sizes[0]); ++which)
    for (int failure = 0; failure < (sizes[which] > PS_CHUNK ? 3 : 1);
         ++failure) {
      fault f = {0};
      ps_io io = {&f, disk_read, disk_write, disk_sync, disk_reserve, NULL};
      ps_root_options o = {0};
      o.storage.io = &io;
      ps_root *root = NULL;
      ps_namespace *ns = NULL;
      ps_namespace_append *append = NULL;
      ps_namespace_status status;
      ok(ps_root_open(path, 1, &o, &root, &e), &e);
      ok(ps_root_namespace_acquire(root, "frame", 1, &ns, &e), &e);
      ok(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                                   &e),
         &e);
      ok(append_body(root, append, sizes[which], 1, &e), &e);
      ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
      ps_namespace_append_close(append);
      ok(ps_namespace_status_get(ns, &status, &e), &e);
      uint64_t n = status.tail;
      ok(ps_namespace_append_begin(ns, 0, n, PS_APPEND_COMPLETE, NULL, &append,
                                   &e),
         &e);
      ok(append_body(root, append, sizes[which], 2, &e), &e);
      if (failure == 1) {
        allocation = 0;
        fail_allocation = 1;
      }
      if (failure == 2)
        f.armed = 1;
      int rc = ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e);
      fail_allocation = 0;
      f.armed = 0;
      REQUIRE(rc == (failure == 1   ? LC_ERR_NOMEM
                     : failure == 2 ? LC_ERR_TRANSPORT
                                    : LC_OK));
      clear(&e);
      ps_namespace_append_close(append);
      if (failure == 2) {
        /* An uncertain physical read retains the owner's poison contract. */
        ps_namespace_release(ns);
        int closing = ps_root_close(root, &e);
        REQUIRE(closing == LC_OK || closing == LC_ERR_TRANSPORT);
        clear(&e);
        ok(ps_root_open(path, 0, &o, &root, &e), &e);
        ok(ps_root_namespace_acquire(root, "frame", 0, &ns, &e), &e);
        ps_namespace_view *view = NULL;
        observer v = {ns, sizes[which], 0, n + PS_FRAME_BYTES};
        ok(ps_namespace_view_open(ns, &view, &e), &e);
        ok(ps_namespace_view_visit(view, 1, inspect, &v, &e), &e);
        REQUIRE(v.count == 1);
        ps_namespace_view_close(view);
      } else
        ok(ps_check(ps_root_space(root), &e), &e);
      if (failure) {
        ok(ps_namespace_append_begin(ns, 0, n, PS_APPEND_COMPLETE, NULL,
                                     &append, &e),
           &e);
        ok(append_body(root, append, sizes[which], 2, &e), &e);
        ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
        ps_namespace_append_close(append);
      }
      for (int cold = 0; cold < 2; ++cold) {
        ps_namespace_view *view = NULL;
        observer v = {ns, sizes[which], 0, n + PS_FRAME_BYTES};
        ok(ps_namespace_view_open(ns, &view, &e), &e);
        ok(ps_namespace_view_visit(view, 1, inspect, &v, &e), &e);
        REQUIRE(v.count == 2);
        ps_namespace_view_close(view);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        if (!cold) {
          ok(ps_root_open(path, 0, &o, &root, &e), &e);
          ok(ps_root_namespace_acquire(root, "frame", 0, &ns, &e), &e);
        }
      }
      REQUIRE(unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
/* Alter bytes through the physical test boundary after releasing all canonical
 * owners. These fixtures deliberately violate producer rules to prove that
 * recovery validates a whole batch before accepting any of its records. */
static void recovery(const char *path) {
  lc_error e;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync)
    for (int damage = 0; damage < 7; ++damage) {
      ps_root_options o = {0};
      o.storage.durable_sync = sync;
      ps_root *root = NULL;
      ps_namespace *ns = NULL;
      ps_namespace_info identity;
      ps_namespace_status status;
      ps_namespace_append *append = NULL;
      ok(ps_root_open(path, 1, &o, &root, &e), &e);
      ok(ps_root_namespace_acquire(root, "frame", 1, &ns, &e), &e);
      ok(ps_namespace_identity(ns, &identity, &e), &e);
      ok(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append,
                                   &e),
         &e);
      ok(append_body(root, append, 0, 1, &e), &e);
      ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
      ps_namespace_append_close(append);
      ok(ps_namespace_status_get(ns, &status, &e), &e);
      uint64_t n = status.tail;
      ok(ps_checkpoint(ps_root_space(root), &e), &e);
      ps_namespace_view *pin = NULL;
      ok(ps_namespace_view_open(ns, &pin, &e), &e);
      ok(ps_namespace_append_begin(ns, 0, 2 * n, PS_APPEND_COMPLETE, NULL,
                                   &append, &e),
         &e);
      ok(append_body(root, append, 0, 2, &e), &e);
      ok(append_body(root, append, 0, 3, &e), &e);
      ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
      ps_namespace_append_close(append);
      observer v = {ns, 0, 0, n + PS_FRAME_BYTES};
      ok(ps_namespace_view_visit(pin, 1, inspect, &v, &e), &e);
      REQUIRE(v.count == 1);
      ps_namespace_view_close(pin);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);

      ps_space *space = NULL;
      ps_catalog_view *binding_view = NULL;
      ps_catalog_info binding;
      ps_owned_head *head = NULL;
      ps_owned_status owned;
      ps_owned_run *run = NULL;
      int found;
      const char *leaf = "seg-00000000000000000001.log";
      ok(ps_open(path, 0, &o.storage, &space, &e), &e);
      ok(ps_catalog_get(space, identity.owner, PS_CATALOG_SEGMENT, leaf,
                        strlen(leaf), &binding_view, &e),
         &e);
      REQUIRE(binding_view);
      ok(ps_catalog_view_info(binding_view, &binding, &e), &e);
      ps_catalog_view_close(binding_view);
      ok(ps_owned_open(space, NULL, binding.stream, 0, &binding.target, &head,
                       &owned, &e),
         &e);
      REQUIRE(owned.tail == n);
      ok(ps_owned_grant(head, NULL, 0, 0, &run, &found, &e), &e);
      REQUIRE(found &&
              n + PS_FRAME_BYTES + 2 * n <= ps_owned_run_capacity(run));
      size_t bytes = PS_FRAME_BYTES + (size_t)(2 * n);
      unsigned char *batch = malloc(bytes);
      REQUIRE(batch);
      ok(ps_owned_read(run, n, batch, bytes, &e), &e);
      lc_pouch_record_header h;
      ok(lc_pouch_record_header_decode(batch, &h, &e), &e);
      REQUIRE(h.type == PS_FRAME_TYPE && h.payload_len == 2 * n);
      /* 1/4: invalid CRC; 2/5: malformed final inner record with valid CRC.
       * 3: checkpoint inside an otherwise valid complete frame.
       * 4/5/6: the entire frame belongs to the trusted checkpoint prefix. */
      if (damage == 1 || damage == 4)
        batch[bytes - 1] ^= 1;
      if (damage == 2 || damage == 5) {
        batch[PS_FRAME_BYTES + n] ^= 1;
        h.payload_crc = crc32(0L, batch + PS_FRAME_BYTES, (uInt)(2 * n));
        lc_pouch_record_header_encode(&h, batch);
      }
      ok(ps_owned_write(run, n, batch, bytes, &e), &e);
      free(batch);
      if (damage >= 3) {
        ok(ps_owned_commit(head,
                           damage == 3 ? n + PS_FRAME_BYTES + n : n + bytes,
                           PS_COMMIT_DEFAULT, NULL, NULL, &e),
           &e);
        ok(ps_checkpoint(space, &e), &e);
      }
      ps_owned_run_close(run);
      ps_owned_close(head);
      ok(ps_close(space, &e), &e);

      int rc = ps_root_open(path, 0, &o, &root, &e);
      if (damage >= 3 && damage <= 5) {
        /* The root restores every namespace's checkpoint debt before opening
         * ordinary admission, so trusted-prefix damage fails at root open. */
        REQUIRE(rc == LC_ERR_PROTOCOL && !root);
        clear(&e);
        REQUIRE(unlink(path) == 0);
        continue;
      }
      ok(rc, &e);
      ok(ps_root_namespace_acquire(root, "frame", 0, &ns, &e), &e);
      ok(ps_namespace_status_get(ns, &status, &e), &e);
      REQUIRE(status.tail == (damage == 1 || damage == 2 ? n : n + bytes));
      ps_namespace_view *view = NULL;
      v.count = 0;
      v.ns = ns;
      ok(ps_namespace_view_open(ns, &view, &e), &e);
      ok(ps_namespace_view_visit(view, 1, inspect, &v, &e), &e);
      REQUIRE(v.count == (damage == 1 || damage == 2 ? 1 : 3));
      ps_namespace_view_close(view);
      if (damage == 1 || damage == 2) {
        ok(ps_namespace_append_begin(ns, 0, 2 * n, PS_APPEND_COMPLETE, NULL,
                                     &append, &e),
           &e);
        ok(append_body(root, append, 0, 2, &e), &e);
        ok(append_body(root, append, 0, 3, &e), &e);
        ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
        ps_namespace_append_close(append);
      }
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      if (damage == 1 || damage == 2) {
        ok(ps_root_open(path, 0, &o, &root, &e), &e);
        ok(ps_root_namespace_acquire(root, "frame", 0, &ns, &e), &e);
        ps_namespace_view *view = NULL;
        v.count = 0;
        v.ns = ns;
        ok(ps_namespace_view_open(ns, &view, &e), &e);
        ok(ps_namespace_view_visit(view, 1, inspect, &v, &e), &e);
        REQUIRE(v.count == 3);
        ps_namespace_view_close(view);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
      }
      REQUIRE(unlink(path) == 0);
    }
  lc_error_cleanup(&e);
}
typedef struct run {
  const char *mode, *path;
} run;
static void *worker(void *context) {
  run *r = context;
  if (!strcmp(r->mode, "codec"))
    codec();
  else if (!strcmp(r->mode, "recovery"))
    recovery(r->path);
  else {
    REQUIRE(!strcmp(r->mode, "writer"));
    writer(r->path);
  }
  return NULL;
}
int main(int argc, char **argv) {
  char prefix[1100], template_path[1200], directory[1200], path[1300];
  pthread_t thread;
  REQUIRE(argc == 3);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/frame-", argv[1]) <
          (int)sizeof(prefix));
  REQUIRE(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) <
          (int)sizeof(template_path));
  REQUIRE(
      lc_test_tmp_mkdtemp(template_path, directory, sizeof(directory), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  run r = {argv[2], path};
  REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  lc_test_tmp_cleanup_path(directory, prefix);
  printf("batch frames: %lu checks\n", (unsigned long)checks);
  return 0;
}
