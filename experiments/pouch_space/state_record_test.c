#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_state_record.h"
#include "record_io.h"
#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static size_t checks, allocation, fail_allocation;
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
static const char leaf[] = "seg-00000000000000000001.log";
static unsigned char *body;
static const size_t body_bytes = 1048601;

typedef struct cookie {
  unsigned char *bytes;
  size_t length, at, calls;
  int failure;
} cookie;
static ssize_t cookie_read(void *p, char *bytes, size_t n) {
  cookie *c = p;
  ++c->calls;
  if (c->failure == 2 || (c->failure == 1 && c->calls == 1)) {
    errno = c->failure == 1 ? EINTR : EIO;
    return -1;
  }
  if (n > 3)
    n = 3;
  if (n > c->length - c->at)
    n = c->length - c->at;
  memcpy(bytes, c->bytes + c->at, n);
  c->at += n;
  return (ssize_t)n;
}
static int cookie_seek(void *p, off_t *offset, int whence) {
  cookie *c = p;
  off_t at = *offset;
  if (whence == SEEK_CUR)
    at += (off_t)c->at;
  else if (whence == SEEK_END)
    at += (off_t)c->length;
  if (at < 0 || (uint64_t)at > c->length) {
    errno = EINVAL;
    return -1;
  }
  c->at = (size_t)at;
  *offset = at;
  return 0;
}

typedef struct observed {
  lc_pouch_record_io underlying;
  size_t calls, fail, written, discards;
  int after;
} observed;
static int io_fail(observed *o, lc_error *e) {
  return ++o->calls == o->fail && !o->after
             ? lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected record I/O", NULL,
                            NULL, "test")
             : LC_OK;
}
static int io_finish(observed *o, int rc, lc_error *e) {
  return rc == LC_OK && o->after && o->calls == o->fail
             ? lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected completed I/O",
                            NULL, NULL, "test")
             : rc;
}
static int rd(void *p, uint64_t at, const struct iovec *v, int n, lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return io_finish(
      o, rc == LC_OK ? lc_pouch_record_io_read(o->underlying, at, v, n, e) : rc,
      e);
}
static int wr(void *p, const struct iovec *v, int n, lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  if (rc == LC_OK) {
    rc = lc_pouch_record_io_append(o->underlying, v, n, e);
    if (rc == LC_OK)
      for (int i = 0; i < n; ++i)
        o->written += v[i].iov_len;
  }
  return io_finish(o, rc, e);
}
static int patch(void *p, uint64_t at, const struct iovec *v, int n,
                 lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return io_finish(
      o,
      rc == LC_OK ? lc_pouch_record_io_patch(o->underlying, at, v, n, e) : rc,
      e);
}
static int tail(void *p, uint64_t *at, lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return io_finish(
      o, rc == LC_OK ? lc_pouch_record_io_tail(o->underlying, at, e) : rc, e);
}
static void discard(void *p, uint64_t at) {
  observed *o = p;
  ++o->discards;
  o->underlying.ops->discard(o->underlying.context, at);
}
static lc_pouch_record_io observing(observed *o,
                                    lc_pouch_record_io underlying) {
  static const lc_pouch_record_io_ops ops = {rd, wr, patch, tail, discard};
  lc_pouch_record_io io = {&ops, o};
  memset(o, 0, sizeof(*o));
  o->underlying = underlying;
  return io;
}
typedef struct input {
  lc_source pub;
  size_t at, bytes, fail_at;
  int oversized;
  observed *output;
} input;
static size_t pull(lc_source *pub, void *p, size_t n, lc_error *e) {
  input *s = pub->impl;
  if (s->oversized)
    return SIZE_MAX;
  if (s->fail_at && s->at >= s->fail_at) {
    lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected producer failure", NULL,
                 NULL, "test");
    return 0;
  }
  if (s->at == s->bytes && s->bytes > 1000000)
    REQUIRE(s->output->written > 65536); /* encoded body flowed before EOF */
  if (n > 1301)
    n = 1301;
  if (n > s->bytes - s->at)
    n = s->bytes - s->at;
  memcpy(p, body + s->at, n);
  s->at += n;
  return n;
}
static input source(size_t bytes, observed *o) {
  input s = {0};
  s.pub.read = pull;
  s.bytes = bytes;
  s.output = o;
  return s;
}
static lc_pouch_state_entry request(void) {
  lc_pouch_state_entry r = {0};
  r.key = ".staging/txn/a/key";
  r.content_type = "application/test";
  r.payload_context = "state-record-context";
  r.record_type = 1;
  r.version = 17;
  r.index_seq = 31;
  r.updated_at_unix = 1770000000;
  r.retention = -3600;
  r.has_query_hidden = 1;
  r.query_hidden = 1;
  r.metadata = (unsigned char *)"\000\377side";
  r.metadata_length = 6;
  /* The byte writer must derive these, including on failure paths. */
  r.bytes = r.cipher_bytes = LC_U64_MAX;
  r.etag = "ignored";
  r.descriptor = "ignored";
  r.payload_span.present = 1;
  r.payload_span.container_leaf = "ignored";
  r.payload_span.payload_offset = LC_U64_MAX;
  r.record_container_leaf = "ignored";
  r.control = r.has_record_ref = 1;
  return r;
}
static void metadata(const lc_pouch_state_entry *r,
                     const lc_pouch_state_entry *q, size_t bytes) {
  REQUIRE(r->found && r->seen && r->has_record_ref && r->payload_span.present);
  REQUIRE(!r->control && r->record_offset == r->payload_span.record_offset);
  REQUIRE(!strcmp(r->key, q->key) && !strcmp(r->content_type, q->content_type));
  REQUIRE(r->version == q->version && r->index_seq == q->index_seq);
  REQUIRE(r->record_type == q->record_type && r->retention == q->retention);
  REQUIRE(r->updated_at_unix == q->updated_at_unix && r->bytes == bytes);
  REQUIRE(r->has_query_hidden && r->query_hidden);
  REQUIRE(r->metadata_length == q->metadata_length &&
          !memcmp(r->metadata, q->metadata, q->metadata_length));
  unsigned char digest[32];
  unsigned int length;
  char hex[65];
  REQUIRE(EVP_Digest(body, bytes, digest, &length, EVP_sha256(), NULL) == 1 &&
          length == 32);
  for (size_t i = 0; i < 32; ++i)
    snprintf(hex + 2 * i, 3, "%02x", digest[i]);
  REQUIRE(!strcmp(r->etag, hex));
}
typedef struct encoded {
  lc_source pub;
  lc_pouch_record_io io;
  uint64_t start, bytes, at;
} encoded;
static size_t encoded_read(lc_source *pub, void *p, size_t n, lc_error *e) {
  encoded *s = pub->impl;
  struct iovec part;
  if (n > s->bytes - s->at)
    n = (size_t)(s->bytes - s->at);
  part.iov_base = p;
  part.iov_len = n;
  if (lc_pouch_record_io_read(s->io, s->start + s->at, &part, 1, e) != LC_OK)
    return 0;
  s->at += n;
  return n;
}
static int encoded_reset(lc_source *pub, lc_error *e) {
  (void)e;
  ((encoded *)pub->impl)->at = 0;
  return LC_OK;
}
static void plaintext(lc_pouch_record_io io, lc_pouch_crypto *crypto,
                      lc_pouch_state_entry *r, size_t bytes, lc_error *e) {
  encoded s = {0};
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *plain = NULL;
  unsigned char buffer[2039];
  size_t at = 0, n;
  s.pub.impl = &s;
  s.pub.read = encoded_read;
  s.pub.reset = encoded_reset;
  s.io = io;
  s.start = r->payload_span.payload_offset;
  s.bytes = r->cipher_bytes;
  ok(lc_pouch_crypto_reader_open(crypto, &reader, e), e);
  ok(lc_pouch_crypto_reader_bind_source(reader, r->payload_context,
                                        r->payload_context, &s.pub, s.bytes,
                                        r->descriptor, &plain, e),
     e);
  do {
    n = plain->read(plain, buffer, sizeof(buffer), e);
    REQUIRE(e->code == LC_OK);
    REQUIRE(n <= bytes - at && !memcmp(body + at, buffer, n));
    at += n;
  } while (n);
  REQUIRE(at == bytes);
  lc_pouch_crypto_reader_close(reader);
}
static int recover(ps_stream *stream, uint64_t checkpoint, uint64_t capacity,
                   uint64_t *accepted, void *context, lc_error *e) {
  lc_pouch_state_entry r = {0};
  unsigned char *scratch = NULL;
  uint64_t at = 0, next;
  int found, truncated, rc = LC_OK;
  size_t *records = context;
  *records = 0;
  while (at < capacity) {
    rc = lc_pouch_state_record_read(
        ps_record_recovery_input(stream), &allocator, leaf, at, capacity, 1,
        at >= checkpoint, &scratch, &r, &next, &found, &truncated, e);
    if (rc == LC_ERR_PROTOCOL && at >= checkpoint) {
      clear(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK || !found)
      break;
    REQUIRE(at >= checkpoint || next <= checkpoint);
    at = next;
    ++*records;
  }
  *accepted = at;
  lc_pouch_state_entry_cleanup(&allocator, &r);
  lc_free_with_allocator(&allocator, scratch);
  return rc;
}
static void lifecycle(const char *path, const char *key) {
  lc_error e;
  lc_error_init(&e);
  for (int mode = 0; mode < 4; ++mode) {
    lc_pouch_crypto_open_options options = {0};
    lc_pouch_crypto *crypto = NULL;
    ps_space *space = NULL;
    ps_stream *stream = NULL;
    ps_stream_info info;
    ps_key k = {1, 1, PS_STREAM};
    size_t recovered = 0;
    options.compression_enabled = mode & 1;
    options.key_string = mode & 2 ? key : NULL;
    if (mode)
      ok(lc_pouch_crypto_open(&allocator, &options, &crypto, NULL, &e), &e);
    ok(ps_open(path, 1, NULL, &space, &e), &e);
    ok(ps_stream_open(space, NULL, k, 1, NULL, recover, &recovered, &stream,
                      &e),
       &e);
    size_t sizes[] = {0, 1, 1048601};
    uint64_t starts[6], ends[6];
    for (int disable = 0; disable < 2; ++disable)
      for (size_t n = 0; n < 3; ++n) {
        lc_pouch_state_entry q = request(), r = {0};
        ps_record_writer writer;
        observed observation;
        input s = source(sizes[n], &observation);
        s.pub.impl = &s;
        q.record_type = disable ? 7 : 1;
        q.retention = n == 0 ? 0 : n == 1 ? -3600 : 1770003600;
        q.staged_delete_marker = n == 0;
        ok(ps_record_begin(stream, &writer, &e), &e);
        lc_pouch_record_io io =
            observing(&observation, ps_record_output(&writer));
        starts[disable * 3 + n] = writer.start;
        ok(lc_pouch_state_record_append(&allocator, crypto, io, leaf, &q,
                                        &s.pub, disable, &r, &e),
           &e);
        metadata(&r, &q, sizes[n]);
        REQUIRE(s.at == sizes[n]);
        ends[disable * 3 + n] = writer.tail;
        ok(ps_record_commit(&writer, NULL, NULL, &e), &e);
        ps_record_abort(&writer);
        lc_pouch_state_entry_cleanup(&allocator, &r);
        if (disable == 0 && n == 2 && mode % 2)
          ok(ps_checkpoint(space, &e), &e);
      }
    ok(ps_stream_status(stream, &info, &e), &e);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(space, &e), &e);
    ok(ps_open(path, 0, NULL, &space, &e), &e);
    ok(ps_stream_open(space, NULL, k, 0, &info.ref, recover, &recovered,
                      &stream, &e),
       &e);
    REQUIRE(recovered == 6);
    ps_stream_reader *reader = NULL;
    ok(ps_stream_read_open(stream, 0, ends[5], &reader, &e), &e);
    lc_pouch_record_io io = ps_record_input(reader);
    unsigned char *scratch = NULL;
    for (int disable = 0; disable < 2; ++disable)
      for (size_t n = 0; n < 3; ++n) {
        lc_pouch_state_entry q = request(), r = {0};
        uint64_t next;
        int found, truncated;
        q.record_type = disable ? 7 : 1;
        q.retention = n == 0 ? 0 : n == 1 ? -3600 : 1770003600;
        ok(lc_pouch_state_record_read(
               io, &allocator, leaf, starts[disable * 3 + n], ends[5], 0, 1,
               &scratch, &r, &next, &found, &truncated, &e),
           &e);
        REQUIRE(found && !truncated && next == ends[disable * 3 + n]);
        metadata(&r, &q, sizes[n]);
        plaintext(io, crypto, &r, sizes[n], &e);
        lc_pouch_state_entry_cleanup(&allocator, &r);
      }
    lc_free_with_allocator(&allocator, scratch);
    ps_stream_reader_close(reader);
    ok(ps_stream_seal(stream, &e), &e);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(space, &e), &e);
    lc_pouch_crypto_close(crypto);
    REQUIRE(unlink(path) == 0);
  }
  lc_error_cleanup(&e);
}
static void faults(const char *path) {
  lc_error e;
  lc_error_init(&e);
  int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
  REQUIRE(fd >= 0);
  lc_pouch_record_io underlying = lc_pouch_record_file_io(fd), io;
  lc_pouch_state_entry q = request(), r = {0}, decoded = {0};
  observed o;
  input s;
  uint64_t end, next;
  int found, truncated;
  unsigned char *scratch = NULL;
  s = source(17, &o);
  s.pub.impl = &s;
  io = observing(&o, underlying);
  ok(lc_pouch_state_record_append(&allocator, NULL, io, leaf, &q, &s.pub, 0, &r,
                                  &e),
     &e);
  ok(lc_pouch_record_io_tail(underlying, &end, &e), &e);
  /* Every captured-end cut, strict and repairable, preserves the prior output.
   */
  for (uint64_t cut = 1; cut < end; ++cut) {
    ok(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, cut, 1, 1,
                                  &scratch, &decoded, &next, &found, &truncated,
                                  &e),
       &e);
    REQUIRE(!found && truncated && next == 0 && decoded.key == NULL);
    REQUIRE(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, cut, 0,
                                       1, &scratch, &decoded, &next, &found,
                                       &truncated, &e) == LC_ERR_PROTOCOL);
    clear(&e);
  }
  FILE *fp = fdopen(dup(fd), "rb");
  REQUIRE(fp != NULL);
  ok(lc_pouch_state_record_read(lc_pouch_record_stdio_io(fp), &allocator, leaf,
                                0, end, 0, 1, &scratch, &decoded, &next, &found,
                                &truncated, &e),
     &e);
  REQUIRE(found && next == end);
  metadata(&decoded, &q, 17);
  REQUIRE(fclose(fp) == 0);
  lc_pouch_state_entry_cleanup(&allocator, &decoded);
  cookie c = {0};
  cookie_io_functions_t ops = {cookie_read, NULL, cookie_seek, NULL};
  c.bytes = malloc((size_t)end);
  REQUIRE(c.bytes != NULL);
  c.length = (size_t)end;
  REQUIRE(pread(fd, c.bytes, c.length, 0) == (ssize_t)c.length);
  for (int failure = 0; failure < 3; ++failure) {
    c.at = c.calls = 0;
    c.failure = failure;
    fp = fopencookie(&c, "rb", ops);
    REQUIRE(fp != NULL);
    int rc = lc_pouch_state_record_read(
        lc_pouch_record_stdio_io(fp), &allocator, leaf, 0, end, 1, 1, &scratch,
        &decoded, &next, &found, &truncated, &e);
    if (failure < 2) {
      ok(rc, &e);
      REQUIRE(found && next == end);
      metadata(&decoded, &q, 17);
      lc_pouch_state_entry_cleanup(&allocator, &decoded);
    } else {
      REQUIRE(rc == LC_ERR_TRANSPORT && !found && !truncated &&
              decoded.key == NULL);
      clear(&e);
    }
    REQUIRE(fclose(fp) == 0);
  }
  free(c.bytes);
  /* Every read operation/allocation must fail atomically, including when a
   * caller reuses a live decoded entry. I/O failures are never tail repair. */
  ok(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, end, 0, 1,
                                &scratch, &decoded, &next, &found, &truncated,
                                &e),
     &e);
  for (int kind = 0; kind < 2; ++kind) {
    for (size_t nth = 1;; ++nth) {
      io = observing(&o, underlying);
      char *prior_key = decoded.key;
      if (kind) {
        lc_free_with_allocator(&allocator, scratch);
        scratch = NULL;
        allocation = 0;
        fail_allocation = nth;
      } else {
        o.fail = nth;
      }
      int rc = lc_pouch_state_record_read(io, &allocator, leaf, 0, end, 1, 1,
                                          &scratch, &decoded, &next, &found,
                                          &truncated, &e);
      size_t used = kind ? allocation : o.calls;
      fail_allocation = 0;
      if (rc == LC_OK) {
        REQUIRE(nth == used + 1 && found && !truncated && next == end);
        break;
      }
      REQUIRE(decoded.key == prior_key && !found && !truncated && next == 0);
      clear(&e);
      metadata(&decoded, &q, 17);
    }
  }
  lc_pouch_state_entry_cleanup(&allocator, &decoded);
  /* Pending records are repairable; unknown flags and wrong span identities
   * are corruption even when the caller permits an incomplete active tail. */
  unsigned char wire[32], saved[32];
  lc_pouch_record_header header;
  REQUIRE(pread(fd, saved, sizeof(saved), 0) == (ssize_t)sizeof(saved));
  ok(lc_pouch_record_header_decode(saved, &header, &e), &e);
  for (int flags = 1; flags <= 2; ++flags) {
    header.flags = (unsigned long)flags;
    lc_pouch_record_header_encode(&header, wire);
    REQUIRE(pwrite(fd, wire, sizeof(wire), 0) == (ssize_t)sizeof(wire));
    int rc = lc_pouch_state_record_read(underlying, &allocator, leaf, 0, end, 1,
                                        0, NULL, &decoded, &next, &found,
                                        &truncated, &e);
    REQUIRE(flags == 1 ? rc == LC_OK && truncated : rc == LC_ERR_PROTOCOL);
    REQUIRE(!found && decoded.key == NULL);
    clear(&e);
    REQUIRE(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, end, 0,
                                       0, NULL, &decoded, &next, &found,
                                       &truncated, &e) == LC_ERR_PROTOCOL);
    clear(&e);
  }
  REQUIRE(pwrite(fd, saved, sizeof(saved), 0) == (ssize_t)sizeof(saved));
  REQUIRE(lc_pouch_state_record_read(underlying, &allocator,
                                     "seg-00000000000000000002.log", 0, end, 1,
                                     0, NULL, &decoded, &next, &found,
                                     &truncated, &e) == LC_ERR_PROTOCOL);
  REQUIRE(!found && decoded.key == NULL);
  clear(&e);
  /* Fault every byte-access operation and allocation in a real append. */
  for (int kind = 0; kind < 2; ++kind) {
    size_t calls = 0;
    for (size_t nth = 1;; ++nth) {
      s = source(131077, &o);
      s.pub.impl = &s;
      io = observing(&o, underlying);
      REQUIRE(lseek(fd, (off_t)end, SEEK_SET) == (off_t)end);
      if (kind) {
        allocation = 0;
        fail_allocation = nth;
      } else
        o.fail = nth;
      int rc = lc_pouch_state_record_append(&allocator, NULL, io, leaf, &q,
                                            &s.pub, 0, &decoded, &e);
      size_t used = kind ? allocation : o.calls;
      fail_allocation = 0;
      if (rc == LC_OK) {
        calls = used;
        underlying.ops->discard(underlying.context, end);
        lc_pouch_state_entry_cleanup(&allocator, &decoded);
        REQUIRE(nth == calls + 1);
        break;
      }
      REQUIRE(decoded.key == NULL);
      clear(&e);
      uint64_t after;
      ok(lc_pouch_record_io_tail(underlying, &after, &e), &e);
      REQUIRE(after == end);
      ok(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, end, 0, 1,
                                    &scratch, &decoded, &next, &found,
                                    &truncated, &e),
         &e);
      REQUIRE(found);
      metadata(&decoded, &q, 17);
      lc_pouch_state_entry_cleanup(&allocator, &decoded);
    }
    REQUIRE(calls > 5);
  }
  for (int failure = 0; failure < 2; ++failure) {
    s = source(131077, &o);
    s.pub.impl = &s;
    io = observing(&o, underlying);
    if (failure)
      s.oversized = 1;
    else
      s.fail_at = 70000;
    REQUIRE(lc_pouch_state_record_append(&allocator, NULL, io, leaf, &q, &s.pub,
                                         0, &decoded, &e) != LC_OK);
    clear(&e);
    uint64_t after;
    ok(lc_pouch_record_io_tail(underlying, &after, &e), &e);
    REQUIRE(after == end && decoded.key == NULL);
  }
  /* Stored body corruption fails verification even with a repairable tail. */
  unsigned char byte;
  REQUIRE(pread(fd, &byte, 1, (off_t)r.payload_span.payload_offset) == 1);
  byte ^= 1;
  REQUIRE(pwrite(fd, &byte, 1, (off_t)r.payload_span.payload_offset) == 1);
  REQUIRE(lc_pouch_state_record_read(underlying, &allocator, leaf, 0, end, 1, 1,
                                     &scratch, &decoded, &next, &found,
                                     &truncated, &e) == LC_ERR_PROTOCOL);
  REQUIRE(!found && !truncated && decoded.key == NULL);
  clear(&e);
  lc_pouch_state_entry_cleanup(&allocator, &r);
  lc_free_with_allocator(&allocator, scratch);
  REQUIRE(close(fd) == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
}
static const char snapshot_leaf[] = "snapshot-00000000000000000002.log";
typedef struct snapshot_source {
  lc_source pub;
  encoded range;
  observed *output;
  size_t calls, largest;
  int failure;
} snapshot_source;
static size_t snapshot_pull(lc_source *pub, void *bytes, size_t count,
                            lc_error *e) {
  snapshot_source *s = pub->impl;
  ++s->calls;
  if (count > s->largest)
    s->largest = count;
  REQUIRE(count <= LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES);
  if (s->failure == 3)
    return SIZE_MAX;
  if (s->failure == 2 || s->failure == 5) {
    lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected encoded source error", NULL,
                 NULL, "test");
    return s->failure == 5 ? 1 : 0;
  }
  if (s->range.at > 65536 && s->range.bytes > 1000000)
    REQUIRE(s->output->written > 65536);
  size_t got = encoded_read(&s->range.pub, bytes, count, e);
  if (got && s->failure == 4)
    ((unsigned char *)bytes)[0] ^= 1;
  return got;
}
static void snapshot_source_init(snapshot_source *s, lc_pouch_record_io input,
                                 const lc_pouch_state_entry *r, observed *o) {
  memset(s, 0, sizeof(*s));
  s->pub.impl = s;
  s->pub.read = snapshot_pull;
  s->range.pub.impl = &s->range;
  s->range.io = input;
  s->range.start = r->payload_span.payload_offset;
  s->range.bytes = r->cipher_bytes;
  s->output = o;
}
static void snapshot_fields(const lc_pouch_state_entry *r,
                            const lc_pouch_state_entry *expected) {
  REQUIRE(!strcmp(r->key, expected->key));
  REQUIRE(r->version == expected->version &&
          r->index_seq == expected->index_seq);
  REQUIRE(r->has_query_index_seq &&
          r->query_index_seq == (expected->has_query_index_seq
                                     ? expected->query_index_seq
                                     : expected->index_seq));
  REQUIRE(r->found == expected->found);
  if (r->found) {
    REQUIRE(!strcmp(r->content_type, expected->content_type));
    REQUIRE(!strcmp(r->etag, expected->etag));
    REQUIRE(r->bytes == expected->bytes &&
            r->cipher_bytes == expected->cipher_bytes);
    REQUIRE(r->retention == expected->retention &&
            r->updated_at_unix == expected->updated_at_unix);
    REQUIRE(r->metadata_length == expected->metadata_length &&
            !memcmp(r->metadata, expected->metadata, r->metadata_length));
    REQUIRE(r->has_query_hidden == expected->has_query_hidden &&
            r->query_hidden == expected->query_hidden &&
            r->staged_delete_marker == expected->staged_delete_marker);
    REQUIRE((!r->descriptor && !expected->descriptor) ||
            (r->descriptor && expected->descriptor &&
             !strcmp(r->descriptor, expected->descriptor)));
  }
}
static void snapshot_records(const char *path, const char *key) {
  lc_error e;
  lc_error_init(&e);
  unsigned char *buffer = lc_alloc_with_allocator(
      &allocator, 2 * LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES);
  REQUIRE(buffer != NULL);
  for (int mode = 0; mode < 4; ++mode)
    for (int durable = 0; durable < 2; ++durable)
      for (int file = 0; file < 2; ++file)
        for (int object = 0; object < 2; ++object)
          for (size_t size = 0; size < 3; ++size) {
            ps_options options = {0};
            options.durable_sync = durable;
            lc_pouch_crypto_open_options crypto_options = {0};
            crypto_options.key_string = mode & 1 ? key : NULL;
            crypto_options.compression_enabled = mode & 2;
            lc_pouch_crypto *crypto = NULL;
            if (mode)
              ok(lc_pouch_crypto_open(&allocator, &crypto_options, &crypto,
                                      NULL, &e),
                 &e);
            ps_space *space = NULL;
            ps_stream *original = NULL, *destination = NULL;
            ps_key a = {1, 1, PS_STREAM}, b = {2, 1, PS_STREAM};
            ok(ps_open(path, 1, &options, &space, &e), &e);
            ok(ps_stream_open(space, NULL, a, 1, NULL, NULL, NULL, &original,
                              &e),
               &e);
            size_t bytes = size == 0 ? 0 : size == 1 ? 1 : body_bytes;
            lc_pouch_state_entry q = request(), stored = {0};
            q.record_type = object ? 7 : 1;
            q.retention = size == 0 ? 0 : size == 1 ? -3600 : 1770003600;
            q.staged_delete_marker = size == 0;
            ps_record_writer first;
            observed origin;
            input s = source(bytes, &origin);
            s.pub.impl = &s;
            ok(ps_record_begin(original, &first, &e), &e);
            lc_pouch_record_io original_output =
                observing(&origin, ps_record_output(&first));
            ok(lc_pouch_state_record_append(&allocator, crypto, original_output,
                                            leaf, &q, &s.pub, 0, &stored, &e),
               &e);
            ok(ps_record_commit(&first, NULL, NULL, &e), &e);
            stored.has_query_index_seq = 1;
            stored.query_index_seq = 11;
            ps_stream_reader *original_reader = NULL;
            ok(ps_stream_read_open(original, 0, first.tail, &original_reader,
                                   &e),
               &e);
            lc_pouch_record_io original_input =
                ps_record_input(original_reader);
            char file_path[1200];
            REQUIRE(snprintf(file_path, sizeof(file_path), "%s.snapshot",
                             path) > 0);
            int fd = -1;
            ps_record_writer writer = {0};
            lc_pouch_record_io underlying;
            if (file) {
              fd = open(file_path, O_RDWR | O_CREAT | O_EXCL, 0600);
              REQUIRE(fd >= 0);
              underlying = lc_pouch_record_file_io(fd);
            } else {
              ok(ps_stream_open(space, NULL, b, 1, NULL, NULL, NULL,
                                &destination, &e),
                 &e);
              ok(ps_record_begin(destination, &writer, &e), &e);
              underlying = ps_record_output(&writer);
            }
            observed o;
            lc_pouch_record_io output = observing(&o, underlying);
            ok(lc_pouch_state_record_write_high_water(&allocator, output, 1000,
                                                      900, &e),
               &e);
            size_t before = o.written;
            ok(lc_pouch_state_record_write_snapshot(&allocator, output,
                                                    snapshot_leaf, &stored,
                                                    NULL, NULL, 0, &e),
               &e);
            REQUIRE(o.written - before < 4096);
            snapshot_source copy;
            snapshot_source_init(&copy, original_input, &stored, &o);
            ok(lc_pouch_state_record_write_snapshot(
                   &allocator, output, snapshot_leaf, &stored, &copy.pub,
                   stored.cipher_bytes ? buffer : NULL,
                   stored.cipher_bytes ? 2 * LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES
                                       : 0,
                   &e),
               &e);
            REQUIRE(copy.range.at == stored.cipher_bytes &&
                    copy.largest <= LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES);
            lc_pouch_state_entry deleted = {0}, placeholder = {0};
            deleted.key = "deleted";
            deleted.record_type = object ? 8 : 2;
            deleted.version = 22;
            deleted.index_seq = 43;
            deleted.has_query_index_seq = 1;
            deleted.query_index_seq = 29;
            deleted.etag = "deleted-etag";
            ok(lc_pouch_state_record_write_snapshot(
                   &allocator, output, NULL, &deleted, NULL, NULL, 0, &e),
               &e);
            placeholder = q;
            placeholder.key = "metadata-only";
            placeholder.record_type = 4;
            placeholder.found = 1;
            placeholder.control = 0;
            placeholder.version = 0;
            placeholder.index_seq = 45;
            placeholder.has_query_index_seq = 1;
            placeholder.query_index_seq = 0;
            placeholder.retention = 1770003600;
            placeholder.staged_delete_marker = 0;
            placeholder.etag = "";
            placeholder.bytes = placeholder.cipher_bytes = 0;
            placeholder.payload_span.present = 0;
            placeholder.descriptor = NULL;
            ok(lc_pouch_state_record_write_snapshot(
                   &allocator, output, NULL, &placeholder, NULL, NULL, 0, &e),
               &e);
            lc_pouch_state_entry inherited = stored;
            inherited.has_query_index_seq = 0;
            inherited.query_index_seq = 9000;
            ok(lc_pouch_state_record_write_snapshot(
                   &allocator, output, NULL, &inherited, NULL, NULL, 0, &e),
               &e);
            uint64_t end;
            ok(lc_pouch_record_io_tail(output, &end, &e), &e);
            REQUIRE(!o.discards);
            ps_stream_reader *destination_reader = NULL;
            lc_pouch_record_io input;
            if (file) {
              REQUIRE(close(fd) == 0);
              fd = open(file_path, O_RDWR);
              REQUIRE(fd >= 0);
              input = lc_pouch_record_file_io(fd);
            } else {
              ok(ps_record_commit(&writer, NULL, NULL, &e), &e);
              ok(ps_stream_seal(destination, &e), &e);
              ok(ps_stream_close(destination, &e), &e);
              ok(ps_stream_open(space, NULL, b, 0, NULL, NULL, NULL,
                                &destination, &e),
                 &e);
              ok(ps_stream_read_open(destination, 0, end, &destination_reader,
                                     &e),
                 &e);
              input = ps_record_input(destination_reader);
            }
            unsigned char *verify = NULL;
            uint64_t at = 0, next;
            for (int record = 0; record < 6; ++record) {
              lc_pouch_state_entry r = {0};
              int found, truncated;
              ok(lc_pouch_state_record_read(input, &allocator, snapshot_leaf,
                                            at, end, 0, 1, &verify, &r, &next,
                                            &found, &truncated, &e),
                 &e);
              REQUIRE(found && !truncated);
              if (!record)
                REQUIRE(r.control && r.version == 1000 &&
                        r.query_index_seq == 900);
              else if (record < 3) {
                snapshot_fields(&r, &stored);
                REQUIRE(r.payload_span.present);
                if (record == 1) {
                  REQUIRE(r.record_type == 3 && r.object_link == object);
                  REQUIRE(!strcmp(r.payload_span.container_leaf, leaf));
                  plaintext(original_input, crypto, &r, bytes, &e);
                } else {
                  REQUIRE(r.record_type == (object ? 7 : 1));
                  REQUIRE(
                      !strcmp(r.payload_span.container_leaf, snapshot_leaf));
                  uint64_t copied = 0;
                  while (copied < stored.cipher_bytes) {
                    size_t n = stored.cipher_bytes - copied <
                                       LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES
                                   ? (size_t)(stored.cipher_bytes - copied)
                                   : LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES;
                    struct iovec before = {buffer, n};
                    struct iovec after = {
                        buffer + LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES, n};
                    ok(lc_pouch_record_io_read(
                           original_input,
                           stored.payload_span.payload_offset + copied, &before,
                           1, &e),
                       &e);
                    ok(lc_pouch_record_io_read(
                           input, r.payload_span.payload_offset + copied,
                           &after, 1, &e),
                       &e);
                    REQUIRE(!memcmp(before.iov_base, after.iov_base, n));
                    copied += n;
                  }
                  plaintext(input, crypto, &r, bytes, &e);
                }
              } else if (record < 5) {
                snapshot_fields(&r, record == 3 ? &deleted : &placeholder);
                REQUIRE(!r.payload_span.present &&
                        r.record_type ==
                            (record == 3 ? deleted.record_type : 4));
              } else {
                snapshot_fields(&r, &inherited);
                REQUIRE(r.record_type == 3 && r.object_link == object);
              }
              lc_pouch_state_entry_cleanup(&allocator, &r);
              at = next;
            }
            REQUIRE(at == end);
            lc_free_with_allocator(&allocator, verify);
            if (file) {
              REQUIRE(close(fd) == 0 && unlink(file_path) == 0);
            } else {
              ps_stream_reader_close(destination_reader);
              ok(ps_stream_close(destination, &e), &e);
            }
            lc_pouch_state_entry_cleanup(&allocator, &stored);
            ps_stream_reader_close(original_reader);
            ok(ps_stream_close(original, &e), &e);
            ok(ps_close(space, &e), &e);
            lc_pouch_crypto_close(crypto);
            REQUIRE(unlink(path) == 0);
          }
  lc_free_with_allocator(&allocator, buffer);
  lc_error_cleanup(&e);
}
static void snapshot_prefix_preserved(int fd) {
  struct stat st;
  char prefix[4];
  REQUIRE(fstat(fd, &st) == 0 && st.st_size == 4);
  REQUIRE(pread(fd, prefix, sizeof(prefix), 0) == 4 &&
          !memcmp(prefix, "KEEP", 4));
}
static void snapshot_reset(int fd) {
  REQUIRE(ftruncate(fd, 0) == 0 && lseek(fd, 0, SEEK_SET) == 0);
  REQUIRE(write(fd, "KEEP", 4) == 4);
}
static int far_tail(void *p, uint64_t *tail, lc_error *e) {
  (void)p;
  (void)e;
  *tail = UINT64_MAX;
  return LC_OK;
}
static void snapshot_faults(const char *path) {
  lc_error e;
  lc_error_init(&e);
  int input_fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
  REQUIRE(input_fd >= 0);
  lc_pouch_record_io input_io = lc_pouch_record_file_io(input_fd);
  observed original;
  input plain = source(257, &original);
  plain.pub.impl = &plain;
  lc_pouch_state_entry q = request(), stored = {0}, deleted = {0}, placeholder;
  ok(lc_pouch_state_record_append(&allocator, NULL,
                                  observing(&original, input_io), leaf, &q,
                                  &plain.pub, 0, &stored, &e),
     &e);
  stored.has_query_index_seq = 1;
  stored.query_index_seq = 11;
  deleted.key = "deleted";
  deleted.version = 22;
  deleted.index_seq = 43;
  deleted.query_index_seq = 29;
  deleted.has_query_index_seq = 1;
  deleted.record_type = 2;
  deleted.etag = "";
  placeholder = stored;
  placeholder.key = "metadata-only";
  placeholder.version = 0;
  placeholder.index_seq = 45;
  placeholder.query_index_seq = 0;
  placeholder.record_type = 4;
  placeholder.bytes = placeholder.cipher_bytes = 0;
  memset(&placeholder.payload_span, 0, sizeof(placeholder.payload_span));
  placeholder.descriptor = NULL;
  placeholder.retention = 0;
  placeholder.staged_delete_marker = 0;
  char output_path[1200];
  REQUIRE(snprintf(output_path, sizeof(output_path), "%s.snapshot", path) > 0);
  int output_fd = open(output_path, O_RDWR | O_CREAT | O_EXCL, 0600);
  REQUIRE(output_fd >= 0);
  lc_pouch_record_io underlying = lc_pouch_record_file_io(output_fd);
  unsigned char buffer[193];
  size_t cases = 0;
  for (int operation = 0; operation < 5; ++operation)
    for (int kind = 0; kind < 3; ++kind) {
      size_t operations = 0;
      for (size_t at = 0; at <= operations; ++at) {
        snapshot_reset(output_fd);
        observed o;
        lc_pouch_record_io output = observing(&o, underlying);
        o.after = kind == 2;
        snapshot_source encoded;
        snapshot_source_init(&encoded, input_io, &stored, &o);
        allocation = 0;
        if (!kind)
          fail_allocation = at ? at : SIZE_MAX;
        else
          o.fail = at;
        const lc_pouch_state_entry *r = operation < 2    ? &stored
                                        : operation == 2 ? &deleted
                                                         : &placeholder;
        int rc = operation == 4 ? lc_pouch_state_record_write_high_water(
                                      &allocator, output, 1000, 900, &e)
                                : lc_pouch_state_record_write_snapshot(
                                      &allocator, output, snapshot_leaf, r,
                                      operation ? NULL : &encoded.pub, buffer,
                                      sizeof(buffer), &e);
        if (!at)
          operations = !kind ? allocation : o.calls;
        fail_allocation = 0;
        if (at ? rc == LC_OK : rc != LC_OK)
          fprintf(stderr,
                  "snapshot operation=%d kind=%d at=%lu/%lu rc=%d: %s\n",
                  operation, kind, (unsigned long)at, (unsigned long)operations,
                  rc, e.message ? e.message : "?");
        REQUIRE(at ? rc != LC_OK : rc == LC_OK);
        ++cases;
        clear(&e);
        if (at)
          snapshot_prefix_preserved(output_fd);
        else {
          struct stat st;
          REQUIRE(fstat(output_fd, &st) == 0 && st.st_size > 4);
          lc_pouch_state_entry decoded = {0};
          uint64_t next;
          int found, truncated;
          unsigned char *verify = NULL;
          ok(lc_pouch_state_record_read(
                 underlying, &allocator, snapshot_leaf, 4, (uint64_t)st.st_size,
                 0, 1, &verify, &decoded, &next, &found, &truncated, &e),
             &e);
          REQUIRE(found && !truncated && next == (uint64_t)st.st_size);
          if (operation == 4)
            REQUIRE(decoded.version == 1000 && decoded.query_index_seq == 900);
          else
            snapshot_fields(&decoded, r);
          lc_free_with_allocator(&allocator, verify);
          lc_pouch_state_entry_cleanup(&allocator, &decoded);
        }
      }
    }
  for (int failure = 1; failure <= 5; ++failure)
    for (int null_error = 0; null_error < 2; ++null_error) {
      snapshot_reset(output_fd);
      observed o;
      lc_pouch_record_io output = observing(&o, underlying);
      snapshot_source encoded;
      snapshot_source_init(&encoded, input_io, &stored, &o);
      encoded.failure = failure == 1 ? 0 : failure;
      if (failure == 1)
        --encoded.range.bytes;
      int rc = lc_pouch_state_record_write_snapshot(
          &allocator, output, snapshot_leaf, &stored, &encoded.pub, buffer,
          sizeof(buffer), null_error ? NULL : &e);
      REQUIRE(rc == (failure == 3                   ? LC_ERR_INVALID
                     : failure == 2 || failure == 5 ? LC_ERR_TRANSPORT
                                                    : LC_ERR_PROTOCOL));
      REQUIRE(o.discards == 1);
      snapshot_prefix_preserved(output_fd);
      clear(&e);
    }
  char *overlong = malloc(1048578);
  REQUIRE(overlong != NULL);
  memset(overlong, 'k', 1048577);
  overlong[1048577] = 0;
  for (int bad = 0; bad < 11; ++bad) {
    snapshot_reset(output_fd);
    observed o;
    lc_pouch_record_io output = observing(&o, underlying);
    snapshot_source encoded;
    snapshot_source_init(&encoded, input_io, &stored, &o);
    lc_pouch_state_entry invalid = stored;
    const char *label = snapshot_leaf;
    if (bad == 0)
      invalid.control = 1;
    if (bad == 1)
      invalid.key = NULL;
    if (bad == 2)
      ++invalid.cipher_bytes;
    if (bad == 4)
      invalid.payload_span.present = 0;
    if (bad == 5)
      invalid.retention = LC_I64_MIN;
    if (bad == 6)
      invalid.key = "unstaged-key";
    if (bad == 7)
      label = "../invalid";
    if (bad == 8)
      invalid.version = 0;
    if (bad == 9)
      invalid.key = overlong;
    if (bad == 10) {
      invalid.metadata = (unsigned char *)overlong;
      invalid.metadata_length = 1048577;
    }
    REQUIRE(lc_pouch_state_record_write_snapshot(
                &allocator, output, label, &invalid, &encoded.pub,
                bad == 3 ? NULL : buffer, sizeof(buffer),
                &e) == LC_ERR_INVALID);
    REQUIRE(!encoded.calls);
    snapshot_prefix_preserved(output_fd);
    clear(&e);
  }
  free(overlong);
  snapshot_reset(output_fd);
  observed o;
  lc_pouch_record_io output = observing(&o, underlying);
  lc_pouch_record_io_ops far = *output.ops;
  far.tail = far_tail;
  output.ops = &far;
  snapshot_source encoded;
  snapshot_source_init(&encoded, input_io, &stored, &o);
  REQUIRE(lc_pouch_state_record_write_snapshot(
              &allocator, output, snapshot_leaf, &stored, &encoded.pub, buffer,
              sizeof(buffer), &e) == LC_ERR_INVALID);
  REQUIRE(!encoded.calls && !o.written);
  snapshot_prefix_preserved(output_fd);
  clear(&e);
  REQUIRE(lc_pouch_state_record_write_high_water(&allocator, output, 1000, 900,
                                                 &e) == LC_ERR_INVALID);
  REQUIRE(!o.written);
  snapshot_prefix_preserved(output_fd);
  clear(&e);
  printf("snapshot codec allocation/I/O failure cases: %lu\n",
         (unsigned long)cases);
  lc_pouch_state_entry_cleanup(&allocator, &stored);
  REQUIRE(close(output_fd) == 0 && unlink(output_path) == 0);
  REQUIRE(close(input_fd) == 0 && unlink(path) == 0);
  lc_error_cleanup(&e);
}
typedef struct parallel {
  ps_space *space;
  lc_pouch_crypto *crypto;
  size_t ordinal;
} parallel;
static void *parallel_writer(void *p) {
  parallel *job = p;
  ps_stream *stream = NULL;
  ps_key k = {job->ordinal + 1, 1, PS_STREAM};
  lc_error e;
  lc_error_init(&e);
  size_t recovered = 0;
  ok(ps_stream_open(job->space, NULL, k, 1, NULL, recover, &recovered, &stream,
                    &e),
     &e);
  for (size_t round = 0; round < 4; ++round) {
    lc_pouch_state_entry q = request(), r = {0};
    ps_record_writer writer;
    observed observation;
    input s = source(body_bytes, &observation);
    s.pub.impl = &s;
    q.version += (lc_pouch_generation)round;
    q.index_seq += (lc_pouch_generation)round;
    ok(ps_record_begin(stream, &writer, &e), &e);
    lc_pouch_record_io io = observing(&observation, ps_record_output(&writer));
    ok(lc_pouch_state_record_append(&allocator, job->crypto, io, leaf, &q,
                                    &s.pub, (int)(job->ordinal % 2), &r, &e),
       &e);
    ok(ps_record_commit(&writer, NULL, NULL, &e), &e);
    metadata(&r, &q, body_bytes);
    ps_stream_reader *reader = NULL;
    ok(ps_stream_read_open(stream, 0,
                           r.payload_span.payload_offset + r.cipher_bytes,
                           &reader, &e),
       &e);
    plaintext(ps_record_input(reader), job->crypto, &r, body_bytes, &e);
    ps_stream_reader_close(reader);
    lc_pouch_state_entry_cleanup(&allocator, &r);
  }
  ok(ps_stream_close(stream, &e), &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void concurrency(const char *path, const char *key) {
  ps_space *space = NULL;
  ps_options options = {0};
  lc_pouch_crypto_open_options crypto_options = {0};
  lc_pouch_crypto *crypto = NULL;
  lc_error e;
  lc_error_init(&e);
  options.initial_capacity = 32U * 1024U * 1024U;
  crypto_options.key_string = key;
  crypto_options.compression_enabled = 1;
  ok(lc_pouch_crypto_open(&allocator, &crypto_options, &crypto, NULL, &e), &e);
  ok(ps_open(path, 1, &options, &space, &e), &e);
  parallel jobs[4];
  pthread_t threads[4];
  for (size_t i = 0; i < 4; ++i) {
    jobs[i].space = space;
    jobs[i].crypto = crypto;
    jobs[i].ordinal = i;
    REQUIRE(pthread_create(&threads[i], NULL, parallel_writer, &jobs[i]) == 0);
  }
  for (size_t i = 0; i < 4; ++i)
    REQUIRE(pthread_join(threads[i], NULL) == 0);
  ok(ps_close(space, &e), &e);
  ok(ps_open(path, 0, &options, &space, &e), &e);
  for (size_t i = 0; i < 4; ++i) {
    size_t recovered = 0;
    ps_stream *stream = NULL;
    ps_key k = {i + 1, 1, PS_STREAM};
    ok(ps_stream_open(space, NULL, k, 0, NULL, recover, &recovered, &stream,
                      &e),
       &e);
    REQUIRE(recovered == 4);
    ok(ps_stream_close(stream, &e), &e);
  }
  ok(ps_close(space, &e), &e);
  lc_pouch_crypto_close(crypto);
  lc_error_cleanup(&e);
  REQUIRE(unlink(path) == 0);
}
typedef struct run {
  const char *path, *key, *mode;
} run;
static void *worker(void *p) {
  run *r = p;
  if (!strcmp(r->mode, "lifecycle"))
    lifecycle(r->path, r->key);
  else if (!strcmp(r->mode, "concurrency"))
    concurrency(r->path, r->key);
  else if (!strcmp(r->mode, "snapshots"))
    snapshot_records(r->path, r->key);
  else if (!strcmp(r->mode, "snapshot-faults"))
    snapshot_faults(r->path);
  else
    faults(r->path);
  return NULL;
}
int main(int argc, char **argv) {
  char prefix[1024], pattern[1100], root[1024], path[1100], *key = NULL;
  REQUIRE(argc == 3);
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/state-record-", argv[1]) > 0);
  REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) > 0);
  REQUIRE(lc_test_tmp_mkdtemp(pattern, root, sizeof(root), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/container", root) > 0);
  ok(lc_pouch_crypto_generate_key_string(&key, &e), &e);
  body = malloc(body_bytes);
  REQUIRE(body != NULL);
  uint32_t random = 1;
  for (size_t i = 0; i < body_bytes; ++i) {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    body[i] = (unsigned char)random;
  }
  pthread_t thread;
  run r = {path, key, argv[2]};
  REQUIRE(pthread_create(&thread, NULL, worker, &r) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  free(body);
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(root, prefix);
  printf("pouch-space state records: %lu observable checks passed\n",
         (unsigned long)checks);
  return 0;
}
