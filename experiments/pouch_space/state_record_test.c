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
} observed;
static int io_fail(observed *o, lc_error *e) {
  return ++o->calls == o->fail
             ? lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected record I/O", NULL,
                            NULL, "test")
             : LC_OK;
}
static int rd(void *p, uint64_t at, const struct iovec *v, int n, lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return rc == LC_OK ? lc_pouch_record_io_read(o->underlying, at, v, n, e) : rc;
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
  return rc;
}
static int patch(void *p, uint64_t at, const struct iovec *v, int n,
                 lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return rc == LC_OK ? lc_pouch_record_io_patch(o->underlying, at, v, n, e)
                     : rc;
}
static int tail(void *p, uint64_t *at, lc_error *e) {
  observed *o = p;
  int rc = io_fail(o, e);
  return rc == LC_OK ? lc_pouch_record_io_tail(o->underlying, at, e) : rc;
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
    ok(ps_stream_open(space, k, 1, NULL, recover, &recovered, &stream, &e), &e);
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
        ok(ps_record_commit(&writer, &e), &e);
        ps_record_abort(&writer);
        lc_pouch_state_entry_cleanup(&allocator, &r);
        if (disable == 0 && n == 2 && mode % 2)
          ok(ps_checkpoint(space, &e), &e);
      }
    ok(ps_stream_status(stream, &info, &e), &e);
    ok(ps_stream_close(stream, &e), &e);
    ok(ps_close(space, &e), &e);
    ok(ps_open(path, 0, NULL, &space, &e), &e);
    ok(ps_stream_open(space, k, 0, &info.ref, recover, &recovered, &stream, &e),
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
  ok(ps_stream_open(job->space, k, 1, NULL, recover, &recovered, &stream, &e),
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
    ok(ps_record_commit(&writer, &e), &e);
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
    ok(ps_stream_open(space, k, 0, NULL, recover, &recovered, &stream, &e), &e);
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
