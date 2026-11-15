#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_collection_record.h"
#include "lc_pouch_internal.h"
#include "record_io.h"
#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks;
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static void check(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "record test error %d: %s\n", rc,
            e->message ? e->message : "no detail");
  REQUIRE(rc == LC_OK);
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}

/* Exercise the real segmented adapter's short-vector/EINTR handling. Other
 * fixture I/O remains unmodified. Zero/error modes are terminal for this call.
 */
static int transfer_mode, transfer_calls;
ssize_t __real_writev(int, const struct iovec *, int);
ssize_t __real_write(int, const void *, size_t);
#ifdef RECORD_PREADV64
#define REAL_PREADV __real_preadv64
#define WRAP_PREADV __wrap_preadv64
#define REAL_PREAD __real_pread64
#define WRAP_PREAD __wrap_pread64
#else
#define REAL_PREADV __real_preadv
#define WRAP_PREADV __wrap_preadv
#define REAL_PREAD __real_pread
#define WRAP_PREAD __wrap_pread
#endif
ssize_t REAL_PREADV(int, const struct iovec *, int, off_t);
ssize_t REAL_PREAD(int, void *, size_t, off_t);
static ssize_t scalar_scripted(int fd, void *bytes, size_t n, off_t at,
                               int rd) {
  if (!transfer_mode)
    return rd ? REAL_PREAD(fd, bytes, n, at) : __real_write(fd, bytes, n);
  ++transfer_calls;
  if (transfer_mode == 2)
    return 0;
  if (transfer_mode == 3) {
    errno = EIO;
    return -1;
  }
  if (transfer_calls % 2) {
    errno = EINTR;
    return -1;
  }
  if (n > 3)
    n = 3;
  return rd ? REAL_PREAD(fd, bytes, n, at) : __real_write(fd, bytes, n);
}
ssize_t __wrap_write(int fd, const void *bytes, size_t n) {
  return scalar_scripted(fd, (void *)bytes, n, 0, 0);
}
ssize_t WRAP_PREAD(int fd, void *bytes, size_t n, off_t at) {
  return scalar_scripted(fd, bytes, n, at, 1);
}
static ssize_t scripted(int fd, const struct iovec *p, int n, off_t at,
                        int rd) {
  struct iovec first;
  if (!transfer_mode)
    return rd ? REAL_PREADV(fd, p, n, at) : __real_writev(fd, p, n);
  ++transfer_calls;
  if (transfer_mode == 2)
    return 0;
  if (transfer_mode == 3) {
    errno = EIO;
    return -1;
  }
  if (transfer_calls % 2) {
    errno = EINTR;
    return -1;
  }
  while (n && !p->iov_len) {
    ++p;
    --n;
  }
  if (!n)
    return 0;
  first = *p;
  if (first.iov_len > 3)
    first.iov_len = 3;
  return rd ? REAL_PREADV(fd, &first, 1, at) : __real_writev(fd, &first, 1);
}
ssize_t __wrap_writev(int fd, const struct iovec *p, int n) {
  return scripted(fd, p, n, 0, 0);
}
ssize_t WRAP_PREADV(int fd, const struct iovec *p, int n, off_t at) {
  return scripted(fd, p, n, at, 1);
}
static void native_vectors(const char *path) {
  lc_error e;
  int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
  lc_pouch_record_io io = lc_pouch_record_file_io(fd);
  struct iovec parts[4], before[4];
  unsigned char buffer[16], prefix[32];
  lc_pouch_record_header h;
  uint64_t tail;
  lc_error_init(&e);
  REQUIRE(fd >= 0);
  parts[0].iov_base = (void *)"abcde";
  parts[0].iov_len = 5;
  parts[1].iov_base = NULL;
  parts[1].iov_len = 0;
  parts[2].iov_base = (void *)"fghijkl";
  parts[2].iov_len = 7;
  parts[3].iov_base = NULL;
  parts[3].iov_len = 0;
  memcpy(before, parts, sizeof(parts));
  transfer_mode = 1;
  transfer_calls = 0;
  check(lc_pouch_record_io_append(io, parts, 4, &e), &e);
  REQUIRE(transfer_calls >= 10);
  REQUIRE(!memcmp(before, parts, sizeof(parts)));
  /* Scalar operations retain their native syscalls, with the same exact and
   * interrupted-transfer contract as vectors. */
  transfer_calls = 0;
  check(lc_pouch_record_io_append(io, parts, 1, &e), &e);
  REQUIRE(transfer_calls >= 4);
  parts[0].iov_base = buffer;
  transfer_calls = 0;
  check(lc_pouch_record_io_read(io, 12, parts, 1, &e), &e);
  REQUIRE(transfer_calls >= 4 && !memcmp(buffer, "abcde", 5));
  parts[0].iov_base = (void *)"abcde";
  parts[0].iov_base = buffer;
  parts[0].iov_len = 4;
  parts[2].iov_base = buffer + 4;
  parts[2].iov_len = 8;
  memcpy(before, parts, sizeof(parts));
  transfer_calls = 0;
  check(lc_pouch_record_io_read(io, 0, parts, 4, &e), &e);
  REQUIRE(transfer_calls >= 10);
  REQUIRE(!memcmp(buffer, "abcdefghijkl", 12));
  REQUIRE(!memcmp(before, parts, sizeof(parts)));
  for (transfer_mode = 2; transfer_mode <= 3; ++transfer_mode) {
    REQUIRE(lc_pouch_record_io_read(io, 0, parts, 4, &e) ==
            (transfer_mode == 2 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT));
    clear(&e);
    REQUIRE(lc_pouch_record_io_read(io, 0, parts, 1, &e) ==
            (transfer_mode == 2 ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT));
    clear(&e);
    REQUIRE(lc_pouch_record_io_append(io, parts, 1, &e) == LC_ERR_TRANSPORT);
    clear(&e);
    REQUIRE(lc_pouch_record_io_append(io, parts, 4, &e) == LC_ERR_TRANSPORT);
    clear(&e);
  }
  transfer_mode = 0;
  REQUIRE(lc_pouch_record_io_read(io, UINT64_MAX, parts, 4, &e) ==
          LC_ERR_INVALID);
  clear(&e);
  parts[1].iov_len = 1;
  REQUIRE(lc_pouch_record_io_append(io, parts, 4, &e) == LC_ERR_INVALID);
  clear(&e);
  parts[1].iov_len = 0;
  REQUIRE(lc_pouch_record_io_read(io, 0, parts, 16, &e) == LC_ERR_INVALID);
  clear(&e);
  io.ops->discard(io.context, 0);
  check(lc_pouch_record_write_prefix(io, 1, "key", 3, "meta", 4, 0, 0,
                                     LC_POUCH_RECORD_FLAG_PENDING, &e),
        &e);
  REQUIRE(pread(fd, prefix, sizeof(prefix), 0) == sizeof(prefix));
  check(lc_pouch_record_header_decode(prefix, &h, &e), &e);
  REQUIRE(h.flags == LC_POUCH_RECORD_FLAG_PENDING);
  transfer_mode = 3;
  REQUIRE(lc_pouch_record_finalize(io, 0, 1, "key", 3, "done", 4, 0, 0, &e) ==
          LC_ERR_TRANSPORT);
  clear(&e);
  transfer_mode = 0;
  REQUIRE(pread(fd, prefix, sizeof(prefix), 0) == sizeof(prefix));
  check(lc_pouch_record_header_decode(prefix, &h, &e), &e);
  REQUIRE(h.flags == LC_POUCH_RECORD_FLAG_PENDING);
  check(lc_pouch_record_finalize(io, 0, 1, "key", 3, "done", 4, 0, 0, &e), &e);
  check(lc_pouch_record_io_tail(io, &tail, &e), &e);
  REQUIRE(tail == 39);
  REQUIRE(pread(fd, prefix, sizeof(prefix), 0) == sizeof(prefix));
  check(lc_pouch_record_header_decode(prefix, &h, &e), &e);
  REQUIRE(!h.flags && h.key_len == 3 && h.meta_len == 4);
  REQUIRE(pread(fd, buffer, 4, 35) == 4 && !memcmp(buffer, "done", 4));
  REQUIRE(lc_pouch_record_finalize(io, UINT64_MAX, 1, "k", 1, NULL, 0, 0, 0,
                                   &e) == LC_ERR_INVALID);
  clear(&e);
  REQUIRE(close(fd) == 0);
  REQUIRE(unlink(path) == 0);
  lc_error_cleanup(&e);
}

static const char leaf[] = "seg-00000000000000000001.log";
typedef struct input {
  lc_source pub;
  const unsigned char *body;
  size_t bytes, at, chunk;
  ps_record_writer *output;
  int observed_flow, fail;
} input;
static size_t produce(lc_source *source, void *buffer, size_t count,
                      lc_error *e) {
  input *in = source->impl;
  if (in->fail && in->at >= in->bytes / 2) {
    lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected producer failure", NULL,
                 NULL, "test");
    return 0;
  }
  if (in->at >= in->bytes / 2 && in->bytes > 131072 && in->output->writer &&
      in->output->tail > in->output->start + 16384)
    in->observed_flow = 1;
  if (count > in->chunk)
    count = in->chunk;
  if (count > in->bytes - in->at)
    count = in->bytes - in->at;
  memcpy(buffer, in->body + in->at, count);
  in->at += count;
  return count;
}
typedef struct encoded_source {
  lc_source pub;
  ps_stream_reader *reader;
  uint64_t start, bytes, at;
} encoded_source;
static size_t encoded_read(lc_source *source, void *buffer, size_t count,
                           lc_error *e) {
  encoded_source *s = source->impl;
  if (count > s->bytes - s->at)
    count = (size_t)(s->bytes - s->at);
  if (ps_stream_read_at(s->reader, s->start + s->at, buffer, count, e) != LC_OK)
    return 0;
  s->at += count;
  return count;
}
static int encoded_reset(lc_source *source, lc_error *e) {
  encoded_source *s = source->impl;
  (void)e;
  s->at = 0;
  return LC_OK;
}
static void verify_body(lc_pouch *pouch, ps_stream_reader *reader,
                        const lc_pouch_collection_record *record,
                        const unsigned char *expected, size_t length,
                        lc_error *e) {
  lc_pouch_crypto_reader *codec = NULL;
  lc_source *decoded = NULL;
  encoded_source encoded;
  unsigned char buffer[1024], digest[32];
  char hash[65];
  size_t got, at;
  unsigned int size;
  memset(&encoded, 0, sizeof(encoded));
  encoded.pub.impl = &encoded;
  encoded.pub.read = encoded_read;
  encoded.pub.reset = encoded_reset;
  encoded.reader = reader;
  encoded.start = record->payload_offset;
  encoded.bytes = record->stored_bytes;
  REQUIRE(EVP_Digest(expected, length, digest, &size, EVP_sha256(), NULL) ==
              1 &&
          size == 32);
  for (size_t i = 0; i < 32; ++i)
    snprintf(hash + 2 * i, 3, "%02x", digest[i]);
  REQUIRE(!strcmp(hash, record->etag));
  check(lc_pouch_crypto_reader_open(pouch->crypto, &codec, e), e);
  for (int pass = 0; pass < 2; ++pass) {
    check(lc_pouch_crypto_reader_bind_source(
              codec, record->context, record->context, &encoded.pub,
              encoded.bytes, record->descriptor[0] ? record->descriptor : NULL,
              &decoded, e),
          e);
    at = 0;
    while ((got = decoded->read(decoded, buffer, sizeof(buffer), e)) != 0) {
      REQUIRE(e->code == LC_OK && got <= length - at);
      REQUIRE(!memcmp(buffer, expected + at, got));
      at += got;
    }
    REQUIRE(e->code == LC_OK && at == length);
    lc_pouch_crypto_reader_clear(codec);
    encoded.at = 0;
  }
  lc_pouch_crypto_reader_close(codec);
}
typedef struct recovered {
  size_t records;
} recovered;
static int recover_records(ps_stream *stream, uint64_t checkpoint,
                           uint64_t capacity, uint64_t *accepted, void *context,
                           lc_error *e) {
  recovered *found = context;
  lc_pouch_record_io io = ps_record_recovery_input(stream);
  lc_pouch_collection_record record;
  struct iovec part;
  unsigned char bytes[1024];
  uint64_t at = 0, cursor;
  unsigned long crc;
  int rc;
  found->records = 0;
  while (at < capacity) {
    rc = lc_pouch_collection_record_read(io, at, capacity, leaf, &record, e);
    if (rc != LC_OK) {
      if (at < checkpoint || (rc != LC_ERR_PROTOCOL && rc != LC_ERR_PENDING))
        return rc;
      clear(e);
      break;
    }
    if (record.record_offset >= checkpoint) {
      cursor = record.payload_offset;
      crc = 0;
      while (cursor < record.next_offset) {
        part.iov_base = bytes;
        part.iov_len = record.next_offset - cursor < sizeof(bytes)
                           ? (size_t)(record.next_offset - cursor)
                           : sizeof(bytes);
        rc = lc_pouch_record_io_read(io, cursor, &part, 1, e);
        if (rc != LC_OK)
          return rc;
        crc = crc32(crc, bytes, (uInt)part.iov_len);
        cursor += part.iov_len;
      }
      if (crc != record.stored_crc)
        break;
    }
    REQUIRE(record.next_offset > at &&
            (at >= checkpoint || record.next_offset <= checkpoint));
    at = record.next_offset;
    ++found->records;
  }
  *accepted = at;
  return LC_OK;
}
/* Fail each canonical callback before or after its actual bytes are written.
 * The latter includes a valid final header whose callback reports failure. */
typedef struct fault_output {
  lc_pouch_record_io inner;
  size_t at, fail;
  int after;
} fault_output;
static int injected(lc_error *e) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected canonical I/O failure",
                      NULL, NULL, "test");
}
static int fault_append(void *p, const struct iovec *v, int n, lc_error *e) {
  fault_output *f = p;
  int hit = ++f->at == f->fail;
  if (hit && !f->after)
    return injected(e);
  int rc = lc_pouch_record_io_append(f->inner, v, n, e);
  return hit && rc == LC_OK ? injected(e) : rc;
}
static int fault_patch(void *p, uint64_t at, const struct iovec *v, int n,
                       lc_error *e) {
  fault_output *f = p;
  ps_record_writer *output = f->inner.context;
  REQUIRE(output->tail - output->start > PS_CHUNK);
  int hit = ++f->at == f->fail;
  if (hit && !f->after)
    return injected(e);
  int rc = lc_pouch_record_io_patch(f->inner, at, v, n, e);
  return hit && rc == LC_OK ? injected(e) : rc;
}
static int fault_tail(void *p, uint64_t *at, lc_error *e) {
  fault_output *f = p;
  int hit = ++f->at == f->fail;
  if (hit && !f->after)
    return injected(e);
  int rc = lc_pouch_record_io_tail(f->inner, at, e);
  return hit && rc == LC_OK ? injected(e) : rc;
}
static void fault_discard(void *p, uint64_t at) {
  fault_output *f = p;
  f->inner.ops->discard(f->inner.context, at);
}
static const lc_pouch_record_io_ops fault_ops = {
    NULL, fault_append, fault_patch, fault_tail, fault_discard};

typedef struct run {
  char path[1200];
  unsigned char *body, *fault_body;
  size_t bytes;
  char *key;
} run;
static void *container_records(void *context) {
  run *run = context;
  lc_error e;
  lc_pouch *pouch = calloc(1, sizeof(*pouch));
  ps_space *space;
  ps_stream *stream;
  ps_stream_reader *reader, *window;
  ps_record_writer output;
  ps_stream_info status;
  ps_key key = {1, 1, PS_STREAM};
  char id[LC_XID_STRING_SIZE];
  recovered found;
  lc_pouch_collection_record original, read, next;
  unsigned char prefix[17], byte;
  size_t prefix_size, got;
  input source;
  lc_error_init(&e);
  REQUIRE(pouch != NULL);
  check(lc_xid_new(id, &e), &e);
  for (int mode = 0; mode < 4; ++mode)
    for (int durable = 0; durable < 2; ++durable) {
      ps_options opts = {0};
      opts.durable_sync = durable;
      lc_pouch_crypto_open_options crypto = {0};
      crypto.key_string = mode & 1 ? run->key : NULL;
      crypto.compression_enabled = mode & 2;
      check(lc_pouch_crypto_open(NULL, &crypto, &pouch->crypto, NULL, &e), &e);
      check(ps_open(run->path, 1, &opts, &space, &e), &e);
      check(ps_stream_open(space, NULL, key, 1, NULL, recover_records, &found,
                           &stream, &e),
            &e);
      check(ps_record_begin(stream, &output, &e), &e);
      memset(&source, 0, sizeof(source));
      source.pub.impl = &source;
      source.pub.read = produce;
      source.body = run->body;
      source.bytes = run->bytes;
      source.chunk = 4093;
      source.output = &output;
      check(lc_pouch_collection_record_append(
                pouch, ps_record_output(&output), leaf, id, "canonical", 5, 7,
                -60, 1, -120, &source.pub, &original, &e),
            &e);
      REQUIRE(source.observed_flow && original.published &&
              original.position == 5 && original.generation == 7);
      REQUIRE(original.retention == original.updated_at_unix + 60 &&
              original.collection_retention == original.updated_at_unix + 120);
      REQUIRE(original.next_offset == output.tail &&
              original.stored_bytes > 65536);
      check(ps_record_commit(&output, NULL, NULL, &e), &e);
      check(ps_stream_read_open(stream, 0, original.next_offset, &reader, &e),
            &e);
      check(lc_pouch_collection_record_read_prefix(
                ps_record_input(reader), 0, original.next_offset, leaf, &read,
                prefix, sizeof(prefix), &prefix_size, &e),
            &e);
      REQUIRE(prefix_size == sizeof(prefix) &&
              read.next_offset == original.next_offset);
      REQUIRE(!strcmp(read.etag, original.etag) &&
              !strcmp(read.descriptor, original.descriptor));
      verify_body(pouch, reader, &read, run->body, run->bytes, &e);
      check(ps_stream_read_open(stream, read.payload_offset, 8, &window, &e),
            &e);
      REQUIRE(ps_stream_read_at(window, read.payload_offset - 1, &byte, 1,
                                &e) == LC_ERR_INVALID);
      clear(&e);
      REQUIRE(ps_stream_read_at(window, read.payload_offset + 8, &byte, 1,
                                &e) == LC_ERR_INVALID);
      clear(&e);
      check(ps_stream_read_at(window, read.payload_offset, &byte, 1, &e), &e);
      check(ps_stream_read(window, prefix, 1, &got, &e), &e);
      REQUIRE(got == 1 && prefix[0] == byte);
      ps_stream_reader_close(window);
      /* A failed producer discards only its private suffix; a held old reader
       * remains valid across rollback and a subsequent successful append. */
      check(ps_record_begin(stream, &output, &e), &e);
      source.at = 0;
      source.fail = 1;
      REQUIRE(lc_pouch_collection_record_append(
                  pouch, ps_record_output(&output), leaf, id, "failed", 6, 8, 0,
                  0, 0, &source.pub, &next, &e) == LC_ERR_TRANSPORT);
      clear(&e);
      ps_record_abort(&output);
      source.fail = 0;
      check(ps_stream_status(stream, &status, &e), &e);
      REQUIRE(status.tail == original.next_offset);
      /* Flush records into physical grants before finalization, then exercise
       * every canonical append/patch/tail failure site, including a fully
       * written final header whose callback reports failure.
       */
      size_t sites = 0;
      for (int after = 0; after < 2; ++after)
        for (size_t site = 0; site <= sites; ++site) {
          fault_output fault = {0};
          lc_pouch_record_io io = {&fault_ops, &fault};
          check(ps_record_begin(stream, &output, &e), &e);
          fault.inner = ps_record_output(&output);
          fault.fail = site;
          fault.after = after;
          source.body = run->fault_body;
          source.bytes = 65537;
          source.at = 0;
          source.chunk = 4093;
          int rc = lc_pouch_collection_record_append(pouch, io, leaf, id,
                                                     "small", 6, 8, 0, 0, 0,
                                                     &source.pub, &next, &e);
          if (!site) {
            check(rc, &e);
            sites = fault.at;
            REQUIRE(sites >= 6);
          } else {
            REQUIRE(rc == LC_ERR_TRANSPORT);
            clear(&e);
          }
          ps_record_abort(&output);
          check(ps_stream_status(stream, &status, &e), &e);
          REQUIRE(status.tail == original.next_offset);
        }
      verify_body(pouch, reader, &read, run->body, run->bytes, &e);
      check(ps_record_begin(stream, &output, &e), &e);
      source.at = 0;
      check(lc_pouch_collection_record_append(pouch, ps_record_output(&output),
                                              leaf, id, "committed", 6, 8, 0, 0,
                                              0, &source.pub, &next, &e),
            &e);
      check(ps_record_commit(&output, NULL, NULL, &e), &e);
      REQUIRE(ps_stream_read_at(reader, original.next_offset, &byte, 1, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      verify_body(pouch, reader, &read, run->body, run->bytes, &e);
      ps_stream_reader_close(reader);
      check(ps_checkpoint(space, &e), &e);
      check(ps_stream_close(stream, &e), &e);
      check(ps_close(space, &e), &e);
      check(ps_open(run->path, 0, &opts, &space, &e), &e);
      check(ps_stream_open(space, NULL, key, 0, NULL, recover_records, &found,
                           &stream, &e),
            &e);
      REQUIRE(found.records == 2);
      check(ps_stream_read_open(stream, 0, original.next_offset, &reader, &e),
            &e);
      check(lc_pouch_collection_record_read(ps_record_input(reader), 0,
                                            original.next_offset, leaf, &read,
                                            &e),
            &e);
      verify_body(pouch, reader, &read, run->body, run->bytes, &e);
      check(ps_stream_retire(stream, &e), &e);
      verify_body(pouch, reader, &read, run->body, run->bytes, &e);
      REQUIRE(ps_stream_read_open(stream, 0, 1, &window, &e) == LC_ERR_INVALID);
      clear(&e);
      ps_stream_reader_close(reader);
      check(ps_checkpoint(space, &e), &e);
      check(ps_stream_close(stream, &e), &e);
      check(ps_close(space, &e), &e);
      REQUIRE(unlink(run->path) == 0);
      lc_pouch_crypto_close(pouch->crypto);
      pouch->crypto = NULL;
    }
  free(pouch);
  lc_error_cleanup(&e);
  return NULL;
}
int main(int argc, char **argv) {
  char root[1024], prefix[1024], template_path[1100];
  run run = {0};
  pthread_t worker;
  lc_error e;
  uint32_t random = 1;
  REQUIRE(argc == 2);
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/pouch-record-", argv[1]) > 0);
  REQUIRE(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) >
          0);
  REQUIRE(lc_test_tmp_mkdtemp(template_path, root, sizeof(root), prefix));
  REQUIRE(snprintf(run.path, sizeof(run.path), "%s/container", root) > 0);
  native_vectors(run.path);
  lc_error_init(&e);
  check(lc_pouch_crypto_generate_key_string(&run.key, &e), &e);
  run.bytes = 1048601;
  run.body = malloc(run.bytes);
  REQUIRE(run.body != NULL);
  memcpy(run.body, "{\"data\":\"", 9);
  for (size_t i = 9; i < run.bytes - 2; ++i) {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    run.body[i] = (unsigned char)('a' + random % 26);
  }
  run.body[run.bytes - 2] = '"';
  run.body[run.bytes - 1] = '}';
  run.fault_body = malloc(65537);
  REQUIRE(run.fault_body != NULL);
  memcpy(run.fault_body, run.body, 65535);
  run.fault_body[65535] = '"';
  run.fault_body[65536] = '}';
  REQUIRE(pthread_create(&worker, NULL, container_records, &run) == 0);
  REQUIRE(pthread_join(worker, NULL) == 0);
  free(run.body);
  free(run.fault_body);
  lc_pouch_crypto_key_string_free(run.key);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(root, prefix);
  printf("pouch-space canonical records: %lu observable checks passed\n",
         (unsigned long)checks);
  return 0;
}
