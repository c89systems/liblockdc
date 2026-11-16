#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "lc_pouch_record_io.h"
#include "lc_api_internal.h"
#include <errno.h>
#include <string.h>
#include <unistd.h>

#define RECORD_PARTS 15
static int io_error(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch");
}
static int parts_valid(const struct iovec *parts, int count, uint64_t offset) {
  int i;
  if (!parts || count <= 0 || count > RECORD_PARTS ||
      offset > (uint64_t)LC_I64_MAX)
    return 0;
  for (i = 0; i < count; ++i) {
    if ((!parts[i].iov_base && parts[i].iov_len) ||
        parts[i].iov_len > (uint64_t)LC_I64_MAX - offset)
      return 0;
    offset += parts[i].iov_len;
  }
  return 1;
}
static int file_scalar(int fd, uint64_t offset, void *bytes, size_t count,
                       int reading, lc_error *e) {
  unsigned char *p = bytes;
  ssize_t n;
  while (count) {
    n = reading ? pread(fd, p, count, (off_t)offset) : write(fd, p, count);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return lc_error_set(e, reading && !n ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
                          0,
                          reading ? "failed to read pouch record bytes"
                                  : "failed to write pouch record bytes",
                          n < 0 ? strerror(errno) : NULL,
                          reading && !n ? "collection_corrupt" : NULL, "pouch");
    p += (size_t)n;
    count -= (size_t)n;
    offset += (size_t)n;
  }
  return LC_OK;
}
/* All parts were validated before any I/O. Keep the caller's vectors intact
 * on partial transfers, EINTR and failure. Fifteen stays within POSIX IOV_MAX.
 */
static int file_transfer(int fd, uint64_t offset, const struct iovec *parts,
                         int count, int reading, lc_error *e) {
  struct iovec pending[RECORD_PARTS];
  int first = 0;
  ssize_t n;
  size_t used;
  memcpy(pending, parts, (size_t)count * sizeof(*parts));
  while (first < count) {
    if (!pending[first].iov_len) {
      ++first;
      continue;
    }
    if (reading)
      n = preadv(fd, pending + first, count - first, (off_t)offset);
    else
      n = writev(fd, pending + first, count - first);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return lc_error_set(e, reading && !n ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
                          0,
                          reading ? "failed to read pouch record bytes"
                                  : "failed to write pouch record bytes",
                          n < 0 ? strerror(errno) : NULL,
                          reading && !n ? "collection_corrupt" : NULL, "pouch");
    offset += (size_t)n;
    used = (size_t)n;
    while (first < count && used >= pending[first].iov_len) {
      used -= pending[first].iov_len;
      ++first;
    }
    if (first < count) {
      pending[first].iov_base = (char *)pending[first].iov_base + used;
      pending[first].iov_len -= used;
    }
  }
  return LC_OK;
}
static int file_read(void *context, uint64_t offset, const struct iovec *parts,
                     int count, lc_error *e) {
  if (count == 1)
    return file_scalar((int)(intptr_t)context, offset, parts[0].iov_base,
                       parts[0].iov_len, 1, e);
  return file_transfer((int)(intptr_t)context, offset, parts, count, 1, e);
}
static int stdio_read(void *context, uint64_t offset, const struct iovec *parts,
                      int count, lc_error *e) {
  FILE *fp = context;
  off_t current = ftello(fp);
  int i;
  if (current < 0 ||
      ((uint64_t)current != offset && fseeko(fp, (off_t)offset, SEEK_SET) != 0))
    return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                        "failed to seek pouch record input", strerror(errno),
                        NULL, "pouch");
  for (i = 0; i < count; ++i) {
    size_t remaining = parts[i].iov_len;
    unsigned char *p = parts[i].iov_base;
    while (remaining) {
      size_t got = fread(p, 1, remaining, fp);
      remaining -= got;
      p += got;
      if (ferror(fp)) {
        if (errno == EINTR) {
          clearerr(fp);
          continue;
        }
        return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                            "failed to read pouch record input",
                            strerror(errno), NULL, "pouch");
      }
      if (!got)
        return io_error(e, LC_ERR_PROTOCOL, "pouch record input is truncated");
    }
  }
  return LC_OK;
}
lc_pouch_record_io lc_pouch_record_stdio_io(FILE *fp) {
  static const lc_pouch_record_io_ops ops = {stdio_read, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = fp != NULL ? &ops : NULL;
  io.context = fp;
  return io;
}
static int file_append(void *context, const struct iovec *parts, int count,
                       lc_error *e) {
  if (count == 1)
    return file_scalar((int)(intptr_t)context, 0, parts[0].iov_base,
                       parts[0].iov_len, 0, e);
  return file_transfer((int)(intptr_t)context, 0, parts, count, 0, e);
}
static int file_patch(void *context, uint64_t offset, const struct iovec *parts,
                      int count, lc_error *e) {
  int fd = (int)(intptr_t)context;
  if (lseek(fd, (off_t)offset, SEEK_SET) < 0)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                        "failed to seek pouch record patch", strerror(errno),
                        NULL, "pouch");
  if (count == 1)
    return file_scalar(fd, 0, parts[0].iov_base, parts[0].iov_len, 0, e);
  return file_transfer(fd, 0, parts, count, 0, e);
}
static int file_tail(void *context, uint64_t *out, lc_error *e) {
  off_t end = lseek((int)(intptr_t)context, 0, SEEK_END);
  if (end < 0)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                        "failed to seek pouch record tail", strerror(errno),
                        NULL, "pouch");
  *out = (uint64_t)end;
  return LC_OK;
}
static void file_discard(void *context, uint64_t offset) {
  int ignored, fd = (int)(intptr_t)context;
  if (offset > (uint64_t)LC_I64_MAX)
    return;
  ignored = ftruncate(fd, (off_t)offset);
  (void)ignored;
  (void)lseek(fd, (off_t)offset, SEEK_SET);
}
lc_pouch_record_io lc_pouch_record_file_io(int fd) {
  static const lc_pouch_record_io_ops ops = {file_read, file_append, file_patch,
                                             file_tail, file_discard};
  lc_pouch_record_io io;
  io.ops = fd == -1 ? NULL : &ops;
  io.context = (void *)(intptr_t)fd;
  return io;
}
int lc_pouch_record_io_read(lc_pouch_record_io io, uint64_t offset,
                            const struct iovec *parts, int count, lc_error *e) {
  if (!io.ops || !io.ops->read || !parts_valid(parts, count, offset))
    return io_error(e, LC_ERR_INVALID, "invalid pouch record read");
  return io.ops->read(io.context, offset, parts, count, e);
}
int lc_pouch_record_io_append(lc_pouch_record_io io, const struct iovec *parts,
                              int count, lc_error *e) {
  if (!io.ops || !io.ops->append || !parts_valid(parts, count, 0))
    return io_error(e, LC_ERR_INVALID, "invalid pouch record append");
  return io.ops->append(io.context, parts, count, e);
}
int lc_pouch_record_io_patch(lc_pouch_record_io io, uint64_t offset,
                             const struct iovec *parts, int count,
                             lc_error *e) {
  if (!io.ops || !io.ops->patch || !parts_valid(parts, count, offset))
    return io_error(e, LC_ERR_INVALID, "invalid pouch record patch");
  return io.ops->patch(io.context, offset, parts, count, e);
}
int lc_pouch_record_io_tail(lc_pouch_record_io io, uint64_t *out, lc_error *e) {
  if (!io.ops || !io.ops->tail || !out)
    return io_error(e, LC_ERR_INVALID, "invalid pouch record tail");
  return io.ops->tail(io.context, out, e);
}
static size_t span_read(lc_source *source, void *bytes, size_t capacity,
                        lc_error *e) {
  lc_pouch_record_span *span = source->impl;
  uint64_t remaining = span->length - (span->offset - span->start);
  size_t take = span->prefix_size - span->prefix_used;
  struct iovec part;
  if (capacity > remaining)
    capacity = (size_t)remaining;
  if (take > capacity)
    take = capacity;
  if (take) {
    memmove(bytes, span->prefix + span->prefix_used, take);
    span->prefix_used += take;
    span->offset += take;
    return take;
  }
  if (!capacity)
    return 0U;
  part.iov_base = bytes;
  part.iov_len = capacity;
  if (lc_pouch_record_io_read(span->input, span->offset, &part, 1, e) != LC_OK)
    return 0U;
  span->offset += capacity;
  return capacity;
}
static int span_reset(lc_source *source, lc_error *e) {
  lc_pouch_record_span *span = source->impl;
  (void)e;
  if (span->prefix_used) {
    span->prefix = NULL;
    span->prefix_size = span->prefix_used = 0U;
  }
  span->offset = span->start;
  return LC_OK;
}
int lc_pouch_record_span_bind(lc_pouch_record_span *span,
                              lc_pouch_record_io input, uint64_t start,
                              uint64_t length, const void *prefix,
                              size_t prefix_size, lc_error *e) {
  if (!span || !input.ops || !input.ops->read || start > (uint64_t)LC_I64_MAX ||
      length > (uint64_t)LC_I64_MAX - start || prefix_size > length ||
      (!prefix && prefix_size))
    return io_error(e, LC_ERR_INVALID, "invalid pouch encoded range");
  memset(span, 0, sizeof(*span));
  span->source.impl = span;
  span->source.read = span_read;
  span->source.reset = span_reset;
  span->input = input;
  span->prefix = prefix;
  span->prefix_size = prefix_size;
  span->start = span->offset = start;
  span->length = length;
  return LC_OK;
}
static int encode(unsigned char type, const void *key, size_t key_bytes,
                  const void *meta, size_t meta_bytes, uint64_t payload,
                  unsigned long crc, unsigned long flags, unsigned char *wire,
                  lc_error *e) {
  lc_pouch_record_header h;
  if ((!key && key_bytes) || (!meta && meta_bytes) ||
      key_bytes > 0xffffffffUL || meta_bytes > 0xffffffffUL)
    return io_error(e, LC_ERR_INVALID, "invalid pouch record prefix");
  memset(&h, 0, sizeof(h));
  h.type = type;
  h.flags = flags;
  h.key_len = (unsigned long)key_bytes;
  h.meta_len = (unsigned long)meta_bytes;
  h.payload_len = payload;
  h.payload_crc = crc;
  lc_pouch_record_header_encode(&h, wire);
  return LC_OK;
}
int lc_pouch_record_write_prefix(lc_pouch_record_io io, unsigned char type,
                                 const void *key, size_t key_bytes,
                                 const void *meta, size_t meta_bytes,
                                 uint64_t payload, unsigned long crc,
                                 unsigned long flags, lc_error *e) {
  unsigned char wire[LC_POUCH_RECORD_HEADER_BYTES];
  struct iovec parts[3];
  int rc = encode(type, key, key_bytes, meta, meta_bytes, payload, crc, flags,
                  wire, e);
  if (rc != LC_OK)
    return rc;
  parts[0].iov_base = wire;
  parts[0].iov_len = sizeof(wire);
  parts[1].iov_base = (void *)key;
  parts[1].iov_len = key_bytes;
  parts[2].iov_base = (void *)meta;
  parts[2].iov_len = meta_bytes;
  return lc_pouch_record_io_append(io, parts, 3, e);
}
int lc_pouch_record_finalize(lc_pouch_record_io io, uint64_t offset,
                             unsigned char type, const void *key,
                             size_t key_bytes, const void *meta,
                             size_t meta_bytes, uint64_t payload,
                             unsigned long crc, lc_error *e) {
  unsigned char wire[LC_POUCH_RECORD_HEADER_BYTES];
  struct iovec parts[2];
  int rc =
      encode(type, key, key_bytes, meta, meta_bytes, payload, crc, 0, wire, e);
  if (rc != LC_OK)
    return rc;
  if (offset > (uint64_t)LC_I64_MAX - sizeof(wire) ||
      key_bytes > (uint64_t)LC_I64_MAX - offset - sizeof(wire) ||
      meta_bytes > (uint64_t)LC_I64_MAX - offset - sizeof(wire) - key_bytes)
    return io_error(e, LC_ERR_INVALID,
                    "pouch record finalization exceeds range");
  parts[0].iov_base = (void *)key;
  parts[0].iov_len = key_bytes;
  parts[1].iov_base = (void *)meta;
  parts[1].iov_len = meta_bytes;
  if (key_bytes || meta_bytes)
    rc = lc_pouch_record_io_patch(io, offset + sizeof(wire), parts, 2, e);
  if (rc == LC_OK) {
    parts[0].iov_base = wire;
    parts[0].iov_len = sizeof(wire);
    rc = lc_pouch_record_io_patch(io, offset, parts, 1, e);
  }
  return rc;
}
