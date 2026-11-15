#include "record_io.h"
#include "frame.h"
#include "lc_api_internal.h"
#include <string.h>
#include <zlib.h>

static int record_error(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL,
                      "pouch-stream");
}
int ps_record_begin(ps_stream *stream, ps_record_writer *out, lc_error *e) {
  int rc;
  if (!out)
    return record_error(e, "canonical append requires output");
  memset(out, 0, sizeof(*out));
  rc = ps_stream_begin(stream, &out->writer, e);
  if (rc == LC_OK)
    out->start = out->tail = ps_stream_writer_start(out->writer);
  return rc;
}
int ps_record_reserve(ps_record_writer *out, ps_budget *budget, uint64_t bytes,
                      lc_error *e) {
  if (!out || !out->writer || out->failed)
    return record_error(e, "canonical append is not open");
  return ps_stream_reserve(out->writer, budget, bytes, e);
}
void ps_record_abort(ps_record_writer *out) {
  if (out) {
    ps_stream_writer_close(out->writer);
    out->writer = NULL;
  }
}
int ps_record_frame_begin(ps_record_writer *out, uint64_t length, lc_error *e) {
  unsigned char bytes[PS_FRAME_BYTES] = {0};
  int rc;
  if (!out || !out->writer || out->failed || out->frame_length || !length ||
      out->tail != out->start ||
      length > UINT64_MAX - PS_FRAME_BYTES - out->start)
    return record_error(e, "batch frame requires a new bounded append");
  rc = ps_stream_write(out->writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK) {
    out->tail += sizeof(bytes);
    out->frame_length = length;
  }
  return rc;
}
static int frame_finish(ps_record_writer *out, lc_error *e) {
  lc_pouch_record_header header;
  unsigned char bytes[PS_FRAME_BYTES];
  int rc = LC_OK;
  if (!out->frame_length)
    return LC_OK;
  if (out->tail - out->start - PS_FRAME_BYTES != out->frame_length)
    return record_error(e, "batch frame differs from its declared span");
  if (out->frame_dirty)
    rc = ps_stream_writer_crc(out->writer, PS_FRAME_BYTES, out->frame_length,
                              &out->frame_crc, e);
  if (rc != LC_OK)
    return rc;
  memset(&header, 0, sizeof(header));
  header.type = PS_FRAME_TYPE;
  header.payload_len = out->frame_length;
  header.payload_crc = out->frame_crc;
  lc_pouch_record_header_encode(&header, bytes);
  return ps_stream_write_at(out->writer, 0, bytes, sizeof(bytes), e);
}
int ps_record_commit(ps_record_writer *out, enum ps_commit_mode mode,
                     ps_commit_publish_fn publish, void *context, lc_error *e) {
  int rc;
  if (!out || !out->writer || out->failed)
    return record_error(e, "canonical append is not open");
  if (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC)
    return record_error(e, "invalid canonical commit mode");
  rc = frame_finish(out, e);
  if (rc == LC_OK)
    rc = ps_stream_commit(out->writer, mode, publish, context, e);
  ps_record_abort(out);
  return rc;
}
static int append(void *context, const struct iovec *parts, int count,
                  lc_error *e) {
  ps_record_writer *out = context;
  int i, rc = LC_OK;
  uint64_t end;
  size_t at, n;
  if (!out || !out->writer || out->failed)
    return record_error(e, "canonical append is closed");
  if (out->limit) {
    end = out->tail;
    if (end > out->limit) {
      out->failed = 1;
      return record_error(
          e, "canonical append exceeds its declared complete span");
    }
    for (i = 0; i < count; ++i) {
      if (parts[i].iov_len > out->limit - end) {
        out->failed = 1;
        return record_error(
            e, "canonical append exceeds its declared complete span");
      }
      end += parts[i].iov_len;
    }
  }
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (parts[i].iov_len > UINT64_MAX - out->tail)
      return record_error(e, "canonical append exceeds logical range");
    rc = ps_stream_write(out->writer, parts[i].iov_base, parts[i].iov_len, e);
    if (rc == LC_OK) {
      if (out->frame_length && !out->frame_dirty)
        for (at = 0; at < parts[i].iov_len; at += n) {
          n = parts[i].iov_len - at;
          if (n > PS_CHUNK)
            n = PS_CHUNK;
          out->frame_crc = (unsigned long)crc32(
              (uLong)out->frame_crc,
              (const unsigned char *)parts[i].iov_base + at, (uInt)n);
        }
      out->tail += parts[i].iov_len;
    }
  }
  return rc;
}
static int patch(void *context, uint64_t offset, const struct iovec *parts,
                 int count, lc_error *e) {
  ps_record_writer *out = context;
  uint64_t end = offset;
  int i, rc = LC_OK;
  if (!out || !out->writer || out->failed ||
      offset < out->start + (out->frame_length ? PS_FRAME_BYTES : 0) ||
      offset > out->tail)
    return record_error(e, "canonical patch outside private append");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > out->tail - end)
      return record_error(e, "canonical patch exceeds private append");
    end += parts[i].iov_len;
  }
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (out->frame_length && parts[i].iov_len)
      out->frame_dirty = 1;
    rc = ps_stream_write_at(out->writer, offset - out->start, parts[i].iov_base,
                            parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
static int tail(void *context, uint64_t *value, lc_error *e) {
  ps_record_writer *out = context;
  if (!out || !out->writer)
    return record_error(e, "canonical append is closed");
  *value = out->tail;
  return LC_OK;
}
static void discard(void *context, uint64_t offset) {
  ps_record_writer *out = context;
  (void)offset;
  /* The physical append is the private failure domain, even when it contains
   * several canonical records. Existing committed bytes are never truncated. */
  ps_record_abort(out);
}
lc_pouch_record_io ps_record_output(ps_record_writer *out) {
  static const lc_pouch_record_io_ops ops = {NULL, append, patch, tail,
                                             discard};
  lc_pouch_record_io io;
  io.ops = &ops;
  io.context = out;
  return io;
}
static int read_parts(void *context, uint64_t offset, const struct iovec *parts,
                      int count, lc_error *e) {
  int i, rc = LC_OK;
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = ps_stream_read_at(context, offset, parts[i].iov_base, parts[i].iov_len,
                           e);
    offset += parts[i].iov_len;
  }
  return rc;
}
lc_pouch_record_io ps_record_input(ps_stream_reader *reader) {
  static const lc_pouch_record_io_ops ops = {read_parts, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = &ops;
  io.context = reader;
  return io;
}
static int window_parts(void *context, uint64_t offset,
                        const struct iovec *parts, int count, lc_error *e) {
  int i, rc = LC_OK;
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = ps_stream_window_read(context, offset, parts[i].iov_base,
                               parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
lc_pouch_record_io ps_record_window_input(ps_stream_window *window) {
  static const lc_pouch_record_io_ops ops = {window_parts, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = &ops;
  io.context = window;
  return io;
}
static int recovery_parts(void *context, uint64_t offset,
                          const struct iovec *parts, int count, lc_error *e) {
  int i, rc = LC_OK;
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = ps_stream_recovery_read(context, offset, parts[i].iov_base,
                                 parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
lc_pouch_record_io ps_record_recovery_input(ps_stream *stream) {
  static const lc_pouch_record_io_ops ops = {recovery_parts, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = &ops;
  io.context = stream;
  return io;
}
