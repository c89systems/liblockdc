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
  if (!out || !out->writer || out->failed || out->framed ||
      out->tail != out->start || out->start > UINT64_MAX - PS_FRAME_BYTES ||
      length > UINT64_MAX - PS_FRAME_BYTES - out->start)
    return record_error(e, "batch frame requires a new append");
  rc = ps_stream_write(out->writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK) {
    out->tail += sizeof(bytes);
    out->framed = 1;
    out->frame_length = length;
  }
  return rc;
}
static int streaming_frame_crc(ps_record_writer *out, uint64_t length,
                               lc_error *e) {
  lc_pouch_record_header h;
  unsigned char bytes[LC_POUCH_RECORD_HEADER_BYTES];
  unsigned long crc = 0, prefix_crc;
  uint64_t offset = PS_FRAME_BYTES, prefix, end = offset + length;
  int rc;

  while (offset < end) {
    if (end - offset < sizeof(bytes))
      return record_error(e, "streamed frame has an incomplete prefix");
    rc = ps_stream_writer_read(out->writer, offset, bytes, sizeof(bytes), e);
    if (rc == LC_OK)
      rc = lc_pouch_record_header_decode(bytes, &h, e);
    if (rc != LC_OK)
      return rc;
    prefix = sizeof(bytes) + (uint64_t)h.key_len + h.meta_len;
    if (h.flags || h.type == PS_FRAME_TYPE || prefix > end - offset ||
        h.payload_len > end - offset - prefix || h.payload_len > INT64_MAX)
      return record_error(e, "streamed frame has an invalid record span");
    rc = ps_stream_writer_crc(out->writer, offset, prefix, &prefix_crc, e);
    if (rc != LC_OK)
      return rc;
    crc = (unsigned long)crc32_combine(crc, prefix_crc, (z_off_t)prefix);
    if (h.payload_len)
      crc = (unsigned long)crc32_combine(crc, h.payload_crc,
                                         (z_off_t)h.payload_len);
    offset += prefix + h.payload_len;
  }
  out->frame_crc = crc;
  return LC_OK;
}
static int frame_finish(ps_record_writer *out, lc_error *e) {
  lc_pouch_record_header header;
  unsigned char bytes[PS_FRAME_BYTES];
  uint64_t length;
  int rc = LC_OK;
  if (!out->framed)
    return LC_OK;
  length = out->tail - out->start - PS_FRAME_BYTES;
  if (!length || (out->frame_length && length != out->frame_length))
    return record_error(e, "batch frame differs from its declared span");
  if (!out->frame_length)
    rc = streaming_frame_crc(out, length, e);
  else if (out->frame_dirty)
    rc = ps_stream_writer_crc(out->writer, PS_FRAME_BYTES, length,
                              &out->frame_crc, e);
  if (rc != LC_OK)
    return rc;
  memset(&header, 0, sizeof(header));
  header.type = PS_FRAME_TYPE;
  header.payload_len = length;
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
  if (out->admit && out->tail == out->record_end) {
    lc_pouch_record_header header;
    if (count < 3 || parts[0].iov_len != LC_POUCH_RECORD_HEADER_BYTES)
      return record_error(e, "canonical admission requires a complete prefix");
    rc = lc_pouch_record_header_decode(parts[0].iov_base, &header, e);
    if (rc == LC_OK &&
        ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) ||
         header.key_len != parts[1].iov_len ||
         header.meta_len != parts[2].iov_len ||
         out->tail > (uint64_t)INT64_MAX - LC_POUCH_RECORD_HEADER_BYTES ||
         header.key_len >
             (uint64_t)INT64_MAX - out->tail - LC_POUCH_RECORD_HEADER_BYTES ||
         header.meta_len > (uint64_t)INT64_MAX - out->tail -
                               LC_POUCH_RECORD_HEADER_BYTES - header.key_len ||
         header.payload_len > (uint64_t)INT64_MAX - out->tail -
                                  LC_POUCH_RECORD_HEADER_BYTES -
                                  header.key_len - header.meta_len))
      rc = record_error(e, "canonical admission prefix exceeds its bounds");
    if (rc == LC_OK)
      rc = out->admit(out->admission_context, &header, parts[1].iov_base,
                      parts[2].iov_base, e);
    if (rc != LC_OK) {
      out->failed = 1;
      return rc;
    }
    out->record_start = out->tail;
    out->record_end = header.flags & LC_POUCH_RECORD_FLAG_PENDING
                          ? UINT64_MAX
                          : out->tail + LC_POUCH_RECORD_HEADER_BYTES +
                                header.key_len + header.meta_len +
                                header.payload_len;
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
  uint64_t end = offset, record_end = 0;
  int i, rc = LC_OK;
  if (!out || !out->writer || out->failed ||
      offset < out->start + (out->framed ? PS_FRAME_BYTES : 0) ||
      offset > out->tail)
    return record_error(e, "canonical patch outside private append");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > out->tail - end)
      return record_error(e, "canonical patch exceeds private append");
    end += parts[i].iov_len;
  }
  if (out->admit && offset == out->record_start && count == 1 &&
      parts[0].iov_len == LC_POUCH_RECORD_HEADER_BYTES) {
    lc_pouch_record_header header;
    rc = lc_pouch_record_header_decode(parts[0].iov_base, &header, e);
    if (rc == LC_OK &&
        ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) ||
         LC_POUCH_RECORD_HEADER_BYTES > out->tail - offset ||
         header.key_len > out->tail - offset - LC_POUCH_RECORD_HEADER_BYTES ||
         header.meta_len > out->tail - offset - LC_POUCH_RECORD_HEADER_BYTES -
                               header.key_len ||
         header.payload_len > out->tail - offset -
                                  LC_POUCH_RECORD_HEADER_BYTES -
                                  header.key_len - header.meta_len))
      rc = record_error(e, "canonical fixup exceeds its private record");
    if (rc != LC_OK)
      return rc;
    record_end = header.flags & LC_POUCH_RECORD_FLAG_PENDING
                     ? UINT64_MAX
                     : offset + LC_POUCH_RECORD_HEADER_BYTES + header.key_len +
                           header.meta_len + header.payload_len;
  }
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (out->frame_length && parts[i].iov_len)
      out->frame_dirty = 1;
    rc = ps_stream_write_at(out->writer, offset - out->start, parts[i].iov_base,
                            parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  if (rc == LC_OK && record_end)
    out->record_end = record_end;
  return rc;
}
int ps_record_admission_set(ps_record_writer *out, ps_record_admit_fn admit,
                            void *context, lc_error *e) {
  if (!out || !out->writer || out->failed || !admit || out->admit ||
      out->tail != out->start + (out->framed ? PS_FRAME_BYTES : 0))
    return record_error(e, "canonical admission requires a new private append");
  out->admit = admit;
  out->admission_context = context;
  out->record_start = out->record_end = out->tail;
  return LC_OK;
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
static int private_read(void *context, uint64_t offset,
                        const struct iovec *parts, int count, lc_error *e) {
  ps_record_writer *out = context;
  uint64_t end = offset;
  int i, rc = LC_OK;
  if (!out || !out->writer || out->failed || offset < out->start ||
      offset > out->tail)
    return record_error(e, "canonical read outside private append");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > out->tail - end)
      return record_error(e, "canonical read exceeds private append");
    end += parts[i].iov_len;
  }
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = ps_stream_writer_read(out->writer, offset - out->start,
                               parts[i].iov_base, parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
lc_pouch_record_io ps_record_output(ps_record_writer *out) {
  static const lc_pouch_record_io_ops ops = {private_read, append, patch, tail,
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
static int value_parts(void *context, uint64_t offset,
                       const struct iovec *parts, int count, lc_error *e) {
  int i, rc = LC_OK;
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = ps_read_at(context, offset, parts[i].iov_base, parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
lc_pouch_record_io ps_record_value_input(ps_reader *reader) {
  static const lc_pouch_record_io_ops ops = {value_parts, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = reader ? &ops : NULL;
  io.context = reader;
  return io;
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
