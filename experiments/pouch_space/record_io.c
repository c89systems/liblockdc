#include "record_io.h"
#include "lc_api_internal.h"
#include <string.h>

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
int ps_record_reserve(ps_record_writer *out, uint64_t bytes, lc_error *e) {
  if (!out || !out->writer)
    return record_error(e, "canonical append is not open");
  return ps_stream_reserve(out->writer, bytes, e);
}
void ps_record_abort(ps_record_writer *out) {
  if (out) {
    ps_stream_writer_close(out->writer);
    out->writer = NULL;
  }
}
int ps_record_commit(ps_record_writer *out, ps_commit_publish_fn publish,
                     void *context, lc_error *e) {
  int rc;
  if (!out || !out->writer)
    return record_error(e, "canonical append is not open");
  rc = ps_stream_commit(out->writer, publish, context, e);
  ps_record_abort(out);
  return rc;
}
static int append(void *context, const struct iovec *parts, int count,
                  lc_error *e) {
  ps_record_writer *out = context;
  int i, rc = LC_OK;
  if (!out || !out->writer)
    return record_error(e, "canonical append is closed");
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (parts[i].iov_len > UINT64_MAX - out->tail)
      return record_error(e, "canonical append exceeds logical range");
    rc = ps_stream_write(out->writer, parts[i].iov_base, parts[i].iov_len, e);
    if (rc == LC_OK)
      out->tail += parts[i].iov_len;
  }
  return rc;
}
static int patch(void *context, uint64_t offset, const struct iovec *parts,
                 int count, lc_error *e) {
  ps_record_writer *out = context;
  uint64_t end = offset;
  int i, rc = LC_OK;
  if (!out || !out->writer || offset < out->start || offset > out->tail)
    return record_error(e, "canonical patch outside private append");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > out->tail - end)
      return record_error(e, "canonical patch exceeds private append");
    end += parts[i].iov_len;
  }
  for (i = 0; i < count && rc == LC_OK; ++i) {
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
