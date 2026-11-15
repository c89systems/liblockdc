#include "frame.h"
#include "lc_api_internal.h"
#include "space.h"
#include <string.h>
#include <zlib.h>

static int invalid(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_PROTOCOL, 0, message, NULL,
                      "pouch_namespace_corrupt", "pouch");
}
int ps_frame_read(lc_pouch_record_io io, uint64_t start, uint64_t end,
                  unsigned char **scratch, ps_frame *out, lc_error *e) {
  unsigned char bytes[PS_FRAME_BYTES];
  lc_pouch_record_header header;
  struct iovec part;
  uint64_t at;
  unsigned long crc = 0;
  size_t n;
  int rc;
  if (!scratch || !out)
    return lc_error_set(e, LC_ERR_INVALID, 0, "frame probe requires outputs",
                        NULL, NULL, "pouch");
  memset(out, 0, sizeof(*out));
  out->start = start;
  out->end = end;
  if (start > end || end - start < PS_FRAME_BYTES)
    return invalid(e, "namespace recovery unit header is incomplete");
  part.iov_base = bytes;
  part.iov_len = sizeof(bytes);
  rc = lc_pouch_record_io_read(io, start, &part, 1, e);
  if (rc == LC_OK)
    rc = lc_pouch_record_header_decode(bytes, &header, e);
  if (rc != LC_OK || header.type != PS_FRAME_TYPE)
    return rc;
  if (header.flags || header.key_len || header.meta_len ||
      !header.payload_len || header.payload_len > end - start - PS_FRAME_BYTES)
    return invalid(e, "namespace batch frame fields or span are invalid");
  if (!*scratch)
    *scratch = lc_alloc_with_allocator(NULL, PS_CHUNK);
  if (!*scratch)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate frame CRC buffer",
                        NULL, NULL, "pouch");
  out->start = start + PS_FRAME_BYTES;
  out->end = out->start + header.payload_len;
  for (at = out->start; at < out->end; at += n) {
    n = out->end - at < PS_CHUNK ? (size_t)(out->end - at) : PS_CHUNK;
    part.iov_base = *scratch;
    part.iov_len = n;
    rc = lc_pouch_record_io_read(io, at, &part, 1, e);
    if (rc != LC_OK)
      return rc;
    crc = (unsigned long)crc32((uLong)crc, *scratch, (uInt)n);
  }
  if (crc != header.payload_crc)
    return invalid(e, "namespace batch frame checksum mismatch");
  out->framed = 1;
  return LC_OK;
}
