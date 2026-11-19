#include "lc_collection_unit_recode.h"
#include "lc_api_internal.h"
#include <string.h>
#include <zlib.h>

#define LC_CUR_CHUNK 65536U
struct lc_collection_unit_recode {
  lc_allocator allocator;
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_reader *reader;
  lc_pouch_crypto_writer *writer;
  lc_pouch_record_io input, output;
  lc_source encoded;
  lc_sink destination;
  unsigned char *buffer;
  char context[256];
  lc_u64 start, at, remaining, stored, emitted, bound;
  unsigned long crc;
};
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}
int lc_collection_unit_recode_bound(lc_pouch_crypto *crypto, lc_u64 plain,
                                    lc_u64 *out, lc_error *e) {
  uint64_t encoded;
  if (!out || !plain || plain > LC_COLLECTION_UNIT_MAX)
    return failure(e, LC_ERR_INVALID,
                   "conversion unit exceeds plaintext bound");
  encoded = lc_pouch_crypto_stream_bound(crypto, plain, 1, NULL);
  if (encoded > (lc_u64)LC_I64_MAX - LC_COLLECTION_UNIT_HEADER)
    return failure(e, LC_ERR_INVALID, "conversion unit exceeds encoded bound");
  *out = encoded + LC_COLLECTION_UNIT_HEADER;
  return LC_OK;
}
static size_t read_encoded(lc_source *source, void *bytes, size_t count,
                           lc_error *e) {
  lc_collection_unit_recode *work = source->impl;
  struct iovec part;
  if ((lc_u64)count > work->remaining)
    count = (size_t)work->remaining;
  part.iov_base = bytes;
  part.iov_len = count;
  if (count &&
      lc_pouch_record_io_read(work->input, work->at, &part, 1, e) != LC_OK)
    return 0;
  work->crc = (unsigned long)crc32((uLong)work->crc, bytes, (uInt)count);
  work->at += count;
  work->remaining -= count;
  return count;
}
static int reset_encoded(lc_source *source, lc_error *e) {
  lc_collection_unit_recode *work = source->impl;
  (void)e;
  work->at = work->start;
  work->remaining = work->stored;
  work->crc = (unsigned long)crc32(0L, Z_NULL, 0);
  return LC_OK;
}
static int write_encoded(lc_sink *sink, const void *bytes, size_t count,
                         lc_error *e) {
  lc_collection_unit_recode *work = sink->impl;
  struct iovec part;
  int rc;
  if ((lc_u64)count > work->bound - work->emitted) {
    failure(e, LC_ERR_PROTOCOL, "converted unit exceeded admitted output");
    return 0;
  }
  part.iov_base = (void *)bytes;
  part.iov_len = count;
  rc = lc_pouch_record_io_append(work->output, &part, 1, e);
  if (rc == LC_OK)
    work->emitted += count;
  return rc == LC_OK;
}
void lc_collection_unit_recode_close(lc_collection_unit_recode *work) {
  if (!work)
    return;
  lc_pouch_crypto_reader_close(work->reader);
  lc_pouch_crypto_writer_close(work->writer);
  lc_free_with_allocator(&work->allocator, work->buffer);
  lc_free_with_allocator(&work->allocator, work);
}
int lc_collection_unit_recode_open(const lc_allocator *allocator,
                                   lc_pouch_crypto *crypto,
                                   lc_collection_unit_recode **out,
                                   lc_error *e) {
  lc_collection_unit_recode *work;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "unit conversion requires output");
  work = lc_calloc_with_allocator(allocator, 1, sizeof(*work));
  if (!work)
    return failure(e, LC_ERR_NOMEM, "cannot allocate unit converter");
  if (allocator)
    work->allocator = *allocator;
  work->crypto = crypto;
  work->encoded.impl = work;
  work->encoded.read = read_encoded;
  work->encoded.reset = reset_encoded;
  work->destination.impl = work;
  work->destination.write = write_encoded;
  work->buffer = lc_alloc_with_allocator(allocator, LC_CUR_CHUNK);
  rc = work->buffer ? lc_pouch_crypto_reader_open(crypto, &work->reader, e)
                    : failure(e, LC_ERR_NOMEM, "cannot allocate unit scratch");
  if (rc == LC_OK)
    *out = work;
  else
    lc_collection_unit_recode_close(work);
  return rc;
}
int lc_collection_unit_recode_run(lc_collection_unit_recode *work,
                                  lc_pouch_record_io input, lc_u64 at,
                                  lc_u64 stored, lc_pouch_record_io output,
                                  lc_u64 *offset, lc_collection_unit *out,
                                  lc_error *e) {
  lc_collection_unit unit;
  lc_pouch_record_header header;
  unsigned char prefix[LC_COLLECTION_UNIT_HEADER];
  lc_source *source = NULL;
  lc_sink *sink;
  struct iovec part;
  const char *descriptor = NULL;
  lc_u64 start = 0, bound = 0, decoded = 0, plain = 0, encoded = 0;
  unsigned long crc;
  size_t got;
  lc_error local;
  int rc, attempted = 0;
  if (!work || !offset || !out || !input.ops || !input.ops->read ||
      !output.ops || !output.ops->append || !output.ops->patch ||
      !output.ops->tail || !output.ops->discard ||
      (input.ops == output.ops && input.context == output.context) ||
      at > (lc_u64)LC_I64_MAX - LC_COLLECTION_UNIT_HEADER || !stored ||
      stored > (lc_u64)LC_I64_MAX - LC_COLLECTION_UNIT_HEADER - at)
    return failure(e, LC_ERR_INVALID,
                   "unit conversion requires distinct byte owners");
  lc_error_init(&local);
  if (!e)
    e = &local;
  rc = lc_collection_unit_read(input, at, stored, &unit, e);
  if (rc == LC_OK)
    rc = lc_collection_unit_recode_bound(work->crypto, unit.plain, &bound, e);
  if (rc == LC_OK)
    rc = lc_pouch_record_io_tail(output, &start, e);
  if (rc == LC_OK && start > (lc_u64)LC_I64_MAX - bound)
    rc = failure(e, LC_ERR_INVALID,
                 "converted unit exceeds output offset bound");
  if (rc != LC_OK)
    goto done;
  work->input = input;
  work->output = output;
  work->start = at + LC_COLLECTION_UNIT_HEADER;
  work->stored = stored;
  work->emitted = 0;
  work->bound = bound - LC_COLLECTION_UNIT_HEADER;
  strcpy(work->context, unit.context);
  (void)reset_encoded(&work->encoded, NULL);
  rc = lc_pouch_crypto_reader_bind_source(work->reader, unit.context,
                                          unit.context, &work->encoded, stored,
                                          unit.descriptor, &source, e);
  if (rc == LC_OK) {
    if (!work->writer)
      rc = lc_pouch_crypto_writer_open_sink(
          work->crypto, work->context, &work->destination, &work->writer, e);
    else
      rc = lc_pouch_crypto_writer_restart(work->writer, work->context, e);
  }
  memset(&header, 0, sizeof(header));
  memset(prefix, 0, sizeof(prefix));
  header.type = LC_POUCH_COLLECTION_CHUNK;
  header.meta_len = LC_COLLECTION_UNIT_META;
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  lc_pouch_record_header_encode(&header, prefix);
  part.iov_base = prefix;
  part.iov_len = sizeof(prefix);
  if (rc == LC_OK) {
    attempted = 1;
    rc = lc_pouch_record_io_append(output, &part, 1, e);
  }
  sink = lc_pouch_crypto_writer_sink(work->writer);
  while (rc == LC_OK) {
    got = source->read(source, work->buffer, LC_CUR_CHUNK, e);
    if (e->code != LC_OK) {
      rc = e->code;
      break;
    }
    if (!got)
      break;
    if (got > LC_CUR_CHUNK || (lc_u64)got > unit.plain - decoded) {
      rc = failure(e, LC_ERR_PROTOCOL,
                   "converted unit exceeds published plaintext");
      break;
    }
    decoded += got;
    if (!sink->write(sink, work->buffer, got, e))
      rc = e->code != LC_OK ? e->code : LC_ERR_TRANSPORT;
  }
  if (rc == LC_OK &&
      (decoded != unit.plain || work->remaining || work->crc != unit.crc))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "collection conversion unit integrity check failed");
  if (rc == LC_OK)
    rc = lc_pouch_crypto_writer_finish_borrowed(work->writer, &plain, &encoded,
                                                &crc, &descriptor, e);
  if (rc == LC_OK &&
      (plain != unit.plain || encoded != work->emitted ||
       (descriptor && strlen(descriptor) >= sizeof(unit.descriptor))))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "converted unit storage facts are inconsistent");
  if (rc == LC_OK) {
    unit.stored = encoded;
    unit.crc = crc;
    unit.descriptor[0] = 0;
    if (descriptor)
      strcpy(unit.descriptor, descriptor);
    rc = lc_collection_unit_write_meta(output, start, &unit, e);
  }
  if (rc == LC_OK) {
    *out = unit;
    *offset = start;
  } else if (attempted)
    output.ops->discard(output.context, start);
done:
  if (rc != LC_OK) {
    lc_pouch_crypto_writer_close(work->writer);
    work->writer = NULL;
  }
  lc_pouch_crypto_reader_clear(work->reader);
  memset(&work->input, 0, sizeof(work->input));
  memset(&work->output, 0, sizeof(work->output));
  lc_error_cleanup(&local);
  return rc;
}
