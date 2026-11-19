#include "lc_api_internal.h"
#include "lc_collection_record_recode.h"
#include "lc_collection_unit_recode.h"
#include "lc_pouch_record.h"
#include <string.h>
#include <zlib.h>

struct lc_collection_stream_recode {
  lc_allocator allocator;
  lc_pouch_crypto *crypto;
  lc_collection_unit_recode *codec;
  lc_collection_chunk_reader *input;
  lc_collection_recode_output_fn output;
  void *context;
  lc_collection_chunks chunks;
  lc_collection_recode_stream result;
  lc_u64 plain;
  int finished, failed;
};
static int failure(lc_error *e, int code, const char *text) {
  return lc_error_set(e, code, 0, text, NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}
static int write_index(void *context, unsigned int level, const void *tuples,
                       size_t bytes, lc_collection_chunk_ref *ref,
                       lc_error *e) {
  lc_collection_stream_recode *work = context;
  lc_pouch_record_io output = {0};
  lc_u64 segment = 0;
  int rc = work->output(work->context, 40U + bytes, 1, &output, &segment, e);
  if (rc == LC_OK && !segment)
    rc = failure(e, LC_ERR_INVALID, "converted index has no segment identity");
  return rc == LC_OK ? lc_collection_chunks_write_index(output, segment, level,
                                                        tuples, bytes, ref, e)
                     : rc;
}
void lc_collection_stream_recode_close(lc_collection_stream_recode *work) {
  if (!work)
    return;
  lc_collection_chunks_close(&work->chunks);
  lc_collection_unit_recode_close(work->codec);
  lc_free_with_allocator(&work->allocator, work);
}
int lc_collection_stream_recode_open(
    const lc_allocator *allocator, lc_pouch_crypto *crypto,
    lc_collection_chunk_reader *input, lc_collection_recode_output_fn output,
    void *context, lc_collection_stream_recode **out, lc_error *e) {
  lc_collection_stream_recode *work;
  int rc;
  if (out)
    *out = NULL;
  if (!out || !input || !input->record || !input->record->stream_extents ||
      !input->initialized || input->started || !output)
    return failure(e, LC_ERR_INVALID,
                   "stream conversion requires an unused unit iterator");
  work = lc_calloc_with_allocator(allocator, 1, sizeof(*work));
  if (!work)
    return failure(e, LC_ERR_NOMEM, "cannot allocate stream conversion owner");
  if (allocator)
    work->allocator = *allocator;
  work->crypto = crypto;
  work->input = input;
  work->output = output;
  work->context = context;
  work->chunks.append_index = write_index;
  work->chunks.index_context = work;
  rc = lc_collection_unit_recode_open(allocator, crypto, &work->codec, e);
  if (rc == LC_OK)
    *out = work;
  else
    lc_collection_stream_recode_close(work);
  return rc;
}
int lc_collection_stream_recode_step(lc_collection_stream_recode *work,
                                     size_t limit, size_t *visited,
                                     int *finished,
                                     lc_collection_recode_stream *out,
                                     lc_error *e) {
  lc_collection_chunk_ref ref, converted;
  lc_collection_unit unit;
  lc_pouch_record_io input = {0}, output = {0};
  const lc_pouch_collection_record *record;
  lc_u64 bound = 0, offset = 0, segment = 0;
  size_t count = 0;
  lc_error local;
  int rc = LC_OK, found;
  if (!work || work->failed || !limit || !visited || !finished || !out)
    return failure(e, LC_ERR_INVALID,
                   "stream conversion requires open owner and positive bound");
  lc_error_init(&local);
  if (!e)
    e = &local;
  record = work->input->record;
  while (!work->finished && count < limit &&
         work->result.units < record->stream_extents) {
    found = 0;
    rc = lc_collection_chunks_next_unit(work->input, &found, &input, &ref,
                                        &unit, e);
    if (rc == LC_OK && !found)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "converted stream ended before its unit count");
    if (rc == LC_OK)
      rc = lc_collection_unit_recode_bound(work->crypto, unit.plain, &bound, e);
    if (rc == LC_OK)
      rc = work->output(work->context, bound, 0, &output, &segment, e);
    if (rc == LC_OK && !segment)
      rc = failure(e, LC_ERR_INVALID, "converted unit has no segment identity");
    if (rc == LC_OK)
      rc = lc_collection_unit_recode_run(work->codec, input, ref.offset,
                                         ref.bytes, output, &offset, &unit, e);
    if (rc == LC_OK && (unit.ordinal != work->result.units ||
                        unit.plain > record->bytes - work->plain ||
                        unit.stored > LC_U64_MAX - work->result.stored))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "converted stream unit changed its logical order");
    if (rc != LC_OK)
      break;
    if (!work->result.units)
      work->result.first = unit;
    converted.segment = segment;
    converted.offset = offset;
    converted.bytes = unit.stored;
    rc = lc_collection_chunks_link(NULL, &work->chunks, &converted, e);
    if (rc != LC_OK)
      break;
    work->plain += unit.plain;
    work->result.stored += unit.stored;
    ++work->result.units;
    ++count;
  }
  if (rc == LC_OK && !work->finished &&
      work->result.units == record->stream_extents) {
    found = 0;
    rc = lc_collection_chunks_next_unit(work->input, &found, &input, &ref,
                                        &unit, e);
    if (rc == LC_OK && (found || work->plain != record->bytes))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "converted stream differs from its published size");
    if (rc == LC_OK)
      rc = lc_collection_chunks_finish(NULL, &work->chunks, e);
    if (rc == LC_OK) {
      work->result.root = work->chunks.root;
      work->result.height = work->chunks.height;
      work->finished = 1;
    }
  }
  if (rc == LC_OK) {
    *visited = count;
    *finished = work->finished;
    if (work->finished)
      *out = work->result;
  } else
    work->failed = 1;
  lc_error_cleanup(&local);
  return rc;
}

static lc_u64 index_number(const unsigned char *p) {
  lc_u64 n = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    n |= (lc_u64)p[i] << (8U * i);
  return n;
}
static void index_encode(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
int lc_collection_index_recode(lc_pouch_record_io input, lc_u64 offset,
                               lc_u64 bytes, unsigned int height,
                               lc_collection_recode_resolve_fn resolve,
                               void *context, lc_pouch_record_io output,
                               lc_u64 segment, lc_collection_recode_tree *out,
                               lc_error *e) {
  unsigned char prefix[40], tuples[LC_COLLECTION_CHUNK_PAGE];
  lc_pouch_record_header header;
  lc_collection_recode_tree result, child;
  lc_collection_chunk_ref source;
  struct iovec part;
  lc_u64 tail;
  size_t at;
  int rc;
  if (!resolve || !out || !segment || !height ||
      height >= LC_COLLECTION_CHUNK_LEVELS || !bytes ||
      bytes > sizeof(tuples) || bytes % LC_COLLECTION_CHUNK_TUPLE ||
      !output.ops || !output.ops->discard)
    return lc_collection_failure(e, LC_ERR_INVALID, NULL,
                                 "index conversion requires bounded placement");
  memset(&result, 0, sizeof(result));
  part.iov_base = prefix;
  part.iov_len = sizeof(prefix);
  rc = lc_pouch_record_io_read(input, offset, &part, 1, e);
  if (rc == LC_OK)
    rc = lc_pouch_record_header_decode(prefix, &header, e);
  part.iov_base = tuples;
  part.iov_len = (size_t)bytes;
  if (rc == LC_OK)
    rc = lc_pouch_record_io_read(input, offset + 40U, &part, 1, e);
  if (rc == LC_OK &&
      (header.type != LC_POUCH_COLLECTION_NODE || header.flags ||
       header.key_len || header.meta_len != 8U || header.payload_len != bytes ||
       index_number(prefix + 32U) != height - 1U ||
       header.payload_crc != (unsigned long)crc32(0L, tuples, (uInt)bytes)))
    rc = lc_collection_failure(
        e, LC_ERR_PROTOCOL, "collection_corrupt",
        "converted index source framing or CRC is invalid");
  for (at = 0U; rc == LC_OK && at < bytes; at += LC_COLLECTION_CHUNK_TUPLE) {
    source.segment = index_number(tuples + at);
    source.offset = index_number(tuples + at + 8U);
    source.bytes = index_number(tuples + at + 16U);
    memset(&child, 0, sizeof(child));
    rc = resolve(context, &source, height - 1U, &child, e);
    if (rc != LC_OK)
      break;
    if (!child.root.segment || !child.root.bytes || !child.first.segment ||
        !child.stored || !child.units ||
        (height == 1U &&
         (child.units != 1U || child.root.bytes != child.stored)) ||
        (height > 1U && child.root.bytes != source.bytes) ||
        child.stored > LC_U64_MAX - result.stored ||
        child.units > LC_U64_MAX - result.units)
      return lc_collection_failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                                   "converted index child facts are invalid");
    if (!at)
      result.first = child.first;
    result.stored += child.stored;
    result.units += child.units;
    index_encode(tuples + at, child.root.segment);
    index_encode(tuples + at + 8U, child.root.offset);
    index_encode(tuples + at + 16U, child.root.bytes);
  }
  if (rc != LC_OK)
    return rc;
  rc = lc_pouch_record_io_tail(output, &tail, e);
  if (rc != LC_OK)
    return rc;
  rc = lc_collection_chunks_write_index(output, segment, height - 1U, tuples,
                                        (size_t)bytes, &result.root, e);
  if (rc == LC_OK)
    *out = result;
  else
    output.ops->discard(output.context, tail);
  return rc;
}
