#include "lc_api_internal.h"
#include "lc_collection_record_recode.h"
#include "lc_collection_unit_recode.h"
#include <string.h>

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
