#include "checkpoint_record.h"
#include "lc_api_internal.h"
#include "record_io.h"
#include "space_internal.h"
#include <string.h>

typedef struct metadata_output {
  ps_checkpoint_record *record;
  size_t offset;
} metadata_output;
static int invalid(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL, NULL, "pouch");
}
static int metadata_append(void *context, const struct iovec *parts, int count,
                           lc_error *e) {
  metadata_output *output = context;
  int i;
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > output->record->length - output->offset)
      return invalid(e, LC_ERR_INVALID,
                     "canonical metadata exceeds its measured span");
    if (parts[i].iov_len)
      memcpy(output->record->bytes + output->offset, parts[i].iov_base,
             parts[i].iov_len);
    output->offset += parts[i].iov_len;
  }
  return LC_OK;
}
static int metadata_tail(void *context, uint64_t *tail, lc_error *e) {
  metadata_output *output = context;
  (void)e;
  *tail = output->offset;
  return LC_OK;
}
static void metadata_discard(void *context, uint64_t offset) {
  metadata_output *output = context;
  output->offset = (size_t)offset;
}
void ps_checkpoint_record_close(ps_checkpoint_record *record) {
  if (!record)
    return;
  lc_free_with_allocator(&record->allocator, record->bytes);
  memset(record, 0, sizeof(*record));
}
int ps_checkpoint_record_encode(const lc_allocator *allocator,
                                const lc_pouch_state_entry *entry,
                                ps_checkpoint_record *out, lc_error *e) {
  lc_allocator fallback = {0};
  metadata_output output;
  lc_pouch_record_io io;
  uint64_t bytes = 0;
  int rc;
  static const lc_pouch_record_io_ops ops = {NULL, metadata_append, NULL,
                                             metadata_tail, metadata_discard};
  if (!entry || !out)
    return invalid(e, LC_ERR_INVALID,
                   "canonical metadata encoding requires entry and output");
  memset(out, 0, sizeof(*out));
  if (!allocator)
    allocator = &fallback;
  out->allocator = *allocator;
  rc = lc_pouch_state_record_measure_snapshot(allocator, NULL, entry, 0, &bytes,
                                              e);
  if (rc == LC_OK && bytes > (uint64_t)(size_t)-1)
    rc = invalid(e, LC_ERR_INVALID,
                 "canonical metadata exceeds local memory bounds");
  if (rc == LC_OK) {
    out->length = (size_t)bytes;
    out->bytes = lc_alloc_with_allocator(allocator, out->length);
    if (!out->bytes)
      rc = invalid(e, LC_ERR_NOMEM, "cannot allocate canonical metadata");
  }
  if (rc == LC_OK) {
    output.record = out;
    output.offset = 0;
    io.ops = &ops;
    io.context = &output;
    rc = lc_pouch_state_record_write_snapshot(allocator, io, NULL, entry, NULL,
                                              NULL, 0, e);
    if (rc == LC_OK && output.offset != out->length)
      rc = invalid(e, LC_ERR_PROTOCOL,
                   "canonical metadata differs from its measured span");
  }
  if (rc != LC_OK)
    ps_checkpoint_record_close(out);
  else
    out->deletion_bytes = lc_pouch_state_entry_delete_bytes(entry);
  return rc;
}
int ps_checkpoint_record_write(ps_space *space, ps_budget *budget, ps_key key,
                               const ps_checkpoint_record *record,
                               ps_writer **out, lc_error *e) {
  ps_writer *writer = NULL;
  int rc;
  if (out)
    *out = NULL;
  if (!record || !record->bytes || !record->length || !out ||
      (key.kind != PS_KEY_METADATA && key.kind != PS_KEY_DECISION))
    return invalid(e, LC_ERR_INVALID,
                   "checkpoint output requires canonical slot and bytes");
  rc = ps_begin(space, budget, key, &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, record->bytes, record->length, e);
  if (rc == LC_OK && key.kind == PS_KEY_METADATA)
    rc = ps_writer_key_deletion(writer, record->deletion_bytes, e);
  if (rc == LC_OK)
    *out = writer;
  else
    ps_writer_close(writer);
  return rc;
}
int ps_checkpoint_record_read(ps_reader *reader, const lc_allocator *allocator,
                              const char *leaf, lc_pouch_state_entry *out,
                              lc_error *e) {
  lc_allocator fallback = {0};
  const unsigned char *span;
  uint64_t next = 0;
  int found = 0, truncated = 0, rc;
  if (!reader || !leaf || !out)
    return invalid(e, LC_ERR_INVALID,
                   "checkpoint input requires captured value and output");
  if (!allocator)
    allocator = &fallback;
  rc = ps_internal_checkpoint_span(reader, &span, e);
  if (rc != LC_OK)
    return rc;
  if (span)
    return lc_pouch_state_record_decode_span(span, (size_t)ps_read_size(reader),
                                             allocator, leaf, out, e);
  rc = lc_pouch_state_record_read(ps_record_value_input(reader), allocator,
                                  leaf, 0, ps_read_size(reader), 0, 0, NULL,
                                  out, &next, &found, &truncated, e);
  if (rc == LC_OK && (!found || truncated || next != ps_read_size(reader)))
    rc = invalid(e, LC_ERR_PROTOCOL,
                 "checkpoint metadata is not one complete canonical record");
  return rc;
}
