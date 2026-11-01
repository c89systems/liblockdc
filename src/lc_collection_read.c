#include "lc_collection_internal.h"
#include "lc_collection_payload.h"
#include "lc_collection_query.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <lonejson.h>
#include <lql/lql.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#define LC_CR_TOKEN_BYTES 136U
#define LC_CR_CHUNK_BYTES 16384U
#define LC_CR_PROGRESS_PAGES 4U

typedef struct lc_collection_reader {
  lc_collection_call *call;
  const lc_collection_read_opts *opts;
  lc_collection_read_res *result;
  lc_sink *sink;
  lc_error *error;
  lc_collection_order order;
  lc_collection_order_iter iter;
  lc_pouch_collection_record record;
  lc_collection_payload payload;
  lc_collection_query_plan plan;
  int reader_fd;
  int record_fd;
  lc_u64 file_segment;
  lc_u64 file_end;
  lc_u64 upper;
  lc_u64 instant;
  /* Working live-input budget. The returned resume token records only the
   * accepted boundary, including any coalesced nonmatch progress. */
  lc_u64 remaining;
  unsigned char selection_hash[32];
  unsigned char namespace_hash[32];
  int cancelled;
  char proposed[LC_COLLECTION_CURSOR_BYTES];
} lc_collection_reader;

static void lc_cr_put(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)value;
    value >>= 8U;
  }
}

static lc_u64 lc_cr_get(const unsigned char *bytes) {
  lc_u64 value;
  unsigned int i;
  value = 0U;
  for (i = 8U; i > 0U; --i)
    value = (value << 8U) | bytes[i - 1U];
  return value;
}

static int lc_cr_selection(lc_collection_reader *reader,
                           const char *expression) {
  const lc_collection_read_opts *opts = reader->opts;
  unsigned char bytes[64];
  int rc;
  if ((opts->has_after != 0 && opts->has_after != 1) ||
      (opts->has_through != 0 && opts->has_through != 1) ||
      (opts->has_first_n != 0 && opts->has_first_n != 1) ||
      (opts->has_through && opts->has_after && opts->after > opts->through))
    return lc_collection_failure(
        reader->error, LC_ERR_INVALID, "selection_invalid",
        "collection selection flags or bounds are invalid");
  memset(bytes, 0, sizeof(bytes));
  rc = lc_collection_hash(expression, strlen(expression), bytes, reader->error);
  if (rc != LC_OK)
    return rc;
  bytes[32] = (unsigned char)(opts->has_after | (opts->has_through << 1) |
                              (opts->has_first_n << 2));
  /* Counted windows select before filtering. Reject development cursors that
   * used the former match-count meaning of first_n. */
  bytes[33] = (unsigned char)opts->has_first_n;
  lc_cr_put(bytes + 40U, opts->has_after ? opts->after : 0U);
  lc_cr_put(bytes + 48U, opts->has_through ? opts->through : 0U);
  lc_cr_put(bytes + 56U, opts->has_first_n ? opts->first_n : 0U);
  reader->remaining = opts->has_first_n ? opts->first_n : 0U;
  return lc_collection_hash(bytes, sizeof(bytes), reader->selection_hash,
                            reader->error);
}

static int lc_cr_cursor(lc_collection_reader *reader, lc_u64 after,
                        lc_u64 remaining, int checkpoint, char *out) {
  unsigned char bytes[LC_CR_TOKEN_BYTES];
  lonejson_error json_error;
  size_t needed;
  unsigned long crc;
  lonejson_status status;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, "LCCR001", 7U);
  memcpy(bytes + 8U, reader->call->collection->id, 20U);
  bytes[28] = checkpoint ? 2U : 1U;
  lc_cr_put(bytes + 32U, after);
  lc_cr_put(bytes + 40U, reader->upper);
  lc_cr_put(bytes + 48U, reader->instant);
  memcpy(bytes + 56U, reader->selection_hash, 32U);
  memcpy(bytes + 88U, reader->namespace_hash, 32U);
  lc_cr_put(bytes + 120U, remaining);
  crc = (unsigned long)crc32(0L, bytes, LC_CR_TOKEN_BYTES - 4U);
  bytes[132] = (unsigned char)crc;
  bytes[133] = (unsigned char)(crc >> 8U);
  bytes[134] = (unsigned char)(crc >> 16U);
  bytes[135] = (unsigned char)(crc >> 24U);
  lonejson_error_init(&json_error);
  needed = 0U;
  status = lonejson_base64_encode(bytes, sizeof(bytes), LONEJSON_BASE64_URL_RAW,
                                  out, LC_COLLECTION_CURSOR_BYTES - 1U, &needed,
                                  &json_error);
  if (status != LONEJSON_STATUS_OK)
    return lc_collection_failure(reader->error, LC_ERR_PROTOCOL, NULL,
                                 "failed to encode collection cursor");
  out[needed] = '\0';
  return LC_OK;
}

static int lc_cr_resume(lc_collection_reader *reader, lc_u64 *after,
                        int *checkpoint) {
  unsigned char bytes[LC_CR_TOKEN_BYTES];
  lonejson_error json_error;
  lonejson_status status;
  unsigned long crc;
  size_t needed;
  size_t i;
  const char *token;
  lc_u64 lower;
  token = reader->opts->cursor;
  lower = reader->opts->has_after ? reader->opts->after : 0U;
  *after = lower;
  *checkpoint = 0;
  if (token == NULL)
    return LC_OK;
  if (strlen(token) >= LC_COLLECTION_CURSOR_BYTES)
    goto invalid;
  lonejson_error_init(&json_error);
  needed = 0U;
  status = lonejson_base64_decode(token, strlen(token), LONEJSON_BASE64_URL_RAW,
                                  bytes, sizeof(bytes), &needed, &json_error);
  if (status != LONEJSON_STATUS_OK || needed != sizeof(bytes) ||
      memcmp(bytes, "LCCR001", 8U) != 0 ||
      memcmp(bytes + 8U, reader->call->collection->id, 20U) != 0 ||
      (bytes[28] != 1U && bytes[28] != 2U) || bytes[29] != 0U ||
      bytes[30] != 0U || bytes[31] != 0U ||
      memcmp(bytes + 56U, reader->selection_hash, 32U) != 0 ||
      memcmp(bytes + 88U, reader->namespace_hash, 32U) != 0)
    goto invalid;
  for (i = 128U; i < 132U; ++i)
    if (bytes[i] != 0U)
      goto invalid;
  crc = (unsigned long)bytes[132] | ((unsigned long)bytes[133] << 8U) |
        ((unsigned long)bytes[134] << 16U) | ((unsigned long)bytes[135] << 24U);
  if (crc != (unsigned long)crc32(0L, bytes, LC_CR_TOKEN_BYTES - 4U))
    goto invalid;
  *after = lc_cr_get(bytes + 32U);
  reader->upper = lc_cr_get(bytes + 40U);
  reader->instant = lc_cr_get(bytes + 48U);
  reader->remaining = lc_cr_get(bytes + 120U);
  *checkpoint = bytes[28] == 2U;
  if (*after < lower || (*after > reader->upper && *after != lower) ||
      reader->instant > (lc_u64)LC_I64_MAX ||
      (lc_u64)(time_t)reader->instant != reader->instant ||
      (reader->opts->has_through && reader->upper > reader->opts->through) ||
      (!reader->opts->has_first_n && reader->remaining != 0U) ||
      (reader->opts->has_first_n &&
       reader->remaining > reader->opts->first_n) ||
      (*checkpoint &&
       reader->remaining !=
           (reader->opts->has_first_n ? reader->opts->first_n : 0U)) ||
      (*checkpoint && !reader->opts->has_first_n &&
       *after != (reader->upper > lower ? reader->upper : lower)))
    goto invalid;
  /* Round-trip checks enforce canonical unpadded base64url, including tail
   * bits. */
  if (lc_cr_cursor(reader, *after, reader->remaining, *checkpoint,
                   reader->proposed) != LC_OK)
    return reader->error->code;
  if (strcmp(token, reader->proposed) != 0)
    goto invalid;
  return LC_OK;
invalid:
  return lc_collection_failure(reader->error, LC_ERR_INVALID, "cursor_invalid",
                               "collection cursor is malformed or bound to "
                               "another collection or selection");
}

static int lc_cr_cancel(lc_collection_reader *reader) {
  int cancelled;
  if (reader->cancelled)
    return 1;
  if (reader->opts->cancelled == NULL)
    return 0;
  lc_collection_call_callback(reader->call, 1, 0);
  cancelled = reader->opts->cancelled(reader->opts->cancel_context);
  lc_collection_call_callback(reader->call, 0, 0);
  if (cancelled)
    reader->cancelled = 1;
  return cancelled;
}

static int lc_cr_write(lc_collection_reader *reader, const void *bytes,
                       size_t size) {
  int accepted;
  if (lc_cr_cancel(reader))
    return LC_ERR_PENDING;
  lc_collection_call_callback(reader->call, 1, 0);
  accepted = reader->sink->write(reader->sink, bytes, size, reader->error);
  lc_collection_call_callback(reader->call, 0, 0);
  if (accepted)
    return LC_OK;
  return reader->error->code != LC_OK
             ? reader->error->code
             : lc_collection_failure(reader->error, LC_ERR_TRANSPORT, NULL,
                                     "collection sink rejected bytes");
}

static int lc_cr_accept(lc_collection_reader *reader, lc_u64 after, int entry) {
  lc_collection_read_metadata metadata;
  lc_u64 remaining;
  int rc;
  if (lc_cr_cancel(reader))
    return LC_ERR_PENDING;
  remaining = reader->remaining;
  if (entry && reader->opts->has_first_n)
    --remaining;
  rc = lc_cr_cursor(reader, after, remaining, 0, reader->proposed);
  if (rc != LC_OK)
    return rc;
  if (reader->opts->on_metadata != NULL) {
    memset(&metadata, 0, sizeof(metadata));
    metadata.kind = entry ? LC_COLLECTION_READ_METADATA_ENTRY
                          : LC_COLLECTION_READ_METADATA_PROGRESS;
    metadata.last_examined_position = after;
    metadata.resume_cursor = reader->proposed;
    if (entry) {
      metadata.position = reader->record.position;
      metadata.append_id = reader->record.append_id;
      metadata.etag = reader->record.etag;
      metadata.bytes = reader->record.bytes;
      metadata.retention_support = LC_STATE_RETENTION_SUPPORTED;
      metadata.has_retention_expires_at_unix = 1;
      metadata.retention_expires_at_unix = reader->record.retention;
    }
    lc_collection_call_callback(reader->call, 1, 0);
    rc = reader->opts->on_metadata(reader->opts->metadata_context, &metadata,
                                   reader->error);
    lc_collection_call_callback(reader->call, 0, 0);
    if (rc != LC_OK) {
      if (reader->error->code == LC_OK)
        lc_collection_failure(reader->error, rc, NULL,
                              "collection metadata callback rejected progress");
      return rc;
    }
  }
  memcpy(reader->result->resume_cursor, reader->proposed,
         strlen(reader->proposed) + 1U);
  reader->result->last_examined_position = after;
  reader->remaining = remaining;
  if (entry)
    ++reader->result->completed_records;
  return LC_OK;
}

static int lc_cr_body_open(lc_collection_reader *reader) {
  return lc_collection_payload_open(&reader->payload, reader->call->collection,
                                    reader->record_fd, &reader->record,
                                    reader->error);
}

static int lc_cr_body_read(lc_collection_reader *reader, unsigned char *bytes,
                           size_t capacity, size_t *got) {
  *got = 0U;
  if (lc_cr_cancel(reader))
    return LC_ERR_PENDING;
  return lc_collection_payload_read(&reader->payload, bytes, capacity, got,
                                    reader->error);
}

static int lc_cr_body_finish(lc_collection_reader *reader) {
  return lc_collection_payload_finish(&reader->payload, reader->error);
}

static int lc_cr_query_page(lc_collection_reader *reader,
                            const lc_collection_order_row *rows, size_t count,
                            lc_u64 *possible, lc_u64 *proven) {
  lc_collection_index index;
  int found;
  int rc;
  *possible = ((lc_u64)1U << count) - 1U;
  *proven = 0U;
  rc = lc_collection_query_load(
      reader->call->collection, reader->order.query_id,
      reader->iter.page_number * LC_COLLECTION_ORDER_PAGE_ROWS,
      lc_collection_query_row_hash(rows, count), &index, &found, reader->error);
  if (rc == LC_OK && found && index.rows == count) {
    if (reader->plan.prefix)
      rc = lc_collection_index_lookup_prefix(&index, reader->plan.keys,
                                             reader->plan.presence, possible,
                                             proven, reader->error);
    else
      rc = lc_collection_index_lookup(&index, reader->plan.keys,
                                      reader->plan.presence, possible, proven,
                                      reader->error);
  }
  lc_collection_index_cleanup(&index);
  return rc;
}

static lql_status lc_cr_lql_empty(void *user, unsigned char *bytes,
                                  size_t capacity, size_t *got,
                                  lql_error *error) {
  (void)user;
  (void)bytes;
  (void)capacity;
  (void)error;
  *got = 0U;
  return LQL_STATUS_OK;
}

static lql_status lc_cr_lql_read(void *user, unsigned char *bytes,
                                 size_t capacity, size_t *got,
                                 lql_error *error) {
  lc_collection_reader *reader;
  int rc;
  (void)error;
  reader = (lc_collection_reader *)user;
  rc = lc_cr_body_read(reader, bytes, capacity, got);
  return rc == LC_OK ? LQL_STATUS_OK : LQL_STATUS_CALLBACK_ERROR;
}

static time_t lc_cr_now(void *user) {
  return (time_t)((lc_collection_reader *)user)->instant;
}

static int lc_cr_lql_failure(lc_collection_reader *reader, lql_status status,
                             const lql_error *error) {
  if (reader->cancelled)
    return LC_ERR_PENDING;
  if (reader->error->code != LC_OK)
    return reader->error->code;
  return lc_error_set(reader->error,
                      status == LQL_STATUS_UNSUPPORTED  ? LC_ERR_UNSUPPORTED
                      : status == LQL_STATUS_NO_MEMORY  ? LC_ERR_NOMEM
                      : status == LQL_STATUS_JSON_ERROR ? LC_ERR_PROTOCOL
                                                        : LC_ERR_INVALID,
                      0L, "collection LQL filter cannot be applied",
                      error->message,
                      status == LQL_STATUS_UNSUPPORTED  ? "filter_unsupported"
                      : status == LQL_STATUS_JSON_ERROR ? "collection_corrupt"
                                                        : "filter_invalid",
                      "liblql");
}

int lc_collection_read_method(lc_collection *self,
                              const lc_collection_read_opts *opts,
                              lc_sink *sink, lc_collection_read_res *out,
                              lc_error *error) {
  lc_collection_call call;
  lc_collection_reader reader;
  lc_collection_read_opts defaults;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_collection_order_row physical[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_collection_handle *collection;
  lql *runtime;
  lql_selector *selector;
  lql_stream_request request;
  lql_stream_result result;
  lql_error query_error;
  lql_status status;
  unsigned char *chunk;
  const char *expression;
  size_t count;
  size_t i;
  size_t got;
  size_t physical_count, slot;
  lc_u64 possible, proven, bit;
  lc_u64 after;
  lc_u64 captured_upper;
  lc_u64 examined;
  lc_u64 pending;
  unsigned int quiet_pages;
  int checkpoint;
  int matched;
  int rc, commit_pin;
  time_t now;
  if (sink == NULL || sink->write == NULL || out == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection read requires a sink and initialized result");
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  memset(&defaults, 0, sizeof(defaults));
  if (opts != NULL)
    defaults = *opts;
  opts = &defaults;
  memset(&reader, 0, sizeof(reader));
  reader.call = &call;
  reader.opts = opts;
  reader.sink = sink;
  reader.result = out;
  reader.error = error;
  reader.order.fd = reader.reader_fd = reader.record_fd = -1;
  runtime = NULL;
  selector = NULL;
  chunk = NULL;
  memset(out, 0, sizeof(*out));
  out->stop_reason = LC_COLLECTION_READ_ERROR;
  expression = opts->selector_lql != NULL ? opts->selector_lql : "";
  rc = lc_cr_selection(&reader, expression);
  if (rc == LC_OK)
    rc = lc_collection_hash(collection->ns, strlen(collection->ns),
                            reader.namespace_hash, error);
  if (rc == LC_OK)
    rc = lc_cr_resume(&reader, &after, &checkpoint);
  lql_error_init(&query_error);
  memset(&request, 0, sizeof(request));
  if (rc == LC_OK && expression[0] != '\0') {
    status = lql_new(&runtime, &query_error);
    if (status == LQL_STATUS_OK)
      status =
          runtime->selector_parse(runtime, expression, &selector, &query_error);
    if (status == LQL_STATUS_OK) {
      request.reader = lc_cr_lql_empty;
      request.selector = selector;
      request.time_now = lc_cr_now;
      request.time_user = &reader;
      status = lql_stream_apply(runtime, &request, &result, &query_error);
    }
    if (status != LQL_STATUS_OK)
      rc = lc_cr_lql_failure(&reader, status, &query_error);
  }
  if (rc == LC_OK && collection->pouch->query_indexing_enabled)
    rc = lc_collection_query_plan_init(&reader.plan, &collection->allocator,
                                       runtime, selector, error);
  if (rc == LC_OK) {
    rc = lc_collection_capture_enter(collection, &commit_pin, error);
    if (rc == LC_OK) {
      rc = lc_collection_refresh_view(collection, error);
      if (rc == LC_OK)
        rc = lc_collection_lease_check(collection, 0, error);
      if (rc == LC_OK)
        rc = lc_collection_head_match(collection, opts->expected_head, error);
      if (rc == LC_OK) {
        captured_upper = collection->order.upper_position;
        if (opts->cursor == NULL || checkpoint) {
          reader.upper = captured_upper;
          if (opts->has_through && reader.upper > opts->through)
            reader.upper = opts->through;
          now = time(NULL);
          if (now < (time_t)0)
            rc = lc_collection_failure(
                error, LC_ERR_TRANSPORT, NULL,
                "failed to obtain collection evaluation time");
          else
            reader.instant = (lc_u64)now;
        } else if (reader.upper > captured_upper)
          rc = lc_collection_failure(
              error, LC_ERR_INVALID, "cursor_unavailable",
              "collection cursor exceeds retained committed history");
        if (rc == LC_OK && after > reader.upper &&
            !(opts->has_after && after == opts->after))
          rc = lc_collection_failure(
              error, LC_ERR_INVALID, "cursor_unavailable",
              "collection checkpoint exceeds retained committed history");
        if (rc == LC_OK) {
          rc = lc_collection_read_pin(collection, collection->head.snapshot,
                                      &reader.reader_fd, error);
        }
        if (rc == LC_OK) {
          reader.order = collection->order;
          reader.order.fd = dup(collection->order.fd);
          if (reader.order.fd < 0)
            rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                       "failed to pin collection order view");
          else
            rc = lc_collection_order_seek(&reader.order, after, reader.upper,
                                          reader.order.generation, 0,
                                          &reader.iter, error);
        }
      }
      lc_collection_capture_leave(collection, &commit_pin);
    }
  }
  if (rc == LC_OK) {
    out->resume_cursor =
        (char *)lc_alloc_with_allocator(NULL, LC_COLLECTION_CURSOR_BYTES);
    out->checkpoint_cursor =
        (char *)lc_alloc_with_allocator(NULL, LC_COLLECTION_CURSOR_BYTES);
    chunk = (unsigned char *)lc_alloc_with_allocator(&collection->allocator,
                                                     LC_CR_CHUNK_BYTES);
    if (out->resume_cursor == NULL || out->checkpoint_cursor == NULL ||
        chunk == NULL) {
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate bounded collection read buffers");
      lc_free_with_allocator(NULL, out->resume_cursor);
      out->resume_cursor = NULL;
    } else {
      out->resume_cursor[0] = out->checkpoint_cursor[0] = '\0';
      out->upper_position = reader.upper;
      out->last_examined_position = after;
      rc =
          lc_cr_cursor(&reader, after, reader.remaining, 0, out->resume_cursor);
    }
  }
  pending = out->last_examined_position;
  quiet_pages = 0U;
  while (rc == LC_OK && !reader.iter.done &&
         (!opts->has_first_n || reader.remaining != 0U)) {
    if (lc_cr_cancel(&reader))
      break;
    rc = lc_collection_metadata_enter(collection, 0, error);
    count = 0U;
    physical_count = 0U;
    possible = LC_U64_MAX;
    proven = 0U;
    examined = 0U;
    if (rc == LC_OK) {
      rc = lc_collection_order_refresh(&reader.order, error);
      if (rc == LC_OK)
        rc = lc_collection_order_batch(&reader.order, &reader.iter, rows,
                                       LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                       &examined, error);
      if (rc == LC_OK && count != 0U && reader.plan.count != 0U)
        rc = lc_collection_order_rows(
            &reader.order,
            reader.iter.page_number * LC_COLLECTION_ORDER_PAGE_ROWS, physical,
            LC_COLLECTION_ORDER_PAGE_ROWS, &physical_count, error);
      lc_collection_metadata_leave(collection);
    }
    if (rc == LC_OK && count != 0U && reader.plan.count != 0U)
      rc = lc_cr_query_page(&reader, physical, physical_count, &possible,
                            &proven);
    slot = 0U;
    for (i = 0U; rc == LC_OK && !reader.cancelled && i < count &&
                 (!opts->has_first_n || reader.remaining != 0U);
         ++i) {
      if (lc_cr_cancel(&reader))
        break;
      /* A gap is known examined before any bytes of the next live object. */
      if (rows[i].position > pending + 1U) {
        pending = rows[i].position - 1U;
        rc = lc_cr_accept(&reader, pending, 0);
        quiet_pages = 0U;
      }
      bit = 0U;
      if (rc == LC_OK && reader.plan.count != 0U) {
        while (slot < physical_count &&
               physical[slot].position < rows[i].position)
          ++slot;
        if (slot == physical_count ||
            physical[slot].position != rows[i].position) {
          rc = lc_collection_failure(
              error, LC_ERR_PROTOCOL, "collection_corrupt",
              "collection query page does not contain its ordered entry");
          break;
        }
        bit = (lc_u64)1U << slot;
        if ((possible & bit) == 0U) {
          if (opts->has_first_n)
            --reader.remaining;
          pending = rows[i].position;
          continue;
        }
      }
      if (rc == LC_OK)
        rc = lc_collection_record_ref(collection, rows[i].segment,
                                      rows[i].record_offset, &reader.record_fd,
                                      &reader.file_segment, &reader.file_end,
                                      &reader.record, error);
      if (rc == LC_OK)
        rc = lc_collection_record_at(collection, reader.iter.generation,
                                     &reader.record_fd, &reader.file_segment,
                                     &reader.file_end, &reader.record, error);
      if (rc == LC_OK && (reader.record.deleted ||
                          reader.record.position != rows[i].position ||
                          reader.record.published_at != rows[i].published_at ||
                          reader.record.bytes != rows[i].bytes))
        rc = lc_collection_failure(
            error, LC_ERR_PROTOCOL, "collection_corrupt",
            "collection order points to a different canonical entry");
      matched = 1;
      if (rc == LC_OK && selector != NULL && (proven & bit) == 0U) {
        rc = lc_cr_body_open(&reader);
        if (rc == LC_OK) {
          request.reader = lc_cr_lql_read;
          request.reader_user = &reader;
          status = lql_stream_apply(runtime, &request, &result, &query_error);
          if (status != LQL_STATUS_OK)
            rc = lc_cr_lql_failure(&reader, status, &query_error);
          else if (result.records_seen != 1U)
            rc = lc_collection_failure(
                error, LC_ERR_PROTOCOL, "collection_corrupt",
                "collection entry is not one JSON object");
          if (rc == LC_OK)
            rc = lc_cr_body_finish(&reader);
          matched = result.records_matched == 1U;
        }
      }
      if (rc == LC_OK && matched) {
        if (pending > out->last_examined_position)
          rc = lc_cr_accept(&reader, pending, 0);
        if (rc == LC_OK)
          rc = lc_cr_body_open(&reader);
        while (rc == LC_OK) {
          rc = lc_cr_body_read(&reader, chunk, LC_CR_CHUNK_BYTES, &got);
          if (rc != LC_OK || got == 0U)
            break;
          rc = lc_cr_write(&reader, chunk, got);
        }
        if (rc == LC_OK)
          rc = lc_cr_body_finish(&reader);
        if (rc == LC_OK)
          rc = lc_cr_write(&reader, "\n", 1U);
      }
      if (rc == LC_OK && matched) {
        rc = lc_cr_accept(&reader, rows[i].position, 1);
        quiet_pages = 0U;
      }
      if (rc == LC_OK && !matched && opts->has_first_n)
        --reader.remaining;
      if (rc == LC_OK)
        pending = rows[i].position;
      lc_collection_payload_reset(&reader.payload);
    }
    if (rc == LC_OK && !reader.cancelled &&
        (!opts->has_first_n || reader.remaining != 0U)) {
      if (examined > pending)
        pending = examined;
      ++quiet_pages;
      if (quiet_pages >= LC_CR_PROGRESS_PAGES &&
          pending > out->last_examined_position) {
        rc = lc_cr_accept(&reader, pending, 0);
        quiet_pages = 0U;
      }
    }
  }
  if (rc == LC_OK && !reader.cancelled) {
    if (opts->has_first_n && reader.remaining == 0U &&
        pending > out->last_examined_position)
      rc = lc_cr_accept(&reader, pending, 0);
    if (rc == LC_OK && out->last_examined_position < reader.upper &&
        (!opts->has_first_n || reader.remaining != 0U))
      rc = lc_cr_accept(&reader, reader.upper, 0);
    if (rc == LC_OK)
      rc = lc_cr_cursor(&reader, out->last_examined_position,
                        opts->has_first_n ? opts->first_n : 0U, 1,
                        out->checkpoint_cursor);
    if (rc == LC_OK) {
      out->complete = 1;
      out->stop_reason = LC_COLLECTION_READ_FINISHED;
    }
  }
  if (reader.cancelled &&
      (rc == LC_OK || (rc == LC_ERR_PENDING && error->code == LC_OK))) {
    rc = LC_OK;
    out->stop_reason = LC_COLLECTION_READ_CANCELLED;
  }
  if (!out->complete) {
    lc_free_with_allocator(NULL, out->checkpoint_cursor);
    out->checkpoint_cursor = NULL;
  } else {
    lc_free_with_allocator(NULL, out->resume_cursor);
    out->resume_cursor = NULL;
  }
  lc_collection_payload_cleanup(&reader.payload);
  lc_collection_query_plan_cleanup(&reader.plan);
  if (reader.reader_fd >= 0)
    (void)close(reader.reader_fd);
  if (reader.record_fd >= 0)
    (void)close(reader.record_fd);
  lc_collection_order_close(&reader.order);
  lc_free_with_allocator(&collection->allocator, chunk);
  if (selector != NULL)
    runtime->selector_destroy(runtime, selector);
  if (runtime != NULL)
    runtime->destroy(runtime);
  lc_collection_call_end(&call);
  return rc;
}
