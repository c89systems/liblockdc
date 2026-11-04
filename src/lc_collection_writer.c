#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_collection_json.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

enum { LC_CW_OPEN, LC_CW_FAILED, LC_CW_COMMITTED, LC_CW_ABORTED };
typedef struct lc_collection_writer_handle {
  lc_collection_writer pub;
  lc_collection_handle *collection;
  lc_allocator allocator;
  lc_pouch_collection_writer *encoder;
  lc_sink destination, plaintext;
  lc_pouch_crypto_writer *codec;
  lc_collection_unit unit;
  char base_context[256], unit_context[256], first_descriptor[128];
  char final_etag[65];
  lc_u64 unit_plain, unit_limit, unit_max, ordinal, total_stored;
  unsigned long first_crc;
  int unit_done;
  lc_collection_chunks chunks;
  int pinned;
  int committing;
  int emitted;
  int direct;
  int hash_only;
  int state;
  int active;
  int closed;
  unsigned int references;
  lc_u64 ownership_generation;
  lc_collection_call *call;
  lc_collection_guard *guard;
  int entered;
} lc_collection_writer_handle;

static pthread_mutex_t lc_cw_lifecycle = PTHREAD_MUTEX_INITIALIZER;

static void lc_cw_discard(lc_collection_writer_handle *writer) {
  lc_pouch_collection_writer_close(writer->encoder);
  writer->encoder = NULL;
  lc_pouch_crypto_writer_close(writer->codec);
  writer->codec = NULL;
  lc_collection_chunks_close(&writer->chunks);
  pthread_mutex_lock(&lc_cw_lifecycle);
  if (writer->pinned && --writer->collection->stream_writers == 0)
    (void)flock(writer->collection->streams_fd, LOCK_UN);
  writer->pinned = 0;
  pthread_mutex_unlock(&lc_cw_lifecycle);
  if (writer->emitted && writer->state != LC_CW_COMMITTED)
    lc_pouch_compaction_note_mutation(writer->collection->pouch,
                                      writer->collection->ns, 0);
  writer->emitted = 0;
}

static int lc_cw_storage_enter(lc_collection_writer_handle *writer,
                               lc_error *error) {
  int rc;
  if (writer->entered)
    return LC_OK;
  rc = lc_collection_stream_enter(writer->call, writer->guard, error);
  if (rc != LC_OK)
    return rc;
  writer->entered = 1;
  if (writer->ownership_generation !=
      writer->collection->head.lease.fencing_token)
    return lc_collection_failure(error, LC_ERR_INVALID, "lease_changed",
                                 "collection ownership changed during upload");
  return LC_OK;
}

static int lc_cw_storage_leave(lc_collection_writer_handle *writer, int rc,
                               lc_error *error) {
  if (writer->entered && !writer->committing) {
    if (rc == LC_OK) {
      off_t end = lseek(writer->collection->active_fd, 0, SEEK_END);
      rc = end < 0
               ? lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                       "failed to locate canonical stream tail")
               : lc_collection_order_checkpoint(&writer->collection->order,
                                                writer->collection->head.active,
                                                (lc_u64)end, error);
    }
    lc_collection_metadata_leave(writer->collection);
    lc_collection_write_leave(writer->guard);
    writer->entered = 0;
  }
  return rc;
}

static int lc_cw_emit(lc_sink *sink, const void *bytes, size_t count,
                      lc_error *error) {
  lc_collection_writer_handle *writer = sink->impl;
  int rc;
  /* Continuing a private unit needs neither a new reservation nor a replay
   * lookup. Unit finalization and commit still check the current owner. */
  if (!writer->committing && !writer->entered && writer->chunks.pending.bytes)
    return lc_collection_chunks_append(writer->collection, &writer->chunks,
                                       bytes, count, error) == LC_OK;
  rc = lc_cw_storage_enter(writer, error);
  if (rc != LC_OK)
    return 0;
  /* If the producer never filled a codec chunk, EOF can write that bounded
   * final chunk straight into the existing contiguous record format. No
   * payload is copied or retained after this callback. */
  if (writer->committing && !writer->ordinal && !writer->chunks.extents &&
      !writer->chunks.pending.bytes) {
    const unsigned char *p = bytes;
    ssize_t written;
    int rc;
    char leaf[64];
    if (!writer->direct) {
      rc = lc_collection_rotate(writer->collection, 0, error);
      if (rc == LC_OK)
        rc = lc_collection_record_leaf(writer->collection->head.active, leaf,
                                       error);
      if (rc == LC_OK)
        rc = lc_pouch_collection_writer_attach(
            writer->encoder, writer->collection->active_fd, leaf, error);
      if (rc != LC_OK)
        return 0;
      writer->direct = 1;
    }
    while (count) {
      written = write(writer->collection->active_fd, p, count);
      if (written < 0 && errno == EINTR)
        continue;
      if (written <= 0) {
        lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                              "failed to write final collection codec chunk");
        return 0;
      }
      p += written;
      count -= (size_t)written;
    }
    return 1;
  }
  if (!writer->committing && !writer->pinned) {
    int rc = 0;
    pthread_mutex_lock(&lc_cw_lifecycle);
    if (!writer->collection->stream_writers) {
      do {
        rc = flock(writer->collection->streams_fd, LOCK_SH);
      } while (rc != 0 && errno == EINTR);
    }
    if (rc == 0) {
      ++writer->collection->stream_writers;
      writer->pinned = 1;
    }
    pthread_mutex_unlock(&lc_cw_lifecycle);
    if (rc != 0) {
      lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                            "failed to pin canonical stream");
      return 0;
    }
  }
  writer->emitted = 1;
  if (!writer->chunks.pending.bytes) {
    lc_u64 available, low, high, middle;
    lc_u64 required =
        lc_pouch_crypto_writer_bound(writer->codec, writer->unit_plain);
    rc = lc_collection_chunks_prepare(writer->collection, &writer->chunks,
                                      required, &available, error);
    if (rc == LC_OK && lc_pouch_crypto_writer_bound(
                           writer->codec, writer->unit_limit) > available) {
      low = writer->unit_plain;
      high = writer->unit_limit;
      while (low < high) {
        middle = low + (high - low + 1U) / 2U;
        if (lc_pouch_crypto_writer_bound(writer->codec, middle) <= available)
          low = middle;
        else
          high = middle - 1U;
      }
      writer->unit_limit = low;
    }
  }
  /* Reservation and fencing need append authority; writing its exclusively
   * owned bytes does not. Keep the commit's authority for final publication. */
  rc = lc_cw_storage_leave(writer, rc, error);
  if (rc == LC_OK)
    rc = lc_collection_chunks_append(writer->collection, &writer->chunks, bytes,
                                     count, error);
  return rc == LC_OK;
}

static int lc_cw_unit_finish(lc_collection_writer_handle *writer,
                             lc_error *error) {
  const char *descriptor = NULL;
  int rc, seal_first = 0;
  if (writer->unit_done || !writer->unit_plain)
    return LC_OK;
  memset(&writer->unit, 0, sizeof(writer->unit));
  rc = lc_pouch_crypto_writer_finish_borrowed(
      writer->codec, &writer->unit.plain, &writer->unit.stored,
      &writer->unit.crc, &descriptor, error);
  if (rc != LC_OK)
    return rc;
  if (descriptor && strlen(descriptor) >= sizeof(writer->unit.descriptor))
    return lc_collection_failure(
        error, LC_ERR_PROTOCOL, NULL,
        "collection unit descriptor exceeds its bound");
  if (descriptor)
    strcpy(writer->unit.descriptor, descriptor);
  strcpy(writer->unit.context, writer->unit_context);
  writer->unit.ordinal = writer->ordinal;
  if (!writer->ordinal) {
    strcpy(writer->unit.etag, writer->final_etag);
    strcpy(writer->first_descriptor, writer->unit.descriptor);
    writer->first_crc = writer->unit.crc;
  }
  if (writer->unit.stored > (lc_u64)LC_I64_MAX - writer->total_stored)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection encoded length exceeds its bound");
  if (!writer->direct) {
    rc = lc_cw_storage_enter(writer, error);
    if (rc == LC_OK)
      rc = lc_collection_chunks_end_unit(writer->collection, &writer->chunks,
                                         &writer->unit, error);
    if (rc == LC_OK)
      seal_first =
          writer->chunks.first.segment != writer->collection->head.active;
    rc = lc_cw_storage_leave(writer, rc, error);
    if (rc == LC_OK && !writer->committing && seal_first)
      rc = lc_collection_chunks_seal_first(&writer->chunks, error);
  }
  if (rc == LC_OK) {
    writer->total_stored += writer->unit.stored;
    ++writer->ordinal;
    writer->unit_done = 1;
  }
  return rc;
}

static int lc_cw_plaintext(lc_sink *sink, const void *input, size_t length,
                           lc_error *error) {
  lc_collection_writer_handle *writer = sink->impl;
  const unsigned char *bytes = input;
  lc_sink *encoded = lc_pouch_crypto_writer_sink(writer->codec);
  size_t take;
  int rc;
  while (length) {
    /* A full final unit waits only for the next input or commit. This lets
     * a single unit receive its complete fingerprint before its one metadata
     * write, without another hash or a value-sized buffer. */
    if (!writer->unit_done && writer->unit_plain == writer->unit_limit &&
        lc_cw_unit_finish(writer, error) != LC_OK)
      return 0;
    if (writer->unit_done) {
      rc = lc_collection_unit_context(writer->base_context, writer->ordinal,
                                      writer->unit_context, error);
      if (rc == LC_OK)
        rc = lc_pouch_crypto_writer_restart(writer->codec, writer->unit_context,
                                            error);
      if (rc != LC_OK)
        return 0;
      writer->unit_plain = 0;
      writer->unit_limit = writer->unit_max;
      writer->unit_done = 0;
    }
    take = writer->unit_limit - writer->unit_plain < length
               ? (size_t)(writer->unit_limit - writer->unit_plain)
               : length;
    writer->unit_plain += take;
    if (!encoded->write(encoded, bytes, take, error))
      return 0;
    bytes += take;
    length -= take;
  }
  return 1;
}

static int lc_cw_encode(lc_collection_writer_handle *writer, const void *bytes,
                        size_t count, lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  int rc;
  rc = lc_collection_call_begin_held(writer->collection, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  writer->call = &call;
  writer->guard = &guard;
  writer->entered = 0;
  rc = lc_pouch_collection_writer_write(writer->encoder, bytes, count, error);
  if (writer->entered) {
    lc_collection_metadata_leave(writer->collection);
    lc_collection_write_leave(&guard);
  }
  writer->call = NULL;
  writer->guard = NULL;
  lc_collection_call_end(&call);
  return rc;
}

static void lc_cw_destroy(lc_collection_writer_handle *writer) {
  lc_allocator allocator = writer->allocator;
  lc_collection_handle *collection = writer->collection;
  lc_cw_discard(writer);
  lc_free_with_allocator(&allocator, writer);
  lc_collection_handle_release(collection);
}

static int lc_cw_enter(lc_collection_writer *self, int require_open,
                       lc_collection_writer_handle **out, lc_error *error) {
  lc_collection_writer_handle *writer;
  *out = NULL;
  if (self == NULL || self->impl == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "operation requires a collection append writer");
  writer = self->impl;
  pthread_mutex_lock(&lc_cw_lifecycle);
  if (writer->closed || writer->active ||
      (require_open && writer->state != LC_CW_OPEN)) {
    pthread_mutex_unlock(&lc_cw_lifecycle);
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection writer is closed, busy or terminal");
  }
  writer->active = 1;
  ++writer->references;
  pthread_mutex_unlock(&lc_cw_lifecycle);
  *out = writer;
  return LC_OK;
}

static void lc_cw_leave(lc_collection_writer_handle *writer) {
  int release;
  pthread_mutex_lock(&lc_cw_lifecycle);
  writer->active = 0;
  release = --writer->references == 0U;
  pthread_mutex_unlock(&lc_cw_lifecycle);
  if (release)
    lc_cw_destroy(writer);
}

static void lc_cw_close(lc_collection_writer *self) {
  lc_collection_writer_handle *writer = self->impl;
  int release = 0;
  pthread_mutex_lock(&lc_cw_lifecycle);
  if (!writer->closed) {
    writer->closed = 1;
    release = --writer->references == 0U;
  }
  pthread_mutex_unlock(&lc_cw_lifecycle);
  if (release)
    lc_cw_destroy(writer);
}

static lc_error *lc_cw_error(lc_error *error, lc_error *local) {
  lc_error_init(local);
  if (error == NULL)
    return local;
  lc_error_cleanup(error);
  lc_error_init(error);
  return error;
}

static int lc_cw_write(lc_collection_writer *self, const void *bytes,
                       size_t count, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_error local;
  int rc;
  error = lc_cw_error(error, &local);
  rc = lc_cw_enter(self, 1, &writer, error);
  if (rc == LC_OK) {
    rc = lc_cw_encode(writer, bytes, count, error);
    if (rc != LC_OK)
      writer->state = LC_CW_FAILED;
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

static int lc_cw_finish_body(lc_collection_writer_handle *writer,
                             lc_pouch_collection_record *body,
                             lc_error *error) {
  int rc = lc_pouch_collection_writer_finish(writer->encoder, body, error);
  if (rc == LC_OK && !writer->hash_only) {
    strcpy(writer->final_etag, body->etag);
    rc = lc_cw_unit_finish(writer, error);
  }
  if (rc == LC_OK && !writer->hash_only) {
    body->stored_bytes = writer->total_stored;
    body->stored_crc = writer->direct ? writer->first_crc : 0;
    strcpy(body->descriptor, writer->first_descriptor);
    rc = lc_pouch_collection_writer_storage(writer->encoder, body, error);
  }
  return rc;
}

static int lc_cw_commit(lc_collection_writer *self,
                        lc_collection_append_res *out, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_collection_call call;
  lc_collection_guard guard;
  lc_pouch_collection_record body;
  lc_pouch_collection_writer *encoder;
  lc_error local;
  int rc, prepared = 0;
  error = lc_cw_error(error, &local);
  if (out == NULL) {
    rc = lc_collection_failure(error, LC_ERR_INVALID, NULL,
                               "collection writer commit requires output");
    lc_error_cleanup(&local);
    return rc;
  }
  memset(out, 0, sizeof(*out));
  rc = lc_cw_enter(self, 1, &writer, error);
  if (rc == LC_OK) {
    rc = lc_collection_call_begin_held(writer->collection, 1, &call, error);
    if (rc == LC_OK) {
      writer->call = &call;
      writer->guard = &guard;
      writer->entered = 0;
      writer->committing = 0;
      /* Durable data seals can overlap producers. NoSync keeps its usual
       * single publication scope; cross-inode prerequisites remain mandatory
       * in bind when a producer hasn't already sealed them. */
      if (writer->emitted && !writer->hash_only &&
          writer->collection->pouch->durable_sync) {
        rc = lc_cw_finish_body(writer, &body, error);
        rc = lc_cw_storage_leave(writer, rc, error);
        prepared = rc == LC_OK;
      }
      if (rc == LC_OK)
        rc = lc_collection_append_enter(&call, &guard, error);
      if (rc == LC_OK) {
        writer->entered = 1;
        writer->committing = 1;
        if (!prepared)
          rc = lc_cw_finish_body(writer, &body, error);
        if (rc == LC_OK && !writer->hash_only && !writer->direct)
          rc = lc_collection_chunks_finish(writer->collection, &writer->chunks,
                                           error);
        writer->call = NULL;
        writer->guard = NULL;
        writer->entered = 0;
        if (rc == LC_OK) {
          encoder = writer->encoder;
          writer->encoder = NULL;
          body.stream_segment = writer->chunks.root.segment;
          body.stream_offset = writer->chunks.root.offset;
          body.stream_extents = writer->chunks.extents;
          body.stream_root_bytes = writer->chunks.root.bytes;
          body.stream_height = writer->chunks.height;
          rc = lc_collection_publish_stream(&call, &guard, encoder,
                                            writer->ownership_generation, &body,
                                            &writer->chunks, out, error);
        } else {
          /* A final-chunk record may already be pending. Roll it back before
           * releasing append authority, including codec/validation failures. */
          lc_pouch_collection_writer_close(writer->encoder);
          writer->encoder = NULL;
          lc_collection_metadata_leave(writer->collection);
          lc_collection_write_leave(&guard);
        }
      }
      writer->call = NULL;
      writer->guard = NULL;
      writer->entered = 0;
      lc_collection_call_end(&call);
    }
    writer->state = rc == LC_OK ? LC_CW_COMMITTED : LC_CW_FAILED;
    if (rc == LC_OK && out->duplicate && writer->emitted)
      lc_pouch_compaction_note_mutation(writer->collection->pouch,
                                        writer->collection->ns, 0);
    lc_cw_discard(writer);
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

static int lc_cw_abort(lc_collection_writer *self, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_error local;
  int rc;
  error = lc_cw_error(error, &local);
  rc = lc_cw_enter(self, 0, &writer, error);
  if (rc == LC_OK) {
    if (writer->state != LC_CW_COMMITTED)
      writer->state = LC_CW_ABORTED;
    lc_cw_discard(writer);
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

int lc_collection_append_begin_method(lc_collection *self,
                                      const lc_collection_append_req *req,
                                      lc_collection_writer **out,
                                      lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_writer_handle *writer = NULL;
  lc_pouch_collection_record existing;
  lc_u64 segment;
  char upload[LC_XID_STRING_SIZE];
  int rc, entered = 0, found = 0;
  if (out != NULL)
    *out = NULL;
  if (out == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection append begin requires output");
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_append_validate(call.collection, req, error);
  if (rc == LC_OK) {
    writer = lc_calloc_with_allocator(&call.collection->allocator, 1U,
                                      sizeof(*writer));
    if (writer == NULL)
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection append writer");
    else {
      writer->pub.write = lc_cw_write;
      writer->pub.commit = lc_cw_commit;
      writer->pub.abort = lc_cw_abort;
      writer->pub.close = lc_cw_close;
      writer->pub.impl = writer;
      writer->collection = call.collection;
      writer->allocator = call.collection->allocator;
      writer->references = 1U;
      writer->destination.write = lc_cw_emit;
      writer->destination.impl = writer;
      writer->plaintext.write = lc_cw_plaintext;
      writer->plaintext.impl = writer;
      writer->unit_limit = LC_COLLECTION_UNIT_MAX;
      lc_collection_handle_hold(writer->collection);
    }
  }
  if (rc == LC_OK) {
    rc = lc_collection_append_enter(&call, &guard, error);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK)
    writer->ownership_generation = call.collection->head.lease.fencing_token;
  if (rc == LC_OK)
    rc = lc_collection_lookup_id(call.collection, req->append_id, &existing,
                                 &segment, &found, error);
  if (rc == LC_OK)
    writer->hash_only = found;
  if (rc == LC_OK && !found && call.collection->streams_fd < 0) {
    char *path = lc_pouch_path_join(
        &writer->allocator, call.collection->directory, ".streams.lock");
    if (!path)
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate stream gate path");
    else {
      call.collection->streams_fd =
          open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
      lc_free_with_allocator(&writer->allocator, path);
      if (call.collection->streams_fd < 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to open stream gate");
    }
  }
  if (rc == LC_OK)
    rc = lc_xid_new(upload, error);
  if (rc == LC_OK) {
    int n = snprintf(writer->base_context, sizeof(writer->base_context),
                     "collection:%s:%s:%s", call.collection->id, req->append_id,
                     upload);
    if (n < 0 || (size_t)n >= sizeof(writer->base_context))
      rc = lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection upload context exceeds its bound");
    strcpy(writer->unit_context, writer->base_context);
  }
  if (rc == LC_OK && !found)
    rc = lc_pouch_crypto_writer_open_sink(
        call.pouch->crypto, writer->unit_context, &writer->destination,
        &writer->codec, error);
  if (rc == LC_OK && !found) {
    lc_u64 low = 1, high = LC_COLLECTION_UNIT_MAX, middle;
    lc_u64 nominal = call.pouch->segment_target_bytes;
    lc_u64 capacity = lc_collection_chunks_capacity(call.pouch);
    /* Reserve framing overhead separately from the nominal plaintext unit.
     * A target-sized value should not require another codec unit for its tag
     * or metadata. Neither bound describes an in-memory allocation. */
    if (nominal < 131072U)
      nominal = 131072U;
    if (high > nominal)
      high = nominal;
    if (lc_pouch_crypto_writer_bound(writer->codec, high) > capacity) {
      while (low < high) {
        middle = low + (high - low + 1U) / 2U;
        if (lc_pouch_crypto_writer_bound(writer->codec, middle) <= capacity)
          low = middle;
        else
          high = middle - 1U;
      }
    } else
      low = high;
    writer->unit_max = writer->unit_limit = low;
  }
  if (rc == LC_OK)
    rc = lc_pouch_collection_writer_open_sink(
        call.collection->pouch, found ? NULL : &writer->plaintext,
        call.collection->id, req->append_id, writer->base_context,
        req->has_retention_seconds
            ? -(lc_pouch_unix_seconds)req->retention_seconds
            : 0,
        req->has_collection_retention_seconds,
        req->has_collection_retention_seconds
            ? -(lc_pouch_unix_seconds)req->collection_retention_seconds
            : 0,
        &writer->encoder, error);
  if (entered) {
    lc_collection_metadata_leave(call.collection);
    lc_collection_write_leave(&guard);
  }
  if (rc == LC_OK)
    *out = &writer->pub;
  else if (writer != NULL)
    lc_cw_destroy(writer);
  lc_collection_call_end(&call);
  return rc;
}

int lc_collection_append_staged_method(lc_collection *self,
                                       const lc_collection_append_req *req,
                                       lc_source *source,
                                       lc_collection_append_res *out,
                                       lc_error *error) {
  lc_collection_call call;
  lc_collection_writer *writer = NULL;
  unsigned char *buffer = NULL;
  size_t got;
  int rc;
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (out == NULL || source == NULL || source->read == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "staged append requires source and output");
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_append_begin_method(self, req, &writer, error);
  if (rc == LC_OK) {
    buffer = lc_alloc_with_allocator(&call.collection->allocator,
                                     LC_COLLECTION_JSON_CHUNK_BYTES);
    if (buffer == NULL)
      rc =
          lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                "failed to allocate staged append input chunk");
  }
  while (rc == LC_OK) {
    lc_collection_call_callback(&call, 1, 1);
    got = source->read(source, buffer, LC_COLLECTION_JSON_CHUNK_BYTES, error);
    lc_collection_call_callback(&call, 0, 0);
    if (error->code != LC_OK)
      rc = error->code;
    else if (got > LC_COLLECTION_JSON_CHUNK_BYTES)
      rc = lc_collection_failure(
          error, LC_ERR_INVALID, NULL,
          "collection source exceeded requested chunk size");
    else if (got == 0U) {
      rc = lc_collection_writer_commit(writer, out, error);
      break;
    } else
      rc = lc_collection_writer_write(writer, buffer, got, error);
  }
  lc_collection_writer_close(writer);
  lc_free_with_allocator(&call.collection->allocator, buffer);
  lc_collection_call_end(&call);
  return rc;
}
