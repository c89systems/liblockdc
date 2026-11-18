#include "stream.h"
#include "lc_api_internal.h"
#include "owned.h"

#include <pthread.h>
#include <string.h>
#include <zlib.h>

typedef struct stream_run {
  ps_owned_run *handle;
  uint64_t base, capacity;
} stream_run;
struct ps_stream {
  ps_owned_head *head;
  ps_ref ref;
  pthread_mutex_t mutex;
  stream_run *runs;
  size_t count, allocated, readers;
  uint64_t tail, capacity, preferred_grant, checked;
  ps_piece_checksum checksum;
  uint64_t recovery_floor;
  unsigned char *recovery_buffer;
  uint64_t recovery_offset;
  size_t recovery_bytes;
  int appending, recovering, repair_pending, sealed, retired;
};
struct ps_stream_writer {
  ps_stream *stream;
  uint64_t start, bytes, flushed, prior_checked;
  ps_piece_checksum prior_checksum;
  size_t used;
  int failed, published;
  unsigned char buffer[PS_CHUNK];
};
struct ps_stream_window {
  ps_stream *stream;
  uint64_t start, end;
};
struct ps_stream_reader {
  ps_stream_window window;
  uint64_t offset;
  size_t at, used;
  unsigned char buffer[PS_CHUNK];
};
static int stream_error(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-stream");
}
static void note_grant(ps_stream *s, uint64_t capacity) {
  uint64_t next = (capacity + PS_RUN_HEADER) * 4;
  if (next > PS_CHUNK)
    next = PS_CHUNK;
  if (s->preferred_grant < next)
    s->preferred_grant = next;
}
static int stream_grow(ps_stream *s, lc_error *error) {
  size_t n;
  stream_run *runs;
  if (s->count < s->allocated)
    return LC_OK;
  n = s->allocated ? s->allocated * 2 : 4;
  if (n < s->allocated || n > (size_t)-1 / sizeof(*runs))
    return stream_error(error, LC_ERR_INVALID,
                        "stream run directory too large");
  runs = lc_realloc_with_allocator(NULL, s->runs, n * sizeof(*runs));
  if (!runs)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot grow stream run directory");
  s->runs = runs;
  s->allocated = n;
  return LC_OK;
}
/* Mapping is immutable after insertion. Capture by value under the stream
 * mutex so the pointer array may grow while readers perform physical I/O. */
static int stream_mapping(ps_stream *s, uint64_t offset, stream_run *out,
                          lc_error *error) {
  size_t lo = 0, hi, mid;
  pthread_mutex_lock(&s->mutex);
  hi = s->count;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2;
    if (s->runs[mid].base + s->runs[mid].capacity <= offset)
      lo = mid + 1;
    else
      hi = mid;
  }
  if (lo == s->count || offset < s->runs[lo].base) {
    pthread_mutex_unlock(&s->mutex);
    return stream_error(error, LC_ERR_PROTOCOL, "stream range has no grant");
  }
  *out = s->runs[lo];
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}
static int stream_io(ps_stream *s, uint64_t offset, void *bytes, size_t n,
                     int writing, lc_error *error) {
  unsigned char *p = bytes;
  stream_run run;
  size_t take;
  int rc;
  while (n) {
    rc = stream_mapping(s, offset, &run, error);
    if (rc != LC_OK)
      return rc;
    take = (size_t)(run.base + run.capacity - offset);
    if (take > n)
      take = n;
    rc = writing ? ps_owned_write(run.handle, offset - run.base, p, take, error)
                 : ps_owned_read(run.handle, offset - run.base, p, take, error);
    if (rc != LC_OK)
      return rc;
    n -= take;
    p += take;
    offset += take;
  }
  return LC_OK;
}
/* Only one appender/recovery owns completion. A partial piece's footer advances
 * with its canonical append; complete pieces never need another checksum write
 * or a whole-source scan. The scratch buffer is reused after body input.
 */
static int stream_pieces(ps_stream *s, uint64_t tail, int inspect, int partial,
                         void *scratch, int *repaired, lc_error *error) {
  stream_run run;
  uint64_t bytes, end, prior;
  ps_piece_checksum checksum, rebuilt = {0};
  int rc;
  while (s->checked < tail) {
    rc = stream_mapping(s, s->checked, &run, error);
    if (rc != LC_OK)
      return rc;
    end = run.base + run.capacity;
    if (end > tail && !partial)
      break;
    bytes = end > tail ? tail - run.base : run.capacity;
    prior = s->checked - run.base;
    rc = inspect ? ps_owned_piece_inspect(run.handle, bytes, &checksum, error)
                 : ps_owned_piece_extend(run.handle, prior,
                                         prior ? &s->checksum : NULL, bytes,
                                         scratch, &checksum, error);
    if (inspect == 2 && rc == LC_ERR_PROTOCOL) {
      /* Only the canonical recovery callback can authorize this repair.
       * Fully covered pieces were already inspected in strict mode. A valid
       * existing footer needs no writes during ordinary read admission. */
      lc_error_cleanup(error);
      rebuilt.reconstructed = run.base < s->recovery_floor;
      rc = ps_owned_piece_extend(run.handle, 0, &rebuilt, bytes, scratch,
                                 &checksum, error);
      if (rc == LC_OK && repaired)
        *repaired = 1;
    }
    if (rc != LC_OK)
      return rc;
    s->checked = run.base + bytes;
    s->checksum = checksum;
  }
  return LC_OK;
}
int ps_stream_recovery_cover(ps_stream *s, uint64_t covered, lc_error *error) {
  if (!s || !s->recovering || covered > s->capacity)
    return stream_error(error, LC_ERR_INVALID,
                        "piece coverage requires canonical recovery");
  if (s->recovery_floor < covered)
    s->recovery_floor = covered;
  return stream_pieces(s, covered, 1, 0, NULL, NULL, error);
}
static int clear_suffix(ps_stream *s, uint64_t offset, unsigned char *zero,
                        lc_error *error) {
  stream_run run;
  uint64_t n;
  int rc = LC_OK;
  memset(zero, 0, PS_CHUNK);
  while (offset < s->capacity && rc == LC_OK) {
    rc = stream_mapping(s, offset, &run, error);
    if (rc == LC_OK) {
      n = run.base + run.capacity - offset;
      rc = ps_owned_clear(run.handle, offset - run.base, n, zero, error);
      offset += n;
    }
  }
  return rc;
}
static void stream_destroy(ps_stream *s) {
  size_t i;
  for (i = 0; i < s->count; ++i)
    ps_owned_run_close(s->runs[i].handle);
  ps_owned_close(s->head);
  lc_free_with_allocator(NULL, s->recovery_buffer);
  lc_free_with_allocator(NULL, s->runs);
  (void)pthread_mutex_destroy(&s->mutex);
  lc_free_with_allocator(NULL, s);
}
int ps_stream_open(ps_space *space, ps_budget *budget, ps_key key, int create,
                   const ps_ref *ref, ps_stream_recover_fn recover,
                   void *context, ps_stream **out, lc_error *error) {
  ps_owned_status status;
  ps_owned_run *run = NULL;
  ps_stream *s;
  uint64_t i, accepted;
  int rc, found, repaired = 0;
  if (out)
    *out = NULL;
  if (!out)
    return stream_error(error, LC_ERR_INVALID, "stream open requires output");
  s = lc_calloc_with_allocator(NULL, 1, sizeof(*s));
  if (!s)
    return stream_error(error, LC_ERR_NOMEM, "cannot allocate stream owner");
  if (pthread_mutex_init(&s->mutex, NULL)) {
    lc_free_with_allocator(NULL, s);
    return stream_error(error, LC_ERR_TRANSPORT,
                        "cannot initialize stream mutex");
  }
  rc = ps_owned_open(space, budget, key, create, ref, &s->head, &status, error);
  if (rc == LC_OK) {
    s->ref = status.ref;
    s->tail = status.tail;
    s->recovery_floor = status.tail;
    s->sealed = status.sealed;
  }
  for (i = 0; rc == LC_OK && i < status.runs; ++i) {
    rc = stream_grow(s, error);
    if (rc == LC_OK)
      rc = ps_owned_grant(s->head, NULL, s->capacity, 0, &run, &found, error);
    if (rc == LC_OK && !found)
      rc = stream_error(error, LC_ERR_PROTOCOL,
                        "stream directory missing grant");
    if (rc == LC_OK) {
      s->runs[s->count].handle = run;
      s->runs[s->count].base = s->capacity;
      s->runs[s->count].capacity = ps_owned_run_capacity(run);
      s->capacity += ps_owned_run_capacity(run);
      note_grant(s, ps_owned_run_capacity(run));
      ++s->count;
    }
  }
  if (rc == LC_OK && s->capacity != status.capacity)
    rc = stream_error(error, LC_ERR_PROTOCOL,
                      "stream directory capacity mismatch");
  if (rc == LC_OK)
    rc = stream_pieces(s, s->tail, 1, s->sealed, NULL, NULL, error);
  if (rc == LC_OK && s->capacity && !s->sealed) {
    if (!recover)
      rc = stream_error(error, LC_ERR_INVALID,
                        "existing stream requires canonical recovery");
    else {
      s->recovery_buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
      if (!s->recovery_buffer)
        rc = stream_error(error, LC_ERR_NOMEM,
                          "cannot allocate recovery read-ahead");
      s->recovering = 1;
      accepted = s->tail;
      if (rc == LC_OK)
        rc = recover(s, s->tail, s->capacity, &accepted, context, error);
      s->recovering = 0;
      if (rc == LC_OK && (accepted < s->tail || accepted > s->capacity))
        rc = stream_error(error, LC_ERR_PROTOCOL,
                          "canonical recovery returned invalid tail");
      if (rc == LC_OK)
        rc = stream_pieces(s, accepted, 2, 1, s->recovery_buffer, &repaired,
                           error);
      /* Readers need only the validated prefix. Newly accepted bytes and
       * repaired footers join readiness, even if the canonical tail stayed
       * put. Rejected suffix repair waits for the first write admission. */
      if (rc == LC_OK && (accepted > s->tail || repaired))
        rc = ps_owned_commit(s->head, accepted, PS_COMMIT_DEFER_SYNC, NULL,
                             NULL, error);
      if (rc == LC_OK) {
        s->tail = accepted;
        s->repair_pending = 1;
      }
      lc_free_with_allocator(NULL, s->recovery_buffer);
      s->recovery_buffer = NULL;
    }
  }
  if (rc == LC_OK)
    *out = s;
  else
    stream_destroy(s);
  return rc;
}
int ps_stream_close(ps_stream *s, lc_error *error) {
  if (!s)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->appending || s->readers) {
    pthread_mutex_unlock(&s->mutex);
    return stream_error(error, LC_ERR_INVALID,
                        "close stream appenders and readers first");
  }
  pthread_mutex_unlock(&s->mutex);
  stream_destroy(s);
  return LC_OK;
}
int ps_stream_status(ps_stream *s, ps_stream_info *out, lc_error *error) {
  ps_owned_status status;
  int rc;
  if (!s || !out)
    return stream_error(error, LC_ERR_INVALID,
                        "stream status requires owner and output");
  rc = ps_owned_status_get(s->head, &status, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    out->ref = s->ref;
    out->tail = s->tail;
    out->preferred_grant_bytes =
        s->preferred_grant ? s->preferred_grant : PS_PAGE;
    out->capacity = s->capacity;
    out->runs = s->count;
    out->readers = s->readers;
    out->sealed = s->sealed;
    out->retired = s->retired;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}
static int stream_transition(ps_stream *s, int retirement, lc_error *error) {
  ps_owned_status status;
  int rc;
  if (!s)
    return stream_error(error, LC_ERR_INVALID,
                        "stream transition requires owner");
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, error);
  if (rc == LC_OK &&
      (s->appending || s->recovering || (!retirement && s->retired)))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream transition is busy or retired");
  if (rc == LC_OK)
    s->appending = 1;
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    return rc;
  rc = retirement ? ps_owned_retire(s->head, error)
                  : ps_owned_seal(s->head, error);
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK) {
    if (retirement)
      s->retired = 1;
    else
      s->sealed = 1;
  }
  s->appending = 0;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_stream_seal(ps_stream *s, lc_error *error) {
  return stream_transition(s, 0, error);
}
int ps_stream_retire(ps_stream *s, lc_error *error) {
  return stream_transition(s, 1, error);
}
int ps_stream_recovery_read(ps_stream *s, uint64_t offset, void *bytes,
                            size_t n, lc_error *error) {
  unsigned char *p = bytes;
  size_t at, take;
  int rc;
  if (!s || !s->recovering || (!bytes && n) || offset > s->capacity ||
      n > s->capacity - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "candidate read outside canonical recovery");
  while (n) {
    if (offset < s->recovery_offset ||
        offset - s->recovery_offset >= s->recovery_bytes) {
      take = (size_t)(s->capacity - offset);
      if (take > PS_CHUNK)
        take = PS_CHUNK;
      rc = stream_io(s, offset, s->recovery_buffer, take, 0, error);
      if (rc != LC_OK)
        return rc;
      s->recovery_offset = offset;
      s->recovery_bytes = take;
    }
    at = (size_t)(offset - s->recovery_offset);
    take = s->recovery_bytes - at;
    if (take > n)
      take = n;
    memcpy(p, s->recovery_buffer + at, take);
    n -= take;
    p += take;
    offset += take;
  }
  return LC_OK;
}
int ps_stream_begin(ps_stream *s, ps_stream_writer **out, lc_error *error) {
  ps_stream_writer *w;
  ps_owned_status status;
  int rc, repair = 0;
  if (out)
    *out = NULL;
  if (!s || !out)
    return stream_error(error, LC_ERR_INVALID,
                        "stream appender requires owner and output");
  w = lc_calloc_with_allocator(NULL, 1, sizeof(*w));
  if (!w)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot allocate bounded stream appender");
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, error);
  if (rc == LC_OK && (s->appending || s->recovering || s->sealed || s->retired))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream is busy, sealed or retired");
  if (rc == LC_OK) {
    s->appending = 1;
    w->stream = s;
    w->start = s->tail;
    w->prior_checked = s->checked;
    w->prior_checksum = s->checksum;
    repair = s->repair_pending;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && repair) {
    rc = clear_suffix(s, w->start, w->buffer, error);
    if (rc == LC_OK)
      rc = ps_owned_recovered(s->head, w->start, error);
    pthread_mutex_lock(&s->mutex);
    if (rc == LC_OK)
      s->repair_pending = 0;
    else
      s->appending = 0;
    pthread_mutex_unlock(&s->mutex);
  }
  if (rc == LC_OK)
    *out = w;
  else
    lc_free_with_allocator(NULL, w);
  return rc;
}
uint64_t ps_stream_writer_start(const ps_stream_writer *w) {
  return w ? w->start : 0;
}
static int ensure_capacity(ps_stream *s, ps_budget *budget, uint64_t end,
                           int exact, lc_error *error) {
  ps_owned_run *run;
  uint64_t desired;
  int rc = LC_OK, found;
  while (s->capacity < end) {
    pthread_mutex_lock(&s->mutex);
    rc = stream_grow(s, error);
    desired = s->preferred_grant ? s->preferred_grant : PS_PAGE;
    if (exact && end - s->capacity < desired - PS_RUN_HEADER) {
      desired = end - s->capacity + PS_RUN_HEADER;
      desired = (desired + PS_PAGE - 1) / PS_PAGE * PS_PAGE;
    }
    pthread_mutex_unlock(&s->mutex);
    if (rc == LC_OK)
      rc = ps_owned_grant(s->head, budget, s->capacity, desired, &run, &found,
                          error);
    if (rc != LC_OK)
      break;
    pthread_mutex_lock(&s->mutex);
    s->runs[s->count].handle = run;
    s->runs[s->count].base = s->capacity;
    s->runs[s->count].capacity = ps_owned_run_capacity(run);
    s->capacity += ps_owned_run_capacity(run);
    note_grant(s, ps_owned_run_capacity(run));
    ++s->count;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}
static int reserve(ps_stream_writer *w, ps_budget *budget, uint64_t bytes,
                   int exact, lc_error *error) {
  int rc;
  if (!w || w->failed || w->published || bytes > (uint64_t)INT64_MAX - w->start)
    return stream_error(error, LC_ERR_INVALID,
                        "stream reservation arguments invalid");
  rc = ensure_capacity(w->stream, budget, w->start + bytes, exact, error);
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_stream_reserve(ps_stream_writer *w, ps_budget *budget, uint64_t bytes,
                      lc_error *error) {
  return reserve(w, budget, bytes, 0, error);
}
int ps_stream_reserve_exact(ps_stream_writer *w, ps_budget *budget,
                            uint64_t bytes, lc_error *error) {
  return reserve(w, budget, bytes, 1, error);
}
static int stream_flush(ps_stream_writer *w, lc_error *error) {
  int rc;
  if (!w->used)
    return LC_OK;
  rc = ensure_capacity(w->stream, NULL, w->start + w->flushed + w->used, 0,
                       error);
  if (rc == LC_OK)
    rc = stream_io(w->stream, w->start + w->flushed, w->buffer, w->used, 1,
                   error);
  if (rc == LC_OK) {
    w->flushed += w->used;
    w->used = 0;
  }
  return rc;
}
int ps_stream_write(ps_stream_writer *w, const void *bytes, size_t n,
                    lc_error *error) {
  const unsigned char *p = bytes;
  size_t take;
  int rc = LC_OK;
  if (!w || (!bytes && n) || w->failed || w->published ||
      n > (uint64_t)INT64_MAX - w->start - w->bytes)
    return stream_error(error, LC_ERR_INVALID,
                        "stream append arguments invalid");
  while (n && rc == LC_OK) {
    take = PS_CHUNK - w->used;
    if (take > n)
      take = n;
    memcpy(w->buffer + w->used, p, take);
    w->used += take;
    w->bytes += take;
    p += take;
    n -= take;
    if (w->used == PS_CHUNK)
      rc = stream_flush(w, error);
  }
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_stream_write_at(ps_stream_writer *w, uint64_t offset, const void *bytes,
                       size_t n, lc_error *error) {
  const unsigned char *p = bytes;
  size_t take;
  int rc = LC_OK;
  if (!w || (!bytes && n) || w->failed || w->published || offset > w->bytes ||
      n > w->bytes - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "stream patch outside private append");
  if (!n)
    return LC_OK;
  if (offset < w->flushed) {
    take = (size_t)(w->flushed - offset);
    if (take > n)
      take = n;
    rc = stream_io(w->stream, w->start + offset, (void *)p, take, 1, error);
    offset += take;
    n -= take;
    p += take;
  }
  if (rc == LC_OK && n)
    memcpy(w->buffer + (size_t)(offset - w->flushed), p, n);
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_stream_writer_read(ps_stream_writer *w, uint64_t offset, void *bytes,
                          size_t n, lc_error *error) {
  unsigned char *p = bytes;
  size_t take;
  int rc = LC_OK;
  if (!w || (!bytes && n) || w->failed || w->published || offset > w->bytes ||
      n > w->bytes - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "stream read outside private append");
  if (n && offset < w->flushed) {
    take = (size_t)(w->flushed - offset);
    if (take > n)
      take = n;
    rc = stream_io(w->stream, w->start + offset, p, take, 0, error);
    offset += take;
    p += take;
    n -= take;
  }
  if (rc == LC_OK && n)
    memcpy(p, w->buffer + (size_t)(offset - w->flushed), n);
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_stream_writer_crc(ps_stream_writer *w, uint64_t offset, uint64_t length,
                         unsigned long *out, lc_error *error) {
  unsigned char *scratch = NULL;
  unsigned long crc = 0;
  size_t n;
  int rc = LC_OK;
  if (!w || !out || w->failed || w->published || offset > w->bytes ||
      length > w->bytes - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "stream checksum outside private append");
  if (length && offset < w->flushed) {
    scratch = lc_alloc_with_allocator(NULL, PS_CHUNK);
    if (!scratch)
      return stream_error(error, LC_ERR_NOMEM,
                          "cannot allocate private stream CRC buffer");
  }
  while (length && offset < w->flushed && rc == LC_OK) {
    uint64_t left = w->flushed - offset;
    if (left > length)
      left = length;
    n = left < PS_CHUNK ? (size_t)left : PS_CHUNK;
    rc = stream_io(w->stream, w->start + offset, scratch, n, 0, error);
    if (rc == LC_OK)
      crc = (unsigned long)crc32((uLong)crc, scratch, (uInt)n);
    offset += n;
    length -= n;
  }
  if (rc == LC_OK && length)
    crc = (unsigned long)crc32(
        (uLong)crc, w->buffer + (size_t)(offset - w->flushed), (uInt)length);
  lc_free_with_allocator(NULL, scratch);
  if (rc == LC_OK)
    *out = crc;
  return rc;
}
int ps_stream_commit(ps_stream_writer *w, enum ps_commit_mode mode,
                     ps_commit_publish_fn publish, void *context,
                     lc_error *error) {
  int rc;
  if (!w || w->failed || w->published)
    return stream_error(error, LC_ERR_INVALID, "stream appender cannot commit");
  if (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC)
    return stream_error(error, LC_ERR_INVALID, "invalid canonical commit mode");
  rc = stream_flush(w, error);
  if (rc == LC_OK)
    rc = stream_pieces(w->stream, w->start + w->bytes, 0, 1, w->buffer, NULL,
                       error);
  if (rc == LC_OK)
    rc = ps_owned_commit(w->stream->head, w->start + w->bytes, mode, publish,
                         context, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&w->stream->mutex);
    w->stream->tail = w->start + w->bytes;
    w->stream->appending = 0;
    w->published = 1;
    pthread_mutex_unlock(&w->stream->mutex);
  } else
    w->failed = 1;
  return rc;
}
void ps_stream_writer_close(ps_stream_writer *w) {
  ps_owned_status status;
  lc_error error;
  int rc;
  if (!w)
    return;
  if (!w->published) {
    lc_error_init(&error);
    rc = ps_owned_status_get(w->stream->head, &status, &error);
    if (rc == LC_OK && w->flushed) {
      rc = clear_suffix(w->stream, w->start, w->buffer, &error);
      if (rc == LC_OK)
        (void)ps_owned_barrier(w->stream->head, &error);
    }
    lc_error_cleanup(&error);
    pthread_mutex_lock(&w->stream->mutex);
    w->stream->appending = 0;
    w->stream->checked = w->prior_checked;
    w->stream->checksum = w->prior_checksum;
    pthread_mutex_unlock(&w->stream->mutex);
  }
  lc_free_with_allocator(NULL, w);
}
static int window_admit(ps_stream *s, uint64_t offset, uint64_t bytes,
                        ps_stream_window *window, lc_error *error) {
  ps_owned_status status;
  int rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, error);
  if (rc == LC_OK && (s->recovering || s->retired || offset > s->tail ||
                      bytes > s->tail - offset))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream read outside committed prefix");
  if (rc == LC_OK) {
    window->stream = s;
    window->start = offset;
    window->end = offset + bytes;
    ++s->readers;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static void window_release(ps_stream_window *window) {
  pthread_mutex_lock(&window->stream->mutex);
  --window->stream->readers;
  pthread_mutex_unlock(&window->stream->mutex);
}
int ps_stream_window_open(ps_stream *s, uint64_t offset, uint64_t bytes,
                          ps_stream_window **out, lc_error *error) {
  ps_stream_window *window;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out)
    return stream_error(error, LC_ERR_INVALID,
                        "stream window requires owner and output");
  window = lc_calloc_with_allocator(NULL, 1, sizeof(*window));
  if (!window)
    return stream_error(error, LC_ERR_NOMEM, "cannot allocate stream window");
  rc = window_admit(s, offset, bytes, window, error);
  if (rc == LC_OK)
    *out = window;
  else
    lc_free_with_allocator(NULL, window);
  return rc;
}
int ps_stream_window_read(ps_stream_window *window, uint64_t offset,
                          void *bytes, size_t n, lc_error *error) {
  if (!window || (!bytes && n) || offset < window->start ||
      offset > window->end || n > window->end - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "offset read outside captured stream window");
  return stream_io(window->stream, offset, bytes, n, 0, error);
}
void ps_stream_window_close(ps_stream_window *window) {
  if (!window)
    return;
  window_release(window);
  lc_free_with_allocator(NULL, window);
}
int ps_stream_read_open(ps_stream *s, uint64_t offset, uint64_t bytes,
                        ps_stream_reader **out, lc_error *error) {
  ps_stream_reader *reader;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out)
    return stream_error(error, LC_ERR_INVALID,
                        "stream reader requires owner and output");
  reader = lc_calloc_with_allocator(NULL, 1, sizeof(*reader));
  if (!reader)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot allocate bounded stream reader");
  rc = window_admit(s, offset, bytes, &reader->window, error);
  if (rc == LC_OK) {
    reader->offset = offset;
    *out = reader;
  } else
    lc_free_with_allocator(NULL, reader);
  return rc;
}
int ps_stream_read(ps_stream_reader *r, void *bytes, size_t n, size_t *out,
                   lc_error *error) {
  unsigned char *p = bytes;
  size_t take;
  int rc;
  if (out)
    *out = 0;
  if (!r || (!bytes && n) || !out)
    return stream_error(error, LC_ERR_INVALID, "stream read arguments invalid");
  while (n && (r->at < r->used || r->offset < r->window.end)) {
    if (r->at == r->used) {
      take = (size_t)(r->window.end - r->offset);
      if (take > PS_CHUNK)
        take = PS_CHUNK;
      rc = stream_io(r->window.stream, r->offset, r->buffer, take, 0, error);
      if (rc != LC_OK)
        return rc;
      r->at = 0;
      r->used = take;
      r->offset += take;
    }
    take = r->used - r->at;
    if (take > n)
      take = n;
    memcpy(p, r->buffer + r->at, take);
    r->at += take;
    p += take;
    n -= take;
    *out += take;
  }
  return LC_OK;
}
int ps_stream_read_at(ps_stream_reader *r, uint64_t offset, void *bytes,
                      size_t n, lc_error *error) {
  return ps_stream_window_read(r ? &r->window : NULL, offset, bytes, n, error);
}
void ps_stream_reader_close(ps_stream_reader *r) {
  if (!r)
    return;
  window_release(&r->window);
  lc_free_with_allocator(NULL, r);
}
