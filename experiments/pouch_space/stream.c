#include "stream.h"
#include "lc_api_internal.h"
#include "owned.h"

#include <pthread.h>
#include <string.h>

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
  uint64_t tail, capacity;
  unsigned char *recovery_buffer;
  uint64_t recovery_offset;
  size_t recovery_bytes;
  int appending, recovering, sealed, retired;
};
struct ps_stream_writer {
  ps_stream *stream;
  uint64_t start, bytes, flushed;
  size_t used;
  int failed, published;
  unsigned char buffer[PS_CHUNK];
};
struct ps_stream_reader {
  ps_stream *stream;
  uint64_t offset, end;
  size_t at, used;
  unsigned char buffer[PS_CHUNK];
};
static int stream_error(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-stream");
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
static int clear_suffix(ps_stream *s, uint64_t offset, unsigned char *buffer,
                        lc_error *error) {
  unsigned char *zero = buffer;
  stream_run run;
  uint64_t n;
  int rc = LC_OK;
  if (!zero)
    zero = lc_calloc_with_allocator(NULL, 1, PS_CHUNK);
  else
    memset(zero, 0, PS_CHUNK);
  if (!zero)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot allocate stream repair buffer");
  while (offset < s->capacity && rc == LC_OK) {
    rc = stream_mapping(s, offset, &run, error);
    if (rc == LC_OK) {
      n = run.base + run.capacity - offset;
      rc = ps_owned_clear(run.handle, offset - run.base, n, zero, error);
      offset += n;
    }
  }
  if (!buffer)
    lc_free_with_allocator(NULL, zero);
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
int ps_stream_open(ps_space *space, ps_key key, int create, const ps_ref *ref,
                   ps_stream_recover_fn recover, void *context, ps_stream **out,
                   lc_error *error) {
  ps_owned_status status;
  ps_owned_run *run = NULL;
  ps_stream *s;
  uint64_t i, accepted;
  int rc, found;
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
  rc = ps_owned_open(space, key, create, ref, &s->head, &status, error);
  if (rc == LC_OK) {
    s->ref = status.ref;
    s->tail = status.tail;
    s->sealed = status.sealed;
  }
  for (i = 0; rc == LC_OK && i < status.runs; ++i) {
    rc = stream_grow(s, error);
    if (rc == LC_OK)
      rc = ps_owned_grant(s->head, s->capacity, 0, &run, &found, error);
    if (rc == LC_OK && !found)
      rc = stream_error(error, LC_ERR_PROTOCOL,
                        "stream directory missing grant");
    if (rc == LC_OK) {
      s->runs[s->count].handle = run;
      s->runs[s->count].base = s->capacity;
      s->runs[s->count].capacity = ps_owned_run_capacity(run);
      s->capacity += ps_owned_run_capacity(run);
      ++s->count;
    }
  }
  if (rc == LC_OK && s->capacity != status.capacity)
    rc = stream_error(error, LC_ERR_PROTOCOL,
                      "stream directory capacity mismatch");
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
      lc_free_with_allocator(NULL, s->recovery_buffer);
      s->recovery_buffer = NULL;
      if (rc == LC_OK && (accepted < s->tail || accepted > s->capacity))
        rc = stream_error(error, LC_ERR_PROTOCOL,
                          "canonical recovery returned invalid tail");
      if (rc == LC_OK)
        rc = clear_suffix(s, accepted, NULL, error);
      if (rc == LC_OK)
        rc = ps_owned_recovered(s->head, accepted, error);
      if (rc == LC_OK)
        s->tail = accepted;
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
    out->capacity = s->capacity;
    out->runs = s->count;
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
  int rc;
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
    *out = w;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, w);
  return rc;
}
uint64_t ps_stream_writer_start(const ps_stream_writer *w) {
  return w ? w->start : 0;
}
static int ensure_capacity(ps_stream *s, uint64_t end, lc_error *error) {
  ps_owned_run *run;
  uint64_t desired;
  int rc = LC_OK, found;
  while (s->capacity < end) {
    pthread_mutex_lock(&s->mutex);
    rc = stream_grow(s, error);
    desired = s->count ? (s->runs[s->count - 1].capacity + PS_RUN_HEADER) * 4
                       : PS_PAGE;
    if (desired > PS_MIB)
      desired = PS_MIB;
    pthread_mutex_unlock(&s->mutex);
    if (rc == LC_OK)
      rc = ps_owned_grant(s->head, s->capacity, desired, &run, &found, error);
    if (rc != LC_OK)
      break;
    pthread_mutex_lock(&s->mutex);
    s->runs[s->count].handle = run;
    s->runs[s->count].base = s->capacity;
    s->runs[s->count].capacity = ps_owned_run_capacity(run);
    s->capacity += ps_owned_run_capacity(run);
    ++s->count;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}
static int stream_flush(ps_stream_writer *w, lc_error *error) {
  int rc;
  if (!w->used)
    return LC_OK;
  rc = ensure_capacity(w->stream, w->start + w->flushed + w->used, error);
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
int ps_stream_commit(ps_stream_writer *w, lc_error *error) {
  int rc;
  if (!w || w->failed || w->published)
    return stream_error(error, LC_ERR_INVALID, "stream appender cannot commit");
  rc = stream_flush(w, error);
  if (rc == LC_OK)
    rc = ps_owned_commit(w->stream->head, w->start + w->bytes, error);
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
    pthread_mutex_unlock(&w->stream->mutex);
  }
  lc_free_with_allocator(NULL, w);
}
int ps_stream_read_open(ps_stream *s, uint64_t offset, uint64_t bytes,
                        ps_stream_reader **out, lc_error *error) {
  ps_stream_reader *r;
  ps_owned_status status;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out)
    return stream_error(error, LC_ERR_INVALID,
                        "stream reader requires owner and output");
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot allocate bounded stream reader");
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, error);
  if (rc == LC_OK && (s->recovering || s->retired || offset > s->tail ||
                      bytes > s->tail - offset))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream read outside committed prefix");
  if (rc == LC_OK) {
    r->stream = s;
    r->offset = offset;
    r->end = offset + bytes;
    ++s->readers;
    *out = r;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, r);
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
  while (n && (r->at < r->used || r->offset < r->end)) {
    if (r->at == r->used) {
      take = (size_t)(r->end - r->offset);
      if (take > PS_CHUNK)
        take = PS_CHUNK;
      rc = stream_io(r->stream, r->offset, r->buffer, take, 0, error);
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
void ps_stream_reader_close(ps_stream_reader *r) {
  if (!r)
    return;
  pthread_mutex_lock(&r->stream->mutex);
  --r->stream->readers;
  pthread_mutex_unlock(&r->stream->mutex);
  lc_free_with_allocator(NULL, r);
}
