#include "collection_segment.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include <pthread.h>
#include <string.h>

typedef struct ps_collection_run {
  ps_owned_run *run;
  uint64_t base, bytes;
} ps_collection_run;

struct ps_collection_segment {
  ps_space *space;
  ps_owned_head *head;
  pthread_mutex_t mutex;
  ps_collection_run *runs;
  unsigned char *zero;
  size_t count, room;
  uint64_t tail, capacity, preferred;
  unsigned long references;
  int sealed, recovering, shared, editing;
};

static int cs_error(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}

static int cs_room(ps_collection_segment *s, lc_error *e) {
  size_t room;
  ps_collection_run *runs;
  if (s->count < s->room)
    return LC_OK;
  room = s->room ? s->room * 2U : 4U;
  if (room < s->room || room > (size_t)-1 / sizeof(*runs))
    return cs_error(e, LC_ERR_NOMEM, "collection range directory is too large");
  runs = lc_realloc_with_allocator(NULL, s->runs, room * sizeof(*runs));
  if (!runs)
    return cs_error(e, LC_ERR_NOMEM,
                    "cannot allocate collection range directory");
  s->runs = runs;
  s->room = room;
  return LC_OK;
}

static void cs_add(ps_collection_segment *s, ps_owned_run *run) {
  uint64_t bytes = ps_owned_run_capacity(run);
  s->runs[s->count].run = run;
  s->runs[s->count].base = ps_owned_run_base(run);
  s->runs[s->count].bytes = bytes;
  if (s->capacity < s->runs[s->count].base + bytes)
    s->capacity = s->runs[s->count].base + bytes;
  ++s->count;
  bytes = (bytes + PS_RUN_HEADER) * 4U;
  if (bytes > PS_MIB)
    bytes = PS_MIB;
  if (s->preferred < bytes)
    s->preferred = bytes;
}

/* Copy immutable mapping facts while briefly holding the directory mutex.
 * The owner pin keeps each run alive after vector growth and during I/O. */
static int cs_mapping(ps_collection_segment *s, uint64_t at,
                      ps_collection_run *run, lc_error *e) {
  size_t lo = 0U, hi, mid;
  pthread_mutex_lock(&s->mutex);
  hi = s->count;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2U;
    if (s->runs[mid].base + s->runs[mid].bytes <= at)
      lo = mid + 1U;
    else
      hi = mid;
  }
  if (lo == s->count || at < s->runs[lo].base) {
    pthread_mutex_unlock(&s->mutex);
    return cs_error(e, LC_ERR_PROTOCOL,
                    "collection canonical range has no physical owner");
  }
  *run = s->runs[lo];
  if (s->shared)
    ps_owned_run_retain(run->run);
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}

static int cs_transfer(ps_collection_segment *s, uint64_t at, void *bytes,
                       size_t n, int writing, lc_error *e) {
  ps_collection_run run = {0};
  unsigned char *p = bytes;
  size_t take;
  int rc;
  while (n) {
    rc = cs_mapping(s, at, &run, e);
    if (rc != LC_OK)
      return rc;
    take = run.bytes - (at - run.base) < (uint64_t)n
               ? (size_t)(run.bytes - (at - run.base))
               : n;
    rc = writing ? ps_owned_write(run.run, at - run.base, p, take, e)
                 : ps_owned_read(run.run, at - run.base, p, take, e);
    if (s->shared)
      ps_owned_run_close(run.run);
    if (rc != LC_OK)
      return rc;
    at += take;
    p += take;
    n -= take;
  }
  return LC_OK;
}

/* Called with allocation/append authority; private payload writes do not
 * hold this mutex while performing physical I/O. */
static int cs_grant(ps_collection_segment *s, uint64_t end, lc_error *e) {
  ps_owned_run *run;
  int found, rc;
  while (s->capacity < end) {
    rc = cs_room(s, e);
    if (rc != LC_OK)
      return rc;
    run = NULL;
    rc = ps_owned_grant(s->head, NULL, s->capacity, s->preferred, &run, &found,
                        e);
    if (rc != LC_OK)
      return rc;
    if (!found || !run)
      return cs_error(e, LC_ERR_PROTOCOL,
                      "collection reservation returned no owned range");
    cs_add(s, run);
  }
  return LC_OK;
}

static int cs_clear(ps_collection_segment *s, uint64_t start, uint64_t end,
                    lc_error *e) {
  size_t i;
  uint64_t from, through;
  int rc = LC_OK;
  for (i = 0U; i < s->count && rc == LC_OK; ++i) {
    ps_collection_run *r = &s->runs[i];
    if (end <= r->base || start >= r->base + r->bytes)
      continue;
    from = start > r->base ? start - r->base : 0U;
    through = end < r->base + r->bytes ? end - r->base : r->bytes;
    rc = ps_owned_clear(r->run, from, through - from, s->zero, e);
  }
  return rc;
}

static int cs_read(void *context, uint64_t at, const struct iovec *parts,
                   int count, lc_error *e) {
  ps_collection_segment *s = context;
  uint64_t limit;
  int i, rc = LC_OK;
  pthread_mutex_lock(&s->mutex);
  limit = s->recovering ? s->capacity : s->tail;
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (at > limit || (uint64_t)parts[i].iov_len > limit - at)
      return cs_error(e, LC_ERR_PROTOCOL,
                      "collection read exceeds its captured canonical range");
    rc = cs_transfer(s, at, parts[i].iov_base, parts[i].iov_len, 0, e);
    at += parts[i].iov_len;
  }
  return rc;
}

static int cs_patch(void *context, uint64_t at, const struct iovec *parts,
                    int count, lc_error *e) {
  ps_collection_segment *s = context;
  uint64_t limit;
  int i, rc = LC_OK;
  pthread_mutex_lock(&s->mutex);
  limit = s->tail;
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; i < count && rc == LC_OK; ++i) {
    if (at > limit || (uint64_t)parts[i].iov_len > limit - at)
      return cs_error(e, LC_ERR_INVALID,
                      "collection patch exceeds its private canonical range");
    rc = cs_transfer(s, at, parts[i].iov_base, parts[i].iov_len, 1, e);
    at += parts[i].iov_len;
  }
  return rc;
}

static int cs_tail(void *context, uint64_t *tail, lc_error *e) {
  ps_collection_segment *s = context;
  (void)e;
  pthread_mutex_lock(&s->mutex);
  *tail = s->tail;
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}

static int cs_append(void *context, const struct iovec *parts, int count,
                     lc_error *e) {
  ps_collection_segment *s = context;
  uint64_t end, at;
  int i, rc;
  pthread_mutex_lock(&s->mutex);
  at = end = s->tail;
  for (i = 0; i < count; ++i) {
    if ((uint64_t)parts[i].iov_len > (uint64_t)LC_I64_MAX - end) {
      pthread_mutex_unlock(&s->mutex);
      return cs_error(e, LC_ERR_INVALID,
                      "collection canonical segment exceeds its offset range");
    }
    end += parts[i].iov_len;
  }
  rc = s->sealed ? cs_error(e, LC_ERR_INVALID,
                            "cannot append to a sealed collection segment")
                 : LC_OK;
  if (rc == LC_OK && s->shared && !s->editing) {
    rc = ps_owned_mark_edit(s->head, e);
    s->editing = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = cs_grant(s, end, e);
  if (rc == LC_OK)
    s->tail = end;
  pthread_mutex_unlock(&s->mutex);
  /* Logical append authority excludes other appenders. Mapping capture and
   * private range writes remain independent of this bounded metadata append. */
  for (i = 0; i < count && rc == LC_OK; ++i) {
    rc = cs_transfer(s, at, parts[i].iov_base, parts[i].iov_len, 1, e);
    at += parts[i].iov_len;
  }
  return rc;
}

static void cs_discard(void *context, uint64_t at) {
  lc_error e;
  lc_error_init(&e);
  (void)ps_collection_segment_truncate(context, at, &e);
  lc_error_cleanup(&e);
}

static const lc_pouch_record_io_ops cs_ops = {cs_read, cs_append, cs_patch,
                                              cs_tail, cs_discard};
static const lc_pouch_record_io_ops cs_view_ops = {cs_read, NULL, NULL, cs_tail,
                                                   NULL};

lc_pouch_record_io ps_collection_segment_io(ps_collection_segment *s) {
  lc_pouch_record_io io;
  io.ops = s ? (s->head ? &cs_ops : &cs_view_ops) : NULL;
  io.context = s;
  return io;
}

static int cs_new(ps_space *space, int writable, ps_collection_segment **out,
                  lc_error *e) {
  ps_collection_segment *s;
  s = lc_calloc_with_allocator(NULL, 1U, sizeof(*s));
  if (!s)
    return cs_error(e, LC_ERR_NOMEM, "cannot allocate collection segment");
  if (writable)
    s->zero = lc_calloc_with_allocator(NULL, 1U, PS_CHUNK);
  if (writable && !s->zero) {
    lc_free_with_allocator(NULL, s->zero);
    lc_free_with_allocator(NULL, s);
    return cs_error(e, LC_ERR_NOMEM,
                    "cannot allocate collection recovery scratch");
  }
  if (pthread_mutex_init(&s->mutex, NULL) != 0) {
    lc_free_with_allocator(NULL, s->zero);
    lc_free_with_allocator(NULL, s);
    return cs_error(e, LC_ERR_TRANSPORT,
                    "cannot initialize collection canonical owner");
  }
  s->preferred = PS_PAGE;
  s->space = space;
  s->shared = ps_coordination_shared(ps_space_coordination(space));
  s->references = 1UL;
  *out = s;
  return LC_OK;
}

static int cs_load(ps_collection_segment *s, ps_owned_head *head,
                   ps_owned_status *status, lc_error *e) {
  ps_owned_run **captured = NULL;
  size_t count = 0U, i;
  int rc = ps_owned_runs_capture_status(head, status, &captured, &count, e);
  for (i = 0U; rc == LC_OK && i < count; ++i) {
    rc = cs_room(s, e);
    if (rc == LC_OK &&
        ((!status->sealed && ps_owned_run_base(captured[i]) != s->capacity) ||
         ps_owned_run_base(captured[i]) < s->capacity ||
         ps_owned_run_base(captured[i]) > status->capacity ||
         ps_owned_run_capacity(captured[i]) >
             status->capacity - ps_owned_run_base(captured[i])))
      rc = cs_error(e, LC_ERR_PROTOCOL,
                    "collection segment has an invalid owned range");
    if (rc == LC_OK) {
      cs_add(s, captured[i]);
      captured[i] = NULL;
    }
  }
  for (i = 0U; i < count; ++i)
    ps_owned_run_close(captured[i]);
  lc_free_with_allocator(NULL, captured);
  if (rc == LC_OK && ((!status->sealed && s->capacity != status->capacity) ||
                      s->count != status->runs))
    rc = cs_error(e, LC_ERR_PROTOCOL,
                  "collection owned range directory is inconsistent");
  if (rc == LC_OK) {
    s->capacity = status->capacity;
    s->tail = status->tail;
    s->sealed = status->sealed;
  }
  return rc;
}

static int cs_recover(ps_collection_segment *s, const ps_owned_status *status,
                      ps_collection_segment_recover_fn recover, void *context,
                      lc_error *e) {
  uint64_t accepted = status->tail;
  int rc;
  pthread_mutex_lock(&s->mutex);
  s->recovering = 1;
  pthread_mutex_unlock(&s->mutex);
  rc = recover(ps_collection_segment_io(s), status->tail, status->capacity,
               &accepted, context, e);
  pthread_mutex_lock(&s->mutex);
  s->recovering = 0;
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && (accepted < status->tail || accepted > status->capacity))
    rc = cs_error(e, LC_ERR_PROTOCOL,
                  "collection recovery returned an invalid boundary");
  if (rc == LC_OK && accepted < status->capacity)
    rc = cs_clear(s, accepted, status->capacity, e);
  if (rc == LC_OK)
    rc = ps_owned_recovered(s->head, accepted, e);
  if (rc == LC_OK && s->shared)
    rc = ps_owned_commit_canonical(s->head, accepted, PS_COMMIT_DEFER_SYNC,
                                   NULL, NULL, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    s->tail = accepted;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}

int ps_collection_segment_open(ps_space *space, ps_key key, int create,
                               const ps_ref *expected,
                               ps_collection_segment_recover_fn recover,
                               void *context, ps_collection_segment **out,
                               lc_error *e) {
  ps_collection_segment *s = NULL;
  ps_owned_status status;
  int rc;
  if (out)
    *out = NULL;
  if (!space || !out)
    return cs_error(e, LC_ERR_INVALID,
                    "collection canonical owner requires a space and output");
  rc = cs_new(space, 1, &s, e);
  if (rc == LC_OK)
    rc = create || expected
             ? ps_owned_open(space, NULL, key, create, expected, &s->head,
                             &status, e)
             : ps_owned_open_prepared(space, key, &s->head, &status, e);
  if (rc == LC_OK)
    rc = cs_load(s, s->head, &status, e);
  if (rc == LC_OK && !create && !status.sealed && recover)
    rc = cs_recover(s, &status, recover, context, e);
  if (rc != LC_OK) {
    ps_collection_segment_close(s);
    return rc;
  }
  *out = s;
  return LC_OK;
}

int ps_collection_segment_view(ps_collection_segment *source,
                               ps_collection_segment **out, lc_error *e) {
  ps_collection_segment *view = NULL;
  ps_owned_status status;
  int rc = cs_new(source->space, 0, &view, e);
  if (rc == LC_OK)
    rc = cs_load(view, source->head, &status, e);
  if (rc != LC_OK) {
    ps_collection_segment_close(view);
    return rc;
  }
  *out = view;
  return LC_OK;
}

int ps_collection_segment_refresh(ps_collection_segment *s,
                                  ps_collection_segment_recover_fn recover,
                                  void *context, lc_error *e) {
  ps_collection_segment *view = NULL;
  ps_collection_run *old;
  ps_owned_status status;
  size_t count;
  int reload, rc;
  rc = ps_owned_status_capture(s->head, &status, e);
  if (rc != LC_OK)
    return rc;
  if (status.retired || status.sealed)
    return cs_error(e, LC_ERR_PROTOCOL, "selected collection writer is closed");
  pthread_mutex_lock(&s->mutex);
  reload = s->count != status.runs || s->capacity != status.capacity;
  pthread_mutex_unlock(&s->mutex);
  if (reload) {
    rc = cs_new(s->space, 0, &view, e);
    if (rc == LC_OK)
      rc = cs_load(view, s->head, &status, e);
    if (rc != LC_OK) {
      ps_collection_segment_close(view);
      return rc;
    }
    pthread_mutex_lock(&s->mutex);
    old = s->runs;
    count = s->count;
    s->runs = view->runs;
    s->count = view->count;
    s->room = view->room;
    s->capacity = view->capacity;
    s->preferred = view->preferred;
    view->runs = old;
    view->count = count;
    pthread_mutex_unlock(&s->mutex);
    ps_collection_segment_close(view);
  }
  if (!status.complete && status.capacity)
    rc = cs_recover(s, &status, recover, context, e);
  else {
    pthread_mutex_lock(&s->mutex);
    s->tail = status.tail;
    pthread_mutex_unlock(&s->mutex);
  }
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    s->editing = 0;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}

void ps_collection_segment_retain(ps_collection_segment *s) {
  if (s)
    (void)__atomic_add_fetch(&s->references, 1UL, __ATOMIC_RELAXED);
}

void ps_collection_segment_close(ps_collection_segment *s) {
  size_t i;
  if (!s || __atomic_sub_fetch(&s->references, 1UL, __ATOMIC_ACQ_REL))
    return;
  for (i = 0U; i < s->count; ++i)
    ps_owned_run_close(s->runs[i].run);
  ps_owned_close(s->head);
  pthread_mutex_destroy(&s->mutex);
  lc_free_with_allocator(NULL, s->runs);
  lc_free_with_allocator(NULL, s->zero);
  lc_free_with_allocator(NULL, s);
}

int ps_collection_segment_status(ps_collection_segment *s,
                                 ps_owned_status *status, uint64_t *tail,
                                 lc_error *e) {
  int rc = s->shared ? ps_owned_status_capture(s->head, status, e)
                     : ps_owned_status_get(s->head, status, e);
  if (rc == LC_OK && tail)
    rc = cs_tail(s, tail, e);
  return rc;
}

int ps_collection_segment_reserve(ps_collection_segment *s, uint64_t end,
                                  lc_error *e) {
  int rc;
  pthread_mutex_lock(&s->mutex);
  rc = end < s->tail || end > (uint64_t)LC_I64_MAX || s->sealed
           ? cs_error(e, LC_ERR_INVALID,
                      "invalid collection canonical reservation")
           : cs_grant(s, end, e);
  if (rc == LC_OK)
    s->tail = end;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}

int ps_collection_segment_truncate(ps_collection_segment *s, uint64_t end,
                                   lc_error *e) {
  ps_owned_status status;
  int rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, e);
  if (rc == LC_OK && (end < status.tail || end > s->tail || s->sealed))
    rc = cs_error(e, LC_ERR_INVALID,
                  "cannot truncate a published collection canonical range");
  if (rc == LC_OK && end < s->tail)
    rc = cs_clear(s, end, s->tail, e);
  if (rc == LC_OK && end < s->tail)
    rc = ps_owned_barrier(s->head, e);
  if (rc == LC_OK)
    s->tail = end;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}

int ps_collection_segment_publish(ps_collection_segment *s,
                                  enum ps_commit_mode mode, lc_error *e) {
  uint64_t tail;
  int rc;
  (void)cs_tail(s, &tail, e);
  if (!s->shared)
    return ps_owned_commit(s->head, tail, mode, NULL, NULL, e);
  rc = ps_owned_commit_canonical(s->head, tail, mode, NULL, NULL, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    s->editing = 0;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}

int ps_collection_segment_sync(ps_collection_segment *s, lc_error *e) {
  return ps_owned_barrier(s->head, e);
}

int ps_collection_segment_seal(ps_collection_segment *s, lc_error *e) {
  int rc = ps_collection_segment_publish(s, PS_COMMIT_DEFER_SYNC, e);
  if (rc == LC_OK)
    rc = ps_owned_seal(s->head, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    s->sealed = 1;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}

int ps_collection_segment_claim_orphan(ps_collection_segment *s, lc_error *e) {
  return ps_owned_claim_orphan(s->head, e);
}
int ps_collection_segment_retire(ps_collection_segment *s, lc_error *e) {
  return ps_owned_retire(s->head, e);
}

int ps_collection_segment_discard_prefix(ps_collection_segment *s,
                                         uint64_t through,
                                         ps_piece_select_fn select,
                                         void *context, uint64_t *removed,
                                         lc_error *e) {
  ps_owned_run *replacement = NULL;
  ps_collection_run *r;
  uint64_t bytes = 0U;
  size_t i = 0U;
  int rc;
  if (!s || !removed || !select || !s->sealed || through > s->tail)
    return cs_error(e, LC_ERR_INVALID,
                    "collection prefix disposal requires sealed liveness");
  while (i < s->count) {
    r = &s->runs[i];
    if (through < s->tail && r->base + r->bytes > through)
      break;
    rc = ps_owned_run_repack(s->head, r->run, NULL, NULL, 0U, select, context,
                             &replacement, e);
    if (rc != LC_OK)
      return rc;
    bytes += ps_owned_run_storage(r->run);
    ps_owned_run_close(r->run);
    memmove(r, r + 1U, (s->count - i - 1U) * sizeof(*r));
    --s->count;
  }
  *removed = bytes;
  return LC_OK;
}

static int cs_owner_clone(void *context, lc_collection_segment *out,
                          lc_error *e) {
  (void)e;
  ps_collection_segment_bind(context, out);
  return LC_OK;
}
static void cs_owner_close(void *context) {
  ps_collection_segment_close(context);
}
static int cs_owner_reserve(void *context, lc_u64 end, lc_error *e) {
  return ps_collection_segment_reserve(context, end, e);
}
static int cs_owner_truncate(void *context, lc_u64 end, lc_error *e) {
  return ps_collection_segment_truncate(context, end, e);
}
static int cs_owner_patch(void *context, lc_u64 at, const void *bytes,
                          size_t count, lc_error *e) {
  struct iovec part;
  part.iov_base = (void *)bytes;
  part.iov_len = count;
  return lc_pouch_record_io_patch(ps_collection_segment_io(context), at, &part,
                                  1, e);
}
static int cs_owner_sync(void *context, lc_error *e) {
  return ps_collection_segment_sync(context, e);
}
static int cs_owner_publish(void *context, lc_pouch *pouch, lc_error *e) {
  ps_collection_segment *s = context;
  return pouch->durable_sync ? ps_flush(s->space, e) : ps_check(s->space, e);
}
static int cs_owner_ready(void *context, lc_error *e) {
  return ps_collection_segment_publish(context, PS_COMMIT_DEFER_SYNC, e);
}
static int cs_owner_seal(void *context, lc_error *e) {
  int rc = ps_collection_segment_publish(context, PS_COMMIT_DEFAULT, e);
  return rc == LC_OK ? ps_collection_segment_seal(context, e) : rc;
}
void ps_collection_segment_bind(ps_collection_segment *segment,
                                lc_collection_segment *out) {
  static const lc_collection_segment_ops ops = {
      cs_owner_clone, cs_owner_close, cs_owner_reserve, cs_owner_truncate,
      cs_owner_patch, cs_owner_sync,  cs_owner_ready,   cs_owner_publish,
      cs_owner_sync,  cs_owner_seal};
  static const lc_collection_segment_ops view_ops = {
      cs_owner_clone, cs_owner_close, NULL, NULL, NULL,
      NULL,           NULL,           NULL, NULL, NULL};
  memset(out, 0, sizeof(*out));
  if (!segment)
    return;
  out->ops = segment->head ? &ops : &view_ops;
  out->context = segment;
  out->io = out->unit_io = ps_collection_segment_io(segment);
  ps_collection_segment_retain(segment);
}
