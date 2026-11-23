#include "stream.h"
#include "crc.h"
#include "lc_api_internal.h"
#include "owned.h"

#include <pthread.h>
#include <string.h>
#include <zlib.h>

enum stream_mappings { STREAM_FULL, STREAM_PARKED, STREAM_SUFFIX };
struct ps_stream {
  ps_owned_head *head;
  ps_space *space;
  ps_key key;
  ps_ref ref;
  pthread_mutex_t mutex;
  ps_owned_mapping *runs;
  size_t count, allocated, readers;
  uint64_t tail, capacity, preferred_grant, checked, edit_wire;
  ps_piece_checksum checksum;
  uint64_t recovery_floor;
  unsigned char *recovery_buffer;
  uint64_t recovery_offset;
  size_t recovery_bytes;
  ps_stream_recover_fn recover;
  void *recovery_context;
  enum stream_mappings mappings;
  int edit_valid;
  int appending, recovering, repair_pending, sealed, retired, remapping;
  int detached;
};
struct ps_stream_writer {
  ps_stream *stream;
  ps_coordination_guard *edit;
  uint64_t start, bytes, flushed, prior_checked;
  ps_piece_checksum prior_checksum;
  size_t used;
  int failed, published, wrote;
  unsigned char buffer[PS_CHUNK];
};
struct ps_stream_window {
  ps_stream *stream;
  uint64_t start, end;
  ps_owned_mapping *runs;
  size_t count;
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
  if (next > PS_STREAM_PIECE_BYTES)
    next = PS_STREAM_PIECE_BYTES;
  if (s->preferred_grant < next)
    s->preferred_grant = next;
}
static int stream_grow(ps_stream *s, lc_error *error) {
  size_t n;
  ps_owned_mapping *runs;
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
static size_t mapping_lower(const ps_owned_mapping *runs, size_t count,
                            uint64_t offset) {
  size_t lo = 0, hi = count, mid;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2;
    if (runs[mid].base + runs[mid].capacity <= offset)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}
static int stream_mapping_next(ps_stream *s, uint64_t offset,
                               ps_owned_mapping *out, size_t *at) {
  int found;
  pthread_mutex_lock(&s->mutex);
  *at = mapping_lower(s->runs, s->count, offset);
  found = *at < s->count;
  if (found) {
    *out = s->runs[*at];
    ps_owned_run_retain(out->handle);
  }
  pthread_mutex_unlock(&s->mutex);
  return found;
}
static int stream_mapping(ps_stream *s, uint64_t offset, ps_owned_mapping *out,
                          lc_error *error) {
  size_t lo;
  pthread_mutex_lock(&s->mutex);
  lo = mapping_lower(s->runs, s->count, offset);
  if (lo == s->count || offset < s->runs[lo].base) {
    pthread_mutex_unlock(&s->mutex);
    return stream_error(error, LC_ERR_PROTOCOL, "stream range has no grant");
  }
  *out = s->runs[lo];
  ps_owned_run_retain(out->handle);
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}
static int stream_io(ps_stream *s, uint64_t offset, void *bytes, size_t n,
                     int writing, lc_error *error) {
  unsigned char *p = bytes;
  ps_owned_mapping run;
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
    ps_owned_run_close(run.handle);
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
                         const ps_owned_footer *footers, void *scratch,
                         int *repaired, lc_error *error) {
  ps_owned_mapping run = {0};
  uint64_t bytes, end, prior;
  size_t at;
  ps_piece_checksum checksum, rebuilt = {0};
  int rc, found;
  while (s->checked < tail) {
    found = stream_mapping_next(s, s->checked, &run, &at);
    if (!found || run.base >= tail) {
      if (found)
        ps_owned_run_close(run.handle);
      s->checked = tail;
      memset(&s->checksum, 0, sizeof(s->checksum));
      break;
    }
    if (s->checked < run.base) {
      s->checked = run.base;
      memset(&s->checksum, 0, sizeof(s->checksum));
    }
    end = run.base + run.capacity;
    if (end > tail && !partial) {
      ps_owned_run_close(run.handle);
      break;
    }
    bytes = end > tail ? tail - run.base : run.capacity;
    prior = s->checked - run.base;
    if (inspect)
      rc = footers
               ? ps_owned_piece_inspect_initial(run.handle, bytes, &footers[at],
                                                &checksum, error)
               : ps_owned_piece_inspect(run.handle, bytes, &checksum, error);
    else
      rc = ps_owned_piece_extend(run.handle, prior, prior ? &s->checksum : NULL,
                                 bytes, scratch, &checksum, error);
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
    ps_owned_run_close(run.handle);
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
  return stream_pieces(s, covered, 1, 0, NULL, NULL, NULL, error);
}
static int clear_suffix(ps_stream *s, uint64_t offset, unsigned char *zero,
                        lc_error *error) {
  ps_owned_mapping run;
  uint64_t n;
  int rc = LC_OK;
  memset(zero, 0, PS_CHUNK);
  while (offset < s->capacity && rc == LC_OK) {
    rc = stream_mapping(s, offset, &run, error);
    if (rc == LC_OK) {
      n = run.base + run.capacity - offset;
      rc = ps_owned_clear(run.handle, offset - run.base, n, zero, error);
      ps_owned_run_close(run.handle);
      offset += n;
    }
  }
  return rc;
}
static void stream_destroy(ps_stream *s) {
  ps_owned_runs_close(s->runs, s->count);
  ps_owned_close(s->head);
  (void)pthread_mutex_destroy(&s->mutex);
  lc_free_with_allocator(NULL, s);
}
static void install_mappings(ps_stream *s, ps_owned_mapping *runs, size_t count,
                             enum stream_mappings mappings) {
  size_t i;
  for (i = 0; i < count; ++i)
    note_grant(s, runs[i].capacity);
  ps_owned_runs_close(s->runs, s->count);
  s->runs = runs;
  s->allocated = s->count = count;
  s->mappings = mappings;
}
/* Namespace owners can keep validated scalar state independently of the
 * physical handles. Under the same edit owner, only the incomplete piece and
 * later grants are needed for an append. Full capture remains the recovery
 * and maintenance path. */
static int capture_mappings(ps_stream *s, enum stream_mappings mappings,
                            ps_owned_footer **footers, lc_error *error) {
  ps_owned_mapping *runs = NULL;
  size_t count = 0;
  int rc = LC_OK;
  if (mappings == STREAM_FULL)
    rc = footers ? ps_owned_runs_capture_initial(s->head, &runs, &count,
                                                 footers, error)
                 : ps_owned_runs_capture(s->head, &runs, &count, error);
  else if (s->checked < s->capacity)
    rc = ps_owned_runs_edit_range(s->head, s->checked, s->capacity, &runs,
                                  &count, error);
  if (rc == LC_OK)
    install_mappings(s, runs, count, mappings);
  return rc;
}
/* Load under the stream's edit guard for a shared space. The target either
 * has no callers yet or is a private view staged for a handoff. */
static int stream_load(ps_stream *s, const ps_owned_status *status,
                       ps_stream_recover_fn recover, void *context,
                       const ps_owned_initial *initial, lc_error *error) {
  ps_owned_footer *footers = NULL;
  uint64_t accepted;
  int rc = LC_OK, repaired = 0, recover_tail = 0;
  s->ref = status->ref;
  s->tail = s->recovery_floor = status->tail;
  s->sealed = status->sealed;
  s->edit_wire = status->edit_wire;
  if (initial) {
    install_mappings(s, initial->runs, initial->count, STREAM_FULL);
    footers = initial->footers;
  } else
    rc = capture_mappings(s, STREAM_FULL, &footers, error);
  if (rc == LC_OK)
    s->capacity = status->capacity;
  if (rc == LC_OK)
    rc = stream_pieces(s, s->tail, 1, s->sealed, footers, NULL, NULL, error);
  if (rc == LC_OK && s->capacity && !s->sealed) {
    if (!recover)
      rc = stream_error(error, LC_ERR_INVALID,
                        "existing stream requires canonical recovery");
    else if (!status->complete)
      recover_tail = 1;
    else {
      rc = stream_pieces(s, s->tail, 1, 1, footers, NULL, NULL, error);
      if (rc == LC_ERR_PROTOCOL) {
        /* Canonical validation can rebuild an uncovered partial footer. */
        lc_error_cleanup(error);
        rc = LC_OK;
        recover_tail = 1;
      }
    }
  }
  lc_free_with_allocator(NULL, footers);
  if (!recover_tail)
    return rc;
  s->recovery_buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
  if (!s->recovery_buffer)
    return stream_error(error, LC_ERR_NOMEM,
                        "cannot allocate recovery read-ahead");
  s->recovering = 1;
  accepted = s->tail;
  rc = recover(s, s->tail, s->capacity, &accepted, context, error);
  s->recovering = 0;
  if (rc == LC_OK && (accepted < s->tail || accepted > s->capacity))
    rc = stream_error(error, LC_ERR_PROTOCOL,
                      "canonical recovery returned invalid tail");
  if (rc == LC_OK)
    rc = stream_pieces(s, accepted, 2, 1, NULL, s->recovery_buffer, &repaired,
                       error);
  /* Rejected suffix repair waits for the first write admission. */
  if (rc == LC_OK && (accepted > s->tail || repaired))
    rc = ps_owned_restore_tail(s->head, accepted, accepted > s->tail, error);
  if (rc == LC_OK) {
    s->tail = accepted;
    s->repair_pending = 1;
  }
  lc_free_with_allocator(NULL, s->recovery_buffer);
  s->recovery_buffer = NULL;
  return rc;
}
int ps_stream_open(ps_space *space, ps_budget *budget, ps_key key, int create,
                   const ps_ref *ref, ps_stream_recover_fn recover,
                   void *context, ps_stream **out, lc_error *error) {
  ps_owned_status status;
  ps_owned_initial initial;
  ps_coordination_guard *edit = NULL;
  ps_stream *s;
  int rc, continued, selected;
  if (out)
    *out = NULL;
  if (!out)
    return stream_error(error, LC_ERR_INVALID, "stream open requires output");
  s = lc_calloc_with_allocator(NULL, 1, sizeof(*s));
  if (!s)
    return stream_error(error, LC_ERR_NOMEM, "cannot allocate stream owner");
  s->space = space;
  s->key = key;
  if (pthread_mutex_init(&s->mutex, NULL)) {
    lc_free_with_allocator(NULL, s);
    return stream_error(error, LC_ERR_TRANSPORT,
                        "cannot initialize stream mutex");
  }
  selected =
      !create && ref && ps_coordination_shared(ps_space_coordination(space));
  if (selected) {
    rc = ps_owned_open_initial(space, key, ref, &s->head, &status, &initial,
                               error);
    edit = initial.edit;
  } else
    rc = ps_owned_open_guarded(space, budget, key, create, ref, &s->head,
                               &status, error);
  if (rc == LC_OK && !selected &&
      ps_coordination_shared(ps_space_coordination(space))) {
    /* Head creation is structural. Independent peers must be able to pass it
     * while this newly created stream has a long-running producer. */
    if (create)
      rc = ps_flush(space, error);
    if (rc == LC_OK)
      rc = ps_owned_edit_begin(s->head, 0, &edit, &status, &continued, error);
  }
  if (rc == LC_OK)
    rc = stream_load(s, &status, recover, context, selected ? &initial : NULL,
                     error);
  if (edit) {
    s->recover = recover;
    s->recovery_context = context;
    s->edit_valid = rc == LC_OK;
    rc = ps_owned_edit_end(s->head, edit, rc, error);
  }
  if (rc == LC_OK)
    *out = s;
  else
    stream_destroy(s);
  return rc;
}
/* Capture and verify a foreign mapping before replacing the resident one.
 * Existing windows retain their exact runs and read without edit authority.
 * Allocation or recovery failures leave the resident view intact. */
static int stream_refresh_edit(ps_stream *s, const ps_owned_status *status,
                               int continued, int append, lc_error *error) {
  ps_stream staged = {0};
  ps_owned_mapping *old;
  size_t count;
  int rc = LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (status->retired) {
    s->retired = 1;
    pthread_mutex_unlock(&s->mutex);
    return stream_error(error, LC_ERR_INVALID, "stream is retired");
  }
  if (continued && s->edit_valid && s->tail == status->tail &&
      s->capacity == status->capacity &&
      (s->mappings != STREAM_FULL || s->count == status->runs) &&
      s->sealed == status->sealed &&
      ((status->complete && !s->repair_pending) ||
       s->edit_wire == status->edit_wire)) {
    if (s->mappings == STREAM_PARKED ||
        (!append && s->mappings == STREAM_SUFFIX))
      rc = capture_mappings(s, append ? STREAM_SUFFIX : STREAM_FULL, NULL,
                            error);
    pthread_mutex_unlock(&s->mutex);
    return rc;
  }
  s->edit_valid = 0;
  pthread_mutex_unlock(&s->mutex);
  if (pthread_mutex_init(&staged.mutex, NULL))
    return stream_error(error, LC_ERR_TRANSPORT,
                        "cannot initialize refreshed stream view");
  staged.head = s->head;
  rc = stream_load(&staged, status, s->recover, s->recovery_context, NULL,
                   error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    old = s->runs;
    count = s->count;
    s->runs = staged.runs;
    s->count = staged.count;
    s->allocated = staged.allocated;
    s->mappings = STREAM_FULL;
    s->tail = staged.tail;
    s->capacity = staged.capacity;
    s->preferred_grant = staged.preferred_grant;
    s->checked = staged.checked;
    s->edit_wire = staged.edit_wire;
    s->checksum = staged.checksum;
    s->sealed = staged.sealed;
    s->repair_pending = staged.repair_pending;
    s->edit_valid = 1;
    pthread_mutex_unlock(&s->mutex);
    ps_owned_runs_close(old, count);
  } else
    ps_owned_runs_close(staged.runs, staged.count);
  pthread_mutex_destroy(&staged.mutex);
  return rc;
}
void ps_stream_park(ps_stream *s) {
  ps_owned_mapping *runs = NULL;
  size_t count = 0;
  pthread_mutex_lock(&s->mutex);
  if (!s->appending && !s->remapping && !s->recovering) {
    runs = s->runs;
    count = s->count;
    s->runs = NULL;
    s->count = s->allocated = 0;
    s->mappings = STREAM_PARKED;
  }
  pthread_mutex_unlock(&s->mutex);
  ps_owned_runs_close(runs, count);
}
int ps_stream_pin_mappings(ps_stream *s, lc_error *error) {
  int rc = LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->mappings != STREAM_FULL)
    rc = capture_mappings(s, STREAM_FULL, NULL, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_stream_close(ps_stream *s, lc_error *error) {
  if (!s)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->appending || s->readers || s->remapping) {
    pthread_mutex_unlock(&s->mutex);
    return stream_error(error, LC_ERR_INVALID,
                        "close stream appenders and readers first");
  }
  pthread_mutex_unlock(&s->mutex);
  stream_destroy(s);
  return LC_OK;
}
void ps_stream_drop_view(ps_stream *s) {
  int destroy;
  if (!s)
    return;
  pthread_mutex_lock(&s->mutex);
  s->detached = 1;
  destroy = !s->readers;
  pthread_mutex_unlock(&s->mutex);
  if (destroy)
    stream_destroy(s);
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
    out->runs = s->mappings == STREAM_FULL ? s->count : status.runs;
    out->stored_bytes = status.stored_bytes;
    out->readers = s->readers;
    out->sealed = s->sealed;
    out->retired = s->retired;
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}
int ps_stream_piece_next(ps_stream *s, uint64_t cursor,
                         ps_stream_piece_info *out, int *found,
                         lc_error *error) {
  size_t at;
  ps_owned_status status;
  int rc;
  if (found)
    *found = 0;
  if (!s || !out || !found)
    return stream_error(error, LC_ERR_INVALID,
                        "piece observation requires owner and outputs");
  pthread_mutex_lock(&s->mutex);
  rc = ps_owned_status_get(s->head, &status, error);
  at = mapping_lower(s->runs, s->count, cursor);
  if (rc == LC_OK && at < s->count) {
    out->base = s->runs[at].base;
    out->bytes = s->runs[at].capacity;
    out->completed_bytes = s->tail > out->base ? s->tail - out->base : 0;
    if (out->completed_bytes > out->bytes)
      out->completed_bytes = out->bytes;
    out->mapped = ps_owned_run_mapped(s->runs[at].handle);
    out->stored_bytes = ps_owned_run_storage(s->runs[at].handle);
    *found = 1;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_stream_piece_matches(ps_stream *s, uint64_t base,
                            const ps_owned_range *ranges, size_t count,
                            int *matches, lc_error *error) {
  size_t at;
  int rc = LC_OK;
  if (!s || !matches || (count && !ranges))
    return stream_error(error, LC_ERR_INVALID,
                        "piece comparison requires owner, ranges and output");
  *matches = 0;
  pthread_mutex_lock(&s->mutex);
  at = mapping_lower(s->runs, s->count, base);
  if (at == s->count || s->runs[at].base != base)
    rc = stream_error(error, LC_ERR_INVALID, "piece is no longer selected");
  else
    *matches = ps_owned_run_matches(s->runs[at].handle, ranges, count);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
typedef struct stream_piece_selection {
  ps_stream *stream;
  uint64_t base;
  ps_owned_run *replacement;
  ps_piece_select_fn select;
  void *context;
  ps_piece_publish_fn publish;
  void *publication;
} stream_piece_selection;
static int publish_repacked_piece(void *context, lc_error *error) {
  stream_piece_selection *selection = context;
  ps_stream *s = selection->stream;
  ps_owned_run *old = NULL;
  ps_coordination_guard *edit = NULL;
  ps_owned_status status;
  size_t at;
  int continued;
  int rc = ps_owned_edit_begin(s->head, 0, &edit, &status, &continued, error);
  if (rc == LC_OK)
    rc = selection->publish(selection->publication, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    at = mapping_lower(s->runs, s->count, selection->base);
    if (at < s->count && s->runs[at].base == selection->base) {
      old = s->runs[at].handle;
      if (selection->replacement) {
        ps_owned_run_retain(selection->replacement);
        s->runs[at].handle = selection->replacement;
      } else {
        memmove(s->runs + at, s->runs + at + 1,
                (s->count - at - 1) * sizeof(*s->runs));
        --s->count;
      }
    }
    pthread_mutex_unlock(&s->mutex);
  }
  ps_owned_run_close(old);
  return ps_owned_edit_end(s->head, edit, rc, error);
}
static int select_repacked_piece(void *context, ps_piece_publish_fn publish,
                                 void *publication, lc_error *error) {
  stream_piece_selection *selection = context;
  selection->publish = publish;
  selection->publication = publication;
  return selection->select
             ? selection->select(selection->context, publish_repacked_piece,
                                 selection, error)
             : publish_repacked_piece(selection, error);
}
int ps_stream_piece_repack(ps_stream *s, uint64_t base, ps_budget *budget,
                           const ps_owned_range *ranges, size_t count,
                           ps_piece_select_fn select, void *context,
                           lc_error *error) {
  ps_owned_run *old = NULL;
  stream_piece_selection selection = {0};
  int rc;
  if (!s)
    return stream_error(error, LC_ERR_INVALID,
                        "piece replacement requires owner");
  pthread_mutex_lock(&s->mutex);
  rc = s->remapping || s->recovering || s->retired
           ? stream_error(error, LC_ERR_INVALID,
                          "piece replacement is busy or unavailable")
           : LC_OK;
  if (rc == LC_OK)
    s->remapping = 1;
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    return rc;
  /* The allocator selects and pins an exact mapping at the certified
   * prefix. Keep the writer's stamp and resident append state intact.
   * Publication checks this predecessor under namespace and stream
   * authority after the private copy is complete. */
  rc = ps_owned_capture_piece(s->head, base, count != 0, &old, error);
  selection.stream = s;
  selection.base = base;
  selection.select = select;
  selection.context = context;
  if (rc == LC_OK)
    rc = ps_owned_run_repack(s->head, old, budget, ranges, count,
                             select_repacked_piece, &selection,
                             &selection.replacement, error);
  pthread_mutex_lock(&s->mutex);
  s->remapping = 0;
  pthread_mutex_unlock(&s->mutex);
  ps_owned_run_close(selection.replacement);
  ps_owned_run_close(old);
  return rc;
}
static int stream_transition_end(ps_stream *s, ps_coordination_guard *edit,
                                 int retirement, int rc, lc_error *error) {
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK) {
    if (retirement)
      s->retired = 1;
    else
      s->sealed = 1;
  }
  rc = ps_owned_edit_end(s->head, edit, rc, error);
  s->appending = 0;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static int stream_transition_begin(ps_stream *s, int retirement,
                                   ps_coordination_guard **edit,
                                   lc_error *error) {
  ps_owned_status status;
  int rc = LC_OK, continued;
  if (!s)
    return stream_error(error, LC_ERR_INVALID,
                        "stream transition requires owner");
  pthread_mutex_lock(&s->mutex);
  if (s->appending || s->recovering || s->remapping ||
      (!retirement && s->retired))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream transition is busy or retired");
  if (rc == LC_OK)
    s->appending = 1;
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    return rc;
  rc = ps_owned_edit_begin(s->head, 0, edit, &status, &continued, error);
  if (rc == LC_OK && *edit && !retirement)
    rc = stream_refresh_edit(s, &status, continued, 0, error);
  if (rc != LC_OK)
    rc = stream_transition_end(s, *edit, retirement, rc, error);
  return rc;
}
static int stream_transition(ps_stream *s, int retirement, lc_error *error) {
  ps_coordination_guard *edit = NULL;
  int rc = stream_transition_begin(s, retirement, &edit, error);
  if (rc != LC_OK)
    return rc;
  rc = retirement ? ps_owned_retire(s->head, error)
                  : ps_owned_seal(s->head, error);
  return stream_transition_end(s, edit, retirement, rc, error);
}
int ps_stream_seal(ps_stream *s, lc_error *error) {
  return stream_transition(s, 0, error);
}
int ps_stream_has_sparse_runs(ps_stream *s) {
  uint64_t covered = 0;
  size_t i;
  for (i = 0; i < s->count && s->runs[i].base == covered &&
              !ps_owned_run_mapped(s->runs[i].handle);
       ++i)
    covered += s->runs[i].capacity;
  return covered != s->capacity;
}
int ps_stream_retire(ps_stream *s, lc_error *error) {
  return stream_transition(s, 1, error);
}
int ps_stream_retire_batch(ps_stream *const *streams, size_t count,
                           size_t *completed, lc_error *error) {
  ps_coordination_guard *edits[PS_STREAM_RETIRE_LIMIT] = {0};
  lc_error interrupted = {0};
  uint64_t latest = 0, sequence;
  size_t i, recorded = 0;
  int rc, prepared = LC_OK, durable = LC_OK;
  *completed = 0;
  if (!count || count > PS_STREAM_RETIRE_LIMIT)
    return stream_error(error, LC_ERR_INVALID, "invalid retirement count");
  if (count == 1) {
    rc = ps_stream_retire(streams[0], error);
    *completed = rc == LC_OK;
    return rc;
  }
  for (i = 0; i < count; ++i) {
    if (!streams[i] || streams[i]->space != streams[0]->space)
      return stream_error(error, LC_ERR_INVALID,
                          "retirement requires streams in one container");
  }
  while (prepared == LC_OK && recorded < count) {
    ps_stream *s = streams[recorded];
    prepared = stream_transition_begin(s, 1, &edits[recorded], &interrupted);
    if (prepared != LC_OK)
      break;
    prepared = ps_owned_retire_record(s->head, &sequence, &interrupted);
    if (prepared != LC_OK) {
      prepared =
          stream_transition_end(s, edits[recorded], 1, prepared, &interrupted);
      break;
    }
    if (sequence > latest)
      latest = sequence;
    ++recorded;
  }
  /* Each record follows its durable catalog unlink. The existing journal
   * accepts a prefix of these independent removals after interruption. */
  if (recorded)
    durable = ps_owned_retire_wait(streams[0]->head, latest, error);
  if (durable == LC_OK)
    *completed = recorded;
  rc = durable;
  for (i = 0; i < recorded; ++i) {
    int closed = stream_transition_end(streams[i], edits[i], 1, durable,
                                       rc == LC_OK ? error : NULL);
    if (rc == LC_OK)
      rc = closed;
  }
  if (rc == LC_OK && prepared != LC_OK) {
    rc = prepared;
    if (error) {
      lc_error_cleanup(error);
      *error = interrupted;
      lc_error_init(&interrupted);
    }
  }
  lc_error_cleanup(&interrupted);
  return rc;
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
      /* A clean open usually examines only the next header at the known
       * tail. If it identifies a record, the next miss uses read-ahead. */
      if (!s->recovery_bytes && offset >= s->tail && n <= 64U)
        take = n;
      else if (take > PS_CHUNK)
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
  int rc = LC_OK, continued, repair;
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
  if (s->appending || s->recovering || s->sealed || s->retired)
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream is busy, sealed or retired");
  if (rc == LC_OK)
    s->appending = 1;
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, w);
    return rc;
  }
  rc = ps_owned_edit_begin(s->head, 1, &w->edit, &status, &continued, error);
  if (rc == LC_OK && w->edit)
    rc = stream_refresh_edit(s, &status, continued, 1, error);
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK && s->sealed)
    rc = stream_error(error, LC_ERR_INVALID, "stream is sealed");
  if (rc == LC_OK) {
    w->stream = s;
    w->start = s->tail;
    w->prior_checked = s->checked;
    w->prior_checksum = s->checksum;
  }
  repair = s->repair_pending;
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && repair) {
    rc = clear_suffix(s, w->start, w->buffer, error);
    if (rc == LC_OK)
      rc = ps_owned_recovered(s->head, w->start, error);
    pthread_mutex_lock(&s->mutex);
    if (rc == LC_OK)
      s->repair_pending = 0;
    pthread_mutex_unlock(&s->mutex);
  }
  if (rc == LC_OK)
    *out = w;
  else {
    rc = ps_owned_edit_end(s->head, w->edit, rc, error);
    pthread_mutex_lock(&s->mutex);
    s->appending = 0;
    pthread_mutex_unlock(&s->mutex);
    lc_free_with_allocator(NULL, w);
  }
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
  if (rc == LC_OK) {
    w->wrote = 1;
    rc = stream_io(w->stream, w->start + w->flushed, w->buffer, w->used, 1,
                   error);
  }
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
      crc = (unsigned long)ps_crc32((uLong)crc, scratch, (uInt)n);
    offset += n;
    length -= n;
  }
  if (rc == LC_OK && length)
    crc = (unsigned long)ps_crc32(
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
    rc = stream_pieces(w->stream, w->start + w->bytes, 0, 1, NULL, w->buffer,
                       NULL, error);
  if (rc == LC_OK)
    rc = ps_owned_commit_canonical(w->stream->head, w->start + w->bytes, mode,
                                   publish, context, error);
  if (rc == LC_OK) {
    rc = ps_owned_edit_end(w->stream->head, w->edit, rc, error);
    w->edit = NULL;
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
    if (rc == LC_OK && w->wrote) {
      rc = clear_suffix(w->stream, w->start, w->buffer, &error);
      if (rc == LC_OK)
        (void)ps_owned_barrier(w->stream->head, &error);
    }
    (void)ps_owned_edit_end(w->stream->head, w->edit, rc, &error);
    lc_error_cleanup(&error);
    pthread_mutex_lock(&w->stream->mutex);
    w->stream->appending = 0;
    w->stream->edit_valid = 0;
    w->stream->checked = w->prior_checked;
    w->stream->checksum = w->prior_checksum;
    pthread_mutex_unlock(&w->stream->mutex);
  }
  lc_free_with_allocator(NULL, w);
}
static int window_capture(ps_space *, ps_key, const ps_ref *, uint64_t,
                          uint64_t, ps_stream_window *, lc_error *);
static int window_admit(ps_stream *s, uint64_t offset, uint64_t bytes,
                        ps_stream_window *window, lc_error *error) {
  ps_owned_status status;
  size_t first, last, i;
  uint64_t cursor;
  int rc = LC_OK, direct;
  pthread_mutex_lock(&s->mutex);
  direct = bytes && s->mappings != STREAM_FULL;
  if (!direct)
    rc = ps_owned_status_capture(s->head, &status, error);
  if (rc == LC_OK &&
      (s->recovering || s->retired || (!direct && status.retired) ||
       offset > s->tail || bytes > s->tail - offset))
    rc = stream_error(error, LC_ERR_INVALID,
                      "stream read outside committed prefix");
  if (rc == LC_OK && direct)
    rc = window_capture(s->space, s->key, &s->ref, offset, offset + bytes,
                        window, error);
  else if (rc == LC_OK && bytes) {
    first = mapping_lower(s->runs, s->count, offset);
    last = first;
    cursor = offset;
    while (bytes && cursor < offset + bytes && last < s->count) {
      if (s->runs[last].base > cursor)
        break;
      cursor = s->runs[last].base + s->runs[last].capacity;
      ++last;
    }
    if (bytes && cursor < offset + bytes)
      rc = stream_error(error, LC_ERR_PROTOCOL,
                        "captured stream range is unmapped");
    if (rc == LC_OK && last > first) {
      window->runs =
          lc_alloc_with_allocator(NULL, (last - first) * sizeof(*window->runs));
      if (!window->runs)
        rc =
            stream_error(error, LC_ERR_NOMEM, "cannot capture stream mappings");
    }
    if (rc == LC_OK) {
      window->count = last - first;
      for (i = 0; i < window->count; ++i) {
        window->runs[i] = s->runs[first + i];
        ps_owned_run_retain(window->runs[i].handle);
      }
    }
  }
  if (rc == LC_OK) {
    window->stream = s;
    window->start = offset;
    window->end = offset + bytes;
    ++s->readers;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static void window_runs_close(ps_stream_window *window) {
  ps_owned_runs_close(window->runs, window->count);
  window->runs = NULL;
  window->count = 0;
}
static void window_release(ps_stream_window *window) {
  int destroy;
  window_runs_close(window);
  if (!window->stream)
    return;
  pthread_mutex_lock(&window->stream->mutex);
  destroy = !--window->stream->readers && window->stream->detached;
  pthread_mutex_unlock(&window->stream->mutex);
  if (destroy)
    stream_destroy(window->stream);
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
static int window_geometry(ps_stream_window *window, uint64_t start,
                           uint64_t end, lc_error *error) {
  uint64_t cursor = start;
  size_t i;
  for (i = 0; i < window->count; ++i) {
    uint64_t base = window->runs[i].base;
    uint64_t limit = base + window->runs[i].capacity;
    if (base > cursor || limit <= cursor)
      return stream_error(error, LC_ERR_PROTOCOL,
                          "certified stream range is unmapped");
    cursor = limit;
  }
  if (cursor < end)
    return stream_error(error, LC_ERR_PROTOCOL,
                        "certified stream range is incomplete");
  return LC_OK;
}
static int window_capture(ps_space *space, ps_key key, const ps_ref *ref,
                          uint64_t start, uint64_t end,
                          ps_stream_window *window, lc_error *error) {
  int rc = ps_owned_runs_range(space, key, ref, start, end, &window->runs,
                               &window->count, error);
  if (rc == LC_OK)
    rc = window_geometry(window, start, end, error);
  if (rc != LC_OK)
    window_runs_close(window);
  return rc;
}
int ps_stream_window_pin_locked(ps_space *space, ps_key key, const ps_ref *ref,
                                uint64_t start, uint64_t end, int provisional,
                                ps_stream_window **out, lc_error *error) {
  ps_stream_window *window;
  int rc;
  *out = NULL;
  window = lc_calloc_with_allocator(NULL, 1, sizeof(*window));
  if (!window)
    return stream_error(error, LC_ERR_NOMEM, "cannot allocate stream window");
  rc = ps_owned_runs_pin_range(space, key, ref, start, end, provisional,
                               &window->runs, &window->count, error);
  if (rc == LC_OK) {
    window->start = start;
    window->end = end;
    *out = window;
  } else
    lc_free_with_allocator(NULL, window);
  return rc;
}
int ps_stream_window_verify(ps_stream_window *window, lc_error *error) {
  int rc = ps_owned_runs_verify(window->runs, window->count, NULL, error);
  if (rc == LC_OK)
    rc = window_geometry(window, window->start, window->end, error);
  return rc;
}
void ps_stream_window_discard_locked(ps_stream_window *window) {
  if (window) {
    ps_owned_runs_discard_locked(window->runs, window->count);
    lc_free_with_allocator(NULL, window);
  }
}
int ps_stream_window_read(ps_stream_window *window, uint64_t offset,
                          void *bytes, size_t n, lc_error *error) {
  unsigned char *out = bytes;
  size_t at, take;
  int rc;
  if (!window || (!bytes && n) || offset < window->start ||
      offset > window->end || n > window->end - offset)
    return stream_error(error, LC_ERR_INVALID,
                        "offset read outside captured stream window");
  at = mapping_lower(window->runs, window->count, offset);
  while (n) {
    const ps_owned_mapping *run;
    if (at == window->count || offset < window->runs[at].base)
      return stream_error(error, LC_ERR_PROTOCOL,
                          "captured stream range is unmapped");
    run = &window->runs[at];
    take = (size_t)(run->base + run->capacity - offset);
    if (take > n)
      take = n;
    rc = ps_owned_read(run->handle, offset - run->base, out, take, error);
    if (rc != LC_OK)
      return rc;
    offset += take;
    out += take;
    n -= take;
    ++at;
  }
  return LC_OK;
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
      rc = ps_stream_window_read(&r->window, r->offset, r->buffer, take, error);
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
