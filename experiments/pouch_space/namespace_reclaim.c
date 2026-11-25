#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "namespace_internal.h"
#include "space_internal.h"
#include <string.h>
static int failure(lc_error *, const char *);
static int plan_output(ps_budget_plan *plan, size_t name_bytes, uint64_t bytes,
                       lc_error *e) {
  int rc = ps_budget_plan_stream(plan, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_catalog(plan, name_bytes, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_span(plan, bytes, e);
  return rc;
}

struct ps_namespace_body {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_ns_resource *resource;
  ps_record_writer writer;
  ps_budget *budget;
  uint64_t bytes;
  int attempted, owns_budget;
  int bounded;
};
int ps_ns_body_plan(ps_budget_plan *plan, uint64_t bytes, lc_error *e) {
  return plan_output(plan, 29, bytes, e);
}
static int body_begin(ps_namespace *ns, ps_budget *budget, uint64_t bytes,
                      int bounded, ps_namespace_body **out, lc_error *e) {
  ps_namespace_body *b;
  ps_budget_plan plan = {0};
  struct ps_namespace_data *d;
  uint64_t id;
  int rc;
  if (out)
    *out = NULL;
  if (!out || !bytes)
    return failure(e, "namespace body build requires output and positive span");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  b = lc_calloc_with_allocator(NULL, 1, sizeof(*b));
  if (!b)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate body builder",
                        NULL, NULL, "pouch");
  b->ns = ns;
  b->data = d;
  b->bytes = bytes;
  b->budget = budget;
  b->owns_budget = !budget;
  b->bounded = bounded;
  ps_namespace_retain(ns);
  if (!budget) {
    rc = plan_output(&plan, 29, bytes, e);
    if (rc == LC_OK)
      rc = ps_budget_open(ns->root->space, &plan, &b->budget, e);
  }
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(ns->root->space, &id, e);
  if (rc == LC_OK) {
    b->resource = ps_ns_resource_new(PS_CATALOG_BODY, id, e);
    if (!b->resource)
      rc = LC_ERR_NOMEM;
  }
  if (rc == LC_OK) {
    ps_ns_resource *r = b->resource;
    r->binding.stream.owner = id;
    r->binding.stream.key = ns->identity.owner;
    r->binding.stream.kind = PS_STREAM;
    if (rc == LC_OK)
      rc = ps_stream_open(ns->root->space, b->budget, r->binding.stream, 1,
                          NULL, NULL, NULL, &r->stream, e);
    if (rc == LC_OK)
      rc = ps_record_begin(r->stream, &b->writer, e);
    if (rc == LC_OK && !bounded)
      rc = ps_stream_reserve_exact(b->writer.writer, b->budget, bytes, e);
    if (rc == LC_OK)
      b->writer.limit = bytes;
  }
  if (rc == LC_OK)
    *out = b;
  else
    ps_namespace_body_close(b);
  return rc;
}
int ps_namespace_body_begin(ps_namespace *ns, uint64_t bytes,
                            ps_namespace_body **out, lc_error *e) {
  return body_begin(ns, NULL, bytes, 0, out, e);
}
int ps_namespace_body_begin_bound(ps_namespace *ns, uint64_t maximum,
                                  ps_namespace_body **out, lc_error *e) {
  return body_begin(ns, NULL, maximum, 1, out, e);
}
int ps_ns_body_begin(ps_namespace *ns, ps_budget *budget, uint64_t maximum,
                     ps_namespace_body **out, lc_error *e) {
  if (!budget)
    return failure(e, "funded body construction requires a budget");
  return body_begin(ns, budget, maximum, 1, out, e);
}
static int bounded_append(void *context, const struct iovec *parts, int count,
                          lc_error *e) {
  ps_namespace_body *b = context;
  uint64_t end = b->writer.tail;
  int i, rc;
  if (b->attempted || !b->writer.writer || count < 0 || (count && !parts) ||
      end > b->bytes)
    return failure(e, "bounded body output is closed or invalid");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > b->bytes - end)
      return failure(e, "bounded body output exceeds admitted span");
    end += parts[i].iov_len;
  }
  rc = ps_stream_reserve_exact(b->writer.writer, b->budget, end, e);
  if (rc == LC_OK)
    rc = lc_pouch_record_io_append(ps_record_output(&b->writer), parts, count,
                                   e);
  return rc;
}
static int bounded_patch(void *context, uint64_t offset,
                         const struct iovec *parts, int count, lc_error *e) {
  ps_namespace_body *b = context;
  return lc_pouch_record_io_patch(ps_record_output(&b->writer), offset, parts,
                                  count, e);
}
static int bounded_tail(void *context, uint64_t *out, lc_error *e) {
  ps_namespace_body *b = context;
  return lc_pouch_record_io_tail(ps_record_output(&b->writer), out, e);
}
static void bounded_discard(void *context, uint64_t offset) {
  ps_namespace_body *b = context;
  lc_pouch_record_io io = ps_record_output(&b->writer);
  io.ops->discard(io.context, offset);
}
lc_pouch_record_io ps_namespace_body_output(ps_namespace_body *b) {
  static const lc_pouch_record_io_ops ops = {
      NULL, bounded_append, bounded_patch, bounded_tail, bounded_discard};
  lc_pouch_record_io output;
  if (b && b->bounded) {
    output.ops = &ops;
    output.context = b;
    return output;
  }
  return ps_record_output(b ? &b->writer : NULL);
}
const char *ps_namespace_body_leaf(const ps_namespace_body *b) {
  return b && b->resource ? b->resource->leaf : NULL;
}
int ps_ns_body_commit(ps_namespace_body *b, lc_error *e) {
  ps_stream_info info;
  ps_ns_resource *r;
  int rc;
  if (!b || b->attempted || !b->writer.writer || !b->writer.tail ||
      b->writer.tail > b->bytes || (!b->bounded && b->writer.tail != b->bytes))
    return failure(e, "body build is closed or differs from its measured size");
  b->attempted = 1;
  r = b->resource;
  rc = ps_record_commit(&b->writer, PS_COMMIT_DEFAULT, NULL, NULL, e);
  if (rc == LC_OK)
    rc = ps_stream_seal(r->stream, e);
  if (rc == LC_OK)
    rc = ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&b->data->mutex);
    rc = ps_ns_index_room(b->data, e);
    pthread_mutex_unlock(&b->data->mutex);
  }
  if (rc == LC_OK)
    rc = ps_catalog_put(b->ns->root->space, b->budget, b->ns->identity.owner,
                        PS_CATALOG_BODY, r->leaf, strlen(r->leaf), NULL,
                        r->binding.stream, &info.ref, &r->binding, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&b->data->mutex);
    ps_ns_resource_park(b->data, r);
    ps_ns_resource_retain(r);
    ps_ns_insert(b->data, r);
    pthread_mutex_unlock(&b->data->mutex);
  }
  return rc;
}
int ps_namespace_body_commit(ps_namespace_body *b, lc_error *e) {
  int rc;
  if (!b)
    return failure(e, "namespace body build is not open");
  rc = ps_ns_enter(b->data, e);
  if (rc == LC_OK) {
    rc = ps_ns_body_commit(b, e);
    ps_ns_leave(b->data);
  }
  return rc;
}
void ps_namespace_body_close(ps_namespace_body *b) {
  if (!b)
    return;
  ps_record_abort(&b->writer);
  if (b->resource && !b->resource->pooled && b->resource->stream &&
      ps_check(b->ns->root->space, NULL) == LC_OK)
    ps_stream_retire(b->resource->stream, NULL);
  ps_ns_resource_release(b->resource);
  if (b->owns_budget)
    ps_budget_close(b->budget, NULL);
  ps_namespace_release(b->ns);
  lc_free_with_allocator(NULL, b);
}
static int failure(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL, "pouch");
}
/* Mutation authority excludes directory changes, while the mutex excludes read
 * admission. Catalog removal precedes physical retirement, never the reverse.
 */
static int drop(struct ps_namespace_data *d, ps_ns_resource *r, lc_error *e) {
  int rc = ps_catalog_remove(d->ns->root->space, d->ns->identity.owner, r->kind,
                             r->leaf, strlen(r->leaf), &r->binding.revision, e);
  if (rc == LC_OK)
    ps_ns_remove(d, r);
  return rc;
}
static ps_namespace_retirement *retain_retirement(struct ps_namespace_data *d,
                                                  ps_ns_resource *r) {
  __atomic_store_n(&r->retiring, 1, __ATOMIC_RELEASE);
  ps_namespace_retain(d->ns);
  return r;
}
static int segment_covered(struct ps_namespace_data *d, ps_ns_resource *r,
                           const ps_checkpoint_info *coverage,
                           const ps_stream_info *info) {
  return r->id != d->active && coverage->epoch &&
         (r->id < coverage->segment ||
          (r->id == coverage->segment && coverage->offset >= info->tail));
}
static int unlink_covered_locked(struct ps_namespace_data *d,
                                 uint64_t minimum_bytes, size_t minimum_count,
                                 ps_namespace_retirement **retirements,
                                 size_t *completed, lc_error *e) {
  ps_catalog_removal removals[PS_NAMESPACE_RETIRE_LIMIT];
  ps_ns_resource *sources[PS_NAMESPACE_RETIRE_LIMIT], *r;
  ps_checkpoint_info coverage;
  ps_stream_info info;
  size_t i, count = 0;
  int rc = LC_OK;
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  for (i = 0;
       rc == LC_OK && d->checkpoint_ready && coverage.epoch &&
       !coverage.pending && i < d->count && count < PS_NAMESPACE_RETIRE_LIMIT;
       ++i) {
    r = d->items[i];
    if (r->kind != PS_CATALOG_SEGMENT || r->unselected || r->id == d->active ||
        r->id > coverage.segment ||
        ps_key_checkpoint_source_refs(d->checkpoint, r->kind, r->id))
      continue;
    rc = ps_stream_status(r->stream, &info, e);
    if (rc != LC_OK || !info.stored_bytes ||
        info.stored_bytes < minimum_bytes ||
        !segment_covered(d, r, &coverage, &info))
      continue;
    sources[count] = r;
    removals[count].name = r->leaf;
    removals[count].length = strlen(r->leaf);
    removals[count].expected = &r->binding.revision;
    removals[count].kind = r->kind;
    ++count;
  }
  if (rc == LC_OK && count >= minimum_count)
    rc = ps_ns_require_unpinned(d, e);
  if (rc == LC_OK && count >= minimum_count)
    rc = ps_ns_stabilize_cut(d, coverage.control, e);
  if (rc == LC_OK && count >= minimum_count)
    rc = ps_catalog_remove_many(d->ns->root->space, d->ns->identity.owner,
                                removals, count, completed, e);
  for (i = 0; i < *completed; ++i) {
    ps_ns_remove(d, sources[i]);
    retirements[i] = retain_retirement(d, sources[i]);
  }
  return rc;
}
int ps_namespace_unlink_covered(ps_namespace *ns, uint64_t minimum_bytes,
                                ps_namespace_retirement **retirements,
                                size_t *completed, lc_error *e) {
  struct ps_namespace_data *d;
  int rc;
  if (!retirements || !completed)
    return failure(e, "covered unlink requires retirement outputs");
  *completed = 0;
  rc = ps_ns_data_get(ns, &d, e);
  if (rc == LC_OK)
    rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  rc = unlink_covered_locked(d, minimum_bytes, 2, retirements, completed, e);
  pthread_mutex_unlock(&d->mutex);
  ps_ns_leave(d);
  return rc;
}

/* The appending owner already holds namespace edit authority. Complete
 * durable coverage before unlinking sources that lost their last reference.
 * The common selector rechecks coverage and view witnesses after the flush. */
int ps_ns_reclaim_before_append(struct ps_namespace_data *d, lc_error *e) {
  ps_namespace_retirement *retirements[PS_NAMESPACE_RETIRE_LIMIT];
  ps_ns_resource **link = &d->retired;
  lc_error local = {0};
  unsigned int retired;
  size_t i, count = 0;
  int rc = LC_OK, done;
  if (!e)
    e = &local;
  pthread_mutex_lock(&d->mutex);
  for (i = 0; i < d->count; ++i) {
    ps_ns_resource *source = d->items[i];
    if (source->kind == PS_CATALOG_SEGMENT && !source->unselected &&
        source->id < d->active &&
        !ps_key_checkpoint_source_refs(d->checkpoint, source->kind, source->id))
      break;
  }
  if (i < d->count) {
    rc = ps_ns_require_unpinned(d, e);
    if (rc == LC_OK)
      rc = ps_key_checkpoint_flush(d->checkpoint, e);
    if (rc == LC_OK)
      rc = unlink_covered_locked(d, 0, 1, retirements, &count, e);
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc == LC_ERR_PENDING ||
      (rc == LC_ERR_INVALID && e->server_code &&
       !strcmp(e->server_code, "pouch_namespace_busy"))) {
    lc_error_cleanup(e);
    rc = LC_OK;
  }
  /* Confirmed unlinks were prepended under this same namespace authority.
   * Release completed owners directly. Captured windows own their mappings
   * independently, and unfinished retirements stay available to the reaper. */
  done = ps_namespace_retire_unlinked_batch(d->ns, retirements, count, &retired,
                                            rc == LC_OK ? e : NULL);
  if (rc == LC_OK)
    rc = done;
  while (count) {
    ps_ns_resource *source = retirements[--count];
    if (retired & (1U << count)) {
      pthread_mutex_lock(&d->mutex);
      *link = source->next;
      pthread_mutex_unlock(&d->mutex);
      ps_ns_resource_release(source);
    } else
      link = &source->next;
  }
  lc_error_cleanup(&local);
  return rc;
}
static int reap(struct ps_namespace_data *d, uint64_t *closed,
                uint64_t *pending, lc_error *e) {
  ps_ns_resource *r = d->retired, **link = &d->retired;
  ps_stream_info info;
  int rc = LC_OK;
  *closed = *pending = 0;
  while (r && rc == LC_OK) {
    if (__atomic_load_n(&r->retiring, __ATOMIC_ACQUIRE) ||
        __atomic_load_n(&r->repacking, __ATOMIC_ACQUIRE)) {
      ++*pending;
      link = &r->next;
      r = r->next;
      continue;
    }
    rc = ps_stream_status(r->stream, &info, e);
    if (rc == LC_OK && !info.retired) {
      rc = ps_stream_retire(r->stream, e);
      if (rc == LC_ERR_INVALID) {
        /* A catalog view may still pin an already removed binding. There is
         * no appender on these sealed/private-abandoned sources. */
        lc_error_cleanup(e);
        rc = LC_OK;
        ++*pending;
        link = &r->next;
        r = r->next;
        continue;
      }
    }
    if (rc != LC_OK)
      break;
    if (info.readers) {
      ++*pending;
      link = &r->next;
      r = r->next;
    } else {
      ps_ns_resource *next = r->next;
      /* Retirement rejects new windows. Existing windows can only drain, so
       * the zero-reader observation remains safe until close. */
      pthread_mutex_lock(&d->mutex);
      *link = next;
      pthread_mutex_unlock(&d->mutex);
      ps_ns_resource_release(r);
      r = next;
      ++*closed;
    }
  }
  return rc;
}
int ps_namespace_reap(ps_namespace *ns, uint64_t *closed, uint64_t *pending,
                      lc_error *e) {
  struct ps_namespace_data *d;
  int rc;
  if (!closed || !pending)
    return failure(e, "namespace reap requires counts");
  *closed = *pending = 0;
  rc = ps_ns_data_get(ns, &d, e);
  if (rc == LC_OK) {
    rc = ps_ns_enter(d, e);
    if (rc == LC_OK) {
      ps_ns_resource *unused =
          d->active == LC_U64_MAX
              ? NULL
              : ps_ns_find(d, PS_CATALOG_SEGMENT, d->active + 1);
      if (unused && unused->unselected) {
        ps_space *space = ns->root->space;
        ps_stream_info info;
        rc = ps_stream_status(unused->stream, &info, e);
        if (rc == LC_OK && (info.tail || info.sealed ||
                            ps_key_checkpoint_source_refs(
                                d->checkpoint, unused->kind, unused->id)))
          rc = lc_error_set(e, LC_ERR_PROTOCOL, 0,
                            "unselected namespace source has canonical state",
                            NULL, NULL, "pouch");
        /* A failed NoSync producer may have framed the binding without a
         * barrier. Certify it before the ordinary removal and retirement. */
        if (rc == LC_OK && ps_internal_exclusive_nosync(space)) {
          ps_catalog_view *binding = NULL;
          rc = ps_catalog_get_segment_for_edit(
              space, ns->identity.owner, unused->leaf, strlen(unused->leaf),
              &binding, e);
          ps_catalog_view_close(binding);
        }
        if (rc == LC_OK) {
          pthread_mutex_lock(&d->mutex);
          rc = drop(d, unused, e);
          pthread_mutex_unlock(&d->mutex);
        }
      }
      if (rc == LC_OK)
        rc = reap(d, closed, pending, e);
      ps_ns_leave(d);
    }
  }
  return rc;
}
int ps_namespace_unlink_active(ps_namespace *ns, uint64_t expected,
                               ps_namespace_retirement **retirement,
                               lc_error *e) {
  struct ps_namespace_data *d;
  ps_checkpoint_info coverage;
  ps_ns_resource *source = NULL;
  int rc;
  if (!retirement)
    return failure(e, "active unlink requires retirement output");
  *retirement = NULL;
  rc = ps_ns_enter_checkpoint(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  if (d->active == expected && d->tail && coverage.epoch &&
      coverage.segment == expected && coverage.offset == d->tail &&
      !coverage.pending &&
      !ps_key_checkpoint_source_refs(d->checkpoint, PS_CATALOG_SEGMENT,
                                     expected)) {
    rc = ps_ns_require_unpinned(d, e);
    if (rc == LC_OK)
      source = ps_ns_find(d, PS_CATALOG_SEGMENT, expected);
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc == LC_OK && source)
    rc = ps_ns_select_successor(d, NULL, e);
  if (rc == LC_OK && source)
    rc = ps_ns_stabilize_cut(d, coverage.control, e);
  if (rc == LC_OK && source) {
    /* The covered predecessor is unreachable from the new durable route.
     * A crash before unlink leaves the source eligible for normal
     * reclamation. After unlink the orphan sweep can finish retirement. */
    pthread_mutex_lock(&d->mutex);
    rc = drop(d, source, e);
    if (rc == LC_OK) {
      d->seal_pending = 0;
      *retirement = retain_retirement(d, source);
    }
    pthread_mutex_unlock(&d->mutex);
  }
  ps_ns_leave(d);
  return rc;
}
int ps_namespace_retire_unlinked(ps_namespace *ns,
                                 ps_namespace_retirement *retirement,
                                 lc_error *e) {
  unsigned int retired;
  int rc = ps_namespace_retire_unlinked_batch(ns, &retirement, 1, &retired, e);
  if (rc == LC_OK && !retired)
    rc = lc_error_set(e, LC_ERR_PENDING, 0,
                      "unlinked source still has an active owner", NULL, NULL,
                      "pouch");
  return rc;
}
int ps_namespace_retire_unlinked_batch(ps_namespace *ns,
                                       ps_namespace_retirement *const *items,
                                       size_t count, unsigned int *retired,
                                       lc_error *e) {
  ps_stream *streams[PS_NAMESPACE_RETIRE_LIMIT];
  size_t i, next = 0, completed;
  int rc = LC_OK;
  *retired = 0;
  if (!count)
    return LC_OK;
  if (!ns || count > PS_NAMESPACE_RETIRE_LIMIT)
    return failure(e, "invalid namespace retirements");
  for (i = 0; i < count; ++i) {
    if (!items[i])
      return failure(e, "retire requires an unlinked namespace source");
    streams[i] = items[i]->stream;
  }
  while (rc == LC_OK && next < count) {
    rc = ps_stream_retire_batch(streams + next, count - next, &completed, e);
    for (i = 0; i < completed; ++i)
      *retired |= 1U << (next + i);
    next += completed;
    if ((rc == LC_ERR_PENDING || rc == LC_ERR_INVALID) && next < count) {
      ++next;
      lc_error_cleanup(e);
      rc = LC_OK;
    }
  }
  for (i = 0; i < count; ++i) {
    __atomic_store_n(&items[i]->retiring, 0, __ATOMIC_RELEASE);
    ps_namespace_release(ns);
  }
  return rc;
}
int ps_namespace_reclaim(ps_namespace *ns, unsigned int kind, uint64_t id,
                         lc_error *e) {
  struct ps_namespace_data *d;
  ps_ns_resource *r;
  ps_checkpoint_info coverage;
  ps_stream_info info;
  uint64_t closed, pending;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_namespace_checkpoint_flush(ns, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  rc = ps_ns_require_unpinned(d, e);
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  if (rc == LC_OK &&
      (!id || (kind != PS_CATALOG_BODY && kind != PS_CATALOG_SEGMENT)))
    rc = failure(e, "namespace source is not obsolete");
  else if (rc == LC_OK) {
    r = ps_ns_find(d, kind, id);
    if (r)
      rc = ps_stream_status(r->stream, &info, e);
    if (rc == LC_OK && r &&
        (r->unselected || (kind == PS_CATALOG_BODY && !info.sealed) ||
         ps_key_checkpoint_source_refs(d->checkpoint, kind, id) ||
         (kind == PS_CATALOG_SEGMENT &&
          !segment_covered(d, r, &coverage, &info))))
      rc = failure(e, "namespace source is still referenced or uncovered");
    if (rc == LC_OK && r && kind == PS_CATALOG_SEGMENT)
      rc = ps_ns_stabilize_cut(d, coverage.control, e);
    if (rc == LC_OK && r)
      rc = drop(d, r, e);
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc == LC_OK)
    rc = reap(d, &closed, &pending, e);
  ps_ns_leave(d);
  return rc;
}
