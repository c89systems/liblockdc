#include "lc_api_internal.h"
#include "namespace_internal.h"
#include <string.h>
static int failure(lc_error *, const char *);

struct ps_namespace_body {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_ns_resource *resource;
  ps_record_writer writer;
  ps_budget *budget;
  uint64_t bytes;
  int attempted;
};
int ps_namespace_body_begin(ps_namespace *ns, uint64_t bytes,
                            ps_namespace_body **out, lc_error *e) {
  ps_namespace_body *b;
  ps_budget_plan plan = {0};
  struct ps_namespace_data *d;
  uint64_t id;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, "namespace body build requires output");
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
  ps_namespace_retain(ns);
  rc = ps_catalog_allocate_id(ns->root->space, &id, e);
  if (rc == LC_OK) {
    b->resource = ps_ns_resource_new(PS_CATALOG_BODY, id, e);
    if (!b->resource)
      rc = LC_ERR_NOMEM;
  }
  if (rc == LC_OK) {
    ps_ns_resource *r = b->resource;
    rc = ps_budget_plan_stream(&plan, e);
    if (rc == LC_OK)
      rc = ps_budget_plan_catalog(&plan, strlen(r->leaf), e);
    if (rc == LC_OK)
      rc = ps_budget_plan_span(&plan, bytes, e);
    if (rc == LC_OK)
      rc = ps_budget_open(ns->root->space, &plan, &b->budget, e);
    r->binding.stream.owner = id;
    r->binding.stream.key = ns->identity.owner;
    r->binding.stream.kind = PS_STREAM;
    if (rc == LC_OK)
      rc = ps_stream_open(ns->root->space, b->budget, r->binding.stream, 1,
                          NULL, NULL, NULL, &r->stream, e);
    if (rc == LC_OK)
      rc = ps_record_begin(r->stream, &b->writer, e);
    if (rc == LC_OK)
      rc = ps_stream_reserve_exact(b->writer.writer, b->budget, bytes, e);
  }
  if (rc == LC_OK)
    *out = b;
  else
    ps_namespace_body_close(b);
  return rc;
}
lc_pouch_record_io ps_namespace_body_output(ps_namespace_body *b) {
  return ps_record_output(b ? &b->writer : NULL);
}
const char *ps_namespace_body_leaf(const ps_namespace_body *b) {
  return b && b->resource ? b->resource->leaf : NULL;
}
int ps_namespace_body_commit(ps_namespace_body *b, lc_error *e) {
  ps_stream_info info;
  ps_ns_resource *r;
  int rc;
  if (!b || b->attempted || !b->writer.writer || b->writer.tail != b->bytes)
    return failure(e, "body build is closed or differs from its measured size");
  rc = ps_ns_enter(b->data, e);
  if (rc != LC_OK)
    return rc;
  b->attempted = 1;
  r = b->resource;
  rc = ps_record_commit(&b->writer, NULL, NULL, e);
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
    ps_ns_insert(b->data, r);
    pthread_mutex_unlock(&b->data->mutex);
  }
  ps_ns_leave(b->data);
  return rc;
}
void ps_namespace_body_close(ps_namespace_body *b) {
  if (!b)
    return;
  ps_record_abort(&b->writer);
  if (b->resource && !b->resource->pooled) {
    if (b->resource->stream && ps_check(b->ns->root->space, NULL) == LC_OK)
      ps_stream_retire(b->resource->stream, NULL);
    ps_ns_resource_free(b->resource);
  }
  ps_budget_close(b->budget, NULL);
  ps_namespace_release(b->ns);
  lc_free_with_allocator(NULL, b);
}
struct ps_namespace_snapshot {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_ns_resource *resource;
  ps_namespace_view *view;
  ps_record_writer writer;
  uint64_t base, boundary;
  int building, attempted;
};
static int failure(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_INVALID, 0, message, NULL, NULL, "pouch");
}
/* Mutation authority excludes directory changes, while the mutex excludes read
 * admission. Catalog removal precedes physical retirement, never the reverse.
 */
static int drop(struct ps_namespace_data *d, ps_ns_resource *r, lc_error *e) {
  int rc = ps_catalog_remove(d->ns->root->space, d->ns->identity.owner, r->kind,
                             r->leaf, strlen(r->leaf), &r->binding.revision, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    ps_ns_remove(d, r);
    pthread_mutex_unlock(&d->mutex);
  }
  return rc;
}
static int reap(struct ps_namespace_data *d, uint64_t *closed,
                uint64_t *pending, lc_error *e) {
  ps_ns_resource *r = d->retired, **link = &d->retired;
  ps_stream_info info;
  int rc = LC_OK;
  *closed = *pending = 0;
  while (r && rc == LC_OK) {
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
      ps_ns_resource_free(r);
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
      rc = reap(d, closed, pending, e);
      ps_ns_leave(d);
    }
  }
  return rc;
}
int ps_namespace_reclaim(ps_namespace *ns, unsigned int kind, uint64_t id,
                         lc_error *e) {
  struct ps_namespace_data *d;
  ps_ns_resource *r;
  uint64_t closed, pending;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  if (!id || (kind != PS_CATALOG_BODY &&
              (!d->snapshot_id ||
               (kind == PS_CATALOG_SEGMENT    ? id > d->snapshot_boundary
                : kind == PS_CATALOG_SNAPSHOT ? id >= d->snapshot_id
                                              : 1))))
    rc = failure(e, "namespace source is not obsolete");
  else {
    r = ps_ns_find(d, kind, id);
    if (r)
      rc = drop(d, r, e);
    if (rc == LC_OK)
      rc = reap(d, &closed, &pending, e);
  }
  ps_ns_leave(d);
  return rc;
}
int ps_namespace_snapshot_begin(ps_namespace *ns, ps_namespace_snapshot **out,
                                lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_snapshot *b;
  uint64_t id, closed, pending;
  size_t i;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, "namespace snapshot requires output");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  b = lc_calloc_with_allocator(NULL, 1, sizeof(*b));
  if (!b)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate snapshot builder",
                        NULL, NULL, "pouch");
  b->ns = ns;
  b->data = d;
  ps_namespace_retain(ns);
  rc = ps_ns_enter(d, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    if (d->snapshot_building)
      rc = failure(e, "namespace already has a snapshot builder");
    else
      b->building = d->snapshot_building = 1;
    pthread_mutex_unlock(&d->mutex);
    if (rc == LC_OK)
      rc = ps_ns_finish_seal(d, e);
    /* IDs are globally monotonic. Before selecting a higher ID remove every
     * unpublished binding, so a retained older ID always denotes a snapshot
     * which was actually selected, including after crash/reopen. */
    for (i = 0; rc == LC_OK && i < d->count;) {
      ps_ns_resource *r = d->items[i];
      if (r->kind == PS_CATALOG_SNAPSHOT && r->unselected)
        rc = drop(d, r, e);
      else
        ++i;
    }
    if (rc == LC_OK)
      rc = reap(d, &closed, &pending, e);
    if (rc == LC_OK && !d->active)
      rc = failure(e, "namespace has no snapshot prefix");
    if (rc == LC_OK && d->tail)
      rc = ps_ns_rotate(d, e);
    if (rc == LC_OK && !d->snapshot_id && d->active - 1 <= d->snapshot_boundary)
      rc = failure(e, "namespace snapshot prefix has not advanced");
    if (rc == LC_OK) {
      b->base = d->snapshot_id;
      b->boundary = d->active - 1;
      rc = ps_namespace_view_open(ns, &b->view, e);
    }
    if (rc == LC_OK)
      rc = ps_catalog_allocate_id(ns->root->space, &id, e);
    if (rc == LC_OK) {
      b->resource = ps_ns_resource_new(PS_CATALOG_SNAPSHOT, id, e);
      if (!b->resource)
        rc = LC_ERR_NOMEM;
    }
    if (rc == LC_OK) {
      ps_ns_resource *r = b->resource;
      r->unselected = 1;
      r->binding.stream.owner = id;
      r->binding.stream.key = ns->identity.owner;
      r->binding.stream.kind = PS_STREAM;
      rc = ps_stream_open(ns->root->space, NULL, r->binding.stream, 1, NULL,
                          NULL, NULL, &r->stream, e);
      if (rc == LC_OK)
        rc = ps_record_begin(r->stream, &b->writer, e);
    }
    ps_ns_leave(d);
  }
  if (rc == LC_OK)
    *out = b;
  else
    ps_namespace_snapshot_close(b);
  return rc;
}
ps_namespace_view *ps_namespace_snapshot_view(ps_namespace_snapshot *b) {
  return b ? b->view : NULL;
}
lc_pouch_record_io ps_namespace_snapshot_output(ps_namespace_snapshot *b) {
  return ps_record_output(b ? &b->writer : NULL);
}
const char *ps_namespace_snapshot_leaf(const ps_namespace_snapshot *b) {
  return b && b->resource ? b->resource->leaf : NULL;
}
uint64_t ps_namespace_snapshot_boundary(const ps_namespace_snapshot *b) {
  return b ? b->boundary : 0;
}
int ps_namespace_snapshot_reserve(ps_namespace_snapshot *b, uint64_t bytes,
                                  lc_error *e) {
  if (!b || b->attempted || !b->writer.writer)
    return failure(e, "namespace snapshot is not open");
  return ps_stream_reserve_exact(b->writer.writer, NULL, bytes, e);
}
int ps_namespace_snapshot_commit(ps_namespace_snapshot *b, lc_error *e) {
  struct ps_namespace_data *d;
  ps_ns_resource *r;
  ps_stream_info info;
  int rc;
  if (!b || b->attempted || !b->writer.writer)
    return failure(e, "namespace snapshot is not open");
  d = b->data;
  r = b->resource;
  rc = ps_check(b->ns->root->space, e);
  if (rc == LC_OK)
    rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  b->attempted = 1;
  if (d->snapshot_id != b->base || d->active <= b->boundary)
    rc = failure(e, "namespace snapshot base is stale");
  if (rc == LC_OK)
    rc = ps_ns_finish_seal(d, e);
  if (rc == LC_OK)
    rc = ps_record_commit(&b->writer, NULL, NULL, e);
  if (rc == LC_OK)
    rc = ps_stream_seal(r->stream, e);
  if (rc == LC_OK)
    rc = ps_stream_status(r->stream, &info, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    rc = ps_ns_index_room(d, e);
    pthread_mutex_unlock(&d->mutex);
  }
  if (rc == LC_OK)
    rc = ps_catalog_put(b->ns->root->space, NULL, b->ns->identity.owner,
                        PS_CATALOG_SNAPSHOT, r->leaf, strlen(r->leaf), NULL,
                        r->binding.stream, &info.ref, &r->binding, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    ps_ns_insert(d, r);
    pthread_mutex_unlock(&d->mutex);
    rc = ps_ns_head_write(d, NULL, d->active, r->id, b->boundary, e);
  }
  if (rc == LC_OK)
    rc = ps_flush(b->ns->root->space, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->snapshot_id = r->id;
    d->snapshot_boundary = b->boundary;
    r->unselected = 0;
    pthread_mutex_unlock(&d->mutex);
  }
  ps_ns_leave(d);
  return rc;
}
void ps_namespace_snapshot_close(ps_namespace_snapshot *b) {
  if (!b)
    return;
  ps_record_abort(&b->writer);
  ps_namespace_view_close(b->view);
  if (b->resource && !b->resource->pooled) {
    if (b->resource->stream && ps_check(b->ns->root->space, NULL) == LC_OK)
      ps_stream_retire(b->resource->stream, NULL);
    ps_ns_resource_free(b->resource);
  }
  if (b->building) {
    pthread_mutex_lock(&b->data->mutex);
    b->data->snapshot_building = 0;
    pthread_mutex_unlock(&b->data->mutex);
  }
  ps_namespace_release(b->ns);
  lc_free_with_allocator(NULL, b);
}
